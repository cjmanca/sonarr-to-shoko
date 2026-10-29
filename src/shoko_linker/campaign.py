"""M7-b: unattended backlog campaign — one approval, many bounded batches.

The campaign is the PLAN M7 controlled path for the finite backlog: an operator reviews
an exported canonical **dry-run plan** (produced by the daemon machinery in ``DRY_RUN``
— zero writes), approves **one** campaign by supplying the plan file's canonical
SHA-256 digest *separately from the file* (``campaign approve``), then runs bounded
``campaign apply`` invocations that process **only approved candidates** through the M5
guarded path (preflight → ``prepare_send`` → ``mark_sent`` → ``submit_link`` → record →
inline verify) under the M6 durability machine. Progress persists in the state DB;
repeated invocations consume the ONE approval (no re-approval); a changed plan file
pauses the campaign (``plan_drift``) with zero POSTs, a changed candidate is
**quarantined** (``campaign_quarantine`` — never silently replaced, never
auto-approved), and global churn / mapping drift / write-stop / source discontinuity /
lease expiry pause the campaign (persisted status, reported).

Pending-target flow (PLAN: mapped-but-not-yet-created Shoko episodes stay approved):
a candidate approved with **exact preapproved AniDB episode identities** and an empty
``shoko_episode_ids`` list resolves its Shoko internal IDs at apply time via the M5
resolver; the resolved identities must equal the preapproved set **exactly** (mismatch
→ ``target_drift`` quarantine, zero POSTs for that file), and the POST happens only
after normal preflight. The approved set never expands: a file the plan file does not
name is never processed, and a newly resolvable mapping for a non-approved file never
enters the campaign.

Attestation: ``campaign apply`` requires a valid lease scoped ``campaign:<id>``
(:mod:`shoko_linker.attestation`) plus the standard gate/eligibility conjunction reused
from the daemon.

Shoko queue guard (M7 review P1): every candidate passes the same preflight queue check
as the daemon — ``BlockedCount`` above :func:`shoko_linker.daemon.effective_blocked_over`
(config ``sync.queue_blocked_tolerance``, default 0) **defers** the candidate
(``QUEUE_BLOCKED`` is a deferring reason: zero POSTs, the row stays ``planned``, no
quarantine) and a missing queue snapshot defers via ``QUEUE_UNKNOWN``.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import math
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final, cast

from .attestation import AttestationVerdict
from .config import Settings, apply_gate
from .daemon import (
    CycleMode,
    CycleReport,
    DaemonDeps,
    apply_lock_lease_expiry,
    attestation_gate,
    check_plan_sonarr_source,
    compute_eligibility,
    effective_blocked_over,
    mapping_generation_stale,
    run_cycle,
    verify_and_close,
)
from .errors import LinkerError
from .linking import (
    EpisodeIdResolver,
    EpisodeResolutionStatus,
    LinkPlan,
    LinkTarget,
    PlanStatus,
    PreflightReason,
    QueueSnapshot,
    SendOutcome,
    SendResult,
    fetch_queue,
    plan_fingerprint,
    preflight,
    preflight_file,
    preflight_link_jobs_and_targets,
    preflight_reasons_are_retryable,
    primary_hard_preflight_reason,
    request_fingerprint,
    submit_link,
)
from .mapping_store import ActiveMappings, load_active
from .policies import RunPolicy
from .shoko import ImportFolder, canonical_ed2k
from .sonarr import SourceRelationshipStatus
from .state import (
    CAMPAIGN_DRAIN_REVOKED_REASON,
    CAMPAIGN_QUARANTINE_PROGRESS_PENDING_MARKER,
    CAMPAIGN_QUARANTINE_PROGRESS_RECORDED_MARKER,
    CAMPAIGN_STATUS_APPROVED,
    META_CAMPAIGN_ATTESTATION_GATE,
    META_CAMPAIGN_DEFERRED_UNTIL,
    META_CAMPAIGN_ZERO_PROGRESS,
    ApplyLockUnavailableError,
    BudgetRefusal,
    CampaignBinding,
    CampaignDrainRefusedError,
    CampaignRecord,
    CampaignSendRefusedError,
    CampaignSubmissionLimitError,
    Cohort,
    FileState,
    LockHeldError,
    SendToken,
    StateError,
    StateStore,
    TransitionError,
    WriteDisabledError,
    acquire_or_steal_apply_lock,
    release_apply_lock,
)

PLAN_SCHEMA: Final[str] = "shoko-linker-campaign-plan/2"
DIGEST_HEX_CHARS: Final[int] = 64
ANIDB_IDENTITY_FIELDS: Final[int] = 4
MAX_DETAIL_CHARS: Final[int] = 1000
PLAN_DOCUMENT_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "schema",
        "campaign_id",
        "created_iso",
        "source_instance",
        "mapping_revision",
        "targets_digest",
        "structure_digest",
        "policy",
        "files",
        "totals",
        "deadline_iso",
        "rate_limits",
        "stop_conditions",
    }
)
STOP_CONDITIONS: Final[tuple[str, ...]] = (
    "mapping_churn",
    "mapping_drift",
    "writes_disabled",
    "source_discontinuity",
    "attestation_expired",
    "deadline_exceeded",
    "max_submissions_reached",
    "plan_drift",
)
_CAMPAIGN_QUEUE_STATES: Final[frozenset[FileState]] = frozenset(
    {FileState.PLANNED, FileState.MAPPED}
)
#: Budget refusals that are campaign-wide — every later file in the batch refuses identically,
#: so the batch stops and defers the rest (F9-1d). A per-file ``pending_episode_jobs_exhausted``
#: depends on *that file's* episode count and does NOT stop the batch (healthy later files
#: still send); it is a non-destructive per-file deferral.
_CAMPAIGN_WIDE_BUDGET_REFUSALS: Final[frozenset[str]] = frozenset(
    {
        "paused",
        "pending_files_exhausted",
        "cycle_new_exhausted",
        "rolling_new_exhausted",
        "legacy_campaign_history_unresolved",
    }
)
_SEND_PROGRESS_STATES: Final[frozenset[FileState]] = frozenset(
    {
        FileState.REQUEST_MAY_HAVE_BEEN_SENT,
        FileState.VERIFYING,
        FileState.VERIFIED,
        FileState.PARTIAL,
        FileState.INDETERMINATE_WRITE,
        FileState.CONFLICT,
    }
)
logger = logging.getLogger(__name__)


class CampaignError(LinkerError):
    """The plan file is malformed, a campaign command was refused, or nothing is eligible."""


class CampaignDigestMismatchError(CampaignError):
    """The plan file's canonical digest does not equal the separately supplied digest."""

    def __init__(self, supplied: str, computed: str) -> None:
        super().__init__(
            f"approval digest mismatch: supplied {supplied} != plan file digest {computed} "
            "(the plan file and the approval digest must describe the same canonical document)"
        )
        self.supplied = supplied
        self.computed = computed


class CampaignPlanInvalidError(CampaignError):
    """The plan file on disk is unusable: unreadable, non-canonical JSON, or schema-invalid.

    M7 review P3-3: this is **invalid input** (CLI exit 2), not a campaign-state refusal
    (exit 3). Campaign-state refusals (not-approved/paused/revoked/deadline/
    max-submissions/plan drift) stay ``CampaignError`` → exit 3.
    """


class CampaignEligibilityRefusalError(CampaignError):
    """The cycle eligibility verdict refused campaign apply.

    F18 review (Finding 4): the refusal carries the **structured** verdict tuple, so the
    daemon's recoverable-deferral branch triggers ONLY on the pure ``budgets_exhausted``
    verdict. A mixed refusal (e.g. budget exhaustion together with ``mapping_unstable`` /
    ``source_unstable``) auto-pauses the campaign through the drift family; a deferral
    marker written there is inert while paused and stale on operator resume.
    """

    def __init__(self, reasons: tuple[str, ...]) -> None:
        super().__init__(
            f"campaign apply refused — eligibility reasons: {', '.join(reasons)} "
            "(campaign status reported; zero POSTs)"
        )
        self.eligibility_reasons = reasons


class CampaignDeferredError(CampaignError):
    """A non-destructive campaign batch deferral with stable machine-readable reasons."""

    def __init__(self, reason_code: str, reason_codes: Sequence[str], message: str) -> None:
        self.reason_code = reason_code
        self.reason_codes = tuple(dict.fromkeys(reason_codes))
        super().__init__(message)


class CampaignReconciliationIncompleteError(CampaignDeferredError):
    """The dry-run inventory snapshot is insufficient to make campaign drift decisions."""

    def __init__(self, reason_codes: Sequence[str]) -> None:
        normalized = tuple(dict.fromkeys(reason_codes))
        detail = ", ".join(normalized) or "reconciliation_not_ready"
        super().__init__(
            "reconciliation_incomplete",
            normalized,
            f"campaign apply deferred: reconciliation incomplete ({detail}); "
            "approved targets and progress are unchanged, zero POSTs",
        )


class CampaignMappingGenerationChangedError(CampaignDeferredError):
    """The mapping generation changed after the campaign sweep; retry with a fresh sweep."""

    def __init__(self) -> None:
        super().__init__(
            "mapping_generation_changed",
            (),
            "campaign apply deferred: mapping generation changed after reconciliation; "
            "retry with a fresh sweep, zero POSTs",
        )


# ------------------------------------------------------------------ plan records


type AnidbIdentity = tuple[int, int, str, int]


@dataclass(frozen=True)
class CampaignFile:
    """One approved plan-file entry (the binding contract for every apply)."""

    shoko_file_id: int
    sonarr_series_id: int
    sonarr_file_id: int
    path: str
    size: int
    shoko_ed2k: str
    plan_fingerprint: str
    request_fingerprint: str
    mapping_targets_digest: str
    anidb_target_identities: tuple[AnidbIdentity, ...]
    shoko_episode_ids: tuple[int, ...]


@dataclass(frozen=True)
class CampaignPlan:
    """The canonical plan document (the approval digest covers every byte of it)."""

    schema: str
    campaign_id: str
    created_iso: str
    source_instance: str
    mapping_revision: str
    targets_digest: str
    structure_digest: str
    files: tuple[CampaignFile, ...]
    max_submissions: int
    deadline_iso: str
    rate_limits: Mapping[str, float]
    stop_conditions: tuple[str, ...]
    policy: str


def _canonical_bytes(plan: CampaignPlan) -> bytes:
    document = {
        "schema": plan.schema,
        "campaign_id": plan.campaign_id,
        "created_iso": plan.created_iso,
        "source_instance": plan.source_instance,
        "mapping_revision": plan.mapping_revision,
        "targets_digest": plan.targets_digest,
        "structure_digest": plan.structure_digest,
        "policy": plan.policy,
        "files": [
            {
                "shoko_file_id": item.shoko_file_id,
                "sonarr_series_id": item.sonarr_series_id,
                "sonarr_file_id": item.sonarr_file_id,
                "path": item.path,
                "size": item.size,
                "shoko_ed2k": item.shoko_ed2k,
                "plan_fingerprint": item.plan_fingerprint,
                "request_fingerprint": item.request_fingerprint,
                "mapping_targets_digest": item.mapping_targets_digest,
                "anidb_target_identities": [
                    list(identity) for identity in item.anidb_target_identities
                ],
                "shoko_episode_ids": list(item.shoko_episode_ids),
            }
            for item in plan.files
        ],
        "totals": {"max_submissions": plan.max_submissions, "files": len(plan.files)},
        "deadline_iso": plan.deadline_iso,
        "rate_limits": dict(plan.rate_limits),
        "stop_conditions": list(plan.stop_conditions),
    }
    return (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def plan_digest(plan: CampaignPlan) -> str:
    """Canonical SHA-256 of the plan document — the **approval digest**."""
    return hashlib.sha256(_canonical_bytes(plan)).hexdigest()


def write_plan_file(path: str | Path, plan: CampaignPlan) -> str:
    """Write the plan file as its canonical bytes; returns the approval digest."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(_canonical_bytes(plan))
    return plan_digest(plan)


def read_plan_file(path: str | Path) -> tuple[CampaignPlan, str]:
    """Parse and re-digest a plan file (the stored bytes are the canonical form).

    Every parse/schema failure raises :class:`CampaignPlanInvalidError` (invalid input,
    CLI exit 2); it is never reported as a campaign-state refusal.
    """
    target = Path(path)
    try:
        raw = target.read_bytes()
    except OSError as exc:
        raise CampaignPlanInvalidError(
            f"cannot read plan file {target}: {exc.strerror or exc}"
        ) from exc
    digest = hashlib.sha256(raw).hexdigest()
    try:
        document = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise CampaignPlanInvalidError(
            f"plan file {target} is not valid canonical JSON: {exc}"
        ) from exc
    try:
        plan = _plan_from_document(document)
    except CampaignError as exc:
        raise CampaignPlanInvalidError(str(exc)) from exc
    return plan, digest


def _plan_from_document(raw: Any) -> CampaignPlan:
    if not isinstance(raw, dict):
        raise CampaignError("plan file: expected a JSON object")
    document = cast("dict[str, Any]", raw)
    _require_plan_fields(document)
    if document["schema"] != PLAN_SCHEMA:
        if document["schema"] == "shoko-linker-campaign-plan/1":
            raise CampaignError(
                "plan file uses legacy schema 'shoko-linker-campaign-plan/1', which does not "
                "freeze Shoko ED2K file-content identity; re-prepare and re-approve the campaign"
            )
        raise CampaignError(f"plan file: schema must be {PLAN_SCHEMA!r}")
    campaign_id = document["campaign_id"]
    if not isinstance(campaign_id, str) or not campaign_id.strip():
        raise CampaignError("plan file: campaign_id must be a non-empty string")
    created_iso = _require_iso(document["created_iso"], "created_iso")
    deadline_iso = _require_iso(document["deadline_iso"], "deadline_iso")
    for key in ("targets_digest", "structure_digest"):
        _require_hex64(document[key], key)
    max_submissions, files_total = _plan_totals(document)
    files = _plan_file_entries(document["files"], files_total, max_submissions)
    rate_limits = _plan_rate_limits(document)
    stop_conditions = _plan_stop_conditions(document)
    created = _parse_iso(created_iso)
    deadline = _parse_iso(deadline_iso)
    if deadline <= created:
        raise CampaignError("plan file: deadline_iso must be after created_iso")
    return CampaignPlan(
        schema=PLAN_SCHEMA,
        campaign_id=campaign_id.strip(),
        created_iso=created_iso,
        source_instance=str(document["source_instance"]),
        mapping_revision=str(document["mapping_revision"]),
        targets_digest=str(document["targets_digest"]),
        structure_digest=str(document["structure_digest"]),
        files=files,
        max_submissions=max_submissions,
        deadline_iso=deadline_iso,
        rate_limits=rate_limits,
        stop_conditions=stop_conditions,
        policy=str(document["policy"]),
    )


def _require_plan_fields(document: Mapping[str, Any]) -> None:
    """The plan document carries exactly the schema's top-level fields."""
    if set(document) != PLAN_DOCUMENT_FIELDS:
        missing = sorted(PLAN_DOCUMENT_FIELDS - set(document))
        extra = sorted(set(document) - PLAN_DOCUMENT_FIELDS)
        raise CampaignError(
            f"plan file: fields must be exactly the schema fields (missing {missing}, "
            f"extra {extra})"
        )


def _plan_totals(document: Mapping[str, Any]) -> tuple[int, int]:
    totals = document["totals"]
    if not isinstance(totals, dict) or set(totals) != {"max_submissions", "files"}:
        raise CampaignError("plan file: totals must carry max_submissions and files")
    max_submissions = totals["max_submissions"]
    files_total = totals["files"]
    if (
        isinstance(max_submissions, bool)
        or not isinstance(max_submissions, int)
        or max_submissions < 1
        or isinstance(files_total, bool)
        or not isinstance(files_total, int)
        or files_total < 1
    ):
        raise CampaignError("plan file: totals must be positive whole numbers")
    return int(max_submissions), int(files_total)


def _plan_rate_limits(document: Mapping[str, Any]) -> Mapping[str, float]:
    rate_raw = document["rate_limits"]
    if not isinstance(rate_raw, dict):
        raise CampaignError("plan file: rate_limits must be a mapping")
    rate_limits = cast("Mapping[str, Any]", rate_raw)
    for key, value in rate_limits.items():
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise CampaignError(f"plan file: rate_limits.{key} must be a number")
    return MappingProxyType({str(key): float(value) for key, value in rate_limits.items()})


def _plan_stop_conditions(document: Mapping[str, Any]) -> tuple[str, ...]:
    stop_raw = document["stop_conditions"]
    if not isinstance(stop_raw, list) or not all(isinstance(item, str) for item in stop_raw):
        raise CampaignError("plan file: stop_conditions must be a list of strings")
    return tuple(stop_raw)


def _plan_file_entries(
    files_raw: Any, files_total: int, max_submissions: int
) -> tuple[CampaignFile, ...]:
    if not isinstance(files_raw, list) or not files_raw:
        raise CampaignError("plan file: files must be a non-empty list")
    if len(files_raw) != files_total or max_submissions != files_total:
        raise CampaignError(
            "plan file: totals.files and totals.max_submissions must equal the number of "
            "entries in files (one submission per approved file)"
        )
    return tuple(_campaign_file(item, index) for index, item in enumerate(files_raw))


def _campaign_file(item: Any, index: int) -> CampaignFile:
    field = f"plan file: files[{index}]"
    if not isinstance(item, dict):
        raise CampaignError(f"{field}: expected an object")
    entry = cast("dict[str, Any]", item)
    required = {
        "shoko_file_id",
        "sonarr_series_id",
        "sonarr_file_id",
        "path",
        "size",
        "shoko_ed2k",
        "plan_fingerprint",
        "request_fingerprint",
        "mapping_targets_digest",
        "anidb_target_identities",
        "shoko_episode_ids",
    }
    if set(entry) != required:
        raise CampaignError(f"{field}: fields must be exactly the entry schema fields")

    def whole(key: str) -> int:
        value = entry[key]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise CampaignError(f"{field}: {key} must be a non-negative whole number")
        return int(value)

    identities_raw = entry["anidb_target_identities"]
    if not isinstance(identities_raw, list) or not identities_raw:
        raise CampaignError(f"{field}: anidb_target_identities must be a non-empty list")
    identities: list[AnidbIdentity] = []
    for identity in identities_raw:
        if (
            not isinstance(identity, list)
            or len(identity) != ANIDB_IDENTITY_FIELDS
            or isinstance(identity[0], bool)
            or isinstance(identity[1], bool)
            or isinstance(identity[3], bool)
            or not isinstance(identity[0], int)
            or not isinstance(identity[1], int)
            or not isinstance(identity[3], int)
            or identity[3] <= 0
            or not isinstance(identity[2], str)
        ):
            raise CampaignError(
                f"{field}: every identity is [anidb_anime_id, anidb_episode_number, "
                "episode_type, anidb_episode_id]"
            )
        identities.append((identity[0], identity[1], identity[2], identity[3]))
    episode_ids_raw = entry["shoko_episode_ids"]
    if not isinstance(episode_ids_raw, list):
        raise CampaignError(f"{field}: shoko_episode_ids must be a list (empty = pending target)")
    episode_ids: list[int] = []
    for value in episode_ids_raw:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise CampaignError(f"{field}: shoko_episode_ids entries must be positive integers")
        episode_ids.append(value)
    path = entry["path"]
    if not isinstance(path, str) or not path.startswith("/"):
        raise CampaignError(f"{field}: path must be a POSIX path string")
    ed2k = canonical_ed2k(entry["shoko_ed2k"])
    if ed2k is None:
        raise CampaignError(f"{field}: shoko_ed2k must be a raw 32-hex ED2K value")
    for key in ("plan_fingerprint", "request_fingerprint", "mapping_targets_digest"):
        _require_hex64(entry[key], f"{field}: {key}")
    return CampaignFile(
        shoko_file_id=whole("shoko_file_id"),
        sonarr_series_id=whole("sonarr_series_id"),
        sonarr_file_id=whole("sonarr_file_id"),
        path=path,
        size=whole("size"),
        shoko_ed2k=ed2k,
        plan_fingerprint=str(entry["plan_fingerprint"]),
        request_fingerprint=str(entry["request_fingerprint"]),
        mapping_targets_digest=str(entry["mapping_targets_digest"]),
        anidb_target_identities=tuple(sorted(identities)),
        shoko_episode_ids=tuple(sorted(episode_ids)),
    )


def _require_iso(value: Any, name: str) -> str:
    if not isinstance(value, str):
        raise CampaignError(f"plan file: {name} must be an ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise CampaignError(f"plan file: {name} is not a valid ISO-8601 timestamp: {exc}") from exc
    return parsed.isoformat()


def _require_hex64(value: Any, name: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != DIGEST_HEX_CHARS
        or not all(char in "0123456789abcdef" for char in value)
    ):
        raise CampaignError(f"plan file: {name} must be a lowercase SHA-256 hex digest")


# ------------------------------------------------------------------ prepare


def source_instance(settings: Settings) -> str:
    """The campaign's recorded source identity (configured URLs — docs §4, no invented probes)."""
    return f"sonarr={settings.sonarr.url};shoko={settings.shoko.url}"


def _plan_entry(plan: LinkPlan, mapping_targets_digest: str) -> CampaignFile:
    identities = {
        (
            target.anidb_anime_id,
            target.anidb_episode_number,
            target.episode_type,
            target.anidb_episode_id,
        )
        for target in plan.targets
    } | {
        (item.anidb_anime_id, item.anidb_episode_number, item.episode_type, item.anidb_episode_id)
        for item in plan.pending_targets
    }
    return CampaignFile(
        shoko_file_id=plan.shoko_file_id,
        sonarr_series_id=plan.sonarr_series_id,
        sonarr_file_id=plan.sonarr_episode_file_id,
        path=plan.sonarr_path,
        size=plan.sonarr_size,
        shoko_ed2k=plan.shoko_ed2k.lower(),
        plan_fingerprint=plan.fingerprint,
        request_fingerprint=request_fingerprint(plan),
        mapping_targets_digest=mapping_targets_digest,
        anidb_target_identities=tuple(sorted(identities)),
        shoko_episode_ids=plan.episode_ids,
    )


def _campaign_deadline(created: datetime, hours: float) -> datetime:
    if not math.isfinite(hours) or hours <= 0:
        raise CampaignError("--deadline-hours must be finite and greater than zero")
    try:
        deadline = created + timedelta(hours=hours)
    except OverflowError as exc:
        raise CampaignError("--deadline-hours exceeds the supported datetime range") from exc
    if deadline <= created:
        raise CampaignError("--deadline-hours is too small to produce a future deadline")
    return deadline


def prepare_campaign(
    deps: DaemonDeps,
    settings: Settings,
    store: StateStore,
    now_iso: str,
    *,
    limit: int,
    deadline_hours: float,
) -> tuple[CampaignPlan, str]:
    """Full dry-run reconciliation (daemon machinery, **zero writes**) → canonical plan.

    Selects backlog-cohort, mapping-resolved, plan-frozen candidates (``planned`` state;
    pending-target plans enter with an empty ``shoko_episode_ids`` and their preapproved
    AniDB identities) bounded by ``limit`` (capped by ``campaign.max_campaign_files``).
    Missing/ambiguous mapping answers never enter — the digest covers only included
    files. Prepare runs a ``DRY_RUN`` cycle (the daemon skips the apply lock in that mode,
    sends nothing) and is intentionally **lock-free**: it opens no writes, so it takes no
    apply lock.
    """
    if limit < 1:
        raise CampaignError("--limit must be at least 1")
    created = _parse_iso(now_iso)
    deadline = _campaign_deadline(created, deadline_hours)
    cap = min(limit, settings.campaign.max_campaign_files)
    report = run_cycle(
        deps,
        now_iso=now_iso,
        mode=CycleMode.DRY_RUN,
        policy=RunPolicy.BACKLOG,
        cycle_key="campaign-prepare",
    )
    if report.failed:
        raise CampaignError(
            "dry-run reconciliation cycle failed (transport): no plan was written; fix "
            "connectivity and prepare again"
        )
    active = load_active(deps.mapping_store, _load_overrides(deps))
    if active is None:
        raise CampaignError(
            "no active mapping generation: refresh the mapping source before preparing a campaign"
        )
    if not report.reconciliation_ready:
        raise CampaignError(
            "reconciliation is not ready (an inventory is incomplete): prepare refuses"
        )
    entries: list[CampaignFile] = []
    for file_id in sorted(report.plans):
        frozen = report.plans[file_id]
        record = store.get_file(file_id)
        if record is None:
            continue
        if record.cohort is not Cohort.BACKLOG:
            continue
        linkable = frozen.status is PlanStatus.LINKABLE and record.state is FileState.PLANNED
        pending = frozen.status is PlanStatus.PENDING_TARGETS and record.state is FileState.MAPPED
        if not (linkable or pending):
            continue
        if frozen.mapping_revision != str(active.revision):
            continue
        entries.append(_plan_entry(frozen, active.targets_digest))
        if len(entries) >= cap:
            break
    if not entries:
        raise CampaignError(
            "no backlog candidates are plan-frozen right now: the dry-run cycle found no "
            "eligible planned files (missing/ambiguous mappings never enter a plan)"
        )
    plan = CampaignPlan(
        schema=PLAN_SCHEMA,
        campaign_id=str(uuid.uuid4()),
        created_iso=created.isoformat(),
        source_instance=source_instance(settings),
        mapping_revision=str(active.revision),
        targets_digest=active.targets_digest,
        structure_digest=active.structure_digest,
        files=tuple(entries),
        max_submissions=len(entries),
        deadline_iso=deadline.isoformat(),
        rate_limits=MappingProxyType(
            {
                "per_cycle_new": float(settings.sync.max_new_per_cycle),
                "rolling_new_limit": float(settings.sync.rolling_new_limit),
                "rolling_window_hours": settings.sync.rolling_window_hours,
                "max_pending_episode_jobs": float(settings.sync.max_pending_episode_jobs),
            }
        ),
        stop_conditions=STOP_CONDITIONS,
        policy=RunPolicy.BACKLOG.value,
    )
    return plan, plan_digest(plan)


def campaign_deadline_warning(
    file_count: int,
    deadline_hours: float,
    rolling_new_limit: int,
    rolling_window_hours: float,
    *,
    max_new_per_cycle: int,
    max_pending_episode_jobs: int,
) -> str | None:
    """Warn when capacity is disabled or the rolling-budget horizon reaches the deadline."""
    disabled_limits = []
    if max_new_per_cycle == 0:
        disabled_limits.append("max_new_per_cycle=0")
    if max_pending_episode_jobs == 0:
        disabled_limits.append("max_pending_episode_jobs=0")
    if rolling_new_limit == 0:
        disabled_limits.append("rolling_new_limit=0")
    if disabled_limits == ["rolling_new_limit=0"]:
        return (
            f"campaign has {file_count} files but rolling_new_limit=0, so it cannot complete "
            "under the configured submission limit; the deadline was not changed. The "
            "safety-attestation lease expires separately and must be renewed after reinspection"
        )
    if disabled_limits:
        return (
            f"campaign has {file_count} files but submission capacity is disabled by "
            f"{', '.join(disabled_limits)}, so it cannot complete under the configured "
            "submission limits; the deadline was not changed. The safety-attestation lease "
            "expires separately and must be renewed after reinspection"
        )
    required_windows = (file_count + rolling_new_limit - 1) // rolling_new_limit
    minimum_wait_hours = max(0, required_windows - 1) * rolling_window_hours
    if minimum_wait_hours < deadline_hours:
        return None
    return (
        f"campaign deadline is shorter than the optimistic minimum wait of "
        f"{minimum_wait_hours:g} hours for {file_count} files at "
        f"rolling_new_limit={rolling_new_limit} per {rolling_window_hours:g}-hour window; "
        "transient failures, pending Shoko jobs, and verification delays can extend completion. "
        "The deadline was not changed, and the safety-attestation lease expires separately "
        "and must be renewed after reinspection"
    )


def _load_overrides(deps: DaemonDeps) -> Any:
    from . import mappings as _mappings

    if deps.overrides_text is None:
        return None
    return _mappings.load_overrides(deps.overrides_text)


# ------------------------------------------------------------------ approve


def approve_campaign(
    store: StateStore,
    plan_path: str | Path,
    supplied_digest: str,
    now_iso: str,
) -> CampaignPlan:
    """Approve the campaign: the digest is supplied **separately** and must match.

    A mismatch raises :class:`CampaignDigestMismatchError` carrying **both** digests (the CLI
    exits 2). On match the campaign row persists (approved, bound digests, totals,
    deadline, attestation scope ``campaign:<id>``, approval time) and the per-file
    approved fingerprints bind onto the tracked rows.
    """
    plan, computed = read_plan_file(plan_path)
    if supplied_digest.strip().lower() != computed:
        raise CampaignDigestMismatchError(supplied_digest.strip().lower(), computed)
    if store.get_campaign(plan.campaign_id) is not None:
        raise CampaignError(f"campaign {plan.campaign_id} is already approved")
    bindings = tuple(
        CampaignBinding(
            shoko_file_id=item.shoko_file_id,
            plan_fingerprint=item.plan_fingerprint,
            request_fingerprint=item.request_fingerprint,
        )
        for item in plan.files
    )
    store.create_campaign(
        plan.campaign_id,
        computed,
        max_submissions=plan.max_submissions,
        deadline_iso=plan.deadline_iso,
        attestation_scope=f"campaign:{plan.campaign_id}",
        approval_iso=now_iso,
        iso=now_iso,
        bindings=bindings,
        plan_path=str(Path(plan_path).resolve()),
    )
    bound = store.bind_campaign_files(plan.campaign_id, now_iso)
    logger.info(
        "campaign %s approved: digest %s, %d file(s) in plan, %d row(s) bound",
        plan.campaign_id,
        computed,
        len(plan.files),
        bound,
    )
    return plan


# ------------------------------------------------------------------ apply


@dataclass(frozen=True)
class CampaignApplyReport:
    """One bounded ``campaign apply`` invocation outcome (every number is persisted truth).

    ``processed`` is a **scan** counter (files the batch examined: submissions, per-file budget
    deferrals and pending-target resolutions) — never submission progress.
    ``progress`` is the real progress the batch produced: submissions + verifications +
    newly-created quarantines. Repeated observations of an existing quarantine are deferred and
    do not count as fresh progress. The zero-progress breaker keys off ``progress`` (zero real
    progress → pause), and ``stopped = "plan_complete"`` reflects **persisted** campaign progress,
    never a scan count.
    """

    campaign_id: str
    processed: int
    submitted: int
    verified: int
    quarantined: int
    deferred: int
    progress: int
    total_sent: int
    remaining: int
    reasons: tuple[str, ...]
    stopped: str | None


@dataclass(frozen=True)
class _SendTally:
    submitted: int
    verified: int
    quarantined: int
    deferred: int
    reasons: tuple[str, ...]
    budget_blocked: bool = False
    #: F11: a per-submission freshness refusal (deadline/status/shutdown/generation swap) stops
    #: the batch — the remainder is deferred non-destructively (queueable), never quarantined.
    stop_batch: bool = False


def _bind_campaign(store: StateStore, campaign_id: str, iso: str) -> int:
    return store.bind_campaign_files(campaign_id, iso)


def _acquire_apply_lock(deps: DaemonDeps, now_iso: str) -> tuple[str | None, bool]:
    """Acquire the daemon's single-process apply lock for one campaign batch (F1, F5).

    Same ownership entry as :func:`shoko_linker.daemon._lock_preflight`:
    :func:`~shoko_linker.state.acquire_or_steal_apply_lock` takes the **OS advisory lock
    first** and holds it for the whole batch, so a batch longer than the meta lease can never
    be fenced out — a live owner (kernel-held lock) is contended and a meta
    lease expiry alone never grants ownership. Only a DEAD owner (expired lease + a free OS
    lock) is stolen (audit-recorded). The holder is named in the deferral reason; the campaign
    stays approved and no candidate is sent. ``deps.pid``/``deps.lease_seconds`` carry the
    CLI-wired ``os.getpid()``/``sync.apply_lock_lease_seconds``.
    """
    try:
        lease_iso = apply_lock_lease_expiry(now_iso, deps.lease_seconds)
    except ValueError as exc:
        raise CampaignError(str(exc)) from exc
    try:
        acquire_or_steal_apply_lock(deps.store, deps.pid, now_iso, lease_expires_iso=lease_iso)
    except LockHeldError as exc:
        holder = exc.holder.owner_pid if exc.holder is not None else "?"
        return (f"apply_lock_contended:{holder}", False)
    except ApplyLockUnavailableError as exc:
        return (f"apply_lock_unavailable:{exc}", False)
    except WriteDisabledError:
        return ("writes_disabled", False)
    return (None, True)


def _release_apply_lock(deps: DaemonDeps, now_iso: str) -> None:
    """Release the campaign apply lock in the ``finally`` (daemon pattern; F5).

    Meta ownership is released first, then the OS advisory lock. A store that stopped writing
    mid-batch is tolerated (the persisted lease is recovered by the expiry-steal rule, and the
    kernel frees the OS lock at process exit); a lease stolen after a crash past expiry is
    recorded in the ``last_lock_release_denied`` meta audit (the authoritative surface — the
    report was built before ``finally`` ran), never propagated, and never wedges the next
    invocation.
    """
    try:
        release_apply_lock(deps.store, deps.pid, now_iso)
    except WriteDisabledError:
        pass
    except LockHeldError as exc:
        holder = exc.holder.owner_pid if exc.holder is not None else "unknown"
        with contextlib.suppress(WriteDisabledError):
            deps.store.set_meta("last_lock_release_denied", str(holder))


def _defer_remaining(
    store: StateStore,
    files: Sequence[CampaignFile],
    counters: _CampaignCounters,
    reasons: list[str],
    prefix: str,
) -> None:
    """Defer the unprocessed tail of a stopped batch (F9/F11): non-destructive by design —
    every row stays queueable (never quarantined) for a later batch to re-validate."""
    for deferred in files:
        record = store.get_file(deferred.shoko_file_id)
        if record is None or record.state not in _CAMPAIGN_QUEUE_STATES:
            continue
        counters.deferred += 1
        reasons.append(f"{prefix}:{deferred.shoko_file_id}")


def _campaign_scan_order(
    store: StateStore, campaign_id: str, files: tuple[CampaignFile, ...]
) -> tuple[CampaignFile, ...]:
    """Start after the last scanned approved file, wrapping once through the immutable set."""
    raw_cursor = store.get_meta(f"campaign_scan_cursor:v1:{campaign_id}")
    try:
        cursor_file_id = int(raw_cursor or "0")
    except ValueError:
        cursor_file_id = 0
    cursor_position = next(
        (index for index, entry in enumerate(files) if entry.shoko_file_id == cursor_file_id),
        -1,
    )
    start = cursor_position + 1
    return files[start:] + files[:start]


def _fold_tally(counters: _CampaignCounters, tally: _SendTally, reasons: list[str]) -> None:
    """Fold one submission's tally into the batch counters and reason list."""
    counters.submitted += tally.submitted
    counters.verified += tally.verified
    counters.quarantined += tally.quarantined
    counters.deferred += tally.deferred
    counters.sent_running += tally.submitted
    reasons.extend(tally.reasons)


def _should_retry_rejected_file_at_scan_head(
    store: StateStore, campaign_id: str, file_id: int, tally: _SendTally
) -> bool:
    """Keep a first definite zero-write rejection at the scan head for one retry."""
    rejected_reasons = {
        f"send_{SendOutcome.REJECTED.value}:{file_id}",
        f"send_{SendOutcome.REFUSED.value}:{file_id}",
    }
    if not any(reason in rejected_reasons for reason in tally.reasons):
        return False
    history = tuple(
        row for row in store.campaign_send_history(campaign_id) if row.shoko_file_id == file_id
    )
    return len(history) == 1 and history[0].outcome in {
        SendOutcome.REJECTED.value,
        SendOutcome.REFUSED.value,
    }


def _campaign_budget_cycle_key(
    store: StateStore,
    campaign_id: str,
    now_iso: str,
    *,
    daemon_drain: bool,
    budget_cycle_key: str | None,
) -> str:
    """Use the outer daemon key for automatic work; allocate one for each manual batch."""
    if daemon_drain:
        if budget_cycle_key is None or not budget_cycle_key.strip():
            raise CampaignError("campaign daemon drain requires the outer daemon cycle key")
        return budget_cycle_key
    # F9: every manual invocation gets its own fresh durable budget key.
    return store.begin_cycle(f"campaign-{_short(campaign_id)}", now_iso)


def apply_campaign(
    deps: DaemonDeps,
    settings: Settings,
    store: StateStore,
    plan_path: str | Path,
    now_iso: str,
    *,
    batch_size: int | None = None,
    lock_held: bool = False,
    daemon_drain: bool = False,
    budget_cycle_key: str | None = None,
    budget_start_mono: float | None = None,
    fresh_campaign_quarantines: frozenset[int] = frozenset(),
) -> CampaignApplyReport:
    """One bounded batch of the approved campaign through the full guarded path.

    Order: **apply lock first** (F1/P2-1: the single-process daemon lock, acquired under
    the same lease rule as ``daemon._lock_preflight`` **before any guard mutation**; a live
    foreign lease or write-disabled store **defers the whole batch** — zero POSTs, the
    campaign stays approved, the report reason is ``apply_lock_contended:<holder>`` /
    ``writes_disabled``, no state mutation beyond the report; released in ``finally``) →
    plan file digest == stored approval digest (``plan_drift`` → campaign paused,
    zero POSTs) → campaign status / deadline / max-submissions → write gate → attestation
    (campaign scope; persisted pause transition) → sweep (the same daemon DRY_RUN
    machinery) → eligibility (churn/continuity/budget/mapping conjunction) → approved plan
    digests == active
    generation → ≤ ``batch_size`` candidates (capped by ``campaign.max_batch_size``):
    binding/drift quarantine checks, preflight, ``prepare_send`` → ``mark_sent`` →
    ``submit_link`` → record → inline verify.

    ``lock_held=True`` (F7 daemon drain): the caller already owns this process's OS apply
    lock, so the acquire/release pair is skipped (a same-process re-acquire self-contends).
    Every other guard runs unchanged. The CLI ``campaign apply`` path keeps ``lock_held=False``
    and its own lock-first acquire/release. ``daemon_drain=True`` (the daemon's
    :func:`~shoko_linker.daemon._drain_campaign` only) uses the outer daemon cycle's durable
    budget key and reconsults the F10 drain authorization before **every** submission, so a
    mid-batch ``campaign revoke-drain`` stops the running batch. Manual ``campaign apply``
    allocates its own durable budget key and never requires the drain authorization.
    ``budget_start_mono`` inherits an outer cycle start without granting a fresh wall-time
    window; an omitted value gives a standalone invocation its own bounded window.
    """
    plan, digest = read_plan_file(plan_path)
    campaign = _require_campaign(store, plan.campaign_id)
    size = _batch_size(settings, batch_size)
    reasons: list[str] = []
    if lock_held:
        # F7 daemon drain: the caller (the daemon cycle) already holds the OS apply lock
        # for this process, so acquire/release are skipped — a same-process re-acquire
        # would self-contend. The campaign CLI path keeps ``lock_held=False`` and its own
        # lock-first ordering unchanged.
        lock_reason, lock_acquired = None, True
    else:
        lock_reason, lock_acquired = _acquire_apply_lock(deps, now_iso)
    if lock_reason is not None:
        remaining = max(0, len(plan.files) - campaign_send_progress(store, campaign.campaign_id))
        return CampaignApplyReport(
            campaign_id=campaign.campaign_id,
            processed=0,
            submitted=0,
            verified=0,
            quarantined=0,
            deferred=min(size, len(plan.files)),
            progress=0,
            total_sent=campaign.total_sent,
            remaining=remaining,
            reasons=(lock_reason,),
            stopped=None,
        )
    try:
        # P3-2: the batch wall-time budget starts at batch start, BEFORE the DRY_RUN sweep, so
        # the whole cycle (sweep + apply) respects ``max_cycle_seconds`` (daemon ``_apply`` parity).
        local_start_mono = deps.monotonic()
        cycle_start_mono = (
            local_start_mono
            if budget_start_mono is None
            else min(local_start_mono, budget_start_mono)
        )
        _apply_guards(deps, settings, store, campaign, digest, now_iso, reasons)
        if deps.monotonic() >= cycle_start_mono + deps.max_cycle_seconds:
            raise CampaignDeferredError(
                "batch_time_limit",
                ("batch_time_limit:inherited_cycle",),
                "campaign apply deferred: cycle wall-time exhausted; zero POSTs",
            )
        preexisting_quarantines = frozenset(
            record.shoko_file_id
            for record in store.campaign_files(campaign.campaign_id)
            if record.state is FileState.BLOCKED_SAFETY
            and record.state_detail.startswith("campaign_quarantine:")
        )
        sweep = _apply_sweep(
            deps,
            campaign.campaign_id,
            now_iso,
            reasons,
            cycle_start_mono=cycle_start_mono,
            resolution_priority_file_ids=frozenset(entry.shoko_file_id for entry in plan.files),
        )
        active = _apply_mapping(deps, sweep)
        counters = _CampaignCounters(sent_running=campaign.total_sent)
        queue = _campaign_queue_snapshot(
            deps, active, now_iso, deadline=cycle_start_mono + deps.max_cycle_seconds
        )
        batch_cycle_key = _campaign_budget_cycle_key(
            store,
            campaign.campaign_id,
            now_iso,
            daemon_drain=daemon_drain,
            budget_cycle_key=budget_cycle_key,
        )
        context = _ApplyContext(
            deps=deps,
            settings=settings,
            store=store,
            campaign=campaign,
            sweep=sweep,
            active=active,
            resolver=EpisodeIdResolver(
                deps.shoko,
                clock=deps.monotonic,
                deadline=cycle_start_mono + deps.max_cycle_seconds,
            ),
            queue=queue,
            now_iso=now_iso,
            cycle_key=batch_cycle_key,
            preexisting_quarantines=preexisting_quarantines,
            fresh_campaign_quarantines=fresh_campaign_quarantines,
        )
        _apply_campaign_candidates(
            context,
            plan.files,
            size,
            counters,
            reasons,
            cycle_start_mono=cycle_start_mono,
            daemon_drain=daemon_drain,
        )
        # P1/P2: real progress is persisted submissions/verifications/quarantines, never a scan
        # count. ``plan_complete`` reflects persisted campaign progress; the
        # zero-progress breaker (daemon) reads ``progress`` so per-file deferrals and
        # pending-target walks with zero submissions trip it instead of resetting every cycle.
        persisted_progress = campaign_send_progress(store, campaign.campaign_id)
        remaining = max(0, len(plan.files) - persisted_progress)
        progress = counters.submitted + counters.verified + counters.quarantined
        plan_complete = persisted_progress >= len(plan.files)
        if plan_complete:
            # P3: the plan is drained to completion — drop the campaign's gate/zero-progress
            # drain metas (bounded bookkeeping cleanup; semantics of the report unchanged).
            clear_campaign_terminal_meta(store, campaign.campaign_id)
        return CampaignApplyReport(
            campaign_id=campaign.campaign_id,
            processed=counters.processed,
            submitted=counters.submitted,
            verified=counters.verified,
            quarantined=counters.quarantined,
            deferred=counters.deferred,
            progress=progress,
            total_sent=campaign.total_sent + counters.submitted,
            remaining=remaining,
            reasons=tuple(dict.fromkeys(reasons)),
            stopped="plan_complete" if plan_complete else None,
        )
    finally:
        if lock_acquired and not lock_held:
            _release_apply_lock(deps, now_iso)


@dataclass
class _CampaignCounters:
    """One apply invocation's counters (bounded by batch size and max_submissions)."""

    processed: int = 0
    submitted: int = 0
    verified: int = 0
    quarantined: int = 0
    deferred: int = 0
    sent_running: int = 0
    attempts: int = 0


@dataclass(frozen=True)
class _ApplyContext:
    """The read-only inputs one campaign apply batch shares across files."""

    deps: DaemonDeps
    settings: Settings
    store: StateStore
    campaign: CampaignRecord
    sweep: CycleReport
    active: ActiveMappings
    resolver: EpisodeIdResolver
    queue: QueueSnapshot | None
    now_iso: str
    cycle_key: str
    preexisting_quarantines: frozenset[int] = frozenset()
    fresh_campaign_quarantines: frozenset[int] = frozenset()


def _apply_campaign_candidates(
    context: _ApplyContext,
    files: tuple[CampaignFile, ...],
    size: int,
    counters: _CampaignCounters,
    reasons: list[str],
    *,
    cycle_start_mono: float,
    daemon_drain: bool,
) -> None:
    """Process one bounded, rotating scan of the campaign's immutable approved set."""
    deps = context.deps
    store = context.store
    campaign_id = context.campaign.campaign_id
    cursor_key = f"campaign_scan_cursor:v1:{campaign_id}"
    ordered_files = _campaign_scan_order(store, campaign_id, files)
    # F9/F29: the batch guard counts meaningful attempts; every deferred row remains queueable.
    for index, entry in enumerate(ordered_files):
        if counters.attempts >= size:
            _defer_remaining(store, ordered_files[index:], counters, reasons, "batch_limit")
            break
        if deps.monotonic() - cycle_start_mono >= deps.max_cycle_seconds:
            # Never interrupt an in-flight send; defer the remaining queueable files.
            _defer_remaining(store, ordered_files[index:], counters, reasons, "batch_time_limit")
            break
        if deps.shutdown_requested():
            # F11: a shutdown request stops starting submissions; recovery can resume the tail.
            _defer_remaining(store, ordered_files[index:], counters, reasons, "shutdown_requested")
            break
        candidate = _prepare_candidate(context, entry, counters, reasons)
        if candidate is None:
            store.set_meta(cursor_key, str(entry.shoko_file_id))
            continue
        counters.attempts += 1
        tally = _send_one(
            deps,
            context.settings,
            store,
            candidate,
            campaign_id,
            context.sweep.folders,
            context.active,
            context.queue,
            context.cycle_key,
            daemon_drain=daemon_drain,
            source_check_deadline=cycle_start_mono + deps.max_cycle_seconds,
        )
        _fold_tally(counters, tally, reasons)
        if tally.stop_batch or tally.budget_blocked:
            # A stale guard or campaign-wide budget refusal stops the batch without advancing.
            counters.deferred += len(ordered_files) - index - 1
            break
        if not _should_retry_rejected_file_at_scan_head(
            store, campaign_id, entry.shoko_file_id, tally
        ):
            store.set_meta(cursor_key, str(entry.shoko_file_id))
        counters.processed += 1


def _batch_size(settings: Settings, batch_size: int | None) -> int:
    cap = settings.campaign.max_batch_size
    size = cap if batch_size is None else batch_size
    if size > cap:
        raise CampaignError(f"batch size {size} exceeds campaign.max_batch_size ({cap})")
    return size


def _campaign_mapping_resolution_skip(sweep: CycleReport, file_id: int) -> str | None:
    prefix = f"skip:{file_id}:"
    mapping_statuses = (
        "target_ambiguous",
        "target_conflict",
        "target_unmapped",
        "target_no_equivalent",
    )
    for reason in sweep.reasons:
        if reason.startswith(prefix):
            detail = reason.removeprefix(prefix)
            if detail.startswith(mapping_statuses):
                return detail
    return None


def _campaign_mapping_drift_detail(
    entry: CampaignFile, active: ActiveMappings, resolution: str
) -> str:
    approved = sorted(entry.anidb_target_identities)
    return (
        f"mapping_target_drift:resolution={resolution}; approved={approved!r}; "
        f"approved_targets_digest={entry.mapping_targets_digest}; "
        f"current_targets_digest={active.targets_digest}; "
        f"current_structure_digest={active.structure_digest}; current_revision={active.revision}"
    )


def _apply_guards(
    deps: DaemonDeps,
    settings: Settings,
    store: StateStore,
    campaign: CampaignRecord,
    digest: str,
    now_iso: str,
    reasons: list[str],
) -> None:
    """Every pre-POST refusal: status, digest drift, deadline, budget, gate, attestation."""
    if campaign.status == "revoked":
        raise CampaignError(f"campaign {campaign.campaign_id} is revoked: apply is refused")
    if campaign.digest != digest:
        _pause_campaign(store, campaign.campaign_id, "plan_drift", now_iso)
        raise CampaignError(
            f"plan file digest {digest} no longer matches the approved digest "
            f"{campaign.digest}: the campaign is paused (plan_drift), zero POSTs — "
            "re-prepare and re-approve the changed plan"
        )
    if campaign.status == "paused":
        raise CampaignError(
            f"campaign {campaign.campaign_id} is paused ({campaign.status_reason}): apply is "
            "refused until the operator resumes it"
        )
    _require_campaign_window(store, campaign, now_iso)
    if not apply_gate(settings):
        raise CampaignError(
            "apply gate closed: campaign apply requires the write gate (sync.apply_changes "
            "and the operator opt-in); nothing was sent"
        )
    reasons.extend(
        attestation_gate(
            deps,
            now_iso,
            mutate=True,
            scope=f"campaign:{campaign.campaign_id}",
            campaign_id=campaign.campaign_id,
        )
    )
    if not deps.attestation_valid():
        verdict = deps.attestation_verdict()
        raise CampaignError(
            "safety attestation is not valid for campaign apply "
            f"({verdict.status if verdict is not None else 'absent'}): writes are suppressed"
        )


def _require_campaign_window(store: StateStore, campaign: CampaignRecord, now_iso: str) -> None:
    """Deadline and max-submissions refusals (both auto-pause, F7 review P1)."""
    now = _parse_iso(now_iso)
    if campaign.deadline_iso is not None and now > _parse_iso(campaign.deadline_iso):
        _pause_campaign(store, campaign.campaign_id, "deadline_exceeded", now_iso)
        raise CampaignError(
            f"campaign deadline {campaign.deadline_iso} passed at {now.isoformat()}: "
            "the campaign is auto-paused, apply is refused"
        )
    if campaign.total_sent >= campaign.max_submissions:
        _pause_campaign(store, campaign.campaign_id, "max_submissions_reached", now_iso)
        raise CampaignError(
            f"campaign {campaign.campaign_id} reached max_submissions "
            f"({campaign.max_submissions}): auto-paused, apply is refused"
        )


def _apply_sweep(
    deps: DaemonDeps,
    campaign_id: str,
    now_iso: str,
    reasons: list[str],
    *,
    campaign_quarantine_limit: int = 0,
    cycle_start_mono: float,
    resolution_priority_file_ids: frozenset[int],
) -> CycleReport:
    """The dry-run reconciliation sweep whose frozen plans feed the bounded apply loop.

    Campaign source-identity quarantines are deferred to that loop so the batch allowance
    accounts for them alongside resolution and send attempts.
    """
    sweep = run_cycle(
        deps,
        now_iso=now_iso,
        mode=CycleMode.DRY_RUN,
        policy=RunPolicy.BACKLOG,
        cycle_key=f"campaign-{_short(campaign_id)}-apply",
        campaign_quarantine_limit=campaign_quarantine_limit,
        bounded_read_only=True,
        budget_start_mono=cycle_start_mono,
        resolution_priority_file_ids=resolution_priority_file_ids,
    )
    reasons.extend(sweep.reasons)
    reasons.extend(f"historical_mapping_drift:{item}" for item in sweep.existing_link_drift)
    if sweep.failed:
        raise CampaignError(
            "campaign apply: the reconciliation sweep failed (transport); zero POSTs"
        )
    if not sweep.reconciliation_ready:
        reason_codes = tuple(
            reason
            for reason in sweep.reasons
            if reason.startswith(
                (
                    "sonarr_inventory_incomplete:",
                    "shoko_snapshot_incomplete:",
                    "mapping_unavailable",
                )
            )
        )
        if not reason_codes:
            reason_codes = ("reconciliation_not_ready",)
        reasons.append("campaign_deferred:reconciliation_incomplete")
        raise CampaignReconciliationIncompleteError(reason_codes)
    eligibility = compute_eligibility(deps, now_iso, campaign=True)
    if not eligibility.eligible:
        _pause_for_eligibility(deps.store, campaign_id, eligibility.reasons, now_iso, reasons)
        raise CampaignEligibilityRefusalError(eligibility.reasons)
    return sweep


def _apply_mapping(
    deps: DaemonDeps,
    sweep: CycleReport,
) -> ActiveMappings:
    """Use the sweep's generation and fail non-destructively if it changed afterward."""
    swept = sweep.mapping_snapshot
    if swept is None:
        raise CampaignError("campaign apply: reconciliation sweep omitted its mapping snapshot")
    current = load_active(deps.mapping_store, _load_overrides(deps))
    if current is None:
        raise CampaignError("campaign apply: the active mapping generation vanished; zero POSTs")
    current_generation = (
        current.revision,
        current.content_sha256,
        current.targets_digest,
        current.structure_digest,
        current.overrides_applied,
    )
    if current_generation != swept:
        raise CampaignMappingGenerationChangedError
    return current


def _ensure_campaign_mapping_generation(
    deps: DaemonDeps,
    active: ActiveMappings,
    now_iso: str,
    *,
    generation_lock_held: bool = False,
) -> None:
    """Do not make drift/quarantine decisions from a sweep stale before candidate processing."""
    stale = (
        mapping_generation_stale(deps, active, now_iso, generation_lock_held=True)
        if generation_lock_held
        else mapping_generation_stale(deps, active, now_iso)
    )
    if stale:
        raise CampaignMappingGenerationChangedError()


def _campaign_quarantine_with_generation_guard(
    context: _ApplyContext,
    file_id: int,
    reason: str,
    counters: _CampaignCounters,
    reasons: list[str],
) -> None:
    """Fence mapping-derived quarantine against activation until its state write commits."""
    try:
        with context.deps.mapping_store.generation_lock() as acquired:
            if not acquired:
                raise CampaignDeferredError(
                    "mapping_generation_busy",
                    (),
                    "campaign apply deferred: mapping generation is being activated; "
                    "retry with a fresh sweep, zero POSTs",
                )
            _ensure_campaign_mapping_generation(
                context.deps,
                context.active,
                context.now_iso,
                generation_lock_held=True,
            )
            _campaign_quarantine(context.store, file_id, reason, context.now_iso, counters, reasons)
    except ApplyLockUnavailableError as exc:
        raise CampaignDeferredError(
            "mapping_generation_lock_unavailable",
            (),
            "campaign apply deferred: mapping generation lock is unavailable; zero POSTs",
        ) from exc


def _campaign_queue_snapshot(
    deps: DaemonDeps, active: ActiveMappings, now_iso: str, *, deadline: float | None = None
) -> QueueSnapshot | None:
    """Read the queue, then ensure its network interval did not cross a mapping swap."""
    queue = fetch_queue(deps.shoko, deadline=deadline, clock=deps.monotonic)
    _ensure_campaign_mapping_generation(deps, active, now_iso)
    return queue


def _entry_gate(
    context: _ApplyContext,
    entry: CampaignFile,
    counters: _CampaignCounters,
    reasons: list[str],
) -> Any | None:
    """The per-file budget/binding/queue-state gate; a bound queueable row or ``None``."""
    store, campaign, file_id = context.store, context.campaign, entry.shoko_file_id
    if counters.sent_running >= campaign.max_submissions:
        counters.deferred += 1
        reasons.append(f"max_submissions_reached:{file_id}")
        return None
    record = store.get_file(file_id)
    if record is None or record.campaign_id != campaign.campaign_id:
        counters.deferred += 1
        reasons.append(f"file_missing:{file_id}" if record is None else f"unbound:{file_id}")
        return None
    if record.state is FileState.AMBIGUOUS:
        mapping_skip = _campaign_mapping_resolution_skip(context.sweep, file_id)
        source_exclusion = record.state_detail.startswith("source_exclusion:")
        if mapping_skip is not None:
            _ensure_campaign_mapping_generation(context.deps, context.active, context.now_iso)
            detail = _campaign_mapping_drift_detail(entry, context.active, mapping_skip)
            _campaign_quarantine_with_generation_guard(context, file_id, detail, counters, reasons)
        elif source_exclusion:
            _campaign_quarantine(
                store, file_id, record.state_detail, context.now_iso, counters, reasons
            )
        if mapping_skip is not None or source_exclusion:
            return None
    if record.state is FileState.BLOCKED_SAFETY and record.state_detail.startswith(
        "campaign_quarantine:"
    ):
        counters.processed += 1
        detail = record.state_detail.removeprefix("campaign_quarantine:")
        has_pending_progress = CAMPAIGN_QUARANTINE_PROGRESS_PENDING_MARKER in detail
        newly_reported = has_pending_progress and store.claim_campaign_quarantine_progress(
            file_id, campaign.campaign_id
        )
        if newly_reported:
            counters.attempts += 1
            counters.quarantined += 1
            detail = detail.replace(CAMPAIGN_QUARANTINE_PROGRESS_PENDING_MARKER, "")
            detail = detail.replace(CAMPAIGN_QUARANTINE_PROGRESS_RECORDED_MARKER, "")
            reasons.append(f"campaign_quarantine:{file_id}:{detail}")
        else:
            counters.deferred += 1
            duplicate_reason = (
                "already_quarantined"
                if file_id in context.preexisting_quarantines
                and file_id not in context.fresh_campaign_quarantines
                else "already_accounted"
            )
            reasons.append(
                f"not_queueable:{file_id}:{record.state.value}:{duplicate_reason}:{detail}"
            )
        return None
    if record.state not in _CAMPAIGN_QUEUE_STATES:
        counters.deferred += 1
        reasons.append(f"not_queueable:{file_id}:{record.state.value}")
        return None
    return record


def _prepare_candidate(
    context: _ApplyContext,
    entry: CampaignFile,
    counters: _CampaignCounters,
    reasons: list[str],
) -> LinkPlan | None:
    """Decide one approved entry: the plan to send, or ``None`` (deferred/quarantined)."""
    store, now_iso, file_id = context.store, context.now_iso, entry.shoko_file_id
    record = _entry_gate(context, entry, counters, reasons)
    if record is None:
        return None
    fingerprints_changed = (
        record.plan_fingerprint != entry.plan_fingerprint
        or record.mapping_targets_digest != entry.mapping_targets_digest
    )
    # prepare_send persists its current-generation request fingerprint before transport. A
    # changed fingerprint therefore marks a durable prior intent whose resulting candidate is
    # still checked against the immutable target set below (not against global mapping digests).
    prior_send = fingerprints_changed and record.request_fingerprint not in (
        None,
        entry.request_fingerprint,
    )
    drift = _candidate_drift(record, entry, prior_send=prior_send)
    if drift is not None:
        _campaign_quarantine(store, file_id, drift, now_iso, counters, reasons)
        return None
    candidate = context.sweep.plans.get(file_id)
    if candidate is None:
        mapping_skip = _campaign_mapping_resolution_skip(context.sweep, file_id)
        identity_drift = context.sweep.campaign_identity_drift_details.get(file_id)
        if mapping_skip is not None:
            _ensure_campaign_mapping_generation(context.deps, context.active, context.now_iso)
        if mapping_skip is not None:
            reason = _campaign_mapping_drift_detail(entry, context.active, mapping_skip)
        elif identity_drift is not None:
            code, detail = identity_drift
            reason = f"{code}: {detail}"
        elif file_id in context.sweep.resolution_deferred_file_ids:
            counters.deferred += 1
            reasons.append(f"resolution_deferred:{file_id}")
            return None
        else:
            reason = "plan_no_longer_frozen"
        if mapping_skip is not None:
            _campaign_quarantine_with_generation_guard(context, file_id, reason, counters, reasons)
        else:
            _campaign_quarantine(store, file_id, reason, now_iso, counters, reasons)
        return None
    return _candidate_plan(context, entry, record, candidate, counters, reasons)


def _candidate_plan(
    context: _ApplyContext,
    entry: CampaignFile,
    record: Any,
    candidate: LinkPlan,
    counters: _CampaignCounters,
    reasons: list[str],
) -> LinkPlan | None:
    """Approved-plan identity rules: source identity and effective mapped target equality."""
    store, now_iso, file_id = context.store, context.now_iso, entry.shoko_file_id
    source_drift = _campaign_candidate_source_drift(entry, record, candidate)
    if source_drift is not None:
        _campaign_quarantine(store, file_id, source_drift, now_iso, counters, reasons)
        return None
    if entry.shoko_episode_ids == ():
        _ensure_campaign_mapping_generation(context.deps, context.active, context.now_iso)
        return _pending_plan(context, entry, record, candidate, counters, reasons)
    if candidate.status is not PlanStatus.LINKABLE:
        _campaign_quarantine_with_generation_guard(
            context, file_id, "plan_not_linkable", counters, reasons
        )
        return None
    approved_targets = set(entry.anidb_target_identities)
    current_targets = {
        (
            target.anidb_anime_id,
            target.anidb_episode_number,
            target.episode_type,
            target.anidb_episode_id,
        )
        for target in candidate.targets
    }
    if current_targets != approved_targets:
        _ensure_campaign_mapping_generation(context.deps, context.active, context.now_iso)
        current_source_rules = sorted({target.source_rule for target in candidate.targets})
        detail = (
            f"mapping_target_drift:approved={sorted(approved_targets)!r}; "
            f"current={sorted(current_targets)!r}; current_source_rules={current_source_rules!r}; "
            f"approved_targets_digest={entry.mapping_targets_digest}; "
            f"current_targets_digest={context.active.targets_digest}; "
            f"current_structure_digest={context.active.structure_digest}; "
            f"current_revision={context.active.revision}"
        )
        _campaign_quarantine_with_generation_guard(context, file_id, detail, counters, reasons)
        return None
    if candidate.episode_ids != entry.shoko_episode_ids:
        _ensure_campaign_mapping_generation(context.deps, context.active, now_iso)
        detail = (
            f"shoko_target_identity_drift:approved_episode_ids={entry.shoko_episode_ids!r}; "
            f"current_episode_ids={candidate.episode_ids!r}; "
            f"current_source_rules={sorted({target.source_rule for target in candidate.targets})!r}"
        )
        _campaign_quarantine_with_generation_guard(context, file_id, detail, counters, reasons)
        return None
    return candidate


def _campaign_candidate_source_drift(
    entry: CampaignFile, record: Any, candidate: LinkPlan
) -> str | None:
    """Keep the approved file/source binding independent of global mapping-generation digests."""
    if canonical_ed2k(candidate.shoko_ed2k) != entry.shoko_ed2k:
        return "ed2k_drift"
    source_identity = getattr(record, "identity", None)
    if (
        candidate.shoko_file_id != entry.shoko_file_id
        or candidate.sonarr_series_id != entry.sonarr_series_id
        or candidate.sonarr_episode_file_id != entry.sonarr_file_id
        or candidate.sonarr_path != entry.path
        or candidate.sonarr_size != entry.size
        or candidate.shoko_size != entry.size
        or source_identity is None
        or canonical_ed2k(source_identity.ed2k) != entry.shoko_ed2k
    ):
        return "file_identity_drift"
    return None


def _pending_plan(
    context: _ApplyContext,
    entry: CampaignFile,
    record: Any,
    candidate: LinkPlan,
    counters: _CampaignCounters,
    reasons: list[str],
) -> LinkPlan | None:
    """An approved entry with no frozen Shoko IDs: resolve now, never expand the set."""
    if candidate.status is PlanStatus.PENDING_TARGETS:
        return _resolve_and_rebuild(context, entry, record, candidate, counters, reasons)
    return _materialized_candidate(context, entry, record, candidate, counters, reasons)


def _resolve_and_rebuild(
    context: _ApplyContext,
    entry: CampaignFile,
    record: Any,
    candidate: LinkPlan,
    counters: _CampaignCounters,
    reasons: list[str],
) -> LinkPlan | None:
    """Resolve a pending-target plan; the exact-identity rule, fail closed (no POST on drift)."""
    store, now_iso, file_id = context.store, context.now_iso, entry.shoko_file_id
    resolved_plan, reason = _resolved_pending_plan(candidate, entry, context.resolver)
    # P3: every pending-target resolution runs the AniDB→Shoko chain GETs before the branch, so
    # count ONE scan attempt here (pending_metadata, quarantine and success paths alike). This
    # bounds the resolve-chain quarantine walks too — an all-pending plan can never walk all N
    # files per batch, and a quarantined resolve no longer slips past the batch cap.
    counters.attempts += 1
    if reason is EpisodeResolutionStatus.PENDING_METADATA:
        counters.processed += 1
        counters.deferred += 1
        reasons.append(f"pending_target:{file_id}")
        return None
    if reason is EpisodeResolutionStatus.TRANSPORT_FAILURE:
        counters.processed += 1
        counters.deferred += 1
        reasons.append(f"resolution_deferred:{file_id}:transport_failure")
        return None
    if reason is not None:
        _ensure_campaign_mapping_generation(context.deps, context.active, now_iso)
        detail = f"target_{reason.value}" if isinstance(reason, EpisodeResolutionStatus) else reason
        if reason == "target_drift":
            current = sorted(
                (
                    target.anidb_anime_id,
                    target.anidb_episode_number,
                    target.episode_type,
                    target.anidb_episode_id,
                    target.source_rule,
                )
                for target in candidate.pending_targets
            )
            detail = (
                f"target_drift:approved_anidb_target_identities="
                f"{sorted(entry.anidb_target_identities)!r}; current_pending_targets={current!r}; "
                f"current_revision={context.active.revision}"
            )
        _campaign_quarantine_with_generation_guard(context, file_id, detail, counters, reasons)
        return None
    assert resolved_plan is not None
    _materialize_planned(store, record, file_id, now_iso)
    return resolved_plan


def _materialized_candidate(
    context: _ApplyContext,
    entry: CampaignFile,
    record: Any,
    candidate: LinkPlan,
    counters: _CampaignCounters,
    reasons: list[str],
) -> LinkPlan | None:
    """Materialized between approval and apply: the exact preapproved set must still match."""
    store, now_iso, file_id = context.store, context.now_iso, entry.shoko_file_id
    observed = {
        (
            target.anidb_anime_id,
            target.anidb_episode_number,
            target.episode_type,
            target.anidb_episode_id,
        )
        for target in candidate.targets
    }
    if observed != set(entry.anidb_target_identities) or candidate.targets == ():
        _ensure_campaign_mapping_generation(context.deps, context.active, now_iso)
        current = sorted(
            (
                target.anidb_anime_id,
                target.anidb_episode_number,
                target.episode_type,
                target.anidb_episode_id,
                target.source_rule,
            )
            for target in candidate.targets
        )
        detail = (
            f"target_drift:approved_anidb_target_identities="
            f"{sorted(entry.anidb_target_identities)!r}; current_targets={current!r}; "
            f"approved_shoko_episode_ids={entry.shoko_episode_ids!r}; "
            f"current_shoko_episode_ids={candidate.episode_ids!r}; "
            f"current_revision={context.active.revision}"
        )
        _campaign_quarantine_with_generation_guard(context, file_id, detail, counters, reasons)
        return None
    _materialize_planned(store, record, file_id, now_iso)
    return candidate


def _materialize_planned(store: StateStore, record: Any, file_id: int, iso: str) -> None:
    """An approved pending plan froze in ``mapped``; re-enter the walk via mapped→planned."""
    if record.state is FileState.MAPPED:
        store.advance(
            file_id, FileState.PLANNED, _clip("campaign pending target resolved"), iso=iso
        )


def _campaign_quarantine(
    store: StateStore,
    file_id: int,
    reason: str,
    iso: str,
    counters: _CampaignCounters,
    reasons: list[str],
) -> None:
    _quarantine(store, file_id, reason, iso)
    counters.attempts += 1
    counters.processed += 1
    counters.quarantined += 1
    reasons.append(f"campaign_quarantine:{file_id}:{reason}")


def campaign_send_progress(store: StateStore, campaign_id: str) -> int:
    """Distinct approved files with any send progress, per the campaign progress API.

    M7 review P3-2: the batch's own submits are **not** added on top of the persisted
    progress rows — the rows include them, so a same-batch verified file was counted
    twice and ``remaining`` under-reported (reaching 0 while files were untouched).
    """
    progress = store.campaign_progress(campaign_id)
    return sum(progress[state.value] for state in _SEND_PROGRESS_STATES)


def _pause_for_eligibility(
    store: StateStore,
    campaign_id: str,
    eligibility_reasons: tuple[str, ...],
    iso: str,
    reasons: list[str],
) -> None:
    mapping = {"mapping_unstable": "mapping_drift", "source_unstable": "source_discontinuity"}
    if store.writes_disabled:
        mapping["writes_disabled"] = "writes_disabled"
    paused = {mapping[name] for name in eligibility_reasons if name in mapping}
    for reason in sorted(paused):
        _pause_campaign(store, campaign_id, reason, iso, reasons=reasons)


def _pause_campaign(
    store: StateStore, campaign_id: str, reason: str, iso: str, *, reasons: list[str] | None = None
) -> None:
    with contextlib.suppress(WriteDisabledError, StateError):
        store.set_campaign_status(campaign_id, "paused", reason, iso=iso)
    if reasons is not None:
        reasons.append(f"campaign_paused:{campaign_id}:{reason}")
    logger.warning("campaign %s paused: %s", campaign_id, reason)


def _candidate_drift(record: Any, entry: CampaignFile, *, prior_send: bool = False) -> str | None:
    # prepare_send persists the live generation's plan/request fingerprints on the row. A
    # rejected/refused send can return to the campaign queue after an unrelated mapping update;
    # in that case candidate-level target validation below is the authorization check, while
    # the immutable campaign binding remains in the plan digest and campaign record.
    if record.plan_fingerprint != entry.plan_fingerprint and not prior_send:
        return "binding_fingerprint"
    if record.sonarr_path != entry.path:
        return "path_drift"
    if record.sonarr_size != entry.size:
        return "size_drift"
    stored = getattr(record, "mapping_targets_digest", None)
    if stored is not None and stored != entry.mapping_targets_digest and not prior_send:
        return "targets_digest_drift"
    return None


def _quarantine(store: StateStore, file_id: int, reason: str, iso: str) -> None:
    """Campaign drift quarantine: the approved row goes to ``blocked_safety`` (or
    ``waiting``) with a ``campaign_quarantine`` reason — never silently replaced."""
    with contextlib.suppress(TransitionError, WriteDisabledError, StateError):
        store.advance(
            file_id, FileState.BLOCKED_SAFETY, _clip(f"campaign_quarantine:{reason}"), iso=iso
        )
        return
    with contextlib.suppress(TransitionError, WriteDisabledError, StateError):
        store.advance(file_id, FileState.WAITING, _clip(f"campaign_quarantine:{reason}"), iso=iso)


def _resolved_pending_plan(
    plan: LinkPlan,
    entry: CampaignFile,
    resolver: EpisodeIdResolver,
) -> tuple[LinkPlan, None] | tuple[None, str | EpisodeResolutionStatus]:
    """Resolve a pending-target plan; the exact-identity rule, fail closed.

    The preapproved set is the plan file's frozen ``(anime, episode_number, type,
    anidb_episode_id)`` identities. Returns the rebuilt LINKABLE plan, or a reason:
    ``PENDING_METADATA`` (Shoko metadata not materialized yet — defer, the file stays
    approved), ``TRANSPORT_FAILURE`` (retryable service failure — defer), or
    ``target_drift`` (resolved identities do not equal the preapproved set exactly —
    quarantine, zero POSTs).
    """
    approved = set(entry.anidb_target_identities)
    targets, failure = _resolve_pending_targets(plan, approved, resolver)
    if failure is not None:
        return None, failure
    complete_targets: list[LinkTarget] = []
    shoko_id_by_identity: dict[AnidbIdentity, int] = {}
    for target in (*plan.targets, *targets):
        identity = (
            target.anidb_anime_id,
            target.anidb_episode_number,
            target.episode_type,
            target.anidb_episode_id,
        )
        if identity not in approved:
            return None, "target_drift"
        previous_shoko_id = shoko_id_by_identity.get(identity)
        if previous_shoko_id is not None and previous_shoko_id != target.shoko_episode_id:
            return None, "target_drift"
        shoko_id_by_identity[identity] = target.shoko_episode_id
        # Retain each TVDB source occurrence: preflight validates the mapping at every
        # source position even when multiple positions share one AniDB/Shoko destination.
        complete_targets.append(target)
    if set(shoko_id_by_identity) != approved:
        return None, "target_drift"
    return _freeze_resolved_plan(plan, tuple(complete_targets)), None


def _resolve_pending_targets(
    plan: LinkPlan,
    approved: set[AnidbIdentity],
    resolver: EpisodeIdResolver,
) -> tuple[list[LinkTarget], str | EpisodeResolutionStatus | None]:
    """Resolve each pending target; the first failure names the quarantine/defer reason."""
    tvdb_series = plan.pending_targets[0].tvdb_series
    targets: list[LinkTarget] = []
    for pending in plan.pending_targets:
        identity = (
            pending.anidb_anime_id,
            pending.anidb_episode_number,
            pending.episode_type,
            pending.anidb_episode_id,
        )
        if identity not in approved:
            return [], "target_drift"
        resolved = resolver.resolve(
            pending.anidb_anime_id, pending.episode_type, pending.anidb_episode_number
        )
        if resolved.status in {
            EpisodeResolutionStatus.PENDING_METADATA,
            EpisodeResolutionStatus.TRANSPORT_FAILURE,
        }:
            return [], resolved.status
        if resolved.status is not EpisodeResolutionStatus.RESOLVED:
            return [], f"target_{resolved.status.value}"
        if resolved.shoko_episode_id is None or resolved.anidb_episode_id is None:
            return [], "target_drift"
        if (
            pending.anidb_anime_id,
            pending.anidb_episode_number,
            pending.episode_type,
            resolved.anidb_episode_id,
        ) not in approved:
            return [], "target_drift"
        targets.append(
            LinkTarget(
                tvdb_series=tvdb_series,
                season_number=pending.season_number,
                episode_number=pending.episode_number,
                absolute_episode_number=pending.absolute_episode_number,
                anidb_anime_id=pending.anidb_anime_id,
                anidb_episode_number=pending.anidb_episode_number,
                episode_type=pending.episode_type,
                source_rule=pending.source_rule,
                anidb_episode_id=resolved.anidb_episode_id,
                shoko_episode_id=resolved.shoko_episode_id,
            )
        )
    return targets, None


def _freeze_resolved_plan(plan: LinkPlan, targets: tuple[LinkTarget, ...]) -> LinkPlan:
    """Rebuild a frozen plan with resolved targets (pending-target apply path).

    The approved binding for a pending plan covers the exact preapproved AniDB
    identities; the rebuilt plan's fingerprints are derived from the canonical freeze
    over those identities — they are never independently trusted.
    """
    episode_ids = tuple(sorted({target.shoko_episode_id for target in targets}))
    draft = replace(
        plan,
        targets=tuple(sorted(targets, key=_target_key)),
        pending_targets=(),
        episode_ids=episode_ids,
        status=PlanStatus.LINKABLE,
        fingerprint="",
    )
    return replace(draft, fingerprint=plan_fingerprint(draft))


def _target_key(target: LinkTarget) -> tuple[int, int, int, int, str, str]:
    return (
        target.tvdb_series,
        target.season_number,
        target.episode_number,
        target.anidb_anime_id,
        target.episode_type,
        target.source_rule,
    )


def _daemon_drain_staleness(store: StateStore, campaign_id: str) -> str | None:
    """Cross-cutting review P2 (Finding 2): the **daemon drain path** reconsults the F10 drain
    authorization before every submission, so a mid-batch ``campaign revoke-drain`` (or any
    authorization withdrawal/digest invalidation) stops the running batch at the next POST.
    Manual ``campaign apply`` never consults this guard — it is exempt by construction.
    """
    campaign = store.get_campaign(campaign_id)
    if campaign is None:
        return "campaign_missing"
    selection = campaign_drain_selection(campaign)
    if selection == "authorized":
        return None
    return f"drain_authorization_revoked:{campaign_id}:{selection}"


def _campaign_send_staleness(
    deps: DaemonDeps,
    store: StateStore,
    campaign_id: str,
    active: ActiveMappings,
    send_iso: str,
    *,
    generation_lock_held: bool = False,
) -> str | None:
    """F11: the inputs a batch-start guard could not see, re-evaluated before each POST.

    Returns the refusal reason (the candidate defers non-destructively and the batch stops) or
    ``None`` when the POST stays authorized. The lease and the write gate are already fresh per
    submission inside :func:`~shoko_linker.linking.preflight` (P2-4), and the file/XRef identity
    is re-read by the final file preflight; this covers the clock-bound and store-bound inputs:
    the operator shutdown, the campaign deadline, the campaign status, and the active mapping
    generation identity. The daemon drain adds the F10 authorization per submission via
    :func:`_daemon_drain_staleness`.
    """
    if deps.shutdown_requested():
        return "shutdown_requested"
    campaign = store.get_campaign(campaign_id)
    if campaign is None:
        return "campaign_missing"
    if campaign.deadline_iso is not None and _parse_iso(send_iso) > _parse_iso(
        campaign.deadline_iso
    ):
        _pause_campaign(store, campaign_id, "deadline_exceeded", send_iso)
        return "deadline_exceeded"
    if campaign.status != CAMPAIGN_STATUS_APPROVED:
        return f"campaign_{campaign.status}"
    source_invalid = deps.mapping_store.invalid_source_hold()
    if source_invalid or mapping_generation_stale(
        deps, active, send_iso, generation_lock_held=generation_lock_held
    ):
        return "mapping_source_invalid" if source_invalid else "mapping_generation_changed"
    return None


def _preflight_tally(
    verdict: Any, store: StateStore, plan: LinkPlan, send_iso: str
) -> _SendTally | None:
    """The preflight verdict as a tally: ``None`` when the send may proceed.

    A deferring reason (transient safety/queue) keeps the row queueable (F1/P2-4); a hard
    preflight block quarantines the candidate (never silently replaced).
    """
    if verdict.ok:
        return None
    if preflight_reasons_are_retryable(verdict.reasons):
        reason = verdict.reasons[0].value
        if PreflightReason.TARGET_UNAVAILABLE in verdict.reasons:
            detail = "; ".join(verdict.details) or (
                "Shoko target metadata is unavailable; target identity cannot be confirmed, "
                "retry later; no POST"
            )
            return _SendTally(
                0,
                0,
                0,
                1,
                (f"preflight_deferred:{plan.shoko_file_id}:target_unavailable: {detail}",),
            )
        return _SendTally(0, 0, 0, 1, (f"preflight_deferred:{plan.shoko_file_id}:{reason}",))
    hard_reason = primary_hard_preflight_reason(verdict.reasons)
    reason = hard_reason.value if hard_reason is not None else "preflight_blocked"
    _quarantine(store, plan.shoko_file_id, f"preflight_{reason}", send_iso)
    return _SendTally(
        0,
        0,
        1,
        0,
        (f"campaign_quarantine:{plan.shoko_file_id}:preflight_{reason}",),
    )


def _post_preflight_send_staleness(
    deps: DaemonDeps,
    settings: Settings,
    store: StateStore,
    campaign_id: str,
    active: ActiveMappings,
    plan: LinkPlan,
    *,
    cycle_deadline: float | None,
    daemon_drain: bool = False,
    generation_lock_held: bool = False,
) -> tuple[str, _SendTally | None]:
    """P2-1: the local re-evaluation between preflight's network phase and the intent commit.

    Preflight's target/file GETs and the current Sonarr source check are slow (10s connect / 30s
    read), so a shutdown,
    deadline pass, status change, generation swap, lease expiry, gate close, or daemon-drain
    revocation that lands DURING them must not ride the pre-preflight verdict into
    ``prepare_send``. The clock is re-taken and every check is local (store re-read, disk
    lease, config gate) — no network, no polling.
    Returns the fresh submission timestamp and the deferral tally (``None`` when the POST stays
    authorized).
    """
    send_iso = deps.now()
    stale = _campaign_send_staleness(
        deps,
        store,
        campaign_id,
        active,
        send_iso,
        generation_lock_held=generation_lock_held,
    )
    if stale is None and daemon_drain:
        stale = _daemon_drain_staleness(store, campaign_id)
    if stale is not None:
        return send_iso, _SendTally(
            0,
            0,
            0,
            1,
            (f"send_stale:{plan.shoko_file_id}:{stale}",),
            stop_batch=True,
        )
    if not deps.attestation_valid() or not apply_gate(settings):
        # F8 semantics: a transient lease/gate expiry defers the file (row stays queueable).
        return send_iso, _SendTally(
            0, 0, 0, 1, (f"preflight_deferred:{plan.shoko_file_id}:blocked_safety",)
        )
    if cycle_deadline is not None and deps.monotonic() >= cycle_deadline:
        return send_iso, _SendTally(
            0,
            0,
            0,
            1,
            ("cycle_time_budget_exhausted",),
            stop_batch=True,
        )
    return send_iso, None


def _commit_send(  # noqa: PLR0911
    deps: DaemonDeps,
    store: StateStore,
    plan: LinkPlan,
    campaign_id: str,
    send_iso: str,
    prepared: SendToken | BudgetRefusal,
    *,
    deadline: float | None = None,
) -> tuple[_SendTally, SendResult | None]:
    """Durably commit intent and one POST; verification happens after the mapping lease."""
    if isinstance(prepared, BudgetRefusal):
        # F9-1d: only campaign-wide refusals break the batch. A per-file
        # ``pending_episode_jobs_exhausted`` depends on this file's own episode count, so the
        # healthy later files must still send — defer this file, keep the batch running.
        campaign_wide = prepared.reason in _CAMPAIGN_WIDE_BUDGET_REFUSALS
        return (
            _SendTally(
                0,
                0,
                0,
                1,
                (f"budget_refusal:{plan.shoko_file_id}:{prepared.reason}",),
                budget_blocked=campaign_wide,
            ),
            None,
        )
    token: SendToken = prepared
    # mark_sent atomically debits the campaign attempt ceiling and durably commits
    # request_may_have_been_sent BEFORE any bytes reach HTTP. A concurrent apply may have used
    # the last slot after the earlier guards; close this definitely-not-sent prepared intent and
    # stop without handing it to transport.
    transport_started = False
    try:
        store.mark_sent(token, iso=send_iso)
        with store.protect_transport(token):
            transport_started = True
            send = submit_link(deps.shoko, plan, clock=deps.monotonic, deadline=deadline)
    except CampaignSendRefusedError as exc:
        store.void_prepared_send(
            plan.shoko_file_id,
            token.request_fingerprint,
            iso=send_iso,
            reason=str(exc),
        )
        is_limit = isinstance(exc, CampaignSubmissionLimitError)
        reason = (
            f"max_submissions_reached:{plan.shoko_file_id}"
            if is_limit
            else f"campaign_send_refused:{plan.shoko_file_id}: {exc}"
        )
        return (
            _SendTally(
                0,
                0,
                0,
                1,
                (reason,),
                stop_batch=True,
            ),
            None,
        )
    except WriteDisabledError:
        # A stop may commit after prepare but before mark_sent. Only a still-prepared row is
        # definitely unsent and eligible for cleanup; attempted rows remain verify/hold only.
        with contextlib.suppress(WriteDisabledError, StateError):
            store._void_prepared_send_after_write_stop(
                plan.shoko_file_id,
                token.request_fingerprint,
                iso=send_iso,
                reason="durable write stop fenced transport",
            )
        return (
            _SendTally(
                int(transport_started),
                0,
                0,
                int(not transport_started),
                ("writes_disabled",),
                stop_batch=True,
            ),
            None,
        )
    try:
        store.record_send_outcome(
            token, send.outcome, iso=send_iso, http_status=send.http_status, detail=send.detail
        )
        store.record_campaign_send(campaign_id, send_iso)
    except WriteDisabledError:
        # The POST is already an irrevocably attempted send; retain its durable hold for recovery.
        return (_SendTally(1, 0, 0, 0, ("writes_disabled",), stop_batch=True), None)
    if send.outcome is SendOutcome.AMBIGUOUS_SEND:
        return (
            _SendTally(
                1, 0, 0, 0, (f"ambiguous_send:{plan.shoko_file_id} - verify, never resubmit",)
            ),
            None,
        )
    if send.outcome is not SendOutcome.ACCEPTED_UNVERIFIED:
        return (
            _SendTally(1, 0, 0, 0, (f"send_{send.outcome.value}:{plan.shoko_file_id}",)),
            None,
        )
    return _SendTally(1, 0, 0, 0, ()), send


def _send_one(  # noqa: PLR0911
    deps: DaemonDeps,
    settings: Settings,
    store: StateStore,
    plan: LinkPlan,
    campaign_id: str,
    folders: Mapping[int, ImportFolder],
    active: ActiveMappings,
    queue: QueueSnapshot | None,
    cycle_key: str,
    *,
    daemon_drain: bool = False,
    source_check_deadline: float | None = None,
) -> _SendTally:
    """One approved candidate through the guarded path; zero writes unless preflight passes.

    F11/P2-1: the submission runs on a **fresh** clock — staleness is evaluated before preflight
    (to skip network reads when authorization is already gone) AND re-evaluated locally after
    preflight's network phase, immediately before the intent commit; every submission timestamp
    (prepare intent, attempted anchor, campaign progress) comes from ``deps.now()``, never from
    the batch-start snapshot. ``daemon_drain=True`` (the F7 daemon drain) also reconsults the
    F10 drain authorization per submission; manual apply does not.
    """
    send_iso = deps.now()
    stale = _campaign_send_staleness(deps, store, campaign_id, active, send_iso)
    if stale is None and daemon_drain:
        stale = _daemon_drain_staleness(store, campaign_id)
    if stale is not None:
        return _SendTally(
            0,
            0,
            0,
            1,
            (f"send_stale:{plan.shoko_file_id}:{stale}",),
            stop_batch=True,
        )
    verdict = preflight(
        deps.shoko,
        plan,
        settings,
        folders,
        # P2-4: the campaign lease/gate is re-checked per submission, so a lease that expires
        # mid-batch stops later files (no POST) instead of riding the guard-time verdict.
        attestation_valid=deps.attestation_valid(),
        apply_gate_open=apply_gate(settings),
        queue=queue,
        active_targets_digest=active.targets_digest,
        active_structure_digest=active.structure_digest,
        blocked_over=effective_blocked_over(deps),
        mapping_index=active.index,
        check_file=False,
        check_targets=False,
    )
    blocked = _preflight_tally(verdict, store, plan, send_iso)
    if blocked is not None:
        return blocked
    source_tally = _campaign_source_check_tally(deps, store, plan, deadline=source_check_deadline)
    if source_tally is not None:
        return source_tally
    target_gate = preflight_link_jobs_and_targets(
        deps.shoko, plan, deadline=source_check_deadline, clock=deps.monotonic
    )
    blocked = _preflight_tally(target_gate, store, plan, deps.now())
    if blocked is not None:
        return blocked
    file_verdict = preflight_file(
        deps.shoko,
        plan,
        settings,
        folders,
        deadline=source_check_deadline,
        clock=deps.monotonic,
    )
    send: SendResult | None = None
    tally: _SendTally
    try:
        # Mapping refresh shares this lock when changing the active generation or persisting the
        # invalid-source hold. Keep it from the final local guard through send intent and POST.
        with deps.mapping_store.generation_lock() as acquired:
            if not acquired:
                return _SendTally(
                    0,
                    0,
                    0,
                    1,
                    (f"send_stale:{plan.shoko_file_id}:mapping_generation_busy",),
                    stop_batch=True,
                )
            send_iso, stale_after = _post_preflight_send_staleness(
                deps,
                settings,
                store,
                campaign_id,
                active,
                plan,
                cycle_deadline=source_check_deadline,
                daemon_drain=daemon_drain,
                generation_lock_held=True,
            )
            if stale_after is not None:
                return stale_after
            blocked = _preflight_tally(file_verdict, store, plan, send_iso)
            if blocked is not None:
                return blocked
            prepared = store.prepare_send(
                plan.shoko_file_id,
                plan.fingerprint,
                request_fingerprint(plan),
                targets_digest=plan.targets_digest,
                structure_digest=plan.structure_digest,
                mapping_revision=str(active.revision),
                cycle_key=cycle_key,
                iso=send_iso,
                policy=plan.policy,
                campaign_id=campaign_id,
                expected_episode_jobs=len(plan.episode_ids),
                target_ids=tuple(
                    (target.anidb_anime_id, target.anidb_episode_id, target.shoko_episode_id)
                    for target in plan.targets
                ),
            )
            tally, send = _commit_send(
                deps, store, plan, campaign_id, send_iso, prepared, deadline=source_check_deadline
            )
    except ApplyLockUnavailableError:
        return _SendTally(
            0,
            0,
            0,
            1,
            (f"send_stale:{plan.shoko_file_id}:mapping_generation_lock_unavailable",),
            stop_batch=True,
        )
    except WriteDisabledError:
        return _SendTally(0, 0, 0, 1, ("writes_disabled",), stop_batch=True)
    if send is None:
        return tally
    detail = verify_and_close(
        deps,
        plan,
        plan.shoko_file_id,
        cycle_key,
        send_iso,
        deadline_mono=source_check_deadline,
    )
    return _SendTally(1, int(detail.endswith(":verified")), 0, 0, (detail,))


def _campaign_source_check_tally(
    deps: DaemonDeps,
    store: StateStore,
    plan: LinkPlan,
    *,
    deadline: float | None,
) -> _SendTally | None:
    """Return a defer/quarantine tally when the current Sonarr source has drifted."""
    source = check_plan_sonarr_source(deps, plan, deadline=deadline)
    if source.status is SourceRelationshipStatus.UNAVAILABLE:
        return _SendTally(
            0,
            0,
            0,
            1,
            (f"preflight_deferred:{plan.shoko_file_id}:sonarr_source_unavailable",),
        )
    if source.status is SourceRelationshipStatus.CHANGED:
        _quarantine(store, plan.shoko_file_id, "sonarr_source_changed", deps.now())
        return _SendTally(
            0,
            0,
            1,
            0,
            (
                f"campaign_quarantine:{plan.shoko_file_id}:sonarr_source_changed: "
                f"{source.detail}; frozen target retained, no POST",
            ),
        )
    return None


# ------------------------------------------------------------------ lifecycle


def clear_campaign_terminal_meta(store: StateStore, campaign_id: str) -> None:
    """P3: a campaign at a terminal state (``plan_complete`` / revoked) sheds its drain
    bookkeeping metas — the per-campaign attestation gate marker, the zero-progress
    counter, and the F18 recoverable-deferral marker. Best-effort: cleanup never changes
    campaign semantics and never fails a batch."""
    for key in (
        f"{META_CAMPAIGN_ATTESTATION_GATE}:{campaign_id}",
        f"{META_CAMPAIGN_ZERO_PROGRESS}:{campaign_id}",
        f"{META_CAMPAIGN_DEFERRED_UNTIL}:{campaign_id}",
        f"campaign_scan_cursor:v1:{campaign_id}",
    ):
        with contextlib.suppress(WriteDisabledError, StateError):
            store.clear_meta(key)


def campaign_pause(
    store: StateStore, campaign_id: str, reason: str, now_iso: str
) -> CampaignRecord:
    campaign = _require_campaign(store, campaign_id)
    if campaign.status == "revoked":
        raise CampaignError(f"campaign {campaign_id} is revoked: pause is refused")
    _pause_campaign(store, campaign_id, reason, now_iso)
    updated = store.get_campaign(campaign_id)
    assert updated is not None
    return updated


def campaign_resume(
    store: StateStore,
    campaign_id: str,
    now_iso: str,
    *,
    attestation: AttestationVerdict | None,
    gate_open: bool,
) -> CampaignRecord:
    """Re-check the attestation lease and the gate, then clear the campaign pause."""
    campaign = _require_campaign(store, campaign_id)
    if campaign.status == "revoked":
        raise CampaignError(f"campaign {campaign_id} is revoked: resume is refused")
    unresolved = store.unresolved_campaign_send_history()
    if unresolved:
        send_ids = ", ".join(str(row.send_id) for row in unresolved)
        raise CampaignError(
            "campaign resume refused: legacy campaign send history is ambiguous for send(s) "
            f"{send_ids}; resolve each with campaign history-resolve before resuming"
        )
    if attestation is None or not attestation.valid_for_apply:
        raise CampaignError(
            f"campaign {campaign_id} resume refused: the safety attestation is not valid "
            f"({attestation.status if attestation is not None else 'absent'})"
        )
    if not gate_open:
        raise CampaignError(f"campaign {campaign_id} resume refused: the apply gate is closed")
    store.set_campaign_status(campaign_id, "approved", "resumed", iso=now_iso)
    # F8 (P2-3): resume with a valid lease re-arms the campaign gate, so the *next* lease
    # expiry is recorded as a fresh valid→invalid transition (not skipped as "already
    # paused") and re-pauses the campaign — no head-of-line approved campaign with a dead lease.
    store.set_meta(f"{META_CAMPAIGN_ATTESTATION_GATE}:{campaign_id}", "valid")
    updated = store.get_campaign(campaign_id)
    assert updated is not None
    return updated


def campaign_revoke(store: StateStore, campaign_id: str, now_iso: str) -> int:
    """Revoke: queued bound rows go to ``waiting`` with the reason; future apply refuses."""
    _require_campaign(store, campaign_id)
    store.set_campaign_status(campaign_id, "revoked", "operator_revoked", iso=now_iso)
    unbound = store.unbind_campaign_files(campaign_id, "campaign_revoked", iso=now_iso)
    # P3: a revoked campaign is terminal — drop its gate/zero-progress drain metas.
    clear_campaign_terminal_meta(store, campaign_id)
    logger.warning(
        "campaign %s revoked: %d queued row(s) returned to waiting", campaign_id, unbound
    )
    return unbound


# ----------------------------------------------------------- drain authorization (F10 + F19)

#: The strict rule :func:`authorize_campaign_drain` enforces — documented once, reused by the
#: CLI output and the selection/report surfaces.
DRAIN_AUTHORIZATION_RULE: Final[str] = (
    "drain authorization requires the campaign APPROVED and its canary SETTLED "
    "against the DURABLE SEND LEDGER: every send ever attempted for the campaign must be "
    "terminal VERIFIED. A rejected/refused send (its file back in waiting), an in-flight "
    "request_may_have_been_sent/verifying send, and an indeterminate_write/partial/conflict "
    "outcome are unaccounted media-side-effect risk and REFUSE the grant, visibly listed by "
    "file id + outcome + state. The grant binds the campaign's CURRENT approval digest, "
    "persists across restarts, and is invalidated by a digest/plan-fingerprint change or "
    "an operator revoke. Manual 'campaign apply' never requires it — only the automatic "
    "daemon drain does."
)


def campaign_drain_selection(campaign: CampaignRecord) -> str:
    """Why :func:`select_next_daemon_campaign` does (not) drain this campaign (F10).

    ``authorized`` drains; ``drain_not_authorized`` / ``drain_revoked`` / ``drain_digest_stale``
    / ``status_<status>`` skip — the reason is surfaced in ``campaign report``.
    """
    if campaign.status != CAMPAIGN_STATUS_APPROVED:
        return f"status_{campaign.status}"
    if campaign.drain_authorized_iso is None:
        if campaign.drain_invalidated_reason == CAMPAIGN_DRAIN_REVOKED_REASON:
            return "drain_revoked"
        return "drain_not_authorized"
    if campaign.drain_authorized_digest != campaign.digest:
        return "drain_digest_stale"
    return "authorized"


def campaign_deferred_until_iso(store: StateStore, campaign_id: str, now_iso: str) -> str | None:
    """F18: the campaign's ACTIVE recoverable drain deferral (durable meta), else ``None``.

    A deferral marks a recoverable-wait retry time — a rolling-budget window expiry, a
    bounded stall cooldown, or a pending-metadata cooldown: the campaign stays APPROVED and
    drain-authorized, is skipped for automatic drain until the persisted retry ISO passes, and
    resumes automatically afterwards. An unparseable marker is treated as no deferral
    (fail-open for scheduling only; budgets stay authoritative inside
    :func:`apply_campaign`)."""
    raw = store.get_meta(f"{META_CAMPAIGN_DEFERRED_UNTIL}:{campaign_id}")
    if raw is None:
        return None
    try:
        expiry = _parse_iso(raw)
    except ValueError:
        return None
    return expiry.isoformat() if expiry > _parse_iso(now_iso) else None


def campaign_deferral_reasons(store: StateStore, now_iso: str) -> tuple[str, ...]:
    """F18: ``campaign_deferred:<id>:recoverable_cooldown:until=<iso>`` for every
    drain-eligible campaign currently skipped by its recoverable deferral (surfaced in cycle
    reasons so the deferral and its retry time are visible). The durable meta stores only the
    retry ISO, so the skip surface uses the family-neutral ``recoverable_cooldown`` label; the
    write-time surface (``daemon._defer_campaign_drain``) names the exact family."""
    reasons: list[str] = []
    for campaign in store.list_campaigns():
        if campaign_drain_selection(campaign) != "authorized":
            continue
        deferred = campaign_deferred_until_iso(store, campaign.campaign_id, now_iso)
        if deferred is not None:
            reasons.append(
                f"campaign_deferred:{campaign.campaign_id}:recoverable_cooldown:until={deferred}"
            )
    return tuple(reasons)


def authorize_campaign_drain(
    store: StateStore, campaign_id: str, now_iso: str, *, note: str
) -> CampaignRecord:
    """F10 + F19: the operator's explicit, digest-bound confirmation that the canary passed.

    Enforces :data:`DRAIN_AUTHORIZATION_RULE` verbatim, computed from the **durable send
    ledger** (``store.campaign_send_history``), never from current file states alone: a
    REJECTED/REFUSED send returns its file to ``waiting`` while the send row stays durable,
    and it is accounted here. Refuses a campaign that is unknown, paused, revoked, has no
    attempted send (no canary), or whose attempted sends are not settled by terminal
    VERIFIED associations — the refusal lists every outstanding send (file id, outcome, file state)
    so the operator sees exactly what is unaccounted. A zero-write documented REJECTED
    request is NOT a verified association; the file must be resubmitted and
    verified through the normal bounded flows before the grant succeeds.
    """
    if not note.strip():
        raise CampaignError(
            "drain authorization refused: the note must record the canary inspection "
            "(physical-media side effects — renames/deletions/MyList writes — reviewed)"
        )
    campaign = _require_campaign(store, campaign_id)
    try:
        attempted_count = store.authorize_campaign_drain(
            campaign_id, campaign.digest, iso=now_iso, expected_campaign=campaign
        )
    except CampaignDrainRefusedError as exc:
        raise CampaignError(str(exc)) from exc
    logger.info(
        "campaign %s drain authorized against digest %s at %s (%d attempted send(s), "
        "settled by VERIFIED associations)",
        campaign_id,
        campaign.digest,
        now_iso,
        attempted_count,
    )
    updated = store.get_campaign(campaign_id)
    assert updated is not None
    return updated


def revoke_campaign_drain(
    store: StateStore, campaign_id: str, now_iso: str, *, note: str
) -> CampaignRecord:
    """F10: withdraw the drain authorization — automatic submissions stop, manual apply stays.

    The campaign's status is untouched (``campaign apply`` remains available for a manual
    batch); the campaign drops out of daemon drain selection with reason ``drain_revoked``
    until the operator re-authorizes after a fresh review.
    """
    if not note.strip():
        raise CampaignError("drain revocation refused: the note must record the reason")
    _require_campaign(store, campaign_id)
    store.revoke_campaign_drain(campaign_id, CAMPAIGN_DRAIN_REVOKED_REASON, iso=now_iso)
    logger.warning("campaign %s drain authorization revoked by the operator", campaign_id)
    updated = store.get_campaign(campaign_id)
    assert updated is not None
    return updated


def campaign_report(
    store: StateStore, campaign_id: str, plan_path: str | Path | None
) -> dict[str, Any]:
    """Progress/digest/pause payload for ``campaign report`` (read-only: no writes, no lock)."""
    campaign = _require_campaign(store, campaign_id)
    progress = store.campaign_progress(campaign_id)
    file_digest: str | None = None
    if plan_path is not None:
        _, file_digest = read_plan_file(plan_path)
    return {
        "campaign_id": campaign.campaign_id,
        "status": campaign.status,
        "status_reason": campaign.status_reason,
        "approval_digest": campaign.digest,
        "plan_file_digest": file_digest,
        "plan_matches_file": None if file_digest is None else file_digest == campaign.digest,
        "max_submissions": campaign.max_submissions,
        "deadline_iso": campaign.deadline_iso,
        "attestation_scope": campaign.attestation_scope,
        "approval_iso": campaign.approval_iso,
        "total_sent": campaign.total_sent,
        "last_batch_iso": campaign.last_batch_iso,
        "approved_files": len(campaign.bindings),
        "progress": progress,
        "remaining_planned": progress[FileState.PLANNED.value],
        "verified": progress[FileState.VERIFIED.value],
        "quarantined": progress["quarantined"],
        "drain_authorized_iso": campaign.drain_authorized_iso,
        "drain_authorized_digest": campaign.drain_authorized_digest,
        "drain_invalidated_reason": campaign.drain_invalidated_reason,
        "drain_selection": campaign_drain_selection(campaign),
        # F18: the recoverable drain deferral (retry ISO) when the campaign is cooling down.
        "deferred_until": store.get_meta(f"{META_CAMPAIGN_DEFERRED_UNTIL}:{campaign_id}"),
        # F19: the durable attempted-send audit — every send ever attempted for this campaign
        # (ids, outcomes, current file states), including files that moved on (e.g. waiting).
        "attempted_sends": [
            {
                "send_id": row.send_id,
                "shoko_file_id": row.shoko_file_id,
                "outcome": row.outcome,
                "verification": row.verification,
                "state": row.file_state.value,
                "attempted_iso": row.attempted_iso,
            }
            for row in store.campaign_send_history(campaign_id)
        ],
        "unresolved_legacy_sends": [
            {
                "send_id": row.send_id,
                "shoko_file_id": row.shoko_file_id,
                "outcome": row.outcome,
                "state": row.file_state.value,
                "attempted_iso": row.attempted_iso,
            }
            for row in store.unresolved_campaign_send_history()
        ],
    }


def campaign_verify(
    deps: DaemonDeps,
    store: StateStore,
    campaign_id: str,
    now_iso: str,
) -> dict[str, Any]:
    """Re-poll verification of campaign sends through the daemon recovery machinery.

    A ``DRY_RUN`` cycle consumes persisted ``verifying``/hold rows (XRef GETs only —
    **zero POSTs** by the mode contract); the campaign progress rollup reports the
    result. Verify is **read-only**, so it takes no apply lock (the lock guards sends).
    """
    _require_campaign(store, campaign_id)
    report = run_cycle(
        deps,
        now_iso=now_iso,
        mode=CycleMode.DRY_RUN,
        policy=RunPolicy.BACKLOG,
        cycle_key=f"campaign-{_short(campaign_id)}-verify",
    )
    progress = store.campaign_progress(campaign_id)
    return {
        "campaign_id": campaign_id,
        "cycle_key": report.cycle_key,
        "recovery_reasons": [reason for reason in report.reasons if reason.startswith("recovery_")],
        "progress": progress,
        "verified": progress[FileState.VERIFIED.value],
        "pending_verification": progress[FileState.VERIFYING.value],
    }


def _require_campaign(store: StateStore, campaign_id: str) -> CampaignRecord:
    campaign = store.get_campaign(campaign_id)
    if campaign is None:
        raise CampaignError(f"campaign {campaign_id} does not exist in the state DB")
    return campaign


def _short(campaign_id: str) -> str:
    """Short stable campaign label for cycle/window keys."""
    return campaign_id[:8]


def select_next_daemon_campaign(
    store: StateStore, now_iso: str | None = None
) -> CampaignRecord | None:
    """The F7 daemon's drain target: the oldest **approved, drain-authorized** campaign.

    Selection is by earliest ``approval_iso`` then ``campaign_id``; ``paused``/``revoked``
    campaigns are skipped (operator controls authoritative). The **drain authorization** (F10)
    is the operator's explicit confirmation of a settled canary (see
    :func:`authorize_campaign_drain`): a campaign drains only when the authorization exists, its
    digest binding still matches the campaign's CURRENT approval digest (a plan fingerprint
    change invalidates it — the campaign is skipped with ``drain_digest_stale``), and the
    operator has not revoked it. Send progress ≥ 1 stays a hard floor (the canary exists), and a
    newly approved campaign WITHOUT the authorization is never drained: the manual canary and
    bounded ``campaign apply`` remain the un-authorized path. Campaigns drop out of selection
    through the existing terminal transitions — normal completion (``max_submissions_reached``
    auto-pause), deadline expiry (``deadline_exceeded`` auto-pause, P1 fix), drift, attestation
    pause — plus the drain's ``no_queueable_candidates`` auto-pause (P2-3 fix), so a deferred
    campaign never blocks the queue head. **F18:** when ``now_iso`` is supplied, a campaign
    inside its recoverable budget-cooldown deferral (:func:`campaign_deferred_until_iso`) is
    skipped for this cycle and stays in selection once the persisted retry time passes — the
    deferral keeps the campaign APPROVED, so a later daemon cycle resumes it automatically
    (and a deferring head-of-line campaign never starves a later drainable one). Returns
    ``None`` when nothing is drainable.
    """
    drained = [
        campaign
        for campaign in store.list_campaigns()
        if campaign.status == CAMPAIGN_STATUS_APPROVED
        and campaign_send_progress(store, campaign.campaign_id) >= 1
        and campaign_drain_selection(campaign) == "authorized"
        and (
            now_iso is None
            or campaign_deferred_until_iso(store, campaign.campaign_id, now_iso) is None
        )
    ]
    if not drained:
        return None
    return min(drained, key=lambda campaign: (campaign.approval_iso, campaign.campaign_id))


def _parse_iso(raw: str) -> datetime:
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _clip(detail: str) -> str:
    return detail[:MAX_DETAIL_CHARS]
