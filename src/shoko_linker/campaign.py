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
import uuid
from collections.abc import Mapping
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
    attestation_gate,
    compute_eligibility,
    effective_blocked_over,
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
    fetch_queue,
    plan_fingerprint,
    preflight,
    request_fingerprint,
    submit_link,
)
from .mapping_store import ActiveMappings, load_active
from .policies import RunPolicy
from .shoko import ImportFolder
from .state import (
    BudgetRefusal,
    CampaignBinding,
    CampaignRecord,
    Cohort,
    FileState,
    SendToken,
    StateError,
    StateStore,
    TransitionError,
    WriteDisabledError,
)

PLAN_SCHEMA: Final[str] = "shoko-linker-campaign-plan/1"
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
_MAPPING_DRIFT_DETAIL: Final[str] = (
    "the active mapping generation no longer matches the approved plan digests: the campaign "
    "is paused (mapping_drift); a new mapping never expands the approved set — prepare a new "
    "campaign for newly mapped files. Zero POSTs"
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
_DEFERRING_PREFLIGHT_REASONS: Final[frozenset[PreflightReason]] = frozenset(
    {
        PreflightReason.QUEUE_UNKNOWN,
        PreflightReason.QUEUE_BLOCKED,
        PreflightReason.TRANSPORT_FAILURE,
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
    for key in ("plan_fingerprint", "request_fingerprint", "mapping_targets_digest"):
        _require_hex64(entry[key], f"{field}: {key}")
    return CampaignFile(
        shoko_file_id=whole("shoko_file_id"),
        sonarr_series_id=whole("sonarr_series_id"),
        sonarr_file_id=whole("sonarr_file_id"),
        path=path,
        size=whole("size"),
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
        plan_fingerprint=plan.fingerprint,
        request_fingerprint=request_fingerprint(plan),
        mapping_targets_digest=mapping_targets_digest,
        anidb_target_identities=tuple(sorted(identities)),
        shoko_episode_ids=plan.episode_ids,
    )


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
    files.
    """
    if limit < 1:
        raise CampaignError("--limit must be at least 1")
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
    created = _parse_iso(now_iso)
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
        deadline_iso=(created + timedelta(hours=deadline_hours)).isoformat(),
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
    """One bounded ``campaign apply`` invocation outcome (every number is persisted truth)."""

    campaign_id: str
    processed: int
    submitted: int
    verified: int
    quarantined: int
    deferred: int
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


def _bind_campaign(store: StateStore, campaign_id: str, iso: str) -> int:
    return store.bind_campaign_files(campaign_id, iso)


def apply_campaign(
    deps: DaemonDeps,
    settings: Settings,
    store: StateStore,
    plan_path: str | Path,
    now_iso: str,
    *,
    batch_size: int | None = None,
) -> CampaignApplyReport:
    """One bounded batch of the approved campaign through the full guarded path.

    Order: plan file digest == stored approval digest (``plan_drift`` → campaign paused,
    zero POSTs) → campaign status / deadline / max-submissions → write gate → attestation
    (campaign scope; persisted pause transition) → sweep (the same daemon DRY_RUN
    machinery) → eligibility (churn/continuity/budget/mapping conjunction) → approved
    plan digests == active generation → ≤ ``batch_size`` candidates (capped by
    ``campaign.max_batch_size``): binding/drift quarantine checks, preflight,
    ``prepare_send`` → ``mark_sent`` → ``submit_link`` → record → inline verify.
    """
    plan, digest = read_plan_file(plan_path)
    campaign = _require_campaign(store, plan.campaign_id)
    size = _batch_size(settings, batch_size)
    reasons: list[str] = []
    _apply_guards(deps, settings, store, campaign, digest, now_iso, reasons)
    sweep = _apply_sweep(deps, campaign.campaign_id, now_iso, reasons)
    active = _apply_mapping(deps, store, plan, campaign.campaign_id, now_iso)
    counters = _CampaignCounters(sent_running=campaign.total_sent)
    context = _ApplyContext(
        deps=deps,
        settings=settings,
        store=store,
        campaign=campaign,
        sweep=sweep,
        active=active,
        resolver=EpisodeIdResolver(deps.shoko, clock=deps.monotonic),
        queue=fetch_queue(deps.shoko),
        now_iso=now_iso,
    )
    for entry in plan.files:
        if counters.processed - counters.quarantined >= size:
            counters.deferred += 1
            reasons.append(f"batch_limit:{entry.shoko_file_id}")
            continue
        candidate = _prepare_candidate(context, entry, counters, reasons)
        if candidate is None:
            continue
        tally = _send_one(
            deps,
            settings,
            store,
            candidate,
            campaign.campaign_id,
            sweep.folders,
            active,
            context.queue,
            now_iso,
        )
        counters.processed += 1
        counters.submitted += tally.submitted
        counters.verified += tally.verified
        counters.quarantined += tally.quarantined
        counters.deferred += tally.deferred
        counters.sent_running += tally.submitted
        reasons.extend(tally.reasons)
    remaining = len(plan.files) - campaign_send_progress(store, campaign.campaign_id)
    return CampaignApplyReport(
        campaign_id=campaign.campaign_id,
        processed=counters.processed,
        submitted=counters.submitted,
        verified=counters.verified,
        quarantined=counters.quarantined,
        deferred=counters.deferred,
        total_sent=campaign.total_sent + counters.submitted,
        remaining=max(0, remaining),
        reasons=tuple(dict.fromkeys(reasons)),
        stopped=None if counters.processed < len(plan.files) else "plan_complete",
    )


@dataclass
class _CampaignCounters:
    """One apply invocation's counters (bounded by batch size and max_submissions)."""

    processed: int = 0
    submitted: int = 0
    verified: int = 0
    quarantined: int = 0
    deferred: int = 0
    sent_running: int = 0


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


def _batch_size(settings: Settings, batch_size: int | None) -> int:
    cap = settings.campaign.max_batch_size
    size = cap if batch_size is None else batch_size
    if size > cap:
        raise CampaignError(f"batch size {size} exceeds campaign.max_batch_size ({cap})")
    return size


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
    reasons.extend(attestation_gate(deps, now_iso, mutate=True))
    if not deps.attestation_valid():
        verdict = deps.attestation_verdict()
        raise CampaignError(
            "safety attestation is not valid for campaign apply "
            f"({verdict.status if verdict is not None else 'absent'}): writes are suppressed"
        )


def _require_campaign_window(store: StateStore, campaign: CampaignRecord, now_iso: str) -> None:
    """Deadline and max-submissions refusals (the over-budget one auto-pauses)."""
    now = _parse_iso(now_iso)
    if campaign.deadline_iso is not None and now > _parse_iso(campaign.deadline_iso):
        raise CampaignError(
            f"campaign deadline {campaign.deadline_iso} passed at {now.isoformat()}: "
            "apply is refused"
        )
    if campaign.total_sent >= campaign.max_submissions:
        _pause_campaign(store, campaign.campaign_id, "max_submissions_reached", now_iso)
        raise CampaignError(
            f"campaign {campaign.campaign_id} reached max_submissions "
            f"({campaign.max_submissions}): auto-paused, apply is refused"
        )


def _apply_sweep(
    deps: DaemonDeps, campaign_id: str, now_iso: str, reasons: list[str]
) -> CycleReport:
    """The dry-run reconciliation sweep (zero POSTs) whose frozen plans are the apply input."""
    sweep = run_cycle(
        deps,
        now_iso=now_iso,
        mode=CycleMode.DRY_RUN,
        policy=RunPolicy.BACKLOG,
        cycle_key=f"campaign-{_short(campaign_id)}-apply",
    )
    reasons.extend(sweep.reasons)
    if sweep.failed:
        raise CampaignError(
            "campaign apply: the reconciliation sweep failed (transport); zero POSTs"
        )
    eligibility = compute_eligibility(deps, now_iso)
    if not eligibility.eligible:
        _pause_for_eligibility(deps.store, campaign_id, eligibility.reasons, now_iso, reasons)
        raise CampaignError(
            f"campaign apply refused — eligibility reasons: {', '.join(eligibility.reasons)} "
            "(campaign status reported; zero POSTs)"
        )
    return sweep


def _apply_mapping(
    deps: DaemonDeps,
    store: StateStore,
    plan: CampaignPlan,
    campaign_id: str,
    now_iso: str,
) -> ActiveMappings:
    """The approved plan digests must still equal the active generation (mapping_drift)."""
    active = load_active(deps.mapping_store, _load_overrides(deps))
    if active is None:
        raise CampaignError("campaign apply: the active mapping generation vanished; zero POSTs")
    if active.targets_digest != plan.targets_digest:
        _pause_campaign(store, campaign_id, "mapping_drift", now_iso)
        raise CampaignError(_MAPPING_DRIFT_DETAIL)
    if active.structure_digest != plan.structure_digest:
        _pause_campaign(store, campaign_id, "mapping_drift", now_iso)
        raise CampaignError(_MAPPING_DRIFT_DETAIL)
    return active


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
    if record is None:
        counters.deferred += 1
        reasons.append(f"file_missing:{file_id}")
        return None
    if record.campaign_id != campaign.campaign_id:
        counters.deferred += 1
        reasons.append(f"unbound:{file_id}")
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
    drift = _candidate_drift(record, entry)
    if drift is not None:
        _campaign_quarantine(store, file_id, drift, now_iso, counters, reasons)
        return None
    candidate = context.sweep.plans.get(file_id)
    if candidate is None:
        _campaign_quarantine(store, file_id, "plan_no_longer_frozen", now_iso, counters, reasons)
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
    """Approved-plan identity rules: pending targets, materialization, fingerprint drift."""
    store, now_iso, file_id = context.store, context.now_iso, entry.shoko_file_id
    if entry.shoko_episode_ids == ():
        return _pending_plan(context, entry, record, candidate, counters, reasons)
    if candidate.status is not PlanStatus.LINKABLE:
        _campaign_quarantine(store, file_id, "plan_not_linkable", now_iso, counters, reasons)
        return None
    if candidate.fingerprint != entry.plan_fingerprint:
        _campaign_quarantine(store, file_id, "fingerprint_drift", now_iso, counters, reasons)
        return None
    return candidate


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
    if reason == "pending_metadata":
        counters.processed += 1
        counters.deferred += 1
        reasons.append(f"pending_target:{file_id}")
        return None
    if reason is not None:
        _campaign_quarantine(store, file_id, reason, now_iso, counters, reasons)
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
        _campaign_quarantine(store, file_id, "target_drift", now_iso, counters, reasons)
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


def _candidate_drift(record: Any, entry: CampaignFile) -> str | None:
    if record.plan_fingerprint != entry.plan_fingerprint:
        return "binding_fingerprint"
    if record.sonarr_path != entry.path:
        return "path_drift"
    if record.sonarr_size != entry.size:
        return "size_drift"
    stored = getattr(record, "mapping_targets_digest", None)
    if stored is not None and stored != entry.mapping_targets_digest:
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
) -> tuple[LinkPlan, None] | tuple[None, str]:
    """Resolve a pending-target plan; the exact-identity rule, fail closed.

    The preapproved set is the plan file's frozen ``(anime, episode_number, type,
    anidb_episode_id)`` identities. Returns the rebuilt LINKABLE plan, or a reason:
    ``pending_metadata`` (Shoko metadata not materialized yet — defer, the file stays
    approved) / ``target_drift`` (resolved identities do not equal the preapproved set
    exactly — quarantine, zero POSTs).
    """
    approved = set(entry.anidb_target_identities)
    targets, failure = _resolve_pending_targets(plan, approved, resolver)
    if failure is not None:
        return None, failure
    resolved_identities = {
        (
            target.anidb_anime_id,
            target.anidb_episode_number,
            target.episode_type,
            target.anidb_episode_id,
        )
        for target in targets
    }
    if resolved_identities != approved or len(targets) != len(approved):
        return None, "target_drift"
    return _freeze_resolved_plan(plan, tuple(targets)), None


def _resolve_pending_targets(
    plan: LinkPlan,
    approved: set[AnidbIdentity],
    resolver: EpisodeIdResolver,
) -> tuple[list[LinkTarget], str | None]:
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
        if resolved.status is EpisodeResolutionStatus.PENDING_METADATA:
            return [], "pending_metadata"
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


def _send_one(
    deps: DaemonDeps,
    settings: Settings,
    store: StateStore,
    plan: LinkPlan,
    campaign_id: str,
    folders: Mapping[int, ImportFolder],
    active: ActiveMappings,
    queue: QueueSnapshot | None,
    now_iso: str,
) -> _SendTally:
    """One approved candidate through the guarded path; zero writes unless preflight passes."""
    verdict = preflight(
        deps.shoko,
        plan,
        settings,
        folders,
        attestation_valid=True,
        apply_gate_open=True,
        queue=queue,
        active_targets_digest=active.targets_digest,
        active_structure_digest=active.structure_digest,
        blocked_over=effective_blocked_over(deps),
        mapping_index=active.index,
    )
    if not verdict.ok:
        reason = verdict.reasons[0].value if verdict.reasons else "preflight_blocked"
        if any(item in _DEFERRING_PREFLIGHT_REASONS for item in verdict.reasons):
            return _SendTally(0, 0, 0, 1, (f"preflight_deferred:{plan.shoko_file_id}:{reason}",))
        _quarantine(store, plan.shoko_file_id, f"preflight_{reason}", now_iso)
        return _SendTally(
            0,
            0,
            1,
            0,
            (f"campaign_quarantine:{plan.shoko_file_id}:preflight_{reason}",),
        )
    prepared = store.prepare_send(
        plan.shoko_file_id,
        plan.fingerprint,
        request_fingerprint(plan),
        targets_digest=plan.targets_digest,
        structure_digest=plan.structure_digest,
        mapping_revision=str(active.revision),
        cycle_key=f"campaign-{_short(campaign_id)}",
        iso=now_iso,
        policy=plan.policy,
        campaign_id=campaign_id,
        expected_episode_jobs=len(plan.episode_ids),
        target_ids=tuple(
            (target.anidb_anime_id, target.anidb_episode_id, target.shoko_episode_id)
            for target in plan.targets
        ),
    )
    if isinstance(prepared, BudgetRefusal):
        return _SendTally(0, 0, 0, 1, (f"budget_refusal:{plan.shoko_file_id}:{prepared.reason}",))
    token: SendToken = prepared
    # mark_sent durably commits request_may_have_been_sent BEFORE any bytes reach HTTP.
    store.mark_sent(token, iso=now_iso)
    send = submit_link(deps.shoko, plan, clock=deps.monotonic)
    store.record_send_outcome(
        token, send.outcome, iso=now_iso, http_status=send.http_status, detail=send.detail
    )
    store.record_campaign_send(campaign_id, now_iso)
    if send.outcome is SendOutcome.AMBIGUOUS_SEND:
        return _SendTally(
            1, 0, 0, 0, (f"ambiguous_send:{plan.shoko_file_id} - verify, never resubmit",)
        )
    if send.outcome is not SendOutcome.ACCEPTED_UNVERIFIED:
        return _SendTally(1, 0, 0, 0, (f"send_{send.outcome.value}:{plan.shoko_file_id}",))
    detail = verify_and_close(
        deps, plan, plan.shoko_file_id, f"campaign-{_short(campaign_id)}", now_iso
    )
    verified = 1 if detail.endswith(":verified") else 0
    return _SendTally(1, verified, 0, 0, (detail,))


# ------------------------------------------------------------------ lifecycle


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
    if attestation is None or not attestation.valid_for_apply:
        raise CampaignError(
            f"campaign {campaign_id} resume refused: the safety attestation is not valid "
            f"({attestation.status if attestation is not None else 'absent'})"
        )
    if not gate_open:
        raise CampaignError(f"campaign {campaign_id} resume refused: the apply gate is closed")
    store.set_campaign_status(campaign_id, "approved", "resumed", iso=now_iso)
    updated = store.get_campaign(campaign_id)
    assert updated is not None
    return updated


def campaign_revoke(store: StateStore, campaign_id: str, now_iso: str) -> int:
    """Revoke: queued bound rows go to ``waiting`` with the reason; future apply refuses."""
    _require_campaign(store, campaign_id)
    store.set_campaign_status(campaign_id, "revoked", "operator_revoked", iso=now_iso)
    unbound = store.unbind_campaign_files(campaign_id, "campaign_revoked", iso=now_iso)
    logger.warning(
        "campaign %s revoked: %d queued row(s) returned to waiting", campaign_id, unbound
    )
    return unbound


def campaign_report(
    store: StateStore, campaign_id: str, plan_path: str | Path | None
) -> dict[str, Any]:
    """Progress/digest/pause payload for ``campaign report``."""
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
    result.
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


def _parse_iso(raw: str) -> datetime:
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _clip(detail: str) -> str:
    return detail[:MAX_DETAIL_CHARS]
