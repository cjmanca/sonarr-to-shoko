"""M6-b: daemon reconciliation loop, cycle runner, and mapping-refresh scheduler.

This module integrates the M2-M6-a modules into a durable polling daemon. All external
I/O goes through the injected :class:`~shoko_linker.http.ApiEndpoint` objects, the
:class:`~shoko_linker.state.StateStore`, and the
:class:`~shoko_linker.mapping_store.MappingStore`; every clock is injected so cycles
are deterministic offline (tests use fake clocks, ``httpx.MockTransport`` endpoints,
and tmp SQLite/MappingStore files). CLI wiring is M7.

Hard safety properties this module enforces (PROJECT.md, docs/compatibility.md §2):

* **Zero Shoko writes in dry-run.** :func:`run_cycle` with ``mode=DRY_RUN`` never
  reaches :func:`shoko_linker.linking.submit_link`; it only GETs and persists
  local state transitions.
* **The apply path executes ONLY through ``linking.submit_link``**, and only after the
  durable ``prepare_send`` → ``mark_sent`` ordering that
  :mod:`shoko_linker.state` documents. ``mark_sent`` (which commits
  ``request_may_have_been_sent``) is durably committed *before* any bytes are handed
  to HTTP; ``submit_link`` is called *after* it.
* **Only the ``new_arrival`` cohort auto-applies in daemon cycles.** Backlog and
  campaign files are never auto-submitted (backlog/campaign runs are M7 CLI
  campaigns). ``classify_new`` decides cohort membership from the recorded baseline.
* **Apply requires the whole eligibility conjunction**: an open apply gate, a valid
  safety attestation, budget headroom, an active mapping generation, source
  continuity, a complete auto-apply-eligible baseline, and no pending rebaseline.
* **A possibly-sent file is never resubmitted** (state transition table); the daemon
  verifies/holds it. Mapping churn and source discontinuity
  pause/suppress writes persistently.
* **Every cycle consumes persisted recovery before processing new candidates** (M6 review
  P1-3): ``verifying`` rows are re-polled against the send row's persisted target triples,
  ``request_may_have_been_sent``/``indeterminate_write`` rows are inspected read-only via
  ``GET File/{id}?include=XRefs`` and closed as verified / contradiction / past-deadline
  partial (with the slot released), ``prepared_not_sent`` rows are voided (P2-2) and
  re-prepared, and persisted ``waiting``/``ambiguous`` rows re-walk the mapping resolve.
  No recovery branch ever issues a POST.
* **The apply lock never wedges** (P2-1): acquisition steals only an expired lease (with a
  persisted audit record), and the cycle body runs under ``try/finally`` so the
  release always runs; a stolen/lost lock is guarded (non-owner release raises, is
  reported, never wedges the next cycle).
* **Budget slots release exactly once** (P1-2/P2-4): the daemon reserves the plan's real
  episode-job count; the terminal-fact APIs (:meth:`~shoko_linker.state.StateStore.
  record_verification` and friends) release the stored reservation; ``mark_completed``
  only records cycle counters.
"""

from __future__ import annotations

import contextlib
import hashlib
import os
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum
from types import MappingProxyType
from typing import Any, Final

import httpx

from . import mappings as _mappings
from .config import Settings, apply_gate
from .errors import HttpPolicyError
from .http import ApiEndpoint, resolve_and_send
from .linking import (
    EpisodeIdResolver,
    EpisodeResolutionStatus,
    EpisodeTargetResolution,
    LinkPlan,
    PlanSkip,
    PlanStatus,
    PreflightReason,
    QueueSnapshot,
    SendOutcome,
    ShokoFileXRefs,
    VerificationVerdict,
    fetch_queue,
    freeze_plan,
    parse_file_xrefs,
    preflight,
    request_fingerprint,
    submit_link,
    verify_link,
)
from .mapping_store import (
    ActiveMappings,
    MappingStore,
    RefreshOutcome,
    load_active,
)
from .mapping_store import (
    due as mapping_due,
)
from .mapping_store import (
    refresh as mapping_refresh,
)
from .mappings import MappingIndex, ResolutionStatus, TargetIdentity, effective_targets
from .mappings import resolve as resolve_mapping
from .policies import RunPolicy
from .shoko import ImportFolder, ShokoDiscovery, collect_shoko_discovery, match_shoko_files
from .sonarr import SonarrFile, SonarrInventory, collect_sonarr_inventory
from .state import (
    BaselineFile,
    BudgetRefusal,
    Cohort,
    EligibilityVerdict,
    FileIdentitySnapshot,
    FileSeed,
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

XREF_INCLUDE_FLAGS: Final[str] = "XRefs"
MIN_SUCCESS_STATUS: Final[int] = 200
MAX_SUCCESS_STATUS: Final[int] = 300

MAX_BACKOFF_SECONDS: Final[float] = 24 * 3600.0
SOURCE_IDENTITY_FIELDS: Final[str] = "sonarr_url,shoko_url"


class CycleMode(Enum):
    """The two ways a cycle runs: read-only observation, or guarded apply."""

    DRY_RUN = "dry_run"
    APPLY = "apply"


def _noop_advance(_seconds: float) -> None:
    return None


def _no_jitter() -> float:
    return 0.5


def _attestation_closed() -> bool:
    return False


@dataclass(frozen=True)
class DaemonDeps:
    """Every injected dependency a cycle needs: endpoints, stores, clocks, safety inputs.

    M7 supplies the real endpoints/attestation lease; tests inject ``MockTransport``
    endpoints, fake clocks, and tmp stores. Defaults are safe-closed (no attestation,
    no jitter, monotonic clock) so a partially wired daemon never writes.
    """

    settings: Settings
    store: StateStore
    mapping_store: MappingStore
    sonarr: ApiEndpoint
    shoko: ApiEndpoint
    mapping_source: ApiEndpoint
    overrides_text: str | None = None
    monotonic: Callable[[], float] = time.monotonic
    advance: Callable[[float], None] = _noop_advance
    jitter: Callable[[], float] = _no_jitter
    pid: int = field(default_factory=os.getpid)
    attestation_valid: Callable[[], bool] = _attestation_closed
    lease_seconds: float = 600.0
    blocked_over: int | None = None
    verification_timeout_seconds: float = 600.0
    poll_interval_seconds: float = 5.0
    max_verify_polls: int = 100
    max_submissions: int = 1
    max_cycle_seconds: float = 300.0
    churn_limit: int = 500
    max_recovery_rows_per_cycle: int = 100


@dataclass(frozen=True)
class CycleCounts:
    """Per-cycle tallies reported at the observability boundary."""

    discovered: int = 0
    matched: int = 0
    mapped: int = 0
    planned: int = 0
    submitted: int = 0
    verified: int = 0
    pending: int = 0
    indeterminate: int = 0
    blocked: int = 0
    backlog: int = 0
    new_arrival: int = 0


@dataclass(frozen=True)
class CycleReport:
    """One cycle's outcome at the three observability levels.

    ``alive`` means the cycle ran; ``reconciliation_ready`` means both inventories
    were complete and an active mapping generation is present;
    ``apply_eligible`` is the composed eligibility verdict.
    """

    mode: CycleMode
    cycle_key: str
    alive: bool
    reconciliation_ready: bool
    apply_eligible: bool
    writes_suppressed: bool
    mapping_outcome: RefreshOutcome | None
    mapping_revision: int
    counts: CycleCounts
    reasons: tuple[str, ...]
    drift: tuple[str, ...]
    existing_link_drift: tuple[str, ...]
    deferred_planned: int
    failed: bool
    live_held_sends: int = 0
    live_held_slots: int = 0


@dataclass(frozen=True)
class DaemonReport:
    """Aggregate of a daemon run: every cycle report and the exit cause."""

    cycles: tuple[CycleReport, ...]
    exit_reason: str
    shutdown_requested: bool


@dataclass(frozen=True)
class _EligibilityFlags:
    attestation: bool
    gate: bool
    budgets_ok: bool
    mapping_stable: bool
    source_stable: bool


def _parse_iso(raw: str) -> datetime:
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _source_identity(settings: Settings) -> str:
    return f"sonarr={settings.sonarr.url};shoko={settings.shoko.url}"


def roots_fingerprint(settings: Settings) -> str:
    """Canonical fingerprint of the root-list/allowlist shape (continuity input).

    Identity is the **configured** shape (docs §4: no invented endpoints): a roots change
    is a source discontinuity, ordinary inventory growth is not.
    """
    lines = [f"sn|{root}" for root in sorted(settings.sonarr.allowed_roots)]
    lines.extend(f"sh|{root}" for root in sorted(settings.shoko.allowed_roots))
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


def inventory_snapshot_hash(inventory: SonarrInventory, discovery: ShokoDiscovery) -> str:
    """Deterministic hash of the Sonarr inventory summary plus Shoko discovery totals.

    Keyed on stable identity fields only (series/file ids, sizes, reported totals,
    Shoko file ids): formatting/ordering changes never move it, a source replacement
    (Sonarr DB restore, Shoko reimport) does.
    """
    lines: list[str] = [
        f"sn|{f.series_id}|{f.episode_file_id}|{f.size}"
        for f in sorted(inventory.files, key=lambda item: (item.series_id, item.episode_file_id))
    ]
    lines.append(f"sh_total|{discovery.total_reported}")
    lines.extend(f"sh_id|{item.id}" for item in sorted(discovery.files, key=lambda f: f.id))
    canonical = "\n".join(sorted(lines))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _churned_series(
    old: Mapping[int, frozenset[TargetIdentity]],
    new: Mapping[int, frozenset[TargetIdentity]],
) -> frozenset[int]:
    series = set(old) | set(new)
    return frozenset(
        item for item in series if old.get(item, frozenset()) != new.get(item, frozenset())
    )


def _identity_rule_lines_by_series(index: MappingIndex) -> Mapping[int, tuple[str, ...]]:
    """Daemon-local per-series identity-rule declarations (P2-3 diff input).

    ``effective_targets``/``targets_digest`` intentionally cover only enumerable rules;
    unbounded identity rules (entry ``episodeoffset``, ``defaulttvdbseason="a"``
    absolute-identity claims, TVDB-0 no-counterpart markers) resolve at query time, so a
    generation that adds one moves NO enumerable digest. This helper reads the parsed
    index structures (no mappings.py semantics change) to diff them per TVDB series.
    """
    per_series: dict[int, list[str]] = {}
    for (tvdb_series, season), claims in index.entry_offsets.items():
        for claim in claims:
            per_series.setdefault(tvdb_series, []).append(
                f"O|{season}|{claim.anidb_id}|{claim.offset}"
            )
    for tvdb_series, absolute_claims in index.absolute.items():
        for absolute_claim in absolute_claims:
            covered = ",".join(str(item) for item in sorted(absolute_claim.covered))
            spans = ",".join(f"{start}-{end}" for start, end in sorted(absolute_claim.intervals))
            per_series.setdefault(tvdb_series, []).append(
                f"A|{absolute_claim.anidb_id}|{covered}|{spans}"
            )
    for (tvdb_series, season), declared in index.no_counterpart.items():
        for anidb_id, episode, episode_type in declared:
            per_series.setdefault(tvdb_series, []).append(
                f"N|{season}|{anidb_id}|{episode}|{episode_type.value}"
            )
    return MappingProxyType({series: tuple(sorted(lines)) for series, lines in per_series.items()})


def _churned_identity_rules(old: MappingIndex, new: MappingIndex) -> frozenset[int]:
    """TVDB series whose identity-rule declarations (offset/a-rule/no-counterpart) changed."""
    old_lines = _identity_rule_lines_by_series(old)
    new_lines = _identity_rule_lines_by_series(new)
    series = set(old_lines) | set(new_lines)
    return frozenset(item for item in series if old_lines.get(item, ()) != new_lines.get(item, ()))


# ------------------------------------------------------------------ mapping scheduler


def run_mapping_check(
    deps: DaemonDeps,
    now_iso: str,
    *,
    force: bool = False,
) -> RefreshOutcome:
    """One scheduled Anime-Lists refresh: run only when due (``force`` overrides).

    Timing/backoff live in the :class:`~shoko_linker.mapping_store.MappingStore`; this
    call is independent of the reconciliation interval and respects
    ``next_due`` across restarts (a new store instance reads the persisted schedule).
    """
    now = _parse_iso(now_iso)
    return mapping_refresh(
        deps.mapping_store,
        deps.mapping_source,
        now=now,
        refresh_hours=deps.settings.mappings.refresh_hours,
        force=force,
        overrides_text=deps.overrides_text,
    )


@dataclass(frozen=True)
class _MappingPhase:
    outcome: RefreshOutcome | None
    old: ActiveMappings | None
    active: ActiveMappings | None
    reactivated: bool
    churned: frozenset[int]
    churn_paused: bool


def _mapping_phase(deps: DaemonDeps, now_iso: str) -> _MappingPhase:
    """Run the scheduler, then classify the generation change (activation/churn)."""
    old = load_active(deps.mapping_store, _load_overrides(deps))
    outcome: RefreshOutcome | None = None
    if mapping_due(deps.mapping_store, _parse_iso(now_iso)):
        outcome = run_mapping_check(deps, now_iso)
    else:
        outcome = RefreshOutcome.NOT_DUE
    active = load_active(deps.mapping_store, _load_overrides(deps))
    reactivated = False
    churned: frozenset[int] = frozenset()
    churn_paused = False
    if outcome is RefreshOutcome.ACTIVATED and old is not None and active is not None:
        # P2-3: churn includes identity-rule changes (offsets, a-rules, no-counterpart
        # markers) — unbounded rules move no enumerable target digest.
        churned = _churned_series(effective_targets(old.index), effective_targets(active.index))
        churned = churned | _churned_identity_rules(old.index, active.index)
        reactivated = True
        # P3-A: the churn guard fires at exactly the limit (``>=``): a churn of exactly
        # ``churn_limit`` series pauses; the brief's boundary is inclusive.
        if len(churned) >= deps.churn_limit and not deps.store.writes_disabled:
            deps.store.pause("mapping_churn", now_iso)
            churn_paused = True
    return _MappingPhase(outcome, old, active, reactivated, churned, churn_paused)


def _load_overrides(deps: DaemonDeps) -> _mappings.Overrides | None:
    if deps.overrides_text is None:
        return None
    return _mappings.load_overrides(deps.overrides_text)


# ------------------------------------------------------------------ eligibility


def _budget_headroom(deps: DaemonDeps) -> bool:
    """Non-mutating peek: is there room for at least one more submission?

    The authoritative reservation happens in ``prepare_send``; this only feeds the
    cycle-level eligibility verdict so a saturated daemon suppresses apply early.
    """
    view = deps.store.budget_view()
    limits = deps.store.budget_limits
    if view.paused_reason is not None:
        return False
    if limits is None:
        return True
    if view.pending_file_requests >= limits.max_pending_files:
        return False
    if view.pending_episode_jobs >= limits.max_pending_episode_jobs:
        return False
    return view.rolling_new_submissions < limits.rolling_new


def _eligibility_flags(deps: DaemonDeps, mapping_present: bool) -> _EligibilityFlags:
    source_stable = deps.store.get_meta("requires_rebaseline") != "1"
    paused = deps.store.budget_view().paused_reason is not None
    return _EligibilityFlags(
        attestation=deps.attestation_valid(),
        gate=apply_gate(deps.settings),
        budgets_ok=_budget_headroom(deps),
        mapping_stable=mapping_present and not paused,
        source_stable=source_stable,
    )


def compute_eligibility(deps: DaemonDeps, now_iso: str) -> EligibilityVerdict:  # noqa: ARG001
    """Compose the cycle-level auto-apply verdict from every safety input.

    Refuses when any of: the apply gate is closed, the attestation is missing, budgets
    are exhausted, no active mapping generation is present, the source is discontinuous,
    or writes are paused. Daemon cycles consult this in APPLY mode.
    """
    active = load_active(deps.mapping_store, _load_overrides(deps))
    flags = _eligibility_flags(deps, active is not None)
    reasons: list[str] = []
    if deps.store.writes_disabled:
        reasons.append("writes_disabled")
    if not flags.gate:
        reasons.append("apply_gate_closed")
    if not flags.attestation:
        reasons.append("attestation_invalid")
    if not flags.budgets_ok:
        reasons.append("budgets_exhausted")
    if active is None:
        reasons.append("mapping_unavailable")
    if not flags.mapping_stable:
        reasons.append("mapping_unstable")
    if not flags.source_stable:
        reasons.append("source_unstable")
    return EligibilityVerdict(not reasons, tuple(reasons))


# ------------------------------------------------------------------ reconcile


@dataclass(frozen=True)
class _ReconcileResult:
    inserted: int
    backlog: int
    new_arrival: int
    pending: int
    plans: Mapping[int, LinkPlan]
    skip_reasons: Mapping[int, str]


def _cohort_counts(rows: Sequence[tuple[int, Cohort]]) -> tuple[int, int]:
    backlog = sum(1 for _, cohort in rows if cohort is Cohort.BACKLOG)
    new_arrival = sum(1 for _, cohort in rows if cohort is Cohort.NEW_ARRIVAL)
    return backlog, new_arrival


def _record_and_match(
    deps: DaemonDeps,
    matches: Sequence[Any],
    now_iso: str,
) -> tuple[dict[int, Cohort], int]:
    known = _tracked_file_ids(deps.store)
    cohorts: dict[int, Cohort] = {}
    seeds: list[FileSeed] = []
    for match in matches:
        record = deps.store.get_file(match.shoko_file_id)
        if record is None:
            cohort = deps.store.classify_new(
                match.shoko_file_id,
                sonarr_series_id=match.sonarr_file.series_id,
                sonarr_file_id=match.sonarr_file.episode_file_id,
                known_shoko_file_ids=known,
            )
            seeds.append(
                FileSeed(
                    shoko_file_id=match.shoko_file_id,
                    sonarr_series_id=match.sonarr_file.series_id,
                    sonarr_file_id=match.sonarr_file.episode_file_id,
                    sonarr_path=match.sonarr_file.path,
                    sonarr_size=match.sonarr_file.size,
                    cohort=cohort,
                    iso=now_iso,
                    identity=_identity_snapshot(match),
                )
            )
            known = known | {match.shoko_file_id}
            cohorts[match.shoko_file_id] = cohort
        else:
            cohorts[match.shoko_file_id] = record.cohort
    # P3b: bulk one-transaction ingest + bulk validated advance for first-scan
    # (7k backlog wall-time target; identical per-edge semantics, illegal edges skipped).
    deps.store.record_files(seeds)
    deps.store.advance_bulk(
        [match.shoko_file_id for match in matches], FileState.MATCHED, "matched", iso=now_iso
    )
    return cohorts, len(seeds)


def _tracked_file_ids(store: StateStore) -> frozenset[int]:
    ids: set[int] = set()
    for cohort in Cohort:
        ids.update(record.shoko_file_id for record in store.files_in_cohort(cohort))
    return frozenset(ids)


def _identity_snapshot(match: Any) -> FileIdentitySnapshot:
    return FileIdentitySnapshot(
        ed2k=match.hashes.ed2k,
        sha1=match.hashes.sha1,
        md5=match.hashes.md5,
        crc32=match.hashes.crc32,
        shoko_paths=tuple(match.shoko_paths),
    )


def _advance(deps: DaemonDeps, file_id: int, target: FileState, detail: str, iso: str) -> None:
    record = deps.store.get_file(file_id)
    if record is None:
        return
    if record.state is target:
        return
    try:
        deps.store.advance(file_id, target, detail, iso=iso)
    except Exception:
        # A state already past this point (e.g. verifying a sent file) is left alone.
        return


def _resolve_shoko_paths(discovery: ShokoDiscovery, file_id: int) -> tuple[str, ...]:
    for item in discovery.files:
        if item.id == file_id:
            return tuple(sorted({loc.absolute_path for loc in item.locations}))
    return ()


def _episode_resolutions(
    resolver: EpisodeIdResolver,
    match: Any,
    tvdb_series: int,
    active: ActiveMappings,
) -> list[EpisodeTargetResolution] | str:
    """Map every episode, then resolve each AniDB target to a Shoko internal ID.

    Returns the resolution list, or a machine-readable skip reason when any episode
    fails to resolve cleanly (fail closed: never a partial plan).
    """
    resolutions: list[EpisodeTargetResolution] = []
    for episode in match.sonarr_file.episodes:
        answer = resolve_mapping(
            active.index,
            tvdb_series,
            episode.season_number,
            episode.episode_number,
            episode.absolute_episode_number,
        )
        if answer.status is not ResolutionStatus.RESOLVED or answer.target is None:
            return f"target_{answer.status.value}"
        target = answer.target
        triple = (target.anidb_id, target.episode_type.value, target.anidb_episode_number)
        resolved = resolver.resolve(*triple)
        pending = resolved.status is EpisodeResolutionStatus.PENDING_METADATA
        if resolved.status is not EpisodeResolutionStatus.RESOLVED and not pending:
            return f"target_{resolved.status.value.lower()}"
        resolutions.append(
            EpisodeTargetResolution(
                episode=episode,
                tvdb_series=tvdb_series,
                status=ResolutionStatus.RESOLVED,
                anidb_anime_id=target.anidb_id,
                anidb_episode_number=target.anidb_episode_number,
                episode_type=target.episode_type.value,
                source_rule=target.source_rule.value,
                anidb_episode_id=resolved.anidb_episode_id,
                shoko_episode_id=resolved.shoko_episode_id,
                pending_metadata=pending,
                detail=resolved.detail,
            )
        )
    return resolutions


def _reconcile(
    deps: DaemonDeps,
    inventory: SonarrInventory,
    discovery: ShokoDiscovery,
    active: ActiveMappings,
    now_iso: str,
) -> _ReconcileResult:
    series_tvdb = {
        item.series.id: item.series.tvdb_id
        for item in inventory.eligible_series
        if item.series.tvdb_id is not None
    }
    match_result = match_shoko_files(
        discovery, inventory.files, inventory.verdict.complete, deps.settings
    )
    cohorts, inserted = _record_and_match(deps, match_result.matches, now_iso)
    resolver = EpisodeIdResolver(deps.shoko, clock=deps.monotonic)
    plans: dict[int, LinkPlan] = {}
    skip_reasons: dict[int, str] = {}
    mapped_ids: list[int] = []
    planned_ids: list[int] = []
    pending = 0
    for match in sorted(match_result.matches, key=lambda m: m.shoko_file_id):
        file_id = match.shoko_file_id
        tvdb_series = series_tvdb.get(match.sonarr_file.series_id)
        if tvdb_series is None:
            skip_reasons[file_id] = "no_tvdb_series"
            _advance(deps, file_id, FileState.AMBIGUOUS, "no tvdb series", now_iso)
            continue
        resolutions = _episode_resolutions(resolver, match, tvdb_series, active)
        if isinstance(resolutions, str):
            skip_reasons[file_id] = resolutions
            _advance(deps, file_id, FileState.AMBIGUOUS, resolutions, now_iso)
            continue
        mapped_ids.append(file_id)
        shoko_paths = _resolve_shoko_paths(discovery, file_id)
        frozen = freeze_plan(
            match,
            resolutions,
            policy=RunPolicy.NEW_ARRIVAL,
            targets_digest=active.targets_digest,
            structure_digest=active.structure_digest,
            shoko_paths=shoko_paths,
            mapping_revision=str(active.revision),
            overrides_active=active.overrides_applied,
        )
        if isinstance(frozen, PlanSkip):
            skip_reasons[file_id] = frozen.reason
            _advance(deps, file_id, FileState.AMBIGUOUS, frozen.reason, now_iso)
            continue
        if frozen.status is PlanStatus.PENDING_TARGETS:
            pending += 1
            plans[file_id] = frozen
            continue
        plans[file_id] = frozen
        planned_ids.append(file_id)
    # P3b: the walk steps run bulk (one transaction each, per-edge validation
    # identical to per-file advance — illegal edges skipped).
    deps.store.advance_bulk(mapped_ids, FileState.MAPPED, "mapped", iso=now_iso)
    deps.store.advance_bulk(planned_ids, FileState.PLANNED, "planned", iso=now_iso)
    backlog, new_arrival = _cohort_counts(
        [(file_id, cohort) for file_id, cohort in cohorts.items()]
    )
    return _ReconcileResult(
        inserted=inserted,
        backlog=backlog,
        new_arrival=new_arrival,
        pending=pending,
        plans=plans,
        skip_reasons=skip_reasons,
    )


# ------------------------------------------------------------------ generation change


def _sonarr_to_tvdb(inventory: SonarrInventory) -> dict[int, int]:
    return {
        item.series.id: item.series.tvdb_id
        for item in inventory.eligible_series
        if item.series.tvdb_id is not None
    }


def _apply_generation_change(
    deps: DaemonDeps,
    now_iso: str,
    affected: frozenset[int],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Re-evaluate affected candidates on an ACTIVATED generation (PLAN M4/M6 semantics).

    ``affected`` is the set of Sonarr series whose effective mapping target set changed.

    * files in ``ambiguous``/``conflict`` in an affected series → ``waiting`` (prompt
      re-evaluation this cycle);
    * ``planned`` (unlinked, in-flight approvals) in an affected series → ``waiting``
      with a ``mapping_changed`` quarantine reason (their frozen digests no longer match);
    * ``verified`` (already-linked) files in an affected series → drift **report only**,
      never unlinked.

    The resolved-ID cache is per-cycle (a new generation is a new cycle), so no
    in-cycle cache invalidation is needed here.
    """
    churned = affected
    drift: list[str] = []
    existing_link: list[str] = []
    for state, target, reason in (
        (FileState.AMBIGUOUS, FileState.WAITING, "mapping_reactivated"),
        (FileState.CONFLICT, FileState.WAITING, "mapping_reactivated"),
        (FileState.PLANNED, FileState.WAITING, "mapping_changed"),
    ):
        for record in deps.store.files_in_state(state):
            if record.sonarr_series_id not in churned:
                continue
            with contextlib.suppress(TransitionError):
                deps.store.advance(record.shoko_file_id, target, reason, iso=now_iso)
                if state is FileState.PLANNED:
                    drift.append(
                        f"mapping_changed: file {record.shoko_file_id} quarantined from planned"
                    )
    for record in deps.store.files_in_state(FileState.VERIFIED):
        if record.sonarr_series_id in churned:
            existing_link.append(
                f"existing_link drift: verified file {record.shoko_file_id} in a "
                "changed mapping series (never unlinked)"
            )
    return tuple(drift), tuple(existing_link)


# ------------------------------------------------------------------ apply loop


def _xref_fetcher(endpoint: ApiEndpoint, file_id: int) -> Callable[[], ShokoFileXRefs | None]:
    """Build the verification poll: ``GET File/{id}?include=XRefs`` (read-only)."""

    def fetch() -> ShokoFileXRefs | None:
        try:
            response = resolve_and_send(
                endpoint, "GET", f"api/v3/File/{file_id}", params={"include": XREF_INCLUDE_FLAGS}
            )
        except (HttpPolicyError, httpx.HTTPError):
            return None
        try:
            if not MIN_SUCCESS_STATUS <= response.status_code < MAX_SUCCESS_STATUS:
                return None
            try:
                payload = response.json()
            except ValueError:
                return None
        finally:
            response.close()
        return parse_file_xrefs(payload)

    return fetch


@dataclass(frozen=True)
class _ApplyOutcome:
    submitted: int
    deferred: int
    reasons: tuple[str, ...]
    drift: tuple[str, ...]


def _verify_and_close(
    deps: DaemonDeps, plan: LinkPlan, file_id: int, cycle_key: str, now_iso: str
) -> str:
    """Poll the XRef set to the exact planned set and close the loop (slot released on VERIFIED)."""
    deadline = deps.monotonic() + deps.verification_timeout_seconds
    verification = verify_link(
        _xref_fetcher(deps.shoko, file_id),
        plan,
        clock=deps.monotonic,
        deadline=deadline,
        poll_interval=deps.poll_interval_seconds,
        advance=deps.advance,
        max_polls=deps.max_verify_polls,
    )
    deps.store.record_verification(file_id, verification, iso=now_iso)
    if verification is VerificationVerdict.VERIFIED:
        deps.store.mark_completed(cycle_key, now_iso)
    return f"verification:{verification.value}"


def inspect_send_state(deps: DaemonDeps, file_id: int) -> ShokoFileXRefs | None:
    """Read-only recovery inspection: ``GET File/{id}?include=XRefs`` (P1-3).

    Never a POST, never a DELETE — the hold branches verify, they do not repair.
    """
    return _xref_fetcher(deps.shoko, file_id)()


def _xref_triples(observed: ShokoFileXRefs) -> frozenset[tuple[int, int, int | None]]:
    """Observed XRef triples; the Shoko internal ID is nullable on the wire (an AniDB
    episode whose Shoko metadata has not materialized) — never guessed (docs §2)."""
    return frozenset(
        (xref.series_anidb_id, episode.anidb_episode_id, episode.shoko_episode_id)
        for xref in observed.xrefs
        for episode in xref.episodes
    )


def _stored_triples_verdict(
    observed: ShokoFileXRefs,
    file_id: int,
    expected: frozenset[tuple[int, int, int]],
    *,
    now: datetime,
    deadline: datetime,
    partial_on_empty_past: bool = False,
) -> VerificationVerdict:
    """M5 verification semantics re-applied to persisted send-row triples (P1-3).

    Exact set → VERIFIED; unexpected/unrelated links → CONTRADICTION (no
    unlink, no resubmit); subset/pending keeps PENDING before the deadline. Past the
    deadline a non-empty subset is PARTIAL; an empty set is CONTRADICTION (leaving the
    file Unrecognized does not prove success) unless ``partial_on_empty_past`` (the
    hold-branch policy: not-linked past the deadline is a PARTIAL close that releases
    the held slot and is reported for the operator).
    """
    if observed.file_id != file_id:
        return VerificationVerdict.CONTRADICTION
    actual = _xref_triples(observed)
    if actual == expected:
        return VerificationVerdict.VERIFIED
    actual_ids = {(anime, anidb) for anime, anidb, _ in actual}
    expected_ids = {(anime, anidb) for anime, anidb, _ in expected}
    wrong_links = {item for item in actual if item[2] is not None and item not in expected}
    if not actual_ids <= expected_ids or wrong_links:
        return VerificationVerdict.CONTRADICTION
    if now < deadline:
        return VerificationVerdict.PENDING
    if not actual:
        return (
            VerificationVerdict.PARTIAL
            if partial_on_empty_past
            else VerificationVerdict.CONTRADICTION
        )
    return VerificationVerdict.PARTIAL


@dataclass(frozen=True)
class _RecoveryRowOutcome:
    reason: str


def _recovery_poll_row(deps: DaemonDeps, row: Any, now_iso: str) -> _RecoveryRowOutcome:
    """(a) one ``verifying``/``partial`` row: re-poll XRefs against the persisted plan targets."""
    file_id = row.shoko_file_id
    inspection = deps.store.active_send_details(file_id)
    if inspection is None or not inspection.target_ids:
        return _RecoveryRowOutcome(f"recovery_poll_skipped:{file_id}")
    expected = frozenset(inspection.target_ids)
    now = _parse_iso(now_iso)
    # P1-B: the verification deadline is anchored to the immutable ``attempted_iso``
    # (set once by mark_sent), NOT ``updated_iso`` — a poll never slides the deadline.
    deadline = _parse_iso(inspection.attempted_iso) + timedelta(
        seconds=deps.verification_timeout_seconds
    )
    observed = inspect_send_state(deps, file_id)
    if observed is None:
        with contextlib.suppress(TransitionError):
            deps.store.record_verification(file_id, VerificationVerdict.UNKNOWN, iso=now_iso)
        return _RecoveryRowOutcome(f"recovery_poll_unknown:{file_id}")
    verdict = _stored_triples_verdict(observed, file_id, expected, now=now, deadline=deadline)
    _advance(deps, file_id, FileState.VERIFYING, "recovery re-poll", now_iso)
    with contextlib.suppress(TransitionError):
        deps.store.record_verification(file_id, verdict, iso=now_iso)
    return _RecoveryRowOutcome(f"recovery_poll:{file_id}:{verdict.value}")


def _recovery_hold_row(deps: DaemonDeps, row: Any, now_iso: str) -> _RecoveryRowOutcome:
    """(b) one ``request_may_have_been_sent``/``indeterminate_write``: inspect — verify/hold,
    never resubmit."""
    file_id = row.shoko_file_id
    inspection = deps.store.active_send_details(file_id)
    if inspection is None:
        return _RecoveryRowOutcome(f"recovery_hold_no_send:{file_id}")
    if not inspection.target_ids:
        # No persisted expected set: keep holding, report for the operator.
        return _RecoveryRowOutcome(f"recovery_hold_targets_missing:{file_id}")
    expected = frozenset(inspection.target_ids)
    now = _parse_iso(now_iso)
    # P1-B: deadline anchored to the immutable ``attempted_iso`` (mark_sent), not ``updated_iso``.
    deadline = _parse_iso(inspection.attempted_iso) + timedelta(
        seconds=deps.verification_timeout_seconds
    )
    observed = inspect_send_state(deps, file_id)
    if observed is None:
        return _RecoveryRowOutcome(f"recovery_hold_unknown:{file_id}")
    verdict = _stored_triples_verdict(
        observed,
        file_id,
        expected,
        now=now,
        deadline=deadline,
        partial_on_empty_past=True,
    )
    _advance(deps, file_id, FileState.VERIFYING, "recovery inspection", now_iso)
    with contextlib.suppress(TransitionError):
        deps.store.record_verification(file_id, verdict, iso=now_iso)
    if verdict is VerificationVerdict.PARTIAL and not _xref_triples(observed):
        # NOT-linked past the deadline: partial close, release the held slot per policy.
        # P1-A: the release is owned by the send row's ``slot_released`` flag — a late
        # ManualLinkJob that later verifies this row VERIFIED is a no-op, never a second release.
        deps.store.release_send_slot(file_id, inspection.request_fingerprint, iso=now_iso)
    return _RecoveryRowOutcome(f"recovery_hold:{file_id}:{verdict.value}")


def _recovery_replan_rows(
    deps: DaemonDeps, rows: Sequence[Any], now_iso: str
) -> list[_RecoveryRowOutcome]:
    """(d) ``prepared_not_sent`` (definitely not sent): void the send, re-prepare in the
    same cycle (P2-2)."""
    outcomes: list[_RecoveryRowOutcome] = []
    for row in rows:
        file_id = row.shoko_file_id
        if row.request_fingerprint is None:
            outcomes.append(_RecoveryRowOutcome(f"recovery_replan_skipped:{file_id}"))
            continue
        try:
            deps.store.void_prepared_send(
                file_id,
                row.request_fingerprint,
                iso=now_iso,
                reason="daemon replan after restart",
            )
        except (StateError, TransitionError):
            outcomes.append(_RecoveryRowOutcome(f"recovery_replan_refused:{file_id}"))
            continue
        # The file is back in ``planned``; this cycle's reconcile re-freezes the plan and
        # the apply loop re-prepares it (budget permitting). No resubmission of the old row.
        outcomes.append(_RecoveryRowOutcome(f"recovery_replan_voided:{file_id}"))
    return outcomes


def _recovery_phase(deps: DaemonDeps, now_iso: str, cycle_start_mono: float) -> tuple[str, ...]:
    """Consume persisted recovery rows BEFORE processing new candidates (P1-3/P2-2).

    (a) continue_polling re-polls with the persisted target triples; (b) hold rows are
    inspected read-only (verify/hold, never resubmit); (c) waiting/ambiguous re_evaluate
    rows are consumed by the cycle's own mapping re-walk (the reconcile pass
    advances persisted waiting/ambiguous rows through the evaluation edges);
    (d) replannable rows are voided (slot released, audit row ``voided``) and re-prepared
    in-cycle by reconcile + apply. Never issues a POST in any recovery branch.

    P2-A: the GET-heavy branches (poll + hold) are bounded by ``max_recovery_rows_per_cycle``
    and the cycle's monotonic budget (``max_cycle_seconds``), oldest-first by the immutable
    ``attempted_iso`` deadline anchor; the remainder is deferred (reported as
    ``recovery_deferred:<n>``). A huge verifying backlog drains across cycles and terminates
    instead of pinning the cycle wall-time and issuing one GET per row.
    """
    plan = deps.store.recover_after_restart()
    outcomes: list[_RecoveryRowOutcome] = []
    queue: list[tuple[Any, str]] = [(row, "poll") for row in plan.continue_polling]
    queue.extend((row, "hold") for row in plan.hold_for_verification)
    queue.sort(key=lambda entry: (_recovery_sort_key(deps, entry[0]), entry[0].shoko_file_id))
    cap = deps.max_recovery_rows_per_cycle
    deferred = 0
    for processed, (row, kind) in enumerate(queue):
        if processed >= cap or deps.monotonic() - cycle_start_mono > deps.max_cycle_seconds:
            deferred = len(queue) - processed
            break
        if kind == "poll":
            outcomes.append(_recovery_poll_row(deps, row, now_iso))
        else:
            outcomes.append(_recovery_hold_row(deps, row, now_iso))
    if deferred:
        outcomes.append(_RecoveryRowOutcome(f"recovery_deferred:{deferred}"))
    # (d) replan rows are DB-only (no GETs) and bounded by the prepared-not-sent count.
    outcomes.extend(_recovery_replan_rows(deps, plan.replannable, now_iso))
    if plan.re_evaluate:
        # Summarized: the reconcile pass in this same cycle re-walks these
        # rows (persisted waiting/ambiguous consumption) — a 7k-row list per cycle is noise.
        outcomes.append(_RecoveryRowOutcome(f"recovery_re_evaluate:{len(plan.re_evaluate)}"))
    return tuple(item.reason for item in outcomes)


def _recovery_sort_key(deps: DaemonDeps, row: Any) -> str:
    """Oldest-first ordering key: the immutable ``attempted_iso`` deadline anchor (P1-B)."""
    inspection = deps.store.active_send_details(row.shoko_file_id)
    if inspection is not None:
        return inspection.attempted_iso
    record = deps.store.get_file(row.shoko_file_id)
    return record.updated_iso if record is not None else ""


def _process_candidate(
    deps: DaemonDeps,
    plan: LinkPlan,
    file_id: int,
    flags: _EligibilityFlags,
    active: ActiveMappings,
    folders: Mapping[int, ImportFolder],
    queue: QueueSnapshot | None,
    cycle_key: str,
    now_iso: str,
) -> _ApplyOutcome:
    """One file's guarded send: eligibility → preflight → prepare → mark_sent → submit → verify."""
    verdict = deps.store.can_auto_apply(
        file_id,
        now=now_iso,
        attestation_valid=flags.attestation,
        gate_open=flags.gate,
        budgets_ok=flags.budgets_ok,
        mapping_stable=flags.mapping_stable,
        source_stable=flags.source_stable,
    )
    if not verdict.eligible:
        return _ApplyOutcome(0, 0, tuple(verdict.reasons), ())
    gate = preflight(
        deps.shoko,
        plan,
        deps.settings,
        folders,
        attestation_valid=flags.attestation,
        apply_gate_open=flags.gate,
        queue=queue,
        active_targets_digest=active.targets_digest,
        active_structure_digest=active.structure_digest,
        blocked_over=deps.blocked_over,
        mapping_index=active.index,
    )
    if not gate.ok:
        reason = gate.reasons[0].value if gate.reasons else "preflight_blocked"
        drift: tuple[str, ...] = ()
        if PreflightReason.MAPPING_CHANGED in gate.reasons:
            _advance(deps, file_id, FileState.WAITING, "mapping_drift", now_iso)
            drift = (f"mapping_changed: file {file_id} in-flight plan drift, no POST",)
        return _ApplyOutcome(0, 0, (f"preflight_{reason}",), drift)
    prepared = deps.store.prepare_send(
        file_id,
        plan.fingerprint,
        request_fingerprint(plan),
        targets_digest=plan.targets_digest,
        structure_digest=plan.structure_digest,
        mapping_revision=str(active.revision),
        cycle_key=cycle_key,
        iso=now_iso,
        policy=plan.policy,
        # P2-4: reserve the plan's real episode-job count; P1-3: persist the expected
        # verification triples so a persisted row survives restarts (re-poll/re-inspect).
        expected_episode_jobs=len(plan.episode_ids),
        target_ids=tuple(
            (target.anidb_anime_id, target.anidb_episode_id, target.shoko_episode_id)
            for target in plan.targets
        ),
    )
    if isinstance(prepared, BudgetRefusal):
        return _ApplyOutcome(0, 1, ("budget_refusal",), ())
    token: SendToken = prepared
    # mark_sent durably commits request_may_have_been_sent BEFORE any bytes reach HTTP.
    deps.store.mark_sent(token, iso=now_iso)
    send = submit_link(deps.shoko, plan, clock=deps.monotonic)
    deps.store.record_send_outcome(
        token, send.outcome, iso=now_iso, http_status=send.http_status, detail=send.detail
    )
    if send.outcome is SendOutcome.AMBIGUOUS_SEND:
        return _ApplyOutcome(
            1, 0, (), (f"ambiguous_send: file {file_id} - verify, never resubmit",)
        )
    if send.outcome is not SendOutcome.ACCEPTED_UNVERIFIED:
        return _ApplyOutcome(1, 0, (), ())
    return _ApplyOutcome(1, 0, (), (_verify_and_close(deps, plan, file_id, cycle_key, now_iso),))


def _apply(
    deps: DaemonDeps,
    plans: Mapping[int, LinkPlan],
    flags: _EligibilityFlags,
    active: ActiveMappings,
    folders: Mapping[int, ImportFolder],
    queue: QueueSnapshot | None,
    cycle_key: str,
    now_iso: str,
    cycle_start_mono: float,
    max_submissions: int,
) -> tuple[int, int, tuple[str, ...], tuple[str, ...]]:
    """The apply path over eligible new-arrival candidates in deterministic order.

    ``max_submissions`` bounds **POSTs attempted**, not enumerated files; the rest stay
    ``planned`` (deferred). Returns ``(submitted, deferred, reasons, drift)``.
    """
    submitted = 0
    deferred = 0
    reasons: list[str] = []
    drift: list[str] = []
    ordered = sorted(plans)
    for index, file_id in enumerate(ordered):
        record = deps.store.get_file(file_id)
        if record is None or record.state is not FileState.PLANNED:
            continue
        if record.cohort is not Cohort.NEW_ARRIVAL:
            continue  # backlog/campaign never auto-submitted by the daemon
        if submitted >= max_submissions:
            deferred += len(ordered) - index
            break
        if deps.monotonic() - cycle_start_mono > deps.max_cycle_seconds:
            deferred += len(ordered) - index
            break
        outcome = _process_candidate(
            deps, plans[file_id], file_id, flags, active, folders, queue, cycle_key, now_iso
        )
        submitted += outcome.submitted
        deferred += outcome.deferred
        reasons.extend(outcome.reasons)
        drift.extend(outcome.drift)
    return submitted, deferred, tuple(reasons), tuple(drift)


# ------------------------------------------------------------------ cycle runner


def _lock_preflight(
    deps: DaemonDeps, mode: CycleMode, now_iso: str
) -> tuple[str | None, bool, bool]:
    """Apply-lock preflight (APPLY only): returns ``(lock_reason, writes_suppressed, acquired)``.

    A contended lock or a writes-disabled store yields a read-only cycle (the caller
    reports the reason); the lock is held for the duration of the apply cycle.
    """
    writes_suppressed = deps.store.writes_disabled
    if mode is not CycleMode.APPLY:
        return (None, writes_suppressed, False)
    if writes_suppressed:
        return ("writes_disabled", True, False)
    lease = _parse_iso(now_iso) + timedelta(seconds=deps.lease_seconds)
    try:
        # P2-1: steals only a lease expired at ``now`` (audit-recorded in meta); a live
        # lease is contended (read-only cycle, holder named).
        acquire_or_steal_apply_lock(
            deps.store, deps.pid, now_iso, lease_expires_iso=lease.isoformat()
        )
    except LockHeldError as exc:
        holder = exc.holder.owner_pid if exc.holder else "?"
        return (f"apply_lock_contended: holder {holder}", False, False)
    except WriteDisabledError:
        return ("writes_disabled", True, False)
    return (None, False, True)


def _establish_baseline(
    deps: DaemonDeps, inventory: SonarrInventory, discovery: ShokoDiscovery, now_iso: str
) -> tuple[str, ...]:
    """Record the first complete baseline (cohort backlog); report incompleteness otherwise."""
    if deps.store.latest_baseline() is not None:
        return ()
    if not (inventory.verdict.complete and discovery.verdict.complete):
        return ("baseline_incomplete",)
    files = [
        _baseline_file(item)
        for item in sorted(inventory.files, key=lambda f: (f.series_id, f.episode_file_id))
    ]
    deps.store.record_baseline(
        files,
        complete=True,
        auto_apply_eligible=inventory.verdict.auto_apply_eligible,
        iso=now_iso,
        series_snapshot_hash=inventory_snapshot_hash(inventory, discovery),
        reason="daemon_first_cycle",
    )
    return ()


def _read_only_report(
    mode: CycleMode, cycle_key: str, reason: str, outcome: RefreshOutcome | None
) -> CycleReport:
    return CycleReport(
        mode=mode,
        cycle_key=cycle_key,
        alive=True,
        reconciliation_ready=False,
        apply_eligible=False,
        writes_suppressed=True,
        mapping_outcome=outcome,
        mapping_revision=0,
        counts=CycleCounts(),
        reasons=(reason,),
        drift=(),
        existing_link_drift=(),
        deferred_planned=0,
        failed=False,
    )


def run_cycle(
    deps: DaemonDeps,
    *,
    now_iso: str,
    mode: CycleMode,
    policy: RunPolicy,  # noqa: ARG001 - mandated cycle input; cohort gating is in can_auto_apply
    cycle_key: str,
) -> CycleReport:
    """Run one reconciliation cycle. See module docstring for the safety contract.

    Order: apply-lock preflight (APPLY only) → mapping scheduler → Sonarr
    inventory → Shoko discovery → baseline establishment → source continuity →
    persisted recovery consumption → reconcile matches → (APPLY only) the ordered
    send/verify loop. ``policy`` names the daemon's cohort policy (only ``NEW_ARRIVAL``
    auto-applies; backlog/campaign campaigns are M7 CLI runs).

    P2-1: the acquired apply lock is released in ``finally`` — a mid-cycle exception can
    no longer wedge it. A non-owner release (the lease was stolen after a crash)
    raises ``LockHeldError``; it is caught in the ``finally`` only, reported, never
    propagated and never wedging the next cycle.
    """
    lock_reason, writes_suppressed, lock_acquired = _lock_preflight(deps, mode, now_iso)
    if writes_suppressed:
        return _read_only_report(mode, cycle_key, "writes_disabled", None)
    if lock_reason is not None:
        return _read_only_report(mode, cycle_key, lock_reason, None)
    try:
        return _run_cycle_body(deps, now_iso=now_iso, mode=mode, cycle_key=cycle_key)
    finally:
        if lock_acquired:
            try:
                release_apply_lock(deps.store, deps.pid, now_iso)
            except WriteDisabledError:
                # Storage stopped mid-cycle: the persisted lease is recovered by the
                # expiry-steal rule once writes resume — never wedge a live cycle.
                pass
            except LockHeldError as exc:
                # The lease was stolen after us (crash past expiry): report-only, never
                # propagated — the next cycle re-acquires/steals through the lease rule.
                holder = exc.holder.owner_pid if exc.holder is not None else "unknown"
                with contextlib.suppress(WriteDisabledError):
                    deps.store.set_meta("last_lock_release_denied", str(holder))


def _run_cycle_body(
    deps: DaemonDeps, *, now_iso: str, mode: CycleMode, cycle_key: str
) -> CycleReport:
    """The cycle body — runs with the apply lock held (APPLY) under the caller's
    ``try/finally`` that guarantees the release (P2-1)."""
    cycle_start_mono = deps.monotonic()
    reasons: list[str] = []
    drift: list[str] = []
    existing_link: list[str] = []

    phase = _mapping_phase(deps, now_iso)
    inventory = collect_sonarr_inventory(deps.sonarr, deps.settings)
    discovery = collect_shoko_discovery(deps.shoko)
    reconciliation_ready = (
        inventory.verdict.complete and discovery.verdict.complete and phase.active is not None
    )
    reasons.extend(_establish_baseline(deps, inventory, discovery, now_iso))
    snapshot_id = inventory_snapshot_hash(inventory, discovery)
    # P1-C: the continuity/anchor inputs are trusted ONLY when the discovery is complete
    # (non-truncated). A truncated page's inflated ``Total`` must never re-anchor the
    # continuity total (it would inflate the anchor and wedge the next honest cycle).
    discovery_complete = inventory.verdict.complete and discovery.verdict.complete
    continuity = deps.store.check_source_continuity(
        _source_identity(deps.settings),
        snapshot_id,
        iso=now_iso,
        roots_fingerprint=roots_fingerprint(deps.settings),
        discovery_total=discovery.total_reported if discovery_complete else None,
        complete=discovery_complete,
    )
    if continuity.requires_rebaseline:
        reasons.extend(continuity.reasons)
    elif not discovery_complete:
        # Incomplete discovery: neutral continuity observation, no anchor, no drop trigger.
        reasons.extend(continuity.reasons)
    elif discovery.total_reported is not None:
        # P1-1: a fully-complete, non-truncated, continuity-OK cycle re-anchors the
        # observation, so ordinary inventory growth never accumulates as discontinuity
        # and a triggered flag clears on the first healthy full-scan cycle.
        deps.store.record_source_observation(
            _source_identity(deps.settings),
            snapshot_id,
            iso=now_iso,
            roots_fingerprint=roots_fingerprint(deps.settings),
            discovery_total=discovery.total_reported,
        )

    active = phase.active
    if active is None:
        reasons.append("mapping_unavailable")
        return _unavailable_report(mode, cycle_key, phase, tuple(reasons), deps)

    if phase.reactivated:
        series_tvdb = _sonarr_to_tvdb(inventory)
        affected = frozenset(
            sonarr_id for sonarr_id, tvdb in series_tvdb.items() if tvdb in phase.churned
        )
        gen_drift, gen_existing = _apply_generation_change(deps, now_iso, affected)
        drift.extend(gen_drift)
        existing_link.extend(gen_existing)
        if phase.churn_paused:
            reasons.append("mapping_churn")

    # P1-3: consume persisted recovery rows BEFORE processing new candidates (the
    # budget ceiling is enforced against the true in-flight count). P2-A: bounded by the
    # per-cycle recovery row cap and the monotonic cycle budget.
    reasons.extend(_recovery_phase(deps, now_iso, cycle_start_mono))

    reconcile = _reconcile(deps, inventory, discovery, active, now_iso)
    for file_id, skip_reason in sorted(reconcile.skip_reasons.items()):
        reasons.append(f"skip:{file_id}:{skip_reason}")

    flags = _eligibility_flags(deps, mapping_present=True)
    eligibility = compute_eligibility(deps, now_iso)
    cycle_eligible = eligibility.eligible
    if not cycle_eligible:
        # P3c: every suppression path surfaces its refusal reasons.
        reasons.extend(eligibility.reasons)
    # P1-C: apply requires a *reconciliation-ready* cycle — an incomplete
    # discovery (reconciliation not ready) suppresses apply that cycle, independent of the
    # continuity flag (the truncated cycle never anchors, so it is not a rebaseline wedge).
    apply_running = (
        mode is CycleMode.APPLY
        and cycle_eligible
        and reconciliation_ready
        and not phase.churn_paused
    )
    submitted, deferred = _run_apply(
        deps,
        reconcile.plans,
        flags,
        active,
        discovery.folders,
        cycle_key,
        now_iso,
        cycle_start_mono,
        apply_running,
        reasons,
        drift,
    )

    counts = _final_counts(
        deps,
        reconcile.inserted,
        reconcile.backlog,
        reconcile.new_arrival,
        submitted,
        reconcile.pending,
    )
    live_held_sends, live_held_slots = deps.store.live_send_counts()

    return CycleReport(
        mode=mode,
        cycle_key=cycle_key,
        alive=True,
        reconciliation_ready=reconciliation_ready,
        apply_eligible=cycle_eligible,
        writes_suppressed=not apply_running,
        mapping_outcome=phase.outcome,
        mapping_revision=active.revision,
        counts=counts,
        reasons=tuple(dict.fromkeys(reasons)),
        drift=tuple(dict.fromkeys(drift)),
        existing_link_drift=tuple(dict.fromkeys(existing_link)),
        deferred_planned=deferred,
        failed=False,
        live_held_sends=live_held_sends,
        live_held_slots=live_held_slots,
    )


def _unavailable_report(
    mode: CycleMode,
    cycle_key: str,
    phase: _MappingPhase,
    reasons: tuple[str, ...],
    deps: DaemonDeps,
) -> CycleReport:
    return CycleReport(
        mode=mode,
        cycle_key=cycle_key,
        alive=True,
        reconciliation_ready=False,
        apply_eligible=False,
        writes_suppressed=True,
        mapping_outcome=phase.outcome,
        mapping_revision=0,
        counts=_final_counts(deps, 0, 0, 0, 0, 0),
        reasons=reasons,
        drift=(),
        existing_link_drift=(),
        deferred_planned=0,
        failed=_is_transport_failure(phase.outcome),
    )


def _run_apply(
    deps: DaemonDeps,
    plans: Mapping[int, LinkPlan],
    flags: _EligibilityFlags,
    active: ActiveMappings,
    folders: Mapping[int, ImportFolder],
    cycle_key: str,
    now_iso: str,
    cycle_start_mono: float,
    apply_running: bool,
    reasons: list[str],
    drift: list[str],
) -> tuple[int, int]:
    """Drive the apply loop and fold its reasons/drift into the cycle's lists."""
    if not apply_running:
        return (0, 0)
    queue = fetch_queue(deps.shoko)
    submitted, deferred, apply_reasons, apply_drift = _apply(
        deps,
        plans,
        flags,
        active,
        folders,
        queue,
        cycle_key,
        now_iso,
        cycle_start_mono,
        deps.max_submissions,
    )
    reasons.extend(apply_reasons)
    drift.extend(apply_drift)
    reasons.extend(queue_reasons(queue))
    return (submitted, deferred)


def queue_reasons(queue: QueueSnapshot | None) -> tuple[str, ...]:
    if queue is None:
        return ("queue_unknown",)
    if queue.blocked_count > 0:
        return (f"queue_blocked:{queue.blocked_count}",)
    return ()


def _baseline_file(sonarr_file: SonarrFile) -> BaselineFile:
    return BaselineFile(
        sonarr_series_id=sonarr_file.series_id,
        sonarr_file_id=sonarr_file.episode_file_id,
        path=sonarr_file.path,
        size=sonarr_file.size,
    )


def _is_transport_failure(outcome: RefreshOutcome | None) -> bool:
    return outcome in (
        RefreshOutcome.FAILED_TRANSPORT,
        RefreshOutcome.NO_ACTIVE_BUT_FAILED,
    )


def _final_counts(
    deps: DaemonDeps,
    discovered: int,
    backlog: int,
    new_arrival: int,
    submitted: int,
    pending: int,
) -> CycleCounts:
    store = deps.store
    matched = len(store.files_in_state(FileState.MATCHED))
    mapped = len(store.files_in_state(FileState.MAPPED))
    planned = len(store.files_in_state(FileState.PLANNED))
    verified = len(store.files_in_state(FileState.VERIFIED))
    indeterminate = len(store.files_in_state(FileState.INDETERMINATE_WRITE)) + len(
        store.files_in_state(FileState.REQUEST_MAY_HAVE_BEEN_SENT)
    )
    blocked = len(store.files_in_state(FileState.BLOCKED_SAFETY))
    return CycleCounts(
        discovered=discovered,
        matched=matched,
        mapped=mapped,
        planned=planned,
        submitted=submitted,
        verified=verified,
        pending=pending,
        indeterminate=indeterminate,
        blocked=blocked,
        backlog=backlog,
        new_arrival=new_arrival,
    )


# ------------------------------------------------------------------ daemon loop


def _compute_sleep(
    base_seconds: float,
    consecutive_failures: int,
    jitter_ratio: float,
    rng: Callable[[], float],
) -> float:
    """Exponential backoff (24h cap) with jitter for failures; base interval for success."""
    if consecutive_failures <= 0:
        seconds = base_seconds
    else:
        seconds = min(MAX_BACKOFF_SECONDS, base_seconds * 2.0 ** (consecutive_failures - 1))
    jitter = 1.0 + jitter_ratio * (rng() - 0.5)
    return seconds * jitter


def run_daemon(
    deps: DaemonDeps,
    *,
    cycle_key: Callable[[int], str],
    now_iso: Callable[[], str],
    interval_seconds: float,
    stop_after_cycles: int | None = None,
    should_stop: Callable[[], bool] | None = None,
    policy: RunPolicy = RunPolicy.NEW_ARRIVAL,
    mode: CycleMode = CycleMode.APPLY,
    jitter_ratio: float = 0.1,
) -> DaemonReport:
    """Poll cycles forever (or ``stop_after_cycles``), sleeping ``interval_seconds``
    with jitter between them; SIGTERM is honoured at the ``should_stop`` check
    **after** the current cycle completes (no mid-cycle abort).

    Transport-level cycle failures grow the sleep with exponential backoff (24h cap,
    jitter); a successful cycle resets it. The persisted budget ``next_due``/pause
    state in the stores means a restart neither disables refreshes nor tight-loops.
    """
    reports: list[CycleReport] = []
    failures = 0
    index = 0
    shutdown_requested = False
    while True:
        report = run_cycle(
            deps,
            now_iso=now_iso(),
            mode=mode,
            policy=policy,
            cycle_key=cycle_key(index),
        )
        reports.append(report)
        index += 1
        if should_stop is not None and should_stop():
            shutdown_requested = True
            break
        if stop_after_cycles is not None and index >= stop_after_cycles:
            break
        failures = failures + 1 if report.failed else 0
        sleep_seconds = _compute_sleep(interval_seconds, failures, jitter_ratio, deps.jitter)
        deps.advance(sleep_seconds)
    exit_reason = "shutdown_requested" if shutdown_requested else "stop_after_cycles"
    return DaemonReport(
        cycles=tuple(reports),
        exit_reason=exit_reason,
        shutdown_requested=shutdown_requested,
    )
