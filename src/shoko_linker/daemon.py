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
* **Advanced daemon cycles only auto-apply the ``new_arrival`` cohort.** Normal service mode
   separately uses the explicit ``SERVICE`` policy to process backlog and arrivals after current
   onboarding authorization; backlog is never auto-submitted under ``NEW_ARRIVAL``. An
   **approved** advanced backlog campaign is drained by the daemon
   (F7, when ``campaign.daemon_auto_apply`` is set) as ONE bounded batch per apply cycle, after
   the operator's explicit approval + canary, through the full campaign guard chain under the
   cycle's apply lock — the plan's approval digest, campaign status, deadline, budgets,
   write gate, campaign-scope attestation, queue guard, and mapping revalidation all stay
   authoritative. The one-batch CLI ``campaign apply`` remains for manual operation; a daemon
   starting with ``apply_changes=true`` still never submits an unapproved backlog.
    **F18 budget cooldown:** a budget-blocked (or pending-metadata / pending-verification /
    queue-congested) campaign is a RECOVERABLE wait — it is DEFERRED with a persisted retry
     time (the rolling-window expiry, or the bounded stall cooldown of
     :data:`STALL_YIELD_COOLDOWN_CYCLES` cycle intervals when no window blocks), stays APPROVED and
    drain-authorized, is skipped while the retry is in the future (so a later drain-authorized
    campaign is never starved by a stalled head), and resumes automatically once it passes.
    The 3-cycle zero-progress breaker pauses ONLY
    genuinely-unexpected stalls (``no_queueable_candidates``); ``max_submissions_reached`` and
    the safety pauses stay terminal.
  New arrivals inside ``sync.new_file_grace_period_seconds`` (default 300s, measured from
  the persisted ``first_seen_iso``) defer with ``grace_period`` — they stay ``planned``
  and consume no budget slot — so Shoko's post-arrival import state settles first.
* **Apply requires the whole eligibility conjunction**: advanced mode requires its open apply
  gate and valid safety attestation; normal service mode requires current onboarding consent.
  Both require budget headroom, an active mapping generation, source continuity, a complete
  auto-apply-eligible baseline, and no pending rebaseline.
* **A possibly-sent file is never resubmitted** (state transition table); the daemon
  verifies/holds it. Mapping churn and source discontinuity
  pause/suppress writes persistently.
* **Every cycle consumes persisted recovery before processing new candidates** (M6 review
  P1-3): ``verifying`` rows are re-polled against the send row's persisted target triples,
  ``request_may_have_been_sent``/``indeterminate_write`` rows are inspected read-only via
  ``GET File/{id}?include=XRefs``: exact targets close VERIFIED; terminal partial/contradiction
  results are persisted for operator review with the slot held. ``prepared_not_sent`` rows
  are voided (P2-2) and
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
import json
import logging
import math
import os
import time
from collections import deque
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from enum import Enum
from types import MappingProxyType
from typing import Any, Final

import httpx

from . import mappings as _mappings
from .attestation import SCOPE_DAEMON, AttestationVerdict
from .backoff import capped_backoff
from .config import Settings, apply_gate
from .errors import ConfigError, HttpPolicyError
from .file_display import MAX_DISPLAY_LOCATIONS, safe_display_path, series_display_name
from .http import ApiEndpoint, BoundedJsonError, get_bounded_json
from .linking import (
    EpisodeIdResolver,
    EpisodeResolutionStatus,
    EpisodeTargetResolution,
    LinkPlan,
    LinkTarget,
    MetadataDemand,
    PendingTarget,
    PlanSkip,
    PlanStatus,
    PreflightReason,
    PreflightVerdict,
    QueueSnapshot,
    ResolvedEpisode,
    SendOutcome,
    SendResult,
    ShokoFileXRefs,
    VerificationVerdict,
    fetch_queue,
    freeze_metadata_demand,
    freeze_plan,
    metadata_demand_fingerprint,
    parse_file_xrefs,
    preflight,
    preflight_file,
    preflight_link_jobs_and_targets,
    preflight_reasons_are_retryable,
    primary_hard_preflight_reason,
    request_fingerprint,
    submit_link,
    verify_link,
)
from .logging_setup import diagnostic_text
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
from .mapping_store import (
    status as mapping_status,
)
from .mappings import (
    EpisodeType,
    MappingIndex,
    MappingTarget,
    ResolutionStatus,
    effective_source_targets,
)
from .mappings import resolve as resolve_mapping
from .metadata_operations import metadata_source_key, recover_metadata_operations
from .metadata_refresh import MetadataRefreshOutcome, submit_metadata_refresh
from .metadata_status import observe_metadata_status, observe_resolved_metadata_plans
from .onboarding import (
    METADATA_MAINTENANCE_CONFIRMATION,
    METADATA_MAINTENANCE_POLICY,
    OnboardingManager,
    OnboardingRecord,
    current_metadata_maintenance_policy,
)
from .policies import RunPolicy
from .queue_inventory import Presence, QueueJobKind, fetch_queue_inventory
from .rate_control import (
    MAX_SERVICE_CYCLE_SUBMISSIONS,
    RateSignals,
    RateState,
    advance_rate,
    decode_rate_state,
    effective_submission_cap,
    encode_rate_state,
    service_budget_limits,
)
from .scan_status import ScanSummary, scan_context_identity, scan_summary_payload
from .shoko import (
    ExclusionReason,
    ImportFolder,
    ShokoDiscovery,
    ShokoDiscoveryProgress,
    ShokoMatchResult,
    canonical_ed2k,
    collect_shoko_discovery,
    match_shoko_files,
)
from .sonarr import (
    SonarrEpisode,
    SonarrFile,
    SonarrInventory,
    SourceRelationshipStatus,
    SourceRelationshipVerdict,
    check_current_file_relationship,
    collect_sonarr_inventory,
)
from .state import (
    ATTESTATION_PAUSE_REASON,
    CAMPAIGN_QUARANTINE_PROGRESS_PENDING_MARKER,
    CAMPAIGN_QUEUE_STATES,
    CAMPAIGN_STATUS_PAUSED,
    DIGEST_HEX_CHARS,
    MAPPING_RESOLVED_ID_COUNT,
    MAX_DETAIL_CHARS,
    META_ATTESTATION_GATE,
    META_CAMPAIGN_ATTESTATION_GATE,
    META_CAMPAIGN_DEFERRED_UNTIL,
    META_CAMPAIGN_ZERO_PROGRESS,
    META_GLOBAL_PAUSE_ORIGIN,
    META_NEXT_FULL_RECONCILE_ISO,
    META_RECOVERY_CURSOR,
    ApplyLockUnavailableError,
    BaselineFile,
    BudgetLimits,
    BudgetRefusal,
    BudgetView,
    Cohort,
    EligibilityVerdict,
    FileIdentitySnapshot,
    FileRecord,
    FileSeed,
    FileSeriesDisplay,
    FileState,
    LockHeldError,
    MetadataOperation,
    MetadataOperationState,
    ScanExclusionRow,
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
MAX_MAPPING_ORIGIN_LABEL_CHARS: Final[int] = 80
MAPPING_TARGET_KEY_PARTS: Final[int] = 7
MAX_VERIFIED_MAPPING_SNAPSHOT_CHARS: Final[int] = 1_000_000
MAX_EXCLUSION_EXAMPLES: Final[int] = 20
MAX_EXCLUSION_EXAMPLE_CHARS: Final[int] = 512
SOURCE_IDENTITY_FIELDS: Final[str] = "sonarr_url,shoko_url"
META_SHOKO_DISCOVERY_PROGRESS: Final[str] = "shoko_discovery_progress:v1"
# Matches the ``grace_period`` retryable label in :mod:`shoko_linker.state` (reason taxonomy).
GRACE_PERIOD_REASON: Final[str] = "grace_period"

#: Bounded daemon history (F4): an unbounded daemon retains only the last N cycle reports
#: (older reports are released for GC); a bounded ``stop_after_cycles`` run retains
#: every executed cycle (the run is finite). ``DaemonReport.total_cycles``/``total_failed``
#: are the accurate counts and never depend on this bound.
DAEMON_HISTORY_MAXLEN: Final[int] = 100
SCOPE_EXCLUSION_REASONS: Final[frozenset[ExclusionReason]] = frozenset(
    {
        ExclusionReason.DROP_FOLDER_TYPE,
        ExclusionReason.MIXED_OWNERSHIP,
        ExclusionReason.OUTSIDE_ALLOWED_ROOT,
    }
)


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


def _service_authorization_closed() -> bool:
    return False


def _no_attestation_verdict() -> AttestationVerdict | None:
    return None


def _current_wall_iso() -> str:
    """The wall clock for per-submission timestamps (test seam: a fake clock)."""
    return datetime.now(UTC).isoformat()


def _no_shutdown_requested() -> bool:
    """No shutdown seam (CLI/once paths): the flag stays False."""
    return False


@dataclass(frozen=True)
class AttestationScopeHooks:
    """Per-campaign attestation callables (F7 daemon drain, campaign-scope lease).

    A campaign batch is authorized by a lease scoped ``campaign:<id>``, not the daemon
    lease. The CLI wires :data:`DaemonDeps.campaign_attestation` to a factory that returns
    this pair for a campaign id; the daemon drain builds a campaign-scoped copy of
    ``deps`` from it. With no factory (plain-bool test wiring) the daemon's own attestation
    stays authoritative and drain proceeds under it.
    """

    valid: Callable[[], bool]
    verdict: Callable[[], AttestationVerdict | None]


@dataclass(frozen=True)
class DaemonDeps:
    """Every injected dependency a cycle needs: endpoints, stores, clocks, safety inputs.

    M7 supplies the real endpoints/attestation lease; tests inject ``MockTransport``
    endpoints, fake clocks, and tmp stores. Defaults are safe-closed (no attestation or service
    consent, no jitter, monotonic clock) so a partially wired daemon never writes.
    ``blocked_over=None`` (the default) is **not** "no threshold": every preflight call
    site resolves it through :func:`effective_blocked_over` to
    ``sync.queue_blocked_tolerance`` (default 0 — a blocked queue suppresses apply).
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
    attestation_verdict: Callable[[], AttestationVerdict | None] = _no_attestation_verdict
    #: The ordinary service has its own durable, scope-bound consent. It never borrows the
    #: advanced CLI's apply gate or operator attestation.
    service_mode: bool = False
    service_authorized: Callable[[], bool] = _service_authorization_closed
    #: Production remote authorization is bound once to the actual cycle I/O deadline.
    #: The no-argument callback remains useful for offline/local callers; production
    #: supplies this callback instead, so repeated checks never get a fresh allowance.
    service_authorized_for_deadline: Callable[[float], bool] | None = None
    #: Local persistence/pause re-check after the last Shoko network guard. Production service
    #: wiring captures the fresh remote authorization context immediately before that guard.
    #: Required for service-mode writes: a missing callback fails closed and never falls back to
    #: another remote authorization request after the final protected-link read.
    service_authorized_local: Callable[[], bool] | None = None
    lease_seconds: float = 600.0
    blocked_over: int | None = None
    verification_timeout_seconds: float = 600.0
    poll_interval_seconds: float = 5.0
    max_verify_polls: int = 100
    max_submissions: int = 1
    max_cycle_seconds: float = 300.0
    churn_limit: int = 500
    max_recovery_rows_per_cycle: int = 100
    campaign_attestation: Callable[[str], AttestationScopeHooks] | None = None
    #: F11: the fresh wall clock per submission (never the batch/cycle-start snapshot) and the
    #: SIGTERM flag wired by the daemon runner. Defaults are safe for test/once paths.
    now: Callable[[], str] = _current_wall_iso
    shutdown_requested: Callable[[], bool] = _no_shutdown_requested


@dataclass(frozen=True)
class CycleCounts:
    """Per-cycle new-arrival tallies; campaign sends are reported on CycleReport separately."""

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
    ``apply_eligible`` is retained as the new-arrival eligibility alias. Explicit
    new-arrival/campaign eligibility and write activity fields distinguish both mutation
    paths; ``writes_suppressed`` means every cycle mutation path was gated. ``campaign`` is
    the bounded campaign-batch summary (:class:`~shoko_linker.campaign.CampaignApplyReport`)
    drained in this cycle under the cycle's own apply lock and budget key (F7), or ``None`` when
    no batch ran. ``campaign_identity_drift_details`` is an internal handoff for campaign apply
    when reconciliation defers a quarantine to respect the batch allowance.
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
    full: bool = True
    plans: Mapping[int, LinkPlan] = MappingProxyType({})
    folders: Mapping[int, ImportFolder] = MappingProxyType({})
    campaign: Any | None = None
    mapping_snapshot: tuple[int, str, str, str, bool] | None = field(
        default=None, repr=False, compare=False
    )
    campaign_identity_drift_details: Mapping[int, tuple[str, str]] = field(
        default_factory=lambda: MappingProxyType({}), repr=False, compare=False
    )
    exclusion_counts: Mapping[str, int] = field(
        default_factory=lambda: MappingProxyType({}), repr=False, compare=False
    )
    scope_excluded_count: int = 0
    resolution_deferred_file_ids: frozenset[int] = field(
        default_factory=frozenset, repr=False, compare=False
    )
    new_arrival_apply_eligible: bool = False
    campaign_drain_selected: bool = False
    campaign_apply_eligible: bool = False
    new_arrival_submitted: int = 0
    campaign_submitted: int = 0
    total_link_requests_attempted: int = 0
    new_arrival_writes_suppressed: bool = True
    campaign_writes_suppressed: bool = True
    metadata_requests_attempted: int = 0
    metadata_deferred: int = 0
    metadata_observations: tuple[tuple[int, str], ...] = ()


@dataclass(frozen=True)
class _CampaignDrainOutcome:
    """Campaign drain selection and eligibility, kept separate from its submission count."""

    selected: bool = False
    apply_eligible: bool = False
    report: Any | None = None


@dataclass(frozen=True)
class DaemonReport:
    """Aggregate of a daemon run: a bounded cycle history, accurate totals, and the exit cause.

    ``cycles`` is a **bounded history** (F4): for a bounded ``stop_after_cycles`` run it holds
    every executed cycle (the run is finite); for an unbounded daemon it holds
    the last :data:`DAEMON_HISTORY_MAXLEN` cycles — older reports (with their per-file plan and
    import-folder data) are released for GC so an unattended service does not grow without bound.
    ``total_cycles`` and ``total_failed`` are the accurate run counts and never depend on the
    history bound; ``last_cycle`` is the most recent completed report (``None`` before any cycle).
    """

    cycles: tuple[CycleReport, ...]
    exit_reason: str
    shutdown_requested: bool
    total_cycles: int = 0
    total_failed: int = 0
    last_cycle: CycleReport | None = None


@dataclass(frozen=True)
class _EligibilityFlags:
    attestation: bool | None
    gate: bool | None
    service_authorized: bool | None
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


def full_cycle_due(store: StateStore, now_iso: str) -> bool:
    """True when a full reconcile is due: no/invalid marker, or ``now`` at/after the marker.

    The marker is the persisted ``next_full_reconcile_iso`` (written by the CLI cycle
    bookkeeping from ``sync.full_reconcile_interval_hours``); a light streak can never
    starve the full pass beyond the configured interval — the interval gate forces it.
    """
    raw = store.get_meta(META_NEXT_FULL_RECONCILE_ISO)
    if raw is None:
        return True
    try:
        due = _parse_iso(raw)
    except ValueError:
        return True
    return _parse_iso(now_iso) >= due


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
            covered = ",".join(
                f"{episode}:{episode_type.value}"
                for episode, episode_type in sorted(
                    absolute_claim.covered, key=lambda item: (item[0], item[1].value)
                )
            )
            spans = ",".join(
                f"{start}-{end}@{anidbseason}"
                for start, end, anidbseason in sorted(absolute_claim.intervals)
            )
            per_series.setdefault(tvdb_series, []).append(
                f"A|{absolute_claim.anidb_id}|{covered}|{spans}"
            )
    for (tvdb_series, season), declared in index.no_counterpart.items():
        for anidb_id, episode, episode_type in declared:
            per_series.setdefault(tvdb_series, []).append(
                f"N|{season}|{anidb_id}|{episode}|{episode_type.value}"
            )
    for tvdb_series, season, episode in index.unsafe_tvdb_keys:
        per_series.setdefault(tvdb_series, []).append(f"F|{season}|{episode}")
    for tvdb_series in index.unsafe_tvdb_ids:
        per_series.setdefault(tvdb_series, []).append("X|fenced-owner")
    return MappingProxyType({series: tuple(sorted(lines)) for series, lines in per_series.items()})


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
    last_outcome: RefreshOutcome | None = None


def _mapping_phase(deps: DaemonDeps, now_iso: str) -> _MappingPhase:
    """Run the scheduler, then classify the generation change (activation/churn)."""
    overrides = _load_overrides(deps)
    old = load_active(deps.mapping_store, overrides)
    outcome: RefreshOutcome | None = None
    if mapping_due(deps.mapping_store, _parse_iso(now_iso), overrides):
        outcome = run_mapping_check(deps, now_iso)
    else:
        outcome = RefreshOutcome.NOT_DUE
    active = load_active(deps.mapping_store, overrides)
    reactivated = False
    churned: frozenset[int] = frozenset()
    churn_paused = False
    if active is not None:
        # P2-3: churn includes identity-rule changes (offsets, a-rules, no-counterpart
        # markers) — unbounded rules move no enumerable target digest.
        prior = deps.store.get_meta("reconciled_mapping_semantics:v1")
        current = _mapping_semantics(active.index)
        previous = (
            json.loads(prior)
            if prior is not None
            else (_mapping_semantics(old.index) if old is not None else current)
        )
        churned = frozenset(
            int(series)
            for series in set(previous) | set(current)
            if previous.get(series, []) != current.get(series, [])
        )
        reactivated = bool(churned) or outcome is RefreshOutcome.ACTIVATED
        # P3-A: the churn guard fires at exactly the limit (``>=``): a churn of exactly
        # ``churn_limit`` series pauses; the brief's boundary is inclusive.
        if len(churned) >= deps.churn_limit:
            # Preserve any existing operator/system pause and its origin. A mapping-churn
            # marker is campaign-bypassable only when this guard installed it itself.
            if not deps.store.writes_disabled:
                deps.store.pause_if_unpaused("mapping_churn", now_iso, origin="mapping_churn")
            churn_paused = True
    last_outcome = (
        mapping_status(
            deps.mapping_store, _parse_iso(now_iso), overrides_text=deps.overrides_text
        ).last_outcome
        if outcome is RefreshOutcome.NO_ACTIVE_BUT_FAILED
        else outcome
    )
    return _MappingPhase(outcome, old, active, reactivated, churned, churn_paused, last_outcome)


def _mapping_semantics(index: MappingIndex) -> dict[str, list[str]]:
    enumerable = effective_source_targets(index)
    identity = _identity_rule_lines_by_series(index)
    return {
        str(series): sorted([*enumerable.get(series, ()), *identity.get(series, ())])
        for series in set(enumerable) | set(identity)
    }


def _load_overrides(deps: DaemonDeps) -> _mappings.Overrides | None:
    if deps.overrides_text is None:
        return None
    return _mappings.load_overrides(deps.overrides_text)


# ------------------------------------------------------------------ eligibility


def effective_blocked_over(deps: DaemonDeps) -> int:
    """The fail-closed queue-block threshold for every preflight call site.

    M7 review P1: a blocked Shoko queue makes ``ManualLinkJob`` delivery untrustworthy,
    so an unwired threshold is never "no threshold" — it is ``sync.queue_blocked_tolerance``
    (default **0**: any blocked work refuses apply). ``queue=None`` keeps its independent
    ``QUEUE_UNKNOWN`` fail-closed refusal.
    """
    if deps.blocked_over is not None:
        return deps.blocked_over
    return deps.settings.sync.queue_blocked_tolerance


def _global_pause_reason(deps: DaemonDeps, *, campaign: bool) -> str | None:
    """The global write pause that must gate this scope, or ``None``.

    F8: the daemon-scope lease transition records a global write pause
    (``attestation_expired`` under ``META_ATTESTATION_GATE=paused``). That pause gates daemon
    writes ONLY — it must never block an authorized campaign drain. F24 also allows a campaign
    to continue through the daemon's mapping-churn pause only when that pause was generated by
    the mapping-churn guard: each approved candidate rechecks its effective targets before POST.
    Both bypasses are **origin-gated**; an operator ``pause-writes`` kill switch, source break,
    queue anomaly, or any other global pause still gates every scope.
    """
    reason = deps.store.budget_view().paused_reason
    if reason is None:
        return None
    if (
        campaign
        and reason == ATTESTATION_PAUSE_REASON
        and deps.store.get_meta(META_ATTESTATION_GATE) == "paused"
        and deps.store.get_meta(META_GLOBAL_PAUSE_ORIGIN) in (None, "attestation_gate")
    ):
        # Missing origin is accepted only for legacy databases created before origins were
        # persisted; the paired daemon gate marker and reserved reason identify that pause.
        return None
    if (
        campaign
        and reason == "mapping_churn"
        and deps.store.get_meta(META_GLOBAL_PAUSE_ORIGIN) == "mapping_churn"
    ):
        return None
    return reason


def _budget_headroom(deps: DaemonDeps, now_iso: str, *, campaign: bool = False) -> bool:
    """Non-mutating peek: is there room for at least one more submission?

    The authoritative reservation happens in ``prepare_send``; this only feeds the
    cycle-level eligibility verdict so a saturated daemon suppresses apply early.
    The peek counts the same durable trailing-window reservation timestamps as
    ``StateStore._reserve_tx``; expiration releases only the expired reservations.
    """
    view = deps.store.budget_view()
    limits = deps.store.budget_limits
    if _global_pause_reason(deps, campaign=campaign) is not None:
        return False
    if limits is None:
        return not (deps.service_mode and not campaign)
    if deps.service_mode and not campaign:
        limits = service_budget_limits(limits)
    if view.pending_file_requests >= limits.max_pending_files:
        return False
    if view.pending_episode_jobs >= limits.max_pending_episode_jobs:
        return False
    return not rolling_budget_exhausted(view, limits, now_iso)


def _service_candidate_budget_headroom(
    deps: DaemonDeps, now_iso: str, expected_episode_jobs: int
) -> bool:
    """Recheck effective hard service budgets immediately before reserving a send."""
    if not deps.service_mode:
        return True
    configured = deps.store.budget_limits
    if configured is None:
        return False
    limits = service_budget_limits(configured)
    view = deps.store.budget_view()
    return not (
        view.cycle_new_submissions >= limits.per_cycle_new
        or view.pending_file_requests + 1 > limits.max_pending_files
        or view.pending_episode_jobs + expected_episode_jobs > limits.max_pending_episode_jobs
        or rolling_budget_exhausted(view, limits, now_iso)
    )


def _apply_service_budget_limits(deps: DaemonDeps) -> None:
    """Make the existing durable reservation path enforce service hard ceilings verbatim."""
    configured = deps.store.budget_limits
    if deps.service_mode and configured is not None:
        deps.store.budget_limits = service_budget_limits(configured)


def _service_mapping_refresh_failed(
    deps: DaemonDeps, phase: _MappingPhase, reasons: list[str]
) -> bool:
    if deps.mapping_store.invalid_source_hold():
        reasons.append("mapping_source_invalid")
        return True
    if deps.service_mode and _is_transport_failure(phase.last_outcome):
        reasons.append("mapping_source_transport_failure")
        return True
    if (
        deps.service_mode
        and phase.active is None
        and phase.last_outcome is RefreshOutcome.FAILED_SIZE
    ):
        reasons.append("mapping_source_too_large")
        return True
    return False


def rolling_budget_exhausted(view: BudgetView, limits: BudgetLimits, now_iso: str) -> bool:
    """The shared trailing-window reservation peek.

    Shared verbatim by :func:`_budget_headroom` (daemon eligibility), the campaign drain's
    cooldown computation, and :func:`shoko_linker.reporting.budgets_ok` (report/health rollup):
    each reservation expires independently, exactly as in ``StateStore._reserve_tx``.
    """
    return view.rolling_exhausted(limits, now_iso)


def _eligibility_flags(
    deps: DaemonDeps, mapping_present: bool, now_iso: str, *, campaign: bool = False
) -> _EligibilityFlags:
    source_stable = deps.store.get_meta("requires_rebaseline") != "1"
    paused = _global_pause_reason(deps, campaign=campaign) is not None
    use_service_authorization = deps.service_mode and not campaign
    return _EligibilityFlags(
        attestation=None if use_service_authorization else deps.attestation_valid(),
        gate=None if use_service_authorization else apply_gate(deps.settings),
        service_authorized=deps.service_authorized() if use_service_authorization else None,
        budgets_ok=_budget_headroom(deps, now_iso, campaign=campaign),
        mapping_stable=mapping_present and not paused,
        source_stable=source_stable,
    )


def compute_eligibility(
    deps: DaemonDeps,
    now_iso: str,
    *,
    campaign: bool = False,
) -> EligibilityVerdict:
    """Compose the cycle-level auto-apply verdict from every safety input.

    Advanced mode requires the apply gate and operator attestation; explicitly selected normal
    service mode instead requires current onboarding consent. Both modes still require budget
    headroom, an active mapping generation, source continuity, and no applicable write pause.
    Campaign eligibility always uses its separate legacy authorization path.
    """
    active = load_active(deps.mapping_store, _load_overrides(deps))
    flags = _eligibility_flags(deps, active is not None, now_iso, campaign=campaign)
    reasons: list[str] = []
    if deps.store.writes_disabled:
        reasons.append("writes_disabled")
    if deps.service_mode and not campaign:
        if not flags.service_authorized:
            reasons.append("onboarding_not_authorized")
    else:
        if flags.gate is False:
            reasons.append("apply_gate_closed")
        if flags.attestation is False:
            reasons.append("attestation_invalid")
    if not flags.budgets_ok:
        reasons.append("budgets_exhausted")
    if active is None:
        reasons.append("mapping_unavailable")
    if not flags.mapping_stable:
        reasons.append("mapping_unstable")
    if deps.mapping_store.invalid_source_hold():
        reasons.append("mapping_source_invalid")
    if not flags.source_stable:
        reasons.append("source_unstable")
    return EligibilityVerdict(not reasons, tuple(reasons))


# ------------------------------------------------------------------ attestation gate


def _clear_gate_pause(
    deps: DaemonDeps, gate_key: str, now_iso: str, is_campaign: bool
) -> tuple[str, ...]:
    """Clear a persisted pause on the first invalid→valid transition for one scope."""
    reasons: list[str] = []
    gate = deps.store.get_meta(gate_key)
    if not is_campaign and gate == "paused":
        resumed = False
        gate_cleared = False
        with contextlib.suppress(WriteDisabledError, StateError):
            resumed = deps.store.resume_if_pause(
                ATTESTATION_PAUSE_REASON,
                now_iso,
                allowed_origins=(None, "attestation_gate"),
            )
            deps.store.set_meta(gate_key, "valid")
            gate_cleared = True
        if gate_cleared:
            reasons.append("attestation_resumed" if resumed else "attestation_gate_cleared")
    elif gate != "valid":
        with contextlib.suppress(WriteDisabledError, StateError):
            deps.store.set_meta(gate_key, "valid")
        if is_campaign and gate == "paused":
            # F4: the campaign gate meta is cleared, but the campaign itself stays PAUSED (the
            # operator resumes it via 'campaign resume') — report the gate clear, not a resume.
            reasons.append("campaign_attestation_gate_cleared")
    return tuple(reasons)


def _persist_gate_pause(
    deps: DaemonDeps,
    gate_key: str,
    now_iso: str,
    *,
    is_campaign: bool,
    campaign_id: str | None,
    status: str,
    reason: str,
) -> tuple[str, ...]:
    """Persist the once-per-transition pause for one scope (idempotent campaign status)."""
    log = logging.getLogger(__name__)
    reasons: list[str] = []
    gate = deps.store.get_meta(gate_key)
    if is_campaign and campaign_id is not None:
        # P2-3: idempotently persist the campaign status pause on EVERY invalid cycle so a
        # resumed campaign whose lease dies is re-paused (drops out of drain selection), even
        # when the gate meta is already "paused". No-op safe if already paused.
        with contextlib.suppress(WriteDisabledError, StateError):
            deps.store.set_campaign_status(
                campaign_id, CAMPAIGN_STATUS_PAUSED, ATTESTATION_PAUSE_REASON, iso=now_iso
            )
    if gate != "paused":
        # F8 review: the gate pauses writes ONLY when no global pause is already active. A
        # foreign pause (mapping churn, source break, queue anomaly, an operator pause-writes
        # reason) KEEPS its own reason: overwriting it with ``attestation_expired`` would arm
        # the origin-gated campaign bypass (gate meta "paused" + reason "attestation_expired")
        # on a churn-paused deployment and relabel the operator's kill switch.
        with contextlib.suppress(WriteDisabledError, StateError):
            if not is_campaign:
                deps.store.pause_if_unpaused(
                    ATTESTATION_PAUSE_REASON, now_iso, origin="attestation_gate"
                )
            deps.store.set_meta(gate_key, "paused")
        log.warning(
            "safety attestation is %s (%s) for scope %s: writes for this scope are paused "
            "(persisted, once per transition); the daemon stays alive reporting read-only",
            status,
            reason,
            gate_key,
        )
        reasons.append("attestation_pause_persisted")
    else:
        reasons.append("attestation_pause_active")
    return tuple(reasons)


def attestation_gate(
    deps: DaemonDeps,
    now_iso: str,
    *,
    mutate: bool = True,
    scope: str = SCOPE_DAEMON,
    campaign_id: str | None = None,
) -> tuple[str, ...]:
    """Compute the attestation gate and the persisted pause *transition* (once) for one scope.

    A lease that is absent/invalid/expired/stale suppresses apply for **its own scope** (the
    eligibility conjunction already refuses with ``attestation_invalid``). The first
    valid→invalid transition persists one pause for that scope and logs a warning; repeated
    invalid cycles do **not** re-pause (no restart loop, daemon stays alive reporting
    read-only). The first invalid→valid transition clears the pause.

    F8 scope separation: the **daemon** scope persists a global write pause
    (``attestation_expired`` via :meth:`shoko_linker.state.StateStore.pause`) under
    ``META_ATTESTATION_GATE`` only when no global pause is already active — a foreign pause
    (churn/source/queue/operator) keeps its reason, so the origin-gated campaign bypass never
    arms on a churn- or operator-paused deployment; the invalid→valid transition clears it only
    when the pause reason is exactly ``attestation_expired`` (a mapping-churn pause is never
    cleared here). The
    **campaign** scope never touches the global write pause — it records a per-campaign gate
    (``META_CAMPAIGN_ATTESTATION_GATE:<id>``) and idempotently pauses the campaign via
    ``set_campaign_status`` so a campaign-lease failure stops only that campaign's submissions,
    never daemon writes. ``mutate=False`` (daemon DRY_RUN) computes the verdict and warns
    without touching persisted gate state.

    Without a verdict seam (pure-bool M6-a wiring) this is a no-op: the bool input stays
    authoritative and no pause bookkeeping happens.
    """
    verdict = deps.attestation_verdict()
    if verdict is None:
        return ()
    reasons: list[str] = []
    log = logging.getLogger(__name__)
    is_campaign = scope != SCOPE_DAEMON
    if is_campaign and campaign_id is None:
        # P3-7: a campaign scope with no campaign_id would silently share the daemon gate key.
        raise ValueError("attestation_gate: a campaign scope requires an explicit campaign_id")
    gate_key = (
        f"{META_CAMPAIGN_ATTESTATION_GATE}:{campaign_id}" if is_campaign else META_ATTESTATION_GATE
    )
    log.debug(
        "attestation gate: scope=%s status=%s reason=%s", scope, verdict.status, verdict.reason
    )
    if verdict.valid_for_apply:
        if verdict.warn:
            reasons.append("attestation_expires_soon")
            log.warning(
                "safety attestation expires soon (%s): re-record the lease before "
                "apply resumes being refused",
                verdict.reason,
            )
        if not mutate:
            return tuple(reasons)
        reasons.extend(_clear_gate_pause(deps, gate_key, now_iso, is_campaign))
        return tuple(reasons)
    reasons.append(f"attestation_{verdict.status}")
    if not mutate:
        return tuple(reasons)
    reasons.extend(
        _persist_gate_pause(
            deps,
            gate_key,
            now_iso,
            is_campaign=is_campaign,
            campaign_id=campaign_id,
            status=verdict.status,
            reason=verdict.reason,
        )
    )
    return tuple(reasons)


# ------------------------------------------------------------------ reconcile


@dataclass(frozen=True)
class _ReconcileResult:
    inserted: int
    backlog: int
    new_arrival: int
    pending: int
    plans: Mapping[int, LinkPlan]
    skip_reasons: Mapping[int, str]
    existing_link_drift: tuple[str, ...]
    fresh_campaign_quarantines: frozenset[int]
    campaign_identity_drift_details: Mapping[int, tuple[str, str]]
    exclusion_counts: Mapping[str, int]
    scope_excluded_count: int
    resolution_deferred_file_ids: frozenset[int]
    metadata_demands: Mapping[int, MetadataDemand] = field(
        default_factory=lambda: MappingProxyType({})
    )


@dataclass(frozen=True)
class _MetadataWait:
    demand: MetadataDemand
    plan: LinkPlan | None


class _ResolutionDeferral(Enum):
    TRANSPORT_FAILURE = "transport_failure"


_SEND_RECOVERY_STATES: Final[frozenset[FileState]] = frozenset(
    {
        FileState.REQUEST_MAY_HAVE_BEEN_SENT,
        FileState.VERIFYING,
        FileState.INDETERMINATE_WRITE,
        FileState.PARTIAL,
    }
)


def _cohort_counts(rows: Sequence[tuple[int, Cohort]]) -> tuple[int, int]:
    backlog = sum(1 for _, cohort in rows if cohort is Cohort.BACKLOG)
    new_arrival = sum(1 for _, cohort in rows if cohort is Cohort.NEW_ARRIVAL)
    return backlog, new_arrival


def _record_and_match(
    deps: DaemonDeps,
    matches: Sequence[Any],
    now_iso: str,
    *,
    preserve_unresolved: frozenset[int] = frozenset(),
) -> tuple[dict[int, Cohort], int, dict[int, tuple[str, str]]]:
    preserve_unresolved |= _quarantine_legacy_mapping_targets(deps, now_iso)
    known = _tracked_file_ids(deps.store)
    cohorts: dict[int, Cohort] = {}
    seeds: list[FileSeed] = []
    identity_drifts: dict[int, tuple[str, str]] = {}
    for match in matches:
        record = deps.store.get_file(match.shoko_file_id)
        identity: FileIdentitySnapshot | None
        if record is None:
            identity = _identity_snapshot(match)
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
                    identity=identity,
                )
            )
            known = known | {match.shoko_file_id}
            cohorts[match.shoko_file_id] = cohort
        else:
            identity = record.identity
            cohorts[match.shoko_file_id] = record.cohort
        identity_drift = _source_hash_identity_drift(identity, match)
        if identity_drift is not None:
            identity_drifts[match.shoko_file_id] = identity_drift
    # P3b: bulk one-transaction ingest + bulk validated advance for first-scan
    # (7k backlog wall-time target; identical per-edge semantics, illegal edges skipped).
    deps.store.record_files(seeds)
    deps.store.advance_bulk(
        [
            match.shoko_file_id
            for match in matches
            if match.shoko_file_id not in preserve_unresolved
        ],
        FileState.MATCHED,
        "matched",
        iso=now_iso,
    )
    return cohorts, len(seeds), identity_drifts


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


def _source_hash_identity_drift(
    snapshot: FileIdentitySnapshot | None, match: Any
) -> tuple[str, str] | None:
    """Compare only the immutable first-seen ED2K with the current discovery identity."""
    original_raw = None if snapshot is None else snapshot.ed2k
    original = canonical_ed2k(original_raw)
    current_raw = match.hashes.ed2k
    current = canonical_ed2k(current_raw)
    if original is None:
        return (
            "source_hash_identity_missing",
            f"file {match.shoko_file_id} has no valid persisted original ED2K "
            f"(stored {original_raw!r}); refusing to re-freeze source identity",
        )
    if current is None:
        return (
            "source_hash_identity_unavailable",
            f"file {match.shoko_file_id} current discovery ED2K is missing or malformed "
            f"({current_raw!r}); refusing to re-freeze source identity",
        )
    if original != current:
        return (
            "source_hash_identity_drift",
            f"file {match.shoko_file_id} source ED2K identity drift: persisted original "
            f"{original_raw!r}, current discovery {current_raw!r}; Shoko ID reuse/content "
            "change requires operator review; no link plan will be frozen",
        )
    return None


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


def _quarantine_source_identity_drift(
    deps: DaemonDeps,
    file_id: int,
    reason: str,
    detail: str,
    now_iso: str,
    *,
    allow_campaign_quarantine: bool = True,
    deferred_campaign_details: dict[int, tuple[str, str]] | None = None,
) -> bool:
    """Quarantine identity drift unless a campaign batch must account for it itself."""
    record = deps.store.get_file(file_id)
    if record is None or record.state in _SEND_RECOVERY_STATES:
        return False
    campaign_pending = record.campaign_id is not None and record.state in (
        CAMPAIGN_QUEUE_STATES | {FileState.MATCHED, FileState.WAITING}
    )
    if campaign_pending:
        if not allow_campaign_quarantine:
            if deferred_campaign_details is not None:
                deferred_campaign_details[file_id] = (reason, detail)
            return False
        detail = (
            f"campaign_quarantine:{reason}{CAMPAIGN_QUARANTINE_PROGRESS_PENDING_MARKER} {detail}"
        )
    _advance(deps, file_id, FileState.BLOCKED_SAFETY, detail, now_iso)
    if not campaign_pending:
        return False
    updated = deps.store.get_file(file_id)
    return (
        updated is not None
        and updated.state is FileState.BLOCKED_SAFETY
        and updated.state_detail.startswith("campaign_quarantine:")
    )


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
    *,
    recover_missing_anime: bool = False,
) -> list[EpisodeTargetResolution] | str | _ResolutionDeferral:
    """Map every episode, then resolve each AniDB target to a Shoko internal ID.

    Returns the resolution list, a machine-readable skip reason for a permanent failure,
    or a typed retryable deferral for a transport failure (never a partial plan).
    """
    resolutions: list[EpisodeTargetResolution] = []
    mapped: list[tuple[SonarrEpisode, MappingTarget]] = []
    for episode in match.sonarr_file.episodes:
        answer = resolve_mapping(
            active.index,
            tvdb_series,
            episode.season_number,
            episode.episode_number,
            episode.absolute_episode_number,
        )
        if answer.status is not ResolutionStatus.RESOLVED or not answer.targets:
            prefix = f"target_{answer.status.value}:"
            provenance_label = "; candidate_provenance="
            provenance_fallback = f"[omitted={len(answer.candidates)}]"
            # Leave room for campaign-level approved identities, mapping digests and revision
            # when this reason is wrapped into a durable campaign quarantine detail.
            max_reason_chars = MAX_DETAIL_CHARS - 300
            detail_limit = (
                max_reason_chars - len(prefix) - len(provenance_label) - len(provenance_fallback)
            )
            detail = answer.detail
            if len(detail) > detail_limit:
                detail = detail[: max(0, detail_limit - 1)] + "…" if detail_limit else ""
            prefix += detail + provenance_label
            provenance = _compact_mapping_candidate_provenance(
                answer.candidates, max_chars=max_reason_chars - len(prefix)
            )
            return prefix + provenance
        for target in answer.targets:
            if (
                type(target.anidb_id) is not int
                or target.anidb_id <= 0
                or type(target.anidb_episode_number) is not int
                or target.anidb_episode_number <= 0
                or not isinstance(target.episode_type, EpisodeType)
            ):
                return "target_provenance_invalid:invalid mapped AniDB identity"
            mapped.append((episode, target))
    # Every source episode must map before any target metadata lookup. An early
    # missing anime must not create work for an ultimately ambiguous whole file.
    observed: dict[tuple[int, str, int], ResolvedEpisode] = {}
    for episode, target in mapped:
        triple = (target.anidb_id, target.episode_type.value, target.anidb_episode_number)
        resolved = observed.get(triple)
        if resolved is None:
            resolved = resolver.resolve(*triple)
            observed[triple] = resolved
        missing = resolved.status in (
            EpisodeResolutionStatus.PENDING_METADATA,
            EpisodeResolutionStatus.ANIME_NOT_CACHED,
        )
        pending = (
            missing
            if recover_missing_anime
            else resolved.status is EpisodeResolutionStatus.PENDING_METADATA
        )
        if resolved.status is EpisodeResolutionStatus.TRANSPORT_FAILURE:
            return _ResolutionDeferral.TRANSPORT_FAILURE
        if resolved.status is not EpisodeResolutionStatus.RESOLVED and not pending:
            return f"target_{resolved.status.value.lower()}:{resolved.detail}"
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
                metadata_status=resolved.status if missing else None,
            )
        )
    return resolutions


def _compact_mapping_candidate_provenance(candidates: Sequence[Any], *, max_chars: int) -> str:
    """Keep ambiguity provenance within the durable file-detail limit without hiding omissions."""
    grouped: dict[tuple[str, str], list[tuple[int, str, int]]] = {}
    for target in candidates:
        key = (target.source_rule.value, target.origin)
        grouped.setdefault(key, []).append(
            (target.anidb_id, target.episode_type.value, target.anidb_episode_number)
        )

    rendered_groups: list[tuple[str, int]] = []
    for (rule, origin), identities in sorted(grouped.items()):
        # Origins are diagnostic labels; cap an unusually long source label so it cannot crowd
        # out the candidate identities from the persisted reason.
        short_origin = (
            origin
            if len(origin) <= MAX_MAPPING_ORIGIN_LABEL_CHARS
            else origin[: MAX_MAPPING_ORIGIN_LABEL_CHARS - 1] + "…"
        )
        packed = ",".join(
            f"{anime}/{episode_type}/{number}"
            for anime, episode_type, number in sorted(set(identities))
        )
        rendered_groups.append((f"{rule}@{short_origin}:[{packed}]", len(identities)))

    selected: list[str] = []
    selected_count = 0
    total = len(candidates)
    for rendered, count in rendered_groups:
        omitted_after = total - selected_count - count
        suffix = f"; omitted={omitted_after}" if omitted_after else ""
        if len("[" + "; ".join((*selected, rendered)) + suffix + "]") <= max_chars:
            selected.append(rendered)
            selected_count += count
    omitted = total - selected_count
    result = "[" + "; ".join(selected)
    if omitted:
        result += f"; omitted={omitted}"
    result += "]"
    if len(result) <= max_chars:
        return result
    # The prefix itself consumed the detail budget. Retain an explicit omitted count whenever
    # even the compact representation cannot fit.
    return f"[omitted={total}]"[:max_chars]


def _recoverable_service_ambiguity(
    record: FileRecord, *, affected_sonarr_series: frozenset[int] | None = None
) -> bool:
    """Retry only known waits after a fresh complete source match, never review holds.

    Source exclusions describe a previous failed match, not immutable source
    drift. Current matching must succeed before either caller can reopen them.
    """
    if (
        record.state is not FileState.AMBIGUOUS
        or record.campaign_id is not None
        or record.cohort is Cohort.CAMPAIGN
    ):
        return False
    if record.state_detail.startswith("target_unmapped:"):
        # Full/legacy scans retry absent mappings. Incremental activations retry
        # only affected series; metadata and source readiness are independent.
        return affected_sonarr_series is None or record.sonarr_series_id in affected_sonarr_series
    if record.state_detail.startswith("target_anime_not_cached:"):
        return True
    category, _, rest = record.state_detail.partition(":")
    reason, separator, _ = rest.partition(":")
    # Every emitted exclusion describes a condition that the current complete
    # matcher must independently disprove. This is not permission to retry while
    # that exclusion still applies, or to erase an unknown/operator review hold.
    return (
        category == "source_exclusion"
        and bool(separator)
        and reason in {reason.value for reason in ExclusionReason}
    )


def _preserve_unaffected_service_unresolved(
    deps: DaemonDeps,
    matches: Sequence[Any],
    affected_sonarr_series: frozenset[int] | None,
    *,
    recover_metadata_waits: bool = False,
) -> frozenset[int]:
    if not deps.service_mode:
        return frozenset()
    preserved: set[int] = set()
    for match in matches:
        record = deps.store.get_file(match.shoko_file_id)
        if record is None:
            continue
        if (
            deps.store.mapping_target_quarantined(match.shoko_file_id)
            or record.state_detail.startswith("service_mapping_target_changed")
        ) or (
            record.state in (FileState.AMBIGUOUS, FileState.CONFLICT)
            and not (
                recover_metadata_waits
                and _recoverable_service_ambiguity(
                    record, affected_sonarr_series=affected_sonarr_series
                )
            )
            and (
                recover_metadata_waits
                or (
                    affected_sonarr_series is not None
                    and record.sonarr_series_id not in affected_sonarr_series
                )
            )
        ):
            preserved.add(match.shoko_file_id)
    return frozenset(preserved)


def _mapping_target_key(
    tvdb: int,
    season: int,
    episode: int,
    absolute: int | None,
    anime: int,
    number: int,
    episode_type: str,
) -> str:
    return json.dumps((tvdb, season, episode, absolute, anime, number, episode_type))


def _source_targets(
    match: Any, tvdb: int, active: ActiveMappings
) -> dict[str, tuple[int | None, int | None]]:
    """Collect mapping identities before Shoko metadata I/O can defer the file."""
    targets: dict[str, tuple[int | None, int | None]] = {}
    for episode in match.sonarr_file.episodes:
        answer = resolve_mapping(
            active.index,
            tvdb,
            episode.season_number,
            episode.episode_number,
            episode.absolute_episode_number,
        )
        if not answer.targets:
            return {}
        for target in answer.targets:
            key = _mapping_target_key(
                tvdb,
                episode.season_number,
                episode.episode_number,
                episode.absolute_episode_number,
                target.anidb_id,
                target.anidb_episode_number,
                target.episode_type.value,
            )
            targets[key] = (None, None)
    return targets


def _resolved_targets(plan: LinkPlan) -> dict[str, tuple[int | None, int | None]]:
    combined: Sequence[LinkTarget | PendingTarget] = (*plan.targets, *plan.pending_targets)
    return {
        _mapping_target_key(
            target.tvdb_series,
            target.season_number,
            target.episode_number,
            target.absolute_episode_number,
            target.anidb_anime_id,
            target.anidb_episode_number,
            target.episode_type,
        ): (target.anidb_episode_id, getattr(target, "shoko_episode_id", None))
        for target in combined
    }


def _source_target_conflicts(
    deps: DaemonDeps,
    matches: Sequence[Any],
    series_tvdb: Mapping[int, int],
    active: ActiveMappings,
    affected_sonarr_series: frozenset[int] | None,
) -> tuple[frozenset[int], Mapping[int, tuple[tuple[int, int, int, int | None], ...] | None]]:
    snapshots: dict[int, dict[str, tuple[int | None, int | None]]] = {}
    conflicts: set[int] = set()
    verified_drift_positions: dict[int, tuple[tuple[int, int, int, int | None], ...] | None] = {}
    for match in matches:
        file_id = match.shoko_file_id
        record = deps.store.get_file(file_id)
        if record is None or record.campaign_id is not None or record.cohort is Cohort.CAMPAIGN:
            continue  # Campaigns own their immutable approved plan and quarantine accounting.
        if deps.service_mode and record.state is FileState.VERIFIED:
            # A verified row is diagnostic-only. Its durable source positions are compared below;
            # do not rebuild or enrich the immutable snapshot from the Unrecognized listing.
            continue
        tvdb = series_tvdb.get(match.sonarr_file.series_id)
        if tvdb is None:
            continue
        prior = deps.store.get_meta(f"file_mapping_targets:v1:{file_id}")
        if deps.store.mapping_target_quarantined(file_id) or (
            prior is None
            and (
                record.state in (FileState.PLANNED, FileState.MAPPED)
                or record.state_detail.startswith("service_mapping_target_changed")
            )
        ):
            # Upgrade from a version that never persisted its planned destinations:
            # their identity cannot be reconstructed safely from today's mapping.
            conflicts.add(file_id)
            continue
        targets = _source_targets(match, tvdb, active)
        if targets:
            snapshots[file_id] = targets
        elif deps.store.get_meta(f"file_mapping_targets:v1:{file_id}") is not None:
            conflicts.add(file_id)
    if deps.service_mode:
        for record in deps.store.files_in_state(FileState.VERIFIED):
            if record.campaign_id is not None or record.cohort is Cohort.CAMPAIGN:
                continue
            raw = deps.store.get_meta(f"file_mapping_targets:v1:{record.shoko_file_id}")
            changed_positions = (
                None if raw is None else _changed_frozen_target_positions(raw, active)
            )
            if changed_positions:
                # Compare only positions that this durable verified file actually resolved.
                # This is read-only evidence: never reconstruct a path or candidate from it.
                conflicts.add(record.shoko_file_id)
                verified_drift_positions[record.shoko_file_id] = changed_positions
            elif changed_positions is None and (
                affected_sonarr_series is not None
                and record.sonarr_series_id in affected_sonarr_series
            ):
                # A legacy/malformed snapshot cannot prove which positions were previously
                # linked. Keep the historical conservative affected-series warning.
                conflicts.add(record.shoko_file_id)
                verified_drift_positions[record.shoko_file_id] = None
    conflicts.update(deps.store.remember_mapping_targets_bulk(snapshots))
    return frozenset(conflicts), MappingProxyType(verified_drift_positions)


def _changed_frozen_target_positions(  # noqa: PLR0911, PLR0912 - fail-closed checks stay explicit
    encoded: str, active: ActiveMappings
) -> tuple[tuple[int, int, int, int | None], ...] | None:
    """Return changed frozen source positions, or None when legacy evidence is malformed.

    The snapshot is a diagnostic record of what a verified link used. It is never rewritten or
    treated as current path/scope evidence. Only those exact source positions are queried in the
    active mapping, so unrelated positions elsewhere in the same TVDB series cannot create drift.
    """
    if len(encoded) > MAX_VERIFIED_MAPPING_SNAPSHOT_CHARS:
        return None
    try:
        document = json.loads(encoded)
    except (RecursionError, TypeError, ValueError):
        return None
    if not isinstance(document, dict) or not document:
        return None

    previous: dict[tuple[int, int, int, int | None], set[tuple[int, int, str]]] = {}
    for source, resolved_ids in document.items():
        if (
            not isinstance(source, str)
            or not isinstance(resolved_ids, list)
            or len(resolved_ids) != MAPPING_RESOLVED_ID_COUNT
        ):
            return None
        if any(
            value is not None and (type(value) is not int or value <= 0) for value in resolved_ids
        ):
            return None
        try:
            values = json.loads(source)
        except (RecursionError, TypeError, ValueError):
            return None
        if not isinstance(values, list) or len(values) != MAPPING_TARGET_KEY_PARTS:
            return None
        tvdb, season, episode, absolute, anime, number, episode_type = values
        if (
            type(tvdb) is not int
            or tvdb < 1
            or type(season) is not int
            or season < 0
            or type(episode) is not int
            or episode < 1
            or (absolute is not None and (type(absolute) is not int or absolute < 1))
            or type(anime) is not int
            or anime < 1
            or type(number) is not int
            or number < 1
            or not isinstance(episode_type, str)
        ):
            return None
        try:
            EpisodeType(episode_type)
        except ValueError:
            return None
        position = (tvdb, season, episode, absolute)
        previous.setdefault(position, set()).add((anime, number, episode_type))
    if not previous or len({position[0] for position in previous}) != 1:
        return None

    changed: list[tuple[int, int, int, int | None]] = []
    for position, frozen_targets in previous.items():
        answer = resolve_mapping(active.index, *position)
        active_targets = (
            {
                (target.anidb_id, target.anidb_episode_number, target.episode_type.value)
                for target in answer.targets
            }
            if answer.status is ResolutionStatus.RESOLVED
            else set()
        )
        if active_targets != frozen_targets:
            changed.append(position)
    return tuple(
        sorted(
            changed,
            key=lambda position: (
                position[0],
                position[1],
                position[2],
                -1 if position[3] is None else position[3],
            ),
        )
    )


def _record_service_verified_mapping_drifts(
    deps: DaemonDeps,
    changed_positions_by_file: Mapping[int, tuple[tuple[int, int, int, int | None], ...] | None],
    active: ActiveMappings,
    now_iso: str,
) -> tuple[str, ...]:
    """Persist and report only verified service files with changed frozen targets."""
    if not deps.service_mode:
        return ()
    drift: list[str] = []
    for file_id, changed_positions in sorted(changed_positions_by_file.items()):
        record = deps.store.get_file(file_id)
        if (
            record is None
            or record.state is not FileState.VERIFIED
            or record.campaign_id is not None
            or record.cohort is Cohort.CAMPAIGN
        ):
            continue
        if changed_positions is None:
            reason = "frozen target evidence unavailable after an affected mapping change"
        else:
            positions = ", ".join(
                f"TVDB {tvdb} S{season}E{episode}"
                + (f" (absolute {absolute})" if absolute is not None else "")
                for tvdb, season, episode, absolute in changed_positions
            )
            reason = f"changed mapping at {positions}"
        # A corrupt pre-existing marker is not permission to stall service reconciliation. The
        # current report remains visible; the durable marker stays untouched.
        with contextlib.suppress(StateError):
            deps.store.record_service_mapping_drift(
                file_id,
                previous_targets_digest=record.mapping_targets_digest,
                active_targets_digest=active.targets_digest,
                previous_structure_digest=record.mapping_structure_digest,
                active_structure_digest=active.structure_digest,
                mapping_revision=str(active.revision),
                reason=reason,
                iso=now_iso,
            )
        drift.append(f"existing_link drift: verified file {file_id} {reason} (never unlinked)")
    return tuple(drift)


def _filter_resolved_target_drift(
    deps: DaemonDeps, plans: dict[int, LinkPlan], skip_reasons: dict[int, str], now_iso: str
) -> None:
    snapshots = {}
    for file_id, plan in plans.items():
        record = deps.store.get_file(file_id)
        if (
            record is not None
            and record.campaign_id is None
            and record.cohort is not Cohort.CAMPAIGN
        ):
            snapshots[file_id] = _resolved_targets(plan)
    for file_id in deps.store.remember_mapping_targets_bulk(snapshots):
        skip_reasons[file_id] = "service_mapping_target_changed"
        _quarantine_mapping_target(deps, file_id, now_iso)
        del plans[file_id]


def _quarantine_mapping_target(deps: DaemonDeps, file_id: int, now_iso: str) -> None:
    deps.store.quarantine_mapping_targets((file_id,), iso=now_iso)


def _quarantine_legacy_mapping_targets(deps: DaemonDeps, now_iso: str) -> frozenset[int]:
    """Classify legacy resolution evidence before any retry erases state/details."""
    return deps.store.quarantine_unfrozen_mapping_targets(iso=now_iso)


def _resolve_match_plan(
    resolver: EpisodeIdResolver,
    match: Any,
    tvdb: int,
    active: ActiveMappings,
    discovery: ShokoDiscovery,
    policy: RunPolicy,
    *,
    scope_fingerprint: str | None = None,
) -> LinkPlan | _MetadataWait | PlanSkip | _ResolutionDeferral:
    resolutions = _episode_resolutions(
        resolver, match, tvdb, active, recover_missing_anime=scope_fingerprint is not None
    )
    if resolutions is _ResolutionDeferral.TRANSPORT_FAILURE:
        return resolutions
    if isinstance(resolutions, str):
        return PlanSkip(resolutions, resolutions)
    demand: MetadataDemand | None = None
    if scope_fingerprint is not None and any(
        item.metadata_status is not None for item in resolutions
    ):
        frozen_demand = freeze_metadata_demand(
            match,
            resolutions,
            scope_fingerprint=scope_fingerprint,
            policy=policy,
            targets_digest=active.targets_digest,
            structure_digest=active.structure_digest,
            shoko_paths=_resolve_shoko_paths(discovery, match.shoko_file_id),
            mapping_revision=str(active.revision),
            overrides_active=active.overrides_applied,
        )
        if isinstance(frozen_demand, PlanSkip):
            return frozen_demand
        demand = frozen_demand
        if any(
            item.metadata_status is EpisodeResolutionStatus.ANIME_NOT_CACHED for item in resolutions
        ):
            return _MetadataWait(demand, None)
    plan = freeze_plan(
        match,
        resolutions,
        policy=policy,
        targets_digest=active.targets_digest,
        structure_digest=active.structure_digest,
        shoko_paths=_resolve_shoko_paths(discovery, match.shoko_file_id),
        mapping_revision=str(active.revision),
        overrides_active=active.overrides_applied,
    )
    if isinstance(plan, PlanSkip):
        return plan
    return _MetadataWait(demand, plan) if demand is not None else plan


def _metadata_scope_fingerprint(settings: Settings, policy: RunPolicy) -> str:
    """Current selected scope only; no API keys, timing or metadata observations."""
    scope = (
        policy.value,
        settings.sonarr.url,
        settings.shoko.url,
        sorted(settings.sonarr.allowed_roots),
        sorted(settings.shoko.allowed_roots),
        sorted((mapping.sonarr, mapping.shoko) for mapping in settings.path_mappings),
        settings.anime_only,
    )
    return hashlib.sha256(json.dumps(scope, separators=(",", ":")).encode("utf-8")).hexdigest()


def _metadata_candidate_allowed(deps: DaemonDeps, match: Any) -> bool:
    record = deps.store.get_file(match.shoko_file_id)
    if record is None:
        return True
    source = match.sonarr_file
    return (
        record.campaign_id is None
        and record.cohort is not Cohort.CAMPAIGN
        and not deps.store.mapping_target_quarantined(match.shoko_file_id)
        and (
            _recoverable_service_ambiguity(record)
            or record.state
            in {
                FileState.DISCOVERED,
                FileState.MATCHED,
                FileState.MAPPED,
                FileState.PLANNED,
                FileState.WAITING,
            }
        )
        and (record.sonarr_series_id, record.sonarr_file_id, record.sonarr_path, record.sonarr_size)
        == (source.series_id, source.episode_file_id, source.path, source.size)
    )


def _record_exclusions(
    deps: DaemonDeps, result: ShokoMatchResult, discovery: ShokoDiscovery, now_iso: str
) -> tuple[dict[int, str], Mapping[str, int], int, dict[str, Any]]:
    """Retain categorized exclusions without admitting them to the write plan."""
    skipped: dict[int, str] = {}
    files = {file.id: file for file in discovery.files}
    counts = {reason.value: 0 for reason in ExclusionReason}
    scope_excluded = 0
    examples: list[dict[str, Any]] = []
    known = _tracked_file_ids(deps.store)
    for exclusion in result.exclusions:
        file_id = exclusion.shoko_file_id
        source = exclusion.sonarr_file
        reason = exclusion.reason.value
        counts[reason] += 1
        if exclusion.reason in SCOPE_EXCLUSION_REASONS:
            scope_excluded += 1
        safe_detail = diagnostic_text(exclusion.detail, limit=MAX_EXCLUSION_EXAMPLE_CHARS)
        detail = f"exclusion:{reason}: {safe_detail}"
        skipped[file_id] = detail
        if len(examples) < MAX_EXCLUSION_EXAMPLES:
            examples.append(
                {
                    "shoko_file_id": file_id,
                    "reason": reason,
                    "detail": safe_detail,
                }
            )

        file = files.get(file_id)
        record = deps.store.get_file(file_id)
        if record is None and source is not None and file is not None:
            cohort = deps.store.classify_new(
                file_id,
                sonarr_series_id=source.series_id,
                sonarr_file_id=source.episode_file_id,
                known_shoko_file_ids=known,
            )
            deps.store.record_file(
                file_id,
                sonarr_series_id=source.series_id,
                sonarr_file_id=source.episode_file_id,
                sonarr_path=source.path,
                sonarr_size=source.size,
                cohort=cohort,
                iso=now_iso,
                identity=FileIdentitySnapshot(
                    ed2k=file.hashes.ed2k,
                    sha1=file.hashes.sha1,
                    md5=file.hashes.md5,
                    crc32=file.hashes.crc32,
                    shoko_paths=tuple(sorted({loc.absolute_path for loc in file.locations})),
                ),
            )
            known = known | {file_id}
        record = deps.store.get_file(file_id)
        if record is not None and (
            source is None
            or (record.sonarr_series_id, record.sonarr_file_id)
            == (source.series_id, source.episode_file_id)
        ):
            mutable_states = {
                FileState.DISCOVERED,
                FileState.MATCHED,
                FileState.MAPPED,
                FileState.PLANNED,
                FileState.WAITING,
                FileState.AMBIGUOUS,
            }
            if record.state in mutable_states:
                state_detail = f"source_exclusion:{reason}: {safe_detail}"
                if record.state in (FileState.PLANNED, FileState.AMBIGUOUS):
                    _advance(deps, file_id, FileState.WAITING, state_detail, now_iso)
                _advance(deps, file_id, FileState.AMBIGUOUS, state_detail, now_iso)

    summary = {
        "version": 1,
        "total": len(result.exclusions),
        "counts": counts,
        "scope_excluded": scope_excluded,
        "examples": examples,
    }
    return skipped, MappingProxyType(counts), scope_excluded, summary


def _persist_reconciliation_summary(
    deps: DaemonDeps,
    result: ShokoMatchResult,
    discovery: ShokoDiscovery,
    now_iso: str,
    scan_context: str | None,
    exclusion_summary: dict[str, Any],
) -> None:
    # Complete matching partitions unique discovered files into one correspondence
    # match or one exclusion each. This describes source matching, never link success.
    if not result.eligible:
        return
    files = {file.id: file for file in discovery.files}
    matched_ids = {match.shoko_file_id for match in result.matches}
    excluded_ids = {item.shoko_file_id for item in result.exclusions}
    if (
        len(files) != len(discovery.files)
        or len(excluded_ids) != len(result.exclusions)
        or len(matched_ids) != len(result.matches)
        or matched_ids & excluded_ids
        or matched_ids | excluded_ids != files.keys()
    ):
        return
    if scan_context is not None and scan_context == scan_context_identity(
        deps.store, deps.settings.sonarr.url, deps.settings.shoko.url, settings=deps.settings
    ):
        times = [now_iso, deps.now()]
        if discovery.progress is not None:
            times.extend(discovery.progress.fetched_at.values())
        try:
            observed = sorted(_parse_iso(value) for value in times)
        except (TypeError, ValueError):
            return
        exclusion_summary["scan"] = scan_summary_payload(
            ScanSummary(
                context_identity=scan_context,
                observed_from=observed[0].isoformat(),
                observed_until=observed[-1].isoformat(),
                reported_unrecognized=discovery.total_reported,
                observed_files=len({file.id for file in discovery.files}),
                matched_files=len(result.matches),
                excluded_files=len(result.exclusions),
                outside_scope_files=exclusion_summary["scope_excluded"],
            )
        )
    rows = []
    for exclusion in result.exclusions:
        file = files[exclusion.shoko_file_id]
        paths = tuple(safe_display_path(location.absolute_path) for location in file.locations)
        complete = len(paths) <= MAX_DISPLAY_LOCATIONS and all(path is not None for path in paths)
        source = exclusion.sonarr_file
        rows.append(
            ScanExclusionRow(
                exclusion.shoko_file_id,
                exclusion.reason.value,
                diagnostic_text(exclusion.detail, limit=MAX_EXCLUSION_EXAMPLE_CHARS),
                (safe_display_path(source.path) or "") if source is not None else "",
                tuple(path for path in paths[:MAX_DISPLAY_LOCATIONS] if path is not None),
                complete,
            )
        )
    # Rows are display evidence only; atomically replace the latest complete set,
    # never the durable file/send history. No summary is published before this step.
    with contextlib.suppress(WriteDisabledError, StateError):
        deps.store.replace_latest_scan_exclusions(exclusion_summary, rows)


def _resolution_deadline(deps: DaemonDeps, cycle_start_mono: float) -> float:
    """Leave half the remaining cycle for fresh preflight, submit and verification."""
    now = deps.monotonic()
    remaining = max(0.0, cycle_start_mono + deps.max_cycle_seconds - now)
    return now + remaining / 2


def _inventory_deadline(deps: DaemonDeps, cycle_start_mono: float) -> float:
    """Use at most half the remaining cycle for one bounded inventory slice."""
    now = deps.monotonic()
    remaining = max(0.0, cycle_start_mono + deps.max_cycle_seconds - now)
    return now + remaining / 2


def _load_shoko_discovery_progress(  # noqa: PLR0911
    deps: DaemonDeps, now_iso: str
) -> ShokoDiscoveryProgress | None:
    """Load a fresh, well-formed persisted Shoko page sweep, failing closed otherwise."""
    raw = deps.store.get_meta(META_SHOKO_DISCOVERY_PROGRESS)
    if raw is None:
        return None
    endpoint_identity = str(deps.shoko.client.base_url).rstrip("/")

    def invalidate() -> None:
        deps.store.clear_meta(META_SHOKO_DISCOVERY_PROGRESS)

    try:
        document = json.loads(raw)
        fields = {
            "version",
            "endpoint_identity",
            "folders_fingerprint",
            "total",
            "page_size",
            "cursor",
            "swept_pages",
            "pages",
            "fetched_at",
        }
        if (
            not isinstance(document, dict)
            or set(document) != fields
            or type(document["version"]) is not int
            or document["version"] != 1
            or not isinstance(document["endpoint_identity"], str)
            or not document["endpoint_identity"]
            or document["endpoint_identity"] != endpoint_identity
            or not isinstance(document["folders_fingerprint"], str)
            or len(document["folders_fingerprint"]) != DIGEST_HEX_CHARS
            or any(char not in "0123456789abcdef" for char in document["folders_fingerprint"])
            or type(document["total"]) is not int
            or document["total"] < 0
            or type(document["page_size"]) is not int
            or document["page_size"] <= 0
            or type(document["cursor"]) is not int
            or document["cursor"] < 0
            or not isinstance(document["swept_pages"], list)
            or not isinstance(document["pages"], dict)
            or not isinstance(document["fetched_at"], dict)
        ):
            invalidate()
            return None

        expected_pages = max(
            1,
            (document["total"] + document["page_size"] - 1) // document["page_size"],
        )
        pages: dict[int, tuple[Any, ...]] = {}
        for raw_page, rows in document["pages"].items():
            page = int(raw_page)
            if (
                str(page) != raw_page
                or page <= 0
                or page > expected_pages
                or not isinstance(rows, list)
                or any(not isinstance(row, dict) for row in rows)
            ):
                invalidate()
                return None
            pages[page] = tuple(rows)
        if set(document["fetched_at"]) != {str(page) for page in pages}:
            invalidate()
            return None
        swept_values = document["swept_pages"]
        if any(type(page) is not int or page <= 0 or page not in pages for page in swept_values):
            invalidate()
            return None
        if len(swept_values) != len(set(swept_values)) or document["cursor"] > expected_pages:
            invalidate()
            return None
        fetched_at = {int(page): value for page, value in document["fetched_at"].items()}
        if any(not isinstance(value, str) for value in fetched_at.values()):
            invalidate()
            return None
        now = _parse_iso(now_iso)
        max_cache_age = timedelta(hours=deps.settings.sync.full_reconcile_interval_hours)
        for timestamp in fetched_at.values():
            fetched = _parse_iso(timestamp)
            if fetched > now or now - fetched >= max_cache_age:
                invalidate()
                return None
        return ShokoDiscoveryProgress(
            endpoint_identity=document["endpoint_identity"],
            folders_fingerprint=document["folders_fingerprint"],
            total=document["total"],
            page_size=document["page_size"],
            cursor=document["cursor"],
            swept_pages=frozenset(swept_values),
            pages=pages,
            fetched_at=fetched_at,
        )
    except (KeyError, TypeError, ValueError):
        invalidate()
        return None


def _save_shoko_discovery_progress(
    deps: DaemonDeps, progress: ShokoDiscoveryProgress | None
) -> None:
    if progress is None:
        deps.store.clear_meta(META_SHOKO_DISCOVERY_PROGRESS)
        return
    document = {
        "version": 1,
        "endpoint_identity": progress.endpoint_identity,
        "folders_fingerprint": progress.folders_fingerprint,
        "total": progress.total,
        "page_size": progress.page_size,
        "cursor": progress.cursor,
        "swept_pages": sorted(progress.swept_pages),
        "pages": {str(page): list(rows) for page, rows in progress.pages.items()},
        "fetched_at": {str(page): value for page, value in progress.fetched_at.items()},
    }
    deps.store.set_meta(
        META_SHOKO_DISCOVERY_PROGRESS,
        json.dumps(document, sort_keys=True, separators=(",", ":")),
    )


def _collect_bounded_shoko_discovery(
    deps: DaemonDeps, cycle_start_mono: float, now_iso: str
) -> ShokoDiscovery:
    """Resume a bounded Shoko page sweep without reconciling stale or partial pages."""
    progress = _load_shoko_discovery_progress(deps, now_iso)
    discovery = collect_shoko_discovery(
        deps.shoko,
        cached_progress=progress,
        endpoint_identity=str(deps.shoko.client.base_url).rstrip("/"),
        deadline=_inventory_deadline(deps, cycle_start_mono),
        monotonic=deps.monotonic,
        observed_at=now_iso,
    )
    _save_shoko_discovery_progress(deps, discovery.progress)
    return discovery


def _collect_bounded_sonarr_inventory(deps: DaemonDeps, cycle_start_mono: float) -> SonarrInventory:
    """Resume a stable, rotating full-series inventory without trusting partial sweeps."""
    manifest, cursor, swept, encoded_fragments = deps.store.load_sonarr_inventory_cache()
    children: dict[int, tuple[tuple[Any, ...], tuple[Any, ...]]] = {}
    invalidated: set[int] = set()
    try:
        freshness_now = _parse_iso(deps.now())
    except ValueError:
        freshness_now = None
    max_cache_age = timedelta(hours=deps.settings.sync.full_reconcile_interval_hours)
    for series_id, encoded in encoded_fragments.items():
        try:
            document = json.loads(encoded)
        except ValueError:
            invalidated.add(series_id)
            continue
        if (
            isinstance(document, dict)
            and set(document) == {"episodes", "files", "fetched_at"}
            and isinstance(document["episodes"], list)
            and isinstance(document["files"], list)
            and isinstance(document["fetched_at"], str)
        ):
            try:
                fetched_at = _parse_iso(document["fetched_at"])
            except ValueError:
                invalidated.add(series_id)
                continue
            if (
                freshness_now is None
                or fetched_at > freshness_now
                or freshness_now - fetched_at >= max_cache_age
            ):
                invalidated.add(series_id)
                continue
            children[series_id] = (tuple(document["episodes"]), tuple(document["files"]))
        else:
            # Legacy fragments have no persisted observation time, so they cannot be
            # trusted across restarts.  New fragments remain resumable until the bound.
            invalidated.add(series_id)

    inventory = collect_sonarr_inventory(
        deps.sonarr,
        deps.settings,
        cached_manifest_hash=manifest,
        cached_series_children=children,
        series_cursor=cursor,
        swept_series_ids=frozenset(swept & children.keys()),
        series_deadline=_inventory_deadline(deps, cycle_start_mono),
        deadline=_inventory_deadline(deps, cycle_start_mono),
        monotonic=deps.monotonic,
    )
    if inventory.series_manifest_hash is None:
        return inventory

    invalidated.update(inventory.invalidated_series_ids)
    try:
        fetched_at_iso = deps.now()
        _parse_iso(fetched_at_iso)
    except ValueError:
        fetched_at_iso = None
    fetched_fragments = {}
    for series_id, payloads in (inventory.fetched_series_children or {}).items():
        if fetched_at_iso is None:
            invalidated.add(series_id)
            continue
        fetched_fragments[series_id] = json.dumps(
            {
                "episodes": list(payloads[0]),
                "files": list(payloads[1]),
                "fetched_at": fetched_at_iso,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    manifest_changed = manifest != inventory.series_manifest_hash
    deps.store.save_sonarr_inventory_cache(
        inventory.series_manifest_hash,
        inventory.series_cursor,
        () if inventory.refresh_sweep_complete else sorted(inventory.swept_series_ids),
        fetched_fragments,
        reset=manifest_changed,
        invalidated_series_ids=sorted(invalidated),
    )
    return inventory


def _resolution_order(
    deps: DaemonDeps,
    matches: Sequence[Any],
    policy: RunPolicy,
    limit: int | None,
    deadline: float | None,
    priority_file_ids: frozenset[int],
) -> Iterator[tuple[Any, bool]]:
    """Yield every safety check, but rotate a bounded set of remote resolutions.

    Cursor persistence deliberately precedes I/O: interrupted or unavailable anime
    metadata must not repeatedly starve later files. Explicitly approved campaign files
    lead the bounded window so unrelated backlog work cannot starve an active campaign.
    Frozen targets are independent of this scheduling marker and are never discarded by
    rotation.
    """
    cursor_key = f"reconcile_cursor:v1:{policy.value}"
    cursor = int(deps.store.get_meta(cursor_key) or "0") if limit is not None else 0
    ordered = sorted(
        matches,
        key=lambda m: (
            m.shoko_file_id not in priority_file_ids,
            m.shoko_file_id <= cursor,
            m.shoko_file_id,
        ),
    )
    count = 0
    for match in ordered:
        record = deps.store.get_file(match.shoko_file_id)
        terminal = record is not None and record.state in (
            _SEND_RECOVERY_STATES | {FileState.VERIFIED, FileState.BLOCKED_SAFETY}
        )
        due = (
            not terminal
            and (limit is None or count < limit)
            and (deadline is None or deps.monotonic() < deadline)
        )
        if due:
            count += 1
            if limit is not None:
                deps.store.set_meta(cursor_key, str(match.shoko_file_id))
        yield match, due


def _remember_series_display(
    deps: DaemonDeps,
    inventory: SonarrInventory,
    matches: Sequence[Any],
    identity_drifts: Mapping[int, Any],
) -> None:
    """Display labels from the exact selected inventory; never infer identity from a title."""
    series_titles = {item.series.id: item.series.title for item in inventory.eligible_series}
    source_key = metadata_source_key(deps.shoko)
    display_scope = _metadata_scope_fingerprint(deps.settings, RunPolicy.SERVICE)
    deps.store.remember_file_series_names_bulk(
        [
            FileSeriesDisplay(
                match.shoko_file_id,
                series_id=match.sonarr_file.series_id,
                episode_file_id=match.sonarr_file.episode_file_id,
                path=match.sonarr_file.path,
                title=series_titles.get(match.sonarr_file.series_id, ""),
            )
            for match in matches
            if match.shoko_file_id not in identity_drifts
        ],
        source_key=source_key,
        scope_fingerprint=display_scope,
    )


def _reconcile(  # noqa: PLR0915 - keep safety checks and durable state transitions in phase order
    deps: DaemonDeps,
    inventory: SonarrInventory,
    discovery: ShokoDiscovery,
    active: ActiveMappings,
    now_iso: str,
    *,
    policy: RunPolicy = RunPolicy.NEW_ARRIVAL,
    campaign_quarantine_limit: int | None = None,
    affected_sonarr_series: frozenset[int] | None = None,
    resolution_limit: int | None = None,
    resolution_deadline: float | None = None,
    resolution_priority_file_ids: frozenset[int] = frozenset(),
    scan_context: str | None = None,
) -> _ReconcileResult:
    series_tvdb = {
        item.series.id: item.series.tvdb_id
        for item in inventory.eligible_series
        if item.series.tvdb_id is not None
    }
    match_result = match_shoko_files(
        discovery, inventory.files, inventory.verdict.complete, deps.settings
    )
    preserve_unresolved = _preserve_unaffected_service_unresolved(
        deps,
        match_result.matches,
        affected_sonarr_series,
        recover_metadata_waits=policy is RunPolicy.SERVICE,
    )
    metadata_eligible = (
        frozenset(
            match.shoko_file_id
            for match in match_result.matches
            if _metadata_candidate_allowed(deps, match)
        )
        if deps.service_mode and policy is RunPolicy.SERVICE
        else frozenset()
    )
    cohorts, inserted, identity_drifts = _record_and_match(
        deps,
        match_result.matches,
        now_iso,
        preserve_unresolved=preserve_unresolved,
    )
    _remember_series_display(deps, inventory, match_result.matches, identity_drifts)
    resolver = EpisodeIdResolver(deps.shoko, clock=deps.monotonic, deadline=resolution_deadline)
    plans: dict[int, LinkPlan] = {}
    metadata_demands: dict[int, MetadataDemand] = {}
    series_titles = {item.series.id: item.series.title for item in inventory.eligible_series}
    skip_reasons, exclusion_counts, scope_excluded_count, exclusion_summary = _record_exclusions(
        deps, match_result, discovery, now_iso
    )
    fresh_campaign_quarantines: set[int] = set()
    deferred_campaign_identity_details: dict[int, tuple[str, str]] = {}
    mapped_ids: list[int] = []
    resolution_deferred_ids: set[int] = set()
    target_conflicts, verified_drift_positions = _source_target_conflicts(
        deps, match_result.matches, series_tvdb, active, affected_sonarr_series
    )
    # Exact frozen destinations isolate the source positions changed for verified files,
    # including files Shoko omits from its Unrecognized listing. Legacy rows without usable
    # evidence retain a conservative affected-series warning.
    existing_link_drift = _record_service_verified_mapping_drifts(
        deps,
        verified_drift_positions,
        active,
        now_iso,
    )
    for match, resolution_due in _resolution_order(
        deps,
        match_result.matches,
        policy,
        resolution_limit,
        resolution_deadline,
        resolution_priority_file_ids,
    ):
        file_id = match.shoko_file_id
        identity_drift = identity_drifts.get(file_id)
        if identity_drift is not None:
            reason, detail = identity_drift
            skip_reasons[file_id] = reason
            campaign_quarantine_available = (
                campaign_quarantine_limit is None
                or len(fresh_campaign_quarantines) < campaign_quarantine_limit
            )
            if _quarantine_source_identity_drift(
                deps,
                file_id,
                reason,
                detail,
                now_iso,
                allow_campaign_quarantine=campaign_quarantine_available,
                deferred_campaign_details=deferred_campaign_identity_details,
            ):
                fresh_campaign_quarantines.add(file_id)
            continue
        if file_id in preserve_unresolved:
            continue
        tvdb_series = series_tvdb.get(match.sonarr_file.series_id)
        if tvdb_series is None:
            skip_reasons[file_id] = "no_tvdb_series"
            _advance(deps, file_id, FileState.AMBIGUOUS, "no tvdb series", now_iso)
            continue
        if file_id in target_conflicts:
            skip_reasons[file_id] = "service_mapping_target_changed"
            _quarantine_mapping_target(deps, file_id, now_iso)
            continue
        if not resolution_due:
            resolution_deferred_ids.add(file_id)
            continue
        frozen = _resolve_match_plan(
            resolver,
            match,
            tvdb_series,
            active,
            discovery,
            policy,
            scope_fingerprint=(
                _metadata_scope_fingerprint(deps.settings, policy)
                if file_id in metadata_eligible
                else None
            ),
        )
        if frozen is _ResolutionDeferral.TRANSPORT_FAILURE:
            resolution_deferred_ids.add(file_id)
            skip_reasons[file_id] = "resolution_deferred:transport_failure"
            continue
        if isinstance(frozen, PlanSkip):
            skip_reasons[file_id] = frozen.reason
            _advance(deps, file_id, FileState.AMBIGUOUS, frozen.reason, now_iso)
            continue
        if isinstance(frozen, _MetadataWait):
            metadata_demands[file_id] = replace(
                frozen.demand,
                series_name=series_display_name(series_titles.get(match.sonarr_file.series_id)),
            )
            _advance(deps, file_id, FileState.WAITING, "pending_metadata", now_iso)
            if frozen.plan is not None:
                plans[file_id] = frozen.plan
            continue
        mapped_ids.append(file_id)
        if frozen.status is PlanStatus.PENDING_TARGETS:
            plans[file_id] = frozen
            continue
        plans[file_id] = frozen
    # P3b: the walk steps run bulk (one transaction each, per-edge validation
    # identical to per-file advance — illegal edges skipped).
    _filter_resolved_target_drift(deps, plans, skip_reasons, now_iso)
    metadata_demands = {
        file_id: demand
        for file_id, demand in metadata_demands.items()
        if file_id not in skip_reasons
    }
    pending = sum(plan.status is PlanStatus.PENDING_TARGETS for plan in plans.values()) + sum(
        file_id not in plans for file_id in metadata_demands
    )
    planned_ids = [file_id for file_id, plan in plans.items() if plan.status is PlanStatus.LINKABLE]
    deps.store.advance_bulk(mapped_ids, FileState.MAPPED, "mapped", iso=now_iso)
    deps.store.advance_bulk(planned_ids, FileState.PLANNED, "planned", iso=now_iso)
    backlog, new_arrival = _cohort_counts(
        [(file_id, cohort) for file_id, cohort in cohorts.items()]
    )
    _persist_reconciliation_summary(
        deps, match_result, discovery, now_iso, scan_context, exclusion_summary
    )
    return _ReconcileResult(
        inserted=inserted,
        backlog=backlog,
        new_arrival=new_arrival,
        pending=pending,
        plans=plans,
        skip_reasons=skip_reasons,
        existing_link_drift=existing_link_drift,
        fresh_campaign_quarantines=frozenset(fresh_campaign_quarantines),
        campaign_identity_drift_details=MappingProxyType(deferred_campaign_identity_details),
        exclusion_counts=exclusion_counts,
        scope_excluded_count=scope_excluded_count,
        resolution_deferred_file_ids=frozenset(resolution_deferred_ids),
        metadata_demands=MappingProxyType(metadata_demands),
    )


# ------------------------------------------------------------------ generation change


def _sonarr_to_tvdb(inventory: SonarrInventory) -> dict[int, int]:
    return {
        item.series.id: item.series.tvdb_id
        for item in inventory.eligible_series
        if item.series.tvdb_id is not None
    }


def _reopen_generation_unresolved(
    deps: DaemonDeps, now_iso: str, affected: frozenset[int]
) -> list[str]:
    """Re-open affected unresolved files and quarantine frozen service targets."""
    _quarantine_legacy_mapping_targets(deps, now_iso)
    drift: list[str] = []
    for state, target, reason in (
        (FileState.AMBIGUOUS, FileState.WAITING, "mapping_reactivated"),
        (FileState.CONFLICT, FileState.WAITING, "mapping_reactivated"),
    ):
        for record in deps.store.files_in_state(state):
            if record.sonarr_series_id not in affected:
                continue
            if deps.service_mode and (
                record.campaign_id is not None or record.cohort is Cohort.CAMPAIGN
            ):
                continue
            if deps.store.mapping_target_quarantined(record.shoko_file_id) or (
                state is FileState.CONFLICT
                and record.state_detail.startswith("service_mapping_target_changed")
            ):
                continue
            with contextlib.suppress(TransitionError):
                deps.store.advance(record.shoko_file_id, target, reason, iso=now_iso)
    if deps.service_mode:
        for record in deps.store.files_in_state(FileState.WAITING):
            if (
                record.sonarr_series_id in affected
                and record.campaign_id is None
                and record.cohort is not Cohort.CAMPAIGN
                and record.state_detail in {"mapping_drift", "mapping_changed"}
            ):
                with contextlib.suppress(TransitionError):
                    _quarantine_mapping_target(deps, record.shoko_file_id, now_iso)
                    drift.append(
                        f"mapping_changed: waiting file {record.shoko_file_id} quarantined"
                    )
    for record in deps.store.files_in_state(FileState.PLANNED):
        if record.sonarr_series_id not in affected:
            continue
        if deps.service_mode and (
            record.campaign_id is not None or record.cohort is Cohort.CAMPAIGN
        ):
            continue
        if deps.store.get_meta(f"file_mapping_targets:v1:{record.shoko_file_id}") is not None:
            continue  # Compare the actual per-file destinations during reconciliation.
        with contextlib.suppress(TransitionError):
            _quarantine_mapping_target(deps, record.shoko_file_id, now_iso)
            drift.append(f"mapping_changed: file {record.shoko_file_id} quarantined from planned")
    return drift


def _apply_generation_change(
    deps: DaemonDeps,
    now_iso: str,
    affected: frozenset[int],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Re-evaluate affected candidates when durable mapping semantics change.

    ``affected`` is the set of Sonarr series whose effective mapping target set changed.

    * files in ``ambiguous``/``conflict`` in an affected series → ``waiting`` (prompt
      re-evaluation this cycle);
    * ``planned`` files without durable destinations are conservatively quarantined;
      files with frozen destinations compare their actual source→target sets during
      reconciliation and quarantine only changes;
    * ``verified`` service files are compared against their frozen per-file destinations during
      reconciliation; only a file whose targets changed gets a durable mapping-drift marker.
      Advanced mode retains its series-level report-only behavior.

    The resolved-ID cache is per-cycle (a new generation is a new cycle), so no
    in-cycle cache invalidation is needed here.
    """
    drift = _reopen_generation_unresolved(deps, now_iso, affected)
    existing_link: list[str] = []
    for record in deps.store.files_in_state(FileState.VERIFIED):
        if record.sonarr_series_id in affected:
            if deps.service_mode:
                # In service mode the reconciliation compares this file's frozen source
                # destinations below; a series-level semantic change alone does not mean
                # every episode's existing XRefs have drifted.
                continue
            existing_link.append(
                f"existing_link drift: verified file {record.shoko_file_id} in a "
                "changed mapping series (never unlinked)"
            )
    return tuple(drift), tuple(existing_link)


# ------------------------------------------------------------------ apply loop


def _xref_fetcher(
    endpoint: ApiEndpoint,
    file_id: int,
    *,
    expected_ed2k: str | None = None,
    expected_size: int | None = None,
    deadline: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> Callable[[], ShokoFileXRefs | None]:
    """Build the verification poll: ``GET File/{id}?include=XRefs`` (read-only).

    Expected ED2K and size bind the same File response to the frozen file before
    its XRefs can be used as verification evidence.
    """

    def fetch() -> ShokoFileXRefs | None:
        try:
            status, payload = get_bounded_json(
                endpoint,
                f"api/v3/File/{file_id}",
                {"include": XREF_INCLUDE_FLAGS},
                deadline=deadline,
                clock=clock,
            )
        except (BoundedJsonError, HttpPolicyError, httpx.HTTPError):
            return None
        if not MIN_SUCCESS_STATUS <= status < MAX_SUCCESS_STATUS:
            return None
        if expected_ed2k is not None:
            expected = canonical_ed2k(expected_ed2k)
            entry = payload if isinstance(payload, Mapping) else None
            hashes = entry.get("Hashes") if entry is not None else None
            current_ed2k = (
                canonical_ed2k(hashes.get("ED2K")) if isinstance(hashes, Mapping) else None
            )
            if expected is None or current_ed2k != expected:
                return None
        observed = parse_file_xrefs(payload)
        if observed is not None and expected_size is not None and observed.size != expected_size:
            return None
        return observed

    return fetch


@dataclass(frozen=True)
class _ApplyOutcome:
    submitted: int
    deferred: int
    reasons: tuple[str, ...]
    drift: tuple[str, ...]
    verified: bool = False


def _preflight_failure_outcome(
    deps: DaemonDeps,
    file_id: int,
    verdict: PreflightVerdict,
    now_iso: str,
    *,
    attempted: FileRecord | None,
    plan: LinkPlan,
) -> _ApplyOutcome:
    """Defer only wholly retryable verdicts; persist every hard block for operator review."""
    # The pre-I/O snapshot must describe the attempted plan, not a replaced source.
    if attempted is not None and (
        attempted.sonarr_series_id,
        attempted.sonarr_file_id,
        attempted.sonarr_path,
        attempted.sonarr_size,
        canonical_ed2k(attempted.identity.ed2k) if attempted.identity is not None else None,
    ) != (
        plan.sonarr_series_id,
        plan.sonarr_episode_file_id,
        plan.sonarr_path,
        plan.sonarr_size,
        canonical_ed2k(plan.shoko_ed2k),
    ):
        attempted = None
    if preflight_reasons_are_retryable(verdict.reasons):
        reason = verdict.reasons[0].value
        if attempted is not None:
            deps.store.record_preflight_deferral(
                attempted,
                expected_targets=_resolved_targets(plan),
                code=reason,
                message="; ".join(verdict.details) or "Retry later; no link was submitted",
                observed_iso=now_iso,
            )
        if PreflightReason.TARGET_UNAVAILABLE in verdict.reasons:
            detail = "; ".join(verdict.details) or (
                "Shoko target metadata is unavailable; target identity cannot be confirmed, "
                "retry later; no link was submitted"
            )
            return _ApplyOutcome(
                0,
                1,
                (f"preflight_deferred:{file_id}:target_unavailable: {detail}",),
                (),
            )
        if (
            PreflightReason.QUEUE_BLOCKED in verdict.reasons
            or PreflightReason.QUEUE_UNKNOWN in verdict.reasons
        ):
            return _ApplyOutcome(0, 1, (f"preflight_{reason}",), ())
        return _ApplyOutcome(0, 1, (f"preflight_deferred:{file_id}:{reason}",), ())

    hard_reason = primary_hard_preflight_reason(verdict.reasons)
    reason = hard_reason.value if hard_reason is not None else "preflight_blocked"
    detail = f"preflight_quarantine:{reason}"
    if verdict.details:
        detail += f": {'; '.join(verdict.details)}"
    record = deps.store.get_file(file_id)
    if hard_reason is PreflightReason.MAPPING_CHANGED and record is not None:
        deps.store.quarantine_mapping_targets((file_id,), iso=now_iso, reason=detail)
    elif record is not None and record.state is FileState.PLANNED:
        deps.store.advance(file_id, FileState.BLOCKED_SAFETY, detail, iso=now_iso)
    drift = (
        (f"mapping_changed: file {file_id} in-flight plan drift, no POST",)
        if hard_reason is PreflightReason.MAPPING_CHANGED
        else ()
    )
    return _ApplyOutcome(0, 0, (f"preflight_{reason}",), drift)


def _verify_and_close(
    deps: DaemonDeps,
    plan: LinkPlan,
    file_id: int,
    cycle_key: str,
    now_iso: str,
    *,
    deadline_mono: float | None = None,
) -> str:
    """Poll the XRef set to the exact planned set and close the loop (slot released on VERIFIED)."""
    deadline = deps.monotonic() + deps.verification_timeout_seconds
    # Keep the persisted/outcome timeout separate from the cycle I/O cutoff.
    # Standalone callers without a workflow budget retain the legacy poll contract.
    io_deadline = min(deadline, deadline_mono) if deadline_mono is not None else None
    expected_ed2k = canonical_ed2k(plan.shoko_ed2k)
    same_file_observation = False

    def fetch_xrefs() -> ShokoFileXRefs | None:
        nonlocal same_file_observation
        observed = _xref_fetcher(
            deps.shoko,
            file_id,
            expected_ed2k=expected_ed2k,
            expected_size=plan.shoko_size,
            deadline=io_deadline,
            clock=deps.monotonic,
        )()
        same_file_observation = observed is not None and observed.file_id == file_id
        return observed

    def advance(seconds: float) -> None:
        deps.advance(
            seconds
            if io_deadline is None
            else min(seconds, max(0.0, io_deadline - deps.monotonic()))
        )

    verification = (
        VerificationVerdict.UNKNOWN
        if expected_ed2k is None
        else verify_link(
            fetch_xrefs,
            plan,
            clock=deps.monotonic,
            deadline=deadline,
            poll_interval=deps.poll_interval_seconds,
            advance=advance,
            max_polls=deps.max_verify_polls,
        )
    )
    deps.store.record_verification(
        file_id,
        verification,
        iso=now_iso,
        inspection=deps.store.active_send_details(file_id) if same_file_observation else None,
    )
    if verification is VerificationVerdict.VERIFIED:
        deps.store.mark_completed(cycle_key, now_iso)
    return f"verification:{verification.value}"


def verify_and_close(
    deps: DaemonDeps,
    plan: LinkPlan,
    file_id: int,
    cycle_key: str,
    now_iso: str,
    *,
    deadline_mono: float | None = None,
) -> str:
    """Public inline verification wrapper (campaign apply reuses the daemon's poll loop)."""
    return _verify_and_close(deps, plan, file_id, cycle_key, now_iso, deadline_mono=deadline_mono)


def xref_fetcher(endpoint: ApiEndpoint, file_id: int) -> Callable[[], ShokoFileXRefs | None]:
    """Public alias for the XRef poll fetcher (campaign verification path)."""
    return _xref_fetcher(endpoint, file_id)


def inspect_send_state(
    deps: DaemonDeps, file_id: int, *, deadline_mono: float | None = None
) -> ShokoFileXRefs | None:
    """Read-only recovery inspection: ``GET File/{id}?include=XRefs`` (P1-3).

    Never a POST, never a DELETE — the hold branches verify, they do not repair.
    """
    record = deps.store.get_file(file_id)
    if record is None or record.identity is None:
        return None
    expected_ed2k = canonical_ed2k(record.identity.ed2k)
    if expected_ed2k is None:
        return None
    return _xref_fetcher(
        deps.shoko,
        file_id,
        expected_ed2k=expected_ed2k,
        expected_size=record.sonarr_size,
        deadline=deadline_mono,
        clock=deps.monotonic,
    )()


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
    hold-branch review classification for not-linked files). Terminal PARTIAL and
    CONTRADICTION results retain the reservation for explicit operator resolution.
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


def _recovery_poll_row(
    deps: DaemonDeps, row: Any, now_iso: str, *, deadline_mono: float | None = None
) -> _RecoveryRowOutcome:
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
    if deadline_mono is not None and deps.monotonic() >= deadline_mono:
        return _RecoveryRowOutcome(f"recovery_deadline:{file_id}")
    observed = inspect_send_state(deps, file_id, deadline_mono=deadline_mono)
    if observed is None or observed.file_id != file_id:
        with contextlib.suppress(TransitionError):
            if observed is None:
                deps.store.record_verification(file_id, VerificationVerdict.UNKNOWN, iso=now_iso)
        return _RecoveryRowOutcome(f"recovery_poll_unknown:{file_id}")
    verdict = _stored_triples_verdict(observed, file_id, expected, now=now, deadline=deadline)
    _advance(deps, file_id, FileState.VERIFYING, "recovery re-poll", now_iso)
    with contextlib.suppress(TransitionError):
        deps.store.record_verification(file_id, verdict, iso=now_iso, inspection=inspection)
    return _RecoveryRowOutcome(f"recovery_poll:{file_id}:{verdict.value}")


def _recovery_hold_row(
    deps: DaemonDeps, row: Any, now_iso: str, *, deadline_mono: float | None = None
) -> _RecoveryRowOutcome:
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
    if deadline_mono is not None and deps.monotonic() >= deadline_mono:
        return _RecoveryRowOutcome(f"recovery_deadline:{file_id}")
    observed = inspect_send_state(deps, file_id, deadline_mono=deadline_mono)
    if observed is None or observed.file_id != file_id:
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
        deps.store.record_verification(file_id, verdict, iso=now_iso, inspection=inspection)
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


def _file_recovery_phase(
    deps: DaemonDeps, now_iso: str, cycle_start_mono: float, *, max_rows: int
) -> tuple[tuple[str, ...], int]:
    """Consume persisted recovery rows BEFORE processing new candidates (P1-3/P2-2).

    (a) continue_polling re-polls with the persisted target triples; (b) hold rows are
    inspected read-only (verify/hold, never resubmit); (c) waiting/ambiguous re_evaluate
    rows are consumed by the cycle's own mapping re-walk (the reconcile pass
    advances persisted waiting/ambiguous rows through the evaluation edges);
    (d) replannable rows are voided (slot released, audit row ``voided``) and re-prepared
    in-cycle by reconcile + apply. Never issues a POST in any recovery branch.

    All branches, including prepared cleanup, share ``max_recovery_rows_per_cycle``
    and the cycle's monotonic budget (``max_cycle_seconds``), oldest-first by the immutable
    ``attempted_iso`` deadline anchor; the remainder is deferred (reported as
    ``recovery_deferred:<n>``). A huge verifying backlog drains across cycles and terminates
    instead of decoding terminal history or issuing one GET per persisted row.
    """
    raw_cursor = deps.store.get_meta(META_RECOVERY_CURSOR)
    try:
        cursor = max(0, int(raw_cursor or "0"))
    except ValueError:
        cursor = 0
    deadline = cycle_start_mono + deps.max_cycle_seconds
    batch = deps.store.file_recovery_batch(
        after_id=cursor, limit=max_rows if deps.monotonic() < deadline else 0
    )
    outcomes: list[_RecoveryRowOutcome] = []
    processed_count = 0
    for file_id in batch.ids:
        if deps.monotonic() >= deadline:
            break
        try:
            row = deps.store.get_file(file_id)
            if row is None:
                outcome = _RecoveryRowOutcome(f"recovery_missing:{file_id}")
            elif row.state is FileState.PREPARED_NOT_SENT:
                outcome = _recovery_replan_rows(deps, (row,), now_iso)[0]
            elif row.state in (FileState.REQUEST_MAY_HAVE_BEEN_SENT, FileState.INDETERMINATE_WRITE):
                outcome = _recovery_hold_row(deps, row, now_iso, deadline_mono=deadline)
            elif row.state in (FileState.VERIFYING, FileState.PARTIAL, FileState.CONFLICT):
                outcome = _recovery_poll_row(deps, row, now_iso, deadline_mono=deadline)
            else:
                outcome = _RecoveryRowOutcome(f"recovery_skipped:{file_id}")
        except (StateError, ValueError):
            outcome = _RecoveryRowOutcome(f"recovery_invalid:{file_id}")
        outcomes.append(outcome)
        processed_count += 1
        deps.store.set_meta(META_RECOVERY_CURSOR, str(file_id))
    deferred = batch.total - processed_count
    if deferred:
        outcomes.append(_RecoveryRowOutcome(f"recovery_deferred:{deferred}"))
    re_evaluate = deps.store.recovery_re_evaluate_count()
    if re_evaluate:
        outcomes.append(_RecoveryRowOutcome(f"recovery_re_evaluate:{re_evaluate}"))
    return tuple(item.reason for item in outcomes), processed_count


def _recovery_phase(
    deps: DaemonDeps, now_iso: str, cycle_start_mono: float, *, full: bool = True
) -> tuple[str, ...]:
    """Share one bounded read-only recovery allowance; alternate first service lane.

    Alternation also makes progress with a one-row light-cycle allowance when both
    operation kinds have long-running jobs. Each lane keeps its own durable cursor.
    """
    cap = (
        deps.max_recovery_rows_per_cycle if full else max(1, deps.max_recovery_rows_per_cycle // 2)
    )
    if not deps.service_mode:
        return _file_recovery_phase(deps, now_iso, cycle_start_mono, max_rows=cap)[0]
    reasons: list[str] = []
    metadata_first = deps.store.get_meta("service_recovery_metadata_first:v1") == "1"
    remaining = cap
    for metadata in (metadata_first, not metadata_first):
        if metadata:
            report = recover_metadata_operations(
                deps.shoko,
                deps.store,
                source_key=metadata_source_key(deps.shoko),
                now_iso=now_iso,
                deadline=cycle_start_mono + deps.max_cycle_seconds,
                max_rows=remaining,
                clock=deps.monotonic,
            )
            remaining -= report.processed_count
            reasons.extend(
                f"metadata_recovery:{item.anime_id}:{item.status.value}"
                for item in report.observations
            )
            if report.deferred_count:
                reasons.append(f"metadata_recovery_deferred:{report.deferred_count}")
        else:
            file_reasons, processed = _file_recovery_phase(
                deps, now_iso, cycle_start_mono, max_rows=remaining
            )
            reasons.extend(file_reasons)
            remaining -= processed
    deps.store.set_meta("service_recovery_metadata_first:v1", "0" if metadata_first else "1")
    return tuple(reasons)


def _candidate_freshness_guard(
    deps: DaemonDeps,
    active: ActiveMappings,
    send_iso: str,
    *,
    generation_lock_held: bool = False,
) -> _ApplyOutcome | None:
    """F11: per-candidate freshness refusals — deferral (stays ``planned``/queueable), never
    a quarantine, never a POST. The lease/gate freshness is owned by the
    per-candidate eligibility + preflight below (F2/P2-4)."""
    if deps.shutdown_requested():
        return _ApplyOutcome(0, 1, ("shutdown_requested",), ())
    if deps.mapping_store.invalid_source_hold():
        return _ApplyOutcome(0, 1, ("mapping_source_invalid",), ())
    if mapping_generation_stale(deps, active, send_iso, generation_lock_held=generation_lock_held):
        # The plan was frozen against the guard-time generation: a swap defers the candidate
        # (the next cycle re-validates), never POSTs on the stale digests.
        return _ApplyOutcome(0, 1, ("mapping_generation_changed",), ())
    return None


def _post_preflight_freshness_guard(
    deps: DaemonDeps,
    active: ActiveMappings,
    send_iso: str,
    *,
    cycle_deadline: float | None,
    generation_lock_held: bool = False,
) -> _ApplyOutcome | None:
    """P2-1: the guard re-run between preflight's network phase and the intent commit.

    The target/file and current-Sonarr-source GETs are slow (10s connect / 30s read), so a
    shutdown, generation swap, lease expiry, or gate close that lands DURING them must not ride the
    pre-preflight verdict into ``prepare_send``. This re-evaluation is purely local (store meta +
    the disk lease + config gate) — no network, no polling.
    """
    guard = _candidate_freshness_guard(
        deps, active, send_iso, generation_lock_held=generation_lock_held
    )
    if guard is not None:
        return guard
    if deps.service_mode:
        authorized = deps.service_authorized_local is not None and deps.service_authorized_local()
        if not authorized:
            return _ApplyOutcome(0, 1, ("service_authorization_changed",), ())
    elif not deps.attestation_valid() or not apply_gate(deps.settings):
        # Transient safety (F8 semantics): defer, never quarantine.
        return _ApplyOutcome(0, 1, ("attestation_expired",), ())
    if cycle_deadline is not None and deps.monotonic() >= cycle_deadline:
        return _ApplyOutcome(0, 1, ("cycle_time_budget_exhausted",), ())
    return None


def _close_send(
    deps: DaemonDeps,
    plan: LinkPlan,
    file_id: int,
    cycle_key: str,
    send_iso: str,
    send: Any,
    *,
    deadline_mono: float | None = None,
) -> _ApplyOutcome:
    """Close the durable send row's outcome: inline verify on acceptance, no resubmit ever."""
    if send.outcome is SendOutcome.AMBIGUOUS_SEND:
        return _ApplyOutcome(
            1, 0, (), (f"ambiguous_send: file {file_id} - verify, never resubmit",)
        )
    if send.outcome is not SendOutcome.ACCEPTED_UNVERIFIED:
        return _ApplyOutcome(1, 0, (), ())
    verification = _verify_and_close(
        deps, plan, file_id, cycle_key, send_iso, deadline_mono=deadline_mono
    )
    return _ApplyOutcome(
        1,
        0,
        (),
        (verification,),
        verified=verification == "verification:verified",
    )


def check_plan_sonarr_source(
    deps: DaemonDeps, plan: LinkPlan | MetadataDemand, *, deadline: float | None
) -> SourceRelationshipVerdict:
    """Compare a frozen plan with current Sonarr series, file, and episode rows."""
    if isinstance(plan, MetadataDemand):
        tvdb_series_ids = {item.tvdb_series for item in plan.mapped_targets}
    else:
        tvdb_series_ids = {item.tvdb_series for item in plan.targets}
        tvdb_series_ids.update(item.tvdb_series for item in plan.pending_targets)
    if len(tvdb_series_ids) != 1:
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.UNAVAILABLE,
            "frozen plan does not identify exactly one TVDB series",
        )
    return check_current_file_relationship(
        deps.sonarr,
        series_id=plan.sonarr_series_id,
        episode_file_id=plan.sonarr_episode_file_id,
        path=plan.sonarr_path,
        size=plan.sonarr_size,
        tvdb_series_id=next(iter(tvdb_series_ids)),
        allowed_roots=deps.settings.sonarr.allowed_roots,
        episode_identities=tuple(
            (
                episode.episode_id,
                episode.tvdb_episode_id,
                episode.season_number,
                episode.episode_number,
                episode.absolute_episode_number,
            )
            for episode in plan.episodes
        ),
        deadline=deadline,
        monotonic=deps.monotonic,
    )


@dataclass(frozen=True)
class MetadataExecutionOutcome:
    """One bounded metadata decision, with no file/canary completion credit.

    attempted is the durable admission count, including uncertain delivery; callers
    must share its request allowance with file submissions. Replan only on a new
    reconciliation pass; this primitive never patches a frozen demand or link plan.
    """

    reason: str
    attempted: int = 0


@dataclass(frozen=True)
class _MetadataBatchAdmission:
    """Empty-queue observation scoped to this locked cycle's frozen demands."""

    store: StateStore
    source_key: str
    cycle_key: str
    deadline: float
    demands: frozenset[tuple[int, str]]


def _metadata_mapping_current(demand: MetadataDemand, active: ActiveMappings) -> bool:
    if (
        not demand.mapped_targets
        or demand.episodes != tuple(dict.fromkeys(t.source for t in demand.mapped_targets))
        or not demand.needs
        or not demand.missing_targets <= demand.required_targets
        or any(need.target not in demand.mapped_targets for need in demand.needs)
        or demand.fingerprint != metadata_demand_fingerprint(demand)
        or (
            demand.targets_digest,
            demand.structure_digest,
            demand.mapping_revision,
            demand.overrides_active,
        )
        != (
            active.targets_digest,
            active.structure_digest,
            str(active.revision),
            active.overrides_applied,
        )
    ):
        return False
    grouped: dict[tuple[int, int, int, int | None], set[tuple[int, int, str, str]]] = {}
    for target in demand.mapped_targets:
        source = target.source
        key = (
            target.tvdb_series,
            source.season_number,
            source.episode_number,
            source.absolute_episode_number,
        )
        grouped.setdefault(key, set()).add(
            (
                target.anidb_anime_id,
                target.anidb_episode_number,
                target.episode_type.value,
                target.source_rule,
            )
        )
    for key, expected in grouped.items():
        current = resolve_mapping(active.index, *key)
        if (
            current.status is not ResolutionStatus.RESOLVED
            or {
                (t.anidb_id, t.anidb_episode_number, t.episode_type.value, t.source_rule.value)
                for t in current.targets
            }
            != expected
        ):
            return False
    return True


def _metadata_file_current(deps: DaemonDeps, demand: MetadataDemand) -> bool:
    record = deps.store.get_file(demand.shoko_file_id)
    return (
        record is not None
        and record.campaign_id is None
        and record.cohort is not Cohort.CAMPAIGN
        and record.state
        in {FileState.MATCHED, FileState.MAPPED, FileState.PLANNED, FileState.WAITING}
        and not deps.store.mapping_target_quarantined(demand.shoko_file_id)
        and record.identity is not None
        and canonical_ed2k(record.identity.ed2k) == demand.shoko_ed2k
        and (record.sonarr_series_id, record.sonarr_file_id, record.sonarr_path, record.sonarr_size)
        == (
            demand.sonarr_series_id,
            demand.sonarr_episode_file_id,
            demand.sonarr_path,
            demand.sonarr_size,
        )
        and demand.shoko_size == demand.sonarr_size
        and not _grace_period_active(deps, record, deps.now())
    )


def _metadata_consent_current(deps: DaemonDeps, expected: str, revision: str | None) -> bool:
    if (
        deps.store.get_onboarding_json() != expected
        or deps.store.get_service_authorization_revision() != revision
        or current_metadata_maintenance_policy(deps.store) != METADATA_MAINTENANCE_POLICY
        or OnboardingManager(deps.store).reconfirmation_required()
        or deps.store.get_service_pause()[0]
    ):
        return False
    record = OnboardingRecord.from_json(expected)
    binding = record.binding
    return (
        binding.metadata_maintenance_policy == METADATA_MAINTENANCE_POLICY
        and METADATA_MAINTENANCE_CONFIRMATION in record.confirmed_facts
        and binding.sonarr_url == deps.settings.sonarr.url
        and binding.shoko_url == deps.settings.shoko.url
        and str(deps.shoko.base_url).rstrip("/") == binding.shoko_url.rstrip("/")
        and str(deps.sonarr.base_url).rstrip("/") == binding.sonarr_url.rstrip("/")
        and binding.roots == tuple(sorted(set(deps.settings.sonarr.allowed_roots)))
        and set(binding.path_mappings) == set(deps.settings.path_mappings)
    )


def _metadata_local_guard(
    deps: DaemonDeps,
    demand: MetadataDemand,
    active: ActiveMappings,
    expected: str,
    revision: str | None,
    deadline: float,
    *,
    generation_lock_held: bool = False,
) -> bool:
    return (
        deps.service_mode
        and demand.policy == RunPolicy.SERVICE.value
        and math.isfinite(deadline)
        and deps.monotonic() < deadline
        and not deps.shutdown_requested()
        and not deps.store.writes_disabled
        and deps.store.owns_apply_ownership(deps.pid, deps.now())
        and demand.scope_fingerprint
        == _metadata_scope_fingerprint(deps.settings, RunPolicy.SERVICE)
        and _metadata_consent_current(deps, expected, revision)
        and _metadata_file_current(deps, demand)
        and deps.store.can_auto_apply(
            demand.shoko_file_id,
            now=deps.now(),
            attestation_valid=None,
            gate_open=None,
            service_mode=True,
            service_authorized=True,
            service_backlog=True,
            budgets_ok=True,
            mapping_stable=True,
            source_stable=True,
        ).eligible
        and _post_preflight_freshness_guard(
            deps,
            active,
            deps.now(),
            cycle_deadline=deadline,
            generation_lock_held=generation_lock_held,
        )
        is None
    )


def _fresh_metadata_missing(
    deps: DaemonDeps,
    demand: MetadataDemand,
    deadline: float,
) -> frozenset[tuple[int, EpisodeType, int]] | None:
    resolver = EpisodeIdResolver(deps.shoko, clock=deps.monotonic, deadline=deadline)
    missing: set[tuple[int, EpisodeType, int]] = set()
    known = {need.target.identity: need.known_anidb_episode_id for need in demand.needs}
    for anime, kind, number in sorted(
        demand.required_targets, key=lambda t: (t[0], t[1].value, t[2])
    ):
        result = resolver.resolve(anime, kind.value, number)
        previous_id = known.get((anime, kind, number))
        if previous_id is not None and result.anidb_episode_id != previous_id:
            return None
        if result.status in {
            EpisodeResolutionStatus.ANIME_NOT_CACHED,
            EpisodeResolutionStatus.PENDING_METADATA,
        }:
            missing.add((anime, kind, number))
        elif result.status is not EpisodeResolutionStatus.RESOLVED:
            return None
    return frozenset(missing) if deps.monotonic() < deadline else None


def _metadata_queue_admission(
    deps: DaemonDeps,
    groups: Mapping[int, MetadataDemand],
    cycle_key: str,
    deadline: float,
) -> _MetadataBatchAdmission | str:
    inventory = fetch_queue_inventory(deps.shoko, deadline=deadline, clock=deps.monotonic)
    if not inventory.complete or inventory.total_count is None:
        return "metadata_queue_not_clear"
    if inventory.total_count != 0:
        if len(groups) == 1:
            anime, demand = next(iter(groups.items()))
            if inventory.lookup(QueueJobKind.ANIME_METADATA, anime).presence is Presence.PRESENT:
                source_key = metadata_source_key(deps.shoko)
                with contextlib.suppress(StateError):
                    if deps.store.get_metadata_operation(source_key, anime) is None:
                        deps.store.observe_external_metadata_operation(
                            source_key,
                            anime,
                            demand_fingerprint=demand.fingerprint,
                            request_fingerprint=_metadata_request_fingerprint(source_key, anime),
                            request_version=1,
                            iso=deps.now(),
                            required_targets=tuple(
                                t for t in demand.required_targets if t[0] == anime
                            ),
                            missing_targets=tuple(
                                t for t in demand.missing_targets if t[0] == anime
                            ),
                        )
                return "metadata_waiting_external_queue"
            if (
                inventory.lookup(QueueJobKind.MANUAL_LINK, demand.shoko_file_id).presence
                is Presence.PRESENT
            ):
                return "metadata_waiting_external_file_queue"
        return "metadata_queue_busy"
    return _MetadataBatchAdmission(
        deps.store,
        metadata_source_key(deps.shoko),
        cycle_key,
        deadline,
        frozenset((anime, demand.fingerprint) for anime, demand in groups.items()),
    )


def process_metadata_demand(  # noqa: PLR0911, PLR0912, PLR0915 - one ordered send boundary
    deps: DaemonDeps,
    demand: MetadataDemand,
    anime_id: int,
    active: ActiveMappings,
    folders: Mapping[int, ImportFolder],
    flags: _EligibilityFlags,
    cycle_key: str,
    *,
    deadline: float,
    _batch: _MetadataBatchAdmission | None = None,
) -> MetadataExecutionOutcome:
    """Authorize and execute at most one normal-service metadata request.

    Caller owns the cycle's apply lock and shared submission allowance. Existing
    own operations are inspect-only; read-only recovery is independent of the current
    option and authorization.
    """
    deps = _bind_service_authorization(deps, deadline)
    operation: MetadataOperation | None = None
    attempted = 0
    try:
        expected = deps.store.get_onboarding_json()  # capture BEFORE remote authorization
        revision = deps.store.get_service_authorization_revision()
        # Rebuild against the current override configuration, not a caller-supplied
        # index whose frozen digest labels could still name a different index.
        verified = load_active(deps.mapping_store, _load_overrides(deps))
        if verified is None or (
            active.revision,
            active.content_sha256,
            active.targets_digest,
            active.structure_digest,
            active.source_epoch,
            active.overrides_applied,
        ) != (
            verified.revision,
            verified.content_sha256,
            verified.targets_digest,
            verified.structure_digest,
            verified.source_epoch,
            verified.overrides_applied,
        ):
            return MetadataExecutionOutcome("metadata_generation_or_overrides_changed")
        active = verified
        if (
            expected is None
            or not _metadata_local_guard(deps, demand, active, expected, revision, deadline)
            or not _metadata_mapping_current(demand, active)
        ):
            return MetadataExecutionOutcome("metadata_authorization_or_demand_changed")
        required = tuple(t for t in demand.required_targets if t[0] == anime_id)
        if not required or not any(t[0] == anime_id for t in demand.missing_targets):
            return MetadataExecutionOutcome("metadata_anime_not_required")
        source_key = metadata_source_key(deps.shoko)
        prior = deps.store.get_metadata_operation(source_key, anime_id)
        if prior is not None and (
            (
                prior.reservation_owned
                and prior.state
                not in {MetadataOperationState.READY, MetadataOperationState.DEFINITELY_REJECTED}
            )
            or prior.state
            in {
                MetadataOperationState.PREPARED,
                MetadataOperationState.POSSIBLY_DELIVERED,
                MetadataOperationState.NEEDS_ATTENTION,
            }
        ):
            return MetadataExecutionOutcome("metadata_operation_retained")
        eligible = deps.store.can_auto_apply(
            demand.shoko_file_id,
            now=deps.now(),
            attestation_valid=None,
            gate_open=None,
            service_mode=True,
            service_authorized=True,
            service_backlog=True,
            budgets_ok=flags.budgets_ok and _service_candidate_budget_headroom(deps, deps.now(), 1),
            mapping_stable=flags.mapping_stable,
            source_stable=flags.source_stable,
        )
        if not eligible.eligible or not deps.service_authorized():
            return MetadataExecutionOutcome("metadata_service_not_eligible")
        source = check_plan_sonarr_source(deps, demand, deadline=deadline)
        if source.status is not SourceRelationshipStatus.CURRENT:
            return MetadataExecutionOutcome("metadata_sonarr_source_changed_or_unavailable")
        # A second remote authorization observes current physical/folder/settings facts
        # after source reads; no remote authorization follows the protected file read.
        if not deps.service_authorized():
            return MetadataExecutionOutcome("metadata_authorization_changed")
        if _batch is not None:
            if (
                _batch.store is not deps.store
                or _batch.source_key != source_key
                or _batch.cycle_key != cycle_key
                or _batch.deadline != deadline
                or (anime_id, demand.fingerprint) not in _batch.demands
            ):
                return MetadataExecutionOutcome("metadata_authorization_or_demand_changed")
        else:
            refusal = _metadata_queue_admission(deps, {anime_id: demand}, cycle_key, deadline)
            if isinstance(refusal, str):
                return MetadataExecutionOutcome(refusal)
        missing = _fresh_metadata_missing(deps, demand, deadline)
        if missing is None:
            return MetadataExecutionOutcome("metadata_targets_unavailable")
        selected_missing = tuple(t for t in missing if t[0] == anime_id)
        if not selected_missing:
            return MetadataExecutionOutcome("metadata_replan")
        # Final network guard; never invent a LinkPlan or internal episode IDs.
        file_gate = preflight_file(
            deps.shoko, demand, deps.settings, folders, deadline=deadline, clock=deps.monotonic
        )
        if not file_gate.ok:
            return MetadataExecutionOutcome("metadata_file_preflight_refused")
        with deps.mapping_store.generation_lock() as acquired:
            if not acquired or not _metadata_local_guard(
                deps, demand, active, expected, revision, deadline, generation_lock_held=True
            ):
                return MetadataExecutionOutcome("metadata_authorization_or_generation_changed")
            prepared = deps.store.prepare_metadata_operation(
                source_key,
                anime_id,
                demand_fingerprint=demand.fingerprint,
                request_fingerprint=_metadata_request_fingerprint(source_key, anime_id),
                request_version=1,
                cycle_key=cycle_key,
                iso=deps.now(),
                expected_document_json=expected,
                required_targets=required,
                missing_targets=selected_missing,
                series_name=demand.series_name,
                retry_unsent_claim=(
                    prior.claim
                    if prior is not None
                    and prior.state is MetadataOperationState.DEFINITELY_REJECTED
                    else None
                ),
                adopt_external_claim=(
                    prior.claim
                    if prior is not None
                    and not prior.reservation_owned
                    and prior.attempted_iso is None
                    else None
                ),
                after_ready_claim=(
                    prior.claim
                    if prior is not None and prior.state is MetadataOperationState.READY
                    else None
                ),
            )
            if isinstance(prepared, BudgetRefusal):
                return MetadataExecutionOutcome(
                    prepared.reason
                    if prepared.reason.startswith("metadata_")
                    else "metadata_" + prepared.reason
                )
            operation = prepared
            # Bind display context to the new generation before any possible HTTP,
            # including READY re-arms whose old claim belongs to another file.
            with contextlib.suppress(StateError):
                observe_metadata_status(
                    deps.store,
                    source_key,
                    anime_id,
                    "metadata_prepared",
                    deps.now(),
                    demand=demand,
                    operation_snapshot=operation,
                )
            if not _metadata_local_guard(
                deps, demand, active, expected, revision, deadline, generation_lock_held=True
            ):
                deps.store.cancel_prepared_metadata_operation(operation, iso=deps.now())
                return MetadataExecutionOutcome("metadata_authorization_changed")
            now_iso = deps.now()
            operation = deps.store.mark_metadata_possibly_delivered(
                operation,
                iso=now_iso,
                verification_deadline_iso=(
                    _parse_iso(now_iso) + timedelta(seconds=deps.verification_timeout_seconds)
                ).isoformat(),
                expected_document_json=expected,
            )
            if not _metadata_local_guard(
                deps, demand, active, expected, revision, deadline, generation_lock_held=True
            ):
                # Definitely no HTTP; release only if persistence is still writable.
                deps.store.record_metadata_operation(
                    operation, MetadataOperationState.DEFINITELY_REJECTED, iso=deps.now()
                )
                return MetadataExecutionOutcome("metadata_authorization_changed")
            deps.store.admit_metadata_transport(operation, expected_document_json=expected)
            if not _metadata_local_guard(
                deps, demand, active, expected, revision, deadline, generation_lock_held=True
            ):
                deps.store.record_metadata_operation(
                    operation, MetadataOperationState.DEFINITELY_REJECTED, iso=deps.now()
                )
                return MetadataExecutionOutcome("metadata_authorization_changed")
            attempted = 1
            result = submit_metadata_refresh(
                deps.shoko, anime_id, deadline=deadline, clock=deps.monotonic
            )
            if result.outcome is MetadataRefreshOutcome.NOT_SENT:
                deps.store.record_metadata_operation(
                    operation, MetadataOperationState.DEFINITELY_REJECTED, iso=deps.now()
                )
                return MetadataExecutionOutcome("metadata_not_sent", attempted)
            if result.outcome is MetadataRefreshOutcome.ACCEPTED_QUEUED:
                deps.store.record_metadata_operation(
                    operation, MetadataOperationState.ACCEPTED_WAITING, iso=deps.now()
                )
                return MetadataExecutionOutcome("metadata_accepted_waiting", attempted)
            return MetadataExecutionOutcome("metadata_delivery_unknown", attempted)
    except (StateError, ConfigError, ValueError, ApplyLockUnavailableError, httpx.HTTPError):
        # Never release a possibly delivered claim or replay after persistence failure.
        return MetadataExecutionOutcome("metadata_state_or_guard_unavailable", attempted)


def _metadata_request_fingerprint(source_key: str, anime_id: int) -> str:
    # The version identifies the reviewed six-flag request contract, not mutable demand.
    return hashlib.sha256(f"metadata_refresh:v1:{source_key}:{anime_id}".encode()).hexdigest()


def _refresh_service_canary(deps: DaemonDeps, now_iso: str) -> tuple[int, str] | None:
    """Advance the durable first-send canary from the file's durable ledger state."""
    canary = deps.store.get_service_canary()
    if not deps.service_mode or canary is None or canary[1] != "pending":
        return canary
    file_id, _status = canary
    record = deps.store.get_file(file_id)
    if record is not None and record.state is FileState.VERIFIED:
        deps.store.set_service_canary_status("verified", now_iso)
        return (file_id, "verified")
    if record is not None and record.state in {
        FileState.MATCHED,
        FileState.MAPPED,
        FileState.PLANNED,
        FileState.PREPARED_NOT_SENT,
        FileState.REQUEST_MAY_HAVE_BEEN_SENT,
        FileState.VERIFYING,
    }:
        return canary
    if record is not None and record.state is FileState.WAITING:
        outcomes = deps.store.service_canary_send_outcomes()
        if len(outcomes) == 1 and outcomes[0] in {
            SendOutcome.REJECTED.value,
            SendOutcome.REFUSED.value,
        }:
            return canary
    deps.store.set_service_canary_status("failed", now_iso)
    return (file_id, "failed")


def _service_canary_resolution_priority(
    deps: DaemonDeps, file_ids: frozenset[int]
) -> frozenset[int]:
    """Keep the pending service canary inside the bounded resolution window."""
    canary = deps.store.get_service_canary() if deps.service_mode else None
    if canary is not None and canary[1] == "pending":
        return file_ids | {canary[0]}
    return file_ids


def _finish_service_candidate(
    deps: DaemonDeps,
    plan: LinkPlan,
    file_id: int,
    cycle_key: str,
    send_iso: str,
    send: Any,
    *,
    deadline_mono: float | None = None,
) -> _ApplyOutcome:
    outcome = _close_send(
        deps, plan, file_id, cycle_key, send_iso, send, deadline_mono=deadline_mono
    )
    canary = _refresh_service_canary(deps, send_iso)
    if canary is not None and canary[1] == "failed":
        return replace(outcome, reasons=(*outcome.reasons, "service_canary_failed"))
    return outcome


def _service_canary_candidates(
    deps: DaemonDeps,
    plans: Mapping[int, LinkPlan],
    ordered: list[int],
    now_iso: str,
) -> tuple[list[int], str | None]:
    if not deps.service_mode:
        return ordered, None
    canary = _refresh_service_canary(deps, now_iso)
    if canary is None or canary[1] == "verified":
        return ordered, None
    if canary[1] == "failed":
        return [], "service_canary_failed"
    record = deps.store.get_file(canary[0])
    if record is None or record.state is not FileState.PLANNED or canary[0] not in plans:
        return [], "service_canary_pending"
    return [canary[0]], None


def _service_canary_hold_reason(deps: DaemonDeps) -> str | None:
    if not deps.service_mode:
        return None
    canary = _refresh_service_canary(deps, deps.now())
    if canary is None or canary[1] == "verified":
        return None
    return "service_canary_failed" if canary[1] == "failed" else "service_canary_pending"


def _process_candidate(  # noqa: PLR0911, PLR0912
    deps: DaemonDeps,
    plan: LinkPlan,
    file_id: int,
    flags: _EligibilityFlags,
    active: ActiveMappings,
    folders: Mapping[int, ImportFolder],
    queue: QueueSnapshot | None,
    cycle_key: str,
    *,
    service_backlog: bool = False,
    source_check_deadline: float | None = None,
) -> _ApplyOutcome:
    """One file's guarded send: fresh safety inputs → eligibility → preflight → prepare →
    mark_sent → submit → verify (F11: every submission timestamp comes from the fresh clock)."""
    if source_check_deadline is not None:
        deps = _bind_service_authorization(deps, source_check_deadline)
    # Advanced mode re-evaluates its lease and apply gate per candidate; normal service mode
    # re-evaluates current onboarding consent. A blocked candidate defers (stays planned), never
    # quarantined.
    service_authorized = deps.service_authorized() if deps.service_mode else None
    attestation_ok = None if deps.service_mode else deps.attestation_valid()
    gate_open = None if deps.service_mode else apply_gate(deps.settings)
    # F11: fresh per-submission time + live safety inputs. A cycle-start snapshot must never
    # authorize a POST after a shutdown lands or the mapping generation swaps mid-cycle.
    send_iso = deps.now()
    stale = _candidate_freshness_guard(deps, active, send_iso)
    if stale is not None:
        return stale
    verdict = deps.store.can_auto_apply(
        file_id,
        now=send_iso,
        attestation_valid=attestation_ok,
        gate_open=gate_open,
        service_mode=deps.service_mode,
        service_authorized=service_authorized,
        service_backlog=service_backlog,
        budgets_ok=flags.budgets_ok
        and _service_candidate_budget_headroom(deps, send_iso, len(plan.episode_ids)),
        mapping_stable=flags.mapping_stable,
        source_stable=flags.source_stable,
    )
    if not verdict.eligible:
        return _ApplyOutcome(0, 0, tuple(verdict.reasons), ())
    attempted = deps.store.get_file(file_id)
    gate = preflight(
        deps.shoko,
        plan,
        deps.settings,
        folders,
        attestation_valid=attestation_ok,
        apply_gate_open=gate_open,
        service_mode=deps.service_mode,
        service_authorized=service_authorized,
        queue=queue,
        active_targets_digest=active.targets_digest,
        active_structure_digest=active.structure_digest,
        blocked_over=effective_blocked_over(deps),
        mapping_index=active.index,
        check_file=False,
        check_targets=False,
    )
    if not gate.ok:
        return _preflight_failure_outcome(
            deps, file_id, gate, send_iso, attempted=attempted, plan=plan
        )
    source = check_plan_sonarr_source(deps, plan, deadline=source_check_deadline)
    if source.status is SourceRelationshipStatus.UNAVAILABLE:
        return _ApplyOutcome(
            0,
            1,
            (f"sonarr_source_unavailable:{file_id}",),
            (f"sonarr_source_check_deferred: file {file_id} - {source.detail}",),
        )
    if source.status is SourceRelationshipStatus.CHANGED:
        deps.store.quarantine_mapping_targets(
            (file_id,), iso=deps.now(), reason="sonarr_source_changed"
        )
        return _ApplyOutcome(
            0,
            0,
            (f"sonarr_source_changed:{file_id}",),
            (
                f"sonarr_source_changed: file {file_id} - {source.detail}; "
                "frozen target retained for review, no POST",
            ),
        )
    # All source/target/authorization network checks precede the final protected-link read.
    # For normal service the remote check captures current setup facts; a local callback below
    # catches a pause/consent change that lands during this final Shoko request.
    if deps.service_mode and not deps.service_authorized():
        return _ApplyOutcome(0, 1, ("service_authorization_changed",), ())
    target_gate = preflight_link_jobs_and_targets(
        deps.shoko, plan, deadline=source_check_deadline, clock=deps.monotonic
    )
    if not target_gate.ok:
        return _preflight_failure_outcome(
            deps, file_id, target_gate, deps.now(), attempted=attempted, plan=plan
        )
    file_gate = preflight_file(
        deps.shoko,
        plan,
        deps.settings,
        folders,
        deadline=source_check_deadline,
        clock=deps.monotonic,
    )
    if deps.store.writes_disabled:
        return _ApplyOutcome(0, 1, ("writes_disabled",), ())
    # The final hold/generation check, durable send intent, and one Shoko mutation share the
    # generation lock with mapping refresh. A refresh may fetch concurrently, but cannot commit
    # an invalid-source hold or swap the active generation between this authorization and POST.
    send: SendResult
    token: SendToken | None = None
    transport_started = False
    try:
        with deps.mapping_store.generation_lock() as acquired:
            if not acquired:
                return _ApplyOutcome(0, 1, ("mapping_generation_busy",), ())
            send_iso = deps.now()
            stale_after = _post_preflight_freshness_guard(
                deps,
                active,
                send_iso,
                cycle_deadline=source_check_deadline,
                generation_lock_held=True,
            )
            if stale_after is not None:
                return stale_after
            if not file_gate.ok:
                return _preflight_failure_outcome(
                    deps, file_id, file_gate, send_iso, attempted=attempted, plan=plan
                )
            prepared = deps.store.prepare_send(
                file_id,
                plan.fingerprint,
                request_fingerprint(plan),
                targets_digest=plan.targets_digest,
                structure_digest=plan.structure_digest,
                mapping_revision=str(active.revision),
                cycle_key=cycle_key,
                iso=send_iso,
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
            token = prepared
            if deps.service_mode:
                deps.store.record_service_canary(file_id, send_iso, send_id=prepared.send_id)
            # mark_sent durably commits request_may_have_been_sent BEFORE any bytes reach HTTP.
            deps.store.mark_sent(token, iso=send_iso)
            with deps.store.protect_transport(token):
                transport_started = True
                send = submit_link(
                    deps.shoko, plan, clock=deps.monotonic, deadline=source_check_deadline
                )
            deps.store.record_send_outcome(
                token, send.outcome, iso=send_iso, http_status=send.http_status, detail=send.detail
            )
    except ApplyLockUnavailableError:
        return _ApplyOutcome(0, 1, ("mapping_generation_lock_unavailable",), ())
    except WriteDisabledError:
        # Only prepared rows are voidable. A send already marked attempted stays in verify/hold,
        # even when the stop fenced the transport guard before HTTP began.
        if token is not None:
            with contextlib.suppress(WriteDisabledError, StateError):
                deps.store._void_prepared_send_after_write_stop(
                    file_id,
                    token.request_fingerprint,
                    iso=send_iso,
                    reason="durable write stop fenced transport",
                )
        return _ApplyOutcome(
            int(transport_started),
            int(not transport_started),
            ("writes_disabled",),
            (),
        )
    return _finish_service_candidate(
        deps, plan, file_id, cycle_key, send_iso, send, deadline_mono=source_check_deadline
    )


def mapping_generation_identity(
    deps: DaemonDeps, _now_iso: str, *, generation_lock_held: bool = False
) -> tuple[int, str, str, str, str | None] | None:
    """The disk-active generation's identity tuple, ``None`` when the active cache is absent.

    Cheap by design (meta + content digest read, no index parse): F11 re-checks it per submission
    so a mapping refresh that swaps generations mid-batch is observed *before* the next POST.
    """
    if generation_lock_held:
        return deps.mapping_store.active_identity_locked()
    return deps.mapping_store.active_identity()


def mapping_generation_stale(
    deps: DaemonDeps,
    active: ActiveMappings,
    now_iso: str,
    *,
    generation_lock_held: bool = False,
) -> bool:
    """True when the disk-active generation is no longer the one the guards validated against."""
    return mapping_generation_identity(
        deps, now_iso, generation_lock_held=generation_lock_held
    ) != (
        active.revision,
        active.content_sha256,
        active.targets_digest,
        active.structure_digest,
        active.source_epoch,
    )


def _grace_period_active(deps: DaemonDeps, record: FileRecord, now_iso: str) -> bool:
    """True while a new arrival is inside the ``new_file_grace_period_seconds`` window.

    Shoko's own import/XRef state settles *after* a file arrives, so submitting the file
    in its arrival cycle races that settling. The recorded ``first_seen_iso`` (the cycle
    that first saw the file) is the age anchor; ``grace == 0`` disables the window. An
    unparseable anchor defers (fail closed: never a POST on an unknown age).
    """
    grace = deps.settings.sync.new_file_grace_period_seconds
    if grace <= 0:
        return False
    try:
        first_seen = _parse_iso(record.first_seen_iso)
    except ValueError:
        return True
    return _parse_iso(now_iso) - first_seen < timedelta(seconds=grace)


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
    *,
    policy: RunPolicy,
) -> tuple[int, int, tuple[str, ...], tuple[str, ...], int]:
    """Apply to deterministic arrivals and explicitly authorized service backlog candidates.

    ``max_submissions`` bounds **POSTs attempted**, not enumerated files; the rest stay
    ``planned`` (deferred). Returns submissions, deferrals, reasons, drift, and verified count.

    New arrivals inside the ``sync.new_file_grace_period_seconds`` window defer
    with the ``grace_period`` reason: the row stays ``planned``, consumes **no** budget
    reservation (no ``prepare_send``), and is never quarantined — the next cycle that
    starts after the window closes submits it. Backlog files require service mode plus the
    explicit ``SERVICE`` policy; campaign files remain on the separate F7/CLI campaign paths.
    """
    submitted = 0
    verified = 0
    deferred = 0
    reasons: list[str] = []
    drift: list[str] = []
    ordered = sorted(plans)
    service_backlog = deps.service_mode and policy is RunPolicy.SERVICE
    ordered, canary_gate_reason = _service_canary_candidates(deps, plans, ordered, now_iso)
    if canary_gate_reason is not None:
        return (0, len(plans), (canary_gate_reason,), (), 0)
    for index, file_id in enumerate(ordered):
        record = deps.store.get_file(file_id)
        if record is None or record.state is not FileState.PLANNED:
            continue
        if record.cohort is not Cohort.NEW_ARRIVAL and not (
            service_backlog and record.cohort is Cohort.BACKLOG
        ):
            continue  # backlog requires the explicit onboarding-authorized SERVICE policy
        if deps.store.writes_disabled:
            reasons.append("writes_disabled")
            deferred += len(ordered) - index
            break
        if submitted >= max_submissions:
            deferred += len(ordered) - index
            break
        if deps.monotonic() - cycle_start_mono >= deps.max_cycle_seconds:
            deferred += len(ordered) - index
            break
        if deps.shutdown_requested():
            # F11: a shutdown request stops starting NEW submissions mid-cycle (never
            # aborts an in-flight send); the remainder stays planned (queueable, non-destructive).
            deferred += len(ordered) - index
            reasons.append("shutdown_requested")
            break
        if _grace_period_active(deps, record, now_iso):
            deferred += 1
            reasons.append(GRACE_PERIOD_REASON)
            continue
        outcome = _process_candidate(
            deps,
            plans[file_id],
            file_id,
            flags,
            active,
            folders,
            queue,
            cycle_key,
            service_backlog=service_backlog,
            source_check_deadline=cycle_start_mono + deps.max_cycle_seconds,
        )
        submitted += outcome.submitted
        verified += int(outcome.verified)
        deferred += outcome.deferred
        reasons.extend(outcome.reasons)
        drift.extend(outcome.drift)
        canary_hold_reason = _service_canary_hold_reason(deps)
        if canary_hold_reason is not None:
            reasons.append(canary_hold_reason)
            deferred += max(0, len(ordered) - index - 1)
            break
    return submitted, deferred, tuple(reasons), tuple(drift), verified


# ------------------------------------------------------------------ cycle runner


def _campaign_scoped_deps(deps: DaemonDeps, campaign_id: str) -> DaemonDeps:
    """A copy of ``deps`` whose attestation consults the ``campaign:<id>`` lease (F7)."""
    if deps.campaign_attestation is None:
        return deps
    hooks = deps.campaign_attestation(campaign_id)
    return replace(deps, attestation_valid=hooks.valid, attestation_verdict=hooks.verdict)


#: Per-file reason prefixes aggregated before they enter the cycle report (F7 review P2-2:
#: a full backlog plan must not flood the persisted per-cycle reason surface).
_FILE_SCOPED_REASON_PREFIXES: Final[tuple[str, ...]] = (
    "batch_limit:",
    "batch_time_limit:",
    "max_submissions_reached:",
    "file_missing:",
    "unbound:",
    "not_queueable:",
    "pending_target:",
    "campaign_quarantine:",
    "preflight_deferred:",
    "resolution_deferred:",
    "budget_refusal:",
    "send_stale:",
    "shutdown_requested:",
    "drain_authorization_revoked:",
)

#: Consecutive zero-progress drain cycles before the campaign auto-pauses
#: (F7 review P2-3: an all-quarantined campaign must not burn full sweeps forever).
#: F18: the breaker counts ONLY genuinely-unexpected stalls (unqueueable/blocked rows,
#: missing/unbound plan rows, revoked-authorization staleness) — recoverable
#: waits (budget windows, pending Shoko metadata/async verification, transient queue
#: congestion, batch capacity) defer durably instead of pausing (see :func:`_defer_campaign_drain`).
ZERO_PROGRESS_PAUSE_CYCLES: Final[int] = 3

#: F18 (Finding 1 head-of-line fix): recoverable drain stalls WITHOUT a future rolling-window
#: expiry (pending Shoko metadata walks, per-file job-cap holds, transient queue/gate/batch
#: capacity) defer with a bounded cooldown of ``max(cycle interval,
#: STALL_YIELD_COOLDOWN_CYCLES x cycle interval)`` — the persisted deferral meta keeps the
#: campaign APPROVED and drain-authorized, drain selection skips it while the
#: retry time is in the future so a later queue campaign drains, and the head is deterministically
#: re-selected once the cooldown passes (auto-resume, no operator action; every stall cycle re-arms
#: the deferral, so a stalled head never permanently blocks the queue and never pauses).
#: The cooldown is FIVE daemon cycle intervals (``sync.interval_seconds`` — no new config key),
#: sized to the PRODUCTION cadence: :func:`run_daemon` sleeps ``interval x (1 + jitter)``
#: AFTER each cycle, so a one-interval cooldown always expires before the next cycle starts and
#: the stalled head is re-selected every cycle (the queue yield is inert). Five intervals is the
#: smallest span that yields the queue for a meaningful multi-cycle window under that cadence:
#: the head defers, a later drain-authorized campaign drains, and the head auto-resumes at the
#: cooldown mark (the e2e suite pins this realistic interval x 1.05 cadence; frozen-clock
#: daemon cycles land strictly before the expiry, so the stalled head yields them all).
STALL_YIELD_COOLDOWN_CYCLES: Final[int] = 5

#: Zero-progress reason families that are RECOVERABLE waits (F18): the campaign is not stalled
#: — it waits on the rolling-budget window (``budget_refusal:``), Shoko metadata materialization
#: (``pending_target:``), a transient queue/gate (``preflight_deferred:``), or batch capacity
#: (``batch_limit:`` / ``batch_time_limit:`` / ``shutdown_requested:``). These cycles
#: DEFER with a durable retry time and NEVER count toward the permanent-pause breaker.
_RECOVERABLE_ZERO_PROGRESS_PREFIXES: Final[tuple[str, ...]] = (
    "budget_refusal:",
    "pending_target:",
    "preflight_deferred:",
    "resolution_deferred:",
    "batch_limit:",
    "batch_time_limit:",
    "shutdown_requested:",
)
#: Reason markers that keep the zero-progress breaker active (F18 hard category): rows parked
#: in operator-review safety states never self-heal through daemon cycles.
_HARD_ZERO_PROGRESS_MARKERS: Final[tuple[str, ...]] = (":blocked_safety", ":conflict")
#: Campaign file states whose pending Shoko verification (ManualLinkJob async work, metadata
#: not materialized) makes a zero-progress cycle recoverable (F18).
_PENDING_VERIFICATION_STATES: Final[frozenset[FileState]] = frozenset(
    {
        FileState.VERIFYING,
        FileState.REQUEST_MAY_HAVE_BEEN_SENT,
        FileState.INDETERMINATE_WRITE,
        FileState.PARTIAL,
    }
)


def _aggregate_campaign_reasons(reasons: Sequence[str]) -> list[str]:
    """Collapse per-file campaign reasons into ``<prefix>x<count>:first=<sample>`` entries."""
    groups: dict[str, list[str]] = {}
    others: list[str] = []
    for reason in reasons:
        prefix = next((p for p in _FILE_SCOPED_REASON_PREFIXES if reason.startswith(p)), None)
        if prefix is None:
            others.append(reason)
        else:
            groups.setdefault(prefix, []).append(reason)
    aggregated = list(others)
    for prefix, items in groups.items():
        aggregated.append(f"{prefix}x{len(items)}:first={items[0]}")
    return aggregated


def _pause_deferred_campaign(deps: DaemonDeps, campaign_id: str, reason: str, now_iso: str) -> None:
    """Persist a terminal drain deferral as a campaign pause (F7 review P1: a permanently
    deferred campaign must drop out of selection so later campaigns drain)."""
    with contextlib.suppress(WriteDisabledError, StateError):
        deps.store.set_campaign_status(campaign_id, CAMPAIGN_STATUS_PAUSED, reason, iso=now_iso)


def _zero_progress_is_recoverable(
    deps: DaemonDeps, campaign_id: str, reasons: Sequence[str]
) -> bool:
    """F18: classify a zero-progress drain cycle as a RECOVERABLE wait (defer) vs a genuinely
    unexpected stall (burns the breaker).

    Recoverable: budget refusals, pending Shoko metadata, transient queue/gate preflight
    deferrals, batch capacity, shutdown — or remaining campaign files sit in the pending
    verification states (``verifying``/``request_may_have_been_sent``/``indeterminate_write``/
    ``partial``), where the daemon's own recovery polling eventually closes them. Hard: any
    unqueueable row parked in an operator-review safety state (``blocked_safety``/``conflict``)
    keeps the breaker active — those never self-heal."""
    hard = any(
        reason.startswith("not_queueable:")
        and any(marker in reason for marker in _HARD_ZERO_PROGRESS_MARKERS)
        for reason in reasons
    )
    if hard:
        return False
    # The final local guard emits an exact cycle-cap reason, distinct from the
    # campaign authorization deadline (a hard stop). Do not accept suffixed variants.
    if "cycle_time_budget_exhausted" in reasons or any(
        reason.startswith(_RECOVERABLE_ZERO_PROGRESS_PREFIXES) for reason in reasons
    ):
        return True
    return any(
        record.state in _PENDING_VERIFICATION_STATES
        for record in deps.store.campaign_files(campaign_id)
    )


def _stall_yield_cooldown_iso(deps: DaemonDeps, now_iso: str) -> str:
    """F18 (Finding 1): the bounded cooldown retry for a recoverable stall with no future
    rolling-window expiry — ``max(interval, STALL_YIELD_COOLDOWN_CYCLES x interval)`` seconds
    after ``now`` (see :data:`STALL_YIELD_COOLDOWN_CYCLES` for the formula and its rationale)."""
    interval = deps.settings.sync.interval_seconds
    cooldown = max(interval, STALL_YIELD_COOLDOWN_CYCLES * interval)
    return (_parse_iso(now_iso) + timedelta(seconds=cooldown)).isoformat()


def _rolling_cooldown_retry_iso(deps: DaemonDeps, now_iso: str, reasons: Sequence[str]) -> str:
    """F18: the expected retry time for a budget-deferred campaign — the earliest rolling
    slot expiry (enough individual reservations expire to leave headroom) when the rolling window
    is what blocks, else the bounded stall cooldown (:func:`_stall_yield_cooldown_iso`).

    Non-rolling stalls (the per-cycle cap resets with every batch's fresh window key; a
    per-file pending-jobs refusal clears as jobs verify; a pending-target walk waits
    on Shoko metadata) previously retried ``now`` — a no-op deferral that re-selected
    the stalled head every cycle forever (Finding 1: head-of-line starvation). The cooldown
    makes the deferral real: the campaign keeps its authorization, yields the queue,
    and re-enters selection automatically once the cooldown passes. A rolling expiry that has
    already elapsed returns ``now`` (the window is open — the campaign runs and is expected to
    progress)."""
    rolling_blocked = any("rolling_new_exhausted" in reason for reason in reasons)
    limits = deps.store.budget_limits
    if limits is None:
        return _stall_yield_cooldown_iso(deps, now_iso)
    view = deps.store.budget_view()
    if not rolling_blocked and rolling_budget_exhausted(view, limits, now_iso):
        rolling_blocked = True
    if not rolling_blocked:
        return _stall_yield_cooldown_iso(deps, now_iso)
    retry = view.rolling_retry_iso(limits, now_iso)
    return retry if retry is not None else now_iso


def _recoverable_stall_family(deps: DaemonDeps, campaign_id: str, reasons: Sequence[str]) -> str:
    """F18 (Finding 2): name what a recoverable zero-progress stall is waiting on, so the
    deferral surface says the truth: Shoko metadata materialization (``pending_target:`` rows
    or files parked in the pending-verification states) is a ``pending_metadata_cooldown``;
    a per-file budget refusal is a ``budget_cooldown``; anything else recoverable (queue/gate
    congestion, batch capacity) is a ``recoverable_cooldown``."""
    if any(reason.startswith("pending_target:") for reason in reasons):
        return "pending_metadata_cooldown"
    if any(reason.startswith("budget_refusal:") for reason in reasons):
        return "budget_cooldown"
    if any(
        record.state in _PENDING_VERIFICATION_STATES
        for record in deps.store.campaign_files(campaign_id)
    ):
        return "pending_metadata_cooldown"
    return "recoverable_cooldown"


def _defer_campaign_drain(
    deps: DaemonDeps, campaign_id: str, retry_iso: str, reasons: list[str], family: str
) -> None:
    """F18: persist a RECOVERABLE drain deferral durably (survives a container restart).

    The campaign stays APPROVED and drain-authorized;
    :func:`shoko_linker.campaign.select_next_daemon_campaign` skips it until the persisted
    retry ISO passes, then a daemon cycle resumes it automatically — submissions stay bounded
    by the per-cycle cap and the rolling budget (unchanged reservation path). The
    zero-progress counter is reset so recoverable cycles never burn toward a permanent pause.
    No rate limit, ceiling, or safety gate is touched. ``family`` names the wait
    (:func:`_recoverable_stall_family`, Finding 2); the retry time is a future
    cooldown/rolling expiry — the meta is never written as a retry==now no-op.
    """
    with contextlib.suppress(WriteDisabledError, StateError):
        deps.store.set_meta(f"{META_CAMPAIGN_DEFERRED_UNTIL}:{campaign_id}", retry_iso)
        deps.store.set_meta(f"{META_CAMPAIGN_ZERO_PROGRESS}:{campaign_id}", "0")
    reasons.append(f"campaign_deferred:{campaign_id}:{family}:until={retry_iso}")


def _record_zero_progress(deps: DaemonDeps, campaign_id: str, now_iso: str, reason: str) -> bool:
    """Count consecutive genuinely-unexpected zero-progress drain cycles; pause at
    the bound (F7 review P2-3: an all-quarantined/unqueueable campaign must not burn full
    sweeps forever).

    F18: budget exhaustion, pending Shoko metadata/async verification, and transient
    queue congestion do NOT reach this breaker — they defer durably
    (:func:`_defer_campaign_drain`). ``reason`` names the auto-pause:
    ``no_queueable_candidates`` for a campaign whose remaining files are parked in states the
    daemon cannot re-queue (operator review required). Returns True when the campaign
    was auto-paused."""
    key = f"{META_CAMPAIGN_ZERO_PROGRESS}:{campaign_id}"
    raw = deps.store.get_meta(key)
    try:
        count = int(raw) if raw is not None else 0
    except ValueError:
        count = 0
    count += 1
    with contextlib.suppress(WriteDisabledError, StateError):
        deps.store.set_meta(key, str(count))
    if count >= ZERO_PROGRESS_PAUSE_CYCLES:
        _pause_deferred_campaign(deps, campaign_id, reason, now_iso)
        with contextlib.suppress(WriteDisabledError, StateError):
            deps.store.set_meta(key, "0")
        return True
    return False


def _drain_campaign(
    deps: DaemonDeps,
    now_iso: str,
    reasons: list[str],
    *,
    cycle_key: str,
    fresh_campaign_quarantines: frozenset[int],
    budget_start_mono: float | None = None,
) -> _CampaignDrainOutcome:
    """Drain **one** bounded batch of the oldest approved campaign (F7), under this cycle's lock.

    Runs entirely through :func:`shoko_linker.campaign.apply_campaign` with
    ``lock_held=True`` and this cycle's existing ``cycle_key`` (the daemon cycle already owns
    the OS apply lock), so arrival and campaign reservations share one durable per-cycle ceiling.
    ``budget_start_mono`` carries the original outer cycle start through the nested sweep
    and all subsequent I/O; campaign-scoped dependencies preserve the outer wall-time ceiling.
    The full guard chain applies unchanged: plan digest, status/deadline/max-submissions,
    write gate,
    campaign-scope attestation (an expiring lease pauses writes and stops the batch, zero
    POSTs, the daemon stays alive read-only), queue snapshot fail-closed, mapping/drift
    revalidation, eligibility, binding quarantine, preflight → prepare_send → mark_sent →
    submit_link → record → inline verify, and restart recovery via the durable rows.
    ``daemon_drain=True`` reconsults the F10 drain authorization before every submission, so a
    mid-batch ``campaign revoke-drain`` stops the running batch at the next POST (deferral,
    remainder queueable).

    A missing plan path or an unreadable/malformed plan file defers (a named reason, no
    crash, no POSTs); a campaign-state refusal defers the batch and is reported. **F18:** a
    budget-blocked campaign (eligibility verdict PURELY ``budgets_exhausted`` (Finding 4) or a
    zero-progress batch of budget/pending-metadata/pending-verification/queue deferrals) is
    DEFERRED durably with a future retry time — the rolling-window expiry, else the bounded
    stall cooldown (Finding 1: no more ``retry == now`` no-op, so a stalled head yields the
    queue and auto-resumes) — it stays APPROVED, is skipped while the retry is in the future,
    and resumes automatically in a later cycle (the old ``no_budget_progress`` permanent pause
    is gone; only genuinely-unexpected stalls keep the 3-cycle breaker). The result separately
    reports drain authorization, campaign eligibility, and the bounded batch summary.
    """
    from .campaign import (
        CampaignDeferredError,
        CampaignEligibilityRefusalError,
        CampaignError,
        CampaignPlanInvalidError,
        apply_campaign,
        campaign_deferral_reasons,
        clear_campaign_terminal_meta,
        select_next_daemon_campaign,
    )

    log = logging.getLogger(__name__)
    campaign = select_next_daemon_campaign(deps.store, now_iso)
    if campaign is None:
        # F18: surface the deferral (id + retry time) of a campaign skipped for the cooldown.
        reasons.extend(campaign_deferral_reasons(deps.store, now_iso))
        return _CampaignDrainOutcome()
    if campaign.plan_path is None:
        reasons.append(f"campaign_deferred:{campaign.campaign_id}:plan_path_missing")
        _pause_deferred_campaign(deps, campaign.campaign_id, "plan_path_missing", now_iso)
        return _CampaignDrainOutcome(selected=True)
    campaign_deps = _campaign_scoped_deps(deps, campaign.campaign_id)
    try:
        report = apply_campaign(
            campaign_deps,
            deps.settings,
            deps.store,
            campaign.plan_path,
            now_iso,
            lock_held=True,
            daemon_drain=True,
            budget_cycle_key=cycle_key,
            budget_start_mono=budget_start_mono,
            fresh_campaign_quarantines=fresh_campaign_quarantines,
        )
    except CampaignPlanInvalidError as exc:
        reasons.append(f"campaign_deferred:{campaign.campaign_id}:plan_unreadable")
        _pause_deferred_campaign(deps, campaign.campaign_id, "plan_unreadable", now_iso)
        log.warning(
            "campaign %s drain deferred: plan file unreadable (%s): paused, zero POSTs",
            campaign.campaign_id,
            exc,
        )
        return _CampaignDrainOutcome(selected=True)
    except CampaignDeferredError as exc:
        reasons.append(f"campaign_deferred:{campaign.campaign_id}:{exc.reason_code}")
        reasons.extend(exc.reason_codes)
        log.warning("campaign %s drain deferred: %s", campaign.campaign_id, exc)
        if (
            exc.reason_code in {"batch_time_limit", "reconciliation_incomplete"}
            and budget_start_mono is not None
            and deps.monotonic() >= budget_start_mono + deps.max_cycle_seconds
        ):
            retry = _rolling_cooldown_retry_iso(deps, now_iso, ())
            _defer_campaign_drain(deps, campaign.campaign_id, retry, reasons, "batch_cooldown")
        return _CampaignDrainOutcome(selected=True)
    except CampaignError as exc:
        reasons.append(f"campaign_deferred:{campaign.campaign_id}")
        log.warning("campaign %s drain deferred: %s", campaign.campaign_id, exc)
        # F18: a campaign the eligibility gate refuses PURELY on budget exhaustion makes zero
        # submissions every cycle — rolling-window exhaustion is a RECOVERABLE wait, so the
        # campaign is DEFERRED with the earliest rolling-slot retry time (persisted; survives
        # restart) and resumes automatically once the window permits. The old F9
        # ``no_budget_progress`` permanent pause is gone: budget cycles never burn the
        # zero-progress breaker. Finding 4: the structured eligibility verdict gates this branch —
        # a MIXED refusal (budget exhaustion with a drift/source family, which auto-pauses the
        # campaign) must NOT write a deferral marker, only a pure ``budgets_exhausted`` verdict
        # defers. Hard-stop pauses (deadline, drift, attestation, revoked) raise their own
        # CampaignErrors and stay untouched.
        if isinstance(exc, CampaignEligibilityRefusalError) and exc.eligibility_reasons == (
            "budgets_exhausted",
        ):
            retry = _rolling_cooldown_retry_iso(deps, now_iso, ())
            _defer_campaign_drain(deps, campaign.campaign_id, retry, reasons, "budget_cooldown")
        return _CampaignDrainOutcome(selected=True)
    reasons.extend(_aggregate_campaign_reasons(report.reasons))
    key = f"{META_CAMPAIGN_ZERO_PROGRESS}:{campaign.campaign_id}"
    # P2: the breaker keys on REAL progress (submissions/verifications/quarantines), not the
    # scan counter — a batch of per-file deferrals or pending-target walks makes zero
    # submissions and must not reset itself every cycle into endless full sweeps.
    if report.stopped == "plan_complete":
        # P3: a campaign drained to plan_complete is terminal — its gate/zero-progress/deferral
        # metas are cleared (inside apply_campaign); do not re-create any of them here.
        clear_campaign_terminal_meta(deps.store, campaign.campaign_id)
    elif report.progress == 0:
        # F18: recoverable zero-progress (budget window / pending Shoko metadata / pending
        # async verification / transient queue congestion / batch capacity) → durable deferral
        # with the retry time, breaker NOT burned, campaign stays APPROVED. Only a
        # genuinely-unexpected stall (unqueueable rows in operator-review safety states)
        # counts toward the ``no_queueable_candidates`` auto-pause.
        if _zero_progress_is_recoverable(deps, campaign.campaign_id, report.reasons):
            family = _recoverable_stall_family(deps, campaign.campaign_id, report.reasons)
            retry = _rolling_cooldown_retry_iso(deps, now_iso, report.reasons)
            _defer_campaign_drain(deps, campaign.campaign_id, retry, reasons, family)
        elif _record_zero_progress(deps, campaign.campaign_id, now_iso, "no_queueable_candidates"):
            reasons.append(f"campaign_zero_progress_pause:{campaign.campaign_id}")
    else:
        with contextlib.suppress(WriteDisabledError, StateError):
            deps.store.set_meta(key, "0")
            deps.store.clear_meta(f"{META_CAMPAIGN_DEFERRED_UNTIL}:{campaign.campaign_id}")
    return _CampaignDrainOutcome(selected=True, apply_eligible=True, report=report)


def apply_lock_lease_expiry(now_iso: str, seconds: float) -> str:
    """Compute a usable lease before either apply entry point acquires ownership."""
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("sync.apply_lock_lease_seconds must be finite and greater than zero")
    now = _parse_iso(now_iso)
    try:
        expiry = now + timedelta(seconds=seconds)
    except OverflowError as exc:
        raise ValueError(
            "sync.apply_lock_lease_seconds exceeds the supported datetime range"
        ) from exc
    if expiry <= now:
        raise ValueError("sync.apply_lock_lease_seconds is too small to produce a future lease")
    return expiry.isoformat()


def _lock_preflight(
    deps: DaemonDeps, mode: CycleMode, now_iso: str
) -> tuple[str | None, bool, bool]:
    """Apply-lock preflight (APPLY only): returns ``(lock_reason, writes_suppressed, acquired)``.

    A contended lock or a writes-disabled store yields a read-only cycle (the caller
    reports the reason); the lock is held for the duration of the apply cycle. Ownership is
    fenced by the OS advisory lock (F5): the daemon acquires it first and holds it for the
    whole cycle, so a lease timeout can never hand ownership to a second live process.
    """
    writes_suppressed = deps.store.writes_disabled
    if mode is not CycleMode.APPLY or writes_suppressed:
        reason = "writes_disabled" if mode is CycleMode.APPLY else None
        return (reason, writes_suppressed, False)
    try:
        lease_iso = apply_lock_lease_expiry(now_iso, deps.lease_seconds)
    except ValueError as exc:
        return (f"apply_lock_invalid_lease: {exc}", True, False)
    try:
        # F5: exclusive ownership is the OS advisory lock, taken FIRST; a live owner
        # (kernel-held lock) is contended — a meta lease expiry never fences it out. The
        # meta lease is audit/visibility only; an expired lease + a free OS lock steals the
        # lease of a DEAD owner (audit-recorded). Contention → read-only cycle, holder named.
        acquire_or_steal_apply_lock(deps.store, deps.pid, now_iso, lease_expires_iso=lease_iso)
    except LockHeldError as exc:
        holder = exc.holder.owner_pid if exc.holder else "?"
        return (f"apply_lock_contended: holder {holder}", False, False)
    except ApplyLockUnavailableError as exc:
        return (f"apply_lock_unavailable: {exc}", False, False)
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


def _reconcile_mapping_generation(
    deps: DaemonDeps,
    phase: _MappingPhase,
    inventory: SonarrInventory,
    active: ActiveMappings,
    now_iso: str,
) -> tuple[frozenset[int] | None, tuple[str, ...], tuple[str, ...]]:
    if not inventory.verdict.complete:
        # A truncated series inventory cannot account for all durable files. Keep
        # the old marker so the next complete pass repeats generation reconciliation.
        return None, (), ()
    if not phase.reactivated:
        deps.store.set_meta(
            "reconciled_mapping_semantics:v1", json.dumps(_mapping_semantics(active.index))
        )
        return None, (), ()
    series_tvdb = _sonarr_to_tvdb(inventory)
    affected = frozenset(
        sonarr_id for sonarr_id, tvdb in series_tvdb.items() if tvdb in phase.churned
    )
    drift, existing = _apply_generation_change(deps, now_iso, affected)
    # Written AFTER durable quarantine/reopen work. A crash earlier repeats that
    # idempotent work; per-file frozen targets independently fence every rebuild.
    deps.store.set_meta(
        "reconciled_mapping_semantics:v1", json.dumps(_mapping_semantics(active.index))
    )
    return affected, drift, existing


def _incomplete_reconciliation_reasons(
    source: str, complete: bool, reasons: Sequence[str]
) -> tuple[str, ...]:
    """Name an incomplete source and preserve its stable machine-readable causes."""
    if complete:
        return ()
    if reasons:
        return tuple(f"{source}:{reason}" for reason in reasons)
    return (source,)


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
    policy: RunPolicy,
    cycle_key: str,
    full: bool = True,
    campaign_quarantine_limit: int | None = None,
    bounded_read_only: bool = False,
    budget_start_mono: float | None = None,
    resolution_priority_file_ids: frozenset[int] = frozenset(),
) -> CycleReport:
    """Run one reconciliation cycle. See module docstring for the safety contract.

    Order: apply-lock preflight (APPLY only) → legacy attestation gate (advanced mode only;
    APPLY mutates its persisted pause transition, dry-run only warns) → mapping scheduler →
    persisted recovery consumption → inventory → Shoko discovery → baseline establishment →
    source continuity → reconcile matches → (APPLY only) the ordered
    send/verify loop. Advanced daemon policy ``NEW_ARRIVAL`` submits only new arrivals; an
    **approved** backlog campaign drains as one bounded batch per apply cycle on the F7 path when
    ``campaign.daemon_auto_apply`` is set. Explicitly authorized normal service mode uses
    ``RunPolicy.SERVICE`` for backlog and arrivals without campaign preparation. The one-batch CLI
    ``campaign apply`` remains for manual advanced runs.

    M7-b full/light cadence: ``full=True`` (the default — every explicit ``once`` and
    every due full-reconcile) runs the complete cycle. A **light** cycle (``full=False``)
    fetches the **same inventories** with a reduced work budget: the recovery phase cap
    and the apply submission cap are halved, so prompt polling of new/changed series never
    starves the recovery/apply queues; the interval gate (``full_reconcile_interval_hours``
    via the persisted ``next_full_reconcile_iso`` marker) forces a full cycle. True delta
    fetching (Sonarr change cache / Shoko discovery delta) is NOT
    implemented and is documented as the M9 optimization — no endpoints are invented.

    P2-1: the acquired apply lock is released in ``finally`` — a mid-cycle exception can
    no longer wedge it. A non-owner release (the lease was stolen after a crash)
    raises ``LockHeldError``; it is caught in the ``finally`` only, reported, never
    propagated and never wedging the next cycle.

    ``campaign_quarantine_limit`` bounds new campaign identity quarantines created by this
    reconciliation. Automatic drains default it to zero so their bounded campaign loop owns
    those decisions; campaign apply's inner sweep uses the same deferral to avoid pre-loop
    quarantine work escaping its batch size.
    """
    if campaign_quarantine_limit is not None and campaign_quarantine_limit < 0:
        raise ValueError("campaign_quarantine_limit must be non-negative")
    if (
        campaign_quarantine_limit is None
        and mode is CycleMode.APPLY
        and deps.settings.campaign.daemon_auto_apply
    ):
        # Campaign quarantine mutations from the outer reconciliation belong to the same
        # bounded automatic-drain invocation; leave them for its candidate loop to count.
        campaign_quarantine_limit = 0
    lock_reason, writes_suppressed, lock_acquired = _lock_preflight(deps, mode, now_iso)
    if writes_suppressed:
        return _read_only_report(mode, cycle_key, lock_reason or "writes_disabled", None)
    if lock_reason is not None:
        return _read_only_report(mode, cycle_key, lock_reason, None)
    original_budget_limits = deps.store.budget_limits
    try:
        _apply_service_budget_limits(deps)
        cycle_start_mono = deps.monotonic() if budget_start_mono is None else budget_start_mono
        deps = _bind_service_authorization(deps, cycle_start_mono + deps.max_cycle_seconds)
        return _run_cycle_body(
            deps,
            now_iso=now_iso,
            mode=mode,
            cycle_key=cycle_key,
            policy=policy,
            full=full,
            campaign_quarantine_limit=campaign_quarantine_limit,
            bounded_read_only=bounded_read_only,
            budget_start_mono=cycle_start_mono,
            resolution_priority_file_ids=resolution_priority_file_ids,
        )
    finally:
        if deps.service_mode:
            deps.store.budget_limits = original_budget_limits
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


def _bind_service_authorization(deps: DaemonDeps, deadline: float) -> DaemonDeps:
    """Bind production remote checks to one cycle budget, including rate feedback."""
    if not deps.service_mode or deps.service_authorized_for_deadline is None:
        return deps
    authorize = deps.service_authorized_for_deadline
    clock = deps.monotonic

    def bounded_authorization(current_deadline: float) -> bool:
        limit = min(deadline, current_deadline)
        return clock() < limit and authorize(limit) and clock() < limit

    return replace(
        deps,
        service_authorized=lambda: bounded_authorization(deadline),
        service_authorized_for_deadline=bounded_authorization,
    )


def _run_cycle_body(  # noqa: PLR0915 - retain the bounded cycle phase ordering
    deps: DaemonDeps,
    *,
    now_iso: str,
    mode: CycleMode,
    cycle_key: str,
    policy: RunPolicy,
    full: bool,
    campaign_quarantine_limit: int | None,
    bounded_read_only: bool,
    budget_start_mono: float | None,
    resolution_priority_file_ids: frozenset[int],
) -> CycleReport:
    """The cycle body — runs with the apply lock held (APPLY) under the caller's
    ``try/finally`` that guarantees the release (P2-1). ``full=False`` is the M7-b light
    cycle (same inventories, halved recovery/apply work budgets; see :func:`run_cycle`).
    """
    cycle_start_mono = deps.monotonic() if budget_start_mono is None else budget_start_mono
    reasons: list[str] = []
    drift: list[str] = []
    existing_link: list[str] = []
    scan_context = scan_context_identity(
        deps.store, deps.settings.sonarr.url, deps.settings.shoko.url, settings=deps.settings
    )

    if not deps.service_mode:
        reasons.extend(attestation_gate(deps, now_iso, mutate=mode is CycleMode.APPLY))

    phase = _mapping_phase(deps, now_iso)
    service_mapping_refresh_failed = _service_mapping_refresh_failed(deps, phase, reasons)
    # Reconcile already-sent jobs before taking the discovery/continuity snapshot. Otherwise
    # an async link that landed since the previous cycle looks like an unexplained source
    # drop until recovery verifies it, spuriously latching a hard rebaseline hold. Recovery
    # is bounded and read-only remotely; it never submits new links.
    reasons.extend(_recovery_phase(deps, now_iso, cycle_start_mono, full=full))
    inventory = (
        _collect_bounded_sonarr_inventory(deps, cycle_start_mono)
        if mode is CycleMode.APPLY or bounded_read_only
        else collect_sonarr_inventory(deps.sonarr, deps.settings)
    )
    discovery = (
        _collect_bounded_shoko_discovery(deps, cycle_start_mono, now_iso)
        if mode is CycleMode.APPLY or bounded_read_only
        else collect_shoko_discovery(deps.shoko)
    )
    reconciliation_ready = (
        inventory.verdict.complete and discovery.verdict.complete and phase.active is not None
    )
    reasons.extend(
        _incomplete_reconciliation_reasons(
            "sonarr_inventory_incomplete",
            inventory.verdict.complete,
            tuple(reason.value for reason in inventory.verdict.reasons),
        )
    )
    reasons.extend(
        _incomplete_reconciliation_reasons(
            "shoko_snapshot_incomplete",
            discovery.verdict.complete,
            tuple(reason.value for reason in discovery.verdict.reasons),
        )
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
        discovered_file_ids=frozenset(file.id for file in discovery.files),
    )
    if not continuity.ok or not discovery_complete:
        reasons.extend(continuity.reasons)
    elif discovery.total_reported is not None:
        # P1-1: a fully-complete, non-truncated, continuity-OK cycle re-anchors the
        # observation, so ordinary inventory growth never accumulates as discontinuity.
        # A latched discontinuity takes the requires_rebaseline branch above.
        deps.store.record_source_observation(
            _source_identity(deps.settings),
            snapshot_id,
            iso=now_iso,
            roots_fingerprint=roots_fingerprint(deps.settings),
            discovery_total=discovery.total_reported,
        )
    if not continuity.ok and not continuity.requires_rebaseline:
        # A departed in-flight job is not evidence of either a source reset or a
        # successful link. Hold both normal apply and campaign sweeps/drains until
        # bounded recovery can explain the snapshot; never re-anchor this wait.
        reconciliation_ready = False

    active = phase.active
    if active is None:
        return _unavailable_service_mapping_report(
            deps, mode, cycle_key, phase, reasons, drift, now_iso
        )

    resolution_priority_file_ids = _service_canary_resolution_priority(
        deps, resolution_priority_file_ids
    )

    affected_sonarr_series, gen_drift, gen_existing = _reconcile_mapping_generation(
        deps, phase, inventory, active, now_iso
    )
    drift.extend(gen_drift)
    existing_link.extend(gen_existing)
    if phase.churn_paused:
        reasons.append("mapping_churn")

    reconcile = _reconcile(
        deps,
        inventory,
        discovery,
        active,
        now_iso,
        scan_context=scan_context,
        policy=policy,
        campaign_quarantine_limit=campaign_quarantine_limit,
        affected_sonarr_series=affected_sonarr_series,
        resolution_limit=(
            MAX_SERVICE_CYCLE_SUBMISSIONS if mode is CycleMode.APPLY or bounded_read_only else None
        ),
        resolution_deadline=(
            _resolution_deadline(deps, cycle_start_mono)
            if mode is CycleMode.APPLY or bounded_read_only
            else None
        ),
        resolution_priority_file_ids=resolution_priority_file_ids,
    )
    existing_link.extend(reconcile.existing_link_drift)
    for file_id, skip_reason in sorted(reconcile.skip_reasons.items()):
        reasons.append(f"skip:{file_id}:{skip_reason}")
        if skip_reason == "service_mapping_target_changed":
            drift.append(f"mapping_changed: file {file_id} immutable targets quarantined")

    flags = _eligibility_flags(deps, mapping_present=True, now_iso=now_iso)
    eligibility = compute_eligibility(deps, now_iso)
    cycle_eligible = eligibility.eligible and continuity.ok
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
        and not service_mapping_refresh_failed
    )
    submitted, deferred, metadata = _run_apply(
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
        policy,
        mode,
        full=full,
        metadata_demands=reconcile.metadata_demands,
    )

    # F7: drain ONE bounded batch of the oldest approved backlog campaign through the full
    # campaign guard chain, under this cycle's apply lock and durable budget key (lock_held).
    # Only in APPLY mode
    # on a reconciliation-ready cycle, and only when the operator opted in
    # (campaign.daemon_auto_apply). F8: this gate does NOT require the daemon's own apply
    # eligibility — an authorized campaign drains even when the daemon lease is invalid (the
    # campaign applies its own campaign-scope lease and eligibility inside apply_campaign).
    # F24: a mapping-churn pause still suppresses new-arrival writes, while an approved campaign
    # revalidates each candidate against immutable AniDB target identities before any POST.
    # Manual CLI campaign apply stays fully available.
    campaign_drain = _CampaignDrainOutcome()
    if (
        mode is CycleMode.APPLY
        and reconciliation_ready
        and not deps.service_mode
        and deps.settings.campaign.daemon_auto_apply
    ):
        campaign_drain = _drain_campaign(
            deps,
            now_iso,
            reasons,
            cycle_key=cycle_key,
            fresh_campaign_quarantines=reconcile.fresh_campaign_quarantines,
            budget_start_mono=cycle_start_mono,
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
        writes_suppressed=(not apply_running and not campaign_drain.apply_eligible),
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
        full=full,
        plans=reconcile.plans,
        folders=discovery.folders,
        campaign=campaign_drain.report,
        new_arrival_apply_eligible=cycle_eligible,
        campaign_drain_selected=campaign_drain.selected,
        campaign_apply_eligible=campaign_drain.apply_eligible,
        new_arrival_submitted=submitted,
        campaign_submitted=(
            campaign_drain.report.submitted if campaign_drain.report is not None else 0
        ),
        total_link_requests_attempted=(
            submitted + campaign_drain.report.submitted
            if campaign_drain.report is not None
            else submitted
        ),
        new_arrival_writes_suppressed=not apply_running,
        campaign_writes_suppressed=not campaign_drain.apply_eligible,
        mapping_snapshot=(
            active.revision,
            active.content_sha256,
            active.targets_digest,
            active.structure_digest,
            active.overrides_applied,
        ),
        campaign_identity_drift_details=reconcile.campaign_identity_drift_details,
        exclusion_counts=reconcile.exclusion_counts,
        scope_excluded_count=reconcile.scope_excluded_count,
        resolution_deferred_file_ids=reconcile.resolution_deferred_file_ids,
        metadata_requests_attempted=metadata.attempted,
        metadata_deferred=metadata.deferred,
        metadata_observations=metadata.observations,
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
        failed=_is_transport_failure(phase.last_outcome),
    )


def _unavailable_service_mapping_report(
    deps: DaemonDeps,
    mode: CycleMode,
    cycle_key: str,
    phase: _MappingPhase,
    reasons: list[str],
    drift: list[str],
    now_iso: str,
) -> CycleReport:
    reasons.append("mapping_unavailable")
    if deps.service_mode and mode is CycleMode.APPLY:
        _observe_service_rate_hold(deps, now_iso, reasons, drift)
    return _unavailable_report(mode, cycle_key, phase, tuple(reasons), deps)


def _load_service_rate_state(deps: DaemonDeps, now_iso: str) -> RateState:
    encoded = deps.store.get_service_rate_state_json()
    if encoded is None:
        state = RateState.initial(now_iso)
        deps.store.save_service_rate_state_json(encode_rate_state(state), now_iso)
        return state
    try:
        return decode_rate_state(encoded)
    except (StateError, ValueError):
        # Corrupt durable feedback can only restore the one-send cautious starting cap.
        state = RateState.initial(now_iso)
        deps.store.save_service_rate_state_json(encode_rate_state(state), now_iso)
        return state


class _MetadataRateHealth(Enum):
    NORMAL_WAIT = "normal_wait"
    API_FAILURE = "api_failure"
    SAFETY_HOLD = "safety_hold"


def _metadata_rate_health(reason: str) -> _MetadataRateHealth | None:
    """Classify exact metadata outcomes without granting file-success credit.

    Ordinary waits/yields are not failures. Unknown future outcomes refuse a
    healthy-cycle ramp, so new metadata branches cannot silently escape feedback.
    """
    if reason.startswith("metadata_recovery:"):
        status = reason.rsplit(":", 1)[-1]
        health_by_status = dict.fromkeys(
            ("unsent_released", "ready", "waiting_metadata", "waiting_queue", "deadline"),
            _MetadataRateHealth.NORMAL_WAIT,
        )
        health_by_status.update(
            dict.fromkeys(
                ("queue_unknown", "state_unavailable"),
                _MetadataRateHealth.API_FAILURE,
            )
        )
        return health_by_status.get(status, _MetadataRateHealth.SAFETY_HOLD)
    if reason.startswith("metadata_recovery_deferred:"):
        return _MetadataRateHealth.NORMAL_WAIT
    if not reason.startswith("metadata_"):
        return None
    if reason in {
        "metadata_disabled",
        "metadata_anime_not_required",
        "metadata_operation_retained",
        "metadata_operation_exists",
        "metadata_service_not_eligible",  # includes ordinary durable budget headroom waits
        "metadata_waiting_external_queue",
        "metadata_waiting_external_file_queue",
        "metadata_queue_busy",
        "metadata_replan",
        "metadata_not_sent",  # transport proves the deadline expired before any HTTP
        "metadata_accepted_waiting",
        "metadata_pending_files_exhausted",
        "metadata_pending_episode_jobs_exhausted",
        "metadata_cycle_new_exhausted",
        "metadata_rolling_new_exhausted",
    }:
        return _MetadataRateHealth.NORMAL_WAIT
    if reason in {
        "metadata_queue_unavailable_or_blocked",
        "metadata_queue_not_clear",
        "metadata_targets_unavailable",
        "metadata_sonarr_source_changed_or_unavailable",
        "metadata_delivery_unknown",
        "metadata_state_or_guard_unavailable",
    }:
        return _MetadataRateHealth.API_FAILURE
    # Includes changed authorization/generation/demand, protected-file refusal,
    # overdue verification and invalid coverage, rather than guessing safety.
    return _MetadataRateHealth.SAFETY_HOLD


def _update_service_rate_state(
    deps: DaemonDeps,
    state: RateState,
    *,
    now_iso: str,
    queue: QueueSnapshot | None,
    submitted: int,
    verified: int,
    reasons: Sequence[str],
    drift: Sequence[str],
    deadline: float | None = None,
) -> RateState:
    if deadline is not None:
        deps = _bind_service_authorization(deps, deadline)
    if not deps.service_mode:
        return state
    configured = deps.store.budget_limits
    limits = None if configured is None else service_budget_limits(configured)
    budget_view = deps.store.budget_view()
    all_reasons = tuple(reasons) + tuple(drift)
    metadata_health = {_metadata_rate_health(reason) for reason in all_reasons}
    mapping_markers = (
        "mapping_unstable",
        "mapping_churn",
        "mapping_generation_changed",
        "mapping_changed",
        "mapping_source_transport_failure",
        "mapping_source_invalid",
        "source_unstable",
        "source_discontinuity",
        "source_identity_drift",
    )
    failure_markers = (
        "transport_failure",
        "queue_unknown",
        "api_failure",
        "http_",
        "throttl",
    )
    anomaly_markers = (
        *mapping_markers,
        "queue_blocked",
        "onboarding_not_authorized",
        "service_authorization_changed",
        "service_canary_failed",
        "existing_link drift",
        "preflight_blocked_safety",
        "unsafe_shoko_settings",
        "writes_disabled",
        "recovery_poll",
        "recovery_hold",
        "verification:pending",
        "verification:partial",
        "verification:contradiction",
        "verification:unknown",
        "conflict",
        "indeterminate",
        "identity_drift",
    )
    canary = deps.store.get_service_canary()
    signals = RateSignals(
        queue_healthy=(
            queue is not None and queue.blocked_count <= deps.settings.sync.queue_blocked_tolerance
        ),
        pending_episode_jobs=budget_view.pending_episode_jobs,
        max_pending_episode_jobs=(0 if limits is None else limits.max_pending_episode_jobs),
        submitted=submitted,
        verified=verified,
        api_healthy=(
            _MetadataRateHealth.API_FAILURE not in metadata_health
            and not any(marker in reason for reason in all_reasons for marker in failure_markers)
        ),
        mapping_stable=not any(
            marker in reason for reason in all_reasons for marker in mapping_markers
        ),
        authorization_valid=deps.service_authorized(),
        anomaly_free=(
            _MetadataRateHealth.SAFETY_HOLD not in metadata_health
            and not any(marker in reason for reason in all_reasons for marker in anomaly_markers)
        ),
        canary_verified=canary is not None and canary[1] == "verified",
    )
    next_state = advance_rate(
        state,
        signals,
        now_iso=now_iso,
        hard_cycle_cap=min(
            deps.settings.sync.max_new_per_cycle,
            MAX_SERVICE_CYCLE_SUBMISSIONS,
        ),
    )
    if next_state != state:
        deps.store.save_service_rate_state_json(encode_rate_state(next_state), now_iso)
    return next_state


def _observe_service_rate_hold(
    deps: DaemonDeps, now_iso: str, reasons: Sequence[str], drift: Sequence[str]
) -> None:
    state = _load_service_rate_state(deps, now_iso)
    _update_service_rate_state(
        deps,
        state,
        now_iso=now_iso,
        queue=None,
        submitted=0,
        verified=0,
        reasons=reasons,
        drift=drift,
    )


def _apply_cycle_limit(deps: DaemonDeps, *, full: bool) -> int:
    if deps.service_mode:
        configured = min(
            deps.max_submissions,
            deps.settings.sync.max_new_per_cycle,
            MAX_SERVICE_CYCLE_SUBMISSIONS,
        )
        return max(1, configured // 2) if not full and configured > 0 else configured
    return deps.max_submissions if full else max(1, deps.max_submissions // 2)


@dataclass(frozen=True)
class _MetadataScheduleReport:
    attempted: int = 0
    deferred: int = 0
    observations: tuple[tuple[int, str], ...] = ()


def _schedule_metadata_demands(  # noqa: PLR0912 - ordered bounded batch admission and execution
    deps: DaemonDeps,
    demands: Mapping[int, MetadataDemand],
    flags: _EligibilityFlags,
    active: ActiveMappings,
    folders: Mapping[int, ImportFolder],
    cycle_key: str,
    *,
    remaining_submissions: int,
    deadline: float,
) -> _MetadataScheduleReport:
    """After ready links, consider bounded distinct anime using one complete file each."""
    if not deps.service_mode or not demands:
        return _MetadataScheduleReport()
    # Pick a reproducible supporting file, without manufacturing a partial demand.
    groups: dict[int, MetadataDemand] = {}
    for file_id in sorted(demands):
        demand = demands[file_id]
        if not _metadata_file_current(deps, demand):
            continue
        for anime, _kind, _number in sorted(
            demand.missing_targets, key=lambda target: (target[0], target[1].value, target[2])
        ):
            groups.setdefault(anime, demand)
    if not groups:
        return _MetadataScheduleReport()
    source_key = metadata_source_key(deps.shoko)
    for anime, demand in list(groups.items())[:20]:
        with contextlib.suppress(StateError):
            observe_metadata_status(
                deps.store, source_key, anime, "metadata_waiting", deps.now(), demand=demand
            )
    if current_metadata_maintenance_policy(deps.store) != METADATA_MAINTENANCE_POLICY:
        return _MetadataScheduleReport(
            deferred=len(groups), observations=((0, "metadata_disabled"),)
        )
    canary_hold = _service_canary_hold_reason(deps)
    if canary_hold is not None or remaining_submissions <= 0:
        return _MetadataScheduleReport(
            deferred=len(groups),
            observations=() if canary_hold is None else ((0, canary_hold),),
        )
    admission = _metadata_queue_admission(deps, groups, cycle_key, deadline)
    if isinstance(admission, str):
        for anime, demand in list(groups.items())[:20]:
            with contextlib.suppress(StateError):
                observe_metadata_status(
                    deps.store, source_key, anime, admission, deps.now(), demand=demand
                )
        return _MetadataScheduleReport(
            deferred=len(groups),
            observations=tuple(
                (anime, admission) for anime in sorted(groups)[:MAX_SERVICE_CYCLE_SUBMISSIONS]
            ),
        )
    cursor_key = "service_metadata_schedule_cursor:v1"
    try:
        cursor = int(deps.store.get_meta(cursor_key) or "0")
    except ValueError:
        cursor = 0
    ordered = sorted(groups)
    ordered = [anime for anime in ordered if anime > cursor] + [
        anime for anime in ordered if anime <= cursor
    ]
    attempted = 0
    observations: list[tuple[int, str]] = []
    # A blocked first group cannot monopolize each cycle's bounded planning work.
    for anime in ordered[:MAX_SERVICE_CYCLE_SUBMISSIONS]:
        if (
            attempted >= remaining_submissions
            or deps.monotonic() >= deadline
            or deps.shutdown_requested()
            or deps.store.writes_disabled
        ):
            break
        outcome = process_metadata_demand(
            deps,
            groups[anime],
            anime,
            active,
            folders,
            flags,
            cycle_key,
            deadline=deadline,
            _batch=admission,
        )
        attempted += outcome.attempted
        observations.append((anime, outcome.reason))
        with contextlib.suppress(StateError):
            observe_metadata_status(
                deps.store, source_key, anime, outcome.reason, deps.now(), demand=groups[anime]
            )
        deps.store.set_meta(cursor_key, str(anime))
        if outcome.reason not in {
            "metadata_accepted_waiting",
            "metadata_operation_retained",
            "metadata_replan",
            "metadata_anime_not_required",
        }:
            # An uncertain/refused transport or lost guard invalidates the remaining batch.
            break
    return _MetadataScheduleReport(attempted, len(groups) - attempted, tuple(observations))


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
    policy: RunPolicy,
    mode: CycleMode,
    full: bool = True,
    metadata_demands: Mapping[int, MetadataDemand] | None = None,
) -> tuple[int, int, _MetadataScheduleReport]:
    """Drive the apply loop and fold its reasons/drift into the cycle's lists."""
    if not apply_running:
        if deps.service_mode and mode is CycleMode.APPLY:
            _observe_service_rate_hold(deps, now_iso, reasons, drift)
        return (0, 0, _MetadataScheduleReport())
    queue = fetch_queue(
        deps.shoko, deadline=cycle_start_mono + deps.max_cycle_seconds, clock=deps.monotonic
    )
    configured_limit = _apply_cycle_limit(deps, full=full)
    rate_state = _load_service_rate_state(deps, now_iso) if deps.service_mode else None
    max_submissions = (
        effective_submission_cap(rate_state, configured_limit)
        if rate_state is not None
        else configured_limit
    )
    submitted, deferred, apply_reasons, apply_drift, verified = _apply(
        deps,
        plans,
        flags,
        active,
        folders,
        queue,
        cycle_key,
        now_iso,
        cycle_start_mono,
        max_submissions,
        policy=policy,
    )
    reasons.extend(apply_reasons)
    drift.extend(apply_drift)
    if deps.service_mode:
        with contextlib.suppress(StateError):
            observe_resolved_metadata_plans(
                deps.store, metadata_source_key(deps.shoko), plans, deps.now()
            )
    metadata = _schedule_metadata_demands(
        deps,
        metadata_demands or {},
        flags,
        active,
        folders,
        cycle_key,
        remaining_submissions=max(0, configured_limit - submitted),
        deadline=cycle_start_mono + deps.max_cycle_seconds,
    )
    reasons.extend(reason for _anime, reason in metadata.observations)
    current_queue_reasons = queue_reasons(queue)
    reasons.extend(current_queue_reasons)
    if rate_state is not None:
        try:
            if deps.store.writes_disabled:
                reasons.append("writes_disabled")
            else:
                _update_service_rate_state(
                    deps,
                    rate_state,
                    now_iso=deps.now(),
                    queue=queue,
                    submitted=submitted,
                    verified=verified,
                    reasons=reasons,
                    drift=drift,
                    deadline=cycle_start_mono + deps.max_cycle_seconds,
                )
        except WriteDisabledError:
            reasons.append("writes_disabled")
    return (submitted, deferred, metadata)


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
    return outcome is RefreshOutcome.FAILED_TRANSPORT


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
        seconds = capped_backoff(base_seconds, consecutive_failures - 1, MAX_BACKOFF_SECONDS)
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
    full_cycle: Callable[[str], bool] | None = None,
    on_cycle_complete: Callable[[CycleReport], None] | None = None,
    inter_cycle_wait: Callable[[float], None] | None = None,
) -> DaemonReport:
    """Poll cycles forever (or ``stop_after_cycles``), sleeping ``interval_seconds``
    with jitter between them; a shutdown request is honoured at the ``should_stop`` check
    **after** the current cycle completes and **immediately on waking** from the inter-cycle
    sleep (no mid-cycle abort, and no new cycle is started after a wake).

    Transport-level cycle failures grow the sleep with exponential backoff (24h cap,
    jitter); a successful cycle resets it. The persisted budget ``next_due``/pause
    state in the stores means a restart neither disables refreshes nor tight-loops.

    ``full_cycle`` (M7-b cadence): when supplied, each cycle's ``full`` flag comes from
    ``full_cycle(now)`` — the CLI wires it to :func:`full_cycle_due` (interval-gated
    full vs light cycle). Without it every cycle is full (M6 behavior, default).

    ``on_cycle_complete`` (F4): invoked immediately after EVERY cycle completes, before
    the shutdown check and the sleep. The CLI wires it to persist cycle
    metadata mid-run, log a concise summary, and let the report be garbage-collected. The
    daemon keeps only a bounded history (:data:`DAEMON_HISTORY_MAXLEN` for an unbounded
    run, every cycle for a finite ``stop_after_cycles`` run) plus accurate
    ``total_cycles``/``total_failed`` counts, so an unattended daemon never retains
    a per-cycle report list that grows without bound. A callback failure is logged,
    never fatal to the loop.

    ``inter_cycle_wait`` (F6): the seam used for the inter-cycle sleep, distinct from
    ``deps.advance`` (which paces *in-cycle* verification polls). The CLI wires it to an
    interruptible event-backed sleeper so a shutdown signal wakes the daemon promptly
    while in-cycle pacing keeps its normal delay. Default: ``deps.advance``.
    """
    history: deque[CycleReport] = deque(
        maxlen=DAEMON_HISTORY_MAXLEN if stop_after_cycles is None else stop_after_cycles
    )
    failures = 0
    index = 0
    total_cycles = 0
    total_failed = 0
    last_cycle: CycleReport | None = None
    shutdown_requested = False
    exit_reason = "stop_after_cycles"
    first_cycle = True
    while True:
        # F6: after waking from an interruptible sleep, exit before starting another cycle
        # (the first cycle always runs, matching the "SIGTERM after the current cycle" contract).
        if not first_cycle and should_stop is not None and should_stop():
            shutdown_requested = True
            exit_reason = "shutdown_requested"
            break
        iso = now_iso()
        report = run_cycle(
            deps,
            now_iso=iso,
            mode=mode,
            policy=policy,
            cycle_key=cycle_key(index),
            full=True if full_cycle is None else full_cycle(iso),
        )
        index += 1
        total_cycles += 1
        failures = failures + 1 if report.failed else 0
        total_failed += 1 if report.failed else 0
        last_cycle = report
        history.append(report)
        if on_cycle_complete is not None:
            try:
                on_cycle_complete(report)
            except Exception:
                logging.getLogger(__name__).warning(
                    "cycle completion callback failed", exc_info=True
                )
        first_cycle = False
        if should_stop is not None and should_stop():
            shutdown_requested = True
            exit_reason = "shutdown_requested"
            break
        if stop_after_cycles is not None and index >= stop_after_cycles:
            exit_reason = "stop_after_cycles"
            break
        sleep_seconds = _compute_sleep(interval_seconds, failures, jitter_ratio, deps.jitter)
        wait = inter_cycle_wait if inter_cycle_wait is not None else deps.advance
        wait(sleep_seconds)
    return DaemonReport(
        cycles=tuple(history),
        exit_reason=exit_reason,
        shutdown_requested=shutdown_requested,
        total_cycles=total_cycles,
        total_failed=total_failed,
        last_cycle=last_cycle,
    )
