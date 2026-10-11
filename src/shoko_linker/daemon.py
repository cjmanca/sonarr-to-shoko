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
* **Only the ``new_arrival`` cohort auto-applies in daemon cycles.** Backlog files are never
  auto-submitted as new arrivals. An **approved** backlog campaign is drained by the daemon
  (F7, when ``campaign.daemon_auto_apply`` is set) as ONE bounded batch per apply cycle, after
  the operator's explicit approval + canary, through the full campaign guard chain under the
  cycle's apply lock — the plan's approval digest, campaign status, deadline, budgets,
  write gate, campaign-scope attestation, queue guard, and mapping revalidation all stay
  authoritative. The one-batch CLI ``campaign apply`` remains for manual operation; a daemon
  starting with ``apply_changes=true`` still never submits an unapproved backlog.
  New arrivals inside ``sync.new_file_grace_period_seconds`` (default 300s, measured from
  the persisted ``first_seen_iso``) defer with ``grace_period`` — they stay ``planned``
  and consume no budget slot — so Shoko's post-arrival import state settles first.
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
import logging
import os
import time
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from enum import Enum
from types import MappingProxyType
from typing import Any, Final

import httpx

from . import mappings as _mappings
from .attestation import SCOPE_DAEMON, AttestationVerdict
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
from .mapping_store import (
    status as mapping_status,
)
from .mappings import MappingIndex, ResolutionStatus, TargetIdentity, effective_targets
from .mappings import resolve as resolve_mapping
from .policies import RunPolicy
from .shoko import ImportFolder, ShokoDiscovery, collect_shoko_discovery, match_shoko_files
from .sonarr import SonarrFile, SonarrInventory, collect_sonarr_inventory
from .state import (
    ATTESTATION_PAUSE_REASON,
    CAMPAIGN_STATUS_PAUSED,
    META_ATTESTATION_GATE,
    META_CAMPAIGN_ATTESTATION_GATE,
    META_NEXT_FULL_RECONCILE_ISO,
    ApplyLockUnavailableError,
    BaselineFile,
    BudgetRefusal,
    Cohort,
    EligibilityVerdict,
    FileIdentitySnapshot,
    FileRecord,
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
# Matches the ``grace_period`` retryable label in :mod:`shoko_linker.state` (reason taxonomy).
GRACE_PERIOD_REASON: Final[str] = "grace_period"

#: Bounded daemon history (F4): an unbounded daemon retains only the last N cycle reports
#: (older reports are released for GC); a bounded ``stop_after_cycles`` run retains
#: every executed cycle (the run is finite). ``DaemonReport.total_cycles``/``total_failed``
#: are the accurate counts and never depend on this bound.
DAEMON_HISTORY_MAXLEN: Final[int] = 100


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
    endpoints, fake clocks, and tmp stores. Defaults are safe-closed (no attestation,
    no jitter, monotonic clock) so a partially wired daemon never writes.
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
    ``apply_eligible`` is the composed eligibility verdict. ``campaign`` is the bounded
    campaign-batch summary (:class:`~shoko_linker.campaign.CampaignApplyReport`) drained in
    this cycle under the cycle's own apply lock (F7), or ``None`` when no batch ran.
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
    writes ONLY — it must never block an authorized campaign drain. The bypass is **origin-gated**:
    it applies only to the pause the attestation gate itself recorded (gate meta
    ``paused`` AND reason ``attestation_expired``); an operator ``pause-writes --reason
    attestation_expired`` is refused at the CLI, and any other global pause reason — mapping
    churn, source break, queue anomaly, a plain operator pause — gates every scope.
    """
    reason = deps.store.budget_view().paused_reason
    if reason is None:
        return None
    if (
        campaign
        and reason == ATTESTATION_PAUSE_REASON
        and deps.store.get_meta(META_ATTESTATION_GATE) == "paused"
    ):
        return None
    return reason


def _budget_headroom(deps: DaemonDeps, *, campaign: bool = False) -> bool:
    """Non-mutating peek: is there room for at least one more submission?

    The authoritative reservation happens in ``prepare_send``; this only feeds the
    cycle-level eligibility verdict so a saturated daemon suppresses apply early.
    """
    view = deps.store.budget_view()
    limits = deps.store.budget_limits
    if _global_pause_reason(deps, campaign=campaign) is not None:
        return False
    if limits is None:
        return True
    if view.pending_file_requests >= limits.max_pending_files:
        return False
    if view.pending_episode_jobs >= limits.max_pending_episode_jobs:
        return False
    return view.rolling_new_submissions < limits.rolling_new


def _eligibility_flags(
    deps: DaemonDeps, mapping_present: bool, *, campaign: bool = False
) -> _EligibilityFlags:
    source_stable = deps.store.get_meta("requires_rebaseline") != "1"
    paused = _global_pause_reason(deps, campaign=campaign) is not None
    return _EligibilityFlags(
        attestation=deps.attestation_valid(),
        gate=apply_gate(deps.settings),
        budgets_ok=_budget_headroom(deps, campaign=campaign),
        mapping_stable=mapping_present and not paused,
        source_stable=source_stable,
    )


def compute_eligibility(
    deps: DaemonDeps,
    now_iso: str,  # noqa: ARG001
    *,
    campaign: bool = False,
) -> EligibilityVerdict:
    """Compose the cycle-level auto-apply verdict from every safety input.

    Refuses when any of: the apply gate is closed, the attestation is missing, budgets
    are exhausted, no active mapping generation is present, the source is discontinuous,
    or writes are paused. Daemon cycles consult this in APPLY mode; campaign apply consults
    it with ``campaign=True`` so a daemon-lease write pause never blocks an authorized campaign.
    """
    active = load_active(deps.mapping_store, _load_overrides(deps))
    flags = _eligibility_flags(deps, active is not None, campaign=campaign)
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


# ------------------------------------------------------------------ attestation gate


def _clear_gate_pause(
    deps: DaemonDeps, gate_key: str, now_iso: str, is_campaign: bool
) -> tuple[str, ...]:
    """Clear a persisted pause on the first invalid→valid transition for one scope."""
    reasons: list[str] = []
    gate = deps.store.get_meta(gate_key)
    if (
        not is_campaign
        and gate == "paused"
        and deps.store.budget_view().paused_reason == ATTESTATION_PAUSE_REASON
    ):
        with contextlib.suppress(WriteDisabledError, StateError):
            deps.store.resume(now_iso)
            deps.store.set_meta(gate_key, "valid")
        reasons.append("attestation_resumed")
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
        with contextlib.suppress(WriteDisabledError, StateError):
            if not is_campaign:
                deps.store.pause(ATTESTATION_PAUSE_REASON, now_iso)
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
    ``META_ATTESTATION_GATE``; the invalid→valid transition clears it only when the pause reason
    is exactly ``attestation_expired`` (a mapping-churn pause is never cleared here). The
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
    *,
    policy: RunPolicy = RunPolicy.NEW_ARRIVAL,
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
            policy=policy,
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


def verify_and_close(
    deps: DaemonDeps, plan: LinkPlan, file_id: int, cycle_key: str, now_iso: str
) -> str:
    """Public inline verification wrapper (campaign apply reuses the daemon's poll loop)."""
    return _verify_and_close(deps, plan, file_id, cycle_key, now_iso)


def xref_fetcher(endpoint: ApiEndpoint, file_id: int) -> Callable[[], ShokoFileXRefs | None]:
    """Public alias for the XRef poll fetcher (campaign verification path)."""
    return _xref_fetcher(endpoint, file_id)


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


def _recovery_phase(
    deps: DaemonDeps, now_iso: str, cycle_start_mono: float, *, full: bool = True
) -> tuple[str, ...]:
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
    cap = (
        deps.max_recovery_rows_per_cycle if full else max(1, deps.max_recovery_rows_per_cycle // 2)
    )
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


def _candidate_freshness_guard(
    deps: DaemonDeps, active: ActiveMappings, send_iso: str
) -> _ApplyOutcome | None:
    """F11: per-candidate freshness refusals — deferral (stays ``planned``/queueable), never
    a quarantine, never a POST. The lease/gate freshness is owned by the
    per-candidate eligibility + preflight below (F2/P2-4)."""
    if deps.shutdown_requested():
        return _ApplyOutcome(0, 1, ("shutdown_requested",), ())
    if mapping_generation_stale(deps, active, send_iso):
        # The plan was frozen against the guard-time generation: a swap defers the candidate
        # (the next cycle re-validates), never POSTs on the stale digests.
        return _ApplyOutcome(0, 1, ("mapping_generation_changed",), ())
    return None


def _post_preflight_freshness_guard(
    deps: DaemonDeps, active: ActiveMappings, send_iso: str
) -> _ApplyOutcome | None:
    """P2-1: the guard re-run between preflight's network phase and the intent commit.

    Preflight's File + per-episode GETs are slow (10s connect / 30s read), so a
    shutdown, generation swap, lease expiry, or gate close that lands DURING them must not ride the
    pre-preflight verdict into ``prepare_send``. This re-evaluation is purely local (store meta +
    the disk lease + config gate) — no network, no polling.
    """
    guard = _candidate_freshness_guard(deps, active, send_iso)
    if guard is not None:
        return guard
    if not deps.attestation_valid() or not apply_gate(deps.settings):
        # Transient safety (F8 semantics): defer, never quarantine.
        return _ApplyOutcome(0, 1, ("attestation_expired",), ())
    return None


def _close_send(
    deps: DaemonDeps, plan: LinkPlan, file_id: int, cycle_key: str, send_iso: str, send: Any
) -> _ApplyOutcome:
    """Close the durable send row's outcome: inline verify on acceptance, no resubmit ever."""
    if send.outcome is SendOutcome.AMBIGUOUS_SEND:
        return _ApplyOutcome(
            1, 0, (), (f"ambiguous_send: file {file_id} - verify, never resubmit",)
        )
    if send.outcome is not SendOutcome.ACCEPTED_UNVERIFIED:
        return _ApplyOutcome(1, 0, (), ())
    return _ApplyOutcome(1, 0, (), (_verify_and_close(deps, plan, file_id, cycle_key, send_iso),))


def _process_candidate(
    deps: DaemonDeps,
    plan: LinkPlan,
    file_id: int,
    flags: _EligibilityFlags,
    active: ActiveMappings,
    folders: Mapping[int, ImportFolder],
    queue: QueueSnapshot | None,
    cycle_key: str,
) -> _ApplyOutcome:
    """One file's guarded send: fresh safety inputs → eligibility → preflight → prepare →
    mark_sent → submit → verify (F11: every submission timestamp comes from the fresh clock)."""
    # F2: the attestation lease and the apply gate are re-evaluated per candidate, not from the
    # cycle-start snapshot, so a lease that expires (or a gate that closes) mid-cycle stops the
    # remaining submissions. A blocked candidate defers (stays planned) — never quarantined.
    attestation_ok = deps.attestation_valid()
    gate_open = apply_gate(deps.settings)
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
        attestation_valid=attestation_ok,
        apply_gate_open=gate_open,
        queue=queue,
        active_targets_digest=active.targets_digest,
        active_structure_digest=active.structure_digest,
        blocked_over=effective_blocked_over(deps),
        mapping_index=active.index,
    )
    if not gate.ok:
        reason = gate.reasons[0].value if gate.reasons else "preflight_blocked"
        drift: tuple[str, ...] = ()
        if PreflightReason.MAPPING_CHANGED in gate.reasons:
            _advance(deps, file_id, FileState.WAITING, "mapping_drift", send_iso)
            drift = (f"mapping_changed: file {file_id} in-flight plan drift, no POST",)
        return _ApplyOutcome(0, 0, (f"preflight_{reason}",), drift)
    # P2-1: preflight's network phase is slow — re-take the fresh clock and re-run the LOCAL
    # evaluation (shutdown, generation swap, lease verdict, gate) immediately before the intent
    # commit, so an authorization that expired mid-preflight never authorizes this POST.
    send_iso = deps.now()
    stale_after = _post_preflight_freshness_guard(deps, active, send_iso)
    if stale_after is not None:
        return stale_after
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
    token: SendToken = prepared
    # mark_sent durably commits request_may_have_been_sent BEFORE any bytes reach HTTP.
    deps.store.mark_sent(token, iso=send_iso)
    send = submit_link(deps.shoko, plan, clock=deps.monotonic)
    deps.store.record_send_outcome(
        token, send.outcome, iso=send_iso, http_status=send.http_status, detail=send.detail
    )
    return _close_send(deps, plan, file_id, cycle_key, send_iso, send)


def mapping_generation_identity(deps: DaemonDeps, now_iso: str) -> tuple[int, str, str, str] | None:
    """The disk-active generation's identity tuple, ``None`` when the active cache is absent.

    Cheap by design (meta + content digest read, no index parse): F11 re-checks it per submission
    so a mapping refresh that swaps generations mid-batch is observed *before* the next POST.
    """
    status = mapping_status(deps.mapping_store, _parse_iso(now_iso))
    if not status.active_present or status.content_sha256 is None:
        return None
    return (
        status.revision,
        status.content_sha256,
        status.targets_digest or "",
        status.structure_digest or "",
    )


def mapping_generation_stale(deps: DaemonDeps, active: ActiveMappings, now_iso: str) -> bool:
    """True when the disk-active generation is no longer the one the guards validated against."""
    return mapping_generation_identity(deps, now_iso) != (
        active.revision,
        active.content_sha256,
        active.targets_digest,
        active.structure_digest,
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
) -> tuple[int, int, tuple[str, ...], tuple[str, ...]]:
    """The apply path over eligible new-arrival candidates in deterministic order.

    ``max_submissions`` bounds **POSTs attempted**, not enumerated files; the rest stay
    ``planned`` (deferred). Returns ``(submitted, deferred, reasons, drift)``.

    New arrivals inside the ``sync.new_file_grace_period_seconds`` window defer
    with the ``grace_period`` reason: the row stays ``planned``, consumes **no** budget
    reservation (no ``prepare_send``), and is never quarantined — the next cycle that
    starts after the window closes submits it. Backlog/campaign files are outside this
    new-arrival loop (the daemon never submits them as new arrivals); approved backlog
    campaigns drain on the separate F7 path (see :func:`_drain_campaign`), and the CLI
    ``campaign apply`` is the operator's manual act on aged, plan-approved inventory.
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
            deps, plans[file_id], file_id, flags, active, folders, queue, cycle_key
        )
        submitted += outcome.submitted
        deferred += outcome.deferred
        reasons.extend(outcome.reasons)
        drift.extend(outcome.drift)
    return submitted, deferred, tuple(reasons), tuple(drift)


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
    "budget_refusal:",
    "send_stale:",
    "shutdown_requested:",
)

#: Consecutive zero-progress drain cycles before the campaign auto-pauses
#: (F7 review P2-3: an all-quarantined campaign must not burn full sweeps forever).
ZERO_PROGRESS_PAUSE_CYCLES: Final[int] = 3


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


def _record_zero_progress(deps: DaemonDeps, campaign_id: str, now_iso: str, reason: str) -> bool:
    """Count consecutive zero-progress drain cycles; pause at the bound.

    ``reason`` names the auto-pause: ``no_queueable_candidates`` for a campaign whose remaining
    files are unqueueable, ``no_budget_progress`` (F9) for one whose submissions were every
    budget-refused — a budget-exhausted campaign must trip the breaker, not stall forever.
    Returns True when the campaign was auto-paused."""
    key = f"campaign_zero_progress:{campaign_id}"
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


def _drain_campaign(deps: DaemonDeps, now_iso: str, reasons: list[str]) -> Any | None:
    """Drain **one** bounded batch of the oldest approved campaign (F7), under this cycle's lock.

    Runs entirely through :func:`shoko_linker.campaign.apply_campaign` with
    ``lock_held=True`` (the daemon cycle already owns the OS apply lock), so the FULL
    guard chain applies unchanged: plan digest, status/deadline/max-submissions, write gate,
    campaign-scope attestation (an expiring lease pauses writes and stops the batch, zero
    POSTs, the daemon stays alive read-only), queue snapshot fail-closed, mapping/drift
    revalidation, eligibility, binding quarantine, preflight → prepare_send → mark_sent →
    submit_link → record → inline verify, and restart recovery via the durable rows.

    A missing plan path or an unreadable/malformed plan file defers (a named reason, no
    crash, no POSTs); a campaign-state refusal defers the batch and is reported. Returns
    the bounded :class:`~shoko_linker.campaign.CampaignApplyReport`, or ``None``.
    """
    from .campaign import (
        CampaignError,
        CampaignPlanInvalidError,
        apply_campaign,
        select_next_daemon_campaign,
    )

    log = logging.getLogger(__name__)
    campaign = select_next_daemon_campaign(deps.store)
    if campaign is None:
        return None
    if campaign.plan_path is None:
        reasons.append(f"campaign_deferred:{campaign.campaign_id}:plan_path_missing")
        _pause_deferred_campaign(deps, campaign.campaign_id, "plan_path_missing", now_iso)
        return None
    campaign_deps = _campaign_scoped_deps(deps, campaign.campaign_id)
    try:
        report = apply_campaign(
            campaign_deps,
            deps.settings,
            deps.store,
            campaign.plan_path,
            now_iso,
            lock_held=True,
        )
    except CampaignPlanInvalidError as exc:
        reasons.append(f"campaign_deferred:{campaign.campaign_id}:plan_unreadable")
        _pause_deferred_campaign(deps, campaign.campaign_id, "plan_unreadable", now_iso)
        log.warning(
            "campaign %s drain deferred: plan file unreadable (%s): paused, zero POSTs",
            campaign.campaign_id,
            exc,
        )
        return None
    except CampaignError as exc:
        reasons.append(f"campaign_deferred:{campaign.campaign_id}")
        log.warning("campaign %s drain deferred: %s", campaign.campaign_id, exc)
        # F9: a campaign the eligibility gate refuses purely on budget exhaustion makes zero
        # submissions every cycle — feed the same zero-progress breaker so it
        # auto-pauses (``no_budget_progress``) instead of churning full sweeps forever.
        if "budgets_exhausted" in str(exc) and _record_zero_progress(
            deps, campaign.campaign_id, now_iso, "no_budget_progress"
        ):
            reasons.append(f"campaign_zero_progress_pause:{campaign.campaign_id}")
        return None
    reasons.extend(_aggregate_campaign_reasons(report.reasons))
    key = f"campaign_zero_progress:{campaign.campaign_id}"
    # P2: the breaker keys on REAL progress (submissions/verifications/quarantines), not the
    # scan counter — a batch of per-file budget deferrals or pending-target walks makes zero
    # submissions and must trip the breaker, not reset it every cycle.
    if report.progress == 0:
        # F9: a zero-progress batch driven purely by budget refusals pauses as
        # ``no_budget_progress`` (the campaign is budget-blocked, not unqueueable);
        # any other zero-progress stall keeps the existing ``no_queueable_candidates``.
        zero_reason = (
            "no_budget_progress"
            if any(reason.startswith("budget_refusal:") for reason in report.reasons)
            else "no_queueable_candidates"
        )
        if _record_zero_progress(deps, campaign.campaign_id, now_iso, zero_reason):
            reasons.append(f"campaign_zero_progress_pause:{campaign.campaign_id}")
    else:
        with contextlib.suppress(WriteDisabledError, StateError):
            deps.store.set_meta(key, "0")
    return report


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
    if mode is not CycleMode.APPLY:
        return (None, writes_suppressed, False)
    if writes_suppressed:
        return ("writes_disabled", True, False)
    lease = _parse_iso(now_iso) + timedelta(seconds=deps.lease_seconds)
    try:
        # F5: exclusive ownership is the OS advisory lock, taken FIRST; a live owner
        # (kernel-held lock) is contended — a meta lease expiry never fences it out. The
        # meta lease is audit/visibility only; an expired lease + a free OS lock steals the
        # lease of a DEAD owner (audit-recorded). Contention → read-only cycle, holder named.
        acquire_or_steal_apply_lock(
            deps.store, deps.pid, now_iso, lease_expires_iso=lease.isoformat()
        )
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
) -> CycleReport:
    """Run one reconciliation cycle. See module docstring for the safety contract.

    Order: apply-lock preflight (APPLY only) → attestation gate (APPLY mutates the
    persisted pause transition; dry-run only warns) → mapping scheduler → Sonarr
    inventory → Shoko discovery → baseline establishment → source continuity →
    persisted recovery consumption → reconcile matches → (APPLY only) the ordered
    send/verify loop. ``policy`` names the daemon's cohort policy (only ``NEW_ARRIVAL``
    auto-applies in the new-arrival loop; an **approved** backlog campaign drains as one
    bounded batch per apply cycle on the F7 path when ``campaign.daemon_auto_apply`` is set,
    and the one-batch CLI ``campaign apply`` remains for manual runs).

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
    """
    lock_reason, writes_suppressed, lock_acquired = _lock_preflight(deps, mode, now_iso)
    if writes_suppressed:
        return _read_only_report(mode, cycle_key, "writes_disabled", None)
    if lock_reason is not None:
        return _read_only_report(mode, cycle_key, lock_reason, None)
    try:
        return _run_cycle_body(
            deps, now_iso=now_iso, mode=mode, cycle_key=cycle_key, policy=policy, full=full
        )
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
    deps: DaemonDeps,
    *,
    now_iso: str,
    mode: CycleMode,
    cycle_key: str,
    policy: RunPolicy,
    full: bool,
) -> CycleReport:
    """The cycle body — runs with the apply lock held (APPLY) under the caller's
    ``try/finally`` that guarantees the release (P2-1). ``full=False`` is the M7-b light
    cycle (same inventories, halved recovery/apply work budgets; see :func:`run_cycle`).
    """
    cycle_start_mono = deps.monotonic()
    reasons: list[str] = []
    drift: list[str] = []
    existing_link: list[str] = []

    reasons.extend(attestation_gate(deps, now_iso, mutate=mode is CycleMode.APPLY))

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
    # per-cycle recovery row cap and the monotonic cycle budget (M7-b light cycles
    # run with the halved cap).
    reasons.extend(_recovery_phase(deps, now_iso, cycle_start_mono, full=full))

    reconcile = _reconcile(deps, inventory, discovery, active, now_iso, policy=policy)
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
        full=full,
    )

    # F7: drain ONE bounded batch of the oldest approved backlog campaign through the full
    # campaign guard chain, under this same cycle's apply lock (lock_held). Only in APPLY mode
    # on a reconciliation-ready, churn-stable cycle, and only when the operator opted in
    # (campaign.daemon_auto_apply). F8: this gate does NOT require the daemon's own apply
    # eligibility — an authorized campaign drains even when the daemon lease is invalid (the
    # campaign applies its own campaign-scope lease and eligibility inside apply_campaign).
    # Manual CLI campaign apply stays fully available.
    campaign_report: Any | None = None
    if (
        mode is CycleMode.APPLY
        and reconciliation_ready
        and not phase.churn_paused
        and deps.settings.campaign.daemon_auto_apply
    ):
        campaign_report = _drain_campaign(deps, now_iso, reasons)

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
        full=full,
        plans=reconcile.plans,
        folders=discovery.folders,
        campaign=campaign_report,
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
    full: bool = True,
) -> tuple[int, int]:
    """Drive the apply loop and fold its reasons/drift into the cycle's lists."""
    if not apply_running:
        return (0, 0)
    queue = fetch_queue(deps.shoko)
    max_submissions = deps.max_submissions if full else max(1, deps.max_submissions // 2)
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
        max_submissions,
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
