"""Command line interface.

``main(argv)`` returns an exit code; it never raises for expected failures.

Exit-code matrix (M7-b):

    0  success — a completed cycle, a mapping refresh that fetched/304'd
        (``UNCHANGED``/``NOT_DUE``/``ACTIVATED``), an informational ``mappings status``
        (an absent generation is reported, not an error), ``report``/``health``/``audit``,
        a confirmed operator action, a clean ``daemon`` exit, a campaign prepare/approve/
        report/pause/resume/revoke that completed
    2  configuration/request invalid (the message names the failed field, never a
        secret value; includes an unconfirmed operator command, bad flags, a malformed
        plan file — on **any** campaign command, including ``apply`` (plan-file parse/
        digest-format errors are invalid input, not a state refusal) — and an
        ``approve`` digest mismatch, the refusal prints **both** digests)
    3  apply-gate refusal (live writes requested without the default-deny gate opening;
        campaign-state ``apply`` refusals — not-approved/revoked/paused/deadline/
        max-submissions/plan drift/attestation/eligibility — land here too)
    4  runtime/network failure (mapping-source fetch failure, a transport-failed cycle,
        an audit whose Shoko listing failed)
    5  state error (the state store refused: illegal transition, held apply lock,
        stopped writes, budget/record refusals)

Wiring notes:

* Endpoint/store construction is centralized in the module-level factory seams
  (:func:`open_state_store`, :func:`open_mapping_store`, :func:`sonarr_endpoint`,
  :func:`shoko_endpoint`, :func:`source_endpoint`, :func:`current_iso`,
  :func:`attestation_input`, :func:`attestation_verdict_for`, :func:`sleep`) so offline
  tests inject ``httpx.MockTransport`` endpoints, fake clocks and tmp stores. Production
  code always uses the real factories.
* M7-b supplies the real safety attestation lease: :func:`attestation_input` evaluates
  ``safety.attestation_file`` offline (endpoint/scope/expiry/version checks, no
  network). Absent/invalid/expired suppresses every apply and persists one write pause
  per transition (see :func:`shoko_linker.daemon.attestation_gate`).
* API key values never enter output: reports carry the environment-variable *names*.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import math
import os
import signal
import sys
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import FrameType
from typing import Any, Final, Literal, cast

from .action_history import action_history_page, action_history_view
from .attestation import (
    SCOPE_DAEMON,
    AttestationError,
    AttestationRecord,
    AttestationVerdict,
    build_record,
    campaign_lease_path,
    lease_expiry_cap,
    load_attestation,
    parse_attestation_document,
    validate_scope,
    write_attestation,
)
from .audit import collect_audit_inputs, run_audit
from .bootstrap import DEFAULT_DATABASE_PATH, BootstrapSettings, load_bootstrap_settings
from .campaign import (
    DRAIN_AUTHORIZATION_RULE,
    CampaignDigestMismatchError,
    CampaignError,
    CampaignPlanInvalidError,
    apply_campaign,
    approve_campaign,
    authorize_campaign_drain,
    campaign_deadline_warning,
    campaign_drain_selection,
    campaign_pause,
    campaign_report,
    campaign_resume,
    campaign_revoke,
    campaign_verify,
    plan_digest,
    prepare_campaign,
    read_plan_file,
    revoke_campaign_drain,
    write_plan_file,
)
from .config import (
    APPLY_ENV_VAR,
    DEFAULT_CONFIG_PATH,
    Settings,
    apply_gate,
    apply_gate_status,
    load_settings,
    parse_runtime_options,
)
from .daemon import (
    AttestationScopeHooks,
    CycleMode,
    CycleReport,
    DaemonDeps,
    _no_shutdown_requested,
    compute_eligibility,
    full_cycle_due,
    run_cycle,
    run_daemon,
)
from .dashboard import (
    DashboardApplication,
    DashboardStatus,
    DashboardTable,
    create_dashboard_server,
)
from .errors import ConfigError, HttpPolicyError, LinkerError, PolicyError
from .file_display import file_display, tracked_file_display
from .http import ApiEndpoint, make_shoko_client, make_sonarr_client
from .logging_setup import configure_logging, register_secret
from .mapping_store import (
    MappingStatus,
    MappingStore,
    RefreshOutcome,
    load_active,
    make_source_endpoint,
)
from .mapping_store import (
    refresh as mapping_refresh,
)
from .mapping_store import (
    status as mapping_status,
)
from .metadata_operations import metadata_source_key, recover_metadata_operations
from .metadata_status import MetadataStatusView, metadata_status_view
from .onboarding import OnboardingContext, OnboardingManager, OnboardingStatus, ScopeBinding
from .policies import PolicyRequest, RunPolicy, parse_policy
from .reporting import (
    META_CYCLE_ALIVE,
    META_CYCLE_ALL_WRITES_SUPPRESSED,
    META_CYCLE_APPLY_ELIGIBLE,
    META_CYCLE_CAMPAIGN_ELIGIBLE,
    META_CYCLE_CAMPAIGN_ID,
    META_CYCLE_CAMPAIGN_REMAINING,
    META_CYCLE_CAMPAIGN_SELECTED,
    META_CYCLE_CAMPAIGN_SUBMITTED,
    META_CYCLE_CAMPAIGN_SUPPRESSED,
    META_CYCLE_FAILED,
    META_CYCLE_KEY,
    META_CYCLE_MODE,
    META_CYCLE_NEW_ARRIVAL_ELIGIBLE,
    META_CYCLE_NEW_ARRIVAL_SUBMITTED,
    META_CYCLE_NEW_ARRIVAL_SUPPRESSED,
    META_CYCLE_RECONCILIATION_READY,
    META_CYCLE_SUBMITTED,
    META_CYCLE_TOTAL_LINK_REQUESTS,
    META_OPERATOR_ACTIONS,
    REFRESH_SUCCESS_OUTCOMES,
    audit_payload,
    audit_summary_line,
    campaign_apply_payload,
    campaign_apply_summary_line,
    cycle_report_payload,
    cycle_summary_line,
    daemon_report_payload,
    health_payload,
    json_payload,
    mapping_status_payload,
    operator_action_payload,
    refresh_payload,
    report_payload,
)
from .scan_status import last_scan_evidence, scan_context_identity, scan_exclusion_page
from .service import (
    SERVICE_POLL_SECONDS,
    bootstrap_endpoints,
    probe_connections,
    run_dashboard_worker,
    settings_for_scope,
)
from .service_settings import (
    ServiceSettingsController,
    apply_service_settings,
    configured_bootstrap,
    load_service_settings,
)
from .service_setup import ServiceSetupController
from .state import (
    ATTESTATION_PAUSE_REASON,
    META_LAST_CYCLE_END_ISO,
    META_LAST_CYCLE_START_ISO,
    META_LAST_FULL_RECONCILE_ISO,
    META_NEXT_CYCLE_DUE_ISO,
    META_NEXT_FULL_RECONCILE_ISO,
    META_WRITES_DISABLED_REASON,
    BudgetLimits,
    EligibilityVerdict,
    StateError,
    StateStore,
    WriteDisabledError,
    steal_apply_lock,
)

type CommandHandler = Callable[[argparse.Namespace], int]
type SignalHandler = Callable[[int, FrameType | None], Any]

EXIT_OK: Final[int] = 0
EXIT_CONFIG_INVALID: Final[int] = 2
EXIT_APPLY_GATE_REFUSAL: Final[int] = 3
EXIT_RUNTIME: Final[int] = 4
EXIT_STATE: Final[int] = 5

_POLICY_CHOICES: Final[list[str]] = [
    policy.value for policy in RunPolicy if policy is not RunPolicy.SERVICE
]
DASHBOARD_PORT_MAX: Final[int] = 65535
DASHBOARD_BIND_HOST: Final[str] = "0.0.0.0"  # noqa: S104 - container port is unpublished by default
_NOT_IMPLEMENTED_LIVE_READ_ONLY: Final[str] = (
    "validate --live-read-only: not implemented — live read-only checks arrive in "
    "the M7-b slice; no network call was made"
)
_REFUSED_APPLY: Final[str] = (
    "REFUSED: live writes stay disabled. Enabling them requires "
    f"'sync.apply_changes: true' in the config AND {APPLY_ENV_VAR}=1 set by the "
    "operator. Nothing was sent to Sonarr or Shoko."
)
_CONFIRM_REQUIRED: Final[str] = (
    "refused: this operator command changes durable state and requires the explicit "
    "--confirm flag (plus --note TEXT recording why); nothing was written"
)
_RESOLVE_REPLAN_NOTE: Final[str] = (
    "the closed send authorizes only a future re-plan with a FRESH request "
    "fingerprint; the resolved fingerprint is never resubmitted"
)
#: Human-output cap for the campaign report's attempted-send audit (F19);
#: the ``--json`` payload always carries the full list.
ATTEMPTED_SEND_DISPLAY_LIMIT: Final[int] = 10
_ATTESTATION_NOTE: Final[str] = (
    "safety attestation: the daemon lease at safety.attestation_file and each campaign "
    "lease in its campaign-<id> sibling are evaluated offline before every apply; an invalid "
    "lease suppresses only its own scope's writes and persists one pause per transition (the "
    "daemon stays alive reporting read-only)"
)


# ------------------------------------------------------------------ dependency seams


def current_iso() -> str:
    """The wall clock for cycle timestamps (test seam)."""
    return datetime.now(UTC).isoformat()


def attestation_target_path(
    settings: Settings, scope: str = SCOPE_DAEMON, *, for_write: bool = False
) -> Path | None:
    """Resolve the lease file path for one scope from ``safety.attestation_file`` (test seam).

    The daemon lease stays at the configured path. A ``campaign:<id>`` lease lives in its
    deterministic sibling: writing (``for_write=True``) always targets the sibling so recording
    a campaign lease never clobbers the daemon lease; reading resolves the sibling when present
    and falls back to the configured path only for backward compatibility (a campaign record the
    operator wrote directly at ``attestation_file``).
    """
    if settings.safety.attestation_file is None:
        return None
    base = Path(settings.safety.attestation_file)
    validated = validate_scope(scope)
    if validated == SCOPE_DAEMON:
        return base
    campaign_id = validated.removeprefix("campaign:")
    sibling = campaign_lease_path(base, campaign_id)
    if for_write:
        return sibling
    return sibling if sibling.exists() else base


def attestation_verdict_for(
    settings: Settings, *, scope: str = SCOPE_DAEMON, version_hint: str | None = None
) -> AttestationVerdict:
    """Evaluate the configured safety attestation lease offline (test seam, no network).

    The Shoko URL identity, the requested ``scope`` (``daemon`` for the daemon/
    ``once --apply``; ``campaign:<id>`` for a campaign apply — each evaluated from its own
    lease file), the configured warn lead, the configured version pin
    (``safety.attested_shoko_version`` — M7 review P3-4: a lease recorded for a different
    Shoko version is ``stale_version``, fail closed; an explicit ``version_hint``
    overrides it), and :func:`current_iso` are the only inputs — nothing is fetched, nothing
    is guessed. An unconfigured lease is ``absent``.
    """
    hint = settings.safety.attested_shoko_version if version_hint is None else version_hint
    return load_attestation(
        attestation_target_path(settings, scope),
        shoko_url=settings.shoko.url,
        now_iso=current_iso(),
        scope=validate_scope(scope),
        version_hint=hint,
        warn_lead_seconds=settings.safety.attestation_warn_seconds,
    )


def attestation_input(settings: Settings, *, scope: str = SCOPE_DAEMON) -> bool:
    """The apply-eligibility input: True only for a valid, in-scope, live lease.

    M7 review P3-4: the configured ``safety.attested_shoko_version`` is supplied as the
    version hint, so a lease recorded for a different Shoko version fails closed
    (``stale_version``) on every apply path that consults this input.
    """
    return attestation_verdict_for(
        settings, scope=scope, version_hint=settings.safety.attested_shoko_version
    ).valid_for_apply


def sleep(seconds: float) -> None:
    """In-cycle pacing (test seam; also the ``DaemonDeps.advance`` default).

    Plain wall-clock delay: in-cycle verification polls keep their pacing even after a
    shutdown request (F6 review: a shared interruptible seam would turn remaining polls
    into a GET burst). The inter-cycle sleep uses :func:`interruptible_sleep` instead.
    """
    time.sleep(seconds)


def interruptible_sleep(seconds: float) -> None:
    """Inter-cycle sleep wired to the process-wide shutdown event (F6, test seam).

    Production: a SIGTERM/SIGINT arriving during a long inter-cycle sleep wakes the
    daemon promptly (the handler sets the event) instead of waiting out the full interval —
    after which Docker escalates to SIGKILL. Tests inject a deterministic fake via this seam.
    """
    _SHUTDOWN_WAKE.wait(timeout=seconds)


def budget_limits_from_settings(settings: Settings) -> BudgetLimits:
    """Map the configured budgets onto the state store's :class:`BudgetLimits`.

    ``max_pending_files`` uses ``max_new_per_cycle``: a cycle never submits more files
    than that ceiling, so it also bounds in-flight pending file requests (0 disables
    submissions).
    """
    sync = settings.sync
    return BudgetLimits(
        max_pending_files=sync.max_new_per_cycle,
        max_pending_episode_jobs=sync.max_pending_episode_jobs,
        per_cycle_new=sync.max_new_per_cycle,
        rolling_new=sync.rolling_new_limit,
        rolling_window_hours=sync.rolling_window_hours,
    )


def open_state_store(settings: Settings) -> StateStore:
    """Open the durable state store (test seam; the caller closes it)."""
    limits = budget_limits_from_settings(settings)
    return StateStore(Path(settings.database.path), budget_limits=limits)


def state_path(settings: Settings) -> Path:
    """Resolve the state DB path (test seam mirroring :func:`open_state_store`)."""
    return Path(settings.database.path)


def open_mapping_store(settings: Settings) -> MappingStore:
    """Open the Anime-Lists cache (derived directory, test seam)."""
    return MappingStore.from_settings(settings)


def sonarr_endpoint(settings: Settings) -> ApiEndpoint:
    """Real Sonarr endpoint (test seam)."""
    return make_sonarr_client(settings)


def shoko_endpoint(settings: Settings) -> ApiEndpoint:
    """Real Shoko endpoint (test seam)."""
    return make_shoko_client(settings)


def source_endpoint(store: MappingStore, settings: Settings) -> ApiEndpoint:
    """Real mapping-source endpoint (test seam): origin-only, identity encoding."""
    return make_source_endpoint(
        store,
        connect_seconds=settings.sync.connect_timeout_seconds,
        read_seconds=settings.sync.read_timeout_seconds,
    )


def read_overrides_text(settings: Settings) -> str | None:
    """Operator overrides text when configured; a configured file must be readable."""
    if settings.mappings.overrides_file is None:
        return None
    path = Path(settings.mappings.overrides_file)
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise ConfigError(
            f"mappings.overrides_file {path} is not valid UTF-8 text (at byte offset "
            f"{exc.start}: {exc.reason})"
        ) from exc
    except OSError as exc:
        detail = exc.strerror or str(exc)
        raise ConfigError(f"cannot read mappings.overrides_file {path}: {detail}") from exc


def _campaign_attestation_hooks(settings: Settings) -> Callable[[str], AttestationScopeHooks]:
    """Factory mapping a campaign id to its ``campaign:<id>`` attestation callables (F7)."""

    def build(campaign_id: str) -> AttestationScopeHooks:
        scope = f"campaign:{campaign_id}"
        return AttestationScopeHooks(
            valid=lambda: attestation_input(settings, scope=scope),
            verdict=lambda: attestation_verdict_for(
                settings, scope=scope, version_hint=settings.safety.attested_shoko_version
            ),
        )

    return build


def build_daemon_deps(
    settings: Settings,
    *,
    store: StateStore,
    mapping_store: MappingStore,
    max_submissions: int,
    overrides_text: str | None = None,
    attestation_scope: str = SCOPE_DAEMON,
    shutdown_requested: Callable[[], bool] | None = None,
) -> DaemonDeps:
    """Wire one :class:`~shoko_linker.daemon.DaemonDeps` from validated settings."""
    return DaemonDeps(
        settings=settings,
        store=store,
        mapping_store=mapping_store,
        sonarr=sonarr_endpoint(settings),
        shoko=shoko_endpoint(settings),
        mapping_source=source_endpoint(mapping_store, settings),
        overrides_text=overrides_text,
        monotonic=time.monotonic,
        advance=sleep,
        jitter=time_rng_unit,
        pid=os.getpid(),
        now=current_iso,
        shutdown_requested=shutdown_requested or _no_shutdown_requested,
        attestation_valid=lambda: attestation_input(settings, scope=attestation_scope),
        attestation_verdict=lambda: attestation_verdict_for(
            settings,
            scope=attestation_scope,
            version_hint=settings.safety.attested_shoko_version,
        ),
        lease_seconds=settings.sync.apply_lock_lease_seconds,
        blocked_over=settings.sync.queue_blocked_tolerance,
        verification_timeout_seconds=settings.sync.verification_timeout_seconds,
        max_submissions=max_submissions,
        max_cycle_seconds=settings.sync.max_cycle_seconds,
        churn_limit=settings.sync.mapping_churn_limit,
        max_recovery_rows_per_cycle=settings.sync.max_recovery_rows_per_cycle,
        campaign_attestation=_campaign_attestation_hooks(settings),
    )


def time_rng_unit() -> float:
    """Jitter source in ``[0, 1)`` (test seam)."""
    return time.time() % 1.0


# ------------------------------------------------------------------ signal handling


_SHUTDOWN_WAKE: Final[threading.Event] = threading.Event()
# Process-wide interrupt target for the inter-cycle :func:`sleep`. ``_run_daemon`` clears it
# at startup; the signal handlers set it so a sleeping daemon wakes immediately (F6).


@dataclass
class StopState:
    """Shutdown flag set by the installed signal handlers, plus an event that wakes an
    interruptible inter-cycle sleep the moment a shutdown signal arrives (F6)."""

    stopped: bool = False
    handlers: list[int] = field(default_factory=list)
    shutdown_event: threading.Event = field(default_factory=threading.Event)

    def should_stop(self) -> bool:
        return self.stopped


def make_signal_handlers(state: StopState) -> list[int]:
    """Install SIGINT (and SIGTERM when the platform defines it) handlers.

    Windows note: ``signal.SIGINT`` is the Ctrl+C path; ``signal.SIGTERM`` exists on
    Windows and is registered when present (CPython emulates it for ``os.kill``). The
    handlers only set the flag and wake :attr:`StopState.shutdown_event` —
    :func:`shoko_linker.daemon.run_daemon` checks ``should_stop`` **after** the current
    cycle completes and **immediately on waking** from the (event-backed) inter-cycle sleep,
    so a signal never aborts a cycle mid-write and never starts a new cycle after a wake.
    """
    installed: list[int] = []
    numbers: list[int] = [int(signal.SIGINT)]
    if hasattr(signal, "SIGTERM"):
        numbers.append(int(signal.SIGTERM))
    for number in numbers:
        signal.signal(number, _stop_handler(state))
        installed.append(number)
    state.handlers = installed
    return installed


def _stop_handler(state: StopState) -> SignalHandler:
    def handler(_signum: int, _frame: FrameType | None) -> None:
        # Async-signal-safe: a plain attribute assignment plus Event.set() — the events
        # wake the interruptible sleep so a SIGTERM during a long sleep is honoured promptly
        # instead of waiting out the interval (Docker would escalate to SIGKILL).
        state.stopped = True
        state.shutdown_event.set()
        _SHUTDOWN_WAKE.set()

    return handler


# ------------------------------------------------------------------ cycle bookkeeping meta


def _iso_plus(iso: str, seconds: float) -> str:
    parsed = datetime.fromisoformat(iso)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return (parsed.astimezone(UTC) + timedelta(seconds=seconds)).isoformat()


def _start_cycle_meta(store: StateStore, cycle_key: str, iso: str, interval_seconds: float) -> None:
    """Persist cycle start bookkeeping (best-effort: a stopped store never crashes a cycle)."""
    with contextlib.suppress(WriteDisabledError, StateError):
        store.set_meta(META_LAST_CYCLE_START_ISO, iso)
        store.set_meta(META_CYCLE_KEY, cycle_key)
        store.set_meta(META_NEXT_CYCLE_DUE_ISO, _iso_plus(iso, interval_seconds))


def _begin_budget_cycle(store: StateStore, label: str, iso: str, fallback: str) -> str:
    """Allocate this cycle's durable budget-window key (F9), best-effort.

    The key is unique across process restarts (persisted run-id + monotonic sequence), so a
    new process never collides with a prior run's persisted per-cycle counters. A stopped store
    falls back to a label-derived key for reporting only — with writes disabled no
    submission reserves a slot, so the fallback row is inert.
    """
    with contextlib.suppress(WriteDisabledError, StateError):
        return store.begin_cycle(label, iso)
    return fallback


def _set_cycle_campaign_meta(store: StateStore, report: CycleReport) -> None:
    """Persist a bounded per-cycle campaign-batch summary (F7) on the cycle-meta path.

    Fixed meta keys, overwritten every cycle (no per-cycle list grows): the counts and the
    last-drained campaign id advance mid-run for ``report``/``health`` freshness while
    the F4 bounded-history invariants stay intact.
    """
    campaign = getattr(report, "campaign", None)
    if campaign is not None:
        store.set_meta(META_CYCLE_CAMPAIGN_ID, campaign.campaign_id)
        store.set_meta(META_CYCLE_CAMPAIGN_SUBMITTED, str(campaign.submitted))
        store.set_meta(META_CYCLE_CAMPAIGN_REMAINING, str(campaign.remaining))
    else:
        store.set_meta(META_CYCLE_CAMPAIGN_ID, "")
        store.set_meta(META_CYCLE_CAMPAIGN_SUBMITTED, "0")
        store.set_meta(META_CYCLE_CAMPAIGN_REMAINING, "")


def persist_cycle_meta(
    store: StateStore,
    report: CycleReport,
    *,
    started_iso: str,
    ended_iso: str,
    interval_seconds: float,
    full_reconcile_hours: float,
) -> None:
    """Persist one cycle's observability levels and cadence markers.

    The full-reconcile markers advance **only on full cycles** (M7-b): a light cycle
    leaves ``next_full_reconcile_iso`` untouched, so the interval gate forces the next
    full pass — light cycles can run between full reconciles but never starve them.
    """
    with contextlib.suppress(WriteDisabledError, StateError):
        store.set_meta(META_CYCLE_KEY, report.cycle_key)
        store.set_meta(META_CYCLE_MODE, report.mode.value)
        store.set_meta(META_CYCLE_ALIVE, "1" if report.alive else "0")
        store.set_meta(META_CYCLE_RECONCILIATION_READY, "1" if report.reconciliation_ready else "0")
        store.set_meta(META_CYCLE_APPLY_ELIGIBLE, "1" if report.apply_eligible else "0")
        store.set_meta(
            META_CYCLE_NEW_ARRIVAL_ELIGIBLE,
            "1" if report.new_arrival_apply_eligible else "0",
        )
        store.set_meta(META_CYCLE_CAMPAIGN_SELECTED, "1" if report.campaign_drain_selected else "0")
        store.set_meta(META_CYCLE_CAMPAIGN_ELIGIBLE, "1" if report.campaign_apply_eligible else "0")
        store.set_meta(META_CYCLE_FAILED, "1" if report.failed else "0")
        store.set_meta(META_CYCLE_SUBMITTED, str(report.new_arrival_submitted))
        store.set_meta(META_CYCLE_NEW_ARRIVAL_SUBMITTED, str(report.new_arrival_submitted))
        store.set_meta(META_CYCLE_TOTAL_LINK_REQUESTS, str(report.total_link_requests_attempted))
        store.set_meta(
            META_CYCLE_NEW_ARRIVAL_SUPPRESSED,
            "1" if report.new_arrival_writes_suppressed else "0",
        )
        store.set_meta(
            META_CYCLE_CAMPAIGN_SUPPRESSED,
            "1" if report.campaign_writes_suppressed else "0",
        )
        store.set_meta(META_CYCLE_ALL_WRITES_SUPPRESSED, "1" if report.writes_suppressed else "0")
        _set_cycle_campaign_meta(store, report)
        store.set_meta(META_LAST_CYCLE_START_ISO, started_iso)
        store.set_meta(META_LAST_CYCLE_END_ISO, ended_iso)
        store.set_meta(META_NEXT_CYCLE_DUE_ISO, _iso_plus(ended_iso, interval_seconds))
        if not report.full:
            return
        due_raw = store.get_meta(META_NEXT_FULL_RECONCILE_ISO)
        due_at = datetime.fromisoformat(due_raw) if due_raw is not None else None
        if due_at is not None and due_at.tzinfo is None:
            due_at = due_at.replace(tzinfo=UTC)
        if due_at is None or ended_iso >= due_at.isoformat():
            store.set_meta(META_LAST_FULL_RECONCILE_ISO, started_iso)
            store.set_meta(
                META_NEXT_FULL_RECONCILE_ISO,
                _iso_plus(started_iso, full_reconcile_hours * 3600.0),
            )


def _record_operator_action(
    store: StateStore, command: str, note: str, iso: str, result: Mapping[str, Any]
) -> None:
    """Append one entry to the persisted operator-action audit list (best-effort)."""
    payload = operator_action_payload(command, note, iso=iso, result=result)
    raw = store.get_meta(META_OPERATOR_ACTIONS)
    entries: list[Any] = []
    if raw is not None:
        with contextlib.suppress(ValueError):
            document = json.loads(raw)
            if isinstance(document, list):
                entries = document
    entries.append(payload)
    with contextlib.suppress(WriteDisabledError, StateError):
        store.set_meta(META_OPERATOR_ACTIONS, json.dumps(entries[-50:], sort_keys=True))


# ------------------------------------------------------------------ commands


def _run_validate(args: argparse.Namespace) -> int:
    if cast(bool, args.live_read_only):
        print(_NOT_IMPLEMENTED_LIVE_READ_ONLY)
        return EXIT_RUNTIME
    settings = load_settings(cast(str, args.config))
    print_config_summary(settings)
    print("Configuration is valid. Offline checks only: no Sonarr or Shoko request was made.")
    return EXIT_OK


def _run_once(args: argparse.Namespace) -> int:
    """One reconciliation cycle: dry-run by default, gate-enforced apply on request."""
    settings = load_settings(cast(str, args.config))
    request = PolicyRequest(parse_policy(cast(str, args.policy))).validated()
    apply_requested = cast(bool, args.apply)
    if apply_requested and cast(bool, args.dry_run):
        print("request invalid: --dry-run and --apply cannot be combined; choose one mode")
        return EXIT_CONFIG_INVALID
    json_out = cast(bool, args.json)
    mode = CycleMode.DRY_RUN
    if not json_out:
        print(f"policy: {request.policy.label}")
    if apply_requested:
        gate = apply_gate_status(settings)
        if not gate.allowed:
            # Refusal exits here: no state store is opened and no endpoint is built.
            print(_REFUSED_APPLY)
            return EXIT_APPLY_GATE_REFUSAL
        if not json_out:
            print(f"apply gate: {gate.describe()}")
        mode = CycleMode.APPLY
    if not json_out:
        print(f"mode: {_mode_label(mode)}")
    max_submissions = _max_links(args, settings)
    store = open_state_store(settings)
    try:
        deps = build_daemon_deps(
            settings,
            store=store,
            mapping_store=open_mapping_store(settings),
            max_submissions=max_submissions,
            overrides_text=read_overrides_text(settings),
        )
        started = current_iso()
        cycle_key = _begin_budget_cycle(store, "once", started, "once-1")
        _start_cycle_meta(store, cycle_key, started, settings.sync.interval_seconds)
        report = run_cycle(
            deps,
            now_iso=started,
            mode=mode,
            policy=request.policy,
            cycle_key=cycle_key,
        )
        ended = current_iso()
        eligibility = compute_eligibility(deps, started)
        persist_cycle_meta(
            store,
            report,
            started_iso=started,
            ended_iso=ended,
            interval_seconds=settings.sync.interval_seconds,
            full_reconcile_hours=settings.sync.full_reconcile_interval_hours,
        )
    finally:
        store.close()
    payload = cycle_report_payload(
        report, eligibility=eligibility, started_iso=started, ended_iso=ended
    )
    if cast(bool, args.json):
        print(json_payload(payload))
    else:
        _print_cycle_report(report, eligibility, max_submissions)
    return EXIT_RUNTIME if report.failed else EXIT_OK


def _print_cycle_report(
    report: CycleReport, eligibility: EligibilityVerdict, max_submissions: int
) -> None:
    print(cycle_summary_line(report))
    print(
        f"max_submissions (POSTs, not enumerated files): {max_submissions}; "
        f"deferred_planned: {report.deferred_planned}"
    )
    print(f"eligibility: {'ELIGIBLE' if eligibility.eligible else 'REFUSED'}")
    for reason in eligibility.reasons:
        print(f"  eligibility reason: {reason}")
    for reason in report.reasons[:10]:
        print(f"  cycle reason: {reason}")
    for drift in report.drift[:10]:
        print(f"  drift: {drift}")
    for drift in report.existing_link_drift[:10]:
        print(f"  existing-link drift (report only, never unlinked): {drift}")
    if report.mode is CycleMode.APPLY:
        print(_ATTESTATION_NOTE)


def _max_links(args: argparse.Namespace, settings: Settings) -> int:
    raw = getattr(args, "max_links", None)
    return settings.sync.max_new_per_cycle if raw is None else int(raw)


def _mode_label(mode: CycleMode) -> str:
    if mode is CycleMode.DRY_RUN:
        return "dry-run (no remote changes, no writes)"
    return "apply (writes only through the gated daemon path, eligibility-gated)"


def _daemon_preamble(
    settings: Settings, request: PolicyRequest, mode: CycleMode, stop_after: int | None
) -> list[str]:
    """Human-readable startup summary for ``daemon`` (printed only in human mode)."""
    cadence = (
        f"cadence: interval={settings.sync.interval_seconds:g}s, "
        f"full_reconcile={settings.sync.full_reconcile_interval_hours:g}h "
        "(full/light cycle wiring: light cycles poll the same inventories with reduced "
        "work budgets; the interval gate forces a full reconcile)"
    )
    bounded = "unbounded (Ctrl+C / SIGTERM)" if stop_after is None else f"bounded: {stop_after}"
    campaign_drain = (
        "approved backlog campaigns drain automatically (one bounded batch per apply cycle, "
        "only after the operator's canary AND 'campaign authorize-drain' on the settled, "
        "verified canary)"
        if settings.campaign.daemon_auto_apply
        else "campaign drain off (campaign.daemon_auto_apply=false): manual CLI only"
    )
    return [
        f"requested policy: {request.policy.label}",
        f"loop mode: {_mode_label(mode)} (apply requires the gate; validated once at start)",
        cadence,
        (
            f"budgets: max_new_per_cycle={settings.sync.max_new_per_cycle}, "
            f"rolling_new_limit={settings.sync.rolling_new_limit}, "
            f"max_pending_episode_jobs={settings.sync.max_pending_episode_jobs}"
        ),
        f"campaign drain: {campaign_drain}",
        f"cycles: {bounded}",
        _ATTESTATION_NOTE,
    ]


def _run_daemon(args: argparse.Namespace) -> int:
    """The polling daemon: real cycles through ``run_daemon`` until stopped."""
    settings = load_settings(cast(str, args.config))
    request = PolicyRequest(parse_policy(cast(str, args.policy))).validated()
    configure_logging(logging.DEBUG if cast(bool, args.verbose) else logging.INFO)
    gate = apply_gate(settings)
    mode = CycleMode.APPLY if gate else CycleMode.DRY_RUN
    cycles = cast("int | None", args.cycles)
    once_signal = cast(bool, args.once_signal)
    stop_after = 1 if once_signal else cycles
    json_out = cast(bool, args.json)
    if not json_out:
        print_config_summary(settings)
    preamble = _daemon_preamble(settings, request, mode, stop_after)
    state = StopState()
    # F6: clear the inter-cycle interrupt target BEFORE handlers exist, so a signal that
    # arrives during startup is never set-then-cleared (the wake would be lost and the
    # daemon would sleep uninterruptibly).
    _SHUTDOWN_WAKE.clear()
    installed = make_signal_handlers(state)
    preamble.append(f"signal handlers installed: {', '.join(_signal_name(n) for n in installed)}")
    if not json_out:
        for line in preamble:
            print(line)
    store = open_state_store(settings)
    try:
        deps = build_daemon_deps(
            settings,
            store=store,
            mapping_store=open_mapping_store(settings),
            max_submissions=settings.sync.max_new_per_cycle,
            overrides_text=read_overrides_text(settings),
            shutdown_requested=state.should_stop,
        )
        index, last_cycle_key = -1, "cycle-0"

        def now_iso() -> str:
            nonlocal index, last_cycle_key
            index += 1
            iso = current_iso()
            last_cycle_key = _begin_budget_cycle(store, "cycle", iso, _daemon_cycle_key(index))
            _start_cycle_meta(store, last_cycle_key, iso, settings.sync.interval_seconds)
            logging.getLogger(__name__).info("cycle %s starting (%s)", last_cycle_key, iso)
            return iso

        def on_cycle_complete(report: CycleReport) -> None:
            # F4: persist cadence/health metadata after EVERY completed cycle (mid-run),
            # log a concise summary, and let the report be garbage-collected.
            ended = current_iso()
            started = store.get_meta(META_LAST_CYCLE_START_ISO) or ended
            persist_cycle_meta(
                store,
                report,
                started_iso=started,
                ended_iso=ended,
                interval_seconds=settings.sync.interval_seconds,
                full_reconcile_hours=settings.sync.full_reconcile_interval_hours,
            )
            logging.getLogger(__name__).info(
                "cycle %s complete: mode=%s new_arrival_submitted=%d full=%s failed=%s",
                report.cycle_key,
                report.mode.value,
                report.new_arrival_submitted,
                report.full,
                report.failed,
            )
            campaign = getattr(report, "campaign", None)
            if campaign is not None:
                logging.getLogger(__name__).info(
                    "cycle %s campaign batch: %s submitted=%d quarantined=%d remaining=%d%s",
                    report.cycle_key,
                    campaign.campaign_id,
                    campaign.submitted,
                    campaign.quarantined,
                    campaign.remaining,
                    f" stopped={campaign.stopped}" if campaign.stopped else "",
                )

        daemon_report = run_daemon(
            deps,
            cycle_key=lambda _index: last_cycle_key,
            now_iso=now_iso,
            interval_seconds=settings.sync.interval_seconds,
            stop_after_cycles=stop_after,
            should_stop=state.should_stop,
            policy=request.policy,
            mode=mode,
            jitter_ratio=settings.sync.jitter_ratio,
            full_cycle=lambda iso: full_cycle_due(store, iso),
            on_cycle_complete=on_cycle_complete,
            inter_cycle_wait=interruptible_sleep,
        )
    finally:
        store.close()
    last = daemon_report.last_cycle
    exit_line = (
        f"daemon exit: {daemon_report.exit_reason} after {daemon_report.total_cycles} cycle(s) "
        f"({daemon_report.total_failed} failed); last cycle: "
        f"{last.cycle_key if last is not None else '<none>'}"
    )
    if json_out:
        print(
            json_payload(
                daemon_report_payload(
                    daemon_report,
                    cycles=[(report, None, None) for report in daemon_report.cycles],
                )
            )
        )
    else:
        for report in daemon_report.cycles:
            print(cycle_summary_line(report))
        print(exit_line)
    return EXIT_RUNTIME if last is not None and last.failed else EXIT_OK


class _NormalServiceRuntime:
    _INITIAL_ACTION = "Create the dashboard account and complete setup to begin."

    def __init__(
        self,
        settings: BootstrapSettings,
        sonarr: ApiEndpoint,
        shoko: ApiEndpoint,
        state: StopState,
    ) -> None:
        self.settings = settings
        self.sonarr = sonarr
        self.shoko = shoko
        self.state = state
        self.logger = logging.getLogger(__name__)
        self._latest_lock = threading.Lock()
        self.latest: dict[str, Any] = {
            "connection": None,
            "authorized": False,
            "next_action": self._INITIAL_ACTION,
            "diagnostics": (),
            "worker_activity": "",
            "mapping_cache": "Available after scope and safety setup is confirmed above.",
        }
        self.setup = ServiceSetupController(settings, sonarr, shoko, settings.database_path)
        self._tick_cycle_started = False

    def _update_latest(self, **changes: Any) -> None:
        with self._latest_lock:
            self.latest.update(changes)

    def _latest_snapshot(self) -> dict[str, Any]:
        with self._latest_lock:
            return self.latest.copy()

    @staticmethod
    def _operational_status(
        store: StateStore,
        *,
        authorized: bool,
        next_action: str,
        report: CycleReport | None = None,
        mapping_action: str | None = None,
    ) -> tuple[bool, str]:
        """Project write availability separately from consent and cycle success."""
        paused, pause_reason = store.get_service_pause()
        if paused:
            # A current setup/connection decision is more useful than a generic pause.
            # Before the first check, derive guidance from the durable hold instead.
            if (
                authorized
                or next_action == _NormalServiceRuntime._INITIAL_ACTION
                or pause_reason == "user_paused"
            ):
                if pause_reason == "unsafe_shoko_settings":
                    next_action = "Review and fix Shoko safety settings, then confirm setup again."
                elif OnboardingManager(store).reconfirmation_required():
                    next_action = "Confirm the current scope and safety setup again."
                elif pause_reason == "user_paused":
                    next_action = "Automatic linking is paused. Resume when you are ready."
                else:
                    next_action = "Review the current scope, safety setup and connections."
            return False, next_action
        if store.writes_disabled or store.get_meta("requires_rebaseline") == "1":
            return False, (
                "Automatic linking is stopped by a safety check. "
                "Review advanced diagnostics before retrying."
            )
        canary = store.get_service_canary()
        if canary is not None and canary[1] == "failed":
            return False, (
                "Automatic linking is stopped because the initial link could not be verified. "
                "Review advanced diagnostics before retrying."
            )
        if not authorized:
            return False, next_action
        if report is not None and (report.failed or report.writes_suppressed):
            next_action = "A service check needs attention; automatic linking will retry safely."
            if mapping_action is not None and any(
                reason == "mapping_unavailable" or reason.startswith("mapping_source_")
                for reason in report.reasons
            ):
                next_action = mapping_action
            return False, next_action
        if canary is not None and canary[1] == "pending":
            authorized = False
            next_action = (
                "Waiting for the initial link to be verified before processing more files. "
                "The service will check again automatically."
            )
        return _NormalServiceRuntime._queue_wait_status(report, authorized, next_action)

    @staticmethod
    def _queue_wait_status(
        report: CycleReport | None, active: bool, next_action: str
    ) -> tuple[bool, str]:
        """Explain queue refusals even when fully mapped files need no metadata."""
        if not active or report is None:
            return active, next_action
        # Interpret structured refusal codes only, never upstream messages.
        partial = bool(report.counts.submitted or report.counts.verified)
        if "preflight_queue_unknown" in report.reasons:
            return False, (
                "Some files are waiting because " if partial else "Linking is waiting because "
            ) + (
                "Shoko's job queue could not be checked completely. "
                "Check Shoko connectivity and the configured Shoko account or API key's "
                "administrator permissions to view its job queue. Incomplete or oversized "
                "queue results can also prevent this check. The service will retry read-only "
                "checks automatically; new links wait until related jobs can be ruled out."
            )
        if "preflight_queue_blocked" in report.reasons:
            return False, (
                "Some files are waiting for related Shoko jobs to finish. "
                if partial
                else "Waiting for related Shoko jobs to finish before submitting new links. "
            ) + "The service will check again automatically."
        return active, next_action

    @staticmethod
    def _mapping_guidance(status: MappingStatus, now: datetime) -> str:
        """Render only validated status facts, never source URLs or server responses."""
        retry = "The service will check again automatically."
        if status.next_due_iso is not None:
            due = datetime.fromisoformat(status.next_due_iso).astimezone(UTC)
            if due > now:
                retry = f"Next mapping check: {due.strftime('%Y-%m-%d %H:%M:%S UTC')}."
        retained = (
            " Last verified mappings are retained."
            if status.active_present
            else " Linking waits for verified episode mappings."
        )
        if status.invalid_source_hold or status.last_outcome is RefreshOutcome.FAILED_INVALID:
            return (
                "The episode mapping download failed validation; "
                "no unverified mappings will be used."
                + retained
                + " Check the mapping source in Settings. "
                + retry
            )
        if status.last_outcome is RefreshOutcome.FAILED_TRANSPORT:
            return (
                "Could not download episode mappings."
                + retained
                + " Check network access and the mapping source in Settings. "
                + retry
            )
        if status.last_outcome is RefreshOutcome.FAILED_SIZE:
            return (
                "The episode mapping download exceeded the safe size limit."
                + retained
                + " Check the mapping source in Settings. "
                + retry
            )
        if not status.active_present:
            return "No verified episode mappings are cached yet. Linking will wait. " + retry
        return "Verified episode mappings are available. " + retry

    def status(self) -> DashboardStatus:
        latest = self._latest_snapshot()
        connection = latest["connection"]
        message = (
            connection.messages[0]
            if connection is not None and connection.messages
            else "Connected to Sonarr and Shoko."
            if connection is not None and connection.ready
            else "Checking the Sonarr and Shoko connections."
        )
        path = Path(self.settings.database_path)
        if not path.exists():
            return DashboardStatus(
                message,
                0,
                0,
                0,
                False,
                latest["next_action"],
                worker_activity=cast(str, latest["worker_activity"]),
            )
        store = StateStore(path)
        try:
            active, next_action = self._operational_status(
                store,
                authorized=bool(latest["authorized"]),
                next_action=(
                    self._INITIAL_ACTION
                    if latest["worker_activity"] and store.get_service_pause()[0]
                    else cast(str, latest["next_action"])
                ),
            )
            tracked_files = store.service_file_status()
            counts = tracked_files.counts
            preferences = load_service_settings(store, self.settings)
            metadata = (
                metadata_status_view(
                    store,
                    metadata_source_key(self.shoko),
                    enabled=preferences["allow_missing_metadata_refresh"],
                )
                if preferences["shoko_url"] == self.settings.shoko_url
                else MetadataStatusView(
                    message="Restart the container to check the saved Shoko server."
                )
            )
            scan = last_scan_evidence(store, preferences["sonarr_url"], preferences["shoko_url"])
            exclusion_counts = () if scan is None else scan.exclusion_counts
            exclusion_diagnostics = tuple(
                f"Excluded Shoko file {item.shoko_file_id}: {item.reason}: {item.detail}"
                for item in (() if scan is None else scan.examples[:5])
            )
            return DashboardStatus(
                message,
                counts.verified_files,
                counts.waiting_files,
                counts.review_files,
                active,
                next_action,
                diagnostics=(
                    *cast(tuple[str, ...], latest["diagnostics"]),
                    *exclusion_diagnostics,
                ),
                mapping_cache_status=cast(str, latest["mapping_cache"]),
                excluded_files=None if scan is None else scan.summary.excluded_files,
                scope_excluded_files=None if scan is None else scan.summary.outside_scope_files,
                exclusion_counts=exclusion_counts,
                skipped_file_details=tuple(
                    f"{item.detail} (Shoko file ID {item.shoko_file_id})"
                    + (
                        " Check that the recorded path exists and is readable inside Shoko; "
                        "a read-only mount is sufficient."
                        if item.reason == "inaccessible_location"
                        else ""
                    )
                    for item in (() if scan is None else scan.examples)
                ),
                worker_activity=cast(str, latest["worker_activity"]),
                metadata=metadata,
                tracked_files=tracked_files,
                last_scan=None if scan is None else scan.summary,
                action_history=action_history_view(store),
            )
        finally:
            store.close()

    def _checking_activity(self, message: str) -> None:
        # Activity describes read-only work; it never grants write authorization.
        self._update_latest(authorized=False, worker_activity=message, next_action=message)

    def table(self, kind: str, page: int, page_size: int) -> DashboardTable:
        """Read local pagination and confirmed display scope; never refresh remote state."""
        store = StateStore(self.settings.database_path)
        try:
            preferences = load_service_settings(store, self.settings)
            urls = (preferences["sonarr_url"], preferences["shoko_url"])
            identity = scan_context_identity(store, *urls)
            record = OnboardingManager(store).record() if identity is not None else None
            roots = () if record is None else record.binding.roots
            mappings = () if record is None else record.binding.path_mappings
            if kind == "history":
                history = action_history_page(store, page=page, page_size=page_size)
                displays = tuple(
                    file_display(
                        row.current_path,
                        (),
                        roots,
                        mappings,
                        paths_complete=row.current_path_complete,
                    )
                    for row in history.rows
                )
                result = DashboardTable(history=history, displays=displays, counts=history.counts)
            elif kind == "exclusions":
                exclusions = scan_exclusion_page(store, *urls, page=page, page_size=page_size)
                counts = store.service_file_page("review", page=1, page_size=1).counts
                result = DashboardTable(exclusions=exclusions, counts=counts)
            else:
                files = store.service_file_page(kind, page=page, page_size=page_size)
                displays = tuple(tracked_file_display(row, roots, mappings) for row in files.rows)
                result = DashboardTable(files=files, displays=displays, counts=files.counts)
            current_preferences = load_service_settings(store, self.settings)
            if (
                current_preferences["sonarr_url"],
                current_preferences["shoko_url"],
            ) != urls or scan_context_identity(store, *urls) != identity:
                result = replace(result, displays=(), exclusions=None)
            return result
        finally:
            store.close()

    def worker_tick(self) -> None:
        started = time.monotonic()
        self._tick_cycle_started = False
        recovery_limits: tuple[float, int] | None = None
        try:
            # Snapshot limits before connection/setup I/O. A blocked check must not
            # grant recovery a fresh wall-time allowance when it eventually returns.
            with contextlib.closing(StateStore(Path(self.settings.database_path))) as store:
                document = load_service_settings(store, self.settings)
                sync, _, _, _ = parse_runtime_options(
                    {name: document[name] for name in ("sync", "mappings", "safety", "campaign")}
                )
                recovery_limits = (
                    started + sync.max_cycle_seconds,
                    sync.max_recovery_rows_per_cycle,
                )
            self._worker_tick()
        except Exception:
            # Include failures before opening the state store (connection/configuration I/O).
            self._cycle_deferred()
            self.logger.error("Normal service check failed; it will be retried.")
        finally:
            if not self._tick_cycle_started and recovery_limits is not None:
                self._recover_blocked_metadata(*recovery_limits)
            self._update_latest(worker_activity="")

    def _recover_blocked_metadata(self, deadline: float, max_rows: int) -> None:
        """Inspect accepted work even when setup or pause prevents a daemon cycle.

        This path never reconciles files, reserves capacity or sends remote writes.
        Authorized cycles own their existing shared file/metadata recovery budget.
        """
        if time.monotonic() >= deadline or max_rows == 0 or self.state.should_stop():
            return
        try:
            with contextlib.closing(StateStore(Path(self.settings.database_path))) as store:
                document = load_service_settings(store, self.settings)
                # Runtime endpoints are replaced only on restart. Never inspect the
                # old server after a saved Shoko URL change, or follow historical URLs.
                if document["shoko_url"] != self.settings.shoko_url:
                    return
                report = recover_metadata_operations(
                    self.shoko,
                    store,
                    source_key=metadata_source_key(self.shoko),
                    now_iso=current_iso(),
                    deadline=deadline,
                    max_rows=max_rows,
                )
                if report.observations:
                    self._update_latest(
                        diagnostics=tuple(
                            f"metadata_recovery:{item.anime_id}:{item.status.value}"
                            for item in report.observations[:20]
                        )
                    )
        except Exception:
            # Preserve the actual setup/pause guidance and never expose endpoint
            # exceptions, response bodies or credentials through recovery status.
            self.logger.warning("Queued metadata inspection was deferred; it will retry.")

    def _worker_tick(self) -> None:
        self._checking_activity("Checking Sonarr and Shoko connections.")
        if configured_bootstrap(self.settings) != self.settings:
            self._update_latest(
                authorized=False,
                next_action=(
                    "Restart the container to use saved connection URLs, then confirm setup."
                ),
            )
            return
        connection = probe_connections(self.sonarr, self.shoko)
        self._update_latest(connection=connection)
        if not connection.ready:
            self._update_latest(
                authorized=False,
                next_action=(
                    connection.messages[0]
                    if connection.messages
                    else "Check the Sonarr and Shoko connections."
                ),
            )
            return
        store = StateStore(Path(self.settings.database_path))
        try:
            self._checking_activity("Checking the confirmed library, mappings and Shoko safety.")
            context = self.setup.current_context(store)
            if context is None:
                self._update_latest(
                    authorized=False,
                    next_action=(
                        "Complete or review scope and safety setup before automatic linking."
                    ),
                )
                return
            binding = context.binding
            manager = OnboardingManager(store)
            decision = manager.evaluate_context(context, iso=current_iso())
            if decision.status is not OnboardingStatus.AUTHORIZED:
                self._update_latest(authorized=False, next_action=decision.message)
                return
            self._checking_activity("Preparing episode mappings and automatic processing.")
            self._run_authorized_cycle(store, binding, manager)
        except (ConfigError, HttpPolicyError, StateError, OSError, ValueError):
            self._cycle_deferred()
            self.logger.warning("Normal service cycle was deferred and will be retried.")
        except Exception:
            self._cycle_deferred()
            self.logger.error("Normal service cycle failed; it will be retried.")
        finally:
            store.close()

    def _service_authorized(
        self, store: StateStore, manager: OnboardingManager, expected_binding_digest: str
    ) -> bool:
        context = self._service_authorization_context(store)
        return self._service_context_authorized(manager, context, expected_binding_digest)

    def _service_authorization_context(
        self,
        store: StateStore,
        *,
        deadline: float | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> OnboardingContext | None:
        """Refresh remote facts before onboarding compares them with persisted consent."""
        if deadline is None:
            return self.setup.current_context(store)
        return self.setup.current_context(store, deadline=deadline, clock=clock)

    @staticmethod
    def _service_context_authorized(
        manager: OnboardingManager,
        context: OnboardingContext | None,
        expected_binding_digest: str,
    ) -> bool:
        """Evaluate current facts against consent before rejecting a stale cycle digest."""
        if context is None:
            return False
        binding = context.binding
        decision = manager.evaluate_context(context, iso=current_iso())
        return (
            decision.status is OnboardingStatus.AUTHORIZED
            and binding.digest == expected_binding_digest
        )

    def _run_authorized_cycle(
        self, store: StateStore, binding: ScopeBinding, manager: OnboardingManager
    ) -> None:
        effective = settings_for_scope(
            self.settings, roots=binding.roots, mappings=binding.path_mappings
        )
        effective = apply_service_settings(effective, load_service_settings(store, self.settings))
        store.budget_limits = budget_limits_from_settings(effective)
        mapping_store = open_mapping_store(effective)
        deps = build_daemon_deps(
            effective,
            store=store,
            mapping_store=mapping_store,
            max_submissions=effective.sync.max_new_per_cycle,
            overrides_text=read_overrides_text(effective),
            shutdown_requested=self.state.should_stop,
        )
        endpoints = (deps.sonarr, deps.shoko, deps.mapping_source)
        expected_binding_digest = binding.digest

        authorization_context: list[OnboardingContext | None] = [None]

        def service_authorized(deadline: float) -> bool:
            context = self._service_authorization_context(
                store, deadline=deadline, clock=deps.monotonic
            )
            authorization_context[0] = context
            return self._service_context_authorized(manager, context, expected_binding_digest)

        def service_authorized_local() -> bool:
            document = load_service_settings(store, self.settings)
            if (
                document["sonarr_url"] != self.settings.sonarr_url
                or document["shoko_url"] != self.settings.shoko_url
            ):
                return False
            context = authorization_context[0]
            if context is None:
                return False
            # The remote callback already evaluated this context. If an operator confirms a
            # different scope while the final Shoko read is in flight, the cached facts are
            # stale; refuse this old cycle without misclassifying the new consent as drift.
            record = manager.record()
            if record is None or context.binding.digest != record.binding_digest:
                return False
            return self._service_context_authorized(manager, context, expected_binding_digest)

        deps = replace(
            deps,
            service_mode=True,
            service_authorized_for_deadline=service_authorized,
            service_authorized_local=service_authorized_local,
        )
        try:
            self._tick_cycle_started = True
            self._run_daemon_cycle(deps, store, effective)
        finally:
            for endpoint in endpoints:
                endpoint.client.close()

    def _run_daemon_cycle(self, deps: DaemonDeps, store: StateStore, settings: Settings) -> None:
        cycle_key_value = "service-unstarted"
        cycle_completed = False

        def begin_cycle() -> str:
            nonlocal cycle_key_value
            started = current_iso()
            cycle_key_value = _begin_budget_cycle(store, "service", started, f"service-{started}")
            _start_cycle_meta(store, cycle_key_value, started, settings.sync.interval_seconds)
            return started

        def completed(report: CycleReport) -> None:
            nonlocal cycle_completed
            ended = current_iso()
            started = store.get_meta(META_LAST_CYCLE_START_ISO) or ended
            persist_cycle_meta(
                store,
                report,
                started_iso=started,
                ended_iso=ended,
                interval_seconds=settings.sync.interval_seconds,
                full_reconcile_hours=settings.sync.full_reconcile_interval_hours,
            )
            mapping_view = mapping_status(
                deps.mapping_store,
                datetime.fromisoformat(ended),
                overrides_text=deps.overrides_text,
            )
            mapping_guidance = self._mapping_guidance(mapping_view, datetime.fromisoformat(ended))
            mapping_outcome = (
                mapping_view.last_outcome.value if mapping_view.last_outcome else "none"
            )
            active, next_action = self._operational_status(
                store,
                authorized=True,
                next_action="Automatic linking is ready; waiting for the next scheduled check.",
                report=report,
                mapping_action=mapping_guidance,
            )
            self._update_latest(
                authorized=active,
                worker_activity="",
                diagnostics=(
                    *report.reasons[:20],
                    f"mapping_refresh_outcome:{mapping_outcome}",
                    f"mapping_next_check:{mapping_view.next_due_iso or 'due'}",
                ),
                mapping_cache=mapping_guidance,
                next_action=next_action,
            )
            # The daemon swallows callback failures; only a published status is complete.
            cycle_completed = True

        active, next_action = self._operational_status(
            store,
            authorized=True,
            next_action="Automatic linking is running for the confirmed scope.",
        )
        self._update_latest(
            authorized=active,
            next_action=next_action,
            worker_activity="Checking queued files and verifying links.",
        )
        try:
            run_daemon(
                deps,
                cycle_key=lambda _index: cycle_key_value,
                now_iso=begin_cycle,
                interval_seconds=settings.sync.interval_seconds,
                stop_after_cycles=1,
                should_stop=self.state.should_stop,
                policy=RunPolicy.SERVICE,
                mode=CycleMode.APPLY,
                jitter_ratio=0.0,
                full_cycle=lambda iso: full_cycle_due(store, iso),
                on_cycle_complete=completed,
            )
        finally:
            if not cycle_completed:
                self._cycle_deferred()
            self._update_latest(worker_activity="")

    def _cycle_deferred(self) -> None:
        self._update_latest(
            authorized=False,
            next_action="A service check needs attention; it will retry safely.",
        )


def _run_service(_args: argparse.Namespace) -> int:
    """Run the authenticated dashboard with the guarded worker on its own thread."""
    settings = configured_bootstrap(load_bootstrap_settings())
    configure_logging(logging.DEBUG if cast(bool, _args.verbose) else logging.INFO)
    state = StopState()
    _SHUTDOWN_WAKE.clear()
    installed = make_signal_handlers(state)
    logger = logging.getLogger(__name__)
    if installed:
        logger.debug("signal handlers installed: %s", ", ".join(_signal_name(n) for n in installed))
    sonarr, shoko = bootstrap_endpoints(settings)
    try:
        dashboard_port = int(os.environ.get("DASHBOARD_PORT", "8080"))
        if not 1 <= dashboard_port <= DASHBOARD_PORT_MAX:
            raise ValueError
    except ValueError:
        sonarr.client.close()
        shoko.client.close()
        raise ConfigError("DASHBOARD_PORT must be an integer between 1 and 65535") from None

    runtime = _NormalServiceRuntime(settings, sonarr, shoko, state)
    server = None
    dashboard_endpoints: tuple[ApiEndpoint, ApiEndpoint] | None = None
    try:
        dashboard_endpoints = bootstrap_endpoints(settings)
        dashboard_setup = ServiceSetupController(
            settings, *dashboard_endpoints, settings.database_path
        )

        def dashboard_onboarding_context() -> OnboardingContext | None:
            path = Path(settings.database_path)
            if not path.exists():
                return None
            dashboard_store = StateStore(path)
            try:
                return dashboard_setup.current_context(dashboard_store)
            finally:
                dashboard_store.close()

        worker_wake = threading.Event()
        preferences = ServiceSettingsController(settings, worker_wake.set)
        application = DashboardApplication(
            settings.database_path,
            status_provider=runtime.status,
            table_provider=runtime.table,
            onboarding_context=dashboard_onboarding_context,
            setup_provider=dashboard_setup.view,
            setup_action=dashboard_setup.action,
            settings_provider=preferences.view,
            settings_action=preferences.save,
            control_changed=worker_wake.set,
        )
        server = create_dashboard_server(DASHBOARD_BIND_HOST, dashboard_port, application)
        logger.info("service started; no dashboard host port is published by default")
        run_dashboard_worker(
            server,
            worker_tick=runtime.worker_tick,
            stop_requested=state.should_stop,
            monotonic=time.monotonic,
            worker_interval_seconds=SERVICE_POLL_SECONDS,
            worker_wake=worker_wake,
            interval_provider=preferences.interval,
        )
    except OSError:
        logger.error("Dashboard listener could not start; check its port configuration.")
        return EXIT_RUNTIME
    finally:
        if server is not None:
            server.server_close()
        if dashboard_endpoints is not None:
            dashboard_endpoints[0].client.close()
            dashboard_endpoints[1].client.close()
        sonarr.client.close()
        shoko.client.close()
    return EXIT_OK


def _run_status(_args: argparse.Namespace) -> int:
    """Show normal-service state without network access or requiring connection secrets."""
    path = Path(DEFAULT_DATABASE_PATH)
    if not path.exists():
        print("Automatic linking is waiting for first-use setup in the dashboard.")
        return EXIT_OK
    store = StateStore(path)
    try:
        account = store.get_dashboard_account()
        manager = OnboardingManager(store)
        record = manager.record()
        paused, _reason = store.get_service_pause()
        if paused:
            linking = "paused"
        elif record is None:
            linking = "waiting for setup confirmation"
        else:
            linking = "setup confirmed; current worker state is available in the dashboard"
        print(f"Dashboard account: {'created' if account is not None else 'not created'}")
        print(f"Automatic linking: {linking}")
        counts = store.service_file_counts()
        print(f"Verified files: {counts.verified_files}")
        print(f"Waiting files: {counts.waiting_files}")
        print("  Work is being prepared or waiting for Shoko to confirm a link.")
        print(f"Files needing review: {counts.review_files}")
        print(
            "  Partial or uncertain link results need attention; conflicts and safety issues "
            "are included."
        )
        return EXIT_OK
    except (StateError, OSError, ValueError) as exc:
        print(f"state unavailable: {exc}", file=sys.stderr)
        return EXIT_STATE
    finally:
        store.close()


def _run_pause(_args: argparse.Namespace) -> int:
    """Persist an ordinary local pause using the same onboarding state as the dashboard."""
    store = StateStore(Path(DEFAULT_DATABASE_PATH))
    try:
        OnboardingManager(store).pause(iso=current_iso())
    except (StateError, OSError, ValueError) as exc:
        print(f"could not pause automatic linking: {exc}", file=sys.stderr)
        return EXIT_STATE
    finally:
        store.close()
    print("Automatic linking is paused.")
    return EXIT_OK


def _run_resume(_args: argparse.Namespace) -> int:
    """Resume only after fresh endpoint, scope, compatibility, and Shoko-safety checks."""
    path = Path(DEFAULT_DATABASE_PATH)
    if not path.exists():
        print("Complete dashboard setup before automatic linking can resume.")
        return EXIT_STATE
    store = StateStore(path)
    try:
        try:
            manager = OnboardingManager(store)
            record = manager.record()
        except (StateError, OSError, ValueError) as exc:
            print(f"could not read setup state: {exc}", file=sys.stderr)
            return EXIT_STATE
        if record is None:
            print("Complete dashboard setup before automatic linking can resume.")
            return EXIT_STATE
        try:
            bootstrap = configured_bootstrap(load_bootstrap_settings())
            sonarr, shoko = bootstrap_endpoints(bootstrap)
            try:
                controller = ServiceSetupController(bootstrap, sonarr, shoko, path)
                context = controller.current_context(store)
                if context is None:
                    print("Current services or anime scope need review before linking can resume.")
                    return EXIT_STATE
                decision = manager.resume_context(context, iso=current_iso())
            finally:
                sonarr.client.close()
                shoko.client.close()
        except (ConfigError, HttpPolicyError, StateError, OSError, ValueError) as exc:
            print(f"Current setup could not be verified; automatic linking remains paused: {exc}")
            return EXIT_STATE
        print(decision.message)
        return EXIT_OK if decision.status is OnboardingStatus.AUTHORIZED else EXIT_STATE
    finally:
        store.close()


def _daemon_cycle_key(index: int) -> str:
    return f"cycle-{index + 1}"


def _signal_name(number: int) -> str:
    for name in ("SIGINT", "SIGTERM"):
        if getattr(signal, name, None) is not None and int(getattr(signal, name)) == number:
            return name
    return str(number)


def _run_report(args: argparse.Namespace) -> int:
    """Read-only rollup from the state DB and mapping store. Makes no network request."""
    settings = load_settings(cast(str, args.config))
    # Offline reporting never constructs clients (which normally register these).
    # Legacy diagnostics still need redaction against the configured runtime keys.
    for service in (settings.sonarr, settings.shoko):
        register_secret(service.api_key.resolve())
    now = datetime.fromisoformat(current_iso())
    mapping_store = open_mapping_store(settings)
    status = mapping_status(mapping_store, now, overrides_text=read_overrides_text(settings))
    db_path = state_path(settings)
    store = open_state_store(settings) if db_path.exists() else None
    attestation = attestation_verdict_for(settings).payload()
    try:
        payload = report_payload(
            store,
            status,
            settings,
            now=now,
            refresh_hours=settings.mappings.refresh_hours,
            attestation=attestation,
        )
    finally:
        if store is not None:
            store.close()
    if cast(bool, args.json):
        print(json_payload(payload))
        return EXIT_OK
    _print_report(payload)
    return EXIT_OK


def _print_report(payload: Mapping[str, Any]) -> None:
    state = payload["state"]
    mapping = payload["mapping"]
    generation = f"active rev {mapping['revision']}" if mapping["generation_active"] else "ABSENT"
    state_line = (
        f"state: present at {state['path']}" if state["present"] else "state: absent (no state DB)"
    )
    print(state_line)
    print(f"mapping: source={mapping['source_url']}")
    print(
        f"  generation: {generation}, next_due={mapping['next_due_iso']}, "
        f"last_outcome={mapping['last_outcome']}, "
        f"invalid_source_hold={mapping['invalid_source_hold']}"
    )
    if state["present"]:
        print(
            f"queued: {payload['counts_queued']}  verified: {payload['counts_verified']}  "
            f"indeterminate: {payload['counts_indeterminate']}  "
            f"conflicts: {payload['mapping_conflicts']}"
        )
        print(f"cohorts: {payload['cohorts']}")
        coverage = payload["coverage"]
        print(
            f"coverage: {coverage['coverage_percent']}% "
            f"({coverage['files_verified']}/{coverage['files_tracked_in_state']}) — "
            f"{coverage['note']}"
        )
        safety = payload["safety"]
        print(f"safety: {safety['attestation_note']}")
        print(
            f"  paused_reason={safety['paused_reason']}, "
            f"writes_disabled={safety['writes_disabled']}, "
            f"requires_rebaseline={safety['requires_rebaseline']}"
        )
        print(f"  last_refusal={safety['last_refusal']}")
        campaigns = payload.get("campaigns") or []
        if campaigns:
            for item in campaigns:
                progress = item["progress"]
                print(
                    f"campaign {item['campaign_id']}: {item['status']}"
                    f"{f' ({item['status_reason']})' if item['status_reason'] else ''} "
                    f"sent={item['total_sent']}/{item['max_submissions']} "
                    f"verified={progress.get('verified', 0)} "
                    f"quarantined={progress.get('quarantined', 0)} "
                    f"drain={item['drain_selection']}"
                )
        jobs = payload["outstanding_jobs"]
        print(
            f"outstanding jobs: {jobs['live_held_sends']} sends / {jobs['live_held_slots']} slots "
            f"held; pending {jobs['pending_file_requests']} files / "
            f"{jobs['pending_episode_jobs']} jobs"
        )
        levels = payload["levels"]
        print(
            f"levels: alive={levels['alive']} "
            f"reconciliation_ready={levels['reconciliation_ready']} "
            f"new_arrival_apply_eligible={levels['apply_eligible']}"
        )
        activity = levels["write_activity"]
        print(
            "last-cycle writes: "
            f"new_arrival(eligible={activity['new_arrival_apply_eligible']}, "
            f"submitted={activity['new_arrival_submitted']}, "
            f"suppressed={activity['new_arrival_writes_suppressed']}) "
            f"campaign(selected={activity['campaign_drain_selected']}, "
            f"eligible={activity['campaign_apply_eligible']}, "
            f"submitted={activity['campaign_submitted']}, "
            f"suppressed={activity['campaign_writes_suppressed']}) "
            f"total_link_requests_attempted={activity['total_link_requests_attempted']} "
            f"all_writes_suppressed={activity['all_writes_suppressed']}"
        )
        for item in payload["detail_rollup"][:10]:
            print(f"  detail x{item['count']}: {item['detail']}")


def _run_health(args: argparse.Namespace) -> int:
    """Cheap liveness probe from state/meta and the mapping store only (no network)."""
    settings = load_settings(cast(str, args.config))
    now = datetime.fromisoformat(current_iso())
    mapping_store = open_mapping_store(settings)
    status = mapping_status(mapping_store, now, overrides_text=read_overrides_text(settings))
    db_path = state_path(settings)
    store = open_state_store(settings) if db_path.exists() else None
    try:
        payload = health_payload(store, status)
    finally:
        if store is not None:
            store.close()
    print(json_payload(payload))
    return EXIT_OK


def _run_mappings(args: argparse.Namespace) -> int:
    settings = load_settings(cast(str, args.config))
    command = cast(str, args.mappings_command)
    if command == "status":
        return _run_mappings_status(settings, json_out=cast(bool, args.json))
    return _run_mappings_refresh(
        settings, force=cast(bool, args.force), json_out=cast(bool, args.json)
    )


def _run_mappings_status(settings: Settings, *, json_out: bool) -> int:
    """Offline generation status: digests/age/schedule plus state-DB candidate counts."""
    now = datetime.fromisoformat(current_iso())
    store = open_mapping_store(settings)
    status = mapping_status(store, now, overrides_text=read_overrides_text(settings))
    affected: dict[str, int] = {}
    db_path = state_path(settings)
    state_store: StateStore | None = None
    if db_path.exists():
        state_store = open_state_store(settings)
    try:
        affected = _mapping_affected_counts(state_store)
        payload = mapping_status_payload(
            status,
            now=now,
            refresh_hours=settings.mappings.refresh_hours,
            affected=affected,
        )
    finally:
        if state_store is not None:
            state_store.close()
    if json_out:
        print(json_payload(payload))
        return EXIT_OK
    print(f"mapping source: {payload['source_url']}")
    generation = (
        "active" if payload["generation_active"] else "ABSENT (last-known-good not present)"
    )
    print(f"generation: {generation}, revision {payload['revision']}")
    print(f"  effective digests: targets={payload['targets_digest']}")
    print(f"  structure digest: {payload['structure_digest']}")
    print(f"  content sha256: {payload['content_sha256']}")
    age = payload["active_age_hours"]
    print(f"  activated: {payload['activated_iso']} (age {age if age is not None else 'n/a'} h)")
    print(
        f"  last attempt: {payload['last_attempted_iso']} outcome={payload['last_outcome']}; "
        f"last success: {payload['last_success_iso']}"
    )
    due_now = "yes" if payload["next_due_now"] else "no"
    print(f"  next due refresh: {payload['next_due_iso']} (due now: {due_now})")
    print(f"  consecutive failures: {payload['consecutive_failures']}")
    print(
        f"  indexed: entries={payload['entries_indexed']}, rules={payload['mapping_rules']}, "
        f"explicit_pairs={payload['explicit_pairs']}"
    )
    print(f"  mapping-affected candidates (state DB): {affected or 'state DB absent'}")
    return EXIT_OK


def _run_mappings_refresh(settings: Settings, *, force: bool, json_out: bool) -> int:
    """One due-gated Anime-Lists refresh. The only network call is the mapping source."""
    now = datetime.fromisoformat(current_iso())
    store = open_mapping_store(settings)
    endpoint = source_endpoint(store, settings)
    try:
        outcome = mapping_refresh(
            store,
            endpoint,
            now=now,
            refresh_hours=settings.mappings.refresh_hours,
            force=force,
            overrides_text=read_overrides_text(settings),
        )
    finally:
        endpoint.client.close()
    status = mapping_status(store, now, overrides_text=read_overrides_text(settings))
    payload = refresh_payload(
        outcome,
        status,
        forced=force,
        refresh_hours=settings.mappings.refresh_hours,
        now=now,
    )
    if json_out:
        print(json_payload(payload))
    else:
        generation = payload["generation"]
        print(
            f"refresh outcome: {outcome.value}"
            f"{'' if force else ' (due-gated)'}; success: {payload['success']}"
        )
        print(
            f"generation: {'active' if generation['generation_active'] else 'ABSENT'}, "
            f"revision {generation['revision']}, next_due={generation['next_due_iso']}"
        )
        if outcome not in REFRESH_SUCCESS_OUTCOMES:
            print(
                "last-known-good generation stays active: a failed fetch never moves "
                "the cache; the schedule advanced with exponential backoff"
            )
    return EXIT_OK if outcome in REFRESH_SUCCESS_OUTCOMES else EXIT_RUNTIME


def _mapping_affected_counts(store: StateStore | None) -> dict[str, int]:
    """Cheap indexed counts of mapping-re-evaluation candidates in the state DB."""
    if store is None:
        return {}
    affected = {"waiting": 0, "ambiguous": 0, "indeterminate_write": 0}
    rows = store.connection.execute(
        "SELECT state, COUNT(*) FROM files"
        " WHERE state IN ('waiting', 'ambiguous', 'indeterminate_write') GROUP BY state",
        (),
    ).fetchall()
    for row in rows:
        affected[str(row[0])] = int(row[1])
    return affected


# ------------------------------------------------------------------ operator commands


type OperatorAction = Callable[[Settings, StateStore, str], Mapping[str, Any]]


def _operator_action(
    args: argparse.Namespace,
    command: str,
    action: OperatorAction,
    *,
    allow_stopped_writes: bool = False,
) -> int:
    """Run one confirmed operator mutation against the durable state store."""
    if not cast(bool, args.confirm):
        print(f"{command}: {_CONFIRM_REQUIRED}")
        return EXIT_CONFIG_INVALID
    settings = load_settings(cast(str, args.config))
    note = cast(str, args.note)
    iso = current_iso()
    store = open_state_store(settings)
    try:
        if store.writes_disabled and not allow_stopped_writes:
            reason = store.get_meta(META_WRITES_DISABLED_REASON) or "unspecified"
            print(
                f"{command}: refused — state writes are stopped ({reason}); fix the storage "
                "problem, then run resume-writes --confirm"
            )
            return EXIT_STATE
        result = action(settings, store, iso)
        _record_operator_action(store, command, note, iso, result)
    finally:
        store.close()
    payload = operator_action_payload(command, note, iso=iso, result=result)
    if cast(bool, args.json):
        print(json_payload(payload))
    else:
        print(f"{command}: ok (note: {note})")
        for key, value in sorted(result.items()):
            print(f"  {key}: {value}")
    return EXIT_OK


def _run_resolve_send(args: argparse.Namespace) -> int:
    resolution: Literal["released_after_review", "operator_closed"] = (
        "released_after_review" if cast(bool, args.release) else "operator_closed"
    )

    def action(settings: Settings, store: StateStore, iso: str) -> Mapping[str, Any]:
        file_id = int(args.file_id)
        send_id = store.resolve_send(
            file_id,
            cast(str, args.fingerprint),
            resolution=resolution,
            iso=iso,
            operator_note=cast(str, args.note),
            verification_timeout_seconds=settings.sync.verification_timeout_seconds,
        )
        record = store.get_file(file_id)
        return {
            "send_id": send_id,
            "file_id": file_id,
            "resolution": resolution,
            "state_after": record.state.value if record is not None else None,
            "replan_note": _RESOLVE_REPLAN_NOTE,
        }

    return _operator_action(args, "resolve-send", action)


def _run_clear_requires_rebaseline(args: argparse.Namespace) -> int:
    def action(_settings: Settings, store: StateStore, iso: str) -> Mapping[str, Any]:
        store.clear_requires_rebaseline(iso)
        return {"requires_rebaseline": "cleared", "iso": iso}

    return _operator_action(args, "clear-requires-rebaseline", action)


def _run_steal_apply_lock(args: argparse.Namespace) -> int:
    def action(_settings: Settings, store: StateStore, iso: str) -> Mapping[str, Any]:
        # F5: the OS advisory lock is the fence. A live owner (kernel-held lock) refuses
        # (LockHeldError naming the holder meta, exit 5); only a DEAD owner (expired lease
        # with a FREE OS lock) is taken over — audit-recorded, with an immediately-expired
        # marker lease so the next daemon/campaign acquire takes ownership without a stall.
        acquisition = steal_apply_lock(store, os.getpid(), iso, lease_expires_iso=iso)
        return {
            "stolen": True,
            "holder_pid": acquisition.holder.owner_pid,
            "lease_expires_iso": acquisition.holder.lease_expires_iso,
        }

    return _operator_action(args, "steal-apply-lock", action)


def _run_resume_writes(args: argparse.Namespace) -> int:
    def action(_settings: Settings, store: StateStore, iso: str) -> Mapping[str, Any]:
        # Resume is the sanctioned recovery path: it clears the
        # stopped-writes marker AND the write pause so writes can flow again.
        store.clear_writes_disabled(iso)
        store.resume(iso)
        return {"writes_disabled": False, "paused_reason": store.budget_view().paused_reason}

    return _operator_action(args, "resume-writes", action, allow_stopped_writes=True)


def _run_pause_writes(args: argparse.Namespace) -> int:
    if cast(str, args.reason).strip() == ATTESTATION_PAUSE_REASON:
        print(
            f"refused: --reason {ATTESTATION_PAUSE_REASON!r} is reserved for the safety-"
            "attestation gate's own write pause; operators must pick a distinct reason "
            f"(e.g. 'disk_review', 'maintenance', 'operator_paused')"
        )
        return EXIT_CONFIG_INVALID

    def action(_settings: Settings, store: StateStore, iso: str) -> Mapping[str, Any]:
        reason = cast(str, args.reason)
        hours = getattr(args, "hours", None)
        try:
            until = None if hours is None else _iso_plus(iso, float(hours) * 3600.0)
        except (ValueError, OverflowError) as exc:
            raise ConfigError(
                "--hours exceeds the supported resume horizon timestamp range"
            ) from exc
        store.pause(reason, iso)
        if until is not None:
            with contextlib.suppress(WriteDisabledError, StateError):
                store.set_meta("paused_until_iso", until)
        return {"paused_reason": reason, "paused_until_iso": until}

    return _operator_action(args, "pause-writes", action)


# ------------------------------------------------------------------ attestation commands


def _run_attestation(args: argparse.Namespace) -> int:
    command = cast(str, args.attestation_command)
    if command == "status":
        return _run_attestation_status(args)
    if command == "record":
        return _run_attestation_record(args)
    return _run_attestation_invalidate(args)


def _run_attestation_status(args: argparse.Namespace) -> int:
    """Verdict + record fields (the lease carries no secret values by construction)."""
    settings = load_settings(cast(str, args.config))
    scope = validate_scope(cast("str | None", args.scope) or SCOPE_DAEMON)
    verdict = attestation_verdict_for(settings, scope=scope)
    payload = verdict.payload()
    if cast(bool, args.json):
        print(json_payload(payload))
        return EXIT_OK
    print(f"attestation: {verdict.status} ({verdict.reason})")
    print(f"  scope checked: {scope}")
    resolved = attestation_target_path(settings, scope)
    lease = str(resolved) if resolved is not None else "<safety.attestation_file not configured>"
    print(f"  lease file: {lease}")
    if verdict.digest is not None:
        print(f"  record digest: {verdict.digest}")
    if verdict.expires_in_seconds is not None:
        print(f"  expires in: {verdict.expires_in_seconds:.0f}s")
    record = verdict.record
    if record is not None:
        print(f"  endpoint: {record.endpoint}")
        print(f"  version: {record.version}")
        print(f"  scope: {record.scope}")
        print(f"  inspected: {record.inspected_iso}  expires: {record.expires_iso}")
        print(f"  mounts: {', '.join(record.mount_fingerprint)}")
        flags = ", ".join(
            f"{name}={str(value).lower()}"
            for name, value in sorted(record.settings_summary.items())
        )
        print(f"  settings flags: {flags}")
        print(f"  operator note: {record.operator_note}")
    return EXIT_OK


def _parse_setting_flags(raw: str) -> dict[str, bool]:
    flags: dict[str, bool] = {}
    for item in raw.split(","):
        entry = item.strip()
        if not entry:
            continue
        name, sep, value = entry.partition("=")
        if not sep:
            raise ConfigError(f"settings flag {entry!r} must be name=true/false")
        key = name.strip()
        lowered = value.strip().lower()
        if lowered not in {"true", "false"}:
            raise ConfigError(f"settings flag {entry!r}: value must be true or false")
        flags[key] = lowered == "true"
    return flags


def _run_attestation_record(args: argparse.Namespace) -> int:
    """Operator builds the lease file from flags (the trust anchor is the operator)."""
    if not cast(bool, args.confirm):
        print(f"attestation record: {_CONFIRM_REQUIRED}")
        return EXIT_CONFIG_INVALID
    settings = load_settings(cast(str, args.config))
    scope = validate_scope(cast("str | None", args.scope) or SCOPE_DAEMON)
    target = attestation_target_path(settings, scope, for_write=True)
    if target is None:
        print(
            "refused: safety.attestation_file is not configured — point the config at the "
            "lease path first"
        )
        return EXIT_CONFIG_INVALID
    hours = cast(
        float, args.hours if args.hours is not None else settings.safety.attestation_validity_hours
    )
    ok, cap_reason = lease_expiry_cap(hours, settings.safety.attestation_validity_hours)
    if not ok:
        print(f"refused: {cap_reason}")
        return EXIT_CONFIG_INVALID
    try:
        path, digest, warning, record = _record_lease(args, target, hours, scope)
    except (AttestationError, ConfigError) as exc:
        print(f"request invalid: {exc}")
        return EXIT_CONFIG_INVALID
    payload = {
        "command": "attestation-record",
        "path": str(path),
        "digest": digest,
        "expires_iso": record.expires_iso,
        "scope": record.scope,
        "permission_warning": warning,
    }
    if cast(bool, args.json):
        print(json_payload(payload))
        return EXIT_OK
    print(f"attestation recorded: {path}")
    print(f"  digest: {digest}")
    print(f"  scope: {record.scope}  expires: {record.expires_iso}")
    print(
        f"  NOTE: 0600 is attempted, not enforceable on every platform"
        f"{'' if warning is None else f' ({warning})'} — keep the lease readable only "
        "by the operator"
    )
    return EXIT_OK


def _record_lease(
    args: argparse.Namespace, target: Path, hours: float, scope: str
) -> tuple[Path, str, str | None, AttestationRecord]:
    """Validate the operator's inspected facts and write the lease atomically."""
    flags = _parse_setting_flags(cast(str, args.settings))
    mounts = [item.strip() for item in cast(str, args.mounts).split(",") if item.strip()]
    inspected = current_iso()
    try:
        expires = _iso_plus(inspected, hours * 3600.0)
    except OverflowError as exc:
        raise AttestationError("hours exceeds the supported expiry timestamp range") from exc
    record = build_record(
        endpoint=cast(str, args.endpoint),
        version=cast(str, args.version),
        mounts=mounts,
        settings_flags=flags,
        inspected_iso=inspected,
        expires_iso=expires,
        scope=scope,
        operator_note=cast(str, args.note),
    )
    path, digest, warning = write_attestation(target, record)
    return path, digest, warning, record


def _invalidate_target(settings: Settings, scope: str) -> tuple[Path | None, str | None]:
    """Resolve the file a destructive invalidate may move, without the read fallback.

    A campaign-scope invalidate must NEVER reach the daemon lease: it targets the
    campaign sibling. Only when the sibling is absent does it consider the configured
    path, and then only if that file parses AND records exactly this scope (the legacy
    backward-compat lease the operator wrote directly there). Otherwise it refuses (exit
    nonzero) leaving the daemon lease untouched.
    """
    if settings.safety.attestation_file is None:
        return None, "safety.attestation_file is not configured"
    base = attestation_target_path(settings, SCOPE_DAEMON, for_write=True)
    assert base is not None
    validated = validate_scope(scope)
    if validated == SCOPE_DAEMON:
        return base, None
    campaign_id = validated.removeprefix("campaign:")
    sibling = campaign_lease_path(base, campaign_id)
    if sibling.exists():
        return sibling, None
    if base.exists():
        record: AttestationRecord | None
        try:
            record = parse_attestation_document(base.read_text(encoding="utf-8"))
        except (AttestationError, UnicodeDecodeError, OSError):
            record = None
        if record is not None and record.scope == validated:
            return base, None
    return None, (
        f"no lease to invalidate for scope {validated!r}: the campaign lease {sibling} does not "
        f"exist and {base} does not attest this scope (the daemon lease is never touched)"
    )


def _run_attestation_invalidate(args: argparse.Namespace) -> int:
    """Move the lease aside (``.revoked`` suffix) and audit the action. Apply stays closed."""
    if not cast(bool, args.confirm):
        print(f"attestation invalidate: {_CONFIRM_REQUIRED}")
        return EXIT_CONFIG_INVALID
    settings = load_settings(cast(str, args.config))
    scope = validate_scope(cast("str | None", args.scope) or SCOPE_DAEMON)
    source, refuse = _invalidate_target(settings, scope)
    if refuse is not None:
        print(f"refused: {refuse}")
        return EXIT_CONFIG_INVALID
    assert source is not None
    if not source.exists():
        print(f"refused: attestation file {source} does not exist")
        return EXIT_CONFIG_INVALID
    target = source.with_name(source.name + ".revoked")
    if target.exists():
        print(f"refused: {target} already exists (the audit trail is never overwritten)")
        return EXIT_CONFIG_INVALID
    try:
        source.replace(target)
    except OSError as exc:
        print(f"refused: cannot move the lease: {exc.strerror or exc}")
        return EXIT_CONFIG_INVALID
    iso = current_iso()
    note = cast(str, args.note)
    db_path = state_path(settings)
    store = open_state_store(settings) if db_path.exists() else None
    result: dict[str, Any] = {"revoked_path": str(target), "scope": scope, "note": note}
    if store is not None:
        try:
            _record_operator_action(store, "attestation-invalidate", note, iso, result)
        finally:
            store.close()
    payload = operator_action_payload("attestation-invalidate", note, iso=iso, result=result)
    if cast(bool, args.json):
        print(json_payload(payload))
    else:
        print(f"attestation invalidated: lease moved to {target}")
        print("  applies are now refused (attestation absent) until a new lease is recorded")
    return EXIT_OK


# ------------------------------------------------------------------ campaign commands


def _campaign_deps(settings: Settings, store: StateStore, *, scope: str) -> DaemonDeps:
    return build_daemon_deps(
        settings,
        store=store,
        mapping_store=open_mapping_store(settings),
        max_submissions=settings.sync.max_new_per_cycle,
        overrides_text=read_overrides_text(settings),
        attestation_scope=scope,
    )


def _run_campaign(args: argparse.Namespace) -> int:
    command = cast(str, args.campaign_command)
    handler = _CAMPAIGN_HANDLERS[command]
    try:
        return handler(args)
    except CampaignPlanInvalidError as exc:
        # M7 review P3-3: a malformed plan file is invalid input on every campaign
        # command, including apply (campaign-state refusals below stay exit 3).
        print(f"request invalid: {exc}", file=sys.stderr)
        return EXIT_CONFIG_INVALID
    except CampaignDigestMismatchError as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return EXIT_CONFIG_INVALID
    except CampaignError as exc:
        if command == "apply":
            # Campaign apply refusals (not-approved/revoked/paused/deadline/max-submissions/
            # plan drift/attestation/eligibility/gate) are live-write refusals → exit 3.
            print(f"campaign apply refused: {exc}", file=sys.stderr)
            return EXIT_APPLY_GATE_REFUSAL
        print(f"refused: {exc}", file=sys.stderr)
        return EXIT_CONFIG_INVALID


def _run_campaign_prepare(args: argparse.Namespace) -> int:
    settings = load_settings(cast(str, args.config))
    raw_limit = getattr(args, "limit", None)
    limit = settings.campaign.max_campaign_files if raw_limit is None else int(raw_limit)
    if limit > settings.campaign.max_campaign_files:
        print(
            f"request invalid: --limit {limit} exceeds campaign.max_campaign_files "
            f"({settings.campaign.max_campaign_files})"
        )
        return EXIT_CONFIG_INVALID
    store = open_state_store(settings)
    try:
        deps = _campaign_deps(settings, store, scope=SCOPE_DAEMON)
        plan, digest = prepare_campaign(
            deps,
            settings,
            store,
            current_iso(),
            limit=limit,
            deadline_hours=cast(float, args.deadline_hours),
        )
        warning = campaign_deadline_warning(
            len(plan.files),
            cast(float, args.deadline_hours),
            settings.sync.rolling_new_limit,
            settings.sync.rolling_window_hours,
            max_new_per_cycle=settings.sync.max_new_per_cycle,
            max_pending_episode_jobs=settings.sync.max_pending_episode_jobs,
        )
        write_plan_file(cast(str, args.out), plan)
    finally:
        store.close()
    payload = {
        "campaign_id": plan.campaign_id,
        "approval_digest": digest,
        "plan_file": str(Path(cast(str, args.out))),
        "files": len(plan.files),
        "pending_targets": sum(1 for item in plan.files if item.shoko_episode_ids == ()),
        "max_submissions": plan.max_submissions,
        "deadline_iso": plan.deadline_iso,
        "mapping_revision": plan.mapping_revision,
        "warnings": [] if warning is None else [warning],
        "note": (
            "review this file, run a canary, then approve with: campaign approve --plan "
            f"{cast(str, args.out)} --digest {digest} (the digest is supplied SEPARATELY)"
        ),
    }
    if cast(bool, args.json):
        print(json_payload(payload))
        return EXIT_OK
    for warning_text in cast(list[str], payload["warnings"]):
        print(f"warning: {warning_text}")
    for key in ("campaign_id", "files", "pending_targets", "max_submissions", "deadline_iso"):
        print(f"{key}: {payload[key]}")
    print(f"plan file: {payload['plan_file']}")
    print(f"approval digest: {digest}")
    print(payload["note"])
    return EXIT_OK


def _run_campaign_approve(args: argparse.Namespace) -> int:
    settings = load_settings(cast(str, args.config))
    store = open_state_store(settings)
    try:
        plan = approve_campaign(store, cast(str, args.plan), cast(str, args.digest), current_iso())
        progress = store.campaign_progress(plan.campaign_id)
    finally:
        store.close()
    payload = {
        "campaign_id": plan.campaign_id,
        "approval_digest": plan_digest(plan),
        "files_approved": len(plan.files),
        "max_submissions": plan.max_submissions,
        "deadline_iso": plan.deadline_iso,
        "attestation_scope": f"campaign:{plan.campaign_id}",
        "progress": progress,
        "next": (
            f"campaign apply {cast(str, args.config)} --plan {cast(str, args.plan)} "
            "(bounded batches; one approval, repeated invocations consume no new approval)"
        ),
    }
    if cast(bool, args.json):
        print(json_payload(payload))
        return EXIT_OK
    print(f"campaign approved: {plan.campaign_id}")
    print(f"  approval digest: {payload['approval_digest']}")
    print(f"  files approved: {len(plan.files)}  max_submissions: {plan.max_submissions}")
    print(f"  deadline: {plan.deadline_iso}")
    print(payload["next"])
    return EXIT_OK


def _run_campaign_apply(args: argparse.Namespace) -> int:
    settings = load_settings(cast(str, args.config))
    batch_size = getattr(args, "batch_size", None)
    plan, _ = read_plan_file(cast(str, args.plan))
    store = open_state_store(settings)
    try:
        deps = _campaign_deps(settings, store, scope=f"campaign:{plan.campaign_id}")
        report = apply_campaign(
            deps,
            settings,
            store,
            cast(str, args.plan),
            current_iso(),
            batch_size=None if batch_size is None else int(batch_size),
        )
    finally:
        store.close()
    payload = campaign_apply_payload(report)
    if cast(bool, args.json):
        print(json_payload(payload))
        return EXIT_OK
    print(campaign_apply_summary_line(report))
    for reason in report.reasons[:10]:
        print(f"  reason: {reason}")
    return EXIT_OK


def _run_campaign_verify(args: argparse.Namespace) -> int:
    settings = load_settings(cast(str, args.config))
    campaign_id = cast(str, args.campaign_id)
    store = open_state_store(settings)
    try:
        deps = _campaign_deps(settings, store, scope=f"campaign:{campaign_id}")
        payload = campaign_verify(deps, store, campaign_id, current_iso())
    finally:
        store.close()
    if cast(bool, args.json):
        print(json_payload(payload))
        return EXIT_OK
    print(
        f"campaign verify {campaign_id}: verified={payload['verified']} "
        f"pending={payload['pending_verification']}"
    )
    for reason in payload["recovery_reasons"][:10]:
        print(f"  {reason}")
    return EXIT_OK


def _run_campaign_report(args: argparse.Namespace) -> int:
    settings = load_settings(cast(str, args.config))
    store = open_state_store(settings)
    try:
        payload = campaign_report(store, cast(str, args.campaign_id), getattr(args, "plan", None))
    finally:
        store.close()
    if cast(bool, args.json):
        print(json_payload(payload))
        return EXIT_OK
    print(
        f"campaign {payload['campaign_id']}: status={payload['status']} "
        f"({payload['status_reason']})"
    )
    print(f"  approval digest: {payload['approval_digest']}")
    print(
        f"  sent: {payload['total_sent']}/{payload['max_submissions']}  "
        f"verified: {payload['verified']}  "
    )
    print(f"  quarantined: {payload['quarantined']}  remaining: {payload['remaining_planned']}")
    print(f"  deadline: {payload['deadline_iso']}  last batch: {payload['last_batch_iso']}")
    print(
        f"  drain authorization: {payload['drain_selection']} "
        f"(authorized_iso={payload['drain_authorized_iso']} "
        f"digest_bound={payload['drain_authorized_digest']} "
        f"invalidated={payload['drain_invalidated_reason'] or 'none'})"
    )
    print(f"  drain rule: {DRAIN_AUTHORIZATION_RULE}")
    # F19: the durable attempted-send audit (every send ever attempted for this campaign).
    attempted = payload["attempted_sends"]
    print(f"  attempted sends: {len(attempted)}")
    for row in attempted[:ATTEMPTED_SEND_DISPLAY_LIMIT]:
        print(
            f"    file {row['shoko_file_id']} send {row['send_id']}: outcome={row['outcome']} "
            f"state={row['state']} attempted={row['attempted_iso']}"
        )
    remaining = len(attempted) - ATTEMPTED_SEND_DISPLAY_LIMIT
    if remaining > 0:
        print(f"    +{remaining} more")
    unresolved = payload["unresolved_legacy_sends"]
    if unresolved:
        print(
            f"  legacy history hold: {len(unresolved)} ambiguous send(s); resolve with "
            "campaign history-resolve before submitting or authorizing a drain"
        )
        for row in unresolved[:ATTEMPTED_SEND_DISPLAY_LIMIT]:
            print(
                f"    file {row['shoko_file_id']} send {row['send_id']}: "
                f"outcome={row['outcome']} state={row['state']}"
            )
    # F18: recoverable budget-cooldown deferral (campaign stays approved; auto-resumes).
    if payload["deferred_until"] is not None:
        print(f"  drain deferral: retry at {payload['deferred_until']} (campaign stays approved)")
    if payload["plan_file_digest"] is not None:
        print(
            f"  plan file digest: {payload['plan_file_digest']} "
            f"(matches: {payload['plan_matches_file']})"
        )
    return EXIT_OK


def _run_campaign_history_resolve(args: argparse.Namespace) -> int:
    if not cast(bool, args.confirm):
        print(f"campaign history-resolve: {_CONFIRM_REQUIRED}")
        return EXIT_CONFIG_INVALID
    settings = load_settings(cast(str, args.config))
    send_id = cast(int, args.send_id)
    campaign_id = None if cast(bool, args.non_campaign) else cast(str, args.campaign_id)
    note = cast(str, args.note)
    iso = current_iso()
    store = open_state_store(settings)
    try:
        store.resolve_legacy_send_ownership(send_id, campaign_id=campaign_id, note=note, iso=iso)
        unresolved = [row.send_id for row in store.unresolved_campaign_send_history()]
    finally:
        store.close()
    owner = "non_campaign" if campaign_id is None else f"campaign:{campaign_id}"
    payload = operator_action_payload(
        "campaign-history-resolve",
        note,
        iso=iso,
        result={"send_id": send_id, "owner": owner, "unresolved_send_ids": unresolved},
    )
    if cast(bool, args.json):
        print(json_payload(payload))
    else:
        print(f"send {send_id} ownership resolved as {owner}; unresolved sends: {len(unresolved)}")
    return EXIT_OK


def _run_campaign_pause(args: argparse.Namespace) -> int:
    if not cast(bool, args.confirm):
        print(f"campaign pause: {_CONFIRM_REQUIRED}")
        return EXIT_CONFIG_INVALID
    settings = load_settings(cast(str, args.config))
    store = open_state_store(settings)
    try:
        campaign = campaign_pause(
            store, cast(str, args.campaign_id), cast(str, args.reason), current_iso()
        )
    finally:
        store.close()
    payload = operator_action_payload(
        "campaign-pause",
        cast(str, args.note),
        iso=current_iso(),
        result={"status": campaign.status, "reason": campaign.status_reason},
    )
    if cast(bool, args.json):
        print(json_payload(payload))
    else:
        print(f"campaign {campaign.campaign_id} paused: {campaign.status_reason}")
    return EXIT_OK


def _run_campaign_resume(args: argparse.Namespace) -> int:
    if not cast(bool, args.confirm):
        print(f"campaign resume: {_CONFIRM_REQUIRED}")
        return EXIT_CONFIG_INVALID
    settings = load_settings(cast(str, args.config))
    campaign_id = cast(str, args.campaign_id)
    store = open_state_store(settings)
    try:
        verdict = attestation_verdict_for(settings, scope=f"campaign:{campaign_id}")
        campaign = campaign_resume(
            store, campaign_id, current_iso(), attestation=verdict, gate_open=apply_gate(settings)
        )
    finally:
        store.close()
    payload = operator_action_payload(
        "campaign-resume",
        cast(str, args.note),
        iso=current_iso(),
        result={"status": campaign.status},
    )
    if cast(bool, args.json):
        print(json_payload(payload))
    else:
        print(f"campaign {campaign.campaign_id} resumed (attestation valid, gate re-checked)")
    return EXIT_OK


def _run_campaign_revoke(args: argparse.Namespace) -> int:
    if not cast(bool, args.confirm):
        print(f"campaign revoke: {_CONFIRM_REQUIRED}")
        return EXIT_CONFIG_INVALID
    settings = load_settings(cast(str, args.config))
    store = open_state_store(settings)
    try:
        unbound = campaign_revoke(store, cast(str, args.campaign_id), current_iso())
    finally:
        store.close()
    payload = operator_action_payload(
        "campaign-revoke",
        cast(str, args.note),
        iso=current_iso(),
        result={"queued_rows_released": unbound},
    )
    if cast(bool, args.json):
        print(json_payload(payload))
    else:
        print(
            f"campaign {cast(str, args.campaign_id)} revoked: "
            f"{unbound} queued row(s) back to waiting"
        )
    return EXIT_OK


def _run_campaign_authorize_drain(args: argparse.Namespace) -> int:
    if not cast(bool, args.confirm):
        print(f"campaign authorize-drain: {_CONFIRM_REQUIRED}")
        return EXIT_CONFIG_INVALID
    note = cast(str, args.note)
    if not note.strip():
        print(
            "campaign authorize-drain refused: --note must record the canary inspection "
            "(physical-media side effects reviewed); nothing was written"
        )
        return EXIT_CONFIG_INVALID
    settings = load_settings(cast(str, args.config))
    campaign_id = cast(str, args.campaign_id)
    store = open_state_store(settings)
    try:
        campaign = authorize_campaign_drain(store, campaign_id, current_iso(), note=note)
        result = {
            "campaign_id": campaign.campaign_id,
            "drain_authorized": True,
            "drain_authorized_digest": campaign.drain_authorized_digest,
            "drain_selection": campaign_drain_selection(campaign),
        }
        _record_operator_action(store, "campaign-authorize-drain", note, current_iso(), result)
    finally:
        store.close()
    payload = operator_action_payload(
        "campaign-authorize-drain", note, iso=current_iso(), result=result
    )
    if cast(bool, args.json):
        print(json_payload(payload))
    else:
        print(
            f"campaign {campaign.campaign_id} drain authorized: every submitted canary file is "
            f"VERIFIED, bound to approval digest {campaign.drain_authorized_digest}"
        )
        print(f"  rule: {DRAIN_AUTHORIZATION_RULE}")
    return EXIT_OK


def _run_campaign_revoke_drain(args: argparse.Namespace) -> int:
    if not cast(bool, args.confirm):
        print(f"campaign revoke-drain: {_CONFIRM_REQUIRED}")
        return EXIT_CONFIG_INVALID
    note = cast(str, args.note)
    if not note.strip():
        print(
            "campaign revoke-drain refused: --note must record the revocation reason; "
            "nothing was written"
        )
        return EXIT_CONFIG_INVALID
    settings = load_settings(cast(str, args.config))
    campaign_id = cast(str, args.campaign_id)
    store = open_state_store(settings)
    try:
        campaign = revoke_campaign_drain(store, campaign_id, current_iso(), note=note)
        result = {
            "campaign_id": campaign.campaign_id,
            "drain_authorized": False,
            "drain_selection": campaign_drain_selection(campaign),
        }
        _record_operator_action(store, "campaign-revoke-drain", note, current_iso(), result)
    finally:
        store.close()
    payload = operator_action_payload(
        "campaign-revoke-drain", note, iso=current_iso(), result=result
    )
    if cast(bool, args.json):
        print(json_payload(payload))
    else:
        print(
            f"campaign {campaign.campaign_id} drain authorization revoked: the daemon skips it "
            "(manual campaign apply stays available)"
        )
    return EXIT_OK


_CAMPAIGN_HANDLERS: Final[dict[str, CommandHandler]] = {
    "prepare": _run_campaign_prepare,
    "approve": _run_campaign_approve,
    "apply": _run_campaign_apply,
    "verify": _run_campaign_verify,
    "report": _run_campaign_report,
    "history-resolve": _run_campaign_history_resolve,
    "pause": _run_campaign_pause,
    "resume": _run_campaign_resume,
    "revoke": _run_campaign_revoke,
    "authorize-drain": _run_campaign_authorize_drain,
    "revoke-drain": _run_campaign_revoke_drain,
}


# ------------------------------------------------------------------ audit command


def _run_audit(args: argparse.Namespace) -> int:
    """Bounded read-only existing-link audit (GETs only; zero writes; informational exit)."""
    settings = load_settings(cast(str, args.config))
    mapping_store = open_mapping_store(settings)
    from .mappings import load_overrides

    overrides = (
        load_overrides(cast(str, read_overrides_text(settings)))
        if settings.mappings.overrides_file
        else None
    )
    active = load_active(mapping_store, overrides)
    if active is None:
        print(
            "audit refused: no active mapping generation — run mappings refresh first "
            "(the audit compares against verified mappings)"
        )
        return EXIT_CONFIG_INVALID
    sonarr = sonarr_endpoint(settings)
    shoko = shoko_endpoint(settings)
    try:
        files, tvdb, problem = collect_audit_inputs(sonarr, settings)
        report = run_audit(
            shoko=shoko,
            settings=settings,
            active=active,
            sonarr_files=files,
            series_tvdb=tvdb,
            series_anidb=getattr(args, "series_anidb", None),
            limit=cast(int, args.limit),
            max_requests=cast(int, args.max_requests),
        )
    finally:
        sonarr.client.close()
        shoko.client.close()
    payload = audit_payload(report)
    if problem is not None:
        payload["warnings"] = [problem]
    if cast(bool, args.json):
        print(json_payload(payload))
    else:
        print(audit_summary_line(report))
        for finding in report.findings[:25]:
            print(
                f"  file {finding.shoko_file_id}: {finding.classification.value} "
                f"(linked={finding.linked_identities} targets={finding.target_identities}) — "
                f"{finding.detail}"
            )
        for failure in report.failures[:5]:
            print(f"  audit failure: {failure}")
    return EXIT_RUNTIME if report.transport_failed else EXIT_OK


# ------------------------------------------------------------------ parser


def build_parser() -> argparse.ArgumentParser:
    """Construct the full command tree (M7-a read/operate surface)."""
    parser = argparse.ArgumentParser(
        prog="shoko-sonarr-linker",
        description=(
            "Link unrecognized Shoko files to Shoko episodes using Sonarr episode metadata "
            "and Anime-Lists mappings."
        ),
        epilog=(
            "Safety defaults: dry-run only. Live writes require BOTH 'sync.apply_changes: true' "
            f"in the config AND the operator opt-in {APPLY_ENV_VAR}=1 in the environment; no "
            "command-line flag enables writes by itself. Operator commands require --confirm.\n"
            "Exit codes: 0 ok (incl. mapping 304/unchanged and informational status/report), "
            "2 config/request invalid, 3 apply-gate refusal, 4 runtime/network failure, "
            "5 state error."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="debug-level logging")
    subparsers = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")
    _add_service(subparsers)
    _add_ordinary_controls(subparsers)
    _add_validate(subparsers)
    _add_once(subparsers)
    _add_daemon(subparsers)
    _add_report(subparsers)
    _add_mappings(subparsers)
    _add_config(subparsers)
    _add_health(subparsers)
    _add_resolve_send(subparsers)
    _add_clear_requires_rebaseline(subparsers)
    _add_steal_apply_lock(subparsers)
    _add_resume_writes(subparsers)
    _add_pause_writes(subparsers)
    _add_attestation(subparsers)
    _add_campaign(subparsers)
    _add_audit(subparsers)
    return parser


def _add_service(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "service",
        help=(
            "Start the automatic service (read-only connection monitoring until setup "
            "completes)."
        ),
    )
    parser.set_defaults(handler=_run_service)


def _add_ordinary_controls(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Expose plain-language local service controls without a YAML configuration."""
    definitions: tuple[tuple[str, str, CommandHandler], ...] = (
        ("status", "Show normal service status without network access.", _run_status),
        ("pause", "Pause automatic linking and preserve queued work.", _run_pause),
        ("resume", "Recheck setup and safety before resuming automatic linking.", _run_resume),
    )
    for name, help_text, handler in definitions:
        parser = subparsers.add_parser(name, help=help_text)
        parser.set_defaults(handler=handler)


def _add_validate(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("validate", help="Validate a configuration file (offline).")
    _add_config_argument(parser)
    parser.add_argument(
        "--offline",
        action="store_true",
        default=True,
        help="validate without network access (default)",
    )
    parser.add_argument(
        "--live-read-only",
        action="store_true",
        help="NOT IMPLEMENTED in this build: refuses immediately and makes no network call",
    )


def _add_once(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "once", help="Run one reconciliation cycle (dry-run by default)."
    )
    _add_config_argument(parser)
    parser.add_argument(
        "--dry-run", action="store_true", help="report proposals only (the default)"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help=f"request live writes; allowed only by the gate ({APPLY_ENV_VAR}=1 + config opt-in)",
    )
    parser.add_argument(
        "--max-links",
        type=_whole_number_at_least_one,
        default=None,
        metavar="N",
        help="maximum POST submissions this cycle (default: sync.max_new_per_cycle); "
        "counts submissions, not enumerated files",
    )
    _add_policy_argument(parser, RunPolicy.NEW_ARRIVAL)
    _add_json_argument(parser)


def _add_daemon(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "daemon", help="Run the polling daemon (SIGINT/SIGTERM graceful)."
    )
    _add_config_argument(parser)
    parser.add_argument(
        "--cycles",
        type=_whole_number_at_least_one,
        default=None,
        metavar="N",
        help="stop after N cycles (bounded run; documented test/CI aid)",
    )
    parser.add_argument(
        "--once-signal",
        action="store_true",
        help="run exactly one cycle with signal handling, then exit (smoke aid)",
    )
    _add_policy_argument(parser, RunPolicy.NEW_ARRIVAL)
    _add_json_argument(parser)


def _add_report(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "report", help="Read-only rollup from the state DB and mapping cache (no network)."
    )
    _add_config_argument(parser)
    _add_json_argument(parser)


def _add_health(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("health", help="Cheap liveness probe (state/meta + cache only).")
    _add_config_argument(parser)
    _add_json_argument(parser)


def _add_mappings(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("mappings", help="Anime-Lists mapping cache commands.")
    mapping_sub = parser.add_subparsers(
        dest="mappings_command", required=True, metavar="SUBCOMMAND"
    )
    status = mapping_sub.add_parser("status", help="Active generation, age, schedule (offline).")
    _add_config_argument(status)
    _add_json_argument(status)
    refresh = mapping_sub.add_parser("refresh", help="Due-gated refresh of the mapping source.")
    _add_config_argument(refresh)
    refresh.add_argument("--force", action="store_true", help="run now even when not due")
    _add_json_argument(refresh)


def _add_config(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("config", help="Report the resolved configuration file path.")
    parser.add_argument(
        "path",
        nargs="?",
        default=DEFAULT_CONFIG_PATH,
        help=f"configuration file (default: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument("--check", action="store_true", help="load and validate the file")


def _add_resolve_send(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "resolve-send", help="Operator close of a live terminal-review send (--confirm required)."
    )
    _add_config_argument(parser)
    parser.add_argument("--file-id", type=_whole_number_at_least_one, required=True, metavar="N")
    parser.add_argument(
        "--fingerprint", required=True, metavar="FP", help="request fingerprint (64 hex)"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--release", action="store_true", help="close as released_after_review")
    group.add_argument("--block", action="store_true", help="close as operator_closed")
    _add_confirm_note_arguments(parser)
    _add_json_argument(parser)


def _add_clear_requires_rebaseline(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    parser = subparsers.add_parser(
        "clear-requires-rebaseline", help="Operator clear of the persisted rebaseline flag."
    )
    _add_config_argument(parser)
    _add_confirm_note_arguments(parser)
    _add_json_argument(parser)


def _add_steal_apply_lock(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "steal-apply-lock",
        help="Take over a dead owner's lease (needs a free OS lock; a live owner refuses).",
    )
    _add_config_argument(parser)
    _add_confirm_note_arguments(parser)
    _add_json_argument(parser)


def _add_resume_writes(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "resume-writes", help="Clear the stopped-writes marker and write pause (operator recovery)."
    )
    _add_config_argument(parser)
    _add_confirm_note_arguments(parser)
    _add_json_argument(parser)


def _add_pause_writes(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("pause-writes", help="Persist a write pause with a reason.")
    _add_config_argument(parser)
    parser.add_argument(
        "--reason",
        required=True,
        metavar="TEXT",
        help="why writes pause (a distinct operator reason, e.g. disk_review/maintenance/"
        "operator_paused; the reserved reason 'attestation_expired' belongs to the gate)",
    )
    parser.add_argument(
        "--hours",
        type=_positive_number,
        default=None,
        metavar="H",
        help="record a suggested resume horizon",
    )
    _add_confirm_note_arguments(parser)
    _add_json_argument(parser)


def _add_attestation(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "attestation", help="Safety attestation lease: status / record / invalidate."
    )
    attestation_sub = parser.add_subparsers(
        dest="attestation_command", required=True, metavar="SUBCOMMAND"
    )
    status = attestation_sub.add_parser(
        "status", help="Evaluate the configured lease offline (no network; informational)."
    )
    _add_config_argument(status)
    status.add_argument(
        "--scope",
        default=SCOPE_DAEMON,
        metavar="SCOPE",
        help="daemon | campaign:<id> (default daemon)",
    )
    _add_json_argument(status)
    record = attestation_sub.add_parser(
        "record",
        help="Operator builds the lease file from inspected facts (--confirm required).",
    )
    _add_config_argument(record)
    record.add_argument("--endpoint", required=True, metavar="URL", help="Shoko base URL attested")
    record.add_argument("--version", required=True, metavar="VER", help="inspected Shoko version")
    record.add_argument(
        "--mounts",
        required=True,
        metavar="ITEMS",
        help="comma-separated read-only-mount summaries (operator-inspected)",
    )
    record.add_argument(
        "--settings",
        required=True,
        metavar="FLAGS",
        help="comma-separated safety flags: FileQualityFilterEnabled=false,RenameOnImport=false,"
        "MoveOnImport=false,MyListAddFiles=false (the operator's recorded values — never a "
        "raw dump)",
    )
    record.add_argument(
        "--hours",
        type=_positive_number,
        default=None,
        metavar="H",
        help="lease validity (default: safety.attestation_validity_hours; capped by it)",
    )
    record.add_argument(
        "--scope", default=SCOPE_DAEMON, metavar="SCOPE", help="daemon | campaign:<id>"
    )
    record.add_argument(
        "--note", required=True, metavar="TEXT", help="operator note recorded in the lease"
    )
    record.add_argument(
        "--confirm",
        action="store_true",
        help="required confirmation that this records a safety attestation",
    )
    _add_json_argument(record)
    invalidate = attestation_sub.add_parser(
        "invalidate", help="Move the lease aside (.revoked suffix) with an audit note."
    )
    _add_config_argument(invalidate)
    invalidate.add_argument(
        "--scope",
        default=SCOPE_DAEMON,
        metavar="SCOPE",
        help="daemon | campaign:<id> (default daemon)",
    )
    _add_confirm_note_arguments(invalidate)
    _add_json_argument(invalidate)


def _add_campaign(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "campaign",
        help=(
            "Backlog campaign: prepare / approve / apply / verify / report / "
            "history-resolve / pause / resume / revoke / authorize-drain / revoke-drain."
        ),
    )
    campaign_sub = parser.add_subparsers(
        dest="campaign_command", required=True, metavar="SUBCOMMAND"
    )
    prepare = campaign_sub.add_parser(
        "prepare", help="Dry-run reconciliation → canonical plan file (zero writes)."
    )
    _add_config_argument(prepare)
    prepare.add_argument(
        "--limit",
        type=_whole_number_at_least_one,
        default=None,
        metavar="N",
        help="max candidate files (default: campaign.max_campaign_files)",
    )
    prepare.add_argument(
        "--deadline-hours",
        type=_positive_number,
        default=168.0,
        metavar="H",
        help="campaign deadline from preparation time (default 168)",
    )
    prepare.add_argument("--out", required=True, metavar="FILE", help="plan file to write")
    _add_json_argument(prepare)
    approve = campaign_sub.add_parser(
        "approve", help="Approve the plan with the digest supplied SEPARATELY from the file."
    )
    _add_config_argument(approve)
    approve.add_argument("--plan", required=True, metavar="FILE")
    approve.add_argument(
        "--digest", required=True, metavar="HEX", help="expected canonical SHA-256 (64 hex)"
    )
    _add_json_argument(approve)
    apply_command = campaign_sub.add_parser(
        "apply", help="One bounded batch of approved candidates through the guarded path."
    )
    _add_config_argument(apply_command)
    apply_command.add_argument("--plan", required=True, metavar="FILE")
    apply_command.add_argument(
        "--batch-size",
        type=_whole_number_at_least_one,
        default=None,
        metavar="N",
        help="files processed this invocation (default/cap: campaign.max_batch_size)",
    )
    _add_json_argument(apply_command)
    verify = campaign_sub.add_parser(
        "verify", help="Re-poll verification of campaign sends (read-only cycle; zero POSTs)."
    )
    _add_config_argument(verify)
    verify.add_argument("--campaign-id", required=True, metavar="ID")
    _add_json_argument(verify)
    report = campaign_sub.add_parser("report", help="Campaign progress, digests, pause reasons.")
    _add_config_argument(report)
    report.add_argument("--campaign-id", required=True, metavar="ID")
    report.add_argument(
        "--plan", default=None, metavar="FILE", help="also check the plan file digest"
    )
    _add_json_argument(report)
    _add_campaign_history_resolve(campaign_sub)
    pause = campaign_sub.add_parser("pause", help="Persist a campaign pause with a reason.")
    _add_config_argument(pause)
    pause.add_argument("--campaign-id", required=True, metavar="ID")
    pause.add_argument("--reason", default="operator_paused", metavar="TEXT")
    _add_confirm_note_arguments(pause)
    _add_json_argument(pause)
    resume = campaign_sub.add_parser(
        "resume", help="Resume after re-checking attestation and gate."
    )
    _add_config_argument(resume)
    resume.add_argument("--campaign-id", required=True, metavar="ID")
    _add_confirm_note_arguments(resume)
    _add_json_argument(resume)
    revoke = campaign_sub.add_parser(
        "revoke", help="Revoke: queued rows go back to waiting; future apply refuses."
    )
    _add_config_argument(revoke)
    revoke.add_argument("--campaign-id", required=True, metavar="ID")
    _add_confirm_note_arguments(revoke)
    _add_json_argument(revoke)
    _add_campaign_drain_subcommands(campaign_sub)


def _add_campaign_history_resolve(
    campaign_sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    history_resolve = campaign_sub.add_parser(
        "history-resolve",
        help="Resolve one ambiguous legacy send owner after operator review.",
    )
    _add_config_argument(history_resolve)
    history_resolve.add_argument("--send-id", type=_whole_number_at_least_one, required=True)
    owner = history_resolve.add_mutually_exclusive_group(required=True)
    owner.add_argument(
        "--campaign-id",
        metavar="ID",
        help="attribute only when the immutable send matches this campaign's approved binding",
    )
    owner.add_argument(
        "--non-campaign",
        action="store_true",
        help="explicitly attest the send was outside every campaign (requires --confirm and note)",
    )
    _add_confirm_note_arguments(history_resolve)
    _add_json_argument(history_resolve)


def _add_campaign_drain_subcommands(
    campaign_sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """F10: the operator drain-authorization pair (grant + withdrawal)."""
    authorize_drain = campaign_sub.add_parser(
        "authorize-drain",
        help=(
            "Confirm the settled canary and authorize the automatic daemon drain "
            "(digest-bound; manual apply needs no authorization)."
        ),
    )
    _add_config_argument(authorize_drain)
    authorize_drain.add_argument("--campaign-id", required=True, metavar="ID")
    _add_confirm_note_arguments(authorize_drain)
    _add_json_argument(authorize_drain)
    revoke_drain = campaign_sub.add_parser(
        "revoke-drain",
        help=(
            "Withdraw the drain authorization: the daemon skips the campaign "
            "(manual apply stays available)."
        ),
    )
    _add_config_argument(revoke_drain)
    revoke_drain.add_argument("--campaign-id", required=True, metavar="ID")
    _add_confirm_note_arguments(revoke_drain)
    _add_json_argument(revoke_drain)


def _add_audit(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "audit",
        help=(
            "Read-only existing-link audit (GETs only, zero writes, rate-limited; "
            "informational exit)."
        ),
    )
    _add_config_argument(parser)
    parser.add_argument(
        "--series-anidb",
        type=_whole_number_at_least_one,
        default=None,
        metavar="N",
        help="restrict the audit to files whose cross-references name this AniDB series",
    )
    parser.add_argument(
        "--limit",
        type=_whole_number_at_least_one,
        default=100,
        metavar="N",
        help="max linked Shoko files examined (default 100)",
    )
    parser.add_argument(
        "--max-requests",
        type=_whole_number_at_least_one,
        default=100,
        metavar="N",
        help="hard Shoko request budget (default 100); the audit stops at the cap",
    )
    _add_json_argument(parser)


def _add_config_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "config",
        nargs="?",
        default=DEFAULT_CONFIG_PATH,
        help=f"configuration file (default: {DEFAULT_CONFIG_PATH})",
    )


def _add_json_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--json", action="store_true", help="machine-readable nonsecret JSON output"
    )


def _add_confirm_note_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="required confirmation that this command mutates durable state",
    )
    parser.add_argument(
        "--note", required=True, metavar="TEXT", help="operator note recorded in the audit"
    )


def _add_policy_argument(parser: argparse.ArgumentParser, default: RunPolicy) -> None:
    parser.add_argument(
        "--policy",
        default=default.value,
        choices=_POLICY_CHOICES,
        help=f"run policy for this invocation (default: {default.value})",
    )


def _whole_number_at_least_one(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{raw!r} is not a whole number") from None
    if value < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return value


def _positive_number(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{raw!r} is not a number") from None
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError("must be finite and greater than 0")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    """Parse arguments, run the selected command, return its exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if cast(bool, args.verbose):
        configure_logging(logging.DEBUG)
    command = cast(str, args.command)
    handler = _COMMAND_HANDLERS[command]
    try:
        return handler(args)
    except ConfigError as exc:
        return _report_config_invalid(exc)
    except PolicyError as exc:
        print(f"request invalid: {exc}", file=sys.stderr)
        return EXIT_CONFIG_INVALID
    except StateError as exc:
        print(f"state refused: {exc}", file=sys.stderr)
        return EXIT_STATE
    except HttpPolicyError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_RUNTIME
    except LinkerError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_CONFIG_INVALID


def print_config_summary(settings: Settings) -> None:
    """Human-readable configuration summary. Secret values are never included."""
    print(
        f"config file: {settings.config_path if settings.config_path is not None else '<string>'}"
    )
    print(f"  sonarr: {settings.sonarr.url}")
    _print_roots(settings.sonarr.allowed_roots)
    print(f"  shoko: {settings.shoko.url}")
    _print_roots(settings.shoko.allowed_roots)
    print(f"  path mappings ({len(settings.path_mappings)}):")
    for mapping in settings.path_mappings:
        print(f"    {mapping.sonarr} -> {mapping.shoko}")
    print(
        f"  mappings: source={settings.mappings.source}, "
        f"refresh_hours={settings.mappings.refresh_hours:g}, "
        f"overrides={settings.mappings.overrides_file or '<none>'}"
    )
    sync = settings.sync
    print(
        f"  sync: interval_seconds={sync.interval_seconds:g}, "
        f"full_reconcile_interval_hours={sync.full_reconcile_interval_hours:g}, "
        f"new_file_grace_period_seconds={sync.new_file_grace_period_seconds:g}, "
        f"max_concurrent_links={sync.max_concurrent_links}"
    )
    print(
        f"  budgets: max_new_per_cycle={sync.max_new_per_cycle}, "
        f"rolling_new_limit={sync.rolling_new_limit}, "
        f"rolling_window_hours={sync.rolling_window_hours:g}, "
        f"max_pending_episode_jobs={sync.max_pending_episode_jobs}"
    )
    print(
        f"  cycle: max_cycle_seconds={sync.max_cycle_seconds:g}, "
        f"max_recovery_rows_per_cycle={sync.max_recovery_rows_per_cycle}, "
        f"verification_timeout_seconds={sync.verification_timeout_seconds:g}, "
        f"apply_lock_lease_seconds={sync.apply_lock_lease_seconds:g}, "
        f"jitter_ratio={sync.jitter_ratio:g}, mapping_churn_limit={sync.mapping_churn_limit}"
    )
    print(
        f"  timeouts: connect={sync.connect_timeout_seconds:g}s, "
        f"read={sync.read_timeout_seconds:g}s"
    )
    print(f"  database: {settings.database.path}")
    safety = settings.safety
    print(
        f"  safety: attestation_file={safety.attestation_file or '<null — applies stay closed>'}, "
        f"validity_hours={safety.attestation_validity_hours:g}, "
        f"warn_seconds={safety.attestation_warn_seconds:g}"
    )
    campaign = settings.campaign
    print(
        f"  campaign: max_campaign_files={campaign.max_campaign_files}, "
        f"max_batch_size={campaign.max_batch_size}, "
        f"daemon_auto_apply={campaign.daemon_auto_apply}"
    )
    print(
        f"  api keys: read from the environment at access time "
        f"({', '.join(settings.secret_env_vars)}); values are never stored, printed, or logged"
    )
    print(f"  writes: {apply_gate_status(settings).describe()}")


def _print_roots(roots: Sequence[str]) -> None:
    print(f"    allowed_roots ({len(roots)}): {', '.join(roots)}")


def _report_config_invalid(exc: ConfigError) -> int:
    print(f"config invalid: {exc}", file=sys.stderr)
    return EXIT_CONFIG_INVALID


def _run_config(args: argparse.Namespace) -> int:
    resolved = Path(cast(str, args.path)).expanduser().resolve()
    print(f"config path: {resolved}")
    if not cast(bool, args.check):
        print("check: not requested (pass --check to load and validate the file)")
        return EXIT_OK
    settings = load_settings(resolved)
    print_config_summary(settings)
    return EXIT_OK


_COMMAND_HANDLERS: Final[dict[str, CommandHandler]] = {
    "service": _run_service,
    "status": _run_status,
    "pause": _run_pause,
    "resume": _run_resume,
    "validate": _run_validate,
    "once": _run_once,
    "daemon": _run_daemon,
    "report": _run_report,
    "health": _run_health,
    "mappings": _run_mappings,
    "config": _run_config,
    "resolve-send": _run_resolve_send,
    "clear-requires-rebaseline": _run_clear_requires_rebaseline,
    "steal-apply-lock": _run_steal_apply_lock,
    "resume-writes": _run_resume_writes,
    "pause-writes": _run_pause_writes,
    "attestation": _run_attestation,
    "campaign": _run_campaign,
    "audit": _run_audit,
}


if __name__ == "__main__":
    sys.exit(main())
