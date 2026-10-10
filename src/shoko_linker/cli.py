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
import os
import signal
import sys
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import FrameType
from typing import Any, Final, Literal, cast

from .attestation import (
    SCOPE_DAEMON,
    AttestationError,
    AttestationRecord,
    AttestationVerdict,
    build_record,
    lease_expiry_cap,
    load_attestation,
    validate_scope,
    write_attestation,
)
from .audit import collect_audit_inputs, run_audit
from .campaign import (
    CampaignDigestMismatchError,
    CampaignError,
    CampaignPlanInvalidError,
    apply_campaign,
    approve_campaign,
    campaign_pause,
    campaign_report,
    campaign_resume,
    campaign_revoke,
    campaign_verify,
    plan_digest,
    prepare_campaign,
    read_plan_file,
    write_plan_file,
)
from .config import (
    APPLY_ENV_VAR,
    DEFAULT_CONFIG_PATH,
    Settings,
    apply_gate,
    apply_gate_status,
    load_settings,
)
from .daemon import (
    CycleMode,
    CycleReport,
    DaemonDeps,
    compute_eligibility,
    full_cycle_due,
    run_cycle,
    run_daemon,
)
from .errors import ConfigError, HttpPolicyError, LinkerError, PolicyError
from .http import ApiEndpoint, make_shoko_client, make_sonarr_client
from .logging_setup import configure_logging
from .mapping_store import MappingStore, load_active, make_source_endpoint
from .mapping_store import (
    refresh as mapping_refresh,
)
from .mapping_store import (
    status as mapping_status,
)
from .policies import PolicyRequest, RunPolicy, parse_policy
from .reporting import (
    META_CYCLE_ALIVE,
    META_CYCLE_APPLY_ELIGIBLE,
    META_CYCLE_FAILED,
    META_CYCLE_KEY,
    META_CYCLE_MODE,
    META_CYCLE_RECONCILIATION_READY,
    META_CYCLE_SUBMITTED,
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
from .state import (
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
    steal_expired_apply_lock,
)

type CommandHandler = Callable[[argparse.Namespace], int]
type SignalHandler = Callable[[int, FrameType | None], Any]

EXIT_OK: Final[int] = 0
EXIT_CONFIG_INVALID: Final[int] = 2
EXIT_APPLY_GATE_REFUSAL: Final[int] = 3
EXIT_RUNTIME: Final[int] = 4
EXIT_STATE: Final[int] = 5

_POLICY_CHOICES: Final[list[str]] = [policy.value for policy in RunPolicy]
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
_ATTESTATION_NOTE: Final[str] = (
    "safety attestation: the lease at safety.attestation_file is evaluated offline "
    "before every apply; absent/invalid/expired suppresses writes and persists one pause "
    "per transition (the daemon stays alive reporting read-only)"
)


# ------------------------------------------------------------------ dependency seams


def current_iso() -> str:
    """The wall clock for cycle timestamps (test seam)."""
    return datetime.now(UTC).isoformat()


def attestation_target_path(settings: Settings) -> Path | None:
    """Resolve the lease file path from ``safety.attestation_file`` (test seam)."""
    if settings.safety.attestation_file is None:
        return None
    return Path(settings.safety.attestation_file)


def attestation_verdict_for(
    settings: Settings, *, scope: str = SCOPE_DAEMON, version_hint: str | None = None
) -> AttestationVerdict:
    """Evaluate the configured safety attestation lease offline (test seam, no network).

    The Shoko URL identity, the requested ``scope`` (``daemon`` for the daemon/
    ``once --apply``; ``campaign:<id>`` for a campaign apply), the configured warn lead,
    the configured version pin (``safety.attested_shoko_version`` — M7 review P3-4: a
    lease recorded for a different Shoko version is ``stale_version``, fail closed; an
    explicit ``version_hint`` overrides it), and :func:`current_iso` are the only inputs —
    nothing is fetched, nothing is guessed. An unconfigured lease is ``absent``.
    """
    hint = settings.safety.attested_shoko_version if version_hint is None else version_hint
    return load_attestation(
        attestation_target_path(settings),
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


def build_daemon_deps(
    settings: Settings,
    *,
    store: StateStore,
    mapping_store: MappingStore,
    max_submissions: int,
    overrides_text: str | None = None,
    attestation_scope: str = SCOPE_DAEMON,
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
        store.set_meta(META_CYCLE_FAILED, "1" if report.failed else "0")
        store.set_meta(META_CYCLE_SUBMITTED, str(report.counts.submitted))
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
        _start_cycle_meta(store, "once-1", started, settings.sync.interval_seconds)
        report = run_cycle(
            deps,
            now_iso=started,
            mode=mode,
            policy=request.policy,
            cycle_key="once-1",
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
    return [
        f"requested policy: {request.policy.label}",
        f"loop mode: {_mode_label(mode)} (apply requires the gate; validated once at start)",
        cadence,
        (
            f"budgets: max_new_per_cycle={settings.sync.max_new_per_cycle}, "
            f"rolling_new_limit={settings.sync.rolling_new_limit}, "
            f"max_pending_episode_jobs={settings.sync.max_pending_episode_jobs}"
        ),
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
        )
        index = -1

        def now_iso() -> str:
            nonlocal index
            index += 1
            iso = current_iso()
            cycle_key = _daemon_cycle_key(index)
            _start_cycle_meta(store, cycle_key, iso, settings.sync.interval_seconds)
            logging.getLogger(__name__).info("cycle %s starting (%s)", cycle_key, iso)
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
                "cycle %s complete: mode=%s submitted=%d full=%s failed=%s",
                report.cycle_key,
                report.mode.value,
                report.counts.submitted,
                report.full,
                report.failed,
            )

        daemon_report = run_daemon(
            deps,
            cycle_key=_daemon_cycle_key,
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
    now = datetime.fromisoformat(current_iso())
    mapping_store = open_mapping_store(settings)
    status = mapping_status(mapping_store, now)
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
        f"last_outcome={mapping['last_outcome']}"
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
                    f"quarantined={progress.get('quarantined', 0)}"
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
            f"apply_eligible={levels['apply_eligible']}"
        )
        for item in payload["detail_rollup"][:10]:
            print(f"  detail x{item['count']}: {item['detail']}")


def _run_health(args: argparse.Namespace) -> int:
    """Cheap liveness probe from state/meta and the mapping store only (no network)."""
    settings = load_settings(cast(str, args.config))
    now = datetime.fromisoformat(current_iso())
    mapping_store = open_mapping_store(settings)
    status = mapping_status(mapping_store, now)
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
    status = mapping_status(store, now)
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
    status = mapping_status(store, now)
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

    def action(_settings: Settings, store: StateStore, iso: str) -> Mapping[str, Any]:
        file_id = int(args.file_id)
        send_id = store.resolve_send(
            file_id,
            cast(str, args.fingerprint),
            resolution=resolution,
            iso=iso,
            operator_note=cast(str, args.note),
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
    def action(settings: Settings, store: StateStore, iso: str) -> Mapping[str, Any]:
        # Only an EXPIRED lease is stealable (state API); a live lease
        # raises LockHeldError naming the holder pid and lease expiry (exit 5).
        lease = _iso_plus(iso, settings.sync.apply_lock_lease_seconds)
        acquisition = steal_expired_apply_lock(store, os.getpid(), iso, lease_expires_iso=lease)
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
    def action(_settings: Settings, store: StateStore, iso: str) -> Mapping[str, Any]:
        reason = cast(str, args.reason)
        store.pause(reason, iso)
        hours = getattr(args, "hours", None)
        until = None if hours is None else _iso_plus(iso, float(hours) * 3600.0)
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
    lease = settings.safety.attestation_file or "<safety.attestation_file not configured>"
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
    target = attestation_target_path(settings)
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
    scope = validate_scope(cast("str | None", args.scope) or SCOPE_DAEMON)
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
    expires = _iso_plus(inspected, hours * 3600.0)
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


def _run_attestation_invalidate(args: argparse.Namespace) -> int:
    """Move the lease aside (``.revoked`` suffix) and audit the action. Apply stays closed."""
    if not cast(bool, args.confirm):
        print(f"attestation invalidate: {_CONFIRM_REQUIRED}")
        return EXIT_CONFIG_INVALID
    settings = load_settings(cast(str, args.config))
    source = attestation_target_path(settings)
    if source is None:
        print("refused: safety.attestation_file is not configured")
        return EXIT_CONFIG_INVALID
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
    result: dict[str, Any] = {"revoked_path": str(target), "note": note}
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
        "note": (
            "review this file, run a canary, then approve with: campaign approve --plan "
            f"{cast(str, args.out)} --digest {digest} (the digest is supplied SEPARATELY)"
        ),
    }
    if cast(bool, args.json):
        print(json_payload(payload))
        return EXIT_OK
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
    if payload["plan_file_digest"] is not None:
        print(
            f"  plan file digest: {payload['plan_file_digest']} "
            f"(matches: {payload['plan_matches_file']})"
        )
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


_CAMPAIGN_HANDLERS: Final[dict[str, CommandHandler]] = {
    "prepare": _run_campaign_prepare,
    "approve": _run_campaign_approve,
    "apply": _run_campaign_apply,
    "verify": _run_campaign_verify,
    "report": _run_campaign_report,
    "pause": _run_campaign_pause,
    "resume": _run_campaign_resume,
    "revoke": _run_campaign_revoke,
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
        "steal-apply-lock", help="Take over an EXPIRED apply lease (live leases refuse)."
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
    parser.add_argument("--reason", required=True, metavar="TEXT")
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
    _add_confirm_note_arguments(invalidate)
    _add_json_argument(invalidate)


def _add_campaign(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser(
        "campaign",
        help=(
            "Backlog campaign: prepare / approve / apply / verify / report / "
            "pause / resume / revoke."
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
    if value <= 0:
        raise argparse.ArgumentTypeError("must be greater than 0")
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
        f"max_batch_size={campaign.max_batch_size}"
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
