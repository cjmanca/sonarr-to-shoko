"""Managed service runtime gates cycles on current durable onboarding consent."""

from __future__ import annotations

import json
import threading
from dataclasses import replace
from datetime import timedelta
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
import test_daemon as td

from shoko_linker import cli
from shoko_linker.bootstrap import BootstrapSettings
from shoko_linker.config import SecretRef, Settings, load_settings
from shoko_linker.daemon import (
    CycleCounts,
    CycleMode,
    CycleReport,
    DaemonDeps,
    _bind_service_authorization,
    run_cycle,
    run_daemon,
)
from shoko_linker.dashboard import DashboardApplication
from shoko_linker.http import ApiEndpoint
from shoko_linker.linking import SendOutcome, VerificationVerdict
from shoko_linker.mapping_store import MappingStore, RefreshOutcome
from shoko_linker.mapping_store import status as mapping_status
from shoko_linker.onboarding import OnboardingContext, OnboardingManager, ScopeBinding
from shoko_linker.paths import PathMapping
from shoko_linker.policies import RunPolicy
from shoko_linker.safety_check import assess_settings_payload
from shoko_linker.service import ConnectionReport
from shoko_linker.state import (
    META_LAST_DISCOVERY_EXCLUSIONS,
    BudgetLimits,
    Cohort,
    FileState,
    SendToken,
    StateStore,
    open_state,
)

EXAMPLE_CONFIG = Path(__file__).resolve().parents[1] / "config.example.yaml"


def _assert_scope_consent_is_not_invalidated(
    state_path: Path, expected_roots: tuple[str, ...]
) -> None:
    with open_state(state_path) as store:
        manager = OnboardingManager(store)
        record = manager.record()
        assert record is not None
        assert record.binding.roots == expected_roots
        assert not manager.reconfirmation_required()
        assert store.get_service_pause() == (False, "")


def _service_authorized_local(deps: DaemonDeps) -> bool:
    assert deps.service_authorized_local is not None
    return deps.service_authorized_local()


def test_failed_initial_link_stays_visibly_held_after_healthy_cycle_and_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    files = [
        td.SpecFile(series_id=1, file_id=10, shoko_id=500),
        td.SpecFile(series_id=1, file_id=11, shoko_id=501),
    ]
    td.activate_mapping(tmp_path, td.mapping_xml())
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, files)
    shoko = td.ShokoStub(files)
    shoko.post_status = 400
    shoko.link_on_post = False
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=files,
        shoko_files=files,
        xml=td.mapping_xml(),
        monotonic=td.Clock([0.0]),
        advance=lambda _: None,
        shoko=shoko,
    )
    deps = replace(deps, service_mode=True, service_authorized=lambda: True)
    bootstrap = BootstrapSettings(
        td.SONARR_BASE,
        SecretRef("SONARR_API_KEY"),
        td.SHOKO_BASE,
        SecretRef("SHOKO_API_KEY"),
        tmp_path / "state.sqlite3",
    )
    runtime = cli._NormalServiceRuntime(bootstrap, deps.sonarr, deps.shoko, cli.StopState())
    try:
        for iso in (td.T0, td.T1, td.T2):
            monkeypatch.setattr(cli, "current_iso", lambda iso=iso: iso)
            if iso == td.T2:
                shoko.post_status = None
                shoko.link_on_post = True
            runtime._run_daemon_cycle(deps, store, deps.settings)
        status = runtime.status()
        assert store.get_service_canary() == (500, "failed")
        assert shoko.posts_count() == 2
        assert status.waiting_files == 2
        assert not status.automatic_linking
        assert "initial link could not be verified" in status.next_action
        assert "Review advanced diagnostics" in status.next_action
        assert "retry safely" not in status.next_action
        page = DashboardApplication._status_page(status, "csrf-token", paused=False)
        assert "Paused</span>" in page
        assert "initial link could not be verified" in page

        restarted = cli._NormalServiceRuntime(bootstrap, deps.sonarr, deps.shoko, cli.StopState())
        assert restarted.status().next_action == status.next_action
        assert not restarted.status().automatic_linking

        # Startup cannot briefly advertise running before the completion callback.
        def observe_start(*_args: Any, **_kwargs: Any) -> None:
            assert not restarted.status().automatic_linking
            assert restarted.status().next_action == status.next_action

        monkeypatch.setattr(cli, "run_daemon", observe_start)
        restarted._run_daemon_cycle(deps, store, deps.settings)
        assert shoko.posts_count() == 2
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()


@pytest.mark.parametrize("hold", ["write_stop", "rebaseline", "pause"])
def test_durable_safety_hold_overrides_cached_active_status(tmp_path: Path, hold: str) -> None:
    state_path = tmp_path / "state.sqlite3"
    bootstrap = BootstrapSettings(
        td.SONARR_BASE,
        SecretRef("SONARR_API_KEY"),
        td.SHOKO_BASE,
        SecretRef("SHOKO_API_KEY"),
        state_path,
    )
    endpoint = ApiEndpoint(httpx.Client(), lambda: {})
    runtime = cli._NormalServiceRuntime(bootstrap, endpoint, endpoint, cli.StopState())
    runtime._update_latest(
        authorized=True, next_action="Automatic linking is processing eligible files."
    )
    try:
        with open_state(state_path) as store:
            if hold == "write_stop":
                store.disable_writes("test safety hold")
            elif hold == "rebaseline":
                store.set_meta("requires_rebaseline", "1")
            else:
                OnboardingManager(store).pause(iso=td.T0)
        status = runtime.status()
        assert not status.automatic_linking
        assert "processing eligible" not in status.next_action
        assert (
            "Resume" in status.next_action
            if hold == "pause"
            else "safety check" in status.next_action
        )
        if hold == "pause":
            restarted = cli._NormalServiceRuntime(bootstrap, endpoint, endpoint, cli.StopState())
            assert "Resume" in restarted.status().next_action
    finally:
        endpoint.client.close()


@pytest.mark.parametrize("reason", ["setup_changed", "unsafe_shoko_settings", "setup_unavailable"])
def test_system_pause_preserves_current_decision_and_restart_guidance(
    tmp_path: Path, reason: str
) -> None:
    state_path = tmp_path / "state.sqlite3"
    bootstrap = BootstrapSettings(
        td.SONARR_BASE,
        SecretRef("SONARR_API_KEY"),
        td.SHOKO_BASE,
        SecretRef("SHOKO_API_KEY"),
        state_path,
    )
    endpoint = ApiEndpoint(httpx.Client(), lambda: {})
    runtime = cli._NormalServiceRuntime(bootstrap, endpoint, endpoint, cli.StopState())
    assessment = assess_settings_payload({"FileQualityFilterEnabled": False})
    binding = ScopeBinding(
        td.SONARR_BASE,
        td.SHOKO_BASE,
        "4.0.0",
        "5.3.3",
        (),
        (td.SONARR_ROOT,),
        (PathMapping(td.SONARR_ROOT, td.SHOKO_ROOT),),
        assessment.digest,
        assessment.unverifiable_facts,
    )
    try:
        with open_state(state_path) as store:
            manager = OnboardingManager(store)
            manager.confirm_setup(
                binding,
                assessment,
                confirmed_facts=assessment.unverifiable_facts + assessment.unknown_settings,
                automatic_link_consent=True,
                iso=td.T0,
            )
            if reason == "unsafe_shoko_settings":
                decision = manager.evaluate(
                    binding,
                    assess_settings_payload({"FileQualityFilterEnabled": True}),
                    iso=td.T1,
                )
            else:
                decision = manager.pause_for_review(
                    reason,
                    "Confirm the changed scope and safety facts."
                    if reason == "setup_changed"
                    else "Cannot connect to Shoko. Check its address and API key.",
                    iso=td.T1,
                )
            assert store.get_service_pause() == (True, reason)
            runtime._update_latest(authorized=False, next_action=decision.message)
            assert runtime.status().next_action == decision.message
            assert not runtime.status().automatic_linking
            fresh = cli._NormalServiceRuntime(bootstrap, endpoint, endpoint, cli.StopState())
            assert "Resume" not in fresh.status().next_action
            if reason == "unsafe_shoko_settings":
                assert "fix Shoko safety settings" in fresh.status().next_action
            if reason != "setup_unavailable":
                assert manager.reconfirmation_required()
                assert manager.resume(binding, assessment, iso=td.T2).status.value != "authorized"
                # A subsequent user pause cannot turn a hard hold into a resumable pause.
                manager.pause(iso=td.T2)

        restarted = cli._NormalServiceRuntime(bootstrap, endpoint, endpoint, cli.StopState())
        status = restarted.status()
        assert not status.automatic_linking
        assert "Resume" not in status.next_action
        assert "setup" in status.next_action
        if reason != "setup_unavailable":
            assert "again" in status.next_action
    finally:
        endpoint.client.close()


@pytest.mark.parametrize("condition", ["suppressed", "failed", "pending", "healthy"])
def test_completed_cycle_projects_waits_and_recovers_when_healthy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, condition: str
) -> None:
    store = td.open_tmp_store(tmp_path)
    endpoint = ApiEndpoint(httpx.Client(), lambda: {})
    bootstrap = BootstrapSettings(
        td.SONARR_BASE,
        SecretRef("SONARR_API_KEY"),
        td.SHOKO_BASE,
        SecretRef("SHOKO_API_KEY"),
        tmp_path / "state.sqlite3",
    )
    runtime = cli._NormalServiceRuntime(bootstrap, endpoint, endpoint, cli.StopState())
    report = CycleReport(
        mode=CycleMode.APPLY,
        cycle_key="projection",
        alive=True,
        reconciliation_ready=True,
        apply_eligible=True,
        writes_suppressed=condition == "suppressed",
        mapping_outcome=None,
        mapping_revision=1,
        counts=CycleCounts(),
        reasons=(),
        drift=(),
        existing_link_drift=(),
        deferred_planned=0,
        failed=condition == "failed",
    )
    if condition == "pending":
        store.record_service_canary(500, td.T0)

    def complete_cycle(*_args: Any, **kwargs: Any) -> None:
        assert runtime.status().worker_activity == "Checking queued files and verifying links."
        kwargs["on_cycle_complete"](report)

    monkeypatch.setattr(cli, "run_daemon", complete_cycle)
    try:
        deps = cast(
            DaemonDeps,
            SimpleNamespace(mapping_store=MappingStore(tmp_path / "mappings"), overrides_text=None),
        )
        runtime._run_daemon_cycle(deps, store, td.make_settings(tmp_path))
        status = runtime.status()
        assert status.automatic_linking == (condition == "healthy")
        assert status.worker_activity == ""
        if condition == "pending":
            assert "Waiting for the initial link" in status.next_action
            store.set_service_canary_status("verified", td.T1)
        elif condition != "healthy":
            assert "retry safely" in status.next_action
        report = replace(report, failed=False, writes_suppressed=False)
        runtime._run_daemon_cycle(deps, store, td.make_settings(tmp_path))
        assert runtime.status().automatic_linking
        assert "next scheduled check" in runtime.status().next_action
    finally:
        endpoint.client.close()
        store.close()


@pytest.mark.parametrize("cached", [False, True])
@pytest.mark.parametrize(
    ("failure", "reason", "wording", "invalid_body"),
    [
        (
            RefreshOutcome.FAILED_INVALID,
            "mapping_source_invalid",
            "failed validation",
            b"<secret-server-response",
        ),
        (
            RefreshOutcome.FAILED_INVALID,
            "mapping_source_invalid",
            "failed validation",
            b"<wrong-root/>",
        ),
        (
            RefreshOutcome.FAILED_TRANSPORT,
            "mapping_source_transport_failure",
            "Could not download",
            b"",
        ),
        (RefreshOutcome.FAILED_SIZE, "mapping_source_too_large", "safe size limit", b""),
    ],
)
def test_mapping_download_failure_guidance_survives_backoff_and_clears_after_activation(  # noqa: PLR0915 - full refresh/status/retry integration
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    cached: bool,
    failure: RefreshOutcome,
    reason: str,
    wording: str,
    invalid_body: bytes,
) -> None:
    if cached:
        td.activate_mapping(tmp_path, td.mapping_xml())
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [])
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[],
        shoko_files=[],
        xml=td.mapping_xml(),
        monotonic=lambda: 0.0,
        advance=lambda _: None,
        max_submissions=0,
    )
    requests: list[str] = []
    recovered = False

    def source(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        if recovered:
            return httpx.Response(
                200, stream=httpx.ByteStream(td.mapping_xml().encode()), request=request
            )
        if failure is RefreshOutcome.FAILED_INVALID:
            return httpx.Response(200, stream=httpx.ByteStream(invalid_body), request=request)
        if failure is RefreshOutcome.FAILED_SIZE:
            return httpx.Response(200, stream=httpx.ByteStream(b"x" * 10_000_001), request=request)
        return httpx.Response(503, text="secret-server-response", request=request)

    deps.mapping_source.client.close()
    deps = replace(
        deps,
        mapping_source=td.source_endpoint(source),
        service_mode=True,
        service_authorized=lambda: True,
    )
    bootstrap = BootstrapSettings(
        td.SONARR_BASE,
        SecretRef("SONARR_API_KEY"),
        td.SHOKO_BASE,
        SecretRef("SHOKO_API_KEY"),
        tmp_path / "state.sqlite3",
    )
    runtime = cli._NormalServiceRuntime(bootstrap, deps.sonarr, deps.shoko, cli.StopState())
    reports: list[CycleReport] = []
    real_daemon = run_daemon

    def capture(*args: Any, **kwargs: Any) -> Any:
        completed = kwargs["on_cycle_complete"]

        def record(report: CycleReport) -> None:
            reports.append(report)
            completed(report)

        kwargs["on_cycle_complete"] = record
        return real_daemon(*args, **kwargs)

    monkeypatch.setattr(cli, "run_daemon", capture)
    now = td._parse(td._due_iso() if cached else td.T0)
    try:
        monkeypatch.setattr(cli, "current_iso", lambda: now.isoformat())
        runtime._run_daemon_cycle(deps, store, deps.settings)
        failed_status = runtime.status()
        durable = mapping_status(deps.mapping_store, now)
        assert durable.last_outcome is failure
        assert durable.active_present is cached
        assert wording in failed_status.mapping_cache_status
        assert "Next mapping check:" in failed_status.mapping_cache_status
        assert "secret-server-response" not in str(failed_status)
        assert td.SOURCE_URL not in str(failed_status)
        if not cached or failure is not RefreshOutcome.FAILED_SIZE:
            assert reason in reports[-1].reasons
            assert wording in failed_status.next_action
            assert not failed_status.automatic_linking
        if failure is not RefreshOutcome.FAILED_TRANSPORT:
            assert "mapping_source_transport_failure" not in reports[-1].reasons
            assert not reports[-1].failed
        _, unrelated_action = runtime._operational_status(
            store,
            authorized=True,
            next_action="Ready",
            report=replace(
                reports[-1],
                writes_suppressed=True,
                reasons=("shoko_snapshot_incomplete:transport_failure",),
            ),
            mapping_action=failed_status.mapping_cache_status,
        )
        assert "A service check needs attention" in unrelated_action
        assert wording not in unrelated_action
        call_count = len(requests)
        now += timedelta(seconds=1)
        runtime._run_daemon_cycle(deps, store, deps.settings)
        assert reports[-1].mapping_outcome is RefreshOutcome.NOT_DUE
        assert len(requests) == call_count
        assert wording in runtime.status().mapping_cache_status
        assert durable.next_due_iso is not None
        expected_time = td._parse(durable.next_due_iso).strftime("%Y-%m-%d %H:%M:%S UTC")
        assert expected_time in runtime.status().mapping_cache_status
        assert all(report.counts.submitted == 0 for report in reports)
        recovered = True
        now = td._parse(durable.next_due_iso)
        runtime._run_daemon_cycle(deps, store, deps.settings)
        healthy = runtime.status()
        assert reports[-1].mapping_outcome is (
            RefreshOutcome.UNCHANGED if cached else RefreshOutcome.ACTIVATED
        )
        assert "Verified episode mappings are available" in healthy.mapping_cache_status
        assert wording not in healthy.mapping_cache_status
        assert healthy.automatic_linking
        assert "next scheduled check" in healthy.next_action
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()


def test_empty_mapping_cache_guidance_does_not_invent_download_activity(tmp_path: Path) -> None:
    now = td._parse(td.T0)
    view = mapping_status(MappingStore(tmp_path / "mappings"), now)
    message = cli._NormalServiceRuntime._mapping_guidance(view, now)
    assert "No verified episode mappings are cached yet" in message
    assert "Linking will wait" in message
    assert "will check again automatically" in message
    assert "Downloading" not in message
    assert "failed" not in message
    assert view.source_url not in message


@pytest.mark.parametrize("failure_stage", ["persist", "mapping_status", "guidance", "publish"])
def test_swallowed_completion_failure_defers_status_and_next_real_cycle_recovers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    failure_stage: str,
) -> None:
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [])
    shoko = td.ShokoStub([])
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[],
        shoko_files=[],
        xml=td.mapping_xml(),
        monotonic=lambda: 0.0,
        advance=lambda _: None,
        max_submissions=0,
        shoko=shoko,
    )
    deps = replace(deps, service_mode=True, service_authorized=lambda: True)
    bootstrap = BootstrapSettings(
        td.SONARR_BASE,
        SecretRef("SONARR_API_KEY"),
        td.SHOKO_BASE,
        SecretRef("SHOKO_API_KEY"),
        tmp_path / "state.sqlite3",
    )
    runtime = cli._NormalServiceRuntime(bootstrap, deps.sonarr, deps.shoko, cli.StopState())
    monkeypatch.setattr(cli, "current_iso", lambda: td.T0)

    def fail(*_args: Any, **_kwargs: Any) -> None:
        raise OSError("mock completion failure")

    update_latest = runtime._update_latest

    def fail_final_publication(**changes: Any) -> None:
        if "mapping_cache" in changes:
            fail()
        update_latest(**changes)

    try:
        with monkeypatch.context() as injection:
            if failure_stage == "persist":
                injection.setattr(cli, "persist_cycle_meta", fail)
            elif failure_stage == "mapping_status":
                injection.setattr(cli, "mapping_status", fail)
            elif failure_stage == "guidance":
                injection.setattr(runtime, "_mapping_guidance", fail)
            else:
                injection.setattr(runtime, "_update_latest", fail_final_publication)
            # Use the real daemon: it logs and swallows completion callback errors.
            runtime._run_daemon_cycle(deps, store, deps.settings)
        assert "cycle completion callback failed" in caplog.text
        failed = runtime.status()
        assert not failed.automatic_linking
        assert failed.worker_activity == ""
        assert "retry safely" in failed.next_action
        assert "running" not in failed.next_action
        monkeypatch.setattr(cli, "current_iso", lambda: td.T1)
        runtime._run_daemon_cycle(deps, store, deps.settings)
        recovered = runtime.status()
        assert recovered.automatic_linking
        assert recovered.worker_activity == ""
        assert "next scheduled check" in recovered.next_action
        assert "Verified episode mappings are available" in recovered.mapping_cache_status
        assert shoko.posts == []
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()


def test_dashboard_status_reads_persisted_categorized_exclusion_counts(tmp_path: Path) -> None:
    state_path = tmp_path / "state.sqlite3"
    with open_state(state_path) as store:
        store.set_meta(
            META_LAST_DISCOVERY_EXCLUSIONS,
            json.dumps(
                {
                    "version": 1,
                    "total": 3,
                    "counts": {"size_mismatch": 2, "outside_allowed_root": 1},
                    "scope_excluded": 1,
                    "examples": [
                        {
                            "shoko_file_id": 501,
                            "reason": "size_mismatch",
                            "detail": "Sonarr and Shoko byte sizes differ",
                        }
                    ],
                }
            ),
        )
    bootstrap = BootstrapSettings(
        "http://sonarr:8989",
        SecretRef("SONARR_API_KEY"),
        "http://shoko:8111",
        SecretRef("SHOKO_API_KEY"),
        state_path,
    )
    sonarr = ApiEndpoint(httpx.Client(), lambda: {})
    shoko = ApiEndpoint(httpx.Client(), lambda: {})
    runtime = cli._NormalServiceRuntime(bootstrap, sonarr, shoko, cli.StopState())
    try:
        status = runtime.status()
    finally:
        sonarr.client.close()
        shoko.client.close()

    assert status.excluded_files == 3
    assert status.scope_excluded_files == 1
    assert status.exclusion_counts == (("outside_allowed_root", 1), ("size_mismatch", 2))
    assert any("Shoko file 501: size_mismatch" in detail for detail in status.diagnostics)


def test_status_surfaces_classify_all_persisted_file_states_after_restart_without_remote_io(  # noqa: PLR0915 - full status integration
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    state_path = tmp_path / "state.sqlite3"
    limits = BudgetLimits(
        max_pending_files=20,
        max_pending_episode_jobs=20,
        per_cycle_new=20,
        rolling_new=20,
        rolling_window_hours=24.0,
    )
    now = "2026-10-15T00:00:00+00:00"
    plan_fingerprint = "a" * 64
    targets_digest = "b" * 64
    structure_digest = "c" * 64

    def add_file(store: StateStore, file_id: int, state: FileState) -> None:
        store.record_file(
            file_id,
            sonarr_series_id=1,
            sonarr_file_id=file_id,
            sonarr_path=f"/anime/{file_id}.mkv",
            sonarr_size=1024,
            cohort=Cohort.NEW_ARRIVAL,
            iso=now,
            state=state,
        )

    def prepare_send(store: StateStore, file_id: int) -> SendToken:
        token = store.prepare_send(
            file_id,
            plan_fingerprint,
            f"{file_id:064x}",
            targets_digest=targets_digest,
            structure_digest=structure_digest,
            mapping_revision="1",
            cycle_key=f"status-{file_id}",
            iso=now,
        )
        assert isinstance(token, SendToken)
        return token

    def start_accepted_send(store: StateStore, file_id: int) -> None:
        add_file(store, file_id, FileState.PLANNED)
        token = prepare_send(store, file_id)
        store.mark_sent(token, iso=now)
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=now, http_status=200)

    verifying_only_path = tmp_path / "verifying-only.sqlite3"
    with open_state(verifying_only_path, budget_limits=limits) as store:
        start_accepted_send(store, 92001)
        assert store.get_file(92001).state is FileState.VERIFYING  # type: ignore[union-attr]
        assert store.budget_view().pending_file_requests == 1

    review_only_path = tmp_path / "review-only.sqlite3"
    with open_state(review_only_path, budget_limits=limits) as store:
        start_accepted_send(store, 92002)
        store.record_verification(92002, VerificationVerdict.PARTIAL, iso=now)
        add_file(store, 92003, FileState.PLANNED)
        ambiguous = prepare_send(store, 92003)
        store.mark_sent(ambiguous, iso=now)
        store.record_send_outcome(ambiguous, SendOutcome.AMBIGUOUS_SEND, iso=now)

    with open_state(state_path, budget_limits=limits) as store:
        for file_id, state in (
            (91001, FileState.DISCOVERED),
            (91002, FileState.MATCHED),
            (91003, FileState.MAPPED),
            (91004, FileState.PLANNED),
            (91008, FileState.WAITING),
            (91009, FileState.AMBIGUOUS),
            (91010, FileState.CONFLICT),
            (91013, FileState.BLOCKED_SAFETY),
        ):
            add_file(store, file_id, state)

        # A definitely-unsent prepared record is still visible as pending work.
        add_file(store, 91005, FileState.PLANNED)
        prepare_send(store, 91005)

        # A marked request is uncertain but has a durable verification/hold path,
        # so it belongs with waiting work rather than operator review.
        add_file(store, 91006, FileState.PLANNED)
        marked = prepare_send(store, 91006)
        store.mark_sent(marked, iso=now)

        start_accepted_send(store, 91007)
        assert store.get_file(91007).state is FileState.VERIFYING  # type: ignore[union-attr]
        assert store.budget_view().pending_file_requests == 3

        start_accepted_send(store, 91011)
        store.record_verification(91011, VerificationVerdict.PARTIAL, iso=now)
        add_file(store, 91012, FileState.PLANNED)
        ambiguous = prepare_send(store, 91012)
        store.mark_sent(ambiguous, iso=now)
        store.record_send_outcome(ambiguous, SendOutcome.AMBIGUOUS_SEND, iso=now)

        start_accepted_send(store, 91014)
        store.record_verification(91014, VerificationVerdict.VERIFIED, iso=now)
        assert store.budget_view().pending_file_requests == 5

    requests: list[str] = []

    def reject_remote_request(request: httpx.Request) -> httpx.Response:
        requests.append(str(request.url))
        raise AssertionError("ordinary status must not make remote requests")

    def headers() -> dict[str, str]:
        return {"X-Api-Key": "STATUS_API_KEY_SENTINEL"}

    sonarr = ApiEndpoint(
        httpx.Client(transport=httpx.MockTransport(reject_remote_request)), headers
    )
    shoko = ApiEndpoint(httpx.Client(transport=httpx.MockTransport(reject_remote_request)), headers)
    bootstrap = BootstrapSettings(
        "http://sonarr:8989",
        SecretRef("SONARR_API_KEY"),
        "http://shoko:8111",
        SecretRef("SHOKO_API_KEY"),
        state_path,
    )
    runtime = cli._NormalServiceRuntime(bootstrap, sonarr, shoko, cli.StopState())
    try:
        verifying_runtime = cli._NormalServiceRuntime(
            replace(bootstrap, database_path=verifying_only_path),
            sonarr,
            shoko,
            cli.StopState(),
        )
        verifying_status = verifying_runtime.status()
        verifying_page = DashboardApplication._status_page(
            verifying_status, "csrf-token", paused=False
        )
        assert verifying_status.linked_files == 0
        assert verifying_status.waiting_files == 1
        assert verifying_status.review_files == 0
        assert "<dt>Waiting</dt><dd>1</dd>" in verifying_page

        review_runtime = cli._NormalServiceRuntime(
            replace(bootstrap, database_path=review_only_path), sonarr, shoko, cli.StopState()
        )
        review_status = review_runtime.status()
        assert review_status.linked_files == 0
        assert review_status.waiting_files == 0
        assert review_status.review_files == 2

        monkeypatch.setattr(cli, "DEFAULT_DATABASE_PATH", verifying_only_path)
        assert cli.main(["status"]) == cli.EXIT_OK
        verifying_cli_status = capsys.readouterr().out
        assert "Verified files: 0" in verifying_cli_status
        assert "Waiting files: 1" in verifying_cli_status
        assert "Files needing review: 0" in verifying_cli_status

        monkeypatch.setattr(cli, "DEFAULT_DATABASE_PATH", state_path)
        dashboard_status = runtime.status()
        dashboard_page = DashboardApplication._status_page(
            dashboard_status, "csrf-token", paused=False
        )
        assert dashboard_status.linked_files == 1
        assert dashboard_status.waiting_files == 8
        assert dashboard_status.review_files == 5
        assert "waiting for shoko to confirm a link" in dashboard_page.lower()
        assert "partial or uncertain link results need attention" in dashboard_page.lower()
        assert "910" not in dashboard_page
        assert "STATUS_API_KEY_SENTINEL" not in dashboard_page

        assert cli.main(["status"]) == cli.EXIT_OK
        cli_status = capsys.readouterr().out
        assert "Verified files: 1" in cli_status
        assert "Waiting files: 8" in cli_status
        assert "Files needing review: 5" in cli_status
        assert "waiting for shoko to confirm a link" in cli_status.lower()
        assert "partial or uncertain link results need attention" in cli_status.lower()
        assert "910" not in cli_status
        assert "STATUS_API_KEY_SENTINEL" not in cli_status
        assert requests == []
    finally:
        sonarr.client.close()
        shoko.client.close()


def test_worker_runs_service_policy_only_with_current_durable_consent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assessment = assess_settings_payload(
        {
            "FileQualityFilterEnabled": False,
            "Plugins": {
                "Renamer": {
                    "RenameOnImport": False,
                    "MoveOnImport": False,
                    "RelocateOnImport": False,
                }
            },
            "AniDb": {"MyList_AddFiles": False},
        }
    )
    bootstrap = BootstrapSettings(
        "http://sonarr:8989",
        SecretRef("SONARR_API_KEY"),
        "http://shoko:8111",
        SecretRef("SHOKO_API_KEY"),
        cast(Path, PurePosixPath("/data/state.sqlite3")),
    )
    binding = ScopeBinding(
        bootstrap.sonarr_url,
        bootstrap.shoko_url,
        "4.0.0",
        "5.3.3",
        ("sonarr_api_v3_system_status", "shoko_api_v3_init_version"),
        ("/data/media/anime",),
        (PathMapping("/data/media/anime", "/anime"),),
        assessment.digest,
        assessment.unverifiable_facts,
    )
    state_path = tmp_path / "state.sqlite3"
    with open_state(state_path) as store:
        OnboardingManager(store).confirm_setup(
            binding,
            assessment,
            confirmed_facts=assessment.unverifiable_facts,
            automatic_link_consent=True,
            iso="2026-10-15T00:00:00Z",
        )

    runtime_settings = load_settings(EXAMPLE_CONFIG)
    sonarr = ApiEndpoint(httpx.Client(), lambda: {})
    shoko = ApiEndpoint(httpx.Client(), lambda: {})
    mapping_source = ApiEndpoint(httpx.Client(), lambda: {})
    monkeypatch.setattr(cli, "probe_connections", lambda *_: ConnectionReport(True, True, ()))
    monkeypatch.setattr(cli, "settings_for_scope", lambda *_args, **_kwargs: runtime_settings)
    monkeypatch.setattr(cli, "open_mapping_store", lambda _settings: object())
    monkeypatch.setattr(cli, "read_overrides_text", lambda _settings: None)

    def build_dependencies(
        settings: Settings,
        *,
        store: StateStore,
        mapping_store: Any,
        **_kwargs: Any,
    ) -> DaemonDeps:
        return DaemonDeps(
            settings,
            store,
            cast(MappingStore, mapping_store),
            sonarr,
            shoko,
            mapping_source,
        )

    monkeypatch.setattr(cli, "build_daemon_deps", build_dependencies)
    captured: dict[str, Any] = {}

    def run_one_cycle(deps: DaemonDeps, **kwargs: Any) -> object:
        # The real daemon binds production authorization at its cycle boundary.
        deps = _bind_service_authorization(deps, deps.monotonic() + deps.max_cycle_seconds)
        captured.update(kwargs, deps=deps)
        captured["authorized_initially"] = deps.service_authorized()
        current_setup.binding = replace(
            binding,
            roots=("/other-anime",),
            path_mappings=(PathMapping("/other-anime", "/other-shoko"),),
        )
        OnboardingManager(deps.store).confirm_setup(
            current_setup.binding,
            assessment,
            confirmed_facts=assessment.unverifiable_facts,
            automatic_link_consent=True,
            iso="2026-10-15T00:00:00Z",
        )
        captured["local_after_scope_change"] = _service_authorized_local(deps)
        captured["authorized_after_scope_change"] = deps.service_authorized()
        current_setup.available = False
        captured["authorized_after_revocation"] = deps.service_authorized()
        return object()

    monkeypatch.setattr(cli, "run_daemon", run_one_cycle)
    runtime = cli._NormalServiceRuntime(
        replace(bootstrap, database_path=state_path),
        sonarr,
        shoko,
        cli.StopState(),
    )

    class CurrentSetup:
        available = True
        binding: ScopeBinding

        def current_context(self, _store: StateStore, **_kwargs: Any) -> OnboardingContext | None:
            return (
                OnboardingContext(self.binding, assessment, _store.get_onboarding_json() or "")
                if self.available
                else None
            )

    current_setup = CurrentSetup()
    current_setup.binding = binding
    runtime.setup = cast(Any, current_setup)
    try:
        runtime.worker_tick()
        assert captured["policy"] is RunPolicy.SERVICE
        assert captured["mode"] is CycleMode.APPLY
        assert captured["stop_after_cycles"] == 1
        deps = cast(DaemonDeps, captured["deps"])
        assert deps.service_mode is True
        assert captured["authorized_initially"] is True
        assert captured["local_after_scope_change"] is False
        assert captured["authorized_after_scope_change"] is False
        assert captured["authorized_after_revocation"] is False
        _assert_scope_consent_is_not_invalidated(state_path, ("/other-anime",))
    finally:
        sonarr.client.close()
        shoko.client.close()
        mapping_source.client.close()


def test_different_scope_confirmed_during_preflight_cannot_authorize_old_cycle(
    tmp_path: Path,
) -> None:
    assessment = assess_settings_payload({"FileQualityFilterEnabled": False})
    bootstrap = BootstrapSettings(
        td.SONARR_BASE,
        SecretRef("SONARR_API_KEY"),
        td.SHOKO_BASE,
        SecretRef("SHOKO_API_KEY"),
        tmp_path / "state.sqlite3",
    )
    original = ScopeBinding(
        td.SONARR_BASE,
        td.SHOKO_BASE,
        "4.0.0",
        "5.3.3",
        (),
        (td.SONARR_ROOT,),
        (PathMapping(td.SONARR_ROOT, td.SHOKO_ROOT),),
        assessment.digest,
        assessment.unverifiable_facts,
    )
    changed = replace(
        original,
        roots=("/other-anime",),
        path_mappings=(PathMapping("/other-anime", "/other-shoko"),),
    )
    current = {"binding": original}
    file = td.SpecFile(series_id=1, file_id=10, shoko_id=500)
    td.activate_mapping(tmp_path, td.mapping_xml())
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [file])
    shoko = td.ShokoStub([file])
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=td.mapping_xml(),
        monotonic=td.Clock([0.0]),
        advance=lambda _: None,
        shoko=shoko,
    )
    manager = OnboardingManager(store)

    def confirm(binding: ScopeBinding) -> None:
        manager.confirm_setup(
            binding,
            assessment,
            confirmed_facts=assessment.unverifiable_facts + assessment.unknown_settings,
            automatic_link_consent=True,
            iso=td.T0,
        )

    class CurrentSetup:
        def current_context(self, _store: StateStore) -> OnboardingContext:
            return OnboardingContext(
                current["binding"], assessment, _store.get_onboarding_json() or ""
            )

    runtime = cli._NormalServiceRuntime(bootstrap, deps.sonarr, deps.shoko, cli.StopState())
    runtime.setup = cast(Any, CurrentSetup())
    confirm(original)

    def change_during_preflight(path: str) -> None:
        if path.endswith("/File/500"):
            confirm(changed)
            current["binding"] = changed

    shoko.on_get = change_during_preflight
    deps = replace(
        deps,
        service_mode=True,
        service_authorized=lambda: runtime._service_authorized(store, manager, original.digest),
        service_authorized_local=lambda: runtime._service_authorized(
            store, manager, original.digest
        ),
    )
    try:
        report = run_cycle(
            deps,
            now_iso=td.T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="scope-change",
        )
        assert current["binding"] == changed
        assert report.counts.submitted == 0
        assert "service_authorization_changed" in report.reasons
        assert shoko.posts == []
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()


def test_service_durable_stop_from_second_handle_during_final_file_get_prevents_send(
    tmp_path: Path,
) -> None:
    assessment = assess_settings_payload({"FileQualityFilterEnabled": False})
    state_path = tmp_path / "state.sqlite3"
    bootstrap = BootstrapSettings(
        td.SONARR_BASE,
        SecretRef("SONARR_API_KEY"),
        td.SHOKO_BASE,
        SecretRef("SHOKO_API_KEY"),
        state_path,
    )
    binding = ScopeBinding(
        td.SONARR_BASE,
        td.SHOKO_BASE,
        "4.0.0",
        "5.3.3",
        (),
        (td.SONARR_ROOT,),
        (PathMapping(td.SONARR_ROOT, td.SHOKO_ROOT),),
        assessment.digest,
        assessment.unverifiable_facts,
    )
    file = td.SpecFile(series_id=1, file_id=10, shoko_id=500)
    td.activate_mapping(tmp_path, td.mapping_xml())
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [td.SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = td.ShokoStub([file])
    manager = OnboardingManager(store)
    manager.confirm_setup(
        binding,
        assessment,
        confirmed_facts=assessment.unverifiable_facts + assessment.unknown_settings,
        automatic_link_consent=True,
        iso=td.T0,
    )
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=td.mapping_xml(),
        monotonic=td.Clock([0.0]),
        advance=lambda _: None,
        shoko=shoko,
    )
    runtime = cli._NormalServiceRuntime(bootstrap, deps.sonarr, deps.shoko, cli.StopState())

    class CurrentSetup:
        def current_context(self, _store: StateStore) -> OnboardingContext:
            return OnboardingContext(binding, assessment, _store.get_onboarding_json() or "")

    runtime.setup = cast(Any, CurrentSetup())
    stop_sent = False

    def stop_during_final_file_read(path: str) -> None:
        nonlocal stop_sent
        if path == "/api/v3/File/500" and not stop_sent:
            stop_sent = True
            with open_state(state_path) as stopper:
                stopper.disable_writes("external storage safety stop")

    shoko.on_get = stop_during_final_file_read
    deps = replace(
        deps,
        service_mode=True,
        service_authorized=lambda: runtime._service_authorized(store, manager, binding.digest),
        service_authorized_local=lambda: runtime._service_authorized(
            store, manager, binding.digest
        ),
    )
    try:
        report = run_cycle(
            deps,
            now_iso=td.T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="durable-stop-during-final-get",
        )
        assert report.counts.submitted == 0
        assert "writes_disabled" in report.reasons
        assert store.writes_disabled is True
        assert shoko.posts == []
        assert store.get_file(file.shoko_id).state is FileState.PLANNED  # type: ignore[union-attr]
        assert (
            store.connection.execute(
                "SELECT COUNT(*) FROM sends WHERE shoko_file_id = ?", (file.shoko_id,)
            ).fetchone()[0]
            == 0
        )
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()


def test_real_service_authorization_callback_cannot_mask_current_non_anime_series(
    tmp_path: Path,
) -> None:
    assessment = assess_settings_payload({"FileQualityFilterEnabled": False})
    bootstrap = BootstrapSettings(
        td.SONARR_BASE,
        SecretRef("SONARR_API_KEY"),
        td.SHOKO_BASE,
        SecretRef("SHOKO_API_KEY"),
        tmp_path / "state.sqlite3",
    )
    binding = ScopeBinding(
        td.SONARR_BASE,
        td.SHOKO_BASE,
        "4.0.0",
        "5.3.3",
        (),
        (td.SONARR_ROOT,),
        (PathMapping(td.SONARR_ROOT, td.SHOKO_ROOT),),
        assessment.digest,
        assessment.unverifiable_facts,
    )
    file = td.SpecFile(series_id=1, file_id=10, shoko_id=500)

    class MutableSeriesSonarrStub(td.SonarrStub):
        candidate_is_anime = True

        def _series(self) -> list[dict[str, Any]]:
            rows = super()._series()
            rows[0]["seriesType"] = "anime" if self.candidate_is_anime else "standard"
            rows.append(
                {
                    "id": 2,
                    "title": "Unrelated anime",
                    "rootFolderPath": f"{td.SONARR_ROOT}/other-anime",
                    "seriesType": "anime",
                    "tvdbId": 999_999,
                }
            )
            return rows

    sonarr = MutableSeriesSonarrStub([file])
    td.activate_mapping(tmp_path, td.mapping_xml())
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [td.SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = td.ShokoStub([file])
    manager = OnboardingManager(store)
    manager.confirm_setup(
        binding,
        assessment,
        confirmed_facts=assessment.unverifiable_facts + assessment.unknown_settings,
        automatic_link_consent=True,
        iso=td.T0,
    )

    class CurrentSetup:
        def current_context(self, _store: StateStore) -> OnboardingContext:
            return OnboardingContext(binding, assessment, _store.get_onboarding_json() or "")

    def change_candidate_type_during_target_resolution(path: str) -> None:
        if path.startswith("/api/v3/Series/AniDB/"):
            sonarr.candidate_is_anime = False

    shoko.on_get = change_candidate_type_during_target_resolution
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=td.mapping_xml(),
        monotonic=td.Clock([0.0]),
        advance=lambda _: None,
        shoko=shoko,
        sonarr_stub=sonarr,
    )
    runtime = cli._NormalServiceRuntime(bootstrap, deps.sonarr, deps.shoko, cli.StopState())
    runtime.setup = cast(Any, CurrentSetup())
    deps = replace(
        deps,
        service_mode=True,
        service_authorized=lambda: runtime._service_authorized(store, manager, binding.digest),
        service_authorized_local=lambda: runtime._service_authorized(
            store, manager, binding.digest
        ),
    )
    try:
        report = run_cycle(
            deps,
            now_iso=td.T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="current-series-type-drift",
        )
        record = store.get_file(file.shoko_id)
        quarantined = store.mapping_target_quarantined(file.shoko_id)
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()

    assert report.counts.submitted == 0
    assert shoko.posts == []
    assert record is not None
    assert record.state.value == "conflict"
    assert quarantined
    assert "service_authorization_changed" not in report.reasons


def _confirmed_activity_runtime(
    tmp_path: Path,
) -> tuple[cli._NormalServiceRuntime, ApiEndpoint, OnboardingContext]:
    state_path = tmp_path / "state.sqlite3"
    bootstrap = BootstrapSettings(
        td.SONARR_BASE,
        SecretRef("SONARR_API_KEY"),
        td.SHOKO_BASE,
        SecretRef("SHOKO_API_KEY"),
        state_path,
    )
    endpoint = ApiEndpoint(httpx.Client(), lambda: {})
    runtime = cli._NormalServiceRuntime(bootstrap, endpoint, endpoint, cli.StopState())
    assessment = assess_settings_payload({"FileQualityFilterEnabled": False})
    binding = ScopeBinding(
        td.SONARR_BASE,
        td.SHOKO_BASE,
        "4.0.0",
        "5.3.3",
        (),
        (td.SONARR_ROOT,),
        (PathMapping(td.SONARR_ROOT, td.SHOKO_ROOT),),
        assessment.digest,
        assessment.unverifiable_facts,
    )
    with open_state(state_path) as store:
        OnboardingManager(store).confirm_setup(
            binding,
            assessment,
            confirmed_facts=assessment.unverifiable_facts + assessment.unknown_settings,
            automatic_link_consent=True,
            iso=td.T0,
        )
        context = OnboardingContext(binding, assessment, store.get_onboarding_json() or "")
    return runtime, endpoint, context


@pytest.mark.parametrize("stage", ["connection", "context", "dependencies"])
@pytest.mark.parametrize("failure", [False, True])
def test_worker_publishes_activity_before_blocking_checks_and_clears_on_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stage: str, failure: bool
) -> None:
    runtime, endpoint, approved_context = _confirmed_activity_runtime(tmp_path)
    state_path = tmp_path / "state.sqlite3"
    entered = threading.Event()
    release = threading.Event()

    def block() -> None:
        entered.set()
        assert release.wait(5), "test did not release the worker"
        if failure:
            raise OSError("offline test failure")

    def probe(*_args: Any) -> ConnectionReport:
        if stage == "connection":
            block()
            return ConnectionReport(False, False, ("Cannot connect to Shoko.",))
        return ConnectionReport(True, True, ())

    def context(store: StateStore) -> OnboardingContext | None:
        if stage == "context":
            block()
            return None
        return approved_context

    def dependencies(*_args: Any, **_kwargs: Any) -> DaemonDeps:
        block()
        # End the mock preparation without starting a daemon or making remote writes.
        raise ValueError("mock preparation complete")

    monkeypatch.setattr(cli, "configured_bootstrap", lambda _: runtime.settings)
    monkeypatch.setattr(cli, "probe_connections", probe)
    monkeypatch.setattr(runtime.setup, "current_context", context)
    monkeypatch.setattr(cli, "open_mapping_store", lambda _: object())
    monkeypatch.setattr(cli, "read_overrides_text", lambda _: None)
    monkeypatch.setattr(cli, "build_daemon_deps", dependencies)
    worker = threading.Thread(target=runtime.worker_tick)
    worker.start()
    try:
        assert entered.wait(5), "worker did not reach the blocking stage"
        status = runtime.status()
        expected = {
            "connection": "Checking Sonarr and Shoko connections.",
            "context": "Checking the confirmed library, mappings and Shoko safety.",
            "dependencies": "Preparing episode mappings and automatic processing.",
        }[stage]
        assert status.worker_activity == expected
        assert status.next_action == expected
        assert not status.automatic_linking
        # A user pause remains actionable even while read-only checks finish.
        with open_state(state_path) as store:
            OnboardingManager(store).pause(iso=td.T1)
        paused_status = runtime.status()
        assert "Resume" in paused_status.next_action
        assert not paused_status.automatic_linking
        assert paused_status.worker_activity == expected
    finally:
        release.set()
        worker.join(5)
        endpoint.client.close()
    assert not worker.is_alive()
    assert runtime.status().worker_activity == ""
    assert "Resume" in runtime.status().next_action
    assert not runtime.status().automatic_linking
    with open_state(state_path) as store:
        assert store.get_service_pause() == (True, "user_paused")


@pytest.mark.parametrize("failure", [False, True])
def test_cycle_exit_without_completion_never_leaves_active_processing_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: bool
) -> None:
    runtime, endpoint, _ = _confirmed_activity_runtime(tmp_path)

    def interrupted(*_args: Any, **_kwargs: Any) -> None:
        assert runtime.status().worker_activity == "Checking queued files and verifying links."
        if failure:
            raise OSError("mock interrupted cycle")

    monkeypatch.setattr(cli, "run_daemon", interrupted)
    try:
        with open_state(tmp_path / "state.sqlite3") as store:
            if failure:
                with pytest.raises(OSError, match="mock interrupted"):
                    runtime._run_daemon_cycle(
                        cast(DaemonDeps, object()), store, td.make_settings(tmp_path)
                    )
            else:
                runtime._run_daemon_cycle(
                    cast(DaemonDeps, object()), store, td.make_settings(tmp_path)
                )
        status = runtime.status()
        assert status.worker_activity == ""
        assert not status.automatic_linking
        assert "retry safely" in status.next_action
    finally:
        endpoint.client.close()
