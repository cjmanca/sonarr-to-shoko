"""Managed service runtime gates cycles on current durable onboarding consent."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path, PurePosixPath
from typing import Any, cast

import httpx
import pytest
import test_daemon as td

from shoko_linker import cli
from shoko_linker.bootstrap import BootstrapSettings
from shoko_linker.config import SecretRef, Settings, load_settings
from shoko_linker.daemon import CycleMode, DaemonDeps, run_cycle
from shoko_linker.dashboard import DashboardApplication
from shoko_linker.http import ApiEndpoint
from shoko_linker.linking import SendOutcome, VerificationVerdict
from shoko_linker.mapping_store import MappingStore
from shoko_linker.onboarding import OnboardingManager, ScopeBinding
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
        captured["deps"] = deps
        captured.update(kwargs)
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

        def current_context(self, _store: StateStore) -> tuple[ScopeBinding, Any] | None:
            return (self.binding, assessment) if self.available else None

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
        def current_context(self, _store: StateStore) -> tuple[ScopeBinding, Any]:
            return current["binding"], assessment

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
        def current_context(self, _store: StateStore) -> tuple[ScopeBinding, Any]:
            return binding, assessment

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
        def current_context(self, _store: StateStore) -> tuple[ScopeBinding, Any]:
            return binding, assessment

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
