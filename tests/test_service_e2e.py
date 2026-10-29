"""Fresh-install service onboarding, canary linking, and restart recovery via MockTransport."""

from __future__ import annotations

import hashlib
import re
import threading
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlencode

import e2e_harness as eh
import httpx
import pytest

from shoko_linker import cli, service_setup
from shoko_linker.bootstrap import (
    SHOKO_API_KEY_ENV,
    SHOKO_URL_ENV,
    SONARR_API_KEY_ENV,
    SONARR_URL_ENV,
    load_bootstrap_settings,
)
from shoko_linker.dashboard import DashboardApplication, DashboardResponse
from shoko_linker.http import ApiEndpoint
from shoko_linker.onboarding import OnboardingContext, OnboardingManager
from shoko_linker.paths import PathMapping
from shoko_linker.service_setup import ServiceSetupController
from shoko_linker.state import FileState, open_state

SONARR_ROOT = eh.SONARR_ROOT
SHOKO_ROOT = eh.SHOKO_ROOT
DASHBOARD_HOST = "linker.test"


class ServiceSonarrStub(eh.SonarrStub):
    def __init__(self) -> None:
        super().__init__()
        self.version = "4.0.0"

    def _series_payload(self) -> list[dict[str, Any]]:
        return [{**item, "seriesType": "anime"} for item in super()._series_payload()]

    def handler(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v3/system/status":
            self.requests.append((request.url.path, -1))
            return httpx.Response(200, json={"version": self.version}, request=request)
        return super().handler(request)


class ServiceShokoStub(eh.ShokoStub):
    version = "5.3.3"
    incompatible_queue = False
    renamer_enabled = False

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/api/v3/Init/Version":
            self.requests.append((request.method, path))
            return httpx.Response(200, json={"Server": {"Version": self.version}}, request=request)
        if path == "/api/v3/Queue" and self.incompatible_queue:
            self.requests.append((request.method, path))
            return httpx.Response(200, json={"UnexpectedQueueSchema": True}, request=request)
        if path == "/api/v3/Settings":
            self.requests.append((request.method, path))
            return httpx.Response(
                200,
                json={
                    "FileQualityFilterEnabled": False,
                    "Plugins": {
                        "Renamer": {
                            "RenameOnImport": self.renamer_enabled,
                            "MoveOnImport": self.renamer_enabled,
                            "RelocateOnImport": self.renamer_enabled,
                        }
                    },
                    "AniDb": {"MyList_AddFiles": False},
                },
                request=request,
            )
        return super().handler(request)


def _endpoint(handler: Any, base_url: str) -> ApiEndpoint:
    return eh.endpoint_for(handler, base_url)


def _csrf(body: bytes) -> str:
    match = re.search(rb'name="csrf_token" value="([^"]+)"', body)
    assert match is not None
    return match.group(1).decode("ascii")


def _setup_digest(body: bytes) -> str:
    match = re.search(rb'name="setup_digest" value="([0-9a-f]{64})"', body)
    assert match is not None
    return match.group(1).decode("ascii")


def _confirmation_form(body: bytes) -> dict[str, str]:
    match = re.search(rb'<form method="post" action="/setup/confirm">(.*?)</form>', body, re.DOTALL)
    assert match is not None
    form = match.group(1)
    csrf = re.search(rb'name="csrf_token" value="([^"]+)"', form)
    digest = re.search(rb'name="setup_digest" value="([0-9a-f]{64})"', form)
    consent = re.search(rb'name="automatic_link_consent" value="([^"]+)"', form)
    assert csrf is not None
    assert digest is not None
    assert consent is not None
    assert consent.group(1) == b"1"
    return {
        "csrf_token": csrf.group(1).decode("ascii"),
        "setup_digest": digest.group(1).decode("ascii"),
        "automatic_link_consent": consent.group(1).decode("ascii"),
        **{
            name.decode("ascii"): value.decode("ascii")
            for name, value in re.findall(rb'name="(confirm_\d+)" value="([0-9a-f]{64})"', form)
        },
    }


def _response_header(response: DashboardResponse, name: str) -> str:
    for key, value in response.headers:
        if key.lower() == name.lower():
            return value
    raise AssertionError(f"response is missing {name}")


def _dashboard_request(
    app: DashboardApplication,
    method: str,
    path: str,
    *,
    cookie: str | None = None,
    form: dict[str, str] | None = None,
) -> DashboardResponse:
    headers = {"Host": DASHBOARD_HOST}
    body = b""
    if form is not None:
        headers.update(
            {
                "Origin": f"http://{DASHBOARD_HOST}",
                "Content-Type": "application/x-www-form-urlencoded",
            }
        )
        body = urlencode(form).encode("utf-8")
    if cookie is not None:
        headers["Cookie"] = cookie
    return app.handle(method, path, headers, body)


def _service_world(tmp_path: Path) -> eh.World:
    world = eh.make_world(tmp_path, eh.backlog_xml(1, 2))
    world.sonarr = ServiceSonarrStub()
    world.shoko = ServiceShokoStub()
    world.sonarr.add_series(1, SONARR_ROOT, 300_001)
    anime_id = 4001
    episodes = (eh.EpisodeSpec(1, 1), eh.EpisodeSpec(1, 2))
    world.shoko.register(eh.registry_for(eh.SeriesSpec(1, 300_001, (anime_id,), episodes)))
    for episode_number, file_id, shoko_id in ((1, 11, 801), (2, 12, 802)):
        world.sonarr.add_episode(
            1,
            {
                "id": 100 + episode_number,
                "seriesId": 1,
                "seasonNumber": 1,
                "episodeNumber": episode_number,
                "title": f"Episode {episode_number}",
                "tvdbId": episode_number,
                "episodeFileId": file_id,
            },
        )
        sonarr_path = eh.sonarr_path(1, file_id)
        shoko_path = eh.shoko_path(1, file_id)
        relative = f"series1/file{file_id}.mkv"
        world.sonarr.add_file(
            1,
            {
                "id": file_id,
                "seriesId": 1,
                "path": sonarr_path,
                "size": eh.file_size(shoko_id),
            },
        )
        world.shoko.add_row(
            eh.ShokoRow(
                file_id=shoko_id,
                size=eh.file_size(shoko_id),
                locations=[
                    eh.ShokoLocation(
                        place_id=shoko_id * 10,
                        folder=1,
                        relative=relative,
                        absolute=shoko_path,
                    )
                ],
            )
        )
    return world


def _confirm_setup(controller: ServiceSetupController) -> None:
    initial = controller.refresh_view()
    assert initial is not None
    assert initial.snapshot_digest
    assert SONARR_ROOT in initial.root_options
    root_index = initial.root_options.index(SONARR_ROOT)
    assert (
        "selected"
        in controller.action(
            "scope",
            {"setup_digest": initial.snapshot_digest, f"root_{root_index}": SONARR_ROOT},
        ).lower()
    )
    selected = controller.refresh_view()
    assert selected is not None
    assert selected.confirmation_enabled
    form = {
        "setup_digest": selected.snapshot_digest,
        "automatic_link_consent": "1",
        **{
            f"confirm_{index}": hashlib.sha256(fact.encode("utf-8")).hexdigest()
            for index, fact in enumerate(selected.required_confirmations)
        },
    }
    assert "saved" in controller.action("confirm", form).lower()


@pytest.mark.parametrize("component", ["sonarr", "shoko"])
def test_real_setup_controller_latches_supported_version_drift_until_fresh_confirmation(
    tmp_path: Path,
    component: str,
) -> None:
    """Fresh current facts invalidate saved consent even when the cycle digest is stale."""
    environ = {
        SONARR_URL_ENV: "http://sonarr:8989",
        SONARR_API_KEY_ENV: "service-e2e-sonarr-key-never-real",
        SHOKO_URL_ENV: "http://shoko:8111",
        SHOKO_API_KEY_ENV: "service-e2e-shoko-key-never-real",
    }
    bootstrap = replace(
        load_bootstrap_settings(environ=environ), database_path=tmp_path / "state.sqlite3"
    )
    world = _service_world(tmp_path)
    sonarr = _endpoint(world.sonarr.handler, eh.SONARR_BASE)
    shoko = _endpoint(world.shoko.handler, eh.SHOKO_BASE)
    runtime = cli._NormalServiceRuntime(bootstrap, sonarr, shoko, cli.StopState())
    controller = runtime.setup
    service = (
        cast(ServiceSonarrStub, world.sonarr)
        if component == "sonarr"
        else cast(ServiceShokoStub, world.shoko)
    )
    original_version = service.version
    changed_version = "4.0.1" if component == "sonarr" else "5.3.4"
    try:
        _confirm_setup(controller)
        with open_state(bootstrap.database_path) as store:
            manager = OnboardingManager(store)
            saved = manager.record()
            assert saved is not None
            expected_cycle_digest = saved.binding_digest
            assert runtime._service_authorized(store, manager, expected_cycle_digest)

            service.version = changed_version  # Available API, but not covered by saved consent.
            assert controller.current_context(store) is None
            assert not runtime._service_authorized(store, manager, expected_cycle_digest)
            assert store.get_service_pause() == (True, "setup_changed")
            assert manager.reconfirmation_required()

        # Restart after the changed observation and restore the old version. The hard
        # reconfirmation requirement is persisted independently of the current version.
        service.version = original_version
        with open_state(bootstrap.database_path) as reopened:
            manager = OnboardingManager(reopened)
            assert not runtime._service_authorized(reopened, manager, expected_cycle_digest)
            assert reopened.get_service_pause() == (True, "setup_changed")
            assert manager.reconfirmation_required()

        _confirm_setup(controller)
        with open_state(bootstrap.database_path) as store:
            manager = OnboardingManager(store)
            refreshed = manager.record()
            assert refreshed is not None
            assert refreshed.binding_digest == expected_cycle_digest
            assert not manager.reconfirmation_required()
            assert store.get_service_pause() == (False, "")
            assert runtime._service_authorized(store, manager, expected_cycle_digest)
        assert world.shoko.posts_count() == 0
    finally:
        sonarr.client.close()
        shoko.client.close()


def test_unavailable_scope_check_blocks_posts_and_cannot_clear_reconfirmation(  # noqa: PLR0915 - outage and hard-hold recovery
    tmp_path: Path,
) -> None:
    environ = {
        SONARR_URL_ENV: "http://sonarr:8989",
        SONARR_API_KEY_ENV: "service-e2e-sonarr-key-never-real",
        SHOKO_URL_ENV: "http://shoko:8111",
        SHOKO_API_KEY_ENV: "service-e2e-shoko-key-never-real",
    }
    bootstrap = replace(
        load_bootstrap_settings(environ=environ), database_path=tmp_path / "state.sqlite3"
    )
    world = _service_world(tmp_path)
    sonarr = _endpoint(world.sonarr.handler, eh.SONARR_BASE)
    shoko = _endpoint(world.shoko.handler, eh.SHOKO_BASE)
    runtime = cli._NormalServiceRuntime(bootstrap, sonarr, shoko, cli.StopState())
    service_sonarr = cast(ServiceSonarrStub, world.sonarr)
    try:
        _confirm_setup(runtime.setup)
        with open_state(bootstrap.database_path) as store:
            record = OnboardingManager(store).record()
            assert record is not None
            expected_digest = record.binding_digest

        service_sonarr.down = True
        runtime.worker_tick()
        with open_state(bootstrap.database_path) as store:
            manager = OnboardingManager(store)
            assert store.get_service_pause() == (True, "setup_unavailable")
            assert not manager.reconfirmation_required()
        assert world.shoko.posts_count() == 0

        service_sonarr.down = False
        with open_state(bootstrap.database_path) as store:
            current = runtime.setup.current_context(store)
            assert current is not None
            decision = OnboardingManager(store).resume_context(current, iso="2026-10-16T00:00:00Z")
            assert decision.status.value == "authorized"
            assert store.get_service_pause() == (False, "")

        service_sonarr.version = "4.0.1"
        with open_state(bootstrap.database_path) as store:
            manager = OnboardingManager(store)
            assert not runtime._service_authorized(store, manager, expected_digest)
            assert store.get_service_pause() == (True, "setup_changed")
            assert manager.reconfirmation_required()

        service_sonarr.down = True
        runtime.worker_tick()
        with open_state(bootstrap.database_path) as store:
            manager = OnboardingManager(store)
            assert store.get_service_pause() == (True, "setup_changed")
            assert manager.reconfirmation_required()

        service_sonarr.down = False
        service_sonarr.version = "4.0.0"
        with open_state(bootstrap.database_path) as store:
            manager = OnboardingManager(store)
            assert not runtime._service_authorized(store, manager, expected_digest)
            assert store.get_service_pause() == (True, "setup_changed")
            assert manager.reconfirmation_required()
        assert world.shoko.posts_count() == 0
    finally:
        sonarr.client.close()
        shoko.client.close()


@pytest.mark.parametrize(
    "drift",
    ["folder_identity", "settings_during_folder_outage", "selected_type_during_folder_outage"],
)
def test_mixed_library_selected_folder_identity_drift_before_write_prevents_posts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, drift: str
) -> None:
    bootstrap = replace(
        load_bootstrap_settings(
            environ={
                SONARR_URL_ENV: "http://sonarr:8989",
                SONARR_API_KEY_ENV: "service-e2e-sonarr-key-never-real",
                SHOKO_URL_ENV: "http://shoko:8111",
                SHOKO_API_KEY_ENV: "service-e2e-shoko-key-never-real",
            }
        ),
        database_path=tmp_path / "state.sqlite3",
    )
    world = _service_world(tmp_path)
    cast(ServiceShokoStub, world.shoko).renamer_enabled = True
    world.shoko.folders.update({2: ("/drop", "Source"), 3: ("/manual", "Destination")})
    monkeypatch.setattr(
        cli, "sonarr_endpoint", lambda _: _endpoint(world.sonarr.handler, eh.SONARR_BASE)
    )
    monkeypatch.setattr(
        cli, "shoko_endpoint", lambda _: _endpoint(world.shoko.handler, eh.SHOKO_BASE)
    )

    def mapping_endpoint(store: Any, **_kwargs: Any) -> ApiEndpoint:
        return ApiEndpoint(
            httpx.Client(
                base_url=store.origin,
                transport=httpx.MockTransport(world.source.handler),
                follow_redirects=False,
            ),
            lambda: {"Accept-Encoding": "identity"},
        )

    monkeypatch.setattr(cli, "make_source_endpoint", mapping_endpoint)
    sonarr = _endpoint(world.sonarr.handler, eh.SONARR_BASE)
    shoko = _endpoint(world.shoko.handler, eh.SHOKO_BASE)
    runtime = cli._NormalServiceRuntime(bootstrap, sonarr, shoko, cli.StopState())
    clock = [datetime.now(UTC) + timedelta(seconds=1)]
    monkeypatch.setattr(cli, "current_iso", lambda: clock[0].isoformat())
    try:
        _confirm_setup(runtime.setup)
        runtime.worker_tick()
        assert world.shoko.posts_count() == 0
        assert "grace_period" in runtime.latest["diagnostics"]
        clock[0] += timedelta(seconds=301)
        original_context = runtime._service_authorization_context
        refreshes = 0

        def drift_before_send(store: Any, **kwargs: Any) -> Any:
            nonlocal refreshes
            refreshes += 1
            if refreshes == 2:
                if drift == "folder_identity":
                    world.shoko.folders[7] = world.shoko.folders.pop(1)
                elif drift == "settings_during_folder_outage":
                    cast(ServiceShokoStub, world.shoko).renamer_enabled = False
                    monkeypatch.setattr(
                        service_setup, "collect_import_folders", lambda *_, **__: None
                    )
                else:
                    world.shoko.folders[1] = (SHOKO_ROOT, "Source")
                    world.shoko.folders[2] = ("/drop", "Unknown")
            return original_context(store, **kwargs)

        monkeypatch.setattr(runtime, "_service_authorization_context", drift_before_send)
        runtime.worker_tick()
        assert refreshes >= 2
        assert world.shoko.posts_count() == 0
        with open_state(bootstrap.database_path) as store:
            assert store.get_service_pause() == (True, "setup_changed")
            assert OnboardingManager(store).reconfirmation_required()
    finally:
        sonarr.client.close()
        shoko.client.close()


@pytest.mark.parametrize("shoko_version", ["5.3.3", "5.3.3.0", "5.3.4", "6.0.0"])
def test_fresh_four_variable_service_onboards_links_canary_and_recovers_after_restart(  # noqa: PLR0915
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    shoko_version: str,
) -> None:
    environ = {
        SONARR_URL_ENV: "http://sonarr:8989",
        SONARR_API_KEY_ENV: "service-e2e-sonarr-key-never-real",
        SHOKO_URL_ENV: "http://shoko:8111",
        SHOKO_API_KEY_ENV: "service-e2e-shoko-key-never-real",
    }
    bootstrap = replace(
        load_bootstrap_settings(environ=environ),
        database_path=tmp_path / "state.sqlite3",
    )
    world = _service_world(tmp_path)
    service_shoko = cast(ServiceShokoStub, world.shoko)
    service_shoko.version = shoko_version
    monkeypatch.setattr(
        cli,
        "sonarr_endpoint",
        lambda _settings: _endpoint(world.sonarr.handler, eh.SONARR_BASE),
    )
    monkeypatch.setattr(
        cli,
        "shoko_endpoint",
        lambda _settings: _endpoint(world.shoko.handler, eh.SHOKO_BASE),
    )

    def mapping_endpoint(store: Any, **_kwargs: Any) -> ApiEndpoint:
        client = httpx.Client(
            base_url=store.origin,
            transport=httpx.MockTransport(world.source.handler),
            follow_redirects=False,
        )
        return ApiEndpoint(client, lambda: {"Accept-Encoding": "identity"})

    monkeypatch.setattr(cli, "make_source_endpoint", mapping_endpoint)
    sonarr = _endpoint(world.sonarr.handler, eh.SONARR_BASE)
    shoko = _endpoint(world.shoko.handler, eh.SHOKO_BASE)
    runtime = cli._NormalServiceRuntime(bootstrap, sonarr, shoko, cli.StopState())
    dashboard_sonarr = _endpoint(world.sonarr.handler, eh.SONARR_BASE)
    dashboard_shoko = _endpoint(world.shoko.handler, eh.SHOKO_BASE)
    dashboard_setup = ServiceSetupController(
        bootstrap, dashboard_sonarr, dashboard_shoko, bootstrap.database_path
    )
    background_refresh_started = threading.Event()
    background_refresh_finished = threading.Event()
    selected_inventory_started = threading.Event()
    release_selected_inventory = threading.Event()
    background_refresh_pending = False
    original_inventory = dashboard_setup._inventory
    original_background_refresh = dashboard_setup._refresh_in_background

    def held_selected_inventory(roots: tuple[str, ...]) -> Any:
        if roots == (SONARR_ROOT,):
            selected_inventory_started.set()
            assert release_selected_inventory.wait(timeout=10)
        return original_inventory(roots)

    def tracked_background_refresh() -> None:
        background_refresh_started.set()
        try:
            original_background_refresh()
        finally:
            background_refresh_finished.set()

    monkeypatch.setattr(dashboard_setup, "_inventory", held_selected_inventory)
    monkeypatch.setattr(dashboard_setup, "_refresh_in_background", tracked_background_refresh)

    def dashboard_onboarding_context() -> OnboardingContext | None:
        if not bootstrap.database_path.exists():
            return None
        with open_state(bootstrap.database_path) as store:
            return dashboard_setup.current_context(store)

    try:
        runtime.worker_tick()
        assert world.shoko.posts_count() == 0
        application = DashboardApplication(
            bootstrap.database_path,
            status_provider=runtime.status,
            onboarding_context=dashboard_onboarding_context,
            setup_provider=dashboard_setup.view,
            setup_action=dashboard_setup.action,
        )

        first = _dashboard_request(application, "GET", "/")
        assert first.status == 200
        setup = _dashboard_request(
            application,
            "POST",
            "/setup",
            form={"username": "owner", "password": "a long service password"},
        )
        assert setup.status == 303
        cookie = _response_header(setup, "Set-Cookie").split(";", 1)[0]

        background_refresh_pending = True
        scope_page = _dashboard_request(application, "GET", "/status", cookie=cookie)
        assert scope_page.status == 200
        assert b'role="progressbar"' in scope_page.body
        discovery_deadline = time.monotonic() + 10.0
        while SONARR_ROOT.encode() not in scope_page.body and time.monotonic() < discovery_deadline:
            time.sleep(0.01)
            scope_page = _dashboard_request(application, "GET", "/status", cookie=cookie)
        assert SONARR_ROOT.encode() in scope_page.body
        assert background_refresh_started.wait(timeout=5)
        assert background_refresh_finished.wait(timeout=5)
        background_refresh_pending = False

        background_refresh_started.clear()
        background_refresh_finished.clear()
        selected_inventory_started.clear()
        release_selected_inventory.clear()
        background_refresh_pending = True
        scope_result = _dashboard_request(
            application,
            "POST",
            "/setup/scope",
            cookie=cookie,
            form={
                "csrf_token": _csrf(scope_page.body),
                "setup_digest": _setup_digest(scope_page.body),
                "root_0": SONARR_ROOT,
            },
        )
        assert scope_result.status == 200
        cookie = _response_header(scope_result, "Set-Cookie").split(";", 1)[0]
        assert b'role="progressbar"' in scope_result.body
        assert b'action="/setup/confirm"' not in scope_result.body
        assert background_refresh_started.wait(timeout=5)
        assert selected_inventory_started.wait(timeout=5)

        loading_page = _dashboard_request(application, "GET", "/status", cookie=cookie)
        assert loading_page.status == 200
        assert b'role="progressbar"' in loading_page.body
        assert b'action="/setup/confirm"' not in loading_page.body

        release_selected_inventory.set()
        assert background_refresh_finished.wait(timeout=10)
        background_refresh_pending = False
        confirm_page = _dashboard_request(application, "GET", "/status", cookie=cookie)
        assert confirm_page.status == 200
        assert b'action="/setup/confirm"' in confirm_page.body
        confirm_form = _confirmation_form(confirm_page.body)

        background_refresh_started.clear()
        background_refresh_finished.clear()
        background_refresh_pending = True
        confirmation = _dashboard_request(
            application,
            "POST",
            "/setup/confirm",
            cookie=cookie,
            form=confirm_form,
        )
        assert confirmation.status == 200
        assert background_refresh_started.wait(timeout=5)
        assert background_refresh_finished.wait(timeout=10)
        background_refresh_pending = False
        with open_state(bootstrap.database_path) as store:
            saved = dashboard_setup.onboarding(store).record()
            assert saved is not None
            assert saved.binding.path_mappings == (PathMapping(SONARR_ROOT, SHOKO_ROOT),)

        clock = {"now": datetime.now(UTC) + timedelta(seconds=1)}
        monkeypatch.setattr(cli, "current_iso", lambda: clock["now"].isoformat())
        runtime.worker_tick()
        assert world.shoko.posts_count() == 0
        assert "grace_period" in runtime.latest["diagnostics"]
        clock["now"] += timedelta(seconds=301)
        service_shoko.incompatible_queue = True
        runtime.worker_tick()
        assert world.shoko.posts_count() == 0
        service_shoko.incompatible_queue = False
        clock["now"] += timedelta(seconds=301)
        runtime.worker_tick()
        assert world.shoko.posts_count() == 1
        first_post_id = world.shoko.posts[0][0]
        with open_state(bootstrap.database_path) as store:
            canary = store.get_service_canary()
            first_record = store.get_file(first_post_id)
            assert canary == (first_post_id, "verified")
            assert first_record is not None
            assert first_record.state is FileState.VERIFIED

        restarted_sonarr = _endpoint(world.sonarr.handler, eh.SONARR_BASE)
        restarted_shoko = _endpoint(world.shoko.handler, eh.SHOKO_BASE)
        restarted = cli._NormalServiceRuntime(
            bootstrap, restarted_sonarr, restarted_shoko, cli.StopState()
        )
        for _ in range(3):
            clock["now"] += timedelta(seconds=301)
            restarted.worker_tick()
            if world.shoko.posts_count() == 2:
                break
        assert world.shoko.posts_count() == 2
        assert world.shoko.posts[0][0] == first_post_id
        assert world.shoko.posts[1][0] != first_post_id
        with open_state(bootstrap.database_path) as store:
            first_record = store.get_file(first_post_id)
            assert first_record is not None
            assert first_record.state is FileState.VERIFIED
            assert len(store.files_in_state(FileState.VERIFIED)) == 2
        restarted_sonarr.client.close()
        restarted_shoko.client.close()
    finally:
        release_selected_inventory.set()
        if background_refresh_pending and background_refresh_started.wait(timeout=5):
            background_refresh_finished.wait(timeout=10)
        dashboard_sonarr.client.close()
        dashboard_shoko.client.close()
        sonarr.client.close()
        shoko.client.close()
