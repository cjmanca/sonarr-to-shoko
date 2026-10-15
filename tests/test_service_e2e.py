"""Fresh-install service onboarding, canary linking, and restart recovery via MockTransport."""

from __future__ import annotations

import hashlib
import re
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import e2e_harness as eh
import httpx
import pytest

from shoko_linker import cli
from shoko_linker.bootstrap import (
    SHOKO_API_KEY_ENV,
    SHOKO_URL_ENV,
    SONARR_API_KEY_ENV,
    SONARR_URL_ENV,
    load_bootstrap_settings,
)
from shoko_linker.dashboard import DashboardApplication, DashboardResponse
from shoko_linker.http import ApiEndpoint
from shoko_linker.paths import PathMapping
from shoko_linker.state import FileState, open_state

SONARR_ROOT = eh.SONARR_ROOT
SHOKO_ROOT = eh.SHOKO_ROOT
DASHBOARD_HOST = "linker.test"


class ServiceSonarrStub(eh.SonarrStub):
    def _series_payload(self) -> list[dict[str, Any]]:
        return [{**item, "seriesType": "anime"} for item in super()._series_payload()]

    def handler(self, request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v3/system/status":
            self.requests.append((request.url.path, -1))
            return httpx.Response(200, json={"version": "4.0.0"}, request=request)
        return super().handler(request)


class ServiceShokoStub(eh.ShokoStub):
    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/api/v3/Init/Version":
            self.requests.append((request.method, path))
            return httpx.Response(200, json={"Server": {"Version": "5.3.3"}}, request=request)
        if path == "/api/v3/Settings":
            self.requests.append((request.method, path))
            return httpx.Response(
                200,
                json={
                    "FileQualityFilterEnabled": False,
                    "Plugins": {
                        "Renamer": {
                            "RenameOnImport": False,
                            "MoveOnImport": False,
                            "RelocateOnImport": False,
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


def test_fresh_four_variable_service_onboards_links_canary_and_recovers_after_restart(  # noqa: PLR0915
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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

    try:
        runtime.worker_tick()
        assert world.shoko.posts_count() == 0
        application = DashboardApplication(
            bootstrap.database_path,
            status_provider=runtime.status,
            onboarding_context=runtime.onboarding_context,
            setup_provider=runtime.setup.view,
            setup_action=runtime.setup.action,
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

        scope_page = _dashboard_request(application, "GET", "/status", cookie=cookie)
        assert scope_page.status == 200
        assert SONARR_ROOT.encode() in scope_page.body
        scope_result = _dashboard_request(
            application,
            "POST",
            "/setup/scope",
            cookie=cookie,
            form={"csrf_token": _csrf(scope_page.body), "root_0": SONARR_ROOT},
        )
        assert scope_result.status == 200
        cookie = _response_header(scope_result, "Set-Cookie").split(";", 1)[0]

        confirm_page = _dashboard_request(application, "GET", "/status", cookie=cookie)
        assert confirm_page.status == 200
        setup_view = runtime.setup.view()
        assert setup_view is not None
        assert setup_view.confirmation_enabled
        confirm_form = {
            "csrf_token": _csrf(confirm_page.body),
            "automatic_link_consent": "1",
            **{
                f"confirm_{index}": hashlib.sha256(fact.encode("utf-8")).hexdigest()
                for index, fact in enumerate(setup_view.required_confirmations)
            },
        }
        confirmation = _dashboard_request(
            application,
            "POST",
            "/setup/confirm",
            cookie=cookie,
            form=confirm_form,
        )
        assert confirmation.status == 200
        with open_state(bootstrap.database_path) as store:
            saved = runtime.setup.onboarding(store).record()
            assert saved is not None
            assert saved.binding.path_mappings == (PathMapping(SONARR_ROOT, SHOKO_ROOT),)

        clock = {"now": datetime.now(UTC) + timedelta(seconds=1)}
        monkeypatch.setattr(cli, "current_iso", lambda: clock["now"].isoformat())
        runtime.worker_tick()
        assert world.shoko.posts_count() == 0
        assert "grace_period" in runtime.latest["diagnostics"]
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
        sonarr.client.close()
        shoko.client.close()
