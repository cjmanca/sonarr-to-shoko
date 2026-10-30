"""Authenticated route-specific tables share truthful counts and a single control."""

import re
from dataclasses import replace
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import httpx
import pytest
import test_dashboard as dashboard
from test_state import LIMITS, prepare, walk_to_planned

from shoko_linker import cli
from shoko_linker.bootstrap import BootstrapSettings
from shoko_linker.config import SecretRef
from shoko_linker.dashboard import DashboardApplication, DashboardStatus, DashboardTable
from shoko_linker.file_display import FileDisplay
from shoko_linker.http import ApiEndpoint
from shoko_linker.linking import SendOutcome
from shoko_linker.logging_setup import register_secret
from shoko_linker.onboarding import OnboardingManager
from shoko_linker.scan_status import ScanEvidence, ScanExclusionPage, ScanExclusionView, ScanSummary
from shoko_linker.service_settings import load_service_settings
from shoko_linker.state import (
    Cohort,
    FileState,
    ScanExclusionRow,
    SendToken,
    ServiceFileCounts,
    ServiceFileExample,
    ServiceFilePage,
    StateStore,
    open_state,
    table_page,
)


def request_headers(cookie: str = "") -> dict[str, str]:
    return {
        "cookie": cookie,
        "host": "localhost",
        "origin": "http://localhost",
        "content-type": "application/x-www-form-urlencoded",
    }


def login(app: DashboardApplication) -> str:
    response = app.handle(
        "POST",
        "/setup",
        request_headers(),
        urlencode(
            {
                "username": dashboard.USERNAME,
                "password": dashboard.PASSWORD,
            }
        ).encode(),
    )
    assert response.status == 303
    return dict(response.headers)["Set-Cookie"].split(";", 1)[0]


def assert_shared_header(text: str) -> None:
    text = text.split("<script>", 1)[0]
    before_main, main = text.split("<main", 1)
    header = before_main.split('<header class="site-header">', 1)[1].split("</header>", 1)[0]
    assert text.count("<header") == 1
    assert text.count('data-live="metrics"') == 1
    assert 'data-live="metrics"' in header
    for target in ("/files/verified", "/files/waiting", "/issues"):
        assert f'href="{target}"' in header
    assert header.count('class="icon-button"') == 1
    assert 'class="header-caption"' not in header
    assert 'data-live="metrics"' not in main


@pytest.mark.parametrize(
    "route",
    [
        "/status",
        "/issues",
        "/issues?view=exclusions",
        "/files/verified",
        "/files/waiting",
        "/history",
        "/settings",
    ],
)
def test_authenticated_controls_share_original_site_header(tmp_path: Path, route: str) -> None:
    app = dashboard.make_app(tmp_path)
    text = app.handle("GET", route, request_headers(login(app))).body.decode()
    assert_shared_header(text)


def test_enrollment_and_login_keep_caption_without_service_controls(tmp_path: Path) -> None:
    app = dashboard.make_app(tmp_path)
    enrollment = app.handle("GET", "/", {}).body.decode()
    login(app)
    sign_in = app.handle("GET", "/", {}).body.decode()
    for text in (enrollment, sign_in):
        assert '<span class="header-caption">Your anime library, connected</span>' in text
        assert 'data-live="metrics"' not in text
        assert 'class="icon-button"' not in text


def runtime_app(tmp_path: Path) -> tuple[DashboardApplication, list[str], ApiEndpoint]:
    app = dashboard.make_app(tmp_path, onboarding=True)
    with open_state(app.state_path, budget_limits=LIMITS) as store:
        for index in range(1, 114):
            state = (
                FileState.WAITING
                if index <= 103
                else (FileState.CONFLICT if index <= 111 else FileState.VERIFIED)
            )
            store.record_file(
                index,
                sonarr_series_id=1,
                sonarr_file_id=index,
                sonarr_path=f"/data/media/anime/Show/Episode-{index}.mkv",
                sonarr_size=1024,
                cohort=Cohort.NEW_ARRIVAL,
                iso=dashboard.T0.isoformat(),
                state=state,
            )
    requests: list[str] = []

    def reject(request: httpx.Request) -> httpx.Response:
        requests.append(str(request.url))
        raise AssertionError("Dashboard table providers must not perform HTTP")

    endpoint = ApiEndpoint(httpx.Client(transport=httpx.MockTransport(reject)), lambda: {})
    bootstrap = BootstrapSettings(
        "http://sonarr:8989",
        SecretRef("SONARR_API_KEY"),
        "http://shoko:8111",
        SecretRef("SHOKO_API_KEY"),
        app.state_path,
    )
    runtime = cli._NormalServiceRuntime(bootstrap, endpoint, endpoint, cli.StopState())
    # Deliberately stale status counts must be replaced by the table's own snapshot.
    original = app.status_provider()
    app.status_provider = lambda: replace(
        original, linked_files=900, waiting_files=900, review_files=900
    )
    app.table_provider = runtime.table
    app.setup_provider = lambda: None
    return app, requests, endpoint


def test_deep_links_auth_coherent_header_paginated_files_and_separate_history(
    tmp_path: Path,
) -> None:
    app, requests, endpoint = runtime_app(tmp_path)
    try:
        for route in ("/issues", "/files/verified", "/files/waiting", "/history", "/settings"):
            assert app.handle("GET", route, {}).status == 303
        cookie = login(app)
        headers = request_headers(cookie)
        waiting = app.handle("GET", "/files/waiting?page=2&page_size=50", headers)
        assert waiting.status == 200
        text = waiting.body.decode()
        assert 'href="/files/waiting" aria-current="page"' in text
        assert "Showing 51&ndash;100 of 103" in text
        assert "Episode-51.mkv" in text
        assert "Episode-101.mkv" not in text
        assert "<td>Show</td>" in text
        assert "<td>/data/media/anime</td>" in text
        assert 'class="quick-total waiting"' in text
        assert "</span>103</a>" in text
        assert "</span>8</a>" in text
        assert "</span>2</a>" in text
        assert "900</a>" not in text
        assert text.count('action="/pause"') == 1
        assert 'action="/resume"' not in text
        assert 'aria-label="Pause automatic linking"' in text
        assert 'data-table="files-waiting-review"' in text
        assert '<col data-column="path">' in text
        assert 'scope="col" data-column="filename"' in text
        assert "Shoko queue size" in text
        assert "Set up automatic linking" not in text
        clamped = app.handle("GET", "/files/waiting?page=999&page_size=50", headers).body.decode()
        assert "Showing 101&ndash;103 of 103" in clamped
        assert "page 3 of 3" in clamped
        issues = app.handle("GET", "/issues", headers).body.decode()
        assert "Episode-104.mkv" in issues
        assert "Episode-1.mkv" not in issues
        assert "separate views; their totals are not added" in issues
        verified = app.handle("GET", "/files/verified", headers).body.decode()
        assert "Episode-112.mkv" in verified
        assert "Episode-104.mkv" not in verified
        history = app.handle("GET", "/history", headers).body.decode()
        assert "No records in this view" in history
        assert "Episode-112.mkv" not in history
        assert "not an audit of media changes" in history
        overview = app.handle("GET", "/status", headers).body.decode()
        assert "Connections &amp; mappings" in overview
        assert "<table" not in overview
        assert "Recent linker actions" not in overview
        assert requests == []
    finally:
        endpoint.client.close()


@pytest.mark.parametrize(
    "query",
    [
        "page=0",
        "page=-1",
        "page=1&page=2",
        "page_size=101",
        "page=99999999999999999999",
        "group=verified",
        "filter=anything",
        "view=unknown",
        "view=review&view=exclusions",
        "page=%xx",
        "page=1&",
        "page=1&page_size=2&view=review&x=1",
    ],
)
def test_table_query_rejects_unknown_duplicate_and_unbounded_values(
    tmp_path: Path, query: str
) -> None:
    app = dashboard.make_app(tmp_path)
    cookie = login(app)
    called = []

    def unexpected_provider(_kind: str, _page: int, _size: int) -> DashboardTable:
        called.append(True)
        raise AssertionError("Invalid queries must not reach the table provider")

    app.table_provider = unexpected_provider
    assert app.handle("GET", "/issues?" + query, request_headers(cookie)).status == 400
    assert called == []


def test_stored_pause_controls_single_header_button_and_csrf(tmp_path: Path) -> None:
    app = dashboard.make_app(tmp_path)
    app.status_provider = lambda: DashboardStatus(
        "Connected", 0, 0, 0, False, "Safety review needed."
    )
    cookie = login(app)
    headers = request_headers(cookie)
    text = app.handle("GET", "/issues", headers).body.decode()
    assert "Waiting for safety checks" in text
    assert 'action="/pause"' in text
    assert 'action="/resume"' not in text
    assert app.handle("POST", "/pause", headers, b"csrf_token=bad").status == 403
    token = dashboard.csrf_from(text)
    paused = app.handle("POST", "/pause", headers, urlencode({"csrf_token": token}).encode())
    assert paused.status == 303
    cookie = dict(paused.headers)["Set-Cookie"].split(";", 1)[0]
    text = app.handle("GET", "/files/waiting", request_headers(cookie)).body.decode()
    assert 'action="/resume"' in text
    assert 'action="/pause"' not in text
    assert 'aria-label="Resume automatic linking"' in text
    assert "Paused by you" in text
    assert "File data is unavailable" in text


def test_persisted_system_pause_offers_resume_but_refusal_preserves_safety_hold(
    tmp_path: Path,
) -> None:
    app = dashboard.make_app(tmp_path, onboarding=True)
    with open_state(app.state_path) as store:
        OnboardingManager(store).pause_for_review(
            "setup_changed",
            "Confirm the changed scope.",
            iso=dashboard.T0.isoformat(),
        )
    # Even an old active-worker status cannot replace a real persisted pause.
    app.onboarding_context = None
    cookie = login(app)
    headers = request_headers(cookie)
    text = app.handle("GET", "/issues", headers).body.decode()
    assert "Paused for service checks" in text
    assert 'action="/resume"' in text
    assert 'action="/pause"' not in text
    response = app.handle(
        "POST", "/resume", headers, urlencode({"csrf_token": dashboard.csrf_from(text)}).encode()
    )
    assert response.status == 200
    assert_shared_header(response.body.decode())
    assert "Complete setup before automatic linking can resume" in response.body.decode()
    assert 'action="/resume"' in response.body.decode()
    with open_state(app.state_path) as store:
        assert store.get_service_pause() == (True, "setup_changed")
        assert OnboardingManager(store).reconfirmation_required()


def test_history_displays_requests_not_just_current_verified_files(tmp_path: Path) -> None:
    app, requests, endpoint = runtime_app(tmp_path)
    try:
        with open_state(app.state_path, budget_limits=LIMITS) as store:
            walk_to_planned(store, file_id=201)
            token = prepare(store, file_id=201)
            assert isinstance(token, SendToken)
            store.mark_sent(token, iso="2026-10-29T12:00:00+00:00")
            store.record_send_outcome(
                token,
                SendOutcome.ACCEPTED_UNVERIFIED,
                iso="2026-10-29T12:00:01+00:00",
                http_status=200,
            )
        cookie = login(app)
        text = app.handle("GET", "/history", request_headers(cookie)).body.decode()
        assert "Showing 1&ndash;1 of 1" in text
        assert "Shoko accepted the request" in text
        assert "verified linking is not yet recorded" in text
        assert 'datetime="2026-10-29T12:00:00+00:00"' in text
        assert 'data-column="requested">Requested' in text
        assert 'data-column="updated">Updated' in text
        assert '<td class="time-cell"><time' in text
        assert "Episode-112.mkv" not in text
        assert requests == []
    finally:
        endpoint.client.close()


def test_library_projection_drops_when_connection_preferences_change_mid_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app, requests, endpoint = runtime_app(tmp_path)
    calls = 0

    def changed_preferences(store: StateStore, bootstrap: BootstrapSettings) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        preferences = load_service_settings(store, bootstrap)
        if calls > 1:
            preferences["shoko_url"] = "http://changed:8111"
        return preferences

    monkeypatch.setattr(cli, "load_service_settings", changed_preferences)
    try:
        assert app.table_provider is not None
        result = app.table_provider("waiting", 1, 50)
        assert result.displays == ()
        assert result.counts == ServiceFileCounts(2, 103, 8)
        assert requests == []
    finally:
        endpoint.client.close()


def test_table_filename_library_relative_path_and_hover_text_are_escaped_and_redacted() -> None:
    secret = "table-render-secret-sentinel"
    register_secret(secret)
    counts = ServiceFileCounts(0, 0, 1)
    row = ServiceFileExample(
        1, "conflict", "conflict", f"<conflict> {secret}", "<file>.mkv", "", (), "", ""
    )
    table = DashboardTable(
        files=ServiceFilePage("review", table_page(1, 1, 50), counts, (row,)),
        counts=counts,
        displays=(FileDisplay("<file>.mkv", "Show/<season>", f"/library/{secret}"),),
    )
    text = DashboardApplication._table_section("/issues", "review", table)
    assert secret not in text
    assert "&lt;file&gt;.mkv" in text
    assert "Show/&lt;season&gt;" in text
    assert 'title="&lt;conflict&gt;' in text
    assert '<span class="sr-only">&lt;conflict&gt;' in text
    assert "<conflict>" not in text


@pytest.mark.parametrize(
    "route", ["/files/verified", "/files/waiting", "/issues", "/issues?view=exclusions"]
)
@pytest.mark.parametrize("empty", [False, True])
def test_every_file_table_has_links_and_matching_column_counts(route: str, empty: bool) -> None:
    counts = ServiceFileCounts(0, 0, 1)
    row = ServiceFileExample(
        1,
        "conflict",
        "conflict",
        "Review this file",
        "File.mkv",
        "",
        (),
        "",
        "",
        known_anidb_ids=(12,),
        series_name="Retained name",
    )
    table = DashboardTable(
        files=ServiceFilePage(
            "review", table_page(0 if empty else 1, 1, 50), counts, () if empty else (row,)
        )
    )
    view = "exclusions" if "?" in route else "review"
    if view == "exclusions":
        exclusion = ScanExclusionRow(1, "outside_scope", "Outside selected library")
        table = DashboardTable(
            exclusions=ScanExclusionPage(
                ScanEvidence(ScanSummary("context", "", "", 1, 1, 0, 1, 1), (), ()),
                table_page(0 if empty else 1, 1, 50),
                () if empty else (ScanExclusionView(exclusion, FileDisplay()),),
                True,
                "Exclusions",
            )
        )
    panel = DashboardApplication._table_section(route.split("?", 1)[0], view, table)
    assert panel.count('<th scope="col"') == 6
    assert 'data-column="links">Links' in panel
    if empty:
        assert '<td colspan="6">No records in this view.' in panel
    else:
        body = panel.split("<tbody>", 1)[1]
        assert len(re.findall(r"<td(?: [^>]*)?>", body)) == 6
        if view == "exclusions":
            assert '<td class="links-cell">&mdash;</td>' in body
            assert "https://anidb.net" not in body
        else:
            assert 'href="https://anidb.net/anime/12"' in body
