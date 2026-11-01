"""Offline authenticated dashboard recovery with the real controller and SQLite."""

from pathlib import Path
from typing import NoReturn
from urllib.parse import urlencode

import pytest
import test_dashboard as dashboard
from test_dashboard_routes import login, request_headers
from test_source_rebaseline import poll, setup
from test_source_review import Clock, Collector

from shoko_linker.dashboard import (
    DashboardApplication,
    DashboardResponse,
)
from shoko_linker.source_review import SourceReviewController
from shoko_linker.state import open_state


def fixture(
    tmp_path: Path,
) -> tuple[DashboardApplication, str, Collector, SourceReviewController, Clock, list[bool]]:
    clock = Clock()
    path = tmp_path / "state.sqlite3"
    collector = Collector(clock)
    wakes: list[bool] = []
    controller = SourceReviewController(
        path,
        collector,
        wake=lambda: wakes.append(True),
        shutdown_requested=lambda: False,
        now=lambda: clock.now,
        monotonic=lambda: clock.mono,
    )
    app = DashboardApplication(
        path,
        clock=lambda: clock.now,
        source_review_provider=controller.view,
        source_review_assess=controller.request_assessment,
        source_review_confirm=controller.request_confirmation,
    )
    cookie = login(app)
    with open_state(path) as store:
        setup(store)
    return app, cookie, collector, controller, clock, wakes


def get(app: DashboardApplication, cookie: str, route: str = "/status") -> str:
    response = app.handle("GET", route, request_headers(cookie))
    assert response.status == 200
    return response.body.decode()


def post(
    app: DashboardApplication, cookie: str, route: str, **fields: str
) -> tuple[DashboardResponse, str]:
    fields = {"csrf_token": dashboard.csrf_from(get(app, cookie)), **fields}
    response = app.handle("POST", route, request_headers(cookie), urlencode(fields).encode())
    next_cookie = dict(response.headers).get("Set-Cookie", cookie).split(";", 1)[0]
    return response, next_cookie


@pytest.mark.parametrize(
    "route", ["/status", "/issues", "/files/waiting", "/files/verified", "/history", "/settings"]
)
def test_ordinary_routes_never_request_collection_approval(tmp_path: Path, route: str) -> None:
    app, cookie, collector, controller, clock, wakes = fixture(tmp_path)
    for total in [69, 8124, 68]:
        with open_state(app.state_path) as store:
            poll(store, total=total, at=clock.now.isoformat())
        text = get(app, cookie, route)
        assert 'data-refresh-url="' + route + '"' in text
        for obsolete in [
            "Review library changes",
            "I reviewed",
            "Accept changes",
            'action="/source-review/',
            'name="review_token"',
            "Library changes need review",
            "Changes accepted.",
        ]:
            assert obsolete not in text
        if route == "/status":
            assert 'id="linking-controls"' in text
            assert "Library updates are handled automatically within your saved libraries" in text
    assert collector.capture_calls == 0
    assert not wakes
    assert controller.view("a" * 64).token is None


@pytest.mark.parametrize(
    ("route", "fields"),
    [
        ("/source-review/assess", {}),
        ("/source-review/confirm", {}),
        ("/source-review/confirm", {"review_token": "old-token", "acknowledge": "yes"}),
    ],
)
def test_stale_forms_are_authenticated_harmless_redirects(
    tmp_path: Path, route: str, fields: dict[str, str]
) -> None:
    app, cookie, collector, controller, clock, wakes = fixture(tmp_path)
    with open_state(app.state_path) as store:
        before = list(store.connection.execute("SELECT key, value FROM meta ORDER BY key"))
    for _ in range(3):
        response, cookie = post(app, cookie, route, **fields)
        assert response.status == 303
        assert dict(response.headers)["Location"] == "/status#linking-controls"
        assert "Library updates are handled automatically within your saved libraries" in get(
            app, cookie
        )
    with open_state(app.state_path) as store:
        assert list(store.connection.execute("SELECT key, value FROM meta ORDER BY key")) == before
        assert store.get_source_continuity_review().requires_rebaseline
    assert not wakes
    assert collector.capture_calls == 0
    assert controller.view("a" * 64).token is None


@pytest.mark.parametrize("extra", ["&unexpected=x", "&csrf_token=duplicate", "&acknowledge=yes"])
def test_strict_assessment_forms_and_stale_sessions(tmp_path: Path, extra: str) -> None:
    app, cookie, collector, controller, clock, wakes = fixture(tmp_path)
    csrf = dashboard.csrf_from(get(app, cookie))
    assert (
        app.handle(
            "POST",
            "/source-review/assess",
            request_headers(cookie),
            (urlencode({"csrf_token": csrf}) + extra).encode(),
        ).status
        == 400
    )
    response, new_cookie = post(app, cookie, "/source-review/assess")
    assert response.status == 303
    assert new_cookie != cookie
    assert (
        app.handle(
            "POST",
            "/source-review/assess",
            request_headers(cookie),
            urlencode({"csrf_token": csrf}).encode(),
        ).status
        == 403
    )
    headers = request_headers(new_cookie)
    headers["origin"] = "http://elsewhere"
    assert (
        app.handle(
            "POST", "/source-review/assess", headers, urlencode({"csrf_token": csrf}).encode()
        ).status
        == 403
    )
    assert (
        app.handle(
            "POST",
            "/source-review/assess",
            request_headers(),
            urlencode({"csrf_token": csrf}).encode(),
        ).status
        == 403
    )
    assert collector.capture_calls == 0
    assert not wakes


@pytest.mark.parametrize(
    "payload", [{"review_token": "a" * 257}, {"acknowledge": "no"}, {"owner": "forged"}]
)
def test_confirmation_rejects_untrusted_fields(tmp_path: Path, payload: dict[str, str]) -> None:
    app, cookie, collector, controller, clock, wakes = fixture(tmp_path)
    response, cookie = post(app, cookie, "/source-review/confirm", **payload)
    assert response.status == 400
    assert (
        app.handle(
            "POST", "/source-review/assess", request_headers(cookie), b"csrf_token=" + b"a" * 8192
        ).status
        == 413
    )
    assert not wakes
    assert collector.capture_calls == 0


@pytest.mark.parametrize("route", ["/source-review/assess", "/source-review/confirm"])
def test_retired_routes_never_invoke_compatibility_callbacks(tmp_path: Path, route: str) -> None:
    app, cookie, collector, controller, clock, wakes = fixture(tmp_path)

    def forbidden(*_args: str) -> NoReturn:
        pytest.fail("Retired normal review invoked a diagnostic callback")

    app.source_review_provider = forbidden
    app.source_review_assess = forbidden
    app.source_review_confirm = forbidden
    assert app.handle("POST", route, request_headers(), b"csrf_token=untrusted").status == 403
    csrf = dashboard.csrf_from(get(app, cookie))
    assert (
        app.handle(
            "POST", route, request_headers(cookie), urlencode({"csrf_token": "wrong"}).encode()
        ).status
        == 403
    )
    duplicate = urlencode({"csrf_token": csrf}) + "&review_token=one&review_token=two"
    assert app.handle("POST", route, request_headers(cookie), duplicate.encode()).status == 400
    response, cookie = post(app, cookie, route)
    assert response.status == 303
    assert collector.capture_calls == 0
    assert not wakes
