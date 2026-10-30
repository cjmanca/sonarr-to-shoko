"""Offline authenticated dashboard recovery with the real controller and SQLite."""

import json
import re
import shutil
import subprocess
from pathlib import Path
from urllib.parse import urlencode

import pytest
import test_dashboard as dashboard
from test_dashboard_routes import login, request_headers
from test_source_rebaseline import setup
from test_source_review import Clock, Collector

from shoko_linker.dashboard import _DASHBOARD_JS, DashboardApplication, DashboardResponse
from shoko_linker.source_review import SourceReviewController
from shoko_linker.state import META_SOURCE_CONTINUITY_HOLD, open_state


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


def token(document: str) -> str:
    match = re.search(r'name="review_token" value="([^"]+)"', document)
    assert match is not None
    return match[1]


def test_authenticated_assess_recheck_changed_review_and_local_audit(tmp_path: Path) -> None:
    app, cookie, collector, controller, clock, wakes = fixture(tmp_path)
    for route in ["/status", "/issues", "/files/waiting", "/history"]:
        text = get(app, cookie, route)
        assert "Source collection changed" in text
        assert "/status#source-review" in text
    assert collector.capture_calls == 0
    assert not wakes
    response, cookie = post(app, cookie, "/source-review/assess")
    assert response.status == 200
    assert 'data-refresh-url="/status"' in response.body.decode()
    assert "Fresh assessment queued" in response.body.decode()
    assert collector.capture_calls == 0
    assert controller.worker_tick(clock.mono + 60)
    first = token(get(app, cookie))
    assert "56 unrecognized files" in get(app, cookie)
    response, cookie = post(app, cookie, "/source-review/confirm", review_token=first)
    assert response.status == 400
    collector.content = "changed paths with the same counts"
    response, cookie = post(
        app, cookie, "/source-review/confirm", review_token=first, acknowledge="yes"
    )
    assert response.status == 200
    assert 'data-refresh-url="/status"' in response.body.decode()
    assert "Rechecking fresh evidence" in response.body.decode()
    clock.advance()
    assert controller.worker_tick(clock.mono + 60)
    changed = get(app, cookie)
    second = token(changed)
    assert second != first
    assert "Source content changed again" in changed
    assert 'name="acknowledge" value="yes" required' in changed
    assert " checked" not in changed
    with open_state(app.state_path) as store:
        store.set_service_pause(True, "user_paused", clock.now.isoformat())
    # User pause is independent of baseline acceptance and must survive it.
    response, cookie = post(
        app, cookie, "/source-review/confirm", review_token=second, acknowledge="yes"
    )
    assert response.status == 200
    clock.advance()
    assert controller.worker_tick(clock.mono + 60)
    final = get(app, cookie)
    assert "Local baseline audit" in final
    assert "Paused by you" in final
    assert "Running" not in final.split("<script>")[0]
    with open_state(app.state_path) as store:
        assert not store.get_source_continuity_review().requires_rebaseline
        assert store.get_service_pause()[0]


@pytest.mark.parametrize("extra", ["&unexpected=x", "&csrf_token=duplicate", "&acknowledge=yes"])
def test_strict_assessment_forms_and_stale_sessions(tmp_path: Path, extra: str) -> None:
    app, cookie, collector, controller, clock, wakes = fixture(tmp_path)
    csrf = dashboard.csrf_from(get(app, cookie))
    response = app.handle(
        "POST",
        "/source-review/assess",
        request_headers(cookie),
        (urlencode({"csrf_token": csrf}) + extra).encode(),
    )
    assert response.status == 400
    assert not wakes
    response, new_cookie = post(app, cookie, "/source-review/assess")
    assert response.status == 200
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
    assert collector.capture_calls == 0


def test_confirmation_owner_and_expiry(tmp_path: Path) -> None:
    app, cookie, collector, controller, clock, wakes = fixture(tmp_path)
    response, cookie = post(app, cookie, "/source-review/assess")
    assert controller.worker_tick(clock.mono + 60)
    prepared = token(get(app, cookie))
    response = app.handle(
        "POST",
        "/login",
        request_headers(),
        urlencode({"username": dashboard.USERNAME, "password": dashboard.PASSWORD}).encode(),
    )
    other = dict(response.headers)["Set-Cookie"].split(";", 1)[0]
    assert 'name="review_token"' not in get(app, other)
    response, other = post(
        app, other, "/source-review/confirm", review_token=prepared, acknowledge="yes"
    )
    assert "unavailable or changed" in response.body.decode()
    clock.advance(901)
    text = get(app, cookie)
    assert 'name="review_token"' not in text
    assert "Request a new assessment" in text
    assert "Assess source change" in text
    assert collector.capture_calls > 0


def test_legacy_evidence_is_honest_and_local_get_cannot_change_baseline(tmp_path: Path) -> None:
    app, cookie, collector, controller, clock, wakes = fixture(tmp_path)
    with open_state(app.state_path) as store:
        store._conn.execute("DELETE FROM meta WHERE key = ?", (META_SOURCE_CONTINUITY_HOLD,))
        store._conn.commit()
    text = get(app, cookie)
    assert "original cause and counts were not retained" in text
    assert "Assess source change" in text
    assert not wakes
    assert not collector.works
    with open_state(app.state_path) as store:
        assert store.get_source_continuity_review().requires_rebaseline


def test_poller_binds_acknowledgment_to_token_and_keeps_new_csrf() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node unavailable")
    update = _DASHBOARD_JS.split("  function applyUpdate(next) {", 1)[1].split(
        "  async function update()", 1
    )[0]
    production = "function applyUpdate(next) {" + update
    script = r"""
const assert = require('node:assert/strict');
const vm = require('node:vm');
for (const same of [true, false]) {
  const oldAck = {checked: true}, freshAck = {checked: false};
  const oldCsrf = {value: 'old'}, freshCsrf = {value: 'new'};
  function panel(token, ack, csrf, markup) {
    const form = {dataset: {reviewToken: token}, querySelector() {return ack;}};
    return {dataset: {live: 'source-review'}, tagName: 'SECTION', outerHTML: markup,
      querySelector(selector) {return form;}, querySelectorAll() {return [];},
      replaceWith(fresh) {current = fresh;}};
  }
  let current = panel('original', oldAck, oldCsrf, 'old markup');
  const fresh = panel(same ? 'original' : 'changed', freshAck, freshCsrf, 'new markup');
  const next = {querySelector(selector) {
    return selector === 'input[name="csrf_token"]' ? freshCsrf : fresh;
  }};
  const document = {querySelectorAll(selector) {
    return selector === 'input[name="csrf_token"]' ? [oldCsrf] : [current];
  }};
  const context = {document, notice: {}, refreshUrl: '/status',
    renderedRefresh() {return '/status';}};
  vm.createContext(context);
  vm.runInContext(PRODUCTION, context);
  context.applyUpdate(next);
  assert.equal(freshAck.checked, same);
  assert.equal(oldCsrf.value, 'new');
  assert.equal(freshCsrf.value, 'new');
  assert.equal(current, fresh);
}
""".replace("PRODUCTION", json.dumps(production))
    result = subprocess.run(  # noqa: S603 - fixed local script, resolved Node, no shell
        [node, "-"], input=script, text=True, capture_output=True, timeout=10, check=False
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(
    "payload",
    [
        {"review_token": "a" * 257, "acknowledge": "yes"},
        {"review_token": "a" * 64, "acknowledge": "no"},
        {"review_token": "a" * 64, "acknowledge": "yes", "owner": "forged"},
    ],
)
def test_confirmation_rejects_untrusted_fields(tmp_path: Path, payload: dict[str, str]) -> None:
    app, cookie, collector, controller, clock, wakes = fixture(tmp_path)
    response, cookie = post(app, cookie, "/source-review/confirm", **payload)
    assert response.status == 400
    assert not wakes
    assert not collector.works
    response = app.handle(
        "POST", "/source-review/assess", request_headers(cookie), b"csrf_token=" + b"a" * 8192
    )
    assert response.status == 413
    assert not wakes
