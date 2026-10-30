"""Poll rendered GET views even when a control POST leaves its URL in the browser."""

import html
import json
import re
import shutil
import subprocess
from pathlib import Path
from urllib.parse import urlencode

import pytest
import test_dashboard as dashboard
from test_dashboard_routes import login, request_headers

from shoko_linker.bootstrap import BootstrapSettings
from shoko_linker.config import SecretRef
from shoko_linker.dashboard import _DASHBOARD_JS, DashboardSetupView
from shoko_linker.service_settings import ServiceSettingsController


def canonical(document: str) -> str:
    match = re.search(r'data-refresh-url="([^"]+)"', document)
    assert match is not None
    return html.unescape(match.group(1))


def run_poller(cases: list[dict[str, str | None]]) -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is unavailable for the dashboard JavaScript regression")
    script = r"""
const assert = require('node:assert/strict');
const vm = require('node:vm');
(async () => {
  for (const scenario of CASES) {
    const timers = [], requested = [];
    const notice = {hidden: true, textContent: ''};
    const panel = {dataset: {loading: 'false', setupDigest: 'same confirmed facts'}};
    const setup = {
      dataset: {live: 'setup'}, selectedLibrary: '/my/selected/library',
      querySelector(selector) { return selector === '[data-setup-digest]' ? panel : null; },
      querySelectorAll() { return []; },
      replaceWith() { throw Error('Existing setup choices must remain intact'); }
    };
    function refresh(value) { return value === null ? null : {dataset: {refreshUrl: value}}; }
    const document = {
      addEventListener() {},
      getElementById() { return notice; },
      querySelector(selector) {
        assert.equal(selector, '[data-refresh-url]'); return refresh(scenario.canonical);
      },
      querySelectorAll(selector) {
    if (['time[data-compact-time]', '[data-timezone]', 'table[data-table]']
        .includes(selector)) return [];
    assert.equal(selector, '[data-live]'); return [setup]; }
    };
    class DOMParser {
      parseFromString() {
        return {querySelectorAll() { return []; }, querySelector(selector) {
          if (selector === '[data-live="metrics"]') return {};
          if (selector === 'input[name="csrf_token"]') return null;
          if (selector === '[data-live="setup"]') return setup;
          if (selector === '[data-refresh-url]') return refresh(scenario.nextCanonical);
          throw Error('Unexpected selector ' + selector);
        }};
      }
    }
    const url = scenario.location.split('?');
    const window = {addEventListener() {},
      location: {pathname: url[0], search: url[1] ? '?' + url[1] : ''},
      setTimeout(fn) { timers.push(fn); }};
    const fetch = async value => {
      requested.push(value); return {ok: true, text: async () => ''};
    };
    vm.runInNewContext(PRODUCTION_SCRIPT, {document, window, DOMParser, fetch, AbortSignal});
    await timers.shift()();
    await timers.shift()();
    assert.deepEqual(requested, [scenario.expected, scenario.secondExpected || scenario.expected]);
    assert.equal(notice.textContent, '');
    assert.equal(setup.selectedLibrary, '/my/selected/library');
  }
})().catch(error => { console.error(error); process.exitCode = 1; });
"""
    script = script.replace("CASES", json.dumps(cases)).replace(
        "PRODUCTION_SCRIPT", json.dumps(_DASHBOARD_JS)
    )
    result = subprocess.run(  # noqa: S603 - resolved Node and fixed local script; no shell
        [node, "-"],
        input=script,
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("post_path", ["/setup/confirm", "/setup/scope", "/setup/edit", "/resume"])
def test_actual_200_post_renders_get_canonical_and_poller_keeps_working(
    tmp_path: Path,
    post_path: str,
) -> None:
    app = dashboard.make_app(tmp_path, onboarding=True)
    app.setup_action = lambda *_: "Setup saved."
    app.setup_provider = lambda: DashboardSetupView(
        "Keep choices", ("/anime",), selected_roots=("/anime",)
    )
    app.onboarding_context = None
    cookie = login(app)
    initial = app.handle("GET", "/status", request_headers(cookie)).body.decode()
    response = app.handle(
        "POST",
        post_path,
        request_headers(cookie),
        urlencode(
            {
                "csrf_token": dashboard.csrf_from(initial),
            }
        ).encode(),
    )
    assert response.status == 200
    assert "Location" not in dict(response.headers)
    document = response.body.decode()
    refresh_url = canonical(document)
    assert refresh_url == "/status"
    if post_path == "/resume":
        assert "Complete setup before automatic linking can resume" in document
    cookie = dict(response.headers)["Set-Cookie"].split(";", 1)[0]
    assert app.handle("GET", refresh_url, request_headers(cookie)).status == 200
    assert app.handle("GET", post_path, request_headers(cookie)).status == 404
    run_poller(
        [
            {
                "location": post_path,
                "canonical": refresh_url,
                "nextCanonical": refresh_url,
                "expected": "/status",
            }
        ]
    )


def test_poller_rejects_external_post_and_bad_query_targets_and_reuses_fresh_canonical() -> None:
    cases: list[dict[str, str | None]] = []
    for value in (
        "https://evil.invalid/status",
        "//evil.invalid/status",
        "/setup/confirm",
        "/issues?page=0",
        "/issues?page=1&page=2",
        "/status?view=review",
        "/history?group=verified",
        "/issues?page_size=101",
    ):
        cases.append(
            {
                "location": "/setup/confirm",
                "canonical": value,
                "nextCanonical": value,
                "expected": "/status",
            }
        )
    cases.extend(
        [
            {
                "location": "/resume",
                "canonical": None,
                "nextCanonical": None,
                "expected": "/status",
            },
            {
                "location": "/issues?view=exclusions&page=2",
                "canonical": None,
                "nextCanonical": None,
                "expected": "/issues?view=exclusions&page=2",
            },
            {
                "location": "/settings",
                "canonical": "/settings",
                "nextCanonical": "/settings",
                "expected": "/settings",
            },
            {
                "location": "/setup/confirm",
                "canonical": "/files/waiting?page=2&page_size=50",
                "nextCanonical": "/files/waiting?page=1&page_size=50",
                "expected": "/files/waiting?page=2&page_size=50",
                "secondExpected": "/files/waiting?page=1&page_size=50",
            },
        ]
    )
    run_poller(cases)


def test_rendered_canonical_keeps_valid_table_query_and_settings_route(tmp_path: Path) -> None:
    app = dashboard.make_app(tmp_path)
    cookie = login(app)
    for route, expected in (
        (
            "/issues?view=exclusions&page=2&page_size=50",
            "/issues?view=exclusions&page=2&page_size=50",
        ),
        ("/files/verified?page=%32", "/files/verified?page=2"),
        ("/settings", "/settings"),
    ):
        response = app.handle("GET", route, request_headers(cookie))
        assert response.status == 200
        assert canonical(response.body.decode()) == expected


@pytest.mark.parametrize("interval", ["60", "invalid"])
def test_settings_post_saved_or_validation_error_keeps_settings_canonical(
    tmp_path: Path,
    interval: str,
) -> None:
    app = dashboard.make_app(tmp_path)
    bootstrap = BootstrapSettings(
        "http://sonarr:8989",
        SecretRef("SONARR_API_KEY"),
        "http://shoko:8111",
        SecretRef("SHOKO_API_KEY"),
        app.state_path,
    )
    preferences = ServiceSettingsController(bootstrap, lambda: None)
    app.settings_provider = preferences.view
    app.settings_action = preferences.save
    cookie = login(app)
    initial = app.handle("GET", "/settings", request_headers(cookie)).body.decode()
    view = preferences.view()
    form = {name: value for name, _label, value in view.values}
    form.update(
        csrf_token=dashboard.csrf_from(initial),
        settings_digest=view.digest,
        advanced_yaml=view.advanced_yaml,
        interval_seconds=interval,
    )
    response = app.handle("POST", "/settings", request_headers(cookie), urlencode(form).encode())
    assert response.status == 200
    document = response.body.decode()
    assert canonical(document) == "/settings"
    assert dashboard.csrf_from(document) != form["csrf_token"]
    assert ("Settings saved." in document) == (interval == "60")
    run_poller(
        [
            {
                "location": "/settings",
                "canonical": canonical(document),
                "nextCanonical": "/settings",
                "expected": "/settings",
            }
        ]
    )
