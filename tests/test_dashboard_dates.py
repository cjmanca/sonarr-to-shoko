"""Shared compact dates retain precise evidence and use local browser date parts."""

import json
import os
import shutil
import subprocess
from dataclasses import replace
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path

import pytest
import test_dashboard as dashboard
from test_dashboard_action_history import action
from test_dashboard_routes import login, request_headers
from test_dashboard_source_review import fixture, get, post

from shoko_linker.action_history import ActionHistoryPage
from shoko_linker.dashboard import (
    _DASHBOARD_JS,
    DashboardTable,
    _compact_time,
)
from shoko_linker.metadata_status import MetadataStatusExample, MetadataStatusView
from shoko_linker.scan_status import ScanSummary
from shoko_linker.state import ServiceFileCounts, table_page

NOW = datetime(2026, 10, 30, 12, tzinfo=UTC)


class Text(HTMLParser):
    def __init__(self, markup: str) -> None:
        super().__init__()
        self.parts: list[str] = []
        self.feed(markup)

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2026-10-30T12:00:00Z", "12:00"),
        ("2026-10-29T12:00:01+00:00", "12:00"),
        ("2026-10-29T12:00:00+00:00", "2026-10-29"),
        ("2026-10-31T11:59:59+00:00", "11:59"),
        ("2026-10-31T12:00:00+00:00", "2026-10-31"),
        ("2027-10-31T12:00:00+00:00", "2027-10-31"),
        ("2026-10-30T06:15:00-06:00", "12:15"),
        (None, "Unavailable"),
        ("", "Unavailable"),
        ("2026-10-30T12:00:00", "Unavailable"),
        ("2026-02-30T12:00:00Z", "Unavailable"),
        ('2026-10-30T12:00:00Z" onclick="alert(1)', "Unavailable"),
        ("<script>secret</script>", "Unavailable"),
    ],
)
def test_utc_fallback_boundaries_and_invalid_input(value: str | None, expected: str) -> None:
    markup = _compact_time(value, now=NOW)
    assert "".join(Text(markup).parts) == expected
    if expected != "Unavailable":
        assert 'datetime="' in markup
        assert '(UTC)"' in markup
        assert 'aria-label="' in markup
    else:
        assert markup == "Unavailable"


def test_authenticated_overview_and_history_use_shared_times(tmp_path: Path) -> None:
    app = dashboard.make_app(tmp_path)
    app.clock = lambda: NOW
    status = app.status_provider()
    stamp = "2026-10-30T11:30:00+00:00"
    future = "2026-10-31T11:30:00+00:00"
    app.status_provider = lambda: replace(
        status,
        mapping_last_check_iso=stamp,
        mapping_next_check_iso=future,
        last_scan=ScanSummary("context", stamp, stamp, 2, 2, 1, 1, 1),
        metadata=MetadataStatusView(
            examples=(
                MetadataStatusExample(1, None, "File", "", "", "waiting", "Waiting", stamp, future),
            )
        ),
    )
    counts = ServiceFileCounts(0, 0, 0)
    app.table_provider = lambda *_: DashboardTable(
        history=ActionHistoryPage(
            table_page(1, 1, 50),
            counts,
            (replace(action(), requested_iso=stamp, recorded_iso=stamp, message="<waiting>"),),
        )
    )
    cookie = login(app)
    overview = app.handle("GET", "/status", request_headers(cookie)).body.decode()
    # Count only body markup, excluding the stylesheet/script's selector strings.
    main = overview.split("<main", 1)[1].split("</main>", 1)[0]
    assert main.count("data-compact-time") == 6
    assert "Times shown in UTC" in main
    visible = "".join(Text(main).parts)
    assert "11:30" in visible
    assert stamp not in visible
    assert future not in visible
    assert stamp in main  # retained full machine-readable and accessible precision
    history = app.handle("GET", "/history", request_headers(cookie)).body.decode()
    panel = history.split('data-live="table"', 1)[1].split("</section>", 1)[0]
    assert panel.count("data-compact-time") == 2
    assert stamp not in "".join(Text(panel).parts)
    assert "&lt;waiting&gt;" in panel
    assert "<waiting>" not in panel


def test_recovery_uses_compact_times_with_injected_request_clock(tmp_path: Path) -> None:
    app, cookie, _, controller, clock, _ = fixture(tmp_path)
    response, cookie = post(app, cookie, "/source-review/assess")
    assert response.status == 303
    controller.worker_tick(clock.mono + 60)
    markup = get(app, cookie).split('id="source-review"', 1)[1].split("</section>", 1)[0]
    for label in (
        "First hold:",
        "previous observation:",
        "Latest complete observation:",
        "Fresh collection started:",
        "completed:",
    ):
        assert label in markup
    assert markup.count("data-compact-time") == 5
    assert "+00:00" not in "".join(Text(markup).parts)


def test_production_browser_formatter_local_dates_dst_and_poll_transitions() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node unavailable for production JavaScript verification")
    script = r"""
const assert = require('node:assert/strict');
const vm = require('node:vm');
let now = Date.parse('2026-10-30T12:00:00Z');
class Clock extends Date { static now() { return now; } }
const timers = [];
function time(value) {
  return {textContent: '', attrs: {datetime: value},
    getAttribute(key) { return this.attrs[key]; },
    setAttribute(key, value) { this.attrs[key] = value; }};
}
let visible = [time('2026-10-29T12:00:01+00:00')];
const zone = {textContent: ''};
function times(selector) {
  if (selector === 'time[data-compact-time]') return visible;
  if (selector === '[data-timezone]') return [zone];
  return [];
}
const notice = {};
const document = {getElementById() { return notice; }, querySelector() { return null; },
  querySelectorAll: times, addEventListener() {}};
const incoming = time('2026-10-31T11:59:59+00:00');
const next = {querySelector(selector) {
  return selector === '[data-live="metrics"]' ? {} : null;
}, querySelectorAll(selector) {
  return selector === 'time[data-compact-time]' ? [incoming] : [];
}};
const window = {location: {pathname: '/status', search: ''}, addEventListener() {},
  setTimeout(fn) { timers.push(fn); }};
const context = {document, window, Date: Clock, AbortSignal,
  DOMParser: class {parseFromString() { return next; }},
  fetch: async () => ({ok: true, text: async () => ''})};
vm.createContext(context);
vm.runInContext(PRODUCTION, context);
assert.equal(visible[0].textContent, '06:00');
assert.equal(zone.textContent, 'Times shown in America/Edmonton.');
assert.match(visible[0].attrs['aria-label'], /America\/Edmonton/);
assert.match(visible[0].attrs.title, /2026-10-29T12:00:01\+00:00/);
(async () => {
  now += 1000;
  await timers.shift()();
  assert.equal(visible[0].textContent, '2026-10-29'); // existing DOM crossed exactly 24h
  assert.equal(incoming.textContent, '05:59'); // fresh poll DOM is formatted too
  const productionFormatter = PRODUCTION.split('  function prepareTimes')[1]
    .split('  prepareTimes(document);')[0];
  vm.runInContext('function prepareTimes' + productionFormatter, context);
  for (const [value, current, expected] of [
    ['2026-10-29T01:00:00+00:00', '2026-10-30T12:00:00Z', '2026-10-28'],
    ['2026-11-01T07:30:00Z', '2026-11-01T12:00:00Z', '01:30'],
    ['2026-11-01T08:30:00Z', '2026-11-01T12:00:00Z', '01:30'],
    ['2026-03-08T08:30:00Z', '2026-03-08T12:00:00Z', '01:30'],
    ['2026-03-08T09:30:00Z', '2026-03-08T12:00:00Z', '03:30'],
    ['2026-10-31T12:00:00Z', '2026-10-30T12:00:00Z', '2026-10-31'],
    ['2026-10-30T06:15:00-06:00', '2026-10-30T12:00:00Z', '06:15']
  ]) {
    visible = [time(value)];
    context.prepareTimes(document, Date.parse(current));
    assert.equal(visible[0].textContent, expected);
  }
  visible = [time('invalid')];
  context.prepareTimes(document, now);
  assert.equal(visible[0].textContent, 'Unavailable');
  assert.equal(timers.length, 1); // reuse the one existing poll timer
})().catch(error => { console.error(error); process.exitCode = 1; });
""".replace("PRODUCTION", json.dumps(_DASHBOARD_JS))
    result = subprocess.run(  # noqa: S603 - fixed script and resolved local Node executable
        [node, "-"],
        input=script,
        text=True,
        capture_output=True,
        env={**os.environ, "TZ": "America/Edmonton"},
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
