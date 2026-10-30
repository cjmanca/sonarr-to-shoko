"""Authenticated file tables expose retained reasons without claiming fresh checks."""

import html
from pathlib import Path

import pytest
from test_dashboard_routes import login, request_headers, runtime_app
from test_dashboard_source_review import fixture
from test_state import LIMITS, walk_to_planned

from shoko_linker.dashboard import DashboardTable
from shoko_linker.logging_setup import register_secret
from shoko_linker.state import open_state


def test_authenticated_tables_show_distinct_recorded_reasons_safely(tmp_path: Path) -> None:
    app, requests, endpoint = runtime_app(tmp_path)
    secret = "recorded-reason-registered-secret"
    register_secret(secret)
    unsafe = (
        f'episode_mapping_missing: <img src=x onerror="alert(1)"> {secret} '
        "https://private.invalid/?apikey=unregistered-value \x00\x1b\n"
    )
    conflict = "existing_link_conflict: Shoko episode associations differ from the mapped target"
    try:
        with open_state(app.state_path, budget_limits=LIMITS) as store:
            for file_id, detail in ((1, unsafe), (104, conflict)):
                store.connection.execute(
                    "UPDATE files SET state_detail=? WHERE shoko_file_id=?", (detail, file_id)
                )
        assert app.handle("GET", "/files/waiting", {}).status == 303
        assert app.handle("GET", "/issues", {}).status == 303
        headers = request_headers(login(app))
        waiting = app.handle("GET", "/files/waiting", headers).body.decode()
        review = app.handle("GET", "/issues", headers).body.decode()
        assert 'data-disclosure="file-reason-1"' in waiting
        assert "<summary>Recorded reason</summary>" in waiting
        assert "Last recorded file detail; this is not a fresh check" in waiting
        assert "Current service holds are shown separately" in waiting
        assert waiting.count("episode_mapping_missing:") == 1
        assert "&lt;img src=x onerror=&quot;alert(1)&quot;&gt;" in waiting
        assert "***REDACTED***" in waiting
        assert "[URL omitted]" in waiting
        for value in (secret, "private.invalid", "unregistered-value", "\x00", "\x1b"):
            assert value not in waiting
        assert "<img src=x" not in waiting
        assert 'data-disclosure="file-reason-104"' in review
        assert review.count(conflict) == 1
        # The existing category remains both visible and the accessible icon explanation.
        for text, category in (
            (
                waiting,
                "Not currently confirmed complete; waiting for a safe retry or prerequisite.",
            ),
            (
                review,
                "Conflicting source information or existing episode associations need review.",
            ),
        ):
            assert f'title="{category}"' in text
            assert f'<span class="sr-only">{category}</span>' in text
            assert f"<td>{category}<details" in text
        assert requests == []
    finally:
        endpoint.client.close()


@pytest.mark.parametrize(
    "detail",
    [
        "",
        "planned",
        "new",
        "  planned  ",
        "Not currently confirmed complete; waiting for a safe retry or prerequisite.",
    ],
)
def test_empty_or_generic_file_details_do_not_add_disclosures(tmp_path: Path, detail: str) -> None:
    app, requests, endpoint = runtime_app(tmp_path)
    try:
        with open_state(app.state_path, budget_limits=LIMITS) as store:
            store.connection.execute(
                "UPDATE files SET state_detail=? WHERE shoko_file_id=1", (detail,)
            )
        text = app.handle("GET", "/files/waiting", request_headers(login(app))).body.decode()
        assert 'data-disclosure="file-reason-' not in text
        assert requests == []
    finally:
        endpoint.client.close()


def test_preflight_detail_is_historical_and_keeps_observed_time(tmp_path: Path) -> None:
    app, requests, endpoint = runtime_app(tmp_path)
    try:
        with open_state(app.state_path, budget_limits=LIMITS) as store:
            walk_to_planned(store, file_id=201)
            targets: dict[str, tuple[int | None, int | None]] = {"source": (100, 200)}
            store.remember_mapping_targets_bulk({201: targets})
            record = store.get_file(201)
            assert record is not None
            assert store.record_preflight_deferral(
                record,
                expected_targets=targets,
                code="target_unavailable",
                message="Shoko episode metadata could not be read",
                observed_iso="2026-10-29T12:01:00+00:00",
            )
            expected = store.service_file_page("waiting", page=3, page_size=50)
            recorded = next(row for row in expected.rows if row.shoko_file_id == 201)
        text = app.handle(
            "GET", "/files/waiting?page=3&page_size=50", request_headers(login(app))
        ).body.decode()
        assert text.count(html.escape(recorded.detail)) == 1
        assert "observed 2026-10-29T12:01:00+00:00 (last attempted preflight)" in text
        assert "Last recorded file detail; this is not a fresh check" in text
        assert "Not submitted; awaiting planning or a safe linking opportunity." in text
        assert 'data-disclosure="file-reason-201"' in text
        assert requests == []
    finally:
        endpoint.client.close()


def test_unvisited_global_hold_is_separate_from_recorded_file_reason(tmp_path: Path) -> None:
    app, cookie, collector, _controller, _clock, wakes = fixture(tmp_path)
    with open_state(app.state_path) as store:
        walk_to_planned(store)
        store.connection.execute(
            "UPDATE files SET state_detail=? WHERE shoko_file_id=101",
            ("mapping_incomplete: TVDB episode mapping was unavailable",),
        )

    def table(_kind: str, page: int, size: int) -> DashboardTable:
        with open_state(app.state_path) as store:
            files = store.service_file_page("waiting", page=page, page_size=size)
        return DashboardTable(files=files, counts=files.counts)

    app.table_provider = table
    text = app.handle("GET", "/files/waiting", request_headers(cookie)).body.decode()
    assert 'data-live="source-hold"' in text
    hold = text.split('data-live="source-hold">', 1)[1].split("</div>", 1)[0]
    assert "Library changes need review." in hold
    assert '<a href="/status#source-review">Review library changes</a>' in hold
    reason = text.split('data-disclosure="file-reason-101"', 1)[1].split("</details>", 1)[0]
    assert "TVDB episode mapping was unavailable" in reason
    assert "Last recorded file detail; this is not a fresh check" in reason
    assert "Source collection changed" not in reason
    assert "Library changes need review" not in reason
    assert "/status#source-review" not in reason
    assert collector.capture_calls == 0
    assert not wakes
