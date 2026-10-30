"""Authenticated request history is bounded local evidence, not a media audit."""

from dataclasses import replace
from pathlib import Path

import httpx
import test_dashboard as dashboard

from shoko_linker.action_history import ActionHistoryPage, RecentAction
from shoko_linker.dashboard import DashboardApplication, DashboardTable
from shoko_linker.logging_setup import register_secret
from shoko_linker.state import ServiceFileCounts, table_page


def action(index: int = 1) -> RecentAction:
    return RecentAction(
        f"file:{index}",
        "file_link",
        index,
        "2026-10-29T12:00:00+00:00",
        "2026-10-29T12:01:00+00:00",
        "accepted_unverified",
        "Shoko accepted the request; verified linking is not yet recorded.",
        f"Episode-{index}.mkv",
        f"/sonarr/Show/Episode-{index}.mkv",
        200,
    )


def test_history_status_and_poll_show_actual_outcomes_with_scope_limits(tmp_path: Path) -> None:
    app = dashboard.make_app(tmp_path, onboarding=True)
    history = ActionHistoryPage(
        table_page(2, 1, 50),
        ServiceFileCounts(12, 4, 2),
        (
            action(),
            replace(
                action(2),
                action_id="metadata:claim",
                operation="metadata_refresh",
                subject_id=18208,
                state="ready",
                filename="",
                current_path="",
                requested_iso="",
                recorded_iso="",
                message="Required metadata was verified available. "
                "This does not prove this request "
                "created it or that any file was linked.",
                http_status=None,
            ),
        ),
    )
    app.table_provider = lambda *_: DashboardTable(history=history, counts=history.counts)
    app.setup_provider = lambda: None
    server, thread, base = dashboard.start_server(app)
    try:
        with httpx.Client(follow_redirects=False) as client:
            assert client.get(f"{base}/history").status_code == 303
            assert dashboard.setup_account(client, base).status_code == 303
            for headers in ({}, {"Cache-Control": "no-cache"}):
                response = client.get(f"{base}/history", headers=headers)
                assert response.status_code == 200
                panel = response.text.split('data-live="table"', 1)[1].split("</section>", 1)[0]
                assert "Showing 1&ndash;2 of 2" in panel
                assert "Episode-1.mkv" in panel
                assert "Link request:" in panel
                assert "verified linking is not yet recorded" in panel
                assert "2026-10-29T12:00:00+00:00" in panel
                assert "2026-10-29T12:01:00+00:00" in panel
                assert "Paths are current retained context, not historical path snapshots" in panel
                assert "AniDB anime 18208" in panel
                assert "Metadata retrieval:" in panel
                assert "or that any file was linked" in panel
                assert "Unavailable" in panel
                assert "all configured server sources and modes" in panel
                assert "not an audit of media changes or Shoko hooks" in panel
                assert "accepted_unverified" not in panel
                assert "Set up automatic linking" not in response.text
    finally:
        dashboard.stop_server(server, thread)


def test_history_render_bounds_sanitizes_and_describes_empty_records_truthfully() -> None:
    secret = "action-history-dashboard-secret-sentinel"
    assert register_secret(secret)
    unsafe = replace(
        action(),
        filename="<file>.mkv",
        current_path=f"/media/{secret}/<file>",
        message=f"<pending> https://host/?key={secret}",
        action_id='file:"unsafe',
    )
    history = ActionHistoryPage(
        table_page(25, 1, 20),
        ServiceFileCounts(0, 0, 0),
        (unsafe, *(action(i) for i in range(2, 21))),
    )
    panel = DashboardApplication._table_section(
        "/history", "review", DashboardTable(history=history)
    )
    assert secret not in panel
    assert "&lt;file&gt;.mkv" in panel
    assert "&lt;pending&gt; [URL omitted]" in panel
    assert "Showing 1&ndash;20 of 25" in panel
    assert "Episode-20.mkv" in panel
    assert "Episode-21.mkv" not in panel
    history = replace(history, pagination=table_page(0, 1, 50), rows=())
    empty = DashboardApplication._table_section(
        "/history", "review", DashboardTable(history=history)
    )
    assert "No records in this view" in empty
    assert "No media changes" not in empty
