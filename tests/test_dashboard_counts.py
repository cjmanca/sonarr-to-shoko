"""Dashboard counters, local work details, and scan evidence have distinct meanings."""

from dataclasses import replace
from pathlib import Path

import httpx
import test_dashboard as dashboard

from shoko_linker.dashboard import DashboardApplication, DashboardStatus
from shoko_linker.scan_status import ScanSummary
from shoko_linker.state import (
    ServiceFileCounts,
    ServiceFileExample,
    ServiceFileGroup,
    ServiceFileStatus,
)


def example(index: int, reason: str = "metadata") -> ServiceFileExample:
    return ServiceFileExample(
        9000 + index,
        "waiting",
        reason,
        "Unused provider text",
        f"Episode-{index}.mkv",
        f"/sonarr/Show/Episode-{index}.mkv",
        (f"/shoko/Show/Episode-{index}.mkv",),
        "Internal code",
        "2026-10-29T12:00:00+00:00",
    )


def status() -> DashboardStatus:
    return DashboardStatus(
        "Connected",
        2,
        36,
        8,
        True,
        "No action needed.",
        excluded_files=18,
        scope_excluded_files=4,
        exclusion_counts=(("outside_allowed_root", 4), ("size_mismatch", 14)),
        skipped_file_details=tuple(f"Excluded-{i}.mkv: size differs" for i in range(18)),
        tracked_files=ServiceFileStatus(
            ServiceFileCounts(2, 36, 8),
            ServiceFileGroup(
                36, (("metadata", 30), ("verification", 6)), tuple(example(i) for i in range(20))
            ),
            ServiceFileGroup(8, (("conflict", 8),), (example(100, "conflict"),)),
        ),
        last_scan=ScanSummary(
            "a" * 64, "2026-10-29T12:00:00+00:00", "2026-10-29T12:01:00+00:00", 58, 58, 40, 18, 4
        ),
    )


def test_overview_and_poll_distinguish_tracked_totals_from_58_scan_files(tmp_path: Path) -> None:
    app = dashboard.make_app(tmp_path, onboarding=True)
    app.status_provider = status
    app.setup_provider = lambda: None
    server, thread, base = dashboard.start_server(app)
    try:
        with httpx.Client(follow_redirects=False) as client:
            assert client.get(f"{base}/status").status_code == 303
            assert dashboard.setup_account(client, base).status_code == 303
            for headers in ({}, {"Cache-Control": "no-cache"}):
                response = client.get(f"{base}/status", headers=headers)
                assert response.status_code == 200
                page = response.text
                assert 'title="Waiting linker work"' in page
                assert "</span>36</a>" in page
                assert 'title="Needs review"' in page
                assert "</span>8</a>" in page
                assert "It can remain nonzero when Shoko&#x27;s queue is empty" not in page
                assert "It can remain nonzero when Shoko's queue is empty" in page
                scan = page.split('data-live="last-scan"', 1)[1].split("</section>", 1)[0]
                assert "Shoko reported unrecognized files</dt><dd>58</dd>" in scan
                assert "Matched to selected Sonarr files</dt><dd>40</dd>" in scan
                assert "Excluded during scan</dt><dd>18</dd>" in scan
                assert "Outside scope (included in exclusions)</dt><dd>4</dd>" in scan
                assert "not a live file count or Shoko queue size" in scan
                assert "2026-10-29T12:01:00+00:00" in scan
                assert "<table" not in page
                assert "Set up automatic linking" not in page
    finally:
        dashboard.stop_server(server, thread)


def test_legacy_and_empty_status_do_not_invent_scan_evidence() -> None:
    legacy = DashboardStatus("Connected", 0, 36, 8, False, "Check setup.")
    page = DashboardApplication._status_page(legacy, "csrf", paused=True)
    assert "Scan totals are unavailable until a completed scan" in page
    assert "Shoko reported unrecognized files</dt><dd>0" not in page
    page = DashboardApplication._status_page(legacy, "csrf", paused=True, route="/issues")
    assert "File data is unavailable for this view" in page


def test_scan_preserves_unknown_total_and_explains_changing_paged_inventory() -> None:
    current = status()
    assert current.last_scan is not None
    current = replace(current, last_scan=replace(current.last_scan, reported_unrecognized=None))
    page = DashboardApplication._status_page(current, "csrf", paused=False)
    assert "Shoko reported unrecognized files</dt><dd>Unavailable" in page
    assert "can change while pages are scanned" in page
    assert "A Sonarr match does not mean a verified link" in page
