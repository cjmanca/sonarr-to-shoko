"""Ordinary authenticated metadata progress uses bounded local observations."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import httpx
import test_dashboard as dashboard

from shoko_linker.dashboard import DashboardApplication
from shoko_linker.logging_setup import register_secret
from shoko_linker.metadata_status import MAX_EXAMPLES, MetadataStatusExample, MetadataStatusView


def example(state: str, message: str) -> MetadataStatusExample:
    return MetadataStatusExample(
        anime_id=18208,
        shoko_file_id=149,
        filename="Show - S01E01.mkv",
        sonarr_path="/data/anime/Show/Show - S01E01.mkv",
        shoko_path="/anime/Show/Show - S01E01.mkv",
        state=state,
        message=message,
        observed_iso="2026-10-29T12:00:00+00:00",
    )


def test_authenticated_initial_and_poll_metadata_progress_keeps_setup_closed(
    tmp_path: Path,
) -> None:
    app = dashboard.make_app(tmp_path, onboarding=True)
    current = app.status_provider()
    app.status_provider = lambda: current
    # Confirmed setup stays absent while observations change; metadata does not
    # own the setup provider or open a setup dialog during live HTML replacement.
    app.setup_provider = lambda: None
    server, thread, base_url = dashboard.start_server(app)
    try:
        with httpx.Client(follow_redirects=False) as client:
            assert client.get(f"{base_url}/status").status_code == 303
            assert dashboard.setup_account(client, base_url).status_code == 303
            cases = (
                (
                    False,
                    "disabled",
                    "Retrieval off",
                    "Enable retrieval in Settings and confirm setup.",
                ),
                (True, "accepted", "Accepted; awaiting metadata", "Shoko accepted the request."),
                (True, "uncertain", "Delivery uncertain", "Read-only checks; no automatic replay."),
                (
                    True,
                    "waiting_queue",
                    "Waiting for Shoko metadata work",
                    "Waiting for Shoko work.",
                ),
                (True, "queue_unknown", "Queue visibility incomplete", "New requests wait."),
                (True, "ready", "Metadata ready; file plan will be checked", "Metadata is usable."),
            )
            for enabled, state, label, message in cases:
                current = replace(
                    current,
                    metadata=MetadataStatusView(
                        enabled, f"Retrieval {enabled}.", (example(state, message),)
                    ),
                )
                for headers in ({}, {"Cache-Control": "no-cache"}):
                    page = client.get(f"{base_url}/status", headers=headers)
                    assert page.status_code == 200
                    section = page.text.split('data-live="metadata"', 1)[1].split("</section>", 1)[
                        0
                    ]
                    assert label in section
                    assert message in section
                    assert "Show - S01E01.mkv" in section
                    assert "/data/anime/Show/Show - S01E01.mkv" in section
                    assert "/anime/Show/Show - S01E01.mkv" in section
                    assert "AniDB anime ID</dt><dd>18208" in section
                    assert "Shoko file ID</dt><dd>149" in section
                    assert "Last observation" in section
                    assert "Next read-only check" not in section
                    assert "not a complete pending-work count" in section
                    assert "does not mean the file has been linked" in section
                    assert ("Queue/Items" in section) == (state == "queue_unknown")
                    assert 'data-live="setup"></div>' in page.text
                    assert "Set up automatic linking" not in page.text
                    assert "Linking controls" in page.text
                    assert "&#x4C;" not in page.text
        with httpx.Client() as visitor:
            response = visitor.get(f"{base_url}/status")
            assert response.status_code == 303
            assert "/anime/Show" not in response.text
    finally:
        dashboard.stop_server(server, thread)


def test_metadata_panel_bounds_examples_and_does_not_invent_missing_context() -> None:
    missing = replace(
        example("uncertain", "Only read-only checks; no automatic replay."),
        filename="",
        sonarr_path="",
        shoko_path="",
        shoko_file_id=None,
        observed_iso="",
    )
    section = DashboardApplication._metadata_section(
        MetadataStatusView(
            examples=(missing,) * MAX_EXAMPLES + (example("ready", "overflow marker"),)
        )
    )
    assert section.count("<li>") == MAX_EXAMPLES
    assert "overflow marker" not in section
    assert "File context unavailable" in section
    assert "Unavailable" in section
    assert "Shoko file ID" not in section
    assert "Last observation" not in section
    assert "Next read-only check" not in section
    assert 'href="/settings"' in section
    assert "requires setup confirmation" in section
    assert "cannot cancel accepted Shoko jobs" in section


def test_metadata_panel_scrubs_legacy_context_then_escapes_every_text_field() -> None:
    secret = "metadata-dashboard-private-key-sentinel"
    register_secret(secret)
    hostile = f'<img src=x onerror="bad"> & {secret} https://private.invalid/token\x01'
    row = replace(
        example("ready", hostile),
        filename=hostile,
        sonarr_path=hostile,
        shoko_path=hostile,
        observed_iso=hostile,
        next_read_only_check_iso=hostile,
    )
    section = DashboardApplication._metadata_section(MetadataStatusView(True, hostile, (row,)))
    assert "<img" not in section
    assert "&lt;img" in section
    assert "&amp;" in section
    assert secret not in section
    assert "private.invalid" not in section
    assert "\x01" not in section
    assert "Next read-only check" in section
    assert "Last observation" in section
    assert "[URL omitted]" in section


def test_empty_metadata_panel_uses_off_default_without_claiming_work_started() -> None:
    section = DashboardApplication._metadata_section(MetadataStatusView())
    assert "Automatic missing-metadata retrieval is off" in section
    assert "No metadata observations yet" in section
    assert "Previously accepted Shoko jobs still receive read-only verification" in section
    assert "Next read-only check" not in section
