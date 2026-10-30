"""Ready-file queue refusals remain visible without metadata demand examples."""

from __future__ import annotations

import html
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest
import test_daemon as td
import test_dashboard as dashboard

from shoko_linker import cli
from shoko_linker.bootstrap import BootstrapSettings
from shoko_linker.config import SecretRef
from shoko_linker.daemon import CycleCounts, CycleMode, CycleReport, DaemonDeps
from shoko_linker.http import ApiEndpoint
from shoko_linker.mapping_store import MappingStore
from shoko_linker.onboarding import OnboardingManager
from shoko_linker.reporting import META_CYCLE_KEY
from shoko_linker.state import open_state


def queue_report(reason: str, *, progress: bool = False) -> CycleReport:
    return CycleReport(
        mode=CycleMode.APPLY,
        cycle_key="ready-file-queue-wait",
        alive=True,
        reconciliation_ready=True,
        apply_eligible=True,
        writes_suppressed=False,
        mapping_outcome=None,
        mapping_revision=1,
        counts=CycleCounts(planned=2, submitted=int(progress)),
        reasons=(reason,),
        drift=(),
        existing_link_drift=(),
        deferred_planned=1,
        failed=False,
    )


@pytest.mark.parametrize("reason", ["preflight_queue_unknown", "preflight_queue_blocked"])
@pytest.mark.parametrize("progress", [False, True])
def test_ready_only_queue_wait_reaches_authenticated_live_controls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reason: str, progress: bool
) -> None:
    app = dashboard.make_app(tmp_path, onboarding=True)
    binding = dashboard.scope_binding(dashboard.safe_assessment())
    bootstrap = BootstrapSettings(
        binding.sonarr_url,
        SecretRef("SONARR_API_KEY"),
        binding.shoko_url,
        SecretRef("SHOKO_API_KEY"),
        tmp_path / "state.sqlite3",
    )
    report = queue_report(reason, progress=progress)

    def complete_cycle(*_args: Any, **kwargs: Any) -> None:
        kwargs["on_cycle_complete"](report)

    monkeypatch.setattr(cli, "run_daemon", complete_cycle)
    with httpx.Client(
        transport=httpx.MockTransport(lambda _: pytest.fail("Unexpected remote I/O"))
    ) as remote:
        endpoint = ApiEndpoint(remote, lambda: {})
        runtime = cli._NormalServiceRuntime(bootstrap, endpoint, endpoint, cli.StopState())
        deps = cast(
            DaemonDeps,
            SimpleNamespace(mapping_store=MappingStore(tmp_path / "mappings"), overrides_text=None),
        )
        with open_state(bootstrap.database_path) as store:
            runtime._run_daemon_cycle(deps, store, td.make_settings(tmp_path))
            assert store.get_meta(META_CYCLE_KEY) == report.cycle_key
            assert store.get_service_pause() == (False, "")
            assert not OnboardingManager(store).reconfirmation_required()
        status = runtime.status()
        assert not status.automatic_linking
        assert not status.metadata.examples
        assert "ready;" not in status.next_action
        assert "automatically" in status.next_action
        assert ("Some files" in status.next_action) == progress
        if reason == "preflight_queue_unknown":
            assert "could not be checked completely" in status.next_action
            assert "administrator permissions" in status.next_action
            assert "connectivity" in status.next_action
            assert "read-only" in status.next_action
            assert "403" not in status.next_action
        else:
            assert "related Shoko jobs to finish" in status.next_action
            assert "administrator" not in status.next_action
        app.status_provider = runtime.status
        app.setup_provider = lambda: None
        server, thread, base_url = dashboard.start_server(app)
        try:
            with httpx.Client(follow_redirects=False) as client:
                assert client.get(f"{base_url}/status").status_code == 303
                assert dashboard.setup_account(client, base_url).status_code == 303
                for headers in ({}, {"Cache-Control": "no-cache"}):
                    page = client.get(f"{base_url}/status", headers=headers)
                    assert page.status_code == 200
                    assert (
                        f'<p class="next-action">{html.escape(status.next_action)}</p>' in page.text
                    )
                    assert "Set up automatic linking" not in page.text
        finally:
            dashboard.stop_server(server, thread)


@pytest.mark.parametrize(
    ("condition", "expected"),
    [
        ("setup", "Complete setup first."),
        ("pause", "Resume when you are ready"),
        (
            "source_continuity",
            "Source collection changed. Review source change on Overview "
            "before linking can continue.",
        ),
        ("writes_disabled", "stopped by a safety check"),
        ("failed", "service check needs attention"),
        ("mapping", "Verified mappings are unavailable."),
        ("canary", "Waiting for the initial link"),
    ],
)
def test_queue_wait_does_not_shadow_higher_priority_gates(
    tmp_path: Path, condition: str, expected: str
) -> None:
    report = queue_report("preflight_queue_unknown")
    with open_state(tmp_path / "state.sqlite3") as store:
        if condition == "pause":
            OnboardingManager(store).pause(iso=td.T0)
        elif condition == "source_continuity":
            store.set_meta("requires_rebaseline", "1")
        elif condition == "writes_disabled":
            store.disable_writes("test safety hold")
        elif condition == "failed":
            report = replace(report, failed=True)
        elif condition == "mapping":
            report = replace(
                report, writes_suppressed=True, reasons=(*report.reasons, "mapping_unavailable")
            )
        elif condition == "canary":
            store.record_service_canary(500, td.T0)
        active, message = cli._NormalServiceRuntime._operational_status(
            store,
            authorized=condition != "setup",
            next_action="Complete setup first.",
            report=report,
            mapping_action="Verified mappings are unavailable.",
        )
        assert not active
        assert expected in message
        assert "job queue" not in message


@pytest.mark.parametrize("reason", ["legacy queue unknown", "preflight_queue_unknown:server text"])
def test_unstructured_legacy_queue_text_is_not_interpreted(tmp_path: Path, reason: str) -> None:
    with open_state(tmp_path / "state.sqlite3") as store:
        assert cli._NormalServiceRuntime._operational_status(
            store,
            authorized=True,
            next_action="Existing generic guidance.",
            report=queue_report(reason),
        ) == (True, "Existing generic guidance.")
