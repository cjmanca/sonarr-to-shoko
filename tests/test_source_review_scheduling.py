"""Offline review scheduling through the production runtime and threaded worker loop."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from http.server import HTTPServer
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
from test_source_rebaseline import SESSION, T1, setup
from test_source_review import Clock, Collector

from shoko_linker import cli
from shoko_linker.bootstrap import BootstrapSettings
from shoko_linker.config import SecretRef
from shoko_linker.http import ApiEndpoint
from shoko_linker.service import run_dashboard_worker
from shoko_linker.service_settings import ServiceSettingsController, load_service_settings
from shoko_linker.source_review import SourceReviewController
from shoko_linker.state import META_SERVICE_SETTINGS, open_state


@dataclass
class ScheduledReview:
    runtime: cli._NormalServiceRuntime
    clock: Clock
    collector: Collector
    waits: list[float] = field(default_factory=list)
    wake: threading.Event = field(default_factory=threading.Event)
    on_wait: Callable[[float], bool] = lambda _: False

    def run(self) -> None:
        finished = threading.Event()
        review = self

        class Wake(threading.Event):
            def is_set(self) -> bool:
                return review.wake.is_set()

            def clear(self) -> None:
                review.wake.clear()

            def set(self) -> None:
                review.wake.set()

            def wait(self, timeout: float | None = None) -> bool:
                if self.is_set():
                    return True
                assert timeout is not None
                assert timeout > 0
                review.waits.append(timeout)
                interrupted = review.on_wait(timeout)
                if review.runtime.state.should_stop():
                    finished.set()
                if interrupted:
                    return True
                if timeout >= 900:
                    review.runtime.state.stopped = True
                    finished.set()
                else:
                    review.clock.advance(int(timeout))
                return False

        class Server:
            timeout = 0.0
            closed = False

            def handle_request(self) -> None:
                assert finished.wait(5), "scheduled worker did not finish"

            def server_close(self) -> None:
                self.closed = True

        wake = Wake()
        # Startup itself schedules the first tick; consume its initial request wake.
        wake.clear()
        server = Server()
        preferences = ServiceSettingsController(self.runtime.settings, wake.set)
        run_dashboard_worker(
            cast(HTTPServer, server),
            worker_tick=self.runtime.worker_tick,
            stop_requested=self.runtime.state.should_stop,
            monotonic=lambda: self.clock.mono,
            worker_interval_seconds=300,
            interval_provider=preferences.interval,
            worker_wake=wake,
        )
        assert server.closed


@pytest.fixture
def scheduled_review(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[ScheduledReview]:
    path = tmp_path / "state.sqlite3"
    bootstrap = BootstrapSettings(
        "http://sonarr",
        SecretRef("SONARR_API_KEY"),
        "http://shoko",
        SecretRef("SHOKO_API_KEY"),
        path,
    )
    with open_state(path) as store:
        document = load_service_settings(store, bootstrap)
        document["sync"]["interval_seconds"] = 1000
        document["sync"]["max_cycle_seconds"] = 2
        setup(store)
        store.set_meta(META_SERVICE_SETTINGS, json.dumps(document))
        store.pause_service_for_user(T1)
    clock = Clock()
    endpoint = ApiEndpoint(httpx.Client(), lambda: {})
    runtime = cli._NormalServiceRuntime(bootstrap, endpoint, endpoint, cli.StopState())
    collector = Collector(clock, slices=3)
    result = ScheduledReview(runtime, clock, collector)
    controller = SourceReviewController(
        path,
        collector,
        wake=result.wake.set,
        shutdown_requested=runtime.state.should_stop,
        now=lambda: clock.now,
        monotonic=lambda: clock.mono,
        lifetime_seconds=5,
    )
    monkeypatch.setattr(time, "monotonic", lambda: clock.mono)

    def forbidden(*_args: object) -> None:
        pytest.fail("unconfigured ordinary work ran")

    monkeypatch.setattr(runtime, "_worker_tick", forbidden)
    monkeypatch.setattr(runtime, "_recover_blocked_metadata", forbidden)
    controller.request_assessment(SESSION)
    try:
        yield result
    finally:
        endpoint.client.close()


def test_pending_diagnostic_review_cannot_preempt_normal_worker_interval(
    scheduled_review: ScheduledReview, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = scheduled_review
    calls: list[str] = []
    monkeypatch.setattr(h.runtime, "_worker_tick", lambda: calls.append("ordinary"))
    monkeypatch.setattr(h.runtime, "_recover_blocked_metadata", lambda *_: calls.append("recovery"))
    h.run()
    assert calls == ["ordinary", "recovery"]
    assert h.waits == [1000]
    assert h.collector.works == []
    assert not hasattr(h.runtime, "source_review")
    with open_state(h.runtime.settings.database_path) as store:
        assert store.get_source_continuity_review().requires_rebaseline
        assert store.get_service_pause() == (True, "user_paused")


def test_actual_normal_worker_with_saved_consent_and_legacy_hold_processes_after_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import e2e_harness as eh
    from test_service_e2e import _confirm_setup, _endpoint, _service_world

    world = _service_world(tmp_path)
    bootstrap = BootstrapSettings(
        eh.SONARR_BASE,
        SecretRef("SONARR_API_KEY"),
        eh.SHOKO_BASE,
        SecretRef("SHOKO_API_KEY"),
        tmp_path / "state.sqlite3",
    )
    monkeypatch.setattr(cli, "configured_bootstrap", lambda _: bootstrap)
    monkeypatch.setattr(
        cli, "sonarr_endpoint", lambda _: _endpoint(world.sonarr.handler, eh.SONARR_BASE)
    )
    monkeypatch.setattr(
        cli, "shoko_endpoint", lambda _: _endpoint(world.shoko.handler, eh.SHOKO_BASE)
    )

    def mapping_endpoint(store: Any, **_kwargs: Any) -> ApiEndpoint:
        return ApiEndpoint(
            httpx.Client(
                base_url=store.origin, transport=httpx.MockTransport(world.source.handler)
            ),
            lambda: {"Accept-Encoding": "identity"},
        )

    monkeypatch.setattr(cli, "make_source_endpoint", mapping_endpoint)
    sonarr = _endpoint(world.sonarr.handler, eh.SONARR_BASE)
    shoko = _endpoint(world.shoko.handler, eh.SHOKO_BASE)
    clock = [datetime.now(UTC) + timedelta(seconds=1)]
    monkeypatch.setattr(cli, "current_iso", lambda: clock[0].isoformat())
    try:
        runtime = cli._NormalServiceRuntime(bootstrap, sonarr, shoko, cli.StopState())
        _confirm_setup(runtime.setup)
        with open_state(bootstrap.database_path) as store:
            store.check_source_continuity(
                "old", "old", iso=T1, roots_fingerprint="old", discovery_total=76
            )
            store.check_source_continuity(
                "old", "old", iso=T1, roots_fingerprint="old", discovery_total=69
            )
            consent = store.get_onboarding_json()
            hold = store.get_meta("source_continuity_hold_v1")
        assert not hasattr(runtime, "source_review")
        runtime.worker_tick()
        assert world.shoko.posts_count() == 0  # First observation still obeys grace.
        clock[0] += timedelta(seconds=301)
        runtime.worker_tick()
        assert world.shoko.posts_count() == 1
        restarted = cli._NormalServiceRuntime(bootstrap, sonarr, shoko, cli.StopState())
        startup = restarted.status()
        assert not startup.automatic_linking
        assert "Checking saved libraries" in startup.next_action
        assert "complete setup" not in startup.next_action
        for _ in range(3):
            clock[0] += timedelta(seconds=301)
            restarted.worker_tick()
            if world.shoko.posts_count() == 2:
                break
        assert world.shoko.posts_count() == 2
        assert not hasattr(restarted, "source_review")
        with open_state(bootstrap.database_path) as store:
            assert store.get_onboarding_json() == consent
            assert store.get_meta("source_continuity_hold_v1") == hold
            assert store.get_source_continuity_review().requires_rebaseline
    finally:
        sonarr.client.close()
        shoko.client.close()
