"""Offline review scheduling through the production runtime and threaded worker loop."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from http.server import HTTPServer
from pathlib import Path
from typing import cast

import httpx
import pytest
from test_source_rebaseline import SESSION, T1, setup
from test_source_review import Clock, Collector

from shoko_linker import cli
from shoko_linker.bootstrap import BootstrapSettings
from shoko_linker.config import SecretRef
from shoko_linker.http import ApiEndpoint
from shoko_linker.service import ConnectionReport, run_dashboard_worker
from shoko_linker.service_settings import ServiceSettingsController, load_service_settings
from shoko_linker.source_review import SourceReviewController, SourceReviewWork
from shoko_linker.state import META_SERVICE_SETTINGS, StateStore, open_state


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
    runtime.source_review = SourceReviewController(
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
        pytest.fail("review operation entered ordinary linking or metadata recovery")

    monkeypatch.setattr(runtime, "_worker_tick", forbidden)
    monkeypatch.setattr(runtime, "_recover_blocked_metadata", forbidden)
    runtime.source_review.request_assessment(SESSION)
    try:
        yield result
    finally:
        endpoint.client.close()


@pytest.mark.parametrize("confirm", [False, True])
def test_production_scheduler_continues_paused_review_and_confirmation(
    scheduled_review: ScheduledReview,
    monkeypatch: pytest.MonkeyPatch,
    confirm: bool,
) -> None:
    h = scheduled_review
    confirmation_requested = False
    ordinary_ticks = 0

    def ordinary() -> None:
        nonlocal ordinary_ticks
        ordinary_ticks += 1
        assert h.runtime.source_review.view(SESSION).status == "confirmed"
        assert not h.runtime.status().automatic_linking
        assert "Resume" in h.runtime.status().next_action
        h.runtime._tick_cycle_started = True

    monkeypatch.setattr(h.runtime, "_worker_tick", ordinary)

    def on_wait(delay: float) -> bool:
        nonlocal confirmation_requested
        if confirm and delay >= 900 and not confirmation_requested:
            view = h.runtime.source_review.view(SESSION)
            assert view.status == "ready"
            assert view.token is not None
            h.runtime.source_review.request_confirmation(view.token, SESSION)
            confirmation_requested = True
            # The initial confirmation tick must yield until its timestamp is fresh.
            return h.wake.is_set()
        return False

    h.on_wait = on_wait
    h.run()
    view = h.runtime.source_review.view(SESSION)
    assert view.status == ("confirmed" if confirm else "ready")
    assert len(h.collector.works) == (6 if confirm else 3)
    assert h.waits == ([1, 1, 1000, 1, 1, 1, 1, 1000] if confirm else [1, 1, 1000])
    assert ordinary_ticks == int(confirm)
    assert view.service_pause == (True, "user_paused")


@pytest.mark.parametrize("remaining", ["none", "user", "setup", "writes", "canary", "pending"])
def test_acceptance_discards_cached_source_guidance_and_preserves_remaining_gates(
    scheduled_review: ScheduledReview,
    monkeypatch: pytest.MonkeyPatch,
    remaining: str,
) -> None:
    h = scheduled_review
    with open_state(h.runtime.settings.database_path) as store:
        # The accepted baseline must not need Resume when no independent pause exists.
        assert store.resume_onboarding_if_current(store.get_onboarding_json() or "", T1)
        _, old_guidance = h.runtime._operational_status(
            store, authorized=True, next_action="Processing eligible files."
        )
    h.runtime._update_latest(authorized=False, next_action=old_guidance)
    assert "Review source change on Overview" in h.runtime.status().next_action
    for _ in range(3):
        h.runtime.worker_tick()
        h.clock.advance()
    token = h.runtime.source_review.view(SESSION).token
    assert token is not None
    h.runtime.source_review.request_confirmation(token, SESSION)
    h.clock.advance()
    delays = [h.runtime.worker_tick() for _ in range(3)]
    assert delays == [1, 1, 1]
    assert h.runtime.source_review.view(SESSION).status == "confirmed"
    with open_state(h.runtime.settings.database_path) as store:
        assert not store.get_source_continuity_review().requires_rebaseline
        if remaining == "user":
            store.pause_service_for_user(T1)
        elif remaining == "setup":
            store.pause_service_for_review("setup_changed", T1)
        elif remaining == "writes":
            store.disable_writes("independent safety stop")
        elif remaining in ("canary", "pending"):
            store.record_service_canary(500, T1)
            if remaining == "canary":
                store.set_service_canary_status("failed", T1)
    status = h.runtime.status()
    assert not status.automatic_linking
    assert "Review source change on Overview" not in status.next_action
    expected = {
        "none": "accepted. Checking current requirements",
        "user": "Resume",
        "setup": "Confirm the current scope and safety setup again",
        "writes": "stopped by a safety check",
        "canary": "initial link could not be verified",
        "pending": "Waiting for the initial link to be verified",
    }
    assert expected[remaining] in status.next_action

    # The short handoff belongs to the NEXT operation and is consumed only once.
    calls: list[str] = []

    def probe(*_args: object) -> ConnectionReport:
        calls.append("connections")
        return ConnectionReport(True, True, ())

    def context(*_args: object) -> None:
        calls.append("setup")

    monkeypatch.setattr(cli, "configured_bootstrap", lambda _: h.runtime.settings)
    monkeypatch.setattr(cli, "probe_connections", probe)
    monkeypatch.setattr(h.runtime.setup, "current_context", context)
    monkeypatch.setattr(
        h.runtime, "_worker_tick", lambda: cli._NormalServiceRuntime._worker_tick(h.runtime)
    )
    monkeypatch.setattr(h.runtime, "_recover_blocked_metadata", lambda *_: calls.append("recovery"))
    assert h.runtime.worker_tick() is None
    assert calls == ["connections", "setup", "recovery"]
    assert not h.runtime.status().automatic_linking
    assert "Review source change on Overview" not in h.runtime.status().next_action


@pytest.mark.parametrize("end", ["stalled", "session", "failed", "busy"])
def test_pending_review_yields_and_stops_continuation_on_completion_or_failure(
    scheduled_review: ScheduledReview,
    monkeypatch: pytest.MonkeyPatch,
    end: str,
) -> None:
    h = scheduled_review
    if end == "stalled":

        def stalled(_store: StateStore, work: SourceReviewWork, _deadline: float) -> None:
            h.collector.works.append(work)

        monkeypatch.setattr(h.collector, "collect", stalled)
    elif end == "failed":

        def fail(*_args: object) -> None:
            raise OSError("offline collector failure")

        monkeypatch.setattr(h.collector, "collect", fail)
    elif end == "busy":

        def consume_budget(*_args: object) -> None:
            if len(h.collector.works) == 1:
                h.clock.advance(2)

        h.collector.action = consume_budget
    else:

        def expire_session(delay: float) -> bool:
            if delay < 900:
                with open_state(h.runtime.settings.database_path) as store:
                    store.connection.execute("DELETE FROM dashboard_sessions")
                    store.connection.commit()
            return False

        h.on_wait = expire_session
    h.run()
    assert h.runtime.source_review.continuation_delay() is None
    assert h.waits[-1] == 1000
    if end == "busy":
        assert h.runtime.source_review.view(SESSION).status == "ready"
        assert len(h.collector.works) == 3
    elif end != "session":
        assert h.runtime.source_review.view(SESSION).status == "error"
    if end == "stalled":
        assert len(h.collector.works) == 5
        assert h.waits == [1, 1, 1, 1, 1, 1000]


def test_shutdown_interrupts_pending_review_without_another_slice(
    scheduled_review: ScheduledReview,
) -> None:
    h = scheduled_review

    def shutdown(_delay: float) -> bool:
        h.runtime.state.stopped = True
        h.wake.set()
        return True

    h.on_wait = shutdown
    h.run()
    assert len(h.collector.works) == 1
    assert h.waits == [1]


def test_confirmation_continuation_preserves_original_token_expiry(
    scheduled_review: ScheduledReview,
) -> None:
    h = scheduled_review
    requested = False

    def confirm_near_expiry(delay: float) -> bool:
        nonlocal requested
        if delay >= 900 and not requested:
            view = h.runtime.source_review.view(SESSION)
            assert view.token is not None
            h.clock.advance(4)
            h.runtime.source_review.request_confirmation(view.token, SESSION)
            requested = True
            return h.wake.is_set()
        return False

    h.on_wait = confirm_near_expiry
    h.run()
    assert h.runtime.source_review.view(SESSION).status == "error"
    assert h.runtime.source_review.continuation_delay() is None
    assert len(h.collector.works) == 3
    assert h.waits == [1, 1, 1000, 1, 1000]


def test_control_wake_interrupts_active_review_delay(scheduled_review: ScheduledReview) -> None:
    h = scheduled_review
    woke = False

    def control_wake(delay: float) -> bool:
        nonlocal woke
        if delay < 900 and not woke:
            h.wake.set()
            woke = True
            return True
        return False

    h.on_wait = control_wake
    h.run()
    assert h.runtime.source_review.view(SESSION).status == "ready"
    assert len(h.collector.works) == 3
    assert h.clock.mono == 101
