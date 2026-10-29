"""Blocked normal-service ticks still inspect previously accepted metadata work."""

from __future__ import annotations

import json
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
import test_daemon as td
from test_metadata_operations import Server, operation
from test_service_runtime import _confirmed_activity_runtime
from test_state import LIMITS, T0

from shoko_linker import cli
from shoko_linker.daemon import DaemonDeps
from shoko_linker.onboarding import OnboardingManager
from shoko_linker.service import ConnectionReport
from shoko_linker.service_settings import SETTINGS_META, load_service_settings
from shoko_linker.state import MetadataOperationState as M
from shoko_linker.state import StateStore, open_state


@pytest.mark.parametrize(
    "blocker",
    ["context", "pause", "reconfirm", "optout", "connection", "exception", "dependencies"],
)
def test_blocked_tick_recovers_accepted_current_source_without_post(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, blocker: str
) -> None:
    runtime, endpoint, context = _confirmed_activity_runtime(tmp_path)
    server = Server()
    runtime.shoko = server.endpoint
    path = tmp_path / "state.sqlite3"
    with open_state(path, budget_limits=LIMITS) as store:
        row = operation(store, server.endpoint, "accepted")
        initial = store.budget_view()
        if blocker == "pause":
            OnboardingManager(store).pause(iso=T0)
        elif blocker == "reconfirm":
            OnboardingManager(store).pause_for_review("setup_changed", "Review setup", iso=T0)
        elif blocker == "optout":
            document = load_service_settings(store, runtime.settings)
            document["allow_missing_metadata_refresh"] = False
            store.set_meta(SETTINGS_META, json.dumps(document))

    def probe(*_args: Any) -> ConnectionReport:
        if blocker == "exception":
            raise OSError("synthetic private endpoint failure")
        return ConnectionReport(blocker != "connection", True, ())

    def fail_cycle(*_args: Any, **_kwargs: Any) -> None:
        if blocker == "dependencies":
            raise ValueError("synthetic preparation failure")
        pytest.fail("blocked setup must not reconcile or submit new work")

    monkeypatch.setattr(cli, "configured_bootstrap", lambda _: runtime.settings)
    monkeypatch.setattr(cli, "probe_connections", probe)
    monkeypatch.setattr(cli, "current_iso", lambda: T0)
    monkeypatch.setattr(
        runtime.setup,
        "current_context",
        lambda _: None if blocker in {"context", "optout"} else context,
    )
    monkeypatch.setattr(runtime, "_run_authorized_cycle", fail_cycle)
    try:
        runtime.worker_tick()
        first_reads = len(server.requests)
        assert first_reads > 0
        runtime.worker_tick()
        assert len(server.requests) == first_reads  # READY is terminal, capacity released once.
        with open_state(path, budget_limits=LIMITS) as store:
            current = store.get_metadata_operation(row.source_key, row.anidb_anime_id)
            assert current is not None
            assert current.state is M.READY
            budget = store.budget_view()
            assert budget.pending_episode_jobs == 0
            assert budget.rolling_new_submissions == initial.rolling_new_submissions
            assert budget.completed_submissions == 0
            if blocker == "pause":
                assert store.get_service_pause() == (True, "user_paused")
        assert all(request.method == "GET" for request in server.requests)
        assert runtime.status().worker_activity == ""
        assert not runtime.status().automatic_linking
    finally:
        server.endpoint.client.close()
        endpoint.client.close()


@pytest.mark.parametrize("queue_visible", [True, False])
def test_unknown_queue_still_reads_targets_without_claiming_ready(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, queue_visible: bool
) -> None:
    runtime, endpoint, _ = _confirmed_activity_runtime(tmp_path)
    server = Server()
    server.queue_status = 200 if queue_visible else 403
    runtime.shoko = server.endpoint
    with open_state(tmp_path / "state.sqlite3", budget_limits=LIMITS) as store:
        row = operation(store, server.endpoint, "accepted")
    monkeypatch.setattr(cli, "current_iso", lambda: T0)
    try:
        runtime._recover_blocked_metadata(time.monotonic() + 30, 1)
        assert any("/Series/AniDB/" in request.url.path for request in server.requests)
        with open_state(tmp_path / "state.sqlite3", budget_limits=LIMITS) as store:
            current = store.get_metadata_operation(row.source_key, row.anidb_anime_id)
            assert current is not None
            assert current.state is (M.READY if queue_visible else M.ACCEPTED_WAITING)
            assert store.budget_view().pending_episode_jobs == (0 if queue_visible else 1)
    finally:
        server.endpoint.client.close()
        endpoint.client.close()


@pytest.mark.parametrize("blocked_by", ["expired", "zero", "changed_url", "no_operations"])
def test_unavailable_or_empty_recovery_makes_no_http_requests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, blocked_by: str
) -> None:
    runtime, endpoint, _ = _confirmed_activity_runtime(tmp_path)
    server = Server()
    runtime.shoko = server.endpoint
    with open_state(tmp_path / "state.sqlite3", budget_limits=LIMITS) as store:
        if blocked_by != "no_operations":
            operation(store, server.endpoint, "accepted")
        if blocked_by == "changed_url":
            document = load_service_settings(store, runtime.settings)
            document["shoko_url"] = "http://different-shoko:8111/"
            store.set_meta(SETTINGS_META, json.dumps(document))
    monkeypatch.setattr(cli, "current_iso", lambda: T0)
    try:
        runtime._recover_blocked_metadata(
            time.monotonic() + (-1 if blocked_by == "expired" else 30),
            0 if blocked_by == "zero" else 1,
        )
        assert server.requests == []
    finally:
        server.endpoint.client.close()
        endpoint.client.close()


def test_slow_context_does_not_grant_recovery_a_new_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime, endpoint, _ = _confirmed_activity_runtime(tmp_path)
    server = Server()
    runtime.shoko = server.endpoint
    clock = [0.0]
    with open_state(tmp_path / "state.sqlite3", budget_limits=LIMITS) as store:
        operation(store, server.endpoint, "accepted")
        document = load_service_settings(store, runtime.settings)
        document["sync"]["max_cycle_seconds"] = 1
        store.set_meta(SETTINGS_META, json.dumps(document))

    def context(_store: StateStore) -> None:
        clock[0] = 2.0

    monkeypatch.setattr(time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(cli, "configured_bootstrap", lambda _: runtime.settings)
    monkeypatch.setattr(cli, "probe_connections", lambda *_: ConnectionReport(True, True, ()))
    monkeypatch.setattr(runtime.setup, "current_context", context)
    try:
        runtime.worker_tick()
        assert server.requests == []
    finally:
        server.endpoint.client.close()
        endpoint.client.close()


def test_blocked_tick_honors_configured_total_row_allowance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime, endpoint, _ = _confirmed_activity_runtime(tmp_path)
    server = Server()
    runtime.shoko = server.endpoint
    with open_state(tmp_path / "state.sqlite3", budget_limits=LIMITS) as store:
        first = operation(store, server.endpoint, "accepted", anime=18208)
        second = operation(store, server.endpoint, "accepted", anime=18209)
        document = load_service_settings(store, runtime.settings)
        document["sync"]["max_recovery_rows_per_cycle"] = 1
        store.set_meta(SETTINGS_META, json.dumps(document))
    monkeypatch.setattr(cli, "configured_bootstrap", lambda _: runtime.settings)
    monkeypatch.setattr(cli, "probe_connections", lambda *_: ConnectionReport(True, True, ()))
    monkeypatch.setattr(runtime.setup, "current_context", lambda _: None)
    monkeypatch.setattr(cli, "current_iso", lambda: T0)
    try:
        runtime.worker_tick()
        with open_state(tmp_path / "state.sqlite3") as store:
            rows = [
                store.get_metadata_operation(row.source_key, row.anidb_anime_id)
                for row in (first, second)
            ]
            assert sum(row is not None and row.state is M.READY for row in rows) == 1
            assert store.budget_view().pending_episode_jobs == 1
    finally:
        server.endpoint.client.close()
        endpoint.client.close()


def test_authorized_daemon_owns_recovery_without_second_allowance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime, endpoint, context = _confirmed_activity_runtime(tmp_path)
    server = Server()
    runtime.shoko = server.endpoint
    store = td.open_tmp_store(tmp_path)
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[],
        shoko_files=[],
        xml=td.mapping_xml(),
        monotonic=lambda: 0.0,
        advance=lambda _: None,
    )
    deps = replace(deps, shoko=server.endpoint)
    calls: list[str] = []

    def daemon(_deps: DaemonDeps, _store: StateStore, _settings: Any) -> None:
        calls.append("daemon")

    def unexpected_recovery(*_args: Any) -> None:
        pytest.fail("authorized daemon must retain the sole recovery allowance")

    monkeypatch.setattr(cli, "configured_bootstrap", lambda _: runtime.settings)
    monkeypatch.setattr(cli, "probe_connections", lambda *_: ConnectionReport(True, True, ()))
    monkeypatch.setattr(runtime.setup, "current_context", lambda _: context)
    monkeypatch.setattr(cli, "current_iso", lambda: T0)
    monkeypatch.setattr(cli, "open_mapping_store", lambda _: object())
    monkeypatch.setattr(cli, "read_overrides_text", lambda _: None)
    monkeypatch.setattr(cli, "build_daemon_deps", lambda *_, **__: deps)
    monkeypatch.setattr(runtime, "_run_daemon_cycle", daemon)
    monkeypatch.setattr(runtime, "_recover_blocked_metadata", unexpected_recovery)
    try:
        runtime.worker_tick()
        assert calls == ["daemon"]
        assert server.requests == []
    finally:
        for candidate in (endpoint, deps.sonarr, deps.shoko, deps.mapping_source):
            candidate.client.close()
        store.close()
