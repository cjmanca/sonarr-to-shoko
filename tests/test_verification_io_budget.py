"""Offline verification I/O budgets stay separate from persisted outcome expiry."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import Any

import hash_fixtures as hf
import httpx
import pytest
import test_daemon as td
import test_linking as tl

from shoko_linker import daemon
from shoko_linker.http import ApiEndpoint
from shoko_linker.policies import RunPolicy
from shoko_linker.state import META_RECOVERY_CURSOR, FileState, StateStore


class SlowBody(httpx.SyncByteStream):
    def __init__(self, clock: list[float]) -> None:
        self.clock = clock
        self.yielded = 0
        self.closed = False

    def __iter__(self) -> Iterator[bytes]:
        for chunk in [b"{", b'"ID":1', b"}"]:
            self.yielded += 1
            self.clock[0] += 1.0
            yield chunk

    def close(self) -> None:
        self.closed = True


def make_deps(tmp_path: Path, clock: list[float], endpoint: ApiEndpoint) -> daemon.DaemonDeps:
    store = td.open_tmp_store(tmp_path)
    td._seed_verifying_rows(store, 1)

    def advance(seconds: float) -> None:
        clock[0] += seconds

    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[],
        shoko_files=[],
        xml=td.mapping_xml(),
        monotonic=lambda: clock[0],
        advance=advance,
    )
    deps.shoko.client.close()
    return replace(deps, shoko=endpoint)


def close_deps(deps: daemon.DaemonDeps) -> None:
    deps.store.close()
    for endpoint in (deps.shoko, deps.sonarr, deps.mapping_source):
        endpoint.client.close()


def test_xref_fetch_deadline_stops_slow_body_and_narrows_timeouts() -> None:
    clock = [0.0]
    body = SlowBody(clock)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, stream=body)

    endpoint = ApiEndpoint(
        httpx.Client(
            base_url=td.SHOKO_BASE,
            transport=httpx.MockTransport(handler),
            timeout=httpx.Timeout(connect=0.1, read=30, write=30, pool=None),
        ),
        lambda: {},
    )
    with endpoint.client:
        fetch = daemon._xref_fetcher(endpoint, 1, deadline=0.5, clock=lambda: clock[0])
        assert fetch() is None
        assert fetch() is None  # expired budget cannot start another request
    assert len(requests) == 1
    assert requests[0].extensions["timeout"] == {
        "connect": 0.1,
        "read": 0.5,
        "write": 0.5,
        "pool": 0.5,
    }
    assert body.yielded == 1
    assert body.closed


@pytest.mark.parametrize("hold", [False, True])
def test_recovery_io_expiry_keeps_ownership_and_rotates_cursor_after_restart(
    tmp_path: Path,
    hold: bool,
) -> None:
    clock = [0.0]
    body = SlowBody(clock)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET"
        return httpx.Response(200, stream=body)

    endpoint = td.endpoint_for(handler, td.SHOKO_BASE)
    deps = make_deps(tmp_path, clock, endpoint)
    if hold:
        with sqlite3.connect(deps.store.path) as conn:
            conn.execute("UPDATE files SET state='indeterminate_write'")
            conn.execute("UPDATE sends SET outcome='ambiguous_send'")
    deps = replace(deps, max_cycle_seconds=0.5)
    path = deps.store.path
    try:
        reasons, processed = daemon._file_recovery_phase(deps, td.T1, 0.0, max_rows=1)
        prefix = "recovery_hold_unknown" if hold else "recovery_poll_unknown"
        assert f"{prefix}:1" in reasons
        assert processed == 1
        inspection = deps.store.active_send_details(1)
        assert inspection is not None
        assert not inspection.slot_released
        assert deps.store.get_meta(META_RECOVERY_CURSOR) == "1"
    finally:
        close_deps(deps)
    assert body.yielded == 1
    assert body.closed
    with closing(StateStore(path)) as reopened:
        restarted = replace(deps, store=reopened)
        row = reopened.get_file(1)
        poll = daemon._recovery_hold_row if hold else daemon._recovery_poll_row
        assert poll(restarted, row, td.T1, deadline_mono=0.5).reason == "recovery_deadline:1"
        inspection = reopened.active_send_details(1)
        assert inspection is not None
        assert not inspection.slot_released
    assert len(requests) == 1


@pytest.mark.parametrize("hold", [False, True])
@pytest.mark.parametrize("outcome", ["verified", "partial", "contradiction"])
def test_recovery_reads_after_persisted_expiry_with_remaining_cycle_budget(
    tmp_path: Path,
    hold: bool,
    outcome: str,
) -> None:
    clock = [10.0]
    requests: list[httpx.Request] = []
    episode = td.SHOKO_EP if outcome != "contradiction" else td.SHOKO_EP + 10
    payload: dict[str, Any] = {
        "ID": 1,
        "Size": td.FILE_SIZE,
        "Hashes": {"ED2K": hf.hex32(1)},
        "SeriesIDs": [
            {
                "SeriesID": {"AniDB": td.ANIME_ID},
                "EpisodeIDs": [
                    {"ID": episode, "AniDB": td.ANIDB_EP, "Source": "User"},
                ],
            }
        ],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET"
        return httpx.Response(200, json=payload)

    deps = make_deps(tmp_path, clock, td.endpoint_for(handler, td.SHOKO_BASE))
    with sqlite3.connect(deps.store.path) as conn:
        if outcome == "partial":
            conn.execute(
                "UPDATE sends SET target_ids_json=?",
                (
                    json.dumps(
                        [
                            [td.ANIME_ID, td.ANIDB_EP, td.SHOKO_EP],
                            [td.ANIME_ID, td.ANIDB_EP + 1, td.SHOKO_EP + 1],
                        ]
                    ),
                ),
            )
        if hold:
            conn.execute("UPDATE files SET state='indeterminate_write'")
            conn.execute("UPDATE sends SET outcome='ambiguous_send'")
    try:
        row = deps.store.get_file(1)
        poll = daemon._recovery_hold_row if hold else daemon._recovery_poll_row
        prefix = "recovery_hold" if hold else "recovery_poll"
        result = poll(deps, row, td.T1, deadline_mono=20.0)
        assert result.reason == f"{prefix}:1:{outcome}"
        record = deps.store.get_file(1)
        assert record is not None
        assert (
            record.state
            is {
                "verified": FileState.VERIFIED,
                "partial": FileState.PARTIAL,
                "contradiction": FileState.CONFLICT,
            }[outcome]
        )
        if outcome != "verified":
            with sqlite3.connect(deps.store.path) as conn:
                assert conn.execute("SELECT slot_released FROM sends").fetchone() == (0,)
    finally:
        close_deps(deps)
    assert len(requests) == 1


@pytest.mark.parametrize("remaining", [None, 0.0, 1.0, 100.0])
def test_inline_verification_uses_remaining_cycle_budget(
    tmp_path: Path, remaining: float | None
) -> None:
    clock = [99.0]
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET"
        return httpx.Response(
            200,
            json={
                "ID": 1,
                "Size": td.FILE_SIZE,
                "Hashes": {"ED2K": hf.hex32(1)},
                "SeriesIDs": [],
            },
        )

    deps = make_deps(tmp_path, clock, td.endpoint_for(handler, td.SHOKO_BASE))
    plan = replace(tl.linkable_plan(), shoko_file_id=1, shoko_ed2k=hf.hex32(1))
    try:
        result = daemon.verify_and_close(
            deps,
            plan,
            1,
            "cycle",
            td.T1,
            deadline_mono=None if remaining is None else 99.0 + remaining,
        )
        if remaining is None:
            assert result == "verification:contradiction"
            assert clock[0] == 149.0
            assert len(requests) == 6
            record = deps.store.get_file(1)
            assert record is not None
            assert record.state is FileState.CONFLICT
            return
        assert result == "verification:unknown"
        assert clock[0] == 99.0 + min(remaining, deps.verification_timeout_seconds)
        assert len(requests) == (0 if remaining == 0 else (1 if remaining == 1 else 5))
        inspection = deps.store.active_send_details(1)
        assert inspection is not None
        assert not inspection.slot_released
        record = deps.store.get_file(1)
        assert record is not None
        assert record.state is FileState.VERIFYING
    finally:
        close_deps(deps)


@pytest.mark.parametrize("service_mode", [False, True])
def test_cycle_cannot_restart_verification_budget_after_post(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    service_mode: bool,
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file = td.SpecFile(series_id=1, file_id=10, shoko_id=500)
    td.activate_mapping(tmp_path, td.mapping_xml())
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [td.SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = td.ShokoStub([file])
    shoko.link_on_post = False
    clock = [0.0]
    original = shoko.handler

    def handler(request: httpx.Request) -> httpx.Response:
        response = original(request)
        if request.method == "POST":
            clock[0] = 1.0  # the cycle's remaining I/O budget elapsed during delivery
        return response

    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        shoko=shoko,
        xml=td.mapping_xml(),
        monotonic=lambda: clock[0],
        advance=lambda _: None,
        max_cycle_seconds=1.0,
    )
    deps.shoko.client.close()
    deps = replace(
        deps,
        shoko=td.endpoint_for(handler, td.SHOKO_BASE),
        service_mode=service_mode,
        service_authorized=lambda: True,
    )
    try:
        report = daemon.run_cycle(
            deps,
            now_iso=td.T0,
            mode=daemon.CycleMode.APPLY,
            policy=RunPolicy.SERVICE if service_mode else RunPolicy.NEW_ARRIVAL,
            cycle_key="cycle",
        )
        assert report.counts.submitted == 1
        post_index = next(i for i, request in enumerate(shoko.requests) if request.method == "POST")
        assert len(shoko.requests) == post_index + 1
        row = store.get_file(file.shoko_id)
        assert row is not None
        assert row.state is FileState.VERIFYING
        assert row.state_detail == "verification unknown"
        inspection = store.active_send_details(file.shoko_id)
        assert inspection is not None
        assert not inspection.slot_released
    finally:
        close_deps(deps)
