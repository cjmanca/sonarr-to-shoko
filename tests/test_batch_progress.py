"""Offline regressions for bounded backlog progress and trailing reservation ceilings."""

import sqlite3
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

import httpx
import pytest
import test_daemon as td
import test_state as ts

import shoko_linker.state as state_module
from shoko_linker.daemon import CycleMode, rolling_budget_exhausted, run_cycle
from shoko_linker.policies import RunPolicy
from shoko_linker.rate_control import service_budget_limits
from shoko_linker.state import (
    BudgetLimits,
    BudgetRefusal,
    FileState,
    SendToken,
    SlotReservation,
    WriteDisabledError,
    open_state,
)

T0 = datetime.fromisoformat("2026-01-01T00:00:00+00:00")
LIMITS = BudgetLimits(10, 10, 20, 3, 168)


def fixed_clock(value: str) -> Callable[[], str]:
    return lambda: value


def test_slow_inventory_still_leaves_resolution_and_apply_headroom(tmp_path: Path) -> None:
    files = [td.SpecFile(series_id=1, file_id=10, shoko_id=500)]
    xml = td.mapping_xml()
    td.activate_mapping(tmp_path, xml)
    elapsed = [0.0]
    sonarr = td.SonarrStub(files)

    def slow_inventory(request: httpx.Request) -> httpx.Response:
        elapsed[0] += 40.0
        return sonarr.handler(request)

    with open_state(tmp_path / "state.sqlite3", budget_limits=td.WIDE_LIMITS) as store:
        td.seed_baseline(store, files)
        deps = td.build_deps(
            tmp_path,
            store=store,
            sonarr_files=files,
            shoko_files=files,
            xml=xml,
            monotonic=lambda: elapsed[0],
            advance=lambda _: None,
        )
        deps.sonarr.client.close()
        deps = replace(
            deps,
            sonarr=td.endpoint_for(slow_inventory, td.SONARR_BASE),
            service_mode=True,
            service_authorized=lambda: True,
            max_cycle_seconds=300.0,
        )
        try:
            report = run_cycle(
                deps,
                now_iso=td.T0,
                cycle_key="slow-inventory",
                mode=CycleMode.APPLY,
                policy=RunPolicy.SERVICE,
            )
            assert report.counts.submitted == 1
            assert 150.0 < elapsed[0] < 300.0
        finally:
            deps.sonarr.client.close()
            deps.shoko.client.close()
            deps.mapping_source.client.close()


def test_service_short_window_cannot_bypass_independent_trailing_week(tmp_path: Path) -> None:
    limits = service_budget_limits(BudgetLimits(20, 10, 20, 5000, 24.0))
    path = tmp_path / "state.sqlite3"
    with open_state(path, budget_limits=limits) as store:
        # Durable historical debt from other service/campaign cycles.
        store.connection.execute(
            "INSERT INTO budget_reservations VALUES (?, 4999)", (T0.timestamp(),)
        )
        day_one = (T0 + timedelta(days=1)).isoformat()
        assert isinstance(store.try_reserve_slot("last", day_one), SlotReservation)
        store.release_slot("last", day_one)
    day_two = (T0 + timedelta(days=2)).isoformat()
    with open_state(path, budget_limits=limits) as store:
        assert rolling_budget_exhausted(store.budget_view(), limits, day_two)
        assert isinstance(store.try_reserve_slot("overflow", day_two), BudgetRefusal)
        assert (
            store.budget_view().rolling_retry_iso(limits, day_two)
            == (T0 + timedelta(days=7)).isoformat()
        )


def test_service_long_configured_window_survives_past_the_hard_week(tmp_path: Path) -> None:
    limits = service_budget_limits(BudgetLimits(20, 10, 20, 1, 240.0))
    path = tmp_path / "state.sqlite3"
    day_zero = T0.isoformat()
    day_eight = (T0 + timedelta(days=8)).isoformat()
    with open_state(path, budget_limits=limits) as store:
        assert isinstance(store.try_reserve_slot("long-window", day_zero), SlotReservation)
        store.release_slot("long-window", day_zero)
    with open_state(path, budget_limits=limits) as store:
        assert store.budget_view().rolling_count(limits, day_eight) == 1
        assert rolling_budget_exhausted(store.budget_view(), limits, day_eight)
        result = store.try_reserve_slot("still-in-configured-window", day_eight)
        assert isinstance(result, BudgetRefusal)
        assert result.reason == "rolling_new_exhausted"


def test_trailing_window_keeps_recent_reservations_across_expiry_and_restart(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    times = [T0, T0 + timedelta(days=7, seconds=-30), T0 + timedelta(days=7, seconds=-29)]
    with open_state(path, budget_limits=LIMITS) as store:
        for index, moment in enumerate(times):
            assert isinstance(
                store.try_reserve_slot(str(index), moment.isoformat()), SlotReservation
            )
            store.release_slot(str(index), moment.isoformat())
    boundary = T0 + timedelta(days=7)
    with open_state(path, budget_limits=LIMITS) as store:
        assert not rolling_budget_exhausted(store.budget_view(), LIMITS, boundary.isoformat())
        assert isinstance(store.try_reserve_slot("next", boundary.isoformat()), SlotReservation)
        store.release_slot("next", boundary.isoformat())
        result = store.try_reserve_slot("overflow", (boundary + timedelta(seconds=1)).isoformat())
        assert isinstance(result, BudgetRefusal)
        assert result.reason == "rolling_new_exhausted"
        assert rolling_budget_exhausted(store.budget_view(), LIMITS, boundary.isoformat())
        # Pending-slot release is never a refund of a trailing-window submission.
        assert store.budget_view().pending_file_requests == 0


def test_trailing_window_does_not_expire_recent_sends_when_limit_is_lowered(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_state(path, budget_limits=replace(LIMITS, rolling_new=10)) as store:
        for index in range(4):
            moment = T0 + timedelta(hours=index)
            assert isinstance(
                store.try_reserve_slot(str(index), moment.isoformat()), SlotReservation
            )
            store.release_slot(str(index), moment.isoformat())
        store.budget_limits = LIMITS
        now = (T0 + timedelta(days=7)).isoformat()
        assert rolling_budget_exhausted(store.budget_view(), LIMITS, now)
        assert isinstance(store.try_reserve_slot("blocked", now), BudgetRefusal)


@pytest.mark.parametrize("many_anime", [False, True])
def test_bounded_resolution_rotates_backlog_across_restart(
    tmp_path: Path,
    many_anime: bool,
) -> None:
    """160 unique targets at 1s/GET used to consume 323s before the first POST."""
    files = [
        td.SpecFile(
            series_id=1,
            file_id=10 + index,
            shoko_id=500 + index,
            tvdb_series=td.TVDB_SERIES,
            anime_id=td.ANIME_ID + index if many_anime else td.ANIME_ID,
            # 101-103 are intentionally unverified AniDB number encodings.
            episodes=((1, index + 1 if index < 100 else index + 4, 5501 + index, 777 + index),),
        )
        for index in range(160)
    ]
    if many_anime:
        entries = "".join(
            f"<anime anidbid='{file.anime_id}' tvdbid='{td.TVDB_SERIES}' defaulttvdbseason='1'>"
            "<mapping-list><mapping anidbseason='1' tvdbseason='1'>"
            f";{file.episodes[0][1]}-{file.episodes[0][1]};</mapping></mapping-list></anime>"
            for index, file in enumerate(files)
        )
    else:
        pairs = "".join(f";{file.episodes[0][1]}-{file.episodes[0][1]};" for file in files)
        entries = (
            f"<anime anidbid='{td.ANIME_ID}' tvdbid='{td.TVDB_SERIES}' defaulttvdbseason='1'>"
            f"<mapping-list><mapping anidbseason='1' tvdbseason='1'>{pairs}"
            "</mapping></mapping-list></anime>"
        )
    xml = f"<anime-list>{entries}</anime-list>"
    td.activate_mapping(tmp_path, xml)
    shoko = td.ShokoStub(files)
    # Leave all rows in discovery, including verified ones: they must not be re-resolved.
    seen_batches: list[set[int]] = []
    for cycle in range(170):
        store = td.open_tmp_store(tmp_path)
        store.budget_limits = replace(td.WIDE_LIMITS, rolling_new=5000)
        if cycle == 0:
            td.seed_baseline(store, files)
        elapsed = [0.0]
        now = (datetime.fromisoformat(td.T0) + timedelta(minutes=5 * cycle)).isoformat()

        def tick(_path: str, elapsed: list[float] = elapsed) -> None:
            elapsed[0] += 1.0

        def monotonic(elapsed: list[float] = elapsed) -> float:
            return elapsed[0]

        def wall_clock(value: str = now) -> str:
            return value

        shoko.on_get = tick
        deps = td.build_deps(
            tmp_path,
            store=store,
            sonarr_files=files,
            shoko_files=files,
            xml=xml,
            monotonic=monotonic,
            advance=lambda _: None,
            shoko=shoko,
            now=wall_clock,
        )
        deps = replace(
            deps,
            service_mode=True,
            service_authorized=lambda: True,
            max_submissions=20,
            max_cycle_seconds=300.0,
        )
        before = len(shoko.requests)
        try:
            report = run_cycle(
                deps,
                now_iso=now,
                cycle_key=f"progress-{cycle}",
                mode=CycleMode.APPLY,
                policy=RunPolicy.SERVICE,
            )
            if cycle < 2:
                assert report.counts.submitted > 0
            assert elapsed[0] <= 300.0
            resolved = {
                int(request.url.path.split("/")[5])
                for request in shoko.requests[before:]
                if request.url.path.startswith("/api/v3/Episode/AniDB/")
            }
            seen_batches.append(resolved)
            assert len(resolved) <= 20
            if report.counts.verified == len(files):
                break
        finally:
            deps.sonarr.client.close()
            deps.shoko.client.close()
            deps.mapping_source.client.close()
            store.close()
    assert seen_batches[0].isdisjoint(seen_batches[1])
    assert len(shoko.posts) == len(files)
    with open_state(tmp_path / "state.sqlite3") as store:
        assert len(store.files_in_state(FileState.VERIFIED)) == len(files)


def test_many_sonarr_series_inventory_rotates_and_resumes_after_restart(tmp_path: Path) -> None:
    files = [
        td.SpecFile(
            series_id=index + 1,
            file_id=1000 + index,
            shoko_id=500 + index,
            tvdb_series=3000 + index,
            anime_id=4000 + index,
            episodes=((1, 1, 6000 + index, 7000 + index),),
        )
        for index in range(160)
    ]
    entries = "".join(
        f"<anime anidbid='{file.anime_id}' tvdbid='{file.tvdb_series}' "
        "defaulttvdbseason='1'><mapping-list><mapping anidbseason='1' tvdbseason='1'>"
        ";1-1;</mapping></mapping-list></anime>"
        for file in files
    )
    xml = f"<anime-list>{entries}</anime-list>"
    td.activate_mapping(tmp_path, xml)
    elapsed = [0.0]
    sonarr = td.SonarrStub(files)
    shoko = td.ShokoStub(files)

    def one_second_per_sonarr_get(request: httpx.Request) -> httpx.Response:
        elapsed[0] += 1.0
        return sonarr.handler(request)

    submitted = 0
    reports = []
    source_anchors = []
    for cycle in range(6):
        now = (T0 + timedelta(minutes=5 * cycle)).isoformat()
        cycle_start = elapsed[0]
        store = td.open_tmp_store(tmp_path)
        deps = None
        try:
            if cycle == 0:
                td.seed_baseline(store, files)
            deps = td.build_deps(
                tmp_path,
                store=store,
                sonarr_files=files,
                shoko_files=files,
                xml=xml,
                monotonic=lambda: elapsed[0],
                advance=lambda _: None,
                shoko=shoko,
                now=fixed_clock(now),
            )
            deps.sonarr.client.close()
            deps = replace(
                deps,
                sonarr=td.endpoint_for(one_second_per_sonarr_get, td.SONARR_BASE),
                service_mode=True,
                service_authorized=lambda: True,
                max_submissions=20,
                max_cycle_seconds=300.0,
            )
            report = run_cycle(
                deps,
                now_iso=now,
                cycle_key=f"many-series-{cycle}",
                mode=CycleMode.APPLY,
                policy=RunPolicy.SERVICE,
            )
            reports.append(report)
            submitted += report.counts.submitted
            source_anchors.append(store.get_meta("source_last_discovery_total"))
            assert elapsed[0] - cycle_start <= 300.0
            if submitted:
                break
        finally:
            if deps is not None:
                deps.sonarr.client.close()
                deps.shoko.client.close()
                deps.mapping_source.client.close()
            store.close()

    assert reports[0].reconciliation_ready is False
    assert all(report.counts.submitted == 0 for report in reports[:-1])
    assert reports[-1].reconciliation_ready is True
    assert source_anchors[:-1] == [None] * (len(source_anchors) - 1)
    assert source_anchors[-1] == str(len(files))
    assert (
        len(
            {
                int(request.url.params["seriesId"])
                for request in sonarr.requests
                if request.url.path.endswith("/api/v3/episode")
            }
        )
        == 160
    )
    assert submitted > 0
    assert len(shoko.posts) == submitted


def test_reservation_and_intent_are_one_transaction_with_no_refund_or_replay(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    with open_state(path, budget_limits=LIMITS) as store:
        ts.walk_to_planned(store)
        first = ts.prepare(store)
        assert isinstance(first, SendToken)
        assert isinstance(ts.prepare(store), BudgetRefusal)
        assert store.budget_view().rolling_count(LIMITS, ts.T0) == 1
        store.void_prepared_send(
            first.shoko_file_id, first.request_fingerprint, iso=ts.T0, reason="not sent"
        )
        assert store.budget_view().rolling_count(LIMITS, ts.T0) == 1
        ts.walk_to_planned(store, 102)
        with sqlite3.connect(path) as conn:
            conn.execute(
                "CREATE TRIGGER fail_intent BEFORE INSERT ON sends "
                "BEGIN SELECT RAISE(ABORT, 'injected intent failure'); END"
            )
        with pytest.raises(WriteDisabledError):
            ts.prepare(store, 102)
        assert store.budget_view().rolling_count(LIMITS, ts.T0) == 1
        assert store.budget_view().pending_file_requests == 0
    with open_state(path, budget_limits=LIMITS) as store:
        assert store.budget_view().rolling_count(LIMITS, ts.T0) == 1


def test_rolling_peek_and_cooldown_agree_after_restart_and_cap_reduction(tmp_path: Path) -> None:
    from shoko_linker.daemon import _rolling_cooldown_retry_iso

    path = tmp_path / "state.sqlite3"
    with open_state(path, budget_limits=replace(LIMITS, rolling_new=10)) as store:
        for hour in range(4):
            iso = (T0 + timedelta(hours=hour)).isoformat()
            assert isinstance(store.try_reserve_slot(str(hour), iso), SlotReservation)
            store.release_slot(str(hour), iso)
    now = (T0 + timedelta(days=7)).isoformat()
    with open_state(path, budget_limits=replace(LIMITS, rolling_new=2)) as store:
        deps = td.build_deps(
            tmp_path,
            store=store,
            sonarr_files=[],
            shoko_files=[],
            xml=td.mapping_xml(),
            monotonic=lambda: 0.0,
            advance=lambda _: None,
        )
        try:
            # Three recent reservations remain; two must expire to leave one slot.
            assert (
                _rolling_cooldown_retry_iso(deps, now, ())
                == (T0 + timedelta(days=7, hours=2)).isoformat()
            )
            assert isinstance(store.try_reserve_slot("full", now), BudgetRefusal)
            expiry = (T0 + timedelta(days=7, hours=2)).isoformat()
            assert store.budget_limits is not None
            assert not rolling_budget_exhausted(store.budget_view(), store.budget_limits, expiry)
            assert isinstance(store.try_reserve_slot("free", expiry), SlotReservation)
        finally:
            deps.sonarr.client.close()
            deps.shoko.client.close()
            deps.mapping_source.client.close()


@pytest.mark.parametrize("anonymous", [False, True])
def test_upgrade_recovers_recent_send_debt_without_double_counting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    anonymous: bool,
) -> None:
    class UpgradeClock(datetime):
        @classmethod
        def now(cls, tz: object = None) -> "UpgradeClock":
            return cls.fromtimestamp((T0 + timedelta(days=7)).timestamp(), T0.tzinfo)

    path = tmp_path / "state.sqlite3"
    with open_state(path, budget_limits=LIMITS) as store:
        for file_id, day in ((101, 0), (102, 6)):
            ts.walk_to_planned(store, file_id)
            token = ts.prepare(store, file_id, iso=(T0 + timedelta(days=day)).isoformat())
            assert isinstance(token, SendToken)
            store.void_prepared_send(
                file_id, token.request_fingerprint, iso=ts.T0, reason="not sent"
            )
    # Reconstruct the v4 schema: no timestamp ledger, only the old fixed counter.
    with sqlite3.connect(path) as conn:
        conn.execute("DROP TABLE budget_reservations")
        conn.execute("DELETE FROM schema_version WHERE version = 5")
        conn.execute("PRAGMA user_version = 4")
        conn.execute(
            "UPDATE budgets SET rolling_new_submissions = ? WHERE window_key = '_global'",
            (3 if anonymous else 2,),
        )
    monkeypatch.setattr(state_module, "datetime", UpgradeClock)
    now = (T0 + timedelta(days=7)).isoformat()
    for _ in range(2):
        with open_state(path, budget_limits=LIMITS) as store:
            view = store.budget_view()
            assert view.rolling_count(LIMITS, now) == (2 if anonymous else 1)
            assert view.pending_file_requests == 0
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM sends").fetchone()[0] == 2


def test_competing_connections_share_last_rolling_slot(tmp_path: Path) -> None:
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    path = tmp_path / "state.sqlite3"
    limits = replace(LIMITS, rolling_new=1)
    with open_state(path, budget_limits=limits):
        pass
    barrier = Barrier(2)

    def reserve(label: str) -> SlotReservation | BudgetRefusal:
        with open_state(path, budget_limits=limits) as store:
            barrier.wait(timeout=10)
            return store.try_reserve_slot(label, T0.isoformat())

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(reserve, ("service", "campaign")))
    assert sum(isinstance(result, SlotReservation) for result in results) == 1
    assert sum(isinstance(result, BudgetRefusal) for result in results) == 1
    with open_state(path, budget_limits=limits) as store:
        assert store.budget_view().rolling_count(limits, T0.isoformat()) == 1


def test_upgrade_failure_rolls_back_schema_and_timestamp_debt(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "state.sqlite3"
    with open_state(path, budget_limits=LIMITS) as store:
        ts.walk_to_planned(store)
        assert isinstance(ts.prepare(store), SendToken)
    with sqlite3.connect(path) as conn:
        conn.execute("DROP TABLE budget_reservations")
        conn.execute("DELETE FROM schema_version WHERE version = 5")
        conn.execute("PRAGMA user_version = 4")
    migrate = state_module._MIGRATIONS[4]

    def fail_after_ledger(conn: sqlite3.Connection) -> None:
        migrate(conn)
        raise sqlite3.OperationalError("injected upgrade failure")

    with monkeypatch.context() as patch:
        patch.setattr(
            state_module, "_MIGRATIONS", {**state_module._MIGRATIONS, 4: fail_after_ledger}
        )
        with (
            pytest.raises(state_module.StateError, match="migration from schema version 4"),
            open_state(path, budget_limits=LIMITS),
        ):
            pass
    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 4
        assert (
            conn.execute(
                "SELECT name FROM sqlite_master WHERE name = 'budget_reservations'"
            ).fetchone()
            is None
        )
        assert conn.execute("SELECT COUNT(*) FROM sends").fetchone()[0] == 1
    with open_state(path, budget_limits=LIMITS) as store:
        assert store.budget_view().rolling_count(LIMITS, ts.T0) == 1
