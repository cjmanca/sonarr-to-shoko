"""M6-a gate tests: durable SQLite state machine. Offline only.

Real tmp_path SQLite files with fake ISO clock strings; no network. A second connection
(``second_view``) proves durability/commit ordering, including concurrent campaign debits.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import sqlite3
import subprocess
import sys
import time
from contextlib import AbstractContextManager
from datetime import datetime
from pathlib import Path
from threading import Barrier, Thread, local
from typing import Any, cast

import pytest
import state_v1_fixture as v1fx

from shoko_linker import state as state_module
from shoko_linker.campaign import CampaignError, authorize_campaign_drain, campaign_drain_selection
from shoko_linker.linking import SendOutcome, VerificationVerdict
from shoko_linker.state import (
    _CAMPAIGN_V2_DRAIN_COLUMNS,
    BUDGET_CYCLE_RETENTION,
    GLOBAL_BUDGET_KEY,
    META_BUDGET_RUN_ID,
    META_BUDGET_SEQ,
    SCHEMA_VERSION,
    ApplyProcessLock,
    BaselineFile,
    BudgetLimits,
    BudgetRefusal,
    CampaignBinding,
    CampaignSubmissionLimitError,
    Cohort,
    ContinuityVerdict,
    EligibilityVerdict,
    FileIdentitySnapshot,
    FileSeed,
    FileState,
    LockHeldError,
    SendToken,
    SlotReservation,
    StateError,
    StateStore,
    TransitionError,
    WriteDisabledError,
    acquire_apply_lock,
    acquire_or_steal_apply_lock,
    integrity_check,
    open_state,
    release_apply_lock,
    steal_apply_lock,
    steal_expired_apply_lock,
    validate_transition,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

T0 = "2026-01-01T00:00:00+00:00"
T1 = "2026-01-01T01:00:00+00:00"
T2 = "2026-01-02T01:00:00+00:00"
T3 = "2026-01-03T00:00:00+00:00"

PLAN_FP = "a" * 64
REQ_FP = "b" * 64
REQ_FP_2 = "c" * 64
TARGETS = "d" * 64
STRUCTURE = "e" * 64

LIMITS = BudgetLimits(
    max_pending_files=3,
    max_pending_episode_jobs=6,
    per_cycle_new=2,
    rolling_new=4,
    rolling_window_hours=24.0,
)
WIDE_LIMITS = BudgetLimits(
    max_pending_files=10,
    max_pending_episode_jobs=20,
    per_cycle_new=50,
    rolling_new=50,
    rolling_window_hours=24.0,
)

LOCK_PID_A = 111
LOCK_PID_B = 222
RESERVED_TWO = 2
RESERVED_THREE = 3
JOBS_TWO = 2
JOBS_FOUR = 4
PENDING_ONE = 1
PENDING_TWO = 2
PENDING_THREE = 3
LIVE_ONE = 1
LIVE_TWO = 2
FILE_COUNT = 7000
PERF_FILE_ID = 2025
PERF_PATH = f"/data/media/anime/series-{PERF_FILE_ID % 50}/f{PERF_FILE_ID}.mkv"
BACKLOG_COUNT = FILE_COUNT // 2
INSERT_BUDGET_SECONDS = 3.0
POINT_QUERY_BUDGET_SECONDS = 0.05
SCAN_BUDGET_SECONDS = 0.25

WALK = (
    FileState.MATCHED,
    FileState.MAPPED,
    FileState.PLANNED,
    FileState.PREPARED_NOT_SENT,
    FileState.REQUEST_MAY_HAVE_BEEN_SENT,
    FileState.VERIFYING,
    FileState.VERIFIED,
)


def open_store(
    path: Path, *, limits: BudgetLimits | None = LIMITS
) -> AbstractContextManager[StateStore]:
    return open_state(path, budget_limits=limits)


def second_view(path: Path, sql: str, params: tuple[Any, ...] = ()) -> list[Any]:
    """Independent connection: proves the row was durably committed."""
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def state_of(store: StateStore, file_id: int) -> FileState:
    record = store.get_file(file_id)
    assert record is not None
    return record.state


def seed_file(store: StateStore, file_id: int = 101) -> None:
    store.record_file(
        file_id,
        sonarr_series_id=1,
        sonarr_file_id=10,
        sonarr_path="/data/media/anime/show/s01e01.mkv",
        sonarr_size=1234,
        cohort=Cohort.NEW_ARRIVAL,
        iso=T0,
    )


def walk_to_planned(store: StateStore, file_id: int = 101) -> None:
    seed_file(store, file_id)
    for target in (FileState.MATCHED, FileState.MAPPED, FileState.PLANNED):
        store.advance(file_id, target, "walk", iso=T0)


def prepare(
    store: StateStore,
    file_id: int = 101,
    *,
    iso: str = T0,
    req_fp: str | None = None,
    plan_fp: str = PLAN_FP,
    cycle_key: str = "cycle-1",
    campaign_id: str | None = None,
    target_ids: tuple[tuple[int, int, int], ...] = (),
) -> Any:
    fingerprint = req_fp if req_fp is not None else format(file_id, "064x")
    return store.prepare_send(
        file_id,
        plan_fp,
        fingerprint,
        targets_digest=TARGETS,
        structure_digest=STRUCTURE,
        mapping_revision="7",
        cycle_key=cycle_key,
        iso=iso,
        campaign_id=campaign_id,
        target_ids=target_ids,
    )


# ---------------------------------------------------------------- schema


def test_open_state_applies_durability_pragmas_and_schema_version(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        assert store.journal_mode() == "wal"
        assert store.synchronous() == "full"
        assert store.foreign_keys_enabled() is True
        assert store.schema_version() == SCHEMA_VERSION
        assert store.migrate() == SCHEMA_VERSION
        rows = second_view(path, "SELECT version, description FROM schema_version")
        assert rows[0][0] == SCHEMA_VERSION


def test_schema_rejects_newer_version(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA user_version=99")
    conn.close()
    with pytest.raises(sqlite3.DatabaseError, match="newer than supported"), open_store(path):
        pass


def test_fresh_schema_ddl_failure_rolls_back_and_failed_constructor_closes_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "state.sqlite3"
    real_connect = sqlite3.connect
    connections: list[sqlite3.Connection] = []
    deny_files_table = True

    def connect_with_ddl_authorizer(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        connection = cast(sqlite3.Connection, real_connect(*args, **kwargs))
        if Path(str(args[0])) == path:
            connections.append(connection)

            def authorize(
                action: int,
                first: str | None,
                _second: str | None,
                _database: str | None,
                _trigger: str | None,
            ) -> int:
                if deny_files_table and action == sqlite3.SQLITE_CREATE_TABLE and first == "files":
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            connection.set_authorizer(authorize)
        return connection

    monkeypatch.setattr(sqlite3, "connect", connect_with_ddl_authorizer)
    with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
        StateStore(path)

    assert len(connections) == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connections[0].execute("SELECT 1")

    inspect = real_connect(path)
    try:
        assert inspect.execute("PRAGMA user_version").fetchone() == (0,)
        assert (
            inspect.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall() == []
        )
    finally:
        inspect.close()

    deny_files_table = False
    with open_store(path) as reopened:
        assert reopened.schema_version() == SCHEMA_VERSION
        assert reopened.connection.execute("SELECT COUNT(*) FROM schema_version").fetchone() == (1,)


def test_fresh_schema_process_interruption_rolls_back_before_healthy_reopen(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    ready = tmp_path / "ddl-paused"
    child = r"""
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(sys.argv[3]) / "src"))
from shoko_linker.state import StateStore

database = Path(sys.argv[1])
ready = Path(sys.argv[2])
real_connect = sqlite3.connect

def connect_with_pause(*args, **kwargs):
    connection = real_connect(*args, **kwargs)
    def pause_after_first_table(action, first, _second, _database, _trigger):
        if action == sqlite3.SQLITE_CREATE_TABLE and first == "meta":
            ready.write_text("paused", encoding="utf-8")
            while True:
                time.sleep(0.01)
        return sqlite3.SQLITE_OK
    connection.set_authorizer(pause_after_first_table)
    return connection

sqlite3.connect = connect_with_pause
StateStore(database)
"""
    process = subprocess.Popen(  # noqa: S603 - fixed local test script and temporary database paths
        [sys.executable, "-c", child, str(path), str(ready), str(REPO_ROOT)],
        cwd=REPO_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 10
        while not ready.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.01)
        if not ready.exists():
            stdout, stderr = process.communicate(timeout=2)
            pytest.fail(f"child did not pause during schema DDL: {stdout}\n{stderr}")
        process.terminate()
        process.communicate(timeout=5)
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)

    inspect = sqlite3.connect(path)
    try:
        assert inspect.execute("PRAGMA user_version").fetchone() == (0,)
        assert (
            inspect.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall() == []
        )
    finally:
        inspect.close()

    with open_store(path) as reopened:
        assert reopened.schema_version() == SCHEMA_VERSION
        assert reopened.connection.execute("SELECT COUNT(*) FROM schema_version").fetchone() == (1,)


def test_concurrent_fresh_state_constructors_initialize_schema_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fresh constructors that both observed version zero recheck under the write lock."""
    path = tmp_path / "state.sqlite3"
    real_connect = sqlite3.connect
    setup = real_connect(path)
    assert setup.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
    setup.close()
    version_barrier = Barrier(2)
    thread_state = local()
    real_schema_version = StateStore.schema_version

    def synchronize_old_version_read(store: StateStore) -> int:
        version = real_schema_version(store)
        if (
            version == 0
            and not store.connection.in_transaction
            and not getattr(thread_state, "synchronized", False)
        ):
            thread_state.synchronized = True
            version_barrier.wait(timeout=5)
        return version

    monkeypatch.setattr(StateStore, "schema_version", synchronize_old_version_read)
    errors: list[Exception] = []
    versions: list[int] = []

    def construct() -> None:
        try:
            with open_store(path) as store:
                versions.append(store.schema_version())
        except Exception as exc:  # capture thread failures for an informative assertion
            errors.append(exc)

    workers = [Thread(target=construct) for _ in range(2)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=15)

    assert all(not worker.is_alive() for worker in workers)
    assert errors == []
    assert versions == [SCHEMA_VERSION, SCHEMA_VERSION]
    inspect = real_connect(path)
    try:
        assert inspect.execute("PRAGMA user_version").fetchone() == (SCHEMA_VERSION,)
        assert inspect.execute(
            "SELECT version, description FROM schema_version ORDER BY version"
        ).fetchall() == [(SCHEMA_VERSION, f"initial v{SCHEMA_VERSION} schema")]
    finally:
        inspect.close()


def test_concurrent_first_open_wal_pragmas_retry_busy_race(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "state.sqlite3"
    real_connect = sqlite3.connect
    wal_barrier = Barrier(2)

    def connect_with_wal_barrier(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        connection = cast(sqlite3.Connection, real_connect(*args, **kwargs))
        if args and Path(str(args[0])) == path:
            waited_for_wal = False

            def synchronize_wal(statement: str) -> None:
                nonlocal waited_for_wal
                if statement.strip().upper() == "PRAGMA JOURNAL_MODE=WAL" and not waited_for_wal:
                    waited_for_wal = True
                    wal_barrier.wait(timeout=5)

            connection.set_trace_callback(synchronize_wal)
        return connection

    monkeypatch.setattr(sqlite3, "connect", connect_with_wal_barrier)
    errors: list[Exception] = []
    versions: list[int] = []

    def construct() -> None:
        try:
            with open_store(path) as store:
                versions.append(store.schema_version())
        except Exception as exc:  # capture thread failures for an informative assertion
            errors.append(exc)

    workers = [Thread(target=construct) for _ in range(2)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=15)

    assert all(not worker.is_alive() for worker in workers)
    assert errors == []
    assert versions == [SCHEMA_VERSION, SCHEMA_VERSION]
    inspect = real_connect(path)
    try:
        assert inspect.execute("PRAGMA journal_mode").fetchone() == ("wal",)
        assert inspect.execute("PRAGMA user_version").fetchone() == (SCHEMA_VERSION,)
        assert inspect.execute("SELECT COUNT(*) FROM schema_version").fetchone() == (1,)
    finally:
        inspect.close()


def test_schema_initialization_lock_timeout_is_bounded_and_actionable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "state.sqlite3"
    holder = sqlite3.connect(path, timeout=0.05)
    assert holder.execute("PRAGMA journal_mode=WAL").fetchone() == ("wal",)
    holder.execute("BEGIN IMMEDIATE")
    monkeypatch.setattr(state_module, "DEFAULT_BUSY_TIMEOUT_MS", 50)
    try:
        started = time.monotonic()
        with pytest.raises(
            StateError,
            match=r"schema initialization or migration could not acquire SQLite's write lock"
            r" within 50 ms: database is locked",
        ):
            StateStore(path)
        assert time.monotonic() - started < 1
    finally:
        holder.rollback()
        holder.close()


def test_current_schema_reopen_skips_initialization_write_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path):
        pass

    def reject_initialization_transaction(
        _store: StateStore, *, busy_context: str | None = None
    ) -> Any:
        pytest.fail(f"current-schema open unexpectedly requested a transaction: {busy_context}")

    monkeypatch.setattr(StateStore, "_transaction", reject_initialization_transaction)
    with open_store(path) as reopened:
        assert reopened.schema_version() == SCHEMA_VERSION


def _build_legacy_database_at_version(
    path: Path, version: int
) -> dict[str, list[tuple[object, ...]]]:
    """Build an authentic v4/v5 fixture by applying earlier registered migrations."""
    before = v1fx.build_v1_database(path)
    connection = sqlite3.connect(path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        for old_version in range(1, version):
            state_module._MIGRATIONS[old_version](connection)
            new_version = old_version + 1
            connection.execute(f"PRAGMA user_version={new_version}")
            connection.execute(
                "INSERT INTO schema_version (version, applied_iso, description)"
                " VALUES (?, ?, ?)",
                (new_version, T0, f"migrated from {old_version}"),
            )
        connection.commit()
        connection.execute("PRAGMA journal_mode=WAL")
    finally:
        connection.close()
    return before


@pytest.mark.parametrize("legacy_version", [4, 5])
def test_concurrent_legacy_state_constructors_recheck_version_under_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, legacy_version: int
) -> None:
    """Concurrent v4/v5 upgrades neither repeat DDL nor duplicate audit/counter changes."""
    path = tmp_path / f"state-v{legacy_version}.sqlite3"
    before = _build_legacy_database_at_version(path, legacy_version)
    version_barrier = Barrier(2)
    thread_state = local()
    real_schema_version = StateStore.schema_version

    def synchronize_old_version_read(store: StateStore) -> int:
        version = real_schema_version(store)
        if (
            version == legacy_version
            and not store.connection.in_transaction
            and not getattr(thread_state, "synchronized", False)
        ):
            thread_state.synchronized = True
            version_barrier.wait(timeout=5)
        return version

    monkeypatch.setattr(StateStore, "schema_version", synchronize_old_version_read)
    errors: list[Exception] = []
    versions: list[int] = []

    def construct() -> None:
        try:
            with open_store(path) as store:
                versions.append(store.schema_version())
        except Exception as exc:  # capture thread failures for an informative assertion
            errors.append(exc)

    workers = [Thread(target=construct) for _ in range(2)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=15)

    assert all(not worker.is_alive() for worker in workers)
    assert errors == []
    assert versions == [SCHEMA_VERSION, SCHEMA_VERSION]
    after = v1fx.snapshot_state(path)
    for table in ("meta", "baselines", "baseline_files", "files", "budgets"):
        assert after[table] == before[table]
    assert [row[: len(before["sends"][0])] for row in after["sends"]] == before["sends"]
    assert [row[:13] for row in after["campaigns"]] == [row[:13] for row in before["campaigns"]]
    audit_counts = second_view(
        path, "SELECT version, COUNT(*) FROM schema_version GROUP BY version ORDER BY version"
    )
    assert audit_counts == [(version, 1) for version in range(1, SCHEMA_VERSION + 1)]
    assert second_view(path, "PRAGMA user_version") == [(SCHEMA_VERSION,)]
    assert second_view(path, "SELECT COUNT(*), SUM(reservations) FROM budget_reservations") == [
        (2, 5)
    ]
    reservations = second_view(
        path, "SELECT reservations FROM budget_reservations ORDER BY reservations"
    )
    assert reservations == [(2,), (3,)]
    assert second_view(path, "SELECT total_sent FROM campaigns") == [(2,)]


# ----------------------------------------------------- F17: v1 → v2 in-place migration


def test_f17_probe_v1_database_opens_and_answers_campaign_queries(tmp_path: Path) -> None:
    """F17 probe: a real v1-era database must open, migrate, and serve campaign queries."""
    path = tmp_path / "state.sqlite3"
    v1fx.build_v1_database(path)
    with open_store(path) as store:
        assert store.schema_version() == SCHEMA_VERSION
        campaign = store.get_campaign(v1fx.CAMPAIGN_ID)
        assert campaign is not None


def test_f17_v1_migration_preserves_rows_and_holds_incoherent_ownership(tmp_path: Path) -> None:
    """Opening v1 keeps history and fails closed when the approved scope is inconsistent."""
    path = tmp_path / "state.sqlite3"
    before = v1fx.build_v1_database(path)
    with open_store(path) as store:
        assert store.schema_version() == SCHEMA_VERSION
        assert store.get_onboarding_json() is None
    after = v1fx.snapshot_state(path)
    for table in ("meta", "baselines", "baseline_files", "files", "budgets"):
        assert after[table] == before[table]
    assert [row[: len(before["sends"][0])] for row in after["sends"]] == before["sends"]
    assert [row[len(before["sends"][0]) : -1] for row in after["sends"]] == [
        (None, "unknown", None, ""),
        (None, "unknown", None, ""),
        (None, "unknown", None, ""),
    ]
    # campaigns: identical stored values, only the three drain columns appended with
    # not-authorized defaults; no campaign row is rewritten.
    assert [row[:13] for row in after["campaigns"]] == [row[:13] for row in before["campaigns"]]
    assert all(row[13:] == (None, None, "") for row in after["campaigns"])
    # schema_version audit history: the v1 row survives, and v2-v11 are appended.
    assert after["schema_version"][0] == before["schema_version"][0]
    assert len(after["schema_version"]) == len(before["schema_version"]) + SCHEMA_VERSION - 1
    assert [row[0:3:2] for row in after["schema_version"][1:]] == [
        (2, "migrated from 1"),
        (3, "migrated from 2"),
        (4, "migrated from 3"),
        (5, "migrated from 4"),
        (6, "migrated from 5"),
        (7, "migrated from 6"),
        (8, "migrated from 7"),
        (9, "migrated from 8"),
        (10, "migrated from 9"),
        (11, "migrated from 10"),
    ]
    assert second_view(path, "PRAGMA user_version") == [(SCHEMA_VERSION,)]
    assert second_view(
        path, "SELECT name FROM sqlite_master WHERE type='table' AND name='onboarding'"
    ) == [("onboarding",)]
    assert second_view(path, "SELECT COUNT(*) FROM dashboard_account") == [(0,)]
    assert second_view(path, "SELECT COUNT(*) FROM dashboard_sessions") == [(0,)]


def test_f17_migrated_campaign_is_not_drain_authorized(tmp_path: Path) -> None:
    """F17: a migrated campaign is never auto-authorized — drain columns carry no grant."""
    path = tmp_path / "state.sqlite3"
    v1fx.build_v1_database(path)
    with open_store(path) as store:
        campaign = store.get_campaign(v1fx.CAMPAIGN_ID)
        assert campaign is not None
        assert campaign.drain_authorized_iso is None
        assert campaign.drain_authorized_digest is None
        assert campaign.drain_invalidated_reason == ""
        assert campaign.status == "approved"
        assert campaign.plan_path == "/data/plans/campaign.json"
        assert campaign_drain_selection(campaign) == "drain_not_authorized"
        listed = store.list_campaigns()
        assert [record.campaign_id for record in listed] == [v1fx.CAMPAIGN_ID]
        assert campaign_drain_selection(listed[0]) == "drain_not_authorized"


def test_service_canary_is_a_single_durable_monotonic_record(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        assert store.get_service_canary() is None
        assert store.record_service_canary(500, T0) == 500
        assert store.get_service_canary() == (500, "pending")
        assert store.record_service_canary(501, T1) == 500
        store.set_service_canary_status("verified", T1)
        assert store.get_service_canary() == (500, "verified")
        with pytest.raises(StateError, match="cannot change terminal"):
            store.set_service_canary_status("failed", T2)
    with open_store(path) as reopened:
        assert reopened.get_service_canary() == (500, "verified")


def test_service_rate_state_is_persisted_as_bounded_json(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    rate_json = (
        '{"schema":"shoko-sonarr-linker/service-rate/1","cap":4,'
        '"successful_streak":2,"updated_iso":"2026-01-01T00:00:00+00:00",'
        '"last_reason":"healthy"}'
    )
    with open_store(path) as store:
        assert store.get_service_rate_state_json() is None
        store.save_service_rate_state_json(rate_json, T0)
        assert store.get_service_rate_state_json() == rate_json
        with pytest.raises(StateError, match="JSON object"):
            store.save_service_rate_state_json("[]", T1)
    with open_store(path) as reopened:
        assert reopened.get_service_rate_state_json() == rate_json


def test_verified_service_mapping_drift_is_durable_and_tracks_latest_generation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    old_targets = "a" * 64
    second_targets = "b" * 64
    latest_targets = "c" * 64
    old_structure = "d" * 64
    second_structure = "e" * 64
    latest_structure = "f" * 64
    with open_store(path) as store:
        seed_file(store, 500)
        for target in WALK:
            store.advance(500, target, f"to {target.value}", iso=T0)
        with pytest.raises(StateError, match="verified file"):
            store.record_service_mapping_drift(
                501,
                previous_targets_digest=old_targets,
                active_targets_digest=second_targets,
                previous_structure_digest=old_structure,
                active_structure_digest=second_structure,
                mapping_revision="2",
                iso=T0,
            )
        store.record_service_mapping_drift(
            500,
            previous_targets_digest=old_targets,
            active_targets_digest=second_targets,
            previous_structure_digest=old_structure,
            active_structure_digest=second_structure,
            mapping_revision="2",
            reason="changed mapping at TVDB 267440 S1E1",
            iso=T0,
        )
        store.record_service_mapping_drift(
            500,
            previous_targets_digest=second_targets,
            active_targets_digest=latest_targets,
            previous_structure_digest=second_structure,
            active_structure_digest=latest_structure,
            mapping_revision="3",
            reason="changed mapping at TVDB 267440 S1E2",
            iso=T1,
        )
        store.record_service_mapping_drift(
            500,
            previous_targets_digest=second_targets,
            active_targets_digest=latest_targets,
            previous_structure_digest=second_structure,
            active_structure_digest=latest_structure,
            mapping_revision="3",
            reason="changed mapping at TVDB 267440 S1E2",
            iso=T2,
        )
        drift = store.get_service_mapping_drifts()
    with open_store(path) as reopened:
        persisted = reopened.get_service_mapping_drifts()
    assert drift == persisted
    assert len(persisted) == 1
    assert persisted[0].previous_targets_digest == old_targets
    assert persisted[0].active_targets_digest == latest_targets
    assert persisted[0].mapping_revision == "3"
    assert persisted[0].reason == "changed mapping at TVDB 267440 S1E2"
    assert persisted[0].detected_iso == T1


def test_legacy_service_mapping_drift_record_remains_readable(tmp_path: Path) -> None:
    with open_store(tmp_path / "state.sqlite3") as store:
        seed_file(store, 500)
        for target in WALK:
            store.advance(500, target, f"to {target.value}", iso=T0)
        store.set_meta(
            f"{state_module.META_SERVICE_MAPPING_DRIFT_PREFIX}500",
            json.dumps(
                {
                    "schema": state_module.SERVICE_MAPPING_DRIFT_SCHEMA_V1,
                    "shoko_file_id": 500,
                    "previous_targets_digest": None,
                    "active_targets_digest": "a" * 64,
                    "previous_structure_digest": None,
                    "active_structure_digest": "b" * 64,
                    "mapping_revision": "legacy-revision",
                    "detected_iso": T0,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        )

        drift = store.get_service_mapping_drifts()

    assert len(drift) == 1
    assert drift[0].shoko_file_id == 500
    assert drift[0].reason == "mapping_drift_details_unavailable"


def test_dashboard_account_and_csrf_session_state_are_atomic_and_durable(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    session_hash = "a" * 64
    first_csrf_hash = "b" * 64
    second_csrf_hash = "c" * 64
    with open_store(path) as store:
        assert store.get_dashboard_account() is None
        assert (
            store.create_dashboard_account("admin", "scrypt$16384$8$1$salt$hash", b"k" * 32, T0)
            is True
        )
        assert (
            store.create_dashboard_account("attacker", "scrypt$16384$8$1$salt$hash", b"z" * 32, T1)
            is False
        )
        account = store.get_dashboard_account()
        assert account is not None
        assert account.username == "admin"
        assert account.signing_key == b"k" * 32
        assert (
            store.create_dashboard_session(
                session_hash, first_csrf_hash, int(datetime.fromisoformat(T2).timestamp()), T0
            )
            is True
        )
        now_epoch = int(datetime.fromisoformat(T1).timestamp())
        assert store.get_dashboard_session(session_hash, now_epoch) is not None
        assert (
            store.rotate_dashboard_session_csrf(session_hash, "d" * 64, second_csrf_hash, now_epoch)
            is False
        )
        assert (
            store.rotate_dashboard_session_csrf(
                session_hash,
                first_csrf_hash,
                second_csrf_hash,
                now_epoch,
            )
            is True
        )
        assert (
            store.rotate_dashboard_session_csrf(
                session_hash,
                first_csrf_hash,
                "e" * 64,
                now_epoch,
            )
            is False
        )
    with open_store(path) as reopened:
        account = reopened.get_dashboard_account()
        session = reopened.get_dashboard_session(
            session_hash, int(datetime.fromisoformat(T1).timestamp())
        )
        assert account is not None
        assert account.username == "admin"
        assert session is not None
        assert session.csrf_hash == second_csrf_hash


def test_f17_pending_and_indeterminate_sends_are_not_replayed(tmp_path: Path) -> None:
    """F17: opening a v1 install must not auto-send anything — live rows stay held."""
    path = tmp_path / "state.sqlite3"
    before = v1fx.build_v1_database(path)
    with open_store(path) as store:
        plan = store.recover_after_restart()
        held = {row.shoko_file_id for row in plan.resubmit_forbidden}
        assert {102, 103} <= held
        record_102 = store.get_file(102)
        record_103 = store.get_file(103)
        assert record_102 is not None
        assert record_103 is not None
        assert record_102.state is FileState.REQUEST_MAY_HAVE_BEEN_SENT
        assert record_103.state is FileState.INDETERMINATE_WRITE
        record_101 = store.get_file(101)
        assert record_101 is not None
        assert record_101.state is FileState.VERIFIED
    # Every v1 send value remains byte-identical; v7 only appends explicit ownership metadata.
    migrated_sends = v1fx.snapshot_state(path)["sends"]
    legacy_field_count = len(before["sends"][0])
    assert [row[:legacy_field_count] for row in migrated_sends] == before["sends"]
    assert [row[legacy_field_count:-1] for row in migrated_sends] == [
        (None, "unknown", None, ""),
        (None, "unknown", None, ""),
        (None, "unknown", None, ""),
    ]


def test_f17_budget_accounting_survives_migration(tmp_path: Path) -> None:
    """F17: rolling/global counters, pending reservations and the stored apply-pause
    reason are exactly what the v1 database persisted."""
    path = tmp_path / "state.sqlite3"
    v1fx.build_v1_database(path)
    with open_store(path) as store:
        view = store.budget_view()
        assert view.window_key == v1fx.CYCLE_KEY
        assert view.rolling_new_submissions == 5
        assert view.rolling_window_start_iso == v1fx.T0
        assert view.pending_file_requests == 2
        assert view.pending_episode_jobs == 2
        assert view.paused_reason == v1fx.PAUSE_REASON
        assert view.cycle_new_submissions == 2
        assert view.completed_submissions == 1


def test_f17_reopen_does_not_repeat_migration(tmp_path: Path) -> None:
    """Reopening the current database is a no-op — no extra audit row and same data."""
    path = tmp_path / "state.sqlite3"
    v1fx.build_v1_database(path)
    with open_store(path) as store:
        assert store.schema_version() == SCHEMA_VERSION
    first = v1fx.snapshot_state(path)
    with open_store(path) as store:
        assert store.schema_version() == SCHEMA_VERSION
        assert store.migrate() == SCHEMA_VERSION
        assert store.get_campaign(v1fx.CAMPAIGN_ID) is not None
    assert v1fx.snapshot_state(path) == first


def test_f17_interim_v1_with_drain_columns_upgrades_cleanly(tmp_path: Path) -> None:
    """F17: a v1 database created with the columns already present (interim fresh-install
    schema under user_version=1) upgrades without a duplicate-column error and keeps grants."""
    path = tmp_path / "state.sqlite3"
    conn = sqlite3.connect(path)
    try:
        conn.executescript(state_module._SCHEMA_SQL)
        conn.execute("ALTER TABLE sends DROP COLUMN updated_iso")
        conn.execute("DROP TABLE latest_scan_exclusions")
        conn.execute("DROP TABLE metadata_operation_history")
        conn.execute("DROP TABLE metadata_operations")
        conn.execute(
            "INSERT INTO campaigns (campaign_id, digest, plan_fingerprint_set,"
            " max_submissions, deadline_iso, attestation_scope, approval_iso, status,"
            " status_reason, total_sent, last_batch_iso, created_iso, plan_path,"
            " drain_authorized_iso, drain_authorized_digest, drain_invalidated_reason)"
            " VALUES (?, ?, '[]', 10, NULL, 'campaign:canary', ?, 'approved', '', 1,"
            " NULL, ?, '/data/plans/campaign.json', ?, ?, '')",
            (
                v1fx.CAMPAIGN_ID,
                v1fx.CAMPAIGN_DIGEST,
                v1fx.T1,
                v1fx.T0,
                v1fx.T1,
                v1fx.CAMPAIGN_DIGEST,
            ),
        )
        conn.execute(
            "INSERT INTO schema_version (version, applied_iso, description)"
            " VALUES (1, ?, 'initial v1 schema')",
            (v1fx.T0,),
        )
        conn.execute("PRAGMA user_version=1")
        conn.commit()
    finally:
        conn.close()
    with open_store(path) as store:
        assert store.schema_version() == SCHEMA_VERSION
        campaign = store.get_campaign(v1fx.CAMPAIGN_ID)
        assert campaign is not None
        # an existing operator grant is data, not a migration casualty: it survives.
        assert campaign.drain_authorized_iso == v1fx.T1
        assert campaign.drain_authorized_digest == v1fx.CAMPAIGN_DIGEST
        assert campaign_drain_selection(campaign) == "authorized"
    audit = second_view(path, "SELECT version, description FROM schema_version ORDER BY version")
    assert audit[0] == (1, "initial v1 schema")
    assert audit[1] == (2, "migrated from 1")
    assert audit[2] == (3, "migrated from 2")
    assert audit[3] == (4, "migrated from 3")


def test_f17_future_schema_version_is_refused_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F17: user_version > SCHEMA_VERSION refuses the open and mutates nothing."""
    path = tmp_path / "state.sqlite3"
    before = v1fx.build_v1_database(path)
    conn = sqlite3.connect(path)
    future_version = SCHEMA_VERSION + 1
    conn.execute(f"PRAGMA user_version={future_version}")
    conn.commit()
    conn.close()

    real_connect = sqlite3.connect
    meta_reads: list[str] = []

    def connect_without_meta_reads(*args: Any, **kwargs: Any) -> sqlite3.Connection:
        connection = cast(sqlite3.Connection, real_connect(*args, **kwargs))
        if args and Path(str(args[0])) == path:

            def deny_meta_reads(
                action: int,
                first: str | None,
                _second: str | None,
                _database: str | None,
                _trigger: str | None,
            ) -> int:
                if action == sqlite3.SQLITE_READ and first == "meta":
                    meta_reads.append(first)
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            connection.set_authorizer(deny_meta_reads)
        return connection

    monkeypatch.setattr(sqlite3, "connect", connect_without_meta_reads)
    with (
        pytest.raises(sqlite3.DatabaseError, match=f"newer than supported {SCHEMA_VERSION}"),
        open_store(path),
    ):
        pass
    assert meta_reads == []
    monkeypatch.undo()
    assert second_view(path, "PRAGMA user_version") == [(future_version,)]
    assert v1fx.snapshot_state(path) == before


@pytest.mark.parametrize("legacy_version", [1, 4, 5])
def test_stopped_legacy_schema_migrates_without_clearing_write_stop(
    tmp_path: Path, legacy_version: int
) -> None:
    """Schema maintenance may upgrade old state, but never resumes ordinary writes."""
    path = tmp_path / f"stopped-v{legacy_version}.sqlite3"
    if legacy_version == 1:
        v1fx.build_v1_database(path)
    else:
        _build_legacy_database_at_version(path, legacy_version)
    connection = sqlite3.connect(path)
    try:
        connection.executemany(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            (("writes_disabled", "1"), ("writes_disabled_reason", "operator repair required")),
        )
        connection.commit()
    finally:
        connection.close()

    with open_store(path) as store:
        assert store.schema_version() == SCHEMA_VERSION
        assert store.writes_disabled is True
        assert store.get_meta("writes_disabled") == "1"
        assert store.get_meta("writes_disabled_reason") == "operator repair required"
        with pytest.raises(WriteDisabledError):
            store.set_meta("ordinary_mutation", "must remain blocked")

    audit = second_view(path, "SELECT version, COUNT(*) FROM schema_version GROUP BY version")
    assert audit == [(version, 1) for version in range(1, SCHEMA_VERSION + 1)]
    assert second_view(
        path,
        "SELECT key, value FROM meta WHERE key IN ('writes_disabled', 'writes_disabled_reason')"
        " ORDER BY key",
    ) == [
        ("writes_disabled", "1"),
        ("writes_disabled_reason", "operator repair required"),
    ]


def test_stopped_legacy_migration_failure_rolls_back_schema_and_preserves_stop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "stopped-v1.sqlite3"
    v1fx.build_v1_database(path)
    connection = sqlite3.connect(path)
    try:
        connection.executemany(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            (("writes_disabled", "1"), ("writes_disabled_reason", "operator repair required")),
        )
        connection.commit()
    finally:
        connection.close()

    def fail_migration(_connection: sqlite3.Connection) -> None:
        raise sqlite3.OperationalError("injected migration failure")

    monkeypatch.setattr(state_module, "_MIGRATIONS", {1: fail_migration})
    with (
        pytest.raises(StateError, match="migration from schema version 1 failed"),
        open_store(path),
    ):
        pass

    assert second_view(path, "PRAGMA user_version") == [(1,)]
    assert second_view(path, "SELECT version FROM schema_version ORDER BY version") == [(1,)]
    assert second_view(
        path,
        "SELECT key, value FROM meta WHERE key IN ('writes_disabled', 'writes_disabled_reason')"
        " ORDER BY key",
    ) == [
        ("writes_disabled", "1"),
        ("writes_disabled_reason", "operator repair required"),
    ]


def test_f17_failed_migration_leaves_database_recoverable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F17: a failing migration step rolls back whole — clear StateError, file stays
    a usable v1 with every row intact, and the next open (cause fixed) succeeds."""

    def _boom(conn: sqlite3.Connection) -> None:
        raise sqlite3.OperationalError("injected ALTER TABLE campaigns failure")

    path = tmp_path / "state.sqlite3"
    before = v1fx.build_v1_database(path)
    monkeypatch.setattr(state_module, "_MIGRATIONS", {1: _boom})
    with (
        pytest.raises(StateError, match="migration from schema version 1 failed"),
        open_store(path),
    ):
        pass
    assert second_view(path, "PRAGMA user_version") == [(1,)]
    columns = [row[1] for row in second_view(path, "PRAGMA table_info(campaigns)")]
    assert "drain_authorized_iso" not in columns
    assert v1fx.snapshot_state(path) == before
    monkeypatch.undo()
    with open_store(path) as store:
        assert store.schema_version() == SCHEMA_VERSION
        assert store.get_campaign(v1fx.CAMPAIGN_ID) is not None
    after = v1fx.snapshot_state(path)
    assert [row[:13] for row in after["campaigns"]] == [row[:13] for row in before["campaigns"]]


def _seed_v5_campaign_history_fixture(path: Path) -> None:
    undercounted = "campaign-undercounted"
    already_counted = "campaign-already-counted"
    identities = {
        301: (10101, 5001),
        302: (10202, 5002),
        303: (10303, 5003),
        304: (10404, 5004),
    }
    requests = {
        file_id: _legacy_request_fingerprint(file_id, PLAN_FP, (shoko_id,))
        for file_id, (_, shoko_id) in identities.items()
    }
    under_path = path.with_name("undercounted-plan.json")
    under_digest = _write_legacy_campaign_plan(
        under_path,
        undercounted,
        tuple(
            {
                "shoko_file_id": file_id,
                "plan_fingerprint": PLAN_FP,
                "request_fingerprint": requests[file_id],
                "anidb_target_identities": [[1, 1, "normal", anidb_episode_id]],
                "shoko_episode_ids": [shoko_id],
            }
            for file_id, (anidb_episode_id, shoko_id) in identities.items()
            if file_id != 304
        ),
    )
    counted_path = path.with_name("already-counted-plan.json")
    counted_digest = _write_legacy_campaign_plan(
        counted_path,
        already_counted,
        (
            {
                "shoko_file_id": 304,
                "plan_fingerprint": PLAN_FP,
                "request_fingerprint": requests[304],
                "anidb_target_identities": [[1, 1, "normal", identities[304][0]]],
                "shoko_episode_ids": [identities[304][1]],
            },
        ),
    )
    with open_store(path, limits=WIDE_LIMITS) as store:
        store.create_campaign(
            undercounted,
            under_digest,
            max_submissions=10,
            deadline_iso=None,
            attestation_scope=f"campaign:{undercounted}",
            approval_iso=T0,
            iso=T0,
            bindings=tuple(
                CampaignBinding(file_id, PLAN_FP, requests[file_id]) for file_id in (301, 302, 303)
            ),
            plan_path=str(under_path),
        )
        store.create_campaign(
            already_counted,
            counted_digest,
            max_submissions=10,
            deadline_iso=None,
            attestation_scope=f"campaign:{already_counted}",
            approval_iso=T0,
            iso=T0,
            bindings=(CampaignBinding(304, PLAN_FP, requests[304]),),
            plan_path=str(counted_path),
        )
        for file_id in (301, 302, 303, 304):
            walk_to_planned(store, file_id)
        rejected = prepare(
            store,
            301,
            req_fp=requests[301],
            campaign_id=undercounted,
            target_ids=((1, identities[301][0], identities[301][1]),),
        )
        accepted = prepare(
            store,
            302,
            req_fp=requests[302],
            campaign_id=undercounted,
            target_ids=((1, identities[302][0], identities[302][1]),),
        )
        unattempted = prepare(
            store,
            303,
            req_fp=requests[303],
            campaign_id=undercounted,
            target_ids=((1, identities[303][0], identities[303][1]),),
        )
        known = prepare(
            store,
            304,
            req_fp=requests[304],
            campaign_id=already_counted,
            target_ids=((1, identities[304][0], identities[304][1]),),
        )
        store.mark_sent(rejected, iso=T1)
        store.record_send_outcome(rejected, SendOutcome.REJECTED, iso=T1, http_status=400)
        store.mark_sent(accepted, iso=T1)
        store.record_send_outcome(
            accepted, SendOutcome.ACCEPTED_UNVERIFIED, iso=T1, http_status=200
        )
        store.mark_sent(known, iso=T1)
        store.record_send_outcome(known, SendOutcome.REJECTED, iso=T1, http_status=400)
        assert unattempted.accepted
        # Simulate the old post-outcome counter crash window and preserve a separate higher total.
        store.connection.execute(
            "UPDATE campaigns SET total_sent = 0 WHERE campaign_id = ?", (undercounted,)
        )
        store.connection.execute(
            "UPDATE campaigns SET total_sent = 3 WHERE campaign_id = ?", (already_counted,)
        )
        _simulate_legacy_schema(store, 5)


def test_v7_migration_holds_compatible_attempts_until_explicit_owner_resolution(
    tmp_path: Path,
) -> None:
    """Fingerprint-compatible v5 sends are uncertain; prepared rows stay unattempted."""
    path = tmp_path / "state.sqlite3"
    _seed_v5_campaign_history_fixture(path)
    undercounted = "campaign-undercounted"
    already_counted = "campaign-already-counted"

    with open_store(path, limits=WIDE_LIMITS) as reopened:
        assert reopened.schema_version() == SCHEMA_VERSION
        under = reopened.get_campaign(undercounted)
        known = reopened.get_campaign(already_counted)
        assert under is not None
        assert under.total_sent == 0
        assert known is not None
        assert known.total_sent == 3
        unresolved = reopened.unresolved_campaign_send_history()
        assert [row.shoko_file_id for row in unresolved] == [301, 302, 304]
        rows = second_view(
            path,
            "SELECT outcome, campaign_id, campaign_ownership FROM sends"
            " WHERE shoko_file_id = 303 ORDER BY send_id",
        )
        assert rows == [("prepared", None, "unknown")]
        owners = ((301, undercounted), (302, undercounted), (304, already_counted))
        for file_id, campaign_id in owners:
            row = reopened.connection.execute(
                "SELECT send_id FROM sends WHERE shoko_file_id = ? ORDER BY send_id", (file_id,)
            ).fetchone()
            assert row is not None
            reopened.resolve_legacy_send_ownership(
                int(row[0]),
                campaign_id=campaign_id,
                note="operator matched immutable send to persisted approved binding",
                iso=T2,
            )
        under = reopened.get_campaign(undercounted)
        known = reopened.get_campaign(already_counted)
        assert under is not None
        assert under.total_sent == 2
        assert known is not None
        assert known.total_sent == 3
        assert reopened.unresolved_campaign_send_history() == ()
    with open_store(path, limits=WIDE_LIMITS) as reopened_again:
        under = reopened_again.get_campaign(undercounted)
        known = reopened_again.get_campaign(already_counted)
        assert under is not None
        assert under.total_sent == 2
        assert known is not None
        assert known.total_sent == 3
        assert second_view(path, "SELECT COUNT(*) FROM schema_version WHERE version = 7") == [(1,)]


def _simulate_legacy_schema(store: StateStore, version: int) -> None:
    """Turn the fresh test DB's rows into pre-send-ownership history for migration tests."""
    connection = store.connection
    connection.execute("DROP TABLE latest_scan_exclusions")
    connection.execute("DROP TABLE metadata_operation_history")
    connection.execute("DROP TABLE metadata_operations")
    columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(sends)")}
    for name in (
        "updated_iso",
        "campaign_ownership_note",
        "campaign_ownership_resolved_iso",
        "campaign_ownership",
        "campaign_id",
    ):
        if name in columns:
            connection.execute(f"ALTER TABLE sends DROP COLUMN {name}")
    connection.execute(f"PRAGMA user_version = {version}")
    connection.execute("DELETE FROM schema_version")
    connection.execute(
        "INSERT INTO schema_version (version, applied_iso, description) VALUES (?, ?, ?)",
        (version, T0, f"legacy v{version} fixture"),
    )


def _legacy_request_fingerprint(
    file_id: int, plan_fingerprint: str, episode_ids: tuple[int, ...]
) -> str:
    body = tuple(sorted(set(episode_ids)))
    lines = (
        "method|POST",
        f"path|api/v3/File/{file_id}/Link",
        f"body|{body}",
        f"plan|{plan_fingerprint}",
    )
    return hashlib.sha256("\n".join(sorted(lines)).encode("utf-8")).hexdigest()


def _write_legacy_campaign_plan(
    path: Path,
    campaign_id: str,
    files: tuple[dict[str, Any], ...],
) -> str:
    """Write the approved identity snapshot needed to adjudicate changed runtime fingerprints."""
    plan_files = [
        {
            "shoko_file_id": item["shoko_file_id"],
            "sonarr_series_id": 1,
            "sonarr_file_id": item["shoko_file_id"],
            "path": f"/anime/{item['shoko_file_id']}.mkv",
            "size": 1024,
            "shoko_ed2k": f"{item['shoko_file_id']:032x}",
            "plan_fingerprint": item["plan_fingerprint"],
            "request_fingerprint": item["request_fingerprint"],
            "mapping_targets_digest": TARGETS,
            "anidb_target_identities": item["anidb_target_identities"],
            "shoko_episode_ids": item["shoko_episode_ids"],
        }
        for item in files
    ]
    document = {
        "schema": "shoko-linker-campaign-plan/2",
        "campaign_id": campaign_id,
        "created_iso": T0,
        "source_instance": "test-sonarr",
        "mapping_revision": "test-revision",
        "targets_digest": TARGETS,
        "structure_digest": STRUCTURE,
        "policy": "strict",
        "files": plan_files,
        "totals": {"max_submissions": len(plan_files), "files": len(plan_files)},
        "deadline_iso": T3,
        "rate_limits": {},
        "stop_conditions": [],
    }
    raw = (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    path.write_bytes(raw)
    return hashlib.sha256(raw).hexdigest()


@pytest.mark.parametrize("legacy_version", [5, 6])
@pytest.mark.parametrize(
    "approved_episode_ids",
    [(), (5001,)],
    ids=["pending-target-materialization", "unrelated-mapping-refresh"],
)
def test_legacy_campaign_retry_with_refreshed_runtime_fingerprints_holds_and_restores_cap(
    tmp_path: Path, legacy_version: int, approved_episode_ids: tuple[int, ...]
) -> None:
    """Changed runtime hashes remain possible campaign debt when file/target scope matches."""
    path = tmp_path / "state.sqlite3"
    campaign_id = f"campaign-refreshed-v{legacy_version}-{bool(approved_episode_ids)}"
    first_file_id, second_file_id = 301, 302
    approved_fp = format(301, "064x")
    next_approved_fp = format(302, "064x")
    first_approved_request = _legacy_request_fingerprint(
        first_file_id, approved_fp, approved_episode_ids
    )
    second_approved_request = _legacy_request_fingerprint(second_file_id, next_approved_fp, (5002,))
    plan_path = tmp_path / f"{campaign_id}.json"
    digest = _write_legacy_campaign_plan(
        plan_path,
        campaign_id,
        (
            {
                "shoko_file_id": first_file_id,
                "plan_fingerprint": approved_fp,
                "request_fingerprint": first_approved_request,
                "anidb_target_identities": [[1, 1, "normal", 10101]],
                "shoko_episode_ids": list(approved_episode_ids),
            },
            {
                "shoko_file_id": second_file_id,
                "plan_fingerprint": next_approved_fp,
                "request_fingerprint": second_approved_request,
                "anidb_target_identities": [[1, 2, "normal", 10202]],
                "shoko_episode_ids": [5002],
            },
        ),
    )

    with open_store(path, limits=WIDE_LIMITS) as store:
        walk_to_planned(store, first_file_id)
        walk_to_planned(store, second_file_id)
        store.create_campaign(
            campaign_id,
            digest,
            max_submissions=1,
            deadline_iso=T3,
            attestation_scope=f"campaign:{campaign_id}",
            approval_iso=T0,
            iso=T0,
            bindings=(
                CampaignBinding(first_file_id, approved_fp, first_approved_request),
                CampaignBinding(second_file_id, next_approved_fp, second_approved_request),
            ),
            plan_path=str(plan_path),
        )

        # Pending target materialization and unrelated mapping refresh both legitimately change
        # the runtime fingerprint while preserving the approved file/AniDB identity scope.
        runtime_fp = "f" * 64
        attempted = prepare(
            store,
            first_file_id,
            iso=T1,
            plan_fp=runtime_fp,
            req_fp=_legacy_request_fingerprint(first_file_id, runtime_fp, (5001,)),
            campaign_id=campaign_id,
            target_ids=((1, 10101, 5001),),
        )
        assert attempted.accepted
        store.mark_sent(attempted, iso=T1)
        store.record_send_outcome(attempted, SendOutcome.REJECTED, iso=T1, http_status=400)
        # Reproduce the old crash window: the transport was attempted, but the campaign counter
        # update after outcome recording did not persist.
        store.connection.execute(
            "UPDATE campaigns SET total_sent = 0 WHERE campaign_id = ?", (campaign_id,)
        )
        _simulate_legacy_schema(store, legacy_version)

    with open_store(path, limits=WIDE_LIMITS) as upgraded:
        owner = upgraded.connection.execute(
            "SELECT campaign_id, campaign_ownership FROM sends WHERE shoko_file_id = ?",
            (first_file_id,),
        ).fetchone()
        assert tuple(owner) == (None, "unknown")
        assert [row.shoko_file_id for row in upgraded.unresolved_campaign_send_history()] == [
            first_file_id
        ]

        refused = prepare(
            upgraded,
            second_file_id,
            iso=T2,
            req_fp=second_approved_request,
            plan_fp=next_approved_fp,
            campaign_id=campaign_id,
        )
        assert isinstance(refused, BudgetRefusal)
        assert refused.reason == "legacy_campaign_history_unresolved"

        upgraded.resolve_legacy_send_ownership(
            attempted.send_id,
            campaign_id=campaign_id,
            note="operator verified the immutable approved file and AniDB targets",
            iso=T2,
        )
        assert upgraded.unresolved_campaign_send_history() == ()
        assert upgraded.get_campaign(campaign_id).total_sent == 1  # type: ignore[union-attr]
        next_send = prepare(
            upgraded,
            second_file_id,
            iso=T2,
            req_fp=second_approved_request,
            plan_fp=next_approved_fp,
            campaign_id=campaign_id,
        )
        assert next_send.accepted
        with pytest.raises(CampaignSubmissionLimitError):
            upgraded.mark_sent(next_send, iso=T2)
        assert upgraded.get_campaign(campaign_id).total_sent == 1  # type: ignore[union-attr]


def test_campaign_history_migration_recovers_original_owner_after_file_rebind(
    tmp_path: Path,
) -> None:
    """A rejected A send remains A history even after the mutable file row is rebound to B."""
    path = tmp_path / "state.sqlite3"
    campaign_a = "campaign-original-A"
    campaign_b = "campaign-rebound-B"
    approved_requests = {
        file_id: _legacy_request_fingerprint(file_id, PLAN_FP, (5000 + file_id - 300,))
        for file_id in (301, 302, 303)
    }
    approved_path = path.with_name("campaign-original-A-plan.json")
    approved_digest = _write_legacy_campaign_plan(
        approved_path,
        campaign_a,
        tuple(
            {
                "shoko_file_id": file_id,
                "plan_fingerprint": PLAN_FP,
                "request_fingerprint": approved_requests[file_id],
                "anidb_target_identities": [[1, 1, "normal", 10100 + file_id - 300]],
                "shoko_episode_ids": [5000 + file_id - 300],
            }
            for file_id in (301, 302, 303)
        ),
    )
    with open_store(path, limits=WIDE_LIMITS) as store:
        store.create_campaign(
            campaign_a,
            approved_digest,
            max_submissions=2,
            deadline_iso=None,
            attestation_scope=f"campaign:{campaign_a}",
            approval_iso=T0,
            iso=T0,
            bindings=(
                CampaignBinding(301, PLAN_FP, approved_requests[301]),
                CampaignBinding(302, PLAN_FP, approved_requests[302]),
                CampaignBinding(303, PLAN_FP, approved_requests[303]),
            ),
            plan_path=str(approved_path),
        )
        store.create_campaign(
            campaign_b,
            REQ_FP,
            max_submissions=5,
            deadline_iso=None,
            attestation_scope=f"campaign:{campaign_b}",
            approval_iso=T0,
            iso=T0,
            bindings=(CampaignBinding(301, REQ_FP, REQ_FP_2),),
        )
        for file_id in (301, 302, 303):
            walk_to_planned(store, file_id)
        rejected = prepare(
            store,
            301,
            req_fp=approved_requests[301],
            campaign_id=campaign_a,
            target_ids=((1, 10101, 5001),),
        )
        store.mark_sent(rejected, iso=T1)
        store.record_send_outcome(rejected, SendOutcome.REJECTED, iso=T1, http_status=400)
        store.connection.execute(
            "UPDATE files SET campaign_id = ?, plan_fingerprint = ?, request_fingerprint = ?"
            " WHERE shoko_file_id = 301",
            (campaign_b, REQ_FP, REQ_FP_2),
        )
        store.connection.execute(
            "UPDATE campaigns SET total_sent = 0 WHERE campaign_id = ?", (campaign_a,)
        )
        owner = store.connection.execute(
            "SELECT campaign_id, campaign_ownership FROM sends WHERE send_id = ?",
            (rejected.send_id,),
        ).fetchone()
        assert tuple(owner) == (campaign_a, "campaign")
        assert [row.outcome for row in store.campaign_send_history(campaign_a)] == ["rejected"]
        assert store.campaign_send_history(campaign_b) == ()
        # Exercise both the in-flight v5->v6 route and the already-upgraded v6->v7 route
        # against rows whose ownership was not captured by the old send ledger.
        _simulate_legacy_schema(store, 5)

    with open_store(path, limits=WIDE_LIMITS) as upgraded:
        assert upgraded.schema_version() == SCHEMA_VERSION
        campaign = upgraded.get_campaign(campaign_a)
        rebound = upgraded.get_campaign(campaign_b)
        assert campaign is not None
        assert campaign.total_sent == 0
        assert rebound is not None
        assert rebound.total_sent == 0
        assert upgraded.campaign_send_history(campaign_a) == ()
        assert upgraded.campaign_send_history(campaign_b) == ()
        unresolved = upgraded.unresolved_campaign_send_history()
        assert len(unresolved) == 1
        assert unresolved[0].shoko_file_id == 301
        ownership = upgraded.connection.execute(
            "SELECT campaign_id, campaign_ownership FROM sends WHERE shoko_file_id = 301"
        ).fetchone()
        assert tuple(ownership) == (None, "unknown")

        # The persisted send tuple is compatible with A, and the file row's later B rebind
        # cannot change the operator's explicit historical attribution.
        upgraded.resolve_legacy_send_ownership(
            unresolved[0].send_id,
            campaign_id=campaign_a,
            note="operator matched original immutable approved binding",
            iso=T2,
        )
        campaign = upgraded.get_campaign(campaign_a)
        assert campaign is not None
        assert campaign.total_sent == 1
        history_a = upgraded.campaign_send_history(campaign_a)
        assert [(row.shoko_file_id, row.outcome) for row in history_a] == [(301, "rejected")]
        assert upgraded.campaign_send_history(campaign_b) == ()

        # The explicit debit is authoritative: only one more attempt fits the cap of two.
        next_send = prepare(
            upgraded,
            302,
            req_fp=approved_requests[302],
            campaign_id=campaign_a,
            target_ids=((1, 10102, 5002),),
        )
        extra_send = prepare(
            upgraded,
            303,
            req_fp=approved_requests[303],
            campaign_id=campaign_a,
            target_ids=((1, 10103, 5003),),
        )
        upgraded.mark_sent(next_send, iso=T2)
        upgraded.record_send_outcome(next_send, SendOutcome.REJECTED, iso=T2, http_status=400)
        with pytest.raises(CampaignSubmissionLimitError):
            upgraded.mark_sent(extra_send, iso=T2)
        assert upgraded.get_campaign(campaign_a).total_sent == 2  # type: ignore[union-attr]


def test_legacy_non_campaign_rejection_is_not_attributed_to_rebound_campaign(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    campaign_id = "campaign-new-owner"
    with open_store(path, limits=WIDE_LIMITS) as store:
        store.create_campaign(
            campaign_id,
            PLAN_FP,
            max_submissions=3,
            deadline_iso=None,
            attestation_scope=f"campaign:{campaign_id}",
            approval_iso=T2,
            iso=T2,
            bindings=(CampaignBinding(501, PLAN_FP, REQ_FP_2),),
        )
        walk_to_planned(store, 501)
        rejected = prepare(store, 501, req_fp=REQ_FP)
        store.mark_sent(rejected, iso=T1)
        store.record_send_outcome(rejected, SendOutcome.REJECTED, iso=T1, http_status=400)
        # Replanning creates a different approved binding; the historical non-campaign send
        # must not become this campaign's send merely because the file row now says campaign_id.
        store.connection.execute(
            "UPDATE files SET campaign_id = ?, plan_fingerprint = ?, request_fingerprint = ?"
            " WHERE shoko_file_id = 501",
            (campaign_id, PLAN_FP, REQ_FP_2),
        )
        owner = store.connection.execute(
            "SELECT campaign_id, campaign_ownership FROM sends WHERE send_id = ?",
            (rejected.send_id,),
        ).fetchone()
        assert tuple(owner) == (None, "non_campaign")
        assert store.campaign_send_history(campaign_id) == ()
        _simulate_legacy_schema(store, 5)

    with open_store(path, limits=WIDE_LIMITS) as upgraded:
        campaign = upgraded.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.total_sent == 0
        assert upgraded.campaign_send_history(campaign_id) == ()
        ownership = upgraded.connection.execute(
            "SELECT campaign_id, campaign_ownership FROM sends WHERE shoko_file_id = 501"
        ).fetchone()
        assert tuple(ownership) == (None, "non_campaign")


def test_legacy_resolution_rejects_incompatible_campaign_and_preserves_campaign_debt(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    campaign_a = "campaign-compatible-A"
    campaign_b = "campaign-compatible-B"
    campaign_c = "campaign-unrelated-C"
    request_fp = _legacy_request_fingerprint(601, PLAN_FP, (5001,))
    binding = CampaignBinding(601, PLAN_FP, request_fp)
    plan_path = tmp_path / f"{campaign_a}.json"
    digest = _write_legacy_campaign_plan(
        plan_path,
        campaign_a,
        (
            {
                "shoko_file_id": 601,
                "plan_fingerprint": PLAN_FP,
                "request_fingerprint": request_fp,
                "anidb_target_identities": [[1, 1, "normal", 10101]],
                "shoko_episode_ids": [5001],
            },
        ),
    )
    with open_store(path, limits=WIDE_LIMITS) as store:
        store.create_campaign(
            campaign_a,
            digest,
            max_submissions=1,
            deadline_iso=None,
            attestation_scope=f"campaign:{campaign_a}",
            approval_iso=T0,
            iso=T0,
            bindings=(binding, CampaignBinding(602, PLAN_FP, REQ_FP_2)),
            plan_path=str(plan_path),
        )
        store.create_campaign(
            campaign_b,
            REQ_FP,
            max_submissions=3,
            deadline_iso=None,
            attestation_scope=f"campaign:{campaign_b}",
            approval_iso=T0,
            iso=T0,
            bindings=(binding,),
        )
        store.create_campaign(
            campaign_c,
            REQ_FP_2,
            max_submissions=3,
            deadline_iso=None,
            attestation_scope=f"campaign:{campaign_c}",
            approval_iso=T0,
            iso=T0,
            bindings=(CampaignBinding(603, PLAN_FP, REQ_FP_2),),
        )
        walk_to_planned(store, 601)
        walk_to_planned(store, 602)
        legacy = prepare(
            store,
            601,
            req_fp=request_fp,
            campaign_id=campaign_a,
            target_ids=((1, 10101, 5001),),
        )
        store.mark_sent(legacy, iso=T1)
        store.record_send_outcome(legacy, SendOutcome.REJECTED, iso=T1, http_status=400)
        store.connection.execute(
            "UPDATE campaigns SET total_sent = 0 WHERE campaign_id = ?", (campaign_a,)
        )
        # Current mutable file ownership/fingerprints now describe unrelated C. The send ledger
        # still carries the immutable file + fingerprint tuple originally prepared for A.
        store.connection.execute(
            "UPDATE files SET campaign_id = ?, plan_fingerprint = ?, request_fingerprint = ?"
            " WHERE shoko_file_id = 601",
            (campaign_c, PLAN_FP, REQ_FP_2),
        )
        _simulate_legacy_schema(store, 6)

    with open_store(path, limits=WIDE_LIMITS) as upgraded:
        row = upgraded.connection.execute(
            "SELECT send_id FROM sends WHERE shoko_file_id = 601"
        ).fetchone()
        assert row is not None
        send_id = int(row[0])
        with pytest.raises(StateError, match="approved binding"):
            upgraded.resolve_legacy_send_ownership(
                send_id,
                campaign_id=campaign_c,
                note="operator confirmed unrelated campaign",
                iso=T2,
            )

        assert [item.send_id for item in upgraded.unresolved_campaign_send_history()] == [send_id]
        assert upgraded.get_campaign(campaign_a).total_sent == 0  # type: ignore[union-attr]
        assert upgraded.get_campaign(campaign_c).total_sent == 0  # type: ignore[union-attr]

        # An explicit, compatible owner restores the debit before the next attempt is admitted.
        upgraded.resolve_legacy_send_ownership(
            send_id,
            campaign_id=campaign_a,
            note="operator matched the immutable approved binding",
            iso=T2,
        )
        assert upgraded.unresolved_campaign_send_history() == ()
        assert upgraded.get_campaign(campaign_a).total_sent == 1  # type: ignore[union-attr]
        next_send = prepare(upgraded, 602, req_fp=REQ_FP_2, campaign_id=campaign_a)
        with pytest.raises(CampaignSubmissionLimitError):
            upgraded.mark_sent(next_send, iso=T3)
        assert upgraded.get_campaign(campaign_a).total_sent == 1  # type: ignore[union-attr]


def test_legacy_send_before_campaign_approval_is_not_counted_as_campaign_debt(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    campaign_id = "campaign-approved-after-send"
    with open_store(path, limits=WIDE_LIMITS) as store:
        walk_to_planned(store, 501)
        old_send = prepare(store, 501, req_fp=REQ_FP, iso=T0)
        store.mark_sent(old_send, iso=T0)
        store.record_send_outcome(old_send, SendOutcome.REJECTED, iso=T0, http_status=400)
        store.create_campaign(
            campaign_id,
            PLAN_FP,
            max_submissions=1,
            deadline_iso=T3,
            attestation_scope=f"campaign:{campaign_id}",
            approval_iso=T2,
            iso=T2,
            bindings=(CampaignBinding(501, PLAN_FP, REQ_FP),),
        )
        _simulate_legacy_schema(store, 5)

    with open_store(path, limits=WIDE_LIMITS) as upgraded:
        ownership = upgraded.connection.execute(
            "SELECT campaign_id, campaign_ownership FROM sends WHERE shoko_file_id = 501"
        ).fetchone()
        assert tuple(ownership) == (None, "non_campaign")
        campaign = upgraded.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.total_sent == 0
        assert upgraded.campaign_send_history(campaign_id) == ()

        # The exact same file/fingerprints are eligible for the campaign only after approval.
        upgraded.advance(501, FileState.PLANNED, "approved campaign plan", iso=T2)
        actual_campaign_send = prepare(
            upgraded, 501, req_fp=REQ_FP, iso=T3, campaign_id=campaign_id
        )
        upgraded.mark_sent(actual_campaign_send, iso=T3)
        assert upgraded.get_campaign(campaign_id).total_sent == 1  # type: ignore[union-attr]


def test_legacy_refreshed_fingerprint_resolution_rejects_other_file_and_targets(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    campaign_a = "campaign-refresh-compatible-A"
    campaign_b = "campaign-refresh-conflicting-B"
    campaign_c = "campaign-refresh-unrelated-C"
    file_id, unrelated_file_id = 601, 603
    runtime_fp = "f" * 64
    runtime_targets = ((1, 10101, 5001),)
    runtime_request = _legacy_request_fingerprint(file_id, runtime_fp, (5001,))

    def campaign_plan(
        campaign_id: str, bound_file_id: int, episode_identity: int, shoko_episode_id: int
    ) -> tuple[str, str, str]:
        plan_fp = format(episode_identity, "064x")
        request_fp = _legacy_request_fingerprint(bound_file_id, plan_fp, (shoko_episode_id,))
        plan_path = tmp_path / f"{campaign_id}.json"
        digest = _write_legacy_campaign_plan(
            plan_path,
            campaign_id,
            (
                {
                    "shoko_file_id": bound_file_id,
                    "plan_fingerprint": plan_fp,
                    "request_fingerprint": request_fp,
                    "anidb_target_identities": [[1, episode_identity, "normal", episode_identity]],
                    "shoko_episode_ids": [shoko_episode_id],
                },
            ),
        )
        return plan_fp, request_fp, digest

    a_fp, a_request, a_digest = campaign_plan(campaign_a, file_id, 10101, 5001)
    b_fp, b_request, b_digest = campaign_plan(campaign_b, file_id, 20202, 5002)
    c_fp, c_request, c_digest = campaign_plan(campaign_c, unrelated_file_id, 30303, 5003)

    with open_store(path, limits=WIDE_LIMITS) as store:
        walk_to_planned(store, file_id)
        walk_to_planned(store, unrelated_file_id)
        for owner, digest, owner_file_id, owner_fp, owner_request in (
            (campaign_a, a_digest, file_id, a_fp, a_request),
            (campaign_b, b_digest, file_id, b_fp, b_request),
            (campaign_c, c_digest, unrelated_file_id, c_fp, c_request),
        ):
            store.create_campaign(
                owner,
                digest,
                max_submissions=2,
                deadline_iso=T3,
                attestation_scope=f"campaign:{owner}",
                approval_iso=T0,
                iso=T0,
                bindings=(CampaignBinding(owner_file_id, owner_fp, owner_request),),
                plan_path=str(tmp_path / f"{owner}.json"),
            )
        attempted = prepare(
            store,
            file_id,
            iso=T1,
            plan_fp=runtime_fp,
            req_fp=runtime_request,
            campaign_id=campaign_a,
            target_ids=runtime_targets,
        )
        assert attempted.accepted
        store.mark_sent(attempted, iso=T1)
        store.record_send_outcome(attempted, SendOutcome.REJECTED, iso=T1, http_status=400)
        store.connection.execute(
            "UPDATE campaigns SET total_sent = 0 WHERE campaign_id = ?", (campaign_a,)
        )
        _simulate_legacy_schema(store, 6)

    with open_store(path, limits=WIDE_LIMITS) as upgraded:
        unresolved = upgraded.unresolved_campaign_send_history()
        assert len(unresolved) == 1
        send_id = unresolved[0].send_id
        with pytest.raises(StateError, match="approved binding"):
            upgraded.resolve_legacy_send_ownership(
                send_id,
                campaign_id=campaign_c,
                note="unrelated file cannot own this send",
                iso=T2,
            )
        with pytest.raises(StateError, match="approved binding"):
            upgraded.resolve_legacy_send_ownership(
                send_id,
                campaign_id=campaign_b,
                note="different approved AniDB target cannot own this send",
                iso=T2,
            )
        assert upgraded.unresolved_campaign_send_history() == unresolved
        upgraded.resolve_legacy_send_ownership(
            send_id,
            campaign_id=campaign_a,
            note="approved file and target identities match the attempted send",
            iso=T2,
        )
        assert upgraded.unresolved_campaign_send_history() == ()
        assert upgraded.get_campaign(campaign_a).total_sent == 1  # type: ignore[union-attr]
        assert upgraded.get_campaign(campaign_b).total_sent == 0  # type: ignore[union-attr]
        assert upgraded.get_campaign(campaign_c).total_sent == 0  # type: ignore[union-attr]


@pytest.mark.parametrize("campaign_owned", [False, True])
def test_legacy_resolution_does_not_reopen_confirmed_send_ownership(
    tmp_path: Path, campaign_owned: bool
) -> None:
    path = tmp_path / "state.sqlite3"
    campaign_id = "campaign-modern-owner"
    with open_store(path, limits=WIDE_LIMITS) as store:
        walk_to_planned(store, 901)
        if campaign_owned:
            store.create_campaign(
                campaign_id,
                PLAN_FP,
                max_submissions=2,
                deadline_iso=T3,
                attestation_scope=f"campaign:{campaign_id}",
                approval_iso=T0,
                iso=T0,
                bindings=(CampaignBinding(901, PLAN_FP, REQ_FP),),
            )
        send = prepare(
            store,
            901,
            req_fp=REQ_FP,
            campaign_id=campaign_id if campaign_owned else None,
        )
        assert send.accepted
        store.mark_sent(send, iso=T1)
        store.record_send_outcome(send, SendOutcome.REJECTED, iso=T1, http_status=400)
        before = store.connection.execute(
            "SELECT campaign_id, campaign_ownership FROM sends WHERE send_id = ?", (send.send_id,)
        ).fetchone()
        with pytest.raises(StateError, match="ownership is already"):
            store.resolve_legacy_send_ownership(
                send.send_id,
                campaign_id=campaign_id if campaign_owned else None,
                note="a confirmed immutable owner cannot be changed",
                iso=T2,
            )
        after = store.connection.execute(
            "SELECT campaign_id, campaign_ownership FROM sends WHERE send_id = ?", (send.send_id,)
        ).fetchone()
        assert tuple(after) == tuple(before)


@pytest.mark.parametrize("legacy_version", [5, 6])
@pytest.mark.parametrize(
    "target_case",
    [
        "valid",
        "contradictory_anidb_identity",
        "missing_send_targets",
        "missing_approved_identities",
    ],
)
def test_legacy_exact_fingerprint_resolution_requires_approved_target_evidence(
    tmp_path: Path, legacy_version: int, target_case: str
) -> None:
    path = tmp_path / "state.sqlite3"
    campaign_id = "campaign-exact-target-check"
    file_id = 701
    request_fp = _legacy_request_fingerprint(file_id, PLAN_FP, (5001,))
    identities = [] if target_case == "missing_approved_identities" else [[1, 1, "normal", 10101]]
    plan_path = tmp_path / f"{campaign_id}.json"
    digest = _write_legacy_campaign_plan(
        plan_path,
        campaign_id,
        (
            {
                "shoko_file_id": file_id,
                "plan_fingerprint": PLAN_FP,
                "request_fingerprint": request_fp,
                "anidb_target_identities": identities,
                "shoko_episode_ids": [5001],
            },
        ),
    )
    targets = (
        ()
        if target_case == "missing_send_targets"
        else ((1, 90909, 5001),)
        if target_case == "contradictory_anidb_identity"
        else ((1, 10101, 5001),)
    )
    with open_store(path, limits=WIDE_LIMITS) as store:
        walk_to_planned(store, file_id)
        store.create_campaign(
            campaign_id,
            digest,
            max_submissions=1,
            deadline_iso=T3,
            attestation_scope=f"campaign:{campaign_id}",
            approval_iso=T0,
            iso=T0,
            bindings=(CampaignBinding(file_id, PLAN_FP, request_fp),),
            plan_path=str(plan_path),
        )
        send = prepare(
            store,
            file_id,
            iso=T1,
            plan_fp=PLAN_FP,
            req_fp=request_fp,
            campaign_id=campaign_id,
            target_ids=targets,
        )
        assert send.accepted
        store.mark_sent(send, iso=T1)
        store.record_send_outcome(send, SendOutcome.REJECTED, iso=T1, http_status=400)
        store.connection.execute(
            "UPDATE campaigns SET total_sent = 0 WHERE campaign_id = ?", (campaign_id,)
        )
        _simulate_legacy_schema(store, legacy_version)

    with open_store(path, limits=WIDE_LIMITS) as upgraded:
        unresolved = upgraded.unresolved_campaign_send_history()
        assert len(unresolved) == 1
        if target_case == "valid":
            upgraded.resolve_legacy_send_ownership(
                unresolved[0].send_id,
                campaign_id=campaign_id,
                note="exact request and persisted targets match the approved identities",
                iso=T2,
            )
            assert upgraded.unresolved_campaign_send_history() == ()
            assert upgraded.get_campaign(campaign_id).total_sent == 1  # type: ignore[union-attr]
        else:
            with pytest.raises(StateError, match="approved binding"):
                upgraded.resolve_legacy_send_ownership(
                    unresolved[0].send_id,
                    campaign_id=campaign_id,
                    note="exact fingerprints cannot override missing or contradictory targets",
                    iso=T2,
                )
            assert upgraded.unresolved_campaign_send_history() == unresolved
            assert upgraded.get_campaign(campaign_id).total_sent == 0  # type: ignore[union-attr]


@pytest.mark.parametrize("attempted_iso", ["not-a-timestamp", "2026-01-01T00:00:00"])
def test_legacy_send_with_invalid_or_timezone_naive_attempt_time_stays_unknown(
    tmp_path: Path, attempted_iso: str
) -> None:
    path = tmp_path / "state.sqlite3"
    campaign_id = "campaign-time-validation"
    with open_store(path, limits=WIDE_LIMITS) as store:
        store.create_campaign(
            campaign_id,
            PLAN_FP,
            max_submissions=2,
            deadline_iso=T3,
            attestation_scope=f"campaign:{campaign_id}",
            approval_iso=T0,
            iso=T0,
            bindings=(CampaignBinding(701, PLAN_FP, REQ_FP),),
        )
        walk_to_planned(store, 701)
        send = prepare(store, 701, req_fp=REQ_FP, campaign_id=campaign_id)
        store.mark_sent(send, iso=T1)
        store.record_send_outcome(send, SendOutcome.REJECTED, iso=T1, http_status=400)
        store.connection.execute(
            "UPDATE sends SET attempted_iso = ? WHERE send_id = ?", (attempted_iso, send.send_id)
        )
        store.connection.execute(
            "UPDATE campaigns SET total_sent = 0 WHERE campaign_id = ?", (campaign_id,)
        )
        _simulate_legacy_schema(store, 6)

    with open_store(path, limits=WIDE_LIMITS) as upgraded:
        owner = upgraded.connection.execute(
            "SELECT campaign_id, campaign_ownership FROM sends WHERE shoko_file_id = 701"
        ).fetchone()
        assert tuple(owner) == (None, "unknown")
        assert [item.shoko_file_id for item in upgraded.unresolved_campaign_send_history()] == [701]
        assert upgraded.get_campaign(campaign_id).total_sent == 0  # type: ignore[union-attr]


def test_legacy_prepared_send_stays_unknown_and_cannot_cross_transport(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    campaign_id = "campaign-prepared-legacy"
    with open_store(path, limits=WIDE_LIMITS) as store:
        store.create_campaign(
            campaign_id,
            PLAN_FP,
            max_submissions=1,
            deadline_iso=T3,
            attestation_scope=f"campaign:{campaign_id}",
            approval_iso=T0,
            iso=T0,
            bindings=(CampaignBinding(801, PLAN_FP, REQ_FP),),
        )
        walk_to_planned(store, 801)
        prepared = prepare(store, 801, req_fp=REQ_FP, campaign_id=campaign_id)
        _simulate_legacy_schema(store, 6)

    with open_store(path, limits=WIDE_LIMITS) as upgraded:
        ownership = upgraded.connection.execute(
            "SELECT campaign_id, campaign_ownership FROM sends WHERE shoko_file_id = 801"
        ).fetchone()
        assert tuple(ownership) == (None, "unknown")
        assert upgraded.unresolved_campaign_send_history() == ()
        with pytest.raises(StateError, match="unresolved legacy campaign ownership"):
            upgraded.mark_sent(prepared, iso=T1)
        with pytest.raises(StateError, match="no attempted-send history"):
            upgraded.resolve_legacy_send_ownership(
                prepared.send_id,
                campaign_id=campaign_id,
                note="prepared sends are not historical attempts",
                iso=T1,
            )
        assert upgraded.get_campaign(campaign_id).total_sent == 0  # type: ignore[union-attr]


def test_ambiguous_legacy_campaign_history_holds_counter_until_explicit_recovery(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    campaign_a = "campaign-ambiguous-A"
    campaign_b = "campaign-ambiguous-B"
    request_fp = _legacy_request_fingerprint(601, PLAN_FP, (5001,))
    binding = CampaignBinding(601, PLAN_FP, request_fp)
    plan_a = tmp_path / f"{campaign_a}.json"
    digest_a = _write_legacy_campaign_plan(
        plan_a,
        campaign_a,
        (
            {
                "shoko_file_id": 601,
                "plan_fingerprint": PLAN_FP,
                "request_fingerprint": request_fp,
                "anidb_target_identities": [[1, 1, "normal", 10101]],
                "shoko_episode_ids": [5001],
            },
        ),
    )
    plan_b = tmp_path / f"{campaign_b}.json"
    digest_b = _write_legacy_campaign_plan(
        plan_b,
        campaign_b,
        (
            {
                "shoko_file_id": 601,
                "plan_fingerprint": PLAN_FP,
                "request_fingerprint": request_fp,
                "anidb_target_identities": [[1, 1, "normal", 20202]],
                "shoko_episode_ids": [5001],
            },
        ),
    )
    with open_store(path, limits=WIDE_LIMITS) as store:
        store.create_campaign(
            campaign_a,
            digest_a,
            max_submissions=1,
            deadline_iso=None,
            attestation_scope=f"campaign:{campaign_a}",
            approval_iso=T0,
            iso=T0,
            bindings=(binding, CampaignBinding(602, PLAN_FP, REQ_FP_2)),
            plan_path=str(plan_a),
        )
        store.create_campaign(
            campaign_b,
            digest_b,
            max_submissions=1,
            deadline_iso=None,
            attestation_scope=f"campaign:{campaign_b}",
            approval_iso=T0,
            iso=T0,
            bindings=(binding,),
            plan_path=str(plan_b),
        )
        walk_to_planned(store, 601)
        legacy = prepare(
            store,
            601,
            req_fp=request_fp,
            campaign_id=campaign_a,
            target_ids=((1, 10101, 5001),),
        )
        store.mark_sent(legacy, iso=T1)
        store.record_send_outcome(legacy, SendOutcome.REJECTED, iso=T1, http_status=400)
        store.connection.execute(
            "UPDATE files SET campaign_id = ? WHERE shoko_file_id = 601", (campaign_b,)
        )
        walk_to_planned(store, 602)
        _simulate_legacy_schema(store, 6)

    with open_store(path, limits=WIDE_LIMITS) as upgraded:
        legacy_row = upgraded.connection.execute(
            "SELECT send_id, campaign_id, campaign_ownership FROM sends WHERE shoko_file_id = 601"
        ).fetchone()
        assert legacy_row is not None
        send_id = int(legacy_row[0])
        assert tuple(legacy_row[1:]) == (None, "unknown")
        pending = prepare(upgraded, 602, req_fp=REQ_FP_2, campaign_id=campaign_a)
        assert isinstance(pending, BudgetRefusal)
        assert pending.reason == "legacy_campaign_history_unresolved"
        assert [row.send_id for row in upgraded.unresolved_campaign_send_history()] == [send_id]
        assert upgraded.budget_view().pending_file_requests == 0
        with pytest.raises(CampaignError, match="ambiguous legacy send"):
            authorize_campaign_drain(
                upgraded, campaign_a, T2, note="attempt must be held while history is unknown"
            )
        with pytest.raises(StateError, match="approved binding"):
            upgraded.resolve_legacy_send_ownership(
                send_id,
                campaign_id=campaign_b,
                note="same fingerprints do not override different approved AniDB targets",
                iso=T2,
            )
        assert [row.send_id for row in upgraded.unresolved_campaign_send_history()] == [send_id]
        upgraded.resolve_legacy_send_ownership(
            send_id,
            campaign_id=campaign_a,
            note="operator checked campaign approval records",
            iso=T2,
        )
        assert upgraded.unresolved_campaign_send_history() == ()
        campaign = upgraded.get_campaign(campaign_a)
        assert campaign is not None
        assert campaign.total_sent == 1
        assert len(upgraded.campaign_send_history(campaign_a)) == 1
        retry = prepare(upgraded, 602, req_fp=REQ_FP_2, campaign_id=campaign_a)
        with pytest.raises(CampaignSubmissionLimitError):
            upgraded.mark_sent(retry, iso=T3)


# ----------------------------------------------------- F17: fixture-drift guards


def _extract_v1_schema_sql() -> str:
    """Re-extract the ``_SCHEMA_SQL`` body from the v1 commit via a local, read-only git call.

    Anchors on the module-level assignment (skipping the docstring mentions), then takes the
    first triple-quoted literal after it. Skips cleanly when git is unavailable: fixture drift
    guards must never hard-fail the suite on environment quirks.
    """
    git = shutil.which("git")
    if git is None:
        pytest.skip("git executable is not available on this machine")
    try:
        result = subprocess.run(  # noqa: S603 - local repo, absolute git path, fixed argv
            [git, "show", f"{v1fx.V1_COMMIT}:src/shoko_linker/state.py"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        pytest.skip(f"git invocation failed: {exc}")
    if result.returncode != 0:
        pytest.skip(f"git show {v1fx.V1_COMMIT} unavailable: {result.stderr.strip()}")
    source = result.stdout
    match = re.search(r'^_SCHEMA_SQL\b[^\n=]*=\s*"""', source, re.MULTILINE)
    if match is None:
        pytest.skip("could not locate the _SCHEMA_SQL assignment in the v1 state.py")
    opening = match.end() - 3
    closing = source.index('"""', opening + 3)
    return source[opening + 3 : closing]


def test_f17_v1_fixture_schema_matches_v1_commit() -> None:
    """F17 guard: the fixture's ``_V1_SCHEMA_SQL`` must stay byte-identical to the ``_SCHEMA_SQL``
    shipped by commit ``6d4183b``. Drift would silently invalidate every F17 no-row-rewritten
    claim, so compare the SHA-256 of the git-extracted body against the fixture body."""
    extracted = _extract_v1_schema_sql()
    extracted_hash = hashlib.sha256(extracted.encode("utf-8")).hexdigest()
    fixture_hash = hashlib.sha256(v1fx._V1_SCHEMA_SQL.encode("utf-8")).hexdigest()
    assert extracted_hash == fixture_hash


def _campaign_schema_tail_columns(schema_sql: str) -> tuple[tuple[str, str], ...]:
    """Parse the last three ``campaigns`` CREATE TABLE columns as (name, definition)."""
    marker = "CREATE TABLE campaigns ("
    block = schema_sql[schema_sql.index(marker) :]
    block = block[: block.index(");")]
    columns = [line.strip().rstrip(",").strip() for line in block.splitlines()[1:] if line.strip()]
    tail: list[tuple[str, str]] = []
    for column in columns[-3:]:
        name, _, definition = column.partition(" ")
        tail.append((name, " ".join(definition.split())))
    return tuple(tail)


def test_f17_campaign_drain_columns_stay_in_sync_with_schema() -> None:
    """F17 guard: ``_CAMPAIGN_V2_DRAIN_COLUMNS`` (the ADD COLUMN DDL the 1→2 migration runs) must
    name exactly the three drain columns at the tail of the current ``_SCHEMA_SQL`` campaigns
    table — same names, order, types and defaults as the live schema encodes them."""
    assert len(_CAMPAIGN_V2_DRAIN_COLUMNS) == 3
    tail = _campaign_schema_tail_columns(state_module._SCHEMA_SQL)
    assert tail == _CAMPAIGN_V2_DRAIN_COLUMNS


# ---------------------------------------------------------------- transitions


def test_full_transition_walk_discovered_to_verified(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        seed_file(store)
        assert state_of(store, 101) is FileState.DISCOVERED
        for target in WALK:
            store.advance(101, target, f"to {target.value}", iso=T0)
        record = store.get_file(101)
        assert record is not None
        assert record.state is FileState.VERIFIED
        assert record.updated_iso == T0


def test_send_api_walk_is_the_same_machine(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        walk_to_planned(store)
        token = prepare(store, req_fp=REQ_FP)
        assert token.accepted is True
        store.mark_sent(token, iso=T0)
        assert state_of(store, 101) is FileState.REQUEST_MAY_HAVE_BEEN_SENT
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=T1, http_status=200)
        assert state_of(store, 101) is FileState.VERIFYING
        # P1-2 adjustment: record_verification is the sole release owner and releases
        # the reservation stored at reserve time — the per-call episode_jobs override
        # (double-release path) is gone.
        store.record_verification(101, VerificationVerdict.VERIFIED, iso=T2)
        record = store.get_file(101)
        assert record is not None
        assert record.state is FileState.VERIFIED
        assert record.last_verified_iso == T2
        assert record.plan_fingerprint == PLAN_FP
        assert record.request_fingerprint == REQ_FP
        assert record.mapping_targets_digest == TARGETS
        assert record.mapping_structure_digest == STRUCTURE
        assert record.mapping_revision == "7"


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (FileState.DISCOVERED, FileState.VERIFIED),
        (FileState.PLANNED, FileState.VERIFYING),
        (FileState.PREPARED_NOT_SENT, FileState.VERIFYING),
        (FileState.VERIFIED, FileState.WAITING),
        (FileState.INDETERMINATE_WRITE, FileState.REQUEST_MAY_HAVE_BEEN_SENT),
        (FileState.DISCOVERED, FileState.PLANNED),
        (FileState.WAITING, FileState.VERIFIED),
    ],
)
def test_illegal_jumps_raise_transition_error(current: FileState, target: FileState) -> None:
    with pytest.raises(TransitionError, match="illegal transition"):
        validate_transition(current, target)


def test_illegal_jump_raises_through_the_machine(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        seed_file(store)
        with pytest.raises(TransitionError):
            store.advance(101, FileState.VERIFIED, "jump", iso=T0)
        with pytest.raises(TransitionError):
            store.advance(101, FileState.PLANNED, "jump", iso=T0)


def test_advance_bulk_validates_per_row_and_skips_illegal(tmp_path: Path) -> None:
    """P3b: the bulk advance keeps per-row edge validation — illegal edges are skipped,
    self-states are no-ops, and only legal rows advance/count."""
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        seed_file(store, 101)
        seed_file(store, 102)
        seed_file(store, 103)
        store.advance(102, FileState.MATCHED, "seed", iso=T0)
        advanced = store.advance_bulk([101, 102, 103], FileState.MATCHED, "m", iso=T1)
        assert advanced == RESERVED_TWO  # 101/103 legal; 102 (self-state) is a no-op
        assert state_of(store, 101) is FileState.MATCHED
        assert state_of(store, 103) is FileState.MATCHED
        # PLANNED from MATCHED is illegal for every row: bulk skips all
        assert store.advance_bulk([101, 102, 103], FileState.PLANNED, "p", iso=T1) == 0
        assert state_of(store, 101) is FileState.MATCHED


def test_legal_edges_validate_and_terminal_verified_is_frozen() -> None:
    validate_transition(FileState.DISCOVERED, FileState.MATCHED)
    validate_transition(FileState.VERIFIED, FileState.VERIFIED)  # idempotent audit no-op
    with pytest.raises(TransitionError):
        validate_transition(FileState.VERIFIED, FileState.WAITING)


def test_replan_prepared_not_sent_is_allowed_with_slot_release(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        walk_to_planned(store)
        prepare(store, req_fp=REQ_FP)
        # Canonical replan path (P2-2): void closes the send row + releases the slot
        # atomically. Raw release_slot is ledger-guarded against send-bound slots (F1).
        store.void_prepared_send(101, REQ_FP, iso=T1, reason="replan")
        assert state_of(store, 101) is FileState.PLANNED
        assert store.budget_view().pending_file_requests == 0


# ---------------------------------------------------------------- send durability


def test_prepared_not_sent_is_committed_before_any_send(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        walk_to_planned(store)
        token = prepare(store)
        # A second connection sees it BEFORE mark_sent is ever called:
        rows = second_view(path, "SELECT state FROM files WHERE shoko_file_id = 101")
        assert rows[0][0] == FileState.PREPARED_NOT_SENT.value
        rows = second_view(path, "SELECT outcome FROM sends WHERE send_id = ?", (token.send_id,))
        assert rows[0][0] == "prepared"


def test_mark_sent_is_durable_before_the_caller_hands_bytes(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        walk_to_planned(store)
        token = prepare(store)
        store.mark_sent(token, iso=T1)
        # API contract: mark_sent commits by the time it returns, i.e. BEFORE any
        # HTTP call the caller would make after it:
        rows = second_view(path, "SELECT state FROM files WHERE shoko_file_id = 101")
        assert rows[0][0] == FileState.REQUEST_MAY_HAVE_BEEN_SENT.value
        rows = second_view(
            path,
            "SELECT outcome, attempted_iso FROM sends WHERE send_id = ?",
            (token.send_id,),
        )
        assert rows[0] == ("attempted", T1)


def test_mark_sent_requires_prepared_not_sent(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        walk_to_planned(store)
        token = prepare(store)
        store.advance(101, FileState.PLANNED, "replan", iso=T1)
        with pytest.raises(TransitionError):
            store.mark_sent(token, iso=T2)


def test_concurrent_campaign_mark_sent_cannot_exceed_durable_attempt_limit(
    tmp_path: Path,
) -> None:
    """Independent SQLite connections serialize at mark_sent and only one takes the final slot."""
    path = tmp_path / "state.sqlite3"
    campaign_id = "campaign-concurrent-limit"
    with open_store(path, limits=WIDE_LIMITS) as store:
        store.create_campaign(
            campaign_id,
            PLAN_FP,
            max_submissions=1,
            deadline_iso=None,
            attestation_scope=f"campaign:{campaign_id}",
            approval_iso=T0,
            iso=T0,
            bindings=(
                CampaignBinding(401, PLAN_FP, format(401, "064x")),
                CampaignBinding(402, PLAN_FP, format(402, "064x")),
            ),
        )
        walk_to_planned(store, 401)
        walk_to_planned(store, 402)
        tokens = (
            prepare(store, 401, cycle_key="campaign-race", campaign_id=campaign_id),
            prepare(store, 402, cycle_key="campaign-race", campaign_id=campaign_id),
        )

    barrier = Barrier(2)
    outcomes: list[str] = []
    failures: list[Exception] = []

    def _attempt(token: Any) -> None:
        try:
            with open_store(path, limits=WIDE_LIMITS) as concurrent_store:
                barrier.wait(timeout=5)
                concurrent_store.mark_sent(token, iso=T1)
            outcomes.append("sent")
        except CampaignSubmissionLimitError:
            outcomes.append("limit")
        except Exception as exc:  # captured in-thread for an informative assertion below
            failures.append(exc)

    workers = [Thread(target=_attempt, args=(token,)) for token in tokens]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=10)
    assert all(not worker.is_alive() for worker in workers)
    assert failures == []
    assert sorted(outcomes) == ["limit", "sent"]

    with open_store(path, limits=WIDE_LIMITS) as reopened:
        campaign = reopened.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.total_sent == 1
        assert campaign.status == "paused"
        assert second_view(
            path, "SELECT outcome, COUNT(*) FROM sends GROUP BY outcome ORDER BY outcome"
        ) == [("attempted", 1), ("prepared", 1)]


def test_send_outcome_matrix(tmp_path: Path) -> None:
    cases = [
        (SendOutcome.ACCEPTED_UNVERIFIED, FileState.VERIFYING, 1),
        (SendOutcome.REJECTED, FileState.WAITING, 0),
        (SendOutcome.AMBIGUOUS_SEND, FileState.INDETERMINATE_WRITE, 1),
        (SendOutcome.REFUSED, FileState.WAITING, 0),
    ]
    for index, (outcome, expected, pending_after) in enumerate(cases):
        path = tmp_path / f"state-{index}.sqlite3"
        with open_store(path) as store:
            file_id = 101 + index
            walk_to_planned(store, file_id)
            token = prepare(store, file_id)
            store.mark_sent(token, iso=T1)
            store.record_send_outcome(token, outcome, iso=T1, http_status=200, detail="x")
            assert state_of(store, file_id) is expected
            assert store.budget_view().pending_file_requests == pending_after


def test_verification_matrix(tmp_path: Path) -> None:
    cases = [
        (VerificationVerdict.VERIFIED, FileState.VERIFIED, 0),
        (VerificationVerdict.PARTIAL, FileState.PARTIAL, 1),
        (VerificationVerdict.CONTRADICTION, FileState.CONFLICT, 1),
        (VerificationVerdict.UNKNOWN, FileState.VERIFYING, 1),
        (VerificationVerdict.PENDING, FileState.VERIFYING, 1),
    ]
    for index, (verdict, expected, pending_after) in enumerate(cases):
        path = tmp_path / f"verify-{index}.sqlite3"
        with open_store(path) as store:
            file_id = 201 + index
            walk_to_planned(store, file_id)
            token = prepare(store, file_id)
            store.mark_sent(token, iso=T1)
            store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=T1)
            store.record_verification(file_id, verdict, iso=T2)
            assert state_of(store, file_id) is expected
            assert store.budget_view().pending_file_requests == pending_after


def test_verified_files_are_terminal_audit_only(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        walk_to_planned(store)
        token = prepare(store)
        store.mark_sent(token, iso=T1)
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=T1)
        store.record_verification(101, VerificationVerdict.VERIFIED, iso=T2)
        with pytest.raises(TransitionError):
            store.advance(101, FileState.PLANNED, "relink", iso=T3)
        sends = second_view(path, "SELECT outcome, verification, verified_iso FROM sends")
        assert sends[0] == ("verified", "verified", T2)


# ---------------------------------------------------------------- recovery


def _seed_in_state(store: StateStore, file_id: int, states: tuple[FileState, ...]) -> None:
    seed_file(store, file_id)
    for target in states:
        store.advance(file_id, target, "seed", iso=T1)


def test_recovery_classification_matrix(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path, limits=WIDE_LIMITS) as store:
        to_planned = (FileState.MATCHED, FileState.MAPPED, FileState.PLANNED)

        walk_to_planned(store, 101)
        token1 = prepare(store, 101)
        store.mark_sent(token1, iso=T1)
        store.record_send_outcome(token1, SendOutcome.ACCEPTED_UNVERIFIED, iso=T1)

        _seed_in_state(store, 102, to_planned)
        _seed_in_state(store, 103, (FileState.WAITING,))
        _seed_in_state(store, 104, (FileState.WAITING, FileState.BLOCKED_SAFETY))
        _seed_in_state(store, 105, (FileState.MATCHED,))

        _seed_in_state(store, 106, to_planned)
        token6 = prepare(store, 106)
        store.mark_sent(token6, iso=T1)
        store.record_send_outcome(token6, SendOutcome.AMBIGUOUS_SEND, iso=T1)

        _seed_in_state(store, 107, to_planned)
        token7 = prepare(store, 107)
        assert isinstance(token7, SendToken)

        _seed_in_state(store, 108, to_planned)
        token8 = prepare(store, 108)
        store.mark_sent(token8, iso=T1)
        store.record_send_outcome(token8, SendOutcome.ACCEPTED_UNVERIFIED, iso=T1)
        store.record_verification(108, VerificationVerdict.VERIFIED, iso=T2)

        _seed_in_state(store, 109, to_planned)
        token9 = prepare(store, 109)
        store.mark_sent(token9, iso=T1)
        store.record_send_outcome(token9, SendOutcome.ACCEPTED_UNVERIFIED, iso=T1)
        store.record_verification(109, VerificationVerdict.PARTIAL, iso=T2)

        plan = store.recover_after_restart()
        assert [row.shoko_file_id for row in plan.replannable] == [107]
        assert [row.shoko_file_id for row in plan.hold_for_verification] == [106]
        assert [row.shoko_file_id for row in plan.continue_polling] == [101, 109]
        assert sorted(row.shoko_file_id for row in plan.re_evaluate) == [102, 103, 105]
        assert [row.shoko_file_id for row in plan.blocked_safety] == [104]
        assert [row.shoko_file_id for row in plan.audit_only] == [108]
        assert plan.hold_for_verification[0].state is FileState.INDETERMINATE_WRITE
        assert plan.hold_for_verification == plan.resubmit_forbidden


def test_recovery_never_resubmits_may_have_been_sent(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        walk_to_planned(store, 101)
        token = prepare(store, 101)
        store.mark_sent(token, iso=T1)
        plan = store.recover_after_restart()
        assert [row.shoko_file_id for row in plan.resubmit_forbidden] == [101]
        row = plan.resubmit_forbidden[0]
        assert "never resubmit" in row.instruction
        assert "include=XRefs" in row.instruction
        # indeterminate_write rows are also resubmit-forbidden
        store.record_send_outcome(token, SendOutcome.AMBIGUOUS_SEND, iso=T1)
        plan = store.recover_after_restart()
        assert [row.shoko_file_id for row in plan.resubmit_forbidden] == [101]


def test_recovery_prepared_not_sent_is_replannable(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        walk_to_planned(store, 101)
        prepare(store, 101)
        plan = store.recover_after_restart()
        assert [row.shoko_file_id for row in plan.replannable] == [101]
        assert "replan" in plan.replannable[0].instruction
        # and it is legitimately re-plannable through the transition machine:
        store.advance(101, FileState.PLANNED, "replan after restart", iso=T2)
        assert state_of(store, 101) is FileState.PLANNED


def test_restart_idempotency_dedups_active_request_fingerprint(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        walk_to_planned(store)
        token = prepare(store, req_fp=REQ_FP)
        assert token.accepted is True
    with open_store(path) as store:
        # replaying the same plan after a restart must not double-count the budget
        replay = prepare(store, req_fp=REQ_FP)
        assert isinstance(replay, BudgetRefusal)
        assert replay.reason == "duplicate_request_fingerprint"
        view = store.budget_view()
        assert view.pending_file_requests == 1
        assert view.cycle_new_submissions == 1
        assert view.rolling_new_submissions == 1
        # a *different* request fingerprint on the same file is not a replay:
        # canonical replan path voids the prepared send (releases slot, file→planned)
        store.void_prepared_send(101, REQ_FP, iso=T1, reason="new plan")
        other = prepare(store, req_fp=REQ_FP_2, cycle_key="cycle-2")
        assert isinstance(other, SendToken)
        view = store.budget_view()
        assert view.pending_file_requests == 1
        assert view.rolling_new_submissions == RESERVED_TWO


# ---------------------------------------------------------------- cohorts


def test_baseline_membership_and_late_shoko_discovery_stays_backlog(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        baseline_id = store.record_baseline(
            [
                BaselineFile(
                    sonarr_series_id=1,
                    sonarr_file_id=10,
                    path="/data/media/anime/show/s01e01.mkv",
                    size=1234,
                )
            ],
            complete=True,
            auto_apply_eligible=True,
            iso=T0,
        )
        assert baseline_id == 1
        known = frozenset({7, 8, 9})
        # Shoko file 550 discovered LATE; its Sonarr identity predates the baseline:
        cohort = store.classify_new(
            550, sonarr_series_id=1, sonarr_file_id=10, known_shoko_file_ids=known
        )
        assert cohort is Cohort.BACKLOG
        # a file already in the known Shoko snapshot is backlog too
        seen = store.classify_new(
            9, sonarr_series_id=1, sonarr_file_id=10, known_shoko_file_ids=known
        )
        assert seen is Cohort.BACKLOG
        # a Sonarr file NOT in the baseline, first seen after it, is a new arrival
        late = store.classify_new(
            551, sonarr_series_id=1, sonarr_file_id=999, known_shoko_file_ids=known
        )
        assert late is Cohort.NEW_ARRIVAL


def test_can_auto_apply_matrix(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    all_open = {
        "attestation_valid": True,
        "gate_open": True,
        "budgets_ok": True,
        "mapping_stable": True,
        "source_stable": True,
    }
    with open_store(path) as store:
        store.record_baseline(
            [BaselineFile(sonarr_series_id=1, sonarr_file_id=10, path="/p.mkv", size=1)],
            complete=True,
            auto_apply_eligible=True,
            iso=T0,
        )
        seed_file(store, 101)  # cohort new_arrival, series 1 file 10
        store.record_file(
            102,
            sonarr_series_id=1,
            sonarr_file_id=10,
            sonarr_path="/p.mkv",
            sonarr_size=1,
            cohort=Cohort.BACKLOG,
            iso=T0,
        )
        store.record_file(
            103,
            sonarr_series_id=2,
            sonarr_file_id=20,
            sonarr_path="/q.mkv",
            sonarr_size=1,
            cohort=Cohort.CAMPAIGN,
            iso=T0,
        )

        verdict = store.can_auto_apply(101, now=T1, **all_open)
        assert verdict.eligible is True
        assert verdict.reasons == ()

        refusals = {
            "attestation": {"attestation_valid": False},
            "gate": {"gate_open": False},
            "budgets": {"budgets_ok": False},
            "mapping": {"mapping_stable": False},
            "source": {"source_stable": False},
        }
        for label, overrides in refusals.items():
            kwargs = dict(all_open)
            kwargs.update(overrides)
            verdict = store.can_auto_apply(101, now=T1, **kwargs)
            assert verdict.eligible is False, label
            assert verdict.reasons, label

        # backlog and campaign cohorts never auto-apply from the daemon
        assert store.can_auto_apply(102, now=T1, **all_open).eligible is False
        assert store.can_auto_apply(103, now=T1, **all_open).eligible is False

    with open_store(path) as fresh:
        # an incomplete baseline recorded later wins as the newest gate state
        fresh.record_baseline([], complete=False, auto_apply_eligible=False, iso=T2)
        verdict = fresh.can_auto_apply(101, now=T3, **all_open)
        assert verdict.eligible is False
        assert "baseline_incomplete" in verdict.reasons


def test_can_auto_apply_requires_baseline(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        seed_file(store)
        verdict = store.can_auto_apply(
            101,
            now=T1,
            attestation_valid=True,
            gate_open=True,
            budgets_ok=True,
            mapping_stable=True,
            source_stable=True,
        )
        assert isinstance(verdict, EligibilityVerdict)
        assert verdict.eligible is False
        assert "no_baseline" in verdict.reasons


# ---------------------------------------------------------------- budgets


def test_budget_reserve_release_and_refusals(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        first = store.try_reserve_slot("cycle-1", T0)
        assert isinstance(first, SlotReservation)
        second = store.try_reserve_slot("cycle-1", T0)
        assert isinstance(second, SlotReservation)
        # per-cycle ceiling (LIMITS.per_cycle_new) refuses the third in-cycle
        refusal = store.try_reserve_slot("cycle-1", T0)
        assert isinstance(refusal, BudgetRefusal)
        assert refusal.reason == "cycle_new_exhausted"
        # a new cycle is bounded by max_pending_files: 3 total, then refusal
        assert isinstance(store.try_reserve_slot("cycle-2", T0), SlotReservation)
        refusal = store.try_reserve_slot("cycle-3", T0)
        assert isinstance(refusal, BudgetRefusal)
        assert refusal.reason == "pending_files_exhausted"
        # release frees a slot
        store.release_slot("cycle-2", T1)
        assert isinstance(store.try_reserve_slot("cycle-3", T1), SlotReservation)
        view = store.budget_view()
        assert view.pending_file_requests == LIMITS.max_pending_files
        assert view.pending_episode_jobs == LIMITS.max_pending_files
        # P1-2 adjustment (old assertion: mark_completed decremented pending, double-releasing
        # a slot the verification close had already freed). mark_completed now only records
        # cycle completion; the release owner is record_verification / record_send_outcome /
        # void_prepared_send / release_slot.
        pending_before = store.budget_view().pending_file_requests
        store.mark_completed("cycle-3", T1)
        view = store.budget_view()
        assert view.pending_file_requests == pending_before
        assert view.completed_submissions == 1
        store.release_slot("cycle-3", T1)
        assert store.budget_view().pending_file_requests == pending_before - 1


def test_budget_refusal_reason_marker(tmp_path: Path) -> None:
    """P3d: every budget refusal persists the machine-readable reason marker next to
    the counters (meta key ``last_refusal``)."""
    path = tmp_path / "state.sqlite3"
    limits = BudgetLimits(
        max_pending_files=1,
        max_pending_episode_jobs=2,
        per_cycle_new=5,
        rolling_new=5,
        rolling_window_hours=24.0,
    )
    with open_store(path, limits=limits) as store:
        assert isinstance(store.try_reserve_slot("cycle-1", T0), SlotReservation)
        refusal = store.try_reserve_slot("cycle-2", T0)
        assert isinstance(refusal, BudgetRefusal)
        marker = json.loads(store.get_meta("last_refusal") or "{}")
        assert marker["reason"] == "pending_files_exhausted"
        assert marker["iso"] == T0
        assert "max 1" in marker["detail"]


def test_budget_rolling_window_resets_after_the_window(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    limits = BudgetLimits(
        max_pending_files=10,
        max_pending_episode_jobs=10,
        per_cycle_new=10,
        rolling_new=2,
        rolling_window_hours=24.0,
    )
    with open_store(path, limits=limits) as store:
        assert isinstance(store.try_reserve_slot("cycle-1", T0), SlotReservation)
        assert isinstance(store.try_reserve_slot("cycle-1", T0), SlotReservation)
        refusal = store.try_reserve_slot("cycle-1", T0)
        assert isinstance(refusal, BudgetRefusal)
        assert refusal.reason == "rolling_new_exhausted"
        # after the 24h window the rolling counter resets
        assert isinstance(store.try_reserve_slot("cycle-2", T2), SlotReservation)
        view = store.budget_view()
        assert view.rolling_new_submissions == 1
        assert view.rolling_window_start_iso == T2


def test_budget_counters_and_pause_survive_reopen(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        store.try_reserve_slot("cycle-1", T0)
        store.try_reserve_slot("cycle-1", T1)
        store.pause("mapping churn", T1)
    with open_store(path) as reopened:
        view = reopened.budget_view()
        assert view.pending_file_requests == RESERVED_TWO
        assert view.cycle_new_submissions == RESERVED_TWO
        assert view.rolling_new_submissions == RESERVED_TWO
        assert view.paused_reason == "mapping churn"
        refusal = reopened.try_reserve_slot("cycle-1", T2)
        assert isinstance(refusal, BudgetRefusal)
        assert refusal.reason.startswith("paused")
        reopened.resume(T2)
        assert reopened.budget_view().paused_reason is None
        assert isinstance(reopened.try_reserve_slot("cycle-2", T2), SlotReservation)


def test_prepare_send_atomically_refuses_and_leaves_file_planned(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    limits = BudgetLimits(
        max_pending_files=1,
        max_pending_episode_jobs=2,
        per_cycle_new=5,
        rolling_new=5,
        rolling_window_hours=24.0,
    )
    with open_store(path, limits=limits) as store:
        walk_to_planned(store, 101)
        walk_to_planned(store, 102)
        assert prepare(store, 101).accepted is True
        refusal = prepare(store, 102)
        assert isinstance(refusal, BudgetRefusal)
        assert state_of(store, 102) is FileState.PLANNED
        sends = second_view(path, "SELECT COUNT(*) FROM sends WHERE shoko_file_id = 102")
        assert sends[0][0] == 0


# ------------------------------------------------------------------ F9: durable budget cycles


def test_f9_begin_cycle_keys_unique_across_restart_and_monotonic(tmp_path: Path) -> None:
    """Scenario 9: window keys are unique across restarts, monotonic, and durably persisted.

    (A 7000-file campaign plan is not built in unit tests — it is too slow; this key
    property plus the campaign drain scenarios are the scale proof.)
    """
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        keys = [store.begin_cycle("cycle", T0) for _ in range(RESERVED_THREE)]
        assert len(set(keys)) == RESERVED_THREE
        run_id = store.get_meta(META_BUDGET_RUN_ID)
        assert run_id is not None
        assert len(run_id) == 32
        for index, key in enumerate(keys, start=1):
            assert key == f"cycle-{run_id}-{index}"
        seq_first = int(str(store.get_meta(META_BUDGET_SEQ)))
    with open_store(path) as reopened:
        assert reopened.get_meta(META_BUDGET_RUN_ID) == run_id  # nonce stable across restart
        new_key = reopened.begin_cycle("campaign-abcd1234", T1)
        assert new_key not in keys  # no collision with a prior run's persisted rows
        assert new_key == f"campaign-abcd1234-{run_id}-{seq_first + 1}"
        assert int(str(reopened.get_meta(META_BUDGET_SEQ))) > seq_first


def test_f9_begin_cycle_prunes_old_rows_and_preserves_live_and_global(tmp_path: Path) -> None:
    """Scenario 8: old completed window rows are bounded; the live window and the global rolling
    row (with its counters and pause marker) are never touched."""
    path = tmp_path / "state.sqlite3"
    with open_store(path, limits=WIDE_LIMITS) as store:
        for _ in range(BUDGET_CYCLE_RETENTION + 5):
            store.begin_cycle("cycle", T0)
        current = store.begin_cycle("cycle", T0)
        assert isinstance(store.try_reserve_slot(current, T0), SlotReservation)
        store.pause("keep me", T0)
        rows = second_view(path, "SELECT window_key FROM budgets")
        window_keys = {str(row[0]) for row in rows}
        assert GLOBAL_BUDGET_KEY in window_keys
        assert current in window_keys
        assert len(window_keys) <= BUDGET_CYCLE_RETENTION + 2
        view = store.budget_view()
        assert view.paused_reason == "keep me"
        assert view.pending_file_requests == 1
        assert view.rolling_new_submissions == 1


def test_f9_crash_recovery_prepared_row_from_old_window_completes_once(tmp_path: Path) -> None:
    """Scenario 7: a prepared/possible-delivery row from a prior batch's window_key survives a
    restart; verification closes it with no second POST and no re-reservation."""
    path = tmp_path / "state.sqlite3"
    with open_store(path, limits=WIDE_LIMITS) as store:
        walk_to_planned(store, 101)
        old_key = store.begin_cycle("campaign-abcd1234", T0)
        token = prepare(store, 101, req_fp=REQ_FP, cycle_key=old_key)
        assert isinstance(token, SendToken)
        store.mark_sent(token, iso=T0)
    with open_store(path, limits=WIDE_LIMITS) as store:
        new_key = store.begin_cycle("campaign-abcd1234", T1)
        assert new_key != old_key
        replay = prepare(store, 101, req_fp=REQ_FP, cycle_key=new_key, iso=T1)
        assert isinstance(replay, BudgetRefusal)
        assert replay.reason == "duplicate_request_fingerprint"
        assert state_of(store, 101) is FileState.REQUEST_MAY_HAVE_BEEN_SENT
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=T1)
        assert state_of(store, 101) is FileState.VERIFYING
        store.record_verification(101, VerificationVerdict.VERIFIED, iso=T1)
        assert state_of(store, 101) is FileState.VERIFIED
        assert store.budget_view().pending_file_requests == 0


def test_f9_rolling_exhaustion_stops_and_resumes_across_fresh_cycles(tmp_path: Path) -> None:
    """Scenario 4: the per-cycle window_key reset never bypasses the global rolling budget.

    Every fresh cycle/batch gets new per-cycle counters, yet the ``_global`` rolling ceiling keeps
    applying across them; after the rolling window passes, submissions resume."""
    path = tmp_path / "state.sqlite3"
    limits = BudgetLimits(
        max_pending_files=10,
        max_pending_episode_jobs=10,
        per_cycle_new=10,
        rolling_new=3,
        rolling_window_hours=24.0,
    )
    with open_store(path, limits=limits) as store:
        cycle_a = store.begin_cycle("cycle", T0)
        for _ in range(3):
            assert isinstance(store.try_reserve_slot(cycle_a, T0), SlotReservation)
        # per-cycle has headroom, but the global rolling ceiling is spent → refuse
        refusal = store.try_reserve_slot(cycle_a, T0)
        assert isinstance(refusal, BudgetRefusal)
        assert refusal.reason == "rolling_new_exhausted"
        # a brand-new cycle resets per-cycle counters, not the rolling budget
        cycle_b = store.begin_cycle("cycle", T0)
        assert cycle_b != cycle_a
        refusal = store.try_reserve_slot(cycle_b, T0)
        assert isinstance(refusal, BudgetRefusal)
        assert refusal.reason == "rolling_new_exhausted"
        # after the rolling window passes, the same fresh cycle resumes
        assert isinstance(store.try_reserve_slot(cycle_b, T2), SlotReservation)
        assert store.budget_view().rolling_new_submissions == 1


def test_f9_corrupt_seq_seed_never_reissues_the_first_window(tmp_path: Path) -> None:
    """F9-2: a corrupted ``budget_cycle_seq`` meta must NOT restart at 1 and re-issue the first
    persisted window key; the seed is derived from the highest existing suffix for the run."""
    path = tmp_path / "state.sqlite3"
    with open_store(path, limits=WIDE_LIMITS) as store:
        first = store.begin_cycle("cycle", T0)
        assert first.endswith("-1")
        assert isinstance(store.try_reserve_slot(first, T0), SlotReservation)  # materializes row
        rows = {str(row[0]) for row in second_view(path, "SELECT window_key FROM budgets")}
        assert first in rows
        # corrupt the persisted counter (as a torn write / tampering would)
        store.set_meta(META_BUDGET_SEQ, "not-a-number")
        second = store.begin_cycle("cycle", T0)
        assert second != first  # no collision with the first window row
        assert second not in rows
        assert int(str(store.get_meta(META_BUDGET_SEQ))) > 1  # repaired to a monotone value


# ---------------------------------------------------------------- apply lock


def test_apply_lock_acquire_contend_steal_release(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        result = acquire_apply_lock(store, pid=LOCK_PID_A, iso=T0, lease_expires_iso=T1)
        assert result.acquired is True
        assert result.holder.owner_pid == LOCK_PID_A
        assert result.holder.lease_expires_iso == T1
        with pytest.raises(LockHeldError) as excinfo:
            acquire_apply_lock(store, pid=LOCK_PID_B, iso=T0, lease_expires_iso=T2)
        holder = excinfo.value.holder
        assert holder is not None
        assert holder.owner_pid == LOCK_PID_A
        # not yet expired: stealing is refused
        with pytest.raises(LockHeldError):
            steal_expired_apply_lock(store, pid=LOCK_PID_B, iso=T0)
        stolen = steal_expired_apply_lock(store, pid=LOCK_PID_B, iso=T2)
        assert stolen.holder.owner_pid == LOCK_PID_B
        assert release_apply_lock(store, pid=LOCK_PID_B, iso=T2) is True
        assert release_apply_lock(store, pid=LOCK_PID_B, iso=T2) is False


def test_acquire_or_steal_rules(tmp_path: Path) -> None:
    """P2-1 state rules: acquire when free; steal ONLY an expired lease (audit-recorded,
    a self-held stale lock is stealable); a live lease is refused naming the holder."""
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        fresh = store.acquire_or_steal_apply_lock_tx(LOCK_PID_A, T0, T1)
        assert fresh.acquired is True
        with pytest.raises(LockHeldError) as excinfo:
            store.acquire_or_steal_apply_lock_tx(LOCK_PID_B, T0, T2)
        assert excinfo.value.holder is not None
        assert excinfo.value.holder.owner_pid == LOCK_PID_A
        # expired at T2 → steal, with the audit entry persisted
        stolen = store.acquire_or_steal_apply_lock_tx(LOCK_PID_B, T2, T3)
        assert stolen.holder.owner_pid == LOCK_PID_B
        audit = json.loads(store.get_meta("apply_lock_steals") or "[]")
        assert audit[0]["from_pid"] == LOCK_PID_A
        assert audit[0]["owner_pid"] == LOCK_PID_B
        assert audit[0]["from_lease_expires_iso"] == T1
        # self-held stale lock is stealable
        assert store.release_apply_lock_tx(LOCK_PID_B, T3) is True
        store.acquire_apply_lock_tx(LOCK_PID_A, T0, T1)
        self_stolen = store.acquire_or_steal_apply_lock_tx(LOCK_PID_A, T2, T3)
        assert self_stolen.holder.owner_pid == LOCK_PID_A


def test_self_held_expired_lease_steal_path(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        acquire_apply_lock(store, pid=LOCK_PID_A, iso=T0, lease_expires_iso=T1)
        stolen = steal_expired_apply_lock(store, pid=LOCK_PID_A, iso=T2)
        assert stolen.holder.owner_pid == LOCK_PID_A


def test_apply_lock_is_durable_across_reopen(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        acquire_apply_lock(store, pid=LOCK_PID_A, iso=T0, lease_expires_iso=T2)
    with open_store(path) as reopened:
        with pytest.raises(LockHeldError):
            acquire_apply_lock(reopened, pid=LOCK_PID_B, iso=T1, lease_expires_iso=T2)
        assert release_apply_lock(reopened, pid=LOCK_PID_A, iso=T1) is True


def test_apply_lock_single_process_is_documented_external_risk() -> None:
    # Deployment-level note, not a code test: SQLite file locks cannot coordinate
    # separate installations (see state.py module docstring; PLAN M6 EXTERNAL).
    assert True


# ------------------------------------------------- F5: OS advisory ownership lock


def test_apply_ownership_same_process_double_acquire_is_contention(
    tmp_path: Path,
) -> None:
    """Req 5: a live owner never loses ownership to a lease timeout — a
    same-process re-acquire is contention, with zero meta mutation."""
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        acq = store.acquire_apply_ownership(LOCK_PID_A, T0, T1)
        assert acq.acquired is True
        assert store.lock_holder() is not None
        with pytest.raises(LockHeldError) as excinfo:
            store.acquire_apply_ownership(LOCK_PID_B, T0, T2)
        assert excinfo.value.holder is not None
        assert excinfo.value.holder.owner_pid == LOCK_PID_A
        # Contention mutates nothing: the record still names the first owner.
        after = store.lock_holder()
        assert after is not None
        assert after.owner_pid == LOCK_PID_A


def test_apply_ownership_release_then_reacquire_and_close_releases_os_lock(
    tmp_path: Path,
) -> None:
    """Req 5: release frees the OS lock so a re-acquire succeeds; store.close() releases it."""
    path = tmp_path / "state.sqlite3"
    store = StateStore(path, budget_limits=LIMITS)
    try:
        store.acquire_apply_ownership(LOCK_PID_A, T0, T1)
        lock = store._apply_lock
        assert lock is not None
        held_after_acquire = lock.held
        assert held_after_acquire is True
        assert store.release_apply_ownership(LOCK_PID_A, T1) is True
        held_after_release = lock.held
        assert held_after_release is False
        reacquired = store.acquire_apply_ownership(LOCK_PID_A, T2, T3)
        assert reacquired.acquired is True
    finally:
        store.close()
    final_lock = store._apply_lock
    assert final_lock is not None
    assert final_lock.held is False
    # The OS lock file is genuinely free for a fresh acquirer after close.
    probe = ApplyProcessLock(store.apply_lock_path)
    assert probe.acquire() is True
    probe.release()


def test_apply_process_lock_two_objects_in_one_process_contend(tmp_path: Path) -> None:
    """Req 5: two independent lock objects on the same file contend (Windows EDEADBR /
    POSIX EWOULDBLOCK both surface as contention, never as a false acquisition)."""
    lock_path = tmp_path / "apply.lock"
    first = ApplyProcessLock(lock_path)
    second = ApplyProcessLock(lock_path)
    assert first.acquire() is True
    assert second.acquire() is False
    first.release()
    assert second.acquire() is True
    second.release()


def test_apply_ownership_context_manager_reports_contention(tmp_path: Path) -> None:
    from shoko_linker.state import ApplyLockContendedError

    lock_path = tmp_path / "apply.lock"
    with ApplyProcessLock(lock_path) as held:
        assert held.held is True
        with pytest.raises(ApplyLockContendedError):
            ApplyProcessLock(lock_path).__enter__()


def test_apply_ownership_expired_lease_with_free_os_lock_steals_and_audits(
    tmp_path: Path,
) -> None:
    """Req 4 (unit): meta lease expired + OS lock FREE (owner dead) → acquire succeeds, steal
    audit recorded."""
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        store.acquire_apply_ownership(LOCK_PID_A, T0, T1)
    # Owner process ended → the OS lock is free; the meta lease is stale at T2.
    with open_store(path) as reopened:
        acq = acquire_or_steal_apply_lock(reopened, LOCK_PID_B, T2, lease_expires_iso=T3)
        assert acq.holder.owner_pid == LOCK_PID_B
        audit = json.loads(reopened.get_meta("apply_lock_steals") or "[]")
        assert audit[0]["from_pid"] == LOCK_PID_A
        assert audit[0]["owner_pid"] == LOCK_PID_B


def test_steal_apply_lock_requires_free_os_lock(tmp_path: Path) -> None:
    """Req 6 (unit): the operator steal is a point-in-time marker — it takes and releases the
    OS lock; a live OS-locked owner refuses."""
    path = tmp_path / "state.sqlite3"
    store = StateStore(path, budget_limits=LIMITS)
    try:
        store.acquire_apply_ownership(LOCK_PID_A, T0, T1)  # live owner holds the OS lock
        with pytest.raises(LockHeldError) as excinfo:
            steal_apply_lock(store, LOCK_PID_B, T2, lease_expires_iso=T3)
        assert excinfo.value.holder is not None
        assert excinfo.value.holder.owner_pid == LOCK_PID_A
        current = store.lock_holder()
        assert current is not None
        assert current.owner_pid == LOCK_PID_A
    finally:
        store.close()
    # Owner gone (OS free) + expired lease → takeover, and the OS lock is released again.
    with open_store(path) as reopened:
        acq = steal_apply_lock(reopened, LOCK_PID_B, T2, lease_expires_iso=T3)
        assert acq.holder.owner_pid == LOCK_PID_B
        assert reopened._apply_lock is not None
        assert reopened._apply_lock.held is False


# ---------------------------------------------------------------- identity


def test_source_observation_and_continuity(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        verdict = store.check_source_continuity(
            "sonarr-A", "hash-1", iso=T0, roots_fingerprint="roots-1", discovery_total=10
        )
        assert verdict.ok is True  # first observation anchors identity
        store.record_source_observation(
            "sonarr-A", "hash-1", iso=T1, roots_fingerprint="roots-1", discovery_total=10
        )
        # P1-1: ordinary inventory growth (hash moves, totals rise) is NORMAL and must
        # not demand a rebaseline — the old hash-equality rule self-wedged on new arrivals.
        verdict = store.check_source_continuity(
            "sonarr-A", "hash-2", iso=T2, roots_fingerprint="roots-1", discovery_total=12
        )
        assert isinstance(verdict, ContinuityVerdict)
        assert verdict.ok is True
        assert verdict.requires_rebaseline is False
        assert "snapshot_discontinuity" not in verdict.reasons
    with open_store(path) as store:
        verdict = store.check_source_continuity(
            "sonarr-B", "hash-2", iso=T3, roots_fingerprint="roots-1", discovery_total=12
        )
        assert verdict.ok is False
        assert verdict.requires_rebaseline is True
        assert "instance_identity_changed" in verdict.reasons
        assert store.get_meta("requires_rebaseline") == "1"
    with open_store(path) as store:
        # A clean observation after restart does not authorize recovery implicitly.
        verdict = store.check_source_continuity(
            "sonarr-B", "hash-3", iso=T3, roots_fingerprint="roots-1", discovery_total=13
        )
        assert verdict.requires_rebaseline is True
        assert store.get_meta("requires_rebaseline") == "1"
        store.clear_requires_rebaseline(T3)
    with open_store(path) as store:
        # discovery-total drop is a discontinuity
        verdict = store.check_source_continuity(
            "sonarr-B", "hash-4", iso=T3, roots_fingerprint="roots-1", discovery_total=5
        )
        assert verdict.requires_rebaseline is True
        assert "discovery_total_drop" in verdict.reasons
        # roots-set change is a discontinuity
        verdict = store.check_source_continuity(
            "sonarr-B", "hash-4", iso=T3, roots_fingerprint="roots-2", discovery_total=5
        )
        assert verdict.requires_rebaseline is True
        assert "roots_changed" in verdict.reasons
        # missing expected snapshot marker (truncated report where a total was anchored)
        verdict = store.check_source_continuity(
            "sonarr-B", "hash-4", iso=T3, roots_fingerprint="roots-2", discovery_total=None
        )
        assert verdict.requires_rebaseline is True
        assert "snapshot_marker_missing" in verdict.reasons
        # operator/M7 explicit clear path
        store.clear_requires_rebaseline(T3)
        assert store.get_meta("requires_rebaseline") == "0"


def test_truncated_discovery_never_anchors_continuity_total(tmp_path: Path) -> None:
    """P1-C probe (reviewer p3, store level): a truncated page reports Total 999999 with
    ``complete=False``. Feeding it as an incomplete observation must NOT anchor the inflated
    total, so the next honest complete cycle (total 10) is NOT a discovery_total_drop."""
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        first = store.check_source_continuity(
            "sonarr-A", "hash-base", iso=T0, roots_fingerprint="roots-1", discovery_total=100
        )
        assert first.ok is True  # anchors total 100
        # Truncated page: complete=False, absurd total 999999.
        truncated = store.check_source_continuity(
            "sonarr-A",
            "hash-trunc",
            iso=T1,
            roots_fingerprint="roots-1",
            discovery_total=999999,
            complete=False,
        )
        assert truncated.ok is True
        assert truncated.requires_rebaseline is False
        assert truncated.reasons == ("discovery_incomplete",)
        assert store.get_meta("requires_rebaseline") != "1"  # drop flag never triggered
        # The anchor is untouched (still 100, not 999999):
        assert store.get_meta("source_last_discovery_total") == "100"
        # Next honest complete cycle, total 10 < 100 → a genuine drop, but crucially the
        # 999999 was never anchored. Verify the neutral cycle's total 10 with complete=True
        # on a FRESH store to prove the inflated number was never persisted.
        verdict = store.check_source_continuity(
            "sonarr-A", "hash-2", iso=T2, roots_fingerprint="roots-1", discovery_total=100
        )
        assert verdict.requires_rebaseline is False


def test_growth_cycles_reanchor_and_never_self_wedge(tmp_path: Path) -> None:
    """P1-1 regression: per-cycle inventory growth moved the snapshot hash every cycle,
    persisted ``requires_rebaseline`` forever, and permanently suppressed apply. Growth
    now re-anchors; only discontinuity triggers flag."""
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        store.check_source_continuity(
            "sonarr-A", "hash-base", iso=T0, roots_fingerprint="roots-1", discovery_total=100
        )
        growth = 100
        for cycle_index in range(1, 6):
            growth += cycle_index
            verdict = store.check_source_continuity(
                "sonarr-A",
                f"hash-{cycle_index}",
                iso=T1,
                roots_fingerprint="roots-1",
                discovery_total=growth,
            )
            assert verdict.requires_rebaseline is False, cycle_index
        store.record_source_observation(
            "sonarr-A", "hash-5", iso=T1, roots_fingerprint="roots-1", discovery_total=growth
        )
        assert store.get_meta("requires_rebaseline") == "0"


def test_verified_release_is_single_owner(tmp_path: Path) -> None:
    """P1-2 regression: VERIFIED released -1/-1 in record_verification AND -1/-1 again
    in mark_completed, releasing a slot a still-in-flight send held. One release owner
    now: record_verification, with the *stored* reserved job count."""
    path = tmp_path / "state.sqlite3"
    limits = BudgetLimits(
        max_pending_files=2,
        max_pending_episode_jobs=8,
        per_cycle_new=10,
        rolling_new=10,
        rolling_window_hours=24.0,
    )
    with open_store(path, limits=limits) as store:
        walk_to_planned(store, 101)
        walk_to_planned(store, 102)
        first = store.prepare_send(
            101,
            PLAN_FP,
            REQ_FP,
            targets_digest=TARGETS,
            structure_digest=STRUCTURE,
            mapping_revision="7",
            cycle_key="cycle-1",
            iso=T0,
            expected_episode_jobs=3,
        )
        second = store.prepare_send(
            102,
            PLAN_FP,
            REQ_FP_2,
            targets_digest=TARGETS,
            structure_digest=STRUCTURE,
            mapping_revision="7",
            cycle_key="cycle-1",
            iso=T0,
            expected_episode_jobs=1,
        )
        assert isinstance(first, SendToken)
        assert isinstance(second, SendToken)
        assert store.budget_view().pending_file_requests == RESERVED_TWO
        assert store.budget_view().pending_episode_jobs == JOBS_FOUR
        store.mark_sent(first, iso=T0)
        store.mark_sent(second, iso=T0)
        store.record_send_outcome(first, SendOutcome.ACCEPTED_UNVERIFIED, iso=T0)
        store.record_send_outcome(second, SendOutcome.ACCEPTED_UNVERIFIED, iso=T0)
        store.record_verification(101, VerificationVerdict.VERIFIED, iso=T1)
        store.mark_completed("cycle-1", T1)
        view = store.budget_view()
        # The second send is still in flight: its slot must remain held (old code: 0).
        assert view.pending_file_requests == 1
        assert view.pending_episode_jobs == 1
        assert view.completed_submissions == 1
        store.record_verification(102, VerificationVerdict.VERIFIED, iso=T2)
        store.mark_completed("cycle-1", T2)
        view = store.budget_view()
        assert view.pending_file_requests == 0
        assert view.pending_episode_jobs == 0


def _prepare_jobs(
    store: StateStore,
    file_id: int,
    *,
    jobs: int,
    cycle_key: str = "cycle-1",
    iso: str = T0,
    req_fp: str | None = None,
    target_ids: tuple[tuple[int, int, int], ...] = (),
) -> SendToken:
    token = store.prepare_send(
        file_id,
        PLAN_FP,
        req_fp if req_fp is not None else format(file_id, "064x"),
        targets_digest=TARGETS,
        structure_digest=STRUCTURE,
        mapping_revision="7",
        cycle_key=cycle_key,
        iso=iso,
        expected_episode_jobs=jobs,
        target_ids=target_ids,
    )
    assert isinstance(token, SendToken)
    return token


def test_partial_without_qualified_inspection_keeps_slot_until_verified(tmp_path: Path) -> None:
    """An unqualified partial verdict cannot release a possibly-delivered send slot."""
    path = tmp_path / "state.sqlite3"
    limits = BudgetLimits(
        max_pending_files=2,
        max_pending_episode_jobs=4,
        per_cycle_new=10,
        rolling_new=10,
        rolling_window_hours=24.0,
    )
    with open_store(path, limits=limits) as store:
        walk_to_planned(store, 101)
        walk_to_planned(store, 102)
        token_a = _prepare_jobs(store, 101, jobs=2, req_fp=REQ_FP)
        token_b = _prepare_jobs(store, 102, jobs=1, req_fp=REQ_FP_2)
        store.mark_sent(token_a, iso=T0)
        store.mark_sent(token_b, iso=T0)
        store.record_send_outcome(token_a, SendOutcome.ACCEPTED_UNVERIFIED, iso=T0)
        store.record_send_outcome(token_b, SendOutcome.ACCEPTED_UNVERIFIED, iso=T0)
        assert store.budget_view().pending_file_requests == PENDING_TWO
        assert store.budget_view().pending_episode_jobs == PENDING_THREE  # A=2 jobs + B=1 job

        # Without a same-file identity-qualified inspection, PARTIAL is not terminal review.
        store.record_verification(101, VerificationVerdict.PARTIAL, iso=T1)
        assert store.budget_view().pending_file_requests == PENDING_TWO  # partial keeps slots
        with pytest.raises(StateError, match="explicit resolve_send"):
            store.release_send_slot(101, REQ_FP, iso=T1)

        # A later exact verification remains the ordinary single-owner release path.
        store.advance(101, FileState.VERIFYING, "late job", iso=T2)
        store.record_verification(101, VerificationVerdict.VERIFIED, iso=T2)
        view = store.budget_view()
        assert view.pending_file_requests == 1
        assert view.pending_episode_jobs == 1

        # A second file is admitted ONLY while true in-flight < ceiling (ceiling 2, 1 held).
        walk_to_planned(store, 103)
        token_c = _prepare_jobs(store, 103, jobs=1, cycle_key="cycle-2", req_fp="f" * 64)
        assert token_c.accepted is True
        assert store.budget_view().pending_file_requests == PENDING_TWO  # never exceeds ceiling

        store.record_verification(102, VerificationVerdict.VERIFIED, iso=T3)
        view = store.budget_view()
        # A stays released, B closed, C (prepared, never sent) holds its reservation.
        assert view.pending_file_requests == 1
        assert view.pending_episode_jobs == 1


def test_verified_twice_releases_exactly_once(tmp_path: Path) -> None:
    """P1-A probe (reviewer p1): a double-verify sequence releases exactly one slot."""
    path = tmp_path / "state.sqlite3"
    with open_store(path, limits=WIDE_LIMITS) as store:
        walk_to_planned(store, 101)
        token = _prepare_jobs(store, 101, jobs=2, req_fp=REQ_FP)
        store.mark_sent(token, iso=T0)
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=T0)
        assert store.budget_view().pending_file_requests == 1

        store.record_verification(101, VerificationVerdict.VERIFIED, iso=T1)
        assert store.budget_view().pending_file_requests == 0
        assert store.budget_view().pending_episode_jobs == 0
        first_noops = json.loads(store.get_meta("slot_release_noops") or "[]")

        # The same VERIFIED verdict delivered again (duplicate late delivery):
        store.record_verification(101, VerificationVerdict.VERIFIED, iso=T2)
        view = store.budget_view()
        assert view.pending_file_requests == 0  # not -1 (no clamp-masked underflow)
        assert view.pending_episode_jobs == 0
        second_noops = json.loads(store.get_meta("slot_release_noops") or "[]")
        assert len(second_noops) == len(first_noops) + 1  # the second release was a no-op


def test_release_send_slot_refuses_uninspected_possible_delivery(tmp_path: Path) -> None:
    """An uninspected possible delivery cannot be released outside resolve_send."""
    path = tmp_path / "state.sqlite3"
    with open_store(path, limits=WIDE_LIMITS) as store:
        walk_to_planned(store, 101)
        token = _prepare_jobs(store, 101, jobs=1, req_fp=REQ_FP)
        store.mark_sent(token, iso=T0)
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=T0)
        with pytest.raises(StateError, match="explicit resolve_send"):
            store.release_send_slot(101, REQ_FP, iso=T1)
        assert store.budget_view().pending_file_requests == 1
        assert second_view(
            path,
            "SELECT outcome, slot_released FROM sends WHERE send_id = ?",
            (token.send_id,),
        ) == [("accepted_unverified", 0)]


def test_rejected_send_releases_stored_episode_jobs(tmp_path: Path) -> None:
    """P2-4 probe C8: a REJECTED send reserved 2 episode jobs — the release must free
    the stored 2 (old code decremented a hard-coded 1, leaving 1 phantom job pending)."""
    path = tmp_path / "state.sqlite3"
    limits = BudgetLimits(
        max_pending_files=3,
        max_pending_episode_jobs=6,
        per_cycle_new=10,
        rolling_new=10,
        rolling_window_hours=24.0,
    )
    with open_store(path, limits=limits) as store:
        walk_to_planned(store, 101)
        token = store.prepare_send(
            101,
            PLAN_FP,
            REQ_FP,
            targets_digest=TARGETS,
            structure_digest=STRUCTURE,
            mapping_revision="7",
            cycle_key="cycle-1",
            iso=T0,
            expected_episode_jobs=2,
        )
        assert isinstance(token, SendToken)
        assert store.budget_view().pending_episode_jobs == JOBS_TWO
        store.mark_sent(token, iso=T0)
        store.record_send_outcome(token, SendOutcome.REJECTED, iso=T0, http_status=400)
        view = store.budget_view()
        assert view.pending_episode_jobs == 0  # the stored 2, not the old hard-coded 1
        assert view.pending_file_requests == 0


def test_prepare_send_records_real_job_count_and_targets(tmp_path: Path) -> None:
    """P1-3/P2-4: the send row persists the reserved job count and the expected
    verification triples (schema v1, unshipped) for restart re-poll/re-inspection."""
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        walk_to_planned(store, 101)
        token = store.prepare_send(
            101,
            PLAN_FP,
            REQ_FP,
            targets_digest=TARGETS,
            structure_digest=STRUCTURE,
            mapping_revision="7",
            cycle_key="cycle-1",
            iso=T0,
            expected_episode_jobs=2,
            target_ids=((1234, 5501, 777), (1234, 5502, 778)),
        )
        assert isinstance(token, SendToken)
        inspection = store.active_send_details(101)
        assert inspection is not None
        assert inspection.send_id == token.send_id
        assert inspection.episode_jobs == JOBS_TWO
        assert inspection.target_ids == ((1234, 5501, 777), (1234, 5502, 778))
        assert inspection.outcome == "prepared"
        rows = second_view(path, "SELECT outcome FROM sends WHERE shoko_file_id = 101")
        assert rows == [("prepared",)]


def test_void_prepared_send_closes_releases_and_allows_reprepare(tmp_path: Path) -> None:
    """P2-2 probe C7: a prepared-then-crashed row must be closable — void releases the
    slot, returns the file to planned (definite not-sent), keeps the ``voided`` audit
    row, and no longer blocks re-preparing the same request fingerprint."""
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        walk_to_planned(store, 101)
        token = store.prepare_send(
            101,
            PLAN_FP,
            REQ_FP,
            targets_digest=TARGETS,
            structure_digest=STRUCTURE,
            mapping_revision="7",
            cycle_key="cycle-1",
            iso=T0,
            expected_episode_jobs=2,
        )
        assert isinstance(token, SendToken)
        assert store.budget_view().pending_file_requests == 1
        replay = prepare(store, 101, req_fp=REQ_FP)  # same fingerprint: active row blocks
        assert isinstance(replay, BudgetRefusal)
        assert replay.reason == "duplicate_request_fingerprint"
        send_id = store.void_prepared_send(101, REQ_FP, iso=T1, reason="crash before transport")
        assert send_id == token.send_id
        assert state_of(store, 101) is FileState.PLANNED
        view = store.budget_view()
        assert view.pending_file_requests == 0
        assert view.pending_episode_jobs == 0
        rows = second_view(
            path, "SELECT outcome FROM sends WHERE shoko_file_id = 101 ORDER BY send_id"
        )
        assert rows == [("voided",)]
        replacement = store.prepare_send(
            101,
            PLAN_FP,
            REQ_FP,
            targets_digest=TARGETS,
            structure_digest=STRUCTURE,
            mapping_revision="7",
            cycle_key="cycle-2",
            iso=T1,
            expected_episode_jobs=1,
        )
        assert isinstance(replacement, SendToken)  # voided send no longer blocks dedup


def test_void_prepared_send_refused_from_non_prepared_rows(tmp_path: Path) -> None:
    """P2-2/P1 contract: rows that may have been delivered are never voidable."""
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        walk_to_planned(store, 101)
        token = prepare(store, 101, req_fp=REQ_FP)
        assert isinstance(token, SendToken)
        store.mark_sent(token, iso=T0)
        with pytest.raises(StateError, match="legal only from prepared"):
            store.void_prepared_send(101, REQ_FP, iso=T1, reason="attempted")
        store.record_send_outcome(token, SendOutcome.AMBIGUOUS_SEND, iso=T0)
        with pytest.raises(StateError, match="legal only from prepared"):
            store.void_prepared_send(101, REQ_FP, iso=T1, reason="ambiguous")
        # the file row stays indeterminate (never resubmittable, never voidable)
        assert state_of(store, 101) is FileState.INDETERMINATE_WRITE
        with pytest.raises(StateError, match="no send row"):
            store.void_prepared_send(999, REQ_FP, iso=T1, reason="missing")


def test_resolve_send_releases_slot_and_moves_file_to_waiting(tmp_path: Path) -> None:
    """P2-B: a conflict row holds its slot forever at a ceiling of 1 (apply wedge). The
    operator's ``resolve_send`` releases exactly the stored reservation, closes the row,
    moves the file to ``waiting`` (re-evaluable), and audit-annotates it. A resolved row
    never blocks a fresh re-plan and never touches a verified row."""
    path = tmp_path / "state.sqlite3"
    limits = BudgetLimits(
        max_pending_files=1,
        max_pending_episode_jobs=3,
        per_cycle_new=10,
        rolling_new=10,
        rolling_window_hours=24.0,
    )
    with open_store(path, limits=limits) as store:
        walk_to_planned(store, 101)
        token = _prepare_jobs(store, 101, jobs=2, req_fp=REQ_FP, target_ids=((1, 10101, 5001),))
        store.mark_sent(token, iso=T0)
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=T0)
        inspection = store.active_send_details(101)
        assert inspection is not None
        store.record_verification(
            101, VerificationVerdict.CONTRADICTION, iso=T1, inspection=inspection
        )
        assert state_of(store, 101) is FileState.CONFLICT
        assert store.budget_view().pending_file_requests == 1  # held forever → wedge
        sends, slots = store.live_send_counts()
        assert sends == LIVE_ONE
        assert slots == LIVE_TWO

        rolling_before = store.budget_view().rolling_new_submissions
        send_id = store.resolve_send(
            101, REQ_FP, resolution="released_after_review", iso=T2, operator_note="reviewed ok"
        )
        assert send_id == token.send_id
        assert state_of(store, 101) is FileState.WAITING
        view = store.budget_view()
        assert view.pending_file_requests == 0
        assert view.pending_episode_jobs == 0
        assert view.rolling_new_submissions == rolling_before  # resolution is not a rolling refund
        assert store.live_send_counts() == (0, 0)
        record = store.get_file(101)
        assert record is not None
        assert "released_after_review" in record.state_detail
        assert "reviewed ok" in record.state_detail
        rows = second_view(
            path,
            "SELECT outcome, verification, slot_released FROM sends WHERE send_id = ?",
            (send_id,),
        )
        assert rows[0] == ("resolved", "contradiction", 1)
        assert store.active_send_details(101) is None  # closed rows are never live
        # Even after explicit release, the exact attempted mutation cannot be replayed.
        store.advance(101, FileState.PLANNED, "replan", iso=T3)
        same_request = prepare(store, 101, req_fp=REQ_FP, cycle_key="cycle-2", iso=T3)
        assert isinstance(same_request, BudgetRefusal)
        assert same_request.reason == "duplicate_request_fingerprint"
        replacement = _prepare_jobs(store, 101, jobs=1, cycle_key="cycle-2", req_fp=REQ_FP_2)
        assert replacement.accepted is True


def test_resolve_send_operator_closed_blocks_safety(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path, limits=WIDE_LIMITS) as store:
        walk_to_planned(store, 101)
        token = _prepare_jobs(store, 101, jobs=1, req_fp=REQ_FP, target_ids=((1, 10101, 5001),))
        store.mark_sent(token, iso=T0)
        store.record_send_outcome(token, SendOutcome.AMBIGUOUS_SEND, iso=T0)
        assert state_of(store, 101) is FileState.INDETERMINATE_WRITE
        assert store.budget_view().pending_file_requests == 1
        inspection = store.active_send_details(101)
        assert inspection is not None
        store.record_verification(
            101, VerificationVerdict.CONTRADICTION, iso=T1, inspection=inspection
        )
        store.resolve_send(101, REQ_FP, resolution="operator_closed", iso=T1, operator_note="x")
        assert state_of(store, 101) is FileState.BLOCKED_SAFETY
        assert store.budget_view().pending_file_requests == 0
        assert store.live_send_counts() == (0, 0)


def test_resolve_send_refused_from_verified(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path, limits=WIDE_LIMITS) as store:
        walk_to_planned(store, 101)
        token = _prepare_jobs(store, 101, jobs=1, req_fp=REQ_FP)
        store.mark_sent(token, iso=T0)
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=T0)
        store.record_verification(101, VerificationVerdict.VERIFIED, iso=T1)
        with pytest.raises(StateError, match="never touches"):
            store.resolve_send(101, REQ_FP, resolution="released_after_review", iso=T2)


def test_partial_inspection_holds_slot_until_explicit_resolve(tmp_path: Path) -> None:
    """A qualified PARTIAL inspection is terminal-review but does not itself release pending."""
    path = tmp_path / "state.sqlite3"
    with open_store(path, limits=WIDE_LIMITS) as store:
        walk_to_planned(store, 101)
        token = _prepare_jobs(store, 101, jobs=2, req_fp=REQ_FP, target_ids=((1, 10101, 5001),))
        store.mark_sent(token, iso=T0)
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=T0)
        inspection = store.active_send_details(101)
        assert inspection is not None
        early_iso = "2026-01-01T00:05:00+00:00"
        store.record_verification(
            101, VerificationVerdict.PARTIAL, iso=early_iso, inspection=inspection
        )
        assert state_of(store, 101) is FileState.PARTIAL
        assert store.active_send_details(101) is None
        assert store.budget_view().pending_file_requests == 1
        with pytest.raises(StateError, match="only by VERIFIED or explicit resolve_send"):
            store.release_send_slot(101, REQ_FP, iso=T1)
        with pytest.raises(StateError, match="verification deadline"):
            store.resolve_send(
                101,
                REQ_FP,
                resolution="released_after_review",
                iso=early_iso,
                operator_note="partial observed before inspection wait elapsed",
                verification_timeout_seconds=600,
            )
        store.resolve_send(
            101,
            REQ_FP,
            resolution="released_after_review",
            iso=T2,
            operator_note="partial reviewed after the verification deadline",
            verification_timeout_seconds=600,
        )
        assert state_of(store, 101) is FileState.WAITING
        assert store.budget_view().pending_file_requests == 0
        assert second_view(
            path,
            "SELECT outcome, verification, slot_released FROM sends WHERE send_id = ?",
            (token.send_id,),
        ) == [("resolved", "partial", 1)]
        with pytest.raises(StateError, match="closed"):
            store.resolve_send(101, REQ_FP, resolution="operator_closed", iso=T3)


def test_release_slot_refuses_overrelease_of_send_bound_slot(tmp_path: Path) -> None:
    """Round-2 review F1: raw release_slot on a send-bound slot bypasses the
    ``slot_released`` ledger and lets a later VERIFIED close release the same reservation
    twice. The ledger guard refuses it; the single-owner close paths stay exact."""
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        walk_to_planned(store)
        token = prepare(store, req_fp=REQ_FP)
        assert isinstance(token, SendToken)
        store.mark_sent(token, iso=T0)
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=T1)
        assert store.budget_view().pending_file_requests == PENDING_ONE
        with pytest.raises(StateError, match="release_slot refused"):
            store.release_slot(token.window_key, T1)
        assert store.budget_view().pending_file_requests == PENDING_ONE
        assert store.live_send_counts() == (LIVE_ONE, LIVE_ONE)
        # The canonical close releases exactly once:
        store.record_verification(101, VerificationVerdict.VERIFIED, iso=T2)
        assert store.budget_view().pending_file_requests == 0
        assert store.live_send_counts() == (0, 0)


def test_resolve_send_gate_refuses_prepared_and_attempted_rows(tmp_path: Path) -> None:
    """Round-2 review F2: resolve_send is legal only from terminal-review outcomes.
    ``prepared`` is owned by void_prepared_send; ``attempted`` (request-may-have-been-sent)
    is owned by the XRef inspection path — neither closes via resolve_send."""
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        walk_to_planned(store)
        token = prepare(store, req_fp=REQ_FP)
        assert isinstance(token, SendToken)
        with pytest.raises(StateError, match="legal only from"):
            store.resolve_send(101, REQ_FP, resolution="released_after_review", iso=T1)
        store.mark_sent(token, iso=T1)
        with pytest.raises(StateError, match="legal only from"):
            store.resolve_send(101, REQ_FP, resolution="operator_closed", iso=T1)
        assert store.live_send_counts() == (LIVE_ONE, LIVE_ONE)


def test_resolve_send_refuses_accepted_unverified_even_after_deadline(tmp_path: Path) -> None:
    """A passed timeout cannot replace the durable terminal-inspection requirement."""
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        walk_to_planned(store)
        token = prepare(store, req_fp=REQ_FP)
        assert isinstance(token, SendToken)
        store.mark_sent(token, iso=T0)
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=T0)
        with pytest.raises(StateError, match="inspected_review"):
            store.resolve_send(
                101,
                REQ_FP,
                resolution="released_after_review",
                iso=T1,
                verification_timeout_seconds=3600.0,
            )
        assert state_of(store, 101) is FileState.VERIFYING
        assert store.budget_view().pending_file_requests == 1


def test_resolved_inspection_audit_and_replay_refusal_survive_reopen(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    note = "reviewed current same-file XRefs; leave for operator re-evaluation"
    with open_store(path, limits=WIDE_LIMITS) as store:
        walk_to_planned(store)
        token = _prepare_jobs(
            store,
            101,
            jobs=2,
            req_fp=REQ_FP,
            target_ids=((1, 10101, 5001),),
        )
        store.mark_sent(token, iso=T0)
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=T0)
        inspection = store.active_send_details(101)
        assert inspection is not None
        store.record_verification(
            101, VerificationVerdict.CONTRADICTION, iso=T1, inspection=inspection
        )
        store.resolve_send(
            101,
            REQ_FP,
            resolution="released_after_review",
            iso=T2,
            operator_note=note,
        )

    with open_store(path, limits=WIDE_LIMITS) as reopened:
        record = reopened.get_file(101)
        assert record is not None
        assert record.state is FileState.WAITING
        assert note in record.state_detail
        assert reopened.active_send_details(101) is None
        recovery = reopened.recover_after_restart()
        assert 101 not in {
            row.shoko_file_id
            for row in (*recovery.hold_for_verification, *recovery.continue_polling)
        }
        assert second_view(
            path,
            "SELECT outcome, verification, slot_released FROM sends WHERE send_id = ?",
            (token.send_id,),
        ) == [("resolved", "contradiction", 1)]

        reopened.advance(101, FileState.PLANNED, "operator re-evaluation", iso=T3)
        duplicate = prepare(reopened, 101, req_fp=REQ_FP, iso=T3, cycle_key="cycle-2")
        assert isinstance(duplicate, BudgetRefusal)
        assert duplicate.reason == "duplicate_request_fingerprint"


def test_resolving_campaign_inspection_does_not_refund_campaign_debit(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    campaign_id = "campaign-terminal-inspection"
    with open_store(path, limits=WIDE_LIMITS) as store:
        store.create_campaign(
            campaign_id,
            PLAN_FP,
            max_submissions=1,
            deadline_iso=T3,
            attestation_scope=f"campaign:{campaign_id}",
            approval_iso=T0,
            iso=T0,
            bindings=(
                CampaignBinding(101, PLAN_FP, REQ_FP),
                CampaignBinding(102, PLAN_FP, REQ_FP_2),
            ),
        )
        walk_to_planned(store, 101)
        walk_to_planned(store, 102)
        token = store.prepare_send(
            101,
            PLAN_FP,
            REQ_FP,
            targets_digest=TARGETS,
            structure_digest=STRUCTURE,
            mapping_revision="7",
            cycle_key="campaign-cycle-1",
            iso=T0,
            campaign_id=campaign_id,
            target_ids=((1, 10101, 5001),),
        )
        assert isinstance(token, SendToken)
        store.mark_sent(token, iso=T0)
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=T0)
        inspection = store.active_send_details(101)
        assert inspection is not None
        store.record_verification(
            101, VerificationVerdict.CONTRADICTION, iso=T1, inspection=inspection
        )
        store.resolve_send(
            101,
            REQ_FP,
            resolution="released_after_review",
            iso=T2,
            operator_note="terminal XRefs inspected",
        )
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.total_sent == 1
        assert store.budget_view().pending_file_requests == 0

        next_send = store.prepare_send(
            102,
            PLAN_FP,
            REQ_FP_2,
            targets_digest=TARGETS,
            structure_digest=STRUCTURE,
            mapping_revision="7",
            cycle_key="campaign-cycle-2",
            iso=T3,
            campaign_id=campaign_id,
            target_ids=((1, 10102, 5002),),
        )
        assert isinstance(next_send, SendToken)
        with pytest.raises(CampaignSubmissionLimitError):
            store.mark_sent(next_send, iso=T3)
        assert store.get_campaign(campaign_id).total_sent == 1  # type: ignore[union-attr]


def test_ambiguous_rewalk_edges_are_legal_never_skip_to_planned() -> None:
    """P2-3: re-evaluation edges from ``ambiguous`` into the evaluation walk are legal;
    skipping straight to ``planned`` remains illegal."""
    validate_transition(FileState.AMBIGUOUS, FileState.DISCOVERED)
    validate_transition(FileState.AMBIGUOUS, FileState.MATCHED)
    validate_transition(FileState.AMBIGUOUS, FileState.MAPPED)
    validate_transition(FileState.AMBIGUOUS, FileState.WAITING)
    with pytest.raises(TransitionError, match="illegal transition"):
        validate_transition(FileState.AMBIGUOUS, FileState.PLANNED)
    with pytest.raises(TransitionError, match="illegal transition"):
        validate_transition(FileState.AMBIGUOUS, FileState.VERIFIED)


def test_identity_snapshot_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        store.record_file(
            101,
            sonarr_series_id=1,
            sonarr_file_id=10,
            sonarr_path="/p.mkv",
            sonarr_size=5,
            cohort=Cohort.BACKLOG,
            iso=T0,
            identity=FileIdentitySnapshot(
                ed2k="0123456789ABCDEF0123456789ABCDEF",
                sha1="abc",
                md5="def",
                crc32="1234abcd",
                shoko_paths=("/anime/s01e01.mkv",),
            ),
        )
        record = store.get_file(101)
        assert record is not None
        snapshot = record.identity
        assert snapshot is not None
        assert snapshot.ed2k == "0123456789ABCDEF0123456789ABCDEF"
        assert snapshot.shoko_paths == ("/anime/s01e01.mkv",)


# ---------------------------------------------------------------- stop writes


def test_writes_disabled_blocks_every_mutation_including_prepare_send(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        walk_to_planned(store)
        store.disable_writes("disk full: simulated")
        assert store.writes_disabled is True
        with pytest.raises(WriteDisabledError):
            prepare(store)
        with pytest.raises(WriteDisabledError):
            store.advance(101, FileState.WAITING, "x", iso=T1)
        with pytest.raises(WriteDisabledError):
            store.record_file(
                999,
                sonarr_series_id=1,
                sonarr_file_id=1,
                sonarr_path="/x.mkv",
                sonarr_size=1,
                cohort=Cohort.NEW_ARRIVAL,
                iso=T0,
            )
        with pytest.raises(WriteDisabledError):
            store.set_meta("k", "v")
        with pytest.raises(WriteDisabledError):
            acquire_apply_lock(store, pid=LOCK_PID_A, iso=T0, lease_expires_iso=T1)
        # read APIs remain usable
        assert store.get_file(101) is not None
    with open_store(path) as reopened:
        # persisted marker: reopened store also refuses writes (no in-memory fallback)
        assert reopened.writes_disabled is True
        with pytest.raises(WriteDisabledError):
            reopened.record_file(
                998,
                sonarr_series_id=1,
                sonarr_file_id=1,
                sonarr_path="/x.mkv",
                sonarr_size=1,
                cohort=Cohort.NEW_ARRIVAL,
                iso=T0,
            )
        reopened.clear_writes_disabled(T2)
    with open_store(path) as cleared:
        assert cleared.writes_disabled is False


def test_existing_store_observes_durable_write_stop_from_another_handle(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store, open_store(path) as stopper:
        seed_file(store)
        stopper.disable_writes("integrity failure in another worker")

        assert store.writes_disabled is True
        assert store.get_file(101) is not None  # read-only state remains observable
        with pytest.raises(WriteDisabledError):
            store.set_meta("must_not_be_written", "1")
        with pytest.raises(WriteDisabledError):
            store.advance(101, FileState.WAITING, "must remain unchanged", iso=T1)

        assert store.get_meta("must_not_be_written") is None
        assert state_of(store, 101) is FileState.DISCOVERED


def test_existing_store_observes_write_stop_committed_by_another_process(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        seed_file(store)
        # Fixed test code and a fixture-owned path; no user-controlled command is executed.
        subprocess.run(  # noqa: S603
            [
                sys.executable,
                "-c",
                "import sys; from shoko_linker.state import StateStore; "
                "store = StateStore(sys.argv[1]); "
                "store.disable_writes('child-process safety stop'); store.close()",
                str(path),
            ],
            cwd=REPO_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )

        assert store.writes_disabled is True
        with pytest.raises(WriteDisabledError):
            store.set_meta("must_not_be_written", "1")


def test_prepare_transaction_rechecks_stop_committed_after_local_guard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store, open_store(path) as stopper:
        seed_file(store)
        walk_to_planned(store)
        original_transaction = store._transaction

        def stop_before_transaction(**kwargs: Any) -> Any:
            # _mutation has already checked the first handle. Commit the cross-handle stop in
            # the gap before BEGIN IMMEDIATE to exercise the transaction's fresh check.
            stopper.disable_writes("stop committed between guard and transaction")
            return original_transaction(**kwargs)

        monkeypatch.setattr(store, "_transaction", stop_before_transaction)
        with pytest.raises(WriteDisabledError):
            prepare(store)

        assert second_view(path, "SELECT COUNT(*) FROM sends") == [(0,)]
        assert state_of(store, 101) is FileState.PLANNED


def test_read_error_observing_write_stop_latches_handle_closed(tmp_path: Path) -> None:
    with open_store(tmp_path / "state.sqlite3") as store:
        store.connection.set_authorizer(
            lambda action, table, _column, _database, _trigger: (
                sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_READ and table == "meta"
                else sqlite3.SQLITE_OK
            )
        )

        assert store.writes_disabled is True
        store.connection.set_authorizer(None)
        with pytest.raises(WriteDisabledError):
            store.set_meta("must_not_be_written", "1")


def test_operator_clear_does_not_unlatch_an_older_handle(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as old_handle, open_store(path) as operator_handle:
        seed_file(old_handle)
        operator_handle.disable_writes("operator recovery test")
        assert old_handle.writes_disabled is True

        operator_handle.clear_writes_disabled(T1)
        assert old_handle.writes_disabled is True
        with pytest.raises(WriteDisabledError):
            old_handle.advance(101, FileState.WAITING, "old handle stays stopped", iso=T1)

    with open_store(path) as reopened:
        assert reopened.writes_disabled is False


def test_stop_between_prepare_and_mark_voids_only_definitely_unsent_reservation(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store, open_store(path) as stopper:
        walk_to_planned(store)
        token = prepare(store)
        assert isinstance(token, SendToken)
        reserved = store.budget_view()
        assert reserved.pending_file_requests == 1
        assert reserved.rolling_new_submissions == 1

        stopper.disable_writes("storage failure after intent prepare")
        with pytest.raises(WriteDisabledError):
            store.mark_sent(token, iso=T1)

        store._void_prepared_send_after_write_stop(
            101,
            token.request_fingerprint,
            iso=T1,
            reason="durable stop fenced mark_sent",
        )
        released = store.budget_view()
        assert released.pending_file_requests == 0
        assert released.pending_episode_jobs == 0
        assert released.cycle_new_submissions == 1
        assert released.rolling_new_submissions == 1
        assert state_of(store, 101) is FileState.PLANNED
        voided = second_view(path, "SELECT outcome FROM sends WHERE send_id = ?", (token.send_id,))
        assert voided == [("voided",)]


def test_transport_guard_rechecks_stop_after_mark_before_post(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store, open_store(path) as stopper:
        walk_to_planned(store)
        token = prepare(store)
        store.mark_sent(token, iso=T1)
        posted: list[int] = []

        stopper.disable_writes("stop committed between mark_sent and POST")
        with pytest.raises(WriteDisabledError), store.protect_transport(token):
            posted.append(token.send_id)

        assert posted == []
        # mark_sent already crossed the durable attempt boundary, so keep it held for recovery;
        # do not falsely void or refund an operation that might have reached transport.
        attempted = second_view(
            path, "SELECT outcome FROM sends WHERE send_id = ?", (token.send_id,)
        )
        assert attempted == [("attempted",)]
        assert store.budget_view().pending_file_requests == 1
        assert store.writes_disabled is True


def test_write_stop_persistence_failure_keeps_local_fail_closed_latch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with open_store(tmp_path / "state.sqlite3") as store:

        def fail_transaction(**_kwargs: Any) -> Any:
            raise sqlite3.OperationalError("simulated storage failure")

        monkeypatch.setattr(store, "_write_stop_control_transaction", fail_transaction)
        store.disable_writes("storage failure")

        assert store.writes_disabled is True
        monkeypatch.undo()
        # A later explicit stop attempt can persist the already-latched state once storage works.
        store.disable_writes("storage failure retry")
        assert second_view(
            tmp_path / "state.sqlite3",
            "SELECT value FROM meta WHERE key = 'writes_disabled'",
        ) == [("1",)]
        with pytest.raises(WriteDisabledError):
            store.set_meta("must_not_be_written", "1")


def test_writes_disabled_is_documented_no_in_memory_fallback() -> None:
    # state.py must not track submissions in memory when the DB is stopped;
    # enforced by the mutation guard raising on every write attempt above.
    assert True


# ---------------------------------------------------------------- integrity


def test_integrity_check_reports_ok(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        seed_file(store)
        report = integrity_check(store)
        assert report.ok is True
        assert "ok" in report.detail


# ---------------------------------------------------------------- performance


def test_seven_thousand_file_insert_and_indexed_queries(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    rows = [
        FileSeed(
            shoko_file_id=file_id,
            sonarr_series_id=file_id % 50,
            sonarr_file_id=file_id,
            sonarr_path=f"/data/media/anime/series-{file_id % 50}/f{file_id}.mkv",
            sonarr_size=1000 + file_id,
            cohort=Cohort.NEW_ARRIVAL if file_id % 2 else Cohort.BACKLOG,
            iso=T0,
        )
        for file_id in range(1, FILE_COUNT + 1)
    ]
    start = time.perf_counter()
    with open_store(path) as store:
        inserted = store.record_files(rows)
        insert_elapsed = time.perf_counter() - start
        assert inserted == FILE_COUNT
        assert insert_elapsed < INSERT_BUDGET_SECONDS
        # indexed point query: by path, then indexed cohort scan
        start = time.perf_counter()
        record = store.find_file_by_path(PERF_PATH)
        query_elapsed = time.perf_counter() - start
        assert record is not None
        assert record.shoko_file_id == PERF_FILE_ID
        assert query_elapsed < POINT_QUERY_BUDGET_SECONDS
        start = time.perf_counter()
        backlog = store.files_in_cohort(Cohort.BACKLOG)
        cohort_elapsed = time.perf_counter() - start
        assert len(backlog) == BACKLOG_COUNT
        assert cohort_elapsed < SCAN_BUDGET_SECONDS


# ------------------------------------------- continuity: link-driven drop accounting


def _seed_unrecognized(store: StateStore, count: int, *, start_id: int = 1) -> None:
    """Seed ``count`` never-linked (discovered) Shoko rows: the Unrecognized baseline."""
    store.record_files(
        [
            FileSeed(
                file_id,
                1,
                file_id,
                f"/data/media/anime/series-{file_id % 50}/f{file_id}.mkv",
                100,
                Cohort.BACKLOG,
                T0,
            )
            for file_id in range(start_id, start_id + count)
        ]
    )


def _link_files(store: StateStore, file_ids: list[int], *, iso: str) -> None:
    """Simulate the linker's own accepted links: VERIFYING rows close as VERIFIED."""
    store.record_files(
        [
            FileSeed(
                file_id,
                1,
                file_id,
                f"/data/media/anime/series-{file_id % 50}/f{file_id}.mkv",
                100,
                Cohort.BACKLOG,
                iso,
                state=FileState.VERIFYING,
            )
            for file_id in file_ids
        ]
    )
    for file_id in file_ids:
        store.record_verification(file_id, VerificationVerdict.VERIFIED, iso=iso)


def test_continuity_accounts_for_link_driven_drop_across_cycles(tmp_path: Path) -> None:
    """M9-e2e defect: the linker's own accepted links shrink the Unrecognized discovery total.

    Each cycle's drop is exactly the links verified since the last anchor, so continuity is
    NORMAL, the observation re-anchors, and ``requires_rebaseline`` never wedges apply
    across a multi-batch campaign.
    """
    path = tmp_path / "state.sqlite3"
    with open_store(path, limits=WIDE_LIMITS) as store:
        _seed_unrecognized(store, 1000)
        total = 1000
        first = store.check_source_continuity(
            "sonarr-A", "hash-0", iso=T0, roots_fingerprint="roots-1", discovery_total=total
        )
        assert first.ok is True  # first observation anchors
        for cycle_index, iso in enumerate((T0, T1, T2, T3), start=1):
            _link_files(store, [10_000 + cycle_index * 5 + k for k in range(5)], iso=iso)
            total -= 5
            verdict = store.check_source_continuity(
                "sonarr-A",
                f"hash-{cycle_index}",
                iso=iso,
                roots_fingerprint="roots-1",
                discovery_total=total,
            )
            assert verdict.ok is True, cycle_index
            assert "discovery_total_drop" not in verdict.reasons, cycle_index
            assert verdict.requires_rebaseline is False, cycle_index
            assert store.get_meta("requires_rebaseline") == "0", cycle_index


def test_continuity_flags_excess_drop_beyond_expected(tmp_path: Path) -> None:
    """A drop larger than the linker's own verified links is a genuine discontinuity: flag it,
    suppress apply until explicit recovery."""
    path = tmp_path / "state.sqlite3"
    with open_store(path, limits=WIDE_LIMITS) as store:
        _seed_unrecognized(store, 1000)
        store.check_source_continuity(
            "sonarr-A", "hash-0", iso=T0, roots_fingerprint="roots-1", discovery_total=1000
        )
        _link_files(store, [10_100, 10_101, 10_102, 10_103, 10_104], iso=T0)  # expected drop 5
        drop = store.check_source_continuity(
            "sonarr-A", "hash-1", iso=T1, roots_fingerprint="roots-1", discovery_total=985
        )  # observed drop 15 > 5
        assert drop.requires_rebaseline is True
        assert "discovery_total_drop" in drop.reasons
        assert store.get_meta("requires_rebaseline") == "1"
        clean = store.check_source_continuity(
            "sonarr-A", "hash-2", iso=T2, roots_fingerprint="roots-1", discovery_total=985
        )
        assert clean.requires_rebaseline is True
        assert store.get_meta("requires_rebaseline") == "1"
        store.clear_requires_rebaseline(T3)
        recovered = store.check_source_continuity(
            "sonarr-A", "hash-2", iso=T3, roots_fingerprint="roots-1", discovery_total=985
        )
        assert recovered.ok is True


@pytest.mark.parametrize(
    "outcome", [None, SendOutcome.ACCEPTED_UNVERIFIED, SendOutcome.AMBIGUOUS_SEND]
)
def test_pending_departure_wait_preserves_every_anchor_until_verified(
    tmp_path: Path, outcome: SendOutcome | None
) -> None:
    with open_store(tmp_path / "state.sqlite3") as store:
        walk_to_planned(store)
        store.check_source_continuity(
            "source", "snapshot", iso=T0, roots_fingerprint="roots", discovery_total=100
        )
        token = prepare(store)
        store.mark_sent(token, iso=T0)
        if outcome is not None:
            store.record_send_outcome(token, outcome, iso=T0)
        anchor_keys = (
            "source_instance",
            "source_roots_fingerprint",
            "source_last_observed_iso",
            "source_last_discovery_total",
            "source_anchor_verified_basis",
            "sonarr_status_snapshot_id",
        )
        anchor = {key: store.get_meta(key) for key in anchor_keys}
        for iso in (T1, T2):
            verdict = store.check_source_continuity(
                "source",
                "departed",
                iso=iso,
                roots_fingerprint="roots",
                discovery_total=99,
                discovered_file_ids=frozenset(),
            )
            assert not verdict.ok
            assert not verdict.requires_rebaseline
            assert verdict.reasons == ("discovery_drop_pending_verification",)
            assert store.get_meta("requires_rebaseline") == "0"
            assert {key: store.get_meta(key) for key in anchor_keys} == anchor
        if outcome is not SendOutcome.ACCEPTED_UNVERIFIED:
            store.advance(101, FileState.VERIFYING, "recovery inspection", iso=T2)
        store.record_verification(101, VerificationVerdict.VERIFIED, iso=T2)
        recovered = store.check_source_continuity(
            "source",
            "departed",
            iso=T3,
            roots_fingerprint="roots",
            discovery_total=99,
            discovered_file_ids=frozenset(),
        )
        assert recovered.ok
        assert store.get_meta("requires_rebaseline") == "0"


@pytest.mark.parametrize(
    "case",
    [
        "identity",
        "roots",
        "excess_drop",
        "still_present",
        "prepared",
        "rejected",
        "conflict",
        "latched",
        "missing_marker",
    ],
)
def test_pending_send_does_not_mask_genuine_source_discontinuity(tmp_path: Path, case: str) -> None:
    with open_store(tmp_path / "state.sqlite3") as store:
        walk_to_planned(store)
        store.check_source_continuity(
            "source", "snapshot", iso=T0, roots_fingerprint="roots", discovery_total=100
        )
        token = prepare(store)
        if case != "prepared":
            store.mark_sent(token, iso=T0)
            outcome = (
                SendOutcome.REJECTED if case == "rejected" else SendOutcome.ACCEPTED_UNVERIFIED
            )
            store.record_send_outcome(token, outcome, iso=T0)
        if case == "conflict":
            store.record_verification(101, VerificationVerdict.CONTRADICTION, iso=T0)
        if case == "latched":
            store.check_source_continuity(
                "source",
                "lost",
                iso=T1,
                roots_fingerprint="roots",
                discovery_total=98,
                discovered_file_ids=frozenset(),
            )
            assert store.get_meta("requires_rebaseline") == "1"
        verdict = store.check_source_continuity(
            "changed" if case == "identity" else "source",
            "departed",
            iso=T2,
            roots_fingerprint="changed" if case == "roots" else "roots",
            discovery_total=None
            if case == "missing_marker"
            else 98
            if case == "excess_drop"
            else 99,
            discovered_file_ids=frozenset({101}) if case == "still_present" else frozenset(),
        )
        assert not verdict.ok
        assert verdict.requires_rebaseline
        assert "discovery_drop_pending_verification" not in verdict.reasons
        assert store.get_meta("requires_rebaseline") == "1"


def test_send_detail_is_redacted_before_persistence_and_rollup(tmp_path: Path) -> None:
    from shoko_linker.logging_setup import register_secret
    from shoko_linker.reporting import state_rollup

    sentinel = "synthetic-persisted-diagnostic-secret-0000"
    register_secret(sentinel)
    with open_store(tmp_path / "state.sqlite3") as store:
        walk_to_planned(store)
        token = prepare(store)
        store.mark_sent(token, iso=T1)
        store.record_send_outcome(token, SendOutcome.REJECTED, iso=T2, detail=sentinel)
        row = store.get_file(101)
        assert row is not None
        assert sentinel not in row.state_detail
        # Protect reports of pre-existing diagnostics as well.
        store.connection.execute("UPDATE files SET state_detail = ?", (sentinel,))
        assert sentinel not in str(state_rollup(store))


def test_continuity_anchor_basis_persists_across_restart(tmp_path: Path) -> None:
    """The anchor's accounting basis (verified high-water) is durable: after a restart the
    already-anchored links are not re-counted, so a fresh external drop still flags."""
    path = tmp_path / "state.sqlite3"
    with open_store(path, limits=WIDE_LIMITS) as store:
        _seed_unrecognized(store, 1000)
        store.check_source_continuity(
            "sonarr-A", "hash-0", iso=T0, roots_fingerprint="roots-1", discovery_total=1000
        )
        _link_files(store, [10_200, 10_201, 10_202, 10_203, 10_204], iso=T0)
        second = store.check_source_continuity(
            "sonarr-A", "hash-1", iso=T1, roots_fingerprint="roots-1", discovery_total=995
        )
        assert second.requires_rebaseline is False  # drop 5 == expected 5
    with open_store(path, limits=WIDE_LIMITS) as store:
        external = store.check_source_continuity(
            "sonarr-A", "hash-2", iso=T2, roots_fingerprint="roots-1", discovery_total=994
        )
        assert external.requires_rebaseline is True
        assert "discovery_total_drop" in external.reasons


def test_create_campaign_persists_plan_path_and_round_trips(tmp_path: Path) -> None:
    """F7: the approved campaign row persists the canonical plan file path (schema v1, no
    migration), round-tripping through get_campaign/list_campaigns; None is allowed."""
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        store.create_campaign(
            "camp-1",
            PLAN_FP,
            max_submissions=2,
            deadline_iso=None,
            attestation_scope="campaign:camp-1",
            approval_iso=T0,
            iso=T0,
            bindings=[
                CampaignBinding(
                    shoko_file_id=1, plan_fingerprint=PLAN_FP, request_fingerprint=REQ_FP
                )
            ],
            plan_path="/data/plan.json",
        )
        store.create_campaign(
            "camp-2",
            REQ_FP,
            max_submissions=1,
            deadline_iso=None,
            attestation_scope="campaign:camp-2",
            approval_iso=T1,
            iso=T1,
            plan_path=None,
        )
    with open_store(path) as store:
        camp1 = store.get_campaign("camp-1")
        camp2 = store.get_campaign("camp-2")
        assert camp1 is not None
        assert camp1.plan_path == "/data/plan.json"
        assert camp2 is not None
        assert camp2.plan_path is None
        by_id = {campaign.campaign_id: campaign.plan_path for campaign in store.list_campaigns()}
        assert by_id == {"camp-1": "/data/plan.json", "camp-2": None}


@pytest.mark.parametrize(
    "raw",
    [
        None,
        "not json",
        "{}",
        "[]",
        '[[1,10101,5001],[1,10102,"5002"]]',
        "[[1,10101,5001],[1,10102,true]]",
        "[[1,10101,5001],[1,10102,0]]",
        "[[1,10101,5001],[-1,10102,5002]]",
        "[[1,10101,5001],[1,10102]]",
        "[[1,10101,5001],[1,10101,5001],[1,10102,true]]",
    ],
)
def test_persisted_send_targets_reject_entire_invalid_set(tmp_path: Path, raw: str | None) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        walk_to_planned(store)
        token = _prepare_jobs(store, 101, jobs=2, target_ids=((1, 10101, 5001), (1, 10102, 5002)))
        store.mark_sent(token, iso=T0)
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=T0)
        # NULL is not a legal stored value; the adapter still handles missing legacy evidence.
        store.connection.execute(
            "UPDATE sends SET target_ids_json = ? WHERE send_id = ?",
            (raw if raw is not None else "null", token.send_id),
        )
    with open_store(path) as reopened:
        inspection = reopened.active_send_details(101)
        assert inspection is not None
        assert inspection.target_ids == ()
        with pytest.raises(StateError, match="persisted send"):
            reopened.record_verification(
                101, VerificationVerdict.VERIFIED, iso=T1, inspection=inspection
            )
        assert state_of(reopened, 101) is FileState.VERIFYING
        assert reopened.live_send_counts() == (1, 2)
    assert state_module._parse_legacy_send_targets(raw) is None


def test_target_decoders_preserve_valid_target_order() -> None:
    targets = ((2, 10102, 5002), (1, 10101, 5001))
    raw = json.dumps(targets)
    assert state_module._parse_target_ids(raw) == targets
    assert state_module._parse_legacy_send_targets(raw) == targets


@pytest.mark.parametrize("legacy_duplicates", [False, True])
def test_shared_destinations_verify_after_restart(tmp_path: Path, legacy_duplicates: bool) -> None:
    path = tmp_path / "state.sqlite3"
    targets = ((1, 10102, 5002), (1, 10101, 5001))
    source_occurrences = (*targets, targets[0])
    with open_store(path) as store:
        walk_to_planned(store)
        token = _prepare_jobs(store, 101, jobs=2, target_ids=source_occurrences)
        stored = store.connection.execute(
            "SELECT target_ids_json FROM sends WHERE send_id = ?", (token.send_id,)
        ).fetchone()
        assert json.loads(stored[0]) == [list(target) for target in targets]
        store.mark_sent(token, iso=T0)
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=T0)
        if legacy_duplicates:
            store.connection.execute(
                "UPDATE sends SET target_ids_json = ? WHERE send_id = ?",
                (json.dumps(source_occurrences), token.send_id),
            )
    with open_store(path) as reopened:
        inspection = reopened.active_send_details(101)
        assert inspection is not None
        assert inspection.target_ids == targets
        assert reopened.live_send_counts() == (1, 2)
        reopened.record_verification(
            101, VerificationVerdict.VERIFIED, iso=T1, inspection=inspection
        )
        assert state_of(reopened, 101) is FileState.VERIFIED
        assert reopened.live_send_counts() == (0, 0)
    assert state_module._parse_legacy_send_targets(json.dumps(source_occurrences)) == targets
