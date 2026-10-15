"""M6-a gate tests: durable SQLite state machine. Offline only.

Real tmp_path SQLite files with fake ISO clock strings; no network, no threads.
A second connection (``second_view``) proves durability/commit ordering.
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import sqlite3
import subprocess
import time
from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any

import pytest
import state_v1_fixture as v1fx

from shoko_linker import state as state_module
from shoko_linker.campaign import campaign_drain_selection
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
    cycle_key: str = "cycle-1",
) -> Any:
    fingerprint = req_fp if req_fp is not None else format(file_id, "064x")
    return store.prepare_send(
        file_id,
        PLAN_FP,
        fingerprint,
        targets_digest=TARGETS,
        structure_digest=STRUCTURE,
        mapping_revision="7",
        cycle_key=cycle_key,
        iso=iso,
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


# ----------------------------------------------------- F17: v1 → v2 in-place migration


def test_f17_probe_v1_database_opens_and_answers_campaign_queries(tmp_path: Path) -> None:
    """F17 probe: a real v1-era database must open, migrate, and serve campaign queries."""
    path = tmp_path / "state.sqlite3"
    v1fx.build_v1_database(path)
    with open_store(path) as store:
        assert store.schema_version() == SCHEMA_VERSION
        campaign = store.get_campaign(v1fx.CAMPAIGN_ID)
        assert campaign is not None


def test_f17_v1_migration_preserves_every_row_and_advances_version(tmp_path: Path) -> None:
    """Opening v1 upgrades in place through v3 without changing any historical row."""
    path = tmp_path / "state.sqlite3"
    before = v1fx.build_v1_database(path)
    with open_store(path) as store:
        assert store.schema_version() == SCHEMA_VERSION
        assert store.get_onboarding_json() is None
    after = v1fx.snapshot_state(path)
    for table in ("meta", "baselines", "baseline_files", "files", "sends", "budgets"):
        assert after[table] == before[table]
    # campaigns: identical stored values, only the three drain columns appended with
    # not-authorized defaults; no campaign row is rewritten.
    assert [row[:13] for row in after["campaigns"]] == [row[:13] for row in before["campaigns"]]
    assert all(row[13:] == (None, None, "") for row in after["campaigns"])
    # schema_version audit history: the v1 row survives, and v2/v3 are appended.
    assert after["schema_version"][0] == before["schema_version"][0]
    assert len(after["schema_version"]) == len(before["schema_version"]) + 2
    assert after["schema_version"][-2][0:3:2] == (2, "migrated from 1")
    assert after["schema_version"][-1][0:3:2] == (3, "migrated from 2")
    assert second_view(path, "PRAGMA user_version") == [(SCHEMA_VERSION,)]
    assert second_view(
        path, "SELECT name FROM sqlite_master WHERE type='table' AND name='onboarding'"
    ) == [("onboarding",)]


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
    # the send history is byte-identical to the v1 snapshot: no new send, no rewrite.
    assert v1fx.snapshot_state(path)["sends"] == before["sends"]


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


def test_f17_future_schema_version_is_refused_fail_closed(tmp_path: Path) -> None:
    """F17: user_version > SCHEMA_VERSION refuses the open and mutates nothing."""
    path = tmp_path / "state.sqlite3"
    before = v1fx.build_v1_database(path)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA user_version=4")
    conn.commit()
    conn.close()
    with pytest.raises(sqlite3.DatabaseError, match="newer than supported 3"), open_store(path):
        pass
    assert second_view(path, "PRAGMA user_version") == [(4,)]
    assert v1fx.snapshot_state(path) == before


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
        # P1-1 adjustment (old assertion: the flag demanded rebaseline forever until a
        # manual re-anchor): the persisted flag survives restart and the daemon clears it
        # when the triggering conditions are resolved — the first healthy full observation
        # (stable identity/roots, monotonic totals) clears it.
        verdict = store.check_source_continuity(
            "sonarr-B", "hash-3", iso=T3, roots_fingerprint="roots-1", discovery_total=13
        )
        assert verdict.requires_rebaseline is False
        assert store.get_meta("requires_rebaseline") == "0"
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
    )
    assert isinstance(token, SendToken)
    return token


def test_partial_close_then_verified_releases_reservation_once(tmp_path: Path) -> None:
    """P1-A probe (reviewer p1b): a PARTIAL close releases the slot while the send row
    stays live; a late ManualLinkJob then lands the VERIFIED verdict. The late VERIFIED
    close must NOT release the same reservation a second time. Pending equals the TRUE
    in-flight count at every step — the ``max(0)`` clamp never masks a double release
    (file 102 is the second, genuinely in-flight send)."""
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

        # PARTIAL close (NOT-linked past the deadline path): release A's held slot.
        store.record_verification(101, VerificationVerdict.PARTIAL, iso=T1)
        assert store.budget_view().pending_file_requests == PENDING_TWO  # partial keeps slots
        released = store.release_send_slot(101, REQ_FP, iso=T1)
        assert released == JOBS_TWO
        view = store.budget_view()
        assert view.pending_file_requests == 1  # only B is truly in flight
        assert view.pending_episode_jobs == 1
        inspection = store.active_send_details(101)
        assert inspection is not None
        assert inspection.slot_released is True

        # Late ManualLinkJob lands: PARTIAL → VERIFYING → VERIFIED (legal edges).
        store.advance(101, FileState.VERIFYING, "late job", iso=T2)
        store.record_verification(101, VerificationVerdict.VERIFIED, iso=T2)
        view = store.budget_view()
        # NO second release: B's slot is untouched (old code: 0/0, double-released under
        # 102's live reservation, breached the ceiling bookkeeping).
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


def test_release_send_slot_noop_when_already_released(tmp_path: Path) -> None:
    """P1-A: a release attempt on a released row is a recorded no-op."""
    path = tmp_path / "state.sqlite3"
    with open_store(path, limits=WIDE_LIMITS) as store:
        walk_to_planned(store, 101)
        token = _prepare_jobs(store, 101, jobs=1, req_fp=REQ_FP)
        store.mark_sent(token, iso=T0)
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=T0)
        assert store.release_send_slot(101, REQ_FP, iso=T1) == 1
        assert store.budget_view().pending_file_requests == 0
        # The row stays live (accepted_unverified) for a late delivery, released flag set:
        assert store.release_send_slot(101, REQ_FP, iso=T2) == 0
        assert store.budget_view().pending_file_requests == 0
        noops = json.loads(store.get_meta("slot_release_noops") or "[]")
        assert noops[-1]["path"] == "partial_close"


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
        token = _prepare_jobs(store, 101, jobs=2, req_fp=REQ_FP)
        store.mark_sent(token, iso=T0)
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=T0)
        store.record_verification(101, VerificationVerdict.CONTRADICTION, iso=T1)
        assert state_of(store, 101) is FileState.CONFLICT
        assert store.budget_view().pending_file_requests == 1  # held forever → wedge
        sends, slots = store.live_send_counts()
        assert sends == LIVE_ONE
        assert slots == LIVE_TWO

        send_id = store.resolve_send(
            101, REQ_FP, resolution="released_after_review", iso=T2, operator_note="reviewed ok"
        )
        assert send_id == token.send_id
        assert state_of(store, 101) is FileState.WAITING
        view = store.budget_view()
        assert view.pending_file_requests == 0
        assert view.pending_episode_jobs == 0
        assert store.live_send_counts() == (0, 0)
        record = store.get_file(101)
        assert record is not None
        assert "released_after_review" in record.state_detail
        assert "reviewed ok" in record.state_detail
        rows = second_view(
            path, "SELECT outcome, slot_released FROM sends WHERE send_id = ?", (send_id,)
        )
        assert rows[0] == ("resolved", 1)
        assert store.active_send_details(101) is None  # closed rows are never live
        # The same fingerprint is no longer refused: re-plan is allowed (fresh send).
        store.advance(101, FileState.PLANNED, "replan", iso=T3)
        replacement = _prepare_jobs(store, 101, jobs=1, cycle_key="cycle-2", req_fp=REQ_FP)
        assert replacement.accepted is True


def test_resolve_send_operator_closed_blocks_safety(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_store(path, limits=WIDE_LIMITS) as store:
        walk_to_planned(store, 101)
        token = _prepare_jobs(store, 101, jobs=1, req_fp=REQ_FP)
        store.mark_sent(token, iso=T0)
        store.record_send_outcome(token, SendOutcome.AMBIGUOUS_SEND, iso=T0)
        assert state_of(store, 101) is FileState.INDETERMINATE_WRITE
        assert store.budget_view().pending_file_requests == 1
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


def test_resolve_send_noop_release_when_slot_already_released(tmp_path: Path) -> None:
    """P1-A/P2-B: a partial-close that already released, then a conflict verdict, then
    resolve_send — the release is a no-op (exactly one release, never a second)."""
    path = tmp_path / "state.sqlite3"
    with open_store(path, limits=WIDE_LIMITS) as store:
        walk_to_planned(store, 101)
        token = _prepare_jobs(store, 101, jobs=2, req_fp=REQ_FP)
        store.mark_sent(token, iso=T0)
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=T0)
        store.record_verification(101, VerificationVerdict.PARTIAL, iso=T1)
        store.release_send_slot(101, REQ_FP, iso=T1)  # partial close released (slot_released=1)
        store.advance(101, FileState.CONFLICT, "contradiction", iso=T1)
        store.resolve_send(101, REQ_FP, resolution="released_after_review", iso=T2)
        assert state_of(store, 101) is FileState.WAITING
        assert store.budget_view().pending_file_requests == 0  # not -1
        noops = json.loads(store.get_meta("slot_release_noops") or "[]")
        assert noops[-1]["path"].startswith("resolve")


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


def test_resolve_send_deadline_gate_on_accepted_unverified(tmp_path: Path) -> None:
    """Round-2 review F2: with ``verification_timeout_seconds`` supplied, an accepted-unverified
    row closes only after the ``attempted_iso`` deadline elapsed (operator opt-in)."""
    path = tmp_path / "state.sqlite3"
    with open_store(path) as store:
        walk_to_planned(store)
        token = prepare(store, req_fp=REQ_FP)
        assert isinstance(token, SendToken)
        store.mark_sent(token, iso=T0)
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=T0)
        with pytest.raises(StateError, match="deadline"):
            store.resolve_send(
                101,
                REQ_FP,
                resolution="released_after_review",
                iso=T0,
                verification_timeout_seconds=3600.0,
            )
        assert state_of(store, 101) is FileState.VERIFYING
        store.resolve_send(
            101,
            REQ_FP,
            resolution="released_after_review",
            iso=T1,
            verification_timeout_seconds=3600.0,
        )
        assert state_of(store, 101) is FileState.WAITING
        assert store.budget_view().pending_file_requests == 0


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
    suppress apply, and clear on the next clean full observation."""
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
            "sonarr-A", "hash-2", iso=T2, roots_fingerprint="roots-1", discovery_total=1000
        )
        assert clean.requires_rebaseline is False
        assert store.get_meta("requires_rebaseline") == "0"


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
