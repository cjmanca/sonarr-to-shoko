"""Offline request ledger history, never Shoko's job or filesystem audit."""

from __future__ import annotations

import sqlite3
from dataclasses import asdict
from pathlib import Path

import pytest
from test_metadata_operations import Server, operation
from test_state import LIMITS, T0, T1, T2, T3, prepare, walk_to_planned

from shoko_linker import state as state_module
from shoko_linker.action_history import RecentAction, action_history_page, action_history_view
from shoko_linker.linking import SendOutcome, VerificationVerdict
from shoko_linker.logging_setup import register_secret
from shoko_linker.state import (
    MetadataOperationState,
    SendToken,
    ServiceFileCounts,
    StateError,
    StateStore,
    open_state,
)


@pytest.mark.parametrize("outcome", list(SendOutcome))
def test_outcome_records_its_own_request_update(tmp_path: Path, outcome: SendOutcome) -> None:
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        walk_to_planned(store)
        token = prepare(store)
        assert isinstance(token, SendToken)
        store.mark_sent(token, iso=T1)
        store.record_send_outcome(token, outcome, iso=T2)
        item = action_history_view(store).examples[0]
        assert item.requested_iso == T1
        assert item.recorded_iso == T2


@pytest.mark.parametrize("verdict", list(VerificationVerdict))
def test_inspection_records_request_event_time(
    tmp_path: Path, verdict: VerificationVerdict
) -> None:
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as store:
        walk_to_planned(store)
        token = prepare(store, target_ids=((18208, 650, 777),))
        store.mark_sent(token, iso=T0)
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=T1)
        inspection = store.active_send_details(101)
        assert inspection is not None
        store.record_verification(101, verdict, iso=T2, inspection=inspection)
        if verdict is VerificationVerdict.CONTRADICTION:
            store.resolve_send(101, token.request_fingerprint, resolution="operator_closed", iso=T3)
        if verdict is VerificationVerdict.VERIFIED:
            store.record_verification(101, verdict, iso=T3)
    with open_state(path) as store:
        item = action_history_page(store).rows[0]
        assert item.requested_iso == T0
        assert item.recorded_iso == (T3 if verdict is VerificationVerdict.CONTRADICTION else T2)


def test_void_new_attempt_and_failed_mutation_keep_separate_request_times(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        walk_to_planned(store)
        first = prepare(store)
        store.void_prepared_send(101, first.request_fingerprint, iso=T1, reason="cancel")
        second = prepare(store, iso=T2, cycle_key="next")
        assert isinstance(second, SendToken)
        before = action_history_view(store)
        assert {r.action_id: r.recorded_iso for r in before.examples} == {
            f"file:{first.send_id}": T1,
            f"file:{second.send_id}": T2,
        }
        with pytest.raises(StateError):
            store.resolve_send(
                101, second.request_fingerprint, resolution="operator_closed", iso=T3
            )
        with pytest.raises(StateError):
            store.mark_sent(second, iso="invalid")
        assert action_history_view(store) == before


def test_v10_upgrade_retains_only_request_times_and_preserves_ledger(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as store:
        walk_to_planned(store)
        token = prepare(store)
        store.mark_sent(token, iso=T0)
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=T1)
        store.connection.execute("ALTER TABLE sends DROP COLUMN updated_iso")
        store.connection.execute(
            "UPDATE sends SET verified_iso=?,campaign_ownership_resolved_iso=?", (T1, T2)
        )
        store.connection.execute("UPDATE files SET updated_iso=?", (T3,))
        store.connection.execute(
            "INSERT INTO sends (shoko_file_id,request_fingerprint,plan_fingerprint,"
            "attempted_iso,outcome) VALUES (101,'unknown','unknown','invalid','resolved')"
        )
        before = store.connection.execute("SELECT * FROM sends").fetchall()
        budget = store.budget_view()
        reservations = store.connection.execute("SELECT * FROM budget_reservations").fetchall()
        store.connection.execute("DELETE FROM schema_version WHERE version>=11")
        store.connection.execute("PRAGMA user_version=10")
    with open_state(path, budget_limits=LIMITS) as store:
        rows = store.connection.execute("SELECT * FROM sends").fetchall()
        assert [row[:-1] for row in rows] == before
        assert [row[-1] for row in rows] == [T2, None]
        assert store.budget_view() == budget
        assert (
            store.connection.execute("SELECT * FROM budget_reservations").fetchall() == reservations
        )
        assert store.connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert {row.action_id: row.recorded_iso for row in action_history_view(store).examples} == {
            "file:1": T2,
            "file:2": "",
        }
        # Ordinary ownership resolution is also a request-local event.
        store.connection.execute("UPDATE sends SET campaign_ownership='unknown' WHERE send_id=1")
        store.resolve_legacy_send_ownership(1, campaign_id=None, note="reviewed", iso=T3)
        assert action_history_view(store).examples[0].recorded_iso == T3


def test_v10_timestamp_migration_failure_rolls_back_added_column_and_events(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as store:
        walk_to_planned(store)
        prepare(store)
        store.connection.execute("ALTER TABLE sends DROP COLUMN updated_iso")
        store.connection.execute("DELETE FROM schema_version WHERE version>=11")
        store.connection.execute("PRAGMA user_version=10")
        before = store.connection.execute("SELECT * FROM sends").fetchall()

    real_step = state_module._MIGRATIONS[10]

    def fail_after_backfill(connection: sqlite3.Connection) -> None:
        real_step(connection)
        raise sqlite3.OperationalError("injected timestamp migration failure")

    monkeypatch.setattr(state_module, "_MIGRATIONS", {10: fail_after_backfill})
    with pytest.raises(StateError, match="migration from schema version 10 failed"):
        StateStore(path)
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (10,)
        assert connection.execute("SELECT * FROM sends").fetchall() == before
        assert "updated_iso" not in {
            row[1] for row in connection.execute("PRAGMA table_info(sends)")
        }
        assert connection.execute("SELECT version FROM schema_version").fetchall() == []


@pytest.mark.parametrize(
    ("attempted", "verified", "resolved", "expected"),
    [
        (T0, "20260102T000000+00:00", None, "20260102T000000+00:00"),
        ("20260101T000000+00:00", T2, None, T2),
        (T0, "2026-01-01T01:00:00+02:00", None, T0),
        (T0, "2026-01-01T02:00:00+02:00", None, T0),
        (T0, "2026-01-03T00:00:00", None, T0),
        (T0, "0001-01-01T00:00:00+01:00", None, T0),
        (T0, "9999-12-31T23:59:59-01:00", None, T0),
        (T0, "invalid", b"2026-01-03T00:00:00+00:00", T0),
        (T0, 20260103, "9" * 200, T0),
        ("invalid", None, None, None),
        ("2026-01-01T00:00:00", b"not text", 20260103, None),
    ],
)
def test_v10_backfill_uses_displayable_aware_python_iso_times(
    tmp_path: Path,
    attempted: str,
    verified: str | bytes | int | None,
    resolved: str | bytes | int | None,
    expected: str | None,
) -> None:
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as store:
        walk_to_planned(store)
        prepare(store)
        store.connection.execute("ALTER TABLE sends DROP COLUMN updated_iso")
        store.connection.execute(
            "UPDATE sends SET attempted_iso=?,verified_iso=?,campaign_ownership_resolved_iso=?",
            (attempted, verified, resolved),
        )
        store.connection.execute("DELETE FROM schema_version WHERE version>=11")
        store.connection.execute("PRAGMA user_version=10")
    with open_state(path) as store:
        assert store.connection.execute("SELECT updated_iso FROM sends").fetchone() == (expected,)


@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        ("prepared", "not sent"),
        ("attempted", "may have been sent"),
        ("accepted_unverified", "accepted"),
        ("rejected", "rejected"),
        ("ambiguous_send", "uncertain"),
        ("verified", "were verified"),
    ],
)
def test_file_delivery_and_verified_results_survive_restart(
    tmp_path: Path, outcome: str, expected: str
) -> None:
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as store:
        walk_to_planned(store)
        token = prepare(store, target_ids=((18208, 650, 777),))
        assert isinstance(token, SendToken)
        if outcome != "prepared":
            store.mark_sent(token, iso=T1)
            if outcome != "attempted":
                result = "accepted_unverified" if outcome == "verified" else outcome
                store.record_send_outcome(token, SendOutcome(result), iso=T1, http_status=200)
            if outcome == "verified":
                store.record_verification(101, VerificationVerdict.VERIFIED, iso=T2)
    with open_state(path, budget_limits=LIMITS) as store:
        before = store._conn.total_changes
        view = action_history_view(store)
        assert view.total == 1
        assert len(view.examples) == 1
        item = view.examples[0]
        assert expected in item.message
        assert item.state == outcome
        assert item.filename == "s01e01.mkv"
        assert item.current_path_complete
        assert item.recorded_iso == (
            T2 if outcome == "verified" else T0 if outcome == "prepared" else T1
        )
        assert "across all" in view.scope_message
        assert "current retained context" in view.scope_message
        assert store._conn.total_changes == before


@pytest.mark.parametrize(
    ("boundary", "expected"),
    [
        ("prepared", "not sent"),
        ("marked", "uncertain"),
        ("accepted", "accepted"),
        ("attention", "needs attention"),
        ("ready", "does not prove"),
    ],
)
def test_metadata_requests_not_external_observations(
    tmp_path: Path, boundary: str, expected: str
) -> None:
    server = Server()
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        row = operation(store, server.endpoint, "accepted" if boundary == "ready" else boundary)
        operation(store, server.endpoint, "external", anime=1)
        if boundary == "ready":
            store.record_metadata_operation(
                row,
                MetadataOperationState.READY,
                iso=T1,
                verified_required_targets=row.required_targets,
            )
        view = action_history_view(store)
        assert view.total == 1
        assert expected in view.examples[0].message
        assert view.examples[0].subject_id == 18208
        assert view.examples[0].filename == ""
        assert not view.examples[0].current_path_complete
        assert not server.requests


def test_bound_count_source_honesty_archived_generations_and_malformed_results(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as store:
        walk_to_planned(store)
        token = prepare(store)
        assert isinstance(token, SendToken)
    # Persisted fixtures include an old endpoint and archived metadata generation.
    # Nothing in the view implies these belong to today's configured endpoint.
    with sqlite3.connect(path) as connection:
        for index in range(30):
            connection.execute(
                "INSERT INTO sends (shoko_file_id,request_fingerprint,plan_fingerprint,"
                "attempted_iso,outcome) VALUES (101,?,?,?,?)",
                (str(index), "b" * 64, T1, "unrecognized result"),
            )
        connection.execute("UPDATE sends SET attempted_iso='invalid' WHERE send_id=1")
    with open_state(path, budget_limits=LIMITS) as store:
        first = operation(store, Server().endpoint, "accepted")
        store.record_metadata_operation(
            first,
            MetadataOperationState.READY,
            iso=T2,
            verified_required_targets=first.required_targets,
        )
        # Archive exactly as the supported new-target generation mechanism does.
        with sqlite3.connect(path) as connection:
            connection.execute(
                "INSERT INTO metadata_operation_history SELECT * FROM metadata_operations"
            )
            connection.execute("DELETE FROM metadata_operations")
        statements: list[str] = []
        store._conn.set_trace_callback(statements.append)
        view = action_history_view(store)
        assert view.total == 32
        assert len(view.examples) == 20
        assert view.examples[0].operation == "metadata_refresh"
        assert "does not prove" in view.examples[0].message
        assert "unknown" in view.examples[1].message
        assert all(item.recorded_iso for item in view.examples)
        assert any("LIMIT 20" in statement for statement in statements)
        assert len(statements) == 1  # single read: no network/state writes/JSON decoding


def test_sanitized_paths_invalid_time_and_empty_history(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as store:
        assert action_history_view(store).total == 0
        assert action_history_view(store).examples == ()
        walk_to_planned(store)
        prepare(store)
    secret = "history-fixture-secret"
    register_secret(secret)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE files SET sonarr_path=?",
            (f"/media/<script>/{secret}/http://host/unregistered-private-key.mkv",),
        )
        connection.execute("UPDATE sends SET attempted_iso='not a date',updated_iso='not a date'")
    with open_state(path, budget_limits=LIMITS) as store:
        view = action_history_view(store)
        item = view.examples[0]
        assert item.requested_iso == ""
        assert item.recorded_iso == ""
        assert secret not in str(asdict(view))
        assert "http://" not in str(asdict(view))
        assert "unregistered-private-key" not in str(asdict(view))
        assert "\x00" not in item.current_path
        assert "[URL omitted]" in item.current_path
        assert not item.current_path_complete
        assert action_history_page(store).rows == view.examples


def test_direct_history_row_does_not_claim_path_proof() -> None:
    row = RecentAction(
        "file:1",
        "file_link",
        1,
        T0,
        T0,
        "prepared",
        "Prepared locally; not sent.",
        current_path="/anime/Show/Episode.mkv",
    )
    assert not row.current_path_complete


def test_only_twenty_selected_paths_are_hydrated_from_large_history(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    recent_path = "/media/" + "r" * 8000 + ".mkv"
    old_path = "/media/old-history-path-must-not-be-hydrated.mkv"
    with open_state(path, budget_limits=LIMITS) as store:
        walk_to_planned(store)
        prepare(store)
    with sqlite3.connect(path) as connection:
        connection.execute("UPDATE files SET sonarr_path=?", (recent_path,))
        connection.execute(
            "INSERT INTO files (shoko_file_id,state,cohort,updated_iso,first_seen_iso,sonarr_path)"
            " VALUES (102,'waiting','new_arrival',?,?,?)",
            (T0, T0, old_path),
        )
        connection.executemany(
            "INSERT INTO sends (shoko_file_id,request_fingerprint,plan_fingerprint,"
            "attempted_iso,outcome) VALUES (?,?,?,?,?)",
            [(101, str(index), "a" * 64, T1, "attempted") for index in range(1000)]
            + [(102, str(index), "b" * 64, T0, "attempted") for index in range(100)],
        )
    with open_state(path, budget_limits=LIMITS) as store:
        evaluated_paths: list[str] = []

        def measured_length(value: str | bytes | None) -> int | None:
            assert value != old_path, "a path outside the selected 20 was hydrated"
            if value == recent_path:
                evaluated_paths.append(recent_path)
            return None if value is None else len(value)

        store._conn.create_function("length", 1, measured_length)
        statements: list[str] = []
        store._conn.set_trace_callback(statements.append)
        before = store._conn.total_changes
        view = action_history_view(store)
        assert view.total == 1101
        assert len(view.examples) == 20
        assert len(evaluated_paths) == 20
        assert {item.subject_id for item in view.examples} == {101}
        assert all(item.current_path == recent_path for item in view.examples)
        assert len(statements) == 1
        assert store._conn.total_changes == before
        plan = store._conn.execute("EXPLAIN QUERY PLAN " + statements[0]).fetchall()
        assert any("MATERIALIZE recent" in row[3] for row in plan)
        assert any("SEARCH f USING INTEGER PRIMARY KEY" in row[3] for row in plan)


def test_metadata_mutable_text_is_bounded_for_current_and_archived_rows(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    server = Server()
    with open_state(path, budget_limits=LIMITS) as store:
        first = operation(store, server.endpoint, "accepted")
        store.record_metadata_operation(
            first,
            MetadataOperationState.READY,
            iso=T1,
            verified_required_targets=first.required_targets,
        )
        operation(store, server.endpoint, "accepted", anime=1)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO metadata_operation_history SELECT * FROM metadata_operations"
            " WHERE anidb_anime_id=18208"
        )
        connection.execute("DELETE FROM metadata_operations WHERE anidb_anime_id=18208")
        for table in ("metadata_operations", "metadata_operation_history"):
            connection.execute(
                f"UPDATE {table} SET claim=?,attempted_iso=?,updated_iso=?",  # noqa: S608
                ("c" * 10000, "t" * 10000, "u" * 10000),
            )
    with open_state(path, budget_limits=LIMITS) as store:
        view = action_history_view(store)
        assert view.total == 2
        assert len(view.examples) == 2
        for item in view.examples:
            assert len(item.action_id) < 128
            assert item.requested_iso == ""
            assert item.recorded_iso == ""
        assert not server.requests


def test_history_pages_select_keys_before_payload_and_preserve_total(tmp_path: Path) -> None:
    path = tmp_path / "page.sqlite3"
    with open_state(path, budget_limits=LIMITS) as store:
        walk_to_planned(store)
        prepare(store)
        store.connection.executemany(
            "INSERT INTO sends (shoko_file_id,request_fingerprint,plan_fingerprint,"
            "attempted_iso,outcome) VALUES (101,?,?,?,'attempted')",
            [(str(i), "b" * 64, T1) for i in range(206)],
        )
        # External observations do not enlarge request history; own archived rows do.
        row = operation(store, Server().endpoint, "accepted")
        store.record_metadata_operation(
            row,
            MetadataOperationState.READY,
            iso=T2,
            verified_required_targets=row.required_targets,
        )
        store.connection.execute(
            "INSERT INTO metadata_operation_history SELECT * FROM metadata_operations"
        )
        store.connection.execute("DELETE FROM metadata_operations")
        operation(store, Server().endpoint, "external", anime=1)
        pages = [action_history_page(store, page=p, page_size=100) for p in range(1, 4)]
        assert [len(p.rows) for p in pages] == [100, 100, 8]
        ids = [r.action_id for p in pages for r in p.rows]
        assert len(set(ids)) == len(ids) == 208
        assert pages[0].rows[0].operation == "metadata_refresh"
        assert "does not prove" in pages[0].rows[0].message
        assert pages[2].pagination.total == 208
        assert pages[2].pagination.end == 208
        assert action_history_page(store, page=999, page_size=100).rows == pages[2].rows
        statements: list[str] = []
        paths: list[str] = []

        def measured_length(value: str | bytes | None) -> int | None:
            if value == "/data/media/anime/show/s01e01.mkv":
                paths.append(str(value))
            return None if value is None else len(value)

        store.connection.create_function("length", 1, measured_length)
        store.connection.set_trace_callback(statements.append)
        before = store.connection.total_changes
        page = action_history_page(store, page=2, page_size=50)
        assert len(page.rows) == 50
        assert len(paths) == 50
        query = next(s for s in statements if "WITH keys" in s)
        assert "LIMIT 50 OFFSET 50" in query
        assert "length(s.target_ids_json)<=65536" in query.lower()
        assert store.connection.total_changes == before
        assert not store.connection.in_transaction
        plan = store.connection.execute("EXPLAIN QUERY PLAN " + query).fetchall()
        assert any("MATERIALIZE recent" in row[3] for row in plan)
    with open_state(path, budget_limits=LIMITS) as store:
        assert action_history_page(store, page=2, page_size=50) == page


def test_history_page_and_header_share_worker_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "page.sqlite3"
    with open_state(path, budget_limits=LIMITS) as store, open_state(path) as worker:
        walk_to_planned(store)
        prepare(store)
        original = store.service_file_counts

        def concurrent_change() -> ServiceFileCounts:
            counts = original()
            worker.connection.execute("DELETE FROM sends")
            worker.connection.execute("UPDATE files SET state='verified'")
            return counts

        monkeypatch.setattr(store, "service_file_counts", concurrent_change)
        result = action_history_page(store)
        assert result.pagination.total == 1
        assert len(result.rows) == 1
        assert result.counts.waiting_files == 1
        assert result.counts.verified_files == 0
        assert action_history_view(store).total == 0
        assert original().verified_files == 1


@pytest.mark.parametrize("source", ["/inside/\x00" + "x" * 20000, b"/inside/E.mkv"])
def test_history_rejects_nul_hidden_or_nontext_path_before_hydration(
    tmp_path: Path, source: str | bytes
) -> None:
    with open_state(tmp_path / "state.sqlite3", budget_limits=LIMITS) as store:
        walk_to_planned(store)
        prepare(store)
        store.connection.execute("UPDATE files SET sonarr_path=?", (source,))
        # Inspect the SQL result before any Python display sanitizer runs.
        total, raw = store.recent_action_rows()
        assert total == 1
        assert raw[0]["current_path"] is None
        paged_raw = store.action_rows_page()
        assert paged_raw.rows[0]["current_path"] is None
        for item in (action_history_view(store).examples[0], action_history_page(store).rows[0]):
            assert item.current_path == ""
            assert item.current_path_complete is False
        for row in (
            store.service_file_status().waiting.examples[0],
            store.service_file_page("waiting").rows[0],
        ):
            assert row.sonarr_path == ""
            assert row.display_paths_complete is False
