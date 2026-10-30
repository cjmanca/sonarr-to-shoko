"""Anime-operation durability and aggregate budget ownership; offline SQLite only."""

from __future__ import annotations

import sqlite3
from dataclasses import replace
from pathlib import Path
from threading import Barrier, Thread
from typing import cast

import pytest
from test_state import LIMITS, T0, T1, T2, prepare, walk_to_planned

from shoko_linker.mappings import EpisodeType
from shoko_linker.state import (
    SCHEMA_VERSION,
    BudgetLimits,
    BudgetRefusal,
    MetadataEpisodeIdentity,
    MetadataOperation,
    StateError,
    StateStore,
    WriteDisabledError,
    open_state,
)
from shoko_linker.state import (
    MetadataOperationState as M,
)

SOURCE = "a" * 64
DEMAND = "b" * 64
REQUEST = "c" * 64


def prepare_anime(
    store: StateStore,
    anime: int = 18208,
    *,
    retry_unsent_claim: str | None = None,
    adopt_external_claim: str | None = None,
) -> MetadataOperation:
    result = store.prepare_metadata_operation(
        SOURCE,
        anime,
        demand_fingerprint=DEMAND,
        request_fingerprint=REQUEST,
        request_version=1,
        cycle_key="cycle",
        iso=T0,
        retry_unsent_claim=retry_unsent_claim,
        adopt_external_claim=adopt_external_claim,
    )
    assert isinstance(result, MetadataOperation)
    return result


def test_v7_migration_preserves_history_and_fresh_database(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as store:
        walk_to_planned(store)
        send = prepare(store)
        assert send.accepted
        before = store.connection.execute("SELECT * FROM sends").fetchall()
        store.connection.execute("DROP TABLE latest_scan_exclusions")
        store.connection.execute("DROP TABLE metadata_operation_history")
        store.connection.execute("DROP TABLE metadata_operations")
        store.connection.execute("PRAGMA user_version=7")
    with open_state(path, budget_limits=LIMITS) as store:
        assert store.schema_version() == SCHEMA_VERSION
        assert store.connection.execute("SELECT * FROM sends").fetchall() == before
        assert store.metadata_operations(SOURCE) == ()
        assert store.live_send_counts() == (1, 1)
        prepare_anime(store)


def test_two_handles_claim_once_and_consent_never_creates_another(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    barrier = Barrier(2)
    results: list[MetadataOperation | BudgetRefusal] = []
    errors: list[Exception] = []

    def worker() -> None:
        try:
            with open_state(path, budget_limits=LIMITS) as store:
                barrier.wait(timeout=10)
                results.append(
                    store.prepare_metadata_operation(
                        SOURCE,
                        18208,
                        demand_fingerprint=DEMAND,
                        request_fingerprint=REQUEST,
                        request_version=1,
                        cycle_key="cycle",
                        iso=T0,
                    )
                )
        except Exception as exc:
            errors.append(exc)

    threads = [Thread(target=worker), Thread(target=worker)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=20)
    assert not errors
    assert len(results) == 2
    assert sum(isinstance(result, MetadataOperation) for result in results) == 1
    with open_state(path, budget_limits=LIMITS) as store:
        result = store.prepare_metadata_operation(
            SOURCE,
            18208,
            demand_fingerprint="d" * 64,
            request_fingerprint="e" * 64,
            request_version=2,
            cycle_key="new-consent",
            iso=T1,
        )
        assert isinstance(result, BudgetRefusal)
        assert store.budget_view().pending_file_requests == 1
        assert store.live_send_counts() == (0, 0)
        assert store.connection.execute("SELECT COUNT(*) FROM sends").fetchone()[0] == 0
        assert store.budget_view().completed_submissions == 0


@pytest.mark.parametrize(
    "boundary", ["prepared", "marked", "admitted", "accepted", "attention", "ready"]
)
def test_restart_preserves_identity_without_replay(tmp_path: Path, boundary: str) -> None:
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as store:
        operation = prepare_anime(store)
        if boundary != "prepared":
            operation = store.mark_metadata_possibly_delivered(
                operation,
                iso=T0,
                verification_deadline_iso=T1,
            )
        if boundary in {"admitted", "accepted", "attention", "ready"}:
            store.admit_metadata_transport(operation)
        if boundary in {"accepted", "attention", "ready"}:
            operation = store.record_metadata_operation(operation, M.ACCEPTED_WAITING, iso=T0)
        if boundary in {"attention", "ready"}:
            operation = store.record_metadata_operation(operation, M.NEEDS_ATTENTION, iso=T1)
        if boundary == "ready":
            operation = store.record_metadata_operation(operation, M.READY, iso=T2)
    with open_state(path, budget_limits=LIMITS) as store:
        current = store.get_metadata_operation(SOURCE, 18208)
        assert current == operation or (current is not None and current.transport_admitted)
        assert current is not None
        assert current.verification_deadline_iso == (None if boundary == "prepared" else T1)
        duplicate = store.prepare_metadata_operation(
            SOURCE,
            18208,
            demand_fingerprint=DEMAND,
            request_fingerprint=REQUEST,
            request_version=1,
            cycle_key="restarted",
            iso=T2,
            retry_unsent_claim=current.claim,
            adopt_external_claim=current.claim,
        )
        assert isinstance(duplicate, BudgetRefusal)
        assert store.budget_view().pending_file_requests == (0 if boundary == "ready" else 1)
        with pytest.raises(StateError):
            store.admit_metadata_transport(current)


def test_unsent_cancel_retry_stale_sender_and_release_exactly_once(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        first = prepare_anime(store)
        store.record_metadata_operation(first, M.DEFINITELY_REJECTED, iso=T0)
        store.record_metadata_operation(first, M.DEFINITELY_REJECTED, iso=T0)
        assert store.budget_view().pending_file_requests == 0
        second = prepare_anime(store, retry_unsent_claim=first.claim)
        assert second.claim != first.claim
        with pytest.raises(StateError, match="stale"):
            store.mark_metadata_possibly_delivered(first, iso=T0, verification_deadline_iso=T1)
        with pytest.raises(StateError, match="stale"):
            store.record_metadata_operation(first, M.READY, iso=T1)
        assert store.budget_view().pending_file_requests == 1
        store.record_metadata_operation(second, M.READY, iso=T1)
        store.record_metadata_operation(second, M.READY, iso=T2)
        assert store.budget_view().pending_file_requests == 0
        assert store.budget_view().rolling_new_submissions == 2


def test_transport_admission_is_one_shot_and_obeys_other_handle_stop(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as first, open_state(path) as second:
        operation = prepare_anime(first)
        with pytest.raises(StateError):
            second.admit_metadata_transport(operation)
        marked = first.mark_metadata_possibly_delivered(
            operation, iso=T0, verification_deadline_iso=T1
        )
        with pytest.raises(StateError):
            second.admit_metadata_transport(marked)
        first.admit_metadata_transport(marked)
        with pytest.raises(StateError):
            first.admit_metadata_transport(marked)
        second.disable_writes("synthetic disk stop")
        with pytest.raises(WriteDisabledError):
            first.record_metadata_operation(marked, M.READY, iso=T1)
    with open_state(path) as reopened:
        assert reopened.writes_disabled
        restored = reopened.get_metadata_operation(SOURCE, 18208)
        assert restored is not None
        assert restored.state == M.POSSIBLY_DELIVERED


def test_external_work_no_budget_and_explicit_first_own_request_only(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        external = store.observe_external_metadata_operation(
            SOURCE,
            18208,
            demand_fingerprint=DEMAND,
            request_fingerprint=REQUEST,
            request_version=1,
            iso=T0,
        )
        assert not external.reservation_owned
        assert external.attempted_iso is None
        assert store.budget_view().pending_file_requests == 0
        own = prepare_anime(store, adopt_external_claim=external.claim)
        store.mark_metadata_possibly_delivered(own, iso=T0, verification_deadline_iso=T1)
        observed = store.observe_external_metadata_operation(
            SOURCE,
            18208,
            demand_fingerprint=DEMAND,
            request_fingerprint=REQUEST,
            request_version=1,
            iso=T1,
        )
        assert observed.state == M.POSSIBLY_DELIVERED
        assert observed.claim == own.claim
        with pytest.raises(StateError, match="stale"):
            store.record_metadata_operation(external, M.READY, iso=T1)


@pytest.mark.parametrize(
    ("limit", "reason"),
    [
        (replace(LIMITS, max_pending_files=1), "pending_files_exhausted"),
        (replace(LIMITS, max_pending_episode_jobs=1), "pending_episode_jobs_exhausted"),
        (replace(LIMITS, per_cycle_new=1), "cycle_new_exhausted"),
        (replace(LIMITS, rolling_new=1), "rolling_new_exhausted"),
        (replace(LIMITS, additional_rolling_limits=((1, 168),)), "rolling_new_exhausted"),
    ],
)
@pytest.mark.parametrize("metadata_first", [True, False])
def test_file_and_metadata_share_all_caps(
    tmp_path: Path,
    limit: BudgetLimits,
    reason: str,
    metadata_first: bool,
) -> None:
    with open_state(tmp_path / "state.db", budget_limits=limit) as store:
        walk_to_planned(store)
        if metadata_first:
            prepare_anime(store)
            result = prepare(store, cycle_key="cycle")
        else:
            assert prepare(store, cycle_key="cycle").accepted
            result = store.prepare_metadata_operation(
                SOURCE,
                18208,
                demand_fingerprint=DEMAND,
                request_fingerprint=REQUEST,
                request_version=1,
                cycle_key="cycle",
                iso=T0,
            )
        assert isinstance(result, BudgetRefusal)
        assert result.reason == reason


def test_anonymous_release_cannot_free_metadata_or_file_owned_slots(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        operation = prepare_anime(store)
        with pytest.raises(StateError, match="release_slot refused"):
            store.release_slot("cycle", T0)
        reservation = store.try_reserve_slot("cycle", T0)
        assert reservation.accepted
        store.release_slot("cycle", T0)
        assert store.budget_view().pending_file_requests == 1
        store.record_metadata_operation(operation, M.NEEDS_ATTENTION, iso=T1)
        with pytest.raises(StateError):
            store.release_slot("cycle", T1)


@pytest.mark.parametrize("step", ["prepare", "mark", "admit", "release"])
def test_commit_failure_rolls_back_and_persists_stop(tmp_path: Path, step: str) -> None:
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as store:
        operation = None if step == "prepare" else prepare_anime(store)
        if step == "admit":
            assert operation is not None
            operation = store.mark_metadata_possibly_delivered(
                operation, iso=T0, verification_deadline_iso=T1
            )
        before = store.get_metadata_operation(SOURCE, 18208)
        pending = store.budget_view().pending_file_requests
        denied = False

        def authorizer(action: int, arg: str | None, *_: object) -> int:
            nonlocal denied
            if action == sqlite3.SQLITE_TRANSACTION and arg == "COMMIT" and not denied:
                denied = True
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        store.connection.set_authorizer(authorizer)

        def mutate() -> None:
            if step == "prepare":
                prepare_anime(store)
            elif step == "mark":
                assert operation is not None
                store.mark_metadata_possibly_delivered(
                    operation, iso=T0, verification_deadline_iso=T1
                )
            elif step == "admit":
                assert operation is not None
                store.admit_metadata_transport(operation)
            else:
                assert operation is not None
                store.record_metadata_operation(operation, M.READY, iso=T1)

        with pytest.raises(WriteDisabledError):
            mutate()
        store.connection.set_authorizer(None)
        assert store.get_metadata_operation(SOURCE, 18208) == before
        assert store.budget_view().pending_file_requests == pending
    with open_state(path) as reopened:
        assert reopened.writes_disabled


@pytest.mark.parametrize("anime", [0, -1, True, 1.5, "18208"])
def test_identity_refuses_invalid_anime_ids(tmp_path: Path, anime: object) -> None:
    with (
        open_state(tmp_path / "state.db", budget_limits=LIMITS) as store,
        pytest.raises(StateError, match="positive integer"),
    ):
        prepare_anime(store, anime)  # type: ignore[arg-type]


@pytest.mark.parametrize("step", ["prepare", "mark", "admit"])
def test_consent_guard_is_fresh_at_each_boundary(tmp_path: Path, step: str) -> None:
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as store, open_state(path) as other:
        store.save_onboarding_json('{"revision":1}', T0)
        operation = None if step == "prepare" else prepare_anime(store)
        if step == "admit":
            assert operation is not None
            operation = store.mark_metadata_possibly_delivered(
                operation,
                iso=T0,
                verification_deadline_iso=T1,
                expected_document_json='{"revision":1}',
            )
        other.save_onboarding_json('{"revision":2}', T1)

        def stale_action() -> None:
            if step == "prepare":
                store.prepare_metadata_operation(
                    SOURCE,
                    18208,
                    demand_fingerprint=DEMAND,
                    request_fingerprint=REQUEST,
                    request_version=1,
                    cycle_key="cycle",
                    iso=T0,
                    expected_document_json='{"revision":1}',
                )
            elif step == "mark":
                assert operation is not None
                store.mark_metadata_possibly_delivered(
                    operation,
                    iso=T0,
                    verification_deadline_iso=T1,
                    expected_document_json='{"revision":1}',
                )
            else:
                assert operation is not None
                store.admit_metadata_transport(operation, expected_document_json='{"revision":1}')

        with pytest.raises(StateError, match="authorization"):
            stale_action()
        other.set_service_pause(True, "user", T1)
        with pytest.raises(StateError, match="authorization"):
            store.prepare_metadata_operation(
                SOURCE,
                100,
                demand_fingerprint=DEMAND,
                request_fingerprint=REQUEST,
                request_version=1,
                cycle_key="cycle",
                iso=T0,
                expected_document_json='{"revision":2}',
            )


def test_stop_only_cleanup_releases_prepared_but_not_possible_delivery(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as store:
        prepared = prepare_anime(store)
        marked = prepare_anime(store, 18209)
        store.mark_metadata_possibly_delivered(marked, iso=T0, verification_deadline_iso=T1)
        with pytest.raises(StateError, match="latched write stop"):
            store.void_prepared_metadata_after_write_stop(prepared, iso=T1)
        store.disable_writes("synthetic stop before HTTP")
        store.void_prepared_metadata_after_write_stop(prepared, iso=T1)
        with pytest.raises(StateError, match="possibly delivered"):
            store.void_prepared_metadata_after_write_stop(marked, iso=T1)
        with pytest.raises(StateError):
            store.void_prepared_metadata_after_write_stop(prepared, iso=T1)
        assert store.budget_view().pending_file_requests == 1
        assert store.writes_disabled
    with open_state(path) as reopened:
        assert reopened.writes_disabled
        assert reopened.budget_view().pending_file_requests == 1


def test_only_proven_rejection_can_retry_after_transport(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        operation = prepare_anime(store)
        marked = store.mark_metadata_possibly_delivered(
            operation, iso=T0, verification_deadline_iso=T1
        )
        store.admit_metadata_transport(marked)
        rejected = store.record_metadata_operation(marked, M.DEFINITELY_REJECTED, iso=T0)
        replacement = prepare_anime(store, retry_unsent_claim=rejected.claim)
        marked = store.mark_metadata_possibly_delivered(
            replacement, iso=T0, verification_deadline_iso=T1
        )
        store.admit_metadata_transport(marked)
        accepted = store.record_metadata_operation(marked, M.ACCEPTED_WAITING, iso=T0)
        with pytest.raises(StateError, match="invalid"):
            store.record_metadata_operation(accepted, M.DEFINITELY_REJECTED, iso=T1)
        store.record_metadata_operation(accepted, M.NEEDS_ATTENTION, iso=T1)
        assert store.budget_view().pending_file_requests == 1


def test_v8_migration_failure_keeps_prior_history_intact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as store:
        store.set_meta("historical", "preserve")
        store.connection.execute("DROP TABLE latest_scan_exclusions")
        store.connection.execute("DROP TABLE metadata_operation_history")
        store.connection.execute("DROP TABLE metadata_operations")
        store.connection.execute("PRAGMA user_version=7")
    real_connect = sqlite3.connect

    def connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        connection = cast("sqlite3.Connection", real_connect(*args, **kwargs))  # type: ignore[call-overload]
        connection.set_authorizer(
            lambda action, table, *_: sqlite3.SQLITE_DENY
            if action == sqlite3.SQLITE_CREATE_TABLE and table == "metadata_operations"
            else sqlite3.SQLITE_OK
        )
        return connection

    monkeypatch.setattr(sqlite3, "connect", connect)
    with (
        pytest.raises(StateError, match="migration from schema version 7 failed"),
        open_state(path),
    ):
        pass
    with real_connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (7,)
        assert connection.execute("SELECT value FROM meta WHERE key='historical'").fetchone() == (
            "preserve",
        )
        assert (
            connection.execute(
                "SELECT name FROM sqlite_master WHERE name='metadata_operations'"
            ).fetchall()
            == []
        )


E1: MetadataEpisodeIdentity = (18208, EpisodeType.EPISODE, 1)
E2: MetadataEpisodeIdentity = (18208, EpisodeType.EPISODE, 2)
E3: MetadataEpisodeIdentity = (18208, EpisodeType.SPECIAL, 1)


def prepare_episode(
    store: StateStore,
    targets: tuple[MetadataEpisodeIdentity, ...],
    *,
    previous: MetadataOperation | None = None,
    demand: str = DEMAND,
    expected_document_json: str | None = None,
) -> MetadataOperation | BudgetRefusal:
    return store.prepare_metadata_operation(
        SOURCE,
        18208,
        demand_fingerprint=demand,
        request_fingerprint=REQUEST,
        request_version=1,
        cycle_key="cycle",
        iso=T0,
        required_targets=targets,
        missing_targets=targets,
        after_ready_claim=None if previous is None else previous.claim,
        expected_document_json=expected_document_json,
    )


def finish_episode(store: StateStore, operation: MetadataOperation) -> MetadataOperation:
    marked = store.mark_metadata_possibly_delivered(operation, iso=T0, verification_deadline_iso=T1)
    store.admit_metadata_transport(marked)
    accepted = store.record_metadata_operation(marked, M.ACCEPTED_WAITING, iso=T0)
    return store.record_metadata_operation(
        accepted,
        M.READY,
        iso=T1,
        verified_required_targets=operation.required_targets,
    )


def test_ready_new_episode_rearms_but_file_or_consent_change_does_not(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        first = prepare_episode(store, (E1,))
        assert isinstance(first, MetadataOperation)
        ready = finish_episode(store, first)
        snapshot = store.connection.execute("SELECT * FROM metadata_operations").fetchone()
        same = prepare_episode(store, (E1,), previous=ready, demand="d" * 64)
        assert isinstance(same, BudgetRefusal)
        second = prepare_episode(store, (E2,), previous=ready)
        assert isinstance(second, MetadataOperation)
        assert second.claim != ready.claim
        assert store.budget_view().pending_file_requests == 1
        assert store.budget_view().rolling_new_submissions == 2
        assert store.live_send_counts() == (0, 0)
        assert (
            store.connection.execute("SELECT * FROM metadata_operation_history").fetchone()
            == snapshot
        )
        assert snapshot[11] == 1  # Original admitted transport marker retained.
        with pytest.raises(StateError, match="stale"):
            store.record_metadata_operation(ready, M.READY, iso=T2)
        with pytest.raises(StateError):
            store.admit_metadata_transport(ready)
        assert store.budget_view().pending_file_requests == 1


def test_accumulated_ready_coverage_survives_restart_and_cannot_alternate(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as store:
        first = prepare_episode(store, (E1,))
        assert isinstance(first, MetadataOperation)
        first = finish_episode(store, first)
        second = prepare_episode(store, (E2,), previous=first)
        assert isinstance(second, MetadataOperation)
        second = finish_episode(store, second)
    with open_state(path, budget_limits=replace(LIMITS, per_cycle_new=3)) as store:
        again = prepare_episode(store, (E1,), previous=second, demand="d" * 64)
        assert isinstance(again, BudgetRefusal)
        third = prepare_episode(store, (E3,), previous=second)
        assert isinstance(third, MetadataOperation)
        assert store.connection.execute(
            "SELECT COUNT(*) FROM metadata_operation_history"
        ).fetchone() == (2,)
        assert store.budget_view().rolling_new_submissions == 3
        assert store.budget_view().pending_file_requests == 1
        with pytest.raises(StateError, match="release_slot refused"):
            store.release_slot("cycle", T1)


def test_two_handles_can_rearm_only_one_exact_ready_generation(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as store:
        operation = prepare_episode(store, (E1,))
        assert isinstance(operation, MetadataOperation)
        ready = finish_episode(store, operation)
    barrier = Barrier(2)
    results: list[MetadataOperation | BudgetRefusal] = []
    errors: list[Exception] = []

    def rearm() -> None:
        try:
            with open_state(path, budget_limits=LIMITS) as store:
                barrier.wait(timeout=10)
                results.append(prepare_episode(store, (E2,), previous=ready))
        except Exception as exc:
            errors.append(exc)

    threads = [Thread(target=rearm), Thread(target=rearm)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=20)
    assert not errors
    assert len(results) == 2
    assert sum(isinstance(result, MetadataOperation) for result in results) == 1
    with open_state(path) as store:
        assert store.budget_view().pending_file_requests == 1
        assert store.budget_view().rolling_new_submissions == 2
        assert store.connection.execute(
            "SELECT COUNT(*) FROM metadata_operation_history"
        ).fetchone() == (1,)


@pytest.mark.parametrize("failure", ["commit", "archive"])
def test_rearm_failure_rolls_back_archive_generation_budget_and_latches_stop(
    tmp_path: Path, failure: str
) -> None:
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as store:
        operation = prepare_episode(store, (E1,))
        assert isinstance(operation, MetadataOperation)
        ready = finish_episode(store, operation)
        before = store.budget_view()
        denied = False

        def authorizer(action: int, name: str | None, *_: object) -> int:
            nonlocal denied
            matching = (
                action == sqlite3.SQLITE_TRANSACTION and name == "COMMIT"
                if failure == "commit"
                else action == sqlite3.SQLITE_INSERT and name == "metadata_operation_history"
            )
            if matching and not denied:
                denied = True
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        store.connection.set_authorizer(authorizer)
        with pytest.raises(WriteDisabledError):
            prepare_episode(store, (E2,), previous=ready)
        store.connection.set_authorizer(None)
        assert store.get_metadata_operation(SOURCE, 18208) == ready
        assert store.budget_view() == before
        assert store.connection.execute(
            "SELECT COUNT(*) FROM metadata_operation_history"
        ).fetchone() == (0,)
    with open_state(path) as store:
        assert store.writes_disabled
        assert store.get_metadata_operation(SOURCE, 18208) == ready


@pytest.mark.parametrize("stage", ["prepared", "possible", "accepted", "attention"])
def test_incomplete_own_generation_never_rearms_for_new_episode(tmp_path: Path, stage: str) -> None:
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        operation = prepare_episode(store, (E1,))
        assert isinstance(operation, MetadataOperation)
        if stage != "prepared":
            operation = store.mark_metadata_possibly_delivered(
                operation, iso=T0, verification_deadline_iso=T1
            )
        if stage in {"accepted", "attention"}:
            store.admit_metadata_transport(operation)
            operation = store.record_metadata_operation(operation, M.ACCEPTED_WAITING, iso=T0)
        if stage == "attention":
            operation = store.record_metadata_operation(operation, M.NEEDS_ATTENTION, iso=T1)
        result = prepare_episode(store, (E2,), previous=operation)
        assert isinstance(result, BudgetRefusal)
        assert store.budget_view().rolling_new_submissions == 1


def test_ready_requires_all_and_only_verified_required_targets(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        operation = prepare_episode(store, (E2, E1, E2))
        assert isinstance(operation, MetadataOperation)
        assert operation.required_targets == (E1, E2)
        with pytest.raises(StateError, match="verified"):
            store.record_metadata_operation(operation, M.READY, iso=T1)
        with pytest.raises(StateError, match="exactly"):
            store.record_metadata_operation(
                operation, M.READY, iso=T1, verified_required_targets=(E1,)
            )
        with pytest.raises(StateError, match="exactly"):
            store.record_metadata_operation(
                operation, M.READY, iso=T1, verified_required_targets=(E1, E2, E3)
            )
        assert store.budget_view().pending_file_requests == 1
        ready = store.record_metadata_operation(
            operation, M.READY, iso=T1, verified_required_targets=(E2, E1)
        )
        assert ready.verified_required_targets == (E1, E2)
        assert store.budget_view().pending_file_requests == 0


def test_rearm_revoked_or_changed_consent_refuses_without_archival(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        store.save_onboarding_json('{"revision":1}', T0)
        operation = prepare_episode(store, (E1,))
        assert isinstance(operation, MetadataOperation)
        ready = finish_episode(store, operation)
        store.save_onboarding_json('{"revision":2}', T1)
        with pytest.raises(StateError, match="authorization"):
            prepare_episode(store, (E2,), previous=ready, expected_document_json='{"revision":1}')
        store.set_service_pause(True, "user", T1)
        with pytest.raises(StateError, match="authorization"):
            prepare_episode(store, (E2,), previous=ready, expected_document_json='{"revision":2}')
        assert store.get_metadata_operation(SOURCE, 18208) == ready
        assert store.connection.execute(
            "SELECT COUNT(*) FROM metadata_operation_history"
        ).fetchone() == (0,)


def test_v8_ready_upgrade_preserves_unknown_coverage_and_refuses_rearm(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as store:
        operation = prepare_anime(store)
        ready = store.record_metadata_operation(operation, M.READY, iso=T1)
        store.connection.execute("DROP TABLE latest_scan_exclusions")
        store.connection.execute("DROP TABLE metadata_operation_history")
        for column in ("required_targets_json", "missing_targets_json", "verified_targets_json"):
            store.connection.execute(f"ALTER TABLE metadata_operations DROP COLUMN {column}")
        store.connection.execute("DELETE FROM schema_version WHERE version>=9")
        store.connection.execute("PRAGMA user_version=8")
        original = store.connection.execute("SELECT * FROM metadata_operations").fetchone()
    with open_state(path, budget_limits=LIMITS) as store:
        assert store.schema_version() == SCHEMA_VERSION
        assert (
            store.connection.execute("SELECT * FROM metadata_operations").fetchone()[:14]
            == original
        )
        assert store.get_metadata_operation(SOURCE, 18208) == ready
        with pytest.raises(StateError, match="unknown"):
            prepare_episode(store, (E2,), previous=ready)
        assert store.live_send_counts() == (0, 0)


@pytest.mark.parametrize(
    "target",
    [
        (18209, EpisodeType.EPISODE, 1),
        (18208, "Episode", 1),
        (18208, EpisodeType.EPISODE, True),
        (18208, EpisodeType.EPISODE, 0),
    ],
)
def test_canonical_target_identity_refuses_cross_anime_or_untyped_data(
    tmp_path: Path, target: object
) -> None:
    with (
        open_state(tmp_path / "state.db", budget_limits=LIMITS) as store,
        pytest.raises(StateError),
    ):
        prepare_episode(store, (target,))  # type: ignore[arg-type]


def test_unsent_retry_cannot_change_back_to_historically_covered_episode(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.db", budget_limits=replace(LIMITS, per_cycle_new=3)) as store:
        first = prepare_episode(store, (E1,))
        assert isinstance(first, MetadataOperation)
        ready = finish_episode(store, first)
        second = prepare_episode(store, (E2,), previous=ready)
        assert isinstance(second, MetadataOperation)
        cancelled = store.record_metadata_operation(second, M.DEFINITELY_REJECTED, iso=T0)
        disguised_old = store.prepare_metadata_operation(
            SOURCE,
            18208,
            demand_fingerprint=DEMAND,
            request_fingerprint=REQUEST,
            request_version=1,
            cycle_key="cycle",
            iso=T0,
            required_targets=(E1,),
            missing_targets=(E1,),
            retry_unsent_claim=cancelled.claim,
        )
        assert isinstance(disguised_old, BudgetRefusal)
        same_unsent = store.prepare_metadata_operation(
            SOURCE,
            18208,
            demand_fingerprint=DEMAND,
            request_fingerprint=REQUEST,
            request_version=1,
            cycle_key="cycle",
            iso=T0,
            required_targets=(E2,),
            missing_targets=(E2,),
            retry_unsent_claim=cancelled.claim,
        )
        assert isinstance(same_unsent, MetadataOperation)
        assert store.budget_view().rolling_new_submissions == 3


def observe_episode(
    store: StateStore, targets: tuple[MetadataEpisodeIdentity, ...]
) -> MetadataOperation:
    return store.observe_external_metadata_operation(
        SOURCE,
        18208,
        demand_fingerprint=DEMAND,
        request_fingerprint=REQUEST,
        request_version=1,
        iso=T0,
        required_targets=targets,
        missing_targets=targets,
    )


def test_external_typed_ready_allows_new_episode_without_replaying_historical_target(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as store:
        external = observe_episode(store, (E1, E1))
        assert external.required_targets == (E1,)
        assert external.missing_targets == (E1,)
        assert not external.reservation_owned
        assert store.budget_view().rolling_new_submissions == 0
        with pytest.raises(StateError, match="verified"):
            store.record_metadata_operation(external, M.READY, iso=T1)
        external_ready = store.record_metadata_operation(
            external,
            M.READY,
            iso=T1,
            verified_required_targets=(E1,),
        )
        assert store.budget_view().pending_file_requests == 0
    with open_state(path, budget_limits=LIMITS) as store:
        second = prepare_episode(store, (E2,), previous=external_ready)
        assert isinstance(second, MetadataOperation)
        second_ready = finish_episode(store, second)
        snapshot = store.connection.execute(
            "SELECT reservation_owned, transport_admitted, verified_targets_json"
            " FROM metadata_operation_history"
        ).fetchone()
        assert snapshot == (0, 0, '[[18208,"Episode",1]]')
        assert store.budget_view().rolling_new_submissions == 1
    with open_state(path, budget_limits=LIMITS) as store:
        replay = prepare_episode(store, (E1,), previous=second_ready, demand="f" * 64)
        assert isinstance(replay, BudgetRefusal)
        assert store.budget_view().pending_file_requests == 0
        assert store.budget_view().rolling_new_submissions == 1


def test_external_typed_observation_preserves_same_anime_own_intent(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        own = prepare_episode(store, (E1,))
        assert isinstance(own, MetadataOperation)
        marked = store.mark_metadata_possibly_delivered(own, iso=T0, verification_deadline_iso=T1)
        store.admit_metadata_transport(marked)
        before = store.get_metadata_operation(SOURCE, 18208)
        budget = store.budget_view()
        observed = observe_episode(store, (E2,))
        assert observed == before
        assert store.budget_view() == budget
        assert observed.required_targets == (E1,)
        assert observed.transport_admitted
        assert observed.reservation_owned


def test_external_typed_observation_commit_failure_rolls_back_coverage_and_stops(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as store:
        denied = False

        def authorizer(action: int, name: str | None, *_: object) -> int:
            nonlocal denied
            if action == sqlite3.SQLITE_TRANSACTION and name == "COMMIT" and not denied:
                denied = True
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        store.connection.set_authorizer(authorizer)
        with pytest.raises(WriteDisabledError):
            observe_episode(store, (E1,))
        store.connection.set_authorizer(None)
        assert store.get_metadata_operation(SOURCE, 18208) is None
        assert store.budget_view().pending_file_requests == 0
    with open_state(path) as store:
        assert store.writes_disabled
        assert store.get_metadata_operation(SOURCE, 18208) is None


@pytest.mark.parametrize("external", [False, True])
@pytest.mark.parametrize("missing", [None, (), (E2,)])
def test_external_and_own_boundaries_share_canonical_demand_validation(
    tmp_path: Path,
    external: bool,
    missing: tuple[MetadataEpisodeIdentity, ...] | None,
) -> None:
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:

        def observe_or_prepare() -> None:
            if external:
                store.observe_external_metadata_operation(
                    SOURCE,
                    18208,
                    demand_fingerprint=DEMAND,
                    request_fingerprint=REQUEST,
                    request_version=1,
                    iso=T0,
                    required_targets=(E1,),
                    missing_targets=missing,
                )
            else:
                store.prepare_metadata_operation(
                    SOURCE,
                    18208,
                    demand_fingerprint=DEMAND,
                    request_fingerprint=REQUEST,
                    request_version=1,
                    cycle_key="cycle",
                    iso=T0,
                    required_targets=(E1,),
                    missing_targets=missing,
                )

        with pytest.raises(StateError, match="subsets"):
            observe_or_prepare()
        assert store.get_metadata_operation(SOURCE, 18208) is None
