"""Read-only recovery through real mocked HTTP and durable SQLite rows."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest
import queue_fixtures as qf
from test_linking import anidb_episode, anidb_list, shoko_episode
from test_state import LIMITS, T0, T1, T2

from shoko_linker.http import ApiEndpoint
from shoko_linker.linking import EpisodeIdResolver, EpisodeResolutionStatus
from shoko_linker.mappings import EpisodeType
from shoko_linker.metadata_operations import (
    MetadataRecoveryReport,
    MetadataRecoveryStatus,
    metadata_source_key,
    recover_metadata_operations,
)
from shoko_linker.state import (
    BudgetRefusal,
    MetadataOperation,
    RecoveryBatch,
    StateError,
    StateStore,
    WriteDisabledError,
    open_state,
)
from shoko_linker.state import MetadataOperationState as M

TARGET = (18208, EpisodeType.EPISODE, 1)


class Server:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.available = True
        self.queue: list[dict[str, Any]] = []
        self.queue_status = 200
        self.after_queue: Callable[[], None] = lambda: None
        self.after_request: Callable[[], None] = lambda: None
        self.endpoint = ApiEndpoint(
            httpx.Client(
                base_url="http://shoko:8111/",
                transport=httpx.MockTransport(self.handle),
                timeout=httpx.Timeout(30, connect=1, pool=2, write=3),
            ),
            lambda: {"apikey": "synthetic-secret-never-real"},
        )

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        assert request.method == "GET"
        self.after_request()
        if request.url.path == "/api/v3/Queue":
            result = httpx.Response(200, json=qf.summary(self.queue))
            self.after_queue()
            return result
        if request.url.path == "/api/v3/Queue/Items":
            return httpx.Response(
                self.queue_status, json={"Total": len(self.queue), "List": self.queue}
            )
        if "/Series/AniDB/" in request.url.path:
            anime = int(request.url.path.split("/")[-2])
            return httpx.Response(
                200 if self.available else 404,
                json=anidb_list([anidb_episode(650, anime, "Episode", 1)]),
            )
        assert request.url.path == "/api/v3/Episode/AniDB/650/Episode"
        return httpx.Response(200 if self.available else 404, json=shoko_episode(777, 650))


def operation(
    store: StateStore, endpoint: ApiEndpoint, state: str, anime: int = 18208
) -> MetadataOperation:
    key = metadata_source_key(endpoint)
    targets = [(anime, EpisodeType.EPISODE, 1)]
    if state == "external":
        return store.observe_external_metadata_operation(
            key,
            anime,
            demand_fingerprint="b" * 64,
            request_fingerprint="c" * 64,
            request_version=1,
            iso=T0,
            required_targets=targets,
            missing_targets=targets,
        )
    row = store.prepare_metadata_operation(
        key,
        anime,
        demand_fingerprint="b" * 64,
        request_fingerprint="c" * 64,
        request_version=1,
        cycle_key="cycle",
        iso=T0,
        required_targets=targets,
        missing_targets=targets,
    )
    assert isinstance(row, MetadataOperation)
    if state == "prepared":
        return row
    row = store.mark_metadata_possibly_delivered(row, iso=T0, verification_deadline_iso=T1)
    if state == "marked":
        return row
    store.admit_metadata_transport(row)
    if state == "admitted":
        return row
    row = store.record_metadata_operation(row, M.ACCEPTED_WAITING, iso=T0)
    if state == "attention":
        row = store.record_metadata_operation(row, M.NEEDS_ATTENTION, iso=T0)
    return row


def recover(server: Server, store: StateStore, **kwargs: Any) -> MetadataRecoveryReport:
    return recover_metadata_operations(
        server.endpoint,
        store,
        source_key=metadata_source_key(server.endpoint),
        now_iso=kwargs.pop("now_iso", T0),
        deadline=kwargs.pop("deadline", 100.0),
        max_rows=kwargs.pop("max_rows", 10),
        clock=kwargs.pop("clock", lambda: 0.0),
        **kwargs,
    )


@pytest.mark.parametrize(
    "boundary", ["prepared", "marked", "admitted", "accepted", "attention", "external"]
)
def test_restart_recovers_without_transport(tmp_path: Path, boundary: str) -> None:
    server = Server()
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as store:
        row = operation(store, server.endpoint, boundary)
    with open_state(path, budget_limits=LIMITS) as store:
        report = recover(server, store)
        expected = (
            MetadataRecoveryStatus.UNSENT_RELEASED
            if boundary == "prepared"
            else MetadataRecoveryStatus.READY
        )
        assert report.observations[0].status is expected
        current = store.get_metadata_operation(row.source_key, row.anidb_anime_id)
        assert current is not None
        assert current.state is (M.DEFINITELY_REJECTED if boundary == "prepared" else M.READY)
        assert current.verification_deadline_iso == row.verification_deadline_iso
        assert store.budget_view().pending_episode_jobs == 0
        assert store.budget_view().completed_submissions == 0
        assert recover(server, store).processed_count == 0
        assert store.budget_view().pending_episode_jobs == 0


@pytest.mark.parametrize("queue_state", ["waiting", "running", "blocked"])
def test_available_metadata_is_not_ready_while_refresh_active(
    tmp_path: Path, queue_state: str
) -> None:
    server = Server()
    server.queue = [qf.item("AniDB.GetAniDBAnimeJob_AnimeID:18208", queue_state)]
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        row = operation(store, server.endpoint, "accepted")
        assert recover(server, store).observations[0].status is MetadataRecoveryStatus.WAITING_QUEUE
        current = store.get_metadata_operation(row.source_key, 18208)
        assert current is not None
        assert current.state is M.ACCEPTED_WAITING
        assert store.budget_view().pending_episode_jobs == 1


@pytest.mark.parametrize("visible", [True, False])
def test_missing_and_unknown_are_read_only_waits(tmp_path: Path, visible: bool) -> None:
    server = Server()
    server.available = False
    server.queue_status = 200 if visible else 403
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        operation(store, server.endpoint, "accepted")
        report = recover(server, store)
        expected = (
            MetadataRecoveryStatus.WAITING_METADATA
            if visible
            else MetadataRecoveryStatus.QUEUE_UNKNOWN
        )
        assert report.observations[0].status is expected
        assert any("/Series/AniDB/" in request.url.path for request in server.requests)
        assert store.budget_view().pending_episode_jobs == 1


def test_refresh_completion_requires_second_fresh_chain(tmp_path: Path) -> None:
    server = Server()
    server.after_queue = lambda: setattr(server, "available", False)
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        operation(store, server.endpoint, "accepted")
        assert (
            recover(server, store).observations[0].status is MetadataRecoveryStatus.WAITING_METADATA
        )
        assert sum("/Series/AniDB/" in request.url.path for request in server.requests) == 2
        assert store.budget_view().pending_episode_jobs == 1


@pytest.mark.parametrize("state", ["accepted", "marked", "external"])
def test_original_deadline_retained_external_remains_adoptable(tmp_path: Path, state: str) -> None:
    server = Server()
    server.available = False
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        row = operation(store, server.endpoint, state)
        report = recover(server, store, now_iso=T2)
        expected = (
            MetadataRecoveryStatus.WAITING_METADATA
            if state == "external"
            else MetadataRecoveryStatus.NEEDS_ATTENTION
        )
        assert report.observations[0].status is expected
        current = store.get_metadata_operation(row.source_key, 18208)
        assert current is not None
        assert current.verification_deadline_iso == row.verification_deadline_iso
        assert current.state is (M.ACCEPTED_WAITING if state == "external" else M.NEEDS_ATTENTION)
        assert store.budget_view().pending_episode_jobs == (0 if state == "external" else 1)


@pytest.mark.parametrize("coverage", [None, '"broken"', '[[18208,"Episode",2]]'])
def test_legacy_corrupt_or_different_coverage_never_invents_readiness(
    tmp_path: Path, coverage: str | None
) -> None:
    server = Server()
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        operation(store, server.endpoint, "accepted")
        store.connection.execute(
            "UPDATE metadata_operations SET required_targets_json=?", (coverage,)
        )
        report = recover(server, store)
        assert report.observations[0].status is not MetadataRecoveryStatus.READY
        assert store.budget_view().pending_episode_jobs == 1


def test_rotation_and_corrupt_row_do_not_starve_other_rows(tmp_path: Path) -> None:
    server = Server()
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        for anime in (1, 2, 3):
            operation(store, server.endpoint, "external", anime)
        store.connection.execute(
            "UPDATE metadata_operations SET required_targets_json='broken' WHERE anidb_anime_id=1"
        )
        reports = [recover(server, store, max_rows=1) for _ in range(3)]
        assert [report.observations[0].anime_id for report in reports] == [1, 2, 3]
        assert reports[0].observations[0].status is MetadataRecoveryStatus.INVALID_COVERAGE
        assert reports[-1].observations[0].status is MetadataRecoveryStatus.READY
        assert reports[0].deferred_count == 2


@pytest.mark.parametrize("boundary", ["prepared", "accepted"])
def test_latched_stop_never_cleared_or_sends(tmp_path: Path, boundary: str) -> None:
    server = Server()
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        operation(store, server.endpoint, boundary)
        store.disable_writes("synthetic stop")
        report = recover(server, store)
        expected = (
            MetadataRecoveryStatus.UNSENT_RELEASED
            if boundary == "prepared"
            else MetadataRecoveryStatus.STATE_UNAVAILABLE
        )
        assert report.observations[0].status is expected
        assert store.writes_disabled
        assert store.budget_view().pending_episode_jobs == (0 if boundary == "prepared" else 1)


@pytest.mark.parametrize(
    ("initial", "advance", "expected_gets"), [(2.0, 0.0, 0), (0.0, 2.0, 1), (0.0, 0.6, 2)]
)
def test_resolver_deadline_checks_each_hop_and_narrows_timeout(
    initial: float, advance: float, expected_gets: int
) -> None:
    server = Server()
    now = [initial]
    server.after_request = lambda: now.__setitem__(0, now[0] + advance)
    result = EpisodeIdResolver(server.endpoint, deadline=1.0, clock=lambda: now[0]).resolve(
        18208, "Episode", 1
    )
    assert result.status is EpisodeResolutionStatus.TRANSPORT_FAILURE
    assert len(server.requests) == expected_gets
    for request in server.requests:
        assert all(value <= 1.0 for value in request.extensions["timeout"].values())


def test_zero_deadline_or_row_cap_defers_without_io(tmp_path: Path) -> None:
    server = Server()
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        operation(store, server.endpoint, "accepted")
        assert recover(server, store, max_rows=0).deferred_count == 1
        assert recover(server, store, deadline=0.0).deferred_count == 1
        assert not server.requests


def test_complete_persisted_coverage_required_even_when_current_first_target_ready(
    tmp_path: Path,
) -> None:
    server = Server()
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        row = operation(store, server.endpoint, "accepted")
        store.connection.execute(
            "UPDATE metadata_operations SET required_targets_json=?",
            ('[[18208,"Episode",1],[18208,"Episode",2]]',),
        )
        assert (
            recover(server, store).observations[0].status is MetadataRecoveryStatus.WAITING_METADATA
        )
        current = store.get_metadata_operation(row.source_key, 18208)
        assert current is not None
        assert current.state is M.ACCEPTED_WAITING
        assert current.verified_required_targets is None


def test_readonly_recovery_ignores_missing_consent_and_paused_setup(tmp_path: Path) -> None:
    server = Server()
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        operation(store, server.endpoint, "accepted")
        store.set_meta("service_user_paused", "1")
        # No consent document or enabled preference is present. Previously sent
        # jobs still need verification and releasing capacity is not a new write.
        assert recover(server, store).observations[0].status is MetadataRecoveryStatus.READY


def test_ready_record_failure_rolls_back_release_and_latches_stop(tmp_path: Path) -> None:
    server = Server()
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        row = operation(store, server.endpoint, "accepted")
        store.connection.execute(
            "CREATE TRIGGER abort_ready BEFORE UPDATE ON metadata_operations "
            "WHEN NEW.state='ready' BEGIN SELECT RAISE(ABORT,'synthetic failure'); END"
        )
        assert (
            recover(server, store).observations[0].status
            is MetadataRecoveryStatus.STATE_UNAVAILABLE
        )
        assert store.writes_disabled
        current = store.get_metadata_operation(row.source_key, 18208)
        assert current is not None
        assert current.state is M.ACCEPTED_WAITING
        assert not current.slot_released
        assert current.verified_required_targets is None
        assert store.budget_view().pending_episode_jobs == 1


def test_target_reads_precede_queue_even_if_queue_exhausts_cycle(tmp_path: Path) -> None:
    server = Server()
    now = [0.0]
    server.after_queue = lambda: now.__setitem__(0, 101.0)
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        operation(store, server.endpoint, "accepted")
        report = recover(server, store, clock=lambda: now[0])
        assert report.observations[0].status is MetadataRecoveryStatus.DEADLINE
        assert "/Series/AniDB/" in server.requests[0].url.path
        assert store.budget_view().pending_episode_jobs == 1


def test_deadline_timeout_does_not_extend_configured_phase_ceilings() -> None:
    server = Server()
    result = EpisodeIdResolver(server.endpoint, deadline=10, clock=lambda: 0).resolve(
        18208, "Episode", 1
    )
    assert result.status is EpisodeResolutionStatus.RESOLVED
    assert server.requests[0].extensions["timeout"] == {
        "connect": 1.0,
        "read": 10.0,
        "write": 3.0,
        "pool": 2.0,
    }


def test_expired_verification_is_recorded_before_slow_metadata_read(tmp_path: Path) -> None:
    server = Server()
    now = [0.0]
    server.after_request = lambda: now.__setitem__(0, 101.0)
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        row = operation(store, server.endpoint, "accepted")
        report = recover(server, store, now_iso=T2, clock=lambda: now[0])
        assert report.observations[0].status is MetadataRecoveryStatus.NEEDS_ATTENTION
        current = store.get_metadata_operation(row.source_key, 18208)
        assert current is not None
        assert current.state is M.NEEDS_ATTENTION
        assert current.verification_deadline_iso == T1
        assert store.budget_view().pending_episode_jobs == 1


def test_expired_resolver_does_not_return_old_positive_cache() -> None:
    server = Server()
    now = [0.0]
    resolver = EpisodeIdResolver(server.endpoint, deadline=1, clock=lambda: now[0])
    assert resolver.resolve(18208, "Episode", 1).status is EpisodeResolutionStatus.RESOLVED
    now[0] = 2.0
    assert resolver.resolve(18208, "Episode", 1).status is EpisodeResolutionStatus.TRANSPORT_FAILURE
    assert len(server.requests) == 2


@pytest.mark.parametrize("boundary", ["marked", "admitted", "accepted"])
def test_prepared_recovery_snapshot_cannot_cancel_concurrent_delivery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, boundary: str
) -> None:
    server = Server()
    path = tmp_path / "state.db"
    with (
        open_state(path, budget_limits=LIMITS) as recovery_store,
        open_state(path, budget_limits=LIMITS) as sender,
    ):
        row = operation(sender, server.endpoint, "prepared")
        cancel = recovery_store.cancel_prepared_metadata_operation

        def concurrent_mark(snapshot: MetadataOperation, *, iso: str) -> None:
            assert snapshot.state is M.PREPARED
            marked = sender.mark_metadata_possibly_delivered(
                row, iso=T0, verification_deadline_iso=T1
            )
            if boundary in {"admitted", "accepted"}:
                sender.admit_metadata_transport(marked)
            if boundary == "accepted":
                sender.record_metadata_operation(marked, M.ACCEPTED_WAITING, iso=T0)
            cancel(snapshot, iso=iso)

        monkeypatch.setattr(recovery_store, "cancel_prepared_metadata_operation", concurrent_mark)
        report = recover(server, recovery_store)
        assert report.observations[0].status is MetadataRecoveryStatus.STATE_UNAVAILABLE
        current = sender.get_metadata_operation(row.source_key, 18208)
        assert current is not None
        assert current.state is (
            M.ACCEPTED_WAITING if boundary == "accepted" else M.POSSIBLY_DELIVERED
        )
        assert not current.slot_released
        assert sender.budget_view().pending_episode_jobs == 1
        assert sender.budget_view().pending_file_requests == 1
        assert not server.requests
        duplicate = sender.prepare_metadata_operation(
            row.source_key,
            18208,
            demand_fingerprint="b" * 64,
            request_fingerprint="c" * 64,
            request_version=1,
            cycle_key="next",
            iso=T0,
            retry_unsent_claim=row.claim,
            required_targets=[TARGET],
            missing_targets=[TARGET],
        )
        assert isinstance(duplicate, BudgetRefusal)


@pytest.mark.parametrize("admitted", [False, True])
def test_stop_cleanup_refuses_stale_prepared_snapshot_after_concurrent_mark(
    tmp_path: Path, admitted: bool
) -> None:
    server = Server()
    path = tmp_path / "state.db"
    with (
        open_state(path, budget_limits=LIMITS) as recovery_store,
        open_state(path, budget_limits=LIMITS) as sender,
    ):
        snapshot = operation(sender, server.endpoint, "prepared")
        marked = sender.mark_metadata_possibly_delivered(
            snapshot, iso=T0, verification_deadline_iso=T1
        )
        if admitted:
            sender.admit_metadata_transport(marked)
        sender.disable_writes("synthetic stop after mark")
        with pytest.raises(StateError, match="possibly delivered"):
            recovery_store.void_prepared_metadata_after_write_stop(snapshot, iso=T0)
        current = sender.get_metadata_operation(snapshot.source_key, 18208)
        assert current is not None
        assert current.state is M.POSSIBLY_DELIVERED
        assert current.transport_admitted is admitted
        assert not current.slot_released
        assert recovery_store.writes_disabled
        assert sender.budget_view().pending_episode_jobs == 1


@pytest.mark.parametrize("stopped", [False, True])
def test_prepared_cancel_rolls_back_release_if_terminal_update_fails(
    tmp_path: Path, stopped: bool
) -> None:
    server = Server()
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        row = operation(store, server.endpoint, "prepared")
        store.connection.execute(
            "CREATE TRIGGER abort_cancel BEFORE UPDATE ON metadata_operations "
            "WHEN NEW.state='definitely_rejected' "
            "BEGIN SELECT RAISE(ABORT,'synthetic cancellation failure'); END"
        )
        if stopped:
            store.disable_writes("synthetic stop before cancel")
            with pytest.raises(StateError, match="cleanup could not be persisted"):
                store.void_prepared_metadata_after_write_stop(row, iso=T0)
        else:
            with pytest.raises(WriteDisabledError):
                store.cancel_prepared_metadata_operation(row, iso=T0)
        current = store.get_metadata_operation(row.source_key, 18208)
        assert current is not None
        assert current.state is M.PREPARED
        assert not current.slot_released
        assert store.budget_view().pending_episode_jobs == 1
        assert store.writes_disabled


def test_stopped_prepared_cleanup_failure_does_not_abort_later_recovery(tmp_path: Path) -> None:
    server = Server()
    server.available = False
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        prepared = operation(store, server.endpoint, "prepared", anime=1)
        external = operation(store, server.endpoint, "external", anime=2)
        store.connection.execute(
            "CREATE TRIGGER abort_cancel BEFORE UPDATE ON metadata_operations "
            "WHEN NEW.state='definitely_rejected' "
            "BEGIN SELECT RAISE(ABORT,'synthetic secret failure details'); END"
        )
        store.disable_writes("synthetic preexisting stop")
        report = recover(server, store)
        assert report.processed_count == 2
        assert [observation.status for observation in report.observations] == [
            MetadataRecoveryStatus.STATE_UNAVAILABLE,
            MetadataRecoveryStatus.WAITING_METADATA,
        ]
        assert "synthetic secret" not in repr(report)
        assert any(
            request.url.path == "/api/v3/Series/AniDB/2/Episode" for request in server.requests
        )
        assert store.get_metadata_operation(prepared.source_key, 1) == prepared
        assert store.get_metadata_operation(external.source_key, 2) == external
        assert store.budget_view().pending_episode_jobs == 1
        assert store.budget_view().pending_file_requests == 1
        assert store.writes_disabled


def test_cancel_stale_claim_does_not_cancel_reprepared_generation(tmp_path: Path) -> None:
    server = Server()
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        old = operation(store, server.endpoint, "prepared")
        store.cancel_prepared_metadata_operation(old, iso=T0)
        new = store.prepare_metadata_operation(
            old.source_key,
            18208,
            demand_fingerprint="b" * 64,
            request_fingerprint="c" * 64,
            request_version=1,
            cycle_key="next",
            iso=T0,
            retry_unsent_claim=old.claim,
            required_targets=[TARGET],
            missing_targets=[TARGET],
        )
        assert isinstance(new, MetadataOperation)
        assert new.claim != old.claim
        with pytest.raises(StateError, match="stale"):
            store.cancel_prepared_metadata_operation(old, iso=T0)
        assert store.get_metadata_operation(old.source_key, 18208) == new
        assert store.budget_view().pending_episode_jobs == 1


def test_source_key_normalizes_only_endpoint_identity_and_rejects_old_source(
    tmp_path: Path,
) -> None:
    server = Server()

    def key(url: str) -> str:
        return metadata_source_key(ApiEndpoint(httpx.Client(base_url=url), lambda: {}))

    assert key("HTTP://SHOKO:80/base") == key("http://shoko/base/")
    assert key("http://user:pass@shoko/base/?apikey=secret#fragment") == key("http://shoko/base/")
    assert key("http://shoko/Base") != key("http://shoko/base")
    assert key("http://shoko/base") != key("http://shoko")
    with (
        open_state(tmp_path / "state.db", budget_limits=LIMITS) as store,
        pytest.raises(ValueError, match="source"),
    ):
        recover_metadata_operations(
            server.endpoint, store, source_key="a" * 64, now_iso=T0, deadline=10, max_rows=1
        )
    assert not server.requests


@pytest.mark.parametrize("expired", [False, True])
def test_no_allowance_does_not_decode_or_advance_cursor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, expired: bool
) -> None:
    server = Server()
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        operation(store, server.endpoint, "accepted")
        key = f"metadata_recovery_cursor:v1:{metadata_source_key(server.endpoint)}"
        store.set_meta(key, "9")

        def forbidden(*_args: object) -> None:
            pytest.fail("no allowance must not decode an operation")

        monkeypatch.setattr(store, "get_metadata_operation", forbidden)
        report = recover(
            server, store, max_rows=10 if expired else 0, deadline=0.0 if expired else 100.0
        )
        assert report.processed_count == 0
        assert report.deferred_count == 1
        assert store.get_meta(key) == "9"
        assert not server.requests


def test_terminal_history_is_not_decoded_or_charged_to_pending_allowance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = Server()
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        for anime in range(1, 41):
            row = operation(store, server.endpoint, "external", anime)
            store.record_metadata_operation(
                row, M.READY, iso=T0, verified_required_targets=row.required_targets
            )
        for anime in (41, 42, 43):
            operation(store, server.endpoint, "external", anime)
        # Foreign server operations must not count or consume the local allowance.
        store.observe_external_metadata_operation(
            "a" * 64,
            44,
            demand_fingerprint="b" * 64,
            request_fingerprint="c" * 64,
            request_version=1,
            iso=T0,
        )
        original = store.get_metadata_operation
        decoded: list[int] = []

        def tracked(source: str, anime: int) -> MetadataOperation | None:
            decoded.append(anime)
            return original(source, anime)

        monkeypatch.setattr(store, "get_metadata_operation", tracked)
        server.available = False
        report = recover(server, store, max_rows=2)
        assert decoded == [41, 42]
        assert report.processed_count == 2
        assert report.deferred_count == 1


def test_metadata_recovery_checks_deadline_after_decoding_before_http(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    server = Server()
    clock = [0.0]
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        operation(store, server.endpoint, "accepted")
        original = store.get_metadata_operation

        def slow_decode(source: str, anime: int) -> MetadataOperation | None:
            row = original(source, anime)
            clock[0] = 1.0
            return row

        monkeypatch.setattr(store, "get_metadata_operation", slow_decode)
        report = recover(server, store, deadline=1.0, clock=lambda: clock[0])
        assert report.observations[0].status is MetadataRecoveryStatus.DEADLINE
        assert not server.requests
        assert store.budget_view().pending_episode_jobs == 1


def test_metadata_pending_cursor_wraps_across_restarts_with_corrupt_first_row(
    tmp_path: Path,
) -> None:
    server = Server()
    server.available = False
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as store:
        for anime in (1, 2, 3):
            operation(store, server.endpoint, "external", anime)
        store.connection.execute(
            "UPDATE metadata_operations SET required_targets_json='broken' WHERE anidb_anime_id=1"
        )
    seen: list[int] = []
    for _ in range(4):
        with open_state(path, budget_limits=LIMITS) as store:
            report = recover(server, store, max_rows=1)
            assert report.deferred_count == 2
            seen.append(report.observations[0].anime_id)
    assert seen == [1, 2, 3, 1]
    assert all(request.method == "GET" for request in server.requests)


@pytest.mark.parametrize("settlement", ["ready", "rejected", "missing"])
def test_selected_identity_consumes_allowance_when_other_handle_settles_or_removes_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, settlement: str
) -> None:
    server = Server()
    path = tmp_path / "state.db"
    with (
        open_state(path, budget_limits=LIMITS) as store,
        open_state(path, budget_limits=LIMITS) as peer,
    ):
        row = operation(
            store,
            server.endpoint,
            "marked"
            if settlement == "rejected"
            else "external"
            if settlement == "missing"
            else "accepted",
        )
        original = store.metadata_recovery_batch

        def interleave(source: str, *, after_id: int, limit: int) -> RecoveryBatch:
            batch = original(source, after_id=after_id, limit=limit)
            assert batch.ids == (row.anidb_anime_id,)
            if settlement == "missing":
                # Simulate loss of an unowned external observation after selection.
                peer.connection.execute("DELETE FROM metadata_operations")
                peer.connection.commit()
            else:
                peer.record_metadata_operation(
                    row,
                    M.READY if settlement == "ready" else M.DEFINITELY_REJECTED,
                    iso=T0,
                    verified_required_targets=row.required_targets
                    if settlement == "ready"
                    else None,
                )
            return batch

        monkeypatch.setattr(store, "metadata_recovery_batch", interleave)
        report = recover(server, store, max_rows=1)
        assert report.processed_count == 1
        assert report.deferred_count == 0
        assert store.get_meta(f"metadata_recovery_cursor:v1:{row.source_key}") == str(
            row.anidb_anime_id
        )
        if settlement == "rejected":
            assert report.observations == ()
        else:
            assert report.observations[0].status is (
                MetadataRecoveryStatus.READY
                if settlement == "ready"
                else MetadataRecoveryStatus.INVALID_COVERAGE
            )
        assert store.budget_view().pending_episode_jobs == 0
        assert not server.requests
