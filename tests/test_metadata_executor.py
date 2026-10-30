"""One guarded metadata mutation, exercised offline against HTTP and SQLite."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import httpx
import pytest
import queue_fixtures as qf
import test_daemon as td
import test_onboarding as to
from test_linking import DripFileBody
from test_metadata_planning import MetadataStub, _target

from shoko_linker import daemon
from shoko_linker.linking import MetadataDemand, metadata_demand_fingerprint
from shoko_linker.linking import preflight_file as original_file_preflight
from shoko_linker.mapping_store import ActiveMappings, load_active
from shoko_linker.mappings import EpisodeType, Resolution, ResolutionStatus
from shoko_linker.metadata_operations import metadata_source_key
from shoko_linker.metadata_refresh import (
    MetadataRefreshOutcome,
    MetadataRefreshReason,
    MetadataRefreshResult,
)
from shoko_linker.onboarding import (
    METADATA_MAINTENANCE_POLICY,
    OnboardingManager,
    required_confirmation_facts,
)
from shoko_linker.policies import RunPolicy
from shoko_linker.shoko import ImportFolder, collect_shoko_discovery
from shoko_linker.sonarr import collect_sonarr_inventory
from shoko_linker.state import (
    META_SERVICE_AUTHORIZATION_REVISION,
    META_SERVICE_SETTINGS,
    BudgetLimits,
    MetadataOperation,
    MetadataOperationState,
    StateError,
    acquire_or_steal_apply_lock,
    open_state,
    release_apply_lock,
)


class RefreshStub(MetadataStub):
    def __init__(self, files: list[td.SpecFile]) -> None:
        super().__init__(files)
        self.refreshes: list[httpx.Request] = []
        self.fail_refresh = False

    def _respond(self, request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path.endswith("/Refresh"):
            self.refreshes.append(request)
            if self.fail_refresh:
                raise httpx.ReadTimeout("synthetic timeout", request=request)
            return httpx.Response(200, stream=httpx.ByteStream(b"false"), request=request)
        return super()._respond(request)


@dataclass
class Executor:
    deps: daemon.DaemonDeps
    shoko: RefreshStub
    sonarr: td.SonarrStub
    demand: MetadataDemand
    active: ActiveMappings
    folders: dict[int, ImportFolder]
    clock: list[float]

    def run(self, demand: MetadataDemand | None = None) -> daemon.MetadataExecutionOutcome:
        return daemon.process_metadata_demand(
            self.deps,
            demand or self.demand,
            td.ANIME_ID,
            self.active,
            self.folders,
            daemon._EligibilityFlags(None, None, True, True, True, True),
            "cycle-1",
            deadline=100.0,
        )

    def row(self) -> MetadataOperation | None:
        return self.deps.store.get_metadata_operation(
            metadata_source_key(self.deps.shoko), td.ANIME_ID
        )

    def revoke(self) -> None:
        self.deps.store.set_meta(
            META_SERVICE_SETTINGS, json.dumps({"allow_missing_metadata_refresh": False})
        )


@pytest.fixture
def executor(tmp_path: Path) -> Iterator[Executor]:
    file = td.SpecFile(series_id=1, file_id=10, shoko_id=500)
    td.activate_mapping(tmp_path, td.mapping_xml())
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [], td.T0)
    shoko = RefreshStub([file])
    shoko.queue_items = []
    sonarr = td.SonarrStub([file])
    clock = [0.0]
    deps = replace(
        td.build_deps(
            tmp_path,
            store=store,
            sonarr_files=[file],
            shoko_files=[file],
            sonarr_stub=sonarr,
            shoko=shoko,
            xml=td.mapping_xml(),
            monotonic=lambda: clock[0],
            advance=lambda _seconds: None,
        ),
        service_mode=True,
        service_authorized=lambda: True,
        now=lambda: td.T1,
    )
    assessment = to.safe_assessment()
    binding = replace(
        to.make_binding(assessment),
        sonarr_url=deps.settings.sonarr.url,
        shoko_url=deps.settings.shoko.url,
        roots=tuple(deps.settings.sonarr.allowed_roots),
        path_mappings=tuple(deps.settings.path_mappings),
        metadata_maintenance_policy=METADATA_MAINTENANCE_POLICY,
    )
    store.set_meta(META_SERVICE_SETTINGS, json.dumps({"allow_missing_metadata_refresh": True}))
    OnboardingManager(store).confirm_setup(
        binding,
        assessment,
        confirmed_facts=required_confirmation_facts(binding, assessment),
        automatic_link_consent=True,
        iso=td.T0,
    )
    active = load_active(deps.mapping_store)
    assert active is not None
    discovery = collect_shoko_discovery(deps.shoko)
    reconciled = daemon._reconcile(
        deps,
        collect_sonarr_inventory(deps.sonarr, deps.settings),
        discovery,
        active,
        td.T0,
        policy=RunPolicy.SERVICE,
    )
    demand = reconciled.metadata_demands[500]
    acquire_or_steal_apply_lock(store, deps.pid, td.T0, lease_expires_iso=td.T2)
    current = Executor(deps, shoko, sonarr, demand, active, dict(discovery.folders), clock)
    try:
        yield current
        assert store.connection.execute("SELECT COUNT(*) FROM sends").fetchone()[0] == 0
        assert store.get_service_canary() is None
    finally:
        store.close()
        for endpoint in (deps.shoko, deps.sonarr, deps.mapping_source):
            endpoint.client.close()


def test_queued_false_is_accepted_once_not_ready_and_file_read_is_last(executor: Executor) -> None:
    result = executor.run()
    assert result == daemon.MetadataExecutionOutcome("metadata_accepted_waiting", 1)
    row = executor.row()
    assert row is not None
    assert row.state is MetadataOperationState.ACCEPTED_WAITING
    assert row.required_targets == tuple(executor.demand.required_targets)
    assert row.reservation_owned
    assert not row.slot_released
    assert executor.run().reason == "metadata_operation_retained"
    assert len(executor.shoko.refreshes) == 1
    request = executor.shoko.refreshes[0]
    assert dict(request.url.params) == {
        "force": "false",
        "downloadRelations": "false",
        "createSeriesEntry": "true",
        "immediate": "false",
        "cacheOnly": "false",
        "skipTmdbUpdate": "true",
    }
    assert request.content == b""
    index = executor.shoko.requests.index(request)
    assert executor.shoko.requests[index - 1].url.path == "/api/v3/File/500"
    budget = executor.deps.store.budget_view()
    assert budget.pending_file_requests == budget.pending_episode_jobs == 1
    assert budget.cycle_new_submissions == 1
    assert executor.deps.store.live_send_counts() == (0, 0)


@pytest.mark.parametrize("boundary", ["source", "authorization", "queue", "targets", "file"])
def test_revocation_during_remote_checks_never_sends(executor: Executor, boundary: str) -> None:
    if boundary == "source":
        executor.sonarr.on_get = lambda _request: executor.revoke()
    elif boundary == "authorization":

        def revoke_authorization() -> bool:
            executor.revoke()
            return True

        executor.deps = replace(executor.deps, service_authorized=revoke_authorization)
    else:
        path = {
            "queue": "/api/v3/Queue/Items",
            "targets": f"/api/v3/Series/AniDB/{td.ANIME_ID}/Episode",
            "file": "/api/v3/File/500",
        }[boundary]
        executor.shoko.on_get = lambda observed: executor.revoke() if observed == path else None
    assert executor.run().attempted == 0
    assert executor.shoko.refreshes == []
    assert executor.row() is None


@pytest.mark.parametrize(
    "boundary",
    ["prepare_metadata_operation", "mark_metadata_possibly_delivered", "admit_metadata_transport"],
)
def test_revocation_at_durable_boundaries_never_sends(
    executor: Executor,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    original: Callable[..., Any] = getattr(executor.deps.store, boundary)
    expected = executor.deps.store.get_onboarding_json()

    def revoke_after(*args: Any, **kwargs: Any) -> Any:
        assert kwargs["expected_document_json"] == expected
        result = original(*args, **kwargs)
        executor.revoke()
        return result

    monkeypatch.setattr(executor.deps.store, boundary, revoke_after)
    assert executor.run().attempted == 0
    assert executor.shoko.refreshes == []
    row = executor.row()
    assert row is not None
    assert row.state is MetadataOperationState.DEFINITELY_REJECTED
    assert row.slot_released


@pytest.mark.parametrize(
    "change",
    [
        "off",
        "advanced",
        "shutdown",
        "deadline",
        "no_lock",
        "expired_lock",
        "mapping",
        "scope",
        "source",
        "hash",
        "path",
        "protected",
        "unknown_queue",
    ],
)
def test_closed_or_stale_inputs_never_send(executor: Executor, change: str) -> None:  # noqa: PLR0912
    demand = executor.demand
    if change == "off":
        executor.revoke()
    elif change == "advanced":
        executor.deps = replace(executor.deps, service_mode=False)
    elif change == "shutdown":
        executor.deps = replace(executor.deps, shutdown_requested=lambda: True)
    elif change == "deadline":
        executor.clock[0] = 100.0
    elif change == "no_lock":
        release_apply_lock(executor.deps.store, executor.deps.pid, td.T1)
    elif change == "expired_lock":
        executor.deps = replace(executor.deps, now=lambda: td.T2)
    elif change == "mapping":
        target = replace(demand.mapped_targets[0], anidb_episode_number=99)
        demand = replace(demand, mapped_targets=(target,))
        demand = replace(demand, fingerprint=metadata_demand_fingerprint(demand))
    elif change == "scope":
        demand = replace(demand, scope_fingerprint="e" * 64)
        demand = replace(demand, fingerprint=metadata_demand_fingerprint(demand))
    elif change == "source":
        executor.sonarr.series_tvdb[1] = 99999
    elif change == "hash":
        executor.shoko.ed2k_overrides[500] = "e" * 32
    elif change == "path":
        executor.shoko.size_overrides[500] = 1
    elif change == "protected":
        executor.shoko.link_unrelated(500)
    else:
        executor.shoko.queue_items_status = 403
    assert executor.run(demand).attempted == 0
    assert executor.shoko.refreshes == []
    assert executor.row() is None


def test_external_job_wait_then_adopt_first_own_request(executor: Executor) -> None:
    executor.shoko.queue_items = [
        qf.item(f"AniDB.GetAniDBAnimeJob_AnimeID:{td.ANIME_ID}_CacheOnly:true")
    ]
    assert executor.run().reason == "metadata_waiting_external_queue"
    external = executor.row()
    assert external is not None
    assert not external.reservation_owned
    assert executor.deps.store.budget_view().pending_file_requests == 0
    executor.shoko.queue_items = []
    assert executor.run().reason == "metadata_accepted_waiting"
    own = executor.row()
    assert own is not None
    assert own.claim != external.claim
    assert own.reservation_owned
    assert len(executor.shoko.refreshes) == 1


def test_same_file_manual_link_job_waits(executor: Executor) -> None:
    executor.shoko.queue_items = [
        qf.item("Import.ManualLinkJob_VideoLocalID:500_EpisodeID:999_Percentage:50")
    ]
    assert executor.run().reason == "metadata_waiting_external_file_queue"
    assert executor.shoko.refreshes == []


def test_metadata_now_usable_replans_without_refresh(executor: Executor) -> None:
    executor.shoko.missing_anime.clear()
    assert executor.run().reason == "metadata_replan"
    assert executor.shoko.refreshes == []


def test_second_hop_missing_is_retrieved(executor: Executor) -> None:
    executor.shoko.missing_anime.clear()
    executor.shoko.unmaterialized.add(td.ANIDB_EP)
    assert executor.run().reason == "metadata_accepted_waiting"
    assert len(executor.shoko.refreshes) == 1


def test_ambiguous_refresh_never_replays(executor: Executor) -> None:
    executor.shoko.fail_refresh = True
    assert executor.run() == daemon.MetadataExecutionOutcome("metadata_delivery_unknown", 1)
    row = executor.row()
    assert row is not None
    assert row.state is MetadataOperationState.POSSIBLY_DELIVERED
    assert not row.slot_released
    assert executor.run().reason == "metadata_operation_retained"
    assert len(executor.shoko.refreshes) == 1


def test_shared_budget_refusal(executor: Executor) -> None:
    executor.deps.store.budget_limits = BudgetLimits(0, 100, 10, 10, 168)
    assert executor.run().reason == "metadata_service_not_eligible"
    assert executor.row() is None
    assert executor.shoko.refreshes == []


def test_fake_persisted_lease_is_not_apply_ownership(executor: Executor) -> None:
    store = executor.deps.store
    release_apply_lock(store, executor.deps.pid, td.T1)
    store.acquire_apply_lock_tx(executor.deps.pid, td.T1, td.T2)
    assert not store.owns_apply_ownership(executor.deps.pid, td.T1)
    assert executor.run().attempted == 0
    assert executor.shoko.refreshes == []


def _reconcile(executor: Executor, file_id: int = 500) -> MetadataDemand:
    result = daemon._reconcile(
        executor.deps,
        collect_sonarr_inventory(executor.deps.sonarr, executor.deps.settings),
        collect_shoko_discovery(executor.deps.shoko),
        executor.active,
        td.T1,
        policy=RunPolicy.SERVICE,
    )
    return result.metadata_demands[file_id]


def test_two_files_same_anime_share_one_request(executor: Executor) -> None:
    second = td.SpecFile(series_id=1, file_id=11, shoko_id=501)
    executor.sonarr.files.append(second)
    executor.shoko.files[501] = second
    other = _reconcile(executor, 501)
    assert other.fingerprint != executor.demand.fingerprint
    assert executor.run().attempted == 1
    assert executor.run(other).reason == "metadata_operation_retained"
    assert len(executor.shoko.refreshes) == 1


@pytest.mark.parametrize("failure", [False, True])
def test_split_anime_complete_coverage_and_other_target_failure_suppresses_send(
    executor: Executor,
    monkeypatch: pytest.MonkeyPatch,
    failure: bool,
) -> None:
    other_anime = td.ANIME_ID + 1
    targets = (_target(td.ANIME_ID, 1), _target(other_anime, 1))
    answer = Resolution(ResolutionStatus.RESOLVED, targets[0], targets, "verified split fixture")
    monkeypatch.setattr(daemon, "resolve_mapping", lambda *_args: answer)
    executor.shoko.missing_anime.add(other_anime)
    second_file = td.SpecFile(series_id=1, file_id=11, shoko_id=501)
    executor.sonarr.files.append(second_file)
    executor.shoko.files[501] = second_file
    executor.demand = _reconcile(executor, 501)
    assert len(executor.demand.required_targets) == 2
    if failure:
        path = f"/api/v3/Series/AniDB/{other_anime}/Episode"
        executor.shoko.target_read_counts.clear()
        executor.shoko.target_read_failures[path] = (1, 503)
        assert executor.run().reason == "metadata_targets_unavailable"
        assert executor.row() is None
        assert executor.shoko.refreshes == []
    else:
        assert executor.run().reason == "metadata_accepted_waiting"
        row = executor.row()
        assert row is not None
        assert row.required_targets == ((td.ANIME_ID, EpisodeType.EPISODE, 1),)
        assert row.missing_targets == row.required_targets


def test_ready_coverage_allows_new_episode_then_remembers_history(executor: Executor) -> None:
    assert executor.run().attempted == 1
    first = executor.row()
    assert first is not None
    store = executor.deps.store
    store.record_metadata_operation(
        first,
        MetadataOperationState.READY,
        iso=td.T1,
        verified_required_targets=first.required_targets,
    )
    replacement = td.SpecFile(
        series_id=1, file_id=11, shoko_id=501, episodes=((1, 2, td.ANIDB_EP + 1, td.SHOKO_EP + 1),)
    )
    executor.sonarr.files.append(replacement)
    executor.shoko.files[501] = replacement
    executor.demand = _reconcile(executor, 501)
    assert executor.run().reason == "metadata_accepted_waiting"
    second = executor.row()
    assert second is not None
    assert second.claim != first.claim
    store.record_metadata_operation(
        second,
        MetadataOperationState.READY,
        iso=td.T1,
        verified_required_targets=second.required_targets,
    )
    old_episode = td.SpecFile(series_id=1, file_id=12, shoko_id=502)
    executor.sonarr.files.append(old_episode)
    executor.shoko.files[502] = old_episode
    executor.demand = _reconcile(executor, 502)
    assert executor.run().reason == "metadata_operation_exists"
    assert len(executor.shoko.refreshes) == 2


@pytest.mark.parametrize(
    "boundary",
    ["prepare_metadata_operation", "mark_metadata_possibly_delivered", "admit_metadata_transport"],
)
@pytest.mark.parametrize("change", ["revision", "shutdown", "ownership", "write_stop"])
def test_final_local_changes_and_write_stop_fence_every_boundary(
    executor: Executor,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
    change: str,
) -> None:
    store = executor.deps.store
    original: Callable[..., Any] = getattr(store, boundary)
    stopped = [False]
    executor.deps = replace(executor.deps, shutdown_requested=lambda: stopped[0])

    def change_after(*args: Any, **kwargs: Any) -> Any:
        result = original(*args, **kwargs)
        if change == "revision":
            # Same enabled preference/consent after an ABA change still cannot authorize.
            store.set_meta(META_SERVICE_AUTHORIZATION_REVISION, "1" * 32)
        elif change == "shutdown":
            stopped[0] = True
        elif change == "ownership":
            release_apply_lock(store, executor.deps.pid, td.T1)
        else:
            store.disable_writes("synthetic stop before metadata transport")
        return result

    monkeypatch.setattr(store, boundary, change_after)
    assert executor.run().attempted == 0
    assert executor.shoko.refreshes == []
    row = executor.row()
    assert row is not None
    if change == "write_stop":
        assert store.writes_disabled
        assert not row.slot_released  # stop prevents recording; recovery owns cleanup
    else:
        assert row.slot_released


def test_deadline_not_sent_releases_only_proven_zero_delivery(
    executor: Executor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def not_sent(*args: Any, **kwargs: Any) -> MetadataRefreshResult:
        return MetadataRefreshResult(
            MetadataRefreshOutcome.NOT_SENT, MetadataRefreshReason.DEADLINE
        )

    monkeypatch.setattr(daemon, "submit_metadata_refresh", not_sent)
    assert executor.run().reason == "metadata_not_sent"
    row = executor.row()
    assert row is not None
    assert row.state is MetadataOperationState.DEFINITELY_REJECTED
    assert row.slot_released
    assert executor.shoko.refreshes == []


def test_result_persistence_failure_retains_uncertainty_and_never_replays(
    executor: Executor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*args: Any, **kwargs: Any) -> MetadataOperation:
        raise StateError("synthetic persistence failure")

    monkeypatch.setattr(executor.deps.store, "record_metadata_operation", fail)
    assert executor.run().attempted == 1
    assert executor.run().reason == "metadata_operation_retained"
    row = executor.row()
    assert row is not None
    assert row.state is MetadataOperationState.POSSIBLY_DELIVERED
    assert not row.slot_released
    assert len(executor.shoko.refreshes) == 1


def test_other_handle_claim_race_never_steals_transport(
    executor: Executor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = executor.deps.store
    original = store.prepare_metadata_operation
    with open_state(store.path, budget_limits=td.WIDE_LIMITS) as rival:

        def raced(*args: Any, **kwargs: Any) -> MetadataOperation | Any:
            rival.prepare_metadata_operation(*args, **kwargs)
            return original(*args, **kwargs)

        monkeypatch.setattr(store, "prepare_metadata_operation", raced)
        assert executor.run().reason == "metadata_operation_exists"
    assert executor.shoko.refreshes == []
    row = executor.row()
    assert row is not None
    assert row.state is MetadataOperationState.PREPARED
    assert not row.transport_admitted


@pytest.mark.parametrize("change", ["invalid_source", "generation", "review_hold", "queue_blocked"])
def test_changes_during_final_reads_reject_the_request(executor: Executor, change: str) -> None:
    def changed(path: str) -> None:
        if path != "/api/v3/File/500":
            return
        if change == "invalid_source":
            executor.deps.mapping_store.invalid_source_hold_path.write_text(
                "held", encoding="utf-8"
            )
        elif change == "generation":
            executor.deps.mapping_store.active_path.write_text("changed", encoding="utf-8")
        else:
            executor.deps.store.pause_service_for_review("setup_changed", td.T1)

    if change == "queue_blocked":
        executor.shoko.queue_items = [qf.item("Other.BlockedJob", "blocked")]
    else:
        executor.shoko.on_get = changed
    assert executor.run().attempted == 0
    assert executor.shoko.refreshes == []
    assert executor.row() is None


def test_known_anidb_episode_identity_drift_defers_metadata(executor: Executor) -> None:
    executor.shoko.missing_anime.clear()
    executor.shoko.unmaterialized.add(td.ANIDB_EP)
    executor.demand = _reconcile(executor)
    assert executor.demand.needs[0].known_anidb_episode_id == td.ANIDB_EP
    executor.shoko.files[500] = replace(
        executor.shoko.files[500], episodes=((1, 1, td.ANIDB_EP + 1, td.SHOKO_EP + 1),)
    )
    executor.shoko.unmaterialized.add(td.ANIDB_EP + 1)
    assert executor.run().reason == "metadata_targets_unavailable"
    assert executor.row() is None
    assert executor.shoko.refreshes == []


def test_override_configuration_must_match_the_activated_generation(executor: Executor) -> None:
    executor.deps = replace(executor.deps, overrides_text="not: a valid override document")
    assert executor.run().attempted == 0
    assert executor.shoko.refreshes == []
    assert executor.row() is None


@pytest.mark.parametrize("expired", [False, True])
def test_final_file_preflight_uses_metadata_deadline_before_reserving(
    executor: Executor, monkeypatch: pytest.MonkeyPatch, expired: bool
) -> None:
    bodies: list[DripFileBody] = []
    final_requests: list[httpx.Request] = []

    def bounded_preflight(*args: Any, **kwargs: Any) -> Any:
        endpoint = args[0]
        assert kwargs["deadline"] == 100.0
        assert kwargs["clock"] is executor.deps.monotonic
        executor.clock[0] = 100.0 if expired else 99.0

        def handler(request: httpx.Request) -> httpx.Response:
            final_requests.append(request)
            response = executor.shoko.handler(request)
            body = DripFileBody(response.content, executor.clock)
            bodies.append(body)
            return httpx.Response(200, stream=body, request=request)

        with httpx.Client(
            base_url=endpoint.base_url, transport=httpx.MockTransport(handler), timeout=30.0
        ) as client:
            return original_file_preflight(
                type(endpoint)(client, endpoint.headers_factory), *args[1:], **kwargs
            )

    monkeypatch.setattr(daemon, "preflight_file", bounded_preflight)
    result = executor.run()
    assert result.reason == "metadata_file_preflight_refused"
    assert result.attempted == 0
    assert executor.row() is None
    assert executor.shoko.refreshes == []
    assert executor.deps.store.budget_view().pending_file_requests == 0
    assert executor.deps.store.budget_view().rolling_new_submissions == 0
    if expired:
        assert final_requests == []
    else:
        assert len(final_requests) == 1
        assert final_requests[0].extensions["timeout"]["read"] == 1.0
        assert executor.clock[0] == 100.0
        assert bodies[0].chunks == 2
        assert bodies[0].closed


def test_direct_metadata_executor_binds_its_explicit_authorization_deadline(
    executor: Executor,
) -> None:
    deadlines: list[float] = []

    def authorize(deadline: float) -> bool:
        deadlines.append(deadline)
        executor.clock[0] = deadline
        return True

    executor.deps = replace(executor.deps, service_authorized_for_deadline=authorize)
    result = executor.run()
    assert deadlines == [100.0]
    assert result.attempted == 0
    assert executor.shoko.refreshes == []
    assert executor.row() is None
    assert executor.deps.store.budget_view().pending_episode_jobs == 0


@pytest.mark.parametrize("change", ["cycle", "server", "deadline", "demand"])
def test_empty_queue_admission_cannot_be_reused_outside_its_operation(
    executor: Executor, change: str
) -> None:
    admission = daemon._metadata_queue_admission(
        executor.deps, {td.ANIME_ID: executor.demand}, "cycle-1", 100.0
    )
    assert isinstance(admission, daemon._MetadataBatchAdmission)
    if change == "cycle":
        admission = replace(admission, cycle_key="another-cycle")
    elif change == "server":
        admission = replace(admission, source_key="another-server")
    elif change == "deadline":
        admission = replace(admission, deadline=200.0)
    else:
        admission = replace(admission, demands=frozenset())
    outcome = daemon.process_metadata_demand(
        executor.deps,
        executor.demand,
        td.ANIME_ID,
        executor.active,
        executor.folders,
        daemon._EligibilityFlags(None, None, True, True, True, True),
        "cycle-1",
        deadline=100.0,
        _batch=admission,
    )
    assert outcome.reason == "metadata_authorization_or_demand_changed"
    assert outcome.attempted == 0
    assert executor.shoko.refreshes == []
    assert executor.row() is None
