"""Offline normal-daemon metadata scheduling, shared budgets and recovery."""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import httpx
import pytest
import queue_fixtures as qf
import test_daemon as td
import test_onboarding as to
from test_metadata_executor import RefreshStub
from test_metadata_planning import _target

from shoko_linker import daemon
from shoko_linker.linking import QueueSnapshot, fetch_queue
from shoko_linker.mapping_store import load_active
from shoko_linker.mappings import Resolution, ResolutionStatus
from shoko_linker.metadata_operations import metadata_source_key
from shoko_linker.metadata_refresh import (
    MetadataRefreshOutcome,
    MetadataRefreshReason,
    MetadataRefreshResult,
    submit_metadata_refresh,
)
from shoko_linker.onboarding import (
    METADATA_MAINTENANCE_POLICY,
    OnboardingManager,
    required_confirmation_facts,
)
from shoko_linker.policies import RunPolicy
from shoko_linker.queue_inventory import fetch_queue_inventory
from shoko_linker.rate_control import RateState, decode_rate_state, encode_rate_state
from shoko_linker.shoko import collect_import_folders
from shoko_linker.state import (
    META_SERVICE_SETTINGS,
    BudgetLimits,
    FileState,
    MetadataOperation,
    MetadataOperationState,
    RecoveryBatch,
    StateError,
    StateStore,
    open_state,
)


@dataclass
class Service:
    deps: daemon.DaemonDeps
    shoko: RefreshStub
    sonarr: td.SonarrStub
    clock: list[float]

    def cycle(self, key: str, *, full: bool = True) -> daemon.CycleReport:
        return daemon.run_cycle(
            self.deps,
            now_iso=td.T1,
            mode=daemon.CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key=key,
            full=full,
        )


@pytest.fixture
def service(tmp_path: Path) -> Iterator[Service]:
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
            max_submissions=20,
            now=lambda: td.T1,
        ),
        service_mode=True,
        service_authorized=lambda: True,
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
    current = Service(deps, shoko, sonarr, clock)
    try:
        yield current
    finally:
        current.deps.store.close()
        for endpoint in (deps.shoko, deps.sonarr, deps.mapping_source):
            endpoint.client.close()


def test_missing_only_bootstraps_one_then_recovers_without_replay(service: Service) -> None:
    first = service.cycle("first")
    assert first.reconciliation_ready
    assert first.apply_eligible
    assert first.metadata_requests_attempted == 1
    assert first.counts.submitted == first.total_link_requests_attempted == 0
    assert first.counts.pending == 1
    assert service.deps.store.get_service_canary() is None
    rate = decode_rate_state(service.deps.store.get_service_rate_state_json() or "")
    assert rate.cap == 1
    assert rate.successful_streak == 0
    assert len(service.shoko.refreshes) == 1
    second = service.cycle("second")
    assert second.metadata_requests_attempted == 0
    assert len(service.shoko.refreshes) == 1
    assert any(reason.startswith("metadata_recovery:") for reason in second.reasons)
    # Restart the SQLite handle; uncertainty stays a verification operation.
    old = service.deps.store
    path = old.path
    limits = old.budget_limits
    old.close()
    service.deps = replace(service.deps, store=StateStore(path, budget_limits=limits))
    restarted = service.cycle("restart")
    assert restarted.metadata_requests_attempted == 0
    assert len(service.shoko.refreshes) == 1


def test_arrival_replans_complete_file_and_links_without_metadata_success_credit(
    service: Service,
) -> None:
    service.cycle("missing")
    service.shoko.missing_anime.clear()
    report = service.cycle("arrived")
    assert report.metadata_requests_attempted == 0
    assert report.counts.submitted == report.total_link_requests_attempted == 1
    assert report.counts.pending == 0
    assert service.deps.store.get_service_canary() == (500, "verified")
    record = service.deps.store.get_file(500)
    assert record is not None
    assert record.state is FileState.VERIFIED
    assert len(service.shoko.refreshes) == 1


@pytest.mark.parametrize("queue", ["unknown", "foreign"])
def test_unknown_or_foreign_queue_refuses_new_metadata(service: Service, queue: str) -> None:
    if queue == "unknown":
        service.shoko.queue_items_status = 403
    else:
        service.shoko.queue_items = [qf.item(f"AniDB.GetAniDBAnimeJob_AnimeID:{td.ANIME_ID}")]
    report = service.cycle("blocked")
    assert report.metadata_requests_attempted == 0
    assert service.shoko.refreshes == []
    assert any(reason.startswith("metadata_") for reason in report.reasons)


def test_off_still_replans_after_manual_metadata_arrives(service: Service) -> None:
    service.deps.store.set_meta(
        META_SERVICE_SETTINGS, json.dumps({"allow_missing_metadata_refresh": False})
    )
    report = service.cycle("off")
    assert report.metadata_observations == ((0, "metadata_disabled"),)
    assert report.counts.pending == 1
    assert not service.shoko.refreshes
    service.shoko.missing_anime.clear()
    report = service.cycle("manual-arrival")
    assert report.counts.submitted == 1
    assert report.metadata_requests_attempted == 0


def test_disabled_still_recovers_accepted_requests(service: Service) -> None:
    service.cycle("accepted")
    service.deps.store.set_meta(
        META_SERVICE_SETTINGS, json.dumps({"allow_missing_metadata_refresh": False})
    )
    service.shoko.missing_anime.clear()
    report = service.cycle("off-after-acceptance")
    assert any(reason.endswith(":ready") for reason in report.reasons)
    assert len(service.shoko.refreshes) == 1
    row = service.deps.store.get_metadata_operation(
        metadata_source_key(service.deps.shoko), td.ANIME_ID
    )
    assert row is not None
    assert row.slot_released


def _add_anime(
    service: Service, tmp_path: Path, *, missing: bool = True, anime: int = 2
) -> td.SpecFile:
    other = td.SpecFile(
        series_id=anime,
        file_id=anime * 10,
        shoko_id=499 + anime,
        anime_id=anime,
        tvdb_series=anime,
        episodes=((1, 1, 20001, 30001),),
    )
    td.activate_mapping(
        tmp_path,
        td.mapping_xml_mapped(
            *((f.tvdb_series, f.anime_id) for f in [*service.sonarr.files[1:], other])
        ),
    )
    service.sonarr.files.append(other)
    service.sonarr.series_ids.append(anime)
    service.sonarr.series_tvdb[anime] = anime
    service.shoko.files[other.shoko_id] = other
    if missing:
        service.shoko.missing_anime.add(anime)
    return other


def test_ready_links_run_before_metadata_with_separate_throughput_cap(
    service: Service, tmp_path: Path
) -> None:
    _add_anime(service, tmp_path, missing=False)
    first = service.cycle("ready-first")
    assert first.counts.submitted == 1
    assert first.metadata_requests_attempted == 1
    posts = [r.url.path for r in service.shoko.requests if r.method == "POST"]
    assert posts == ["/api/v3/File/501/Link", f"/api/v3/Series/AniDB/{td.ANIME_ID}/Refresh"]
    assert service.deps.store.get_service_canary() == (501, "verified")
    second = service.cycle("metadata-next")
    assert second.counts.submitted == 0
    assert second.metadata_requests_attempted == 0


def test_two_missing_anime_share_configured_cap_without_canary(
    service: Service, tmp_path: Path
) -> None:
    _add_anime(service, tmp_path)
    first = service.cycle("first-anime")
    assert first.metadata_requests_attempted == 2
    assert len(service.shoko.refreshes) == 2
    second = service.cycle("second-anime")
    assert second.metadata_requests_attempted == 0
    assert {request.url.path for request in service.shoko.refreshes} == {
        f"/api/v3/Series/AniDB/{td.ANIME_ID}/Refresh",
        "/api/v3/Series/AniDB/2/Refresh",
    }
    third = service.cycle("both-pending")
    assert third.metadata_requests_attempted == 0
    assert len(service.shoko.refreshes) == 2


def test_duplicate_files_for_one_anime_make_one_request(service: Service) -> None:
    other = td.SpecFile(series_id=1, file_id=20, shoko_id=501)
    service.sonarr.files.append(other)
    service.shoko.files[501] = other
    report = service.cycle("same-anime")
    assert report.counts.pending == 2
    assert report.metadata_requests_attempted == 1
    assert len(service.shoko.refreshes) == 1


@pytest.mark.parametrize("status", ["pending", "failed"])
def test_existing_canary_holds_metadata(service: Service, tmp_path: Path, status: str) -> None:
    _add_anime(service, tmp_path, missing=False)
    service.shoko.link_on_post = False
    service.deps.store.save_service_rate_state_json(
        encode_rate_state(RateState(2, 0, td.T1, "test")), td.T1
    )
    first = service.cycle("canary-send")
    assert first.counts.submitted == 1
    assert first.metadata_requests_attempted == 0
    assert service.deps.store.get_service_canary() == (501, "pending")
    if status == "failed":
        service.deps.store.set_service_canary_status("failed", td.T1)
    report = service.cycle("canary-hold")
    assert report.metadata_requests_attempted == 0
    assert f"service_canary_{status}" in report.reasons
    assert service.shoko.refreshes == []


def test_light_cycle_uses_half_of_actual_shared_cap(service: Service, tmp_path: Path) -> None:
    _add_anime(service, tmp_path)
    service.deps = replace(service.deps, max_submissions=2)
    service.deps.store.save_service_rate_state_json(
        encode_rate_state(RateState(2, 0, td.T1, "test")), td.T1
    )
    report = service.cycle("light", full=False)
    assert report.metadata_requests_attempted == 1
    assert len(service.shoko.refreshes) == 1


@pytest.mark.parametrize("hold", ["pause", "shutdown", "deadline", "unauthorized"])
def test_current_holds_make_no_metadata_request(service: Service, hold: str) -> None:
    if hold == "pause":
        service.deps.store.set_service_pause(True, "synthetic user pause", td.T1)
    elif hold == "shutdown":
        service.deps = replace(service.deps, shutdown_requested=lambda: True)
    elif hold == "deadline":
        service.deps = replace(service.deps, max_cycle_seconds=0.0)
    else:
        service.deps = replace(service.deps, service_authorized=lambda: False)
    report = service.cycle("held")
    assert report.metadata_requests_attempted == 0
    assert service.shoko.refreshes == []


def test_ready_link_and_metadata_share_two_total_requests(service: Service, tmp_path: Path) -> None:
    _add_anime(service, tmp_path, missing=False)
    service.deps.store.save_service_rate_state_json(
        encode_rate_state(RateState(2, 0, td.T1, "test")), td.T1
    )
    report = service.cycle("shared-two")
    assert report.counts.submitted == 1
    assert report.metadata_requests_attempted == 1
    posts = [request.url.path for request in service.shoko.requests if request.method == "POST"]
    assert posts == ["/api/v3/File/501/Link", f"/api/v3/Series/AniDB/{td.ANIME_ID}/Refresh"]
    assert report.total_link_requests_attempted == 1


@pytest.mark.parametrize("limit", ["cycle", "pending", "rolling"])
def test_durable_budgets_stop_metadata_without_second_lane_allowance(
    service: Service, limit: str
) -> None:
    values = {"per_cycle_new": 20, "max_pending_files": 20, "rolling_new": 20}
    values[
        {"cycle": "per_cycle_new", "pending": "max_pending_files", "rolling": "rolling_new"}[limit]
    ] = 0
    service.deps.store.budget_limits = BudgetLimits(
        max_pending_episode_jobs=10,
        rolling_window_hours=168,
        max_pending_files=values["max_pending_files"],
        per_cycle_new=values["per_cycle_new"],
        rolling_new=values["rolling_new"],
    )
    report = service.cycle("budget-stop")
    assert report.metadata_requests_attempted == 0
    assert service.shoko.refreshes == []


def test_file_and_metadata_recovery_share_one_row_and_alternate(service: Service) -> None:
    service.cycle("accepted")
    td._seed_verifying_rows(service.deps.store, 1)
    service.deps = replace(service.deps, max_recovery_rows_per_cycle=1)
    first = daemon._recovery_phase(service.deps, td.T1, 0.0, full=False)
    second = daemon._recovery_phase(service.deps, td.T1, 0.0, full=False)
    assert sum(reason.startswith("metadata_recovery:") for reason in first) == 1
    assert "recovery_deferred:1" in first
    assert sum(reason.startswith("recovery_poll") for reason in second) == 1
    assert "metadata_recovery_deferred:1" in second
    assert not any(reason.startswith("metadata_recovery:") for reason in second)
    assert len(service.shoko.refreshes) == 1


def test_expired_recovery_deadline_neither_lane_processes_row(service: Service) -> None:
    service.cycle("accepted")
    td._seed_verifying_rows(service.deps.store, 1)
    service.deps = replace(service.deps, max_cycle_seconds=1)
    service.clock[0] = 1.0
    before = len(service.shoko.requests)
    reasons = daemon._recovery_phase(service.deps, td.T1, 0.0)
    assert "recovery_deferred:1" in reasons
    assert "metadata_recovery_deferred:1" in reasons
    assert len(service.shoko.requests) == before


def test_split_file_requests_each_anime_once_with_complete_frozen_demand(
    service: Service, monkeypatch: pytest.MonkeyPatch
) -> None:
    targets = (_target(td.ANIME_ID, 1), _target(2, 1))
    resolved = Resolution(ResolutionStatus.RESOLVED, targets[0], targets, "verified split fixture")
    monkeypatch.setattr(daemon, "resolve_mapping", lambda *_args: resolved)
    service.shoko.missing_anime.add(2)
    first = service.cycle("split-first")
    second = service.cycle("split-second")
    assert first.counts.pending == second.counts.pending == 1
    assert first.metadata_requests_attempted == 2
    assert second.metadata_requests_attempted == 0
    for anime in (2, td.ANIME_ID):
        row = service.deps.store.get_metadata_operation(
            metadata_source_key(service.deps.shoko), anime
        )
        assert row is not None
        assert row.required_targets is not None
        assert len(row.required_targets) == 1
        assert row.required_targets[0][0] == anime
    service.cycle("split-no-replay")
    assert len(service.shoko.refreshes) == 2


def test_foreign_first_anime_defers_entire_metadata_batch(service: Service, tmp_path: Path) -> None:
    _add_anime(service, tmp_path)
    service.shoko.queue_items = [qf.item("AniDB.GetAniDBAnimeJob_AnimeID:2")]
    first = service.cycle("foreign-first")
    assert first.metadata_requests_attempted == 0
    assert first.metadata_observations == (
        (2, "metadata_queue_busy"),
        (td.ANIME_ID, "metadata_queue_busy"),
    )
    assert service.shoko.refreshes == []


def test_no_canary_does_not_clamp_metadata_even_with_saved_rate(
    service: Service, tmp_path: Path
) -> None:
    _add_anime(service, tmp_path)
    service.deps.store.save_service_rate_state_json(
        encode_rate_state(RateState(20, 0, td.T1, "test")), td.T1
    )
    report = service.cycle("no-canary-initial-ceiling")
    assert report.metadata_requests_attempted == 2
    assert len(service.shoko.refreshes) == 2
    assert service.deps.store.get_service_canary() is None


def test_incomplete_source_snapshot_never_schedules_metadata(service: Service) -> None:
    service.sonarr.files_status[1] = 503
    report = service.cycle("source-unavailable")
    assert not report.reconciliation_ready
    assert report.metadata_requests_attempted == 0
    assert service.shoko.refreshes == []


def _seed_healthy_rate(service: Service) -> None:
    service.deps.store.save_service_rate_state_json(
        encode_rate_state(RateState(2, 1, td.T1, "healthy")), td.T1
    )


def test_metadata_queue_permission_loss_after_verified_file_reduces_rate(
    service: Service,
    tmp_path: Path,
) -> None:
    _add_anime(service, tmp_path, missing=False)
    _seed_healthy_rate(service)
    service.shoko.on_post = lambda: setattr(service.shoko, "queue_items_status", 403)
    report = service.cycle("metadata-queue-permission-loss")
    assert report.counts.submitted == 1
    assert report.counts.verified == 1
    assert (td.ANIME_ID, "metadata_queue_not_clear") in report.metadata_observations
    assert report.metadata_requests_attempted == 0
    rate = decode_rate_state(service.deps.store.get_service_rate_state_json() or "")
    assert rate.cap == 1
    assert rate.successful_streak == 0
    assert rate.last_reason == "api_failure"


@pytest.mark.parametrize("failure", ["transport", "persistence"])
def test_uncertain_metadata_after_verified_file_never_gains_rate_credit(
    service: Service,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    _add_anime(service, tmp_path, missing=False)
    _seed_healthy_rate(service)
    if failure == "transport":
        service.shoko.fail_refresh = True
        expected = "metadata_delivery_unknown"
    else:

        def fail_record(*_args: Any, **_kwargs: Any) -> MetadataOperation:
            raise StateError("synthetic persistence failure after HTTP")

        monkeypatch.setattr(service.deps.store, "record_metadata_operation", fail_record)
        expected = "metadata_state_or_guard_unavailable"
    report = service.cycle("uncertain-metadata")
    assert report.counts.submitted == report.counts.verified == 1
    assert report.metadata_requests_attempted == 1
    assert (td.ANIME_ID, expected) in report.metadata_observations
    rate = decode_rate_state(service.deps.store.get_service_rate_state_json() or "")
    assert rate.cap == 1
    assert rate.successful_streak == 0
    assert rate.last_reason == "verification_pending"


def test_normal_external_metadata_queue_wait_preserves_real_file_success(
    service: Service,
    tmp_path: Path,
) -> None:
    _add_anime(service, tmp_path, missing=False)
    _seed_healthy_rate(service)
    service.shoko.queue_items = [qf.item(f"AniDB.GetAniDBAnimeJob_AnimeID:{td.ANIME_ID}")]
    report = service.cycle("normal-external-queue-wait")
    assert report.counts.submitted == report.counts.verified == 1
    assert report.metadata_requests_attempted == 0
    assert (td.ANIME_ID, "metadata_waiting_external_queue") in report.metadata_observations
    rate = decode_rate_state(service.deps.store.get_service_rate_state_json() or "")
    assert rate.cap == 4
    assert rate.successful_streak == 2
    assert rate.last_reason == "healthy"


@pytest.mark.parametrize(
    "reason",
    [
        "metadata_recovery:1234:queue_unknown",
        "metadata_recovery:1234:state_unavailable",
        "metadata_recovery:1234:needs_attention",
        "metadata_recovery:1234:invalid_coverage",
        "metadata_future_unclassified_uncertainty",
        "metadata_authorization_changed",
        "metadata_generation_or_overrides_changed",
        "metadata_queue_unavailable_or_blocked",
    ],
)
def test_metadata_uncertainty_holds_feedback_even_without_owned_reservation(
    service: Service,
    reason: str,
) -> None:
    updated = daemon._update_service_rate_state(
        service.deps,
        RateState(2, 1, td.T1, "healthy"),
        now_iso=td.T1,
        queue=fetch_queue(service.deps.shoko),
        submitted=1,
        verified=1,
        reasons=(reason,),
        drift=(),
    )
    assert updated.cap == 1
    assert updated.successful_streak == 0
    assert updated.last_reason != "healthy"


@pytest.mark.parametrize(
    "reason",
    [
        "metadata_disabled",
        "metadata_waiting_external_queue",
        "metadata_operation_exists",
        "metadata_service_not_eligible",
        "metadata_cycle_new_exhausted",
        "metadata_rolling_new_exhausted",
        "metadata_pending_files_exhausted",
        "metadata_pending_episode_jobs_exhausted",
        "metadata_recovery:1234:waiting_queue",
        "metadata_recovery:1234:waiting_metadata",
        "metadata_recovery:1234:deadline",
        "metadata_recovery_deferred:1",
        "metadata_not_sent",
    ],
)
def test_normal_metadata_waits_do_not_mark_real_file_success_as_failure(
    service: Service,
    reason: str,
) -> None:
    updated = daemon._update_service_rate_state(
        service.deps,
        RateState(2, 1, td.T1, "healthy"),
        now_iso=td.T1,
        queue=fetch_queue(service.deps.shoko),
        submitted=1,
        verified=1,
        reasons=(reason,),
        drift=(),
    )
    assert updated.cap == 4
    assert updated.successful_streak == 2
    assert updated.last_reason == "healthy"


def test_metadata_ready_does_not_supply_file_success_or_canary_credit(service: Service) -> None:
    updated = daemon._update_service_rate_state(
        service.deps,
        RateState(1, 0, td.T1, "initial"),
        now_iso=td.T1,
        queue=fetch_queue(service.deps.shoko),
        submitted=0,
        verified=0,
        reasons=("metadata_recovery:1234:ready",),
        drift=(),
    )
    assert updated.cap == 1
    assert updated.successful_streak == 0
    assert updated.last_reason == "initial"


def test_observed_external_file_job_wait_is_not_unknown_queue_failure(
    service: Service,
    tmp_path: Path,
) -> None:
    _add_anime(service, tmp_path, missing=False)
    _seed_healthy_rate(service)
    service.shoko.queue_items = [qf.item("Import.ManualLinkJob_VideoLocalID:500_EpisodeID:999")]
    report = service.cycle("normal-external-file-wait")
    assert report.counts.submitted == report.counts.verified == 1
    assert report.metadata_requests_attempted == 0
    assert (td.ANIME_ID, "metadata_waiting_external_file_queue") in report.metadata_observations
    rate = decode_rate_state(service.deps.store.get_service_rate_state_json() or "")
    assert rate.cap == 4
    assert rate.successful_streak == 2
    assert rate.last_reason == "healthy"


@pytest.mark.parametrize("expired", [False, True])
def test_coordinator_no_allowance_decodes_neither_lane(
    service: Service, monkeypatch: pytest.MonkeyPatch, expired: bool
) -> None:
    service.cycle("accepted")
    td._seed_verifying_rows(service.deps.store, 2)
    service.deps = replace(
        service.deps, max_cycle_seconds=1, max_recovery_rows_per_cycle=10 if expired else 0
    )
    service.clock[0] = 1.0 if expired else 0.0
    before = len(service.shoko.requests)

    def forbidden(*_args: object) -> None:
        pytest.fail("no allowance must not decode persisted rows")

    monkeypatch.setattr(service.deps.store, "get_file", forbidden)
    monkeypatch.setattr(service.deps.store, "get_metadata_operation", forbidden)
    reasons = daemon._recovery_phase(service.deps, td.T1, 0.0)
    assert "recovery_deferred:2" in reasons
    assert "metadata_recovery_deferred:1" in reasons
    assert len(service.shoko.requests) == before


def test_file_corruption_and_terminal_history_do_not_starve_bounded_recovery(
    service: Service, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = service.deps.store
    td._seed_verifying_rows(store, 4)
    store.connection.execute("UPDATE files SET sonarr_size='broken' WHERE shoko_file_id=1")
    store.connection.execute("UPDATE files SET state='verified' WHERE shoko_file_id=4")
    original = store.get_file
    decoded: list[int] = []

    def tracked(file_id: int) -> Any:
        decoded.append(file_id)
        return original(file_id)

    monkeypatch.setattr(store, "get_file", tracked)
    before = len(service.shoko.requests)
    first, processed = daemon._file_recovery_phase(service.deps, td.T1, 0.0, max_rows=1)
    assert first == ("recovery_invalid:1", "recovery_deferred:2")
    assert processed == 1
    assert decoded == [1]
    decoded.clear()
    second, processed = daemon._file_recovery_phase(service.deps, td.T1, 0.0, max_rows=1)
    assert processed == 1
    assert decoded
    assert all(file_id == 2 for file_id in decoded)
    assert any(reason.startswith("recovery_poll") for reason in second)
    assert all(request.method == "GET" for request in service.shoko.requests[before:])


def test_file_recovery_checks_deadline_after_decoding_before_http(
    service: Service, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = service.deps.store
    td._seed_verifying_rows(store, 1)
    service.deps = replace(service.deps, max_cycle_seconds=1)
    original = store.get_file

    def slow_decode(file_id: int) -> Any:
        row = original(file_id)
        service.clock[0] = 1.0
        return row

    monkeypatch.setattr(store, "get_file", slow_decode)
    before = len(service.shoko.requests)
    reasons, processed = daemon._file_recovery_phase(service.deps, td.T1, 0.0, max_rows=1)
    assert processed == 1
    assert reasons == ("recovery_deadline:1",)
    assert len(service.shoko.requests) == before
    assert store.active_send_details(1) is not None


@pytest.mark.parametrize("removed", [False, True])
def test_concurrent_metadata_settlement_still_consumes_shared_one_row_allowance(
    service: Service, monkeypatch: pytest.MonkeyPatch, removed: bool
) -> None:
    service.cycle("accepted")
    store = service.deps.store
    td._seed_verifying_rows(store, 1)
    store.set_meta("service_recovery_metadata_first:v1", "1")
    service.deps = replace(service.deps, max_recovery_rows_per_cycle=1)
    original = store.metadata_recovery_batch
    row = store.get_metadata_operation(metadata_source_key(service.deps.shoko), td.ANIME_ID)
    assert row is not None
    pending_before = store.budget_view().pending_episode_jobs
    with open_state(store.path, budget_limits=store.budget_limits) as peer:

        def interleave(source: str, *, after_id: int, limit: int) -> RecoveryBatch:
            batch = original(source, after_id=after_id, limit=limit)
            assert batch.ids == (td.ANIME_ID,)
            if removed:
                # Fault injection: losing the row must not grant a second recovery visit
                # or release its potentially delivered request's durable capacity.
                peer.connection.execute("DELETE FROM metadata_operations")
                peer.connection.commit()
            else:
                peer.record_metadata_operation(
                    row,
                    MetadataOperationState.READY,
                    iso=td.T1,
                    verified_required_targets=row.required_targets,
                )
            return batch

        def forbidden(*_args: object) -> None:
            pytest.fail("the metadata visit consumed the shared cap; do not decode file rows")

        monkeypatch.setattr(store, "metadata_recovery_batch", interleave)
        monkeypatch.setattr(store, "get_file", forbidden)
        before = len(service.shoko.requests)
        reasons = daemon._recovery_phase(service.deps, td.T1, 0.0)
        assert "recovery_deferred:1" in reasons
        assert not any(reason.startswith("metadata_recovery_deferred:") for reason in reasons)
        expected = "invalid_coverage" if removed else "ready"
        assert f"metadata_recovery:{td.ANIME_ID}:{expected}" in reasons
        assert store.get_meta(f"metadata_recovery_cursor:v1:{row.source_key}") == str(td.ANIME_ID)
        assert len(service.shoko.requests) == before
        assert store.active_send_details(1) is not None
        assert store.budget_view().pending_episode_jobs == pending_before - (0 if removed else 1)


@pytest.mark.parametrize("metadata_missing", [False, True])
def test_cycle_remote_authorization_calls_share_absolute_budget_and_refuse_late_permission(
    service: Service, metadata_missing: bool
) -> None:
    if not metadata_missing:
        service.shoko.missing_anime.clear()
    deadlines: list[float] = []
    service.deps = replace(service.deps, max_cycle_seconds=10.0)

    def authorize(deadline: float) -> bool:
        deadlines.append(deadline)
        # Initial eligibility succeeds; the next fresh read finishes exactly at
        # the same budget boundary. Its true result cannot authorize a send.
        if len(deadlines) == 2:
            service.clock[0] = deadline
        return True

    service.deps = replace(service.deps, service_authorized_for_deadline=authorize)
    report = daemon.run_cycle(
        service.deps,
        now_iso=td.T1,
        mode=daemon.CycleMode.APPLY,
        policy=RunPolicy.SERVICE,
        cycle_key="authorization-budget",
        budget_start_mono=2.0,
    )
    assert deadlines == [12.0, 12.0]
    assert report.counts.submitted == report.metadata_requests_attempted == 0
    assert service.shoko.posts == []
    assert service.shoko.refreshes == []
    assert service.deps.store.live_send_counts() == (0, 0)
    assert service.deps.store.budget_view().pending_episode_jobs == 0
    assert (
        service.deps.store.get_metadata_operation(
            metadata_source_key(service.deps.shoko), td.ANIME_ID
        )
        is None
    )


def test_expired_cycle_rate_feedback_never_calls_remote_authorization_or_increases_cap(
    service: Service,
) -> None:
    _seed_healthy_rate(service)
    deadlines: list[float] = []

    def authorize(deadline: float) -> bool:
        deadlines.append(deadline)
        return True

    deps = daemon._bind_service_authorization(
        replace(service.deps, service_authorized_for_deadline=authorize), 1.0
    )
    assert deps.service_authorized()
    assert deps.service_authorized()
    assert deadlines == [1.0, 1.0]
    service.clock[0] = 1.0
    updated = daemon._update_service_rate_state(
        deps,
        RateState(2, 1, td.T1, "healthy"),
        now_iso=td.T1,
        queue=QueueSnapshot(0, 0, 0),
        submitted=1,
        verified=1,
        reasons=(),
        drift=(),
        deadline=1.0,
    )
    assert deadlines == [1.0, 1.0]
    assert updated.cap == 1
    assert updated.successful_streak == 0
    assert updated.last_reason != "healthy"


def test_direct_normal_candidate_binds_its_explicit_authorization_deadline(
    service: Service,
) -> None:
    service.shoko.missing_anime.clear()
    sweep = daemon.run_cycle(
        service.deps,
        now_iso=td.T1,
        mode=daemon.CycleMode.DRY_RUN,
        policy=RunPolicy.SERVICE,
        cycle_key="direct-candidate-sweep",
    )
    plan = sweep.plans[500]
    active = load_active(service.deps.mapping_store)
    assert active is not None
    folders = collect_import_folders(service.deps.shoko)
    assert folders is not None
    deadlines: list[float] = []

    def authorize(deadline: float) -> bool:
        deadlines.append(deadline)
        service.clock[0] = deadline
        return True

    deps = replace(service.deps, service_authorized_for_deadline=authorize)
    daemon._process_candidate(
        deps,
        plan,
        500,
        daemon._EligibilityFlags(None, None, True, True, True, True),
        active,
        {folder.id: folder for folder in folders},
        QueueSnapshot(0, 0, 0),
        "direct-candidate",
        service_backlog=True,
        source_check_deadline=10.0,
    )
    assert deadlines == [10.0]
    assert service.shoko.posts == []
    assert service.deps.store.live_send_counts() == (0, 0)
    assert service.deps.store.budget_view().pending_episode_jobs == 0


def test_rebinding_authorization_cannot_extend_an_owned_deadline(service: Service) -> None:
    deadlines: list[float] = []

    def authorize(deadline: float) -> bool:
        deadlines.append(deadline)
        return True

    production = replace(service.deps, service_authorized_for_deadline=authorize)
    bound = daemon._bind_service_authorization(production, 1.0)
    rebound = daemon._bind_service_authorization(bound, 10.0)
    assert rebound.service_authorized()
    assert deadlines == [1.0]
    service.clock[0] = 1.0
    updated = daemon._update_service_rate_state(
        production,
        RateState(2, 1, td.T1, "healthy"),
        now_iso=td.T1,
        queue=QueueSnapshot(0, 0, 0),
        submitted=1,
        verified=1,
        reasons=(),
        drift=(),
        deadline=1.0,
    )
    assert deadlines == [1.0]
    assert updated.cap == 1
    assert updated.successful_streak == 0


@pytest.mark.parametrize("cap", [1, 2, 20])
def test_empty_queue_batch_uses_configured_cap_once_even_when_own_jobs_appear(
    service: Service, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cap: int
) -> None:
    _add_anime(service, tmp_path)
    _add_anime(service, tmp_path, anime=3)
    service.deps = replace(service.deps, max_submissions=cap)
    original = submit_metadata_refresh
    checked = 0
    fetch = fetch_queue_inventory

    def observe(*args: Any, **kwargs: Any) -> Any:
        nonlocal checked
        checked += 1
        assert not service.shoko.refreshes
        return fetch(*args, **kwargs)

    def enqueue(*args: Any, **kwargs: Any) -> Any:
        result = original(*args, **kwargs)
        assert service.shoko.queue_items is not None
        service.shoko.queue_items.append(qf.item(f"AniDB.GetAniDBAnimeJob_AnimeID:{args[1]}"))
        return result

    monkeypatch.setattr(daemon, "fetch_queue_inventory", observe)
    monkeypatch.setattr(daemon, "submit_metadata_refresh", enqueue)
    report = service.cycle("empty-batch")
    assert report.metadata_requests_attempted == min(3, cap)
    assert checked == 1
    assert service.deps.store.get_service_canary() is None
    rate = decode_rate_state(service.deps.store.get_service_rate_state_json() or "")
    assert rate.cap == 1
    assert rate.successful_streak == 0


@pytest.mark.parametrize("state", ["waiting", "running", "blocked"])
def test_unrelated_job_of_any_state_blocks_metadata_batch(
    service: Service, tmp_path: Path, state: str
) -> None:
    _add_anime(service, tmp_path)
    service.shoko.queue_items = [qf.item("Other.UnrelatedJob", state)]
    report = service.cycle("unrelated-busy")
    assert report.metadata_requests_attempted == 0
    assert {reason for _, reason in report.metadata_observations} == {"metadata_queue_busy"}
    assert service.shoko.refreshes == []


@pytest.mark.parametrize(
    "failure", ["timeout", "429", "source", "mapping", "scope", "protected", "consent", "deadline"]
)
def test_batch_stops_after_uncertainty_or_fresh_safety_drift(
    service: Service, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    _add_anime(service, tmp_path)
    _add_anime(service, tmp_path, anime=3)
    original = submit_metadata_refresh

    def change(*args: Any, **kwargs: Any) -> Any:
        if failure == "timeout":
            service.shoko.fail_refresh = True
        if failure == "429":
            service.shoko.refreshes.append(httpx.Request("POST", "http://shoko/Refresh"))
            return MetadataRefreshResult(
                MetadataRefreshOutcome.UNKNOWN, MetadataRefreshReason.HTTP_RESPONSE, 429
            )
        result = original(*args, **kwargs)
        if failure == "source":
            service.sonarr.series_tvdb[3] = 9999
        elif failure == "mapping":
            service.deps.mapping_store.active_path.write_text("changed", encoding="utf-8")
        elif failure == "scope":
            monkeypatch.setattr(daemon, "_metadata_scope_fingerprint", lambda *_: "e" * 64)
        elif failure == "protected":
            service.shoko.link_unrelated(502)
        elif failure == "consent":
            service.deps.store.set_meta(
                META_SERVICE_SETTINGS, json.dumps({"allow_missing_metadata_refresh": False})
            )
        elif failure == "deadline":
            service.clock[0] = service.deps.max_cycle_seconds
        return result

    monkeypatch.setattr(daemon, "submit_metadata_refresh", change)
    report = service.cycle("batch-drift")
    assert report.metadata_requests_attempted == 1
    assert len(service.shoko.refreshes) == 1
    row = service.deps.store.get_metadata_operation(metadata_source_key(service.deps.shoko), 2)
    assert row is not None
    assert row.reservation_owned
    assert not row.slot_released


def test_busy_metadata_does_not_starve_ready_file_link(service: Service, tmp_path: Path) -> None:
    _add_anime(service, tmp_path, missing=False)
    service.shoko.queue_items = [qf.item("Other.UnrelatedJob")]
    report = service.cycle("busy-but-ready")
    assert report.counts.submitted == report.counts.verified == 1
    assert report.metadata_requests_attempted == 0
    assert report.metadata_observations == ((td.ANIME_ID, "metadata_queue_busy"),)


def test_file_link_that_queues_work_defers_new_metadata_batch(
    service: Service, tmp_path: Path
) -> None:
    _add_anime(service, tmp_path, missing=False)
    service.shoko.on_post = lambda: setattr(
        service.shoko, "queue_items", [qf.item("Other.FileProcessingJob")]
    )
    report = service.cycle("ready-creates-job")
    assert report.counts.submitted == report.counts.verified == 1
    assert report.metadata_requests_attempted == 0
    assert service.shoko.refreshes == []


@pytest.mark.parametrize(
    "limit", ["per_cycle_new", "rolling_new", "max_pending_files", "max_pending_episode_jobs"]
)
def test_metadata_batch_cannot_exceed_shared_durable_budgets(
    service: Service, tmp_path: Path, limit: str
) -> None:
    _add_anime(service, tmp_path)
    _add_anime(service, tmp_path, anime=3)
    bounds = td.WIDE_LIMITS
    service.deps.store.budget_limits = replace(
        bounds,
        per_cycle_new=2 if limit == "per_cycle_new" else bounds.per_cycle_new,
        rolling_new=2 if limit == "rolling_new" else bounds.rolling_new,
        max_pending_files=2 if limit == "max_pending_files" else bounds.max_pending_files,
        max_pending_episode_jobs=(
            2 if limit == "max_pending_episode_jobs" else bounds.max_pending_episode_jobs
        ),
    )
    report = service.cycle("bounded-metadata-batch")
    assert report.metadata_requests_attempted == 2
    assert len(service.shoko.refreshes) == 2


def test_changing_queue_inventory_never_admits_metadata_batch(service: Service) -> None:
    def change(path: str) -> None:
        if path.endswith("/Queue/Items"):
            service.shoko.queue_items = [qf.item("Other.ArrivedJob")]

    service.shoko.on_get = change
    report = service.cycle("incomplete-queue")
    assert report.metadata_requests_attempted == 0
    assert report.metadata_observations == ((td.ANIME_ID, "metadata_queue_not_clear"),)
    assert service.shoko.refreshes == []
