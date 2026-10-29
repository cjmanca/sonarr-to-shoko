"""Offline complete-file metadata demands and fresh-pass recovery; no refresh writes."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import FrozenInstanceError, dataclass, replace
from pathlib import Path
from typing import Any

import httpx
import pytest
import test_daemon as td

from shoko_linker import daemon
from shoko_linker.daemon import DaemonDeps
from shoko_linker.linking import EpisodeResolutionStatus, PlanStatus, metadata_demand_fingerprint
from shoko_linker.mapping_store import load_active
from shoko_linker.mappings import (
    EpisodeType,
    MappingIndex,
    MappingTarget,
    Resolution,
    ResolutionStatus,
    SourceRule,
)
from shoko_linker.mappings import resolve as resolve_mapping
from shoko_linker.policies import RunPolicy
from shoko_linker.shoko import collect_shoko_discovery
from shoko_linker.sonarr import collect_sonarr_inventory
from shoko_linker.state import FileState


class MetadataStub(td.ShokoStub):
    def __init__(self, files: list[td.SpecFile]) -> None:
        super().__init__(files)
        self.missing_anime: set[int] = {td.ANIME_ID}

    def _respond_get(self, path: str, request: httpx.Request) -> httpx.Response | None:
        if any(path == f"/api/v3/Series/AniDB/{anime}/Episode" for anime in self.missing_anime):
            return httpx.Response(404, json={}, request=request)
        return super()._respond_get(path, request)


@dataclass
class Harness:
    deps: DaemonDeps
    shoko: MetadataStub
    file: td.SpecFile

    def reconcile(
        self,
        *,
        affected: frozenset[int] | None = None,
        policy: RunPolicy = RunPolicy.SERVICE,
    ) -> daemon._ReconcileResult:
        active = load_active(self.deps.mapping_store)
        assert active is not None
        return daemon._reconcile(
            self.deps,
            collect_sonarr_inventory(self.deps.sonarr, self.deps.settings),
            collect_shoko_discovery(self.deps.shoko),
            active,
            td.T1,
            policy=policy,
            affected_sonarr_series=affected,
        )


@pytest.fixture
def harness(tmp_path: Path) -> Iterator[Harness]:
    file = td.SpecFile(series_id=1, file_id=10, shoko_id=500)
    td.activate_mapping(tmp_path, td.mapping_xml())
    store = td.open_tmp_store(tmp_path)
    shoko = MetadataStub([file])
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=td.mapping_xml(),
        monotonic=lambda: 0.0,
        advance=lambda _seconds: None,
        shoko=shoko,
    )
    deps = replace(deps, service_mode=True, service_authorized=lambda: True)
    current = Harness(deps, shoko, file)
    try:
        yield current
        assert all(request.method == "GET" for request in shoko.requests)
        assert store.connection.execute("SELECT COUNT(*) FROM sends").fetchone()[0] == 0
        budget = store.budget_view()
        assert budget.pending_file_requests == 0
        assert budget.pending_episode_jobs == 0
        assert budget.rolling_new_submissions == 0
    finally:
        for endpoint in (current.deps.sonarr, current.deps.shoko, current.deps.mapping_source):
            endpoint.client.close()
        store.close()


def test_missing_anime_is_retryable_frozen_demand_then_fresh_ready_plan(harness: Harness) -> None:
    first = harness.reconcile()
    demand = first.metadata_demands[500]
    assert first.pending == 1
    assert first.plans == {}
    assert first.skip_reasons == {}
    assert demand.needs[0].missing is EpisodeResolutionStatus.ANIME_NOT_CACHED
    assert demand.needs[0].known_anidb_episode_id is None
    assert demand.shoko_paths == (td._shoko_path(harness.file),)
    assert demand.shoko_paths != (td._sonarr_path(harness.file),)
    assert demand.shoko_ed2k == demand.shoko_ed2k.lower()
    assert (
        demand.required_targets
        == demand.missing_targets
        == {
            (td.ANIME_ID, EpisodeType.EPISODE, 1),
        }
    )
    record = harness.deps.store.get_file(500)
    assert record is not None
    assert record.state is FileState.WAITING
    assert record.state_detail == "pending_metadata"
    assert demand.fingerprint == metadata_demand_fingerprint(demand)
    assert demand.fingerprint != metadata_demand_fingerprint(
        replace(demand, scope_fingerprint="d" * 64)
    )
    assert demand.fingerprint != metadata_demand_fingerprint(replace(demand, shoko_ed2k="e" * 32))
    changed_target = replace(demand.mapped_targets[0], source_rule="override")
    assert demand.fingerprint != metadata_demand_fingerprint(
        replace(demand, mapped_targets=(changed_target,))
    )
    with pytest.raises(FrozenInstanceError):
        demand.shoko_size = 0  # type: ignore[misc]

    # Default-off maintenance does not prevent externally imported metadata recovery.
    harness.shoko.missing_anime.clear()
    ready = harness.reconcile()
    assert ready.metadata_demands == {}
    assert ready.plans[500].status is PlanStatus.LINKABLE
    assert ready.plans[500].episode_ids == (td.SHOKO_EP,)
    record = harness.deps.store.get_file(500)
    assert record is not None
    assert record.state is FileState.PLANNED


def test_internal_episode_wait_retains_positive_pending_id_and_stable_demand(
    harness: Harness,
) -> None:
    missing_anime = harness.reconcile().metadata_demands[500]
    harness.shoko.missing_anime.clear()
    harness.shoko.unmaterialized.add(td.ANIDB_EP)
    second = harness.reconcile()
    demand = second.metadata_demands[500]
    assert demand.fingerprint == missing_anime.fingerprint
    assert demand.needs[0].missing is EpisodeResolutionStatus.PENDING_METADATA
    assert demand.needs[0].known_anidb_episode_id == td.ANIDB_EP
    assert second.plans[500].pending_targets[0].anidb_episode_id == td.ANIDB_EP
    assert second.plans[500].episode_ids == ()
    assert second.pending == 1
    harness.shoko.unmaterialized.clear()
    assert harness.reconcile().plans[500].episode_ids == (td.SHOKO_EP,)


@pytest.mark.parametrize("failure", [ResolutionStatus.AMBIGUOUS, ResolutionStatus.UNMAPPED])
def test_all_source_mappings_checked_before_any_metadata_get(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    failure: ResolutionStatus,
) -> None:
    _add_second_episode(harness)
    original = resolve_mapping

    def mapping(
        index: MappingIndex, tvdb: int, season: int, number: int, absolute: int | None
    ) -> Resolution:
        if number == 2:
            return Resolution(
                status=failure, target=None, candidates=(), detail="second source not mapped"
            )
        return original(index, tvdb, season, number, absolute)

    monkeypatch.setattr(daemon, "resolve_mapping", mapping)
    result = harness.reconcile()
    assert result.metadata_demands == {}
    assert result.plans == {}
    assert not any("/Series/AniDB/" in request.url.path for request in harness.shoko.requests)
    assert any(failure.value in reason for reason in result.skip_reasons.values())


def _add_second_episode(harness: Harness) -> None:
    replacement = replace(
        harness.file,
        episodes=(
            (1, 1, td.ANIDB_EP, td.SHOKO_EP),
            (1, 2, td.ANIDB_EP + 1, td.SHOKO_EP + 1),
        ),
    )
    harness.file = replacement
    harness.shoko.files[500] = replacement
    stub = td.SonarrStub([replacement])
    harness.deps.sonarr.client.close()
    harness.deps = replace(harness.deps, sonarr=td.endpoint_for(stub.handler, td.SONARR_BASE))


def _target(anime: int, number: int) -> MappingTarget:
    return MappingTarget(
        anidb_id=anime,
        anidb_episode_number=number,
        episode_type=EpisodeType.EPISODE,
        source_rule=SourceRule.EXPLICIT_PAIR,
        precedence_rank=1,
        origin="verified-fixture",
    )


def test_split_anime_all_sources_and_multi_targets_collect_mixed_needs_and_dedup(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _add_second_episode(harness)
    other_anime = td.ANIME_ID + 1
    targets = (_target(td.ANIME_ID, 1), _target(other_anime, 1))
    answer = Resolution(
        status=ResolutionStatus.RESOLVED,
        target=targets[0],
        candidates=targets,
        detail="verified split",
    )
    monkeypatch.setattr(daemon, "resolve_mapping", lambda *_args: answer)
    # Other anime is present in AniDB metadata, but lacks its Shoko internal episode.
    harness.shoko.files[501] = replace(harness.file, shoko_id=501, anime_id=other_anime)
    harness.shoko.unmaterialized.add(td.ANIDB_EP)
    result = harness.reconcile()
    demand = result.metadata_demands[500]
    assert len(demand.episodes) == 2
    assert len(demand.mapped_targets) == 4
    assert len(demand.needs) == 2
    assert {need.missing for need in demand.needs} == {
        EpisodeResolutionStatus.ANIME_NOT_CACHED,
        EpisodeResolutionStatus.PENDING_METADATA,
    }
    assert demand.required_targets == {
        (td.ANIME_ID, EpisodeType.EPISODE, 1),
        (other_anime, EpisodeType.EPISODE, 1),
    }
    assert result.plans == {}


@pytest.mark.parametrize("status", [503, 200, 404])
def test_missing_target_cannot_mask_other_target_failure(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    status: int,
) -> None:
    _add_second_episode(harness)
    targets = (_target(td.ANIME_ID, 1), _target(td.ANIME_ID + 1, 1))
    answer = Resolution(
        status=ResolutionStatus.RESOLVED,
        target=targets[0],
        candidates=targets,
        detail="verified split",
    )
    monkeypatch.setattr(daemon, "resolve_mapping", lambda *_args: answer)
    other_path = f"/api/v3/Series/AniDB/{td.ANIME_ID + 1}/Episode"
    # 200 invalid JSON = hard failure; 404 is a second valid missing-anime demand.
    harness.shoko.target_read_failures[other_path] = (1, status)
    result = harness.reconcile()
    if status == 404:
        assert len(result.metadata_demands[500].needs) == 2
    else:
        assert result.metadata_demands == {}
        assert result.plans == {}
        if status == 503:
            assert result.resolution_deferred_file_ids == {500}


@pytest.mark.parametrize("detail", ["target_anime_not_cached:old skip", "target_unmapped:old skip"])
@pytest.mark.parametrize("affected", [None, frozenset({1}), frozenset({2}), frozenset()])
def test_legacy_missing_target_recovery_respects_mapping_activation_context(
    harness: Harness,
    detail: str,
    affected: frozenset[int] | None,
) -> None:
    harness.reconcile()
    harness.deps.store.advance(500, FileState.AMBIGUOUS, detail, iso=td.T1)
    retry = detail.startswith("target_anime_not_cached:") or affected is None or 1 in affected
    result = harness.reconcile(affected=affected)
    assert (500 in result.metadata_demands) is retry
    harness.shoko.missing_anime.clear()
    ready = harness.reconcile(affected=affected)
    if retry:
        assert ready.plans[500].status is PlanStatus.LINKABLE
    else:
        assert ready.plans == {}
        record = harness.deps.store.get_file(500)
        assert record is not None
        assert record.state is FileState.AMBIGUOUS
        assert record.state_detail == detail


@pytest.mark.parametrize("protection", ["conflict", "ambiguous", "quarantine", "hash", "source"])
def test_existing_protections_never_become_metadata_demand(
    harness: Harness, protection: str
) -> None:
    harness.reconcile()
    if protection == "conflict":
        harness.deps.store.advance(500, FileState.CONFLICT, "existing_link", iso=td.T1)
    elif protection == "ambiguous":
        harness.deps.store.advance(500, FileState.AMBIGUOUS, "target_ambiguous:review", iso=td.T1)
    elif protection == "quarantine":
        harness.deps.store.quarantine_mapping_targets((500,), iso=td.T1)
    elif protection == "hash":
        harness.shoko.ed2k_overrides[500] = "c" * 32
    else:
        _add_second_episode(harness)
        harness.shoko.files[500] = replace(harness.file, file_id=11)
        stub = td.SonarrStub([harness.shoko.files[500]])
        harness.deps.sonarr.client.close()
        harness.deps = replace(harness.deps, sonarr=td.endpoint_for(stub.handler, td.SONARR_BASE))
    assert harness.reconcile(affected=frozenset()).metadata_demands == {}


@pytest.mark.parametrize("mode", [daemon.CycleMode.DRY_RUN, daemon.CycleMode.APPLY])
def test_advanced_missing_anime_keeps_existing_skip_and_zero_posts(
    harness: Harness, mode: daemon.CycleMode
) -> None:
    harness.deps = replace(harness.deps, service_mode=False)
    report = daemon.run_cycle(
        harness.deps,
        now_iso=td.T1,
        mode=mode,
        policy=RunPolicy.NEW_ARRIVAL,
        cycle_key="advanced",
    )
    assert report.counts.submitted == 0
    record = harness.deps.store.get_file(500)
    assert record is not None
    assert record.state is FileState.AMBIGUOUS
    assert record.state_detail.startswith("target_anime_not_cached:")


def test_invalid_later_target_never_looks_up_earlier_missing_anime(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    targets = (_target(td.ANIME_ID, 1), _target(0, 1))
    answer = Resolution(ResolutionStatus.RESOLVED, targets[0], targets, "invalid fixture target")
    monkeypatch.setattr(daemon, "resolve_mapping", lambda *_args: answer)
    result = harness.reconcile()
    assert result.metadata_demands == {}
    assert result.plans == {}
    assert not any("/Series/AniDB/" in request.url.path for request in harness.shoko.requests)


@pytest.mark.parametrize("status", [503, 200])
def test_second_hop_transport_or_malformed_failure_suppresses_whole_file_demand(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    status: int,
) -> None:
    _add_second_episode(harness)
    targets = (_target(td.ANIME_ID, 1), _target(td.ANIME_ID + 1, 1))
    answer = Resolution(ResolutionStatus.RESOLVED, targets[0], targets, "split fixture")
    monkeypatch.setattr(daemon, "resolve_mapping", lambda *_args: answer)
    harness.shoko.files[501] = replace(harness.file, shoko_id=501, anime_id=td.ANIME_ID + 1)
    path = f"/api/v3/Episode/AniDB/{td.ANIDB_EP}/Episode"
    harness.shoko.target_read_failures[path] = (1, status)
    result = harness.reconcile()
    assert result.metadata_demands == {}
    assert result.plans == {}
    if status == 503:
        assert result.resolution_deferred_file_ids == {500}


def test_changed_known_episode_identity_quarantines_wait_instead_of_requesting_metadata(
    harness: Harness,
) -> None:
    harness.shoko.missing_anime.clear()
    harness.shoko.unmaterialized.add(td.ANIDB_EP)
    first = harness.reconcile()
    assert first.pending == 1
    assert first.metadata_demands[500].needs[0].known_anidb_episode_id == td.ANIDB_EP
    harness.shoko.files[500] = replace(
        harness.file,
        episodes=((1, 1, td.ANIDB_EP + 1, td.SHOKO_EP),),
    )
    harness.shoko.unmaterialized.add(td.ANIDB_EP + 1)
    changed = harness.reconcile()
    assert changed.metadata_demands == {}
    assert changed.plans == {}
    assert changed.pending == 0
    assert changed.skip_reasons[500] == "service_mapping_target_changed"
    assert harness.deps.store.mapping_target_quarantined(500)


@pytest.mark.parametrize("state", [FileState.AMBIGUOUS, FileState.CONFLICT])
def test_unrelated_review_state_survives_repeated_full_reconciliation(
    harness: Harness,
    state: FileState,
) -> None:
    harness.reconcile()
    harness.deps.store.advance(500, state, "unrelated_review_required", iso=td.T1)
    for _attempt in range(2):
        result = harness.reconcile()
        assert result.metadata_demands == {}
        assert result.plans == {}
        record = harness.deps.store.get_file(500)
        assert record is not None
        assert record.state is state
        assert record.state_detail == "unrelated_review_required"
    # Explicit generation recovery owns reopening; the preservation guard must
    # not turn an affected, freshly reactivated source into a permanent hold.
    daemon._reopen_generation_unresolved(harness.deps, td.T2, frozenset({1}))
    assert 500 in harness.reconcile().metadata_demands


@pytest.mark.parametrize("kind", ["size", "path", "access", "ignored"])
@pytest.mark.parametrize("metadata_missing", [False, True])
@pytest.mark.parametrize("affected", [None, frozenset({2})])
def test_corrected_source_exclusion_recovers_independently_of_mapping_activation(
    harness: Harness,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    metadata_missing: bool,
    affected: frozenset[int] | None,
) -> None:
    harness.shoko.missing_anime.clear()
    assert harness.reconcile().plans[500].status is PlanStatus.LINKABLE
    original_location = harness.shoko._location
    bad_location = True
    original_file_row = harness.shoko._file_row

    def location(source: td.SpecFile) -> dict[str, Any]:
        result = original_location(source)
        if bad_location and kind == "path":
            result["AbsolutePath"] = f"{td.SHOKO_ROOT}/other/500.mkv"
        if bad_location and kind == "access":
            result["IsAccessible"] = False
        return result

    def file_row(shoko_id: int, xrefs: list[dict[str, Any]]) -> dict[str, Any]:
        result = original_file_row(shoko_id, xrefs)
        if bad_location and kind == "ignored":
            result["IsIgnored"] = True
        return result

    monkeypatch.setattr(harness.shoko, "_location", location)
    monkeypatch.setattr(harness.shoko, "_file_row", file_row)
    if kind == "size":
        harness.shoko.size_overrides[500] = td.FILE_SIZE + 1
    # Missing metadata cannot justify a demand while the file still mismatches.
    harness.shoko.missing_anime.add(td.ANIME_ID)
    reason = {
        "size": "size_mismatch",
        "path": "location_path_mismatch",
        "access": "inaccessible_location",
        "ignored": "ignored",
    }[kind]
    for _attempt in range(2):
        failed = harness.reconcile()
        assert failed.metadata_demands == {}
        assert failed.plans == {}
        assert failed.skip_reasons[500].startswith(f"exclusion:{reason}:")
        record = harness.deps.store.get_file(500)
        assert record is not None
        assert record.state is FileState.AMBIGUOUS
        assert record.state_detail.startswith(f"source_exclusion:{reason}:")
    bad_location = False
    harness.shoko.size_overrides.clear()
    if not metadata_missing:
        harness.shoko.missing_anime.clear()
    for _attempt in range(2):
        recovered = harness.reconcile(affected=affected)
        assert recovered.skip_reasons == {}
        if metadata_missing:
            assert recovered.metadata_demands[500].missing_targets == {
                (td.ANIME_ID, EpisodeType.EPISODE, 1),
            }
            assert recovered.plans == {}
        else:
            assert recovered.plans[500].status is PlanStatus.LINKABLE
            assert recovered.metadata_demands == {}


def test_unknown_source_exclusion_remains_review_hold(harness: Harness) -> None:
    harness.reconcile()
    harness.deps.store.advance(
        500,
        FileState.AMBIGUOUS,
        "source_exclusion:unknown_future_reason:review",
        iso=td.T1,
    )
    for _attempt in range(2):
        assert harness.reconcile().metadata_demands == {}
        record = harness.deps.store.get_file(500)
        assert record is not None
        assert record.state is FileState.AMBIGUOUS
        assert record.state_detail == "source_exclusion:unknown_future_reason:review"
