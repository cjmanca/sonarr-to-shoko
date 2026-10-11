"""M9-e2e: end-to-end mocked scenario suite over the production modules.

Every scenario drives the real daemon/campaign/mapping/state/linking machinery against
a synthetic world (``e2e_harness``): MockTransport Sonarr/Shoko/mapping-source
endpoints, tmp-dir ``MappingStore``, tmp-file ``StateStore``, operator attestation
leases, fake ISO/monotonic clocks with injected advance/jitter. Assertions land on
final persisted state, report/JSON payload fields, and request/POST accounting from
the transport logs — not crash-only checks.

Measurement method: wall time via ``time.perf_counter``; a Python-heap high-water
mark via ``tracemalloc`` serves as the peak-RSS delta proxy (psutil is not a project
dependency, and POSIX ``resource.getrusage().ru_maxrss`` is unavailable on this
Windows dev box). Request counts come from the per-transport logs; POST bodies are
captured verbatim.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any, cast

import e2e_harness as eh
import httpx
import pytest
import yaml  # type: ignore[import-untyped]

from shoko_linker import cli
from shoko_linker.daemon import CycleMode, run_cycle
from shoko_linker.linking import (
    EpisodeTargetResolution,
    LinkPlan,
    PreflightReason,
    QueueSnapshot,
    freeze_plan,
    preflight,
)
from shoko_linker.mapping_store import RefreshOutcome, load_active
from shoko_linker.mapping_store import due as mapping_due
from shoko_linker.mapping_store import status as mapping_status
from shoko_linker.mappings import ResolutionStatus, build_mapping_index, parse_anime_lists, resolve
from shoko_linker.policies import RunPolicy
from shoko_linker.reporting import cycle_report_payload, report_payload
from shoko_linker.shoko import (
    ExclusionReason,
    FileMatch,
    IdentityChange,
    ShokoFile,
    ShokoHashes,
    ShokoLocation,
    build_identity,
    collect_shoko_discovery,
    compare_identity,
    match_shoko_files,
)
from shoko_linker.sonarr import collect_sonarr_inventory
from shoko_linker.state import BaselineFile, Cohort, FileState, SendToken, StateStore

N_SERIES = 1
N_TWO = 2
N_THREE = 3
N_FIVE = 5
N_SEVEN = 7
N_TEN = 10
N_TWELVE = 12
BATCH_TWO = 2
BATCH_THREE = 3
BATCH_TEN = 10
SERIES_SEVENTY = 70
PER_SERIES_HUNDRED = 100
BACKLOG_FILES = SERIES_SEVENTY * PER_SERIES_HUNDRED
LIMIT_TWENTY = 20
LIMIT_HUNDRED = 100
GETS_PER_FILE_CEILING = 3.0
# Generous upper bound for the measured full-world scenario (isolated it runs in ~50s).
# Raised from 60s: the scenario is CPU/IO sensitive under full-suite contention
# and a tight wall budget produced spurious CI failures, not real regressions. Kept a hard
# ceiling (a runaway cycle must still fail) — not an assertion on absolute speed.
WALL_BUDGET_SECONDS = 150.0
QUEUE_BLOCKED_THREE = 3
HUGE_QUEUE: tuple[int, int, int] = (15000, 0, 20000)
JOBS_TWO = 2
ANIME_A = 1500
ANIME_B = 1501
ANIME_S0 = 1502
ANIME_ABS = 1503
TVDB_SPLIT = 500001
TVDB_ABS = 500002
TVDB_COLLIDE = 500003
TVDB_PENDING = 500004
ANIME_COL_A = 1510
ANIME_COL_B = 1511
ANIME_PEND = 1520
MANUAL_FILE = 800
FIRST_FILE = 801
LIVE_SERIES = 900
AOT_S1_ANIME = 9541
AOT_FINAL2_ANIME = 16177
NEW_A = 21001
NEW_B = 21002
NEW_C = 21003
AMBIG_FILE = 41023
PEND_FILE = 41024
PEND_EP = 10
EP1 = 1
EP2 = 2
EP5 = 5
EP13 = 13
EP24 = 24
EP55 = 55
EP56 = 56
EP60 = 60
FILE_FIRST_COUR = 31001
FILE_SECOND_COUR = 31013
FILE_LAST_COUR = 31024
FILE_SPECIALS = 31000
FILE_MULTI = 31002
FILE_DUP5 = 31005
FILE_ABS = 32013
LATE_FILE = 11003
BACKLOG_SHOKO = 11001
MIXED_FILE = 11099
PENDING_ANIDB_EP = ANIME_PEND * 1000 + PEND_EP
ANIME11_EP1 = 500_000 + 4011 * 1000 + EP1


@pytest.fixture(autouse=True)
def _no_root_logging_leak() -> Iterator[None]:
    yield
    import logging

    for handler in list(logging.getLogger().handlers):
        logging.getLogger().removeHandler(handler)


def _lease_world(
    tmp_path: Path,
    xml: str,
    *,
    campaign: dict[str, Any] | None = None,
    **sync_extra: Any,
) -> tuple[eh.World, eh.FakeClock, Path]:
    lease = tmp_path / "lease.yaml"
    eh.record_lease(lease)
    world = eh.make_world(
        tmp_path, xml, lease=lease, campaign=campaign, sync_extra=dict(sync_extra) or None
    )
    clock = eh.FakeClock()
    eh.seed_mapping(world, clock)
    return world, clock, lease


def _small_mapped_world(
    tmp_path: Path,
    *,
    n_series: int = N_SERIES,
    per_series: int = N_TEN,
    extra_xml: str = "",
    **sync_extra: Any,
) -> tuple[eh.World, eh.FakeClock, Path]:
    inner = eh.backlog_xml(n_series, per_series)[len("<anime-list>") : -len("</anime-list>")]
    world, clock, lease = _lease_world(tmp_path, eh.xml_document(inner + extra_xml), **sync_extra)
    eh.seed_backlog(world, n_series, per_series)
    return world, clock, lease


def _seed_baseline(store: StateStore, specs: list[eh.FileSpec]) -> None:
    files = [
        BaselineFile(
            sonarr_series_id=spec.series_id,
            sonarr_file_id=spec.file_id,
            path=eh.sonarr_path(spec.series_id, spec.file_id),
            size=eh.file_size(spec.shoko_id),
        )
        for spec in specs
    ]
    store.record_baseline(
        files,
        complete=True,
        auto_apply_eligible=True,
        iso=eh.BASE_ISO,
    )


def _backlog_specs(count: int = N_TEN) -> list[eh.FileSpec]:
    return [eh.FileSpec(N_SERIES, 1001 + index, BACKLOG_SHOKO + index) for index in range(count)]


def _add_new_arrival(
    world: eh.World, series_id: int, shoko_id: int, number: int = EP1
) -> eh.FileSpec:
    spec = eh.FileSpec(series_id, series_id * 1000 + 1, shoko_id, (0,))
    series = eh.SeriesSpec(
        series_id,
        300_000 + series_id,
        (4000 + series_id,),
        (eh.EpisodeSpec(season=1, number=number),),
    )
    eh.add_new_arrival(world, series, spec)
    return spec


def _new_arrival_xml(series_id: int, number: int = EP1) -> str:
    return eh.pair_entry(4000 + series_id, 300_000 + series_id, f";{number}-{number};")


def _campaign_env(
    tmp_path: Path,
    world: eh.World,
    clock: eh.FakeClock,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    *,
    campaign_max: int = 200,
    batch: int = BATCH_TEN,
    auto_drain: bool = True,
) -> eh.CampaignEnv:
    lease = tmp_path / "lease.yaml"
    sync_extra = {"max_new_per_cycle": 120, "max_pending_episode_jobs": 30}
    campaign = {
        "max_campaign_files": campaign_max,
        "max_batch_size": batch,
        "daemon_auto_apply": auto_drain,
    }
    document = eh.settings_document(lease=lease, sync_extra=sync_extra, campaign=campaign)
    (tmp_path / "config.yaml").write_text(yaml.safe_dump(document), encoding="utf-8")
    env = eh.CampaignEnv(
        world=world,
        clock=clock,
        lease=lease,
        plan_path=tmp_path / "plan.json",
        now=clock.iso(),
    )
    env.read_out = lambda: capsys.readouterr().out
    eh.wire_campaign_env(env, monkeypatch)
    return env


def _shoko_file(file_id: int, size: int, absolute: str) -> ShokoFile:
    location = ShokoLocation(
        place_id=file_id * 10,
        file_id=file_id,
        import_folder_id=N_SERIES,
        relative_path="file.mkv",
        absolute_path=absolute,
        is_accessible=True,
    )
    return ShokoFile(
        id=file_id,
        size=size,
        is_ignored=False,
        is_variation=False,
        imported="2024-01-01T00:00:00",
        hashes=ShokoHashes(f"ed2k-{file_id}", None, file_id, None),
        xref_count=0,
        locations=(location,),
    )


def _run_apply(deps: Any, clock: eh.FakeClock, key: str, offset: float = 0) -> Any:
    return run_cycle(
        deps,
        now_iso=eh.iso_plus(clock.iso(), offset),
        mode=CycleMode.APPLY,
        policy=RunPolicy.NEW_ARRIVAL,
        cycle_key=key,
    )


def _fixed_clock() -> tuple[Callable[[], float], Callable[[float], None]]:
    def monotonic() -> float:
        return 1000.0

    def advance(_seconds: float) -> None:
        return None

    return monotonic, advance


def _row(shoko_id: int, absolute: str, relative: str) -> eh.ShokoRow:
    return eh.ShokoRow(
        file_id=shoko_id,
        size=eh.file_size(shoko_id),
        locations=[
            eh.ShokoLocation(place_id=shoko_id * 10, folder=1, relative=relative, absolute=absolute)
        ],
    )


# ------------------------------------------------------------------- 1. first-run


def test_first_run_seven_k_backlog_zero_posts_cohort_split(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    world, clock, _lease = _lease_world(
        tmp_path, eh.backlog_xml(SERIES_SEVENTY, PER_SERIES_HUNDRED)
    )
    specs = eh.seed_backlog(world, SERIES_SEVENTY, PER_SERIES_HUNDRED)
    assert len(specs) == BACKLOG_FILES
    store = eh.open_store(world)
    try:
        deps = eh.build_deps(world, store, clock, max_submissions=LIMIT_TWENTY)
        report = _run_apply(deps, clock, "c1")
        payload = cycle_report_payload(report)
        counts = cast("dict[str, int]", payload["counts"])
        status = mapping_status(world.mapping_store(), clock.now)
        coverage = report_payload(store, status, world.settings, now=clock.now, refresh_hours=168.0)
    finally:
        store.close()
    assert counts["backlog"] == BACKLOG_FILES
    assert counts["new_arrival"] == 0
    assert counts["submitted"] == 0
    cov = cast("dict[str, Any]", coverage["coverage"])
    assert cov["files_tracked_in_state"] == BACKLOG_FILES
    assert cov["files_verified"] == 0
    assert cov["coverage_percent"] == 0.0
    assert "files tracked in the state DB" in str(cov["denominator"])
    assert world.shoko.posts_count() == 0


# ------------------------------------------------------------------ 2. new arrival


def test_new_sonarr_file_after_baseline_links_and_verifies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    extra = _new_arrival_xml(11)
    world, clock, _lease = _small_mapped_world(tmp_path, extra_xml=extra)
    store = eh.open_store(world)
    _seed_baseline(store, _backlog_specs())
    spec = _add_new_arrival(world, 11, NEW_A)
    try:
        deps = eh.build_deps(world, store, clock, max_submissions=N_SERIES)
        report = _run_apply(deps, clock, "c1")
        record = store.get_file(spec.shoko_id)
        view = store.budget_view()
    finally:
        store.close()
    assert report.counts.submitted == 1
    assert report.counts.verified == 1
    assert record is not None
    assert record.state is FileState.VERIFIED
    assert record.cohort is Cohort.NEW_ARRIVAL
    assert world.shoko.posts_count() == 1
    assert world.shoko.posts[0][1] == [ANIME11_EP1]
    assert view.pending_file_requests == 0
    assert view.pending_episode_jobs == 0


# ------------------------------------------------------------ 3. late-arriving file


def test_old_file_late_in_shoko_stays_backlog_cohort(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    world, clock, _lease = _small_mapped_world(tmp_path, per_series=N_THREE)
    del world.shoko.rows[LATE_FILE]
    store = eh.open_store(world)
    _seed_baseline(store, _backlog_specs(N_THREE))
    try:
        deps = eh.build_deps(world, store, clock, max_submissions=LIMIT_TWENTY)
        first = _run_apply(deps, clock, "c1")
        assert first.counts.discovered == N_TWO
        world.shoko.add_row(_row(LATE_FILE, eh.shoko_path(1, 1003), "series1/file1003.mkv"))
        second = _run_apply(deps, clock, "c2", 60)
        record = store.get_file(LATE_FILE)
        verdict = store.can_auto_apply(
            LATE_FILE,
            now=clock.iso(),
            attestation_valid=True,
            gate_open=True,
            budgets_ok=True,
            mapping_stable=True,
            source_stable=True,
        )
    finally:
        store.close()
    assert record is not None
    assert record.cohort is Cohort.BACKLOG
    assert second.counts.new_arrival == 0
    assert not verdict.eligible
    assert "backlog_not_auto_applied" in verdict.reasons
    assert world.shoko.posts_count() == 0


# ------------------------------------------------------- 4/5. split cour and absolutes


def _split_cour_xml() -> str:
    specials = (
        "<anime anidbid='1502' tvdbid='500001' defaulttvdbseason='0'>"
        "<mapping-list><mapping anidbseason='0' tvdbseason='0'>;1-1;2-2;</mapping>"
        "</mapping-list></anime>"
    )
    parts = (
        "<anime anidbid='1500' tvdbid='500001' defaulttvdbseason='1' episodeoffset='0'/>"
        "<anime anidbid='1501' tvdbid='500001' defaulttvdbseason='1' episodeoffset='12'/>"
        f"{specials}"
        "<anime anidbid='1503' tvdbid='500002' defaulttvdbseason='a'/>"
    )
    return eh.xml_document(parts)


def _add_file(
    world: eh.World,
    series_id: int,
    file_id: int,
    shoko_id: int,
    episodes: tuple[tuple[int, int], ...],
    absolutes: dict[int, int] | None = None,
) -> None:
    for season, number in episodes:
        row: dict[str, Any] = {
            "id": file_id * 100 + season * 10 + number,
            "seriesId": series_id,
            "seasonNumber": season,
            "episodeNumber": number,
            "title": f"S{season}E{number}",
            "tvdbId": number,
            "episodeFileId": file_id,
        }
        absolute = (absolutes or {}).get(number)
        if absolute is not None:
            row["absoluteEpisodeNumber"] = absolute
        world.sonarr.add_episode(series_id, row)
    world.sonarr.add_file(
        series_id,
        {
            "id": file_id,
            "seriesId": series_id,
            "path": eh.sonarr_path(series_id, file_id),
            "size": eh.file_size(shoko_id),
        },
    )
    world.shoko.add_row(
        _row(shoko_id, eh.shoko_path(series_id, file_id), f"series{series_id}/file{file_id}.mkv")
    )


def _split_cour_world(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[eh.World, eh.FakeClock]:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    lease = tmp_path / "lease.yaml"
    eh.record_lease(lease)
    world = eh.make_world(tmp_path, _split_cour_xml(), lease=lease)
    clock = eh.FakeClock()
    eh.seed_mapping(world, clock)
    world.sonarr.add_series(21, eh.SONARR_ROOT, TVDB_SPLIT)
    world.sonarr.add_series(22, eh.SONARR_ROOT, TVDB_ABS)
    episodes = tuple(eh.EpisodeSpec(season=1, number=n) for n in range(1, 25))
    specials = (
        eh.EpisodeSpec(season=0, number=EP1, type="Special"),
        eh.EpisodeSpec(season=0, number=EP2, type="Special"),
    )
    absolutes = tuple(eh.EpisodeSpec(N_SERIES, n, n) for n in range(13, 25))
    world.shoko.register(
        eh.registry_for(
            eh.SeriesSpec(21, TVDB_SPLIT, (ANIME_A, ANIME_B, ANIME_S0), episodes + specials)
        )
    )
    world.shoko.register(eh.registry_for(eh.SeriesSpec(22, TVDB_ABS, (ANIME_ABS,), absolutes)))
    _add_file(world, 21, 2101, FILE_FIRST_COUR, ((1, EP5),))
    _add_file(world, 21, 2113, FILE_SECOND_COUR, ((1, EP13),))
    _add_file(world, 21, 2124, FILE_LAST_COUR, ((1, EP24),))
    _add_file(world, 21, 2100, FILE_SPECIALS, ((0, EP1), (0, EP2)))
    _add_file(world, 21, 2102, FILE_MULTI, ((1, 1), (1, 2), (1, 3)))
    _add_file(world, 21, 2105, FILE_DUP5, ((1, EP5),))
    _add_file(world, 22, 2213, FILE_ABS, ((1, EP13),), absolutes={EP13: EP13})
    return world, clock


def test_split_cour_absolute_and_season_zero_groups(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    world, clock = _split_cour_world(tmp_path, monkeypatch)
    store = eh.open_store(world)
    _seed_baseline(store, [eh.FileSpec(99, 99001, 99999)])
    try:
        deps = eh.build_deps(world, store, clock, max_submissions=N_SEVEN)
        report = _run_apply(deps, clock, "c1")
        plans = report.plans
        bodies = dict(world.shoko.posts)
        first_cour = plans[FILE_FIRST_COUR].targets[0]
        second_cour = plans[FILE_SECOND_COUR].targets[0]
        last_cour = plans[FILE_LAST_COUR].targets[0]
        special = plans[FILE_SPECIALS].targets[0]
        absolute = plans[FILE_ABS].targets[0]
        multi = plans[FILE_MULTI].targets
    finally:
        store.close()
    assert (first_cour.anidb_anime_id, first_cour.anidb_episode_number) == (ANIME_A, EP5)
    assert (second_cour.anidb_anime_id, second_cour.anidb_episode_number) == (ANIME_B, 1)
    assert (last_cour.anidb_anime_id, last_cour.anidb_episode_number) == (ANIME_B, 12)
    assert (special.anidb_anime_id, special.anidb_episode_number) == (ANIME_S0, EP1)
    assert special.episode_type == "Special"
    assert (absolute.anidb_anime_id, absolute.anidb_episode_number) == (ANIME_ABS, EP13)
    assert absolute.source_rule == "absolute_identity"
    assert {target.anidb_anime_id for target in multi} == {ANIME_A}
    assert sorted(t.anidb_episode_number for t in multi) == [1, 2, 3]
    expected_multi = sorted(500_000 + ANIME_A * 1000 + n for n in (1, 2, 3))
    assert bodies[FILE_MULTI] == expected_multi
    assert report.counts.submitted == N_SEVEN
    assert report.counts.verified == N_SEVEN
    assert len(world.shoko.posts) == N_SEVEN


def test_aot_real_fixture_split_cour_anchor() -> None:
    fixture = Path(__file__).parent / "fixtures" / "aot_real_entries.xml"
    parsed = parse_anime_lists(fixture.read_bytes())
    index = build_mapping_index(parsed)
    answer = resolve(index, 267440, 4, 26, 85)
    assert answer.status is ResolutionStatus.RESOLVED
    assert answer.target is not None
    assert (answer.target.anidb_id, answer.target.anidb_episode_number) == (AOT_FINAL2_ANIME, 10)
    season_zero = resolve(index, 267440, 0, 1)
    assert season_zero.status is ResolutionStatus.RESOLVED
    assert season_zero.target is not None
    assert season_zero.target.anidb_id == AOT_S1_ANIME
    assert season_zero.target.episode_type.value == "Special"


# --------------------------------------------------- 6. ambiguous and pending


def _add_scenario_file(
    world: eh.World,
    series_id: int,
    tvdb: int,
    anime_ids: tuple[int, ...],
    number: int,
    shoko_id: int,
) -> None:
    world.sonarr.add_series(series_id, eh.SONARR_ROOT, tvdb)
    world.shoko.register(
        eh.registry_for(eh.SeriesSpec(series_id, tvdb, anime_ids, (eh.EpisodeSpec(1, number),)))
    )
    _add_file(world, series_id, series_id * 1000 + 1, shoko_id, ((1, number),))


def _ambiguous_world(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[eh.World, eh.FakeClock]:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    xml = eh.xml_document(
        eh.pair_entry(ANIME_COL_A, TVDB_COLLIDE, ";4-4;")
        + eh.pair_entry(ANIME_COL_B, TVDB_COLLIDE, ";9-4;")
        + eh.pair_entry(ANIME_PEND, TVDB_PENDING, f";{PEND_EP}-{PEND_EP};")
    )
    lease = tmp_path / "lease.yaml"
    eh.record_lease(lease)
    world = eh.make_world(tmp_path, xml, lease=lease)
    clock = eh.FakeClock()
    eh.seed_mapping(world, clock)
    _add_scenario_file(world, 23, TVDB_COLLIDE, (ANIME_COL_A, ANIME_COL_B), 4, AMBIG_FILE)
    _add_scenario_file(world, 24, TVDB_PENDING, (ANIME_PEND,), PEND_EP, PEND_FILE)
    world.shoko.pending.add(PENDING_ANIDB_EP)
    return world, clock


def test_ambiguous_mapping_refuses_file_and_pending_target_defers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    world, clock = _ambiguous_world(tmp_path, monkeypatch)
    store = eh.open_store(world)
    try:
        deps = eh.build_deps(world, store, clock, max_submissions=N_TWO)
        report = _run_apply(deps, clock, "c1")
        ambiguous = store.get_file(AMBIG_FILE)
        pending = store.get_file(PEND_FILE)
    finally:
        store.close()
    assert ambiguous is not None
    assert ambiguous.state is FileState.AMBIGUOUS
    assert "target_ambiguous" in ambiguous.state_detail
    assert pending is not None
    assert pending.state is FileState.MAPPED
    assert report.counts.pending == 1
    assert report.counts.submitted == 0
    assert world.shoko.posts_count() == 0


def test_pending_target_applies_after_shoko_metadata_created(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    world, clock = _ambiguous_world(tmp_path, monkeypatch)
    store = eh.open_store(world)
    _seed_baseline(store, [eh.FileSpec(24, 24001, PEND_FILE)])
    store.close()
    env = _campaign_env(tmp_path, world, clock, monkeypatch, capsys, campaign_max=50, batch=N_FIVE)
    prepared = env.prepare(limit=N_TEN)
    env.approve(str(prepared["approval_digest"]))
    code, payload = env.apply(batch=N_FIVE)
    assert code == cli.EXIT_OK
    assert payload["submitted"] == 0
    assert any("pending_target:" in str(reason) for reason in payload["reasons"])
    assert world.shoko.posts_count() == 0
    world.shoko.pending.discard(PENDING_ANIDB_EP)
    code, payload = env.apply(batch=N_FIVE)
    assert code == cli.EXIT_OK
    assert payload["submitted"] == 1
    assert payload["verified"] == 1
    assert world.shoko.posts_count() == 1
    store = env.open_store()
    try:
        record = store.get_file(PEND_FILE)
    finally:
        store.close()
    assert record is not None
    assert record.state is FileState.VERIFIED


# ---------------------------------------------------------- 7. mixed ownership


def test_mixed_ownership_location_drops_file_with_zero_writes(tmp_path: Path) -> None:
    world, clock, _lease = _small_mapped_world(tmp_path, per_series=N_TWO)
    _add_scenario_file(world, 99, 300099, (3099,), EP1, MIXED_FILE)
    world.shoko.folders[2] = (eh.OUTSIDE_ROOT, "None")
    row = world.shoko.rows[MIXED_FILE]
    extra = eh.ShokoLocation(
        place_id=MIXED_FILE * 10 + 1,
        folder=2,
        relative="copy1099.mkv",
        absolute=f"{eh.OUTSIDE_ROOT}/copy1099.mkv",
    )
    row.locations.append(extra)
    store = eh.open_store(world)
    try:
        deps = eh.build_deps(world, store, clock, max_submissions=N_SERIES)
        report = run_cycle(
            deps,
            now_iso=clock.iso(),
            mode=CycleMode.DRY_RUN,
            policy=RunPolicy.BACKLOG,
            cycle_key="c1",
        )
        record = store.get_file(MIXED_FILE)
    finally:
        store.close()
    sonarr, shoko, _source = world.endpoints()
    inventory = collect_sonarr_inventory(sonarr, world.settings)
    discovery = collect_shoko_discovery(shoko)
    match = match_shoko_files(
        discovery, inventory.files, inventory.verdict.complete, world.settings
    )
    exclusion = next(item for item in match.exclusions if item.shoko_file_id == MIXED_FILE)
    assert exclusion.reason is ExclusionReason.MIXED_OWNERSHIP
    assert "all-location ownership" in exclusion.detail
    assert record is None
    assert report.writes_suppressed is True
    assert world.shoko.posts_count() == 0


# ------------------------------------------------- 8. preexisting wrong manual link


def _add_linked_world_file(
    world: eh.World,
    series_id: int,
    anime: int,
    file_id: int,
    shoko_id: int,
    episodes: tuple[int, ...],
) -> None:
    tvdb = 300_000 + series_id
    specs = tuple(eh.EpisodeSpec(1, ep) for ep in (*episodes, EP2, EP60))
    world.sonarr.add_series(series_id, eh.SONARR_ROOT, tvdb)
    world.shoko.register(eh.registry_for(eh.SeriesSpec(series_id, tvdb, (anime,), specs)))
    _add_file(world, series_id, file_id, shoko_id, tuple((1, ep) for ep in episodes))


def _preflight_existing_link(world: eh.World, clock: eh.FakeClock) -> tuple[bool, tuple[Any, ...]]:
    sonarr, shoko, _source = world.endpoints()
    inventory = collect_sonarr_inventory(sonarr, world.settings)
    active = load_active(world.mapping_store())
    assert active is not None
    sonarr_file = next(item for item in inventory.files if item.episode_file_id == MANUAL_FILE)
    resolutions: list[EpisodeTargetResolution] = []
    for episode in sonarr_file.episodes:
        answer = resolve(active.index, 300080, 1, episode.episode_number, None)
        assert answer.status is ResolutionStatus.RESOLVED
        assert answer.target is not None
        ep_number = answer.target.anidb_episode_number
        resolutions.append(
            EpisodeTargetResolution(
                episode=episode,
                tvdb_series=300080,
                status=ResolutionStatus.RESOLVED,
                anidb_anime_id=4080,
                anidb_episode_number=ep_number,
                episode_type=answer.target.episode_type.value,
                source_rule=answer.target.source_rule.value,
                anidb_episode_id=4080_000 + ep_number,
                shoko_episode_id=500_000 + 4080_000 + ep_number,
            )
        )
    match = FileMatch(
        shoko_file_id=MANUAL_FILE,
        sonarr_file=sonarr_file,
        shoko_paths=(eh.shoko_path(80, MANUAL_FILE),),
        hashes=ShokoHashes(f"ed2k-{MANUAL_FILE}", None, MANUAL_FILE, None),
    )
    frozen = freeze_plan(
        match,
        resolutions,
        policy=RunPolicy.NEW_ARRIVAL,
        targets_digest=active.targets_digest,
        structure_digest=active.structure_digest,
        shoko_paths=(eh.shoko_path(80, MANUAL_FILE),),
        mapping_revision=str(active.revision),
    )
    assert isinstance(frozen, LinkPlan)
    folders = collect_shoko_discovery(shoko).folders
    queue = QueueSnapshot(1, 0, 4)
    verdict = preflight(
        shoko,
        frozen,
        world.settings,
        folders,
        attestation_valid=True,
        apply_gate_open=True,
        queue=queue,
        active_targets_digest=active.targets_digest,
        active_structure_digest=active.structure_digest,
        blocked_over=0,
        mapping_index=active.index,
    )
    return verdict.ok, verdict.reasons


def test_existing_wrong_manual_link_is_protected_and_audited(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    extra = eh.pair_entry(4080, 300080, f";{EP55}-{EP55};{EP56}-{EP56};") + eh.pair_entry(
        4081, 300081, ";1-1;"
    )
    inner = eh.backlog_xml(N_SERIES, N_TEN)[len("<anime-list>") : -len("</anime-list>")]
    lease = tmp_path / "lease.yaml"
    eh.record_lease(lease)
    world = eh.make_world(tmp_path, eh.xml_document(inner + extra), lease=lease)
    clock = eh.FakeClock()
    eh.seed_mapping(world, clock)
    eh.seed_backlog(world, N_SERIES, N_TEN)
    _add_linked_world_file(world, 80, 4080, MANUAL_FILE, MANUAL_FILE, (EP55, EP56))
    _add_linked_world_file(world, 81, 4081, FIRST_FILE, FIRST_FILE, (EP1,))
    world.shoko.link_manually(MANUAL_FILE, 4080, 4080_000 + EP55, 500_000 + 4080_000 + EP55)
    world.shoko.link_manually(MANUAL_FILE, 4080, 4080_000 + EP60, 500_000 + 4080_000 + EP60)
    world.shoko.link_manually(FIRST_FILE, 4081, 4081_000 + EP2, 500_000 + 4081_000 + EP2)
    store = eh.open_store(world)
    try:
        deps = eh.build_deps(world, store, clock, max_submissions=LIMIT_TWENTY)
        _run_apply(deps, clock, "c1")
        assert world.shoko.posts_count() == 0
        assert store.get_file(MANUAL_FILE) is None
        assert all(method == "GET" for method, _ in world.shoko.requests)
        ok, reasons = _preflight_existing_link(world, clock)
    finally:
        store.close()
    assert ok is False
    assert PreflightReason.EXISTING_LINK in reasons
    env = _campaign_env(tmp_path, world, clock, monkeypatch, capsys)
    payload = env.audit()
    counts = cast("dict[str, int]", payload["counts"])
    assert counts["existing_link_mismatch"] == 1
    assert counts["wrong_first_episode"] == 1
    assert all(method == "GET" for method, _ in world.shoko.requests)


# ------------------------------------------------------- 9. weekly refresh cycle


def test_weekly_refresh_304_then_activated_remaps_backlog_without_posts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    entry1 = eh.range_entry(4001, 300001, 1, PER_SERIES_HUNDRED)
    entry2 = eh.range_entry(4002, 300002, 1, PER_SERIES_HUNDRED)
    world, clock, _lease = _lease_world(tmp_path, eh.xml_document(entry1))
    eh.seed_backlog(world, N_TWO, PER_SERIES_HUNDRED)
    store = eh.open_store(world)
    try:
        deps = eh.build_deps(world, store, clock, max_submissions=N_SERIES)
        first = _run_apply(deps, clock, "c1")
        assert first.counts.planned == PER_SERIES_HUNDRED
        assert first.counts.backlog == 2 * PER_SERIES_HUNDRED
        clock.jump(7 * 24 * 3600.0)
        unchanged = _run_apply(deps, clock, "c2")
        after_304 = mapping_status(world.mapping_store(), clock.now)
        after_304_now = clock.now
        world.source.set_xml(eh.xml_document(entry1 + entry2))
        clock.jump(7 * 24 * 3600.0)
        activated = _run_apply(deps, clock, "c3")
        planned = len(store.files_in_state(FileState.PLANNED))
    finally:
        store.close()
    assert unchanged.mapping_outcome is RefreshOutcome.UNCHANGED
    assert after_304.next_due_iso is not None
    assert eh.parse_iso(after_304.next_due_iso) > after_304_now
    assert activated.mapping_outcome is RefreshOutcome.ACTIVATED
    assert activated.counts.submitted == 0
    assert world.shoko.posts_count() == 0
    assert planned == 2 * PER_SERIES_HUNDRED
    fresh = eh.open_store(world)
    try:
        deps2 = eh.build_deps(world, fresh, clock, max_submissions=N_SERIES)
        before = len(world.source.requests)
        quiet = _run_apply(deps2, clock, "c4", 60)
        after = len(world.source.requests)
    finally:
        fresh.close()
    assert quiet.mapping_outcome is RefreshOutcome.NOT_DUE
    assert after == before


# ---------------------------------------------------- 10. invalid XML recovery


def test_corrupt_mapping_source_keeps_last_known_good_with_backoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    entry1 = eh.range_entry(4001, 300001, 1, N_TEN)
    entry2 = eh.range_entry(4002, 300002, 1, N_TEN)
    world, clock, _lease = _lease_world(tmp_path, eh.xml_document(entry1))
    eh.seed_backlog(world, N_TWO, N_TEN)
    store = eh.open_store(world)
    try:
        active_bytes = (world.mapping_dir / "active.xml").read_bytes()
        baseline_digest = hashlib.sha256(active_bytes).hexdigest()
        deps = eh.build_deps(world, store, clock, max_submissions=N_SERIES)
        world.source.corrupt = True
        clock.jump(7 * 24 * 3600.0)
        failed = _run_apply(deps, clock, "c1")
        status = mapping_status(world.mapping_store(), clock.now)
        after_bytes = (world.mapping_dir / "active.xml").read_bytes()
        reopened = world.mapping_store()
        due_time = eh.parse_iso(eh.iso_plus(clock.iso(), 3600))
        assert hashlib.sha256(after_bytes).hexdigest() == baseline_digest
        assert failed.mapping_outcome is RefreshOutcome.FAILED_INVALID
        assert status.next_due_iso is not None
        assert eh.parse_iso(status.next_due_iso) == due_time
        assert mapping_due(reopened, clock.now) is False
        assert mapping_due(reopened, due_time) is True
        assert load_active(reopened) is not None
        world.source.corrupt = False
        world.source.set_xml(eh.xml_document(entry1 + entry2))
        activated = _run_apply(deps, clock, "c2", 3600)
    finally:
        store.close()
    assert activated.mapping_outcome is RefreshOutcome.ACTIVATED
    assert load_active(world.mapping_store()) is not None
    assert world.shoko.posts_count() == 0


# ------------------------------------------------------ 11. override drift


OVERRIDE_YAML = (
    "add:\n"
    "  - tvdb_series: 500003\n"
    "    season: 1\n"
    "    episode: 4\n"
    "    anidb_id: 1510\n"
    "    anidb_episode: 4\n"
    "    type: Episode\n"
)


def test_override_add_moves_ambiguous_file_to_planned_without_posts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    xml = eh.xml_document(
        eh.pair_entry(ANIME_COL_A, TVDB_COLLIDE, ";4-4;")
        + eh.pair_entry(ANIME_COL_B, TVDB_COLLIDE, ";9-4;")
    )
    world, clock, _lease = _lease_world(tmp_path, xml)
    _add_scenario_file(world, 23, TVDB_COLLIDE, (ANIME_COL_A, ANIME_COL_B), 4, AMBIG_FILE)
    store = eh.open_store(world)
    try:
        deps = eh.build_deps(world, store, clock, max_submissions=N_SERIES)
        first = _run_apply(deps, clock, "c1")
        assert first.counts.planned == 0
        assert store.get_file(AMBIG_FILE) is not None
        clock.jump(7 * 24 * 3600.0)
        deps2 = eh.build_deps(
            world, store, clock, max_submissions=N_SERIES, overrides_text=OVERRIDE_YAML
        )
        second = _run_apply(deps2, clock, "c2")
        record = store.get_file(AMBIG_FILE)
        status = mapping_status(world.mapping_store(), clock.now)
    finally:
        store.close()
    assert second.mapping_outcome is RefreshOutcome.ACTIVATED
    assert record is not None
    assert record.state is FileState.PLANNED
    assert record.cohort is Cohort.BACKLOG
    assert second.counts.submitted == 0
    assert world.shoko.posts_count() == 0
    assert status.targets_digest is not None


# ---------------------------------------------- 12. file-id reuse and rebaseline


def test_file_id_reuse_detected_and_rebaseline_flag_clears(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    extra = _new_arrival_xml(11) + _new_arrival_xml(12)
    world, clock, _lease = _small_mapped_world(tmp_path, extra_xml=extra)
    store = eh.open_store(world)
    _seed_baseline(store, _backlog_specs())
    arrival = _add_new_arrival(world, 11, NEW_A)
    try:
        deps = eh.build_deps(world, store, clock, max_submissions=N_TEN)
        first = _run_apply(deps, clock, "c1")
        assert first.counts.verified == 1
        record = store.get_file(NEW_A)
        assert record is not None
        assert record.identity is not None
        old_file = _shoko_file(NEW_A, eh.file_size(NEW_A), eh.shoko_path(11, arrival.file_id))
        row = world.shoko.rows[NEW_A]
        row.xrefs = []
        row.size = eh.file_size(NEW_A) + 5
        row.locations = [
            eh.ShokoLocation(
                place_id=999_999,
                folder=1,
                relative="series999/file99999.mkv",
                absolute=f"{eh.SHOKO_ROOT}/series999/file99999.mkv",
            )
        ]
        new_file = _shoko_file(NEW_A, row.size, f"{eh.SHOKO_ROOT}/series999/file9999.mkv")
        drift = compare_identity(build_identity(old_file), build_identity(new_file))
        second = _run_apply(deps, clock, "c2", 300)
        record_after = store.get_file(NEW_A)
        # Shoko DB restore: several unrecognized rows vanish (a drop far larger than the
        # linker's own single accepted link), so continuity flags the excess drop.
        del world.shoko.rows[NEW_A]
        for gone in (BACKLOG_SHOKO + 1, BACKLOG_SHOKO + 2, BACKLOG_SHOKO + 3):
            del world.shoko.rows[gone]
        _add_new_arrival(world, 12, NEW_B)
        third = _run_apply(deps, clock, "c3", 600)
        flag = store.get_meta("requires_rebaseline")
        world.shoko.add_row(_row(NEW_A, eh.shoko_path(11, 11001), "series11/file11001.mkv"))
        for idx, file_no in enumerate((1002, 1003, 1004), start=1):
            world.shoko.add_row(
                _row(BACKLOG_SHOKO + idx, eh.shoko_path(1, file_no), f"series1/file{file_no}.mkv")
            )
        restored = _run_apply(deps, clock, "c4", 900)
        cleared = store.get_meta("requires_rebaseline")
        record_new = store.get_file(NEW_B)
    finally:
        store.close()
    assert IdentityChange.SIZE_CHANGED in drift.categories
    assert IdentityChange.LOCATIONS_CHANGED in drift.categories
    assert second.counts.submitted == 0
    assert record_after is not None
    assert record_after.state is FileState.VERIFIED
    assert flag == "1"
    assert "discovery_total_drop" in third.reasons
    assert third.counts.submitted == 0
    assert cleared == "0"
    assert restored.counts.submitted == 1
    assert record_new is not None
    assert record_new.state is FileState.VERIFIED
    assert sum(len(ids) for _, ids in world.shoko.posts) == N_TWO


# ------------------------------------------------ 13. side-effect safety (queue)


def _queue_world(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, queue: tuple[int, int, int]
) -> tuple[eh.World, eh.FakeClock]:
    extra = "".join(_new_arrival_xml(series) for series in range(31, 31 + N_FIVE))
    world, clock, _lease = _small_mapped_world(tmp_path, extra_xml=extra)
    store = eh.open_store(world)
    _seed_baseline(store, _backlog_specs())
    store.close()
    for series in range(31, 31 + N_FIVE):
        _add_new_arrival(world, series, 31000 + series)
    world.shoko.queue = queue
    return world, clock


def test_blocked_queue_zero_posts_with_tolerance_zero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    world, clock = _queue_world(tmp_path, monkeypatch, (5, QUEUE_BLOCKED_THREE, 20))
    store = eh.open_store(world)
    try:
        deps = eh.build_deps(world, store, clock, max_submissions=N_FIVE)
        report = _run_apply(deps, clock, "c1")
    finally:
        store.close()
    assert world.shoko.posts_count() == 0
    assert report.counts.submitted == 0
    assert f"queue_blocked:{QUEUE_BLOCKED_THREE}" in report.reasons
    assert any(reason.startswith("preflight_queue_blocked") for reason in report.reasons)


def test_huge_benign_queue_applies_within_budget_caps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    world, clock = _queue_world(tmp_path, monkeypatch, HUGE_QUEUE)
    store = eh.open_store(world)
    try:
        deps = eh.build_deps(world, store, clock, max_submissions=BATCH_TWO)
        report = _run_apply(deps, clock, "c1")
    finally:
        store.close()
    assert report.counts.submitted == BATCH_TWO
    assert world.shoko.posts_count() == BATCH_TWO
    assert report.deferred_planned == N_FIVE - BATCH_TWO


# ----------------------------------------------- 14/15. async jobs and lost POSTs


def test_async_xrefs_materialize_late_and_verify_across_cycles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    extra = _new_arrival_xml(11)
    world, clock, _lease = _small_mapped_world(tmp_path, extra_xml=extra)
    store = eh.open_store(world)
    _seed_baseline(store, _backlog_specs())
    _add_new_arrival(world, 11, NEW_A)
    world.shoko.link_on_post = False
    monotonic, advance = _fixed_clock()
    try:
        deps = eh.build_deps(
            world, store, clock, max_submissions=N_SERIES, monotonic=monotonic, advance=advance
        )
        first = _run_apply(deps, clock, "c1")
        record = store.get_file(NEW_A)
        assert record is not None
        assert record.state is FileState.VERIFYING
        assert first.live_held_slots == 1
        posted = world.shoko.posts[0][1]
        world.shoko.materialize(NEW_A, posted)
        clock.jump(30)
        second = _run_apply(deps, clock, "c2")
        closed = store.get_file(NEW_A)
        view = store.budget_view()
    finally:
        store.close()
    assert second.counts.verified == 1
    assert closed is not None
    assert closed.state is FileState.VERIFIED
    assert view.pending_file_requests == 0
    assert world.shoko.posts_count() == 1


def test_partial_materialization_past_deadline_releases_slot_per_ledger(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    entry = eh.pair_entry(4101, 300101, ";1-1;2-2;")
    inner = eh.backlog_xml(N_SERIES, N_TEN)[len("<anime-list>") : -len("</anime-list>")]
    world, clock, _lease = _lease_world(tmp_path, eh.xml_document(entry + inner))
    eh.seed_backlog(world, N_SERIES, N_TEN)
    store = eh.open_store(world)
    _seed_baseline(store, _backlog_specs())
    spec = eh.FileSpec(101, 101001, 21101, (0, 1))
    series = eh.SeriesSpec(101, 300101, (4101,), (eh.EpisodeSpec(1, 1), eh.EpisodeSpec(1, 2)))
    eh.add_new_arrival(world, series, spec)
    world.shoko.link_subset_on_post = True
    try:
        deps = eh.build_deps(
            world,
            store,
            clock,
            max_submissions=N_SERIES,
            verification_timeout=60.0,
            poll_interval=20.0,
            max_verify_polls=5,
        )
        first = _run_apply(deps, clock, "c1")
        assert first.counts.submitted == 1
        posted = world.shoko.posts[0][1]
        record = store.get_file(21101)
        view = store.budget_view()
        assert record is not None
        assert record.state is FileState.PARTIAL
        assert view.pending_file_requests == 1
        assert store.live_send_counts() == (1, JOBS_TWO)
        world.shoko.link_subset_on_post = False
        world.shoko.materialize(21101, posted)
        clock.advance(120)
        second = _run_apply(deps, clock, "c2")
        closed = store.get_file(21101)
        final_view = store.budget_view()
        noops = store.get_meta("slot_release_noops")
        held_after = store.live_send_counts()
    finally:
        store.close()
    assert second.counts.verified == 1
    assert closed is not None
    assert closed.state is FileState.VERIFIED
    assert final_view.pending_file_requests == 0
    assert noops is None
    assert held_after == (0, 0)


def test_lost_post_connection_reset_indeterminate_then_xref_inspection_closes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    extra = _new_arrival_xml(11)
    world, clock, _lease = _small_mapped_world(tmp_path, extra_xml=extra)
    store = eh.open_store(world)
    _seed_baseline(store, _backlog_specs())
    _add_new_arrival(world, 11, NEW_A)
    world.shoko.post_raises = httpx.ReadError("connection reset mid-POST")
    try:
        deps = eh.build_deps(world, store, clock, max_submissions=N_SERIES)
        first = _run_apply(deps, clock, "c1")
        record = store.get_file(NEW_A)
        assert record is not None
        assert record.state is FileState.INDETERMINATE_WRITE
        assert first.counts.indeterminate == 1
        assert store.active_send_details(NEW_A) is not None
        world.shoko.post_raises = None
        world.shoko.materialize(NEW_A, [ANIME11_EP1])
        second = _run_apply(deps, clock, "c2", 30)
        closed = store.get_file(NEW_A)
    finally:
        store.close()
    assert second.counts.verified == 1
    assert closed is not None
    assert closed.state is FileState.VERIFIED
    assert world.shoko.posts_count() == 1


def test_lost_post_http_502_indeterminate_then_partial_close_releases_slot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    extra = _new_arrival_xml(11)
    world, clock, _lease = _small_mapped_world(tmp_path, extra_xml=extra)
    store = eh.open_store(world)
    _seed_baseline(store, _backlog_specs())
    _add_new_arrival(world, 11, NEW_A)
    world.shoko.post_status = 502
    try:
        deps = eh.build_deps(world, store, clock, max_submissions=N_SERIES)
        first = _run_apply(deps, clock, "c1")
        record = store.get_file(NEW_A)
        assert record is not None
        assert record.state is FileState.INDETERMINATE_WRITE
        assert first.counts.indeterminate == 1
        assert store.live_send_counts()[0] == 1
        world.shoko.post_status = None
        clock.advance(61)
        second = _run_apply(deps, clock, "c2")
        closed = store.get_file(NEW_A)
        view = store.budget_view()
    finally:
        store.close()
    assert second.counts.indeterminate == 0
    assert closed is not None
    assert closed.state is FileState.PARTIAL
    assert view.pending_file_requests == 0
    assert world.shoko.posts_count() == 1


# ---------------------------------------------------- 16. durable restart mid-cycle


def test_crash_between_prepare_and_send_replans_once_without_double_post(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    extra = "".join(_new_arrival_xml(series) for series in (11, 12, 13))
    world, clock, _lease = _small_mapped_world(tmp_path, extra_xml=extra)
    store = eh.open_store(world)
    _seed_baseline(store, _backlog_specs())
    _add_new_arrival(world, 11, NEW_A)
    _add_new_arrival(world, 12, NEW_B)
    _add_new_arrival(world, 13, NEW_C)
    world.shoko.link_on_post = False
    marks: list[int] = []
    original = store.mark_sent
    monotonic, advance = _fixed_clock()

    def crashing_mark_sent(token: SendToken, *, iso: str) -> None:
        marks.append(token.shoko_file_id)
        if len(marks) == N_TWO:
            raise RuntimeError("simulated crash after prepare_send")
        original(token, iso=iso)

    monkeypatch.setattr(store, "mark_sent", crashing_mark_sent)
    try:
        deps = eh.build_deps(
            world, store, clock, max_submissions=N_THREE, monotonic=monotonic, advance=advance
        )
        with pytest.raises(RuntimeError, match="simulated crash"):
            _run_apply(deps, clock, "c1")
        plan = store.recover_after_restart()
        replannable = sorted(row.shoko_file_id for row in plan.replannable)
        polling = sorted(row.shoko_file_id for row in plan.continue_polling)
        view_after_crash = store.budget_view()
    finally:
        store.close()
    assert replannable == [NEW_B]
    assert polling == [NEW_A]
    assert view_after_crash.pending_file_requests == N_TWO
    _finish_crash_cycle(world, clock, monkeypatch)


def _finish_crash_cycle(
    world: eh.World,
    clock: eh.FakeClock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reopened = eh.open_store(world)
    world.shoko.link_on_post = True
    posted = world.shoko.posts[0][1]
    world.shoko.materialize(NEW_A, posted)
    deps2 = eh.build_deps(world, reopened, clock, max_submissions=N_THREE)
    second = _run_apply(deps2, clock, "c2", 60)
    void_after_replan = reopened.connection.execute(
        "SELECT COUNT(*) FROM sends WHERE outcome = 'voided'"
    ).fetchone()
    flag_after_drop = reopened.get_meta("requires_rebaseline")
    third = _run_apply(deps2, clock, "c3", 120)
    rows = [reopened.get_file(file_id) for file_id in (NEW_A, NEW_B, NEW_C)]
    voided = reopened.connection.execute(
        "SELECT COUNT(*) FROM sends WHERE outcome = 'voided'"
    ).fetchone()
    final_view = reopened.budget_view()
    reopened.close()
    assert second.counts.submitted == 0
    assert void_after_replan[0] == 1
    assert flag_after_drop == "1"
    assert third.counts.submitted == N_TWO
    assert world.shoko.posts_count() == N_THREE
    assert sorted(file_id for file_id, _ in world.shoko.posts) == [NEW_A, NEW_B, NEW_C]
    assert rows[0] is not None
    assert rows[0].state is FileState.VERIFIED
    assert rows[1] is not None
    assert rows[1].state is FileState.VERIFIED
    assert rows[2] is not None
    assert rows[2].state is FileState.VERIFIED
    assert voided[0] == 1
    assert final_view.pending_file_requests == 0


# ------------------------------------------------------- 17. campaign on the 7k world


def _campaign_batch(
    env: eh.CampaignEnv,
    batch: int,
    counters: dict[str, int],
) -> dict[str, Any]:
    code, payload = env.apply(batch=batch)
    assert code == cli.EXIT_OK, payload
    reasons = list(payload["reasons"])
    # M9-e2e fix: the campaign's own accepted links shrink the Unrecognized discovery total.
    # Continuity treats that as NORMAL (net of the linker's links): no rebaseline flag, no
    # source discontinuity, and eligibility (campaign status) stays open between batches.
    assert "discovery_total_drop" not in reasons, reasons
    assert not any("source_discontinuity" in reason for reason in reasons), reasons
    assert not any("source_unstable" in reason for reason in reasons), reasons
    store = env.open_store()
    try:
        assert store.get_meta("requires_rebaseline") != "1", reasons
    finally:
        store.close()
    assert env.report(str(payload["campaign_id"]))["status"] == "approved", reasons
    counters["submitted"] += int(payload["submitted"])
    counters["verified"] += int(payload["verified"])
    counters["quarantined"] = int(payload["quarantined"])
    return cast("dict[str, Any]", payload)


def test_campaign_canary_batches_quarantine_and_revoke(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    xml = eh.xml_document(eh.range_entry(4001, 300001, 1, PER_SERIES_HUNDRED))
    world, clock, _lease = _lease_world(tmp_path, xml)
    eh.seed_backlog(world, SERIES_SEVENTY, PER_SERIES_HUNDRED)
    world.shoko.pending.add(4001 * 1000 + PEND_EP)
    env = _campaign_env(tmp_path, world, clock, monkeypatch, capsys)
    prepared = env.prepare(limit=N_TWELVE)
    assert prepared["files"] == N_TWELVE
    assert world.shoko.posts_count() == 0
    env.approve(str(prepared["approval_digest"]))
    counters = {"submitted": 0, "verified": 0, "quarantined": 0}
    _campaign_batch(env, BATCH_TWO, counters)
    victim = world.shoko.rows[11011]
    victim.size = victim.size + 1
    _campaign_batch(env, BATCH_TWO, counters)
    world.shoko.pending.discard(4001 * 1000 + PEND_EP)
    _campaign_batch(env, BATCH_TWO, counters)
    _campaign_batch(env, BATCH_THREE, counters)
    _campaign_batch(env, BATCH_THREE, counters)
    assert counters["submitted"] == N_TWELVE - 1
    assert counters["verified"] == N_TWELVE - 1
    assert counters["quarantined"] == 1
    assert world.shoko.posts_count() == N_TWELVE - 1
    report = env.report(str(prepared["campaign_id"]))
    assert report["status"] == "approved"
    assert report["plan_matches_file"] is True
    assert report["total_sent"] == N_TWELVE - 1
    assert report["verified"] == N_TWELVE - 1
    assert report["quarantined"] == 1
    second = env.prepare(limit=N_FIVE)
    assert second["campaign_id"] != prepared["campaign_id"]
    assert second["files"] == N_FIVE
    env.approve(str(second["approval_digest"]))
    assert env.revoke(str(second["campaign_id"])) == cli.EXIT_OK
    capsys.readouterr()
    code, _payload = env.apply(batch=BATCH_TWO)
    assert code == cli.EXIT_APPLY_GATE_REFUSAL


def test_daemon_drains_approved_campaign_end_to_end_after_canary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """F7 (real machinery): after ONE manual canary the daemon drains the approved
    campaign across bounded batches with ZERO further CLI invocations, all files verified."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    xml = eh.xml_document(eh.range_entry(4001, 300001, 1, PER_SERIES_HUNDRED))
    # World campaign: drain on, small bounded batch (2) so many daemon batches are exercised.
    world, clock, _lease = _lease_world(
        tmp_path,
        xml,
        campaign={"max_campaign_files": 12, "max_batch_size": 2, "daemon_auto_apply": True},
    )
    eh.seed_backlog(world, 1, 12)  # a 12-file backlog (single mapped series)
    env = _campaign_env(tmp_path, world, clock, monkeypatch, capsys, campaign_max=12, batch=2)
    prepared = env.prepare(limit=12)
    assert prepared["files"] == 12
    assert world.shoko.posts_count() == 0  # prepare is dry-run
    env.approve(str(prepared["approval_digest"]))
    capsys.readouterr()
    # The operator's manual canary (one file) opens the daemon drain.
    code, payload = env.apply(batch=1)
    assert code == cli.EXIT_OK
    assert payload["submitted"] == 1
    assert world.shoko.posts_count() == 1
    # The daemon cycles drain the remaining 11 files automatically, ≤2 POSTs per cycle.
    store = env.open_store()
    try:
        deps = eh.build_deps(world, store, clock, max_submissions=2)
        for index in range(8):
            report = _run_apply(deps, clock, f"drain-{index}")
            if report.campaign is not None:
                assert report.campaign.submitted <= 2  # bounded batch
        campaign = store.get_campaign(str(prepared["campaign_id"]))
    finally:
        store.close()
    assert campaign is not None
    assert campaign.total_sent == 12  # fully drained with no further manual invocations
    assert world.shoko.posts_count() == 12
    report = env.report(str(prepared["campaign_id"]))
    assert report["total_sent"] == 12
    assert report["verified"] == 12


# ------------------------------------------- 17b. Imported=null backlog progress


def _set_representative_imported_null(world: eh.World) -> set[int]:
    # Mark a deterministic portion of the fully hashed backlog as never-imported
    # (Imported=null); the rest keep real timestamps (mixed, as in the real backlog).
    null_ids: set[int] = set()
    for file_id, row in world.shoko.rows.items():
        if file_id % 2 == 0:
            row.imported = None
            null_ids.add(file_id)
    return null_ids


def test_imported_null_backlog_files_progress_through_full_pipeline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    xml = eh.xml_document(eh.range_entry(4001, 300001, 1, N_TEN))
    world, clock, _lease = _lease_world(tmp_path, xml)
    eh.seed_backlog(world, N_SERIES, N_TEN)
    null_ids = _set_representative_imported_null(world)
    assert null_ids
    assert null_ids < set(world.shoko.rows)
    env = _campaign_env(
        tmp_path, world, clock, monkeypatch, capsys, campaign_max=LIMIT_TWENTY, batch=N_TEN
    )
    prepared = env.prepare(limit=N_TEN)
    assert prepared["files"] == N_TEN
    assert world.shoko.posts_count() == 0
    env.approve(str(prepared["approval_digest"]))
    code, payload = env.apply(batch=N_TEN)
    assert code == cli.EXIT_OK
    assert payload["submitted"] == N_TEN
    assert payload["verified"] == N_TEN
    assert world.shoko.posts_count() == N_TEN
    linked = {file_id for file_id, _ids in world.shoko.posts}
    assert null_ids <= linked
    methods = {method for method, _path in world.shoko.requests}
    assert methods <= {"GET", "POST"}
    store = env.open_store()
    try:
        verified_null = [store.get_file(file_id) for file_id in sorted(null_ids)]
    finally:
        store.close()
    assert verified_null
    for record in verified_null:
        assert record is not None
        assert record.state is FileState.VERIFIED


# ------------------------------------------------- 18. lease-expiry degradation


def test_lease_expiry_degrades_daemon_to_read_only_then_reattest_resumes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    extra = _new_arrival_xml(11) + _new_arrival_xml(12)
    world, clock, lease = _small_mapped_world(tmp_path, extra_xml=extra)
    store = eh.open_store(world)
    _seed_baseline(store, _backlog_specs())
    _add_new_arrival(world, 11, NEW_A)
    _add_new_arrival(world, 12, NEW_B)
    try:
        deps = eh.build_deps(world, store, clock, use_lease_verdict=True, max_submissions=N_SERIES)
        first = _run_apply(deps, clock, "c1")
        assert first.counts.submitted == 1
        assert world.shoko.posts_count() == 1
        clock.jump(7 * 24 * 3600.0 + 60)
        second = _run_apply(deps, clock, "c2")
        paused = store.budget_view().paused_reason
        third = _run_apply(deps, clock, "c3", 300)
        third_posts = world.shoko.posts_count()
        third_paused = store.budget_view().paused_reason
        eh.record_lease(lease, inspected_iso=clock.iso())
        fourth = _run_apply(deps, clock, "c4", 600)
        resumed_paused = store.budget_view().paused_reason
    finally:
        store.close()
    assert second.alive is True
    assert second.writes_suppressed is True
    assert second.apply_eligible is False
    assert "attestation_expired" in second.reasons
    assert "attestation_pause_persisted" in second.reasons
    assert paused == "attestation_expired"
    assert third.counts.submitted == 0
    assert third_posts == 1
    assert "attestation_pause_active" in third.reasons
    assert third_paused == "attestation_expired"
    assert "attestation_resumed" in fourth.reasons
    assert resumed_paused is None
    assert fourth.counts.submitted == 1
    assert world.shoko.posts_count() == N_TWO


# ------------------------------------------------------- 19. excluded roots


def test_excluded_live_action_series_is_never_contacted(tmp_path: Path) -> None:
    world, clock, _lease = _small_mapped_world(tmp_path, per_series=N_TWO)
    world.sonarr.add_series(LIVE_SERIES, eh.LIVE_ROOT, 777777)
    world.sonarr.add_episode(
        LIVE_SERIES,
        {
            "id": 1,
            "seriesId": LIVE_SERIES,
            "seasonNumber": 1,
            "episodeNumber": 1,
            "title": "Live",
            "tvdbId": 1,
            "episodeFileId": 9001,
        },
    )
    path = f"{eh.LIVE_ROOT}/series900/file9001.mkv"
    world.sonarr.add_file(
        LIVE_SERIES, {"id": 9001, "seriesId": LIVE_SERIES, "path": path, "size": 5}
    )
    world.sonarr.roots.append(eh.LIVE_ROOT)
    store = eh.open_store(world)
    try:
        deps = eh.build_deps(world, store, clock, max_submissions=LIMIT_TWENTY)
        _run_apply(deps, clock, "c1")
        sonarr, _shoko, _source = world.endpoints()
        inventory = collect_sonarr_inventory(sonarr, world.settings)
    finally:
        store.close()
    secondary = [p for p, series_id in world.sonarr.requests if series_id == LIVE_SERIES]
    assert secondary == []
    assert world.shoko.posts_count() == 0
    assert [item.id for item in inventory.excluded_series] == [LIVE_SERIES]
    assert any(
        "outside the configured anime allowlist" in warning for warning in inventory.warnings
    )


# ---------------------------------------------------------- 20. Sonarr outage


def test_sonarr_outage_cycle_is_read_only_then_recovery_resumes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    extra = _new_arrival_xml(11) + _new_arrival_xml(12)
    world, clock, _lease = _small_mapped_world(tmp_path, extra_xml=extra)
    store = eh.open_store(world)
    _seed_baseline(store, _backlog_specs())
    _add_new_arrival(world, 11, NEW_A)
    _add_new_arrival(world, 12, NEW_B)
    try:
        deps = eh.build_deps(world, store, clock, max_submissions=N_SERIES)
        first = _run_apply(deps, clock, "c1")
        assert first.counts.submitted == 1
        world.sonarr.down = True
        outage = _run_apply(deps, clock, "c2", 300)
        posts_during_outage = world.shoko.posts_count()
        world.sonarr.down = False
        recovered = _run_apply(deps, clock, "c3", 600)
        resumed = _run_apply(deps, clock, "c4", 900)
    finally:
        store.close()
    assert posts_during_outage == 1
    assert outage.reconciliation_ready is False
    assert outage.writes_suppressed is True
    assert outage.counts.submitted == 0
    # M9-e2e fix: the outage's discovery-total drop is explained by the cycle-1
    # accepted link (NEW_A), so continuity stays clean on recovery — the first healthy cycle
    # resumes applying (NEW_B) instead of being wedged by a false discontinuity.
    assert recovered.counts.submitted == 1
    assert resumed.counts.submitted == 0
    assert world.shoko.posts_count() == N_TWO


# ------------------------------------------------------------- measured performance


@pytest.mark.slow
def test_measured_full_world_first_scan_campaign_and_daemon_cycles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    xml = eh.xml_document(eh.range_entry(4001, 300001, 1, PER_SERIES_HUNDRED))
    world, clock, _lease = _lease_world(
        tmp_path, xml, campaign={"max_campaign_files": LIMIT_HUNDRED, "daemon_auto_apply": False}
    )
    specs = eh.seed_backlog(world, SERIES_SEVENTY, PER_SERIES_HUNDRED)
    assert len(specs) == BACKLOG_FILES
    # F7: this perf scenario measures first-scan + ONE campaign batch + IDLE daemon cycles
    # (new-arrival only). The daemon auto-drain is exercised deterministically in
    # test_daemon_campaign_auto_drain; here it is off so the per-file GET ceiling
    # measures the first-scan + single-batch cost, unchanged by repeated drain sweeps.
    env = _campaign_env(tmp_path, world, clock, monkeypatch, capsys, auto_drain=False)
    submissions = 0
    with eh.measure(world) as stats:
        store = eh.open_store(world)
        try:
            deps = eh.build_deps(world, store, clock, max_submissions=N_SERIES)
            first = _run_apply(deps, clock, "perf-c1")
            assert first.counts.backlog == BACKLOG_FILES
        finally:
            store.close()
    first_scan_wall = stats.wall_seconds
    started = time.perf_counter()
    prepared = env.prepare(limit=LIMIT_HUNDRED)
    env.approve(str(prepared["approval_digest"]))
    code, payload = env.apply(batch=BATCH_TEN)
    assert code == cli.EXIT_OK
    submissions = int(payload["submitted"])
    # F1: the campaign batch acquires the single-process apply lock and releases it, so
    # it is not lock-deferred and the daemon cycles below re-acquire the same lock freely.
    assert not any("apply_lock_contended" in reason for reason in payload["reasons"])
    daemon_store = eh.open_store(world)
    try:
        deps2 = eh.build_deps(world, daemon_store, clock, max_submissions=N_SERIES)
        for index in range(5):
            _run_apply(deps2, clock, f"perf-daemon-{index}", 300 * (index + 1))
    finally:
        daemon_store.close()
    rest_wall = time.perf_counter() - started
    stats.wall_seconds = first_scan_wall + rest_wall
    stats.sonarr_requests = world.sonarr.get_counts()
    stats.shoko_gets = world.shoko.get_counts()
    stats.shoko_posts = world.shoko.posts_count()
    stats.source_requests = len(world.source.requests)
    total_gets = stats.shoko_gets + stats.sonarr_requests + stats.source_requests
    per_file = total_gets / BACKLOG_FILES
    print(
        f"E2E-PERF first_scan={first_scan_wall:.2f}s campaign_daemon={rest_wall:.2f}s "
        f"wall={stats.wall_seconds:.2f}s tracemalloc_peak={stats.tracemalloc_peak_bytes}B "
        f"gets={total_gets} posts={stats.shoko_posts} per_file_gets={per_file:.2f} "
        f"submissions={submissions}"
    )
    assert stats.wall_seconds < WALL_BUDGET_SECONDS
    assert per_file < GETS_PER_FILE_CEILING
    assert stats.shoko_posts == submissions == BATCH_TEN


def test_e2e_module_scenario_matrix_is_complete() -> None:
    source = Path(__file__).read_text(encoding="utf-8")
    expected = [
        "test_first_run_seven_k_backlog_zero_posts_cohort_split",
        "test_new_sonarr_file_after_baseline_links_and_verifies",
        "test_old_file_late_in_shoko_stays_backlog_cohort",
        "test_split_cour_absolute_and_season_zero_groups",
        "test_aot_real_fixture_split_cour_anchor",
        "test_ambiguous_mapping_refuses_file_and_pending_target_defers",
        "test_pending_target_applies_after_shoko_metadata_created",
        "test_mixed_ownership_location_drops_file_with_zero_writes",
        "test_existing_wrong_manual_link_is_protected_and_audited",
        "test_weekly_refresh_304_then_activated_remaps_backlog_without_posts",
        "test_corrupt_mapping_source_keeps_last_known_good_with_backoff",
        "test_override_add_moves_ambiguous_file_to_planned_without_posts",
        "test_file_id_reuse_detected_and_rebaseline_flag_clears",
        "test_blocked_queue_zero_posts_with_tolerance_zero",
        "test_huge_benign_queue_applies_within_budget_caps",
        "test_async_xrefs_materialize_late_and_verify_across_cycles",
        "test_partial_materialization_past_deadline_releases_slot_per_ledger",
        "test_lost_post_connection_reset_indeterminate_then_xref_inspection_closes",
        "test_lost_post_http_502_indeterminate_then_partial_close_releases_slot",
        "test_crash_between_prepare_and_send_replans_once_without_double_post",
        "test_campaign_canary_batches_quarantine_and_revoke",
        "test_lease_expiry_degrades_daemon_to_read_only_then_reattest_resumes",
        "test_excluded_live_action_series_is_never_contacted",
        "test_sonarr_outage_cycle_is_read_only_then_recovery_resumes",
        "test_measured_full_world_first_scan_campaign_and_daemon_cycles",
    ]
    missing = [name for name in expected if f"def {name}(" not in source]
    assert missing == []
