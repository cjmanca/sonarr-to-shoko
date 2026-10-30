"""Sonarr inventory and baseline (M2): root-scoped eligibility, indexing, baseline validity.

Every request goes through the real http.py redirect/auth policy over an
httpx.MockTransport: nothing here touches the network. The request log per test
proves which API calls actually happened (excluded series must produce zero
secondary requests).
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

from shoko_linker.config import Settings, load_settings
from shoko_linker.http import ApiEndpoint, make_timeout, sonarr_headers
from shoko_linker.sonarr import (
    EligibleSeries,
    EpisodeParseResult,
    FileIndexResult,
    FileParseResult,
    FileRejectReason,
    RebaselineReason,
    SonarrEpisode,
    SonarrFileRecord,
    SonarrInventory,
    SonarrSeries,
    SourceRelationshipStatus,
    VerdictReason,
    build_baseline,
    check_current_file_relationship,
    collect_sonarr_inventory,
    compare_baseline,
    index_series_files,
    parse_episode_entries,
    parse_file_entries,
    parse_series_entries,
    select_eligible_series,
    validate_root_folders,
)

FAKE_KEY = "fake-key-never-a-real-one-0000"
BASE_URL = "http://sonarr:8989"
ANIME_ROOT = "/data/media/anime"
EPISODE_PATH = "/api/v3/episode"
EPISODE_FILE_PATH = "/api/v3/episodefile"
SECONDARY_PATHS = frozenset({EPISODE_PATH, EPISODE_FILE_PATH})
SERIES_ANIME = 1
SERIES_LIVE = 2
SERIES_OLD = 3
SERIES_TOKU = 4
SERIES_BROKEN = 5
FILE_SPECIAL = 100
FILE_PAIR = 101
FILE_ORPHAN = 102
PAIR_FILE_SIZE = 2500
EPISODE_RECORD_COUNT = 5
INDEXED_FILE_COUNT = 3
COLLISION_REJECT_COUNT = 2
HOPS_AFTER_ONE_REDIRECT = 2

Handler = Callable[[httpx.Request], httpx.Response]
Writer = Callable[[dict[str, Any]], Path]


def sonarr_endpoint(handler: Handler, base_url: str = BASE_URL) -> ApiEndpoint:
    client = httpx.Client(
        base_url=base_url.rstrip("/") + "/",
        transport=httpx.MockTransport(handler),
        follow_redirects=False,
        timeout=make_timeout(1.0, 1.0),
    )
    return ApiEndpoint(client, lambda: sonarr_headers(FAKE_KEY))


class SonarrStub:
    """Programmable offline Sonarr v3 API with a request log."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.series: list[dict[str, Any]] = []
        self.episodes: dict[int, list[dict[str, Any]]] = {}
        self.files: dict[int, list[dict[str, Any]]] = {}
        self.rootfolders: list[dict[str, Any]] | None = [{"path": ANIME_ROOT}]
        self.series_status = 200
        self.episodes_status: dict[int, int] = {}
        self.files_status: dict[int, int] = {}

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        if path == "/api/v3/series":
            return httpx.Response(self.series_status, json=self.series, request=request)
        if path == "/api/v3/rootfolder":
            if self.rootfolders is None:
                return httpx.Response(404, json={"message": "gone"}, request=request)
            return httpx.Response(200, json=self.rootfolders, request=request)
        if path in SECONDARY_PATHS:
            series_id = _series_param(request)
            table = self.episodes if path == EPISODE_PATH else self.files
            statuses = self.episodes_status if path == EPISODE_PATH else self.files_status
            status = statuses.get(series_id, 200)
            return httpx.Response(status, json=table.get(series_id, []), request=request)
        return httpx.Response(404, json={"message": "unknown path"}, request=request)

    def secondary_series_ids(self) -> set[int]:
        secondary = [r for r in self.requests if r.url.path in SECONDARY_PATHS]
        return {_series_param(request) for request in secondary}


def _series_param(request: httpx.Request) -> int:
    raw = request.url.params.get("seriesId")
    return int(raw) if raw is not None else -1


def settings_with_roots(
    base_document: dict[str, Any],
    write_document: Writer,
    roots: list[str],
) -> Settings:
    base_document["sonarr"]["allowed_roots"] = roots
    return load_settings(write_document(base_document))


def series_entry(
    id_: int,
    title: str,
    root: str,
    type_: str = "anime",
    tvdb_id: int | None = None,
) -> dict[str, Any]:
    return {
        "id": id_,
        "title": title,
        "rootFolderPath": root,
        "seriesType": type_,
        "tvdbId": tvdb_id if tvdb_id is not None else 1000 + id_,
    }


def episode_entry(
    id_: int,
    series_id: int,
    season: int,
    number: int,
    title: str,
    file_id: int | None = 0,
    absolute: int | None = None,
    tvdb_id: int | None = None,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "id": id_,
        "seriesId": series_id,
        "seasonNumber": season,
        "episodeNumber": number,
        "title": title,
        "absoluteEpisodeNumber": absolute,
        "tvdbId": tvdb_id if tvdb_id is not None else 5000 + id_,
    }
    if file_id is not None:
        entry["episodeFileId"] = file_id
    return entry


def file_entry(id_: int, series_id: int, path: str, size: int) -> dict[str, Any]:
    return {"id": id_, "seriesId": series_id, "path": path, "size": size}


def standard_stub() -> SonarrStub:
    stub = SonarrStub()
    stub.series = [
        series_entry(SERIES_ANIME, "Anime A", f"{ANIME_ROOT}/Anime A"),
        series_entry(SERIES_LIVE, "Live Drama", "/data/media/tv/Drama", type_="standard"),
    ]
    stub.episodes = {
        SERIES_ANIME: [
            episode_entry(10, SERIES_ANIME, 0, 1, "Special", file_id=FILE_SPECIAL, absolute=None),
            episode_entry(11, SERIES_ANIME, 1, 1, "E01", file_id=FILE_PAIR, absolute=1),
            episode_entry(12, SERIES_ANIME, 1, 2, "E02", file_id=FILE_PAIR, absolute=2),
            episode_entry(13, SERIES_ANIME, 1, 3, "E03", file_id=0),
            episode_entry(14, SERIES_ANIME, 1, 4, "E04", file_id=None),
        ]
    }
    stub.files = {
        SERIES_ANIME: [
            file_entry(FILE_SPECIAL, SERIES_ANIME, f"{ANIME_ROOT}/Anime A/S00/E01.mkv", 1000),
            file_entry(
                FILE_PAIR,
                SERIES_ANIME,
                f"{ANIME_ROOT}/Anime A/S01/E01-E02.mkv",
                PAIR_FILE_SIZE,
            ),
            file_entry(FILE_ORPHAN, SERIES_ANIME, f"{ANIME_ROOT}/Anime A/orphan.mkv", 700),
        ]
    }
    return stub


def collect(stub: SonarrStub, settings: Settings) -> SonarrInventory:
    endpoint = sonarr_endpoint(stub.handler)
    try:
        return collect_sonarr_inventory(endpoint, settings)
    finally:
        endpoint.client.close()


def test_read_only_scope_inventory_does_not_require_an_unconfirmed_path_mapping() -> None:
    stub = standard_stub()
    endpoint = sonarr_endpoint(stub.handler)
    progress: list[tuple[int, int]] = []
    try:
        inventory = collect_sonarr_inventory(
            endpoint,
            allowed_roots=(ANIME_ROOT,),
            anime_only=True,
            on_progress=lambda done, total: progress.append((done, total)),
        )
    finally:
        endpoint.client.close()
    assert inventory.verdict.complete is True
    assert progress == [(0, 1), (1, 1)]
    assert {file.series_id for file in inventory.files} == {SERIES_ANIME}
    child_requests = [request for request in stub.requests if request.url.path in SECONDARY_PATHS]
    assert {request.url.params["seriesId"] for request in child_requests} == {str(SERIES_ANIME)}


def test_excluded_series_produce_no_secondary_requests(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = standard_stub()
    stub.series.append(series_entry(SERIES_OLD, "Anime Old", "/data/media/anime-old/Show"))
    stub.episodes[SERIES_OLD] = [episode_entry(30, SERIES_OLD, 1, 1, "Never Fetched")]
    stub.files[SERIES_OLD] = [file_entry(300, SERIES_OLD, "/data/media/anime-old/Show/x.mkv", 1)]
    inventory = collect(stub, settings_with_roots(base_document, write_document, [ANIME_ROOT]))
    assert {item.series.id for item in inventory.eligible_series} == {SERIES_ANIME}
    assert {item.id for item in inventory.excluded_series} == {SERIES_LIVE, SERIES_OLD}
    assert [request.url.path for request in stub.requests].count("/api/v3/series") == 1
    # Live-action (series 2) and the /anime-old prefix sibling (series 3)
    # never trigger /episode or /episodefile reads.
    assert stub.secondary_series_ids() == {SERIES_ANIME}
    assert inventory.verdict.complete is True
    assert inventory.verdict.auto_apply_eligible is True


def test_changed_series_manifest_discards_cached_children(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = standard_stub()
    extra_id = 6
    extra_root = f"{ANIME_ROOT}/Extra"
    stub.series.append(series_entry(extra_id, "Extra", extra_root))
    stub.episodes[extra_id] = [episode_entry(60, extra_id, 1, 1, "E01", file_id=600)]
    stub.files[extra_id] = [file_entry(600, extra_id, f"{extra_root}/E01.mkv", 6000)]
    settings = settings_with_roots(base_document, write_document, [ANIME_ROOT])
    endpoint = sonarr_endpoint(stub.handler)
    try:
        first = collect_sonarr_inventory(endpoint, settings)
        assert first.verdict.complete is True
        assert first.series_manifest_hash is not None
        assert first.fetched_series_children is not None

        stub.series[0]["tvdbId"] = 9999
        changed = collect_sonarr_inventory(
            endpoint,
            settings,
            cached_manifest_hash=first.series_manifest_hash,
            cached_series_children=first.fetched_series_children,
            series_cursor=extra_id,
            swept_series_ids=first.swept_series_ids,
            series_deadline=0.0,
            monotonic=lambda: 0.0,
        )
    finally:
        endpoint.client.close()

    assert changed.series_cursor == 0
    assert changed.files == ()
    assert changed.verdict.complete is False
    assert VerdictReason.INCOMPLETE_SNAPSHOT in changed.verdict.reasons


def test_failed_refresh_invalidates_previously_swept_child_fragment(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = standard_stub()
    settings = settings_with_roots(base_document, write_document, [ANIME_ROOT])
    endpoint = sonarr_endpoint(stub.handler)
    try:
        first = collect_sonarr_inventory(endpoint, settings)
        assert first.verdict.complete is True
        assert first.swept_series_ids == frozenset({SERIES_ANIME})

        stub.episodes_status[SERIES_ANIME] = 503
        failed = collect_sonarr_inventory(
            endpoint,
            settings,
            cached_manifest_hash=first.series_manifest_hash,
            cached_series_children=first.fetched_series_children,
            series_cursor=first.series_cursor,
            swept_series_ids=first.swept_series_ids,
        )
    finally:
        endpoint.client.close()

    assert failed.verdict.complete is False
    assert failed.files == ()
    assert failed.swept_series_ids == frozenset()
    assert failed.refresh_sweep_complete is False


def test_failed_fragment_cannot_complete_a_later_rotation_from_old_sweep_membership(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = standard_stub()
    for series_id in (6, 7, 8):
        root = f"{ANIME_ROOT}/Series {series_id}"
        stub.series.append(series_entry(series_id, f"Series {series_id}", root))
        stub.episodes[series_id] = [episode_entry(series_id * 10, series_id, 1, 1, "E01")]
        stub.files[series_id] = [
            file_entry(series_id * 100, series_id, f"{root}/E01.mkv", series_id * 1000)
        ]
    settings = settings_with_roots(base_document, write_document, [ANIME_ROOT])
    elapsed = [0.0]

    def timed_handler(request: httpx.Request) -> httpx.Response:
        response = stub.handler(request)
        if request.url.path in SECONDARY_PATHS:
            elapsed[0] += 1.0
        return response

    endpoint = sonarr_endpoint(timed_handler)
    try:
        first = collect_sonarr_inventory(endpoint, settings)
        assert first.verdict.complete is True

        # Persist one successful child refresh as an incomplete sweep, like a bounded
        # apply slice.  The remaining cached fragments are from the earlier full sweep.
        elapsed[0] = 0.0
        partial = collect_sonarr_inventory(
            endpoint,
            settings,
            cached_manifest_hash=first.series_manifest_hash,
            cached_series_children=first.fetched_series_children,
            series_cursor=first.series_cursor,
            swept_series_ids=frozenset(),
            series_deadline=2.0,
            monotonic=lambda: elapsed[0],
        )
        assert partial.swept_series_ids == frozenset({SERIES_ANIME})
        assert partial.refresh_sweep_complete is False

        # All child refreshes fail.  The previously successful series 1 fragment and
        # its sweep membership must both be invalidated, not carried into the next pass.
        stub.episodes_status.update({series_id: 503 for series_id in (1, 6, 7, 8)})
        failed = collect_sonarr_inventory(
            endpoint,
            settings,
            cached_manifest_hash=first.series_manifest_hash,
            cached_series_children=first.fetched_series_children,
            series_cursor=partial.series_cursor,
            swept_series_ids=partial.swept_series_ids,
        )

        # Series 1 remains unavailable while the rest recover.  A stale cache plus the
        # old sweep bit would let series 6-8 finish a false-complete inventory before
        # series 1 is ever re-read.
        stub.episodes_status = {1: 503}
        remaining_cache = first.fetched_series_children if failed.files else {}
        recovered = collect_sonarr_inventory(
            endpoint,
            settings,
            cached_manifest_hash=first.series_manifest_hash,
            cached_series_children=remaining_cache,
            series_cursor=failed.series_cursor,
            swept_series_ids=failed.swept_series_ids,
        )
    finally:
        endpoint.client.close()

    assert failed.files == ()
    assert failed.swept_series_ids == frozenset()
    assert recovered.verdict.complete is False
    assert recovered.refresh_sweep_complete is False
    assert SERIES_ANIME not in recovered.swept_series_ids
    assert all(file.series_id != SERIES_ANIME for file in recovered.files)


@pytest.mark.parametrize("anime_only", [None, False, True])
@pytest.mark.parametrize("series_type", ["standard", "daily", "unknown", "", None])
def test_all_inventory_paths_admit_known_types_only_in_selected_root(
    base_document: dict[str, Any],
    write_document: Writer,
    anime_only: bool | None,
    series_type: str | None,
) -> None:
    settings = settings_with_roots(base_document, write_document, [ANIME_ROOT])
    stub = standard_stub()
    shared_live_id = 700
    stub.series.append(
        series_entry(
            shared_live_id,
            "Live Action in Shared Root",
            f"{ANIME_ROOT}/Live",
            type_=series_type or "",
        )
    )
    if series_type is None:
        stub.series[-1].pop("seriesType")
    stub.episodes[shared_live_id] = [
        episode_entry(7001, shared_live_id, 1, 1, "Drama E01", file_id=7000)
    ]
    stub.files[shared_live_id] = [
        file_entry(7000, shared_live_id, f"{ANIME_ROOT}/Live/S01E01.mkv", 900)
    ]
    stub.series.append(series_entry(701, "Unselected daily", "/data/media/anime-old/Show", "daily"))

    endpoint = sonarr_endpoint(stub.handler)
    try:
        inventory = collect_sonarr_inventory(endpoint, settings, anime_only=anime_only)
    finally:
        endpoint.client.close()

    if series_type in {"standard", "daily"}:
        assert {item.series.id for item in inventory.eligible_series} == {
            SERIES_ANIME,
            shared_live_id,
        }
        assert stub.secondary_series_ids() == {SERIES_ANIME, shared_live_id}
        assert any(file.episode_file_id == 7000 for file in inventory.files)
    else:
        assert {item.series.id for item in inventory.eligible_series} == {SERIES_ANIME}
        assert stub.secondary_series_ids() == {SERIES_ANIME}
    if series_type == "unknown":
        assert shared_live_id in {item.id for item in inventory.excluded_series}
    assert 701 in {item.id for item in inventory.excluded_series}
    assert SERIES_LIVE in {item.id for item in inventory.excluded_series}


def test_series_parser_records_verified_series_type() -> None:
    result = parse_series_entries([series_entry(SERIES_ANIME, "Anime", ANIME_ROOT, type_="anime")])

    assert result.reasons == ()
    assert result.series[0].series_type == "anime"


@pytest.mark.parametrize(
    "duplicate_fields",
    [{}, {"tvdbId": 999999}, {"title": None}, {"seriesType": "standard"}],
    ids=["identical", "conflicting-identity", "malformed", "conflicting-scope"],
)
@pytest.mark.parametrize("reverse", [False, True])
def test_duplicate_series_ids_exclude_all_copies_before_fetching_children(
    base_document: dict[str, Any],
    write_document: Writer,
    duplicate_fields: dict[str, Any],
    reverse: bool,
) -> None:
    stub = standard_stub()
    duplicate = dict(stub.series[0], **duplicate_fields)
    stub.series.append(duplicate)
    safe_root = f"{ANIME_ROOT}/Safe"
    stub.series.append(series_entry(SERIES_BROKEN, "Safe", safe_root))
    stub.episodes[SERIES_BROKEN] = [episode_entry(50, SERIES_BROKEN, 1, 1, "E01", file_id=500)]
    stub.files[SERIES_BROKEN] = [file_entry(500, SERIES_BROKEN, f"{safe_root}/E01.mkv", 10)]
    if reverse:
        stub.series.reverse()

    inventory = collect(stub, settings_with_roots(base_document, write_document, [ANIME_ROOT]))

    assert VerdictReason.INCONSISTENT_ENUMERATION in inventory.verdict.reasons
    assert any("repeats series id 1" in detail for detail in inventory.verdict.details)
    assert inventory.verdict.complete is False
    assert inventory.verdict.auto_apply_eligible is False
    assert {item.series.id for item in inventory.eligible_series} == {SERIES_BROKEN}
    assert stub.secondary_series_ids() == {SERIES_BROKEN}
    assert {(file.series_id, file.episode_file_id) for file in inventory.files} == {
        (SERIES_BROKEN, 500)
    }
    assert {episode.series_id for episode in inventory.episodes} == {SERIES_BROKEN}


def test_multiple_allowed_roots_admit_their_series(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    toku_root = "/data/media/tokusatsu"
    base_document["shoko"]["allowed_roots"].append("/tokusatsu")
    base_document["paths"]["mappings"].append({"sonarr": toku_root, "shoko": "/tokusatsu"})
    stub = standard_stub()
    stub.series.append(series_entry(SERIES_TOKU, "Toku Show", f"{toku_root}/Toku Show"))
    stub.episodes[SERIES_TOKU] = [episode_entry(40, SERIES_TOKU, 1, 1, "Toku E01", file_id=400)]
    toku_path = f"{toku_root}/Toku Show/S01E01.mkv"
    stub.files[SERIES_TOKU] = [file_entry(400, SERIES_TOKU, toku_path, 4200)]
    inventory = collect(
        stub, settings_with_roots(base_document, write_document, [ANIME_ROOT, toku_root])
    )
    assert {item.series.id for item in inventory.eligible_series} == {SERIES_ANIME, SERIES_TOKU}
    assert stub.secondary_series_ids() == {SERIES_ANIME, SERIES_TOKU}
    indexed = {(f.series_id, f.episode_file_id) for f in inventory.files}
    assert indexed >= {(SERIES_ANIME, FILE_PAIR), (SERIES_TOKU, 400)}
    assert inventory.verdict.complete is True


def test_season_zero_specials_and_multi_episode_grouping(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = settings_with_roots(base_document, write_document, [ANIME_ROOT])
    inventory = collect(standard_stub(), settings)
    by_id = {(f.series_id, f.episode_file_id): f for f in inventory.files}
    special = by_id[(SERIES_ANIME, FILE_SPECIAL)]
    assert special.episodes[0].season_number == 0
    pair = by_id[(SERIES_ANIME, FILE_PAIR)]
    assert tuple(e.id for e in pair.episodes) == (11, 12)
    assert pair.path == f"{ANIME_ROOT}/Anime A/S01/E01-E02.mkv"
    assert pair.size == PAIR_FILE_SIZE
    # Episodes 13 (episodeFileId 0) and 14 (key absent) are episodes, not files.
    assert all(e.id not in (13, 14) for f in inventory.files for e in f.episodes)
    assert len(inventory.episodes) == EPISODE_RECORD_COUNT
    # File 102 is referenced by no episode: flagged, with an empty episode group.
    assert inventory.unlinked_file_ids == ((SERIES_ANIME, FILE_ORPHAN),)
    assert by_id[(SERIES_ANIME, FILE_ORPHAN)].episodes == ()
    assert inventory.verdict.complete is True


def test_paths_escaping_the_allowed_root_are_rejected_not_indexed(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = standard_stub()
    stub.files[SERIES_ANIME] = [
        file_entry(FILE_SPECIAL, SERIES_ANIME, f"{ANIME_ROOT}/Anime A/../../secret/x.mkv", 1000),
        file_entry(FILE_PAIR, SERIES_ANIME, "/data/media/tv/leak.mkv", 2500),
        file_entry(FILE_ORPHAN, SERIES_ANIME, f"{ANIME_ROOT}/Anime A/ok.mkv", 700),
    ]
    stub.episodes[SERIES_ANIME] = [
        episode_entry(15, SERIES_ANIME, 1, 5, "E05", file_id=FILE_ORPHAN)
    ]
    inventory = collect(stub, settings_with_roots(base_document, write_document, [ANIME_ROOT]))
    reasons = {r.reason for r in inventory.rejected_files}
    assert FileRejectReason.INVALID_PATH in reasons
    assert FileRejectReason.OUTSIDE_ALLOWED_ROOT in reasons
    indexed = {(f.series_id, f.episode_file_id) for f in inventory.files}
    assert indexed == {(SERIES_ANIME, FILE_ORPHAN)}
    # Scope rejections are recorded, not enumeration failures: snapshot stays complete.
    assert inventory.verdict.complete is True


def test_malformed_file_record_fails_closed(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = standard_stub()
    del stub.files[SERIES_ANIME][1]["size"]
    inventory = collect(stub, settings_with_roots(base_document, write_document, [ANIME_ROOT]))
    rejected = [
        r for r in inventory.rejected_files if r.reason is FileRejectReason.MISSING_REQUIRED_FIELD
    ]
    assert len(rejected) == 1
    assert (rejected[0].series_id, rejected[0].episode_file_id) == (SERIES_ANIME, FILE_PAIR)
    assert all(f.episode_file_id != FILE_PAIR for f in inventory.files)
    assert VerdictReason.MALFORMED_RESPONSE in inventory.verdict.reasons
    assert inventory.verdict.complete is False
    assert inventory.verdict.auto_apply_eligible is False


def test_duplicate_file_ids_within_a_series_are_flagged(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = standard_stub()
    copy_path = f"{ANIME_ROOT}/Anime A/S00/E01 copy.mkv"
    stub.files[SERIES_ANIME].append(file_entry(FILE_SPECIAL, SERIES_ANIME, copy_path, 999))
    inventory = collect(stub, settings_with_roots(base_document, write_document, [ANIME_ROOT]))
    collisions = [
        r for r in inventory.rejected_files if r.reason is FileRejectReason.DUPLICATE_FILE_ID
    ]
    assert len(collisions) == COLLISION_REJECT_COUNT
    assert all(r.episode_file_id == FILE_SPECIAL for r in collisions)
    assert all(f.episode_file_id != FILE_SPECIAL for f in inventory.files)
    assert VerdictReason.FILE_ID_COLLISION in inventory.verdict.reasons
    assert inventory.verdict.auto_apply_eligible is False


def test_transport_failure_makes_the_snapshot_auto_apply_unsafe(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = standard_stub()
    stub.series.append(series_entry(SERIES_BROKEN, "Anime B", f"{ANIME_ROOT}/Anime B"))
    stub.episodes_status[SERIES_BROKEN] = 500
    stub.files[SERIES_BROKEN] = [
        file_entry(500, SERIES_BROKEN, f"{ANIME_ROOT}/Anime B/S01E01.mkv", 10)
    ]
    inventory = collect(stub, settings_with_roots(base_document, write_document, [ANIME_ROOT]))
    assert VerdictReason.TRANSPORT_FAILURE in inventory.verdict.reasons
    assert inventory.verdict.complete is False
    assert inventory.verdict.auto_apply_eligible is False
    assert any(f"series {SERIES_BROKEN}" in detail for detail in inventory.verdict.details)
    # Series 1 remains usable; the broken series contributes no files.
    indexed = {(f.series_id, f.episode_file_id) for f in inventory.files}
    assert indexed >= {(SERIES_ANIME, FILE_SPECIAL), (SERIES_ANIME, FILE_PAIR)}
    assert all(f.series_id != SERIES_BROKEN for f in inventory.files)


def test_series_enumeration_failure_stops_all_secondary_requests(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = standard_stub()
    stub.series_status = 503
    inventory = collect(stub, settings_with_roots(base_document, write_document, [ANIME_ROOT]))
    assert inventory.verdict.reasons == (VerdictReason.SERIES_ENUMERATION_FAILED,)
    assert inventory.verdict.auto_apply_eligible is False
    assert inventory.eligible_series == ()
    assert stub.secondary_series_ids() == set()


def test_rootfolder_listing_failure_only_warns(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = standard_stub()
    stub.rootfolders = None
    inventory = collect(stub, settings_with_roots(base_document, write_document, [ANIME_ROOT]))
    assert any("root folder listing unavailable" in warning for warning in inventory.warnings)
    assert inventory.verdict.complete is True
    assert len(inventory.files) == INDEXED_FILE_COUNT


def test_rootfolder_mismatch_is_a_warning_and_never_expands_scope(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = standard_stub()
    stub.rootfolders = [{"path": "/data/media/tv"}]
    inventory = collect(stub, settings_with_roots(base_document, write_document, [ANIME_ROOT]))
    warnings = inventory.warnings
    assert any(f"configured root {ANIME_ROOT!r}" in warning for warning in warnings)
    assert any("outside the configured anime allowlist" in warning for warning in warnings)
    # The Sonarr-managed TV root stays ineligible: no scope expansion.
    assert {item.series.id for item in inventory.eligible_series} == {SERIES_ANIME}
    assert inventory.verdict.complete is True


def test_rebaseline_when_file_size_changes_replacement(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = settings_with_roots(base_document, write_document, [ANIME_ROOT])
    stub = standard_stub()
    first = build_baseline(collect(stub, settings))
    assert first.complete is True
    assert first.auto_apply_eligible is True
    replaced = file_entry(FILE_PAIR, SERIES_ANIME, f"{ANIME_ROOT}/Anime A/S01/E01-E02.mkv", 9999)
    stub.files[SERIES_ANIME] = [stub.files[SERIES_ANIME][0], replaced, stub.files[SERIES_ANIME][2]]
    second = build_baseline(collect(stub, settings))
    verdict = compare_baseline(first, second)
    assert verdict.requires_rebaseline is True
    assert verdict.reasons == (RebaselineReason.FILE_IDENTITY_CHANGED,)
    assert any(f"file {FILE_PAIR}" in detail for detail in verdict.details)


def test_rename_same_size_is_path_drift_not_rebaseline(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = settings_with_roots(base_document, write_document, [ANIME_ROOT])
    stub = standard_stub()
    first = build_baseline(collect(stub, settings))
    moved = file_entry(FILE_PAIR, SERIES_ANIME, f"{ANIME_ROOT}/Anime A/other/E01-E02.mkv", 2500)
    stub.files[SERIES_ANIME] = [stub.files[SERIES_ANIME][0], moved, stub.files[SERIES_ANIME][2]]
    second = build_baseline(collect(stub, settings))
    verdict = compare_baseline(first, second)
    assert verdict.requires_rebaseline is False
    assert verdict.reasons == ()
    assert any(
        "renamed from" in detail and "path drift churn" in detail for detail in verdict.details
    )


def test_rebaseline_when_series_root_changes_title_alone_is_churn(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = settings_with_roots(base_document, write_document, [ANIME_ROOT])
    stub = standard_stub()
    first = build_baseline(collect(stub, settings))
    stub.series[0]["rootFolderPath"] = f"{ANIME_ROOT}/Renamed"
    second = build_baseline(collect(stub, settings))
    verdict = compare_baseline(first, second)
    assert verdict.requires_rebaseline is True
    assert verdict.reasons == (RebaselineReason.SERIES_IDENTITY_CHANGED,)


def test_title_only_edits_are_churn_and_renumbering_is_rebaseline(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = settings_with_roots(base_document, write_document, [ANIME_ROOT])
    stub = standard_stub()
    first = build_baseline(collect(stub, settings))
    stub.series[0]["title"] = "Wholly Different Title"
    stub.episodes[SERIES_ANIME][1]["title"] = "Not The Same Episode"
    second = build_baseline(collect(stub, settings))
    verdict = compare_baseline(first, second)
    assert verdict.requires_rebaseline is False
    assert any("title changed (metadata churn)" in detail for detail in verdict.details)
    stub.episodes[SERIES_ANIME][1]["seasonNumber"] = 9
    third = build_baseline(collect(stub, settings))
    renumbered = compare_baseline(first, third)
    assert renumbered.requires_rebaseline is True
    assert renumbered.reasons == (RebaselineReason.EPISODE_IDENTITY_CHANGED,)


def test_added_and_removed_files_are_churn_not_rebaseline(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = settings_with_roots(base_document, write_document, [ANIME_ROOT])
    stub = standard_stub()
    first = build_baseline(collect(stub, settings))
    stub.files[SERIES_ANIME] = [
        file_entry(FILE_SPECIAL, SERIES_ANIME, f"{ANIME_ROOT}/Anime A/S00/E01.mkv", 1000),
        file_entry(
            FILE_PAIR, SERIES_ANIME, f"{ANIME_ROOT}/Anime A/S01/E01-E02.mkv", PAIR_FILE_SIZE
        ),
        file_entry(103, SERIES_ANIME, f"{ANIME_ROOT}/Anime A/new.mkv", 6000),
    ]
    stub.episodes[SERIES_ANIME].append(episode_entry(16, SERIES_ANIME, 1, 6, "E06", file_id=103))
    second = build_baseline(collect(stub, settings))
    verdict = compare_baseline(first, second)
    assert verdict.requires_rebaseline is False
    assert verdict.reasons == ()


def test_urlbase_307_redirect_reaches_prefixed_api_path(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    seen: list[httpx.Request] = []
    redirecting = True

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal redirecting
        seen.append(request)
        if request.url.path == "/sonarr/api/v3/series" and redirecting:
            redirecting = False
            return httpx.Response(
                307, headers={"location": "/sonarr/api/v3/series"}, request=request
            )
        if request.url.path == "/sonarr/api/v3/series":
            return httpx.Response(200, json=[], request=request)
        return httpx.Response(404, json={"message": "unknown"}, request=request)

    base_document["sonarr"]["url"] = f"{BASE_URL}/sonarr"
    settings = load_settings(write_document(base_document))
    endpoint = sonarr_endpoint(handler, base_url=f"{BASE_URL}/sonarr")
    try:
        inventory = collect_sonarr_inventory(endpoint, settings)
    finally:
        endpoint.client.close()
    assert inventory.verdict.complete is True
    series_requests = [r for r in seen if r.url.path.endswith("/api/v3/series")]
    assert len(series_requests) == HOPS_AFTER_ONE_REDIRECT
    assert str(series_requests[1].url) == f"{BASE_URL}/sonarr/api/v3/series"
    assert series_requests[1].headers["X-Api-Key"] == FAKE_KEY


# ----------------------------------------------------------- pure decision coverage


def test_select_eligible_series_is_pure_and_component_aware() -> None:
    series = (
        SonarrSeries(
            id=1, title="In", root_folder_path="/anime", tvdb_id=None, series_type="anime"
        ),
        SonarrSeries(id=2, title="Sibling", root_folder_path="/anime-old/X", tvdb_id=None),
        SonarrSeries(id=3, title="Escaper", root_folder_path="/anime/../secret", tvdb_id=None),
        SonarrSeries(id=4, title="Other", root_folder_path="/other/X", tvdb_id=None),
    )
    eligible, excluded = select_eligible_series(series, ("/anime",))
    assert [item.series.id for item in eligible] == [1]
    assert eligible[0].root == "/anime"
    assert [item.id for item in excluded] == [2, 3, 4]


def test_index_series_files_flags_dangling_episode_reference() -> None:
    item = EligibleSeries(series=SonarrSeries(1, "Show", ANIME_ROOT, None), root=ANIME_ROOT)
    episodes = EpisodeParseResult(
        episodes=(SonarrEpisode(11, 1, 1, 1, "E01", None, None, 999),),
        tainted_file_ids=frozenset(),
        reasons=(),
        details=(),
    )
    no_files = FileParseResult(records=(), rejected=(), reasons=(), details=())
    result: FileIndexResult = index_series_files(item, episodes, no_files)
    assert result.files == ()
    assert VerdictReason.INCONSISTENT_ENUMERATION in result.reasons


def test_index_series_files_taints_files_of_malformed_episodes() -> None:
    item = EligibleSeries(series=SonarrSeries(1, "Show", ANIME_ROOT, None), root=ANIME_ROOT)
    tainted = EpisodeParseResult(
        episodes=(),
        tainted_file_ids=frozenset({100}),
        reasons=(VerdictReason.MALFORMED_RESPONSE,),
        details=(),
    )
    records = FileParseResult(
        records=(SonarrFileRecord(id=100, series_id=1, path=f"{ANIME_ROOT}/Show/a.mkv", size=1),),
        rejected=(),
        reasons=(),
        details=(),
    )
    result = index_series_files(item, tainted, records)
    assert result.files == ()
    assert [r.reason for r in result.rejected] == [FileRejectReason.MALFORMED_EPISODE_DATA]


def test_validate_root_folders_reports_both_directions_as_warnings() -> None:
    warnings = validate_root_folders(["/anime", "/deep/media/anime"], ["/anime/tv", "/tv"])
    assert any("'/anime' is not listed" in warning for warning in warnings)
    assert any("'/deep/media/anime' is not listed" in warning for warning in warnings)
    assert any("'/tv' is outside" in warning for warning in warnings)
    assert not any("'/anime/tv' is outside" in warning for warning in warnings)


# ------------------------------------------------------- review-driven regressions


def test_unparseable_episode_file_id_taints_whole_series_file_set(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = standard_stub()
    # Episode 12 references file 101 as a STRING: target unresolvable.
    stub.episodes[SERIES_ANIME][2]["episodeFileId"] = "101"
    inventory = collect(stub, settings_with_roots(base_document, write_document, [ANIME_ROOT]))
    assert inventory.files == ()
    assert all(
        r.reason is FileRejectReason.MALFORMED_EPISODE_DATA for r in inventory.rejected_files
    )
    assert len(inventory.rejected_files) == INDEXED_FILE_COUNT
    assert VerdictReason.MALFORMED_RESPONSE in inventory.verdict.reasons
    assert inventory.verdict.complete is False
    assert inventory.verdict.auto_apply_eligible is False


def test_malformed_episode_reference_taints_only_its_file(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = standard_stub()
    # String absoluteEpisodeNumber: the episode itself is malformed, file 101 is not indexed.
    stub.episodes[SERIES_ANIME][1]["absoluteEpisodeNumber"] = "1"
    inventory = collect(stub, settings_with_roots(base_document, write_document, [ANIME_ROOT]))
    indexed = {(f.series_id, f.episode_file_id) for f in inventory.files}
    assert (SERIES_ANIME, FILE_PAIR) not in indexed
    assert (SERIES_ANIME, FILE_SPECIAL) in indexed
    taints = [
        r for r in inventory.rejected_files if r.reason is FileRejectReason.MALFORMED_EPISODE_DATA
    ]
    assert [(r.series_id, r.episode_file_id) for r in taints] == [(SERIES_ANIME, FILE_PAIR)]
    assert inventory.verdict.complete is False
    assert inventory.verdict.auto_apply_eligible is False


def test_duplicate_episode_ids_mark_inconsistent_enumeration(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = standard_stub()
    dupe = dict(stub.episodes[SERIES_ANIME][1])
    stub.episodes[SERIES_ANIME].append(dupe)
    inventory = collect(stub, settings_with_roots(base_document, write_document, [ANIME_ROOT]))
    assert VerdictReason.INCONSISTENT_ENUMERATION in inventory.verdict.reasons
    assert inventory.verdict.complete is False
    assert inventory.verdict.auto_apply_eligible is False


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("id", True),
        ("id", 12.0),
        ("id", -7),
        ("seasonNumber", "1"),
        ("episodeNumber", 0),
        ("episodeNumber", -3),
        ("title", None),
        ("seriesId", "1"),
    ],
)
def test_episode_type_oddities_fail_closed(
    base_document: dict[str, Any], field: str, value: Any
) -> None:
    entry = episode_entry(50, SERIES_ANIME, 1, 1, "E", file_id=101)
    entry[field] = value
    result = parse_episode_entries([entry], SERIES_ANIME)
    assert result.episodes == ()
    assert VerdictReason.MALFORMED_RESPONSE in result.reasons
    # A valid file reference taints its file; an unusable reference taints the series.
    assert result.tainted_file_ids == frozenset({101})
    assert result.file_reference_tainted is (field == "episodeFileId")


@pytest.mark.parametrize(("file_id", "owner_id"), [(-7, 1), (0, 1), (7, 0), (7, -2), (7, True)])
def test_file_id_oddities_fail_closed(file_id: Any, owner_id: Any) -> None:
    entry = file_entry(file_id, owner_id, f"{ANIME_ROOT}/Show/x.mkv", 10)
    result = parse_file_entries([entry], SERIES_ANIME)
    assert result.records == ()
    assert VerdictReason.MALFORMED_RESPONSE in result.reasons


def test_windows_style_series_root_is_excluded_at_collection(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = standard_stub()
    stub.series.append(series_entry(SERIES_OLD, "Windows Show", "C:\\media\\anime\\Show"))
    inventory = collect(stub, settings_with_roots(base_document, write_document, [ANIME_ROOT]))
    assert {item.id for item in inventory.excluded_series} == {SERIES_LIVE, SERIES_OLD}
    assert stub.secondary_series_ids() == {SERIES_ANIME}


def test_series_ordering_is_deterministic_and_rejects_stably(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    def build_stub(order: list[int]) -> SonarrStub:
        stub = SonarrStub()
        roots = {9: f"{ANIME_ROOT}/Nine", 1: f"{ANIME_ROOT}/One", 7: f"{ANIME_ROOT}/Seven"}
        stub.series = [series_entry(sid, f"Show {sid}", roots[sid]) for sid in order]
        stub.episodes = {sid: [] for sid in order}
        # A collision in series 1 exercises reject-order determinism.
        stub.files = {
            1: [
                file_entry(200, 1, f"{ANIME_ROOT}/One/a.mkv", 5),
                file_entry(200, 1, f"{ANIME_ROOT}/One/b.mkv", 6),
            ],
            7: [],
            9: [],
        }
        return stub

    settings = settings_with_roots(base_document, write_document, [ANIME_ROOT])
    forward = collect(build_stub([9, 1, 7]), settings)
    backward = collect(build_stub([7, 1, 9]), settings)
    assert [item.series.id for item in forward.eligible_series] == [1, 7, 9]
    assert [item.id for item in forward.excluded_series] == []
    assert forward.rejected_files == backward.rejected_files
    collisions = [
        r for r in forward.rejected_files if r.reason is FileRejectReason.DUPLICATE_FILE_ID
    ]
    assert [str(r.path) for r in collisions] == sorted(str(r.path) for r in collisions)


@pytest.mark.parametrize(
    "expire_path", ["/api/v3/series", "/api/v3/episode", "/api/v3/episodefile"]
)
def test_inventory_io_deadline_stops_after_delayed_read(expire_path: str) -> None:
    stub = standard_stub()
    elapsed = [0.0]

    def handler(request: httpx.Request) -> httpx.Response:
        response = stub.handler(request)
        assert all(value <= 1.0 for value in request.extensions["timeout"].values())
        if request.url.path == expire_path:
            elapsed[0] = 2.0
        return response

    endpoint = sonarr_endpoint(handler)
    try:
        inventory = collect_sonarr_inventory(
            endpoint,
            allowed_roots=[ANIME_ROOT],
            anime_only=True,
            deadline=2.0,
            monotonic=lambda: elapsed[0],
        )
    finally:
        endpoint.client.close()
    assert not inventory.verdict.complete
    assert stub.requests[-1].url.path == expire_path
    assert not inventory.files


def test_inventory_expired_io_deadline_makes_no_requests() -> None:
    stub = standard_stub()
    endpoint = sonarr_endpoint(stub.handler)
    try:
        inventory = collect_sonarr_inventory(
            endpoint,
            allowed_roots=[ANIME_ROOT],
            anime_only=True,
            deadline=0.0,
            monotonic=lambda: 0.0,
        )
    finally:
        endpoint.client.close()
    assert not inventory.verdict.complete
    assert not stub.requests


def test_inventory_redirect_rechecks_io_deadline() -> None:
    requests: list[httpx.Request] = []
    elapsed = [0.0]

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        elapsed[0] = 2.0
        return httpx.Response(302, headers={"location": "/sonarr/redirected"})

    endpoint = sonarr_endpoint(handler)
    try:
        inventory = collect_sonarr_inventory(
            endpoint,
            allowed_roots=[ANIME_ROOT],
            anime_only=True,
            deadline=2.0,
            monotonic=lambda: elapsed[0],
        )
    finally:
        endpoint.client.close()
    assert not inventory.verdict.complete
    assert len(requests) == 1


@pytest.mark.parametrize("expire_path", ["/api/v3/series", "/api/v3/episode"])
def test_source_guard_deadline_stops_after_delayed_read(expire_path: str) -> None:
    stub = standard_stub()
    elapsed = [0.0]

    def handler(request: httpx.Request) -> httpx.Response:
        response = stub.handler(request)
        if request.url.path == expire_path:
            elapsed[0] = 2.0
        return response

    endpoint = sonarr_endpoint(handler)
    try:
        verdict = check_current_file_relationship(
            endpoint,
            series_id=SERIES_ANIME,
            episode_file_id=FILE_PAIR,
            path=f"{ANIME_ROOT}/Anime A/S01/E01-E02.mkv",
            size=PAIR_FILE_SIZE,
            episode_identities=(),
            tvdb_series_id=stub.series[0]["tvdbId"],
            allowed_roots=[ANIME_ROOT],
            deadline=2.0,
            monotonic=lambda: elapsed[0],
        )
    finally:
        endpoint.client.close()
    assert verdict.status is SourceRelationshipStatus.UNAVAILABLE
    assert stub.requests[-1].url.path == expire_path


def test_inventory_io_deadline_narrows_remaining_timeout() -> None:
    timeouts: list[dict[str, float]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        timeouts.append(request.extensions["timeout"])
        return httpx.Response(200, json=[])

    endpoint = sonarr_endpoint(handler)
    try:
        inventory = collect_sonarr_inventory(
            endpoint,
            allowed_roots=[ANIME_ROOT],
            anime_only=True,
            deadline=0.25,
            monotonic=lambda: 0.0,
        )
    finally:
        endpoint.client.close()
    assert inventory.verdict.complete
    assert timeouts
    assert all(value == 0.25 for timeout in timeouts for value in timeout.values())


def test_expired_root_read_is_incomplete_even_with_no_eligible_series() -> None:
    elapsed = [0.0]

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v3/rootfolder":
            elapsed[0] = 2.0
        return httpx.Response(200, json=[])

    endpoint = sonarr_endpoint(handler)
    try:
        inventory = collect_sonarr_inventory(
            endpoint,
            allowed_roots=[ANIME_ROOT],
            anime_only=True,
            deadline=2.0,
            monotonic=lambda: elapsed[0],
        )
    finally:
        endpoint.client.close()
    assert not inventory.verdict.complete
    assert VerdictReason.INCOMPLETE_SNAPSHOT in inventory.verdict.reasons
