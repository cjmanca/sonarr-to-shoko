"""M5 linking gates: ID chain, complete-target rule, preflight, single POST, verification.

Every request goes through the real http.py auth/redirect/no-retry policy over an
httpx.MockTransport: nothing here touches the network. Clocks are injected fakes;
verification fetches are scripted. The request log per test is the evidence: the
dry-run path shows zero POSTs, the mutation path shows exactly one POST, and
every failure shows zero additional POSTs.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import fields
from pathlib import Path
from typing import Any, TypeVar

import hash_fixtures as hf
import httpx
import pytest

from shoko_linker.config import Settings, load_settings
from shoko_linker.errors import ConfigError
from shoko_linker.http import ApiEndpoint, make_timeout, shoko_headers
from shoko_linker.linking import (
    EpisodeIdResolver,
    EpisodeTargetResolution,
    FileXRef,
    LinkPlan,
    PlanSkip,
    PlanStatus,
    PreflightReason,
    PreflightVerdict,
    QueueSnapshot,
    ReconcileResult,
    ReconcileStage,
    SendOutcome,
    ShokoFileXRefs,
    VerificationVerdict,
    XrefEpisode,
    freeze_plan,
    parse_file_xrefs,
    plan_fingerprint,
    preflight,
    reconcile_link_plan,
    request_fingerprint,
    resolve_episode_ids,
    submit_link,
    verify_link,
)
from shoko_linker.linking import EpisodeResolutionStatus as EpiStatus
from shoko_linker.mappings import ResolutionStatus, build_mapping_index, parse_anime_lists
from shoko_linker.policies import RunPolicy
from shoko_linker.shoko import FileMatch, ImportFolder, ShokoHashes
from shoko_linker.sonarr import SonarrEpisode, SonarrFile

FAKE_KEY = "fake-key-never-a-real-one-0000"
SHOKO_BASE = "http://shoko:8111"
SONARR_ROOT = "/data/media/anime"
SHOKO_ROOT = "/anime"
SONARR_FILE_PATH = f"{SONARR_ROOT}/Show/S01E01+02.mkv"
SHOKO_FILE_PATH = f"{SHOKO_ROOT}/Show/S01E01+02.mkv"
SHOKO_FILE_ID = 500
SONARR_SIZE = 1000
ANIME_ID = 1234
TVDB_SERIES = 267440
EPISODE_A = (10, 5501, 777)
EPISODE_B = (11, 5502, 778)
TARGETS_DIGEST = "a" * 64
STRUCTURE_DIGEST = "b" * 64
ANIDB_EP_A = 5501
ANIDB_EP_B = 5502
SHOKO_EP_A = 777
SHOKO_EP_B = 778
CHAIN_GETS = 2
STATUS_OK = 200
STATUS_VALIDATION = 400
STATUS_UNAVAILABLE = 503
SUBMIT_AT = 5.0
RECONCILE_AT = 42.0
FOLDERS = {
    1: ImportFolder(
        id=1, name="Sonarr", path=SHOKO_ROOT, drop_folder_type="None", path_usable=True
    ),
    2: ImportFolder(
        id=2, name="Drop", path="/incoming", drop_folder_type="Source", path_usable=True
    ),
}

Handler = Callable[[httpx.Request], httpx.Response]
Writer = Callable[[dict[str, Any]], Path]
T = TypeVar("T")


class LinkStub:
    """Programmable offline Shoko v5.3.3 API with a request log and body capture."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.post_bodies: list[Any] = []
        self.routes: dict[tuple[str, str], Any] = {}
        self.statuses: dict[tuple[str, str], int] = {}
        self.errors: dict[tuple[str, str], Exception] = {}
        self.redirects: dict[tuple[str, str], str] = {}

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        key = (request.method, request.url.path)
        if key in self.errors:
            raise self.errors[key]
        if key in self.redirects:
            return httpx.Response(302, headers={"location": self.redirects[key]}, request=request)
        if key not in self.routes:
            return httpx.Response(404, json={"message": "not found"}, request=request)
        payload = self.routes[key]
        if request.method == "POST":
            self.post_bodies.append(json.loads(request.content))
        return httpx.Response(self.statuses.get(key, 200), json=payload, request=request)

    def methods(self) -> list[str]:
        return [request.method for request in self.requests]

    def paths(self) -> list[str]:
        return [request.url.path for request in self.requests]

    def posts(self) -> list[httpx.Request]:
        return [request for request in self.requests if request.method == "POST"]


class FakeClock:
    """Successive clock values; the last value repeats (deterministic polling)."""

    def __init__(self, values: list[float]) -> None:
        self._values = values
        self._index = 0

    def __call__(self) -> float:
        value = self._values[min(self._index, len(self._values) - 1)]
        self._index += 1
        return value


def link_endpoint(stub: LinkStub) -> ApiEndpoint:
    client = httpx.Client(
        base_url=SHOKO_BASE.rstrip("/") + "/",
        transport=httpx.MockTransport(stub.handler),
        follow_redirects=False,
        timeout=make_timeout(1.0, 1.0),
    )
    return ApiEndpoint(client, lambda: shoko_headers(FAKE_KEY))


def with_endpoint(stub: LinkStub, action: Callable[[ApiEndpoint], T]) -> T:
    endpoint = link_endpoint(stub)
    try:
        return action(endpoint)
    finally:
        endpoint.client.close()


def location(
    place_id: int,
    file_id: int,
    folder_id: int,
    relative: str,
    absolute: str,
    accessible: bool = True,
) -> dict[str, Any]:
    return {
        "ID": place_id,
        "FileID": file_id,
        "ImportFolderID": folder_id,
        "RelativePath": relative,
        "AbsolutePath": absolute,
        "IsAccessible": accessible,
    }


def xref_episode(shoko_id: int | None, anidb_id: int, source: str = "User") -> dict[str, Any]:
    return {"ID": shoko_id, "AniDB": anidb_id, "TMDB": None, "Source": source}


def xref(series_anidb: int, episodes: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "SeriesID": {"ID": None, "AniDB": series_anidb, "TMDB": None},
        "EpisodeIDs": episodes,
    }


def file_json(
    id_: int = SHOKO_FILE_ID,
    size: int = SONARR_SIZE,
    locations: list[dict[str, Any]] | None = None,
    xrefs: list[dict[str, Any]] | None = None,
    imported: str | None = "2024-01-01T00:00:00",
    hashes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    places = (
        locations
        if locations is not None
        else [location(900, id_, 1, "Show/S01E01+02.mkv", SHOKO_FILE_PATH)]
    )
    return {
        "ID": id_,
        "Size": size,
        "IsIgnored": False,
        "IsVariation": False,
        "Imported": imported,
        "Hashes": hf.ed2k_hashes(id_, size) if hashes is None else hashes,
        "Locations": places,
        "SeriesIDs": [] if xrefs is None else xrefs,
    }


def anidb_list(entries: list[dict[str, Any]]) -> dict[str, Any]:
    return {"Total": len(entries), "List": entries}


def anidb_episode(id_: int, anime_id: int, type_: str, number: int) -> dict[str, Any]:
    return {
        "ID": id_,
        "AnimeID": anime_id,
        "Type": type_,
        "EpisodeNumber": number,
        "AirDate": None,
        "Title": f"Ep {number}",
        "Titles": [],
        "Description": None,
        "Rating": None,
    }


def shoko_episode(internal_id: int | None, anidb_id: int) -> dict[str, Any]:
    return {
        "IDs": {"ID": internal_id, "AniDB": anidb_id, "TMDB": None},
        "Type": "Episode",
        "EpisodeNumber": 10,
        "Title": "Ep",
    }


def chain_routes(
    *,
    internal_a: int = 777,
    anidb_a: int = 5501,
    include_b: bool = True,
) -> dict[tuple[str, str], Any]:
    routes: dict[tuple[str, str], Any] = {
        ("GET", f"/api/v3/Series/AniDB/{ANIME_ID}/Episode"): anidb_list(
            [anidb_episode(5501, ANIME_ID, "Episode", 10)]
        ),
        ("GET", "/api/v3/Episode/AniDB/5501/Episode"): shoko_episode(internal_a, anidb_a),
    }
    if include_b:
        routes[("GET", "/api/v3/Series/AniDB/1235/Episode")] = anidb_list(
            [anidb_episode(5502, 1235, "Episode", 11)]
        )
        routes[("GET", "/api/v3/Episode/AniDB/5502/Episode")] = shoko_episode(778, 5502)
    return routes


def episode_json(id_: int, number: int, tvdb_id: int) -> SonarrEpisode:
    return SonarrEpisode(
        id=id_,
        series_id=7,
        season_number=1,
        episode_number=number,
        title=f"Ep {number}",
        absolute_episode_number=None,
        tvdb_id=tvdb_id,
        episode_file_id=40,
    )


EPISODE_101 = episode_json(101, 1, 1)
EPISODE_102 = episode_json(102, 2, 2)


def build_match(episodes: tuple[SonarrEpisode, ...] = (EPISODE_101, EPISODE_102)) -> FileMatch:
    sonarr_file = SonarrFile(
        series_id=7,
        episode_file_id=40,
        path=SONARR_FILE_PATH,
        size=SONARR_SIZE,
        episodes=episodes,
    )
    return FileMatch(
        shoko_file_id=SHOKO_FILE_ID,
        sonarr_file=sonarr_file,
        shoko_paths=(SONARR_FILE_PATH,),
        hashes=ShokoHashes(None, None, None, None),
    )


def res(
    episode: SonarrEpisode,
    mapping: tuple[int, int, int] | None,
    *,
    status: ResolutionStatus = ResolutionStatus.RESOLVED,
    pending: bool = False,
) -> EpisodeTargetResolution:
    anidb_number, anidb_ep_id, shoko_id = mapping if mapping is not None else (0, 0, 0)
    return EpisodeTargetResolution(
        episode=episode,
        tvdb_series=TVDB_SERIES,
        status=status,
        anidb_anime_id=ANIME_ID if mapping is not None else None,
        anidb_episode_number=anidb_number if mapping is not None else None,
        episode_type="Episode" if mapping is not None else None,
        source_rule="entry_offset" if mapping is not None else None,
        anidb_episode_id=anidb_ep_id if mapping is not None else None,
        shoko_episode_id=shoko_id,
        pending_metadata=pending,
        detail=status.value,
    )


def freeze(resolutions: list[EpisodeTargetResolution], **kwargs: Any) -> LinkPlan | PlanSkip:
    options: dict[str, Any] = {
        "policy": RunPolicy.BACKLOG,
        "targets_digest": TARGETS_DIGEST,
        "structure_digest": STRUCTURE_DIGEST,
        "shoko_paths": [SHOKO_FILE_PATH],
    }
    options.update(kwargs)
    return freeze_plan(build_match(), resolutions, **options)


def must_plan(result: LinkPlan | PlanSkip) -> LinkPlan:
    assert isinstance(result, LinkPlan)
    return result


def linkable_plan(**kwargs: Any) -> LinkPlan:
    plan = freeze([res(EPISODE_101, EPISODE_A), res(EPISODE_102, EPISODE_B)], **kwargs)
    plan = must_plan(plan)
    assert plan.status is PlanStatus.LINKABLE
    return plan


def preflight_ok_routes() -> dict[tuple[str, str], Any]:
    return {
        ("GET", f"/api/v3/File/{SHOKO_FILE_ID}"): file_json(),
        ("GET", "/api/v3/Episode/AniDB/5501/Episode"): shoko_episode(777, 5501),
        ("GET", "/api/v3/Episode/AniDB/5502/Episode"): shoko_episode(778, 5502),
        ("POST", f"/api/v3/File/{SHOKO_FILE_ID}/Link"): {},
    }


def run_preflight(
    stub: LinkStub,
    plan: LinkPlan,
    settings: Settings,
    **kwargs: Any,
) -> PreflightVerdict:
    defaults: dict[str, Any] = {
        "attestation_valid": True,
        "apply_gate_open": True,
        "queue": QueueSnapshot(0, 0, 0),
        "active_targets_digest": plan.targets_digest,
        "active_structure_digest": plan.structure_digest,
    }
    defaults.update(kwargs)
    return with_endpoint(
        stub, lambda endpoint: preflight(endpoint, plan, settings, FOLDERS, **defaults)
    )


BENIGN_QUEUE = QueueSnapshot(waiting_count=12, blocked_count=0, total_count=30)


def base_settings(base_document: dict[str, Any], write_document: Writer) -> Settings:
    return load_settings(write_document(base_document))


# ---------------------------------------------------------------- ID chain gates


def test_id_chain_happy_path_walks_the_two_step_contract() -> None:
    stub = LinkStub()
    stub.routes = chain_routes()
    resolver_result = with_endpoint(
        stub,
        lambda endpoint: EpisodeIdResolver(endpoint, clock=FakeClock([0.0])).resolve(
            ANIME_ID, "Episode", 10
        ),
    )
    assert resolver_result.status is EpiStatus.RESOLVED
    assert resolver_result.anidb_episode_id == ANIDB_EP_A
    assert resolver_result.shoko_episode_id == SHOKO_EP_A
    assert stub.paths() == [
        "/api/v3/Series/AniDB/1234/Episode",
        "/api/v3/Episode/AniDB/5501/Episode",
    ]
    listing = stub.requests[0]
    assert listing.url.params["type"] == "Episode"
    assert all(request.headers["apikey"] == FAKE_KEY for request in stub.requests)
    assert stub.methods() == ["GET", "GET"]


def test_id_chain_second_step_404_is_pending_metadata() -> None:
    stub = LinkStub()
    stub.routes = chain_routes(include_b=False)
    del stub.routes[("GET", "/api/v3/Episode/AniDB/5501/Episode")]
    resolved = with_endpoint(
        stub,
        lambda endpoint: EpisodeIdResolver(endpoint, clock=FakeClock([0.0])).resolve(
            ANIME_ID, "Episode", 10
        ),
    )
    assert resolved.status is EpiStatus.PENDING_METADATA
    assert resolved.anidb_episode_id == ANIDB_EP_A
    assert resolved.shoko_episode_id is None


def test_id_chain_anidb_identity_mismatch_blocks() -> None:
    stub = LinkStub()
    stub.routes = chain_routes(internal_a=777, anidb_a=9999, include_b=False)
    resolved = with_endpoint(
        stub,
        lambda endpoint: EpisodeIdResolver(endpoint, clock=FakeClock([0.0])).resolve(
            ANIME_ID, "Episode", 10
        ),
    )
    assert resolved.status is EpiStatus.MISMATCH
    assert resolved.shoko_episode_id is None


def test_id_chain_first_step_404_is_anime_not_cached() -> None:
    stub = LinkStub()
    stub.routes = {}
    resolved = with_endpoint(
        stub,
        lambda endpoint: EpisodeIdResolver(endpoint, clock=FakeClock([0.0])).resolve(
            ANIME_ID, "Episode", 10
        ),
    )
    assert resolved.status is EpiStatus.ANIME_NOT_CACHED


def test_id_chain_number_type_mismatch_is_episode_not_found() -> None:
    stub = LinkStub()
    stub.routes = {
        ("GET", f"/api/v3/Series/AniDB/{ANIME_ID}/Episode"): anidb_list(
            [anidb_episode(5501, ANIME_ID, "Special", 10)]
        ),
    }
    resolved = with_endpoint(
        stub,
        lambda endpoint: EpisodeIdResolver(endpoint, clock=FakeClock([0.0])).resolve(
            ANIME_ID, "Episode", 10
        ),
    )
    assert resolved.status is EpiStatus.EPISODE_NOT_FOUND


def test_id_chain_cache_hit_performs_no_second_request() -> None:
    stub = LinkStub()
    stub.routes = chain_routes(include_b=False)
    resolver = EpisodeIdResolver(link_endpoint(stub), clock=FakeClock([0.0, 0.0]))
    first = resolver.resolve(ANIME_ID, "Episode", 10)
    second = resolver.resolve(ANIME_ID, "Episode", 10)
    assert first.status is EpiStatus.RESOLVED
    assert second.shoko_episode_id == SHOKO_EP_A
    assert len(stub.requests) == CHAIN_GETS


def test_id_chain_negative_result_is_invalidated_and_re_read() -> None:
    stub = LinkStub()
    stub.routes = chain_routes(internal_a=777, anidb_a=9999, include_b=False)
    resolver = EpisodeIdResolver(link_endpoint(stub), clock=FakeClock([0.0, 0.0]))
    first = resolver.resolve(ANIME_ID, "Episode", 10)
    second = resolver.resolve(ANIME_ID, "Episode", 10)
    assert first.status is EpiStatus.MISMATCH
    assert second.status is EpiStatus.MISMATCH
    assert len(stub.requests) == CHAIN_GETS * 2


def test_id_chain_cache_expires_on_the_injected_clock() -> None:
    stub = LinkStub()
    stub.routes = chain_routes(include_b=False)
    resolver = EpisodeIdResolver(
        link_endpoint(stub), clock=FakeClock([0.0, 10.0, 400.0]), ttl_seconds=300.0
    )
    resolver.resolve(ANIME_ID, "Episode", 10)
    resolver.resolve(ANIME_ID, "Episode", 10)
    assert len(stub.requests) == CHAIN_GETS
    resolver.resolve(ANIME_ID, "Episode", 10)
    assert len(stub.requests) == CHAIN_GETS * 2


def test_resolve_episode_ids_fetches_once_per_unique_target() -> None:
    stub = LinkStub()
    stub.routes = chain_routes()
    results = with_endpoint(
        stub,
        lambda endpoint: resolve_episode_ids(
            endpoint,
            [
                (ANIME_ID, "Episode", 10),
                (ANIME_ID, "Episode", 10),
                (1235, "Episode", 11),
            ],
            clock=FakeClock([0.0, 0.0, 0.0]),
        ),
    )
    assert results[(ANIME_ID, "Episode", 10)].shoko_episode_id == SHOKO_EP_A
    assert results[(1235, "Episode", 11)].shoko_episode_id == SHOKO_EP_B
    assert len(stub.requests) == CHAIN_GETS * 2


# ----------------------------------------------------- complete-target rule gates


def test_multi_episode_file_with_one_unmapped_episode_skips_the_whole_file() -> None:
    plan = freeze(
        [res(EPISODE_101, EPISODE_A), res(EPISODE_102, None, status=ResolutionStatus.UNMAPPED)]
    )
    assert isinstance(plan, PlanSkip)
    assert plan.reason == "target_unmapped"
    assert "102" in plan.detail


def test_ambiguous_target_skips_the_whole_file() -> None:
    plan = freeze(
        [res(EPISODE_101, EPISODE_A), res(EPISODE_102, None, status=ResolutionStatus.AMBIGUOUS)]
    )
    assert isinstance(plan, PlanSkip)
    assert plan.reason == "target_ambiguous"


def test_conflict_and_no_equivalent_targets_skip_the_file() -> None:
    for status, reason in (
        (ResolutionStatus.CONFLICT, "target_conflict"),
        (ResolutionStatus.NO_EQUIVALENT, "target_no_equivalent"),
    ):
        plan = freeze([res(EPISODE_101, EPISODE_A), res(EPISODE_102, None, status=status)])
        assert isinstance(plan, PlanSkip)
        assert plan.reason == reason


def test_all_resolved_yields_sorted_deduplicated_shoko_ids() -> None:
    plan = freeze([res(EPISODE_102, EPISODE_B), res(EPISODE_101, (10, 5501, 777))])
    plan = must_plan(plan)
    assert plan.status is PlanStatus.LINKABLE
    assert plan.episode_ids == (777, 778)


def test_same_shoko_id_from_two_episodes_is_deduplicated() -> None:
    plan = freeze([res(EPISODE_101, (10, 5501, 777)), res(EPISODE_102, (11, 5502, 777))])
    plan = must_plan(plan)
    assert plan.episode_ids == (777,)


def test_pending_metadata_target_makes_plan_pending_not_linkable() -> None:
    plan = freeze([res(EPISODE_101, EPISODE_A), res(EPISODE_102, EPISODE_B, pending=True)])
    plan = must_plan(plan)
    assert plan.status is PlanStatus.PENDING_TARGETS
    assert plan.episode_ids == ()
    assert len(plan.pending_targets) == 1
    assert plan.pending_targets[0].anidb_episode_id == ANIDB_EP_B


def test_resolution_set_missing_an_episode_fails_closed() -> None:
    plan = freeze([res(EPISODE_101, EPISODE_A)])
    assert isinstance(plan, PlanSkip)
    assert plan.reason == "incomplete_resolution_set"


def test_resolved_target_without_shoko_id_is_skipped() -> None:
    plan = freeze([res(EPISODE_101, (10, 5501, 0)), res(EPISODE_102, EPISODE_B)])
    assert isinstance(plan, PlanSkip)
    assert plan.reason == "target_unresolved_id"


# --------------------------------------------------------------- preflight gates


def test_preflight_ok_reads_file_fresh_and_revalidates_targets(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    verdict = run_preflight(stub, plan, settings, queue=BENIGN_QUEUE)
    assert verdict.ok
    assert stub.methods() == ["GET", "GET", "GET"]
    file_request = stub.requests[0]
    assert file_request.url.params["include"] == "XRefs,AbsolutePaths"
    assert all(request.headers["apikey"] == FAKE_KEY for request in stub.requests)


def test_preflight_blocks_a_freshly_appeared_user_link(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    stub.routes[("GET", f"/api/v3/File/{SHOKO_FILE_ID}")] = file_json(
        xrefs=[xref(ANIME_ID, [xref_episode(777, 5501, "User")])]
    )
    verdict = run_preflight(stub, plan, settings)
    assert not verdict.ok
    assert verdict.reasons == (PreflightReason.EXISTING_LINK,)


def test_preflight_blocks_an_anidb_source_xref(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    stub.routes[("GET", f"/api/v3/File/{SHOKO_FILE_ID}")] = file_json(
        xrefs=[xref(ANIME_ID, [xref_episode(777, 5501, "AniDB")])]
    )
    verdict = run_preflight(stub, plan, settings)
    assert not verdict.ok
    assert verdict.reasons == (PreflightReason.EXISTING_LINK,)


def test_preflight_blocks_a_location_added_after_planning(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    stub.routes[("GET", f"/api/v3/File/{SHOKO_FILE_ID}")] = file_json(
        locations=[
            location(900, SHOKO_FILE_ID, 1, "Show/S01E01+02.mkv", SHOKO_FILE_PATH),
            location(901, SHOKO_FILE_ID, 2, "Show/S01E01+02.mkv", "/incoming/Show/S01E01+02.mkv"),
        ]
    )
    verdict = run_preflight(stub, plan, settings)
    assert not verdict.ok
    assert set(verdict.reasons) == {
        PreflightReason.IDENTITY_CHANGED,
        PreflightReason.OWNERSHIP_FAILED,
    }


def test_preflight_size_change_is_identity_changed(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    stub.routes[("GET", f"/api/v3/File/{SHOKO_FILE_ID}")] = file_json(size=1001)
    verdict = run_preflight(stub, plan, settings)
    assert not verdict.ok
    assert verdict.reasons == (PreflightReason.IDENTITY_CHANGED,)


def test_preflight_missing_file_blocks(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    del stub.routes[("GET", f"/api/v3/File/{SHOKO_FILE_ID}")]
    verdict = run_preflight(stub, plan, settings)
    assert not verdict.ok
    assert verdict.reasons == (PreflightReason.FILE_NOT_FOUND,)


def test_preflight_mapping_digest_change_is_mapping_changed(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    verdict = run_preflight(stub, plan, settings, active_targets_digest="c" * 64)
    assert not verdict.ok
    assert verdict.reasons == (PreflightReason.MAPPING_CHANGED,)


def test_preflight_target_drift_when_internal_id_changed(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    stub.routes[("GET", "/api/v3/Episode/AniDB/5501/Episode")] = shoko_episode(999, 5501)
    verdict = run_preflight(stub, plan, settings)
    assert not verdict.ok
    assert verdict.reasons == (PreflightReason.TARGET_DRIFT,)


def test_preflight_target_drift_when_the_target_404s(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    del stub.routes[("GET", "/api/v3/Episode/AniDB/5502/Episode")]
    verdict = run_preflight(stub, plan, settings)
    assert not verdict.ok
    assert verdict.reasons == (PreflightReason.TARGET_DRIFT,)


def test_preflight_candidate_set_change_is_detected(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    index = build_mapping_index(
        parse_anime_lists(
            f'<anime-list><anime anidbid="{ANIME_ID}" tvdbid="{TVDB_SERIES}" '
            'defaulttvdbseason="1" episodeoffset="0"/></anime-list>'
        )
    )
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    verdict = run_preflight(stub, plan, settings, mapping_index=index)
    assert not verdict.ok
    assert verdict.reasons == (PreflightReason.CANDIDATE_SET_CHANGED,)


@pytest.mark.parametrize(
    ("attestation_valid", "apply_gate_open"),
    [(False, True), (True, False), (False, False)],
)
def test_preflight_safety_inputs_block_with_zero_requests(
    base_document: dict[str, Any],
    write_document: Writer,
    attestation_valid: bool,
    apply_gate_open: bool,
) -> None:
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    verdict = run_preflight(
        stub,
        plan,
        settings,
        attestation_valid=attestation_valid,
        apply_gate_open=apply_gate_open,
    )
    assert not verdict.ok
    assert verdict.reasons == (PreflightReason.BLOCKED_SAFETY,)
    assert stub.requests == []


def test_preflight_blocked_queue_over_threshold(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    verdict = run_preflight(
        stub,
        plan,
        settings,
        queue=QueueSnapshot(waiting_count=12, blocked_count=15, total_count=30),
        blocked_over=10,
    )
    assert not verdict.ok
    assert verdict.reasons == (PreflightReason.QUEUE_BLOCKED,)


def test_preflight_benign_nonzero_queue_passes(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    verdict = run_preflight(stub, plan, settings, queue=BENIGN_QUEUE, blocked_over=10)
    assert verdict.ok


def test_preflight_blocks_when_the_queue_state_is_unknown(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    verdict = run_preflight(stub, plan, settings, queue=None)
    assert not verdict.ok
    assert verdict.reasons == (PreflightReason.QUEUE_UNKNOWN,)
    assert stub.methods() == []


# ----------------------------------------------------------------- POST matrix


def test_post_success_is_accepted_unverified_with_exact_body() -> None:
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    result = with_endpoint(
        stub, lambda endpoint: submit_link(endpoint, plan, clock=lambda: SUBMIT_AT)
    )
    assert result.outcome is SendOutcome.ACCEPTED_UNVERIFIED
    assert result.http_status == STATUS_OK
    assert result.attempted_at == SUBMIT_AT
    assert len(stub.posts()) == 1
    assert stub.post_bodies == [{"EpisodeIDs": [777, 778]}]
    post = stub.posts()[0]
    assert post.headers["apikey"] == FAKE_KEY
    assert post.url.path == f"/api/v3/File/{SHOKO_FILE_ID}/Link"
    assert request_fingerprint(plan) == result.request_fingerprint
    assert plan.fingerprint == result.plan_fingerprint


def test_post_redirect_is_ambiguous_with_exactly_one_attempt() -> None:
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    stub.redirects[("POST", f"/api/v3/File/{SHOKO_FILE_ID}/Link")] = (
        f"/api/v3/File/{SHOKO_FILE_ID}/Link/"
    )
    result = with_endpoint(stub, lambda endpoint: submit_link(endpoint, plan, clock=lambda: 1.0))
    assert result.outcome is SendOutcome.AMBIGUOUS_SEND
    assert result.retry_forbidden
    assert "include=XRefs" in result.detail
    assert len(stub.posts()) == 1


def test_post_timeout_is_ambiguous_and_retry_forbidden() -> None:
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    stub.errors[("POST", f"/api/v3/File/{SHOKO_FILE_ID}/Link")] = httpx.ReadTimeout(
        "read timed out"
    )
    result = with_endpoint(stub, lambda endpoint: submit_link(endpoint, plan, clock=lambda: 1.0))
    assert result.outcome is SendOutcome.AMBIGUOUS_SEND
    assert result.retry_forbidden
    assert result.http_status is None
    assert len(stub.posts()) == 1


def test_post_connect_error_is_ambiguous_with_zero_retries() -> None:
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    stub.errors[("POST", f"/api/v3/File/{SHOKO_FILE_ID}/Link")] = httpx.ConnectError(" refused")
    result = with_endpoint(stub, lambda endpoint: submit_link(endpoint, plan, clock=lambda: 1.0))
    assert result.outcome is SendOutcome.AMBIGUOUS_SEND
    assert result.retry_forbidden
    assert len(stub.posts()) == 1


def test_post_validation_problem_is_rejected_with_one_attempt() -> None:
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    key = ("POST", f"/api/v3/File/{SHOKO_FILE_ID}/Link")
    stub.statuses[key] = STATUS_VALIDATION
    stub.routes[key] = {
        "type": "https://shoko/problem/validation",
        "title": "ValidationProblem",
        "status": STATUS_VALIDATION,
        "detail": "the file already carries protected associations",
        "errors": {"EpisodeIDs": ["unknown episode id"]},
    }
    result = with_endpoint(stub, lambda endpoint: submit_link(endpoint, plan, clock=lambda: 1.0))
    assert result.outcome is SendOutcome.REJECTED
    assert result.http_status == STATUS_VALIDATION
    assert "ValidationProblem" in result.detail
    assert "unknown episode id" in result.detail
    assert len(stub.posts()) == 1


def test_post_server_error_is_ambiguous_and_never_retried() -> None:
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    key = ("POST", f"/api/v3/File/{SHOKO_FILE_ID}/Link")
    stub.statuses[key] = STATUS_UNAVAILABLE
    stub.routes[key] = {"message": "unavailable"}
    result = with_endpoint(stub, lambda endpoint: submit_link(endpoint, plan, clock=lambda: 1.0))
    assert result.outcome is SendOutcome.AMBIGUOUS_SEND
    assert result.http_status == STATUS_UNAVAILABLE
    assert result.retry_forbidden
    assert len(stub.posts()) == 1


def test_submit_refuses_an_empty_episode_id_set_without_request() -> None:
    plan = linkable_plan(mapping_revision="rev")
    pending = freeze(
        [res(EPISODE_101, EPISODE_A), res(EPISODE_102, EPISODE_B, pending=True)],
        shoko_paths=[SHOKO_FILE_PATH],
    )
    pending = must_plan(pending)
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    result = with_endpoint(stub, lambda endpoint: submit_link(endpoint, pending, clock=lambda: 0.0))
    assert result.outcome is SendOutcome.REFUSED
    assert stub.requests == []
    assert plan.episode_ids == (777, 778)


# ---------------------------------------------------------- verification polling


def observed_xrefs(*triples: tuple[int, int, int | None]) -> ShokoFileXRefs:
    episodes = tuple(
        XrefEpisode(shoko_id, anidb_id, "User") for (_anime, anidb_id, shoko_id) in triples
    )
    return ShokoFileXRefs(
        SHOKO_FILE_ID,
        SONARR_SIZE,
        (FileXRef(ANIME_ID, episodes),) if triples else (),
    )


def verify_with(observed: ShokoFileXRefs | None, plan: LinkPlan, now: float) -> VerificationVerdict:
    return verify_link(lambda: observed, plan, clock=lambda: now, deadline=100.0, max_polls=1)


def test_verification_exact_full_set_is_verified() -> None:
    plan = linkable_plan()
    verdict = verify_with(observed_xrefs((ANIME_ID, 5501, 777), (ANIME_ID, 5502, 778)), plan, 10.0)
    assert verdict is VerificationVerdict.VERIFIED


def test_verification_subset_within_deadline_is_pending() -> None:
    plan = linkable_plan()
    verdict = verify_with(observed_xrefs((ANIME_ID, 5501, 777)), plan, 10.0)
    assert verdict is VerificationVerdict.PENDING


def test_verification_subset_past_deadline_is_partial() -> None:
    plan = linkable_plan()
    verdict = verify_with(observed_xrefs((ANIME_ID, 5501, 777)), plan, 200.0)
    assert verdict is VerificationVerdict.PARTIAL


def test_verification_extra_identity_is_contradiction() -> None:
    plan = linkable_plan()
    verdict = verify_with(
        observed_xrefs((ANIME_ID, 5501, 777), (ANIME_ID, 5502, 778), (ANIME_ID, 5503, 779)),
        plan,
        10.0,
    )
    assert verdict is VerificationVerdict.CONTRADICTION


def test_verification_wrong_internal_id_is_contradiction() -> None:
    plan = linkable_plan()
    verdict = verify_with(observed_xrefs((ANIME_ID, 5501, 777), (ANIME_ID, 5502, 999)), plan, 10.0)
    assert verdict is VerificationVerdict.CONTRADICTION


def test_verification_still_unrecognized_past_deadline_is_contradiction() -> None:
    plan = linkable_plan()
    assert verify_with(observed_xrefs(), plan, 10.0) is VerificationVerdict.PENDING
    assert verify_with(observed_xrefs(), plan, 200.0) is VerificationVerdict.CONTRADICTION


def test_verification_fetch_failure_is_unknown() -> None:
    plan = linkable_plan()

    def boom() -> ShokoFileXRefs | None:
        raise httpx.ConnectError("no route to shoko")

    assert verify_link(boom, plan, clock=lambda: 0.0, deadline=100.0) is VerificationVerdict.UNKNOWN
    assert (
        verify_link(lambda: None, plan, clock=lambda: 0.0, deadline=100.0)
        is VerificationVerdict.UNKNOWN
    )


def test_verification_null_internal_id_is_not_a_verified_link() -> None:
    plan = linkable_plan()
    verdict = verify_with(observed_xrefs((ANIME_ID, 5501, 777), (ANIME_ID, 5502, None)), plan, 10.0)
    assert verdict is VerificationVerdict.PENDING


def test_verification_polls_until_the_set_completes() -> None:
    plan = linkable_plan()
    script = [
        observed_xrefs((ANIME_ID, 5501, 777)),
        observed_xrefs((ANIME_ID, 5501, 777), (ANIME_ID, 5502, 778)),
    ]
    index = 0

    def fetch() -> ShokoFileXRefs:
        nonlocal index
        value = script[min(index, len(script) - 1)]
        index += 1
        return value

    verdict = verify_link(
        fetch,
        plan,
        clock=FakeClock([0.0, 50.0, 90.0]),
        deadline=100.0,
    )
    assert verdict is VerificationVerdict.VERIFIED


def test_parse_file_xrefs_reads_the_wire_shape_and_rejects_malformed() -> None:
    payload = file_json(xrefs=[xref(ANIME_ID, [xref_episode(777, 5501)])])
    parsed = parse_file_xrefs(payload)
    assert parsed is not None
    assert parsed.xrefs[0].episodes[0].source == "User"
    assert parsed.xrefs[0].series_anidb_id == ANIME_ID
    assert parse_file_xrefs({"ID": 1, "Size": 2}) is None
    assert parse_file_xrefs({"ID": 1, "Size": 2, "SeriesIDs": "not-a-list"}) is None
    broken = {
        "ID": 1,
        "Size": 2,
        "SeriesIDs": [
            {
                "SeriesID": {"AniDB": 1234},
                "EpisodeIDs": [{"ID": None, "AniDB": 0, "Source": "User"}],
            }
        ],
    }
    assert parse_file_xrefs(broken) is None


# ----------------------------------------------------------------- fingerprint


def test_fingerprint_is_stable_across_permuted_inputs_and_clocks() -> None:
    first = freeze(
        [res(EPISODE_101, EPISODE_A), res(EPISODE_102, EPISODE_B)],
        mapping_revision="rev-1",
    )
    second = freeze(
        [res(EPISODE_102, EPISODE_B), res(EPISODE_101, EPISODE_A)],
        mapping_revision="rev-1",
    )
    first_plan = must_plan(first)
    second_plan = must_plan(second)
    assert plan_fingerprint(first_plan) == plan_fingerprint(second_plan)
    assert first_plan.fingerprint == plan_fingerprint(first_plan)
    rebuilt = linkable_plan(mapping_revision="rev-1")
    assert plan_fingerprint(rebuilt) == plan_fingerprint(first_plan)
    assert plan_fingerprint(rebuilt) != plan_fingerprint(linkable_plan())


def test_fingerprint_excludes_timestamp_and_queue_fields() -> None:
    plan = linkable_plan()
    names = {field.name for field in fields(plan)}
    assert not {"timestamp", "attempted_at", "queue", "waiting", "blocked", "clock"} & names
    assert plan_fingerprint(plan) == plan_fingerprint(plan)


def test_plan_and_fingerprints_carry_no_credentials() -> None:
    plan = linkable_plan()
    assert FAKE_KEY not in repr(plan)
    assert FAKE_KEY not in plan_fingerprint(plan)
    assert FAKE_KEY not in request_fingerprint(plan)


def test_freeze_plan_rejects_malformed_digests() -> None:
    resolutions = [res(EPISODE_101, EPISODE_A), res(EPISODE_102, EPISODE_B)]
    with pytest.raises(ConfigError):
        freeze(resolutions, targets_digest="nope")


# --------------------------------------------------------------- dry-run gates


def reconcile(
    stub: LinkStub,
    plan: LinkPlan,
    settings: Settings,
    **kwargs: Any,
) -> ReconcileResult:
    defaults: dict[str, Any] = {
        "active_targets_digest": plan.targets_digest,
        "active_structure_digest": plan.structure_digest,
        "attestation_valid": True,
        "apply_gate_open": True,
        "queue": BENIGN_QUEUE,
    }
    defaults.update(kwargs)
    return with_endpoint(
        stub,
        lambda endpoint: reconcile_link_plan(
            endpoint,
            plan,
            settings,
            FOLDERS,
            clock=lambda: RECONCILE_AT,
            **defaults,
        ),
    )


def test_dry_run_reconcile_performs_zero_posts(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    result = reconcile(stub, plan, settings)
    assert result.stage is ReconcileStage.DRY_RUN
    assert result.send is None
    assert stub.posts() == []
    assert set(stub.methods()) == {"GET"}


def test_dry_run_short_circuits_before_the_isolated_post(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    stub.redirects[("POST", f"/api/v3/File/{SHOKO_FILE_ID}/Link")] = "/"
    result = reconcile(stub, plan, settings, dry_run=True)
    assert result.stage is ReconcileStage.DRY_RUN
    assert stub.posts() == []


def test_apply_path_reaches_submit_only_when_preflight_ok(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    result = reconcile(stub, plan, settings, dry_run=False)
    assert result.stage is ReconcileStage.SUBMITTED
    assert result.send is not None
    assert result.send.outcome is SendOutcome.ACCEPTED_UNVERIFIED
    assert result.send.attempted_at == RECONCILE_AT
    assert len(stub.posts()) == 1
    assert stub.post_bodies == [{"EpisodeIDs": [777, 778]}]
    assert result.verification is None


def test_apply_path_stops_at_a_blocked_preflight_with_zero_posts(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    stub.routes[("GET", f"/api/v3/File/{SHOKO_FILE_ID}")] = file_json(
        xrefs=[xref(ANIME_ID, [xref_episode(777, 5501, "User")])]
    )
    result = reconcile(stub, plan, settings, dry_run=False)
    assert result.stage is ReconcileStage.PREFLIGHT_BLOCKED
    assert result.preflight is not None
    assert result.preflight.reasons == (PreflightReason.EXISTING_LINK,)
    assert stub.posts() == []


def test_apply_blocked_by_closed_gate_never_touches_the_network(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    result = reconcile(stub, plan, settings, dry_run=False, apply_gate_open=False)
    assert result.stage is ReconcileStage.PREFLIGHT_BLOCKED
    assert stub.requests == []


def test_apply_verifies_after_an_accepted_send(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    result = reconcile(
        stub,
        plan,
        settings,
        dry_run=False,
        verify_fetch=lambda: observed_xrefs((ANIME_ID, 5501, 777), (ANIME_ID, 5502, 778)),
        verify_deadline=100.0,
    )
    assert result.stage is ReconcileStage.SUBMITTED
    assert result.verification is VerificationVerdict.VERIFIED


def test_pending_target_plan_never_reaches_preflight(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = base_settings(base_document, write_document)
    plan = must_plan(
        freeze([res(EPISODE_101, EPISODE_A), res(EPISODE_102, EPISODE_B, pending=True)])
    )
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    result = reconcile(stub, plan, settings, dry_run=False)
    assert result.stage is ReconcileStage.PENDING_TARGETS
    assert stub.requests == []


# ------------------------------------------ M5 review-driven regressions (M5 pass 2)


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("IsIgnored", True, PreflightReason.NOT_CANDIDATE),
        ("IsVariation", True, PreflightReason.NOT_CANDIDATE),
        ("Hashes", None, PreflightReason.NOT_CANDIDATE),
        ("Imported", 12345, PreflightReason.MALFORMED),
    ],
)
def test_preflight_blocks_when_fresh_read_flags_file_out_of_candidacy(
    base_document: dict[str, Any],
    write_document: Writer,
    field: str,
    value: Any,
    reason: PreflightReason,
) -> None:
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    body = file_json()
    body[field] = value
    stub.routes[("GET", f"/api/v3/File/{SHOKO_FILE_ID}")] = body
    verdict = run_preflight(stub, plan, settings)
    assert verdict.ok is False
    assert reason in verdict.reasons


def test_preflight_accepts_fully_hashed_imported_null_candidate(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    # Imported=null is diagnostic only: a fully hashed, zero-XRef, size-matching,
    # fully-owned candidate passes fresh preflight despite never having been imported.
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    stub.routes[("GET", f"/api/v3/File/{SHOKO_FILE_ID}")] = file_json(imported=None)
    verdict = run_preflight(stub, plan, settings)
    assert verdict.ok is True
    assert PreflightReason.NOT_CANDIDATE not in verdict.reasons


def test_apply_path_links_fully_hashed_imported_null_candidate(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    # A valid AniDB mapping yields a proposed link, and mocked apply accepts the
    # Imported=null candidate: preflight ok → exactly one POST to the link endpoint.
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    stub.routes[("GET", f"/api/v3/File/{SHOKO_FILE_ID}")] = file_json(imported=None)
    result = reconcile(stub, plan, settings, dry_run=False)
    assert result.stage is ReconcileStage.SUBMITTED
    assert result.send is not None
    assert result.send.outcome is SendOutcome.ACCEPTED_UNVERIFIED
    assert len(stub.posts()) == 1
    assert stub.post_bodies == [{"EpisodeIDs": [777, 778]}]


PREFLIGHT_ED2K = hf.hex32(SHOKO_FILE_ID)

UNSETTLED_PREFLIGHT_IDENTITIES: list[tuple[str, dict[str, Any]]] = [
    (
        "empty ED2K while Shoko is still hashing the file",
        {"ED2K": "", "SHA1": None, "CRC32": None, "MD5": None},
    ),
    (
        "empty ED2K with a populated SHA1",
        {"ED2K": "", "SHA1": hf.hex40(SHOKO_FILE_ID), "CRC32": hf.hex8(SHOKO_FILE_ID), "MD5": None},
    ),
    ("whitespace-only ED2K", {"ED2K": "  ", "SHA1": None, "CRC32": None, "MD5": None}),
    ("placeholder-shaped ED2K", {"ED2K": "ed2k-500", "SHA1": None, "CRC32": None, "MD5": None}),
    ("garbage ED2K", {"ED2K": "not-a-hash", "SHA1": None, "CRC32": None, "MD5": None}),
    ("numeric ED2K", {"ED2K": SHOKO_FILE_ID, "SHA1": None, "CRC32": None, "MD5": None}),
    ("truncated ED2K (31 hex chars)", {"ED2K": PREFLIGHT_ED2K[:-1], "SHA1": None}),
    ("oversized ED2K (33 hex chars)", {"ED2K": PREFLIGHT_ED2K + "a", "SHA1": None}),
    ("non-hexadecimal ED2K (32 chars)", {"ED2K": "z" * 32, "SHA1": None}),
    ("missing ED2K key", {"SHA1": None, "CRC32": None, "MD5": None}),
    ("all-null Hashes object", {"ED2K": None, "SHA1": None, "CRC32": None, "MD5": None}),
    ("empty SHA1 string", {"ED2K": PREFLIGHT_ED2K, "SHA1": "", "CRC32": None, "MD5": None}),
    ("short SHA1 string (39 hex)", {"ED2K": PREFLIGHT_ED2K, "SHA1": "a" * 39, "MD5": None}),
    ("non-hex MD5 string", {"ED2K": PREFLIGHT_ED2K, "SHA1": None, "MD5": "z" * 32}),
    ("numeric CRC32", {"ED2K": PREFLIGHT_ED2K, "SHA1": None, "CRC32": SHOKO_FILE_ID, "MD5": None}),
    (
        "short CRC32 string (6 hex)",
        {"ED2K": PREFLIGHT_ED2K, "SHA1": None, "CRC32": "ABC123", "MD5": None},
    ),
]


@pytest.mark.parametrize(
    ("label", "hashes"),
    UNSETTLED_PREFLIGHT_IDENTITIES,
    ids=[label for label, _ in UNSETTLED_PREFLIGHT_IDENTITIES],
)
def test_preflight_blocks_unsettled_hash_identity_on_fresh_read(
    base_document: dict[str, Any],
    write_document: Writer,
    label: str,
    hashes: dict[str, Any],
) -> None:
    # Fresh-read gate (F13): an empty/whitespace/malformed/partially populated Hashes
    # object is not a settled identity, so the POST is never built on it.
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    stub.routes[("GET", f"/api/v3/File/{SHOKO_FILE_ID}")] = file_json(imported=None, hashes=hashes)
    verdict = run_preflight(stub, plan, settings)
    assert verdict.ok is False
    assert PreflightReason.NOT_CANDIDATE in verdict.reasons
    assert any("hashing not settled" in detail for detail in verdict.details)
    assert stub.posts() == []


def test_apply_path_refuses_unsettled_hash_identity_without_post(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    # Apply path: the preflight gate stops the cycle before any POST is sent.
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    empty = {"ED2K": "", "SHA1": None, "CRC32": None, "MD5": None}
    stub.routes[("GET", f"/api/v3/File/{SHOKO_FILE_ID}")] = file_json(imported=None, hashes=empty)
    result = reconcile(stub, plan, settings, dry_run=False)
    assert result.stage is not ReconcileStage.SUBMITTED
    assert stub.posts() == []


def test_preflight_accepts_complete_ed2k_identity_with_full_hash_set(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    # Settled identity at preflight: valid ED2K + 40-hex SHA1 + 8-hex CRC32 + 32-hex MD5,
    # Imported=null.
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    full = hf.full_hashes(SHOKO_FILE_ID, SONARR_SIZE, "S01E01+02.mkv")
    stub.routes[("GET", f"/api/v3/File/{SHOKO_FILE_ID}")] = file_json(imported=None, hashes=full)
    verdict = run_preflight(stub, plan, settings)
    assert verdict.ok is True
    assert PreflightReason.NOT_CANDIDATE not in verdict.reasons


def test_preflight_accepts_ed2k_only_with_null_optional_hashes(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    # Shoko reports a complete 32-hex ED2K with SHA1/CRC32/MD5 as null: an uncomputed
    # optional hash must never fail a settled file at fresh preflight (Imported=null).
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    stub.routes[("GET", f"/api/v3/File/{SHOKO_FILE_ID}")] = file_json(
        imported=None, hashes=hf.ed2k_hashes(SHOKO_FILE_ID, SONARR_SIZE, "S01E01+02.mkv")
    )
    verdict = run_preflight(stub, plan, settings)
    assert verdict.ok is True
    assert PreflightReason.NOT_CANDIDATE not in verdict.reasons


def test_preflight_accepts_lowercase_ed2k_identity(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    # Case-insensitive validation with the exact bytes preserved for comparison.
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    hashes = {
        "ED2K": PREFLIGHT_ED2K.lower(),
        "SHA1": hf.hex40(SHOKO_FILE_ID).lower(),
        "CRC32": hf.hex8(SHOKO_FILE_ID).lower(),
        "MD5": hf.hex32(SHOKO_FILE_ID).lower(),
    }
    stub.routes[("GET", f"/api/v3/File/{SHOKO_FILE_ID}")] = file_json(imported=None, hashes=hashes)
    verdict = run_preflight(stub, plan, settings)
    assert verdict.ok is True
    assert PreflightReason.NOT_CANDIDATE not in verdict.reasons


def test_preflight_protects_a_file_that_already_carries_xrefs(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    # A fully hashed file (valid 32-hex ED2K) that already carries a User XRef
    # is protected: the settled-hash gate passes, then the existing-link gate blocks
    # the POST. This proves F16's hash fix does not weaken existing-link protection.
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    stub.routes[("GET", f"/api/v3/File/{SHOKO_FILE_ID}")] = file_json(
        imported=None,
        hashes=hf.full_hashes(SHOKO_FILE_ID, SONARR_SIZE, "S01E01+02.mkv"),
        xrefs=[xref(ANIME_ID, [xref_episode(777, 5501, "User")])],
    )
    verdict = run_preflight(stub, plan, settings)
    assert verdict.ok is False
    assert verdict.reasons == (PreflightReason.EXISTING_LINK,)
    assert stub.posts() == []


def test_preflight_blocks_on_echoed_file_id_mismatch(
    base_document: dict[str, Any],
    write_document: Writer,
) -> None:
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    stub.routes[("GET", f"/api/v3/File/{SHOKO_FILE_ID}")] = file_json(id_=SHOKO_FILE_ID + 1)
    verdict = run_preflight(stub, plan, settings)
    assert verdict.ok is False
    assert PreflightReason.MALFORMED in verdict.reasons
    assert any("echoes" in detail for detail in verdict.details)


def test_truncated_listing_is_list_truncated_not_absence() -> None:
    stub = LinkStub()
    stub.routes = {
        ("GET", f"/api/v3/Series/AniDB/{ANIME_ID}/Episode"): {
            "Total": 200,
            "List": [anidb_episode(5501, ANIME_ID, "Episode", 10)],
        }
    }
    resolved = with_endpoint(
        stub,
        lambda endpoint: EpisodeIdResolver(endpoint, clock=FakeClock([0.0])).resolve(
            ANIME_ID, "Episode", 11
        ),
    )
    assert resolved.status is EpiStatus.LIST_TRUNCATED
    assert "cannot be" in resolved.detail


def test_reconcile_passes_verify_throttle_through(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = base_settings(base_document, write_document)
    plan = linkable_plan()
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    advances: list[float] = []
    result = reconcile(
        stub,
        plan,
        settings,
        dry_run=False,
        verify_fetch=lambda: observed_xrefs((ANIME_ID, 5501, 777)),
        verify_deadline=10_000.0,
        verify_poll_interval=2.5,
        verify_advance=advances.append,
    )
    assert result.verification is VerificationVerdict.PENDING
    assert len(advances) > 0
    assert set(advances) == {2.5}


def test_preflight_dedups_target_rereads_sharing_an_anidb_episode(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = base_settings(base_document, write_document)
    # ";1-1;2-1;" parts: two Sonarr episodes, one AniDB episode id, one Shoko id.
    plan = must_plan(freeze([res(EPISODE_101, EPISODE_A), res(EPISODE_102, EPISODE_A)]))
    assert plan.status is PlanStatus.LINKABLE
    stub = LinkStub()
    stub.routes = preflight_ok_routes()
    verdict = run_preflight(stub, plan, settings)
    assert verdict.ok is True
    episode_reads = [p for p in stub.paths() if p == "/api/v3/Episode/AniDB/5501/Episode"]
    assert len(episode_reads) == 1
