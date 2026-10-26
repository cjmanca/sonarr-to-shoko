"""Automatic Sonarr anime/root discovery and plain-language scope decisions."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import httpx
import pytest

from shoko_linker.http import ApiEndpoint, make_timeout, sonarr_headers
from shoko_linker.paths import PathMapping
from shoko_linker.setup import (
    AnimeRootCandidate,
    AnimeScopeDiscovery,
    ApiCompatibility,
    MappingDiscoveryStatus,
    PathMappingDiscovery,
    ScopeReadStatus,
    ScopeStatus,
    SetupReadinessStage,
    build_setup_readiness,
    discover_sonarr_scope,
    infer_path_mappings,
    inspect_api_compatibility,
)
from shoko_linker.shoko import (
    ImportFolder,
    ShokoDiscovery,
    ShokoFile,
    ShokoHashes,
    ShokoLocation,
    ShokoVerdict,
)
from shoko_linker.sonarr import (
    EligibleSeries,
    InventoryVerdict,
    SonarrFile,
    SonarrInventory,
    SonarrSeries,
)

SONARR_KEY = "sonarr-setup-test-key-never-a-real-secret"
ANIME_ROOT = "/data/media/anime"
SECOND_ROOT = "/data/media/anime-2"
LIVE_ROOT = "/data/media/tv"
SHOKO_ROOT = "/anime"
FILE_SIZE = 1048576
VALID_ED2K = "0123456789abcdef0123456789abcdef"
Handler = Callable[[httpx.Request], httpx.Response]


def series_entry(
    series_id: int,
    root: str,
    *,
    series_type: str | None = "anime",
    tvdb_id: int | None = None,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "id": series_id,
        "title": f"Series {series_id}",
        "rootFolderPath": root,
        "tvdbId": 1000 + series_id if tvdb_id is None else tvdb_id,
    }
    if series_type is not None:
        entry["seriesType"] = series_type
    return entry


def root_entry(path: str, *, accessible: bool = True) -> dict[str, Any]:
    return {"path": path, "accessible": accessible}


def inventory_for(
    entries: tuple[tuple[int, str, int, str, int], ...],
    *,
    complete: bool = True,
) -> SonarrInventory:
    series_by_id: dict[int, SonarrSeries] = {}
    roots_by_series: dict[int, str] = {}
    files: list[SonarrFile] = []
    for series_id, root, file_id, path, size in entries:
        series = SonarrSeries(series_id, f"Series {series_id}", root, 2000 + series_id, "anime")
        series_by_id[series_id] = series
        roots_by_series[series_id] = root
        files.append(SonarrFile(series_id, file_id, path, size, ()))
    eligible = tuple(
        EligibleSeries(series, roots_by_series[series_id])
        for series_id, series in sorted(series_by_id.items())
    )
    return SonarrInventory(
        eligible,
        (),
        (),
        tuple(files),
        (),
        (),
        (),
        InventoryVerdict(complete, complete, (), ()),
    )


def shoko_file(
    file_id: int,
    path: str,
    *,
    size: int = FILE_SIZE,
    folder_id: int = 1,
    folder_path: str = SHOKO_ROOT,
    relative_path: str | None = None,
    locations: tuple[ShokoLocation, ...] | None = None,
) -> ShokoFile:
    relative = path.removeprefix(f"{folder_path}/") if relative_path is None else relative_path
    return ShokoFile(
        file_id,
        size,
        False,
        False,
        None,
        ShokoHashes(VALID_ED2K, None, None, None),
        0,
        locations or (ShokoLocation(file_id + 1000, file_id, folder_id, relative, path, True),),
    )


def shoko_snapshot(
    files: tuple[ShokoFile, ...],
    folders: tuple[ImportFolder, ...] | None = None,
    *,
    complete: bool = True,
) -> ShokoDiscovery:
    folder_values = folders or (ImportFolder(1, "Anime", SHOKO_ROOT, "None", True),)
    return ShokoDiscovery(
        files,
        {folder.id: folder for folder in folder_values},
        ShokoVerdict(complete, (), ()),
        1,
        len(files),
        1000,
    )


def endpoint_for(
    *,
    series: list[dict[str, Any]],
    roots: list[dict[str, Any]],
    statuses: dict[str, int] | None = None,
    base_url: str = "http://sonarr:8989",
) -> tuple[ApiEndpoint, list[httpx.Request]]:
    requests: list[httpx.Request] = []
    response_statuses = statuses or {}

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        status = response_statuses.get(request.url.path, 200)
        if request.url.path.endswith("/api/v3/series"):
            return httpx.Response(status, json=series, request=request)
        if request.url.path.endswith("/api/v3/rootfolder"):
            return httpx.Response(status, json=roots, request=request)
        return httpx.Response(404, json={"message": "unexpected request"}, request=request)

    client = httpx.Client(
        base_url=base_url.rstrip("/") + "/",
        timeout=make_timeout(1.0, 1.0),
        follow_redirects=False,
        transport=httpx.MockTransport(handler),
    )
    return ApiEndpoint(client, lambda: sonarr_headers(SONARR_KEY)), requests


def response_endpoint(
    base_url: str,
    path: str,
    payload: Any,
    *,
    status: int = 200,
) -> tuple[ApiEndpoint, list[httpx.Request]]:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status, json=payload, request=request)

    client = httpx.Client(
        base_url=base_url.rstrip("/") + "/",
        timeout=make_timeout(1.0, 1.0),
        follow_redirects=False,
        transport=httpx.MockTransport(handler),
    )
    return ApiEndpoint(client, lambda: sonarr_headers(SONARR_KEY)), requests


def test_single_anime_root_is_presented_for_one_time_confirmation() -> None:
    endpoint, requests = endpoint_for(
        series=[
            series_entry(1, ANIME_ROOT),
            series_entry(2, LIVE_ROOT, series_type="standard"),
        ],
        roots=[root_entry(ANIME_ROOT), root_entry(LIVE_ROOT)],
    )
    try:
        discovery = discover_sonarr_scope(endpoint)
    finally:
        endpoint.client.close()

    assert discovery.status is ScopeStatus.CONFIRM_SCOPE
    assert [(root.path, root.series_ids) for root in discovery.candidates] == [(ANIME_ROOT, (1,))]
    assert discovery.message == f"Confirm this Sonarr anime folder: {ANIME_ROOT}"
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/api/v3/series"),
        ("GET", "/api/v3/rootfolder"),
    ]


def test_multiple_anime_roots_ask_one_plain_language_question() -> None:
    endpoint, _ = endpoint_for(
        series=[series_entry(2, SECOND_ROOT), series_entry(1, ANIME_ROOT)],
        roots=[root_entry(SECOND_ROOT), root_entry(ANIME_ROOT)],
    )
    try:
        discovery = discover_sonarr_scope(endpoint)
    finally:
        endpoint.client.close()

    assert discovery.status is ScopeStatus.SELECT_SCOPE
    assert tuple(root.path for root in discovery.candidates) == (ANIME_ROOT, SECOND_ROOT)
    assert discovery.message == "Which Sonarr folder contains your anime?"


def test_unidentified_series_type_does_not_guess_anime_scope() -> None:
    endpoint, _ = endpoint_for(
        series=[series_entry(1, ANIME_ROOT, series_type=None)],
        roots=[root_entry(ANIME_ROOT)],
    )
    try:
        discovery = discover_sonarr_scope(endpoint)
    finally:
        endpoint.client.close()

    assert discovery.status is ScopeStatus.BLOCKED
    assert discovery.candidates == ()
    assert "supported anime series types" in discovery.message
    assert discovery.read_status is ScopeReadStatus.INCOMPLETE


def test_inaccessible_anime_root_blocks_scope_discovery() -> None:
    endpoint, _ = endpoint_for(
        series=[series_entry(1, ANIME_ROOT)],
        roots=[root_entry(ANIME_ROOT, accessible=False)],
    )
    try:
        discovery = discover_sonarr_scope(endpoint)
    finally:
        endpoint.client.close()

    assert discovery.status is ScopeStatus.BLOCKED
    assert discovery.candidates == ()
    assert "accessible" in discovery.message.lower()


def test_incomplete_or_malformed_inventory_fails_closed() -> None:
    endpoint, _ = endpoint_for(
        series=[series_entry(1, ANIME_ROOT)],
        roots=[{"path": ANIME_ROOT, "accessible": "yes"}],
    )
    try:
        discovery = discover_sonarr_scope(endpoint)
    finally:
        endpoint.client.close()

    assert discovery.status is ScopeStatus.BLOCKED
    assert discovery.candidates == ()
    assert discovery.read_status is ScopeReadStatus.INCOMPLETE


@pytest.mark.parametrize("failed_path", ["/api/v3/series", "/api/v3/rootfolder"])
def test_scope_endpoint_failures_are_not_complete_empty_scope(failed_path: str) -> None:
    endpoint, requests = endpoint_for(
        series=[series_entry(1, ANIME_ROOT)],
        roots=[root_entry(ANIME_ROOT)],
        statuses={failed_path: 503},
    )
    try:
        discovery = discover_sonarr_scope(endpoint)
    finally:
        endpoint.client.close()

    assert discovery.status is ScopeStatus.BLOCKED
    assert discovery.read_status is ScopeReadStatus.UNAVAILABLE
    assert discovery.candidates == ()
    assert all(request.method == "GET" for request in requests)


def test_successful_empty_series_inventory_is_complete_observation() -> None:
    endpoint, _ = endpoint_for(series=[], roots=[])
    try:
        discovery = discover_sonarr_scope(endpoint)
    finally:
        endpoint.client.close()

    assert discovery.status is ScopeStatus.NO_ANIME
    assert discovery.read_status is ScopeReadStatus.COMPLETE


def test_successful_rootfolder_response_missing_confirmed_root_is_complete_observation() -> None:
    endpoint, _ = endpoint_for(series=[series_entry(1, ANIME_ROOT)], roots=[])
    try:
        discovery = discover_sonarr_scope(endpoint)
    finally:
        endpoint.client.close()

    assert discovery.status is ScopeStatus.BLOCKED
    assert discovery.read_status is ScopeReadStatus.COMPLETE
    assert discovery.candidates == ()


def test_sonarr_url_base_is_preserved_for_both_read_only_discovery_requests() -> None:
    endpoint, requests = endpoint_for(
        series=[series_entry(1, ANIME_ROOT)],
        roots=[root_entry(ANIME_ROOT)],
        base_url="http://sonarr:8989/sonarr",
    )
    try:
        discovery = discover_sonarr_scope(endpoint)
    finally:
        endpoint.client.close()

    assert discovery.status is ScopeStatus.CONFIRM_SCOPE
    assert [request.url.path for request in requests] == [
        "/sonarr/api/v3/series",
        "/sonarr/api/v3/rootfolder",
    ]
    assert all(request.method == "GET" for request in requests)


def test_verified_api_versions_report_compatible_supported_endpoints() -> None:
    sonarr, sonarr_requests = response_endpoint(
        "http://sonarr:8989",
        "/api/v3/system/status",
        {"version": "6.0.0.0", "urlBase": ""},
    )
    shoko, shoko_requests = response_endpoint(
        "http://shoko:8111",
        "/api/v3/Init/Version",
        {"Server": {"Version": "5.3.3", "ReleaseChannel": "Stable"}},
    )
    try:
        compatibility = inspect_api_compatibility(sonarr, shoko)
    finally:
        sonarr.client.close()
        shoko.client.close()

    assert isinstance(compatibility, ApiCompatibility)
    assert compatibility.sonarr_version == "6.0.0.0"
    assert compatibility.shoko_version == "5.3.3"
    assert compatibility.compatible is True
    assert compatibility.messages == ()
    assert compatibility.capabilities == (
        "sonarr_api_v3_system_status",
        "shoko_api_v3_init_version",
    )
    assert [request.url.path for request in sonarr_requests] == ["/api/v3/system/status"]
    assert [request.url.path for request in shoko_requests] == ["/api/v3/Init/Version"]
    assert all(request.method == "GET" for request in (*sonarr_requests, *shoko_requests))


def test_sonarr_v4_and_v5_api_v3_versions_are_supported() -> None:
    for version in ("4.0.15.2941", "5.0.0.0"):
        sonarr, _ = response_endpoint(
            "http://sonarr:8989", "/api/v3/system/status", {"version": version}
        )
        shoko, _ = response_endpoint(
            "http://shoko:8111",
            "/api/v3/Init/Version",
            {"Server": {"Version": "5.3.3"}},
        )
        try:
            compatibility = inspect_api_compatibility(sonarr, shoko)
        finally:
            sonarr.client.close()
            shoko.client.close()
        assert compatibility.sonarr_supported is True
        assert compatibility.compatible is True


@pytest.mark.parametrize("version", ["5.3.3.0", "5.3.4", "6.0.0", "6.1.0-dev.42"])
def test_shoko_release_labels_do_not_restrict_available_v3_api(version: str) -> None:
    sonarr, _ = response_endpoint(
        "http://sonarr:8989", "/api/v3/system/status", {"version": "4.0.0"}
    )
    shoko, requests = response_endpoint(
        "http://shoko:8111", "/api/v3/Init/Version", {"Server": {"Version": version}}
    )
    try:
        compatibility = inspect_api_compatibility(sonarr, shoko)
    finally:
        sonarr.client.close()
        shoko.client.close()
    assert compatibility.compatible
    assert compatibility.shoko_version == version
    assert compatibility.messages == ()
    assert all(request.method == "GET" for request in requests)


def test_unsupported_sonarr_version_still_blocks_setup() -> None:
    sonarr, _ = response_endpoint(
        "http://sonarr:8989", "/api/v3/system/status", {"version": "7.0.0.1"}
    )
    shoko, _ = response_endpoint(
        "http://shoko:8111", "/api/v3/Init/Version", {"Server": {"Version": "6.0.0"}}
    )
    try:
        compatibility = inspect_api_compatibility(sonarr, shoko)
    finally:
        sonarr.client.close()
        shoko.client.close()

    assert compatibility.compatible is False
    assert compatibility.sonarr_supported is False
    assert compatibility.shoko_supported is True
    assert len(compatibility.messages) == 1


def test_api_version_probe_refuses_malformed_version_payloads() -> None:
    sonarr, _ = response_endpoint("http://sonarr:8989", "/api/v3/system/status", {"version": 4})
    shoko, _ = response_endpoint(
        "http://shoko:8111", "/api/v3/Init/Version", {"Settings": {"Password": "sentinel"}}
    )
    try:
        compatibility = inspect_api_compatibility(sonarr, shoko)
    finally:
        sonarr.client.close()
        shoko.client.close()

    assert compatibility.compatible is False
    assert compatibility.sonarr_version is None
    assert compatibility.shoko_version is None
    assert "sentinel" not in repr(compatibility)


def test_setup_readiness_reports_the_next_plain_language_action() -> None:
    compatible = ApiCompatibility("6.0.0.0", "5.3.3", True, True, (), ())
    one_root = AnimeScopeDiscovery(
        ScopeStatus.CONFIRM_SCOPE,
        (AnimeRootCandidate(ANIME_ROOT, (1,)),),
        f"Confirm this Sonarr anime folder: {ANIME_ROOT}",
    )
    selected = build_setup_readiness(compatible, one_root)

    assert selected.stage is SetupReadinessStage.CONFIRM_SCOPE
    assert selected.message == one_root.message
    path_discovery = build_setup_readiness(compatible, one_root, scope_confirmed=True)
    assert path_discovery.stage is SetupReadinessStage.DISCOVER_PATHS

    paths = PathMappingDiscovery(
        MappingDiscoveryStatus.READY,
        (PathMapping(ANIME_ROOT, SHOKO_ROOT),),
        (),
        "Paths match.",
    )
    safety = build_setup_readiness(compatible, one_root, paths, scope_confirmed=True)

    assert safety.stage is SetupReadinessStage.SAFETY_CHECKS
    assert "safety" in safety.message.lower()


def test_setup_readiness_blocks_unsupported_apis_and_ambiguous_scope() -> None:
    incompatible = ApiCompatibility(
        "7.0.0.1",
        "6.0.0",
        False,
        False,
        (),
        ("unsupported Sonarr", "unsupported Shoko"),
    )
    ambiguous = AnimeScopeDiscovery(
        ScopeStatus.SELECT_SCOPE,
        (
            AnimeRootCandidate(ANIME_ROOT, (1,)),
            AnimeRootCandidate(SECOND_ROOT, (2,)),
        ),
        "Which Sonarr folder contains your anime?",
    )

    version_result = build_setup_readiness(incompatible, ambiguous)
    scope_result = build_setup_readiness(
        ApiCompatibility("6.0.0.0", "5.3.3", True, True, (), ()), ambiguous
    )

    assert version_result.stage is SetupReadinessStage.BLOCKED
    assert version_result.message == "unsupported Sonarr"
    assert scope_result.stage is SetupReadinessStage.SELECT_SCOPE
    assert scope_result.message == "Which Sonarr folder contains your anime?"


def test_infers_a_unique_mapping_from_exact_relative_path_and_size() -> None:
    sonarr_root = "/data/media/anime"
    sonarr_path = f"{sonarr_root}/Show A/S01/E01.mkv"
    shoko_path = f"{SHOKO_ROOT}/Show A/S01/E01.mkv"
    inventory = inventory_for(((1, sonarr_root, 11, sonarr_path, FILE_SIZE),))
    discovery = shoko_snapshot((shoko_file(101, shoko_path),))

    result = infer_path_mappings(inventory, discovery)

    assert result.status is MappingDiscoveryStatus.READY
    assert result.mappings == (PathMapping(sonarr_root, SHOKO_ROOT),)
    assert result.evidence[0].shoko_file_ids == (101,)


def test_does_not_infer_mapping_from_basename_and_size_without_relative_path_match() -> None:
    sonarr_root = "/data/media/anime"
    inventory = inventory_for(((1, sonarr_root, 11, f"{sonarr_root}/Show A/E01.mkv", FILE_SIZE),))
    discovery = shoko_snapshot((shoko_file(101, f"{SHOKO_ROOT}/Different Show/E01.mkv"),))

    result = infer_path_mappings(inventory, discovery)

    assert result.status is MappingDiscoveryStatus.NEEDS_USER_MAPPING
    assert result.mappings == ()


def test_does_not_infer_mapping_when_exact_relative_path_has_different_size() -> None:
    sonarr_root = "/data/media/anime"
    inventory = inventory_for(((1, sonarr_root, 11, f"{sonarr_root}/Show A/E01.mkv", FILE_SIZE),))
    discovery = shoko_snapshot(
        (shoko_file(101, f"{SHOKO_ROOT}/Show A/E01.mkv", size=FILE_SIZE + 1),)
    )

    result = infer_path_mappings(inventory, discovery)

    assert result.status is MappingDiscoveryStatus.NEEDS_USER_MAPPING
    assert result.mappings == ()


def test_duplicate_exact_sonarr_candidates_remain_ambiguous() -> None:
    inventory = inventory_for(
        (
            (1, "/data/media/anime-a", 11, "/data/media/anime-a/Show/E01.mkv", FILE_SIZE),
            (2, "/data/media/anime-b", 22, "/data/media/anime-b/Show/E01.mkv", FILE_SIZE),
        )
    )
    discovery = shoko_snapshot((shoko_file(101, f"{SHOKO_ROOT}/Show/E01.mkv"),))

    result = infer_path_mappings(inventory, discovery)

    assert result.status is MappingDiscoveryStatus.NEEDS_USER_MAPPING
    assert result.mappings == ()


def test_manual_drop_folders_and_incomplete_snapshots_are_never_mapping_evidence() -> None:
    sonarr_root = "/data/media/anime"
    inventory = inventory_for(((1, sonarr_root, 11, f"{sonarr_root}/Show/E01.mkv", FILE_SIZE),))
    manual = ImportFolder(1, "Manual", SHOKO_ROOT, "Source", True)
    file = shoko_file(101, f"{SHOKO_ROOT}/Show/E01.mkv")

    manual_result = infer_path_mappings(inventory, shoko_snapshot((file,), (manual,)))
    incomplete_result = infer_path_mappings(inventory, shoko_snapshot((file,), complete=False))
    sonarr_incomplete = infer_path_mappings(
        inventory_for(
            ((1, sonarr_root, 11, f"{sonarr_root}/Show/E01.mkv", FILE_SIZE),), complete=False
        ),
        shoko_snapshot((file,)),
    )

    assert manual_result.status is MappingDiscoveryStatus.NEEDS_USER_MAPPING
    assert manual_result.mappings == ()
    assert incomplete_result.status is MappingDiscoveryStatus.BLOCKED
    assert sonarr_incomplete.status is MappingDiscoveryStatus.BLOCKED


def test_every_location_of_a_shoko_file_must_be_accessible_and_safe() -> None:
    sonarr_root = "/data/media/anime"
    sonarr_path = f"{sonarr_root}/Show/E01.mkv"
    inventory = inventory_for(((1, sonarr_root, 11, sonarr_path, FILE_SIZE),))
    multi_location_file = shoko_file(
        101,
        f"{SHOKO_ROOT}/Show/E01.mkv",
        locations=(
            ShokoLocation(1, 101, 1, "Show/E01.mkv", f"{SHOKO_ROOT}/Show/E01.mkv", True),
            ShokoLocation(2, 101, 2, "Show/E01.mkv", "/mirror/Show/E01.mkv", False),
        ),
    )
    discovery = shoko_snapshot(
        (multi_location_file,),
        (
            ImportFolder(1, "Anime", SHOKO_ROOT, "None", True),
            ImportFolder(2, "Mirror", "/mirror", "None", True),
        ),
    )

    result = infer_path_mappings(inventory, discovery)

    assert result.status is MappingDiscoveryStatus.NEEDS_USER_MAPPING
    assert result.mappings == ()


def test_conflicting_shoko_roots_for_one_sonarr_root_require_user_mapping() -> None:
    sonarr_root = "/data/media/anime"
    inventory = inventory_for(
        (
            (1, sonarr_root, 11, f"{sonarr_root}/Show A/E01.mkv", FILE_SIZE),
            (1, sonarr_root, 12, f"{sonarr_root}/Show B/E01.mkv", FILE_SIZE + 1),
        )
    )
    discovery = shoko_snapshot(
        (
            shoko_file(101, "/anime-a/Show A/E01.mkv", folder_path="/anime-a"),
            shoko_file(
                102,
                "/anime-b/Show B/E01.mkv",
                folder_id=2,
                folder_path="/anime-b",
                size=FILE_SIZE + 1,
            ),
        ),
        (
            ImportFolder(1, "Anime A", "/anime-a", "None", True),
            ImportFolder(2, "Anime B", "/anime-b", "None", True),
        ),
    )

    result = infer_path_mappings(inventory, discovery)

    assert result.status is MappingDiscoveryStatus.NEEDS_USER_MAPPING
    assert result.mappings == ()
    assert len(result.evidence) == 2
