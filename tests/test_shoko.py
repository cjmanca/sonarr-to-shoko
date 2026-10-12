"""Shoko discovery, ownership and exact correspondence (M3).

Every request goes through the real http.py redirect/auth policy over an
httpx.MockTransport: nothing here touches the network. The request log per test
proves which API calls actually happened — every request in this suite is a GET,
so zero Shoko writes are performed.
"""

from __future__ import annotations

import copy
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import hash_fixtures as hf
import httpx
import pytest

from shoko_linker.config import Settings, load_settings
from shoko_linker.errors import PathError
from shoko_linker.http import ApiEndpoint, make_timeout, shoko_headers
from shoko_linker.paths import PathMapping
from shoko_linker.shoko import (
    PAGE_SIZE,
    ExclusionReason,
    FileMatch,
    IdentityChange,
    ImportFolder,
    ShokoDiscovery,
    ShokoFile,
    ShokoHashes,
    ShokoLocation,
    ShokoMatchResult,
    SnapshotReason,
    build_identity,
    collect_shoko_discovery,
    compare_identity,
    index_sonarr_files,
    join_location_path,
    match_shoko_files,
    parse_import_folders,
    select_reverse_mapping,
    translate_to_sonarr,
)
from shoko_linker.sonarr import SonarrEpisode, SonarrFile

FAKE_KEY = "fake-key-never-a-real-one-0000"
SHOKO_BASE = "http://shoko:8111"
SONARR_ROOT = "/data/media/anime"
SHOKO_ROOT = "/anime"
MAPPING = {"sonarr": SONARR_ROOT, "shoko": SHOKO_ROOT}
FILE_PATH = "/api/v3/File"
FOLDER_PATH = "/api/v3/ImportFolder"
IMPORTED_NULL_PAGES = 3
SMALL_TOTAL = 5
SMALL_PAGE_SIZE = 2
SMALL_PAGES = 3
PAGE_CAP = 4
CAPPED_FILES = 8
VALID_ED2K = hf.hex32(112)
LARGE_TOTAL = 7000
LARGE_PAGE_SIZE = 1000
LARGE_PAGES = 7
WALL_TIME_LIMIT_SECONDS = 2.0
SLASH_FORM_FILE_ID = 720

Handler = Callable[[httpx.Request], httpx.Response]
Writer = Callable[[dict[str, Any]], Path]


def shoko_endpoint(handler: Handler, base_url: str = SHOKO_BASE) -> ApiEndpoint:
    client = httpx.Client(
        base_url=base_url.rstrip("/") + "/",
        transport=httpx.MockTransport(handler),
        follow_redirects=False,
        timeout=make_timeout(1.0, 1.0),
    )
    return ApiEndpoint(client, lambda: shoko_headers(FAKE_KEY))


class ShokoStub:
    """Programmable offline Shoko v5.3.3 API with a request log. Zero write paths."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.pages: dict[int, Any] = {}
        self.folders: Any = []
        self.file_status = 200
        self.folders_status = 200
        self.transport_error: Exception | None = None
        self.fail_pages: set[int] = set()

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        if path == FOLDER_PATH:
            return httpx.Response(self.folders_status, json=self.folders, request=request)
        if path == FILE_PATH:
            page = int(request.url.params.get("page", "1"))
            if self.transport_error is not None and page in self.fail_pages:
                raise self.transport_error
            payload = self.pages.get(page, {"Total": 0, "List": []})
            return httpx.Response(self.file_status, json=payload, request=request)
        return httpx.Response(404, json={"message": "unknown path"}, request=request)

    def methods(self) -> list[str]:
        return [request.method for request in self.requests]

    def paths(self) -> list[str]:
        return [request.url.path for request in self.requests]

    def file_requests(self) -> list[httpx.Request]:
        return [request for request in self.requests if request.url.path == FILE_PATH]


def shoko_settings(
    base_document: dict[str, Any],
    write_document: Writer,
    *,
    roots: list[str] | None = None,
    mappings: list[dict[str, str]] | None = None,
) -> Settings:
    document = copy.deepcopy(base_document)
    document["shoko"]["allowed_roots"] = list(roots or [SHOKO_ROOT])
    document["paths"]["mappings"] = list(mappings or [MAPPING])
    return load_settings(write_document(document))


def folder(id_: int, name: str, path: str, drop_type: str = "None") -> dict[str, Any]:
    return {
        "ID": id_,
        "Name": name,
        "Path": path,
        "WatchForNewFiles": True,
        "DropFolderType": drop_type,
        "Size": 10,
        "FileSize": 100,
    }


def location(
    place_id: int,
    file_id: int,
    folder_id: int,
    relative: str,
    absolute: str | None,
    accessible: bool = True,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "ID": place_id,
        "FileID": file_id,
        "ImportFolderID": folder_id,
        "RelativePath": relative,
        "IsAccessible": accessible,
    }
    if absolute is not None:
        entry["AbsolutePath"] = absolute
    return entry


def shoko_file(
    id_: int,
    size: int,
    locations: list[dict[str, Any]],
    *,
    xrefs: list[dict[str, Any]] | None = None,
    variation: bool = False,
    imported: str | None = "2024-01-01T00:00:00",
    ignored: bool = False,
    hashes: dict[str, Any] | None = None,
) -> dict[str, Any]:
    hash_values = hf.full_hashes(id_, size) if hashes is None else hashes
    return {
        "ID": id_,
        "Size": size,
        "IsIgnored": ignored,
        "IsVariation": variation,
        "Imported": imported,
        "Hashes": hash_values,
        "Locations": locations,
        "SeriesIDs": [] if xrefs is None else xrefs,
    }


def envelope(rows: list[dict[str, Any]], total: int) -> dict[str, Any]:
    return {"Total": total, "List": rows}


def sonarr_files(*specs: tuple[int, int, str, int]) -> tuple[SonarrFile, ...]:
    return tuple(
        SonarrFile(series_id=series, episode_file_id=file_id, path=path, size=size, episodes=())
        for series, file_id, path, size in specs
    )


def discover(stub: ShokoStub, **kwargs: Any) -> ShokoDiscovery:
    endpoint = shoko_endpoint(stub.handler)
    try:
        return collect_shoko_discovery(endpoint, **kwargs)
    finally:
        endpoint.client.close()


def reconcile(
    discovery: ShokoDiscovery,
    files: tuple[SonarrFile, ...],
    settings: Settings,
) -> ShokoMatchResult:
    return match_shoko_files(discovery, files, True, settings)


def single_match_stub(
    shoko_id: int = 500,
    shoko_size: int = 1000,
    shoko_path: str = f"{SHOKO_ROOT}/Show A/S01E01.mkv",
    sonarr_size: int | None = None,
) -> tuple[ShokoStub, tuple[SonarrFile, ...]]:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    relative = shoko_path.removeprefix(f"{SHOKO_ROOT}/")
    stub.pages = {
        1: envelope(
            [shoko_file(shoko_id, shoko_size, [location(900, shoko_id, 1, relative, shoko_path)])],
            1,
        )
    }
    size = shoko_size if sonarr_size is None else sonarr_size
    files = sonarr_files((7, 40, f"{SONARR_ROOT}/Show A/S01E01.mkv", size))
    return stub, files


# ------------------------------------------------------------------- pagination


def test_stable_pagination_walks_all_pages_and_uses_contract_params(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    rows = [
        shoko_file(
            id_,
            1000 + id_,
            [location(1000 + id_, id_, 1, f"S{id_}/x.mkv", f"{SHOKO_ROOT}/S{id_}/x.mkv")],
        )
        for id_ in range(1, 6)
    ]
    stub.pages = {
        1: envelope(rows[0:2], SMALL_TOTAL),
        2: envelope(rows[2:4], SMALL_TOTAL),
        3: envelope(rows[4:5], SMALL_TOTAL),
    }
    discovery = discover(stub, page_size=SMALL_PAGE_SIZE)
    assert discovery.verdict.complete is True
    assert discovery.total_reported == SMALL_TOTAL
    assert len(discovery.files) == SMALL_TOTAL
    assert discovery.pages_fetched == SMALL_PAGES
    assert discovery.page_size == SMALL_PAGE_SIZE
    pages = [request.url.params.get("page") for request in stub.file_requests()]
    assert pages == ["1", "2", "3"]
    assert {request.url.params.get("pageSize") for request in stub.file_requests()} == {"2"}
    assert len(stub.file_requests()) == SMALL_PAGES
    assert {request.url.params.get("include_only") for request in stub.file_requests()} == {
        "Unrecognized"
    }
    assert {request.url.params.get("include") for request in stub.file_requests()} == {
        "XRefs,AbsolutePaths"
    }


def test_total_change_between_pages_marks_snapshot_unstable(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    first = shoko_file(1, 10, [location(11, 1, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv")])
    second = shoko_file(2, 20, [location(12, 2, 1, "b.mkv", f"{SHOKO_ROOT}/b.mkv")])
    third = shoko_file(3, 30, [location(13, 3, 1, "c.mkv", f"{SHOKO_ROOT}/c.mkv")])
    stub.pages = {1: envelope([first, second], 4), 2: envelope([third], 6)}
    discovery = discover(stub, page_size=2)
    assert discovery.verdict.complete is False
    assert SnapshotReason.TOTAL_CHANGED in discovery.verdict.reasons
    assert any("Total changed from 4 to 6" in detail for detail in discovery.verdict.details)
    result = reconcile(discovery, (), shoko_settings(base_document, write_document))
    assert result.eligible is False
    assert result.matches == ()


def test_duplicate_file_ids_across_pages_keep_one_copy_and_flag_snapshot(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    first = shoko_file(1, 10, [location(11, 1, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv")])
    dupe = shoko_file(1, 11, [location(12, 1, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv")])
    second = shoko_file(2, 20, [location(13, 2, 1, "b.mkv", f"{SHOKO_ROOT}/b.mkv")])
    stub.pages = {1: envelope([first], 2), 2: envelope([dupe, second], 2)}
    discovery = discover(stub, page_size=1)
    assert SnapshotReason.DUPLICATE_FILE_IDS in discovery.verdict.reasons
    assert discovery.verdict.complete is False
    assert [item.id for item in discovery.files] == [1, 2]


def test_short_page_with_missing_tail_is_pages_exhausted() -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    rows = [
        shoko_file(id_, 10 * id_, [location(id_, id_, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv")])
        for id_ in (1, 2, 3)
    ]
    stub.pages = {1: envelope(rows, 5), 2: envelope([], 5)}
    discovery = discover(stub, page_size=5)
    assert discovery.verdict.complete is False
    assert SnapshotReason.PAGES_EXHAUSTED in discovery.verdict.reasons
    assert any("missing ids" in detail for detail in discovery.verdict.details)


def test_transport_failure_mid_scan_marks_incomplete() -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    first = shoko_file(1, 10, [location(11, 1, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv")])
    stub.pages = {1: envelope([first], 4)}
    stub.transport_error = httpx.ConnectError("connection refused")
    stub.fail_pages = {2}
    discovery = discover(stub, page_size=2)
    assert discovery.verdict.complete is False
    assert SnapshotReason.TRANSPORT_FAILURE in discovery.verdict.reasons
    assert len(discovery.files) == 1


def test_non_success_status_marks_incomplete() -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    stub.pages = {1: envelope([], 1)}
    stub.file_status = 500
    discovery = discover(stub)
    assert SnapshotReason.TRANSPORT_FAILURE in discovery.verdict.reasons
    assert discovery.verdict.complete is False


def test_max_pages_guard_stops_unbounded_paging() -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    for page in range(1, 6):
        rows = [
            shoko_file(
                (page - 1) * 2 + i,
                10 + i,
                [
                    location(
                        (page - 1) * 2 + i, (page - 1) * 2 + i, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv"
                    )
                ],
            )
            for i in (1, 2)
        ]
        stub.pages[page] = envelope(rows, 10000)
    discovery = discover(stub, page_size=SMALL_PAGE_SIZE, max_pages=PAGE_CAP)
    assert SnapshotReason.MAX_PAGES_EXCEEDED in discovery.verdict.reasons
    assert discovery.verdict.complete is False
    assert discovery.pages_fetched == PAGE_CAP
    assert len(discovery.files) == CAPPED_FILES


def test_oversized_page_size_is_clamped_to_the_server_maximum() -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    stub.pages = {1: envelope([], 0)}
    discovery = discover(stub, page_size=5000)
    assert discovery.page_size == PAGE_SIZE
    assert any("clamped to 1000" in detail for detail in discovery.verdict.details)
    assert discovery.verdict.complete is True


def test_page_size_zero_unlimited_is_refused_client_side() -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    stub.pages = {1: envelope([], 0)}
    discovery = discover(stub, page_size=0)
    assert discovery.page_size == PAGE_SIZE
    assert any("pageSize 0" in detail for detail in discovery.verdict.details)


@pytest.mark.parametrize(
    "payload",
    [
        {"Total": 1, "List": [], "Page": 1},
        {"Total": 1},
        {"List": []},
        [],
        {"Total": 1, "List": {"ID": 1}},
        {"Total": "1", "List": []},
        {"Total": -3, "List": []},
    ],
)
def test_malformed_list_result_envelope_marks_incomplete(payload: Any) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    stub.pages = {1: payload}
    discovery = discover(stub)
    assert SnapshotReason.MALFORMED_ENVELOPE in discovery.verdict.reasons
    assert discovery.verdict.complete is False


def test_malformed_file_entry_fails_closed() -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    entry = shoko_file(1, 10, [location(11, 1, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv")])
    del entry["Size"]
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    assert SnapshotReason.MALFORMED_FILE in discovery.verdict.reasons
    assert discovery.verdict.complete is False
    assert discovery.files == ()


def test_location_without_absolute_path_is_incomplete_snapshot() -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    entry = shoko_file(1, 10, [location(11, 1, 1, "a.mkv", None)])
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    assert discovery.verdict.complete is False
    assert SnapshotReason.ABSOLUTE_PATHS_MISSING in discovery.verdict.reasons
    assert any("include=AbsolutePaths" in detail for detail in discovery.verdict.details)


def test_missing_series_ids_is_incomplete_snapshot() -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    entry = shoko_file(1, 10, [location(11, 1, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv")])
    del entry["SeriesIDs"]
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    assert SnapshotReason.XREFS_MISSING in discovery.verdict.reasons
    assert discovery.verdict.complete is False


def test_import_folder_listing_failure_marks_incomplete() -> None:
    stub = ShokoStub()
    stub.folders = []
    stub.folders_status = 401
    stub.pages = {1: envelope([], 0)}
    discovery = discover(stub)
    assert discovery.verdict.complete is False
    assert SnapshotReason.TRANSPORT_FAILURE in discovery.verdict.reasons


def test_import_folder_listing_shape_failure_marks_incomplete() -> None:
    stub = ShokoStub()
    stub.folders = {"Total": 1}
    stub.pages = {1: envelope([], 0)}
    discovery = discover(stub)
    assert SnapshotReason.MALFORMED_ENVELOPE in discovery.verdict.reasons


def test_malformed_import_folder_entry_marks_incomplete() -> None:
    stub = ShokoStub()
    entry = folder(1, "Sonarr", SHOKO_ROOT)
    del entry["DropFolderType"]
    stub.folders = [entry]
    stub.pages = {1: envelope([], 1)}
    discovery = discover(stub)
    assert SnapshotReason.MALFORMED_IMPORT_FOLDER in discovery.verdict.reasons
    assert discovery.verdict.complete is False


def test_import_folder_listing_preserves_shape() -> None:
    folders, reasons, details = parse_import_folders(
        [folder(3, "Sonarr", "/anime"), folder(4, "Drop", "/shoko-drop", "Source")]
    )
    assert reasons == []
    assert folders[3] == ImportFolder(3, "Sonarr", "/anime", "None", True)
    assert folders[4].drop_folder_type == "Source"
    assert details == []


def test_unknown_drop_folder_type_is_not_none() -> None:
    folders, reasons, details = parse_import_folders([folder(5, "Weird", "/anime", "Manual")])
    assert reasons == []
    assert folders[5].drop_folder_type == "Unknown"
    assert any("unknown DropFolderType" in detail for detail in details)


# ------------------------------------------------------------------- ownership


def test_multi_location_mixed_ownership_drops_the_whole_file(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [
        folder(1, "Sonarr", SHOKO_ROOT, "None"),
        folder(2, "Manual drop", "/shoko-manual", "Source"),
    ]
    entry = shoko_file(
        77,
        1000,
        [
            location(701, 77, 1, "Show A/S01E01.mkv", f"{SHOKO_ROOT}/Show A/S01E01.mkv"),
            location(702, 77, 2, "S01E01.mkv", "/shoko-manual/S01E01.mkv"),
        ],
    )
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    assert discovery.verdict.complete is True
    result = reconcile(discovery, (), shoko_settings(base_document, write_document))
    assert result.matches == ()
    dropped = [(item.shoko_file_id, item.reason) for item in result.exclusions]
    assert dropped == [(77, ExclusionReason.MIXED_OWNERSHIP)]
    # Request log proves the run performed only GETs: zero Shoko writes.
    assert stub.methods() == ["GET", "GET"]


def test_location_relative_join_mismatch_rejects_the_file(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    entry = shoko_file(
        80, 1000, [location(800, 80, 1, "Show A/S01E01.mkv", f"{SHOKO_ROOT}/other/S01E01.mkv")]
    )
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    result = reconcile(discovery, (), shoko_settings(base_document, write_document))
    assert [(item.shoko_file_id, item.reason) for item in result.exclusions] == [
        (80, ExclusionReason.LOCATION_PATH_MISMATCH)
    ]


def test_relative_path_with_escape_is_rejected(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    entry = shoko_file(81, 1000, [location(810, 81, 1, "../secret/x.mkv", f"{SHOKO_ROOT}/x.mkv")])
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    assert discovery.verdict.complete is True
    result = reconcile(discovery, (), shoko_settings(base_document, write_document))
    dropped = [(item.shoko_file_id, item.reason) for item in result.exclusions]
    assert dropped == [(81, ExclusionReason.LOCATION_PATH_MISMATCH)]
    assert any("RelativePath must not contain" in item.detail for item in result.exclusions)


def test_inaccessible_location_drops_the_file(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    entry = shoko_file(
        82,
        1000,
        [location(820, 82, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv", accessible=False)],
    )
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    result = reconcile(discovery, (), shoko_settings(base_document, write_document))
    assert [(item.shoko_file_id, item.reason) for item in result.exclusions] == [
        (82, ExclusionReason.INACCESSIBLE_LOCATION)
    ]


def test_unknown_import_folder_drops_the_file(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    entry = shoko_file(83, 1000, [location(830, 83, 99, "a.mkv", f"{SHOKO_ROOT}/a.mkv")])
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    result = reconcile(discovery, (), shoko_settings(base_document, write_document))
    assert [(item.shoko_file_id, item.reason) for item in result.exclusions] == [
        (83, ExclusionReason.UNKNOWN_IMPORT_FOLDER)
    ]


def test_unusable_import_folder_path_fails_ownership(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Escape", "/anime/../secret")]
    entry = shoko_file(84, 1000, [location(840, 84, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv")])
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    assert discovery.verdict.complete is True
    result = reconcile(discovery, (), shoko_settings(base_document, write_document))
    assert [(item.shoko_file_id, item.reason) for item in result.exclusions] == [
        (84, ExclusionReason.UNKNOWN_IMPORT_FOLDER)
    ]


def test_location_outside_allowed_roots_is_rejected(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Other", "/shoko-other")]
    entry = shoko_file(85, 1000, [location(850, 85, 1, "a.mkv", "/shoko-other/a.mkv")])
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    result = reconcile(discovery, (), shoko_settings(base_document, write_document))
    assert [(item.shoko_file_id, item.reason) for item in result.exclusions] == [
        (85, ExclusionReason.OUTSIDE_ALLOWED_ROOT)
    ]


def test_sibling_prefix_root_is_not_contained(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sibling", "/anime-old")]
    entry = shoko_file(86, 1000, [location(860, 86, 1, "a.mkv", "/anime-old/a.mkv")])
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    result = reconcile(discovery, (), shoko_settings(base_document, write_document))
    assert [(item.shoko_file_id, item.reason) for item in result.exclusions] == [
        (86, ExclusionReason.OUTSIDE_ALLOWED_ROOT)
    ]


def test_all_locations_must_be_drop_folder_none(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [
        folder(1, "Primary", SHOKO_ROOT, "Destination"),
        folder(2, "Second", "/anime2", "None"),
    ]
    entry = shoko_file(
        87,
        1000,
        [
            location(870, 87, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv"),
            location(871, 87, 2, "a.mkv", "/anime2/a.mkv"),
        ],
    )
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    result = reconcile(discovery, (), shoko_settings(base_document, write_document))
    assert [(item.shoko_file_id, item.reason) for item in result.exclusions] == [
        (87, ExclusionReason.DROP_FOLDER_TYPE)
    ]


def test_unmapped_shoko_location_is_refused(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Outside", "/outside")]
    entry = shoko_file(88, 1000, [location(880, 88, 1, "a.mkv", "/outside/a.mkv")])
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    settings = shoko_settings(base_document, write_document, roots=[SHOKO_ROOT, "/outside"])
    result = reconcile(discovery, (), settings)
    assert [(item.shoko_file_id, item.reason) for item in result.exclusions] == [
        (88, ExclusionReason.UNMAPPED_LOCATION)
    ]


def test_nested_reverse_mapping_tie_is_ambiguity_not_a_guess(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    entry = shoko_file(89, 1000, [location(890, 89, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv")])
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    settings = shoko_settings(
        base_document,
        write_document,
        mappings=[
            {"sonarr": SONARR_ROOT, "shoko": SHOKO_ROOT},
            {"sonarr": "/data/media/tv", "shoko": SHOKO_ROOT},
        ],
    )
    result = reconcile(discovery, (), settings)
    assert [(item.shoko_file_id, item.reason) for item in result.exclusions] == [
        (89, ExclusionReason.AMBIGUOUS_MAPPING)
    ]
    assert any("ambiguous Shoko path mappings" in item.detail for item in result.exclusions)


def test_more_specific_reverse_mapping_wins(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    entry = shoko_file(90, 1000, [location(900, 90, 1, "deep/x.mkv", "/anime/deep/x.mkv")])
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    settings = shoko_settings(
        base_document,
        write_document,
        roots=[SHOKO_ROOT, "/anime/deep"],
        mappings=[
            {"sonarr": SONARR_ROOT, "shoko": SHOKO_ROOT},
            {"sonarr": "/data/media/tv", "shoko": "/anime/deep"},
        ],
    )
    files = sonarr_files((3, 30, "/data/media/tv/x.mkv", 1000))
    result = reconcile(discovery, files, settings)
    assert [(match.shoko_file_id, match.sonarr_file.path) for match in result.matches] == [
        (90, "/data/media/tv/x.mkv")
    ]


def test_reverse_mapping_helpers_are_component_exact() -> None:
    mappings = [PathMapping(SONARR_ROOT, SHOKO_ROOT)]
    assert select_reverse_mapping("/anime/Show/x.mkv", mappings) == mappings[0]
    assert select_reverse_mapping("/anime-old/x.mkv", mappings) is None
    assert translate_to_sonarr("/anime/Show/x.mkv", mappings[0]) == f"{SONARR_ROOT}/Show/x.mkv"
    with pytest.raises(PathError, match="not inside mapping target"):
        translate_to_sonarr("/anime-old/x.mkv", mappings[0])


def test_join_location_path_requires_relative_posix_paths() -> None:
    assert join_location_path("/anime", "Show A/x.mkv") == "/anime/Show A/x.mkv"
    for relative in ("", "/abs/x.mkv", "..\\x.mkv", "../x.mkv"):
        with pytest.raises(PathError):
            join_location_path("/anime", relative)


# ------------------------------------------------------------------ exclusions


def test_variation_and_xref_files_are_excluded_and_snapshot_stays_complete(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    variation = shoko_file(
        101,
        1000,
        [location(1010, 101, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv")],
        variation=True,
    )
    linked = shoko_file(
        102,
        1000,
        [location(1020, 102, 1, "b.mkv", f"{SHOKO_ROOT}/b.mkv")],
        xrefs=[{"SeriesID": {"ID": 9, "AniDB": 1721, "TMDB": None}, "EpisodeIDs": []}],
    )
    clean = shoko_file(103, 1000, [location(1030, 103, 1, "c.mkv", f"{SHOKO_ROOT}/c.mkv")])
    stub.pages = {1: envelope([variation, linked, clean], 3)}
    discovery = discover(stub)
    assert discovery.verdict.complete is True
    result = reconcile(discovery, (), shoko_settings(base_document, write_document))
    reasons = {item.shoko_file_id: item.reason for item in result.exclusions}
    assert reasons[101] is ExclusionReason.VARIATION
    assert reasons[102] is ExclusionReason.XREF_PRESENT
    assert reasons[103] is ExclusionReason.NOT_IN_INVENTORY


def test_ignored_files_are_never_candidates(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    entry = shoko_file(
        120, 1000, [location(1200, 120, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv")], ignored=True
    )
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    assert discovery.verdict.complete is True
    files = sonarr_files((7, 40, f"{SONARR_ROOT}/a.mkv", 1000))
    result = reconcile(discovery, files, shoko_settings(base_document, write_document))
    assert result.matches == ()
    assert [(item.shoko_file_id, item.reason) for item in result.exclusions] == [
        (120, ExclusionReason.IGNORED)
    ]


def test_fully_hashed_imported_null_file_is_a_candidate(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    # Shoko sets DateTimeImported only after AniDB identification, so a fully hashed
    # file with Imported=null stays a valid candidate forever. Imported is diagnostic
    # only; candidacy must not depend on it.
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    entry = shoko_file(
        110, 1000, [location(1100, 110, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv")], imported=None
    )
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    assert discovery.verdict.complete is True
    assert discovery.files[0].imported is None
    files = sonarr_files((7, 40, f"{SONARR_ROOT}/a.mkv", 1000))
    result = reconcile(discovery, files, shoko_settings(base_document, write_document))
    assert [item.reason for item in result.exclusions] == []
    assert [match.shoko_file_id for match in result.matches] == [110]


def test_file_without_available_hashes_is_excluded(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    # Eligibility requires at least one available hash: an all-null Hashes object is
    # identity-not-settled (hashing still in flight), never a link candidate.
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    entry = shoko_file(
        112,
        1000,
        [location(1120, 112, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv")],
        imported=None,
        hashes={"ED2K": None, "SHA1": None, "CRC32": None, "MD5": None},
    )
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    assert discovery.verdict.complete is True
    files = sonarr_files((7, 40, f"{SONARR_ROOT}/a.mkv", 1000))
    result = reconcile(discovery, files, shoko_settings(base_document, write_document))
    assert [(item.shoko_file_id, item.reason) for item in result.exclusions] == [
        (112, ExclusionReason.NO_HASHES)
    ]
    assert result.matches == ()


def hash_shape(
    ed2k: Any,
    *,
    sha1: Any = None,
    crc32: Any = None,
    md5: Any = None,
) -> dict[str, Any]:
    """A wire ``Hashes`` object with the given (deliberately unsettled) shapes."""
    return {"ED2K": ed2k, "SHA1": sha1, "CRC32": crc32, "MD5": md5}


UNSETTLED_HASH_IDENTITIES: list[tuple[str, dict[str, Any]]] = [
    ("empty ED2K while Shoko is still hashing the new file", hash_shape("")),
    ("empty ED2K with a populated SHA1", hash_shape("", sha1=hf.hex40(112), crc32=hf.hex8(112))),
    ("whitespace-only ED2K", hash_shape("   ")),
    ("placeholder-shaped ED2K", hash_shape("ed2k-112", crc32=hf.hex8(112))),
    ("garbage ED2K", hash_shape("not-a-hash")),
    ("numeric ED2K", hash_shape(112, crc32=hf.hex8(112))),
    ("truncated ED2K (31 hex chars)", hash_shape(VALID_ED2K[:-1])),
    ("oversized ED2K (33 hex chars)", hash_shape(VALID_ED2K + "a")),
    ("non-hexadecimal ED2K (32 chars)", hash_shape("z" * 32)),
    ("ED2K with surrounding non-hex", hash_shape(f" {VALID_ED2K[:-1]} ")),
    ("missing ED2K key", {"SHA1": None, "CRC32": None, "MD5": None}),
    ("all-null Hashes object", hash_shape(None)),
    ("empty SHA1 string", hash_shape(VALID_ED2K, sha1="")),
    ("whitespace-only MD5 string", hash_shape(VALID_ED2K, md5=" ")),
    ("short SHA1 string (39 hex chars)", hash_shape(VALID_ED2K, sha1="a" * 39)),
    ("oversized SHA1 string (41 hex chars)", hash_shape(VALID_ED2K, sha1="a" * 41)),
    ("non-hex MD5 string", hash_shape(VALID_ED2K, md5="z" * 32)),
    ("numeric SHA1", hash_shape(VALID_ED2K, sha1=112)),
    ("empty CRC32 string", hash_shape(VALID_ED2K, crc32="")),
    ("short CRC32 string (6 hex chars)", hash_shape(VALID_ED2K, crc32="ABC123")),
    ("oversized CRC32 string (9 hex chars)", hash_shape(VALID_ED2K, crc32="ABCDEF1234")),
    ("non-hex CRC32 string", hash_shape(VALID_ED2K, crc32="ZZZZZZZZ")),
    ("numeric CRC32", hash_shape(VALID_ED2K, crc32=112)),
]


@pytest.mark.parametrize(
    ("label", "hashes"),
    UNSETTLED_HASH_IDENTITIES,
    ids=[label for label, _ in UNSETTLED_HASH_IDENTITIES],
)
def test_unsettled_hash_identity_is_excluded_as_no_hashes(
    base_document: dict[str, Any],
    write_document: Writer,
    label: str,
    hashes: dict[str, Any],
) -> None:
    # Shoko 5.3.3 exposes an empty ED2K string while a new file record is still being
    # hashed: only a complete, structurally valid ED2K identity (with every populated
    # field plausible) settles candidacy. Empty/whitespace/malformed/partial shapes are
    # NO_HASHES exclusions, and the snapshot stays complete for them.
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    entry = shoko_file(
        112,
        1000,
        [location(1120, 112, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv")],
        imported=None,
        hashes=hashes,
    )
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    assert discovery.verdict.complete is True
    files = sonarr_files((7, 40, f"{SONARR_ROOT}/a.mkv", 1000))
    result = reconcile(discovery, files, shoko_settings(base_document, write_document))
    assert [(item.shoko_file_id, item.reason) for item in result.exclusions] == [
        (112, ExclusionReason.NO_HASHES)
    ]
    assert result.matches == ()


def test_ed2k_only_candidate_with_null_optional_hashes_is_a_candidate(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    # A complete 32-hex ED2K settles the identity even when Shoko reports SHA1/CRC32/MD5
    # as null (uncomputed optional hashes are permitted), Imported=null included.
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    entry = shoko_file(
        113,
        1000,
        [location(1130, 113, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv")],
        imported=None,
        hashes=hf.ed2k_hashes(113, 1000, "a.mkv"),
    )
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    assert discovery.verdict.complete is True
    files = sonarr_files((7, 40, f"{SONARR_ROOT}/a.mkv", 1000))
    result = reconcile(discovery, files, shoko_settings(base_document, write_document))
    assert [item.reason for item in result.exclusions] == []
    assert [match.shoko_file_id for match in result.matches] == [113]


def test_full_valid_hash_set_without_xrefs_is_a_candidate(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    # Raw-hex wire set: 32-hex ED2K + 40-hex SHA1 + 8-hex CRC32 + 32-hex MD5, zero
    # cross-references: eligible.
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    entry = shoko_file(
        114,
        1000,
        [location(1140, 114, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv")],
        hashes=hf.full_hashes(114, 1000, "a.mkv"),
    )
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    assert discovery.verdict.complete is True
    files = sonarr_files((7, 40, f"{SONARR_ROOT}/a.mkv", 1000))
    result = reconcile(discovery, files, shoko_settings(base_document, write_document))
    assert [item.reason for item in result.exclusions] == []
    assert [match.shoko_file_id for match in result.matches] == [114]


def test_lowercase_ed2k_is_a_candidate(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    # Validation is case-insensitive: Shoko stores ED2K uppercase, but a 32-hex value in
    # lowercase is accepted (the raw value is preserved, not case-normalized).
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    entry = shoko_file(
        115,
        1000,
        [location(1150, 115, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv")],
        imported=None,
        hashes=hash_shape(hf.hex32(115).lower()),
    )
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    assert discovery.verdict.complete is True
    files = sonarr_files((7, 40, f"{SONARR_ROOT}/a.mkv", 1000))
    result = reconcile(discovery, files, shoko_settings(base_document, write_document))
    assert [item.reason for item in result.exclusions] == []
    assert [match.shoko_file_id for match in result.matches] == [115]
    assert result.matches[0].hashes.ed2k == hf.hex32(115).lower()


def test_file_without_locations_is_excluded(
    base_document: dict[str, Any],
    write_document: Writer,
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    stub.pages = {1: envelope([shoko_file(111, 1000, [])], 1)}
    discovery = discover(stub)
    result = reconcile(discovery, (), shoko_settings(base_document, write_document))
    assert [(item.shoko_file_id, item.reason) for item in result.exclusions] == [
        (111, ExclusionReason.NO_LOCATIONS)
    ]


# ---------------------------------------------------------------- correspondence


def test_same_basename_in_different_directory_never_matches(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub, files = single_match_stub(shoko_path=f"{SHOKO_ROOT}/Show B/S01E01.mkv")
    discovery = discover(stub)
    result = reconcile(discovery, files, shoko_settings(base_document, write_document))
    assert result.matches == ()
    assert [(item.shoko_file_id, item.reason) for item in result.exclusions] == [
        (500, ExclusionReason.NOT_IN_INVENTORY)
    ]


def test_same_path_with_different_size_is_a_size_mismatch(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub, files = single_match_stub(shoko_size=1001, sonarr_size=1000)
    discovery = discover(stub)
    result = reconcile(discovery, files, shoko_settings(base_document, write_document))
    assert result.matches == ()
    assert [(item.shoko_file_id, item.reason) for item in result.exclusions] == [
        (500, ExclusionReason.SIZE_MISMATCH)
    ]


def test_exact_path_equality_picks_one_of_two_same_path_sonarr_files(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    entry = shoko_file(
        200, 1500, [location(2000, 200, 1, "Show A/S01E01.mkv", f"{SHOKO_ROOT}/Show A/S01E01.mkv")]
    )
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    files = sonarr_files(
        (7, 40, f"{SONARR_ROOT}/Show A/S01E01.mkv", 1000),
        (7, 41, f"{SONARR_ROOT}/Show A/S01E01.mkv", 1500),
    )
    result = reconcile(discovery, files, shoko_settings(base_document, write_document))
    assert [match.sonarr_file.episode_file_id for match in result.matches] == [41]
    assert result.warnings == ()


def test_shoko_copy_at_an_unknown_path_is_not_in_inventory(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    first = shoko_file(
        300, 1000, [location(3000, 300, 1, "Show A/S01E01.mkv", f"{SHOKO_ROOT}/Show A/S01E01.mkv")]
    )
    second = shoko_file(
        301, 1000, [location(3001, 301, 1, "Copy/S01E01.mkv", f"{SHOKO_ROOT}/Copy/S01E01.mkv")]
    )
    stub.pages = {1: envelope([first, second], 2)}
    discovery = discover(stub)
    files = sonarr_files((7, 40, f"{SONARR_ROOT}/Show A/S01E01.mkv", 1000))
    result = reconcile(
        discovery, files, shoko_settings(base_document, write_document, roots=[SHOKO_ROOT])
    )
    assert [match.shoko_file_id for match in result.matches] == [300]
    assert [(item.shoko_file_id, item.reason) for item in result.exclusions] == [
        (301, ExclusionReason.NOT_IN_INVENTORY)
    ]


def test_two_shoko_files_at_the_same_path_and_size_both_match_with_warning(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    first = shoko_file(
        310, 1000, [location(3100, 310, 1, "Show A/S01E01.mkv", f"{SHOKO_ROOT}/Show A/S01E01.mkv")]
    )
    second = shoko_file(
        311, 1000, [location(3101, 311, 1, "Show A/S01E01.mkv", f"{SHOKO_ROOT}/Show A/S01E01.mkv")]
    )
    stub.pages = {1: envelope([first, second], 2)}
    discovery = discover(stub)
    files = sonarr_files((7, 40, f"{SONARR_ROOT}/Show A/S01E01.mkv", 1000))
    result = reconcile(discovery, files, shoko_settings(base_document, write_document))
    assert [match.shoko_file_id for match in result.matches] == [310, 311]
    assert any("shoko_duplicate_size" in warning for warning in result.warnings)


def test_multi_location_matching_distinct_sonarr_files_is_ambiguous(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [
        folder(1, "Primary", SHOKO_ROOT),
        folder(2, "Mirror", "/anime-mirror"),
    ]
    entry = shoko_file(
        330,
        1000,
        [
            location(3300, 330, 1, "Show A/S01E01.mkv", f"{SHOKO_ROOT}/Show A/S01E01.mkv"),
            location(3301, 330, 2, "Show B/S01E02.mkv", "/anime-mirror/Show B/S01E02.mkv"),
        ],
    )
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    settings = shoko_settings(
        base_document,
        write_document,
        roots=[SHOKO_ROOT, "/anime-mirror"],
        mappings=[
            {"sonarr": SONARR_ROOT, "shoko": SHOKO_ROOT},
            {"sonarr": "/data/media/other", "shoko": "/anime-mirror"},
        ],
    )
    files = sonarr_files(
        (7, 40, f"{SONARR_ROOT}/Show A/S01E01.mkv", 1000),
        (8, 41, "/data/media/other/Show B/S01E02.mkv", 1000),
    )
    result = reconcile(discovery, files, settings)
    assert result.matches == ()
    assert [(item.shoko_file_id, item.reason) for item in result.exclusions] == [
        (330, ExclusionReason.AMBIGUOUS_MULTIPLE_SONARR_CANDIDATES)
    ]


def test_multi_location_with_a_path_outside_the_inventory_is_divergence(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [
        folder(1, "Primary", SHOKO_ROOT),
        folder(2, "Mirror", "/anime-mirror"),
    ]
    entry = shoko_file(
        335,
        1000,
        [
            location(3350, 335, 1, "Show A/x.mkv", f"{SHOKO_ROOT}/Show A/x.mkv"),
            location(3351, 335, 2, "Show A/x.mkv", "/anime-mirror/Show A/x.mkv"),
        ],
    )
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    settings = shoko_settings(
        base_document,
        write_document,
        roots=[SHOKO_ROOT, "/anime-mirror"],
        mappings=[
            {"sonarr": SONARR_ROOT, "shoko": SHOKO_ROOT},
            {"sonarr": "/data/media/mirror", "shoko": "/anime-mirror"},
        ],
    )
    files = sonarr_files((7, 40, f"{SONARR_ROOT}/Show A/x.mkv", 1000))
    result = reconcile(discovery, files, settings)
    assert result.matches == ()
    assert [(item.shoko_file_id, item.reason) for item in result.exclusions] == [
        (335, ExclusionReason.MULTI_LOCATION_DIVERGENCE)
    ]


def test_size_matching_at_only_some_locations_is_no_size_match(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [
        folder(1, "Primary", SHOKO_ROOT),
        folder(2, "Mirror", "/anime-mirror"),
    ]
    entry = shoko_file(
        340,
        1000,
        [
            location(3400, 340, 1, "Show A/x.mkv", f"{SHOKO_ROOT}/Show A/x.mkv"),
            location(3401, 340, 2, "Show A/x.mkv", "/anime-mirror/Show A/x.mkv"),
        ],
    )
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    settings = shoko_settings(
        base_document,
        write_document,
        roots=[SHOKO_ROOT, "/anime-mirror"],
        mappings=[
            {"sonarr": SONARR_ROOT, "shoko": SHOKO_ROOT},
            {"sonarr": "/data/media/mirror", "shoko": "/anime-mirror"},
        ],
    )
    files = sonarr_files(
        (7, 40, f"{SONARR_ROOT}/Show A/x.mkv", 1000),
        (9, 41, "/data/media/mirror/Show A/x.mkv", 2000),
    )
    result = reconcile(discovery, files, settings)
    assert result.matches == ()
    assert [(item.shoko_file_id, item.reason) for item in result.exclusions] == [
        (340, ExclusionReason.NO_SIZE_MATCH)
    ]


def test_ambiguous_size_match_across_sonarr_ids_is_refused(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    entry = shoko_file(
        350, 1000, [location(3500, 350, 1, "Show A/x.mkv", f"{SHOKO_ROOT}/Show A/x.mkv")]
    )
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    files = sonarr_files(
        (7, 40, f"{SONARR_ROOT}/Show A/x.mkv", 1000),
        (8, 41, f"{SONARR_ROOT}/Show A/x.mkv", 1000),
    )
    result = reconcile(discovery, files, shoko_settings(base_document, write_document))
    assert result.matches == ()
    assert [(item.shoko_file_id, item.reason) for item in result.exclusions] == [
        (350, ExclusionReason.AMBIGUOUS_MULTIPLE_SONARR_CANDIDATES)
    ]


def test_incomplete_snapshot_and_incomplete_inventory_refuse_reconciliation(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    settings = shoko_settings(base_document, write_document)
    stub, files = single_match_stub()
    discovery = discover(stub)
    assert discovery.verdict.complete is True
    incomplete = match_shoko_files(discovery, files, False, settings)
    assert incomplete.eligible is False
    assert incomplete.matches == ()
    assert any("Sonarr inventory incomplete" in warning for warning in incomplete.warnings)
    stub.pages = {1: {"Total": 1, "List": {}, "Page": 1}}
    broken = discover(stub)
    refused = match_shoko_files(broken, files, True, settings)
    assert refused.eligible is False
    assert any("snapshot incomplete" in warning for warning in refused.warnings)


# ------------------------------------------------------------------- identity


def identity(id_: int, size: int, paths: tuple[str, ...], folders: tuple[int, ...]) -> Any:
    item = ShokoFile(
        id=id_,
        size=size,
        is_ignored=False,
        is_variation=False,
        imported="2024-01-01T00:00:00",
        hashes=ShokoHashes(hf.hex32(id_), hf.hex40(id_), hf.hex8(id_), hf.hex32(id_)),
        xref_count=0,
        locations=tuple(
            ShokoLocation(place_id + 7000, id_, folder_id, "x.mkv", path, True)
            for place_id, folder_id, path in zip(range(len(paths)), folders, paths, strict=True)
        ),
    )
    return build_identity(item)


def test_identity_comparison_reports_each_change_category() -> None:
    base = identity(900, 1000, ("/data/media/anime/A/x.mkv",), (1,))
    size_changed = compare_identity(base, identity(900, 2000, ("/data/media/anime/A/x.mkv",), (1,)))
    assert size_changed.categories == (IdentityChange.SIZE_CHANGED,)
    assert size_changed.stable is False

    hash_base = build_identity(
        ShokoFile(
            id=900,
            size=1000,
            is_ignored=False,
            is_variation=False,
            imported=None,
            hashes=ShokoHashes(hf.hex32(1), hf.hex40(1), hf.hex8(1), hf.hex32(1)),
            xref_count=0,
            locations=(ShokoLocation(1, 900, 1, "x.mkv", "/anime/x.mkv", True),),
        )
    )
    hash_next = build_identity(
        ShokoFile(
            id=900,
            size=1000,
            is_ignored=False,
            is_variation=False,
            imported=None,
            hashes=ShokoHashes(hf.hex32(2), hf.hex40(1), hf.hex8(1), hf.hex32(1)),
            xref_count=0,
            locations=(ShokoLocation(1, 900, 1, "x.mkv", "/anime/x.mkv", True),),
        )
    )
    hashes = compare_identity(hash_base, hash_next)
    assert hashes.categories == (IdentityChange.HASH_CHANGED,)

    # Identity comparison is case-sensitive on the exact reported bytes: the same
    # digest reported uppercase vs lowercase is a change (values preserved,
    # never case-normalized).
    upper = build_identity(
        ShokoFile(
            id=900,
            size=1000,
            is_ignored=False,
            is_variation=False,
            imported=None,
            hashes=ShokoHashes(hf.hex32(0xABCDEF).upper(), None, None, None),
            xref_count=0,
            locations=(ShokoLocation(1, 900, 1, "x.mkv", "/anime/x.mkv", True),),
        )
    )
    lower = build_identity(
        ShokoFile(
            id=900,
            size=1000,
            is_ignored=False,
            is_variation=False,
            imported=None,
            hashes=ShokoHashes(hf.hex32(0xABCDEF).lower(), None, None, None),
            xref_count=0,
            locations=(ShokoLocation(1, 900, 1, "x.mkv", "/anime/x.mkv", True),),
        )
    )
    assert compare_identity(upper, lower).categories == (IdentityChange.HASH_CHANGED,)
    assert compare_identity(upper, upper).stable is True

    added = compare_identity(base, identity(900, 1000, ("/a/x.mkv", "/b/x.mkv"), (1, 2)))
    assert IdentityChange.LOCATIONS_ADDED in added.categories

    removed = compare_identity(identity(900, 1000, ("/a/x.mkv", "/b/x.mkv"), (1, 2)), base)
    assert removed.categories == (IdentityChange.LOCATIONS_REMOVED,)

    moved = compare_identity(base, identity(900, 1000, ("/data/media/anime/B/x.mkv",), (1,)))
    assert moved.categories == (IdentityChange.LOCATIONS_CHANGED,)

    stable = compare_identity(base, identity(900, 1000, ("/data/media/anime/A/x.mkv",), (1,)))
    assert stable.categories == ()
    assert stable.stable is True


def test_identity_uses_one_canonical_space_and_sorts_deterministically() -> None:
    item = ShokoFile(
        id=901,
        size=10,
        is_ignored=False,
        is_variation=False,
        imported=None,
        hashes=ShokoHashes(None, None, None, None),
        xref_count=0,
        locations=(
            ShokoLocation(2, 901, 2, "b.mkv", "/anime/b.mkv", True),
            ShokoLocation(1, 901, 1, "a.mkv", "/anime/a.mkv", True),
        ),
    )
    translated = build_identity(item, ["/data/media/anime/b.mkv", "/data/media/anime/a.mkv"])
    raw = build_identity(item)
    # Compared field stays in Shoko space regardless of translation state.
    assert translated.paths == ("/anime/a.mkv", "/anime/b.mkv")
    assert raw.paths == ("/anime/a.mkv", "/anime/b.mkv")
    assert translated.sonarr_paths == ("/data/media/anime/a.mkv", "/data/media/anime/b.mkv")
    assert raw.sonarr_paths == ()
    assert translated.folders == (1, 2)
    # An ownership flip (translation lost) must NOT report location drift.
    flip = compare_identity(translated, raw)
    assert flip.stable is True


def test_ownership_flip_across_observations_is_stable_identity(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    rows = [
        shoko_file(
            60,
            10,
            [location(600, 60, 1, "x.mkv", f"{SHOKO_ROOT}/x.mkv")],
        )
    ]
    stub.pages = {1: envelope(rows, 1)}
    settings = shoko_settings(base_document, write_document)
    first = reconcile(discover(stub), (), settings)
    # Same Shoko file, now in a drop folder: identity paths unchanged.
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT, drop_type="Source")]
    second = reconcile(discover(stub), (), settings)
    first_identity = first.identities[0]
    second_identity = second.identities[0]
    assert compare_identity(first_identity, second_identity).stable is True
    assert [e.reason for e in second.exclusions] == [ExclusionReason.DROP_FOLDER_TYPE]


# ------------------------------------------------------------- determinism, scale


def test_outputs_are_sorted_by_shoko_file_id(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    rows = [
        shoko_file(30, 10, [location(300, 30, 1, "z.mkv", f"{SHOKO_ROOT}/z.mkv")]),
        shoko_file(10, 10, [location(100, 10, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv")]),
        shoko_file(20, 10, [location(200, 20, 1, "m.mkv", f"{SHOKO_ROOT}/m.mkv")], variation=True),
    ]
    stub.pages = {1: envelope(rows, 3)}
    discovery = discover(stub)
    files = sonarr_files(
        (7, 70, f"{SONARR_ROOT}/a.mkv", 10),
        (7, 71, f"{SONARR_ROOT}/z.mkv", 10),
    )
    result = reconcile(discovery, files, shoko_settings(base_document, write_document))
    assert [match.shoko_file_id for match in result.matches] == [10, 30]
    assert [item.shoko_file_id for item in result.exclusions] == [20]
    assert [identity_.shoko_file_id for identity_ in result.identities] == [10, 20, 30]
    first = reconcile(discovery, files, shoko_settings(base_document, write_document))
    assert first.matches == result.matches
    assert first.exclusions == result.exclusions


def test_seven_thousand_files_reconcile_linearly_with_bounded_requests(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    for page in range(1, LARGE_PAGES + 1):
        start = (page - 1) * LARGE_PAGE_SIZE
        rows = [
            shoko_file(
                start + offset,
                1000 + start + offset,
                [
                    location(
                        50_000 + start + offset,
                        start + offset,
                        1,
                        f"Show {start + offset}/x.mkv",
                        f"{SHOKO_ROOT}/Show {start + offset}/x.mkv",
                    )
                ],
            )
            for offset in range(1, LARGE_PAGE_SIZE + 1)
        ]
        stub.pages[page] = envelope(rows, LARGE_TOTAL)
    inventory = sonarr_files(
        *(
            (1, file_id, f"{SONARR_ROOT}/Show {file_id}/x.mkv", 1000 + file_id)
            for file_id in range(1, LARGE_TOTAL + 1)
        )
    )
    settings = shoko_settings(base_document, write_document)
    started = time.perf_counter()
    discovery = discover(stub)
    result = reconcile(discovery, inventory, settings)
    elapsed = time.perf_counter() - started
    assert discovery.verdict.complete is True
    assert len(discovery.files) == LARGE_TOTAL
    assert len(result.matches) == LARGE_TOTAL
    assert result.warnings == ()
    assert elapsed < WALL_TIME_LIMIT_SECONDS
    # 7 File pages + 1 ImportFolder listing: the Sonarr side is never re-fetched
    # per Shoko file (it is a pre-built inventory), and Shoko writes are zero.
    assert len(stub.requests) == LARGE_PAGES + 1
    assert set(stub.methods()) == {"GET"}


def test_zero_writes_are_proved_by_the_request_log(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub, files = single_match_stub()
    discovery = discover(stub)
    result = reconcile(discovery, files, shoko_settings(base_document, write_document))
    assert len(result.matches) == 1
    assert set(stub.methods()) == {"GET"}
    assert stub.paths() == [FOLDER_PATH, FILE_PATH]
    assert all(request.url.params.get("apikey") is None for request in stub.requests)
    assert all(request.headers.get("apikey") == FAKE_KEY for request in stub.requests)


def test_no_api_key_material_is_recorded_in_results(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub, files = single_match_stub()
    discovery = discover(stub)
    result = reconcile(discovery, files, shoko_settings(base_document, write_document))
    rendered = repr(discovery) + repr(result)
    assert FAKE_KEY not in rendered
    assert FAKE_KEY not in str(result.warnings)
    assert FAKE_KEY not in str(result.identities)


def test_index_sonarr_files_groups_by_exact_path() -> None:
    files = sonarr_files(
        (1, 10, "/data/media/anime/A/x.mkv", 1),
        (1, 11, "/data/media/anime/A/x.mkv", 2),
        (2, 12, "/data/media/anime/B/y.mkv", 1),
    )
    index = index_sonarr_files(files)
    assert sorted(index) == ["/data/media/anime/A/x.mkv", "/data/media/anime/B/y.mkv"]
    assert [item.episode_file_id for item in index["/data/media/anime/A/x.mkv"]] == [10, 11]


def test_file_match_exposes_hash_values_as_received(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub, files = single_match_stub()
    discovery = discover(stub)
    result = reconcile(discovery, files, shoko_settings(base_document, write_document))
    match = result.matches[0]
    assert isinstance(match, FileMatch)
    assert match.hashes == ShokoHashes(hf.hex32(500), hf.hex40(500), hf.hex8(500), hf.hex32(500))
    assert match.shoko_paths == (f"{SONARR_ROOT}/Show A/S01E01.mkv",)


def test_hash_shape_oddities_fail_closed() -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    entry = shoko_file(
        400, 10, [location(4000, 400, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv")], hashes={"ED2K": ["x"]}
    )
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    assert SnapshotReason.MALFORMED_FILE in discovery.verdict.reasons


def test_absent_hashes_key_is_recorded_without_failing_the_page() -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    entry = shoko_file(401, 10, [location(4010, 401, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv")])
    del entry["Hashes"]
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    assert discovery.verdict.complete is True
    assert discovery.files[0].hashes == ShokoHashes(None, None, None, None)
    assert any("Hashes key absent" in detail for detail in discovery.verdict.details)


def test_non_string_imported_value_is_malformed_file() -> None:
    # Imported is diagnostic-only (null is eligible), but a present non-string/non-null
    # value is a malformed-file failure: the parse-level Imported TYPE check stays.
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    entry = shoko_file(403, 10, [location(4030, 403, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv")])
    entry["Imported"] = 12345
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    assert SnapshotReason.MALFORMED_FILE in discovery.verdict.reasons
    assert any("invalid Imported" in detail for detail in discovery.verdict.details)
    assert discovery.files == ()


def test_location_file_id_cross_reference_must_match_the_file() -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    entry = shoko_file(402, 10, [location(4020, 777, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv")])
    stub.pages = {1: envelope([entry], 1)}
    discovery = discover(stub)
    assert SnapshotReason.MALFORMED_FILE in discovery.verdict.reasons
    assert discovery.files == ()


def test_imported_null_files_are_never_excluded_as_never_imported(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    # Imported=null is diagnostic only, never a candidacy signal: these files clear the
    # candidacy gate and are then judged on correspondence (here an absent Sonarr
    # inventory), not on the Imported field.
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    rows = [
        shoko_file(
            500 + page,
            10,
            [location(6000 + page, 500 + page, 1, "a.mkv", f"{SHOKO_ROOT}/a.mkv")],
            imported=None,
        )
        for page in range(1, IMPORTED_NULL_PAGES + 1)
    ]
    stub.pages = {page: envelope([row], len(rows)) for page, row in enumerate(rows, start=1)}
    discovery = discover(stub, page_size=1)
    result = reconcile(discovery, (), shoko_settings(base_document, write_document))
    reasons = {item.reason for item in result.exclusions}
    assert reasons == {ExclusionReason.NOT_IN_INVENTORY}
    assert len(result.exclusions) == IMPORTED_NULL_PAGES


# ---------------------------------------------------- review-driven gap coverage


def test_page_overshoot_beyond_reported_total_is_contract_violation() -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    rows = [
        shoko_file(
            700 + n, 10, [location(7000 + n, 700 + n, 1, f"x{n}.mkv", f"{SHOKO_ROOT}/x{n}.mkv")]
        )
        for n in range(3)
    ]
    stub.pages = {1: envelope(rows, 2)}
    discovery = discover(stub)
    assert SnapshotReason.PAGE_OVERSHOOT in discovery.verdict.reasons
    assert discovery.verdict.complete is False


def test_case_sensitive_root_containment_is_enforced(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", "/Anime")]
    stub.pages = {
        1: envelope([shoko_file(710, 10, [location(7100, 710, 1, "x.mkv", "/Anime/x.mkv")])], 1)
    }
    result = reconcile(discover(stub), (), shoko_settings(base_document, write_document))
    assert [item.reason for item in result.exclusions] == [ExclusionReason.OUTSIDE_ALLOWED_ROOT]


def test_trailing_and_double_slash_forms_do_not_false_reject(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", f"{SHOKO_ROOT}/")]
    stub.pages = {
        1: envelope(
            [
                shoko_file(
                    SLASH_FORM_FILE_ID,
                    10,
                    [location(7200, SLASH_FORM_FILE_ID, 1, "x.mkv", f"{SHOKO_ROOT}//x.mkv")],
                )
            ],
            1,
        )
    }
    files = sonarr_files((7, 80, f"{SONARR_ROOT}/x.mkv", 10))
    result = reconcile(discover(stub), files, shoko_settings(base_document, write_document))
    assert len(result.matches) == 1
    assert result.matches[0].shoko_file_id == SLASH_FORM_FILE_ID


def test_null_absolute_path_is_an_invalid_location_not_a_guess(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    loc = location(7300, 730, 1, "x.mkv", None)
    loc["AbsolutePath"] = None
    stub.pages = {1: envelope([shoko_file(730, 10, [loc])], 1)}
    discovery = discover(stub)
    assert discovery.verdict.complete is True
    result = reconcile(discovery, (), shoko_settings(base_document, write_document))
    assert [item.reason for item in result.exclusions] == [ExclusionReason.INVALID_LOCATION_PATH]


def test_composite_drop_folder_type_string_refuses_ownership(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT, drop_type="Source, Destination")]
    stub.pages = {
        1: envelope(
            [shoko_file(740, 10, [location(7400, 740, 1, "x.mkv", f"{SHOKO_ROOT}/x.mkv")])], 1
        )
    }
    discovery = discover(stub)
    assert discovery.verdict.complete is True
    result = reconcile(discovery, (), shoko_settings(base_document, write_document))
    assert [item.reason for item in result.exclusions] == [ExclusionReason.DROP_FOLDER_TYPE]


def test_negative_page_size_is_clamped_to_the_maximum() -> None:
    stub = ShokoStub()
    stub.folders = []
    stub.pages = {1: envelope([], 0)}
    discovery = discover(stub, page_size=-5)
    assert discovery.page_size == PAGE_SIZE


def test_multi_episode_sonarr_file_passes_through_intact(
    base_document: dict[str, Any], write_document: Writer
) -> None:
    episodes = (
        SonarrEpisode(11, 7, 1, 1, "E01", None, None, 40),
        SonarrEpisode(12, 7, 1, 2, "E02", None, None, 40),
    )
    pair = SonarrFile(
        series_id=7,
        episode_file_id=40,
        path=f"{SONARR_ROOT}/Show A/S01E01-E02.mkv",
        size=1000,
        episodes=episodes,
    )
    stub = ShokoStub()
    stub.folders = [folder(1, "Sonarr", SHOKO_ROOT)]
    stub.pages = {
        1: envelope(
            [
                shoko_file(
                    750,
                    1000,
                    [
                        location(
                            7500,
                            750,
                            1,
                            "Show A/S01E01-E02.mkv",
                            f"{SHOKO_ROOT}/Show A/S01E01-E02.mkv",
                        )
                    ],
                )
            ],
            1,
        )
    }
    result = reconcile(discover(stub), (pair,), shoko_settings(base_document, write_document))
    assert len(result.matches) == 1
    assert tuple(e.id for e in result.matches[0].sonarr_file.episodes) == (11, 12)
