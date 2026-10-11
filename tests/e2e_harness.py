"""M9-e2e synthetic-world harness: production modules driven over a simulated dataset.

The world is a scripted Sonarr/Shoko/Anime-Lists trio behind ``httpx.MockTransport``
endpoints, a tmp-directory ``MappingStore``, a tmp-file ``StateStore``, an
operator-built attestation lease, and fake ISO/monotonic clocks with injected
advance/jitter. Nothing here touches the network; every Shoko write is a POST the
stub records verbatim.

Measurement method (psutil is not a project dependency): wall time via
``time.perf_counter`` and a Python-heap high-water mark via ``tracemalloc`` (an RSS
delta proxy; ``resource.getrusage(...).ru_maxrss`` is POSIX-only and unavailable on
this Windows dev box). Request accounting comes from per-transport logs; POST bodies
are captured verbatim.
"""

from __future__ import annotations

import hashlib
import json
import time
import tracemalloc
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import httpx
import yaml  # type: ignore[import-untyped]

from shoko_linker import cli
from shoko_linker.attestation import (
    SCOPE_DAEMON,
    AttestationVerdict,
    build_record,
    load_attestation,
    write_attestation,
)
from shoko_linker.config import Settings, parse_settings
from shoko_linker.daemon import DaemonDeps
from shoko_linker.http import ApiEndpoint, make_timeout
from shoko_linker.mapping_store import MappingStore
from shoko_linker.mapping_store import refresh as mapping_refresh
from shoko_linker.state import BudgetLimits, StateStore

SONARR_BASE = "http://sonarr:8989"
SHOKO_BASE = "http://shoko:8111"
SOURCE_BASE = "https://raw.githubusercontent.com"
SOURCE_PATH = "/Anime-Lists/anime-lists/master/anime-list-full.xml"
SOURCE_URL = SOURCE_BASE + SOURCE_PATH
SONARR_ROOT = "/data/media/anime"
LIVE_ROOT = "/data/media/live-action"
SHOKO_ROOT = "/anime"
OUTSIDE_ROOT = "/scratch"
LEASE_PATH = "/data/lease.yaml"
STATE_PATH = "/data/state.sqlite3"

BASE_ISO = "2026-01-01T00:00:00+00:00"
WEEK_SECONDS = 7 * 24 * 3600.0
SHOKO_PAGE_SIZE = 1000

DEFAULT_FLAGS: dict[str, bool] = {
    "FileQualityFilterEnabled": False,
    "RenameOnImport": False,
    "MoveOnImport": False,
    "MyListAddFiles": False,
}

WIDE_LIMITS = BudgetLimits(
    max_pending_files=50,
    max_pending_episode_jobs=100,
    per_cycle_new=50,
    rolling_new=200,
    rolling_window_hours=24.0,
)
CAMPAIGN_LIMITS = BudgetLimits(
    max_pending_files=120,
    max_pending_episode_jobs=30,
    per_cycle_new=120,
    rolling_new=200,
    rolling_window_hours=24.0,
)


def parse_iso(raw: str) -> datetime:
    parsed = datetime.fromisoformat(raw)
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def iso_plus(iso: str, seconds: float) -> str:
    return (parse_iso(iso) + timedelta(seconds=seconds)).isoformat()


def endpoint_for(handler: Callable[[httpx.Request], httpx.Response], base: str) -> ApiEndpoint:
    client = httpx.Client(
        base_url=base,
        transport=httpx.MockTransport(handler),
        follow_redirects=False,
        timeout=make_timeout(1.0, 1.0),
    )
    return ApiEndpoint(client, lambda: {})


def source_endpoint(handler: Callable[[httpx.Request], httpx.Response]) -> ApiEndpoint:
    client = httpx.Client(
        base_url=SOURCE_BASE,
        transport=httpx.MockTransport(handler),
        follow_redirects=False,
        timeout=make_timeout(1.0, 1.0),
    )
    return ApiEndpoint(client, lambda: {"Accept-Encoding": "identity"})


# ----------------------------------------------------------------------- clocks


@dataclass
class FakeClock:
    """Injected ISO + monotonic time: advance/jitter are deterministic and fake."""

    start_iso: str = BASE_ISO
    start_mono: float = 1000.0

    def __post_init__(self) -> None:
        self.now = parse_iso(self.start_iso)
        self.mono = self.start_mono

    def iso(self) -> str:
        return self.now.isoformat()

    def set_iso(self, raw: str) -> None:
        self.now = parse_iso(raw)

    def monotonic(self) -> float:
        return self.mono

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)
        self.mono += seconds

    def jump(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)

    def jitter(self) -> float:
        return 0.5


# ----------------------------------------------------------------- world records


@dataclass(frozen=True)
class EpisodeSpec:
    season: int
    number: int
    absolute: int | None = None
    type: str = "Episode"


@dataclass(frozen=True)
class SeriesSpec:
    series_id: int
    tvdb_id: int
    anime_ids: tuple[int, ...]
    episodes: tuple[EpisodeSpec, ...]
    root: str = SONARR_ROOT


@dataclass(frozen=True)
class FileSpec:
    series_id: int
    file_id: int
    shoko_id: int
    episode_index: tuple[int, ...] = (0,)


@dataclass(frozen=True)
class RegistryEpisode:
    anime: int
    type: str
    number: int
    anidb_ep: int
    shoko_ep: int


@dataclass(frozen=True)
class ShokoLocation:
    place_id: int
    folder: int
    relative: str
    absolute: str
    accessible: bool = True


@dataclass
class ShokoRow:
    file_id: int
    size: int
    locations: list[ShokoLocation]
    xrefs: list[dict[str, Any]] = field(default_factory=list)
    ignored: bool = False
    variation: bool = False
    imported: str | None = "2024-01-01T00:00:00"


def sonarr_path(series_id: int, file_id: int) -> str:
    return f"{SONARR_ROOT}/series{series_id}/file{file_id}.mkv"


def shoko_path(series_id: int, file_id: int) -> str:
    return f"{SHOKO_ROOT}/series{series_id}/file{file_id}.mkv"


def file_size(shoko_id: int) -> int:
    return 1_000_000 + shoko_id


# ------------------------------------------------------------------ mapping XML


def range_entry(anime: int, tvdb: int, start: int, end: int) -> str:
    return (
        f"<anime anidbid='{anime}' tvdbid='{tvdb}' defaulttvdbseason='1'>"
        f"<mapping-list><mapping anidbseason='1' tvdbseason='1' start='{start}' end='{end}'"
        " offset='0'>;</mapping></mapping-list></anime>"
    )


def pair_entry(anime: int, tvdb: int, pairs: str, season: str = "1") -> str:
    return (
        f"<anime anidbid='{anime}' tvdbid='{tvdb}' defaulttvdbseason='{season}'>"
        f"<mapping-list><mapping anidbseason='1' tvdbseason='1'>{pairs}</mapping>"
        "</mapping-list></anime>"
    )


def xml_document(parts: str) -> str:
    return f"<anime-list>{parts}</anime-list>"


def backlog_xml(n_series: int, per_series: int) -> str:
    parts = "".join(
        range_entry(4000 + series, 300_000 + series, 1, per_series)
        for series in range(1, n_series + 1)
    )
    return xml_document(parts)


def registry_for(series: SeriesSpec) -> list[RegistryEpisode]:
    rows: list[RegistryEpisode] = []
    for episode in series.episodes:
        for anime in series.anime_ids:
            rows.append(
                RegistryEpisode(
                    anime=anime,
                    type=episode.type,
                    number=episode.number,
                    anidb_ep=anime * 1000 + episode.number,
                    shoko_ep=500_000 + anime * 1000 + episode.number,
                )
            )
    return rows


# ------------------------------------------------------------------- Shoko stub


class ShokoStub:
    """Scripted Shoko v5.3.3 wire surface: listing, file reads, ID chain, queue, POST."""

    def __init__(self) -> None:
        self.rows: dict[int, ShokoRow] = {}
        self.registry: list[RegistryEpisode] = []
        self.pending: set[int] = set()
        self.folders: dict[int, tuple[str, str]] = {1: (SHOKO_ROOT, "None")}
        self.queue: tuple[int, int, int] = (1, 0, 4)
        self.requests: list[tuple[str, str]] = []
        self.posts: list[tuple[int, list[int]]] = []
        self.post_status: int | None = None
        self.post_raises: Exception | None = None
        self.post_redirect = False
        self.fail_queue = False
        self.link_on_post = True
        self.link_subset_on_post = False
        self.on_post: Callable[[], None] | None = None

    def add_row(self, row: ShokoRow) -> None:
        self.rows[row.file_id] = row

    def register(self, episodes: list[RegistryEpisode]) -> None:
        self.registry.extend(episodes)

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append((request.method, request.url.path))
        return self._respond(request)

    def get_counts(self) -> int:
        return sum(1 for method, _ in self.requests if method == "GET")

    def posts_count(self) -> int:
        return len(self.posts)

    def _respond(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "POST" and path.endswith("/Link"):
            return self._post(request, path)
        if request.method != "GET":
            return httpx.Response(405, json={"message": "method"}, request=request)
        return self._respond_get(request, path)

    def _respond_get(self, request: httpx.Request, path: str) -> httpx.Response:
        routes: dict[str, Callable[[], httpx.Response]] = {
            "/api/v3/ImportFolder": lambda: self._folders(request),
            "/api/v3/Queue": lambda: self._queue(request),
            "/api/v3/File": lambda: self._listing(request),
        }
        if path in routes:
            return routes[path]()
        if path.startswith("/api/v3/Series/AniDB/"):
            return self._chain_first(request, path)
        if path.startswith("/api/v3/Episode/AniDB/"):
            return self._chain_second(request, path)
        if path.startswith("/api/v3/File/"):
            return self._file(request, path)
        return httpx.Response(404, json={"message": "no route"}, request=request)

    def _folders(self, request: httpx.Request) -> httpx.Response:
        payload = [
            {
                "ID": folder,
                "Name": f"folder{folder}",
                "Path": spec[0],
                "DropFolderType": spec[1],
            }
            for folder, spec in sorted(self.folders.items())
        ]
        return httpx.Response(200, json=payload, request=request)

    def _queue(self, request: httpx.Request) -> httpx.Response:
        if self.fail_queue:
            return httpx.Response(503, json={"message": "queue down"}, request=request)
        waiting, blocked, total = self.queue
        return httpx.Response(
            200,
            json={"WaitingCount": waiting, "BlockedCount": blocked, "TotalCount": total},
            request=request,
        )

    def _listing(self, request: httpx.Request) -> httpx.Response:
        page = int(request.url.params.get("page", "1"))
        size = int(request.url.params.get("pageSize", str(SHOKO_PAGE_SIZE)))
        unrecognized_only = request.url.params.get("include_only") == "Unrecognized"
        keys = sorted(self.rows)
        if unrecognized_only:
            keys = [key for key in keys if not self.rows[key].xrefs]
        window = keys[(page - 1) * size : page * size]
        rows = [self._row_payload(self.rows[key]) for key in window]
        return httpx.Response(200, json={"Total": len(keys), "List": rows}, request=request)

    def _file(self, request: httpx.Request, path: str) -> httpx.Response:
        file_id = int(path.split("/")[4])
        row = self.rows.get(file_id)
        if row is None:
            return httpx.Response(404, json={"message": "gone"}, request=request)
        return httpx.Response(200, json=self._row_payload(row), request=request)

    def _row_payload(self, row: ShokoRow) -> dict[str, Any]:
        ed2k = f"ed2k-{row.file_id}"
        return {
            "ID": row.file_id,
            "Size": row.size,
            "IsIgnored": row.ignored,
            "IsVariation": row.variation,
            "Imported": row.imported,
            "Hashes": {
                "ED2K": ed2k,
                "SHA1": None,
                "CRC32": row.file_id,
                "MD5": None,
            },
            "Locations": [self._location_payload(item) for item in row.locations],
            "SeriesIDs": list(row.xrefs),
        }

    @staticmethod
    def _location_payload(location: ShokoLocation) -> dict[str, Any]:
        return {
            "ID": location.place_id,
            "FileID": location.place_id // 10,
            "ImportFolderID": location.folder,
            "RelativePath": location.relative,
            "AbsolutePath": location.absolute,
            "IsAccessible": location.accessible,
        }

    def _chain_first(self, request: httpx.Request, path: str) -> httpx.Response:
        anime = int(path.split("/")[5])
        type_name = request.url.params.get("type", "Episode")
        rows = [
            {
                "ID": item.anidb_ep,
                "AnimeID": anime,
                "Type": item.type,
                "EpisodeNumber": item.number,
                "AirDate": None,
                "Title": f"Ep {item.number}",
            }
            for item in self.registry
            if item.anime == anime and item.type == type_name
        ]
        return httpx.Response(200, json={"Total": len(rows), "List": rows}, request=request)

    def _chain_second(self, request: httpx.Request, path: str) -> httpx.Response:
        ep_id = int(path.split("/")[5])
        if ep_id in self.pending:
            return httpx.Response(404, json={"title": "not materialized"}, request=request)
        item = next((row for row in self.registry if row.anidb_ep == ep_id), None)
        if item is None:
            return httpx.Response(404, json={"title": "no such AniDB episode"}, request=request)
        payload = {"IDs": {"ID": item.shoko_ep, "AniDB": ep_id, "TMDB": None}, "Type": item.type}
        return httpx.Response(200, json=payload, request=request)

    def xrefs_for_ids(self, shoko_id: int, episode_ids: list[int]) -> list[dict[str, Any]]:
        by_anime: dict[int, list[dict[str, Any]]] = {}
        for item in self.registry:
            if item.shoko_ep in episode_ids:
                entry = {
                    "ID": item.shoko_ep,
                    "AniDB": item.anidb_ep,
                    "TMDB": None,
                    "Source": "User",
                }
                by_anime.setdefault(item.anime, []).append(entry)
        return [
            {"SeriesID": {"ID": None, "AniDB": anime, "TMDB": None}, "EpisodeIDs": episodes}
            for anime, episodes in sorted(by_anime.items())
        ]

    def _post(self, request: httpx.Request, path: str) -> httpx.Response:
        file_id = int(path.split("/")[4])
        body = cast("dict[str, Any]", json.loads(request.content))
        raw_ids = cast("list[Any]", body.get("EpisodeIDs", []))
        episode_ids = [int(item) for item in raw_ids]
        if self.on_post is not None:
            self.on_post()
        self.posts.append((file_id, episode_ids))
        if self.post_raises is not None:
            raise self.post_raises
        if self.post_redirect:
            return httpx.Response(307, headers={"location": path}, request=request)
        if self.post_status is not None:
            return httpx.Response(self.post_status, json={"title": "problem"}, request=request)
        if self.link_on_post and file_id in self.rows:
            linked_ids = episode_ids[:1] if self.link_subset_on_post else episode_ids
            self.rows[file_id].xrefs = self.xrefs_for_ids(file_id, linked_ids)
        return httpx.Response(200, json={}, request=request)

    def materialize(self, file_id: int, episode_ids: list[int]) -> None:
        row = self.rows.get(file_id)
        if row is not None:
            row.xrefs = self.xrefs_for_ids(file_id, episode_ids)

    def link_manually(self, file_id: int, anidb_anime: int, anidb_ep: int, shoko_ep: int) -> None:
        row = self.rows.get(file_id)
        if row is None:
            return
        group = next(
            (item for item in row.xrefs if item["SeriesID"].get("AniDB") == anidb_anime), None
        )
        episode = {"ID": shoko_ep, "AniDB": anidb_ep, "TMDB": None, "Source": "User"}
        if group is None:
            series_id = {"ID": None, "AniDB": anidb_anime, "TMDB": None}
            row.xrefs.append({"SeriesID": series_id, "EpisodeIDs": [episode]})
        else:
            episodes = cast("list[dict[str, Any]]", group["EpisodeIDs"])
            episodes.append(episode)


class SourceStub:
    """Anime-Lists source with content-bound ETag/304 behavior and failure modes."""

    def __init__(self, xml: str) -> None:
        self.xml = xml.encode("utf-8")
        self.etag = self._etag_for(self.xml)
        self.corrupt = False
        self.down = False
        self.requests: list[str] = []

    @staticmethod
    def _etag_for(body: bytes) -> str:
        return '"e2e-' + hashlib.sha256(body).hexdigest()[:10] + '"'

    def set_xml(self, xml: str) -> None:
        self.xml = xml.encode("utf-8")
        self.etag = self._etag_for(self.xml)

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(str(request.url))
        if self.down:
            return httpx.Response(503, request=request)
        body = b"<anime-list><anime anidbid='x'" if self.corrupt else self.xml
        if not self.corrupt and request.headers.get("if-none-match") == self.etag:
            return httpx.Response(304, headers={"ETag": self.etag}, request=request)
        return httpx.Response(200, headers={"ETag": self.etag}, content=body, request=request)


class SonarrStub:
    """Scripted Sonarr v3 read-only surface; outage mode fails every route."""

    def __init__(self) -> None:
        self.series: dict[int, tuple[str, int]] = {}
        self.episodes: dict[int, list[dict[str, Any]]] = {}
        self.files: dict[int, list[dict[str, Any]]] = {}
        self.roots: list[str] = [SONARR_ROOT]
        self.down = False
        self.requests: list[tuple[str, int]] = []

    def add_series(self, series_id: int, root: str, tvdb_id: int) -> None:
        self.series[series_id] = (root, tvdb_id)

    def add_episode(self, series_id: int, row: dict[str, Any]) -> None:
        self.episodes.setdefault(series_id, []).append(row)

    def add_file(self, series_id: int, row: dict[str, Any]) -> None:
        self.files.setdefault(series_id, []).append(row)

    def handler(self, request: httpx.Request) -> httpx.Response:
        series_param = request.url.params.get("seriesId")
        series_id = int(series_param) if series_param is not None else -1
        self.requests.append((request.url.path, series_id))
        if self.down:
            return httpx.Response(503, json={"message": "sonarr down"}, request=request)
        path = request.url.path
        if path == "/api/v3/series":
            return httpx.Response(200, json=self._series_payload(), request=request)
        if path == "/api/v3/rootfolder":
            return httpx.Response(200, json=self._root_payload(), request=request)
        if path == "/api/v3/episode":
            wanted = int(request.url.params["seriesId"])
            return httpx.Response(200, json=self.episodes.get(wanted, []), request=request)
        if path == "/api/v3/episodefile":
            wanted = int(request.url.params["seriesId"])
            return httpx.Response(200, json=self.files.get(wanted, []), request=request)
        return httpx.Response(404, json={"message": "no route"}, request=request)

    def _series_payload(self) -> list[dict[str, Any]]:
        return [
            {
                "id": series_id,
                "title": f"Series {series_id}",
                "rootFolderPath": root,
                "tvdbId": tvdb,
            }
            for series_id, (root, tvdb) in sorted(self.series.items())
        ]

    def _root_payload(self) -> list[dict[str, Any]]:
        return [
            {"id": index, "path": root, "accessible": True}
            for index, root in enumerate(self.roots, 1)
        ]

    def get_counts(self) -> int:
        return len(self.requests)


# ------------------------------------------------------------------------ world


@dataclass
class World:
    tmp_path: Path
    settings: Settings
    mapping_dir: Path
    source: SourceStub
    sonarr: SonarrStub
    shoko: ShokoStub
    lease_path: Path | None = None

    def mapping_store(self) -> MappingStore:
        return MappingStore(self.mapping_dir, source_url=SOURCE_URL)

    def endpoints(self) -> tuple[ApiEndpoint, ApiEndpoint, ApiEndpoint]:
        sonarr = endpoint_for(self.sonarr.handler, SONARR_BASE)
        shoko = endpoint_for(self.shoko.handler, SHOKO_BASE)
        source = source_endpoint(self.source.handler)
        return sonarr, shoko, source


def settings_document(
    *,
    apply_changes: bool = True,
    lease: Path | None = None,
    sync_extra: dict[str, Any] | None = None,
    campaign: dict[str, int] | None = None,
    overrides_file: str | None = None,
) -> dict[str, Any]:
    sync: dict[str, Any] = {
        "apply_changes": apply_changes,
        "max_concurrent_links": 4,
        "new_file_grace_period_seconds": 0,
        "max_new_per_cycle": 50,
        "max_pending_episode_jobs": 100,
        "rolling_new_limit": 200,
    }
    sync.update(sync_extra or {})
    mappings: dict[str, Any] = {
        "source": "anime-lists",
        "source_url": SOURCE_URL,
        "refresh_hours": 168,
    }
    if overrides_file is not None:
        mappings["overrides_file"] = overrides_file
    document: dict[str, Any] = {
        "sonarr": {
            "url": SONARR_BASE,
            "api_key_env": "SONARR_API_KEY",
            "allowed_roots": [SONARR_ROOT],
        },
        "shoko": {
            "url": SHOKO_BASE,
            "api_key_env": "SHOKO_API_KEY",
            "allowed_roots": [SHOKO_ROOT],
        },
        "paths": {"mappings": [{"sonarr": SONARR_ROOT, "shoko": SHOKO_ROOT}]},
        "mappings": mappings,
        "sync": sync,
        "database": {"path": STATE_PATH},
    }
    if lease is not None:
        document["safety"] = {
            "attestation_file": LEASE_PATH,
            "attestation_validity_hours": 168,
            "attestation_warn_seconds": 3600,
        }
    if campaign is not None:
        document["campaign"] = campaign
    return document


def make_settings(**kwargs: Any) -> Settings:
    return parse_settings(yaml.safe_dump(settings_document(**kwargs)))


def make_world(
    tmp_path: Path,
    xml: str,
    *,
    apply_changes: bool = True,
    lease: Path | None = None,
    sync_extra: dict[str, Any] | None = None,
    campaign: dict[str, int] | None = None,
    overrides_file: str | None = None,
) -> World:
    settings = make_settings(
        apply_changes=apply_changes,
        lease=lease,
        sync_extra=sync_extra,
        campaign=campaign,
        overrides_file=overrides_file,
    )
    source = SourceStub(xml)
    return World(
        tmp_path=tmp_path,
        settings=settings,
        mapping_dir=tmp_path / "mappings",
        source=source,
        sonarr=SonarrStub(),
        shoko=ShokoStub(),
        lease_path=lease,
    )


def seed_backlog(
    world: World,
    n_series: int,
    per_series: int,
    *,
    file_offset: int = 10_000,
) -> list[FileSpec]:
    """Materialize a backlog world of n_series x per_series files (both inventories)."""
    specs: list[FileSpec] = []
    for series in range(1, n_series + 1):
        series_spec = SeriesSpec(
            series_id=series,
            tvdb_id=300_000 + series,
            anime_ids=(4000 + series,),
            episodes=tuple(
                EpisodeSpec(season=1, number=index) for index in range(1, per_series + 1)
            ),
        )
        specs.extend(_seed_series(world, series_spec, file_offset))
    return specs


def _seed_series(world: World, series: SeriesSpec, file_offset: int) -> list[FileSpec]:
    world.sonarr.add_series(series.series_id, series.root, series.tvdb_id)
    world.shoko.register(registry_for(series))
    episode_rows: list[dict[str, Any]] = []
    file_rows: list[dict[str, Any]] = []
    specs: list[FileSpec] = []
    for index, episode in enumerate(series.episodes, start=1):
        file_id = series.series_id * 1000 + index
        shoko_id = file_offset + file_id
        specs.append(FileSpec(series.series_id, file_id, shoko_id, (index - 1,)))
        episode_rows.append(
            {
                "id": file_id * 10 + index,
                "seriesId": series.series_id,
                "seasonNumber": episode.season,
                "episodeNumber": episode.number,
                "title": f"S{episode.season:02d}E{index:02d}",
                "tvdbId": index,
                "episodeFileId": file_id,
            }
        )
        file_rows.append(
            {
                "id": file_id,
                "seriesId": series.series_id,
                "path": sonarr_path(series.series_id, file_id),
                "size": file_size(shoko_id),
            }
        )
        world.shoko.add_row(
            ShokoRow(
                file_id=shoko_id,
                size=file_size(shoko_id),
                locations=[
                    ShokoLocation(
                        place_id=shoko_id * 10,
                        folder=1,
                        relative=f"series{series.series_id}/file{file_id}.mkv",
                        absolute=shoko_path(series.series_id, file_id),
                    )
                ],
                # Representative unrecognized backlog: Shoko only sets DateTimeImported
                # after AniDB identification, so a deterministic portion of fully
                # hashed files stays Imported=null forever (mixed with real timestamps).
                imported=None if index % 2 == 0 else "2024-01-01T00:00:00",
            )
        )
    world.sonarr.episodes[series.series_id] = episode_rows
    world.sonarr.files[series.series_id] = file_rows
    return specs


def add_new_arrival(
    world: World,
    series_spec: SeriesSpec,
    spec: FileSpec,
    *,
    size: int | None = None,
) -> ShokoRow:
    """Add one file to both inventories (and its Shoko episode registry)."""
    world.sonarr.add_series(series_spec.series_id, series_spec.root, series_spec.tvdb_id)
    world.shoko.register(registry_for(series_spec))
    for index, episode in enumerate(series_spec.episodes):
        row: dict[str, Any] = {
            "id": spec.file_id * 10 + index,
            "seriesId": series_spec.series_id,
            "seasonNumber": episode.season,
            "episodeNumber": episode.number,
            "title": f"S{episode.season}E{episode.number}",
            "tvdbId": episode.number,
            "episodeFileId": spec.file_id,
        }
        if episode.absolute is not None:
            row["absoluteEpisodeNumber"] = episode.absolute
        world.sonarr.add_episode(series_spec.series_id, row)
    effective = size if size is not None else file_size(spec.shoko_id)
    world.sonarr.add_file(
        series_spec.series_id,
        {
            "id": spec.file_id,
            "seriesId": series_spec.series_id,
            "path": sonarr_path(spec.series_id, spec.file_id),
            "size": effective,
        },
    )
    record = ShokoRow(
        file_id=spec.shoko_id,
        size=effective,
        locations=[
            ShokoLocation(
                place_id=spec.shoko_id * 10,
                folder=1,
                relative=f"series{spec.series_id}/file{spec.file_id}.mkv",
                absolute=shoko_path(spec.series_id, spec.file_id),
            )
        ],
    )
    world.shoko.add_row(record)
    return record


def seed_mapping(world: World, clock: FakeClock, *, force: bool = True) -> None:
    store = world.mapping_store()
    endpoint = source_endpoint(world.source.handler)
    try:
        now = parse_iso(clock.iso())
        mapping_refresh(store, endpoint, now=now, refresh_hours=168.0, force=force)
    finally:
        endpoint.client.close()


def open_store(world: World, limits: BudgetLimits | None = None) -> StateStore:
    store = StateStore(world.tmp_path / "state.sqlite3", budget_limits=limits or WIDE_LIMITS)
    store.set_meta("budget_last_cycle_key", "cycle-1")
    return store


# ----------------------------------------------------------------- attestation


def record_lease(
    path: Path,
    *,
    scope: str = SCOPE_DAEMON,
    inspected_iso: str = BASE_ISO,
    expires_iso: str | None = None,
    version: str = "5.3.3",
    mounts: tuple[str, ...] = ("/anime read-only (host-inspected)",),
    flags: dict[str, bool] | None = None,
    note: str = "e2e lease",
    overwrite: bool = True,
) -> str:
    expires = expires_iso or iso_plus(inspected_iso, WEEK_SECONDS)
    record = build_record(
        endpoint=SHOKO_BASE,
        version=version,
        mounts=list(mounts),
        settings_flags=dict(flags or DEFAULT_FLAGS),
        inspected_iso=inspected_iso,
        expires_iso=expires,
        scope=scope,
        operator_note=note,
    )
    target, digest, _warning = write_attestation(path, record, overwrite=overwrite)
    assert target == path
    return digest


def lease_verdict(
    world: World, clock: FakeClock, *, scope: str = SCOPE_DAEMON
) -> AttestationVerdict:
    assert world.lease_path is not None
    return load_attestation(
        world.lease_path,
        shoko_url=SHOKO_BASE,
        now_iso=clock.iso(),
        scope=scope,
        warn_lead_seconds=world.settings.safety.attestation_warn_seconds,
    )


# -------------------------------------------------------------------- daemon


def build_deps(
    world: World,
    store: StateStore,
    clock: FakeClock,
    *,
    attestation: bool = True,
    use_lease_verdict: bool = False,
    attestation_scope: str = SCOPE_DAEMON,
    max_submissions: int = 1,
    blocked_over: int | None = 0,
    verification_timeout: float = 60.0,
    poll_interval: float = 10.0,
    max_verify_polls: int = 20,
    max_cycle_seconds: float = 1e9,
    churn_limit: int = 500,
    max_recovery_rows_per_cycle: int = 100,
    overrides_text: str | None = None,
    monotonic: Callable[[], float] | None = None,
    advance: Callable[[float], None] | None = None,
) -> DaemonDeps:
    sonarr, shoko, source = world.endpoints()

    def verdict() -> AttestationVerdict | None:
        if use_lease_verdict and world.lease_path is not None:
            return lease_verdict(world, clock, scope=attestation_scope)
        return None

    def valid() -> bool:
        if use_lease_verdict:
            current = verdict()
            return current is not None and current.valid_for_apply
        return attestation

    return DaemonDeps(
        settings=world.settings,
        store=store,
        mapping_store=world.mapping_store(),
        sonarr=sonarr,
        shoko=shoko,
        mapping_source=source,
        overrides_text=overrides_text,
        monotonic=monotonic if monotonic is not None else clock.monotonic,
        advance=advance if advance is not None else clock.advance,
        jitter=clock.jitter,
        pid=4242,
        attestation_valid=valid,
        attestation_verdict=verdict,
        blocked_over=blocked_over,
        verification_timeout_seconds=verification_timeout,
        poll_interval_seconds=poll_interval,
        max_verify_polls=max_verify_polls,
        max_submissions=max_submissions,
        max_cycle_seconds=max_cycle_seconds,
        churn_limit=churn_limit,
        max_recovery_rows_per_cycle=max_recovery_rows_per_cycle,
    )


# ----------------------------------------------------------------- campaigns


@dataclass
class CampaignEnv:
    """CLI-seam wiring for campaign commands over a scripted world."""

    world: World
    clock: FakeClock
    lease: Path
    plan_path: Path
    now: str
    read_out: Callable[[], str] = lambda: ""

    def open_store(self) -> StateStore:
        return StateStore(self.world.tmp_path / "state.sqlite3", budget_limits=CAMPAIGN_LIMITS)

    def write_lease(self, campaign_id: str) -> str:
        return record_lease(
            self.lease,
            scope=f"campaign:{campaign_id}",
            inspected_iso=self.now,
            expires_iso=iso_plus(self.now, WEEK_SECONDS),
        )

    def prepare(self, *, limit: int = 12, deadline_hours: str = "168") -> dict[str, Any]:
        seed_mapping(self.world, self.clock)
        config = str(self.world.tmp_path / "config.yaml")
        args = [
            "campaign",
            "prepare",
            config,
            "--out",
            str(self.plan_path),
            "--deadline-hours",
            deadline_hours,
            "--limit",
            str(limit),
            "--json",
        ]
        assert self._main(args) == cli.EXIT_OK
        payload = cast("dict[str, Any]", json.loads(self.read_out()))
        self.write_lease(str(payload["campaign_id"]))
        return payload

    def approve(self, digest: str) -> dict[str, Any]:
        args = [
            "campaign",
            "approve",
            str(self.world.tmp_path / "config.yaml"),
            "--plan",
            str(self.plan_path),
            "--digest",
            digest,
            "--json",
        ]
        assert self._main(args) == cli.EXIT_OK
        return cast("dict[str, Any]", json.loads(self.read_out()))

    def apply(self, *, batch: int | None = None) -> tuple[int, Any]:
        args = [
            "campaign",
            "apply",
            str(self.world.tmp_path / "config.yaml"),
            "--plan",
            str(self.plan_path),
            "--json",
        ]
        if batch is not None:
            args += ["--batch-size", str(batch)]
        code = self._main(args)
        captured = self.read_out()
        if code == cli.EXIT_OK:
            return code, json.loads(captured)
        return code, captured

    def report(self, campaign_id: str) -> dict[str, Any]:
        args = [
            "campaign",
            "report",
            str(self.world.tmp_path / "config.yaml"),
            "--campaign-id",
            campaign_id,
            "--plan",
            str(self.plan_path),
            "--json",
        ]
        assert self._main(args) == cli.EXIT_OK
        return cast("dict[str, Any]", json.loads(self.read_out()))

    def revoke(self, campaign_id: str) -> int:
        args = [
            "campaign",
            "revoke",
            str(self.world.tmp_path / "config.yaml"),
            "--campaign-id",
            campaign_id,
            "--note",
            "e2e revoke",
            "--confirm",
            "--json",
        ]
        return self._main(args)

    def audit(self, extra: list[str] | None = None) -> dict[str, Any]:
        args = [
            "audit",
            str(self.world.tmp_path / "config.yaml"),
            "--max-requests",
            "400",
            "--limit",
            "400",
            "--json",
        ]
        args += extra or []
        assert self._main(args) == cli.EXIT_OK
        return cast("dict[str, Any]", json.loads(self.read_out()))

    def _main(self, args: list[str]) -> int:
        return int(cli.main(args))


def wire_campaign_env(env: CampaignEnv, monkeypatch: Any) -> None:
    """Patch the CLI seams to the scripted world (tmp stores, fake clock, lease)."""
    world = env.world

    def endpoint(handler: Callable[[httpx.Request], httpx.Response], base: str) -> ApiEndpoint:
        return endpoint_for(handler, base)

    def source(store: MappingStore, settings: Settings) -> ApiEndpoint:
        return source_endpoint(world.source.handler)

    def open_state(settings: Settings) -> StateStore:
        return env.open_store()

    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    monkeypatch.setattr(cli, "open_state_store", open_state)
    monkeypatch.setattr(cli, "state_path", lambda settings: world.tmp_path / "state.sqlite3")
    monkeypatch.setattr(
        cli,
        "attestation_target_path",
        lambda settings, scope=SCOPE_DAEMON, *, for_write=False: env.lease,
    )
    monkeypatch.setattr(cli, "current_iso", lambda: env.now)
    monkeypatch.setattr(cli, "sleep", lambda seconds: None)
    monkeypatch.setattr(cli, "interruptible_sleep", lambda seconds: None)
    monkeypatch.setattr(
        cli, "sonarr_endpoint", lambda settings: endpoint(world.sonarr.handler, SONARR_BASE)
    )
    monkeypatch.setattr(
        cli, "shoko_endpoint", lambda settings: endpoint(world.shoko.handler, SHOKO_BASE)
    )
    monkeypatch.setattr(cli, "source_endpoint", source)
    monkeypatch.setattr(cli, "open_mapping_store", lambda settings: world.mapping_store())
    monkeypatch.setattr(cli, "read_overrides_text", lambda settings: None)


# ------------------------------------------------------------------- measuring


@dataclass
class RunStats:
    wall_seconds: float = 0.0
    tracemalloc_peak_bytes: int = 0
    sonarr_requests: int = 0
    shoko_requests: int = 0
    shoko_gets: int = 0
    shoko_posts: int = 0
    source_requests: int = 0


@contextmanager
def measure(world: World) -> Iterator[RunStats]:
    stats = RunStats()
    tracemalloc.start()
    started = time.perf_counter()
    try:
        yield stats
    finally:
        stats.wall_seconds = time.perf_counter() - started
        _, peak = tracemalloc.get_traced_memory()
        stats.tracemalloc_peak_bytes = int(peak)
        tracemalloc.stop()
        stats.sonarr_requests = world.sonarr.get_counts()
        stats.shoko_requests = len(world.shoko.requests)
        stats.shoko_gets = world.shoko.get_counts()
        stats.shoko_posts = world.shoko.posts_count()
        stats.source_requests = len(world.source.requests)


__all__ = [
    "CampaignEnv",
    "EpisodeSpec",
    "FakeClock",
    "FileSpec",
    "RegistryEpisode",
    "RunStats",
    "SeriesSpec",
    "ShokoLocation",
    "ShokoRow",
    "ShokoStub",
    "SonarrStub",
    "SourceStub",
    "World",
    "add_new_arrival",
    "backlog_xml",
    "build_deps",
    "endpoint_for",
    "file_size",
    "iso_plus",
    "lease_verdict",
    "make_settings",
    "make_world",
    "measure",
    "open_store",
    "pair_entry",
    "parse_iso",
    "record_lease",
    "registry_for",
    "seed_backlog",
    "seed_mapping",
    "settings_document",
    "shoko_path",
    "sonarr_path",
    "source_endpoint",
    "wire_campaign_env",
    "xml_document",
]
