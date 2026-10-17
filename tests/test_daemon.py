"""M6-b daemon gates: reconciliation cycle, mapping scheduler, daemon loop.

Every test is offline: injected endpoints run on ``httpx.MockTransport`` (no network,
no live Sonarr/Shoko), clocks are fake, and the state + mapping stores live in
``tmp_path``. The request log per test is the evidence: dry-run and
suppressed cycles show **zero POSTs**, the apply happy path shows **exactly one POST**,
and every crash/ambiguity/churn path shows the durable ordering.
"""

from __future__ import annotations

import contextlib
import json
import logging
import re
import sqlite3
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar, cast
from urllib.parse import urlencode

import hash_fixtures as hf
import httpx
import pytest
import yaml  # type: ignore[import-untyped]

from shoko_linker import daemon as daemon_module
from shoko_linker.attestation import AttestationVerdict
from shoko_linker.campaign import (
    apply_campaign,
    approve_campaign,
    authorize_campaign_drain,
    campaign_send_progress,
    prepare_campaign,
    write_plan_file,
)
from shoko_linker.cli import StopState, persist_cycle_meta
from shoko_linker.config import Settings, parse_settings
from shoko_linker.daemon import (
    DAEMON_HISTORY_MAXLEN,
    CycleMode,
    CycleReport,
    DaemonDeps,
    attestation_gate,
    compute_eligibility,
    full_cycle_due,
    run_cycle,
    run_daemon,
    run_mapping_check,
)
from shoko_linker.dashboard import DashboardApplication, DashboardStatus, create_dashboard_server
from shoko_linker.http import ApiEndpoint, make_timeout
from shoko_linker.linking import EpisodeIdResolver, SendOutcome, VerificationVerdict
from shoko_linker.mapping_store import (
    ActiveMappings,
    MappingStore,
    RefreshOutcome,
    load_active,
    refresh,
)
from shoko_linker.mappings import (
    EpisodeType,
    MappingTarget,
    Resolution,
    ResolutionStatus,
    SourceRule,
)
from shoko_linker.policies import RunPolicy
from shoko_linker.reporting import (
    META_CYCLE_CAMPAIGN_ID,
    META_CYCLE_CAMPAIGN_SUBMITTED,
    META_CYCLE_KEY,
    META_CYCLE_SUBMITTED,
    cycle_report_payload,
)
from shoko_linker.service import run_dashboard_worker
from shoko_linker.state import (
    MAX_DETAIL_CHARS,
    META_LAST_DISCOVERY_EXCLUSIONS,
    META_LAST_FULL_RECONCILE_ISO,
    META_NEXT_FULL_RECONCILE_ISO,
    BaselineFile,
    BudgetLimits,
    Cohort,
    FileState,
    SendToken,
    StateError,
    StateStore,
    acquire_apply_lock,
    release_apply_lock,
)

SONARR_BASE = "http://sonarr:8989"
SHOKO_BASE = "http://shoko:8111"
SOURCE_BASE = "https://raw.githubusercontent.com"
SOURCE_PATH = "/Anime-Lists/anime-lists/master/anime-list-full.xml"
SOURCE_URL = SOURCE_BASE + SOURCE_PATH
SONARR_ROOT = "/data/media/anime"
SHOKO_ROOT = "/anime"
ANIME_ID = 1234
CHANGED_ANIME_ID = 5678
CHANGED_ANIDB_EPISODE = 6501
CHANGED_SHOKO_EPISODE = 778
TVDB_SERIES = 267440
ANIDB_EP = 5501
SHOKO_EP = 777
FILE_SIZE = 1000
T0 = "2026-01-01T00:00:00+00:00"
T1 = "2026-01-01T00:10:00+00:00"
T2 = "2026-01-01T00:20:00+00:00"
HOURS = 168.0
DAYS_7 = 168.0
MONO_ZERO = 100.0
VERIFICATION_TIMEOUT = 50.0
POLL_INTERVAL = 10.0
FIRST_RUN_COUNT = 7000
COUNT_TWO = 2
COUNT_THREE = 3
EPSILON = 1e-6
STOP_AT = 2
COUNT_ONE = 1
COUNT_FOUR = 4
STALE_LOCK_PID = 999
DAEMON_PID = 4242
FIRST_SCAN_WALL_BUDGET_SECONDS = 10.0
POSTS_ONE = 1
POSTS_TWO = 2
POSTS_THREE = 3
POSTS_FOUR = 4
JOBS_TWO = 2
RECOVERY_ROW_CAP = 500
DRAIN_WALL_SECONDS = 30.0
MONO_GETS_BOUND = 200
MONO_REMAIN_FLOOR = 400
MONO_BUDGET_SECONDS = 5.0
CYCLE_GET_OVERHEAD = 8
SEED_MONO_ROWS = 500
MONO_CLOCK_SPAN = 4000
MONO_CAP_DISABLED = 100000

WIDE_LIMITS = BudgetLimits(
    max_pending_files=50,
    max_pending_episode_jobs=100,
    per_cycle_new=50,
    rolling_new=50,
    rolling_window_hours=24.0,
)
TWO_LIMITS = BudgetLimits(
    max_pending_files=50,
    max_pending_episode_jobs=100,
    per_cycle_new=50,
    rolling_new=50,
    rolling_window_hours=24.0,
)


@dataclass(frozen=True)
class SpecFile:
    series_id: int
    file_id: int
    shoko_id: int
    episodes: tuple[tuple[int, int, int, int], ...] = ((1, 1, ANIDB_EP, SHOKO_EP),)
    tvdb_series: int = TVDB_SERIES
    anime_id: int = ANIME_ID
    absolute_episode_numbers: tuple[int, ...] = ()


def _sonarr_path(file: SpecFile) -> str:
    return f"{SONARR_ROOT}/series{file.series_id}/{file.shoko_id}.mkv"


def _shoko_path(file: SpecFile) -> str:
    return f"{SHOKO_ROOT}/series{file.series_id}/{file.shoko_id}.mkv"


# ------------------------------------------------------------------ Shoko stub


class ShokoStub:
    """Scripted Shoko v5.3.3 API: discovery, preflight reads, ID chain, queue, POST."""

    def __init__(self, files: list[SpecFile], blocked_count: int = 0) -> None:
        self.files = {file.shoko_id: file for file in files}
        self.linked: dict[int, list[dict[str, Any]]] = {}
        self.requests: list[httpx.Request] = []
        self.posts: list[dict[str, Any]] = []
        self.fail_post: Exception | None = None
        self.fail_queue = False
        self.blocked_count = blocked_count
        self.link_on_post = True
        self.post_status: int | None = None
        self.on_post: Callable[[], None] | None = None
        self.unmaterialized: set[int] = set()  # AniDB episode ids whose Shoko metadata is not ready
        self.ed2k_overrides: dict[int, str] = {}
        self.file_id_overrides: dict[int, int] = {}
        self.target_episode_overrides: dict[int, int] = {}
        self.target_read_failures: dict[str, tuple[int, int | None]] = {}
        self.target_read_counts: dict[str, int] = {}
        #: P2-1: fired on every GET with the request path — simulates a slow preflight network
        #: phase (authorization state flips mid-call, before the POST).
        self.on_get: Callable[[str], None] | None = None

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.method == "GET" and self.on_get is not None:
            self.on_get(request.url.path)
        if request.method == "GET" and request.url.path in self.target_read_failures:
            path = request.url.path
            read_number = self.target_read_counts.get(path, 0) + 1
            self.target_read_counts[path] = read_number
            failure = self.target_read_failures[path]
            if read_number == failure[0]:
                if failure[1] is None:
                    raise httpx.ReadTimeout("synthetic-api-key-must-not-leak", request=request)
                if failure[1] == 200:
                    return httpx.Response(200, content=b"{", request=request)
                return httpx.Response(failure[1], json={}, request=request)
        return self._respond(request)

    def _queue(self, request: httpx.Request) -> httpx.Response:
        if self.fail_queue:
            return httpx.Response(503, request=request)
        return self._json(
            request,
            {
                "WaitingCount": 5,
                "BlockedCount": self.blocked_count,
                "TotalCount": 20,
            },
        )

    def _respond(self, request: httpx.Request) -> httpx.Response:
        method = request.method
        path = request.url.path
        response: httpx.Response | None = None
        if method == "POST" and path.endswith("/Link"):
            response = self._post(request, path)
        elif method == "GET":
            response = self._respond_get(path, request)
        if response is None:
            response = httpx.Response(404, json={"message": "no route"}, request=request)
        return response

    def _respond_get(self, path: str, request: httpx.Request) -> httpx.Response | None:
        response: httpx.Response | None = None
        if path == "/api/v3/ImportFolder":
            response = self._json(request, [self._folder()])
        elif path == "/api/v3/Queue":
            response = self._queue(request)
        elif path.startswith("/api/v3/Series/AniDB/"):
            response = self._chain_first(request)
        elif path.startswith("/api/v3/Episode/AniDB/"):
            response = self._chain_second(request, path)
        elif path == "/api/v3/File":
            response = self._listing(request)
        elif path.startswith("/api/v3/File/"):
            response = self._file(request, path)
        return response

    def _json(self, request: httpx.Request, payload: Any) -> httpx.Response:
        return httpx.Response(200, json=payload, request=request)

    def _folder(self) -> dict[str, Any]:
        return {
            "ID": 1,
            "Name": "Sonarr",
            "Path": SHOKO_ROOT,
            "DropFolderType": "None",
            "WatchForNewFiles": True,
        }

    def _listing(self, request: httpx.Request) -> httpx.Response:
        rows = [self._file_row(shoko_id, xrefs=[]) for shoko_id in sorted(self.files)]
        page = int(request.url.params.get("page", "1"))
        page_size = int(request.url.params.get("pageSize", "1000"))
        start = (page - 1) * page_size
        return self._json(
            request,
            {"Total": len(rows), "List": rows[start : start + page_size]},
        )

    def _post(self, request: httpx.Request, path: str) -> httpx.Response:
        shoko_id = int(path.split("/")[4])
        if self.on_post is not None:
            self.on_post()
        self.posts.append(json.loads(request.content))
        if self.link_on_post:
            file = self.files[shoko_id]
            self.linked[shoko_id] = [self._xref(file.anime_id, file.episodes)]
        if self.fail_post is not None:
            raise self.fail_post
        if self.post_status is not None:
            return httpx.Response(
                self.post_status, json={"title": "validation problem"}, request=request
            )
        return self._json(request, {})

    def materialize(
        self, shoko_id: int, episodes: tuple[tuple[int, int, int, int], ...] | None = None
    ) -> None:
        """Simulate the async ManualLinkJob landing server-side (read side only)."""
        file = self.files[shoko_id]
        self.linked[shoko_id] = [self._xref(file.anime_id, episodes or file.episodes)]

    def link_unrelated(self, shoko_id: int) -> None:
        """Server-side state carries an XRef to an UNRELATED episode set."""
        self.linked[shoko_id] = [
            {
                "SeriesID": {"ID": None, "AniDB": 31337, "TMDB": None},
                "EpisodeIDs": [{"ID": 888, "AniDB": 42424, "TMDB": None, "Source": "User"}],
            }
        ]

    def _file(self, request: httpx.Request, path: str) -> httpx.Response:
        shoko_id = int(path.split("/")[4])
        if shoko_id not in self.files:
            return httpx.Response(404, json={"message": "gone"}, request=request)
        include = request.url.params.get("include", "")
        if "AbsolutePaths" in include:
            return self._json(
                request, self._file_row(shoko_id, xrefs=self.linked.get(shoko_id, []))
            )
        return self._json(request, self._file_row(shoko_id, xrefs=self.linked.get(shoko_id, [])))

    def _file_row(self, shoko_id: int, xrefs: list[dict[str, Any]]) -> dict[str, Any]:
        file = self.files[shoko_id]
        hashes = hf.ed2k_hashes(shoko_id, FILE_SIZE, f"{shoko_id}.mkv")
        hashes["ED2K"] = self.ed2k_overrides.get(shoko_id, hashes["ED2K"])
        return {
            "ID": self.file_id_overrides.get(shoko_id, shoko_id),
            "Size": FILE_SIZE,
            "IsIgnored": False,
            "IsVariation": False,
            "Imported": "2024-01-01T00:00:00",
            "Hashes": hashes,
            "Locations": [self._location(file)],
            "SeriesIDs": xrefs,
        }

    def _location(self, file: SpecFile) -> dict[str, Any]:
        return {
            "ID": file.shoko_id * 10,
            "FileID": file.shoko_id,
            "ImportFolderID": 1,
            "RelativePath": f"series{file.series_id}/{file.shoko_id}.mkv",
            "AbsolutePath": _shoko_path(file),
            "IsAccessible": True,
        }

    def _xref(
        self, anime_id: int, episodes: tuple[tuple[int, int, int, int], ...]
    ) -> dict[str, Any]:
        return {
            "SeriesID": {"ID": None, "AniDB": anime_id, "TMDB": None},
            "EpisodeIDs": [
                {"ID": shoko_ep, "AniDB": anidb_ep, "TMDB": None, "Source": "User"}
                for _season, _number, anidb_ep, shoko_ep in episodes
            ],
        }

    def _chain_first(self, request: httpx.Request) -> httpx.Response:
        anime_id = int(request.url.path.split("/")[5])
        rows: list[dict[str, Any]] = []
        seen: set[int] = set()
        for file in self.files.values():
            if file.anime_id != anime_id:
                continue
            for _season, number, anidb_ep, _shoko in file.episodes:
                if anidb_ep in seen:
                    continue
                seen.add(anidb_ep)
                rows.append(self._anidb_episode(anidb_ep, anime_id, number))
        return self._json(request, {"Total": len(rows), "List": rows})

    def _anidb_episode(self, ep_id: int, anime_id: int, number: int) -> dict[str, Any]:
        return {
            "ID": ep_id,
            "AnimeID": anime_id,
            "Type": "Episode",
            "EpisodeNumber": number,
            "AirDate": None,
            "Title": f"Ep {number}",
            "Titles": [],
            "Description": None,
            "Rating": None,
        }

    def _chain_second(self, request: httpx.Request, path: str) -> httpx.Response:
        ep_id = int(path.split("/")[5])
        if ep_id in self.unmaterialized:
            return httpx.Response(404, json={"title": "not materialized"}, request=request)
        shoko_ep = SHOKO_EP
        for file in self.files.values():
            for _season, _number, anidb_ep, file_shoko in file.episodes:
                if anidb_ep == ep_id:
                    shoko_ep = file_shoko
        shoko_ep = self.target_episode_overrides.get(ep_id, shoko_ep)
        return self._json(
            request,
            {"IDs": {"ID": shoko_ep, "AniDB": ep_id, "TMDB": None}, "Type": "Episode"},
        )

    def posts_count(self) -> int:
        return len(self.posts)

    def methods(self) -> list[str]:
        return [request.method for request in self.requests]


class MappingChangeShokoStub(ShokoStub):
    """Resolve both old and new Anime-Lists targets so quarantine is the only safe outcome."""

    def _chain_first(self, request: httpx.Request) -> httpx.Response:
        anime_id = int(request.url.path.split("/")[5])
        episode = {
            ANIME_ID: ANIDB_EP,
            CHANGED_ANIME_ID: CHANGED_ANIDB_EPISODE,
        }.get(anime_id)
        rows = [] if episode is None else [self._anidb_episode(episode, anime_id, 1)]
        return self._json(request, {"Total": len(rows), "List": rows})

    def _chain_second(self, request: httpx.Request, path: str) -> httpx.Response:
        episode = int(path.split("/")[5])
        shoko_episode = {
            ANIDB_EP: SHOKO_EP,
            CHANGED_ANIDB_EPISODE: CHANGED_SHOKO_EPISODE,
        }.get(episode)
        if shoko_episode is None:
            return httpx.Response(404, json={"title": "not materialized"}, request=request)
        return self._json(
            request,
            {"IDs": {"ID": shoko_episode, "AniDB": episode, "TMDB": None}, "Type": "Episode"},
        )


# ------------------------------------------------------------------ Sonarr stub


class SonarrStub:
    """Scripted Sonarr v3 read-only inventory for a scenario."""

    def __init__(self, files: list[SpecFile]) -> None:
        self.files = files
        self.series_ids = sorted({file.series_id for file in files})
        self.series_tvdb = {file.series_id: file.tvdb_series for file in files}
        self.requests: list[httpx.Request] = []
        self.include_extra: SpecFile | None = None
        self.episodes_status: dict[int, int] = {}
        self.files_status: dict[int, int] = {}
        self.on_get: Callable[[httpx.Request], None] | None = None

    def handler(self, request: httpx.Request) -> httpx.Response:  # noqa: PLR0911
        self.requests.append(request)
        if self.on_get is not None:
            self.on_get(request)
        path = request.url.path
        if path == "/api/v3/series":
            return self._json(request, self._series())
        if path == "/api/v3/rootfolder":
            return self._json(request, [{"id": 1, "path": SONARR_ROOT, "accessible": True}])
        if path == "/api/v3/episode":
            series_id = int(request.url.params["seriesId"])
            status = self.episodes_status.get(series_id, 200)
            if status != 200:
                return httpx.Response(status, json={"message": "injected"}, request=request)
            return self._json(request, self._episodes(series_id))
        if path == "/api/v3/episodefile":
            series_id = int(request.url.params["seriesId"])
            status = self.files_status.get(series_id, 200)
            if status != 200:
                return httpx.Response(status, json={"message": "injected"}, request=request)
            return self._json(request, self._files(series_id))
        return httpx.Response(404, json={"message": "no route"}, request=request)

    def _json(self, request: httpx.Request, payload: Any) -> httpx.Response:
        return httpx.Response(200, json=payload, request=request)

    def _series(self) -> list[dict[str, Any]]:
        return [
            {
                "id": series_id,
                "title": f"Series {series_id}",
                "rootFolderPath": f"{SONARR_ROOT}/series{series_id}",
                "seriesType": "anime",
                "tvdbId": self.series_tvdb[series_id],
            }
            for series_id in self.series_ids
        ]

    def _episodes(self, series_id: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for file in [item for item in self.files if item.series_id == series_id]:
            for index, (season, number, _anidb, _shoko) in enumerate(file.episodes):
                row = {
                    "id": file.file_id * 100 + number,
                    "seriesId": series_id,
                    "seasonNumber": season,
                    "episodeNumber": number,
                    "title": f"S{season}E{number}",
                    "tvdbId": number,
                    "episodeFileId": file.file_id,
                }
                if index < len(file.absolute_episode_numbers):
                    row["absoluteEpisodeNumber"] = file.absolute_episode_numbers[index]
                rows.append(row)
        return rows

    def _files(self, series_id: int) -> list[dict[str, Any]]:
        return [
            {
                "id": file.file_id,
                "seriesId": series_id,
                "path": _sonarr_path(file),
                "size": FILE_SIZE,
            }
            for file in [item for item in self.files if item.series_id == series_id]
        ]


# ------------------------------------------------------------------ fixtures


def _entry(tvdb: int, anidb: int) -> str:
    return (
        f"<anime anidbid='{anidb}' tvdbid='{tvdb}' defaulttvdbseason='1'>"
        "<mapping-list><mapping anidbseason='1' tvdbseason='1'>;1-1;</mapping></mapping-list>"
        "</anime>"
    )


def mapping_xml() -> str:
    return "<anime-list>" + _entry(TVDB_SERIES, ANIME_ID) + "</anime-list>"


def mapping_xml_mapped(*extra: tuple[int, int]) -> str:
    parts = [_entry(TVDB_SERIES, ANIME_ID)]
    parts.extend(_entry(tvdb, anidb) for tvdb, anidb in sorted(extra))
    return "<anime-list>" + "".join(parts) + "</anime-list>"


# Two generations that flip the target sets of series 1 and 2 (series 267440 stable).
GEN_A = mapping_xml_mapped((1, 1001), (2, 1002))
GEN_B = mapping_xml_mapped((1, 2001), (2, 2002))


def make_settings(
    tmp_path: Path,
    *,
    apply_changes: bool = True,
    sync_extra: dict[str, Any] | None = None,
) -> Settings:
    # Harness default: the new-file grace window is OFF so cycle tests pin the apply
    # path they were written for. Grace enforcement is proven by the ``grace`` tests
    # below, which set the knob explicitly (the shipped default is 300).
    sync: dict[str, Any] = {
        "apply_changes": apply_changes,
        "max_concurrent_links": 4,
        "new_file_grace_period_seconds": 0,
    }
    sync.update(sync_extra or {})
    document = {
        "sonarr": {
            "url": SONARR_BASE,
            "api_key_env": "SONARR_API_KEY",
            "allowed_roots": [SONARR_ROOT],
        },
        "shoko": {"url": SHOKO_BASE, "api_key_env": "SHOKO_API_KEY", "allowed_roots": [SHOKO_ROOT]},
        "paths": {"mappings": [{"sonarr": SONARR_ROOT, "shoko": SHOKO_ROOT}]},
        "mappings": {"source": "anime-lists", "source_url": SOURCE_URL, "refresh_hours": HOURS},
        "sync": sync,
        "database": {"path": "/daemon/state.sqlite3"},
    }
    return parse_settings(yaml.safe_dump(document))


def endpoint_for(handler: Callable[[httpx.Request], httpx.Response], base: str) -> ApiEndpoint:
    client = httpx.Client(
        base_url=base,
        transport=httpx.MockTransport(handler),
        follow_redirects=False,
        timeout=make_timeout(1.0, 1.0),
    )
    return ApiEndpoint(client, lambda: {})


def source_endpoint(handler: Callable[[httpx.Request], httpx.Response]) -> ApiEndpoint:
    """Mapping-source endpoint: origin base with the identity encoding the ETag needs."""
    client = httpx.Client(
        base_url=SOURCE_BASE,
        transport=httpx.MockTransport(handler),
        follow_redirects=False,
        timeout=make_timeout(1.0, 1.0),
    )
    return ApiEndpoint(client, lambda: {"Accept-Encoding": "identity"})


def source_handler(xml: str) -> Callable[[httpx.Request], httpx.Response]:
    def build(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"ETag": '"identity-src"'},
            stream=httpx.ByteStream(xml.encode()),
            request=request,
        )

    return build


class Clock:
    """Successive monotonic values; the last value repeats."""

    def __init__(self, values: list[float]) -> None:
        self._values = values
        self._index = 0

    def __call__(self) -> float:
        value = self._values[min(self._index, len(self._values) - 1)]
        self._index += 1
        return value


class Sleeper:
    def __init__(self) -> None:
        self.sleeps: list[float] = []

    def advance(self, seconds: float) -> None:
        self.sleeps.append(seconds)


def seed_baseline(store: StateStore, files: list[SpecFile], iso: str = T0) -> None:
    store.record_baseline(
        [
            BaselineFile(
                sonarr_series_id=file.series_id,
                sonarr_file_id=file.file_id,
                path=_sonarr_path(file),
                size=FILE_SIZE,
            )
            for file in files
        ],
        complete=True,
        auto_apply_eligible=True,
        iso=iso,
    )


def seed_mapping(store_dir: Path, xml: str, now_iso: str) -> None:
    store = MappingStore(store_dir, source_url=SOURCE_URL)
    endpoint = source_endpoint(source_handler(xml))
    try:
        refresh(store, endpoint, now=_parse(now_iso), refresh_hours=HOURS, force=True)
    finally:
        endpoint.client.close()


def _parse(raw: str) -> datetime:
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def build_deps(
    tmp_path: Path,
    *,
    store: StateStore,
    sonarr_files: list[SpecFile],
    shoko_files: list[SpecFile],
    xml: str,
    monotonic: Callable[[], float],
    advance: Callable[[float], None],
    jitter: Callable[[], float] = lambda: 0.5,
    attestation: bool = True,
    attestation_valid: Callable[[], bool] | None = None,
    max_submissions: int = 1,
    verification_timeout: float = VERIFICATION_TIMEOUT,
    poll_interval: float = POLL_INTERVAL,
    max_cycle_seconds: float = 1e9,
    churn_limit: int = 500,
    max_recovery_rows_per_cycle: int = 100,
    blocked_over: int | None = 0,
    settings: Settings | None = None,
    shoko: ShokoStub | None = None,
    sonarr_stub: SonarrStub | None = None,
    mapping_dir: Path | None = None,
    now: Callable[[], str] = lambda: T0,
    shutdown_requested: Callable[[], bool] = lambda: False,
) -> DaemonDeps:
    settings = settings or make_settings(tmp_path)
    mapping_directory = mapping_dir if mapping_dir is not None else tmp_path / "mappings"
    mapping_store = MappingStore(mapping_directory, source_url=SOURCE_URL)
    shoko_stub = shoko if shoko is not None else ShokoStub(shoko_files)
    sonarr_stub = sonarr_stub or SonarrStub(sonarr_files)
    return DaemonDeps(
        settings=settings,
        store=store,
        mapping_store=mapping_store,
        sonarr=endpoint_for(sonarr_stub.handler, SONARR_BASE),
        shoko=endpoint_for(shoko_stub.handler, SHOKO_BASE),
        mapping_source=source_endpoint(source_handler(xml)),
        monotonic=monotonic,
        advance=advance,
        jitter=jitter,
        pid=4242,
        attestation_valid=(
            attestation_valid if attestation_valid is not None else lambda: attestation
        ),
        service_authorized_local=lambda: True,
        blocked_over=blocked_over,
        verification_timeout_seconds=verification_timeout,
        poll_interval_seconds=poll_interval,
        max_verify_polls=20,
        max_submissions=max_submissions,
        max_cycle_seconds=max_cycle_seconds,
        churn_limit=churn_limit,
        max_recovery_rows_per_cycle=max_recovery_rows_per_cycle,
        now=now,
        shutdown_requested=shutdown_requested,
    )


def activate_mapping(tmp_path: Path, xml: str) -> None:
    seed_mapping(tmp_path / "mappings", xml, T0)


def open_tmp_store(tmp_path: Path) -> StateStore:
    store = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
    store.set_meta("budget_last_cycle_key", "cycle-1")
    return store


@pytest.mark.parametrize("service_mode", [False, True])
def test_unavailable_second_target_read_defers_daemon_and_service_without_send_intent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, service_mode: bool
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [], T0)
    shoko = ShokoStub([file])
    target_path = f"/api/v3/Series/AniDB/{ANIME_ID}/Episode"
    shoko.target_read_failures[target_path] = (2, 503)
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=mapping_xml(),
        monotonic=lambda: 0.0,
        advance=lambda _seconds: None,
        shoko=shoko,
    )
    if service_mode:
        deps = replace(
            deps,
            service_mode=True,
            service_authorized=lambda: True,
            service_authorized_local=lambda: True,
        )
    policy = RunPolicy.SERVICE if service_mode else RunPolicy.NEW_ARRIVAL
    try:
        first = run_cycle(
            deps,
            now_iso=T0,
            mode=CycleMode.APPLY,
            policy=policy,
            cycle_key="target-unavailable-first",
        )

        assert first.counts.submitted == 0
        assert first.deferred_planned == 1
        assert shoko.target_read_counts[target_path] == 2
        assert any("preflight_deferred" in reason for reason in first.reasons), first.reasons
        assert any("target_unavailable" in reason for reason in first.reasons), first.reasons
        assert any(
            "retry" in reason.lower() and "no post" in reason.lower() for reason in first.reasons
        )
        assert "synthetic-api-key-must-not-leak" not in repr(first)
        assert shoko.posts == []
        record = store.get_file(file.shoko_id)
        assert record is not None
        assert record.state is FileState.PLANNED
        assert store.connection.execute("SELECT COUNT(*) FROM sends").fetchone()[0] == 0
        budget = store.budget_view()
        assert budget.pending_file_requests == 0
        assert budget.pending_episode_jobs == 0
        assert budget.rolling_new_submissions == 0

        shoko.target_read_failures.clear()
        second = run_cycle(
            deps,
            now_iso=T1,
            mode=CycleMode.APPLY,
            policy=policy,
            cycle_key="target-unavailable-recovery",
        )

        assert second.counts.submitted == 1
        assert shoko.posts == [{"EpisodeIDs": [SHOKO_EP]}]
        record = store.get_file(file.shoko_id)
        assert record is not None
        assert record.state is FileState.VERIFIED
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()


def _two_target_file(hard_first: bool = True) -> tuple[SpecFile, int, int]:
    hard_id, soft_id = (ANIDB_EP, ANIDB_EP + 1) if hard_first else (ANIDB_EP + 1, ANIDB_EP)
    episode_ids = (hard_id, soft_id)
    file = SpecFile(
        series_id=1,
        file_id=10,
        shoko_id=500,
        episodes=(
            (1, 1, episode_ids[0], SHOKO_EP + episode_ids[0] - ANIDB_EP),
            (1, 2, episode_ids[1], SHOKO_EP + episode_ids[1] - ANIDB_EP),
        ),
    )
    return file, hard_id, soft_id


@pytest.mark.parametrize("service_mode", [False, True])
@pytest.mark.parametrize("hard_first", [True, False])
@pytest.mark.parametrize(
    ("hard_kind", "soft_status", "hard_reason"),
    [("drift", 503, "target_drift"), ("malformed", 404, "malformed")],
)
def test_mixed_preflight_reasons_quarantine_hard_failure_in_daemon_and_service(  # noqa: PLR0915
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    service_mode: bool,
    hard_first: bool,
    hard_kind: str,
    soft_status: int,
    hard_reason: str,
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file, hard_target_id, soft_target_id = _two_target_file(hard_first)
    xml = mapping_xml().replace(";1-1;", ";1-1;2-2;")
    activate_mapping(tmp_path, xml)
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [], T0)
    shoko = ShokoStub([file])
    hard_path = f"/api/v3/Episode/AniDB/{hard_target_id}/Episode"
    soft_path = f"/api/v3/Episode/AniDB/{soft_target_id}/Episode"
    target_reads: dict[str, int] = {}

    def drift_frozen_target_on_preflight(path: str) -> None:
        if path == hard_path:
            target_reads[path] = target_reads.get(path, 0) + 1
            if target_reads[path] == 2:
                expected = SHOKO_EP + hard_target_id - ANIDB_EP
                shoko.target_episode_overrides[hard_target_id] = expected + 222

    if hard_kind == "drift":
        shoko.on_get = drift_frozen_target_on_preflight
        shoko.target_read_failures[soft_path] = (2, soft_status)
    else:
        shoko.target_read_failures[hard_path] = (2, 200)
        shoko.target_read_failures[soft_path] = (2, soft_status)
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=xml,
        monotonic=lambda: 0.0,
        advance=lambda _seconds: None,
        shoko=shoko,
    )
    if service_mode:
        deps = replace(
            deps,
            service_mode=True,
            service_authorized=lambda: True,
            service_authorized_local=lambda: True,
        )
    policy = RunPolicy.SERVICE if service_mode else RunPolicy.NEW_ARRIVAL
    try:
        first = run_cycle(
            deps,
            now_iso=T0,
            mode=CycleMode.APPLY,
            policy=policy,
            cycle_key="mixed-hard-soft-first",
        )
        record = store.get_file(file.shoko_id)
        assert first.counts.submitted == 0
        assert first.deferred_planned == 0
        assert any(f"preflight_{hard_reason}" in reason for reason in first.reasons), first.reasons
        assert shoko.posts == []
        assert record is not None
        assert record.state is FileState.BLOCKED_SAFETY
        assert record.state_detail.startswith(f"preflight_quarantine:{hard_reason}")
        target_snapshot = store.get_meta(f"file_mapping_targets:v1:{file.shoko_id}")
        assert target_snapshot is not None
        assert store.connection.execute("SELECT COUNT(*) FROM sends").fetchone()[0] == 0
        budget = store.budget_view()
        assert budget.pending_file_requests == 0
        assert budget.pending_episode_jobs == 0
        assert budget.rolling_new_submissions == 0

        store.close()
        store = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
        deps = replace(deps, store=store)
        shoko.on_get = None
        shoko.target_read_failures.clear()
        shoko.target_episode_overrides.clear()
        second = run_cycle(
            deps,
            now_iso=T1,
            mode=CycleMode.APPLY,
            policy=policy,
            cycle_key="mixed-hard-soft-healthy-restart",
        )
        record = store.get_file(file.shoko_id)
        assert second.counts.submitted == 0
        assert shoko.posts == []
        assert record is not None
        assert record.state is FileState.BLOCKED_SAFETY
        assert record.state_detail.startswith(f"preflight_quarantine:{hard_reason}")
        assert store.get_meta(f"file_mapping_targets:v1:{file.shoko_id}") == target_snapshot
        assert store.connection.execute("SELECT COUNT(*) FROM sends").fetchone()[0] == 0
        budget = store.budget_view()
        assert budget.pending_file_requests == 0
        assert budget.pending_episode_jobs == 0
        assert budget.rolling_new_submissions == 0
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()


@pytest.mark.parametrize("service_mode", [False, True])
def test_multiple_target_all_soft_preflight_defers_and_recovers_after_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, service_mode: bool
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file, _hard_target_id, _soft_target_id = _two_target_file()
    xml = mapping_xml().replace(";1-1;", ";1-1;2-2;")
    activate_mapping(tmp_path, xml)
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [], T0)
    shoko = ShokoStub([file])
    target_paths = [
        f"/api/v3/Episode/AniDB/{episode_id}/Episode" for episode_id in (ANIDB_EP, ANIDB_EP + 1)
    ]
    shoko.target_read_failures = {path: (2, 503) for path in target_paths}
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=xml,
        monotonic=lambda: 0.0,
        advance=lambda _seconds: None,
        shoko=shoko,
    )
    if service_mode:
        deps = replace(
            deps,
            service_mode=True,
            service_authorized=lambda: True,
            service_authorized_local=lambda: True,
        )
    policy = RunPolicy.SERVICE if service_mode else RunPolicy.NEW_ARRIVAL
    try:
        first = run_cycle(
            deps,
            now_iso=T0,
            mode=CycleMode.APPLY,
            policy=policy,
            cycle_key="multiple-soft-first",
        )
        record = store.get_file(file.shoko_id)
        assert first.counts.submitted == 0
        assert first.deferred_planned == 1
        assert record is not None
        assert record.state is FileState.PLANNED
        assert shoko.posts == []
        assert store.connection.execute("SELECT COUNT(*) FROM sends").fetchone()[0] == 0
        budget = store.budget_view()
        assert budget.pending_file_requests == 0
        assert budget.pending_episode_jobs == 0
        assert budget.rolling_new_submissions == 0

        store.close()
        store = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
        deps = replace(deps, store=store)
        shoko.target_read_failures.clear()
        second = run_cycle(
            deps,
            now_iso=T1,
            mode=CycleMode.APPLY,
            policy=policy,
            cycle_key="multiple-soft-recovered",
        )
        record = store.get_file(file.shoko_id)
        assert second.counts.submitted == 1
        assert second.counts.verified == 1
        assert shoko.posts == [{"EpisodeIDs": [SHOKO_EP, SHOKO_EP + 1]}]
        assert record is not None
        assert record.state is FileState.VERIFIED
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()


def test_multiple_target_all_hard_preflight_quarantines_without_partial_link(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file, _hard_target_id, _soft_target_id = _two_target_file()
    xml = mapping_xml().replace(";1-1;", ";1-1;2-2;")
    activate_mapping(tmp_path, xml)
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [], T0)
    shoko = ShokoStub([file])
    target_reads: dict[str, int] = {}
    target_paths = {
        f"/api/v3/Episode/AniDB/{episode_id}/Episode": episode_id
        for episode_id in (ANIDB_EP, ANIDB_EP + 1)
    }

    def drift_each_target_on_preflight(path: str) -> None:
        episode_id = target_paths.get(path)
        if episode_id is None:
            return
        target_reads[path] = target_reads.get(path, 0) + 1
        if target_reads[path] == 2:
            expected = SHOKO_EP + episode_id - ANIDB_EP
            shoko.target_episode_overrides[episode_id] = expected + 222

    shoko.on_get = drift_each_target_on_preflight
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=xml,
        monotonic=lambda: 0.0,
        advance=lambda _seconds: None,
        shoko=shoko,
    )
    try:
        report = run_cycle(
            deps,
            now_iso=T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="multiple-hard",
        )
        record = store.get_file(file.shoko_id)
        assert report.counts.submitted == 0
        assert report.deferred_planned == 0
        assert any("preflight_target_drift" in reason for reason in report.reasons)
        assert shoko.posts == []
        assert record is not None
        assert record.state is FileState.BLOCKED_SAFETY
        assert record.state_detail.startswith("preflight_quarantine:target_drift")
        assert store.connection.execute("SELECT COUNT(*) FROM sends").fetchone()[0] == 0
        budget = store.budget_view()
        assert budget.pending_file_requests == 0
        assert budget.pending_episode_jobs == 0
        assert budget.rolling_new_submissions == 0
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()


# ------------------------------------------------------------------ gate: first-run backlog


def test_first_run_seven_k_backlog_applies_zero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    files = [
        SpecFile(series_id=index % 70 + 1, file_id=index, shoko_id=10_000 + index)
        for index in range(1, FIRST_RUN_COUNT + 1)
    ]
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=files,
            shoko_files=files,
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
        )
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
    finally:
        store.close()
    assert report.counts.submitted == 0
    assert report.counts.new_arrival == 0
    assert report.counts.backlog == FIRST_RUN_COUNT
    assert report.mode is CycleMode.APPLY


def test_first_run_zero_posts_shoko(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    files = [
        SpecFile(series_id=index % 70 + 1, file_id=index, shoko_id=10_000 + index)
        for index in range(1, 500)
    ]
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    shoko = ShokoStub(files)
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=files,
            shoko_files=files,
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
    finally:
        store.close()
    assert shoko.posts_count() == 0


# ------------------------------------------------------------------ gate: new arrival single POST


def test_new_arrival_submits_exactly_one_and_verifies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    new_file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    # Baseline contains a DIFFERENT identity so the discovered file is a new arrival.
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([new_file])
    sleeper = Sleeper()
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[new_file],
            shoko_files=[new_file],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO, MONO_ZERO]),
            advance=sleeper.advance,
            shoko=shoko,
        )
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
        record = store.get_file(500)
    finally:
        store.close()
    assert shoko.posts_count() == 1
    assert report.counts.submitted == 1
    assert record is not None
    assert record.state is FileState.VERIFIED
    assert report.counts.verified == 1


def test_service_apply_uses_current_onboarding_consent_without_legacy_gates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SHOKO_LINKER_ALLOW_APPLY", raising=False)
    new_file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([new_file])
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[new_file],
            shoko_files=[new_file],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO, MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            attestation=False,
        )
        deps = replace(deps, service_mode=True, service_authorized=lambda: True)
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="svc"
        )
    finally:
        store.close()
    assert shoko.posts_count() == 1
    assert report.counts.verified == 1


def test_service_apply_refuses_without_current_onboarding_consent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    new_file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([new_file])
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[new_file],
            shoko_files=[new_file],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            attestation=True,
        )
        deps = replace(deps, service_mode=True)
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="svc"
        )
    finally:
        store.close()
    assert shoko.posts_count() == 0
    assert "onboarding_not_authorized" in report.reasons


def test_service_rechecks_onboarding_after_preflight_before_post(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SHOKO_LINKER_ALLOW_APPLY", raising=False)
    new_file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([new_file])
    authorization = {"current": True}

    def revoke_during_preflight(path: str) -> None:
        if path.endswith("/File/500"):
            authorization["current"] = False

    shoko.on_get = revoke_during_preflight
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[new_file],
            shoko_files=[new_file],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            attestation=False,
        )
        deps = replace(
            deps,
            service_mode=True,
            service_authorized=lambda: authorization["current"],
            service_authorized_local=lambda: authorization["current"],
        )
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="svc"
        )
    finally:
        store.close()
    assert shoko.posts_count() == 0
    assert any("service_authorization_changed" in reason for reason in report.reasons)


def test_service_apply_without_local_guard_fails_closed_after_final_shoko_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([file])
    final_file_read = False
    deps_ref: list[DaemonDeps] = []

    daemon = cast(Any, daemon_module)
    original_preflight_file = daemon.preflight_file

    def observe_final_read(*args: Any, **kwargs: Any) -> Any:
        nonlocal final_file_read
        verdict = original_preflight_file(*args, **kwargs)
        final_file_read = True
        return verdict

    monkeypatch.setattr(daemon, "preflight_file", observe_final_read)

    def remote_authorization() -> bool:
        if final_file_read:
            # Simulate a remote setup refresh that issues a Sonarr GET and discovers a new
            # manual association after the protected-link read. A remote fallback here would
            # authorize a POST using a stale Shoko snapshot.
            sonarr = deps_ref[0].sonarr
            sonarr.client.get("api/v3/series", headers=sonarr.headers_factory())
            shoko.link_unrelated(500)
        return True

    try:
        sonarr_stub = SonarrStub([file])
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[file],
            shoko_files=[file],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            sonarr_stub=sonarr_stub,
        )
        deps_ref.append(deps)
        # Deliberately omit service_authorized_local: applying must fail closed rather than
        # repeat a remote authorization check after the final protected-link GET.
        deps = replace(
            deps,
            service_mode=True,
            service_authorized=remote_authorization,
            service_authorized_local=None,
        )
        report = run_cycle(
            deps,
            now_iso=T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="missing-local-guard",
        )
    finally:
        store.close()

    assert final_file_read
    assert shoko.posts == []
    assert report.counts.submitted == 0
    assert "service_authorization_changed" in report.reasons


def test_manual_link_added_during_sonarr_source_check_blocks_daemon_post(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The final Shoko association read follows Sonarr's slower current-source checks."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([file])
    sonarr = SonarrStub([file])
    episode_reads = 0

    def add_manual_association(request: httpx.Request) -> None:
        nonlocal episode_reads
        if request.url.path == "/api/v3/episode" and request.url.params.get("seriesId") == "1":
            episode_reads += 1
            if episode_reads == 2:
                shoko.link_unrelated(500)

    sonarr.on_get = add_manual_association
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[file],
            shoko_files=[file],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            sonarr_stub=sonarr,
        )
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="manual"
        )
    finally:
        store.close()
    assert episode_reads >= 2
    assert shoko.posts == []
    assert report.counts.submitted == 0
    assert any("existing_link" in reason for reason in report.reasons)


@pytest.mark.parametrize(
    "source_failure", ["changed", "episodes_unavailable", "episode_file_unavailable"]
)
def test_source_episode_change_during_shoko_preflight_is_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, source_failure: str
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    original = SpecFile(series_id=1, file_id=10, shoko_id=500)
    current_sonarr_files = [original]
    sonarr = SonarrStub(current_sonarr_files)
    xml = mapping_xml().replace(";1-1;", ";1-1;2-2;")
    activate_mapping(tmp_path, xml)
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([original])

    def change_sonarr_during_preflight(path: str) -> None:
        if path.startswith("/api/v3/Episode/AniDB/"):
            if source_failure == "changed":
                current_sonarr_files[:] = [
                    replace(original, episodes=((1, 2, ANIDB_EP, SHOKO_EP),))
                ]
            elif source_failure == "episodes_unavailable":
                sonarr.episodes_status[1] = 503
            else:
                sonarr.files_status[1] = 503

    shoko.on_get = change_sonarr_during_preflight
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=current_sonarr_files,
        shoko_files=[original],
        xml=xml,
        monotonic=Clock([MONO_ZERO]),
        advance=Sleeper().advance,
        shoko=shoko,
        sonarr_stub=sonarr,
        max_submissions=1,
    )
    try:
        report = run_cycle(
            deps,
            now_iso=T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="sonarr-source-drift",
        )
        record = store.get_file(500)
        frozen_targets = store.get_meta("file_mapping_targets:v1:500")
        assert report.counts.submitted == 0
        assert shoko.posts == []
        assert record is not None
        assert frozen_targets is not None
        frozen_document = json.loads(frozen_targets)
        assert frozen_document == {
            json.dumps((TVDB_SERIES, 1, 1, None, ANIME_ID, 1, "Episode")): [ANIDB_EP, SHOKO_EP]
        }
        if source_failure == "changed":
            assert record.state is FileState.CONFLICT
            assert record.state_detail == "sonarr_source_changed"
            assert store.mapping_target_quarantined(500)
            assert any("sonarr_source_changed" in reason for reason in report.reasons)
        else:
            assert record.state is FileState.PLANNED
            assert not store.mapping_target_quarantined(500)
            assert any("sonarr_source_unavailable" in reason for reason in report.reasons)
    finally:
        deps.sonarr.client.close()
        deps.shoko.client.close()
        deps.mapping_source.client.close()
        store.close()


@pytest.mark.parametrize(
    "series_change",
    [
        "standard",
        "daily",
        "unknown",
        "tvdb_id",
        "root",
        "path",
        "series_id",
        "unavailable",
        "malformed",
    ],
)
def test_current_sonarr_series_identity_is_revalidated_before_final_shoko_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, series_change: str
) -> None:
    """A still-authorized root and a second anime cannot mask candidate series drift."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    candidate = SpecFile(series_id=1, file_id=10, shoko_id=500)
    current_sonarr_files = [candidate]

    class MutableSeriesSonarrStub(SonarrStub):
        series_reads = 0
        candidate_metadata: ClassVar[dict[str, Any]] = {}
        candidate_path_changed = False
        malformed_candidate = False

        def handler(self, request: httpx.Request) -> httpx.Response:
            if request.url.path == "/api/v3/series":
                self.series_reads += 1
                if self.series_reads == 2:
                    if series_change in {"standard", "daily", "unknown"}:
                        self.candidate_metadata["seriesType"] = {
                            "standard": "standard",
                            "daily": "daily",
                            "unknown": None,
                        }[series_change]
                    elif series_change == "tvdb_id":
                        self.candidate_metadata["tvdbId"] = 999_999
                    elif series_change == "root":
                        self.candidate_metadata["rootFolderPath"] = f"{SONARR_ROOT}-old/series1"
                    elif series_change == "series_id":
                        self.candidate_metadata["id"] = 77
                    elif series_change == "path":
                        self.candidate_path_changed = True
                    elif series_change == "unavailable":
                        self.requests.append(request)
                        if self.on_get is not None:
                            self.on_get(request)
                        return httpx.Response(503, json={"message": "injected"}, request=request)
                    elif series_change == "malformed":
                        self.malformed_candidate = True
            return super().handler(request)

        def _series(self) -> list[dict[str, Any]]:
            rows = super()._series()
            row = next(item for item in rows if item["id"] == 1)
            row.update(self.candidate_metadata)
            if self.malformed_candidate:
                row["id"] = "1"
            # This positively identified anime shares the authorized root, but is not the
            # candidate's frozen Sonarr/TVDB identity and must never be substituted for it.
            rows.append(
                {
                    "id": 2,
                    "title": "Unrelated anime",
                    "rootFolderPath": f"{SONARR_ROOT}/other-anime",
                    "seriesType": "anime",
                    "tvdbId": 999_999,
                }
            )
            return rows

        def _files(self, series_id: int) -> list[dict[str, Any]]:
            rows = super()._files(series_id)
            if self.candidate_path_changed and series_id == 1 and rows:
                rows[0]["path"] = f"{SONARR_ROOT}/series1/replaced.mkv"
            return rows

    sonarr = MutableSeriesSonarrStub(current_sonarr_files)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([candidate])
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=current_sonarr_files,
        shoko_files=[candidate],
        xml=mapping_xml(),
        monotonic=Clock([MONO_ZERO]),
        advance=Sleeper().advance,
        shoko=shoko,
        sonarr_stub=sonarr,
    )
    try:
        report = run_cycle(
            deps,
            now_iso=T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key=f"series-{series_change}",
        )
        record = store.get_file(candidate.shoko_id)
        final_shoko_reads = [
            request
            for request in shoko.requests
            if request.method == "GET" and request.url.path == "/api/v3/File/500"
        ]
        frozen_targets = store.get_meta("file_mapping_targets:v1:500")
        quarantined = store.mapping_target_quarantined(candidate.shoko_id)
    finally:
        deps.sonarr.client.close()
        deps.shoko.client.close()
        deps.mapping_source.client.close()
        store.close()

    assert report.counts.submitted == 0
    assert shoko.posts == []
    assert final_shoko_reads == []
    assert record is not None
    assert frozen_targets is not None
    if series_change in {"unavailable", "malformed"}:
        assert record.state is FileState.PLANNED
        assert not quarantined
        assert any("sonarr_source_unavailable" in reason for reason in report.reasons)
    else:
        assert record.state is FileState.CONFLICT
        assert quarantined
        assert any("sonarr_source_changed" in reason for reason in report.reasons)


def test_ninety_day_inventory_cache_cannot_complete_before_expired_series_is_refetched(  # noqa: PLR0915
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    files = [
        SpecFile(series_id=series_id, file_id=10 + series_id, shoko_id=500 + series_id)
        for series_id in (1, 2, 3, 4)
    ]
    current_sonarr_files = list(files)
    sonarr = SonarrStub(current_sonarr_files)
    shoko = ShokoStub(files)
    elapsed = [0.0]
    wall_time = [T0]
    mapping = mapping_xml().replace(";1-1;", ";1-1;2-2;")
    settings = make_settings(tmp_path, sync_extra={"full_reconcile_interval_hours": 24})

    def advance_for_child_gets(request: httpx.Request) -> None:
        if request.url.path in ("/api/v3/episode", "/api/v3/episodefile"):
            elapsed[0] += 1.0

    sonarr.on_get = advance_for_child_gets
    activate_mapping(tmp_path, mapping)
    original_targets: str | None = None
    series_one_child_gets_before_expiry = 0

    def child_gets(series_id: int) -> int:
        return sum(
            request.url.path in ("/api/v3/episode", "/api/v3/episodefile")
            and request.url.params.get("seriesId") == str(series_id)
            for request in sonarr.requests
        )

    def cache_snapshot() -> tuple[int, frozenset[int]]:
        store = open_tmp_store(tmp_path)
        try:
            _manifest, cursor, swept, _fragments = store.load_sonarr_inventory_cache()
            return cursor, swept
        finally:
            store.close()

    def mapping_snapshot(file_id: int) -> str | None:
        store = open_tmp_store(tmp_path)
        try:
            return store.get_meta(f"file_mapping_targets:v1:{file_id}")
        finally:
            store.close()

    def run_slice(cycle: int, *, max_cycle_seconds: float, authorized: bool) -> Any:
        elapsed[0] = 0.0
        store = open_tmp_store(tmp_path)
        if cycle == 0:
            seed_baseline(store, files)
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=current_sonarr_files,
            sonarr_stub=sonarr,
            shoko_files=files,
            shoko=shoko,
            xml=mapping,
            settings=settings,
            monotonic=lambda: elapsed[0],
            advance=Sleeper().advance,
            now=lambda: wall_time[0],
            max_cycle_seconds=max_cycle_seconds,
            max_submissions=1,
        )
        deps = replace(
            deps,
            service_mode=True,
            service_authorized=lambda: authorized,
        )
        try:
            return run_cycle(
                deps,
                now_iso=wall_time[0],
                mode=CycleMode.APPLY,
                policy=RunPolicy.SERVICE,
                cycle_key=f"inventory-age-{cycle}",
            )
        finally:
            deps.sonarr.client.close()
            deps.shoko.client.close()
            deps.mapping_source.client.close()
            store.close()

    monkeypatch.delenv("SHOKO_LINKER_ALLOW_APPLY", raising=False)
    initial = run_slice(0, max_cycle_seconds=100.0, authorized=False)
    assert initial.reconciliation_ready
    original_targets = mapping_snapshot(501)
    assert original_targets is not None

    wall_time[0] = (_parse(T0) + timedelta(minutes=5)).isoformat()
    partial = run_slice(1, max_cycle_seconds=4.0, authorized=False)
    assert not partial.reconciliation_ready
    cursor, swept = cache_snapshot()
    assert cursor == 1
    assert swept == frozenset({1})

    current_sonarr_files[0] = replace(files[0], episodes=((1, 2, ANIDB_EP, SHOKO_EP),))
    series_one_child_gets_before_expiry = child_gets(1)
    stale_start = _parse(T0) + timedelta(days=90)
    reports = []
    for offset in range(4):
        wall_time[0] = (stale_start + timedelta(minutes=5 * offset)).isoformat()
        reports.append(
            run_slice(
                2 + offset,
                max_cycle_seconds=5.0 if offset == 3 else 4.0,
                authorized=True,
            )
        )
        if offset < 3:
            assert not reports[-1].reconciliation_ready
            assert reports[-1].counts.submitted == 0
            assert child_gets(1) == series_one_child_gets_before_expiry

    assert reports[-1].reconciliation_ready
    assert child_gets(1) > series_one_child_gets_before_expiry
    assert reports[-1].counts.submitted == 1
    assert [
        int(request.url.path.split("/")[4])
        for request in shoko.requests
        if request.method == "POST" and request.url.path.endswith("/Link")
    ] == [502]
    store = open_tmp_store(tmp_path)
    try:
        stale_record = store.get_file(501)
        assert stale_record is not None
        assert stale_record.state is FileState.CONFLICT
        assert store.mapping_target_quarantined(501)
        assert store.get_meta("file_mapping_targets:v1:501") == original_targets
    finally:
        store.close()


@pytest.mark.parametrize(
    "fault",
    [None, "page_failure", "page_change", "endpoint_change", "folder_change", "cache_expired"],
)
def test_shoko_discovery_resumes_across_restarts_within_cycle_budget(  # noqa: PLR0915
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str | None
) -> None:
    from shoko_linker.shoko import ShokoDiscovery, collect_shoko_discovery

    files = [
        SpecFile(
            series_id=1,
            file_id=10 + index,
            shoko_id=500 + index,
            episodes=((1, index + 1, ANIDB_EP + index, SHOKO_EP + index),),
        )
        for index in range(13)
    ]
    elapsed = [0.0]
    wall_time = [T0]
    xml = (
        "<anime-list><anime anidbid='1234' tvdbid='267440' defaulttvdbseason='1'>"
        "<mapping-list><mapping anidbseason='1' tvdbseason='1' start='1' end='13' "
        "offset='0'/></mapping-list></anime></anime-list>"
    )

    class PagedShokoStub(ShokoStub):
        def __init__(self, files: list[SpecFile]) -> None:
            super().__init__(files)
            self.fail_pages: set[int] = set()
            self.changed_page_one = False
            self.changed_folder = False

        def _folder(self) -> dict[str, Any]:
            result = super()._folder()
            if self.changed_folder:
                result["Name"] = "Sonarr updated"
            return result

        def _listing(self, request: httpx.Request) -> httpx.Response:
            page = int(request.url.params["page"])
            page_size = int(request.url.params["pageSize"])
            if page in self.fail_pages:
                return httpx.Response(503, request=request)
            rows = [self._file_row(file_id, xrefs=[]) for file_id in sorted(self.files)]
            rows = rows[(page - 1) * page_size : page * page_size]
            if page == 1 and self.changed_page_one:
                rows[0] = {**rows[0], "Imported": "2026-01-02T00:00:00"}
            return self._json(request, {"Total": len(self.files), "List": rows})

    shoko = PagedShokoStub(files)
    shoko.on_get = lambda path: elapsed.__setitem__(
        0, elapsed[0] + (25.0 if path == "/api/v3/File" else 0.0)
    )
    sonarr = SonarrStub(files)
    activate_mapping(tmp_path, xml)
    monkeypatch.delenv("SHOKO_LINKER_ALLOW_APPLY", raising=False)

    def one_row_per_page(endpoint: ApiEndpoint, **kwargs: Any) -> ShokoDiscovery:
        kwargs.setdefault("page_size", 1)
        return collect_shoko_discovery(endpoint, **kwargs)

    monkeypatch.setattr(daemon_module, "collect_shoko_discovery", one_row_per_page)

    def run_slice(cycle: int, store: StateStore) -> Any:
        elapsed[0] = 0.0
        age_minutes = 25 * 60 if fault == "cache_expired" and cycle > 0 else 5 * cycle
        wall_time[0] = (_parse(T0) + timedelta(minutes=age_minutes)).isoformat()
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=files,
            sonarr_stub=sonarr,
            shoko_files=files,
            shoko=shoko,
            xml=xml,
            monotonic=lambda: elapsed[0],
            advance=Sleeper().advance,
            now=lambda: wall_time[0],
            max_cycle_seconds=120.0,
        )
        if fault == "endpoint_change" and cycle > 0:
            deps.shoko.client.base_url = httpx.URL("http://shoko-updated:8111/")
        deps = replace(deps, service_mode=True, service_authorized=lambda: True)
        try:
            report = run_cycle(
                deps,
                now_iso=wall_time[0],
                mode=CycleMode.APPLY,
                policy=RunPolicy.SERVICE,
                cycle_key=f"shoko-pages-{cycle}",
            )
            assert elapsed[0] <= 120.0
            return report
        finally:
            deps.sonarr.client.close()
            deps.shoko.client.close()
            deps.mapping_source.client.close()

    store = open_tmp_store(tmp_path)
    seed_baseline(store, files)
    first = run_slice(0, store)
    assert not first.reconciliation_ready
    assert first.counts.submitted == 0
    assert shoko.posts == []
    store.close()

    if fault is not None:
        store = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
        if fault == "page_failure":
            shoko.fail_pages = {4}
        elif fault == "page_change":
            shoko.changed_page_one = True
        else:
            shoko.fail_pages = {2}
            shoko.changed_folder = fault == "folder_change"
        request_start = len(shoko.requests)
        second = run_slice(1, store)
        second_pages = [
            request.url.params.get("page")
            for request in shoko.requests[request_start:]
            if request.url.path == "/api/v3/File"
        ]
        store.close()
        assert not second.reconciliation_ready
        assert second.counts.submitted == 0
        assert shoko.posts == []
        if fault in {"page_failure", "endpoint_change", "folder_change", "cache_expired"}:
            assert any(
                "shoko_snapshot_incomplete:transport_failure" in item for item in second.reasons
            )
            expected_failed_page = "4" if fault == "page_failure" else "2"
            assert expected_failed_page in second_pages
        else:
            assert any(
                item.startswith("shoko_snapshot_incomplete:") for item in second.reasons
            ), second.reasons
        return

    report = first
    for cycle in range(1, 8):
        store = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
        report = run_slice(cycle, store)
        store.close()
        if report.reconciliation_ready:
            break
        assert report.counts.submitted == 0
        assert shoko.posts == []

    assert report.reconciliation_ready
    store = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
    record = store.get_file(500)
    store.close()
    assert report.counts.submitted == 1, (
        f"counts={report.counts}; reasons={report.reasons}; eligible={report.apply_eligible}; "
        f"suppressed={report.writes_suppressed}; exclusions={report.exclusion_counts}; "
        f"state={None if record is None else record.state}; "
        f"detail={None if record is None else record.state_detail}"
    )
    assert shoko.posts == [{"EpisodeIDs": [SHOKO_EP]}]
    posted_ids = [
        int(request.url.path.split("/")[4])
        for request in shoko.requests
        if request.method == "POST" and request.url.path.endswith("/Link")
    ]
    assert posted_ids == [500]
    assert elapsed[0] <= 120.0


@pytest.mark.parametrize("existing_state", [None, FileState.PLANNED, FileState.AMBIGUOUS])
def test_duplicate_shoko_correspondences_are_durable_skips_without_plans(
    tmp_path: Path, existing_state: FileState | None
) -> None:
    file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    duplicate = replace(file, shoko_id=501)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [file, duplicate])
    if existing_state is not None:
        store.record_file(
            500,
            sonarr_series_id=1,
            sonarr_file_id=10,
            sonarr_path=_sonarr_path(file),
            sonarr_size=FILE_SIZE,
            cohort=Cohort.BACKLOG,
            iso=T0,
            state=existing_state,
        )
        if existing_state is FileState.PLANNED:
            # A current planned row has durable destinations. Missing-snapshot
            # legacy plans are separately covered by upgrade quarantine regressions.
            store.remember_mapping_targets_bulk(
                {
                    500: {
                        json.dumps((TVDB_SERIES, 1, 1, None, ANIME_ID, 1, "Episode")): (
                            ANIDB_EP,
                            SHOKO_EP,
                        )
                    }
                }
            )

    class DuplicateShokoStub(ShokoStub):
        def _location(self, spec: SpecFile) -> dict[str, Any]:
            location = super()._location(spec)
            location["RelativePath"] = "series1/500.mkv"
            location["AbsolutePath"] = f"{SHOKO_ROOT}/series1/500.mkv"
            return location

    shoko = DuplicateShokoStub([file, duplicate])
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file, duplicate],
        xml=mapping_xml(),
        monotonic=Clock([0.0]),
        advance=lambda _: None,
        shoko=shoko,
    )
    deps = replace(deps, service_mode=True, service_authorized=lambda: True)
    try:
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="duplicates"
        )
        assert report.plans == {}
        assert shoko.posts == []
        for file_id in (500, 501):
            row = store.get_file(file_id)
            assert row is not None
            assert row.state is FileState.AMBIGUOUS
            assert "ambiguous_multiple_shoko_candidates" in row.state_detail
            assert "500, 501" in row.state_detail
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()


def test_size_mismatch_exclusion_is_durable_reported_and_retried_when_resolved(  # noqa: PLR0915
    tmp_path: Path,
) -> None:
    files = [
        SpecFile(series_id=1, file_id=10, shoko_id=500),
        SpecFile(series_id=1, file_id=11, shoko_id=501),
    ]

    class SizeMismatchShokoStub(ShokoStub):
        mismatched: ClassVar[set[int]] = {501}

        def _file_row(self, shoko_id: int, xrefs: list[dict[str, Any]]) -> dict[str, Any]:
            row = super()._file_row(shoko_id, xrefs)
            if shoko_id in self.mismatched:
                row["Size"] = FILE_SIZE + 1
            return row

    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, files)
    shoko = SizeMismatchShokoStub(files)
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=files,
        shoko_files=files,
        xml=mapping_xml(),
        monotonic=Clock([0.0]),
        advance=lambda _: None,
        shoko=shoko,
    )
    deps = replace(deps, service_mode=True, service_authorized=lambda: True)
    try:
        first = run_cycle(
            deps,
            now_iso=T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="size-mismatch",
        )
        assert first.counts.verified == 1
        assert shoko.posts == [{"EpisodeIDs": [777]}]
        excluded = store.get_file(501)
        assert excluded is not None
        assert excluded.state is FileState.AMBIGUOUS
        assert excluded.state_detail.startswith("source_exclusion:size_mismatch:")
        assert "skip:501:exclusion:size_mismatch:" in " ".join(first.reasons)
        assert first.exclusion_counts["size_mismatch"] == 1
        assert cycle_report_payload(first)["discovery_exclusions"]["counts"]["size_mismatch"] == 1

        summary = json.loads(store.get_meta(META_LAST_DISCOVERY_EXCLUSIONS) or "null")
        assert summary["counts"]["size_mismatch"] == 1
        store.close()
        store = open_tmp_store(tmp_path)
        excluded = store.get_file(501)
        assert excluded is not None
        assert excluded.state is FileState.AMBIGUOUS
        assert excluded.state_detail.startswith("source_exclusion:size_mismatch:")
        assert (
            json.loads(store.get_meta(META_LAST_DISCOVERY_EXCLUSIONS) or "null")["counts"][
                "size_mismatch"
            ]
            == 1
        )

        shoko.mismatched.clear()
        deps = replace(deps, store=store)
        second = run_cycle(
            deps,
            now_iso=T1,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="size-restored",
        )
        recovered = store.get_file(501)
        assert second.counts.verified == 2
        assert recovered is not None
        assert recovered.state is FileState.VERIFIED
        assert [request.url.path for request in shoko.requests if request.method == "POST"] == [
            "/api/v3/File/500/Link",
            "/api/v3/File/501/Link",
        ]

        verified_before = store.get_file(500)
        assert verified_before is not None
        shoko.mismatched.add(500)
        third = run_cycle(
            deps,
            now_iso=T2,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="verified-excluded",
        )
        verified_after = store.get_file(500)
        assert "skip:500:exclusion:size_mismatch:" in " ".join(third.reasons)
        assert verified_after is not None
        assert verified_after.state is FileState.VERIFIED
        assert verified_after.state_detail == verified_before.state_detail
        assert verified_after.last_verified_iso == verified_before.last_verified_iso
        assert len(shoko.posts) == 2
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()


def test_path_exclusion_recovers_after_path_is_corrected_without_posting_early(
    tmp_path: Path,
) -> None:
    file = SpecFile(series_id=1, file_id=10, shoko_id=501)

    class PathMismatchShokoStub(ShokoStub):
        bad_path = False

        def _location(self, source: SpecFile) -> dict[str, Any]:
            location = super()._location(source)
            if self.bad_path and source.shoko_id == 501:
                location["AbsolutePath"] = f"{SHOKO_ROOT}/different/501.mkv"
            return location

    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [file])
    shoko = PathMismatchShokoStub([file])
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=mapping_xml(),
        monotonic=Clock([0.0]),
        advance=lambda _: None,
        shoko=shoko,
    )
    deps = replace(deps, service_mode=True, service_authorized=lambda: True)
    try:
        planned = run_cycle(
            deps,
            now_iso=T0,
            mode=CycleMode.DRY_RUN,
            policy=RunPolicy.SERVICE,
            cycle_key="path-good-dry-run",
        )
        assert planned.plans
        assert shoko.posts == []

        shoko.bad_path = True
        excluded = run_cycle(
            deps,
            now_iso=T1,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="path-mismatch",
        )
        record = store.get_file(501)
        assert record is not None
        assert record.state is FileState.AMBIGUOUS
        assert record.state_detail.startswith("source_exclusion:location_path_mismatch:")
        assert "skip:501:exclusion:location_path_mismatch:" in " ".join(excluded.reasons)
        assert shoko.posts == []

        shoko.bad_path = False
        recovered = run_cycle(
            deps,
            now_iso=T2,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="path-restored",
        )
        record = store.get_file(501)
        assert recovered.counts.verified == 1
        assert record is not None
        assert record.state is FileState.VERIFIED
        assert shoko.posts == [{"EpisodeIDs": [777]}]
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()


def test_scope_exclusion_is_counted_separately_and_never_tracked_for_writes(
    tmp_path: Path,
) -> None:
    files = [
        SpecFile(series_id=1, file_id=10, shoko_id=500),
        SpecFile(series_id=2, file_id=20, shoko_id=501),
    ]
    settings = make_settings(tmp_path)
    settings = replace(settings, shoko=replace(settings.shoko, allowed_roots=("/anime/series1",)))
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, files)
    shoko = ShokoStub(files)
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=files,
        shoko_files=files,
        xml=mapping_xml(),
        monotonic=Clock([0.0]),
        advance=lambda _: None,
        shoko=shoko,
        settings=settings,
    )
    deps = replace(deps, service_mode=True, service_authorized=lambda: True)
    try:
        report = run_cycle(
            deps,
            now_iso=T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="scope-excluded",
        )
        assert report.exclusion_counts["outside_allowed_root"] == 1
        assert report.scope_excluded_count == 1
        assert "skip:501:exclusion:outside_allowed_root:" in " ".join(report.reasons)
        assert store.get_file(501) is None
        assert [request.url.path for request in shoko.requests if request.method == "POST"] == [
            "/api/v3/File/500/Link"
        ]
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()


@pytest.mark.parametrize("blocked_stage", ["preflight", "verification"])
def test_dashboard_pause_during_slow_service_request_prevents_next_post(  # noqa: PLR0915
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, blocked_stage: str
) -> None:
    monkeypatch.delenv("SHOKO_LINKER_ALLOW_APPLY", raising=False)
    new_file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    next_file = SpecFile(series_id=2, file_id=20, shoko_id=501)
    activate_mapping(tmp_path, mapping_xml())
    seed_store = open_tmp_store(tmp_path)
    seed_baseline(seed_store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    seed_store.close()
    shoko = ShokoStub([new_file, next_file])
    state_path = tmp_path / "state.sqlite3"
    slow_request_started = threading.Event()
    release_slow_request = threading.Event()

    def block_slow_request(path: str) -> None:
        expected_posts = 0 if blocked_stage == "preflight" else 1
        if (
            path.endswith("/File/500")
            and shoko.posts_count() == expected_posts
            and not slow_request_started.is_set()
        ):
            slow_request_started.set()
            if not release_slow_request.wait(timeout=5):
                raise TimeoutError("test Shoko request was not released")

    shoko.on_get = block_slow_request

    def currently_authorized() -> bool:
        controls = StateStore(state_path)
        try:
            return not controls.get_service_pause()[0]
        finally:
            controls.close()

    app = DashboardApplication(
        state_path,
        status_provider=lambda: DashboardStatus(
            "Sonarr and Shoko are connected.", 0, 1, 0, True, "Automatic linking is running."
        ),
    )
    server = create_dashboard_server("127.0.0.1", 0, app)
    base_url = f"http://127.0.0.1:{server.server_port}"
    setup = app.handle(
        "POST",
        "/setup",
        {
            "origin": base_url,
            "host": base_url.removeprefix("http://"),
            "content-type": "application/x-www-form-urlencoded",
        },
        urlencode(
            {"username": "library-admin", "password": "a sufficiently long password"}
        ).encode(),
    )
    assert setup.status == 303
    cookie = dict(setup.headers)["Set-Cookie"].split(";", 1)[0]
    status_page = app.handle("GET", "/status", {"cookie": cookie})
    csrf = re.search(r'name="csrf_token" value="([^"]+)"', status_page.body.decode())
    assert csrf is not None
    stop = threading.Event()
    reports: list[Any] = []
    failures: list[BaseException] = []

    def worker_tick() -> None:
        worker_store: StateStore | None = None
        try:
            worker_store = StateStore(state_path, budget_limits=WIDE_LIMITS)
            deps = build_deps(
                tmp_path,
                store=worker_store,
                sonarr_files=[new_file, next_file],
                shoko_files=[new_file, next_file],
                xml=mapping_xml(),
                monotonic=Clock([MONO_ZERO]),
                advance=Sleeper().advance,
                shoko=shoko,
                attestation=False,
            )
            deps = replace(
                deps,
                service_mode=True,
                service_authorized=currently_authorized,
                service_authorized_local=currently_authorized,
            )
            reports.append(
                run_cycle(
                    deps,
                    now_iso=T0,
                    mode=CycleMode.APPLY,
                    policy=RunPolicy.SERVICE,
                    cycle_key="svc",
                )
            )
            if blocked_stage == "verification":
                reports.append(
                    run_cycle(
                        deps,
                        now_iso=T1,
                        mode=CycleMode.APPLY,
                        policy=RunPolicy.SERVICE,
                        cycle_key="svc-next",
                    )
                )
        except BaseException as exc:
            failures.append(exc)
        finally:
            if worker_store is not None:
                worker_store.close()
            stop.set()

    runner = threading.Thread(
        target=run_dashboard_worker,
        kwargs={
            "server": server,
            "worker_tick": worker_tick,
            "stop_requested": stop.is_set,
            "monotonic": time.monotonic,
            "worker_interval_seconds": 60.0,
            "request_poll_seconds": 0.05,
        },
    )
    try:
        runner.start()
        assert slow_request_started.wait(timeout=3)
        with httpx.Client(timeout=2) as client:
            status = client.get(f"{base_url}/status", headers={"Cookie": cookie})
            assert status.status_code == 200
            paused = client.post(
                f"{base_url}/pause",
                data={"csrf_token": csrf.group(1)},
                headers={"Cookie": cookie, "Origin": base_url},
            )
            assert paused.status_code == 303
        controls = StateStore(state_path)
        try:
            assert controls.get_service_pause() == (True, "user_paused")
        finally:
            controls.close()
        release_slow_request.set()
        runner.join(timeout=5)
        assert not runner.is_alive()
    finally:
        stop.set()
        release_slow_request.set()
        runner.join(timeout=5)
        server.server_close()

    controls = StateStore(state_path)
    try:
        assert controls.get_service_pause() == (True, "user_paused")
    finally:
        controls.close()
    assert not failures
    assert reports
    expected_posts = 0 if blocked_stage == "preflight" else 1
    assert shoko.posts_count() == expected_posts
    if blocked_stage == "preflight":
        assert any("service_authorization_changed" in reason for reason in reports[0].reasons)
    else:
        assert reports[0].counts.verified == 1
        assert len(reports) == 2
        assert any("onboarding_not_authorized" in reason for reason in reports[1].reasons)


def test_service_policy_processes_backlog_after_onboarding_consent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SHOKO_LINKER_ALLOW_APPLY", raising=False)
    backlog = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [backlog])
    shoko = ShokoStub([backlog])
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[backlog],
            shoko_files=[backlog],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO, MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            attestation=False,
        )
        deps = replace(deps, service_mode=True, service_authorized=lambda: True)
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="svc"
        )
        record = store.get_file(backlog.shoko_id)
        canary = store.get_service_canary()
    finally:
        store.close()
    assert shoko.posts_count() == 1
    assert record is not None
    assert record.cohort is Cohort.BACKLOG
    assert record.state is FileState.VERIFIED
    assert report.counts.verified == 1
    assert canary == (backlog.shoko_id, "verified")


def test_new_arrival_policy_does_not_submit_backlog_even_in_service_mode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SHOKO_LINKER_ALLOW_APPLY", raising=False)
    backlog = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [backlog])
    shoko = ShokoStub([backlog])
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[backlog],
            shoko_files=[backlog],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            attestation=False,
        )
        deps = replace(deps, service_mode=True, service_authorized=lambda: True)
        run_cycle(
            deps,
            now_iso=T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="legacy-policy",
        )
        record = store.get_file(backlog.shoko_id)
    finally:
        store.close()
    assert shoko.posts_count() == 0
    assert record is not None
    assert record.cohort is Cohort.BACKLOG
    assert record.state is FileState.PLANNED


def test_service_canary_holds_expansion_until_async_verification_is_complete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SHOKO_LINKER_ALLOW_APPLY", raising=False)
    files = [
        SpecFile(series_id=1, file_id=10, shoko_id=500),
        SpecFile(series_id=1, file_id=11, shoko_id=501),
    ]
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, files)
    shoko = ShokoStub(files)
    shoko.link_on_post = False
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=files,
            shoko_files=files,
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        deps = replace(
            deps,
            service_mode=True,
            service_authorized=lambda: True,
            max_submissions=20,
            verification_timeout_seconds=10_000,
        )
        first = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="svc-1"
        )
        store.close()
        store = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=files,
            shoko_files=files,
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        deps = replace(
            deps,
            service_mode=True,
            service_authorized=lambda: True,
            max_submissions=20,
            verification_timeout_seconds=10_000,
        )
        second = run_cycle(
            deps, now_iso=T1, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="svc-2"
        )
        canary_id = store.get_service_canary()
        second_file = store.get_file(501)
    finally:
        store.close()
    assert shoko.posts_count() == 1
    assert first.counts.submitted == 1
    assert "service_canary_pending" in second.reasons
    assert canary_id == (500, "pending")
    assert second_file is not None
    assert second_file.state is FileState.PLANNED


def test_rejected_service_canary_retries_same_file_before_expansion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SHOKO_LINKER_ALLOW_APPLY", raising=False)
    files = [
        SpecFile(series_id=1, file_id=10, shoko_id=500),
        SpecFile(series_id=1, file_id=11, shoko_id=501),
    ]
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, files)
    shoko = ShokoStub(files)
    shoko.post_status = 400
    shoko.link_on_post = False
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=files,
            shoko_files=files,
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        deps = replace(
            deps,
            service_mode=True,
            service_authorized=lambda: True,
            max_submissions=20,
        )
        first = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="svc-1"
        )
        first_canary = store.get_service_canary()
        first_file = store.get_file(500)
        shoko.post_status = None
        shoko.link_on_post = True
        second = run_cycle(
            deps, now_iso=T1, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="svc-2"
        )
        canary = store.get_service_canary()
        second_file = store.get_file(500)
        untried_file = store.get_file(501)
        rolling_submissions = store.budget_view().rolling_new_submissions
    finally:
        store.close()
    assert first.counts.submitted == 1
    assert first_canary == (500, "pending")
    assert first_file is not None
    assert first_file.state is FileState.WAITING
    assert second.counts.submitted == 1, second.reasons
    assert shoko.posts_count() == 2
    posted_file_ids = [
        int(request.url.path.split("/")[4])
        for request in shoko.requests
        if request.method == "POST"
    ]
    assert posted_file_ids == [500, 500]
    assert canary == (500, "verified")
    assert second_file is not None
    assert second_file.state is FileState.VERIFIED
    assert untried_file is not None
    assert untried_file.state is FileState.PLANNED
    assert rolling_submissions == 2  # the definite rejection still consumes budget debt


def test_rejected_service_canary_is_prioritized_across_rotating_window_and_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SHOKO_LINKER_ALLOW_APPLY", raising=False)
    files = [
        SpecFile(
            series_id=1,
            file_id=10 + index,
            shoko_id=500 + index,
            episodes=((1, index + 1, ANIDB_EP + index, SHOKO_EP + index),),
        )
        for index in range(41)
    ]
    xml = (
        "<anime-list>"
        f"<anime anidbid='{ANIME_ID}' tvdbid='{TVDB_SERIES}' defaulttvdbseason='1'>"
        "<mapping-list><mapping anidbseason='1' tvdbseason='1' "
        "start='1' end='41' offset='0'/></mapping-list></anime></anime-list>"
    )
    activate_mapping(tmp_path, xml)
    store = open_tmp_store(tmp_path)
    seed_baseline(store, files)
    shoko = ShokoStub(files)
    shoko.post_status = 400
    shoko.link_on_post = False
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=files,
            shoko_files=files,
            xml=xml,
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            max_submissions=20,
        )
        deps = replace(deps, service_mode=True, service_authorized=lambda: True)
        first = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="svc-1"
        )
        first_canary = store.get_service_canary()
        first_record = store.get_file(500)
        assert first.counts.submitted == 1
        assert first_canary == (500, "pending")
        assert first_record is not None
        assert first_record.state is FileState.WAITING
        assert [
            int(request.url.path.split("/")[4])
            for request in shoko.requests
            if request.method == "POST"
        ] == [500]
    finally:
        store.close()

    # The canary ledger and resolution cursor survive restart; a definite rejection may be
    # retried once as that same file before the bounded cursor resumes the rest of the backlog.
    store = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
    shoko.post_status = None
    shoko.link_on_post = True
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=files,
            shoko_files=files,
            xml=xml,
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            max_submissions=20,
        )
        deps = replace(deps, service_mode=True, service_authorized=lambda: True)
        second = run_cycle(
            deps, now_iso=T1, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="svc-2"
        )
        canary = store.get_service_canary()
        retry_record = store.get_file(500)
        assert second.counts.submitted == 1
        assert canary == (500, "verified")
        assert retry_record is not None
        assert retry_record.state is FileState.VERIFIED
        assert [
            int(request.url.path.split("/")[4])
            for request in shoko.requests
            if request.method == "POST"
        ] == [500, 500]

        third = run_cycle(
            deps, now_iso=T2, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="svc-3"
        )
        beyond_window = store.get_file(540)
        assert beyond_window is not None
        assert beyond_window.state in {FileState.PLANNED, FileState.VERIFIED}
        assert third.reconciliation_ready
    finally:
        store.close()


def test_rejected_service_canary_retry_is_bounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SHOKO_LINKER_ALLOW_APPLY", raising=False)
    files = [
        SpecFile(series_id=1, file_id=10, shoko_id=500),
        SpecFile(series_id=1, file_id=11, shoko_id=501),
    ]
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, files)
    shoko = ShokoStub(files)
    shoko.post_status = 400
    shoko.link_on_post = False
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=files,
            shoko_files=files,
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        deps = replace(
            deps,
            service_mode=True,
            service_authorized=lambda: True,
            max_submissions=20,
        )
        first = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="svc-1"
        )
        second = run_cycle(
            deps, now_iso=T1, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="svc-2"
        )
        shoko.post_status = None
        shoko.link_on_post = True
        third = run_cycle(
            deps,
            now_iso=T2,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="svc-3",
        )
        canary = store.get_service_canary()
    finally:
        store.close()
    assert first.counts.submitted == second.counts.submitted == 1
    assert "service_canary_failed" in second.reasons
    assert third.counts.submitted == 0
    assert "service_canary_failed" in third.reasons
    assert shoko.posts_count() == 2
    assert canary == (500, "failed")


def test_service_rate_cap_ramps_and_survives_state_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SHOKO_LINKER_ALLOW_APPLY", raising=False)
    files = [SpecFile(series_id=1, file_id=index, shoko_id=500 + index) for index in range(10, 18)]
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, files)
    shoko = ShokoStub(files)
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=files,
            shoko_files=files,
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        deps = replace(
            deps,
            service_mode=True,
            service_authorized=lambda: True,
            max_submissions=20,
        )
        first = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="svc-1"
        )
        rate_after_canary = store.get_service_rate_state_json()
        store.close()
        store = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=files,
            shoko_files=files,
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        deps = replace(
            deps,
            service_mode=True,
            service_authorized=lambda: True,
            max_submissions=20,
        )
        second = run_cycle(
            deps, now_iso=T1, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="svc-2"
        )
        third = run_cycle(
            deps,
            now_iso="2026-01-01T00:20:00+00:00",
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="svc-3",
        )
        rate_after_third = store.get_service_rate_state_json()
        limits_after_service_cycles = store.budget_limits
    finally:
        store.close()
    assert first.counts.submitted == 1
    assert second.counts.submitted == 2
    assert third.counts.submitted == 4
    assert shoko.posts_count() == 7
    assert rate_after_canary is not None
    assert rate_after_third is not None
    assert limits_after_service_cycles == WIDE_LIMITS
    assert json.loads(rate_after_canary)["cap"] == 2
    assert json.loads(rate_after_third)["cap"] == 8


def test_service_rate_immediately_drops_on_blocked_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SHOKO_LINKER_ALLOW_APPLY", raising=False)
    files = [SpecFile(series_id=1, file_id=index, shoko_id=500 + index) for index in range(10, 13)]
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, files)
    shoko = ShokoStub(files)
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=files,
            shoko_files=files,
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        deps = replace(
            deps,
            service_mode=True,
            service_authorized=lambda: True,
            max_submissions=20,
        )
        canary = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="svc-1"
        )
        shoko.blocked_count = 1
        blocked = run_cycle(
            deps, now_iso=T1, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="svc-2"
        )
        rate_json = store.get_service_rate_state_json()
    finally:
        store.close()
    assert canary.counts.submitted == 1
    assert blocked.counts.submitted == 0
    assert any("preflight_queue_blocked" in reason for reason in blocked.reasons)
    assert rate_json is not None
    assert json.loads(rate_json)["cap"] == 1
    assert shoko.posts_count() == 1


def test_restart_preserves_original_ed2k_and_rejects_reused_shoko_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    candidate = SpecFile(series_id=1, file_id=10, shoko_id=100)
    activate_mapping(tmp_path, mapping_xml())
    path = tmp_path / "state.sqlite3"
    first_store = StateStore(path, budget_limits=WIDE_LIMITS)
    seed_baseline(first_store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([candidate])
    try:
        first_deps = build_deps(
            tmp_path,
            store=first_store,
            sonarr_files=[candidate],
            shoko_files=[candidate],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        first_report = run_cycle(
            first_deps,
            now_iso=T0,
            mode=CycleMode.DRY_RUN,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="identity-first-scan",
        )
        original = first_store.get_file(candidate.shoko_id)
        assert original is not None
        original_identity = original.identity
        assert original_identity is not None
        original_ed2k = original_identity.ed2k
        assert original_ed2k == hf.hex32(candidate.shoko_id)
        assert candidate.shoko_id in first_report.plans
    finally:
        first_store.close()

    # A database restore can make a different file reuse the same Shoko ID. Reopen the
    # durable state and report a new valid ED2K at that ID; the first snapshot must win.
    shoko.ed2k_overrides[candidate.shoko_id] = hf.hex32(candidate.shoko_id + 1)
    reopened = StateStore(path, budget_limits=WIDE_LIMITS)
    try:
        deps = build_deps(
            tmp_path,
            store=reopened,
            sonarr_files=[candidate],
            shoko_files=[candidate],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        report = run_cycle(
            deps,
            now_iso=T1,
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="identity-after-restore",
        )
        record = reopened.get_file(candidate.shoko_id)
        assert record is not None
        identity = record.identity
        assert identity is not None
        assert identity.ed2k == original_ed2k
        assert record.state is FileState.BLOCKED_SAFETY
        assert "ED2K identity drift" in record.state_detail
    finally:
        reopened.close()

    assert report.counts.submitted == 0
    assert shoko.posts_count() == 0
    assert any("source_hash_identity_drift" in reason for reason in report.reasons)


def test_inline_verification_holds_when_posted_file_ed2k_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    candidate = SpecFile(series_id=1, file_id=10, shoko_id=101)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([candidate])
    shoko.on_post = lambda: shoko.ed2k_overrides.__setitem__(
        candidate.shoko_id, hf.hex32(candidate.shoko_id + 1)
    )
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[candidate],
            shoko_files=[candidate],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO, MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
        record = store.get_file(candidate.shoko_id)
        view = store.budget_view()
    finally:
        store.close()

    assert shoko.posts_count() == 1
    assert report.counts.verified == 0
    assert record is not None
    assert record.state is FileState.VERIFYING
    assert view.pending_file_requests == 1


def test_restart_ed2k_drift_keeps_materialized_async_send_recoverable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    candidate = SpecFile(series_id=1, file_id=10, shoko_id=102)
    activate_mapping(tmp_path, mapping_xml())
    path = tmp_path / "state.sqlite3"
    first_store = StateStore(path, budget_limits=WIDE_LIMITS)
    seed_baseline(first_store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([candidate])
    shoko.link_on_post = False  # Accepted job has not materialized its XRefs yet.
    try:
        first_deps = build_deps(
            tmp_path,
            store=first_store,
            sonarr_files=[candidate],
            shoko_files=[candidate],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO, MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        first_report = run_cycle(
            first_deps,
            now_iso=T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c1",
        )
        first_record = first_store.get_file(candidate.shoko_id)
        assert first_record is not None
        assert first_record.state is FileState.VERIFYING
        assert first_report.live_held_sends == 1
    finally:
        first_store.close()

    # The async job materializes the original targets after the restart, but this is now a
    # different file reusing the same Shoko ID/path/size.
    shoko.materialize(candidate.shoko_id)
    shoko.ed2k_overrides[candidate.shoko_id] = hf.hex32(candidate.shoko_id + 1)
    reopened = StateStore(path, budget_limits=WIDE_LIMITS)
    try:
        deps = build_deps(
            tmp_path,
            store=reopened,
            sonarr_files=[candidate],
            shoko_files=[candidate],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        report = run_cycle(
            deps,
            now_iso=T1,
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c2",
        )
        record = reopened.get_file(candidate.shoko_id)
        recovery = reopened.recover_after_restart()
    finally:
        reopened.close()

    assert shoko.posts_count() == 1
    assert report.counts.submitted == 0
    assert candidate.shoko_id not in report.plans
    assert any("source_hash_identity_drift" in reason for reason in report.reasons)
    assert record is not None
    assert record.state is FileState.VERIFYING
    assert record.identity is not None
    assert record.identity.ed2k == hf.hex32(candidate.shoko_id)
    assert [row.shoko_file_id for row in recovery.continue_polling] == [candidate.shoko_id]
    assert report.live_held_sends == 1
    assert report.live_held_slots == 1


def test_new_arrival_marks_sent_before_post(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Durability: at the POST moment the row is already ``request_may_have_been_sent``."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    new_file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    path = tmp_path / "state.sqlite3"
    store = StateStore(path, budget_limits=WIDE_LIMITS)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([new_file])
    state_at_post: list[str] = []

    def hook() -> None:
        import sqlite3

        conn = sqlite3.connect(path)
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            row = conn.execute("SELECT state FROM files WHERE shoko_file_id = 500").fetchone()
        finally:
            conn.close()
        state_at_post.append(row[0])

    shoko.on_post = hook
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[new_file],
            shoko_files=[new_file],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO, MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
    finally:
        store.close()
    assert shoko.posts_count() == 1
    # mark_sent durably committed request_may_have_been_sent BEFORE the POST was issued.
    assert state_at_post == [FileState.REQUEST_MAY_HAVE_BEEN_SENT.value]
    rows = _raw_read(path, "SELECT outcome FROM sends WHERE shoko_file_id = 500 ORDER BY send_id")
    assert rows[0][0] == "verified"


def _raw_read(path: Path, sql: str, params: tuple[Any, ...] = ()) -> list[Any]:
    import sqlite3

    conn = sqlite3.connect(path)
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


# ------------------------------------------------------------------ gate: crash window


def test_crash_after_mark_sent_is_never_resubmitted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    new_file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    path = tmp_path / "state.sqlite3"
    store = StateStore(path, budget_limits=WIDE_LIMITS)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([new_file])
    shoko.fail_post = RuntimeError("simulated crash after mark_sent")
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[new_file],
            shoko_files=[new_file],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO, MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        with pytest.raises(RuntimeError, match="simulated crash"):
            run_cycle(
                deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
            )
    finally:
        store.close()
    # Durability: reopen, classify, confirm no resubmission.
    reopened = StateStore(path, budget_limits=WIDE_LIMITS)
    plan = reopened.recover_after_restart()
    assert [row.shoko_file_id for row in plan.resubmit_forbidden] == [500]
    release_apply_lock(reopened, pid=4242, iso=T1)
    shoko.fail_post = None
    try:
        deps2 = build_deps(
            tmp_path,
            store=reopened,
            sonarr_files=[new_file],
            shoko_files=[new_file],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO, MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        run_cycle(
            deps2, now_iso=T1, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c2"
        )
        record = reopened.get_file(500)
    finally:
        reopened.close()
    assert shoko.posts_count() == 1
    assert record is not None
    assert record.state not in (FileState.PLANNED, FileState.PREPARED_NOT_SENT)


# ------------------------------------------------------------------ gate: async verification


def test_pending_verification_keeps_file_verifying(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    new_file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([new_file])
    shoko.link_on_post = False  # jobs have not materialized the XRef yet
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=[new_file],
        shoko_files=[new_file],
        xml=mapping_xml(),
        monotonic=Clock([MONO_ZERO, MONO_ZERO]),
        advance=Sleeper().advance,
        shoko=shoko,
        verification_timeout=VERIFICATION_TIMEOUT,
        poll_interval=POLL_INTERVAL,
    )
    try:
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
        record = store.get_file(500)
        view = store.budget_view()
    finally:
        store.close()
    assert shoko.posts_count() == 1
    assert record is not None
    assert record.state is FileState.VERIFYING
    assert report.counts.submitted == 1
    assert view.pending_file_requests == 1


@pytest.mark.parametrize("service_mode", [False, True])
def test_async_departure_between_recovery_and_discovery_defers_without_latching(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, service_mode: bool
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    files = [SpecFile(1, 10, 500), SpecFile(1, 11, 501)]

    class AsyncShokoStub(ShokoStub):
        def _listing(self, request: httpx.Request) -> httpx.Response:
            rows = [
                self._file_row(file_id, xrefs=[])
                for file_id in sorted(self.files)
                if file_id not in self.linked
            ]
            return self._json(request, {"Total": len(rows), "List": rows})

    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(99, 990, 999)])
    shoko = AsyncShokoStub(files)
    shoko.link_on_post = False
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=files,
        shoko_files=files,
        xml=mapping_xml(),
        monotonic=Clock([MONO_ZERO]),
        advance=Sleeper().advance,
        shoko=shoko,
        max_submissions=1,
        verification_timeout=10_000,
    )
    deps = replace(deps, service_mode=service_mode, service_authorized=lambda: True)
    policy = RunPolicy.SERVICE if service_mode else RunPolicy.NEW_ARRIVAL
    try:
        first = run_cycle(deps, now_iso=T0, mode=CycleMode.APPLY, policy=policy, cycle_key="c1")
        assert first.counts.submitted == 1
        row = store.get_file(500)
        assert row is not None
        assert row.state is FileState.VERIFYING
        assert store.get_meta("requires_rebaseline") == "0"
        anchor = store.get_meta("source_last_discovery_total")
        basis = store.get_meta("source_anchor_verified_basis")

        def materialize_after_recovery(path: str) -> None:
            if path == "/api/v3/ImportFolder":
                shoko.materialize(500)

        shoko.on_get = materialize_after_recovery
        second = run_cycle(deps, now_iso=T1, mode=CycleMode.APPLY, policy=policy, cycle_key="c2")
        assert store.get_meta("requires_rebaseline") == "0"
        assert "discovery_drop_pending_verification" in second.reasons
        assert not second.reconciliation_ready
        assert not second.apply_eligible
        assert second.counts.submitted == 0
        assert second.writes_suppressed
        assert store.get_meta("source_last_discovery_total") == anchor
        assert store.get_meta("source_anchor_verified_basis") == basis

        store.close()
        store = open_tmp_store(tmp_path)
        deps = replace(deps, store=store)
        shoko.on_get = None
        third = run_cycle(deps, now_iso=T2, mode=CycleMode.APPLY, policy=policy, cycle_key="c3")
        row = store.get_file(500)
        assert row is not None
        assert row.state is FileState.VERIFIED
        assert store.get_meta("requires_rebaseline") == "0"
        assert third.reconciliation_ready
        assert third.counts.submitted == 1
        assert (
            sum(
                request.method == "POST" and request.url.path == "/api/v3/File/500/Link"
                for request in shoko.requests
            )
            == 1
        )
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()


def test_partial_past_deadline_keeps_slot(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    # A two-episode file so a non-empty proper subset is representable.
    multi = SpecFile(
        series_id=1,
        file_id=10,
        shoko_id=500,
        episodes=((1, 1, ANIDB_EP, SHOKO_EP), (1, 2, ANIDB_EP + 1, SHOKO_EP + 1)),
    )
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = _PartialStub([multi])
    monotonic_ticks = iter(range(0, 10_000, 100))
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=[multi],
        shoko_files=[multi],
        xml=mapping_xml(),
        monotonic=lambda: next(monotonic_ticks),
        advance=Sleeper().advance,
        shoko=shoko,
        verification_timeout=VERIFICATION_TIMEOUT,
        poll_interval=POLL_INTERVAL,
    )
    try:
        run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
        record = store.get_file(500)
        view = store.budget_view()
    finally:
        store.close()
    assert record is not None
    assert record.state is FileState.PARTIAL
    assert view.pending_file_requests == 1


class _PartialStub(ShokoStub):
    """Verify poll: first read empty (PENDING), later a non-empty proper subset (PARTIAL)."""

    def __init__(self, files: list[SpecFile]) -> None:
        super().__init__(files)
        self._verify_reads = 0

    def _file(self, request: httpx.Request, path: str) -> httpx.Response:
        shoko_id = int(path.split("/")[4])
        include = request.url.params.get("include", "")
        if "AbsolutePaths" in include:
            return self._json(request, self._file_row(shoko_id, xrefs=[]))
        self._verify_reads += 1
        file = self.files[shoko_id]
        subset = file.episodes[:1]
        return self._json(
            request, self._file_row(shoko_id, xrefs=[self._xref(file.anime_id, subset)])
        )


# ------------------------------------------------------------------ gate: mapping reactivation


def test_ambiguous_mapping_provenance_fits_durable_detail_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidates = tuple(
        MappingTarget(
            anidb_id=4000 + index,
            anidb_episode_number=1,
            episode_type=EpisodeType.EPISODE,
            source_rule=SourceRule.EXPLICIT_PAIR,
            precedence_rank=1,
            origin="anime-list-full.xml:explicit-pair",
        )
        for index in range(32)
    )
    answer = Resolution(
        status=ResolutionStatus.AMBIGUOUS,
        target=None,
        candidates=candidates,
        detail="TVDB 300001 S1E2 writes prohibited",
    )
    monkeypatch.setattr(daemon_module, "resolve_mapping", lambda *_args: answer)
    match = SimpleNamespace(
        sonarr_file=SimpleNamespace(
            episodes=(
                SimpleNamespace(
                    season_number=1,
                    episode_number=2,
                    absolute_episode_number=None,
                ),
            )
        )
    )

    reason = daemon_module._episode_resolutions(
        resolver=cast(EpisodeIdResolver, None),
        match=match,
        tvdb_series=300001,
        active=cast(ActiveMappings, SimpleNamespace(index=object())),
    )

    assert isinstance(reason, str)
    assert len(reason) <= MAX_DETAIL_CHARS - 300
    assert "TVDB 300001 S1E2" in reason
    assert "4000" in reason
    assert "4031" in reason
    assert "explicit_pair" in reason
    assert "anime-list-full.xml:explicit-pair" in reason


def test_mapping_activated_reevaluates_backlog_without_posts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    unmapped = SpecFile(series_id=2, file_id=20, shoko_id=600, tvdb_series=999, anime_id=1001)
    activate_mapping(tmp_path, mapping_xml())  # gen1 maps only series 267440
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [unmapped])
    shoko = ShokoStub([unmapped])
    sleeper = Sleeper()
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[unmapped],
            shoko_files=[unmapped],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=sleeper.advance,
            shoko=shoko,
        )
        # Cycle 1: file is unmapped → ambiguous.
        run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
        record1 = store.get_file(600)
        assert record1 is not None
        assert record1.state is FileState.AMBIGUOUS
        # Cycle 2 at a due time: the scheduler's own refresh activates a generation
        # mapping the file's series, so it is re-evaluated in THIS cycle.
        new_xml = mapping_xml_mapped((999, 1001))
        deps2 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[unmapped],
            shoko_files=[unmapped],
            xml=new_xml,
            monotonic=Clock([MONO_ZERO]),
            advance=sleeper.advance,
            shoko=shoko,
        )
        report = run_cycle(
            deps2,
            now_iso=_due_iso(),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c2",
        )
        mapped_state = store.get_file(600)
    finally:
        store.close()
    assert report.mapping_outcome is RefreshOutcome.ACTIVATED
    assert mapped_state is not None
    assert mapped_state.state is FileState.PLANNED
    assert shoko.posts_count() == 0
    assert report.counts.planned >= 1


def test_service_mapping_activation_reconsiders_and_processes_unresolved_backlog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SHOKO_LINKER_ALLOW_APPLY", raising=False)
    unresolved = SpecFile(series_id=2, file_id=20, shoko_id=600, tvdb_series=999, anime_id=1001)
    unaffected = SpecFile(series_id=3, file_id=30, shoko_id=601, tvdb_series=998, anime_id=1002)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [unresolved, unaffected])
    shoko = ShokoStub([unresolved, unaffected])
    sleeper = Sleeper()
    resolution_calls: list[int] = []
    original_resolve = daemon_module._episode_resolutions

    def count_resolution_calls(*args: Any, **kwargs: Any) -> Any:
        match = args[1]
        resolution_calls.append(match.sonarr_file.series_id)
        return original_resolve(*args, **kwargs)

    monkeypatch.setattr(daemon_module, "_episode_resolutions", count_resolution_calls)
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[unresolved, unaffected],
            shoko_files=[unresolved, unaffected],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=sleeper.advance,
            shoko=shoko,
        )
        deps = replace(deps, service_mode=True, service_authorized=lambda: True, max_submissions=20)
        initial = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="svc-1"
        )
        initial_record = store.get_file(unresolved.shoko_id)
        unaffected_initial = store.get_file(unaffected.shoko_id)
        assert resolution_calls == [2, 3]
        resolution_calls.clear()
        new_xml = mapping_xml_mapped((999, 1001))
        deps = replace(deps, mapping_source=source_endpoint(source_handler(new_xml)))
        activated = run_cycle(
            deps,
            now_iso=_due_iso(),
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="svc-2",
        )
        final_record = store.get_file(unresolved.shoko_id)
        unaffected_final = store.get_file(unaffected.shoko_id)
    finally:
        store.close()
    assert initial_record is not None
    assert initial_record.state is FileState.AMBIGUOUS
    assert unaffected_initial is not None
    assert unaffected_initial.state is FileState.AMBIGUOUS
    assert initial.counts.submitted == 0
    assert activated.mapping_outcome is RefreshOutcome.ACTIVATED
    assert final_record is not None
    assert final_record.state is FileState.VERIFIED
    assert unaffected_final is not None
    assert unaffected_final.state is FileState.AMBIGUOUS
    assert resolution_calls == [2]
    assert activated.counts.verified == 1
    assert shoko.posts_count() == 1


def test_service_quarantines_planned_file_when_mapping_target_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SHOKO_LINKER_ALLOW_APPLY", raising=False)
    candidate = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = MappingChangeShokoStub([candidate])
    state_at_post: list[FileState | None] = []

    def capture_state_at_post() -> None:
        record = store.get_file(candidate.shoko_id)
        state_at_post.append(None if record is None else record.state)

    shoko.on_post = capture_state_at_post
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[candidate],
            shoko_files=[candidate],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        deps = replace(
            deps,
            service_mode=True,
            service_authorized=lambda: True,
            max_submissions=0,
        )
        first = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="svc-1"
        )
        record_before = store.get_file(candidate.shoko_id)
        new_xml = "<anime-list>" + _entry(TVDB_SERIES, CHANGED_ANIME_ID) + "</anime-list>"
        deps = replace(deps, mapping_source=source_endpoint(source_handler(new_xml)))
        deps = replace(deps, max_submissions=20)
        updated = run_cycle(
            deps,
            now_iso=_due_iso(),
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="svc-2",
        )
        record_after = store.get_file(candidate.shoko_id)
        deps = replace(deps, mapping_source=source_endpoint(source_handler(mapping_xml())))
        retried = run_cycle(
            deps,
            now_iso=_due_iso(base=_due_iso()),
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="svc-3",
        )
        record_after_retry = store.get_file(candidate.shoko_id)
    finally:
        store.close()
    assert first.counts.submitted == 0
    assert record_before is not None
    assert record_before.state is FileState.PLANNED
    assert updated.mapping_outcome is RefreshOutcome.ACTIVATED
    assert retried.mapping_outcome is RefreshOutcome.ACTIVATED
    assert record_after is not None
    assert record_after.state is FileState.CONFLICT
    assert state_at_post == []
    assert shoko.posts_count() == 0
    assert record_after.state_detail == "service_mapping_target_changed"
    assert record_after_retry is not None
    assert record_after_retry.state is FileState.CONFLICT
    assert record_after_retry.state_detail == "service_mapping_target_changed"


def test_verified_service_file_mapping_drift_is_persisted_without_link_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SHOKO_LINKER_ALLOW_APPLY", raising=False)
    candidate = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = MappingChangeShokoStub([candidate])
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[candidate],
            shoko_files=[candidate],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        deps = replace(deps, service_mode=True, service_authorized=lambda: True, max_submissions=20)
        first = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="svc-1"
        )
        before = store.get_file(candidate.shoko_id)
        before_xrefs = list(shoko.linked[candidate.shoko_id])
        old_mapping = before.mapping_targets_digest if before is not None else None
        new_xml = "<anime-list>" + _entry(TVDB_SERIES, CHANGED_ANIME_ID) + "</anime-list>"
        deps = replace(deps, mapping_source=source_endpoint(source_handler(new_xml)))
        second = run_cycle(
            deps,
            now_iso=_due_iso(),
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="svc-2",
        )
        drift_records = store.get_service_mapping_drifts()
        after = store.get_file(candidate.shoko_id)
        after_xrefs = list(shoko.linked[candidate.shoko_id])
    finally:
        store.close()
    assert first.counts.verified == 1
    assert second.mapping_outcome is RefreshOutcome.ACTIVATED
    assert after is not None
    assert after.state is FileState.VERIFIED
    assert after_xrefs == before_xrefs
    assert shoko.posts_count() == 1
    assert len(drift_records) == 1
    assert drift_records[0].shoko_file_id == candidate.shoko_id
    assert drift_records[0].previous_targets_digest == old_mapping
    assert drift_records[0].active_targets_digest != old_mapping


def _positional_fence_case(identity_rule: str) -> tuple[str, str, list[SpecFile]]:
    identity_attribute = (
        "defaulttvdbseason='1' episodeoffset='0'"
        if identity_rule == "offset"
        else "defaulttvdbseason='a'"
    )
    old_entry = f"<anime anidbid='{ANIME_ID}' tvdbid='{TVDB_SERIES}' {identity_attribute}/>"
    fenced_entry = (
        f"<anime anidbid='{ANIME_ID}' tvdbid='{TVDB_SERIES}' {identity_attribute}>"
        "<mapping-list><mapping anidbseason='0' tvdbseason='1'>"
        ";101-1;102-3;</mapping></mapping-list></anime>"
    )
    old_xml = f"<anime-list>{old_entry}</anime-list>"
    fenced_xml = f"<anime-list>{fenced_entry}</anime-list>"
    files = [
        SpecFile(
            series_id=1,
            file_id=10 * number,
            shoko_id=500 + number,
            episodes=((1, number, number, 776 + number),),
            absolute_episode_numbers=(number,) if identity_rule == "absolute" else (),
        )
        for number in (1, 2, 3)
    ]
    return old_xml, fenced_xml, files


def _build_positional_fence_service(
    tmp_path: Path, old_xml: str, files: list[SpecFile]
) -> tuple[StateStore, ShokoStub, DaemonDeps]:
    activate_mapping(tmp_path, old_xml)
    store = open_tmp_store(tmp_path)
    seed_baseline(store, files)
    shoko = ShokoStub(files)
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=files,
        shoko_files=files,
        xml=old_xml,
        monotonic=Clock([MONO_ZERO]),
        advance=Sleeper().advance,
        shoko=shoko,
        max_submissions=1,
    )
    return store, shoko, replace(deps, service_mode=True, service_authorized=lambda: True)


def _assert_positional_fence_restart_result(
    reopened: StateStore,
    report: CycleReport,
    shoko: ShokoStub,
    identity_rule: str,
    legacy_snapshot_missing: bool,
    file_ids: tuple[int, int, int],
    before_links: dict[int, list[dict[str, Any]]],
) -> None:
    first_file_id, unaffected_file_id, unsent_file_id = file_ids
    affected = reopened.get_file(first_file_id)
    unaffected = reopened.get_file(unaffected_file_id)
    unsent = reopened.get_file(unsent_file_id)
    drifts = reopened.get_service_mapping_drifts()
    assert report.mapping_outcome is RefreshOutcome.NOT_DUE
    expected_drift_ids = [first_file_id]
    if legacy_snapshot_missing:
        expected_drift_ids.append(unaffected_file_id)
    expected_drift: tuple[str, ...] = (
        f"existing_link drift: verified file {first_file_id} changed mapping at "
        f"TVDB {TVDB_SERIES} S1E1"
        + (" (absolute 1)" if identity_rule == "absolute" else "")
        + " (never unlinked)",
    )
    if legacy_snapshot_missing:
        expected_drift += (
            f"existing_link drift: verified file {unaffected_file_id} frozen target evidence "
            "unavailable after an affected mapping change (never unlinked)",
        )
    assert report.existing_link_drift == expected_drift
    assert affected is not None
    assert affected.state is FileState.VERIFIED
    assert unaffected is not None
    assert unaffected.state is FileState.VERIFIED
    assert unsent is not None
    assert unsent.state is FileState.CONFLICT
    assert not reopened.mapping_target_quarantined(first_file_id)
    assert not reopened.mapping_target_quarantined(unaffected_file_id)
    assert reopened.mapping_target_quarantined(unsent_file_id)
    assert [item.shoko_file_id for item in drifts] == expected_drift_ids
    frozen_semantics = json.loads(reopened.get_meta("reconciled_mapping_semantics:v1") or "null")
    assert "F|1|1" in frozen_semantics[str(TVDB_SERIES)]
    assert "F|1|3" in frozen_semantics[str(TVDB_SERIES)]
    assert drifts[0].previous_targets_digest != drifts[0].active_targets_digest
    after_links = {file_id: list(shoko.linked[file_id]) for file_id in file_ids[:2]}
    assert after_links == before_links
    assert shoko.posts_count() == 2


@pytest.mark.parametrize("identity_rule", ["offset", "absolute"])
@pytest.mark.parametrize(
    "legacy_snapshot_missing", [False, True], ids=["frozen-targets", "legacy-unfrozen-targets"]
)
def test_service_restart_reconciles_positional_fence_drift_only_for_affected_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    identity_rule: str,
    legacy_snapshot_missing: bool,
) -> None:
    monkeypatch.delenv("SHOKO_LINKER_ALLOW_APPLY", raising=False)
    old_xml, fenced_xml, files = _positional_fence_case(identity_rule)
    store, shoko, deps = _build_positional_fence_service(tmp_path, old_xml, files)
    first = run_cycle(
        deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="fence-1"
    )
    second = run_cycle(
        deps, now_iso=T1, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="fence-2"
    )
    before_links = {file.shoko_id: list(shoko.linked[file.shoko_id]) for file in files[:2]}
    first_file_id, unaffected_file_id, unsent_file_id = (file.shoko_id for file in files)
    assert first.counts.verified == 1
    assert second.counts.verified == 2
    assert store.get_file(first_file_id).state is FileState.VERIFIED  # type: ignore[union-attr]
    assert store.get_file(unaffected_file_id).state is FileState.VERIFIED  # type: ignore[union-attr]
    assert store.get_file(unsent_file_id).state is FileState.PLANNED  # type: ignore[union-attr]
    if legacy_snapshot_missing:
        # Historical verified rows without per-file destination snapshots must stay
        # conservative because their positions cannot be compared against the old mapping.
        store.clear_meta(f"file_mapping_targets:v1:{unaffected_file_id}")

    # Mapping activation occurs between process cycles; the next process must compare its
    # complete persisted semantic snapshot rather than relying on the refresh outcome.
    _activate_new_generation(deps, fenced_xml, _due_iso())
    store.close()
    reopened = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
    try:
        restarted_deps = build_deps(
            tmp_path,
            store=reopened,
            sonarr_files=files,
            shoko_files=files,
            xml=fenced_xml,
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            max_submissions=0,
        )
        restarted_deps = replace(restarted_deps, service_mode=True, service_authorized=lambda: True)
        report = run_cycle(
            restarted_deps,
            now_iso=_due_iso(),
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="fence-after-restart",
        )
        _assert_positional_fence_restart_result(
            reopened,
            report,
            shoko,
            identity_rule,
            legacy_snapshot_missing,
            (first_file_id, unaffected_file_id, unsent_file_id),
            before_links,
        )
    finally:
        reopened.close()


def test_partial_discovery_still_reports_legacy_verified_mapping_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    old_xml, fenced_xml, files = _positional_fence_case("offset")
    store, shoko, deps = _build_positional_fence_service(tmp_path, old_xml, files)
    run_cycle(
        deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="partial-1"
    )
    run_cycle(
        deps, now_iso=T1, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="partial-2"
    )
    legacy_file_id = files[1].shoko_id
    assert store.get_file(legacy_file_id).state is FileState.VERIFIED  # type: ignore[union-attr]
    store.clear_meta(f"file_mapping_targets:v1:{legacy_file_id}")
    _activate_new_generation(deps, fenced_xml, _due_iso())
    store.close()

    reopened = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
    try:
        restarted_deps = build_deps(
            tmp_path,
            store=reopened,
            sonarr_files=files,
            shoko_files=files,
            xml=fenced_xml,
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            max_submissions=0,
        )
        restarted_deps = replace(restarted_deps, service_mode=True, service_authorized=lambda: True)
        collect_discovery = daemon_module._collect_bounded_shoko_discovery

        def omit_legacy_file(deps: DaemonDeps, cycle_start_mono: float, now_iso: str) -> Any:
            discovery = collect_discovery(deps, cycle_start_mono, now_iso)
            return replace(
                discovery,
                files=tuple(file for file in discovery.files if file.id != legacy_file_id),
                verdict=replace(discovery.verdict, complete=False),
            )

        monkeypatch.setattr(daemon_module, "_collect_bounded_shoko_discovery", omit_legacy_file)
        report = run_cycle(
            restarted_deps,
            now_iso=_due_iso(),
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="partial-after-activation",
        )

        assert report.reconciliation_ready is False
        assert report.existing_link_drift == (
            f"existing_link drift: verified file {files[0].shoko_id} changed mapping at "
            f"TVDB {TVDB_SERIES} S1E1 (never unlinked)",
            f"existing_link drift: verified file {legacy_file_id} frozen target evidence "
            "unavailable after an affected mapping change (never unlinked)",
        )
        assert [item.shoko_file_id for item in reopened.get_service_mapping_drifts()] == [
            files[0].shoko_id,
            legacy_file_id,
        ]
        assert reopened.get_meta(f"file_mapping_targets:v1:{legacy_file_id}") is None
        assert shoko.posts_count() == 2
    finally:
        reopened.close()


@pytest.mark.parametrize("identity_rule", ["offset", "absolute"])
def test_service_reports_mapping_drift_for_linked_file_omitted_from_unrecognized_listing(
    tmp_path: Path, identity_rule: str
) -> None:
    old_xml, fenced_xml, files = _positional_fence_case(identity_rule)
    source = files[0]
    store, shoko, deps = _build_positional_fence_service(tmp_path, old_xml, [source])
    try:
        first = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="linked-1"
        )
        assert first.counts.verified == 1
        record = store.get_file(source.shoko_id)
        assert record is not None
        assert record.state is FileState.VERIFIED
        frozen_key = f"file_mapping_targets:v1:{source.shoko_id}"
        frozen_targets = store.get_meta(frozen_key)
        assert frozen_targets is not None
        before_links = list(shoko.linked[source.shoko_id])
        assert shoko.posts_count() == 1

        # Shoko's Unrecognized listing correctly drops a file once it has XRefs.
        shoko.files.clear()
        _activate_new_generation(deps, fenced_xml, T1)
        store.close()

        reopened = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
        try:
            restarted_deps = build_deps(
                tmp_path,
                store=reopened,
                sonarr_files=[source],
                shoko_files=[],
                xml=fenced_xml,
                monotonic=Clock([MONO_ZERO]),
                advance=Sleeper().advance,
                shoko=shoko,
                max_submissions=0,
            )
            restarted_deps = replace(
                restarted_deps, service_mode=True, service_authorized=lambda: True
            )
            report = run_cycle(
                restarted_deps,
                now_iso=T2,
                mode=CycleMode.APPLY,
                policy=RunPolicy.SERVICE,
                cycle_key="linked-after-restart",
            )

            assert report.existing_link_drift == (
                f"existing_link drift: verified file {source.shoko_id} changed mapping at "
                f"TVDB {TVDB_SERIES} S1E1"
                + (" (absolute 1)" if identity_rule == "absolute" else "")
                + " (never unlinked)",
            )
            drifts = reopened.get_service_mapping_drifts()
            assert [item.shoko_file_id for item in drifts] == [source.shoko_id]
            assert drifts[0].reason == report.existing_link_drift[0].removeprefix(
                f"existing_link drift: verified file {source.shoko_id} "
            ).removesuffix(" (never unlinked)")
            assert reopened.get_meta(frozen_key) == frozen_targets
            assert reopened.get_file(source.shoko_id).state is FileState.VERIFIED  # type: ignore[union-attr]
            assert shoko.linked[source.shoko_id] == before_links
            assert shoko.posts_count() == 1

            repeated = run_cycle(
                restarted_deps,
                now_iso="2026-01-01T00:30:00+00:00",
                mode=CycleMode.APPLY,
                policy=RunPolicy.SERVICE,
                cycle_key="linked-repeat",
            )
            assert repeated.existing_link_drift == report.existing_link_drift
            assert reopened.get_meta(frozen_key) == frozen_targets
            assert reopened.get_service_mapping_drifts() == drifts
            assert shoko.linked[source.shoko_id] == before_links
            assert shoko.posts_count() == 1
        finally:
            reopened.close()
    finally:
        with contextlib.suppress(Exception):
            store.close()


def test_service_reports_only_changed_position_for_omitted_composite_file(tmp_path: Path) -> None:
    old_xml, fenced_xml, _files = _positional_fence_case("offset")
    composite = SpecFile(
        series_id=1,
        file_id=10,
        shoko_id=500,
        episodes=((1, 1, 776, 777), (1, 2, 777, 778)),
    )
    store, shoko, deps = _build_positional_fence_service(tmp_path, old_xml, [composite])
    try:
        first = run_cycle(
            deps,
            now_iso=T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="composite-linked-1",
        )
        assert first.counts.verified == 1
        frozen_key = f"file_mapping_targets:v1:{composite.shoko_id}"
        frozen_targets = store.get_meta(frozen_key)
        assert frozen_targets is not None
        assert len(json.loads(frozen_targets)) == 2
        before_links = list(shoko.linked[composite.shoko_id])
        assert shoko.posts_count() == 1

        shoko.files.clear()
        _activate_new_generation(deps, fenced_xml, T1)
        store.close()
        reopened = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
        try:
            restarted_deps = build_deps(
                tmp_path,
                store=reopened,
                sonarr_files=[composite],
                shoko_files=[],
                xml=fenced_xml,
                monotonic=Clock([MONO_ZERO]),
                advance=Sleeper().advance,
                shoko=shoko,
                max_submissions=0,
            )
            restarted_deps = replace(
                restarted_deps, service_mode=True, service_authorized=lambda: True
            )
            report = run_cycle(
                restarted_deps,
                now_iso=T2,
                mode=CycleMode.APPLY,
                policy=RunPolicy.SERVICE,
                cycle_key="composite-linked-after-restart",
            )

            assert report.existing_link_drift == (
                f"existing_link drift: verified file {composite.shoko_id} changed mapping at "
                f"TVDB {TVDB_SERIES} S1E1 (never unlinked)",
            )
            assert "S1E2" not in report.existing_link_drift[0]
            assert reopened.get_meta(frozen_key) == frozen_targets
            assert reopened.get_file(composite.shoko_id).state is FileState.VERIFIED  # type: ignore[union-attr]
            assert shoko.linked[composite.shoko_id] == before_links
            assert shoko.posts_count() == 1
        finally:
            reopened.close()
    finally:
        with contextlib.suppress(Exception):
            store.close()


@pytest.mark.parametrize("malformed_snapshot", ["not-json", "{}", '{"broken": [true]}'])
def test_malformed_verified_mapping_snapshot_reports_conservatively_without_stalling(
    tmp_path: Path, malformed_snapshot: str
) -> None:
    old_xml, fenced_xml, files = _positional_fence_case("offset")
    source = files[0]
    store, shoko, deps = _build_positional_fence_service(tmp_path, old_xml, [source])
    try:
        first = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="bad-1"
        )
        assert first.counts.verified == 1
        frozen_key = f"file_mapping_targets:v1:{source.shoko_id}"
        store.set_meta(frozen_key, malformed_snapshot)
        shoko.files.clear()
        _activate_new_generation(deps, fenced_xml, T1)
        store.close()

        reopened = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
        try:
            restarted_deps = build_deps(
                tmp_path,
                store=reopened,
                sonarr_files=[source],
                shoko_files=[],
                xml=fenced_xml,
                monotonic=Clock([MONO_ZERO]),
                advance=Sleeper().advance,
                shoko=shoko,
                max_submissions=0,
            )
            restarted_deps = replace(
                restarted_deps, service_mode=True, service_authorized=lambda: True
            )
            report = run_cycle(
                restarted_deps,
                now_iso=T2,
                mode=CycleMode.APPLY,
                policy=RunPolicy.SERVICE,
                cycle_key="bad-after-restart",
            )
            assert report.existing_link_drift == (
                f"existing_link drift: verified file {source.shoko_id} frozen target evidence "
                "unavailable after an affected mapping change (never unlinked)",
            )
            drift = reopened.get_service_mapping_drifts()
            assert len(drift) == 1
            assert drift[0].reason == (
                "frozen target evidence unavailable after an affected mapping change"
            )
            assert reopened.get_meta(frozen_key) == malformed_snapshot
            assert shoko.posts_count() == 1
        finally:
            reopened.close()
    finally:
        with contextlib.suppress(Exception):
            store.close()


def _activate_new_generation(deps: DaemonDeps, xml: str, now_iso: str) -> None:
    endpoint = source_endpoint(source_handler(xml))
    try:
        refresh(deps.mapping_store, endpoint, now=_parse(now_iso), refresh_hours=HOURS, force=True)
    finally:
        endpoint.client.close()


# ------------------------------------------------------------------ gate: churn guard


def test_churn_guard_pauses_and_suppresses_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, GEN_A)
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [file])
    shoko = ShokoStub([file])
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[file],
            shoko_files=[file],
            xml=GEN_B,
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            churn_limit=1,
        )
        # The cycle's own refresh activates a generation that flips two series (> limit).
        report = run_cycle(
            deps,
            now_iso=_due_iso(),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c1",
        )
    finally:
        store.close()
    reopened = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
    try:
        view = reopened.budget_view()
        assert view.paused_reason == "mapping_churn"
    finally:
        reopened.close()
    assert report.counts.submitted == 0
    assert shoko.posts_count() == 0
    assert report.writes_suppressed is True
    assert "mapping_churn" in report.reasons


# ------------------------------------------------------------------ gate: in-flight plan drift


def test_inflight_plan_mapping_change_blocks_post(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([file])
    sleeper = Sleeper()
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=mapping_xml(),
        monotonic=Clock([MONO_ZERO, MONO_ZERO]),
        advance=sleeper.advance,
        shoko=shoko,
        max_submissions=0,  # cycle 1 plans without submitting
    )
    # Cycle 1: plan the file against gen1 (in-flight approvals keep frozen digests).
    run_cycle(deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1")
    record1 = store.get_file(500)
    assert record1 is not None
    assert record1.state is FileState.PLANNED
    # Cycle 2 at a due time: the scheduler activates a generation that changes the
    # planned target → in-flight plan drift, quarantined to waiting, no POST.
    drift_xml = "<anime-list>" + _entry(TVDB_SERIES, 2222) + "</anime-list>"
    deps2 = build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=drift_xml,
        monotonic=Clock([MONO_ZERO]),
        advance=sleeper.advance,
        shoko=shoko,
    )
    report = run_cycle(
        deps2,
        now_iso=_due_iso(),
        mode=CycleMode.APPLY,
        policy=RunPolicy.NEW_ARRIVAL,
        cycle_key="c2",
    )
    record = store.get_file(500)
    store.close()
    # In-flight plan drift: quarantined out of planned, no POST, drift reported.
    assert record is not None
    assert record.state is FileState.CONFLICT
    assert shoko.posts_count() == 0
    assert any("mapping_changed" in item for item in report.drift)


# ------------------------------------------------------------------ gate: scheduler timing


def test_scheduler_runs_refresh_only_at_due(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [file])
    requests: list[str] = []

    def counting_source(request: httpx.Request) -> httpx.Response:
        requests.append(str(request.url))
        return httpx.Response(304, headers={"ETag": '"identity-src"'}, request=request)

    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=mapping_xml(),
        monotonic=Clock([MONO_ZERO]),
        advance=Sleeper().advance,
    )
    # Point the mapping source at a 304-only handler and run cycles at schedule times.
    deps = _swap_source(deps, counting_source)
    try:
        r1 = run_cycle(
            deps,
            now_iso=_due_iso(),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c1",
        )
        before = len(requests)
        r2 = run_cycle(
            deps, now_iso=T1, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c2"
        )
        after = len(requests)
    finally:
        store.close()
    # The due cycle refreshes (304 → unchanged); the non-due cycle makes zero requests.
    assert before == 1
    assert after == 1
    assert r1.mapping_outcome is RefreshOutcome.UNCHANGED
    assert r2.mapping_outcome is RefreshOutcome.NOT_DUE


def _swap_source(
    deps: DaemonDeps, handler: Callable[[httpx.Request], httpx.Response]
) -> DaemonDeps:
    from dataclasses import replace

    return replace(deps, mapping_source=source_endpoint(handler))


def test_mapping_unavailable_writes_zero(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [file])
    shoko = ShokoStub([file])
    requests: list[str] = []

    def explode(request: httpx.Request) -> httpx.Response:
        requests.append(str(request.url))
        return httpx.Response(503, request=request)

    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=mapping_xml(),
        monotonic=Clock([MONO_ZERO]),
        advance=Sleeper().advance,
        shoko=shoko,
    )
    deps = _swap_source(deps, explode)
    try:
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
    finally:
        store.close()
    assert "mapping_unavailable" in report.reasons
    assert shoko.posts_count() == 0


def test_invalid_split_entry_cannot_expand_ownership_into_verified_service_post(
    tmp_path: Path,
) -> None:
    """A malformed split owner must not let another AniDB entry claim its TVDB episodes."""
    file = SpecFile(
        series_id=1,
        file_id=10,
        shoko_id=500,
        episodes=((1, 17, 17, SHOKO_EP),),
        tvdb_series=267440,
        anime_id=100,
    )
    invalid_xml = (
        "<anime-list>"
        "<anime anidbid='100' tvdbid='267440' defaulttvdbseason='1' episodeoffset='0'/>"
        "<anime anidbid='200' tvdbid='267440' defaulttvdbseason='1' episodeoffset='16'>"
        "<mapping-list><mapping anidbseason='1' tvdbseason='1'>;1-2oops;</mapping>"
        "</mapping-list></anime></anime-list>"
    )
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [file])
    shoko = ShokoStub([file])
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=invalid_xml,
        monotonic=Clock([MONO_ZERO]),
        advance=Sleeper().advance,
        shoko=shoko,
    )
    deps = replace(deps, service_mode=True, service_authorized=lambda: True)
    try:
        report = run_cycle(
            deps,
            now_iso=T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="invalid-split-entry",
        )
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()

    assert report.counts.submitted == 0
    assert "mapping_unavailable" in report.reasons
    assert shoko.posts == []


def test_new_known_unsupported_scope_quarantines_frozen_file_and_links_unrelated_service_file(
    tmp_path: Path,
) -> None:
    affected = SpecFile(series_id=1, file_id=10, shoko_id=500, tvdb_series=267440, anime_id=100)
    healthy = SpecFile(series_id=2, file_id=20, shoko_id=501, tvdb_series=999, anime_id=300)
    supported_xml = (
        "<anime-list>"
        "<anime anidbid='100' tvdbid='267440' defaulttvdbseason='1' episodeoffset='0'/>"
        + _entry(999, 300)
        + "</anime-list>"
    )
    fenced_xml = supported_xml.replace(
        "</anime-list>",
        "<anime anidbid='200' tvdbid='267440' defaulttvdbseason='a' episodeoffset='4'>"
        "<mapping-list><mapping anidbseason='1' tvdbseason='1' start='1' end='4' offset='0'/>"
        "</mapping-list></anime></anime-list>",
    )

    activate_mapping(tmp_path, supported_xml)
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [affected, healthy])
    shoko = ShokoStub([affected, healthy])
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=[affected, healthy],
        shoko_files=[affected, healthy],
        xml=supported_xml,
        monotonic=Clock([MONO_ZERO]),
        advance=Sleeper().advance,
        shoko=shoko,
    )
    deps = replace(deps, service_mode=True, service_authorized=lambda: True, max_submissions=0)
    next_source = source_endpoint(source_handler(fenced_xml))
    try:
        first = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="fence-1"
        )
        affected_before = store.get_file(affected.shoko_id)
        healthy_before = store.get_file(healthy.shoko_id)
        deps = replace(deps, mapping_source=next_source, max_submissions=20)
        updated = run_cycle(
            deps,
            now_iso=_due_iso(),
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="fence-2",
        )
        affected_after = store.get_file(affected.shoko_id)
        healthy_after = store.get_file(healthy.shoko_id)
        post_paths = [request.url.path for request in shoko.requests if request.method == "POST"]
        assert not deps.mapping_store.invalid_source_hold()
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()

    assert first.counts.submitted == 0
    assert affected_before is not None
    assert affected_before.state is FileState.PLANNED
    assert healthy_before is not None
    assert healthy_before.state is FileState.PLANNED
    assert updated.mapping_outcome is RefreshOutcome.ACTIVATED
    assert updated.counts.submitted == 1
    assert affected_after is not None
    assert affected_after.state is FileState.CONFLICT
    assert affected_after.state_detail == "service_mapping_target_changed"
    assert affected_after.mapping_targets_digest == affected_before.mapping_targets_digest
    assert healthy_after is not None
    assert healthy_after.state is FileState.VERIFIED
    assert post_paths == ["/api/v3/File/501/Link"]


def test_restart_does_not_disable_refresh_or_tight_loop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    mapping_dir = tmp_path / "mappings"
    requests: list[int] = []

    def source(request: httpx.Request) -> httpx.Response:
        requests.append(1)
        return httpx.Response(
            200,
            headers={"ETag": '"identity-src"'},
            stream=httpx.ByteStream(mapping_xml().encode()),
            request=request,
        )

    endpoint = source_endpoint(source)
    store1 = MappingStore(mapping_dir, source_url=SOURCE_URL)
    refresh(store1, endpoint, now=_parse(T0), refresh_hours=HOURS)
    endpoint.client.close()
    # Reopen as a brand-new store instance at a non-due time: not due (no tight loop).
    store2 = MappingStore(mapping_dir, source_url=SOURCE_URL)
    from shoko_linker.mapping_store import due

    assert due(store2, _parse(T1)) is False
    assert due(store2, _parse(T0) + _days(7)) is True


def test_daemon_recovers_missing_active_cache_before_weekly_due_time(
    tmp_path: Path,
) -> None:
    file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    mapping_store = MappingStore(tmp_path / "mappings", source_url=SOURCE_URL)
    mapping_store.active_path.unlink()
    requests: list[str] = []

    def recover(request: httpx.Request) -> httpx.Response:
        requests.append(str(request.url))
        return httpx.Response(
            200,
            headers={"ETag": '"recovered"'},
            stream=httpx.ByteStream(mapping_xml().encode()),
            request=request,
        )

    state_store = open_tmp_store(tmp_path)
    try:
        deps = build_deps(
            tmp_path,
            store=state_store,
            sonarr_files=[file],
            shoko_files=[file],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
        )
        deps = _swap_source(deps, recover)
        report = run_cycle(
            deps,
            now_iso=T1,
            mode=CycleMode.DRY_RUN,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="recovery",
        )
        assert len(requests) == 1
        assert report.mapping_outcome in (RefreshOutcome.ACTIVATED, RefreshOutcome.UNCHANGED)
        assert load_active(mapping_store) is not None
    finally:
        state_store.close()
        deps.mapping_source.client.close()


def _days(n: float) -> timedelta:
    return timedelta(days=n)


def _due_iso(base: str = T0, hours: float = DAYS_7) -> str:
    return (_parse(base) + timedelta(hours=hours)).isoformat()


# ------------------------------------------------------------------ gate: max_submissions


def test_max_submissions_counts_posts_not_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    files = [
        SpecFile(series_id=index + 1, file_id=10 + index, shoko_id=500 + index)
        for index in range(5)
    ]
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub(files)
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=files,
        shoko_files=files,
        xml=mapping_xml(),
        monotonic=Clock([MONO_ZERO, MONO_ZERO, MONO_ZERO, MONO_ZERO, MONO_ZERO, MONO_ZERO]),
        advance=Sleeper().advance,
        shoko=shoko,
        max_submissions=COUNT_TWO,
    )
    try:
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
    finally:
        store.close()
    assert shoko.posts_count() == COUNT_TWO
    assert report.counts.submitted == COUNT_TWO
    assert report.deferred_planned == COUNT_THREE


# ------------------------------------------------------------------ gate: blocked Shoko queue
# (M7 review P1: a blocked queue must suppress apply in the production wiring)


NEW_ARRIVAL_SINGLE = SpecFile(series_id=1, file_id=10, shoko_id=500)


def _blocked_queue_new_arrival(
    tmp_path: Path,
    *,
    blocked_count: int,
    blocked_over: int | None,
    sync_extra: dict[str, Any] | None = None,
) -> tuple[ShokoStub, list[SpecFile], Settings]:
    shoko = ShokoStub([NEW_ARRIVAL_SINGLE], blocked_count=blocked_count)
    settings = make_settings(tmp_path, sync_extra=sync_extra)
    return shoko, [NEW_ARRIVAL_SINGLE], settings


def test_blocked_queue_tolerance_zero_refuses_all_posts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Wired apply with BlockedCount 3 > tolerance 0: zero POSTs + `queue_blocked:3`."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko, files, settings = _blocked_queue_new_arrival(tmp_path, blocked_count=3, blocked_over=0)
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=files,
            shoko_files=files,
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO, MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            settings=settings,
            blocked_over=0,
        )
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
    finally:
        store.close()
    assert shoko.posts_count() == 0
    assert report.counts.submitted == 0
    assert "queue_blocked:3" in report.reasons
    assert any("preflight_queue_blocked" in reason for reason in report.reasons)


def test_blocked_queue_light_cycle_refuses_all_posts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The safety conjunction is unchanged in light (full=False) cycles."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko, files, settings = _blocked_queue_new_arrival(tmp_path, blocked_count=3, blocked_over=0)
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=files,
            shoko_files=files,
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO, MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            settings=settings,
            blocked_over=0,
        )
        report = run_cycle(
            deps,
            now_iso=T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c1",
            full=False,
        )
    finally:
        store.close()
    assert report.full is False
    assert shoko.posts_count() == 0
    assert report.counts.submitted == 0
    assert "queue_blocked:3" in report.reasons


def test_unwired_blocked_over_uses_config_tolerance_zero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DaemonDeps.blocked_over=None must fall back to the config tolerance (default 0)."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko, files, settings = _blocked_queue_new_arrival(
        tmp_path, blocked_count=3, blocked_over=None
    )
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=files,
            shoko_files=files,
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO, MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            settings=settings,
            blocked_over=None,
        )
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
    finally:
        store.close()
    assert shoko.posts_count() == 0
    assert report.counts.submitted == 0
    assert "queue_blocked:3" in report.reasons


def test_queue_blocked_tolerance_knob_above_blocked_count_applies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """sync.queue_blocked_tolerance = 5 with BlockedCount 3: the knob opens apply."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko, files, settings = _blocked_queue_new_arrival(
        tmp_path, blocked_count=3, blocked_over=None, sync_extra={"queue_blocked_tolerance": 5}
    )
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=files,
            shoko_files=files,
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO, MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            settings=settings,
            blocked_over=None,
        )
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
    finally:
        store.close()
    assert shoko.posts_count() == 1
    assert report.counts.submitted == 1
    # The blocked count stays a cycle report; the tolerance opens the preflight gate.
    assert "queue_blocked:3" in report.reasons
    assert not any(reason.startswith("preflight_queue_blocked") for reason in report.reasons)


def test_queue_read_failure_keeps_queue_unknown_zero_posts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """queue=None (read failure) stays the independent QUEUE_UNKNOWN fail-closed refusal."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([NEW_ARRIVAL_SINGLE])
    shoko.fail_queue = True
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[NEW_ARRIVAL_SINGLE],
            shoko_files=[NEW_ARRIVAL_SINGLE],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO, MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            blocked_over=0,
        )
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
    finally:
        store.close()
    assert shoko.posts_count() == 0
    assert report.counts.submitted == 0
    assert "queue_unknown" in report.reasons
    assert any("preflight_queue_unknown" in reason for reason in report.reasons)


# ------------------------------------------------------------------ gate: two-process lock


def test_two_processes_contended_lock_is_read_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([file])
    # First process holds the apply lock.
    acquire_apply_lock(store, pid=111, iso=T0, lease_expires_iso=T2)
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[file],
            shoko_files=[file],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
    finally:
        store.close()
    assert shoko.posts_count() == 0
    assert report.reasons[0].startswith("apply_lock_contended")
    assert report.writes_suppressed is True


def test_unavailable_apply_lock_defers_read_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P3-1: a filesystem failure on apply.lock (here: a directory in its
    place) is NOT contention and must never crash the loop — the cycle defers read-only
    with a named reason and zero submissions."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([file])
    (tmp_path / "apply.lock").mkdir()  # os.open on a directory -> ENOENT/EISDIR
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[file],
            shoko_files=[file],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
    finally:
        store.close()
    assert shoko.posts_count() == 0
    assert report.reasons[0].startswith("apply_lock_unavailable")
    assert report.writes_suppressed is True


# ------------------------------------------------------------------ gate: writes disabled


def test_writes_disabled_cycle_is_read_only_with_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    store.disable_writes("simulated disk full")
    shoko = ShokoStub([file])
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[file],
            shoko_files=[file],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
    finally:
        store.close()
    assert report.reasons == ("writes_disabled",)
    assert shoko.posts_count() == 0
    assert report.writes_suppressed is True


def test_dry_run_zero_posts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([file])
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[file],
            shoko_files=[file],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.DRY_RUN, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
    finally:
        store.close()
    assert shoko.posts_count() == 0
    assert report.counts.submitted == 0


# ------------------------------------------------------------------ gate: backoff + SIGTERM


def test_backoff_grows_sleeps_and_success_resets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [file])

    # Transport-level failures on the mapping source every cycle → consecutive failures grow sleep.
    def explode(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, request=request)

    sleeper = Sleeper()
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=mapping_xml(),
        monotonic=Clock([MONO_ZERO]),
        advance=sleeper.advance,
        jitter=lambda: 1.0,
        max_submissions=1,
    )
    deps = _swap_source(deps, explode)
    # Times advance along the store's own failure backoff so every cycle is due (fails).
    times = iter(
        [T0, _due_iso(hours=1.0), _due_iso(hours=3.0), _due_iso(hours=7.0), _due_iso(hours=15.0)]
    )
    try:
        report = run_daemon(
            deps,
            cycle_key=lambda index: f"c{index}",
            now_iso=lambda: next(times),
            interval_seconds=10.0,
            stop_after_cycles=4,
        )
    finally:
        store.close()
    sleeps = sleeper.sleeps
    assert len(sleeps) == COUNT_THREE
    # 10, 20, 40 with jitter multiplier 1.05 (rng()=1.0).
    assert sleeps[0] < sleeps[1] < sleeps[2]
    assert all(report.cycles[index].failed for index in range(4))


def test_success_after_failures_resets_backoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [file])
    sleeper = Sleeper()
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=mapping_xml(),
        monotonic=Clock([MONO_ZERO]),
        advance=sleeper.advance,
        jitter=lambda: 0.5,
    )
    # Active mapping present and not due → cycles succeed (no backoff).
    times = iter([T1, T1, T1])
    try:
        run_daemon(
            deps,
            cycle_key=lambda index: f"c{index}",
            now_iso=lambda: next(times),
            interval_seconds=10.0,
            stop_after_cycles=3,
        )
    finally:
        store.close()
    # All sleeps equal the base interval (jitter neutral at 0.5).
    assert all(abs(value - 10.0) < EPSILON for value in sleeper.sleeps)


def test_sigterm_exits_after_current_cycle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [file])
    completed = {"n": 0}

    def on_complete(_report: Any) -> None:
        completed["n"] += 1

    def should_stop() -> bool:
        # A pure predicate: a shutdown becomes visible once STOP_AT cycles have completed
        # (the flag is set after the cycle finishes, so the daemon stops after that cycle).
        return completed["n"] >= STOP_AT

    times = iter([T1, T1, T1, T1, T1])
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=mapping_xml(),
        monotonic=Clock([MONO_ZERO]),
        advance=Sleeper().advance,
    )
    try:
        report = run_daemon(
            deps,
            cycle_key=lambda index: f"c{index}",
            now_iso=lambda: next(times),
            interval_seconds=10.0,
            should_stop=should_stop,
            on_cycle_complete=on_complete,
        )
    finally:
        store.close()
    assert report.shutdown_requested is True
    assert report.exit_reason == "shutdown_requested"
    assert report.total_cycles == COUNT_TWO
    assert len(report.cycles) == COUNT_TWO


# ------------------------------------------------------------------ gate: eligibility


def test_compute_eligibility_requires_all_safety_inputs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [file])
    # Gate closed (no env opt-in in this test → apply_gate False).
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=mapping_xml(),
        monotonic=Clock([MONO_ZERO]),
        advance=Sleeper().advance,
        attestation=False,
    )
    try:
        verdict = compute_eligibility(deps, T0)
        assert verdict.eligible is False
        assert "apply_gate_closed" in verdict.reasons
        assert "attestation_invalid" in verdict.reasons
        service_consent_must_not_unlock_advanced_mode = replace(
            deps, service_authorized=lambda: True
        )
        advanced_verdict = compute_eligibility(service_consent_must_not_unlock_advanced_mode, T0)
        assert advanced_verdict.eligible is False
        assert "apply_gate_closed" in advanced_verdict.reasons
        assert "attestation_invalid" in advanced_verdict.reasons
        monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
        deps_open = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[file],
            shoko_files=[file],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            attestation=True,
        )
        open_verdict = compute_eligibility(deps_open, T0)
    finally:
        store.close()
    assert open_verdict.eligible is True


# ------------------------------------------------------------------ M6 review regressions


def _iso_offset(base: str = T0, minutes: float = 0.0) -> str:
    return (_parse(base) + timedelta(minutes=minutes)).isoformat()


def _iso_offset_seconds(base: str = T0, seconds: float = 0.0) -> str:
    return (_parse(base) + timedelta(seconds=seconds)).isoformat()


def _rising_clock(start: float, count: int) -> Clock:
    return Clock([start + float(index) for index in range(count)])


class _LazyLinkStub(ShokoStub):
    """POST links ONLY files in ``instant_ids``; the rest materialize via ``materialize``."""

    def __init__(self, files: list[SpecFile], *, instant_ids: list[int]) -> None:
        super().__init__(files)
        self.instant_ids = set(instant_ids)

    def _post(self, request: httpx.Request, path: str) -> httpx.Response:
        shoko_id = int(path.split("/")[4])
        if self.on_post is not None:
            self.on_post()
        self.posts.append(json.loads(request.content))
        if shoko_id in self.instant_ids:
            file = self.files[shoko_id]
            self.linked[shoko_id] = [self._xref(file.anime_id, file.episodes)]
        if self.fail_post is not None:
            raise self.fail_post
        if self.post_status is not None:
            return httpx.Response(
                self.post_status, json={"title": "validation problem"}, request=request
            )
        return self._json(request, {})


TWO_FILE_LIMITS = BudgetLimits(
    max_pending_files=2,
    max_pending_episode_jobs=10,
    per_cycle_new=50,
    rolling_new=50,
    rolling_window_hours=24.0,
)


def test_inventory_growth_keeps_apply_eligible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P1-1 probe (self-wedge): inventory growth changed the snapshot hash every cycle,
    so ``requires_rebaseline`` persisted forever and apply was permanently suppressed.
    Growth cycles must stay eligible and submit each new arrival (1 post per cycle)."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    a = SpecFile(series_id=1, file_id=10, shoko_id=500)
    b = SpecFile(series_id=2, file_id=20, shoko_id=501)
    c = SpecFile(series_id=3, file_id=30, shoko_id=502)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([a, b, c])
    sleeper = Sleeper()
    reports = []
    try:
        for index, files in enumerate(([a], [a, b], [a, b, c])):
            deps = build_deps(
                tmp_path,
                store=store,
                sonarr_files=files,
                shoko_files=files,
                xml=mapping_xml(),
                monotonic=_rising_clock(float(index * 10), 6),
                advance=sleeper.advance,
                shoko=shoko,
                max_submissions=1,
            )
            reports.append(
                run_cycle(
                    deps,
                    now_iso=_iso_offset(minutes=10.0 * index),
                    mode=CycleMode.APPLY,
                    policy=RunPolicy.NEW_ARRIVAL,
                    cycle_key=f"c{index + 1}",
                )
            )
        rebaseline_flag = store.get_meta("requires_rebaseline")
        record_c = store.get_file(502)
    finally:
        store.close()
    assert all(report.apply_eligible for report in reports)
    assert all(report.counts.submitted == 1 for report in reports)
    assert shoko.posts_count() == POSTS_THREE
    assert rebaseline_flag == "0"
    assert record_c is not None
    assert record_c.state is FileState.VERIFIED


def test_growth_cycles_submit_each_new_arrival_and_apply_stays_eligible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P1-1 regression: cycle1 baseline → cycle2 first new arrival submitted (1 post) →
    cycle3 second new arrival submitted (1 post); continuity never self-wedges."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    a = SpecFile(series_id=1, file_id=10, shoko_id=500)
    b = SpecFile(series_id=2, file_id=20, shoko_id=501)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([a, b])
    sleeper = Sleeper()
    try:
        for index, files in enumerate(([a], [a, b])):
            deps = build_deps(
                tmp_path,
                store=store,
                sonarr_files=files,
                shoko_files=files,
                xml=mapping_xml(),
                monotonic=_rising_clock(float(index * 10), 6),
                advance=sleeper.advance,
                shoko=shoko,
                max_submissions=1,
            )
            run_cycle(
                deps,
                now_iso=_iso_offset(minutes=10.0 * index),
                mode=CycleMode.APPLY,
                policy=RunPolicy.NEW_ARRIVAL,
                cycle_key=f"c{index + 1}",
            )
        state_a = store.get_file(500)
        state_b = store.get_file(501)
    finally:
        store.close()
    assert shoko.posts_count() == POSTS_TWO
    assert state_a is not None
    assert state_a.state is FileState.VERIFIED
    assert state_b is not None
    assert state_b.state is FileState.VERIFIED


def make_settings_shifted(tmp_path: Path) -> Settings:
    """Same config shape with a DIFFERENT Sonarr instance URL (source identity change)."""
    document = {
        "sonarr": {
            "url": "http://sonarr:8990",
            "api_key_env": "SONARR_API_KEY",
            "allowed_roots": [SONARR_ROOT],
        },
        "shoko": {"url": SHOKO_BASE, "api_key_env": "SHOKO_API_KEY", "allowed_roots": [SHOKO_ROOT]},
        "paths": {"mappings": [{"sonarr": SONARR_ROOT, "shoko": SHOKO_ROOT}]},
        "mappings": {"source": "anime-lists", "source_url": SOURCE_URL, "refresh_hours": HOURS},
        "sync": {"apply_changes": True, "max_concurrent_links": 4},
        "database": {"path": "/daemon/state.sqlite3"},
    }
    return parse_settings(yaml.safe_dump(document))


def test_identity_change_suppresses_apply_until_reanchored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A source identity change holds writes across clean polls until operator recovery."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    a = SpecFile(series_id=1, file_id=10, shoko_id=500)
    b = SpecFile(series_id=2, file_id=20, shoko_id=501)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([a, b])
    sleeper = Sleeper()
    settings_a = make_settings(tmp_path)
    settings_b = make_settings_shifted(tmp_path)
    try:
        deps_a = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a],
            shoko_files=[a],
            xml=mapping_xml(),
            monotonic=_rising_clock(0.0, 6),
            advance=sleeper.advance,
            shoko=shoko,
            settings=settings_a,
            max_submissions=1,
        )
        r1 = run_cycle(
            deps_a,
            now_iso=T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c1",
        )
        deps_shift = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a, b],
            shoko_files=[a, b],
            xml=mapping_xml(),
            monotonic=_rising_clock(20.0, 6),
            advance=sleeper.advance,
            shoko=shoko,
            settings=settings_b,
            max_submissions=1,
        )
        r2 = run_cycle(
            deps_shift,
            now_iso=_iso_offset(minutes=10),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c2",
        )
        flag_after_change = store.get_meta("requires_rebaseline")
        posts_after_change_cycle = shoko.posts_count()
        deps_stable = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a, b],
            shoko_files=[a, b],
            xml=mapping_xml(),
            monotonic=_rising_clock(40.0, 6),
            advance=sleeper.advance,
            shoko=shoko,
            settings=settings_b,
            max_submissions=1,
        )
        r3 = run_cycle(
            deps_stable,
            now_iso=_iso_offset(minutes=20),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c3",
        )
        flag_after_reanchor = store.get_meta("requires_rebaseline")
        store.clear_requires_rebaseline(_iso_offset(minutes=25))
        recovered = run_cycle(
            deps_stable,
            now_iso=_iso_offset(minutes=30),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c4",
        )
    finally:
        store.close()
    assert r1.counts.submitted == 1
    # Identity changed: discontinuity → flag persisted, apply suppressed, ZERO posts.
    assert "instance_identity_changed" in r2.reasons
    assert r2.writes_suppressed is True
    assert "source_unstable" in r2.reasons
    assert flag_after_change == "1"
    assert posts_after_change_cycle == POSTS_ONE
    assert flag_after_reanchor == "1"
    assert r3.counts.submitted == 0
    assert recovered.counts.submitted == 1
    assert shoko.posts_count() == POSTS_TWO


def test_verified_send_releases_exactly_one_slot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P1-2 probe: with max_pending_files=2 and 4 candidates, cycle1 verifies one
    send and keeps one verifying — pending_files must equal the true in-flight count (1);
    the old code double-released (verification close AND mark_completed) so a live send's
    slot vanished and the ceiling was breached. Cycle2: the verifying send closes
    (0 extra posts) and the last two submit; pending returns to 0."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    files = [
        SpecFile(series_id=index + 1, file_id=10 + index, shoko_id=500 + index)
        for index in range(4)
    ]
    activate_mapping(tmp_path, mapping_xml())
    store = StateStore(tmp_path / "state.sqlite3", budget_limits=TWO_FILE_LIMITS)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = _LazyLinkStub(files, instant_ids=[500, 502, 503])
    sleeper = Sleeper()
    try:
        deps1 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=files,
            shoko_files=files,
            xml=mapping_xml(),
            monotonic=_rising_clock(0.0, 60),
            advance=sleeper.advance,
            shoko=shoko,
            max_submissions=2,
            poll_interval=1.0,
        )
        r1 = run_cycle(
            deps1,
            now_iso=T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c1",
        )
        view1 = store.budget_view()
        shoko.materialize(501)
        deps2 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=files,
            shoko_files=files,
            xml=mapping_xml(),
            monotonic=_rising_clock(100.0, 60),
            advance=sleeper.advance,
            shoko=shoko,
            max_submissions=2,
            poll_interval=1.0,
        )
        r2 = run_cycle(
            deps2,
            now_iso=_iso_offset(minutes=5),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c2",
        )
        view2 = store.budget_view()
        record501 = store.get_file(501)
    finally:
        store.close()
    assert r1.counts.submitted == COUNT_TWO
    assert view1.pending_file_requests == 1  # exactly the in-flight send (501 verifying)
    assert view1.pending_file_requests <= TWO_FILE_LIMITS.max_pending_files
    assert view1.pending_episode_jobs == 1
    assert r2.counts.submitted == COUNT_TWO
    assert shoko.posts_count() == POSTS_FOUR
    assert record501 is not None
    assert record501.state is FileState.VERIFIED
    assert view2.pending_file_requests == 0
    assert view2.pending_episode_jobs == 0


def test_restart_verifying_row_repolls_to_verified(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P1-3 probe C4: cycle1 ends verifying (PENDING); the link materializes server-side;
    cycle2 (no new candidates) re-polls the persisted row to VERIFIED, releases the slot,
    and issues ZERO extra POSTs."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    a = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = _LazyLinkStub([a], instant_ids=[])
    sleeper = Sleeper()
    try:
        deps1 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a],
            shoko_files=[a],
            xml=mapping_xml(),
            monotonic=_rising_clock(0.0, 40),
            advance=sleeper.advance,
            shoko=shoko,
            poll_interval=1.0,
        )
        run_cycle(
            deps1, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
        record1 = store.get_file(500)
        view1 = store.budget_view()
        posts_after_cycle1 = shoko.posts_count()
        shoko.materialize(500)
        deps2 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a],
            shoko_files=[a],
            xml=mapping_xml(),
            monotonic=_rising_clock(100.0, 20),
            advance=sleeper.advance,
            shoko=shoko,
            poll_interval=1.0,
        )
        run_cycle(
            deps2,
            now_iso=_iso_offset(minutes=5),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c2",
        )
        record2 = store.get_file(500)
        view2 = store.budget_view()
    finally:
        store.close()
    assert record1 is not None
    assert record1.state is FileState.VERIFYING
    assert view1.pending_file_requests == 1
    assert posts_after_cycle1 == 1
    assert record2 is not None
    assert record2.state is FileState.VERIFIED
    assert view2.pending_file_requests == 0
    assert shoko.posts_count() == 1  # zero extra POSTs in the recovery cycle
    assert all(request.method != "DELETE" for request in shoko.requests)


def test_mark_sent_crash_past_deadline_records_partial_and_holds_slot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P1-3 probe P2: crash after mark_sent (request_may_have_been_sent), XRefs show
    NOT linked, deadline past → durable PARTIAL review, slot held, posts stay 1 — no resubmit."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    a = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([a])
    shoko.link_on_post = False
    shoko.fail_post = RuntimeError("crash after mark_sent")
    try:
        deps1 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a],
            shoko_files=[a],
            xml=mapping_xml(),
            monotonic=_rising_clock(0.0, 20),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        with pytest.raises(RuntimeError, match="crash after mark_sent"):
            run_cycle(
                deps1,
                now_iso=T0,
                mode=CycleMode.APPLY,
                policy=RunPolicy.NEW_ARRIVAL,
                cycle_key="c1",
            )
        shoko.fail_post = None
        deps2 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a],
            shoko_files=[a],
            xml=mapping_xml(),
            monotonic=_rising_clock(100.0, 20),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        report = run_cycle(
            deps2,
            now_iso=_iso_offset(minutes=20),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c2",
        )
        record = store.get_file(500)
        view = store.budget_view()
        live_counts = store.live_send_counts()
        recovery = store.recover_after_restart()
    finally:
        store.close()
    assert record is not None
    assert record.state is FileState.PARTIAL
    assert view.pending_file_requests == 1
    assert live_counts == (COUNT_ONE, COUNT_ONE)
    assert _raw_read(
        store.path,
        "SELECT outcome, verification, slot_released FROM sends WHERE shoko_file_id = ?",
        (500,),
    ) == [("inspected_review", "partial", 0)]
    assert 500 not in {
        row.shoko_file_id for row in (*recovery.hold_for_verification, *recovery.continue_polling)
    }
    assert shoko.posts_count() == 1
    assert any("recovery_hold:500:partial" in reason for reason in report.reasons)
    assert all(request.method != "DELETE" for request in shoko.requests)


def test_may_have_been_sent_unrelated_link_is_contradiction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P1-3 wrong-link probe: XRefs show an UNRELATED link → contradiction report, NO
    unlink, NO resubmission (slot stays held for operator review)."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    a = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([a])
    shoko.link_on_post = False
    shoko.fail_post = RuntimeError("crash after mark_sent")
    try:
        deps1 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a],
            shoko_files=[a],
            xml=mapping_xml(),
            monotonic=_rising_clock(0.0, 20),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        with pytest.raises(RuntimeError, match="crash after mark_sent"):
            run_cycle(
                deps1,
                now_iso=T0,
                mode=CycleMode.APPLY,
                policy=RunPolicy.NEW_ARRIVAL,
                cycle_key="c1",
            )
        shoko.fail_post = None
        shoko.link_unrelated(500)
        deps2 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a],
            shoko_files=[a],
            xml=mapping_xml(),
            monotonic=_rising_clock(100.0, 20),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        run_cycle(
            deps2,
            now_iso=_iso_offset(minutes=5),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c2",
        )
        record = store.get_file(500)
        view = store.budget_view()
    finally:
        store.close()
    assert record is not None
    assert record.state is FileState.CONFLICT
    assert shoko.posts_count() == 1
    assert view.pending_file_requests == 1  # held for operator review, never auto-repaired
    assert all(request.method != "DELETE" for request in shoko.requests)


def test_http_202_pending_send_is_held_until_restart_xref_inspection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """HTTP 202 is accepted, not terminal; restart recovery inspects without reposting."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    candidate = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    path = tmp_path / "state.sqlite3"
    store = StateStore(path, budget_limits=WIDE_LIMITS)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([candidate])
    shoko.post_status = 202
    shoko.link_on_post = False
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=[candidate],
        shoko_files=[candidate],
        xml=mapping_xml(),
        monotonic=_rising_clock(0.0, 30),
        advance=Sleeper().advance,
        shoko=shoko,
    )
    deps = replace(deps, max_verify_polls=1)
    try:
        first = run_cycle(
            deps,
            now_iso=T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="accepted-202",
        )
        inspection = store.active_send_details(candidate.shoko_id)
        assert inspection is not None
        assert inspection.outcome == "accepted_unverified"
        assert store.budget_view().pending_file_requests == 1
        assert _raw_read(
            path,
            "SELECT http_status, verification, outcome FROM sends WHERE send_id = ?",
            (inspection.send_id,),
        ) == [(202, "pending", "accepted_unverified")]
        assert first.counts.submitted == 1
        with pytest.raises(StateError, match="inspected_review"):
            store.resolve_send(
                candidate.shoko_id,
                inspection.request_fingerprint,
                resolution="released_after_review",
                iso=T1,
                verification_timeout_seconds=VERIFICATION_TIMEOUT,
            )
    finally:
        store.close()

    reopened = StateStore(path, budget_limits=WIDE_LIMITS)
    deps_after_restart = build_deps(
        tmp_path,
        store=reopened,
        sonarr_files=[candidate],
        shoko_files=[candidate],
        xml=mapping_xml(),
        monotonic=_rising_clock(100.0, 30),
        advance=Sleeper().advance,
        shoko=shoko,
    )
    try:
        run_cycle(
            deps_after_restart,
            now_iso=T1,
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="recovery-after-202",
        )
        row = _raw_read(
            path,
            "SELECT outcome, verification, slot_released FROM sends WHERE shoko_file_id = ?",
            (candidate.shoko_id,),
        )
        record = reopened.get_file(candidate.shoko_id)
        assert row == [("inspected_review", "contradiction", 0)]
        assert record is not None
        assert record.state is FileState.CONFLICT
        assert reopened.budget_view().pending_file_requests == 1
        assert reopened.active_send_details(candidate.shoko_id) is None
    finally:
        reopened.close()
        for deps_to_close in (deps, deps_after_restart):
            for endpoint in (
                deps_to_close.sonarr,
                deps_to_close.shoko,
                deps_to_close.mapping_source,
            ):
                endpoint.client.close()

    assert shoko.posts_count() == 1
    assert all(request.method != "DELETE" for request in shoko.requests)


@pytest.mark.parametrize(
    "inspection_case",
    ["empty", "contradictory", "partial", "exact", "wrong_ed2k", "wrong_file_id"],
)
def test_crash_send_inspection_is_durable_and_terminal_without_repost(  # noqa: PLR0915
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, inspection_case: str
) -> None:
    """A reopened crash-after-mark-sent row is resolved only by same-file XRef inspection.

    Empty, partial, and contradictory observations become durable operator-review outcomes,
    retain the pending slot, and leave no polling work after restart. Exact target XRefs alone
    close as verified. Recovery never POSTs in any case.
    """
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    episodes = (
        ((1, 1, ANIDB_EP, SHOKO_EP), (1, 2, ANIDB_EP + 1, SHOKO_EP + 1))
        if inspection_case == "partial"
        else ((1, 1, ANIDB_EP, SHOKO_EP),)
    )
    candidate = SpecFile(series_id=1, file_id=10, shoko_id=500, episodes=episodes)
    xml = mapping_xml().replace(";1-1;", ";1-1;2-2;") if len(episodes) > 1 else mapping_xml()
    activate_mapping(tmp_path, xml)
    path = tmp_path / "state.sqlite3"
    store = StateStore(path, budget_limits=WIDE_LIMITS)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([candidate])
    shoko.link_on_post = False
    shoko.fail_post = RuntimeError("unknown HTTP outcome after mark_sent")
    try:
        deps1 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[candidate],
            shoko_files=[candidate],
            xml=xml,
            monotonic=_rising_clock(0.0, 20),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        with pytest.raises(RuntimeError, match="unknown HTTP outcome"):
            run_cycle(
                deps1,
                now_iso=T0,
                mode=CycleMode.APPLY,
                policy=RunPolicy.NEW_ARRIVAL,
                cycle_key="crash",
            )
    finally:
        store.close()

    shoko.fail_post = None
    if inspection_case == "contradictory":
        shoko.link_unrelated(candidate.shoko_id)
    elif inspection_case == "partial":
        shoko.materialize(candidate.shoko_id, episodes=(episodes[0],))
    elif inspection_case == "exact":
        shoko.materialize(candidate.shoko_id)
    elif inspection_case in {"wrong_ed2k", "wrong_file_id"}:
        shoko.link_unrelated(candidate.shoko_id)
        if inspection_case == "wrong_ed2k":
            shoko.ed2k_overrides[candidate.shoko_id] = "f" * 32
        else:
            shoko.file_id_overrides[candidate.shoko_id] = candidate.shoko_id + 1

    reopened = StateStore(path, budget_limits=WIDE_LIMITS)
    release_apply_lock(reopened, pid=4242, iso=T1)
    try:
        deps2 = build_deps(
            tmp_path,
            store=reopened,
            sonarr_files=[candidate],
            shoko_files=[candidate],
            xml=xml,
            monotonic=_rising_clock(100.0, 20),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        run_cycle(
            deps2,
            now_iso=T1,
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="recovery",
        )
        record = reopened.get_file(candidate.shoko_id)
        recovery = reopened.recover_after_restart()
        pending = reopened.budget_view().pending_file_requests
        active_send = reopened.active_send_details(candidate.shoko_id)
        if inspection_case in {"wrong_ed2k", "wrong_file_id"}:
            assert active_send is not None
            with pytest.raises(StateError):
                reopened.resolve_send(
                    candidate.shoko_id,
                    active_send.request_fingerprint,
                    resolution="released_after_review",
                    iso=T2,
                    operator_note="identity-mismatched XRef response",
                )
        send_rows = _raw_read(
            path,
            "SELECT sends.outcome, sends.verification, sends.slot_released, files.state"
            " FROM sends JOIN files USING (shoko_file_id) WHERE sends.shoko_file_id = ?",
            (candidate.shoko_id,),
        )
    finally:
        reopened.close()

    assert shoko.posts_count() == 1
    assert all(request.method != "DELETE" for request in shoko.requests)
    assert record is not None
    assert send_rows == [
        (
            "verified"
            if inspection_case == "exact"
            else "attempted"
            if inspection_case in {"wrong_ed2k", "wrong_file_id"}
            else "inspected_review",
            "verified"
            if inspection_case == "exact"
            else None
            if inspection_case in {"wrong_ed2k", "wrong_file_id"}
            else "contradiction"
            if inspection_case == "contradictory"
            else "partial",
            1 if inspection_case == "exact" else 0,
            {
                "exact": FileState.VERIFIED.value,
                "contradictory": FileState.CONFLICT.value,
                "partial": FileState.PARTIAL.value,
                "empty": FileState.PARTIAL.value,
                "wrong_ed2k": FileState.REQUEST_MAY_HAVE_BEEN_SENT.value,
                "wrong_file_id": FileState.REQUEST_MAY_HAVE_BEEN_SENT.value,
            }[inspection_case],
        )
    ]
    assert pending == (0 if inspection_case == "exact" else 1)
    assert (active_send is not None) == (inspection_case in {"wrong_ed2k", "wrong_file_id"})
    if inspection_case in {"wrong_ed2k", "wrong_file_id"}:
        assert candidate.shoko_id in {row.shoko_file_id for row in recovery.hold_for_verification}
    else:
        assert candidate.shoko_id not in {
            row.shoko_file_id
            for row in (*recovery.hold_for_verification, *recovery.continue_polling)
        }


def test_prepared_crash_row_voided_and_reprepared(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P2-2 probe C7: prepared-then-crashed row is voided next cycle (audit row
    ``voided``, slot released, file back to planned) and re-prepared + submitted in
    that same cycle; pending returns to 0."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    a = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    path = tmp_path / "state.sqlite3"
    store = StateStore(path, budget_limits=WIDE_LIMITS)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([a])
    sleeper = Sleeper()

    def boom(_token: Any, *, iso: str) -> None:
        raise RuntimeError("crash between prepare_send and transport")

    monkeypatch.setattr(store, "mark_sent", boom)
    try:
        deps1 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a],
            shoko_files=[a],
            xml=mapping_xml(),
            monotonic=_rising_clock(0.0, 20),
            advance=sleeper.advance,
            shoko=shoko,
        )
        with pytest.raises(RuntimeError, match="crash between prepare_send"):
            run_cycle(
                deps1,
                now_iso=T0,
                mode=CycleMode.APPLY,
                policy=RunPolicy.NEW_ARRIVAL,
                cycle_key="c1",
            )
        record1 = store.get_file(500)
        holder1 = store.lock_holder()
        view1 = store.budget_view()
        del store.mark_sent  # drop the instance patch: the real bound method is used again
        assert "mark_sent" not in store.__dict__
        deps2 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a],
            shoko_files=[a],
            xml=mapping_xml(),
            monotonic=_rising_clock(100.0, 20),
            advance=sleeper.advance,
            shoko=shoko,
        )
        run_cycle(
            deps2,
            now_iso=_iso_offset(minutes=5),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c2",
        )
        record2 = store.get_file(500)
        view2 = store.budget_view()
    finally:
        store.close()
    assert record1 is not None
    assert record1.state is FileState.PREPARED_NOT_SENT
    assert holder1 is None  # P2-1: the mid-cycle crash released the apply lock
    assert view1.pending_file_requests == 1
    assert shoko.posts_count() == 1
    assert record2 is not None
    assert record2.state is FileState.VERIFIED
    assert view2.pending_file_requests == 0
    assert view2.pending_episode_jobs == 0
    rows = _raw_read(path, "SELECT outcome FROM sends WHERE shoko_file_id = 500 ORDER BY send_id")
    assert [tuple(row) for row in rows] == [("voided",), ("verified",)]


def test_expired_apply_lock_is_stolen_and_cycle_applies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P2-1 probe C6: a wedged lock (crashed holder) no longer suppresses apply
    forever — past the holder's lease expiry the cycle steals (audit-recorded), posts,
    and releases cleanly."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    a = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([a])
    acquire_apply_lock(store, pid=999, iso=T0, lease_expires_iso=_iso_offset(minutes=1))
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a],
            shoko_files=[a],
            xml=mapping_xml(),
            monotonic=_rising_clock(0.0, 20),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        report = run_cycle(
            deps,
            now_iso=_iso_offset(minutes=10),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c1",
        )
        holder_after = store.lock_holder()
        audit = json.loads(store.get_meta("apply_lock_steals") or "[]")
    finally:
        store.close()
    assert shoko.posts_count() == 1
    assert report.counts.submitted == 1
    assert holder_after is None
    assert audit[0]["from_pid"] == STALE_LOCK_PID
    assert audit[0]["owner_pid"] == DAEMON_PID


def test_live_apply_lock_cycle_is_read_only_names_holder(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P2-1 probe C6 (before expiry): a live lease is contended — read-only cycle, holder
    named, zero posts, the lease is NOT stolen."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    a = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([a])
    acquire_apply_lock(store, pid=999, iso=T0, lease_expires_iso=_iso_offset(minutes=120))
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a],
            shoko_files=[a],
            xml=mapping_xml(),
            monotonic=_rising_clock(0.0, 20),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        report = run_cycle(
            deps,
            now_iso=_iso_offset(minutes=10),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c1",
        )
        holder = store.lock_holder()
    finally:
        store.close()
    assert shoko.posts_count() == 0
    assert report.reasons[0].startswith("apply_lock_contended: holder 999")
    assert report.writes_suppressed is True
    assert holder is not None
    assert holder.owner_pid == STALE_LOCK_PID


def test_mid_cycle_crash_releases_the_apply_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P2-1: the cycle body runs under try/finally — a mid-cycle exception always
    releases the lock (no manual release_apply_lock needed after a crash)."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    a = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([a])
    shoko.fail_post = RuntimeError("simulated mid-cycle crash")
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a],
            shoko_files=[a],
            xml=mapping_xml(),
            monotonic=_rising_clock(0.0, 20),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        with pytest.raises(RuntimeError, match="simulated mid-cycle crash"):
            run_cycle(
                deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
            )
        holder = store.lock_holder()
    finally:
        store.close()
    assert holder is None


def test_identity_rule_change_reevaluates_ambiguous_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P2-3 probe C5: a family whose UNBOUNDED offset rule appears in generation 2
    moves no enumerable target digest. The identity-rule diff must mark the series
    affected so ambiguous files re-walk to planned. Cohort gating stays separate: the
    backlog file re-evaluates to planned with ZERO posts; only the new_arrival submits."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    backlog = SpecFile(
        series_id=2,
        file_id=20,
        shoko_id=600,
        tvdb_series=999,
        anime_id=1002,
        episodes=((1, 2, ANIDB_EP, SHOKO_EP),),
    )
    arrival = SpecFile(
        series_id=2,
        file_id=21,
        shoko_id=601,
        tvdb_series=999,
        anime_id=1002,
        episodes=((1, 2, ANIDB_EP, SHOKO_EP),),
    )
    gen1 = "<anime-list>" + _entry_offset(999, 1001, 2, 0) + "</anime-list>"
    gen2 = (
        "<anime-list>"
        + _entry_offset(999, 1001, 2, 0)
        + _entry_offset(999, 1002, 1, 0)
        + "</anime-list>"
    )
    activate_mapping(tmp_path, gen1)
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [backlog])  # the backlog file predates the baseline
    shoko = ShokoStub([backlog, arrival])
    sleeper = Sleeper()
    try:
        deps1 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[backlog, arrival],
            shoko_files=[backlog, arrival],
            xml=gen1,
            monotonic=_rising_clock(0.0, 10),
            advance=sleeper.advance,
            shoko=shoko,
        )
        run_cycle(
            deps1, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
        state1 = store.get_file(600)
        assert state1 is not None
        assert state1.state is FileState.AMBIGUOUS
        deps2 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[backlog, arrival],
            shoko_files=[backlog, arrival],
            xml=gen2,
            monotonic=_rising_clock(20.0, 10),
            advance=sleeper.advance,
            shoko=shoko,
        )
        report = run_cycle(
            deps2,
            now_iso=_due_iso(),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c2",
        )
        backlog_state = store.get_file(600)
        arrival_state = store.get_file(601)
    finally:
        store.close()
    assert report.mapping_outcome is RefreshOutcome.ACTIVATED
    assert backlog_state is not None
    assert backlog_state.state is FileState.PLANNED
    assert arrival_state is not None
    assert arrival_state.state is FileState.VERIFIED
    assert shoko.posts_count() == 1  # backlog stays no-post; only the new arrival submitted
    assert shoko.posts[0]["EpisodeIDs"] == [SHOKO_EP]


def _entry_offset(tvdb: int, anidb: int, season: int, offset: int) -> str:
    return (
        f"<anime anidbid='{anidb}' tvdbid='{tvdb}' defaulttvdbseason='{season}'"
        f" episodeoffset='{offset}'></anime>"
    )


def test_multi_episode_send_reserves_real_job_count(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P2-4 probe C8: a 2-episode plan reserves 2 episode jobs (not 1); the
    REJECTED release frees the stored 2 — pending_episode_jobs returns to 0, not 1."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    multi = SpecFile(
        series_id=1,
        file_id=10,
        shoko_id=500,
        episodes=((1, 1, ANIDB_EP, SHOKO_EP), (1, 2, ANIDB_EP + 1, SHOKO_EP + 1)),
    )
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([multi])
    shoko.post_status = 400  # Shoko refuses (zero writes per the pinned contract)
    reserved: list[int | None] = []
    real_prepare = store.prepare_send

    def spy_prepare(*args: Any, **kwargs: Any) -> Any:
        reserved.append(kwargs.get("expected_episode_jobs"))
        return real_prepare(*args, **kwargs)

    monkeypatch.setattr(store, "prepare_send", spy_prepare)
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[multi],
            shoko_files=[multi],
            xml=mapping_xml(),
            monotonic=_rising_clock(0.0, 20),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
        record = store.get_file(500)
        view = store.budget_view()
    finally:
        store.close()
    assert reserved == [JOBS_TWO]  # the daemon reserves the plan's real job count
    assert record is not None
    assert record.state is FileState.WAITING
    assert view.pending_episode_jobs == 0
    assert view.pending_file_requests == 0


def test_budgets_exhausted_cycle_reports_reason(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P3c: suppression paths surface the compute_eligibility refusal reasons."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    a = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    zero_limits = BudgetLimits(
        max_pending_files=0,
        max_pending_episode_jobs=5,
        per_cycle_new=5,
        rolling_new=5,
        rolling_window_hours=24.0,
    )
    store = StateStore(tmp_path / "state.sqlite3", budget_limits=zero_limits)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([a])
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a],
            shoko_files=[a],
            xml=mapping_xml(),
            monotonic=_rising_clock(0.0, 10),
            advance=Sleeper().advance,
            shoko=shoko,
        )
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
    finally:
        store.close()
    assert "budgets_exhausted" in report.reasons
    assert report.apply_eligible is False
    assert shoko.posts_count() == 0


def test_first_scan_bulk_record_wall_time(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """P3b: first-scan bulk ingest (record_files) — 7k-file cycle wall-time target."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    files = [
        SpecFile(series_id=index % 70 + 1, file_id=index, shoko_id=10_000 + index)
        for index in range(1, FIRST_RUN_COUNT + 1)
    ]
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=files,
            shoko_files=files,
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
        )
        start = time.perf_counter()
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
        elapsed = time.perf_counter() - start
    finally:
        store.close()
    assert report.counts.backlog == FIRST_RUN_COUNT
    assert report.counts.submitted == 0  # first scan never applies the backlog
    assert elapsed < FIRST_SCAN_WALL_BUDGET_SECONDS


def test_run_mapping_check_is_due_only(tmp_path: Path) -> None:
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[],
            shoko_files=[],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
        )
        deps = _swap_source(
            deps,
            lambda request: httpx.Response(
                304, headers={"ETag": '"identity-src"'}, request=request
            ),
        )
        # Not due at T1 (activated at T0 with a 168h schedule).
        assert run_mapping_check(deps, T1) is RefreshOutcome.NOT_DUE
        due_time = _parse(T0) + _days(7)
        assert run_mapping_check(deps, due_time.isoformat()) is RefreshOutcome.UNCHANGED
    finally:
        store.close()


# ------------------------------------------------------------------ second-round regressions


class _TruncatedListStub(ShokoStub):
    """Discovery whose page reports an inflated Total (999999) with a short List."""

    def _listing(self, request: httpx.Request) -> httpx.Response:
        rows = [self._file_row(shoko_id, xrefs=[]) for shoko_id in sorted(self.files)]
        return self._json(request, {"Total": 999999, "List": rows})


class _EmptyXRefStub(ShokoStub):
    """Recovery-drain stub: discovery is empty and any ``File/{id}?include=XRefs`` read
    returns a zero-xref (Unrecognized) file row, so past-deadline polls terminate (CONTRADICTION).
    """

    def _listing(self, request: httpx.Request) -> httpx.Response:
        return self._json(request, {"Total": 0, "List": []})

    def _file(self, request: httpx.Request, path: str) -> httpx.Response:
        shoko_id = int(path.split("/")[4])
        row: dict[str, Any] = {
            "ID": shoko_id,
            "Size": FILE_SIZE,
            "IsIgnored": False,
            "IsVariation": False,
            "Imported": "2024-01-01T00:00:00",
            "Hashes": hf.ed2k_hashes(shoko_id, FILE_SIZE, f"{shoko_id}.mkv"),
            "Locations": [],
            "SeriesIDs": [],
        }
        return self._json(request, row)


def _seed_verifying_rows(store: StateStore, count: int) -> None:
    """Bulk-seed persisted ``verifying`` rows with a live send row (past deadline)."""
    conn = sqlite3.connect(str(store.path))
    conn.execute("PRAGMA busy_timeout=5000")
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.executemany(
            "INSERT INTO files (shoko_file_id, sonarr_series_id, sonarr_file_id, sonarr_path,"
            " sonarr_size, state, cohort, state_detail, updated_iso, first_seen_iso,"
            " identity_hash_json)"
            " VALUES (?, ?, ?, ?, ?, 'verifying', 'new_arrival', 'verify', ?, ?, ?)",
            [
                (
                    file_id,
                    file_id % 50 + 1,
                    file_id,
                    f"/data/media/anime/series{file_id}/f{file_id}.mkv",
                    1000 + file_id,
                    T0,
                    T0,
                    json.dumps(
                        {
                            "ed2k": hf.hex32(file_id),
                            "sha1": None,
                            "md5": None,
                            "crc32": None,
                            "paths": [],
                        }
                    ),
                )
                for file_id in range(1, count + 1)
            ],
        )
        conn.executemany(
            "INSERT INTO sends (shoko_file_id, request_fingerprint, plan_fingerprint,"
            " attempted_iso, outcome, episode_jobs, target_ids_json) VALUES (?, ?, ?, ?,"
            " 'accepted_unverified', 1, ?)",
            [
                (
                    file_id,
                    format(file_id, "064x"),
                    "a" * 64,
                    T0,
                    json.dumps([[ANIME_ID, ANIDB_EP, SHOKO_EP]]),
                )
                for file_id in range(1, count + 1)
            ],
        )
        conn.commit()
    finally:
        conn.close()


ONE_FILE_LIMITS = BudgetLimits(
    max_pending_files=1,
    max_pending_episode_jobs=4,
    per_cycle_new=10,
    rolling_new=10,
    rolling_window_hours=24.0,
)


def test_partial_inspection_holds_slot_until_operator_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A terminal PARTIAL review holds the ceiling until explicit operator resolution."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    a = SpecFile(series_id=1, file_id=10, shoko_id=500)
    b = SpecFile(series_id=2, file_id=20, shoko_id=501)
    activate_mapping(tmp_path, mapping_xml())
    store = StateStore(tmp_path / "state.sqlite3", budget_limits=ONE_FILE_LIMITS)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([a, b])
    shoko.link_on_post = False
    shoko.fail_post = RuntimeError("crash after mark_sent")
    sleeper = Sleeper()
    try:
        # Cycle 1: submit A, crash after mark_sent → request_may_have_been_sent, held.
        deps1 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a],
            shoko_files=[a],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=sleeper.advance,
            shoko=shoko,
        )
        with pytest.raises(RuntimeError, match="crash after mark_sent"):
            run_cycle(
                deps1,
                now_iso=T0,
                mode=CycleMode.APPLY,
                policy=RunPolicy.NEW_ARRIVAL,
                cycle_key="c1",
            )
        assert store.budget_view().pending_file_requests == 1  # A holds the only slot
        shoko.fail_post = None
        # Cycle 2 past the deadline: NOT-linked → terminal PARTIAL review, still held.
        deps2 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a, b],
            shoko_files=[a, b],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=sleeper.advance,
            shoko=shoko,
        )
        r2 = run_cycle(
            deps2,
            now_iso=_iso_offset(minutes=5),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c2",
        )
        file_500 = store.get_file(500)
        assert file_500 is not None
        assert file_500.state is FileState.PARTIAL
        assert r2.counts.submitted == 0
        assert store.budget_view().pending_file_requests == 1
        assert store.live_send_counts() == (COUNT_ONE, COUNT_ONE)
        assert _raw_read(
            store.path,
            "SELECT outcome, verification, slot_released FROM sends WHERE shoko_file_id = ?",
            (500,),
        ) == [("inspected_review", "partial", 0)]
        row = store.connection.execute(
            "SELECT request_fingerprint FROM sends WHERE shoko_file_id = ?"
            " ORDER BY send_id DESC LIMIT 1",
            (500,),
        ).fetchone()
        assert row is not None

        # Only an explicit review resolution releases the pending slot for B.
        store.resolve_send(
            500,
            str(row[0]),
            resolution="released_after_review",
            iso=_iso_offset(minutes=10),
            operator_note="reviewed terminal same-file XRefs",
            verification_timeout_seconds=VERIFICATION_TIMEOUT,
        )
        assert store.budget_view().pending_file_requests == 0
        shoko.link_on_post = True
        deps3 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a, b],
            shoko_files=[a, b],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=sleeper.advance,
            shoko=shoko,
            max_submissions=1,
        )
        r3 = run_cycle(
            deps3,
            now_iso=_iso_offset(minutes=15),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c4",
        )
        record_b = store.get_file(501)
    finally:
        store.close()
    assert r3.counts.submitted == 1
    assert shoko.posts_count() == POSTS_TWO  # A (crashed) + B
    assert record_b is not None
    assert record_b.state is FileState.VERIFIED


def test_recovery_pending_deadline_terminates_partial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A persisted PENDING row keeps its anchored deadline and holds after terminal PARTIAL.

    With interval 20 s and timeout 50 s, recovery becomes terminal after the deadline without
    sliding it, while retaining the reservation for explicit operator resolution.
    """
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    a = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([a])
    shoko.link_on_post = False
    shoko.fail_post = RuntimeError("crash after mark_sent")
    sleeper = Sleeper()
    cycle_count = 11
    try:
        deps1 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a],
            shoko_files=[a],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=sleeper.advance,
            shoko=shoko,
        )
        with pytest.raises(RuntimeError, match="crash after mark_sent"):
            run_cycle(
                deps1,
                now_iso=T0,
                mode=CycleMode.APPLY,
                policy=RunPolicy.NEW_ARRIVAL,
                cycle_key="c0",
            )
        shoko.fail_post = None
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a],
            shoko_files=[a],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=sleeper.advance,
            shoko=shoko,
            verification_timeout=50.0,
            poll_interval=1.0,
        )
        times = iter([_iso_offset_seconds(T0, 60.0 + 20.0 * index) for index in range(cycle_count)])
        report = run_daemon(
            deps,
            cycle_key=lambda index: f"c{index}",
            now_iso=lambda: next(times),
            interval_seconds=20.0,
            stop_after_cycles=cycle_count,
        )
        record = store.get_file(500)
        view = store.budget_view()
        get_count = sum(1 for request in shoko.requests if request.method == "GET")
        live_counts = store.live_send_counts()
        active_send = store.active_send_details(500)
    finally:
        store.close()
    assert record is not None
    assert record.state in (FileState.PARTIAL, FileState.CONFLICT)  # terminal, not PENDING
    assert record.state is not FileState.VERIFYING
    assert view.pending_file_requests == 1  # terminal review retains its reservation
    assert live_counts == (COUNT_ONE, COUNT_ONE)
    assert active_send is None
    assert _raw_read(
        store.path,
        "SELECT outcome, verification, slot_released FROM sends WHERE shoko_file_id = ?",
        (500,),
    ) == [("inspected_review", "partial", 0)]
    assert (
        shoko.posts_count() == POSTS_ONE
    )  # the single crash-cycle submit; recovery adds zero POSTs
    assert get_count < 8 * cycle_count  # GETs bounded per cycle (old: deadline slid forever)
    assert any(
        reason.startswith("recovery_hold:500:partial") for reason in report.cycles[0].reasons
    )


def test_truncated_discovery_does_not_wedge_continuity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P1-C probe (reviewer p3, daemon level): a truncated page reports Total 999999. It must
    NOT anchor the continuity total; the next honest complete cycle (total 10) must not fire
    discovery_total_drop, apply stays eligible, and the new arrival posts."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    a = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    sleeper = Sleeper()
    try:
        # Cycle 1: truncated discovery (inflated Total 999999) → apply suppressed, no anchor.
        shoko_trunc = _TruncatedListStub([a])
        deps1 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a],
            shoko_files=[a],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=sleeper.advance,
            shoko=shoko_trunc,
            max_submissions=1,
        )
        r1 = run_cycle(
            deps1, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
        flag_after_trunc = store.get_meta("requires_rebaseline")
        trunc_posts = shoko_trunc.posts_count()
        # Cycle 2: honest complete discovery (Total = 1 file) → eligible, posts.
        shoko = ShokoStub([a])
        deps2 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a],
            shoko_files=[a],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=sleeper.advance,
            shoko=shoko,
            max_submissions=1,
        )
        r2 = run_cycle(
            deps2,
            now_iso=_iso_offset(minutes=10),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c2",
        )
        record = store.get_file(500)
    finally:
        store.close()
    # Truncated cycle: no drop flag, apply suppressed, no posts.
    assert "discovery_incomplete" in r1.reasons
    assert flag_after_trunc != "1"
    assert r1.writes_suppressed is True
    assert trunc_posts == 0
    # Next honest cycle: NOT a drop wedge — eligible, re-anchored, posts, VERIFIED.
    assert "discovery_total_drop" not in r2.reasons
    assert r2.apply_eligible is True
    assert r2.counts.submitted == 1
    assert shoko.posts_count() == POSTS_ONE
    assert record is not None
    assert record.state is FileState.VERIFIED


def test_recovery_row_cap_bounds_cycle_and_terminates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P2-A probe (reviewer p7): 7k persisted verifying rows must not pin the cycle.
    A per-cycle recovery row cap bounds GETs per cycle, reports
    ``recovery_deferred``, and the backlog drains to terminal states across cycles."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    activate_mapping(tmp_path, mapping_xml())
    store = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
    store.set_meta("budget_last_cycle_key", "c1")
    _seed_verifying_rows(store, FIRST_RUN_COUNT)
    assert len(store.files_in_state(FileState.VERIFYING)) == FIRST_RUN_COUNT
    shoko = _EmptyXRefStub([])  # discovery empty → XRefs empty → each poll closes past deadline
    sleeper = Sleeper()
    cap = RECOVERY_ROW_CAP
    cycle_budget = 16
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[],
            shoko_files=[],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=sleeper.advance,
            shoko=shoko,
            max_recovery_rows_per_cycle=cap,
            max_cycle_seconds=1e9,
        )
        before = len(shoko.requests)
        r1 = run_cycle(
            deps,
            now_iso=_iso_offset(minutes=10),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c1",
        )
        first_cycle_gets = len(shoko.requests) - before
        start = time.perf_counter()
        report = run_daemon(
            deps,
            cycle_key=lambda index: f"c{index + 2}",
            now_iso=lambda: _iso_offset(minutes=10),
            interval_seconds=0.0,
            stop_after_cycles=cycle_budget,
        )
        elapsed = time.perf_counter() - start
        verifying_after = len(store.files_in_state(FileState.VERIFYING))
    finally:
        store.close()
    assert any(reason.startswith("recovery_deferred:") for reason in r1.reasons)
    assert first_cycle_gets <= cap + CYCLE_GET_OVERHEAD  # bounded by the row cap, not 7k
    assert shoko.posts_count() == 0
    assert verifying_after == 0  # drained to terminal states
    assert all(cycle.counts.submitted == 0 for cycle in report.cycles)
    assert elapsed < DRAIN_WALL_SECONDS


def test_recovery_cursor_rotates_after_unknown_row_across_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A persistent unknown read must not starve later verifying rows when the cap is one."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    activate_mapping(tmp_path, mapping_xml())
    store = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
    store.set_meta("budget_last_cycle_key", "c0")
    _seed_verifying_rows(store, 2)

    class OneUnknownRecoveryStub(_EmptyXRefStub):
        def _file(self, request: httpx.Request, path: str) -> httpx.Response:
            file_id = int(path.split("/")[4])
            if file_id == 1:
                return httpx.Response(503, request=request)
            response = super()._file(request, path)
            payload = response.json()
            response.close()
            payload["SeriesIDs"] = [
                {
                    "SeriesID": {"ID": None, "AniDB": ANIME_ID, "TMDB": None},
                    "EpisodeIDs": [
                        {
                            "ID": SHOKO_EP,
                            "AniDB": ANIDB_EP,
                            "TMDB": None,
                            "Source": "User",
                        }
                    ],
                }
            ]
            return self._json(request, payload)

    shoko = OneUnknownRecoveryStub([])
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[],
            shoko_files=[],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            max_recovery_rows_per_cycle=1,
        )
        first = run_cycle(
            deps,
            now_iso=T1,
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="recovery-1",
        )
        first_file = store.get_file(1)
        assert first_file is not None
        assert first_file.state is FileState.VERIFYING
        assert any("recovery_poll_unknown:1" in reason for reason in first.reasons)
        deps.sonarr.client.close()
        deps.shoko.client.close()
        deps.mapping_source.client.close()
        store.close()

        store = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[],
            shoko_files=[],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            max_recovery_rows_per_cycle=1,
        )
        second = run_cycle(
            deps,
            now_iso=T1,
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="recovery-2",
        )
        first_file = store.get_file(1)
        second_file = store.get_file(2)
        attempted = store._conn.execute(
            "SELECT shoko_file_id, attempted_iso FROM sends ORDER BY shoko_file_id"
        ).fetchall()
    finally:
        store.close()
    assert any("recovery_poll:2:verified" in reason for reason in second.reasons)
    assert first_file is not None
    assert first_file.state is FileState.VERIFYING
    assert second_file is not None
    assert second_file.state is FileState.VERIFIED
    assert [(int(row[0]), str(row[1])) for row in attempted] == [(1, T0), (2, T0)]
    assert shoko.posts_count() == 0


def test_recovery_respects_monotonic_budget_clock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P2-A: the recovery phase honours the injected monotonic clock — with a tiny
    ``max_cycle_seconds`` the GET-heavy rows are cut off mid-cycle (deferred), bounding wall."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    activate_mapping(tmp_path, mapping_xml())
    store = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
    store.set_meta("budget_last_cycle_key", "c1")
    _seed_verifying_rows(store, SEED_MONO_ROWS)
    shoko = _EmptyXRefStub([])
    sleeper = Sleeper()
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[],
            shoko_files=[],
            xml=mapping_xml(),
            monotonic=_rising_clock(0.0, MONO_CLOCK_SPAN),
            advance=sleeper.advance,
            shoko=shoko,
            max_recovery_rows_per_cycle=MONO_CAP_DISABLED,  # row cap off → clock bounds it
            max_cycle_seconds=MONO_BUDGET_SECONDS,
        )
        before = len(shoko.requests)
        report = run_cycle(
            deps,
            now_iso=_iso_offset(minutes=10),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c1",
        )
        cycle_gets = len(shoko.requests) - before
        verifying_after = len(store.files_in_state(FileState.VERIFYING))
    finally:
        store.close()
    assert any(reason.startswith("recovery_deferred:") for reason in report.reasons)
    assert cycle_gets < MONO_GETS_BOUND  # the monotonic budget cut the row loop off
    assert verifying_after > MONO_REMAIN_FLOOR  # most rows deferred to later cycles


def test_conflict_row_starvation_released_by_resolve_send(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P2-B probe (reviewer p2b): a conflict row holds the only slot at ceiling 1 → apply
    starves. The operator's ``resolve_send`` releases it exactly once; the next cycle's apply
    proceeds (the daemon never initiates the close — M7 CLI does)."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    a = SpecFile(series_id=1, file_id=10, shoko_id=500)
    b = SpecFile(series_id=2, file_id=20, shoko_id=501)
    activate_mapping(tmp_path, mapping_xml())
    store = StateStore(tmp_path / "state.sqlite3", budget_limits=ONE_FILE_LIMITS)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([a, b])
    sleeper = Sleeper()

    def seed_conflict(spec: SpecFile) -> str:
        store.record_file(
            spec.shoko_id,
            sonarr_series_id=spec.series_id,
            sonarr_file_id=spec.file_id,
            sonarr_path=_sonarr_path(spec),
            sonarr_size=FILE_SIZE,
            cohort=Cohort.BACKLOG,
            iso=T0,
        )
        for target in (FileState.MATCHED, FileState.MAPPED, FileState.PLANNED):
            store.advance(spec.shoko_id, target, "walk", iso=T0)
        token = store.prepare_send(
            spec.shoko_id,
            "a" * 64,
            format(spec.shoko_id, "064x"),
            targets_digest="d" * 64,
            structure_digest="e" * 64,
            mapping_revision="7",
            cycle_key="seed",
            iso=T0,
            expected_episode_jobs=1,
            target_ids=((ANIME_ID, ANIDB_EP, SHOKO_EP),),
        )
        assert isinstance(token, SendToken)
        store.mark_sent(token, iso=T0)
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=T0)
        inspection = store.active_send_details(spec.shoko_id)
        assert inspection is not None
        store.record_verification(
            spec.shoko_id,
            VerificationVerdict.CONTRADICTION,
            iso=T0,
            inspection=inspection,
        )
        return format(spec.shoko_id, "064x")

    req_a = seed_conflict(a)
    try:
        # Cycle 1: conflict row (A) holds the only slot; new arrival B is refused.
        deps1 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a, b],
            shoko_files=[a, b],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=sleeper.advance,
            shoko=shoko,
            max_submissions=1,
        )
        r1 = run_cycle(
            deps1, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
        assert r1.counts.submitted == 0
        assert shoko.posts_count() == 0
        assert r1.live_held_sends == COUNT_ONE
        assert r1.live_held_slots >= COUNT_ONE

        # Operator closes the conflict row (M7 CLI path — resolve_send).
        store.resolve_send(500, req_a, resolution="released_after_review", iso=T1)
        assert store.budget_view().pending_file_requests == 0

        # Cycle 2: the slot is free; B is admitted, posted and verified.
        deps2 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a, b],
            shoko_files=[a, b],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=sleeper.advance,
            shoko=shoko,
            max_submissions=1,
        )
        r2 = run_cycle(
            deps2,
            now_iso=_iso_offset(minutes=5),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c2",
        )
        record_b = store.get_file(501)
        live_counts = store.live_send_counts()
    finally:
        store.close()
    assert r2.counts.submitted == 1
    assert shoko.posts_count() == POSTS_ONE
    assert record_b is not None
    assert record_b.state is FileState.VERIFIED
    assert live_counts == (0, 0)


def test_churn_guard_pauses_at_exactly_the_limit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P3-A boundary: churning EXACTLY ``churn_limit`` series pauses (``>=``, not ``>``)."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, GEN_A)
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [file])
    shoko = ShokoStub([file])
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[file],
            shoko_files=[file],
            xml=GEN_B,
            monotonic=Clock([MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            churn_limit=COUNT_TWO,  # GEN_A→GEN_B flips exactly 2 series (1, 2)
        )
        report = run_cycle(
            deps,
            now_iso=_due_iso(),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c1",
        )
    finally:
        store.close()
    reopened = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
    try:
        assert reopened.budget_view().paused_reason == "mapping_churn"
    finally:
        reopened.close()
    assert report.counts.submitted == 0
    assert shoko.posts_count() == 0
    assert "mapping_churn" in report.reasons


def test_churn_below_limit_does_not_pause(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """P3-A boundary (other side): churning fewer than ``churn_limit`` series does NOT pause."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, GEN_A)
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([file])
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[file],
            shoko_files=[file],
            xml=GEN_B,
            monotonic=Clock([MONO_ZERO, MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            churn_limit=COUNT_THREE,  # flips 2 series < 3 → no pause
            max_submissions=1,
        )
        report = run_cycle(
            deps,
            now_iso=_due_iso(),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c1",
        )
    finally:
        store.close()
    reopened = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
    try:
        assert reopened.budget_view().paused_reason is None
    finally:
        reopened.close()
    assert "mapping_churn" not in report.reasons


# ---------------------------------------------------------- M8 review F2: new-file grace window


EXAMPLE_CONFIG = Path(__file__).resolve().parents[1] / "config.example.yaml"
GRACE_WINDOW_SECONDS = 300.0
GRACE_ELAPSED_SECONDS = 301.0


def _grace_settings(tmp_path: Path, seconds: float) -> Settings:
    """Harness settings with the grace knob pinned explicitly."""
    return make_settings(tmp_path, sync_extra={"new_file_grace_period_seconds": seconds})


def test_new_arrival_inside_grace_window_defers_with_zero_posts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F2(1): a file first seen THIS cycle (age 0 < 300) is not POSTed. The row stays
    ``planned`` with the ``grace_period`` reason and is reported as one deferred file."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    new_file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([new_file])
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[new_file],
            shoko_files=[new_file],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO, MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            settings=_grace_settings(tmp_path, GRACE_WINDOW_SECONDS),
            max_submissions=1,
        )
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
        record = store.get_file(500)
    finally:
        store.close()
    assert shoko.posts_count() == 0
    assert report.counts.submitted == 0
    assert "grace_period" in report.reasons
    assert report.deferred_planned == 1
    assert record is not None
    assert record.state is FileState.PLANNED
    assert record.first_seen_iso == T0


def test_new_arrival_submits_the_cycle_after_grace_elapses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F2(2): the same file submits in the first cycle that starts after the window closes."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    new_file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([new_file])
    reports: list[Any] = []
    try:
        for index, iso in enumerate((T0, _iso_offset_seconds(T0, GRACE_ELAPSED_SECONDS))):
            deps = build_deps(
                tmp_path,
                store=store,
                sonarr_files=[new_file],
                shoko_files=[new_file],
                xml=mapping_xml(),
                monotonic=Clock([MONO_ZERO + index, MONO_ZERO + index]),
                advance=Sleeper().advance,
                shoko=shoko,
                settings=_grace_settings(tmp_path, GRACE_WINDOW_SECONDS),
                max_submissions=1,
            )
            reports.append(
                run_cycle(
                    deps,
                    now_iso=iso,
                    mode=CycleMode.APPLY,
                    policy=RunPolicy.NEW_ARRIVAL,
                    cycle_key=f"c{index + 1}",
                )
            )
        record = store.get_file(500)
    finally:
        store.close()
    assert reports[0].counts.submitted == 0
    assert "grace_period" in reports[0].reasons
    assert shoko.posts_count() == POSTS_ONE
    assert reports[1].counts.submitted == 1
    assert record is not None
    assert record.state is FileState.VERIFIED


def test_grace_zero_applies_in_the_arrival_cycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F2(3): the knob at 0 disables the window; the shipped example config says 300."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    example_grace = parse_settings(
        EXAMPLE_CONFIG.read_text(encoding="utf-8")
    ).sync.new_file_grace_period_seconds
    assert example_grace == GRACE_WINDOW_SECONDS
    new_file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([new_file])
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[new_file],
            shoko_files=[new_file],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO, MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            settings=_grace_settings(tmp_path, 0.0),
            max_submissions=1,
        )
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
    finally:
        store.close()
    assert shoko.posts_count() == POSTS_ONE
    assert report.counts.submitted == 1
    assert "grace_period" not in report.reasons


def test_backlog_row_inside_window_is_not_grace_gated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F2(4): cohort scoping. A BACKLOG row recorded inside the window is excluded by the
    cohort policy, never by the grace gate: zero posts and NO ``grace_period`` reason."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    aged = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    # Same identity as the baseline → classify_new gives BACKLOG, and the row is first
    # recorded in this (post-baseline) cycle, i.e. its age is inside the window.
    seed_baseline(store, [aged], iso=_iso_offset_seconds(T0, -GRACE_ELAPSED_SECONDS))
    shoko = ShokoStub([aged])
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[aged],
            shoko_files=[aged],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO, MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            settings=_grace_settings(tmp_path, GRACE_WINDOW_SECONDS),
            max_submissions=1,
        )
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
        record = store.get_file(500)
    finally:
        store.close()
    assert record is not None
    assert record.cohort is Cohort.BACKLOG
    assert shoko.posts_count() == 0
    assert report.counts.submitted == 0
    assert "grace_period" not in report.reasons
    assert record.state is FileState.PLANNED


def test_grace_deferrals_consume_zero_budget_counters(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F2(5): grace deferral never reserves a slot — no send rows, no counters, no held slot."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    files = [
        SpecFile(series_id=1 + index, file_id=10 + index, shoko_id=500 + index)
        for index in range(COUNT_TWO)
    ]
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub(files)
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=files,
            shoko_files=files,
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO, MONO_ZERO]),
            advance=Sleeper().advance,
            shoko=shoko,
            settings=_grace_settings(tmp_path, GRACE_WINDOW_SECONDS),
            max_submissions=COUNT_TWO,
        )
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
        budgets = store.budget_view()
        held = store.live_send_counts()
        send_rows = _raw_read(tmp_path / "state.sqlite3", "SELECT COUNT(*) FROM sends")
    finally:
        store.close()
    assert shoko.posts_count() == 0
    assert report.deferred_planned == COUNT_TWO
    assert "grace_period" in report.reasons
    assert budgets.rolling_new_submissions == 0
    assert budgets.cycle_new_submissions == 0
    assert budgets.pending_file_requests == 0
    assert budgets.pending_episode_jobs == 0
    assert held == (0, 0)
    assert send_rows[0][0] == 0


# ------------------------------------------------------------------ F4/F6: daemon reliability


def _iso_seq(start: str, step_seconds: float, count: int) -> list[str]:
    base = datetime.fromisoformat(start)
    return [(base + timedelta(seconds=step_seconds * index)).isoformat() for index in range(count)]


def _many_cycle_deps(
    tmp_path: Path, store: StateStore, advance: Callable[[float], None]
) -> DaemonDeps:
    activate_mapping(tmp_path, mapping_xml())
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    return build_deps(
        tmp_path,
        store=store,
        sonarr_files=[],
        shoko_files=[],
        xml=mapping_xml(),
        monotonic=Clock([MONO_ZERO]),
        advance=advance,
    )


def test_daemon_bounds_history_and_totals_across_many_cycles(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F4: an unbounded daemon keeps a small history, fires the callback every cycle, and
    reports accurate totals (executed/failed) independent of the history bound."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    store = open_tmp_store(tmp_path)
    sleeper = Sleeper()
    deps = _many_cycle_deps(tmp_path, store, sleeper.advance)
    total = 1000
    times = iter(_iso_seq(T0, 10.0, total))
    seen = {"n": 0}
    completed: list[str] = []

    def on_complete(report: Any) -> None:
        completed.append(report.cycle_key)

    def now_iso() -> str:
        seen["n"] += 1
        return next(times)

    def should_stop() -> bool:
        return seen["n"] >= total

    try:
        report = run_daemon(
            deps,
            cycle_key=lambda index: f"c{index}",
            now_iso=now_iso,
            interval_seconds=10.0,
            should_stop=should_stop,
            mode=CycleMode.DRY_RUN,
            on_cycle_complete=on_complete,
        )
    finally:
        store.close()
    assert report.exit_reason == "shutdown_requested"
    assert report.shutdown_requested is True
    assert report.total_cycles == total
    assert report.total_failed == 0
    assert len(completed) == total
    assert len(report.cycles) <= DAEMON_HISTORY_MAXLEN
    assert report.last_cycle is not None
    assert report.last_cycle.cycle_key == f"c{total - 1}"
    assert report.cycles[-1].cycle_key == f"c{total - 1}"


def test_cycle_metadata_persists_mid_run_via_callback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F4: the CLI persistence callback runs after EVERY completed cycle, so the persisted
    cycle meta advances during the run — not only at exit."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    store = open_tmp_store(tmp_path)
    deps = _many_cycle_deps(tmp_path, store, Sleeper().advance)
    cycles = 5
    times = iter(_iso_seq(T0, 10.0, cycles))
    iso_by_key: dict[str, str] = {}

    def on_complete(report: Any) -> None:
        iso = iso_by_key[report.cycle_key]
        persist_cycle_meta(
            store,
            report,
            started_iso=iso,
            ended_iso=iso,
            interval_seconds=10.0,
            full_reconcile_hours=1000.0,
        )

    def now_iso() -> str:
        iso = next(times)
        iso_by_key[f"c{len(iso_by_key)}"] = iso
        return iso

    try:
        report = run_daemon(
            deps,
            cycle_key=lambda index: f"c{index}",
            now_iso=now_iso,
            interval_seconds=10.0,
            stop_after_cycles=cycles,
            mode=CycleMode.DRY_RUN,
            on_cycle_complete=on_complete,
        )
        mid_run_key = store.get_meta(META_CYCLE_KEY)
        mid_run_submitted = store.get_meta(META_CYCLE_SUBMITTED)
    finally:
        store.close()
    assert report.exit_reason == "stop_after_cycles"
    assert report.total_cycles == cycles
    # The callback persisted the LAST completed cycle's key mid-run.
    assert mid_run_key == f"c{cycles - 1}"
    assert mid_run_submitted == "0"


def test_full_light_cadence_forces_full_reconcile_with_callback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F4: driving the real ``full_cycle_due`` gate with a fake clock, the cadence starts
    full, runs light cycles, then forces a full reconcile; markers advance mid-run."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    store = open_tmp_store(tmp_path)
    deps = _many_cycle_deps(tmp_path, store, Sleeper().advance)
    cycles = 6
    times = iter(_iso_seq(T0, 1200.0, cycles))  # 20-minute steps, 1h full interval
    iso_by_key: dict[str, str] = {}
    full_flags: list[bool] = []

    def on_complete(report: Any) -> None:
        full_flags.append(report.full)
        iso = iso_by_key[report.cycle_key]
        persist_cycle_meta(
            store,
            report,
            started_iso=iso,
            ended_iso=iso,
            interval_seconds=10.0,
            full_reconcile_hours=1.0,
        )

    def now_iso() -> str:
        iso = next(times)
        iso_by_key[f"c{len(iso_by_key)}"] = iso
        return iso

    try:
        report = run_daemon(
            deps,
            cycle_key=lambda index: f"c{index}",
            now_iso=now_iso,
            interval_seconds=10.0,
            stop_after_cycles=cycles,
            mode=CycleMode.DRY_RUN,
            full_cycle=lambda iso: full_cycle_due(store, iso),
            on_cycle_complete=on_complete,
        )
        last_full = store.get_meta(META_LAST_FULL_RECONCILE_ISO)
        next_full = store.get_meta(META_NEXT_FULL_RECONCILE_ISO)
    finally:
        store.close()
    assert report.total_cycles == cycles
    assert full_flags[0] is True  # first cycle is full (no marker)
    assert False in full_flags  # light cycles occur between full reconciles
    # The interval gate forces a full pass again after the light streak (>=2 full cycles).
    assert sum(1 for flag in full_flags if flag) >= COUNT_TWO
    assert last_full is not None
    assert next_full is not None


def test_failed_cycle_totals_and_history_flags(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F4: injected transport failures are counted accurately in the totals and history flags."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    store = open_tmp_store(tmp_path)
    file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    seed_baseline(store, [file])
    sleeper = Sleeper()
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=mapping_xml(),
        monotonic=Clock([MONO_ZERO]),
        advance=sleeper.advance,
        jitter=lambda: 1.0,
        max_submissions=1,
    )
    deps = _swap_source(deps, lambda request: httpx.Response(503, request=request))
    times = iter(
        [T0, _due_iso(hours=1.0), _due_iso(hours=3.0), _due_iso(hours=7.0), _due_iso(hours=15.0)]
    )
    try:
        report = run_daemon(
            deps,
            cycle_key=lambda index: f"c{index}",
            now_iso=lambda: next(times),
            interval_seconds=10.0,
            stop_after_cycles=COUNT_FOUR,
        )
    finally:
        store.close()
    assert report.total_cycles == COUNT_FOUR
    assert report.total_failed == COUNT_FOUR
    assert all(cycle.failed for cycle in report.cycles)


def test_interruptible_sleep_wakes_and_starts_no_new_cycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F6: a stop arriving during the inter-cycle sleep wakes the daemon, which exits the
    moment it wakes — no additional cycle starts."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    store = open_tmp_store(tmp_path)
    state = StopState()
    sleeper = Sleeper()
    deps = _many_cycle_deps(tmp_path, store, sleeper.advance)
    completed = {"n": 0}

    def advance(seconds: float) -> None:
        sleeper.advance(seconds)
        # Emulate SIGTERM landing inside the sleep: wake and mark shutdown.
        state.stopped = True
        state.shutdown_event.set()

    deps = DaemonDeps(**{**deps.__dict__, "advance": advance})
    times = iter(_iso_seq(T0, 10.0, 3))

    def on_complete(_report: Any) -> None:
        completed["n"] += 1

    try:
        report = run_daemon(
            deps,
            cycle_key=lambda index: f"c{index}",
            now_iso=lambda: next(times),
            interval_seconds=300.0,
            should_stop=state.should_stop,
            mode=CycleMode.DRY_RUN,
            on_cycle_complete=on_complete,
        )
    finally:
        store.close()
    assert report.total_cycles == 1  # woke after the first cycle's sleep; no second cycle
    assert report.shutdown_requested is True
    assert completed["n"] == 1
    assert len(sleeper.sleeps) == 1
    assert sleeper.sleeps[0] > 0.0
    assert state.shutdown_event.is_set()


def test_shutdown_mid_protected_unit_completes_unit_and_resubmits_zero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F6: shutdown requested inside a protected unit lets the current cycle finish (its
    single POST completes), starts no further POSTs, and leaves durable state recoverable."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    state = StopState()
    shoko = ShokoStub([file])
    sleeper = Sleeper()

    def on_post() -> None:
        state.stopped = True
        state.shutdown_event.set()

    shoko.on_post = on_post
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=mapping_xml(),
        monotonic=Clock([MONO_ZERO]),
        advance=sleeper.advance,
        shoko=shoko,
        max_submissions=1,
    )
    times = iter(_iso_seq(T0, 10.0, 5))
    try:
        report = run_daemon(
            deps,
            cycle_key=lambda index: f"c{index}",
            now_iso=lambda: next(times),
            interval_seconds=300.0,
            should_stop=state.should_stop,
            mode=CycleMode.APPLY,
        )
        record = store.get_file(500)
        posts_after_cycle = shoko.posts_count()
    finally:
        store.close()
    assert report.total_cycles == 1
    assert report.shutdown_requested is True
    assert posts_after_cycle == POSTS_ONE  # the unit's single POST completed
    assert record is not None
    assert record.state is FileState.VERIFIED
    # Durable recovery on restart re-walks the VERIFIED row and issues zero POSTs.
    restart = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
    try:
        plan = restart.recover_after_restart()
        assert not plan.hold_for_verification
        assert not plan.continue_polling
        assert shoko.posts_count() == POSTS_ONE  # recovery re-walk issues zero new POSTs
    finally:
        restart.close()


def test_cycle_completion_callback_failure_is_never_fatal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """F4 review: a CLI persistence callback crash is logged and the daemon keeps
    cycling — the mid-run observability seam never becomes a new crash surface."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    store = open_tmp_store(tmp_path)
    sleeper = Sleeper()
    deps = _many_cycle_deps(tmp_path, store, sleeper.advance)
    cycles = 3
    times = iter(_iso_seq(T0, 10.0, cycles))

    def on_complete(_report: Any) -> None:
        raise RuntimeError("meta write exploded")

    with caplog.at_level(logging.WARNING, logger="shoko_linker.daemon"):
        report = run_daemon(
            deps,
            cycle_key=lambda index: f"c{index}",
            now_iso=lambda: next(times),
            interval_seconds=10.0,
            stop_after_cycles=cycles,
            mode=CycleMode.DRY_RUN,
            on_cycle_complete=on_complete,
        )
    store.close()
    assert report.total_cycles == cycles
    assert report.exit_reason == "stop_after_cycles"
    warned = sum(
        1 for record in caplog.records if "cycle completion callback failed" in record.message
    )
    assert warned == cycles


def test_inter_cycle_wait_seam_is_separate_from_in_cycle_advance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F6 review: the inter-cycle sleep uses the dedicated ``inter_cycle_wait`` seam;
    ``deps.advance`` keeps its pacing role for in-cycle verification polls only."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    store = open_tmp_store(tmp_path)
    sleeper = Sleeper()
    deps = _many_cycle_deps(tmp_path, store, sleeper.advance)
    waits: list[float] = []
    cycles_three = 3
    times = iter(_iso_seq(T0, 10.0, cycles_three))
    try:
        report = run_daemon(
            deps,
            cycle_key=lambda index: f"c{index}",
            now_iso=lambda: next(times),
            interval_seconds=10.0,
            stop_after_cycles=cycles_three,
            mode=CycleMode.DRY_RUN,
            inter_cycle_wait=lambda seconds: waits.append(seconds),
        )
    finally:
        store.close()
    assert report.total_cycles == cycles_three
    assert len(waits) == cycles_three - 1  # between-cycle sleeps only, via the dedicated seam
    assert all(seconds > 0.0 for seconds in waits)
    assert sleeper.sleeps == []  # in-cycle advance untouched by inter-cycle sleep


# ------------------------------------------------------------------ F7: daemon campaign drain


def _drain_settings(tmp_path: Path, batch: int) -> Settings:
    """Settings with a small campaign.max_batch_size so the daemon drains in many batches."""
    document = yaml.safe_load(
        yaml.safe_dump(
            {
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
                "mappings": {
                    "source": "anime-lists",
                    "source_url": SOURCE_URL,
                    "refresh_hours": HOURS,
                },
                "sync": {
                    "apply_changes": True,
                    "max_concurrent_links": 4,
                    "new_file_grace_period_seconds": 0,
                    "max_new_per_cycle": 50,
                    "max_pending_episode_jobs": 100,
                },
                "database": {"path": "/daemon/state.sqlite3"},
                "campaign": {
                    "max_campaign_files": 7000,
                    "max_batch_size": batch,
                    "daemon_auto_apply": True,
                },
            }
        )
    )
    return parse_settings(yaml.safe_dump(document))


def _with_auto_drain(settings: Settings, *, auto: bool) -> Settings:
    campaign = replace(settings.campaign, daemon_auto_apply=auto)
    return replace(settings, campaign=campaign)


def test_daemon_auto_drains_campaign_batches_with_bounded_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F7 + F4: run_daemon drains an approved campaign across MANY bounded batches; the cycle
    history stays bounded, the campaign meta advances mid-run, and zero manual invocations follow
    the canary."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    files = [
        SpecFile(series_id=index + 1, file_id=10 + index, shoko_id=500 + index)
        for index in range(6)
    ]
    activate_mapping(tmp_path, mapping_xml())
    settings = _drain_settings(tmp_path, batch=2)
    store = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
    store.set_meta("budget_last_cycle_key", "cycle-1")
    seed_baseline(store, files)  # the discovered files are backlog (they predate the baseline)
    sleeper = Sleeper()
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=files,
        shoko_files=files,
        xml=mapping_xml(),
        monotonic=Clock([MONO_ZERO, MONO_ZERO, MONO_ZERO]),
        advance=sleeper.advance,
        settings=settings,
        max_submissions=1,
    )
    # Prepare + approve the finite campaign (dry-run machinery, zero writes).
    plan, digest = prepare_campaign(deps, settings, store, T0, limit=6, deadline_hours=168.0)
    write_plan_file(tmp_path / "plan.json", plan)
    approve_campaign(store, tmp_path / "plan.json", digest, T0)
    approved_campaign = store.get_campaign(plan.campaign_id)
    assert approved_campaign is not None
    assert len(approved_campaign.bindings) == 6
    # The operator's manual canary batch (one file) opens the daemon drain.
    canary = apply_campaign(deps, settings, store, tmp_path / "plan.json", T0, batch_size=1)
    assert canary.submitted == 1
    # F10: the drain starts only on the operator's explicit, digest-bound confirmation.
    authorize_campaign_drain(store, plan.campaign_id, T0, note="canary inspected")
    campaign_meta: list[str] = []

    def on_complete(report: Any) -> None:
        persist_cycle_meta(
            store,
            report,
            started_iso=T1,
            ended_iso=T1,
            interval_seconds=10.0,
            full_reconcile_hours=1000.0,
        )
        if report.campaign is not None:
            campaign_meta.append(report.campaign.campaign_id)

    times = iter(_iso_seq(T1, 10.0, 8))
    try:
        report = run_daemon(
            deps,
            cycle_key=lambda index: f"c{index}",
            now_iso=lambda: next(times),
            interval_seconds=10.0,
            stop_after_cycles=8,
            mode=CycleMode.APPLY,
            on_cycle_complete=on_complete,
        )
        campaign = store.get_campaign(plan.campaign_id)
        campaign_id_meta = store.get_meta(META_CYCLE_CAMPAIGN_ID)
        submitted_meta = store.get_meta(META_CYCLE_CAMPAIGN_SUBMITTED)
    finally:
        store.close()
    # The daemon drained the whole campaign with no further manual invocations after the canary.
    assert campaign is not None
    assert campaign.total_sent == 6
    # F4: bounded history for an unbounded run flag; here a bounded 8-cycle run keeps all 8.
    assert report.total_cycles == 8
    assert len(report.cycles) <= max(DAEMON_HISTORY_MAXLEN, 8)
    # The per-cycle campaign-batch summary persisted mid-run via the callback (the drained
    # campaign id advanced the cycle meta), and the F4 history bound held.
    assert plan.campaign_id in campaign_meta
    assert len(campaign_meta) >= 3  # multiple bounded batches drained across cycles
    assert campaign_id_meta is not None
    assert submitted_meta is not None


def test_daemon_campaign_drain_skipped_when_disabled(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F7: campaign.daemon_auto_apply=false keeps the daemon new-arrival-only (manual CLI)."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    files = [
        SpecFile(series_id=index + 1, file_id=10 + index, shoko_id=500 + index)
        for index in range(4)
    ]
    activate_mapping(tmp_path, mapping_xml())
    settings = _drain_settings(tmp_path, batch=2)
    settings = _with_auto_drain(settings, auto=False)
    store = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
    store.set_meta("budget_last_cycle_key", "cycle-1")
    seed_baseline(store, files)
    sleeper = Sleeper()
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=files,
        shoko_files=files,
        xml=mapping_xml(),
        monotonic=Clock([MONO_ZERO, MONO_ZERO, MONO_ZERO]),
        advance=sleeper.advance,
        settings=settings,
        max_submissions=1,
    )
    plan, digest = prepare_campaign(deps, settings, store, T0, limit=4, deadline_hours=168.0)
    write_plan_file(tmp_path / "plan.json", plan)
    approve_campaign(store, tmp_path / "plan.json", digest, T0)
    apply_campaign(deps, settings, store, tmp_path / "plan.json", T0, batch_size=1)
    times = iter(_iso_seq(T1, 10.0, 4))
    try:
        report = run_daemon(
            deps,
            cycle_key=lambda index: f"c{index}",
            now_iso=lambda: next(times),
            interval_seconds=10.0,
            stop_after_cycles=4,
            mode=CycleMode.APPLY,
        )
        campaign = store.get_campaign(plan.campaign_id)
    finally:
        store.close()
    assert campaign is not None
    # Drain disabled: the daemon never advances the campaign beyond the canary.
    assert campaign.total_sent == 1
    assert all(cycle.campaign is None for cycle in report.cycles)


def test_campaign_pending_target_deferrals_defer_and_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F18: a campaign whose remaining files never materialize (pending targets) makes zero
    submissions every drain cycle — a RECOVERABLE wait: the campaign defers with a bounded
    cooldown (a real future retry, Finding 1), stays APPROVED (no permanent pause, breaker not
    burned), YIELDS the queue while deferred (the later cycles skip it entirely at a frozen
    clock), and resumes automatically once the cooldown passes and the targets materialize
    (no operator resume needed)."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    cooldown_seconds = 1500.0  # STALL_YIELD_COOLDOWN_CYCLES x sync.interval_seconds (Finding 1)
    head = SpecFile(series_id=1, file_id=10, shoko_id=500, episodes=((1, 1, ANIDB_EP, SHOKO_EP),))
    pending = [
        SpecFile(
            series_id=2 + index,
            file_id=20 + index,
            shoko_id=501 + index,
            episodes=((1, 2 + index, ANIDB_EP + 1 + index, SHOKO_EP + 1 + index),),
        )
        for index in range(4)
    ]
    files = [head, *pending]
    xml = (
        "<anime-list>"
        f"<anime anidbid='{ANIME_ID}' tvdbid='{TVDB_SERIES}' defaulttvdbseason='1'>"
        "<mapping-list><mapping anidbseason='1' tvdbseason='1'>;1-1;2-2;3-3;4-4;5-5;</mapping>"
        "</mapping-list></anime></anime-list>"
    )
    activate_mapping(tmp_path, xml)
    settings = _drain_settings(tmp_path, batch=5)
    store = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
    store.set_meta("budget_last_cycle_key", "cycle-1")
    seed_baseline(store, files)
    shoko = ShokoStub(files)
    # The remaining AniDB episodes never materialize → prepare builds genuine pending-target plans.
    shoko.unmaterialized = {ANIDB_EP + 1 + index for index in range(4)}
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=files,
        shoko_files=files,
        xml=xml,
        monotonic=Clock([MONO_ZERO, MONO_ZERO, MONO_ZERO]),
        advance=Sleeper().advance,
        settings=settings,
        max_submissions=10,
        shoko=shoko,
    )
    plan, digest = prepare_campaign(deps, settings, store, T0, limit=5, deadline_hours=168.0)
    write_plan_file(tmp_path / "plan.json", plan)
    approve_campaign(store, tmp_path / "plan.json", digest, T0)
    campaign_id = plan.campaign_id
    try:
        canary = apply_campaign(deps, settings, store, tmp_path / "plan.json", T0, batch_size=1)
        assert canary.submitted == 1  # the materialized head file opens the drain
        assert campaign_send_progress(store, campaign_id) == 1
        authorize_campaign_drain(store, campaign_id, T0, note="canary inspected")  # F10
        posts_before = len(shoko.posts)
        stall = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.BACKLOG, cycle_key="p1"
        )
        assert stall.campaign is not None
        assert stall.campaign.submitted == 0  # zero submissions every pending-target walk
        assert stall.campaign.progress == 0  # pending-target deferrals make no progress
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status == "approved"  # recoverable deferral — never a permanent pause
        assert not any("campaign_zero_progress_pause" in reason for reason in stall.reasons)
        expected_deferral = (
            datetime.fromisoformat(T0) + timedelta(seconds=cooldown_seconds)
        ).isoformat()
        assert store.get_meta(f"campaign_deferred_until:{campaign_id}") == expected_deferral
        assert any(
            "pending_metadata_cooldown" in reason for reason in stall.reasons
        )  # Finding 2: the label names the real wait
        for key in ("p2", "p3", "p4", "p5"):
            cycle = run_cycle(
                deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.BACKLOG, cycle_key=key
            )
            assert cycle.campaign is None  # the deferred head yields every cycle it is deferred
            assert not any("campaign_zero_progress_pause" in reason for reason in cycle.reasons)
        assert len(shoko.posts) == posts_before  # zero POSTs across the zero-progress cycles
        assert store.get_meta(f"campaign_zero_progress:{campaign_id}") in (None, "0")
        # Targets materialize and the cooldown passes: the campaign resumes automatically
        # (no operator resume) and later batches progress with the counter intact.
        shoko.unmaterialized = set()
        resume_iso = (
            datetime.fromisoformat(T0) + timedelta(seconds=cooldown_seconds + 1.0)
        ).isoformat()
        report = run_cycle(
            deps,
            now_iso=resume_iso,
            mode=CycleMode.APPLY,
            policy=RunPolicy.BACKLOG,
            cycle_key="resume",
        )
        assert report.campaign is not None
        assert report.campaign.submitted > 0  # submissions resumed, no pause false-positive
        assert len(shoko.posts) > posts_before
    finally:
        store.close()


#: Finding 1: the stall-yield cooldown is ``STALL_YIELD_COOLDOWN_CYCLES`` daemon cycle intervals
#: (the default ``sync.interval_seconds`` in :func:`_drain_settings` is 300.0 → 1500.0). The
#: production daemon sleeps ``interval x (1 + jitter)`` AFTER each cycle (upper bound
#: interval x 1.05 = 315.0), so the head-of-line span is walked at this realistic cadence.
HOL_INTERVAL_SECONDS = 300.0
HOL_CADENCE_SECONDS = HOL_INTERVAL_SECONDS * 1.05
HOL_COOLDOWN_SECONDS = 5 * HOL_INTERVAL_SECONDS


def _hol_two_campaigns(
    tmp_path: Path,
    *,
    new_arrival: SpecFile | None = None,
) -> tuple[DaemonDeps, StateStore, ShokoStub, str, str]:
    """Two drain-authorized campaigns: the OLDEST (A) stalls every cycle (Shoko metadata never
    materializes for its remaining files); the NEWER (B) is healthy with a queued
    candidate. ``new_arrival`` (optional) is a materialized file present in both
    inventories but seeded in NO baseline/campaign, so a daemon cycle classifies it
    ``new_arrival`` and applies it independently of the campaign drain. Returns
    (deps, store, shoko, campaign_a, campaign_b)."""
    head = SpecFile(series_id=1, file_id=10, shoko_id=500, episodes=((1, 1, ANIDB_EP, SHOKO_EP),))
    pending = [
        SpecFile(
            series_id=2 + index,
            file_id=20 + index,
            shoko_id=501 + index,
            episodes=((1, 2 + index, ANIDB_EP + 1 + index, SHOKO_EP + 1 + index),),
        )
        for index in range(2)
    ]
    b_files = [
        SpecFile(
            series_id=6, file_id=60, shoko_id=400, episodes=((1, 6, ANIDB_EP + 5, SHOKO_EP + 5),)
        ),
        SpecFile(
            series_id=7, file_id=61, shoko_id=401, episodes=((1, 7, ANIDB_EP + 6, SHOKO_EP + 6),)
        ),
    ]
    inventory = [head, *pending, *b_files]
    if new_arrival is not None:
        inventory = [*inventory, new_arrival]
    xml = (
        "<anime-list>"
        f"<anime anidbid='{ANIME_ID}' tvdbid='{TVDB_SERIES}' defaulttvdbseason='1'>"
        "<mapping-list><mapping anidbseason='1' tvdbseason='1'>;1-1;2-2;6-6;7-7;</mapping>"
        "</mapping-list></anime></anime-list>"
    )
    activate_mapping(tmp_path, xml)
    settings = _drain_settings(tmp_path, batch=5)
    store = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
    store.set_meta("budget_last_cycle_key", "cycle-1")
    seed_baseline(store, [head, *pending])
    shoko = ShokoStub(inventory)
    # The head campaign's remaining AniDB episodes never materialize → genuine pending targets.
    shoko.unmaterialized = {ANIDB_EP + 1 + index for index in range(2)}
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=inventory,
        shoko_files=inventory,
        xml=xml,
        monotonic=Clock([MONO_ZERO, MONO_ZERO, MONO_ZERO]),
        advance=Sleeper().advance,
        settings=settings,
        max_submissions=10,
        shoko=shoko,
    )
    plan_a, digest_a = prepare_campaign(deps, settings, store, T0, limit=3, deadline_hours=168.0)
    write_plan_file(tmp_path / "planA.json", plan_a)
    approve_campaign(store, tmp_path / "planA.json", digest_a, T0)
    campaign_a = plan_a.campaign_id
    canary_a = apply_campaign(deps, settings, store, tmp_path / "planA.json", T0, batch_size=1)
    assert canary_a.submitted == 1  # the materialized head file opens the drain
    authorize_campaign_drain(store, campaign_a, T0, note="canary inspected")  # F10
    seed_baseline(store, [head, *pending, *b_files])  # B's files arrive as new backlog
    store._conn.execute("UPDATE files SET cohort = 'backlog' WHERE shoko_file_id IN (400, 401)")
    plan_b, digest_b = prepare_campaign(deps, settings, store, T0, limit=2, deadline_hours=168.0)
    write_plan_file(tmp_path / "planB.json", plan_b)
    later_iso = (datetime.fromisoformat(T0) + timedelta(seconds=60.0)).isoformat()
    approve_campaign(
        store, tmp_path / "planB.json", digest_b, later_iso
    )  # B is NEWER: A is the head
    campaign_b = plan_b.campaign_id
    assert {item.shoko_file_id for item in plan_b.files} == {400, 401}
    canary_b = apply_campaign(deps, settings, store, tmp_path / "planB.json", T0, batch_size=1)
    assert canary_b.submitted == 1
    authorize_campaign_drain(store, campaign_b, T0, note="canary inspected")
    return deps, store, shoko, campaign_a, campaign_b


def _hol_cycle(deps: DaemonDeps, key: str, now_iso: str) -> Any:
    return run_cycle(
        deps, now_iso=now_iso, mode=CycleMode.APPLY, policy=RunPolicy.BACKLOG, cycle_key=key
    )


def test_f18_stalled_head_cooldown_yields_the_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Head-of-line fix (Finding 1) at production cadence: the stalled head (pending Shoko
    metadata → zero-progress every drain cycle) defers with a bounded FIVE-interval cooldown — a
    FUTURE retry time, not the old retry==now no-op — so across a full cooldown span walked at the
    realistic sleep-after-cycle cadence (interval x 1.05) the head yields the queue, the later
    drain-authorized campaign (B) drains, the head is never paused, its authorization stays
    intact, its files are never POSTed, and the head auto-resumes exactly at the cooldown mark."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    deps, store, shoko, campaign_a, campaign_b = _hol_two_campaigns(tmp_path)
    base = datetime.fromisoformat(T0)
    try:
        posts_at_setup = len(shoko.posts)  # {500, 400} — one canary each
        stall = _hol_cycle(deps, "hol-1", T0)
        assert stall.campaign is not None
        assert stall.campaign.submitted == 0  # pending targets: zero progress
        assert stall.campaign.progress == 0
        deferred_a = store.get_meta(f"campaign_deferred_until:{campaign_a}")
        expected = (base + timedelta(seconds=HOL_COOLDOWN_SECONDS)).isoformat()
        assert deferred_a == expected  # a real, bounded five-interval cooldown (Finding 1)
        assert any(
            reason.startswith("campaign_deferred:") and "pending_metadata_cooldown" in reason
            for reason in stall.reasons
        )  # Finding 2: the label names the real wait
        # Walk the full five-interval span at the realistic sleep-after-cycle cadence.
        b_drains = 0
        idle_skips = 0
        recoverable_surfaced = 0
        head_resumes = 0
        for step in range(1, 6):
            iso = (base + timedelta(seconds=step * HOL_CADENCE_SECONDS)).isoformat()
            cycle = _hol_cycle(deps, f"hol-span-{step}", iso)
            campaign = store.get_campaign(campaign_a)
            assert campaign is not None
            assert campaign.status == "approved"  # the head never pauses across the span
            assert campaign.drain_authorized_iso is not None  # authorization intact
            assert store.get_meta(f"campaign_zero_progress:{campaign_a}") in (None, "0")
            if cycle.campaign is None:
                idle_skips += 1  # the deferred head yields every idle cycle inside the cooldown
                if any(
                    reason.startswith(f"campaign_deferred:{campaign_a}")
                    and "recoverable_cooldown" in reason
                    for reason in cycle.reasons
                ):
                    recoverable_surfaced += 1  # the deferral and its retry time are surfaced
            elif cycle.campaign.campaign_id == campaign_b:
                b_drains += 1  # the later drain-authorized campaign takes the yielded cycle
                assert cycle.campaign.submitted >= 1  # B drains while the head defers
            else:
                assert cycle.campaign.campaign_id == campaign_a  # auto-resume at the cooldown mark
                assert cycle.campaign.submitted == 0  # the metadata never landed: stalls again
                head_resumes += 1
                deferred_a = store.get_meta(f"campaign_deferred_until:{campaign_a}")
                assert (
                    deferred_a
                    == (
                        datetime.fromisoformat(iso) + timedelta(seconds=HOL_COOLDOWN_SECONDS)
                    ).isoformat()
                )  # the cooldown re-arms from the resume time
        assert b_drains == 1  # campaign B drains while the head yields the queue
        assert idle_skips == 3  # the head yields the queue for the idle cooldown cycles
        assert recoverable_surfaced >= 2  # the recoverable_cooldown skip reason surfaces
        assert head_resumes == 1  # the head is deterministically re-selected at 5x interval
        assert len(shoko.posts) == posts_at_setup + 1  # the head's pending files never POST
        post_paths = [request.url.path for request in shoko.requests if request.method == "POST"]
        assert sorted(int(path.split("/")[4]) for path in post_paths) == [400, 401, 500]
    finally:
        store.close()


def test_f18_stalled_head_deferral_survives_restart_and_rearms_cooldown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Head-of-line fix (Finding 1, continued): the cooldown deferral survives a store reopen
    mid-cooldown (the head is skipped, zero POSTs, the deferral surfaces in the cycle reasons);
    once the cooldown passes the head is SELECTED again automatically (no operator action),
    stalls, and the cooldown re-arms from the new time — it never permanently pauses and
    every file is POSTed exactly once across the whole sequence."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    deps, store, shoko, campaign_a, campaign_b = _hol_two_campaigns(tmp_path)
    try:
        stall = _hol_cycle(deps, "holr-1", T0)
        assert stall.campaign is not None  # the head takes its first stall cycle
        assert len(shoko.posts) == 2
    finally:
        store.close()
    reopened = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
    reopened_deps = replace(deps, store=reopened)
    try:
        posts_at_restart = len(shoko.posts)
        mid = _hol_cycle(reopened_deps, "holr-2", T0)
        assert mid.campaign is not None  # restart mid-cooldown: the head yields, B drains
        assert mid.campaign.campaign_id == campaign_b
        assert mid.campaign.submitted == 1  # 401 takes the cycle the stalled head would burn
        assert len(shoko.posts) == posts_at_restart + 1
        after = _hol_cycle(reopened_deps, "holr-3", T0)
        assert after.campaign is None  # B complete (max-submissions auto-pause)
        surfaced = _hol_cycle(reopened_deps, "holr-4", T0)
        assert surfaced.campaign is None  # nothing drainable: the deferred head yields
        assert any(
            reason.startswith(f"campaign_deferred:{campaign_a}") for reason in surfaced.reasons
        )  # the durable deferral survived the restart and is surfaced
        resume_iso = (
            datetime.fromisoformat(T0) + timedelta(seconds=HOL_COOLDOWN_SECONDS + 1.0)
        ).isoformat()
        resumed = _hol_cycle(reopened_deps, "holr-5", resume_iso)
        assert resumed.campaign is not None  # auto-resume: the head is selected again
        assert resumed.campaign.campaign_id == campaign_a
        assert resumed.campaign.submitted == 0  # the metadata never landed: it stalls again
        deferred_again = reopened.get_meta(f"campaign_deferred_until:{campaign_a}")
        assert (
            deferred_again
            == (
                datetime.fromisoformat(resume_iso) + timedelta(seconds=HOL_COOLDOWN_SECONDS)
            ).isoformat()
        )  # the cooldown re-armed from the resume time
        campaign = reopened.get_campaign(campaign_a)
        assert campaign is not None
        assert campaign.status == "approved"  # never permanently paused
        assert campaign.drain_authorized_iso is not None
        assert len(shoko.posts) == posts_at_restart + 1  # no POSTs after B's drain
        post_paths = [request.url.path for request in shoko.requests if request.method == "POST"]
        assert sorted(int(path.split("/")[4]) for path in post_paths) == [400, 401, 500]
    finally:
        reopened.close()


def test_f18_deferred_head_leaves_daemon_duties_unaffected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F18 (Finding 1, verification): a recoverable cooldown-deferred head campaign is SKIPPED by
    drain selection, yet the daemon's OTHER duties run unaffected — a new arrival applies in the
    same cycle the head defers, and the head stays skipped (with the ``recoverable_cooldown`` skip
    reason surfaced) once the later campaign drains out. The head never pauses, keeps its
    authorization, and its pending files are never POSTed."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    na = SpecFile(
        series_id=8,
        file_id=70,
        shoko_id=410,
        episodes=((1, 6, ANIDB_EP + 5, SHOKO_EP + 5),),
    )
    deps, store, shoko, campaign_a, campaign_b = _hol_two_campaigns(tmp_path, new_arrival=na)
    base = datetime.fromisoformat(T0)

    def new_arrival_cycle(key: str, iso: str) -> Any:
        return run_cycle(
            deps, now_iso=iso, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key=key
        )

    try:
        posts_at_setup = len(shoko.posts)  # {500, 400} — one canary each
        # The head (A) stalls and defers in the SAME cycle a new arrival applies.
        stall = new_arrival_cycle("holna-1", T0)
        assert stall.campaign is not None
        assert stall.campaign.campaign_id == campaign_a
        assert stall.campaign.submitted == 0  # pending targets: the head makes zero drain progress
        deferred_a = store.get_meta(f"campaign_deferred_until:{campaign_a}")
        assert deferred_a == (base + timedelta(seconds=HOL_COOLDOWN_SECONDS)).isoformat()
        assert stall.counts.submitted >= 1  # the new-arrival apply is NOT gated by the deferral
        campaign = store.get_campaign(campaign_a)
        assert campaign is not None
        assert campaign.status == "approved"  # the head never pauses
        assert campaign.drain_authorized_iso is not None  # authorization intact
        # Walk the cooldown: B takes its cycles while the head defers, then drains out and the
        # deferred head yields the idle cycles (campaign skipped, recoverable_cooldown surfaced).
        for step in (2, 3, 4):
            idle = new_arrival_cycle(
                f"holna-{step}", (base + timedelta(seconds=step * HOL_CADENCE_SECONDS)).isoformat()
            )
            campaign = store.get_campaign(campaign_a)
            assert campaign is not None
            assert campaign.status == "approved"  # the head never pauses
            assert campaign.drain_authorized_iso is not None  # authorization intact
        assert idle.campaign is None  # the deferred head is SKIPPED by drain selection
        assert any(
            reason.startswith(f"campaign_deferred:{campaign_a}")
            and "recoverable_cooldown" in reason
            for reason in idle.reasons
        ), idle.reasons  # the recoverable skip reason surfaces
        # The new arrival posted exactly once; the head's pending files never posted.
        post_paths = [request.url.path for request in shoko.requests if request.method == "POST"]
        posted_ids = sorted(int(path.split("/")[4]) for path in post_paths)
        assert posted_ids.count(na.shoko_id) == 1  # the new arrival POSTed once, no replay
        assert 501 not in posted_ids  # the head's pending files never POST
        assert 502 not in posted_ids
        assert campaign_send_progress(store, campaign_a) == 1  # the head's only POST is its canary
        # Across the span: the canaries (500, 400) + B's drain (401) + the new arrival (410).
        assert sorted(set(posted_ids)) == [400, 401, 410, 500]
        assert len(shoko.posts) == posts_at_setup + 2  # only B's drain and the new arrival added
    finally:
        store.close()


def test_attestation_gate_campaign_scope_requires_campaign_id() -> None:
    """P3-7: a campaign scope with campaign_id=None must raise, never silently share the daemon
    gate key (which would make a campaign lease mutate the daemon write pause)."""
    deps = SimpleNamespace(attestation_verdict=lambda: AttestationVerdict("expired", "x"))
    with pytest.raises(ValueError, match="campaign_id"):
        attestation_gate(
            deps,  # type: ignore[arg-type]
            "2026-01-01T00:00:00+00:00",
            mutate=True,
            scope="campaign:camp-1",
            campaign_id=None,
        )


def test_daemon_lease_expiry_mid_cycle_stops_later_new_arrivals(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F2: the daemon re-evaluates the lease per candidate — a lease that expires after the
    first send stops the remaining new arrivals (zero POSTs), and the blocked candidate defers
    (stays planned), never quarantines."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    a = SpecFile(series_id=1, file_id=10, shoko_id=500)
    b = SpecFile(series_id=2, file_id=20, shoko_id=501)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([a, b])
    state = {"valid": True}
    shoko.on_post = lambda: state.__setitem__("valid", False)
    sleeper = Sleeper()
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a, b],
            shoko_files=[a, b],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=sleeper.advance,
            shoko=shoko,
            attestation_valid=lambda: state["valid"],
            max_submissions=2,
        )
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
        state_a = store.get_file(500)
        state_b = store.get_file(501)
    finally:
        store.close()
    assert len(shoko.posts) == 1  # only the first new arrival POSTed
    assert state_a is not None
    assert state_a.state is FileState.VERIFIED
    assert state_b is not None
    assert state_b.state is FileState.PLANNED  # deferred, NOT quarantined
    assert any("attestation_invalid" in reason for reason in report.reasons)


def test_shutdown_flag_stops_daemon_apply_loop_new_submissions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F11: the daemon apply loop checks the shutdown flag before each new submission. A SIGTERM
    that lands mid-cycle stops the remaining new arrivals (zero further POSTs); the deferred rows
    stay planned (queueable), never quarantined, and the already-sent row stays durably tracked."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    a = SpecFile(series_id=1, file_id=10, shoko_id=500)
    b = SpecFile(series_id=2, file_id=20, shoko_id=501)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([a, b])
    state = {"stopped": False}
    shoko.on_post = lambda: state.__setitem__("stopped", True)  # SIGTERM lands during file 500
    sleeper = Sleeper()
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a, b],
            shoko_files=[a, b],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=sleeper.advance,
            shoko=shoko,
            shutdown_requested=lambda: state["stopped"],
            max_submissions=2,
        )
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
        state_a = store.get_file(500)
        state_b = store.get_file(501)
        sent_rows = store._conn.execute(
            "SELECT outcome FROM sends WHERE shoko_file_id = 500"
        ).fetchall()
    finally:
        store.close()
    assert len(shoko.posts) == 1  # the shutdown stops later submissions, never the in-flight one
    assert state_a is not None
    assert state_a.state is FileState.VERIFIED  # the in-flight send completed normally
    assert state_b is not None
    assert state_b.state is FileState.PLANNED  # deferred, NOT quarantined
    # Durably tracked exactly once: the completed row is closed (verified), never replayable.
    assert len(sent_rows) == 1
    assert str(sent_rows[0][0]) == "verified"
    assert any("shutdown_requested" in reason for reason in report.reasons)


def test_shutdown_during_preflight_network_phase_stops_the_daemon_post(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P2-1: a shutdown that lands DURING preflight's slow network phase (File + episode GETs)
    must not authorize the POST. The local freshness guard runs again after preflight, before
    ``prepare_send`` — the candidate defers (stays planned), zero POSTs, zero send rows."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    a = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([a])
    state = {"stopped": False}

    def _flip_on_preflight_read(path: str) -> None:
        if path.startswith("/api/v3/File/500"):
            state["stopped"] = True  # SIGTERM lands mid-preflight

    shoko.on_get = _flip_on_preflight_read
    sleeper = Sleeper()
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a],
            shoko_files=[a],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=sleeper.advance,
            shoko=shoko,
            shutdown_requested=lambda: state["stopped"],
            max_submissions=2,
        )
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
        record = store.get_file(500)
        rows = store._conn.execute("SELECT COUNT(*) FROM sends").fetchone()
    finally:
        store.close()
    assert len(shoko.posts) == 0  # the expired authorization never sent
    assert record is not None
    assert record.state is FileState.PLANNED  # deferred, NOT quarantined
    assert int(rows[0]) == 0  # nothing durably in-flight to replay
    assert any("shutdown_requested" in reason for reason in report.reasons)


def test_lease_expiry_during_preflight_network_phase_defers_daemon_post(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P2-1: a lease that expires DURING preflight's network phase must not ride the
    pre-preflight verdict into a POST — the lease/gate verdict is re-taken locally before
    ``prepare_send``; the candidate defers (stays planned), zero POSTs."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    a = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([a])
    lease = {"valid": True}

    def _expire_on_preflight_read(path: str) -> None:
        if path.startswith("/api/v3/File/500"):
            lease["valid"] = False  # the lease dies mid-preflight

    shoko.on_get = _expire_on_preflight_read
    sleeper = Sleeper()
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a],
            shoko_files=[a],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=sleeper.advance,
            shoko=shoko,
            attestation_valid=lambda: lease["valid"],
            max_submissions=2,
        )
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=RunPolicy.NEW_ARRIVAL, cycle_key="c1"
        )
        record = store.get_file(500)
        rows = store._conn.execute("SELECT COUNT(*) FROM sends").fetchone()
    finally:
        store.close()
    assert len(shoko.posts) == 0
    assert record is not None
    assert record.state is FileState.PLANNED
    assert int(rows[0]) == 0
    assert any("attestation_expired" in reason for reason in report.reasons)


@pytest.mark.parametrize("service_mode", [False, True])
def test_cycle_deadline_at_final_local_guard_leaves_no_send_or_budget_debt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, service_mode: bool
) -> None:
    """Slow final authorization plus the last Shoko read cannot reserve at the exact deadline."""
    if service_mode:
        monkeypatch.delenv("SHOKO_LINKER_ALLOW_APPLY", raising=False)
    else:
        monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    shoko = ShokoStub([file])
    clock = {"now": 0.0}
    final_file_read = {"done": False}
    auth_slept = {"done": False}

    def slow_final_file_read(path: str) -> None:
        if path == "/api/v3/File/500" and not final_file_read["done"]:
            final_file_read["done"] = True
            clock["now"] += 8.0

    def slow_final_authorization() -> bool:
        if final_file_read["done"] and not auth_slept["done"]:
            auth_slept["done"] = True
            clock["now"] += 2.0
        return True

    shoko.on_get = slow_final_file_read
    try:
        deps = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[file],
            shoko_files=[file],
            xml=mapping_xml(),
            monotonic=lambda: clock["now"],
            advance=Sleeper().advance,
            attestation_valid=slow_final_authorization,
            shoko=shoko,
            max_submissions=2,
            max_cycle_seconds=10.0,
        )
        policy = RunPolicy.NEW_ARRIVAL
        if service_mode:
            deps = replace(
                deps,
                service_mode=True,
                service_authorized=lambda: True,
                service_authorized_local=slow_final_authorization,
            )
            policy = RunPolicy.SERVICE
        report = run_cycle(
            deps, now_iso=T0, mode=CycleMode.APPLY, policy=policy, cycle_key="deadline-final-guard"
        )
        record = store.get_file(500)
        sends = store._conn.execute("SELECT COUNT(*) FROM sends").fetchone()
        budget = store.budget_view()
        last_request = shoko.requests[-1]
    finally:
        store.close()
    assert final_file_read["done"]
    assert auth_slept["done"]
    assert last_request.method == "GET"
    assert last_request.url.path == "/api/v3/File/500"
    assert shoko.posts_count() == 0
    assert int(sends[0]) == 0
    assert budget.pending_file_requests == 0
    assert budget.rolling_new_submissions == 0
    assert record is not None
    assert record.state is FileState.PLANNED
    assert any("cycle_time_budget_exhausted" in reason for reason in report.reasons)
