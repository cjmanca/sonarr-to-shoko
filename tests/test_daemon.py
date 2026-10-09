"""M6-b daemon gates: reconciliation cycle, mapping scheduler, daemon loop.

Every test is offline: injected endpoints run on ``httpx.MockTransport`` (no network,
no live Sonarr/Shoko), clocks are fake, and the state + mapping stores live in
``tmp_path``. The request log per test is the evidence: dry-run and
suppressed cycles show **zero POSTs**, the apply happy path shows **exactly one POST**,
and every crash/ambiguity/churn path shows the durable ordering.
"""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml  # type: ignore[import-untyped]

from shoko_linker.config import Settings, parse_settings
from shoko_linker.daemon import (
    CycleMode,
    DaemonDeps,
    compute_eligibility,
    run_cycle,
    run_daemon,
    run_mapping_check,
)
from shoko_linker.http import ApiEndpoint, make_timeout
from shoko_linker.linking import SendOutcome, VerificationVerdict
from shoko_linker.mapping_store import MappingStore, RefreshOutcome, refresh
from shoko_linker.policies import RunPolicy
from shoko_linker.state import (
    BaselineFile,
    BudgetLimits,
    Cohort,
    FileState,
    SendToken,
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


def _sonarr_path(file: SpecFile) -> str:
    return f"{SONARR_ROOT}/series{file.series_id}/{file.shoko_id}.mkv"


def _shoko_path(file: SpecFile) -> str:
    return f"{SHOKO_ROOT}/series{file.series_id}/{file.shoko_id}.mkv"


# ------------------------------------------------------------------ Shoko stub


class ShokoStub:
    """Scripted Shoko v5.3.3 API: discovery, preflight reads, ID chain, queue, POST."""

    def __init__(self, files: list[SpecFile]) -> None:
        self.files = {file.shoko_id: file for file in files}
        self.linked: dict[int, list[dict[str, Any]]] = {}
        self.requests: list[httpx.Request] = []
        self.posts: list[dict[str, Any]] = []
        self.fail_post: Exception | None = None
        self.fail_queue = False
        self.link_on_post = True
        self.post_status: int | None = None
        self.on_post: Callable[[], None] | None = None

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return self._respond(request)

    def _queue(self, request: httpx.Request) -> httpx.Response:
        if self.fail_queue:
            return httpx.Response(503, request=request)
        return self._json(request, {"WaitingCount": 5, "BlockedCount": 0, "TotalCount": 20})

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
        return self._json(request, {"Total": len(rows), "List": rows})

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
            return self._json(request, self._file_row(shoko_id, xrefs=[]))
        return self._json(request, self._file_row(shoko_id, xrefs=self.linked.get(shoko_id, [])))

    def _file_row(self, shoko_id: int, xrefs: list[dict[str, Any]]) -> dict[str, Any]:
        file = self.files[shoko_id]
        return {
            "ID": shoko_id,
            "Size": FILE_SIZE,
            "IsIgnored": False,
            "IsVariation": False,
            "Imported": "2024-01-01T00:00:00",
            "Hashes": {"ED2K": f"ed2k-{shoko_id}", "SHA1": None, "CRC32": shoko_id, "MD5": None},
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
        for file in self.files.values():
            if file.anime_id != anime_id:
                continue
            for _season, number, anidb_ep, _shoko in file.episodes:
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
        shoko_ep = SHOKO_EP
        for file in self.files.values():
            for _season, _number, anidb_ep, file_shoko in file.episodes:
                if anidb_ep == ep_id:
                    shoko_ep = file_shoko
        return self._json(
            request,
            {"IDs": {"ID": shoko_ep, "AniDB": ep_id, "TMDB": None}, "Type": "Episode"},
        )

    def posts_count(self) -> int:
        return len(self.posts)

    def methods(self) -> list[str]:
        return [request.method for request in self.requests]


# ------------------------------------------------------------------ Sonarr stub


class SonarrStub:
    """Scripted Sonarr v3 read-only inventory for a scenario."""

    def __init__(self, files: list[SpecFile]) -> None:
        self.files = files
        self.series_ids = sorted({file.series_id for file in files})
        self.series_tvdb = {file.series_id: file.tvdb_series for file in files}
        self.requests: list[httpx.Request] = []
        self.include_extra: SpecFile | None = None

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        path = request.url.path
        if path == "/api/v3/series":
            return self._json(request, self._series())
        if path == "/api/v3/rootfolder":
            return self._json(request, [{"id": 1, "path": SONARR_ROOT, "accessible": True}])
        if path == "/api/v3/episode":
            series_id = int(request.url.params["seriesId"])
            return self._json(request, self._episodes(series_id))
        if path == "/api/v3/episodefile":
            series_id = int(request.url.params["seriesId"])
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
                "tvdbId": self.series_tvdb[series_id],
            }
            for series_id in self.series_ids
        ]

    def _episodes(self, series_id: int) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for file in [item for item in self.files if item.series_id == series_id]:
            for season, number, _anidb, _shoko in file.episodes:
                rows.append(
                    {
                        "id": file.file_id * 100 + number,
                        "seriesId": series_id,
                        "seasonNumber": season,
                        "episodeNumber": number,
                        "title": f"S{season}E{number}",
                        "tvdbId": number,
                        "episodeFileId": file.file_id,
                    }
                )
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


def make_settings(tmp_path: Path, *, apply_changes: bool = True) -> Settings:
    document = {
        "sonarr": {
            "url": SONARR_BASE,
            "api_key_env": "SONARR_API_KEY",
            "allowed_roots": [SONARR_ROOT],
        },
        "shoko": {"url": SHOKO_BASE, "api_key_env": "SHOKO_API_KEY", "allowed_roots": [SHOKO_ROOT]},
        "paths": {"mappings": [{"sonarr": SONARR_ROOT, "shoko": SHOKO_ROOT}]},
        "mappings": {"source": "anime-lists", "source_url": SOURCE_URL, "refresh_hours": HOURS},
        "sync": {"apply_changes": apply_changes, "max_concurrent_links": 4},
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
            200, headers={"ETag": '"identity-src"'}, content=xml.encode(), request=request
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
    max_submissions: int = 1,
    verification_timeout: float = VERIFICATION_TIMEOUT,
    poll_interval: float = POLL_INTERVAL,
    max_cycle_seconds: float = 1e9,
    churn_limit: int = 500,
    max_recovery_rows_per_cycle: int = 100,
    settings: Settings | None = None,
    shoko: ShokoStub | None = None,
    mapping_dir: Path | None = None,
) -> DaemonDeps:
    settings = settings or make_settings(tmp_path)
    mapping_directory = mapping_dir if mapping_dir is not None else tmp_path / "mappings"
    mapping_store = MappingStore(mapping_directory, source_url=SOURCE_URL)
    shoko_stub = shoko if shoko is not None else ShokoStub(shoko_files)
    sonarr_stub = SonarrStub(sonarr_files)
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
        attestation_valid=lambda: attestation,
        blocked_over=0,
        verification_timeout_seconds=verification_timeout,
        poll_interval_seconds=poll_interval,
        max_verify_polls=20,
        max_submissions=max_submissions,
        max_cycle_seconds=max_cycle_seconds,
        churn_limit=churn_limit,
        max_recovery_rows_per_cycle=max_recovery_rows_per_cycle,
    )


def activate_mapping(tmp_path: Path, xml: str) -> None:
    seed_mapping(tmp_path / "mappings", xml, T0)


def open_tmp_store(tmp_path: Path) -> StateStore:
    store = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
    store.set_meta("budget_last_cycle_key", "cycle-1")
    return store


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
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=[multi],
        shoko_files=[multi],
        xml=mapping_xml(),
        monotonic=Clock([0.0, 100.0, 200.0, 300.0, 400.0, 500.0, 600.0, 700.0]),
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
    assert record.state in (FileState.WAITING, FileState.AMBIGUOUS)
    assert record.state is not FileState.PLANNED
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


def test_restart_does_not_disable_refresh_or_tight_loop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    mapping_dir = tmp_path / "mappings"
    requests: list[int] = []

    def source(request: httpx.Request) -> httpx.Response:
        requests.append(1)
        return httpx.Response(
            200, headers={"ETag": '"identity-src"'}, content=mapping_xml().encode(), request=request
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
    flags = {"stop": False}
    cycles_seen = {"n": 0}

    def should_stop() -> bool:
        cycles_seen["n"] += 1
        flags["stop"] = cycles_seen["n"] >= STOP_AT
        return flags["stop"]

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
        )
    finally:
        store.close()
    assert report.shutdown_requested is True
    assert report.exit_reason == "shutdown_requested"
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
    """P1-1 regression: source identity change → requires_rebaseline + apply suppressed
    for that cycle; the next fresh full-scan cycle with the stable new identity re-anchors
    and clears the flag, and apply resumes."""
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
    finally:
        store.close()
    assert r1.counts.submitted == 1
    # Identity changed: discontinuity → flag persisted, apply suppressed, ZERO posts.
    assert "instance_identity_changed" in r2.reasons
    assert r2.writes_suppressed is True
    assert "source_unstable" in r2.reasons
    assert flag_after_change == "1"
    assert posts_after_change_cycle == POSTS_ONE
    # Fresh full-scan observation with the stable new identity clears the flag.
    assert flag_after_reanchor == "0"
    assert r3.counts.submitted == 1
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


def test_mark_sent_crash_past_deadline_closes_partial_and_releases_slot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P1-3 probe P2: crash after mark_sent (request_may_have_been_sent), XRefs show
    NOT linked, deadline past → PARTIAL close, slot released, posts stay 1 — no resubmit."""
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
    finally:
        store.close()
    assert record is not None
    assert record.state is FileState.PARTIAL
    assert view.pending_file_requests == 0
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
            "Hashes": {"ED2K": f"ed2k-{shoko_id}", "SHA1": None, "CRC32": shoko_id, "MD5": None},
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
            " sonarr_size, state, cohort, state_detail, updated_iso, first_seen_iso)"
            " VALUES (?, ?, ?, ?, ?, 'verifying', 'new_arrival', 'verify', ?, ?)",
            [
                (
                    file_id,
                    file_id % 50 + 1,
                    file_id,
                    f"/data/media/anime/series{file_id}/f{file_id}.mkv",
                    1000 + file_id,
                    T0,
                    T0,
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


def test_late_delivery_does_not_double_release_slot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P1-A probe (reviewer p1b, daemon level, ceiling 1): a partial close releases A's slot;
    a late ManualLinkJob lands A VERIFIED → NO second release; pending never exceeds the
    ceiling; file B is admitted only after A's true release."""
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
        # Cycle 2 past the deadline: NOT-linked → partial close, release (slot_released=1).
        deps2 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a],
            shoko_files=[a],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
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
        file_500 = store.get_file(500)
        assert file_500 is not None
        assert file_500.state is FileState.PARTIAL
        assert store.budget_view().pending_file_requests == 0
        inspection = store.active_send_details(500)
        assert inspection is not None
        assert inspection.slot_released is True

        # Late ManualLinkJob lands: A materializes server-side.
        shoko.materialize(500)
        shoko.link_on_post = True  # the server now materializes XRefs on the wire
        # Cycle 3: A (partial) re-polls to VERIFIED → NO second release (pending stays 0).
        deps3 = build_deps(
            tmp_path,
            store=store,
            sonarr_files=[a],
            shoko_files=[a],
            xml=mapping_xml(),
            monotonic=Clock([MONO_ZERO]),
            advance=sleeper.advance,
            shoko=shoko,
        )
        run_cycle(
            deps3,
            now_iso=_iso_offset(minutes=10),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c3",
        )
        record_a = store.get_file(500)
        view_after_verify = store.budget_view()
        assert record_a is not None
        assert record_a.state is FileState.VERIFIED
        assert view_after_verify.pending_file_requests == 0  # never -1 / never re-released
        assert store.live_send_counts() == (0, 0)

        # Cycle 4: B (new arrival) is admitted only because A's slot is truly released.
        deps4 = build_deps(
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
        r4 = run_cycle(
            deps4,
            now_iso=_iso_offset(minutes=15),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="c4",
        )
        record_b = store.get_file(501)
    finally:
        store.close()
    assert r4.counts.submitted == 1
    assert shoko.posts_count() == POSTS_TWO  # A (crashed) + B
    assert record_b is not None
    assert record_b.state is FileState.VERIFIED


def test_recovery_pending_deadline_terminates_partial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P1-B probe (reviewer p2d): a persisted PENDING row must not slide its deadline with
    every poll. With interval 20 s and timeout 50 s, 11 cycles take it past the deadline to a
    PARTIAL terminal (slot released per P1-A), GETs stay bounded, posts stay 0."""
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
    finally:
        store.close()
    assert record is not None
    assert record.state in (FileState.PARTIAL, FileState.CONFLICT)  # terminal, not PENDING
    assert record.state is not FileState.VERIFYING
    assert view.pending_file_requests == 0  # slot released exactly once (P1-A)
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
        )
        assert isinstance(token, SendToken)
        store.mark_sent(token, iso=T0)
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=T0)
        store.record_verification(spec.shoko_id, VerificationVerdict.CONTRADICTION, iso=T0)
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
