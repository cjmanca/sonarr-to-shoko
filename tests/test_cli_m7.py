"""M7-a CLI gates: command surface, budgets, gate refusal, operator commands.

Every test is offline: the CLI dependency seams (:func:`shoko_linker.cli.open_state_store`,
:func:`shoko_linker.cli.open_mapping_store`, :func:`shoko_linker.cli.sonarr_endpoint`,
:func:`shoko_linker.cli.shoko_endpoint`, :func:`shoko_linker.cli.source_endpoint`,
:func:`shoko_linker.cli.current_iso`, :func:`shoko_linker.cli.sleep`) are replaced with
``httpx.MockTransport`` endpoints, tmp stores and fixed clocks. The recorded request logs
are the evidence: mapping/report/health commands make **zero** Sonarr/Shoko requests,
dry-run cycles make **zero POSTs**, and the submission cap counts POSTs, not files.
"""

from __future__ import annotations

import json
import logging
import multiprocessing
import os
import signal
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import yaml  # type: ignore[import-untyped]

from shoko_linker import cli
from shoko_linker.attestation import SCOPE_DAEMON, build_record, write_attestation
from shoko_linker.cli import (
    EXIT_APPLY_GATE_REFUSAL,
    EXIT_CONFIG_INVALID,
    EXIT_OK,
    EXIT_RUNTIME,
    EXIT_STATE,
    main,
)
from shoko_linker.config import load_settings
from shoko_linker.daemon import CycleMode, run_daemon
from shoko_linker.http import ApiEndpoint, make_timeout
from shoko_linker.linking import SendOutcome
from shoko_linker.mapping_store import MappingStore, RefreshOutcome
from shoko_linker.state import (
    BaselineFile,
    Cohort,
    FileSeed,
    FileState,
    StateStore,
    acquire_apply_lock,
    acquire_or_steal_apply_lock,
    release_apply_lock,
)

SONARR_BASE = "http://sonarr:8989"
SHOKO_BASE = "http://shoko:8111"
SOURCE_ORIGIN = "https://raw.githubusercontent.com"
SOURCE_PATH = "/Anime-Lists/anime-lists/master/anime-list-full.xml"
SOURCE_URL = SOURCE_ORIGIN + SOURCE_PATH
SONARR_ROOT = "/data/media/anime"
SHOKO_ROOT = "/anime"
ANIME_ID = 1234
TVDB_SERIES = 267440
ANIDB_EP = 5501
SHOKO_EP = 777
FILE_SIZE = 1000
T0 = "2026-01-01T00:00:00+00:00"
PLAN_FP = "a" * 64
REQUEST_FP = "b" * 64
DUMMY_SHOKO_KEY = "dummy-shoko-key-value-00000000"
DUMMY_SONARR_KEY = "dummy-sonarr-key-value-00000000"
FIRST_RUN_COUNT = 7000
VERIFIED_COUNT = 6000
COUNT_TWO = 2
COUNT_FIVE = 5
LOCK_VICTIM_PID = 999
OK_STATUS = 200
MAX_LINKS = 2
NEW_ARRIVAL_FILE_ID = 500
SOURCE_REQUESTS_TWO = 2
CYCLES_TWO = 2
CYCLES_ONE = 1


def _entry(tvdb: int, anidb: int) -> str:
    return (
        f"<anime anidbid='{anidb}' tvdbid='{tvdb}' defaulttvdbseason='1'>"
        "<mapping-list><mapping anidbseason='1' tvdbseason='1'>;1-1;</mapping></mapping-list>"
        "</anime>"
    )


def mapping_xml() -> str:
    return "<anime-list>" + _entry(TVDB_SERIES, ANIME_ID) + "</anime-list>"


@dataclass(frozen=True)
class SpecFile:
    series_id: int
    file_id: int
    shoko_id: int


def _sonarr_path(file: SpecFile) -> str:
    return f"{SONARR_ROOT}/series{file.series_id}/{file.shoko_id}.mkv"


def _shoko_path(file: SpecFile) -> str:
    return f"{SHOKO_ROOT}/series{file.series_id}/{file.shoko_id}.mkv"


# ------------------------------------------------------------------ request-recording stubs


@dataclass
class Stubs:
    """Minimal scripted Sonarr/Shoko/source endpoints with per-service request logs."""

    files: list[SpecFile] = field(default_factory=list)
    sonarr_requests: list[httpx.Request] = field(default_factory=list)
    shoko_requests: list[httpx.Request] = field(default_factory=list)
    source_requests: list[httpx.Request] = field(default_factory=list)
    posts: list[dict[str, Any]] = field(default_factory=list)
    linked: dict[int, list[dict[str, Any]]] = field(default_factory=dict)
    source_xml: str = field(default_factory=mapping_xml)
    source_status: int = 200
    source_etag: str = '"identity-src"'
    conditional_304: bool = False
    blocked_count: int = 0

    def all_service_requests(self) -> list[httpx.Request]:
        return [*self.sonarr_requests, *self.shoko_requests]

    def sonarr(self, request: httpx.Request) -> httpx.Response:
        self.sonarr_requests.append(request)
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

    def shoko(self, request: httpx.Request) -> httpx.Response:  # noqa: PLR0911
        self.shoko_requests.append(request)
        path = request.url.path
        if request.method == "POST" and path.endswith("/Link"):
            file_id = int(path.split("/")[4])
            self.posts.append(json.loads(request.content))
            self.linked[file_id] = [self._xref(file_id)]
            return self._json(request, {})
        if path == "/api/v3/ImportFolder":
            return self._json(
                request,
                [{"ID": 1, "Name": "Sonarr", "Path": SHOKO_ROOT, "DropFolderType": "None"}],
            )
        if path == "/api/v3/Queue":
            return self._json(
                request,
                {
                    "WaitingCount": 1,
                    "BlockedCount": self.blocked_count,
                    "TotalCount": 4,
                    "ThreadCount": 1,
                },
            )
        if path.startswith("/api/v3/Series/AniDB/"):
            anime_id = int(path.split("/")[5])
            rows = [self._anidb_episode() for _ in self.files if anime_id == ANIME_ID]
            return self._json(request, {"Total": len(rows), "List": rows})
        if path.startswith("/api/v3/Episode/AniDB/"):
            return self._json(
                request,
                {"IDs": {"ID": SHOKO_EP, "AniDB": ANIDB_EP, "TMDB": None}, "Type": "Episode"},
            )
        if path == "/api/v3/File":
            rows = [self._file_row(file_id) for file_id in sorted(f.shoko_id for f in self.files)]
            return self._json(request, {"Total": len(rows), "List": rows})
        if path.startswith("/api/v3/File/"):
            file_id = int(path.split("/")[4])
            return self._json(request, self._file_row(file_id))
        return httpx.Response(404, json={"message": "no route"}, request=request)

    def source(self, request: httpx.Request) -> httpx.Response:
        self.source_requests.append(request)
        if self.source_status != OK_STATUS:
            return httpx.Response(
                self.source_status, json={"message": "source down"}, request=request
            )
        if self.conditional_304 and "if-none-match" in request.headers:
            return httpx.Response(304, request=request)
        return httpx.Response(
            200,
            headers={"ETag": self.source_etag},
            content=self.source_xml.encode(),
            request=request,
        )

    def _json(self, request: httpx.Request, payload: Any) -> httpx.Response:
        return httpx.Response(200, json=payload, request=request)

    def _series(self) -> list[dict[str, Any]]:
        return [
            {
                "id": file.series_id,
                "title": f"Series {file.series_id}",
                "rootFolderPath": f"{SONARR_ROOT}/series{file.series_id}",
                "tvdbId": TVDB_SERIES,
            }
            for file in {f.series_id: f for f in self.files}.values()
        ]

    def _episodes(self, series_id: int) -> list[dict[str, Any]]:
        return [
            {
                "id": file.file_id * 100 + 1,
                "seriesId": series_id,
                "seasonNumber": 1,
                "episodeNumber": 1,
                "title": "S01E01",
                "tvdbId": 1,
                "episodeFileId": file.file_id,
            }
            for file in self.files
            if file.series_id == series_id
        ]

    def _files(self, series_id: int) -> list[dict[str, Any]]:
        return [
            {
                "id": file.file_id,
                "seriesId": series_id,
                "path": _sonarr_path(file),
                "size": FILE_SIZE,
            }
            for file in self.files
            if file.series_id == series_id
        ]

    def _file_row(self, file_id: int) -> dict[str, Any]:
        file = next(item for item in self.files if item.shoko_id == file_id)
        return {
            "ID": file_id,
            "Size": FILE_SIZE,
            "IsIgnored": False,
            "IsVariation": False,
            "Imported": "2024-01-01T00:00:00",
            "Hashes": {"ED2K": f"ed2k-{file_id}", "SHA1": None, "CRC32": file_id, "MD5": None},
            "Locations": [
                {
                    "ID": file_id * 10,
                    "FileID": file_id,
                    "ImportFolderID": 1,
                    "RelativePath": f"series{file.series_id}/{file.shoko_id}.mkv",
                    "AbsolutePath": _shoko_path(file),
                    "IsAccessible": True,
                }
            ],
            "SeriesIDs": self.linked.get(file_id, []),
        }

    def _xref(self, file_id: int) -> dict[str, Any]:
        return {
            "SeriesID": {"ID": None, "AniDB": ANIME_ID, "TMDB": None},
            "EpisodeIDs": [{"ID": SHOKO_EP, "AniDB": ANIDB_EP, "TMDB": None, "Source": "User"}],
        }

    def _anidb_episode(self) -> dict[str, Any]:
        return {
            "ID": ANIDB_EP,
            "AnimeID": ANIME_ID,
            "Type": "Episode",
            "EpisodeNumber": 1,
            "AirDate": None,
            "Title": "Ep 1",
            "Titles": [],
            "Description": None,
            "Rating": None,
        }


# ------------------------------------------------------------------ environment fixture


@dataclass
class Env:
    tmp_path: Path
    stubs: Stubs
    sleeps: list[float]
    state_open_calls: list[Path]
    config_path: Path
    now_iso: str = T0

    @property
    def state_path(self) -> Path:
        return self.tmp_path / "state.sqlite3"

    @property
    def mapping_dir(self) -> Path:
        return self.tmp_path / "mappings"

    def limits(self) -> Any:
        settings = load_settings(self.config_path)
        return cli.budget_limits_from_settings(settings)

    def open_store(self) -> StateStore:
        return StateStore(self.state_path, budget_limits=self.limits())

    def seed_mapping(self) -> None:
        """Seed the last-known-good cache without polluting the env request logs."""
        from shoko_linker.mapping_store import refresh

        seed_stubs = Stubs(source_xml=self.stubs.source_xml)
        store = MappingStore(self.mapping_dir, source_url=SOURCE_URL)
        endpoint = _source_endpoint(store, seed_stubs.source)
        try:
            refresh(store, endpoint, now=_parse(self.now_iso), refresh_hours=168.0, force=True)
        finally:
            endpoint.client.close()

    def seed_baseline(self, files: list[SpecFile]) -> None:
        store = self.open_store()
        try:
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
                iso=self.now_iso,
            )
        finally:
            store.close()


def _parse(raw: str) -> Any:
    from datetime import UTC, datetime

    parsed = datetime.fromisoformat(raw)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _endpoint(handler: Callable[[httpx.Request], httpx.Response], base: str) -> ApiEndpoint:
    client = httpx.Client(
        base_url=base,
        transport=httpx.MockTransport(handler),
        follow_redirects=False,
        timeout=make_timeout(1.0, 1.0),
    )
    return ApiEndpoint(client, lambda: {})


def _source_endpoint(
    store: MappingStore, handler: Callable[[httpx.Request], httpx.Response]
) -> ApiEndpoint:
    client = httpx.Client(
        base_url=store.origin + "/",
        transport=httpx.MockTransport(handler),
        follow_redirects=False,
        timeout=make_timeout(1.0, 1.0),
    )
    return ApiEndpoint(client, lambda: {"Accept-Encoding": "identity"})


def cli_document(
    *, apply_changes: bool = False, sync_extra: dict[str, Any] | None = None
) -> dict[str, Any]:
    # Grace window OFF by default in the CLI harness: the apply-path tests here pin the
    # submit/verify wiring. Grace enforcement is proven in tests/test_daemon.py.
    sync: dict[str, Any] = {
        "apply_changes": apply_changes,
        "jitter_ratio": 0.0,
        "new_file_grace_period_seconds": 0,
    }
    sync.update(sync_extra or {})
    return {
        "sonarr": {
            "url": SONARR_BASE,
            "api_key_env": "SONARR_API_KEY",
            "allowed_roots": [SONARR_ROOT],
        },
        "shoko": {"url": SHOKO_BASE, "api_key_env": "SHOKO_API_KEY", "allowed_roots": [SHOKO_ROOT]},
        "paths": {"mappings": [{"sonarr": SONARR_ROOT, "shoko": SHOKO_ROOT}]},
        "mappings": {"source": "anime-lists", "source_url": SOURCE_URL, "refresh_hours": 168},
        "sync": sync,
        "database": {"path": "/data/state.sqlite3"},
    }


@pytest.fixture(autouse=True)
def _no_root_logging_leak() -> Iterator[None]:
    yield
    for handler in list(logging.getLogger().handlers):
        logging.getLogger().removeHandler(handler)


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Env]:
    """CLI wired to tmp stores, mocked endpoints and a fixed clock."""
    original_sigint = signal.getsignal(signal.SIGINT)
    config_path = write_config(tmp_path, cli_document())
    environment = Env(
        tmp_path=tmp_path,
        stubs=Stubs(),
        sleeps=[],
        state_open_calls=[],
        config_path=config_path,
    )

    monkeypatch.setattr(
        cli, "open_state_store", lambda settings: _record_open(environment, settings)
    )
    monkeypatch.setattr(cli, "state_path", lambda settings: environment.state_path)
    monkeypatch.setattr(
        cli,
        "open_mapping_store",
        lambda settings: MappingStore(environment.mapping_dir, source_url=SOURCE_URL),
    )
    monkeypatch.setattr(
        cli, "sonarr_endpoint", lambda settings: _endpoint(environment.stubs.sonarr, SONARR_BASE)
    )
    monkeypatch.setattr(
        cli, "shoko_endpoint", lambda settings: _endpoint(environment.stubs.shoko, SHOKO_BASE)
    )
    monkeypatch.setattr(
        cli,
        "source_endpoint",
        lambda store, settings: _source_endpoint(store, environment.stubs.source),
    )
    monkeypatch.setattr(cli, "current_iso", lambda: environment.now_iso)
    # F6 seam split: in-cycle pacing uses ``sleep``; the inter-cycle daemon sleep is the
    # dedicated ``interruptible_sleep`` seam — record it as the interval.
    monkeypatch.setattr(cli, "sleep", lambda seconds: None)

    def _record_interval(seconds: float) -> None:
        environment.sleeps.append(seconds)

    monkeypatch.setattr(cli, "interruptible_sleep", _record_interval)
    yield environment
    signal.signal(signal.SIGINT, cast("Any", original_sigint))


def _record_open(environment: Env, settings: Any) -> StateStore:
    environment.state_open_calls.append(environment.state_path)
    return StateStore(
        environment.state_path, budget_limits=cli.budget_limits_from_settings(settings)
    )


def write_config(tmp_path: Path, document: dict[str, Any]) -> Path:
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return path


def write_daemon_lease(path: Path) -> None:
    """A valid daemon-scope lease for ``SHOKO_BASE`` (the operator-built trust anchor)."""
    record = build_record(
        endpoint=SHOKO_BASE,
        version="5.3.3",
        mounts=["/anime mounted read-only (operator-inspected on the host)"],
        settings_flags={
            "FileQualityFilterEnabled": False,
            "RenameOnImport": False,
            "MoveOnImport": False,
            "MyListAddFiles": False,
        },
        inspected_iso=T0,
        expires_iso="2026-01-08T00:00:00+00:00",
        scope=SCOPE_DAEMON,
        operator_note="offline gate fixture lease",
    )
    write_attestation(path, record)


def captured_json(capsys: pytest.CaptureFixture[str]) -> dict[str, Any]:
    return cast("dict[str, Any]", json.loads(capsys.readouterr().out))


# ------------------------------------------------------------------ mappings refresh


def test_mappings_refresh_activation_then_304_makes_no_service_requests(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    config = str(env.config_path)
    assert main(["mappings", "refresh", config, "--json"]) == EXIT_OK
    activated = captured_json(capsys)
    assert activated["outcome"] == RefreshOutcome.ACTIVATED.value
    assert env.mapping_dir.joinpath("active.xml").exists()

    env.stubs.conditional_304 = True
    env.now_iso = "2026-01-08T00:00:00+00:00"
    assert main(["mappings", "refresh", config, "--json"]) == EXIT_OK
    unchanged = captured_json(capsys)
    assert unchanged["outcome"] == RefreshOutcome.UNCHANGED.value
    assert unchanged["generation"]["revision"] == activated["generation"]["revision"]
    assert len(env.stubs.source_requests) == SOURCE_REQUESTS_TWO
    assert env.stubs.all_service_requests() == []


def test_mappings_refresh_failure_keeps_last_known_good(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    env.seed_mapping()
    before = (env.mapping_dir / "active.xml").read_bytes()
    env.stubs.source_status = 500
    env.now_iso = "2026-01-08T00:00:00+00:00"
    code = main(["mappings", "refresh", str(env.config_path), "--json"])
    payload = captured_json(capsys)
    assert code == EXIT_RUNTIME
    assert payload["outcome"] == RefreshOutcome.FAILED_TRANSPORT.value
    assert payload["success"] is False
    assert (env.mapping_dir / "active.xml").read_bytes() == before
    assert payload["generation"]["generation_active"] is True
    assert env.stubs.all_service_requests() == []


def test_mappings_refresh_not_due_makes_zero_requests(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    env.seed_mapping()
    env.now_iso = "2026-01-02T00:00:00+00:00"
    assert main(["mappings", "refresh", str(env.config_path), "--json"]) == EXIT_OK
    payload = captured_json(capsys)
    assert payload["outcome"] == RefreshOutcome.NOT_DUE.value
    assert env.stubs.source_requests == []
    assert main(["mappings", "refresh", "--force", str(env.config_path), "--json"]) == EXIT_OK
    assert len(env.stubs.source_requests) == 1


# ------------------------------------------------------------------ mappings status


def test_mappings_status_reports_generation_and_state_counts(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    env.seed_mapping()
    store = env.open_store()
    try:
        store.record_files(
            [
                FileSeed(
                    shoko_file_id=file_id,
                    sonarr_series_id=1,
                    sonarr_file_id=file_id,
                    sonarr_path=f"{SONARR_ROOT}/series1/{file_id}.mkv",
                    sonarr_size=FILE_SIZE,
                    cohort=Cohort.NEW_ARRIVAL,
                    iso=T0,
                    state=state,
                )
                for file_id, state in (
                    (11, FileState.WAITING),
                    (12, FileState.WAITING),
                    (13, FileState.AMBIGUOUS),
                    (14, FileState.INDETERMINATE_WRITE),
                )
            ]
        )
    finally:
        store.close()
    code = main(["mappings", "status", str(env.config_path), "--json"])
    payload = captured_json(capsys)
    assert code == EXIT_OK
    assert payload["generation_active"] is True
    assert payload["revision"] >= 1
    assert payload["targets_digest"]
    assert payload["structure_digest"]
    assert payload["active_age_hours"] == 0.0
    assert payload["next_due_iso"] is not None
    assert payload["mapping_affected_candidates"] == {
        "waiting": 2,
        "ambiguous": 1,
        "indeterminate_write": 1,
    }
    assert env.stubs.all_service_requests() == []
    assert DUMMY_SHOKO_KEY not in json.dumps(payload)


def test_mappings_status_absent_generation_is_informational(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(["mappings", "status", str(env.config_path), "--json"])
    payload = captured_json(capsys)
    assert code == EXIT_OK
    assert payload["generation_active"] is False
    assert payload["revision"] == 0
    assert env.stubs.all_service_requests() == []


# ------------------------------------------------------------------ once


def test_once_dry_run_runs_full_cycle_with_zero_posts(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    files = [SpecFile(series_id=1, file_id=10, shoko_id=500)]
    env.stubs.files = files
    env.seed_mapping()
    code = main(["once", "--dry-run", str(env.config_path), "--json"])
    payload = captured_json(capsys)
    assert code == EXIT_OK
    assert payload["mode"] == CycleMode.DRY_RUN.value
    assert payload["counts"]["discovered"] == 1
    assert payload["counts"]["submitted"] == 0
    assert env.stubs.posts == []
    store = env.open_store()
    try:
        planned = store.files_in_state(FileState.PLANNED)
        assert len(planned) == 1
        assert planned[0].shoko_file_id == NEW_ARRIVAL_FILE_ID
    finally:
        store.close()


def test_once_human_output_names_policy_mode_and_cap(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    env.seed_mapping()
    env.stubs.files = [SpecFile(series_id=1, file_id=10, shoko_id=500)]
    code = main(["once", "--policy", "new_arrival", str(env.config_path)])
    out = capsys.readouterr().out
    assert code == EXIT_OK
    assert "policy: new arrival" in out
    assert "dry-run" in out
    assert "max_submissions (POSTs, not enumerated files)" in out


def test_once_max_links_counts_submissions_not_files(
    env: Env, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Apply needs the full M7-b conjunction: gate open AND a valid daemon-scope lease."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    write_daemon_lease(env.tmp_path / "lease.yaml")
    monkeypatch.setattr(
        cli,
        "attestation_target_path",
        lambda settings, scope=SCOPE_DAEMON, *, for_write=False: env.tmp_path / "lease.yaml",
    )
    config = write_config(env.tmp_path, cli_document(apply_changes=True))
    files = [
        SpecFile(series_id=1, file_id=20 + index, shoko_id=600 + index)
        for index in range(COUNT_FIVE)
    ]
    env.stubs.files = files
    env.seed_mapping()
    env.seed_baseline([SpecFile(series_id=99, file_id=990, shoko_id=999)])
    code = main(["once", "--apply", "--max-links", str(MAX_LINKS), str(config), "--json"])
    payload = captured_json(capsys)
    assert code == EXIT_OK
    assert payload["counts"]["new_arrival"] == COUNT_FIVE
    assert payload["counts"]["submitted"] == MAX_LINKS
    assert payload["deferred_planned"] == COUNT_FIVE - MAX_LINKS
    assert len(env.stubs.posts) == MAX_LINKS


def test_once_dry_run_and_apply_cannot_be_combined(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(["once", "--dry-run", "--apply", str(env.config_path)])
    out = capsys.readouterr().out
    assert code == EXIT_CONFIG_INVALID
    assert "cannot be combined" in out
    assert env.state_open_calls == []


def test_once_apply_without_gate_refuses_before_touching_state(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(["once", "--apply", str(env.config_path)])
    out = capsys.readouterr().out
    assert code == EXIT_APPLY_GATE_REFUSAL
    assert "REFUSED" in out
    assert "SHOKO_LINKER_ALLOW_APPLY" in out
    assert env.state_open_calls == []
    assert not env.state_path.exists()
    assert env.stubs.all_service_requests() == []


def test_once_apply_with_gate_keeps_attestation_closed(
    env: Env, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    config = write_config(env.tmp_path, cli_document(apply_changes=True))
    env.stubs.files = [SpecFile(series_id=1, file_id=10, shoko_id=500)]
    env.seed_mapping()
    env.seed_baseline([SpecFile(series_id=99, file_id=990, shoko_id=999)])
    code = main(["once", "--apply", str(config), "--json"])
    payload = captured_json(capsys)
    assert code == EXIT_OK
    assert payload["mode"] == CycleMode.APPLY.value
    assert payload["eligibility"]["eligible"] is False
    assert "attestation_invalid" in payload["eligibility"]["reasons"]
    assert env.stubs.posts == []


# ------------------------------------------------------------- M7 review: queue guard, version pin


def _wire_daemon_lease(env: Env, monkeypatch: pytest.MonkeyPatch, document: dict[str, Any]) -> Path:
    """Gate-open config + daemon-scope lease at the seam path; returns the config path."""
    write_daemon_lease(env.tmp_path / "lease.yaml")
    monkeypatch.setattr(
        cli,
        "attestation_target_path",
        lambda settings, scope=SCOPE_DAEMON, *, for_write=False: env.tmp_path / "lease.yaml",
    )
    return write_config(env.tmp_path, document)


def test_once_apply_blocked_queue_refuses_all_posts(
    env: Env, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """M7 review P1 once-wiring: BlockedCount 3 > tolerance 0 → zero POSTs + queue_blocked:3."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    config = _wire_daemon_lease(env, monkeypatch, cli_document(apply_changes=True))
    files = [
        SpecFile(series_id=1, file_id=20 + index, shoko_id=600 + index)
        for index in range(COUNT_FIVE)
    ]
    env.stubs.files = files
    env.stubs.blocked_count = 3
    env.seed_mapping()
    env.seed_baseline([SpecFile(series_id=99, file_id=990, shoko_id=999)])
    code = main(["once", "--apply", str(config), "--json"])
    payload = captured_json(capsys)
    assert code == EXIT_OK
    assert payload["counts"]["submitted"] == 0
    assert env.stubs.posts == []
    assert "queue_blocked:3" in payload["reasons"]


def test_once_apply_blocked_queue_within_tolerance_applies(
    env: Env, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """sync.queue_blocked_tolerance = 5: BlockedCount 3 stays inside tolerance → applies."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    config = _wire_daemon_lease(
        env,
        monkeypatch,
        cli_document(apply_changes=True, sync_extra={"queue_blocked_tolerance": 5}),
    )
    env.stubs.files = [SpecFile(series_id=1, file_id=10, shoko_id=500)]
    env.stubs.blocked_count = 3
    env.seed_mapping()
    env.seed_baseline([SpecFile(series_id=99, file_id=990, shoko_id=999)])
    code = main(["once", "--apply", str(config), "--json"])
    payload = captured_json(capsys)
    assert code == EXIT_OK
    assert payload["counts"]["submitted"] == 1
    assert len(env.stubs.posts) == 1


def test_attested_shoko_version_mismatch_suppresses_apply(
    env: Env, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """M7 review P3-4: lease v5.3.3 vs pin v5.3.4 → stale_version, zero POSTs."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    document = cli_document(apply_changes=True)
    document["safety"] = {
        "attestation_file": "/data/lease.yaml",
        "attested_shoko_version": "5.3.4",
    }
    config = _wire_daemon_lease(env, monkeypatch, document)
    env.stubs.files = [SpecFile(series_id=1, file_id=10, shoko_id=500)]
    env.seed_mapping()
    env.seed_baseline([SpecFile(series_id=99, file_id=990, shoko_id=999)])
    code = main(["once", "--apply", str(config), "--json"])
    payload = captured_json(capsys)
    assert code == EXIT_OK
    assert payload["eligibility"]["eligible"] is False
    assert "attestation_invalid" in payload["eligibility"]["reasons"]
    assert "attestation_stale_version" in payload["reasons"]
    assert env.stubs.posts == []


def test_attested_shoko_version_match_keeps_apply_open(
    env: Env, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Pin equals the inspected lease version (5.3.3): apply behavior is unchanged."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    document = cli_document(apply_changes=True)
    document["safety"] = {
        "attestation_file": "/data/lease.yaml",
        "attested_shoko_version": "5.3.3",
    }
    config = _wire_daemon_lease(env, monkeypatch, document)
    env.stubs.files = [SpecFile(series_id=1, file_id=10, shoko_id=500)]
    env.seed_mapping()
    env.seed_baseline([SpecFile(series_id=99, file_id=990, shoko_id=999)])
    code = main(["once", "--apply", str(config), "--json"])
    payload = captured_json(capsys)
    assert code == EXIT_OK
    assert payload["counts"]["submitted"] == 1
    assert len(env.stubs.posts) == 1


# ------------------------------------------------------------------ daemon


def test_daemon_runs_bounded_cycles_and_respects_interval(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    env.stubs.files = [SpecFile(series_id=1, file_id=10, shoko_id=500)]
    env.seed_mapping()
    code = main(["daemon", "--cycles", str(CYCLES_TWO), str(env.config_path)])
    out = capsys.readouterr().out
    assert code == EXIT_OK
    assert out.count("cycle cycle-") == CYCLES_TWO
    assert f"daemon exit: stop_after_cycles after {CYCLES_TWO} cycle(s)" in out
    assert env.sleeps == [300.0]
    store = env.open_store()
    try:
        assert store.get_meta("last_cycle_end_iso") == T0
    finally:
        store.close()


def test_daemon_once_signal_runs_a_single_cycle(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    env.seed_mapping()
    code = main(["daemon", "--once-signal", str(env.config_path)])
    assert code == EXIT_OK
    assert "after 1 cycle(s)" in capsys.readouterr().out
    assert env.sleeps == []


def test_daemon_preamble_reports_campaign_auto_drain(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F7 + F10: the daemon preamble states auto-drain AND the explicit drain-authorization gate."""
    env.seed_mapping()
    code = main(["daemon", "--cycles", str(CYCLES_ONE), str(env.config_path)])
    out = capsys.readouterr().out
    assert code == EXIT_OK
    assert "campaign drain: approved backlog campaigns drain automatically" in out
    assert "campaign authorize-drain" in out


def test_config_check_shows_campaign_daemon_auto_apply(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F7: the resolved config summary surfaces the campaign.daemon_auto_apply knob."""
    code = main(["config", str(env.config_path), "--check"])
    out = capsys.readouterr().out
    assert code == EXIT_OK
    assert "daemon_auto_apply=True" in out


def test_daemon_json_stdout_is_a_single_json_document(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F1: ``daemon --json`` stdout is exactly one JSON document (no preamble/config summary)."""
    env.seed_mapping()
    env.stubs.files = [SpecFile(series_id=1, file_id=10, shoko_id=500)]
    code = main(["daemon", "--cycles", str(CYCLES_ONE), "--json", str(env.config_path)])
    out = capsys.readouterr().out
    assert code == EXIT_OK
    payload = cast("dict[str, Any]", json.loads(out))
    assert out.startswith("{")
    assert payload["cycles_run"] == CYCLES_ONE
    assert payload["exit_reason"] == "stop_after_cycles"
    for marker in ("requested policy:", "loop mode:", "cycles:", "signal handlers installed:"):
        assert marker not in out


def test_signal_handlers_stop_the_daemon_after_the_current_cycle(env: Env) -> None:
    env.seed_mapping()
    env.stubs.files = [SpecFile(series_id=1, file_id=10, shoko_id=500)]
    settings = load_settings(env.config_path)
    original = signal.getsignal(signal.SIGINT)
    state = cli.StopState()
    try:
        installed = cli.make_signal_handlers(state)
        assert int(signal.SIGINT) in installed
        assert not state.should_stop()
        handler = signal.getsignal(signal.SIGINT)
        assert callable(handler)
        handler(signal.SIGINT, None)
        assert state.should_stop()
        store = env.open_store()
        try:
            deps = cli.build_daemon_deps(
                settings,
                store=store,
                mapping_store=MappingStore(env.mapping_dir, source_url=SOURCE_URL),
                max_submissions=1,
            )
            report = run_daemon(
                deps,
                cycle_key=lambda index: f"cycle-{index + 1}",
                now_iso=lambda: env.now_iso,
                interval_seconds=300.0,
                should_stop=state.should_stop,
            )
        finally:
            store.close()
    finally:
        signal.signal(signal.SIGINT, cast("Any", original))
    assert report.exit_reason == "shutdown_requested"
    assert report.shutdown_requested is True
    assert len(report.cycles) == 1


# ------------------------------------------------------------------ report


def test_report_seven_k_rollup_is_offline_json_with_denominators(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    env.seed_mapping()
    store = env.open_store()
    try:
        seeds = [
            FileSeed(
                shoko_file_id=10_000 + index,
                sonarr_series_id=index % 70 + 1,
                sonarr_file_id=index,
                sonarr_path=f"{SONARR_ROOT}/series{index % 70 + 1}/{10_000 + index}.mkv",
                sonarr_size=FILE_SIZE,
                cohort=Cohort.BACKLOG,
                iso=T0,
                state=FileState.VERIFIED if index <= VERIFIED_COUNT else FileState.MATCHED,
            )
            for index in range(1, FIRST_RUN_COUNT + 1)
        ]
        store.record_files(seeds)
        store.record_baseline([], complete=True, auto_apply_eligible=True, iso=T0)
        store.pause("mapping_churn", T0)
    finally:
        store.close()
    code = main(["report", str(env.config_path), "--json"])
    payload = captured_json(capsys)
    text = json.dumps(payload)
    assert code == EXIT_OK
    assert payload["state"]["present"] is True
    assert sum(payload["counts_by_state"].values()) == FIRST_RUN_COUNT
    assert payload["counts_verified"] == VERIFIED_COUNT
    assert payload["cohorts"]["backlog"] == FIRST_RUN_COUNT
    assert payload["coverage"]["files_tracked_in_state"] == FIRST_RUN_COUNT
    assert payload["coverage"]["coverage_percent"] == round(
        100.0 * VERIFIED_COUNT / FIRST_RUN_COUNT, 1
    )
    assert "based on last cycle inventory" in payload["coverage"]["note"]
    assert payload["safety"]["paused_reason"] == "mapping_churn"
    assert payload["safety"]["attestation"] == "absent"
    assert payload["levels"]["alive"] is False
    assert env.stubs.all_service_requests() == []
    assert DUMMY_SHOKO_KEY not in text
    assert DUMMY_SONARR_KEY not in text


def test_report_with_absent_state_is_informational(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(["report", str(env.config_path), "--json"])
    payload = captured_json(capsys)
    assert code == EXIT_OK
    assert payload["state"]["present"] is False
    assert payload["coverage"]["coverage_percent"] is None
    assert env.stubs.all_service_requests() == []


def test_health_reports_liveness_from_meta_only(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    env.seed_mapping()
    store = env.open_store()
    try:
        store.set_meta("last_cycle_alive", "1")
        store.set_meta("last_cycle_failed", "0")
        store.set_meta("last_cycle_end_iso", T0)
    finally:
        store.close()
    code = main(["health", str(env.config_path), "--json"])
    payload = captured_json(capsys)
    assert code == EXIT_OK
    assert payload["alive"] is True
    assert payload["cycle_ok"] is True
    assert payload["last_cycle_iso"] == T0
    assert payload["mapping_age_hours"] == 0.0
    assert env.stubs.all_service_requests() == []


# ------------------------------------------------------------------ operator commands


def _seed_sent_file(env: Env, file_id: int, *, verified: bool = False) -> None:
    from shoko_linker.linking import VerificationVerdict
    from shoko_linker.state import SendToken

    store = env.open_store()
    try:
        store.record_file(
            file_id,
            sonarr_series_id=1,
            sonarr_file_id=file_id,
            sonarr_path=f"{SONARR_ROOT}/series1/{file_id}.mkv",
            sonarr_size=FILE_SIZE,
            cohort=Cohort.NEW_ARRIVAL,
            iso=env.now_iso,
            state=FileState.PLANNED,
        )
        token = store.prepare_send(
            file_id,
            PLAN_FP,
            REQUEST_FP,
            targets_digest="c" * 64,
            structure_digest="d" * 64,
            mapping_revision="1",
            cycle_key="cycle-1",
            iso=env.now_iso,
        )
        assert isinstance(token, SendToken)
        store.mark_sent(token, iso=env.now_iso)
        store.record_send_outcome(token, SendOutcome.ACCEPTED_UNVERIFIED, iso=env.now_iso)
        if verified:
            store.record_verification(file_id, VerificationVerdict.VERIFIED, iso=env.now_iso)
    finally:
        store.close()


def test_resolve_send_release_frees_the_slot_and_refuses_verified(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    _seed_sent_file(env, 500)
    code = main(
        [
            "resolve-send",
            str(env.config_path),
            "--file-id",
            "500",
            "--fingerprint",
            REQUEST_FP,
            "--release",
            "--note",
            "operator verified XRefs after deadline",
            "--confirm",
            "--json",
        ]
    )
    payload = captured_json(capsys)
    assert code == EXIT_OK
    assert payload["result"]["resolution"] == "released_after_review"
    assert payload["result"]["state_after"] == "waiting"
    assert "FRESH request fingerprint" in payload["result"]["replan_note"]
    store = env.open_store()
    try:
        assert store.budget_view().pending_file_requests == 0
    finally:
        store.close()

    _seed_sent_file(env, 501, verified=True)
    code = main(
        [
            "resolve-send",
            str(env.config_path),
            "--file-id",
            "501",
            "--fingerprint",
            REQUEST_FP,
            "--block",
            "--note",
            "attempt on a verified row",
            "--confirm",
        ]
    )
    captured = capsys.readouterr()
    assert code == EXIT_STATE
    assert "state refused" in captured.err


def test_operator_commands_require_confirm(env: Env, capsys: pytest.CaptureFixture[str]) -> None:
    code = main(
        [
            "pause-writes",
            str(env.config_path),
            "--reason",
            "maintenance",
            "--note",
            "n",
        ]
    )
    out = capsys.readouterr().out
    assert code == EXIT_CONFIG_INVALID
    assert "--confirm" in out


def test_pause_writes_refuses_the_reserved_attestation_reason(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """P1-2: 'attestation_expired' is the gate's reserved internal reason — the operator pause
    command must refuse it so it never masquerades as the origin-gated attestation pause."""
    code = main(
        [
            "pause-writes",
            str(env.config_path),
            "--reason",
            "attestation_expired",
            "--note",
            "n",
            "--confirm",
        ]
    )
    out = capsys.readouterr().out
    assert code == EXIT_CONFIG_INVALID
    assert "reserved" in out
    store = env.open_store()
    try:
        assert store.budget_view().paused_reason is None  # nothing was paused
    finally:
        store.close()


def test_pause_writes_allows_an_operator_reason(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """P1-2: a distinct operator reason (not the reserved one) pauses writes normally."""
    code = main(
        [
            "pause-writes",
            str(env.config_path),
            "--reason",
            "disk_review",
            "--note",
            "ops",
            "--confirm",
            "--json",
        ]
    )
    payload = captured_json(capsys)
    assert code == EXIT_OK
    assert payload["result"]["paused_reason"] == "disk_review"


def test_pause_resume_and_clear_rebaseline_persist(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    base = str(env.config_path)
    code = main(
        [
            "pause-writes",
            base,
            "--reason",
            "lease_exhaustion",
            "--hours",
            "2",
            "--note",
            "ops",
            "--confirm",
            "--json",
        ]
    )
    payload = captured_json(capsys)
    assert code == EXIT_OK
    assert payload["result"]["paused_reason"] == "lease_exhaustion"
    store = env.open_store()
    try:
        assert store.budget_view().paused_reason == "lease_exhaustion"
        assert store.get_meta("paused_until_iso") is not None
    finally:
        store.close()

    code = main(["clear-requires-rebaseline", base, "--note", "rebased", "--confirm", "--json"])
    payload = captured_json(capsys)
    assert code == EXIT_OK
    assert payload["result"]["requires_rebaseline"] == "cleared"

    store = env.open_store()
    try:
        store.disable_writes("disk full (test)")
    finally:
        store.close()

    code = main(
        [
            "resolve-send",
            base,
            "--file-id",
            "500",
            "--fingerprint",
            REQUEST_FP,
            "--release",
            "--note",
            "n",
            "--confirm",
        ]
    )
    captured = capsys.readouterr()
    assert code == EXIT_STATE
    assert "writes are stopped" in captured.out

    code = main(["resume-writes", base, "--note", "volume fixed", "--confirm", "--json"])
    payload = captured_json(capsys)
    assert code == EXIT_OK
    assert payload["result"]["writes_disabled"] is False
    store = env.open_store()
    try:
        assert store.writes_disabled is False
        assert store.budget_view().paused_reason is None
        actions = json.loads(str(store.get_meta("operator_actions")))
        assert [entry["command"] for entry in actions][-1] == "resume-writes"
    finally:
        store.close()


def test_steal_apply_lock_refuses_live_lease_and_takes_expired(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    base = str(env.config_path)
    store = env.open_store()
    try:
        acquire_apply_lock(
            store, LOCK_VICTIM_PID, T0, lease_expires_iso="2026-12-31T00:00:00+00:00"
        )
    finally:
        store.close()
    code = main(["steal-apply-lock", base, "--note", "crash recovery", "--confirm"])
    captured = capsys.readouterr()
    assert code == EXIT_STATE
    assert "state refused" in captured.err
    assert str(LOCK_VICTIM_PID) in captured.err

    store = env.open_store()
    try:
        release_apply_lock(store, LOCK_VICTIM_PID, T0)
        acquire_apply_lock(store, LOCK_VICTIM_PID, T0, lease_expires_iso=T0)
    finally:
        store.close()
    env.now_iso = "2026-01-02T00:00:00+00:00"
    code = main(["steal-apply-lock", base, "--note", "expired lease", "--confirm", "--json"])
    payload = captured_json(capsys)
    assert code == EXIT_OK
    assert payload["result"]["stolen"] is True
    assert payload["result"]["holder_pid"] == os.getpid()
    # P2-1: the takeover is audit-recorded with the prior holder named.
    store = env.open_store()
    try:
        steals = json.loads(str(store.get_meta("apply_lock_steals")))
        assert any(entry["from_pid"] == LOCK_VICTIM_PID for entry in steals)
        # P2-2: the marker lease expires immediately — the next acquire takes ownership
        # without a stall window (daemon unblocked on its very next cycle).
        acquisition = acquire_or_steal_apply_lock(
            store, os.getpid() + 1, env.now_iso, lease_expires_iso=env.now_iso
        )
        assert acquisition.acquired is True
        assert acquisition.holder.owner_pid == os.getpid() + 1
    finally:
        store.close()


def _cli_hold_apply_lock(path_str: str) -> None:  # pragma: no cover - runs in a spawned child
    """Spawned child: take apply ownership with an ALREADY-EXPIRED meta lease.

    The meta lease alone would be stealable; only the OS advisory lock keeps a live owner
    fenced. The kernel frees the lock when the child exits."""
    from shoko_linker.state import StateStore

    store = StateStore(path_str)
    store.acquire_apply_ownership(os.getpid(), T0, T0)
    time.sleep(30)
    store.close()


def _wait_for_cli_holder(env: Env, owner_pid: int) -> None:
    deadline = time.monotonic() + 15.0
    while time.monotonic() < deadline:
        store = StateStore(env.state_path, budget_limits=env.limits())
        try:
            holder = store.lock_holder()
        finally:
            store.close()
        if holder is not None and holder.owner_pid == owner_pid:
            return
        time.sleep(0.2)
    raise AssertionError("the spawned holder never acquired the apply lease")


def test_steal_apply_lock_refused_while_live_holder_holds_the_os_lock(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F5: the operator steal requires a FREE OS lock — a live owner (kernel-held lock
    with an expired meta lease) is refused, never fenced out."""
    ctx = multiprocessing.get_context("spawn")
    process = ctx.Process(target=_cli_hold_apply_lock, args=(str(env.state_path),))
    process.start()
    assert process.pid is not None
    owner_pid = process.pid
    try:
        _wait_for_cli_holder(env, owner_pid)
        code = main(["steal-apply-lock", str(env.config_path), "--note", "takeover", "--confirm"])
        captured = capsys.readouterr()
        assert code == EXIT_STATE
        assert "state refused" in captured.err
        assert str(owner_pid) in captured.err
        # The live owner's meta record is untouched by the refused steal.
        store = env.open_store()
        try:
            holder = store.lock_holder()
            assert holder is not None
            assert holder.owner_pid == owner_pid
        finally:
            store.close()
    finally:
        process.terminate()
        process.join(timeout=20)


# ------------------------------------------------------------------ docs agreement


def test_readme_documents_every_cli_command_and_flags() -> None:
    readme = (Path(__file__).resolve().parents[1] / "README.md").read_text(encoding="utf-8")
    commands = set(cli._COMMAND_HANDLERS)
    assert {"validate", "once", "daemon", "report", "mappings", "health", "config"} <= commands
    for command in sorted(commands):
        assert command in readme, f"README must document: {command}"
    for subcommand in ("status", "refresh"):
        assert f"mappings {subcommand}" in readme
    for flag in ("--max-links", "--json", "--confirm", "--cycles", "--force", "--policy"):
        assert flag in readme
    for marker in ("exit codes", "state error", "apply-gate refusal"):
        assert marker in readme.lower()
