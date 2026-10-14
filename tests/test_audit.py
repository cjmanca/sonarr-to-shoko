"""M7-b gate: read-only existing-link audit — classification matrix, GET-only, rate limit.

All offline: mocked endpoints + fixed synthetic fixtures. Evidence: the request log
contains **only GETs** (zero POSTs), stays within ``max_requests``, and
``linked_consistent`` fires only on the exact identity set (existence alone never
marks a link correct).
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import hash_fixtures as hf
import httpx
import pytest
import yaml  # type: ignore[import-untyped]

from shoko_linker import cli
from shoko_linker.audit import AuditClass, run_audit
from shoko_linker.cli import EXIT_CONFIG_INVALID, EXIT_OK, main
from shoko_linker.config import Settings, load_settings
from shoko_linker.http import ApiEndpoint
from shoko_linker.mapping_store import MappingStore, load_active, refresh

SONARR_BASE = "http://sonarr:8989"
SHOKO_BASE = "http://shoko:8111"
SONARR_ROOT = "/data/media/anime"
SHOKO_ROOT = "/anime"
ANIME_ID = 1234
TVDB_MAIN = 267440
TVDB_AMBIG = 300000
AMBIG_A = 5000
AMBIG_B = 5001
EP1_ID = 5501
EP2_ID = 5502
WRONG_ID = 9999
FILE_SIZE = 1000
T0 = "2026-01-01T00:00:00+00:00"
OK_STATUS = 200
BUDGET_TWO = 2
LIMIT_100 = 100
MULTI_FILE_ID = 601
EXISTENCE_FILE_ID = 603
PARTIAL_COUNT = 2


def _mapping_xml() -> bytes:
    return (
        "<anime-list>"
        f"<anime anidbid='{ANIME_ID}' tvdbid='{TVDB_MAIN}' defaulttvdbseason='1'>"
        "<mapping-list><mapping anidbseason='1' tvdbseason='1'>;1-1;;2-2;</mapping></mapping-list>"
        "</anime>"
        f"<anime anidbid='{AMBIG_A}' tvdbid='{TVDB_AMBIG}' defaulttvdbseason='1' "
        "episodeoffset='0'/>"
        f"<anime anidbid='{AMBIG_B}' tvdbid='{TVDB_AMBIG}' defaulttvdbseason='1' "
        "episodeoffset='0'/>"
        "</anime-list>"
    ).encode()


def _path(shoko_id: int, series_id: int) -> tuple[str, str]:
    return (
        f"{SONARR_ROOT}/series{series_id}/{shoko_id}.mkv",
        f"{SHOKO_ROOT}/series{series_id}/{shoko_id}.mkv",
    )


@dataclass
class AuditStubs:
    """Shoko rows with pre-seeded links; Sonarr inventory mirrors them."""

    shoko_requests: list[httpx.Request] = field(default_factory=list)
    sonarr_requests: list[httpx.Request] = field(default_factory=list)
    file_status: int = 200

    def specs(self) -> list[tuple[int, int, int, list[tuple[int, int]]]]:
        # (series_id, file_id, shoko_id, linked identities [(anime, anidb_ep)])
        return [
            (1, 20, 600, [(ANIME_ID, WRONG_ID)]),  # wrong episode linked to ep-1 file
            (1, 21, 601, [(ANIME_ID, EP1_ID)]),  # multi-episode file: half the targets
            (1, 22, 602, [(ANIME_ID, EP1_ID)]),  # exact set
            (1, 23, 603, []),  # xref entry exists with NO episode ids (existence-only)
            (2, 30, 700, [(AMBIG_A, 6001)]),  # ambiguous mapping series
        ]

    def _xrefs(self, identities: list[tuple[int, int]]) -> list[dict[str, Any]]:
        if not identities:
            return [
                {
                    "SeriesID": {"ID": None, "AniDB": ANIME_ID, "TMDB": None},
                    "EpisodeIDs": [],
                }
            ]
        return [
            {
                "SeriesID": {"ID": None, "AniDB": anime, "TMDB": None},
                "EpisodeIDs": [{"ID": None, "AniDB": episode, "TMDB": None, "Source": "User"}],
            }
            for anime, episode in identities
        ]

    def file_row(self, spec: tuple[int, int, int, list[tuple[int, int]]]) -> dict[str, Any]:
        series_id, _file_id, shoko_id, identities = spec
        sonarr_side, shoko_side = _path(shoko_id, series_id)
        return {
            "ID": shoko_id,
            "Size": FILE_SIZE,
            "IsIgnored": False,
            "IsVariation": False,
            "Imported": "2024-01-01T00:00:00",
            "Hashes": hf.ed2k_hashes(shoko_id, FILE_SIZE, f"{shoko_id}.mkv"),
            "Locations": [
                {
                    "ID": shoko_id * 10,
                    "FileID": shoko_id,
                    "ImportFolderID": 1,
                    "RelativePath": f"series{series_id}/{shoko_id}.mkv",
                    "AbsolutePath": shoko_side,
                    "IsAccessible": True,
                }
            ],
            "SeriesIDs": self._xrefs(identities),
        }

    def shoko(self, request: httpx.Request) -> httpx.Response:
        self.shoko_requests.append(request)
        path = request.url.path
        if self.file_status != OK_STATUS and path.startswith("/api/v3/File"):
            return httpx.Response(self.file_status, json={"title": "down"}, request=request)
        if path == "/api/v3/ImportFolder":
            return httpx.Response(
                200,
                json=[{"ID": 1, "Name": "Sonarr", "Path": SHOKO_ROOT, "DropFolderType": "None"}],
                request=request,
            )
        if path.startswith("/api/v3/Series/AniDB/"):
            anime_id = int(path.split("/")[5])
            if anime_id == ANIME_ID:
                rows = [
                    {"ID": EP1_ID, "AnimeID": ANIME_ID, "Type": "Episode", "EpisodeNumber": 1},
                    {"ID": EP2_ID, "AnimeID": ANIME_ID, "Type": "Episode", "EpisodeNumber": 2},
                ]
            else:
                rows = [{"ID": 6001, "AnimeID": anime_id, "Type": "Episode", "EpisodeNumber": 1}]
            return httpx.Response(200, json={"Total": len(rows), "List": rows}, request=request)
        if path == "/api/v3/File":
            rows = [self.file_row(spec) for spec in self.specs()]
            return httpx.Response(200, json={"Total": len(rows), "List": rows}, request=request)
        return httpx.Response(404, json={}, request=request)

    def sonarr(self, request: httpx.Request) -> httpx.Response:
        self.sonarr_requests.append(request)
        path = request.url.path
        if path == "/api/v3/series":
            return httpx.Response(
                200,
                json=[
                    {
                        "id": 1,
                        "title": "Main",
                        "rootFolderPath": f"{SONARR_ROOT}/series1",
                        "tvdbId": TVDB_MAIN,
                    },
                    {
                        "id": 2,
                        "title": "Ambig",
                        "rootFolderPath": f"{SONARR_ROOT}/series2",
                        "tvdbId": TVDB_AMBIG,
                    },
                ],
                request=request,
            )
        if path == "/api/v3/rootfolder":
            return httpx.Response(
                200, json=[{"id": 1, "path": SONARR_ROOT, "accessible": True}], request=request
            )
        if path == "/api/v3/episode":
            series_id = int(request.url.params["seriesId"])
            specs = [spec for spec in self.specs() if spec[0] == series_id]
            episodes: list[dict[str, Any]] = []
            for _series_id, file_id, shoko_id, _identities in specs:
                episode_range = (1, 2) if shoko_id == MULTI_FILE_ID else (1,)
                for number in episode_range:
                    episodes.append(
                        {
                            "id": file_id * 100 + number,
                            "seriesId": series_id,
                            "seasonNumber": 1,
                            "episodeNumber": number,
                            "title": f"E{number}",
                            "tvdbId": number,
                            "episodeFileId": file_id,
                        }
                    )
            return httpx.Response(200, json=episodes, request=request)
        if path == "/api/v3/episodefile":
            series_id = int(request.url.params["seriesId"])
            return httpx.Response(
                200,
                json=[
                    {
                        "id": file_id,
                        "seriesId": series_id,
                        "path": _path(shoko_id, series_id)[0],
                        "size": FILE_SIZE,
                    }
                    for _series, file_id, shoko_id, _ids in self.specs()
                    if _series == series_id
                ],
                request=request,
            )
        return httpx.Response(404, json={}, request=request)


def _endpoint(handler: Any, base: str) -> ApiEndpoint:
    client = httpx.Client(
        base_url=base,
        transport=httpx.MockTransport(handler),
        follow_redirects=False,
        timeout=httpx.Timeout(5.0),
    )
    return ApiEndpoint(client, lambda: {})


def _document() -> dict[str, Any]:
    return {
        "sonarr": {
            "url": SONARR_BASE,
            "api_key_env": "SONARR_API_KEY",
            "allowed_roots": [SONARR_ROOT],
        },
        "shoko": {"url": SHOKO_BASE, "api_key_env": "SHOKO_API_KEY", "allowed_roots": [SHOKO_ROOT]},
        "paths": {"mappings": [{"sonarr": SONARR_ROOT, "shoko": SHOKO_ROOT}]},
        "mappings": {"source": "anime-lists", "refresh_hours": 168},
        "sync": {"apply_changes": False, "jitter_ratio": 0.0},
        "database": {"path": "/data/state.sqlite3"},
    }


@pytest.fixture(autouse=True)
def _no_root_logging_leak() -> Iterator[None]:
    yield
    import logging

    for handler in list(logging.getLogger().handlers):
        logging.getLogger().removeHandler(handler)


@pytest.fixture
def stubs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AuditStubs:
    environment = AuditStubs()
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(_document()), encoding="utf-8")
    monkeypatch.setattr(
        cli, "sonarr_endpoint", lambda settings: _endpoint(environment.sonarr, SONARR_BASE)
    )
    monkeypatch.setattr(
        cli, "shoko_endpoint", lambda settings: _endpoint(environment.shoko, SHOKO_BASE)
    )

    def _source(store: MappingStore, settings: Settings) -> ApiEndpoint:
        client = httpx.Client(
            base_url="https://raw.githubusercontent.com/",
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200,
                    headers={"ETag": '"a"'},
                    stream=httpx.ByteStream(_mapping_xml()),
                    request=request,
                )
            ),
            follow_redirects=False,
            timeout=httpx.Timeout(5.0),
        )
        return ApiEndpoint(client, lambda: {"Accept-Encoding": "identity"})

    monkeypatch.setattr(cli, "source_endpoint", _source)
    monkeypatch.setattr(
        cli, "open_mapping_store", lambda settings: MappingStore(tmp_path / "mappings")
    )
    monkeypatch.setattr(cli, "current_iso", lambda: T0)
    environment.config_path = config_path  # type: ignore[attr-defined]
    return environment


def _seed_mapping(tmp_path: Path) -> MappingStore:
    store = MappingStore(tmp_path / "mappings")
    client = httpx.Client(
        base_url="https://raw.githubusercontent.com/",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200,
                headers={"ETag": '"a"'},
                stream=httpx.ByteStream(_mapping_xml()),
                request=request,
            )
        ),
        follow_redirects=False,
        timeout=httpx.Timeout(5.0),
    )
    endpoint = ApiEndpoint(client, lambda: {"Accept-Encoding": "identity"})
    try:
        refresh(store, endpoint, now=datetime.fromisoformat(T0), refresh_hours=168.0, force=True)
    finally:
        endpoint.client.close()
    return store


def test_audit_classifies_existing_links_exactly(stubs: AuditStubs, tmp_path: Path) -> None:
    store = _seed_mapping(tmp_path)
    active = load_active(store)
    assert active is not None
    settings = load_settings(stubs.config_path)  # type: ignore[attr-defined]
    from shoko_linker.audit import collect_audit_inputs

    files, tvdb, problem = collect_audit_inputs(_endpoint(stubs.sonarr, SONARR_BASE), settings)
    assert problem is None
    report = run_audit(
        shoko=_endpoint(stubs.shoko, SHOKO_BASE),
        settings=settings,
        active=active,
        sonarr_files=files,
        series_tvdb=tvdb,
        max_requests=LIMIT_100,
    )
    found = {int(finding.shoko_file_id): finding.classification for finding in report.findings}
    assert found[600] is AuditClass.WRONG_FIRST_EPISODE  # ep-1 file linked to a foreign episode
    assert found[601] is AuditClass.PARTIAL_LINK  # multi-episode file, only ep-1 linked
    assert found[602] is AuditClass.LINKED_CONSISTENT  # exact identity set
    assert (
        found[603] is AuditClass.PARTIAL_LINK
    )  # existence-only (no episode ids) is NOT consistent
    assert found[700] is AuditClass.AMBIGUOUS_MATCH  # verified mapping answers ambiguously
    # GETs only, within budget
    assert {request.method for request in stubs.shoko_requests} == {"GET"}
    assert report.requests_made <= LIMIT_100
    assert report.transport_failed is False


def test_audit_exact_set_rule(stubs: AuditStubs, tmp_path: Path) -> None:
    store = _seed_mapping(tmp_path)
    active = load_active(store)
    assert active is not None
    settings = load_settings(stubs.config_path)  # type: ignore[attr-defined]
    report = _run(stubs, settings, active)
    consistent = [
        finding
        for finding in report.findings
        if finding.classification is AuditClass.LINKED_CONSISTENT
    ]
    for finding in consistent:
        assert set(finding.linked_identities) == set(finding.target_identities)
    existence_only = next(
        finding for finding in report.findings if finding.shoko_file_id == EXISTENCE_FILE_ID
    )
    assert existence_only.classification is not AuditClass.LINKED_CONSISTENT


def _run(stubs: AuditStubs, settings: Settings, active: Any) -> Any:
    from shoko_linker.audit import collect_audit_inputs

    files, tvdb, _problem = collect_audit_inputs(_endpoint(stubs.sonarr, SONARR_BASE), settings)
    return run_audit(
        shoko=_endpoint(stubs.shoko, SHOKO_BASE),
        settings=settings,
        active=active,
        sonarr_files=files,
        series_tvdb=tvdb,
        max_requests=LIMIT_100,
    )


def test_audit_series_filter(stubs: AuditStubs, tmp_path: Path) -> None:
    store = _seed_mapping(tmp_path)
    active = load_active(store)
    assert active is not None
    settings = load_settings(stubs.config_path)  # type: ignore[attr-defined]
    report = _run(stubs, settings, active)
    series_filtered = [
        finding
        for finding in report.findings
        if finding.classification is AuditClass.AMBIGUOUS_MATCH
    ]
    assert len(series_filtered) == 1


def test_audit_cli_is_get_only_and_informational(
    stubs: AuditStubs, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _ = _seed_mapping(tmp_path)
    config = str(stubs.config_path)  # type: ignore[attr-defined]
    code = main(["audit", config, "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert code == EXIT_OK
    assert payload["counts"]["linked_consistent"] == 1
    assert payload["counts"]["wrong_first_episode"] == 1
    assert payload["counts"]["partial_link"] == PARTIAL_COUNT
    assert payload["counts"]["ambiguous_match"] == 1
    assert {request.method for request in stubs.shoko_requests} == {"GET"}
    assert {request.method for request in stubs.sonarr_requests} == {"GET"}
    assert all(request.method != "POST" for request in stubs.shoko_requests)
    for secret in ("SHOKO_API_KEY", "SONARR_API_KEY"):
        assert secret not in json.dumps(payload)
    assert payload["requests_made"] <= payload["max_requests"]


def test_audit_cli_respects_limit_and_budget(
    stubs: AuditStubs, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _seed_mapping(tmp_path)
    config = str(stubs.config_path)  # type: ignore[attr-defined]
    code = main(["audit", config, "--limit", "1", "--max-requests", "2", "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert code == EXIT_OK
    assert payload["requests_made"] <= BUDGET_TWO
    assert payload["truncated"] is True
    assert len(payload["findings"]) <= 1


def test_audit_transport_failure_exits_4(
    stubs: AuditStubs, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _seed_mapping(tmp_path)
    stubs.file_status = 500
    config = str(stubs.config_path)  # type: ignore[attr-defined]
    code = main(["audit", config, "--json"])
    capsys.readouterr()
    from shoko_linker.cli import EXIT_RUNTIME

    assert code == EXIT_RUNTIME


def test_audit_without_mapping_generation_refuses(
    stubs: AuditStubs, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config = str(stubs.config_path)  # type: ignore[attr-defined]
    code = main(["audit", config])
    out = capsys.readouterr().out
    assert code == EXIT_CONFIG_INVALID
    assert "mapping generation" in out
