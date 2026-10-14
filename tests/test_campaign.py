"""M7-b gate: backlog campaign — one approval, bounded batches, quarantine, pending targets.

All offline: tmp stores, ``httpx.MockTransport`` stubs, fixed clock, and an
operator-built campaign-scope attestation lease. The request log is the evidence:
prepare makes zero POSTs; one approval drives three bounded apply
invocations (2+2+1) with progress persisted across store reopens; a changed
plan file pauses (``plan_drift``) with **zero POSTs**; a changed candidate is
quarantined and never auto-approved; a pending-target file applies only
after the exact-identity rule passes; deadline and ``max_submissions``
refuse; revoke releases queued rows.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import sys
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast

import hash_fixtures as hf
import httpx
import pytest
import yaml  # type: ignore[import-untyped]

from shoko_linker import campaign as campaign_mod
from shoko_linker import cli
from shoko_linker import daemon as daemon_mod
from shoko_linker.attestation import (
    SCOPE_DAEMON,
    build_record,
    campaign_lease_path,
    canonical_record,
    load_attestation,
    write_attestation,
)
from shoko_linker.campaign import (
    CampaignApplyReport,
    CampaignDigestMismatchError,
    CampaignError,
    CampaignPlanInvalidError,
    apply_campaign,
    approve_campaign,
    authorize_campaign_drain,
    campaign_drain_selection,
    campaign_report,
    campaign_revoke,
    campaign_send_progress,
    read_plan_file,
    revoke_campaign_drain,
    select_next_daemon_campaign,
)
from shoko_linker.cli import EXIT_APPLY_GATE_REFUSAL, EXIT_CONFIG_INVALID, EXIT_OK, main
from shoko_linker.config import load_settings
from shoko_linker.daemon import CycleMode, DaemonDeps, run_cycle
from shoko_linker.http import ApiEndpoint
from shoko_linker.linking import EpisodeIdResolver, LinkPlan
from shoko_linker.mapping_store import MappingStore, load_active
from shoko_linker.mapping_store import refresh as mapping_refresh
from shoko_linker.policies import RunPolicy
from shoko_linker.shoko import collect_shoko_discovery
from shoko_linker.state import (
    ATTESTATION_PAUSE_REASON,
    CAMPAIGN_DRAIN_FINGERPRINT_REASON,
    CAMPAIGN_STATUS_APPROVED,
    CAMPAIGN_STATUS_REVOKED,
    BaselineFile,
    BudgetRefusal,
    CampaignBinding,
    CampaignRecord,
    Cohort,
    FileState,
    StateStore,
    acquire_apply_lock,
    release_apply_lock,
)

SONARR_BASE = "http://sonarr:8989"
SHOKO_BASE = "http://shoko:8111"
SONARR_ROOT = "/data/media/anime"
SHOKO_ROOT = "/anime"
ANIME_ID = 1234
TVDB = 267440
ANIDB_EP = 5501
DIVERGENT_EP = 5502
SHOKO_EP = 777
DIVERGENT_SHOKO_EP = 778
FILE_SIZE = 1000
DIGEST_HEX = 64
ZERO_DIGEST = "0" * DIGEST_HEX
T0 = "2026-01-01T00:00:00+00:00"
COUNT_FIVE = 5
COUNT_TWO = 2
COUNT_THREE = 3
REMAINING_TWO = 2
BATCH_TWO = 2
BATCH_THREE = 3
BLOCKED_QUEUE_COUNT = 3
FOREIGN_PID = 111
LEASE_LONG_SECONDS = 7200.0
LEASE_SHORT_SECONDS = 10.0
LEASE_EXPIRY_OFFSET_SECONDS = 60.0
CAMPAIGN_APPLY_MAX = 20
COUNT_HUNDRED = 100
COUNT_TWELVE = 12
CAP_THREE = 3
ROLLING_WINDOW_SECONDS = 3600.0


@dataclass(frozen=True)
class SpecFile:
    series_id: int
    file_id: int
    shoko_id: int


def _sonarr_path(spec: SpecFile) -> str:
    return f"{SONARR_ROOT}/series{spec.series_id}/{spec.shoko_id}.mkv"


def _shoko_path(spec: SpecFile) -> str:
    return f"{SHOKO_ROOT}/series{spec.series_id}/{spec.shoko_id}.mkv"


@dataclass
class Stubs:
    specs: list[SpecFile] = field(default_factory=list)
    sizes: dict[int, int] = field(default_factory=dict)
    pending: set[int] = field(default_factory=set)
    divergent: bool = False
    verify_empty: set[int] = field(default_factory=set)
    blocked_count: int = 0
    posts: list[dict[str, Any]] = field(default_factory=list)
    posted_files: list[int] = field(default_factory=list)
    linked: dict[int, list[dict[str, Any]]] = field(default_factory=dict)
    ed2k_overrides: dict[int, str] = field(default_factory=dict)
    sonarr_episodefile_failures: set[int] = field(default_factory=set)
    shoko_page_failures: set[int] = field(default_factory=set)
    shoko_empty_pages: set[int] = field(default_factory=set)
    shoko_malformed_file_ids: set[int] = field(default_factory=set)
    shoko_total_overrides: dict[int, int] = field(default_factory=dict)
    shoko_file_list_calls: int = 0
    malformed_shoko_list_call: int | None = None
    sonarr_requests: list[httpx.Request] = field(default_factory=list)
    shoko_requests: list[httpx.Request] = field(default_factory=list)
    post_hook: Callable[[int], None] | None = None
    #: F19: when set, every Link POST responds with this status (e.g. 400 → REJECTED with
    #: zero server-side writes) instead of the accepted 200, and nothing is linked.
    post_status: int | None = None
    #: P2-1: fired on every Shoko GET with the request path — simulates a slow preflight
    #: network phase (authorization state flips mid-call, before the POST).
    get_hook: Callable[[str], None] | None = None

    def size(self, spec: SpecFile) -> int:
        return self.sizes.get(spec.shoko_id, FILE_SIZE)

    def xref(self, spec: SpecFile) -> dict[str, Any]:
        if self.divergent:
            return {
                "SeriesID": {"ID": None, "AniDB": ANIME_ID, "TMDB": None},
                "EpisodeIDs": [
                    {
                        "ID": DIVERGENT_SHOKO_EP,
                        "AniDB": DIVERGENT_EP,
                        "TMDB": None,
                        "Source": "User",
                    }
                ],
            }
        return {
            "SeriesID": {"ID": None, "AniDB": ANIME_ID, "TMDB": None},
            "EpisodeIDs": [{"ID": SHOKO_EP, "AniDB": ANIDB_EP, "TMDB": None, "Source": "User"}],
        }

    def file_row(self, spec: SpecFile) -> dict[str, Any]:
        hashes = hf.ed2k_hashes(spec.shoko_id, self.size(spec), f"{spec.shoko_id}.mkv")
        hashes["ED2K"] = self.ed2k_overrides.get(spec.shoko_id, hashes["ED2K"])
        return {
            "ID": spec.shoko_id,
            "Size": self.size(spec),
            "IsIgnored": False,
            "IsVariation": False,
            "Imported": "2024-01-01T00:00:00",
            "Hashes": hashes,
            "Locations": [
                {
                    "ID": spec.shoko_id * 10,
                    "FileID": spec.shoko_id,
                    "ImportFolderID": 1,
                    "RelativePath": f"series{spec.series_id}/{spec.shoko_id}.mkv",
                    "AbsolutePath": _shoko_path(spec),
                    "IsAccessible": True,
                }
            ],
            "SeriesIDs": self.linked.get(spec.shoko_id, []),
        }

    def sonarr(self, request: httpx.Request) -> httpx.Response:
        self.sonarr_requests.append(request)
        path = request.url.path
        if path == "/api/v3/series":
            series = {spec.series_id: spec for spec in self.specs}
            return httpx.Response(
                200,
                json=[
                    {
                        "id": spec.series_id,
                        "title": f"Series {spec.series_id}",
                        "rootFolderPath": f"{SONARR_ROOT}/series{spec.series_id}",
                        "tvdbId": TVDB,
                    }
                    for spec in series.values()
                ],
                request=request,
            )
        if path == "/api/v3/rootfolder":
            return httpx.Response(
                200, json=[{"id": 1, "path": SONARR_ROOT, "accessible": True}], request=request
            )
        if path == "/api/v3/episode":
            series_id = int(request.url.params["seriesId"])
            return httpx.Response(
                200,
                json=[
                    {
                        "id": spec.file_id * 100 + 1,
                        "seriesId": series_id,
                        "seasonNumber": 1,
                        "episodeNumber": 1,
                        "title": "S01E01",
                        "tvdbId": 1,
                        "episodeFileId": spec.file_id,
                    }
                    for spec in self.specs
                    if spec.series_id == series_id
                ],
                request=request,
            )
        if path == "/api/v3/episodefile":
            series_id = int(request.url.params["seriesId"])
            if series_id in self.sonarr_episodefile_failures:
                return httpx.Response(503, json={}, request=request)
            return httpx.Response(
                200,
                json=[
                    {
                        "id": spec.file_id,
                        "seriesId": series_id,
                        "path": _sonarr_path(spec),
                        "size": self.size(spec),
                    }
                    for spec in self.specs
                    if spec.series_id == series_id
                ],
                request=request,
            )
        return httpx.Response(404, json={}, request=request)

    def shoko(self, request: httpx.Request) -> httpx.Response:  # noqa: PLR0911
        self.shoko_requests.append(request)
        path = request.url.path
        if self.get_hook is not None and request.method == "GET":
            self.get_hook(path)
        if request.method == "POST" and path.endswith("/Link"):
            file_id = int(path.split("/")[4])
            self.posts.append(json.loads(request.content))
            self.posted_files.append(file_id)
            if self.post_status is not None:
                # F19: the server refuses the request (zero writes assumed).
                return httpx.Response(self.post_status, json={"title": "problem"}, request=request)
            spec = next(item for item in self.specs if item.shoko_id == file_id)
            if file_id not in self.verify_empty:
                self.linked[file_id] = [self.xref(spec)]
            if self.post_hook is not None:
                self.post_hook(file_id)
            return httpx.Response(200, json={}, request=request)
        if path == "/api/v3/ImportFolder":
            return httpx.Response(
                200,
                json=[{"ID": 1, "Name": "Sonarr", "Path": SHOKO_ROOT, "DropFolderType": "None"}],
                request=request,
            )
        if path == "/api/v3/Queue":
            return httpx.Response(
                200,
                json={
                    "WaitingCount": 1,
                    "BlockedCount": self.blocked_count,
                    "TotalCount": 4,
                    "ThreadCount": 1,
                },
                request=request,
            )
        if path.startswith("/api/v3/Series/AniDB/"):
            episode_id = DIVERGENT_EP if self.divergent else ANIDB_EP
            return httpx.Response(
                200,
                json={
                    "Total": 1,
                    "List": [
                        {
                            "ID": episode_id,
                            "AnimeID": ANIME_ID,
                            "Type": "Episode",
                            "EpisodeNumber": 1,
                        }
                    ],
                },
                request=request,
            )
        if path.startswith("/api/v3/Episode/AniDB/"):
            queried = int(path.split("/")[5])
            materialized = DIVERGENT_EP if self.divergent else ANIDB_EP
            if queried == materialized and not any(
                spec.shoko_id in self.pending for spec in self.specs
            ):
                internal = DIVERGENT_SHOKO_EP if self.divergent else SHOKO_EP
                return httpx.Response(
                    200,
                    json={
                        "IDs": {"ID": internal, "AniDB": queried, "TMDB": None},
                        "Type": "Episode",
                    },
                    request=request,
                )
            return httpx.Response(404, json={"title": "not found"}, request=request)
        if path == "/api/v3/File":
            return self._file_listing(request)
        if path.startswith("/api/v3/File/"):
            file_id = int(path.split("/")[4])
            spec = next(item for item in self.specs if item.shoko_id == file_id)
            return httpx.Response(200, json=self.file_row(spec), request=request)
        return httpx.Response(404, json={}, request=request)

    def _file_listing(self, request: httpx.Request) -> httpx.Response:
        self.shoko_file_list_calls += 1
        page = int(request.url.params.get("page", "1"))
        if page in self.shoko_page_failures:
            return httpx.Response(503, json={}, request=request)
        rows = [self.file_row(spec) for spec in sorted(self.specs, key=lambda item: item.shoko_id)]
        if self.malformed_shoko_list_call == self.shoko_file_list_calls and rows:
            rows[0].pop("Size", None)
        page_size = int(request.url.params.get("pageSize", str(len(rows) or 1)))
        page_rows = rows[(page - 1) * page_size : page * page_size]
        for row in page_rows:
            if row["ID"] in self.shoko_malformed_file_ids:
                row.pop("Size", None)
        if page in self.shoko_empty_pages:
            page_rows = []
        total = self.shoko_total_overrides.get(page, len(rows))
        return httpx.Response(200, json={"Total": total, "List": page_rows}, request=request)


def _source_xml() -> bytes:
    return (
        "<anime-list>"
        f"<anime anidbid='{ANIME_ID}' tvdbid='{TVDB}' defaulttvdbseason='1'>"
        "<mapping-list><mapping anidbseason='1' tvdbseason='1'>;1-1;</mapping></mapping-list>"
        "</anime></anime-list>"
    ).encode()


def _document(lease: Path | None) -> dict[str, Any]:
    return {
        "sonarr": {
            "url": SONARR_BASE,
            "api_key_env": "SONARR_API_KEY",
            "allowed_roots": [SONARR_ROOT],
        },
        "shoko": {"url": SHOKO_BASE, "api_key_env": "SHOKO_API_KEY", "allowed_roots": [SHOKO_ROOT]},
        "paths": {"mappings": [{"sonarr": SONARR_ROOT, "shoko": SHOKO_ROOT}]},
        "mappings": {"source": "anime-lists", "refresh_hours": 168},
        "sync": {
            "apply_changes": True,
            "jitter_ratio": 0.0,
            "max_new_per_cycle": 20,
            "max_pending_episode_jobs": 10,
        },
        "database": {"path": "/data/state.sqlite3"},
        "safety": {
            "attestation_file": "/data/lease.yaml" if lease is not None else None,
            "attestation_validity_hours": 168,
            "attestation_warn_seconds": 3600,
        },
        "campaign": {"max_campaign_files": 7000, "max_batch_size": 10},
    }


@dataclass
class Env:
    tmp_path: Path
    stubs: Stubs
    config_path: Path
    lease_path: Path
    now_iso: str = T0

    def open_store(self) -> StateStore:
        return StateStore(
            self.tmp_path / "state.sqlite3",
            budget_limits=cli.budget_limits_from_settings(load_settings(self.config_path)),
        )

    def seed(self, specs: list[SpecFile]) -> None:
        self.stubs.specs = specs
        store = self.open_store()
        try:
            store.record_baseline(
                [
                    BaselineFile(
                        sonarr_series_id=spec.series_id,
                        sonarr_file_id=spec.file_id,
                        path=_sonarr_path(spec),
                        size=FILE_SIZE,
                    )
                    for spec in specs
                ],
                complete=True,
                auto_apply_eligible=True,
                iso=T0,
            )
        finally:
            store.close()

    def write_campaign_lease(
        self, campaign_id: str, *, hours: float = 24.0, expired: bool = False
    ) -> None:
        inspected = self.now_iso if not expired else _iso_plus(self.now_iso, -7200.0)
        expiry_hours = hours if not expired else 1.0  # expires = inspected+1h = one hour ago
        expires_iso = (
            datetime.fromisoformat(inspected) + timedelta(hours=expiry_hours)
        ).isoformat()
        record = build_record(
            endpoint=SHOKO_BASE,
            version="5.3.3",
            mounts=["/anime read-only (host-inspected)"],
            settings_flags={
                "FileQualityFilterEnabled": False,
                "RenameOnImport": False,
                "MoveOnImport": False,
                "MyListAddFiles": False,
            },
            inspected_iso=inspected,
            expires_iso=expires_iso,
            scope=f"campaign:{campaign_id}",
            operator_note="campaign lease",
        )
        write_attestation(self.lease_path, record, overwrite=True)


def _iso_plus(iso: str, seconds: float) -> str:
    return (datetime.fromisoformat(iso) + timedelta(seconds=seconds)).isoformat()


@pytest.fixture(autouse=True)
def _no_root_logging_leak() -> Iterator[None]:
    yield
    import logging

    for handler in list(logging.getLogger().handlers):
        logging.getLogger().removeHandler(handler)


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Env:
    lease = tmp_path / "lease.yaml"
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(_document(lease)), encoding="utf-8")
    environment = Env(tmp_path=tmp_path, stubs=Stubs(), config_path=config_path, lease_path=lease)
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    monkeypatch.setattr(
        cli,
        "open_state_store",
        lambda settings: StateStore(
            environment.tmp_path / "state.sqlite3",
            budget_limits=cli.budget_limits_from_settings(settings),
        ),
    )
    monkeypatch.setattr(cli, "state_path", lambda settings: environment.tmp_path / "state.sqlite3")
    monkeypatch.setattr(
        cli,
        "attestation_target_path",
        lambda settings, scope=SCOPE_DAEMON, *, for_write=False: environment.lease_path,
    )
    monkeypatch.setattr(cli, "current_iso", lambda: environment.now_iso)
    monkeypatch.setattr(cli, "sleep", lambda seconds: None)

    def _endpoint(handler: Any, base: str) -> ApiEndpoint:
        client = httpx.Client(
            base_url=base,
            transport=httpx.MockTransport(handler),
            follow_redirects=False,
            timeout=httpx.Timeout(5.0),
        )
        return ApiEndpoint(client, lambda: {})

    monkeypatch.setattr(
        cli, "sonarr_endpoint", lambda settings: _endpoint(environment.stubs.sonarr, SONARR_BASE)
    )
    monkeypatch.setattr(
        cli, "shoko_endpoint", lambda settings: _endpoint(environment.stubs.shoko, SHOKO_BASE)
    )

    def _source(store: Any, settings: Any) -> ApiEndpoint:
        client = httpx.Client(
            base_url="https://raw.githubusercontent.com/",
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, headers={"ETag": '"s"'}, content=_source_xml(), request=request
                )
            ),
            follow_redirects=False,
            timeout=httpx.Timeout(5.0),
        )
        return ApiEndpoint(client, lambda: {"Accept-Encoding": "identity"})

    monkeypatch.setattr(cli, "source_endpoint", _source)
    monkeypatch.setattr(
        cli, "open_mapping_store", lambda settings: MappingStore(environment.tmp_path / "mappings")
    )
    return environment


def seed_mapping(env: Env) -> None:
    store = MappingStore(env.tmp_path / "mappings")
    settings = load_settings(env.config_path)
    endpoint = cli.source_endpoint(store, settings)
    try:
        mapping_refresh(
            store, endpoint, now=datetime.fromisoformat(T0), refresh_hours=168.0, force=True
        )
    finally:
        endpoint.client.close()


def specs(count: int) -> list[SpecFile]:
    return [
        SpecFile(series_id=index + 1, file_id=10 + index, shoko_id=500 + index)
        for index in range(count)
    ]


def prepare(
    env: Env,
    capsys: pytest.CaptureFixture[str],
    *,
    deadline_hours: str = "168",
    limit: str | None = None,
) -> dict[str, Any]:
    seed_mapping(env)
    args = [
        "campaign",
        "prepare",
        str(env.config_path),
        "--out",
        str(env.tmp_path / "plan.json"),
        "--deadline-hours",
        deadline_hours,
        "--json",
    ]
    if limit is not None:
        args += ["--limit", limit]
    assert main(args) == EXIT_OK
    payload = cast("dict[str, Any]", json.loads(capsys.readouterr().out))
    env.write_campaign_lease(payload["campaign_id"])
    return payload


def approve(env: Env, capsys: pytest.CaptureFixture[str], digest: str) -> dict[str, Any]:
    code = main(
        [
            "campaign",
            "approve",
            str(env.config_path),
            "--plan",
            str(env.tmp_path / "plan.json"),
            "--digest",
            digest,
            "--json",
        ]
    )
    assert code == EXIT_OK
    return cast("dict[str, Any]", json.loads(capsys.readouterr().out))


def apply(
    env: Env, capsys: pytest.CaptureFixture[str], *, batch: str | None = None
) -> dict[str, Any]:
    args = [
        "campaign",
        "apply",
        str(env.config_path),
        "--plan",
        str(env.tmp_path / "plan.json"),
        "--json",
    ]
    if batch is not None:
        args += ["--batch-size", batch]
    code = main(args)
    captured = capsys.readouterr()
    if code == EXIT_OK:
        return {"exit": code, "payload": json.loads(captured.out)}
    return {"exit": code, "payload": captured.err + captured.out}


def _force_shoko_page_size_one(monkeypatch: pytest.MonkeyPatch) -> None:
    collect = collect_shoko_discovery
    monkeypatch.setattr(
        daemon_mod,
        "collect_shoko_discovery",
        lambda endpoint: collect(endpoint, page_size=1),
    )


def _configure_incomplete_campaign_snapshot(
    env: Env,
    monkeypatch: pytest.MonkeyPatch,
    scenario: str,
    candidate: SpecFile,
) -> None:
    if scenario in {"short_shoko_page", "changed_shoko_total", "later_shoko_page_failure"}:
        _force_shoko_page_size_one(monkeypatch)
    if scenario == "short_shoko_page":
        env.stubs.shoko_empty_pages.add(2)
    elif scenario == "changed_shoko_total":
        env.stubs.shoko_total_overrides.update({1: COUNT_TWO, 2: COUNT_THREE})
    elif scenario == "malformed_shoko_file":
        env.stubs.shoko_malformed_file_ids.add(candidate.shoko_id)
    elif scenario == "later_shoko_page_failure":
        env.stubs.shoko_page_failures.add(2)
    elif scenario == "incomplete_sonarr_inventory":
        env.stubs.sonarr_episodefile_failures.add(candidate.series_id)
    else:
        raise AssertionError(f"unknown incomplete snapshot scenario: {scenario}")


def _clear_incomplete_campaign_snapshot(env: Env) -> None:
    env.stubs.sonarr_episodefile_failures.clear()
    env.stubs.shoko_page_failures.clear()
    env.stubs.shoko_empty_pages.clear()
    env.stubs.shoko_malformed_file_ids.clear()
    env.stubs.shoko_total_overrides.clear()


# ------------------------------------------------------------------ main flow


def test_prepare_approve_apply_three_batches_with_one_approval(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    env.seed(specs(COUNT_FIVE))
    prepared = prepare(env, capsys)
    assert prepared["files"] == COUNT_FIVE
    assert len(prepared["approval_digest"]) == DIGEST_HEX
    plan_document = json.loads((env.tmp_path / "plan.json").read_bytes())
    assert plan_document["schema"] == "shoko-linker-campaign-plan/2"
    assert all(
        entry["shoko_ed2k"] == hf.hex32(entry["shoko_file_id"]).lower()
        for entry in plan_document["files"]
    )
    assert env.stubs.posts == []

    approved = approve(env, capsys, prepared["approval_digest"])
    assert approved["files_approved"] == COUNT_FIVE

    first = apply(env, capsys, batch="2")
    assert first["exit"] == EXIT_OK
    assert first["payload"]["submitted"] == BATCH_TWO
    assert first["payload"]["remaining"] == COUNT_THREE
    second = apply(env, capsys, batch="2")
    assert second["exit"] == EXIT_OK
    assert second["payload"]["submitted"] == BATCH_TWO
    assert second["payload"]["remaining"] == 1
    third = apply(env, capsys, batch="2")
    assert third["exit"] == EXIT_OK
    assert third["payload"]["submitted"] == 1
    assert third["payload"]["remaining"] == 0

    assert len(env.stubs.posts) == COUNT_FIVE
    plan_document = json.loads((env.tmp_path / "plan.json").read_bytes())
    approved_ep_ids = {ep for item in plan_document["files"] for ep in item["shoko_episode_ids"]}
    for body in env.stubs.posts:
        assert set(body["EpisodeIDs"]) <= approved_ep_ids

    store = env.open_store()
    try:
        campaign = store.get_campaign(prepared["campaign_id"])
        assert campaign is not None
        assert campaign.total_sent == COUNT_FIVE
        assert campaign.last_batch_iso is not None
        assert campaign.status == "approved"
        verified = [
            record
            for record in store.campaign_files(prepared["campaign_id"])
            if record.state is FileState.VERIFIED
        ]
        assert len(verified) == COUNT_FIVE
        assert campaign.digest == prepared["approval_digest"]
    finally:
        store.close()


def test_approve_rejects_digest_mismatch_with_both_digests(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    wrong = "f" * 64
    code = main(
        [
            "campaign",
            "approve",
            str(env.config_path),
            "--plan",
            str(env.tmp_path / "plan.json"),
            "--digest",
            wrong,
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == EXIT_CONFIG_INVALID
    assert prepared["approval_digest"] in captured.err
    assert wrong in captured.err
    store = env.open_store()
    try:
        assert store.get_campaign(prepared["campaign_id"]) is None
    finally:
        store.close()
    assert env.stubs.posts == []


def test_plan_file_edited_after_approval_pauses_plan_drift_zero_posts(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    plan_path = env.tmp_path / "plan.json"
    tampered = plan_path.read_bytes().replace(b'"size":1000', b'"size":1001', 1)
    assert tampered != plan_path.read_bytes()
    plan_path.write_bytes(tampered)
    result = apply(env, capsys)
    assert result["exit"] == EXIT_APPLY_GATE_REFUSAL
    assert "plan_drift" in str(result["payload"])
    assert env.stubs.posts == []
    store = env.open_store()
    try:
        campaign = store.get_campaign(prepared["campaign_id"])
        assert campaign is not None
        assert campaign.status == "paused"
        assert campaign.status_reason == "plan_drift"
    finally:
        store.close()


def test_candidate_size_change_after_approval_is_quarantined_not_replaced(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    specs_list = specs(COUNT_FIVE)
    env.seed(specs_list)
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    victim = specs_list[2]
    env.stubs.sizes[victim.shoko_id] = FILE_SIZE + 1
    result = apply(env, capsys)
    assert result["exit"] == EXIT_OK
    payload = result["payload"]
    assert payload["submitted"] == COUNT_FIVE - 1
    assert payload["quarantined"] == 1
    assert len(env.stubs.posts) == COUNT_FIVE - 1
    store = env.open_store()
    try:
        progress = store.campaign_progress(prepared["campaign_id"])
        assert progress["quarantined"] == 1
        record = store.get_file(victim.shoko_id)
        assert record is not None
        assert record.state in (FileState.BLOCKED_SAFETY, FileState.WAITING)
        assert record.state_detail.startswith("campaign_quarantine:")
    finally:
        store.close()
    # A re-apply never re-approves the quarantined file: it is not queueable.
    result2 = apply(env, capsys)
    assert result2["exit"] == EXIT_OK
    assert result2["payload"]["submitted"] == 0
    assert any("not_queueable" in reason for reason in result2["payload"]["reasons"])


def test_approved_campaign_quarantines_changed_ed2k_before_post(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    specs_list = specs(COUNT_TWO)
    env.seed(specs_list)
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    changed = specs_list[0]
    env.stubs.ed2k_overrides[changed.shoko_id] = hf.hex32(changed.shoko_id + 1)

    result = apply(env, capsys)

    assert result["exit"] == EXIT_OK
    assert result["payload"]["processed"] == COUNT_TWO
    assert result["payload"]["quarantined"] == 1
    assert any("source_hash_identity_drift" in reason for reason in result["payload"]["reasons"])
    assert any(
        reason.startswith(f"campaign_quarantine:{changed.shoko_id}:")
        for reason in result["payload"]["reasons"]
    )
    assert changed.shoko_id not in env.stubs.posted_files
    assert len(env.stubs.posts) == COUNT_TWO - 1
    store = env.open_store()
    try:
        record = store.get_file(changed.shoko_id)
        assert record is not None
        assert record.state is FileState.BLOCKED_SAFETY
        assert record.state_detail.startswith("campaign_quarantine:")
        assert "source_hash_identity_drift" in record.state_detail
        assert "ED2K identity drift" in record.state_detail
        assert store.campaign_progress(prepared["campaign_id"])["quarantined"] == 1
    finally:
        store.close()


def test_repeated_ed2k_campaign_quarantine_is_deferred_and_counts_as_zero_progress(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    specs_list = specs(COUNT_TWO)
    env.seed(specs_list)
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    changed = specs_list[0]
    env.stubs.ed2k_overrides[changed.shoko_id] = hf.hex32(changed.shoko_id + 1)

    first = apply(env, capsys)
    assert first["exit"] == EXIT_OK
    assert first["payload"]["quarantined"] == 1
    assert changed.shoko_id not in env.stubs.posted_files
    posts_after_first_apply = len(env.stubs.posts)
    assert posts_after_first_apply == COUNT_TWO - 1

    repeated = apply(env, capsys)
    assert repeated["exit"] == EXIT_OK
    assert repeated["payload"]["quarantined"] == 0
    assert repeated["payload"]["progress"] == 0
    assert repeated["payload"]["deferred"] >= 1
    assert any("already_quarantined" in reason for reason in repeated["payload"]["reasons"])
    assert changed.shoko_id not in env.stubs.posted_files
    assert len(env.stubs.posts) == posts_after_first_apply

    store = env.open_store()
    try:
        record = store.get_file(changed.shoko_id)
        assert record is not None
        assert record.state is FileState.BLOCKED_SAFETY
        assert record.state_detail.startswith("campaign_quarantine:")
        assert store.campaign_progress(prepared["campaign_id"])["quarantined"] == 1

        _authorize_drain(env, store, prepared["campaign_id"])
        for index, key in enumerate(ZERO_PROGRESS_CYCLE_KEYS):
            cycle = _daemon_cycle(env, store, prepared["campaign_id"], key)
            assert cycle.campaign is not None
            assert cycle.campaign.quarantined == 0
            assert cycle.campaign.progress == 0
            assert len(env.stubs.posts) == posts_after_first_apply
            if index < len(ZERO_PROGRESS_CYCLE_KEYS) - 1:
                assert store.get_meta(f"campaign_zero_progress:{prepared['campaign_id']}") == str(
                    index + 1
                )
        campaign = store.get_campaign(prepared["campaign_id"])
        assert campaign is not None
        assert campaign.status == "paused"
        assert campaign.status_reason == "no_queueable_candidates"
        quarantined = store.get_file(changed.shoko_id)
        assert quarantined is not None
        assert quarantined.state is FileState.BLOCKED_SAFETY
        assert store.campaign_progress(prepared["campaign_id"])["quarantined"] == 1
    finally:
        store.close()


def test_daemon_first_ed2k_quarantine_counts_fresh_progress_once_then_breaks_on_repeats(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    _set_campaign(env, daemon_auto_apply=True)
    specs_list = specs(COUNT_TWO)
    env.seed(specs_list)
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    changed = specs_list[1]

    store = env.open_store()
    try:
        canary = _canary(env, store, prepared["campaign_id"])
        assert canary.verified == COUNT_ONE
        _authorize_drain(env, store, prepared["campaign_id"])
        canary_posts = len(env.stubs.posts)
        assert env.stubs.posted_files == [specs_list[0].shoko_id]

        store.advance(changed.shoko_id, FileState.WAITING, "rejected send", iso=env.now_iso)
        env.stubs.ed2k_overrides[changed.shoko_id] = hf.hex32(changed.shoko_id + 1)
        dry_run = run_cycle(
            _campaign_deps(env, store, prepared["campaign_id"]),
            now_iso=env.now_iso,
            mode=CycleMode.DRY_RUN,
            policy=RunPolicy.BACKLOG,
            cycle_key="dry-run-first-ed2k-quarantine",
        )
        assert dry_run.campaign is None
        dry_run_quarantine = store.get_file(changed.shoko_id)
        assert dry_run_quarantine is not None
        assert dry_run_quarantine.state is FileState.BLOCKED_SAFETY
        store.close()
        store = env.open_store()

        first = _daemon_cycle(env, store, prepared["campaign_id"], "daemon-first-ed2k-quarantine")

        assert first.campaign is not None
        assert first.campaign.quarantined == COUNT_ONE
        assert first.campaign.progress == COUNT_ONE
        assert first.campaign.submitted == 0
        assert changed.shoko_id not in env.stubs.posted_files
        assert len(env.stubs.posts) == canary_posts

        quarantined = store.get_file(changed.shoko_id)
        assert quarantined is not None
        assert quarantined.state is FileState.BLOCKED_SAFETY
        assert quarantined.state_detail.startswith("campaign_quarantine:")

        for index, key in enumerate(ZERO_PROGRESS_CYCLE_KEYS):
            repeated = _daemon_cycle(env, store, prepared["campaign_id"], key)
            assert repeated.campaign is not None
            assert repeated.campaign.quarantined == 0
            assert repeated.campaign.progress == 0
            assert changed.shoko_id not in env.stubs.posted_files
            assert len(env.stubs.posts) == canary_posts
            if index < len(ZERO_PROGRESS_CYCLE_KEYS) - 1:
                assert store.get_meta(f"campaign_zero_progress:{prepared['campaign_id']}") == str(
                    index + 1
                )

        campaign = store.get_campaign(prepared["campaign_id"])
        assert campaign is not None
        assert campaign.status == "paused"
        assert campaign.status_reason == "no_queueable_candidates"
    finally:
        store.close()


def test_pending_campaign_candidate_quarantines_ed2k_drift_before_target_resolution(
    env: Env,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    specs_list = specs(COUNT_TWO)
    pending = specs_list[1]
    env.stubs.pending.add(pending.shoko_id)
    env.seed(specs_list)
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    plan, _digest = read_plan_file(env.tmp_path / "plan.json")
    entry = next(item for item in plan.files if item.shoko_file_id == pending.shoko_id)
    assert entry.shoko_episode_ids == ()
    assert entry.shoko_ed2k == hf.hex32(pending.shoko_id).lower()

    store = env.open_store()
    deps: DaemonDeps | None = None
    try:
        deps = _campaign_deps(env, store, plan.campaign_id)
        assert deps is not None
        sweep = run_cycle(
            deps,
            now_iso=env.now_iso,
            mode=CycleMode.DRY_RUN,
            policy=RunPolicy.BACKLOG,
            cycle_key="pending-ed2k-probe",
        )
        candidate = sweep.plans[pending.shoko_id]
        assert candidate.status.value == "pending_targets"
        changed_candidate = replace(candidate, shoko_ed2k=hf.hex32(pending.shoko_id + 1))
        record = store.get_file(pending.shoko_id)
        assert record is not None
        campaign = store.get_campaign(plan.campaign_id)
        assert campaign is not None
        active = load_active(deps.mapping_store, campaign_mod._load_overrides(deps))
        assert active is not None

        pending_branch_calls: list[int] = []

        def pending_branch(*_args: Any) -> LinkPlan:
            pending_branch_calls.append(pending.shoko_id)
            return changed_candidate

        monkeypatch.setattr(
            campaign_mod,
            "_pending_plan",
            pending_branch,
        )
        counters = campaign_mod._CampaignCounters()
        reasons: list[str] = []
        context = campaign_mod._ApplyContext(
            deps=deps,
            settings=load_settings(env.config_path),
            store=store,
            campaign=campaign,
            sweep=sweep,
            active=active,
            resolver=EpisodeIdResolver(deps.shoko, clock=deps.monotonic),
            queue=None,
            now_iso=env.now_iso,
            cycle_key="pending-ed2k-probe",
        )

        result = campaign_mod._candidate_plan(
            context,
            entry,
            record,
            changed_candidate,
            counters,
            reasons,
        )

        assert result is None
        assert pending_branch_calls == []
        assert counters.quarantined == 1
        assert any(f"campaign_quarantine:{pending.shoko_id}:ed2k_drift" in r for r in reasons)
        quarantined = store.get_file(pending.shoko_id)
        assert quarantined is not None
        assert quarantined.state is FileState.BLOCKED_SAFETY
    finally:
        if deps is not None:
            deps.sonarr.client.close()
            deps.shoko.client.close()
            deps.mapping_source.client.close()
        store.close()


def test_legacy_campaign_approval_requires_reprepare_and_reapproval(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    plan_path = env.tmp_path / "plan.json"
    document = json.loads(plan_path.read_bytes())
    document["schema"] = "shoko-linker-campaign-plan/1"
    for entry in document["files"]:
        entry.pop("shoko_ed2k", None)
    raw = (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()
    plan_path.write_bytes(raw)
    legacy_digest = hashlib.sha256(raw).hexdigest()
    bindings = tuple(
        CampaignBinding(
            shoko_file_id=entry["shoko_file_id"],
            plan_fingerprint=entry["plan_fingerprint"],
            request_fingerprint=entry["request_fingerprint"],
        )
        for entry in document["files"]
    )
    store = env.open_store()
    try:
        store.create_campaign(
            document["campaign_id"],
            legacy_digest,
            max_submissions=document["totals"]["max_submissions"],
            deadline_iso=document["deadline_iso"],
            attestation_scope=f"campaign:{document['campaign_id']}",
            approval_iso=T0,
            iso=T0,
            bindings=bindings,
            plan_path=str(plan_path.resolve()),
        )
        store.bind_campaign_files(document["campaign_id"], T0)
    finally:
        store.close()
    env.write_campaign_lease(prepared["campaign_id"])

    result = apply(env, capsys)

    assert result["exit"] == EXIT_CONFIG_INVALID
    assert "re-prepare and re-approve" in str(result["payload"])
    assert env.stubs.posts == []


@pytest.mark.parametrize(
    ("scenario", "reason"),
    [
        ("short_shoko_page", "shoko_snapshot_incomplete:pages_exhausted"),
        ("changed_shoko_total", "shoko_snapshot_incomplete:total_changed"),
        ("malformed_shoko_file", "shoko_snapshot_incomplete:malformed_file"),
        ("later_shoko_page_failure", "shoko_snapshot_incomplete:transport_failure"),
        ("incomplete_sonarr_inventory", "sonarr_inventory_incomplete:transport_failure"),
    ],
)
def test_campaign_apply_defers_incomplete_reconciliation_then_recovers(
    env: Env,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    scenario: str,
    reason: str,
) -> None:
    specs_list = specs(COUNT_TWO)
    candidate = specs_list[1]
    env.seed(specs_list)
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    first = apply(env, capsys, batch="1")
    assert first["exit"] == EXIT_OK
    assert first["payload"]["submitted"] == COUNT_ONE
    posts_before_incomplete = len(env.stubs.posts)

    store = env.open_store()
    try:
        campaign_before = store.get_campaign(prepared["campaign_id"])
        candidate_before = store.get_file(candidate.shoko_id)
        assert campaign_before is not None
        assert candidate_before is not None
        assert candidate_before.state is FileState.PLANNED
        progress_before = campaign_send_progress(store, prepared["campaign_id"])
        quarantines_before = store.campaign_progress(prepared["campaign_id"])["quarantined"]
    finally:
        store.close()

    _configure_incomplete_campaign_snapshot(env, monkeypatch, scenario, candidate)
    deferred = apply(env, capsys)

    assert deferred["exit"] == EXIT_APPLY_GATE_REFUSAL
    assert "reconciliation incomplete" in str(deferred["payload"])
    assert reason in str(deferred["payload"])
    assert len(env.stubs.posts) == posts_before_incomplete

    store = env.open_store()
    try:
        campaign_after = store.get_campaign(prepared["campaign_id"])
        candidate_after = store.get_file(candidate.shoko_id)
        assert campaign_after is not None
        assert candidate_after is not None
        assert campaign_after.status == campaign_before.status == "approved"
        assert campaign_after.digest == campaign_before.digest
        assert campaign_after.bindings == campaign_before.bindings
        assert candidate_after.state is candidate_before.state
        assert candidate_after.campaign_id == candidate_before.campaign_id
        assert campaign_send_progress(store, prepared["campaign_id"]) == progress_before
        assert store.campaign_progress(prepared["campaign_id"])["quarantined"] == quarantines_before
    finally:
        store.close()

    _clear_incomplete_campaign_snapshot(env)
    recovered = apply(env, capsys)

    assert recovered["exit"] == EXIT_OK
    assert recovered["payload"]["submitted"] == COUNT_ONE
    assert len(env.stubs.posts) == posts_before_incomplete + COUNT_ONE
    assert candidate.shoko_id in env.stubs.posted_files


def test_daemon_outer_complete_inner_incomplete_sweep_defers_campaign_drain(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    _set_campaign(env, daemon_auto_apply=True)
    specs_list = specs(COUNT_TWO)
    candidate = specs_list[1]
    env.seed(specs_list)
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])

    store = env.open_store()
    try:
        canary = _canary(env, store, prepared["campaign_id"])
        assert canary.verified == COUNT_ONE
        _authorize_drain(env, store, prepared["campaign_id"])
        posts_before_incomplete = len(env.stubs.posts)
        progress_before = campaign_send_progress(store, prepared["campaign_id"])
        env.stubs.shoko_file_list_calls = 0
        env.stubs.malformed_shoko_list_call = 2

        incomplete = _daemon_cycle(
            env,
            store,
            prepared["campaign_id"],
            "daemon-outer-complete-inner-incomplete",
        )

        assert incomplete.reconciliation_ready is True
        assert incomplete.campaign is None
        assert any(
            reason.startswith(
                f"campaign_deferred:{prepared['campaign_id']}:reconciliation_incomplete"
            )
            for reason in incomplete.reasons
        )
        assert "shoko_snapshot_incomplete:malformed_file" in incomplete.reasons
        assert len(env.stubs.posts) == posts_before_incomplete
        quarantined = store.get_file(candidate.shoko_id)
        campaign = store.get_campaign(prepared["campaign_id"])
        assert quarantined is not None
        assert quarantined.state is FileState.PLANNED
        assert campaign is not None
        assert campaign.status == "approved"
        assert campaign_send_progress(store, prepared["campaign_id"]) == progress_before
        assert store.get_meta(f"campaign_zero_progress:{prepared['campaign_id']}") is None

        env.stubs.malformed_shoko_list_call = None
        recovered = _daemon_cycle(
            env,
            store,
            prepared["campaign_id"],
            "daemon-retry-after-complete-snapshot",
        )

        assert recovered.reconciliation_ready is True
        assert recovered.campaign is not None
        assert recovered.campaign.submitted == COUNT_ONE
        assert len(env.stubs.posts) == posts_before_incomplete + COUNT_ONE
        assert candidate.shoko_id in env.stubs.posted_files
    finally:
        store.close()


# ------------------------------------------------------------------ pending targets


def test_pending_target_applies_only_after_exact_identity_resolution(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    specs_list = specs(COUNT_TWO)
    pending_spec = specs_list[1]
    env.stubs.pending.add(pending_spec.shoko_id)
    env.seed(specs_list)
    prepared = prepare(env, capsys)
    plan, digest = read_plan_file(env.tmp_path / "plan.json")
    entry = next(item for item in plan.files if item.shoko_file_id == pending_spec.shoko_id)
    assert entry.shoko_episode_ids == ()
    assert entry.anidb_target_identities == ((ANIME_ID, 1, "Episode", ANIDB_EP),)
    approve(env, capsys, prepared["approval_digest"])

    first = apply(env, capsys)
    assert first["exit"] == EXIT_OK
    assert first["payload"]["submitted"] == 0
    pending_deferrals = [
        reason for reason in first["payload"]["reasons"] if reason.startswith("pending_target:")
    ]
    assert len(pending_deferrals) == COUNT_TWO
    assert len(env.stubs.posts) == 0

    env.stubs.pending.clear()  # Shoko metadata materialized
    second = apply(env, capsys)
    assert second["exit"] == EXIT_OK
    assert second["payload"]["submitted"] == COUNT_TWO
    assert len(env.stubs.posts) == COUNT_TWO
    store = env.open_store()
    try:
        record = store.get_file(pending_spec.shoko_id)
        assert record is not None
        assert record.state is FileState.VERIFIED
    finally:
        store.close()


def test_pending_target_divergent_resolution_quarantines_target_drift(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    specs_list = specs(COUNT_TWO)
    pending_spec = specs_list[1]
    env.stubs.pending.add(pending_spec.shoko_id)
    env.seed(specs_list)
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    env.stubs.pending.clear()
    env.stubs.divergent = True  # AniDB episode 5502 materialized — NOT the approved identity
    result = apply(env, capsys)
    assert result["exit"] == EXIT_OK
    assert any("target_drift" in reason for reason in result["payload"]["reasons"])
    store = env.open_store()
    try:
        record = store.get_file(pending_spec.shoko_id)
        assert record is not None
        assert record.state in (FileState.BLOCKED_SAFETY, FileState.WAITING)
        assert "target_drift" in record.state_detail
        assert record.state is not FileState.VERIFIED
    finally:
        store.close()
    # zero POSTs: the divergent target is quarantined and the linkable file's plan
    # fingerprint moved (quarantine, not a silent replacement)
    assert len(env.stubs.posts) == 0


def test_approved_set_never_expands_to_newly_mapped_files(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    env.stubs.specs = specs(COUNT_FIVE)  # three more files appear (new mappings)
    result = apply(env, capsys)
    assert result["exit"] == EXIT_OK
    assert result["payload"]["submitted"] == COUNT_TWO
    assert len(env.stubs.posts) == COUNT_TWO
    posted_files = set(env.stubs.posted_files)
    approved_files = {spec.shoko_id for spec in specs(COUNT_TWO)}
    assert posted_files == approved_files
    store = env.open_store()
    try:
        for spec in specs(COUNT_FIVE)[COUNT_TWO:]:
            record = store.get_file(spec.shoko_id)
            assert record is None or record.campaign_id != prepared["campaign_id"]
    finally:
        store.close()


# ------------------------------------------------------------------ bounds + lifecycle


def test_apply_refused_after_deadline(env: Env, capsys: pytest.CaptureFixture[str]) -> None:
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys, deadline_hours="1")
    approve(env, capsys, prepared["approval_digest"])
    env.now_iso = _iso_plus(T0, 2 * 3600)  # deadline was 1h after prepare
    result = apply(env, capsys)
    assert result["exit"] == EXIT_APPLY_GATE_REFUSAL
    assert "deadline" in str(result["payload"])
    assert env.stubs.posts == []


def test_max_submissions_exceeded_auto_pauses(env: Env, capsys: pytest.CaptureFixture[str]) -> None:
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys, limit="2")
    approve(env, capsys, prepared["approval_digest"])
    result = apply(env, capsys)
    assert result["exit"] == EXIT_OK
    assert result["payload"]["submitted"] == COUNT_TWO
    second = apply(env, capsys)
    assert second["exit"] == EXIT_APPLY_GATE_REFUSAL
    assert second["exit"] == EXIT_APPLY_GATE_REFUSAL
    assert "max_submissions" in str(second["payload"])
    store = env.open_store()
    try:
        campaign = store.get_campaign(prepared["campaign_id"])
        assert campaign is not None
        assert campaign.status == "paused"
        assert campaign.status_reason == "max_submissions_reached"
    finally:
        store.close()
    assert len(env.stubs.posts) == COUNT_TWO


def test_revoke_mid_campaign_releases_queued_rows(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    env.seed(specs(COUNT_FIVE))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    first = apply(env, capsys, batch="2")
    assert first["payload"]["submitted"] == BATCH_TWO
    code = main(
        [
            "campaign",
            "revoke",
            str(env.config_path),
            "--campaign-id",
            prepared["campaign_id"],
            "--note",
            "scope review",
            "--confirm",
            "--json",
        ]
    )
    assert code == EXIT_OK
    payload = json.loads(capsys.readouterr().out)
    assert payload["result"]["queued_rows_released"] == COUNT_FIVE - BATCH_TWO
    result = apply(env, capsys)
    assert result["exit"] == EXIT_APPLY_GATE_REFUSAL
    assert "revoked" in str(result["payload"])
    assert len(env.stubs.posts) == BATCH_TWO


def test_pause_resume_round_trip(env: Env, capsys: pytest.CaptureFixture[str]) -> None:
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    code = main(
        [
            "campaign",
            "pause",
            str(env.config_path),
            "--campaign-id",
            prepared["campaign_id"],
            "--reason",
            "disk review",
            "--note",
            "ops",
            "--confirm",
            "--json",
        ]
    )
    assert code == EXIT_OK
    capsys.readouterr()
    result = apply(env, capsys)
    assert result["exit"] == EXIT_APPLY_GATE_REFUSAL
    assert "paused" in str(result["payload"])
    # resume refused while the campaign lease is absent/invalid:
    env.lease_path.unlink()
    code = main(
        [
            "campaign",
            "resume",
            str(env.config_path),
            "--campaign-id",
            prepared["campaign_id"],
            "--note",
            "lease missing",
            "--confirm",
            "--json",
        ]
    )
    assert code == EXIT_CONFIG_INVALID
    capsys.readouterr()
    env.write_campaign_lease(prepared["campaign_id"])
    code = main(
        [
            "campaign",
            "resume",
            str(env.config_path),
            "--campaign-id",
            prepared["campaign_id"],
            "--note",
            "re-checked",
            "--confirm",
            "--json",
        ]
    )
    assert code == EXIT_OK
    capsys.readouterr()
    result = apply(env, capsys)
    assert result["exit"] == EXIT_OK
    assert result["payload"]["submitted"] == COUNT_TWO


def test_campaign_verify_closes_async_jobs_on_next_invocation(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    specs_list = specs(COUNT_TWO)
    slow = specs_list[0]
    env.stubs.verify_empty.add(slow.shoko_id)  # ManualLinkJob lands later
    env.seed(specs_list)
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    result = apply(env, capsys)
    assert result["exit"] == EXIT_OK
    assert result["payload"]["submitted"] == COUNT_TWO
    store = env.open_store()
    try:
        record = store.get_file(slow.shoko_id)
        assert record is not None
        assert record.state is FileState.VERIFYING  # accepted, not yet verified
    finally:
        store.close()
    env.stubs.verify_empty.discard(slow.shoko_id)
    env.stubs.linked[slow.shoko_id] = [env.stubs.xref(slow)]
    code = main(
        [
            "campaign",
            "verify",
            str(env.config_path),
            "--campaign-id",
            prepared["campaign_id"],
            "--json",
        ]
    )
    assert code == EXIT_OK
    payload = json.loads(capsys.readouterr().out)
    assert payload["verified"] == COUNT_TWO
    store = env.open_store()
    try:
        record = store.get_file(slow.shoko_id)
        assert record is not None
        assert record.state is FileState.VERIFIED
    finally:
        store.close()


def test_campaign_report_shows_digests_and_progress(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    apply(env, capsys)
    code = main(
        [
            "campaign",
            "report",
            str(env.config_path),
            "--campaign-id",
            prepared["campaign_id"],
            "--plan",
            str(env.tmp_path / "plan.json"),
            "--json",
        ]
    )
    assert code == EXIT_OK
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "approved"
    assert payload["approval_digest"] == prepared["approval_digest"]
    assert payload["plan_file_digest"] == prepared["approval_digest"]
    assert payload["plan_matches_file"] is True
    assert payload["total_sent"] == COUNT_TWO
    assert payload["verified"] == COUNT_TWO


# ----------------------------------------------------------- M7 review: queue, remaining, exit 2


def test_blocked_queue_defers_all_candidates_without_quarantine(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """M7 review P1: BlockedCount 3 > tolerance 0 → zero POSTs, deferral, no quarantine."""
    env.stubs.blocked_count = BLOCKED_QUEUE_COUNT
    env.seed(specs(COUNT_FIVE))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    result = apply(env, capsys)
    assert result["exit"] == EXIT_OK
    payload = result["payload"]
    assert payload["submitted"] == 0
    assert payload["quarantined"] == 0
    assert payload["remaining"] == COUNT_FIVE
    assert len(env.stubs.posts) == 0
    assert any(
        "preflight_deferred" in reason and "queue_blocked" in reason
        for reason in payload["reasons"]
    )
    assert not any("campaign_quarantine" in reason for reason in payload["reasons"])
    store = env.open_store()
    try:
        campaign = store.get_campaign(prepared["campaign_id"])
        assert campaign is not None
        assert campaign.status == "approved"
        for spec in specs(COUNT_FIVE):
            record = store.get_file(spec.shoko_id)
            assert record is not None
            assert record.state is FileState.PLANNED
    finally:
        store.close()


def test_remaining_counts_distinct_files_with_send_progress(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """M7 review P3-2: 5 files, one fully-verified batch of 3 → remaining 2, not 0."""
    env.seed(specs(COUNT_FIVE))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    result = apply(env, capsys, batch="3")
    assert result["exit"] == EXIT_OK
    payload = result["payload"]
    assert payload["submitted"] == BATCH_THREE
    assert payload["verified"] == BATCH_THREE
    assert payload["remaining"] == REMAINING_TWO


def test_apply_with_unparseable_plan_file_exits_two(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """M7 review P3-3: a malformed plan file on apply is invalid input (exit 2), not 3."""
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    plan_path = env.tmp_path / "plan.json"
    plan_path.write_text("{not json", encoding="utf-8")
    result = apply(env, capsys)
    assert result["exit"] == EXIT_CONFIG_INVALID
    assert "request invalid" in str(result["payload"])
    assert env.stubs.posts == []


def test_apply_with_digest_format_invalid_plan_file_exits_two(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """Plan-file digest-format errors on apply land on the CampaignPlanInvalid path (exit 2)."""
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    plan_path = env.tmp_path / "plan.json"
    document = json.loads(plan_path.read_bytes())
    document["files"][0]["plan_fingerprint"] = "not-a-digest"
    plan_path.write_text(
        json.dumps(document, sort_keys=True, separators=(",", ":")), encoding="utf-8"
    )
    result = apply(env, capsys)
    assert result["exit"] == EXIT_CONFIG_INVALID
    assert "plan_fingerprint" in str(result["payload"])
    assert env.stubs.posts == []


def test_read_plan_file_malformed_raises_campaign_plan_invalid(tmp_path: Path) -> None:
    path = tmp_path / "plan.json"
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(CampaignPlanInvalidError, match="canonical JSON"):
        read_plan_file(path)


# ------------------------------------------------------------------ module-level edge


def test_read_plan_file_rejects_malformed_documents(tmp_path: Path) -> None:
    path = tmp_path / "plan.json"
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(Exception, match="canonical JSON"):
        read_plan_file(path)


def test_approve_digest_mismatch_exception_carries_both(
    tmp_path: Path, env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    store = env.open_store()
    try:
        with pytest.raises(CampaignDigestMismatchError) as excinfo:
            approve_campaign(store, env.tmp_path / "plan.json", ZERO_DIGEST, T0)
        assert excinfo.value.supplied == ZERO_DIGEST
        assert excinfo.value.computed == prepared["approval_digest"]
    finally:
        store.close()


# ------------------------------------------------------------------ F1: apply lock

# The campaign apply send loop shares the daemon's single-process apply lock
# (PROJECT: one apply writer per store). Every fixture below supplies ``pid`` and
# ``lease_seconds`` through :func:`shoko_linker.cli.build_daemon_deps` — the same wiring the
# CLI uses: ``pid=os.getpid()`` and ``lease_seconds=settings.sync.apply_lock_lease_seconds``
# (the campaign document leaves the default 3600). No campaign fixture needs a manual
# ``DaemonDeps``: apply goes through the CLI seam, so the lease inputs are carried by
# ``DaemonDeps`` itself (pid/os.getpid, lease_seconds/config default).


def _campaign_deps(env: Env, store: StateStore, campaign_id: str) -> Any:
    settings = load_settings(env.config_path)
    return cli.build_daemon_deps(
        settings,
        store=store,
        mapping_store=cli.open_mapping_store(settings),
        max_submissions=CAMPAIGN_APPLY_MAX,
        overrides_text=cli.read_overrides_text(settings),
        attestation_scope=f"campaign:{campaign_id}",
    )


def _set_campaign(env: Env, **campaign: Any) -> None:
    """Rewrite the fixture config's campaign section (e.g. a smaller max_batch_size)."""
    document = _document(env.lease_path)
    document["campaign"] = {**document["campaign"], **campaign}
    env.config_path.write_text(yaml.safe_dump(document), encoding="utf-8")


def _set_sync(env: Env, **sync: Any) -> None:
    """Rewrite the fixture config's sync section (budget ceilings per scenario)."""
    document = _document(env.lease_path)
    document["sync"] = {**document["sync"], **sync}
    env.config_path.write_text(yaml.safe_dump(document), encoding="utf-8")


def _budget_window_keys(store: StateStore) -> set[str]:
    """Persisted per-cycle budget window keys (the F9 durable-cycle identity rows)."""
    rows = store._conn.execute("SELECT window_key FROM budgets").fetchall()
    return {str(row[0]) for row in rows}


def _daemon_cycle(env: Env, store: StateStore, campaign_id: str, cycle_key: str) -> Any:
    deps = _campaign_deps(env, store, campaign_id)
    return run_cycle(
        deps,
        now_iso=env.now_iso,
        mode=CycleMode.APPLY,
        policy=RunPolicy.BACKLOG,
        cycle_key=cycle_key,
    )


def _campaign_sent(store: StateStore, campaign_id: str) -> int:
    campaign = store.get_campaign(campaign_id)
    assert campaign is not None
    return int(campaign.total_sent)


def test_apply_campaign_defers_batch_when_apply_lock_held_by_foreign_pid(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    settings = load_settings(env.config_path)
    store = env.open_store()
    try:
        acquire_apply_lock(
            store,
            pid=FOREIGN_PID,
            iso=T0,
            lease_expires_iso=_iso_plus(T0, LEASE_LONG_SECONDS),
        )
        deps = _campaign_deps(env, store, prepared["campaign_id"])
        report = apply_campaign(deps, settings, store, env.tmp_path / "plan.json", T0)
        assert report.submitted == 0
        assert report.processed == 0
        assert any(
            reason.startswith("apply_lock_contended:") and str(FOREIGN_PID) in reason
            for reason in report.reasons
        )
        assert env.stubs.posts == []
        campaign = store.get_campaign(prepared["campaign_id"])
        assert campaign is not None
        assert campaign.status == "approved"
        holder = store.lock_holder()
        assert holder is not None
        assert holder.owner_pid == FOREIGN_PID
        daemon_report = run_cycle(
            deps,
            now_iso=T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.BACKLOG,
            cycle_key="daemon-under-foreign-lock",
        )
        assert daemon_report.writes_suppressed
        assert any("apply_lock_contended" in reason for reason in daemon_report.reasons)
        assert daemon_report.counts.submitted == 0
    finally:
        store.close()
    assert env.stubs.posts == []


def test_apply_campaign_contended_lock_makes_no_guard_mutation(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """P2-1: the lock is acquired before any guard runs — a deferred batch mutates
    nothing (no attestation-gate meta, no pause transition), daemon-parity ordering."""
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    settings = load_settings(env.config_path)
    store = env.open_store()
    try:
        acquire_apply_lock(
            store,
            pid=FOREIGN_PID,
            iso=T0,
            lease_expires_iso=_iso_plus(T0, LEASE_LONG_SECONDS),
        )
        deps = _campaign_deps(env, store, prepared["campaign_id"])
        report = apply_campaign(deps, settings, store, env.tmp_path / "plan.json", T0)
        assert report.submitted == 0
        assert any(reason.startswith("apply_lock_contended:") for reason in report.reasons)
        assert env.stubs.posts == []
        assert store.get_meta("attestation_gate") is None
        assert store.budget_view().paused_reason is None
        campaign = store.get_campaign(prepared["campaign_id"])
        assert campaign is not None
        assert campaign.status == "approved"
        assert campaign.total_sent == 0
    finally:
        store.close()


def test_apply_campaign_unavailable_lock_defers_without_crash(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """P3-1: a filesystem failure on apply.lock (here: a directory in its place) defers
    the batch with a named reason — never a crash, zero POSTs, campaign stays approved."""
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    settings = load_settings(env.config_path)
    store = env.open_store()
    (env.tmp_path / "apply.lock").mkdir()
    try:
        deps = _campaign_deps(env, store, prepared["campaign_id"])
        report = apply_campaign(deps, settings, store, env.tmp_path / "plan.json", T0)
        assert report.submitted == 0
        assert any(reason.startswith("apply_lock_unavailable:") for reason in report.reasons)
        assert env.stubs.posts == []
        campaign = store.get_campaign(prepared["campaign_id"])
        assert campaign is not None
        assert campaign.status == "approved"
        assert campaign.total_sent == 0
    finally:
        store.close()


def test_apply_campaign_write_disabled_store_defers_with_reason(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """P2-2: a write-disabled store defers the batch (daemon parity) — exit-clean
    report with reason ``writes_disabled``, no exception, zero POSTs."""
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    settings = load_settings(env.config_path)
    store = env.open_store()
    try:
        store.disable_writes("integrity probe")
        deps = _campaign_deps(env, store, prepared["campaign_id"])
        report = apply_campaign(deps, settings, store, env.tmp_path / "plan.json", T0)
        assert report.submitted == 0
        assert report.reasons == ("writes_disabled",)
        assert env.stubs.posts == []
        campaign = store.get_campaign(prepared["campaign_id"])
        assert campaign is not None
        assert campaign.status == "approved"
    finally:
        store.close()


def test_apply_campaign_steals_expired_lease_records_audit_and_releases(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    store = env.open_store()
    try:
        acquire_apply_lock(
            store,
            pid=FOREIGN_PID,
            iso=T0,
            lease_expires_iso=_iso_plus(T0, LEASE_SHORT_SECONDS),
        )
        expired_now = _iso_plus(T0, LEASE_EXPIRY_OFFSET_SECONDS)
        deps = _campaign_deps(env, store, prepared["campaign_id"])
        report = apply_campaign(
            deps, load_settings(env.config_path), store, env.tmp_path / "plan.json", expired_now
        )
        assert report.submitted == COUNT_TWO
        assert len(env.stubs.posts) == COUNT_TWO
        steals = json.loads(str(store.get_meta("apply_lock_steals")))
        assert any(entry["from_pid"] == FOREIGN_PID for entry in steals)
        assert store.lock_holder() is None
        campaign = store.get_campaign(prepared["campaign_id"])
        assert campaign is not None
        assert campaign.status == "approved"
    finally:
        store.close()


def test_campaign_apply_then_daemon_cycle_share_one_free_lock(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    env.seed(specs(COUNT_FIVE))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    settings = load_settings(env.config_path)
    store = env.open_store()
    try:
        deps = _campaign_deps(env, store, prepared["campaign_id"])
        report = apply_campaign(
            deps,
            settings,
            store,
            env.tmp_path / "plan.json",
            T0,
            batch_size=BATCH_TWO,
        )
        assert report.submitted == BATCH_TWO
        assert store.lock_holder() is None
        assert len(env.stubs.posts) == BATCH_TWO
        _authorize_drain(env, store, prepared["campaign_id"])  # F10: operator confirms the canary
        daemon_report = run_cycle(
            deps,
            now_iso=T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.BACKLOG,
            cycle_key="daemon-after-campaign",
        )
        # F7: the daemon re-acquires the same free lock (no self-contention) and drains the
        # remaining approved files automatically — the CLI batch (2) + daemon drain (3) = 5.
        assert not daemon_report.writes_suppressed
        assert not any("apply_lock_contended" in reason for reason in daemon_report.reasons)
        assert store.lock_holder() is None
        assert daemon_report.campaign is not None
        assert daemon_report.campaign.submitted == COUNT_THREE
        campaign = store.get_campaign(prepared["campaign_id"])
        assert campaign is not None
        assert campaign.total_sent == COUNT_FIVE
    finally:
        store.close()
    assert len(env.stubs.posts) == COUNT_FIVE


def test_apply_campaign_releases_lock_when_a_send_crashes_mid_batch(
    env: Env, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    settings = load_settings(env.config_path)
    store = env.open_store()
    try:
        deps = _campaign_deps(env, store, prepared["campaign_id"])

        campaign_dict = sys.modules["shoko_linker.campaign"].__dict__
        real_submit = cast("Callable[..., Any]", campaign_dict["submit_link"])

        def _boom(*args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("mid-apply crash")

        monkeypatch.setattr("shoko_linker.campaign.submit_link", _boom)
        with pytest.raises(RuntimeError, match="mid-apply crash"):
            apply_campaign(deps, settings, store, env.tmp_path / "plan.json", T0)
        assert store.lock_holder() is None
        monkeypatch.setattr("shoko_linker.campaign.submit_link", real_submit)
        second = apply_campaign(
            deps, settings, store, env.tmp_path / "plan.json", T0, batch_size=BATCH_TWO
        )
        assert second.submitted == 1
        assert len(env.stubs.posts) == 1
    finally:
        store.close()


def test_cli_campaign_apply_with_contended_lock_exits_zero_with_reason(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    store = env.open_store()
    try:
        acquire_apply_lock(
            store,
            pid=FOREIGN_PID,
            iso=T0,
            lease_expires_iso=_iso_plus(T0, LEASE_LONG_SECONDS),
        )
    finally:
        store.close()
    code = main(
        [
            "campaign",
            "apply",
            str(env.config_path),
            "--plan",
            str(env.tmp_path / "plan.json"),
            "--json",
        ]
    )
    captured = capsys.readouterr()
    assert code == EXIT_OK
    payload = json.loads(captured.out)
    assert payload["submitted"] == 0
    assert payload["processed"] == 0
    assert any(reason.startswith("apply_lock_contended:") for reason in payload["reasons"])
    assert env.stubs.posts == []


def test_apply_campaign_report_is_typed_batch_deferral(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """The contended path returns a report (not an exception) with the deferral reason."""
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    settings = load_settings(env.config_path)
    store = env.open_store()
    try:
        acquire_apply_lock(
            store,
            pid=FOREIGN_PID,
            iso=T0,
            lease_expires_iso=_iso_plus(T0, LEASE_LONG_SECONDS),
        )
        deps = _campaign_deps(env, store, prepared["campaign_id"])
        report = apply_campaign(deps, settings, store, env.tmp_path / "plan.json", T0)
        assert isinstance(report, CampaignApplyReport)
        assert report.campaign_id == prepared["campaign_id"]
        assert report.deferred == COUNT_TWO
        assert report.total_sent == 0
        assert report.remaining == COUNT_TWO
        assert report.stopped is None
    finally:
        store.close()


# ------------------------------------------------------------------ F7: daemon auto-drain

# The daemon drains an APPROVED campaign automatically — one bounded batch per apply cycle —
# ONLY after the operator's manual canary batch completes AND the operator explicitly authorizes
# the drain (F10: `campaign authorize-drain` on a settled, all-VERIFIED canary, digest-bound).
# Every scenario runs the full campaign guard chain via the daemon cycle; the daemon reuses its
# own apply lock (apply_campaign lock_held=True), so a daemon batch never self-contends.

COUNT_ONE = 1
BATCH_ONE = 1
COUNT_SIX = 6
COUNT_SEVEN = 7
CAMPAIGN_DAEMON_KEY = "daemon-drain"
LEASE_SHORT_HOURS = 1.0
AUTH_NOTE = "canary inspected: renames/deletions/MyList side effects reviewed"


def _canary(env: Env, store: StateStore, campaign_id: str) -> CampaignApplyReport:
    """The operator's manual first batch (the canary): one file through the CLI path."""
    return apply_campaign(
        _campaign_deps(env, store, campaign_id),
        load_settings(env.config_path),
        store,
        env.tmp_path / "plan.json",
        env.now_iso,
        batch_size=BATCH_ONE,
    )


def _authorize_drain(env: Env, store: StateStore, campaign_id: str) -> CampaignRecord:
    """F10: the operator's explicit, digest-bound drain authorization after a settled canary.

    The daemon drains only on this confirmation — a bare canary (verified or not) never
    opens the automatic drain.
    """
    return authorize_campaign_drain(store, campaign_id, env.now_iso, note=AUTH_NOTE)


def test_daemon_drains_nothing_before_the_operator_canary(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F7 canary gate: a freshly approved campaign (zero send progress) is never drained."""
    _set_campaign(env, max_batch_size=BATCH_TWO)
    env.seed(specs(COUNT_FIVE))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    store = env.open_store()
    try:
        cycle = _daemon_cycle(env, store, prepared["campaign_id"], CAMPAIGN_DAEMON_KEY)
        assert cycle.campaign is None  # nothing drained (canary not run)
        assert cycle.counts.submitted == 0
        assert env.stubs.posts == []
        campaign = store.get_campaign(prepared["campaign_id"])
        assert campaign is not None
        assert campaign.total_sent == 0
        assert campaign.status == "approved"
    finally:
        store.close()


def test_daemon_drains_all_batches_after_one_canary_zero_manual(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F7 core: after ONE manual canary + drain authorization, daemon cycles drain the rest
    in bounded batches."""
    _set_campaign(env, max_batch_size=BATCH_TWO)
    env.seed(specs(COUNT_FIVE))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    settings = load_settings(env.config_path)
    store = env.open_store()
    try:
        deps = _campaign_deps(env, store, prepared["campaign_id"])
        canary = apply_campaign(
            deps, settings, store, env.tmp_path / "plan.json", T0, batch_size=BATCH_ONE
        )
        assert canary.submitted == COUNT_ONE
        assert len(env.stubs.posts) == COUNT_ONE
        _authorize_drain(env, store, prepared["campaign_id"])  # F10: operator confirms the canary
        # Two daemon cycles drain the remaining 4 (2 per bounded batch), zero CLI calls.
        first = _daemon_cycle(env, store, prepared["campaign_id"], "d1")
        assert first.campaign is not None
        assert first.campaign.submitted == BATCH_TWO
        second = _daemon_cycle(env, store, prepared["campaign_id"], "d2")
        assert second.campaign is not None
        assert second.campaign.submitted == BATCH_TWO
        campaign = store.get_campaign(prepared["campaign_id"])
        assert campaign is not None
        assert campaign.total_sent == COUNT_FIVE
        assert len(env.stubs.posts) == COUNT_FIVE
        verified = sum(
            1
            for record in store.campaign_files(prepared["campaign_id"])
            if record.state is FileState.VERIFIED
        )
        assert verified == COUNT_FIVE
    finally:
        store.close()


def test_daemon_drain_resumes_after_restart_without_double_send(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F7 restart: a store reopen mid-drain resumes progress with zero re-POSTs."""
    _set_campaign(env, max_batch_size=BATCH_TWO)
    specs_list = specs(COUNT_FIVE)
    env.seed(specs_list)
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    settings = load_settings(env.config_path)
    store = env.open_store()
    try:
        deps = _campaign_deps(env, store, prepared["campaign_id"])
        apply_campaign(deps, settings, store, env.tmp_path / "plan.json", T0, batch_size=BATCH_ONE)
        _authorize_drain(env, store, prepared["campaign_id"])  # F10: confirmed canary
        cycle = _daemon_cycle(env, store, prepared["campaign_id"], "pre-restart")
        assert cycle.campaign is not None
        assert cycle.campaign.submitted == BATCH_TWO
        posts_before = len(env.stubs.posts)
        assert posts_before == COUNT_THREE  # 1 canary + 2 drained
    finally:
        store.close()
    reopened = env.open_store()
    try:
        cycle2 = _daemon_cycle(env, reopened, prepared["campaign_id"], "post-restart")
        campaign = reopened.get_campaign(prepared["campaign_id"])
        assert campaign is not None
        # Recovery re-walks the durable rows and drains the remaining 2; no file is ever re-sent.
        assert cycle2.campaign is not None
        assert cycle2.campaign.submitted == COUNT_TWO
        assert campaign.total_sent == COUNT_FIVE
        posted = env.stubs.posted_files
        assert len(posted) == COUNT_FIVE  # exactly one POST per approved file, no duplicates
        assert len(set(posted)) == COUNT_FIVE
        assert set(posted) == {spec.shoko_id for spec in specs_list}
    finally:
        reopened.close()


def test_daemon_drain_defers_on_missing_plan_with_zero_posts(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F7: a missing plan file defers the batch (named reason), no crash, zero POSTs."""
    _set_campaign(env, max_batch_size=BATCH_TWO)
    env.seed(specs(COUNT_FIVE))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    store = env.open_store()
    try:
        _canary(env, store, prepared["campaign_id"])
        _authorize_drain(env, store, prepared["campaign_id"])  # F10: drain is authorized
        posts_before = len(env.stubs.posts)
    finally:
        store.close()
    (env.tmp_path / "plan.json").unlink()
    store = env.open_store()
    try:
        cycle = _daemon_cycle(env, store, prepared["campaign_id"], "missing-plan")
        assert cycle.alive is True
        assert cycle.campaign is None
        deferred = [reason for reason in cycle.reasons if "campaign_deferred" in reason]
        assert deferred
        assert "plan_unreadable" in deferred[0]  # the bound plan path is now unreadable
        assert len(env.stubs.posts) == posts_before  # zero new POSTs
        campaign = store.get_campaign(prepared["campaign_id"])
        assert campaign is not None
        # P1 fix: an unreadable bound plan is terminal for the daemon drain — the campaign
        # pauses (the operator re-prepares/re-approves to resume), never a head-of-line stall.
        assert campaign.status == "paused"
        assert campaign.status_reason == "plan_unreadable"
    finally:
        store.close()


def test_daemon_drain_stops_on_expired_attestation_mid_drain(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F7: an expiring campaign lease mid-drain stops the batch (persisted pause), zero POSTs."""
    _set_campaign(env, max_batch_size=BATCH_TWO)
    env.seed(specs(COUNT_FIVE))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    store = env.open_store()
    try:
        _canary(env, store, prepared["campaign_id"])
        _authorize_drain(env, store, prepared["campaign_id"])  # F10: drain is authorized
        _daemon_cycle(env, store, prepared["campaign_id"], "d1")
        posts_before = len(env.stubs.posts)
        # Re-record a lease that expires in 1h, then advance the clock past it.
        record = build_record(
            endpoint=SHOKO_BASE,
            version="5.3.3",
            mounts=["/anime read-only (host-inspected)"],
            settings_flags={
                "FileQualityFilterEnabled": False,
                "RenameOnImport": False,
                "MoveOnImport": False,
                "MyListAddFiles": False,
            },
            inspected_iso=env.now_iso,
            expires_iso=_iso_plus(env.now_iso, 3600.0),
            scope=f"campaign:{prepared['campaign_id']}",
            operator_note="short campaign lease",
        )
        write_attestation(env.lease_path, record, overwrite=True)
        env.now_iso = _iso_plus(env.now_iso, LEASE_SHORT_HOURS * 3600.0 + 60.0)
        cycle = _daemon_cycle(env, store, prepared["campaign_id"], "expired")
        assert cycle.alive is True
        assert cycle.writes_suppressed is True
        assert cycle.apply_eligible is False
        assert any("attestation" in reason for reason in cycle.reasons)
        assert len(env.stubs.posts) == posts_before  # zero new POSTs
        assert store.budget_view().paused_reason == "attestation_expired"
    finally:
        store.close()


def test_daemon_drain_quarantines_drifted_candidate_no_silent_replace(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F7: a candidate whose size changed mid-drain is quarantined, never silently replaced."""
    _set_campaign(env, max_batch_size=COUNT_FIVE)
    specs_list = specs(COUNT_FIVE)
    env.seed(specs_list)
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    store = env.open_store()
    try:
        _canary(env, store, prepared["campaign_id"])
        _authorize_drain(env, store, prepared["campaign_id"])  # F10: drain is authorized
        victim = specs_list[3]
        env.stubs.sizes[victim.shoko_id] = FILE_SIZE + 1
        cycle = _daemon_cycle(env, store, prepared["campaign_id"], "drift")
        assert cycle.campaign is not None
        assert cycle.campaign.quarantined == COUNT_ONE
        record = store.get_file(victim.shoko_id)
        assert record is not None
        assert record.state in (FileState.BLOCKED_SAFETY, FileState.WAITING)
        assert record.state_detail.startswith("campaign_quarantine:")
        posted = set(env.stubs.posted_files)
        assert victim.shoko_id not in posted
    finally:
        store.close()


def test_daemon_drain_never_processes_unapproved_files(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F7: newly mapped files that are not in the approved plan are never POSTed by the drain."""
    _set_campaign(env, max_batch_size=BATCH_TWO)
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    approved = {spec.shoko_id for spec in specs(COUNT_TWO)}
    store = env.open_store()
    try:
        _canary(env, store, prepared["campaign_id"])
        _authorize_drain(env, store, prepared["campaign_id"])  # F10: drain is authorized
        env.stubs.specs = specs(COUNT_FIVE)  # three more files appear (new mappings)
        _daemon_cycle(env, store, prepared["campaign_id"], "extra")
        posted = set(env.stubs.posted_files)
        assert posted <= approved
        assert len(posted) == COUNT_TWO
    finally:
        store.close()


def test_operator_pause_resume_revoke_control_the_daemon_drain(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F7: pause stops the drain, resume continues it, revoke stops it and refuses apply."""
    _set_campaign(env, max_batch_size=BATCH_TWO)
    env.seed(specs(COUNT_FIVE))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)
        _authorize_drain(env, store, campaign_id)  # F10: drain is authorized
        _daemon_cycle(env, store, campaign_id, "d1")
        sent_running = _campaign_sent(store, campaign_id)
    finally:
        store.close()
    assert (
        main(
            [
                "campaign",
                "pause",
                str(env.config_path),
                "--campaign-id",
                campaign_id,
                "--reason",
                "ops review",
                "--note",
                "ops",
                "--confirm",
                "--json",
            ]
        )
        == EXIT_OK
    )
    capsys.readouterr()
    store = env.open_store()
    try:
        paused = _daemon_cycle(env, store, campaign_id, "paused")
        assert paused.campaign is None
        assert _campaign_sent(store, campaign_id) == sent_running  # drain stopped
    finally:
        store.close()
    assert (
        main(
            [
                "campaign",
                "resume",
                str(env.config_path),
                "--campaign-id",
                campaign_id,
                "--note",
                "resumed",
                "--confirm",
                "--json",
            ]
        )
        == EXIT_OK
    )
    capsys.readouterr()
    store = env.open_store()
    try:
        resumed = _daemon_cycle(env, store, campaign_id, "resumed")
        assert resumed.campaign is not None  # drain continues
        assert resumed.campaign.submitted == COUNT_FIVE - sent_running
    finally:
        store.close()
    assert (
        main(
            [
                "campaign",
                "revoke",
                str(env.config_path),
                "--campaign-id",
                campaign_id,
                "--note",
                "scope review",
                "--confirm",
                "--json",
            ]
        )
        == EXIT_OK
    )
    capsys.readouterr()
    store = env.open_store()
    try:
        after_revoke = _daemon_cycle(env, store, campaign_id, "revoked")
        assert after_revoke.campaign is None  # revoked campaign is skipped
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status == "revoked"
    finally:
        store.close()


def test_daemon_skips_campaign_after_deadline(env: Env, capsys: pytest.CaptureFixture[str]) -> None:
    """F7: past the campaign deadline the drain refuses (persisted refusal), zero POSTs."""
    _set_campaign(env, max_batch_size=BATCH_TWO)
    env.seed(specs(COUNT_FIVE))
    prepared = prepare(env, capsys, deadline_hours="1")
    approve(env, capsys, prepared["approval_digest"])
    store = env.open_store()
    try:
        _canary(env, store, prepared["campaign_id"])
        _authorize_drain(env, store, prepared["campaign_id"])  # F10: drain is authorized
        posts_before = len(env.stubs.posts)
        env.now_iso = _iso_plus(T0, 2 * 3600)  # deadline was 1h after prepare
        cycle = _daemon_cycle(env, store, prepared["campaign_id"], "deadline")
        assert cycle.campaign is None
        assert any("campaign_deferred" in reason for reason in cycle.reasons)
        assert len(env.stubs.posts) == posts_before
    finally:
        store.close()


def test_approve_persists_canonical_plan_path_for_daemon_drain(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F7: ``campaign approve`` records the canonical plan file path so the daemon drains the
    campaign WITHOUT the operator re-supplying --plan every cycle."""
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    plan_path = env.tmp_path / "plan.json"
    store = env.open_store()
    try:
        campaign = store.get_campaign(prepared["campaign_id"])
        assert campaign is not None
        assert campaign.plan_path == str(plan_path.resolve())
    finally:
        store.close()


def test_daemon_campaign_batch_runs_under_held_daemon_lock_no_self_contention(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F7 lock parity: apply_campaign(lock_held=True) never self-contends a held lock."""
    _set_campaign(env, max_batch_size=BATCH_TWO)
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    settings = load_settings(env.config_path)
    store = env.open_store()
    try:
        # Simulate the daemon cycle owning the apply lock for this process (same pid).
        acquire_apply_lock(store, pid=os.getpid(), iso=T0, lease_expires_iso=_iso_plus(T0, 7200.0))
        deps = _campaign_deps(env, store, prepared["campaign_id"])
        report = apply_campaign(
            deps, settings, store, env.tmp_path / "plan.json", T0, lock_held=True
        )
        assert report.submitted == COUNT_TWO
        assert not any(reason.startswith("apply_lock_contended") for reason in report.reasons)
        assert len(env.stubs.posts) == COUNT_TWO
        # lock_held does not release the caller's lock.
        holder = store.lock_holder()
        assert holder is not None
        assert holder.owner_pid == os.getpid()
    finally:
        release_apply_lock(store, os.getpid(), T0)
        store.close()


# ----------------------------------------------------- F7 review round: drops, pauses, aggregation

DEADLINE_ONE_HOUR = "1"
DEADLINE_WEEK = "168"
HOUR_SECONDS = 3600.0
ZERO_PROGRESS_CYCLE_KEYS = ("zp1", "zp2", "zp3")


def test_daemon_drain_pauses_deadline_exceeded_campaign_with_zero_posts(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """P1 fix: a past-deadline campaign auto-pauses on the drain attempt, drops out
    of selection, and submits zero new POSTs."""
    _set_campaign(env, max_batch_size=BATCH_TWO)
    env.seed(specs(COUNT_FIVE))
    prepared = prepare(env, capsys, deadline_hours=DEADLINE_ONE_HOUR)
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)
        _authorize_drain(env, store, campaign_id)  # F10: drain is authorized
        posts_before = len(env.stubs.posts)
        env.now_iso = _iso_plus(T0, 2.0 * HOUR_SECONDS)
        cycle = _daemon_cycle(env, store, campaign_id, "past-deadline")
        assert cycle.campaign is None
        assert any(reason.startswith("campaign_deferred:") for reason in cycle.reasons)
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status == "paused"
        assert campaign.status_reason == "deadline_exceeded"
        assert len(env.stubs.posts) == posts_before
        later = _daemon_cycle(env, store, campaign_id, "after-dropout")
        assert later.campaign is None
        assert len(env.stubs.posts) == posts_before
    finally:
        store.close()


def test_daemon_drain_head_of_line_campaign_drops_out_and_the_next_drains(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """P1 fix + multi-campaign: the oldest campaign past its deadline pauses on the drain
    attempt; the next approved campaign then drains automatically (no head-of-line stall)."""
    _set_campaign(env, max_batch_size=BATCH_TWO)
    env.seed(specs(COUNT_TWO))  # campaign A: shoko 500, 501
    prepared_a = prepare(env, capsys, deadline_hours="2")
    approve(env, capsys, prepared_a["approval_digest"])
    store = env.open_store()
    try:
        _canary(env, store, prepared_a["campaign_id"])  # 500 linked under A
        _authorize_drain(env, store, prepared_a["campaign_id"])  # F10: A's drain is authorized
        # Campaign B: two fresh files plus 501 (A's unlinked remainder stays eligible).
        env.seed(
            [
                SpecFile(series_id=3, file_id=12, shoko_id=602),
                SpecFile(series_id=4, file_id=13, shoko_id=603),
            ]
        )
        env.now_iso = _iso_plus(T0, HOUR_SECONDS / 2.0)
        plan_b_path = env.tmp_path / "planB.json"
        assert (
            main(
                [
                    "campaign",
                    "prepare",
                    str(env.config_path),
                    "--out",
                    str(plan_b_path),
                    "--deadline-hours",
                    DEADLINE_WEEK,
                    "--limit",
                    "3",
                    "--json",
                ]
            )
            == EXIT_OK
        )
        payload_b = json.loads(capsys.readouterr().out)
        campaign_b = str(payload_b["campaign_id"])
        env.write_campaign_lease(campaign_b)
        assert (
            main(
                [
                    "campaign",
                    "approve",
                    str(env.config_path),
                    "--plan",
                    str(plan_b_path),
                    "--digest",
                    str(payload_b["approval_digest"]),
                    "--json",
                ]
            )
            == EXIT_OK
        )
        settings = load_settings(env.config_path)
        apply_campaign(
            _campaign_deps(env, store, campaign_b),
            settings,
            store,
            plan_b_path,
            env.now_iso,
            batch_size=1,
        )  # B's canary (501 linked under B)
        _authorize_drain(env, store, campaign_b)  # F10: B's drain is authorized
        # Past A's deadline: A wins selection, the guard pauses it, B stays untouched.
        env.now_iso = _iso_plus(T0, 2.2 * HOUR_SECONDS)
        cycle_a = _daemon_cycle(env, store, campaign_b, "a-pauses")
        assert cycle_a.campaign is None
        campaign_a = store.get_campaign(prepared_a["campaign_id"])
        assert campaign_a is not None
        assert campaign_a.status == "paused"
        assert campaign_a.status_reason == "deadline_exceeded"
        assert _campaign_sent(store, campaign_b) == COUNT_ONE
        # A drops out; the daemon drains campaign B automatically. 602/603 are also
        # new-arrival-eligible, so either path linking them is correct — the point is the
        # head-of-line campaign no longer blocks progress.
        cycle_b = _daemon_cycle(env, store, campaign_b, "b-drains")
        assert cycle_b.campaign is not None
        assert cycle_b.campaign.campaign_id == campaign_b
        campaign = store.get_campaign(campaign_b)
        assert campaign is not None
        assert campaign.total_sent == COUNT_TWO
        posted = set(env.stubs.posted_files)
        assert posted == {500, 602, 603}
        # A's unlinked remainder stays planned (A paused by deadline; operator resumes A
        # to finish it) — the daemon drained B without head-of-line stalling.
        remainder = store.get_file(501)
        assert remainder is not None
        assert remainder.state is FileState.PLANNED
    finally:
        store.close()


def test_daemon_zero_progress_all_blocked_campaign_auto_pauses(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """P2-3 fix: a campaign whose remaining files are all unqueueable pauses after the
    bounded zero-progress cycles (no endless full-sweep churn); zero POSTs."""
    _set_campaign(env, max_batch_size=BATCH_TWO)
    specs_list = specs(COUNT_FIVE)
    env.seed(specs_list)
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)
        _authorize_drain(env, store, campaign_id)  # F10: drain is authorized
        posts_before = len(env.stubs.posts)
        for spec in specs_list[1:]:
            store.advance(spec.shoko_id, FileState.BLOCKED_SAFETY, "operator-blocked", iso=T0)
        pause_cycle: CampaignApplyReport | None = None
        for key in ZERO_PROGRESS_CYCLE_KEYS:
            cycle = _daemon_cycle(env, store, campaign_id, key)
            if key == ZERO_PROGRESS_CYCLE_KEYS[-1]:
                assert any(
                    reason.startswith("campaign_zero_progress_pause:") for reason in cycle.reasons
                )
                pause_cycle = cycle.campaign
        assert pause_cycle is not None
        assert pause_cycle.processed == 0
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status == "paused"
        assert campaign.status_reason == "no_queueable_candidates"
        assert len(env.stubs.posts) == posts_before
        after = _daemon_cycle(env, store, campaign_id, "after-pause")
        assert after.campaign is None
        assert len(env.stubs.posts) == posts_before
    finally:
        store.close()


def test_daemon_drain_aggregates_per_file_reasons(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """P2-2 fix: per-file deferral reasons collapse into one aggregated entry per family
    in the cycle report (no reason flood at backlog scale)."""
    _set_campaign(env, max_batch_size=BATCH_TWO)
    env.seed(specs(COUNT_FIVE))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)
        _authorize_drain(env, store, campaign_id)  # F10: drain is authorized
        cycle = _daemon_cycle(env, store, campaign_id, "aggregate")
        assert cycle.campaign is not None
        assert cycle.campaign.submitted == BATCH_TWO
        batch_limit_entries = [
            reason for reason in cycle.reasons if reason.startswith("batch_limit:")
        ]
        expected = f"batch_limit:x{COUNT_FIVE - BATCH_TWO - 1}:first=batch_limit:503"
        assert batch_limit_entries == [expected]
    finally:
        store.close()


# ------------------------------------------------------------------ F9: per-batch budget reset

# The per-cycle submission budget used to key every batch of a campaign to a constant window
# (``campaign-<short>``), so its counter accumulated forever and any campaign past
# ``sync.max_new_per_cycle`` stalled permanently. F9 gives every campaign batch (and every
# reconciliation cycle) a durable window_key unique across restarts, so the per-cycle cap is
# per batch, not per campaign.


def test_f9_campaign_per_cycle_budget_resets_each_batch(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """Scenario 1: 100 approved files under a per-cycle cap of 5 — daemon drain batches keep
    progressing past 5/10/20/50 (cap resets each batch) and the campaign completes."""
    _set_campaign(env, max_batch_size=BATCH_THREE)
    _set_sync(env, max_new_per_cycle=5, max_pending_episode_jobs=100, rolling_new_limit=1000)
    specs_list = specs(COUNT_HUNDRED)
    env.seed(specs_list)
    prepared = prepare(env, capsys, limit=str(COUNT_HUNDRED))
    assert prepared["files"] == COUNT_HUNDRED
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)
        _authorize_drain(env, store, campaign_id)  # F10: drain is authorized
        assert _campaign_sent(store, campaign_id) == 1
        milestones: list[int] = []
        for index in range(40):
            cycle = _daemon_cycle(env, store, campaign_id, f"drain-{index}")
            if cycle.campaign is None:
                break
            milestones.append(_campaign_sent(store, campaign_id))
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.total_sent == COUNT_HUNDRED
        assert campaign.status == "paused"  # max_submissions_reached
        for threshold in (5, 10, 20, 50):
            assert any(count > threshold for count in milestones)
        posted = env.stubs.posted_files
        assert len(posted) == COUNT_HUNDRED
        assert len(set(posted)) == COUNT_HUNDRED
    finally:
        store.close()


def test_f9_daemon_restart_new_cycle_keys_never_collide(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """Scenario 2: a store reopen mid-drain (new process) allocates cycle keys that never collide
    with the persisted pre-restart rows, and the campaign continues to completion."""
    _set_campaign(env, max_batch_size=BATCH_TWO)
    specs_list = specs(COUNT_FIVE)
    env.seed(specs_list)
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)
        _authorize_drain(env, store, campaign_id)  # F10: drain is authorized
        _daemon_cycle(env, store, campaign_id, "pre-restart")
        keys_before = _budget_window_keys(store)
    finally:
        store.close()
    reopened = env.open_store()
    try:
        new_key = reopened.begin_cycle("cycle", env.now_iso)
        assert new_key not in keys_before  # fresh process never reuses a persisted window row
        cycle = _daemon_cycle(env, reopened, campaign_id, "post-restart")
        assert cycle.campaign is not None
        campaign = reopened.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.total_sent == COUNT_FIVE
        posted = env.stubs.posted_files
        assert len(set(posted)) == COUNT_FIVE  # no file re-sent
    finally:
        reopened.close()


def test_f9_manual_apply_invocations_each_get_a_fresh_batch_key(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """Scenario 3: repeated one-shot ``campaign apply`` invocations each allocate a fresh batch
    key and make progress (the old constant key stalled every batch after the first cap)."""
    _set_campaign(env, max_batch_size=3)
    _set_sync(env, max_new_per_cycle=2)  # cap of 2 < batch of 3: only resets let it progress
    env.seed(specs(COUNT_FIVE))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]
    store = env.open_store()
    try:
        total = 0
        for _ in range(3):
            result = apply(env, capsys, batch="3")
            assert result["exit"] == EXIT_OK
            campaign = store.get_campaign(campaign_id)
            assert campaign is not None
            assert campaign.total_sent > total  # progress on every invocation
            total = campaign.total_sent
        assert total == COUNT_FIVE
        assert len(env.stubs.posts) == COUNT_FIVE
    finally:
        store.close()


def test_f9_outstanding_job_cap_enforced_across_batches(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """Scenario 5: the outstanding async-job ceiling keeps applying across batches — sends
    that stay in ``verifying`` hold their jobs, so a later batch defers at the cap.

    F10: the canary file verifies (a settled canary is the drain-authorization precondition);
    the remaining files never verify, so their jobs accumulate against the ceiling."""
    _set_campaign(env, max_batch_size=5)
    _set_sync(env, max_new_per_cycle=5, max_pending_episode_jobs=1, rolling_new_limit=1000)
    specs_list = specs(COUNT_FIVE)
    env.seed(specs_list)
    # Never verify (jobs held) — the canary file (500) excepted, so the drain can be authorized.
    env.stubs.verify_empty = {spec.shoko_id for spec in specs_list[1:]}
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)  # 1 job held
        _authorize_drain(env, store, campaign_id)  # F10: drain is authorized
        first = _daemon_cycle(env, store, campaign_id, "j1")
        assert first.campaign is not None
        assert first.campaign.submitted == 1  # 2nd job fills the ceiling
        blocked = _daemon_cycle(env, store, campaign_id, "j2")
        assert blocked.campaign is None  # jobs exhausted → the batch defers
        assert any("budgets_exhausted" in reason for reason in blocked.reasons)
    finally:
        store.close()


def test_f9_budget_blocked_campaign_defers_not_pauses(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """Scenario 6 (F18 rewrite): a campaign whose submissions are every budget-refused is
    DEFERRED with a persisted rolling-window retry time — it stays APPROVED and
    drain-authorized, never burns the zero-progress breaker, and is skipped while the
    rolling window is closed (the old ``no_budget_progress`` permanent pause is gone)."""
    _set_campaign(env, max_batch_size=5)
    # The canary's single submission spends the global rolling budget (window 168h), so every
    # later drain is budget-blocked: zero submissions until the rolling window reopens.
    _set_sync(env, max_new_per_cycle=1, rolling_new_limit=1)
    env.seed(specs(COUNT_FIVE))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)  # the one submission that opens the drain
        _authorize_drain(env, store, campaign_id)  # F10: operator confirms the settled canary
        posts_before = len(env.stubs.posts)
        assert posts_before == 1
        for key in ZERO_PROGRESS_CYCLE_KEYS:
            cycle = _daemon_cycle(env, store, campaign_id, key)
            assert cycle.campaign is None  # budget-blocked batches defer
            assert any("budgets_exhausted" in reason for reason in cycle.reasons)
            campaign = store.get_campaign(campaign_id)
            assert campaign is not None
            assert campaign.status == "approved"  # recoverable deferral — never a permanent pause
            assert not any("campaign_zero_progress_pause" in reason for reason in cycle.reasons)
        assert len(env.stubs.posts) == posts_before  # zero further POSTs
        deferred = store.get_meta(f"{F18_DEFER_KEY}:{campaign_id}")
        assert deferred is not None
        assert store.get_meta(f"{F18_ZERO_KEY}:{campaign_id}") in (None, "0")  # breaker not burned
    finally:
        store.close()


def test_f9_batch_loop_preflights_are_bounded_by_attempts(
    env: Env, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """F9-1(a): 12-file plan, per-cycle cap 3, batch 12. Submissions stop at the cap and the
    batch never walks the whole plan — preflight network reads stay O(cap), not O(len(plan))."""
    _set_campaign(env, max_batch_size=COUNT_TWELVE)
    _set_sync(
        env, max_new_per_cycle=CAP_THREE, max_pending_episode_jobs=100, rolling_new_limit=1000
    )
    env.seed(specs(COUNT_TWELVE))
    prepared = prepare(env, capsys, limit=str(COUNT_TWELVE))
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]

    preflight_calls = 0
    real_preflight = campaign_mod.preflight  # type: ignore[attr-defined]

    def _spy(*args: Any, **kwargs: Any) -> Any:
        nonlocal preflight_calls
        preflight_calls += 1
        return real_preflight(*args, **kwargs)

    monkeypatch.setattr(campaign_mod, "preflight", _spy)
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)
        _authorize_drain(env, store, campaign_id)  # F10: drain is authorized
        preflight_calls = 0  # measure only the drain batch
        cycle = _daemon_cycle(env, store, campaign_id, "cap3-batch12")
        assert cycle.campaign is not None
        assert cycle.campaign.submitted == CAP_THREE
        assert len(env.stubs.posts) == CAP_THREE + 1  # 3 drain + 1 canary
        # bounded by ~cap (the accepted files + one refused stop), never the 12-file plan
        assert preflight_calls <= CAP_THREE + 1
        assert preflight_calls < COUNT_TWELVE
    finally:
        store.close()


def test_f9_batch_loop_stops_at_the_wall_time_bound(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F9-1(c): a monotonic clock past ``max_cycle_seconds`` stops starting new submissions;
    the deferred remainder stays queueable (non-destructive) with zero further POSTs."""
    _set_campaign(env, max_batch_size=COUNT_TWELVE)
    _set_sync(
        env, max_new_per_cycle=CAP_THREE, max_pending_episode_jobs=100, rolling_new_limit=1000
    )
    env.seed(specs(COUNT_TWELVE))
    prepared = prepare(env, capsys, limit=str(COUNT_TWELVE))
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]
    settings = load_settings(env.config_path)
    store = env.open_store()
    try:
        base = _campaign_deps(env, store, campaign_id)
        clock = {"value": 0.0}

        def _fake_monotonic() -> float:
            current = clock["value"]
            clock["value"] += 100.0  # every reading crosses the 10s bound
            return current

        deps = replace(base, monotonic=_fake_monotonic, max_cycle_seconds=10.0)
        posts_before = len(env.stubs.posts)
        report = apply_campaign(deps, settings, store, env.tmp_path / "plan.json", T0)
        assert report.submitted == 0  # the clock is past the bound before the first submission
        assert any(reason.startswith("batch_time_limit:") for reason in report.reasons)
        assert len(env.stubs.posts) == posts_before  # zero POSTs after the bound
        planned = [
            record
            for record in store.campaign_files(campaign_id)
            if record.state is FileState.PLANNED
        ]
        assert len(planned) == COUNT_TWELVE  # the whole plan stays queueable (no quarantine)
    finally:
        store.close()


def test_f9_pending_target_scan_is_bounded_by_batch_size(
    env: Env, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """F9-1c: an all-pending-target plan must not walk every file's AniDB→Shoko chain each
    batch. Resolution attempts count toward the batch bound, so the resolver GET chain is
    O(batch size), files stay queueable, and later batches progress once targets exist."""
    _set_campaign(env, max_batch_size=CAP_THREE)
    _set_sync(
        env, max_new_per_cycle=CAP_THREE, max_pending_episode_jobs=100, rolling_new_limit=1000
    )
    specs_list = specs(COUNT_TWELVE)
    env.stubs.pending = {spec.shoko_id for spec in specs_list}  # Shoko metadata not materialized
    env.seed(specs_list)
    prepared = prepare(env, capsys, limit=str(COUNT_TWELVE))
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]

    resolve_calls = 0
    real_resolve = campaign_mod._resolve_and_rebuild  # the apply-batch pending resolution path

    def _spy(*args: Any, **kwargs: Any) -> Any:
        nonlocal resolve_calls
        resolve_calls += 1
        return real_resolve(*args, **kwargs)

    monkeypatch.setattr(campaign_mod, "_resolve_and_rebuild", _spy)
    store = env.open_store()
    try:
        first = apply(env, capsys, batch=str(CAP_THREE))
        assert first["exit"] == EXIT_OK
        assert first["payload"]["submitted"] == 0  # nothing materialized yet
        # bounded by the batch (3), not the 12-file plan
        assert resolve_calls <= CAP_THREE
        assert resolve_calls < COUNT_TWELVE
        assert len(env.stubs.posts) == 0
        bound = [
            record
            for record in store.campaign_files(campaign_id)
            if record.state in (FileState.MAPPED, FileState.PLANNED)
        ]
        assert len(bound) == COUNT_TWELVE  # every file stays queueable (no quarantine)

        resolve_calls = 0
        env.stubs.pending.clear()  # Shoko metadata materialized
        second = apply(env, capsys, batch=str(CAP_THREE))
        assert second["exit"] == EXIT_OK
        assert second["payload"]["submitted"] == CAP_THREE  # later batches progress
    finally:
        store.close()


def test_f9_head_job_exhaustion_defers_only_that_file(
    env: Env, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """F9-1d: a per-file ``pending_episode_jobs_exhausted`` (a multi-episode head file over the
    job cap) must NOT starve the campaign — healthy later files send, the head file defers
    (stays queueable, never quarantined), and the campaign is not auto-paused."""
    _set_campaign(env, max_batch_size=COUNT_FIVE)
    _set_sync(
        env, max_new_per_cycle=COUNT_FIVE, max_pending_episode_jobs=100, rolling_new_limit=1000
    )
    specs_list = specs(COUNT_FIVE)
    env.seed(specs_list)
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]
    head_id = specs_list[0].shoko_id

    store = env.open_store()
    real_prepare = store.prepare_send

    def _fake_prepare(file_id: int, *args: Any, **kwargs: Any) -> Any:
        # Simulate the head multi-episode file exceeding ``max_pending_episode_jobs``; every
        # other (1-episode) file reserves through the real path.
        if file_id == head_id:
            return BudgetRefusal(False, "pending_episode_jobs_exhausted", "forced per-file probe")
        return real_prepare(file_id, *args, **kwargs)

    monkeypatch.setattr(store, "prepare_send", _fake_prepare)
    try:
        report = apply_campaign(
            _campaign_deps(env, store, campaign_id),
            load_settings(env.config_path),
            store,
            env.tmp_path / "plan.json",
            T0,
            batch_size=COUNT_FIVE,
        )
        assert report.submitted == COUNT_FIVE - 1  # healthy files all sent
        assert any(
            reason.startswith(f"budget_refusal:{head_id}:pending_episode_jobs_exhausted")
            for reason in report.reasons
        )
        head = store.get_file(head_id)
        assert head is not None
        assert head.state is FileState.PLANNED  # deferred, never quarantined
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status == "approved"  # not starved into a zero-progress pause
        assert len(env.stubs.posts) == COUNT_FIVE - 1
        assert head_id not in env.stubs.posted_files
    finally:
        store.close()


def _force_job_exhaustion(
    store: StateStore, monkeypatch: pytest.MonkeyPatch, *, exclude: set[int]
) -> None:
    """Force a per-file ``pending_episode_jobs_exhausted`` refusal on every file except
    ``exclude`` (the reviewer's P2 probe: deferrals must never count as progress)."""
    real_prepare = store.prepare_send

    def _fake_prepare(file_id: int, *args: Any, **kwargs: Any) -> Any:
        if file_id in exclude:
            return real_prepare(file_id, *args, **kwargs)
        return BudgetRefusal(False, "pending_episode_jobs_exhausted", "forced per-file probe")

    monkeypatch.setattr(store, "prepare_send", _fake_prepare)


def test_f9_all_refused_batch_is_not_plan_complete(
    env: Env, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """P1: a batch where every file is per-file budget-refused must NOT report plan_complete."""
    _set_campaign(env, max_batch_size=COUNT_FIVE)
    _set_sync(
        env, max_new_per_cycle=COUNT_FIVE, max_pending_episode_jobs=100, rolling_new_limit=1000
    )
    env.seed(specs(COUNT_FIVE))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]
    store = env.open_store()
    _force_job_exhaustion(store, monkeypatch, exclude=set())
    try:
        report = apply_campaign(
            _campaign_deps(env, store, campaign_id),
            load_settings(env.config_path),
            store,
            env.tmp_path / "plan.json",
            T0,
            batch_size=COUNT_FIVE,
        )
        assert report.submitted == 0
        assert report.progress == 0  # zero real progress
        assert report.stopped != "plan_complete"  # scan activity is not completion
        assert report.stopped is None
        assert len(env.stubs.posts) == 0
        queueable = [
            record
            for record in store.campaign_files(campaign_id)
            if record.state in (FileState.PLANNED, FileState.MAPPED)
        ]
        assert len(queueable) == COUNT_FIVE  # every file stays queueable
    finally:
        store.close()


def test_f9_per_file_refusals_defer_without_pause(
    env: Env, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """P2 (F18 rewrite, Finding 1 head-of-line fix): after the canary, every later file is
    per-file budget-refused → zero submissions every drain cycle. This is a RECOVERABLE budget
    condition: the campaign defers with a bounded cooldown (a real future retry, not the old
    retry==now no-op), stays APPROVED, the breaker is never burned, and the deferred head
    YIELDS the queue — every later cycle skips it, so a queued campaign can drain.
    ``test_f18_stalled_head_cooldown_yields_the_queue`` (tests/test_daemon.py) proves the full
    two-campaign yield/resume; here the same family at a frozen clock."""
    _set_campaign(env, max_batch_size=COUNT_FIVE)
    _set_sync(
        env, max_new_per_cycle=COUNT_FIVE, max_pending_episode_jobs=100, rolling_new_limit=1000
    )
    env.seed(specs(COUNT_FIVE))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)  # opens the drain with one real submission
        _authorize_drain(env, store, campaign_id)  # F10: operator confirms the settled canary
        canary_files = set(env.stubs.posted_files)
        _force_job_exhaustion(store, monkeypatch, exclude=canary_files)
        posts_before = len(env.stubs.posts)
        assert posts_before == 1
        first = _daemon_cycle(env, store, campaign_id, "zp1")
        assert first.campaign is not None  # the drain runs (campaign is drain-eligible)
        assert first.campaign.submitted == 0
        assert first.campaign.progress == 0  # per-file refusals make zero real progress
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status == "approved"  # recoverable deferral, never paused
        assert not any("campaign_zero_progress_pause" in reason for reason in first.reasons)
        deferred = store.get_meta(f"{F18_DEFER_KEY}:{campaign_id}")
        assert deferred is not None  # a real bounded cooldown (Finding 1, not retry==now)
        assert deferred > env.now_iso
        assert any("budget_cooldown" in reason for reason in first.reasons)  # F2: budget family
        for key in ("zp2", "zp3"):
            cycle = _daemon_cycle(env, store, campaign_id, key)
            assert cycle.campaign is None  # the deferred head yields the queue every cycle
            assert not any("campaign_zero_progress_pause" in reason for reason in cycle.reasons)
        assert len(env.stubs.posts) == posts_before  # zero further POSTs across the cycles
        assert store.get_meta(f"{F18_ZERO_KEY}:{campaign_id}") in (None, "0")  # breaker not burned
    finally:
        store.close()


def test_f9_plan_complete_reflects_persisted_progress(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """P1: ``plan_complete`` is reported only when persisted campaign progress covers the plan."""
    _set_campaign(env, max_batch_size=COUNT_FIVE)
    _set_sync(
        env, max_new_per_cycle=COUNT_FIVE, max_pending_episode_jobs=100, rolling_new_limit=1000
    )
    env.seed(specs(COUNT_FIVE))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    store = env.open_store()
    try:
        report = apply_campaign(
            _campaign_deps(env, store, prepared["campaign_id"]),
            load_settings(env.config_path),
            store,
            env.tmp_path / "plan.json",
            T0,
            batch_size=COUNT_FIVE,
        )
        assert report.submitted == COUNT_FIVE
        assert report.progress > 0
        assert report.remaining == 0
        assert report.stopped == "plan_complete"  # all 5 files have persisted send progress
        assert campaign_send_progress(store, prepared["campaign_id"]) == COUNT_FIVE
    finally:
        store.close()


# ------------------------------------------------------------------ F8: independent lease scopes


def _tmp_posix(tmp_path: Path, name: str) -> str:
    """POSIX absolute string for ``tmp_path/<name>`` that ``Path`` resolves identically.

    ``safety.attestation_file`` must be POSIX-style; stripping the drive letter yields a rooted
    string that ``Path`` maps to the same on-disk location on the current drive (where pytest's
    tmp_path lives), so the real scope-aware resolver is exercised against real files.
    """
    posix = tmp_path.as_posix()
    if len(posix) >= 2 and posix[1] == ":":
        posix = posix[2:]
    return f"{posix}/{name}"


def _dual_document(lease_posix: str) -> dict[str, Any]:
    document = _document(Path(lease_posix))
    document["safety"]["attestation_file"] = lease_posix
    document["sync"]["max_new_per_cycle"] = 20
    document["sync"]["max_pending_episode_jobs"] = 20
    document["sync"]["new_file_grace_period_seconds"] = 0
    document["campaign"] = {
        "max_campaign_files": 7000,
        "max_batch_size": BATCH_TWO,
        "daemon_auto_apply": True,
    }
    return document


@dataclass
class DualEnv:
    """Production co-existence fixture: real scope-aware resolver, daemon + campaign sibling leases.

    The daemon lease lives at ``safety.attestation_file``; each campaign lease lives in its
    deterministic sibling. ``attestation_target_path`` is NOT monkeypatched — the real resolver
    selects the right file per scope, exactly as the daemon/CLI wiring does in production.
    """

    tmp_path: Path
    stubs: Stubs
    config_path: Path
    lease: Path
    now_iso: str = T0

    def open_store(self) -> StateStore:
        return StateStore(
            self.tmp_path / "state.sqlite3",
            budget_limits=cli.budget_limits_from_settings(load_settings(self.config_path)),
        )

    def write_daemon_lease(self, *, hours: float = 24.0) -> None:
        record = build_record(
            endpoint=SHOKO_BASE,
            version="5.3.3",
            mounts=["/anime read-only (host-inspected)"],
            settings_flags={
                "FileQualityFilterEnabled": False,
                "RenameOnImport": False,
                "MoveOnImport": False,
                "MyListAddFiles": False,
            },
            inspected_iso=self.now_iso,
            expires_iso=_iso_plus(self.now_iso, hours * 3600.0),
            scope=SCOPE_DAEMON,
            operator_note="daemon lease",
        )
        write_attestation(self.lease, record, overwrite=True)

    def write_campaign_lease(self, campaign_id: str, *, hours: float = 24.0) -> None:
        target = campaign_lease_path(self.lease, campaign_id)
        record = build_record(
            endpoint=SHOKO_BASE,
            version="5.3.3",
            mounts=["/anime read-only (host-inspected)"],
            settings_flags={
                "FileQualityFilterEnabled": False,
                "RenameOnImport": False,
                "MoveOnImport": False,
                "MyListAddFiles": False,
            },
            inspected_iso=self.now_iso,
            expires_iso=_iso_plus(self.now_iso, hours * 3600.0),
            scope=f"campaign:{campaign_id}",
            operator_note="campaign lease",
        )
        write_attestation(target, record, overwrite=True)

    def write_expired_campaign_lease(self, campaign_id: str) -> None:
        """Overwrite the campaign sibling with a lease already expired at ``now_iso``."""
        target = campaign_lease_path(self.lease, campaign_id)
        record = build_record(
            endpoint=SHOKO_BASE,
            version="5.3.3",
            mounts=["/anime read-only (host-inspected)"],
            settings_flags={
                "FileQualityFilterEnabled": False,
                "RenameOnImport": False,
                "MoveOnImport": False,
                "MyListAddFiles": False,
            },
            inspected_iso=_iso_plus(self.now_iso, -7200.0),
            expires_iso=_iso_plus(self.now_iso, -3600.0),
            scope=f"campaign:{campaign_id}",
            operator_note="expired campaign lease",
        )
        write_attestation(target, record, overwrite=True)

    def write_unsafe_campaign_lease(self, campaign_id: str, *, hours: float = 24.0) -> None:
        """F15: overwrite the campaign sibling with a structurally valid lease whose
        recorded file-quality filter is ENABLED (``build_record`` refuses it, so it is
        written by hand — a manually edited / previously generated unsafe lease)."""
        target = campaign_lease_path(self.lease, campaign_id)
        record = build_record(
            endpoint=SHOKO_BASE,
            version="5.3.3",
            mounts=["/anime read-only (host-inspected)"],
            settings_flags={
                "FileQualityFilterEnabled": False,
                "RenameOnImport": False,
                "MoveOnImport": False,
                "MyListAddFiles": False,
            },
            inspected_iso=self.now_iso,
            expires_iso=_iso_plus(self.now_iso, hours * 3600.0),
            scope=f"campaign:{campaign_id}",
            operator_note="campaign lease",
        )
        canonical = canonical_record(record)
        canonical["settings_summary"] = {
            "FileQualityFilterEnabled": True,
            "RenameOnImport": False,
            "MoveOnImport": False,
            "MyListAddFiles": False,
        }
        target.write_text(
            yaml.safe_dump(canonical, sort_keys=True, allow_unicode=True), encoding="utf-8"
        )

    def campaign_deps(self, store: StateStore, campaign_id: str) -> Any:
        """Daemon deps wired to the campaign-scope lease (the CLI campaign-apply wiring)."""
        settings = load_settings(self.config_path)
        return cli.build_daemon_deps(
            settings,
            store=store,
            mapping_store=cli.open_mapping_store(settings),
            max_submissions=settings.sync.max_new_per_cycle,
            overrides_text=None,
            attestation_scope=f"campaign:{campaign_id}",
        )

    def daemon_deps(self, store: StateStore) -> Any:
        """Production daemon deps: daemon-scope lease + campaign-scope hooks (the CLI wiring)."""
        settings = load_settings(self.config_path)
        return cli.build_daemon_deps(
            settings,
            store=store,
            mapping_store=cli.open_mapping_store(settings),
            max_submissions=settings.sync.max_new_per_cycle,
            overrides_text=cli.read_overrides_text(settings),
        )

    def daemon_cycle(self, store: StateStore, cycle_key: str) -> Any:
        return run_cycle(
            self.daemon_deps(store),
            now_iso=self.now_iso,
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key=cycle_key,
        )


@pytest.fixture
def dual_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DualEnv:
    lease = Path(_tmp_posix(tmp_path, "lease.yaml"))
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(_dual_document(lease.as_posix())), encoding="utf-8")
    environment = DualEnv(tmp_path=tmp_path, stubs=Stubs(), config_path=config_path, lease=lease)
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    monkeypatch.setattr(
        cli,
        "open_state_store",
        lambda settings: StateStore(
            environment.tmp_path / "state.sqlite3",
            budget_limits=cli.budget_limits_from_settings(settings),
        ),
    )
    monkeypatch.setattr(cli, "state_path", lambda settings: environment.tmp_path / "state.sqlite3")
    # attestation_target_path is deliberately NOT patched: the real scope-aware resolver runs.
    monkeypatch.setattr(cli, "current_iso", lambda: environment.now_iso)
    monkeypatch.setattr(cli, "sleep", lambda seconds: None)
    monkeypatch.setattr(cli, "interruptible_sleep", lambda seconds: None)

    def _endpoint(handler: Any, base: str) -> ApiEndpoint:
        client = httpx.Client(
            base_url=base,
            transport=httpx.MockTransport(handler),
            follow_redirects=False,
            timeout=httpx.Timeout(5.0),
        )
        return ApiEndpoint(client, lambda: {})

    monkeypatch.setattr(
        cli, "sonarr_endpoint", lambda settings: _endpoint(environment.stubs.sonarr, SONARR_BASE)
    )
    monkeypatch.setattr(
        cli, "shoko_endpoint", lambda settings: _endpoint(environment.stubs.shoko, SHOKO_BASE)
    )

    def _source(store: Any, settings: Any) -> ApiEndpoint:
        client = httpx.Client(
            base_url="https://raw.githubusercontent.com/",
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, headers={"ETag": '"s"'}, content=_source_xml(), request=request
                )
            ),
            follow_redirects=False,
            timeout=httpx.Timeout(5.0),
        )
        return ApiEndpoint(client, lambda: {"Accept-Encoding": "identity"})

    monkeypatch.setattr(cli, "source_endpoint", _source)
    monkeypatch.setattr(
        cli, "open_mapping_store", lambda settings: MappingStore(environment.tmp_path / "mappings")
    )
    monkeypatch.setattr(cli, "read_overrides_text", lambda settings: None)
    return environment


def _dual_seed_mapping(env: DualEnv) -> None:
    store = MappingStore(env.tmp_path / "mappings")
    settings = load_settings(env.config_path)
    endpoint = cli.source_endpoint(store, settings)
    try:
        mapping_refresh(
            store, endpoint, now=datetime.fromisoformat(T0), refresh_hours=168.0, force=True
        )
    finally:
        endpoint.client.close()


def _dual_prepare(env: DualEnv, capsys: pytest.CaptureFixture[str]) -> dict[str, Any]:
    _dual_seed_mapping(env)
    assert (
        main(
            [
                "campaign",
                "prepare",
                str(env.config_path),
                "--out",
                str(env.tmp_path / "plan.json"),
                "--deadline-hours",
                "168",
                "--json",
            ]
        )
        == EXIT_OK
    )
    return cast("dict[str, Any]", json.loads(capsys.readouterr().out))


def _dual_approve(env: DualEnv, capsys: pytest.CaptureFixture[str], digest: str) -> None:
    assert (
        main(
            [
                "campaign",
                "approve",
                str(env.config_path),
                "--plan",
                str(env.tmp_path / "plan.json"),
                "--digest",
                digest,
                "--json",
            ]
        )
        == EXIT_OK
    )
    capsys.readouterr()


def _dual_canary(env: DualEnv, store: StateStore, capsys: pytest.CaptureFixture[str]) -> None:
    """The operator's manual first batch: one file through the CLI campaign-apply path.

    The CLI apply path resolves the ``campaign:<id>`` scope to the sibling lease.
    """
    assert (
        main(
            [
                "campaign",
                "apply",
                str(env.config_path),
                "--plan",
                str(env.tmp_path / "plan.json"),
                "--batch-size",
                "1",
                "--json",
            ]
        )
        == EXIT_OK
    )
    capsys.readouterr()


def _seed_backlog_and_new(env: DualEnv) -> None:
    """specs 0,1 are baseline backlog (campaign files); specs 2..4 are new arrivals."""
    all_specs = specs(COUNT_FIVE)
    env.stubs.specs = all_specs
    store = env.open_store()
    try:
        store.record_baseline(
            [
                BaselineFile(
                    sonarr_series_id=s.series_id,
                    sonarr_file_id=s.file_id,
                    path=_sonarr_path(s),
                    size=FILE_SIZE,
                )
                for s in all_specs[:2]
            ],
            complete=True,
            auto_apply_eligible=True,
            iso=T0,
        )
    finally:
        store.close()


def _dual_campaign(env: DualEnv, capsys: pytest.CaptureFixture[str]) -> str:
    """Seed a 2-file backlog campaign, approve it, and write its campaign-scope sibling lease."""
    _seed_backlog_and_new(env)
    prepared = _dual_prepare(env, capsys)
    _dual_approve(env, capsys, prepared["approval_digest"])
    campaign_id = cast(str, prepared["campaign_id"])
    env.write_campaign_lease(campaign_id)
    return campaign_id


def test_both_leases_valid_campaign_drains_and_daemon_submits(
    dual_env: DualEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    """F8 acceptance: a daemon lease and a campaign lease are simultaneously valid; the
    campaign drains backlog AND the daemon submits new arrivals in the same cycle."""
    env = dual_env
    campaign_id = _dual_campaign(env, capsys)
    env.write_daemon_lease()
    store = env.open_store()
    try:
        _dual_canary(env, store, capsys)
        authorize_campaign_drain(store, campaign_id, env.now_iso, note=AUTH_NOTE)  # F10
        posts_after_canary = len(env.stubs.posts)
        assert posts_after_canary == 1
        cycle = env.daemon_cycle(store, "both-valid")
        assert cycle.alive is True
        assert cycle.campaign is not None
        assert cycle.campaign.submitted == 1  # backlog 501 drained
        assert cycle.counts.submitted == COUNT_FIVE - 2  # new arrivals 502,503,504
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.total_sent == 2
        assert store.get_meta("attestation_gate") == "valid"
    finally:
        store.close()
    assert set(env.stubs.posted_files) == {500, 501, 502, 503, 504}


def test_daemon_lease_expired_campaign_still_drains(
    dual_env: DualEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    """F8 core: an expired daemon lease pauses only daemon writes.

    An authorized campaign must still drain in the same cycle.
    """
    env = dual_env
    campaign_id = _dual_campaign(env, capsys)
    env.write_daemon_lease()
    store = env.open_store()
    try:
        _dual_canary(env, store, capsys)
        authorize_campaign_drain(store, campaign_id, env.now_iso, note=AUTH_NOTE)  # F10
        # Expire the daemon lease only (campaign lease stays valid), advance the clock.
        env.write_daemon_lease(hours=0.5)
        env.now_iso = _iso_plus(env.now_iso, 3600.0 + 60.0)
        posts_before = len(env.stubs.posts)
        cycle = env.daemon_cycle(store, "daemon-expired")
        assert cycle.alive is True
        assert cycle.apply_eligible is False
        assert cycle.counts.submitted == 0
        assert store.budget_view().paused_reason == ATTESTATION_PAUSE_REASON
        assert store.get_meta("attestation_gate") == "paused"
        # ...and the authorized campaign still drains (backlog 501):
        assert cycle.campaign is not None
        assert cycle.campaign.submitted == 1
        assert len(env.stubs.posts) == posts_before + 1
        assert 501 in env.stubs.posted_files
    finally:
        store.close()


def test_campaign_lease_absent_daemon_still_submits(
    dual_env: DualEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    """F8 core: a campaign with no valid lease defers only that campaign.

    The daemon keeps submitting new arrivals in the same cycle.
    """
    env = dual_env
    campaign_id = _dual_campaign(env, capsys)
    env.write_daemon_lease()
    store = env.open_store()
    try:
        _dual_canary(env, store, capsys)
        authorize_campaign_drain(store, campaign_id, env.now_iso, note=AUTH_NOTE)  # F10
        # Remove the campaign lease (sibling); the daemon lease stays valid.
        campaign_lease_path(env.lease, campaign_id).unlink()
        posts_before = len(env.stubs.posts)
        cycle = env.daemon_cycle(store, "campaign-absent")
        assert cycle.alive is True
        assert cycle.apply_eligible is True
        assert cycle.counts.submitted == COUNT_FIVE - 2
        assert any(reason.startswith("campaign_deferred:") for reason in cycle.reasons)
        assert store.get_meta(f"campaign_attestation_gate:{campaign_id}") == "paused"
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status == "paused"
        assert store.get_meta("attestation_gate") == "valid"
        # only the 3 new arrivals were posted — backlog 501 was NOT sent by the paused campaign.
        assert len(env.stubs.posts) == posts_before + (COUNT_FIVE - 2)
        assert 501 not in env.stubs.posted_files
    finally:
        store.close()


def test_campaign_lease_expired_does_not_globally_pause_daemon(
    dual_env: DualEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    """F8 core: a campaign-lease transition never records the global write pause."""
    env = dual_env
    campaign_id = _dual_campaign(env, capsys)
    env.write_daemon_lease()
    store = env.open_store()
    try:
        _dual_canary(env, store, capsys)
        authorize_campaign_drain(store, campaign_id, env.now_iso, note=AUTH_NOTE)  # F10
        # Expire the campaign lease only (daemon lease stays valid).
        env.write_campaign_lease(campaign_id, hours=0.5)
        env.now_iso = _iso_plus(env.now_iso, 3600.0 + 60.0)
        cycle = env.daemon_cycle(store, "campaign-expired")
        assert cycle.alive is True
        assert store.get_meta(f"campaign_attestation_gate:{campaign_id}") == "paused"
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status == "paused"
        assert campaign.status_reason == ATTESTATION_PAUSE_REASON
        # daemon writes are NOT globally paused: new arrivals submit in the same cycle.
        assert store.budget_view().paused_reason is None
        assert cycle.apply_eligible is True
        assert cycle.counts.submitted == COUNT_FIVE - 2
    finally:
        store.close()


def test_manual_campaign_apply_with_unsafe_lease_performs_zero_posts(
    dual_env: DualEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    """F15: a manual ``campaign apply`` against a structurally valid campaign lease that
    attests the file-quality filter ENABLED is refused (zero POSTs, campaign paused)."""
    env = dual_env
    campaign_id = _dual_campaign(env, capsys)  # a valid lease, then overwritten
    env.write_daemon_lease()
    env.write_unsafe_campaign_lease(campaign_id)
    store = env.open_store()
    try:
        posts_before = len(env.stubs.posts)
        code = main(
            [
                "campaign",
                "apply",
                str(env.config_path),
                "--plan",
                str(env.tmp_path / "plan.json"),
                "--batch-size",
                "1",
                "--json",
            ]
        )
        capsys.readouterr()
        assert code == EXIT_APPLY_GATE_REFUSAL  # attestation refusal is a clean nonzero exit
        assert len(env.stubs.posts) == posts_before  # zero POSTs
        assert env.stubs.posts == []
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status == "paused"
        assert campaign.status_reason == ATTESTATION_PAUSE_REASON
        assert store.get_meta(f"campaign_attestation_gate:{campaign_id}") == "paused"
        # the daemon lease stays valid (scope isolation): no global write pause is armed, and
        # the campaign-apply path never touches the daemon-scope gate meta.
        assert store.budget_view().paused_reason is None
    finally:
        store.close()


def test_campaign_drain_with_unsafe_lease_performs_zero_posts(
    dual_env: DualEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    """F15: automatic campaign draining (daemon drain selection + cycle) with an unsafe
    campaign lease defers the campaign and writes nothing; daemon writes stay gated
    only by their own (valid) lease."""
    env = dual_env
    campaign_id = _dual_campaign(env, capsys)
    env.write_daemon_lease()
    store = env.open_store()
    try:
        _dual_canary(env, store, capsys)  # canary under the valid lease (posts backlog 500)
        authorize_campaign_drain(store, campaign_id, env.now_iso, note=AUTH_NOTE)  # F10
        env.write_unsafe_campaign_lease(campaign_id)  # the campaign lease turns unsafe
        cycle = env.daemon_cycle(store, "campaign-unsafe")
        assert cycle.alive is True
        assert cycle.campaign is None  # campaign deferred (its lease is invalid)
        # Zero campaign POSTs: the pending campaign target (backlog 501) is never sent, while
        # the daemon's OWN new arrivals (502,503,504) still post under the valid daemon lease.
        assert 501 not in env.stubs.posted_files
        assert cycle.counts.submitted == COUNT_FIVE - 2
        assert store.get_meta(f"campaign_attestation_gate:{campaign_id}") == "paused"
        assert store.get_meta("attestation_gate") == "valid"  # daemon lease untouched
        assert store.budget_view().paused_reason is None  # no global write pause
    finally:
        store.close()


def test_both_leases_expired_no_writes_daemon_alive(
    dual_env: DualEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    """F8: with both leases expired nothing is submitted; the daemon stays alive read-only."""
    env = dual_env
    campaign_id = _dual_campaign(env, capsys)
    env.write_daemon_lease(hours=0.5)
    env.write_campaign_lease(campaign_id, hours=0.5)
    store = env.open_store()
    try:
        _dual_canary(env, store, capsys)  # canary at T0 (leases valid at record time)
        authorize_campaign_drain(store, campaign_id, env.now_iso, note=AUTH_NOTE)  # F10
        env.now_iso = _iso_plus(T0, 3600.0 + 60.0)
        posts_before = len(env.stubs.posts)
        cycle = env.daemon_cycle(store, "both-expired")
        assert cycle.alive is True
        assert cycle.counts.submitted == 0
        assert cycle.campaign is None  # campaign deferred (its lease is expired)
        assert len(env.stubs.posts) == posts_before
        assert store.budget_view().paused_reason == ATTESTATION_PAUSE_REASON
        assert store.get_meta("attestation_gate") == "paused"
        assert store.get_meta(f"campaign_attestation_gate:{campaign_id}") == "paused"
    finally:
        store.close()


def test_daemon_and_campaign_leases_survive_restart(
    dual_env: DualEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    """F8: persisted sibling leases give identical verdicts on a fresh loader read after restart."""
    env = dual_env
    campaign_id = _dual_campaign(env, capsys)
    env.write_daemon_lease()
    campaign_lease = campaign_lease_path(env.lease, campaign_id)

    settings = load_settings(env.config_path)
    first_daemon = cli.attestation_verdict_for(settings, scope=SCOPE_DAEMON)
    first_campaign = cli.attestation_verdict_for(settings, scope=f"campaign:{campaign_id}")
    assert first_daemon.valid_for_apply is True
    assert first_campaign.valid_for_apply is True

    # Fresh loader read of the persisted files yields the same verdicts (container restart).
    reloaded_daemon = load_attestation(
        env.lease, shoko_url=SHOKO_BASE, now_iso=T0, scope=SCOPE_DAEMON, warn_lead_seconds=3600.0
    )
    reloaded_campaign = load_attestation(
        campaign_lease,
        shoko_url=SHOKO_BASE,
        now_iso=T0,
        scope=f"campaign:{campaign_id}",
        warn_lead_seconds=3600.0,
    )
    assert reloaded_daemon.valid_for_apply is True
    assert reloaded_campaign.valid_for_apply is True
    assert reloaded_daemon.digest == first_daemon.digest
    assert reloaded_campaign.digest == first_campaign.digest


def test_operator_pause_blocks_campaign_drain(
    dual_env: DualEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    """P1-2: the campaign bypass is origin-gated. A plain operator global pause (reason
    'disk_review', gate meta NOT 'paused') gates every scope, including campaigns."""
    env = dual_env
    campaign_id = _dual_campaign(env, capsys)
    env.write_daemon_lease()
    store = env.open_store()
    try:
        _dual_canary(env, store, capsys)
        authorize_campaign_drain(store, campaign_id, env.now_iso, note=AUTH_NOTE)  # F10
        store.pause("disk_review", env.now_iso)
        posts_before = len(env.stubs.posts)
        cycle = env.daemon_cycle(store, "op-pause")
        assert store.budget_view().paused_reason == "disk_review"
        assert store.get_meta("attestation_gate") != "paused"
        assert cycle.counts.submitted == 0  # daemon gated by the pause
        assert cycle.campaign is None  # campaign drain gated by the pause (no bypass)
        assert len(env.stubs.posts) == posts_before  # zero campaign POSTs
    finally:
        store.close()


def test_attestation_pause_lets_campaign_drain(
    dual_env: DualEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    """P1-2 (preserved): an attestation-gate daemon pause (gate meta 'paused' + reason
    'attestation_expired') lets an authorized campaign drain."""
    env = dual_env
    campaign_id = _dual_campaign(env, capsys)
    env.write_daemon_lease()
    store = env.open_store()
    try:
        _dual_canary(env, store, capsys)
        authorize_campaign_drain(store, campaign_id, env.now_iso, note=AUTH_NOTE)  # F10
        env.write_daemon_lease(hours=0.5)
        env.now_iso = _iso_plus(env.now_iso, 3600.0 + 60.0)
        cycle = env.daemon_cycle(store, "attest-pause")
        assert store.budget_view().paused_reason == ATTESTATION_PAUSE_REASON
        assert store.get_meta("attestation_gate") == "paused"
        assert cycle.campaign is not None
        assert cycle.campaign.submitted == 1  # backlog 501 drains under the origin-gated bypass
    finally:
        store.close()


MAPPING_CHURN_PAUSE_REASON = "mapping_churn"
OPERATOR_PAUSE_REASON = "disk_review"
LEASE_EXPIRY_CYCLE_KEYS = ("churn-expiry", "operator-expiry", "churn-reattested")


def test_daemon_lease_expiry_keeps_mapping_churn_pause_and_blocks_drain(
    dual_env: DualEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    """Cross-cutting review P2 (Finding 1): the daemon-scope valid->invalid transition must NOT
    overwrite an active foreign pause. A mapping-churn pause + daemon lease expiry keeps reason
    'mapping_churn' (gate meta 'paused' is still recorded), so the F8/F9 origin-gated campaign
    bypass stays DISARMED and an authorized campaign drain makes zero POSTs."""
    env = dual_env
    campaign_id = _dual_campaign(env, capsys)
    env.write_daemon_lease()
    store = env.open_store()
    try:
        _dual_canary(env, store, capsys)
        authorize_campaign_drain(store, campaign_id, env.now_iso, note=AUTH_NOTE)  # F10
        store.pause(MAPPING_CHURN_PAUSE_REASON, env.now_iso)  # the churn pause is active
        env.write_daemon_lease(hours=0.5)
        env.now_iso = _iso_plus(env.now_iso, 3600.0 + 60.0)
        posts_before = len(env.stubs.posts)
        cycle = env.daemon_cycle(store, LEASE_EXPIRY_CYCLE_KEYS[0])
        assert store.budget_view().paused_reason == MAPPING_CHURN_PAUSE_REASON  # never relabeled
        assert store.get_meta("attestation_gate") == "paused"  # gate transition IS recorded
        assert cycle.counts.submitted == 0  # daemon gated by the churn pause
        assert cycle.campaign is None  # bypass disarmed: the drain is gated too
        assert any(reason.startswith("campaign_deferred:") for reason in cycle.reasons)
        assert len(env.stubs.posts) == posts_before  # zero campaign POSTs
    finally:
        store.close()


def test_daemon_lease_expiry_keeps_operator_pause_writes_reason(
    dual_env: DualEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    """Finding 1: an operator pause-writes kill switch is never silently relabeled into the
    bypassable 'attestation_expired' reason — campaign drain AND daemon writes stay gated."""
    env = dual_env
    campaign_id = _dual_campaign(env, capsys)
    env.write_daemon_lease()
    store = env.open_store()
    try:
        _dual_canary(env, store, capsys)
        authorize_campaign_drain(store, campaign_id, env.now_iso, note=AUTH_NOTE)  # F10
        assert (
            main(
                [
                    "pause-writes",
                    str(env.config_path),
                    "--reason",
                    OPERATOR_PAUSE_REASON,
                    "--note",
                    "operator kill switch during media review",
                    "--confirm",
                    "--json",
                ]
            )
            == EXIT_OK
        )
        capsys.readouterr()
        env.write_daemon_lease(hours=0.5)
        env.now_iso = _iso_plus(env.now_iso, 3600.0 + 60.0)
        posts_before = len(env.stubs.posts)
        cycle = env.daemon_cycle(store, LEASE_EXPIRY_CYCLE_KEYS[1])
        assert store.budget_view().paused_reason == OPERATOR_PAUSE_REASON  # kill switch intact
        assert store.get_meta("attestation_gate") == "paused"
        assert cycle.counts.submitted == 0  # daemon blocked
        assert cycle.campaign is None  # campaign blocked (no bypass on a foreign reason)
        assert len(env.stubs.posts) == posts_before
    finally:
        store.close()


def test_reattested_lease_clears_gate_without_resuming_foreign_pause(
    dual_env: DualEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    """Finding 1: the invalid->valid transition clears the gate meta but must NEVER resume a
    foreign pause — the churn-paused deployment stays gated (campaign and daemon) after
    re-attestation, and the pause reason is untouched."""
    env = dual_env
    campaign_id = _dual_campaign(env, capsys)
    env.write_daemon_lease()
    store = env.open_store()
    try:
        _dual_canary(env, store, capsys)
        authorize_campaign_drain(store, campaign_id, env.now_iso, note=AUTH_NOTE)  # F10
        store.pause(MAPPING_CHURN_PAUSE_REASON, env.now_iso)
        env.write_daemon_lease(hours=0.5)
        env.now_iso = _iso_plus(env.now_iso, 3600.0 + 60.0)
        posts_before = len(env.stubs.posts)
        expired = env.daemon_cycle(store, "churn-reattested-expiry")
        assert expired.campaign is None
        assert store.get_meta("attestation_gate") == "paused"
        # Re-attest the daemon lease while the churn pause remains active.
        env.now_iso = T0
        env.write_daemon_lease(hours=24.0)
        cycle = env.daemon_cycle(store, "churn-reattested")
        assert store.get_meta("attestation_gate") == "valid"  # gate cleared
        assert store.budget_view().paused_reason == MAPPING_CHURN_PAUSE_REASON  # NOT resumed
        assert "attestation_resumed" not in cycle.reasons
        assert cycle.counts.submitted == 0  # daemon stays gated by the foreign reason
        assert cycle.campaign is None  # campaign stays gated by the foreign reason
        assert len(env.stubs.posts) == posts_before
    finally:
        store.close()


def test_resume_then_lease_expiry_repauses_campaign(
    dual_env: DualEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    """P2-3: after a lease expiry pauses the campaign, a valid-lease resume re-arms the gate
    meta, so the next expiry re-pauses the campaign (no head-of-line APPROVED campaign with a
    dead lease)."""
    env = dual_env
    campaign_id = _dual_campaign(env, capsys)
    env.write_daemon_lease()
    store = env.open_store()
    try:
        _dual_canary(env, store, capsys)
        authorize_campaign_drain(store, campaign_id, env.now_iso, note=AUTH_NOTE)  # F10
        # First expiry: campaign pauses, gate meta "paused".
        env.write_campaign_lease(campaign_id, hours=0.5)
        env.now_iso = _iso_plus(env.now_iso, 3600.0 + 60.0)
        env.daemon_cycle(store, "first-expiry")
        first = store.get_campaign(campaign_id)
        assert first is not None
        assert first.status == "paused"
        assert store.get_meta(f"campaign_attestation_gate:{campaign_id}") == "paused"
        # Re-record a valid lease, then resume: status approved + gate meta cleared to "valid".
        env.now_iso = T0
        env.write_campaign_lease(campaign_id, hours=24.0)
        assert (
            main(
                [
                    "campaign",
                    "resume",
                    str(env.config_path),
                    "--campaign-id",
                    campaign_id,
                    "--note",
                    "re-attested",
                    "--confirm",
                    "--json",
                ]
            )
            == EXIT_OK
        )
        capsys.readouterr()
        resumed = store.get_campaign(campaign_id)
        assert resumed is not None
        assert resumed.status == CAMPAIGN_STATUS_APPROVED
        assert store.get_meta(f"campaign_attestation_gate:{campaign_id}") == "valid"
        # Second expiry: the gate sees "valid" → fresh transition → campaign re-paused, not
        # left APPROVED with a dead lease.
        env.write_campaign_lease(campaign_id, hours=0.5)
        env.now_iso = _iso_plus(T0, 3600.0 + 60.0)
        env.daemon_cycle(store, "second-expiry")
        second = store.get_campaign(campaign_id)
        assert second is not None
        assert second.status == "paused"
        assert store.get_meta(f"campaign_attestation_gate:{campaign_id}") == "paused"
    finally:
        store.close()


def test_campaign_lease_expiry_mid_batch_defers_second_send(
    dual_env: DualEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    """F1: the campaign lease is re-checked per submission. After the first POST the lease
    expires, the second file is DEFERRED (transient BLOCKED_SAFETY) — zero POSTs, NO quarantine,
    the row stays queueable, and a re-attestation re-apply finishes it with no operator
    review."""
    env = dual_env
    campaign_id = _dual_campaign(env, capsys)  # backlog 500,501; valid campaign lease
    store = env.open_store()
    try:
        env.stubs.post_hook = lambda file_id: env.write_expired_campaign_lease(campaign_id)
        report = apply_campaign(
            env.campaign_deps(store, campaign_id),
            load_settings(env.config_path),
            store,
            env.tmp_path / "plan.json",
            env.now_iso,
            batch_size=2,
        )
        assert env.stubs.posted_files == [500]  # first file posted; second never POSTs
        assert report.submitted == 1
        assert report.quarantined == 0  # F1: transient safety is a deferral, not quarantine
        assert report.deferred >= 1
        assert any(reason.startswith("preflight_deferred:501:") for reason in report.reasons)
        # 501 stays queueable (planned), NOT quarantined to blocked_safety.
        record = store.get_file(501)
        assert record is not None
        assert record.state in (FileState.PLANNED, FileState.MAPPED)
        # Re-attestation (fresh valid lease) + re-apply finishes 501, no operator review.
        env.stubs.post_hook = None
        env.write_campaign_lease(campaign_id, hours=24.0)
        second = apply_campaign(
            env.campaign_deps(store, campaign_id),
            load_settings(env.config_path),
            store,
            env.tmp_path / "plan.json",
            env.now_iso,
            batch_size=2,
        )
        assert 501 in env.stubs.posted_files
        assert second.submitted == 1
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.total_sent == 2
    finally:
        store.close()


def test_campaign_gate_clear_reports_accurate_reason(
    dual_env: DualEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    """F4: a valid-lease campaign apply that clears a persisted 'paused' gate meta reports
    'campaign_attestation_gate_cleared', never 'attestation_resumed' (the campaign is not resumed
    by the gate)."""
    env = dual_env
    campaign_id = _dual_campaign(env, capsys)  # approved + valid campaign lease (backlog 500,501)
    store = env.open_store()
    try:
        store.set_meta(f"campaign_attestation_gate:{campaign_id}", "paused")
        report = apply_campaign(
            env.campaign_deps(store, campaign_id),
            load_settings(env.config_path),
            store,
            env.tmp_path / "plan.json",
            env.now_iso,
            batch_size=2,
        )
        assert "campaign_attestation_gate_cleared" in report.reasons
        assert "attestation_resumed" not in report.reasons
        # P3 terminal-state cleanup: this batch drains the 2-file plan to plan_complete, so
        # the gate meta is cleared (removed), not left as a lingering "valid" marker.
        assert store.get_meta(f"campaign_attestation_gate:{campaign_id}") is None
        assert report.stopped == "plan_complete"
    finally:
        store.close()


# ------------------------------------------------------------------ F10: drain authorization
#
# The daemon drains an approved campaign ONLY on the operator's EXPLICIT, digest-bound drain
# authorization (`campaign authorize-drain`), recorded after the operator inspected a
# COMPLETED canary. An ATTEMPTED send (request_may_have_been_sent / verifying / partial /
# indeterminate_write / conflict) never settles the canary: the authorization
# rule requires every SENT campaign file to be VERIFIED, with at least one. Manual
# ``campaign apply`` never requires the authorization.

STALE_DIGEST = "1" * DIGEST_HEX
F10_CYCLE_KEY = "f10-cycle"


def _authorize(env: Env, store: StateStore, campaign_id: str) -> CampaignRecord:
    return _authorize_drain(env, store, campaign_id)


def _f10_campaign(env: Env, capsys: pytest.CaptureFixture[str]) -> str:
    """Seed/approve a 5-file campaign (batch 2) and return its id."""
    _set_campaign(env, max_batch_size=BATCH_TWO)
    env.seed(specs(COUNT_FIVE))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    return cast(str, prepared["campaign_id"])


def test_daemon_does_not_drain_verified_canary_without_authorization(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F10 core: a completed (verified) canary alone never opens the automatic drain."""
    campaign_id = _f10_campaign(env, capsys)
    store = env.open_store()
    try:
        canary = _canary(env, store, campaign_id)
        assert canary.verified == COUNT_ONE
        cycle = _daemon_cycle(env, store, campaign_id, "unauthorized")
        assert cycle.campaign is None  # no operator authorization → no drain
        assert cycle.counts.submitted == 0
        assert len(env.stubs.posts) == COUNT_ONE  # only the canary POSTed
        report = campaign_report(store, campaign_id, None)
        assert report["drain_selection"] == "drain_not_authorized"
        assert report["drain_authorized_iso"] is None
    finally:
        store.close()


def test_authorize_drain_refuses_before_any_canary(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F10: zero send progress means there is no completed canary to confirm."""
    campaign_id = _f10_campaign(env, capsys)
    store = env.open_store()
    try:
        with pytest.raises(CampaignError, match="canary"):
            _authorize(env, store, campaign_id)
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.drain_authorized_iso is None
    finally:
        store.close()


def test_authorize_drain_refuses_attempted_unverified_canary(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F10: an attempted send that never verified (verifying) does not settle the canary."""
    campaign_id = _f10_campaign(env, capsys)
    env.stubs.verify_empty = {spec.shoko_id for spec in specs(COUNT_FIVE)}
    store = env.open_store()
    try:
        canary = _canary(env, store, campaign_id)
        assert canary.submitted == COUNT_ONE
        record = store.get_file(500)
        assert record is not None
        assert record.state is FileState.VERIFYING
        with pytest.raises(CampaignError, match="not settled"):
            _authorize(env, store, campaign_id)
        cycle = _daemon_cycle(env, store, campaign_id, "attempted")
        assert cycle.campaign is None
        assert len(env.stubs.posts) == COUNT_ONE
    finally:
        store.close()


def test_authorize_drain_refuses_indeterminate_canary(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F10: an indeterminate_write canary is an uninspected side-effect risk — refused."""
    campaign_id = _f10_campaign(env, capsys)
    env.stubs.verify_empty = {spec.shoko_id for spec in specs(COUNT_FIVE)}
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)
        store.advance(500, FileState.INDETERMINATE_WRITE, "ambiguous send", iso=env.now_iso)
        with pytest.raises(CampaignError, match="indeterminate_write"):
            _authorize(env, store, campaign_id)
        cycle = _daemon_cycle(env, store, campaign_id, "indeterminate")
        assert cycle.campaign is None
    finally:
        store.close()


def test_authorize_drain_refuses_partial_and_conflict_canary(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F10 strict rule: partial (incomplete link) and conflict rows are NOT settled."""
    campaign_id = _f10_campaign(env, capsys)
    env.stubs.verify_empty = {spec.shoko_id for spec in specs(COUNT_FIVE)}
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)
        store.advance(500, FileState.PARTIAL, "partial link", iso=env.now_iso)
        with pytest.raises(CampaignError, match="partial"):
            _authorize(env, store, campaign_id)
        store.advance(500, FileState.CONFLICT, "contradiction", iso=env.now_iso)
        with pytest.raises(CampaignError, match="conflict"):
            _authorize(env, store, campaign_id)
    finally:
        store.close()


def test_authorize_drain_refuses_paused_revoked_or_unknown_campaign(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F10: authorization is only for an approved campaign that exists."""
    campaign_id = _f10_campaign(env, capsys)
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)
        with pytest.raises(CampaignError, match="does not exist"):
            authorize_campaign_drain(
                store, "00000000-0000-0000-0000-000000000000", T0, note=AUTH_NOTE
            )
        store.set_campaign_status(campaign_id, "paused", "ops review", iso=env.now_iso)
        with pytest.raises(CampaignError, match="paused"):
            _authorize(env, store, campaign_id)
        store.set_campaign_status(
            campaign_id, CAMPAIGN_STATUS_REVOKED, "scope review", iso=env.now_iso
        )
        with pytest.raises(CampaignError, match="revoked"):
            _authorize(env, store, campaign_id)
    finally:
        store.close()


def test_authorized_verified_canary_opens_bounded_daemon_drain(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F10 core fix: the explicit confirmation after a verified canary drains the rest."""
    campaign_id = _f10_campaign(env, capsys)
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)
        authorized = _authorize(env, store, campaign_id)
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert authorized.drain_authorized_iso == env.now_iso
        assert authorized.drain_authorized_digest == campaign.digest
        assert campaign_drain_selection(campaign) == "authorized"
        first = _daemon_cycle(env, store, campaign_id, "auth-1")
        assert first.campaign is not None
        assert first.campaign.submitted == BATCH_TWO
        second = _daemon_cycle(env, store, campaign_id, "auth-2")
        assert second.campaign is not None
        assert second.campaign.submitted == BATCH_TWO
        after = store.get_campaign(campaign_id)
        assert after is not None
        assert after.total_sent == COUNT_FIVE
        assert len(env.stubs.posts) == COUNT_FIVE
        report = campaign_report(store, campaign_id, None)
        assert report["drain_selection"] == "authorized"
    finally:
        store.close()


def test_drain_authorization_survives_store_restart(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F10: authorization is durable SQLite state — a reopen keeps the campaign drainable."""
    campaign_id = _f10_campaign(env, capsys)
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)
        _authorize(env, store, campaign_id)
    finally:
        store.close()
    reopened = env.open_store()
    try:
        selected = select_next_daemon_campaign(reopened)
        assert selected is not None
        assert selected.campaign_id == campaign_id
        assert selected.drain_authorized_iso == T0
        cycle = _daemon_cycle(env, reopened, campaign_id, "post-restart")
        assert cycle.campaign is not None
        assert cycle.campaign.submitted == BATCH_TWO
    finally:
        reopened.close()


def test_revoke_drain_stops_further_daemon_submissions(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F10: the inverse command withdraws the authorization; the campaign leaves selection."""
    campaign_id = _f10_campaign(env, capsys)
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)
        _authorize(env, store, campaign_id)
        drained = _daemon_cycle(env, store, campaign_id, "auth-1")
        assert drained.campaign is not None
        posts_before = len(env.stubs.posts)
        revoke_campaign_drain(store, campaign_id, env.now_iso, note="side effects under review")
        cycle = _daemon_cycle(env, store, campaign_id, "revoked")
        assert cycle.campaign is None
        assert len(env.stubs.posts) == posts_before  # zero further submissions
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.drain_authorized_iso is None
        assert campaign_drain_selection(campaign) == "drain_revoked"
        report = campaign_report(store, campaign_id, None)
        assert report["drain_selection"] == "drain_revoked"
    finally:
        store.close()


def test_plan_digest_change_invalidates_drain_authorization(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F10: the authorization is bound to the plan digest — a changed fingerprint set skips it."""
    campaign_id = _f10_campaign(env, capsys)
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)
        _authorize(env, store, campaign_id)
        conn = sqlite3.connect(str(store.path))
        conn.execute(
            "UPDATE campaigns SET digest = ? WHERE campaign_id = ?", (STALE_DIGEST, campaign_id)
        )
        conn.commit()
        conn.close()
        cycle = _daemon_cycle(env, store, campaign_id, "digest-changed")
        assert cycle.campaign is None  # stale authorization → skipped, not drained
        assert len(env.stubs.posts) == COUNT_ONE
        report = campaign_report(store, campaign_id, None)
        assert report["drain_selection"] == "drain_digest_stale"
    finally:
        store.close()


def _tamper_plan_file_canonical(plan_path: Path) -> tuple[bytes, str]:
    """Structurally rewrite a canonical plan file and return ``(original_bytes, new_digest)``.

    The edit changes ``totals.max_submissions`` (lowered below the approved file count)
    together with the bound file set (the plan schema requires
    ``totals.files == totals.max_submissions == len(files)``, so an inconsistent document
    would hit the invalid-plan path instead of the digest gate). The file is written back
    as byte-exact canonical bytes via ``write_bytes`` — no ``write_text`` newline
    translation — so the byte/digest change is identical on Windows and Linux, and the
    assertions here fail loudly if the tamper is ever a no-op.
    """
    original_bytes = plan_path.read_bytes()
    document = json.loads(plan_path.read_text(encoding="utf-8"))
    totals = document["totals"]
    assert "max_submissions" in totals
    original_max = totals["max_submissions"]
    tampered_max = original_max - 1
    assert tampered_max != original_max  # a real structural change, not a no-op edit
    document["files"] = document["files"][:tampered_max]
    totals["max_submissions"] = tampered_max
    totals["files"] = tampered_max
    tampered_bytes = (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )
    plan_path.write_bytes(tampered_bytes)
    # (a) the file bytes changed and (b) the canonical file digest changed.
    file_bytes = plan_path.read_bytes()
    assert file_bytes != original_bytes
    tampered_digest = hashlib.sha256(file_bytes).hexdigest()
    assert tampered_digest != hashlib.sha256(original_bytes).hexdigest()
    # guard: the tampered file is byte-exact canonical JSON (no newline translation),
    # so its digest is stable under re-serialization — a future write_text() newline
    # regression on Windows would make this comparison fail.
    reparsed = json.loads(file_bytes.decode("utf-8"))
    canonical_again = (json.dumps(reparsed, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )
    assert canonical_again == file_bytes
    assert hashlib.sha256(canonical_again).hexdigest() == tampered_digest
    return original_bytes, tampered_digest


def test_campaign_apply_refuses_authorized_plan_drift_with_zero_posts(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F10: a bound plan file rewritten after authorization is plan_drift —
    the campaign pauses with zero POSTs (the authorization never authorizes a plan change).

    The tamper is STRUCTURAL (``totals.max_submissions`` 5→4 with the bound file set)
    written back as byte-exact canonical bytes — platform-independent (no newline
    translation), with explicit byte/digest-change assertions before the cycle, so the
    drift is real on every OS, not a platform artifact."""
    campaign_id = _f10_campaign(env, capsys)
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)
        _authorize(env, store, campaign_id)
        plan_path = env.tmp_path / "plan.json"
        approved = store.get_campaign(campaign_id)
        assert approved is not None
        plan_document = json.loads(plan_path.read_text(encoding="utf-8"))
        assert plan_document["totals"]["max_submissions"] == COUNT_FIVE
        original_bytes, tampered_digest = _tamper_plan_file_canonical(plan_path)
        original_digest = hashlib.sha256(original_bytes).hexdigest()
        # the approval bind is the file digest; (c) the new digest differs from it.
        assert original_digest == approved.digest
        assert tampered_digest != approved.digest
        posts_before = len(env.stubs.posts)
        assert posts_before == COUNT_ONE  # only the canary POSTed
        sends_before = len(store.campaign_send_history(campaign_id))
        cycle = _daemon_cycle(env, store, campaign_id, "plan-changed")
        assert cycle.campaign is None
        assert len(env.stubs.posts) == posts_before
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status == "paused"
        assert campaign.status_reason == "plan_drift"
        # the verified canary is untouched: VERIFIED file, unchanged sends.
        canary_record = store.get_file(500)
        assert canary_record is not None
        assert canary_record.state is FileState.VERIFIED
        assert len(store.campaign_send_history(campaign_id)) == sends_before
        assert campaign.total_sent == COUNT_ONE
        # the changed plan file cannot be bypassed by the drain authorization.
        assert campaign_drain_selection(campaign) == "status_paused"
        assert select_next_daemon_campaign(store) is None
        report = campaign_report(store, campaign_id, None)
        assert report["drain_selection"] == "status_paused"
        after = _daemon_cycle(env, store, campaign_id, "plan-changed-2")
        assert after.campaign is None
        assert len(env.stubs.posts) == posts_before
    finally:
        store.close()


def test_manual_apply_never_requires_drain_authorization(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F10 boundary: manual batch commands stay unchanged (no authorization needed)."""
    campaign_id = _f10_campaign(env, capsys)
    result = apply(env, capsys, batch="2")
    assert result["exit"] == EXIT_OK
    assert result["payload"]["submitted"] == BATCH_TWO
    store = env.open_store()
    try:
        report = campaign_report(store, campaign_id, None)
        assert report["drain_selection"] == "drain_not_authorized"
        cycle = _daemon_cycle(env, store, campaign_id, "manual-only")
        assert cycle.campaign is None  # manual batches do not authorize the drain
    finally:
        store.close()


def test_cli_authorize_drain_requires_confirm_and_nonempty_note(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F10 CLI: the authorization is an operator command — confirm + a real note required."""
    campaign_id = _f10_campaign(env, capsys)
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)
    finally:
        store.close()
    base = ["campaign", "authorize-drain", str(env.config_path), "--campaign-id", campaign_id]
    capsys.readouterr()
    assert main([*base, "--note", "reviewed", "--json"]) == EXIT_CONFIG_INVALID
    assert "--confirm" in capsys.readouterr().out
    assert main([*base, "--note", "", "--confirm", "--json"]) == EXIT_CONFIG_INVALID
    assert "note" in capsys.readouterr().out
    assert main([*base, "--note", AUTH_NOTE, "--confirm", "--json"]) == EXIT_OK
    payload = json.loads(capsys.readouterr().out)
    assert payload["result"]["drain_authorized"] is True
    store = env.open_store()
    try:
        report = campaign_report(store, campaign_id, None)
        assert report["drain_selection"] == "authorized"
        actions = json.loads(str(store.get_meta("operator_actions")))
        assert any(entry["command"] == "campaign-authorize-drain" for entry in actions)
        cycle = _daemon_cycle(env, store, campaign_id, "cli-authorized")
        assert cycle.campaign is not None
        assert cycle.campaign.submitted == BATCH_TWO
    finally:
        store.close()


def test_cli_revoke_drain_requires_confirm_and_clears_authorization(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F10 CLI: revoke-drain is the confirm-gated inverse and is audited like other operators."""
    campaign_id = _f10_campaign(env, capsys)
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)
        _authorize(env, store, campaign_id)
    finally:
        store.close()
    base = ["campaign", "revoke-drain", str(env.config_path), "--campaign-id", campaign_id]
    capsys.readouterr()
    assert main([*base, "--note", "review", "--json"]) == EXIT_CONFIG_INVALID
    capsys.readouterr()
    assert main([*base, "--note", "side effects under review", "--confirm", "--json"]) == EXIT_OK
    payload = json.loads(capsys.readouterr().out)
    assert payload["result"]["drain_authorized"] is False
    store = env.open_store()
    try:
        report = campaign_report(store, campaign_id, None)
        assert report["drain_selection"] == "drain_revoked"
        actions = json.loads(str(store.get_meta("operator_actions")))
        assert any(entry["command"] == "campaign-revoke-drain" for entry in actions)
    finally:
        store.close()


def test_fingerprint_set_refresh_clears_drain_authorization(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F10: a plan-fingerprint-set refresh (the state bind path) invalidates the grant."""
    campaign_id = _f10_campaign(env, capsys)
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)
        _authorize(env, store, campaign_id)
        # A refresh writes a fingerprint set that no longer matches the campaign's bindings
        # (non-canonical stored rows are the observable drift at the bind path).
        fingerprint = "a" * DIGEST_HEX
        conn = sqlite3.connect(str(store.path))
        conn.execute(
            "UPDATE campaigns SET plan_fingerprint_set = ? WHERE campaign_id = ?",
            (f'[[500,"{fingerprint}","{fingerprint}"]]', campaign_id),
        )
        conn.commit()
        conn.close()
        store.bind_campaign_files(campaign_id, env.now_iso)  # the refresh rewrites the set
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.drain_authorized_iso is None
        assert campaign.drain_invalidated_reason == CAMPAIGN_DRAIN_FINGERPRINT_REASON
        report = campaign_report(store, campaign_id, None)
        assert report["drain_selection"] == "drain_not_authorized"
        assert report["drain_invalidated_reason"] == CAMPAIGN_DRAIN_FINGERPRINT_REASON
        cycle = _daemon_cycle(env, store, campaign_id, "after-refresh")
        assert cycle.campaign is None  # re-grant is the operator's decision after review
    finally:
        store.close()


def test_campaign_report_surfaces_authorization_identity(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F10: report/status shows who authorized what digest, and why selection skips."""
    campaign_id = _f10_campaign(env, capsys)
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        _authorize(env, store, campaign_id)
        report = campaign_report(store, campaign_id, None)
        assert report["drain_selection"] == "authorized"
        assert report["drain_authorized_iso"] == env.now_iso
        assert report["drain_authorized_digest"] == campaign.digest
    finally:
        store.close()
    code = main(
        [
            "campaign",
            "report",
            str(env.config_path),
            "--campaign-id",
            campaign_id,
        ]
    )
    assert code == EXIT_OK
    out = capsys.readouterr().out
    assert "drain authorization: authorized" in out


# ------------------------------------------------------------------ F11: per-submission freshness


def _extra_source_xml() -> bytes:
    """A mapping document that changes the target/structure digests (a new generation)."""
    return _source_xml().replace(
        b"</anime-list>",
        b"<anime anidbid='999' tvdbid='11111' defaulttvdbseason='1'>"
        b"<mapping-list><mapping anidbseason='1' tvdbseason='1'>;1-1;</mapping></mapping-list>"
        b"</anime></anime-list>",
    )


def _swap_mapping_generation(env: Env) -> None:
    """Activate a NEW mapping generation on disk (what a concurrent refresh does mid-batch)."""
    store = MappingStore(env.tmp_path / "mappings")
    client = httpx.Client(
        base_url="https://raw.githubusercontent.com/",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                200, headers={"ETag": '"s"'}, content=_extra_source_xml(), request=request
            )
        ),
        follow_redirects=False,
        timeout=httpx.Timeout(5.0),
    )
    endpoint = ApiEndpoint(client, lambda: {"Accept-Encoding": "identity"})
    try:
        mapping_refresh(
            store, endpoint, now=datetime.fromisoformat(T0), refresh_hours=168.0, force=True
        )
    finally:
        endpoint.client.close()


def test_f11_deadline_expires_mid_batch_stops_later_sends(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F11: the deadline is re-evaluated with a FRESH clock before each POST. A deadline that
    passes mid-batch auto-pauses (``deadline_exceeded``), the remainder stays queueable (no
    quarantine), and zero POSTs follow the batch-start authorization."""
    _set_campaign(env, max_batch_size=COUNT_TWO)
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys, deadline_hours="1")
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]
    store = env.open_store()
    try:

        def _past_deadline(file_id: int) -> None:
            env.now_iso = _iso_plus(T0, 2 * 3600.0)  # fake clock: deadline passed during file 500

        env.stubs.post_hook = _past_deadline
        report = apply_campaign(
            _campaign_deps(env, store, campaign_id),
            load_settings(env.config_path),
            store,
            env.tmp_path / "plan.json",
            T0,
            batch_size=COUNT_TWO,
        )
        assert env.stubs.posted_files == [500]  # the deadline never authorizes file 501
        assert report.submitted == 1
        assert report.quarantined == 0  # a stale authorization defers, never quarantines
        assert any(r.startswith("send_stale:501:deadline_exceeded") for r in report.reasons)
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status == "paused"
        assert campaign.status_reason == "deadline_exceeded"
        record = store.get_file(501)
        assert record is not None
        assert record.state is FileState.PLANNED  # remainder stays queueable
        assert len(env.stubs.posts) == 1
    finally:
        store.close()


def test_f11_operator_pause_mid_batch_stops_later_sends(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F11: campaign status is re-read from the store before each POST. A concurrent
    operator pause (second store handle) stops the batch with zero POSTs after the change."""
    _set_campaign(env, max_batch_size=COUNT_TWO)
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]
    store = env.open_store()
    try:

        def _pause(file_id: int) -> None:
            other = env.open_store()  # the concurrent CLI writing the same SQLite DB
            try:
                other.set_campaign_status(campaign_id, "paused", "operator_paused", iso=T0)
            finally:
                other.close()

        env.stubs.post_hook = _pause
        report = apply_campaign(
            _campaign_deps(env, store, campaign_id),
            load_settings(env.config_path),
            store,
            env.tmp_path / "plan.json",
            T0,
            batch_size=COUNT_TWO,
        )
        assert env.stubs.posted_files == [500]
        assert report.submitted == 1
        assert report.quarantined == 0
        assert any(r.startswith("send_stale:501:campaign_paused") for r in report.reasons)
        record = store.get_file(501)
        assert record is not None
        assert record.state is FileState.PLANNED
        assert len(env.stubs.posts) == 1
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status_reason == "operator_paused"
    finally:
        store.close()


def test_f11_shutdown_flag_stops_campaign_batch_without_replaying_the_sent_row(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F11: the batch loop honors the shutdown flag before starting a new POST; the deferred
    remainder stays queueable and a restart finishes the batch WITHOUT re-POSTing the sent file."""
    _set_campaign(env, max_batch_size=COUNT_TWO)
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]
    store = env.open_store()
    try:
        shutdown = {"value": False}
        env.stubs.post_hook = lambda file_id: shutdown.__setitem__("value", True)
        deps = replace(
            _campaign_deps(env, store, campaign_id),
            shutdown_requested=lambda: shutdown["value"],
        )
        report = apply_campaign(
            deps,
            load_settings(env.config_path),
            store,
            env.tmp_path / "plan.json",
            T0,
            batch_size=COUNT_TWO,
        )
        assert report.submitted == 1
        assert env.stubs.posted_files == [500]
        # The batch-loop shutdown check fires before the next preflight (no network reads
        # during shutdown); the deferral is non-destructive either way.
        assert any(r.startswith("shutdown_requested:501") for r in report.reasons)
        record = store.get_file(501)
        assert record is not None
        assert record.state is FileState.PLANNED  # non-destructive deferral
        # Restart with the shutdown cleared: the sent row is durably tracked and
        # never replayed; only the deferred file POSTs.
        env.stubs.post_hook = None
        shutdown["value"] = False
        second = apply_campaign(
            deps,
            load_settings(env.config_path),
            store,
            env.tmp_path / "plan.json",
            T0,
            batch_size=COUNT_TWO,
        )
        assert second.submitted == 1
        assert sum(1 for posted in env.stubs.posted_files if posted == 500) == 1  # no replay
        assert 501 in env.stubs.posted_files
    finally:
        store.close()


def test_f11_mapping_generation_swap_mid_batch_defers_and_stops(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F11: the guard-time generation identity is re-checked per submission. A concurrent
    mapping refresh mid-batch stops the batch (defer, queueable) — no POST rides the stale
    digest equality."""
    _set_campaign(env, max_batch_size=COUNT_TWO)
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]
    store = env.open_store()
    try:
        env.stubs.post_hook = lambda file_id: _swap_mapping_generation(env)
        report = apply_campaign(
            _campaign_deps(env, store, campaign_id),
            load_settings(env.config_path),
            store,
            env.tmp_path / "plan.json",
            T0,
            batch_size=COUNT_TWO,
        )
        assert env.stubs.posted_files == [500]
        assert report.submitted == 1
        assert report.quarantined == 0
        assert any(
            r.startswith("send_stale:501:mapping_generation_changed") for r in report.reasons
        )
        record = store.get_file(501)
        assert record is not None
        assert record.state is FileState.PLANNED
        assert len(env.stubs.posts) == 1
    finally:
        store.close()


def test_f11_daemon_drain_mid_batch_revoke_stops_later_posts(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """Cross-cutting review P2 (Finding 2): ``campaign revoke-drain`` fired DURING the first
    drain POST stops the RUNNING batch at the next submission. The daemon drain path reconsults
    the F10 authorization per submission — zero campaign POSTs land after the operator's stop
    command, the remainder stays queueable (no quarantine), and the report names the revocation."""
    campaign_id = _f10_campaign(env, capsys)  # 5 files, max_batch_size 2
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)  # file 500: the manual canary
        _authorize(env, store, campaign_id)
        posts_before = len(env.stubs.posts)

        def _revoke(file_id: int) -> None:
            other = env.open_store()  # the concurrent operator CLI (campaign revoke-drain)
            try:
                revoke_campaign_drain(
                    other, campaign_id, env.now_iso, note="side effects under review"
                )
            finally:
                other.close()

        env.stubs.post_hook = _revoke  # fires between the 501 POST and the 502 attempt
        cycle = _daemon_cycle(env, store, campaign_id, "mid-batch-revoke")
        env.stubs.post_hook = None
        assert cycle.campaign is not None
        assert cycle.campaign.submitted == BATCH_ONE  # only 501 rode the pre-revoke grant
        assert len(env.stubs.posts) == posts_before + BATCH_ONE  # ZERO POSTs after the revoke
        assert 502 not in env.stubs.posted_files[posts_before:]
        assert any("drain_authorization_revoked" in reason for reason in cycle.campaign.reasons)
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status == CAMPAIGN_STATUS_APPROVED  # revoke withdraws the grant only
        assert campaign_drain_selection(campaign) == "drain_revoked"
        record = store.get_file(502)
        assert record is not None
        assert record.state is FileState.PLANNED  # the remainder stays queueable
        # The queuehead resumes after a fresh authorization — the deferral was non-destructive.
        _authorize(env, store, campaign_id)
        resumed = _daemon_cycle(env, store, campaign_id, "mid-batch-revoke-regrant")
        assert resumed.campaign is not None
        assert resumed.campaign.submitted == BATCH_TWO  # 502 + 503 drain
        assert 502 in env.stubs.posted_files
    finally:
        store.close()


def test_f11_manual_apply_stays_exempt_after_drain_revocation(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """Finding 2 boundary: the per-submission authorization recheck is daemon-drain-only. A
    revoked drain authorization never blocks a manual ``campaign apply`` batch."""
    campaign_id = _f10_campaign(env, capsys)
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)
        _authorize(env, store, campaign_id)
        revoke_campaign_drain(store, campaign_id, env.now_iso, note="side effects under review")
        posts_before = len(env.stubs.posts)
        report = apply_campaign(
            _campaign_deps(env, store, campaign_id),
            load_settings(env.config_path),
            store,
            env.tmp_path / "plan.json",
            env.now_iso,
            batch_size=BATCH_TWO,
        )
        assert report.submitted == BATCH_TWO  # manual apply stays authorized
        assert not any("drain_authorization_revoked" in reason for reason in report.reasons)
        assert len(env.stubs.posts) == posts_before + BATCH_TWO
    finally:
        store.close()


def test_f11_daemon_drain_stops_at_next_cycle_after_revoke(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """Finding 2 (selection path, preserved): a revoke that lands BETWEEN drain cycles keeps the
    existing selection-time stop — the campaign drops out with zero campaign POSTs."""
    campaign_id = _f10_campaign(env, capsys)
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)
        _authorize(env, store, campaign_id)
        posts_before = len(env.stubs.posts)
        revoke_campaign_drain(store, campaign_id, env.now_iso, note="side effects under review")
        cycle = _daemon_cycle(env, store, campaign_id, "between-cycles-revoke")
        assert cycle.campaign is None  # selection-time skip (drain_revoked)
        assert len(env.stubs.posts) == posts_before
    finally:
        store.close()


def test_campaign_revoke_clears_drain_bookkeeping_metas(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """P3 (bounded cleanup): a revoked campaign sheds its ``campaign_attestation_gate:<id>`` and
    ``campaign_zero_progress:<id>`` metas; campaign semantics are otherwise untouched."""
    campaign_id = _f10_campaign(env, capsys)
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)
        _authorize(env, store, campaign_id)
        _daemon_cycle(env, store, campaign_id, "meta-before-revoke")
        store.set_meta(f"campaign_zero_progress:{campaign_id}", "2")
        assert store.get_meta(f"campaign_attestation_gate:{campaign_id}") is not None
        campaign_revoke(store, campaign_id, env.now_iso)
        assert store.get_meta(f"campaign_attestation_gate:{campaign_id}") is None
        assert store.get_meta(f"campaign_zero_progress:{campaign_id}") is None
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status == CAMPAIGN_STATUS_REVOKED  # semantics preserved
    finally:
        store.close()


def test_plan_complete_clears_campaign_drain_bookkeeping_metas(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """P3: a campaign drained to ``plan_complete`` clears its gate/zero-progress metas, so a
    finished campaign leaves no per-campaign bookkeeping behind."""
    campaign_id = _f10_campaign(env, capsys)  # 5 files: 1 canary + 2 + 2
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)
        _authorize(env, store, campaign_id)
        _daemon_cycle(env, store, campaign_id, "complete-1")
        assert store.get_meta(f"campaign_attestation_gate:{campaign_id}") is not None
        assert store.get_meta(f"campaign_zero_progress:{campaign_id}") is not None
        last = _daemon_cycle(env, store, campaign_id, "complete-2")
        assert last.campaign is not None
        assert last.campaign.stopped == "plan_complete"
        assert store.get_meta(f"campaign_attestation_gate:{campaign_id}") is None
        assert store.get_meta(f"campaign_zero_progress:{campaign_id}") is None
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.total_sent == COUNT_FIVE  # progress truth preserved
    finally:
        store.close()


def test_f11_send_rows_use_fresh_timestamps_per_submission(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F11: submission timestamps (prepare_send intent / attempted anchor) come from the fresh
    clock per submission, never the batch-start snapshot."""
    _set_campaign(env, max_batch_size=COUNT_THREE)
    _set_sync(
        env, max_new_per_cycle=COUNT_THREE, max_pending_episode_jobs=100, rolling_new_limit=1000
    )
    env.seed(specs(COUNT_THREE))
    prepared = prepare(env, capsys, limit=str(COUNT_THREE))
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]
    store = env.open_store()
    try:
        ticks = {"n": 0}

        def _now() -> str:
            ticks["n"] += 1
            return _iso_plus(T0, 60.0 * ticks["n"])

        deps = replace(_campaign_deps(env, store, campaign_id), now=_now)
        report = apply_campaign(
            deps,
            load_settings(env.config_path),
            store,
            env.tmp_path / "plan.json",
            T0,
            batch_size=COUNT_THREE,
        )
        assert report.submitted == COUNT_THREE
        rows = store._conn.execute(
            "SELECT shoko_file_id, attempted_iso FROM sends ORDER BY send_id"
        ).fetchall()
        stamps = [str(row[1]) for row in rows]
        assert len(stamps) == COUNT_THREE
        assert stamps == sorted(stamps)
        assert len(set(stamps)) == COUNT_THREE  # strictly increasing: fresh time per submission
        assert stamps[0] != T0  # never the batch-start timestamp
    finally:
        store.close()


# ----- P2-1: authorization expiring DURING the preflight network phase (slow GETs) -----


def test_f11_shutdown_during_preflight_network_phase_stops_the_post(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """P2-1: preflight's network phase (File + per-episode GETs) is slow. A shutdown that lands
    DURING it must not authorize the POST: the local freshness re-evaluation runs after preflight
    and before ``prepare_send``, defers the file non-destructively, and stops the batch — zero
    POSTs."""
    _set_campaign(env, max_batch_size=COUNT_TWO)
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]
    store = env.open_store()
    try:
        shutdown = {"value": False}

        def _flip_on_preflight_read(path: str) -> None:
            if path.startswith(f"/api/v3/File/{500}"):
                shutdown["value"] = True  # SIGTERM lands mid-preflight of file 500

        env.stubs.get_hook = _flip_on_preflight_read
        deps = replace(
            _campaign_deps(env, store, campaign_id),
            shutdown_requested=lambda: shutdown["value"],
        )
        report = apply_campaign(
            deps,
            load_settings(env.config_path),
            store,
            env.tmp_path / "plan.json",
            T0,
            batch_size=COUNT_TWO,
        )
        env.stubs.get_hook = None
        assert env.stubs.posts == []  # zero POSTs: the expired authorization never sent
        assert report.submitted == 0
        assert report.quarantined == 0
        assert any(r.startswith("send_stale:500:shutdown_requested") for r in report.reasons)
        record = store.get_file(500)
        assert record is not None
        assert record.state is FileState.PLANNED  # non-destructive deferral
        record2 = store.get_file(501)
        assert record2 is not None
        assert record2.state is FileState.PLANNED
        # Nothing was durably in-flight: no send rows exist to replay.
        rows = store._conn.execute("SELECT COUNT(*) FROM sends").fetchone()
        assert int(rows[0]) == 0
    finally:
        store.close()


def test_f11_lease_expiry_during_preflight_defers_without_post(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """P2-1: a campaign lease that expires DURING preflight's network phase must not ride the
    pre-preflight verdict into a POST. The lease/gate verdict is re-taken locally
    before ``prepare_send`` (transient safety: defer, no quarantine, no batch stop), and a
    re-attestation finishes the file with zero replay."""
    _set_campaign(env, max_batch_size=COUNT_TWO)
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]
    store = env.open_store()
    try:

        def _expire_on_preflight_read(path: str) -> None:
            if path.startswith(f"/api/v3/File/{500}"):
                env.write_campaign_lease(campaign_id, expired=True)  # lease dies mid-preflight

        env.stubs.get_hook = _expire_on_preflight_read
        report = apply_campaign(
            _campaign_deps(env, store, campaign_id),
            load_settings(env.config_path),
            store,
            env.tmp_path / "plan.json",
            T0,
            batch_size=COUNT_TWO,
        )
        env.stubs.get_hook = None
        assert 500 not in env.stubs.posted_files  # the expired lease never authorized a POST
        assert report.quarantined == 0  # F8 semantics: transient safety defers
        assert any(r.startswith("preflight_deferred:500:blocked_safety") for r in report.reasons)
        record = store.get_file(500)
        assert record is not None
        assert record.state is FileState.PLANNED
        rows = store._conn.execute(
            "SELECT COUNT(*) FROM sends WHERE shoko_file_id = 500"
        ).fetchone()
        assert int(rows[0]) == 0  # nothing durably in-flight for the deferred file
        # Re-attestation: the never-sent files complete normally (exactly one POST each).
        env.write_campaign_lease(campaign_id, hours=24.0)
        second = apply_campaign(
            _campaign_deps(env, store, campaign_id),
            load_settings(env.config_path),
            store,
            env.tmp_path / "plan.json",
            T0,
            batch_size=COUNT_TWO,
        )
        assert second.submitted == COUNT_TWO
        assert env.stubs.posted_files.count(500) == 1
        assert sorted(env.stubs.posted_files) == [500, 501]
    finally:
        store.close()


def test_f11_deadline_expires_during_preflight_stops_the_post(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """P2-1: the deadline is re-checked on a fresh clock after preflight's network phase. A fake
    clock that passes the deadline during the preflight GETs must not POST — the campaign
    auto-pauses (``deadline_exceeded``) and the rows stay queueable."""
    _set_campaign(env, max_batch_size=COUNT_TWO)
    env.seed(specs(COUNT_TWO))
    prepared = prepare(env, capsys, deadline_hours="1")
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]
    store = env.open_store()
    try:

        def _past_deadline_on_read(path: str) -> None:
            if path.startswith(f"/api/v3/File/{500}"):
                env.now_iso = _iso_plus(T0, 2 * 3600.0)  # deadline passes mid-preflight

        env.stubs.get_hook = _past_deadline_on_read
        report = apply_campaign(
            _campaign_deps(env, store, campaign_id),
            load_settings(env.config_path),
            store,
            env.tmp_path / "plan.json",
            T0,
            batch_size=COUNT_TWO,
        )
        env.stubs.get_hook = None
        assert env.stubs.posts == []
        assert report.submitted == 0
        assert any(r.startswith("send_stale:500:deadline_exceeded") for r in report.reasons)
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status == "paused"
        assert campaign.status_reason == "deadline_exceeded"
        record = store.get_file(500)
        assert record is not None
        assert record.state is FileState.PLANNED
        rows = store._conn.execute("SELECT COUNT(*) FROM sends").fetchone()
        assert int(rows[0]) == 0
    finally:
        store.close()


# ------------------------------------------------------------------ F18: budget cooldown defers

# The zero-progress breaker must not pause a campaign for RECOVERABLE waits: a rolling/cycle
# budget exhaustion marks the campaign DEFERRED (persisted retry iso, campaign stays APPROVED and
# drain-authorized) and the daemon resumes it automatically once the rolling window permits.

F18_DEFER_KEY = "campaign_deferred_until"
F18_ZERO_KEY = "campaign_zero_progress"


def _f18_setup(
    env: Env,
    capsys: pytest.CaptureFixture[str],
    count: int,
    *,
    per_cycle: int,
    rolling: int,
    batch: int,
    window_hours: float = 1.0,
    jobs: int = 100,
) -> tuple[str, StateStore]:
    """Seed/approve/authorize a campaign under tight rolling-budget knobs (fake clock)."""
    _set_campaign(env, max_batch_size=batch)
    _set_sync(
        env,
        max_new_per_cycle=per_cycle,
        rolling_new_limit=rolling,
        rolling_window_hours=window_hours,
        max_pending_episode_jobs=jobs,
    )
    env.seed(specs(count))
    prepared = prepare(env, capsys, limit=str(count))
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]
    store = env.open_store()
    _canary(env, store, campaign_id)
    _authorize_drain(env, store, campaign_id)
    return campaign_id, store


def test_f18_rolling_budget_defers_campaign_and_resumes_across_windows(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F18 core (100 files, per-cycle cap 5, rolling cap 10, 1-hour window): ≤5 per cycle, stops
    at 10 within the window, stays APPROVED + drain-authorized while deferred, submits zero
    before the window permits, and completes automatically across ≥3 rolling windows."""
    campaign_id, store = _f18_setup(env, capsys, COUNT_HUNDRED, per_cycle=5, rolling=10, batch=5)
    try:
        assert _campaign_sent(store, campaign_id) == 1  # the canary
        first = _daemon_cycle(env, store, campaign_id, "f18-c1")
        assert first.campaign is not None
        assert first.campaign.submitted == 5  # per-cycle cap, never more
        assert _campaign_sent(store, campaign_id) == 6
        second = _daemon_cycle(env, store, campaign_id, "f18-c2")
        assert second.campaign is not None
        assert second.campaign.submitted == 4  # stops at the rolling ceiling inside the window
        assert _campaign_sent(store, campaign_id) == 10
        posts_at_cap = len(env.stubs.posts)
        blocked = _daemon_cycle(env, store, campaign_id, "f18-c3")
        assert blocked.campaign is None  # budget-blocked: deferred this cycle, zero submissions
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status == "approved"  # NOT paused
        assert campaign.drain_authorized_iso is not None  # drain authorization intact
        deferred = store.get_meta(f"{F18_DEFER_KEY}:{campaign_id}")
        assert deferred == _iso_plus(T0, ROLLING_WINDOW_SECONDS)  # rolling-slot expiry
        assert any("budget_cooldown:until=" in reason for reason in blocked.reasons)
        before = _daemon_cycle(env, store, campaign_id, "f18-c4")
        assert before.campaign is None  # window not open: skipped
        assert len(env.stubs.posts) == posts_at_cap  # zero submissions before the window permits
        env.now_iso = _iso_plus(T0, ROLLING_WINDOW_SECONDS + 1.0)
        resumed = _daemon_cycle(env, store, campaign_id, "f18-c5")
        assert resumed.campaign is not None  # automatic resume, no operator action
        assert resumed.campaign.submitted == 5
        cycles = 5
        while _campaign_sent(store, campaign_id) < COUNT_HUNDRED and cycles < 60:
            cycles += 1
            cycle = _daemon_cycle(env, store, campaign_id, f"f18-loop-{cycles}")
            if cycle.campaign is None:
                env.now_iso = _iso_plus(env.now_iso, ROLLING_WINDOW_SECONDS + 1.0)
        final = _daemon_cycle(env, store, campaign_id, "f18-final")
        assert final.campaign is None  # max_submissions reached: the terminal auto-pause refuses
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.total_sent == COUNT_HUNDRED
        assert campaign.status == "paused"  # plan complete: max_submissions_reached terminal
        assert campaign.status_reason == "max_submissions_reached"
        posted = env.stubs.posted_files
        assert len(set(posted)) == COUNT_HUNDRED  # every file sent exactly once
        elapsed = (datetime.fromisoformat(env.now_iso) - datetime.fromisoformat(T0)).total_seconds()
        assert elapsed >= 3 * ROLLING_WINDOW_SECONDS  # crossed at least 3 rolling windows
    finally:
        store.close()


def test_f18_restart_mid_cooldown_preserves_deferral_schedule(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F18: the deferral is durable — a store reopen mid-cooldown respects the persisted
    deferred-until (no immediate burst) and resumes once the window permits."""
    campaign_id, store = _f18_setup(env, capsys, COUNT_FIVE, per_cycle=2, rolling=2, batch=2)
    try:
        first = _daemon_cycle(env, store, campaign_id, "f18-r1")
        assert first.campaign is not None
        assert first.campaign.submitted == 1  # rolling ceiling (2) reached
        blocked = _daemon_cycle(env, store, campaign_id, "f18-r2")
        assert blocked.campaign is None
        assert store.get_meta(f"{F18_DEFER_KEY}:{campaign_id}") == _iso_plus(
            T0, ROLLING_WINDOW_SECONDS
        )
    finally:
        store.close()
    reopened = env.open_store()
    try:
        posts_before = len(env.stubs.posts)
        mid = _daemon_cycle(env, reopened, campaign_id, "f18-r3")
        assert mid.campaign is None  # deferral respected after restart
        assert len(env.stubs.posts) == posts_before  # zero burst
        campaign = reopened.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status == "approved"
        assert campaign.drain_authorized_iso is not None
        env.now_iso = _iso_plus(T0, ROLLING_WINDOW_SECONDS + 1.0)
        resumed = _daemon_cycle(env, reopened, campaign_id, "f18-r4")
        assert resumed.campaign is not None  # auto-resume after the window
        assert resumed.campaign.submitted <= 2  # bounded by the per-cycle cap — no burst
    finally:
        reopened.close()


def test_f18_pending_verification_zero_progress_never_pauses(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F18: pending Shoko async jobs (files stuck in ``verifying``, holding their job slots)
    make every later drain cycle budget-blocked with zero submissions — a RECOVERABLE wait:
    the campaign defers durably with a bounded cooldown, stays APPROVED, the breaker is never
    burned, and the campaign recovers automatically once the jobs land and the cooldown
    passes (no operator resume)."""
    # Finding 1: the non-rolling stall cooldown is STALL_YIELD_COOLDOWN_CYCLES interval
    # seconds (5 x the default interval in this fixture), NOT one interval.
    cooldown_seconds = 5 * load_settings(env.config_path).sync.interval_seconds
    campaign_id, store = _f18_setup(
        env, capsys, COUNT_SIX, per_cycle=5, rolling=1000, batch=5, jobs=2
    )
    env.stubs.verify_empty = {spec.shoko_id for spec in specs(COUNT_SIX)[1:]}
    try:
        first = _daemon_cycle(env, store, campaign_id, "f18-p0")
        assert first.campaign is not None
        assert first.campaign.submitted == 2  # the 2-job ceiling fills with verifying files
        for index in range(1, 6):  # five consecutive zero-progress verifying-pending cycles
            cycle = _daemon_cycle(env, store, campaign_id, f"f18-p{index}")
            assert cycle.campaign is None  # budget-blocked: deferred (index 1), skipped (2+)
            assert any("budgets_exhausted" in reason for reason in cycle.reasons)
            campaign = store.get_campaign(campaign_id)
            assert campaign is not None
            assert campaign.status == "approved"  # never a permanent pause
            assert campaign.drain_authorized_iso is not None
            assert not any("campaign_zero_progress_pause" in reason for reason in cycle.reasons)
            assert store.get_meta(f"{F18_ZERO_KEY}:{campaign_id}") in (None, "0")
        deferred = store.get_meta(f"{F18_DEFER_KEY}:{campaign_id}")
        assert deferred == _iso_plus(T0, cooldown_seconds)  # Finding 1: a real cooldown, not now
        env.stubs.verify_empty = set()  # the ManualLinkJobs land: XRefs materialize
        for spec in specs(COUNT_SIX)[1:3]:
            env.stubs.linked[spec.shoko_id] = [env.stubs.xref(spec)]
        env.now_iso = _iso_plus(T0, cooldown_seconds + 1.0)  # the cooldown passes
        recovered = _daemon_cycle(env, store, campaign_id, "f18-p-recover")
        assert recovered.campaign is not None  # slots released → the drain resumes
        assert recovered.campaign.submitted > 0
        verified = sum(
            1 for record in store.campaign_files(campaign_id) if record.state is FileState.VERIFIED
        )
        assert verified >= 3  # the pending jobs closed VERIFIED (canary + 2 repolls)
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status == "approved"
    finally:
        store.close()


def test_f18_hard_safety_failures_still_pause_the_drain(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F18: the deferral path never weakens the hard stops — an expired campaign lease pauses
    the campaign (``attestation_expired``) and a revoked campaign is out of drain selection
    with manual apply refused; both with zero further POSTs."""
    campaign_id, store = _f18_setup(env, capsys, COUNT_FIVE, per_cycle=5, rolling=1000, batch=2)
    try:
        posts_before = len(env.stubs.posts)
        env.write_campaign_lease(campaign_id, expired=True)
        expired_cycle = _daemon_cycle(env, store, campaign_id, "f18-hard-lease")
        assert expired_cycle.campaign is None
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status == "paused"
        assert campaign.status_reason == "attestation_expired"
        assert len(env.stubs.posts) == posts_before
        campaign_revoke(store, campaign_id, env.now_iso)
        revoked_cycle = _daemon_cycle(env, store, campaign_id, "f18-hard-revoked")
        assert revoked_cycle.campaign is None
        manual = apply(env, capsys, batch="1")
        assert manual["exit"] == EXIT_APPLY_GATE_REFUSAL
        assert len(env.stubs.posts) == posts_before
    finally:
        store.close()


def test_f18_mixed_eligibility_refusal_pauses_without_deferral_meta(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """Finding 4: the deferral branch fires ONLY for a pure ``budgets_exhausted``
    eligibility verdict. A mixed refusal (budget exhaustion + source instability)
    pauses the campaign for the drift family and writes NO deferral meta (a stale marker on a
    resumed campaign is exactly what the structured verdict must prevent)."""
    _set_campaign(env, max_batch_size=BATCH_TWO)
    _set_sync(env, max_new_per_cycle=1, rolling_new_limit=1, rolling_window_hours=168.0)
    env.seed(specs(COUNT_FIVE))
    prepared = prepare(env, capsys)
    approve(env, capsys, prepared["approval_digest"])
    campaign_id = prepared["campaign_id"]
    store = env.open_store()
    try:
        _canary(env, store, campaign_id)  # the one submission spends the rolling budget
        _authorize_drain(env, store, campaign_id)
        store.pause("queue_anomaly", T0)  # a global pause rides with the budget stall
        cycle = _daemon_cycle(env, store, campaign_id, "mixed-refusal")
        assert cycle.campaign is None
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status == "paused"  # the drift family keeps pausing
        assert campaign.status_reason == "mapping_drift"
        assert store.get_meta(f"{F18_DEFER_KEY}:{campaign_id}") is None  # no deferral meta
        assert len(env.stubs.posts) == COUNT_ONE  # zero POSTs from the refused batch
    finally:
        store.close()


def test_f18_reporting_budgets_ok_resets_the_elapsed_rolling_window(
    env: Env,
) -> None:
    """Finding 3: the report/health ``budgets_ok`` peek applies the same rolling-window reset
    the daemon eligibility peek and the reservation path apply — an exhausted rolling counter
    whose window already elapsed is headroom, not exhaustion."""
    from shoko_linker.reporting import budgets_ok

    _set_sync(env, max_new_per_cycle=5, rolling_new_limit=1, rolling_window_hours=1.0)
    store = env.open_store()
    try:
        store.record_file(
            500,
            sonarr_series_id=1,
            sonarr_file_id=10,
            sonarr_path=f"{SONARR_ROOT}/series1/500.mkv",
            sonarr_size=FILE_SIZE,
            cohort=Cohort.BACKLOG,
            iso=T0,
        )
        store.begin_cycle("cycle", T0)
        store.advance(500, FileState.WAITING, "report probe", iso=T0)
        store.advance(500, FileState.MAPPED, "report probe", iso=T0)
        store.advance(500, FileState.PLANNED, "report probe", iso=T0)
        token = store.prepare_send(
            500,
            "a" * DIGEST_HEX,
            "b" * DIGEST_HEX,
            targets_digest="c" * DIGEST_HEX,
            structure_digest="d" * DIGEST_HEX,
            mapping_revision="1",
            cycle_key="cycle",
            iso=T0,
            expected_episode_jobs=1,
        )
        assert not isinstance(token, BudgetRefusal)  # the one-slot rolling budget is spent
        assert budgets_ok(store, T0) is False  # inside the window: exhausted
        after = _iso_plus(T0, ROLLING_WINDOW_SECONDS + 1.0)
        assert budgets_ok(store, after) is True  # window elapsed: headroom (mirrors the daemon)
    finally:
        store.close()


# ------------------------------------------------------------------ F19: canary send-history gate

# Drain authorization is computed from the DURABLE send ledger: every send attempted for the
# campaign must be terminal VERIFIED. A REJECTED/REFUSED send maps its file back to ``waiting``
# — the send row stays in the ledger and REFUSES the grant, visibly listed.


def test_f19_authorize_refuses_rejected_canary_then_succeeds_after_verified_resubmit(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F19 core: a REJECTED canary (file back in ``waiting``, send row durable) refuses the
    authorization — visibly naming the outstanding send — and the grant succeeds (and persists
    across restart) after the file is resubmitted and verified through the normal bounded flow.
    The canary audit keeps BOTH sends visible."""
    campaign_id = _f10_campaign(env, capsys)
    store = env.open_store()
    try:
        env.stubs.post_status = 400  # Shoko refuses with zero writes → REJECTED → waiting
        canary = _canary(env, store, campaign_id)
        env.stubs.post_status = None
        assert canary.submitted == COUNT_ONE
        record = store.get_file(500)
        assert record is not None
        assert record.state is FileState.WAITING
        with pytest.raises(CampaignError, match="not settled") as exc:
            _authorize(env, store, campaign_id)
        message = str(exc.value)
        assert "500" in message
        assert "rejected" in message  # visible handling, not silent exclusion
        report = campaign_report(store, campaign_id, None)
        attempted = report["attempted_sends"]
        assert [row["shoko_file_id"] for row in attempted] == [500]
        assert attempted[0]["outcome"] == "rejected"
        assert attempted[0]["state"] == "waiting"
        walk = _daemon_cycle(env, store, campaign_id, "f19-replan")
        assert walk.campaign is None  # zero VERIFIED canary progress keeps the drain closed
        resubmitted = apply(env, capsys, batch="1")  # the recovery walk re-planned 500
        assert resubmitted["exit"] == EXIT_OK
        authorized = _authorize(env, store, campaign_id)
        assert authorized.drain_authorized_iso == env.now_iso
        final = campaign_report(store, campaign_id, None)
        outcomes = [row["outcome"] for row in final["attempted_sends"]]
        assert outcomes == ["rejected", "verified"]  # the earlier rejected send stays visible
    finally:
        store.close()
    reopened = env.open_store()
    try:
        selected = select_next_daemon_campaign(reopened)
        assert selected is not None
        assert selected.campaign_id == campaign_id
        assert selected.drain_authorized_iso is not None
    finally:
        reopened.close()


def test_f19_verified_plus_rejected_cannot_silently_pass(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F19: a canary batch of one VERIFIED file + one REJECTED file cannot authorize on the
    strength of the verified file alone — the rejected send is named in the refusal."""
    campaign_id = _f10_campaign(env, capsys)
    store = env.open_store()
    try:
        canary = _canary(env, store, campaign_id)
        assert canary.verified == COUNT_ONE
        env.stubs.post_status = 400
        second = apply(env, capsys, batch="1")  # 501: REJECTED → waiting
        env.stubs.post_status = None
        assert second["exit"] == EXIT_OK
        record = store.get_file(501)
        assert record is not None
        assert record.state is FileState.WAITING
        with pytest.raises(CampaignError, match="501") as exc:
            _authorize(env, store, campaign_id)
        assert "rejected" in str(exc.value)
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.drain_authorized_iso is None
    finally:
        store.close()


def test_f19_authorize_refuses_inflight_and_indeterminate_canaries_visibly(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F19: in-flight (verifying) and indeterminate_write canary sends are refused with the
    outstanding send listed by file id and state."""
    campaign_id = _f10_campaign(env, capsys)
    env.stubs.verify_empty = {spec.shoko_id for spec in specs(COUNT_FIVE)}
    store = env.open_store()
    try:
        canary = _canary(env, store, campaign_id)
        assert canary.submitted == COUNT_ONE
        record = store.get_file(500)
        assert record is not None
        assert record.state is FileState.VERIFYING
        with pytest.raises(CampaignError, match="500") as inflight:
            _authorize(env, store, campaign_id)
        assert "verifying" in str(inflight.value)
        store.advance(500, FileState.INDETERMINATE_WRITE, "ambiguous send", iso=env.now_iso)
        with pytest.raises(CampaignError, match="500") as indeterminate:
            _authorize(env, store, campaign_id)
        assert "indeterminate_write" in str(indeterminate.value)
    finally:
        store.close()


def test_cli_authorize_drain_refuses_rejected_canary_with_visible_history(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    """F19 CLI: authorize-drain on a rejected canary is a refusal that shows the send history;
    the operator-confirmation gate (confirm + non-empty note) is unchanged."""
    campaign_id = _f10_campaign(env, capsys)
    store = env.open_store()
    try:
        env.stubs.post_status = 400
        _canary(env, store, campaign_id)
        env.stubs.post_status = None
    finally:
        store.close()
    capsys.readouterr()
    code = main(
        [
            "campaign",
            "authorize-drain",
            str(env.config_path),
            "--campaign-id",
            campaign_id,
            "--note",
            AUTH_NOTE,
            "--confirm",
            "--json",
        ]
    )
    assert code == EXIT_CONFIG_INVALID
    err = capsys.readouterr().err
    assert "500" in err
    assert "rejected" in err
    store = env.open_store()
    try:
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.drain_authorized_iso is None
    finally:
        store.close()
