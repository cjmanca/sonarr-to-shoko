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

import json
import os
import sys
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import yaml  # type: ignore[import-untyped]

from shoko_linker import cli
from shoko_linker.attestation import build_record, write_attestation
from shoko_linker.campaign import (
    CampaignApplyReport,
    CampaignDigestMismatchError,
    CampaignPlanInvalidError,
    apply_campaign,
    approve_campaign,
    read_plan_file,
)
from shoko_linker.cli import EXIT_APPLY_GATE_REFUSAL, EXIT_CONFIG_INVALID, EXIT_OK, main
from shoko_linker.config import load_settings
from shoko_linker.daemon import CycleMode, run_cycle
from shoko_linker.http import ApiEndpoint
from shoko_linker.mapping_store import MappingStore
from shoko_linker.mapping_store import refresh as mapping_refresh
from shoko_linker.policies import RunPolicy
from shoko_linker.state import (
    BaselineFile,
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
    sonarr_requests: list[httpx.Request] = field(default_factory=list)
    shoko_requests: list[httpx.Request] = field(default_factory=list)

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
        return {
            "ID": spec.shoko_id,
            "Size": self.size(spec),
            "IsIgnored": False,
            "IsVariation": False,
            "Imported": "2024-01-01T00:00:00",
            "Hashes": {
                "ED2K": f"e{spec.shoko_id}",
                "SHA1": None,
                "CRC32": spec.shoko_id,
                "MD5": None,
            },
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
        if request.method == "POST" and path.endswith("/Link"):
            file_id = int(path.split("/")[4])
            self.posts.append(json.loads(request.content))
            self.posted_files.append(file_id)
            spec = next(item for item in self.specs if item.shoko_id == file_id)
            if file_id not in self.verify_empty:
                self.linked[file_id] = [self.xref(spec)]
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
            rows = [
                self.file_row(spec) for spec in sorted(self.specs, key=lambda item: item.shoko_id)
            ]
            return httpx.Response(200, json={"Total": len(rows), "List": rows}, request=request)
        if path.startswith("/api/v3/File/"):
            file_id = int(path.split("/")[4])
            spec = next(item for item in self.specs if item.shoko_id == file_id)
            return httpx.Response(200, json=self.file_row(spec), request=request)
        return httpx.Response(404, json={}, request=request)


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

    def write_campaign_lease(self, campaign_id: str) -> None:
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
            expires_iso=(datetime.fromisoformat(self.now_iso) + timedelta(hours=24)).isoformat(),
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
    monkeypatch.setattr(cli, "attestation_target_path", lambda settings: environment.lease_path)
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


# ------------------------------------------------------------------ main flow


def test_prepare_approve_apply_three_batches_with_one_approval(
    env: Env, capsys: pytest.CaptureFixture[str]
) -> None:
    env.seed(specs(COUNT_FIVE))
    prepared = prepare(env, capsys)
    assert prepared["files"] == COUNT_FIVE
    assert len(prepared["approval_digest"]) == DIGEST_HEX
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
# ONLY after the operator's manual canary batch completes (canary gate: send progress ≥ 1).
# Every scenario runs the full campaign guard chain via the daemon cycle; the daemon reuses its
# own apply lock (apply_campaign lock_held=True), so a daemon batch never self-contends.

COUNT_ONE = 1
BATCH_ONE = 1
COUNT_SIX = 6
CAMPAIGN_DAEMON_KEY = "daemon-drain"
LEASE_SHORT_HOURS = 1.0


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
    """F7 core: after ONE manual canary, daemon cycles drain the rest in bounded batches."""
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
