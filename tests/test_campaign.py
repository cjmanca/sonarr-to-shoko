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
from collections.abc import Iterator
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
    CampaignDigestMismatchError,
    CampaignPlanInvalidError,
    approve_campaign,
    read_plan_file,
)
from shoko_linker.cli import EXIT_APPLY_GATE_REFUSAL, EXIT_CONFIG_INVALID, EXIT_OK, main
from shoko_linker.config import load_settings
from shoko_linker.http import ApiEndpoint
from shoko_linker.mapping_store import MappingStore
from shoko_linker.mapping_store import refresh as mapping_refresh
from shoko_linker.state import BaselineFile, FileState, StateStore

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
