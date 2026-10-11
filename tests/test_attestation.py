"""M7-b gate: safety attestation lease — validation matrix, CLI lifecycle, enforcement.

All offline: the lease is a tmp file, endpoints are ``httpx.MockTransport`` stubs, the
clock is a fixed ISO string. The daemon/cycle evidence is the request log: expired
leases produce **zero POSTs** while cycles stay alive (no restart loop), a valid lease
lets the guarded path proceed, and the persisted write pause fires **once per transition**.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml  # type: ignore[import-untyped]

from shoko_linker import cli
from shoko_linker.attestation import (
    SCOPE_DAEMON,
    AttestationVerdict,
    build_record,
    campaign_lease_path,
    lease_expiry_cap,
    load_attestation,
    parse_attestation_document,
    record_digest,
    write_attestation,
)
from shoko_linker.cli import EXIT_APPLY_GATE_REFUSAL, EXIT_CONFIG_INVALID, EXIT_OK, main
from shoko_linker.config import Settings, load_settings
from shoko_linker.http import ApiEndpoint
from shoko_linker.mapping_store import MappingStore
from shoko_linker.mapping_store import refresh as mapping_refresh
from shoko_linker.state import BaselineFile, StateStore

SONARR_BASE = "http://sonarr:8989"
SHOKO_BASE = "http://shoko:8111"
SONARR_ROOT = "/data/media/anime"
SHOKO_ROOT = "/anime"
T0 = "2026-01-01T00:00:00+00:00"
LATER = "2026-01-02T00:00:00+00:00"
HOURS_24 = 24.0
WARM = 3600.0
DIGEST_HEX = 64
SECONDS_24H = 86400.0


def _record(**overrides: Any) -> Any:
    base: dict[str, Any] = {
        "endpoint": SHOKO_BASE,
        "version": "5.3.3",
        "mounts": ["/anime read-only (host-inspected 2026-01-01)"],
        "settings_flags": {
            "FileQualityFilterEnabled": False,
            "RenameOnImport": False,
            "MoveOnImport": False,
            "MyListAddFiles": False,
        },
        "inspected_iso": T0,
        "expires_iso": LATER,
        "scope": "daemon",
        "operator_note": "quality filter off, renamer off, mounts read-only",
    }
    base.update(overrides)
    return build_record(
        endpoint=base["endpoint"],
        version=base["version"],
        mounts=base["mounts"],
        settings_flags=base["settings_flags"],
        inspected_iso=base["inspected_iso"],
        expires_iso=base["expires_iso"],
        scope=base["scope"],
        operator_note=base["operator_note"],
    )


def _load(
    record: Any, path: Path, *, now: str = T0, scope: str = "daemon", **kwargs: Any
) -> AttestationVerdict:
    return load_attestation(
        path, shoko_url=SHOKO_BASE, now_iso=now, scope=scope, warn_lead_seconds=WARM, **kwargs
    )


# ------------------------------------------------------------------ module matrix


def test_valid_lease_roundtrip(tmp_path: Path) -> None:
    record = _record()
    path, digest, warning = write_attestation(tmp_path / "lease.yaml", record)
    assert len(digest) == DIGEST_HEX
    assert warning is None or "0600" in warning  # chmod attempted, warning names it only
    parsed = parse_attestation_document(path.read_text(encoding="utf-8"))
    assert parsed == record
    assert record_digest(parsed) == digest
    verdict = _load(record, path)
    assert verdict.status == "valid"
    assert verdict.valid_for_apply is True
    assert verdict.digest == digest
    assert verdict.expires_in_seconds == SECONDS_24H


def test_absent_and_missing_file(tmp_path: Path) -> None:
    absent = load_attestation(None, shoko_url=SHOKO_BASE, now_iso=T0, scope="daemon")
    assert absent.status == "absent"
    missing = load_attestation(
        tmp_path / "nope.yaml", shoko_url=SHOKO_BASE, now_iso=T0, scope="daemon"
    )
    assert missing.status == "absent"


def test_malformed_lease_is_invalid(tmp_path: Path) -> None:
    path = tmp_path / "lease.yaml"
    path.write_text("just a string, not a record\n", encoding="utf-8")
    verdict = _load(None, path)
    assert verdict.status == "invalid"
    assert verdict.reason
    path.write_text("endpoint: http://shoko:8111\nversion: 5.3.3\n", encoding="utf-8")
    verdict = _load(None, path)
    assert verdict.status == "invalid"
    assert "missing field" in verdict.reason


def test_endpoint_scope_and_expiry_matrix(tmp_path: Path) -> None:
    path, _digest, _warning = write_attestation(tmp_path / "lease.yaml", _record())
    mismatch = load_attestation(
        path, shoko_url="http://other-shoko:8111", now_iso=T0, scope="daemon"
    )
    assert mismatch.status == "invalid"
    assert "endpoint mismatch" in mismatch.reason
    scope = _load(None, path, scope="campaign:abc-123")
    assert scope.status == "invalid"
    assert "scope mismatch" in scope.reason
    expired = _load(None, path, now="2027-01-01T00:00:00+00:00")
    assert expired.status == "expired"
    assert expired.valid_for_apply is False
    warned = _load(None, path, now="2026-01-01T23:30:00+00:00")
    assert warned.status == "valid"
    assert warned.warn is True
    assert warned.valid_for_apply is True


def test_version_hint_stale_version_fails_closed(tmp_path: Path) -> None:
    path, _digest, _warning = write_attestation(tmp_path / "lease.yaml", _record())
    match = _load(None, path, version_hint="5.3.3")
    assert match.status == "valid"
    stale = _load(None, path, version_hint="5.4.0")
    assert stale.status == "stale_version"
    assert stale.valid_for_apply is False
    assert "treated as invalid" in stale.reason


def test_lease_cannot_exceed_validity_cap() -> None:
    ok, _reason = lease_expiry_cap(72.0, 168.0)
    assert ok is True
    ok, reason = lease_expiry_cap(200.0, 168.0)
    assert ok is False
    assert "exceeds" in reason


def test_settings_flags_are_the_only_allowed_summary(tmp_path: Path) -> None:
    with pytest.raises(Exception, match="settings"):
        _record(settings_flags={"FileQualityFilterEnabled": False, "AniDb.Password": True})
    path = tmp_path / "lease.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "endpoint": SHOKO_BASE,
                "version": "5.3.3",
                "mount_fingerprint": ["/anime read-only"],
                "settings_summary": {"FileQualityFilterEnabled": "no"},
                "inspected_iso": T0,
                "expires_iso": LATER,
                "scope": "daemon",
                "operator_note": "x",
            }
        ),
        encoding="utf-8",
    )
    verdict = _load(None, path)
    assert verdict.status == "invalid"
    assert "settings_summary" in verdict.reason


def test_record_payload_is_secret_free(tmp_path: Path) -> None:
    record = _record(operator_note="read-only mounts verified; key material never recorded")
    path, _digest, _warning = write_attestation(tmp_path / "lease.yaml", record)
    payload = _load(None, path).payload()
    text = json.dumps(payload)
    for secret in ("SHOKO_API_KEY", "api_key", "apikey", "password"):
        assert secret not in text.lower()
    assert (
        payload["record"]["operator_note"] == record.operator_note
    )  # record fields shown, not redacted


# ------------------------------------------------------------------ CLI + enforcement


@dataclass
class StubEnv:
    tmp_path: Path
    config_path: Path
    lease_path: Path
    posts: list[dict[str, Any]] = field(default_factory=list)
    linked: dict[int, list[dict[str, Any]]] = field(default_factory=dict)
    now_iso: str = T0

    def open_store(self) -> StateStore:
        settings = load_settings(self.config_path)
        return StateStore(
            Path(self.tmp_path / "state.sqlite3"),
            budget_limits=cli.budget_limits_from_settings(settings),
        )


def _document(lease: Path | None, *, apply_changes: bool = True) -> dict[str, Any]:
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
            "apply_changes": apply_changes,
            "jitter_ratio": 0.0,
            "new_file_grace_period_seconds": 0,
        },
        "database": {"path": "/data/state.sqlite3"},
        "safety": {
            "attestation_file": "/data/lease.yaml" if lease is not None else None,
            "attestation_validity_hours": 168,
            "attestation_warn_seconds": 3600,
        },
    }


def _shoko_handler(env: StubEnv) -> Any:
    def handle(request: httpx.Request) -> httpx.Response:  # noqa: PLR0911
        path = request.url.path
        if request.method == "POST" and path.endswith("/Link"):
            env.posts.append(json.loads(request.content))
            file_id = int(path.split("/")[4])
            env.linked[file_id] = [
                {
                    "SeriesID": {"ID": None, "AniDB": 1234, "TMDB": None},
                    "EpisodeIDs": [{"ID": 777, "AniDB": 5501, "TMDB": None, "Source": "User"}],
                }
            ]
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
                json={"WaitingCount": 0, "BlockedCount": 0, "TotalCount": 0, "ThreadCount": 1},
                request=request,
            )
        if path.startswith("/api/v3/Series/AniDB/"):
            return httpx.Response(
                200,
                json={
                    "Total": 1,
                    "List": [{"ID": 5501, "AnimeID": 1234, "Type": "Episode", "EpisodeNumber": 1}],
                },
                request=request,
            )
        if path.startswith("/api/v3/Episode/AniDB/"):
            return httpx.Response(
                200,
                json={"IDs": {"ID": 777, "AniDB": 5501, "TMDB": None}, "Type": "Episode"},
                request=request,
            )
        if path == "/api/v3/File":
            return httpx.Response(200, json=_file_listing(env), request=request)
        if path.startswith("/api/v3/File/"):
            return httpx.Response(200, json=_file_row(env, 500), request=request)
        return httpx.Response(404, json={}, request=request)

    return handle


def _file_row(env: StubEnv, file_id: int) -> dict[str, Any]:
    return {
        "ID": file_id,
        "Size": 1000,
        "IsIgnored": False,
        "IsVariation": False,
        "Imported": "2024-01-01T00:00:00",
        "Hashes": {"ED2K": f"e{file_id}", "SHA1": None, "CRC32": file_id, "MD5": None},
        "Locations": [
            {
                "ID": file_id * 10,
                "FileID": file_id,
                "ImportFolderID": 1,
                "RelativePath": f"series1/{file_id}.mkv",
                "AbsolutePath": f"{SHOKO_ROOT}/series1/{file_id}.mkv",
                "IsAccessible": True,
            }
        ],
        "SeriesIDs": env.linked.get(file_id, []),
    }


def _file_listing(env: StubEnv) -> dict[str, Any]:
    rows = [_file_row(env, 500)]
    return {"Total": len(rows), "List": rows}


def _sonarr_handler() -> Any:
    def handle(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/api/v3/series":
            return httpx.Response(
                200,
                json=[
                    {
                        "id": 1,
                        "title": "S1",
                        "rootFolderPath": f"{SONARR_ROOT}/series1",
                        "tvdbId": 267440,
                    }
                ],
                request=request,
            )
        if path == "/api/v3/rootfolder":
            return httpx.Response(
                200, json=[{"id": 1, "path": SONARR_ROOT, "accessible": True}], request=request
            )
        if path == "/api/v3/episode":
            return httpx.Response(
                200,
                json=[
                    {
                        "id": 101,
                        "seriesId": 1,
                        "seasonNumber": 1,
                        "episodeNumber": 1,
                        "title": "E1",
                        "tvdbId": 1,
                        "episodeFileId": 10,
                    }
                ],
                request=request,
            )
        if path == "/api/v3/episodefile":
            return httpx.Response(
                200,
                json=[
                    {
                        "id": 10,
                        "seriesId": 1,
                        "path": f"{SONARR_ROOT}/series1/500.mkv",
                        "size": 1000,
                    }
                ],
                request=request,
            )
        return httpx.Response(404, json={}, request=request)

    return handle


def _source_handler() -> Any:
    xml = (
        "<anime-list><anime anidbid='1234' tvdbid='267440' defaulttvdbseason='1'>"
        "<mapping-list><mapping anidbseason='1' tvdbseason='1'>;1-1;</mapping></mapping-list>"
        "</anime></anime-list>"
    )

    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={"ETag": '"src"'}, content=xml.encode(), request=request)

    return handle


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> StubEnv:
    lease = tmp_path / "lease.yaml"
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(_document(lease)), encoding="utf-8")
    environment = StubEnv(tmp_path=tmp_path, config_path=config_path, lease_path=lease)
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
    monkeypatch.setattr(cli, "interruptible_sleep", lambda seconds: None)

    def _endpoint(handler: Any, base: str) -> ApiEndpoint:
        client = httpx.Client(
            base_url=base,
            transport=httpx.MockTransport(handler),
            follow_redirects=False,
            timeout=httpx.Timeout(1.0),
        )
        return ApiEndpoint(client, lambda: {})

    monkeypatch.setattr(
        cli, "sonarr_endpoint", lambda settings: _endpoint(_sonarr_handler(), SONARR_BASE)
    )
    monkeypatch.setattr(
        cli, "shoko_endpoint", lambda settings: _endpoint(_shoko_handler(environment), SHOKO_BASE)
    )

    def _source(store: Any, settings: Settings) -> ApiEndpoint:
        client = httpx.Client(
            base_url="https://raw.githubusercontent.com/",
            transport=httpx.MockTransport(_source_handler()),
            follow_redirects=False,
            timeout=httpx.Timeout(1.0),
        )
        return ApiEndpoint(client, lambda: {"Accept-Encoding": "identity"})

    monkeypatch.setattr(cli, "source_endpoint", _source)
    monkeypatch.setattr(
        cli, "open_mapping_store", lambda settings: MappingStore(environment.tmp_path / "mappings")
    )
    return environment


def _seed_mapping(env: StubEnv) -> None:
    store = MappingStore(env.tmp_path / "mappings")
    settings = load_settings(env.config_path)
    endpoint = cli.source_endpoint(store, settings)
    try:
        mapping_refresh(
            store, endpoint, now=datetime.fromisoformat(T0), refresh_hours=168.0, force=True
        )
    finally:
        endpoint.client.close()


def _seed_baseline(env: StubEnv) -> None:
    store = env.open_store()
    try:
        store.record_baseline(
            [
                BaselineFile(
                    sonarr_series_id=9,
                    sonarr_file_id=99,
                    path="/data/media/anime/old/9.mkv",
                    size=1,
                )
            ],
            complete=True,
            auto_apply_eligible=True,
            iso=T0,
        )
    finally:
        store.close()


def test_attestation_status_reports_absent_without_lease(
    env: StubEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(["attestation", "status", str(env.config_path), "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert code == EXIT_OK
    assert payload["status"] == "absent"


def test_record_requires_confirm_and_writes_lease(
    env: StubEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(
        [
            "attestation",
            "record",
            str(env.config_path),
            "--endpoint",
            SHOKO_BASE,
            "--version",
            "5.3.3",
            "--mounts",
            "/anime read-only",
            "--settings",
            "FileQualityFilterEnabled=false,RenameOnImport=false,MoveOnImport=false,MyListAddFiles=false",
            "--hours",
            "24",
            "--note",
            "checked mounts",
        ]
    )
    out = capsys.readouterr().out
    assert code == EXIT_CONFIG_INVALID
    assert "--confirm" in out
    assert not env.lease_path.exists()
    code = main(
        [
            "attestation",
            "record",
            str(env.config_path),
            "--endpoint",
            SHOKO_BASE,
            "--version",
            "5.3.3",
            "--mounts",
            "/anime read-only",
            "--settings",
            "FileQualityFilterEnabled=false,RenameOnImport=false,MoveOnImport=false,MyListAddFiles=false",
            "--hours",
            "24",
            "--note",
            "checked mounts",
            "--confirm",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert code == EXIT_OK
    assert env.lease_path.exists()
    assert len(str(payload["digest"])) == DIGEST_HEX
    status = main(["attestation", "status", str(env.config_path), "--json"])
    verdict = json.loads(capsys.readouterr().out)
    assert status == EXIT_OK
    assert verdict["status"] == "valid"
    assert verdict["record"]["mount_fingerprint"] == ["/anime read-only"]


def test_record_refuses_hours_beyond_cap(env: StubEnv, capsys: pytest.CaptureFixture[str]) -> None:
    code = main(
        [
            "attestation",
            "record",
            str(env.config_path),
            "--endpoint",
            SHOKO_BASE,
            "--version",
            "5.3.3",
            "--mounts",
            "/anime read-only",
            "--settings",
            "FileQualityFilterEnabled=false,RenameOnImport=false,MoveOnImport=false,MyListAddFiles=false",
            "--hours",
            "500",
            "--note",
            "n",
            "--confirm",
        ]
    )
    out = capsys.readouterr().out
    assert code == EXIT_CONFIG_INVALID
    assert "exceeds" in out
    assert not env.lease_path.exists()


def test_invalidate_moves_lease_aside(env: StubEnv, capsys: pytest.CaptureFixture[str]) -> None:
    env.lease_path.write_text("x: 1\n", encoding="utf-8")
    env.open_store().close()
    code = main(
        [
            "attestation",
            "invalidate",
            str(env.config_path),
            "--note",
            "mount changed",
            "--confirm",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert code == EXIT_OK
    assert not env.lease_path.exists()
    assert (env.tmp_path / "lease.yaml.revoked").exists()
    assert payload["result"]["revoked_path"].endswith(".revoked")
    store = env.open_store()
    try:
        actions = json.loads(str(store.get_meta("operator_actions")))
        assert [item["command"] for item in actions][-1] == "attestation-invalidate"
    finally:
        store.close()
    assert main(["attestation", "status", str(env.config_path), "--json"]) == EXIT_OK
    assert json.loads(capsys.readouterr().out)["status"] == "absent"


def _write_valid_lease(env: StubEnv, *, hours: float = 24.0) -> None:
    record = _record(expires_iso=_iso_plus(env.now_iso, hours * 3600.0))
    write_attestation(env.lease_path, record, overwrite=True)


def _iso_plus(iso: str, seconds: float) -> str:
    return (datetime.fromisoformat(iso) + timedelta(seconds=seconds)).isoformat()


def test_once_apply_with_valid_lease_submits(
    env: StubEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    _seed_mapping(env)
    _seed_baseline(env)
    _write_valid_lease(env)
    code = main(["once", "--apply", str(env.config_path), "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert code == EXIT_OK
    assert payload["mode"] == "apply"
    assert payload["eligibility"]["eligible"] is True
    assert payload["counts"]["submitted"] == 1, payload["reasons"]
    assert payload["counts"]["verified"] == 1
    assert len(env.posts) == 1


def test_expired_lease_pauses_writes_once_and_daemon_stays_alive(
    env: StubEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    _seed_mapping(env)
    _seed_baseline(env)
    _write_valid_lease(env, hours=0.5)
    env.now_iso = "2026-01-01T06:00:00+00:00"  # lease expired at 00:30
    code = main(["daemon", "--cycles", "3", str(env.config_path)])
    out = capsys.readouterr().out
    assert code == EXIT_OK
    assert "daemon exit: stop_after_cycles after 3 cycle(s)" in out
    assert env.posts == []
    store = env.open_store()
    try:
        assert store.budget_view().paused_reason == "attestation_expired"
        assert store.get_meta("attestation_gate") == "paused"
    finally:
        store.close()


def test_expired_lease_transition_persists_exactly_one_pause(
    env: StubEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    _seed_mapping(env)
    _seed_baseline(env)
    _write_valid_lease(env, hours=0.5)
    env.now_iso = "2026-01-01T06:00:00+00:00"
    assert main(["daemon", "--cycles", "1", str(env.config_path)]) == EXIT_OK
    capsys.readouterr()
    store = env.open_store()
    try:
        assert store.budget_view().paused_reason == "attestation_expired"
        store.resume(env.now_iso)  # operator clears the pause manually
    finally:
        store.close()
    assert main(["daemon", "--cycles", "1", str(env.config_path)]) == EXIT_OK
    store = env.open_store()
    try:
        # the lease is still expired: the gate is already "paused" — no second pause
        assert store.budget_view().paused_reason is None
        assert store.get_meta("attestation_gate") == "paused"
    finally:
        store.close()


def test_revalidated_lease_clears_the_attestation_pause(
    env: StubEnv, capsys: pytest.CaptureFixture[str]
) -> None:
    _seed_mapping(env)
    _seed_baseline(env)
    _write_valid_lease(env, hours=0.5)
    env.now_iso = "2026-01-01T06:00:00+00:00"
    assert main(["daemon", "--cycles", "1", str(env.config_path)]) == EXIT_OK
    capsys.readouterr()
    store = env.open_store()
    try:
        assert store.budget_view().paused_reason == "attestation_expired"
    finally:
        store.close()
    _write_valid_lease(env, hours=24.0)
    env.now_iso = "2026-01-01T07:00:00+00:00"
    assert main(["daemon", "--cycles", "1", str(env.config_path)]) == EXIT_OK
    store = env.open_store()
    try:
        assert store.budget_view().paused_reason is None
        assert store.get_meta("attestation_gate") == "valid"
    finally:
        store.close()


def test_once_apply_without_gate_refuses(
    env: StubEnv, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SHOKO_LINKER_ALLOW_APPLY")
    _seed_mapping(env)
    code = main(["once", "--apply", str(env.config_path)])
    assert code == EXIT_APPLY_GATE_REFUSAL
    assert "REFUSED" in capsys.readouterr().out


# ------------------------------------------------------------------ F8: scope-specific leases


def _tmp_posix(tmp_path: Path, name: str) -> str:
    """A POSIX absolute string for ``tmp_path/<name>`` that ``Path`` resolves identically.

    ``safety.attestation_file`` must be a POSIX-style path (the config layer rejects Windows
    backslashes); stripping the drive letter yields a rooted POSIX string that ``Path`` resolves
    to the same on-disk location on the current drive (where pytest's tmp_path lives).
    """
    posix = tmp_path.as_posix()
    if len(posix) >= 2 and posix[1] == ":":
        posix = posix[2:]
    return f"{posix}/{name}"


def _scope_document(lease: Path) -> dict[str, Any]:
    document = _document(lease)
    document["safety"]["attestation_file"] = lease.as_posix()
    return document


def _write_lease_file(path: Path, *, scope: str) -> Path:
    write_attestation(path, _record(scope=scope, expires_iso=LATER), overwrite=True)
    return path


@pytest.fixture
def scope_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    """Real scope-aware resolver: daemon lease at the configured path, campaign leases as siblings.

    The real :func:`cli.attestation_target_path` is used (NO flattening monkeypatch) so the
    record/status/invalidate commands must select the right file for each scope.
    """
    lease = Path(_tmp_posix(tmp_path, "lease.yaml"))
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(_scope_document(lease)), encoding="utf-8")
    monkeypatch.setattr(cli, "current_iso", lambda: T0)
    monkeypatch.setattr(cli, "state_path", lambda settings: tmp_path / "state.sqlite3")
    monkeypatch.setattr(
        cli, "open_state_store", lambda settings: StateStore(tmp_path / "state.sqlite3")
    )
    return lease, config_path


def _record_cli(config_path: Path, scope: str) -> int:
    return main(
        [
            "attestation",
            "record",
            str(config_path),
            "--endpoint",
            SHOKO_BASE,
            "--version",
            "5.3.3",
            "--mounts",
            "/anime read-only",
            "--settings",
            "FileQualityFilterEnabled=false,RenameOnImport=false,MoveOnImport=false,MyListAddFiles=false",
            "--hours",
            "24",
            "--scope",
            scope,
            "--note",
            f"lease {scope}",
            "--confirm",
            "--json",
        ]
    )


def test_campaign_lease_path_is_a_sibling_of_the_daemon_lease() -> None:
    base = Path("/data/lease.yaml")
    sibling = campaign_lease_path(base, "camp-1")
    assert sibling.name == "lease.campaign-camp-1.yaml"
    assert sibling.parent == base.parent
    assert sibling != base


def test_record_and_status_target_the_right_file_per_scope(
    scope_env: tuple[Path, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    lease, config_path = scope_env
    campaign_lease = campaign_lease_path(lease, "camp-1")

    assert _record_cli(config_path, "daemon") == EXIT_OK
    capsys.readouterr()
    assert lease.exists()
    assert not campaign_lease.exists()

    assert _record_cli(config_path, "campaign:camp-1") == EXIT_OK
    capsys.readouterr()
    assert campaign_lease.exists()
    # recording a campaign lease must never clobber the daemon lease
    daemon = cli.attestation_verdict_for(load_settings(config_path), scope=SCOPE_DAEMON)
    assert daemon.valid_for_apply is True
    assert daemon.record is not None
    assert daemon.record.scope == "daemon"
    campaign = cli.attestation_verdict_for(load_settings(config_path), scope="campaign:camp-1")
    assert campaign.valid_for_apply is True
    assert campaign.record is not None
    assert campaign.record.scope == "campaign:camp-1"

    daemon_status = main(["attestation", "status", str(config_path), "--scope", "daemon", "--json"])
    assert daemon_status == EXIT_OK
    assert json.loads(capsys.readouterr().out)["status"] == "valid"
    campaign_status = main(
        ["attestation", "status", str(config_path), "--scope", "campaign:camp-1", "--json"]
    )
    assert campaign_status == EXIT_OK
    assert json.loads(capsys.readouterr().out)["status"] == "valid"


def test_invalidate_campaign_scope_leaves_the_daemon_lease_valid(
    scope_env: tuple[Path, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    lease, config_path = scope_env
    campaign_lease = campaign_lease_path(lease, "camp-1")
    assert _record_cli(config_path, "daemon") == EXIT_OK
    assert _record_cli(config_path, "campaign:camp-1") == EXIT_OK
    capsys.readouterr()

    code = main(
        [
            "attestation",
            "invalidate",
            str(config_path),
            "--scope",
            "campaign:camp-1",
            "--note",
            "campaign lease retired",
            "--confirm",
            "--json",
        ]
    )
    assert code == EXIT_OK
    assert not campaign_lease.exists()
    assert campaign_lease.with_name(campaign_lease.name + ".revoked").exists()
    assert lease.exists()  # the daemon lease is untouched by a campaign-scope invalidate
    capsys.readouterr()

    daemon_status = main(["attestation", "status", str(config_path), "--scope", "daemon", "--json"])
    assert daemon_status == EXIT_OK
    assert json.loads(capsys.readouterr().out)["status"] == "valid"
    # No campaign sibling remains; the read resolver falls back to the configured path, whose
    # daemon-scoped record fails the campaign scope match — fail closed (never "valid").
    campaign_status = main(
        ["attestation", "status", str(config_path), "--scope", "campaign:camp-1", "--json"]
    )
    assert campaign_status == EXIT_OK
    campaign_after = json.loads(capsys.readouterr().out)
    assert campaign_after["status"] in {"absent", "invalid"}


def test_backward_compatible_campaign_record_at_the_configured_path(
    scope_env: tuple[Path, Path],
) -> None:
    """A ``campaign:<id>`` record written directly at safety.attestation_file keeps working.

    The read resolver falls back to the configured path when no sibling exists, so a legacy
    campaign record at that path still authorizes its campaign scope.
    """
    lease, config_path = scope_env
    _write_lease_file(lease, scope="campaign:camp-1")
    verdict = cli.attestation_verdict_for(load_settings(config_path), scope="campaign:camp-1")
    assert verdict.valid_for_apply is True
    # the same file must NOT authorize the daemon scope (fail closed on the scope match)
    daemon = cli.attestation_verdict_for(load_settings(config_path), scope=SCOPE_DAEMON)
    assert daemon.valid_for_apply is False
    assert "scope mismatch" in daemon.reason


def test_per_scope_evaluation_rules_are_identical_fail_closed(scope_env: tuple[Path, Path]) -> None:
    lease, _config_path = scope_env
    campaign_lease = campaign_lease_path(lease, "camp-1")
    _write_lease_file(campaign_lease, scope="campaign:camp-1")

    endpoint = load_attestation(
        campaign_lease, shoko_url="http://other-shoko:8111", now_iso=T0, scope="campaign:camp-1"
    )
    assert endpoint.status == "invalid"
    assert "endpoint mismatch" in endpoint.reason

    stale = load_attestation(
        campaign_lease,
        shoko_url=SHOKO_BASE,
        now_iso=T0,
        scope="campaign:camp-1",
        version_hint="5.9.9",
    )
    assert stale.status == "stale_version"
    assert stale.valid_for_apply is False

    cross_scope = load_attestation(campaign_lease, shoko_url=SHOKO_BASE, now_iso=T0, scope="daemon")
    assert cross_scope.status == "invalid"
    assert "scope mismatch" in cross_scope.reason

    expired = load_attestation(
        campaign_lease,
        shoko_url=SHOKO_BASE,
        now_iso="2027-01-01T00:00:00+00:00",
        scope="campaign:camp-1",
    )
    assert expired.status == "expired"


def test_invalidate_campaign_scope_never_revokes_the_daemon_lease(
    scope_env: tuple[Path, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    """P1-1: campaign-scope invalidate with the sibling ABSENT must NOT fall back to the
    base daemon lease (destructive read-fallback would destroy daemon authorization)."""
    lease, config_path = scope_env
    assert _record_cli(config_path, "daemon") == EXIT_OK
    capsys.readouterr()
    before = lease.read_bytes()

    code = main(
        [
            "attestation",
            "invalidate",
            str(config_path),
            "--scope",
            "campaign:ghost",
            "--note",
            "attempt campaign revoke",
            "--confirm",
            "--json",
        ]
    )
    assert code == EXIT_CONFIG_INVALID  # refused, nonzero
    out = capsys.readouterr().out
    assert "refused" in out
    assert lease.read_bytes() == before  # the daemon lease is byte-for-byte untouched
    assert not lease.with_name(lease.name + ".revoked").exists()
    # daemon authorization remains valid
    daemon_status = main(["attestation", "status", str(config_path), "--scope", "daemon", "--json"])
    assert daemon_status == EXIT_OK
    assert json.loads(capsys.readouterr().out)["status"] == "valid"


def test_hostile_campaign_scope_is_refused_without_traceback(
    scope_env: tuple[Path, Path], capsys: pytest.CaptureFixture[str]
) -> None:
    """F3: path separators, colon/`< > | ? * "`, and over-long campaign ids are refused by
    validate_scope (clean nonzero exit), never a raw OSError/ValueError traceback."""
    _lease, config_path = scope_env
    overlong = "campaign:" + "x" * 300
    for hostile in (
        "campaign:a/b",
        "campaign:a\\b",
        "campaign:../escape",
        "campaign:a:b",
        'campaign:a"b',
        "campaign:a|b",
        "campaign:a*b",
        "campaign:a?b",
        "campaign:a<b>c",
        overlong,
    ):
        code = main(["attestation", "status", str(config_path), "--scope", hostile, "--json"])
        assert code == EXIT_CONFIG_INVALID
        captured = capsys.readouterr()
        assert "Traceback" not in captured.out + captured.err
        assert "scope" in captured.err.lower()

    # The record path (the reported crash) must refuse a colon id cleanly, writing no lease file.
    code = main(
        [
            "attestation",
            "record",
            str(config_path),
            "--endpoint",
            SHOKO_BASE,
            "--version",
            "5.3.3",
            "--mounts",
            "/anime read-only",
            "--settings",
            "FileQualityFilterEnabled=false,RenameOnImport=false,MoveOnImport=false,MyListAddFiles=false",
            "--hours",
            "24",
            "--scope",
            "campaign:a:b",
            "--note",
            "n",
            "--confirm",
        ]
    )
    assert code == EXIT_CONFIG_INVALID
    captured = capsys.readouterr()
    assert "Traceback" not in captured.out + captured.err
