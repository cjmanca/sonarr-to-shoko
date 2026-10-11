"""Final validation: a realistic 100-file campaign scenario suite (PLAN: Final validation).

Every scenario drives the **production** CLI, daemon, campaign, state and attestation
machinery over the synthetic ``e2e_harness`` world (MockTransport Sonarr/Shoko/mapping
source, tmp ``MappingStore``/``StateStore``, fake clocks) with deliberately SMALL
budgets (``sync.max_new_per_cycle=5``, a small ``campaign.max_batch_size``, a
rolling cap comfortably above the plan) so one campaign crosses several per-cycle
boundaries. Lease handling uses the REAL scope-aware resolver with the
F8 sibling layout (daemon lease at ``safety.attestation_file``, one
``lease.campaign-<id>.yaml`` per campaign), exactly as production wires it.

Assertions are explicit and land on persisted state, campaign reports, and
the stub's verbatim POST ledger (exactly one POST per file, no replay, zero writes on
refusal paths) -- never crash-only smoke. All offline: no network, no live services.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import e2e_harness as eh
import pytest
import yaml  # type: ignore[import-untyped]

from shoko_linker import cli
from shoko_linker.attestation import (
    SCOPE_DAEMON,
    campaign_lease_path,
    load_attestation,
)
from shoko_linker.campaign import campaign_send_progress, read_plan_file
from shoko_linker.config import load_settings
from shoko_linker.daemon import ZERO_PROGRESS_PAUSE_CYCLES, CycleMode, run_cycle
from shoko_linker.linking import SendOutcome
from shoko_linker.mapping_store import MappingStore
from shoko_linker.mapping_store import refresh as mapping_refresh
from shoko_linker.policies import RunPolicy
from shoko_linker.state import (
    ATTESTATION_PAUSE_REASON,
    FileState,
    SendToken,
    StateStore,
)

N_TEN = 10
N_TWELVE = 12
N_TWENTY = 20
N_FIVE = 5
N_TWO = 2
N_ONE = 1
N_ZERO = 0
FILES_HUNDRED = 100
PER_CYCLE = 5
BATCH_TEN = 10
ROLLING_BIG = 500
JOBS_TEN = 10
JOBS_THREE = 3
JOBS_FIVE = 5
MILESTONES = (5, 10, 20, 50, 100)
MAX_DRAIN_CYCLES = 40
CYCLE_SECONDS = 300.0
SWAP_ANIME = 999
SWAP_TVDB = 11111
ARRIVAL_ANIME = 4031
ARRIVAL_TVDB = 30031
ARRIVAL_SHOKO = 31031
ARRIVAL_SERIES = 31
AUTH_NOTE = "canary inspected: renames/deletions/MyList reviewed"
EXTRA_SWAP_ENTRY = (
    f"<anime anidbid='{SWAP_ANIME}' tvdbid='{SWAP_TVDB}' defaulttvdbseason='1'>"
    "<mapping-list><mapping anidbseason='1' tvdbseason='1'>;1-1;</mapping>"
    "</mapping-list></anime></anime-list>"
)


@pytest.fixture(autouse=True)
def _no_root_logging_leak() -> Iterator[None]:
    yield
    import logging

    for handler in list(logging.getLogger().handlers):
        logging.getLogger().removeHandler(handler)


def _tmp_posix(tmp_path: Path, name: str) -> str:
    """POSIX absolute string for ``tmp_path/<name>`` that ``Path`` resolves identically.

    Same trick as the F8 dual-lease tests: stripping the drive letter yields a rooted
    string ``Path`` maps to the same on-disk location, so the real scope-aware lease
    resolver runs against real files on the current drive.
    """
    posix = tmp_path.as_posix()
    if len(posix) >= 2 and posix[1] == ":":
        posix = posix[2:]
    return f"{posix}/{name}"


@dataclass
class CampEnv:
    """Campaign+daemon world over the harness with real F8 sibling leases.

    Unlike ``CampaignEnv`` (which flattens the lease seam for CLI-only tests), this env
    leaves ``cli.attestation_target_path`` UNPATCHED: the production scope-aware resolver
    selects the daemon lease at ``safety.attestation_file`` and each campaign's
    ``campaign-<id>`` sibling, exactly as ``cli.build_daemon_deps`` wires the drain's
    campaign-scope attestation hooks in production.
    """

    world: eh.World
    clock: eh.FakeClock
    lease: Path
    config_path: Path
    plan_path: Path
    now: str
    read_out: Callable[[], str] = lambda: ""
    stopped: dict[str, bool] = field(default_factory=lambda: {"value": False})

    def settings(self) -> Any:
        return load_settings(self.config_path)

    def open_store(self) -> StateStore:
        settings = self.settings()
        return StateStore(
            self.world.tmp_path / "state.sqlite3",
            budget_limits=cli.budget_limits_from_settings(settings),
        )

    def advance(self, seconds: float = CYCLE_SECONDS) -> str:
        self.clock.advance(seconds)
        self.now = self.clock.iso()
        return self.now

    def write_daemon_lease(self, *, hours: float = 24.0) -> None:
        eh.record_lease(
            self.lease,
            scope=SCOPE_DAEMON,
            inspected_iso=self.now,
            expires_iso=eh.iso_plus(self.now, hours * 3600.0),
        )

    def write_campaign_lease(self, campaign_id: str, *, hours: float = 24.0) -> None:
        eh.record_lease(
            campaign_lease_path(self.lease, campaign_id),
            scope=f"campaign:{campaign_id}",
            inspected_iso=self.now,
            expires_iso=eh.iso_plus(self.now, hours * 3600.0),
        )

    def campaign_lease_verdict(self, campaign_id: str) -> Any:
        return load_attestation(
            campaign_lease_path(self.lease, campaign_id),
            shoko_url=eh.SHOKO_BASE,
            now_iso=self.now,
            scope=f"campaign:{campaign_id}",
            warn_lead_seconds=self.settings().safety.attestation_warn_seconds,
        )

    def daemon_deps(self, store: StateStore) -> Any:
        settings = self.settings()
        return cli.build_daemon_deps(
            settings,
            store=store,
            mapping_store=self.world.mapping_store(),
            max_submissions=settings.sync.max_new_per_cycle,
            overrides_text=None,
            shutdown_requested=lambda: self.stopped["value"],
        )

    def cycle(self, store: StateStore, *, advance: float = CYCLE_SECONDS) -> Any:
        """One daemon apply cycle at the advanced fake clock, keyed like the CLI run."""
        iso = self.advance(advance)
        cycle_key = store.begin_cycle("cycle", iso)
        return run_cycle(
            self.daemon_deps(store),
            now_iso=iso,
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key=cycle_key,
        )

    def _cli(self, args: list[str]) -> tuple[int, str]:
        code = int(cli.main(args))
        return code, self.read_out()

    def prepare(self, limit: int) -> dict[str, Any]:
        code, out = self._cli(
            [
                "campaign",
                "prepare",
                str(self.config_path),
                "--out",
                str(self.plan_path),
                "--deadline-hours",
                "168",
                "--limit",
                str(limit),
                "--json",
            ]
        )
        assert code == cli.EXIT_OK, out
        return cast("dict[str, Any]", json.loads(out))

    def approve(self, digest: str) -> dict[str, Any]:
        code, out = self._cli(
            [
                "campaign",
                "approve",
                str(self.config_path),
                "--plan",
                str(self.plan_path),
                "--digest",
                digest,
                "--json",
            ]
        )
        assert code == cli.EXIT_OK, out
        return cast("dict[str, Any]", json.loads(out))

    def apply(self, *, batch: int) -> tuple[int, dict[str, Any] | str]:
        code, out = self._cli(
            [
                "campaign",
                "apply",
                str(self.config_path),
                "--plan",
                str(self.plan_path),
                "--batch-size",
                str(batch),
                "--json",
            ]
        )
        if code == cli.EXIT_OK:
            return code, cast("dict[str, Any]", json.loads(out))
        return code, out

    def report(self, campaign_id: str) -> dict[str, Any]:
        code, out = self._cli(
            [
                "campaign",
                "report",
                str(self.config_path),
                "--campaign-id",
                campaign_id,
                "--plan",
                str(self.plan_path),
                "--json",
            ]
        )
        assert code == cli.EXIT_OK, out
        return cast("dict[str, Any]", json.loads(out))

    def authorize(self, campaign_id: str) -> int:
        code, _out = self._cli(
            [
                "campaign",
                "authorize-drain",
                str(self.config_path),
                "--campaign-id",
                campaign_id,
                "--note",
                AUTH_NOTE,
                "--confirm",
                "--json",
            ]
        )
        return code

    def resume(self, campaign_id: str) -> int:
        code, _out = self._cli(
            [
                "campaign",
                "resume",
                str(self.config_path),
                "--campaign-id",
                campaign_id,
                "--note",
                "re-attested, operator reviewed",
                "--confirm",
                "--json",
            ]
        )
        return code

    def invalidate_campaign_lease(self, campaign_id: str) -> int:
        code, _out = self._cli(
            [
                "attestation",
                "invalidate",
                str(self.config_path),
                "--scope",
                f"campaign:{campaign_id}",
                "--note",
                "operator revoked the campaign attestation mid-campaign",
                "--confirm",
                "--json",
            ]
        )
        return code

    def record_campaign_lease_cli(self, campaign_id: str) -> int:
        code, _out = self._cli(
            [
                "attestation",
                "record",
                str(self.config_path),
                "--endpoint",
                eh.SHOKO_BASE,
                "--version",
                "5.3.3",
                "--mounts",
                "/anime read-only (host-inspected)",
                "--settings",
                "FileQualityFilterEnabled=false,RenameOnImport=false,"
                "MoveOnImport=false,MyListAddFiles=false",
                "--hours",
                "24",
                "--scope",
                f"campaign:{campaign_id}",
                "--note",
                "re-inspected, campaign lease re-recorded",
                "--confirm",
                "--json",
            ]
        )
        return code

    def plan_file_ids(self) -> set[int]:
        plan, _digest = read_plan_file(self.plan_path)
        return {item.shoko_file_id for item in plan.files}

    def posted_files(self) -> list[int]:
        return [file_id for file_id, _ids in self.world.shoko.posts]

    def posts_for(self, file_id: int) -> int:
        return sum(1 for posted, _ids in self.world.shoko.posts if posted == file_id)

    def total_sent(self, store: StateStore, campaign_id: str) -> int:
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        return campaign.total_sent


def _wire(env: CampEnv, monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch every CLI dependency seam to the scripted world -- EXCEPT the lease resolver."""
    world = env.world
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    monkeypatch.setattr(cli, "open_state_store", lambda settings: env.open_store())
    monkeypatch.setattr(cli, "state_path", lambda settings: world.tmp_path / "state.sqlite3")
    monkeypatch.setattr(cli, "current_iso", lambda: env.now)
    monkeypatch.setattr(cli, "sleep", lambda seconds: None)
    monkeypatch.setattr(cli, "interruptible_sleep", lambda seconds: None)

    def _sonarr_endpoint(settings: Any) -> Any:
        return eh.endpoint_for(world.sonarr.handler, eh.SONARR_BASE)

    def _shoko_endpoint(settings: Any) -> Any:
        return eh.endpoint_for(world.shoko.handler, eh.SHOKO_BASE)

    def _source_endpoint(store: Any, settings: Any) -> Any:
        return eh.source_endpoint(world.source.handler)

    monkeypatch.setattr(cli, "sonarr_endpoint", _sonarr_endpoint)
    monkeypatch.setattr(cli, "shoko_endpoint", _shoko_endpoint)
    monkeypatch.setattr(cli, "source_endpoint", _source_endpoint)
    monkeypatch.setattr(cli, "open_mapping_store", lambda settings: world.mapping_store())
    monkeypatch.setattr(cli, "read_overrides_text", lambda settings: None)


def _campaign_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    *,
    n_series: int,
    per_series: int,
    per_cycle: int = PER_CYCLE,
    jobs: int = JOBS_TEN,
    rolling: int = ROLLING_BIG,
    batch: int = BATCH_TEN,
    extra_xml: str = "",
) -> CampEnv:
    """A mapped backlog world + config with tiny budgets and real lease paths."""
    lease_posix = _tmp_posix(tmp_path, "lease.yaml")
    inner = eh.backlog_xml(n_series, per_series)[len("<anime-list>") : -len("</anime-list>")]
    xml = eh.xml_document(inner + extra_xml)
    sync_extra = {
        "max_new_per_cycle": per_cycle,
        "max_pending_episode_jobs": jobs,
        "rolling_new_limit": rolling,
        "new_file_grace_period_seconds": 0,
    }
    campaign = {
        "max_campaign_files": FILES_HUNDRED,
        "max_batch_size": batch,
        "daemon_auto_apply": True,
    }
    world = eh.make_world(
        tmp_path, xml, lease=Path(lease_posix), sync_extra=sync_extra, campaign=campaign
    )
    clock = eh.FakeClock()
    eh.seed_mapping(world, clock)
    eh.seed_backlog(world, n_series, per_series)
    document = eh.settings_document(
        lease=Path(lease_posix), sync_extra=sync_extra, campaign=campaign
    )
    document["safety"]["attestation_file"] = lease_posix
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(document), encoding="utf-8")
    env = CampEnv(
        world=world,
        clock=clock,
        lease=Path(lease_posix),
        config_path=config_path,
        plan_path=tmp_path / "plan.json",
        now=clock.iso(),
        read_out=lambda: capsys.readouterr().out,
    )
    _wire(env, monkeypatch)
    return env


def _drain_loop(
    env: CampEnv,
    store: StateStore,
    campaign_id: str,
    *,
    metric: Callable[[StateStore, str], int],
    target: int,
    start: int | None = None,
) -> tuple[list[int], bool, list[int]]:
    """Daemon cycles until ``metric`` reaches ``target``: per-cycle caps hold, progress is
    strict, and ``plan_complete`` is observed from persisted progress. Returns the observed
    metric sequence, the plan_complete flag, and the per-cycle submission counts."""
    observed: list[int] = [] if start is None else [start]
    submitted: list[int] = []
    plan_complete = False
    for _ in range(MAX_DRAIN_CYCLES):
        report = env.cycle(store)
        batch = report.campaign
        assert batch is not None, report.reasons
        assert batch.submitted <= PER_CYCLE
        submitted.append(batch.submitted)
        value = metric(store, campaign_id)
        if observed:
            assert value > observed[-1]
        observed.append(value)
        if batch.stopped == "plan_complete":
            plan_complete = True
        if value >= target:
            break
    return observed, plan_complete, submitted


def _assert_one_post_per_file(env: CampEnv, file_ids: set[int], total: int) -> None:
    """The campaign ledger: exactly ``total`` POSTs, one per approved file, no duplicates."""
    posts = env.world.shoko.posts
    assert len(posts) == total
    assert {file_id for file_id, _ids in posts} == file_ids
    assert all(env.posts_for(file_id) == N_ONE for file_id in file_ids)


def _rewrite_plan_file(env: CampEnv, key: str, value: float) -> str:
    """Rewrite the plan file's canonical bytes (a changed plan document) and return its digest."""
    document = json.loads(env.plan_path.read_bytes())
    document["rate_limits"] = {**document["rate_limits"], key: value}
    canonical = (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()
    env.plan_path.write_bytes(canonical)
    return hashlib.sha256(canonical).hexdigest()


def _ready_campaign(env: CampEnv, capsys: pytest.CaptureFixture[str], limit: int) -> str:
    """Plan -> approve -> both leases -> one-file manual canary -> F10 drain authorization."""
    prepared = env.prepare(limit)
    assert prepared["files"] == limit
    assert env.world.shoko.posts_count() == N_ZERO  # prepare is a dry-run: zero writes
    campaign_id = str(prepared["campaign_id"])
    env.approve(str(prepared["approval_digest"]))
    env.write_daemon_lease()
    env.write_campaign_lease(campaign_id)
    code, payload = env.apply(batch=N_ONE)
    assert code == cli.EXIT_OK
    assert isinstance(payload, dict)
    assert payload["submitted"] == N_ONE
    assert payload["verified"] == N_ONE
    assert env.posts_for(env.posted_files()[N_ZERO]) == N_ONE
    assert env.authorize(campaign_id) == cli.EXIT_OK
    capsys.readouterr()
    return campaign_id


def _reprepare_campaign(
    env: CampEnv, capsys: pytest.CaptureFixture[str], limit: int
) -> tuple[str, int]:
    """The operator's post-drift recovery: re-prepare, approve, lease, canary, authorize."""
    second = env.prepare(limit)
    campaign_id = str(second["campaign_id"])
    env.approve(str(second["approval_digest"]))
    env.write_campaign_lease(campaign_id)
    code, payload = env.apply(batch=N_ONE)
    assert code == cli.EXIT_OK
    assert isinstance(payload, dict)
    assert payload["submitted"] == N_ONE
    assert env.authorize(campaign_id) == cli.EXIT_OK
    capsys.readouterr()
    return campaign_id, int(second["files"])


def _drain(store: StateStore, env: CampEnv, campaign_id: str, *, until: int) -> list[Any]:
    """Run daemon cycles until campaign progress reaches ``until`` (per-cycle caps hold)."""
    reports: list[Any] = []
    while env.total_sent(store, campaign_id) < until:
        report = env.cycle(store)
        reports.append(report)
        batch = report.campaign
        assert batch is not None, report.reasons
        assert batch.submitted <= PER_CYCLE  # per-cycle cap respected inside each cycle
    return reports


def _add_new_arrival(env: CampEnv) -> eh.FileSpec:
    spec = eh.FileSpec(ARRIVAL_SERIES, ARRIVAL_SERIES * 1000 + 1, ARRIVAL_SHOKO, (0,))
    series = eh.SeriesSpec(
        ARRIVAL_SERIES,
        ARRIVAL_TVDB,
        (ARRIVAL_ANIME,),
        (eh.EpisodeSpec(season=1, number=N_ONE),),
    )
    eh.add_new_arrival(env.world, series, spec)
    return spec


def _refresh_generation(env: CampEnv, xml: str) -> None:
    """Force a mapping refresh (the mid-batch concurrent-refresh producer)."""
    env.world.source.set_xml(xml)
    store = MappingStore(env.world.mapping_dir, source_url=eh.SOURCE_URL)
    endpoint = eh.source_endpoint(env.world.source.handler)
    try:
        mapping_refresh(
            store,
            endpoint,
            now=datetime.fromisoformat(env.now),
            refresh_hours=168.0,
            force=True,
        )
    finally:
        endpoint.client.close()


def _arrival_xml() -> str:
    return eh.range_entry(ARRIVAL_ANIME, ARRIVAL_TVDB, 1, 1)


# ------------------------------------------------------------------ 1. 100-file campaign


def test_f_campaign_hundred_file_daemon_drain_crosses_every_cycle_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """The headline campaign: 100 files, max_new_per_cycle=5, batch 10 -> many per-cycle
    boundaries crossed; the drain is daemon-only after the canary + authorization, exactly
    one POST per file, and ``plan_complete`` comes from persisted progress."""
    env = _campaign_env(tmp_path, monkeypatch, capsys, n_series=N_TWENTY, per_series=N_FIVE)
    campaign_id = _ready_campaign(env, capsys, FILES_HUNDRED)
    plan_ids = env.plan_file_ids()
    assert len(plan_ids) == FILES_HUNDRED
    store = env.open_store()
    try:
        start = env.total_sent(store, campaign_id)
        sent, plan_complete_seen, submitted = _drain_loop(
            env, store, campaign_id, metric=env.total_sent, target=FILES_HUNDRED, start=start
        )
        assert sent[-1] == FILES_HUNDRED
        assert plan_complete_seen  # stopped reflects persisted progress, not a scan count
        assert any(value == PER_CYCLE for value in submitted)  # the per-cycle cap binds
        for threshold in MILESTONES:
            assert any(value >= threshold for value in sent)
        posts = env.world.shoko.posts
        assert len(posts) == FILES_HUNDRED  # canary + drain, exactly one POST per file
        assert {file_id for file_id, _ids in posts} == plan_ids  # never widens scope
        assert all(env.posts_for(file_id) == N_ONE for file_id in plan_ids)
        progress = store.campaign_progress(campaign_id)
        assert progress[FileState.VERIFIED.value] == FILES_HUNDRED
        assert campaign_send_progress(store, campaign_id) == FILES_HUNDRED
    finally:
        store.close()


# ---------------------------------------------------------------------- 2. restart


def _crash_after_post(env: CampEnv, store: StateStore, monkeypatch: pytest.MonkeyPatch) -> int:
    """Crash the cycle right after a POST is delivered and Shoko's job is still pending.

    Returns the file whose row durably persists as ``request_may_have_been_sent``.
    """

    def crashing_record(
        token: SendToken,
        outcome: SendOutcome,
        *,
        iso: str,
        http_status: int | None = None,
        detail: str = "",
    ) -> FileState:
        raise RuntimeError("simulated crash after the POST, before the outcome was recorded")

    original = store.record_send_outcome
    env.world.shoko.link_on_post = False  # the ManualLinkJob is still pending server-side
    monkeypatch.setattr(store, "record_send_outcome", crashing_record)
    try:
        with pytest.raises(RuntimeError, match="simulated crash"):
            env.cycle(store)
    finally:
        monkeypatch.setattr(store, "record_send_outcome", original)
    return env.posted_files()[-1]


def _crash_after_prepare(env: CampEnv, store: StateStore, monkeypatch: pytest.MonkeyPatch) -> int:
    """Crash the cycle after ``prepare_send`` committed and before the send.

    Returns the file whose row durably persists as ``prepared_not_sent``.
    """

    def crashing_mark(token: SendToken, *, iso: str) -> None:
        raise RuntimeError("simulated crash after prepare_send, before the send")

    original = store.mark_sent
    monkeypatch.setattr(store, "mark_sent", crashing_mark)
    try:
        with pytest.raises(RuntimeError, match="simulated crash"):
            env.cycle(store)
    finally:
        monkeypatch.setattr(store, "mark_sent", original)
    rows = store.files_in_state(FileState.PREPARED_NOT_SENT)
    assert len(rows) == N_ONE
    return rows[0].shoko_file_id


def _crash_to_restart_state(
    env: CampEnv,
    store: StateStore,
    monkeypatch: pytest.MonkeyPatch,
    campaign_id: str,
    plan_ids: set[int],
) -> tuple[int, int, int, int, str]:
    """Drain to 50, crash one send after the POST and one after prepare_send, and verify the
    two durable restart rows. Returns (sent_file, prepared_file, posts, progress, auth_iso)."""
    _drain(store, env, campaign_id, until=N_FIVE * 10)
    authorized = store.get_campaign(campaign_id)
    assert authorized is not None
    authorized_iso = str(authorized.drain_authorized_iso)

    sent_file = _crash_after_post(env, store, monkeypatch)
    sent_row = store.get_file(sent_file)
    assert sent_row is not None
    assert sent_row.state is FileState.REQUEST_MAY_HAVE_BEEN_SENT

    prepared_file = _crash_after_prepare(env, store, monkeypatch)
    assert prepared_file in plan_ids
    assert prepared_file != sent_file
    prepared_row = store.get_file(prepared_file)
    assert prepared_row is not None
    assert prepared_row.state is FileState.PREPARED_NOT_SENT
    assert env.posts_for(sent_file) == N_ONE
    assert env.posts_for(prepared_file) == N_ZERO

    posts = env.world.shoko.posts_count()
    progress = campaign_send_progress(store, campaign_id)
    assert progress < FILES_HUNDRED
    return sent_file, prepared_file, posts, progress, authorized_iso


def test_f_campaign_restart_halfway_never_replays_pending_or_sent_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Restart at ~50/100 sent with one ``prepared_not_sent`` and one
    ``request_may_have_been_sent`` row persisted: the pending row completes with
    its single POST, the sent row verifies by inspection (never re-POSTed), the
    drain authorization and both leases survive the reopen."""
    env = _campaign_env(tmp_path, monkeypatch, capsys, n_series=N_TWENTY, per_series=N_FIVE)
    campaign_id = _ready_campaign(env, capsys, FILES_HUNDRED)
    plan_ids = env.plan_file_ids()
    store = env.open_store()
    try:
        sent_file, prepared_file, posts_before, progress_before, authorized_iso = (
            _crash_to_restart_state(env, store, monkeypatch, campaign_id, plan_ids)
        )
    finally:
        store.close()

    env.world.shoko.link_on_post = True
    episode_ids = env.world.shoko.posts[-1][1]
    env.world.shoko.materialize(sent_file, episode_ids)  # the async job lands (GET-verifiable)
    reopened = env.open_store()
    try:
        assert campaign_send_progress(reopened, campaign_id) == progress_before
        resumed = reopened.get_campaign(campaign_id)
        assert resumed is not None
        assert resumed.drain_authorized_iso == authorized_iso
        assert env.campaign_lease_verdict(campaign_id).valid_for_apply is True
        campaign_lease = campaign_lease_path(env.lease, campaign_id)
        assert campaign_lease.exists()
        assert env.lease.exists()
        progress, plan_complete_seen, _submitted = _drain_loop(
            env,
            reopened,
            campaign_id,
            metric=campaign_send_progress,
            target=FILES_HUNDRED,
            start=progress_before,
        )
        assert progress[-1] == FILES_HUNDRED
        assert plan_complete_seen
        # The campaign counter counts durably recorded sends; the row whose outcome commit
        # crashed is verified from its ledger row and never counted twice (99 < 100).
        assert env.total_sent(reopened, campaign_id) == FILES_HUNDRED - N_ONE
        assert env.posts_for(sent_file) == N_ONE  # the sent row never re-POSTed
        assert env.posts_for(prepared_file) == N_ONE  # the pending row completed once
        assert env.world.shoko.posts_count() == posts_before + (FILES_HUNDRED - progress_before)
        _assert_one_post_per_file(env, plan_ids, FILES_HUNDRED)
        closed_sent = reopened.get_file(sent_file)
        closed_prepared = reopened.get_file(prepared_file)
        assert closed_sent is not None
        assert closed_prepared is not None
        assert closed_sent.state is FileState.VERIFIED
        assert closed_prepared.state is FileState.VERIFIED
        assert reopened.campaign_progress(campaign_id)[FileState.VERIFIED.value] == FILES_HUNDRED
    finally:
        reopened.close()


# ----------------------------------------------------------------- 3. scope discipline


def test_f_campaign_never_widens_scope(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Every campaign POST targets a plan-file file; a new mapping never enters the
    approved set; a rewritten plan file pauses (``plan_drift``) with ZERO POSTs."""
    env = _campaign_env(
        tmp_path, monkeypatch, capsys, n_series=N_TWELVE, per_series=N_ONE, extra_xml=_arrival_xml()
    )
    campaign_id = _ready_campaign(env, capsys, N_TWELVE)
    plan_ids = env.plan_file_ids()
    store = env.open_store()
    try:
        _drain(store, env, campaign_id, until=N_FIVE + N_ONE)
        arrival = _add_new_arrival(env)
        report = env.cycle(store)
        assert report.campaign is not None
        assert report.counts.submitted == N_ONE  # the daemon submits it under its OWN lease
        assert arrival.shoko_id in env.posted_files()
        assert arrival.shoko_id not in plan_ids  # never a campaign target
        record = store.get_file(arrival.shoko_id)
        assert record is not None
        assert record.campaign_id is None  # never bound to the approved plan
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert len(campaign.bindings) == N_TWELVE  # binding set never grows
        assert store.bind_campaign_files(campaign_id, env.now) <= N_TWELVE

        plan_bytes = env.plan_path.read_bytes()
        _rewrite_plan_file(env, "rolling_new_limit", 1234.0)
        posts_before = env.world.shoko.posts_count()
        report = env.cycle(store)
        assert report.campaign is None
        assert env.world.shoko.posts_count() == posts_before  # zero POSTs on the drift path
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status == "paused"
        assert campaign.status_reason == "plan_drift"
        campaign_targets = {file_id for file_id in env.posted_files() if file_id in plan_ids}
        assert campaign_targets <= plan_ids  # every campaign POST targets an approved file
        # the daemon-path arrival is the only non-campaign POST
        assert set(env.posted_files()) - plan_ids == {arrival.shoko_id}
        assert env.resume(campaign_id) == cli.EXIT_OK
        env.plan_path.write_bytes(plan_bytes)  # the operator restores the reviewed plan file
        for _ in range(N_TEN):
            cycle = env.cycle(store)
            batch = cycle.campaign
            if batch is not None and batch.stopped == "plan_complete":
                break
        assert env.total_sent(store, campaign_id) == N_TWELVE
        assert {file_id for file_id, _ids in env.world.shoko.posts} <= plan_ids | {arrival.shoko_id}
        assert env.posts_for(arrival.shoko_id) == N_ONE
    finally:
        store.close()


# ---------------------------------------------------------------- 4. authorization gate


def test_f_campaign_drain_authorization_gate_and_digest_staleness(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """F10 end-to-end: no drain before the authorization (zero POSTs); a digest change
    mid-run skips the campaign with ``drain_digest_stale`` and zero POSTs until the
    operator re-authorizes, then the drain resumes and completes."""
    env = _campaign_env(tmp_path, monkeypatch, capsys, n_series=N_TWELVE, per_series=N_ONE)
    prepared = env.prepare(N_TWELVE)
    campaign_id = str(prepared["campaign_id"])
    env.approve(str(prepared["approval_digest"]))
    env.write_daemon_lease()
    env.write_campaign_lease(campaign_id)
    code, payload = env.apply(batch=N_ONE)
    assert code == cli.EXIT_OK
    store = env.open_store()
    try:
        posts_before = env.world.shoko.posts_count()
        for _ in range(N_TWO):
            report = env.cycle(store)
            assert report.campaign is None  # a verified canary alone never opens the drain
        assert env.world.shoko.posts_count() == posts_before  # zero POSTs, gate closed
        report = env.report(campaign_id)
        assert report["drain_selection"] == "drain_not_authorized"

        assert env.authorize(campaign_id) == cli.EXIT_OK
        capsys.readouterr()
        _drain(store, env, campaign_id, until=N_FIVE + N_ONE)

        new_digest = _rewrite_plan_file(env, "rolling_new_limit", 777.0)
        connection = sqlite3.connect(str(store.path))
        connection.execute(
            "UPDATE campaigns SET digest = ? WHERE campaign_id = ?", (new_digest, campaign_id)
        )
        connection.commit()
        connection.close()
        posts_before = env.world.shoko.posts_count()
        for _ in range(N_TWO):
            report = env.cycle(store)
            assert report.campaign is None  # stale binding -> skipped, not drained
        assert env.world.shoko.posts_count() == posts_before  # zero POSTs under the stale grant
        assert env.report(campaign_id)["drain_selection"] == "drain_digest_stale"

        assert env.authorize(campaign_id) == cli.EXIT_OK
        capsys.readouterr()
        assert env.report(campaign_id)["drain_selection"] == "authorized"
        _drain(store, env, campaign_id, until=N_TWELVE)
        assert env.world.shoko.posts_count() == N_TWELVE
        assert len(set(env.posted_files())) == N_TWELVE
    finally:
        store.close()


# ------------------------------------------------------------------ 5. lease revocation


def test_f_campaign_lease_revocation_pauses_only_the_campaign(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """F8 end-to-end: an operator ``attestation invalidate --scope campaign:<id>`` mid-campaign
    pauses ONLY that campaign (no global daemon pause, zero campaign POSTs), the daemon keeps
    processing new arrivals under its valid daemon lease, and re-record + ``campaign resume``
    + re-authorize resumes the drain."""
    env = _campaign_env(
        tmp_path, monkeypatch, capsys, n_series=N_TWENTY, per_series=N_ONE, extra_xml=_arrival_xml()
    )
    campaign_id = _ready_campaign(env, capsys, N_TWENTY)
    store = env.open_store()
    try:
        _drain(store, env, campaign_id, until=N_TEN + N_ONE)
        _add_new_arrival(env)
        sent_before = env.total_sent(store, campaign_id)
        posts_before = env.world.shoko.posts_count()
        assert env.invalidate_campaign_lease(campaign_id) == cli.EXIT_OK
        capsys.readouterr()
        revoked = campaign_lease_path(env.lease, campaign_id)
        assert not revoked.exists()
        assert (revoked.parent / (revoked.name + ".revoked")).exists()  # audit trail kept

        report = env.cycle(store)
        assert report.alive is True
        assert report.campaign is None  # the campaign left drain selection (paused)
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status == "paused"
        assert campaign.status_reason == ATTESTATION_PAUSE_REASON
        assert store.get_meta(f"campaign_attestation_gate:{campaign_id}") == "paused"
        # F8 isolation: NO global daemon pause; the daemon lease stays valid.
        assert store.budget_view().paused_reason is None
        assert store.get_meta("attestation_gate") == "valid"
        assert report.apply_eligible is True
        assert report.counts.submitted == N_ONE  # the new arrival is applied under the daemon lease
        assert env.total_sent(store, campaign_id) == sent_before  # zero campaign submissions
        assert env.world.shoko.posts_count() == posts_before + N_ONE  # the arrival's single POST
        campaign_posts = {
            file_id for file_id, _ids in env.world.shoko.posts if file_id in env.plan_file_ids()
        }
        assert len(campaign_posts) == sent_before

        assert env.record_campaign_lease_cli(campaign_id) == cli.EXIT_OK
        assert env.campaign_lease_verdict(campaign_id).valid_for_apply is True
        assert env.resume(campaign_id) == cli.EXIT_OK
        assert env.authorize(campaign_id) == cli.EXIT_OK
        capsys.readouterr()
        _drain(store, env, campaign_id, until=N_TWENTY)
        assert env.total_sent(store, campaign_id) == N_TWENTY
        assert len(set(env.posted_files()) & env.plan_file_ids()) == N_TWENTY
    finally:
        store.close()


# -------------------------------------------------------------------- 6. shutdown seam


def test_f_campaign_shutdown_stops_new_submissions_and_restart_completes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """F11 end-to-end: the SIGTERM flag flips inside a POST; new submissions stop mid-batch,
    the remainder stays queueable, and a restart with fresh deps (flag cleared) completes the
    campaign with no duplicate POSTs."""
    env = _campaign_env(tmp_path, monkeypatch, capsys, n_series=N_TWENTY, per_series=N_ONE)
    campaign_id = _ready_campaign(env, capsys, N_TWENTY)
    plan_ids = env.plan_file_ids()
    store = env.open_store()
    try:
        _drain(store, env, campaign_id, until=N_FIVE + N_ONE)

        def flip() -> None:
            env.stopped["value"] = True  # SIGTERM lands mid-batch, right after this POST

        env.world.shoko.on_post = flip
        report = env.cycle(store)
        env.world.shoko.on_post = None
        batch = report.campaign
        assert batch is not None
        assert batch.submitted == N_ONE  # the in-flight send completes; no new ones start
        assert batch.quarantined == N_ZERO  # non-destructive: a shutdown defers, never quarantines
        assert any(reason.startswith("shutdown_requested:") for reason in batch.reasons)
        remaining = [
            store.get_file(file_id)
            for file_id in plan_ids
            if file_id not in {posted for posted, _ids in env.world.shoko.posts}
        ]
        assert remaining
        assert all(record is not None and record.state is FileState.PLANNED for record in remaining)
    finally:
        store.close()

    env.stopped["value"] = False  # the restart: fresh store + fresh deps, flag cleared
    reopened = env.open_store()
    try:
        _drain(reopened, env, campaign_id, until=N_TWENTY)
        assert env.world.shoko.posts_count() == N_TWENTY
        assert {file_id for file_id, _ids in env.world.shoko.posts} == plan_ids
        assert all(env.posts_for(file_id) == N_ONE for file_id in plan_ids)
        assert reopened.campaign_progress(campaign_id)[FileState.VERIFIED.value] == N_TWENTY
    finally:
        reopened.close()


# ---------------------------------------------------------------- 7. mapping swap


def _swap_mid_batch(env: CampEnv, store: StateStore, swapped_xml: str) -> Any:
    """Run one daemon cycle whose FIRST POST triggers a concurrent generation swap."""
    fired = {"done": False}

    def swap_once() -> None:
        if not fired["done"]:
            fired["done"] = True
            _refresh_generation(env, swapped_xml)

    env.world.shoko.on_post = swap_once
    try:
        report = env.cycle(store)
    finally:
        env.world.shoko.on_post = None
    return report


def test_f_campaign_mapping_swap_mid_drain_defers_then_resumes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """F11 end-to-end: a concurrent generation swap between candidates defers the rest of
    the batch (``mapping_generation_changed``, zero POSTs under the swapped generation);
    later cycles refuse at the batch guard (``mapping_drift``, zero POSTs); a clean cycle
    back on the approved digests re-validates (the campaign never rides the moved
    generation), and the operator's re-prepare + re-approve resumes the drain to
    completion with exactly one POST per file."""
    env = _campaign_env(tmp_path, monkeypatch, capsys, n_series=N_TWENTY, per_series=N_ONE)
    campaign_id = _ready_campaign(env, capsys, N_TWENTY)
    plan_ids = env.plan_file_ids()
    base_xml = env.world.source.xml.decode("utf-8")
    swapped = base_xml.replace("</anime-list>", EXTRA_SWAP_ENTRY)
    store = env.open_store()
    try:
        _drain(store, env, campaign_id, until=N_FIVE + N_ONE)

        posts_before = env.world.shoko.posts_count()
        report = _swap_mid_batch(env, store, swapped)
        batch = report.campaign
        assert batch is not None
        assert batch.submitted == N_ONE  # only the in-flight candidate POSTs
        assert batch.quarantined == N_ZERO
        assert any(
            reason.startswith("send_stale:") and "mapping_generation_changed" in reason
            for reason in batch.reasons
        )
        assert env.world.shoko.posts_count() == posts_before + N_ONE  # zero POSTs under the swap
        for record in store.files_in_state(FileState.PLANNED):
            assert record.state is FileState.PLANNED  # deferred remainder stays queueable

        sent_before = env.total_sent(store, campaign_id)
        posts_before = env.world.shoko.posts_count()
        for _ in range(N_TWO):
            report = env.cycle(store)
            assert report.campaign is None
        assert env.world.shoko.posts_count() == posts_before  # zero POSTs while mapping_drift
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status == "paused"
        assert campaign.status_reason == "mapping_drift"
        assert env.total_sent(store, campaign_id) == sent_before

        _refresh_generation(env, base_xml)  # a clean cycle back on the approved digests
        # The paused campaign never rides the moved generation: the operator's documented
        # recovery is re-prepare + re-approve (a new plan binding), and the daemon drain
        # resumes the remainder under it.
        report = env.cycle(store)
        assert report.campaign is None  # still paused: no POST rides a stale binding
        assert env.world.shoko.posts_count() == posts_before
        remaining = N_TWENTY - env.total_sent(store, campaign_id)
        campaign_id_2, planned_2 = _reprepare_campaign(env, capsys, N_TWENTY)
        assert planned_2 == remaining
        plan_ids_2 = env.plan_file_ids()
        assert plan_ids_2 <= plan_ids
        _drain(store, env, campaign_id_2, until=remaining)
        _assert_one_post_per_file(env, plan_ids, N_TWENTY)
        assert all(env.posts_for(file_id) == N_ONE for file_id in plan_ids_2)
    finally:
        store.close()


# ------------------------------------------------------------- 8. outstanding jobs


def test_f_campaign_outstanding_jobs_cap_defers_and_progresses_as_jobs_settle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """F9 end-to-end: with Shoko's async jobs pending at the outstanding-jobs cap the
    campaign defers (no budget bypass -- the pending-job counter never exceeds the cap) and
    progresses as jobs settle; every file lands on exactly one POST."""
    env = _campaign_env(
        tmp_path,
        monkeypatch,
        capsys,
        n_series=N_TWENTY,
        per_series=N_ONE,
        jobs=JOBS_THREE,
        per_cycle=JOBS_FIVE,
    )
    campaign_id = _ready_campaign(env, capsys, N_TWENTY)
    env.world.shoko.link_on_post = False  # ManualLinkJobs stay pending: sends hold slots
    store = env.open_store()
    try:
        first = env.cycle(store)
        batch = first.campaign
        assert batch is not None
        assert batch.submitted <= JOBS_THREE  # the outstanding-jobs cap, not the file cap
        assert any("pending_episode_jobs_exhausted" in reason for reason in batch.reasons)
        assert store.budget_view().pending_episode_jobs <= JOBS_THREE
        assert store.live_send_counts()[1] <= JOBS_THREE

        cycles = 0
        while campaign_send_progress(store, campaign_id) < N_TWENTY and cycles < MAX_DRAIN_CYCLES:
            for file_id, episode_ids in list(env.world.shoko.posts):
                row = env.world.shoko.rows[file_id]
                if not row.xrefs:
                    env.world.shoko.materialize(file_id, episode_ids)  # async jobs land
            report = env.cycle(store)
            cycles += 1
            batch = report.campaign
            assert batch is not None, report.reasons
            assert batch.submitted <= JOBS_THREE  # the jobs cap binds every cycle
            assert store.budget_view().pending_file_requests <= JOBS_FIVE
            assert store.budget_view().pending_episode_jobs <= JOBS_THREE
            assert store.live_send_counts()[1] <= JOBS_THREE
        assert env.total_sent(store, campaign_id) == N_TWENTY
        for file_id, episode_ids in list(env.world.shoko.posts):
            row = env.world.shoko.rows[file_id]
            if not row.xrefs:
                env.world.shoko.materialize(file_id, episode_ids)
        env.cycle(store)  # the last async jobs settle and close their rows
        assert env.world.shoko.posts_count() == N_TWENTY
        assert all(env.posts_for(file_id) == N_ONE for file_id in env.plan_file_ids())
        assert store.campaign_progress(campaign_id)[FileState.VERIFIED.value] == N_TWENTY
        assert store.budget_view().pending_episode_jobs == N_ZERO
        assert store.live_send_counts() == (N_ZERO, N_ZERO)
    finally:
        store.close()


# ----------------------------------------------------------------- 9. zero-progress honesty


def test_f_campaign_all_pending_targets_trip_zero_progress_then_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """F9 end-to-end honesty: a campaign whose remaining files are ALL non-materializing
    pending targets trips the zero-progress pause in exactly ``ZERO_PROGRESS_PAUSE_CYCLES``
    cycles (no infinite full-sweep churn, zero POSTs) and resumes cleanly once Shoko's
    metadata materializes."""
    env = _campaign_env(tmp_path, monkeypatch, capsys, n_series=N_TWELVE, per_series=N_ONE)
    for series in range(N_TWO, N_TWELVE + 1):  # every campaign file except the canary's is pending
        env.world.shoko.pending.add((4000 + series) * 1000 + 1)  # the AniDB episode id
    campaign_id = _ready_campaign(env, capsys, N_TWELVE)
    store = env.open_store()
    try:
        for index in range(ZERO_PROGRESS_PAUSE_CYCLES - 1):
            report = env.cycle(store)
            batch = report.campaign
            assert batch is not None, report.reasons
            assert batch.submitted == N_ZERO  # pending targets defer: zero submissions
            assert batch.progress == N_ZERO
            assert any(reason.startswith("pending_target:") for reason in batch.reasons)
            campaign = store.get_campaign(campaign_id)
            assert campaign is not None
            assert campaign.status == "approved", index
        posts_before = env.world.shoko.posts_count()
        report = env.cycle(store)  # the Nth consecutive zero-progress drain pauses it
        assert any(reason.startswith("campaign_zero_progress_pause:") for reason in report.reasons)
        campaign = store.get_campaign(campaign_id)
        assert campaign is not None
        assert campaign.status == "paused"
        assert campaign.status_reason == "no_queueable_candidates"
        assert env.world.shoko.posts_count() == posts_before  # zero POSTs across the churn
        report = env.cycle(store)
        assert report.campaign is None  # the paused campaign drops out: no further sweeps
        assert env.world.shoko.posts_count() == posts_before

        env.world.shoko.pending.clear()
        assert env.resume(campaign_id) == cli.EXIT_OK
        assert env.authorize(campaign_id) == cli.EXIT_OK
        capsys.readouterr()
        _drain(store, env, campaign_id, until=N_TWELVE)
        assert env.world.shoko.posts_count() == N_TWELVE
        assert all(env.posts_for(file_id) == N_ONE for file_id in env.plan_file_ids())
        assert store.campaign_progress(campaign_id)[FileState.VERIFIED.value] == N_TWELVE
    finally:
        store.close()


# --------------------------------------------------------------------- scenario matrix


def test_f_campaign_module_scenario_matrix_is_complete() -> None:
    source = Path(__file__).read_text(encoding="utf-8")
    expected = [
        "test_f_campaign_hundred_file_daemon_drain_crosses_every_cycle_cap",
        "test_f_campaign_restart_halfway_never_replays_pending_or_sent_rows",
        "test_f_campaign_never_widens_scope",
        "test_f_campaign_drain_authorization_gate_and_digest_staleness",
        "test_f_campaign_lease_revocation_pauses_only_the_campaign",
        "test_f_campaign_shutdown_stops_new_submissions_and_restart_completes",
        "test_f_campaign_mapping_swap_mid_drain_defers_then_resumes",
        "test_f_campaign_outstanding_jobs_cap_defers_and_progresses_as_jobs_settle",
        "test_f_campaign_all_pending_targets_trip_zero_progress_then_resume",
    ]
    missing = [name for name in expected if f"def {name}(" not in source]
    assert missing == []
