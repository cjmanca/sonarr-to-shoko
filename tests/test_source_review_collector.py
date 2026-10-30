"""Production adapter against mocked HTTP GETs and real SQLite/mapping cache."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import e2e_harness as eh
import httpx
import pytest
from test_service_e2e import ServiceShokoStub, _confirm_setup, _service_world
from test_source_rebaseline import SESSION, tables

from shoko_linker import cli
from shoko_linker.bootstrap import BootstrapSettings
from shoko_linker.config import SecretRef
from shoko_linker.daemon import META_SHOKO_DISCOVERY_PROGRESS
from shoko_linker.onboarding import OnboardingManager, OnboardingStatus
from shoko_linker.service_settings import load_service_settings
from shoko_linker.shoko import collect_shoko_discovery
from shoko_linker.sonarr import collect_sonarr_inventory
from shoko_linker.source_review import (
    SourceReviewController,
    SourceReviewEvidence,
    SourceReviewWork,
)
from shoko_linker.source_review_collector import FreshSourceReviewCollector
from shoko_linker.state import (
    META_SERVICE_AUTHORIZATION_REVISION,
    META_SERVICE_SETTINGS,
    StateStore,
    open_state,
)


def _snapshot(store: StateStore) -> dict[str, list[tuple[Any, ...]]]:
    return tables(store) | {
        "meta": [tuple(row) for row in store._conn.execute("SELECT * FROM meta ORDER BY key")]
    }


@dataclass
class Harness:
    world: eh.World
    clock: eh.FakeClock
    runtime: cli._NormalServiceRuntime
    collector: FreshSourceReviewCollector
    controller: SourceReviewController
    sonarr_hook: Callable[[httpx.Request], None] | None = None
    shoko_hook: Callable[[httpx.Request], None] | None = None
    refs: object = None
    change_series_path: str | None = None
    requests: list[tuple[str, str]] | None = None

    @property
    def path(self) -> Path:
        return self.runtime.settings.database_path

    def tick(self, seconds: float = 60) -> bool:
        return self.controller.worker_tick(self.clock.mono + seconds)

    def ready(self) -> str:
        self.controller.request_assessment(SESSION)
        assert self.tick()
        view = self.controller.view(SESSION)
        assert view.status == "ready", view.message
        assert view.token is not None
        return view.token

    def collect(self) -> SourceReviewEvidence:
        with open_state(self.path) as store:
            work = SourceReviewWork(
                self.clock.iso(),
                self.collector.capture_bindings(store, self.clock.mono + 60),
                lambda: False,
            )
            result = self.collector.collect(store, work, self.clock.mono + 60)
        assert result is not None
        return result


@pytest.fixture
def h(tmp_path: Path) -> Iterator[Harness]:
    world = _service_world(tmp_path)
    clock = eh.FakeClock(start_iso=datetime.now(UTC).isoformat())
    eh.seed_mapping(world, clock)
    bootstrap = BootstrapSettings(
        eh.SONARR_BASE,
        SecretRef("SONARR_API_KEY"),
        eh.SHOKO_BASE,
        SecretRef("SHOKO_API_KEY"),
        tmp_path / "state.sqlite3",
    )
    requests: list[tuple[str, str]] = []
    holder: list[Harness] = []

    def sonarr_get(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        requests.append(("sonarr", request.url.path))
        response = world.sonarr.handler(request)
        if request.url.path == "/api/v3/series":
            payload = response.json()
            for item in payload:
                item["path"] = f"{item['rootFolderPath']}/series{item['id']}"
                if holder and holder[0].change_series_path:
                    item["path"] = holder[0].change_series_path
            response = httpx.Response(200, json=payload, request=request)
        if holder and holder[0].sonarr_hook is not None:
            holder[0].sonarr_hook(request)
        return response

    def shoko_get(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        requests.append(("shoko", request.url.path))
        response = world.shoko.handler(request)
        if holder and holder[0].refs is not None and request.url.path == "/api/v3/File":
            payload = response.json()
            for item in payload["List"]:
                item["SeriesIDs"] = holder[0].refs
            response = httpx.Response(200, json=payload, request=request)
        if holder and holder[0].shoko_hook is not None:
            holder[0].shoko_hook(request)
        return response

    sonarr = eh.endpoint_for(sonarr_get, eh.SONARR_BASE)
    shoko = eh.endpoint_for(shoko_get, eh.SHOKO_BASE)
    runtime = cli._NormalServiceRuntime(bootstrap, sonarr, shoko, cli.StopState())
    _confirm_setup(runtime.setup)
    with open_state(bootstrap.database_path) as store:
        store.create_dashboard_account("operator", "scrypt$test", b"s" * 32, clock.iso())
        store.create_dashboard_session(SESSION, "b" * 64, 2_000_000_000, clock.iso())
        store.check_source_continuity(
            "old", "old", iso=clock.iso(), roots_fingerprint="old", discovery_total=3
        )
        store.check_source_continuity(
            "old", "old", iso=clock.iso(), roots_fingerprint="old", discovery_total=2
        )
        OnboardingManager(store).pause(iso=clock.iso())
    collector = FreshSourceReviewCollector(
        bootstrap, sonarr, shoko, runtime.setup, now=lambda: clock.now, monotonic=clock.monotonic
    )
    controller = SourceReviewController(
        bootstrap.database_path,
        collector,
        wake=lambda: None,
        shutdown_requested=runtime.state.should_stop,
        now=lambda: clock.now,
        monotonic=clock.monotonic,
    )
    runtime.source_review = controller
    harness = Harness(world, clock, runtime, collector, controller, requests=requests)
    holder.append(harness)
    requests.clear()
    try:
        yield harness
    finally:
        sonarr.client.close()
        shoko.client.close()


def test_fresh_first_and_confirmation_sweeps_preserve_user_pause(h: Harness) -> None:
    # Setup already has valid inventories; the review still fetches every selected child.
    assert h.runtime.setup._inventory_cache is not None
    token = h.ready()
    assert h.requests is not None
    first = list(h.requests)
    assert ("sonarr", "/api/v3/episode") in first
    assert ("sonarr", "/api/v3/episodefile") in first
    assert ("shoko", "/api/v3/File") in first
    h.requests.clear()
    h.controller.request_confirmation(token, SESSION)
    h.clock.advance(1)
    assert h.tick()
    assert h.controller.view(SESSION).status == "confirmed"
    assert ("sonarr", "/api/v3/episode") in h.requests
    assert ("sonarr", "/api/v3/episodefile") in h.requests
    assert ("shoko", "/api/v3/File") in h.requests
    with open_state(h.path) as store:
        assert store.get_service_pause() == (True, "user_paused")
        assert not store.get_source_continuity_review().requires_rebaseline
    assert not h.world.shoko.posts


def test_existing_valid_daemon_caches_are_ignored_and_preserved(h: Harness) -> None:
    with open_state(h.path) as store:
        settings = h.collector._settings(store, h.clock.mono + 60)
        inventory = collect_sonarr_inventory(h.runtime.sonarr, settings)
        assert inventory.series_manifest_hash is not None
        store.save_sonarr_inventory_cache(
            inventory.series_manifest_hash,
            inventory.series_cursor,
            (),
            {
                identifier: json.dumps(
                    {
                        "episodes": list(children[0]),
                        "files": list(children[1]),
                        "fetched_at": h.clock.iso(),
                    }
                )
                for identifier, children in (inventory.fetched_series_children or {}).items()
            },
        )
        discovery = collect_shoko_discovery(
            h.runtime.shoko, endpoint_identity=eh.SHOKO_BASE, observed_at=h.clock.iso()
        )
        progress = discovery.progress
        assert progress is not None
        document = {
            "version": 1,
            "endpoint_identity": progress.endpoint_identity,
            "folders_fingerprint": progress.folders_fingerprint,
            "total": progress.total,
            "page_size": progress.page_size,
            "cursor": progress.cursor,
            "swept_pages": sorted(progress.swept_pages),
            "pages": {str(page): list(rows) for page, rows in progress.pages.items()},
            "fetched_at": {str(page): value for page, value in progress.fetched_at.items()},
        }
        cache = json.dumps(document, sort_keys=True)
        store.set_meta(META_SHOKO_DISCOVERY_PROGRESS, cache)
        before = store.load_sonarr_inventory_cache()
    assert h.requests is not None
    h.requests.clear()
    h.ready()
    assert ("sonarr", "/api/v3/episode") in h.requests
    assert ("sonarr", "/api/v3/episodefile") in h.requests
    assert ("shoko", "/api/v3/File") in h.requests
    with open_state(h.path) as store:
        assert store.load_sonarr_inventory_cache() == before
        assert store.get_meta(META_SHOKO_DISCOVERY_PROGRESS) == cache


def test_views_are_local_and_never_schedule(h: Harness) -> None:
    with open_state(h.path) as store:
        before = _snapshot(store)
    for _ in range(5):
        assert h.controller.view(SESSION).status == "idle"
    assert h.requests == []
    assert not h.tick()
    with open_state(h.path) as store:
        assert _snapshot(store) == before


def test_known_empty_inventory_is_complete_with_nonempty_scope(h: Harness) -> None:
    h.world.sonarr.files.clear()
    h.world.sonarr.episodes.clear()
    h.world.shoko.rows.clear()
    evidence = h.collect()
    assert evidence.summary.selected_roots == 1
    assert evidence.summary.sonarr_files == evidence.summary.shoko_files == 0
    assert evidence.assessment.observation.discovery_total == 0


@pytest.mark.parametrize(
    "change", ["file_path", "series_path", "episode", "association", "unlinked_path"]
)
def test_strong_digest_binds_content_beyond_summary_counts(h: Harness, change: str) -> None:
    if change == "unlinked_path":
        h.world.sonarr.files[1].append(
            {"id": 99, "seriesId": 1, "path": f"{eh.SONARR_ROOT}/orphan.mkv", "size": 5}
        )
    original = h.collect()
    if change == "file_path":
        h.world.sonarr.files[1][0]["path"] = f"{eh.SONARR_ROOT}/renamed.mkv"
    elif change == "series_path":
        h.change_series_path = f"{eh.SONARR_ROOT}/renamed-series"
    elif change == "episode":
        h.world.sonarr.episodes[1][0]["tvdbId"] = 123456
    elif change == "association":
        h.refs = [
            {
                "SeriesID": {"AniDB": 4001},
                "EpisodeIDs": [{"ID": 123, "AniDB": 456, "Source": "User"}],
            }
        ]
    else:
        h.world.sonarr.files[1][-1]["path"] = f"{eh.SONARR_ROOT}/other-orphan.mkv"
    changed = h.collect()
    assert changed.assessment.observation == original.assessment.observation
    assert changed.assessment.content_sha256 != original.assessment.content_sha256


def test_changed_confirmation_requires_new_token_and_explicit_acceptance(h: Harness) -> None:
    token = h.ready()
    h.world.sonarr.episodes[1][0]["tvdbId"] = 123456
    h.controller.request_confirmation(token, SESSION)
    h.clock.advance(1)
    h.tick()
    view = h.controller.view(SESSION)
    assert view.status == "changed"
    assert view.token is not None
    assert view.token != token
    with open_state(h.path) as store:
        assert store.get_source_continuity_review().requires_rebaseline
    h.controller.request_confirmation(view.token, SESSION)
    h.clock.advance(1)
    h.tick()
    assert h.controller.view(SESSION).status == "confirmed"


@pytest.mark.parametrize(
    "refs",
    [
        [{}],
        [{"SeriesID": {"AniDB": 1}}],
        [{"SeriesID": {"AniDB": 1}, "EpisodeIDs": [{"AniDB": 2, "Source": "User"}]}],
        "unknown",
    ],
)
def test_unknown_association_identity_never_becomes_ready(h: Harness, refs: object) -> None:
    h.refs = refs
    h.controller.request_assessment(SESSION)
    h.tick()
    assert h.controller.view(SESSION).status != "ready"
    with open_state(h.path) as store:
        assert store.get_source_continuity_review().requires_rebaseline


def test_review_owns_runtime_operation_before_pause_and_skips_metadata_recovery(
    h: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    def forbidden(*args: Any, **kwargs: Any) -> None:
        pytest.fail("ordinary work ran in a source review operation")

    monkeypatch.setattr(h.runtime, "_worker_tick", forbidden)
    monkeypatch.setattr(h.runtime, "_recover_blocked_metadata", forbidden)
    h.controller.request_assessment(SESSION)
    h.runtime.worker_tick()
    assert h.controller.view(SESSION).status == "ready"


def test_pure_review_evaluation_preserves_tables_and_normal_pause(h: Harness) -> None:
    with open_state(h.path) as store:
        context = h.runtime.setup.current_context(
            store, deadline=h.clock.mono + 60, clock=h.clock.monotonic
        )
        assert context is not None
        before = _snapshot(store)
        manager = OnboardingManager(store)
        assert (
            manager.evaluate_source_review_context(context, iso=h.clock.iso()).status
            is OnboardingStatus.AUTHORIZED
        )
        assert _snapshot(store) == before
        assert (
            manager.evaluate_context(context, iso=h.clock.iso()).status is OnboardingStatus.PAUSED
        )
        assert store.get_service_pause() == (True, "user_paused")


def test_partial_sonarr_sweep_resumes_only_its_owned_children(h: Harness) -> None:
    h.world.sonarr.add_series(2, eh.SONARR_ROOT, 300_002)
    calls: list[int] = []

    def budget(request: httpx.Request) -> None:
        if request.url.path == "/api/v3/episode":
            series_id = int(request.url.params["seriesId"])
            calls.append(series_id)
            if series_id == 2:
                h.clock.advance(60)

    h.sonarr_hook = budget
    h.controller.request_assessment(SESSION)
    h.tick()
    assert h.controller.view(SESSION).status == "assessing"
    assert calls == [1, 2]
    h.sonarr_hook = (
        lambda request: calls.append(int(request.url.params["seriesId"]))
        if request.url.path == "/api/v3/episode"
        else None
    )
    h.tick()
    assert h.controller.view(SESSION).status == "ready"
    assert calls == [1, 2, 2]


@pytest.mark.parametrize("drift", ["none", "file", "folder", "total"])
def test_partial_shoko_sweep_resumes_owned_pages_and_original_interval(
    h: Harness, drift: str
) -> None:
    original = h.world.shoko.rows[801]
    for identifier in range(1000, 2001):
        h.world.shoko.add_row(
            eh.ShokoRow(
                identifier,
                original.size,
                [
                    eh.ShokoLocation(
                        identifier * 10, 1, f"file{identifier}.mkv", f"/anime/file{identifier}.mkv"
                    )
                ],
            )
        )

    def budget(request: httpx.Request) -> None:
        if (
            request.url.path == "/api/v3/File"
            and request.url.params.get("pageSize") == "1000"
            and request.url.params.get("page") == "2"
        ):
            h.clock.advance(60)

    h.shoko_hook = budget
    h.controller.request_assessment(SESSION)
    started = h.clock.iso()
    h.tick()
    assert h.controller.view(SESSION).status == "assessing"
    h.shoko_hook = None
    if drift == "file":
        location = h.world.shoko.rows[801].locations[0]
        h.world.shoko.rows[801].locations[0] = replace(location, absolute="/anime/changed.mkv")
    elif drift == "folder":
        h.world.shoko.folders[1] = ("/anime-changed", "None")
    elif drift == "total":
        h.world.shoko.rows.pop(801)
    h.tick()
    view = h.controller.view(SESSION)
    if drift != "none":
        assert view.status == "error"
        return
    assert view.status == "ready"
    assert view.started_iso == started
    assert view.summary is not None
    assert view.summary.shoko_files == 1003


@pytest.mark.parametrize(
    "change", ["unsafe", "folder", "settings", "url", "authorization", "mapping", "series"]
)
def test_drift_after_inventory_io_never_prepares_review(h: Harness, change: str) -> None:
    changed = False

    def drift(request: httpx.Request) -> None:
        nonlocal changed
        if (
            changed
            or request.url.path != "/api/v3/File"
            or request.url.params.get("pageSize") != "1000"
        ):
            return
        changed = True
        if change == "unsafe":
            cast(ServiceShokoStub, h.world.shoko).renamer_enabled = True
        elif change == "folder":
            h.world.shoko.folders[1] = ("/anime-changed", "None")
        elif change == "mapping":
            h.world.mapping_store().active_path.write_bytes(b"invalid mapping data")
        elif change == "series":
            h.change_series_path = f"{eh.SONARR_ROOT}/moved"
        else:
            with open_state(h.path) as store:
                if change == "authorization":
                    store.set_meta(META_SERVICE_AUTHORIZATION_REVISION, "f" * 32)
                else:
                    document = load_service_settings(store, h.runtime.settings)
                    if change == "url":
                        document["sonarr_url"] = "http://another:8989"
                    else:
                        document["sync"]["interval_seconds"] += 1
                    store.set_meta(META_SERVICE_SETTINGS, json.dumps(document))

    h.shoko_hook = drift
    h.controller.request_assessment(SESSION)
    h.tick()
    assert changed
    assert h.controller.view(SESSION).status == "error"
    with open_state(h.path) as store:
        assert store.get_source_continuity_review().requires_rebaseline
        assert store.get_source_rebaseline_review().prepared is None


def test_url_restart_needed_rejected_before_any_remote_io(h: Harness) -> None:
    with open_state(h.path) as store:
        document = load_service_settings(store, h.runtime.settings)
        document["sonarr_url"] = "http://another:8989"
        store.set_meta(META_SERVICE_SETTINGS, json.dumps(document))
    h.controller.request_assessment(SESSION)
    h.tick()
    assert h.controller.view(SESSION).status == "error"
    assert h.requests == []


def test_deadline_late_response_and_shutdown_never_ready(h: Harness) -> None:
    with open_state(h.path) as store, pytest.raises(TimeoutError):
        h.collector.capture_bindings(store, h.clock.mono)
    h.shoko_hook = (
        lambda request: h.clock.advance(60)
        if request.url.path == "/api/v3/File" and request.url.params.get("pageSize") == "1000"
        else None
    )
    h.controller.request_assessment(SESSION)
    h.tick()
    assert h.controller.view(SESSION).status != "ready"
    h.clock.advance(901)
    h.controller.request_assessment(SESSION)
    h.runtime.state.stopped = True
    h.tick()
    assert h.controller.view(SESSION).status == "error"


def test_same_association_count_changed_episode_or_series_identity_changes_digest(
    h: Harness,
) -> None:
    h.refs = [
        {
            "SeriesID": {"ID": 1, "AniDB": 4001},
            "EpisodeIDs": [{"ID": 123, "AniDB": 456, "Source": "User"}],
        }
    ]
    original = h.collect()
    h.refs = [
        {
            "SeriesID": {"ID": 1, "AniDB": 4001},
            "EpisodeIDs": [{"ID": 124, "AniDB": 456, "Source": "User"}],
        }
    ]
    episode_changed = h.collect()
    h.refs = [
        {
            "SeriesID": {"ID": 2, "AniDB": 4001},
            "EpisodeIDs": [{"ID": 124, "AniDB": 456, "Source": "User"}],
        }
    ]
    series_changed = h.collect()
    assert (
        original.assessment.observation
        == episode_changed.assessment.observation
        == series_changed.assessment.observation
    )
    assert (
        len(
            {
                original.assessment.content_sha256,
                episode_changed.assessment.content_sha256,
                series_changed.assessment.content_sha256,
            }
        )
        == 3
    )


def test_empty_series_requires_still_current_scope_consent(h: Harness) -> None:
    # The existing setup policy invalidates consent when its selected root no
    # longer has any eligible series. Recovery must preserve that independent gate.
    h.world.sonarr.series.clear()
    h.world.sonarr.files.clear()
    h.world.sonarr.episodes.clear()
    h.world.shoko.rows.clear()
    h.controller.request_assessment(SESSION)
    h.tick()
    assert h.controller.view(SESSION).status == "error"
    with open_state(h.path) as store:
        assert store.get_source_continuity_review().requires_rebaseline


def test_manifest_drift_between_partial_slices_rejects_review(h: Harness) -> None:
    h.world.sonarr.add_series(2, eh.SONARR_ROOT, 300_002)

    def budget(request: httpx.Request) -> None:
        if request.url.path == "/api/v3/episode" and request.url.params.get("seriesId") == "2":
            h.clock.advance(60)

    h.sonarr_hook = budget
    h.controller.request_assessment(SESSION)
    h.tick()
    assert h.controller.view(SESSION).status == "assessing"
    h.sonarr_hook = None
    h.world.sonarr.add_series(3, eh.SONARR_ROOT, 300_003)
    h.tick()
    assert h.controller.view(SESSION).status == "error"


def test_header_titles_and_poll_timestamps_do_not_change_strong_identity(h: Harness) -> None:
    original = h.collect()
    for ep in h.world.sonarr.episodes[1]:
        ep["title"] = "Changed display title"
    h.clock.advance(1)
    later = h.collect()
    assert original.assessment.content_sha256 == later.assessment.content_sha256
    assert original.assessment.completed_iso != later.assessment.completed_iso
