"""Persisted user-facing metadata observations, isolated from write authority."""

from __future__ import annotations

import json
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
import test_daemon as td
from test_metadata_operations import Server, operation, recover
from test_metadata_planning import Harness
from test_metadata_planning import harness as metadata_harness
from test_metadata_scheduling import Service
from test_metadata_scheduling import service as metadata_service
from test_state import LIMITS, T0

from shoko_linker import cli
from shoko_linker.bootstrap import BootstrapSettings
from shoko_linker.config import SecretRef
from shoko_linker.logging_setup import register_secret
from shoko_linker.metadata_identity import metadata_source_key_for_url
from shoko_linker.metadata_operations import metadata_source_key
from shoko_linker.metadata_status import (
    MAX_DOCUMENT_CHARS,
    MAX_EXAMPLES,
    STATUS_KEY,
    metadata_status_payload,
    metadata_status_view,
    observe_metadata_status,
)
from shoko_linker.reporting import cycle_report_payload
from shoko_linker.service_settings import load_service_settings
from shoko_linker.state import (
    META_SERVICE_SETTINGS,
    MetadataOperation,
    MetadataOperationState,
    open_state,
)

harness = metadata_harness
service = metadata_service


def test_delayed_ready_snapshot_cannot_overwrite_new_prepared_generation(
    harness: Harness, tmp_path: Path
) -> None:
    demand = replace(harness.reconcile().metadata_demands[500], fingerprint="b" * 64)
    server = Server()
    key = metadata_source_key(server.endpoint)
    with open_state(tmp_path / "generations.sqlite3", budget_limits=LIMITS) as store:
        first = operation(store, server.endpoint, "accepted", anime=td.ANIME_ID)
        first = store.record_metadata_operation(
            first,
            MetadataOperationState.READY,
            iso=T0,
            verified_required_targets=first.required_targets,
        )
        assert first.required_targets
        extra = (td.ANIME_ID, first.required_targets[0][1], 2)
        second = store.prepare_metadata_operation(
            key,
            td.ANIME_ID,
            demand_fingerprint="e" * 64,
            request_fingerprint="f" * 64,
            request_version=1,
            cycle_key="second",
            iso=T0,
            required_targets=(*first.required_targets, extra),
            missing_targets=(extra,),
            after_ready_claim=first.claim,
        )
        assert isinstance(second, MetadataOperation)
        current_demand = replace(demand, fingerprint="e" * 64)
        observe_metadata_status(
            store,
            key,
            td.ANIME_ID,
            "metadata_prepared",
            T0,
            demand=current_demand,
            operation_snapshot=second,
        )
        expected = store.get_meta(STATUS_KEY)
        assert metadata_status_view(store, key, enabled=True).examples[0].state == "prepared"
        observe_metadata_status(
            store,
            key,
            td.ANIME_ID,
            "recovery:ready",
            T0,
            operation_snapshot=first,
        )
        assert store.get_meta(STATUS_KEY) == expected
        assert metadata_status_view(store, key, enabled=True).examples[0].state == "prepared"
        assert store.get_metadata_operation(key, td.ANIME_ID) == second
    server.endpoint.client.close()


def test_unclaimed_context_loses_race_to_new_operation_without_full_decode(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = harness.deps.store
    key = metadata_source_key(harness.deps.shoko)
    demand = harness.reconcile().metadata_demands[500]
    original_save = store.save_metadata_status_if_current
    original_get = store.get_metadata_operation
    decoded: list[int] = []

    def tracked(source: str, anime: int) -> MetadataOperation | None:
        decoded.append(anime)
        return original_get(source, anime)

    with open_state(store.path, budget_limits=store.budget_limits) as other:

        def competing_claim(**kwargs: Any) -> bool:
            operation(other, harness.deps.shoko, "external", anime=td.ANIME_ID)
            return original_save(**kwargs)

        monkeypatch.setattr(store, "get_metadata_operation", tracked)
        monkeypatch.setattr(store, "save_metadata_status_if_current", competing_claim)
        observe_metadata_status(store, key, td.ANIME_ID, "metadata_waiting", T0, demand=demand)
    assert decoded == [td.ANIME_ID]
    assert store.get_meta(STATUS_KEY) is None


def test_document_cas_preserves_other_handles_newer_anime_observation(
    harness: Harness, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = harness.deps.store
    key = metadata_source_key(harness.deps.shoko)
    observe_metadata_status(store, key, 1, "metadata_waiting", T0)
    original_save = store.save_metadata_status_if_current
    with open_state(store.path, budget_limits=store.budget_limits) as other:

        def competing_observation(**kwargs: Any) -> bool:
            observe_metadata_status(other, key, 2, "metadata_queue_not_clear", T0)
            return original_save(**kwargs)

        monkeypatch.setattr(store, "save_metadata_status_if_current", competing_observation)
        observe_metadata_status(store, key, 1, "metadata_delivery_unknown", T0)
    examples = metadata_status_view(store, key, enabled=True).examples
    assert [(item.anime_id, item.state) for item in examples] == [
        (2, "queue_unknown"),
        (1, "waiting"),
    ]


@pytest.mark.parametrize("starts_after_switch", [False, True])
def test_old_source_cannot_displace_new_source_status(
    harness: Harness, monkeypatch: pytest.MonkeyPatch, starts_after_switch: bool
) -> None:
    store = harness.deps.store
    old = metadata_source_key(harness.deps.shoko)
    new_url = "http://new-shoko:8111/"
    new = metadata_source_key_for_url(new_url)
    store.set_meta(
        META_SERVICE_SETTINGS, json.dumps({"shoko_url": str(harness.deps.shoko.base_url)})
    )
    observe_metadata_status(store, old, 1, "metadata_waiting", T0)
    original_save = store.save_metadata_status_if_current
    with open_state(store.path, budget_limits=store.budget_limits) as other:

        def switch() -> None:
            other.set_meta(META_SERVICE_SETTINGS, json.dumps({"shoko_url": new_url}))
            observe_metadata_status(other, new, 2, "metadata_waiting", T0)

        def competing_switch(**kwargs: Any) -> bool:
            switch()
            return original_save(**kwargs)

        if starts_after_switch:
            switch()
        else:
            monkeypatch.setattr(store, "save_metadata_status_if_current", competing_switch)
        observe_metadata_status(store, old, 1, "metadata_delivery_unknown", T0)
    examples = metadata_status_view(store, new, enabled=True).examples
    assert len(examples) == 1
    assert examples[0].anime_id == 2
    assert examples[0].state == "waiting"


@pytest.mark.parametrize("configured", [None, 42, "http://bad\x00host/", {"url": "private"}])
def test_invalid_current_source_refuses_status_replacement(
    harness: Harness, configured: object
) -> None:
    store = harness.deps.store
    key = metadata_source_key(harness.deps.shoko)
    observe_metadata_status(store, key, 1, "metadata_waiting", T0)
    expected = store.get_meta(STATUS_KEY)
    store.set_meta(META_SERVICE_SETTINGS, json.dumps({"shoko_url": configured}))
    observe_metadata_status(store, key, 1, "metadata_delivery_unknown", T0)
    assert store.get_meta(STATUS_KEY) == expected


def test_real_scheduler_persists_filename_and_runtime_reopens_without_http(
    service: Service,
) -> None:
    report = service.cycle("metadata-status")
    assert report.metadata_requests_attempted == 1
    payload = cycle_report_payload(report)
    assert payload["metadata"]["requests_attempted"] == 1
    assert payload["total_link_requests_attempted"] == 0
    deps = service.deps
    bootstrap = BootstrapSettings(
        td.SONARR_BASE,
        SecretRef("SONARR_API_KEY"),
        td.SHOKO_BASE,
        SecretRef("SHOKO_API_KEY"),
        deps.store.path,
    )
    # Runtime requires the complete ordinary saved settings document.
    deps.store.clear_meta(META_SERVICE_SETTINGS)
    preferences = load_service_settings(deps.store, bootstrap)
    preferences["allow_missing_metadata_refresh"] = True
    deps.store.set_meta(META_SERVICE_SETTINGS, json.dumps(preferences))
    runtime = cli._NormalServiceRuntime(bootstrap, deps.sonarr, deps.shoko, cli.StopState())
    before = len(service.shoko.requests)
    status = runtime.status()
    assert len(service.shoko.requests) == before
    example = status.metadata.examples[0]
    assert example.filename.endswith(".mkv")
    assert example.sonarr_path.endswith(example.filename)
    assert example.shoko_path
    assert example.shoko_file_id == 500
    assert example.anime_id == td.ANIME_ID
    assert example.state == "accepted"
    assert example.next_read_only_check_iso is None
    assert status.linked_files == 0
    assert deps.store.get_service_canary() is None
    # A saved endpoint change must not display the old server's operation.
    preferences["shoko_url"] = "http://other-shoko:8111/"
    deps.store.set_meta(META_SERVICE_SETTINGS, json.dumps(preferences))
    assert runtime.status().metadata.examples == ()
    assert len(service.shoko.requests) == before


def test_disabled_scheduler_still_explains_the_actual_file(service: Service) -> None:
    service.deps.store.set_meta(
        META_SERVICE_SETTINGS, json.dumps({"allow_missing_metadata_refresh": False})
    )
    service.cycle("disabled")
    view = metadata_status_view(
        service.deps.store, metadata_source_key(service.deps.shoko), enabled=False
    )
    assert view.examples[0].state == "disabled"
    assert view.examples[0].filename.endswith(".mkv")
    assert view.examples[0].shoko_file_id == 500
    assert not service.shoko.refreshes
    service.shoko.missing_anime.clear()
    service.cycle("manual-metadata-arrived")
    assert (
        metadata_status_view(
            service.deps.store, metadata_source_key(service.deps.shoko), enabled=False
        )
        .examples[0]
        .state
        == "ready"
    )


def test_blocked_recovery_updates_retained_context_without_post(
    service: Service, monkeypatch: pytest.MonkeyPatch
) -> None:
    service.cycle("accepted")
    deps = service.deps
    bootstrap = BootstrapSettings(
        td.SONARR_BASE,
        SecretRef("SONARR_API_KEY"),
        td.SHOKO_BASE,
        SecretRef("SHOKO_API_KEY"),
        deps.store.path,
    )
    deps.store.clear_meta(META_SERVICE_SETTINGS)
    preferences = load_service_settings(deps.store, bootstrap)
    preferences["allow_missing_metadata_refresh"] = False
    deps.store.set_meta(META_SERVICE_SETTINGS, json.dumps(preferences))
    runtime = cli._NormalServiceRuntime(bootstrap, deps.sonarr, deps.shoko, cli.StopState())
    deps.store.set_service_pause(True, "user_paused", td.T1)
    service.shoko.missing_anime.clear()
    before = len(service.shoko.refreshes)
    monkeypatch.setattr(cli, "current_iso", lambda: td.T1)
    runtime._recover_blocked_metadata(time.monotonic() + 30, 1)
    status = runtime.status()
    assert not status.metadata.enabled
    assert status.metadata.examples[0].state == "ready"
    assert status.metadata.examples[0].filename.endswith(".mkv")
    assert len(service.shoko.refreshes) == before
    assert not status.automatic_linking


@pytest.mark.parametrize(
    ("reason", "expected"),
    [
        ("metadata_waiting_external_queue", "waiting_queue"),
        ("metadata_waiting_external_file_queue", "waiting_file_queue"),
        ("metadata_queue_not_clear", "queue_unknown"),
        ("metadata_delivery_unknown", "uncertain"),
        ("metadata_replan", "ready"),
        ("metadata_accepted_waiting", "accepted"),
        ("recovery:needs_attention", "needs_attention"),
        ("new_unrecognized_reason", "needs_attention"),
    ],
)
def test_exact_observations_never_claim_link_success(
    harness: Harness, reason: str, expected: str
) -> None:
    demand = harness.reconcile().metadata_demands[500]
    store = harness.deps.store
    key = metadata_source_key(harness.deps.shoko)
    observe_metadata_status(store, key, td.ANIME_ID, reason, T0, demand=demand)
    example = metadata_status_view(store, key, enabled=True).examples[0]
    assert example.state == expected
    assert example.shoko_file_id == 500
    assert store.get_service_canary() is None
    assert store.budget_view().completed_submissions == 0


def test_crash_after_request_preserves_only_exact_demand_context(
    harness: Harness, tmp_path: Path
) -> None:
    demand = replace(harness.reconcile().metadata_demands[500], fingerprint="b" * 64)
    server = Server()
    key = metadata_source_key(server.endpoint)
    path = tmp_path / "recovery.sqlite3"
    with open_state(path, budget_limits=LIMITS) as store:
        # Demand context is committed before transport can create a claim.
        observe_metadata_status(store, key, td.ANIME_ID, "metadata_waiting", T0, demand=demand)
        operation(store, server.endpoint, "accepted", anime=td.ANIME_ID)
    with open_state(path, budget_limits=LIMITS) as store:
        assert metadata_status_view(store, key, enabled=False).examples[0].state == "accepted"
        recover(server, store)
        example = metadata_status_view(store, key, enabled=False).examples[0]
        assert example.state == "ready"
        assert example.shoko_file_id == 500
        assert example.filename.endswith(".mkv")
        assert all(request.method == "GET" for request in server.requests)
        # Stale context cannot be attached to a replacement generation.
        store.connection.execute("UPDATE metadata_operations SET claim=?", ("d" * 32,))
        store.connection.commit()
        assert metadata_status_view(store, key, enabled=True).examples == ()
        assert metadata_status_view(store, "e" * 64, enabled=True).examples == ()
    server.endpoint.client.close()


def test_bounded_context_and_untrusted_legacy_data_are_sanitized(harness: Harness) -> None:
    demand = harness.reconcile().metadata_demands[500]
    store = harness.deps.store
    key = metadata_source_key(harness.deps.shoko)
    secret = "synthetic-status-secret-very-long"
    register_secret(secret)
    hostile = f"/anime/name-{secret}-\x00-https://private.invalid/payload?token=other.mkv"
    demand = replace(demand, sonarr_path=hostile, shoko_paths=(hostile,))
    for anime in range(1, MAX_EXAMPLES + 7):
        matching_demand = replace(
            demand,
            mapped_targets=tuple(
                replace(target, anidb_anime_id=anime) for target in demand.mapped_targets
            ),
        )
        observe_metadata_status(store, key, anime, "metadata_waiting", T0, demand=matching_demand)
    raw = store.get_meta(STATUS_KEY) or ""
    assert secret not in raw
    assert "private.invalid" not in raw
    assert "token=other" not in raw
    assert "\\u0000" not in raw
    assert len(metadata_status_view(store, key, enabled=True).examples) == MAX_EXAMPLES
    assert metadata_status_view(store, key, enabled=True).examples[0].anime_id == MAX_EXAMPLES + 6
    doc = json.loads(raw)
    doc["examples"][0]["filename"] = hostile
    doc["examples"][0]["sonarr_path"] = hostile
    store.set_meta(STATUS_KEY, json.dumps(doc))
    rendered = json.dumps(metadata_status_payload(metadata_status_view(store, key, enabled=True)))
    assert secret not in rendered
    assert "private.invalid" not in rendered
    assert "\\u0000" not in rendered
    observe_metadata_status(store, key, 99, "metadata_waiting", T0)
    assert secret not in (store.get_meta(STATUS_KEY) or "")


@pytest.mark.parametrize(
    "raw",
    ["{broken", "[]", "[" * 2000, "x" * (MAX_DOCUMENT_CHARS + 1)],
    ids=["broken", "list", "deep", "oversized"],
)
def test_malformed_local_status_is_bounded_and_ignored(tmp_path: Path, raw: str) -> None:
    with open_state(tmp_path / "state.sqlite3") as store:
        store.set_meta(STATUS_KEY, raw)
        assert metadata_status_view(store, "a" * 64, enabled=True).examples == ()
