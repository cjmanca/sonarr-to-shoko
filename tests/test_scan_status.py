"""Historical scan totals remain distinct from tracked work and Shoko's live queue."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import httpx
import pytest
import test_daemon as td
import test_onboarding as onboarding_fx

from shoko_linker import cli
from shoko_linker import daemon as daemon_module
from shoko_linker.bootstrap import BootstrapSettings
from shoko_linker.config import SecretRef
from shoko_linker.daemon import CycleMode, run_cycle
from shoko_linker.dashboard import DashboardApplication
from shoko_linker.http import ApiEndpoint
from shoko_linker.onboarding import OnboardingRecord
from shoko_linker.policies import RunPolicy
from shoko_linker.scan_status import last_scan_evidence, last_scan_summary, scan_context_identity
from shoko_linker.service_settings import load_service_settings
from shoko_linker.shoko import ShokoDiscoveryProgress, collect_shoko_discovery, match_shoko_files
from shoko_linker.state import (
    META_LAST_DISCOVERY_EXCLUSIONS,
    META_SERVICE_DRAFT_ROOTS,
    META_SERVICE_SETTINGS,
    StateStore,
    open_state,
)


def _confirm(store: StateStore) -> None:
    binding = onboarding_fx.make_binding()
    record = OnboardingRecord(binding, binding.digest, binding.unverifiable_facts, True, td.T0)
    store.save_onboarding_json(record.to_json(), td.T0)


def test_complete_scan_partitions_global_files_and_preserves_prior_on_partial(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    files = [td.SpecFile(series_id=1, file_id=10 + i, shoko_id=500 + i) for i in range(58)]

    class InaccessibleFiles(td.ShokoStub):
        def _location(self, file: td.SpecFile) -> dict[str, Any]:
            row = super()._location(file)
            row["IsAccessible"] = file.shoko_id < 540
            return row

    td.activate_mapping(tmp_path, td.mapping_xml())
    shoko = InaccessibleFiles(files)
    with open_state(tmp_path / "state.sqlite3") as store:
        _confirm(store)
        deps = td.build_deps(
            tmp_path,
            store=store,
            sonarr_files=files,
            shoko_files=files,
            xml=td.mapping_xml(),
            monotonic=td.Clock([0.0]),
            advance=lambda _: None,
            shoko=shoko,
        )
        deps = replace(deps, service_mode=True, service_authorized=lambda: True)
        try:
            run_cycle(
                deps,
                now_iso=td.T0,
                mode=CycleMode.DRY_RUN,
                policy=RunPolicy.SERVICE,
                cycle_key="scan",
            )
            identity = scan_context_identity(store, td.SONARR_BASE, td.SHOKO_BASE)
            scan = last_scan_summary(store, identity)
            assert scan is not None
            assert scan.reported_unrecognized == scan.observed_files == 58
            assert scan.matched_files == 40
            assert scan.excluded_files == 18
            assert scan.outside_scope_files == 0
            assert scan.observed_from == scan.observed_until == td.T0
            assert shoko.posts == []
            before = store.get_meta(META_LAST_DISCOVERY_EXCLUSIONS)
            # Source match counts do not become durable queue/verified counts.
            assert store.service_file_status().counts.verified_files == 0
            shoko.target_read_failures["/api/v3/File"] = (1, 503)
            run_cycle(
                deps,
                now_iso=td.T1,
                mode=CycleMode.DRY_RUN,
                policy=RunPolicy.SERVICE,
                cycle_key="scan",
            )
            assert shoko.target_read_counts["/api/v3/File"] == 1
            assert store.get_meta(META_LAST_DISCOVERY_EXCLUSIONS) == before
            assert last_scan_summary(store, identity) == scan
            shoko.target_read_failures.clear()

            def fail_reconciliation(*args: Any, **kwargs: Any) -> None:
                raise RuntimeError("interrupted after matching")

            monkeypatch.setattr(
                daemon_module, "_record_service_verified_mapping_drifts", fail_reconciliation
            )
            with pytest.raises(RuntimeError, match="interrupted after matching"):
                run_cycle(
                    deps,
                    now_iso=td.T2,
                    mode=CycleMode.DRY_RUN,
                    policy=RunPolicy.SERVICE,
                    cycle_key="failed-scan",
                )
            assert store.get_meta(META_LAST_DISCOVERY_EXCLUSIONS) == before
        finally:
            for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
                endpoint.client.close()


def test_scan_scope_changes_are_unavailable_and_read_only(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.sqlite3") as store:
        _confirm(store)
        original = scan_context_identity(store, td.SONARR_BASE, td.SHOKO_BASE)
        assert original is not None
        assert scan_context_identity(store, td.SONARR_BASE, "http://different:8111") is None
        store.set_meta(META_SERVICE_DRAFT_ROOTS, json.dumps(["/other"]))
        assert scan_context_identity(store, td.SONARR_BASE, td.SHOKO_BASE) is None
        store.clear_meta(META_SERVICE_DRAFT_ROOTS)
        settings = td.make_settings(tmp_path)
        assert (
            scan_context_identity(store, td.SONARR_BASE, td.SHOKO_BASE, settings=settings)
            == original
        )
        changed = replace(settings, sonarr=replace(settings.sonarr, allowed_roots=("/other",)))
        assert scan_context_identity(store, td.SONARR_BASE, td.SHOKO_BASE, settings=changed) is None
        before = store.get_onboarding_json()
        assert last_scan_summary(store, original) is None
        assert store.get_onboarding_json() == before


@pytest.mark.parametrize(
    "mutation",
    [
        {"matched_files": True},
        {"excluded_files": -1},
        {"observed_files": 100},
        {"outside_scope_files": 19},
        {"observed_from": "<script>bad</script>"},
        {"observed_until": "2025-01-01T00:00:00+00:00"},
        {"observed_until": "2026-01-01T00:00:00"},
        {"version": True},
        {"context_identity": "other"},
        {"reported_unrecognized": True},
        {"observed_from": "0001-01-01T00:00:00+01:00"},
        {"observed_until": "9999-12-31T23:59:59-01:00"},
    ],
)
def test_corrupt_scan_evidence_never_becomes_current_counts(
    tmp_path: Path, mutation: dict[str, Any]
) -> None:
    with open_state(tmp_path / "state.sqlite3") as store:
        scan = {
            "version": 1,
            "context_identity": "a" * 64,
            "observed_from": td.T0,
            "observed_until": td.T1,
            "reported_unrecognized": 58,
            "observed_files": 58,
            "matched_files": 40,
            "excluded_files": 18,
            "outside_scope_files": 0,
        }
        payload = {"scan": scan, "total": 18, "scope_excluded": 0}
        store.set_meta(META_LAST_DISCOVERY_EXCLUSIONS, json.dumps(payload))
        assert last_scan_summary(store, "a" * 64) is not None
        scan.update(mutation)
        raw = json.dumps(payload)
        store.set_meta(META_LAST_DISCOVERY_EXCLUSIONS, raw)
        assert last_scan_summary(store, "a" * 64) is None
        assert store.get_meta(META_LAST_DISCOVERY_EXCLUSIONS) == raw


def test_resumed_scan_records_observation_range_and_source_count_separately(
    tmp_path: Path,
) -> None:
    td.activate_mapping(tmp_path, td.mapping_xml())
    with open_state(tmp_path / "state.sqlite3") as store:
        _confirm(store)
        deps = td.build_deps(
            tmp_path,
            store=store,
            sonarr_files=[],
            shoko_files=[],
            xml=td.mapping_xml(),
            monotonic=td.Clock([0.0]),
            advance=lambda _: None,
        )
        deps = replace(deps, now=lambda: td.T2)
        try:
            discovery = collect_shoko_discovery(deps.shoko)
            progress = ShokoDiscoveryProgress(
                "source", "folders", 0, 1000, 1, frozenset(), {1: ()}, {1: td.T0}
            )
            discovery = replace(discovery, progress=progress, total_reported=1)
            result = match_shoko_files(discovery, (), True, deps.settings)
            identity = scan_context_identity(store, td.SONARR_BASE, td.SHOKO_BASE)
            daemon_module._persist_reconciliation_summary(
                deps,
                result,
                discovery,
                td.T1,
                identity,
                {"version": 1, "total": 0, "counts": {}, "scope_excluded": 0, "examples": []},
            )
            scan = last_scan_summary(store, identity)
            assert scan is not None
            assert scan.observed_from == td.T0
            assert scan.observed_until == td.T2
            assert scan.observed_files == 0
            # A differing server report remains separate instead of inventing a partition.
            assert scan.reported_unrecognized == 1
        finally:
            for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
                endpoint.client.close()


@pytest.mark.parametrize(
    "raw",
    ['{"version":1}', "{", "[" * 2000, "x" * 65_537],
    ids=["legacy", "invalid-json", "recursive", "oversized"],
)
def test_legacy_invalid_or_oversized_summary_is_unavailable(tmp_path: Path, raw: str) -> None:
    with open_state(tmp_path / "state.sqlite3") as store:
        store.set_meta(META_LAST_DISCOVERY_EXCLUSIONS, raw)
        assert last_scan_summary(store, "a" * 64) is None
        assert store.get_meta(META_LAST_DISCOVERY_EXCLUSIONS) == raw


def _scan_document(identity: str, excluded: int, name: str = "old-file.mkv") -> dict[str, Any]:
    return {
        "version": 1,
        "total": excluded,
        "scope_excluded": 0,
        "counts": {"size_mismatch": excluded},
        "examples": [{"shoko_file_id": 501, "reason": "size_mismatch", "detail": name}],
        "scan": {
            "version": 1,
            "context_identity": identity,
            "observed_from": td.T0,
            "observed_until": td.T1,
            "reported_unrecognized": 58,
            "observed_files": 58,
            "matched_files": 58 - excluded,
            "excluded_files": excluded,
            "outside_scope_files": 0,
        },
    }


def _runtime(tmp_path: Path) -> tuple[cli._NormalServiceRuntime, ApiEndpoint]:
    def no_remote(request: httpx.Request) -> httpx.Response:
        pytest.fail(f"Status attempted remote I/O: {request.method} {request.url.path}")

    endpoint = ApiEndpoint(httpx.Client(transport=httpx.MockTransport(no_remote)), lambda: {})
    bootstrap = BootstrapSettings(
        td.SONARR_BASE,
        SecretRef("SONARR_API_KEY"),
        td.SHOKO_BASE,
        SecretRef("SHOKO_API_KEY"),
        tmp_path / "state.sqlite3",
    )
    return cli._NormalServiceRuntime(bootstrap, endpoint, endpoint, cli.StopState()), endpoint


def test_runtime_uses_one_scan_document_when_worker_replaces_it_mid_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with open_state(tmp_path / "state.sqlite3") as store:
        _confirm(store)
        identity = scan_context_identity(store, td.SONARR_BASE, td.SHOKO_BASE)
        assert identity is not None
        store.set_meta(META_LAST_DISCOVERY_EXCLUSIONS, json.dumps(_scan_document(identity, 18)))
    runtime, endpoint = _runtime(tmp_path)
    original = StateStore.get_meta
    reads = 0

    def replace_after_read(self: StateStore, key: str) -> str | None:
        nonlocal reads
        raw = original(self, key)
        if key == META_LAST_DISCOVERY_EXCLUSIONS:
            reads += 1
            if reads == 1:
                with open_state(tmp_path / "state.sqlite3") as worker:
                    worker.set_meta(
                        META_LAST_DISCOVERY_EXCLUSIONS,
                        json.dumps(_scan_document(identity, 20, "new-file.mkv")),
                    )
        return raw

    monkeypatch.setattr(StateStore, "get_meta", replace_after_read)
    try:
        status = runtime.status()
        assert reads == 1
        assert status.last_scan is not None
        assert status.last_scan.excluded_files == status.excluded_files == 18
        assert status.exclusion_counts == (("size_mismatch", 18),)
        assert any("old-file.mkv" in detail for detail in status.skipped_file_details)
        assert not any("new-file.mkv" in detail for detail in status.skipped_file_details)
        following = runtime.status()
        assert reads == 2
        assert following.last_scan is not None
        assert following.last_scan.excluded_files == following.excluded_files == 20
        assert any("new-file.mkv" in detail for detail in following.skipped_file_details)
    finally:
        endpoint.client.close()


@pytest.mark.parametrize("invalid", ["endpoint", "scope", "draft", "legacy", "missing"])
def test_runtime_suppresses_both_scan_totals_and_examples_when_unavailable(
    tmp_path: Path,
    invalid: str,
) -> None:
    runtime, endpoint = _runtime(tmp_path)
    with open_state(tmp_path / "state.sqlite3") as store:
        _confirm(store)
        identity = scan_context_identity(store, td.SONARR_BASE, td.SHOKO_BASE)
        assert identity is not None
        document = _scan_document(identity, 18, "obsolete-file.mkv")
        if invalid == "endpoint":
            preferences = load_service_settings(store, runtime.settings)
            preferences["shoko_url"] = "http://changed:8111"
            store.set_meta(META_SERVICE_SETTINGS, json.dumps(preferences))
        elif invalid == "scope":
            document["scan"]["context_identity"] = "0" * 64
        elif invalid == "draft":
            store.set_meta(META_SERVICE_DRAFT_ROOTS, json.dumps(["/changed"]))
        elif invalid == "legacy":
            del document["scan"]
        if invalid != "missing":
            store.set_meta(META_LAST_DISCOVERY_EXCLUSIONS, json.dumps(document))
    try:
        status = runtime.status()
        assert status.last_scan is None
        assert status.excluded_files is None
        assert status.scope_excluded_files is None
        assert status.exclusion_counts == ()
        assert status.skipped_file_details == ()
        page = DashboardApplication._status_page(status, "csrf", paused=True)
        assert "unavailable until a completed scan" in page
        exclusion_page = DashboardApplication._status_page(
            status,
            "csrf",
            paused=True,
            route="/issues",
            view="exclusions",
            table=runtime.table("exclusions", 1, 50),
        )
        exclusions = exclusion_page.split('data-live="table"', 1)[1].split("</section>", 1)[0]
        assert "File data is unavailable" in exclusions
        assert "obsolete-file.mkv" not in page
        assert "0 files excluded" not in exclusions
        assert "Showing 0 of 0 exclusions" not in exclusions
    finally:
        endpoint.client.close()


def test_scan_scope_change_during_document_read_refuses_stale_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with open_state(tmp_path / "state.sqlite3") as store:
        _confirm(store)
        identity = scan_context_identity(store, td.SONARR_BASE, td.SHOKO_BASE)
        assert identity is not None
        store.set_meta(META_LAST_DISCOVERY_EXCLUSIONS, json.dumps(_scan_document(identity, 18)))
        original = StateStore.get_meta

        def change_scope(self: StateStore, key: str) -> str | None:
            value = original(self, key)
            if key == META_LAST_DISCOVERY_EXCLUSIONS:
                self.set_meta(META_SERVICE_DRAFT_ROOTS, json.dumps(["/changed"]))
            return value

        monkeypatch.setattr(StateStore, "get_meta", change_scope)
        assert last_scan_evidence(store, td.SONARR_BASE, td.SHOKO_BASE) is None


@pytest.mark.parametrize(
    "corruption",
    [
        "boolean-count",
        "wrong-count-sum",
        "unhashable-reason",
        "duplicate-example",
        "nontext-detail",
        "boolean-total",
        "excess-examples",
    ],
)
def test_complete_projection_refuses_inconsistent_exclusion_payload(
    tmp_path: Path,
    corruption: str,
) -> None:
    with open_state(tmp_path / "state.sqlite3") as store:
        _confirm(store)
        identity = scan_context_identity(store, td.SONARR_BASE, td.SHOKO_BASE)
        assert identity is not None
        document = _scan_document(identity, 1)
        if corruption == "boolean-count":
            document["counts"]["size_mismatch"] = True
        elif corruption == "wrong-count-sum":
            document["counts"]["size_mismatch"] = 2
        elif corruption == "unhashable-reason":
            document["examples"][0]["reason"] = []
        elif corruption == "duplicate-example":
            document = _scan_document(identity, 18)
            document["examples"] *= 2
        elif corruption == "nontext-detail":
            document["examples"][0]["detail"] = []
        elif corruption == "boolean-total":
            document["total"] = True
        else:
            document["examples"].append(
                {
                    "shoko_file_id": 502,
                    "reason": "size_mismatch",
                    "detail": "extra.mkv",
                }
            )
        raw = json.dumps(document)
        store.set_meta(META_LAST_DISCOVERY_EXCLUSIONS, raw)
        assert last_scan_evidence(store, td.SONARR_BASE, td.SHOKO_BASE) is None
        assert store.get_meta(META_LAST_DISCOVERY_EXCLUSIONS) == raw
