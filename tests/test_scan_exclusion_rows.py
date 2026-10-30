"""Complete latest-scan rows, atomic replacement, bounded pages, and path display."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
import test_daemon as td
import test_scan_status as fx
import test_state as state_fx

from shoko_linker import state as state_module
from shoko_linker.daemon import CycleMode, run_cycle
from shoko_linker.file_display import FileDisplay, file_display, tracked_file_display
from shoko_linker.logging_setup import register_secret
from shoko_linker.paths import PathMapping
from shoko_linker.policies import RunPolicy
from shoko_linker.scan_status import last_scan_evidence, scan_context_identity, scan_exclusion_page
from shoko_linker.state import (
    META_LAST_DISCOVERY_EXCLUSIONS,
    META_SERVICE_DRAFT_ROOTS,
    SCHEMA_VERSION,
    ScanExclusionRow,
    StateError,
    StateStore,
    open_state,
)

ROOT = "/data/media/anime"
MAPPINGS = (PathMapping(ROOT, "/anime"),)


def _persist(store: StateStore, total: int = 137) -> None:
    fx._confirm(store)
    identity = scan_context_identity(store, td.SONARR_BASE, td.SHOKO_BASE)
    assert identity is not None
    document = fx._scan_document(identity, total)
    document["scan"].update(observed_files=total, matched_files=0, reported_unrecognized=total)
    if total == 0:
        document["examples"] = []
    rows = tuple(
        ScanExclusionRow(i, "size_mismatch", f"Recorded {i}.mkv", "", (f"/anime/Show/{i}.mkv",))
        for i in range(1, total + 1)
    )
    store.replace_latest_scan_exclusions(document, rows)


def test_all_137_rows_paginate_without_duplicates_and_survive_restart(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_state(path) as store:
        _persist(store)
    with open_state(path) as store:
        ids: list[int] = []
        for number, expected in ((1, 50), (2, 50), (3, 37)):
            result = scan_exclusion_page(store, td.SONARR_BASE, td.SHOKO_BASE, page=number)
            assert result is not None
            assert result.full_rows_available
            assert result.pagination.total == result.evidence.summary.excluded_files == 137
            assert len(result.rows) == expected
            ids.extend(item.row.shoko_file_id for item in result.rows)
            assert result.rows[0].display.library == ROOT
            assert result.rows[0].display.relative_path == "Show"
        assert ids == list(range(1, 138))
        last = scan_exclusion_page(store, td.SONARR_BASE, td.SHOKO_BASE, page=999)
        assert last is not None
        assert last.pagination.page == 3


def test_page_limit_precedes_hydration_and_empty_is_complete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with open_state(tmp_path / "state.sqlite3") as store:
        _persist(store)
        hydrated = 0
        original = state_module._scan_exclusion_row

        def count(row: Any) -> ScanExclusionRow:
            nonlocal hydrated
            hydrated += 1
            return original(row)

        monkeypatch.setattr(state_module, "_scan_exclusion_row", count)
        result = scan_exclusion_page(store, td.SONARR_BASE, td.SHOKO_BASE, page_size=100)
        assert result is not None
        assert len(result.rows) == hydrated == 100
        with pytest.raises(ValueError, match="page_size"):
            scan_exclusion_page(store, td.SONARR_BASE, td.SHOKO_BASE, page_size=101)
        assert hydrated == 100
        _persist(store, 0)
        empty = scan_exclusion_page(store, td.SONARR_BASE, td.SHOKO_BASE)
        assert empty is not None
        assert empty.full_rows_available
        assert empty.pagination.total == 0
        assert empty.rows == ()


def test_failed_full_replace_rolls_back_rows_generation_and_summary(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.sqlite3") as store:
        _persist(store, 25)
        before = store.get_meta(META_LAST_DISCOVERY_EXCLUSIONS)
        old_rows = store.connection.execute("SELECT * FROM latest_scan_exclusions").fetchall()
        store.connection.execute(
            "CREATE TRIGGER reject_row BEFORE INSERT ON latest_scan_exclusions"
            " WHEN NEW.shoko_file_id=3 BEGIN SELECT RAISE(ABORT, 'test failure'); END"
        )
        with pytest.raises(StateError):
            _persist(store, 120)
        assert store.get_meta(META_LAST_DISCOVERY_EXCLUSIONS) == before
        assert (
            store.connection.execute("SELECT * FROM latest_scan_exclusions").fetchall() == old_rows
        )
        result = scan_exclusion_page(store, td.SONARR_BASE, td.SHOKO_BASE)
        assert result is not None
        assert result.pagination.total == 25


def test_duplicate_rows_refused_and_generation_drift_not_mixed(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.sqlite3") as store:
        _persist(store, 2)
        old = last_scan_evidence(store, td.SONARR_BASE, td.SHOKO_BASE)
        assert old is not None
        assert old.rows_generation is not None
        document = json.loads(store.get_meta(META_LAST_DISCOVERY_EXCLUSIONS) or "null")
        row = ScanExclusionRow(1, "size_mismatch", "Repeated")
        with pytest.raises(StateError, match="duplicate"):
            store.replace_latest_scan_exclusions(document, (row, row))
        _persist(store, 3)
        assert store.latest_scan_exclusion_page(old.rows_generation, 2) is None


def test_legacy_examples_are_explicitly_incomplete_and_invalid_scope_has_no_page(
    tmp_path: Path,
) -> None:
    with open_state(tmp_path / "state.sqlite3") as store:
        fx._confirm(store)
        identity = scan_context_identity(store, td.SONARR_BASE, td.SHOKO_BASE)
        assert identity is not None
        document = fx._scan_document(identity, 137)
        document["scan"].update(observed_files=137, matched_files=0, reported_unrecognized=137)
        store.set_meta(META_LAST_DISCOVERY_EXCLUSIONS, json.dumps(document))
        legacy = scan_exclusion_page(store, td.SONARR_BASE, td.SHOKO_BASE)
        assert legacy is not None
        assert not legacy.full_rows_available
        assert legacy.pagination.total == 1
        assert legacy.evidence.summary.excluded_files == 137
        assert "1 of 137" in legacy.message
        assert "next successful scan" in legacy.message
        assert legacy.rows[0].display == FileDisplay()
        assert scan_exclusion_page(store, td.SONARR_BASE, "http://other:8111") is None
        store.set_meta(META_SERVICE_DRAFT_ROOTS, json.dumps(["/other"]))
        assert scan_exclusion_page(store, td.SONARR_BASE, td.SHOKO_BASE) is None


def test_v9_upgrade_adds_only_latest_rows_table_preserving_file_and_send_history(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    with open_state(path, budget_limits=state_fx.LIMITS) as store:
        state_fx.walk_to_planned(store)
        state_fx.prepare(store)
        tables = ("files", "sends", "metadata_operations", "metadata_operation_history")
        before = {
            table: store.connection.execute(
                f"SELECT * FROM {table}"  # noqa: S608 - fixed table names defined in this test
            ).fetchall()
            for table in tables
        }
        store.connection.execute("DROP TABLE latest_scan_exclusions")
        store.connection.execute("DELETE FROM schema_version WHERE version>=10")
        store.connection.execute("PRAGMA user_version=9")
    with open_state(path) as store:
        assert store.schema_version() == SCHEMA_VERSION
        for table in tables:
            assert (
                store.connection.execute(
                    f"SELECT * FROM {table}"  # noqa: S608 - fixed table names defined in this test
                ).fetchall()
                == before[table]
            )
        assert store.connection.execute(
            "SELECT COUNT(*) FROM latest_scan_exclusions"
        ).fetchone() == (0,)


@pytest.mark.parametrize(
    ("source", "locations", "roots", "mappings", "expected"),
    [
        (
            "",
            ("/anime/Show/A [x] & ü.mkv",),
            (ROOT,),
            MAPPINGS,
            FileDisplay("A [x] & ü.mkv", "Show", ROOT),
        ),
        (
            "/data/media/anime/Sub/Show/E.mkv",
            (),
            (ROOT, ROOT + "/Sub"),
            MAPPINGS,
            FileDisplay("E.mkv", "Show", ROOT + "/Sub"),
        ),
        ("", ("/anime-old/Show/E.mkv",), (ROOT,), MAPPINGS, FileDisplay("E.mkv")),
        ("", ("/anime/Show/A.mkv", "/anime/Show/B.mkv"), (ROOT,), MAPPINGS, FileDisplay()),
        ("", ("/manual/Show/E.mkv",), (ROOT,), MAPPINGS, FileDisplay("E.mkv")),
        (
            "",
            ("/anime/Show/E.mkv",),
            (ROOT,),
            (*MAPPINGS, PathMapping("/other", "/anime/Show")),
            FileDisplay("E.mkv"),
        ),
    ],
)
def test_structured_path_projection_is_component_aware_and_unambiguous(
    source: str,
    locations: tuple[str, ...],
    roots: tuple[str, ...],
    mappings: tuple[PathMapping, ...],
    expected: FileDisplay,
) -> None:
    assert file_display(source, locations, roots, mappings) == expected


def test_redaction_truncation_or_incomplete_locations_never_prove_library() -> None:
    secret = "scan-path-secret-sentinel"
    assert register_secret(secret)
    assert file_display("", (f"/anime/{secret}/E.mkv",), (ROOT,), MAPPINGS) == FileDisplay()
    assert file_display("", ("/anime/" + "x" * 9000,), (ROOT,), MAPPINGS) == FileDisplay()
    incomplete = file_display("", ("/anime/Show/E.mkv",), (ROOT,), MAPPINGS, paths_complete=False)
    assert incomplete.filename == "E.mkv"
    assert incomplete.library is None


def test_daemon_retains_every_structured_exclusion_and_partial_scan_keeps_them(
    tmp_path: Path,
) -> None:
    files = [td.SpecFile(series_id=1, file_id=10 + i, shoko_id=500 + i) for i in range(125)]

    class Inaccessible(td.ShokoStub):
        def _location(self, file: td.SpecFile) -> dict[str, Any]:
            location = super()._location(file)
            location["IsAccessible"] = False
            return location

    td.activate_mapping(tmp_path, td.mapping_xml())
    shoko = Inaccessible(files)
    with open_state(tmp_path / "state.sqlite3") as store:
        fx._confirm(store)
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
                cycle_key="all-exclusions",
            )
            evidence = last_scan_evidence(store, td.SONARR_BASE, td.SHOKO_BASE)
            assert evidence is not None
            assert evidence.summary.observed_files == evidence.summary.excluded_files == 125
            assert evidence.summary.matched_files == 0
            assert len(evidence.examples) == 20
            result = scan_exclusion_page(store, td.SONARR_BASE, td.SHOKO_BASE, page=3)
            assert result is not None
            assert result.pagination.total == 125
            assert [view.row.shoko_file_id for view in result.rows] == list(range(600, 625))
            assert result.rows[0].row.shoko_paths == ("/anime/series1/600.mkv",)
            assert result.rows[0].display == FileDisplay("600.mkv", "series1", ROOT)
            before = store.get_meta(META_LAST_DISCOVERY_EXCLUSIONS)
            shoko.target_read_failures["/api/v3/File"] = (1, 503)
            run_cycle(
                deps,
                now_iso=td.T1,
                mode=CycleMode.DRY_RUN,
                policy=RunPolicy.SERVICE,
                cycle_key="incomplete-exclusions",
            )
            assert store.get_meta(META_LAST_DISCOVERY_EXCLUSIONS) == before
            assert scan_exclusion_page(store, td.SONARR_BASE, td.SHOKO_BASE, page=3) == result
            assert shoko.posts == []
        finally:
            for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
                endpoint.client.close()


def test_tracked_projection_retains_sanitization_proof_loss_marker(tmp_path: Path) -> None:
    secret = "tracked-library-path-secret"
    assert register_secret(secret)
    with open_state(tmp_path / "state.sqlite3") as store:
        store.record_file(
            1,
            sonarr_series_id=1,
            sonarr_file_id=1,
            sonarr_path=f"{ROOT}/{secret}/Episode.mkv",
            sonarr_size=100,
            cohort=state_module.Cohort.NEW_ARRIVAL,
            iso=td.T0,
        )
        page = store.service_file_page("waiting")
        assert len(page.rows) == 1
        example = page.rows[0]
        assert secret not in example.sonarr_path
        assert not example.display_paths_complete
        assert tracked_file_display(example, (ROOT,), MAPPINGS).library is None


@pytest.mark.parametrize("new_total", [0, 3])
def test_replaced_empty_generation_cannot_claim_current_empty_rows(
    tmp_path: Path,
    new_total: int,
) -> None:
    with open_state(tmp_path / "state.sqlite3") as store:
        _persist(store, 0)
        old = last_scan_evidence(store, td.SONARR_BASE, td.SHOKO_BASE)
        assert old is not None
        assert old.rows_generation is not None
        current_empty = store.latest_scan_exclusion_page(old.rows_generation, 0)
        assert current_empty is not None
        assert current_empty.pagination.total == 0
        _persist(store, new_total)
        assert store.latest_scan_exclusion_page(old.rows_generation, 0) is None
        current = scan_exclusion_page(store, td.SONARR_BASE, td.SHOKO_BASE)
        assert current is not None
        assert current.pagination.total == new_total


def test_empty_summary_replaced_during_status_read_does_not_become_complete_empty_page(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "state.sqlite3"
    with open_state(path) as store:
        _persist(store, 0)
        original = StateStore.get_meta
        swapped = False

        def replace_after_summary_read(self: StateStore, key: str) -> str | None:
            nonlocal swapped
            raw = original(self, key)
            if key == META_LAST_DISCOVERY_EXCLUSIONS and not swapped:
                swapped = True
                with open_state(path) as worker:
                    _persist(worker, 3)
            return raw

        monkeypatch.setattr(StateStore, "get_meta", replace_after_summary_read)
        assert scan_exclusion_page(store, td.SONARR_BASE, td.SHOKO_BASE) is None
        current = scan_exclusion_page(store, td.SONARR_BASE, td.SHOKO_BASE)
        assert current is not None
        assert current.pagination.total == 3


@pytest.mark.parametrize(
    "raw",
    [None, "{", "[" * 2000, "x" * 65_537, '{"version":1}'],
    ids=["missing", "bad-json", "recursive", "oversized", "legacy"],
)
def test_empty_generation_refuses_missing_corrupt_or_unbound_summary(
    tmp_path: Path,
    raw: str | None,
) -> None:
    with open_state(tmp_path / "state.sqlite3") as store:
        _persist(store, 0)
        evidence = last_scan_evidence(store, td.SONARR_BASE, td.SHOKO_BASE)
        assert evidence is not None
        assert evidence.rows_generation is not None
        if raw is None:
            store.clear_meta(META_LAST_DISCOVERY_EXCLUSIONS)
        else:
            store.set_meta(META_LAST_DISCOVERY_EXCLUSIONS, raw)
        assert store.latest_scan_exclusion_page(evidence.rows_generation, 0) is None
