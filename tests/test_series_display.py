"""Display context stays local, bounded and independent of linking authority."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from test_metadata_executor import Executor
from test_metadata_executor import executor as _executor
from test_metadata_state import E1, E2, SOURCE, finish_episode
from test_service_runtime import _confirmed_activity_runtime
from test_state import LIMITS, T0, T2, WIDE_LIMITS, prepare, walk_to_planned

from shoko_linker import cli
from shoko_linker import state as state_module
from shoko_linker.action_history import action_history_page, action_history_view
from shoko_linker.daemon import _metadata_scope_fingerprint, _remember_series_display
from shoko_linker.file_display import display_anidb_ids, display_document
from shoko_linker.linking import metadata_demand_fingerprint
from shoko_linker.logging_setup import register_secret
from shoko_linker.metadata_identity import metadata_source_key_for_url
from shoko_linker.metadata_status import metadata_status_view
from shoko_linker.onboarding import OnboardingManager
from shoko_linker.paths import PathMapping
from shoko_linker.policies import RunPolicy
from shoko_linker.scan_status import scan_context_identity
from shoko_linker.service import settings_for_scope
from shoko_linker.service_settings import load_service_settings
from shoko_linker.state import (
    META_SERVICE_DRAFT_ROOTS,
    META_SERVICE_SETTINGS,
    Cohort,
    FileSeed,
    FileSeriesDisplay,
    MetadataEpisodeIdentity,
    MetadataOperation,
    ScanExclusionRow,
    WriteDisabledError,
    open_state,
)

executor = _executor


def test_worker_series_display_excludes_identity_drifts_and_unknown_titles(
    executor: Executor,
) -> None:
    store = executor.deps.store
    record = store.get_file(500)
    assert record is not None
    store.connection.execute("DELETE FROM meta WHERE key='file_series_display:v1:500'")
    inventory: Any = SimpleNamespace(
        eligible_series=[
            SimpleNamespace(
                series=SimpleNamespace(id=record.sonarr_series_id, title="Verified inventory label")
            )
        ]
    )
    match: Any = SimpleNamespace(
        shoko_file_id=500,
        sonarr_file=SimpleNamespace(
            series_id=record.sonarr_series_id,
            episode_file_id=record.sonarr_file_id,
            path=record.sonarr_path,
        ),
    )
    _remember_series_display(executor.deps, inventory, [match], {500: object()})
    assert store.get_meta("file_series_display:v1:500") is None
    inventory.eligible_series[0].series.title = None
    _remember_series_display(executor.deps, inventory, [match], {})
    assert store.get_meta("file_series_display:v1:500") is None
    inventory.eligible_series[0].series.title = "Verified inventory label"
    _remember_series_display(executor.deps, inventory, [match], {})
    assert store.file_series_name(500) == "Verified inventory label"


def test_bulk_series_display_persists_whole_inventory_in_one_transaction(tmp_path: Path) -> None:
    count = 7000
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        rows = [
            FileSeriesDisplay(index, 1, index, f"/anime/show/{index}.mkv", "Managed Series")
            for index in range(1, count + 1)
        ]
        store.record_files(
            [
                FileSeed(
                    row.shoko_file_id, 1, row.episode_file_id, row.path, 10, Cohort.BACKLOG, T0
                )
                for row in rows
            ]
        )
        statements: list[str] = []
        store.connection.set_trace_callback(statements.append)
        before = store.connection.total_changes
        store.remember_file_series_names_bulk(rows, source_key=SOURCE, scope_fingerprint="e" * 64)
        store.connection.set_trace_callback(None)
        assert store.connection.total_changes - before == count
        assert sum(statement.startswith("BEGIN") for statement in statements) == 1
        assert statements.count("COMMIT") == 1
        for row in rows:
            assert store.file_series_name(row.shoko_file_id, source_key=SOURCE) == row.title
        retained = json.loads(store.get_meta(f"file_series_display:v1:{count}") or "null")
        assert retained == {
            "source_key": SOURCE,
            "scope_fingerprint": "e" * 64,
            "series_id": 1,
            "episode_file_id": count,
            "path": rows[-1].path,
            "title": "Managed Series",
        }


def test_bulk_series_display_skips_invalid_rows_without_changing_file_identity(
    tmp_path: Path,
) -> None:
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        walk_to_planned(store)
        before = store.get_file(101)
        budget = store.budget_view()
        good = FileSeriesDisplay(101, 1, 10, "/data/media/anime/show/s01e01.mkv", "Good")
        store.remember_file_series_names_bulk(
            [
                good,
                replace(good, series_id=2),
                replace(good, episode_file_id=11),
                replace(good, shoko_file_id=102),
                replace(good, path="/other-root/file.mkv"),
                replace(good, path=good.path + "\0"),
                replace(good, path="/" + "x" * 8192),
                replace(good, path="relative/file.mkv"),
                replace(good, title="x" * 65537),
                replace(good, title="\0"),
                replace(good, title=""),
                replace(good, series_id=True),
            ],
            source_key=SOURCE,
        )
        assert store.file_series_name(101) == "Good"
        assert store.file_series_name(102) == ""
        assert store.get_file(101) == before
        assert store.budget_view() == budget


def test_bulk_series_display_rolls_back_all_rows_on_database_failure(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        walk_to_planned(store, 101)
        walk_to_planned(store, 102)
        store.connection.execute(
            "CREATE TRIGGER reject_display BEFORE INSERT ON meta"
            " WHEN NEW.key='file_series_display:v1:102'"
            " BEGIN SELECT RAISE(ABORT,'injected display failure'); END"
        )
        rows = [
            FileSeriesDisplay(index, 1, 10, "/data/media/anime/show/s01e01.mkv", "Good")
            for index in (101, 102)
        ]
        with pytest.raises(WriteDisabledError, match="injected display failure"):
            store.remember_file_series_names_bulk(rows, source_key=SOURCE)
        assert store.get_meta("file_series_display:v1:101") is None
        assert store.get_meta("file_series_display:v1:102") is None
        assert store.writes_disabled


@pytest.mark.parametrize("change", ["identity", "stop"])
def test_bulk_series_display_rechecks_identity_and_stop_inside_transaction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    database = tmp_path / "state.db"
    with open_state(database, budget_limits=LIMITS) as store:
        walk_to_planned(store)
        transaction = store._transaction

        def change_before_transaction(**kwargs: Any) -> Any:
            with sqlite3.connect(database) as writer:
                if change == "identity":
                    writer.execute("UPDATE files SET sonarr_series_id=2 WHERE shoko_file_id=101")
                else:
                    writer.execute("INSERT INTO meta(key,value) VALUES('writes_disabled','1')")
            return transaction(**kwargs)

        monkeypatch.setattr(store, "_transaction", change_before_transaction)
        rows = [FileSeriesDisplay(101, 1, 10, "/data/media/anime/show/s01e01.mkv", "Good")]
        if change == "stop":
            with pytest.raises(WriteDisabledError):
                store.remember_file_series_names_bulk(rows, source_key=SOURCE)
        else:
            store.remember_file_series_names_bulk(rows, source_key=SOURCE)
        assert store.get_meta("file_series_display:v1:101") is None


@pytest.mark.parametrize(
    "raw",
    [
        "null",
        "false",
        "{}",
        "[[true,2,3]]",
        '[["1",2,3]]',
        "[[0,2,3]]",
        "[[1,2,3],[2,3]]",
        "[[9223372036854775808,2,3]]",
        "[[1,2,3]]\0",
        "[" * 1000,
        " " * 65537,
        json.dumps([[1, 2, 3]] * 257),
    ],
    ids=[str(index) for index in range(12)],
)
def test_corrupt_targets_supply_no_links(raw: str) -> None:
    assert display_anidb_ids(raw) == ()


def test_display_names_and_immutable_history_targets_are_local(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        walk_to_planned(store)
        path = "/data/media/anime/show/s01e01.mkv"
        store.remember_file_series_name(
            101,
            source_key=SOURCE,
            series_id=1,
            episode_file_id=10,
            path=path,
            title="Managed Series",
        )
        token = prepare(store, target_ids=((12, 2, 3), (11, 4, 5), (12, 6, 7)))
        assert token.accepted
        assert store.service_file_page("waiting").rows[0].known_anidb_ids == (11, 12)
        key = json.dumps([99, 1, 1, None, 22, 1, "Episode"])
        store.remember_mapping_targets_bulk({101: {key: (None, None)}})
        before = store.connection.total_changes
        statements: list[str] = []
        store.connection.set_trace_callback(statements.append)
        tracked = store.service_file_page("waiting").rows[0]
        history = action_history_view(store).examples[0]
        assert tracked.series_name == history.series_name == "Managed Series"
        assert tracked.known_anidb_ids == (22,)
        assert history.known_anidb_ids == (11, 12)
        assert store.connection.total_changes == before
        assert all(not s.startswith(("INSERT", "UPDATE", "DELETE")) for s in statements)
        assert store.file_series_name(101, source_key="d" * 64) == ""
        store.connection.execute("UPDATE files SET sonarr_series_id=2 WHERE shoko_file_id=101")
        assert action_history_view(store).examples[0].series_name == ""
        assert store.service_file_page("waiting").rows[0].series_name == ""
        assert ScanExclusionRow(101, "excluded", "unknown").known_anidb_ids == ()


def test_name_sanitization_and_wrong_source_identity_do_not_grant_authority(tmp_path: Path) -> None:
    register_secret("display-secret-abcdefgh")
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        walk_to_planned(store)
        before = store.get_file(101)
        budget = store.budget_view()
        path = "/data/media/anime/show/s01e01.mkv"
        store.remember_file_series_name(
            101,
            source_key=SOURCE,
            series_id=1,
            episode_file_id=10,
            path=path,
            title="Name <script> display-secret-abcdefgh https://secret.test/key\n\0" + "x" * 2000,
        )
        title = store.file_series_name(101)
        assert len(title) == 512
        assert "display-secret-abcdefgh" not in title
        assert "https://" not in title
        assert "\n" not in title
        assert "\0" not in title
        store.remember_file_series_name(
            101,
            source_key=SOURCE,
            series_id=2,
            episode_file_id=10,
            path=path,
            title="Other library",
        )
        assert store.file_series_name(101) == title
        assert store.get_file(101) == before
        assert store.budget_view() == budget
        raw = store.get_meta("file_series_display:v1:101")
        assert raw is not None
        assert "display-secret-abcdefgh" not in raw
        for malformed in (" " * 65537, "{}\0", "false", '{"series_id":true}'):
            store.set_meta("file_series_display:v1:101", malformed)
            assert store.file_series_name(101) == ""


def test_metadata_series_snapshot_survives_acceptance_restart_and_archive(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as store:
        first = store.prepare_metadata_operation(
            SOURCE,
            18208,
            demand_fingerprint="b" * 64,
            request_fingerprint="c" * 64,
            request_version=1,
            cycle_key="cycle",
            iso=T0,
            required_targets=(E1,),
            missing_targets=(E1,),
            series_name="First managed name",
        )
        assert isinstance(first, MetadataOperation)
        assert action_history_page(store).rows[0].series_name == "First managed name"
        ready = finish_episode(store, first)
        assert action_history_page(store).rows[0].series_name == "First managed name"
        second = store.prepare_metadata_operation(
            SOURCE,
            18208,
            demand_fingerprint="d" * 64,
            request_fingerprint="c" * 64,
            request_version=1,
            cycle_key="next",
            iso=T2,
            required_targets=(E2,),
            missing_targets=(E2,),
            after_ready_claim=ready.claim,
            series_name="Second name",
        )
        assert isinstance(second, MetadataOperation)
        assert second.created_iso == T2
    with open_state(path, budget_limits=LIMITS) as store:
        rows = action_history_page(store).rows
        assert [row.series_name for row in rows] == ["Second name", "First managed name"]
        assert all(row.known_anidb_ids == (18208,) for row in rows)
        store.connection.execute("DELETE FROM meta WHERE key LIKE 'metadata_series_display:%'")
        before = store.connection.total_changes
        assert all(row.series_name == "" for row in action_history_page(store).rows)
        assert store.connection.total_changes == before


def test_verified_inventory_title_enters_metadata_request_without_authority_change(
    executor: Executor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    demand = executor.demand
    assert demand.series_name == "Series 1"
    altered = replace(demand, series_name="Different display name")
    assert altered == demand
    assert metadata_demand_fingerprint(altered) == demand.fingerprint
    assert executor.run().reason == "metadata_accepted_waiting"
    item = action_history_page(executor.deps.store).rows[0]
    assert item.series_name == "Series 1"
    source = executor.row()
    assert source is not None
    view = metadata_status_view(executor.deps.store, source.source_key, enabled=True)
    assert view.examples[0].series_name == "Series 1"
    assert view.examples[0].known_anidb_ids == (source.anidb_anime_id,)
    # GET projections are entirely local after the mocked request completes.
    before = executor.deps.store.connection.total_changes
    request_count = len(executor.shoko.refreshes)

    def no_http(*args: object, **kwargs: object) -> None:
        pytest.fail("Display GET projection attempted HTTP")

    monkeypatch.setattr(httpx.Client, "send", no_http)
    action_history_page(executor.deps.store)
    executor.deps.store.service_file_page("waiting")
    metadata_status_view(executor.deps.store, source.source_key, enabled=True)
    assert len(executor.shoko.refreshes) == request_count
    assert executor.deps.store.connection.total_changes == before


@pytest.mark.parametrize("bad", [False, 0, -1, "12", None])
def test_frozen_mapping_corruption_never_becomes_a_link(bad: object) -> None:
    key = json.dumps([99, 1, 1, None, bad, 1, "Episode"])
    assert display_anidb_ids(json.dumps({key: [None, None]}), frozen=True) == ()


def test_frozen_mapping_ids_are_deduplicated_and_whole_document_checked() -> None:
    targets = {
        json.dumps([99, 1, n, None, anime, n, "Episode"]): [None, None]
        for n, anime in enumerate((12, 13, 12), start=1)
    }
    assert display_anidb_ids(json.dumps(targets), frozen=True) == (12, 13)
    targets["broken"] = [None, None]
    assert display_anidb_ids(json.dumps(targets), frozen=True) == ()


@pytest.mark.parametrize(
    "mismatch", ["source", "scope", "claim", "demand", "path", "file", "mapping", "none"]
)
def test_legacy_metadata_name_requires_exact_retained_source_and_target_context(
    executor: Executor,
    mismatch: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert executor.run().reason == "metadata_accepted_waiting"
    store = executor.deps.store
    operation = executor.row()
    assert operation is not None
    store.connection.execute("DELETE FROM meta WHERE key LIKE 'metadata_series_display:%'")
    # Simulate a legacy operation with current worker-observed, exact source title.
    assert action_history_page(store).rows[0].series_name == "Series 1"
    doc = json.loads(store.get_meta("service_metadata_status:v1") or "null")
    if mismatch == "source":
        doc["source_key"] = "f" * 64
    elif mismatch in {"scope", "claim", "demand", "path", "file"}:
        key, value = {
            "scope": ("scope_fingerprint", "f" * 64),
            "claim": ("claim", "f" * 32),
            "demand": ("demand_fingerprint", "f" * 64),
            "path": ("sonarr_path", "/other-library/file.mkv"),
            "file": ("shoko_file_id", True),
        }[mismatch]
        doc["examples"][0][key] = value
    elif mismatch == "mapping":
        store.set_meta("file_mapping_targets:v1:500", "{}")
    if mismatch != "none":
        store.set_meta("service_metadata_status:v1", json.dumps(doc))
    before = store.connection.total_changes

    def no_http(*args: object, **kwargs: object) -> None:
        pytest.fail("Legacy display fallback attempted HTTP")

    monkeypatch.setattr(httpx.Client, "send", no_http)
    name = action_history_page(store).rows[0].series_name
    assert name == ("Series 1" if mismatch == "none" else "")
    assert store.connection.total_changes == before


@pytest.mark.parametrize("anime", [True, 0, -1, 2**63])
def test_metadata_display_rejects_unbounded_or_boolean_ids(tmp_path: Path, anime: object) -> None:
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        store.set_meta(
            "service_metadata_status:v1",
            json.dumps(
                {
                    "version": 1,
                    "source_key": SOURCE,
                    "examples": [{"anime_id": anime}],
                }
            ),
        )
        assert metadata_status_view(store, SOURCE, enabled=True).examples == ()


@pytest.mark.parametrize("raw", [" " * 65537, "[[1,2,3]]\0"], ids=["oversize", "nul"])
def test_sql_discards_unsafe_targets_before_json_hydration(
    tmp_path: Path,
    raw: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        walk_to_planned(store)
        prepare(store, target_ids=((12, 2, 3),))
        store.connection.execute("UPDATE sends SET target_ids_json=?", (raw,))
        store.set_meta("file_mapping_targets:v1:101", raw)
        decode = json.loads

        def guarded_decode(value: str, *args: object, **kwargs: object) -> object:
            assert value != raw, "Unsafe persisted target text was hydrated"
            return decode(value, *args, **kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(json, "loads", guarded_decode)
        assert action_history_page(store).rows[0].known_anidb_ids == ()
        assert store.service_file_page("waiting").rows[0].known_anidb_ids == ()


def test_legacy_history_captures_title_scope_and_identity_in_one_sql_snapshot(
    executor: Executor,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert executor.run().reason == "metadata_accepted_waiting"
    store = executor.deps.store
    store.connection.execute("DELETE FROM meta WHERE key LIKE 'metadata_series_display:%'")
    raw = store.get_meta("file_series_display:v1:500")
    assert raw is not None
    changed = json.loads(raw)
    changed.update(title="Other source title", scope_fingerprint="f" * 64)
    database = store.connection.execute("PRAGMA database_list").fetchone()[2]
    decode = display_document
    switched = False

    def replace_context_during_hydration(value: object) -> Any:
        nonlocal switched
        if value == raw and not switched:
            switched = True
            # The old implementation reread this changed label after checking
            # the previous scope. One SQL snapshot must retain the original pair.
            with sqlite3.connect(database) as writer:
                writer.execute(
                    "UPDATE meta SET value=? WHERE key=?",
                    (json.dumps(changed), "file_series_display:v1:500"),
                )
        return decode(value)

    monkeypatch.setattr(state_module, "display_document", replace_context_during_hydration)
    statements: list[str] = []
    store.connection.set_trace_callback(statements.append)
    view = action_history_view(store)
    assert switched
    assert view.examples[0].series_name == "Series 1"
    assert len(statements) == 1
    assert "WITH keys" in statements[0]
    assert store.get_meta("file_series_display:v1:500") == json.dumps(changed)


@pytest.mark.parametrize(
    "key",
    ["service_metadata_status:v1", "file_series_display:v1:500", "file_mapping_targets:v1:500"],
)
@pytest.mark.parametrize("raw", [" " * 65537, "{}\0", b"{}"], ids=["oversized", "nul", "blob"])
def test_legacy_history_discards_unsafe_snapshot_evidence_before_hydration(
    executor: Executor,
    key: str,
    raw: str | bytes,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert executor.run().reason == "metadata_accepted_waiting"
    store = executor.deps.store
    store.connection.execute("DELETE FROM meta WHERE key LIKE 'metadata_series_display:%'")
    store.connection.execute("UPDATE meta SET value=? WHERE key=?", (raw, key))
    decode = display_document
    decode_ids = display_anidb_ids

    def guarded_decode(value: object) -> Any:
        assert value != raw, "Unsafe legacy display evidence was hydrated"
        return decode(value)

    def guarded_ids(value: object, *, frozen: bool = False) -> tuple[int, ...]:
        assert value != raw, "Unsafe legacy target evidence was hydrated"
        return decode_ids(value, frozen=frozen)

    monkeypatch.setattr(state_module, "display_document", guarded_decode)
    monkeypatch.setattr(state_module, "display_anidb_ids", guarded_ids)
    statements: list[str] = []
    store.connection.set_trace_callback(statements.append)
    before = store.connection.total_changes
    view = action_history_view(store)
    assert view.examples[0].series_name == ""
    assert len(statements) == 1
    assert store.connection.total_changes == before


@pytest.mark.parametrize("kind", ["waiting", "history"])
@pytest.mark.parametrize(
    "change",
    [
        "current",
        "endpoint",
        "draft_roots",
        "confirmed_roots",
        "refreshed_endpoint",
        "mid_read",
        "wrong_source",
        "wrong_scope",
    ],
)
def test_runtime_file_labels_require_current_confirmed_source_and_scope(  # noqa: PLR0915
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    change: str,
) -> None:
    runtime, endpoint, context = _confirmed_activity_runtime(tmp_path)
    database = tmp_path / "state.sqlite3"
    effective = settings_for_scope(
        runtime.settings, roots=context.binding.roots, mappings=context.binding.path_mappings
    )
    source = metadata_source_key_for_url(context.binding.shoko_url)
    scope = _metadata_scope_fingerprint(effective, RunPolicy.SERVICE)
    with open_state(database, budget_limits=WIDE_LIMITS) as store:
        walk_to_planned(store)
        store.remember_file_series_name(
            101,
            source_key="f" * 64 if change == "wrong_source" else source,
            scope_fingerprint="f" * 64 if change == "wrong_scope" else scope,
            series_id=1,
            episode_file_id=10,
            path="/data/media/anime/show/s01e01.mkv",
            title="Current endpoint series",
        )
        prepare(store, target_ids=((12, 2, 3),))

        def named_request(
            target: MetadataEpisodeIdentity,
            name: str,
            prior: MetadataOperation | None = None,
        ) -> MetadataOperation:
            result = store.prepare_metadata_operation(
                SOURCE,
                18208,
                demand_fingerprint="b" * 64,
                request_fingerprint="c" * 64,
                request_version=1,
                cycle_key="metadata",
                iso=T0,
                required_targets=(target,),
                missing_targets=(target,),
                series_name=name,
                after_ready_claim=prior.claim if prior else None,
            )
            assert isinstance(result, MetadataOperation)
            return result

        ready = finish_episode(store, named_request(E1, "Archived metadata series"))
        named_request(E2, "Current metadata series", ready)

        def change_endpoint() -> None:
            settings = load_service_settings(store, runtime.settings)
            settings["shoko_url"] = "http://other-shoko:8111"
            assert store.save_service_settings_if_current(
                store.get_meta(META_SERVICE_SETTINGS),
                json.dumps(settings),
                authorization_changed=True,
                iso=T2,
            )

        if change == "endpoint":
            change_endpoint()
        elif change == "draft_roots":
            store.set_meta(META_SERVICE_DRAFT_ROOTS, json.dumps(["/other-library"]))
        elif change == "confirmed_roots":
            binding = replace(
                context.binding,
                roots=("/other-library",),
                path_mappings=(PathMapping("/other-library", "/other-anime"),),
            )
            OnboardingManager(store).confirm_setup(
                binding,
                context.assessment,
                automatic_link_consent=True,
                iso=T2,
                confirmed_facts=context.assessment.unverifiable_facts
                + context.assessment.unknown_settings,
            )
        elif change == "refreshed_endpoint":
            change_endpoint()
            binding = replace(context.binding, shoko_url="http://other-shoko:8111")
            OnboardingManager(store).confirm_setup(
                binding,
                context.assessment,
                automatic_link_consent=True,
                iso=T2,
                confirmed_facts=context.assessment.unverifiable_facts
                + context.assessment.unknown_settings,
            )
            fresh = settings_for_scope(
                replace(runtime.settings, shoko_url=binding.shoko_url),
                roots=binding.roots,
                mappings=binding.path_mappings,
            )
            store.remember_file_series_name(
                101,
                source_key=metadata_source_key_for_url(binding.shoko_url),
                scope_fingerprint=_metadata_scope_fingerprint(fresh, RunPolicy.SERVICE),
                series_id=1,
                episode_file_id=10,
                path="/data/media/anime/show/s01e01.mkv",
                title="Current endpoint series",
            )
        elif change == "mid_read":
            checks = 0

            def context_drift(*args: Any, **kwargs: Any) -> str | None:
                nonlocal checks
                checks += 1
                if checks == 2:
                    change_endpoint()
                return scan_context_identity(*args, **kwargs)

            monkeypatch.setattr(cli, "scan_context_identity", context_drift)

        def no_http(*args: object, **kwargs: object) -> None:
            pytest.fail("Runtime table attempted HTTP")

        monkeypatch.setattr(httpx.Client, "send", no_http)
        runtime_changes: list[int] = []
        close = state_module.StateStore.close

        def record_runtime_changes(current: state_module.StateStore) -> None:
            if current is not store:
                runtime_changes.append(current.connection.total_changes)
            close(current)

        monkeypatch.setattr(state_module.StateStore, "close", record_runtime_changes)
        before = store.connection.total_changes
        try:
            table = runtime.table(kind, 1, 50)
        finally:
            endpoint.client.close()
        assert runtime_changes == [0]
        expected = "Current endpoint series" if change in {"current", "refreshed_endpoint"} else ""
        if kind == "waiting":
            assert table.files is not None
            assert table.files.rows[0].series_name == expected
            assert table.files.rows[0].known_anidb_ids == (12,)
        else:
            assert table.history is not None
            file_row = next(row for row in table.history.rows if row.operation == "file_link")
            assert file_row.series_name == expected
            assert file_row.known_anidb_ids == (12,)
            metadata = [row for row in table.history.rows if row.operation == "metadata_refresh"]
            assert {row.series_name for row in metadata} == {
                "Archived metadata series",
                "Current metadata series",
            }
            assert all(row.known_anidb_ids == (18208,) for row in metadata)
        if change != "mid_read":
            assert store.connection.total_changes == before
