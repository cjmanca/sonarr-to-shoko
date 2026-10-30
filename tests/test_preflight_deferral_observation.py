"""Attempted preflight diagnostics are historical evidence, never scheduling writes."""

from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from test_daemon import (
    ANIME_ID,
    ShokoStub,
    SpecFile,
    activate_mapping,
    build_deps,
    mapping_xml,
    open_tmp_store,
    seed_baseline,
)
from test_state import T0, T1, T2, WIDE_LIMITS, prepare, walk_to_planned

from shoko_linker.daemon import CycleMode, run_cycle
from shoko_linker.logging_setup import register_secret
from shoko_linker.policies import RunPolicy
from shoko_linker.state import (
    MAX_DETAIL_CHARS,
    Cohort,
    EligibilityVerdict,
    FileIdentitySnapshot,
    FileRecord,
    FileState,
    StateError,
    StateStore,
    open_state,
)

TARGETS: dict[str, tuple[int | None, int | None]] = {"source": (100, 200)}


def _seed(store: StateStore) -> FileRecord:
    walk_to_planned(store)
    walk_to_planned(store, 102)
    store.remember_mapping_targets_bulk({101: TARGETS, 102: TARGETS})
    record = store.get_file(101)
    assert record is not None
    return record


def _observe(
    store: StateStore, expected: FileRecord, message: str = "HTTP 503; retry later"
) -> bool:
    return store.record_preflight_deferral(
        expected,
        expected_targets=TARGETS,
        code="target_unavailable",
        message=message,
        observed_iso=T1,
    )


def _snapshot(store: StateStore) -> dict[str, list[tuple[object, ...]]]:
    """All persisted data, so clock/budget/target invariance is tested independently."""
    tables = store.connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    return {
        name: store.connection.execute(f'SELECT * FROM "{name}"').fetchall()  # noqa: S608
        for (name,) in tables
    }


def test_observation_changes_only_detail_preserving_clocks_order_and_budget(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_state(path, budget_limits=WIDE_LIMITS) as store:
        expected = _seed(store)
        before = _snapshot(store)
        assert _observe(store, expected)
        updated = store.get_file(101)
        assert updated is not None
        assert replace(updated, state_detail=expected.state_detail) == expected
        assert store.service_file_page("waiting").rows[0].reason == "pre_send"
        # Restore just the observed column to compare every persisted table.
        store.connection.execute(
            "UPDATE files SET state_detail=? WHERE shoko_file_id=101", (expected.state_detail,)
        )
        assert _snapshot(store) == before
        assert _observe(store, expected)
        persisted = store.get_file(101)
    with open_state(path, budget_limits=WIDE_LIMITS) as store:
        assert store.get_file(101) == persisted
        assert persisted is not None
        assert store.record_preflight_deferral(
            persisted,
            expected_targets=TARGETS,
            code="file_unavailable",
            message="A later attempted read failed; retry later",
            observed_iso=T2,
        )
        updated = store.get_file(101)
        assert updated is not None
        assert "file_unavailable" in updated.state_detail
        assert "target_unavailable" not in updated.state_detail
        assert T2 in updated.state_detail
        assert updated.updated_iso == T0
        store.advance(101, FileState.BLOCKED_SAFETY, "preflight_quarantine:conflict", iso=T2)
        updated = store.get_file(101)
        assert updated is not None
        assert updated.state_detail == "preflight_quarantine:conflict"


@pytest.mark.parametrize(
    "field",
    ["sonarr_series_id", "sonarr_file_id", "sonarr_path", "sonarr_size", "identity_json", "cohort"],
)
def test_stale_candidate_identity_is_rejected(tmp_path: Path, field: str) -> None:
    with open_state(tmp_path / "state.sqlite3") as store:
        current = _seed(store)
        values: dict[str, Any] = {
            "sonarr_series_id": 2,
            "sonarr_file_id": 20,
            "sonarr_path": "/another/file.mkv",
            "sonarr_size": 9876,
            "identity_json": repr(FileIdentitySnapshot(ed2k="a" * 32)),
            "cohort": Cohort.BACKLOG,
        }
        stale = replace(current, **{field: values[field]})
        before = _snapshot(store)
        assert not _observe(store, stale)
        assert _snapshot(store) == before


@pytest.mark.parametrize("change", ["targets", "missing_targets", "nonplanned", "quarantine"])
def test_changed_or_missing_frozen_targets_and_nonplanned_rows_reject(
    tmp_path: Path, change: str
) -> None:
    with open_state(tmp_path / "state.sqlite3") as store:
        expected = _seed(store)
        if change == "targets":
            store.set_meta("file_mapping_targets:v1:101", '{"source":[100,201]}')
        elif change == "missing_targets":
            store.connection.execute("DELETE FROM meta WHERE key='file_mapping_targets:v1:101'")
        elif change == "quarantine":
            store.quarantine_mapping_targets((101,), iso=T1)
        else:
            store.advance(101, FileState.BLOCKED_SAFETY, "hard stop", iso=T1)
        before = _snapshot(store)
        assert not _observe(store, expected)
        assert _snapshot(store) == before


def test_redacts_before_clipping_and_removes_control_characters(tmp_path: Path) -> None:
    secret = "observation-secret-value"
    register_secret(secret)
    with open_state(tmp_path / "state.sqlite3") as store:
        expected = _seed(store)
        message = (
            f"{secret}\x00\n\r\x1b https://user:{secret}@private.invalid/path?apikey={secret} "
            + "x" * MAX_DETAIL_CHARS
            + f" https://private.invalid/{secret}"
        )
        assert _observe(store, expected, message)
        updated = store.get_file(101)
        assert updated is not None
        detail = updated.state_detail
        assert len(detail) <= MAX_DETAIL_CHARS
        assert secret not in detail
        assert "https://" not in detail
        assert "private.invalid" not in detail
        assert "[URL omitted]" in detail
        assert "***REDACTED***" in detail
        assert not any(ord(char) < 32 for char in detail)


@pytest.mark.parametrize("payload", ["observation-boundary-secret", "https://private.invalid/key"])
def test_redaction_precedes_clipping_at_the_limit(tmp_path: Path, payload: str) -> None:
    register_secret("observation-boundary-secret")
    prefix = f"preflight_deferred:target_unavailable: observed {T1} (last attempted preflight): "
    padding = "x" * (MAX_DETAIL_CHARS - len(prefix) - 5)
    with open_state(tmp_path / "state.sqlite3") as store:
        assert _observe(store, _seed(store), padding + payload)
        record = store.get_file(101)
        assert record is not None
        assert payload[:5] not in record.state_detail[len(prefix) :]
        assert len(record.state_detail) <= MAX_DETAIL_CHARS


def test_successful_preparation_replaces_historical_reason(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS) as store:
        assert _observe(store, _seed(store))
        prepare(store, iso=T2)
        record = store.get_file(101)
        assert record is not None
        assert record.state is FileState.PREPARED_NOT_SENT
        assert record.state_detail == "prepared for send"


@pytest.mark.parametrize(
    "observed_iso",
    ["not-a-timestamp", "2026-01-01T00:00:00." + "1" * 1001 + "+00:00"],
)
def test_invalid_or_oversized_observation_timestamp_is_rejected_before_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, observed_iso: str
) -> None:
    with open_state(tmp_path / "state.sqlite3") as store:
        expected = _seed(store)
        before = _snapshot(store)

        def no_transaction() -> None:
            pytest.fail("transaction began before timestamp validation")

        with monkeypatch.context() as patch:
            patch.setattr(store, "_transaction", no_transaction)
            with pytest.raises(StateError, match="observed_iso:.*bounded observation timestamp"):
                store.record_preflight_deferral(
                    expected,
                    expected_targets=TARGETS,
                    code="target_unavailable",
                    message="HTTP 503; retry later",
                    observed_iso=observed_iso,
                )
        assert _snapshot(store) == before
        assert _observe(store, expected)
        updated = store.get_file(101)
        assert updated is not None
        assert "HTTP 503; retry later" in updated.state_detail


def test_changed_stored_source_cannot_acquire_attempted_plan_diagnostic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    files = [SpecFile(series_id=1, file_id=10, shoko_id=500)]
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [], T0)
    shoko = ShokoStub(files)
    shoko.target_read_failures[f"/api/v3/Series/AniDB/{ANIME_ID}/Episode"] = (2, 503)
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=files,
        shoko_files=files,
        xml=mapping_xml(),
        monotonic=lambda: 0.0,
        advance=lambda _seconds: None,
        shoko=shoko,
    )
    original = store.can_auto_apply
    replaced: list[FileRecord] = []

    def replace_source_after_eligibility(*args: Any, **kwargs: Any) -> EligibilityVerdict:
        result = original(*args, **kwargs)
        if args and args[0] == 500 and result.eligible:
            store.connection.execute(
                "UPDATE files SET sonarr_path='/replacement/file.mkv' WHERE shoko_file_id=500"
            )
            record = store.get_file(500)
            assert record is not None
            replaced.append(record)
        return result

    monkeypatch.setattr(store, "can_auto_apply", replace_source_after_eligibility)
    try:
        result = run_cycle(
            deps,
            now_iso=T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="replaced-source",
        )
        assert result.deferred_planned == 1
        assert any("target_unavailable" in reason for reason in result.reasons)
        assert len(replaced) == 1
        assert store.get_file(500) == replaced[0]
        assert "preflight_deferred" not in replaced[0].state_detail
        assert not shoko.posts
        assert store.connection.execute("SELECT COUNT(*) FROM sends").fetchone()[0] == 0
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()


@pytest.mark.parametrize("service_mode", [False, True])
@pytest.mark.parametrize("hold", [True, False])
def test_global_hold_and_unvisited_rows_do_not_receive_attempt_diagnostics(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, service_mode: bool, hold: bool
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    files = [SpecFile(series_id=1, file_id=10, shoko_id=500)]
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [], T0)
    shoko = ShokoStub(files)
    deps = build_deps(
        tmp_path,
        store=store,
        sonarr_files=files,
        shoko_files=files,
        xml=mapping_xml(),
        monotonic=lambda: 0.0,
        advance=lambda _seconds: None,
        shoko=shoko,
    )
    if service_mode:
        deps = replace(
            deps,
            service_mode=True,
            service_authorized=lambda: True,
            service_authorized_local=lambda: True,
        )
    policy = RunPolicy.SERVICE if service_mode else RunPolicy.NEW_ARRIVAL
    try:
        run_cycle(deps, now_iso=T0, mode=CycleMode.DRY_RUN, policy=policy, cycle_key="plan")
        before = store.get_file(500)
        assert before is not None
        assert before.state is FileState.PLANNED
        if hold:
            store.set_meta("requires_rebaseline", "1")
        else:
            deps = replace(deps, shutdown_requested=lambda: True)
        result = run_cycle(
            deps, now_iso=T1, mode=CycleMode.APPLY, policy=policy, cycle_key="unvisited"
        )
        assert result.counts.submitted == 0
        assert store.get_file(500) == before
        assert "preflight_deferred" not in before.state_detail
        assert not shoko.posts
        assert store.connection.execute("SELECT COUNT(*) FROM sends").fetchone()[0] == 0
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()
