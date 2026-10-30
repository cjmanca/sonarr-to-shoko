"""Offline, real SQLite tests for explicit local source-baseline acceptance."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, replace
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from test_state import LIMITS, walk_to_planned
from test_state import prepare as prepare_send

from shoko_linker.linking import VerificationVerdict
from shoko_linker.state import (
    MAX_CONTINUITY_DOCUMENT_CHARS,
    META_SERVICE_AUTHORIZATION_REVISION,
    META_SERVICE_DRAFT_ROOTS,
    META_SERVICE_SETTINGS,
    META_SOURCE_REBASELINE_AUDIT,
    META_SOURCE_REBASELINE_PREPARED,
    Cohort,
    FileSeed,
    FileState,
    SourceObservation,
    SourceRebaselineAssessment,
    SourceRebaselineBindings,
    SourceRebaselinePrepared,
    StateError,
    StateStore,
    WriteDisabledError,
    open_state,
)

T0 = "2026-01-01T00:00:00+00:00"
T1 = "2026-01-01T00:01:00+00:00"
T2 = "2026-01-01T00:02:00+00:00"
T3 = "2026-01-01T00:03:00+00:00"
EXPIRY = "2026-01-01T00:10:00+00:00"
SESSION = "a" * 64
OBSERVATION = SourceObservation("configured-source", "roots", "weak-snapshot", 56)


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def assessment(at: str = T1) -> SourceRebaselineAssessment:
    return SourceRebaselineAssessment(
        OBSERVATION, digest("full normalized inventory: files paths relationships"), at, at
    )


def bindings(store: StateStore) -> SourceRebaselineBindings:
    return SourceRebaselineBindings(
        digest("effective endpoints settings scope"),
        digest("mapping content"),
        store.get_source_rebaseline_local_binding(),
    )


def poll(store: StateStore, total: int = 56, at: str = T1, **kwargs: Any) -> None:
    store.check_source_continuity(
        "configured-source",
        "weak-snapshot",
        iso=at,
        roots_fingerprint="roots",
        discovery_total=total,
        **kwargs,
    )


def setup(store: StateStore) -> None:
    store.create_dashboard_account("operator", "scrypt$test", b"s" * 32, T0)
    store.create_dashboard_session(SESSION, "b" * 64, 2_000_000_000, T0)
    store.save_onboarding_json('{"consent":true}', T0)
    store.set_meta(META_SERVICE_SETTINGS, '{"mode":"monitor"}')
    store.set_meta(META_SERVICE_DRAFT_ROOTS, '["/anime"]')
    store.set_meta(META_SERVICE_AUTHORIZATION_REVISION, "c" * 32)
    poll(store, 57, T0)
    poll(store)


def prepare(store: StateStore) -> SourceRebaselinePrepared:
    hold = store.get_source_continuity_review().hold
    return store.prepare_source_rebaseline(
        assessment(),
        expected_hold_id=None if hold is None else hold.hold_id,
        bindings=bindings(store),
        owner_session_sha256=SESSION,
        iso=T1,
        expires_iso=EXPIRY,
    )


def confirm(store: StateStore, prepared: SourceRebaselinePrepared, **kwargs: Any) -> Any:
    arguments: dict[str, Any] = {
        "bindings": bindings(store),
        "owner_session_sha256": SESSION,
        "iso": T3,
    } | kwargs
    return store.confirm_source_rebaseline(prepared.token, assessment(T2), **arguments)


def tables(store: StateStore) -> dict[str, list[tuple[Any, ...]]]:
    return {
        name: [
            tuple(row)
            for row in store._conn.execute(
                f'SELECT * FROM "{name}"'  # noqa: S608 - names from SQLite's own schema.
            )
        ]
        for (name,) in store._conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        if name != "meta"
    }


def test_atomic_acceptance_changes_only_continuity_and_local_audit(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with open_state(path, budget_limits=LIMITS) as store:
        setup(store)
        # Representative unrelated durable state, including pause owners and file ledgers.
        walk_to_planned(store)
        send = prepare_send(store)
        store.mark_sent(send, iso=T0)
        store.create_campaign(
            "paused-campaign",
            digest("campaign"),
            max_submissions=1,
            deadline_iso=None,
            attestation_scope="campaign:paused-campaign",
            approval_iso=T0,
            iso=T0,
        )
        store.set_campaign_status("paused-campaign", "paused", "operator review", iso=T0)
        store.pause_service_for_user(T0)
        store.pause("unrelated budget pause", T0, origin="operator")
        store.set_meta("normal_service_canary_v1", '{"file_id":12}')
        store.set_meta("file_mapping_quarantine:v1:12", "review")
        store.set_meta("unrelated", "preserve")
        store.record_files([FileSeed(12, 1, 12, "/anime/file.mkv", 10, Cohort.BACKLOG, T0)])
        prepared = prepare(store)
        before = tables(store)
        metadata = dict(store._conn.execute("SELECT key, value FROM meta"))
        poll(store, at=T2)  # timestamp-only movement must preserve review validity.
        audit = confirm(store, prepared)
        assert tables(store) == before
        after = dict(store._conn.execute("SELECT key, value FROM meta"))
        allowed = {
            "requires_rebaseline",
            "source_continuity_hold_v1",
            "source_continuity_latest_complete_v1",
            "source_continuity_check_complete",
            "source_last_observed_iso",
            META_SOURCE_REBASELINE_PREPARED,
            META_SOURCE_REBASELINE_AUDIT,
        }
        assert {k: v for k, v in metadata.items() if k not in allowed} == {
            k: v for k, v in after.items() if k not in allowed
        }
        assert not store.get_source_continuity_review().requires_rebaseline
        review = store.get_source_rebaseline_review()
        assert review.valid
        assert review.prepared is None
        assert review.last_audit == audit
        assert audit.before.prior is not None
        assert audit.before.prior.discovery_total == 57
        assert audit.prepared.observation.discovery_total == 56
        assert "scrypt" not in after[META_SOURCE_REBASELINE_AUDIT]
        assert SESSION not in after[META_SOURCE_REBASELINE_AUDIT]
    with open_state(path) as store:
        assert store.get_source_rebaseline_review().last_audit == audit
        with pytest.raises(StateError):
            confirm(store, prepared)


def test_competing_preparations_and_two_handles_single_use(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with open_state(path) as first, open_state(path) as second:
        setup(first)
        old = prepare(first)
        current = prepare(second)
        with pytest.raises(StateError):
            confirm(first, old)
        confirm(second, current)
        with pytest.raises(StateError):
            confirm(first, current)


@pytest.mark.parametrize("started", [T1, "2025-12-31T17:01:00-07:00", T0])
def test_confirmation_rejects_reused_or_preparation_time_assessment(
    tmp_path: Path, started: str
) -> None:
    with open_state(tmp_path / "state.db") as store:
        setup(store)
        prepared = prepare(store)
        before = dict(store._conn.execute("SELECT key, value FROM meta"))
        other_tables = tables(store)
        with pytest.raises(StateError, match="expired or changed"):
            store.confirm_source_rebaseline(
                prepared.token,
                assessment(started),
                bindings=bindings(store),
                owner_session_sha256=SESSION,
                iso=T3,
            )
        assert dict(store._conn.execute("SELECT key, value FROM meta")) == before
        assert tables(store) == other_tables
        # A strictly later interval succeeds, even with a lexically earlier offset date.
        fresh = SourceRebaselineAssessment(
            OBSERVATION, assessment().content_sha256, "2025-12-31T17:02:00-07:00", T3
        )
        audit = store.confirm_source_rebaseline(
            prepared.token, fresh, bindings=bindings(store), owner_session_sha256=SESSION, iso=T3
        )
        assert store.get_source_rebaseline_review().last_audit == audit


def test_audit_decoder_rejects_collection_start_at_preparation_time(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.db") as store:
        setup(store)
        prepared = prepare(store)
        audit = asdict(confirm(store, prepared))
        audit["assessment_started_iso"] = prepared.created_iso
        store.set_meta(
            META_SOURCE_REBASELINE_AUDIT, json.dumps(audit, sort_keys=True, separators=(",", ":"))
        )
        assert not store.get_source_rebaseline_review().valid


def test_same_session_csrf_rotation_preserves_rebaseline_owner(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.db") as store:
        setup(store)
        # dashboard_control rotates before dispatching both prepare and confirm actions.
        assert store.rotate_dashboard_session_csrf(
            SESSION, "b" * 64, "c" * 64, int(datetime.fromisoformat(T1).timestamp())
        )
        prepared = prepare(store)
        assert store.rotate_dashboard_session_csrf(
            SESSION, "c" * 64, "d" * 64, int(datetime.fromisoformat(T3).timestamp())
        )
        assert not store.rotate_dashboard_session_csrf(
            SESSION, "c" * 64, "e" * 64, int(datetime.fromisoformat(T3).timestamp())
        )
        audit = confirm(store, prepared)
        assert audit.prepared.owner_sha256 == prepared.owner_sha256
        assert store.get_source_rebaseline_review().last_audit == audit


def test_valid_session_expiry_extension_preserves_owner(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.db") as store:
        setup(store)
        prepared = prepare(store)
        # No public extension API exists; simulate only a durable valid expiry change.
        store._conn.execute("UPDATE dashboard_sessions SET expires_epoch = 2100000000")
        confirm(store, prepared)


@pytest.mark.parametrize(
    "change",
    [
        "expiry",
        "content",
        "path",
        "relationships",
        "mapping",
        "effective",
        "owner",
        "session_deleted",
        "account",
        "consent",
        "revision",
        "settings",
        "scope",
        "source",
        "incomplete",
        "accounting",
        "clear",
        "record",
        "aba",
    ],
)
def test_stale_reviews_fail_closed(tmp_path: Path, change: str) -> None:  # noqa: PLR0912
    with open_state(tmp_path / "state.db") as store:
        setup(store)
        prepared = prepare(store)
        fresh = assessment(T2)
        bound = bindings(store)
        at, owner = T3, SESSION
        if change == "expiry":
            at = EXPIRY
        elif change in {"content", "path", "relationships"}:
            fresh = replace(fresh, content_sha256=digest(change))
        elif change == "mapping":
            bound = replace(bound, mapping_identity_sha256=digest("new mapping"))
        elif change == "effective":
            bound = replace(bound, effective_identity_sha256=digest("new settings"))
        elif change == "owner":
            owner = "d" * 64
            store.create_dashboard_session(owner, "e" * 64, 2_000_000_000, T0)
        elif change == "session_deleted":
            store._conn.execute("DELETE FROM dashboard_sessions")
        elif change == "account":
            store._conn.execute("UPDATE dashboard_account SET password_hash = 'scrypt$new'")
        elif change == "consent":
            store.save_onboarding_json('{"consent":false}', T2)
        elif change in {"revision", "settings", "scope"}:
            key = {
                "revision": META_SERVICE_AUTHORIZATION_REVISION,
                "settings": META_SERVICE_SETTINGS,
                "scope": META_SERVICE_DRAFT_ROOTS,
            }[change]
            store.set_meta(key, '"changed"')
        elif change == "source":
            poll(store, 55, T2)
            fresh = replace(fresh, observation=replace(OBSERVATION, discovery_total=55))
        elif change == "incomplete":
            poll(store, at=T2, complete=False)
        elif change == "accounting":
            store.record_files(
                [
                    FileSeed(
                        1, 1, 1, "/anime/a.mkv", 1, Cohort.BACKLOG, T0, state=FileState.VERIFYING
                    )
                ]
            )
            store.record_verification(1, VerificationVerdict.PARTIAL, iso=T2)
        elif change in {"clear", "aba"}:
            store.clear_requires_rebaseline(T2)
            if change == "aba":
                poll(store, 55, T2)
                poll(store, 56, T2)
        elif change == "record":
            store.record_source_observation(
                "configured-source",
                "weak-snapshot",
                iso=T2,
                roots_fingerprint="roots",
                discovery_total=56,
            )
        with pytest.raises(StateError):
            store.confirm_source_rebaseline(
                prepared.token, fresh, bindings=bound, owner_session_sha256=owner, iso=at
            )
        if change not in {"clear", "record"}:
            assert store.get_source_continuity_review().requires_rebaseline
        assert store.get_source_rebaseline_review().last_audit is None


@pytest.mark.parametrize(
    "change", ["session_expired", "session_recreated", "account_username", "account_signing_key"]
)
def test_confirmation_retains_live_owner_and_account_checks(tmp_path: Path, change: str) -> None:
    with open_state(tmp_path / "state.db") as store:
        setup(store)
        prepared = prepare(store)
        if change == "session_expired":
            store._conn.execute(
                "UPDATE dashboard_sessions SET expires_epoch = ?",
                (int(datetime.fromisoformat(T3).timestamp()),),
            )
        elif change == "session_recreated":
            store._conn.execute("DELETE FROM dashboard_sessions")
            store.create_dashboard_session(SESSION, "b" * 64, 2_000_000_000, T2)
        elif change == "account_username":
            store._conn.execute("UPDATE dashboard_account SET username = 'different-account'")
        elif change == "account_signing_key":
            store._conn.execute("UPDATE dashboard_account SET signing_key = ?", (b"n" * 32,))
        before = dict(store._conn.execute("SELECT key, value FROM meta"))
        with pytest.raises(StateError):
            confirm(store, prepared)
        assert dict(store._conn.execute("SELECT key, value FROM meta")) == before


def test_audit_failure_rolls_back_anchor_flag_and_token(tmp_path: Path, monkeypatch: Any) -> None:
    with open_state(tmp_path / "state.db") as store:
        setup(store)
        prepared = prepare(store)
        before = dict(store._conn.execute("SELECT key, value FROM meta"))
        original = store._set_meta_tx

        def fail(key: str, value: str) -> None:
            original(key, value)
            if key == META_SOURCE_REBASELINE_AUDIT:
                raise StateError("injected audit failure")

        monkeypatch.setattr(store, "_set_meta_tx", fail)
        with pytest.raises(StateError, match="injected"):
            confirm(store, prepared)
        assert dict(store._conn.execute("SELECT key, value FROM meta")) == before


@pytest.mark.parametrize(
    "bad",
    [
        "{}",
        "x" * (MAX_CONTINUITY_DOCUMENT_CHARS + 1),
        b"blob",
        1,
        '{"version":1,"version":1}',
        '{"version":1}\u0000',
    ],
)
def test_malformed_storage_never_hydrates_or_clears(tmp_path: Path, bad: Any) -> None:
    with open_state(tmp_path / "state.db") as store:
        setup(store)
        prepared = prepare(store)
        store._conn.execute(
            "UPDATE meta SET value = ? WHERE key = ?",
            (
                bad,
                META_SOURCE_REBASELINE_PREPARED,
            ),
        )
        review = store.get_source_rebaseline_review()
        assert not review.valid
        assert review.prepared is None
        with pytest.raises(StateError):
            confirm(store, prepared)
        assert store.get_source_continuity_review().requires_rebaseline


def test_legacy_hold_review_preserves_unknown_first_cause(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.db") as store:
        setup(store)
        store._conn.execute("DELETE FROM meta WHERE key = 'source_continuity_hold_v1'")
        assert store.get_source_continuity_review().hold is None
        prepared = prepare(store)
        hold = store.get_source_continuity_review().hold
        assert hold is not None
        assert hold.hold_id == prepared.hold_id
        assert hold.prior is None
        assert hold.current is None
        assert hold.detected_iso is None
        audit = confirm(store, prepared)
        assert audit.before == hold


def test_write_stop_remains_terminal(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.db") as store:
        setup(store)
        prepared = prepare(store)
        store.disable_writes("storage incident")
        before = dict(store._conn.execute("SELECT key, value FROM meta"))
        with pytest.raises(WriteDisabledError):
            confirm(store, prepared)
        assert dict(store._conn.execute("SELECT key, value FROM meta")) == before


@pytest.mark.parametrize("invalid", ["x" * 65, "2026-01-01T25:00:00Z", "\u0000", ""])
def test_invalid_times_never_begin_transaction(
    tmp_path: Path, invalid: str, monkeypatch: Any
) -> None:
    with open_state(tmp_path / "state.db") as store:
        setup(store)
        hold = store.get_source_continuity_review().hold
        assert hold is not None
        bound = bindings(store)

        def fail(*args: Any, **kwargs: Any) -> Any:
            pytest.fail("transaction began before timestamp validation")

        monkeypatch.setattr(store, "_transaction", fail)
        for fresh in (
            replace(assessment(), started_iso=invalid),
            replace(assessment(), completed_iso=invalid),
        ):
            with pytest.raises(StateError):
                store.prepare_source_rebaseline(
                    fresh,
                    expected_hold_id=hold.hold_id,
                    bindings=bound,
                    owner_session_sha256=SESSION,
                    iso=T1,
                    expires_iso=EXPIRY,
                )
            with pytest.raises(StateError):
                store.confirm_source_rebaseline(
                    "a" * 32,
                    fresh,
                    bindings=bound,
                    owner_session_sha256=SESSION,
                    iso=T3,
                )


def test_canonical_stored_review_rejects_unknown_fields(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.db") as store:
        setup(store)
        prepared = prepare(store)
        raw = asdict(prepared) | {"unexpected": "field"}
        store.set_meta(
            META_SOURCE_REBASELINE_PREPARED, json.dumps(raw, sort_keys=True, separators=(",", ":"))
        )
        assert not store.get_source_rebaseline_review().valid


@pytest.mark.parametrize(
    "bad",
    ["", "A" * 32, "a" * 33, "\x00" * 32, "credential" * 10_000],
    ids=["empty", "uppercase", "long", "nul", "oversized"],
)
def test_invalid_tokens_are_bounded_and_errors_do_not_echo_input(tmp_path: Path, bad: str) -> None:
    with open_state(tmp_path / "state.db") as store:
        setup(store)
        prepare(store)
        with pytest.raises(StateError, match="^invalid rebaseline token$"):
            store.confirm_source_rebaseline(
                bad, assessment(T2), bindings=bindings(store), owner_session_sha256=SESSION, iso=T3
            )
        assert store.get_source_continuity_review().requires_rebaseline


def test_getters_are_read_only_and_invalid_current_times_never_begin(
    tmp_path: Path, monkeypatch: Any
) -> None:
    with open_state(tmp_path / "state.db") as store:
        setup(store)
        prepared = prepare(store)
        before = store._conn.total_changes
        assert store.get_source_rebaseline_review().prepared == prepared
        bound = bindings(store)
        assert store._conn.total_changes == before

        def fail(*args: Any, **kwargs: Any) -> Any:
            pytest.fail("transaction began before timestamp validation")

        monkeypatch.setattr(store, "_transaction", fail)
        for created, expires in (("bad", EXPIRY), (T1, "bad")):
            with pytest.raises(StateError):
                store.prepare_source_rebaseline(
                    assessment(),
                    expected_hold_id=prepared.hold_id,
                    bindings=bound,
                    owner_session_sha256=SESSION,
                    iso=created,
                    expires_iso=expires,
                )
        with pytest.raises(StateError):
            store.confirm_source_rebaseline(
                prepared.token,
                assessment(T2),
                bindings=bound,
                owner_session_sha256=SESSION,
                iso="bad",
            )


@pytest.mark.parametrize("field", ["instance_identity", "roots_fingerprint", "snapshot_summary_id"])
def test_source_proof_drift_rejects_review(tmp_path: Path, field: str) -> None:
    with open_state(tmp_path / "state.db") as store:
        setup(store)
        prepared = prepare(store)
        changes: dict[str, Any] = {field: "changed"}
        observation = replace(OBSERVATION, **changes)
        store.check_source_continuity(
            observation.instance_identity,
            observation.snapshot_summary_id,
            roots_fingerprint=observation.roots_fingerprint,
            discovery_total=56,
            iso=T2,
        )
        with pytest.raises(StateError):
            store.confirm_source_rebaseline(
                prepared.token,
                replace(assessment(T2), observation=observation),
                bindings=bindings(store),
                owner_session_sha256=SESSION,
                iso=T3,
            )
        assert store.get_source_continuity_review().requires_rebaseline
