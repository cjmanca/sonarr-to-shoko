"""Offline durable first-cause evidence and local read-only review contracts."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from test_state import LIMITS, T0, T1, T2, T3, prepare, walk_to_planned

from shoko_linker.linking import VerificationVerdict
from shoko_linker.state import (
    MAX_CONTINUITY_DOCUMENT_CHARS,
    META_SOURCE_CONTINUITY_HOLD,
    META_SOURCE_CONTINUITY_LATEST,
    Cohort,
    FileSeed,
    FileState,
    StateError,
    StateStore,
    WriteDisabledError,
    open_state,
)


def check(store: StateStore, total: int, iso: str = T0, **kwargs: Any) -> None:
    store.check_source_continuity(
        "configured-source",
        "snapshot",
        iso=iso,
        roots_fingerprint="roots",
        discovery_total=total,
        **kwargs,
    )


def test_first_cause_survives_poll_and_restart(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    with open_state(path) as store:
        check(store, 57)
        check(store, 56, T1)
        first = store.get_source_continuity_review()
        assert first.evidence_status == "available"
        assert first.hold is not None
        assert first.hold.prior is not None
        assert first.hold.current is not None
        assert first.hold.prior.discovery_total == 57
        assert first.hold.current.discovery_total == 56
        assert first.hold.detected_iso == T1
        assert first.hold.previous_anchor_iso == T0
        assert first.hold.reasons == ("discovery_total_drop",)
        assert first.hold.expected_drop == first.hold.tolerance == 0
        check(store, 56, T2)
        assert store.get_source_continuity_review().hold == first.hold
        assert store.get_meta("source_last_discovery_total") == "56"
    with open_state(path) as store:
        check(store, 56, T3)
        review = store.get_source_continuity_review()
        assert review.requires_rebaseline
        assert review.hold == first.hold
        assert review.latest_complete is not None
        assert review.latest_complete.observed_iso == T3


def test_semantic_revision_stable_but_changes_with_accounting_or_content(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.db") as store:
        check(store, 57)
        first = store.get_source_continuity_review().latest_complete
        store.record_source_observation(
            "configured-source", "snapshot", iso=T1, roots_fingerprint="roots", discovery_total=57
        )
        second = store.get_source_continuity_review().latest_complete
        assert first is not None
        assert second is not None
        assert first.revision == second.revision
        assert first.content_sha256 == second.content_sha256
        assert second.observed_iso == T1
        store.record_files(
            [FileSeed(1, 1, 1, "/anime/a.mkv", 1, Cohort.BACKLOG, T0, state=FileState.VERIFYING)]
        )
        store.record_verification(1, VerificationVerdict.PARTIAL, iso=T1)
        check(store, 57, T2)
        third = store.get_source_continuity_review().latest_complete
        assert third is not None
        assert third.landed_count == 1
        assert third.revision != second.revision
        check(store, 58, T3)
        fourth = store.get_source_continuity_review().latest_complete
        assert fourth is not None
        assert fourth.revision != third.revision


def test_incomplete_poll_preserves_first_cause_and_latest_but_marks_stale(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.db") as store:
        check(store, 57)
        check(store, 56, T1)
        first = store.get_source_continuity_review()
        check(store, 1, T2, complete=False)
        review = store.get_source_continuity_review()
        assert review.hold == first.hold
        assert review.latest_complete == first.latest_complete
        assert not review.latest_check_complete
        assert store.get_meta("source_last_discovery_total") == "56"
        assert review.requires_rebaseline


def test_pending_departure_observation_does_not_advance_anchor_or_latch(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.db", budget_limits=LIMITS) as store:
        walk_to_planned(store)
        check(store, 57)
        token = prepare(store)
        store.mark_sent(token, iso=T0)
        verdict = store.check_source_continuity(
            "configured-source",
            "snapshot",
            iso=T1,
            roots_fingerprint="roots",
            discovery_total=56,
            discovered_file_ids=frozenset(),
        )
        assert verdict.reasons == ("discovery_drop_pending_verification",)
        review = store.get_source_continuity_review()
        assert not review.requires_rebaseline
        assert review.hold is None
        assert review.latest_complete is not None
        assert review.latest_complete.observation.discovery_total == 56
        assert store.get_meta("source_last_discovery_total") == "57"


def test_hold_records_partial_and_verified_landed_accounting(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.db") as store:
        check(store, 57)
        for file_id, verdict in (
            (1, VerificationVerdict.PARTIAL),
            (2, VerificationVerdict.VERIFIED),
        ):
            store.record_files(
                [
                    FileSeed(
                        file_id,
                        1,
                        file_id,
                        f"/anime/{file_id}.mkv",
                        1,
                        Cohort.BACKLOG,
                        T0,
                        state=FileState.VERIFYING,
                    )
                ]
            )
            store.record_verification(file_id, verdict, iso=T1)
        check(store, 53, T2, tolerance=1)
        hold = store.get_source_continuity_review().hold
        assert hold is not None
        assert hold.anchor_landed_count == 0
        assert hold.current_landed_count == hold.expected_drop == 2
        assert hold.tolerance == 1
        assert hold.reasons == ("discovery_total_drop",)


@pytest.mark.parametrize("clear_method", ["clear", "record"])
def test_explicit_clear_invalidates_old_hold_identity(tmp_path: Path, clear_method: str) -> None:
    with open_state(tmp_path / "state.db") as store:
        check(store, 57)
        check(store, 56, T1)
        first = store.get_source_continuity_review()
        if clear_method == "clear":
            store.clear_requires_rebaseline(T2)
        else:
            store.record_source_observation(
                "configured-source",
                "snapshot",
                iso=T2,
                roots_fingerprint="roots",
                discovery_total=56,
            )
        assert store.get_source_continuity_review().hold is None
        check(store, 55, T3)
        second = store.get_source_continuity_review()
        assert first.hold is not None
        assert second.hold is not None
        assert first.hold.hold_id != second.hold.hold_id


@pytest.mark.parametrize("method", ["record", "check"])
@pytest.mark.parametrize(
    "invalid_iso",
    ["2026-01-01T00:00:00." + "1" * 80 + "+00:00", "0001-01-01T00:00:00+23:59", "invalid"],
    ids=["long_parseable_fraction", "utc_overflow", "malformed"],
)
def test_invalid_timestamp_cannot_change_anchor_evidence_or_hold(
    tmp_path: Path, method: str, invalid_iso: str
) -> None:
    with open_state(tmp_path / "state.db") as store:
        check(store, 57)
        check(store, 56, T1)
        first = store.get_source_continuity_review()
        before = store.connection.execute("SELECT key, value FROM meta ORDER BY key").fetchall()
        changes = store.connection.total_changes
        observe = (
            store.record_source_observation if method == "record" else store.check_source_continuity
        )
        with pytest.raises(StateError, match="bounded continuity timestamp"):
            observe(
                "configured-source",
                "snapshot",
                iso=invalid_iso,
                roots_fingerprint="roots",
                discovery_total=56,
            )
        assert store.connection.total_changes == changes
        assert (
            store.connection.execute("SELECT key, value FROM meta ORDER BY key").fetchall()
            == before
        )
        assert store.get_source_continuity_review() == first

        store.record_source_observation(
            "configured-source",
            "snapshot",
            iso=T2,
            roots_fingerprint="roots",
            discovery_total=56,
        )
        cleared = store.get_source_continuity_review()
        assert not cleared.requires_rebaseline
        assert cleared.hold is None
        assert cleared.latest_complete is not None
        assert cleared.latest_complete.observed_iso == T2
        check(store, 55, T3)
        recovered = store.get_source_continuity_review()
        assert recovered.evidence_status == "available"
        assert first.hold is not None
        assert recovered.hold is not None
        assert recovered.hold.hold_id != first.hold.hold_id
        assert recovered.hold.previous_anchor_iso == T2


def test_legacy_getter_is_read_only_and_original_evidence_stays_unknown(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.db") as store:
        check(store, 56)
        store.set_meta("requires_rebaseline", "1")
        traces: list[str] = []
        store.connection.set_trace_callback(traces.append)
        first = store.get_source_continuity_review()
        second = store.get_source_continuity_review()
        store.connection.set_trace_callback(None)
        assert first == second
        assert traces
        assert all(sql.startswith("SELECT") for sql in traces)
        assert first.evidence_status == "legacy_unknown"
        assert first.hold is None
        assert store.get_meta(META_SOURCE_CONTINUITY_HOLD) is None
        check(store, 56, T1)
        hold = store.get_source_continuity_review().hold
        assert hold is not None
        assert hold.hold_id
        assert hold.detected_iso is None
        assert hold.prior is None
        assert hold.current is None
        assert hold.previous_anchor_iso is None
        assert hold.reasons == ()
        check(store, 56, T2)
        assert store.get_source_continuity_review().hold == hold


@pytest.mark.parametrize("key", [META_SOURCE_CONTINUITY_HOLD, META_SOURCE_CONTINUITY_LATEST])
@pytest.mark.parametrize(
    "corruption", ["json", "oversize", "nul_oversize", "blob", "boolean", "overflow", "version"]
)
def test_corrupt_metadata_fails_closed_without_repair(
    tmp_path: Path, key: str, corruption: str
) -> None:
    with open_state(tmp_path / "state.db") as store:
        check(store, 57)
        check(store, 56, T1)
        raw = store.get_meta(key)
        assert raw is not None
        document = json.loads(raw)
        if corruption == "json":
            raw = "{"
        elif corruption == "oversize":
            raw = " " * (MAX_CONTINUITY_DOCUMENT_CHARS + 1)
        elif corruption == "nul_oversize":
            raw = "{}\x00" + " " * MAX_CONTINUITY_DOCUMENT_CHARS
        elif corruption == "blob":
            store.connection.execute("UPDATE meta SET value = ? WHERE key = ?", (b"{}", key))
            review = store.get_source_continuity_review()
            assert review.evidence_status == "invalid"
            assert review.requires_rebaseline
            return
        else:
            if corruption == "boolean":
                document[
                    "current_landed_count" if key == META_SOURCE_CONTINUITY_HOLD else "landed_count"
                ] = True
            elif corruption == "overflow":
                document[
                    "detected_iso" if key == META_SOURCE_CONTINUITY_HOLD else "observed_iso"
                ] = "0001-01-01T00:00:00+23:59"
            else:
                document["version"] = True
            raw = json.dumps(document)
        store.set_meta(key, raw)
        review = store.get_source_continuity_review()
        assert review.evidence_status == "invalid"
        assert review.requires_rebaseline
        assert review.hold is None
        assert review.latest_complete is None
        assert not review.latest_check_complete
        assert store.get_meta(key) == raw


def test_flag_and_first_evidence_roll_back_together(tmp_path: Path, monkeypatch: Any) -> None:
    path = tmp_path / "state.db"
    with open_state(path) as store:
        check(store, 57)
        original = store._set_meta_tx

        def fail_flag(key: str, value: str) -> None:
            if key == "requires_rebaseline" and value == "1":
                raise sqlite3.OperationalError("injected disk failure")
            original(key, value)

        monkeypatch.setattr(store, "_set_meta_tx", fail_flag)
        with pytest.raises(WriteDisabledError):
            check(store, 56, T1)
        assert store.get_meta("requires_rebaseline") == "0"
        assert store.get_meta(META_SOURCE_CONTINUITY_HOLD) is None
        assert store.get_meta("source_last_discovery_total") == "57"
    with open_state(path) as store:
        assert store.get_meta(META_SOURCE_CONTINUITY_HOLD) is None
        assert store.get_meta("requires_rebaseline") == "0"


def test_evidence_retains_exact_identity_proof_without_raw_credentials(tmp_path: Path) -> None:
    secret_identity = "sonarr=https://user:secret@example.test/?apikey=hidden;shoko=http://shoko"
    with open_state(tmp_path / "state.db") as store:
        for total, iso in ((57, T0), (56, T1)):
            store.check_source_continuity(
                secret_identity, "snapshot", iso=iso, discovery_total=total
            )
        hold = store.get_source_continuity_review().hold
        assert hold is not None
        assert hold.current is not None
        assert (
            hold.current.instance_identity_sha256
            == hashlib.sha256(secret_identity.encode()).hexdigest()
        )
        for key in (META_SOURCE_CONTINUITY_HOLD, META_SOURCE_CONTINUITY_LATEST):
            raw = store.get_meta(key)
            assert raw is not None
            assert "secret" not in raw
            assert "hidden" not in raw
            assert "example.test" not in raw
            assert len(raw) < MAX_CONTINUITY_DOCUMENT_CHARS
