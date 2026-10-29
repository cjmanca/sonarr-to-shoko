"""Durable scope-bound onboarding consent and schema-v2 migration behavior."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import pytest
import state_v1_fixture as v1fx

from shoko_linker.onboarding import (
    OnboardingError,
    OnboardingManager,
    OnboardingStatus,
    ScopeBinding,
)
from shoko_linker.paths import PathMapping
from shoko_linker.safety_check import SafetyAssessment, assess_settings_payload
from shoko_linker.state import (
    _CAMPAIGN_V2_DRAIN_COLUMNS,
    META_ONBOARDING_RECONFIRMATION_REQUIRED,
    SCHEMA_VERSION,
    StateError,
    StateStore,
    WriteDisabledError,
    open_state,
)

SONARR_ROOT = "/data/media/anime"
SHOKO_ROOT = "/anime"
SONARR_URL = "http://sonarr:8989"
SHOKO_URL = "http://shoko:8111"
SONARR_VERSION = "6.0.0.0"
SHOKO_VERSION = "5.3.3"
SETTINGS_SECRET = "persisted-safety-secret-sentinel"
T0 = "2026-10-15T00:00:00+00:00"
T_PLUS_30_DAYS = "2026-11-14T00:00:00+00:00"


def safe_assessment() -> SafetyAssessment:
    return assess_settings_payload(
        {
            "FileQualityFilterEnabled": False,
            "Plugins": {
                "Renamer": {
                    "RenameOnImport": False,
                    "MoveOnImport": False,
                    "RelocateOnImport": False,
                }
            },
            "AniDb": {"MyList_AddFiles": False, "Password": SETTINGS_SECRET},
            "Database": {"Password": SETTINGS_SECRET},
        }
    )


def make_binding(assessment: SafetyAssessment | None = None) -> ScopeBinding:
    safety = safe_assessment() if assessment is None else assessment
    return ScopeBinding(
        sonarr_url=SONARR_URL,
        shoko_url=SHOKO_URL,
        sonarr_version=SONARR_VERSION,
        shoko_version=SHOKO_VERSION,
        capabilities=("sonarr_api_v3_system_status", "shoko_api_v3_init_version"),
        roots=(SONARR_ROOT,),
        path_mappings=(PathMapping(SONARR_ROOT, SHOKO_ROOT),),
        safety_digest=safety.digest,
        unverifiable_facts=safety.unverifiable_facts,
    )


def confirmed_facts(assessment: SafetyAssessment) -> tuple[str, ...]:
    return assessment.unverifiable_facts + assessment.unknown_settings


def _create_v2_database(path: Path) -> dict[str, list[tuple[object, ...]]]:
    """Build an actual populated schema-v2 database from the immutable v1 fixture."""
    v1fx.build_v1_database(path)
    conn = sqlite3.connect(path)
    try:
        for name, definition in _CAMPAIGN_V2_DRAIN_COLUMNS:
            conn.execute(f"ALTER TABLE campaigns ADD COLUMN {name} {definition}")
        conn.execute(
            "INSERT INTO schema_version (version, applied_iso, description)"
            " VALUES (2, ?, 'migrated from 1')",
            (v1fx.T1,),
        )
        conn.execute("PRAGMA user_version=2")
        conn.commit()
    finally:
        conn.close()
    return v1fx.snapshot_state(path)


def test_schema_v2_migrates_in_place_without_rewriting_history(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    before = _create_v2_database(path)
    assert before["campaigns"]
    assert before["campaigns"][0][7] == "approved"

    with open_state(path) as store:
        assert store.schema_version() == SCHEMA_VERSION
        # Prior campaign approval survives, but does not populate normal-service consent.
        assert store.get_onboarding_json() is None
        assert store.get_service_pause() == (False, "")
        assert store.get_dashboard_account() is None
        assert store.get_dashboard_session("a" * 64, 1792041600) is None

    after = v1fx.snapshot_state(path)
    for table in ("meta", "baselines", "baseline_files", "files", "campaigns", "budgets"):
        assert after[table] == before[table]
    legacy_send_fields = len(before["sends"][0])
    assert [row[:legacy_send_fields] for row in after["sends"]] == before["sends"]
    assert [row[legacy_send_fields:] for row in after["sends"]] == [
        (None, "unknown", None, ""),
        (None, "unknown", None, ""),
        (None, "unknown", None, ""),
    ]
    assert after["schema_version"][: len(before["schema_version"])] == before["schema_version"]
    assert [row[0:3:2] for row in after["schema_version"][len(before["schema_version"]) :]] == [
        (3, "migrated from 2"),
        (4, "migrated from 3"),
        (5, "migrated from 4"),
        (6, "migrated from 5"),
        (7, "migrated from 6"),
        (8, "migrated from 7"),
        (9, "migrated from 8"),
    ]
    conn = sqlite3.connect(path)
    try:
        assert conn.execute("PRAGMA user_version").fetchone() == (SCHEMA_VERSION,)
    finally:
        conn.close()


def test_unexpected_onboarding_table_shape_refuses_migration_without_mutating_history(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.sqlite3"
    before = _create_v2_database(path)
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE onboarding (unexpected TEXT)")
        conn.commit()
    finally:
        conn.close()

    with (
        pytest.raises(StateError, match="migration from schema version 2 failed"),
        open_state(path),
    ):
        pass

    conn = sqlite3.connect(path)
    try:
        assert conn.execute("PRAGMA user_version").fetchone() == (2,)
    finally:
        conn.close()
    assert v1fx.snapshot_state(path) == before


def test_setup_consent_requires_all_nonobservable_facts_and_explicit_auto_link_opt_in(
    tmp_path: Path,
) -> None:
    assessment = safe_assessment()
    binding = make_binding(assessment)
    with open_state(tmp_path / "state.sqlite3") as store:
        manager = OnboardingManager(store)
        with pytest.raises(OnboardingError, match="confirm"):
            manager.confirm_setup(
                binding,
                assessment,
                confirmed_facts=(),
                automatic_link_consent=True,
                iso=T0,
            )
        with pytest.raises(OnboardingError, match="automatic linking"):
            manager.confirm_setup(
                binding,
                assessment,
                confirmed_facts=confirmed_facts(assessment),
                automatic_link_consent=False,
                iso=T0,
            )
        assert (
            manager.evaluate(binding, assessment, iso=T0).status is OnboardingStatus.NOT_CONFIGURED
        )


def test_explicit_scope_consent_persists_and_has_no_seven_day_expiry(tmp_path: Path) -> None:
    assessment = safe_assessment()
    binding = make_binding(assessment)
    path = tmp_path / "state.sqlite3"
    with open_state(path) as store:
        manager = OnboardingManager(store)
        manager.confirm_setup(
            binding,
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T0,
        )
        assert (
            manager.evaluate(binding, assessment, iso=T_PLUS_30_DAYS).status
            is OnboardingStatus.AUTHORIZED
        )

    with open_state(path) as reopened:
        manager = OnboardingManager(reopened)
        assert (
            manager.evaluate(binding, assessment, iso=T_PLUS_30_DAYS).status
            is OnboardingStatus.AUTHORIZED
        )
        stored = reopened.get_onboarding_json()
        assert stored is not None
        assert SONARR_URL in stored
        assert SHOKO_URL in stored
        assert "SONARR_API_KEY" not in stored
        assert "SHOKO_API_KEY" not in stored
        assert SETTINGS_SECRET not in stored
        assert json.loads(stored)["automatic_link_consent"] is True


@pytest.mark.parametrize(
    "changed_binding",
    [
        lambda binding: replace(binding, shoko_version="5.3.4"),
        lambda binding: replace(
            binding,
            roots=("/data/media/other-anime",),
            path_mappings=(PathMapping("/data/media/other-anime", "/anime"),),
        ),
        lambda binding: replace(
            binding,
            path_mappings=(PathMapping("/data/media/anime", "/different-shoko-root"),),
        ),
    ],
)
def test_version_scope_or_mapping_change_invalidates_prior_consent(
    tmp_path: Path, changed_binding: Callable[[ScopeBinding], ScopeBinding]
) -> None:
    assessment = safe_assessment()
    binding = make_binding(assessment)
    with open_state(tmp_path / "state.sqlite3") as store:
        manager = OnboardingManager(store)
        manager.confirm_setup(
            binding,
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T0,
        )
        decision = manager.evaluate(changed_binding(binding), assessment, iso=T_PLUS_30_DAYS)

        assert decision.status is OnboardingStatus.NEEDS_CONFIRMATION
        assert store.get_service_pause() == (True, "setup_changed")


def test_unsafe_settings_cannot_be_overridden_by_user_confirmation(tmp_path: Path) -> None:
    assessment = assess_settings_payload({"FileQualityFilterEnabled": True})
    binding = make_binding(assessment)
    with open_state(tmp_path / "state.sqlite3") as store:
        manager = OnboardingManager(store)
        with pytest.raises(OnboardingError, match="unsafe"):
            manager.confirm_setup(
                binding,
                assessment,
                confirmed_facts=confirmed_facts(assessment),
                automatic_link_consent=True,
                iso=T0,
            )
        assert manager.evaluate(binding, assessment, iso=T0).status is OnboardingStatus.UNSAFE


def test_observed_safety_digest_change_invalidates_existing_consent(tmp_path: Path) -> None:
    assessment = safe_assessment()
    binding = make_binding(assessment)
    with open_state(tmp_path / "state.sqlite3") as store:
        manager = OnboardingManager(store)
        manager.confirm_setup(
            binding,
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T0,
        )
        changed_assessment = replace(assessment, digest="f" * 64)

        decision = manager.evaluate(binding, changed_assessment, iso=T_PLUS_30_DAYS)

        assert decision.status is OnboardingStatus.NEEDS_CONFIRMATION
        assert store.get_service_pause() == (True, "setup_changed")


def test_pause_survives_restart_and_resume_rechecks_current_safety(tmp_path: Path) -> None:
    assessment = safe_assessment()
    binding = make_binding(assessment)
    path = tmp_path / "state.sqlite3"
    with open_state(path) as store:
        manager = OnboardingManager(store)
        manager.confirm_setup(
            binding,
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T0,
        )
        manager.pause(iso=T0)

    with open_state(path) as reopened:
        manager = OnboardingManager(reopened)
        assert (
            manager.evaluate(binding, assessment, iso=T_PLUS_30_DAYS).status
            is OnboardingStatus.PAUSED
        )
        assert (
            manager.resume(binding, assessment, iso=T_PLUS_30_DAYS).status
            is OnboardingStatus.AUTHORIZED
        )


def test_user_pause_survives_scope_reconfirmation_until_explicit_resume(tmp_path: Path) -> None:
    assessment = safe_assessment()
    binding = make_binding(assessment)
    changed = replace(
        binding,
        roots=("/data/media/other-anime",),
        path_mappings=(PathMapping("/data/media/other-anime", SHOKO_ROOT),),
    )
    with open_state(tmp_path / "state.sqlite3") as store:
        manager = OnboardingManager(store)
        manager.confirm_setup(
            binding,
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T0,
        )
        manager.pause(iso=T0)

        assert (
            manager.evaluate(changed, assessment, iso=T_PLUS_30_DAYS).status
            is OnboardingStatus.NEEDS_CONFIRMATION
        )
        assert store.get_service_pause() == (True, "user_paused")
        manager.confirm_setup(
            changed,
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T_PLUS_30_DAYS,
        )

        assert (
            manager.evaluate(changed, assessment, iso=T_PLUS_30_DAYS).status
            is OnboardingStatus.PAUSED
        )
        assert (
            manager.resume(changed, assessment, iso=T_PLUS_30_DAYS).status
            is OnboardingStatus.AUTHORIZED
        )


def test_hard_reconfirmation_survives_transient_failures_and_restart(tmp_path: Path) -> None:
    assessment = safe_assessment()
    binding = make_binding(assessment)
    changed = replace(binding, shoko_version="5.3.4")
    path = tmp_path / "state.sqlite3"

    with open_state(path) as store:
        manager = OnboardingManager(store)
        manager.confirm_setup(
            binding,
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T0,
        )
        assert (
            manager.evaluate(changed, assessment, iso=T_PLUS_30_DAYS).status
            is OnboardingStatus.NEEDS_CONFIRMATION
        )
        manager.pause_for_review(
            "setup_unavailable", "Services are temporarily unavailable.", iso=T_PLUS_30_DAYS
        )
        assert store.get_service_pause() == (True, "setup_changed")

    with open_state(path) as reopened:
        manager = OnboardingManager(reopened)
        manager.pause_for_review(
            "setup_unavailable", "Services are still unavailable.", iso=T_PLUS_30_DAYS
        )
        assert reopened.get_service_pause() == (True, "setup_changed")
        assert (
            manager.resume(binding, assessment, iso=T_PLUS_30_DAYS).status
            is OnboardingStatus.NEEDS_CONFIRMATION
        )

        manager.confirm_setup(
            binding,
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T_PLUS_30_DAYS,
        )
        assert reopened.get_service_pause() == (False, "")
        assert manager.evaluate(binding, assessment, iso=T_PLUS_30_DAYS).status is (
            OnboardingStatus.AUTHORIZED
        )


def test_user_pause_cannot_clear_hard_reconfirmation_across_restart(tmp_path: Path) -> None:
    assessment = safe_assessment()
    binding = make_binding(assessment)
    changed = replace(binding, shoko_version="5.3.4")
    path = tmp_path / "state.sqlite3"

    with open_state(path) as store:
        manager = OnboardingManager(store)
        manager.confirm_setup(
            binding,
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T0,
        )
        manager.evaluate(changed, assessment, iso=T_PLUS_30_DAYS)
        manager.pause(iso=T_PLUS_30_DAYS)
        assert store.get_service_pause() == (True, "user_paused")

    with open_state(path) as reopened:
        manager = OnboardingManager(reopened)
        assert (
            manager.resume(binding, assessment, iso=T_PLUS_30_DAYS).status
            is OnboardingStatus.NEEDS_CONFIRMATION
        )
        assert reopened.get_service_pause() == (True, "user_paused")

        manager.confirm_setup(
            binding,
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T_PLUS_30_DAYS,
        )
        assert manager.evaluate(binding, assessment, iso=T_PLUS_30_DAYS).status is (
            OnboardingStatus.PAUSED
        )
        assert manager.resume(binding, assessment, iso=T_PLUS_30_DAYS).status is (
            OnboardingStatus.AUTHORIZED
        )


def test_transient_setup_failure_resumes_with_existing_consent(tmp_path: Path) -> None:
    assessment = safe_assessment()
    binding = make_binding(assessment)
    path = tmp_path / "state.sqlite3"

    with open_state(path) as store:
        manager = OnboardingManager(store)
        manager.confirm_setup(
            binding,
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T0,
        )
        manager.pause_for_review(
            "setup_unavailable", "Services are temporarily unavailable.", iso=T_PLUS_30_DAYS
        )

    with open_state(path) as reopened:
        manager = OnboardingManager(reopened)
        assert manager.resume(binding, assessment, iso=T_PLUS_30_DAYS).status is (
            OnboardingStatus.AUTHORIZED
        )
        assert reopened.get_service_pause() == (False, "")


def test_legacy_hard_pause_is_latched_before_a_temporary_outage_can_replace_it(
    tmp_path: Path,
) -> None:
    assessment = safe_assessment()
    binding = make_binding(assessment)
    path = tmp_path / "state.sqlite3"

    with open_state(path) as store:
        manager = OnboardingManager(store)
        manager.confirm_setup(
            binding,
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T0,
        )
        store.set_service_pause(True, "setup_changed", T_PLUS_30_DAYS)
        assert store.get_meta("onboarding_reconfirmation_required") is None
        manager.pause_for_review(
            "setup_unavailable", "Services are temporarily unavailable.", iso=T_PLUS_30_DAYS
        )

    with open_state(path) as reopened:
        manager = OnboardingManager(reopened)
        manager.pause(iso=T_PLUS_30_DAYS)
        assert (
            manager.resume(binding, assessment, iso=T_PLUS_30_DAYS).status
            is OnboardingStatus.NEEDS_CONFIRMATION
        )


def test_inaccessible_settings_require_confirmation_of_unknown_flags(tmp_path: Path) -> None:
    assessment = assess_settings_payload({})
    binding = make_binding(assessment)
    with open_state(tmp_path / "state.sqlite3") as store:
        manager = OnboardingManager(store)
        with pytest.raises(OnboardingError, match="confirm"):
            manager.confirm_setup(
                binding,
                assessment,
                confirmed_facts=assessment.unverifiable_facts,
                automatic_link_consent=True,
                iso=T0,
            )
        manager.confirm_setup(
            binding,
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T0,
        )
        assert manager.evaluate(binding, assessment, iso=T0).status is OnboardingStatus.AUTHORIZED


def test_sanitized_settings_baseline_is_bound_and_checked_at_confirmation(tmp_path: Path) -> None:
    assessment = safe_assessment()
    binding = replace(
        make_binding(), settings_observation=tuple(assessment.observed_settings.items())
    )
    assert ScopeBinding.from_document(binding.document()) == binding
    assert binding.digest != make_binding().digest
    assert SETTINGS_SECRET not in json.dumps(binding.document())
    assert "settings_observation" not in make_binding().document()
    assert ScopeBinding.from_document(make_binding().document()) == make_binding()
    mismatched = replace(
        binding,
        settings_observation=tuple(
            (name, True if name == "RenameOnImport" else value)
            for name, value in assessment.observed_settings.items()
        ),
    )
    with open_state(tmp_path / "state.sqlite3") as store:
        with pytest.raises(OnboardingError, match="observation does not match"):
            OnboardingManager(store).confirm_setup(
                mismatched,
                assessment,
                confirmed_facts=confirmed_facts(assessment),
                automatic_link_consent=True,
                iso=T0,
            )
        assert store.get_onboarding_json() is None


@pytest.mark.parametrize("invalid", [{}, {"DatabasePassword": "secret"}, None])
def test_invalid_settings_baseline_is_rejected(invalid: object) -> None:
    document = make_binding().document()
    document["settings_observation"] = invalid
    with pytest.raises(OnboardingError, match="observation is malformed"):
        ScopeBinding.from_document(document)


def test_selected_folder_observation_is_digest_bound_and_legacy_compatible() -> None:
    legacy = make_binding()
    assert "selected_folders_observation" not in legacy.document()
    binding = replace(legacy, selected_folders_observation=((1, SHOKO_ROOT, "None"),))
    assert ScopeBinding.from_document(binding.document()) == binding
    assert binding.digest != legacy.digest
    assert ScopeBinding.from_document(legacy.document()).digest == legacy.digest


@pytest.mark.parametrize(
    "invalid",
    [
        None,
        [],
        [[True, SHOKO_ROOT, "None"]],
        [[1, "relative", "None"]],
        [[1, SHOKO_ROOT, "Unknown"]],
        [[1, SHOKO_ROOT, "Source"]],
        [[1, SHOKO_ROOT, "None"], [1, "/other", "None"]],
    ],
)
def test_invalid_selected_folder_observation_is_rejected(invalid: object) -> None:
    document = make_binding().document()
    document["selected_folders_observation"] = invalid
    with pytest.raises(OnboardingError, match="folder observation"):
        ScopeBinding.from_document(document)


@pytest.mark.parametrize("interleave", ["confirmation", "pause"])
def test_observed_review_atomically_checks_consent_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, interleave: str
) -> None:
    state_path = tmp_path / "state.sqlite3"
    assessment = safe_assessment()
    with open_state(state_path) as store:
        manager = OnboardingManager(store)
        record = manager.confirm_setup(
            make_binding(),
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T0,
        )
        original_transition = store.pause_onboarding_if_current

        def interleaved_transition(expected_document_json: str, reason: str, iso: str) -> bool:
            # Race after manager's identity read, before persistence obtains its writer lock.
            with open_state(state_path) as other:
                other_manager = OnboardingManager(other)
                if interleave == "confirmation":
                    other_manager.confirm_setup(
                        record.binding,
                        assessment,
                        confirmed_facts=record.confirmed_facts,
                        automatic_link_consent=True,
                        iso=T_PLUS_30_DAYS,
                    )
                else:
                    other_manager.pause(iso=T_PLUS_30_DAYS)
            return original_transition(expected_document_json, reason, iso)

        monkeypatch.setattr(store, "pause_onboarding_if_current", interleaved_transition)
        applied = manager.observe_review(record, "setup_changed", iso=T_PLUS_30_DAYS)
        assert applied is (interleave == "pause")
        assert manager.reconfirmation_required() is (interleave == "pause")
        assert store.get_service_pause() == (
            (True, "user_paused") if interleave == "pause" else (False, "")
        )


@pytest.mark.parametrize("interleave", ["review", "confirmation", "pause"])
def test_confirmation_does_not_erase_review_of_new_consent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, interleave: str
) -> None:
    state_path = tmp_path / "state.sqlite3"
    assessment = safe_assessment()
    with open_state(state_path) as store:
        manager = OnboardingManager(store)
        original_commit = store._commit_tx
        observed = False

        def observe_after_commit() -> None:
            nonlocal observed
            original_commit()
            if observed:
                return
            observed = True
            # Another process can observe the new consent as soon as it commits.
            with open_state(state_path) as other:
                observer = OnboardingManager(other)
                if interleave == "confirmation":
                    observer.confirm_setup(
                        make_binding(),
                        assessment,
                        confirmed_facts=confirmed_facts(assessment),
                        automatic_link_consent=True,
                        iso=T_PLUS_30_DAYS,
                    )
                elif interleave == "pause":
                    observer.pause(iso=T_PLUS_30_DAYS)
                current = observer.record()
                assert current is not None
                assert observer.observe_review(current, "setup_changed", iso=T_PLUS_30_DAYS)

        monkeypatch.setattr(store, "_commit_tx", observe_after_commit)
        manager.confirm_setup(
            make_binding(),
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T0,
        )
        assert observed
        assert manager.reconfirmation_required()
        assert store.get_service_pause() == (
            (True, "user_paused") if interleave == "pause" else (True, "setup_changed")
        )
        record = manager.record()
        assert record is not None
        assert record.consented_iso == (T_PLUS_30_DAYS if interleave == "confirmation" else T0)
    with open_state(state_path) as reopened:
        assert (
            OnboardingManager(reopened)
            .evaluate(make_binding(), assessment, iso=T_PLUS_30_DAYS)
            .status
            is OnboardingStatus.NEEDS_CONFIRMATION
        )


@pytest.mark.parametrize(
    "old_reason", ["setup_changed", "unsafe_shoko_settings", "setup_unavailable", "user_paused"]
)
def test_confirmation_publishes_consent_and_releases_only_system_pause(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, old_reason: str
) -> None:
    state_path = tmp_path / "state.sqlite3"
    assessment = safe_assessment()
    with open_state(state_path) as store:
        manager = OnboardingManager(store)
        store.set_meta(META_ONBOARDING_RECONFIRMATION_REQUIRED, "1")
        store.set_service_pause(True, "setup_changed", T0)
        original_begin = store._begin_immediate_tx

        def pause_before_lock(busy_context: str | None) -> None:
            # A concurrent user action commits after confirmation validation but
            # before this connection obtains the writer lock.
            with open_state(state_path) as other:
                other.set_service_pause(True, old_reason, T_PLUS_30_DAYS)
            original_begin(busy_context)

        monkeypatch.setattr(store, "_begin_immediate_tx", pause_before_lock)
        manager.confirm_setup(
            make_binding(),
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T_PLUS_30_DAYS,
        )
        assert not manager.reconfirmation_required()
        assert store.get_service_pause() == (
            (True, "user_paused") if old_reason == "user_paused" else (False, "")
        )


def test_failed_confirmation_commit_rolls_back_consent_and_hold_release(tmp_path: Path) -> None:
    state_path = tmp_path / "state.sqlite3"
    assessment = safe_assessment()
    with open_state(state_path) as store:
        manager = OnboardingManager(store)
        previous = manager.confirm_setup(
            make_binding(),
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T0,
        )
        manager.pause_for_review("setup_changed", "Review setup", iso=T0)
        denied = False

        def deny_first_commit(
            action: int,
            arg1: str | None,
            _arg2: str | None,
            _database: str | None,
            _source: str | None,
        ) -> int:
            nonlocal denied
            if action == sqlite3.SQLITE_TRANSACTION and arg1 == "COMMIT" and not denied:
                denied = True
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        store.connection.set_authorizer(deny_first_commit)
        with pytest.raises(WriteDisabledError, match="state writes stopped"):
            manager.confirm_setup(
                make_binding(),
                assessment,
                confirmed_facts=confirmed_facts(assessment),
                automatic_link_consent=True,
                iso=T_PLUS_30_DAYS,
            )
        assert denied
        assert manager.record() == previous
        assert manager.reconfirmation_required()
        assert store.get_service_pause() == (True, "setup_changed")
        assert store.writes_disabled
    with open_state(state_path) as reopened:
        assert OnboardingManager(reopened).record() == previous
        assert OnboardingManager(reopened).reconfirmation_required()
        assert reopened.get_service_pause() == (True, "setup_changed")
        assert reopened.writes_disabled


@pytest.mark.parametrize("reason", ["setup_changed", "unsafe_shoko_settings", "setup_unavailable"])
@pytest.mark.parametrize("timing", ["before_writer_lock", "after_commit"])
def test_automatic_review_preserves_concurrent_user_pause_through_confirmation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reason: str, timing: str
) -> None:
    state_path = tmp_path / "state.sqlite3"
    assessment = safe_assessment()
    binding = make_binding()
    with open_state(state_path) as store:
        manager = OnboardingManager(store)
        manager.confirm_setup(
            binding,
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T0,
        )
        original_begin = store._begin_immediate_tx
        original_commit = store._commit_tx
        injected = False

        def user_pause() -> None:
            nonlocal injected
            if injected:
                return
            injected = True
            with open_state(state_path) as other:
                OnboardingManager(other).pause(iso=T_PLUS_30_DAYS)

        def pause_before_lock(busy_context: str | None) -> None:
            user_pause()
            original_begin(busy_context)

        def pause_after_commit() -> None:
            original_commit()
            user_pause()

        with monkeypatch.context() as patch:
            if timing == "before_writer_lock":
                patch.setattr(store, "_begin_immediate_tx", pause_before_lock)
            else:
                patch.setattr(store, "_commit_tx", pause_after_commit)
            manager.pause_for_review(reason, "Review setup", iso=T_PLUS_30_DAYS)
        assert injected
        assert store.get_service_pause() == (True, "user_paused")
        assert manager.reconfirmation_required() is (reason != "setup_unavailable")
    with open_state(state_path) as reopened:
        manager = OnboardingManager(reopened)
        assert reopened.get_service_pause() == (True, "user_paused")
        assert manager.reconfirmation_required() is (reason != "setup_unavailable")
        manager.confirm_setup(
            binding,
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T_PLUS_30_DAYS,
        )
        assert not manager.reconfirmation_required()
        assert reopened.get_service_pause() == (True, "user_paused")
        assert manager.evaluate(binding, assessment, iso=T_PLUS_30_DAYS).status is (
            OnboardingStatus.PAUSED
        )


def test_failed_automatic_review_commit_rolls_back_hold_and_stops_writes(tmp_path: Path) -> None:
    state_path = tmp_path / "state.sqlite3"
    assessment = safe_assessment()
    with open_state(state_path) as store:
        manager = OnboardingManager(store)
        manager.confirm_setup(
            make_binding(),
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T0,
        )
        denied = False

        def deny_first_commit(
            action: int,
            arg1: str | None,
            _arg2: str | None,
            _database: str | None,
            _source: str | None,
        ) -> int:
            nonlocal denied
            if action == sqlite3.SQLITE_TRANSACTION and arg1 == "COMMIT" and not denied:
                denied = True
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        store.connection.set_authorizer(deny_first_commit)
        with pytest.raises(WriteDisabledError, match="state writes stopped"):
            manager.pause_for_review("setup_changed", "Review setup", iso=T_PLUS_30_DAYS)
        assert denied
        assert store.writes_disabled
        assert store.get_service_pause() == (False, "")
        assert not manager.reconfirmation_required()
    with open_state(state_path) as reopened:
        assert reopened.writes_disabled
        assert reopened.get_service_pause() == (False, "")
        assert not OnboardingManager(reopened).reconfirmation_required()


@pytest.mark.parametrize("timing", ["before_writer_lock", "after_commit"])
def test_user_pause_preserves_fresh_confirmation_without_resurrecting_old_hold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, timing: str
) -> None:
    state_path = tmp_path / "state.sqlite3"
    assessment = safe_assessment()
    binding = make_binding()
    with open_state(state_path) as store:
        manager = OnboardingManager(store)
        manager.confirm_setup(
            binding,
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T0,
        )
        manager.pause_for_review("setup_changed", "Review setup", iso=T0)
        original_begin = store._begin_immediate_tx
        original_commit = store._commit_tx
        injected = False

        def confirm() -> None:
            nonlocal injected
            if injected:
                return
            injected = True
            with open_state(state_path) as other:
                OnboardingManager(other).confirm_setup(
                    binding,
                    assessment,
                    confirmed_facts=confirmed_facts(assessment),
                    automatic_link_consent=True,
                    iso=T_PLUS_30_DAYS,
                )

        def confirm_before_lock(busy_context: str | None) -> None:
            confirm()
            original_begin(busy_context)

        def confirm_after_commit() -> None:
            original_commit()
            confirm()

        with monkeypatch.context() as patch:
            if timing == "before_writer_lock":
                patch.setattr(store, "_begin_immediate_tx", confirm_before_lock)
            else:
                patch.setattr(store, "_commit_tx", confirm_after_commit)
            assert manager.pause(iso=T_PLUS_30_DAYS).status is OnboardingStatus.PAUSED
        assert injected
        assert not manager.reconfirmation_required()
        assert store.get_service_pause() == (True, "user_paused")
    with open_state(state_path) as reopened:
        manager = OnboardingManager(reopened)
        assert not manager.reconfirmation_required()
        assert manager.evaluate(binding, assessment, iso=T_PLUS_30_DAYS).status is (
            OnboardingStatus.PAUSED
        )
        assert manager.resume(binding, assessment, iso=T_PLUS_30_DAYS).status is (
            OnboardingStatus.AUTHORIZED
        )


@pytest.mark.parametrize("timing", ["before_writer_lock", "after_commit"])
def test_user_pause_preserves_genuinely_new_observed_hold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, timing: str
) -> None:
    state_path = tmp_path / "state.sqlite3"
    assessment = safe_assessment()
    with open_state(state_path) as store:
        manager = OnboardingManager(store)
        record = manager.confirm_setup(
            make_binding(),
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T0,
        )
        original_begin = store._begin_immediate_tx
        original_commit = store._commit_tx
        injected = False

        def observe() -> None:
            nonlocal injected
            if injected:
                return
            injected = True
            with open_state(state_path) as other:
                assert OnboardingManager(other).observe_review(
                    record, "setup_changed", iso=T_PLUS_30_DAYS
                )

        def observe_before_lock(busy_context: str | None) -> None:
            observe()
            original_begin(busy_context)

        def observe_after_commit() -> None:
            original_commit()
            observe()

        with monkeypatch.context() as patch:
            if timing == "before_writer_lock":
                patch.setattr(store, "_begin_immediate_tx", observe_before_lock)
            else:
                patch.setattr(store, "_commit_tx", observe_after_commit)
            manager.pause(iso=T_PLUS_30_DAYS)
        assert injected
        assert manager.reconfirmation_required()
        assert store.get_service_pause() == (True, "user_paused")
    with open_state(state_path) as reopened:
        assert (
            OnboardingManager(reopened)
            .resume(make_binding(), assessment, iso=T_PLUS_30_DAYS)
            .status
            is OnboardingStatus.NEEDS_CONFIRMATION
        )
        assert reopened.get_service_pause() == (True, "user_paused")


@pytest.mark.parametrize("reason", ["setup_changed", "unsafe_shoko_settings"])
def test_user_pause_migrates_current_legacy_hard_hold(tmp_path: Path, reason: str) -> None:
    state_path = tmp_path / "state.sqlite3"
    assessment = safe_assessment()
    with open_state(state_path) as store:
        manager = OnboardingManager(store)
        manager.confirm_setup(
            make_binding(),
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T0,
        )
        store.set_service_pause(True, reason, T0)
        assert store.get_meta(META_ONBOARDING_RECONFIRMATION_REQUIRED) is None
        manager.pause(iso=T_PLUS_30_DAYS)
        assert store.get_meta(META_ONBOARDING_RECONFIRMATION_REQUIRED) == "1"
        assert store.get_service_pause() == (True, "user_paused")
    with open_state(state_path) as reopened:
        assert (
            OnboardingManager(reopened)
            .resume(make_binding(), assessment, iso=T_PLUS_30_DAYS)
            .status
            is OnboardingStatus.NEEDS_CONFIRMATION
        )
        assert reopened.get_service_pause() == (True, "user_paused")


@pytest.mark.parametrize("reason", ["setup_changed", "unsafe_shoko_settings"])
def test_failed_user_pause_commit_rolls_back_legacy_hold_migration(
    tmp_path: Path, reason: str
) -> None:
    state_path = tmp_path / "state.sqlite3"
    with open_state(state_path) as store:
        manager = OnboardingManager(store)
        store.set_service_pause(True, reason, T0)
        assert store.get_meta(META_ONBOARDING_RECONFIRMATION_REQUIRED) is None
        denied = False

        def deny_first_commit(
            action: int,
            arg1: str | None,
            _arg2: str | None,
            _database: str | None,
            _source: str | None,
        ) -> int:
            nonlocal denied
            if action == sqlite3.SQLITE_TRANSACTION and arg1 == "COMMIT" and not denied:
                denied = True
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        store.connection.set_authorizer(deny_first_commit)
        with pytest.raises(WriteDisabledError, match="state writes stopped"):
            manager.pause(iso=T_PLUS_30_DAYS)
        assert denied
        assert store.writes_disabled
        assert store.get_meta(META_ONBOARDING_RECONFIRMATION_REQUIRED) is None
        assert store.get_service_pause() == (True, reason)
    with open_state(state_path) as reopened:
        assert reopened.writes_disabled
        assert reopened.get_meta(META_ONBOARDING_RECONFIRMATION_REQUIRED) is None
        assert reopened.get_service_pause() == (True, reason)


@pytest.mark.parametrize("change", ["observed_hold", "legacy_hold", "new_consent"])
def test_resume_refuses_concurrent_hold_or_consent_replacement(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    state_path = tmp_path / "state.sqlite3"
    assessment = safe_assessment()
    binding = make_binding()
    with open_state(state_path) as store:
        manager = OnboardingManager(store)
        record = manager.confirm_setup(
            binding,
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T0,
        )
        manager.pause(iso=T0)
        original_begin = store._begin_immediate_tx
        injected = False

        def change_before_lock(busy_context: str | None) -> None:
            nonlocal injected
            if not injected:
                injected = True
                with open_state(state_path) as other:
                    observer = OnboardingManager(other)
                    if change == "observed_hold":
                        assert observer.observe_review(record, "setup_changed", iso=T_PLUS_30_DAYS)
                    elif change == "legacy_hold":
                        other.set_service_pause(True, "unsafe_shoko_settings", T_PLUS_30_DAYS)
                    else:
                        observer.confirm_setup(
                            binding,
                            assessment,
                            confirmed_facts=record.confirmed_facts,
                            automatic_link_consent=True,
                            iso=T_PLUS_30_DAYS,
                        )
            original_begin(busy_context)

        with monkeypatch.context() as patch:
            patch.setattr(store, "_begin_immediate_tx", change_before_lock)
            decision = manager.resume(binding, assessment, iso=T_PLUS_30_DAYS)
        assert injected
        assert decision.status is OnboardingStatus.NEEDS_CONFIRMATION
        assert manager.reconfirmation_required() is (change != "new_consent")
        expected_pause = (
            (True, "unsafe_shoko_settings") if change == "legacy_hold" else (True, "user_paused")
        )
        assert store.get_service_pause() == expected_pause
    with open_state(state_path) as reopened:
        manager = OnboardingManager(reopened)
        assert reopened.get_service_pause() == expected_pause
        assert manager.evaluate(binding, assessment, iso=T_PLUS_30_DAYS).status is (
            OnboardingStatus.PAUSED
            if change == "new_consent"
            else OnboardingStatus.NEEDS_CONFIRMATION
        )
        if change == "new_consent":
            # Current valid consent permits a fresh explicit Resume, without reconfirmation.
            assert manager.resume(binding, assessment, iso=T_PLUS_30_DAYS).status is (
                OnboardingStatus.AUTHORIZED
            )


def test_failed_resume_commit_preserves_pause_and_stops_writes(tmp_path: Path) -> None:
    state_path = tmp_path / "state.sqlite3"
    assessment = safe_assessment()
    with open_state(state_path) as store:
        manager = OnboardingManager(store)
        record = manager.confirm_setup(
            make_binding(),
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T0,
        )
        manager.pause(iso=T0)
        denied = False

        def deny_first_commit(
            action: int,
            arg1: str | None,
            _arg2: str | None,
            _database: str | None,
            _source: str | None,
        ) -> int:
            nonlocal denied
            if action == sqlite3.SQLITE_TRANSACTION and arg1 == "COMMIT" and not denied:
                denied = True
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        store.connection.set_authorizer(deny_first_commit)
        with pytest.raises(WriteDisabledError, match="state writes stopped"):
            manager.resume(make_binding(), assessment, iso=T_PLUS_30_DAYS)
        assert denied
        assert store.writes_disabled
        assert store.get_service_pause() == (True, "user_paused")
        assert manager.record() == record
        assert not manager.reconfirmation_required()
    with open_state(state_path) as reopened:
        assert reopened.writes_disabled
        assert reopened.get_service_pause() == (True, "user_paused")
        assert OnboardingManager(reopened).record() == record


@pytest.mark.parametrize("timing", ["after_record_read", "before_writer_lock"])
@pytest.mark.parametrize("reconfirm", [False, True])
@pytest.mark.parametrize("unsafe", [False, True])
def test_evaluate_mismatch_holds_only_the_consent_it_observed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, timing: str, reconfirm: bool, unsafe: bool
) -> None:
    reason = "unsafe_shoko_settings" if unsafe else "setup_changed"
    state_path = tmp_path / "state.sqlite3"
    assessment = safe_assessment()
    original_binding = make_binding()
    current_binding = replace(original_binding, shoko_version="5.3.3.0")
    with open_state(state_path) as store:
        manager = OnboardingManager(store)
        original_record = manager.confirm_setup(
            original_binding,
            assessment,
            confirmed_facts=confirmed_facts(assessment),
            automatic_link_consent=True,
            iso=T0,
        )
        original_read = store.get_onboarding_json
        original_begin = store._begin_immediate_tx
        injected = False

        def maybe_confirm() -> None:
            nonlocal injected
            if injected:
                return
            injected = True
            if reconfirm:
                with open_state(state_path) as other:
                    OnboardingManager(other).confirm_setup(
                        current_binding,
                        assessment,
                        confirmed_facts=original_record.confirmed_facts,
                        automatic_link_consent=True,
                        iso=T_PLUS_30_DAYS,
                    )

        def confirm_after_read() -> str | None:
            record = original_read()
            maybe_confirm()
            return record

        def confirm_before_lock(busy_context: str | None) -> None:
            maybe_confirm()
            original_begin(busy_context)

        with monkeypatch.context() as patch:
            if timing == "after_record_read":
                patch.setattr(store, "get_onboarding_json", confirm_after_read)
            else:
                patch.setattr(store, "_begin_immediate_tx", confirm_before_lock)
            observed = (
                assess_settings_payload({"FileQualityFilterEnabled": True})
                if unsafe
                else assessment
            )
            decision = manager.evaluate(current_binding, observed, iso=T_PLUS_30_DAYS)
        assert injected
        # Stale checks always refuse; only a fresh call may authorize the new consent.
        assert decision.status is (
            OnboardingStatus.UNSAFE
            if unsafe and not reconfirm
            else OnboardingStatus.NEEDS_CONFIRMATION
        )
        assert manager.reconfirmation_required() is not reconfirm
        assert store.get_service_pause() == ((False, "") if reconfirm else (True, reason))
    with open_state(state_path) as reopened:
        manager = OnboardingManager(reopened)
        assert manager.reconfirmation_required() is not reconfirm
        if reconfirm:
            assert manager.record() is not None
            assert manager.resume(current_binding, assessment, iso=T_PLUS_30_DAYS).status is (
                OnboardingStatus.AUTHORIZED
            )
            assert reopened.get_service_pause() == (False, "")
        else:
            # Returning to the original observable facts cannot clear an observed hard hold.
            assert manager.resume(original_binding, assessment, iso=T_PLUS_30_DAYS).status is (
                OnboardingStatus.NEEDS_CONFIRMATION
            )
            assert reopened.get_service_pause() == (True, reason)


@pytest.mark.parametrize("channel", ["environment", "folders", "evaluation"])
def test_observation_generation_fences_aba_and_stale_safe_reads(
    tmp_path: Path, channel: str
) -> None:
    path = tmp_path / "observations.sqlite3"
    with open_state(path) as store, open_state(path) as other:
        manager = OnboardingManager(store)
        record = manager.confirm_setup(
            make_binding(),
            safe_assessment(),
            confirmed_facts=confirmed_facts(safe_assessment()),
            automatic_link_consent=True,
            iso=T0,
        )
        encoded = record.to_json()
        initial = store.get_service_authorization_revision()

        def observe(handle: StateStore, revision: str | None, hazard: bool) -> bool:
            return handle.observe_onboarding_facts(
                encoded,
                expected_authorization_revision=revision,
                channel=channel,
                fingerprint=("a" if hazard else "b") * 64,
                reason="unsafe_shoko_settings" if hazard else None,
                iso=T0,
            )

        assert observe(other, initial, True)
        first = store.get_service_authorization_revision()
        assert first != initial
        assert observe(other, first, True)
        assert store.get_service_authorization_revision() == first
        # A safe request started before this hazard cannot erase its identity.
        assert not observe(store, initial, False)
        assert observe(other, first, True)
        assert store.get_service_authorization_revision() == first
        assert observe(store, first, False)
        assert observe(other, first, True)
        second = store.get_service_authorization_revision()
        assert second not in {initial, first}
        assert not observe(store, first, False)
        assert manager.reconfirmation_required()
        manager.confirm_setup(
            make_binding(),
            safe_assessment(),
            confirmed_facts=record.confirmed_facts,
            automatic_link_consent=True,
            iso=T_PLUS_30_DAYS,
        )
        assert not observe(other, second, True)
        assert not manager.reconfirmation_required()


def test_observed_hard_hold_revision_and_fact_identity_roll_back_atomically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "rollback.sqlite3"
    with open_state(path) as store:
        manager = OnboardingManager(store)
        record = manager.confirm_setup(
            make_binding(),
            safe_assessment(),
            confirmed_facts=confirmed_facts(safe_assessment()),
            automatic_link_consent=True,
            iso=T0,
        )
        revision = store.get_service_authorization_revision()
        original = store._set_meta_tx

        def fail(key: str, value: str) -> None:
            if key == META_ONBOARDING_RECONFIRMATION_REQUIRED:
                raise StateError("injected review failure")
            original(key, value)

        with monkeypatch.context() as patch:
            patch.setattr(store, "_set_meta_tx", fail)
            with pytest.raises(StateError, match="injected"):
                store.observe_onboarding_facts(
                    record.to_json(),
                    expected_authorization_revision=revision,
                    channel="environment",
                    fingerprint="a" * 64,
                    reason="unsafe_shoko_settings",
                    iso=T0,
                )
        assert store.get_service_authorization_revision() == revision
        assert store.get_meta("onboarding_observation_v1:environment") is None
        assert not manager.reconfirmation_required()
        assert store.get_service_pause() == (False, "")
