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
from shoko_linker.state import _CAMPAIGN_V2_DRAIN_COLUMNS, StateError, open_state

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

    with open_state(path) as store:
        assert store.schema_version() == 4
        assert store.get_onboarding_json() is None
        assert store.get_service_pause() == (False, "")
        assert store.get_dashboard_account() is None
        assert store.get_dashboard_session("a" * 64, 1792041600) is None

    after = v1fx.snapshot_state(path)
    for table in ("meta", "baselines", "baseline_files", "files", "sends", "campaigns", "budgets"):
        assert after[table] == before[table]
    assert after["schema_version"][: len(before["schema_version"])] == before["schema_version"]
    assert after["schema_version"][-2][0:3:2] == (3, "migrated from 2")
    assert after["schema_version"][-1][0:3:2] == (4, "migrated from 3")
    conn = sqlite3.connect(path)
    try:
        assert conn.execute("PRAGMA user_version").fetchone() == (4,)
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
