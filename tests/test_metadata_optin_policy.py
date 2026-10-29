"""Normal-service metadata maintenance is opt-in, revocable and separately consented."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import test_service_settings as preferences_tests
import test_service_setup as setup_tests

from shoko_linker.onboarding import (
    METADATA_MAINTENANCE_CONFIRMATION,
    METADATA_MAINTENANCE_POLICY,
    OnboardingManager,
    OnboardingStatus,
    ScopeBinding,
)
from shoko_linker.safety_check import SafetyAssessment
from shoko_linker.service_settings import (
    SETTINGS_META,
    ServiceSettingsController,
    load_service_settings,
)
from shoko_linker.service_setup import ServiceSetupController
from shoko_linker.setup import AnimeScopeDiscovery, ApiCompatibility
from shoko_linker.state import StateStore, open_state


def test_legacy_preferences_and_binding_keep_off_digest(tmp_path: Path) -> None:
    preferences = preferences_tests.controller(tmp_path)
    document = preferences.document()
    assert document.pop("allow_missing_metadata_refresh") is False
    with open_state(preferences.bootstrap.database_path) as store:
        store.set_meta(SETTINGS_META, json.dumps(document))
        assert (
            load_service_settings(store, preferences.bootstrap)["allow_missing_metadata_refresh"]
            is False
        )
    import test_onboarding as onboarding_tests

    binding = onboarding_tests.make_binding()
    encoded = binding.document()
    assert "metadata_maintenance_policy" not in encoded
    assert ScopeBinding.from_document(encoded).digest == binding.digest


@pytest.mark.parametrize("invalid", [1, "true", None, [], {}])
def test_saved_preference_is_strict_boolean(tmp_path: Path, invalid: object) -> None:
    preferences = preferences_tests.controller(tmp_path)
    document = preferences.document()
    document["allow_missing_metadata_refresh"] = invalid
    with open_state(preferences.bootstrap.database_path) as store:
        store.set_meta(SETTINGS_META, json.dumps(document))
    from shoko_linker.errors import ConfigError

    with pytest.raises(ConfigError, match="boolean"):
        preferences.document()


def test_enabled_requires_new_ack_and_revocation_blocks_old_context(  # noqa: PLR0915 - complete consent lifecycle
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup, form, _, _, _, sonarr, shoko = setup_tests._ready_setup_form(tmp_path, monkeypatch)
    preferences = ServiceSettingsController(setup.bootstrap, lambda: None)
    try:
        assert "Setup saved" in setup.action("confirm", form)
        with open_state(setup.state_path) as store:
            original = OnboardingManager(store).record()
            assert original is not None
            assert original.binding.metadata_maintenance_policy is None
            context = setup.current_context(store)
            assert context is not None
            assert (
                OnboardingManager(store)
                .evaluate_context(context, iso="2026-10-29T00:00:00+00:00")
                .status
                is OnboardingStatus.AUTHORIZED
            )
        enabled = preferences_tests.settings_form(preferences)
        enabled["allow_missing_metadata_refresh"] = "1"
        assert "Confirm setup again" in preferences.save(enabled)
        assert preferences.document()["allow_missing_metadata_refresh"] is True
        assert "Setup was not saved" in setup.action("confirm", form)
        view = setup.refresh_view()
        assert view is not None
        assert METADATA_MAINTENANCE_CONFIRMATION in view.required_confirmations
        fresh = setup_tests._consent_form(view)
        index = view.required_confirmations.index(METADATA_MAINTENANCE_CONFIRMATION)
        without_ack = dict(fresh)
        del without_ack[f"confirm_{index}"]
        assert "confirm every" in setup.action("confirm", without_ack)
        view = setup.refresh_view()
        assert view is not None
        assert "Setup saved" in setup.action("confirm", setup_tests._consent_form(view))
        with open_state(setup.state_path) as store:
            record = OnboardingManager(store).record()
            assert record is not None
            assert record.binding.metadata_maintenance_policy == METADATA_MAINTENANCE_POLICY
            enabled_context = setup.current_context(store)
            assert enabled_context is not None
            assert (
                OnboardingManager(store)
                .evaluate_context(enabled_context, iso="2026-10-29T00:00:00+00:00")
                .status
                is OnboardingStatus.AUTHORIZED
            )
        off = preferences_tests.settings_form(preferences)
        assert "Confirm setup again" in preferences.save(off)
        with open_state(setup.state_path) as store:
            assert OnboardingManager(store).reconfirmation_required()
            assert (
                OnboardingManager(store)
                .evaluate_context(enabled_context, iso="2026-10-29T00:00:01+00:00")
                .status
                is not OnboardingStatus.AUTHORIZED
            )
        assert (
            ServiceSettingsController(setup.bootstrap, lambda: None).document()[
                "allow_missing_metadata_refresh"
            ]
            is False
        )
        view = setup.refresh_view()
        assert view is not None
        assert METADATA_MAINTENANCE_CONFIRMATION not in view.required_confirmations
        assert "Setup saved" in setup.action("confirm", setup_tests._consent_form(view))
        with open_state(setup.state_path) as store:
            record = OnboardingManager(store).record()
            assert record is not None
            assert record.binding.digest == original.binding.digest
    finally:
        sonarr.client.close()
        shoko.client.close()


def test_option_change_during_confirm_cannot_clear_new_hold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup, form, _, _, _, sonarr, shoko = setup_tests._ready_setup_form(tmp_path, monkeypatch)
    preferences = ServiceSettingsController(setup.bootstrap, lambda: None)
    original = StateStore.confirm_onboarding_json
    changed = False

    def confirm(
        store: StateStore,
        document_json: str,
        iso: str,
        *,
        expected_settings_json: str | None = None,
        check_settings: bool = False,
        expected_authorization_revision: str | None = None,
        expected_draft_json: str | None = None,
    ) -> None:
        nonlocal changed
        if not changed:
            changed = True
            enabled = preferences_tests.settings_form(preferences)
            enabled["allow_missing_metadata_refresh"] = "1"
            assert "Settings saved" in preferences.save(enabled)
        original(
            store,
            document_json,
            iso,
            expected_settings_json=expected_settings_json,
            check_settings=check_settings,
            expected_authorization_revision=expected_authorization_revision,
            expected_draft_json=expected_draft_json,
        )

    monkeypatch.setattr(StateStore, "confirm_onboarding_json", confirm)
    try:
        assert "settings changed" in setup.action("confirm", form)
        with open_state(setup.state_path) as store:
            assert store.get_onboarding_json() is None
            assert OnboardingManager(store).reconfirmation_required()
            assert preferences.document()["allow_missing_metadata_refresh"] is True
    finally:
        sonarr.client.close()
        shoko.client.close()


def test_opt_out_during_context_collection_refuses_stale_permission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup, _, _, _, _, sonarr, shoko = setup_tests._ready_setup_form(tmp_path, monkeypatch)
    preferences = ServiceSettingsController(setup.bootstrap, lambda: None)
    enabled = preferences_tests.settings_form(preferences)
    enabled["allow_missing_metadata_refresh"] = "1"
    assert "Settings saved" in preferences.save(enabled)
    try:
        view = setup.refresh_view()
        assert view is not None
        assert "Setup saved" in setup.action("confirm", setup_tests._consent_form(view))
        environment = setup._environment
        changed = False

        def collect() -> (
            tuple[
                ApiCompatibility,
                AnimeScopeDiscovery,
                SafetyAssessment,
            ]
        ):
            nonlocal changed
            if not changed:
                changed = True
                assert "Settings saved" in preferences.save(
                    preferences_tests.settings_form(preferences)
                )
            return environment()

        monkeypatch.setattr(setup, "_environment", collect)
        with open_state(setup.state_path) as store:
            assert setup.current_context(store) is None
            assert OnboardingManager(store).reconfirmation_required()
        assert preferences.document()["allow_missing_metadata_refresh"] is False
    finally:
        sonarr.client.close()
        shoko.client.close()


@pytest.mark.parametrize("initially_enabled", [False, True])
def test_option_roundtrip_invalidates_old_form_even_with_identical_timestamp(  # noqa: PLR0915 - durable generation checks
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, initially_enabled: bool
) -> None:
    from datetime import UTC, datetime, tzinfo

    from shoko_linker import service_settings

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> FrozenDateTime:
            return cls(2026, 10, 29, tzinfo=UTC)

    monkeypatch.setattr(service_settings, "datetime", FrozenDateTime)
    setup, _, _, _, _, sonarr, shoko = setup_tests._ready_setup_form(tmp_path, monkeypatch)
    preferences = ServiceSettingsController(setup.bootstrap, lambda: None)
    try:
        initial = preferences_tests.settings_form(preferences)
        if initially_enabled:
            initial["allow_missing_metadata_refresh"] = "1"
        assert "Settings saved" in preferences.save(initial)
        old_view = setup.refresh_view()
        assert old_view is not None
        stale = setup_tests._consent_form(old_view)
        with open_state(setup.state_path) as store:
            old_revision = store.get_service_authorization_revision()
        # A second controller/connection changes the choice twice, ending at exactly
        # the original document. A timestamp-based generation would miss this.
        other = ServiceSettingsController(setup.bootstrap, lambda: None)
        for enabled in (not initially_enabled, initially_enabled):
            form = preferences_tests.settings_form(other)
            if enabled:
                form["allow_missing_metadata_refresh"] = "1"
            assert "Settings saved" in other.save(form)
        with open_state(setup.state_path) as store:
            revision = store.get_service_authorization_revision()
            assert revision is not None
            assert revision != old_revision
            assert OnboardingManager(store).reconfirmation_required()
        assert "choice changed" in setup.action("confirm", stale)
        # Reloading the controller sees the persisted generation; only a fresh form
        # can clear the later review hold.
        restarted = ServiceSetupController(setup.bootstrap, sonarr, shoko, setup.state_path)
        monkeypatch.setattr(restarted, "_environment", setup._environment)
        monkeypatch.setattr(restarted, "_inventory", setup._inventory)
        fresh = restarted.refresh_view()
        assert fresh is not None
        assert fresh.authorization_revision == revision
        assert "selected" in restarted.action(
            "scope", {"setup_digest": fresh.snapshot_digest, "root_0": setup_tests.ANIME_ROOT}
        )
        fresh = restarted.refresh_view()
        assert fresh is not None
        assert "Setup saved" in restarted.action("confirm", setup_tests._consent_form(fresh))
        with open_state(setup.state_path) as store:
            revision = store.get_service_authorization_revision()
        routine = preferences_tests.settings_form(other)
        if initially_enabled:
            routine["allow_missing_metadata_refresh"] = "1"
        routine["interval_seconds"] = "90"
        assert "Settings saved" in other.save(routine)
        with open_state(setup.state_path) as store:
            assert store.get_service_authorization_revision() == revision
            assert not OnboardingManager(store).reconfirmation_required()
    finally:
        sonarr.client.close()
        shoko.client.close()


@pytest.mark.parametrize("enabled", [False, True])
def test_settings_control_explains_scope_of_opt_in(tmp_path: Path, enabled: bool) -> None:
    from shoko_linker.dashboard import DashboardApplication

    preferences = preferences_tests.controller(tmp_path)
    form = preferences_tests.settings_form(preferences)
    if enabled:
        form["allow_missing_metadata_refresh"] = "1"
    assert "Settings saved" in preferences.save(form)
    rendered = DashboardApplication._settings_section(preferences.view(), "csrf-test")
    expected = (
        'name="allow_missing_metadata_refresh" value="1"' + (" checked" if enabled else "") + ">"
    )
    assert expected in rendered
    assert "Let Shoko retrieve missing anime and episode metadata" in rendered
    assert "including your manual collection" in rendered
    assert "does not cancel" in rendered
    assert "Restart the container" not in preferences.view().message
