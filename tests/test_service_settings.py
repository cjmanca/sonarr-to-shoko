"""Authenticated preferences, validated persistence, and automatic-service scope changes."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import httpx
import pytest
import test_dashboard as dashboard_tests
import test_service_setup as setup_tests
import yaml  # type: ignore[import-untyped]

from shoko_linker.bootstrap import BootstrapSettings
from shoko_linker.config import SecretRef
from shoko_linker.dashboard import DashboardApplication
from shoko_linker.errors import ConfigError
from shoko_linker.onboarding import OnboardingManager
from shoko_linker.paths import PathMapping
from shoko_linker.service import settings_for_scope
from shoko_linker.service_settings import (
    SETTINGS_META,
    ServiceSettingsController,
    apply_service_settings,
    configured_bootstrap,
    load_service_settings,
)
from shoko_linker.service_setup import DRAFT_ROOTS_META, ServiceSetupController
from shoko_linker.setup import AnimeRootCandidate, MappingDiscoveryStatus, PathMappingDiscovery
from shoko_linker.state import StateStore, WriteDisabledError, open_state


def controller(tmp_path: Path) -> ServiceSettingsController:
    return ServiceSettingsController(
        BootstrapSettings(
            "http://sonarr:8989",
            SecretRef("SONARR_API_KEY"),
            "http://shoko:8111",
            SecretRef("SHOKO_API_KEY"),
            tmp_path / "state.sqlite3",
        ),
        lambda: None,
    )


def settings_form(preferences: ServiceSettingsController) -> dict[str, str]:
    view = preferences.view()
    return {
        **{name: value for name, _label, value in view.values},
        "advanced_yaml": view.advanced_yaml,
        "settings_digest": view.digest,
    }


def test_preferences_round_trip_apply_to_runtime_and_reject_stale_form(tmp_path: Path) -> None:
    preferences = controller(tmp_path)
    wakes: list[bool] = []
    preferences.wake = lambda: wakes.append(True)
    form = settings_form(preferences)
    stale = dict(form)
    form.update(interval_seconds="45", refresh_hours="24", max_new_per_cycle="3")
    advanced = yaml.safe_load(form["advanced_yaml"])
    advanced["sync"]["timeouts"]["read_seconds"] = 90
    form["advanced_yaml"] = yaml.safe_dump(advanced)
    assert preferences.save(form).startswith("Settings saved.")
    assert wakes == [True]
    reopened = ServiceSettingsController(preferences.bootstrap, lambda: None)
    assert reopened.interval() == 45

    assessment = dashboard_tests.safe_assessment()
    binding = dashboard_tests.scope_binding(assessment)
    effective = apply_service_settings(
        settings_for_scope(
            preferences.bootstrap, roots=binding.roots, mappings=binding.path_mappings
        ),
        reopened.document(),
    )
    assert effective.sync.max_new_per_cycle == 3
    assert effective.sync.read_timeout_seconds == 90
    assert effective.mappings.refresh_hours == 24
    assert preferences.save(stale).startswith("Settings were not saved.")
    assert reopened.interval() == 45


@pytest.mark.parametrize("persisted", [False, True])
@pytest.mark.parametrize("winner_interval", [60, 90])
def test_concurrent_forms_compare_and_save_atomically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, persisted: bool, winner_interval: int
) -> None:
    preferences = controller(tmp_path)
    if persisted:
        assert preferences.save(settings_form(preferences)).startswith("Settings saved.")
    stale = settings_form(preferences)
    winner = dict(stale, interval_seconds=str(winner_interval))
    stale.update(interval_seconds=str(150 - winner_interval), sonarr_url="http://stale:8989")
    original = StateStore.save_service_settings_if_current
    interleaved = False
    wakes: list[bool] = []
    preferences.wake = lambda: wakes.append(True)

    def save(
        store: StateStore,
        expected_json: str | None,
        document_json: str,
        *,
        authorization_changed: bool,
        iso: str,
    ) -> bool:
        nonlocal interleaved
        if not interleaved:
            interleaved = True
            # Another controller/SQLite handle wins after the stale form's read.
            assert preferences.save(winner).startswith("Settings saved.")
        return original(
            store,
            expected_json,
            document_json,
            authorization_changed=authorization_changed,
            iso=iso,
        )

    monkeypatch.setattr(StateStore, "save_service_settings_if_current", save)
    assert "changed in another window" in preferences.save(stale)
    assert interleaved
    assert wakes == [True]
    assert preferences.interval() == winner_interval
    with open_state(preferences.bootstrap.database_path) as store:
        assert store.get_service_pause() == (False, "")
        assert not OnboardingManager(store).reconfirmation_required()
        assert (
            load_service_settings(store, preferences.bootstrap)["sonarr_url"]
            == "http://sonarr:8989"
        )


@pytest.mark.parametrize("connection_first", [False, True])
def test_concurrent_connection_change_keeps_review_and_user_pause(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, connection_first: bool
) -> None:
    preferences = controller(tmp_path)
    form = settings_form(preferences)
    connection = dict(form, sonarr_url="http://new-sonarr:8989")
    routine = dict(form, interval_seconds="90")
    original = StateStore.save_service_settings_if_current
    interleaved = False

    def save(
        store: StateStore,
        expected_json: str | None,
        document_json: str,
        *,
        authorization_changed: bool,
        iso: str,
    ) -> bool:
        nonlocal interleaved
        if not interleaved:
            interleaved = True
            with open_state(preferences.bootstrap.database_path) as other:
                other.pause_service_for_user(iso)
                other.pause_service_for_review("unsafe_shoko_settings", iso)
            assert preferences.save(connection if connection_first else routine).startswith(
                "Settings saved."
            )
        return original(
            store,
            expected_json,
            document_json,
            authorization_changed=authorization_changed,
            iso=iso,
        )

    monkeypatch.setattr(StateStore, "save_service_settings_if_current", save)
    assert "changed in another window" in preferences.save(
        routine if connection_first else connection
    )
    # After a rejected stale connection form, a freshly reviewed form still works.
    if not connection_first:
        assert preferences.save(
            dict(settings_form(preferences), sonarr_url=connection["sonarr_url"])
        ).startswith("Settings saved.")
    with open_state(preferences.bootstrap.database_path) as restarted:
        assert restarted.get_service_pause() == (True, "user_paused")
        assert OnboardingManager(restarted).reconfirmation_required()
    assert configured_bootstrap(preferences.bootstrap).sonarr_url == connection["sonarr_url"]


def test_missing_preferences_use_runtime_url_defaults_when_saving(tmp_path: Path) -> None:
    preferences = controller(tmp_path)
    preferences.bootstrap = replace(preferences.bootstrap, sonarr_url="http://runtime-sonarr:8989")
    form = settings_form(preferences)
    form["interval_seconds"] = "75"
    assert preferences.save(form).startswith("Settings saved.")
    with open_state(preferences.bootstrap.database_path) as store:
        assert (
            load_service_settings(store, preferences.bootstrap)["sonarr_url"] == form["sonarr_url"]
        )
        assert store.get_service_pause() == (False, "")
        assert not OnboardingManager(store).reconfirmation_required()


@pytest.mark.parametrize("persisted", [False, True])
def test_failed_settings_commit_rolls_back_preferences_and_review(
    tmp_path: Path, persisted: bool
) -> None:
    preferences = controller(tmp_path)
    if persisted:
        assert preferences.save(settings_form(preferences)).startswith("Settings saved.")
    iso = dashboard_tests.T0.isoformat()
    with open_state(preferences.bootstrap.database_path) as store:
        original_json = store.get_meta(SETTINGS_META)
        document = load_service_settings(store, preferences.bootstrap)
        document["sonarr_url"] = "http://new-sonarr:8989"
        denied = False

        def deny_commit(
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

        store.connection.set_authorizer(deny_commit)
        with pytest.raises(WriteDisabledError, match="state writes stopped"):
            store.save_service_settings_if_current(
                original_json, json.dumps(document), authorization_changed=True, iso=iso
            )
        assert denied
        assert store.get_meta(SETTINGS_META) == original_json
        assert store.get_service_authorization_revision() is None
        assert store.get_service_pause() == (False, "")
        assert not OnboardingManager(store).reconfirmation_required()
        assert store.writes_disabled
    with open_state(preferences.bootstrap.database_path) as reopened:
        assert reopened.get_meta(SETTINGS_META) == original_json
        assert reopened.get_service_authorization_revision() is None
        assert reopened.get_service_pause() == (False, "")
        assert not OnboardingManager(reopened).reconfirmation_required()
        assert reopened.writes_disabled
        with pytest.raises(WriteDisabledError):
            reopened.save_service_settings_if_current(
                original_json, json.dumps(document), authorization_changed=True, iso=iso
            )


@pytest.mark.parametrize("change", ["limit", "apply", "key", "url", "yaml"])
def test_invalid_preferences_do_not_persist_or_change_consent(tmp_path: Path, change: str) -> None:
    preferences = controller(tmp_path)
    form = settings_form(preferences)
    advanced = yaml.safe_load(form["advanced_yaml"])
    if change == "limit":
        form["max_new_per_cycle"] = "21"
    elif change == "apply":
        advanced["sync"]["apply_changes"] = True
    elif change == "key":
        advanced["sonarr_api_key"] = "never-real-secret"
    elif change == "url":
        form["sonarr_url"] = "http://user:never-real-secret@sonarr:8989"
    else:
        form["advanced_yaml"] = "["
    if change != "yaml":
        form["advanced_yaml"] = yaml.safe_dump(advanced)
    assert preferences.save(form).startswith("Settings were not saved.")
    with open_state(preferences.bootstrap.database_path) as store:
        assert store.get_meta(SETTINGS_META) is None
        assert store.get_onboarding_json() is None
        assert store.get_service_pause() == (False, "")


def test_http_mapping_source_is_rejected_on_save_load_and_runtime_apply(tmp_path: Path) -> None:
    preferences = controller(tmp_path)
    form = settings_form(preferences)
    advanced = yaml.safe_load(form["advanced_yaml"])
    advanced["mappings"]["source_url"] = "http://example.invalid/mappings.xml"
    form["advanced_yaml"] = yaml.safe_dump(advanced)
    assert "HTTPS" in preferences.save(form)
    with open_state(preferences.bootstrap.database_path) as store:
        assert store.get_meta(SETTINGS_META) is None
        document = load_service_settings(store, preferences.bootstrap)
        document["mappings"]["source_url"] = "http://example.invalid/mappings.xml"
        # A saved document from an older version cannot bypass the current source policy.
        store.set_meta(SETTINGS_META, json.dumps(document))
        with pytest.raises(ConfigError, match="HTTPS"):
            load_service_settings(store, preferences.bootstrap)
    binding = dashboard_tests.scope_binding(dashboard_tests.safe_assessment())
    settings = settings_for_scope(
        preferences.bootstrap, roots=binding.roots, mappings=binding.path_mappings
    )
    with pytest.raises(ConfigError, match="HTTPS"):
        apply_service_settings(settings, document)


def test_connection_change_holds_writes_until_restart_and_new_confirmation(tmp_path: Path) -> None:
    preferences = controller(tmp_path)
    with open_state(preferences.bootstrap.database_path) as store:
        assessment = dashboard_tests.safe_assessment()
        binding = dashboard_tests.scope_binding(assessment)
        OnboardingManager(store).confirm_setup(
            binding,
            assessment,
            confirmed_facts=assessment.unverifiable_facts + assessment.unknown_settings,
            automatic_link_consent=True,
            iso=dashboard_tests.T0.isoformat(),
        )
    form = settings_form(preferences)
    form["sonarr_url"] = "http://new-sonarr:8989"
    assert preferences.save(form).startswith("Settings saved.")
    with open_state(preferences.bootstrap.database_path) as store:
        assert OnboardingManager(store).reconfirmation_required()
        assert store.get_service_pause()[0]
        assert (
            load_service_settings(store, preferences.bootstrap)["sonarr_url"] == form["sonarr_url"]
        )
    assert configured_bootstrap(preferences.bootstrap).sonarr_url == form["sonarr_url"]
    assert "Restart" in preferences.view().message


def test_settings_page_and_mutations_require_authentication_and_csrf(tmp_path: Path) -> None:
    preferences = controller(tmp_path)
    app = DashboardApplication(
        preferences.bootstrap.database_path,
        settings_provider=preferences.view,
        settings_action=preferences.save,
    )
    server, thread, base = dashboard_tests.start_server(app)
    try:
        with httpx.Client(follow_redirects=False) as client:
            assert client.get(f"{base}/settings").status_code == 303
            assert (
                client.post(
                    f"{base}/settings",
                    data=settings_form(preferences),
                    headers=dashboard_tests.same_origin(base),
                ).status_code
                == 403
            )
            assert dashboard_tests.setup_account(client, base).status_code == 303
            page = client.get(f"{base}/settings")
            assert page.status_code == 200
            assert "<summary>Show advanced</summary>" in page.text
            assert "<details open" not in page.text
            assert "Change libraries and folder mappings" in page.text
            form = settings_form(preferences)
            form["interval_seconds"] = "60"
            assert (
                client.post(
                    f"{base}/settings", data=form, headers=dashboard_tests.same_origin(base)
                ).status_code
                == 403
            )
            form["csrf_token"] = dashboard_tests.csrf_from(page.text)
            saved = client.post(
                f"{base}/settings", data=form, headers=dashboard_tests.same_origin(base)
            )
            assert saved.status_code == 200
            assert "Settings saved." in saved.text
            assert preferences.interval() == 60
    finally:
        dashboard_tests.stop_server(server, thread)


def test_library_editor_pauses_existing_consent_and_persists_draft_across_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup, form, _environment, _folders, _clock, sonarr, shoko = setup_tests._ready_setup_form(
        tmp_path, monkeypatch
    )
    try:
        assert "saved" in setup.action("confirm", form).lower()
        assert setup.refresh_view() is None
        compatibility, scope, assessment = setup._environment()
        new_root = "/data/media/other-anime"
        updated_scope = replace(
            scope,
            candidates=(
                *scope.candidates,
                AnimeRootCandidate(new_root, (42,)),
            ),
        )
        monkeypatch.setattr(
            setup, "_environment", lambda: (compatibility, updated_scope, assessment)
        )
        inventory, discovery, _path_result = setup._inventory((setup_tests.ANIME_ROOT,))
        monkeypatch.setattr(
            setup,
            "_inventory",
            lambda roots: (
                inventory,
                discovery,
                PathMappingDiscovery(
                    MappingDiscoveryStatus.READY,
                    tuple(PathMapping(root, setup_tests.SHOKO_ROOT) for root in roots),
                    (),
                    "Exact paths and sizes establish the folder match.",
                ),
            ),
        )
        assert "Review" in setup.action("edit", {})
        view = setup.refresh_view()
        assert view is not None
        assert "selected" in setup.action(
            "scope", {"setup_digest": view.snapshot_digest, "root_1": new_root}
        )
        with open_state(setup.state_path) as store:
            assert OnboardingManager(store).reconfirmation_required()
            assert store.get_service_pause()[0]
            assert store.get_meta(DRAFT_ROOTS_META) is not None
        restarted = ServiceSetupController(setup.bootstrap, sonarr, shoko, setup.state_path)
        monkeypatch.setattr(restarted, "_environment", setup._environment)
        monkeypatch.setattr(restarted, "_inventory", setup._inventory)
        restored = restarted.refresh_view()
        assert restored is not None
        assert restored.selected_roots == (new_root,)
        assert restored.inferred_mappings == ((new_root, setup_tests.SHOKO_ROOT),)
        assert restored.confirmation_enabled
        confirmed = dict(form, setup_digest=restored.snapshot_digest)
        assert "saved" in restarted.action("confirm", confirmed).lower()
        with open_state(setup.state_path) as store:
            assert not OnboardingManager(store).reconfirmation_required()
            assert store.get_meta(DRAFT_ROOTS_META) is None
            record = OnboardingManager(store).record()
            assert record is not None
            assert record.binding.roots == (new_root,)
    finally:
        sonarr.client.close()
        shoko.client.close()


def test_mapping_editor_replaces_saved_mapping_for_unchanged_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup, form, _environment, folders, _clock, sonarr, shoko = setup_tests._ready_setup_form(
        tmp_path, monkeypatch
    )
    try:
        assert "saved" in setup.action("confirm", form).lower()
        folders["value"] = ["/new-anime"]
        inventory, discovery, _path_result = setup._inventory((setup_tests.ANIME_ROOT,))
        monkeypatch.setattr(
            setup,
            "_inventory",
            lambda roots: (
                inventory,
                discovery,
                PathMappingDiscovery(
                    MappingDiscoveryStatus.READY,
                    (PathMapping(setup_tests.ANIME_ROOT, "/new-anime"),),
                    (),
                    "Verified new folder match.",
                ),
            ),
        )
        setup.action("edit", {})
        view = setup.refresh_view()
        assert view is not None
        assert "selected" in setup.action(
            "scope", {"setup_digest": view.snapshot_digest, "root_0": setup_tests.ANIME_ROOT}
        )
        with open_state(setup.state_path) as store:
            assert json.loads(store.get_meta(DRAFT_ROOTS_META) or "null") == [
                setup_tests.ANIME_ROOT
            ]
        restarted = ServiceSetupController(setup.bootstrap, sonarr, shoko, setup.state_path)
        monkeypatch.setattr(restarted, "_environment", setup._environment)
        monkeypatch.setattr(restarted, "_inventory", setup._inventory)
        refreshed = restarted.refresh_view()
        assert refreshed is not None
        assert refreshed.inferred_mappings == ((setup_tests.ANIME_ROOT, "/new-anime"),)
        assert refreshed.confirmation_enabled
        assert (
            "saved"
            in restarted.action(
                "confirm", dict(form, setup_digest=refreshed.snapshot_digest)
            ).lower()
        )
        with open_state(setup.state_path) as store:
            record = OnboardingManager(store).record()
            assert record is not None
            assert record.binding.path_mappings == (
                PathMapping(setup_tests.ANIME_ROOT, "/new-anime"),
            )
            context = restarted.current_context(store)
            assert context is not None
            assert context.binding.path_mappings == record.binding.path_mappings
    finally:
        sonarr.client.close()
        shoko.client.close()
