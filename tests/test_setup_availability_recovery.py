"""Saved setup survives temporary availability failures without new consent."""

from dataclasses import replace
from pathlib import Path

import pytest
from test_service_setup import _ready_setup_form

from shoko_linker.dashboard import DashboardApplication
from shoko_linker.onboarding import OnboardingStatus
from shoko_linker.service_setup import ServiceSetupController
from shoko_linker.setup import ScopeReadStatus
from shoko_linker.state import META_SERVICE_SETTINGS, open_state

NOW = "2026-11-01T00:00:00Z"


def test_restart_outage_recovers_automatically_with_saved_choices(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, form, environment, _, _, sonarr, shoko = _ready_setup_form(tmp_path, monkeypatch)
    try:
        assert "Setup saved" in controller.action("confirm", form)
        restarted = ServiceSetupController(
            controller.bootstrap, sonarr, shoko, controller.state_path
        )
        unavailable = replace(environment[1], read_status=ScopeReadStatus.UNAVAILABLE)
        monkeypatch.setattr(
            restarted, "_environment", lambda: (environment[0], unavailable, environment[2])
        )
        with open_state(controller.state_path) as store:
            consent = store.get_onboarding_json()
            assert restarted.current_context(store) is None
            assert store.get_service_pause() == (True, "setup_unavailable")
        view = restarted.refresh_view()
        assert view is not None
        assert view.saved_setup
        rendered = DashboardApplication._setup_section(view, "csrf")
        assert "Checking saved libraries" in rendered
        assert "<form" not in rendered
        assert "confirm_" not in rendered
        # Close/reopen the database and use another fresh controller.
        recovered = ServiceSetupController(
            controller.bootstrap, sonarr, shoko, controller.state_path
        )
        monkeypatch.setattr(recovered, "_environment", lambda: environment)
        with open_state(controller.state_path) as store:
            context = recovered.current_context(store)
            assert context is not None
            decision = recovered.onboarding(store).evaluate_context(context, iso=NOW)
            assert decision.status is OnboardingStatus.AUTHORIZED
            assert store.get_service_pause() == (False, "")
            assert store.get_onboarding_json() == consent
    finally:
        sonarr.client.close()
        shoko.client.close()


@pytest.mark.parametrize(
    "change", ["user", "new_temporary", "hard_hold", "settings", "scope", "consent"]
)
def test_recovery_cas_preserves_concurrent_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    controller, form, _, _, _, sonarr, shoko = _ready_setup_form(tmp_path, monkeypatch)
    try:
        controller.action("confirm", form)
        with open_state(controller.state_path) as store:
            store.pause_service_for_review("setup_unavailable", NOW)
            context = controller.current_context(store)
            assert context is not None
            clear = store.clear_temporary_setup_pause_if_current

            def concurrent(*args: object, **kwargs: object) -> bool:
                with open_state(controller.state_path) as other:
                    if change == "user":
                        other.pause_service_for_user(NOW)
                    elif change == "new_temporary":
                        other.pause_service_for_review("setup_unavailable", NOW)
                    elif change == "hard_hold":
                        other.pause_service_for_review("setup_changed", NOW)
                    elif change == "settings":
                        other.set_meta(META_SERVICE_SETTINGS, "changed")
                    elif change == "scope":
                        other.save_service_scope_selection(
                            '["/different"]',
                            expected_authorization_revision=context.authorization_revision,
                            iso=NOW,
                        )
                    else:
                        other.save_onboarding_json(context.consent_json + " ", NOW)
                return clear(*args, **kwargs)  # type: ignore[arg-type]

            monkeypatch.setattr(store, "clear_temporary_setup_pause_if_current", concurrent)
            assert (
                controller.onboarding(store).evaluate_context(context, iso=NOW).status
                is OnboardingStatus.PAUSED
            )
            assert store.get_service_pause()[0]
            if change == "user":
                assert store.get_service_pause()[1] == "user_paused"
    finally:
        sonarr.client.close()
        shoko.client.close()


def test_missing_inventory_keeps_saved_setup_retry_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, form, _, _, _, sonarr, shoko = _ready_setup_form(tmp_path, monkeypatch)
    try:
        controller.action("confirm", form)
        inventory, discovery, paths = controller._inventory(("/data/media/anime",))
        incomplete = replace(inventory, verdict=replace(inventory.verdict, complete=False))
        monkeypatch.setattr(controller, "_inventory", lambda _: (incomplete, discovery, paths))
        controller._inventory_cache = None
        view = controller.refresh_view()
        assert view is not None
        assert view.saved_setup
        assert not view.confirmation_enabled
        assert "<form" not in DashboardApplication._setup_section(view, "csrf")
        with open_state(controller.state_path) as store:
            assert not controller.onboarding(store).reconfirmation_required()
    finally:
        sonarr.client.close()
        shoko.client.close()


@pytest.mark.parametrize("timing", ["before", "during"])
def test_user_pause_during_fresh_read_is_preserved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, timing: str
) -> None:
    controller, form, environment, _, _, sonarr, shoko = _ready_setup_form(tmp_path, monkeypatch)
    try:
        controller.action("confirm", form)
        with open_state(controller.state_path) as store:
            store.pause_service_for_review("setup_unavailable", NOW)
            if timing == "before":
                store.pause_service_for_user(NOW)

            def read_environment():  # type: ignore[no-untyped-def]
                if timing == "during":
                    with open_state(controller.state_path) as other:
                        other.pause_service_for_user(NOW)
                return environment

            monkeypatch.setattr(controller, "_environment", read_environment)
            context = controller.current_context(store)
            assert context is not None
            assert (
                controller.onboarding(store).evaluate_context(context, iso=NOW).status
                is OnboardingStatus.PAUSED
            )
            assert store.get_service_pause() == (True, "user_paused")
    finally:
        sonarr.client.close()
        shoko.client.close()


def test_new_runtime_automatically_recovers_http_outage(tmp_path: Path) -> None:
    import e2e_harness as eh
    from test_service_e2e import _confirm_setup, _endpoint, _service_world

    from shoko_linker import cli
    from shoko_linker.bootstrap import BootstrapSettings
    from shoko_linker.config import SecretRef
    from shoko_linker.onboarding import OnboardingManager

    world = _service_world(tmp_path)
    sonarr = _endpoint(world.sonarr.handler, eh.SONARR_BASE)
    shoko = _endpoint(world.shoko.handler, eh.SHOKO_BASE)
    bootstrap = BootstrapSettings(
        eh.SONARR_BASE,
        SecretRef("SONARR_API_KEY"),
        eh.SHOKO_BASE,
        SecretRef("SHOKO_API_KEY"),
        tmp_path / "state.sqlite3",
    )
    try:
        original = cli._NormalServiceRuntime(bootstrap, sonarr, shoko, cli.StopState())
        _confirm_setup(original.setup)
        with open_state(bootstrap.database_path) as store:
            record = OnboardingManager(store).record()
            assert record is not None
        world.sonarr.down = True
        with open_state(bootstrap.database_path) as store:
            assert original.setup.current_context(store) is None
            assert store.get_service_pause() == (True, "setup_unavailable")
        restarted = cli._NormalServiceRuntime(bootstrap, sonarr, shoko, cli.StopState())
        assert restarted.setup.refresh_view().saved_setup  # type: ignore[union-attr]
        world.sonarr.down = False
        with open_state(bootstrap.database_path) as store:
            assert restarted._service_authorized(
                store, OnboardingManager(store), record.binding_digest
            )
            assert store.get_service_pause() == (False, "")
        assert world.shoko.posts_count() == 0
    finally:
        sonarr.client.close()
        shoko.client.close()


def test_selected_folder_temporarily_inaccessible_recovers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from shoko_linker import service_setup
    from shoko_linker.shoko import ImportFolder

    controller, form, _, _, _, sonarr, shoko = _ready_setup_form(tmp_path, monkeypatch)
    try:
        controller.action("confirm", form)
        folders = [ImportFolder(1, "Anime", "/anime", "None", False)]
        monkeypatch.setattr(service_setup, "collect_import_folders", lambda *_: tuple(folders))
        with open_state(controller.state_path) as store:
            assert controller.current_context(store) is None
            assert store.get_service_pause() == (True, "setup_unavailable")
            assert not controller.onboarding(store).reconfirmation_required()
        folders[0] = replace(folders[0], path_usable=True)
        with open_state(controller.state_path) as store:
            context = controller.current_context(store)
            assert context is not None
            assert (
                controller.onboarding(store).evaluate_context(context, iso=NOW).status
                is OnboardingStatus.AUTHORIZED
            )
            assert store.get_service_pause() == (False, "")
    finally:
        sonarr.client.close()
        shoko.client.close()


@pytest.mark.parametrize(
    "reason",
    [
        "user_paused",
        "global_budget_exhausted",
        "canary_failed",
        "uncertain_request",
        "setup_changed",
    ],
)
def test_outage_never_replaces_an_independent_pause(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, reason: str
) -> None:
    controller, form, _, _, _, sonarr, shoko = _ready_setup_form(tmp_path, monkeypatch)
    try:
        controller.action("confirm", form)
        with open_state(controller.state_path) as store:
            store.set_service_pause(True, reason, NOW)
            store.pause_service_for_review("setup_unavailable", NOW)
            assert store.get_service_pause() == (True, reason)
            context = controller.current_context(store)
            assert context is not None
            assert (
                controller.onboarding(store).evaluate_context(context, iso=NOW).status
                is not OnboardingStatus.AUTHORIZED
            )
            assert store.get_service_pause() == (True, reason)
    finally:
        sonarr.client.close()
        shoko.client.close()


def test_recovery_clear_rolls_back_atomically_on_storage_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from shoko_linker.state import StateError

    controller, form, _, _, _, sonarr, shoko = _ready_setup_form(tmp_path, monkeypatch)
    try:
        controller.action("confirm", form)
        with open_state(controller.state_path) as store:
            store.pause_service_for_review("setup_unavailable", NOW)
            context = controller.current_context(store)
            assert context is not None
            original = store._set_service_pause_tx

            def failed_update(paused: bool, reason: str, iso: str) -> None:
                original(paused, reason, iso)
                raise StateError("injected storage failure")

            monkeypatch.setattr(store, "_set_service_pause_tx", failed_update)
            with pytest.raises(StateError, match="injected storage failure"):
                controller.onboarding(store).evaluate_context(context, iso=NOW)
            assert store.get_service_pause() == (True, "setup_unavailable")
            assert store.temporary_setup_pause_token() == context.temporary_pause_token
    finally:
        sonarr.client.close()
        shoko.client.close()
