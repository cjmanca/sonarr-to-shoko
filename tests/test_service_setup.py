"""Normal-service setup discovery and durable confirmation orchestration."""

from __future__ import annotations

import hashlib
import threading
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import cast

import httpx
import pytest

from shoko_linker import service_setup
from shoko_linker.bootstrap import BootstrapSettings
from shoko_linker.config import SecretRef
from shoko_linker.dashboard import DashboardApplication, DashboardSetupView
from shoko_linker.http import ApiEndpoint
from shoko_linker.onboarding import ScopeBinding
from shoko_linker.paths import PathMapping
from shoko_linker.safety_check import (
    SafetyAssessment,
    assess_settings_payload,
    evaluate_service_safety,
)
from shoko_linker.service_setup import ServiceSetupController
from shoko_linker.setup import (
    AnimeRootCandidate,
    AnimeScopeDiscovery,
    ApiCompatibility,
    MappingDiscoveryStatus,
    PathMappingDiscovery,
    ScopeReadStatus,
    ScopeStatus,
)
from shoko_linker.shoko import ImportFolder, ShokoDiscovery, ShokoVerdict
from shoko_linker.sonarr import InventoryVerdict, SonarrInventory
from shoko_linker.state import open_state

ANIME_ROOT = "/data/media/anime"
SHOKO_ROOT = "/anime"


def _ready_setup_form(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> tuple[
    ServiceSetupController,
    dict[str, str],
    tuple[ApiCompatibility, AnimeScopeDiscovery, SafetyAssessment],
    dict[str, list[str]],
    list[float],
    ApiEndpoint,
    ApiEndpoint,
]:
    assessment = assess_settings_payload({"FileQualityFilterEnabled": False})
    compatibility = ApiCompatibility("4.0.0", "5.3.3", True, True, (), ())
    scope = AnimeScopeDiscovery(
        ScopeStatus.CONFIRM_SCOPE,
        (AnimeRootCandidate(ANIME_ROOT, (41,)),),
        "Confirm the Sonarr anime folder.",
    )
    inventory = SonarrInventory((), (), (), (), (), (), (), InventoryVerdict(True, True, (), ()))
    path_result = PathMappingDiscovery(
        MappingDiscoveryStatus.NEEDS_USER_MAPPING,
        (),
        (),
        "Choose a safe folder mapping.",
    )
    folder_paths = {"value": [SHOKO_ROOT]}

    def current_discovery() -> ShokoDiscovery:
        folders = tuple(
            ImportFolder(index, path.rsplit("/", 1)[-1], path, "None", True)
            for index, path in enumerate(folder_paths["value"], start=1)
        )
        return ShokoDiscovery(
            (),
            {folder.id: folder for folder in folders},
            ShokoVerdict(True, (), ()),
            len(folders),
            0,
            1000,
        )

    environment = (compatibility, scope, assessment)
    sonarr = ApiEndpoint(httpx.Client(), lambda: {})
    shoko = ApiEndpoint(httpx.Client(), lambda: {})
    state_path = tmp_path / "state.sqlite3"
    bootstrap = BootstrapSettings(
        "http://sonarr:8989",
        SecretRef("SONARR_API_KEY"),
        "http://shoko:8111",
        SecretRef("SHOKO_API_KEY"),
        state_path,
    )
    controller = ServiceSetupController(bootstrap, sonarr, shoko, state_path)
    clock = [100.0]
    monkeypatch.setattr(service_setup, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    monkeypatch.setattr(controller, "_environment", lambda: environment)
    monkeypatch.setattr(
        controller,
        "_inventory",
        lambda _roots: (inventory, current_discovery(), path_result),
    )
    monkeypatch.setattr(
        service_setup,
        "collect_import_folders",
        lambda *_: tuple(current_discovery().folders.values()),
    )
    controller._selected_roots = (ANIME_ROOT,)
    view = controller.refresh_view()
    assert view is not None
    assert view.confirmation_enabled
    form = {
        "setup_digest": view.snapshot_digest,
        "automatic_link_consent": "1",
        "mapping_0": "0",
    }
    form.update(
        {
            f"confirm_{index}": hashlib.sha256(fact.encode("utf-8")).hexdigest()
            for index, fact in enumerate(view.required_confirmations)
        }
    )
    return controller, form, environment, folder_paths, clock, sonarr, shoko


def _consent_form(view: DashboardSetupView) -> dict[str, str]:
    return {
        "setup_digest": view.snapshot_digest,
        "automatic_link_consent": "1",
        "mapping_0": "0",
        **{
            f"confirm_{index}": hashlib.sha256(fact.encode("utf-8")).hexdigest()
            for index, fact in enumerate(view.required_confirmations)
        },
    }


@pytest.fixture
def mixed_library(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[tuple[ServiceSetupController, dict[int, ImportFolder], list[SafetyAssessment]]]:
    controller, _, environment, _, _, sonarr, shoko = _ready_setup_form(tmp_path, monkeypatch)
    raw = [
        assess_settings_payload(
            {
                "FileQualityFilterEnabled": False,
                "Plugins": {
                    "Renamer": {
                        "MoveOnImport": True,
                        "RenameOnImport": True,
                        "RelocateOnImport": True,
                    }
                },
                "AniDb": {"MyList_AddFiles": False},
            }
        )
    ]
    folders = {
        1: ImportFolder(1, "Sonarr", SHOKO_ROOT, "None", True),
        2: ImportFolder(2, "Manual drop", "/drop", "Source", True),
        3: ImportFolder(3, "Manual library", "/manual", "Destination", True),
    }
    original_inventory = controller._inventory

    def inventory(
        roots: tuple[str, ...],
    ) -> tuple[SonarrInventory, ShokoDiscovery, PathMappingDiscovery]:
        sonarr_inventory, discovery, mapping = original_inventory(roots)
        return sonarr_inventory, replace(discovery, folders=dict(folders)), mapping

    monkeypatch.setattr(controller, "_environment", lambda: (*environment[:2], raw[0]))
    monkeypatch.setattr(controller, "_inventory", inventory)
    monkeypatch.setattr(service_setup, "collect_import_folders", lambda *_: tuple(folders.values()))
    controller._inventory_cache = None
    yield controller, folders, raw
    sonarr.client.close()
    shoko.client.close()


@pytest.mark.parametrize("inferred", [False, True])
def test_mixed_library_consent_requires_physical_facts_and_persists_selected_only_digest(
    mixed_library: tuple[ServiceSetupController, dict[int, ImportFolder], list[SafetyAssessment]],
    monkeypatch: pytest.MonkeyPatch,
    inferred: bool,
) -> None:
    controller, folders, raw = mixed_library
    if inferred:
        inventory = controller._inventory

        def inferred_inventory(
            roots: tuple[str, ...],
        ) -> tuple[SonarrInventory, ShokoDiscovery, PathMappingDiscovery]:
            sonarr_inventory, discovery, _mapping = inventory(roots)
            return (
                sonarr_inventory,
                discovery,
                PathMappingDiscovery(
                    MappingDiscoveryStatus.READY,
                    (PathMapping(ANIME_ROOT, SHOKO_ROOT),),
                    (),
                    "Exact paths and sizes match.",
                ),
            )

        monkeypatch.setattr(controller, "_inventory", inferred_inventory)
    view = controller.refresh_view()
    assert view is not None
    assert view.confirmation_enabled
    assert view.mapping_required is not inferred
    assert any("read-only inside Shoko" in fact for fact in view.required_confirmations)
    assert any("writable hardlink" in fact for fact in view.required_confirmations)
    assert any("external hooks" in fact for fact in view.required_confirmations)
    form = _consent_form(view)
    form.pop("confirm_0")
    assert "confirm every" in controller.action("confirm", form)
    with open_state(controller.state_path) as store:
        assert controller.onboarding(store).record() is None
    view = controller.refresh_view()
    assert view is not None
    assert "Setup saved." in controller.action("confirm", _consent_form(view))
    effective = evaluate_service_safety(raw[0], (PathMapping(ANIME_ROOT, SHOKO_ROOT),), folders)
    with open_state(controller.state_path) as store:
        record = controller.onboarding(store).record()
        assert record is not None
        assert record.binding.safety_digest == effective.digest
        assert record.binding.selected_folders_observation == ((1, SHOKO_ROOT, "None"),)
        current = controller.current_context(store)
        assert current is not None
        assert (
            controller.onboarding(store)
            .evaluate_context(current, iso="2026-10-26T00:00:00Z")
            .status.value
            == "authorized"
        )
        folders[3] = replace(folders[3], path="/renamed-manual")
        current = controller.current_context(store)
        assert current is not None
        assert (
            controller.onboarding(store)
            .evaluate_context(current, iso="2026-10-26T00:00:00Z")
            .status.value
            == "authorized"
        )
    assert controller.refresh_view() is None


@pytest.mark.parametrize("hazard", ["FileQualityFilterEnabled", "MyList_AddFiles"])
def test_mixed_library_hard_settings_remain_blocked(
    mixed_library: tuple[ServiceSetupController, dict[int, ImportFolder], list[SafetyAssessment]],
    hazard: str,
) -> None:
    controller, _folders, raw = mixed_library
    raw[0] = assess_settings_payload(
        {"AniDb": {hazard: True}} if hazard == "MyList_AddFiles" else {hazard: True}
    )
    view = controller.refresh_view()
    assert view is not None
    assert not view.confirmation_enabled
    assert hazard in view.message


@pytest.mark.parametrize("hazard", [None, "FileQualityFilterEnabled", "MyList_AddFiles"])
@pytest.mark.parametrize("outage", ["unavailable", "unknown", "unusable"])
def test_folder_outage_cannot_hide_observed_independent_hazard(
    mixed_library: tuple[ServiceSetupController, dict[int, ImportFolder], list[SafetyAssessment]],
    monkeypatch: pytest.MonkeyPatch,
    hazard: str | None,
    outage: str,
) -> None:
    controller, folders, raw = mixed_library
    view = controller.refresh_view()
    assert view is not None
    assert "Setup saved." in controller.action("confirm", _consent_form(view))
    safe_settings = raw[0]
    original_folder = folders[1]
    if hazard is not None:
        raw[0] = assess_settings_payload(
            {
                "FileQualityFilterEnabled": hazard == "FileQualityFilterEnabled",
                "AniDb": {"MyList_AddFiles": hazard == "MyList_AddFiles"},
                "Plugins": {
                    "Renamer": {
                        "MoveOnImport": True,
                        "RenameOnImport": True,
                        "RelocateOnImport": True,
                    }
                },
            }
        )
    if outage == "unavailable":
        monkeypatch.setattr(service_setup, "collect_import_folders", lambda *_: None)
    elif outage == "unknown":
        folders[1] = replace(original_folder, drop_folder_type="Unknown")
    else:
        folders[1] = replace(original_folder, path_usable=False)
    with open_state(controller.state_path) as store:
        assert controller.current_context(store) is None
        manager = controller.onboarding(store)
        assert manager.reconfirmation_required() is (hazard is not None)
        expected_reason = "unsafe_shoko_settings" if hazard else "setup_unavailable"
        assert store.get_service_pause() == (True, expected_reason)

    raw[0] = safe_settings
    folders[1] = original_folder
    monkeypatch.setattr(service_setup, "collect_import_folders", lambda *_: tuple(folders.values()))
    restarted = ServiceSetupController(
        controller.bootstrap, controller.sonarr, controller.shoko, controller.state_path
    )
    monkeypatch.setattr(restarted, "_environment", controller._environment)
    with open_state(controller.state_path) as store:
        current = restarted.current_context(store)
        assert current is not None
        manager = restarted.onboarding(store)
        decision = manager.resume_context(current, iso="2026-10-26T00:00:00Z")
        assert decision.status.value == ("needs_confirmation" if hazard else "authorized")
        assert manager.reconfirmation_required() is (hazard is not None)
    if hazard:
        view = controller.refresh_view()
        assert view is not None
        assert "Setup saved." in controller.action("confirm", _consent_form(view))
        with open_state(controller.state_path) as store:
            assert not controller.onboarding(store).reconfirmation_required()


@pytest.mark.parametrize("outage", ["unavailable", "unknown", "unusable"])
@pytest.mark.parametrize("initial", [True, False])
@pytest.mark.parametrize("partial", [True, False])
def test_observed_settings_drift_survives_folder_outage_and_revert(
    mixed_library: tuple[ServiceSetupController, dict[int, ImportFolder], list[SafetyAssessment]],
    monkeypatch: pytest.MonkeyPatch,
    outage: str,
    initial: bool,
    partial: bool,
) -> None:
    controller, folders, raw = mixed_library

    def settings(value: bool, *, partial: bool = False) -> SafetyAssessment:
        return assess_settings_payload(
            {
                "FileQualityFilterEnabled": False,
                "AniDb": {"MyList_AddFiles": False},
                "Plugins": {
                    "Renamer": {
                        "RenameOnImport": value,
                        **({} if partial else {"MoveOnImport": True, "RelocateOnImport": True}),
                    }
                },
            }
        )

    raw[0] = settings(initial)
    view = controller.refresh_view()
    assert view is not None
    assert "Setup saved." in controller.action("confirm", _consent_form(view))
    baseline = raw[0]
    original_folder = folders[1]
    raw[0] = settings(not initial, partial=partial)
    if outage == "unavailable":
        monkeypatch.setattr(service_setup, "collect_import_folders", lambda *_: None)
    elif outage == "unknown":
        folders[1] = replace(original_folder, drop_folder_type="Unknown")
    else:
        folders[1] = replace(original_folder, path_usable=False)
    with open_state(controller.state_path) as store:
        assert controller.current_context(store) is None
        assert controller.onboarding(store).reconfirmation_required()
        assert store.get_service_pause() == (True, "setup_changed")
    raw[0] = baseline
    folders[1] = original_folder
    monkeypatch.setattr(service_setup, "collect_import_folders", lambda *_: tuple(folders.values()))
    restarted = ServiceSetupController(
        controller.bootstrap, controller.sonarr, controller.shoko, controller.state_path
    )
    monkeypatch.setattr(restarted, "_environment", controller._environment)
    with open_state(controller.state_path) as store:
        current = restarted.current_context(store)
        assert current is not None
        assert (
            restarted.onboarding(store)
            .resume_context(current, iso="2026-10-26T00:00:00Z")
            .status.value
            == "needs_confirmation"
        )
    view = controller.refresh_view()
    assert view is not None
    assert "Setup saved." in controller.action("confirm", _consent_form(view))
    with open_state(controller.state_path) as store:
        current = restarted.current_context(store)
        assert current is not None
        assert (
            restarted.onboarding(store)
            .resume_context(current, iso="2026-10-26T00:00:00Z")
            .status.value
            == "authorized"
        )


@pytest.mark.parametrize("drift", ["sonarr_version", "shoko_version", "capabilities", "roots"])
def test_independent_scope_drift_is_not_lost_during_folder_outage(
    mixed_library: tuple[ServiceSetupController, dict[int, ImportFolder], list[SafetyAssessment]],
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    controller, _folders, _raw = mixed_library
    view = controller.refresh_view()
    assert view is not None
    assert "Setup saved." in controller.action("confirm", _consent_form(view))
    compatibility, scope, assessment = controller._environment()
    if drift == "roots":
        scope = replace(scope, candidates=())
    elif drift == "capabilities":
        compatibility = replace(compatibility, capabilities=("new_capability",))
    elif drift == "sonarr_version":
        compatibility = replace(compatibility, sonarr_version="99.0.0")
    else:
        compatibility = replace(compatibility, shoko_version="99.0.0")
    monkeypatch.setattr(controller, "_environment", lambda: (compatibility, scope, assessment))
    monkeypatch.setattr(service_setup, "collect_import_folders", lambda *_: None)
    with open_state(controller.state_path) as store:
        assert controller.current_context(store) is None
        assert controller.onboarding(store).reconfirmation_required()
        assert store.get_service_pause() == (True, "setup_changed")


@pytest.mark.parametrize("baseline", ["settings", "folders"])
def test_older_scoped_consent_without_settings_baseline_requires_confirmation(
    mixed_library: tuple[ServiceSetupController, dict[int, ImportFolder], list[SafetyAssessment]],
    monkeypatch: pytest.MonkeyPatch,
    baseline: str,
) -> None:
    controller, _folders, _raw = mixed_library
    view = controller.refresh_view()
    assert view is not None
    assert "Setup saved." in controller.action("confirm", _consent_form(view))
    with open_state(controller.state_path) as store:
        record = controller.onboarding(store).record()
        assert record is not None
        legacy_binding = (
            replace(record.binding, settings_observation=None)
            if baseline == "settings"
            else replace(record.binding, selected_folders_observation=None)
        )
        legacy = replace(record, binding=legacy_binding, binding_digest=legacy_binding.digest)
        store.save_onboarding_json(legacy.to_json(), "2026-10-26T00:00:00Z")
        monkeypatch.setattr(service_setup, "collect_import_folders", lambda *_: None)
        assert controller.current_context(store) is None
        assert controller.onboarding(store).reconfirmation_required()


@pytest.mark.parametrize("outage", ["unknown", "unusable"])
@pytest.mark.parametrize("drift", [None, "Source", "Destination", "id", "path", "overlap"])
def test_known_selected_folder_drift_survives_unrelated_folder_outage(
    mixed_library: tuple[ServiceSetupController, dict[int, ImportFolder], list[SafetyAssessment]],
    monkeypatch: pytest.MonkeyPatch,
    outage: str,
    drift: str | None,
) -> None:
    controller, folders, _raw = mixed_library
    view = controller.refresh_view()
    assert view is not None
    assert "Setup saved." in controller.action("confirm", _consent_form(view))
    original = dict(folders)
    if drift in {"Source", "Destination"}:
        folders[1] = replace(folders[1], drop_folder_type=drift)
    elif drift == "id":
        folders[7] = replace(folders.pop(1), id=7)
    elif drift == "path":
        folders[1] = replace(folders[1], path="/changed")
    elif drift == "overlap":
        folders[8] = ImportFolder(8, "Overlapping folder", "/anime/nested", "Source", True)
    if outage == "unknown":
        folders[2] = replace(folders[2], drop_folder_type="Unknown")
    else:
        folders[2] = replace(folders[2], path_usable=False)
    with open_state(controller.state_path) as store:
        assert controller.current_context(store) is None
        assert controller.onboarding(store).reconfirmation_required() is (drift is not None)
        expected = "setup_changed" if drift else "setup_unavailable"
        assert store.get_service_pause() == (True, expected)
    folders.clear()
    folders.update(original)
    restarted = ServiceSetupController(
        controller.bootstrap, controller.sonarr, controller.shoko, controller.state_path
    )
    monkeypatch.setattr(restarted, "_environment", controller._environment)
    with open_state(controller.state_path) as store:
        current = restarted.current_context(store)
        assert current is not None
        decision = restarted.onboarding(store).resume_context(current, iso="2026-10-26T00:00:00Z")
        assert decision.status.value == ("needs_confirmation" if drift else "authorized")
    if drift:
        view = controller.refresh_view()
        assert view is not None
        assert "Setup saved." in controller.action("confirm", _consent_form(view))
        with open_state(controller.state_path) as store:
            current = restarted.current_context(store)
            assert current is not None
            assert (
                restarted.onboarding(store)
                .resume_context(current, iso="2026-10-26T00:00:00Z")
                .status.value
                == "authorized"
            )


@pytest.mark.parametrize("boundary", ["view", "confirm", "scope"])
@pytest.mark.parametrize("hazard", ["hard_setting", "selected_folder"])
def test_setup_observation_boundaries_durably_invalidate_saved_consent(
    mixed_library: tuple[ServiceSetupController, dict[int, ImportFolder], list[SafetyAssessment]],
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
    hazard: str,
) -> None:
    controller, folders, raw = mixed_library
    view = controller.refresh_view()
    assert view is not None
    assert "Setup saved." in controller.action("confirm", _consent_form(view))
    controller._editing_scope = True
    view = controller.refresh_view()
    assert view is not None
    original = raw[0], folders[1]
    if hazard == "hard_setting":
        raw[0] = assess_settings_payload(
            {
                "FileQualityFilterEnabled": True,
                "AniDb": {"MyList_AddFiles": False},
                "Plugins": {
                    "Renamer": {
                        name: raw[0].observed_settings[name]
                        for name in ("MoveOnImport", "RenameOnImport", "RelocateOnImport")
                    }
                },
            }
        )
    else:
        folders[1] = replace(folders[1], drop_folder_type="Source")
    if boundary == "view":
        assert controller.refresh_view() is not None
    elif boundary == "confirm":
        assert "Setup was not saved." in controller.action("confirm", _consent_form(view))
    else:
        result = controller.action(
            "scope", {"setup_digest": view.snapshot_digest, "root_0": ANIME_ROOT}
        )
        if hazard == "hard_setting":
            assert "Setup was not saved." in result
    with open_state(controller.state_path) as store:
        assert controller.onboarding(store).reconfirmation_required()
    raw[0], folders[1] = original
    restarted = ServiceSetupController(
        controller.bootstrap, controller.sonarr, controller.shoko, controller.state_path
    )
    monkeypatch.setattr(restarted, "_environment", controller._environment)
    with open_state(controller.state_path) as store:
        current = restarted.current_context(store)
        assert current is not None
        assert (
            restarted.onboarding(store)
            .resume_context(current, iso="2026-10-26T00:00:00Z")
            .status.value
            == "needs_confirmation"
        )


@pytest.mark.parametrize("interleave", ["new_confirmation", "user_pause"])
@pytest.mark.parametrize("observation", ["settings", "folders"])
def test_observation_respects_new_consent_and_preserves_user_pause(
    mixed_library: tuple[ServiceSetupController, dict[int, ImportFolder], list[SafetyAssessment]],
    monkeypatch: pytest.MonkeyPatch,
    interleave: str,
    observation: str,
) -> None:
    controller, folders, raw = mixed_library
    view = controller.refresh_view()
    assert view is not None
    assert "Setup saved." in controller.action("confirm", _consent_form(view))
    with open_state(controller.state_path) as store:
        approved = controller.onboarding(store).record()
    assert approved is not None
    environment = controller._environment()
    scoped = evaluate_service_safety(raw[0], approved.binding.path_mappings, folders)

    def mutate_consent() -> None:
        # A different controller/store acts while the older worker's I/O is in flight.
        other = ServiceSetupController(
            controller.bootstrap, controller.sonarr, controller.shoko, controller.state_path
        )
        with open_state(controller.state_path) as store:
            manager = other.onboarding(store)
            if interleave == "new_confirmation":
                manager.confirm_setup(
                    approved.binding,
                    scoped,
                    confirmed_facts=approved.confirmed_facts,
                    automatic_link_consent=True,
                    iso="2026-10-27T00:00:00Z",
                )
            else:
                manager.pause(iso="2026-10-27T00:00:00Z")

    if observation == "settings":

        def late_environment() -> tuple[ApiCompatibility, AnimeScopeDiscovery, SafetyAssessment]:
            mutate_consent()
            monkeypatch.setattr(controller, "_environment", lambda: environment)
            hazard = assess_settings_payload({"FileQualityFilterEnabled": True})
            return *environment[:2], hazard

        monkeypatch.setattr(controller, "_environment", late_environment)
    else:

        def late_folders(*_: object) -> tuple[ImportFolder, ...]:
            mutate_consent()
            monkeypatch.setattr(
                service_setup, "collect_import_folders", lambda *_: tuple(folders.values())
            )
            return tuple(
                replace(folder, drop_folder_type="Source") if folder.id == 1 else folder
                for folder in folders.values()
            )

        monkeypatch.setattr(service_setup, "collect_import_folders", late_folders)
    controller.refresh_view()
    with open_state(controller.state_path) as store:
        manager = controller.onboarding(store)
        assert manager.reconfirmation_required() is (interleave == "user_pause")
        assert store.get_service_pause() == (
            (True, "user_paused") if interleave == "user_pause" else (False, "")
        )
        current = controller.current_context(store)
        assert current is not None
        assert manager.resume_context(current, iso="2026-10-27T01:00:00Z").status.value == (
            "needs_confirmation" if interleave == "user_pause" else "authorized"
        )


def test_draft_scope_preview_does_not_invalidate_unchanged_approved_scope(
    mixed_library: tuple[ServiceSetupController, dict[int, ImportFolder], list[SafetyAssessment]],
) -> None:
    controller, _folders, _raw = mixed_library
    view = controller.refresh_view()
    assert view is not None
    assert "Setup saved." in controller.action("confirm", _consent_form(view))
    controller._editing_scope = True
    controller._selected_roots = ("/proposed-unconfirmed-root",)
    controller._selected_mappings = (PathMapping("/proposed-unconfirmed-root", "/drop"),)
    assert controller.refresh_view() is not None
    with open_state(controller.state_path) as store:
        assert not controller.onboarding(store).reconfirmation_required()
        assert store.get_service_pause() == (False, "")


@pytest.mark.parametrize(
    "observed", ["changed_type", "missing_ids", "overlap_source", "overlap_none"]
)
def test_fresh_partial_inventory_only_latches_positive_folder_facts(
    mixed_library: tuple[ServiceSetupController, dict[int, ImportFolder], list[SafetyAssessment]],
    monkeypatch: pytest.MonkeyPatch,
    observed: str,
) -> None:
    controller, folders, _raw = mixed_library
    view = controller.refresh_view()
    assert view is not None
    assert "Setup saved." in controller.action("confirm", _consent_form(view))
    inventory_reader = controller._inventory

    def partial_inventory(
        roots: tuple[str, ...],
    ) -> tuple[SonarrInventory, ShokoDiscovery, PathMappingDiscovery]:
        inventory, discovery, mappings = inventory_reader(roots)
        known: dict[int, ImportFolder] = {}
        if observed == "changed_type":
            known = {1: replace(folders[1], drop_folder_type="Source")}
        elif observed.startswith("overlap_"):
            known = {
                8: ImportFolder(
                    8,
                    "Overlapping owner",
                    "/anime/nested",
                    "Source" if observed == "overlap_source" else "None",
                    True,
                )
            }
        return (
            inventory,
            replace(discovery, folders=known, verdict=ShokoVerdict(False, (), ())),
            mappings,
        )

    controller._inventory_cache = None
    monkeypatch.setattr(controller, "_inventory", partial_inventory)
    controller.refresh_view()
    with open_state(controller.state_path) as store:
        assert controller.onboarding(store).reconfirmation_required() is (observed != "missing_ids")
    restarted = ServiceSetupController(
        controller.bootstrap, controller.sonarr, controller.shoko, controller.state_path
    )
    monkeypatch.setattr(restarted, "_environment", controller._environment)
    with open_state(controller.state_path) as store:
        current = restarted.current_context(store)
        assert current is not None
        assert restarted.onboarding(store).resume_context(
            current, iso="2026-10-27T01:00:00Z"
        ).status.value == ("authorized" if observed == "missing_ids" else "needs_confirmation")


@pytest.mark.parametrize("new_folder_drift", [False, True])
def test_current_context_does_not_mix_old_settings_with_new_consent(
    mixed_library: tuple[ServiceSetupController, dict[int, ImportFolder], list[SafetyAssessment]],
    monkeypatch: pytest.MonkeyPatch,
    new_folder_drift: bool,
) -> None:
    controller, folders, raw = mixed_library
    view = controller.refresh_view()
    assert view is not None
    assert "Setup saved." in controller.action("confirm", _consent_form(view))
    old_environment = controller._environment()
    with open_state(controller.state_path) as store:
        old = controller.onboarding(store).record()
    assert old is not None
    original_environment = controller._environment

    def consent_during_old_read() -> tuple[ApiCompatibility, AnimeScopeDiscovery, SafetyAssessment]:
        raw[0] = assess_settings_payload(
            {
                "FileQualityFilterEnabled": False,
                "AniDb": {"MyList_AddFiles": False},
                "Plugins": {
                    "Renamer": {
                        "MoveOnImport": False,
                        "RenameOnImport": False,
                        "RelocateOnImport": False,
                    }
                },
            }
        )
        fresh = evaluate_service_safety(raw[0], old.binding.path_mappings, folders)
        binding = replace(
            old.binding,
            safety_digest=fresh.digest,
            settings_observation=tuple(fresh.observed_settings.items()),
        )
        with open_state(controller.state_path) as other:
            controller.onboarding(other).confirm_setup(
                binding,
                fresh,
                confirmed_facts=old.confirmed_facts,
                automatic_link_consent=True,
                iso="2026-10-27T00:00:00Z",
            )
        if new_folder_drift:
            folders[1] = replace(folders[1], drop_folder_type="Source")
        monkeypatch.setattr(controller, "_environment", original_environment)
        return old_environment

    monkeypatch.setattr(controller, "_environment", consent_during_old_read)
    with open_state(controller.state_path) as store:
        assert controller.current_context(store) is None
        manager = controller.onboarding(store)
        assert manager.reconfirmation_required() is new_folder_drift
        record = manager.record()
        assert record is not None
        assert record.consented_iso == "2026-10-27T00:00:00Z"
        if new_folder_drift:
            folders[1] = replace(folders[1], drop_folder_type="None")
        current = controller.current_context(store)
        assert current is not None
        assert manager.resume_context(current, iso="2026-10-27T01:00:00Z").status.value == (
            "needs_confirmation" if new_folder_drift else "authorized"
        )


@pytest.mark.parametrize("drift", ["id", "path", "type", "ambiguous", "unusable", "missing"])
def test_selected_folder_drift_rejects_fresh_submit_and_runtime_authorization(
    mixed_library: tuple[ServiceSetupController, dict[int, ImportFolder], list[SafetyAssessment]],
    drift: str,
) -> None:
    controller, folders, _raw = mixed_library
    view = controller.refresh_view()
    assert view is not None
    form = _consent_form(view)
    assert "Setup saved." in controller.action("confirm", form)
    controller.action("edit", {})
    view = controller.refresh_view()
    assert view is not None
    assert view.confirmation_enabled
    form = _consent_form(view)
    if drift == "id":
        folders[7] = replace(folders.pop(1), id=7)
    elif drift == "path":
        folders[1] = replace(folders[1], path="/new-anime")
    elif drift == "type":
        folders[1] = replace(folders[1], drop_folder_type="Source")
    elif drift == "ambiguous":
        folders[7] = ImportFolder(7, "Alias", SHOKO_ROOT + "/nested", "None", True)
    elif drift == "unusable":
        folders[1] = replace(folders[1], path_usable=False)
    else:
        del folders[1]
    assert "Setup was not saved." in controller.action("confirm", form)
    with open_state(controller.state_path) as store:
        current = controller.current_context(store)
        if current is not None:
            decision = controller.onboarding(store).evaluate_context(
                current, iso="2026-10-26T00:00:00Z"
            )
            assert decision.status.value == "needs_confirmation"
        assert store.get_service_pause()[0]


@pytest.mark.parametrize("available", [True, False])
def test_unobservable_settings_show_truthful_scoped_checkbox_facts(
    mixed_library: tuple[ServiceSetupController, dict[int, ImportFolder], list[SafetyAssessment]],
    available: bool,
) -> None:
    controller, _folders, raw = mixed_library
    raw[0] = assess_settings_payload({} if available else None)
    view = controller.refresh_view()
    assert view is not None
    assert view.confirmation_enabled
    rendered = DashboardApplication._setup_section(view, "csrf-token")
    assert "FileQualityFilterEnabled is disabled in Shoko" in view.required_confirmations
    assert "MyList_AddFiles is disabled in Shoko" in view.required_confirmations
    assert "FileQualityFilterEnabled is disabled in Shoko</label>" in rendered
    assert "MyList_AddFiles is disabled in Shoko</label>" in rendered
    for setting in ("MoveOnImport", "RenameOnImport", "RelocateOnImport"):
        assert any(
            fact.startswith(setting + " cannot be inspected;") and "excluded" in fact
            for fact in view.required_confirmations
        )
        assert not any(setting + " is disabled" in fact for fact in view.required_confirmations)
        assert setting + " is disabled" not in rendered
        assert setting + " cannot be inspected;" in rendered
    assert "Setup saved." in controller.action("confirm", _consent_form(view))
    with open_state(controller.state_path) as store:
        current = controller.current_context(store)
        assert current is not None
        assert (
            controller.onboarding(store)
            .evaluate_context(current, iso="2026-10-26T00:00:00Z")
            .status.value
            == "authorized"
        )


def test_raw_settings_consent_requires_new_scope_policy_confirmation(
    mixed_library: tuple[ServiceSetupController, dict[int, ImportFolder], list[SafetyAssessment]],
) -> None:
    controller, _folders, raw = mixed_library
    raw[0] = assess_settings_payload({"FileQualityFilterEnabled": False})
    with open_state(controller.state_path) as store:
        manager = controller.onboarding(store)
        manager.confirm_setup(
            ScopeBinding(
                controller.bootstrap.sonarr_url,
                controller.bootstrap.shoko_url,
                "4.0.0",
                "5.3.3",
                (),
                (ANIME_ROOT,),
                (PathMapping(ANIME_ROOT, SHOKO_ROOT),),
                raw[0].digest,
                raw[0].unverifiable_facts,
            ),
            raw[0],
            confirmed_facts=raw[0].unknown_settings + raw[0].unverifiable_facts,
            automatic_link_consent=True,
            iso="2026-10-26T00:00:00Z",
        )
        assert controller.current_context(store) is None
        assert manager.reconfirmation_required()
    view = controller.refresh_view()
    assert view is not None
    assert view.confirmation_enabled
    assert "Setup saved." in controller.action("confirm", _consent_form(view))


def test_unknown_unrelated_folder_type_keeps_setup_read_only(
    mixed_library: tuple[ServiceSetupController, dict[int, ImportFolder], list[SafetyAssessment]],
) -> None:
    controller, folders, _raw = mixed_library
    view = controller.refresh_view()
    assert view is not None
    form = _consent_form(view)
    folders[2] = replace(folders[2], drop_folder_type="Unknown")
    assert "Setup was not saved." in controller.action("confirm", form)
    with open_state(controller.state_path) as store:
        assert controller.onboarding(store).record() is None
    view = controller.refresh_view()
    assert view is not None
    assert not view.confirmation_enabled


def test_manual_mapping_filters_ambiguous_options_without_blocking_independent_safe_choice(
    mixed_library: tuple[ServiceSetupController, dict[int, ImportFolder], list[SafetyAssessment]],
) -> None:
    controller, folders, raw = mixed_library
    raw[0] = assess_settings_payload({"FileQualityFilterEnabled": False})
    folders[3] = replace(folders[3], drop_folder_type="None")
    folders[4] = ImportFolder(4, "Nested manual drop", "/manual/drop", "Source", True)
    view = controller.refresh_view()
    assert view is not None
    assert view.mapping_required
    assert view.confirmation_enabled
    assert view.mapping_options == ((ANIME_ROOT, SHOKO_ROOT),)
    assert any("cannot be inspected;" in fact for fact in view.required_confirmations)
    rendered = DashboardApplication._setup_section(view, "csrf-token")
    assert f"{ANIME_ROOT} → /manual</option>" not in rendered

    forged = _consent_form(view)
    forged["mapping_0"] = "1"
    assert "Setup was not saved." in controller.action("confirm", forged)
    with open_state(controller.state_path) as store:
        assert controller.onboarding(store).record() is None

    view = controller.refresh_view()
    assert view is not None
    assert "Setup saved." in controller.action("confirm", _consent_form(view))
    with open_state(controller.state_path) as store:
        record = controller.onboarding(store).record()
        assert record is not None
        assert record.binding.path_mappings == (PathMapping(ANIME_ROOT, SHOKO_ROOT),)
        folders[4] = replace(folders[4], path="/manual/other-drop")
        current = controller.current_context(store)
        assert current is not None
        decision = controller.onboarding(store).evaluate_context(
            current, iso="2026-10-26T00:00:00Z"
        )
        assert decision.status.value == "authorized"


def test_status_polling_reuses_inventory_until_folders_change_or_refresh_is_due(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, _form, _environment, folder_paths, clock, sonarr, shoko = _ready_setup_form(
        tmp_path, monkeypatch
    )
    original_inventory = controller._inventory
    reads = 0

    def counted_inventory(
        roots: tuple[str, ...],
    ) -> tuple[SonarrInventory, ShokoDiscovery, PathMappingDiscovery]:
        nonlocal reads
        reads += 1
        return original_inventory(roots)

    monkeypatch.setattr(controller, "_inventory", counted_inventory)
    controller._inventory_cache = None
    try:
        assert controller.refresh_view() is not None
        for _ in range(4):
            clock[0] += service_setup.SETUP_VIEW_CACHE_SECONDS + 1
            view = controller.refresh_view()
            assert view is not None
            assert view.progress_percent == 100
        assert reads == 1
        folder_paths["value"].append("/new-anime")
        assert controller.refresh_view() is not None
        assert reads == 2
        clock[0] += service_setup.SETUP_INVENTORY_CACHE_SECONDS + 1
        assert controller.refresh_view() is not None
        assert reads == 3
    finally:
        sonarr.client.close()
        shoko.client.close()


def test_confirmed_setup_stays_hidden_during_repeated_background_refreshes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, form, _environment, _folders, clock, sonarr, shoko = _ready_setup_form(
        tmp_path, monkeypatch
    )
    compute = controller._compute_view
    started = threading.Event()
    release = threading.Event()
    inventory = controller._inventory
    reads = 0

    def counted_inventory(
        roots: tuple[str, ...],
    ) -> tuple[SonarrInventory, ShokoDiscovery, PathMappingDiscovery]:
        nonlocal reads
        reads += 1
        return inventory(roots)

    def blocked_compute(
        roots: tuple[str, ...], mappings: tuple[PathMapping, ...]
    ) -> tuple[
        DashboardSetupView | None, tuple[ApiCompatibility, AnimeScopeDiscovery, SafetyAssessment]
    ]:
        started.set()
        assert release.wait(timeout=5)
        return compute(roots, mappings)

    monkeypatch.setattr(controller, "_inventory", counted_inventory)
    monkeypatch.setattr(controller, "_compute_view", blocked_compute)
    controller._inventory_cache = None
    try:
        assert controller.action("confirm", form).startswith("Setup saved.")
        for _ in range(3):
            started.clear()
            release.clear()
            assert controller.view() is None
            assert started.wait(timeout=2)
            worker = next(
                thread
                for thread in threading.enumerate()
                if thread.name == "shoko-linker-setup-discovery"
            )
            try:
                for _poll in range(10):
                    assert controller.view() is None
            finally:
                release.set()
                worker.join(timeout=5)
            assert not worker.is_alive()
            assert controller.view() is None
            clock[0] += service_setup.SETUP_VIEW_CACHE_SECONDS + 1
        assert reads == 1  # The inventory cache survives all three view-cache expirations.
        # Explicit editing still opens discovery and choices rather than keeping them hidden.
        controller.action("edit", {})
        started.clear()
        release.clear()
        loading = controller.view()
        assert loading is not None
        assert loading.loading
        assert started.wait(timeout=2)
        worker = next(
            thread
            for thread in threading.enumerate()
            if thread.name == "shoko-linker-setup-discovery"
        )
        release.set()
        worker.join(timeout=5)
        choices = controller.view()
        assert choices is not None
        assert not choices.loading
    finally:
        release.set()
        sonarr.client.close()
        shoko.client.close()


@pytest.mark.parametrize("drift", ["version", "settings", "folder"])
def test_hidden_setup_refresh_publishes_real_drift_and_keeps_reconfirmation_hold(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, drift: str
) -> None:
    controller, form, environment, folders, clock, sonarr, shoko = _ready_setup_form(
        tmp_path, monkeypatch
    )
    try:
        assert controller.action("confirm", form).startswith("Setup saved.")
        assert controller.refresh_view() is None
        compatibility, scope, assessment = environment
        if drift == "version":
            compatibility = replace(compatibility, shoko_version="5.3.3.0")
        elif drift == "settings":
            assessment = assess_settings_payload({"FileQualityFilterEnabled": True})
        else:
            folders["value"] = ["/changed-anime"]
        monkeypatch.setattr(controller, "_environment", lambda: (compatibility, scope, assessment))
        finished = threading.Event()
        refresh = controller._refresh_in_background

        def tracked_refresh() -> None:
            try:
                refresh()
            finally:
                finished.set()

        monkeypatch.setattr(controller, "_refresh_in_background", tracked_refresh)
        clock[0] += service_setup.SETUP_VIEW_CACHE_SECONDS + 1
        assert controller.view() is None
        assert finished.wait(timeout=5)
        view = controller.view()
        assert view is not None
        assert not view.loading
        with open_state(controller.state_path) as store:
            assert controller.onboarding(store).reconfirmation_required()
            assert store.get_service_pause()[0]
        # Restoring the old facts must not erase the already observed safety change.
        folders["value"] = [SHOKO_ROOT]
        monkeypatch.setattr(controller, "_environment", lambda: environment)
        assert controller.refresh_view() is not None
        with open_state(controller.state_path) as store:
            assert controller.onboarding(store).reconfirmation_required()
    finally:
        sonarr.client.close()
        shoko.client.close()


def test_setup_controller_requires_explicit_scope_consent_and_all_unknown_safety_facts(  # noqa: PLR0915
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    compatibility = ApiCompatibility("4.0.0", "5.3.3", True, True, (), ())
    scope = AnimeScopeDiscovery(
        ScopeStatus.CONFIRM_SCOPE,
        (AnimeRootCandidate(ANIME_ROOT, (41,)),),
        "Confirm the Sonarr anime folder.",
    )
    path_discovery = PathMappingDiscovery(
        MappingDiscoveryStatus.READY,
        (PathMapping(ANIME_ROOT, SHOKO_ROOT),),
        (),
        "Exact file paths and sizes establish this folder match.",
    )
    assessment = assess_settings_payload({"FileQualityFilterEnabled": False})
    current_assessment = {"value": assessment}
    inventory = SonarrInventory((), (), (), (), (), (), (), InventoryVerdict(True, True, (), ()))
    discovery = ShokoDiscovery(
        (),
        {1: ImportFolder(1, "Anime", SHOKO_ROOT, "None", True)},
        ShokoVerdict(True, (), ()),
        1,
        0,
        1000,
    )
    monkeypatch.setattr(service_setup, "inspect_api_compatibility", lambda *_: compatibility)
    monkeypatch.setattr(service_setup, "discover_sonarr_scope", lambda *_: scope)
    monkeypatch.setattr(
        service_setup,
        "inspect_shoko_safety_settings",
        lambda *_: current_assessment["value"],
    )
    inventory_calls = 0

    def collect_inventory(*_: object, **__: object) -> SonarrInventory:
        nonlocal inventory_calls
        inventory_calls += 1
        return inventory

    monkeypatch.setattr(service_setup, "collect_sonarr_inventory", collect_inventory)
    monkeypatch.setattr(service_setup, "collect_shoko_discovery", lambda *_, **__: discovery)
    monkeypatch.setattr(service_setup, "infer_path_mappings", lambda *_: path_discovery)
    monkeypatch.setattr(
        service_setup, "collect_import_folders", lambda *_: tuple(discovery.folders.values())
    )

    sonarr = ApiEndpoint(httpx.Client(), lambda: {})
    shoko = ApiEndpoint(httpx.Client(), lambda: {})
    bootstrap = BootstrapSettings(
        "http://sonarr:8989",
        SecretRef("SONARR_API_KEY"),
        "http://shoko:8111",
        SecretRef("SHOKO_API_KEY"),
        cast(Path, PurePosixPath("/data/state.sqlite3")),
    )
    controller = ServiceSetupController(bootstrap, sonarr, shoko, tmp_path / "state.sqlite3")
    try:
        initial = controller.refresh_view()
        assert isinstance(initial, DashboardSetupView)
        assert initial.root_options == (ANIME_ROOT,)
        assert initial.confirmation_enabled is False

        scope = replace(scope, read_status=ScopeReadStatus.INCOMPLETE)
        stale_scope_selection = controller.action(
            "scope", {"setup_digest": initial.snapshot_digest, "root_0": ANIME_ROOT}
        )
        assert "incomplete" in stale_scope_selection.lower()
        incomplete = controller.refresh_view()
        assert incomplete is not None
        incomplete_scope_selection = controller.action(
            "scope", {"setup_digest": incomplete.snapshot_digest, "root_0": ANIME_ROOT}
        )
        assert "incomplete" in incomplete_scope_selection.lower()
        scope = replace(scope, read_status=ScopeReadStatus.COMPLETE)
        assert controller.refresh_view() == initial

        missing_scope_digest = controller.action("scope", {"root_0": ANIME_ROOT})
        assert "choices changed" in missing_scope_digest.lower()
        assert controller.refresh_view() == initial

        assert "selected" in controller.action(
            "scope", {"setup_digest": initial.snapshot_digest, "root_0": ANIME_ROOT}
        )
        selected = controller.refresh_view()
        assert selected is not None
        assert selected.selected_roots == (ANIME_ROOT,)
        assert selected.inferred_mappings == ((ANIME_ROOT, SHOKO_ROOT),)
        assert selected.required_confirmations
        assert selected.confirmation_enabled is True

        missing_confirmation_digest = controller.action("confirm", {"automatic_link_consent": "1"})
        assert "choices changed" in missing_confirmation_digest.lower()
        assert controller.refresh_view() == selected

        denied = controller.action(
            "confirm",
            {
                "setup_digest": selected.snapshot_digest,
                "automatic_link_consent": "1",
            },
        )
        assert "confirm" in denied.lower()
        assert controller.refresh_view() == selected
        with open_state(tmp_path / "state.sqlite3") as store:
            assert store.get_onboarding_json() is None

        form = {
            "setup_digest": selected.snapshot_digest,
            "automatic_link_consent": "1",
        }
        form.update(
            {
                f"confirm_{index}": hashlib.sha256(fact.encode("utf-8")).hexdigest()
                for index, fact in enumerate(selected.required_confirmations)
            }
        )
        current_assessment["value"] = assess_settings_payload(
            {"Plugins": {"Renamer": {"RenameOnImport": False}}}
        )
        controller.refresh_view()
        stale_confirmation = controller.action("confirm", form)
        assert "setup" in stale_confirmation.lower()
        with open_state(tmp_path / "state.sqlite3") as store:
            assert store.get_onboarding_json() is None

        current_assessment["value"] = assessment
        assert controller.refresh_view() == selected
        scope = replace(scope, read_status=ScopeReadStatus.INCOMPLETE)
        incomplete_scope = controller.action("confirm", form)
        assert "setup" in incomplete_scope.lower()
        with open_state(tmp_path / "state.sqlite3") as store:
            assert store.get_onboarding_json() is None

        scope = replace(scope, read_status=ScopeReadStatus.COMPLETE)
        assert controller.refresh_view() == selected
        inventory_calls_before_confirm = inventory_calls
        confirmed = controller.action("confirm", form)
        assert inventory_calls == inventory_calls_before_confirm
        assert "saved" in confirmed.lower()
        with open_state(tmp_path / "state.sqlite3") as store:
            record = controller.onboarding(store).record()
            assert record is not None
            assert record.binding.roots == (ANIME_ROOT,)
            assert record.binding.path_mappings == (PathMapping(ANIME_ROOT, SHOKO_ROOT),)
            assert set(record.confirmed_facts) == set(selected.required_confirmations)
        assert controller.refresh_view() is None

        with open_state(tmp_path / "state.sqlite3") as store:
            onboarding = controller.onboarding(store)
            onboarding.pause_for_review(
                "setup_changed",
                "Services changed; confirm setup again.",
                iso="2026-10-15T00:00:00Z",
            )
            onboarding.pause_for_review(
                "setup_unavailable",
                "Services are temporarily unavailable.",
                iso="2026-10-15T00:00:00Z",
            )
        reconfirmation_view = controller.refresh_view()
        assert reconfirmation_view is not None
        assert reconfirmation_view.confirmation_enabled is True

        # An unrelated safe folder cannot mask a selected folder becoming a drop folder.
        scoped_assessment = evaluate_service_safety(
            assessment, (PathMapping(ANIME_ROOT, SHOKO_ROOT),), discovery.folders
        )
        for mappings in (
            (PathMapping(ANIME_ROOT, SHOKO_ROOT),),
            (
                PathMapping(ANIME_ROOT + "/one", SHOKO_ROOT),
                PathMapping(ANIME_ROOT + "/two", "/other"),
            ),
        ):
            with open_state(tmp_path / "state.sqlite3") as store:
                record = controller.onboarding(store).record()
                assert record is not None
                controller.onboarding(store).confirm_setup(
                    replace(record.binding, path_mappings=mappings),
                    scoped_assessment,
                    confirmed_facts=record.confirmed_facts,
                    automatic_link_consent=True,
                    iso="2026-10-15T00:00:00Z",
                )
                monkeypatch.setattr(
                    service_setup,
                    "collect_import_folders",
                    lambda *_: (
                        ImportFolder(1, "Drop", SHOKO_ROOT, "Source", True),
                        ImportFolder(2, "Other", "/other", "None", True),
                    ),
                )
                assert controller.current_context(store) is None
                assert store.get_service_pause() == (True, "setup_changed")

        current_assessment["value"] = assess_settings_payload({"FileQualityFilterEnabled": True})
        with open_state(tmp_path / "state.sqlite3") as store:
            assert controller.current_context(store) is None
            assert store.get_service_pause() == (True, "unsafe_shoko_settings")

        current_assessment["value"] = assessment
        scope = AnimeScopeDiscovery(ScopeStatus.BLOCKED, (), "Sonarr anime scope changed.")
        with open_state(tmp_path / "state.sqlite3") as store:
            record = controller.onboarding(store).record()
            assert record is not None
            controller.onboarding(store).confirm_setup(
                record.binding,
                scoped_assessment,
                confirmed_facts=record.confirmed_facts,
                automatic_link_consent=True,
                iso="2026-10-15T00:00:00Z",
            )
            assert controller.current_context(store) is None
            assert store.get_service_pause() == (True, "setup_changed")
    finally:
        sonarr.client.close()
        shoko.client.close()


@pytest.mark.parametrize("action", ["scope", "confirm"])
def test_incompatible_api_is_reported_instead_of_false_scope_or_safety_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, action: str
) -> None:
    controller, form, environment, _folders, _clock, sonarr, shoko = _ready_setup_form(
        tmp_path, monkeypatch
    )
    message = "Cannot verify Shoko compatibility. Check its connection and API version."
    unavailable = replace(
        environment[0], shoko_version=None, shoko_supported=False, messages=(message,)
    )
    monkeypatch.setattr(controller, "_environment", lambda: (unavailable, *environment[1:]))
    try:
        result = controller.action(action, {**form, "root_0": ANIME_ROOT})
        assert result == f"Setup was not saved. {message}"
        assert "scope or safety changed" not in result
        with open_state(controller.state_path) as store:
            assert controller.onboarding(store).record() is None
    finally:
        sonarr.client.close()
        shoko.client.close()


def test_setup_confirmation_accepts_form_valid_at_submission_after_slow_revalidation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, form, environment, _folders, clock, sonarr, shoko = _ready_setup_form(
        tmp_path, monkeypatch
    )

    def slow_revalidation() -> tuple[ApiCompatibility, AnimeScopeDiscovery, SafetyAssessment]:
        clock[0] += service_setup.SETUP_VIEW_CACHE_SECONDS + 1
        return environment

    monkeypatch.setattr(controller, "_environment", slow_revalidation)
    try:
        result = controller.action("confirm", form)
        assert result.startswith("Setup saved.")
        with open_state(tmp_path / "state.sqlite3") as store:
            assert controller.onboarding(store).record() is not None
    finally:
        sonarr.client.close()
        shoko.client.close()


def test_scope_selection_accepts_form_valid_at_submission_after_slow_revalidation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, confirm_form, environment, _folders, clock, sonarr, shoko = _ready_setup_form(
        tmp_path, monkeypatch
    )
    form = {"setup_digest": confirm_form["setup_digest"], "root_0": ANIME_ROOT}

    def slow_revalidation() -> tuple[ApiCompatibility, AnimeScopeDiscovery, SafetyAssessment]:
        clock[0] += service_setup.SETUP_VIEW_CACHE_SECONDS + 1
        return environment

    monkeypatch.setattr(controller, "_environment", slow_revalidation)
    try:
        result = controller.action("scope", form)
        assert result.startswith("Anime scope selected.")
    finally:
        sonarr.client.close()
        shoko.client.close()


def test_setup_confirmation_rejects_form_expired_at_submission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, form, environment, _folders, clock, sonarr, shoko = _ready_setup_form(
        tmp_path, monkeypatch
    )
    calls = 0

    def revalidation() -> tuple[ApiCompatibility, AnimeScopeDiscovery, SafetyAssessment]:
        nonlocal calls
        calls += 1
        return environment

    monkeypatch.setattr(controller, "_environment", revalidation)
    clock[0] += service_setup.SETUP_VIEW_CACHE_SECONDS
    try:
        result = controller.action("confirm", form)
        assert "expired" in result.lower()
        assert calls == 0
        with open_state(tmp_path / "state.sqlite3") as store:
            assert controller.onboarding(store).record() is None
    finally:
        sonarr.client.close()
        shoko.client.close()


def test_setup_confirmation_rejects_snapshot_refreshed_during_revalidation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, form, environment, folders, _clock, sonarr, shoko = _ready_setup_form(
        tmp_path, monkeypatch
    )
    revalidating = False

    def revalidation_with_refresh() -> (
        tuple[ApiCompatibility, AnimeScopeDiscovery, SafetyAssessment]
    ):
        nonlocal revalidating
        if not revalidating:
            revalidating = True
            folders["value"] = ["/aaa", SHOKO_ROOT]
            changed_view = controller.refresh_view()
            assert changed_view is not None
            assert changed_view.snapshot_digest != form["setup_digest"]
            folders["value"] = [SHOKO_ROOT]
            restored_view = controller.refresh_view()
            assert restored_view is not None
            assert restored_view.snapshot_digest == form["setup_digest"]
        return environment

    monkeypatch.setattr(controller, "_environment", revalidation_with_refresh)
    try:
        result = controller.action("confirm", form)
        assert "choices changed during this request" in result.lower()
        with open_state(tmp_path / "state.sqlite3") as store:
            assert controller.onboarding(store).record() is None
    finally:
        sonarr.client.close()
        shoko.client.close()


def test_setup_confirmation_rejects_refresh_in_flight_before_persistence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, form, environment, _folders, _clock, sonarr, shoko = _ready_setup_form(
        tmp_path, monkeypatch
    )
    main_thread = threading.current_thread()
    refresh_started = threading.Event()
    release_refresh = threading.Event()
    refresh_finished = threading.Event()
    pause_refresh = True

    def run_refresh() -> None:
        controller.refresh_view()
        refresh_finished.set()

    refresh_thread = threading.Thread(target=run_refresh)

    def current_environment() -> tuple[ApiCompatibility, AnimeScopeDiscovery, SafetyAssessment]:
        nonlocal pause_refresh
        if threading.current_thread() is main_thread:
            refresh_thread.start()
            assert refresh_started.wait(timeout=3)
        elif pause_refresh:
            pause_refresh = False
            refresh_started.set()
            assert release_refresh.wait(timeout=3)
        return environment

    monkeypatch.setattr(controller, "_environment", current_environment)
    try:
        result = controller.action("confirm", form)
        assert "choices changed during this request" in result.lower()
        with open_state(tmp_path / "state.sqlite3") as store:
            assert controller.onboarding(store).record() is None
    finally:
        release_refresh.set()
        refresh_thread.join(timeout=5)
        sonarr.client.close()
        shoko.client.close()
    assert refresh_finished.is_set()


@pytest.mark.parametrize(
    "failure",
    [
        "scope_unavailable",
        "scope_incomplete",
        "folders_unavailable",
        "safety_unavailable",
        "safety_incomplete",
        "scope_missing_root",
        "folders_empty",
    ],
)
def test_setup_read_failures_are_temporary_but_complete_scope_drift_requires_reconfirmation(  # noqa: PLR0915 - read-failure recovery matrix
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    assessment = assess_settings_payload({"FileQualityFilterEnabled": False})
    assessment_state = {"value": assessment}
    compatibility = ApiCompatibility("4.0.0", "5.3.3", True, True, (), ())
    compatibility_state = {"value": compatibility}
    scope = AnimeScopeDiscovery(
        ScopeStatus.CONFIRM_SCOPE,
        (AnimeRootCandidate(ANIME_ROOT, (41,)),),
        "Confirm the Sonarr anime folder.",
    )
    scope_state = {"value": scope}
    folder = ImportFolder(1, "Anime", SHOKO_ROOT, "None", True)
    folders_state: dict[str, tuple[ImportFolder, ...] | None] = {"value": (folder,)}
    inventory = SonarrInventory((), (), (), (), (), (), (), InventoryVerdict(True, True, (), ()))
    discovery = ShokoDiscovery(
        (),
        {folder.id: folder},
        ShokoVerdict(True, (), ()),
        1,
        0,
        1000,
    )
    path_result = PathMappingDiscovery(
        MappingDiscoveryStatus.READY,
        (PathMapping(ANIME_ROOT, SHOKO_ROOT),),
        (),
        "Exact paths establish this mapping.",
    )
    state_path = tmp_path / "state.sqlite3"
    bootstrap = BootstrapSettings(
        "http://sonarr:8989",
        SecretRef("SONARR_API_KEY"),
        "http://shoko:8111",
        SecretRef("SHOKO_API_KEY"),
        state_path,
    )
    sonarr = ApiEndpoint(httpx.Client(), lambda: {})
    shoko = ApiEndpoint(httpx.Client(), lambda: {})
    controller = ServiceSetupController(bootstrap, sonarr, shoko, state_path)
    monkeypatch.setattr(
        controller,
        "_environment",
        lambda: (compatibility_state["value"], scope_state["value"], assessment_state["value"]),
    )
    monkeypatch.setattr(
        controller,
        "_inventory",
        lambda _roots: (inventory, discovery, path_result),
    )
    monkeypatch.setattr(
        service_setup,
        "collect_import_folders",
        lambda *_: folders_state["value"],
    )

    assert compatibility.sonarr_version is not None
    assert compatibility.shoko_version is not None
    scoped_assessment = evaluate_service_safety(
        assessment, (PathMapping(ANIME_ROOT, SHOKO_ROOT),), discovery.folders
    )
    with open_state(state_path) as store:
        manager = controller.onboarding(store)
        manager.confirm_setup(
            ScopeBinding(
                bootstrap.sonarr_url,
                bootstrap.shoko_url,
                compatibility.sonarr_version,
                compatibility.shoko_version,
                compatibility.capabilities,
                (ANIME_ROOT,),
                (PathMapping(ANIME_ROOT, SHOKO_ROOT),),
                scoped_assessment.digest,
                scoped_assessment.unverifiable_facts,
                tuple(scoped_assessment.observed_settings.items()),
                ((folder.id, folder.path, folder.drop_folder_type),),
            ),
            scoped_assessment,
            confirmed_facts=(
                scoped_assessment.unverifiable_facts + scoped_assessment.unknown_settings
            ),
            automatic_link_consent=True,
            iso="2026-10-15T00:00:00Z",
        )

    hard_drift = failure in {"scope_missing_root", "folders_empty"}
    if failure == "scope_unavailable":
        scope_state["value"] = replace(scope, read_status=ScopeReadStatus.UNAVAILABLE)
    elif failure == "scope_incomplete":
        scope_state["value"] = replace(scope, read_status=ScopeReadStatus.INCOMPLETE)
    elif failure == "folders_unavailable":
        folders_state["value"] = None
    elif failure == "safety_unavailable":
        unavailable_client = httpx.Client(
            base_url="http://shoko:8111/",
            transport=httpx.MockTransport(lambda request: httpx.Response(503, request=request)),
        )
        unavailable_endpoint = ApiEndpoint(unavailable_client, lambda: {})
        try:
            from shoko_linker.safety_check import inspect_shoko_safety_settings

            assessment_state["value"] = inspect_shoko_safety_settings(unavailable_endpoint)
        finally:
            unavailable_client.close()
    elif failure == "safety_incomplete":
        assessment_state["value"] = assess_settings_payload({})
    elif failure == "scope_missing_root":
        scope_state["value"] = AnimeScopeDiscovery(
            ScopeStatus.BLOCKED,
            (),
            "The previously confirmed root is no longer reported.",
            read_status=ScopeReadStatus.COMPLETE,
        )
    else:
        folders_state["value"] = ()

    with open_state(state_path) as store:
        assert controller.current_context(store) is None
        expected_reason = "setup_changed" if hard_drift else "setup_unavailable"
        assert store.get_service_pause() == (True, expected_reason)
        assert controller.onboarding(store).reconfirmation_required() is hard_drift

    scope_state["value"] = scope
    folders_state["value"] = (folder,)
    assessment_state["value"] = assessment
    with open_state(state_path) as store:
        current = controller.current_context(store)
        assert current is not None
        manager = controller.onboarding(store)
        decision = manager.resume_context(current, iso="2026-10-16T00:00:00Z")
        if hard_drift:
            assert decision.status.value == "needs_confirmation"
            assert store.get_service_pause() == (True, "setup_changed")
            assert manager.reconfirmation_required()
        else:
            assert decision.status.value == "authorized"
            assert store.get_service_pause() == (False, "")
            assert not manager.reconfirmation_required()
    sonarr.client.close()
    shoko.client.close()


@pytest.mark.parametrize("action", ["evaluate_context", "resume_context"])
@pytest.mark.parametrize("stale_fact", ["version", "unsafe", "same_binding"])
def test_collected_context_cannot_hold_or_resume_newer_confirmation(
    mixed_library: tuple[ServiceSetupController, dict[int, ImportFolder], list[SafetyAssessment]],
    monkeypatch: pytest.MonkeyPatch,
    action: str,
    stale_fact: str,
) -> None:
    controller, _folders, _raw = mixed_library
    view = controller.refresh_view()
    assert view is not None
    assert "Setup saved." in controller.action("confirm", _consent_form(view))
    with open_state(controller.state_path) as store:
        context = controller.current_context(store)
        assert context is not None
        safe_assessment = context.assessment
        original_record = controller.onboarding(store).record()
        assert original_record is not None
        fresh_binding = context.binding
        if stale_fact == "version":
            fresh_binding = replace(fresh_binding, shoko_version="5.3.3.0")
        if stale_fact == "unsafe":
            unsafe = assess_settings_payload({"FileQualityFilterEnabled": True})
            context = context._replace(assessment=unsafe)
        with open_state(controller.state_path) as other:
            manager = controller.onboarding(other)
            fresh_record = manager.confirm_setup(
                fresh_binding,
                safe_assessment,
                confirmed_facts=original_record.confirmed_facts,
                automatic_link_consent=True,
                iso="2026-10-27T03:00:00Z",
            )
            manager.pause(iso="2026-10-27T03:00:01Z")
        decision = getattr(controller.onboarding(store), action)(
            context, iso="2026-10-27T03:00:02Z"
        )
        assert decision.status.value == "needs_confirmation"
        assert store.get_service_pause() == (True, "user_paused")
        assert not controller.onboarding(store).reconfirmation_required()
        assert store.get_onboarding_json() == fresh_record.to_json()
    environment = controller._environment()
    monkeypatch.setattr(
        controller,
        "_environment",
        lambda: (
            replace(environment[0], shoko_version=fresh_binding.shoko_version),
            *environment[1:],
        ),
    )
    with open_state(controller.state_path) as restarted:
        fresh = controller.current_context(restarted)
        assert fresh is not None
        assert (
            controller.onboarding(restarted)
            .resume_context(fresh, iso="2026-10-27T03:00:03Z")
            .status.value
            == "authorized"
        )
        assert restarted.get_service_pause() == (False, "")


@pytest.mark.parametrize("unsafe", [False, True])
def test_current_consent_observation_still_latches_real_drift(
    mixed_library: tuple[ServiceSetupController, dict[int, ImportFolder], list[SafetyAssessment]],
    unsafe: bool,
) -> None:
    controller, _folders, _raw = mixed_library
    view = controller.refresh_view()
    assert view is not None
    assert "Setup saved." in controller.action("confirm", _consent_form(view))
    with open_state(controller.state_path) as store:
        context = controller.current_context(store)
        assert context is not None
        if unsafe:
            context = context._replace(
                assessment=assess_settings_payload({"FileQualityFilterEnabled": True})
            )
        else:
            context = context._replace(binding=replace(context.binding, shoko_version="5.3.3.0"))
        decision = controller.onboarding(store).evaluate_context(
            context, iso="2026-10-27T03:00:00Z"
        )
        assert decision.status.value == ("unsafe" if unsafe else "needs_confirmation")
        assert controller.onboarding(store).reconfirmation_required()
    with open_state(controller.state_path) as restarted:
        assert controller.onboarding(restarted).reconfirmation_required()


def test_late_temporary_context_hold_cannot_pause_new_confirmation(
    mixed_library: tuple[ServiceSetupController, dict[int, ImportFolder], list[SafetyAssessment]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller, folders, _raw = mixed_library
    view = controller.refresh_view()
    assert view is not None
    assert "Setup saved." in controller.action("confirm", _consent_form(view))
    with open_state(controller.state_path) as store:
        context = controller.current_context(store)
        assert context is not None
        record = controller.onboarding(store).record()
        assert record is not None
        original_hold = store.pause_onboarding_if_current
        injected = False

        def confirm_before_hold(expected: str | None, reason: str, iso: str) -> bool:
            nonlocal injected
            assert reason == "setup_unavailable"
            injected = True
            with open_state(controller.state_path) as other:
                controller.onboarding(other).confirm_setup(
                    context.binding,
                    context.assessment,
                    confirmed_facts=record.confirmed_facts,
                    automatic_link_consent=True,
                    iso="2026-10-27T04:00:00Z",
                )
            return original_hold(expected, reason, iso)

        monkeypatch.setattr(service_setup, "collect_import_folders", lambda *_: None)
        with monkeypatch.context() as patch:
            patch.setattr(store, "pause_onboarding_if_current", confirm_before_hold)
            assert controller.current_context(store) is None
        assert injected
        assert store.get_service_pause() == (False, "")
        assert not controller.onboarding(store).reconfirmation_required()
    monkeypatch.setattr(service_setup, "collect_import_folders", lambda *_: tuple(folders.values()))
    with open_state(controller.state_path) as restarted:
        fresh = controller.current_context(restarted)
        assert fresh is not None
        assert (
            controller.onboarding(restarted)
            .evaluate_context(fresh, iso="2026-10-27T04:00:01Z")
            .status.value
            == "authorized"
        )
