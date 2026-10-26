"""Normal-service setup discovery and durable confirmation orchestration."""

from __future__ import annotations

import hashlib
import threading
from dataclasses import replace
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import cast

import httpx
import pytest

from shoko_linker import service_setup
from shoko_linker.bootstrap import BootstrapSettings
from shoko_linker.config import SecretRef
from shoko_linker.dashboard import DashboardSetupView
from shoko_linker.http import ApiEndpoint
from shoko_linker.onboarding import ScopeBinding
from shoko_linker.paths import PathMapping
from shoko_linker.safety_check import SafetyAssessment, assess_settings_payload
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
                    assessment,
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

        current_assessment["value"] = assess_settings_payload(
            {"Plugins": {"Renamer": {"MoveOnImport": True}}}
        )
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
                assessment,
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
                assessment.digest,
                assessment.unverifiable_facts,
            ),
            assessment,
            confirmed_facts=assessment.unverifiable_facts + assessment.unknown_settings,
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
        decision = manager.resume(*current, iso="2026-10-16T00:00:00Z")
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
