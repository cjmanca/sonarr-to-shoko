"""Normal-service setup discovery and durable confirmation orchestration."""

from __future__ import annotations

import hashlib
from pathlib import Path, PurePosixPath
from typing import cast

import httpx
import pytest

from shoko_linker import service_setup
from shoko_linker.bootstrap import BootstrapSettings
from shoko_linker.config import SecretRef
from shoko_linker.dashboard import DashboardSetupView
from shoko_linker.http import ApiEndpoint
from shoko_linker.paths import PathMapping
from shoko_linker.safety_check import assess_settings_payload
from shoko_linker.service_setup import ServiceSetupController
from shoko_linker.setup import (
    AnimeRootCandidate,
    AnimeScopeDiscovery,
    ApiCompatibility,
    MappingDiscoveryStatus,
    PathMappingDiscovery,
    ScopeStatus,
)
from shoko_linker.shoko import ImportFolder, ShokoDiscovery, ShokoVerdict
from shoko_linker.sonarr import InventoryVerdict, SonarrInventory
from shoko_linker.state import open_state

ANIME_ROOT = "/data/media/anime"
SHOKO_ROOT = "/anime"


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
    monkeypatch.setattr(service_setup, "collect_sonarr_inventory", lambda *_, **__: inventory)
    monkeypatch.setattr(service_setup, "collect_shoko_discovery", lambda *_: discovery)
    monkeypatch.setattr(service_setup, "infer_path_mappings", lambda *_: path_discovery)

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
        initial = controller.view()
        assert isinstance(initial, DashboardSetupView)
        assert initial.root_options == (ANIME_ROOT,)
        assert initial.confirmation_enabled is False

        assert "selected" in controller.action("scope", {"root_0": ANIME_ROOT})
        selected = controller.view()
        assert selected is not None
        assert selected.selected_roots == (ANIME_ROOT,)
        assert selected.inferred_mappings == ((ANIME_ROOT, SHOKO_ROOT),)
        assert selected.required_confirmations
        assert selected.confirmation_enabled is True

        denied = controller.action("confirm", {"automatic_link_consent": "1"})
        assert "confirm" in denied.lower()
        with open_state(tmp_path / "state.sqlite3") as store:
            assert store.get_onboarding_json() is None

        form = {"automatic_link_consent": "1"}
        form.update(
            {
                f"confirm_{index}": hashlib.sha256(fact.encode("utf-8")).hexdigest()
                for index, fact in enumerate(selected.required_confirmations)
            }
        )
        current_assessment["value"] = assess_settings_payload(
            {"Plugins": {"Renamer": {"RenameOnImport": False}}}
        )
        stale_confirmation = controller.action("confirm", form)
        assert "confirm" in stale_confirmation.lower()
        with open_state(tmp_path / "state.sqlite3") as store:
            assert store.get_onboarding_json() is None

        current_assessment["value"] = assessment
        confirmed = controller.action("confirm", form)
        assert "saved" in confirmed.lower()
        with open_state(tmp_path / "state.sqlite3") as store:
            record = controller.onboarding(store).record()
            assert record is not None
            assert record.binding.roots == (ANIME_ROOT,)
            assert record.binding.path_mappings == (PathMapping(ANIME_ROOT, SHOKO_ROOT),)
            assert set(record.confirmed_facts) == set(selected.required_confirmations)
        assert controller.view() is None

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
