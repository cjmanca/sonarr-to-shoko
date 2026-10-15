"""Read-only service discovery and explicit dashboard onboarding decisions."""

from __future__ import annotations

import hashlib
import time
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from .bootstrap import BootstrapSettings
from .dashboard import DashboardSetupView
from .errors import ConfigError, PathError
from .http import ApiEndpoint
from .onboarding import (
    OnboardingError,
    OnboardingManager,
    OnboardingRecord,
    ScopeBinding,
)
from .paths import PathMapping, validate_mappings
from .safety_check import SafetyAssessment, SafetyStatus, inspect_shoko_safety_settings
from .setup import (
    AnimeScopeDiscovery,
    ApiCompatibility,
    MappingDiscoveryStatus,
    PathMappingDiscovery,
    ScopeStatus,
    discover_sonarr_scope,
    infer_path_mappings,
    inspect_api_compatibility,
)
from .shoko import (
    DROP_TYPE_NONE,
    ShokoDiscovery,
    collect_import_folders,
    collect_shoko_discovery,
)
from .sonarr import SonarrInventory, collect_sonarr_inventory
from .state import StateError, StateStore, open_state

SETUP_VIEW_CACHE_SECONDS = 30.0


class ServiceSetupController:
    """Translate fresh read-only discovery into authenticated setup choices and consent."""

    def __init__(
        self,
        bootstrap: BootstrapSettings,
        sonarr: ApiEndpoint,
        shoko: ApiEndpoint,
        state_path: str | Path,
    ) -> None:
        self.bootstrap = bootstrap
        self.sonarr = sonarr
        self.shoko = shoko
        self.state_path = Path(state_path)
        self._selected_roots: tuple[str, ...] = ()
        self._selected_mappings: tuple[PathMapping, ...] = ()
        self._view_cache: tuple[float, DashboardSetupView | None] | None = None
        self._view_environment: (
            tuple[ApiCompatibility, AnimeScopeDiscovery, SafetyAssessment] | None
        ) = None

    @staticmethod
    def onboarding(store: StateStore) -> OnboardingManager:
        return OnboardingManager(store)

    def _environment(
        self,
    ) -> tuple[ApiCompatibility, AnimeScopeDiscovery, SafetyAssessment]:
        return (
            inspect_api_compatibility(self.sonarr, self.shoko),
            discover_sonarr_scope(self.sonarr),
            inspect_shoko_safety_settings(self.shoko),
        )

    def _saved_record(self) -> OnboardingRecord | None:
        try:
            with open_state(self.state_path) as store:
                record = self.onboarding(store).record()
        except (OSError, StateError, OnboardingError):
            return None
        return record

    def current_context(self, store: StateStore) -> tuple[ScopeBinding, SafetyAssessment] | None:
        """Return current non-secret authorization facts only for a still-present scope."""
        try:
            record = self.onboarding(store).record()
            if record is None:
                return None
            compatibility, scope, assessment = self._environment()
            if assessment.status is SafetyStatus.UNSAFE:
                self.onboarding(store).evaluate(
                    record.binding, assessment, iso=datetime.now(UTC).isoformat()
                )
                return None
            if (
                not compatibility.compatible
                or compatibility.sonarr_version is None
                or compatibility.shoko_version is None
                or not set(record.binding.roots)
                <= {candidate.path for candidate in scope.candidates}
            ):
                self.onboarding(store).pause_for_review(
                    "setup_changed",
                    "Current services or anime scope need review before automatic linking.",
                    iso=datetime.now(UTC).isoformat(),
                )
                return None
            folders = collect_import_folders(self.shoko)
            safe_targets = {
                folder.path
                for folder in folders or ()
                if folder.path_usable and folder.drop_folder_type == DROP_TYPE_NONE
            }
            if (
                folders is None
                or {mapping.shoko for mapping in record.binding.path_mappings} > safe_targets
            ):
                self.onboarding(store).pause_for_review(
                    "setup_changed",
                    "Shoko import folders changed. Review setup before automatic linking.",
                    iso=datetime.now(UTC).isoformat(),
                )
                return None
            binding = replace(
                record.binding,
                sonarr_url=self.bootstrap.sonarr_url,
                shoko_url=self.bootstrap.shoko_url,
                sonarr_version=compatibility.sonarr_version,
                shoko_version=compatibility.shoko_version,
                capabilities=compatibility.capabilities,
                safety_digest=assessment.digest,
                unverifiable_facts=assessment.unverifiable_facts,
            )
            return binding, assessment
        except (OnboardingError, OSError, PathError, StateError, ValueError):
            return None

    def _inventory(
        self, roots: tuple[str, ...]
    ) -> tuple[SonarrInventory, ShokoDiscovery, PathMappingDiscovery]:
        inventory = collect_sonarr_inventory(self.sonarr, allowed_roots=roots, anime_only=True)
        discovery = collect_shoko_discovery(self.shoko)
        mapping = infer_path_mappings(inventory, discovery)
        return inventory, discovery, mapping

    def _mapping_state(
        self, compatibility: ApiCompatibility, roots: tuple[str, ...]
    ) -> tuple[
        SonarrInventory | None,
        ShokoDiscovery | None,
        PathMappingDiscovery | None,
        tuple[PathMapping, ...],
        bool,
        tuple[tuple[str, str], ...],
    ]:
        if not compatibility.compatible or not roots:
            return None, None, None, (), False, ()
        try:
            inventory, discovery, path_result = self._inventory(roots)
        except (ValueError, OSError, PathError):
            return None, None, None, (), False, ()
        inferred_roots = {item.sonarr for item in path_result.mappings}
        if path_result.status is MappingDiscoveryStatus.READY and inferred_roots == set(roots):
            return inventory, discovery, path_result, path_result.mappings, False, ()
        if path_result.status not in {
            MappingDiscoveryStatus.NEEDS_USER_MAPPING,
            MappingDiscoveryStatus.READY,
        }:
            return inventory, discovery, path_result, (), False, ()
        folders = tuple(
            sorted(
                {
                    folder.path
                    for folder in discovery.folders.values()
                    if folder.path_usable and folder.drop_folder_type == DROP_TYPE_NONE
                }
            )
        )
        options = tuple((root, folder) for root in roots for folder in folders)
        return inventory, discovery, path_result, (), True, options

    @staticmethod
    def _safe_roots(discovery: ShokoDiscovery | None) -> tuple[str, ...]:
        if discovery is None:
            return ()
        return tuple(
            sorted(
                {
                    folder.path
                    for folder in discovery.folders.values()
                    if folder.path_usable and folder.drop_folder_type == DROP_TYPE_NONE
                }
            )
        )

    @staticmethod
    def _message(
        compatibility: ApiCompatibility,
        scope: AnimeScopeDiscovery,
        assessment: SafetyAssessment,
        roots: tuple[str, ...],
        inventory_ready: bool,
        mapping_ready: bool,
        path_result: PathMappingDiscovery | None,
        required: tuple[str, ...],
    ) -> str:
        if not compatibility.compatible:
            message = (
                compatibility.messages[0]
                if compatibility.messages
                else "Service versions need review."
            )
        elif scope.status in {ScopeStatus.BLOCKED, ScopeStatus.NO_ANIME} or not roots:
            message = scope.message
        elif assessment.status is SafetyStatus.UNSAFE:
            message = assessment.message
        elif not inventory_ready:
            message = "Cannot safely compare Sonarr and Shoko files yet. Check both connections."
        elif not mapping_ready:
            message = (
                path_result.message
                if path_result is not None
                else "A verified folder mapping is required before setup can continue."
            )
        elif required:
            message = f"{assessment.message} Review and confirm the listed safety facts."
        else:
            message = assessment.message
        return message

    def view(self, *, refresh: bool = False) -> DashboardSetupView | None:
        now = time.monotonic()
        if (
            not refresh
            and self._view_cache is not None
            and now - self._view_cache[0] < SETUP_VIEW_CACHE_SECONDS
        ):
            return self._view_cache[1]
        compatibility, scope, assessment = self._environment()
        self._view_environment = (compatibility, scope, assessment)
        saved_record = self._saved_record()
        roots = self._selected_roots or (() if saved_record is None else saved_record.binding.roots)
        mappings = self._selected_mappings or (
            () if saved_record is None else saved_record.binding.path_mappings
        )
        inventory, discovery, path_result, inferred, mapping_required, mapping_options = (
            self._mapping_state(compatibility, roots)
        )
        current_mappings = mappings or inferred
        safe_shoko_roots = self._safe_roots(discovery)
        required = tuple(sorted((*assessment.unknown_settings, *assessment.unverifiable_facts)))
        has_complete_inventory = (
            inventory is not None
            and inventory.verdict.complete
            and discovery is not None
            and discovery.verdict.complete
        )
        manual_mapping_ready = mapping_required and bool(mapping_options)
        mapping_ready = (
            bool(current_mappings)
            and {item.sonarr for item in current_mappings} == set(roots)
            and {item.shoko for item in current_mappings} <= set(safe_shoko_roots)
        ) or manual_mapping_ready
        mappings_verified = (
            bool(current_mappings)
            and {item.sonarr for item in current_mappings} == set(roots)
            and {item.shoko for item in current_mappings} <= set(safe_shoko_roots)
            and (not inferred or inferred == current_mappings)
        )
        if mappings_verified:
            mapping_required = False
            mapping_options = ()
        confirmation_enabled = (
            compatibility.compatible
            and scope.status not in {ScopeStatus.BLOCKED, ScopeStatus.NO_ANIME}
            and bool(roots)
            and {candidate.path for candidate in scope.candidates} >= set(roots)
            and has_complete_inventory
            and mapping_ready
            and assessment.status is not SafetyStatus.UNSAFE
        )

        if (
            saved_record is not None
            and confirmation_enabled
            and mappings_verified
            and compatibility.sonarr_version is not None
            and compatibility.shoko_version is not None
        ):
            current_binding = ScopeBinding(
                sonarr_url=self.bootstrap.sonarr_url,
                shoko_url=self.bootstrap.shoko_url,
                sonarr_version=compatibility.sonarr_version,
                shoko_version=compatibility.shoko_version,
                capabilities=compatibility.capabilities,
                roots=roots,
                path_mappings=current_mappings,
                safety_digest=assessment.digest,
                unverifiable_facts=assessment.unverifiable_facts,
            )
            if current_binding.digest == saved_record.binding_digest and set(required) <= set(
                saved_record.confirmed_facts
            ):
                self._view_cache = (now, None)
                return None

        message = self._message(
            compatibility,
            scope,
            assessment,
            roots,
            has_complete_inventory,
            mapping_ready,
            path_result,
            required,
        )

        inferred_pairs = tuple((item.sonarr, item.shoko) for item in inferred)
        if not inferred_pairs and mappings:
            inferred_pairs = tuple((item.sonarr, item.shoko) for item in mappings)
        root_options = tuple(candidate.path for candidate in scope.candidates)
        view = DashboardSetupView(
            message=message,
            root_options=root_options,
            shoko_root_options=safe_shoko_roots,
            selected_roots=roots,
            inferred_mappings=inferred_pairs,
            mapping_options=mapping_options,
            mapping_required=mapping_required,
            required_confirmations=required,
            confirmation_enabled=confirmation_enabled,
        )
        self._view_cache = (now, view)
        return view

    def action(self, action: str, form: Mapping[str, str]) -> str:
        try:
            self._view_cache = None
            if action == "scope":
                return self._select_scope(form)
            if action == "confirm":
                return self._confirm(form)
        except (ConfigError, OnboardingError, OSError, PathError, StateError, ValueError) as exc:
            return f"Setup was not saved. {exc}"
        return "That setup action is not available."

    def _select_scope(self, form: Mapping[str, str]) -> str:
        compatibility, scope, _assessment = self._environment()
        if not compatibility.compatible:
            return "Service versions need review before choosing a folder."
        candidates = tuple(candidate.path for candidate in scope.candidates)
        selected: list[str] = []
        for key, value in form.items():
            if not key.startswith("root_"):
                continue
            suffix = key.removeprefix("root_")
            if not suffix.isdigit() or int(suffix) >= len(candidates):
                raise ValueError("The selected anime folder is no longer available.")
            expected = candidates[int(suffix)]
            if value != expected:
                raise ValueError("The selected anime folder could not be verified.")
            selected.append(expected)
        if not selected:
            raise ValueError("Select at least one Sonarr anime folder.")
        self._selected_roots = tuple(sorted(set(selected)))
        self._selected_mappings = ()
        return "Anime scope selected. Checking exact file paths and folder correspondence."

    def _confirm(self, form: Mapping[str, str]) -> str:
        view = self.view(refresh=True)
        if view is None or not view.confirmation_enabled:
            raise ValueError("Current scope, file correspondence, or safety checks are not ready.")
        environment = self._view_environment
        if environment is None:
            raise ValueError("Refresh setup before confirming the selected scope.")
        compatibility, scope, assessment = environment
        roots = view.selected_roots
        if not set(roots) <= {candidate.path for candidate in scope.candidates}:
            raise ValueError("The selected Sonarr anime folder is no longer available.")
        mappings: tuple[PathMapping, ...]
        if view.mapping_required:
            chosen: list[PathMapping] = []
            for index, root in enumerate(roots):
                option = form.get(f"mapping_{index}")
                if (
                    option is None
                    or not option.isdigit()
                    or int(option) >= len(view.mapping_options)
                ):
                    raise ValueError(
                        "Choose a verified Sonarr-to-Shoko folder match for each folder."
                    )
                source, target = view.mapping_options[int(option)]
                if source != root:
                    raise ValueError("A folder match does not belong to the selected anime scope.")
                chosen.append(PathMapping(source, target))
            mappings = validate_mappings(chosen)
        else:
            mappings = validate_mappings(
                tuple(PathMapping(source, target) for source, target in view.inferred_mappings)
            )
        required = tuple(sorted((*assessment.unknown_settings, *assessment.unverifiable_facts)))
        confirmed = tuple(
            fact
            for index, fact in enumerate(required)
            if form.get(f"confirm_{index}") == hashlib.sha256(fact.encode("utf-8")).hexdigest()
        )
        if len(confirmed) != len(required):
            raise ValueError("Review and confirm every listed safety fact before continuing.")
        if form.get("automatic_link_consent") != "1":
            raise ValueError("Explicit consent to automatic linking is required.")
        if (
            not compatibility.compatible
            or compatibility.sonarr_version is None
            or compatibility.shoko_version is None
        ):
            raise ValueError("Service versions could not be verified.")
        binding = ScopeBinding(
            sonarr_url=self.bootstrap.sonarr_url,
            shoko_url=self.bootstrap.shoko_url,
            sonarr_version=compatibility.sonarr_version,
            shoko_version=compatibility.shoko_version,
            capabilities=compatibility.capabilities,
            roots=roots,
            path_mappings=mappings,
            safety_digest=assessment.digest,
            unverifiable_facts=assessment.unverifiable_facts,
        )
        with open_state(self.state_path) as store:
            self.onboarding(store).confirm_setup(
                binding,
                assessment,
                confirmed_facts=confirmed,
                automatic_link_consent=True,
                iso=datetime.now(UTC).isoformat(),
            )
        self._selected_roots = roots
        self._selected_mappings = mappings
        self._view_cache = None
        return "Setup saved. Automatic linking will start after current safety checks pass."
