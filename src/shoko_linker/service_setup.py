"""Read-only service discovery and explicit dashboard onboarding decisions."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import threading
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
from .service_settings import configured_bootstrap
from .setup import (
    AnimeScopeDiscovery,
    ApiCompatibility,
    MappingDiscoveryStatus,
    PathMappingDiscovery,
    ScopeReadStatus,
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
SETUP_INVENTORY_CACHE_SECONDS = 3600.0
DRAFT_ROOTS_META = "normal_service_draft_roots_v1"
type _SetupInventory = tuple[SonarrInventory, ShokoDiscovery, PathMappingDiscovery]


def _setup_snapshot_digest(
    view: DashboardSetupView,
    compatibility: ApiCompatibility,
    scope: AnimeScopeDiscovery,
    assessment: SafetyAssessment,
) -> str:
    """Bind a rendered setup form to its exact non-secret choices and safety facts."""
    document = {
        "compatibility": {
            "sonarr_version": compatibility.sonarr_version,
            "shoko_version": compatibility.shoko_version,
            "sonarr_supported": compatibility.sonarr_supported,
            "shoko_supported": compatibility.shoko_supported,
            "capabilities": compatibility.capabilities,
        },
        "scope": {
            "status": scope.status.value,
            "read_status": scope.read_status.value,
            "candidates": tuple(
                (candidate.path, candidate.series_ids) for candidate in scope.candidates
            ),
            "root_options": view.root_options,
            "selected_roots": view.selected_roots,
        },
        "shoko_root_options": view.shoko_root_options,
        "inferred_mappings": view.inferred_mappings,
        "mapping_options": view.mapping_options,
        "mapping_required": view.mapping_required,
        "required_confirmations": view.required_confirmations,
        "confirmation_enabled": view.confirmation_enabled,
        "safety": {
            "api_available": assessment.api_available,
            "status": assessment.status.value,
            "observed_settings": dict(sorted(assessment.observed_settings.items())),
            "unsafe_settings": assessment.unsafe_settings,
            "unknown_settings": assessment.unknown_settings,
            "unverifiable_facts": assessment.unverifiable_facts,
            "digest": assessment.digest,
        },
    }
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


class ServiceSetupController:
    """Translate fresh read-only discovery into authenticated setup choices and consent."""

    def __init__(
        self,
        bootstrap: BootstrapSettings,
        sonarr: ApiEndpoint,
        shoko: ApiEndpoint,
        state_path: str | Path,
    ) -> None:
        self.bootstrap = replace(bootstrap, database_path=Path(state_path))
        self.sonarr = sonarr
        self.shoko = shoko
        self.state_path = Path(state_path)
        self._selected_roots: tuple[str, ...] = ()
        self._selected_mappings: tuple[PathMapping, ...] = ()
        self._view_cache: tuple[float, DashboardSetupView | None] | None = None
        self._view_environment: (
            tuple[ApiCompatibility, AnimeScopeDiscovery, SafetyAssessment] | None
        ) = None
        self._view_lock = threading.Lock()
        self._refresh_lock = threading.Lock()
        self._view_generation = 0
        self._refreshing = False
        self._progress_message = "Checking current Sonarr and Shoko setup."
        self._progress_percent = 0
        self._editing_scope = False
        self._inventory_cache: (
            tuple[float, tuple[str, ...], ApiCompatibility, AnimeScopeDiscovery, _SetupInventory]
            | None
        ) = None

    def _set_progress(self, message: str, percent: int) -> None:
        with self._view_lock:
            self._progress_message = message
            self._progress_percent = max(self._progress_percent, min(99, max(0, percent)))

    @staticmethod
    def onboarding(store: StateStore) -> OnboardingManager:
        return OnboardingManager(store)

    def _environment(
        self,
    ) -> tuple[ApiCompatibility, AnimeScopeDiscovery, SafetyAssessment]:
        self._set_progress("Checking current Sonarr and Shoko setup and service versions.", 2)
        compatibility = inspect_api_compatibility(self.sonarr, self.shoko)
        self._set_progress("Discovering Sonarr anime folders and eligible series.", 8)
        scope = discover_sonarr_scope(self.sonarr)
        self._set_progress("Checking Shoko safety settings.", 15)
        assessment = inspect_shoko_safety_settings(self.shoko)
        return compatibility, scope, assessment

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
            if configured_bootstrap(self.bootstrap) != self.bootstrap:
                return None
            record = self.onboarding(store).record()
            if record is None:
                return None
            compatibility, scope, assessment = self._environment()
            if assessment.status is SafetyStatus.UNSAFE:
                self.onboarding(store).evaluate(
                    record.binding, assessment, iso=datetime.now(UTC).isoformat()
                )
                return None
            pause_reason: str | None = None
            pause_message = ""
            if (
                scope.read_status is not ScopeReadStatus.COMPLETE
                or compatibility.sonarr_version is None
                or compatibility.shoko_version is None
                or (
                    assessment.status is SafetyStatus.UNKNOWN
                    and assessment.digest != record.binding.safety_digest
                )
            ):
                pause_reason = "setup_unavailable"
                pause_message = (
                    "Current setup checks could not be completed. Automatic linking is paused "
                    "until the Sonarr and Shoko checks recover."
                )
            elif not compatibility.compatible or not set(record.binding.roots) <= {
                candidate.path for candidate in scope.candidates
            }:
                pause_reason = "setup_changed"
                pause_message = (
                    "Current services or anime scope need review before automatic linking."
                )
            else:
                folders = collect_import_folders(self.shoko)
                if folders is None or any(
                    not folder.path_usable or folder.drop_folder_type == "Unknown"
                    for folder in folders
                ):
                    pause_reason = "setup_unavailable"
                    pause_message = (
                        "Current Shoko import folders could not be completely verified. Automatic "
                        "linking is paused until the folder check recovers."
                    )
                else:
                    safe_targets = {
                        folder.path
                        for folder in folders
                        if folder.path_usable and folder.drop_folder_type == DROP_TYPE_NONE
                    }
                    if (
                        not {mapping.shoko for mapping in record.binding.path_mappings}
                        <= safe_targets
                    ):
                        pause_reason = "setup_changed"
                        pause_message = (
                            "Shoko import folders changed. Review setup before automatic linking."
                        )
            if pause_reason is not None:
                self.onboarding(store).pause_for_review(
                    pause_reason, pause_message, iso=datetime.now(UTC).isoformat()
                )
                return None
            assert compatibility.sonarr_version is not None
            assert compatibility.shoko_version is not None
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
        self._set_progress("Reading episodes and files from the selected Sonarr anime folders.", 20)
        inventory = collect_sonarr_inventory(
            self.sonarr,
            allowed_roots=roots,
            anime_only=True,
            on_progress=lambda done, total: self._set_progress(
                f"Reading Sonarr anime series: {done} of {total} checked.",
                20 + int(40 * done / max(1, total)),
            ),
        )
        self._set_progress("Reading Shoko import folders and unrecognized files.", 60)
        discovery = collect_shoko_discovery(
            self.shoko,
            on_progress=lambda done, total: self._set_progress(
                f"Reading Shoko unrecognized files: {done} of {total} read.",
                60 + int(30 * done / max(1, total)),
            ),
        )
        self._set_progress("Comparing exact file paths and sizes to verify folder mappings.", 92)
        mapping = infer_path_mappings(inventory, discovery)
        return inventory, discovery, mapping

    def _mapping_state(
        self, compatibility: ApiCompatibility, roots: tuple[str, ...], scope: AnimeScopeDiscovery
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
            cached = self._inventory_cache
            if (
                cached is not None
                and time.monotonic() - cached[0] < SETUP_INVENTORY_CACHE_SECONDS
                and cached[1:4] == (roots, compatibility, scope)
            ):
                current_folders = collect_import_folders(self.shoko)
                if current_folders is None:
                    return None, None, None, (), False, ()
                if {folder.id: folder for folder in current_folders} != cached[4][1].folders:
                    cached = None
            else:
                cached = None
            if cached is None:
                inventory, discovery, path_result = self._inventory(roots)
                if inventory.verdict.complete and discovery.verdict.complete:
                    self._inventory_cache = (
                        time.monotonic(),
                        roots,
                        compatibility,
                        scope,
                        (inventory, discovery, path_result),
                    )
            else:
                inventory, discovery, path_result = cached[4]
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

    @staticmethod
    def _loading_view(
        message: str = "Checking current Sonarr and Shoko setup.", percent: int = 0
    ) -> DashboardSetupView:
        return DashboardSetupView(
            message=f"{message} Scope choices will appear when ready.",
            root_options=(),
            confirmation_enabled=False,
            loading=True,
            progress_percent=percent,
        )

    def view(self, *, refresh: bool = False) -> DashboardSetupView | None:
        """Return the cached setup snapshot; a cache miss schedules discovery off-request."""
        if refresh:
            return self.refresh_view()
        now = time.monotonic()
        with self._view_lock:
            if (
                self._view_cache is not None
                and now - self._view_cache[0] < SETUP_VIEW_CACHE_SECONDS
            ):
                return self._view_cache[1]
            if not self._refreshing:
                self._refreshing = True
                self._progress_message = "Checking current Sonarr and Shoko setup."
                self._progress_percent = 0
                worker = threading.Thread(
                    target=self._refresh_in_background,
                    name="shoko-linker-setup-discovery",
                    daemon=True,
                )
                worker.start()
            progress = self._progress_message
            percent = self._progress_percent
        return self._loading_view(progress, percent)

    def _refresh_in_background(self) -> None:
        try:
            self.refresh_view()
        except Exception:
            logging.getLogger(__name__).exception(
                "Setup discovery failed; it will be retried on a later status request."
            )
        finally:
            with self._view_lock:
                self._refreshing = False

    def refresh_view(self) -> DashboardSetupView | None:
        """Perform a full setup discovery for a worker or an explicit offline/test caller."""
        with self._refresh_lock:
            while True:
                with self._view_lock:
                    self._view_generation += 1
                    generation = self._view_generation
                    self._view_cache = None
                    self._view_environment = None
                    self._progress_percent = 0
                    selected_roots = self._selected_roots
                    selected_mappings = self._selected_mappings
                view, environment = self._compute_view(selected_roots, selected_mappings)
                with self._view_lock:
                    if generation != self._view_generation:
                        continue
                    self._view_environment = environment
                    self._view_cache = (time.monotonic(), view)
                    return view

    def _invalidate_view(self) -> None:
        with self._view_lock:
            self._view_generation += 1
            self._view_cache = None
            self._view_environment = None

    def _compute_view(
        self,
        selected_roots: tuple[str, ...],
        selected_mappings: tuple[PathMapping, ...],
    ) -> tuple[
        DashboardSetupView | None,
        tuple[ApiCompatibility, AnimeScopeDiscovery, SafetyAssessment],
    ]:
        compatibility, scope, assessment = self._environment()
        environment = (compatibility, scope, assessment)
        if configured_bootstrap(self.bootstrap) != self.bootstrap:
            return DashboardSetupView(
                "Restart the container to use the saved connection URLs, then review setup.",
                (),
                confirmation_enabled=False,
            ), environment
        saved_record = self._saved_record()
        with open_state(self.state_path) as store:
            encoded_draft = store.get_meta(DRAFT_ROOTS_META)
        draft_roots: tuple[str, ...] = ()
        if encoded_draft is not None:
            raw_draft = json.loads(encoded_draft)
            if not isinstance(raw_draft, list) or not all(
                isinstance(root, str) for root in raw_draft
            ):
                raise StateError("Saved library selection is invalid.")
            draft_roots = tuple(raw_draft)
        roots = (
            selected_roots
            or draft_roots
            or (() if saved_record is None else saved_record.binding.roots)
        )
        mappings = selected_mappings or (
            saved_record.binding.path_mappings
            if (
                saved_record is not None
                and saved_record.binding.roots == roots
                and not self._editing_scope
                and encoded_draft is None
            )
            else ()
        )
        inventory, discovery, path_result, inferred, mapping_required, mapping_options = (
            self._mapping_state(compatibility, roots, scope)
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
            and scope.read_status is ScopeReadStatus.COMPLETE
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
                with open_state(self.state_path) as store:
                    reconfirmation_required = self.onboarding(store).reconfirmation_required()
                if not reconfirmation_required and not self._editing_scope:
                    return None, environment

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
            progress_percent=100,
        )
        view = replace(
            view,
            snapshot_digest=_setup_snapshot_digest(view, compatibility, scope, assessment),
        )
        return view, environment

    def action(self, action: str, form: Mapping[str, str]) -> str:
        try:
            if action == "edit":
                with self._view_lock:
                    self._editing_scope = True
                    self._selected_mappings = ()
                    self._inventory_cache = None
                return "Review your selected anime folders below. Continue to save a new selection."
            if action == "scope":
                return self._select_scope(form)
            if action == "confirm":
                return self._confirm(form)
        except (ConfigError, OnboardingError, OSError, PathError, StateError, ValueError) as exc:
            return f"Setup was not saved. {exc}"
        finally:
            self._invalidate_view()
        return "That setup action is not available."

    def _select_scope(self, form: Mapping[str, str]) -> str:
        view, environment, generation = self._submitted_view(form)
        if view is None or environment is None:
            raise ValueError("Refresh setup before choosing an anime folder.")
        shown_compatibility, shown_scope, shown_assessment = environment
        compatibility, scope, _assessment = self._environment()
        if scope.read_status is not ScopeReadStatus.COMPLETE:
            raise ValueError(
                "The current Sonarr anime scope check is incomplete or unavailable. Wait for it "
                "to recover before choosing a folder."
            )
        if (
            not compatibility.compatible
            or compatibility != shown_compatibility
            or scope.status is not shown_scope.status
            or scope.read_status is not shown_scope.read_status
            or tuple((candidate.path, candidate.series_ids) for candidate in scope.candidates)
            != tuple((candidate.path, candidate.series_ids) for candidate in shown_scope.candidates)
            or _assessment.digest != shown_assessment.digest
            or _assessment.status is not shown_assessment.status
            or _assessment.unverifiable_facts != shown_assessment.unverifiable_facts
        ):
            raise ValueError("Current scope or safety changed. Review refreshed setup first.")
        candidates = view.root_options
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
        with self._view_lock:
            self._assert_snapshot_current_locked(view, generation)
            selected_scope = tuple(sorted(set(selected)))
            with open_state(self.state_path) as store:
                if self.onboarding(store).record() is not None:
                    self.onboarding(store).pause_for_review(
                        "setup_changed",
                        "Library selection needs confirmation.",
                        iso=datetime.now(UTC).isoformat(),
                    )
                store.set_meta(DRAFT_ROOTS_META, json.dumps(selected_scope))
            self._selected_roots = selected_scope
            self._selected_mappings = ()
        return "Anime scope selected. Checking exact file paths and folder correspondence."

    def _confirm(  # noqa: PLR0912, PLR0915 - ordered fail-closed setup authorization checks
        self, form: Mapping[str, str]
    ) -> str:
        view, environment, generation = self._submitted_view(form)
        with self._view_lock:
            selected_roots = self._selected_roots
        if view is None or not view.confirmation_enabled:
            raise ValueError("Current scope, file correspondence, or safety checks are not ready.")
        if environment is None:
            raise ValueError("Refresh setup before confirming the selected scope.")
        shown_compatibility, shown_scope, shown_assessment = environment
        compatibility, scope, assessment = self._environment()
        roots = view.selected_roots
        if selected_roots and tuple(sorted(selected_roots)) != roots:
            raise ValueError(
                "The selected scope changed. Review refreshed setup before confirming."
            )
        if scope.read_status is not ScopeReadStatus.COMPLETE:
            raise ValueError(
                "The current Sonarr anime scope check is incomplete or unavailable. Wait for it "
                "to recover before confirming setup."
            )
        if (
            compatibility != shown_compatibility
            or scope.status is not shown_scope.status
            or scope.read_status is not shown_scope.read_status
            or tuple((candidate.path, candidate.series_ids) for candidate in scope.candidates)
            != tuple((candidate.path, candidate.series_ids) for candidate in shown_scope.candidates)
            or assessment.digest != shown_assessment.digest
            or assessment.status is not shown_assessment.status
            or assessment.unverifiable_facts != shown_assessment.unverifiable_facts
        ):
            raise ValueError(
                "Current scope or safety changed. Review refreshed setup before confirming."
            )
        if assessment.status is SafetyStatus.UNSAFE:
            raise ValueError("Unsafe Shoko settings must be fixed before setup confirmation.")
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
        if required != view.required_confirmations:
            raise ValueError(
                "Current safety facts changed. Review refreshed setup before confirming."
            )
        confirmed = tuple(
            fact
            for index, fact in enumerate(required)
            if form.get(f"confirm_{index}") == hashlib.sha256(fact.encode("utf-8")).hexdigest()
        )
        if len(confirmed) != len(required):
            raise ValueError("Review and confirm every listed safety fact before continuing.")
        if form.get("automatic_link_consent") != "1":
            raise ValueError("Explicit consent to automatic linking is required.")
        folders = collect_import_folders(self.shoko)
        safe_targets = {
            folder.path
            for folder in folders or ()
            if folder.path_usable and folder.drop_folder_type == DROP_TYPE_NONE
        }
        if (
            folders is None
            or any(
                not folder.path_usable or folder.drop_folder_type == "Unknown" for folder in folders
            )
            or not {mapping.shoko for mapping in mappings} <= safe_targets
        ):
            raise ValueError("The selected Shoko folder is no longer a safe import target.")
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
        with self._view_lock:
            self._assert_snapshot_current_locked(view, generation)
            with open_state(self.state_path) as store:
                self.onboarding(store).confirm_setup(
                    binding,
                    assessment,
                    confirmed_facts=confirmed,
                    automatic_link_consent=True,
                    iso=datetime.now(UTC).isoformat(),
                )
                store.clear_meta(DRAFT_ROOTS_META)
            self._selected_roots = roots
            self._selected_mappings = mappings
            self._editing_scope = False
        return "Setup saved. Automatic linking will start after current safety checks pass."

    def _submitted_view(
        self, form: Mapping[str, str]
    ) -> tuple[
        DashboardSetupView | None,
        tuple[ApiCompatibility, AnimeScopeDiscovery, SafetyAssessment] | None,
        int,
    ]:
        with self._view_lock:
            cached = self._view_cache
            environment = self._view_environment
            generation = self._view_generation
        if cached is None or time.monotonic() - cached[0] >= SETUP_VIEW_CACHE_SECONDS:
            raise ValueError("Setup choices expired. Refresh setup before continuing.")
        view = cached[1]
        supplied = form.get("setup_digest", "")
        if (
            view is None
            or not view.snapshot_digest
            or not supplied
            or not hmac.compare_digest(
                supplied.encode("utf-8"), view.snapshot_digest.encode("ascii")
            )
        ):
            raise ValueError("Setup choices changed or the form is incomplete. Refresh setup.")
        if environment is None:
            raise ValueError("Refresh setup before continuing.")
        return view, environment, generation

    def _assert_snapshot_current_locked(self, view: DashboardSetupView, generation: int) -> None:
        """Prevent a concurrent refresh from changing form meanings before local persistence."""
        cached = self._view_cache
        if (
            generation != self._view_generation
            or cached is None
            or cached[1] is None
            or not hmac.compare_digest(
                cached[1].snapshot_digest.encode("ascii"), view.snapshot_digest.encode("ascii")
            )
        ):
            raise ValueError("Setup choices changed during this request. Review refreshed setup.")
