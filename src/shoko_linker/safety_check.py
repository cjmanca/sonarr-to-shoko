"""Read-only inspection of Shoko settings that can affect a file-link job."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any, Final

import httpx

from .errors import HttpPolicyError, PathError
from .http import ApiEndpoint, resolve_and_send
from .paths import PathMapping, is_within_root, normalize_posix, validate_mappings
from .shoko import VALID_DROP_TYPES, ImportFolder

SETTINGS_PATH: Final[str] = "api/v3/Settings"
MIN_SUCCESS_STATUS: Final[int] = 200
MAX_SUCCESS_STATUS: Final[int] = 300
SAFETY_SETTING_PATHS: Final[Mapping[str, tuple[str, ...]]] = MappingProxyType(
    {
        "FileQualityFilterEnabled": ("FileQualityFilterEnabled",),
        "RenameOnImport": ("Plugins", "Renamer", "RenameOnImport"),
        "MoveOnImport": ("Plugins", "Renamer", "MoveOnImport"),
        "RelocateOnImport": ("Plugins", "Renamer", "RelocateOnImport"),
        "MyList_AddFiles": ("AniDb", "MyList_AddFiles"),
    }
)
UNVERIFIABLE_FACTS: Final[tuple[str, ...]] = (
    "Shoko media mounts follow the operator's read-only policy",
    "installed Shoko plugins do not rename, move, delete, or rewrite media on link events",
)
SERVICE_SAFETY_POLICY_REVISION: Final[str] = "excluded-sonarr-folders-v1"
RENAMER_SETTINGS: Final[frozenset[str]] = frozenset(
    {"RenameOnImport", "MoveOnImport", "RelocateOnImport"}
)
SERVICE_UNVERIFIABLE_FACTS: Final[tuple[str, ...]] = (
    "Selected Sonarr media is mounted read-only inside Shoko, with no writable aliases "
    "or writable hardlink exposure",
    "Shoko plugins and external hooks do not rename, move, delete, or rewrite media on "
    "link events (the separate manual import workflow may remain enabled)",
)


class SafetyStatus(Enum):
    """API-settings verdict; physical setup facts require separate confirmation."""

    API_SETTINGS_SAFE = "api_settings_safe"
    UNSAFE = "unsafe"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class SafetyAssessment:
    """Sanitized safety facts; this object never retains Shoko's raw settings payload."""

    api_available: bool
    status: SafetyStatus
    observed_settings: Mapping[str, bool | None]
    unsafe_settings: tuple[str, ...]
    unknown_settings: tuple[str, ...]
    unverifiable_facts: tuple[str, ...]
    digest: str
    message: str

    @property
    def needs_operator_confirmation(self) -> bool:
        """Physical facts are never inferred; unsafe settings cannot be overridden here."""
        return self.status is not SafetyStatus.UNSAFE


def _setting_value(payload: Mapping[str, Any], path: tuple[str, ...]) -> bool | None:
    value: Any = payload
    for component in path:
        if not isinstance(value, Mapping) or component not in value:
            return None
        value = value[component]
    return value if isinstance(value, bool) else None


def _digest(api_available: bool, values: Mapping[str, bool | None]) -> str:
    canonical = json.dumps(
        {"api_available": api_available, "settings": dict(sorted(values.items()))},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def assess_settings_payload(payload: Any) -> SafetyAssessment:
    """Extract only known safety booleans from a Shoko settings JSON object."""
    settings = payload if isinstance(payload, Mapping) else {}
    values = {name: _setting_value(settings, path) for name, path in SAFETY_SETTING_PATHS.items()}
    unsafe = tuple(sorted(name for name, value in values.items() if value is True))
    unknown = tuple(sorted(name for name, value in values.items() if value is None))
    status = (
        SafetyStatus.UNSAFE
        if unsafe
        else SafetyStatus.UNKNOWN
        if unknown
        else SafetyStatus.API_SETTINGS_SAFE
    )

    if unsafe:
        message = (
            "Unsafe Shoko settings prevent automatic linking: "
            f"{', '.join(unsafe)}. Disable them in Shoko; the service will not change settings."
        )
    elif unknown:
        message = (
            "Some Shoko safety settings cannot be verified through this API key. "
            "Check them in Shoko during setup; automatic linking stays paused."
        )
    else:
        message = (
            "Shoko API safety settings look safe. Confirm Shoko media mounts and plugin "
            "side effects once during setup."
        )

    return SafetyAssessment(
        api_available=isinstance(payload, Mapping),
        status=status,
        observed_settings=MappingProxyType(dict(sorted(values.items()))),
        unsafe_settings=unsafe,
        unknown_settings=unknown,
        unverifiable_facts=UNVERIFIABLE_FACTS,
        digest=_digest(isinstance(payload, Mapping), values),
        message=message,
    )


def selected_service_folders(
    path_mappings: Sequence[PathMapping], folders: Mapping[int, ImportFolder]
) -> tuple[tuple[int, str, str], ...]:
    """Resolve complete, unique folder ownership without inspecting physical mounts."""
    mappings = validate_mappings(path_mappings)
    if len(mappings) != len(path_mappings):
        raise PathError("duplicate selected path mappings")
    normalized: list[tuple[int, str, str]] = []
    for key, folder in folders.items():
        # An unusable path cannot establish independence from the selected namespace.
        if (
            key != folder.id
            or folder.id <= 0
            or not folder.path_usable
            or folder.drop_folder_type not in VALID_DROP_TYPES
        ):
            raise PathError("invalid Shoko import-folder context")
        normalized.append((folder.id, normalize_posix(folder.path), folder.drop_folder_type))
    selected: set[tuple[int, str, str]] = set()
    for mapping in mappings:
        overlapping = [
            folder
            for folder in normalized
            if is_within_root(mapping.shoko, folder[1]) or is_within_root(folder[1], mapping.shoko)
        ]
        if len(overlapping) != 1:
            raise PathError("selected Shoko root has missing or ambiguous import-folder ownership")
        folder_id, path, drop_type = overlapping[0]
        if not is_within_root(mapping.shoko, path) or drop_type != "None":
            raise PathError("selected Shoko roots must belong to excluded (Drop Type None) folders")
        selected.add((folder_id, path, drop_type))
    return tuple(sorted(selected))


def evaluate_service_safety(
    assessment: SafetyAssessment,
    path_mappings: Sequence[PathMapping],
    folders: Mapping[int, ImportFolder],
) -> SafetyAssessment:
    """Apply normal-service exclusion policy to sanitized settings and current scope.

    Only Shoko's renamer flags are scoped by excluded import folders. Quality filtering
    and MyList remain independent hazards. This never establishes mount or hook safety;
    explicit physical confirmation is still required. Advanced callers retain the raw
    conservative settings assessment.
    """
    context_error = False
    try:
        selected = selected_service_folders(path_mappings, folders)
    except PathError:
        selected = ()
        context_error = True
    canonical = json.dumps(
        {
            "policy": SERVICE_SAFETY_POLICY_REVISION,
            "settings_digest": assessment.digest,
            "selected_folders": selected,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    unsafe = tuple(name for name in assessment.unsafe_settings if name not in RENAMER_SETTINGS)
    unknown = tuple(
        f"{name} cannot be inspected; selected Sonarr folders are excluded from renaming/moving"
        if name in RENAMER_SETTINGS
        else f"{name} is disabled in Shoko"
        for name in assessment.unknown_settings
    )
    if context_error:
        status = SafetyStatus.UNSAFE
        message = (
            "Automatic linking requires unambiguous selected Shoko import folders with "
            "usable paths and Drop Type None. Review the selected folder mappings."
        )
    elif unsafe:
        status = SafetyStatus.UNSAFE
        message = (
            "Unsafe Shoko settings prevent automatic linking: "
            f"{', '.join(unsafe)}. Disable them in Shoko; the service will not change settings."
        )
    else:
        status = SafetyStatus.UNKNOWN if unknown else SafetyStatus.API_SETTINGS_SAFE
        message = (
            "Selected Sonarr folders are excluded from Shoko renaming and moving. "
            "Confirm read-only protection inside Shoko and link-event side effects during setup."
        )
        if unknown:
            message += " Some safety settings require manual confirmation."
    return SafetyAssessment(
        api_available=assessment.api_available,
        status=status,
        observed_settings=assessment.observed_settings,
        unsafe_settings=unsafe,
        unknown_settings=unknown,
        unverifiable_facts=SERVICE_UNVERIFIABLE_FACTS,
        digest=hashlib.sha256(canonical).hexdigest(),
        message=message,
    )


def inspect_shoko_safety_settings(endpoint: ApiEndpoint) -> SafetyAssessment:
    """Read only ``GET /Settings`` and discard its secret-bearing raw response immediately."""
    try:
        response = resolve_and_send(endpoint, "GET", SETTINGS_PATH)
    except (HttpPolicyError, httpx.HTTPError):
        return _unavailable_assessment()
    try:
        if not MIN_SUCCESS_STATUS <= response.status_code < MAX_SUCCESS_STATUS:
            return _unavailable_assessment()
        try:
            payload = response.json()
        except ValueError:
            return _unavailable_assessment()
    finally:
        response.close()

    # The secret-bearing response stays local to this call; only booleans escape.
    return assess_settings_payload(payload)


def _unavailable_assessment() -> SafetyAssessment:
    values = {name: None for name in SAFETY_SETTING_PATHS}
    return SafetyAssessment(
        api_available=False,
        status=SafetyStatus.UNKNOWN,
        observed_settings=MappingProxyType(dict(sorted(values.items()))),
        unsafe_settings=(),
        unknown_settings=tuple(sorted(values)),
        unverifiable_facts=UNVERIFIABLE_FACTS,
        digest=_digest(False, values),
        message=(
            "Shoko safety settings are unavailable to this API key. Check them in Shoko "
            "during setup; automatic linking stays paused."
        ),
    )
