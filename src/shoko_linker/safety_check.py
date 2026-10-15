"""Read-only inspection of Shoko settings that can affect a file-link job."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any, Final

import httpx

from .errors import HttpPolicyError
from .http import ApiEndpoint, resolve_and_send

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
