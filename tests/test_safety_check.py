"""Read-only Shoko safety inspection; raw settings must never escape the parser."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import httpx
import pytest

from shoko_linker.http import ApiEndpoint, make_timeout, shoko_headers
from shoko_linker.paths import PathMapping
from shoko_linker.safety_check import (
    SAFETY_SETTING_PATHS,
    SafetyStatus,
    assess_settings_payload,
    evaluate_service_safety,
    inspect_shoko_safety_settings,
)
from shoko_linker.shoko import ImportFolder

SHOKO_KEY = "shoko-safety-test-key-never-a-real-secret"
SETTINGS_SECRET = "database-password-sentinel-never-return"
Handler = Callable[[httpx.Request], httpx.Response]


def safe_settings() -> dict[str, Any]:
    return {
        "FileQualityFilterEnabled": False,
        "Plugins": {
            "Renamer": {
                "RenameOnImport": False,
                "MoveOnImport": False,
                "RelocateOnImport": False,
            }
        },
        "AniDb": {"MyList_AddFiles": False, "Password": SETTINGS_SECRET},
        "Database": {"Password": SETTINGS_SECRET},
    }


def endpoint_for(handler: Handler) -> tuple[ApiEndpoint, list[httpx.Request]]:
    requests: list[httpx.Request] = []

    def logged(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return handler(request)

    client = httpx.Client(
        base_url="http://shoko:8111/",
        timeout=make_timeout(1.0, 1.0),
        follow_redirects=False,
        transport=httpx.MockTransport(logged),
    )
    return ApiEndpoint(client, lambda: shoko_headers(SHOKO_KEY)), requests


@pytest.mark.parametrize("setting", tuple(SAFETY_SETTING_PATHS))
def test_enabled_side_effect_setting_blocks_automatic_linking(setting: str) -> None:
    payload = safe_settings()
    if setting == "FileQualityFilterEnabled":
        payload[setting] = True
    elif setting == "MyList_AddFiles":
        payload["AniDb"][setting] = True
    else:
        payload["Plugins"]["Renamer"][setting] = True
    endpoint, requests = endpoint_for(
        lambda request: httpx.Response(200, json=payload, request=request)
    )
    try:
        assessment = inspect_shoko_safety_settings(endpoint)
    finally:
        endpoint.client.close()

    assert assessment.status is SafetyStatus.UNSAFE
    assert setting in assessment.unsafe_settings
    assert assessment.needs_operator_confirmation is False
    assert [request.method for request in requests] == ["GET"]
    assert SETTINGS_SECRET not in repr(assessment)


def test_safe_api_settings_still_require_physical_setup_confirmation() -> None:
    endpoint, _ = endpoint_for(
        lambda request: httpx.Response(200, json=safe_settings(), request=request)
    )
    try:
        assessment = inspect_shoko_safety_settings(endpoint)
    finally:
        endpoint.client.close()

    assert assessment.status is SafetyStatus.API_SETTINGS_SAFE
    assert assessment.unsafe_settings == ()
    assert assessment.unknown_settings == ()
    assert assessment.needs_operator_confirmation is True
    assert assessment.unverifiable_facts
    assert SETTINGS_SECRET not in repr(assessment)
    assert SETTINGS_SECRET not in assessment.message


def test_missing_or_malformed_api_settings_require_confirmation() -> None:
    payload = safe_settings()
    del payload["Plugins"]["Renamer"]["MoveOnImport"]
    payload["AniDb"]["MyList_AddFiles"] = "false"
    endpoint, _ = endpoint_for(lambda request: httpx.Response(200, json=payload, request=request))
    try:
        assessment = inspect_shoko_safety_settings(endpoint)
    finally:
        endpoint.client.close()

    assert assessment.status is SafetyStatus.UNKNOWN
    assert set(assessment.unknown_settings) == {"MoveOnImport", "MyList_AddFiles"}
    assert assessment.needs_operator_confirmation is True


def test_admin_only_settings_denial_requires_user_confirmation_without_echoing_body() -> None:
    endpoint, requests = endpoint_for(
        lambda request: httpx.Response(403, text=SETTINGS_SECRET, request=request)
    )
    try:
        assessment = inspect_shoko_safety_settings(endpoint)
    finally:
        endpoint.client.close()

    assert assessment.status is SafetyStatus.UNKNOWN
    assert assessment.api_available is False
    assert set(assessment.unknown_settings) == set(SAFETY_SETTING_PATHS)
    assert assessment.needs_operator_confirmation is True
    assert SETTINGS_SECRET not in repr(assessment)
    assert SETTINGS_SECRET not in assessment.message
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/api/v3/Settings")
    ]


def test_settings_digest_uses_only_sanitized_safety_values() -> None:
    endpoint, _ = endpoint_for(
        lambda request: httpx.Response(200, json=safe_settings(), request=request)
    )
    try:
        first = inspect_shoko_safety_settings(endpoint)
        second = inspect_shoko_safety_settings(endpoint)
    finally:
        endpoint.client.close()

    assert first.digest == second.digest
    assert len(first.digest) == 64
    assert SETTINGS_SECRET not in first.digest


def mixed_folders() -> dict[int, ImportFolder]:
    return {
        1: ImportFolder(1, "Sonarr", "/sonarr", "None", True),
        2: ImportFolder(2, "Manual source", "/drop", "Source", True),
        3: ImportFolder(3, "Manual destination", "/manual", "Destination", True),
    }


def renamer_settings() -> dict[str, Any]:
    payload = safe_settings()
    payload["Plugins"]["Renamer"] = {
        name: True for name in ("RenameOnImport", "MoveOnImport", "RelocateOnImport")
    }
    return payload


def test_service_excluded_scope_allows_manual_workflow_but_requires_physical_facts() -> None:
    raw = assess_settings_payload(renamer_settings())
    effective = evaluate_service_safety(raw, [PathMapping("/anime", "/sonarr")], mixed_folders())
    assert raw.status is SafetyStatus.UNSAFE
    assert effective.status is SafetyStatus.API_SETTINGS_SAFE
    assert effective.unsafe_settings == ()
    assert effective.observed_settings == raw.observed_settings
    assert effective.needs_operator_confirmation
    assert any(
        "inside Shoko" in fact and "hardlink" in fact for fact in effective.unverifiable_facts
    )
    assert any("external hooks" in fact for fact in effective.unverifiable_facts)
    assert SETTINGS_SECRET not in repr(effective)
    assert effective.digest != raw.digest


@pytest.mark.parametrize("setting", ["FileQualityFilterEnabled", "MyList_AddFiles"])
def test_service_excluded_folders_never_override_independent_hazards(setting: str) -> None:
    payload = renamer_settings()
    if setting == "MyList_AddFiles":
        payload["AniDb"][setting] = True
    else:
        payload[setting] = True
    effective = evaluate_service_safety(
        assess_settings_payload(payload), [PathMapping("/anime", "/sonarr")], mixed_folders()
    )
    assert effective.status is SafetyStatus.UNSAFE
    assert effective.unsafe_settings == (setting,)
    assert not effective.needs_operator_confirmation


@pytest.mark.parametrize(
    "folders",
    [
        {},
        {1: ImportFolder(1, "Unknown", "/sonarr", "Unknown", True)},
        {1: ImportFolder(1, "Source", "/sonarr", "Source", True)},
        {1: ImportFolder(1, "Bad path", "sonarr", "None", False)},
        {1: ImportFolder(1, "Descendant only", "/sonarr/nested", "None", True)},
        {
            1: ImportFolder(1, "Parent", "/sonarr", "None", True),
            2: ImportFolder(2, "Nested", "/sonarr/nested", "None", True),
        },
        {
            1: ImportFolder(1, "First", "/sonarr", "None", True),
            2: ImportFolder(2, "Duplicate path", "/sonarr/.", "None", True),
        },
        {2: ImportFolder(1, "Mismatched ID", "/sonarr", "None", True)},
        {
            1: ImportFolder(1, "Selected", "/sonarr", "None", True),
            2: ImportFolder(2, "Unknown unrelated", "/manual", "Unknown", True),
        },
    ],
)
def test_service_refuses_missing_or_ambiguous_selected_ownership(
    folders: dict[int, ImportFolder],
) -> None:
    effective = evaluate_service_safety(
        assess_settings_payload(safe_settings()), [PathMapping("/anime", "/sonarr")], folders
    )
    assert effective.status is SafetyStatus.UNSAFE
    assert not effective.needs_operator_confirmation


@pytest.mark.parametrize(
    "mappings",
    [[], [PathMapping("/anime", "/sonarr")] * 2, [PathMapping("/anime", "/sonarr-old")]],
)
def test_service_refuses_missing_duplicate_or_component_mismatched_mappings(
    mappings: list[PathMapping],
) -> None:
    effective = evaluate_service_safety(
        assess_settings_payload(safe_settings()), mappings, mixed_folders()
    )
    assert effective.status is SafetyStatus.UNSAFE


@pytest.mark.parametrize("payload", [None, {}, {"FileQualityFilterEnabled": False}])
def test_service_unknown_settings_require_scoped_manual_confirmation(payload: Any) -> None:
    raw = assess_settings_payload(payload)
    effective = evaluate_service_safety(raw, [PathMapping("/anime", "/sonarr")], mixed_folders())
    assert effective.status is SafetyStatus.UNKNOWN
    assert effective.observed_settings == raw.observed_settings
    assert effective.api_available == raw.api_available
    assert "MyList_AddFiles is disabled in Shoko" in effective.unknown_settings
    if raw.observed_settings["FileQualityFilterEnabled"] is None:
        assert "FileQualityFilterEnabled is disabled in Shoko" in effective.unknown_settings
    assert "MoveOnImport" not in effective.unknown_settings
    assert any("MoveOnImport cannot be inspected" in item for item in effective.unknown_settings)
    assert effective.needs_operator_confirmation


def test_service_digest_binds_selected_folders_and_ignores_unrelated_manual_changes() -> None:
    raw = assess_settings_payload(renamer_settings())
    mappings = [PathMapping("/anime", "/sonarr/series")]
    folders = mixed_folders()
    first = evaluate_service_safety(raw, mappings, folders)
    folders[2] = ImportFolder(2, "Changed manual folder", "/other-drop", "None", True)
    folders[4] = ImportFolder(4, "Another manual destination", "/another", "Destination", True)
    assert evaluate_service_safety(raw, mappings, folders).digest == first.digest
    assert evaluate_service_safety(raw, mappings, dict(reversed(list(folders.items())))).digest == (
        first.digest
    )
    folders[1] = ImportFolder(1, "Renamed display name", "/sonarr/./", "None", True)
    assert evaluate_service_safety(raw, mappings, folders).digest == first.digest
    for replacement in (
        ImportFolder(5, "Sonarr", "/sonarr", "None", True),
        ImportFolder(1, "Sonarr", "/sonarr/series", "None", True),
        ImportFolder(1, "Sonarr", "/sonarr", "Destination", True),
    ):
        changed = {**folders}
        del changed[1]
        changed[replacement.id] = replacement
        assert evaluate_service_safety(raw, mappings, changed).digest != first.digest
    changed_settings = assess_settings_payload(safe_settings())
    assert evaluate_service_safety(changed_settings, mappings, folders).digest != first.digest
