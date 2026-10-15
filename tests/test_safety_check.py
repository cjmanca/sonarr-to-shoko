"""Read-only Shoko safety inspection; raw settings must never escape the parser."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import httpx
import pytest

from shoko_linker.http import ApiEndpoint, make_timeout, shoko_headers
from shoko_linker.safety_check import (
    SAFETY_SETTING_PATHS,
    SafetyStatus,
    inspect_shoko_safety_settings,
)

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
