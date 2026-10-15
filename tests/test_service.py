"""Read-only connection monitoring for container service startup."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import yaml  # type: ignore[import-untyped]

from shoko_linker.http import ApiEndpoint, make_timeout, shoko_headers, sonarr_headers
from shoko_linker.service import (
    SERVICE_POLL_SECONDS,
    probe_connections,
    run_service,
)

FAKE_SONARR_KEY = "sonarr-test-key-never-a-real-secret"
FAKE_SHOKO_KEY = "shoko-test-key-never-a-real-secret"
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
Handler = Callable[[httpx.Request], httpx.Response]


def _endpoint(
    base_url: str, handler: Handler, *, key: str, header: Callable[[str], dict[str, str]]
) -> ApiEndpoint:
    client = httpx.Client(
        base_url=base_url.rstrip("/") + "/",
        timeout=make_timeout(1.0, 1.0),
        follow_redirects=False,
        transport=httpx.MockTransport(handler),
    )
    return ApiEndpoint(client, lambda: header(key))


def _healthy_endpoints(
    seen: list[httpx.Request] | None = None,
) -> tuple[ApiEndpoint, ApiEndpoint]:
    requests = seen if seen is not None else []

    def sonarr_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json={"version": "4.0.0", "instanceName": "sonarr"},
            request=request,
        )

    def shoko_handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=[], request=request)

    return (
        _endpoint(
            "http://sonarr:8989",
            sonarr_handler,
            key=FAKE_SONARR_KEY,
            header=sonarr_headers,
        ),
        _endpoint(
            "http://shoko:8111",
            shoko_handler,
            key=FAKE_SHOKO_KEY,
            header=shoko_headers,
        ),
    )


def test_connection_probe_uses_read_only_verified_endpoints_and_redacts_keys() -> None:
    requests: list[httpx.Request] = []
    sonarr, shoko = _healthy_endpoints(requests)
    try:
        result = probe_connections(sonarr, shoko)
    finally:
        sonarr.client.close()
        shoko.client.close()

    assert result.ready is True
    assert result.messages == ()
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/api/v3/system/status"),
        ("GET", "/api/v3/ImportFolder"),
    ]
    assert all(request.method == "GET" for request in requests)
    assert FAKE_SONARR_KEY not in repr(result)
    assert FAKE_SHOKO_KEY not in repr(result)


def test_connection_probe_uses_actionable_auth_failure_without_echoing_response_body() -> None:
    def denied(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, text=FAKE_SONARR_KEY, request=request)

    sonarr = _endpoint("http://sonarr:8989", denied, key=FAKE_SONARR_KEY, header=sonarr_headers)
    shoko = _endpoint(
        "http://shoko:8111",
        lambda request: httpx.Response(200, json=[], request=request),
        key=FAKE_SHOKO_KEY,
        header=shoko_headers,
    )
    try:
        result = probe_connections(sonarr, shoko)
    finally:
        sonarr.client.close()
        shoko.client.close()

    assert result.ready is False
    assert result.messages == ("Cannot authenticate with Sonarr. Check its address and API key.",)
    assert FAKE_SONARR_KEY not in repr(result)


def test_service_rechecks_connections_and_waits_interruptibly_between_cycles() -> None:
    sonarr, shoko = _healthy_endpoints()
    stopped = {"value": False}
    reports: list[Any] = []
    waits: list[float] = []

    def wait(seconds: float) -> None:
        waits.append(seconds)
        stopped["value"] = True

    try:
        run_service(
            sonarr,
            shoko,
            stop_requested=lambda: stopped["value"],
            wait=wait,
            on_report=reports.append,
        )
    finally:
        sonarr.client.close()
        shoko.client.close()

    assert len(reports) == 1
    assert reports[0].ready is True
    assert waits == [SERVICE_POLL_SECONDS]


def test_container_and_compose_start_service_without_yaml_or_published_port() -> None:
    dockerfile = (REPOSITORY_ROOT / "Dockerfile").read_text(encoding="utf-8")
    compose = yaml.safe_load((REPOSITORY_ROOT / "compose.example.yaml").read_text(encoding="utf-8"))
    service = compose["services"]["shoko-sonarr-linker"]
    environment = service["environment"]

    assert 'ENTRYPOINT ["shoko-sonarr-linker"]' in dockerfile
    assert 'CMD ["service"]' in dockerfile
    assert service["command"] == ["service"]
    assert {"SONARR_URL", "SONARR_API_KEY", "SHOKO_URL", "SHOKO_API_KEY"} <= set(environment)
    assert service["volumes"] == ["shoko-sonarr-linker-data:/data"]
    assert "ports" not in service
