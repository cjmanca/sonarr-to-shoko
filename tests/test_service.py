"""Read-only connection monitoring for container service startup."""

from __future__ import annotations

import re
import signal
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml  # type: ignore[import-untyped]

from shoko_linker.bootstrap import BootstrapSettings
from shoko_linker.config import SecretRef
from shoko_linker.errors import ConfigError
from shoko_linker.http import ApiEndpoint, make_timeout, shoko_headers, sonarr_headers
from shoko_linker.paths import PathMapping
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


def _create_dashboard_account_and_pause(base_url: str) -> None:
    with httpx.Client(timeout=2, follow_redirects=False) as client:
        created = client.post(
            f"{base_url}/setup",
            data={"username": "library-admin", "password": "a sufficiently long password"},
            headers={"Origin": base_url},
        )
        assert created.status_code == 303
        status = client.get(f"{base_url}/status")
        assert status.status_code == 200
        csrf = re.search(r'name="csrf_token" value="([^"]+)"', status.text)
        assert csrf is not None
        paused = client.post(
            f"{base_url}/pause",
            data={"csrf_token": csrf.group(1)},
            headers={"Origin": base_url},
        )
        assert paused.status_code == 303


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


def test_dashboard_stays_responsive_and_pause_is_durable_while_worker_is_blocked(
    tmp_path: Path,
) -> None:
    from shoko_linker.cli import _SHUTDOWN_WAKE, StopState, _stop_handler
    from shoko_linker.dashboard import (
        DashboardApplication,
        DashboardStatus,
        create_dashboard_server,
    )
    from shoko_linker.service import run_dashboard_worker
    from shoko_linker.state import open_state

    state_path = tmp_path / "state.sqlite3"
    app = DashboardApplication(
        state_path,
        status_provider=lambda: DashboardStatus(
            "Sonarr and Shoko are connected.", 0, 0, 0, True, "No action is needed."
        ),
    )
    server = create_dashboard_server("127.0.0.1", 0, app)
    base_url = f"http://127.0.0.1:{server.server_port}"
    worker_entered = threading.Event()
    release_worker = threading.Event()
    stop_state = StopState()
    stop_handler = _stop_handler(stop_state)
    observed_pause: list[tuple[bool, str]] = []
    runner: threading.Thread | None = None

    def worker_tick() -> None:
        worker_entered.set()
        if not release_worker.wait(timeout=5):
            raise TimeoutError("test worker was not released")
        with open_state(state_path) as store:
            observed_pause.append(store.get_service_pause())

    try:
        runner = threading.Thread(
            target=run_dashboard_worker,
            kwargs={
                "server": server,
                "worker_tick": worker_tick,
                "stop_requested": stop_state.should_stop,
                "monotonic": time.monotonic,
                "worker_interval_seconds": 60.0,
                "request_poll_seconds": 0.05,
            },
        )
        runner.start()
        assert worker_entered.wait(timeout=2)

        _create_dashboard_account_and_pause(base_url)

        with open_state(state_path) as store:
            assert store.get_service_pause() == (True, "user_paused")

        stop_handler(int(signal.SIGTERM), None)
        deadline = time.monotonic() + 2
        while server.fileno() != -1 and time.monotonic() < deadline:
            time.sleep(0.01)
        assert server.fileno() == -1  # stop accepting requests before joining the worker
        assert runner.is_alive()  # the protected worker operation has not been killed
    finally:
        stop_handler(int(signal.SIGTERM), None)
        release_worker.set()
        if runner is not None:
            runner.join(timeout=5)
        server.server_close()
        _SHUTDOWN_WAKE.clear()

    assert runner is not None
    assert not runner.is_alive()
    assert observed_pause == [(True, "user_paused")]


@pytest.mark.parametrize(
    "partial",
    [
        b"",
        b"POST /login HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\n\r\na",
        b"GET / HTTP/1.1\r\nHost:",
        b"POST /login HTTP/1.1\r\nContent-Length: 9000\r\n\r\na",
    ],
)
@pytest.mark.parametrize("trickle", [False, True])
def test_partial_dashboard_requests_release_controls_and_shutdown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, partial: bytes, trickle: bool
) -> None:
    import socket

    from shoko_linker import dashboard
    from shoko_linker.service import run_dashboard_worker

    monkeypatch.setattr(dashboard, "DASHBOARD_REQUEST_TIMEOUT_SECONDS", 0.5)
    server = dashboard.create_dashboard_server(
        "127.0.0.1", 0, dashboard.DashboardApplication(tmp_path / "state.sqlite3")
    )
    stop = threading.Event()
    accepted = threading.Event()
    get_request = server.get_request

    def observe_accept() -> tuple[socket.socket, Any]:
        request = get_request()
        accepted.set()
        return request

    monkeypatch.setattr(server, "get_request", observe_accept)
    runner = threading.Thread(
        target=run_dashboard_worker,
        kwargs={
            "server": server,
            "worker_tick": lambda: None,
            "stop_requested": stop.is_set,
            "monotonic": time.monotonic,
            "worker_interval_seconds": 60,
            "request_poll_seconds": 0.02,
        },
    )
    runner.start()
    address = ("127.0.0.1", server.server_port)
    client = socket.create_connection(address, timeout=1)
    second: socket.socket | None = None
    stop_trickle = threading.Event()

    def trickle_bytes() -> None:
        while not stop_trickle.wait(timeout=0.05):
            try:
                client.sendall(b"a")
            except OSError:
                return

    sender = threading.Thread(target=trickle_bytes)
    try:
        client.sendall(partial)
        if trickle:
            sender.start()
        # The next request must be serviced even while the first client stays open.
        _create_dashboard_account_and_pause(f"http://127.0.0.1:{server.server_port}")
        accepted.clear()
        second = socket.create_connection(address, timeout=1)
        second.sendall(partial)
        assert accepted.wait(timeout=1)
        stop.set()
        runner.join(timeout=2)
        assert not runner.is_alive()
        assert server.fileno() == -1
    finally:
        stop.set()
        stop_trickle.set()
        if trickle:
            sender.join(timeout=2)
        client.close()
        if second is not None:
            second.close()
        runner.join(timeout=2)
        server.server_close()


def test_worker_exception_is_retried_and_repeated_start_stop_closes_server(
    tmp_path: Path,
) -> None:
    from shoko_linker.dashboard import DashboardApplication, create_dashboard_server
    from shoko_linker.service import run_dashboard_worker

    for cycle in range(2):
        server = create_dashboard_server(
            "127.0.0.1", 0, DashboardApplication(tmp_path / f"state-{cycle}.sqlite3")
        )
        base_url = f"http://127.0.0.1:{server.server_port}"
        stop = threading.Event()
        retried = threading.Event()
        calls = 0

        def worker_tick(
            current_cycle: int = cycle, retry_signal: threading.Event = retried
        ) -> None:
            nonlocal calls
            calls += 1
            if current_cycle == 0 and calls == 1:
                raise RuntimeError("simulated worker failure")
            retry_signal.set()

        runner = threading.Thread(
            target=run_dashboard_worker,
            kwargs={
                "server": server,
                "worker_tick": worker_tick,
                "stop_requested": stop.is_set,
                "monotonic": time.monotonic,
                "worker_interval_seconds": 0.05,
                "request_poll_seconds": 0.01,
            },
        )
        try:
            runner.start()
            assert retried.wait(timeout=2)
            with httpx.Client(timeout=2) as client:
                assert client.get(base_url).status_code == 200
        finally:
            stop.set()
            runner.join(timeout=5)
            server.server_close()
        assert not runner.is_alive()
        assert server.fileno() == -1
        assert calls >= (2 if cycle == 0 else 1)


def test_container_and_compose_start_service_without_yaml_or_published_port() -> None:
    dockerfile = (REPOSITORY_ROOT / "Dockerfile").read_text(encoding="utf-8")
    compose = yaml.safe_load((REPOSITORY_ROOT / "compose.example.yaml").read_text(encoding="utf-8"))
    service = compose["services"]["shoko-sonarr-linker"]
    environment = service["environment"]

    assert 'ENTRYPOINT ["python", "-m", "shoko_linker.container_entrypoint"]' in dockerfile
    assert 'CMD ["service"]' in dockerfile
    assert service["image"] == "ghcr.io/cjmanca/shoko-sonarr-linker:latest"
    assert service["command"] == ["service"]
    assert {"SONARR_URL", "SONARR_API_KEY", "SHOKO_URL", "SHOKO_API_KEY"} <= set(environment)
    assert service["volumes"] == ["shoko-sonarr-linker-data:/data"]
    assert "ports" not in service
    assert "build" not in service
    assert not service.get("privileged", False)
    assert not any("docker.sock" in mount for mount in service["volumes"])


def test_service_settings_are_built_only_from_confirmed_scope_and_exact_mappings(
    tmp_path: Path,
) -> None:
    from shoko_linker.service import settings_for_scope

    bootstrap = BootstrapSettings(
        sonarr_url="http://sonarr:8989",
        sonarr_api_key=SecretRef("SONARR_API_KEY"),
        shoko_url="http://shoko:8111",
        shoko_api_key=SecretRef("SHOKO_API_KEY"),
        database_path=tmp_path / "state.sqlite3",
    )
    settings = settings_for_scope(
        bootstrap,
        roots=("/data/media/anime",),
        mappings=(PathMapping("/data/media/anime", "/anime"),),
    )
    assert settings.anime_only is True
    assert settings.sonarr.allowed_roots == ("/data/media/anime",)
    assert settings.shoko.allowed_roots == ("/anime",)
    assert settings.path_mappings == (PathMapping("/data/media/anime", "/anime"),)
    assert settings.sync.apply_changes is False
    assert settings.sync.rolling_new_limit == 5000
    assert settings.database.path == str(bootstrap.database_path)
    assert "SONARR_API_KEY" in settings.secret_env_vars
    assert "dummy-sonarr-key" not in repr(settings)
    with pytest.raises(ConfigError, match="explicitly selected"):
        settings_for_scope(bootstrap, roots=(), mappings=())
