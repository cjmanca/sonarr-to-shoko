"""Safe read-only connection monitoring for the normal container service entrypoint."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass, replace
from http.server import HTTPServer
from typing import Final, Literal

import httpx
import yaml  # type: ignore[import-untyped]

from .bootstrap import BootstrapSettings
from .config import DatabaseSettings, Settings, parse_settings
from .errors import ConfigError, HttpPolicyError
from .http import (
    ApiEndpoint,
    make_client,
    resolve_and_send,
    shoko_headers,
    sonarr_headers,
)
from .paths import PathMapping

_logger = logging.getLogger(__name__)

SONARR_STATUS_PATH: Final[str] = "api/v3/system/status"
SHOKO_IMPORT_FOLDERS_PATH: Final[str] = "api/v3/ImportFolder"
SERVICE_POLL_SECONDS: Final[float] = 300.0
MIN_SUCCESS_STATUS: Final[int] = 200
MAX_SUCCESS_STATUS: Final[int] = 300
AUTH_FAILURE_STATUSES: Final[frozenset[int]] = frozenset({401, 403})


@dataclass(frozen=True)
class ConnectionReport:
    """Read-only setup connectivity results without URLs, keys, or raw API payloads."""

    sonarr_available: bool
    shoko_available: bool
    messages: tuple[str, ...]

    @property
    def ready(self) -> bool:
        """Both required services returned their documented top-level response shape."""
        return self.sonarr_available and self.shoko_available


def bootstrap_endpoints(settings: BootstrapSettings) -> tuple[ApiEndpoint, ApiEndpoint]:
    """Create authenticated endpoints while keeping key strings out of settings objects."""
    sonarr_key = settings.sonarr_api_key.resolve()
    shoko_key = settings.shoko_api_key.resolve()
    return (
        make_client(
            settings.sonarr_url,
            lambda: sonarr_headers(sonarr_key),
            secret_values=(sonarr_key,),
        ),
        make_client(
            settings.shoko_url,
            lambda: shoko_headers(shoko_key),
            secret_values=(shoko_key,),
        ),
    )


def settings_for_scope(
    bootstrap: BootstrapSettings,
    *,
    roots: tuple[str, ...],
    mappings: tuple[PathMapping, ...],
) -> Settings:
    """Build full daemon settings only from non-empty, validated setup scope and mappings."""
    if not roots:
        raise ConfigError(
            "Sonarr scope must be explicitly selected before linking can be configured."
        )
    if not mappings:
        raise ConfigError("Path mappings must be verified before linking can be configured.")
    shoko_roots = tuple(sorted({mapping.shoko for mapping in mappings}))
    document = {
        "sonarr": {
            "url": bootstrap.sonarr_url,
            "api_key_env": bootstrap.sonarr_api_key.env_var,
            "allowed_roots": list(roots),
        },
        "shoko": {
            "url": bootstrap.shoko_url,
            "api_key_env": bootstrap.shoko_api_key.env_var,
            "allowed_roots": list(shoko_roots),
        },
        "paths": {
            "mappings": [{"sonarr": mapping.sonarr, "shoko": mapping.shoko} for mapping in mappings]
        },
        "mappings": {"source": "anime-lists"},
        "sync": {"apply_changes": False},
        # Bootstrap owns this service path; keep it outside the POSIX-only media-path schema.
        "database": {"path": "/data/state.sqlite3"},
    }
    parsed = parse_settings(yaml.safe_dump(document, sort_keys=True))
    return replace(
        parsed,
        anime_only=True,
        database=DatabaseSettings(str(bootstrap.database_path)),
    )


def _probe_endpoint(
    endpoint: ApiEndpoint,
    *,
    service_name: str,
    path: str,
    expected_shape: Literal["object", "array"],
) -> tuple[bool, str | None]:
    try:
        response = resolve_and_send(endpoint, "GET", path)
    except (httpx.HTTPError, HttpPolicyError):
        return False, f"Cannot connect to {service_name}. Check its address and API key."

    if response.status_code in AUTH_FAILURE_STATUSES:
        return False, f"Cannot authenticate with {service_name}. Check its address and API key."
    if not MIN_SUCCESS_STATUS <= response.status_code < MAX_SUCCESS_STATUS:
        return False, f"Cannot connect to {service_name}. Check its address and API key."

    try:
        payload = response.json()
    except (ValueError, UnicodeDecodeError):
        return False, f"{service_name} returned an unsupported response. Check its API version."
    expected_type = dict if expected_shape == "object" else list
    if not isinstance(payload, expected_type):
        return False, f"{service_name} returned an unsupported response. Check its API version."
    return True, None


def probe_connections(sonarr: ApiEndpoint, shoko: ApiEndpoint) -> ConnectionReport:
    """Check Sonarr status and Shoko import-folder availability using GET requests only."""
    sonarr_available, sonarr_message = _probe_endpoint(
        sonarr,
        service_name="Sonarr",
        path=SONARR_STATUS_PATH,
        expected_shape="object",
    )
    shoko_available, shoko_message = _probe_endpoint(
        shoko,
        service_name="Shoko",
        path=SHOKO_IMPORT_FOLDERS_PATH,
        expected_shape="array",
    )
    messages = tuple(message for message in (sonarr_message, shoko_message) if message is not None)
    return ConnectionReport(sonarr_available, shoko_available, messages)


def run_service(
    sonarr: ApiEndpoint,
    shoko: ApiEndpoint,
    *,
    stop_requested: Callable[[], bool],
    wait: Callable[[float], None],
    on_report: Callable[[ConnectionReport], None] | None = None,
) -> None:
    """Keep the process alive, polling connections read-only until the caller stops it.

    Discovery, user onboarding, and the guarded worker attach to this lifecycle in later
    milestones. Until they are ready, successful connectivity does not authorize linking.
    """
    while not stop_requested():
        report = probe_connections(sonarr, shoko)
        if on_report is not None:
            on_report(report)
        if report.ready:
            _logger.info(
                "Connected to Sonarr and Shoko; automatic linking remains paused "
                "until setup is complete."
            )
        else:
            for message in report.messages:
                _logger.warning("%s", message)
        if stop_requested():
            break
        wait(SERVICE_POLL_SECONDS)


def run_dashboard_worker(
    server: HTTPServer,
    *,
    worker_tick: Callable[[], None],
    stop_requested: Callable[[], bool],
    monotonic: Callable[[], float],
    worker_interval_seconds: float,
    request_poll_seconds: float = 0.5,
    worker_wake: threading.Event | None = None,
    interval_provider: Callable[[], float] | None = None,
) -> None:
    """Keep the dashboard loop responsive while a dedicated thread runs worker ticks.

    The caller owns the HTTP server loop. On shutdown the listener is closed before waiting
    for the worker, while the worker thread is joined without force-termination so a protected
    Shoko operation can finish and preserve its durable recovery state.
    """
    if worker_interval_seconds <= 0 or request_poll_seconds <= 0:
        raise ValueError("service intervals must be positive")
    worker_stop = threading.Event()
    wake = worker_wake if worker_wake is not None else threading.Event()

    def run_worker() -> None:
        next_worker_tick = monotonic()
        while not worker_stop.is_set() and not stop_requested():
            remaining = next_worker_tick - monotonic()
            if remaining > 0:
                if wake.wait(remaining):
                    wake.clear()
                    next_worker_tick = monotonic()
                continue
            try:
                worker_tick()
            except Exception:
                _logger.exception("Service worker tick failed; it will be retried.")
            try:
                interval = (
                    worker_interval_seconds if interval_provider is None else interval_provider()
                )
            except Exception:
                _logger.warning("Service interval unavailable; using the default retry interval.")
                interval = worker_interval_seconds
            next_worker_tick = monotonic() + interval

    worker = threading.Thread(target=run_worker, name="shoko-link-worker", daemon=False)
    try:
        worker.start()
        while not stop_requested():
            server.timeout = request_poll_seconds
            server.handle_request()
    finally:
        worker_stop.set()
        wake.set()
        server.server_close()
        worker.join()
