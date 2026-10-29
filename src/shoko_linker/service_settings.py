"""Persist validated, non-secret normal-service preferences independently of consent."""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Callable, Mapping
from dataclasses import asdict, replace
from datetime import UTC, datetime
from typing import Any

import yaml  # type: ignore[import-untyped]

from .bootstrap import BootstrapSettings
from .config import Settings, parse_runtime_options, validate_base_url
from .dashboard import DashboardSettingsView
from .errors import ConfigError
from .onboarding import OnboardingManager
from .state import META_SERVICE_SETTINGS, StateStore, open_state

SETTINGS_META = META_SERVICE_SETTINGS
MAX_SERVICE_CYCLE_LINKS = 20
MAX_SERVICE_ROLLING_LINKS = 5000
MIN_SERVICE_ROLLING_HOURS = 168
MAX_SERVICE_PENDING_JOBS = 10
NORMAL_SYNC_FIELDS = (
    ("interval_seconds", "Service check interval (seconds)"),
    ("full_reconcile_interval_hours", "Full library check interval (hours)"),
    ("new_file_grace_period_seconds", "Wait before processing new files (seconds)"),
    ("max_new_per_cycle", "Maximum links per cycle (up to 20)"),
    ("rolling_new_limit", "Maximum links per rolling window (up to 5,000)"),
    ("max_pending_episode_jobs", "Maximum pending episode jobs (up to 10)"),
)


def _defaults(bootstrap: BootstrapSettings) -> dict[str, Any]:
    sync, mappings, safety, campaign = parse_runtime_options(
        {"sync": {}, "mappings": {"source": "anime-lists"}}
    )
    sync_doc = asdict(sync)
    sync_doc["timeouts"] = {
        "connect_seconds": sync_doc.pop("connect_timeout_seconds"),
        "read_seconds": sync_doc.pop("read_timeout_seconds"),
    }
    return {
        "allow_missing_metadata_refresh": False,
        "sonarr_url": bootstrap.sonarr_url,
        "shoko_url": bootstrap.shoko_url,
        "sync": sync_doc,
        "mappings": asdict(mappings),
        "safety": asdict(safety),
        "campaign": asdict(campaign),
    }


def _validate(document: dict[str, Any]) -> dict[str, Any]:
    document.setdefault("allow_missing_metadata_refresh", False)
    if not isinstance(document["allow_missing_metadata_refresh"], bool):
        raise ConfigError("allow_missing_metadata_refresh must be a boolean.")
    if set(document) != {
        "allow_missing_metadata_refresh",
        "sonarr_url",
        "shoko_url",
        "sync",
        "mappings",
        "safety",
        "campaign",
    }:
        raise ConfigError("Unsupported service settings fields.")
    for name in ("sonarr_url", "shoko_url"):
        document[name] = validate_base_url(document[name], name)
    sync, _mappings, _safety, _campaign = parse_runtime_options(
        {name: document[name] for name in ("sync", "mappings", "safety", "campaign")}
    )
    if (
        sync.apply_changes
        or sync.max_concurrent_links != 1
        or sync.max_new_per_cycle > MAX_SERVICE_CYCLE_LINKS
        or sync.rolling_new_limit > MAX_SERVICE_ROLLING_LINKS
        or sync.rolling_window_hours < MIN_SERVICE_ROLLING_HOURS
        or sync.max_pending_episode_jobs > MAX_SERVICE_PENDING_JOBS
    ):
        raise ConfigError(
            "Keep automatic-service safety limits: apply_changes=false, one concurrent link, "
            "at most 20 links per cycle, 5,000 per window of at least 168 hours, "
            "and 10 pending episode jobs. Scope consent authorizes automatic linking."
        )
    return document


def load_service_settings(store: StateStore, bootstrap: BootstrapSettings) -> dict[str, Any]:
    return _decode_service_settings(store.get_meta(SETTINGS_META), bootstrap)


def _decode_service_settings(encoded: str | None, bootstrap: BootstrapSettings) -> dict[str, Any]:
    if encoded is None:
        return _defaults(bootstrap)
    try:
        document = json.loads(encoded)
    except ValueError:
        raise ConfigError("Saved service settings are invalid.") from None
    if not isinstance(document, dict):
        raise ConfigError("Saved service settings are invalid.")
    return _validate(document)


def configured_bootstrap(bootstrap: BootstrapSettings) -> BootstrapSettings:
    if not bootstrap.database_path.exists():
        return bootstrap
    with open_state(bootstrap.database_path) as store:
        settings = load_service_settings(store, bootstrap)
    return replace(bootstrap, sonarr_url=settings["sonarr_url"], shoko_url=settings["shoko_url"])


def apply_service_settings(settings: Settings, document: dict[str, Any]) -> Settings:
    sync, mappings, safety, campaign = parse_runtime_options(
        {name: document[name] for name in ("sync", "mappings", "safety", "campaign")}
    )
    return replace(settings, sync=sync, mappings=mappings, safety=safety, campaign=campaign)


def _digest(document: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(document, sort_keys=True).encode("utf-8")).hexdigest()


class ServiceSettingsController:
    def __init__(self, bootstrap: BootstrapSettings, wake: Callable[[], None]) -> None:
        self.bootstrap = bootstrap
        self.wake = wake

    def document(self) -> dict[str, Any]:
        with open_state(self.bootstrap.database_path) as store:
            return load_service_settings(store, self.bootstrap)

    def interval(self) -> float:
        return float(self.document()["sync"]["interval_seconds"])

    def view(self) -> DashboardSettingsView:
        document = self.document()
        with open_state(self.bootstrap.database_path) as store:
            record = OnboardingManager(store).record()
        normal = [
            ("sonarr_url", "Sonarr URL", document["sonarr_url"]),
            ("shoko_url", "Shoko URL", document["shoko_url"]),
            *[(name, label, str(document["sync"][name])) for name, label in NORMAL_SYNC_FIELDS],
            (
                "refresh_hours",
                "Episode mapping refresh interval (hours)",
                str(document["mappings"]["refresh_hours"]),
            ),
        ]
        advanced = {
            "sync": {
                key: value
                for key, value in document["sync"].items()
                if key not in dict(NORMAL_SYNC_FIELDS)
            },
            "mappings": {
                key: value for key, value in document["mappings"].items() if key != "refresh_hours"
            },
            "safety": document["safety"],
            "campaign": document["campaign"],
        }
        pending = (
            document["sonarr_url"] != self.bootstrap.sonarr_url
            or document["shoko_url"] != self.bootstrap.shoko_url
        )
        return DashboardSettingsView(
            tuple(normal),
            yaml.safe_dump(advanced, sort_keys=False),
            _digest(document),
            "Restart the container to use the saved connection URLs, then confirm setup."
            if pending
            else "Settings take effect on the next service check.",
            str(self.bootstrap.database_path),
            self.bootstrap.secret_env_vars,
            () if record is None else record.binding.roots,
            ()
            if record is None
            else tuple((item.sonarr, item.shoko) for item in record.binding.path_mappings),
            allow_missing_metadata_refresh=document["allow_missing_metadata_refresh"],
        )

    def save(self, form: Mapping[str, str]) -> str:
        try:
            with open_state(self.bootstrap.database_path) as store:
                expected_json = store.get_meta(SETTINGS_META)
            current = _decode_service_settings(expected_json, self.bootstrap)
            if not hmac.compare_digest(form.get("settings_digest", ""), _digest(current)):
                raise ConfigError("Settings changed in another window. Review the current values.")
            advanced = yaml.safe_load(form.get("advanced_yaml", ""))
            if not isinstance(advanced, dict) or set(advanced) != {
                "sync",
                "mappings",
                "safety",
                "campaign",
            }:
                raise ConfigError(
                    "Advanced settings must contain sync, mappings, safety and campaign."
                )
            if not all(isinstance(advanced[name], dict) for name in advanced):
                raise ConfigError("Each advanced settings section must be a mapping.")
            # Normal controls have precedence; duplicate advanced fields are rejected.
            if (
                set(advanced["sync"]) & set(dict(NORMAL_SYNC_FIELDS))
                or "refresh_hours" in advanced["mappings"]
            ):
                raise ConfigError(
                    "Use the normal controls for the displayed timing and budget fields."
                )
            if form.get("allow_missing_metadata_refresh", "") not in ("", "1"):
                raise ConfigError("Invalid metadata retrieval choice.")
            document = {
                **advanced,
                "allow_missing_metadata_refresh": form.get("allow_missing_metadata_refresh") == "1",
                "sonarr_url": form.get("sonarr_url", ""),
                "shoko_url": form.get("shoko_url", ""),
            }
            for name, _label in NORMAL_SYNC_FIELDS:
                advanced["sync"][name] = yaml.safe_load(form.get(name, ""))
            advanced["mappings"]["refresh_hours"] = yaml.safe_load(form.get("refresh_hours", ""))
            _validate(document)
            with open_state(self.bootstrap.database_path) as store:
                if not store.save_service_settings_if_current(
                    expected_json,
                    json.dumps(document, sort_keys=True),
                    authorization_changed=(
                        document["allow_missing_metadata_refresh"]
                        != current["allow_missing_metadata_refresh"]
                    )
                    or any(document[name] != current[name] for name in ("sonarr_url", "shoko_url")),
                    iso=datetime.now(UTC).isoformat(),
                ):
                    raise ConfigError(
                        "Settings changed in another window. Review the current values."
                    )
            self.wake()
            changed = (
                document["allow_missing_metadata_refresh"]
                != current["allow_missing_metadata_refresh"]
            )
            return (
                "Settings saved. "
                + (
                    "Confirm setup again for the changed metadata retrieval choice. "
                    if changed
                    else ""
                )
                + self.view().message
            )
        except ConfigError as exc:
            return f"Settings were not saved. {exc}"
        except (ValueError, TypeError, yaml.YAMLError):
            return (
                "Settings were not saved. Check the values and keep the documented safety limits."
            )
