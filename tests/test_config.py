"""Configuration loading, validation, secret handling and the write gate. Offline only."""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest
import yaml  # type: ignore[import-untyped]

from shoko_linker.config import (
    APPLY_ENV_VAR,
    DEFAULT_CONNECT_TIMEOUT_SECONDS,
    DEFAULT_FULL_RECONCILE_HOURS,
    DEFAULT_INTERVAL_SECONDS,
    DEFAULT_MAPPING_SOURCE_URL,
    DEFAULT_NEW_FILE_GRACE_SECONDS,
    DEFAULT_READ_TIMEOUT_SECONDS,
    DEFAULT_REFRESH_HOURS,
    Settings,
    apply_gate,
    apply_gate_status,
    load_settings,
    parse_settings,
)
from shoko_linker.errors import ConfigError

EXAMPLE_CONFIG = Path(__file__).resolve().parents[1] / "config.example.yaml"
DUMMY_SONARR_KEY = "dummy-sonarr-key-value-00000000"
DUMMY_SHOKO_KEY = "dummy-shoko-key-value-00000000"


def test_example_config_loads() -> None:
    settings = load_settings(EXAMPLE_CONFIG)
    assert settings.sonarr.url == "http://sonarr:8989"
    assert settings.shoko.url == "http://shoko:8111"
    assert settings.sonarr.allowed_roots == ("/data/media/anime",)
    assert settings.shoko.allowed_roots == ("/anime",)
    assert [m.sonarr for m in settings.path_mappings] == ["/data/media/anime"]
    assert settings.mappings.source == "anime-lists"
    assert settings.mappings.refresh_hours == DEFAULT_REFRESH_HOURS
    assert settings.mappings.overrides_file == "/config/overrides.yaml"
    assert settings.mappings.source_url == DEFAULT_MAPPING_SOURCE_URL
    assert settings.sync.apply_changes is False
    assert settings.sync.max_concurrent_links == 1
    assert settings.sync.connect_timeout_seconds == DEFAULT_CONNECT_TIMEOUT_SECONDS
    assert settings.sync.read_timeout_seconds == DEFAULT_READ_TIMEOUT_SECONDS
    assert settings.database.path == "/data/state.sqlite3"


def test_url_path_base_is_preserved() -> None:
    document = _document_with(
        sonarr_url="http://host:8989/sonarr/",
        shoko_url="https://shoko.example/shoko/api",
    )
    settings = parse_settings(_dump(document))
    assert settings.sonarr.url == "http://host:8989/sonarr"
    assert settings.shoko.url == "https://shoko.example/shoko/api"


def test_defaults_are_applied_when_optional_values_are_omitted() -> None:
    document = {
        "sonarr": {
            "url": "http://s:8989",
            "api_key_env": "SONARR_API_KEY",
            "allowed_roots": ["/anime"],
        },
        "shoko": {
            "url": "http://k:8111",
            "api_key_env": "SHOKO_API_KEY",
            "allowed_roots": ["/anime"],
        },
        "paths": {"mappings": [{"sonarr": "/anime", "shoko": "/anime"}]},
        "mappings": {"source": "anime-lists"},
        "sync": {},
        "database": {"path": "/data/state.sqlite3"},
    }
    settings = parse_settings(_dump(document))
    assert settings.sync.interval_seconds == DEFAULT_INTERVAL_SECONDS
    assert settings.sync.full_reconcile_interval_hours == DEFAULT_FULL_RECONCILE_HOURS
    assert settings.sync.new_file_grace_period_seconds == DEFAULT_NEW_FILE_GRACE_SECONDS
    assert settings.sync.apply_changes is False
    assert settings.sync.connect_timeout_seconds == DEFAULT_CONNECT_TIMEOUT_SECONDS
    assert settings.sync.read_timeout_seconds == DEFAULT_READ_TIMEOUT_SECONDS
    assert settings.mappings.refresh_hours == DEFAULT_REFRESH_HOURS
    assert settings.mappings.overrides_file is None


def test_settings_never_hold_key_values(base_document: dict[str, Any]) -> None:
    settings = parse_settings(_dump(base_document))
    assert DUMMY_SHOKO_KEY not in repr(settings)
    assert DUMMY_SONARR_KEY not in repr(settings)
    assert settings.shoko.api_key.env_var == "SHOKO_API_KEY"
    assert settings.secret_env_vars == ("SONARR_API_KEY", "SHOKO_API_KEY")


def test_secret_value_is_read_from_environment_at_access_time() -> None:
    settings = load_settings(EXAMPLE_CONFIG)
    assert settings.shoko.api_key.resolve() == DUMMY_SHOKO_KEY


def test_missing_secret_names_the_variable_not_a_value(
    base_document: dict[str, Any],
    write_document: Callable[[dict[str, Any]], Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SHOKO_API_KEY")
    with pytest.raises(ConfigError) as excinfo:
        load_settings(write_document(base_document))
    message = str(excinfo.value)
    assert "SHOKO_API_KEY" in message
    assert DUMMY_SHOKO_KEY not in message


def test_empty_shoko_allowed_roots_is_rejected(
    base_document: dict[str, Any],
    write_document: Callable[[dict[str, Any]], Path],
) -> None:
    base_document["shoko"]["allowed_roots"] = []
    with pytest.raises(ConfigError, match="shoko.allowed_roots"):
        load_settings(write_document(base_document))


def test_missing_sonarr_allowed_roots_is_rejected(
    base_document: dict[str, Any],
    write_document: Callable[[dict[str, Any]], Path],
) -> None:
    del base_document["sonarr"]["allowed_roots"]
    with pytest.raises(ConfigError, match="sonarr.allowed_roots"):
        load_settings(write_document(base_document))


def test_missing_paths_mapping_section_is_rejected(
    base_document: dict[str, Any],
    write_document: Callable[[dict[str, Any]], Path],
) -> None:
    del base_document["paths"]["mappings"]
    with pytest.raises(ConfigError, match="paths.mappings"):
        load_settings(write_document(base_document))


def test_empty_paths_mapping_list_is_rejected(
    base_document: dict[str, Any],
    write_document: Callable[[dict[str, Any]], Path],
) -> None:
    base_document["paths"]["mappings"] = []
    with pytest.raises(ConfigError, match="paths.mappings"):
        load_settings(write_document(base_document))


def test_nested_path_mappings_are_rejected(
    base_document: dict[str, Any],
    write_document: Callable[[dict[str, Any]], Path],
) -> None:
    base_document["paths"]["mappings"] = [
        {"sonarr": "/data/media", "shoko": "/media-root"},
        {"sonarr": "/data/media/anime", "shoko": "/anime"},
    ]
    with pytest.raises(ConfigError, match="nested"):
        load_settings(write_document(base_document))


def test_conflicting_path_mappings_are_rejected(
    base_document: dict[str, Any],
    write_document: Callable[[dict[str, Any]], Path],
) -> None:
    base_document["paths"]["mappings"] = [
        {"sonarr": "/data/media/anime", "shoko": "/anime"},
        {"sonarr": "/data/media/anime", "shoko": "/shoko-anime"},
    ]
    with pytest.raises(ConfigError, match="conflicting"):
        load_settings(write_document(base_document))


@pytest.mark.parametrize(
    "url",
    [
        "ftp://sonarr:8989",
        "sonarr:8989",
        "http:///no-host",
        "http://host:8989?apikey=leak",
        "http://user:password@host:8989",
        "http://host:8989/base#fragment",
        "",
    ],
)
def test_bad_urls_are_rejected(base_document: dict[str, Any], url: str) -> None:
    base_document["sonarr"]["url"] = url
    with pytest.raises(ConfigError, match="sonarr.url"):
        parse_settings(_dump(base_document))


def test_non_bool_apply_changes_is_rejected(base_document: dict[str, Any]) -> None:
    base_document["sync"]["apply_changes"] = "true"
    with pytest.raises(ConfigError, match="sync: apply_changes"):
        parse_settings(_dump(base_document))


def test_nonpositive_interval_is_rejected(base_document: dict[str, Any]) -> None:
    base_document["sync"]["interval_seconds"] = 0
    with pytest.raises(ConfigError, match="interval_seconds"):
        parse_settings(_dump(base_document))


def test_nonpositive_refresh_hours_is_rejected(base_document: dict[str, Any]) -> None:
    base_document["mappings"]["refresh_hours"] = 0
    with pytest.raises(ConfigError, match="refresh_hours"):
        parse_settings(_dump(base_document))


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
@pytest.mark.parametrize("section_key", ["interval_seconds", "refresh_hours"])
def test_non_finite_numbers_are_rejected(
    base_document: dict[str, Any], section_key: str, value: float
) -> None:
    section = (
        base_document["sync"] if section_key == "interval_seconds" else base_document["mappings"]
    )
    section[section_key] = value
    with pytest.raises(ConfigError, match="must be a finite number"):
        parse_settings(_dump(base_document))


def test_out_of_range_number_is_rejected(base_document: dict[str, Any]) -> None:
    base_document["mappings"]["refresh_hours"] = 10**400
    with pytest.raises(ConfigError, match="out of range"):
        parse_settings(_dump(base_document))


def test_non_utf8_config_file_is_reported(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_bytes(b"sonarr:\n  url: \xff\xfe\x00not-utf8\n")
    with pytest.raises(ConfigError, match="not valid UTF-8"):
        load_settings(path)


def test_max_concurrent_links_must_be_at_least_one(base_document: dict[str, Any]) -> None:
    base_document["sync"]["max_concurrent_links"] = 0
    with pytest.raises(ConfigError, match="max_concurrent_links"):
        parse_settings(_dump(base_document))


def test_mapping_source_is_fixed(base_document: dict[str, Any]) -> None:
    base_document["mappings"]["source"] = "my-homegrown-map"
    with pytest.raises(ConfigError, match="mappings.source"):
        parse_settings(_dump(base_document))


def test_source_url_defaults_to_the_canonical_constant(base_document: dict[str, Any]) -> None:
    settings = parse_settings(_dump(base_document))
    assert settings.mappings.source_url == DEFAULT_MAPPING_SOURCE_URL


def test_custom_source_url_is_preserved(base_document: dict[str, Any]) -> None:
    base_document["mappings"]["source_url"] = "https://example.com/mirror/anime-list-full.xml"
    settings = parse_settings(_dump(base_document))
    assert settings.mappings.source_url == "https://example.com/mirror/anime-list-full.xml"


@pytest.mark.parametrize(
    "raw_url",
    [
        "https://user@host.example/anime-list-full.xml",
        "https://host.example/anime-list-full.xml?ref=main",
        "https://host.example/anime-list-full.xml#fragment",
        "ftp://host.example/anime-list-full.xml",
        "not a url",
    ],
)
def test_invalid_source_url_is_rejected(base_document: dict[str, Any], raw_url: str) -> None:
    base_document["mappings"]["source_url"] = raw_url
    with pytest.raises(ConfigError, match="mappings.source_url"):
        parse_settings(_dump(base_document))


def test_unknown_keys_are_rejected(base_document: dict[str, Any]) -> None:
    base_document["shoko"]["allowed_root"] = ["/anime"]
    with pytest.raises(ConfigError, match="unknown setting"):
        parse_settings(_dump(base_document))


def test_empty_document_is_rejected() -> None:
    with pytest.raises(ConfigError, match="empty"):
        parse_settings("")


def test_malformed_yaml_becomes_a_config_error() -> None:
    with pytest.raises(ConfigError, match="not valid YAML"):
        parse_settings("sonarr:\n  url: [unclosed\n")


def test_unsafe_yaml_tags_are_refused() -> None:
    payload = "sonarr: !!python/object/apply:os.system ['echo pwned']\n"
    with pytest.raises(ConfigError, match="not valid YAML"):
        parse_settings(payload)


def test_unreadable_config_file_is_reported(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="cannot read configuration file"):
        load_settings(tmp_path / "does-not-exist.yaml")


def test_apply_gate_is_default_deny(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = load_settings(EXAMPLE_CONFIG)
    monkeypatch.setenv(APPLY_ENV_VAR, "1")
    assert apply_gate(settings) is False
    assert settings.apply_enabled is False


def test_apply_gate_requires_operator_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _with_apply_changes(load_settings(EXAMPLE_CONFIG))
    monkeypatch.delenv(APPLY_ENV_VAR, raising=False)
    assert apply_gate(settings) is False
    assert settings.apply_enabled is False


def test_apply_gate_opens_only_with_both(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _with_apply_changes(load_settings(EXAMPLE_CONFIG))
    monkeypatch.setenv(APPLY_ENV_VAR, "1")
    assert apply_gate(settings) is True
    assert settings.apply_enabled is True
    status = apply_gate_status(settings)
    assert status.setting_enabled is True
    assert status.operator_opt_in is True
    assert "ALLOWED" in status.describe()


def test_apply_gate_rejects_partial_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _with_apply_changes(load_settings(EXAMPLE_CONFIG))
    monkeypatch.setenv(APPLY_ENV_VAR, "true")
    assert apply_gate(settings) is False


def _with_apply_changes(settings: Settings) -> Settings:
    return replace(settings, sync=replace(settings.sync, apply_changes=True))


def _document_with(*, sonarr_url: str, shoko_url: str) -> dict[str, Any]:
    return {
        "sonarr": {
            "url": sonarr_url,
            "api_key_env": "SONARR_API_KEY",
            "allowed_roots": ["/anime"],
        },
        "shoko": {
            "url": shoko_url,
            "api_key_env": "SHOKO_API_KEY",
            "allowed_roots": ["/anime"],
        },
        "paths": {"mappings": [{"sonarr": "/anime", "shoko": "/anime"}]},
        "mappings": {"source": "anime-lists"},
        "sync": {},
        "database": {"path": "/data/state.sqlite3"},
    }


def _dump(document: dict[str, Any]) -> str:
    return cast("str", yaml.safe_dump(document))
