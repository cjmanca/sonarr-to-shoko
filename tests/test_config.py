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
    _CAMPAIGN_KEYS,
    _DATABASE_KEYS,
    _MAPPINGS_KEYS,
    _SAFETY_KEYS,
    _SERVICE_KEYS,
    _SYNC_KEYS,
    _TIMEOUT_KEYS,
    _TOP_LEVEL_KEYS,
    APPLY_ENV_VAR,
    DEFAULT_APPLY_LOCK_LEASE_SECONDS,
    DEFAULT_ATTESTATION_VALIDITY_HOURS,
    DEFAULT_ATTESTATION_WARN_SECONDS,
    DEFAULT_CONNECT_TIMEOUT_SECONDS,
    DEFAULT_FULL_RECONCILE_HOURS,
    DEFAULT_INTERVAL_SECONDS,
    DEFAULT_JITTER_RATIO,
    DEFAULT_MAPPING_CHURN_LIMIT,
    DEFAULT_MAPPING_SOURCE_URL,
    DEFAULT_MAX_BATCH_SIZE,
    DEFAULT_MAX_CAMPAIGN_FILES,
    DEFAULT_MAX_CYCLE_SECONDS,
    DEFAULT_MAX_NEW_PER_CYCLE,
    DEFAULT_MAX_PENDING_EPISODE_JOBS,
    DEFAULT_MAX_RECOVERY_ROWS_PER_CYCLE,
    DEFAULT_NEW_FILE_GRACE_SECONDS,
    DEFAULT_QUEUE_BLOCKED_TOLERANCE,
    DEFAULT_READ_TIMEOUT_SECONDS,
    DEFAULT_REFRESH_HOURS,
    DEFAULT_ROLLING_NEW_LIMIT,
    DEFAULT_ROLLING_WINDOW_HOURS,
    DEFAULT_VERIFICATION_TIMEOUT_SECONDS,
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
SMALL_CYCLE_QUOTA = 5
SMALL_CHURN_LIMIT = 10
SHORT_WINDOW_HOURS = 48.0
MID_JITTER_RATIO = 0.25
LEASE_VALIDITY_HOURS = 24.0
LEASE_WARN_SECONDS = 60.0
CAMPAIGN_FILES_50 = 50
CAMPAIGN_BATCH_3 = 3
QUEUE_TOLERANCE_5 = 5
ATTESTED_VERSION_534 = "5.3.4"


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
    assert DEFAULT_ROLLING_NEW_LIMIT == 5000
    assert settings.sync.max_new_per_cycle == DEFAULT_MAX_NEW_PER_CYCLE == 20
    assert settings.sync.rolling_new_limit == 5000
    assert settings.sync.max_pending_episode_jobs == DEFAULT_MAX_PENDING_EPISODE_JOBS == 10
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


# ------------------------------------------------------------------ M7-a sync budgets


def _sync_document(**sync_extra: Any) -> str:
    document = _document_with(sonarr_url="http://s:8989", shoko_url="http://k:8111")
    document["sync"].update(sync_extra)
    return _dump(document)


def test_new_sync_defaults_apply_when_omitted() -> None:
    settings = parse_settings(_sync_document())
    sync = settings.sync
    assert sync.max_new_per_cycle == DEFAULT_MAX_NEW_PER_CYCLE
    assert sync.rolling_new_limit == DEFAULT_ROLLING_NEW_LIMIT
    assert sync.rolling_window_hours == DEFAULT_ROLLING_WINDOW_HOURS
    assert sync.max_pending_episode_jobs == DEFAULT_MAX_PENDING_EPISODE_JOBS
    assert sync.max_cycle_seconds == DEFAULT_MAX_CYCLE_SECONDS
    assert sync.max_recovery_rows_per_cycle == DEFAULT_MAX_RECOVERY_ROWS_PER_CYCLE
    assert sync.verification_timeout_seconds == DEFAULT_VERIFICATION_TIMEOUT_SECONDS
    assert sync.apply_lock_lease_seconds == DEFAULT_APPLY_LOCK_LEASE_SECONDS
    assert sync.jitter_ratio == DEFAULT_JITTER_RATIO
    assert sync.mapping_churn_limit == DEFAULT_MAPPING_CHURN_LIMIT
    assert sync.queue_blocked_tolerance == DEFAULT_QUEUE_BLOCKED_TOLERANCE


def test_example_config_documents_the_new_defaults() -> None:
    settings = load_settings(EXAMPLE_CONFIG)
    assert settings.sync.max_new_per_cycle == DEFAULT_MAX_NEW_PER_CYCLE
    assert settings.sync.jitter_ratio == DEFAULT_JITTER_RATIO
    assert settings.sync.mapping_churn_limit == DEFAULT_MAPPING_CHURN_LIMIT
    assert settings.sync.queue_blocked_tolerance == DEFAULT_QUEUE_BLOCKED_TOLERANCE


def test_new_sync_values_parse() -> None:
    settings = parse_settings(
        _sync_document(
            max_new_per_cycle=5,
            rolling_new_limit=50,
            rolling_window_hours=48,
            max_pending_episode_jobs=7,
            max_cycle_seconds=120,
            max_recovery_rows_per_cycle=25,
            verification_timeout_seconds=900,
            apply_lock_lease_seconds=600,
            jitter_ratio=0.25,
            mapping_churn_limit=10,
            queue_blocked_tolerance=5,
        )
    )
    assert settings.sync.max_new_per_cycle == SMALL_CYCLE_QUOTA
    assert settings.sync.rolling_window_hours == SHORT_WINDOW_HOURS
    assert settings.sync.jitter_ratio == MID_JITTER_RATIO
    assert settings.sync.mapping_churn_limit == SMALL_CHURN_LIMIT
    assert settings.sync.queue_blocked_tolerance == QUEUE_TOLERANCE_5


@pytest.mark.parametrize(
    ("key", "value", "match"),
    [
        ("max_new_per_cycle", -1, "must be at least 0"),
        ("rolling_new_limit", -5, "must be at least 0"),
        ("max_pending_episode_jobs", -2, "must be at least 0"),
        ("rolling_window_hours", 0, "must be greater than 0"),
        ("max_cycle_seconds", -1.0, "must be greater than 0"),
        ("max_recovery_rows_per_cycle", 0, "must be at least 1"),
        ("verification_timeout_seconds", 0, "must be greater than 0"),
        ("apply_lock_lease_seconds", 0, "must be greater than 0"),
        ("mapping_churn_limit", 0, "must be at least 1"),
        ("jitter_ratio", 0.6, "must be between 0 and 0.5"),
        ("jitter_ratio", -0.1, "must be between 0 and 0.5"),
        ("max_cycle_seconds", float("inf"), "must be a finite number"),
        ("rolling_window_hours", float("nan"), "must be a finite number"),
        ("max_new_per_cycle", 2.5, "must be a whole number"),
        ("queue_blocked_tolerance", -1, "must be at least 0"),
        ("queue_blocked_tolerance", 1.5, "must be a whole number"),
    ],
)
def test_new_sync_rejects_out_of_range_values(key: str, value: Any, match: str) -> None:
    with pytest.raises(ConfigError, match=match):
        parse_settings(_sync_document(**{key: value}))


def test_example_config_documents_every_loader_key() -> None:
    document = cast("dict[str, Any]", yaml.safe_load(EXAMPLE_CONFIG.read_text(encoding="utf-8")))
    example_top = set(document)
    assert example_top == set(_TOP_LEVEL_KEYS)
    assert set(document["sonarr"]) == set(_SERVICE_KEYS)
    assert set(document["shoko"]) == set(_SERVICE_KEYS)
    assert set(document["paths"]) == {"mappings"}
    assert set(document["mappings"]) == set(_MAPPINGS_KEYS)
    assert set(document["sync"]) == set(_SYNC_KEYS)
    assert set(document["sync"]["timeouts"]) == set(_TIMEOUT_KEYS)
    assert set(document["database"]) == set(_DATABASE_KEYS)
    assert set(document["safety"]) == set(_SAFETY_KEYS)
    assert set(document["campaign"]) == set(_CAMPAIGN_KEYS)


# ------------------------------------------------- M7-b safety / campaign fields


def _safety_document(**safety_extra: Any) -> str:
    document = _document_with(sonarr_url="http://s:8989", shoko_url="http://k:8111")
    document["safety"] = safety_extra
    return _dump(document)


def _campaign_document(**campaign_extra: Any) -> str:
    document = _document_with(sonarr_url="http://s:8989", shoko_url="http://k:8111")
    document["campaign"] = campaign_extra
    return _dump(document)


def test_safety_and_campaign_defaults_apply_when_sections_omitted() -> None:
    settings = parse_settings(_sync_document())
    assert settings.safety.attestation_file is None
    assert settings.safety.attestation_validity_hours == DEFAULT_ATTESTATION_VALIDITY_HOURS
    assert settings.safety.attestation_warn_seconds == DEFAULT_ATTESTATION_WARN_SECONDS
    assert settings.safety.attested_shoko_version is None
    assert settings.campaign.max_campaign_files == DEFAULT_MAX_CAMPAIGN_FILES
    assert settings.campaign.max_batch_size == DEFAULT_MAX_BATCH_SIZE


def test_example_config_documents_the_m7_b_fields() -> None:
    settings = load_settings(EXAMPLE_CONFIG)
    assert settings.safety.attestation_file == "/data/attestation.yaml"
    assert settings.safety.attestation_validity_hours == DEFAULT_ATTESTATION_VALIDITY_HOURS
    assert settings.safety.attestation_warn_seconds == DEFAULT_ATTESTATION_WARN_SECONDS
    assert settings.safety.attested_shoko_version is None
    assert settings.campaign.max_campaign_files == DEFAULT_MAX_CAMPAIGN_FILES
    assert settings.campaign.max_batch_size == DEFAULT_MAX_BATCH_SIZE


def test_safety_fields_parse() -> None:
    settings = parse_settings(
        _safety_document(
            attestation_file="/data/lease.yaml",
            attestation_validity_hours=24,
            attestation_warn_seconds=60,
            attested_shoko_version=ATTESTED_VERSION_534,
        )
    )
    assert settings.safety.attestation_file == "/data/lease.yaml"
    assert settings.safety.attestation_validity_hours == LEASE_VALIDITY_HOURS
    assert settings.safety.attestation_warn_seconds == LEASE_WARN_SECONDS
    assert settings.safety.attested_shoko_version == ATTESTED_VERSION_534


@pytest.mark.parametrize(
    ("value", "match"),
    [
        ("", "must be null or a non-empty version string"),
        (5.3, "must be null or a non-empty version string"),
        (True, "must be null or a non-empty version string"),
    ],
)
def test_attested_shoko_version_rejects_unusable_values(value: Any, match: str) -> None:
    with pytest.raises(ConfigError, match=match):
        parse_settings(_safety_document(attested_shoko_version=value))


def test_campaign_fields_parse() -> None:
    settings = parse_settings(_campaign_document(max_campaign_files=50, max_batch_size=3))
    assert settings.campaign.max_campaign_files == CAMPAIGN_FILES_50
    assert settings.campaign.max_batch_size == CAMPAIGN_BATCH_3
    # F7 default: the daemon drains approved campaigns automatically.
    assert settings.campaign.daemon_auto_apply is True


def test_campaign_daemon_auto_apply_parses_both_values() -> None:
    assert (
        parse_settings(_campaign_document(daemon_auto_apply=False)).campaign.daemon_auto_apply
        is False
    )
    assert (
        parse_settings(_campaign_document(daemon_auto_apply=True)).campaign.daemon_auto_apply
        is True
    )


def test_campaign_daemon_auto_apply_rejects_non_bool() -> None:
    with pytest.raises(ConfigError, match="daemon_auto_apply"):
        parse_settings(_campaign_document(daemon_auto_apply="yes"))


@pytest.mark.parametrize(
    ("key", "value", "match"),
    [
        ("attestation_validity_hours", 0, "must be greater than 0"),
        ("attestation_validity_hours", -1.0, "must be greater than 0"),
        ("attestation_warn_seconds", 0, "must be greater than 0"),
        ("attestation_warn_seconds", "hour", "must be a number"),
        ("attestation_file", "relative/lease.yaml", "absolute POSIX path"),
        ("bogus", 1, "unknown setting"),
    ],
)
def test_safety_rejects_out_of_range_values(key: str, value: Any, match: str) -> None:
    with pytest.raises(ConfigError, match=match):
        parse_settings(_safety_document(**{"attestation_file": None, key: value}))


@pytest.mark.parametrize(
    ("key", "value", "match"),
    [
        ("max_campaign_files", 0, "must be at least 1"),
        ("max_campaign_files", 1.5, "must be a whole number"),
        ("max_batch_size", 0, "must be at least 1"),
        ("max_batch_size", "10", "must be a whole number"),
    ],
)
def test_campaign_rejects_out_of_range_values(key: str, value: Any, match: str) -> None:
    with pytest.raises(ConfigError, match=match):
        parse_settings(_campaign_document(**{key: value}))
