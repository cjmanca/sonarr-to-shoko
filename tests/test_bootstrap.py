"""Connection-only container configuration, secret-safe and independent of YAML."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pytest

from shoko_linker.bootstrap import BootstrapSettings, load_bootstrap_settings
from shoko_linker.errors import ConfigError

SONARR_SECRET = "sonarr-api-key-never-print-4bfa"
SHOKO_SECRET = "shoko-api-key-never-print-c910"
BASE_ENV = {
    "SONARR_URL": "http://sonarr:8989/sonarr",
    "SONARR_API_KEY": SONARR_SECRET,
    "SHOKO_URL": "https://shoko.example:8111",
    "SHOKO_API_KEY": SHOKO_SECRET,
}


def _load_bootstrap(environ: Mapping[str, str]) -> BootstrapSettings:
    return load_bootstrap_settings(environ=environ)


def test_connection_settings_need_only_the_four_service_environment_values() -> None:
    settings = _load_bootstrap(BASE_ENV)

    assert settings.sonarr_url == "http://sonarr:8989/sonarr"
    assert settings.shoko_url == "https://shoko.example:8111"
    assert settings.sonarr_api_key.env_var == "SONARR_API_KEY"
    assert settings.shoko_api_key.env_var == "SHOKO_API_KEY"
    assert settings.secret_env_vars == ("SONARR_API_KEY", "SHOKO_API_KEY")
    assert settings.database_path == Path("/data/state.sqlite3")
    assert settings.sonarr_api_key.resolve(BASE_ENV) == SONARR_SECRET
    assert settings.shoko_api_key.resolve(BASE_ENV) == SHOKO_SECRET
    assert SONARR_SECRET not in repr(settings)
    assert SHOKO_SECRET not in repr(settings)


@pytest.mark.parametrize("variable", tuple(BASE_ENV))
def test_missing_connection_environment_value_names_only_the_variable(variable: str) -> None:
    environ = dict(BASE_ENV)
    del environ[variable]

    with pytest.raises(ConfigError) as excinfo:
        _load_bootstrap(environ)

    message = str(excinfo.value)
    assert variable in message
    assert SONARR_SECRET not in message
    assert SHOKO_SECRET not in message


@pytest.mark.parametrize(
    ("variable", "value"),
    [
        ("SONARR_URL", "ftp://sonarr:8989"),
        ("SHOKO_URL", "http:///missing-host"),
        ("SONARR_URL", "http://user:password@sonarr:8989"),
        ("SHOKO_URL", "http://shoko:8111/?apikey=secret-value"),
        ("SONARR_URL", "http://sonarr:not-a-port"),
        ("SONARR_URL", "http://sonarr:0"),
        ("SHOKO_URL", "http://[invalid-ipv6"),
    ],
)
def test_invalid_connection_urls_fail_without_echoing_url_credentials(
    variable: str, value: str
) -> None:
    environ = dict(BASE_ENV)
    environ[variable] = value

    with pytest.raises(ConfigError) as excinfo:
        _load_bootstrap(environ)

    message = str(excinfo.value)
    assert variable in message
    assert "password" not in message
    assert "secret-value" not in message
    assert SONARR_SECRET not in message
    assert SHOKO_SECRET not in message
