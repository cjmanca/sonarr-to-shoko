"""Connection-only settings for the normal Docker service startup.

The initial setup cannot yet construct the full legacy :class:`Settings`: anime roots
and path mappings are discovered or confirmed after making read-only API requests.
This small value object carries only service URLs and secret *references* until then.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from .config import SecretRef, validate_base_url
from .errors import ConfigError

SONARR_URL_ENV = "SONARR_URL"
SONARR_API_KEY_ENV = "SONARR_API_KEY"
SHOKO_URL_ENV = "SHOKO_URL"
SHOKO_API_KEY_ENV = "SHOKO_API_KEY"
DEFAULT_DATABASE_PATH = Path("/data/state.sqlite3")


@dataclass(frozen=True)
class BootstrapSettings:
    """The four connection values needed before safe scope discovery."""

    sonarr_url: str
    sonarr_api_key: SecretRef
    shoko_url: str
    shoko_api_key: SecretRef
    database_path: Path = DEFAULT_DATABASE_PATH

    @property
    def secret_env_vars(self) -> tuple[str, str]:
        """Names of the environment variables containing API keys, never their values."""
        return (self.sonarr_api_key.env_var, self.shoko_api_key.env_var)


def _required(environ: Mapping[str, str], name: str) -> str:
    value = environ.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{name}: required and must not be empty")
    return value


def load_bootstrap_settings(*, environ: Mapping[str, str] | None = None) -> BootstrapSettings:
    """Load the four required service settings, defaulting only persistent state location.

    API key strings remain in the supplied/process environment and are never copied into
    this object. API clients resolve the ``SecretRef`` only when they are constructed.
    """
    env = os.environ if environ is None else environ
    _required(env, SONARR_API_KEY_ENV)
    _required(env, SHOKO_API_KEY_ENV)
    return BootstrapSettings(
        sonarr_url=validate_base_url(_required(env, SONARR_URL_ENV), SONARR_URL_ENV),
        sonarr_api_key=SecretRef(SONARR_API_KEY_ENV),
        shoko_url=validate_base_url(_required(env, SHOKO_URL_ENV), SHOKO_URL_ENV),
        shoko_api_key=SecretRef(SHOKO_API_KEY_ENV),
    )
