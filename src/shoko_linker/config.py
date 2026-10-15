"""Typed settings loaded from YAML with ``yaml.safe_load`` only.

Secrets are configured by **environment-variable name**. The value is read from the
process environment at access time and is never stored in settings, ``repr``,
logs, reports, or the parsed document. A missing secret raises an error naming the
variable, never its value.

Live writes are default-deny: :func:`apply_gate` is the single gate function and
requires both ``sync.apply_changes: true`` **and** the operator opt-in environment
variable ``SHOKO_LINKER_ALLOW_APPLY=1``. No CLI flag can enable writes on its own.
"""

from __future__ import annotations

import math
import os
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, cast
from urllib.parse import urlsplit, urlunsplit

import yaml  # type: ignore[import-untyped]

from .errors import ConfigError, PathError
from .paths import PathMapping, normalize_posix, validate_mappings

APPLY_ENV_VAR: Final[str] = "SHOKO_LINKER_ALLOW_APPLY"
MAPPING_SOURCE: Final[str] = "anime-lists"
DEFAULT_CONFIG_PATH: Final[str] = "config.yaml"
DEFAULT_MAPPING_SOURCE_URL: Final[str] = (
    "https://raw.githubusercontent.com/Anime-Lists/anime-lists/master/anime-list-full.xml"
)

DEFAULT_CONNECT_TIMEOUT_SECONDS: Final[float] = 10.0
DEFAULT_READ_TIMEOUT_SECONDS: Final[float] = 30.0
DEFAULT_INTERVAL_SECONDS: Final[float] = 300.0
DEFAULT_FULL_RECONCILE_HOURS: Final[float] = 24.0
DEFAULT_NEW_FILE_GRACE_SECONDS: Final[float] = 300.0
DEFAULT_REFRESH_HOURS: Final[float] = 168.0
DEFAULT_MAX_CONCURRENT_LINKS: Final[int] = 1
DEFAULT_MAX_NEW_PER_CYCLE: Final[int] = 20
DEFAULT_ROLLING_NEW_LIMIT: Final[int] = 200
DEFAULT_ROLLING_WINDOW_HOURS: Final[float] = 168.0
DEFAULT_MAX_PENDING_EPISODE_JOBS: Final[int] = 10
DEFAULT_MAX_CYCLE_SECONDS: Final[float] = 300.0
DEFAULT_MAX_RECOVERY_ROWS_PER_CYCLE: Final[int] = 100
DEFAULT_VERIFICATION_TIMEOUT_SECONDS: Final[float] = 3600.0
DEFAULT_APPLY_LOCK_LEASE_SECONDS: Final[float] = 3600.0
DEFAULT_JITTER_RATIO: Final[float] = 0.1
DEFAULT_MAPPING_CHURN_LIMIT: Final[int] = 500
DEFAULT_ATTESTATION_VALIDITY_HOURS: Final[float] = 168.0
DEFAULT_ATTESTATION_WARN_SECONDS: Final[float] = 3600.0
DEFAULT_MAX_CAMPAIGN_FILES: Final[int] = 7000
DEFAULT_MAX_BATCH_SIZE: Final[int] = 10
DEFAULT_CAMPAIGN_DAEMON_AUTO_APPLY: Final[bool] = True
DEFAULT_QUEUE_BLOCKED_TOLERANCE: Final[int] = 0
MAX_JITTER_RATIO: Final[float] = 0.5

_TOP_LEVEL_KEYS: Final[frozenset[str]] = frozenset(
    {"sonarr", "shoko", "paths", "mappings", "sync", "database", "safety", "campaign"}
)
_SERVICE_KEYS: Final[frozenset[str]] = frozenset({"url", "api_key_env", "allowed_roots"})
_SYNC_KEYS: Final[frozenset[str]] = frozenset(
    {
        "interval_seconds",
        "full_reconcile_interval_hours",
        "new_file_grace_period_seconds",
        "apply_changes",
        "max_concurrent_links",
        "timeouts",
        "max_new_per_cycle",
        "rolling_new_limit",
        "rolling_window_hours",
        "max_pending_episode_jobs",
        "max_cycle_seconds",
        "max_recovery_rows_per_cycle",
        "verification_timeout_seconds",
        "apply_lock_lease_seconds",
        "jitter_ratio",
        "mapping_churn_limit",
        "queue_blocked_tolerance",
    }
)
_MAPPINGS_KEYS: Final[frozenset[str]] = frozenset(
    {"source", "refresh_hours", "overrides_file", "source_url"}
)
_SAFETY_KEYS: Final[frozenset[str]] = frozenset(
    {
        "attestation_file",
        "attestation_validity_hours",
        "attestation_warn_seconds",
        "attested_shoko_version",
    }
)
_CAMPAIGN_KEYS: Final[frozenset[str]] = frozenset(
    {"max_campaign_files", "max_batch_size", "daemon_auto_apply"}
)
_TIMEOUT_KEYS: Final[frozenset[str]] = frozenset({"connect_seconds", "read_seconds"})
_DATABASE_KEYS: Final[frozenset[str]] = frozenset({"path"})
_ENV_NAME_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ALLOWED_SCHEMES: Final[frozenset[str]] = frozenset({"http", "https"})


@dataclass(frozen=True)
class SecretRef:
    """A secret referenced by environment-variable name; the value is never stored."""

    env_var: str

    def resolve(self, environ: Mapping[str, str] | None = None) -> str:
        """Read the secret from the environment now. Raises ConfigError naming the variable."""
        env = os.environ if environ is None else environ
        value = env.get(self.env_var)
        if not value:
            raise ConfigError(
                f"missing API key environment variable: {self.env_var} "
                "(set it; its value is never stored)"
            )
        return value


@dataclass(frozen=True)
class ServiceSettings:
    """One service endpoint: base URL (path base preserved), key reference, root allowlist."""

    url: str
    api_key: SecretRef
    allowed_roots: tuple[str, ...]


@dataclass(frozen=True)
class MappingSettings:
    """Anime-Lists mapping source configuration."""

    source: str
    refresh_hours: float
    overrides_file: str | None
    source_url: str


@dataclass(frozen=True)
class SyncSettings:
    """Scheduling, budget and write-safety configuration.

    M7-a budgets map onto :class:`shoko_linker.state.BudgetLimits` as:
    ``per_cycle_new = max_new_per_cycle``, ``rolling_new = rolling_new_limit``,
    ``rolling_window_hours = rolling_window_hours``,
    ``max_pending_episode_jobs = max_pending_episode_jobs`` and
    ``max_pending_files = max_new_per_cycle`` (a cycle never submits more files than
    ``max_new_per_cycle``, so pending files are bounded by the same ceiling; 0 disables
    submissions).

    ``queue_blocked_tolerance`` is the fail-closed Shoko queue guard: when
    ``GET /Queue`` reports ``BlockedCount`` above the tolerance, apply is refused for
    the whole cycle (``ManualLinkJob`` delivery cannot be trusted on a blocked queue).
    Default 0 refuses apply whenever any queue work is blocked.
    """

    interval_seconds: float
    full_reconcile_interval_hours: float
    new_file_grace_period_seconds: float
    apply_changes: bool
    max_concurrent_links: int
    connect_timeout_seconds: float
    read_timeout_seconds: float
    max_new_per_cycle: int
    rolling_new_limit: int
    rolling_window_hours: float
    max_pending_episode_jobs: int
    max_cycle_seconds: float
    max_recovery_rows_per_cycle: int
    verification_timeout_seconds: float
    apply_lock_lease_seconds: float
    jitter_ratio: float
    mapping_churn_limit: int
    queue_blocked_tolerance: int


@dataclass(frozen=True)
class DatabaseSettings:
    """SQLite state file location (persistent volume)."""

    path: str


@dataclass(frozen=True)
class SafetySettings:
    """Safety attestation lease configuration (M7-b).

    ``attestation_file`` is the operator-created daemon-scope lease record path (``None`` =
    no lease exists, so every apply stays refused with ``attestation_invalid``). Each
    ``campaign:<id>`` authorization lives in a deterministic sibling in the same directory
    (``<stem>.campaign-<id>.yaml`` — see :func:`shoko_linker.attestation.campaign_lease_path`),
    so daemon and campaign authorizations are independent, simultaneously valid files on the
    persisted safety volume; a campaign record written directly at ``attestation_file`` keeps
    working for that campaign (backward compatibility). The lease expires
    after a finite validity window; ``attestation_validity_hours`` is the hard cap on any
    lease's lifetime (``attestation record --hours`` may not exceed it) and
    ``attestation_warn_seconds`` is the pre-expiry warning lead. The trust anchor is the
    operator: the tool cannot prove Docker mounts are read-only, so an unattended
    indefinite writable daemon is incompatible with an expiring physical attestation —
    the daemon degrades to read-only reporting after expiry (documented tradeoff).

    ``attested_shoko_version`` pins the exact Shoko version string the operator
    inspected (``null`` = no pin). When configured, a lease whose recorded version
    differs from it reports ``stale_version`` and every apply is suppressed until the
    lease is re-recorded — a Shoko upgrade never silently inherits an old lease.
    """

    attestation_file: str | None
    attestation_validity_hours: float
    attestation_warn_seconds: float
    attested_shoko_version: str | None = None


@dataclass(frozen=True)
class CampaignSettings:
    """Backlog-campaign ceilings (M7-b).

    ``max_campaign_files`` bounds one approved plan (and the default ``campaign prepare
    --limit``); ``max_batch_size`` caps files processed per ``campaign apply``
    invocation. They do not bypass the write gate, the attestation lease, the budgets in
    :class:`shoko_linker.state` or the plan approval digest.

    ``daemon_auto_apply`` (F7) makes the daemon drain **approved** backlog campaigns
    automatically — one bounded batch per apply cycle, after the operator's canary. It
    never approves a campaign, never expands an approved set, and never opens
    the write gate: the gate, the campaign-scope attestation lease, and every campaign guard
    stay authoritative. Set it false to keep campaigns manual (CLI ``campaign apply`` only).
    """

    max_campaign_files: int
    max_batch_size: int
    daemon_auto_apply: bool = DEFAULT_CAMPAIGN_DAEMON_AUTO_APPLY


@dataclass(frozen=True)
class ApplyGateStatus:
    """Components of the write-safety decision, for human-readable reporting."""

    setting_enabled: bool
    operator_opt_in: bool
    allowed: bool

    def describe(self) -> str:
        return (
            f"sync.apply_changes={self.setting_enabled!r}, "
            f"{APPLY_ENV_VAR}={'present' if self.operator_opt_in else 'absent'}, "
            f"live writes={'ALLOWED' if self.allowed else 'disabled'}"
        )


DEFAULT_SAFETY_SETTINGS: Final[SafetySettings] = SafetySettings(
    attestation_file=None,
    attestation_validity_hours=DEFAULT_ATTESTATION_VALIDITY_HOURS,
    attestation_warn_seconds=DEFAULT_ATTESTATION_WARN_SECONDS,
    attested_shoko_version=None,
)
DEFAULT_CAMPAIGN_SETTINGS: Final[CampaignSettings] = CampaignSettings(
    max_campaign_files=DEFAULT_MAX_CAMPAIGN_FILES,
    max_batch_size=DEFAULT_MAX_BATCH_SIZE,
    daemon_auto_apply=DEFAULT_CAMPAIGN_DAEMON_AUTO_APPLY,
)


@dataclass(frozen=True)
class Settings:
    """Validated configuration. Frozen, hashable, and free of secret values."""

    config_path: Path | None
    sonarr: ServiceSettings
    shoko: ServiceSettings
    path_mappings: tuple[PathMapping, ...]
    mappings: MappingSettings
    sync: SyncSettings
    database: DatabaseSettings
    safety: SafetySettings = DEFAULT_SAFETY_SETTINGS
    campaign: CampaignSettings = DEFAULT_CAMPAIGN_SETTINGS
    anime_only: bool = False

    @property
    def apply_enabled(self) -> bool:
        """Live writes are permitted only through :func:`apply_gate`."""
        return apply_gate(self)

    @property
    def secret_env_vars(self) -> tuple[str, str]:
        """The environment-variable names holding API keys (names, never values)."""
        return (self.sonarr.api_key.env_var, self.shoko.api_key.env_var)


def apply_gate(settings: Settings, environ: Mapping[str, str] | None = None) -> bool:
    """The single default-deny gate for live Shoko writes.

    Returns True only when ``sync.apply_changes`` is true **and** the operator opt-in
    environment variable ``SHOKO_LINKER_ALLOW_APPLY`` is exactly ``1``. Everything
    else — including any CLI flag — refuses writes.
    """
    return apply_gate_status(settings, environ).allowed


def apply_gate_status(
    settings: Settings, environ: Mapping[str, str] | None = None
) -> ApplyGateStatus:
    """Explain the gate decision so refusals name both required conditions."""
    env = os.environ if environ is None else environ
    setting_enabled = settings.sync.apply_changes
    operator_opt_in = env.get(APPLY_ENV_VAR) == "1"
    return ApplyGateStatus(
        setting_enabled=setting_enabled,
        operator_opt_in=operator_opt_in,
        allowed=setting_enabled and operator_opt_in,
    )


def load_settings(config_path: str | Path) -> Settings:
    """Read and validate a YAML configuration file."""
    path = Path(config_path)
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError as exc:
        raise ConfigError(
            f"configuration file {path} is not valid UTF-8 text "
            f"(at byte offset {exc.start}: {exc.reason})"
        ) from exc
    except OSError as exc:
        detail = exc.strerror or str(exc)
        raise ConfigError(f"cannot read configuration file {path}: {detail}") from exc
    return parse_settings(text, config_path=path)


def parse_settings(text: str, *, config_path: Path | None = None) -> Settings:
    """Validate configuration ``text`` and return typed settings."""
    doc = _document(text)
    sonarr = _service(_section(doc, "sonarr"), "sonarr")
    shoko = _service(_section(doc, "shoko"), "shoko")
    paths_section = _section(doc, "paths")
    _known_keys(paths_section, {"mappings"}, "paths")
    path_mappings = _path_mappings(paths_section)
    mappings = _mappings(_section(doc, "mappings"))
    sync = _sync(_section(doc, "sync"))
    database = _database(_section(doc, "database"))
    safety = _safety(doc.get("safety"))
    campaign = _campaign(doc.get("campaign"))
    settings = Settings(
        config_path=config_path,
        sonarr=sonarr,
        shoko=shoko,
        path_mappings=path_mappings,
        mappings=mappings,
        sync=sync,
        database=database,
        safety=safety,
        campaign=campaign,
    )
    _require_secret_present(settings)
    return settings


# --------------------------------------------------------------------------- parsing


def _document(text: str) -> Mapping[str, Any]:
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigError(f"configuration is not valid YAML: {exc}") from exc
    if raw is None:
        raise ConfigError("configuration file is empty")
    if not isinstance(raw, dict):
        raise ConfigError("top level: expected a YAML mapping")
    doc = cast("dict[str, Any]", raw)
    _known_keys(doc, _TOP_LEVEL_KEYS, "top level")
    return doc


def _section(doc: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    raw = doc.get(key)
    if raw is None:
        raise ConfigError(f"{key}: section is required")
    if not isinstance(raw, dict):
        raise ConfigError(f"{key}: expected a mapping")
    return cast("Mapping[str, Any]", raw)


def _known_keys(section: Mapping[Any, Any], allowed: Iterable[str], field_name: str) -> None:
    allowed_keys = frozenset(allowed)
    for key in section:
        if not isinstance(key, str):
            raise ConfigError(f"{field_name}: setting names must be strings (got {key!r})")
        if key not in allowed_keys:
            raise ConfigError(f"{field_name}: unknown setting {key!r}")


def _service(section: Mapping[str, Any], name: str) -> ServiceSettings:
    _known_keys(section, _SERVICE_KEYS, name)
    return ServiceSettings(
        url=validate_base_url(section.get("url"), f"{name}.url"),
        api_key=_secret_ref(section, name),
        allowed_roots=_allowed_roots(section, name),
    )


def validate_base_url(raw: Any, field_name: str) -> str:
    """Validate an HTTP(S) service URL without echoing credentials or malformed input."""
    try:
        return _base_url(raw, field_name)
    except ValueError:
        raise ConfigError(f"{field_name}: must be a valid HTTP(S) URL") from None


def _base_url(raw: Any, field_name: str) -> str:
    """Validate a service URL and keep its path base (``http://host/sonarr`` stays)."""
    if not isinstance(raw, str) or not raw.strip():
        raise ConfigError(f"{field_name}: must be a non-empty URL string")
    parts = urlsplit(raw.strip())
    scheme = parts.scheme.lower()
    if scheme not in _ALLOWED_SCHEMES:
        raise ConfigError(f"{field_name}: scheme must be http or https (got {parts.scheme!r})")
    if not parts.hostname:
        raise ConfigError(f"{field_name}: must include a host")
    try:
        port = parts.port
    except ValueError:
        raise ConfigError(f"{field_name}: must contain a valid port") from None
    if port == 0:
        raise ConfigError(f"{field_name}: port must be greater than zero")
    if parts.username or parts.password:
        raise ConfigError(
            f"{field_name}: must not embed credentials (API keys come from the environment)"
        )
    if parts.query or parts.fragment:
        raise ConfigError(
            f"{field_name}: must not include a query or fragment (keep API keys out of URLs)"
        )
    base_path = parts.path.rstrip("/")
    return urlunsplit((scheme, parts.netloc, base_path, "", ""))


def _secret_ref(section: Mapping[str, Any], name: str) -> SecretRef:
    raw = section.get("api_key_env")
    if not isinstance(raw, str) or not raw.strip():
        raise ConfigError(
            f"{name}.api_key_env: required — the NAME of the environment variable "
            "holding the API key"
        )
    env_var = raw.strip()
    if not _ENV_NAME_RE.match(env_var):
        raise ConfigError(f"{name}.api_key_env: must be a valid environment variable name")
    return SecretRef(env_var)


def _allowed_roots(section: Mapping[str, Any], name: str) -> tuple[str, ...]:
    raw = section.get("allowed_roots")
    field_name = f"{name}.allowed_roots"
    if raw is None:
        raise ConfigError(
            f"{field_name}: required and must not be empty (no root allowlist means no scope)"
        )
    if not isinstance(raw, list) or not raw:
        raise ConfigError(f"{field_name}: must be a non-empty list of POSIX paths")
    roots: list[str] = []
    for item in raw:
        if not isinstance(item, str):
            raise ConfigError(f"{field_name}: every root must be a path string")
        try:
            root = normalize_posix(item)
        except PathError as exc:
            raise ConfigError(f"{field_name}: {exc}") from exc
        if root not in roots:
            roots.append(root)
    return tuple(roots)


def _path_mappings(section: Mapping[str, Any]) -> tuple[PathMapping, ...]:
    raw = section.get("mappings")
    if raw is None:
        raise ConfigError(
            "paths.mappings: required and must not be empty (unmapped paths are refused)"
        )
    if not isinstance(raw, list) or not raw:
        raise ConfigError("paths.mappings: must be a non-empty list of {sonarr, shoko} entries")
    entries: list[PathMapping] = []
    for index, item in enumerate(raw):
        field_name = f"paths.mappings[{index}]"
        if not isinstance(item, dict):
            raise ConfigError(f"{field_name}: expected a mapping with 'sonarr' and 'shoko' keys")
        entry = cast("dict[str, Any]", item)
        _known_keys(entry, {"sonarr", "shoko"}, field_name)
        entries.append(
            PathMapping(
                _posix_path(entry.get("sonarr"), f"{field_name}.sonarr"),
                _posix_path(entry.get("shoko"), f"{field_name}.shoko"),
            )
        )
    try:
        return validate_mappings(entries)
    except PathError as exc:
        raise ConfigError(f"paths.mappings: {exc}") from exc


def _mappings(section: Mapping[str, Any]) -> MappingSettings:
    _known_keys(section, _MAPPINGS_KEYS, "mappings")
    source = section.get("source")
    if not isinstance(source, str) or source.strip() != MAPPING_SOURCE:
        raise ConfigError(
            f"mappings.source: must be {MAPPING_SOURCE!r} (the only supported mapping source in v1)"
        )
    overrides_raw = section.get("overrides_file")
    overrides = (
        None if overrides_raw is None else _posix_path(overrides_raw, "mappings.overrides_file")
    )
    return MappingSettings(
        source=MAPPING_SOURCE,
        refresh_hours=_positive_number(section, "refresh_hours", "mappings", DEFAULT_REFRESH_HOURS),
        overrides_file=overrides,
        source_url=_base_url(
            section.get("source_url", DEFAULT_MAPPING_SOURCE_URL), "mappings.source_url"
        ),
    )


def _sync(section: Mapping[str, Any]) -> SyncSettings:
    _known_keys(section, _SYNC_KEYS, "sync")
    connect_seconds, read_seconds = _timeouts(section.get("timeouts"))
    return SyncSettings(
        interval_seconds=_positive_number(
            section, "interval_seconds", "sync", DEFAULT_INTERVAL_SECONDS
        ),
        full_reconcile_interval_hours=_positive_number(
            section, "full_reconcile_interval_hours", "sync", DEFAULT_FULL_RECONCILE_HOURS
        ),
        new_file_grace_period_seconds=_non_negative_number(
            section,
            "new_file_grace_period_seconds",
            "sync",
            DEFAULT_NEW_FILE_GRACE_SECONDS,
        ),
        apply_changes=_bool(section, "apply_changes", "sync", False),
        max_concurrent_links=_whole_number(
            section, "max_concurrent_links", "sync", DEFAULT_MAX_CONCURRENT_LINKS, minimum=1
        ),
        connect_timeout_seconds=connect_seconds,
        read_timeout_seconds=read_seconds,
        max_new_per_cycle=_whole_number(
            section, "max_new_per_cycle", "sync", DEFAULT_MAX_NEW_PER_CYCLE, minimum=0
        ),
        rolling_new_limit=_whole_number(
            section, "rolling_new_limit", "sync", DEFAULT_ROLLING_NEW_LIMIT, minimum=0
        ),
        rolling_window_hours=_positive_number(
            section, "rolling_window_hours", "sync", DEFAULT_ROLLING_WINDOW_HOURS
        ),
        max_pending_episode_jobs=_whole_number(
            section, "max_pending_episode_jobs", "sync", DEFAULT_MAX_PENDING_EPISODE_JOBS, minimum=0
        ),
        max_cycle_seconds=_positive_number(
            section, "max_cycle_seconds", "sync", DEFAULT_MAX_CYCLE_SECONDS
        ),
        max_recovery_rows_per_cycle=_whole_number(
            section,
            "max_recovery_rows_per_cycle",
            "sync",
            DEFAULT_MAX_RECOVERY_ROWS_PER_CYCLE,
            minimum=1,
        ),
        verification_timeout_seconds=_positive_number(
            section, "verification_timeout_seconds", "sync", DEFAULT_VERIFICATION_TIMEOUT_SECONDS
        ),
        apply_lock_lease_seconds=_positive_number(
            section, "apply_lock_lease_seconds", "sync", DEFAULT_APPLY_LOCK_LEASE_SECONDS
        ),
        jitter_ratio=_ratio(
            section, "jitter_ratio", "sync", DEFAULT_JITTER_RATIO, upper=MAX_JITTER_RATIO
        ),
        mapping_churn_limit=_whole_number(
            section, "mapping_churn_limit", "sync", DEFAULT_MAPPING_CHURN_LIMIT, minimum=1
        ),
        queue_blocked_tolerance=_whole_number(
            section,
            "queue_blocked_tolerance",
            "sync",
            DEFAULT_QUEUE_BLOCKED_TOLERANCE,
            minimum=0,
        ),
    )


def _timeouts(raw: Any) -> tuple[float, float]:
    if raw is None:
        return DEFAULT_CONNECT_TIMEOUT_SECONDS, DEFAULT_READ_TIMEOUT_SECONDS
    if not isinstance(raw, dict):
        raise ConfigError("sync.timeouts: expected a mapping with connect_seconds and read_seconds")
    section = cast("dict[str, Any]", raw)
    _known_keys(section, _TIMEOUT_KEYS, "sync.timeouts")
    return (
        _positive_number(
            section, "connect_seconds", "sync.timeouts", DEFAULT_CONNECT_TIMEOUT_SECONDS
        ),
        _positive_number(section, "read_seconds", "sync.timeouts", DEFAULT_READ_TIMEOUT_SECONDS),
    )


def _database(section: Mapping[str, Any]) -> DatabaseSettings:
    _known_keys(section, _DATABASE_KEYS, "database")
    return DatabaseSettings(path=_posix_path(section.get("path"), "database.path"))


def _safety(raw: Any) -> SafetySettings:
    if raw is None:
        return DEFAULT_SAFETY_SETTINGS
    if not isinstance(raw, dict):
        raise ConfigError("safety: expected a mapping")
    section = cast("Mapping[str, Any]", raw)
    _known_keys(section, _SAFETY_KEYS, "safety")
    attestation_raw = section.get("attestation_file")
    attestation_file = (
        None if attestation_raw is None else _posix_path(attestation_raw, "safety.attestation_file")
    )
    return SafetySettings(
        attestation_file=attestation_file,
        attestation_validity_hours=_positive_number(
            section,
            "attestation_validity_hours",
            "safety",
            DEFAULT_ATTESTATION_VALIDITY_HOURS,
        ),
        attestation_warn_seconds=_positive_number(
            section, "attestation_warn_seconds", "safety", DEFAULT_ATTESTATION_WARN_SECONDS
        ),
        attested_shoko_version=_optional_text(section, "attested_shoko_version", "safety"),
    )


def _optional_text(section: Mapping[str, Any], key: str, field_name: str) -> str | None:
    raw = section.get(key)
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw.strip():
        raise ConfigError(f"{field_name}: {key} must be null or a non-empty version string")
    return raw.strip()


def _campaign(raw: Any) -> CampaignSettings:
    if raw is None:
        return CampaignSettings(
            max_campaign_files=DEFAULT_MAX_CAMPAIGN_FILES,
            max_batch_size=DEFAULT_MAX_BATCH_SIZE,
            daemon_auto_apply=DEFAULT_CAMPAIGN_DAEMON_AUTO_APPLY,
        )
    if not isinstance(raw, dict):
        raise ConfigError("campaign: expected a mapping")
    section = cast("Mapping[str, Any]", raw)
    _known_keys(section, _CAMPAIGN_KEYS, "campaign")
    return CampaignSettings(
        max_campaign_files=_whole_number(
            section, "max_campaign_files", "campaign", DEFAULT_MAX_CAMPAIGN_FILES, minimum=1
        ),
        max_batch_size=_whole_number(
            section, "max_batch_size", "campaign", DEFAULT_MAX_BATCH_SIZE, minimum=1
        ),
        daemon_auto_apply=_bool(
            section, "daemon_auto_apply", "campaign", DEFAULT_CAMPAIGN_DAEMON_AUTO_APPLY
        ),
    )


def normalize_service_url(raw: str) -> str:
    """Canonical URL identity for attestation comparisons (mirrors :func:`_base_url` rules).

    Lowercases scheme and host, strips the path's trailing slash, and drops
    query/fragment. Raises :class:`ConfigError` for unusable URLs — an attestation
    endpoint is compared on this identity, never on string equality.
    """
    if not isinstance(raw, str) or not raw.strip():
        raise ConfigError("URL: must be a non-empty URL string")
    parts = urlsplit(raw.strip())
    scheme = parts.scheme.lower()
    if scheme not in _ALLOWED_SCHEMES:
        raise ConfigError(f"URL: scheme must be http or https (got {parts.scheme!r})")
    if not parts.hostname:
        raise ConfigError("URL: must include a host")
    if parts.query or parts.fragment:
        raise ConfigError("URL: must not include a query or fragment")
    host = parts.hostname.lower()
    if parts.port is not None:
        host = f"{host}:{parts.port}"
    return urlunsplit((scheme, host, parts.path.rstrip("/"), "", ""))


def _posix_path(raw: Any, field_name: str) -> str:
    if not isinstance(raw, str):
        raise ConfigError(f"{field_name}: must be a POSIX path string")
    try:
        return normalize_posix(raw)
    except PathError as exc:
        raise ConfigError(f"{field_name}: {exc}") from exc


def _number(section: Mapping[str, Any], key: str, field_name: str, default: float) -> float:
    raw = section.get(key, default)
    if isinstance(raw, bool) or not isinstance(raw, int | float):
        raise ConfigError(f"{field_name}: {key} must be a number")
    try:
        value = float(cast("int | float", raw))
    except OverflowError as exc:
        raise ConfigError(f"{field_name}: {key} is out of range") from exc
    if not math.isfinite(value):
        raise ConfigError(f"{field_name}: {key} must be a finite number")
    return value


def _positive_number(
    section: Mapping[str, Any], key: str, field_name: str, default: float
) -> float:
    value = _number(section, key, field_name, default)
    if value <= 0:
        raise ConfigError(f"{field_name}: {key} must be greater than 0")
    return value


def _non_negative_number(
    section: Mapping[str, Any], key: str, field_name: str, default: float
) -> float:
    value = _number(section, key, field_name, default)
    if value < 0:
        raise ConfigError(f"{field_name}: {key} must not be negative")
    return value


def _ratio(
    section: Mapping[str, Any], key: str, field_name: str, default: float, *, upper: float
) -> float:
    value = _number(section, key, field_name, default)
    if value < 0 or value > upper:
        raise ConfigError(f"{field_name}: {key} must be between 0 and {upper:g}")
    return value


def _whole_number(
    section: Mapping[str, Any], key: str, field_name: str, default: int, *, minimum: int
) -> int:
    raw = section.get(key, default)
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise ConfigError(f"{field_name}: {key} must be a whole number")
    value = cast(int, raw)
    if value < minimum:
        raise ConfigError(f"{field_name}: {key} must be at least {minimum}")
    return value


def _bool(section: Mapping[str, Any], key: str, field_name: str, default: bool) -> bool:
    raw = section.get(key, default)
    if not isinstance(raw, bool):
        raise ConfigError(
            f"{field_name}: {key} must be the YAML value true or false "
            "(writes require explicit true)"
        )
    return raw


def _require_secret_present(settings: Settings) -> None:
    """Fail at load time when a configured API key variable is not set.

    Only the variable name is reported; the value is never read into settings.
    """
    for env_var in settings.secret_env_vars:
        if not os.environ.get(env_var):
            raise ConfigError(
                f"missing API key environment variable: {env_var} (set it before running)"
            )
