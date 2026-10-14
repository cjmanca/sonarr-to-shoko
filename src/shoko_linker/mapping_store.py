"""Anime-Lists runtime download, scheduled refresh, validated atomic activation, last-known-good.

The stateful half of the M4 mapping pipeline (:mod:`shoko_linker.mappings` is the pure
half). This module owns the on-disk cache under ``<database-dir>/mappings``:

* ``active.xml`` — the last-known-good mapping bytes,
* ``active.meta.json`` — digests/revision describing exactly those bytes,
* ``state.json`` — schedule, opaque ETag validator, digests, revision, failure count.

All JSON and cache writes are write-temp + ``os.replace``: a crash never leaves a
half-written file, and a failed refresh never moves ``active.xml``, so the last-known-good
mapping stays live.

Policy (``docs/compatibility.md`` sections 1 and 3):

* Canonical source URL is fetched **at runtime, never bundled** (no license grants
  redistribution); the source URL is recorded in state/meta as provenance attribution.
* Conditional GET uses **ETag only** (the source sends no ``Last-Modified``). The source
  answers ``Vary: Accept-Encoding``, so the validator is stored **with** the exact
  ``Accept-Encoding`` we send — always ``identity`` — and is only reused for that
  encoding. The ETag is opaque; content identity is our SHA-256.
* Every 200 body is parsed and indexed **before** activation; size cap,
  :class:`~shoko_linker.mappings.MappingError` and :class:`~shoko_linker.config.ConfigError`
  failures leave the last-known-good untouched.
* Successes reschedule at ``refresh_hours``; failures advance a bounded exponential
  backoff ``min(24h, 2^(consecutive_failures-1) h)``.

Effective-generation semantics: the outcome is ``ACTIVATED`` only when the **effective
target/structure digests** change (revision increments then); valid content whose digests
are unchanged — 304, byte-identical 200, or formatting-only 200 — reports ``UNCHANGED``
with validators refreshed. Operator overrides are part of the index, hence part of both
digests: the same upstream bytes under different overrides are a new effective generation
(``ACTIVATED``).

No Sonarr/Shoko I/O happens here; the only endpoint is the injected
:class:`~shoko_linker.http.ApiEndpoint`, which must be bound to the source URL's origin
and send ``Accept-Encoding: identity`` (see :func:`make_source_endpoint`).
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Final, cast
from urllib.parse import urlsplit, urlunsplit

import httpx

from .config import DEFAULT_MAPPING_SOURCE_URL, DEFAULT_REFRESH_HOURS, Settings
from .errors import ConfigError, HttpPolicyError
from .http import (
    ApiEndpoint,
    HeadersFactory,
    make_client,
    make_timeout,
    resolve_and_send,
    same_origin,
)
from .mappings import (
    MappingError,
    MappingIndex,
    Overrides,
    build_mapping_index,
    content_digest,
    load_overrides,
    parse_anime_lists,
    structure_digest,
    targets_digest,
)
from .state import ApplyProcessLock

ACCEPT_ENCODING: Final[str] = "identity"
DEFAULT_MAX_BYTES: Final[int] = 10_000_000
MAX_READ_CHUNK_BYTES: Final[int] = 64 * 1024
MAX_BACKOFF_HOURS: Final[float] = 24.0
GENERATION_LOCK_POLL_SECONDS: Final[float] = 0.01
GENERATION_LOCK_REFRESH_WAIT_SECONDS: Final[float] = 1.0
_OK: Final[int] = 200
_NOT_MODIFIED: Final[int] = 304
_HEX64: Final[int] = 64
_MIN_SOURCE_PATH_CHARS: Final[int] = 2

_STATE_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "last_attempted_iso",
        "last_success_iso",
        "next_due_iso",
        "etag",
        "accept_encoding",
        "content_sha256",
        "targets_digest",
        "structure_digest",
        "revision",
        "consecutive_failures",
        "source_url",
        "source_change_pending",
        "last_outcome",
    }
)
_META_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "content_sha256",
        "targets_digest",
        "structure_digest",
        "revision",
        "activated_iso",
        "source_url",
        "entries_indexed",
        "mapping_rules",
        "explicit_pairs",
    }
)


class RefreshOutcome(Enum):
    """Terminal outcome of one refresh attempt."""

    NOT_DUE = "not_due"
    UNCHANGED = "unchanged"
    ACTIVATED = "activated"
    FAILED_TRANSPORT = "failed_transport"
    FAILED_INVALID = "failed_invalid"
    FAILED_SIZE = "failed_size"
    FAILED_LOCKED = "failed_locked"
    NO_ACTIVE_BUT_FAILED = "no_active_but_failed"


@dataclass(frozen=True)
class SourceLocation:
    """Mapping source URL split into origin + path (validators mirror ``config._base_url``)."""

    origin: str
    path: str


def parse_source_url(url: str) -> SourceLocation:
    """Validate the mapping source URL; reject non-http(s), credentials, query, fragment."""
    if not isinstance(url, str) or not url.strip():
        raise ConfigError("mappings source URL: must be a non-empty URL string")
    parts = urlsplit(url.strip())
    scheme = parts.scheme.lower()
    if scheme not in {"http", "https"}:
        raise ConfigError(
            f"mappings source URL: scheme must be http or https (got {parts.scheme!r})"
        )
    if not parts.hostname:
        raise ConfigError("mappings source URL: must include a host")
    if parts.username or parts.password:
        raise ConfigError("mappings source URL: must not embed credentials")
    if parts.query or parts.fragment:
        raise ConfigError("mappings source URL: must not include a query or fragment")
    if not parts.path.startswith("/") or len(parts.path) < _MIN_SOURCE_PATH_CHARS:
        raise ConfigError("mappings source URL: must name a file path")
    origin = urlunsplit((scheme, parts.netloc.lower(), "", "", ""))
    return SourceLocation(origin=origin, path=parts.path)


def _normalized_source_url(url: str) -> str:
    location = parse_source_url(url)
    return location.origin + location.path


def _source_url_matches(store: MappingStore, source_url: str) -> bool:
    try:
        return _normalized_source_url(source_url) == store.source_url
    except (ConfigError, ValueError):
        return False


@dataclass
class _State:
    """Validated view of ``state.json``; a corrupt file loads as the empty state."""

    last_attempted_iso: str | None = None
    last_success_iso: str | None = None
    next_due_iso: str | None = None
    etag: str | None = None
    accept_encoding: str = ACCEPT_ENCODING
    content_sha256: str | None = None
    targets_digest: str | None = None
    structure_digest: str | None = None
    revision: int = 0
    consecutive_failures: int = 0
    source_url: str = DEFAULT_MAPPING_SOURCE_URL
    source_change_pending: bool = False
    last_outcome: str | None = None


@dataclass(frozen=True)
class _Meta:
    """Descriptor of the exact bytes in ``active.xml``."""

    content_sha256: str
    targets_digest: str
    structure_digest: str
    revision: int
    activated_iso: str
    source_url: str
    entries_indexed: int
    mapping_rules: int
    explicit_pairs: int


@dataclass(frozen=True)
class ActiveMappings:
    """A verified, usable active mapping generation loaded from the last-known-good cache."""

    index: MappingIndex
    content_sha256: str
    targets_digest: str
    structure_digest: str
    revision: int
    activated_iso: str
    source_url: str
    overrides_applied: bool


@dataclass(frozen=True)
class MappingStatus:
    """Read-only disk view used for scheduling and reporting (no parsing, no I/O beyond reads)."""

    source_url: str
    active_present: bool
    revision: int
    content_sha256: str | None
    targets_digest: str | None
    structure_digest: str | None
    activated_iso: str | None
    active_age_seconds: float | None
    last_attempted_iso: str | None
    last_success_iso: str | None
    next_due_iso: str | None
    last_outcome: RefreshOutcome | None
    consecutive_failures: int
    entries_indexed: int | None
    mapping_rules: int | None
    explicit_pairs: int | None


def _atomic_write(path: Path, payload: bytes | bytearray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.parent / f"{path.name}.new"
    try:
        # ``Path.write_bytes`` accepts the buffer protocol at runtime; this cast avoids
        # copying a capped bytearray mapping body into a second full-size bytes object.
        temp.write_bytes(cast(bytes, payload))
        temp.replace(path)
    finally:
        if temp.exists():
            temp.unlink()


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8")


def _as_utc(moment: datetime) -> datetime:
    """Normalize to aware UTC; naive datetimes are declared UTC (determinism contract)."""
    if moment.tzinfo is None:
        return moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC)


def _iso(moment: datetime) -> str:
    return _as_utc(moment).isoformat()


def _parse_iso(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if not isinstance(raw, str):
        raise ValueError("timestamp must be a string")
    parsed = datetime.fromisoformat(raw)
    return _as_utc(parsed)


def _is_hex64(raw: Any) -> bool:
    return (
        isinstance(raw, str)
        and len(raw) == _HEX64
        and all(char in "0123456789abcdef" for char in raw)
    )


def _state_from_document(document: dict[str, Any]) -> _State:
    if set(document) - _STATE_FIELDS:
        raise ValueError("unknown state fields")
    _parse_iso(document.get("next_due_iso"))
    _parse_iso(document.get("last_attempted_iso"))
    _parse_iso(document.get("last_success_iso"))
    revision = document.get("revision", 0)
    failures = document.get("consecutive_failures", 0)
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
        raise ValueError("revision must be a non-negative whole number")
    if isinstance(failures, bool) or not isinstance(failures, int) or failures < 0:
        raise ValueError("consecutive_failures must be a non-negative whole number")
    etag = document.get("etag")
    if etag is not None and not isinstance(etag, str):
        raise ValueError("etag must be a string")
    accept_encoding = document.get("accept_encoding", ACCEPT_ENCODING)
    if not isinstance(accept_encoding, str) or not accept_encoding:
        raise ValueError("accept_encoding must be a string")
    source_url = _normalized_source_url(document.get("source_url", DEFAULT_MAPPING_SOURCE_URL))
    last_outcome = document.get("last_outcome")
    if last_outcome is not None and not isinstance(last_outcome, str):
        raise ValueError("last_outcome must be a string")
    source_change_pending = document.get("source_change_pending", False)
    if not isinstance(source_change_pending, bool):
        raise ValueError("source_change_pending must be a boolean")
    for name in ("content_sha256", "targets_digest", "structure_digest"):
        value = document.get(name)
        if value is not None and not _is_hex64(value):
            raise ValueError(f"{name} must be a lowercase SHA-256 hex digest")
    return _State(
        last_attempted_iso=document.get("last_attempted_iso"),
        last_success_iso=document.get("last_success_iso"),
        next_due_iso=document.get("next_due_iso"),
        etag=etag,
        accept_encoding=accept_encoding,
        content_sha256=document.get("content_sha256"),
        targets_digest=document.get("targets_digest"),
        structure_digest=document.get("structure_digest"),
        revision=revision,
        consecutive_failures=failures,
        source_url=source_url,
        source_change_pending=source_change_pending,
        last_outcome=last_outcome,
    )


def _meta_from_document(document: dict[str, Any]) -> _Meta:
    if set(document) - _META_FIELDS:
        raise ValueError("unknown meta fields")
    activated = document.get("activated_iso")
    if not isinstance(activated, str) or _parse_iso(activated) is None:
        raise ValueError("activated_iso must be an ISO-8601 timestamp")
    revision = document.get("revision")
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
        raise ValueError("revision must be a whole number of at least 1")
    for name in ("content_sha256", "targets_digest", "structure_digest"):
        value = document.get(name)
        if not _is_hex64(value):
            raise ValueError(f"{name} must be a lowercase SHA-256 hex digest")
    counts: dict[str, int] = {}
    for name in ("entries_indexed", "mapping_rules", "explicit_pairs"):
        value = document.get(name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{name} must be a non-negative whole number")
        counts[name] = value
    raw_source_url = document.get("source_url")
    if not isinstance(raw_source_url, str):
        raise ValueError("source_url must be a string")
    source_url = _normalized_source_url(raw_source_url)
    return _Meta(
        content_sha256=cast(str, document["content_sha256"]),
        targets_digest=cast(str, document["targets_digest"]),
        structure_digest=cast(str, document["structure_digest"]),
        revision=revision,
        activated_iso=activated,
        source_url=source_url,
        entries_indexed=counts["entries_indexed"],
        mapping_rules=counts["mapping_rules"],
        explicit_pairs=counts["explicit_pairs"],
    )


class MappingStore:
    """Last-known-good Anime-Lists cache bound to one directory."""

    def __init__(
        self,
        directory: str | Path,
        *,
        source_url: str = DEFAULT_MAPPING_SOURCE_URL,
    ) -> None:
        self._source = parse_source_url(source_url)
        self.directory = Path(directory)

    @classmethod
    def from_settings(cls, settings: Settings) -> MappingStore:
        """Cache directory derived from settings: ``<database-dir>/mappings``."""
        directory = Path(settings.database.path).parent / "mappings"
        return cls(directory, source_url=settings.mappings.source_url)

    @property
    def source_url(self) -> str:
        return self._source.origin + self._source.path

    @property
    def origin(self) -> str:
        return self._source.origin

    @property
    def source_path(self) -> str:
        return self._source.path

    @property
    def active_path(self) -> Path:
        return self.directory / "active.xml"

    @property
    def meta_path(self) -> Path:
        return self.directory / "active.meta.json"

    @property
    def state_path(self) -> Path:
        return self.directory / "state.json"

    @property
    def generation_lock_path(self) -> Path:
        return self.directory / "generation.lock"

    @contextmanager
    def generation_lock(self, *, wait_seconds: float = 0.0) -> Iterator[bool]:
        """Serialize mapping activation with campaign decisions that must use one generation.

        Campaign quarantine checks use a non-blocking take and defer on contention. Refresh may
        wait briefly for an in-flight campaign decision; if it still cannot acquire the lock, it
        leaves the active generation unchanged and schedules the normal bounded retry.
        """
        if wait_seconds < 0:
            raise ValueError("wait_seconds must be non-negative")
        lock = ApplyProcessLock(self.generation_lock_path)
        deadline = time.monotonic() + wait_seconds
        while not lock.acquire():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                yield False
                return
            time.sleep(min(GENERATION_LOCK_POLL_SECONDS, remaining))
        try:
            yield True
        finally:
            lock.release()

    def request_headers(self) -> dict[str, str]:
        """Headers for source requests: ``Accept-Encoding: identity`` plus the stored ETag.

        The ETag is sent only with the encoding it was recorded under (``Vary:
        Accept-Encoding``), the configured source, and a verified active generation. A
        validator without its matching bytes or provenance cannot recover a cache on 304.
        """
        headers = {"Accept-Encoding": ACCEPT_ENCODING}
        state = self._load_state()
        if (
            _source_url_matches(self, state.source_url)
            and self._state_matches_active(state)
            and state.etag is not None
            and state.accept_encoding == ACCEPT_ENCODING
        ):
            headers["If-None-Match"] = state.etag
        return headers

    def headers_factory(self) -> HeadersFactory:
        """Factory to give :func:`make_client` so conditional headers track disk state."""
        return self.request_headers

    def _load_state(self) -> _State:
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return _State()
        if not isinstance(payload, dict):
            return _State()
        try:
            return _state_from_document(cast("dict[str, Any]", payload))
        except (ValueError, ConfigError):
            return _State()

    def _save_state(self, state: _State) -> None:
        _atomic_write(self.state_path, _json_bytes(asdict(state)))

    def _load_meta(self) -> _Meta | None:
        try:
            payload = json.loads(self.meta_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        if not isinstance(payload, dict):
            return None
        try:
            return _meta_from_document(cast("dict[str, Any]", payload))
        except (ValueError, ConfigError):
            return None

    def _read_active_bytes(self) -> bytes | None:
        try:
            return self.active_path.read_bytes()
        except OSError:
            return None

    def _active_verified(self) -> bool:
        meta = self._load_meta()
        state = self._load_state()
        body = self._read_active_bytes()
        if (
            meta is None
            or body is None
            or not _source_url_matches(self, meta.source_url)
            or not _source_url_matches(self, state.source_url)
            or state.source_change_pending
            or content_digest(body) != meta.content_sha256
        ):
            return False
        try:
            parse_anime_lists(body)
        except (MappingError, ConfigError):
            return False
        return True

    def _state_matches_active(self, state: _State) -> bool:
        meta = self._load_meta()
        return (
            meta is not None
            and _source_url_matches(self, state.source_url)
            and _source_url_matches(self, meta.source_url)
            and self._active_verified()
            and state.content_sha256 == meta.content_sha256
            and state.targets_digest == meta.targets_digest
            and state.structure_digest == meta.structure_digest
            and state.revision == meta.revision
        )


def make_source_endpoint(
    store: MappingStore,
    *,
    connect_seconds: float = 10.0,
    read_seconds: float = 30.0,
) -> ApiEndpoint:
    """Endpoint for the mapping source: origin-only base, identity encoding, no auth."""
    return make_client(
        store.origin,
        store.headers_factory(),
        make_timeout(connect_seconds, read_seconds),
    )


def _require_source_endpoint(store: MappingStore, endpoint: ApiEndpoint) -> None:
    base = endpoint.base_url
    if not same_origin(base, httpx.URL(store.origin)):
        raise HttpPolicyError(
            f"source endpoint base {str(base)!r} origin does not match mapping source "
            f"origin {store.origin!r} (validators and provenance are origin-bound)"
        )
    if base.path.strip("/"):
        raise HttpPolicyError(
            f"source endpoint base {str(base)!r} must be the mapping source origin "
            "without a path segment (the source file path is joined at request time)"
        )
    if endpoint.headers_factory().get("Accept-Encoding") != ACCEPT_ENCODING:
        raise HttpPolicyError(
            f"source endpoint must send 'Accept-Encoding: {ACCEPT_ENCODING}' "
            "(the stored ETag validator is bound to that exact encoding)"
        )


def _is_due(state: _State, now: datetime) -> bool:
    try:
        next_due = _parse_iso(state.next_due_iso)
    except ValueError:
        return True
    if next_due is None:
        return True
    return now >= next_due


def _state_for_source(store: MappingStore, state: _State) -> tuple[_State, bool]:
    """Return source-specific scheduler state, resetting all inherited validators/timing."""
    if _source_url_matches(store, state.source_url):
        return state, False
    return _State(source_url=store.source_url, source_change_pending=True), True


def _recovery_backoff_active(state: _State, now: datetime) -> bool:
    """Honor the persisted failure retry time when no usable active generation exists."""
    if state.consecutive_failures <= 0:
        return False
    try:
        next_due = _parse_iso(state.next_due_iso)
    except ValueError:
        return False
    return next_due is not None and now < next_due


def _backoff_hours(consecutive_failures: int) -> float:
    return min(MAX_BACKOFF_HOURS, 2.0 ** (consecutive_failures - 1))


def _record_failure(
    store: MappingStore,
    state: _State,
    now: datetime,
    outcome: RefreshOutcome,
) -> RefreshOutcome:
    state.consecutive_failures += 1
    state.next_due_iso = _iso(now + timedelta(hours=_backoff_hours(state.consecutive_failures)))
    state.last_outcome = outcome.value
    store._save_state(state)
    if store._active_verified():
        return outcome
    return RefreshOutcome.NO_ACTIVE_BUT_FAILED


def _record_success(
    store: MappingStore,
    state: _State,
    now: datetime,
    response: httpx.Response,
    refresh_hours: float,
    content_sha256: str,
    targets: str,
    structure: str,
    revision: int,
    outcome: RefreshOutcome,
) -> RefreshOutcome:
    etag = response.headers.get("etag")
    state.etag = etag
    state.accept_encoding = ACCEPT_ENCODING
    state.source_url = store.source_url
    state.source_change_pending = False
    state.content_sha256 = content_sha256
    state.targets_digest = targets
    state.structure_digest = structure
    state.revision = revision
    state.last_success_iso = _iso(now)
    state.next_due_iso = _iso(now + timedelta(hours=refresh_hours))
    state.consecutive_failures = 0
    state.last_outcome = outcome.value
    store._save_state(state)
    return outcome


def _activate(
    store: MappingStore,
    body: bytes | bytearray,
    state_view: tuple[str, str, str, int],
    now: datetime,
    index: MappingIndex,
) -> None:
    content_sha256, targets, structure, revision = state_view
    meta = _Meta(
        content_sha256=content_sha256,
        targets_digest=targets,
        structure_digest=structure,
        revision=revision,
        activated_iso=_iso(now),
        source_url=store.source_url,
        entries_indexed=index.entries_indexed,
        mapping_rules=index.mapping_rules,
        explicit_pairs=index.explicit_pairs,
    )
    _atomic_write(store.active_path, body)
    _atomic_write(store.meta_path, _json_bytes(asdict(meta)))


def _accept_content(
    store: MappingStore,
    state: _State,
    now: datetime,
    response: httpx.Response,
    refresh_hours: float,
    overrides_text: str | None,
    body: bytes | bytearray,
) -> RefreshOutcome:
    overrides: Overrides | None = None
    if overrides_text is not None:
        try:
            overrides = load_overrides(overrides_text)
        except ConfigError:
            return _record_failure(store, state, now, RefreshOutcome.FAILED_INVALID)
    try:
        parsed = parse_anime_lists(body)
        index = build_mapping_index(parsed, overrides)
    except (MappingError, ConfigError):
        return _record_failure(store, state, now, RefreshOutcome.FAILED_INVALID)

    content_sha256 = content_digest(body)
    targets = targets_digest(index)
    structure = structure_digest(index)
    with store.generation_lock(wait_seconds=GENERATION_LOCK_REFRESH_WAIT_SECONDS) as acquired:
        if not acquired:
            return _record_failure(store, state, now, RefreshOutcome.FAILED_LOCKED)
        meta = store._load_meta()
        if (
            meta is not None
            and _source_url_matches(store, meta.source_url)
            and store._active_verified()
            and targets == meta.targets_digest
            and structure == meta.structure_digest
        ):
            return _record_success(
                store,
                state,
                now,
                response,
                refresh_hours,
                meta.content_sha256,
                targets,
                structure,
                meta.revision,
                RefreshOutcome.UNCHANGED,
            )
        revision = meta.revision + 1 if meta is not None else 1
        _activate(store, body, (content_sha256, targets, structure, revision), now, index)
        return _record_success(
            store,
            state,
            now,
            response,
            refresh_hours,
            content_sha256,
            targets,
            structure,
            revision,
            RefreshOutcome.ACTIVATED,
        )


def refresh(
    store: MappingStore,
    endpoint: ApiEndpoint,
    *,
    now: datetime,
    refresh_hours: float = DEFAULT_REFRESH_HOURS,
    force: bool = False,
    max_bytes: int = DEFAULT_MAX_BYTES,
    overrides_text: str | None = None,
) -> RefreshOutcome:
    """Run one conditional refresh; the last-known-good cache never moves on failure.

    Due gate: proceed when ``force`` or when no/overdue ``next_due``; otherwise
    ``NOT_DUE`` with zero requests. On a 200 the body is size-capped, parsed, indexed
    (with ``overrides_text`` applied) and digest-compared **before** atomic activation;
    :class:`~shoko_linker.mappings.MappingError` or
    :class:`~shoko_linker.config.ConfigError` from overrides/parsing are
    ``FAILED_INVALID`` with the cache untouched. ``ACTIVATED`` requires a change of the
    effective target/structure digests (overrides are part of the digests: same bytes +
    different overrides is a new effective generation); validators and timing refresh on
    every success. Raises :class:`~shoko_linker.errors.HttpPolicyError` when the
    endpoint is not bound to the source origin with ``Accept-Encoding: identity`` — a
    wiring error, not a remote outcome.
    """
    if refresh_hours <= 0:
        raise ConfigError("refresh_hours must be greater than 0")
    if max_bytes <= 0:
        raise ConfigError("max_bytes must be greater than 0")
    now_utc = _as_utc(now)
    state, source_changed = _state_for_source(store, store._load_state())
    if source_changed:
        # Persist the source switch before any request so a crash cannot keep the old
        # validator or weekly schedule attached to the new URL.
        store._save_state(state)
    cache_overrides: Overrides | None = None
    overrides_valid = True
    if overrides_text is not None:
        try:
            cache_overrides = load_overrides(overrides_text)
        except ConfigError:
            overrides_valid = False
    active_valid = overrides_valid and load_active(store, cache_overrides) is not None
    if not active_valid and state.etag is not None:
        state.etag = None
        store._save_state(state)
    if not force and not due(store, now_utc, cache_overrides if overrides_valid else None):
        return RefreshOutcome.NOT_DUE
    _require_source_endpoint(store, endpoint)
    state.last_attempted_iso = _iso(now_utc)
    try:
        # max_redirects=0: the canonical source answers 200 (docs §3);
        # any redirect (same- or cross-origin) is a contract violation and
        # foreign content must never be activated as canonical provenance.
        response = resolve_and_send(
            endpoint, "GET", store.source_path, max_redirects=0, stream=True
        )
    except (HttpPolicyError, httpx.HTTPError):
        return _record_failure(store, state, now_utc, RefreshOutcome.FAILED_TRANSPORT)
    try:
        return _dispatch(store, state, now_utc, response, refresh_hours, max_bytes, overrides_text)
    except httpx.HTTPError:
        # Stream failures occur while _dispatch reads the body. _read_capped closes the
        # response in its finally block before the failed refresh is recorded.
        return _record_failure(store, state, now_utc, RefreshOutcome.FAILED_TRANSPORT)


def _dispatch(
    store: MappingStore,
    state: _State,
    now_utc: datetime,
    response: httpx.Response,
    refresh_hours: float,
    max_bytes: int,
    overrides_text: str | None,
) -> RefreshOutcome:
    """Turn one source response into an outcome; the cache moves only on ACTIVATED."""
    if response.status_code == _NOT_MODIFIED:
        response.close()
        return _handle_not_modified(store, state, now_utc, response, refresh_hours, overrides_text)
    if response.status_code != _OK:
        response.close()
        return _record_failure(store, state, now_utc, RefreshOutcome.FAILED_TRANSPORT)
    body, overflow = _read_capped(response, max_bytes)
    if overflow:
        return _record_failure(store, state, now_utc, RefreshOutcome.FAILED_SIZE)
    return _accept_content(store, state, now_utc, response, refresh_hours, overrides_text, body)


def _read_capped(response: httpx.Response, max_bytes: int) -> tuple[bytearray, bool]:
    """Accumulate raw response bytes with a cap plus one bounded transport chunk."""
    body = bytearray()
    total = 0
    overflow = False
    chunk_size = min(max_bytes + 1, MAX_READ_CHUNK_BYTES)
    try:
        for chunk in response.iter_raw(chunk_size=chunk_size):
            total += len(chunk)
            if total > max_bytes:
                overflow = True
                break
            body.extend(chunk)
    finally:
        response.close()
    return body, overflow


def _handle_not_modified(
    store: MappingStore,
    state: _State,
    now_utc: datetime,
    response: httpx.Response,
    refresh_hours: float,
    overrides_text: str | None,
) -> RefreshOutcome:
    overrides: Overrides | None = None
    overrides_valid = True
    if overrides_text is not None:
        try:
            overrides = load_overrides(overrides_text)
        except ConfigError:
            overrides_valid = False
    if (
        not overrides_valid
        or load_active(store, overrides) is None
        or not store._state_matches_active(state)
    ):
        # The validator is alive but our cache is not (deleted/tampered):
        # drop the ETag so the next attempt performs a full fetch, and fail
        # this one — 304 yields no content to activate.
        state.etag = None
        return _record_failure(store, state, now_utc, RefreshOutcome.FAILED_TRANSPORT)
    return _record_success(
        store,
        state,
        now_utc,
        response,
        refresh_hours,
        cast("str", state.content_sha256),
        cast("str", state.targets_digest),
        cast("str", state.structure_digest),
        state.revision,
        RefreshOutcome.UNCHANGED,
    )


def due(store: MappingStore, now: datetime, overrides: Overrides | None = None) -> bool:
    """True when refresh is due or the active generation needs recovery.

    A source change always triggers a fresh request. Missing/unusable active data also
    triggers recovery immediately, except while a persisted failed-recovery backoff is open.
    """
    now_utc = _as_utc(now)
    state = store._load_state()
    if not _source_url_matches(store, state.source_url):
        return True
    if load_active(store, overrides) is None:
        return not _recovery_backoff_active(state, now_utc)
    return _is_due(state, now_utc)


def next_refresh_due(store: MappingStore, now: datetime, refresh_hours: float) -> datetime:
    """Scheduled UTC time of the next refresh: ``now`` when due, else the recorded ``next_due``."""
    if refresh_hours <= 0:
        raise ConfigError("refresh_hours must be greater than 0")
    now_utc = _as_utc(now)
    state, source_changed = _state_for_source(store, store._load_state())
    if source_changed:
        return now_utc
    if not store._active_verified():
        if _recovery_backoff_active(state, now_utc):
            return _parse_iso(state.next_due_iso) or now_utc
        return now_utc
    try:
        scheduled = _parse_iso(state.next_due_iso)
    except ValueError:
        scheduled = None
    if scheduled is None or scheduled <= now_utc:
        return now_utc
    return scheduled


def status(store: MappingStore, now: datetime) -> MappingStatus:
    """Read-only disk view of the cache and schedule (no network, no index parse)."""
    now_utc = _as_utc(now)
    state, source_changed = _state_for_source(store, store._load_state())
    meta = store._load_meta()
    active_present = not source_changed and store._state_matches_active(state)
    if source_changed or not store._active_verified():
        meta = None
    if not active_present and not _recovery_backoff_active(state, now_utc):
        state.next_due_iso = _iso(now_utc)
    age_seconds: float | None = None
    if meta is not None:
        activated = _parse_iso(meta.activated_iso)
        if activated is not None:
            age_seconds = (now_utc - activated).total_seconds()
    last_outcome: RefreshOutcome | None = None
    if state.last_outcome is not None:
        try:
            last_outcome = RefreshOutcome(state.last_outcome)
        except ValueError:
            last_outcome = None
    return MappingStatus(
        source_url=store.source_url,
        active_present=active_present,
        revision=meta.revision if meta is not None else 0,
        content_sha256=meta.content_sha256 if meta is not None else None,
        targets_digest=meta.targets_digest if meta is not None else None,
        structure_digest=meta.structure_digest if meta is not None else None,
        activated_iso=meta.activated_iso if meta is not None else None,
        active_age_seconds=age_seconds,
        last_attempted_iso=state.last_attempted_iso,
        last_success_iso=state.last_success_iso,
        next_due_iso=state.next_due_iso,
        last_outcome=last_outcome,
        consecutive_failures=state.consecutive_failures,
        entries_indexed=meta.entries_indexed if meta is not None else None,
        mapping_rules=meta.mapping_rules if meta is not None else None,
        explicit_pairs=meta.explicit_pairs if meta is not None else None,
    )


def load_active(
    store: MappingStore,
    overrides: Overrides | None = None,
) -> ActiveMappings | None:
    """Startup path: load and fully verify the active cache; never partially trust.

    Verifies ``active.xml`` SHA-256 against the meta, parses and rebuilds the index, and
    compares the recomputed target/structure digests against the meta. Any mismatch,
    corrupt meta, unreadable file, unparseable document, or overrides that do not match
    the activated generation (digests include overrides — pass the same overrides that
    produced the active index) returns ``None``: the caller must refresh before linking.
    ``None`` also means no cache at all.
    """
    meta = store._load_meta()
    body = None if meta is None else store._read_active_bytes()
    if meta is None or body is None or not _source_url_matches(store, meta.source_url):
        return None
    state = store._load_state()
    if not _source_url_matches(store, state.source_url) or state.source_change_pending:
        return None
    if content_digest(body) != meta.content_sha256:
        return None
    try:
        parsed = parse_anime_lists(body)
        index = build_mapping_index(parsed, overrides)
    except (MappingError, ConfigError):
        return None
    if (
        targets_digest(index) != meta.targets_digest
        or structure_digest(index) != meta.structure_digest
    ):
        return None
    return ActiveMappings(
        index=index,
        content_sha256=meta.content_sha256,
        targets_digest=meta.targets_digest,
        structure_digest=meta.structure_digest,
        revision=meta.revision,
        activated_iso=meta.activated_iso,
        source_url=meta.source_url,
        overrides_applied=overrides is not None,
    )
