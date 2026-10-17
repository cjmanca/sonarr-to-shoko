"""Anime-Lists runtime download, scheduled refresh, validated atomic activation, last-known-good.

The stateful half of the M4 mapping pipeline (:mod:`shoko_linker.mappings` is the pure
half). This module owns the on-disk cache under ``<database-dir>/mappings``:

* ``active.generation.json`` — an atomic pointer and successful scheduler snapshot,
* ``generations/<id>/active.xml`` and ``active.meta.json`` — immutable generation data,
* ``state.json`` — schedule, opaque ETag validator, digests, revision, failure count,
* ``source-transition.json`` — durable source-epoch fence across state loss or source switches.

Generation files are staged before the pointer is atomically replaced. A crash before pointer
publication leaves the previous generation selected; a crash afterwards selects a complete,
verified generation whose scheduler snapshot can recover ``state.json``. Legacy flat cache
files remain readable until the first successful generation activation.

Policy (``docs/compatibility.md`` sections 1 and 3):

* Canonical source URL is fetched **at runtime, never bundled** (no license grants
  redistribution); the source URL is recorded in state/meta as provenance attribution.
* Conditional GET uses **ETag only** (the source sends no ``Last-Modified``). The source
  answers ``Vary: Accept-Encoding``, so the validator is stored **with** the exact
  ``Accept-Encoding`` we send — always ``identity`` — and is only reused for that
  encoding. The ETag is opaque; content identity is our SHA-256.
* Every 200 body is parsed and indexed **before** activation; malformed entries fail the update
  and the last-known-good remains selected. The documented absolute-season+episodeoffset case
  is retained only as a TVDB ownership fence, never as a usable target. Size cap,
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

import hashlib
import json
import os
import shutil
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass, replace
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
    legacy_targets_digest,
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
_HEX32: Final[int] = 32
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
        "source_epoch",
        "refresh_attempt_id",
        "source_change_pending",
        "last_outcome",
        "invalid_source_hold",
        "invalid_source_observation_id",
    }
)
_REQUIRED_STATE_FIELDS: Final[frozenset[str]] = frozenset(
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
    }
)
_REQUIRED_SNAPSHOT_STATE_FIELDS: Final[frozenset[str]] = _REQUIRED_STATE_FIELDS | frozenset(
    {"source_epoch", "refresh_attempt_id", "source_change_pending", "last_outcome"}
)
_META_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "content_sha256",
        "targets_digest",
        "structure_digest",
        "revision",
        "activated_iso",
        "source_url",
        "source_epoch",
        "entries_indexed",
        "mapping_rules",
        "explicit_pairs",
    }
)
_TRANSITION_FIELDS: Final[frozenset[str]] = frozenset(
    {"source_url", "source_epoch", "scheduler_state"}
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
    FAILED_STALE = "failed_stale"
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
    source_epoch: str | None = None
    refresh_attempt_id: str | None = None
    source_change_pending: bool = False
    last_outcome: str | None = None
    invalid_source_hold: bool = False
    invalid_source_observation_id: str | None = None


@dataclass(frozen=True)
class _SourceTransition:
    source_url: str
    source_epoch: str
    scheduler_state: _State


@dataclass(frozen=True)
class _InvalidSourceObservation:
    observation_id: str
    observed_at: datetime


@dataclass(frozen=True)
class _Meta:
    """Descriptor of the exact bytes in ``active.xml``."""

    content_sha256: str
    targets_digest: str
    structure_digest: str
    revision: int
    activated_iso: str
    source_url: str
    source_epoch: str | None
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
    source_epoch: str | None
    overrides_applied: bool


@dataclass(frozen=True)
class _VerifiedGeneration:
    files: tuple[Path, Path, str | None]
    meta: _Meta
    index: MappingIndex
    pointer_state: _State | None
    source_fence_valid: bool


@dataclass(frozen=True)
class MappingStatus:
    """Snapshot; status retrieval may reconcile state and prune orphan generations."""

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
    invalid_source_hold: bool
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


def _atomic_write_durable(path: Path, payload: bytes) -> None:
    """Durably publish a small safety marker using a unique same-directory temporary file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.parent / f"{path.name}.{uuid.uuid4().hex}.new"
    try:
        with temp.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        temp.replace(path)
        if os.name != "nt":
            flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            with suppress(OSError):
                directory_fd = os.open(path.parent, flags)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
    finally:
        with suppress(OSError):
            temp.unlink(missing_ok=True)


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


def _is_hex32(raw: Any) -> bool:
    return (
        isinstance(raw, str)
        and len(raw) == _HEX32
        and all(char in "0123456789abcdef" for char in raw)
    )


def _generation_id(meta_bytes: bytes) -> str:
    return hashlib.sha256(meta_bytes).hexdigest()


def _invalid_source_hold_from_document(document: dict[str, Any], last_outcome: str | None) -> bool:
    if "invalid_source_hold" not in document:
        return last_outcome == RefreshOutcome.FAILED_INVALID.value
    hold = document["invalid_source_hold"]
    if not isinstance(hold, bool):
        raise ValueError("invalid_source_hold must be a boolean")
    return hold


def _invalid_source_observation_id_from_document(document: dict[str, Any]) -> str | None:
    observation_id = document.get("invalid_source_observation_id")
    if observation_id is not None and not _is_hex32(observation_id):
        raise ValueError("invalid_source_observation_id must be a lowercase 128-bit hex token")
    return observation_id


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
    if failures > 0 and document.get("next_due_iso") is None:
        raise ValueError("next_due_iso is required when consecutive_failures is positive")
    etag = document.get("etag")
    if etag is not None and not isinstance(etag, str):
        raise ValueError("etag must be a string")
    accept_encoding = document.get("accept_encoding", ACCEPT_ENCODING)
    if not isinstance(accept_encoding, str) or not accept_encoding:
        raise ValueError("accept_encoding must be a string")
    source_url = _normalized_source_url(document.get("source_url", DEFAULT_MAPPING_SOURCE_URL))
    source_epoch = document.get("source_epoch")
    if source_epoch is not None and not _is_hex32(source_epoch):
        raise ValueError("source_epoch must be a lowercase 128-bit hex token")
    refresh_attempt_id = document.get("refresh_attempt_id")
    if refresh_attempt_id is not None and not _is_hex32(refresh_attempt_id):
        raise ValueError("refresh_attempt_id must be a lowercase 128-bit hex token")
    last_outcome = document.get("last_outcome")
    if last_outcome is not None and not isinstance(last_outcome, str):
        raise ValueError("last_outcome must be a string")
    invalid_source_hold = _invalid_source_hold_from_document(document, last_outcome)
    invalid_source_observation_id = _invalid_source_observation_id_from_document(document)
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
        source_epoch=source_epoch,
        refresh_attempt_id=refresh_attempt_id,
        source_change_pending=source_change_pending,
        last_outcome=last_outcome,
        invalid_source_hold=invalid_source_hold,
        invalid_source_observation_id=invalid_source_observation_id,
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
    source_epoch = document.get("source_epoch")
    if source_epoch is not None and not _is_hex32(source_epoch):
        raise ValueError("source_epoch must be a lowercase 128-bit hex token")
    return _Meta(
        content_sha256=cast(str, document["content_sha256"]),
        targets_digest=cast(str, document["targets_digest"]),
        structure_digest=cast(str, document["structure_digest"]),
        revision=revision,
        activated_iso=activated,
        source_url=source_url,
        source_epoch=source_epoch,
        entries_indexed=counts["entries_indexed"],
        mapping_rules=counts["mapping_rules"],
        explicit_pairs=counts["explicit_pairs"],
    )


def _source_transition_from_document(document: Any) -> _SourceTransition | None:
    if not isinstance(document, dict) or set(document) != _TRANSITION_FIELDS:
        return None
    raw_url = document.get("source_url")
    epoch = document.get("source_epoch")
    raw_state = document.get("scheduler_state")
    if (
        not isinstance(raw_url, str)
        or not isinstance(epoch, str)
        or not _is_hex32(epoch)
        or not isinstance(raw_state, dict)
        or not _REQUIRED_SNAPSHOT_STATE_FIELDS.issubset(raw_state)
    ):
        return None
    try:
        source_url = _normalized_source_url(raw_url)
        scheduler_state = _state_from_document(cast("dict[str, Any]", raw_state))
    except (ConfigError, ValueError):
        return None
    if scheduler_state.source_url != source_url or scheduler_state.source_epoch != epoch:
        return None
    return _SourceTransition(source_url, epoch, scheduler_state)


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
        self._verified_active_identity: tuple[int, str, str, str, str | None] | None = None

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
    def generations_path(self) -> Path:
        return self.directory / "generations"

    @property
    def active_generation_path(self) -> Path:
        return self.directory / "active.generation.json"

    @property
    def _legacy_active_path(self) -> Path:
        return self.directory / "active.xml"

    @property
    def _legacy_meta_path(self) -> Path:
        return self.directory / "active.meta.json"

    def _load_active_pointer(self) -> tuple[str, _State] | None:
        try:
            document = json.loads(self.active_generation_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        if not isinstance(document, dict) or set(document) != {"generation", "state"}:
            return None
        generation = document.get("generation")
        raw_state = document.get("state")
        if not isinstance(generation, str) or not _is_hex64(generation):
            return None
        if not isinstance(raw_state, dict) or not _REQUIRED_SNAPSHOT_STATE_FIELDS.issubset(
            raw_state
        ):
            return None
        try:
            state = _state_from_document(cast("dict[str, Any]", raw_state))
        except (ValueError, ConfigError):
            return None
        return generation, state

    def _active_files(self) -> tuple[Path, Path, str | None] | None:
        if self.active_generation_path.exists():
            pointer = self._load_active_pointer()
            if pointer is None:
                return None
            generation, _ = pointer
            generation_dir = self.generations_path / generation
            return (
                generation_dir / "active.xml",
                generation_dir / "active.meta.json",
                generation,
            )
        return self._legacy_active_path, self._legacy_meta_path, None

    @property
    def active_path(self) -> Path:
        files = self._active_files()
        return self._legacy_active_path if files is None else files[0]

    @property
    def meta_path(self) -> Path:
        files = self._active_files()
        return self._legacy_meta_path if files is None else files[1]

    @property
    def state_path(self) -> Path:
        return self.directory / "state.json"

    @property
    def source_transition_path(self) -> Path:
        return self.directory / "source-transition.json"

    @property
    def generation_lock_path(self) -> Path:
        return self.directory / "generation.lock"

    @property
    def invalid_source_hold_path(self) -> Path:
        return self.directory / "invalid-source-hold.json"

    @property
    def invalid_source_hold_lock_path(self) -> Path:
        return self.directory / "invalid-source-hold.lock"

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

    @contextmanager
    def _invalid_source_hold_lock(self) -> Iterator[None]:
        """Serialize only latch-file mutations, never waiting on the generation lease.

        Invalid observations take and release this short independent OS lock before they try
        the generation lock. Successful refreshes may take it while holding the generation
        lock, so there is no lock-order cycle with a protected POST.
        """
        lock = ApplyProcessLock(self.invalid_source_hold_lock_path)
        while not lock.acquire():
            time.sleep(GENERATION_LOCK_POLL_SECONDS)
        try:
            yield
        finally:
            lock.release()

    def _invalid_source_latch_snapshot_unlocked(self) -> tuple[tuple[str, bytes], ...]:
        """Capture the marker and any interrupted atomic-write temporaries."""
        snapshot: list[tuple[str, bytes]] = []
        paths = [self.invalid_source_hold_path]
        paths.extend(
            sorted(
                self.directory.glob(f"{self.invalid_source_hold_path.name}.*.new"),
                key=lambda path: path.name,
            )
        )
        for path in paths:
            try:
                snapshot.append((path.name, path.read_bytes()))
            except FileNotFoundError:
                continue
            except OSError:
                # Unreadable evidence must remain a hold. It cannot compare equal to an
                # ordinary empty snapshot and therefore cannot be cleared by a stale read.
                snapshot.append((path.name, b"\x00unreadable"))
        return tuple(snapshot)

    def _invalid_source_latch_snapshot(self) -> tuple[tuple[str, bytes], ...]:
        with self._invalid_source_hold_lock():
            return self._invalid_source_latch_snapshot_unlocked()

    def _persist_invalid_source_observation(self, now: datetime) -> str:
        """Atomically latch a malformed-source observation before scheduler lock contention."""
        observation_id = uuid.uuid4().hex
        payload = _json_bytes({"observation_id": observation_id, "observed_iso": _iso(now)})
        with self._invalid_source_hold_lock():
            _atomic_write_durable(self.invalid_source_hold_path, payload)
        return observation_id

    def _clear_invalid_source_observation(self, expected: tuple[tuple[str, bytes], ...]) -> bool:
        """Clear only the exact invalid evidence seen by a validated 200 response."""
        with self._invalid_source_hold_lock():
            if self._invalid_source_latch_snapshot_unlocked() != expected:
                return False
            for name, _payload in expected:
                with suppress(OSError):
                    (self.directory / name).unlink(missing_ok=True)
            return True

    def _pending_invalid_source_observation(
        self, now: datetime
    ) -> _InvalidSourceObservation | None:
        snapshot = self._invalid_source_latch_snapshot()
        if not snapshot:
            return None
        if len(snapshot) == 1 and snapshot[0][0] == self.invalid_source_hold_path.name:
            try:
                document = json.loads(snapshot[0][1])
                if (
                    isinstance(document, dict)
                    and set(document) == {"observation_id", "observed_iso"}
                    and _is_hex32(document.get("observation_id"))
                ):
                    observed_at = _parse_iso(document.get("observed_iso"))
                    if observed_at is not None:
                        return _InvalidSourceObservation(document["observation_id"], observed_at)
            except (UnicodeDecodeError, ValueError):
                pass
        identity = hashlib.sha256(
            b"".join(name.encode("utf-8") + b"\0" + payload for name, payload in snapshot)
        ).hexdigest()[:_HEX32]
        return _InvalidSourceObservation(identity, _as_utc(now))

    def request_headers(self) -> dict[str, str]:
        """Headers for source requests: ``Accept-Encoding: identity`` plus the stored ETag.

        The ETag is sent only with the encoding it was recorded under (``Vary:
        Accept-Encoding``), the configured source, and a verified active generation. A
        validator without its matching bytes or provenance cannot recover a cache on 304.
        """
        headers = {"Accept-Encoding": ACCEPT_ENCODING}
        state = self._load_state()
        identity = self.active_identity()
        if (
            _source_url_matches(self, state.source_url)
            and identity
            == (
                state.revision,
                state.content_sha256,
                state.targets_digest,
                state.structure_digest,
                state.source_epoch,
            )
            and state.etag is not None
            and state.accept_encoding == ACCEPT_ENCODING
            and not self.invalid_source_hold()
        ):
            headers["If-None-Match"] = state.etag
        return headers

    def headers_factory(self) -> HeadersFactory:
        """Factory to give :func:`make_client` so conditional headers track disk state."""
        return self.request_headers

    def _load_state(self) -> _State:
        return self._load_state_with_validity()[0]

    def invalid_source_hold(self) -> bool:
        """Return the durable invalid-source write hold without consulting the network.

        Pointer and transition snapshots are included so a crash between their atomic
        update and the scheduler-state update cannot temporarily reopen writes. A corrupt
        existing scheduler file also fails closed until a validated refresh repairs it.
        """
        try:
            if self.invalid_source_hold_path.exists() or any(
                self.directory.glob(f"{self.invalid_source_hold_path.name}.*.new")
            ):
                return True
        except OSError:
            return True
        state, valid = self._load_state_with_validity()
        if state.invalid_source_hold:
            return True
        pointer = self._load_active_pointer()
        if pointer is not None and pointer[1].invalid_source_hold:
            return True
        transition, transition_valid = self._load_source_transition_with_validity()
        if (
            transition_valid
            and transition is not None
            and transition.scheduler_state.invalid_source_hold
        ):
            return True
        return not valid and self.state_path.exists()

    def _load_state_with_validity(self) -> tuple[_State, bool]:
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return _State(), False
        if not isinstance(payload, dict) or not _REQUIRED_STATE_FIELDS.issubset(payload):
            return _State(), False
        try:
            return _state_from_document(cast("dict[str, Any]", payload)), True
        except (ValueError, ConfigError):
            return _State(), False

    def _save_state(self, state: _State) -> None:
        _atomic_write(self.state_path, _json_bytes(asdict(state)))

    def _load_source_transition_with_validity(
        self,
    ) -> tuple[_SourceTransition | None, bool]:
        try:
            document = json.loads(self.source_transition_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None, True
        except (OSError, ValueError):
            return None, False
        transition = _source_transition_from_document(document)
        return transition, transition is not None

    def _save_source_transition(self, state: _State) -> None:
        if state.source_epoch is None:
            raise ValueError("pending source transitions require a source epoch")
        _atomic_write(
            self.source_transition_path,
            _json_bytes(
                {
                    "source_url": state.source_url,
                    "source_epoch": state.source_epoch,
                    "scheduler_state": asdict(state),
                }
            ),
        )

    def _save_pointer_transition(self, state: _State) -> None:
        pointer = self._load_active_pointer()
        if pointer is None:
            return
        _atomic_write(
            self.active_generation_path,
            _json_bytes({"generation": pointer[0], "state": asdict(state)}),
        )

    def _save_pointer_scheduler_state(self, state: _State) -> None:
        pointer = self._load_active_pointer()
        transition, transition_valid = self._load_source_transition_with_validity()
        if not transition_valid:
            return
        if state.source_change_pending:
            self._save_pending_scheduler_state(state, pointer, transition)
            return
        if pointer is None:
            return
        self._save_active_scheduler_state(state, pointer, transition)

    def _save_pending_scheduler_state(
        self,
        state: _State,
        pointer: tuple[str, _State] | None,
        transition: _SourceTransition | None,
    ) -> None:
        if (
            state.source_epoch is None
            or transition is None
            or transition.source_url != state.source_url
            or transition.source_epoch != state.source_epoch
        ):
            return
        if pointer is not None:
            pointer_state = pointer[1]
            if (
                not pointer_state.source_change_pending
                or pointer_state.source_url != state.source_url
                or pointer_state.source_epoch != state.source_epoch
            ):
                return
            _atomic_write(
                self.active_generation_path,
                _json_bytes({"generation": pointer[0], "state": asdict(state)}),
            )
        self._save_source_transition(state)

    def _save_active_scheduler_state(
        self,
        state: _State,
        pointer: tuple[str, _State],
        transition: _SourceTransition | None,
    ) -> None:
        meta = self._load_meta()
        if (
            meta is None
            or not self._active_verified()
            or pointer[0] != _generation_id(_json_bytes(asdict(meta)))
            or not _source_url_matches(self, state.source_url)
            or state.source_change_pending
            or state.source_epoch != meta.source_epoch
            or state.content_sha256 != meta.content_sha256
            or state.targets_digest != meta.targets_digest
            or state.structure_digest != meta.structure_digest
            or state.revision != meta.revision
        ):
            return
        if transition is not None and (transition.source_url, transition.source_epoch) != (
            meta.source_url,
            meta.source_epoch,
        ):
            return
        _atomic_write(
            self.active_generation_path,
            _json_bytes({"generation": pointer[0], "state": asdict(state)}),
        )
        if transition is not None:
            self._save_source_transition(state)

    def _load_meta(self) -> _Meta | None:
        files = self._active_files()
        if files is None:
            return None
        try:
            payload = json.loads(files[1].read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        if not isinstance(payload, dict):
            return None
        try:
            return _meta_from_document(cast("dict[str, Any]", payload))
        except (ValueError, ConfigError):
            return None

    def _read_active_bytes(self) -> bytes | None:
        files = self._active_files()
        if files is None:
            return None
        try:
            return files[0].read_bytes()
        except OSError:
            return None

    def _pointer_matches_meta(self, meta: _Meta) -> bool:
        files = self._active_files()
        if files is None:
            return False
        generation = files[2]
        if generation is None:
            return True
        pointer = self._load_active_pointer()
        if pointer is None:
            return False
        pointer_generation, state = pointer
        return (
            generation == pointer_generation
            and generation == _generation_id(_json_bytes(asdict(meta)))
            and not state.source_change_pending
            and state.source_url == meta.source_url
            and state.source_epoch == meta.source_epoch
            and state.content_sha256 == meta.content_sha256
            and state.targets_digest == meta.targets_digest
            and state.structure_digest == meta.structure_digest
            and state.revision == meta.revision
        )

    def _verify_active_generation(self, overrides: Overrides | None) -> _VerifiedGeneration | None:
        files = self._active_files()
        if files is None:
            return None
        try:
            meta_document = json.loads(files[1].read_text(encoding="utf-8"))
            if not isinstance(meta_document, dict):
                raise ValueError("active metadata must be an object")
            meta = _meta_from_document(cast("dict[str, Any]", meta_document))
            body = files[0].read_bytes()
            parsed = parse_anime_lists(body)
            if parsed.invalid_entries:
                raise MappingError("active generation contains malformed Anime-Lists entries")
            index = build_mapping_index(parsed, overrides)
        except (OSError, ValueError, ConfigError, MappingError):
            return None
        pointer_state: _State | None = None
        source_fence_valid = True
        if files[2] is not None:
            pointer = self._load_active_pointer()
            if pointer is None:
                return None
            generation, pointer_state = pointer
            if generation != files[2] or generation != _generation_id(_json_bytes(asdict(meta))):
                return None
            source_fence_valid = (
                not pointer_state.source_change_pending
                and pointer_state.source_url == meta.source_url
                and pointer_state.source_epoch == meta.source_epoch
                and pointer_state.content_sha256 == meta.content_sha256
                and pointer_state.targets_digest == meta.targets_digest
                and pointer_state.structure_digest == meta.structure_digest
                and pointer_state.revision == meta.revision
            )
        transition, transition_valid = self._load_source_transition_with_validity()
        source_fence_valid = (
            source_fence_valid
            and transition_valid
            and (
                transition is None
                or (
                    transition.source_url == meta.source_url
                    and transition.source_epoch == meta.source_epoch
                )
            )
        )
        if (
            content_digest(body) != meta.content_sha256
            or (
                targets_digest(index) != meta.targets_digest
                and legacy_targets_digest(index) != meta.targets_digest
            )
            or structure_digest(index) != meta.structure_digest
            or index.entries_indexed != meta.entries_indexed
            or index.mapping_rules != meta.mapping_rules
            or index.explicit_pairs != meta.explicit_pairs
        ):
            return None
        return _VerifiedGeneration(files, meta, index, pointer_state, source_fence_valid)

    def _recover_pointer_state(self, verified: _VerifiedGeneration) -> None:
        """Finish bookkeeping from a fully validated, source-bound pointer snapshot."""
        committed_state = verified.pointer_state
        if (
            not verified.source_fence_valid
            or committed_state is None
            or not _source_url_matches(self, verified.meta.source_url)
            or not _source_url_matches(self, committed_state.source_url)
        ):
            return
        transition, transition_valid = self._load_source_transition_with_validity()
        if (
            transition_valid
            and transition is not None
            and transition.source_url == verified.meta.source_url
            and transition.source_epoch == verified.meta.source_epoch
            and transition.scheduler_state != committed_state
        ):
            self._save_source_transition(committed_state)
        current, current_state_valid = self._load_state_with_validity()
        if not current_state_valid:
            self._save_state(committed_state)
            return
        same_source_epoch = (
            _source_url_matches(self, current.source_url)
            and current.source_epoch == committed_state.source_epoch
        )
        current_matches_generation = (
            not current.source_change_pending
            and current.content_sha256 == verified.meta.content_sha256
            and current.targets_digest == verified.meta.targets_digest
            and current.structure_digest == verified.meta.structure_digest
            and current.revision == verified.meta.revision
        )
        current_matches_committed_attempt = (
            current.refresh_attempt_id == committed_state.refresh_attempt_id
            and current.last_attempted_iso == committed_state.last_attempted_iso
        )
        current_matches_scheduler = _same_scheduler_snapshot(current, committed_state)
        interrupted_source_change = current.source_change_pending and same_source_epoch
        interrupted_bookkeeping = same_source_epoch and not current_matches_generation
        interrupted_scheduler_update = (
            same_source_epoch
            and current_matches_generation
            and current_matches_committed_attempt
            and not current_matches_scheduler
        )
        if interrupted_source_change or interrupted_bookkeeping or interrupted_scheduler_update:
            self._save_state(committed_state)

    def _recover_pending_transition_scheduler(self, active: ActiveMappings | None) -> None:
        current, current_valid = self._load_state_with_validity()
        if active is not None:
            if (
                current_valid
                and not current.source_change_pending
                and current.source_url == active.source_url
                and current.source_epoch == active.source_epoch
                and current.content_sha256 == active.content_sha256
                and current.targets_digest == active.targets_digest
                and current.structure_digest == active.structure_digest
                and current.revision == active.revision
            ):
                self._save_pointer_scheduler_state(current)
            return
        pointer = self._load_active_pointer()
        pending_state = (
            pointer[1]
            if pointer is not None
            and pointer[1].source_change_pending
            and _source_url_matches(self, pointer[1].source_url)
            else None
        )
        if pending_state is None:
            transition, transition_valid = self._load_source_transition_with_validity()
            if (
                transition_valid
                and transition is not None
                and transition.scheduler_state.source_change_pending
                and _source_url_matches(self, transition.source_url)
            ):
                pending_state = transition.scheduler_state
        if pending_state is None:
            return
        if (
            current_valid
            and current.source_change_pending
            and current.source_url == pending_state.source_url
            and current.source_epoch == pending_state.source_epoch
        ):
            if current.refresh_attempt_id != pending_state.refresh_attempt_id:
                return
            if _same_scheduler_snapshot(current, pending_state):
                return
        self._save_state(pending_state)

    def _cleanup_generations(self) -> None:
        """Discard unreferenced generations after pointer and state are committed."""
        files = self._active_files()
        if files is None or files[2] is None:
            return
        try:
            generations = tuple(self.generations_path.iterdir())
        except OSError:
            return
        for generation_dir in generations:
            if (
                generation_dir.is_dir()
                and _is_hex64(generation_dir.name)
                and generation_dir.name != files[2]
            ):
                shutil.rmtree(generation_dir, ignore_errors=True)
        for legacy_path in (self._legacy_active_path, self._legacy_meta_path):
            with suppress(OSError):
                legacy_path.unlink(missing_ok=True)

    def _migrate_legacy_active_generation(
        self,
        overrides: Overrides | None,
        *,
        transition_state: _State | None = None,
    ) -> None:
        """Migrate a flat cache before transition, retaining the transition fence."""
        files = self._active_files()
        if files is None or files[2] is not None:
            return
        meta = self._load_meta()
        body = self._read_active_bytes()
        if meta is None or body is None or content_digest(body) != meta.content_sha256:
            return
        try:
            parsed = parse_anime_lists(body)
            if parsed.invalid_entries:
                raise MappingError("legacy generation contains malformed Anime-Lists entries")
            upstream_index = build_mapping_index(parsed)
            index = build_mapping_index(parsed, overrides)
        except (MappingError, ConfigError):
            return
        if (
            upstream_index.entries_indexed != meta.entries_indexed
            or upstream_index.mapping_rules != meta.mapping_rules
            or upstream_index.explicit_pairs != meta.explicit_pairs
        ):
            return
        index_matches = (
            targets_digest(index) == meta.targets_digest
            or legacy_targets_digest(index) == meta.targets_digest
        ) and structure_digest(index) == meta.structure_digest
        if not index_matches and transition_state is None:
            return

        state, state_valid = self._load_state_with_validity()
        if not (
            state_valid
            and not state.source_change_pending
            and state.source_url == meta.source_url
            and state.source_epoch == meta.source_epoch
            and state.content_sha256 == meta.content_sha256
            and state.targets_digest == meta.targets_digest
            and state.structure_digest == meta.structure_digest
            and state.revision == meta.revision
        ):
            state = _State(
                source_url=meta.source_url,
                source_epoch=meta.source_epoch,
                content_sha256=meta.content_sha256,
                targets_digest=meta.targets_digest,
                structure_digest=meta.structure_digest,
                revision=meta.revision,
                last_success_iso=meta.activated_iso,
                invalid_source_hold=self.invalid_source_hold(),
            )
        if transition_state is not None:
            state = transition_state

        meta_bytes = _json_bytes(asdict(meta))
        generation = _generation_id(meta_bytes)
        generation_dir = self.generations_path / generation
        xml_path = generation_dir / "active.xml"
        meta_path = generation_dir / "active.meta.json"
        _atomic_write(xml_path, body)
        _atomic_write(meta_path, meta_bytes)
        with xml_path.open("rb") as staged_file:
            staged_digest = hashlib.file_digest(staged_file, "sha256").hexdigest()
        if staged_digest != meta.content_sha256:
            return
        _atomic_write(
            self.active_generation_path,
            _json_bytes({"generation": generation, "state": asdict(state)}),
        )
        self._cleanup_generations()

    def _active_verified(self) -> bool:
        meta = self._load_meta()
        state = self._load_state()
        body = self._read_active_bytes()
        transition, transition_valid = self._load_source_transition_with_validity()
        if not transition_valid:
            return False
        if (
            meta is None
            or body is None
            or not self._pointer_matches_meta(meta)
            or not _source_url_matches(self, meta.source_url)
            or not _source_url_matches(self, state.source_url)
            or state.source_change_pending
            or state.source_epoch != meta.source_epoch
            or (
                transition is not None
                and (
                    transition.source_url != meta.source_url
                    or transition.source_epoch != meta.source_epoch
                )
            )
            or content_digest(body) != meta.content_sha256
        ):
            return False
        try:
            parsed = parse_anime_lists(body)
        except (MappingError, ConfigError):
            return False
        return parsed.invalid_entries == 0

    def _state_matches_active(self, state: _State) -> bool:
        meta = self._load_meta()
        return (
            meta is not None
            and _source_url_matches(self, state.source_url)
            and _source_url_matches(self, meta.source_url)
            and self._active_verified()
            and state.source_epoch == meta.source_epoch
            and state.content_sha256 == meta.content_sha256
            and state.targets_digest == meta.targets_digest
            and state.structure_digest == meta.structure_digest
            and state.revision == meta.revision
        )

    def active_identity(self) -> tuple[int, str, str, str, str | None] | None:
        """Read a verified active identity, validating XML once per unseen generation."""
        while True:
            with self.generation_lock(
                wait_seconds=GENERATION_LOCK_REFRESH_WAIT_SECONDS
            ) as acquired:
                if acquired:
                    return self.active_identity_locked()

    def active_identity_locked(self) -> tuple[int, str, str, str, str | None] | None:
        """Read identity under the caller's lock, parsing only an unverified generation."""
        files = self._active_files()
        meta = self._load_meta()
        body = self._read_active_bytes()
        state = self._load_state()
        transition, transition_valid = self._load_source_transition_with_validity()
        if (
            files is None
            or meta is None
            or body is None
            or not transition_valid
            or not self._pointer_matches_meta(meta)
            or not _source_url_matches(self, meta.source_url)
            or not _source_url_matches(self, state.source_url)
            or state.source_change_pending
            or state.source_epoch != meta.source_epoch
            or state.content_sha256 != meta.content_sha256
            or state.targets_digest != meta.targets_digest
            or state.structure_digest != meta.structure_digest
            or state.revision != meta.revision
            or content_digest(body) != meta.content_sha256
            or (
                transition is not None
                and (
                    transition.source_url != meta.source_url
                    or transition.source_epoch != meta.source_epoch
                )
            )
        ):
            return None
        identity = (
            meta.revision,
            meta.content_sha256,
            meta.targets_digest,
            meta.structure_digest,
            meta.source_epoch,
        )
        if self._verified_active_identity != identity:
            if not self._active_verified():
                return None
            self._verified_active_identity = identity
        return identity


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


def _begin_from_pointer_transition(
    store: MappingStore,
    state: _State,
    valid: bool,
) -> _State | None:
    pointer = store._load_active_pointer()
    if pointer is None:
        return None
    pointer_state = pointer[1]
    meta = store._load_meta()
    pointer_transition_pending = (
        pointer_state.source_change_pending
        or meta is None
        or pointer_state.source_url != meta.source_url
        or pointer_state.source_epoch != meta.source_epoch
        or pointer_state.content_sha256 != meta.content_sha256
        or pointer_state.targets_digest != meta.targets_digest
        or pointer_state.structure_digest != meta.structure_digest
        or pointer_state.revision != meta.revision
    )
    if not pointer_transition_pending:
        return None
    if pointer_state.source_change_pending and _source_url_matches(store, pointer_state.source_url):
        if (
            valid
            and state.source_url == pointer_state.source_url
            and state.source_epoch == pointer_state.source_epoch
            and state.source_change_pending
        ):
            same_attempt = (
                state.refresh_attempt_id == pointer_state.refresh_attempt_id
                and state.last_attempted_iso == pointer_state.last_attempted_iso
            )
            pointer_state = (
                pointer_state
                if same_attempt and not _same_scheduler_snapshot(state, pointer_state)
                else state
            )
    else:
        state = _State(
            source_url=store.source_url,
            source_epoch=uuid.uuid4().hex,
            source_change_pending=True,
            invalid_source_hold=store.invalid_source_hold(),
        )
        pointer_state = state
    state = pointer_state
    store._save_pointer_transition(state)
    store._save_source_transition(state)
    if not valid or state != store._load_state():
        store._save_state(state)
    return state


def _begin_refresh_state(store: MappingStore, overrides: Overrides | None) -> _State | None:
    with store.generation_lock(wait_seconds=GENERATION_LOCK_REFRESH_WAIT_SECONDS) as acquired:
        if not acquired:
            return None
        return _begin_refresh_state_locked(store, overrides)


def _recover_refresh_state_if_missing(
    store: MappingStore, state: _State, valid: bool, overrides: Overrides | None
) -> tuple[_State, bool]:
    if valid:
        return state, valid
    committed = store._verify_active_generation(overrides)
    if (
        committed is not None
        and committed.source_fence_valid
        and committed.pointer_state is not None
        and _source_url_matches(store, committed.meta.source_url)
    ):
        store._recover_pointer_state(committed)
        return store._load_state_with_validity()
    return state, valid


def _resume_transition_state(
    store: MappingStore,
    state: _State,
    valid: bool,
    transition: _SourceTransition | None,
    transition_valid: bool,
    overrides: Overrides | None,
) -> _State | None:
    if valid and _source_url_matches(store, state.source_url):
        if not transition_valid and state.source_change_pending and state.source_epoch is not None:
            store._save_pointer_transition(state)
            store._save_source_transition(state)
            return state
        if (
            transition_valid
            and transition is not None
            and transition.source_url == state.source_url
            and transition.source_epoch == state.source_epoch
        ):
            snapshot = transition.scheduler_state
            if (
                state.refresh_attempt_id == snapshot.refresh_attempt_id
                and state.last_attempted_iso == snapshot.last_attempted_iso
                and not _same_scheduler_snapshot(state, snapshot)
            ):
                store._migrate_legacy_active_generation(overrides, transition_state=snapshot)
                store._save_state(snapshot)
                return snapshot
            store._migrate_legacy_active_generation(overrides, transition_state=state)
            return state
        if transition_valid and transition is None:
            if state.source_epoch is None and state.source_change_pending:
                state = _State(
                    source_url=store.source_url,
                    source_epoch=uuid.uuid4().hex,
                    source_change_pending=True,
                    invalid_source_hold=store.invalid_source_hold(),
                )
                store._migrate_legacy_active_generation(overrides, transition_state=state)
                store._save_pointer_transition(state)
                store._save_source_transition(state)
                store._save_state(state)
            elif state.source_epoch is not None:
                store._migrate_legacy_active_generation(overrides, transition_state=state)
                store._save_pointer_transition(state)
                store._save_source_transition(state)
            return state
    if not valid:
        return _resume_invalid_source_state(store, state, transition, transition_valid, overrides)
    return None


def _resume_invalid_source_state(
    store: MappingStore,
    state: _State,
    transition: _SourceTransition | None,
    transition_valid: bool,
    overrides: Overrides | None,
) -> _State | None:
    if not transition_valid:
        return None
    if transition is not None and _source_url_matches(store, transition.source_url):
        state = transition.scheduler_state
    elif transition is None and _source_url_matches(store, state.source_url):
        state = _State(
            source_url=store.source_url,
            source_epoch=uuid.uuid4().hex,
            source_change_pending=True,
            invalid_source_hold=store.invalid_source_hold(),
        )
    else:
        return None
    store._migrate_legacy_active_generation(overrides, transition_state=state)
    store._save_pointer_transition(state)
    store._save_source_transition(state)
    store._save_state(state)
    return state


def _start_source_transition(
    store: MappingStore,
    transition: _SourceTransition | None,
    transition_valid: bool,
    overrides: Overrides | None,
) -> _State:
    state = (
        transition.scheduler_state
        if transition_valid
        and transition is not None
        and _source_url_matches(store, transition.source_url)
        and transition.scheduler_state.source_change_pending
        else _State(
            source_url=store.source_url,
            source_epoch=uuid.uuid4().hex,
            source_change_pending=True,
            invalid_source_hold=store.invalid_source_hold(),
        )
    )
    store._migrate_legacy_active_generation(overrides, transition_state=state)
    store._save_pointer_transition(state)
    store._save_source_transition(state)
    store._save_state(state)
    return state


def _begin_refresh_state_locked(store: MappingStore, overrides: Overrides | None) -> _State | None:
    state, valid = store._load_state_with_validity()
    transition, transition_valid = store._load_source_transition_with_validity()
    state, valid = _recover_refresh_state_if_missing(store, state, valid, overrides)
    _reconcile_pending_invalid_source_locked(store)
    state, valid = store._load_state_with_validity()
    transition, transition_valid = store._load_source_transition_with_validity()
    if (
        not valid
        and transition_valid
        and transition is None
        and _source_url_matches(store, state.source_url)
    ):
        files = store._active_files()
        if files is not None and files[2] is None:
            store._migrate_legacy_active_generation(overrides)
            committed = store._verify_active_generation(overrides)
            if (
                committed is not None
                and committed.source_fence_valid
                and _source_url_matches(store, committed.meta.source_url)
            ):
                store._recover_pointer_state(committed)
                state, valid = store._load_state_with_validity()
    pointer_state = _begin_from_pointer_transition(store, state, valid)
    if pointer_state is not None:
        return pointer_state
    if not valid and not transition_valid:
        return None
    active_meta = store._load_meta()
    if active_meta is not None and not _source_url_matches(store, active_meta.source_url):
        return _start_source_transition(store, transition, transition_valid, overrides)
    resumed_state = _resume_transition_state(
        store, state, valid, transition, transition_valid, overrides
    )
    if resumed_state is not None:
        return resumed_state
    return _start_source_transition(store, transition, transition_valid, overrides)


def _state_after_active_check(
    store: MappingStore, state: _State, active_valid: bool
) -> _State | None:
    current = store._load_state()
    if active_valid or current.etag is None:
        return current
    with store.generation_lock(wait_seconds=GENERATION_LOCK_REFRESH_WAIT_SECONDS) as acquired:
        if not acquired:
            return None
        current = store._load_state()
        if _same_refresh_generation(current, state) and _source_url_matches(
            store, current.source_url
        ):
            current.etag = None
            store._save_state(current)
        return current


def _prepare_refresh_state(
    store: MappingStore, overrides_text: str | None
) -> tuple[_State | None, Overrides | None, bool]:
    cache_overrides: Overrides | None = None
    overrides_valid = True
    if overrides_text is not None:
        try:
            cache_overrides = load_overrides(overrides_text)
        except ConfigError:
            overrides_valid = False
    state = _begin_refresh_state(store, cache_overrides)
    if state is None:
        return None, cache_overrides, overrides_valid
    active_valid = overrides_valid and load_active(store, cache_overrides) is not None
    return _state_after_active_check(store, state, active_valid), cache_overrides, overrides_valid


def _start_refresh_attempt(
    store: MappingStore, expected_state: _State, now: datetime
) -> tuple[_State | None, RefreshOutcome | None]:
    with store.generation_lock(wait_seconds=GENERATION_LOCK_REFRESH_WAIT_SECONDS) as acquired:
        if not acquired:
            return None, RefreshOutcome.FAILED_LOCKED
        state = store._load_state()
        if not _source_url_matches(store, state.source_url) or not _same_refresh_generation(
            state, expected_state
        ):
            return None, RefreshOutcome.FAILED_STALE
        state.last_attempted_iso = _iso(now)
        state.refresh_attempt_id = uuid.uuid4().hex
        store._save_state(state)
        return state, None


def _prepare_refresh_request(
    store: MappingStore,
    endpoint: ApiEndpoint,
    now: datetime,
    *,
    force: bool,
    overrides_text: str | None,
) -> tuple[_State | None, RefreshOutcome | None]:
    state, overrides, overrides_valid = _prepare_refresh_state(store, overrides_text)
    if state is None:
        return None, RefreshOutcome.FAILED_LOCKED
    if not force and not due(store, now, overrides if overrides_valid else None):
        return None, RefreshOutcome.NOT_DUE
    state = store._load_state()
    if not _source_url_matches(store, state.source_url):
        return None, RefreshOutcome.FAILED_STALE
    _require_source_endpoint(store, endpoint)
    return _start_refresh_attempt(store, state, now)


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
    return (
        _State(
            source_url=store.source_url,
            source_epoch=uuid.uuid4().hex,
            source_change_pending=True,
            invalid_source_hold=state.invalid_source_hold or store.invalid_source_hold(),
        ),
        True,
    )


def _same_refresh_generation(current: _State, expected: _State) -> bool:
    return (
        current.source_url == expected.source_url
        and current.source_epoch == expected.source_epoch
        and current.refresh_attempt_id == expected.refresh_attempt_id
        and current.source_change_pending == expected.source_change_pending
        and current.content_sha256 == expected.content_sha256
        and current.targets_digest == expected.targets_digest
        and current.structure_digest == expected.structure_digest
        and current.revision == expected.revision
    )


def _same_scheduler_snapshot(current: _State, expected: _State) -> bool:
    return (
        current.last_attempted_iso == expected.last_attempted_iso
        and current.last_success_iso == expected.last_success_iso
        and current.next_due_iso == expected.next_due_iso
        and current.etag == expected.etag
        and current.accept_encoding == expected.accept_encoding
        and current.consecutive_failures == expected.consecutive_failures
        and current.refresh_attempt_id == expected.refresh_attempt_id
        and current.last_outcome == expected.last_outcome
        and current.invalid_source_hold == expected.invalid_source_hold
        and current.invalid_source_observation_id == expected.invalid_source_observation_id
    )


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
    invalid_observation_id = (
        store._persist_invalid_source_observation(now)
        if outcome is RefreshOutcome.FAILED_INVALID
        else None
    )
    with store.generation_lock(wait_seconds=GENERATION_LOCK_REFRESH_WAIT_SECONDS) as acquired:
        if not acquired:
            return RefreshOutcome.FAILED_LOCKED
        current = store._load_state()
        if not _same_refresh_generation(current, state):
            return RefreshOutcome.FAILED_STALE
        current.last_attempted_iso = state.last_attempted_iso or current.last_attempted_iso
        return _record_failure_locked(
            store,
            current,
            now,
            outcome,
            invalid_observation_id=invalid_observation_id,
        )


def _record_failure_locked(
    store: MappingStore,
    state: _State,
    now: datetime,
    outcome: RefreshOutcome,
    *,
    invalid_observation_id: str | None = None,
) -> RefreshOutcome:
    already_recorded = (
        outcome is RefreshOutcome.FAILED_INVALID
        and invalid_observation_id is not None
        and state.invalid_source_observation_id == invalid_observation_id
    )
    if not already_recorded:
        state.consecutive_failures += 1
        state.next_due_iso = _iso(now + timedelta(hours=_backoff_hours(state.consecutive_failures)))
    state.last_outcome = outcome.value
    if outcome is RefreshOutcome.FAILED_INVALID:
        state.invalid_source_hold = True
        if invalid_observation_id is not None:
            state.invalid_source_observation_id = invalid_observation_id
    store._save_pointer_scheduler_state(state)
    store._save_state(state)
    if store._active_verified():
        return outcome
    return RefreshOutcome.NO_ACTIVE_BUT_FAILED


def _reconcile_pending_invalid_source_locked(
    store: MappingStore, now: datetime | None = None
) -> None:
    """Merge a durable lock-independent invalid observation into scheduler snapshots.

    Called only while the generation lock is held. The standalone latch protects writes
    immediately; this serialized merge records bounded retry timing after a competing
    protected operation releases its lease.
    """
    observed_at = _as_utc(now or datetime.now(UTC))
    observation = store._pending_invalid_source_observation(observed_at)
    if observation is None:
        return
    state, _valid = store._load_state_with_validity()
    if state.invalid_source_observation_id == observation.observation_id:
        if state.invalid_source_hold:
            return
        # A crash may have landed after success bookkeeping but before clearing the latch.
        # Reassert the hold and retry soon without charging the same observation twice.
        state.invalid_source_hold = True
        state.consecutive_failures = max(1, state.consecutive_failures)
        retry_at = observation.observed_at + timedelta(
            hours=_backoff_hours(state.consecutive_failures)
        )
        try:
            current_due = _parse_iso(state.next_due_iso)
        except ValueError:
            current_due = None
        if current_due is None or current_due > retry_at:
            state.next_due_iso = _iso(retry_at)
        state.last_outcome = RefreshOutcome.FAILED_INVALID.value
        store._save_pointer_scheduler_state(state)
        store._save_state(state)
        return
    _record_failure_locked(
        store,
        state,
        observation.observed_at,
        RefreshOutcome.FAILED_INVALID,
        invalid_observation_id=observation.observation_id,
    )


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
    *,
    validated_source: bool = True,
    invalid_latch_snapshot: tuple[tuple[str, bytes], ...] | None = None,
) -> RefreshOutcome:
    state = _successful_state(
        store,
        state,
        now,
        response,
        refresh_hours,
        content_sha256,
        targets,
        structure,
        revision,
        outcome,
        validated_source=validated_source,
    )
    store._save_pointer_scheduler_state(state)
    store._save_state(state)
    if validated_source and invalid_latch_snapshot is not None:
        store._clear_invalid_source_observation(invalid_latch_snapshot)
    return outcome


def _successful_state(
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
    *,
    validated_source: bool = True,
) -> _State:
    state = replace(state)
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
    if validated_source:
        state.invalid_source_hold = False
    return state


def _activate(
    store: MappingStore,
    body: bytes | bytearray,
    state_view: tuple[str, str, str, int],
    now: datetime,
    index: MappingIndex,
    successful_state: _State,
) -> None:
    content_sha256, targets, structure, revision = state_view
    meta = _Meta(
        content_sha256=content_sha256,
        targets_digest=targets,
        structure_digest=structure,
        revision=revision,
        activated_iso=_iso(now),
        source_url=store.source_url,
        source_epoch=successful_state.source_epoch,
        entries_indexed=index.entries_indexed,
        mapping_rules=index.mapping_rules,
        explicit_pairs=index.explicit_pairs,
    )
    meta_bytes = _json_bytes(asdict(meta))
    generation = _generation_id(meta_bytes)
    generation_dir = store.generations_path / generation
    xml_path = generation_dir / "active.xml"
    meta_path = generation_dir / "active.meta.json"
    _atomic_write(xml_path, body)
    _atomic_write(meta_path, meta_bytes)

    with xml_path.open("rb") as staged_file:
        staged_digest = hashlib.file_digest(staged_file, "sha256").hexdigest()
    staged_meta = _meta_from_document(
        cast("dict[str, Any]", json.loads(meta_path.read_text(encoding="utf-8")))
    )
    if staged_digest != meta.content_sha256 or staged_meta != meta:
        raise OSError("staged mapping generation did not match its validated metadata")

    pointer = {"generation": generation, "state": asdict(successful_state)}
    _atomic_write(store.active_generation_path, _json_bytes(pointer))


def _parse_accepted_index(
    body: bytes | bytearray, overrides_text: str | None
) -> MappingIndex | None:
    try:
        overrides = load_overrides(overrides_text) if overrides_text is not None else None
        parsed = parse_anime_lists(body)
        if parsed.invalid_entries:
            return None
        return build_mapping_index(parsed, overrides)
    except (MappingError, ConfigError):
        return None


def _accept_content(
    store: MappingStore,
    state: _State,
    now: datetime,
    response: httpx.Response,
    refresh_hours: float,
    overrides_text: str | None,
    body: bytes | bytearray,
) -> RefreshOutcome:
    index = _parse_accepted_index(body, overrides_text)
    if index is None:
        return _record_failure(store, state, now, RefreshOutcome.FAILED_INVALID)

    invalid_latch_snapshot = store._invalid_source_latch_snapshot()
    content_sha256 = content_digest(body)
    targets = targets_digest(index)
    structure = structure_digest(index)
    with store.generation_lock(wait_seconds=GENERATION_LOCK_REFRESH_WAIT_SECONDS) as acquired:
        if not acquired:
            return _record_failure(store, state, now, RefreshOutcome.FAILED_LOCKED)
        _reconcile_pending_invalid_source_locked(store, now)
        current_state = store._load_state()
        if not _source_url_matches(store, current_state.source_url) or not _same_refresh_generation(
            current_state, state
        ):
            return RefreshOutcome.FAILED_STALE
        current_state.last_attempted_iso = (
            state.last_attempted_iso or current_state.last_attempted_iso
        )
        state = current_state
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
                invalid_latch_snapshot=invalid_latch_snapshot,
            )
        revision = meta.revision + 1 if meta is not None else 1
        successful_state = _successful_state(
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
        _activate(
            store,
            body,
            (content_sha256, targets, structure, revision),
            now,
            index,
            successful_state,
        )
        transition, transition_valid = store._load_source_transition_with_validity()
        if (
            transition_valid
            and transition is not None
            and transition.source_url == successful_state.source_url
            and transition.source_epoch == successful_state.source_epoch
        ):
            store._save_source_transition(successful_state)
        store._save_state(successful_state)
        store._cleanup_generations()
        store._clear_invalid_source_observation(invalid_latch_snapshot)
        return RefreshOutcome.ACTIVATED


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
    :class:`~shoko_linker.config.ConfigError` from overrides/parsing, or any malformed semantic
    entry, are ``FAILED_INVALID`` with the cache untouched. The documented unsupported
    absolute-season+episodeoffset case is retained only as an ownership fence. ``ACTIVATED``
    requires a change of the effective target/structure digests (overrides and ownership fences
    are part of the digests: same bytes +
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
    state, preflight_outcome = _prepare_refresh_request(
        store,
        endpoint,
        now_utc,
        force=force,
        overrides_text=overrides_text,
    )
    if state is None:
        return preflight_outcome or RefreshOutcome.FAILED_STALE
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


def _handle_not_modified(  # noqa: PLR0911
    store: MappingStore,
    state: _State,
    now_utc: datetime,
    response: httpx.Response,
    refresh_hours: float,
    overrides_text: str | None,
) -> RefreshOutcome:
    attempted_state = state
    overrides: Overrides | None = None
    overrides_valid = True
    if overrides_text is not None:
        try:
            overrides = load_overrides(overrides_text)
        except ConfigError:
            overrides_valid = False
    active = load_active(store, overrides) if overrides_valid else None
    state = store._load_state()
    if not overrides_valid or active is None:
        # The validator is alive but our cache is not (deleted/tampered):
        # drop the ETag so the next attempt performs a full fetch, and fail
        # this one — 304 yields no content to activate.
        with store.generation_lock(wait_seconds=GENERATION_LOCK_REFRESH_WAIT_SECONDS) as acquired:
            if not acquired:
                return RefreshOutcome.FAILED_LOCKED
            current = store._load_state()
            if not _same_refresh_generation(current, attempted_state):
                return RefreshOutcome.FAILED_STALE
            current.last_attempted_iso = (
                attempted_state.last_attempted_iso or current.last_attempted_iso
            )
            current.etag = None
            return _record_failure_locked(store, current, now_utc, RefreshOutcome.FAILED_TRANSPORT)
    with store.generation_lock(wait_seconds=GENERATION_LOCK_REFRESH_WAIT_SECONDS) as acquired:
        if not acquired:
            return RefreshOutcome.FAILED_LOCKED
        current = store._load_state()
        if (
            not _same_refresh_generation(current, attempted_state)
            or not _source_url_matches(store, current.source_url)
            or not store._state_matches_active(current)
        ):
            return RefreshOutcome.FAILED_STALE
        current.last_attempted_iso = (
            attempted_state.last_attempted_iso or current.last_attempted_iso
        )
        if store.invalid_source_hold():
            # A 304 cannot validate the bytes that previously failed parsing. Treat an
            # unsolicited conditional hit as a failed recovery and keep bounded backoff.
            return _record_failure_locked(store, current, now_utc, RefreshOutcome.FAILED_TRANSPORT)
        return _record_success(
            store,
            current,
            now_utc,
            response,
            refresh_hours,
            cast("str", current.content_sha256),
            cast("str", current.targets_digest),
            cast("str", current.structure_digest),
            current.revision,
            RefreshOutcome.UNCHANGED,
            validated_source=False,
        )


def due(store: MappingStore, now: datetime, overrides: Overrides | None = None) -> bool:
    """True when refresh is due or the active generation needs recovery.

    A source change always triggers a fresh request. Missing/unusable active data also
    triggers recovery immediately, except while a persisted failed-recovery backoff is open.
    """
    now_utc = _as_utc(now)
    while True:
        with store.generation_lock(wait_seconds=GENERATION_LOCK_REFRESH_WAIT_SECONDS) as acquired:
            if not acquired:
                continue
            store._recover_pending_transition_scheduler(None)
            active = _load_active_locked(store, overrides)
            _reconcile_pending_invalid_source_locked(store, now_utc)
            state = store._load_state()
            if not _source_url_matches(store, state.source_url):
                return True
            if active is None:
                return not _recovery_backoff_active(state, now_utc)
            return _is_due(state, now_utc)


def next_refresh_due(store: MappingStore, now: datetime, refresh_hours: float) -> datetime:
    """Scheduled UTC time of the next refresh: ``now`` when due, else the recorded ``next_due``."""
    if refresh_hours <= 0:
        raise ConfigError("refresh_hours must be greater than 0")
    now_utc = _as_utc(now)
    while True:
        with store.generation_lock(wait_seconds=GENERATION_LOCK_REFRESH_WAIT_SECONDS) as acquired:
            if not acquired:
                continue
            store._recover_pending_transition_scheduler(None)
            active = _load_active_locked(store, None)
            _reconcile_pending_invalid_source_locked(store, now_utc)
            state, source_changed = _state_for_source(store, store._load_state())
            if source_changed:
                return now_utc
            if active is None:
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


def status(
    store: MappingStore,
    now: datetime,
    *,
    overrides_text: str | None = None,
) -> MappingStatus:
    """Report the cache and schedule, reconciling any committed generation first."""
    overrides: Overrides | None = None
    overrides_valid = True
    if overrides_text is not None:
        try:
            overrides = load_overrides(overrides_text)
        except ConfigError:
            overrides_valid = False
    while True:
        with store.generation_lock(wait_seconds=GENERATION_LOCK_REFRESH_WAIT_SECONDS) as acquired:
            if acquired:
                active = _load_active_locked(store, overrides) if overrides_valid else None
                store._recover_pending_transition_scheduler(active)
                _reconcile_pending_invalid_source_locked(store, _as_utc(now))
                return _status_locked(store, now, active)


def _status_locked(
    store: MappingStore, now: datetime, active: ActiveMappings | None
) -> MappingStatus:
    now_utc = _as_utc(now)
    state, source_changed = _state_for_source(store, store._load_state())
    meta = store._load_meta()
    active_present = (
        not source_changed
        and active is not None
        and meta is not None
        and _source_url_matches(store, state.source_url)
        and _source_url_matches(store, meta.source_url)
        and not state.source_change_pending
        and state.source_epoch == meta.source_epoch
        and state.content_sha256 == meta.content_sha256
        and state.targets_digest == meta.targets_digest
        and state.structure_digest == meta.structure_digest
        and state.revision == meta.revision
        and active.content_sha256 == meta.content_sha256
        and active.revision == meta.revision
    )
    if source_changed or active is None:
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
        invalid_source_hold=store.invalid_source_hold(),
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
    while True:
        with store.generation_lock(wait_seconds=GENERATION_LOCK_REFRESH_WAIT_SECONDS) as acquired:
            if acquired:
                return _load_active_locked(store, overrides)


def _load_active_locked(
    store: MappingStore,
    overrides: Overrides | None,
) -> ActiveMappings | None:
    """Load a generation while its pointer and files are fenced from activation/cleanup."""
    verified = store._verify_active_generation(overrides)
    if verified is None:
        return None
    if verified.files[2] is not None:
        store._cleanup_generations()
    if not verified.source_fence_valid:
        return None
    store._recover_pointer_state(verified)
    meta = verified.meta
    state = store._load_state()
    if (
        not _source_url_matches(store, meta.source_url)
        or not _source_url_matches(store, state.source_url)
        or state.source_change_pending
        or state.source_epoch != meta.source_epoch
        or state.content_sha256 != meta.content_sha256
        or state.targets_digest != meta.targets_digest
        or state.structure_digest != meta.structure_digest
        or state.revision != meta.revision
    ):
        return None
    store._verified_active_identity = (
        meta.revision,
        meta.content_sha256,
        meta.targets_digest,
        meta.structure_digest,
        meta.source_epoch,
    )
    return ActiveMappings(
        index=verified.index,
        content_sha256=meta.content_sha256,
        targets_digest=meta.targets_digest,
        structure_digest=meta.structure_digest,
        revision=meta.revision,
        activated_iso=meta.activated_iso,
        source_url=meta.source_url,
        source_epoch=meta.source_epoch,
        overrides_applied=overrides is not None,
    )
