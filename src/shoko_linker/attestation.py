"""M7-b: safety attestation lease — the operator-signed, expiring apply precondition.

The lease is a **plain YAML/JSON file created by the operator** (``attestation record``
builds it from flags; nothing here signs it). Integrity is a canonical SHA-256 digest
over the record's canonical JSON — **HMAC-free, no secrets involved**: the trust anchor
IS the operator, so a digest is tamper-*evidence*, not a security boundary. The
file must be readable only by operator convention (0600 where the platform supports it;
see README); the tool cannot enforce that, and it never writes the file without
an explicit, confirmed operator command.

Validation is **offline only**: no network call, no live Shoko check
(:func:`load_attestation` compares the recorded endpoint identity, version hint, scope
and expiry against the caller-supplied expectations, fail closed).

Apply semantics (PROJECT.md M7, PLAN tradeoff): a completely unattended
*indefinite* writable daemon is incompatible with an expiring physical-safety
attestation. When the lease is absent/invalid/expired (or the version hint is stale),
every apply is suppressed and the daemon stays alive reporting read-only — never a
restart loop. The daemon/campaign persist one write pause per valid→invalid transition
(``attestation_expired``) and clear it on a re-attested lease.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, Final, Literal, cast

import yaml  # type: ignore[import-untyped]

from .config import normalize_service_url
from .errors import ConfigError, LinkerError

type AttestationStatus = Literal["valid", "absent", "invalid", "expired", "stale_version"]

RECORD_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "endpoint",
        "version",
        "mount_fingerprint",
        "settings_summary",
        "inspected_iso",
        "expires_iso",
        "scope",
        "operator_note",
    }
)
SAFE_SETTINGS_FLAGS: Final[frozenset[str]] = frozenset(
    {
        "FileQualityFilterEnabled",
        "RenameOnImport",
        "MoveOnImport",
        "MyListAddFiles",
    }
)
SCOPE_DAEMON: Final[str] = "daemon"
MIN_NOTE_CHARS: Final[int] = 1
MAX_NOTE_CHARS: Final[int] = 400
MAX_MOUNT_ITEMS: Final[int] = 50
MAX_MOUNT_ITEM_CHARS: Final[int] = 200


class AttestationError(LinkerError):
    """The lease record is malformed, the record build refused, or the target is unusable."""


@dataclass(frozen=True)
class AttestationRecord:
    """One validated operator attestation record (no secret values by construction)."""

    endpoint: str
    version: str
    mount_fingerprint: tuple[str, ...]
    settings_summary: Mapping[str, bool]
    inspected_iso: str
    expires_iso: str
    scope: str
    operator_note: str


@dataclass(frozen=True)
class AttestationVerdict:
    """The apply-gate answer for one lease, with every refusal reason named."""

    status: AttestationStatus
    reason: str
    record: AttestationRecord | None = None
    digest: str | None = None
    expires_in_seconds: float | None = None
    warn: bool = False

    @property
    def valid_for_apply(self) -> bool:
        """Only a ``valid`` lease opens apply; everything else fails closed."""
        return self.status == "valid"

    def payload(self) -> dict[str, Any]:
        """Nonsecret machine-readable view (record fields plus verdict metadata)."""
        return {
            "status": self.status,
            "reason": self.reason,
            "digest": self.digest,
            "expires_in_seconds": self.expires_in_seconds,
            "warn_before_expiry": self.warn,
            "record": record_payload(self.record),
        }


def canonical_record(record: AttestationRecord) -> dict[str, Any]:
    """The canonical (sorted) mapping the digest is computed over."""
    return {
        "endpoint": record.endpoint,
        "version": record.version,
        "mount_fingerprint": list(record.mount_fingerprint),
        "settings_summary": dict(record.settings_summary),
        "inspected_iso": record.inspected_iso,
        "expires_iso": record.expires_iso,
        "scope": record.scope,
        "operator_note": record.operator_note,
    }


def record_digest(record: AttestationRecord) -> str:
    """Canonical SHA-256 of the record (integrity evidence, HMAC-free)."""
    canonical = json.dumps(canonical_record(record), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def record_payload(record: AttestationRecord | None) -> dict[str, Any] | None:
    return None if record is None else canonical_record(record)


def parse_iso(raw: str) -> datetime:
    """Parse an ISO-8601 timestamp; naive values are declared UTC (determinism)."""
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def validate_scope(scope: str) -> str:
    """Accept ``daemon`` or ``campaign:<id>`` (non-empty id); anything else refuses."""
    if not isinstance(scope, str):
        raise AttestationError("scope: must be 'daemon' or 'campaign:<id>'")
    candidate = scope.strip()
    if candidate == SCOPE_DAEMON:
        return candidate
    if candidate.startswith("campaign:"):
        identifier = candidate.removeprefix("campaign:")
        if not identifier.strip():
            raise AttestationError("scope: 'campaign:' requires a non-empty campaign id")
        return f"campaign:{identifier.strip()}"
    raise AttestationError(f"scope: must be 'daemon' or 'campaign:<id>' (got {scope!r})")


def build_record(
    *,
    endpoint: str,
    version: str,
    mounts: Sequence[str],
    settings_flags: Mapping[str, bool],
    inspected_iso: str,
    expires_iso: str,
    scope: str,
    operator_note: str,
) -> AttestationRecord:
    """Validate one operator-supplied lease record (the ``attestation record`` path)."""
    normalized_endpoint = normalize_service_url(endpoint)
    if not isinstance(version, str) or not version.strip():
        raise AttestationError("version: must be a non-empty string (the inspected Shoko version)")
    mounts_clean = tuple(item.strip() for item in mounts if item.strip())
    if not mounts_clean:
        raise AttestationError(
            "mounts: at least one inspected-mount summary entry is required "
            "(the operator must record what was physically inspected)"
        )
    if len(mounts_clean) > MAX_MOUNT_ITEMS:
        raise AttestationError(f"mounts: at most {MAX_MOUNT_ITEMS} entries are allowed")
    if any(len(item) > MAX_MOUNT_ITEM_CHARS for item in mounts_clean):
        raise AttestationError(
            f"mounts: each entry must be at most {MAX_MOUNT_ITEM_CHARS} characters"
        )
    flags = dict(settings_flags)
    if set(flags) != SAFE_SETTINGS_FLAGS:
        missing = sorted(SAFE_SETTINGS_FLAGS - set(flags))
        unknown = sorted(set(flags) - SAFE_SETTINGS_FLAGS)
        parts: list[str] = []
        if missing:
            parts.append(f"missing {', '.join(missing)}")
        if unknown:
            parts.append(f"unknown {', '.join(unknown)}")
        raise AttestationError(
            "settings: the recorded flags must be exactly "
            f"{', '.join(sorted(SAFE_SETTINGS_FLAGS))} ({'; '.join(parts)})"
        )
    if any(not isinstance(value, bool) for value in flags.values()):
        raise AttestationError("settings: every flag value must be true or false")
    note = operator_note.strip()
    if len(note) < MIN_NOTE_CHARS:
        raise AttestationError("note: a non-empty operator note is required")
    if len(note) > MAX_NOTE_CHARS:
        raise AttestationError(f"note: at most {MAX_NOTE_CHARS} characters")
    inspected = parse_iso(inspected_iso)
    expires = parse_iso(expires_iso)
    if expires <= inspected:
        raise AttestationError("expires: must be strictly after the inspection time")
    return AttestationRecord(
        endpoint=normalized_endpoint,
        version=version.strip(),
        mount_fingerprint=mounts_clean,
        settings_summary=MappingProxyType(dict(sorted(flags.items()))),
        inspected_iso=inspected.isoformat(),
        expires_iso=expires.isoformat(),
        scope=validate_scope(scope),
        operator_note=note,
    )


def write_attestation(
    path: str | Path, record: AttestationRecord, *, overwrite: bool = False
) -> tuple[Path, str, str | None]:
    """Write the lease atomically (temp + replace) and attempt 0600; returns the digest.

    The second element is the canonical SHA-256 of the record; the third is the
    permission-enforcement warning (``None`` when 0600 applied). Refuses to overwrite
    an existing lease without ``overwrite`` — an active lease is replaced only after
    ``attestation invalidate`` (audit trail first).
    """
    target = Path(path)
    if target.exists() and not overwrite:
        raise AttestationError(
            f"attestation file {target} already exists: run 'attestation invalidate' first "
            "(the audit trail is moved aside, never silently replaced)"
        )
    canonical = canonical_record(record)
    text = yaml.safe_dump(canonical, sort_keys=True, allow_unicode=True)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.parent / f"{target.name}.new"
    try:
        temp.write_text(text, encoding="utf-8")
        warning: str | None = None
        try:
            temp.chmod(0o600)
        except OSError as exc:
            warning = (
                f"cannot enforce 0600 on this platform ({exc.strerror or exc}); the lease is "
                "operator-readable-only by convention (see README)"
            )
        temp.replace(target)
    finally:
        if temp.exists():
            temp.unlink()
    return target, record_digest(record), warning


def _require_iso(value: Any, name: str) -> datetime:
    if not isinstance(value, str):
        raise AttestationError(f"{name}: must be an ISO-8601 timestamp string")
    try:
        return parse_iso(value)
    except ValueError as exc:
        raise AttestationError(f"{name}: not a valid ISO-8601 timestamp: {exc}") from exc


def _document_mapping(text: str) -> dict[str, Any]:
    """The lease document as a string-keyed mapping with an exact field set."""
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise AttestationError(f"attestation file is not valid YAML/JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise AttestationError("attestation file: expected a mapping of record fields")
    document = cast("dict[str, Any]", raw)
    if any(not isinstance(key, str) for key in document):
        raise AttestationError("attestation file: setting names must be strings")
    unknown = sorted(set(document) - RECORD_FIELDS)
    if unknown:
        raise AttestationError(f"attestation file: unknown field(s) {', '.join(unknown)}")
    missing = sorted(RECORD_FIELDS - set(document))
    if missing:
        raise AttestationError(f"attestation file: missing field(s) {', '.join(missing)}")
    return document


def _parse_endpoint(raw: Any) -> str:
    if not isinstance(raw, str):
        raise AttestationError("endpoint: must be a URL string")
    try:
        return normalize_service_url(raw)
    except ConfigError as exc:
        raise AttestationError(f"endpoint: {exc}") from exc


def _parse_version(raw: Any) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise AttestationError("version: must be a non-empty string")
    return raw.strip()


def _parse_mounts(raw: Any) -> tuple[str, ...]:
    if not isinstance(raw, list) or not raw:
        raise AttestationError("mount_fingerprint: must be a non-empty list of summary strings")
    mounts: list[str] = []
    for item in raw:
        if not isinstance(item, str) or not item.strip():
            raise AttestationError("mount_fingerprint: every entry must be a non-empty string")
        mounts.append(item.strip())
    if len(mounts) > MAX_MOUNT_ITEMS:
        raise AttestationError(f"mount_fingerprint: at most {MAX_MOUNT_ITEMS} entries")
    return tuple(mounts)


def _parse_settings_summary(raw: Any) -> Mapping[str, bool]:
    if not isinstance(raw, dict):
        raise AttestationError(
            "settings_summary: must be a mapping of the recorded safety flags (never a raw "
            "settings dump)"
        )
    document = cast("dict[str, Any]", raw)
    if set(document) != SAFE_SETTINGS_FLAGS:
        raise AttestationError(
            "settings_summary: keys must be exactly " + ", ".join(sorted(SAFE_SETTINGS_FLAGS))
        )
    if any(not isinstance(value, bool) for value in document.values()):
        raise AttestationError("settings_summary: every flag value must be true or false")
    return MappingProxyType(dict(sorted(document.items())))


def _parse_time_window(inspected_raw: Any, expires_raw: Any) -> tuple[str, str]:
    inspected = _require_iso(inspected_raw, "inspected_iso")
    expires = _require_iso(expires_raw, "expires_iso")
    if expires <= inspected:
        raise AttestationError("expires_iso: must be strictly after inspected_iso")
    return inspected.isoformat(), expires.isoformat()


def _parse_scope(raw: Any) -> str:
    if not isinstance(raw, str):
        raise AttestationError("scope: must be 'daemon' or 'campaign:<id>'")
    try:
        return validate_scope(raw)
    except AttestationError as exc:
        raise AttestationError(f"scope: {exc}") from exc


def _parse_note(raw: Any) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise AttestationError("operator_note: must be a non-empty string")
    note = raw.strip()
    if len(note) > MAX_NOTE_CHARS:
        raise AttestationError(f"operator_note: at most {MAX_NOTE_CHARS} characters")
    return note


def parse_attestation_document(text: str) -> AttestationRecord:
    """Parse lease YAML/JSON text; every defect raises :class:`AttestationError` (fail closed)."""
    document = _document_mapping(text)
    inspected_iso, expires_iso = _parse_time_window(
        document["inspected_iso"], document["expires_iso"]
    )
    return AttestationRecord(
        endpoint=_parse_endpoint(document["endpoint"]),
        version=_parse_version(document["version"]),
        mount_fingerprint=_parse_mounts(document["mount_fingerprint"]),
        settings_summary=_parse_settings_summary(document["settings_summary"]),
        inspected_iso=inspected_iso,
        expires_iso=expires_iso,
        scope=_parse_scope(document["scope"]),
        operator_note=_parse_note(document["operator_note"]),
    )


def _read_lease(target: Path) -> AttestationVerdict | AttestationRecord:
    """Read and parse one lease file: a verdict on any read/parse failure."""
    try:
        text = target.read_text(encoding="utf-8")
    except FileNotFoundError:
        return AttestationVerdict("absent", f"attestation file {target} does not exist")
    except UnicodeDecodeError as exc:
        return AttestationVerdict(
            "invalid",
            f"attestation file {target} is not valid UTF-8 text (at byte offset {exc.start})",
        )
    except OSError as exc:
        return AttestationVerdict(
            "invalid", f"attestation file {target} cannot be read: {exc.strerror or exc}"
        )
    try:
        return parse_attestation_document(text)
    except AttestationError as exc:
        return AttestationVerdict("invalid", str(exc))


def _lease_mismatches(
    record: AttestationRecord,
    *,
    shoko_url: str,
    scope: str,
    version_hint: str | None,
) -> AttestationVerdict | None:
    """Scope/endpoint/version refusals for one parsed lease; ``None`` means no mismatch."""
    digest = record_digest(record)
    try:
        expected_scope = validate_scope(scope)
    except AttestationError as exc:
        return AttestationVerdict("invalid", f"requested scope is unusable: {exc}")
    if record.scope != expected_scope:
        return AttestationVerdict(
            "invalid",
            f"scope mismatch: lease is scoped to {record.scope!r}, this apply needs "
            f"{expected_scope!r}",
            record=record,
            digest=digest,
        )
    try:
        configured = normalize_service_url(shoko_url)
    except ConfigError as exc:
        return AttestationVerdict("invalid", f"configured Shoko URL is unusable: {exc}")
    if record.endpoint != configured:
        return AttestationVerdict(
            "invalid",
            f"endpoint mismatch: lease attests {record.endpoint!r}, this config targets "
            f"{configured!r} (attest the endpoint you write to)",
            record=record,
            digest=digest,
        )
    if version_hint is not None and record.version != version_hint.strip():
        return AttestationVerdict(
            "stale_version",
            f"recorded version {record.version!r} does not match the configured version hint "
            f"{version_hint.strip()!r}: re-inspect and re-record the lease (treated as invalid "
            "for apply)",
            record=record,
            digest=digest,
            warn=True,
        )
    return None


def load_attestation(
    path: str | Path | None,
    *,
    shoko_url: str,
    now_iso: str,
    scope: str,
    version_hint: str | None = None,
    warn_lead_seconds: float = 3600.0,
) -> AttestationVerdict:
    """Evaluate one lease offline against the caller's expectations. Makes no network call.

    ``absent`` when no lease is configured/exists; ``invalid`` on malformed content,
    an endpoint identity mismatch with the configured Shoko URL, or a scope mismatch;
    ``expired`` when ``expires_iso`` is at/before ``now_iso``; ``stale_version`` when a
    supplied ``version_hint`` differs from the recorded version (a warning, treated as
    invalid for apply — fail closed). ``warn_lead_seconds`` marks a *valid* lease that
    expires within the lead window (``warn=True``) — apply still proceeds, the operator
    is warned.
    """
    if path is None:
        return AttestationVerdict("absent", "safety.attestation_file is not configured")
    loaded = _read_lease(Path(path))
    if isinstance(loaded, AttestationVerdict):
        return loaded
    record = loaded
    mismatch = _lease_mismatches(
        record, shoko_url=shoko_url, scope=scope, version_hint=version_hint
    )
    if mismatch is not None:
        return mismatch
    digest = record_digest(record)
    now = parse_iso(now_iso)
    remaining = (parse_iso(record.expires_iso) - now).total_seconds()
    if remaining <= 0:
        return AttestationVerdict(
            "expired",
            f"lease expired at {record.expires_iso} (now {now.isoformat()})",
            record=record,
            digest=digest,
            expires_in_seconds=remaining,
        )
    warn = remaining <= warn_lead_seconds
    return AttestationVerdict(
        "valid",
        "valid" if not warn else f"lease expires in {remaining:.0f}s (warn lead)",
        record=record,
        digest=digest,
        expires_in_seconds=remaining,
        warn=warn,
    )


def lease_expiry_cap(hours: float, validity_cap_hours: float) -> tuple[bool, str]:
    """``attestation record --hours`` cap: leases never outlive the config cap."""
    if hours <= 0:
        return False, "hours must be greater than 0"
    if hours > validity_cap_hours:
        return (
            False,
            f"hours {hours:g} exceeds safety.attestation_validity_hours "
            f"({validity_cap_hours:g}) — the lease lifetime cap",
        )
    return True, ""


__all__ = [
    "RECORD_FIELDS",
    "SAFE_SETTINGS_FLAGS",
    "SCOPE_DAEMON",
    "AttestationError",
    "AttestationRecord",
    "AttestationStatus",
    "AttestationVerdict",
    "build_record",
    "canonical_record",
    "lease_expiry_cap",
    "load_attestation",
    "parse_attestation_document",
    "parse_iso",
    "record_digest",
    "record_payload",
    "validate_scope",
    "write_attestation",
]
