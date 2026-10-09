"""M6-a: durable SQLite state machine — persistence, transitions, budgets, lock.

This module is the storage foundation of the M6 daemon (the polling loop,
reconciliation cadence and HTTP calls are M6-b; campaign binding/CLI is M7).
No network I/O happens here: everything is local, transactional SQLite.

Layout: one SQLite file at ``settings.database.path`` opened through
:func:`open_state`. Durability: ``journal_mode=WAL``, ``synchronous=FULL``,
``foreign_keys=ON``, ``busy_timeout``. Schema is versioned via ``PRAGMA
user_version`` plus the ``schema_version`` table; :meth:`StateStore.migrate` is
forward-only.

**Deployment safety (documented, not closable in code):** SQLite file locking is
unreliable on SMB/NFS mounts — never place the state file on a network share.
On Unraid, use a local path or a cache-backed mount. The apply lock below
coordinates a *single process installation*; SQLite file locks cannot coordinate
separate installations pointed at the same Shoko server (deployment-level risk,
EXTERNAL — see docs/compatibility.md §4 and PROJECT.md M6).

Durability contract for the mutation path (M6-b enforces the call order):

1. :meth:`StateStore.prepare_send` commits the ``prepared_not_sent`` row **and**
   the budget slot in one transaction *before* the caller hands bytes to HTTP.
2. :meth:`StateStore.mark_sent` durably advances to ``request_may_have_been_sent``
   (committed by the time it returns) — the caller must invoke it *before* the POST.
3. :meth:`StateStore.record_send_outcome` records the intent result;
   :meth:`StateStore.record_verification` closes the loop.

A ``request_may_have_been_sent`` row is never resubmitted: recovery classifies
it (and ``indeterminate_write``) as verify/hold — see
:meth:`StateStore.recover_after_restart`. On ``sqlite3.DatabaseError`` (corruption,
disk full, commit failure) writes stop: an in-memory flag plus a persisted meta
marker make every mutating API refuse; there is **no in-memory submission
fallback**.

Budget-slot release ownership (M6 review P1-2/P2-4, second-round P1-A): the reservation
is released **exactly once** — the send row carries a ``slot_released`` flag that is
set atomically in the same transaction as the counter decrement, so *every* release
path (verified, partial-close, void, rejected/refused, and the operator ``resolve_send``)
funnels through :meth:`StateStore._release_send_slot_tx`. A release attempt on a row whose
``slot_released=1`` is a **no-op** (recorded in the ``slot_release_noops`` meta audit list).
:func:`max` clamping in :meth:`StateStore._adjust_pending_tx` remains only as a defensive
backstop — the invariants must (and their tests do) hold **without** relying on it: a
PARTIAL close that releases the held slot, then a late ``ManualLinkJob`` that lands the
VERIFIED verdict, must **not** release the same reservation a second time. The APIs that
observe the terminal facts are :meth:`StateStore.record_verification` (VERIFIED),
:meth:`StateStore.record_send_outcome` (REJECTED/REFUSED, pinned zero-write),
:meth:`StateStore.void_prepared_send` (definite not-sent),
:meth:`StateStore.release_send_slot` (daemon partial-close) and
:meth:`StateStore.resolve_send` (operator terminal review). :meth:`StateStore.mark_completed`
only records cycle completion counters — it never releases pending.

Source continuity (M6 review P1-1): continuity is **identity/roots stability plus snapshot
discontinuity** (identity change, roots-set change, discovery-total drop, missing snapshot
marker). Ordinary inventory growth moves the snapshot summary hash every cycle and is NORMAL:
the observation is re-anchored each healthy cycle, and ``requires_rebaseline`` is set only by
the discontinuity triggers, cleared by the next clean full observation (daemon) or by
:meth:`StateStore.clear_requires_rebaseline` (operator/M7).

Schema v1 is unshipped (M6-a/M6-b, no released image): the ``sends`` table carries
``episode_jobs`` (the reserved count to release) and ``target_ids_json``
(the expected verification triples ``(anidb_anime_id, anidb_episode_id, shoko_episode_id)``
for re-poll/re-inspection of persisted rows), and ``budgets`` carries
``completed_submissions``; these columns were added in-schema, no migration is needed.
The second-round P1-A fix adds ``slot_released`` to ``sends`` — **also schema v1, unshipped**
(no released image reads the old column set, so it is appended to ``_SCHEMA_SQL`` with no
migration entry).

**Operator-only indirect chain (P3-B):** the transition graph contains the path
``indeterminate_write → waiting → planned → prepared_not_sent → request_may_have_been_sent``,
so a held row can reach a re-submittable state **only via the operator's explicit
``waiting``/``planned`` decisions**. The daemon **never initiates** that chain from
``indeterminate_write``/``conflict``/``partial`` — those rows are verified/held, and the only
daemon-side release is the past-deadline partial close. Closing a live-held terminal row is an
operator action (``resolve_send``, M7 CLI). **M7's CLI must not expose a "reset to planned"
for a sent row**: a ``request_may_have_been_sent``/``indeterminate_write`` row is never
rewound to ``planned`` directly, and resolving a sent row keeps the *same* request
fingerprint refused while the row is live; re-plan is allowed only as a **fresh**
``request_fingerprint`` (dedup covers active outcomes only).
"""

from __future__ import annotations

import contextlib
import functools
import json
import sqlite3
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Final, Literal, ParamSpec, TypeVar, cast

from .errors import LinkerError
from .linking import SendOutcome, VerificationVerdict

SCHEMA_VERSION: Final[int] = 1
DEFAULT_BUSY_TIMEOUT_MS: Final[int] = 5000
GLOBAL_BUDGET_KEY: Final[str] = "_global"
MAX_DETAIL_CHARS: Final[int] = 1000
DIGEST_HEX_CHARS: Final[int] = 64
TARGET_TRIPLE_LEN: Final[int] = 3

# meta keys (documented contract for M6-b/M7; the scheduler fills the
# cycle/timestamp keys when the daemon loop lands).
META_APPLY_LOCK: Final[str] = "apply_lock"
META_WRITES_DISABLED: Final[str] = "writes_disabled"
META_WRITES_DISABLED_REASON: Final[str] = "writes_disabled_reason"
META_SOURCE_INSTANCE: Final[str] = "source_instance"
META_SONARR_STATUS_SNAPSHOT_ID: Final[str] = "sonarr_status_snapshot_id"
META_SOURCE_LAST_OBSERVED_ISO: Final[str] = "source_last_observed_iso"
META_SOURCE_ROOTS_FINGERPRINT: Final[str] = "source_roots_fingerprint"
META_SOURCE_LAST_DISCOVERY_TOTAL: Final[str] = "source_last_discovery_total"
META_REQUIRES_REBASELINE: Final[str] = "requires_rebaseline"
META_APPLY_LOCK_STEALS: Final[str] = "apply_lock_steals"
META_LAST_REFUSAL: Final[str] = "last_refusal"
META_SLOT_RELEASE_NOOPS: Final[str] = "slot_release_noops"
META_BASELINE_COMPLETED_ISO: Final[str] = "baseline_completed_iso"
META_BUDGET_CYCLE_KEY: Final[str] = "budget_last_cycle_key"
META_LAST_CYCLE_START_ISO: Final[str] = "last_cycle_start_iso"
META_LAST_CYCLE_END_ISO: Final[str] = "last_cycle_end_iso"
META_NEXT_CYCLE_DUE_ISO: Final[str] = "next_cycle_due_iso"
META_LAST_FULL_RECONCILE_ISO: Final[str] = "last_full_reconcile_iso"
META_NEXT_FULL_RECONCILE_ISO: Final[str] = "next_full_reconcile_iso"

_SYNC_NAMES: Final[Mapping[int, str]] = {0: "off", 1: "normal", 2: "full", 3: "extra"}

OUTCOME_PREPARED: Final[str] = "prepared"
OUTCOME_ATTEMPTED: Final[str] = "attempted"
OUTCOME_VOIDED: Final[str] = "voided"
OUTCOME_VERIFIED: Final[str] = "verified"
OUTCOME_RESOLVED: Final[str] = "resolved"
ACTIVE_SEND_OUTCOMES: Final[frozenset[str]] = frozenset(
    {OUTCOME_PREPARED, OUTCOME_ATTEMPTED, SendOutcome.ACCEPTED_UNVERIFIED.value}
)
# Outcomes that represent a send whose delivery state must be inspected on
# recovery (``ambiguous_send`` included: its slot is held until verification).
LIVE_SEND_OUTCOMES: Final[frozenset[str]] = ACTIVE_SEND_OUTCOMES | {
    SendOutcome.AMBIGUOUS_SEND.value
}
# Terminal/closed outcomes: never live, never resubmitted, never re-releasable.
CLOSED_SEND_OUTCOMES: Final[frozenset[str]] = frozenset(
    {OUTCOME_VOIDED, OUTCOME_VERIFIED, OUTCOME_RESOLVED}
)
# Resolve-gate (round-2 review F2): only terminal-review delivery states are operator-closable.
RESOLVE_LEGAL_OUTCOMES: Final[frozenset[str]] = frozenset(
    {SendOutcome.ACCEPTED_UNVERIFIED.value, SendOutcome.AMBIGUOUS_SEND.value}
)

_RECOVERY_HOLD_INSTRUCTION: Final[str] = (
    "possible delivery: verify with GET File/{id}?include=XRefs (M5 verify_link) and "
    "Shoko's queue; HOLD — never resubmit automatically"
)
_RECOVERY_REPLAN_INSTRUCTION: Final[str] = (
    "definitely not sent: replan with fresh checks (release the slot, re-prepare)"
)


class StateError(LinkerError):
    """The state store was asked something it must refuse (bad key, bad fingerprint)."""


class TransitionError(StateError):
    """A state transition is not on the allowed edge list, or the row is missing."""


class WriteDisabledError(StateError):
    """Writes are stopped (corruption/disk-full/commit failure); reads remain allowed."""


class LockHeldError(StateError):
    """The apply lock is held; the holder is attached for reporting."""

    def __init__(self, message: str, holder: LockHolder | None) -> None:
        super().__init__(message)
        self.holder = holder


class FileState(Enum):
    """The PLAN M6 state machine: the walk plus side states."""

    DISCOVERED = "discovered"
    MATCHED = "matched"
    MAPPED = "mapped"
    PLANNED = "planned"
    PREPARED_NOT_SENT = "prepared_not_sent"
    REQUEST_MAY_HAVE_BEEN_SENT = "request_may_have_been_sent"
    VERIFYING = "verifying"
    VERIFIED = "verified"
    WAITING = "waiting"
    AMBIGUOUS = "ambiguous"
    CONFLICT = "conflict"
    PARTIAL = "partial"
    INDETERMINATE_WRITE = "indeterminate_write"
    BLOCKED_SAFETY = "blocked_safety"


class Cohort(Enum):
    """Cohort membership: backlog, new_arrival, campaign (M2/M6/M7)."""

    BACKLOG = "backlog"
    NEW_ARRIVAL = "new_arrival"
    CAMPAIGN = "campaign"


_SEND_OUTCOME_STATE: Final[Mapping[SendOutcome, FileState]] = {
    SendOutcome.ACCEPTED_UNVERIFIED: FileState.VERIFYING,
    SendOutcome.REJECTED: FileState.WAITING,
    SendOutcome.AMBIGUOUS_SEND: FileState.INDETERMINATE_WRITE,
    SendOutcome.REFUSED: FileState.WAITING,
}

_VERIFICATION_STATE: Final[Mapping[VerificationVerdict, FileState | None]] = {
    VerificationVerdict.VERIFIED: FileState.VERIFIED,
    VerificationVerdict.PARTIAL: FileState.PARTIAL,
    VerificationVerdict.CONTRADICTION: FileState.CONFLICT,
    VerificationVerdict.UNKNOWN: None,
    VerificationVerdict.PENDING: None,
}


class ReasonClass(Enum):
    """Reason taxonomy: permanent (operator action) vs retryable (daemon may re-enter)."""

    PERMANENT = "permanent"
    RETRYABLE = "retryable"


PERMANENT_REASONS: Final[frozenset[str]] = frozenset(
    {
        "mapping_conflict",
        "source_identity_mismatch",
        "protected_link",
        "policy_denied",
        "invalid_identity",
        "file_gone",
    }
)
RETRYABLE_REASONS: Final[frozenset[str]] = frozenset(
    {
        "pending_metadata",
        "transport_failure",
        "queue_blocked",
        "grace_period",
        "budget_exhausted",
        "attestation_expired",
        "mapping_refresh_pending",
        "replan",
    }
)


def classify_reason(code: str) -> ReasonClass:
    """Taxonomy for a recorded reason code; unknown codes fail closed as permanent."""
    if code in RETRYABLE_REASONS:
        return ReasonClass.RETRYABLE
    return ReasonClass.PERMANENT


TRANSITIONS: Final[Mapping[FileState, frozenset[FileState]]] = {
    FileState.DISCOVERED: frozenset(
        {
            FileState.MATCHED,
            FileState.WAITING,
            FileState.AMBIGUOUS,
            FileState.CONFLICT,
            FileState.BLOCKED_SAFETY,
        }
    ),
    FileState.MATCHED: frozenset(
        {
            FileState.MAPPED,
            FileState.WAITING,
            FileState.AMBIGUOUS,
            FileState.CONFLICT,
            FileState.BLOCKED_SAFETY,
        }
    ),
    FileState.MAPPED: frozenset(
        {
            FileState.PLANNED,
            FileState.WAITING,
            FileState.AMBIGUOUS,
            FileState.CONFLICT,
            FileState.BLOCKED_SAFETY,
        }
    ),
    FileState.PLANNED: frozenset(
        {
            FileState.PREPARED_NOT_SENT,
            FileState.WAITING,
            FileState.CONFLICT,
            FileState.BLOCKED_SAFETY,
        }
    ),
    # prepared_not_sent may be re-planned (definitely not sent) — the
    # only way forward to transport is REQUEST_MAY_HAVE_BEEN_SENT via mark_sent.
    FileState.PREPARED_NOT_SENT: frozenset(
        {
            FileState.PLANNED,
            FileState.REQUEST_MAY_HAVE_BEEN_SENT,
            FileState.WAITING,
            FileState.BLOCKED_SAFETY,
        }
    ),
    # No edge from INDETERMINATE_WRITE to REQUEST_MAY_HAVE_BEEN_SENT exists:
    # a possibly-sent file is verified/held, never resubmitted.
    FileState.REQUEST_MAY_HAVE_BEEN_SENT: frozenset(
        {
            FileState.VERIFYING,
            FileState.INDETERMINATE_WRITE,
            FileState.PARTIAL,
            FileState.WAITING,
            FileState.CONFLICT,
            FileState.BLOCKED_SAFETY,
        }
    ),
    FileState.VERIFYING: frozenset(
        {
            FileState.VERIFIED,
            FileState.PARTIAL,
            FileState.INDETERMINATE_WRITE,
            FileState.WAITING,
            FileState.CONFLICT,
            FileState.BLOCKED_SAFETY,
        }
    ),
    FileState.VERIFIED: frozenset(),  # terminal: audit-only
    FileState.WAITING: frozenset(
        {
            FileState.DISCOVERED,
            FileState.MATCHED,
            FileState.MAPPED,
            FileState.PLANNED,
            FileState.AMBIGUOUS,
            FileState.CONFLICT,
            FileState.BLOCKED_SAFETY,
        }
    ),
    # Documented re-evaluation edges (M6 review P2-3): an ``ambiguous`` file whose mapping
    # inputs changed re-walks the evaluation path (discovered/matched/mapped) when the
    # daemon re-evaluates it — an edge into ``planned`` remains illegal (no skipping).
    FileState.AMBIGUOUS: frozenset(
        {
            FileState.DISCOVERED,
            FileState.MATCHED,
            FileState.MAPPED,
            FileState.WAITING,
            FileState.CONFLICT,
            FileState.BLOCKED_SAFETY,
        }
    ),
    FileState.CONFLICT: frozenset({FileState.WAITING, FileState.BLOCKED_SAFETY}),
    FileState.PARTIAL: frozenset(
        {
            FileState.VERIFYING,
            FileState.INDETERMINATE_WRITE,
            FileState.CONFLICT,
            FileState.WAITING,
            FileState.BLOCKED_SAFETY,
        }
    ),
    FileState.INDETERMINATE_WRITE: frozenset(
        {FileState.VERIFYING, FileState.CONFLICT, FileState.WAITING, FileState.BLOCKED_SAFETY}
    ),
    FileState.BLOCKED_SAFETY: frozenset({FileState.WAITING}),
}


def validate_transition(current: FileState, target: FileState) -> None:
    """Explicit-edge validation: no arbitrary jumps, ``verified`` is terminal."""
    if current is target:
        if current is FileState.VERIFIED:
            return  # idempotent audit no-op
        raise TransitionError(f"illegal transition {current.value} -> {target.value}: self-edge")
    allowed = TRANSITIONS[current]
    if target not in allowed:
        raise TransitionError(
            f"illegal transition {current.value} -> {target.value}: allowed edges are "
            f"{sorted(state.value for state in allowed) or 'none (terminal)'}"
        )


# ------------------------------------------------------------------ records


@dataclass(frozen=True)
class FileIdentitySnapshot:
    """ed2k/sha1/md5/crc32 + canonical Shoko paths snapshot (persisted JSON)."""

    ed2k: str | int | None = None
    sha1: str | int | None = None
    md5: str | int | None = None
    crc32: str | int | None = None
    shoko_paths: tuple[str, ...] = ()


@dataclass(frozen=True)
class FileSeed:
    """One row for the bulk :meth:`StateStore.record_files` path."""

    shoko_file_id: int
    sonarr_series_id: int
    sonarr_file_id: int
    sonarr_path: str
    sonarr_size: int
    cohort: Cohort
    iso: str
    state: FileState = FileState.DISCOVERED
    identity: FileIdentitySnapshot | None = None


@dataclass(frozen=True)
class FileRecord:
    """The full tracked-file row, deterministically ordered by ``shoko_file_id``."""

    shoko_file_id: int
    sonarr_series_id: int
    sonarr_file_id: int
    sonarr_path: str
    sonarr_size: int
    state: FileState
    cohort: Cohort
    plan_fingerprint: str | None
    request_fingerprint: str | None
    mapping_targets_digest: str | None
    mapping_structure_digest: str | None
    mapping_revision: str | None
    policy: str | None
    campaign_id: int | None
    state_detail: str
    updated_iso: str
    first_seen_iso: str
    last_verified_iso: str | None
    identity_json: str | None

    @property
    def identity(self) -> FileIdentitySnapshot | None:
        """The stored identity snapshot (None when never recorded)."""
        return _parse_identity(self.identity_json)


@dataclass(frozen=True)
class BaselineFile:
    """One Sonarr-file member of a baseline cohort snapshot."""

    sonarr_series_id: int
    sonarr_file_id: int
    path: str
    size: int


@dataclass(frozen=True)
class Baseline:
    """One recorded baseline (cohort establishment)."""

    baseline_id: int
    created_iso: str
    complete: bool
    auto_apply_eligible: bool
    series_snapshot_hash: str
    reason: str
    file_count: int


@dataclass(frozen=True)
class BudgetLimits:
    """Caller-supplied apply ceilings (the daemon reads them from settings; M6-b/M7
    may add config keys — this slice adds none)."""

    max_pending_files: int
    max_pending_episode_jobs: int
    per_cycle_new: int
    rolling_new: int
    rolling_window_hours: float


@dataclass(frozen=True)
class SlotReservation:
    """Accepted budget reservation (``accepted`` discriminates against refusal)."""

    accepted: bool
    window_key: str
    cycle_new_submissions: int
    rolling_new_submissions: int
    pending_file_requests: int
    pending_episode_jobs: int


@dataclass(frozen=True)
class BudgetRefusal:
    """Refused budget reservation with a machine-readable reason."""

    accepted: bool
    reason: str
    detail: str


@dataclass(frozen=True)
class SendToken:
    """Handle for one durably committed send intent (prepared, slot held)."""

    accepted: bool
    send_id: int
    window_key: str
    shoko_file_id: int
    plan_fingerprint: str
    request_fingerprint: str


@dataclass(frozen=True)
class BudgetView:
    """Current persisted budget counters (window_key = last-used cycle)."""

    window_key: str
    cycle_new_submissions: int
    rolling_new_submissions: int
    rolling_window_start_iso: str | None
    pending_file_requests: int
    pending_episode_jobs: int
    paused_reason: str | None
    completed_submissions: int = 0


@dataclass(frozen=True)
class SendInspection:
    """One live send row for recovery: reserved job count + expected verification triples.

    ``attempted_iso`` is the immutable verification-deadline anchor set by
    :meth:`StateStore.mark_sent` (P1-B): a poll never slides it. ``slot_released``
    reports whether the reservation has already been released (P1-A)."""

    send_id: int
    request_fingerprint: str
    outcome: str
    episode_jobs: int
    target_ids: tuple[tuple[int, int, int], ...]
    attempted_iso: str
    slot_released: bool


@dataclass(frozen=True)
class RecoveryRow:
    """One persisted non-idle file classified for restart recovery."""

    shoko_file_id: int
    state: FileState
    cohort: Cohort
    plan_fingerprint: str | None
    request_fingerprint: str | None
    detail: str
    instruction: str


@dataclass(frozen=True)
class RecoveryPlan:
    """Restart classification: what to replan, hold, poll, re-evaluate, audit."""

    replannable: tuple[RecoveryRow, ...]
    hold_for_verification: tuple[RecoveryRow, ...]
    continue_polling: tuple[RecoveryRow, ...]
    re_evaluate: tuple[RecoveryRow, ...]
    blocked_safety: tuple[RecoveryRow, ...]
    audit_only: tuple[RecoveryRow, ...]

    @property
    def resubmit_forbidden(self) -> tuple[RecoveryRow, ...]:
        """Rows that must be verified/held — automatic resubmission is forbidden."""
        return self.hold_for_verification


@dataclass(frozen=True)
class EligibilityVerdict:
    """Composed auto-apply eligibility with every refusal reason recorded."""

    eligible: bool
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class ContinuityVerdict:
    """Source identity/snapshot continuity answer; drives the rebaseline requirement."""

    ok: bool
    reasons: tuple[str, ...]
    requires_rebaseline: bool


@dataclass(frozen=True)
class SourceObservation:
    """Structured continuity inputs for one cycle (identity/roots shape plus monotonic
    discovery totals and snapshot summary ids)."""

    instance_identity: str
    roots_fingerprint: str
    snapshot_summary_id: str
    discovery_total: int | None = None


@dataclass(frozen=True)
class LockHolder:
    """The apply-lock holder as recorded in meta."""

    owner_pid: int
    acquired_iso: str
    lease_expires_iso: str


@dataclass(frozen=True)
class LockAcquisition:
    """Successful acquisition of the apply lock."""

    acquired: bool
    holder: LockHolder


@dataclass(frozen=True)
class IntegrityReport:
    """Result of ``PRAGMA integrity_check`` (exposed for CLI/M7)."""

    ok: bool
    detail: str


# ----------------------------------------------------------------- schema


_SCHEMA_SQL: Final[str] = """
CREATE TABLE schema_version (
    version INTEGER NOT NULL,
    applied_iso TEXT NOT NULL,
    description TEXT NOT NULL
);
CREATE TABLE meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE baselines (
    baseline_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_iso TEXT NOT NULL,
    complete INTEGER NOT NULL CHECK (complete IN (0, 1)),
    auto_apply_eligible INTEGER NOT NULL CHECK (auto_apply_eligible IN (0, 1)),
    series_snapshot_hash TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL DEFAULT ''
);
CREATE TABLE baseline_files (
    baseline_id INTEGER NOT NULL REFERENCES baselines (baseline_id),
    sonarr_series_id INTEGER NOT NULL,
    sonarr_file_id INTEGER NOT NULL,
    path TEXT NOT NULL,
    size INTEGER NOT NULL,
    PRIMARY KEY (baseline_id, sonarr_series_id, sonarr_file_id)
);
CREATE TABLE files (
    shoko_file_id INTEGER PRIMARY KEY,
    sonarr_series_id INTEGER,
    sonarr_file_id INTEGER,
    sonarr_path TEXT NOT NULL DEFAULT '',
    sonarr_size INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL,
    cohort TEXT NOT NULL CHECK (cohort IN ('backlog', 'new_arrival', 'campaign')),
    plan_fingerprint TEXT,
    request_fingerprint TEXT,
    mapping_targets_digest TEXT,
    mapping_structure_digest TEXT,
    mapping_revision TEXT,
    policy TEXT,
    campaign_id INTEGER,
    state_detail TEXT NOT NULL DEFAULT '',
    updated_iso TEXT NOT NULL,
    first_seen_iso TEXT NOT NULL,
    last_verified_iso TEXT,
    identity_hash_json TEXT
);
CREATE TABLE sends (
    send_id INTEGER PRIMARY KEY AUTOINCREMENT,
    shoko_file_id INTEGER NOT NULL REFERENCES files (shoko_file_id),
    request_fingerprint TEXT NOT NULL,
    plan_fingerprint TEXT NOT NULL,
    attempted_iso TEXT NOT NULL,
    http_status INTEGER,
    outcome TEXT NOT NULL,
    verified_iso TEXT,
    verification TEXT,
    episode_jobs INTEGER NOT NULL DEFAULT 1,
    target_ids_json TEXT NOT NULL DEFAULT '[]',
    slot_released INTEGER NOT NULL DEFAULT 0 CHECK (slot_released IN (0, 1))
);
CREATE TABLE campaigns (
    campaign_id INTEGER PRIMARY KEY,
    digest TEXT NOT NULL,
    plan_fingerprint_set TEXT NOT NULL,
    max_submissions INTEGER NOT NULL,
    deadline_iso TEXT,
    attestation_expiry_iso TEXT,
    total_sent INTEGER NOT NULL DEFAULT 0,
    created_iso TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open'
);
CREATE TABLE budgets (
    window_key TEXT PRIMARY KEY,
    cycle_new_submissions INTEGER NOT NULL DEFAULT 0,
    rolling_new_submissions INTEGER,
    rolling_window_start_iso TEXT,
    pending_file_requests INTEGER,
    pending_episode_jobs INTEGER,
    paused_reason TEXT,
    completed_submissions INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX idx_files_state ON files (state);
CREATE INDEX idx_files_cohort ON files (cohort);
CREATE INDEX idx_files_campaign ON files (campaign_id);
CREATE INDEX idx_files_path ON files (sonarr_path);
CREATE INDEX idx_files_series_file ON files (sonarr_series_id, sonarr_file_id);
CREATE INDEX idx_sends_request_fingerprint ON sends (request_fingerprint);
CREATE INDEX idx_sends_file ON sends (shoko_file_id);
CREATE INDEX idx_baseline_files_identity ON baseline_files (sonarr_series_id, sonarr_file_id);
"""

# Forward-only migration registry: each entry upgrades ``version`` to ``version + 1``.
# v1 is the current schema; future slices append entries here and bump
# SCHEMA_VERSION. Migrations never destroy tables and never run backwards.
_MIGRATIONS: Final[Mapping[int, Callable[[sqlite3.Connection], None]]] = {}


def _parse_iso(raw: str) -> datetime:
    """Parse an ISO-8601 timestamp; naive values are declared UTC (determinism)."""
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _require_hex64(value: str, name: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != DIGEST_HEX_CHARS
        or not all(char in "0123456789abcdef" for char in value)
    ):
        raise StateError(f"{name}: must be a lowercase SHA-256 hex digest")


def _require_iso(value: str, name: str) -> None:
    try:
        _parse_iso(value)
    except ValueError as exc:
        raise StateError(f"{name}: not a valid ISO-8601 timestamp: {exc}") from exc


def _clip(detail: str) -> str:
    return detail[:MAX_DETAIL_CHARS]


def _encode_identity(snapshot: FileIdentitySnapshot | None) -> str | None:
    if snapshot is None:
        return None
    payload = {
        "ed2k": snapshot.ed2k,
        "sha1": snapshot.sha1,
        "md5": snapshot.md5,
        "crc32": snapshot.crc32,
        "paths": list(snapshot.shoko_paths),
    }
    return json.dumps(payload, sort_keys=True)


def _encode_target_ids(triples: Sequence[tuple[int, int, int]]) -> str:
    payload = [[int(anidb), int(episode), int(shoko)] for anidb, episode, shoko in triples]
    return json.dumps(payload)


def _parse_target_ids(raw: str | None) -> tuple[tuple[int, int, int], ...]:
    if raw is None:
        return ()
    try:
        document = json.loads(raw)
    except ValueError:
        return ()
    if not isinstance(document, list):
        return ()
    parsed: list[tuple[int, int, int]] = []
    for item in document:
        if (
            isinstance(item, list)
            and len(item) == TARGET_TRIPLE_LEN
            and all(not isinstance(value, bool) and isinstance(value, int) for value in item)
        ):
            parsed.append((item[0], item[1], item[2]))
    return tuple(parsed)


def _parse_identity(raw: str | None) -> FileIdentitySnapshot | None:
    if raw is None:
        return None
    try:
        document = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(document, dict):
        return None
    entry = cast("dict[str, Any]", document)
    paths = entry.get("paths")
    if not isinstance(paths, list):
        return None
    return FileIdentitySnapshot(
        ed2k=entry.get("ed2k"),
        sha1=entry.get("sha1"),
        md5=entry.get("md5"),
        crc32=entry.get("crc32"),
        shoko_paths=tuple(item for item in paths if isinstance(item, str)),
    )


def _as_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise StateError(f"{name}: expected a stored integer, got {value!r}")
    return int(value)


def _cycle_new(row: Any) -> int:
    return 0 if row is None else _as_int(row[0], "cycle_new_submissions")


def _cycle_completed(row: Any) -> int:
    return 0 if row is None else _as_int(row[1], "completed_submissions")


def _as_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return _as_int(value, "optional integer column")


def _as_str(value: Any, name: str) -> str:
    if not isinstance(value, str):
        raise StateError(f"{name}: expected a stored string, got {value!r}")
    return value


def _as_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    return _as_str(value, "optional string column")


P = ParamSpec("P")
T = TypeVar("T")


def _mutation(fn: Callable[P, T]) -> Callable[P, T]:
    """Guard: refuse when writes are stopped; any DatabaseError stops writes.

    There is deliberately **no in-memory fallback**: once the database is in
    trouble, every mutating API raises :class:`WriteDisabledError` (reads stay
    available) and the stop is persisted so a restart inherits it.
    """

    @functools.wraps(fn)
    def guard(*args: P.args, **kwargs: P.kwargs) -> T:
        store = cast("StateStore", args[0])
        if store.writes_disabled:
            raise WriteDisabledError(f"state writes are disabled: {store._writes_reason}")
        try:
            return fn(*args, **kwargs)
        except WriteDisabledError:
            raise
        except sqlite3.DatabaseError as exc:
            store.disable_writes(f"database failure: {exc}")
            raise WriteDisabledError(f"state writes stopped: {exc}") from exc

    return cast("Callable[P, T]", guard)


# Full literal statements (no runtime string assembly — parameters bind by position).
_SQL_GET_FILE: Final[str] = (
    "SELECT shoko_file_id, sonarr_series_id, sonarr_file_id, sonarr_path, sonarr_size,"
    " state, cohort, plan_fingerprint, request_fingerprint, mapping_targets_digest,"
    " mapping_structure_digest, mapping_revision, policy, campaign_id, state_detail,"
    " updated_iso, first_seen_iso, last_verified_iso, identity_hash_json"
    " FROM files WHERE shoko_file_id = ?"
)
_SQL_FIND_BY_PATH: Final[str] = (
    "SELECT shoko_file_id, sonarr_series_id, sonarr_file_id, sonarr_path, sonarr_size,"
    " state, cohort, plan_fingerprint, request_fingerprint, mapping_targets_digest,"
    " mapping_structure_digest, mapping_revision, policy, campaign_id, state_detail,"
    " updated_iso, first_seen_iso, last_verified_iso, identity_hash_json"
    " FROM files WHERE sonarr_path = ? ORDER BY shoko_file_id LIMIT 1"
)
_SQL_FILES_BY_STATE: Final[str] = (
    "SELECT shoko_file_id, sonarr_series_id, sonarr_file_id, sonarr_path, sonarr_size,"
    " state, cohort, plan_fingerprint, request_fingerprint, mapping_targets_digest,"
    " mapping_structure_digest, mapping_revision, policy, campaign_id, state_detail,"
    " updated_iso, first_seen_iso, last_verified_iso, identity_hash_json"
    " FROM files WHERE state = ? ORDER BY shoko_file_id"
)
_SQL_FILES_BY_COHORT: Final[str] = (
    "SELECT shoko_file_id, sonarr_series_id, sonarr_file_id, sonarr_path, sonarr_size,"
    " state, cohort, plan_fingerprint, request_fingerprint, mapping_targets_digest,"
    " mapping_structure_digest, mapping_revision, policy, campaign_id, state_detail,"
    " updated_iso, first_seen_iso, last_verified_iso, identity_hash_json"
    " FROM files WHERE cohort = ? ORDER BY shoko_file_id"
)
_SQL_ALL_FILES: Final[str] = (
    "SELECT shoko_file_id, sonarr_series_id, sonarr_file_id, sonarr_path, sonarr_size,"
    " state, cohort, plan_fingerprint, request_fingerprint, mapping_targets_digest,"
    " mapping_structure_digest, mapping_revision, policy, campaign_id, state_detail,"
    " updated_iso, first_seen_iso, last_verified_iso, identity_hash_json"
    " FROM files ORDER BY shoko_file_id"
)


def _row_to_file_record(row: Any) -> FileRecord:
    values = tuple(row)
    return FileRecord(
        shoko_file_id=_as_int(values[0], "shoko_file_id"),
        sonarr_series_id=_as_int(values[1], "sonarr_series_id"),
        sonarr_file_id=_as_int(values[2], "sonarr_file_id"),
        sonarr_path=_as_str(values[3], "sonarr_path"),
        sonarr_size=_as_int(values[4], "sonarr_size"),
        state=FileState(_as_str(values[5], "state")),
        cohort=Cohort(_as_str(values[6], "cohort")),
        plan_fingerprint=_as_optional_str(values[7]),
        request_fingerprint=_as_optional_str(values[8]),
        mapping_targets_digest=_as_optional_str(values[9]),
        mapping_structure_digest=_as_optional_str(values[10]),
        mapping_revision=_as_optional_str(values[11]),
        policy=_as_optional_str(values[12]),
        campaign_id=_as_optional_int(values[13]),
        state_detail=_as_str(values[14], "state_detail"),
        updated_iso=_as_str(values[15], "updated_iso"),
        first_seen_iso=_as_str(values[16], "first_seen_iso"),
        last_verified_iso=_as_optional_str(values[17]),
        identity_json=_as_optional_str(values[18]),
    )


# ------------------------------------------------------------------- store


class StateStore:
    """Durable state machine over one SQLite file. No network, no in-memory truths."""

    def __init__(self, path: str | Path, *, budget_limits: BudgetLimits | None = None) -> None:
        self.path = Path(path)
        self.budget_limits = budget_limits
        self._writes_disabled = False
        self._writes_reason = ""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.path))
        self._conn.isolation_level = None  # explicit transactions only
        self._conn.execute(f"PRAGMA busy_timeout={DEFAULT_BUSY_TIMEOUT_MS}")
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()
        if self.get_meta(META_WRITES_DISABLED) == "1":
            self._writes_disabled = True
            self._writes_reason = self.get_meta(META_WRITES_DISABLED_REASON) or "unspecified"

    # ------------------------------------------------------------- lifecycle

    def close(self) -> None:
        self._conn.close()

    @property
    def connection(self) -> sqlite3.Connection:
        """Raw connection for **read-only** external checks (e.g. integrity)."""
        return self._conn

    @property
    def writes_disabled(self) -> bool:
        return self._writes_disabled

    def journal_mode(self) -> str:
        row = self._conn.execute("PRAGMA journal_mode").fetchone()
        return _as_str(row[0], "journal_mode").lower() if row else ""

    def synchronous(self) -> str:
        row = self._conn.execute("PRAGMA synchronous").fetchone()
        return _SYNC_NAMES.get(int(row[0]), "unknown") if row else "unknown"

    def foreign_keys_enabled(self) -> bool:
        row = self._conn.execute("PRAGMA foreign_keys").fetchone()
        return bool(row and row[0] == 1)

    def schema_version(self) -> int:
        row = self._conn.execute("PRAGMA user_version").fetchone()
        return int(row[0]) if row else 0

    def migrate(self) -> int:
        """Forward-only migration entry point: applies registered steps, never
        destroys data, and refuses to open a schema newer than this build."""
        version = self.schema_version()
        if version > SCHEMA_VERSION:
            raise StateError(
                f"schema version {version} is newer than supported {SCHEMA_VERSION}; "
                "refusing to migrate backwards"
            )
        while version < SCHEMA_VERSION:
            step = _MIGRATIONS.get(version)
            if step is None:
                raise StateError(f"no forward migration registered from version {version}")
            with self._transaction():
                step(self._conn)
                version += 1
                self._conn.execute(f"PRAGMA user_version={version}")
                self._conn.execute(
                    "INSERT INTO schema_version (version, applied_iso, description)"
                    " VALUES (?, ?, ?)",
                    (version, datetime.now(UTC).isoformat(), f"migrated from {version - 1}"),
                )
        return version

    def integrity_report(self) -> IntegrityReport:
        return integrity_check(self)

    def _init_schema(self) -> None:
        version = self.schema_version()
        if version > SCHEMA_VERSION:
            raise sqlite3.DatabaseError(
                f"schema version {version} is newer than supported {SCHEMA_VERSION}"
            )
        if version == SCHEMA_VERSION:
            return
        with self._transaction():
            self._conn.executescript(_SCHEMA_SQL)
            self._conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
            self._conn.execute(
                "INSERT INTO schema_version (version, applied_iso, description)"
                " VALUES (?, ?, ?)",
                (SCHEMA_VERSION, datetime.now(UTC).isoformat(), "initial v1 schema"),
            )

    @contextlib.contextmanager
    def _transaction(self) -> Iterator[None]:
        """One explicit transaction: every multi-row mutation commits atomically."""
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            self._conn.rollback()
            raise
        self._conn.commit()

    # ------------------------------------------------------------- meta

    def get_meta(self, key: str) -> str | None:
        row = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return None if row is None else _as_str(row[0], "meta.value")

    @_mutation
    def set_meta(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?)"
            " ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    @_mutation
    def disable_writes(self, reason: str) -> None:
        """Stop writes (fail closed). Persistence is best-effort; the in-memory stop is
        authoritative — there is no in-memory submission tracking fallback."""
        self._writes_disabled = True
        self._writes_reason = _clip(reason)
        with contextlib.suppress(sqlite3.DatabaseError), self._transaction():
            self._conn.execute(
                "INSERT INTO meta (key, value) VALUES (?, '1')"
                " ON CONFLICT (key) DO UPDATE SET value = '1'",
                (META_WRITES_DISABLED,),
            )
            self._conn.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?)"
                " ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                (META_WRITES_DISABLED_REASON, self._writes_reason),
            )

    def clear_writes_disabled(self, iso: str) -> None:
        """Operator-sanctioned recovery after the storage problem is fixed.

        Not mutation-guarded by design: clearing the stop *is* the sanctioned way
        to resume writes once the operator confirms the volume/DB is healthy.
        """
        _require_iso(iso, "iso")
        self._writes_disabled = False
        self._writes_reason = ""
        try:
            with self._transaction():
                self._conn.execute("DELETE FROM meta WHERE key = ?", (META_WRITES_DISABLED,))
                self._conn.execute("DELETE FROM meta WHERE key = ?", (META_WRITES_DISABLED_REASON,))
        except sqlite3.DatabaseError as exc:
            self.disable_writes(f"clear failed, writes stay stopped: {exc}")
            raise WriteDisabledError(f"state writes stay stopped: {exc}") from exc

    def _set_meta_tx(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT INTO meta (key, value) VALUES (?, ?)"
            " ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    # ------------------------------------------------------------- files

    @_mutation
    def record_file(
        self,
        shoko_file_id: int,
        *,
        sonarr_series_id: int,
        sonarr_file_id: int,
        sonarr_path: str,
        sonarr_size: int,
        cohort: Cohort,
        iso: str,
        state: FileState = FileState.DISCOVERED,
        identity: FileIdentitySnapshot | None = None,
        policy: str | None = None,
        campaign_id: int | None = None,
    ) -> None:
        _require_iso(iso, "iso")
        params = (
            shoko_file_id,
            sonarr_series_id,
            sonarr_file_id,
            sonarr_path,
            sonarr_size,
            state.value,
            cohort.value,
            policy,
            campaign_id,
            iso,
            iso,
            _encode_identity(identity),
        )
        with self._transaction():
            self._conn.execute(
                "INSERT OR IGNORE INTO files (shoko_file_id, sonarr_series_id, sonarr_file_id,"
                " sonarr_path, sonarr_size, state, cohort, policy, campaign_id,"
                " updated_iso, first_seen_iso, identity_hash_json)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params,
            )
            self._conn.execute(
                "UPDATE files SET last_verified_iso = ? WHERE shoko_file_id = ?",
                (iso, shoko_file_id),
            )

    @_mutation
    def record_files(self, rows: Sequence[FileSeed]) -> int:
        """Bulk cohort ingest (7k-file backlog): one transaction, ``executemany``."""
        if not rows:
            return 0
        params = [
            (
                row.shoko_file_id,
                row.sonarr_series_id,
                row.sonarr_file_id,
                row.sonarr_path,
                row.sonarr_size,
                row.state.value,
                row.cohort.value,
                row.iso,
                row.iso,
                _encode_identity(row.identity),
            )
            for row in rows
        ]
        with self._transaction():
            before = self._conn.total_changes
            self._conn.executemany(
                "INSERT OR IGNORE INTO files (shoko_file_id, sonarr_series_id,"
                " sonarr_file_id, sonarr_path, sonarr_size, state, cohort,"
                " updated_iso, first_seen_iso, identity_hash_json)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params,
            )
            return self._conn.total_changes - before

    def get_file(self, shoko_file_id: int) -> FileRecord | None:
        row = self._conn.execute(_SQL_GET_FILE, (shoko_file_id,)).fetchone()
        return None if row is None else _row_to_file_record(row)

    def find_file_by_path(self, sonarr_path: str) -> FileRecord | None:
        """Indexed by ``idx_files_path``."""
        row = self._conn.execute(_SQL_FIND_BY_PATH, (sonarr_path,)).fetchone()
        return None if row is None else _row_to_file_record(row)

    def files_in_state(self, state: FileState) -> tuple[FileRecord, ...]:
        """Indexed by ``idx_files_state``; deterministic order by id."""
        rows = self._conn.execute(_SQL_FILES_BY_STATE, (state.value,)).fetchall()
        return tuple(_row_to_file_record(row) for row in rows)

    def files_in_cohort(self, cohort: Cohort) -> tuple[FileRecord, ...]:
        """Indexed by ``idx_files_cohort``; deterministic order by id."""
        rows = self._conn.execute(_SQL_FILES_BY_COHORT, (cohort.value,)).fetchall()
        return tuple(_row_to_file_record(row) for row in rows)

    @_mutation
    def advance(self, shoko_file_id: int, new_state: FileState, detail: str, *, iso: str) -> None:
        """Apply one validated transition; illegal edges raise :class:`TransitionError`."""
        _require_iso(iso, "iso")
        with self._transaction():
            row = self._conn.execute(
                "SELECT state FROM files WHERE shoko_file_id = ?", (shoko_file_id,)
            ).fetchone()
            if row is None:
                raise TransitionError(f"file {shoko_file_id} has no tracked row to advance")
            current = FileState(_as_str(row[0], "state"))
            validate_transition(current, new_state)
            self._conn.execute(
                "UPDATE files SET state = ?, state_detail = ?, updated_iso = ?"
                " WHERE shoko_file_id = ?",
                (new_state.value, _clip(detail), iso, shoko_file_id),
            )

    @_mutation
    def advance_bulk(
        self, file_ids: Sequence[int], new_state: FileState, detail: str, *, iso: str
    ) -> int:
        """One-transaction multi-file advance for the first-scan walk (7k wall-time).

        Per-row semantics are **identical** to :meth:`advance`: every transition is
        validated against the edge table, illegal edges are skipped (the caller
        re-evaluates them on a later cycle), and rows already in ``new_state`` are
        idempotent no-ops. Returns the number of rows advanced.
        """
        _require_iso(iso, "iso")
        if not file_ids:
            return 0
        ids = tuple(dict.fromkeys(file_ids))
        with self._transaction():
            # Full literal statement: the id set is passed as a JSON parameter (json_each),
            # no query string assembly.
            rows = self._conn.execute(
                "SELECT files.shoko_file_id, files.state FROM files"
                " JOIN json_each(?) AS requested ON CAST(requested.value AS INTEGER)"
                " = files.shoko_file_id",
                (json.dumps(list(ids)),),
            ).fetchall()
            legal: list[int] = []
            for file_id, raw in rows:
                current = FileState(_as_str(raw, "state"))
                if current is new_state:
                    continue
                with contextlib.suppress(TransitionError):
                    validate_transition(current, new_state)
                    legal.append(_as_int(file_id, "shoko_file_id"))
            self._conn.executemany(
                "UPDATE files SET state = ?, state_detail = ?, updated_iso = ?"
                " WHERE shoko_file_id = ?",
                [(new_state.value, _clip(detail), iso, file_id) for file_id in legal],
            )
        return len(legal)

    # ------------------------------------------------------------ baselines

    @_mutation
    def record_baseline(
        self,
        files: Sequence[BaselineFile],
        *,
        complete: bool,
        auto_apply_eligible: bool,
        iso: str,
        series_snapshot_hash: str = "",
        reason: str = "",
    ) -> int:
        """Persist one baseline cohort snapshot; completion markers land in meta."""
        _require_iso(iso, "iso")
        deduped = {(item.sonarr_series_id, item.sonarr_file_id): item for item in files}
        with self._transaction():
            cursor = self._conn.execute(
                "INSERT INTO baselines (created_iso, complete, auto_apply_eligible,"
                " series_snapshot_hash, reason) VALUES (?, ?, ?, ?, ?)",
                (iso, int(complete), int(auto_apply_eligible), series_snapshot_hash, _clip(reason)),
            )
            baseline_id = _as_int(cursor.lastrowid, "baseline_id")
            self._conn.executemany(
                "INSERT INTO baseline_files (baseline_id, sonarr_series_id, sonarr_file_id,"
                " path, size) VALUES (?, ?, ?, ?, ?)",
                [
                    (baseline_id, series_id, file_id, item.path, item.size)
                    for (series_id, file_id), item in sorted(deduped.items())
                ],
            )
            if complete:
                self._set_meta_tx(META_BASELINE_COMPLETED_ISO, iso)
        return baseline_id

    def latest_baseline(self) -> Baseline | None:
        row = self._conn.execute(
            "SELECT baseline_id, created_iso, complete, auto_apply_eligible,"
            " series_snapshot_hash, reason FROM baselines ORDER BY baseline_id DESC LIMIT 1",
        ).fetchone()
        if row is None:
            return None
        count = self._conn.execute(
            "SELECT COUNT(*) FROM baseline_files WHERE baseline_id = ?",
            (_as_int(row[0], "baseline_id"),),
        ).fetchone()
        return Baseline(
            baseline_id=_as_int(row[0], "baseline_id"),
            created_iso=_as_str(row[1], "created_iso"),
            complete=bool(_as_int(row[2], "complete")),
            auto_apply_eligible=bool(_as_int(row[3], "auto_apply_eligible")),
            series_snapshot_hash=_as_str(row[4], "series_snapshot_hash"),
            reason=_as_str(row[5], "reason"),
            file_count=_as_int(count[0], "count"),
        )

    def classify_new(
        self,
        shoko_file_id: int,
        *,
        sonarr_series_id: int,
        sonarr_file_id: int,
        known_shoko_file_ids: frozenset[int],
    ) -> Cohort:
        """Cohort classification for one first-seen Shoko file.

        Known Shoko id → backlog. First Shoko sighting of a Sonarr identity that
        predates the baseline (late import) → **backlog**, never a new arrival.
        Otherwise (Sonarr identity first seen after the baseline) → new_arrival.
        """
        if shoko_file_id in known_shoko_file_ids:
            return Cohort.BACKLOG
        row = self._conn.execute(
            "SELECT 1 FROM baseline_files WHERE sonarr_series_id = ? AND sonarr_file_id = ?"
            " LIMIT 1",
            (sonarr_series_id, sonarr_file_id),
        ).fetchone()
        return Cohort.BACKLOG if row is not None else Cohort.NEW_ARRIVAL

    def can_auto_apply(
        self,
        shoko_file_id: int,
        *,
        now: str,
        attestation_valid: bool,
        gate_open: bool,
        budgets_ok: bool,
        mapping_stable: bool,
        source_stable: bool,
    ) -> EligibilityVerdict:
        """Pure composition of durable facts + caller-supplied external facts.

        Daemon auto-apply requires: a complete, auto-apply-eligible baseline, the
        file in the ``new_arrival`` cohort (backlog and campaign files never
        auto-apply), a valid attestation, an open apply gate, budget headroom,
        stable mappings, a stable source, and no pending rebaseline requirement.
        """
        _require_iso(now, "now")
        reasons = self._eligibility_reasons(shoko_file_id, now)
        gates = (
            (gate_open, "apply_gate_closed"),
            (attestation_valid, "attestation_invalid"),
            (budgets_ok, "budgets_exhausted"),
            (mapping_stable, "mapping_unstable"),
            (source_stable, "source_unstable"),
        )
        reasons.extend(label for open_flag, label in gates if not open_flag)
        return EligibilityVerdict(not reasons, tuple(reasons))

    def _eligibility_reasons(self, shoko_file_id: int, now: str) -> list[str]:
        """Durable-side refusals: baseline state, cohort membership, discontinuity."""
        reasons: list[str] = []
        baseline = self.latest_baseline()
        if baseline is None:
            reasons.append("no_baseline")
        else:
            if not baseline.complete:
                reasons.append("baseline_incomplete")
            if not baseline.auto_apply_eligible:
                reasons.append("baseline_not_eligible")
            if _parse_iso(baseline.created_iso) > _parse_iso(now):
                reasons.append("baseline_created_in_future")
        record = self.get_file(shoko_file_id)
        if record is None:
            reasons.append("file_unknown")
        elif record.cohort is not Cohort.NEW_ARRIVAL:
            label = (
                "backlog_not_auto_applied"
                if record.cohort is Cohort.BACKLOG
                else "campaign_requires_approval"
            )
            reasons.append(label)
        if self.get_meta(META_REQUIRES_REBASELINE) == "1":
            reasons.append("source_discontinuity")
        return reasons

    # -------------------------------------------------------------- budgets

    def _require_limits(self) -> BudgetLimits:
        if self.budget_limits is None:
            raise StateError("no BudgetLimits supplied to the store (the daemon passes them)")
        return self.budget_limits

    def budget_view(self) -> BudgetView:
        global_row = self._conn.execute(
            "SELECT rolling_new_submissions, rolling_window_start_iso,"
            " pending_file_requests, pending_episode_jobs, paused_reason"
            " FROM budgets WHERE window_key = ?",
            (GLOBAL_BUDGET_KEY,),
        ).fetchone()
        cycle_key = self.get_meta(META_BUDGET_CYCLE_KEY) or ""
        cycle_row = (
            self._conn.execute(
                "SELECT cycle_new_submissions, completed_submissions FROM budgets"
                " WHERE window_key = ?",
                (cycle_key,),
            ).fetchone()
            if cycle_key
            else None
        )
        if global_row is None:
            return BudgetView(
                window_key=cycle_key,
                cycle_new_submissions=_cycle_new(cycle_row),
                rolling_new_submissions=0,
                rolling_window_start_iso=None,
                pending_file_requests=0,
                pending_episode_jobs=0,
                paused_reason=None,
                completed_submissions=_cycle_completed(cycle_row),
            )
        return BudgetView(
            window_key=cycle_key,
            cycle_new_submissions=_cycle_new(cycle_row),
            rolling_new_submissions=_as_int(global_row[0], "rolling_new_submissions"),
            rolling_window_start_iso=_as_optional_str(global_row[1]),
            pending_file_requests=_as_int(global_row[2], "pending_file_requests"),
            pending_episode_jobs=_as_int(global_row[3], "pending_episode_jobs"),
            paused_reason=_as_optional_str(global_row[4]),
            completed_submissions=_cycle_completed(cycle_row),
        )

    def _ensure_global_budget_tx(self) -> None:
        row = self._conn.execute(
            "SELECT 1 FROM budgets WHERE window_key = ?", (GLOBAL_BUDGET_KEY,)
        ).fetchone()
        if row is None:
            self._conn.execute(
                "INSERT INTO budgets (window_key, rolling_new_submissions,"
                " pending_file_requests, pending_episode_jobs)"
                " VALUES (?, 0, 0, 0)",
                (GLOBAL_BUDGET_KEY,),
            )

    def _reserve_tx(
        self, cycle_key: str, iso: str, *, expected_episode_jobs: int
    ) -> SlotReservation | BudgetRefusal:
        limits = self._require_limits()
        self._ensure_global_budget_tx()
        global_row = self._conn.execute(
            "SELECT rolling_new_submissions, rolling_window_start_iso,"
            " pending_file_requests, pending_episode_jobs, paused_reason"
            " FROM budgets WHERE window_key = ?",
            (GLOBAL_BUDGET_KEY,),
        ).fetchone()
        assert global_row is not None
        paused = _as_optional_str(global_row[4])
        if paused:
            return BudgetRefusal(False, "paused", f"writes paused: {paused}")
        rolling = _as_int(global_row[0], "rolling_new_submissions")
        rolling_start = _as_optional_str(global_row[1])
        now = _parse_iso(iso)
        if rolling_start is None or now - _parse_iso(rolling_start) >= timedelta(
            hours=limits.rolling_window_hours
        ):
            rolling = 0
            rolling_start = iso
        cycle_row = self._conn.execute(
            "SELECT cycle_new_submissions FROM budgets WHERE window_key = ?",
            (cycle_key,),
        ).fetchone()
        cycle_new = _as_int(cycle_row[0], "cycle_new_submissions") if cycle_row else 0
        pending_files = _as_int(global_row[2], "pending_file_requests")
        pending_jobs = _as_int(global_row[3], "pending_episode_jobs")
        refusal = self._limit_refusal(
            limits,
            cycle_new=cycle_new,
            rolling=rolling,
            pending_files=pending_files,
            pending_jobs=pending_jobs,
            expected_episode_jobs=expected_episode_jobs,
        )
        if refusal is not None:
            # P3d: persist the refusal reason marker alongside the counters.
            self._set_meta_tx(
                META_LAST_REFUSAL,
                json.dumps({"reason": refusal.reason, "detail": refusal.detail, "iso": iso}),
            )
            return refusal
        rolling += 1
        cycle_new += 1
        pending_files += 1
        pending_jobs += expected_episode_jobs
        self._conn.execute(
            "UPDATE budgets SET rolling_new_submissions = ?,"
            " rolling_window_start_iso = ?, pending_file_requests = ?,"
            " pending_episode_jobs = ? WHERE window_key = ?",
            (rolling, rolling_start, pending_files, pending_jobs, GLOBAL_BUDGET_KEY),
        )
        self._conn.execute(
            "INSERT INTO budgets (window_key, cycle_new_submissions) VALUES (?, ?)"
            " ON CONFLICT (window_key) DO UPDATE SET cycle_new_submissions = ?",
            (cycle_key, cycle_new, cycle_new),
        )
        self._set_meta_tx(META_BUDGET_CYCLE_KEY, cycle_key)
        return SlotReservation(
            accepted=True,
            window_key=cycle_key,
            cycle_new_submissions=cycle_new,
            rolling_new_submissions=rolling,
            pending_file_requests=pending_files,
            pending_episode_jobs=pending_jobs,
        )

    @staticmethod
    def _limit_refusal(
        limits: BudgetLimits,
        *,
        cycle_new: int,
        rolling: int,
        pending_files: int,
        pending_jobs: int,
        expected_episode_jobs: int,
    ) -> BudgetRefusal | None:
        if pending_files + 1 > limits.max_pending_files:
            return BudgetRefusal(
                False,
                "pending_files_exhausted",
                f"{pending_files} pending file requests (max {limits.max_pending_files})",
            )
        if pending_jobs + expected_episode_jobs > limits.max_pending_episode_jobs:
            return BudgetRefusal(
                False,
                "pending_episode_jobs_exhausted",
                f"{pending_jobs} pending episode jobs + {expected_episode_jobs} expected "
                f"(max {limits.max_pending_episode_jobs})",
            )
        if cycle_new >= limits.per_cycle_new:
            return BudgetRefusal(
                False,
                "cycle_new_exhausted",
                f"{cycle_new} new submissions this cycle (max {limits.per_cycle_new})",
            )
        if rolling >= limits.rolling_new:
            return BudgetRefusal(
                False,
                "rolling_new_exhausted",
                f"{rolling} new submissions in the rolling window (max {limits.rolling_new})",
            )
        return None

    @_mutation
    def try_reserve_slot(
        self, cycle_key: str, iso: str, *, expected_episode_jobs: int = 1
    ) -> SlotReservation | BudgetRefusal:
        """Reserve one submission slot (pending + cycle + rolling counters) atomically."""
        _require_iso(iso, "iso")
        with self._transaction():
            return self._reserve_tx(cycle_key, iso, expected_episode_jobs=expected_episode_jobs)

    @_mutation
    def release_slot(self, cycle_key: str, iso: str, *, episode_jobs: int = 1) -> None:
        """Release an anonymous slot reserved by :meth:`try_reserve_slot` (no send row).

        Send-bound slots are owned by the single-release funnels (``record_verification``,
        ``record_send_outcome``, ``void_prepared_send``, ``resolve_send``, partial-close),
        which mark the send row ``slot_released``. A raw release here that would drive
        ``pending`` below the live unreleased-send ledger is refused (round-2 review F1):
        releasing a send-bound slot here bypassed the ledger and let a later VERIFIED close
        release the same reservation twice.
        """
        self._require_cycle_key(cycle_key)
        _require_iso(iso, "iso")
        with self._transaction():
            self._ensure_global_budget_tx()
            live_sends, live_jobs = self.live_send_counts()
            row = self._conn.execute(
                "SELECT pending_file_requests, pending_episode_jobs FROM budgets"
                " WHERE window_key = ?",
                (GLOBAL_BUDGET_KEY,),
            ).fetchone()
            assert row is not None
            files_after = _as_int(row[0], "pending_file_requests") - 1
            jobs_after = _as_int(row[1], "pending_episode_jobs") - episode_jobs
            if files_after < live_sends or jobs_after < live_jobs:
                raise StateError(
                    f"release_slot refused: live unreleased sends ({live_sends} files / "
                    f"{live_jobs} jobs) outnumber the release; send-bound slots are released "
                    "only through record_verification / record_send_outcome / "
                    "void_prepared_send / resolve_send (F1 ledger guard)"
                )
            self._adjust_pending_tx(-1, -episode_jobs)

    @_mutation
    def mark_completed(self, cycle_key: str, iso: str) -> None:
        """Record one cycle completion (rolling counter) — **never releases pending**.

        The pending-slot release is owned by :meth:`record_verification` /
        :meth:`record_send_outcome` / :meth:`void_prepared_send` (M6 review P1-2: the old
        double release here freed a slot a verified send had already released).
        """
        self._require_cycle_key(cycle_key)
        _require_iso(iso, "iso")
        with self._transaction():
            self._conn.execute(
                "INSERT INTO budgets (window_key, cycle_new_submissions, completed_submissions)"
                " VALUES (?, 0, 1)"
                " ON CONFLICT (window_key) DO UPDATE SET"
                " completed_submissions = completed_submissions + 1",
                (cycle_key,),
            )

    @staticmethod
    def _require_cycle_key(cycle_key: str) -> None:
        if not cycle_key.strip():
            raise StateError("cycle_key must be a non-empty string")

    def _adjust_pending_tx(self, files_delta: int, jobs_delta: int) -> None:
        self._ensure_global_budget_tx()
        row = self._conn.execute(
            "SELECT pending_file_requests, pending_episode_jobs FROM budgets"
            " WHERE window_key = ?",
            (GLOBAL_BUDGET_KEY,),
        ).fetchone()
        assert row is not None
        files_pending = max(0, _as_int(row[0], "pending_file_requests") + files_delta)
        jobs_pending = max(0, _as_int(row[1], "pending_episode_jobs") + jobs_delta)
        self._conn.execute(
            "UPDATE budgets SET pending_file_requests = ?, pending_episode_jobs = ?"
            " WHERE window_key = ?",
            (files_pending, jobs_pending, GLOBAL_BUDGET_KEY),
        )

    @_mutation
    def pause(self, reason: str, iso: str) -> None:
        """Persisted write pause (mapping churn, queue anomalies, lease, source breaks)."""
        _require_iso(iso, "iso")
        with self._transaction():
            self._ensure_global_budget_tx()
            self._conn.execute(
                "UPDATE budgets SET paused_reason = ? WHERE window_key = ?",
                (_clip(reason), GLOBAL_BUDGET_KEY),
            )

    @_mutation
    def resume(self, iso: str) -> None:
        _require_iso(iso, "iso")
        with self._transaction():
            self._ensure_global_budget_tx()
            self._conn.execute(
                "UPDATE budgets SET paused_reason = NULL WHERE window_key = ?",
                (GLOBAL_BUDGET_KEY,),
            )

    # ---------------------------------------------------------- send machine

    @_mutation
    def prepare_send(
        self,
        shoko_file_id: int,
        plan_fingerprint: str,
        request_fingerprint: str,
        *,
        targets_digest: str,
        structure_digest: str,
        mapping_revision: str,
        cycle_key: str,
        iso: str,
        policy: str | None = None,
        campaign_id: int | None = None,
        expected_episode_jobs: int = 1,
        target_ids: Sequence[tuple[int, int, int]] = (),
    ) -> SendToken | BudgetRefusal:
        """Commit ``prepared_not_sent`` **and** the budget slot in ONE transaction.

        Call contract (M6-b): this must complete before any bytes touch HTTP.
        ``expected_episode_jobs`` is the *reserved* count the release-owner APIs
        (:meth:`record_verification` / :meth:`record_send_outcome` /
        :meth:`void_prepared_send`) later free (P2-4: the real plan job count,
        not a file count). ``target_ids`` are the verification triples persisted on the
        send row so a persisted ``verifying`` / hold row can be re-polled/re-inspected
        after a restart (P1-3).
        Replay protection: an active send with the same request fingerprint
        (prepared/attempted/accepted-unverified) is refused as
        ``duplicate_request_fingerprint`` — restarting the same plan never
        double-counts a budget. Terminal rows (``voided`` from
        :meth:`void_prepared_send`, closed ``verified``/partial/contradiction outcomes)
        are NOT active: a voided prepared send never blocks the re-prepare of its
        replacement (P2-2); sent rows are never resubmitted (no transition edge).
        """
        _require_iso(iso, "iso")
        _require_hex64(plan_fingerprint, "plan_fingerprint")
        _require_hex64(request_fingerprint, "request_fingerprint")
        _require_hex64(targets_digest, "targets_digest")
        _require_hex64(structure_digest, "structure_digest")
        jobs = _as_int(expected_episode_jobs, "expected_episode_jobs")
        if jobs < 0:
            raise StateError("expected_episode_jobs must be non-negative")
        with self._transaction():
            # Replay protection FIRST: an active send with the same request
            # fingerprint (prepared/attempted/accepted-unverified) never reserves a
            # second budget slot — replaying a plan after a restart is idempotent.
            # Voided/closed rows are excluded (P2-2): voided sends no longer block.
            duplicate = self._conn.execute(
                "SELECT send_id, plan_fingerprint FROM sends WHERE request_fingerprint = ?"
                " AND outcome IN ('prepared', 'attempted', 'accepted_unverified') LIMIT 1",
                (request_fingerprint,),
            ).fetchone()
            if duplicate is not None:
                self._set_meta_tx(
                    META_LAST_REFUSAL,
                    json.dumps(
                        {
                            "reason": "duplicate_request_fingerprint",
                            "detail": "active send holds the fingerprint",
                            "iso": iso,
                        }
                    ),
                )
                return BudgetRefusal(
                    False,
                    "duplicate_request_fingerprint",
                    f"request {request_fingerprint[:12]}… already has active send "
                    f"{_as_int(duplicate[0], 'send_id')}; budget is not double-counted",
                )
            row = self._conn.execute(
                "SELECT state, cohort FROM files WHERE shoko_file_id = ?",
                (shoko_file_id,),
            ).fetchone()
            if row is None:
                raise TransitionError(f"file {shoko_file_id} has no tracked row to prepare")
            current = FileState(_as_str(row[0], "state"))
            validate_transition(current, FileState.PREPARED_NOT_SENT)
            decision = self._reserve_tx(cycle_key, iso, expected_episode_jobs=expected_episode_jobs)
            if isinstance(decision, BudgetRefusal):
                return decision
            cursor = self._conn.execute(
                "INSERT INTO sends (shoko_file_id, request_fingerprint, plan_fingerprint,"
                " attempted_iso, outcome, episode_jobs, target_ids_json)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    shoko_file_id,
                    request_fingerprint,
                    plan_fingerprint,
                    iso,
                    OUTCOME_PREPARED,
                    jobs,
                    _encode_target_ids(target_ids),
                ),
            )
            send_id = _as_int(cursor.lastrowid, "send_id")
            base_cohort = Cohort(_as_str(row[1], "cohort"))
            cohort = Cohort.CAMPAIGN if campaign_id is not None else base_cohort
            self._conn.execute(
                "UPDATE files SET state = ?, plan_fingerprint = ?, request_fingerprint = ?,"
                " mapping_targets_digest = ?, mapping_structure_digest = ?,"
                " mapping_revision = ?, policy = ?, campaign_id = ?, cohort = ?,"
                " state_detail = ?, updated_iso = ? WHERE shoko_file_id = ?",
                (
                    FileState.PREPARED_NOT_SENT.value,
                    plan_fingerprint,
                    request_fingerprint,
                    targets_digest,
                    structure_digest,
                    mapping_revision,
                    policy,
                    campaign_id,
                    cohort.value,
                    "prepared for send",
                    iso,
                    shoko_file_id,
                ),
            )
        return SendToken(
            accepted=True,
            send_id=send_id,
            window_key=cycle_key,
            shoko_file_id=shoko_file_id,
            plan_fingerprint=plan_fingerprint,
            request_fingerprint=request_fingerprint,
        )

    @_mutation
    def mark_sent(self, token: SendToken, *, iso: str) -> None:
        """Durably advance to ``request_may_have_been_sent`` **before** the caller sends.

        The row is committed by the time this returns; M6-b calls it immediately
        before handing bytes to HTTP. From here on the item is verify/hold —
        resubmission is forbidden by the transition table.
        """
        _require_iso(iso, "iso")
        with self._transaction():
            row = self._conn.execute(
                "SELECT state FROM files WHERE shoko_file_id = ?",
                (token.shoko_file_id,),
            ).fetchone()
            if row is None:
                raise TransitionError(f"file {token.shoko_file_id} has no tracked row")
            current = FileState(_as_str(row[0], "state"))
            validate_transition(current, FileState.REQUEST_MAY_HAVE_BEEN_SENT)
            self._conn.execute(
                "UPDATE files SET state = ?, state_detail = ?, updated_iso = ?"
                " WHERE shoko_file_id = ?",
                (
                    FileState.REQUEST_MAY_HAVE_BEEN_SENT.value,
                    "handing to transport (mark_sent committed first)",
                    iso,
                    token.shoko_file_id,
                ),
            )
            self._conn.execute(
                "UPDATE sends SET outcome = ?, attempted_iso = ? WHERE send_id = ?",
                (OUTCOME_ATTEMPTED, iso, token.send_id),
            )

    @_mutation
    def record_send_outcome(
        self,
        token: SendToken,
        outcome: SendOutcome,
        *,
        iso: str,
        http_status: int | None = None,
        detail: str = "",
    ) -> FileState:
        """Record the intent result: verifying / indeterminate_write / waiting.

        REJECTED and REFUSED carry the pinned zero-write assumption, so the
        slot is released; AMBIGUOUS_SEND keeps the slot (jobs may exist server-side)
        and files the row as ``indeterminate_write`` — never resubmitted.
        """
        _require_iso(iso, "iso")
        target = _SEND_OUTCOME_STATE[outcome]
        with self._transaction():
            row = self._conn.execute(
                "SELECT state FROM files WHERE shoko_file_id = ?",
                (token.shoko_file_id,),
            ).fetchone()
            if row is None:
                raise TransitionError(f"file {token.shoko_file_id} has no tracked row")
            current = FileState(_as_str(row[0], "state"))
            validate_transition(current, target)
            self._conn.execute(
                "UPDATE files SET state = ?, state_detail = ?, updated_iso = ?"
                " WHERE shoko_file_id = ?",
                (
                    target.value,
                    _clip(f"send outcome {outcome.value}: {detail}"),
                    iso,
                    token.shoko_file_id,
                ),
            )
            self._conn.execute(
                "UPDATE sends SET outcome = ?, http_status = ? WHERE send_id = ?",
                (outcome.value, http_status, token.send_id),
            )
            if outcome in (SendOutcome.REJECTED, SendOutcome.REFUSED):
                # P2-4/P1-A: release the *stored* reservation through the single-owner path
                # (file slot + the exact episode-job count recorded at reserve time), which
                # atomically marks the row released. The old hard-coded -1 is gone.
                self._release_send_slot_tx(token.send_id, iso, "send_outcome")
        return target

    def _stored_jobs_tx(self, send_id: int) -> int:
        row = self._conn.execute(
            "SELECT episode_jobs FROM sends WHERE send_id = ?", (send_id,)
        ).fetchone()
        return 0 if row is None else max(0, _as_int(row[0], "episode_jobs"))

    def _record_slot_release_noop_tx(self, send_id: int, iso: str, path: str) -> None:
        """Audit note (P1-A): a release attempt on an already-released row is a no-op."""
        raw = self.get_meta(META_SLOT_RELEASE_NOOPS)
        entries: list[Any] = []
        if raw is not None:
            try:
                document = json.loads(raw)
            except ValueError as exc:
                raise StateError(f"slot release audit is not valid JSON: {exc}") from exc
            if isinstance(document, list):
                entries = document
        entries.append({"send_id": send_id, "iso": iso, "path": path})
        self._set_meta_tx(META_SLOT_RELEASE_NOOPS, json.dumps(entries[-50:], sort_keys=True))

    def _release_send_slot_tx(self, send_id: int, iso: str, path: str) -> int:
        """Release the send row's reservation **exactly once** (P1-A).

        Reads the row's stored ``episode_jobs`` and its ``slot_released`` flag in the same
        transaction: a live row is decremented and atomically marked released; an already
        released row is a recorded no-op. Callers rely on this ownership, never on the
        ``max(0)`` clamp in :meth:`_adjust_pending_tx`.
        """
        row = self._conn.execute(
            "SELECT episode_jobs, slot_released FROM sends WHERE send_id = ?", (send_id,)
        ).fetchone()
        if row is None:
            return 0
        jobs = max(0, _as_int(row[0], "episode_jobs"))
        released = bool(_as_int(row[1], "slot_released"))
        if released:
            self._record_slot_release_noop_tx(send_id, iso, path)
            return 0
        self._adjust_pending_tx(-1, -jobs)
        self._conn.execute("UPDATE sends SET slot_released = 1 WHERE send_id = ?", (send_id,))
        return jobs

    def _release_verified_slot_tx(self, shoko_file_id: int, iso: str) -> int:
        """Release the reservation for a VERIFIED close, gated on ``slot_released``.

        Locates the file's newest send row (live **or** already closed ``verified``) and
        releases it at most once: the second ``record_verification(VERIFIED)`` in a
        double-verify sequence, and a VERIFIED that follows a partial close, are no-ops.
        """
        row = self._conn.execute(
            "SELECT send_id, episode_jobs, slot_released FROM sends WHERE shoko_file_id = ?"
            " ORDER BY send_id DESC LIMIT 1",
            (shoko_file_id,),
        ).fetchone()
        if row is None:
            return 0
        send_id = _as_int(row[0], "send_id")
        return self._release_send_slot_tx(send_id, iso, "verified")

    @_mutation
    def record_verification(
        self,
        shoko_file_id: int,
        verdict: VerificationVerdict,
        *,
        iso: str,
    ) -> FileState | None:
        """Close the verification loop — the **sole release owner** for a verified send.

        UNKNOWN and PENDING keep the row in ``verifying`` (continue polling);
        CONTRADICTION is a conflict for operator review — never an automatic relink.
        VERIFIED releases exactly the reservation recorded at reserve time (file slot +
        stored ``episode_jobs``) — ``mark_completed`` never releases (P1-2). The
        live send lookup covers ``ambiguous_send`` rows too (P1-3): an
        indeterminate row verified via XRef evidence closes and releases its held slot.
        """
        _require_iso(iso, "iso")
        target = _VERIFICATION_STATE[verdict]
        with self._transaction():
            row = self._conn.execute(
                "SELECT state FROM files WHERE shoko_file_id = ?",
                (shoko_file_id,),
            ).fetchone()
            if row is None:
                raise TransitionError(f"file {shoko_file_id} has no tracked row")
            current = FileState(_as_str(row[0], "state"))
            if target is not None and target is not current:
                validate_transition(current, target)
            new_state = current if target is None else target
            self._conn.execute(
                "UPDATE files SET state = ?, state_detail = ?, updated_iso = ?,"
                " last_verified_iso = ? WHERE shoko_file_id = ?",
                (new_state.value, _clip(f"verification {verdict.value}"), iso, iso, shoko_file_id),
            )
            send_row = self._conn.execute(
                "SELECT send_id FROM sends WHERE shoko_file_id = ?"
                " AND outcome IN ('prepared', 'attempted', 'accepted_unverified', 'ambiguous_send')"
                " ORDER BY send_id DESC LIMIT 1",
                (shoko_file_id,),
            ).fetchone()
            # VERIFIED closes the send row (``verified``). PENDING/UNKNOWN/PARTIAL/
            # CONTRADICTION leave the delivery outcome LIVE (the row stays re-pollable /
            # inspectable by the next cycle's recovery consumption, P1-3; the verdict is
            # recorded in the verification columns either way).
            if send_row is not None:
                send_id = _as_int(send_row[0], "send_id")
                if target is FileState.VERIFIED:
                    self._conn.execute(
                        "UPDATE sends SET outcome = ?, verification = ?, verified_iso = ?"
                        " WHERE send_id = ?",
                        ("verified", verdict.value, iso, send_id),
                    )
                else:
                    self._conn.execute(
                        "UPDATE sends SET verification = ?, verified_iso = ? WHERE send_id = ?",
                        (verdict.value, iso, send_id),
                    )
            if target is FileState.VERIFIED:
                # P1-A: release via the send row's ownership flag (file slot + stored jobs).
                # A double-verify, and a VERIFIED that follows a partial close, are no-ops
                # (the reservation was already released); the ``max(0)`` clamp is never the
                # mechanism that keeps the ceiling intact.
                self._release_verified_slot_tx(shoko_file_id, iso)
        return target

    @_mutation
    def void_prepared_send(
        self, shoko_file_id: int, request_fingerprint: str, *, iso: str, reason: str
    ) -> int:
        """Close a definite-not-sent prepared send and return the file to ``planned``.

        Legal only from the ``prepared`` outcome (P2-2): no bytes ever reached HTTP, so
        the ``prepared_not_sent → planned`` edge stands (documented: replanning a
        definitely-not-sent file cannot double-deliver). Releases the stored reservation
        (file slot + stored episode jobs, P2-4), keeps the send row as a ``voided``
        audit entry (audit trail intact), and the voided row never blocks the
        replacement prepare (dedup covers active outcomes only). Rows that may have
        been sent are NEVER voidable here — verify/hold only (P1 contract unchanged).
        """
        _require_iso(iso, "iso")
        with self._transaction():
            row = self._conn.execute(
                "SELECT send_id, outcome, episode_jobs FROM sends"
                " WHERE shoko_file_id = ? AND request_fingerprint = ?"
                " ORDER BY send_id DESC LIMIT 1",
                (shoko_file_id, request_fingerprint),
            ).fetchone()
            if row is None:
                raise StateError(
                    f"no send row for file {shoko_file_id} with request "
                    f"{request_fingerprint[:12]}…: nothing to void"
                )
            send_id = _as_int(row[0], "send_id")
            outcome = _as_str(row[1], "outcome")
            if outcome != OUTCOME_PREPARED:
                raise StateError(
                    f"send {send_id} has outcome {outcome!r}: void is legal only from "
                    "prepared_not_sent (a possible-delivery row is verify/hold, never voided)"
                )
            file_row = self._conn.execute(
                "SELECT state FROM files WHERE shoko_file_id = ?", (shoko_file_id,)
            ).fetchone()
            if file_row is None:
                raise TransitionError(f"file {shoko_file_id} has no tracked row to void")
            current = FileState(_as_str(file_row[0], "state"))
            validate_transition(current, FileState.PLANNED)
            self._conn.execute(
                "UPDATE sends SET outcome = ? WHERE send_id = ?", (OUTCOME_VOIDED, send_id)
            )
            self._conn.execute(
                "UPDATE files SET state = ?, state_detail = ?, updated_iso = ?"
                " WHERE shoko_file_id = ?",
                (
                    FileState.PLANNED.value,
                    _clip(f"prepared send voided: {reason}"),
                    iso,
                    shoko_file_id,
                ),
            )
            # P2-4/P1-A: release the stored reservation via the single-owner path (marks the
            # voided row released atomically; a re-void of the same row is a recorded no-op).
            self._release_send_slot_tx(send_id, iso, "void")
        return send_id

    def active_send_details(self, shoko_file_id: int) -> SendInspection | None:
        """The newest live send row (delivery state, reserved jobs, expected triples).

        Read-only; recovery re-polls/re-inspects persisted rows with these inputs
        (P1-3) — never for resubmission.
        """
        row = self._conn.execute(
            "SELECT send_id, request_fingerprint, outcome, episode_jobs, target_ids_json,"
            " attempted_iso, slot_released"
            " FROM sends WHERE shoko_file_id = ?"
            " AND outcome IN ('prepared', 'attempted', 'accepted_unverified', 'ambiguous_send')"
            " ORDER BY send_id DESC LIMIT 1",
            (shoko_file_id,),
        ).fetchone()
        if row is None:
            return None
        return SendInspection(
            send_id=_as_int(row[0], "send_id"),
            request_fingerprint=_as_str(row[1], "request_fingerprint"),
            outcome=_as_str(row[2], "outcome"),
            episode_jobs=_as_int(row[3], "episode_jobs"),
            target_ids=_parse_target_ids(_as_optional_str(row[4])),
            attempted_iso=_as_str(row[5], "attempted_iso"),
            slot_released=bool(_as_int(row[6], "slot_released")),
        )

    @_mutation
    def release_send_slot(self, shoko_file_id: int, request_fingerprint: str, *, iso: str) -> int:
        """Release the held reservation of a live send row (daemon past-deadline partial close).

        Marks the row released **exactly once** (P1-A): a partial close of a NOT-linked
        past-deadline row frees the slot, and the delivery outcome stays live so a late
        ``ManualLinkJob`` can still be re-verified — the subsequent VERIFIED close is then a
        no-op (``slot_released=1``), never a second release. Returns the jobs released (0 when
        the row was already released).
        """
        _require_iso(iso, "iso")
        with self._transaction():
            row = self._conn.execute(
                "SELECT send_id FROM sends WHERE shoko_file_id = ? AND request_fingerprint = ?"
                " AND outcome IN ('prepared', 'attempted', 'accepted_unverified', 'ambiguous_send')"
                " ORDER BY send_id DESC LIMIT 1",
                (shoko_file_id, request_fingerprint),
            ).fetchone()
            if row is None:
                return 0
            return self._release_send_slot_tx(_as_int(row[0], "send_id"), iso, "partial_close")

    @_mutation
    def resolve_send(
        self,
        shoko_file_id: int,
        request_fingerprint: str,
        *,
        resolution: Literal["released_after_review", "operator_closed"],
        iso: str,
        operator_note: str = "",
        verification_timeout_seconds: float | None = None,
    ) -> int:
        """Operator-sanctioned close of a live **terminal-review** send (P2-B, M7 CLI).

        Legal ONLY from terminal-review send outcomes: ``accepted_unverified`` (verifying /
        conflict) and ``ambiguous_send`` (indeterminate_write). ``prepared`` rows are owned by
        :meth:`void_prepared_send` and ``attempted`` (request-may-have-been-sent) rows are
        owned by the XRef inspection path — closing them here is refused (round-2 review F2;
        the operator closes a delivery-uncertain row only *after* inspection records it).
        When ``verification_timeout_seconds`` is supplied for an ``accepted_unverified`` row,
        the verification deadline (``attempted_iso`` + timeout) must have elapsed. It sets the
        send row closed (``resolved``), releases exactly the stored
        reservation through the P1-A single-owner path (a no-op when already released), and
        moves the file to ``waiting`` (re-evaluable, ``released_after_review``) or
        ``blocked_safety`` (``operator_closed``). It **never** touches a ``verified`` row and
        never re-enables resubmission of a *sent* request: the same ``request_fingerprint``
        stays refused while a row is live, and closing the row only lets the file re-plan as a
        **fresh** ``request_fingerprint`` (the ``resolved`` outcome is closed, so dedup never
        matches it). Rows that may have been delivered keep the audit note; nothing here
        is reachable from the daemon (operator-only chain, P3-B).
        """
        _require_iso(iso, "iso")
        target = (
            FileState.WAITING if resolution == "released_after_review" else FileState.BLOCKED_SAFETY
        )
        with self._transaction():
            row = self._conn.execute(
                "SELECT send_id, outcome, slot_released FROM sends"
                " WHERE shoko_file_id = ? AND request_fingerprint = ?"
                " ORDER BY send_id DESC LIMIT 1",
                (shoko_file_id, request_fingerprint),
            ).fetchone()
            if row is None:
                raise StateError(
                    f"no send row for file {shoko_file_id} with request "
                    f"{request_fingerprint[:12]}…: nothing to resolve"
                )
            send_id = _as_int(row[0], "send_id")
            outcome = _as_str(row[1], "outcome")
            if outcome in CLOSED_SEND_OUTCOMES:
                raise StateError(
                    f"send {send_id} is closed ({outcome!r}): resolve_send never touches "
                    "verified/voided/resolved rows"
                )
            if outcome not in RESOLVE_LEGAL_OUTCOMES:
                raise StateError(
                    f"send {send_id} has outcome {outcome!r}: resolve_send is legal only from "
                    "accepted_unverified/ambiguous_send — prepared rows are voided via "
                    "void_prepared_send, attempted rows are closed only after XRef "
                    "inspection records their delivery state (F2 gate)"
                )
            if (
                outcome == SendOutcome.ACCEPTED_UNVERIFIED.value
                and verification_timeout_seconds is not None
            ):
                attempted = self._conn.execute(
                    "SELECT attempted_iso FROM sends WHERE send_id = ?", (send_id,)
                ).fetchone()
                deadline = _parse_iso(_as_str(attempted[0], "attempted_iso")) + timedelta(
                    seconds=verification_timeout_seconds
                )
                if _parse_iso(iso) < deadline:
                    raise StateError(
                        f"send {send_id}: verification deadline "
                        f"({deadline.isoformat()}) not elapsed at {iso}; resolve_send with a "
                        "verification_timeout_seconds enforces past-deadline closes"
                    )
            file_row = self._conn.execute(
                "SELECT state FROM files WHERE shoko_file_id = ?", (shoko_file_id,)
            ).fetchone()
            if file_row is None:
                raise TransitionError(f"file {shoko_file_id} has no tracked row to resolve")
            current = FileState(_as_str(file_row[0], "state"))
            if current is FileState.VERIFIED:
                raise StateError(f"file {shoko_file_id} is verified: resolve_send is refused")
            validate_transition(current, target)
            self._conn.execute(
                "UPDATE sends SET outcome = ?, verification = ? WHERE send_id = ?",
                (OUTCOME_RESOLVED, f"resolved:{resolution}", send_id),
            )
            self._release_send_slot_tx(send_id, iso, f"resolve:{resolution}")
            self._conn.execute(
                "UPDATE files SET state = ?, state_detail = ?, updated_iso = ?"
                " WHERE shoko_file_id = ?",
                (
                    target.value,
                    _clip(f"send resolved ({resolution}): {operator_note}"),
                    iso,
                    shoko_file_id,
                ),
            )
        return send_id

    def live_send_counts(self) -> tuple[int, int]:
        """Live-held sends and the slots they hold (P2-B observability).

        ``(live_held_sends, live_held_slots)``: the count of send rows still in a live
        delivery outcome that have NOT released their reservation, and the stored
        ``episode_jobs`` those rows still hold.
        """
        row = self._conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(episode_jobs), 0) FROM sends"
            " WHERE outcome IN ('prepared', 'attempted', 'accepted_unverified', 'ambiguous_send')"
            " AND slot_released = 0",
        ).fetchone()
        assert row is not None
        return _as_int(row[0], "live_held_sends"), _as_int(row[1], "live_held_slots")

    # -------------------------------------------------------------- recovery

    def recover_after_restart(self) -> RecoveryPlan:
        """Classify persisted rows for restart (read-only classification).

        ``prepared_not_sent`` = definitely not sent → replannable.
        ``request_may_have_been_sent`` / ``indeterminate_write`` = possibly sent →
        VERIFY/HOLD, never resubmit. ``verifying``/``partial`` continue polling;
        ``waiting``/pre-send states re-evaluate; ``blocked_safety`` stays;
        ``verified`` is audit-only.
        """
        rows = self._conn.execute(_SQL_ALL_FILES).fetchall()
        records = [_row_to_file_record(row) for row in rows]
        replannable: list[RecoveryRow] = []
        hold: list[RecoveryRow] = []
        polling: list[RecoveryRow] = []
        re_evaluate: list[RecoveryRow] = []
        blocked: list[RecoveryRow] = []
        audit: list[RecoveryRow] = []
        for record in records:
            entry = RecoveryRow(
                shoko_file_id=record.shoko_file_id,
                state=record.state,
                cohort=record.cohort,
                plan_fingerprint=record.plan_fingerprint,
                request_fingerprint=record.request_fingerprint,
                detail=record.state_detail,
                instruction=_instruction_for(record.state),
            )
            if record.state is FileState.PREPARED_NOT_SENT:
                replannable.append(entry)
            elif record.state in (
                FileState.REQUEST_MAY_HAVE_BEEN_SENT,
                FileState.INDETERMINATE_WRITE,
            ):
                hold.append(entry)
            elif record.state in (FileState.VERIFYING, FileState.PARTIAL):
                polling.append(entry)
            elif record.state is FileState.BLOCKED_SAFETY:
                blocked.append(entry)
            elif record.state is FileState.VERIFIED:
                audit.append(entry)
            else:
                re_evaluate.append(entry)
        return RecoveryPlan(
            replannable=tuple(replannable),
            hold_for_verification=tuple(hold),
            continue_polling=tuple(polling),
            re_evaluate=tuple(re_evaluate),
            blocked_safety=tuple(blocked),
            audit_only=tuple(audit),
        )

    # ---------------------------------------------------------- source identity

    def _anchor_source_tx(
        self,
        observation: SourceObservation,
        iso: str,
        *,
        clear_flag: bool,
    ) -> None:
        """Persist the observation as the new continuity anchor (re-anchoring on every
        evaluated cycle; growth/monotonic changes re-anchor, P1-1)."""
        self._set_meta_tx(META_SOURCE_INSTANCE, observation.instance_identity)
        self._set_meta_tx(META_SONARR_STATUS_SNAPSHOT_ID, observation.snapshot_summary_id)
        self._set_meta_tx(META_SOURCE_LAST_OBSERVED_ISO, iso)
        if observation.roots_fingerprint:
            self._set_meta_tx(META_SOURCE_ROOTS_FINGERPRINT, observation.roots_fingerprint)
        if observation.discovery_total is not None:
            self._set_meta_tx(META_SOURCE_LAST_DISCOVERY_TOTAL, str(observation.discovery_total))
        if clear_flag:
            self._set_meta_tx(META_REQUIRES_REBASELINE, "0")

    @_mutation
    def record_source_observation(
        self,
        instance_identity: str,
        snapshot_summary_hash: str,
        *,
        iso: str,
        roots_fingerprint: str = "",
        discovery_total: int | None = None,
    ) -> None:
        """Re-anchor one source observation (configured identity strings only —
        identity comes from settings, never from invented endpoints; docs §4).

        The daemon calls this after every fully-complete, non-truncated, continuity-OK
        cycle (P1-1): the anchor advances with normal inventory growth and the
        ``requires_rebaseline`` flag is cleared by a healthy full-scan observation.
        """
        _require_iso(iso, "iso")
        with self._transaction():
            self._anchor_source_tx(
                SourceObservation(
                    instance_identity=instance_identity,
                    roots_fingerprint=roots_fingerprint,
                    snapshot_summary_id=snapshot_summary_hash,
                    discovery_total=discovery_total,
                ),
                iso,
                clear_flag=True,
            )

    @_mutation
    def clear_requires_rebaseline(self, iso: str) -> None:
        """Operator/M7-sanctioned clearing of the persisted rebaseline requirement.

        The daemon also clears the flag implicitly when the triggering
        conditions are resolved with a fresh full-scan observation
        (:meth:`record_source_observation`); this is the explicit operator path.
        """
        _require_iso(iso, "iso")
        with self._transaction():
            self._set_meta_tx(META_REQUIRES_REBASELINE, "0")

    @_mutation
    def check_source_continuity(
        self,
        current_identity: str,
        current_hash: str,
        *,
        iso: str,
        roots_fingerprint: str = "",
        discovery_total: int | None = None,
        complete: bool = True,
    ) -> ContinuityVerdict:
        """Structured continuity check — ordinary inventory growth is NORMAL (P1-1).

        Continuity is **identity/roots stability plus snapshot discontinuity**:
        ``instance_identity_changed`` (configured identity string moved), ``roots_changed``
        (root-list/allowlist shape moved), ``discovery_total_drop`` (totals regressed below
        the anchored value) and ``snapshot_marker_missing`` (the expected snapshot summary
        marker is absent: no discovery total where one was anchored). A moving snapshot
        summary hash with monotonic totals is growth, not discontinuity: the observation is
        re-anchored each evaluated cycle so the check never self-wedges on new
        arrivals. ``requires_rebaseline`` is set only by the triggers, survives restart,
        and is cleared by the next clean observation (this check clears it when no trigger
        fires) or by :meth:`clear_requires_rebaseline` (operator/M7).

        P1-C: anchoring is gated on ``complete``. A **truncated/incomplete** discovery
        (``complete=False``) is not trusted — the check is **neutral**: no anchor, no
        drop trigger, no flag change; it reports ``discovery_incomplete`` and leaves the
        previously anchored total intact (a truncated page's inflated ``Total`` never
        re-anchors the continuity total).
        """
        _require_iso(iso, "iso")
        observation = SourceObservation(
            instance_identity=current_identity,
            roots_fingerprint=roots_fingerprint,
            snapshot_summary_id=current_hash,
            discovery_total=discovery_total,
        )
        with self._transaction():
            if not complete:
                # Truncated discovery: neutral, never anchors, never triggers a drop.
                return ContinuityVerdict(True, ("discovery_incomplete",), False)
            known = self.get_meta(META_SOURCE_INSTANCE)
            if known is None:
                self._anchor_source_tx(observation, iso, clear_flag=True)
                return ContinuityVerdict(True, ("first_observation",), False)
            reasons: list[str] = []
            if known != current_identity:
                reasons.append("instance_identity_changed")
            known_roots = self.get_meta(META_SOURCE_ROOTS_FINGERPRINT)
            if known_roots and roots_fingerprint and known_roots != roots_fingerprint:
                reasons.append("roots_changed")
            known_total_raw = self.get_meta(META_SOURCE_LAST_DISCOVERY_TOTAL)
            if discovery_total is None:
                if known_total_raw is not None:
                    reasons.append("snapshot_marker_missing")
            elif known_total_raw is not None and discovery_total < int(known_total_raw):
                reasons.append("discovery_total_drop")
            requires = bool(reasons)
            # Re-anchor every evaluated cycle: the anchor tracks the source forward, so
            # growth never accumulates as a permanent discontinuity.
            self._anchor_source_tx(observation, iso, clear_flag=not requires)
            if requires:
                self._set_meta_tx(META_REQUIRES_REBASELINE, "1")
        return ContinuityVerdict(not requires, tuple(reasons), requires)

    # ------------------------------------------------------------- apply lock

    @_mutation
    def acquire_apply_lock_tx(self, pid: int, iso: str, lease_expires_iso: str) -> LockAcquisition:
        """Single-process apply lock; a held lease (even expired) requires an explicit
        :func:`steal_expired_apply_lock`. Cross-installation coordination is impossible
        with SQLite file locks — deployment-level risk (module docstring)."""
        _require_iso(iso, "iso")
        _require_iso(lease_expires_iso, "lease_expires_iso")
        with self._transaction():
            holder = self._lock_holder()
            if holder is not None:
                raise LockHeldError(
                    f"apply lock held by pid {holder.owner_pid} "
                    f"(acquired {holder.acquired_iso}, lease expires {holder.lease_expires_iso})",
                    holder,
                )
            new_holder = LockHolder(pid, iso, lease_expires_iso)
            self._set_meta_tx(META_APPLY_LOCK, _encode_holder(new_holder))
        return LockAcquisition(True, new_holder)

    @_mutation
    def steal_expired_apply_lock_tx(
        self, pid: int, iso: str, lease_expires_iso: str | None = None
    ) -> LockAcquisition:
        """Take over a lease whose expiry is at/before ``iso`` — explicit recovery of a
        crashed holder; never a silent race winner."""
        _require_iso(iso, "iso")
        with self._transaction():
            holder = self._lock_holder()
            if holder is None:
                if lease_expires_iso is None:
                    raise StateError("steal without a lock requires an explicit lease expiry")
                new_holder = LockHolder(pid, iso, lease_expires_iso)
            else:
                if _parse_iso(holder.lease_expires_iso) > _parse_iso(iso):
                    raise LockHeldError(
                        f"apply lock held by pid {holder.owner_pid}; lease expires "
                        f"{holder.lease_expires_iso} (not expired at {iso})",
                        holder,
                    )
                expiry = (
                    lease_expires_iso if lease_expires_iso is not None else holder.lease_expires_iso
                )
                new_holder = LockHolder(pid, iso, expiry)
            self._set_meta_tx(META_APPLY_LOCK, _encode_holder(new_holder))
        return LockAcquisition(True, new_holder)

    @_mutation
    def acquire_or_steal_apply_lock_tx(
        self, pid: int, iso: str, lease_expires_iso: str
    ) -> LockAcquisition:
        """Acquire the apply lock, stealing it ONLY when the holder's lease is expired
        at ``iso`` (P2-1, recovery of a crashed holder; a self-held stale lease is also
        stealable). A live lease raises :class:`LockHeldError` naming the holder. Every
        steal is recorded in the persisted ``apply_lock_steals`` meta audit list."""
        _require_iso(iso, "iso")
        _require_iso(lease_expires_iso, "lease_expires_iso")
        with self._transaction():
            holder = self._lock_holder()
            if holder is not None and _parse_iso(holder.lease_expires_iso) > _parse_iso(iso):
                raise LockHeldError(
                    f"apply lock held by pid {holder.owner_pid} "
                    f"(acquired {holder.acquired_iso}, lease expires {holder.lease_expires_iso} "
                    f"— not expired at {iso})",
                    holder,
                )
            if holder is not None:
                self._record_lock_steal_tx(holder, pid, iso)
            new_holder = LockHolder(pid, iso, lease_expires_iso)
            self._set_meta_tx(META_APPLY_LOCK, _encode_holder(new_holder))
        return LockAcquisition(True, new_holder)

    def _record_lock_steal_tx(self, from_holder: LockHolder, pid: int, iso: str) -> None:
        raw = self.get_meta(META_APPLY_LOCK_STEALS)
        entries: list[Any] = []
        if raw is not None:
            try:
                document = json.loads(raw)
            except ValueError as exc:
                raise StateError(f"apply lock steal audit is not valid JSON: {exc}") from exc
            if isinstance(document, list):
                entries = document
        entries.append(
            {
                "owner_pid": pid,
                "from_pid": from_holder.owner_pid,
                "from_lease_expires_iso": from_holder.lease_expires_iso,
                "stolen_iso": iso,
            }
        )
        self._set_meta_tx(META_APPLY_LOCK_STEALS, json.dumps(entries[-50:], sort_keys=True))

    @_mutation
    def release_apply_lock_tx(self, pid: int, iso: str) -> bool:
        _require_iso(iso, "iso")
        with self._transaction():
            holder = self._lock_holder()
            if holder is None:
                return False
            if holder.owner_pid != pid:
                raise LockHeldError(
                    f"apply lock is held by pid {holder.owner_pid}; pid {pid} cannot release it",
                    holder,
                )
            self._conn.execute("DELETE FROM meta WHERE key = ?", (META_APPLY_LOCK,))
        return True

    def lock_holder(self) -> LockHolder | None:
        return self._lock_holder()

    def _lock_holder(self) -> LockHolder | None:
        raw = self.get_meta(META_APPLY_LOCK)
        if raw is None:
            return None
        try:
            document = json.loads(raw)
        except ValueError as exc:
            raise StateError(f"apply lock record is not valid JSON: {exc}") from exc
        if not isinstance(document, dict):
            raise StateError("apply lock record is not an object")
        entry = cast("dict[str, Any]", document)
        return LockHolder(
            owner_pid=_as_int(entry.get("owner_pid"), "owner_pid"),
            acquired_iso=_as_str(entry.get("acquired_iso"), "acquired_iso"),
            lease_expires_iso=_as_str(entry.get("lease_expires_iso"), "lease_expires_iso"),
        )


def _encode_holder(holder: LockHolder) -> str:
    return json.dumps(
        {
            "owner_pid": holder.owner_pid,
            "acquired_iso": holder.acquired_iso,
            "lease_expires_iso": holder.lease_expires_iso,
        },
        sort_keys=True,
    )


def _instruction_for(state: FileState) -> str:
    if state is FileState.PREPARED_NOT_SENT:
        return _RECOVERY_REPLAN_INSTRUCTION
    if state in (FileState.REQUEST_MAY_HAVE_BEEN_SENT, FileState.INDETERMINATE_WRITE):
        return _RECOVERY_HOLD_INSTRUCTION
    if state in (FileState.VERIFYING, FileState.PARTIAL):
        return "continue polling XRefs (verify_link) with the existing plan"
    if state is FileState.BLOCKED_SAFETY:
        return "stays blocked: re-evaluate only when the safety inputs are re-attested"
    if state is FileState.VERIFIED:
        return "audit-only: never relink"
    return "re-evaluate in the next cycle with fresh snapshots"


# ------------------------------------------------------------- module API


@contextlib.contextmanager
def open_state(
    path: str | Path, *, budget_limits: BudgetLimits | None = None
) -> Iterator[StateStore]:
    """Open (and initialize, if needed) the durable state store at ``path``.

    ``path`` is ``settings.database.path`` — a **local** persistent volume.
    SQLite on SMB/NFS is unsafe (locking is unreliable there): on Unraid use a
    local path or a cache-backed mount (documented in the module docstring and
    compose.example.yaml).
    """
    store = StateStore(path, budget_limits=budget_limits)
    try:
        yield store
    finally:
        store.close()


def acquire_apply_lock(
    store: StateStore, pid: int, iso: str, *, lease_expires_iso: str
) -> LockAcquisition:
    """Acquire the single-process apply lock, or raise :class:`LockHeldError` naming
    the current holder."""
    return store.acquire_apply_lock_tx(pid, iso, lease_expires_iso)


def acquire_or_steal_apply_lock(
    store: StateStore, pid: int, iso: str, *, lease_expires_iso: str
) -> LockAcquisition:
    """Acquire the apply lock, stealing an *expired* lease (audit-recorded); a live
    lease raises :class:`LockHeldError` naming the holder (P2-1 daemon path)."""
    return store.acquire_or_steal_apply_lock_tx(pid, iso, lease_expires_iso)


def release_apply_lock(store: StateStore, pid: int, iso: str) -> bool:
    """Release the lock when held by ``pid``; False when no lock is held."""
    return store.release_apply_lock_tx(pid, iso)


def steal_expired_apply_lock(
    store: StateStore, pid: int, iso: str, *, lease_expires_iso: str | None = None
) -> LockAcquisition:
    """Explicitly take over an expired lease (recovery of a crashed holder)."""
    return store.steal_expired_apply_lock_tx(pid, iso, lease_expires_iso)


def integrity_check(store: StateStore) -> IntegrityReport:
    """``PRAGMA integrity_check`` for CLI/M7. Corruption stops writes (fail closed)."""
    try:
        rows = store.connection.execute("PRAGMA integrity_check").fetchall()
    except sqlite3.DatabaseError as exc:
        store.disable_writes(f"integrity_check failed: {exc}")
        return IntegrityReport(False, f"integrity_check error: {exc}")
    detail = "; ".join(_as_str(row[0], "integrity_check row") for row in rows)
    ok = len(rows) == 1 and detail == "ok"
    if not ok:
        store.disable_writes(f"integrity_check reported: {detail}")
    return IntegrityReport(ok, detail)
