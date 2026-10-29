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

**Apply-ownership fence (F5, P1):** exclusive apply ownership is fenced by an OS
advisory lock file (``apply.lock``) in the SAME directory as the state DB, held for
the entire apply operation. The kernel frees it when the owning process exits, so a
crash recovers; a meta lease expiry on its own NEVER grants ownership, so a live
apply operation can never be fenced out by a fixed timeout. The meta ``apply_lock``
lease record is audit/visibility only. The same local-volume caveat applies:
``flock``/byte-range locks are unreliable on SMB/NFS, so the supported deployment is
one container on a local (or cache-backed) volume state directory.

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
path (verified, void, rejected/refused, and the operator ``resolve_send``)
funnels through :meth:`StateStore._release_send_slot_tx`. A partial or contradictory inspection
does not release a possible-delivery reservation; only an explicit operator resolution does.
A release attempt on a row whose
``slot_released=1`` is a **no-op** (recorded in the ``slot_release_noops`` meta audit list).
:func:`max` clamping in :meth:`StateStore._adjust_pending_tx` remains only as a defensive
backstop — the invariants must (and their tests do) hold **without** relying on it. An
unqualified PARTIAL observation keeps its slot held until a later VERIFIED result or
explicit resolution; a qualified PARTIAL/contradictory inspection becomes terminal review
and also keeps its slot held. The APIs that
observe the terminal facts are :meth:`StateStore.record_verification` (VERIFIED),
:meth:`StateStore.record_send_outcome` (REJECTED/REFUSED, pinned zero-write),
:meth:`StateStore.void_prepared_send` (definite not-sent) and
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
migration entry). M7-b (also schema v1, unshipped — no image shipped with v1 columns has
ever been released): ``campaigns.campaign_id`` becomes **TEXT** (the plan file's uuid4),
gains ``attestation_scope``/``approval_iso``/``status_reason``/``last_batch_iso`` and a
``status IN ('approved','paused','revoked')`` check; ``files.campaign_id`` is TEXT for the
same reason, and :meth:`StateStore.prepare_send` takes ``campaign_id: str | None``.
F7 (also schema v1, unshipped — no image shipped with v1 columns has ever been released):
``campaigns.plan_path`` records the canonical plan file path bound at approval, so the daemon
drains an approved campaign against the same file the ``plan_drift`` digest gate re-verifies;
it is appended to ``_SCHEMA_SQL`` (schema v1, unshipped). **F17 supersedes this framing:** the
schema shipped at v2 with a registered ``1 → 2`` migration. M3 adds only an onboarding table
through a registered ``2 → 3`` migration; no prior state is rebuilt or discarded.

**F17 (schema v2): the F10 drain-authorization columns ARE a schema change.** A database created
by the previous implementation carries ``user_version = 1`` **without** those columns, so
``_init_schema`` routes any existing lower version through :meth:`StateStore.migrate`: the
registered ``1 → 2`` step adds each missing drain column via ``ALTER TABLE campaigns ADD COLUMN``
(defaults NULL / NULL / '' — a migrated campaign is **never** auto-authorized; the operator
re-inspects the canary and re-grants), using ``PRAGMA table_info`` so an interim v1 database that
already has the columns upgrades cleanly. The step, the ``PRAGMA user_version=2`` bump and the
``schema_version`` audit row commit in **one transaction**: a failing step rolls back whole, the
store refuses to open with a recoverable :class:`StateError`, and the file stays usable at its
original version with every history row intact. At F17, fresh databases initialized directly at
v2; current fresh databases initialize at v9, and older databases upgrade in place. A
``user_version > SCHEMA_VERSION`` is refused (fail closed). The 5 → 6 migration does not infer
campaign ownership from mutable file rows; 6 → 7 records immutable per-send ownership. Legacy
attempts compatible with a persisted campaign approval stay unknown until explicit operator
resolution; campaign totals are reconciled monotonically only after ownership is established.

**M3 (schema v3):** the `onboarding` table stores scope/version/safety digests, explicit
operator confirmations, automatic-link consent, and pause state. The `2 → 3` migration adds
the table without changing any existing row.

**M5.1 (schema v4):** empty dashboard-account and dashboard-session tables are added by the
`3 → 4` migration. Existing installs never inherit an account or session.

**Metadata operations (schema v8):** a separate anime-level ledger shares the existing
reservation budget and write-stop fence without inventing file sends or campaign progress.
Possibly delivered identities survive restarts and consent changes; uncertain reservations
remain held until verified readiness.

**Trailing budgets (schema v5):** the `4 → 5` migration adds timestamped reservation
counts, recovers debt from send history, and retains anonymous legacy counter debt
conservatively from upgrade time. Release/void paths never refund rolling reservations;
new reservations and send intents commit together under ``BEGIN IMMEDIATE``.

F9 (also schema v1, unshipped — no released image exists): the per-cycle budget window_key is
made durable-unique. :meth:`StateStore.begin_cycle` allocates ``<label>-<run_id>-<seq>`` window
keys from two meta rows (``budget_run_id`` nonce + ``budget_cycle_seq`` monotonic counter), so
every reconciliation cycle and campaign batch gets a fresh budget row and a new process never
collides with a prior run's persisted counters. No new table columns are added — the
``budgets`` rows keyed by ``window_key`` and the global ``_global`` rolling row are unchanged in
shape; completed per-cycle rows beyond :data:`BUDGET_CYCLE_RETENTION` are pruned on cycle start.

**Operator-only indirect chain (P3-B):** the transition graph contains the path
``indeterminate_write → waiting → planned → prepared_not_sent → request_may_have_been_sent``,
so a held row can reach a re-submittable state **only via the operator's explicit
``waiting``/``planned`` decisions**. The daemon **never initiates** that chain from
``indeterminate_write``/``conflict``/``partial`` — those rows are verified/held, and terminal
partial or contradictory inspections keep their reservation for explicit operator resolution.
Closing a live-held terminal row is an operator action (``resolve_send``, M7 CLI).
**M7's CLI must not expose a "reset to planned" for a sent row**:
a ``request_may_have_been_sent``/``indeterminate_write`` row is never
rewound to ``planned`` directly, and resolving a sent row keeps the *same* request
fingerprint refused permanently; re-plan is allowed only as a **fresh**
``request_fingerprint`` (dedup retains every possibly-delivered outcome, including
operator-resolved rows).
"""

from __future__ import annotations

import contextlib
import functools
import hashlib
import json
import os
import sqlite3
import sys
import time
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Final, Literal, LiteralString, ParamSpec, TypeVar, cast

from .errors import LinkerError
from .linking import SendOutcome, VerificationVerdict
from .logging_setup import redact
from .mappings import EpisodeType
from .metadata_identity import metadata_source_key_for_url

# Platform shim for the OS advisory apply lock (the F5 ownership fence). The kernel
# releases an advisory lock when the owning process exits, so a crash frees it; a meta
# lease expiry on its own never grants ownership. Import is per-platform.
if sys.platform == "win32":
    import msvcrt  # Windows byte-range advisory locking (LK_NBLCK); import is per-platform

    ADVISORY_LOCK_KIND: Final[str] = "msvcrt.locking (LK_NBLCK, 1 byte)"

    def _acquire_advisory_lock(fd: int) -> None:
        msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)

    def _release_advisory_lock(fd: int) -> None:
        os.lseek(fd, 0, os.SEEK_SET)
        msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
else:
    import fcntl

    ADVISORY_LOCK_KIND: Final[str] = "fcntl.flock (LOCK_EX | LOCK_NB)"

    def _acquire_advisory_lock(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _release_advisory_lock(fd: int) -> None:
        fcntl.flock(fd, fcntl.LOCK_UN)


_O_BINARY: Final[int] = getattr(os, "O_BINARY", 0)

META_ONBOARDING_RECONFIRMATION_REQUIRED: Final[str] = "onboarding_reconfirmation_required"
META_SERVICE_SETTINGS: Final[str] = "normal_service_settings_v1"
SERVICE_AUTHORIZATION_REVISION_CHARS: Final[int] = 32
META_SERVICE_DRAFT_ROOTS: Final[str] = "normal_service_draft_roots_v1"
META_SERVICE_AUTHORIZATION_REVISION: Final[str] = "normal_service_authorization_revision_v1"
SYSTEM_PAUSE_REASONS: Final[frozenset[str]] = frozenset(
    {"setup_changed", "setup_unavailable", "unsafe_shoko_settings"}
)
SYSTEM_RECONFIRMATION_PAUSE_REASONS: Final[frozenset[str]] = frozenset(
    {"setup_changed", "unsafe_shoko_settings"}
)
SCHEMA_VERSION: Final[int] = 9
MAPPING_RESOLVED_ID_COUNT: Final[int] = 2  # AniDB episode ID, Shoko episode ID
META_MAPPING_TARGET_QUARANTINE_PREFIX: Final[str] = "file_mapping_quarantine:v1:"
DEFAULT_BUSY_TIMEOUT_MS: Final[int] = 5000
GLOBAL_BUDGET_KEY: Final[str] = "_global"
MAX_DETAIL_CHARS: Final[int] = 1000
DIGEST_HEX_CHARS: Final[int] = 64
TARGET_TRIPLE_LEN: Final[int] = 3
ANIDB_IDENTITY_FIELDS: Final[int] = 4
MAX_ONBOARDING_JSON_CHARS: Final[int] = 100_000
MAX_SERVICE_RATE_JSON_CHARS: Final[int] = 4096
MAX_DASHBOARD_USERNAME_CHARS: Final[int] = 64
MAX_DASHBOARD_PASSWORD_HASH_CHARS: Final[int] = 512
MAX_DASHBOARD_SESSIONS: Final[int] = 100
DASHBOARD_SIGNING_KEY_BYTES: Final[int] = 32

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
META_SOURCE_ANCHOR_BASIS: Final[str] = "source_anchor_verified_basis"
META_REQUIRES_REBASELINE: Final[str] = "requires_rebaseline"
META_APPLY_LOCK_STEALS: Final[str] = "apply_lock_steals"
META_LAST_REFUSAL: Final[str] = "last_refusal"
META_SLOT_RELEASE_NOOPS: Final[str] = "slot_release_noops"
META_GLOBAL_PAUSE_ORIGIN: Final[str] = "global_pause_origin"
META_BASELINE_COMPLETED_ISO: Final[str] = "baseline_completed_iso"
META_BUDGET_CYCLE_KEY: Final[str] = "budget_last_cycle_key"
#: Durable budget-cycle identity (F9). ``budget_run_id`` is a random nonce written once
#: per install; ``budget_cycle_seq`` is a monotonic counter advanced at every
#: reconciliation-cycle / campaign-batch start. Combined with the label prefix they give every
#: executing cycle/batch a window_key that is unique across process restarts, so a new
#: process never reuses a prior process's persisted per-cycle budget counters.
META_BUDGET_RUN_ID: Final[str] = "budget_run_id"
META_BUDGET_SEQ: Final[str] = "budget_cycle_seq"
#: Bounded retention for *completed* per-cycle budget rows: on every cycle/batch start
#: the newest ``BUDGET_CYCLE_RETENTION`` window rows (plus the global rolling row and the
#: freshly allocated current window) are kept; older per-cycle counters are pruned so a
#: multi-thousand-file campaign never grows the ``budgets`` table without bound.
BUDGET_CYCLE_RETENTION: Final[int] = 64
META_LAST_CYCLE_START_ISO: Final[str] = "last_cycle_start_iso"
META_LAST_CYCLE_END_ISO: Final[str] = "last_cycle_end_iso"
META_LAST_DISCOVERY_EXCLUSIONS: Final[str] = "last_discovery_exclusions:v1"
META_NEXT_CYCLE_DUE_ISO: Final[str] = "next_cycle_due_iso"
META_LAST_FULL_RECONCILE_ISO: Final[str] = "last_full_reconcile_iso"
META_NEXT_FULL_RECONCILE_ISO: Final[str] = "next_full_reconcile_iso"
META_SERVICE_CANARY_FILE_ID: Final[str] = "service_canary_file_id"
META_SERVICE_CANARY_STATUS: Final[str] = "service_canary_status"
META_SERVICE_CANARY_UPDATED_ISO: Final[str] = "service_canary_updated_iso"
META_SERVICE_CANARY_SEND_ID: Final[str] = "service_canary_send_id"
META_RECOVERY_CURSOR: Final[str] = "recovery_cursor:v1"
META_SERVICE_RATE_STATE: Final[str] = "service_rate_state"
META_SONARR_INVENTORY_MANIFEST: Final[str] = "sonarr_inventory_manifest:v1"
META_SONARR_INVENTORY_CURSOR: Final[str] = "sonarr_inventory_cursor:v1"
META_SONARR_INVENTORY_SWEEP: Final[str] = "sonarr_inventory_sweep:v1"
META_SONARR_INVENTORY_FRAGMENT_PREFIX: Final[str] = "sonarr_inventory_fragment:v1:"
META_SERVICE_MAPPING_DRIFT_PREFIX: Final[str] = "service_mapping_drift:"
SERVICE_MAPPING_DRIFT_SCHEMA_V1: Final[str] = "shoko-sonarr-linker/mapping-drift/1"
SERVICE_MAPPING_DRIFT_SCHEMA: Final[str] = "shoko-sonarr-linker/mapping-drift/2"
META_ATTESTATION_GATE: Final[str] = "attestation_gate"
#: Per-campaign attestation gate bookkeeping key prefix (F8: campaign-scope lease
#: transitions are tracked per campaign, never as the global daemon write pause).
META_CAMPAIGN_ATTESTATION_GATE: Final[str] = "campaign_attestation_gate"
#: Per-campaign zero-progress breaker counter key prefix (F7): consecutive drain cycles with
#: zero real progress. Cleared when the campaign reaches a terminal state (plan complete/revoke).
META_CAMPAIGN_ZERO_PROGRESS: Final[str] = "campaign_zero_progress"
#: F18: per-campaign RECOVERABLE drain deferral (an ISO retry time, the earliest rolling-budget
#: slot expiry for a budget-blocked campaign). A deferred campaign stays APPROVED and
#: drain-authorized — it is skipped for the cycles inside the cooldown and resumes
#: automatically once the window permits. Durable meta (survives restart); cleared on real
#: progress and at terminal states. The zero-progress breaker never counts these cycles.
META_CAMPAIGN_DEFERRED_UNTIL: Final[str] = "campaign_deferred_until"
ATTESTATION_PAUSE_REASON: Final[str] = "attestation_expired"

#: OS advisory lock file (the F5 ownership fence) living in the SAME
#: persistent directory as the SQLite state DB. Its name is deliberately obvious.
APPLY_LOCK_FILENAME: Final[str] = "apply.lock"

_SYNC_NAMES: Final[Mapping[int, str]] = {0: "off", 1: "normal", 2: "full", 3: "extra"}

OUTCOME_PREPARED: Final[str] = "prepared"
OUTCOME_ATTEMPTED: Final[str] = "attempted"
OUTCOME_INSPECTED_REVIEW: Final[str] = "inspected_review"
OUTCOME_VOIDED: Final[str] = "voided"
OUTCOME_VERIFIED: Final[str] = "verified"
OUTCOME_RESOLVED: Final[str] = "resolved"
CAMPAIGN_OWNERSHIP_CAMPAIGN: Final[str] = "campaign"
CAMPAIGN_OWNERSHIP_NON_CAMPAIGN: Final[str] = "non_campaign"
CAMPAIGN_OWNERSHIP_UNKNOWN: Final[str] = "unknown"
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
# Resolve-gate (round-2 review F2): only a durable, identity-qualified terminal inspection is
# operator-closable. An uninspected attempted/accepted/ambiguous send remains held.
RESOLVE_LEGAL_OUTCOMES: Final[frozenset[str]] = frozenset({OUTCOME_INSPECTED_REVIEW})

_RECOVERY_HOLD_INSTRUCTION: Final[str] = (
    "possible delivery: verify with GET File/{id}?include=XRefs (M5 verify_link) and "
    "Shoko's queue; HOLD — never resubmit automatically"
)
_RECOVERY_REPLAN_INSTRUCTION: Final[str] = (
    "definitely not sent: replan with fresh checks (release the slot, re-prepare)"
)


class StateError(LinkerError):
    """The state store was asked something it must refuse (bad key, bad fingerprint)."""


class CampaignDrainRefusedError(StateError):
    """The atomic drain grant found stale approval or unsettled durable sends."""


class CampaignSendRefusedError(StateError):
    """A campaign safety recheck definitively refused a not-yet-attempted request."""


class CampaignSubmissionLimitError(CampaignSendRefusedError):
    """A campaign's durable attempted-send ceiling refused a prepared request."""


class TransitionError(StateError):
    """A state transition is not on the allowed edge list, or the row is missing."""


class WriteDisabledError(StateError):
    """Writes are stopped (corruption/disk-full/commit failure); reads remain allowed."""


class LockHeldError(StateError):
    """The apply lock is held; the holder is attached for reporting."""

    def __init__(self, message: str, holder: LockHolder | None) -> None:
        super().__init__(message)
        self.holder = holder


class ApplyLockContendedError(StateError):
    """The OS advisory apply lock is held by a live process (context-manager path)."""

    def __init__(self, lock_path: Path) -> None:
        super().__init__(
            f"apply lock contended: the OS advisory lock {lock_path} is held by a live process"
        )
        self.lock_path = lock_path


class ApplyLockUnavailableError(StateError):
    """The apply lock file cannot be opened (P3-1): a filesystem failure, NOT contention.

    Apply paths must defer (read-only cycle, named reason) — never crash the daemon."""


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


# Ordinary status presents every persisted file in exactly one actionable group.
# REQUEST_MAY_HAVE_BEEN_SENT is recoverable through the durable verification/hold path,
# so it remains waiting; partial and indeterminate results are surfaced for review.
_SERVICE_WAITING_FILE_STATES: Final[frozenset[FileState]] = frozenset(
    {
        FileState.DISCOVERED,
        FileState.MATCHED,
        FileState.MAPPED,
        FileState.PLANNED,
        FileState.PREPARED_NOT_SENT,
        FileState.REQUEST_MAY_HAVE_BEEN_SENT,
        FileState.VERIFYING,
        FileState.WAITING,
    }
)
_SERVICE_REVIEW_FILE_STATES: Final[frozenset[FileState]] = frozenset(
    {
        FileState.AMBIGUOUS,
        FileState.CONFLICT,
        FileState.PARTIAL,
        FileState.INDETERMINATE_WRITE,
        FileState.BLOCKED_SAFETY,
    }
)


@dataclass(frozen=True)
class ServiceFileCounts:
    """Persisted-file counts for the ordinary dashboard and ``status`` command."""

    verified_files: int
    waiting_files: int
    review_files: int


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
    campaign_id: str | None
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
class ServiceMappingDrift:
    """Persistent review marker for a verified Shoko file whose mapping identity changed."""

    shoko_file_id: int
    previous_targets_digest: str | None
    active_targets_digest: str
    previous_structure_digest: str | None
    active_structure_digest: str
    mapping_revision: str
    detected_iso: str
    reason: str = "mapping_drift_details_unavailable"


@dataclass(frozen=True)
class DashboardAccount:
    """First dashboard user; credential hash and signing key are redacted from repr."""

    username: str
    password_hash: str = field(repr=False)
    signing_key: bytes = field(repr=False)
    created_iso: str


@dataclass(frozen=True)
class DashboardSession:
    """Expiring login record stores only session/CSRF hashes, never bearer values."""

    session_id_hash: str = field(repr=False)
    csrf_hash: str = field(repr=False)
    expires_epoch: int
    created_iso: str


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
    # Independent (reservation cap, trailing hours) ceilings over the same ledger.
    additional_rolling_limits: tuple[tuple[int, float], ...] = ()


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


MetadataEpisodeIdentity = tuple[int, EpisodeType, int]
_METADATA_TARGET_FIELDS: Final[int] = 3


class MetadataOperationState(str, Enum):
    PREPARED = "prepared"
    POSSIBLY_DELIVERED = "possibly_delivered"
    ACCEPTED_WAITING = "accepted_waiting"
    READY = "ready"
    DEFINITELY_REJECTED = "definitely_rejected"
    NEEDS_ATTENTION = "needs_attention"


@dataclass(frozen=True)
class MetadataOperation:
    """Durable anime-level identity; claim fences stale senders and unsent retries.

    No state with possible delivery is eligible for automatic replay, even after
    consent changes. External observations own no reservation. Deadlines are supplied
    by the caller, never extended by polling. Fingerprints contain no raw endpoints.
    """

    source_key: str
    anidb_anime_id: int
    claim: str
    demand_fingerprint: str
    request_fingerprint: str
    request_version: int
    state: MetadataOperationState
    created_iso: str
    updated_iso: str
    attempted_iso: str | None
    verification_deadline_iso: str | None
    transport_admitted: bool
    reservation_owned: bool
    slot_released: bool
    required_targets: tuple[MetadataEpisodeIdentity, ...] | None = None
    missing_targets: tuple[MetadataEpisodeIdentity, ...] | None = None
    verified_required_targets: tuple[MetadataEpisodeIdentity, ...] | None = None


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
    reservation_events: tuple[tuple[float, int], ...] | None = None

    def rolling_count(self, limits: BudgetLimits, now_iso: str) -> int:
        """Reservations in (now - window, now], including future clock-skew debt."""
        cutoff = (_parse_iso(now_iso) - timedelta(hours=limits.rolling_window_hours)).timestamp()
        if self.reservation_events is not None:
            return sum(count for stamp, count in self.reservation_events if stamp > cutoff)
        # Compatibility for callers constructing a historical counter-only view.
        start = self.rolling_window_start_iso
        return (
            self.rolling_new_submissions
            if start is None or _parse_iso(start).timestamp() > cutoff
            else 0
        )

    def rolling_exhausted(self, limits: BudgetLimits, now_iso: str) -> bool:
        """Every configured and immutable rolling ceiling must have headroom."""
        return any(
            self.rolling_count(replace(limits, rolling_window_hours=hours), now_iso) >= cap
            for cap, hours in (
                (limits.rolling_new, limits.rolling_window_hours),
                *limits.additional_rolling_limits,
            )
        )

    def rolling_retry_iso(self, limits: BudgetLimits, now_iso: str) -> str | None:
        """Wait until ALL trailing ceilings leave room, not merely the shortest one."""
        retries = [
            retry
            for cap, hours in (
                (limits.rolling_new, limits.rolling_window_hours),
                *limits.additional_rolling_limits,
            )
            if (
                retry := self._window_retry_iso(
                    replace(limits, rolling_new=cap, rolling_window_hours=hours), now_iso
                )
            )
            is not None
        ]
        return max(retries, key=_parse_iso) if retries else None

    def _window_retry_iso(self, limits: BudgetLimits, now_iso: str) -> str | None:
        """First expiry leaving room for one reservation, even after a cap reduction."""
        count = self.rolling_count(limits, now_iso)
        if count < limits.rolling_new:
            return None
        events = self.reservation_events
        if events is None:
            start = self.rolling_window_start_iso
            return (
                None
                if start is None
                else (_parse_iso(start) + timedelta(hours=limits.rolling_window_hours)).isoformat()
            )
        cutoff = (_parse_iso(now_iso) - timedelta(hours=limits.rolling_window_hours)).timestamp()
        for stamp, weight in events:
            if stamp <= cutoff:
                continue
            count -= weight
            if count < limits.rolling_new:
                return (
                    datetime.fromtimestamp(stamp, UTC)
                    + timedelta(hours=limits.rolling_window_hours)
                ).isoformat()
        return None


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
class SendHistoryRow:
    """F19: one durable ``sends``-ledger row of a campaign's attempted-send history.

    The campaign drain-authorization gate reads this history (not current file states): a
    REJECTED/REFUSED send maps its file back to ``waiting`` while the row stays here, so
    every attempted send remains visible and accountable."""

    send_id: int
    shoko_file_id: int
    outcome: str
    verification: str | None
    attempted_iso: str
    http_status: int | None
    file_state: FileState


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
class RecoveryBatch:
    """Bounded identities plus a SQL count, without decoding persisted payloads."""

    ids: tuple[int, ...]
    total: int


@dataclass(frozen=True)
class EligibilityVerdict:
    """Composed auto-apply eligibility with every refusal reason recorded."""

    eligible: bool
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class CampaignBinding:
    """One approved plan-file entry bound to a campaign (fingerprint contract)."""

    shoko_file_id: int
    plan_fingerprint: str
    request_fingerprint: str


CAMPAIGN_STATUS_APPROVED: Final[str] = "approved"
CAMPAIGN_BINDING_FIELDS: Final[int] = 3
_LEGACY_CAMPAIGN_SNAPSHOT_FIELDS: Final[int] = 7
CAMPAIGN_PLAN_PATH_COLUMN: Final[int] = 12
CAMPAIGN_DRAIN_AUTHORIZED_ISO_COLUMN: Final[int] = 13
CAMPAIGN_DRAIN_AUTHORIZED_DIGEST_COLUMN: Final[int] = 14
CAMPAIGN_DRAIN_INVALIDATED_REASON_COLUMN: Final[int] = 15
CAMPAIGN_DRAIN_REVOKED_REASON: Final[str] = "operator_revoked"
CAMPAIGN_DRAIN_FINGERPRINT_REASON: Final[str] = "plan_fingerprint_set_changed"
CAMPAIGN_STATUS_PAUSED: Final[str] = "paused"
CAMPAIGN_STATUS_REVOKED: Final[str] = "revoked"
CAMPAIGN_QUARANTINE_PROGRESS_PENDING_MARKER: Final[str] = ":campaign_progress_pending:"
CAMPAIGN_QUARANTINE_PROGRESS_RECORDED_MARKER: Final[str] = ":campaign_progress_recorded:"
CAMPAIGN_STATUSES: Final[frozenset[str]] = frozenset(
    {CAMPAIGN_STATUS_APPROVED, CAMPAIGN_STATUS_PAUSED, CAMPAIGN_STATUS_REVOKED}
)
CAMPAIGN_QUEUE_STATES: Final[frozenset[FileState]] = frozenset(
    {FileState.PLANNED, FileState.MAPPED}
)


@dataclass(frozen=True)
class CampaignRecord:
    """One persisted campaign: approval binding, totals, progress and status."""

    campaign_id: str
    digest: str
    max_submissions: int
    deadline_iso: str | None
    attestation_scope: str
    approval_iso: str
    status: str
    status_reason: str
    total_sent: int
    last_batch_iso: str | None
    created_iso: str
    bindings: tuple[CampaignBinding, ...]
    plan_path: str | None = None
    drain_authorized_iso: str | None = None
    drain_authorized_digest: str | None = None
    drain_invalidated_reason: str = ""


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


class ApplyProcessLock:
    """OS advisory lock file held for the duration of one apply ownership (the F5 fence).

    ``acquire()`` is **non-blocking**: it opens ``apply.lock`` (in the same directory as
    the state DB) and takes the platform advisory lock. Contention (a live owner — a
    same-process re-acquire, a second process, a restart race) returns ``False``; the
    kernel frees the lock when the owning process exits, so a crash recovers automatically.
    A meta lease expiry on its own NEVER grants ownership — takeover requires this OS
    lock to be acquirable.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        self._fd: int | None = None
        self.held = False
        self.owner_process: int | None = None

    def acquire(self) -> bool:
        """Non-blocking take. ``False`` on contention (never raises on a busy lock).

        Filesystem failures (open/create/mkdir) raise :class:`ApplyLockUnavailableError`
        (P3-1) so apply paths defer with a named reason instead of crashing the loop.
        """
        if self.held:
            return False
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(str(self.path), os.O_RDWR | os.O_CREAT | _O_BINARY, 0o600)
        except OSError as exc:
            raise ApplyLockUnavailableError(
                f"apply lock file {self.path} cannot be opened ({exc}); deferring apply"
            ) from exc
        try:
            _acquire_advisory_lock(fd)
        except OSError:
            os.close(fd)
            return False
        self._fd = fd
        self.held = True
        self.owner_process = os.getpid()
        return True

    def release(self) -> None:
        """Release the advisory lock and close the descriptor (idempotent, never raises)."""
        fd = self._fd
        self._fd = None
        self.held = False
        self.owner_process = None
        if fd is None:
            return
        with contextlib.suppress(OSError):
            _release_advisory_lock(fd)
        os.close(fd)

    def __enter__(self) -> ApplyProcessLock:
        if not self.acquire():
            raise ApplyLockContendedError(self.path)
        return self

    def __exit__(self, *_exc: object) -> None:
        self.release()


# ----------------------------------------------------------------- schema


_METADATA_SCHEMA_SQL: Final[str] = """
CREATE TABLE metadata_operations (
    source_key TEXT NOT NULL CHECK (length(source_key) = 64),
    anidb_anime_id INTEGER NOT NULL CHECK (anidb_anime_id > 0),
    claim TEXT NOT NULL,
    demand_fingerprint TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    request_version INTEGER NOT NULL CHECK (request_version > 0),
    state TEXT NOT NULL CHECK (state IN ('prepared', 'possibly_delivered',
        'accepted_waiting', 'ready', 'definitely_rejected', 'needs_attention')),
    created_iso TEXT NOT NULL,
    updated_iso TEXT NOT NULL,
    attempted_iso TEXT,
    verification_deadline_iso TEXT,
    transport_admitted INTEGER NOT NULL DEFAULT 0 CHECK (transport_admitted IN (0, 1)),
    reservation_owned INTEGER NOT NULL CHECK (reservation_owned IN (0, 1)),
    slot_released INTEGER NOT NULL DEFAULT 0 CHECK (slot_released IN (0, 1)),
    PRIMARY KEY (source_key, anidb_anime_id)
);
"""


_METADATA_V9_SCHEMA_SQL: Final[str] = """
ALTER TABLE metadata_operations ADD COLUMN required_targets_json TEXT;
ALTER TABLE metadata_operations ADD COLUMN missing_targets_json TEXT;
ALTER TABLE metadata_operations ADD COLUMN verified_targets_json TEXT;
CREATE TABLE metadata_operation_history (
    source_key TEXT NOT NULL,
    anidb_anime_id INTEGER NOT NULL,
    claim TEXT PRIMARY KEY,
    demand_fingerprint TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    request_version INTEGER NOT NULL,
    state TEXT NOT NULL CHECK (state = 'ready'),
    created_iso TEXT NOT NULL,
    updated_iso TEXT NOT NULL,
    attempted_iso TEXT,
    verification_deadline_iso TEXT,
    transport_admitted INTEGER NOT NULL,
    reservation_owned INTEGER NOT NULL,
    slot_released INTEGER NOT NULL CHECK (slot_released = 1),
    required_targets_json TEXT NOT NULL,
    missing_targets_json TEXT NOT NULL,
    verified_targets_json TEXT NOT NULL
);
CREATE INDEX idx_metadata_history_identity
    ON metadata_operation_history (source_key, anidb_anime_id);
"""


_SCHEMA_SQL: Final[str] = (
    """
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
    campaign_id TEXT,
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
    campaign_id TEXT,
    campaign_ownership TEXT NOT NULL DEFAULT 'unknown'
        CHECK (campaign_ownership IN ('campaign', 'non_campaign', 'unknown')),
    campaign_ownership_resolved_iso TEXT,
    campaign_ownership_note TEXT NOT NULL DEFAULT '',
    attempted_iso TEXT NOT NULL,
    http_status INTEGER,
    outcome TEXT NOT NULL,
    verified_iso TEXT,
    verification TEXT,
    episode_jobs INTEGER NOT NULL DEFAULT 1,
    target_ids_json TEXT NOT NULL DEFAULT '[]',
    slot_released INTEGER NOT NULL DEFAULT 0 CHECK (slot_released IN (0, 1))
);
-- F10 drain authorization (schema v2 — F17: existing v1 databases gain these columns via the
-- registered 1→2 migration; fresh installs create them): the daemon
-- drains an APPROVED campaign only with the operator's explicit confirmation of a settled
-- (all-VERIFIED) canary. drain_authorized_iso + drain_authorized_digest record the grant
-- (bound to the campaign's approval digest: a changed digest invalidates selection);
-- drain_invalidated_reason records why an authorization stopped being valid
-- ('operator_revoked' / 'plan_fingerprint_set_changed'). Manual campaign apply never
-- requires it; only the automatic daemon drain does.
CREATE TABLE campaigns (
    campaign_id TEXT PRIMARY KEY,
    digest TEXT NOT NULL,
    plan_fingerprint_set TEXT NOT NULL DEFAULT '[]',
    max_submissions INTEGER NOT NULL,
    deadline_iso TEXT,
    attestation_scope TEXT NOT NULL DEFAULT '',
    approval_iso TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'approved'
        CHECK (status IN ('approved', 'paused', 'revoked')),
    status_reason TEXT NOT NULL DEFAULT '',
    total_sent INTEGER NOT NULL DEFAULT 0,
    last_batch_iso TEXT,
    created_iso TEXT NOT NULL,
    plan_path TEXT,
    drain_authorized_iso TEXT,
    drain_authorized_digest TEXT,
    drain_invalidated_reason TEXT NOT NULL DEFAULT ''
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
CREATE TABLE onboarding (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    document_json TEXT,
    paused INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)),
    pause_reason TEXT NOT NULL DEFAULT '',
    updated_iso TEXT NOT NULL
);
CREATE TABLE dashboard_account (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    username TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    signing_key BLOB NOT NULL CHECK (length(signing_key) = 32),
    created_iso TEXT NOT NULL
);
CREATE TABLE dashboard_sessions (
    session_id_hash TEXT PRIMARY KEY CHECK (length(session_id_hash) = 64),
    csrf_hash TEXT NOT NULL CHECK (length(csrf_hash) = 64),
    expires_epoch INTEGER NOT NULL,
    created_iso TEXT NOT NULL
);
CREATE INDEX idx_files_state ON files (state);
CREATE INDEX idx_files_cohort ON files (cohort);
CREATE INDEX idx_files_campaign ON files (campaign_id);
CREATE INDEX idx_files_path ON files (sonarr_path);
CREATE INDEX idx_files_series_file ON files (sonarr_series_id, sonarr_file_id);
CREATE TABLE budget_reservations (
    reserved_epoch REAL PRIMARY KEY,
    reservations INTEGER NOT NULL CHECK (reservations > 0)
);
CREATE INDEX idx_sends_request_fingerprint ON sends (request_fingerprint);
CREATE INDEX idx_sends_file ON sends (shoko_file_id);
CREATE INDEX idx_baseline_files_identity ON baseline_files (sonarr_series_id, sonarr_file_id);
CREATE INDEX idx_dashboard_sessions_expiry ON dashboard_sessions (expires_epoch);
"""
    + _METADATA_SCHEMA_SQL
    + _METADATA_V9_SCHEMA_SQL
)


def _execute_schema_statements(conn: sqlite3.Connection, script: str) -> None:
    """Execute schema DDL one statement at a time without leaving its transaction."""
    statement = ""
    for line in script.splitlines(keepends=True):
        statement += line
        if sqlite3.complete_statement(statement):
            if statement.strip():
                conn.execute(statement)
            statement = ""
    if statement.strip():
        raise sqlite3.DatabaseError("schema SQL contains an incomplete statement")


#: The three campaigns columns that distinguish v2 from v1 (column name → ADD COLUMN DDL;
#: kept in sync with the tail of ``campaigns`` in ``_SCHEMA_SQL``).
_CAMPAIGN_V2_DRAIN_COLUMNS: Final[tuple[tuple[str, str], ...]] = (
    ("drain_authorized_iso", "TEXT"),
    ("drain_authorized_digest", "TEXT"),
    ("drain_invalidated_reason", "TEXT NOT NULL DEFAULT ''"),
)


def _migrate_v1_to_v2(conn: sqlite3.Connection) -> None:
    """F17: add the F10 drain-authorization columns to a schema-v1 ``campaigns`` table.

    ``PRAGMA table_info`` is consulted per column so an interim v1 database created with the
    columns already present upgrades without a duplicate-column error. The defaults are
    NULL / NULL / '' — migrated campaigns are **not** drain-authorized, and no existing row is
    rewritten: durable submission history, budgets and approvals survive untouched.
    """
    existing = {str(row[1]) for row in conn.execute("PRAGMA table_info(campaigns)")}
    for name, column_ddl in _CAMPAIGN_V2_DRAIN_COLUMNS:
        if name not in existing:
            conn.execute(f"ALTER TABLE campaigns ADD COLUMN {name} {column_ddl}")


def _migrate_v2_to_v3(conn: sqlite3.Connection) -> None:
    """M3: add durable onboarding state without rewriting existing files or sends."""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS onboarding ("
        "singleton INTEGER PRIMARY KEY CHECK (singleton = 1), "
        "document_json TEXT, "
        "paused INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)), "
        "pause_reason TEXT NOT NULL DEFAULT '', "
        "updated_iso TEXT NOT NULL)"
    )
    actual_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(onboarding)")}
    expected_columns = {
        "singleton",
        "document_json",
        "paused",
        "pause_reason",
        "updated_iso",
    }
    if actual_columns != expected_columns:
        raise sqlite3.DatabaseError("onboarding table has an unexpected schema")


def _migrate_v3_to_v4(conn: sqlite3.Connection) -> None:
    """M5.1: add empty dashboard identity/session tables; never inherit an account."""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS dashboard_account ("
        "singleton INTEGER PRIMARY KEY CHECK (singleton = 1), "
        "username TEXT NOT NULL, "
        "password_hash TEXT NOT NULL, "
        "signing_key BLOB NOT NULL CHECK (length(signing_key) = 32), "
        "created_iso TEXT NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS dashboard_sessions ("
        "session_id_hash TEXT PRIMARY KEY CHECK (length(session_id_hash) = 64), "
        "csrf_hash TEXT NOT NULL CHECK (length(csrf_hash) = 64), "
        "expires_epoch INTEGER NOT NULL, "
        "created_iso TEXT NOT NULL)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_dashboard_sessions_expiry"
        " ON dashboard_sessions (expires_epoch)"
    )
    expected = {
        "dashboard_account": {
            "singleton",
            "username",
            "password_hash",
            "signing_key",
            "created_iso",
        },
        "dashboard_sessions": {"session_id_hash", "csrf_hash", "expires_epoch", "created_iso"},
    }
    for table, columns in expected.items():
        actual = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
        if actual != columns:
            raise sqlite3.DatabaseError(f"{table} table has an unexpected schema")
    account_info = {
        str(row[1]): _as_int(row[5], "dashboard_account.primary_key")
        for row in conn.execute("PRAGMA table_info(dashboard_account)")
    }
    session_info = {
        str(row[1]): _as_int(row[5], "dashboard_sessions.primary_key")
        for row in conn.execute("PRAGMA table_info(dashboard_sessions)")
    }
    if account_info["singleton"] != 1 or session_info["session_id_hash"] != 1:
        raise sqlite3.DatabaseError("dashboard tables are missing their required primary keys")
    if conn.execute("SELECT 1 FROM dashboard_account LIMIT 1").fetchone() is not None:
        raise sqlite3.DatabaseError("dashboard account must not be inherited during migration")
    if conn.execute("SELECT 1 FROM dashboard_sessions LIMIT 1").fetchone() is not None:
        raise sqlite3.DatabaseError("dashboard sessions must not be inherited during migration")


def _migrate_v4_to_v5(conn: sqlite3.Connection) -> None:
    """Recover timestamped send debt, conservatively retaining untraceable reservations.

    Old fixed counters can include anonymous reservations with no timestamp. Keep that
    excess for a full window from upgrade; never forgive recent sends at an old boundary.
    All sends (including rejected/voided) represent consumed reservations, not refunds.
    """
    conn.execute(
        "CREATE TABLE IF NOT EXISTS budget_reservations (reserved_epoch REAL PRIMARY KEY,"
        " reservations INTEGER NOT NULL CHECK (reservations > 0))"
    )
    if conn.execute("SELECT 1 FROM budget_reservations LIMIT 1").fetchone() is not None:
        raise sqlite3.DatabaseError("unexpected preexisting reservation history during upgrade")
    stamps = [
        _parse_iso(str(row[0])).timestamp()
        for row in conn.execute("SELECT attempted_iso FROM sends")
    ]
    for stamp in stamps:
        conn.execute(
            "INSERT INTO budget_reservations VALUES (?, 1) ON CONFLICT (reserved_epoch)"
            " DO UPDATE SET reservations = reservations + 1",
            (stamp,),
        )
    row = conn.execute(
        "SELECT rolling_new_submissions, rolling_window_start_iso"
        " FROM budgets WHERE window_key = ?",
        (GLOBAL_BUDGET_KEY,),
    ).fetchone()
    if row is not None:
        start = _parse_iso(str(row[1])).timestamp() if row[1] is not None else float("-inf")
        debt = max(0, int(row[0] or 0) - sum(stamp >= start for stamp in stamps))
        if debt:
            conn.execute(
                "INSERT INTO budget_reservations VALUES (?, ?) ON CONFLICT (reserved_epoch)"
                " DO UPDATE SET reservations = reservations + excluded.reservations",
                (datetime.now(UTC).timestamp(), debt),
            )


def _migrate_v5_to_v6(conn: sqlite3.Connection) -> None:
    """Keep v5→v6 data intact; ownership-safe counter recovery is performed in v7.

    Counting through ``files.campaign_id`` is unsafe because rebinding a file can retroactively
    move old sends between campaigns. The v7 migration can use durable approved binding snapshots
    and event times to identify possible owners, but matching fingerprints alone is not proof of
    campaign ownership.
    """


def _migrate_v6_to_v7(conn: sqlite3.Connection) -> None:
    """Persist send ownership and conservatively backfill legacy ledger rows.

    A campaign whose approved file and approval/deadline window cover a send makes it a *possible*
    owner, even if runtime fingerprints differ: pending-target materialization and unrelated
    mapping refreshes can legitimately change those hashes. Therefore legacy attempted sends in
    any such scope remain ``unknown`` for operator adjudication. A send is safely classified as
    non-campaign only when every persisted campaign approval snapshot is valid and none covered
    its file at the attempt time. Prepared and voided rows have no attempted-send debt and remain
    unknown, so a legacy prepared row cannot be transported under inferred ownership after
    upgrade.

    This intentionally never consults the mutable ``files.campaign_id`` or a plan file path.
    """
    existing_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(sends)")}
    additions = (
        ("campaign_id", "TEXT"),
        (
            "campaign_ownership",
            "TEXT NOT NULL DEFAULT 'unknown' CHECK"
            " (campaign_ownership IN ('campaign', 'non_campaign', 'unknown'))",
        ),
        ("campaign_ownership_resolved_iso", "TEXT"),
        ("campaign_ownership_note", "TEXT NOT NULL DEFAULT ''"),
    )
    for name, ddl in additions:
        if name not in existing_columns:
            conn.execute(f"ALTER TABLE sends ADD COLUMN {name} {ddl}")

    campaign_rows = conn.execute(
        "SELECT campaign_id, digest, attestation_scope, approval_iso, deadline_iso, created_iso,"
        " plan_fingerprint_set FROM campaigns"
    ).fetchall()
    rows = conn.execute(
        "SELECT send_id, shoko_file_id, plan_fingerprint, request_fingerprint, attempted_iso,"
        " outcome FROM sends WHERE campaign_ownership = 'unknown'"
    ).fetchall()
    for send_id_raw, file_id_raw, plan_fp_raw, request_fp_raw, attempted_raw, outcome_raw in rows:
        send_id = _as_int(send_id_raw, "send_id")
        outcome = _as_str(outcome_raw, "send outcome")
        if outcome in (OUTCOME_PREPARED, OUTCOME_VOIDED):
            continue
        possible_owners, snapshots_valid = _legacy_campaign_owner_candidates(
            shoko_file_id=file_id_raw,
            plan_fingerprint=plan_fp_raw,
            request_fingerprint=request_fp_raw,
            attempted_iso=attempted_raw,
            campaign_rows=campaign_rows,
        )
        if snapshots_valid and not possible_owners:
            conn.execute(
                "UPDATE sends SET campaign_id = NULL, campaign_ownership = ? WHERE send_id = ?",
                (CAMPAIGN_OWNERSHIP_NON_CAMPAIGN, send_id),
            )
        # Any eligible file/time overlap, invalid timestamp, or incomplete approval snapshot
        # remains explicitly unknown; the durable hold prevents undercounted writes.

    conn.execute(
        "UPDATE campaigns SET total_sent = MAX(total_sent, ("
        " SELECT COUNT(*) FROM sends s"
        " WHERE s.campaign_ownership = 'campaign'"
        " AND s.campaign_id = campaigns.campaign_id"
        " AND s.outcome NOT IN ('prepared', 'voided')"
        "))"
    )


def _ownership_timestamp(raw: Any) -> datetime | None:
    """Parse a timezone-aware legacy ownership timestamp; missing/naive values are uncertain."""
    if not isinstance(raw, str) or not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


def _legacy_campaign_owner_candidates(
    *,
    shoko_file_id: Any,
    plan_fingerprint: Any,
    request_fingerprint: Any,
    attempted_iso: Any,
    campaign_rows: Sequence[Any],
) -> tuple[tuple[str, ...], bool]:
    """Return possible owners and whether every persisted approval snapshot is valid.

    Each campaign row is ``(id, approval digest, attestation scope, approval time, deadline,
    created time, approved binding JSON)``. The routine is shared by migration and manual
    resolution so neither path can use a looser file/time owner test. A campaign whose persisted
    binding contains this file in its active approval window remains a possible owner regardless
    of fingerprint equality; target identities are validated against its digest-bound plan.
    """
    event_at = _ownership_timestamp(attempted_iso)
    if (
        event_at is None
        or isinstance(shoko_file_id, bool)
        or not isinstance(shoko_file_id, int)
        or shoko_file_id < 1
        or not isinstance(plan_fingerprint, str)
        or not isinstance(request_fingerprint, str)
        or len(plan_fingerprint) != DIGEST_HEX_CHARS
        or len(request_fingerprint) != DIGEST_HEX_CHARS
        or any(char not in "0123456789abcdef" for char in plan_fingerprint + request_fingerprint)
    ):
        return (), False

    possible_owners: list[str] = []
    evidence_complete = True
    for row in campaign_rows:
        snapshot = _parse_legacy_campaign_snapshot(row)
        if snapshot is None:
            evidence_complete = False
            continue
        campaign_id, approved_at, deadline_at, bindings = snapshot
        if approved_at > event_at or (deadline_at is not None and event_at > deadline_at):
            continue
        if any(binding[0] == shoko_file_id for binding in bindings):
            possible_owners.append(campaign_id)

    return tuple(sorted(set(possible_owners))), evidence_complete


def _parse_legacy_campaign_snapshot(
    row: Any,
) -> tuple[str, datetime, datetime | None, tuple[tuple[int, str, str], ...]] | None:
    """Validate a persisted campaign approval row before using it as ownership evidence."""
    if len(row) != _LEGACY_CAMPAIGN_SNAPSHOT_FIELDS:
        return None
    campaign_id, digest, scope, approval_raw, deadline_raw, created_raw, bindings_raw = row
    if (
        not isinstance(campaign_id, str)
        or not campaign_id.strip()
        or not isinstance(digest, str)
        or len(digest) != DIGEST_HEX_CHARS
        or any(char not in "0123456789abcdef" for char in digest)
        or scope != f"campaign:{campaign_id}"
        or not isinstance(bindings_raw, str)
    ):
        return None
    created_at = _ownership_timestamp(created_raw)
    approved_at = _ownership_timestamp(approval_raw)
    deadline_at = None if deadline_raw is None else _ownership_timestamp(deadline_raw)
    if (
        created_at is None
        or approved_at is None
        or (deadline_raw is not None and deadline_at is None)
        or created_at > approved_at
        or (deadline_at is not None and deadline_at < approved_at)
    ):
        return None
    try:
        bindings = json.loads(bindings_raw)
    except (TypeError, ValueError):
        bindings = None
    if not isinstance(bindings, list):
        return None
    parsed: list[tuple[int, str, str]] = []
    seen_file_ids: set[int] = set()
    for item in bindings:
        if not _valid_legacy_campaign_binding(item, seen_file_ids):
            return None
        seen_file_ids.add(item[0])
        parsed.append((item[0], item[1], item[2]))
    return campaign_id, approved_at, deadline_at, tuple(parsed)


def _valid_legacy_campaign_binding(item: Any, seen_file_ids: set[int]) -> bool:
    return (
        isinstance(item, list)
        and len(item) == CAMPAIGN_BINDING_FIELDS
        and not isinstance(item[0], bool)
        and isinstance(item[0], int)
        and item[0] > 0
        and item[0] not in seen_file_ids
        and isinstance(item[1], str)
        and isinstance(item[2], str)
        and len(item[1]) == DIGEST_HEX_CHARS
        and len(item[2]) == DIGEST_HEX_CHARS
        and all(char in "0123456789abcdef" for char in item[1] + item[2])
    )


def _legacy_campaign_identity_scope_matches(
    *,
    campaign_id: str,
    campaign_digest: Any,
    plan_path: Any,
    approved_bindings_raw: Any,
    shoko_file_id: Any,
    plan_fingerprint: Any,
    request_fingerprint: Any,
    target_ids_raw: Any,
) -> bool:
    """Validate persisted send targets against digest-bound approved targets.

    The plan fingerprint is intentionally opaque. The approved campaign document supplies the
    committed AniDB identities and (when already materialized) Shoko episode IDs; the send row
    supplies its immutable target triples. Pending approved targets may acquire Shoko IDs later,
    but their AniDB identity set must still match exactly. Missing target identities remain held:
    exact request fingerprints cover Shoko episode IDs, not the AniDB targets that produced them.
    """
    approved_entry = _read_legacy_campaign_plan_entry(
        campaign_id,
        campaign_digest,
        plan_path,
        approved_bindings_raw,
        shoko_file_id,
    )
    if approved_entry is None:
        return False
    approved_targets = _parse_legacy_approved_targets(approved_entry)
    send_targets = _parse_legacy_send_targets(target_ids_raw)
    if approved_targets is None or send_targets is None:
        return False
    approved_identities, approved_episode_ids = approved_targets
    if len({(target[0], target[1]) for target in send_targets}) != len(send_targets):
        return False
    send_identities = {(anidb_id, episode_id) for anidb_id, episode_id, _ in send_targets}
    if send_identities != approved_identities:
        return False
    episode_ids = tuple(sorted({shoko_id for _, _, shoko_id in send_targets}))
    if approved_episode_ids and episode_ids != approved_episode_ids:
        return False
    return _legacy_request_fingerprint_matches(
        shoko_file_id, plan_fingerprint, request_fingerprint, episode_ids
    )


def _read_legacy_campaign_plan_entry(
    campaign_id: str,
    campaign_digest: Any,
    plan_path: Any,
    approved_bindings_raw: Any,
    shoko_file_id: Any,
) -> Mapping[str, Any] | None:
    """Read one approved file entry only from a plan matching its persisted approval digest."""
    if (
        not isinstance(campaign_digest, str)
        or len(campaign_digest) != DIGEST_HEX_CHARS
        or not isinstance(plan_path, str)
        or not plan_path
        or not isinstance(shoko_file_id, int)
        or isinstance(shoko_file_id, bool)
        or not isinstance(approved_bindings_raw, str)
    ):
        return None
    try:
        raw = Path(plan_path).read_bytes()
        if hashlib.sha256(raw).hexdigest() != campaign_digest:
            return None
        document = json.loads(raw.decode("utf-8"))
        bindings = json.loads(approved_bindings_raw)
    except (OSError, UnicodeDecodeError, ValueError):
        document = None
        bindings = None
    if (
        not isinstance(document, dict)
        or document.get("campaign_id") != campaign_id
        or not isinstance(document.get("files"), list)
        or not isinstance(bindings, list)
    ):
        return None
    approved_bindings = [
        item
        for item in bindings
        if isinstance(item, list)
        and len(item) == CAMPAIGN_BINDING_FIELDS
        and item[0] == shoko_file_id
    ]
    file_entries = [
        item
        for item in document["files"]
        if isinstance(item, dict)
        and not isinstance(item.get("shoko_file_id"), bool)
        and item.get("shoko_file_id") == shoko_file_id
    ]
    if len(approved_bindings) != 1 or len(file_entries) != 1:
        return None
    binding, entry = approved_bindings[0], file_entries[0]
    if (
        not isinstance(binding[1], str)
        or not isinstance(binding[2], str)
        or entry.get("plan_fingerprint") != binding[1]
        or entry.get("request_fingerprint") != binding[2]
    ):
        return None
    return entry


def _parse_legacy_approved_targets(
    approved_entry: Mapping[str, Any],
) -> tuple[frozenset[tuple[int, int]], tuple[int, ...]] | None:
    identities_raw = approved_entry.get("anidb_target_identities")
    episode_ids_raw = approved_entry.get("shoko_episode_ids")
    if (
        not isinstance(identities_raw, list)
        or not identities_raw
        or not isinstance(episode_ids_raw, list)
    ):
        return None
    identities: set[tuple[int, int]] = set()
    for identity in identities_raw:
        if (
            not isinstance(identity, list)
            or len(identity) != ANIDB_IDENTITY_FIELDS
            or isinstance(identity[0], bool)
            or isinstance(identity[1], bool)
            or isinstance(identity[3], bool)
            or not isinstance(identity[0], int)
            or not isinstance(identity[1], int)
            or not isinstance(identity[2], str)
            or not isinstance(identity[3], int)
            or identity[3] <= 0
        ):
            return None
        identities.add((identity[0], identity[3]))
    if len(identities) != len(identities_raw):
        return None
    episode_ids: list[int] = []
    for episode_id in episode_ids_raw:
        if isinstance(episode_id, bool) or not isinstance(episode_id, int) or episode_id <= 0:
            return None
        episode_ids.append(episode_id)
    if len(set(episode_ids)) != len(episode_ids):
        return None
    return frozenset(identities), tuple(sorted(episode_ids))


def _decode_target_ids(raw: Any) -> tuple[tuple[int, int, int], ...] | None:
    """Validate every destination and canonicalize exact repeats, or reject the set."""
    if not isinstance(raw, str):
        return None
    try:
        targets_raw = json.loads(raw)
    except ValueError:
        return None
    if not isinstance(targets_raw, list) or not targets_raw:
        return None
    targets: list[tuple[int, int, int]] = []
    for target in targets_raw:
        if (
            not isinstance(target, list)
            or len(target) != TARGET_TRIPLE_LEN
            or any(
                isinstance(value, bool) or not isinstance(value, int) or value <= 0
                for value in target
            )
        ):
            return None
        targets.append((target[0], target[1], target[2]))
    # Multiple source episodes can legitimately share a destination. Older writers
    # persisted each source occurrence; keep that history recoverable without ever
    # salvaging a valid subset from a malformed row.
    return tuple(dict.fromkeys(targets))


def _legacy_request_fingerprint_matches(
    shoko_file_id: int,
    plan_fingerprint: str,
    request_fingerprint: str,
    episode_ids: tuple[int, ...],
) -> bool:
    lines = (
        "method|POST",
        f"path|api/v3/File/{shoko_file_id}/Link",
        f"body|{episode_ids}",
        f"plan|{plan_fingerprint}",
    )
    actual = hashlib.sha256("\n".join(sorted(lines)).encode("utf-8")).hexdigest()
    return actual == request_fingerprint


def _migrate_v7_to_v8(conn: sqlite3.Connection) -> None:
    _execute_schema_statements(conn, _METADATA_SCHEMA_SQL)


def _migrate_v8_to_v9(conn: sqlite3.Connection) -> None:
    _execute_schema_statements(conn, _METADATA_V9_SCHEMA_SQL)


# Forward-only migration registry: each entry upgrades ``version`` to ``version + 1``.
# v5 adds timestamped reservations; v6 preserves data without inferring campaign ownership.
# v7 adds immutable send ownership, fail-closed legacy attribution, and monotonic attempt totals.
# Future slices append entries here and bump SCHEMA_VERSION. Migrations never destroy
# tables, never rewrite send outcomes, and never run backwards.
_MIGRATIONS: Final[Mapping[int, Callable[[sqlite3.Connection], None]]] = {
    1: _migrate_v1_to_v2,
    2: _migrate_v2_to_v3,
    3: _migrate_v3_to_v4,
    4: _migrate_v4_to_v5,
    5: _migrate_v5_to_v6,
    6: _migrate_v6_to_v7,
    7: _migrate_v7_to_v8,
    8: _migrate_v8_to_v9,
}


def _parse_iso(raw: str) -> datetime:
    """Parse an ISO-8601 timestamp; naive values are declared UTC (determinism)."""
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _enable_wal_mode(connection: sqlite3.Connection) -> None:
    """Set WAL mode, retrying first-open lock races within the configured bound."""
    deadline = time.monotonic() + DEFAULT_BUSY_TIMEOUT_MS / 1000
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise sqlite3.OperationalError(
                    f"could not enable SQLite WAL mode within {DEFAULT_BUSY_TIMEOUT_MS} ms"
                )
            # journal_mode changes may return SQLITE_BUSY without honoring busy_timeout. Use
            # short bounded waits so a concurrent first opener can finish its mode transition.
            timeout_ms = max(1, min(100, int(remaining * 1000)))
            connection.execute(f"PRAGMA busy_timeout={timeout_ms}")
            try:
                row = connection.execute("PRAGMA journal_mode=WAL").fetchone()
            except sqlite3.OperationalError as exc:
                if not any(reason in str(exc).casefold() for reason in ("locked", "busy")):
                    raise
                if time.monotonic() >= deadline:
                    raise sqlite3.OperationalError(
                        f"could not enable SQLite WAL mode within {DEFAULT_BUSY_TIMEOUT_MS} ms: "
                        f"{exc}"
                    ) from exc
                time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
                continue
            mode = str(row[0]).lower() if row else ""
            if mode != "wal":
                raise sqlite3.OperationalError(
                    f"SQLite refused WAL mode (reported {mode or 'no mode'})"
                )
            return
    finally:
        connection.execute(f"PRAGMA busy_timeout={DEFAULT_BUSY_TIMEOUT_MS}")


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


def _canonical_metadata_targets(
    targets: Sequence[MetadataEpisodeIdentity],
    anime_id: int,
) -> tuple[MetadataEpisodeIdentity, ...]:
    for target in targets:
        if (
            not isinstance(target, tuple)
            or len(target) != _METADATA_TARGET_FIELDS
            or type(target[0]) is not int
            or target[0] != anime_id
            or not isinstance(target[1], EpisodeType)
            or type(target[2]) is not int
            or target[2] <= 0
        ):
            raise StateError(
                "metadata targets require matching anime, EpisodeType, positive number"
            )
    return tuple(sorted(set(targets), key=lambda target: (target[0], target[1].value, target[2])))


def _canonical_metadata_demand(
    required_targets: Sequence[MetadataEpisodeIdentity] | None,
    missing_targets: Sequence[MetadataEpisodeIdentity] | None,
    anime_id: int,
) -> tuple[tuple[MetadataEpisodeIdentity, ...] | None, tuple[MetadataEpisodeIdentity, ...] | None]:
    required = (
        None
        if required_targets is None
        else _canonical_metadata_targets(required_targets, anime_id)
    )
    missing = (
        None if missing_targets is None else _canonical_metadata_targets(missing_targets, anime_id)
    )
    if (required is None) != (missing is None) or (
        required is not None
        and (not required or not missing or not set(missing).issubset(required))
    ):
        raise StateError("metadata demand requires nonempty required and missing target subsets")
    return required, missing


def _encode_metadata_targets(targets: tuple[MetadataEpisodeIdentity, ...] | None) -> str | None:
    return (
        None
        if targets is None
        else json.dumps(
            [(anime, kind.value, number) for anime, kind, number in targets], separators=(",", ":")
        )
    )


def _decode_metadata_targets(
    raw: str | None, anime_id: int
) -> tuple[MetadataEpisodeIdentity, ...] | None:
    if raw is None:
        return None
    try:
        data = json.loads(raw)
        if not isinstance(data, list):
            raise ValueError("expected target list")
        targets = []
        for target in data:
            if not isinstance(target, list) or len(target) != _METADATA_TARGET_FIELDS:
                raise ValueError("expected target triple")
            targets.append((target[0], EpisodeType(target[1]), target[2]))
        return _canonical_metadata_targets(targets, anime_id)
    except (TypeError, ValueError) as exc:
        raise StateError("invalid persisted metadata target coverage") from exc


def _clip(detail: str) -> str:
    return redact(detail)[:MAX_DETAIL_CHARS]


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
    # Persist destinations, not source occurrences; LinkPlan retains the latter for preflight.
    payload = [
        [int(anidb), int(episode), int(shoko)] for anidb, episode, shoko in dict.fromkeys(triples)
    ]
    return json.dumps(payload)


def _parse_legacy_send_targets(raw: Any) -> tuple[tuple[int, int, int], ...] | None:
    return _decode_target_ids(raw)


def _parse_target_ids(raw: str | None) -> tuple[tuple[int, int, int], ...]:
    # Recovery treats missing targets as a hold. Never salvage a partial target set:
    # matching that subset could falsely verify a multi-episode send and free its slot.
    return _decode_target_ids(raw) or ()


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
_SQL_CAMPAIGN_FILES: Final[str] = (
    "SELECT shoko_file_id, sonarr_series_id, sonarr_file_id, sonarr_path, sonarr_size,"
    " state, cohort, plan_fingerprint, request_fingerprint, mapping_targets_digest,"
    " mapping_structure_digest, mapping_revision, policy, campaign_id, state_detail,"
    " updated_iso, first_seen_iso, last_verified_iso, identity_hash_json"
    " FROM files WHERE campaign_id = ? ORDER BY shoko_file_id"
)


def _row_to_campaign(row: Any) -> CampaignRecord:
    values = tuple(row)
    bindings: list[CampaignBinding] = []
    try:
        document = json.loads(_as_str(values[11], "plan_fingerprint_set"))
    except ValueError:
        document = []
    if isinstance(document, list):
        for item in document:
            if (
                isinstance(item, list)
                and len(item) == CAMPAIGN_BINDING_FIELDS
                and not isinstance(item[0], bool)
                and isinstance(item[0], int)
                and isinstance(item[1], str)
                and isinstance(item[2], str)
            ):
                bindings.append(
                    CampaignBinding(
                        shoko_file_id=item[0], plan_fingerprint=item[1], request_fingerprint=item[2]
                    )
                )
    return CampaignRecord(
        campaign_id=_as_str(values[0], "campaign_id"),
        digest=_as_str(values[1], "digest"),
        max_submissions=_as_int(values[2], "max_submissions"),
        deadline_iso=_as_optional_str(values[3]),
        attestation_scope=_as_str(values[4], "attestation_scope"),
        approval_iso=_as_str(values[5], "approval_iso"),
        status=_as_str(values[6], "status"),
        status_reason=_as_str(values[7], "status_reason"),
        total_sent=_as_int(values[8], "total_sent"),
        last_batch_iso=_as_optional_str(values[9]),
        created_iso=_as_str(values[10], "created_iso"),
        bindings=tuple(bindings),
        plan_path=_as_optional_str(values[CAMPAIGN_PLAN_PATH_COLUMN])
        if len(values) > CAMPAIGN_PLAN_PATH_COLUMN
        else None,
        drain_authorized_iso=_as_optional_str(values[CAMPAIGN_DRAIN_AUTHORIZED_ISO_COLUMN])
        if len(values) > CAMPAIGN_DRAIN_AUTHORIZED_ISO_COLUMN
        else None,
        drain_authorized_digest=_as_optional_str(values[CAMPAIGN_DRAIN_AUTHORIZED_DIGEST_COLUMN])
        if len(values) > CAMPAIGN_DRAIN_AUTHORIZED_DIGEST_COLUMN
        else None,
        drain_invalidated_reason=(
            _as_str(values[CAMPAIGN_DRAIN_INVALIDATED_REASON_COLUMN], "drain_invalidated_reason")
            if len(values) > CAMPAIGN_DRAIN_INVALIDATED_REASON_COLUMN
            else ""
        ),
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
        campaign_id=_as_optional_str(values[13]),
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
        self._apply_lock: ApplyProcessLock | None = None
        # Ephemeral permission only narrows durable admission; restart never restores it.
        self._metadata_transport_claims: set[str] = set()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.path))
        try:
            if os.name != "nt":
                try:
                    self.path.chmod(0o600)
                except OSError as exc:
                    raise StateError("state database permissions could not be restricted") from exc
            self._conn.isolation_level = None  # explicit transactions only
            self._conn.execute(f"PRAGMA busy_timeout={DEFAULT_BUSY_TIMEOUT_MS}")
            _enable_wal_mode(self._conn)
            self._conn.execute("PRAGMA synchronous=FULL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._init_schema()
            self._secure_database_permissions()
            # Opening a handle is not an authorization to clear a global stop. The property
            # re-reads the durable marker on each check; observe it here too so the local latch
            # cannot later be cleared by another handle's operator recovery.
            _ = self.writes_disabled
        except BaseException:
            with contextlib.suppress(Exception):
                self._conn.close()
            raise

    # ------------------------------------------------------------- lifecycle

    def close(self) -> None:
        """Release the OS advisory apply lock (if this store ever held it) and close the DB."""
        lock = self._apply_lock
        if lock is not None:
            lock.release()
        self._conn.close()

    def _secure_database_permissions(self) -> None:
        """Keep the database and SQLite sidecars owner-only on POSIX installations."""
        if os.name == "nt":
            return
        for candidate in (
            self.path,
            Path(f"{self.path}-wal"),
            Path(f"{self.path}-shm"),
        ):
            if candidate.exists():
                try:
                    candidate.chmod(0o600)
                except OSError as exc:
                    raise StateError("state database permissions could not be restricted") from exc

    @property
    def connection(self) -> sqlite3.Connection:
        """Raw connection for **read-only** external checks (e.g. integrity)."""
        return self._conn

    @property
    def writes_disabled(self) -> bool:
        if self._writes_disabled:
            return True
        try:
            row = self._conn.execute(
                "SELECT (SELECT value FROM meta WHERE key = ?),"
                " (SELECT value FROM meta WHERE key = ?)",
                (META_WRITES_DISABLED, META_WRITES_DISABLED_REASON),
            ).fetchone()
        except sqlite3.Error as exc:
            self._latch_writes_disabled(f"write-stop state cannot be read: {exc}")
            return True
        if row is None:
            self._latch_writes_disabled("write-stop state cannot be read: no result")
            return True
        marker = row[0]
        if marker is not None and not isinstance(marker, str):
            self._latch_writes_disabled("write-stop marker is malformed")
            return True
        if marker is not None:
            reason = row[1] if isinstance(row[1], str) else "unspecified"
            self._latch_writes_disabled(reason or "unspecified")
            return True
        return False

    def _latch_writes_disabled(self, reason: str) -> None:
        """Irreversibly stop this handle before attempting any durable bookkeeping."""
        self._writes_disabled = True
        if not self._writes_reason:
            self._writes_reason = _clip(reason) or "unspecified"

    def _assert_writes_enabled_tx(self) -> None:
        """Check the kill switch after BEGIN IMMEDIATE, against a fresh serialized snapshot."""
        if self._writes_disabled:
            raise WriteDisabledError(f"state writes are disabled: {self._writes_reason}")
        try:
            row = self._conn.execute(
                "SELECT (SELECT value FROM meta WHERE key = ?),"
                " (SELECT value FROM meta WHERE key = ?)",
                (META_WRITES_DISABLED, META_WRITES_DISABLED_REASON),
            ).fetchone()
        except sqlite3.Error as exc:
            reason = f"write-stop state cannot be read: {exc}"
            self._latch_writes_disabled(reason)
            raise WriteDisabledError(f"state writes stopped: {reason}") from exc
        if row is None:
            reason = "write-stop state cannot be read: no result"
            self._latch_writes_disabled(reason)
            raise WriteDisabledError(f"state writes stopped: {reason}")
        marker = row[0]
        if marker is not None and not isinstance(marker, str):
            reason = "write-stop marker is malformed"
            self._latch_writes_disabled(reason)
            raise WriteDisabledError(f"state writes stopped: {reason}")
        if marker is not None:
            reason = row[1] if isinstance(row[1], str) else "unspecified"
            reason = reason or "unspecified"
            self._latch_writes_disabled(reason)
            raise WriteDisabledError(f"state writes are disabled: {reason}")

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
        if version == SCHEMA_VERSION:
            return version
        if version > SCHEMA_VERSION:
            raise StateError(
                f"schema version {version} is newer than supported {SCHEMA_VERSION}; "
                "refusing to migrate backwards"
            )
        while True:
            with self._schema_maintenance_transaction(busy_context="schema migration"):
                version = self.schema_version()
                if version == SCHEMA_VERSION:
                    return version
                self._migrate_step_locked(version)

    def _migrate_step_locked(self, version: int) -> int:
        """Apply one migration step while the caller holds the schema write transaction."""
        if version > SCHEMA_VERSION:
            raise StateError(
                f"schema version {version} is newer than supported {SCHEMA_VERSION}; "
                "refusing to migrate backwards"
            )
        step = _MIGRATIONS.get(version)
        if step is None:
            raise StateError(f"no forward migration registered from version {version}")
        # The step, schema-version bump, and audit row share this transaction. A second opener
        # re-reads the version after obtaining the lock, so it cannot repeat a stale decision.
        try:
            step(self._conn)
        except sqlite3.Error as exc:
            raise StateError(
                f"migration from schema version {version} failed: {exc}; "
                f"state left at version {version} (no changes committed, data intact) "
                "— recoverable: fix the cause and reopen"
            ) from exc
        next_version = version + 1
        self._conn.execute(f"PRAGMA user_version={next_version}")
        self._conn.execute(
            "INSERT INTO schema_version (version, applied_iso, description)" " VALUES (?, ?, ?)",
            (next_version, datetime.now(UTC).isoformat(), f"migrated from {version}"),
        )
        return next_version

    def integrity_report(self) -> IntegrityReport:
        return integrity_check(self)

    def _init_schema(self) -> None:
        version = self.schema_version()
        if version == SCHEMA_VERSION:
            return
        if version > SCHEMA_VERSION:
            raise sqlite3.DatabaseError(
                f"schema version {version} is newer than supported {SCHEMA_VERSION}"
            )
        continue_migration = False
        with self._schema_maintenance_transaction(
            busy_context="schema initialization or migration"
        ):
            # The first read is a read-only fast path for the common current-schema open. For
            # initialization/upgrades, re-read only after BEGIN IMMEDIATE: another constructor
            # may have finished while this connection waited for the writer lock.
            version = self.schema_version()
            if version > SCHEMA_VERSION:
                raise sqlite3.DatabaseError(
                    f"schema version {version} is newer than supported {SCHEMA_VERSION}"
                )
            if version == SCHEMA_VERSION:
                return
            if version > 0:
                self._migrate_step_locked(version)
                continue_migration = version + 1 < SCHEMA_VERSION
            else:
                _execute_schema_statements(self._conn, _SCHEMA_SQL)
                self._conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                self._conn.execute(
                    "INSERT INTO schema_version (version, applied_iso, description)"
                    " VALUES (?, ?, ?)",
                    (
                        SCHEMA_VERSION,
                        datetime.now(UTC).isoformat(),
                        f"initial v{SCHEMA_VERSION} schema",
                    ),
                )
        if continue_migration:
            self.migrate()

    def _begin_immediate_tx(self, busy_context: str | None) -> None:
        try:
            self._conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            if busy_context is None or not any(
                reason in str(exc).casefold() for reason in ("locked", "busy")
            ):
                raise
            raise StateError(
                f"{busy_context} could not acquire SQLite's write lock within "
                f"{DEFAULT_BUSY_TIMEOUT_MS} ms: {exc}"
            ) from exc

    def _rollback_tx(self) -> None:
        self._conn.rollback()

    def _commit_tx(self) -> None:
        try:
            self._conn.commit()
        except BaseException:
            with contextlib.suppress(sqlite3.Error):
                self._conn.rollback()
            raise

    @contextlib.contextmanager
    def _schema_maintenance_transaction(self, *, busy_context: str | None = None) -> Iterator[None]:
        """Serialize DDL and registered migrations without clearing the durable write stop.

        This bypass is intentionally separate from ordinary state transactions and is used only
        by schema initialization/migration. Once construction finishes, ``writes_disabled`` reads
        and latches the unchanged marker; callers still need the operator recovery command to clear
        it.
        """
        self._begin_immediate_tx(busy_context)
        try:
            yield
        except BaseException:
            self._rollback_tx()
            raise
        self._commit_tx()

    @contextlib.contextmanager
    def _write_stop_control_transaction(self) -> Iterator[None]:
        """Transaction reserved for persisting or clearing the stop marker itself."""
        self._begin_immediate_tx(None)
        try:
            yield
        except BaseException:
            self._rollback_tx()
            raise
        self._commit_tx()

    @contextlib.contextmanager
    def _transaction(
        self,
        *,
        busy_context: str | None = None,
    ) -> Iterator[None]:
        """One write-serialized transaction with a fresh, durable stop check by default."""
        self._begin_immediate_tx(busy_context)
        try:
            self._assert_writes_enabled_tx()
            yield
        except BaseException:
            self._rollback_tx()
            raise
        self._commit_tx()

    # ------------------------------------------------------------- meta

    def get_meta(self, key: str) -> str | None:
        row = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return None if row is None else _as_str(row[0], "meta.value")

    @_mutation
    def set_meta(self, key: str, value: str) -> None:
        with self._transaction():
            self._conn.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?)"
                " ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    @_mutation
    def save_metadata_status_if_current(
        self,
        *,
        key: str,
        source_key: str,
        anime_id: int,
        expected_claim: str | None,
        expected_document_json: str | None,
        document_json: str,
    ) -> bool:
        """Atomically fence an observational document against generation/source races.

        The lightweight claim query deliberately does not decode another operation
        against recovery's row budget. No observation grants transport permission.
        """
        self._metadata_identity(source_key, anime_id)
        with self._transaction():
            if self.get_meta(key) != expected_document_json:
                return False
            row = self._conn.execute(
                "SELECT claim FROM metadata_operations WHERE source_key=? AND anidb_anime_id=?",
                (source_key, anime_id),
            ).fetchone()
            if (None if row is None else _as_str(row[0], "metadata claim")) != expected_claim:
                return False
            if not self._metadata_status_source_current_tx(source_key, expected_document_json):
                return False
            self._set_meta_tx(key, document_json)
            return True

    def _metadata_status_source_current_tx(  # noqa: PLR0911 - ordered source fallbacks
        self, source_key: str, previous_status: str | None
    ) -> bool:
        """Use the current saved endpoint, falling back to legacy setup binding."""
        try:
            encoded = self.get_meta(META_SERVICE_SETTINGS)
            settings = {} if encoded is None else json.loads(encoded)
            if not isinstance(settings, dict):
                return False
            url = settings.get("shoko_url")
            if "shoko_url" in settings and not isinstance(url, str):
                return False
            if url is None:
                encoded = self.get_onboarding_json()
                onboarding = {} if encoded is None else json.loads(encoded)
                if not isinstance(onboarding, dict):
                    return False
                binding = onboarding.get("binding", {})
                if not isinstance(binding, dict):
                    return False
                url = binding.get("shoko_url")
            if url is not None:
                return isinstance(url, str) and metadata_source_key_for_url(url) == source_key
            # Without any durable endpoint, accept the first source but never let
            # an unverified different source displace a current status document.
            if previous_status is None:
                return True
            previous = json.loads(previous_status)
            return isinstance(previous, dict) and previous.get("source_key") == source_key
        except (ValueError, TypeError, RecursionError):
            return False

    @_mutation
    def clear_meta(self, key: str) -> None:
        """Delete one meta key (bounded terminal-state bookkeeping cleanup; no-op if absent)."""
        with self._transaction():
            self._conn.execute("DELETE FROM meta WHERE key = ?", (key,))

    def load_sonarr_inventory_cache(
        self,
    ) -> tuple[str | None, int, frozenset[int], dict[int, str]]:
        """Load resumable Sonarr child responses for one stable series-list manifest."""
        manifest = self.get_meta(META_SONARR_INVENTORY_MANIFEST)
        raw_cursor = self.get_meta(META_SONARR_INVENTORY_CURSOR)
        raw_sweep = self.get_meta(META_SONARR_INVENTORY_SWEEP)
        try:
            cursor = max(0, int(raw_cursor or "0"))
        except ValueError:
            cursor = 0
        try:
            decoded_sweep = json.loads(raw_sweep or "[]")
            if not isinstance(decoded_sweep, list) or any(
                type(item) is not int or item <= 0 for item in decoded_sweep
            ):
                decoded_sweep = []
        except ValueError:
            decoded_sweep = []
        rows = self._conn.execute(
            "SELECT key, value FROM meta WHERE substr(key, 1, ?) = ? ORDER BY key",
            (len(META_SONARR_INVENTORY_FRAGMENT_PREFIX), META_SONARR_INVENTORY_FRAGMENT_PREFIX),
        )
        fragments: dict[int, str] = {}
        for row in rows:
            raw_id = str(row[0])[len(META_SONARR_INVENTORY_FRAGMENT_PREFIX) :]
            try:
                series_id = int(raw_id)
            except ValueError:
                continue
            if series_id > 0:
                fragments[series_id] = _as_str(row[1], "meta.value")
        return manifest, cursor, frozenset(decoded_sweep), fragments

    @_mutation
    def save_sonarr_inventory_cache(
        self,
        manifest_hash: str,
        cursor: int,
        swept_series_ids: Sequence[int],
        fetched_fragments: Mapping[int, str],
        *,
        reset: bool = False,
        invalidated_series_ids: Sequence[int] = (),
    ) -> None:
        """Atomically persist bounded progress, successful children, and invalidations."""
        if not isinstance(manifest_hash, str) or len(manifest_hash) != DIGEST_HEX_CHARS:
            raise StateError("Sonarr inventory manifest hash is invalid")
        if isinstance(cursor, bool) or not isinstance(cursor, int) or cursor < 0:
            raise StateError("Sonarr inventory cursor is invalid")
        if any(
            isinstance(series_id, bool) or not isinstance(series_id, int) or series_id <= 0
            for series_id in (*swept_series_ids, *invalidated_series_ids)
        ):
            raise StateError("Sonarr inventory contains an invalid series id")
        if any(
            isinstance(series_id, bool)
            or not isinstance(series_id, int)
            or series_id <= 0
            or not isinstance(encoded, str)
            for series_id, encoded in fetched_fragments.items()
        ):
            raise StateError("Sonarr inventory cache contains an invalid fragment")
        with self._transaction():
            if reset:
                self._conn.execute(
                    "DELETE FROM meta WHERE substr(key, 1, ?) = ?",
                    (
                        len(META_SONARR_INVENTORY_FRAGMENT_PREFIX),
                        META_SONARR_INVENTORY_FRAGMENT_PREFIX,
                    ),
                )
            for series_id in set(invalidated_series_ids):
                self._conn.execute(
                    "DELETE FROM meta WHERE key = ?",
                    (f"{META_SONARR_INVENTORY_FRAGMENT_PREFIX}{series_id}",),
                )
            for series_id, encoded in fetched_fragments.items():
                self._set_meta_tx(f"{META_SONARR_INVENTORY_FRAGMENT_PREFIX}{series_id}", encoded)
            self._set_meta_tx(META_SONARR_INVENTORY_MANIFEST, manifest_hash)
            self._set_meta_tx(META_SONARR_INVENTORY_CURSOR, str(cursor))
            self._set_meta_tx(
                META_SONARR_INVENTORY_SWEEP,
                json.dumps(sorted(set(swept_series_ids)), separators=(",", ":")),
            )

    @_mutation
    def remember_mapping_targets_bulk(
        self, files: Mapping[int, Mapping[str, tuple[int | None, int | None]]]
    ) -> frozenset[int]:
        """Compare/freeze source→AniDB identities and monotonically fill resolved IDs.

        This versioned meta document predates a send intent: even a pending or
        budget-deferred plan must retain its original targets across restart.
        A mismatch never updates the original document. BEGIN IMMEDIATE fences
        competing reconciliation processes; a crash cannot partially rebaseline it.
        """
        with self._transaction():
            return frozenset(
                file_id
                for file_id, targets in files.items()
                if not self._remember_mapping_targets_tx(file_id, targets)
            )

    def _remember_mapping_targets_tx(
        self, file_id: int, targets: Mapping[str, tuple[int | None, int | None]]
    ) -> bool:
        if not targets or self.get_file(file_id) is None:
            raise StateError("mapping targets require a tracked file and nonempty destinations")
        if self.mapping_target_quarantined(file_id):
            return False
        key = f"file_mapping_targets:v1:{file_id}"
        raw = self.get_meta(key)
        current = {source: list(ids) for source, ids in targets.items()}
        if raw is not None:
            try:
                previous = json.loads(raw)
            except ValueError as exc:
                raise StateError("persisted mapping targets are malformed") from exc
            if not isinstance(previous, dict) or any(
                not isinstance(ids, list)
                or len(ids) != MAPPING_RESOLVED_ID_COUNT
                or any(
                    value is not None and (type(value) is not int or value <= 0) for value in ids
                )
                for ids in previous.values()
            ):
                raise StateError("persisted mapping targets are malformed")
            if previous.keys() != current.keys():
                return False
            for source, ids in current.items():
                for position, value in enumerate(ids):
                    prior = previous[source][position]
                    if prior is not None and value is not None and prior != value:
                        return False
                    if prior is not None:
                        ids[position] = prior
        encoded = json.dumps(current, sort_keys=True, separators=(",", ":"))
        if encoded != raw:
            self._conn.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?)"
                " ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                (key, encoded),
            )
        return True

    def unfrozen_mapping_target_files(self) -> frozenset[int]:
        """Legacy retryable files with evidence of resolved targets but no snapshot.

        WAITING alone is not resolution evidence. Drift reasons and send history
        survive old retry paths, however, and must not be erased by rematching.
        Includes durable quarantines even if a snapshot or detail was subsequently
        changed. In-flight/verified rows retain their independent protection rules.
        """
        rows = self._conn.execute(
            "SELECT f.shoko_file_id FROM files AS f"
            " WHERE f.campaign_id IS NULL AND f.cohort != 'campaign'"
            " AND f.state IN ('discovered', 'matched', 'mapped', 'planned',"
            " 'waiting', 'ambiguous', 'conflict')"
            " AND (EXISTS (SELECT 1 FROM meta WHERE key = ? || f.shoko_file_id)"
            " OR (NOT EXISTS (SELECT 1 FROM meta WHERE"
            " key = 'file_mapping_targets:v1:' || f.shoko_file_id)"
            " AND (f.state IN ('mapped', 'planned')"
            " OR f.state_detail IN ('mapping_drift', 'mapping_changed')"
            " OR instr(f.state_detail, 'service_mapping_target_changed') = 1"
            " OR f.plan_fingerprint IS NOT NULL OR f.request_fingerprint IS NOT NULL"
            " OR f.mapping_targets_digest IS NOT NULL OR f.mapping_structure_digest IS NOT NULL"
            " OR f.mapping_revision IS NOT NULL"
            " OR EXISTS (SELECT 1 FROM sends WHERE shoko_file_id = f.shoko_file_id))))",
            (META_MAPPING_TARGET_QUARANTINE_PREFIX,),
        )
        return frozenset(_as_int(row[0], "shoko_file_id") for row in rows)

    def mapping_target_quarantined(self, file_id: int) -> bool:
        """Quarantine is a durable safety fact, independent of lifecycle state/details."""
        return self.get_meta(f"{META_MAPPING_TARGET_QUARANTINE_PREFIX}{file_id}") is not None

    @_mutation
    def quarantine_unfrozen_mapping_targets(self, *, iso: str) -> frozenset[int]:
        """Atomically classify legacy evidence and latch every candidate for review."""
        _require_iso(iso, "iso")
        with self._transaction():
            return self._quarantine_mapping_targets_tx(self.unfrozen_mapping_target_files(), iso)

    @_mutation
    def quarantine_mapping_targets(
        self,
        file_ids: Sequence[int],
        *,
        iso: str,
        reason: str = "service_mapping_target_changed",
    ) -> frozenset[int]:
        """Latch frozen-target/source drift, including CONFLICT rows; never alter sent files."""
        _require_iso(iso, "iso")
        if not isinstance(reason, str) or not reason.strip():
            raise StateError("mapping target quarantine reason is invalid")
        with self._transaction():
            return self._quarantine_mapping_targets_tx(file_ids, iso, reason.strip())

    def _quarantine_mapping_targets_tx(
        self, file_ids: Iterable[int], iso: str, reason: str = "service_mapping_target_changed"
    ) -> frozenset[int]:
        quarantined: set[int] = set()
        for file_id in sorted(set(file_ids)):
            record = self.get_file(file_id)
            if (
                record is None
                or record.campaign_id is not None
                or record.cohort is Cohort.CAMPAIGN
                or record.state
                not in (
                    FileState.DISCOVERED,
                    FileState.MATCHED,
                    FileState.MAPPED,
                    FileState.PLANNED,
                    FileState.WAITING,
                    FileState.AMBIGUOUS,
                    FileState.CONFLICT,
                )
            ):
                continue
            if record.state is not FileState.CONFLICT:
                validate_transition(record.state, FileState.CONFLICT)
            key = f"{META_MAPPING_TARGET_QUARANTINE_PREFIX}{file_id}"
            # Retain the original evidence. State/detail refresh is not a new
            # quarantine and must not replace its initial audit record.
            self._conn.execute(
                "INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT (key) DO NOTHING",
                (
                    key,
                    json.dumps(
                        {
                            "state": record.state.value,
                            "detail": _clip(record.state_detail),
                            "detected_iso": iso,
                        },
                        sort_keys=True,
                    ),
                ),
            )
            if record.state is not FileState.CONFLICT or record.state_detail != reason:
                self._conn.execute(
                    "UPDATE files SET state = 'conflict',"
                    " state_detail = ?, updated_iso = ?"
                    " WHERE shoko_file_id = ?",
                    (reason, iso, file_id),
                )
            quarantined.add(file_id)
        return frozenset(quarantined)

    # ---------------------------------------------------------- onboarding

    @_mutation
    def save_service_settings_if_current(
        self,
        expected_json: str | None,
        document_json: str,
        *,
        authorization_changed: bool,
        iso: str,
    ) -> bool:
        """Publish validated preferences and their authorization review hold atomically.

        The controller supplies the exact raw snapshot used to validate the form,
        including None when runtime defaults supplied the initial preferences.
        """
        _require_iso(iso, "onboarding.updated_iso")
        with self._transaction():
            if self.get_meta(META_SERVICE_SETTINGS) != expected_json:
                return False
            if authorization_changed:
                self._pause_service_for_review_tx("setup_changed", iso)
            self._set_meta_tx(META_SERVICE_SETTINGS, document_json)
        return True

    @_mutation
    def save_service_scope_selection(
        self, draft_json: str, *, expected_authorization_revision: str | None, iso: str
    ) -> None:
        """Atomically replace user scope intent and revoke older setup forms."""
        _require_iso(iso, "onboarding.updated_iso")
        roots = json.loads(draft_json)
        if (
            not isinstance(roots, list)
            or not roots
            or not all(isinstance(root, str) and root for root in roots)
        ):
            raise StateError("Saved library selection is invalid.")
        with self._transaction():
            if self.get_service_authorization_revision() != expected_authorization_revision:
                raise StateError("Setup choices changed; review setup again.")
            self._pause_service_for_review_tx("setup_changed", iso)
            self._set_meta_tx(META_SERVICE_DRAFT_ROOTS, draft_json)

    def get_service_authorization_revision(self) -> str | None:
        """Private generation of authorization intent and newly observed hard safety holds."""
        value = self.get_meta(META_SERVICE_AUTHORIZATION_REVISION)
        if value is not None and (
            len(value) != SERVICE_AUTHORIZATION_REVISION_CHARS
            or any(char not in "0123456789abcdef" for char in value)
        ):
            raise StateError("service authorization revision is malformed")
        return value

    def get_dashboard_account(self) -> DashboardAccount | None:
        row = self._conn.execute(
            "SELECT username, password_hash, signing_key, created_iso"
            " FROM dashboard_account WHERE singleton = 1"
        ).fetchone()
        if row is None:
            return None
        username = _as_str(row[0], "dashboard_account.username")
        password_hash = _as_str(row[1], "dashboard_account.password_hash")
        signing_key = row[2]
        if not isinstance(signing_key, bytes) or len(signing_key) != DASHBOARD_SIGNING_KEY_BYTES:
            raise StateError("dashboard signing key is malformed")
        created_iso = _as_str(row[3], "dashboard_account.created_iso")
        _require_iso(created_iso, "dashboard_account.created_iso")
        return DashboardAccount(username, password_hash, signing_key, created_iso)

    @_mutation
    def create_dashboard_account(
        self, username: str, password_hash: str, signing_key: bytes, iso: str
    ) -> bool:
        """Atomically enroll exactly one dashboard account; credentials are never inferred."""
        if (
            not isinstance(username, str)
            or not username
            or username != username.strip()
            or len(username) > MAX_DASHBOARD_USERNAME_CHARS
            or not username.isprintable()
        ):
            raise StateError("dashboard username is invalid")
        if (
            not isinstance(password_hash, str)
            or not password_hash.startswith("scrypt$")
            or len(password_hash) > MAX_DASHBOARD_PASSWORD_HASH_CHARS
        ):
            raise StateError("dashboard password hash is invalid")
        if not isinstance(signing_key, bytes) or len(signing_key) != DASHBOARD_SIGNING_KEY_BYTES:
            raise StateError("dashboard signing key must contain 32 random bytes")
        _require_iso(iso, "dashboard_account.created_iso")
        with self._transaction():
            existing = self._conn.execute(
                "SELECT 1 FROM dashboard_account WHERE singleton = 1"
            ).fetchone()
            if existing is not None:
                return False
            self._conn.execute(
                "INSERT INTO dashboard_account"
                " (singleton, username, password_hash, signing_key, created_iso)"
                " VALUES (1, ?, ?, ?, ?)",
                (username, password_hash, signing_key, iso),
            )
        self._secure_database_permissions()
        return True

    @_mutation
    def create_dashboard_session(
        self,
        session_id_hash: str,
        csrf_hash: str,
        expires_epoch: int,
        iso: str,
    ) -> bool:
        """Insert one bounded expiring session; raw bearer and CSRF tokens never reach SQLite."""
        _require_hex64(session_id_hash, "dashboard_session.session_id_hash")
        _require_hex64(csrf_hash, "dashboard_session.csrf_hash")
        if isinstance(expires_epoch, bool) or not isinstance(expires_epoch, int):
            raise StateError("dashboard session expiry must be an integer epoch")
        _require_iso(iso, "dashboard_session.created_iso")
        if expires_epoch <= int(_parse_iso(iso).timestamp()):
            raise StateError("dashboard session expiry must be in the future")
        with self._transaction():
            if (
                self._conn.execute("SELECT 1 FROM dashboard_account WHERE singleton = 1").fetchone()
                is None
            ):
                raise StateError("dashboard account is not configured")
            self._conn.execute(
                "DELETE FROM dashboard_sessions WHERE expires_epoch <= ?",
                (int(_parse_iso(iso).timestamp()),),
            )
            active_count = _as_int(
                self._conn.execute("SELECT COUNT(*) FROM dashboard_sessions").fetchone()[0],
                "dashboard_sessions.count",
            )
            if active_count >= MAX_DASHBOARD_SESSIONS:
                return False
            cursor = self._conn.execute(
                "INSERT INTO dashboard_sessions"
                " (session_id_hash, csrf_hash, expires_epoch, created_iso)"
                " VALUES (?, ?, ?, ?) ON CONFLICT (session_id_hash) DO NOTHING",
                (session_id_hash, csrf_hash, expires_epoch, iso),
            )
            return cursor.rowcount == 1

    def get_dashboard_session(
        self, session_id_hash: str, now_epoch: int
    ) -> DashboardSession | None:
        _require_hex64(session_id_hash, "dashboard_session.session_id_hash")
        if isinstance(now_epoch, bool) or not isinstance(now_epoch, int):
            raise StateError("dashboard session clock must be an integer epoch")
        row = self._conn.execute(
            "SELECT session_id_hash, csrf_hash, expires_epoch, created_iso"
            " FROM dashboard_sessions WHERE session_id_hash = ?",
            (session_id_hash,),
        ).fetchone()
        if row is None or _as_int(row[2], "dashboard_session.expires_epoch") <= now_epoch:
            return None
        created_iso = _as_str(row[3], "dashboard_session.created_iso")
        _require_iso(created_iso, "dashboard_session.created_iso")
        return DashboardSession(
            _as_str(row[0], "dashboard_session.session_id_hash"),
            _as_str(row[1], "dashboard_session.csrf_hash"),
            _as_int(row[2], "dashboard_session.expires_epoch"),
            created_iso,
        )

    @_mutation
    def rotate_dashboard_session_csrf(
        self,
        session_id_hash: str,
        expected_csrf_hash: str,
        next_csrf_hash: str,
        now_epoch: int,
    ) -> bool:
        """Consume the current CSRF nonce exactly once and replace it atomically."""
        _require_hex64(session_id_hash, "dashboard_session.session_id_hash")
        _require_hex64(expected_csrf_hash, "dashboard_session.expected_csrf_hash")
        _require_hex64(next_csrf_hash, "dashboard_session.next_csrf_hash")
        if isinstance(now_epoch, bool) or not isinstance(now_epoch, int):
            raise StateError("dashboard session clock must be an integer epoch")
        with self._transaction():
            cursor = self._conn.execute(
                "UPDATE dashboard_sessions SET csrf_hash = ?"
                " WHERE session_id_hash = ? AND csrf_hash = ? AND expires_epoch > ?",
                (next_csrf_hash, session_id_hash, expected_csrf_hash, now_epoch),
            )
        return cursor.rowcount == 1

    def get_service_rate_state_json(self) -> str | None:
        """Return the bounded adaptive-rate controller state, if initialized."""
        encoded = self.get_meta(META_SERVICE_RATE_STATE)
        if encoded is not None and len(encoded) > MAX_SERVICE_RATE_JSON_CHARS:
            raise StateError("persisted service rate state exceeds its size limit")
        return encoded

    @_mutation
    def save_service_rate_state_json(self, document_json: str, iso: str) -> None:
        """Persist one validated, bounded, non-secret adaptive-rate state document."""
        if not isinstance(document_json, str) or len(document_json) > MAX_SERVICE_RATE_JSON_CHARS:
            raise StateError("service rate state must be bounded JSON text")
        _require_iso(iso, "service_rate.updated_iso")
        try:
            document = json.loads(document_json)
        except (TypeError, ValueError):
            raise StateError("service rate state must be a JSON object") from None
        expected = {"schema", "cap", "successful_streak", "updated_iso", "last_reason"}
        if not isinstance(document, dict) or set(document) != expected:
            raise StateError("service rate state must be a JSON object with the supported fields")
        if document["updated_iso"] != iso:
            raise StateError("service rate state timestamp does not match its persistence time")
        with self._transaction():
            self._set_meta_tx(META_SERVICE_RATE_STATE, document_json)

    def get_service_mapping_drifts(self) -> tuple[ServiceMappingDrift, ...]:
        """Return durable review markers for verified files with changed mapping generations."""
        rows = self._conn.execute(
            "SELECT key, value FROM meta WHERE key GLOB ? ORDER BY key",
            (f"{META_SERVICE_MAPPING_DRIFT_PREFIX}*",),
        ).fetchall()
        result: list[ServiceMappingDrift] = []
        for row in rows:
            key = _as_str(row[0], "service_mapping_drift.key")
            suffix = key.removeprefix(META_SERVICE_MAPPING_DRIFT_PREFIX)
            try:
                file_id = int(suffix)
            except ValueError:
                raise StateError("service mapping drift key is malformed") from None
            if file_id < 1 or str(file_id) != suffix:
                raise StateError("service mapping drift key is malformed")
            encoded = _as_str(row[1], "service_mapping_drift.document")
            result.append(self._decode_service_mapping_drift(file_id, encoded))
        return tuple(result)

    @_mutation
    def record_service_mapping_drift(
        self,
        shoko_file_id: int,
        *,
        previous_targets_digest: str | None,
        active_targets_digest: str,
        previous_structure_digest: str | None,
        active_structure_digest: str,
        mapping_revision: str,
        iso: str,
        reason: str = "mapping_target_changed",
    ) -> ServiceMappingDrift:
        """Persist a verified file's changed mapping identity without changing its Shoko XRefs."""
        if (
            isinstance(shoko_file_id, bool)
            or not isinstance(shoko_file_id, int)
            or shoko_file_id < 1
        ):
            raise StateError("service mapping drift file id must be a positive integer")
        for name, digest, optional in (
            ("previous_targets_digest", previous_targets_digest, True),
            ("active_targets_digest", active_targets_digest, False),
            ("previous_structure_digest", previous_structure_digest, True),
            ("active_structure_digest", active_structure_digest, False),
        ):
            if digest is None and optional:
                continue
            if (
                not isinstance(digest, str)
                or len(digest) != DIGEST_HEX_CHARS
                or any(char not in "0123456789abcdef" for char in digest)
            ):
                raise StateError(f"service mapping drift {name} must be a SHA-256 digest")
        if not isinstance(mapping_revision, str) or not mapping_revision.strip():
            raise StateError("service mapping drift revision must be non-empty text")
        if len(mapping_revision) > MAX_DETAIL_CHARS:
            raise StateError("service mapping drift revision is too long")
        if not isinstance(reason, str) or not reason.strip() or len(reason) > MAX_DETAIL_CHARS:
            raise StateError("service mapping drift reason must be non-empty bounded text")
        reason = reason.strip()
        _require_iso(iso, "service_mapping_drift.detected_iso")
        key = f"{META_SERVICE_MAPPING_DRIFT_PREFIX}{shoko_file_id}"
        with self._transaction():
            file_row = self._conn.execute(
                "SELECT state FROM files WHERE shoko_file_id = ?", (shoko_file_id,)
            ).fetchone()
            if file_row is None or _as_str(file_row[0], "files.state") != FileState.VERIFIED.value:
                raise StateError("service mapping drift can only be recorded for a verified file")
            existing_row = self._conn.execute(
                "SELECT value FROM meta WHERE key = ?", (key,)
            ).fetchone()
            if existing_row is not None:
                existing = self._decode_service_mapping_drift(
                    shoko_file_id, _as_str(existing_row[0], "service_mapping_drift.document")
                )
                previous_targets_digest = existing.previous_targets_digest
                previous_structure_digest = existing.previous_structure_digest
                if (
                    existing.active_targets_digest == active_targets_digest
                    and existing.active_structure_digest == active_structure_digest
                    and existing.mapping_revision == mapping_revision.strip()
                    and existing.reason == reason
                ):
                    # Repeated reconciliation of the same mapping change is idempotent; keep
                    # the first durable detection time instead of rewriting the marker.
                    return existing
            record = ServiceMappingDrift(
                shoko_file_id=shoko_file_id,
                previous_targets_digest=previous_targets_digest,
                active_targets_digest=active_targets_digest,
                previous_structure_digest=previous_structure_digest,
                active_structure_digest=active_structure_digest,
                mapping_revision=mapping_revision.strip(),
                detected_iso=iso,
                reason=reason,
            )
            document = {
                "schema": SERVICE_MAPPING_DRIFT_SCHEMA,
                "shoko_file_id": record.shoko_file_id,
                "previous_targets_digest": record.previous_targets_digest,
                "active_targets_digest": record.active_targets_digest,
                "previous_structure_digest": record.previous_structure_digest,
                "active_structure_digest": record.active_structure_digest,
                "mapping_revision": record.mapping_revision,
                "detected_iso": record.detected_iso,
                "reason": record.reason,
            }
            encoded = json.dumps(document, sort_keys=True, separators=(",", ":"))
            if len(encoded) > MAX_DETAIL_CHARS:
                raise StateError("service mapping drift record exceeds its size limit")
            self._set_meta_tx(key, encoded)
        return record

    @staticmethod
    def _decode_service_mapping_drift(file_id: int, encoded: str) -> ServiceMappingDrift:
        if not isinstance(encoded, str) or len(encoded) > MAX_DETAIL_CHARS:
            raise StateError("persisted service mapping drift exceeds its size limit")
        try:
            document = json.loads(encoded)
        except (TypeError, ValueError):
            raise StateError("persisted service mapping drift is malformed") from None
        expected_v1 = {
            "schema",
            "shoko_file_id",
            "previous_targets_digest",
            "active_targets_digest",
            "previous_structure_digest",
            "active_structure_digest",
            "mapping_revision",
            "detected_iso",
        }
        expected_v2 = expected_v1 | {"reason"}
        schema = document.get("schema") if isinstance(document, dict) else None
        if (
            not isinstance(document, dict)
            or schema not in {SERVICE_MAPPING_DRIFT_SCHEMA_V1, SERVICE_MAPPING_DRIFT_SCHEMA}
            or set(document)
            != (expected_v1 if schema == SERVICE_MAPPING_DRIFT_SCHEMA_V1 else expected_v2)
            or isinstance(document["shoko_file_id"], bool)
            or not isinstance(document["shoko_file_id"], int)
            or document["shoko_file_id"] != file_id
        ):
            raise StateError("persisted service mapping drift has an unsupported shape")
        try:
            previous_targets = document["previous_targets_digest"]
            previous_structure = document["previous_structure_digest"]
            active_targets = document["active_targets_digest"]
            active_structure = document["active_structure_digest"]
            revision = document["mapping_revision"]
            detected_iso = document["detected_iso"]
            reason = (
                "mapping_drift_details_unavailable"
                if schema == SERVICE_MAPPING_DRIFT_SCHEMA_V1
                else document["reason"]
            )
            for name, digest, optional in (
                ("previous_targets_digest", previous_targets, True),
                ("active_targets_digest", active_targets, False),
                ("previous_structure_digest", previous_structure, True),
                ("active_structure_digest", active_structure, False),
            ):
                if digest is None and optional:
                    continue
                if (
                    not isinstance(digest, str)
                    or len(digest) != DIGEST_HEX_CHARS
                    or any(char not in "0123456789abcdef" for char in digest)
                ):
                    raise ValueError(f"{name} is invalid")
            if not isinstance(revision, str) or not revision or len(revision) > MAX_DETAIL_CHARS:
                raise ValueError("mapping_revision is invalid")
            if not isinstance(reason, str) or not reason.strip() or len(reason) > MAX_DETAIL_CHARS:
                raise ValueError("reason is invalid")
            if not isinstance(detected_iso, str):
                raise ValueError("detected_iso is invalid")
            _require_iso(detected_iso, "service_mapping_drift.detected_iso")
            return ServiceMappingDrift(
                shoko_file_id=file_id,
                previous_targets_digest=previous_targets,
                active_targets_digest=active_targets,
                previous_structure_digest=previous_structure,
                active_structure_digest=active_structure,
                mapping_revision=revision,
                detected_iso=detected_iso,
                reason=reason,
            )
        except (TypeError, ValueError, StateError) as exc:
            raise StateError(
                f"persisted service mapping drift contains invalid values: {exc}"
            ) from None

    def get_service_canary(self) -> tuple[int, str] | None:
        """Return the durable first-send canary identity and its monotonic status."""
        raw_file_id = self.get_meta(META_SERVICE_CANARY_FILE_ID)
        raw_status = self.get_meta(META_SERVICE_CANARY_STATUS)
        if raw_file_id is None and raw_status is None:
            return None
        if raw_file_id is None or raw_status not in {"pending", "verified", "failed"}:
            raise StateError("service canary metadata is incomplete or invalid")
        try:
            file_id = int(raw_file_id)
        except ValueError:
            raise StateError("service canary file id is invalid") from None
        if file_id < 1:
            raise StateError("service canary file id is invalid")
        updated_iso = self.get_meta(META_SERVICE_CANARY_UPDATED_ISO)
        if updated_iso is None:
            raise StateError("service canary timestamp is missing")
        _require_iso(updated_iso, "service_canary.updated_iso")
        return file_id, raw_status

    @_mutation
    def record_service_canary(
        self, shoko_file_id: int, iso: str, *, send_id: int | None = None
    ) -> int:
        """Durably designate exactly one first service submission; never replace its identity."""
        if (
            isinstance(shoko_file_id, bool)
            or not isinstance(shoko_file_id, int)
            or shoko_file_id < 1
        ):
            raise StateError("service canary file id must be a positive integer")
        if send_id is not None and (
            isinstance(send_id, bool) or not isinstance(send_id, int) or send_id < 1
        ):
            raise StateError("service canary send id must be a positive integer")
        _require_iso(iso, "service_canary.updated_iso")
        with self._transaction():
            existing = self._conn.execute(
                "SELECT value FROM meta WHERE key = ?", (META_SERVICE_CANARY_FILE_ID,)
            ).fetchone()
            if existing is not None:
                raw = _as_str(existing[0], "service_canary.file_id")
                try:
                    existing_file_id = int(raw)
                except ValueError:
                    raise StateError("service canary file id is invalid") from None
                if existing_file_id < 1 or str(existing_file_id) != raw:
                    raise StateError("service canary file id is invalid")
                return existing_file_id
            entries = [
                (META_SERVICE_CANARY_FILE_ID, str(shoko_file_id)),
                (META_SERVICE_CANARY_STATUS, "pending"),
                (META_SERVICE_CANARY_UPDATED_ISO, iso),
            ]
            if send_id is not None:
                send = self._conn.execute(
                    "SELECT shoko_file_id FROM sends WHERE send_id = ?", (send_id,)
                ).fetchone()
                if send is None or _as_int(send[0], "shoko_file_id") != shoko_file_id:
                    raise StateError("service canary send does not belong to its file")
                entries.append((META_SERVICE_CANARY_SEND_ID, str(send_id)))
            self._conn.executemany("INSERT INTO meta (key, value) VALUES (?, ?)", entries)
        return shoko_file_id

    def service_canary_send_outcomes(self) -> tuple[str, ...]:
        """Return attempted outcomes since the durable canary send anchor, in send order."""
        canary = self.get_service_canary()
        if canary is None:
            return ()
        raw_send_id = self.get_meta(META_SERVICE_CANARY_SEND_ID)
        if raw_send_id is None:
            return ()
        try:
            send_id = int(raw_send_id)
        except ValueError:
            raise StateError("service canary send id is invalid") from None
        if send_id < 1 or str(send_id) != raw_send_id:
            raise StateError("service canary send id is invalid")
        rows = self._conn.execute(
            "SELECT outcome FROM sends WHERE shoko_file_id = ? AND send_id >= ?"
            " AND attempted_iso IS NOT NULL ORDER BY send_id",
            (canary[0], send_id),
        ).fetchall()
        return tuple(_as_str(row[0], "send.outcome") for row in rows)

    @_mutation
    def set_service_canary_status(self, status: Literal["verified", "failed"], iso: str) -> None:
        """Close the canary once as verified or failed; terminal results cannot be revised."""
        if status not in {"verified", "failed"}:
            raise StateError("service canary status must be verified or failed")
        _require_iso(iso, "service_canary.updated_iso")
        with self._transaction():
            current = self._conn.execute(
                "SELECT value FROM meta WHERE key = ?", (META_SERVICE_CANARY_STATUS,)
            ).fetchone()
            if current is None:
                raise StateError("service canary has not been recorded")
            current_status = _as_str(current[0], "service_canary.status")
            if current_status == status:
                return
            if current_status != "pending":
                raise StateError("service canary cannot change terminal status")
            self._conn.execute(
                "UPDATE meta SET value = ? WHERE key = ?",
                (status, META_SERVICE_CANARY_STATUS),
            )
            self._conn.execute(
                "UPDATE meta SET value = ? WHERE key = ?",
                (iso, META_SERVICE_CANARY_UPDATED_ISO),
            )

    def get_onboarding_json(self) -> str | None:
        """Return the single sanitized onboarding document, if one is persisted."""
        row = self._conn.execute(
            "SELECT document_json FROM onboarding WHERE singleton = 1"
        ).fetchone()
        return None if row is None else _as_optional_str(row[0])

    @_mutation
    def save_onboarding_json(self, document_json: str, iso: str) -> None:
        """Persist a JSON object containing onboarding facts, never API-key values."""
        with self._transaction():
            self._save_onboarding_json_tx(document_json, iso)

    @_mutation
    def confirm_onboarding_json(
        self,
        document_json: str,
        iso: str,
        *,
        expected_settings_json: str | None = None,
        check_settings: bool = False,
        expected_authorization_revision: str | None = None,
        expected_draft_json: str | None = None,
    ) -> None:
        """Publish validated consent and release its previous review hold atomically.

        Only the onboarding manager calls this after validating explicit confirmation.
        A review of the new consent can commit afterwards and must remain effective.
        Read pause ownership under the writer lock so an existing user pause survives.
        """
        with self._transaction():
            if check_settings and (
                self.get_meta(META_SERVICE_SETTINGS) != expected_settings_json
                or self.get_service_authorization_revision() != expected_authorization_revision
                or self.get_meta(META_SERVICE_DRAFT_ROOTS) != expected_draft_json
            ):
                raise StateError("Service settings changed during setup; review setup again.")
            self._save_onboarding_json_tx(document_json, iso)
            if check_settings:
                self._conn.execute("DELETE FROM meta WHERE key = ?", (META_SERVICE_DRAFT_ROOTS,))
                # Consume this reviewed intent, including mapping choices submitted
                # directly with consent, so a competing form cannot overwrite it.
                self._set_meta_tx(META_SERVICE_AUTHORIZATION_REVISION, uuid.uuid4().hex)
            self._conn.execute(
                "DELETE FROM meta WHERE key = ?", (META_ONBOARDING_RECONFIRMATION_REQUIRED,)
            )
            self._conn.execute(
                "UPDATE onboarding SET paused = 0, pause_reason = '' WHERE singleton = 1"
                " AND paused = 1 AND pause_reason IN"
                " ('setup_changed', 'setup_unavailable', 'unsafe_shoko_settings')"
            )

    def _save_onboarding_json_tx(self, document_json: str, iso: str) -> None:
        """Validate and save the document while the caller holds its write transaction."""
        if not isinstance(document_json, str) or len(document_json) > MAX_ONBOARDING_JSON_CHARS:
            raise StateError("onboarding document must be bounded JSON text")
        _require_iso(iso, "onboarding.updated_iso")
        try:
            document = json.loads(document_json)
        except (TypeError, ValueError):
            raise StateError("onboarding document must be a JSON object") from None
        if not isinstance(document, dict):
            raise StateError("onboarding document must be a JSON object")
        self._conn.execute(
            "INSERT INTO onboarding (singleton, document_json, updated_iso)"
            " VALUES (1, ?, ?) ON CONFLICT (singleton) DO UPDATE SET"
            " document_json = excluded.document_json, updated_iso = excluded.updated_iso",
            (document_json, iso),
        )

    @_mutation
    def pause_onboarding_if_current(
        self, expected_document_json: str | None, reason: str, iso: str
    ) -> bool:
        """Atomically hold only the consent that preceded a read-only observation."""
        if reason not in SYSTEM_PAUSE_REASONS:
            raise StateError("unsupported observed onboarding review reason")
        _require_iso(iso, "onboarding.updated_iso")
        with self._transaction():
            row = self._conn.execute(
                "SELECT document_json FROM onboarding WHERE singleton = 1"
            ).fetchone()
            if (None if row is None else row[0]) != expected_document_json:
                return False
            self._pause_service_for_review_tx(reason, iso)
        return True

    @_mutation
    def observe_onboarding_facts(
        self,
        expected_document_json: str,
        *,
        expected_authorization_revision: str | None,
        channel: str,
        fingerprint: str,
        reason: str | None,
        iso: str,
    ) -> bool:
        """Fence a bounded, credential-free observation against its pre-I/O snapshot.

        Repeated facts are idempotent. Recording intervening safe/different facts
        means a returning hazard is new even when its reason is unchanged. Both
        positive and negative observations reject stale consent or generations.
        """
        if channel not in {"environment", "folders", "evaluation"}:
            raise StateError("unsupported onboarding observation channel")
        _require_hex64(fingerprint, "onboarding observation fingerprint")
        if reason is not None and reason not in SYSTEM_RECONFIRMATION_PAUSE_REASONS:
            raise StateError("unsupported observed onboarding review reason")
        _require_iso(iso, "onboarding.updated_iso")
        with self._transaction():
            if (
                self.get_onboarding_json() != expected_document_json
                or self.get_service_authorization_revision() != expected_authorization_revision
            ):
                return False
            key = "onboarding_observation_v1:" + channel
            identity = hashlib.sha256(
                (expected_document_json + "\n" + fingerprint + "\n" + str(reason)).encode()
            ).hexdigest()
            if self.get_meta(key) == identity:
                return True
            self._set_meta_tx(key, identity)
            if reason is not None:
                self._pause_service_for_review_tx(reason, iso)
        return True

    @_mutation
    def pause_service_for_review(self, reason: str, iso: str) -> None:
        """Latch a system hold without replacing an intentional user pause."""
        if reason not in SYSTEM_PAUSE_REASONS:
            raise StateError("unsupported automatic-service review pause reason")
        _require_iso(iso, "onboarding.updated_iso")
        with self._transaction():
            self._pause_service_for_review_tx(reason, iso)

    def _pause_service_for_review_tx(self, reason: str, iso: str) -> None:
        """Apply system-pause precedence under the caller's writer transaction."""
        paused, existing_reason = self.get_service_pause()
        hard_hold = reason in SYSTEM_RECONFIRMATION_PAUSE_REASONS
        existing_hard_hold = (
            paused and existing_reason in SYSTEM_RECONFIRMATION_PAUSE_REASONS
        ) or self.get_meta(META_ONBOARDING_RECONFIRMATION_REQUIRED) is not None
        if hard_hold:
            # Every new hard observation/explicit intent invalidates forms which
            # preceded it, even if a hold with the same reason was already set.
            self._set_meta_tx(META_SERVICE_AUTHORIZATION_REVISION, uuid.uuid4().hex)
        if hard_hold or existing_hard_hold:
            self._set_meta_tx(META_ONBOARDING_RECONFIRMATION_REQUIRED, "1")
        if (
            (paused and existing_reason == "user_paused")
            or (not hard_hold and existing_hard_hold)
            or (paused and existing_reason == "unsafe_shoko_settings")
        ):
            return
        self._conn.execute(
            "INSERT INTO onboarding (singleton, document_json, paused, pause_reason, updated_iso)"
            " VALUES (1, NULL, 1, ?, ?) ON CONFLICT (singleton) DO UPDATE SET"
            " paused = 1, pause_reason = excluded.pause_reason, updated_iso = excluded.updated_iso",
            (reason, iso),
        )

    @_mutation
    def pause_service_for_user(self, iso: str) -> None:
        """Pause explicitly and preserve only a currently effective hard hold.

        Read legacy pause reasons under the writer lock, so fresh confirmation
        cannot clear a hold between its observation and migration to metadata.
        """
        _require_iso(iso, "onboarding.updated_iso")
        with self._transaction():
            paused, reason = self.get_service_pause()
            if paused and reason in SYSTEM_RECONFIRMATION_PAUSE_REASONS:
                self._set_meta_tx(META_ONBOARDING_RECONFIRMATION_REQUIRED, "1")
            self._set_service_pause_tx(True, "user_paused", iso)

    @_mutation
    def resume_onboarding_if_current(self, expected_document_json: str, iso: str) -> bool:
        """Resume validated consent only if it still has no hard safety hold."""
        _require_iso(iso, "onboarding.updated_iso")
        with self._transaction():
            if self.get_onboarding_json() != expected_document_json:
                return False
            paused, reason = self.get_service_pause()
            if self.get_meta(META_ONBOARDING_RECONFIRMATION_REQUIRED) is not None or (
                paused and reason in SYSTEM_RECONFIRMATION_PAUSE_REASONS
            ):
                return False
            self._set_service_pause_tx(False, "", iso)
        return True

    def get_service_pause(self) -> tuple[bool, str]:
        """Return the durable normal-service pause flag and reason; absent state is unpaused."""
        row = self._conn.execute(
            "SELECT paused, pause_reason FROM onboarding WHERE singleton = 1"
        ).fetchone()
        if row is None:
            return False, ""
        paused = _as_int(row[0], "onboarding.paused")
        if paused not in (0, 1):
            raise StateError("onboarding.paused: persisted value is invalid")
        return bool(paused), _as_str(row[1], "onboarding.pause_reason")

    @_mutation
    def set_service_pause(self, paused: bool, reason: str, iso: str) -> None:
        """Persist Pause/Resume independently of automatic-linking authorization."""
        if not isinstance(paused, bool):
            raise StateError("onboarding.paused: expected a boolean")
        if not isinstance(reason, str) or len(reason) > MAX_DETAIL_CHARS:
            raise StateError("onboarding.pause_reason: invalid text")
        if paused and not reason.strip():
            raise StateError("onboarding.pause_reason: a paused service requires a reason")
        if not paused and reason:
            raise StateError("onboarding.pause_reason: a resumed service must clear the reason")
        _require_iso(iso, "onboarding.updated_iso")
        with self._transaction():
            self._set_service_pause_tx(paused, reason, iso)

    def _set_service_pause_tx(self, paused: bool, reason: str, iso: str) -> None:
        """Write pause state under the caller's writer transaction."""
        self._conn.execute(
            "INSERT INTO onboarding (singleton, document_json, paused, pause_reason,"
            " updated_iso)"
            " VALUES (1, NULL, ?, ?, ?) ON CONFLICT (singleton) DO UPDATE SET"
            " paused = excluded.paused, pause_reason = excluded.pause_reason,"
            " updated_iso = excluded.updated_iso",
            (int(paused), _clip(reason), iso),
        )

    def disable_writes(self, reason: str) -> None:
        """Stop writes (fail closed). Persistence is best-effort; the in-memory stop is
        authoritative — there is no in-memory submission tracking fallback."""
        self._latch_writes_disabled(reason)
        with (
            contextlib.suppress(sqlite3.DatabaseError),
            self._write_stop_control_transaction(),
        ):
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
            with self._write_stop_control_transaction():
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
        campaign_id: str | None = None,
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

    def service_file_counts(self) -> ServiceFileCounts:
        """Classify all persisted files for ordinary status without remote access."""
        verified_states = frozenset({FileState.VERIFIED})
        classified_states = (
            _SERVICE_WAITING_FILE_STATES | _SERVICE_REVIEW_FILE_STATES | verified_states
        )
        if (
            _SERVICE_WAITING_FILE_STATES & _SERVICE_REVIEW_FILE_STATES
            or _SERVICE_WAITING_FILE_STATES & verified_states
            or _SERVICE_REVIEW_FILE_STATES & verified_states
            or classified_states != frozenset(FileState)
        ):
            raise StateError(
                "ordinary status file-state classification is incomplete or overlapping"
            )
        rows = self._conn.execute("SELECT state, COUNT(*) FROM files GROUP BY state").fetchall()
        counts: dict[FileState, int] = {}
        for raw_state, raw_count in rows:
            value = _as_str(raw_state, "file state")
            try:
                state = FileState(value)
            except ValueError as exc:
                raise StateError(f"file state {value!r} is unsupported") from exc
            counts[state] = _as_int(raw_count, "file count")
        return ServiceFileCounts(
            verified_files=counts.get(FileState.VERIFIED, 0),
            waiting_files=sum(counts.get(state, 0) for state in _SERVICE_WAITING_FILE_STATES),
            review_files=sum(counts.get(state, 0) for state in _SERVICE_REVIEW_FILE_STATES),
        )

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
        attestation_valid: bool | None,
        gate_open: bool | None,
        budgets_ok: bool,
        mapping_stable: bool,
        source_stable: bool,
        service_mode: bool = False,
        service_authorized: bool | None = None,
        service_backlog: bool = False,
    ) -> EligibilityVerdict:
        """Pure composition of durable facts + caller-supplied external facts.

        Both paths require a complete eligible baseline, a ``new_arrival`` file, budget
        headroom, stable mappings/source, and no pending rebaseline. Advanced mode additionally
        requires a valid attestation and open apply gate; service mode uses its separate current
        onboarding authorization instead. Backlog/campaign files never use this auto-apply path.
        """
        _require_iso(now, "now")
        reasons = self._eligibility_reasons(
            shoko_file_id, now, allow_backlog=service_mode and service_backlog
        )
        authorization_gates = (
            ((service_authorized is True, "onboarding_not_authorized"),)
            if service_mode
            else (
                (gate_open is True, "apply_gate_closed"),
                (attestation_valid is True, "attestation_invalid"),
            )
        )
        gates = (
            *authorization_gates,
            (budgets_ok, "budgets_exhausted"),
            (mapping_stable, "mapping_unstable"),
            (source_stable, "source_unstable"),
        )
        reasons.extend(label for open_flag, label in gates if not open_flag)
        return EligibilityVerdict(not reasons, tuple(reasons))

    def _eligibility_reasons(
        self, shoko_file_id: int, now: str, *, allow_backlog: bool = False
    ) -> list[str]:
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
        elif record.cohort is Cohort.BACKLOG and allow_backlog:
            pass
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
                reservation_events=self._reservation_events(),
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
            reservation_events=self._reservation_events(),
        )

    def _reservation_events(self) -> tuple[tuple[float, int], ...]:
        return tuple(
            (float(row[0]), int(row[1]))
            for row in self._conn.execute(
                "SELECT reserved_epoch, reservations FROM budget_reservations"
                " ORDER BY reserved_epoch"
            )
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

    @_mutation
    def begin_cycle(self, prefix: str, iso: str) -> str:
        """Allocate this cycle's/batch's durable budget-window identity (F9).

        The window_key is ``<label>-<run_id>-<seq>``: ``run_id`` is a per-install nonce written
        at first use, ``seq`` is a persisted monotonic counter advanced **before** any
        submission. Combining them means every reconciliation cycle and every campaign batch
        gets a distinct durable row that is unique across process restarts — a new process
        starts fresh per-cycle counters instead of colliding with a prior run's persisted
        ``cycle-1`` / constant campaign key. The key stays stable for the whole executing
        cycle/batch (the caller threads it through every ``prepare_send`` / ``mark_completed``).

        Only *completed* per-cycle rows beyond :data:`BUDGET_CYCLE_RETENTION` are pruned; the
        ``_global`` rolling row and the freshly allocated window are never touched, so the
        global/rolling ceilings keep applying across everything and live/pending accounting
        (which lives on the ``_global`` row via the send-row release funnels) is intact.
        """
        _require_iso(iso, "iso")
        label = prefix.strip()
        if not label:
            raise StateError("begin_cycle prefix must be a non-empty label")
        with self._transaction():
            self._ensure_global_budget_tx()
            run_id = self.get_meta(META_BUDGET_RUN_ID)
            if run_id is None:
                run_id = uuid.uuid4().hex
                self._set_meta_tx(META_BUDGET_RUN_ID, run_id)
            raw_seq = self.get_meta(META_BUDGET_SEQ)
            try:
                parsed_seq = int(raw_seq) if raw_seq is not None else 0
            except ValueError:
                # F9-2: a corrupted counter must NOT silently restart at 1 (that re-issues
                # ``<label>-<run_id>-1`` and collides with the first persisted window row).
                # Seed from the highest numeric suffix already present for this run.
                parsed_seq = 0
            seq = max(parsed_seq, self._budget_run_seq_floor_tx(run_id)) + 1
            self._set_meta_tx(META_BUDGET_SEQ, str(seq))
            window_key = f"{label}-{run_id}-{seq}"
            self._prune_budget_rows_tx(window_key)
            self._set_meta_tx(META_BUDGET_CYCLE_KEY, window_key)
        return window_key

    def _budget_run_seq_floor_tx(self, run_id: str) -> int:
        """Highest ``-<run_id>-<n>`` window-key suffix in ``budgets`` (F9-2 seed floor).

        Used to make the ``begin_cycle`` sequence monotone even when the persisted
        counter meta is corrupted, so a fresh key can never collide with an existing row.
        """
        marker = f"-{run_id}-"
        floor = 0
        for row in self._conn.execute("SELECT window_key FROM budgets").fetchall():
            text = _as_str(row[0], "budgets.window_key")
            head, sep, tail = text.rpartition(marker)
            if sep and head and tail.isdigit():
                floor = max(floor, int(tail))
        return floor

    def _prune_budget_rows_tx(self, keep: str) -> None:
        """Delete *completed* per-cycle budget rows beyond the retention bound (F9).

        Never the ``_global`` rolling row, never the freshly allocated current window. Pending
        file/job accounting lives only on the ``_global`` row and every live send is closed
        through the send-row release funnels (F1/P1-A), so dropping an old per-cycle counter row
        can never strand a pending/prepared row; crash recovery reads the persisted ``sends``
        rows (request-fingerprint dedup), which this pruning never deletes.
        """
        self._conn.execute(
            "DELETE FROM budgets"
            " WHERE window_key <> ? AND window_key <> ?"
            " AND rowid NOT IN ("
            "SELECT rowid FROM budgets"
            " WHERE window_key <> ? AND window_key <> ?"
            " ORDER BY rowid DESC LIMIT ?)",
            (GLOBAL_BUDGET_KEY, keep, GLOBAL_BUDGET_KEY, keep, BUDGET_CYCLE_RETENTION),
        )

    def _reserve_tx(
        self, cycle_key: str, iso: str, *, expected_episode_jobs: int, campaign: bool = False
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
        # F8/F24: the daemon attestation pause and daemon-origin mapping-churn pause may be
        # bypassed by an authorized campaign. Both are origin-gated; operator pauses and every
        # other global pause still gate all scopes. Mapping churn is safe for a campaign because
        # every approved candidate is re-evaluated against its immutable target set.
        daemon_attestation_pause = (
            paused == ATTESTATION_PAUSE_REASON
            and _as_optional_str(self.get_meta(META_ATTESTATION_GATE)) == "paused"
            and _as_optional_str(self.get_meta(META_GLOBAL_PAUSE_ORIGIN))
            in (None, "attestation_gate")
        )
        daemon_mapping_churn_pause = (
            paused == "mapping_churn"
            and _as_optional_str(self.get_meta(META_GLOBAL_PAUSE_ORIGIN)) == "mapping_churn"
        )
        if paused and not (campaign and (daemon_attestation_pause or daemon_mapping_churn_pause)):
            return BudgetRefusal(False, "paused", f"writes paused: {paused}")
        events = self._reservation_events()
        cutoff = (_parse_iso(iso) - timedelta(hours=limits.rolling_window_hours)).timestamp()
        active_events = [(stamp, count) for stamp, count in events if stamp > cutoff]
        rolling = sum(count for _, count in active_events)
        rolling_start = (
            datetime.fromtimestamp(active_events[0][0], UTC).isoformat() if active_events else iso
        )
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
        if refusal is None:
            refusal = self._additional_rolling_refusal(limits, events, iso)
        if refusal is not None:
            # P3d: persist the refusal reason marker alongside the counters.
            self._set_meta_tx(
                META_LAST_REFUSAL,
                json.dumps({"reason": refusal.reason, "detail": refusal.detail, "iso": iso}),
            )
            return refusal
        rolling += 1
        self._conn.execute(
            "INSERT INTO budget_reservations VALUES (?, 1) ON CONFLICT (reserved_epoch)"
            " DO UPDATE SET reservations = reservations + 1",
            (_parse_iso(iso).timestamp(),),
        )
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
    def _additional_rolling_refusal(
        limits: BudgetLimits, events: tuple[tuple[float, int], ...], iso: str
    ) -> BudgetRefusal | None:
        for cap, hours in limits.additional_rolling_limits:
            cutoff = (_parse_iso(iso) - timedelta(hours=hours)).timestamp()
            count = sum(weight for stamp, weight in events if stamp > cutoff)
            if count >= cap:
                return BudgetRefusal(
                    False, "rolling_new_exhausted", f"{count} reservations in {hours}h (max {cap})"
                )
        return None

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

        Metadata-owned slots are released only by verified readiness or proven rejection.
        Send-bound slots are owned by the single-release funnels (``record_verification``,
        ``record_send_outcome``, ``void_prepared_send``, ``resolve_send``),
        which mark the send row ``slot_released``. A raw release here that would drive
        ``pending`` below the live unreleased-send ledger is refused (round-2 review F1):
        releasing a send-bound slot here bypassed the ledger and let a later VERIFIED close
        release the same reservation twice.
        """
        self._require_cycle_key(cycle_key)
        _require_iso(iso, "iso")
        with self._transaction():
            self._ensure_global_budget_tx()
            live_sends, live_jobs = self._owned_pending_counts_tx()
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
                    f"release_slot refused: live owned sends/metadata ({live_sends} requests / "
                    f"{live_jobs} jobs) outnumber the release; owned slots are released "
                    "only through record_verification / record_send_outcome / "
                    "void_prepared_send / resolve_send / record_metadata_operation (ledger guard)"
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

    def _campaign_file_is_approved_tx(self, campaign_id: str, shoko_file_id: int) -> bool:
        """Check file membership against persisted approved bindings, never a plan file."""
        row = self._conn.execute(
            "SELECT plan_fingerprint_set FROM campaigns WHERE campaign_id = ?", (campaign_id,)
        ).fetchone()
        if row is None:
            return False
        try:
            bindings = json.loads(_as_str(row[0], "plan_fingerprint_set"))
        except (TypeError, ValueError) as exc:
            raise StateError(f"campaign {campaign_id!r} has invalid approved bindings") from exc
        if not isinstance(bindings, list):
            raise StateError(f"campaign {campaign_id!r} has invalid approved bindings")
        return any(
            isinstance(item, list)
            and len(item) == CAMPAIGN_BINDING_FIELDS
            and not isinstance(item[0], bool)
            and item[0] == shoko_file_id
            for item in bindings
        )

    def _unresolved_campaign_send_ids_tx(self) -> tuple[int, ...]:
        rows = self._conn.execute(
            "SELECT send_id FROM sends WHERE campaign_ownership = ?"
            " AND outcome NOT IN ('prepared', 'voided') ORDER BY send_id",
            (CAMPAIGN_OWNERSHIP_UNKNOWN,),
        ).fetchall()
        return tuple(_as_int(row[0], "send_id") for row in rows)

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
    def pause(self, reason: str, iso: str, *, origin: str = "operator") -> None:
        """Persisted write pause (mapping churn, queue anomalies, lease, source breaks)."""
        _require_iso(iso, "iso")
        if not origin.strip():
            raise StateError("pause origin must be a non-empty string")
        with self._transaction():
            self._ensure_global_budget_tx()
            self._conn.execute(
                "UPDATE budgets SET paused_reason = ? WHERE window_key = ?",
                (_clip(reason), GLOBAL_BUDGET_KEY),
            )
            self._set_meta_tx(META_GLOBAL_PAUSE_ORIGIN, origin)

    @_mutation
    def pause_if_unpaused(self, reason: str, iso: str, *, origin: str) -> bool:
        """Install an origin-tagged pause only if no other pause has already won the race."""
        _require_iso(iso, "iso")
        if not origin.strip():
            raise StateError("pause origin must be a non-empty string")
        with self._transaction():
            self._ensure_global_budget_tx()
            changed = self._conn.execute(
                "UPDATE budgets SET paused_reason = ? WHERE window_key = ?"
                " AND paused_reason IS NULL",
                (_clip(reason), GLOBAL_BUDGET_KEY),
            ).rowcount
            if changed == 1:
                self._set_meta_tx(META_GLOBAL_PAUSE_ORIGIN, origin)
        return changed == 1

    @_mutation
    def resume(self, iso: str) -> None:
        _require_iso(iso, "iso")
        with self._transaction():
            self._ensure_global_budget_tx()
            self._conn.execute(
                "UPDATE budgets SET paused_reason = NULL WHERE window_key = ?",
                (GLOBAL_BUDGET_KEY,),
            )
            self._conn.execute("DELETE FROM meta WHERE key = ?", (META_GLOBAL_PAUSE_ORIGIN,))

    @_mutation
    def resume_if_pause(
        self,
        reason: str,
        iso: str,
        *,
        allowed_origins: Sequence[str | None],
    ) -> bool:
        """Atomically clear only the expected pause from an explicitly allowed origin."""
        _require_iso(iso, "iso")
        with self._transaction():
            self._ensure_global_budget_tx()
            row = self._conn.execute(
                "SELECT paused_reason FROM budgets WHERE window_key = ?",
                (GLOBAL_BUDGET_KEY,),
            ).fetchone()
            current_reason = None if row is None else _as_optional_str(row[0])
            origin = self.get_meta(META_GLOBAL_PAUSE_ORIGIN)
            if current_reason != reason or origin not in allowed_origins:
                return False
            changed = self._conn.execute(
                "UPDATE budgets SET paused_reason = NULL"
                " WHERE window_key = ? AND paused_reason = ?",
                (GLOBAL_BUDGET_KEY, reason),
            ).rowcount
            if changed != 1:
                return False
            self._conn.execute("DELETE FROM meta WHERE key = ?", (META_GLOBAL_PAUSE_ORIGIN,))
        return True

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
        campaign_id: str | None = None,
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
        Replay protection: a request fingerprint from any possibly-delivered send is
        refused as ``duplicate_request_fingerprint``, including after explicit operator
        resolution — a restart or resolution never makes the same mutation reusable.
        Only a ``voided`` prepared send (definitely not sent) stops blocking its replacement
        (P2-2); sent rows are never resubmitted (no transition edge).
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
            # Quarantine and replay protection precede any slot reservation:
            # an active send with the same request
            # fingerprint (prepared/attempted/accepted-unverified) never reserves a
            # second budget slot — replaying a plan after a restart is idempotent.
            # Voided/closed rows are excluded (P2-2): voided sends no longer block.
            if self.mapping_target_quarantined(shoko_file_id):
                return BudgetRefusal(
                    False,
                    "mapping_target_quarantined",
                    f"file {shoko_file_id} requires target review; no send may be prepared",
                )
            duplicate = self._conn.execute(
                "SELECT send_id, plan_fingerprint FROM sends WHERE request_fingerprint = ?"
                " AND outcome IN ('prepared', 'attempted', 'accepted_unverified', 'ambiguous_send',"
                " 'inspected_review', 'verified', 'resolved') LIMIT 1",
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
            if campaign_id is not None:
                campaign_row = self._conn.execute(
                    "SELECT status FROM campaigns WHERE campaign_id = ?", (campaign_id,)
                ).fetchone()
                if campaign_row is None:
                    raise StateError(f"campaign {campaign_id!r} does not exist: send is refused")
                status = _as_str(campaign_row[0], "campaign status")
                if status != CAMPAIGN_STATUS_APPROVED:
                    return BudgetRefusal(
                        False,
                        "campaign_not_approved",
                        f"campaign {campaign_id!r} is {status}: no send may be prepared",
                    )
                if not self._campaign_file_is_approved_tx(campaign_id, shoko_file_id):
                    raise StateError(
                        f"file {shoko_file_id} is not in the approved binding for campaign "
                        f"{campaign_id!r}"
                    )
                unresolved = self._unresolved_campaign_send_ids_tx()
                if unresolved:
                    return BudgetRefusal(
                        False,
                        "legacy_campaign_history_unresolved",
                        "legacy campaign send history is ambiguous; resolve send(s) "
                        + ", ".join(str(send_id) for send_id in unresolved),
                    )
            row = self._conn.execute(
                "SELECT state, cohort FROM files WHERE shoko_file_id = ?",
                (shoko_file_id,),
            ).fetchone()
            if row is None:
                raise TransitionError(f"file {shoko_file_id} has no tracked row to prepare")
            current = FileState(_as_str(row[0], "state"))
            validate_transition(current, FileState.PREPARED_NOT_SENT)
            decision = self._reserve_tx(
                cycle_key,
                iso,
                expected_episode_jobs=expected_episode_jobs,
                campaign=campaign_id is not None,
            )
            if isinstance(decision, BudgetRefusal):
                return decision
            cursor = self._conn.execute(
                "INSERT INTO sends (shoko_file_id, request_fingerprint, plan_fingerprint,"
                " campaign_id, campaign_ownership, attempted_iso, outcome, episode_jobs,"
                " target_ids_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    shoko_file_id,
                    request_fingerprint,
                    plan_fingerprint,
                    campaign_id,
                    CAMPAIGN_OWNERSHIP_CAMPAIGN
                    if campaign_id is not None
                    else CAMPAIGN_OWNERSHIP_NON_CAMPAIGN,
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
        resubmission is forbidden by the transition table. For campaign rows, this same
        transaction reconciles prior ledger history, enforces ``max_submissions``, and debits
        ``total_sent`` before transport can begin. A crash after this commit is conservatively
        counted, and outcome recording never refunds it.
        """
        _require_iso(iso, "iso")
        limit_reached: tuple[str, int, int] | None = None
        with self._transaction():
            row = self._conn.execute(
                "SELECT state FROM files WHERE shoko_file_id = ?", (token.shoko_file_id,)
            ).fetchone()
            if row is None:
                raise TransitionError(f"file {token.shoko_file_id} has no tracked row")
            current = FileState(_as_str(row[0], "state"))
            send_row = self._conn.execute(
                "SELECT outcome, campaign_id, campaign_ownership"
                " FROM sends WHERE send_id = ? AND shoko_file_id = ?",
                (token.send_id, token.shoko_file_id),
            ).fetchone()
            if send_row is None:
                raise TransitionError(
                    f"send {token.send_id} for file {token.shoko_file_id} does not exist"
                )
            if _as_str(send_row[0], "send outcome") != OUTCOME_PREPARED:
                raise StateError(f"send {token.send_id} is not a prepared, not-yet-attempted send")
            validate_transition(current, FileState.REQUEST_MAY_HAVE_BEEN_SENT)
            campaign_id = self._campaign_owner_for_prepared_send_tx(token, send_row)
            if campaign_id is not None:
                limit_reached = self._debit_campaign_attempt_tx(campaign_id, token, iso)
            if limit_reached is None:
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
        if limit_reached is not None:
            campaign_id, total, limit = limit_reached
            raise CampaignSubmissionLimitError(
                f"campaign {campaign_id!r} reached max_submissions ({limit}) at {total}: "
                "send was not attempted"
            )

    @contextlib.contextmanager
    def protect_transport(self, token: SendToken) -> Iterator[None]:
        """Fence transport admission against a concurrent durable write stop.

        ``mark_sent`` remains committed before transport. This fresh, write-serialized check
        closes the mark-to-transport gap when a stop has already committed. Once this check
        succeeds, that attempted row is the one already-authorized in-flight operation: a later
        stop applies to subsequent sends, not retroactively to this operation. Callers must not
        do unrelated work in this context.
        """
        try:
            with self._transaction():
                row = self._conn.execute(
                    "SELECT outcome FROM sends WHERE send_id = ? AND shoko_file_id = ?",
                    (token.send_id, token.shoko_file_id),
                ).fetchone()
                if row is None or _as_str(row[0], "send outcome") != OUTCOME_ATTEMPTED:
                    raise StateError(f"send {token.send_id} is not durably marked before transport")
            yield
        except WriteDisabledError:
            raise
        except sqlite3.DatabaseError as exc:
            self.disable_writes(f"database failure before transport: {exc}")
            raise WriteDisabledError(f"state writes stopped before transport: {exc}") from exc

    def _campaign_owner_for_prepared_send_tx(self, token: SendToken, send_row: Any) -> str | None:
        """Validate the captured owner and return it; caller holds the send transaction."""
        campaign_id = _as_optional_str(send_row[1])
        ownership = _as_str(send_row[2], "campaign ownership")
        if ownership == CAMPAIGN_OWNERSHIP_UNKNOWN:
            raise CampaignSendRefusedError(
                f"send {token.send_id} has unresolved legacy campaign ownership; "
                "resolve campaign send history before transport"
            )
        if ownership == CAMPAIGN_OWNERSHIP_NON_CAMPAIGN:
            if campaign_id is not None:
                raise StateError("non-campaign send unexpectedly carries a campaign id")
            return None
        if ownership != CAMPAIGN_OWNERSHIP_CAMPAIGN:
            raise StateError(f"send {token.send_id} has invalid campaign ownership")
        if campaign_id is None:
            raise CampaignSendRefusedError(
                f"send {token.send_id} is marked campaign-owned without a campaign id"
            )
        if not self._campaign_file_is_approved_tx(campaign_id, token.shoko_file_id):
            raise CampaignSendRefusedError(
                f"send {token.send_id} is for a file outside campaign {campaign_id!r}'s "
                "approved binding"
            )
        unresolved = self._unresolved_campaign_send_ids_tx()
        if unresolved:
            raise CampaignSendRefusedError(
                "legacy campaign send history is ambiguous; resolve send(s) "
                + ", ".join(str(send_id) for send_id in unresolved)
                + " before any campaign submission"
            )
        return campaign_id

    def _debit_campaign_attempt_tx(
        self, campaign_id: str, token: SendToken, iso: str
    ) -> tuple[str, int, int] | None:
        """Atomically enforce the campaign ceiling and reserve one attempted-send count."""
        campaign = self._conn.execute(
            "SELECT total_sent, max_submissions, status FROM campaigns WHERE campaign_id = ?",
            (campaign_id,),
        ).fetchone()
        if campaign is None:
            raise StateError(
                f"campaign {campaign_id!r} for file {token.shoko_file_id} does not exist"
            )
        total = max(
            _as_int(campaign[0], "total_sent"),
            self._campaign_attempt_count_tx(campaign_id),
        )
        limit = _as_int(campaign[1], "max_submissions")
        status = _as_str(campaign[2], "campaign status")
        if status != CAMPAIGN_STATUS_APPROVED:
            raise CampaignSendRefusedError(
                f"campaign {campaign_id!r} is {status}: prepared send is refused"
            )
        if total >= limit:
            self._conn.execute(
                "UPDATE campaigns SET total_sent = ?, status = 'paused',"
                " status_reason = 'max_submissions_reached' WHERE campaign_id = ?",
                (total, campaign_id),
            )
            return campaign_id, total, limit
        self._conn.execute(
            "UPDATE campaigns SET total_sent = ?, last_batch_iso = ? WHERE campaign_id = ?",
            (total + 1, iso, campaign_id),
        )
        return None

    def _campaign_attempt_count_tx(self, campaign_id: str) -> int:
        """Count durable campaign sends that crossed mark_sent; caller holds a write txn."""
        row = self._conn.execute(
            "SELECT COUNT(*) FROM sends s WHERE s.campaign_ownership = ?"
            " AND s.campaign_id = ? AND s.outcome NOT IN ('prepared', 'voided')",
            (CAMPAIGN_OWNERSHIP_CAMPAIGN, campaign_id),
        ).fetchone()
        if row is None:
            raise StateError("campaign attempted-send count query returned no row")
        return _as_int(row[0], "campaign attempted-send count")

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
        inspection: SendInspection | None = None,
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
            # Only a link-confirmed verdict (VERIFIED, PARTIAL-with-link) advances the file's
            # verified high-water: UNKNOWN/PENDING re-polls and CONTRADICTION must not, so a
            # file stuck in ``verifying`` cannot inflate the continuity expected-drop each cycle.
            detail = _clip(f"verification {verdict.value}")
            if target in (FileState.VERIFIED, FileState.PARTIAL):
                self._conn.execute(
                    "UPDATE files SET state = ?, state_detail = ?, updated_iso = ?,"
                    " last_verified_iso = ? WHERE shoko_file_id = ?",
                    (new_state.value, detail, iso, iso, shoko_file_id),
                )
            else:
                self._conn.execute(
                    "UPDATE files SET state = ?, state_detail = ?, updated_iso = ?"
                    " WHERE shoko_file_id = ?",
                    (new_state.value, detail, iso, shoko_file_id),
                )
            send_row = self._conn.execute(
                "SELECT send_id, request_fingerprint, outcome, target_ids_json, slot_released"
                " FROM sends WHERE shoko_file_id = ?"
                " AND outcome IN ('prepared', 'attempted', 'accepted_unverified', 'ambiguous_send',"
                " 'inspected_review')"
                " ORDER BY send_id DESC LIMIT 1",
                (shoko_file_id,),
            ).fetchone()
            if inspection is not None and (
                send_row is None
                or _as_int(send_row[0], "send_id") != inspection.send_id
                or _as_str(send_row[1], "request_fingerprint") != inspection.request_fingerprint
                or _as_str(send_row[2], "send outcome") != inspection.outcome
                or _parse_target_ids(_as_optional_str(send_row[3])) != inspection.target_ids
                or bool(_as_int(send_row[4], "slot_released")) != inspection.slot_released
                or not inspection.target_ids
            ):
                raise StateError(
                    f"file {shoko_file_id} inspection no longer matches its persisted send "
                    "identity/fingerprint/targets"
                )
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
                elif (
                    inspection is not None
                    and verdict in (VerificationVerdict.PARTIAL, VerificationVerdict.CONTRADICTION)
                    and _as_str(send_row[2], "send outcome") != OUTCOME_INSPECTED_REVIEW
                ):
                    self._conn.execute(
                        "UPDATE sends SET outcome = ?, verification = ?, verified_iso = ?"
                        " WHERE send_id = ?",
                        (OUTCOME_INSPECTED_REVIEW, verdict.value, iso, send_id),
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
        return self._void_prepared_send(shoko_file_id, request_fingerprint, iso=iso, reason=reason)

    def _void_prepared_send_after_write_stop(
        self, shoko_file_id: int, request_fingerprint: str, *, iso: str, reason: str
    ) -> int:
        """Release only a definitely-unsent reservation after stop fenced ``mark_sent``."""
        if not self.writes_disabled:
            raise StateError("stop-only prepared-send cleanup requires a latched write stop")
        _require_iso(iso, "iso")
        self._begin_immediate_tx(None)
        try:
            send_id = self._void_prepared_send_tx(
                shoko_file_id, request_fingerprint, iso=iso, reason=reason
            )
        except BaseException:
            self._rollback_tx()
            raise
        self._commit_tx()
        return send_id

    def _void_prepared_send(
        self,
        shoko_file_id: int,
        request_fingerprint: str,
        *,
        iso: str,
        reason: str,
    ) -> int:
        _require_iso(iso, "iso")
        with self._transaction():
            return self._void_prepared_send_tx(
                shoko_file_id, request_fingerprint, iso=iso, reason=reason
            )

    def _void_prepared_send_tx(
        self, shoko_file_id: int, request_fingerprint: str, *, iso: str, reason: str
    ) -> int:
        """Void one known-unsent prepared row while the caller holds its transaction."""
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
        """Refuse implicit partial-close releases; possible deliveries need explicit resolution."""
        _require_iso(iso, "iso")
        with self._transaction():
            row = self._conn.execute(
                "SELECT outcome FROM sends WHERE shoko_file_id = ? AND request_fingerprint = ?"
                " AND outcome IN ('prepared', 'attempted', 'accepted_unverified', 'ambiguous_send',"
                " 'inspected_review')"
                " ORDER BY send_id DESC LIMIT 1",
                (shoko_file_id, request_fingerprint),
            ).fetchone()
            if row is None:
                return 0
            raise StateError(
                f"send for file {shoko_file_id} has outcome {_as_str(row[0], 'send outcome')!r}: "
                "a possibly delivered send slot can be released only by VERIFIED or explicit "
                "resolve_send after terminal inspection"
            )

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
        """Operator-sanctioned close of a durable, identity-qualified terminal inspection.

        Legal ONLY from ``inspected_review``, recorded after the same Shoko file ID's current
        ED2K and persisted target triples were checked against that send's fingerprint. A
        ``prepared``, uninspected ``attempted``, ``accepted_unverified`` (including HTTP 202),
        or ``ambiguous_send`` row is refused. A ``PARTIAL`` inspection additionally requires
        the verification deadline (``attempted_iso`` + timeout) to have elapsed when supplied.
        It sets the send row closed (``resolved``), releases exactly the stored
        reservation through the P1-A single-owner path (a no-op when already released), and
        moves the file to ``waiting`` (re-evaluable, ``released_after_review``) or
        ``blocked_safety`` (``operator_closed``). It **never** touches a ``verified`` row and
        never re-enables resubmission of a *sent* request: the same ``request_fingerprint``
        stays refused even after close, and the file can re-plan only as a **fresh**
        ``request_fingerprint``. Rows that may have been delivered keep the inspection verdict
        and operator note; nothing here
        is reachable from the daemon (operator-only chain, P3-B).
        """
        _require_iso(iso, "iso")
        target = (
            FileState.WAITING if resolution == "released_after_review" else FileState.BLOCKED_SAFETY
        )
        with self._transaction():
            row = self._conn.execute(
                "SELECT send_id, outcome, slot_released, verification, attempted_iso FROM sends"
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
                    "inspected_review after a durable same-file XRef inspection; prepared, "
                    "attempted, accepted_unverified, and ambiguous_send rows are not closable "
                    "before that inspection"
                )
            if _as_optional_str(row[3]) == VerificationVerdict.PARTIAL.value:
                if verification_timeout_seconds is None:
                    raise StateError(
                        f"send {send_id} has only a partial inspection: resolve_send requires "
                        "the configured verification timeout"
                    )
                deadline = _parse_iso(_as_str(row[4], "attempted_iso")) + timedelta(
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
                "UPDATE sends SET outcome = ? WHERE send_id = ?",
                (OUTCOME_RESOLVED, send_id),
            )
            self._release_send_slot_tx(send_id, iso, f"resolve:{resolution}")
            self._conn.execute(
                "UPDATE files SET state = ?, state_detail = ?, updated_iso = ?"
                " WHERE shoko_file_id = ?",
                (
                    target.value,
                    _clip(
                        f"send resolved ({resolution}) after "
                        f"{_as_optional_str(row[3]) or 'inspection'}: {operator_note}"
                    ),
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
            " WHERE outcome IN ('prepared', 'attempted', 'accepted_unverified', 'ambiguous_send',"
            " 'inspected_review')"
            " AND slot_released = 0",
        ).fetchone()
        assert row is not None
        return _as_int(row[0], "live_held_sends"), _as_int(row[1], "live_held_slots")

    def _owned_pending_counts_tx(self) -> tuple[int, int]:
        files, jobs = self.live_send_counts()
        row = self._conn.execute(
            "SELECT COUNT(*) FROM metadata_operations"
            " WHERE reservation_owned = 1 AND slot_released = 0"
        ).fetchone()
        assert row is not None
        owned = _as_int(row[0], "metadata reservations")
        return files + owned, jobs + owned

    @staticmethod
    def _metadata_identity(source_key: str, anidb_anime_id: int) -> None:
        _require_hex64(source_key, "source_key")
        if type(anidb_anime_id) is not int or anidb_anime_id <= 0:
            raise StateError("anidb_anime_id must be a positive integer")

    def get_metadata_operation(
        self, source_key: str, anidb_anime_id: int
    ) -> MetadataOperation | None:
        self._metadata_identity(source_key, anidb_anime_id)
        row = self._conn.execute(
            "SELECT source_key, anidb_anime_id, claim, demand_fingerprint,"
            " request_fingerprint, request_version, state, created_iso, updated_iso,"
            " attempted_iso, verification_deadline_iso, transport_admitted,"
            " reservation_owned, slot_released, required_targets_json, missing_targets_json,"
            " verified_targets_json FROM metadata_operations"
            " WHERE source_key = ? AND anidb_anime_id = ?",
            (source_key, anidb_anime_id),
        ).fetchone()
        if row is None:
            return None
        return MetadataOperation(
            source_key=_as_str(row[0], "source_key"),
            anidb_anime_id=_as_int(row[1], "anidb_anime_id"),
            claim=_as_str(row[2], "claim"),
            demand_fingerprint=_as_str(row[3], "demand_fingerprint"),
            request_fingerprint=_as_str(row[4], "request_fingerprint"),
            request_version=_as_int(row[5], "request_version"),
            state=MetadataOperationState(_as_str(row[6], "metadata state")),
            created_iso=_as_str(row[7], "created_iso"),
            updated_iso=_as_str(row[8], "updated_iso"),
            attempted_iso=_as_optional_str(row[9]),
            verification_deadline_iso=_as_optional_str(row[10]),
            transport_admitted=bool(row[11]),
            reservation_owned=bool(row[12]),
            slot_released=bool(row[13]),
            required_targets=_decode_metadata_targets(_as_optional_str(row[14]), anidb_anime_id),
            missing_targets=_decode_metadata_targets(_as_optional_str(row[15]), anidb_anime_id),
            verified_required_targets=_decode_metadata_targets(
                _as_optional_str(row[16]), anidb_anime_id
            ),
        )

    def metadata_operations(self, source_key: str) -> tuple[MetadataOperation, ...]:
        """Read-only recovery inventory, including terminal replay-protection rows."""
        result = (
            self.get_metadata_operation(source_key, anime_id)
            for anime_id in self.metadata_operation_ids(source_key)
        )
        return tuple(operation for operation in result if operation is not None)

    def metadata_operation_ids(self, source_key: str) -> tuple[int, ...]:
        """Ordered recovery identities; decode each row separately to isolate corruption."""
        _require_hex64(source_key, "source_key")
        ids = self._conn.execute(
            "SELECT anidb_anime_id FROM metadata_operations WHERE source_key=?"
            " ORDER BY anidb_anime_id",
            (source_key,),
        ).fetchall()
        return tuple(_as_int(row[0], "anidb_anime_id") for row in ids)

    def _recovery_batch(
        self, query: LiteralString, parameters: tuple[object, ...], *, after_id: int, limit: int
    ) -> RecoveryBatch:
        """Rotate pending SQL identities; LIMIT applies before any payload decoding.

        Queries supply only id and immutable ordering anchor. If the cursor is no
        longer present, rotation resumes from the oldest pending identity.
        """
        if type(limit) is not int or limit < 0:
            raise ValueError("recovery limit must be nonnegative")
        if limit == 0:
            total_row = self._conn.execute(
                f"SELECT COUNT(*) FROM ({query}) WHERE pending",  # noqa: S608 -- private literal SQL only
                parameters,
            ).fetchone()
            return RecoveryBatch((), _as_int(total_row[0], "pending recovery count"))
        rows = self._conn.execute(
            f"WITH candidates AS ({query}),"  # noqa: S608 -- private literal SQL only
            " anchor AS (SELECT sort_key, id FROM candidates WHERE id=?)"
            " SELECT id,COUNT(*) OVER() FROM candidates WHERE pending"
            " ORDER BY CASE WHEN (sort_key,id) <="
            " (SELECT sort_key,id FROM anchor) THEN 1 ELSE 0 END, sort_key,id LIMIT ?",
            (*parameters, after_id, limit),
        ).fetchall()
        # Count and batch share one SQLite snapshot, including concurrent state handles.
        total = _as_int(rows[0][1], "pending recovery count") if rows else 0
        return RecoveryBatch(tuple(_as_int(row[0], "recovery id") for row in rows), total)

    def metadata_recovery_batch(
        self, source_key: str, *, after_id: int, limit: int
    ) -> RecoveryBatch:
        """Keep terminal replay history out of the bounded recovery decode allowance."""
        _require_hex64(source_key, "source_key")
        return self._recovery_batch(
            "SELECT anidb_anime_id AS id, anidb_anime_id AS sort_key,"
            " state NOT IN ('ready','definitely_rejected') AS pending"
            " FROM metadata_operations WHERE source_key=?"
            " AND (state NOT IN ('ready','definitely_rejected') OR anidb_anime_id=?)",
            (source_key, after_id),
            after_id=after_id,
            limit=limit,
        )

    def file_recovery_batch(self, *, after_id: int, limit: int) -> RecoveryBatch:
        """Global read-only recovery, retaining campaign rows and oldest-send order."""
        return self._recovery_batch(
            "SELECT shoko_file_id AS id, COALESCE((SELECT attempted_iso FROM sends"
            " WHERE sends.shoko_file_id=files.shoko_file_id AND outcome IN"
            " ('prepared','attempted','accepted_unverified','ambiguous_send')"
            " ORDER BY send_id DESC LIMIT 1),updated_iso) AS sort_key,"
            " 1 AS pending FROM files WHERE state IN"
            " ('prepared_not_sent','request_may_have_been_sent','indeterminate_write')"
            " OR (state IN ('verifying','partial','conflict') AND EXISTS"
            " (SELECT 1 FROM sends WHERE sends.shoko_file_id=files.shoko_file_id"
            " AND outcome IN ('prepared','attempted','accepted_unverified','ambiguous_send')))",
            (),
            after_id=after_id,
            limit=limit,
        )

    def recovery_re_evaluate_count(self) -> int:
        """Summary only: reconcile consumes these rows without recovery decoding."""
        row = self._conn.execute(
            "SELECT COUNT(*) FROM files WHERE state NOT IN"
            " ('prepared_not_sent','request_may_have_been_sent','indeterminate_write',"
            " 'blocked_safety','verified','verifying','partial','conflict')"
            " OR (state IN ('verifying','partial','conflict') AND NOT EXISTS"
            " (SELECT 1 FROM sends WHERE sends.shoko_file_id=files.shoko_file_id"
            " AND outcome IN ('prepared','attempted','accepted_unverified','ambiguous_send')))"
        ).fetchone()
        return _as_int(row[0], "re-evaluate count")

    @_mutation
    def observe_external_metadata_operation(
        self,
        source_key: str,
        anidb_anime_id: int,
        *,
        demand_fingerprint: str,
        request_fingerprint: str,
        request_version: int,
        iso: str,
        required_targets: Sequence[MetadataEpisodeIdentity] | None = None,
        missing_targets: Sequence[MetadataEpisodeIdentity] | None = None,
    ) -> MetadataOperation:
        """Track somebody else's queued work without claiming our transport/budget.

        Typed demand uses the same canonical validation and verified READY coverage
        as own requests, allowing later genuinely new episodes after external completion.
        Existing own history is never replaced. If this external job later vanishes
        without readiness, the caller may explicitly adopt its exact claim in prepare
        after fresh queue/safety checks. State does not infer queue absence or readiness.
        """
        self._metadata_identity(source_key, anidb_anime_id)
        _require_hex64(demand_fingerprint, "demand_fingerprint")
        _require_hex64(request_fingerprint, "request_fingerprint")
        _require_iso(iso, "iso")
        if type(request_version) is not int or request_version <= 0:
            raise StateError("request_version must be a positive integer")
        required, missing = _canonical_metadata_demand(
            required_targets, missing_targets, anidb_anime_id
        )
        with self._transaction():
            self._conn.execute(
                "INSERT INTO metadata_operations (source_key, anidb_anime_id, claim,"
                " demand_fingerprint, request_fingerprint, request_version, state,"
                " created_iso, updated_iso, reservation_owned, slot_released,"
                " required_targets_json, missing_targets_json)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 1, ?, ?)"
                " ON CONFLICT (source_key, anidb_anime_id) DO NOTHING",
                (
                    source_key,
                    anidb_anime_id,
                    uuid.uuid4().hex,
                    demand_fingerprint,
                    request_fingerprint,
                    request_version,
                    MetadataOperationState.ACCEPTED_WAITING.value,
                    iso,
                    iso,
                    _encode_metadata_targets(required),
                    _encode_metadata_targets(missing),
                ),
            )
            result = self.get_metadata_operation(source_key, anidb_anime_id)
            assert result is not None
            return result

    def _metadata_owner_tx(self, operation: MetadataOperation) -> MetadataOperation:
        current = self.get_metadata_operation(operation.source_key, operation.anidb_anime_id)
        if current is None or current.claim != operation.claim:
            raise StateError("stale metadata operation claim")
        return current

    def _metadata_consent_tx(self, expected_document_json: str | None) -> None:
        if expected_document_json is not None:
            row = self._conn.execute(
                "SELECT document_json, paused FROM onboarding WHERE singleton = 1"
            ).fetchone()
            if row is None or row[0] != expected_document_json or row[1]:
                raise StateError("metadata operation authorization changed or paused")

    def _metadata_ready_rearm_tx(
        self,
        prior: MetadataOperation | None,
        after_ready_claim: str | None,
        missing: tuple[MetadataEpisodeIdentity, ...] | None,
    ) -> bool:
        if after_ready_claim is None or prior is None:
            return False
        if (
            prior.claim != after_ready_claim
            or prior.state != MetadataOperationState.READY
            or not prior.slot_released
        ):
            return False
        if prior.verified_required_targets is None or prior.required_targets is None:
            raise StateError("legacy READY metadata coverage is unknown; manual attention required")
        if not missing:
            return False
        covered = set(prior.verified_required_targets)
        for row in self._conn.execute(
            "SELECT verified_targets_json FROM metadata_operation_history"
            " WHERE source_key=? AND anidb_anime_id=?",
            (prior.source_key, prior.anidb_anime_id),
        ):
            targets = _decode_metadata_targets(_as_optional_str(row[0]), prior.anidb_anime_id)
            if targets is None:
                raise StateError("historical READY metadata coverage is unknown")
            covered.update(targets)
        return bool(set(missing) - covered)

    @_mutation
    def prepare_metadata_operation(
        self,
        source_key: str,
        anidb_anime_id: int,
        *,
        demand_fingerprint: str,
        request_fingerprint: str,
        request_version: int,
        cycle_key: str,
        iso: str,
        expected_document_json: str | None = None,
        retry_unsent_claim: str | None = None,
        adopt_external_claim: str | None = None,
        after_ready_claim: str | None = None,
        required_targets: Sequence[MetadataEpisodeIdentity] | None = None,
        missing_targets: Sequence[MetadataEpisodeIdentity] | None = None,
    ) -> MetadataOperation | BudgetRefusal:
        """Reserve one request/one parent job, without inventing a file send.

        The caller proves current scope/opt-in/physical safety and queue absence before
        preparing. An explicit retry is possible only for a proven zero-delivery row
        and must name its prior claim. A later verified-ready generation additionally
        requires exact after_ready_claim and a genuinely new missing episode outside
        accumulated verified coverage. File/demand/consent changes alone do not qualify.
        Caller supplies fresh verified support and complete queue absence; no external
        I/O occurs in this transaction. Consent never changes operation identity.
        """
        self._metadata_identity(source_key, anidb_anime_id)
        _require_hex64(demand_fingerprint, "demand_fingerprint")
        _require_hex64(request_fingerprint, "request_fingerprint")
        _require_iso(iso, "iso")
        self._require_cycle_key(cycle_key)
        if type(request_version) is not int or request_version <= 0:
            raise StateError("request_version must be a positive integer")
        required, missing = _canonical_metadata_demand(
            required_targets, missing_targets, anidb_anime_id
        )
        with self._transaction():
            self._metadata_consent_tx(expected_document_json)
            prior = self.get_metadata_operation(source_key, anidb_anime_id)
            ready_rearm = self._metadata_ready_rearm_tx(prior, after_ready_claim, missing)
            if (
                prior is not None
                and not ready_rearm
                and not (
                    prior.state == MetadataOperationState.DEFINITELY_REJECTED
                    and prior.slot_released
                    and retry_unsent_claim == prior.claim
                    and (
                        prior.required_targets is None
                        or (required == prior.required_targets and missing == prior.missing_targets)
                    )
                )
                and not (
                    not prior.reservation_owned
                    and prior.attempted_iso is None
                    and prior.state == MetadataOperationState.ACCEPTED_WAITING
                    and adopt_external_claim == prior.claim
                )
            ):
                return BudgetRefusal(False, "metadata_operation_exists", "anime operation retained")
            decision = self._reserve_tx(cycle_key, iso, expected_episode_jobs=1)
            if isinstance(decision, BudgetRefusal):
                return decision
            if ready_rearm:
                assert prior is not None
                self._conn.execute(
                    "INSERT INTO metadata_operation_history SELECT * FROM metadata_operations"
                    " WHERE source_key=? AND anidb_anime_id=? AND claim=?",
                    (source_key, anidb_anime_id, prior.claim),
                )
            self._conn.execute(
                "INSERT INTO metadata_operations (source_key, anidb_anime_id, claim,"
                " demand_fingerprint, request_fingerprint, request_version, state,"
                " created_iso, updated_iso, reservation_owned,"
                " required_targets_json, missing_targets_json)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)"
                " ON CONFLICT (source_key, anidb_anime_id) DO UPDATE SET"
                " claim=excluded.claim, demand_fingerprint=excluded.demand_fingerprint,"
                " request_fingerprint=excluded.request_fingerprint,"
                " request_version=excluded.request_version, state=excluded.state,"
                " updated_iso=excluded.updated_iso, attempted_iso=NULL,"
                " verification_deadline_iso=NULL, transport_admitted=0,"
                " reservation_owned=1, slot_released=0,"
                " required_targets_json=excluded.required_targets_json,"
                " missing_targets_json=excluded.missing_targets_json, verified_targets_json=NULL",
                (
                    source_key,
                    anidb_anime_id,
                    uuid.uuid4().hex,
                    demand_fingerprint,
                    request_fingerprint,
                    request_version,
                    MetadataOperationState.PREPARED.value,
                    iso,
                    iso,
                    _encode_metadata_targets(required),
                    _encode_metadata_targets(missing),
                ),
            )
            result = self.get_metadata_operation(source_key, anidb_anime_id)
            assert result is not None
            return result

    @_mutation
    def mark_metadata_possibly_delivered(
        self,
        operation: MetadataOperation,
        *,
        iso: str,
        verification_deadline_iso: str,
        expected_document_json: str | None = None,
    ) -> MetadataOperation:
        """Commit before HTTP. A crash after this boundary must only inspect/verify."""
        _require_iso(iso, "iso")
        _require_iso(verification_deadline_iso, "verification_deadline_iso")
        if _parse_iso(verification_deadline_iso) <= _parse_iso(iso):
            raise StateError("metadata verification deadline must follow attempt")
        with self._transaction():
            self._metadata_consent_tx(expected_document_json)
            current = self._metadata_owner_tx(operation)
            if current.state != MetadataOperationState.PREPARED:
                raise StateError("metadata operation is not prepared")
            self._conn.execute(
                "UPDATE metadata_operations SET state=?, attempted_iso=?, updated_iso=?,"
                " verification_deadline_iso=? WHERE source_key=? AND anidb_anime_id=?",
                (
                    MetadataOperationState.POSSIBLY_DELIVERED.value,
                    iso,
                    iso,
                    verification_deadline_iso,
                    operation.source_key,
                    operation.anidb_anime_id,
                ),
            )
            result = self.get_metadata_operation(operation.source_key, operation.anidb_anime_id)
            assert result is not None
        self._metadata_transport_claims.add(result.claim)
        return result

    @_mutation
    def admit_metadata_transport(
        self, operation: MetadataOperation, *, expected_document_json: str | None = None
    ) -> None:
        """One-shot transport fence after durable mark; no external I/O in transaction.

        Only the caller that successfully returns may invoke HTTP, immediately. A crash
        after admission remains possibly delivered, never a reason to replay. Later
        write stops apply to subsequent operations, as for file transport admission.
        """
        if operation.claim not in self._metadata_transport_claims:
            raise StateError("metadata transport requires this handle's successful durable mark")
        with self._transaction():
            self._metadata_consent_tx(expected_document_json)
            current = self._metadata_owner_tx(operation)
            if (
                current.state != MetadataOperationState.POSSIBLY_DELIVERED
                or current.transport_admitted
            ):
                raise StateError("metadata transport already admitted or not durably marked")
            self._conn.execute(
                "UPDATE metadata_operations SET transport_admitted=1"
                " WHERE source_key=? AND anidb_anime_id=?",
                (operation.source_key, operation.anidb_anime_id),
            )
        self._metadata_transport_claims.discard(operation.claim)

    def _release_metadata_slot_tx(self, operation: MetadataOperation) -> None:
        if operation.reservation_owned and not operation.slot_released:
            self._adjust_pending_tx(-1, -1)
        self._conn.execute(
            "UPDATE metadata_operations SET slot_released=1"
            " WHERE source_key=? AND anidb_anime_id=?",
            (operation.source_key, operation.anidb_anime_id),
        )

    def _cancel_prepared_metadata_tx(self, operation: MetadataOperation, iso: str) -> None:
        """Cancel only the exact still-unsent claim under the caller's writer lock."""
        current = self._metadata_owner_tx(operation)
        if current.state != MetadataOperationState.PREPARED:
            raise StateError("prepared cleanup refuses possibly delivered metadata")
        self._release_metadata_slot_tx(current)
        self._conn.execute(
            "UPDATE metadata_operations SET state=?, updated_iso=?"
            " WHERE source_key=? AND anidb_anime_id=?",
            (
                MetadataOperationState.DEFINITELY_REJECTED.value,
                iso,
                current.source_key,
                current.anidb_anime_id,
            ),
        )

    @_mutation
    def cancel_prepared_metadata_operation(self, operation: MetadataOperation, *, iso: str) -> None:
        """Release a definitely-unsent claim, refusing concurrent marking/admission.

        A PREPARED snapshot is insufficient proof: recheck the current state and
        exact claim atomically before releasing capacity or allowing an unsent retry.
        """
        _require_iso(iso, "iso")
        with self._transaction():
            self._cancel_prepared_metadata_tx(operation, iso)

    def void_prepared_metadata_after_write_stop(
        self,
        operation: MetadataOperation,
        *,
        iso: str,
    ) -> None:
        """Stop-only cleanup, like file sends: never void possible delivery or clear stop.

        This deliberately bypasses the ordinary mutation guard only to release a
        durably prepared, definitely unsent row after a stop prevents its mark.
        """
        if not self.writes_disabled:
            raise StateError("stop-only metadata cleanup requires a latched write stop")
        _require_iso(iso, "iso")
        try:
            self._begin_immediate_tx(None)
            try:
                self._cancel_prepared_metadata_tx(operation, iso)
            except BaseException:
                self._rollback_tx()
                raise
            self._commit_tx()
        except sqlite3.Error as exc:
            raise StateError("stop-only metadata cleanup could not be persisted") from exc

    @_mutation
    def record_metadata_operation(
        self,
        operation: MetadataOperation,
        state: MetadataOperationState,
        *,
        iso: str,
        verified_required_targets: Sequence[MetadataEpisodeIdentity] | None = None,
    ) -> MetadataOperation:
        """Observe completion or hold uncertainty. Rejection means proven zero delivery.

        READY must mean verified required metadata is present, not merely HTTP success.
        NEEDS_ATTENTION retains owned capacity, matching uncertain file-send policy.
        """
        _require_iso(iso, "iso")
        with self._transaction():
            current = self._metadata_owner_tx(operation)
            if state == current.state:
                return current
            if state == MetadataOperationState.ACCEPTED_WAITING and not current.transport_admitted:
                raise StateError("own metadata request was not admitted to transport")
            allowed = {
                MetadataOperationState.PREPARED: {
                    MetadataOperationState.DEFINITELY_REJECTED,
                    MetadataOperationState.READY,
                    MetadataOperationState.NEEDS_ATTENTION,
                },
                MetadataOperationState.POSSIBLY_DELIVERED: {
                    MetadataOperationState.ACCEPTED_WAITING,
                    MetadataOperationState.READY,
                    MetadataOperationState.DEFINITELY_REJECTED,
                    MetadataOperationState.NEEDS_ATTENTION,
                },
                MetadataOperationState.ACCEPTED_WAITING: {
                    MetadataOperationState.READY,
                    MetadataOperationState.NEEDS_ATTENTION,
                },
                MetadataOperationState.NEEDS_ATTENTION: {MetadataOperationState.READY},
            }
            if state not in allowed.get(current.state, set()):
                raise StateError("invalid metadata operation transition")
            verified = None
            if state == MetadataOperationState.READY and current.required_targets is not None:
                if verified_required_targets is None:
                    raise StateError("READY requires verified required metadata coverage")
                verified = _canonical_metadata_targets(
                    verified_required_targets, current.anidb_anime_id
                )
                if verified != current.required_targets:
                    raise StateError("READY requires exactly verified required metadata coverage")
            elif verified_required_targets is not None:
                raise StateError("verified coverage is only valid for typed READY metadata")
            if state in {MetadataOperationState.READY, MetadataOperationState.DEFINITELY_REJECTED}:
                self._release_metadata_slot_tx(current)
            self._conn.execute(
                "UPDATE metadata_operations SET state=?, updated_iso=?, verified_targets_json=?"
                " WHERE source_key=? AND anidb_anime_id=?",
                (
                    state.value,
                    iso,
                    _encode_metadata_targets(verified),
                    current.source_key,
                    current.anidb_anime_id,
                ),
            )
            result = self.get_metadata_operation(current.source_key, current.anidb_anime_id)
            assert result is not None
            return result

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
            elif record.state in (FileState.VERIFYING, FileState.PARTIAL, FileState.CONFLICT):
                if self.active_send_details(record.shoko_file_id) is None:
                    re_evaluate.append(entry)
                else:
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

    def _linked_count_tx(self) -> int:
        """Files whose link landed (VERIFIED / PARTIAL): non-null ``last_verified_iso``.

        Read inside the continuity transaction (the store owns the ``files`` table; no new I/O).
        Cohort-independent; discovery-only rows never count (``record_file`` leaves
        ``last_verified_iso`` null, and UNKNOWN/PENDING re-polls never set it).
        """
        row = self._conn.execute(
            "SELECT COUNT(*) FROM files WHERE last_verified_iso IS NOT NULL"
        ).fetchone()
        return _as_int(row[0], "linked_count")

    def _expected_drop_tx(self, basis_raw: str | None) -> int:
        """The linker's own link-driven drops expected since the last anchor.

        A count delta (``linked now - linked at anchor``), **not** a timestamp window: the
        campaign/daemon can observe several batches at the same ISO instant, and a strict
        ``>`` timestamp window silently drops same-instant links. Clamped at zero so a
        store that only ever grows linked rows never under-counts.
        """
        current = self._linked_count_tx()
        anchored = 0 if basis_raw is None else int(basis_raw)
        return max(0, current - anchored)

    def _pending_departures_tx(self, discovered_file_ids: frozenset[int]) -> int:
        """Count only departed, attempted sends still eligible for bounded recovery.

        No remote reads or speculative link credit: these rows can justify waiting,
        never acceptance of a drop. Rejected/prepared/conflicting sends cannot defer it.
        """
        rows = self._conn.execute(
            "SELECT DISTINCT files.shoko_file_id FROM files JOIN sends USING (shoko_file_id)"
            " WHERE files.last_verified_iso IS NULL"
            " AND files.state IN ('request_may_have_been_sent', 'verifying', 'indeterminate_write')"
            " AND sends.outcome IN ('attempted', 'accepted_unverified', 'ambiguous_send')"
            " AND sends.attempted_iso IS NOT NULL"
        )
        return sum(_as_int(row[0], "shoko_file_id") not in discovered_file_ids for row in rows)

    def _anchor_source_tx(
        self,
        observation: SourceObservation,
        iso: str,
        *,
        clear_flag: bool,
    ) -> None:
        """Persist the observation as the new continuity anchor (re-anchoring on every
        evaluated cycle; growth/monotonic changes re-anchor, P1-1).

        The anchor carries the accounting basis: the discovery total **plus** the count of landed
        links at anchor time, so the next cycle's expected drops are the links landed *after* this
        anchor — not the cumulative links the anchored total already nets out.
        """
        self._set_meta_tx(META_SOURCE_INSTANCE, observation.instance_identity)
        self._set_meta_tx(META_SONARR_STATUS_SNAPSHOT_ID, observation.snapshot_summary_id)
        self._set_meta_tx(META_SOURCE_LAST_OBSERVED_ISO, iso)
        if observation.roots_fingerprint:
            self._set_meta_tx(META_SOURCE_ROOTS_FINGERPRINT, observation.roots_fingerprint)
        if observation.discovery_total is not None:
            self._set_meta_tx(META_SOURCE_LAST_DISCOVERY_TOTAL, str(observation.discovery_total))
        self._set_meta_tx(META_SOURCE_ANCHOR_BASIS, str(self._linked_count_tx()))
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
        cycle: the anchor advances with normal inventory growth. Explicit rebaseline
        callers can also use this recovery path to clear a hard hold; ordinary polls
        must pass the latched continuity check first.
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

        Ordinary continuity checks never clear a hard hold. An explicit baseline
        observation (:meth:`record_source_observation`) is the other recovery path.
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
        tolerance: int = 0,
        discovered_file_ids: frozenset[int] | None = None,
    ) -> ContinuityVerdict:
        """Structured continuity check — ordinary inventory growth is NORMAL (P1-1).

        Continuity is **identity/roots stability plus snapshot discontinuity**:
        ``instance_identity_changed`` (configured identity string moved), ``roots_changed``
        (root-list/allowlist shape moved), ``discovery_total_drop`` (totals regressed below
        the anchored value **net of the linker's own links**) and ``snapshot_marker_missing`` (the
        expected snapshot summary marker is absent: no discovery total where one was anchored). A
        moving snapshot summary hash with monotonic totals is growth, not discontinuity: the
        observation is re-anchored each evaluated cycle so the check never self-wedges on new
        arrivals. ``requires_rebaseline`` is set only by the triggers, survives restart,
        and is cleared only by explicit recovery/rebaseline, never by a clean poll.

        **Excess-drop rule (M9-e2e defect):** the ``include_only=Unrecognized`` discovery total
        shrinks as the linker links files (a linked file leaves the unrecognized set). A drop is
        therefore expected whenever it is explained by the linker's own links since the last anchor:
        ``expected_drop`` is the count of landed links gained since the anchored basis
        (cohort-independent, net of the live send ledger; see :meth:`_expected_drop_tx`). Only a
        drop **beyond** that (``discovery_total < anchor_total - expected_drop - tolerance``) is a
        genuine discontinuity (external DB restore / identity reset). A drop within the expected
        budget (``±0`` slack by default; ``tolerance`` widens it) is NORMAL: the observation
        re-anchors with the new total and the refreshed accounting basis, so a multi-batch campaign
        never wedges on its own accepted links.

        Async jobs can land between bounded recovery and discovery. If the excess drop
        could be explained entirely by departed, still-unverified attempted sends,
        defer writes without advancing ANY anchor until recovery settles them. This
        never clears a hard hold or defers an identity/root change. A larger unexplained
        drop still latches immediately; pending sends never count as verified links.

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
            latched = self.get_meta(META_REQUIRES_REBASELINE) == "1"
            if not complete:
                # Truncated discovery: neutral, never anchors, never triggers a drop.
                return ContinuityVerdict(not latched, ("discovery_incomplete",), latched)
            known = self.get_meta(META_SOURCE_INSTANCE)
            if known is None:
                self._anchor_source_tx(observation, iso, clear_flag=not latched)
                return ContinuityVerdict(not latched, ("first_observation",), latched)
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
                expected_drop = self._expected_drop_tx(self.get_meta(META_SOURCE_ANCHOR_BASIS))
                if discovery_total < int(known_total_raw) - expected_drop - tolerance:
                    deficit = int(known_total_raw) - expected_drop - tolerance - discovery_total
                    if (
                        not reasons
                        and not latched
                        and discovered_file_ids is not None
                        and deficit <= self._pending_departures_tx(discovered_file_ids)
                    ):
                        return ContinuityVerdict(
                            False, ("discovery_drop_pending_verification",), False
                        )
                    reasons.append("discovery_total_drop")
            if latched:
                reasons.append("requires_rebaseline")
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

    # ------------------------------------------------- apply ownership (OS lock + meta)

    @property
    def apply_lock_path(self) -> Path:
        """The OS advisory lock file path (sibling of the state DB)."""
        return self.path.parent / APPLY_LOCK_FILENAME

    def owns_apply_ownership(self, pid: int, now_iso: str) -> bool:
        """Read-only proof of this handle's live OS lock and matching current lease.

        A persisted PID/lease alone is not ownership. Inherited handles after a fork
        cannot grant the child permission to send on its parent's lock.
        """
        lock = self._apply_lock
        holder = self.lock_holder()
        return (
            lock is not None
            and lock.held
            and lock.owner_process == os.getpid()
            and holder is not None
            and holder.owner_pid == pid
            and _parse_iso(holder.lease_expires_iso) > _parse_iso(now_iso)
        )

    def _process_lock(self) -> ApplyProcessLock:
        lock = self._apply_lock
        if lock is None:
            lock = ApplyProcessLock(self.apply_lock_path)
            self._apply_lock = lock
        return lock

    def _os_lock_contention(self, action: str) -> LockHeldError:
        holder = self._lock_holder()
        context = (
            f"meta records holder pid {holder.owner_pid} "
            f"(acquired {holder.acquired_iso}, lease expires {holder.lease_expires_iso})"
            if holder is not None
            else "no apply lease is recorded in meta"
        )
        return LockHeldError(
            f"apply lock contended: the OS advisory lock {self.apply_lock_path} "
            f"({ADVISORY_LOCK_KIND}) is held by a live process, so {action} is refused "
            f"({context}); a meta lease expiry alone never grants ownership",
            holder,
        )

    def acquire_apply_ownership(
        self, pid: int, iso: str, lease_expires_iso: str
    ) -> LockAcquisition:
        """Take exclusive apply ownership: the OS advisory lock FIRST, then the meta lease.

        Contention on the OS lock (a live owner) raises :class:`LockHeldError` with the
        meta holder context and mutates NOTHING. Once the OS lock is ours, the meta lease is
        taken/replaced/stolen under the existing rule (an expired lease + a free OS lock is a
        legitimate takeover of a DEAD owner, audit-recorded); a live meta lease releases the OS
        lock and raises. The OS lock handle is retained for the store's lifetime (released by
        :meth:`release_apply_ownership` or on process exit).
        """
        lock = self._process_lock()
        if not lock.acquire():
            raise self._os_lock_contention("apply ownership")
        try:
            return self.acquire_or_steal_apply_lock_tx(pid, iso, lease_expires_iso)
        except BaseException:
            lock.release()
            raise

    def release_apply_ownership(self, pid: int, iso: str) -> bool:
        """Release meta ownership first, then the OS advisory lock.

        The OS lock is released only AFTER the meta release succeeds (not-owner / write-stopped
        refusals propagate and keep the OS lock, which the kernel frees at process exit)."""
        released = self.release_apply_lock_tx(pid, iso)
        self._process_lock().release()
        return released

    def steal_apply_ownership(
        self, pid: int, iso: str, lease_expires_iso: str | None = None
    ) -> LockAcquisition:
        """Operator point-in-time takeover marker (the ``steal-apply-lock`` path).

        Refuses when the OS lock is held (a live owner — the kernel has NOT freed it); when the
        OS lock is free it records the meta steal audit (prior holder named, P2-1) and
        writes an **immediately-expired** lease marker (P2-2), then RELEASES the OS lock.
        The command does not run apply: the next daemon/campaign acquire sees the marker lease
        expired at ``iso`` and takes ownership normally — the operator command never stalls
        the daemon.
        """
        lock = self._process_lock()
        if not lock.acquire():
            raise self._os_lock_contention("an operator steal")
        prior_holder = self._lock_holder()
        marker_lease = lease_expires_iso if lease_expires_iso is not None else iso
        try:
            acquisition = self.steal_expired_apply_lock_tx(pid, iso, marker_lease)
        except BaseException:
            lock.release()
            raise
        if prior_holder is not None:
            self._record_lock_steal_tx(prior_holder, pid, iso)
        lock.release()
        return acquisition

    # ---------------------------------------------------------- campaigns (M7-b)

    @_mutation
    def create_campaign(
        self,
        campaign_id: str,
        digest: str,
        *,
        max_submissions: int,
        deadline_iso: str | None,
        attestation_scope: str,
        approval_iso: str,
        iso: str,
        bindings: Sequence[CampaignBinding] = (),
        plan_path: str | None = None,
    ) -> str:
        """Persist one **approved** campaign row (the ``campaign approve`` path).

        The approval is bound to the plan file's canonical digest; ``bindings`` are the
        per-file (id, plan fingerprint, request fingerprint) pairs the plan file carries.
        ``plan_path`` (F7) records the canonical plan file path the daemon drains against
        (the ``plan_drift`` digest check stays authoritative: the daemon re-reads this file
        and re-digests it every batch). A campaign id is unique — re-approving an existing
        campaign refuses.
        """
        _require_iso(iso, "iso")
        _require_iso(approval_iso, "approval_iso")
        if deadline_iso is not None:
            _require_iso(deadline_iso, "deadline_iso")
        _require_hex64(digest, "digest")
        if not isinstance(campaign_id, str) or not campaign_id.strip():
            raise StateError("campaign_id must be a non-empty string")
        if not isinstance(attestation_scope, str) or not attestation_scope.strip():
            raise StateError("attestation_scope must be a non-empty string")
        if _as_int(max_submissions, "max_submissions") < 1:
            raise StateError("max_submissions must be at least 1")
        binding_rows = [
            [int(b.shoko_file_id), b.plan_fingerprint, b.request_fingerprint] for b in bindings
        ]
        for binding in bindings:
            _require_hex64(binding.plan_fingerprint, "binding plan_fingerprint")
            _require_hex64(binding.request_fingerprint, "binding request_fingerprint")
        with self._transaction():
            existing = self._conn.execute(
                "SELECT 1 FROM campaigns WHERE campaign_id = ?", (campaign_id,)
            ).fetchone()
            if existing is not None:
                raise StateError(f"campaign {campaign_id!r} already exists: approve is one-shot")
            self._conn.execute(
                "INSERT INTO campaigns (campaign_id, digest, plan_fingerprint_set, max_submissions,"
                " deadline_iso, attestation_scope, approval_iso, status, status_reason, total_sent,"
                " last_batch_iso, created_iso, plan_path)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', 0, NULL, ?, ?)",
                (
                    campaign_id,
                    digest,
                    json.dumps(binding_rows),
                    max_submissions,
                    deadline_iso,
                    attestation_scope,
                    approval_iso,
                    CAMPAIGN_STATUS_APPROVED,
                    iso,
                    plan_path,
                ),
            )
        return campaign_id

    def get_campaign(self, campaign_id: str) -> CampaignRecord | None:
        row = self._conn.execute(
            "SELECT campaign_id, digest, max_submissions, deadline_iso, attestation_scope,"
            " approval_iso, status, status_reason, total_sent, last_batch_iso,"
            " created_iso, plan_fingerprint_set, plan_path, drain_authorized_iso,"
            " drain_authorized_digest, drain_invalidated_reason"
            " FROM campaigns WHERE campaign_id = ?",
            (campaign_id,),
        ).fetchone()
        if row is None:
            return None
        return _row_to_campaign(row)

    def list_campaigns(self) -> tuple[CampaignRecord, ...]:
        rows = self._conn.execute(
            "SELECT campaign_id, digest, max_submissions, deadline_iso, attestation_scope,"
            " approval_iso, status, status_reason, total_sent, last_batch_iso, created_iso,"
            " plan_fingerprint_set, plan_path, drain_authorized_iso, drain_authorized_digest,"
            " drain_invalidated_reason FROM campaigns ORDER BY campaign_id",
        ).fetchall()
        return tuple(_row_to_campaign(row) for row in rows)

    @_mutation
    def bind_campaign_files(self, campaign_id: str, iso: str) -> int:
        """Bind the campaign's plan-file fingerprints onto tracked file rows.

        Only rows that exist and sit in a queue state (``planned``/``mapped``) bind;
        the stored fingerprints come from the campaign row's ``plan_fingerprint_set``.
        Returns the number of file rows bound. Binding never advances file state.
        """
        _require_iso(iso, "iso")
        campaign = self.get_campaign(campaign_id)
        if campaign is None:
            raise StateError(f"campaign {campaign_id!r} does not exist: nothing to bind")
        with self._transaction():
            bound = 0
            for binding in campaign.bindings:
                result = self._conn.execute(
                    "UPDATE files SET campaign_id = ?, plan_fingerprint = ?,"
                    " request_fingerprint = ?, updated_iso = ?"
                    " WHERE shoko_file_id = ? AND state IN ('planned', 'mapped')",
                    (
                        campaign_id,
                        binding.plan_fingerprint,
                        binding.request_fingerprint,
                        iso,
                        binding.shoko_file_id,
                    ),
                )
                bound += result.rowcount
            stored = self._conn.execute(
                "SELECT plan_fingerprint_set FROM campaigns WHERE campaign_id = ?",
                (campaign_id,),
            ).fetchone()
            new_set = json.dumps(
                [
                    [b.shoko_file_id, b.plan_fingerprint, b.request_fingerprint]
                    for b in campaign.bindings
                ]
            )
            # F10: a refresh that moves the approved fingerprint set outlives any drain
            # authorization — the operator re-reviews the canary and re-grants.
            fingerprint_drift = (
                stored is not None and _as_str(stored[0], "plan_fingerprint_set") != new_set
            )
            if fingerprint_drift:
                self._conn.execute(
                    "UPDATE campaigns SET plan_fingerprint_set = ?, drain_authorized_iso = NULL,"
                    " drain_authorized_digest = NULL, drain_invalidated_reason = ?"
                    " WHERE campaign_id = ?",
                    (new_set, CAMPAIGN_DRAIN_FINGERPRINT_REASON, campaign_id),
                )
            else:
                self._conn.execute(
                    "UPDATE campaigns SET plan_fingerprint_set = ? WHERE campaign_id = ?",
                    (new_set, campaign_id),
                )
        return bound

    @_mutation
    def set_campaign_status(self, campaign_id: str, status: str, reason: str, *, iso: str) -> None:
        """Persist a campaign lifecycle change (approved/paused/revoked) with its reason."""
        _require_iso(iso, "iso")
        if status not in CAMPAIGN_STATUSES:
            raise StateError(
                f"unknown campaign status {status!r}: allowed are {sorted(CAMPAIGN_STATUSES)}"
            )
        with self._transaction():
            result = self._conn.execute(
                "UPDATE campaigns SET status = ?, status_reason = ? WHERE campaign_id = ?",
                (status, _clip(reason) if status == CAMPAIGN_STATUS_PAUSED else "", campaign_id),
            )
            if result.rowcount == 0:
                raise StateError(f"campaign {campaign_id!r} does not exist")

    @_mutation
    def authorize_campaign_drain(
        self, campaign_id: str, digest: str, *, iso: str, expected_campaign: CampaignRecord
    ) -> int:
        """Validate approval and settled canary under the same write lock as the grant.

        The caller's approval snapshot binds the operator inspection. All ledger and
        ownership reads occur after BEGIN IMMEDIATE, fencing other store handles.
        """
        _require_iso(iso, "iso")
        _require_hex64(digest, "drain authorization digest")
        with self._transaction():
            campaign = self.get_campaign(campaign_id)
            if campaign is None:
                raise CampaignDrainRefusedError(f"campaign {campaign_id!r} does not exist")
            if (
                campaign.digest != digest
                or campaign.approval_iso != expected_campaign.approval_iso
                or campaign.bindings != expected_campaign.bindings
                or campaign.drain_authorized_iso != expected_campaign.drain_authorized_iso
                or campaign.drain_authorized_digest != expected_campaign.drain_authorized_digest
                or campaign.drain_invalidated_reason != expected_campaign.drain_invalidated_reason
            ):
                raise CampaignDrainRefusedError(
                    f"campaign {campaign_id} approval or drain authorization changed; "
                    "inspect the current canary and authorize again"
                )
            attempted_count = self._validate_campaign_drain_canary_tx(campaign)
            self._conn.execute(
                "UPDATE campaigns SET drain_authorized_iso = ?, drain_authorized_digest = ?,"
                " drain_invalidated_reason = '' WHERE campaign_id = ?",
                (iso, digest, campaign_id),
            )
        return attempted_count

    def _validate_campaign_drain_canary_tx(self, campaign: CampaignRecord) -> int:
        """Return attempted count only for settled history; caller holds write lock."""
        campaign_id = campaign.campaign_id
        if campaign.status == CAMPAIGN_STATUS_REVOKED:
            raise CampaignDrainRefusedError(
                f"campaign {campaign_id} is revoked: drain authorization is refused"
            )
        if campaign.status != CAMPAIGN_STATUS_APPROVED:
            raise CampaignDrainRefusedError(
                f"campaign {campaign_id} is {campaign.status} ({campaign.status_reason}): drain "
                "authorization requires an approved campaign — resume it first"
            )
        unresolved = self.unresolved_campaign_send_history()
        if unresolved:
            send_ids = ", ".join(str(row.send_id) for row in unresolved)
            raise CampaignDrainRefusedError(
                f"campaign {campaign_id} canary is blocked by ambiguous legacy send(s) {send_ids}; "
                "resolve each with campaign history-resolve before authorizing a drain"
            )
        history = self.campaign_send_history(campaign_id)
        # A voided ``prepared_not_sent`` row is a definite zero-write (never attempted):
        # it is durable audit history but is not a canary send and never blocks the grant.
        attempted = [row for row in history if row.outcome != OUTCOME_VOIDED]
        if not attempted:
            raise CampaignDrainRefusedError(
                f"campaign {campaign_id} has no completed canary (zero submitted files): run the "
                "manual canary batch, verify it, inspect the physical-media side effects, then "
                "authorize the drain"
            )
        # Only definite zero-write rejection/refusal may be settled by a later verified
        # retry. A later success cannot account for an earlier uncertain request.
        latest = {row.shoko_file_id: row for row in attempted}
        outstanding = [
            row
            for row in attempted
            if (
                row.outcome != OUTCOME_VERIFIED
                and not (
                    row.outcome in (SendOutcome.REJECTED.value, SendOutcome.REFUSED.value)
                    and latest[row.shoko_file_id].outcome == OUTCOME_VERIFIED
                    and latest[row.shoko_file_id].file_state is FileState.VERIFIED
                )
            )
            or row.file_state is not FileState.VERIFIED
        ]
        if outstanding:
            listing = "; ".join(
                f"file {row.shoko_file_id} (send {row.send_id}: outcome {row.outcome}, "
                f"file state {row.file_state.value})"
                for row in outstanding
            )
            raise CampaignDrainRefusedError(
                f"campaign {campaign_id} canary is not settled: {len(outstanding)} attempted "
                f"send(s) in the durable send ledger are not terminal VERIFIED — {listing}. Drain "
                "authorization requires every attempted campaign send VERIFIED: a rejected/refused "
                "send is not a verified association, and an in-flight/indeterminate_write/partial/"
                "conflict send is unreviewed media-side-effect risk. Resubmit the refused file "
                "through the normal bounded flow, verify it, inspect the side "
                "effects, then authorize the drain"
            )
        return len(attempted)

    @_mutation
    def revoke_campaign_drain(self, campaign_id: str, reason: str, *, iso: str) -> None:
        """F10: withdraw the drain authorization; the reason stays visible in the report.

        The campaign's own status is untouched (manual apply stays available): revocation stops
        the automatic drain only, and a later re-grant is the operator's decision.
        """
        _require_iso(iso, "iso")
        if not isinstance(reason, str) or not reason.strip():
            raise StateError("revoke reason must be a non-empty string")
        with self._transaction():
            result = self._conn.execute(
                "UPDATE campaigns SET drain_authorized_iso = NULL, drain_authorized_digest = NULL,"
                " drain_invalidated_reason = ? WHERE campaign_id = ?",
                (_clip(reason), campaign_id),
            )
            if result.rowcount == 0:
                raise StateError(f"campaign {campaign_id!r} does not exist: nothing to revoke")

    @_mutation
    def record_campaign_send(self, campaign_id: str, iso: str) -> int:
        """Compatibility sync from the attempted-send ledger; never increments independently.

        Campaign attempts are now atomically debited by :meth:`mark_sent` before transport.
        Existing callers of this post-outcome hook remain safe and idempotent: it only raises a
        stale total to the durable ledger count and can never double-count or refund an attempt.
        """
        _require_iso(iso, "iso")
        with self._transaction():
            row = self._conn.execute(
                "SELECT total_sent FROM campaigns WHERE campaign_id = ?",
                (campaign_id,),
            ).fetchone()
            if row is None:
                raise StateError(f"campaign {campaign_id!r} does not exist")
            total = _as_int(row[0], "total_sent")
            total = max(total, self._campaign_attempt_count_tx(campaign_id))
            self._conn.execute(
                "UPDATE campaigns SET total_sent = ?, last_batch_iso = ?" " WHERE campaign_id = ?",
                (total, iso, campaign_id),
            )
        return total

    def campaign_files(self, campaign_id: str) -> tuple[FileRecord, ...]:
        rows = self._conn.execute(_SQL_CAMPAIGN_FILES, (campaign_id,)).fetchall()
        return tuple(_row_to_file_record(row) for row in rows)

    def campaign_send_history(self, campaign_id: str) -> tuple[SendHistoryRow, ...]:
        """F19: every send row ever attempted for one campaign, in send order (read-only).

        The durable ``sends`` ledger is the campaign's attempted-send history: rows keep their
        outcome after the file moves on (a REJECTED/REFUSED send's file returns to ``waiting``
        and the row stays), so the drain-authorization gate can account for every attempt.
        Ownership is read from the immutable send ledger, not the mutable current file binding;
        queued rows unbound by revoke have no attempted sends to report."""
        rows = self._conn.execute(
            "SELECT s.send_id, s.shoko_file_id, s.outcome, s.verification, s.attempted_iso,"
            " s.http_status, f.state FROM sends s"
            " JOIN files f ON f.shoko_file_id = s.shoko_file_id"
            " WHERE s.campaign_ownership = ? AND s.campaign_id = ? ORDER BY s.send_id",
            (CAMPAIGN_OWNERSHIP_CAMPAIGN, campaign_id),
        ).fetchall()
        return tuple(
            SendHistoryRow(
                send_id=_as_int(row[0], "send_id"),
                shoko_file_id=_as_int(row[1], "shoko_file_id"),
                outcome=_as_str(row[2], "outcome"),
                verification=_as_optional_str(row[3]),
                attempted_iso=_as_str(row[4], "attempted_iso"),
                http_status=_as_optional_int(row[5]),
                file_state=FileState(_as_str(row[6], "state")),
            )
            for row in rows
        )

    def unresolved_campaign_send_history(self) -> tuple[SendHistoryRow, ...]:
        """Return legacy attempted sends whose campaign ownership cannot yet be proven."""
        rows = self._conn.execute(
            "SELECT s.send_id, s.shoko_file_id, s.outcome, s.verification, s.attempted_iso,"
            " s.http_status, f.state FROM sends s"
            " JOIN files f ON f.shoko_file_id = s.shoko_file_id"
            " WHERE s.campaign_ownership = ?"
            " AND s.outcome NOT IN ('prepared', 'voided') ORDER BY s.send_id",
            (CAMPAIGN_OWNERSHIP_UNKNOWN,),
        ).fetchall()
        return tuple(
            SendHistoryRow(
                send_id=_as_int(row[0], "send_id"),
                shoko_file_id=_as_int(row[1], "shoko_file_id"),
                outcome=_as_str(row[2], "send outcome"),
                verification=_as_optional_str(row[3]),
                attempted_iso=_as_str(row[4], "attempted_iso"),
                http_status=_as_optional_int(row[5]),
                file_state=FileState(_as_str(row[6], "state")),
            )
            for row in rows
        )

    def _legacy_campaign_resolution_is_proven_tx(
        self,
        campaign_id: str,
        send_row: Any,
        possible_candidates: Sequence[str],
    ) -> bool:
        if campaign_id not in possible_candidates:
            return False
        approved = self._conn.execute(
            "SELECT digest, plan_path, plan_fingerprint_set FROM campaigns"
            " WHERE campaign_id = ?",
            (campaign_id,),
        ).fetchone()
        if approved is None:
            return False
        return _legacy_campaign_identity_scope_matches(
            campaign_id=campaign_id,
            campaign_digest=approved[0],
            plan_path=approved[1],
            approved_bindings_raw=approved[2],
            shoko_file_id=send_row[0],
            plan_fingerprint=send_row[1],
            request_fingerprint=send_row[2],
            target_ids_raw=send_row[6],
        )

    @_mutation
    def resolve_legacy_send_ownership(
        self,
        send_id: int,
        *,
        campaign_id: str | None,
        note: str,
        iso: str,
    ) -> None:
        """Explicitly adjudicate one ambiguous legacy send and restore its immutable owner.

        ``campaign_id=None`` is an explicit non-campaign resolution, distinct from an unknown
        legacy owner. Campaign choices must match the immutable send tuple against a valid,
        persisted approved binding and its approval/deadline window. Attempt counters only
        increase; they are reconciled from the now-resolved immutable ledger row.
        """
        _require_iso(iso, "iso")
        if not isinstance(note, str) or not note.strip():
            raise StateError("legacy send ownership resolution requires an operator note")
        with self._transaction():
            row = self._conn.execute(
                "SELECT shoko_file_id, plan_fingerprint, request_fingerprint, outcome,"
                " attempted_iso, campaign_ownership, target_ids_json FROM sends WHERE send_id = ?",
                (send_id,),
            ).fetchone()
            if row is None:
                raise StateError(f"send {send_id} does not exist")
            outcome = _as_str(row[3], "send outcome")
            if outcome in (OUTCOME_PREPARED, OUTCOME_VOIDED):
                raise StateError(
                    f"send {send_id} has no attempted-send history to resolve; ownership remains "
                    "unchanged"
                )
            ownership = _as_str(row[5], "campaign ownership")
            if ownership != CAMPAIGN_OWNERSHIP_UNKNOWN:
                raise StateError(f"send {send_id} ownership is already {ownership!r}")
            campaign_rows = self._conn.execute(
                "SELECT campaign_id, digest, attestation_scope, approval_iso, deadline_iso,"
                " created_iso, plan_fingerprint_set FROM campaigns"
            ).fetchall()
            candidates, snapshots_valid = _legacy_campaign_owner_candidates(
                shoko_file_id=row[0],
                plan_fingerprint=row[1],
                request_fingerprint=row[2],
                attempted_iso=row[4],
                campaign_rows=campaign_rows,
            )
            if campaign_id is not None:
                if not isinstance(campaign_id, str) or not campaign_id.strip():
                    raise StateError("campaign_id must be a non-empty string")
                if not snapshots_valid:
                    raise StateError(
                        f"send {send_id} cannot be assigned to campaign {campaign_id!r}: "
                        "persisted approved campaign evidence or the send timestamp is invalid; "
                        "ownership remains unresolved"
                    )
                if not self._legacy_campaign_resolution_is_proven_tx(campaign_id, row, candidates):
                    compatible = ", ".join(candidates) if candidates else "none"
                    raise StateError(
                        f"send {send_id} cannot be assigned to campaign {campaign_id!r}: no "
                        "matching persisted approved binding file/identity scope covers this "
                        "send and "
                        f"attempt time; compatible campaigns: {compatible}; ownership remains "
                        "unresolved. Review the immutable send and approved snapshot identities; "
                        "confirmation cannot authorize an incompatible owner"
                    )
                next_ownership = CAMPAIGN_OWNERSHIP_CAMPAIGN
            else:
                # This branch is only reachable through the explicit --non-campaign CLI choice
                # (plus its --confirm and required operator note). It records a human attestation;
                # migration never makes this inference when any compatible campaign exists.
                next_ownership = CAMPAIGN_OWNERSHIP_NON_CAMPAIGN
            self._conn.execute(
                "UPDATE sends SET campaign_id = ?, campaign_ownership = ?,"
                " campaign_ownership_resolved_iso = ?, campaign_ownership_note = ?"
                " WHERE send_id = ?",
                (campaign_id, next_ownership, iso, _clip(note), send_id),
            )
            if campaign_id is not None:
                campaign = self._conn.execute(
                    "SELECT total_sent FROM campaigns WHERE campaign_id = ?", (campaign_id,)
                ).fetchone()
                assert campaign is not None
                total = max(
                    _as_int(campaign[0], "total_sent"),
                    self._campaign_attempt_count_tx(campaign_id),
                )
                self._conn.execute(
                    "UPDATE campaigns SET total_sent = ? WHERE campaign_id = ?",
                    (total, campaign_id),
                )

    def campaign_progress(self, campaign_id: str) -> dict[str, int]:
        """Per-state file counts for one campaign (quarantined counted from detail)."""
        progress = {state.value: 0 for state in FileState}
        for row in self._conn.execute(
            "SELECT state, COUNT(*) FROM files WHERE campaign_id = ? GROUP BY state",
            (campaign_id,),
        ).fetchall():
            progress[str(row[0])] = _as_int(row[1], "count")
        quarantined = self._conn.execute(
            "SELECT COUNT(*) FROM files WHERE campaign_id = ?"
            " AND state_detail LIKE 'campaign_quarantine%'",
            (campaign_id,),
        ).fetchone()
        progress["quarantined"] = _as_int(quarantined[0], "quarantined_count")
        campaign = self.get_campaign(campaign_id)
        progress["total_sent"] = 0 if campaign is None else campaign.total_sent
        progress["approved_files"] = 0 if campaign is None else len(campaign.bindings)
        return progress

    @_mutation
    def claim_campaign_quarantine_progress(self, shoko_file_id: int, campaign_id: str) -> bool:
        """Atomically claim one persisted campaign quarantine as newly reported progress.

        Reconciliation can quarantine a bound campaign file in a cycle that does not drain the
        campaign. The marker lives in ``state_detail`` so a later apply (including after restart)
        can count it once without treating every repeated observation as new progress.
        """
        with self._transaction():
            changed = self._conn.execute(
                "UPDATE files SET state_detail = REPLACE(state_detail, ?, ?)"
                " WHERE shoko_file_id = ? AND campaign_id = ? AND state = ?"
                " AND instr(state_detail, ?) > 0",
                (
                    CAMPAIGN_QUARANTINE_PROGRESS_PENDING_MARKER,
                    CAMPAIGN_QUARANTINE_PROGRESS_RECORDED_MARKER,
                    shoko_file_id,
                    campaign_id,
                    FileState.BLOCKED_SAFETY.value,
                    CAMPAIGN_QUARANTINE_PROGRESS_PENDING_MARKER,
                ),
            ).rowcount
        return changed == 1

    @_mutation
    def unbind_campaign_files(self, campaign_id: str, reason: str, *, iso: str) -> int:
        """Revoke path: queued (planned/mapped) bound rows go to ``waiting`` with the reason.

        Rows past ``planned`` (prepared/sent/verifying) are **never** rewound —
        the sent-row contract (verify/hold, no resubmission) stands.
        """
        _require_iso(iso, "iso")
        with self._transaction():
            rows = self._conn.execute(
                "SELECT shoko_file_id, state FROM files WHERE campaign_id = ?"
                " ORDER BY shoko_file_id",
                (campaign_id,),
            ).fetchall()
            rewound = 0
            for file_id, raw in rows:
                current = FileState(_as_str(raw, "state"))
                if current not in CAMPAIGN_QUEUE_STATES:
                    continue
                target = FileState.WAITING
                validate_transition(current, target)
                self._conn.execute(
                    "UPDATE files SET state = ?, state_detail = ?, updated_iso = ?,"
                    " campaign_id = NULL WHERE shoko_file_id = ?",
                    (
                        target.value,
                        _clip(f"campaign unbound: {reason}"),
                        iso,
                        _as_int(file_id, "shoko_file_id"),
                    ),
                )
                rewound += 1
        return rewound


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
    """Meta-only apply-lock acquire (a held lease raises :class:`LockHeldError`).

    Low-level primitive used to seed/inspect a meta lease record; it does NOT take the OS
    advisory lock. Daemon/campaign apply uses :func:`acquire_or_steal_apply_lock`."""
    return store.acquire_apply_lock_tx(pid, iso, lease_expires_iso)


def acquire_or_steal_apply_lock(
    store: StateStore, pid: int, iso: str, *, lease_expires_iso: str
) -> LockAcquisition:
    """Take exclusive apply ownership: the OS advisory lock FIRST, then the meta lease.

    Contention on the OS lock (a live apply process — a restart race, a concurrent operator
    campaign) raises :class:`LockHeldError` naming the meta holder and mutates NOTHING: a meta
    lease expiry on its own NEVER grants ownership, so a live apply operation can never
    be fenced out by a fixed timeout. When the OS lock is acquirable, the meta
    lease is taken/replaced/stolen under the existing rule (an expired lease + a free OS lock is
    a legitimate takeover of a DEAD owner, audit-recorded). Daemon/campaign apply path (F5)."""
    return store.acquire_apply_ownership(pid, iso, lease_expires_iso)


def release_apply_lock(store: StateStore, pid: int, iso: str) -> bool:
    """Release meta ownership (pid must be the owner) then the OS advisory lock.

    Returns False when no lock is held; raises :class:`LockHeldError` for a non-owner release."""
    return store.release_apply_ownership(pid, iso)


def steal_expired_apply_lock(
    store: StateStore, pid: int, iso: str, *, lease_expires_iso: str | None = None
) -> LockAcquisition:
    """Explicitly take over an expired meta lease (recovery of a crashed holder).

    Meta-only primitive: it does NOT consult the OS advisory lock. The operator
    ``steal-apply-lock`` command uses :func:`steal_apply_lock` (which requires a free OS lock)."""
    return store.steal_expired_apply_lock_tx(pid, iso, lease_expires_iso)


def steal_apply_lock(
    store: StateStore, pid: int, iso: str, *, lease_expires_iso: str | None = None
) -> LockAcquisition:
    """Operator takeover marker (``steal-apply-lock``): requires a FREE OS advisory lock.

    A live owner (OS lock held) raises :class:`LockHeldError`; an expired meta lease with a free
    OS lock is audit-recorded as a steal, and the OS lock is released immediately after."""
    return store.steal_apply_ownership(pid, iso, lease_expires_iso)


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
