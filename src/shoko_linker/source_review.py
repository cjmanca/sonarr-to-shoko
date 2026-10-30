"""Explicit local continuity recovery, driven by the existing service worker.

This module does not collect remote data or authorize linking. The production adapter
must implement the small trusted collector contract below; a form supplies only a
session proof and (on confirmation) a token, never assessment evidence.
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import IntEnum
from pathlib import Path
from typing import Literal, Protocol

from .state import (
    MAX_SOURCE_REBASELINE_SECONDS,
    SourceContinuityReview,
    SourceRebaselineAssessment,
    SourceRebaselineBindings,
    SourceRebaselinePrepared,
    StateError,
    StateStore,
    open_state,
)

ReviewStatus = Literal[
    "idle", "queued", "assessing", "ready", "confirming", "changed", "confirmed", "error"
]


class SourceReviewTick(IntEnum):
    """Only IDLE permits ordinary work in the current operation."""

    IDLE = 0
    OWNED = 1
    ACCEPTED = 2


@dataclass(frozen=True)
class SourceReviewSummary:
    selected_roots: int
    sonarr_series: int
    sonarr_files: int
    shoko_files: int
    shoko_folders: int


@dataclass(frozen=True)
class SourceReviewEvidence:
    """Only a complete, fresh, review-owned sweep may produce this result.

    The strong assessment digest covers normalized paths and relationships plus
    endpoint/scope/mapping/safety/consent identity, excluding poll timestamps and
    display labels. Its weak observation must use the ordinary daemon continuity
    summary. An empty file inventory is valid; an empty selected root set is not.
    """

    assessment: SourceRebaselineAssessment
    discovered_file_ids: frozenset[int]
    summary: SourceReviewSummary
    sonarr_complete: bool
    sonarr_sweep_complete: bool
    shoko_complete: bool
    shoko_sweep_complete: bool


@dataclass
class SourceReviewWork:
    """One sweep's private progress; every explicit confirmation gets a NEW instance.

    The adapter starts with empty progress, never daemon/setup caches. It may retain
    only this sweep's collector cursors/pages/children between worker budgets. A
    manifest/folder/scope/endpoint change must fail the job, not silently reuse pages.
    """

    started_iso: str
    bindings: SourceRebaselineBindings
    shutdown_requested: Callable[[], bool] = field(repr=False)
    progress: object | None = field(default=None, repr=False)


class SourceReviewCollector(Protocol):
    """Trusted read-only adapter, invoked exclusively by worker_tick.

    capture_bindings reads CURRENT effective settings, scope, consent and mapping
    content, and uses store.get_source_rebaseline_local_binding(). It must reject
    unavailable mappings or saved URLs requiring restart. collect must obtain fresh
    complete environment/folder/safety facts through current_context with the SAME
    deadline, validate current consent independently of user pause (never Resume),
    and recheck remote facts before returning evidence. Both methods honor deadline
    on each read/chunk and work.shutdown_requested during collection. Only GET is
    allowed remotely. None means incomplete resumable work, never ready evidence.
    """

    def capture_bindings(self, store: StateStore, deadline: float) -> SourceRebaselineBindings: ...

    def collect(
        self, store: StateStore, work: SourceReviewWork, deadline: float
    ) -> SourceReviewEvidence | None: ...


@dataclass(frozen=True)
class SourceReviewView:
    status: ReviewStatus
    continuity: SourceContinuityReview
    token: str | None = field(default=None, repr=False)
    summary: SourceReviewSummary | None = None
    started_iso: str | None = None
    completed_iso: str | None = None
    message: str = ""
    service_pause: tuple[bool, str | None] = (False, None)
    writes_disabled: bool = False


@dataclass
class _Job:
    owner_session: str = field(repr=False)
    owner_identity: str = field(repr=False)
    requested_iso: str
    expires_iso: str
    expires_mono: float
    hold_id: str | None
    previous_review: SourceRebaselinePrepared | None = field(repr=False)
    prepared: SourceRebaselinePrepared | None = field(default=None, repr=False)
    work: SourceReviewWork | None = None
    running: bool = False


@dataclass(frozen=True)
class _ReadyReview:
    prepared: SourceRebaselinePrepared
    expires_mono: float


def _owner_identity(store: StateStore, owner: str, now: datetime) -> str:
    """Same stable owner binding as state; CSRF rotation is intentionally excluded."""
    session = store.get_dashboard_session(owner, int(now.timestamp()))
    account = store.get_dashboard_account()
    if session is None or account is None:
        raise StateError("source review owner session is unavailable")
    document = {
        "session": owner,
        "username": account.username,
        "account_created_iso": account.created_iso,
        "session_created_iso": session.created_iso,
        "account": hashlib.sha256(
            account.password_hash.encode("utf-8") + account.signing_key
        ).hexdigest(),
    }
    return hashlib.sha256(
        json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


class SourceReviewController:
    """A single owned review job; requests queue, local views never schedule work.

    A non-IDLE worker_tick result owns this operation, including a failed/expired
    review. The caller must skip its ordinary link cycle for that tick; ACCEPTED
    requests a prompt separate operation to reassess ordinary authorization gates.
    No threads, polling, I/O on status reads, or automatic expiry refresh are started.
    A restart requires a new explicit assessment; durable old tokens alone cannot
    create confirmation work without this controller's current owned review.
    """

    def __init__(
        self,
        state_path: Path,
        collector: SourceReviewCollector,
        *,
        wake: Callable[[], None],
        shutdown_requested: Callable[[], bool],
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
        lifetime_seconds: int = MAX_SOURCE_REBASELINE_SECONDS,
    ) -> None:
        if not 0 < lifetime_seconds <= MAX_SOURCE_REBASELINE_SECONDS:
            raise ValueError("invalid source review lifetime")
        self._path = state_path
        self._collector = collector
        self._wake = wake
        self._shutdown = shutdown_requested
        self._now = now
        self._monotonic = monotonic
        self._lifetime = lifetime_seconds
        self._lock = threading.Lock()
        self._job: _Job | None = None
        self._owner: str | None = None
        self._ready: _ReadyReview | None = None
        self._status: ReviewStatus = "idle"
        self._summary: SourceReviewSummary | None = None
        self._interval: tuple[str | None, str | None] = (None, None)
        self._message = ""
        self._display_hold_id: str | None = None

    def view(self, owner_session_sha256: str) -> SourceReviewView:
        """Local reads only, including expiry checks; never repair or enqueue."""
        with self._lock, open_state(self._path) as store:
            return self._view(store, owner_session_sha256, self._now())

    def _view(self, store: StateStore, owner: str, now: datetime) -> SourceReviewView:
        identity = _owner_identity(store, owner, now)
        continuity = store.get_source_continuity_review()
        status: ReviewStatus = "idle"
        token = None
        message = ""
        summary = None
        interval: tuple[str | None, str | None] = (None, None)
        if self._status == "confirmed":
            audit = store.get_source_rebaseline_review().last_audit
            same_lifecycle = (
                not continuity.requires_rebaseline
                and audit is not None
                and audit.prepared.hold_id == self._display_hold_id
            )
        else:
            same_lifecycle = self._matches_hold(continuity, self._display_hold_id)
        if identity == self._owner and same_lifecycle:
            status, message, summary, interval = (
                self._status,
                self._message,
                self._summary,
                self._interval,
            )
            if self._job is not None and self._expired(self._job, now):
                status, message = "error", "The source review expired. Request a new assessment."
            elif self._ready is not None and status in ("ready", "changed"):
                prepared = store.get_source_rebaseline_review().prepared
                ready = self._ready.prepared
                if (
                    prepared != ready
                    or self._ready_expired(now)
                    or not continuity.requires_rebaseline
                    or continuity.hold is None
                    or continuity.hold.hold_id != ready.hold_id
                    or store.get_source_rebaseline_local_binding()
                    != ready.bindings.local_state_sha256
                ):
                    status, message = (
                        "error",
                        "The source review changed. Request a new assessment.",
                    )
                else:
                    token = ready.token
        return SourceReviewView(
            status,
            continuity,
            token,
            summary,
            *interval,
            message,
            store.get_service_pause(),
            store.writes_disabled,
        )

    def request_assessment(self, owner_session_sha256: str) -> SourceReviewView:
        with self._lock, open_state(self._path) as store:
            now = self._now()
            identity = _owner_identity(store, owner_session_sha256, now)
            self._require_available(store, identity, now)
            current = self._view(store, owner_session_sha256, now)
            if self._job is not None:
                return current
            if current.status in ("queued", "assessing", "confirming", "ready", "changed"):
                return current
            review = store.get_source_continuity_review()
            if not review.requires_rebaseline or review.evidence_status == "invalid":
                raise StateError("source continuity hold is unavailable")
            previous = store.get_source_rebaseline_review()
            if not previous.valid:
                raise StateError("source review evidence is invalid")
            self._queue(owner_session_sha256, identity, now, review, None, previous.prepared)
            result = self._view(store, owner_session_sha256, now)
        self._wake()
        return result

    def request_confirmation(self, token: str, owner_session_sha256: str) -> SourceReviewView:
        with self._lock, open_state(self._path) as store:
            now = self._now()
            identity = _owner_identity(store, owner_session_sha256, now)
            self._require_available(store, identity, now)
            current = self._view(store, owner_session_sha256, now)
            prepared = store.get_source_rebaseline_review().prepared
            if self._job is not None and self._job.prepared is not None:
                if token != self._job.prepared.token:
                    raise StateError("source review token is unavailable")
                return current
            if (
                identity != self._owner
                or self._ready is None
                or prepared != self._ready.prepared
                or current.token != token
                or current.status not in ("ready", "changed")
            ):
                raise StateError("source review token is unavailable")
            self._queue(owner_session_sha256, identity, now, current.continuity, prepared, prepared)
            result = self._view(store, owner_session_sha256, now)
        self._wake()
        return result

    @staticmethod
    def _matches_hold(review: SourceContinuityReview, hold_id: str | None) -> bool:
        return (
            review.requires_rebaseline
            and (None if review.hold is None else review.hold.hold_id) == hold_id
        )

    def _require_available(self, store: StateStore, identity: str, now: datetime) -> None:
        # Only explicit requests retire obsolete local ownership. GET remains read-only;
        # an adapter currently running still owns its progress until its next check.
        review = store.get_source_continuity_review()
        if (
            self._job is not None
            and not self._job.running
            and not self._matches_hold(review, self._job.hold_id)
        ):
            self._job = None
        if self._ready is not None and not self._matches_hold(review, self._ready.prepared.hold_id):
            self._ready = None
        if self._job is not None:
            # A running adapter owns its progress even if its session just expired.
            if self._job.running or not self._expired(self._job, now):
                if identity != self._job.owner_identity:
                    raise StateError("source review belongs to another session")
            else:
                self._job = None
                self._status = "error"
        if self._ready is not None and not self._ready_expired(now) and identity != self._owner:
            raise StateError("source review belongs to another session")

    def _ready_expired(self, now: datetime) -> bool:
        return (
            self._ready is None
            or now >= datetime.fromisoformat(self._ready.prepared.expires_iso)
            or self._monotonic() >= self._ready.expires_mono
        )

    def _queue(
        self,
        owner: str,
        identity: str,
        now: datetime,
        review: SourceContinuityReview,
        prepared: SourceRebaselinePrepared | None,
        previous_review: SourceRebaselinePrepared | None,
    ) -> None:
        expires_mono = self._monotonic() + self._lifetime
        if prepared is not None:
            # A confirmation gets only its token's ORIGINAL remaining lifetime.
            # A database token without the original process-local deadline is unusable.
            if self._ready is None or self._ready.prepared != prepared or self._ready_expired(now):
                raise StateError("source review token is unavailable")
            expires_mono = min(expires_mono, self._ready.expires_mono)
        else:
            self._ready = None
        self._owner = identity
        self._display_hold_id = None if review.hold is None else review.hold.hold_id
        self._job = _Job(
            owner,
            identity,
            now.isoformat(),
            (now + timedelta(seconds=self._lifetime)).isoformat(),
            expires_mono,
            None if review.hold is None else review.hold.hold_id,
            previous_review,
            prepared,
        )
        self._status = "queued" if prepared is None else "confirming"
        self._summary = None
        self._interval = (None, None)
        self._message = "A fresh read-only source assessment is queued."

    def _expired(self, job: _Job, now: datetime) -> bool:
        return (
            now >= datetime.fromisoformat(job.expires_iso)
            or self._monotonic() >= job.expires_mono
            or (
                job.prepared is not None and now >= datetime.fromisoformat(job.prepared.expires_iso)
            )
        )

    def _check_job(self, store: StateStore, job: _Job) -> None:
        now = self._now()
        review = store.get_source_continuity_review()
        if (
            self._shutdown()
            or self._expired(job, now)
            or _owner_identity(store, job.owner_session, now) != job.owner_identity
            or not self._matches_hold(review, job.hold_id)
            or review.evidence_status == "invalid"
        ):
            raise StateError("source review is no longer current")
        durable = store.get_source_rebaseline_review()
        if not durable.valid or durable.prepared != job.previous_review:
            raise StateError("source review was replaced")

    def _bindings(self, store: StateStore, deadline: float) -> SourceRebaselineBindings:
        result = self._collector.capture_bindings(store, deadline)
        if result.local_state_sha256 != store.get_source_rebaseline_local_binding():
            raise StateError("source review authorization changed")
        return result

    def worker_tick(self, deadline: float) -> SourceReviewTick:
        """Run one slice; acceptance requires a separate ordinary reassessment tick."""
        with self._lock:
            job = self._job
            if job is None:
                return SourceReviewTick.IDLE
            if job.running:
                return SourceReviewTick.OWNED
            job.running = True
            self._status = "assessing" if job.prepared is None else "confirming"
        try:
            with open_state(self._path) as store:
                self._collect_slice(store, job, deadline)
        except Exception:
            # No exception/remote response text enters a local view or log.
            with self._lock:
                self._job = None
                self._ready = None
                self._status = "error"
                self._summary = None
                self._interval = (None, None)
                self._message = (
                    "The source review could not finish safely. Request a new assessment."
                )
        finally:
            with self._lock:
                job.running = False
        with self._lock:
            return (
                SourceReviewTick.ACCEPTED if self._status == "confirmed" else SourceReviewTick.OWNED
            )

    def continuation_delay(self) -> float | None:
        """Yield between owned slices, including collectors that return no progress.

        Only an explicit outstanding job gets this short retry. Expiry and session
        checks still run in the next bounded slice; this never renews their deadlines.
        """
        with self._lock:
            return 1.0 if self._job is not None else None

    def _collect_slice(self, store: StateStore, job: _Job, deadline: float) -> None:
        self._check_job(store, job)
        bounded = min(deadline, job.expires_mono)
        if self._monotonic() >= bounded:
            return
        if job.work is None:
            started = self._now().isoformat()
            # Do not manufacture a later timestamp or reuse first-pass data.
            if job.prepared is not None and (
                datetime.fromisoformat(started) <= datetime.fromisoformat(job.requested_iso)
                or datetime.fromisoformat(started)
                <= datetime.fromisoformat(job.prepared.created_iso)
            ):
                return
            bindings = self._bindings(store, bounded)
            self._check_job(store, job)
            if job.prepared is not None and bindings != job.prepared.bindings:
                raise StateError("source review settings changed")
            job.work = SourceReviewWork(started, bindings, self._shutdown)
        self._check_job(store, job)
        if self._bindings(store, bounded) != job.work.bindings:
            raise StateError("source review settings changed")
        if self._monotonic() >= bounded:
            return
        evidence = self._collector.collect(store, job.work, bounded)
        self._check_job(store, job)
        if self._monotonic() >= bounded:
            # A partial slice may save owned progress; late evidence is unusable.
            if evidence is not None:
                raise StateError("source review exceeded its worker budget")
            return
        if self._bindings(store, bounded) != job.work.bindings:
            raise StateError("source review settings changed")
        self._check_job(store, job)
        if self._monotonic() >= bounded:
            raise StateError("source review exceeded its worker budget")
        if evidence is not None:
            self._finish(store, job, evidence)

    def _finish(self, store: StateStore, job: _Job, evidence: SourceReviewEvidence) -> None:
        assert job.work is not None
        assessment = evidence.assessment
        started = datetime.fromisoformat(assessment.started_iso)
        completed = datetime.fromisoformat(assessment.completed_iso)
        if (
            evidence.sonarr_complete is not True
            or evidence.sonarr_sweep_complete is not True
            or evidence.shoko_complete is not True
            or evidence.shoko_sweep_complete is not True
            or evidence.summary.selected_roots <= 0
            or assessment.observation.discovery_total is None
            or assessment.started_iso != job.work.started_iso
            or not started <= completed <= self._now()
            or (self._now() - started).total_seconds() > MAX_SOURCE_REBASELINE_SECONDS
            or re.fullmatch(r"[0-9a-f]{64}", assessment.content_sha256) is None
        ):
            raise StateError("complete fresh source review evidence required")
        self._check_job(store, job)
        observation = assessment.observation
        store.check_source_continuity(
            observation.instance_identity,
            observation.snapshot_summary_id,
            iso=assessment.completed_iso,
            roots_fingerprint=observation.roots_fingerprint,
            discovery_total=observation.discovery_total,
            complete=True,
            discovered_file_ids=evidence.discovered_file_ids,
        )
        # check_source_continuity initializes unknown legacy evidence only during
        # this explicit worker operation. Original counts/time remain unknown.
        current = store.get_source_continuity_review()
        job.hold_id = None if current.hold is None else current.hold.hold_id
        self._check_job(store, job)
        now = self._now()
        if job.prepared is None or job.prepared.assessment_sha256 != assessment.content_sha256:
            # Bind a new deadline to THIS new token, including time spent preparing it.
            prepared_expires_mono = self._monotonic() + self._lifetime
            prepared = store.prepare_source_rebaseline(
                assessment,
                expected_hold_id=job.hold_id,
                bindings=job.work.bindings,
                owner_session_sha256=job.owner_session,
                iso=now.isoformat(),
                expires_iso=(now + timedelta(seconds=self._lifetime)).isoformat(),
            )
            ready = _ReadyReview(prepared, prepared_expires_mono)
            status: ReviewStatus = "ready" if job.prepared is None else "changed"
            message = (
                "Review the complete source assessment before accepting the local baseline."
                if job.prepared is None
                else "The sources changed. Review the new assessment and explicitly accept again."
            )
        else:
            store.confirm_source_rebaseline(
                job.prepared.token,
                assessment,
                bindings=job.work.bindings,
                owner_session_sha256=job.owner_session,
                iso=now.isoformat(),
            )
            ready, status = None, "confirmed"
            message = (
                "The local source baseline was accepted. Existing pauses and safety gates remain."
            )
        with self._lock:
            self._display_hold_id = job.hold_id
            self._ready = ready
            self._status, self._message = status, message
            self._summary = evidence.summary
            self._interval = assessment.started_iso, assessment.completed_iso
            self._job = None
