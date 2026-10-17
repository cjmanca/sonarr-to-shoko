"""Deterministic, persistent feedback control for normal-service submissions."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

from .state import BudgetLimits

RATE_STATE_SCHEMA: Final[str] = "shoko-sonarr-linker/service-rate/1"
MAX_SERVICE_CYCLE_SUBMISSIONS: Final[int] = 20
MAX_SERVICE_ROLLING_SUBMISSIONS: Final[int] = 5000
MAX_SERVICE_PENDING_FILES: Final[int] = 20
MAX_SERVICE_PENDING_EPISODE_JOBS: Final[int] = 10
MAX_SERVICE_ROLLING_WINDOW_HOURS: Final[float] = 168.0
_RATE_STEPS: Final[tuple[int, ...]] = (1, 2, 4, 8, 16, MAX_SERVICE_CYCLE_SUBMISSIONS)
_MAX_REASON_CHARS: Final[int] = 100
_MAX_SUCCESSFUL_STREAK: Final[int] = len(_RATE_STEPS)


@dataclass(frozen=True)
class RateState:
    """Durable cap and consecutive healthy-cycle count; ``cap`` applies next cycle."""

    cap: int
    successful_streak: int
    updated_iso: str
    last_reason: str

    def __post_init__(self) -> None:
        if isinstance(self.cap, bool) or not 1 <= self.cap <= MAX_SERVICE_CYCLE_SUBMISSIONS:
            raise ValueError("rate cap is outside the service hard cycle limit")
        if (
            isinstance(self.successful_streak, bool)
            or not 0 <= self.successful_streak <= _MAX_SUCCESSFUL_STREAK
        ):
            raise ValueError("rate successful streak is outside its bounded range")
        _validate_iso(self.updated_iso)
        if not isinstance(self.last_reason, str) or len(self.last_reason) > _MAX_REASON_CHARS:
            raise ValueError("rate reason is invalid")

    @classmethod
    def initial(cls, now_iso: str) -> RateState:
        return cls(cap=1, successful_streak=0, updated_iso=now_iso, last_reason="initial")


@dataclass(frozen=True)
class RateSignals:
    """Signals from the just-finished cycle and durable queue/budget view."""

    queue_healthy: bool
    pending_episode_jobs: int
    max_pending_episode_jobs: int
    submitted: int
    verified: int
    api_healthy: bool
    mapping_stable: bool
    authorization_valid: bool
    anomaly_free: bool
    canary_verified: bool

    def __post_init__(self) -> None:
        for name in (
            "queue_healthy",
            "api_healthy",
            "mapping_stable",
            "authorization_valid",
            "anomaly_free",
            "canary_verified",
        ):
            if not isinstance(getattr(self, name), bool):
                raise ValueError(f"rate signal {name} must be boolean")
        for name in ("pending_episode_jobs", "max_pending_episode_jobs", "submitted", "verified"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"rate signal {name} must be a non-negative integer")
        if self.verified > self.submitted:
            raise ValueError("verified submissions cannot exceed submitted requests")


def _validate_iso(value: str) -> None:
    if not isinstance(value, str):
        raise ValueError("rate timestamp must be ISO-8601 text")
    try:
        datetime.fromisoformat(value)
    except ValueError:
        raise ValueError("rate timestamp must be ISO-8601 text") from None


def encode_rate_state(state: RateState) -> str:
    """Serialize only bounded non-secret controller counters."""
    return json.dumps(
        {
            "schema": RATE_STATE_SCHEMA,
            "cap": state.cap,
            "successful_streak": state.successful_streak,
            "updated_iso": state.updated_iso,
            "last_reason": state.last_reason,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def decode_rate_state(encoded: str) -> RateState:
    """Decode strictly; malformed persisted state is never interpreted as extra capacity."""
    try:
        document: Any = json.loads(encoded)
    except (TypeError, ValueError):
        raise ValueError("persisted service rate state is malformed") from None
    expected = {"schema", "cap", "successful_streak", "updated_iso", "last_reason"}
    if not isinstance(document, dict) or set(document) != expected:
        raise ValueError("persisted service rate state has an unsupported shape")
    if document["schema"] != RATE_STATE_SCHEMA:
        raise ValueError("persisted service rate state has an unsupported schema")
    if (
        isinstance(document["cap"], bool)
        or not isinstance(document["cap"], int)
        or isinstance(document["successful_streak"], bool)
        or not isinstance(document["successful_streak"], int)
        or not isinstance(document["updated_iso"], str)
        or not isinstance(document["last_reason"], str)
    ):
        raise ValueError("persisted service rate state contains invalid values")
    try:
        return RateState(
            cap=document["cap"],
            successful_streak=document["successful_streak"],
            updated_iso=document["updated_iso"],
            last_reason=document["last_reason"],
        )
    except ValueError as exc:
        raise ValueError(f"persisted service rate state is invalid: {exc}") from None


def effective_submission_cap(state: RateState, configured_limit: int) -> int:
    """Apply the configured zero/stricter limit without allowing ramp-up past 20 per cycle."""
    if isinstance(configured_limit, bool) or not isinstance(configured_limit, int):
        raise ValueError("configured submission limit must be an integer")
    if configured_limit < 0:
        raise ValueError("configured submission limit must be non-negative")
    return min(state.cap, configured_limit, MAX_SERVICE_CYCLE_SUBMISSIONS)


def service_budget_limits(configured: BudgetLimits) -> BudgetLimits:
    """Clamp service mode to immutable practical ceilings while honoring stricter settings."""
    return BudgetLimits(
        max_pending_files=min(configured.max_pending_files, MAX_SERVICE_PENDING_FILES),
        max_pending_episode_jobs=min(
            configured.max_pending_episode_jobs, MAX_SERVICE_PENDING_EPISODE_JOBS
        ),
        per_cycle_new=min(configured.per_cycle_new, MAX_SERVICE_CYCLE_SUBMISSIONS),
        # Keep the configured rolling ceiling as-is. The immutable service week is
        # enforced independently below, so clamping this window would silently
        # shorten a stricter operator-configured window longer than seven days.
        rolling_new=configured.rolling_new,
        rolling_window_hours=configured.rolling_window_hours,
        # A configured short window is additive, never a replacement for the hard week.
        additional_rolling_limits=tuple(
            dict.fromkeys(
                (
                    *configured.additional_rolling_limits,
                    (MAX_SERVICE_ROLLING_SUBMISSIONS, MAX_SERVICE_ROLLING_WINDOW_HOURS),
                )
            )
        ),
    )


def _next_cap(current: int, hard_cycle_cap: int) -> int:
    larger_steps = [step for step in _RATE_STEPS if step > current]
    return (
        min(hard_cycle_cap, larger_steps[0])
        if larger_steps and hard_cycle_cap > current
        else current
    )


def _lower_cap(current: int) -> int:
    ceiling = max(1, current // 2)
    return max(step for step in _RATE_STEPS if step <= ceiling)


def _blocker(signals: RateSignals) -> str | None:
    checks = (
        (not signals.queue_healthy, "queue_unhealthy"),
        (
            signals.pending_episode_jobs >= signals.max_pending_episode_jobs,
            "pending_jobs_at_limit",
        ),
        (signals.pending_episode_jobs > 0, "verification_pending"),
        (
            signals.submitted > 0 and signals.verified != signals.submitted,
            "verification_incomplete",
        ),
        (not signals.api_healthy, "api_failure"),
        (not signals.mapping_stable, "mapping_unstable"),
        (not signals.authorization_valid, "authorization_invalid"),
        (not signals.anomaly_free, "cycle_anomaly"),
    )
    return next((reason for blocked, reason in checks if blocked), None)


def advance_rate(
    state: RateState,
    signals: RateSignals,
    *,
    now_iso: str,
    hard_cycle_cap: int,
) -> RateState:
    """Return the next persisted cap from one deterministic observation.

    A successful submission cycle doubles one step only when every request verified and all
    queue, pending-job, source/mapping, API, anomaly, and authorization signals remain healthy.
    Delayed canary verification can trigger the first ramp on a later cycle without requiring a
    duplicate send. Unhealthy observations halve the cap and reset its healthy streak.
    """
    _validate_iso(now_iso)
    if isinstance(hard_cycle_cap, bool) or not isinstance(hard_cycle_cap, int):
        raise ValueError("hard cycle cap must be an integer")
    if hard_cycle_cap < 0:
        raise ValueError("hard cycle cap must be non-negative")
    hard_cycle_cap = min(hard_cycle_cap, MAX_SERVICE_CYCLE_SUBMISSIONS)
    current = min(state.cap, max(1, hard_cycle_cap))
    reason = _blocker(signals)
    if reason is not None:
        lowered = min(current, _lower_cap(current))
        if lowered == state.cap and state.successful_streak == 0 and state.last_reason == reason:
            return state
        return RateState(lowered, 0, now_iso, reason)

    if state.cap > hard_cycle_cap > 0:
        return RateState(hard_cycle_cap, state.successful_streak, now_iso, "configured_hard_cap")
    if signals.submitted > 0 and signals.verified == signals.submitted:
        next_streak = min(state.successful_streak + 1, _MAX_SUCCESSFUL_STREAK)
        next_cap = _next_cap(current, hard_cycle_cap)
        return RateState(next_cap, next_streak, now_iso, "healthy")
    if (
        signals.submitted == 0
        and signals.canary_verified
        and state.cap == 1
        and state.successful_streak == 0
        and hard_cycle_cap > 1
    ):
        return RateState(_next_cap(1, hard_cycle_cap), 1, now_iso, "canary_verified")
    return state
