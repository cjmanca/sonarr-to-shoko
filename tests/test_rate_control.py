"""M4.2 adaptive submission-rate decisions and durable state encoding."""

from __future__ import annotations

from dataclasses import replace
from typing import Any, cast

import pytest

from shoko_linker.rate_control import (
    RateSignals,
    RateState,
    advance_rate,
    decode_rate_state,
    effective_submission_cap,
    encode_rate_state,
    service_budget_limits,
)
from shoko_linker.state import BudgetLimits

T0 = "2026-10-15T00:00:00+00:00"
T1 = "2026-10-15T00:05:00+00:00"


def test_rate_ramps_cautiously_through_deterministic_steps() -> None:
    state = RateState.initial(T0)
    caps: list[int] = []
    for second in range(1, 7):
        state = advance_rate(
            state,
            RateSignals(
                queue_healthy=True,
                pending_episode_jobs=0,
                max_pending_episode_jobs=10,
                submitted=state.cap,
                verified=state.cap,
                api_healthy=True,
                mapping_stable=True,
                authorization_valid=True,
                anomaly_free=True,
                canary_verified=True,
            ),
            now_iso=f"2026-10-15T00:0{second}:00+00:00",
            hard_cycle_cap=20,
        )
        caps.append(state.cap)
    assert caps == [2, 4, 8, 16, 20, 20]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("queue_healthy", False),
        ("pending_episode_jobs", 1),
        ("pending_episode_jobs", 10),
        ("api_healthy", False),
        ("mapping_stable", False),
        ("authorization_valid", False),
        ("anomaly_free", False),
        ("verified", 0),
    ],
)
def test_unhealthy_signal_reduces_cap_and_resets_success_streak(
    field: str, value: bool | int
) -> None:
    state = RateState(cap=16, successful_streak=4, updated_iso=T0, last_reason="healthy")
    signals = RateSignals(
        queue_healthy=True,
        pending_episode_jobs=0,
        max_pending_episode_jobs=10,
        submitted=2,
        verified=2,
        api_healthy=True,
        mapping_stable=True,
        authorization_valid=True,
        anomaly_free=True,
        canary_verified=True,
    )
    next_state = advance_rate(
        state,
        replace(signals, **cast(Any, {field: value})),
        now_iso=T1,
        hard_cycle_cap=20,
    )
    assert next_state.cap == 8
    assert next_state.successful_streak == 0
    assert next_state.updated_iso == T1


def test_canary_completion_after_restart_starts_ramp_without_a_new_send() -> None:
    state = RateState.initial(T0)
    next_state = advance_rate(
        state,
        RateSignals(
            queue_healthy=True,
            pending_episode_jobs=0,
            max_pending_episode_jobs=10,
            submitted=0,
            verified=0,
            api_healthy=True,
            mapping_stable=True,
            authorization_valid=True,
            anomaly_free=True,
            canary_verified=True,
        ),
        now_iso=T1,
        hard_cycle_cap=20,
    )
    assert (next_state.cap, next_state.successful_streak) == (2, 1)


def test_configured_cycle_ceiling_can_be_a_non_ladder_value() -> None:
    state = RateState(cap=2, successful_streak=1, updated_iso=T0, last_reason="healthy")
    next_state = advance_rate(
        state,
        RateSignals(
            queue_healthy=True,
            pending_episode_jobs=0,
            max_pending_episode_jobs=10,
            submitted=2,
            verified=2,
            api_healthy=True,
            mapping_stable=True,
            authorization_valid=True,
            anomaly_free=True,
            canary_verified=True,
        ),
        now_iso=T1,
        hard_cycle_cap=3,
    )
    assert next_state.cap == 3


def test_caps_remain_below_configured_and_absolute_cycle_limits() -> None:
    state = RateState(cap=20, successful_streak=6, updated_iso=T0, last_reason="healthy")
    assert effective_submission_cap(state, configured_limit=7) == 7
    assert effective_submission_cap(state, configured_limit=100) == 20
    assert effective_submission_cap(state, configured_limit=0) == 0


def test_service_budget_limits_honor_stricter_config_and_clamp_looser_values() -> None:
    hard = service_budget_limits(
        BudgetLimits(
            max_pending_files=100,
            max_pending_episode_jobs=100,
            per_cycle_new=100,
            rolling_new=100_000,
            rolling_window_hours=240.0,
        )
    )
    assert hard == BudgetLimits(20, 10, 20, 5000, 168.0)
    stricter = service_budget_limits(BudgetLimits(4, 3, 2, 100, 24.0))
    assert stricter == BudgetLimits(4, 3, 2, 100, 24.0)


def test_rate_state_round_trips_and_rejects_malformed_documents() -> None:
    state = RateState(cap=8, successful_streak=3, updated_iso=T1, last_reason="healthy")
    assert decode_rate_state(encode_rate_state(state)) == state
    with pytest.raises(ValueError, match="unsupported"):
        decode_rate_state('{"cap": 20}')
