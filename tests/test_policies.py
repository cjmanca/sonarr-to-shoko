"""Run policy representation and validation. No engine behavior is implemented here."""

from __future__ import annotations

import pytest

from shoko_linker.errors import PolicyError
from shoko_linker.policies import PolicyRequest, RunPolicy, parse_policy

POLICY_COUNT = 4


def test_policies_are_distinct() -> None:
    assert {
        RunPolicy.BACKLOG,
        RunPolicy.NEW_ARRIVAL,
        RunPolicy.APPROVED_CAMPAIGN,
        RunPolicy.SERVICE,
    } == set(RunPolicy)
    assert len({policy.value for policy in RunPolicy}) == POLICY_COUNT


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("backlog", RunPolicy.BACKLOG),
        ("  New_Arrival ", RunPolicy.NEW_ARRIVAL),
        ("approved_campaign", RunPolicy.APPROVED_CAMPAIGN),
        ("service", RunPolicy.SERVICE),
    ],
)
def test_parse_policy_accepts_known_names(raw: str, expected: RunPolicy) -> None:
    assert parse_policy(raw) is expected


def test_parse_policy_refuses_unknown_names() -> None:
    with pytest.raises(PolicyError, match="approved_campaign"):
        parse_policy("guess-what-i-mean")


def test_policy_labels_are_human_readable() -> None:
    assert RunPolicy.APPROVED_CAMPAIGN.label == "approved campaign"


def test_backlog_and_new_arrival_requests_are_consistent() -> None:
    assert PolicyRequest(RunPolicy.BACKLOG, include_preexisting_backlog=True).validated()
    assert PolicyRequest(RunPolicy.NEW_ARRIVAL).validated().policy is RunPolicy.NEW_ARRIVAL


def test_approved_campaign_refuses_silent_backlog_expansion() -> None:
    request = PolicyRequest(RunPolicy.APPROVED_CAMPAIGN, include_preexisting_backlog=True)
    with pytest.raises(PolicyError, match="must not include the preexisting backlog"):
        request.validated()
