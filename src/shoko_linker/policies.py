"""Run policies as data from the start: backlog, new_arrival, approved_campaign.

This module is representation and validation only. No scheduling, no selection logic,
and no linking lives here — later milestones consume these values.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .errors import PolicyError


class RunPolicy(Enum):
    """The three distinct ways this service is asked to run."""

    BACKLOG = "backlog"
    NEW_ARRIVAL = "new_arrival"
    APPROVED_CAMPAIGN = "approved_campaign"

    @property
    def label(self) -> str:
        """Human-readable name for CLI output."""
        return self.value.replace("_", " ")


def parse_policy(raw: str) -> RunPolicy:
    """Parse a policy name, refusing unknown values with the accepted list."""
    try:
        return RunPolicy(raw.strip().lower())
    except ValueError:
        allowed = ", ".join(policy.value for policy in RunPolicy)
        raise PolicyError(f"unknown run policy {raw!r}; expected one of: {allowed}") from None


@dataclass(frozen=True)
class PolicyRequest:
    """A requested run policy plus the one scope question the policies differ on."""

    policy: RunPolicy
    include_preexisting_backlog: bool = False

    def validated(self) -> PolicyRequest:
        """Return self when the request is consistent with project policy.

        Raises:
            PolicyError: an approved campaign must never silently absorb the
                preexisting backlog (PROJECT.md: new mappings do not add backlog files
                to a previously approved campaign).
        """
        if self.policy is RunPolicy.APPROVED_CAMPAIGN and self.include_preexisting_backlog:
            raise PolicyError(
                "approved_campaign must not include the preexisting backlog; "
                "backlog files need their own explicit, reviewed run"
            )
        return self
