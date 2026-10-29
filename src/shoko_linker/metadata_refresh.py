"""One admitted Shoko metadata request, never retried or followed through redirects.

The caller MUST first authorize native same-anime maintenance, verify a complete
file demand and fresh queue/safety observations, and durably admit this transport
attempt. This primitive does not provide those gates or perform persistence.
Shoko may repair associations and run media maintenance/hooks for the same anime.
No caller may treat queue acceptance as verified metadata readiness.

Contract: Shoko Server 5.3.3, commit 892461188e5f1cc3ccf1b9b6eb1d4dfd31bea6f8,
API/v3/Controllers/SeriesController.cs, RefreshAnime (immediate=false).
"""

from __future__ import annotations

import json
import math
import time
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass
from enum import Enum
from http import HTTPStatus
from typing import Final

import httpx

from .errors import HttpPolicyError
from .http import ApiEndpoint, resolve_and_send

_MAX_ID: Final[int] = 2_147_483_647
_MAX_BODY_BYTES: Final[int] = 64


class MetadataRefreshOutcome(Enum):
    NOT_SENT = "not_sent"
    ACCEPTED_QUEUED = "accepted_queued"
    UNKNOWN = "unknown"


class MetadataRefreshReason(Enum):
    DEADLINE = "deadline"
    QUEUED = "queued"
    HTTP_RESPONSE = "http_response"
    INVALID_RESPONSE = "invalid_response"
    TRANSPORT = "transport"


@dataclass(frozen=True)
class MetadataRefreshResult:
    outcome: MetadataRefreshOutcome
    reason: MetadataRefreshReason
    http_status: int | None = None


def submit_metadata_refresh(
    endpoint: ApiEndpoint,
    anime_id: int,
    *,
    deadline: float,
    clock: Callable[[], float] = time.monotonic,
) -> MetadataRefreshResult:
    """Issue at most one POST after external durable transport admission.

    ``deadline`` is absolute monotonic time. Only a deadline exhausted before
    transport proves NOT_SENT. Once transport starts, every failure is UNKNOWN,
    including 4xx: this endpoint has no assumed zero-write rejection guarantee.
    UNKNOWN must retain durable delivery uncertainty and must never cause replay.
    JSON false is Shoko's queued result; JSON true is unexpected with immediate
    disabled and does not prove queue acceptance. No raw remote text is returned.

    Reads check the deadline between bounded raw chunks; a blocking read remains
    subject to the smaller configured HTTP timeout and remaining cycle time.
    """
    if type(anime_id) is not int or not 0 < anime_id <= _MAX_ID:
        raise ValueError("anime ID must be a positive signed 32-bit integer")
    if not math.isfinite(deadline):
        raise ValueError("metadata refresh deadline must be finite")
    remaining = deadline - clock()
    if remaining <= 0:
        return MetadataRefreshResult(
            MetadataRefreshOutcome.NOT_SENT, MetadataRefreshReason.DEADLINE
        )
    timeout = httpx.Timeout(
        **{
            key: remaining if value is None else min(value, remaining)
            for key, value in endpoint.client.timeout.as_dict().items()
        }
    )
    identity_endpoint = ApiEndpoint(
        endpoint.client, lambda: {**endpoint.headers_factory(), "Accept-Encoding": "identity"}
    )
    status: int | None = None
    reason = MetadataRefreshReason.TRANSPORT
    try:
        with closing(
            resolve_and_send(
                identity_endpoint,
                "POST",
                f"api/v3/Series/AniDB/{anime_id}/Refresh",
                params={
                    "force": "false",
                    "downloadRelations": "false",
                    "createSeriesEntry": "true",
                    "immediate": "false",
                    "cacheOnly": "false",
                    "skipTmdbUpdate": "true",
                },
                max_redirects=0,
                stream=True,
                timeout=timeout,
            )
        ) as response:
            status = response.status_code
            if clock() >= deadline:
                reason = MetadataRefreshReason.DEADLINE
            elif status != HTTPStatus.OK:
                reason = MetadataRefreshReason.HTTP_RESPONSE
            elif response.headers.get("content-encoding", "identity").lower() != "identity":
                reason = MetadataRefreshReason.INVALID_RESPONSE
            else:
                return _read_result(response, deadline, clock)
    except (httpx.HTTPError, httpx.StreamError, HttpPolicyError):
        pass
    return MetadataRefreshResult(MetadataRefreshOutcome.UNKNOWN, reason, status)


def _read_result(
    response: httpx.Response, deadline: float, clock: Callable[[], float]
) -> MetadataRefreshResult:
    body = bytearray()
    reason = MetadataRefreshReason.INVALID_RESPONSE
    # iter_raw refuses decompression; the identity header was checked first.
    for chunk in response.iter_raw():
        if clock() >= deadline:
            reason = MetadataRefreshReason.DEADLINE
            break
        if len(body) + len(chunk) > _MAX_BODY_BYTES:
            break
        body.extend(chunk)
    else:
        if clock() >= deadline:
            reason = MetadataRefreshReason.DEADLINE
        else:
            try:
                payload = json.loads(body)
            except (ValueError, RecursionError):
                pass
            else:
                if payload is False:
                    return MetadataRefreshResult(
                        MetadataRefreshOutcome.ACCEPTED_QUEUED,
                        MetadataRefreshReason.QUEUED,
                        response.status_code,
                    )
    return MetadataRefreshResult(MetadataRefreshOutcome.UNKNOWN, reason, response.status_code)
