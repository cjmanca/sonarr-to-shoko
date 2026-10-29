"""Offline single-attempt metadata transport contract; no live Shoko writes."""

from __future__ import annotations

import itertools
from collections.abc import Callable, Iterator

import httpx
import pytest

from shoko_linker.http import ApiEndpoint
from shoko_linker.metadata_refresh import (
    MetadataRefreshOutcome as Outcome,
)
from shoko_linker.metadata_refresh import (
    MetadataRefreshReason as Reason,
)
from shoko_linker.metadata_refresh import (
    submit_metadata_refresh,
)


def _response(
    status: int = 200, body: bytes = b"false", *, encoding: str = "identity", **headers: str
) -> httpx.Response:
    headers["content-encoding"] = encoding
    return httpx.Response(status, stream=httpx.ByteStream(body), headers=headers)


def _run(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    deadline: float = 100,
    clock: Callable[[], float] = lambda: 0,
) -> tuple[Outcome, Reason, int | None, list[httpx.Request]]:
    requests: list[httpx.Request] = []

    def record(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return handler(request)

    with httpx.Client(
        base_url="https://shoko.invalid/base/",
        transport=httpx.MockTransport(record),
        timeout=httpx.Timeout(30, connect=2, pool=1, write=None),
    ) as client:
        result = submit_metadata_refresh(
            ApiEndpoint(client, lambda: {"apikey": "synthetic-test-key"}),
            18208,
            deadline=deadline,
            clock=clock,
        )
    return result.outcome, result.reason, result.http_status, requests


def test_false_is_queued_with_exact_narrow_request() -> None:
    outcome, reason, status, requests = _run(lambda _: _response(), deadline=5)
    assert (outcome, reason, status) == (Outcome.ACCEPTED_QUEUED, Reason.QUEUED, 200)
    assert len(requests) == 1
    request = requests[0]
    assert request.method == "POST"
    assert request.url.path == "/base/api/v3/Series/AniDB/18208/Refresh"
    assert dict(request.url.params) == {
        "force": "false",
        "downloadRelations": "false",
        "createSeriesEntry": "true",
        "immediate": "false",
        "cacheOnly": "false",
        "skipTmdbUpdate": "true",
    }
    assert request.headers["apikey"] == "synthetic-test-key"
    assert request.headers["accept-encoding"] == "identity"
    assert request.content == b""
    assert "content-type" not in request.headers
    assert request.extensions["timeout"] == {"connect": 2, "read": 5, "write": 5, "pool": 1}


@pytest.mark.parametrize("body", [b"true", b"null", b"0", b"{}", b'"false"', b"", b"false x"])
def test_unexpected_schema_is_unknown_and_not_retried(body: bytes) -> None:
    outcome, reason, status, requests = _run(lambda _: _response(body=body))
    assert (outcome, reason, status) == (Outcome.UNKNOWN, Reason.INVALID_RESPONSE, 200)
    assert len(requests) == 1


@pytest.mark.parametrize("status", [201, 204, 400, 401, 403, 404, 409, 429, 500, 502, 503])
def test_error_status_is_not_assumed_zero_delivery(status: int) -> None:
    outcome, reason, observed, requests = _run(lambda _: _response(status))
    assert (outcome, reason, observed) == (Outcome.UNKNOWN, Reason.HTTP_RESPONSE, status)
    assert len(requests) == 1


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
@pytest.mark.parametrize("location", ["/other", "https://foreign.invalid/capture"])
def test_redirect_is_never_followed(status: int, location: str) -> None:
    outcome, reason, _, requests = _run(lambda _: _response(status, location=location))
    assert (outcome, reason) == (Outcome.UNKNOWN, Reason.TRANSPORT)
    assert len(requests) == 1


@pytest.mark.parametrize(
    "error", [httpx.ReadTimeout, httpx.ConnectError, httpx.WriteError, httpx.RemoteProtocolError]
)
def test_transport_failure_never_retries(error: type[httpx.RequestError]) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise error("sensitive remote diagnostic must not propagate", request=request)

    outcome, reason, status, requests = _run(handler)
    assert (outcome, reason, status) == (Outcome.UNKNOWN, Reason.TRANSPORT, None)
    assert len(requests) == 1


def test_exhausted_deadline_is_proven_unsent() -> None:
    outcome, reason, status, requests = _run(lambda _: _response(), deadline=0)
    assert (outcome, reason, status, requests) == (Outcome.NOT_SENT, Reason.DEADLINE, None, [])


def test_deadline_after_send_remains_unknown() -> None:
    times = iter([0, 100])
    outcome, reason, status, requests = _run(lambda _: _response(), clock=lambda: next(times))
    assert (outcome, reason, status) == (Outcome.UNKNOWN, Reason.DEADLINE, 200)
    assert len(requests) == 1


@pytest.mark.parametrize("encoding", ["gzip", "deflate", "br"])
def test_compression_is_refused_before_body_read(encoding: str) -> None:
    outcome, reason, _, requests = _run(lambda _: _response(body=b"false", encoding=encoding))
    assert (outcome, reason) == (Outcome.UNKNOWN, Reason.INVALID_RESPONSE)
    assert len(requests) == 1


def test_oversized_body_is_unknown() -> None:
    outcome, reason, _, _ = _run(lambda _: _response(body=b" " * 65 + b"false"))
    assert (outcome, reason) == (Outcome.UNKNOWN, Reason.INVALID_RESPONSE)


class _Chunks(httpx.SyncByteStream):
    closed = False

    def __iter__(self) -> Iterator[bytes]:
        yield from itertools.repeat(b" ", 1000)

    def close(self) -> None:
        self.closed = True


def test_stream_is_closed_after_bound() -> None:
    stream = _Chunks()
    outcome, reason, _, _ = _run(lambda _: httpx.Response(200, stream=stream))
    assert (outcome, reason) == (Outcome.UNKNOWN, Reason.INVALID_RESPONSE)
    assert stream.closed


def test_deadline_between_chunks_closes_stream() -> None:
    stream = _Chunks()
    times = iter([0, 0, 0, 100])
    outcome, reason, status, _ = _run(
        lambda _: httpx.Response(200, stream=stream), clock=lambda: next(times)
    )
    assert (outcome, reason, status) == (Outcome.UNKNOWN, Reason.DEADLINE, 200)
    assert stream.closed


def test_stream_failure_retains_uncertainty_and_closes() -> None:
    class Broken(_Chunks):
        def __iter__(self) -> Iterator[bytes]:
            yield b"fal"
            raise httpx.ReadTimeout("synthetic private diagnostic")

    stream = Broken()
    outcome, reason, status, requests = _run(lambda _: httpx.Response(200, stream=stream))
    assert (outcome, reason, status) == (Outcome.UNKNOWN, Reason.TRANSPORT, 200)
    assert stream.closed
    assert len(requests) == 1


@pytest.mark.parametrize("deadline", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_deadline_rejected_without_send(deadline: float) -> None:
    with pytest.raises(ValueError, match="deadline"):
        _run(lambda _: pytest.fail("unexpected POST"), deadline=deadline)


@pytest.mark.parametrize("anime_id", [0, -1, 2147483648, True])
def test_invalid_id_never_sends(anime_id: int) -> None:
    with (
        httpx.Client(transport=httpx.MockTransport(lambda _: pytest.fail("unexpected POST"))) as c,
        pytest.raises(ValueError, match="anime ID"),
    ):
        submit_metadata_refresh(ApiEndpoint(c, lambda: {}), anime_id, deadline=100)
