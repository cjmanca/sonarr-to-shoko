"""Setup, optional safety settings and health probes share bounded JSON reads."""

from __future__ import annotations

from collections.abc import Iterator
from functools import partial
from typing import Literal

import httpx
import pytest

from shoko_linker import safety_check, service, setup
from shoko_linker.http import ApiEndpoint, get_bounded_json

_LIMIT = 32768
_SECRET = "private-settings-must-not-escape"
type Consumer = Literal["scope", "safety", "health"]
type Failure = Literal["nested", "oversize", "encoding", "forbidden", "timeout", "malformed"]


class _ResponseStream(httpx.SyncByteStream):
    def __init__(self, failure: Failure) -> None:
        self.failure = failure
        self.closed = False
        self.reads = 0

    def __iter__(self) -> Iterator[bytes]:
        self.reads += 1
        if self.failure == "timeout":
            raise httpx.ReadTimeout(_SECRET)
        if self.failure in {"forbidden", "encoding"}:
            pytest.fail("rejected response body must not be consumed")
        if self.failure == "nested":
            yield b"[" * 10000 + b"0" + b"]" * 10000
        elif self.failure == "oversize":
            yield b" " * (_LIMIT + 1)
            pytest.fail("oversized response must not be read further")
        else:
            yield _SECRET.encode()

    def close(self) -> None:
        self.closed = True


@pytest.mark.parametrize("consumer", ["scope", "safety", "health"])
@pytest.mark.parametrize(
    "failure", ["nested", "oversize", "encoding", "forbidden", "timeout", "malformed"]
)
def test_setup_reads_reject_unsafe_json_and_close_streams(
    monkeypatch: pytest.MonkeyPatch, consumer: Consumer, failure: Failure
) -> None:
    module = {"scope": setup, "safety": safety_check, "health": service}[consumer]
    monkeypatch.setattr(module, "get_bounded_json", partial(get_bounded_json, max_bytes=_LIMIT))
    streams: list[_ResponseStream] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.headers["Accept-Encoding"] == "identity"
        stream = _ResponseStream(failure)
        streams.append(stream)
        return httpx.Response(
            403 if failure == "forbidden" else 200,
            headers={"Content-Encoding": "gzip"} if failure == "encoding" else {},
            stream=stream,
            request=request,
        )

    with httpx.Client(base_url="http://test/", transport=httpx.MockTransport(handler)) as client:
        endpoint = ApiEndpoint(client, lambda: {})
        result: object
        if consumer == "scope":
            scope = setup.discover_sonarr_scope(endpoint)
            result = scope
            assert scope.status is setup.ScopeStatus.BLOCKED
            expected = (
                setup.ScopeReadStatus.UNAVAILABLE
                if failure in {"forbidden", "timeout"}
                else setup.ScopeReadStatus.INCOMPLETE
            )
            assert scope.read_status is expected
            assert scope.candidates == ()
        elif consumer == "safety":
            assessment = safety_check.inspect_shoko_safety_settings(endpoint)
            result = assessment
            assert assessment.status is safety_check.SafetyStatus.UNKNOWN
            assert assessment.api_available is False
            assert set(assessment.unknown_settings) == set(safety_check.SAFETY_SETTING_PATHS)
            assert all(value is None for value in assessment.observed_settings.values())
        else:
            report = service.probe_connections(endpoint, endpoint)
            result = report
            assert not report.ready
            assert not report.sonarr_available
            assert not report.shoko_available
            expected_text = (
                "Cannot authenticate"
                if failure == "forbidden"
                else "Cannot connect"
                if failure == "timeout"
                else "unsupported response"
            )
            assert all(expected_text in message for message in report.messages)
        assert _SECRET not in repr(result)
        assert streams
        assert all(stream.closed for stream in streams)
        if failure in {"forbidden", "encoding"}:
            assert all(stream.reads == 0 for stream in streams)
