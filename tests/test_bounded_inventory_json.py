"""Offline resource/failure regression tests for shared inventory JSON reads."""

from __future__ import annotations

import json
from collections.abc import Iterator
from functools import partial
from typing import Any

import httpx
import pytest

from shoko_linker import audit, daemon, linking
from shoko_linker.http import ApiEndpoint, BoundedJsonError, get_bounded_json
from shoko_linker.shoko import collect_import_folders
from shoko_linker.sonarr import VerdictReason, _fetch_json_list


@pytest.mark.parametrize("consumer", ["linking", "audit", "verification"])
@pytest.mark.parametrize("failure", ["nested", "oversize", "encoding", "read"])
def test_link_boundaries_fail_closed_and_close_stream(
    consumer: str, failure: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = b"[" * 10000 + b"0" + b"]" * 10000 if failure == "nested" else b"[" * 20
    body = RawBody([payload], fail=failure == "read")
    headers = {"Content-Encoding": "gzip"} if failure == "encoding" else None
    endpoint = endpoint_for(body, headers=headers)
    module = {"linking": linking, "audit": audit, "verification": daemon}[consumer]
    if failure == "oversize":
        monkeypatch.setattr(module, "get_bounded_json", partial(get_bounded_json, max_bytes=10))
    with endpoint.client:
        if consumer == "linking":
            result = linking._get(endpoint, "inventory")
            assert result.payload is None
            assert result.status == (None if failure == "read" else 200)
            assert "secret" not in result.detail
        elif consumer == "audit":
            assert audit._get(endpoint, "inventory") is None
        else:
            assert daemon._xref_fetcher(endpoint, 1)() is None
    assert body.closed


@pytest.mark.parametrize("status", [403, 404])
def test_resolver_preserves_forbidden_versus_missing_cache(status: int) -> None:
    body = RawBody([b"secret refusal"])
    endpoint = endpoint_for(body, status=status)
    with endpoint.client:
        result = linking.EpisodeIdResolver(endpoint).resolve(18208, "Episode", 1)
    expected = (
        linking.EpisodeResolutionStatus.ANIME_NOT_CACHED
        if status == 404
        else linking.EpisodeResolutionStatus.TRANSPORT_FAILURE
    )
    assert result.status is expected
    assert body.closed
    assert body.yielded == 0


def test_resolver_deadline_bounds_redirect_and_body_without_second_hop() -> None:
    now = 10.0
    requests: list[httpx.Request] = []
    bodies: list[RawBody] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal now
        requests.append(request)
        body = RawBody([b"[]"])
        bodies.append(body)
        now += 0.3
        if len(requests) == 1:
            return httpx.Response(307, stream=body, headers={"Location": "/listing"})
        return httpx.Response(200, stream=body)

    client = httpx.Client(
        base_url="http://source/",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(connect=0.1, read=2, write=2, pool=None),
    )
    endpoint = ApiEndpoint(client, lambda: {})
    resolver = linking.EpisodeIdResolver(endpoint, deadline=10.5, clock=lambda: now)
    with client:
        result = resolver.resolve(18208, "Episode", 1)
        assert resolver.resolve(18208, "Episode", 1).status is result.status
    assert result.status is linking.EpisodeResolutionStatus.TRANSPORT_FAILURE
    assert "deadline" in result.detail
    assert len(requests) == 2
    for request, remaining in zip(requests, [0.5, 0.2], strict=True):
        assert request.extensions["timeout"] == pytest.approx(
            {"connect": 0.1, "read": remaining, "write": remaining, "pool": remaining}
        )
    assert all(body.closed for body in bodies)


class RawBody(httpx.SyncByteStream):
    def __init__(self, chunks: list[bytes], *, fail: bool = False) -> None:
        self.chunks = chunks
        self.fail = fail
        self.yielded = 0
        self.closed = False

    def __iter__(self) -> Iterator[bytes]:
        for chunk in self.chunks:
            self.yielded += 1
            yield chunk
        if self.fail:
            raise httpx.ReadError("secret response detail")

    def close(self) -> None:
        self.closed = True


def endpoint_for(
    body: RawBody, *, status: int = 200, headers: dict[str, str] | None = None
) -> ApiEndpoint:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.headers["accept-encoding"] == "identity"
        assert request.headers["apikey"] == "test-secret"
        return httpx.Response(status, stream=body, headers=headers)

    return ApiEndpoint(
        httpx.Client(base_url="http://source/", transport=httpx.MockTransport(handler)),
        lambda: {"apikey": "test-secret"},
    )


def test_size_limit_stops_raw_stream_before_exhaustion() -> None:
    body = RawBody([b"[", b"0," * 5, b"0]"])
    endpoint = endpoint_for(body)
    with endpoint.client, pytest.raises(BoundedJsonError, match="size limit"):
        get_bounded_json(endpoint, "inventory", max_bytes=10)
    assert body.yielded == 2
    assert body.closed


@pytest.mark.parametrize("limit", [1, 2, 3])
def test_exact_size_boundary(limit: int) -> None:
    body = RawBody([b"[", b"]"])
    endpoint = endpoint_for(body)
    with endpoint.client:
        if limit < 2:
            with pytest.raises(BoundedJsonError, match="size limit"):
                get_bounded_json(endpoint, "inventory", max_bytes=limit)
        else:
            assert get_bounded_json(endpoint, "inventory", max_bytes=limit) == (200, [])
    assert body.closed


@pytest.mark.parametrize("encoding", ["gzip", "br", "identity, gzip"])
def test_unsupported_encoding_never_reads_or_decompresses(encoding: str) -> None:
    body = RawBody([b"invalid compressed data"])
    endpoint = endpoint_for(body, headers={"Content-Encoding": encoding})
    with endpoint.client, pytest.raises(BoundedJsonError, match="encoding"):
        get_bounded_json(endpoint, "inventory")
    assert body.yielded == 0
    assert body.closed


@pytest.mark.parametrize("payload", [b"[" * 10000 + b"0" + b"]" * 10000, b"not json"])
def test_bad_json_is_sanitized_and_closed(payload: bytes) -> None:
    body = RawBody([payload])
    endpoint = endpoint_for(body)
    with endpoint.client, pytest.raises(BoundedJsonError, match="^invalid JSON response$"):
        get_bounded_json(endpoint, "inventory")
    assert body.closed


@pytest.mark.parametrize("consumer", ["sonarr", "shoko"])
@pytest.mark.parametrize("failure", ["nested", "encoding", "read"])
def test_collectors_fail_closed_without_exposing_response(consumer: str, failure: str) -> None:
    payload = b"[" * 10000 + b"0" + b"]" * 10000 if failure == "nested" else b"["
    body = RawBody([payload], fail=failure == "read")
    headers = {"Content-Encoding": "gzip"} if failure == "encoding" else None
    endpoint = endpoint_for(body, headers=headers)
    with endpoint.client:
        if consumer == "sonarr":
            result = _fetch_json_list(endpoint, "inventory")
            assert result.payload is None
            expected = (
                VerdictReason.TRANSPORT_FAILURE
                if failure == "read"
                else VerdictReason.MALFORMED_RESPONSE
            )
            assert result.reason is expected
            assert "secret" not in result.detail
        else:
            assert collect_import_folders(endpoint) is None
    assert body.closed


@pytest.mark.parametrize("status", [304, 401, 403, 500])
def test_failure_status_keeps_status_without_reading_body(status: int) -> None:
    body = RawBody([b"secret error body"])
    endpoint = endpoint_for(body, status=status)
    with endpoint.client:
        assert get_bounded_json(endpoint, "inventory") == (status, None)
    assert body.closed
    assert body.yielded == 0


def test_large_supported_library_response_remains_usable() -> None:
    # ~8 MiB, larger than the queue reader's 4 MiB default; realistic embedded
    # media detail plus identity/path data across the supported 7,000 episodes.
    records = [
        {"id": n, "path": f"/anime/Series/S01E{n}.mkv", "mediaInfo": {"audio": "a" * 1100}}
        for n in range(1, 7001)
    ]
    payload = json.dumps(records).encode()
    assert len(payload) > 4 * 1024 * 1024
    body = RawBody([payload[i : i + 65536] for i in range(0, len(payload), 65536)])
    endpoint = endpoint_for(body)
    with endpoint.client:
        result = _fetch_json_list(endpoint, "inventory")
    assert result.reason is None
    assert result.payload == records
    assert body.closed


def test_cross_origin_redirect_is_rejected_before_requesting_target() -> None:
    body = RawBody([b"never read"])
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(307, headers={"Location": "http://foreign/"}, stream=body)

    client = httpx.Client(base_url="http://source/", transport=httpx.MockTransport(handler))
    endpoint = ApiEndpoint(client, lambda: {"apikey": "test-secret"})
    with client:
        result = _fetch_json_list(endpoint, "inventory")
    assert result.reason is VerdictReason.TRANSPORT_FAILURE
    assert calls == ["http://source/inventory"]
    assert body.closed
    assert body.yielded == 0


def test_deadline_narrows_all_timeouts_and_closes_on_expiry() -> None:
    body = RawBody([b"[", b"]"])
    observed: list[Any] = []
    times = iter([10.0, 10.0, 10.0, 10.0, 11.0])

    def handler(request: httpx.Request) -> httpx.Response:
        observed.append(request.extensions["timeout"])
        return httpx.Response(200, stream=body)

    client = httpx.Client(
        base_url="http://source/",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(connect=0.1, read=0.2, write=2.0, pool=None),
    )
    endpoint = ApiEndpoint(client, lambda: {})
    with client, pytest.raises(BoundedJsonError, match="deadline"):
        get_bounded_json(endpoint, "inventory", deadline=10.5, clock=lambda: next(times))
    assert observed == [{"connect": 0.1, "read": 0.2, "write": 0.5, "pool": 0.5}]
    assert body.closed
    assert body.yielded == 2


def test_expired_deadline_makes_no_request() -> None:
    body = RawBody([b"[]"])
    endpoint = endpoint_for(body)
    with endpoint.client, pytest.raises(BoundedJsonError, match="deadline"):
        get_bounded_json(endpoint, "inventory", deadline=1.0, clock=lambda: 1.0)
    assert body.yielded == 0


@pytest.mark.parametrize("expire", [False, True])
def test_redirect_hops_refresh_remaining_timeout_and_stop_at_expiry(expire: bool) -> None:
    bodies: list[RawBody] = []
    timeouts: list[Any] = []
    now = 10.0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal now
        timeouts.append(request.extensions["timeout"])
        now += 0.6
        body = RawBody([b"[]"])
        bodies.append(body)
        if len(timeouts) == 1 or expire:
            return httpx.Response(307, stream=body, headers={"Location": "/next"})
        return httpx.Response(200, stream=body)

    client = httpx.Client(
        base_url="http://source/",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(connect=0.1, read=2, write=2, pool=None),
    )
    endpoint = ApiEndpoint(client, lambda: {})
    with client:
        if expire:
            with pytest.raises(httpx.TimeoutException, match="deadline"):
                get_bounded_json(endpoint, "inventory", deadline=11, clock=lambda: now)
        else:
            # Two requests each fit their initial timeout but exceed the total
            # deadline by response arrival; the body check refuses that response.
            with pytest.raises(BoundedJsonError, match="deadline"):
                get_bounded_json(endpoint, "inventory", deadline=11, clock=lambda: now)
    assert len(timeouts) == 2
    assert timeouts[0] == {"connect": 0.1, "read": 1, "write": 1, "pool": 1}
    assert timeouts[1]["connect"] == 0.1
    for name in ("read", "write", "pool"):
        assert timeouts[1][name] == pytest.approx(0.4)
    assert all(body.closed for body in bodies)
    assert all(body.yielded == 0 for body in bodies)
