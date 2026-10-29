"""Offline bounded response and no-replay checks for the File/Link mutation."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path

import httpx
import pytest
from test_daemon import (
    T0,
    T1,
    WIDE_LIMITS,
    ShokoStub,
    Sleeper,
    SpecFile,
    activate_mapping,
    build_deps,
    mapping_xml,
    open_tmp_store,
    seed_baseline,
)
from test_linking import linkable_plan

from shoko_linker.daemon import CycleMode, run_cycle
from shoko_linker.http import ApiEndpoint
from shoko_linker.linking import MAX_LINK_PROBLEM_BYTES, SendOutcome, submit_link
from shoko_linker.policies import RunPolicy
from shoko_linker.state import StateStore


class MutationBody(httpx.SyncByteStream):
    def __init__(self, chunks: list[bytes], failure: Exception | None = None) -> None:
        self.chunks = chunks
        self.failure = failure
        self.closed = False
        self.reads = 0

    def __iter__(self) -> Iterator[bytes]:
        for chunk in self.chunks:
            self.reads += 1
            yield chunk
        if self.failure is not None:
            raise self.failure

    def close(self) -> None:
        self.closed = True


@pytest.mark.parametrize(
    "failure",
    [
        "nested",
        "oversize",
        "compressed",
        "timeout",
        "disconnect",
        "malformed",
        "unicode",
        "contradictory_status",
        "malformed_errors",
        "malformed_title",
        "empty_problem",
    ],
)
def test_unusable_refusal_is_ambiguous_closed_and_never_replayed(failure: str) -> None:
    chunks = [b'{"title":"refused"}']
    error: Exception | None = None
    headers: dict[str, str] = {}
    if failure == "nested":
        chunks = [b"[" * 10000 + b"0" + b"]" * 10000]
    elif failure == "oversize":
        chunks = [b"x" * MAX_LINK_PROBLEM_BYTES, b"x", b"never read"]
    elif failure == "compressed":
        headers["Content-Encoding"] = "gzip"
    elif failure in {"timeout", "disconnect"}:
        error = (
            httpx.ReadTimeout("private body")
            if failure == "timeout"
            else httpx.ReadError("private body")
        )
    elif failure == "unicode":
        chunks = [b"\xff"]
    elif failure == "contradictory_status":
        chunks = [b'{"title":"refused","status":200}']
    elif failure == "malformed_errors":
        chunks = [b'{"title":"refused","errors":{"EpisodeIDs":[true]}}']
    elif failure == "malformed_title":
        chunks = [b'{"title":[],"detail":"refused"}']
    elif failure == "empty_problem":
        chunks = [b"{}"]
    else:
        chunks = [b"{broken"]
    body = MutationBody(chunks, error)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(400, stream=body, headers=headers)

    with httpx.Client(base_url="http://shoko/", transport=httpx.MockTransport(handler)) as client:
        result = submit_link(ApiEndpoint(client, lambda: {}), linkable_plan())
    assert result.outcome is SendOutcome.AMBIGUOUS_SEND
    assert result.http_status == 400
    assert result.retry_forbidden
    assert "private body" not in result.detail
    assert len(requests) == 1
    assert requests[0].method == "POST"
    assert requests[0].headers["Accept-Encoding"] == "identity"
    assert body.closed
    if failure == "compressed":
        assert body.reads == 0
    if failure == "oversize":
        assert body.reads == 2


@pytest.mark.parametrize("status", [200, 202, 204, 503, 429])
def test_status_classification_closes_unneeded_body_without_reading(status: int) -> None:
    body = MutationBody([b"x" * (MAX_LINK_PROBLEM_BYTES + 1)], httpx.ReadTimeout("unused"))
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(status, stream=body, headers={"Content-Encoding": "gzip"})

    with httpx.Client(base_url="http://shoko/", transport=httpx.MockTransport(handler)) as client:
        result = submit_link(ApiEndpoint(client, lambda: {}), linkable_plan())
    expected = SendOutcome.ACCEPTED_UNVERIFIED if status < 300 else SendOutcome.AMBIGUOUS_SEND
    assert result.outcome is expected
    assert result.http_status == status
    assert result.retry_forbidden
    assert len(requests) == 1
    assert body.closed
    assert body.reads == 0


def test_existing_io_deadline_bounds_mutation_read_without_relaxing_timeouts() -> None:
    now = 10.0
    requests: list[httpx.Request] = []

    class SlowBody(MutationBody):
        def __iter__(self) -> Iterator[bytes]:
            nonlocal now
            now = 10.6
            yield b'{"title":"refused"}'

    body = SlowBody([])

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(400, stream=body)

    with httpx.Client(
        base_url="http://shoko/",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(connect=0.1, read=2, write=2, pool=None),
    ) as client:
        result = submit_link(
            ApiEndpoint(client, lambda: {}), linkable_plan(), deadline=10.5, clock=lambda: now
        )
    assert result.outcome is SendOutcome.AMBIGUOUS_SEND
    assert result.http_status == 400
    assert requests[0].extensions["timeout"] == pytest.approx(
        {"connect": 0.1, "read": 0.5, "write": 0.5, "pool": 0.5}
    )
    assert len(requests) == 1
    assert body.closed


def test_valid_refusal_remains_rejected_with_one_bounded_read() -> None:
    body = MutationBody([b'{"title":"Validation problem","errors":{"EpisodeIDs":["missing"]}}'])
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(400, stream=body)

    with httpx.Client(base_url="http://shoko/", transport=httpx.MockTransport(handler)) as client:
        result = submit_link(ApiEndpoint(client, lambda: {}), linkable_plan())
    assert result.outcome is SendOutcome.REJECTED
    assert "EpisodeIDs: missing" in result.detail
    assert body.closed
    assert len(requests) == 1


@pytest.mark.parametrize("service_mode", [False, True])
def test_unusable_refusal_holds_durable_slot_and_never_replays_after_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, service_mode: bool
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    candidate = SpecFile(series_id=1, file_id=10, shoko_id=500)
    activate_mapping(tmp_path, mapping_xml())
    store = open_tmp_store(tmp_path)
    seed_baseline(store, [SpecFile(series_id=99, file_id=990, shoko_id=999)])
    body = MutationBody([b"[" * 10000 + b"0" + b"]" * 10000])

    class MalformedRefusal(ShokoStub):
        def _post(self, request: httpx.Request, path: str) -> httpx.Response:
            self.posts.append({})
            return httpx.Response(400, stream=body)

    shoko = MalformedRefusal([candidate])
    policy = RunPolicy.SERVICE if service_mode else RunPolicy.NEW_ARRIVAL
    for index in range(2):
        try:
            deps = build_deps(
                tmp_path,
                store=store,
                sonarr_files=[candidate],
                shoko_files=[candidate],
                xml=mapping_xml(),
                monotonic=lambda: 0.0,
                advance=Sleeper().advance,
                shoko=shoko,
            )
            deps = replace(
                deps,
                service_mode=service_mode,
                service_authorized=lambda: True,
                max_verify_polls=1,
            )
            run_cycle(
                deps,
                now_iso=T0 if index == 0 else T1,
                mode=CycleMode.APPLY,
                policy=policy,
                cycle_key=f"malformed-refusal-{index}",
            )
            view = store.budget_view()
            assert view.pending_file_requests == 1
            assert view.pending_episode_jobs == 1
            assert shoko.posts_count() == 1
        finally:
            store.close()
        if index == 0:
            store = StateStore(tmp_path / "state.sqlite3", budget_limits=WIDE_LIMITS)
    assert body.closed
