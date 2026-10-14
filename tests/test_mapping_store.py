"""Anime-Lists runtime download/refresh/activation store (M4-b).

All offline: the only I/O is an ``httpx.MockTransport`` endpoint bound to the
canonical source origin; no test performs network access and no test touches Sonarr
or Shoko. Datetimes are fake UTC values.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
import yaml  # type: ignore[import-untyped]

from shoko_linker.config import DEFAULT_MAPPING_SOURCE_URL, DEFAULT_REFRESH_HOURS, parse_settings
from shoko_linker.errors import ConfigError, HttpPolicyError
from shoko_linker.http import ApiEndpoint, make_timeout
from shoko_linker.mapping_store import (
    ACCEPT_ENCODING,
    MAX_BACKOFF_HOURS,
    MappingStore,
    RefreshOutcome,
    due,
    load_active,
    make_source_endpoint,
    next_refresh_due,
    parse_source_url,
    refresh,
    status,
)
from shoko_linker.mappings import (
    EpisodeType,
    ResolutionStatus,
    content_digest,
    load_overrides,
    resolve,
)

BASE_ORIGIN = "https://raw.githubusercontent.com"
SOURCE_PATH = "/Anime-Lists/anime-lists/master/anime-list-full.xml"
SOURCE_URL = DEFAULT_MAPPING_SOURCE_URL
ETAG_A = '"identity-aaa"'
ETAG_B = '"identity-bbb"'
HOURS = 168.0
NOW = datetime(2026, 10, 9, 12, 0, 0)
LATER = NOW + timedelta(hours=1)
SHA256_HEX_LENGTH = 64
SMALL_MAX_BYTES = 100
REVISION_ONE = 1
REVISION_TWO = 2
ONE_FAILURE = 1
TWO_FAILURES = 2
TWO_REQUESTS = 2
THREE_REQUESTS = 3
ONE_HOUR_SECONDS = 3600.0
TWO_HOURS_SECONDS = 7200.0
NO_ENTRIES = 0
FIRST_BACKOFF_HOURS = 1.0

SMALL_XML = b"<anime-list><anime anidbid='1' tvdbid='100' defaulttvdbseason='1'/></anime-list>"
EXTENDED_XML = (
    b"<anime-list>"
    b"<anime anidbid='1' tvdbid='100' defaulttvdbseason='1'/>"
    b"<anime anidbid='2' tvdbid='200' defaulttvdbseason='1'>"
    b"<mapping-list><mapping anidbseason='1' tvdbseason='1'>;5-12;</mapping></mapping-list>"
    b"</anime></anime-list>"
)
REORDERED_XML = (
    b"<anime-list><!-- formatting only -->"
    b"<anime defaulttvdbseason='1' tvdbid='100' anidbid='1'/></anime-list>"
)
MALFORMED_XML = b"<anime-list><anime anidbid="
BULKY_XML = (
    b"<anime-list>"
    + b"<anime anidbid='1' tvdbid='100' defaulttvdbseason='1'/>" * 20
    + b"</anime-list>"
)
OVERRIDE_ADD_GAP = (
    "add:\n  - {tvdb_series: 100, season: 1, episode: 2, anidb_id: 1, "
    "anidb_episode: 2, type: Episode}\n"
)
MALFORMED_OVERRIDES = "nonsense:\n  - 1\n"


class ScriptedSource:
    """MockTransport handler with a queued response script and a full request log."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self._script: list[Callable[[httpx.Request], httpx.Response]] = []

    def queue(self, response: Callable[[httpx.Request], httpx.Response]) -> None:
        self._script.append(response)

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if not self._script:
            raise AssertionError("test endpoint received a request with no scripted response")
        return self._script.pop(0)(request)


class IncrementalBody(httpx.SyncByteStream):
    """Generate response chunks only as the consumer requests them."""

    def __init__(
        self,
        chunks: Iterable[bytes],
        *,
        fail_after_chunks: int | None = None,
        interrupt_after_chunks: int | None = None,
    ) -> None:
        self.chunks = iter(chunks)
        self.fail_after_chunks = fail_after_chunks
        self.interrupt_after_chunks = interrupt_after_chunks
        self.chunks_read = 0
        self.closed = False
        self.request: httpx.Request | None = None

    def __iter__(self) -> Iterator[bytes]:
        for chunk in self.chunks:
            self.chunks_read += 1
            yield chunk
            if self.fail_after_chunks == self.chunks_read:
                assert self.request is not None
                raise httpx.ReadError("stream interrupted", request=self.request)
            if self.interrupt_after_chunks == self.chunks_read:
                raise KeyboardInterrupt

    def close(self) -> None:
        self.closed = True


def streaming_response(
    body: IncrementalBody,
    *,
    status_code: int = 200,
    etag: str | None = ETAG_B,
) -> Callable[[httpx.Request], httpx.Response]:
    def build(request: httpx.Request) -> httpx.Response:
        body.request = request
        headers = {} if etag is None else {"ETag": etag}
        return httpx.Response(status_code, headers=headers, stream=body, request=request)

    return build


def ok_response(
    body: bytes, etag: str | None = ETAG_A
) -> Callable[[httpx.Request], httpx.Response]:
    headers = {"ETag": etag} if etag is not None else {}

    def build(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers=headers, stream=httpx.ByteStream(body), request=request)

    return build


def not_modified(etag: str = ETAG_A) -> Callable[[httpx.Request], httpx.Response]:
    def build(request: httpx.Request) -> httpx.Response:
        return httpx.Response(304, headers={"ETag": etag}, request=request)

    return build


def status_response(code: int) -> Callable[[httpx.Request], httpx.Response]:
    def build(request: httpx.Request) -> httpx.Response:
        return httpx.Response(code, request=request)

    return build


def make_store(tmp_path: Path) -> MappingStore:
    return MappingStore(tmp_path / "mappings")


def endpoint_for(source: ScriptedSource, store: MappingStore) -> ApiEndpoint:
    client = httpx.Client(
        base_url=BASE_ORIGIN,
        transport=httpx.MockTransport(source.handler),
        follow_redirects=False,
        timeout=make_timeout(1.0, 1.0),
    )
    return ApiEndpoint(client, store.headers_factory())


def read_json(path: Path) -> dict[str, Any]:
    return cast("dict[str, Any]", json.loads(path.read_text(encoding="utf-8")))


def iso(moment: datetime) -> str:
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.isoformat()


def activated_store(tmp_path: Path) -> tuple[MappingStore, ScriptedSource, ApiEndpoint]:
    store = make_store(tmp_path)
    source = ScriptedSource()
    source.queue(ok_response(SMALL_XML, ETAG_A))
    endpoint = endpoint_for(source, store)
    outcome = refresh(store, endpoint, now=NOW, refresh_hours=HOURS)
    assert outcome is RefreshOutcome.ACTIVATED
    return store, source, endpoint


def test_parse_source_url_splits_origin_and_path() -> None:
    location = parse_source_url(SOURCE_URL)
    assert location.origin == BASE_ORIGIN
    assert location.path == SOURCE_PATH
    assert location.origin + location.path == SOURCE_URL


@pytest.mark.parametrize(
    ("bad_url", "message"),
    [
        ("ftp://raw.githubusercontent.com/a/b.xml", "scheme"),
        ("https://user:pass@raw.githubusercontent.com/a/b.xml", "credentials"),
        ("https://raw.githubusercontent.com/a/b.xml?ref=main", "query"),
        ("https://raw.githubusercontent.com/a/b.xml#frag", "fragment"),
        ("https://raw.githubusercontent.com", "file path"),
    ],
)
def test_parse_source_url_rejects_bad_urls(bad_url: str, message: str) -> None:
    with pytest.raises(ConfigError, match=message):
        parse_source_url(bad_url)


def test_startup_without_cache(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    assert load_active(store) is None
    assert due(store, NOW) is True
    view = status(store, NOW)
    assert view.active_present is False
    assert view.revision == NO_ENTRIES
    assert view.source_url == SOURCE_URL


def test_first_refresh_activates_and_loads(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    assert len(source.requests) == 1
    assert str(source.requests[0].url) == SOURCE_URL
    assert source.requests[0].headers["Accept-Encoding"] == ACCEPT_ENCODING

    state = read_json(store.state_path)
    assert state["revision"] == REVISION_ONE
    assert state["etag"] == ETAG_A
    assert state["accept_encoding"] == ACCEPT_ENCODING
    assert state["content_sha256"] == content_digest(SMALL_XML)
    assert state["last_success_iso"] == iso(NOW)
    assert state["next_due_iso"] == iso(NOW + timedelta(hours=HOURS))
    assert state["consecutive_failures"] == NO_ENTRIES
    assert state["source_url"] == SOURCE_URL

    meta = read_json(store.meta_path)
    assert store.active_path.read_bytes() == SMALL_XML
    assert meta["content_sha256"] == content_digest(SMALL_XML)
    assert len(meta["targets_digest"]) == SHA256_HEX_LENGTH
    assert len(meta["structure_digest"]) == SHA256_HEX_LENGTH

    active = load_active(store)
    assert active is not None
    assert active.content_sha256 == meta["content_sha256"]
    assert active.targets_digest == meta["targets_digest"]
    assert active.structure_digest == meta["structure_digest"]
    assert active.revision == REVISION_ONE
    resolution = resolve(active.index, 100, 1, 1)
    assert resolution.status is ResolutionStatus.RESOLVED
    assert resolution.target is not None
    assert resolution.target.anidb_id == 1
    assert resolution.target.anidb_episode_number == 1


def test_conditional_get_sends_if_none_match_and_304_preserves_active(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    before = store.active_path.read_bytes()
    source.queue(not_modified(ETAG_A))

    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)
    assert outcome is RefreshOutcome.UNCHANGED
    assert source.requests[1].headers["If-None-Match"] == ETAG_A
    assert store.active_path.read_bytes() == before

    state = read_json(store.state_path)
    assert state["next_due_iso"] == iso(LATER + timedelta(hours=HOURS))
    assert state["last_attempted_iso"] == iso(LATER)
    assert state["consecutive_failures"] == NO_ENTRIES
    assert state["revision"] == REVISION_ONE


def test_not_due_makes_zero_requests_and_force_requests(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    request_count = len(source.requests)

    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS)
    assert outcome is RefreshOutcome.NOT_DUE
    assert len(source.requests) == request_count
    assert not due(store, LATER)

    source.queue(not_modified(ETAG_A))
    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)
    assert outcome is RefreshOutcome.UNCHANGED
    assert len(source.requests) == request_count + 1


def test_next_due_exactly_equal_to_now_is_due(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    boundary = NOW + timedelta(hours=HOURS)
    assert due(store, boundary) is True
    source.queue(not_modified(ETAG_A))
    outcome = refresh(store, endpoint, now=boundary, refresh_hours=HOURS)
    assert outcome is RefreshOutcome.UNCHANGED
    assert len(source.requests) == TWO_REQUESTS


def test_new_valid_content_activates_revision_two(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    old_targets = read_json(store.state_path)["targets_digest"]

    source.queue(ok_response(EXTENDED_XML, ETAG_B))
    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)
    assert outcome is RefreshOutcome.ACTIVATED

    state = read_json(store.state_path)
    assert state["revision"] == REVISION_TWO
    assert state["targets_digest"] != old_targets
    assert state["etag"] == ETAG_B
    assert store.active_path.read_bytes() == EXTENDED_XML
    assert read_json(store.meta_path)["content_sha256"] == content_digest(EXTENDED_XML)


def test_formatting_only_content_is_unchanged_with_validators_refreshed(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    before = read_json(store.state_path)
    assert store.active_path.read_bytes() == SMALL_XML

    source.queue(ok_response(REORDERED_XML, ETAG_B))
    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)
    assert outcome is RefreshOutcome.UNCHANGED

    state = read_json(store.state_path)
    assert state["revision"] == REVISION_ONE
    assert state["targets_digest"] == before["targets_digest"]
    assert state["structure_digest"] == before["structure_digest"]
    assert state["etag"] == ETAG_B
    assert state["next_due_iso"] == iso(LATER + timedelta(hours=HOURS))
    assert store.active_path.read_bytes() == SMALL_XML


def test_identical_200_content_is_unchanged_with_validator_update(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    before = store.active_path.read_bytes()

    source.queue(ok_response(SMALL_XML, ETAG_B))
    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)
    assert outcome is RefreshOutcome.UNCHANGED
    assert store.active_path.read_bytes() == before
    assert read_json(store.state_path)["etag"] == ETAG_B


@pytest.mark.parametrize("extra_capacity", [0, 1])
def test_streamed_mapping_body_accepts_below_and_exact_byte_cap(
    tmp_path: Path, extra_capacity: int
) -> None:
    store = make_store(tmp_path)
    source = ScriptedSource()
    body = IncrementalBody([SMALL_XML[:11], SMALL_XML[11:]])
    source.queue(streaming_response(body))
    endpoint = endpoint_for(source, store)
    try:
        outcome = refresh(
            store,
            endpoint,
            now=NOW,
            refresh_hours=HOURS,
            max_bytes=len(SMALL_XML) + extra_capacity,
        )
    finally:
        endpoint.client.close()

    assert outcome is RefreshOutcome.ACTIVATED
    assert body.chunks_read == 2
    assert body.closed is True
    assert load_active(store) is not None


def test_streamed_mapping_body_stops_at_overflow_and_closes_response(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    active_before = store.active_path.read_bytes()
    meta_before = store.meta_path.read_bytes()
    generated_chunks = 20
    body = IncrementalBody(b"x" * (SMALL_MAX_BYTES + 1) for _ in range(generated_chunks))
    source.queue(streaming_response(body))

    outcome = refresh(
        store,
        endpoint,
        now=LATER,
        refresh_hours=HOURS,
        force=True,
        max_bytes=SMALL_MAX_BYTES,
    )

    assert outcome is RefreshOutcome.FAILED_SIZE
    assert body.chunks_read == 1
    assert body.chunks_read < generated_chunks
    assert body.closed is True
    assert store.active_path.read_bytes() == active_before
    assert store.meta_path.read_bytes() == meta_before


def test_stream_read_error_closes_response_and_preserves_last_known_good(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    active_before = store.active_path.read_bytes()
    body = IncrementalBody([SMALL_XML[:10], SMALL_XML[10:]], fail_after_chunks=1)
    source.queue(streaming_response(body))

    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)

    assert outcome is RefreshOutcome.FAILED_TRANSPORT
    assert body.closed is True
    assert store.active_path.read_bytes() == active_before
    assert load_active(store) is not None


def test_stream_interruption_closes_response_and_propagates(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    active_before = store.active_path.read_bytes()
    body = IncrementalBody([SMALL_XML[:10], SMALL_XML[10:]], interrupt_after_chunks=1)
    source.queue(streaming_response(body))

    with pytest.raises(KeyboardInterrupt):
        refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)

    assert body.closed is True
    assert store.active_path.read_bytes() == active_before


def test_streamed_non_success_and_304_responses_close_without_reading_body(
    tmp_path: Path,
) -> None:
    store, source, endpoint = activated_store(tmp_path)
    error_body = IncrementalBody([b"error body"])
    source.queue(streaming_response(error_body, status_code=503, etag=None))
    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)
    assert outcome is RefreshOutcome.FAILED_TRANSPORT
    assert error_body.chunks_read == 0
    assert error_body.closed is True

    unchanged_body = IncrementalBody([b"unused 304 body"])
    source.queue(streaming_response(unchanged_body, status_code=304, etag=ETAG_A))
    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)
    assert outcome is RefreshOutcome.UNCHANGED
    assert unchanged_body.chunks_read == 0
    assert unchanged_body.closed is True


def test_oversized_body_is_size_failure_and_caps_backoff(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    active_before = store.active_path.read_bytes()
    meta_before = store.meta_path.read_bytes()

    source.queue(ok_response(BULKY_XML, ETAG_B))
    outcome = refresh(
        store, endpoint, now=LATER, refresh_hours=HOURS, force=True, max_bytes=SMALL_MAX_BYTES
    )
    assert outcome is RefreshOutcome.FAILED_SIZE
    assert store.active_path.read_bytes() == active_before
    assert store.meta_path.read_bytes() == meta_before
    state = read_json(store.state_path)
    assert state["consecutive_failures"] == ONE_FAILURE
    assert state["next_due_iso"] == iso(LATER + timedelta(hours=FIRST_BACKOFF_HOURS))

    expected = [2.0, 4.0, 8.0, 16.0, MAX_BACKOFF_HOURS, MAX_BACKOFF_HOURS]
    moment = LATER
    for hours in expected:
        moment = moment + timedelta(hours=HOURS)
        source.queue(status_response(500))
        outcome = refresh(store, endpoint, now=moment, refresh_hours=HOURS, force=True)
        assert outcome is RefreshOutcome.FAILED_TRANSPORT
        state = read_json(store.state_path)
        due_delta = datetime.fromisoformat(cast("str", state["next_due_iso"])) - moment.replace(
            tzinfo=UTC
        )
        assert due_delta == timedelta(hours=hours)
        assert store.active_path.read_bytes() == active_before
    assert state["consecutive_failures"] == len(expected) + ONE_FAILURE


def test_invalid_content_keeps_last_known_good(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    active_before = store.active_path.read_bytes()
    meta_before = store.meta_path.read_bytes()

    source.queue(ok_response(MALFORMED_XML, ETAG_B))
    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)
    assert outcome is RefreshOutcome.FAILED_INVALID
    assert store.active_path.read_bytes() == active_before
    assert store.meta_path.read_bytes() == meta_before
    state = read_json(store.state_path)
    assert state["consecutive_failures"] == ONE_FAILURE
    assert state["revision"] == REVISION_ONE
    assert status(store, LATER).last_outcome is RefreshOutcome.FAILED_INVALID


def test_transport_failure_backs_off_and_keeps_last_known_good(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    active_before = store.active_path.read_bytes()

    source.queue(status_response(500))
    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)
    assert outcome is RefreshOutcome.FAILED_TRANSPORT
    state = read_json(store.state_path)
    assert state["consecutive_failures"] == ONE_FAILURE
    assert state["next_due_iso"] == iso(LATER + timedelta(hours=FIRST_BACKOFF_HOURS))
    assert store.active_path.read_bytes() == active_before

    source.queue(status_response(404))
    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)
    assert outcome is RefreshOutcome.FAILED_TRANSPORT
    assert read_json(store.state_path)["consecutive_failures"] == TWO_FAILURES


def test_transport_exception_is_a_failure_outcome(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    store_dir_state = ScriptedSource()

    def explode(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("unreachable", request=request)

    store_dir_state.queue(explode)
    endpoint = endpoint_for(store_dir_state, store)
    outcome = refresh(store, endpoint, now=NOW, refresh_hours=HOURS)
    assert outcome is RefreshOutcome.NO_ACTIVE_BUT_FAILED
    state = read_json(store.state_path)
    assert state["consecutive_failures"] == ONE_FAILURE
    assert state["last_outcome"] == RefreshOutcome.FAILED_TRANSPORT.value


def test_failure_without_any_active_reports_no_active(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    source = ScriptedSource()
    source.queue(status_response(503))
    endpoint = endpoint_for(source, store)
    outcome = refresh(store, endpoint, now=NOW, refresh_hours=HOURS)
    assert outcome is RefreshOutcome.NO_ACTIVE_BUT_FAILED
    assert not store.active_path.exists()


def test_restart_persistence_sees_state_validators_and_schedule(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    directory = store.directory

    revived = MappingStore(directory, source_url=store.source_url)
    assert due(revived, LATER) is False
    assert due(revived, NOW + timedelta(hours=HOURS)) is True
    headers = revived.request_headers()
    assert headers["Accept-Encoding"] == ACCEPT_ENCODING
    assert headers["If-None-Match"] == ETAG_A
    view = status(revived, LATER)
    assert view.revision == REVISION_ONE
    assert view.active_present is True
    assert view.last_success_iso == iso(NOW)
    assert view.active_age_seconds == ONE_HOUR_SECONDS
    assert revived.source_url == SOURCE_URL
    assert load_active(revived) is not None

    revived_source = ScriptedSource()
    revived_source.queue(not_modified(ETAG_A))
    revived_endpoint = endpoint_for(revived_source, revived)
    outcome = refresh(revived, revived_endpoint, now=LATER, refresh_hours=HOURS, force=True)
    assert outcome is RefreshOutcome.UNCHANGED
    assert revived_source.requests[0].headers["If-None-Match"] == ETAG_A


def test_source_change_on_same_host_requires_fresh_activation(tmp_path: Path) -> None:
    store_a, _, _ = activated_store(tmp_path)
    source_b_url = f"{BASE_ORIGIN}/Anime-Lists/mirror/anime-list-full.xml"
    store_b = MappingStore(store_a.directory, source_url=source_b_url)
    source_b = ScriptedSource()
    source_b.queue(ok_response(SMALL_XML, ETAG_B))
    endpoint_b = endpoint_for(source_b, store_b)
    try:
        # The active XML and weekly schedule belong to source A, even though the host matches.
        assert load_active(store_b) is None
        assert status(store_b, LATER).active_present is False
        assert due(store_b, LATER) is True
        assert store_b.request_headers() == {"Accept-Encoding": ACCEPT_ENCODING}

        outcome = refresh(store_b, endpoint_b, now=LATER, refresh_hours=HOURS)
    finally:
        endpoint_b.client.close()

    assert outcome is RefreshOutcome.ACTIVATED
    request = source_b.requests[0]
    assert str(request.url) == source_b_url
    assert "If-None-Match" not in request.headers
    active_b = load_active(store_b)
    assert active_b is not None
    assert active_b.source_url == source_b_url
    assert active_b.revision == REVISION_TWO
    assert load_active(store_a) is None
    state = read_json(store_b.state_path)
    assert state["source_url"] == source_b_url
    assert state["etag"] == ETAG_B
    assert state["source_change_pending"] is False


def test_failed_source_change_keeps_old_bytes_but_never_authorizes_them(
    tmp_path: Path,
) -> None:
    store_a, _, _ = activated_store(tmp_path)
    old_xml = store_a.active_path.read_bytes()
    source_b_url = f"{BASE_ORIGIN}/Anime-Lists/alternate/anime-list-full.xml"
    store_b = MappingStore(store_a.directory, source_url=source_b_url)
    source_b = ScriptedSource()
    source_b.queue(status_response(503))
    endpoint_b = endpoint_for(source_b, store_b)
    try:
        outcome = refresh(store_b, endpoint_b, now=LATER, refresh_hours=HOURS)
        assert outcome is RefreshOutcome.NO_ACTIVE_BUT_FAILED
        assert source_b.requests[0].headers.get("If-None-Match") is None
    finally:
        endpoint_b.client.close()

    assert store_b.active_path.read_bytes() == old_xml
    assert load_active(store_b) is None
    state = read_json(store_b.state_path)
    assert state["source_url"] == source_b_url
    assert state["etag"] is None
    assert state["source_change_pending"] is True
    assert state["next_due_iso"] == iso(LATER + timedelta(hours=FIRST_BACKOFF_HOURS))
    assert due(store_b, LATER + timedelta(minutes=30)) is False
    assert due(store_b, LATER + timedelta(hours=FIRST_BACKOFF_HOURS)) is True
    reopened_b = MappingStore(store_b.directory, source_url=source_b_url)
    assert due(reopened_b, LATER + timedelta(minutes=30)) is False
    assert reopened_b.request_headers() == {"Accept-Encoding": ACCEPT_ENCODING}

    store_a_again = MappingStore(store_b.directory, source_url=SOURCE_URL)
    assert load_active(store_a_again) is None
    source_a_again = ScriptedSource()
    source_a_again.queue(ok_response(SMALL_XML, ETAG_A))
    endpoint_a_again = endpoint_for(source_a_again, store_a_again)
    try:
        outcome = refresh(store_a_again, endpoint_a_again, now=LATER, refresh_hours=HOURS)
    finally:
        endpoint_a_again.client.close()
    assert outcome is RefreshOutcome.ACTIVATED
    assert load_active(store_a_again) is not None
    assert source_a_again.requests[0].headers.get("If-None-Match") is None


@pytest.mark.parametrize(
    "corruption",
    ["delete_active", "corrupt_active", "delete_meta", "corrupt_meta", "tamper_active"],
)
def test_invalid_active_cache_overrides_future_schedule_and_drops_etag(
    tmp_path: Path, corruption: str
) -> None:
    store, _, _ = activated_store(tmp_path)
    if corruption == "delete_active":
        store.active_path.unlink()
    elif corruption == "corrupt_active":
        store.active_path.write_bytes(b"<anime-list><anime")
    elif corruption == "delete_meta":
        store.meta_path.unlink()
    elif corruption == "corrupt_meta":
        store.meta_path.write_text("{invalid", encoding="utf-8")
    else:
        store.active_path.write_bytes(EXTENDED_XML)

    assert load_active(store) is None
    assert due(store, LATER) is True
    assert store.request_headers() == {"Accept-Encoding": ACCEPT_ENCODING}


def test_missing_active_cache_recovers_before_weekly_due_time(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    store.active_path.unlink()
    source.queue(ok_response(SMALL_XML, ETAG_B))

    assert due(store, LATER) is True
    assert "If-None-Match" not in store.request_headers()
    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS)

    assert outcome in (RefreshOutcome.ACTIVATED, RefreshOutcome.UNCHANGED)
    active = load_active(store)
    assert active is not None
    assert store.active_path.read_bytes() == SMALL_XML
    assert source.requests[-1].headers.get("If-None-Match") is None


def test_failed_missing_cache_recovery_obeys_persisted_backoff(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    store.active_path.unlink()
    source.queue(status_response(503))

    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS)

    assert outcome is RefreshOutcome.NO_ACTIVE_BUT_FAILED
    assert due(store, LATER + timedelta(minutes=30)) is False
    assert due(store, LATER + timedelta(hours=FIRST_BACKOFF_HOURS)) is True
    request_count = len(source.requests)
    reopened = MappingStore(store.directory, source_url=store.source_url)
    assert due(reopened, LATER + timedelta(minutes=30)) is False
    assert (
        refresh(reopened, endpoint, now=LATER + timedelta(minutes=30), refresh_hours=HOURS)
        is RefreshOutcome.NOT_DUE
    )
    assert len(source.requests) == request_count


def test_corrupt_state_file_is_treated_as_empty_not_crash(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    store.state_path.write_text("{not json", encoding="utf-8")
    assert due(store, NOW) is True
    assert status(store, NOW).consecutive_failures == NO_ENTRIES
    assert status(store, NOW).revision == REVISION_ONE
    headers = store.request_headers()
    assert "If-None-Match" not in headers
    source.queue(ok_response(SMALL_XML, ETAG_A))
    outcome = refresh(store, endpoint, now=NOW, refresh_hours=HOURS)
    assert outcome is RefreshOutcome.UNCHANGED


def test_state_with_bad_fields_loads_as_empty(tmp_path: Path) -> None:
    store, _, _ = activated_store(tmp_path)
    bad = {
        "source_url": "ftp://nope/anime.xml",
        "revision": "one",
        "consecutive_failures": -3,
        "content_sha256": "deadbeef",
        "last_outcome": "not-an-outcome",
        "surprise": 1,
    }
    store.state_path.write_text(json.dumps(bad), encoding="utf-8")
    assert due(store, NOW) is True
    assert store.request_headers() == {"Accept-Encoding": ACCEPT_ENCODING}
    view = status(store, NOW)
    assert view.consecutive_failures == NO_ENTRIES
    assert view.last_outcome is None
    assert view.revision == REVISION_ONE


def test_state_json_is_sorted_keys_and_deterministic(tmp_path: Path) -> None:
    store, _, _ = activated_store(tmp_path)
    text = store.state_path.read_text(encoding="utf-8")
    payload = json.loads(text)
    assert text == json.dumps(payload, sort_keys=True) + "\n"


def test_tampered_active_bytes_are_refused(tmp_path: Path) -> None:
    store, _, _ = activated_store(tmp_path)
    assert load_active(store) is not None
    store.active_path.write_bytes(SMALL_XML + b" ")
    assert load_active(store) is None


def test_corrupt_meta_sha_is_refused(tmp_path: Path) -> None:
    store, _, _ = activated_store(tmp_path)
    meta = read_json(store.meta_path)
    meta["content_sha256"] = "f" * SHA256_HEX_LENGTH
    store.meta_path.write_text(json.dumps(meta, sort_keys=True), encoding="utf-8")
    assert load_active(store) is None
    assert store.active_path.read_bytes() == SMALL_XML


def test_corrupt_meta_digest_is_refused(tmp_path: Path) -> None:
    store, _, _ = activated_store(tmp_path)
    meta = read_json(store.meta_path)
    meta["targets_digest"] = "0" * SHA256_HEX_LENGTH
    store.meta_path.write_text(json.dumps(meta, sort_keys=True), encoding="utf-8")
    assert load_active(store) is None


def test_unreadable_meta_is_refused(tmp_path: Path) -> None:
    store, _, _ = activated_store(tmp_path)
    store.meta_path.write_text("[1, 2]", encoding="utf-8")
    assert load_active(store) is None


def test_overrides_are_part_of_the_effective_generation(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)

    source.queue(ok_response(SMALL_XML, ETAG_B))
    outcome = refresh(
        store,
        endpoint,
        now=LATER,
        refresh_hours=HOURS,
        force=True,
        overrides_text=OVERRIDE_ADD_GAP,
    )
    assert outcome is RefreshOutcome.ACTIVATED
    state = read_json(store.state_path)
    assert state["revision"] == REVISION_TWO

    overrides = load_overrides(OVERRIDE_ADD_GAP)
    active = load_active(store, overrides)
    assert active is not None
    targets = active.index.override_adds.get((100, 1, 2))
    assert targets is not None
    assert targets[0].episode_type is EpisodeType.EPISODE
    assert load_active(store) is None


def test_invalid_overrides_fail_without_touching_active(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    active_before = store.active_path.read_bytes()

    source.queue(ok_response(SMALL_XML, ETAG_B))
    outcome = refresh(
        store,
        endpoint,
        now=LATER,
        refresh_hours=HOURS,
        force=True,
        overrides_text=MALFORMED_OVERRIDES,
    )
    assert outcome is RefreshOutcome.FAILED_INVALID
    assert store.active_path.read_bytes() == active_before


def test_endpoint_must_be_bound_to_the_source_origin(tmp_path: Path) -> None:
    store = make_store(tmp_path)

    foreign = httpx.Client(
        base_url="https://evil.example",
        transport=httpx.MockTransport(lambda request: httpx.Response(200, request=request)),
        follow_redirects=False,
    )
    endpoint = ApiEndpoint(foreign, store.headers_factory())
    with pytest.raises(HttpPolicyError, match="origin"):
        refresh(store, endpoint, now=NOW, refresh_hours=HOURS)

    pathed = httpx.Client(
        base_url=f"{BASE_ORIGIN}/extra",
        transport=httpx.MockTransport(lambda request: httpx.Response(200, request=request)),
        follow_redirects=False,
    )
    endpoint = ApiEndpoint(pathed, store.headers_factory())
    with pytest.raises(HttpPolicyError, match="without a path"):
        refresh(store, endpoint, now=NOW, refresh_hours=HOURS)

    no_encoding = httpx.Client(
        base_url=BASE_ORIGIN,
        transport=httpx.MockTransport(lambda request: httpx.Response(200, request=request)),
        follow_redirects=False,
    )
    endpoint = ApiEndpoint(no_encoding, lambda: {})
    with pytest.raises(HttpPolicyError, match="Accept-Encoding"):
        refresh(store, endpoint, now=NOW, refresh_hours=HOURS)
    for client in (foreign, pathed, no_encoding):
        client.close()


def test_zero_service_api_io(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    source.queue(not_modified(ETAG_A))
    source.queue(ok_response(EXTENDED_XML, ETAG_B))
    refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)
    refresh(store, endpoint, now=LATER + timedelta(hours=2), refresh_hours=HOURS, force=True)
    assert len(source.requests) == THREE_REQUESTS
    for request in source.requests:
        assert str(request.url) == SOURCE_URL
        assert "X-Api-Key" not in request.headers
        assert "apikey" not in request.headers


def test_make_source_endpoint_is_anonymous_identity_client(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    endpoint = make_source_endpoint(store)
    try:
        assert str(endpoint.base_url) == f"{BASE_ORIGIN}/"
        headers = endpoint.headers_factory()
        assert headers == {"Accept-Encoding": ACCEPT_ENCODING}
        assert endpoint.client.follow_redirects is False
    finally:
        endpoint.client.close()


def test_due_and_next_refresh_due(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    assert due(store, NOW) is True
    assert next_refresh_due(store, NOW, HOURS) == NOW.replace(tzinfo=UTC)

    store, source, endpoint = activated_store(tmp_path)
    source.queue(not_modified(ETAG_A))
    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)
    assert outcome is RefreshOutcome.UNCHANGED
    assert due(store, LATER + timedelta(minutes=1)) is False
    assert next_refresh_due(store, NOW, HOURS) == LATER.replace(tzinfo=UTC) + timedelta(hours=HOURS)
    overdue = NOW + timedelta(days=365)
    assert due(store, overdue) is True
    assert next_refresh_due(store, overdue, HOURS) == overdue.replace(tzinfo=UTC)


def test_refresh_gate_rejects_bad_parameters(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    source = ScriptedSource()
    endpoint = endpoint_for(source, store)
    with pytest.raises(ConfigError, match="refresh_hours"):
        refresh(store, endpoint, now=NOW, refresh_hours=0.0)
    with pytest.raises(ConfigError, match="max_bytes"):
        refresh(store, endpoint, now=NOW, refresh_hours=HOURS, max_bytes=0)
    assert source.requests == []


def test_settings_derived_store_directory(tmp_path: Path) -> None:
    document = {
        "sonarr": {
            "url": "http://s:8989",
            "api_key_env": "SONARR_API_KEY",
            "allowed_roots": ["/anime"],
        },
        "shoko": {
            "url": "http://k:8111",
            "api_key_env": "SHOKO_API_KEY",
            "allowed_roots": ["/anime"],
        },
        "paths": {"mappings": [{"sonarr": "/anime", "shoko": "/anime"}]},
        "mappings": {"source": "anime-lists", "source_url": SOURCE_URL},
        "sync": {},
        "database": {"path": "/data/state/state.sqlite3"},
    }
    settings = parse_settings(yaml.safe_dump(document))
    store = MappingStore.from_settings(settings)
    expected_parent = Path("/data/state/state.sqlite3").parent
    assert store.directory == expected_parent / "mappings"
    assert store.directory.name == "mappings"
    assert store.source_url == SOURCE_URL


def test_status_reports_active_and_schedule(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    view = status(store, NOW + timedelta(hours=2))
    assert view.active_present is True
    assert view.revision == REVISION_ONE
    assert view.content_sha256 == content_digest(SMALL_XML)
    assert len(cast("str", view.targets_digest)) == SHA256_HEX_LENGTH
    assert len(cast("str", view.structure_digest)) == SHA256_HEX_LENGTH
    assert view.active_age_seconds == TWO_HOURS_SECONDS
    assert view.last_attempted_iso == iso(NOW)
    assert view.last_success_iso == iso(NOW)
    assert view.next_due_iso == iso(NOW + timedelta(hours=HOURS))
    assert view.last_outcome is RefreshOutcome.ACTIVATED
    assert view.consecutive_failures == NO_ENTRIES
    assert view.entries_indexed == 1
    assert view.mapping_rules == NO_ENTRIES
    assert view.explicit_pairs == NO_ENTRIES
    assert DEFAULT_REFRESH_HOURS > 0


# ------------------------------------------------- orchestrator review regressions


def test_304_with_missing_active_cache_forces_full_refetch(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    store.active_path.unlink()
    source.queue(not_modified(ETAG_A))
    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)
    assert outcome is RefreshOutcome.NO_ACTIVE_BUT_FAILED
    assert store._load_state().etag is None
    source.queue(ok_response(EXTENDED_XML, ETAG_B))
    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)
    assert outcome is RefreshOutcome.ACTIVATED
    last_request = source.requests[-1]
    assert "If-None-Match" not in last_request.headers
    active = load_active(store)
    assert active is not None
    assert active.revision == REVISION_TWO


# ------------------------------------------------------- M4 review-driven regressions


def redirect_to(location: str) -> Callable[[httpx.Request], httpx.Response]:
    def build(request: httpx.Request) -> httpx.Response:
        return httpx.Response(302, headers={"location": location}, request=request)

    return build


def test_source_redirect_is_refused_and_cache_never_moves(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    before_active = store.active_path.read_bytes()
    before_meta = store.meta_path.read_bytes()
    before_etag = store._load_state().etag
    source.queue(redirect_to(SOURCE_PATH))
    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)
    assert outcome is RefreshOutcome.FAILED_TRANSPORT
    source.queue(redirect_to("https://evil.example/data.xml"))
    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)
    assert outcome is RefreshOutcome.FAILED_TRANSPORT
    assert store.active_path.read_bytes() == before_active
    assert store.meta_path.read_bytes() == before_meta
    assert store._load_state().etag == before_etag
    assert len(source.requests) == THREE_REQUESTS
    assert all(str(request.url) == SOURCE_URL for request in source.requests)
