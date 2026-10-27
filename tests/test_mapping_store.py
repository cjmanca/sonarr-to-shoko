"""Anime-Lists runtime download/refresh/activation store (M4-b).

All offline: the only I/O is an ``httpx.MockTransport`` endpoint bound to the
canonical source origin; no test performs network access and no test touches Sonarr
or Shoko. Datetimes are fake UTC values.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event, Thread
from typing import Any, cast

import httpx
import pytest
import yaml  # type: ignore[import-untyped]

import shoko_linker.mapping_store as mapping_store_module
from shoko_linker.config import DEFAULT_MAPPING_SOURCE_URL, DEFAULT_REFRESH_HOURS, parse_settings
from shoko_linker.errors import ConfigError, HttpPolicyError
from shoko_linker.http import ApiEndpoint, make_timeout
from shoko_linker.mapping_store import (
    ACCEPT_ENCODING,
    MAX_BACKOFF_HOURS,
    MappingStatus,
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
    build_mapping_index,
    content_digest,
    load_overrides,
    parse_anime_lists,
    resolve,
    structure_digest,
    targets_digest,
)
from shoko_linker.state import ApplyProcessLock

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
CROSS_ENTRY_INVALID_XML = (
    b'<anime-list><anime anidbid="100" tvdbid="267440" defaulttvdbseason="1" '
    b'episodeoffset="0"/><anime anidbid="200" tvdbid="267440" defaulttvdbseason="1" '
    b'episodeoffset="16"><mapping-list><mapping anidbseason="1" tvdbseason="1">'
    b";1-2oops;</mapping></mapping-list></anime></anime-list>"
)

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
        ("http://example.invalid/mappings.xml", "HTTPS"),
        ("ftp://raw.githubusercontent.com/a/b.xml", "scheme"),
        ("https://user:pass@raw.githubusercontent.com/a/b.xml", "credentials"),
        ("https://raw.githubusercontent.com/a/b.xml?ref=main", "query"),
        ("https://raw.githubusercontent.com/a/b.xml#frag", "fragment"),
        ("https://raw.githubusercontent.com", "file path"),
        ("https://example.invalid:0/mappings.xml", "port"),
        ("https://example.invalid:bad/mappings.xml", "port"),
        ("https://[bad/mappings.xml", "valid HTTP"),
    ],
)
def test_parse_source_url_rejects_bad_urls(bad_url: str, message: str) -> None:
    with pytest.raises(ConfigError, match=message):
        parse_source_url(bad_url)


def test_store_rejects_http_source_before_creating_cache(tmp_path: Path) -> None:
    directory = tmp_path / "mappings"
    with pytest.raises(ConfigError, match="HTTPS"):
        MappingStore(directory, source_url="http://example.invalid/mappings.xml")
    assert not directory.exists()


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
    pointer_before = read_json(store.active_generation_path)
    generations_before = tuple(store.generations_path.iterdir())
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
    pointer_after = read_json(store.active_generation_path)
    assert pointer_after["generation"] == pointer_before["generation"]
    assert pointer_after["state"]["etag"] == ETAG_B
    assert pointer_after["state"]["next_due_iso"] == iso(LATER + timedelta(hours=HOURS))
    assert tuple(store.generations_path.iterdir()) == generations_before


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


@pytest.mark.parametrize("failures", [1, 2, 5, 6, 1025, 10**100])
def test_backoff_saturates_before_evaluating_large_failure_counts(failures: int) -> None:
    expected = 2.0 ** (failures - 1) if failures < 6 else MAX_BACKOFF_HOURS
    assert mapping_store_module._backoff_hours(failures) == expected


def test_refresh_persists_failure_and_retry_after_long_failure_streak(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    active_before = store.active_path.read_bytes()
    state = store._load_state()
    state.consecutive_failures = 1024
    store._save_pointer_scheduler_state(state)
    store._save_state(state)
    source.queue(status_response(500))

    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)

    assert outcome is RefreshOutcome.FAILED_TRANSPORT
    expected_due = iso(LATER + timedelta(hours=MAX_BACKOFF_HOURS))
    recorded = read_json(store.state_path)
    assert recorded["consecutive_failures"] == 1025
    assert recorded["next_due_iso"] == expected_due
    assert store.active_path.read_bytes() == active_before

    # The committed generation's scheduler also recovers the capped retry after state loss.
    store.state_path.unlink()
    reopened = make_store(tmp_path)
    recovered = status(reopened, LATER)
    assert recovered.consecutive_failures == 1025
    assert recovered.next_due_iso == expected_due
    assert due(reopened, LATER) is False
    endpoint.client.close()


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


def test_invalid_mapping_entry_does_not_activate_partial_snapshot(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    malformed_mapping = (
        b'<anime-list><anime anidbid="1" tvdbid="100" defaulttvdbseason="1" '
        b'episodeoffset="0"><mapping-list><mapping anidbseason="1" tvdbseason="1">'
        b";1-2oops;</mapping></mapping-list></anime></anime-list>"
    )

    source.queue(ok_response(malformed_mapping, ETAG_B))
    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)

    assert outcome is RefreshOutcome.FAILED_INVALID
    active = load_active(store)
    assert active is not None
    assert active.revision == REVISION_ONE
    assert active.index.entries_excluded == 0
    assert resolve(active.index, 100, 1, 2).status is ResolutionStatus.RESOLVED


def test_known_unsupported_entry_activates_and_fences_persist_across_cache_restart(
    tmp_path: Path,
) -> None:
    aot_path = Path(__file__).parent / "fixtures" / "aot_real_entries.xml"
    extra = (
        b'<anime anidbid="99001" tvdbid="99002" defaulttvdbseason="a" episodeoffset="4">'
        b'<mapping-list><mapping anidbseason="1" tvdbseason="1" start="1" end="4" '
        b'offset="0"/></mapping-list></anime>'
    )
    body = aot_path.read_bytes().replace(b"</anime-list>", extra + b"</anime-list>")
    store = make_store(tmp_path)
    source = ScriptedSource()
    source.queue(ok_response(body, ETAG_A))
    endpoint = endpoint_for(source, store)

    try:
        outcome = refresh(store, endpoint, now=NOW, refresh_hours=HOURS, force=True)
        active = load_active(store)
        assert outcome is RefreshOutcome.ACTIVATED
        assert active is not None
        assert active.index.entries_indexed == 8
        assert active.index.entries_excluded == 1
        assert active.index.unsafe_tvdb_ids == frozenset({99002})
        anchor = resolve(active.index, 267440, 4, 26, absolute=85)
        assert anchor.status is ResolutionStatus.RESOLVED
        assert anchor.target is not None
        assert (anchor.target.anidb_id, anchor.target.anidb_episode_number) == (16177, 10)
        assert resolve(active.index, 99002, 1, 1).status is ResolutionStatus.UNMAPPED
        assert store.invalid_source_hold() is False
        assert store.active_identity() is not None

        plain_index = build_mapping_index(parse_anime_lists(aot_path.read_bytes()))
        assert active.targets_digest != targets_digest(plain_index)
        expected_identity = store.active_identity()
    finally:
        endpoint.client.close()

    reopened = make_store(tmp_path)
    reopened_active = load_active(reopened)
    assert reopened_active is not None
    assert reopened.active_identity() == expected_identity
    assert reopened_active.targets_digest == active.targets_digest
    assert reopened_active.structure_digest == active.structure_digest
    assert reopened_active.index.unsafe_tvdb_ids == frozenset({99002})
    assert resolve(reopened_active.index, 99002, 1, 1).status is ResolutionStatus.UNMAPPED
    assert reopened.invalid_source_hold() is False


def test_pinned_upstream_special_number_uncertainty_does_not_reject_snapshot(
    tmp_path: Path,
) -> None:
    fixture = Path(__file__).parent / "fixtures" / "anime_lists_legacy_number_examples.xml"
    store = make_store(tmp_path)
    source = ScriptedSource()
    source.queue(ok_response(fixture.read_bytes(), ETAG_A))
    endpoint = endpoint_for(source, store)

    try:
        outcome = refresh(store, endpoint, now=NOW, refresh_hours=HOURS, force=True)
        active = load_active(store)

        assert outcome is RefreshOutcome.ACTIVATED
        assert active is not None
        assert active.index.unsafe_tvdb_keys == frozenset(
            {(79032, 0, 3), (79032, 0, 4), (79032, 0, 5)}
        )
        assert active.index.unsafe_tvdb_ids == frozenset()
        trailer = resolve(active.index, 79032, 0, 1)
        other = resolve(active.index, 79032, 0, 2)
        assert trailer.target is not None
        assert other.target is not None
        assert trailer.target.episode_type is EpisodeType.TRAILER
        assert other.target.episode_type is EpisodeType.OTHER
        assert store.invalid_source_hold() is False
    finally:
        endpoint.client.close()


@pytest.mark.parametrize(
    "body",
    [
        (
            b'<anime-list><anime anidbid="100" tvdbid="2" defaulttvdbseason="a" '
            b'episodeoffset="2"/><anime anidbid="100" tvdbid="3" '
            b'defaulttvdbseason="1" episodeoffset="0"/></anime-list>'
        ),
        (
            b'<anime-list><anime anidbid="100" tvdbid="3" defaulttvdbseason="1" '
            b'episodeoffset="0"/><anime anidbid="100" tvdbid="2" '
            b'defaulttvdbseason="a" episodeoffset="2"/></anime-list>'
        ),
        (
            b'<anime-list><anime anidbid="100" tvdbid="2" defaulttvdbseason="a" '
            b'episodeoffset="2"/><anime anidbid="100" tvdbid="3" '
            b'defaulttvdbseason="a" episodeoffset="4"/></anime-list>'
        ),
    ],
    ids=["unsupported-before-valid", "valid-before-unsupported", "multiple-unsupported"],
)
def test_duplicate_anidb_refresh_preserves_last_known_good_and_holds_writes(
    tmp_path: Path, body: bytes
) -> None:
    store, source, endpoint = activated_store(tmp_path)
    active_before = store.active_path.read_bytes()
    meta_before = store.meta_path.read_bytes()
    identity_before = store.active_identity()
    source.queue(ok_response(body, ETAG_B))

    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)

    active = load_active(store)
    assert outcome is RefreshOutcome.FAILED_INVALID
    assert active is not None
    assert active.revision == REVISION_ONE
    assert active.index.unsafe_tvdb_ids == frozenset()
    assert resolve(active.index, 3, 1, 1).status is ResolutionStatus.UNMAPPED
    assert store.active_path.read_bytes() == active_before
    assert store.meta_path.read_bytes() == meta_before
    assert store.active_identity() == identity_before
    assert len(tuple(store.generations_path.iterdir())) == 1
    assert store.invalid_source_hold() is True
    assert status(store, LATER).last_outcome is RefreshOutcome.FAILED_INVALID
    endpoint.client.close()


@pytest.mark.parametrize("line_ending", ["&#10;", "&#13;", "\n", "\r"])
@pytest.mark.parametrize("malformed_first", [False, True], ids=["valid-first", "malformed-first"])
def test_newline_terminated_anidbid_refresh_preserves_last_known_good_and_holds_writes(
    tmp_path: Path, line_ending: str, malformed_first: bool
) -> None:
    store, source, endpoint = activated_store(tmp_path)
    active_before = store.active_path.read_bytes()
    valid = "<anime anidbid='100' tvdbid='3' defaulttvdbseason='1' episodeoffset='0'/>"
    malformed = (
        f"<anime anidbid='100{line_ending}' tvdbid='2' defaulttvdbseason='1' episodeoffset='0'/>"
    )
    body = f"<anime-list>{malformed + valid if malformed_first else valid + malformed}</anime-list>"
    source.queue(ok_response(body.encode("utf-8"), ETAG_B))

    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)

    active = load_active(store)
    assert outcome is RefreshOutcome.FAILED_INVALID
    assert active is not None
    assert active.revision == REVISION_ONE
    assert store.active_path.read_bytes() == active_before
    assert store.invalid_source_hold() is True
    assert status(store, LATER).last_outcome is RefreshOutcome.FAILED_INVALID
    endpoint.client.close()


def test_semantically_invalid_refresh_preserves_last_known_good_and_backs_off(
    tmp_path: Path,
) -> None:
    store, source, endpoint = activated_store(tmp_path)
    active_before = store.active_path.read_bytes()
    meta_before = store.meta_path.read_bytes()
    source.queue(ok_response(CROSS_ENTRY_INVALID_XML, ETAG_B))

    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)

    assert outcome is RefreshOutcome.FAILED_INVALID
    assert store.active_path.read_bytes() == active_before
    assert store.meta_path.read_bytes() == meta_before
    assert len(tuple(store.generations_path.iterdir())) == 1
    active = load_active(store)
    assert active is not None
    assert active.revision == REVISION_ONE
    failed_state = read_json(store.state_path)
    assert failed_state["consecutive_failures"] == ONE_FAILURE
    assert failed_state["next_due_iso"] == iso(LATER + timedelta(hours=FIRST_BACKOFF_HOURS))
    assert status(store, LATER).last_outcome is RefreshOutcome.FAILED_INVALID
    assert status(store, LATER).invalid_source_hold is True

    requests_before_backoff = len(source.requests)
    retry_time = LATER + timedelta(minutes=30)
    assert due(store, retry_time) is False
    assert refresh(store, endpoint, now=retry_time, refresh_hours=HOURS) is RefreshOutcome.NOT_DUE
    assert len(source.requests) == requests_before_backoff


def test_invalid_source_hold_survives_transport_failure_restart_and_backoff(
    tmp_path: Path,
) -> None:
    store, source, endpoint = activated_store(tmp_path)
    invalid_at = NOW + timedelta(hours=HOURS)
    source.queue(ok_response(b'{"anime-list": []}', ETAG_B))

    invalid = refresh(store, endpoint, now=invalid_at, refresh_hours=HOURS)
    invalid_status = status(store, invalid_at)
    endpoint.client.close()

    # A new process must not forget the invalid observation when a later transport
    # failure replaces last_outcome.
    reopened = make_store(tmp_path)
    source.queue(status_response(503))
    reopened_endpoint = endpoint_for(source, reopened)
    transport_at = invalid_at + timedelta(hours=1)
    transport = refresh(reopened, reopened_endpoint, now=transport_at, refresh_hours=HOURS)
    not_due_at = transport_at + timedelta(minutes=30)
    requests_before_backoff = len(source.requests)
    not_due = refresh(reopened, reopened_endpoint, now=not_due_at, refresh_hours=HOURS)
    held_status = status(reopened, not_due_at)

    assert invalid is RefreshOutcome.FAILED_INVALID
    assert invalid_status.invalid_source_hold is True
    assert transport is RefreshOutcome.FAILED_TRANSPORT
    assert not_due is RefreshOutcome.NOT_DUE
    assert len(source.requests) == requests_before_backoff
    assert held_status.last_outcome is RefreshOutcome.FAILED_TRANSPORT
    assert held_status.consecutive_failures == 2
    assert held_status.invalid_source_hold is True
    assert held_status.revision == REVISION_ONE
    reopened_endpoint.client.close()


def test_unsolicited_304_does_not_clear_invalid_source_hold_but_valid_200_does(
    tmp_path: Path,
) -> None:
    store, source, endpoint = activated_store(tmp_path)
    invalid_at = NOW + timedelta(hours=HOURS)
    source.queue(ok_response(b'{"error": "not an Anime-Lists XML document"}', ETAG_B))
    assert (
        refresh(store, endpoint, now=invalid_at, refresh_hours=HOURS)
        is RefreshOutcome.FAILED_INVALID
    )

    # Even an unsolicited 304 does not validate the malformed representation observed
    # above and therefore cannot release the hold or reset its bounded retry schedule.
    source.queue(not_modified(ETAG_A))
    assert (
        refresh(store, endpoint, now=invalid_at + timedelta(minutes=1), force=True)
        is RefreshOutcome.FAILED_TRANSPORT
    )
    assert status(store, invalid_at + timedelta(minutes=1)).invalid_source_hold is True
    assert "If-None-Match" not in source.requests[-1].headers

    # A fetched and fully parsed 200 is the recovery proof, even when its effective
    # mappings are semantically unchanged from the last-known-good cache.
    source.queue(ok_response(SMALL_XML, ETAG_B))
    assert (
        refresh(store, endpoint, now=invalid_at + timedelta(minutes=2), force=True)
        is RefreshOutcome.UNCHANGED
    )
    assert status(store, invalid_at + timedelta(minutes=2)).invalid_source_hold is False
    endpoint.client.close()


def test_invalid_source_hold_retries_unconditionally_with_backoff_and_survives_restart(
    tmp_path: Path,
) -> None:
    store, source, endpoint = activated_store(tmp_path)
    invalid_at = NOW + timedelta(hours=HOURS)
    source.queue(ok_response(b'{"invalid": true}', ETAG_B))
    assert (
        refresh(store, endpoint, now=invalid_at, refresh_hours=HOURS)
        is RefreshOutcome.FAILED_INVALID
    )
    endpoint.client.close()

    reopened = make_store(tmp_path)
    endpoint = endpoint_for(source, reopened)
    retry_at = invalid_at + timedelta(minutes=1)

    # A broken source may still send an unsolicited 304. It cannot prove that the
    # previously-invalid body is repaired, and must schedule a bounded retry.
    source.queue(not_modified(ETAG_A))
    assert refresh(reopened, endpoint, now=retry_at, force=True) is RefreshOutcome.FAILED_TRANSPORT
    assert "If-None-Match" not in source.requests[-1].headers
    held = status(reopened, retry_at)
    assert held.invalid_source_hold is True
    assert held.consecutive_failures == TWO_FAILURES
    assert held.next_due_iso == iso(retry_at + timedelta(hours=2))

    endpoint.client.close()
    reopened = make_store(tmp_path)
    endpoint = endpoint_for(source, reopened)
    source.queue(ok_response(SMALL_XML, ETAG_A))
    assert (
        refresh(reopened, endpoint, now=retry_at + timedelta(hours=2), refresh_hours=HOURS)
        is RefreshOutcome.UNCHANGED
    )
    assert "If-None-Match" not in source.requests[-1].headers
    assert status(reopened, retry_at + timedelta(hours=2)).invalid_source_hold is False
    assert reopened.active_path.read_bytes() == SMALL_XML
    endpoint.client.close()


def test_generation_lock_releases_queued_refresh_when_owner_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, source, original_endpoint = activated_store(tmp_path)
    original_endpoint.client.close()
    queued_store = make_store(tmp_path)
    request_started = Event()
    refresh_started = Event()
    contender_waiting = Event()
    source.queue(ok_response(b'{"invalid": true}', ETAG_B))
    original_handler = source.handler

    def observe_request(request: httpx.Request) -> httpx.Response:
        request_started.set()
        return original_handler(request)

    # Use a separate endpoint so the real refresh's request remains observable while
    # its separate store instance contends on the same on-disk generation lock.
    client = httpx.Client(
        base_url=BASE_ORIGIN,
        transport=httpx.MockTransport(observe_request),
        follow_redirects=False,
        timeout=make_timeout(1.0, 1.0),
    )
    endpoint = ApiEndpoint(client, queued_store.headers_factory())
    outcomes: list[RefreshOutcome] = []
    errors: list[BaseException] = []

    def queued_refresh() -> None:
        refresh_started.set()
        try:
            outcomes.append(refresh(queued_store, endpoint, now=LATER, force=True))
        except BaseException as exc:
            errors.append(exc)

    worker = Thread(target=queued_refresh, daemon=True)
    original_acquire = ApplyProcessLock.acquire

    def observe_contender(lock: Any) -> bool:
        acquired = original_acquire(lock)
        if lock.path.name == "generation.lock" and not acquired:
            contender_waiting.set()
        return acquired

    monkeypatch.setattr(ApplyProcessLock, "acquire", observe_contender)

    def raise_while_refresh_is_queued() -> None:
        with store.generation_lock() as acquired:
            assert acquired
            worker.start()
            assert refresh_started.wait(1.0)
            assert contender_waiting.wait(1.0)
            assert not request_started.wait(0.05)
            raise RuntimeError("owner failed")

    try:
        with pytest.raises(RuntimeError, match="owner failed"):
            raise_while_refresh_is_queued()

        worker.join(timeout=5.0)
        assert not worker.is_alive()
        assert errors == []
        assert outcomes == [RefreshOutcome.FAILED_INVALID]
        assert request_started.is_set()
        assert queued_store.invalid_source_hold()
    finally:
        endpoint.client.close()


def test_invalid_refresh_under_an_independent_generation_lease_is_persisted(
    tmp_path: Path,
) -> None:
    store, _, initial_endpoint = activated_store(tmp_path)
    initial_endpoint.client.close()
    refresh_store = make_store(tmp_path)
    request_started = Event()
    release_response = Event()
    source = ScriptedSource()

    def delayed_invalid_response(request: httpx.Request) -> httpx.Response:
        request_started.set()
        assert release_response.wait(5.0)
        return httpx.Response(
            200,
            stream=httpx.ByteStream(b'{"invalid": "not Anime-Lists XML"}'),
            request=request,
        )

    source.queue(delayed_invalid_response)
    endpoint = endpoint_for(source, refresh_store)
    outcomes: list[RefreshOutcome] = []
    errors: list[BaseException] = []

    def run_refresh() -> None:
        try:
            outcomes.append(refresh(refresh_store, endpoint, now=LATER, force=True))
        except BaseException as exc:
            errors.append(exc)

    worker = Thread(target=run_refresh, daemon=True)
    try:
        worker.start()
        assert request_started.wait(5.0)
        with store.generation_lock() as acquired:
            assert acquired
            release_response.set()
            worker.join(timeout=5.0)
            assert not worker.is_alive()
            assert errors == []
            assert outcomes == [RefreshOutcome.FAILED_LOCKED]
            # The invalid response was fully observed, but the independent refresh
            # store could not update scheduler metadata while the generation lease lived.
            assert refresh_store.invalid_source_hold() is True

        reopened = make_store(tmp_path)
        retry_at = LATER + timedelta(minutes=30)
        recovered = status(reopened, retry_at)
        assert recovered.invalid_source_hold is True
        assert recovered.last_outcome is RefreshOutcome.FAILED_INVALID
        assert recovered.next_due_iso == iso(LATER + timedelta(hours=FIRST_BACKOFF_HOURS))
        assert due(reopened, retry_at) is False

        retry_source = ScriptedSource()
        retry_endpoint = endpoint_for(retry_source, reopened)
        assert refresh(reopened, retry_endpoint, now=retry_at) is RefreshOutcome.NOT_DUE
        assert retry_source.requests == []
        retry_endpoint.client.close()
    finally:
        release_response.set()
        worker.join(timeout=5.0)
        endpoint.client.close()


def test_invalid_observation_is_durable_before_refresh_interruption(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, source, endpoint = activated_store(tmp_path)
    invalid_at = NOW + timedelta(hours=HOURS)
    source.queue(ok_response(b'{"invalid": "not Anime-Lists XML"}', ETAG_B))
    persist_observation = MappingStore._persist_invalid_source_observation

    def interrupt_after_persist(current_store: MappingStore, now: datetime) -> str:
        observation_id = persist_observation(current_store, now)
        raise KeyboardInterrupt(f"simulated process interruption after {observation_id}")

    monkeypatch.setattr(
        MappingStore, "_persist_invalid_source_observation", interrupt_after_persist
    )
    try:
        with pytest.raises(KeyboardInterrupt, match="simulated process interruption"):
            refresh(store, endpoint, now=invalid_at, force=True)
    finally:
        endpoint.client.close()

    reopened = make_store(tmp_path)
    assert reopened.invalid_source_hold() is True
    recovered_state = status(reopened, invalid_at)
    assert recovered_state.invalid_source_hold is True
    assert recovered_state.last_outcome is RefreshOutcome.FAILED_INVALID
    assert recovered_state.next_due_iso == iso(invalid_at + timedelta(hours=FIRST_BACKOFF_HOURS))


def test_legacy_failed_invalid_outcome_migrates_to_persisted_source_hold(
    tmp_path: Path,
) -> None:
    store, _, endpoint = activated_store(tmp_path)
    state = read_json(store.state_path)
    state.pop("invalid_source_hold", None)
    state["last_outcome"] = RefreshOutcome.FAILED_INVALID.value
    state["consecutive_failures"] = ONE_FAILURE
    state["next_due_iso"] = iso(LATER)
    store.state_path.write_text(json.dumps(state), encoding="utf-8")

    pointer = read_json(store.active_generation_path)
    pointer_state = pointer["state"]
    pointer_state.pop("invalid_source_hold", None)
    pointer_state["last_outcome"] = RefreshOutcome.FAILED_INVALID.value
    pointer_state["consecutive_failures"] = ONE_FAILURE
    pointer_state["next_due_iso"] = iso(LATER)
    store.active_generation_path.write_text(json.dumps(pointer), encoding="utf-8")

    migrated = status(store, NOW)

    assert migrated.last_outcome is RefreshOutcome.FAILED_INVALID
    assert migrated.invalid_source_hold is True
    endpoint.client.close()


def test_invalid_legacy_active_cache_is_neither_usable_nor_a_conditional_hit(
    tmp_path: Path,
) -> None:
    store, _, endpoint = activated_store(tmp_path)
    state = read_json(store.state_path)
    meta = read_json(store.meta_path)
    index = build_mapping_index(parse_anime_lists(CROSS_ENTRY_INVALID_XML))
    content_sha = content_digest(CROSS_ENTRY_INVALID_XML)
    targets = targets_digest(index)
    structure = structure_digest(index)
    meta.update(
        {
            "content_sha256": content_sha,
            "targets_digest": targets,
            "structure_digest": structure,
            "entries_indexed": index.entries_indexed,
            "mapping_rules": index.mapping_rules,
            "explicit_pairs": index.explicit_pairs,
        }
    )
    state.update(
        {
            "content_sha256": content_sha,
            "targets_digest": targets,
            "structure_digest": structure,
        }
    )
    store.active_generation_path.unlink()
    store._legacy_active_path.write_bytes(CROSS_ENTRY_INVALID_XML)
    store._legacy_meta_path.write_text(json.dumps(meta), encoding="utf-8")
    store.state_path.write_text(json.dumps(state), encoding="utf-8")

    assert load_active(store) is None
    assert due(store, NOW) is True
    assert store.active_identity() is None
    assert "If-None-Match" not in store.request_headers()
    endpoint.client.close()


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


def test_legacy_flat_cache_is_readable_and_migrates_on_activation(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    legacy_xml = store.active_path.read_bytes()
    legacy_meta = read_json(store.meta_path)
    legacy_state = read_json(store.state_path)
    legacy_meta.pop("source_epoch")
    legacy_state.pop("source_epoch")
    store.active_generation_path.unlink()
    store.source_transition_path.unlink()
    store._legacy_active_path.write_bytes(legacy_xml)
    store._legacy_meta_path.write_text(json.dumps(legacy_meta), encoding="utf-8")
    store.state_path.write_text(json.dumps(legacy_state), encoding="utf-8")

    assert load_active(store) is not None
    assert store.active_path == store._legacy_active_path

    source.queue(ok_response(EXTENDED_XML, ETAG_B))
    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)

    assert outcome is RefreshOutcome.ACTIVATED
    assert store.active_generation_path.exists()
    assert store.active_path.read_bytes() == EXTENDED_XML
    assert not store._legacy_active_path.exists()
    assert not store._legacy_meta_path.exists()


def test_legacy_cache_is_fenced_before_a_source_transition(tmp_path: Path) -> None:
    store_a, _, _ = activated_store(tmp_path)
    legacy_xml = store_a.active_path.read_bytes()
    legacy_meta = read_json(store_a.meta_path)
    legacy_state = read_json(store_a.state_path)
    legacy_meta.pop("source_epoch")
    legacy_state.pop("source_epoch")
    legacy_state.pop("refresh_attempt_id")
    store_a.active_generation_path.unlink()
    store_a.source_transition_path.unlink()
    store_a._legacy_active_path.write_bytes(legacy_xml)
    store_a._legacy_meta_path.write_text(json.dumps(legacy_meta), encoding="utf-8")
    store_a.state_path.write_text(json.dumps(legacy_state), encoding="utf-8")

    source_b_url = f"{BASE_ORIGIN}/Anime-Lists/legacy-transition/anime-list-full.xml"
    store_b = MappingStore(store_a.directory, source_url=source_b_url)
    source_b = ScriptedSource()
    source_b.queue(status_response(503))
    assert (
        refresh(store_b, endpoint_for(source_b, store_b), now=LATER, refresh_hours=HOURS)
        is RefreshOutcome.NO_ACTIVE_BUT_FAILED
    )

    store_a.state_path.unlink()
    store_a.source_transition_path.unlink()
    assert load_active(MappingStore(store_a.directory, source_url=SOURCE_URL)) is None


def test_legacy_transition_fence_survives_changed_overrides_and_state_loss(
    tmp_path: Path,
) -> None:
    store_a = make_store(tmp_path)
    source_a = ScriptedSource()
    source_a.queue(ok_response(SMALL_XML, ETAG_A))
    endpoint_a = endpoint_for(source_a, store_a)
    assert (
        refresh(
            store_a,
            endpoint_a,
            now=NOW,
            refresh_hours=HOURS,
            overrides_text=OVERRIDE_ADD_GAP,
        )
        is RefreshOutcome.ACTIVATED
    )
    legacy_xml = store_a.active_path.read_bytes()
    legacy_meta = read_json(store_a.meta_path)
    store_a.active_generation_path.unlink()
    store_a._legacy_active_path.write_bytes(legacy_xml)
    store_a._legacy_meta_path.write_text(json.dumps(legacy_meta), encoding="utf-8")

    source_b_url = f"{BASE_ORIGIN}/Anime-Lists/changed-overrides/anime-list-full.xml"
    store_b = MappingStore(store_a.directory, source_url=source_b_url)
    source_b = ScriptedSource()
    source_b.queue(status_response(503))
    assert (
        refresh(store_b, endpoint_for(source_b, store_b), now=LATER, refresh_hours=HOURS)
        is RefreshOutcome.NO_ACTIVE_BUT_FAILED
    )
    store_b.state_path.unlink()
    store_b.source_transition_path.unlink()

    reopened_a = MappingStore(store_a.directory, source_url=SOURCE_URL)
    assert load_active(reopened_a, load_overrides(OVERRIDE_ADD_GAP)) is None


@pytest.mark.parametrize("count_field", ["entries_indexed", "mapping_rules", "explicit_pairs"])
def test_legacy_transition_refuses_metadata_counts_that_disagree_with_xml(
    tmp_path: Path, count_field: str
) -> None:
    store_a, _, _ = activated_store(tmp_path)
    legacy_xml = store_a.active_path.read_bytes()
    legacy_meta = read_json(store_a.meta_path)
    legacy_meta[count_field] += 1
    store_a.active_generation_path.unlink()
    store_a._legacy_active_path.write_bytes(legacy_xml)
    store_a._legacy_meta_path.write_text(json.dumps(legacy_meta), encoding="utf-8")

    source_b_url = f"{BASE_ORIGIN}/Anime-Lists/bad-legacy-counts/anime-list-full.xml"
    store_b = MappingStore(store_a.directory, source_url=source_b_url)
    source_b = ScriptedSource()
    source_b.queue(status_response(503))
    outcome = refresh(
        store_b,
        endpoint_for(source_b, store_b),
        now=LATER,
        refresh_hours=HOURS,
        overrides_text=OVERRIDE_ADD_GAP,
    )

    assert outcome is RefreshOutcome.NO_ACTIVE_BUT_FAILED
    assert source_b.requests
    assert not store_a.active_generation_path.exists()


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


def test_due_holds_generation_lock_across_source_and_backoff_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_a, _, _ = activated_store(tmp_path)
    source_b_url = f"{BASE_ORIGIN}/Anime-Lists/due-race/anime-list-full.xml"
    store_b = MappingStore(store_a.directory, source_url=source_b_url)
    source_b = ScriptedSource()
    source_b.queue(status_response(503))
    endpoint_b = endpoint_for(source_b, store_b)
    original_load_state = store_a._load_state
    transition_outcomes: list[RefreshOutcome] = []
    attempted_transition = False

    def attempt_transition_while_due_reads_state() -> Any:
        nonlocal attempted_transition
        if not attempted_transition:
            attempted_transition = True
            transition_outcomes.append(refresh(store_b, endpoint_b, now=LATER, refresh_hours=HOURS))
        return original_load_state()

    monkeypatch.setattr(store_a, "_load_state", attempt_transition_while_due_reads_state)

    assert due(store_a, LATER) is False
    assert transition_outcomes == [RefreshOutcome.FAILED_LOCKED]
    assert store_a._load_state().source_url == SOURCE_URL


def test_lost_state_cannot_reauthorize_old_generation_after_switching_back(
    tmp_path: Path,
) -> None:
    store_a, _, _ = activated_store(tmp_path)
    source_b_url = f"{BASE_ORIGIN}/Anime-Lists/failed-then-returned/anime-list-full.xml"
    store_b = MappingStore(store_a.directory, source_url=source_b_url)
    source_b = ScriptedSource()
    source_b.queue(status_response(503))
    endpoint_b = endpoint_for(source_b, store_b)
    assert (
        refresh(store_b, endpoint_b, now=LATER, refresh_hours=HOURS)
        is RefreshOutcome.NO_ACTIVE_BUT_FAILED
    )

    store_a_again = MappingStore(store_a.directory, source_url=SOURCE_URL)
    source_a_again = ScriptedSource()
    source_a_again.queue(status_response(503))
    endpoint_a_again = endpoint_for(source_a_again, store_a_again)
    assert (
        refresh(
            store_a_again,
            endpoint_a_again,
            now=LATER + timedelta(hours=1),
            refresh_hours=HOURS,
            force=True,
        )
        is RefreshOutcome.NO_ACTIVE_BUT_FAILED
    )
    store_a_again.state_path.unlink()

    reopened_a = MappingStore(store_a.directory, source_url=SOURCE_URL)
    assert load_active(reopened_a) is None


def test_lost_state_default_source_transition_fences_nondefault_generation(
    tmp_path: Path,
) -> None:
    source_a_url = f"{BASE_ORIGIN}/Anime-Lists/nondefault-before-default/anime-list-full.xml"
    store_a = MappingStore(tmp_path / "mappings", source_url=source_a_url)
    source_a = ScriptedSource()
    source_a.queue(ok_response(SMALL_XML, ETAG_A))
    assert (
        refresh(store_a, endpoint_for(source_a, store_a), now=NOW, refresh_hours=HOURS)
        is RefreshOutcome.ACTIVATED
    )
    store_a.state_path.unlink()

    store_default = MappingStore(store_a.directory, source_url=DEFAULT_MAPPING_SOURCE_URL)
    source_default = ScriptedSource()
    source_default.queue(status_response(503))
    assert (
        refresh(
            store_default,
            endpoint_for(source_default, store_default),
            now=LATER,
            refresh_hours=HOURS,
        )
        is RefreshOutcome.NO_ACTIVE_BUT_FAILED
    )
    store_default.state_path.unlink()

    assert load_active(MappingStore(store_a.directory, source_url=source_a_url)) is None


def test_default_source_switchback_stays_fenced_after_state_and_marker_loss(
    tmp_path: Path,
) -> None:
    store_a, _, _ = activated_store(tmp_path)
    store_a.state_path.unlink()
    source_b_url = f"{BASE_ORIGIN}/Anime-Lists/default-switchback/anime-list-full.xml"
    store_b = MappingStore(store_a.directory, source_url=source_b_url)
    source_b = ScriptedSource()
    source_b.queue(status_response(503))
    assert (
        refresh(store_b, endpoint_for(source_b, store_b), now=LATER, refresh_hours=HOURS)
        is RefreshOutcome.NO_ACTIVE_BUT_FAILED
    )
    store_b.state_path.unlink()
    store_b.source_transition_path.unlink()

    assert load_active(MappingStore(store_a.directory, source_url=SOURCE_URL)) is None


def test_source_transition_marker_survives_lost_state_after_switchback(
    tmp_path: Path,
) -> None:
    store_a, _, _ = activated_store(tmp_path)
    source_b_url = f"{BASE_ORIGIN}/Anime-Lists/marker-switchback/anime-list-full.xml"
    store_b = MappingStore(store_a.directory, source_url=source_b_url)
    source_b = ScriptedSource()
    source_b.queue(status_response(503))
    assert (
        refresh(store_b, endpoint_for(source_b, store_b), now=LATER, refresh_hours=HOURS)
        is RefreshOutcome.NO_ACTIVE_BUT_FAILED
    )

    store_a_again = MappingStore(store_a.directory, source_url=SOURCE_URL)
    source_a = ScriptedSource()
    source_a.queue(status_response(503))
    assert (
        refresh(
            store_a_again,
            endpoint_for(source_a, store_a_again),
            now=LATER + timedelta(hours=1),
            refresh_hours=HOURS,
            force=True,
        )
        is RefreshOutcome.NO_ACTIVE_BUT_FAILED
    )
    store_a_again.state_path.unlink()

    reopened_a = MappingStore(store_a.directory, source_url=SOURCE_URL)
    assert load_active(reopened_a) is None
    reopened_a.source_transition_path.unlink()
    assert load_active(reopened_a) is None
    reopened_a.source_transition_path.write_text("{corrupt fence", encoding="utf-8")
    assert load_active(reopened_a) is None


def test_transition_marker_blocks_old_cache_if_source_state_write_is_interrupted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_a, _, _ = activated_store(tmp_path)
    source_b_url = f"{BASE_ORIGIN}/Anime-Lists/interrupted-transition/anime-list-full.xml"
    store_b = MappingStore(store_a.directory, source_url=source_b_url)
    source_b = ScriptedSource()
    endpoint_b = endpoint_for(source_b, store_b)

    def fail_state_write(state: Any) -> None:
        raise OSError("simulated state write interruption")

    monkeypatch.setattr(store_b, "_save_state", fail_state_write)
    with pytest.raises(OSError, match="state write interruption"):
        refresh(store_b, endpoint_b, now=LATER, refresh_hours=HOURS)

    assert source_b.requests == []
    assert store_b.source_transition_path.exists()
    assert load_active(MappingStore(store_a.directory, source_url=source_b_url)) is None
    assert load_active(MappingStore(store_a.directory, source_url=SOURCE_URL)) is None


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


def test_corrupt_state_file_recovers_committed_generation_snapshot(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    store.state_path.write_text("{not json", encoding="utf-8")
    assert due(store, NOW) is False
    assert status(store, NOW).consecutive_failures == NO_ENTRIES
    assert status(store, NOW).revision == REVISION_ONE
    headers = store.request_headers()
    assert headers["If-None-Match"] == ETAG_A
    outcome = refresh(store, endpoint, now=NOW, refresh_hours=HOURS)
    assert outcome is RefreshOutcome.NOT_DUE
    assert len(source.requests) == 1


def test_state_with_bad_fields_recovers_committed_generation_snapshot(tmp_path: Path) -> None:
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
    assert due(store, NOW) is False
    assert store.request_headers() == {
        "Accept-Encoding": ACCEPT_ENCODING,
        "If-None-Match": ETAG_A,
    }
    view = status(store, NOW)
    assert view.consecutive_failures == NO_ENTRIES
    assert view.last_outcome is RefreshOutcome.ACTIVATED
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


def test_http_cached_provenance_requires_unconditional_https_recovery(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    meta = read_json(store.meta_path)
    meta["source_url"] = SOURCE_URL.replace("https://", "http://", 1)
    store.meta_path.write_text(json.dumps(meta), encoding="utf-8")
    assert load_active(store) is None
    source.queue(ok_response(SMALL_XML, ETAG_B))
    assert refresh(store, endpoint, now=LATER, refresh_hours=HOURS) is RefreshOutcome.ACTIVATED
    assert str(source.requests[-1].url) == SOURCE_URL
    assert "If-None-Match" not in source.requests[-1].headers
    assert load_active(store) is not None


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


@pytest.mark.parametrize(
    "location",
    [SOURCE_PATH, "https://evil.example/data.xml", "http://raw.githubusercontent.com/data.xml"],
)
def test_source_redirect_is_refused_and_cache_never_moves(tmp_path: Path, location: str) -> None:
    store, source, endpoint = activated_store(tmp_path)
    before_active = store.active_path.read_bytes()
    before_meta = store.meta_path.read_bytes()
    before_etag = store._load_state().etag
    source.queue(redirect_to(location))
    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)
    assert outcome is RefreshOutcome.FAILED_TRANSPORT
    assert store.active_path.read_bytes() == before_active
    assert store.meta_path.read_bytes() == before_meta
    assert store._load_state().etag == before_etag
    assert len(source.requests) == TWO_REQUESTS
    assert all(str(request.url) == SOURCE_URL for request in source.requests)


def _atomic_boundary_fault(
    store: MappingStore, fault: str, interrupted: list[bool]
) -> Callable[[Path, bytes | bytearray], None]:
    original_atomic_write = mapping_store_module._atomic_write

    def fail_atomic_write(path: Path, payload: bytes | bytearray) -> None:
        generation_file = store.generations_path in path.parents
        if (
            fault == "before_xml"
            and not interrupted[0]
            and generation_file
            and path.name == "active.xml"
        ):
            interrupted[0] = True
            raise OSError("simulated crash before staging XML")
        if (
            fault == "after_xml_stage"
            and not interrupted[0]
            and generation_file
            and path.name == "active.meta.json"
        ):
            interrupted[0] = True
            raise OSError("simulated crash after staging XML")
        if (
            fault == "before_pointer_publish"
            and not interrupted[0]
            and path == store.active_generation_path
        ):
            interrupted[0] = True
            raise OSError("simulated crash before publishing active generation")
        original_atomic_write(path, payload)

    return fail_atomic_write


def _partial_write_boundary_fault(
    store: MappingStore, fault: str, interrupted: list[bool]
) -> Callable[[Path, bytes | bytearray], int]:
    original_write_bytes = Path.write_bytes
    initial_files = store._active_files()
    initial_generation = None if initial_files is None else initial_files[2]

    def fail_during_write(path: Path, payload: bytes | bytearray) -> int:
        if fault == "during_xml_stage" and not interrupted[0] and path.name == "active.xml.new":
            interrupted[0] = True
            original_write_bytes(path, bytes(payload[: max(1, len(payload) // 2)]))
            raise OSError("simulated crash during XML staging")
        if (
            fault == "during_meta_stage"
            and not interrupted[0]
            and path.name == "active.meta.json.new"
        ):
            interrupted[0] = True
            original_write_bytes(path, bytes(payload[: max(1, len(payload) // 2)]))
            raise OSError("simulated crash during metadata staging")
        pointer_temp = store.active_generation_path.with_name(
            f"{store.active_generation_path.name}.new"
        )
        if fault == "during_pointer_publish" and not interrupted[0] and path == pointer_temp:
            interrupted[0] = True
            original_write_bytes(path, bytes(payload[: max(1, len(payload) // 2)]))
            raise OSError("simulated crash during active generation publication")
        state_temp = store.state_path.with_name(f"{store.state_path.name}.new")
        current_files = store._active_files()
        current_generation = None if current_files is None else current_files[2]
        if (
            fault == "during_bookkeeping"
            and not interrupted[0]
            and path == state_temp
            and current_generation != initial_generation
        ):
            interrupted[0] = True
            original_write_bytes(path, bytes(payload[: max(1, len(payload) // 2)]))
            raise OSError("simulated crash during activation bookkeeping")
        return original_write_bytes(path, payload)

    return fail_during_write


def _inject_activation_fault(
    store: MappingStore, monkeypatch: pytest.MonkeyPatch, fault: str
) -> list[bool]:
    interrupted = [False]
    if fault in {"before_xml", "after_xml_stage", "before_pointer_publish"}:
        monkeypatch.setattr(
            mapping_store_module,
            "_atomic_write",
            _atomic_boundary_fault(store, fault, interrupted),
        )
    elif fault in {
        "during_xml_stage",
        "during_meta_stage",
        "during_pointer_publish",
        "during_bookkeeping",
    }:
        monkeypatch.setattr(
            Path,
            "write_bytes",
            _partial_write_boundary_fault(store, fault, interrupted),
        )
    elif fault == "before_bookkeeping":
        initial_files = store._active_files()
        initial_generation = None if initial_files is None else initial_files[2]
        original_save_state = store._save_state

        def fail_before_bookkeeping(state: Any) -> None:
            current_files = store._active_files()
            current_generation = None if current_files is None else current_files[2]
            if current_generation != initial_generation:
                interrupted[0] = True
                raise OSError("simulated crash before activation bookkeeping")
            original_save_state(state)

        monkeypatch.setattr(store, "_save_state", fail_before_bookkeeping)
    else:
        previous_generation = store._active_files()
        previous_id = None if previous_generation is None else previous_generation[2]
        original_cleanup = store._cleanup_generations

        def fail_after_activation() -> None:
            files = store._active_files()
            if files is not None and files[2] != previous_id:
                interrupted[0] = True
                raise OSError("simulated crash after activation")
            original_cleanup()

        monkeypatch.setattr(store, "_cleanup_generations", fail_after_activation)
    return interrupted


@pytest.mark.parametrize(
    ("fault", "expected_xml"),
    [
        ("before_xml", SMALL_XML),
        ("during_xml_stage", SMALL_XML),
        ("after_xml_stage", SMALL_XML),
        ("during_meta_stage", SMALL_XML),
        ("before_pointer_publish", SMALL_XML),
        ("during_pointer_publish", SMALL_XML),
        ("before_bookkeeping", EXTENDED_XML),
        ("during_bookkeeping", EXTENDED_XML),
        ("after_activation", EXTENDED_XML),
    ],
)
def test_interrupted_generation_activation_recovers_consistently(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str, expected_xml: bytes
) -> None:
    store, source, endpoint = activated_store(tmp_path)
    interrupted = _inject_activation_fault(store, monkeypatch, fault)

    source.queue(ok_response(EXTENDED_XML, ETAG_B))
    with pytest.raises(OSError, match="simulated crash"):
        refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)

    assert interrupted == [True]
    reopened = MappingStore(store.directory, source_url=store.source_url)
    active = load_active(reopened)
    assert active is not None
    metadata = read_json(reopened.meta_path)
    state = read_json(reopened.state_path)
    assert reopened.active_path.read_bytes() == expected_xml
    assert metadata["content_sha256"] == content_digest(expected_xml)
    assert active.content_sha256 == metadata["content_sha256"]
    assert state["content_sha256"] == metadata["content_sha256"]
    assert status(reopened, LATER).active_present is True
    assert due(reopened, LATER) is False
    pointer = json.loads(reopened.active_generation_path.read_text(encoding="utf-8"))
    assert {path.name for path in reopened.generations_path.iterdir()} == {pointer["generation"]}
    assert not reopened._legacy_active_path.exists()
    assert not reopened._legacy_meta_path.exists()


def test_interrupted_source_change_before_pointer_never_authorizes_old_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_a, _, _ = activated_store(tmp_path)
    old_xml = store_a.active_path.read_bytes()
    source_b_url = f"{BASE_ORIGIN}/Anime-Lists/new-source/anime-list-full.xml"
    store_b = MappingStore(store_a.directory, source_url=source_b_url)
    source_b = ScriptedSource()
    source_b.queue(ok_response(EXTENDED_XML, ETAG_B))
    endpoint_b = endpoint_for(source_b, store_b)
    initial_files = store_a._active_files()
    assert initial_files is not None
    initial_generation = initial_files[2]
    real_atomic_write = mapping_store_module._atomic_write
    interrupted = False

    def fail_before_pointer(path: Path, payload: bytes | bytearray) -> None:
        nonlocal interrupted
        if path == store_b.active_generation_path:
            pointer = json.loads(payload)
            if (
                not pointer["state"]["source_change_pending"]
                and pointer["generation"] != initial_generation
            ):
                interrupted = True
                raise OSError("simulated source-change interruption before pointer")
        real_atomic_write(path, payload)

    monkeypatch.setattr(mapping_store_module, "_atomic_write", fail_before_pointer)
    with pytest.raises(OSError, match="source-change interruption"):
        refresh(store_b, endpoint_b, now=LATER, refresh_hours=HOURS)

    assert interrupted is True
    reopened_b = MappingStore(store_a.directory, source_url=source_b_url)
    assert load_active(reopened_b) is None
    assert reopened_b.active_path.read_bytes() == old_xml
    assert reopened_b._load_state().source_change_pending is True
    pointer = read_json(reopened_b.active_generation_path)
    assert {path.name for path in reopened_b.generations_path.iterdir()} == {pointer["generation"]}


def test_source_change_activation_recovers_only_its_committed_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store_a, _, _ = activated_store(tmp_path)
    source_b_url = f"{BASE_ORIGIN}/Anime-Lists/committed-source/anime-list-full.xml"
    store_b = MappingStore(store_a.directory, source_url=source_b_url)
    source_b = ScriptedSource()
    source_b.queue(ok_response(EXTENDED_XML, ETAG_B))
    endpoint_b = endpoint_for(source_b, store_b)
    real_save_state = store_b._save_state
    save_count = 0

    def fail_activation_bookkeeping(state: Any) -> None:
        nonlocal save_count
        save_count += 1
        if save_count == 3:
            raise OSError("simulated source-change bookkeeping interruption")
        real_save_state(state)

    monkeypatch.setattr(store_b, "_save_state", fail_activation_bookkeeping)
    with pytest.raises(OSError, match="bookkeeping interruption"):
        refresh(store_b, endpoint_b, now=LATER, refresh_hours=HOURS)

    assert save_count == 3
    reopened_b = MappingStore(store_a.directory, source_url=source_b_url)
    active_b = load_active(reopened_b)
    assert active_b is not None
    assert reopened_b.active_path.read_bytes() == EXTENDED_XML
    assert read_json(reopened_b.meta_path)["source_url"] == source_b_url
    assert reopened_b._load_state().source_change_pending is False
    assert load_active(MappingStore(store_a.directory, source_url=SOURCE_URL)) is None


def test_active_identity_distinguishes_a_reactivated_source_epoch(tmp_path: Path) -> None:
    store_a, _, _ = activated_store(tmp_path)
    first_active = load_active(store_a)
    first_identity = store_a.active_identity()
    assert first_active is not None
    assert first_identity is not None

    source_b_url = f"{BASE_ORIGIN}/Anime-Lists/identity-epoch/anime-list-full.xml"
    store_b = MappingStore(store_a.directory, source_url=source_b_url)
    source_b = ScriptedSource()
    source_b.queue(ok_response(EXTENDED_XML, ETAG_B))
    assert (
        refresh(store_b, endpoint_for(source_b, store_b), now=LATER, refresh_hours=HOURS)
        is RefreshOutcome.ACTIVATED
    )

    store_a_again = MappingStore(store_a.directory, source_url=SOURCE_URL)
    source_a_again = ScriptedSource()
    source_a_again.queue(ok_response(SMALL_XML, '"returning-a"'))
    assert (
        refresh(
            store_a_again,
            endpoint_for(source_a_again, store_a_again),
            now=LATER + timedelta(hours=1),
            refresh_hours=HOURS,
        )
        is RefreshOutcome.ACTIVATED
    )
    second_active = load_active(store_a_again)
    second_identity = store_a_again.active_identity()

    assert second_active is not None
    assert second_active.index == first_active.index
    assert second_identity is not None
    assert second_identity[:4] != first_identity[:4]
    assert second_identity[4] != first_identity[4]


def test_incomplete_state_is_recovered_for_a_nondefault_mapping_source(tmp_path: Path) -> None:
    source_url = f"{BASE_ORIGIN}/Anime-Lists/nondefault/anime-list-full.xml"
    store = MappingStore(tmp_path / "mappings", source_url=source_url)
    source = ScriptedSource()
    source.queue(ok_response(SMALL_XML, ETAG_B))
    endpoint = endpoint_for(source, store)
    assert refresh(store, endpoint, now=NOW, refresh_hours=HOURS) is RefreshOutcome.ACTIVATED
    store.state_path.write_text("{}", encoding="utf-8")

    reopened = MappingStore(store.directory, source_url=source_url)
    active = load_active(reopened)

    assert active is not None
    assert active.source_url == source_url
    assert reopened._load_state().source_url == source_url
    assert reopened.request_headers()["If-None-Match"] == ETAG_B


def test_direct_refresh_after_nondefault_state_loss_keeps_committed_generation(
    tmp_path: Path,
) -> None:
    source_url = f"{BASE_ORIGIN}/Anime-Lists/direct-recovery/anime-list-full.xml"
    store = MappingStore(tmp_path / "mappings", source_url=source_url)
    source = ScriptedSource()
    source.queue(ok_response(SMALL_XML, ETAG_B))
    endpoint = endpoint_for(source, store)
    assert refresh(store, endpoint, now=NOW, refresh_hours=HOURS) is RefreshOutcome.ACTIVATED
    store.state_path.unlink()
    source.queue(status_response(503))
    reopened = MappingStore(store.directory, source_url=source_url)
    reopened_endpoint = endpoint_for(source, reopened)

    outcome = refresh(
        reopened,
        reopened_endpoint,
        now=LATER,
        refresh_hours=HOURS,
        force=True,
    )

    assert outcome is RefreshOutcome.FAILED_TRANSPORT
    assert load_active(reopened) is not None
    assert reopened._load_state().consecutive_failures == ONE_FAILURE


def test_status_recovers_committed_generation_created_with_overrides(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, source, endpoint = activated_store(tmp_path)
    initial_files = store._active_files()
    initial_generation = None if initial_files is None else initial_files[2]
    original_save_state = store._save_state

    def fail_state_write(state: Any) -> None:
        current_files = store._active_files()
        current_generation = None if current_files is None else current_files[2]
        if current_generation != initial_generation:
            raise OSError("simulated override activation bookkeeping interruption")
        original_save_state(state)

    monkeypatch.setattr(store, "_save_state", fail_state_write)
    source.queue(ok_response(SMALL_XML, ETAG_B))
    with pytest.raises(OSError, match="override activation bookkeeping"):
        refresh(
            store,
            endpoint,
            now=LATER,
            refresh_hours=HOURS,
            force=True,
            overrides_text=OVERRIDE_ADD_GAP,
        )

    reopened = MappingStore(store.directory, source_url=store.source_url)
    view = status(reopened, LATER, overrides_text=OVERRIDE_ADD_GAP)

    assert view.active_present is True
    assert view.revision == REVISION_TWO
    assert view.last_success_iso == iso(LATER)


def test_state_loss_after_failed_refresh_recovers_latest_backoff(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    source.queue(status_response(503))
    assert (
        refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)
        is RefreshOutcome.FAILED_TRANSPORT
    )
    expected_due = LATER + timedelta(hours=FIRST_BACKOFF_HOURS)
    assert due(store, LATER + timedelta(minutes=30)) is False
    store.state_path.unlink()

    reopened = MappingStore(store.directory, source_url=store.source_url)
    assert load_active(reopened) is not None

    state = read_json(reopened.state_path)
    assert state["consecutive_failures"] == ONE_FAILURE
    assert state["next_due_iso"] == iso(expected_due)
    assert due(reopened, LATER + timedelta(minutes=30)) is False


def test_pending_source_backoff_survives_state_loss(tmp_path: Path) -> None:
    store_a, _, _ = activated_store(tmp_path)
    source_b_url = f"{BASE_ORIGIN}/Anime-Lists/pending-backoff/anime-list-full.xml"
    store_b = MappingStore(store_a.directory, source_url=source_b_url)
    source_b = ScriptedSource()
    source_b.queue(status_response(503))
    assert (
        refresh(
            store_b,
            endpoint_for(source_b, store_b),
            now=LATER,
            refresh_hours=HOURS,
        )
        is RefreshOutcome.NO_ACTIVE_BUT_FAILED
    )
    store_b.state_path.unlink()

    reopened_b = MappingStore(store_a.directory, source_url=source_b_url)
    retry_source = ScriptedSource()
    retry_endpoint = endpoint_for(retry_source, reopened_b)
    outcome = refresh(
        reopened_b,
        retry_endpoint,
        now=LATER + timedelta(minutes=30),
        refresh_hours=HOURS,
    )

    assert outcome is RefreshOutcome.NOT_DUE
    assert retry_source.requests == []


def test_initial_default_failure_backoff_survives_state_loss(tmp_path: Path) -> None:
    store = make_store(tmp_path)
    source = ScriptedSource()
    source.queue(status_response(503))
    assert (
        refresh(store, endpoint_for(source, store), now=NOW, refresh_hours=HOURS)
        is RefreshOutcome.NO_ACTIVE_BUT_FAILED
    )
    store.state_path.unlink()

    reopened = MappingStore(store.directory, source_url=store.source_url)
    retry_source = ScriptedSource()
    outcome = refresh(
        reopened,
        endpoint_for(retry_source, reopened),
        now=NOW + timedelta(minutes=30),
        refresh_hours=HOURS,
    )

    assert outcome is RefreshOutcome.NOT_DUE
    assert retry_source.requests == []


@pytest.mark.parametrize("next_due_mode", ["missing", "null"])
def test_pending_transition_with_incomplete_scheduler_snapshot_uses_state_backoff(
    tmp_path: Path, next_due_mode: str
) -> None:
    source_url = f"{BASE_ORIGIN}/Anime-Lists/incomplete-snapshot/anime-list-full.xml"
    store = MappingStore(tmp_path / "mappings", source_url=source_url)
    source = ScriptedSource()
    source.queue(status_response(503))
    assert (
        refresh(store, endpoint_for(source, store), now=NOW, refresh_hours=HOURS)
        is RefreshOutcome.NO_ACTIVE_BUT_FAILED
    )
    transition = read_json(store.source_transition_path)
    if next_due_mode == "missing":
        transition["scheduler_state"].pop("next_due_iso")
    else:
        transition["scheduler_state"]["next_due_iso"] = None
    store.source_transition_path.write_text(json.dumps(transition), encoding="utf-8")

    retry_source = ScriptedSource()
    reopened = MappingStore(store.directory, source_url=source_url)
    outcome = refresh(
        reopened,
        endpoint_for(retry_source, reopened),
        now=NOW + timedelta(minutes=30),
        refresh_hours=HOURS,
    )

    assert outcome is RefreshOutcome.NOT_DUE
    assert retry_source.requests == []


def test_incomplete_default_transition_snapshot_fails_closed_after_state_loss(
    tmp_path: Path,
) -> None:
    store = make_store(tmp_path)
    source = ScriptedSource()
    source.queue(status_response(503))
    assert (
        refresh(store, endpoint_for(source, store), now=NOW, refresh_hours=HOURS)
        is RefreshOutcome.NO_ACTIVE_BUT_FAILED
    )
    transition = read_json(store.source_transition_path)
    transition["scheduler_state"].pop("next_due_iso")
    store.source_transition_path.write_text(json.dumps(transition), encoding="utf-8")
    store.state_path.unlink()

    reopened = MappingStore(store.directory, source_url=store.source_url)
    retry_source = ScriptedSource()
    outcome = refresh(
        reopened,
        endpoint_for(retry_source, reopened),
        now=NOW + timedelta(minutes=30),
        refresh_hours=HOURS,
    )

    assert outcome is RefreshOutcome.FAILED_LOCKED
    assert retry_source.requests == []


def test_status_recovers_pending_source_backoff_after_state_loss(tmp_path: Path) -> None:
    store_a, _, _ = activated_store(tmp_path)
    source_b_url = f"{BASE_ORIGIN}/Anime-Lists/status-pending-backoff/anime-list-full.xml"
    store_b = MappingStore(store_a.directory, source_url=source_b_url)
    source_b = ScriptedSource()
    source_b.queue(status_response(503))
    assert (
        refresh(
            store_b,
            endpoint_for(source_b, store_b),
            now=LATER,
            refresh_hours=HOURS,
        )
        is RefreshOutcome.NO_ACTIVE_BUT_FAILED
    )
    store_b.state_path.unlink()

    view = status(
        MappingStore(store_a.directory, source_url=source_b_url),
        LATER + timedelta(minutes=30),
    )

    assert view.active_present is False
    assert view.consecutive_failures == ONE_FAILURE
    assert view.next_due_iso == iso(LATER + timedelta(hours=FIRST_BACKOFF_HOURS))


def test_state_loss_after_304_recovers_latest_validator_and_schedule(tmp_path: Path) -> None:
    store, source, endpoint = activated_store(tmp_path)
    source.queue(not_modified(ETAG_B))
    assert (
        refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)
        is RefreshOutcome.UNCHANGED
    )
    expected_due = LATER + timedelta(hours=HOURS)
    store.state_path.unlink()

    reopened = MappingStore(store.directory, source_url=store.source_url)
    assert load_active(reopened) is not None

    state = read_json(reopened.state_path)
    assert state["etag"] == ETAG_B
    assert state["next_due_iso"] == iso(expected_due)
    assert reopened.request_headers()["If-None-Match"] == ETAG_B
    assert due(reopened, LATER + timedelta(minutes=30)) is False


def test_first_activation_recovers_scheduler_snapshot_after_state_write_interruption(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = make_store(tmp_path)
    source = ScriptedSource()
    source.queue(ok_response(SMALL_XML, ETAG_A))
    endpoint = endpoint_for(source, store)
    original_save_state = store._save_state
    initial_files = store._active_files()
    initial_generation = None if initial_files is None else initial_files[2]

    def fail_state_write(state: Any) -> None:
        current_files = store._active_files()
        current_generation = None if current_files is None else current_files[2]
        if current_generation != initial_generation:
            raise OSError("simulated first-install bookkeeping interruption")
        original_save_state(state)

    monkeypatch.setattr(store, "_save_state", fail_state_write)
    with pytest.raises(OSError, match="first-install bookkeeping"):
        refresh(store, endpoint, now=NOW, refresh_hours=HOURS)

    reopened = MappingStore(store.directory, source_url=store.source_url)
    assert load_active(reopened) is not None
    assert status(reopened, NOW).active_present is True
    assert due(reopened, NOW) is False
    assert reopened.request_headers()["If-None-Match"] == ETAG_A


def test_delayed_response_cannot_revert_a_newer_source_transition_epoch(
    tmp_path: Path,
) -> None:
    store_a, _, _ = activated_store(tmp_path)
    store_a_again = MappingStore(store_a.directory, source_url=SOURCE_URL)
    source_b_url = f"{BASE_ORIGIN}/Anime-Lists/interleaved-source/anime-list-full.xml"
    store_b = MappingStore(store_a.directory, source_url=source_b_url)
    source_b = ScriptedSource()
    endpoint_b = endpoint_for(source_b, store_b)
    source_a_again = ScriptedSource()
    endpoint_a_again = endpoint_for(source_a_again, store_a_again)
    delayed_source = ScriptedSource()
    latest_scheduler_state: list[dict[str, Any]] = []

    def delayed_response(request: httpx.Request) -> httpx.Response:
        source_b.queue(ok_response(EXTENDED_XML, ETAG_B))
        assert (
            refresh(store_b, endpoint_b, now=LATER, refresh_hours=HOURS) is RefreshOutcome.ACTIVATED
        )
        source_a_again.queue(ok_response(EXTENDED_XML, '"returning-a"'))
        assert (
            refresh(
                store_a_again,
                endpoint_a_again,
                now=LATER + timedelta(hours=1),
                refresh_hours=HOURS,
            )
            is RefreshOutcome.ACTIVATED
        )
        latest_scheduler_state.append(read_json(store_a.state_path))
        return ok_response(SMALL_XML, '"stale-a"')(request)

    delayed_source.queue(delayed_response)
    delayed_endpoint = endpoint_for(delayed_source, store_a)
    outcome = refresh(
        store_a,
        delayed_endpoint,
        now=LATER + timedelta(hours=2),
        refresh_hours=HOURS,
        force=True,
    )

    assert outcome is not RefreshOutcome.ACTIVATED
    active = load_active(store_a)
    assert active is not None
    assert store_a.active_path.read_bytes() == EXTENDED_XML
    metadata = store_a._load_meta()
    assert metadata is not None
    assert store_a._load_state().source_epoch == metadata.source_epoch
    assert read_json(store_a.state_path) == latest_scheduler_state[0]


def test_stale_transport_failure_does_not_replace_newer_refresh_schedule(
    tmp_path: Path,
) -> None:
    store, _, _ = activated_store(tmp_path)
    nested_store = MappingStore(store.directory, source_url=store.source_url)
    nested_source = ScriptedSource()
    nested_endpoint = endpoint_for(nested_source, nested_store)
    delayed_source = ScriptedSource()
    newer_state: list[dict[str, Any]] = []

    def delayed_failure(request: httpx.Request) -> httpx.Response:
        nested_source.queue(not_modified(ETAG_B))
        assert (
            refresh(
                nested_store,
                nested_endpoint,
                now=LATER,
                refresh_hours=HOURS,
                force=True,
            )
            is RefreshOutcome.UNCHANGED
        )
        newer_state.append(read_json(store.state_path))
        return status_response(503)(request)

    delayed_source.queue(delayed_failure)
    delayed_endpoint = endpoint_for(delayed_source, store)
    outcome = refresh(
        store,
        delayed_endpoint,
        now=LATER + timedelta(hours=1),
        refresh_hours=HOURS,
        force=True,
    )

    assert outcome is RefreshOutcome.FAILED_STALE
    assert read_json(store.state_path) == newer_state[0]


def _leave_pointer_before_state_bookkeeping(
    store: MappingStore,
    source: ScriptedSource,
    endpoint: ApiEndpoint,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with monkeypatch.context() as fault_patch:
        _inject_activation_fault(store, fault_patch, "before_bookkeeping")
        source.queue(ok_response(EXTENDED_XML, ETAG_B))
        with pytest.raises(OSError, match="activation bookkeeping"):
            refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)


def _start_paused_recovery(
    store: MappingStore, monkeypatch: pytest.MonkeyPatch
) -> tuple[Event, Event, list[Any], list[BaseException], Thread]:
    reader_store = MappingStore(store.directory, source_url=store.source_url)
    original_load_state = reader_store._load_state
    state_read = Event()
    continue_recovery = Event()
    first_read = True

    def pause_recovery_state_read() -> Any:
        nonlocal first_read
        state = original_load_state()
        if first_read:
            first_read = False
            state_read.set()
            if not continue_recovery.wait(timeout=10):
                raise TimeoutError("test did not release mapping recovery")
        return state

    monkeypatch.setattr(reader_store, "_load_state", pause_recovery_state_read)
    reader_result: list[Any] = []
    reader_errors: list[BaseException] = []

    def run_reader() -> None:
        try:
            reader_result.append(load_active(reader_store))
        except BaseException as error:
            reader_errors.append(error)

    reader_thread = Thread(target=run_reader)
    reader_thread.start()
    return state_read, continue_recovery, reader_result, reader_errors, reader_thread


def _start_concurrent_activation(
    store: MappingStore, monkeypatch: pytest.MonkeyPatch
) -> tuple[Event, list[RefreshOutcome], list[BaseException], Thread]:
    writer_store = MappingStore(store.directory, source_url=store.source_url)
    writer_source = ScriptedSource()
    writer_source.queue(ok_response(SMALL_XML, '"newest"'))
    writer_endpoint = endpoint_for(writer_source, writer_store)
    real_save_state = writer_store._save_state
    newest_state_saved = Event()

    def record_newest_state(state: Any) -> None:
        real_save_state(state)
        if state.content_sha256 == content_digest(SMALL_XML):
            newest_state_saved.set()

    monkeypatch.setattr(writer_store, "_save_state", record_newest_state)
    writer_result: list[RefreshOutcome] = []
    writer_errors: list[BaseException] = []

    def run_writer() -> None:
        try:
            writer_result.append(
                refresh(
                    writer_store,
                    writer_endpoint,
                    now=LATER + timedelta(hours=1),
                    refresh_hours=HOURS,
                    force=True,
                )
            )
        except BaseException as error:
            writer_errors.append(error)

    writer_thread = Thread(target=run_writer)
    writer_thread.start()
    return newest_state_saved, writer_result, writer_errors, writer_thread


def test_pointer_state_recovery_cannot_overwrite_a_concurrent_activation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, source, endpoint = activated_store(tmp_path)
    _leave_pointer_before_state_bookkeeping(store, source, endpoint, monkeypatch)
    state_read, continue_recovery, reader_result, reader_errors, reader_thread = (
        _start_paused_recovery(store, monkeypatch)
    )
    assert state_read.wait(timeout=5)
    newest_state_saved, writer_result, writer_errors, writer_thread = _start_concurrent_activation(
        store, monkeypatch
    )
    writer_thread.join(timeout=5)
    committed_before_recovery = newest_state_saved.is_set()
    continue_recovery.set()
    reader_thread.join(timeout=5)
    writer_thread.join(timeout=5)

    assert not reader_thread.is_alive()
    assert not writer_thread.is_alive()
    assert reader_errors == []
    assert writer_errors == []
    assert committed_before_recovery is False
    assert reader_result[0] is not None
    writer_store = MappingStore(store.directory, source_url=store.source_url)
    active_meta = read_json(writer_store.meta_path)
    persisted_state = read_json(writer_store.state_path)
    assert persisted_state["content_sha256"] == active_meta["content_sha256"]


def _start_cleanup_writer(
    store: MappingStore,
    old_generation: Path,
    old_generation_removed: Event,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Event, list[RefreshOutcome], Thread]:
    writer_store = MappingStore(store.directory, source_url=store.source_url)
    writer_source = ScriptedSource()
    writer_source.queue(ok_response(EXTENDED_XML, ETAG_B))
    writer_endpoint = endpoint_for(writer_source, writer_store)
    original_cleanup = writer_store._cleanup_generations
    writer_lock_attempted = Event()
    original_generation_lock = writer_store.generation_lock

    @contextmanager
    def observe_writer_lock(*, wait_seconds: float = 0.0) -> Iterator[bool]:
        writer_lock_attempted.set()
        with original_generation_lock(wait_seconds=wait_seconds) as acquired:
            yield acquired

    monkeypatch.setattr(writer_store, "generation_lock", observe_writer_lock)

    def observe_cleanup() -> None:
        original_cleanup()
        if not old_generation.exists():
            old_generation_removed.set()

    monkeypatch.setattr(writer_store, "_cleanup_generations", observe_cleanup)
    writer_results: list[RefreshOutcome] = []
    writer_thread = Thread(
        target=lambda: writer_results.append(
            refresh(
                writer_store,
                writer_endpoint,
                now=LATER,
                refresh_hours=HOURS,
                force=True,
            )
        )
    )
    writer_thread.start()
    return writer_lock_attempted, writer_results, writer_thread


def test_active_generation_read_is_fenced_from_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, _, _ = activated_store(tmp_path)
    files = store._active_files()
    assert files is not None
    assert files[2] is not None
    old_generation = store.generations_path / files[2]
    old_metadata_path = old_generation / "active.meta.json"
    original_read_text = Path.read_text
    selected_files = Event()
    continue_read = Event()
    old_generation_removed = Event()

    def pause_selected_metadata(path: Path, *args: Any, **kwargs: Any) -> str:
        if path == old_metadata_path:
            selected_files.set()
            if not continue_read.wait(timeout=10):
                raise TimeoutError("test did not release selected generation read")
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", pause_selected_metadata)
    reader_results: list[Any] = []
    reader_thread = Thread(target=lambda: reader_results.append(load_active(store)))
    reader_thread.start()
    assert selected_files.wait(timeout=5)
    writer_lock_attempted, writer_results, writer_thread = _start_cleanup_writer(
        store, old_generation, old_generation_removed, monkeypatch
    )
    assert writer_lock_attempted.wait(timeout=5)
    removed_before_reader_finished = old_generation_removed.is_set()
    continue_read.set()
    reader_thread.join(timeout=5)
    writer_thread.join(timeout=5)

    assert not reader_thread.is_alive()
    assert not writer_thread.is_alive()
    assert removed_before_reader_finished is False
    assert reader_results[0] is not None
    assert reader_results[0].content_sha256 == content_digest(SMALL_XML)
    assert writer_results == [RefreshOutcome.ACTIVATED]
    assert old_generation_removed.is_set()


def test_load_active_retries_until_generation_lock_is_available(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, _, _ = activated_store(tmp_path)
    reader_store = MappingStore(store.directory, source_url=store.source_url)
    monkeypatch.setattr(mapping_store_module, "GENERATION_LOCK_REFRESH_WAIT_SECONDS", 0.02)
    lock_held = Event()
    release_lock = Event()
    second_attempt = Event()

    def hold_generation_lock() -> None:
        with store.generation_lock(wait_seconds=0.0) as acquired:
            assert acquired
            lock_held.set()
            if not release_lock.wait(timeout=5):
                raise TimeoutError("test did not release mapping generation lock")

    holder = Thread(target=hold_generation_lock)
    holder.start()
    assert lock_held.wait(timeout=5)
    original_lock = reader_store.generation_lock
    lock_attempts = 0

    @contextmanager
    def observe_lock_retries(*, wait_seconds: float = 0.0) -> Iterator[bool]:
        nonlocal lock_attempts
        lock_attempts += 1
        if lock_attempts == 2:
            second_attempt.set()
        with original_lock(wait_seconds=wait_seconds) as acquired:
            yield acquired

    monkeypatch.setattr(reader_store, "generation_lock", observe_lock_retries)
    reader_results: list[Any] = []
    reader = Thread(target=lambda: reader_results.append(load_active(reader_store)))
    reader.start()
    assert second_attempt.wait(timeout=5)
    release_lock.set()
    reader.join(timeout=5)
    holder.join(timeout=5)

    assert not reader.is_alive()
    assert not holder.is_alive()
    assert reader_results[0] is not None


def test_status_snapshot_is_fenced_from_concurrent_activation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, _, _ = activated_store(tmp_path)
    files = store._active_files()
    assert files is not None
    assert files[2] is not None
    old_generation = store.generations_path / files[2]
    reader_store = MappingStore(store.directory, source_url=store.source_url)
    original_load_state = reader_store._load_state
    state_reads = 0
    selected_status_state = Event()
    continue_status = Event()

    def pause_status_state() -> Any:
        nonlocal state_reads
        state = original_load_state()
        state_reads += 1
        if state_reads == 2:
            selected_status_state.set()
            if not continue_status.wait(timeout=10):
                raise TimeoutError("test did not release status snapshot")
        return state

    monkeypatch.setattr(reader_store, "_load_state", pause_status_state)
    status_results: list[MappingStatus] = []
    reader_thread = Thread(target=lambda: status_results.append(status(reader_store, LATER)))
    reader_thread.start()
    assert selected_status_state.wait(timeout=5)

    old_generation_removed = Event()
    writer_lock_attempted, writer_results, writer_thread = _start_cleanup_writer(
        store, old_generation, old_generation_removed, monkeypatch
    )
    assert writer_lock_attempted.wait(timeout=5)
    removed_before_status_finished = old_generation_removed.wait(timeout=0.5)
    continue_status.set()
    reader_thread.join(timeout=5)
    writer_thread.join(timeout=5)

    assert not reader_thread.is_alive()
    assert not writer_thread.is_alive()
    assert removed_before_status_finished is False
    assert status_results[0].active_present is True
    assert writer_results == [RefreshOutcome.ACTIVATED]
    assert old_generation_removed.is_set()


@pytest.mark.parametrize("attribute", ["anidbid", "tvdbid", "defaulttvdbseason", "episodeoffset"])
@pytest.mark.parametrize("active_cache", [False, True])
def test_oversized_numeric_xml_records_failure_and_persists_retry_backoff(
    tmp_path: Path, attribute: str, active_cache: bool
) -> None:
    if active_cache:
        store, source, endpoint = activated_store(tmp_path)
    else:
        store = make_store(tmp_path)
        source = ScriptedSource()
        endpoint = endpoint_for(source, store)
    old_content = store.active_path.read_bytes() if active_cache else None
    attrs = {"anidbid": "1", "tvdbid": "100", "defaulttvdbseason": "1", "episodeoffset": "0"}
    attrs[attribute] = "9" * 4301
    attributes = " ".join(f'{key}="{value}"' for key, value in attrs.items())
    bad_xml = f"<anime-list><anime {attributes}/></anime-list>".encode()
    source.queue(ok_response(bad_xml, ETAG_B))

    outcome = refresh(store, endpoint, now=LATER, refresh_hours=HOURS, force=True)

    expected = (
        RefreshOutcome.FAILED_INVALID if active_cache else RefreshOutcome.NO_ACTIVE_BUT_FAILED
    )
    assert outcome is expected
    if active_cache:
        assert store.active_path.read_bytes() == old_content
    else:
        assert load_active(store) is None
    view = status(store, LATER)
    assert view.last_outcome is RefreshOutcome.FAILED_INVALID
    assert view.consecutive_failures == ONE_FAILURE
    assert view.next_due_iso == iso(LATER + timedelta(hours=FIRST_BACKOFF_HOURS))
    reopened = MappingStore(store.directory, source_url=store.source_url)
    retry = LATER + timedelta(minutes=30)
    assert not due(reopened, retry)
    request_count = len(source.requests)
    assert refresh(reopened, endpoint, now=retry, refresh_hours=HOURS) is RefreshOutcome.NOT_DUE
    assert len(source.requests) == request_count
