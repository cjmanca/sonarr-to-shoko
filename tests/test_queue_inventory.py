"""Pinned Shoko key semantics over mocked HTTP; no private server or remote writes."""

from __future__ import annotations

import gzip
import itertools
import json
from collections.abc import Callable, Iterator
from typing import Any

import httpx
import pytest
import queue_fixtures as qf

from shoko_linker.http import ApiEndpoint
from shoko_linker.queue_inventory import (
    InventoryReason,
    Presence,
    QueueInventory,
    QueueJobKind,
    QueueJobState,
    fetch_queue_inventory,
)

ANIME_KEY = "AniDB.GetAniDBAnimeJob_AnimeID:18208"
LINK_KEY = "Import.ManualLinkJob_VideoLocalID:149_EpisodeID:650"
ANIME_KIND = QueueJobKind.ANIME_METADATA
LINK_KIND = QueueJobKind.MANUAL_LINK
type Override = Callable[[httpx.Request], httpx.Response | None]


def _item(key: str = ANIME_KEY, state: QueueJobState = QueueJobState.WAITING) -> dict[str, Any]:
    return {
        "Key": key,
        "Type": "Get AniDB Anime Data" if key.startswith("AniDB.") else "Other Job",
        "Title": "Ignored display title",
        "Details": {"AnimeID": 99999, "VideoLocalID": 99999},
        "IsRunning": state is QueueJobState.RUNNING,
        "IsBlocked": state is QueueJobState.BLOCKED,
        "StartTime": "2026-10-28T00:00:00Z" if state is QueueJobState.RUNNING else None,
    }


def _summary(items: list[dict[str, Any]]) -> dict[str, Any]:
    running = [item for item in items if item["IsRunning"]]
    blocked = sum(item["IsBlocked"] for item in items)
    waiting = len(items) - len(running) - blocked
    return {
        "WaitingCount": waiting,
        "BlockedCount": blocked,
        "TotalCount": waiting + blocked + len(running),
        "CurrentlyExecuting": running,
    }


def _run(
    items: list[dict[str, Any]], override: Override | None = None, **limits: Any
) -> tuple[QueueInventory, list[httpx.Request]]:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.method == "GET"
        assert request.headers["apikey"] == "synthetic-test-key"
        assert request.headers["Accept-Encoding"] == "identity"
        if override is not None and (response := override(request)) is not None:
            return response
        if request.url.path.endswith("/Queue"):
            return httpx.Response(200, json=_summary(items))
        assert request.url.path == "/base/api/v3/Queue/Items"
        assert request.url.params["showAll"] == "true"
        size, page = int(request.url.params["pageSize"]), int(request.url.params["page"])
        assert 0 < size <= 1000
        assert page > 0
        return httpx.Response(
            200, json={"Total": len(items), "List": items[(page - 1) * size : page * size]}
        )

    with httpx.Client(
        base_url="http://shoko/base/", transport=httpx.MockTransport(handler)
    ) as client:
        endpoint = ApiEndpoint(client, lambda: {"apikey": "synthetic-test-key"})
        options = {"deadline": 100.0, "clock": lambda: 0.0, **limits}
        return fetch_queue_inventory(endpoint, **options), requests


@pytest.mark.parametrize("flags", list(itertools.product((False, True), repeat=5)))
def test_anime_identity_matches_every_flag_variant(flags: tuple[bool, ...]) -> None:
    names = (
        "ForceRefresh",
        "CacheOnly",
        "DownloadRelations",
        "CreateSeriesEntry",
        "SkipTmdbUpdate",
    )
    tokens = [f"{name}:true" for name, enabled in zip(names, flags, strict=True) if enabled]
    key = "_".join([ANIME_KEY, *tokens])
    inventory, _ = _run([_item(key)])
    assert inventory.complete
    assert inventory.lookup(ANIME_KIND, 18208).presence is Presence.PRESENT
    assert inventory.lookup(ANIME_KIND, 18209).presence is Presence.ABSENT


def test_explicit_defaults_relation_depth_and_all_states_are_matched() -> None:
    items = [
        _item(
            ANIME_KEY + "_ForceRefresh:false_CacheOnly:false_DownloadRelations:true_RelDepth:2"
            "_CreateSeriesEntry:true_SkipTmdbUpdate:false",
            QueueJobState.BLOCKED,
        ),
        _item(ANIME_KEY + "_CacheOnly:true", QueueJobState.RUNNING),
        _item(ANIME_KEY),
    ]
    inventory, requests = _run(items, page_size=2)
    assert inventory.complete
    assert inventory.pages_fetched == 2
    assert inventory.lookup(ANIME_KIND, 18208).states == frozenset(QueueJobState)
    assert len(requests) == 4


@pytest.mark.parametrize("percentage", [-100, 0, 25, 100, 101, 2_147_483_647])
def test_manual_link_matches_same_file_regardless_of_episode_or_percentage(percentage: int) -> None:
    inventory, _ = _run(
        [
            _item(f"{LINK_KEY}_Percentage:{percentage}"),
            _item("Import.ManualLinkJob_VideoLocalID:149_EpisodeID:999"),
        ]
    )
    assert inventory.complete
    assert inventory.lookup(LINK_KIND, 149).presence is Presence.PRESENT
    assert inventory.lookup(LINK_KIND, 150).presence is Presence.ABSENT


def test_human_titles_and_details_never_supply_identity() -> None:
    item = _item("Import.HashFileJob_VideoLocalID:149")
    item["Title"] = "Get AniDB Anime Data 18208, Manually Linking File 149"
    item["Details"] = {"AnimeID": 18208, "VideoLocalID": 149}
    inventory, _ = _run([item])
    assert inventory.complete
    assert inventory.jobs == ()
    assert inventory.lookup(ANIME_KIND, 18208).presence is Presence.ABSENT
    assert inventory.lookup(LINK_KIND, 149).presence is Presence.ABSENT


@pytest.mark.parametrize(
    "key",
    [
        "AniDB.GetAniDBAnimeJob",
        "DEFAULT.GetAniDBAnimeJob_AnimeID:18208",
        "AniDB.GetAniDBAnimeJob_AnimeID:0",
        "AniDB.GetAniDBAnimeJob_AnimeID:-1",
        "AniDB.GetAniDBAnimeJob_AnimeID:2147483648",
        "AniDB.GetAniDBAnimeJob_AnimeID:018208",
        "AniDB.GetAniDBAnimeJob_AnimeID:true",
        'AniDB.GetAniDBAnimeJob_AnimeID:"18208"',
        ANIME_KEY + "_ForceRefresh:True",
        ANIME_KEY + "_FutureFlag:true",
        ANIME_KEY + "_AnimeID:18208",
        ANIME_KEY + "_CacheOnly:true_ForceRefresh:true",
        "AniDB.GetAniDBAnimeJob_ForceRefresh:true",
        LINK_KEY + "_EpisodeID:651",
        "Import.ManualLinkJob_VideoLocalID:149_EpisodeID:0",
        ANIME_KEY + "_RelDepth:1.5",
    ],
)
def test_unknown_or_malformed_relevant_key_never_proves_absence(key: str) -> None:
    inventory, _ = _run([_item(key)])
    assert not inventory.complete
    assert inventory.reason is InventoryReason.UNKNOWN_JOB_KEY
    assert inventory.lookup(ANIME_KIND, 18208).presence is Presence.UNKNOWN
    assert inventory.lookup(LINK_KIND, 149).presence is Presence.UNKNOWN


@pytest.mark.parametrize("key", [ANIME_KEY + "\nsecret-value", ANIME_KEY + "\x7f", "x" * 4097])
def test_unsafe_keys_are_not_exposed(key: str) -> None:
    inventory, _ = _run([_item(key)])
    assert inventory.reason is InventoryReason.MALFORMED_RESPONSE
    assert key not in repr(inventory)
    assert "secret-value" not in repr(inventory)


@pytest.mark.parametrize(("count", "size"), [(0, 1), (1, 1), (2, 1), (4, 2), (5, 2), (1001, 1000)])
def test_pagination_covers_empty_and_first_last_page_boundaries(count: int, size: int) -> None:
    items = [_item(f"AniDB.GetAniDBAnimeJob_AnimeID:{index + 1}") for index in range(count)]
    inventory, requests = _run(items, page_size=size)
    assert inventory.complete
    assert inventory.pages_fetched == max(1, (count + size - 1) // size)
    assert len(inventory.jobs) == count
    assert inventory.lookup(ANIME_KIND, 18208).presence is Presence.ABSENT
    if count:
        assert inventory.lookup(ANIME_KIND, count).presence is Presence.PRESENT
    assert all("Types" not in request.url.path for request in requests)


def test_duplicate_page_keys_mark_unknown_but_preserve_observed_presence() -> None:
    inventory, _ = _run([_item(), _item()], page_size=1)
    assert inventory.reason is InventoryReason.DUPLICATE_KEYS
    assert inventory.lookup(ANIME_KIND, 18208).presence is Presence.PRESENT
    assert inventory.lookup(ANIME_KIND, 999).presence is Presence.UNKNOWN


@pytest.mark.parametrize(
    ("corruption", "reason"),
    [
        ({"Total": 2, "List": []}, InventoryReason.QUEUE_CHANGED),
        ({"Total": 1, "List": []}, InventoryReason.INCOMPLETE_PAGE),
        ({"Total": 1, "List": [_item(), _item(LINK_KEY)]}, InventoryReason.INCOMPLETE_PAGE),
        ({"Total": True, "List": []}, InventoryReason.MALFORMED_RESPONSE),
        ({"Total": 1}, InventoryReason.MALFORMED_RESPONSE),
        ([], InventoryReason.MALFORMED_RESPONSE),
    ],
)
def test_changed_truncated_or_malformed_pages_are_unknown(
    corruption: Any,
    reason: InventoryReason,
) -> None:
    def override(request: httpx.Request) -> httpx.Response | None:
        return httpx.Response(200, json=corruption) if request.url.path.endswith("/Items") else None

    inventory, _ = _run([_item()], override)
    assert inventory.reason is reason
    assert inventory.lookup(ANIME_KIND, 18208).presence is Presence.UNKNOWN


def test_change_between_page_totals_is_detected() -> None:
    def override(request: httpx.Request) -> httpx.Response | None:
        if request.url.params.get("page") == "2":
            return httpx.Response(200, json={"Total": 3, "List": [_item(LINK_KEY)]})
        return None

    inventory, _ = _run([_item(), _item(LINK_KEY)], override, page_size=1)
    assert inventory.reason is InventoryReason.QUEUE_CHANGED


def test_running_summary_positive_survives_incomplete_page_and_permission_failure() -> None:
    def override(request: httpx.Request) -> httpx.Response | None:
        return httpx.Response(403) if request.url.path.endswith("/Items") else None

    inventory, _ = _run([_item(state=QueueJobState.RUNNING)], override)
    assert inventory.reason is InventoryReason.PERMISSION_DENIED
    assert inventory.lookup(ANIME_KIND, 18208).presence is Presence.PRESENT
    assert inventory.lookup(ANIME_KIND, 999).presence is Presence.UNKNOWN


def test_changed_final_running_identity_is_unknown_and_unions_positive_identities() -> None:
    calls = 0

    def override(request: httpx.Request) -> httpx.Response | None:
        nonlocal calls
        if request.url.path.endswith("/Queue"):
            calls += 1
            if calls == 2:
                return httpx.Response(
                    200,
                    json=_summary(
                        [_item(ANIME_KEY.replace("18208", "18209"), QueueJobState.RUNNING)]
                    ),
                )
        return None

    inventory, _ = _run([_item(state=QueueJobState.RUNNING)], override)
    assert inventory.reason is InventoryReason.QUEUE_CHANGED
    assert inventory.lookup(ANIME_KIND, 18208).presence is Presence.PRESENT
    assert inventory.lookup(ANIME_KIND, 18209).presence is Presence.PRESENT
    assert inventory.lookup(ANIME_KIND, 18210).presence is Presence.UNKNOWN


def test_running_missing_from_page_cannot_silently_become_empty() -> None:
    def override(request: httpx.Request) -> httpx.Response | None:
        if request.url.path.endswith("/Items"):
            return httpx.Response(200, json={"Total": 1, "List": [_item(LINK_KEY)]})
        return None

    inventory, _ = _run([_item(state=QueueJobState.RUNNING)], override)
    assert inventory.reason is InventoryReason.QUEUE_CHANGED
    assert inventory.lookup(ANIME_KIND, 18208).presence is Presence.PRESENT


@pytest.mark.parametrize(
    ("status", "reason"),
    [
        (401, InventoryReason.PERMISSION_DENIED),
        (403, InventoryReason.PERMISSION_DENIED),
        (500, InventoryReason.HTTP_ERROR),
        (204, InventoryReason.HTTP_ERROR),
    ],
)
def test_http_failures_never_look_empty(status: int, reason: InventoryReason) -> None:
    inventory, requests = _run([], lambda request: httpx.Response(status))
    assert inventory.reason is reason
    assert not inventory.complete
    assert inventory.lookup(ANIME_KIND, 18208).presence is Presence.UNKNOWN
    assert len(requests) == 1


@pytest.mark.parametrize("content", [b"not-json", b"\xff", b"[1", b"{" * 2000])
def test_invalid_json_is_unknown(content: bytes) -> None:
    inventory, _ = _run([], lambda request: httpx.Response(200, content=content))
    assert inventory.reason is InventoryReason.MALFORMED_RESPONSE


@pytest.mark.parametrize(
    "content",
    [
        b'{"WaitingCount":1,"WaitingCount":0,"BlockedCount":0,"TotalCount":0,"CurrentlyExecuting":[]}',
        b'{"WaitingCount":0,"BlockedCount":0,"TotalCount":0,"CurrentlyExecuting":[],"Other":NaN}',
    ],
)
def test_duplicate_json_keys_and_non_json_constants_are_unknown(content: bytes) -> None:
    inventory, _ = _run([], lambda request: httpx.Response(200, content=content))
    assert inventory.reason is InventoryReason.MALFORMED_RESPONSE
    assert inventory.lookup(ANIME_KIND, 18208).presence is Presence.UNKNOWN


def test_ignored_identity_encoding_request_cannot_expand_unbounded_compressed_body() -> None:
    compressed = gzip.compress(b" " * 100_000)
    stream = httpx.ByteStream(compressed)
    inventory, _ = _run(
        [], lambda request: httpx.Response(200, headers={"content-encoding": "gzip"}, stream=stream)
    )
    assert inventory.reason is InventoryReason.MALFORMED_RESPONSE


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("Key", 123),
        ("Type", None),
        ("IsRunning", 1),
        ("IsBlocked", None),
    ],
)
def test_malformed_job_shape_cannot_prove_absence(field: str, value: Any) -> None:
    def override(request: httpx.Request) -> httpx.Response | None:
        if request.url.path.endswith("/Items"):
            raw_item = _item()
            raw_item[field] = value
            return httpx.Response(200, json={"Total": 1, "List": [raw_item]})
        return None

    inventory, _ = _run([_item()], override)
    assert inventory.reason is InventoryReason.MALFORMED_RESPONSE


def test_duplicate_running_keys_do_not_prove_absence() -> None:
    inventory, _ = _run([_item(state=QueueJobState.RUNNING), _item(state=QueueJobState.RUNNING)])
    assert inventory.reason is InventoryReason.DUPLICATE_KEYS
    assert inventory.lookup(ANIME_KIND, 18209).presence is Presence.UNKNOWN


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("WaitingCount", True, InventoryReason.MALFORMED_RESPONSE),
        ("BlockedCount", -1, InventoryReason.MALFORMED_RESPONSE),
        ("CurrentlyExecuting", None, InventoryReason.MALFORMED_RESPONSE),
        ("TotalCount", 0, InventoryReason.QUEUE_CHANGED),
    ],
)
def test_inconsistent_or_malformed_summary_is_unknown(
    field: str,
    value: Any,
    reason: InventoryReason,
) -> None:
    summary = _summary([_item()])
    summary[field] = value
    inventory, _ = _run([_item()], lambda request: httpx.Response(200, json=summary))
    assert inventory.reason is reason
    assert inventory.lookup(ANIME_KIND, 18208).presence is Presence.UNKNOWN


def test_redirect_refuses_without_following_or_forwarding_credentials() -> None:
    inventory, requests = _run(
        [],
        lambda request: httpx.Response(
            302, headers={"Location": "http://external.invalid/collect"}
        ),
    )
    assert inventory.reason is InventoryReason.READ_FAILED
    assert len(requests) == 1


def test_row_page_and_combined_body_limits_are_conservative() -> None:
    inventory, requests = _run([_item(), _item(LINK_KEY)], max_rows=1)
    assert inventory.reason is InventoryReason.ROW_LIMIT
    assert len(requests) == 1
    inventory, requests = _run([_item(), _item(LINK_KEY)], page_size=1, max_pages=1)
    assert inventory.reason is InventoryReason.PAGE_LIMIT
    assert len(requests) == 1
    summary_size = len(json.dumps(_summary([]), separators=(",", ":")).encode())
    inventory, requests = _run([], max_body_bytes=summary_size + 1)
    assert inventory.reason is InventoryReason.BODY_LIMIT
    assert len(requests) == 2


def test_twenty_thousand_unrelated_jobs_fit_bounded_default_inventory() -> None:
    items = qf.unrelated(15_000, 0, 20_000)
    body_bytes = 0

    def measure(request: httpx.Request) -> httpx.Response | None:
        nonlocal body_bytes
        if request.url.path.endswith("/Queue"):
            payload = qf.summary(items)
        else:
            page = int(request.url.params["page"])
            size = int(request.url.params["pageSize"])
            payload = {"Total": len(items), "List": items[(page - 1) * size : page * size]}
        response = httpx.Response(200, json=payload)
        body_bytes += len(response.content)
        return response

    inventory, requests = _run(items, measure)
    assert inventory.complete
    assert inventory.pages_fetched == 20
    assert len(requests) == 22
    assert 4_000_000 < body_bytes <= 8_000_000
    assert inventory.lookup(LINK_KIND, 149).presence is Presence.ABSENT
    assert inventory.lookup(ANIME_KIND, 18208).presence is Presence.ABSENT


def test_above_default_row_limit_cannot_authorize_absence() -> None:
    inventory, requests = _run(qf.unrelated(20_001, 0, 20_001))
    assert inventory.reason is InventoryReason.ROW_LIMIT
    assert len(requests) == 1
    assert inventory.lookup(LINK_KIND, 149).presence is Presence.UNKNOWN


def test_large_queue_still_obeys_shared_bytes_and_remaining_deadline() -> None:
    items = qf.unrelated(15_000, 0, 20_000)
    inventory, _ = _run(items, max_body_bytes=2_000_000)
    assert inventory.reason is InventoryReason.BODY_LIMIT
    assert inventory.lookup(LINK_KIND, 149).presence is Presence.UNKNOWN

    now = 0.0

    def advance(request: httpx.Request) -> httpx.Response | None:
        nonlocal now
        remaining = 1.0 - now
        assert all(value <= remaining for value in request.extensions["timeout"].values())
        now += 0.1
        return None

    inventory, requests = _run(items, advance, deadline=1.0, clock=lambda: now)
    assert inventory.reason is InventoryReason.DEADLINE
    assert len(requests) <= 11
    assert inventory.lookup(LINK_KIND, 149).presence is Presence.UNKNOWN


def test_stream_read_failure_is_unknown_and_closes_response() -> None:
    class BrokenStream(httpx.SyncByteStream):
        closed = False

        def __iter__(self) -> Iterator[bytes]:
            yield b'{"WaitingCount":0'
            raise httpx.ReadError("do not report secret-value")

        def close(self) -> None:
            self.closed = True

    stream = BrokenStream()
    inventory, _ = _run([], lambda request: httpx.Response(200, stream=stream))
    assert inventory.reason is InventoryReason.READ_FAILED
    assert stream.closed
    assert "secret-value" not in repr(inventory)


def test_deadline_prevents_requests_and_caps_per_request_timeouts() -> None:
    inventory, requests = _run([], deadline=0.0)
    assert inventory.reason is InventoryReason.DEADLINE
    assert requests == []

    def override(request: httpx.Request) -> httpx.Response | None:
        assert all(value <= 0.25 for value in request.extensions["timeout"].values())
        return None

    inventory, _ = _run([], override, deadline=0.25)
    assert inventory.complete


def test_deadline_during_stream_is_unknown_and_closes_response() -> None:
    now = 0.0

    class SlowStream(httpx.SyncByteStream):
        closed = False

        def __iter__(self) -> Iterator[bytes]:
            nonlocal now
            yield b'{"WaitingCount":'
            now = 2.0
            yield b'0,"BlockedCount":0,"TotalCount":0,"CurrentlyExecuting":[]}'

        def close(self) -> None:
            self.closed = True

    stream = SlowStream()
    inventory, _ = _run(
        [], lambda request: httpx.Response(200, stream=stream), deadline=1.0, clock=lambda: now
    )
    assert inventory.reason is InventoryReason.DEADLINE
    assert stream.closed


def test_stream_body_limit_closes_without_reading_to_eof() -> None:
    class HugeStream(httpx.SyncByteStream):
        chunks_read = 0
        closed = False

        def __iter__(self) -> Iterator[bytes]:
            for _ in range(100):
                self.chunks_read += 1
                yield b" " * 20

        def close(self) -> None:
            self.closed = True

    stream = HugeStream()
    inventory, _ = _run([], lambda request: httpx.Response(200, stream=stream), max_body_bytes=30)
    assert inventory.reason is InventoryReason.BODY_LIMIT
    assert stream.chunks_read == 2
    assert stream.closed


@pytest.mark.parametrize(
    "limits",
    [
        {"page_size": 0},
        {"page_size": 1001},
        {"page_size": True},
        {"max_pages": 0},
        {"max_rows": -1},
        {"max_body_bytes": 0},
        {"deadline": float("inf")},
    ],
)
def test_invalid_caller_limits_refuse_before_io(limits: dict[str, Any]) -> None:
    with pytest.raises(ValueError, match="must"):
        _run([], **limits)


@pytest.mark.parametrize("subject_id", [0, -1, True, 2_147_483_648])
def test_invalid_lookup_ids_are_rejected(subject_id: int) -> None:
    inventory, _ = _run([])
    with pytest.raises(ValueError, match="positive signed 32-bit"):
        inventory.lookup(ANIME_KIND, subject_id)


@pytest.mark.parametrize("state", list(QueueJobState))
def test_whole_queue_total_includes_unrecognized_job_kinds(state: QueueJobState) -> None:
    inventory, _ = _run([_item("Other.UnrecognizedJob", state)])
    assert inventory.complete
    assert inventory.jobs == ()
    assert inventory.total_count == 1


def test_only_complete_empty_inventory_proves_zero_whole_queue_jobs() -> None:
    empty, _ = _run([])
    assert empty.complete
    assert empty.total_count == 0
    unknown, _ = _run([], max_body_bytes=1)
    assert not unknown.complete
    assert unknown.total_count is None
