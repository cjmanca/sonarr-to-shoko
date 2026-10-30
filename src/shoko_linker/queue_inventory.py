"""Bounded, read-only Shoko job identities, verified against Shoko Server 5.3.3.

``Queue/Items?showAll=true`` includes running, waiting and blocked jobs. Both its
Total and Queue.TotalCount include running jobs in the implementation (despite
the Queue model's waiting-only comment). Queue/Types counts only SQL WAITING
triggers and cannot prove absence. Identity comes exclusively from the generated
Quartz key (group.name), never the localized Type/Title or human-readable Details.

The pinned upstream JobKeyBuilder serializes supplied writable properties as JSON,
in property order; JobDataMapBuilder normally omits default values. Thus all flag
variants identify the same anime, and all episode/percentage variants identify the
same file. Unknown relevant key formats invalidate absence rather than guessing.

This is an observation, NOT an atomic check-and-submit or global exactly-once
guarantee: jobs can change after any read, and equal-size substitutions between
pages need not be detectable. Callers still need a durable local operation ledger
and pre-write safety checks. No requests here mutate Shoko or persistent state.

Source: ShokoAnime/ShokoServer commit 892461188e5f1cc3ccf1b9b6eb1d4dfd31bea6f8,
Scheduling/GenericJobBuilder/JobKeyBuilder.cs, Jobs/AniDB/GetAniDBAnimeJob.cs,
Jobs/Shoko/ManualLinkJob.cs and API/v3/Controllers/QueueController.cs.
"""

from __future__ import annotations

import json
import math
import re
import time
from collections.abc import Callable, Mapping
from contextlib import closing
from dataclasses import dataclass
from enum import Enum
from http import HTTPStatus
from typing import Any, Final

import httpx

from .errors import HttpPolicyError
from .http import ApiEndpoint, resolve_and_send

_MAX_ID: Final[int] = 2_147_483_647
_MIN_INT: Final[int] = -2_147_483_648
_MAX_KEY_LENGTH: Final[int] = 4096
_MAX_PAGE_SIZE: Final[int] = 1000
_INT_TOKEN: Final[re.Pattern[str]] = re.compile(r"(?:0|-?[1-9][0-9]{0,9})\Z", re.ASCII)


class QueueJobKind(Enum):
    ANIME_METADATA = "anime_metadata"
    MANUAL_LINK = "manual_link"


class QueueJobState(Enum):
    WAITING = "waiting"
    RUNNING = "running"
    BLOCKED = "blocked"


class Presence(Enum):
    PRESENT = "present"
    ABSENT = "absent"
    UNKNOWN = "unknown"


class InventoryReason(Enum):
    COMPLETE = "complete"
    PERMISSION_DENIED = "permission_denied"
    READ_FAILED = "read_failed"
    HTTP_ERROR = "http_error"
    MALFORMED_RESPONSE = "malformed_response"
    UNKNOWN_JOB_KEY = "unknown_job_key"
    DUPLICATE_KEYS = "duplicate_keys"
    QUEUE_CHANGED = "queue_changed"
    INCOMPLETE_PAGE = "incomplete_page"
    ROW_LIMIT = "row_limit"
    PAGE_LIMIT = "page_limit"
    BODY_LIMIT = "body_limit"
    DEADLINE = "deadline"


@dataclass(frozen=True)
class QueueJob:
    kind: QueueJobKind
    subject_id: int
    state: QueueJobState


@dataclass(frozen=True)
class QueuePresence:
    presence: Presence
    states: frozenset[QueueJobState]
    reason: InventoryReason


@dataclass(frozen=True)
class QueueInventory:
    jobs: tuple[QueueJob, ...]
    complete: bool
    reason: InventoryReason
    pages_fetched: int
    # All jobs, including kinds omitted from ``jobs``; only complete reads prove emptiness.
    total_count: int | None = None

    def lookup(self, kind: QueueJobKind, subject_id: int) -> QueuePresence:
        """Positive identities survive an incomplete read; absence never does."""
        _validate_id(subject_id)
        states = frozenset(
            job.state for job in self.jobs if job.kind is kind and job.subject_id == subject_id
        )
        presence = (
            Presence.PRESENT if states else Presence.ABSENT if self.complete else Presence.UNKNOWN
        )
        return QueuePresence(presence, states, self.reason)


_ANIME_FIELDS: Final[tuple[str, ...]] = (
    "AnimeID",
    "ForceRefresh",
    "CacheOnly",
    "DownloadRelations",
    "RelDepth",
    "CreateSeriesEntry",
    "SkipTmdbUpdate",
)
_LINK_FIELDS: Final[tuple[str, ...]] = ("VideoLocalID", "EpisodeID", "Percentage")
_KEY_TYPES: Final[tuple[tuple[str, str, QueueJobKind, tuple[str, ...]], ...]] = (
    ("AniDB.GetAniDBAnimeJob", "Get AniDB Anime Data", QueueJobKind.ANIME_METADATA, _ANIME_FIELDS),
    (
        "Import.ManualLinkJob",
        "Manually Link File to Episode",
        QueueJobKind.MANUAL_LINK,
        _LINK_FIELDS,
    ),
)


class _InventoryError(Exception):
    def __init__(self, reason: InventoryReason) -> None:
        super().__init__(reason.value)
        self.reason = reason


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _InventoryError(InventoryReason.MALFORMED_RESPONSE)
        result[key] = value
    return result


def _invalid_json_constant(_value: str) -> None:
    raise _InventoryError(InventoryReason.MALFORMED_RESPONSE)


def _validate_id(value: int) -> None:
    if type(value) is not int or not 0 < value <= _MAX_ID:
        raise ValueError("queue subject ID must be a positive signed 32-bit integer")


def _integer(value: object) -> int:
    if type(value) is not int or value < 0:
        raise _InventoryError(InventoryReason.MALFORMED_RESPONSE)
    return value


def _key_subject(key: str, type_name: str) -> tuple[QueueJobKind, int] | None:
    for prefix, label, kind, fields in _KEY_TYPES:
        if not key.startswith(prefix + "_"):
            if key == prefix or prefix.split(".", 1)[1] in key or type_name == label:
                raise _InventoryError(InventoryReason.UNKNOWN_JOB_KEY)
            continue
        return kind, _primary_id(key[len(prefix) + 1 :], fields)
    return None


def _primary_id(suffix: str, fields: tuple[str, ...]) -> int:
    previous = -1
    primary: int | None = None
    for token in suffix.split("_"):
        name, separator, value = token.partition(":")
        if not separator or name not in fields:
            raise _InventoryError(InventoryReason.UNKNOWN_JOB_KEY)
        index = fields.index(name)
        if index <= previous:
            raise _InventoryError(InventoryReason.UNKNOWN_JOB_KEY)
        previous = index
        if name in {
            "ForceRefresh",
            "CacheOnly",
            "DownloadRelations",
            "CreateSeriesEntry",
            "SkipTmdbUpdate",
        }:
            if value not in {"true", "false"}:
                raise _InventoryError(InventoryReason.UNKNOWN_JOB_KEY)
            continue
        if not _INT_TOKEN.fullmatch(value):
            raise _InventoryError(InventoryReason.UNKNOWN_JOB_KEY)
        number = int(value)
        if not _MIN_INT <= number <= _MAX_ID:
            raise _InventoryError(InventoryReason.UNKNOWN_JOB_KEY)
        if name in {"AnimeID", "VideoLocalID", "EpisodeID"} and number <= 0:
            raise _InventoryError(InventoryReason.UNKNOWN_JOB_KEY)
        if index == 0:
            primary = number
    if primary is None:
        raise _InventoryError(InventoryReason.UNKNOWN_JOB_KEY)
    return primary


def _item(raw: Any) -> tuple[str, QueueJob | None, QueueJobState]:
    if not isinstance(raw, dict):
        raise _InventoryError(InventoryReason.MALFORMED_RESPONSE)
    key, label = raw.get("Key"), raw.get("Type")
    running, blocked = raw.get("IsRunning"), raw.get("IsBlocked")
    if (
        not isinstance(key, str)
        or not key
        or len(key) > _MAX_KEY_LENGTH
        or not key.isprintable()
        or not isinstance(label, str)
        or type(running) is not bool
        or type(blocked) is not bool
        or (running and blocked)
    ):
        raise _InventoryError(InventoryReason.MALFORMED_RESPONSE)
    state = (
        QueueJobState.RUNNING
        if running
        else (QueueJobState.BLOCKED if blocked else QueueJobState.WAITING)
    )
    subject = _key_subject(key, label)
    return key, QueueJob(*subject, state) if subject else None, state


@dataclass(frozen=True)
class _Summary:
    waiting: int
    blocked: int
    total: int
    running_keys: frozenset[str]

    @property
    def item_total(self) -> int:
        return self.waiting + self.blocked + len(self.running_keys)


class _Reader:
    def __init__(
        self,
        endpoint: ApiEndpoint,
        deadline: float,
        clock: Callable[[], float],
        max_body_bytes: int,
    ) -> None:
        self.endpoint = ApiEndpoint(
            endpoint.client,
            lambda: {**endpoint.headers_factory(), "Accept-Encoding": "identity"},
        )
        self.deadline = deadline
        self.clock = clock
        self.remaining_bytes = max_body_bytes
        self.pages_fetched = 0

    def check_deadline(self) -> float:
        remaining = self.deadline - self.clock()
        if remaining <= 0:
            raise _InventoryError(InventoryReason.DEADLINE)
        return remaining

    def get(self, path: str, params: Mapping[str, Any] | None = None) -> Any:
        remaining = self.check_deadline()
        configured = self.endpoint.client.timeout.as_dict()
        timeout = httpx.Timeout(
            **{
                key: remaining if value is None else min(value, remaining)
                for key, value in configured.items()
            }
        )
        try:
            with closing(
                resolve_and_send(
                    self.endpoint,
                    "GET",
                    path,
                    params=params,
                    max_redirects=0,
                    stream=True,
                    timeout=timeout,
                )
            ) as response:
                self.check_deadline()
                if response.status_code in {401, 403}:
                    raise _InventoryError(InventoryReason.PERMISSION_DENIED)
                if response.status_code != HTTPStatus.OK:
                    raise _InventoryError(InventoryReason.HTTP_ERROR)
                if response.headers.get("content-encoding", "identity").lower() != "identity":
                    raise _InventoryError(InventoryReason.MALFORMED_RESPONSE)
                body = bytearray()
                for chunk in response.iter_bytes():
                    self.check_deadline()
                    self.remaining_bytes -= len(chunk)
                    if self.remaining_bytes < 0:
                        raise _InventoryError(InventoryReason.BODY_LIMIT)
                    body.extend(chunk)
                payload = json.loads(
                    body,
                    object_pairs_hook=_unique_json_object,
                    parse_constant=_invalid_json_constant,
                )
                self.check_deadline()
                return payload
        except (httpx.HTTPError, httpx.StreamError, HttpPolicyError):
            raise _InventoryError(InventoryReason.READ_FAILED) from None
        except (ValueError, RecursionError):
            raise _InventoryError(InventoryReason.MALFORMED_RESPONSE) from None


def _summary(reader: _Reader, jobs: list[QueueJob], max_rows: int) -> _Summary:
    raw = reader.get("api/v3/Queue")
    if not isinstance(raw, dict) or not isinstance(raw.get("CurrentlyExecuting"), list):
        raise _InventoryError(InventoryReason.MALFORMED_RESPONSE)
    waiting = _integer(raw.get("WaitingCount"))
    blocked = _integer(raw.get("BlockedCount"))
    total = _integer(raw.get("TotalCount"))
    if waiting + blocked + len(raw["CurrentlyExecuting"]) > max_rows:
        raise _InventoryError(InventoryReason.ROW_LIMIT)
    keys: set[str] = set()
    for raw_item in raw["CurrentlyExecuting"]:
        key, job, state = _item(raw_item)
        if state is not QueueJobState.RUNNING:
            raise _InventoryError(InventoryReason.MALFORMED_RESPONSE)
        if key in keys:
            raise _InventoryError(InventoryReason.DUPLICATE_KEYS)
        keys.add(key)
        if job is not None:
            jobs.append(job)
    if total != waiting + blocked + len(keys):
        raise _InventoryError(InventoryReason.QUEUE_CHANGED)
    return _Summary(waiting, blocked, total, frozenset(keys))


def _pages(
    reader: _Reader,
    initial: _Summary,
    jobs: list[QueueJob],
    page_size: int,
    max_pages: int,
) -> None:
    keys: set[str] = set()
    counts = dict.fromkeys(QueueJobState, 0)
    expected = initial.item_total
    required_pages = max(1, (expected + page_size - 1) // page_size)
    if required_pages > max_pages:
        raise _InventoryError(InventoryReason.PAGE_LIMIT)
    for page in range(1, required_pages + 1):
        raw = reader.get(
            "api/v3/Queue/Items", {"pageSize": page_size, "page": page, "showAll": "true"}
        )
        reader.pages_fetched += 1
        if not isinstance(raw, dict) or not isinstance(raw.get("List"), list):
            raise _InventoryError(InventoryReason.MALFORMED_RESPONSE)
        total = _integer(raw.get("Total"))
        if total != expected:
            raise _InventoryError(InventoryReason.QUEUE_CHANGED)
        items = raw["List"]
        if len(items) != min(page_size, expected - len(keys)):
            raise _InventoryError(InventoryReason.INCOMPLETE_PAGE)
        for raw_item in items:
            key, job, state = _item(raw_item)
            if key in keys:
                raise _InventoryError(InventoryReason.DUPLICATE_KEYS)
            keys.add(key)
            counts[state] += 1
            if job is not None:
                jobs.append(job)
    if (
        counts[QueueJobState.WAITING] != initial.waiting
        or counts[QueueJobState.BLOCKED] != initial.blocked
        or counts[QueueJobState.RUNNING] != len(initial.running_keys)
        or not initial.running_keys <= keys
    ):
        raise _InventoryError(InventoryReason.QUEUE_CHANGED)


def fetch_queue_inventory(
    endpoint: ApiEndpoint,
    *,
    deadline: float,
    clock: Callable[[], float] = time.monotonic,
    page_size: int = 1000,
    max_pages: int = 20,
    max_rows: int = 20_000,
    max_body_bytes: int = 8_000_000,
) -> QueueInventory:
    """Observe job presence with positive paging and bounded rows/bytes/time.

    ``deadline`` is an absolute monotonic deadline supplied by the cycle owner.
    It is checked around HTTP reads, each transport chunk and JSON parsing; an
    in-progress blocking read is subject to the narrowed per-request HTTP timeout.
    UNKNOWN must defer submissions, never act like an empty queue. ABSENT only
    describes this bounded observation; external schedulers can still race it.
    No titles, details, arbitrary keys, exception strings or secrets are returned.
    Default limits cover the existing 20,000-job backlog acceptance case, including
    running jobs repeated in both summaries. The byte budget is shared across all
    responses; larger or more verbose inventories still conservatively defer.
    """
    if not math.isfinite(deadline):
        raise ValueError("queue inventory deadline must be finite")
    for name, value in (
        ("page_size", page_size),
        ("max_pages", max_pages),
        ("max_rows", max_rows),
        ("max_body_bytes", max_body_bytes),
    ):
        if type(value) is not int or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
    if page_size > _MAX_PAGE_SIZE:
        raise ValueError("page_size must not exceed 1000")
    reader = _Reader(endpoint, deadline, clock, max_body_bytes)
    jobs: list[QueueJob] = []
    reason = InventoryReason.COMPLETE
    try:
        initial = _summary(reader, jobs, max_rows)
        _pages(reader, initial, jobs, page_size, max_pages)
        if _summary(reader, jobs, max_rows) != initial:
            raise _InventoryError(InventoryReason.QUEUE_CHANGED)
    except _InventoryError as error:
        reason = error.reason
    unique_jobs = tuple(
        sorted(set(jobs), key=lambda item: (item.kind.value, item.subject_id, item.state.value))
    )
    return QueueInventory(
        unique_jobs,
        reason is InventoryReason.COMPLETE,
        reason,
        reader.pages_fetched,
        initial.total if reason is InventoryReason.COMPLETE else None,
    )
