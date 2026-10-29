"""Bounded read-only recovery of durable anime metadata requests.

Recovery never imports the metadata transport or reads current authorization.
Consent can stop new requests, but cannot recall work already accepted by Shoko.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

import httpx

from .http import ApiEndpoint
from .linking import EpisodeIdResolver, EpisodeResolutionStatus
from .metadata_identity import metadata_source_key_for_url
from .metadata_status import observe_metadata_status
from .queue_inventory import Presence, QueueJobKind, fetch_queue_inventory
from .state import MetadataOperation, MetadataOperationState, StateError, StateStore


class MetadataRecoveryStatus(Enum):
    UNSENT_RELEASED = "unsent_released"
    READY = "ready"
    WAITING_METADATA = "waiting_metadata"
    WAITING_QUEUE = "waiting_queue"
    QUEUE_UNKNOWN = "queue_unknown"
    NEEDS_ATTENTION = "needs_attention"
    INVALID_COVERAGE = "invalid_coverage"
    STATE_UNAVAILABLE = "state_unavailable"
    DEADLINE = "deadline"


@dataclass(frozen=True)
class MetadataRecoveryObservation:
    anime_id: int
    status: MetadataRecoveryStatus


@dataclass(frozen=True)
class MetadataRecoveryReport:
    processed_count: int
    deferred_count: int
    observations: tuple[MetadataRecoveryObservation, ...]


def metadata_source_key(endpoint: ApiEndpoint) -> str:
    """Hash the normalized current Shoko base, without userinfo/query/fragment.

    HTTPX canonicalizes scheme, host, default port and dot segments. Preserve
    path case, escaping and internal slashes because a reverse proxy can route
    them differently; normalize only trailing slashes like make_client does.
    Credentials, Sonarr, version, scope and authorization never affect identity.
    A changed server base gets its own ledger; recovery never contacts old bases.
    """
    return metadata_source_key_for_url(endpoint.base_url)


def _aware_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError("timezone required")
    return parsed


def _usable(
    endpoint: ApiEndpoint,
    operation: MetadataOperation,
    deadline: float,
    clock: Callable[[], float],
) -> bool:
    # Each call deliberately has a new resolver, including the post-queue pass.
    resolver = EpisodeIdResolver(endpoint, clock=clock, deadline=deadline)
    assert operation.required_targets
    for anime, episode_type, number in operation.required_targets:
        if clock() >= deadline:
            return False
        result = resolver.resolve(anime, episode_type.value, number)
        if result.status is not EpisodeResolutionStatus.RESOLVED:
            return False
    return clock() < deadline


def _recover_row(
    endpoint: ApiEndpoint,
    store: StateStore,
    operation: MetadataOperation,
    now_iso: str,
    deadline: float,
    clock: Callable[[], float],
) -> MetadataRecoveryStatus:
    state = MetadataOperationState
    if operation.state is state.PREPARED:
        # No mark means definitely no delivery. Only the original marking handle
        # could admit transport; recovery can release, never acquire that right.
        if store.writes_disabled:
            store.void_prepared_metadata_after_write_stop(operation, iso=now_iso)
        else:
            store.cancel_prepared_metadata_operation(operation, iso=now_iso)
        return MetadataRecoveryStatus.UNSENT_RELEASED
    if not operation.required_targets:
        return MetadataRecoveryStatus.INVALID_COVERAGE

    if operation.reservation_owned:
        expired = operation.verification_deadline_iso is None or _aware_time(
            now_iso
        ) >= _aware_time(operation.verification_deadline_iso)
        if expired:
            # Record the immutable verification timeout before slow reads can
            # exhaust this cycle. Future read-only verification can still settle it.
            operation = store.record_metadata_operation(
                operation, state.NEEDS_ATTENTION, iso=now_iso
            )

    usable = _usable(endpoint, operation, deadline, clock)
    if clock() >= deadline:
        return (
            MetadataRecoveryStatus.NEEDS_ATTENTION
            if operation.state is state.NEEDS_ATTENTION
            else MetadataRecoveryStatus.DEADLINE
        )
    # Read targets even when queue access is forbidden or slow. Queue absence
    # alone proves nothing, and target availability during a refresh is unstable.
    inventory = fetch_queue_inventory(endpoint, deadline=deadline, clock=clock)
    presence = inventory.lookup(QueueJobKind.ANIME_METADATA, operation.anidb_anime_id).presence
    if usable and presence is Presence.ABSENT and _usable(endpoint, operation, deadline, clock):
        store.record_metadata_operation(
            operation,
            state.READY,
            iso=now_iso,
            verified_required_targets=operation.required_targets,
        )
        return MetadataRecoveryStatus.READY

    if operation.state is state.NEEDS_ATTENTION:
        return MetadataRecoveryStatus.NEEDS_ATTENTION
    # Unowned external observations remain adoptable if their job disappears
    # without usable metadata. They never own capacity or an own delivery marker.
    return (
        MetadataRecoveryStatus.DEADLINE
        if clock() >= deadline
        else {
            Presence.PRESENT: MetadataRecoveryStatus.WAITING_QUEUE,
            Presence.UNKNOWN: MetadataRecoveryStatus.QUEUE_UNKNOWN,
            Presence.ABSENT: MetadataRecoveryStatus.WAITING_METADATA,
        }[presence]
    )


def recover_metadata_operations(
    endpoint: ApiEndpoint,
    store: StateStore,
    *,
    source_key: str,
    now_iso: str,
    deadline: float,
    max_rows: int,
    clock: Callable[[], float] = time.monotonic,
) -> MetadataRecoveryReport:
    """Inspect current-server operations, with no remote writes or automatic replay.

    Report every visited/decoded identity in processed_count, including a row
    settled or removed after selection, to share the caller's recovery allowance.
    Terminal rows need no I/O. Persistent per-source rotation prevents one waiting
    row starving others. A latched local write stop can prevent recording observations
    or advancing the cursor; it never authorizes a request or clears the stop.
    """
    if source_key != metadata_source_key(endpoint):
        raise ValueError("metadata source does not match the current endpoint")
    if not math.isfinite(deadline):
        raise ValueError("metadata recovery deadline must be finite")
    if type(max_rows) is not int or max_rows < 0:
        raise ValueError("metadata recovery row limit must be nonnegative")
    _aware_time(now_iso)
    cursor_key = f"metadata_recovery_cursor:v1:{source_key}"
    try:
        cursor = int(store.get_meta(cursor_key) or "0")
    except (ValueError, StateError):
        cursor = 0
    batch = store.metadata_recovery_batch(
        source_key,
        after_id=cursor,
        limit=max_rows if clock() < deadline else 0,
    )
    observations: list[MetadataRecoveryObservation] = []
    visited = 0
    for anime in batch.ids:
        if clock() >= deadline:
            break
        visited += 1
        try:
            operation = store.get_metadata_operation(source_key, anime)
        except (StateError, ValueError):
            operation = None
        try:
            status: MetadataRecoveryStatus | None
            terminal_status = {
                MetadataOperationState.READY: MetadataRecoveryStatus.READY,
                MetadataOperationState.DEFINITELY_REJECTED: None,
            }
            if operation is not None and operation.state in terminal_status:
                # Another handle settled this selected row. Charge the visit but
                # neither re-verify terminal history nor mutate its reservation.
                status = terminal_status[operation.state]
            else:
                status = (
                    MetadataRecoveryStatus.DEADLINE
                    if clock() >= deadline
                    else MetadataRecoveryStatus.INVALID_COVERAGE
                    if operation is None
                    else _recover_row(endpoint, store, operation, now_iso, deadline, clock)
                )
        except (StateError, ValueError, httpx.HTTPError):
            status = MetadataRecoveryStatus.STATE_UNAVAILABLE
        if status is not None:
            observations.append(MetadataRecoveryObservation(anime, status))
            with suppress(StateError):
                observe_metadata_status(
                    store,
                    source_key,
                    anime,
                    "recovery:" + status.value,
                    now_iso,
                    operation_snapshot=operation,
                )
        with suppress(StateError):
            store.set_meta(cursor_key, str(anime))
    return MetadataRecoveryReport(visited, batch.total - visited, tuple(observations))
