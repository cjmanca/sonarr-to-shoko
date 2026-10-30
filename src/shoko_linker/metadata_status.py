"""Bounded, local-only metadata progress; never an authorization input."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Any

from .file_display import MAX_ANIDB_ID, series_display_name
from .linking import LinkPlan, MetadataDemand, PlanStatus
from .logging_setup import diagnostic_text
from .state import MetadataOperation, MetadataOperationState, StateStore

STATUS_KEY = "service_metadata_status:v1"
MAX_EXAMPLES = 20
MAX_DOCUMENT_CHARS = 65536
_IDENTITY = re.compile(r"[a-f0-9]{64}\Z")


@dataclass(frozen=True)
class MetadataStatusExample:
    anime_id: int
    shoko_file_id: int | None
    filename: str
    sonarr_path: str
    shoko_path: str
    state: str
    message: str
    observed_iso: str
    next_read_only_check_iso: str | None = None
    series_name: str = ""
    known_anidb_ids: tuple[int, ...] = ()


@dataclass(frozen=True)
class MetadataStatusView:
    enabled: bool = False
    message: str = (
        "Automatic missing-metadata retrieval is off. Enable it in Settings and confirm setup. "
        "Previously accepted Shoko jobs still receive read-only verification."
    )
    examples: tuple[MetadataStatusExample, ...] = ()


def _rows(store: StateStore, source_key: str) -> list[dict[str, Any]]:
    return _decode_rows(store.bounded_display_meta(STATUS_KEY), source_key)


def _decode_rows(raw: str | None, source_key: str) -> list[dict[str, Any]]:
    if not raw or len(raw) > MAX_DOCUMENT_CHARS:
        return []
    try:
        doc = json.loads(raw)
    except (ValueError, RecursionError):
        return []
    if (
        not isinstance(doc, dict)
        or type(doc.get("version")) is not int
        or doc.get("version") != 1
        or doc.get("source_key") != source_key
        or not isinstance(doc.get("examples"), list)
        or len(doc["examples"]) > MAX_EXAMPLES
    ):
        return []
    rows: list[dict[str, Any]] = []
    for row in doc["examples"]:
        if not isinstance(row, dict) or not _positive(row.get("anime_id")):
            continue
        claim = row.get("claim")
        if claim is not None and (
            not isinstance(claim, str) or not re.fullmatch(r"[a-f0-9]{32}", claim)
        ):
            continue
        rows.append(
            {
                "anime_id": row["anime_id"],
                "claim": claim,
                "demand_fingerprint": _text(row, "demand_fingerprint", 64),
                "scope_fingerprint": _text(row, "scope_fingerprint", 64),
                "shoko_file_id": row.get("shoko_file_id")
                if _positive(row.get("shoko_file_id"))
                else None,
                **{
                    name: _text(row, name)
                    for name in ("filename", "sonarr_path", "shoko_path", "series_name")
                },
                "reason": _text(row, "reason", 128),
                "observed_iso": _text(row, "observed_iso", 64),
            }
        )
    return rows


def _positive(value: object) -> bool:
    return type(value) is int and 0 < value <= MAX_ANIDB_ID


def _text(row: dict[str, Any], name: str, limit: int = 512) -> str:
    value = row.get(name)
    return diagnostic_text(value, limit=limit) if isinstance(value, str) else ""


def _save(
    store: StateStore, source_key: str, row: dict[str, Any], expected_document: str | None
) -> None:
    # Latest observation first, one anime per source. A changed source replaces
    # the document; historical server context is never presented as current.
    rows = [
        item
        for item in _decode_rows(expected_document, source_key)
        if item.get("anime_id") != row["anime_id"]
    ]
    rows.insert(0, row)
    store.save_metadata_status_if_current(
        key=STATUS_KEY,
        source_key=source_key,
        anime_id=row["anime_id"],
        expected_claim=row.get("claim"),
        expected_document_json=expected_document,
        document_json=json.dumps(
            {"version": 1, "source_key": source_key, "examples": rows[:MAX_EXAMPLES]},
            ensure_ascii=False,
        ),
    )


def observe_metadata_status(
    store: StateStore,
    source_key: str,
    anime_id: int,
    reason: str,
    observed_iso: str,
    *,
    demand: MetadataDemand | None = None,
    operation_snapshot: MetadataOperation | None = None,
) -> None:
    """Attach actual eligible demand, or update its exact durable generation.

    A retained operation may have been requested for another file. Its claim and
    demand fingerprint fence context: never infer a file from an anime ID alone.
    """
    if not _IDENTITY.fullmatch(source_key) or not _positive(anime_id):
        return
    if demand is not None and anime_id not in {anime for anime, _, _ in demand.required_targets}:
        return
    expected_document = store.get_meta(STATUS_KEY)
    operation = operation_snapshot or store.get_metadata_operation(source_key, anime_id)
    if operation is not None and (
        operation.source_key != source_key or operation.anidb_anime_id != anime_id
    ):
        return
    claim = operation.claim if operation is not None else None
    existing = next(
        (
            row
            for row in _decode_rows(expected_document, source_key)
            if row.get("anime_id") == anime_id
        ),
        None,
    )
    row: dict[str, Any] = {}
    if existing is not None and (
        existing.get("claim") == claim
        or (
            existing.get("claim") is None
            and operation is not None
            and existing.get("demand_fingerprint") == operation.demand_fingerprint
        )
    ):
        # Re-sanitize even legacy/corrupted local data before persisting it again.
        row = {
            "shoko_file_id": existing.get("shoko_file_id")
            if _positive(existing.get("shoko_file_id"))
            else None,
            **{
                name: _text(existing, name)
                for name in ("filename", "sonarr_path", "shoko_path", "series_name")
            },
            "demand_fingerprint": _text(existing, "demand_fingerprint", 64),
            "scope_fingerprint": _text(existing, "scope_fingerprint", 64),
        }
    if demand is not None and (
        operation is None or operation.demand_fingerprint == demand.fingerprint
    ):
        # Sanitize the whole path before basename extraction: splitting a URL
        # first would strip its scheme and evade URL-payload scrubbing.
        path = diagnostic_text(demand.sonarr_path, limit=MAX_DOCUMENT_CHARS)
        row.update(
            series_name=series_display_name(demand.series_name),
            shoko_file_id=demand.shoko_file_id,
            filename=diagnostic_text(path.replace("\\", "/").rsplit("/", 1)[-1]),
            sonarr_path=diagnostic_text(path),
            shoko_path=diagnostic_text(demand.shoko_paths[0]) if demand.shoko_paths else "",
            demand_fingerprint=demand.fingerprint,
            scope_fingerprint=demand.scope_fingerprint,
        )
    if (
        existing is not None
        and existing.get("claim") == claim
        and reason
        in {"metadata_waiting", "metadata_operation_retained", "metadata_operation_exists"}
    ):
        reason = _text(existing, "reason", 128) or reason
    row.update(
        anime_id=anime_id,
        claim=claim,
        reason=diagnostic_text(reason, limit=128),
        observed_iso=diagnostic_text(observed_iso, limit=64),
    )
    _save(store, source_key, row, expected_document)


def observe_resolved_metadata_plans(
    store: StateStore, source_key: str, plans: Mapping[int, LinkPlan], observed_iso: str
) -> None:
    """Settle OFF/manual-arrival waits using an actual freshly resolved file plan.

    Own operations still require their stronger read-only queue/coverage recovery.
    This only updates demand observations that never acquired an operation claim.
    """
    candidates = [row["anime_id"] for row in _rows(store, source_key)]
    for anime in candidates:
        expected_document = store.get_meta(STATUS_KEY)
        row = next(
            (
                item
                for item in _decode_rows(expected_document, source_key)
                if item["anime_id"] == anime
            ),
            None,
        )
        if row is None:
            continue
        file_id = row.get("shoko_file_id")
        plan = plans.get(file_id) if type(file_id) is int else None
        if (
            plan is None
            or plan.status is not PlanStatus.LINKABLE
            or row.get("claim") is not None
            or store.get_metadata_operation(source_key, row["anime_id"]) is not None
            or diagnostic_text(plan.sonarr_path) != row.get("sonarr_path")
            or row["anime_id"] not in {target.anidb_anime_id for target in plan.targets}
        ):
            continue
        row.update(reason="metadata_replan", observed_iso=diagnostic_text(observed_iso, limit=64))
        _save(store, source_key, row, expected_document)


def _description(reason: str) -> tuple[str, str]:  # noqa: PLR0911, PLR0912 - exact classification
    if reason == "metadata_disabled":
        return (
            "disabled",
            "Metadata is missing. Enable automatic retrieval in Settings and confirm setup, "
            "or retrieve it in Shoko.",
        )
    if reason == "metadata_accepted_waiting":
        return "accepted", "Shoko accepted the request. Waiting for verified episode metadata."
    if reason == "metadata_delivery_unknown":
        return (
            "uncertain",
            "Delivery is uncertain. Only read-only verification will run; "
            "this request will not be replayed.",
        )
    if reason in {"metadata_waiting_external_queue", "recovery:waiting_queue"}:
        return (
            "waiting_queue",
            "Shoko metadata work is queued, running or blocked. "
            "Waiting without adding another request.",
        )
    if reason == "metadata_queue_busy":
        return (
            "waiting_queue",
            "Shoko has queued, running or blocked jobs. Metadata retrieval waits for an empty "
            "queue at a later service cycle.",
        )
    if reason == "metadata_waiting_external_file_queue":
        return (
            "waiting_file_queue",
            "A Shoko link job for this file is already queued, running or blocked. Waiting.",
        )
    if reason in {"metadata_queue_not_clear", "recovery:queue_unknown"}:
        return (
            "queue_unknown",
            "Queue visibility is incomplete. Check that the Shoko API key can access "
            "the admin queue inventory. New requests wait.",
        )
    if reason in {"metadata_replan", "recovery:ready"}:
        return (
            "ready",
            "Required metadata is verified. "
            "Normal reconciliation handles the complete file link plan.",
        )
    if reason == "recovery:needs_attention":
        return (
            "needs_attention",
            "Metadata verification is overdue. Read-only checks continue; inspect Shoko jobs. "
            "The request will not be replayed.",
        )
    if reason in {"metadata_waiting", "recovery:waiting_metadata", "recovery:deadline"}:
        return "waiting", "Waiting for usable episode metadata; the service will check again."
    if reason == "metadata_prepared":
        return "prepared", "A request is prepared locally; delivery to Shoko is not yet confirmed."
    if reason == "metadata_queue_unavailable_or_blocked":
        return (
            "queue_blocked",
            "Shoko queue checks are unavailable or show congestion. New requests wait.",
        )
    if reason in {
        "metadata_generation_or_overrides_changed",
        "metadata_authorization_or_demand_changed",
        "metadata_anime_not_required",
        "metadata_authorization_changed",
        "metadata_authorization_or_generation_changed",
        "metadata_file_preflight_refused",
        "metadata_sonarr_source_changed_or_unavailable",
    }:
        return (
            "safety_hold",
            "Source, mappings or authorization need a fresh safety check before retrieval.",
        )
    if reason in {
        "metadata_targets_unavailable",
        "metadata_state_or_guard_unavailable",
        "recovery:state_unavailable",
        "recovery:invalid_coverage",
    }:
        return (
            "needs_attention",
            "Metadata or local verification state could not be checked safely.",
        )
    if reason in {
        "metadata_service_not_eligible",
        "metadata_pending_files_exhausted",
        "metadata_pending_episode_jobs_exhausted",
        "metadata_cycle_new_exhausted",
        "metadata_rolling_new_exhausted",
        "metadata_not_sent",
        "recovery:unsent_released",
    }:
        return (
            "deferred",
            "Metadata retrieval is deferred by current service limits or safety checks.",
        )
    return (
        "needs_attention",
        "A metadata check needs attention. The service will retry safely "
        "without replaying delivered requests.",
    )


def metadata_status_view(
    store: StateStore, source_key: str, *, enabled: bool
) -> MetadataStatusView:
    """Project bounded local observations, checking the current operation claim."""
    examples: list[MetadataStatusExample] = []
    for row in _rows(store, source_key):
        anime = row.get("anime_id")
        if type(anime) is not int or anime <= 0:
            continue
        operation = store.get_metadata_operation(source_key, anime)
        if row.get("claim") != (operation.claim if operation else None) and not (
            row.get("claim") is None
            and operation is not None
            and row.get("demand_fingerprint") == operation.demand_fingerprint
        ):
            continue
        reason = _text(row, "reason", 128)
        # The durable ledger outranks a stale scheduler observation after a crash.
        if operation is not None:
            authoritative = {
                MetadataOperationState.PREPARED: "metadata_prepared",
                MetadataOperationState.READY: "recovery:ready",
                MetadataOperationState.NEEDS_ATTENTION: "recovery:needs_attention",
                MetadataOperationState.POSSIBLY_DELIVERED: "metadata_delivery_unknown",
                MetadataOperationState.DEFINITELY_REJECTED: "metadata_not_sent",
            }
            reason = authoritative.get(operation.state, reason)
            if operation.state is MetadataOperationState.ACCEPTED_WAITING and reason in {
                "metadata_waiting",
                "metadata_operation_retained",
                "metadata_operation_exists",
                "metadata_disabled",
            }:
                reason = "metadata_accepted_waiting"
        elif not enabled and reason != "metadata_replan":
            reason = "metadata_disabled"
        state, message = _description(reason)
        examples.append(
            MetadataStatusExample(
                anime_id=anime,
                series_name=series_display_name(row.get("series_name")),
                known_anidb_ids=(anime,),
                shoko_file_id=row.get("shoko_file_id")
                if _positive(row.get("shoko_file_id"))
                else None,
                filename=_text(row, "filename"),
                sonarr_path=_text(row, "sonarr_path"),
                shoko_path=_text(row, "shoko_path"),
                state=state,
                message=message,
                observed_iso=_text(row, "observed_iso", 64),
            )
        )
    return MetadataStatusView(
        enabled=enabled,
        message=(
            "Automatic missing-metadata retrieval is enabled; "
            "current consent and safety checks still apply."
            if enabled
            else MetadataStatusView().message
        ),
        examples=tuple(examples),
    )


def metadata_status_payload(view: MetadataStatusView) -> dict[str, Any]:
    return asdict(view)
