"""Local, bounded request ledger history; never evidence authorizing a write."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any

from .file_display import safe_display_path
from .logging_setup import diagnostic_text
from .state import ServiceFileCounts, StateStore, TablePage

_MIN_HTTP_STATUS = 100
_MAX_HTTP_STATUS = 599


@dataclass(frozen=True)
class RecentAction:
    action_id: str
    operation: str
    subject_id: int
    requested_iso: str
    recorded_iso: str
    state: str
    message: str
    filename: str = ""
    current_path: str = ""
    http_status: int | None = None
    current_path_complete: bool = False
    series_name: str = ""
    known_anidb_ids: tuple[int, ...] = ()
    series_display_context: tuple[str, str] | None = None


@dataclass(frozen=True)
class ActionHistoryView:
    total: int = 0
    examples: tuple[RecentAction, ...] = ()
    scope_message: str = (
        "Retained linker request records across all configured server sources and modes. "
        "Paths and legacy series labels are current retained context, not historical snapshots. "
        "Prepared or possibly sent requests do not prove an HTTP request reached Shoko. "
        "This is not an audit of media changes or Shoko hooks."
    )


@dataclass(frozen=True)
class ActionHistoryPage:
    pagination: TablePage
    counts: ServiceFileCounts
    rows: tuple[RecentAction, ...]
    scope_message: str = ActionHistoryView().scope_message


_FILE_MESSAGES = {
    "prepared": "Prepared locally; not sent.",
    "voided": "Cancelled before sending.",
    "attempted": "Request may have been sent; delivery is unconfirmed.",
    "accepted_unverified": "Shoko accepted the request; verified linking is not yet recorded.",
    "ambiguous_send": "Delivery is uncertain; automatic replay is blocked.",
    "rejected": "Shoko rejected the request; no link was confirmed.",
    "refused": "Request refused; no link was confirmed.",
    "verified": "The complete requested episode associations were verified in Shoko.",
    "inspected_review": "Read-only verification found associations requiring review.",
    "resolved": "An operator resolved this request record; this does not imply successful linking.",
}
_METADATA_MESSAGES = {
    "prepared": "Metadata retrieval prepared locally; not sent.",
    "possibly_delivered": "Metadata request delivery is uncertain; automatic replay is blocked.",
    "accepted_waiting": "Shoko accepted metadata retrieval; required metadata is not yet verified.",
    "ready": (
        "Required metadata was verified available. This does not prove this request created it "
        "or that any file was linked."
    ),
    "definitely_rejected": "Metadata request was rejected or cancelled before delivery.",
    "needs_attention": "Metadata request needs attention; completion is unconfirmed.",
}


def _text(value: Any) -> str:
    return diagnostic_text(value) if isinstance(value, str) else ""


def _iso(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return ""
    return parsed.isoformat() if parsed.tzinfo is not None else ""


def action_history_view(store: StateStore) -> ActionHistoryView:
    """No HTTP, state mutation, or filesystem inspection; bounded local display data only."""
    total, rows = store.recent_action_rows()
    return ActionHistoryView(total=total, examples=_actions(rows))


def action_history_page(
    store: StateStore, *, page: int = 1, page_size: int = 50
) -> ActionHistoryPage:
    """Read-only bounded history and current header counts from one SQLite snapshot."""
    result = store.action_rows_page(page=page, page_size=page_size)
    return ActionHistoryPage(result.pagination, result.counts, _actions(result.rows))


def _actions(rows: tuple[dict[str, Any], ...]) -> tuple[RecentAction, ...]:
    examples: list[RecentAction] = []
    for row in rows:
        operation = row["operation"]
        state = row["state"]
        messages = _FILE_MESSAGES if operation == "file_link" else _METADATA_MESSAGES
        message = messages.get(state, "Unrecognized stored result; completion is unknown.")
        if operation == "metadata_refresh" and not row["transport_admitted"]:
            message += " No transport admission was recorded."
        verification = row["verification"]
        if operation == "file_link" and verification in {
            "pending",
            "unknown",
            "partial",
            "contradiction",
        }:
            message += {
                "pending": " Latest verification is still pending.",
                "unknown": " Latest verification could not establish the result.",
                "partial": " Latest verification found only some requested associations.",
                "contradiction": " Latest verification found conflicting associations.",
            }[verification]
        raw_path = row["current_path"]
        safe_path = diagnostic_text(raw_path, limit=8192) if isinstance(raw_path, str) else ""
        path = safe_path
        filename = _text(PurePosixPath(safe_path.replace("\\", "/")).name) if safe_path else ""
        examples.append(
            RecentAction(
                action_id=_text(row["action_id"]),
                series_name=row["series_name"],
                known_anidb_ids=row["known_anidb_ids"],
                series_display_context=row["series_display_context"],
                operation=operation,
                subject_id=row["subject_id"],
                requested_iso=_iso(row["requested_iso"]),
                recorded_iso=_iso(row["recorded_iso"]),
                state=_text(state),
                message=message,
                filename=filename,
                current_path=path,
                current_path_complete=(
                    operation == "file_link"
                    and isinstance(raw_path, str)
                    and safe_path == raw_path
                    and safe_display_path(raw_path) is not None
                ),
                http_status=row["http_status"]
                if type(row["http_status"]) is int
                and _MIN_HTTP_STATUS <= row["http_status"] <= _MAX_HTTP_STATUS
                else None,
            )
        )
    return tuple(examples)
