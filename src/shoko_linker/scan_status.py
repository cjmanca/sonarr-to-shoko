"""Bounded local evidence from the last complete unrecognized-file reconciliation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from .file_display import FileDisplay, file_display
from .logging_setup import diagnostic_text
from .onboarding import OnboardingError, OnboardingManager
from .state import (
    MAX_SCAN_EVIDENCE_CHARS,
    META_LAST_DISCOVERY_EXCLUSIONS,
    META_SERVICE_DRAFT_ROOTS,
    META_SERVICE_SETTINGS,
    SCAN_GENERATION_CHARS,
    ScanExclusionRow,
    StateStore,
    TablePage,
    table_page,
    validate_table_page,
)

if TYPE_CHECKING:
    from .config import Settings

_MAX_DOCUMENT_CHARS = MAX_SCAN_EVIDENCE_CHARS
_MAX_TIMESTAMP_CHARS = 64
_MAX_EXCLUSION_EXAMPLES = 20
_MAX_EXCLUSION_REASONS = 64
_MAX_REASON_CHARS = 128
_MAX_DETAIL_CHARS = 512


@dataclass(frozen=True)
class ScanSummary:
    """A historical scan partition, independent of durable tracked-file states."""

    context_identity: str
    observed_from: str
    observed_until: str
    reported_unrecognized: int | None
    observed_files: int
    matched_files: int
    excluded_files: int
    outside_scope_files: int


@dataclass(frozen=True)
class ScanExclusionExample:
    shoko_file_id: int
    reason: str
    detail: str


@dataclass(frozen=True)
class ScanEvidence:
    """One complete validated scan document, never a mixture of worker updates."""

    summary: ScanSummary
    exclusion_counts: tuple[tuple[str, int], ...]
    examples: tuple[ScanExclusionExample, ...]
    rows_generation: str | None = None


@dataclass(frozen=True)
class ScanExclusionView:
    row: ScanExclusionRow
    display: FileDisplay


@dataclass(frozen=True)
class ScanExclusionPage:
    evidence: ScanEvidence
    pagination: TablePage
    rows: tuple[ScanExclusionView, ...]
    full_rows_available: bool
    message: str


def scan_context_identity(  # noqa: PLR0911 - reject absent or mismatched persisted scope
    store: StateStore,
    sonarr_url: str,
    shoko_url: str,
    *,
    settings: Settings | None = None,
) -> str | None:
    """Identify confirmed sources/scope without persisting URLs, paths or credentials.

    A pending scope edit has no current confirmed partition. Safety-consent revisions
    unrelated to scope do not invalidate previously observed file counts.
    """
    if store.get_meta(META_SERVICE_DRAFT_ROOTS) is not None:
        return None
    preferences = store.get_meta(META_SERVICE_SETTINGS)
    if preferences is not None:
        try:
            saved = json.loads(preferences) if len(preferences) <= _MAX_DOCUMENT_CHARS else None
        except (ValueError, RecursionError):
            return None
        if not isinstance(saved, dict) or (saved.get("sonarr_url"), saved.get("shoko_url")) != (
            sonarr_url,
            shoko_url,
        ):
            return None
    try:
        record = OnboardingManager(store).record()
    except (OnboardingError, ValueError, TypeError, KeyError, RecursionError):
        return None
    if record is None:
        return None
    binding = record.binding
    if (binding.sonarr_url, binding.shoko_url) != (sonarr_url, shoko_url):
        return None
    roots = tuple(sorted(binding.roots))
    mappings = tuple(sorted((item.sonarr, item.shoko) for item in binding.path_mappings))
    if settings is not None and (
        tuple(sorted(settings.sonarr.allowed_roots)) != roots
        or tuple(sorted((item.sonarr, item.shoko) for item in settings.path_mappings)) != mappings
        or tuple(sorted(settings.shoko.allowed_roots))
        != tuple(sorted({item[1] for item in mappings}))
    ):
        return None
    payload = (sonarr_url, shoko_url, roots, mappings, binding.selected_folders_observation)
    return hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()


def _timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or len(value) > _MAX_TIMESTAMP_CHARS:
        return None
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.astimezone(UTC) if parsed.tzinfo is not None else None
    except (ValueError, OverflowError):
        return None


def scan_summary_payload(summary: ScanSummary) -> dict[str, Any]:
    """Encode the small structured summary alongside its exclusion evidence."""
    return {"version": 1, **asdict(summary)}


def _read_scan_document(store: StateStore) -> dict[str, Any] | None:
    raw = store.get_meta(META_LAST_DISCOVERY_EXCLUSIONS)
    if raw is None or len(raw) > _MAX_DOCUMENT_CHARS:
        return None
    try:
        document = json.loads(raw)
    except (ValueError, RecursionError):
        return None
    return document if isinstance(document, dict) else None


def _decode_scan_summary(
    document: dict[str, Any], context_identity: str | None
) -> ScanSummary | None:
    if context_identity is None:
        return None
    value = document.get("scan")
    if (
        not isinstance(value, dict)
        or type(value.get("version")) is not int
        or value["version"] != 1
        or value.get("context_identity") != context_identity
    ):
        return None
    fields = ("observed_files", "matched_files", "excluded_files", "outside_scope_files")
    if any(type(value.get(name)) is not int or value[name] < 0 for name in fields):
        return None
    reported = value.get("reported_unrecognized")
    if reported is not None and (type(reported) is not int or reported < 0):
        return None
    start, end = _timestamp(value.get("observed_from")), _timestamp(value.get("observed_until"))
    if (
        start is None
        or end is None
        or start > end
        or value["matched_files"] + value["excluded_files"] != value["observed_files"]
        or value["outside_scope_files"] > value["excluded_files"]
        or type(document.get("total")) is not int
        or type(document.get("scope_excluded")) is not int
        or document.get("total") != value["excluded_files"]
        or document.get("scope_excluded") != value["outside_scope_files"]
    ):
        return None
    return ScanSummary(
        context_identity=context_identity,
        observed_from=start.isoformat(),
        observed_until=end.isoformat(),
        reported_unrecognized=reported,
        **{name: value[name] for name in fields},
    )


def last_scan_summary(store: StateStore, context_identity: str | None) -> ScanSummary | None:
    """Compatibility projection for callers needing only the historical partition."""
    document = _read_scan_document(store)
    return None if document is None else _decode_scan_summary(document, context_identity)


def last_scan_evidence(  # noqa: PLR0911 - reject inconsistent local scan evidence
    store: StateStore, sonarr_url: str, shoko_url: str
) -> ScanEvidence | None:
    """Read one bounded scan document and fence its complete projection by current scope."""
    context = scan_context_identity(store, sonarr_url, shoko_url)
    document = _read_scan_document(store)
    if document is None:
        return None
    summary = _decode_scan_summary(document, context)
    counts, examples = document.get("counts"), document.get("examples")
    if (
        summary is None
        or type(document.get("version")) is not int
        or document["version"] != 1
        or not isinstance(counts, dict)
        or len(counts) > _MAX_EXCLUSION_REASONS
        or not isinstance(examples, list)
        or len(examples) > _MAX_EXCLUSION_EXAMPLES
        or len(examples) > summary.excluded_files
    ):
        return None
    if (
        any(
            not isinstance(reason, str)
            or len(reason) > _MAX_REASON_CHARS
            or diagnostic_text(reason) != reason
            or type(count) is not int
            or count < 0
            for reason, count in counts.items()
        )
        or sum(counts.values()) != summary.excluded_files
    ):
        return None
    decoded = []
    seen: set[int] = set()
    for item in examples:
        if (
            not isinstance(item, dict)
            or type(item.get("shoko_file_id")) is not int
            or item["shoko_file_id"] <= 0
            or item["shoko_file_id"] in seen
            or not isinstance(item.get("reason"), str)
            or item.get("reason") not in counts
            or not isinstance(item.get("detail"), str)
        ):
            return None
        seen.add(item["shoko_file_id"])
        decoded.append(
            ScanExclusionExample(
                item["shoko_file_id"],
                item["reason"],
                diagnostic_text(item["detail"], limit=_MAX_DETAIL_CHARS),
            )
        )
    if context != scan_context_identity(store, sonarr_url, shoko_url):
        return None
    generation = document.get("exclusion_rows_generation")
    if generation is not None and (
        not isinstance(generation, str)
        or len(generation) != SCAN_GENERATION_CHARS
        or any(char not in "0123456789abcdef" for char in generation)
    ):
        return None
    return ScanEvidence(summary, tuple(sorted(counts.items())), tuple(decoded), generation)


def scan_exclusion_page(
    store: StateStore,
    sonarr_url: str,
    shoko_url: str,
    *,
    page: int = 1,
    page_size: int = 50,
) -> ScanExclusionPage | None:
    """All latest exclusions, or explicitly limited retained legacy examples."""
    validate_table_page(page, page_size)
    evidence = last_scan_evidence(store, sonarr_url, shoko_url)
    if evidence is None:
        return None
    if evidence.rows_generation is None:
        pagination = table_page(len(evidence.examples), page, page_size)
        offset = (pagination.page - 1) * page_size
        rows = tuple(
            ScanExclusionRow(item.shoko_file_id, item.reason, item.detail, paths_complete=False)
            for item in evidence.examples[offset : offset + page_size]
        )
        full_rows_available = False
        message = (
            f"Showing retained examples only ({len(evidence.examples)} of "
            f"{evidence.summary.excluded_files} exclusions). Complete rows will be available "
            "after the next successful scan."
        )
    else:
        result = store.latest_scan_exclusion_page(
            evidence.rows_generation,
            evidence.summary.excluded_files,
            page=page,
            page_size=page_size,
        )
        if result is None:
            return None
        pagination, rows = result.pagination, result.rows
        full_rows_available = True
        message = "Files excluded in the last completed scan; not current Shoko queue jobs."
    try:
        record = OnboardingManager(store).record()
    except (OnboardingError, ValueError, RecursionError):
        return None
    if record is None:
        return None
    binding = record.binding
    views = tuple(
        ScanExclusionView(
            row,
            file_display(
                row.sonarr_path,
                row.shoko_paths,
                binding.roots,
                binding.path_mappings,
                paths_complete=row.paths_complete,
            ),
        )
        for row in rows
    )
    if scan_context_identity(store, sonarr_url, shoko_url) != evidence.summary.context_identity:
        return None
    return ScanExclusionPage(evidence, pagination, views, full_rows_available, message)
