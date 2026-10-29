"""M7-b: read-only existing-link audit — bounded, on-demand, **zero writes**.

For eligible Sonarr files the audit enumerates matching Shoko files **including already
recognized / manually linked records** (the M3 discovery deliberately excludes
those; the audit re-includes them read-only), compares each file's existing AniDB episode
associations (XRefs) with the verified Sonarr mapping (active generation resolve), and
classifies:

* ``linked_consistent`` — ONLY when the observed XRef identity set **exactly equals**
  the mapping target identity set (the existence of a link never marks it correct);
* ``partial_link`` — a non-empty strict subset of the target set;
* ``existing_link_mismatch`` — links to episodes outside the target set;
* ``wrong_first_episode`` — the file maps to the first episode and its links point at a
  different episode (identity mismatch on ep-1, the PLAN example);
* ``ambiguous_match`` — the verified mapping cannot answer the file's episodes uniquely
  (the audit reports; it never guesses);
* ``no_sonarr_counterpart`` / ``audit_incomplete`` — informational buckets.

The audit is rate-limited (``--max-requests``, default 100 Shoko requests) and makes
**only GETs** — it never unlinks, never repairs, never POSTs. It consumes no state DB and
no budget slots. Exit codes (CLI): 0 informational, 4 transport failure.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any, Final

import httpx

from .config import Settings
from .errors import HttpPolicyError, PathError
from .http import ApiEndpoint, BoundedJsonError, get_bounded_json
from .linking import EpisodeResolutionStatus, ShokoFileXRefs, _match_anidb_episode, parse_file_xrefs
from .mapping_store import ActiveMappings
from .mappings import ResolutionStatus
from .mappings import resolve as resolve_mapping
from .paths import is_within_root, normalize_posix
from .shoko import (
    DROP_TYPE_NONE,
    ImportFolder,
    ShokoFile,
    ShokoLocation,
    join_location_path,
    parse_file_entries,
    parse_import_folders,
    select_reverse_mapping,
    translate_to_sonarr,
)
from .sonarr import SonarrFile, collect_sonarr_inventory

FILE_PATH: Final[str] = "api/v3/File"
IMPORT_FOLDER_PATH: Final[str] = "api/v3/ImportFolder"
AUDIT_INCLUDE_FLAGS: Final[str] = "XRefs,AbsolutePaths"
PAGE_SIZE: Final[int] = 1000
MIN_SUCCESS_STATUS: Final[int] = 200
MAX_SUCCESS_STATUS: Final[int] = 300
DEFAULT_MAX_REQUESTS: Final[int] = 100
DEFAULT_LIMIT: Final[int] = 100
SEASON_ONE: Final[int] = 1
EPISODE_ONE: Final[int] = 1


class AuditClass(Enum):
    """Classification of one linked Shoko file against the verified mapping."""

    LINKED_CONSISTENT = "linked_consistent"
    EXISTING_LINK_MISMATCH = "existing_link_mismatch"
    PARTIAL_LINK = "partial_link"
    WRONG_FIRST_EPISODE = "wrong_first_episode"
    AMBIGUOUS_MATCH = "ambiguous_match"
    NO_SONARR_COUNTERPART = "no_sonarr_counterpart"
    AUDIT_INCOMPLETE = "audit_incomplete"


@dataclass(frozen=True)
class AuditFinding:
    """One classified Shoko file (identity fields only; no credential material)."""

    shoko_file_id: int
    classification: AuditClass
    sonarr_series_id: int | None
    sonarr_file_id: int | None
    linked_identities: tuple[tuple[int, int], ...]
    target_identities: tuple[tuple[int, int], ...]
    detail: str


@dataclass(frozen=True)
class AuditReport:
    """The bounded audit outcome, with linked_examined counting filter-eligible files."""

    findings: tuple[AuditFinding, ...]
    examined: int
    linked_examined: int
    requests_made: int
    max_requests: int
    truncated: bool
    failures: tuple[str, ...]
    counts: Mapping[str, int]

    @property
    def transport_failed(self) -> bool:
        """True when the Shoko reads never produced a usable file listing."""
        return any(failure.startswith("listing_") for failure in self.failures)


class _Budget:
    """Shoko request budget: every GET costs one; the audit stops at the cap."""

    def __init__(self, max_requests: int) -> None:
        self.max_requests = max_requests
        self.used = 0

    @property
    def exhausted(self) -> bool:
        return self.used >= self.max_requests

    def spend(self) -> bool:
        if self.exhausted:
            return False
        self.used += 1
        return True


def classify_link(
    linked: frozenset[tuple[int, int]],
    targets: frozenset[tuple[int, int]],
    *,
    first_episode_target: bool,
) -> AuditClass:
    """Exact-set classification: existence alone never marks a link consistent."""
    if not linked:
        return AuditClass.LINKED_CONSISTENT if not targets else AuditClass.PARTIAL_LINK
    if linked == targets:
        return AuditClass.LINKED_CONSISTENT
    if linked < targets:
        return AuditClass.PARTIAL_LINK
    if linked & targets:
        return AuditClass.EXISTING_LINK_MISMATCH
    if first_episode_target:
        return AuditClass.WRONG_FIRST_EPISODE
    return AuditClass.EXISTING_LINK_MISMATCH


@dataclass
class _AuditContext:
    """One audit pass's read-only inputs (shared by every classified file row)."""

    shoko: ApiEndpoint
    settings: Settings
    active: ActiveMappings
    folders: Mapping[int, ImportFolder]
    sonarr_index: dict[str, list[SonarrFile]]
    series_tvdb: Mapping[int, int]
    budget: _Budget
    cache: dict[tuple[int, str, int], _EpisodeIdLookup]


@dataclass(frozen=True)
class _EpisodeIdLookup:
    identity: tuple[int, int] | None
    reason: str | None = None


def run_audit(
    *,
    shoko: ApiEndpoint,
    settings: Settings,
    active: ActiveMappings,
    sonarr_files: Sequence[SonarrFile],
    series_tvdb: Mapping[int, int],
    series_anidb: int | None = None,
    limit: int = DEFAULT_LIMIT,
    max_requests: int = DEFAULT_MAX_REQUESTS,
) -> AuditReport:
    """One bounded audit pass. **Only GETs** are issued (asserted by construction).

    Enumerate Shoko files with ``include=XRefs,AbsolutePaths`` (no ``include_only``
    filter: already-linked records are audited), keep files carrying cross-references
    (and, when ``series_anidb`` is given, only files whose XRefs name that AniDB series),
    translate each file's locations through the configured path mappings (M3 ownership,
    all locations), match the Sonarr inventory by exact path+size, resolve the
    verified mapping targets, and classify the observed link set. At most ``limit`` linked
    files matching the optional AniDB series filter are examined; ``linked_examined`` counts
    only those eligible files. File enumeration remains paged and bounded by
    ``max_requests``, which also caps Shoko GETs for target resolution.
    """
    budget = _Budget(max_requests)
    failures: list[str] = []
    folders, folder_reasons, _folder_details = _fetch_import_folders(shoko, budget)
    failures.extend(f"listing_import_folders_{reason}" for reason in folder_reasons)
    context = _AuditContext(
        shoko=shoko,
        settings=settings,
        active=active,
        folders=folders,
        sonarr_index=_index_sonarr(sonarr_files),
        series_tvdb=series_tvdb,
        budget=budget,
        cache={},
    )
    rows, page_failures, truncated = _fetch_file_rows(shoko, budget, enabled=not folder_reasons)
    failures.extend(page_failures)
    findings: list[AuditFinding] = []
    linked_examined = 0
    for entry in rows:
        parsed = parse_file_xrefs(entry)
        if parsed is None:
            findings.append(
                AuditFinding(
                    0, AuditClass.AUDIT_INCOMPLETE, None, None, (), (), "malformed linked-file row"
                )
            )
            continue
        if parsed.file_id <= 0 or not parsed.xrefs:
            continue  # unlinked files are the normal linking path, not an audit finding
        if series_anidb is not None and not any(
            xref.series_anidb_id == series_anidb for xref in parsed.xrefs
        ):
            continue
        if linked_examined >= limit:
            truncated = True
            break
        linked_examined += 1
        findings.append(_audit_entry(context, entry, parsed))
    findings.sort(key=lambda item: (item.shoko_file_id, item.classification.value))
    counts: dict[str, int] = {item.value: 0 for item in AuditClass}
    for finding in findings:
        counts[finding.classification.value] += 1
    return AuditReport(
        findings=tuple(findings),
        examined=len(rows),
        linked_examined=linked_examined,
        requests_made=budget.used,
        max_requests=budget.max_requests,
        truncated=truncated,
        failures=tuple(failures),
        counts=dict(counts),
    )


def _index_sonarr(sonarr_files: Sequence[SonarrFile]) -> dict[str, list[SonarrFile]]:
    index: dict[str, list[SonarrFile]] = {}
    for item in sonarr_files:
        index.setdefault(normalize_posix(item.path), []).append(item)
    return index


def _fetch_file_rows(  # noqa: PLR0912, PLR0915 - each pagination guard fails closed
    shoko: ApiEndpoint, budget: _Budget, *, enabled: bool
) -> tuple[list[dict[str, Any]], list[str], bool]:
    """Paged ``GET File`` enumeration within the budget: ``(rows, failures, truncated)``."""
    rows: list[dict[str, Any]] = []
    failures: list[str] = []
    truncated = False
    if not enabled:
        return rows, failures, truncated
    total: int | None = None
    seen_ids: set[int] = set()
    page = 1
    while True:
        if budget.exhausted:
            truncated = True
            failures.append("budget_exhausted: page fetch")
            break
        budget.used += 1
        outcome = _get(
            shoko,
            FILE_PATH,
            {"include": AUDIT_INCLUDE_FLAGS, "pageSize": PAGE_SIZE, "page": page},
        )
        if outcome is None:
            truncated = True
            failures.append(f"listing_page_{page}_failed_or_malformed")
            break
        page_total, entries = outcome
        if total is None:
            total = page_total
        elif page_total != total:
            truncated = True
            failures.append(
                f"listing_total_changed_page_{page}: total changed from {total} to {page_total}"
            )
            break
        if len(entries) > PAGE_SIZE:
            truncated = True
            failures.append(
                f"listing_page_overshoot_{page}: {len(entries)} rows exceeds pageSize {PAGE_SIZE}"
            )
            break
        page_ids: list[int] = []
        malformed_id = False
        duplicate_id: int | None = None
        for entry in entries:
            if not isinstance(entry, Mapping):
                malformed_id = True
                break
            file_id = entry.get("ID")
            if isinstance(file_id, bool) or not isinstance(file_id, int) or file_id <= 0:
                malformed_id = True
                break
            if file_id in seen_ids or file_id in page_ids:
                duplicate_id = file_id
                break
            page_ids.append(file_id)
        if malformed_id:
            truncated = True
            failures.append(f"listing_malformed_file_id_page_{page}")
            break
        if duplicate_id is not None:
            truncated = True
            failures.append(f"listing_duplicate_file_id_{duplicate_id}_page_{page}")
            break
        if len(rows) + len(entries) > total:
            truncated = True
            failures.append(
                f"listing_page_overshoot_{page}: collected {len(rows) + len(entries)} rows "
                f"exceeds reported Total {total}"
            )
            break
        if not entries and len(rows) < total:
            truncated = True
            failures.append(
                f"listing_premature_empty_page_{page}: collected {len(rows)} of {total} rows"
            )
            break
        if len(rows) + len(entries) < total and len(entries) < PAGE_SIZE:
            truncated = True
            failures.append(
                f"listing_short_page_{page}: collected {len(rows) + len(entries)} of {total} rows"
            )
            break
        seen_ids.update(page_ids)
        rows.extend(entries)
        if len(rows) == total:
            break
        page += 1
    return rows, failures, truncated


def _audit_entry(
    context: _AuditContext, entry: Mapping[str, Any], parsed: ShokoFileXRefs
) -> AuditFinding:
    """Classify one linked Shoko row against the verified mapping (fail closed, read-only)."""
    linked = frozenset(
        (xref.series_anidb_id, episode.anidb_episode_id)
        for xref in parsed.xrefs
        for episode in xref.episodes
    )
    file_row, ownership = _parse_and_own(entry, context.folders, context.settings)
    if file_row is None:
        return AuditFinding(
            parsed.file_id,
            AuditClass.AUDIT_INCOMPLETE,
            None,
            None,
            tuple(sorted(linked)),
            (),
            f"ownership: {ownership}",
        )
    match = _match_sonarr(file_row, ownership, context.sonarr_index)
    if match is None:
        return AuditFinding(
            parsed.file_id,
            AuditClass.NO_SONARR_COUNTERPART,
            None,
            None,
            tuple(sorted(linked)),
            (),
            "no exact Sonarr path+size counterpart",
        )
    sonarr_file, _sonarr_path = match
    tvdb_series = context.series_tvdb.get(sonarr_file.series_id)
    if tvdb_series is None:
        return AuditFinding(
            parsed.file_id,
            AuditClass.AMBIGUOUS_MATCH,
            sonarr_file.series_id,
            sonarr_file.episode_file_id,
            tuple(sorted(linked)),
            (),
            f"Sonarr series {sonarr_file.series_id} has no TVDB series id",
        )
    target_ids, first_episode, blocked = _target_identities(context, sonarr_file, tvdb_series)
    if blocked is not None:
        classification = (
            AuditClass.AUDIT_INCOMPLETE
            if blocked == "budget_exhausted" or blocked.startswith("target_episode_listing_")
            else AuditClass.AMBIGUOUS_MATCH
        )
        return AuditFinding(
            parsed.file_id,
            classification,
            sonarr_file.series_id,
            sonarr_file.episode_file_id,
            tuple(sorted(linked)),
            tuple(sorted(target_ids)),
            blocked,
        )
    classification = classify_link(
        linked, frozenset(target_ids), first_episode_target=first_episode
    )
    return AuditFinding(
        parsed.file_id,
        classification,
        sonarr_file.series_id,
        sonarr_file.episode_file_id,
        tuple(sorted(linked)),
        tuple(sorted(target_ids)),
        f"{len(linked)} linked / {len(target_ids)} target identities",
    )


def _target_identities(
    context: _AuditContext, sonarr_file: SonarrFile, tvdb_series: int
) -> tuple[set[tuple[int, int]], bool, str | None]:
    """The verified mapping's exact target identity set for one Sonarr file.

    Returns ``(target_ids, first_episode_seen, blocked_reason)``: an unmapped/ambiguous
    episode answers ``target_<status>`` and a spent budget answers
    ``budget_exhausted`` — both stop the pass for this file (never a guess).
    """
    target_ids: set[tuple[int, int]] = set()
    first_episode = False
    for episode in sorted(sonarr_file.episodes, key=lambda item: item.id):
        answer = resolve_mapping(
            context.active.index,
            tvdb_series,
            episode.season_number,
            episode.episode_number,
            episode.absolute_episode_number,
        )
        if answer.status is not ResolutionStatus.RESOLVED or not answer.targets:
            return target_ids, first_episode, f"target_{answer.status.value}"
        if episode.season_number == SEASON_ONE and episode.episode_number == EPISODE_ONE:
            first_episode = True
        for target in answer.targets:
            resolved = _resolve_anidb_episode_id(
                context.shoko, target, context.budget, context.cache
            )
            if resolved.identity is None:
                return (
                    target_ids,
                    first_episode,
                    resolved.reason or "target_episode_listing_malformed",
                )
            target_ids.add(resolved.identity)
    return target_ids, first_episode, None


def collect_audit_inputs(
    sonarr: ApiEndpoint,
    settings: Settings,
) -> tuple[tuple[SonarrFile, ...], Mapping[int, int], str | None]:
    """Collect the Sonarr inventory once (read-only) for the audit.

    Returns ``(files, sonarr_series→tvdb map, inventory problem | None)``.
    """
    inventory = collect_sonarr_inventory(sonarr, settings)
    tvdb = {
        item.series.id: item.series.tvdb_id
        for item in inventory.eligible_series
        if item.series.tvdb_id is not None
    }
    problem = None if inventory.verdict.complete else "sonarr inventory incomplete"
    return inventory.files, tvdb, problem


def _resolve_anidb_episode_id(
    shoko: ApiEndpoint,
    target: Any,
    budget: _Budget,
    cache: dict[tuple[int, str, int], _EpisodeIdLookup],
) -> _EpisodeIdLookup:
    """Resolve one identity with the linker's strict AniDB listing validation rules."""
    key = (target.anidb_id, target.episode_type.value, target.anidb_episode_number)
    if key in cache:
        return cache[key]
    if not budget.spend():
        return _EpisodeIdLookup(None, "budget_exhausted")
    outcome = _get(
        shoko, f"api/v3/Series/AniDB/{target.anidb_id}/Episode", {"type": target.episode_type.value}
    )
    result = _EpisodeIdLookup(None, "target_episode_listing_unavailable")
    if outcome is not None:
        total, entries = outcome
        found = _match_anidb_episode(
            {"Total": total, "List": entries},
            target.anidb_id,
            target.episode_type.value,
            target.anidb_episode_number,
        )
        if isinstance(found, int) and not isinstance(found, bool):
            result = _EpisodeIdLookup((target.anidb_id, found))
        elif found is EpisodeResolutionStatus.EPISODE_NOT_FOUND:
            result = _EpisodeIdLookup(None, "target_episode_not_found")
        elif found is EpisodeResolutionStatus.LIST_TRUNCATED:
            result = _EpisodeIdLookup(None, "target_episode_listing_truncated")
        else:
            result = _EpisodeIdLookup(None, "target_episode_listing_malformed")
    cache[key] = result
    return result


def _fetch_import_folders(
    endpoint: ApiEndpoint, budget: _Budget
) -> tuple[Mapping[int, ImportFolder], tuple[str, ...], tuple[str, ...]]:
    if not budget.spend():
        return {}, ("budget_exhausted",), ("no request budget for import folders",)
    entries = _get_array(endpoint, IMPORT_FOLDER_PATH)
    if entries is None:
        return {}, ("transport",), ("import folder listing unavailable",)
    folders, reasons, details = parse_import_folders(entries)
    return folders, tuple(reason.value for reason in reasons), tuple(details)


def _get_array(endpoint: ApiEndpoint, path: str) -> list[Any] | None:
    """One budgeted GET expecting a **bare** Shoko JSON array; None on failure.

    ``api/v3/ImportFolder`` is not a ``ListResult`` envelope (docs §2): it answers a
    plain array, so the envelope-shaped :func:`_get` cannot read it.
    """
    payload = _get_payload(endpoint, path)
    return payload if isinstance(payload, list) else None


def _get_payload(
    endpoint: ApiEndpoint, path: str, params: Mapping[str, Any] | None = None
) -> Any | None:
    """One budgeted GET returning its parsed JSON payload; ``None`` on any failure."""
    try:
        status, payload = get_bounded_json(endpoint, path, params)
    except (BoundedJsonError, HttpPolicyError, httpx.HTTPError):
        return None
    if not MIN_SUCCESS_STATUS <= status < MAX_SUCCESS_STATUS:
        return None
    return payload


def _get(
    endpoint: ApiEndpoint, path: str, params: Mapping[str, Any] | None = None
) -> tuple[int, list[Any]] | None:
    """One budgeted GET expecting a Shoko ``{Total, List}`` envelope; None on failure."""
    payload = _get_payload(endpoint, path, params)
    if not isinstance(payload, dict):
        return None
    total = payload.get("Total")
    entries = payload.get("List")
    if isinstance(total, bool) or not isinstance(total, int) or total < 0:
        return None
    if not isinstance(entries, list):
        return None
    return total, entries


def _parse_and_own(
    entry: Mapping[str, Any],
    folders: Mapping[int, ImportFolder],
    settings: Settings,
) -> tuple[ShokoFile | None, str]:
    """Parse one file row and apply the strict all-location ownership check (M3 rules)."""
    files, reasons, details, absolute_missing, _xrefs_missing = parse_file_entries([entry], 1)
    if reasons or not files:
        return None, details[0] if details else "malformed file row"
    item = files[0]
    if absolute_missing:
        return None, "AbsolutePath missing (include not honored)"
    translated, failure = _own_locations(item, folders, settings)
    if failure is not None:
        return None, failure
    assert translated is not None
    return item, translated


def _own_locations(
    item: ShokoFile,
    folders: Mapping[int, ImportFolder],
    settings: Settings,
) -> tuple[str | None, str | None]:
    """Every location must pass ownership and translate to exactly one Sonarr identity."""
    translated: list[str] = []
    for location in sorted(
        item.locations, key=lambda loc: (loc.import_folder_id, loc.relative_path, loc.place_id)
    ):
        path, failure = _check_location(location, folders, settings)
        if failure is not None:
            return None, failure
        assert path is not None
        translated.append(path)
    if len(set(translated)) != 1:
        return None, "locations translate to more than one Sonarr identity"
    return translated[0], None


def _check_location(
    location: ShokoLocation,
    folders: Mapping[int, ImportFolder],
    settings: Settings,
) -> tuple[str | None, str | None]:
    folder, failure = _location_folder(location, folders)
    if failure is not None:
        return None, failure
    assert folder is not None
    return _location_translation(location, folder, settings)


def _location_folder(
    location: ShokoLocation, folders: Mapping[int, ImportFolder]
) -> tuple[ImportFolder | None, str | None]:
    if not location.is_accessible:
        return None, f"location {location.place_id} is not accessible"
    folder = folders.get(location.import_folder_id)
    if folder is None:
        return None, f"location {location.place_id} references unknown ImportFolderID"
    if not folder.path_usable:
        return None, f"import folder {folder.id} has an unusable path"
    if folder.drop_folder_type != DROP_TYPE_NONE:
        return None, f"import folder {folder.id} is a drop folder"
    return folder, None


def _location_translation(
    location: ShokoLocation, folder: ImportFolder, settings: Settings
) -> tuple[str | None, str | None]:
    normalized, failure = _location_identity(location, folder, settings)
    if failure is not None:
        return None, failure
    assert normalized is not None
    return _location_mapping(normalized, settings)


def _location_identity(
    location: ShokoLocation, folder: ImportFolder, settings: Settings
) -> tuple[str | None, str | None]:
    try:
        normalized = normalize_posix(location.absolute_path)
        joined = join_location_path(folder.path, location.relative_path)
    except PathError as exc:
        return None, f"location {location.place_id}: {exc}"
    if joined != normalized:
        return None, f"location {location.place_id}: folder+relative does not match AbsolutePath"
    if not any(is_within_root(normalized, root) for root in settings.shoko.allowed_roots):
        return (
            None,
            f"location {normalized!r} is outside every configured shoko.allowed_roots entry",
        )
    return normalized, None


def _location_mapping(normalized: str, settings: Settings) -> tuple[str | None, str | None]:
    try:
        mapping = select_reverse_mapping(normalized, settings.path_mappings)
    except PathError as exc:
        return None, f"ambiguous Shoko path mapping: {exc}"
    if mapping is None:
        return None, f"location {normalized!r} is covered by no paths.mappings entry"
    try:
        return translate_to_sonarr(normalized, mapping), None
    except PathError as exc:
        return None, f"location {normalized!r} cannot be translated: {exc}"


def _match_sonarr(
    item: ShokoFile,
    sonarr_path: str,
    index: Mapping[str, list[SonarrFile]],
) -> tuple[SonarrFile, str] | None:
    """Exact correspondence (M3): normalized full path **and** byte size."""
    candidates = [entry for entry in index.get(sonarr_path, ()) if entry.size == item.size]
    if len(candidates) != 1:
        return None
    return candidates[0], sonarr_path


__all__ = [
    "AuditClass",
    "AuditFinding",
    "AuditReport",
    "classify_link",
    "collect_audit_inputs",
    "run_audit",
]
