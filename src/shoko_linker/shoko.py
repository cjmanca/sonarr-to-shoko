"""Shoko v5.3.3 unrecognized-file discovery, ownership, and exact correspondence (M3).

Read-only by construction: this module issues only ``GET`` requests (``api/v3/File``
pages plus one ``api/v3/ImportFolder`` listing) through
:func:`shoko_linker.http.resolve_and_send`; it never writes, links, refreshes, or
touches SQLite. The Shoko wire format is **PascalCase with nulls included** — the
opposite of Sonarr's camelCase null-omission — so parsing here is deliberately
independent of ``sonarr.py`` conventions.

``include_only=Unrecognized`` means exactly *zero episode cross-references*, not
"ready to link". Snapshot completeness is the precondition for every result: the
listing has no snapshot isolation, so a ``Total`` that changes between pages,
duplicate file IDs, a short page that leaves IDs missing, transport failures, a
malformed ``ListResult`` envelope, a page-cap hit, or a server that ignored
``include=AbsolutePaths`` / ``include=XRefs`` mark the snapshot **incomplete**, and
:func:`match_shoko_files` refuses an incomplete snapshot. Files that are simply not
candidates (carrying XRefs, variations, ignored files, files with no locations, files
whose hash identity has not settled — no complete 32-hex ED2K, per
:func:`has_available_hashes`) are recorded as categorized exclusions and keep the snapshot
complete. Shoko's ``Imported``
(``DateTimeImported``) field is **diagnostic only**: it is set once a file is identified
via AniDB, so a fully hashed file with ``Imported=null`` stays a valid candidate
forever — candidacy never depends on it.

Ownership is strict and per-file: **every** location of a Shoko file must be
accessible, inside a configured ``shoko.allowed_roots`` entry, belong to an import
folder whose ``DropFolderType`` is ``"None"``, and satisfy
``normalize(ImportFolder.Path + RelativePath) == AbsolutePath``. Any failing
location drops the whole file (``mixed_ownership`` semantics); a convenient "first
good path" is never chosen. Locations are then translated Shoko→Sonarr through the
reverse of the configured ``paths.mappings`` (unique most-specific match; ties
and unmapped paths are refused), and a file matches a Sonarr file only on the exact
normalized full path **and** byte size, with every location resolving to that same
single Sonarr identity. Basename equality alone never matches.

All decision functions here are pure, I/O-free and deterministic: matches and
exclusions are ordered by Shoko file ID, locations by path. :func:`build_identity`
and :func:`compare_identity` form the identity record M5/M6 use across spaced
observations. No API key material is stored in any structure defined here; recorded
hash values are media file hashes, not credentials.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import PurePosixPath
from typing import Any, Final, cast

import httpx

from .config import Settings
from .errors import HttpPolicyError, PathError
from .http import ApiEndpoint, resolve_and_send
from .paths import PathMapping, is_within_root, normalize_posix, path_parts
from .sonarr import SonarrFile

FILE_PATH: Final[str] = "api/v3/File"
IMPORT_FOLDER_PATH: Final[str] = "api/v3/ImportFolder"
PAGE_SIZE: Final[int] = 1000
MAX_FILE_PAGES: Final[int] = 1000
MIN_SUCCESS_STATUS: Final[int] = 200
MAX_SUCCESS_STATUS: Final[int] = 300
DROP_TYPE_NONE: Final[str] = "None"
VALID_DROP_TYPES: Final[frozenset[str]] = frozenset({"None", "Source", "Destination", "Both"})
LIST_RESULT_KEYS: Final[frozenset[str]] = frozenset({"Total", "List"})
_INCLUDE_FLAGS: Final[str] = "XRefs,AbsolutePaths"
_UNRECOGNIZED_FILTER: Final[str] = "Unrecognized"
# Shoko's ``Hashes`` object carries raw hexadecimal hash strings, not ``ed2k://``
# URIs: ``HashesDict.ED2K`` is the AniDB base hash (MD5 of the file content) as a
# 32-hex string, and SHA1/CRC32/MD5 are nullable hex strings (40/8/32 hex). See
# ``File.cs`` ``HashesDict`` and ``HashFileJob`` at the pinned upstream commit.
_ED2K_HEX_RE: Final[re.Pattern[str]] = re.compile(r"[0-9a-fA-F]{32}")
_SHA1_HEX_RE: Final[re.Pattern[str]] = re.compile(r"[0-9a-fA-F]{40}")
_MD5_HEX_RE: Final[re.Pattern[str]] = re.compile(r"[0-9a-fA-F]{32}")
_CRC32_HEX_RE: Final[re.Pattern[str]] = re.compile(r"[0-9a-fA-F]{8}")


class SnapshotReason(Enum):
    """Machine-readable reasons the Shoko snapshot is not complete."""

    TRANSPORT_FAILURE = "transport_failure"
    MALFORMED_ENVELOPE = "malformed_envelope"
    MALFORMED_FILE = "malformed_file"
    MALFORMED_IMPORT_FOLDER = "malformed_import_folder"
    TOTAL_CHANGED = "total_changed"
    DUPLICATE_FILE_IDS = "duplicate_file_ids"
    PAGES_EXHAUSTED = "pages_exhausted"
    MAX_PAGES_EXCEEDED = "max_pages_exceeded"
    PAGE_OVERSHOOT = "page_overshoot"
    ABSOLUTE_PATHS_MISSING = "absolute_paths_missing"
    XREFS_MISSING = "xrefs_missing"


class ExclusionReason(Enum):
    """Machine-readable reasons a Shoko file is not a usable match candidate."""

    XREF_PRESENT = "xref_present"
    VARIATION = "variation"
    IGNORED = "ignored"
    NO_LOCATIONS = "no_locations"
    NO_HASHES = "no_hashes"
    MIXED_OWNERSHIP = "mixed_ownership"
    INACCESSIBLE_LOCATION = "inaccessible_location"
    LOCATION_PATH_MISMATCH = "location_path_mismatch"
    UNKNOWN_IMPORT_FOLDER = "unknown_import_folder"
    DROP_FOLDER_TYPE = "drop_folder_type"
    OUTSIDE_ALLOWED_ROOT = "outside_allowed_root"
    INVALID_LOCATION_PATH = "invalid_location_path"
    UNMAPPED_LOCATION = "unmapped_location"
    AMBIGUOUS_MAPPING = "ambiguous_mapping"
    MULTI_LOCATION_DIVERGENCE = "multi_location_divergence"
    NOT_IN_INVENTORY = "not_in_inventory"
    NO_SIZE_MATCH = "no_size_match"
    SIZE_MISMATCH = "size_mismatch"
    AMBIGUOUS_MULTIPLE_SONARR_CANDIDATES = "ambiguous_multiple_sonarr_candidates"


class IdentityChange(Enum):
    """Categories of change between two spaced observations of one Shoko file ID."""

    SIZE_CHANGED = "size_changed"
    HASH_CHANGED = "hash_changed"
    LOCATIONS_ADDED = "locations_added"
    LOCATIONS_REMOVED = "locations_removed"
    LOCATIONS_CHANGED = "locations_changed"


@dataclass(frozen=True)
class ShokoHashes:
    """File hashes exactly as Shoko reported them (``None`` = not reported)."""

    ed2k: str | int | None
    sha1: str | int | None
    crc32: str | int | None
    md5: str | int | None


@dataclass(frozen=True)
class ShokoLocation:
    """One ``VideoLocal_Place`` row: a physical location of a Shoko file."""

    place_id: int
    file_id: int
    import_folder_id: int
    relative_path: str
    absolute_path: str
    is_accessible: bool


@dataclass(frozen=True)
class ShokoFile:
    """One unrecognized Shoko file as reported by ``GET api/v3/File``."""

    id: int
    size: int
    is_ignored: bool
    is_variation: bool
    imported: str | None
    hashes: ShokoHashes
    xref_count: int
    locations: tuple[ShokoLocation, ...]


@dataclass(frozen=True)
class ImportFolder:
    """One Shoko import folder with its drop-folder type (validated shape)."""

    id: int
    name: str
    path: str
    drop_folder_type: str
    path_usable: bool


@dataclass(frozen=True)
class ShokoVerdict:
    """Completeness of one discovery pass; incomplete snapshots are never reconciled."""

    complete: bool
    reasons: tuple[SnapshotReason, ...]
    details: tuple[str, ...]


@dataclass(frozen=True)
class ShokoDiscovery:
    """One read-only Shoko pass: files in server order, import folders, verdict."""

    files: tuple[ShokoFile, ...]
    folders: Mapping[int, ImportFolder]
    verdict: ShokoVerdict
    pages_fetched: int
    total_reported: int | None
    page_size: int


@dataclass(frozen=True)
class FileMatch:
    """A Shoko file exactly corresponding to one Sonarr episode file (path + size)."""

    shoko_file_id: int
    sonarr_file: SonarrFile
    shoko_paths: tuple[str, ...]
    hashes: ShokoHashes


@dataclass(frozen=True)
class ShokoExclusion:
    """A Shoko file refused as a candidate, with its machine-readable reason."""

    shoko_file_id: int
    reason: ExclusionReason
    detail: str


@dataclass(frozen=True)
class ShokoFileIdentity:
    """Identity record for stability comparison across spaced observations (M5/M6).

    ``paths`` is canonical **Shoko-side** normalized absolute paths — the only
    location field :func:`compare_identity` compares, so an ownership-status
    change (translation success/failure) never fakes location drift.
    ``sonarr_paths`` is translated context for reporting; it is not
    compared.
    """

    shoko_file_id: int
    size: int
    ed2k: str | int | None
    sha1: str | int | None
    crc32: str | int | None
    md5: str | int | None
    paths: tuple[str, ...]
    folders: tuple[int, ...]
    sonarr_paths: tuple[str, ...] = ()


@dataclass(frozen=True)
class IdentityVerdict:
    """Change categories between two identity observations of the same file."""

    categories: tuple[IdentityChange, ...]
    details: tuple[str, ...]

    @property
    def stable(self) -> bool:
        """True when nothing changed."""
        return not self.categories


@dataclass(frozen=True)
class ShokoMatchResult:
    """Reconciliation output: matches, categorized exclusions, identities, warnings.

    ``eligible`` is False whenever the Shoko snapshot or the Sonarr inventory is
    incomplete; a non-eligible result carries no usable matches. Refused passes
    intentionally carry **no identities**: a snapshot we do not trust must not
    feed drift comparisons in later milestones (M6 observes drift only on
    complete passes).
    """

    eligible: bool
    matches: tuple[FileMatch, ...]
    exclusions: tuple[ShokoExclusion, ...]
    identities: tuple[ShokoFileIdentity, ...]
    warnings: tuple[str, ...]


# ----------------------------------------------------------------------- parsing


def parse_import_folders(
    raw: Sequence[Any],
) -> tuple[dict[int, ImportFolder], list[SnapshotReason], list[str]]:
    """Parse ``GET api/v3/ImportFolder`` entries; malformed shapes fail closed."""
    folders: dict[int, ImportFolder] = {}
    reasons: list[SnapshotReason] = []
    details: list[str] = []
    for index, item in enumerate(raw):
        entry = _as_mapping(item)
        if entry is None:
            _append(reasons, SnapshotReason.MALFORMED_IMPORT_FOLDER)
            details.append(f"import folder entry {index} is not an object")
            continue
        folder_id = _required_int(entry, "ID")
        name = _required_str(entry, "Name")
        path = _required_str(entry, "Path")
        drop_type = _required_str(entry, "DropFolderType")
        if folder_id is None or folder_id <= 0 or name is None or path is None:
            _append(reasons, SnapshotReason.MALFORMED_IMPORT_FOLDER)
            details.append(f"import folder entry {index} is missing/invalid ID, Name or Path")
            continue
        if drop_type is None:
            _append(reasons, SnapshotReason.MALFORMED_IMPORT_FOLDER)
            details.append(f"import folder {folder_id} has a missing/invalid DropFolderType")
            continue
        if drop_type not in VALID_DROP_TYPES:
            details.append(
                f"import folder {folder_id} reports unknown DropFolderType {drop_type!r}; "
                "treated as a drop folder (ownership refuses it)"
            )
            drop_type = "Unknown"
        usable = True
        normalized = path
        try:
            normalized = normalize_posix(path)
        except PathError as exc:
            usable = False
            details.append(f"import folder {folder_id} has an unusable path: {exc}")
        if folder_id in folders:
            _append(reasons, SnapshotReason.MALFORMED_IMPORT_FOLDER)
            details.append(f"import folder {folder_id} reported more than once")
            continue
        folders[folder_id] = ImportFolder(
            id=folder_id,
            name=name,
            path=normalized,
            drop_folder_type=drop_type,
            path_usable=usable,
        )
    return folders, reasons, details


def parse_file_entries(
    raw: Sequence[Any], page: int
) -> tuple[list[ShokoFile], list[SnapshotReason], list[str], bool, bool]:
    """Parse one page's ``List`` entries.

    Returns ``(files, reasons, details, absolute_paths_missing, xrefs_missing)``;
    the two flags report a server that ignored ``include=AbsolutePaths`` /
    ``include=XRefs`` — an incomplete snapshot we must not guess across.
    """
    files: list[ShokoFile] = []
    reasons: list[SnapshotReason] = []
    details: list[str] = []
    absolute_paths_missing = False
    xrefs_missing = False
    for index, item in enumerate(raw):
        entry = _as_mapping(item)
        if entry is None:
            _append(reasons, SnapshotReason.MALFORMED_FILE)
            details.append(f"page {page}: file entry {index} is not an object")
            continue
        file_id = _required_int(entry, "ID")
        size = _required_int(entry, "Size")
        is_ignored = _required_bool(entry, "IsIgnored")
        is_variation = _required_bool(entry, "IsVariation")
        imported, imported_ok = _optional_str(entry, "Imported")
        hashes, hashes_ok, hashes_detail = _parse_hashes(entry)
        xref_count, xrefs_present = _parse_series_ids(entry)
        xrefs_missing |= not xrefs_present
        locations, locations_ok, locations_absent = _parse_locations(entry, file_id)
        absolute_paths_missing |= locations_absent
        problems = _file_problems(file_id, size, is_ignored, is_variation, imported_ok)
        if not hashes_ok:
            problems.append("invalid Hashes")
        if not xrefs_present:
            problems.append("SeriesIDs absent (include=XRefs was not honored)")
        if not locations_ok:
            problems.append("Locations missing")
        if problems:
            _append(reasons, SnapshotReason.MALFORMED_FILE)
            identifier = "?" if file_id is None else file_id
            details.append(
                f"page {page}: file entry {index} (ID {identifier}) malformed: "
                f"{', '.join(problems)}"
            )
            continue
        assert file_id is not None
        assert size is not None
        assert is_ignored is not None
        assert is_variation is not None
        files.append(
            ShokoFile(
                id=file_id,
                size=size,
                is_ignored=is_ignored,
                is_variation=is_variation,
                imported=imported,
                hashes=hashes,
                xref_count=xref_count,
                locations=tuple(locations),
            )
        )
        if hashes_detail:
            details.append(f"page {page}: file {file_id}: {hashes_detail}")
    return files, reasons, details, absolute_paths_missing, xrefs_missing


# ---------------------------------------------------------------- pure decisions


def select_reverse_mapping(path: str, mappings: Sequence[PathMapping]) -> PathMapping | None:
    """Reverse of :func:`shoko_linker.paths.select_mapping` in the Shoko path space.

    Returns the unique mapping whose **Shoko** side most specifically contains
    ``path``, or ``None`` when no mapping covers it.

    Raises:
        PathError: when two mappings match with equal specificity (never guessed).
    """
    matches = [mapping for mapping in mappings if is_within_root(path, mapping.shoko)]
    if not matches:
        return None
    best_depth = max(len(path_parts(mapping.shoko)) for mapping in matches)
    winners = [mapping for mapping in matches if len(path_parts(mapping.shoko)) == best_depth]
    if len(winners) > 1:
        targets = ", ".join(mapping.shoko for mapping in winners)
        raise PathError(f"ambiguous Shoko path mappings for {path!r}: {targets}")
    return winners[0]


def translate_to_sonarr(path: str, mapping: PathMapping) -> str:
    """Translate a Shoko-side path to its Sonarr side on an exact component boundary.

    Raises:
        PathError: when ``path`` is not a usable POSIX path or leaves the mapping.
    """
    source = path_parts(mapping.shoko)
    target = path_parts(mapping.sonarr)
    candidate = path_parts(path)
    if len(candidate) < len(source) or candidate[: len(source)] != source:
        raise PathError(f"Shoko path {path!r} is not inside mapping target {mapping.shoko!r}")
    return "/" + "/".join((*target, *candidate[len(source) :]))


def join_location_path(folder_path: str, relative_path: str) -> str:
    """Compose ``ImportFolder.Path`` + ``RelativePath`` in normalized POSIX form.

    Raises:
        PathError: when the relative path is empty, absolute, backslashed, or
            contains ``..`` (a cross-check input must never be guessable).
    """
    candidate = relative_path.strip()
    if not candidate:
        raise PathError("RelativePath must be a non-empty string")
    if "\\" in candidate:
        raise PathError(f"RelativePath must use POSIX separators, not backslashes: {candidate!r}")
    pure = PurePosixPath(candidate)
    if pure.is_absolute():
        raise PathError(f"RelativePath must be relative to the import folder: {candidate!r}")
    if ".." in pure.parts:
        raise PathError(f"RelativePath must not contain '..' segments: {candidate!r}")
    return normalize_posix(f"{folder_path}/{candidate}")


def index_sonarr_files(files: Sequence[SonarrFile]) -> dict[str, list[SonarrFile]]:
    """Index the Sonarr inventory by exact normalized path (one pass, O(n))."""
    index: dict[str, list[SonarrFile]] = {}
    for item in files:
        index.setdefault(normalize_posix(item.path), []).append(item)
    return index


def build_identity(
    item: ShokoFile,
    translated_paths: Sequence[str] | None = None,
) -> ShokoFileIdentity:
    """Reduce a Shoko file to its identity record in one canonical space.

    ``paths`` always holds the Shoko-side normalized absolute location
    paths (the compared field; stable under ownership flips). Translated
    Sonarr-side paths, when available, are stored in ``sonarr_paths`` for
    reporting context only. Locations are sorted by path.
    """
    paths = tuple(sorted({_safe_path(location.absolute_path) for location in item.locations}))
    sonarr = tuple(sorted(set(translated_paths))) if translated_paths else ()
    folders = tuple(sorted({location.import_folder_id for location in item.locations}))
    return ShokoFileIdentity(
        shoko_file_id=item.id,
        size=item.size,
        ed2k=item.hashes.ed2k,
        sha1=item.hashes.sha1,
        crc32=item.hashes.crc32,
        md5=item.hashes.md5,
        paths=paths,
        folders=folders,
        sonarr_paths=sonarr,
    )


def compare_identity(
    previous: ShokoFileIdentity,
    current: ShokoFileIdentity,
) -> IdentityVerdict:
    """Compare two spaced observations of the same Shoko file ID (pure, M5/M6 input).

    Only same-space canonical fields are compared (Shoko-side paths, size,
    hashes, import-folder membership). Size and hash changes are replacement
    signals; import-folder membership changes are reported as locations
    added/removed; same folders with different Shoko paths is
    ``locations_changed`` (renames/moves). No categories means stable.
    """
    categories: list[IdentityChange] = []
    details: list[str] = []
    if previous.size != current.size:
        _append(categories, IdentityChange.SIZE_CHANGED)
        details.append(f"file {current.shoko_file_id} size {previous.size} -> {current.size}")
    if _hash_tuple(previous) != _hash_tuple(current):
        _append(categories, IdentityChange.HASH_CHANGED)
        details.append(f"file {current.shoko_file_id} reported hash values changed")
    previous_folders = set(previous.folders)
    current_folders = set(current.folders)
    added = sorted(current_folders - previous_folders)
    removed = sorted(previous_folders - current_folders)
    if added:
        _append(categories, IdentityChange.LOCATIONS_ADDED)
        details.append(
            f"file {current.shoko_file_id} gained import folder(s) {', '.join(map(str, added))}"
        )
    if removed:
        _append(categories, IdentityChange.LOCATIONS_REMOVED)
        details.append(
            f"file {current.shoko_file_id} lost import folder(s) " f"{', '.join(map(str, removed))}"
        )
    if not added and not removed and previous.paths != current.paths:
        _append(categories, IdentityChange.LOCATIONS_CHANGED)
        details.append(
            f"file {current.shoko_file_id} paths {list(previous.paths)} -> {list(current.paths)}"
        )
    return IdentityVerdict(tuple(categories), tuple(details))


def match_shoko_files(
    discovery: ShokoDiscovery,
    inventory_files: Sequence[SonarrFile],
    inventory_complete: bool,
    settings: Settings,
) -> ShokoMatchResult:
    """Reconcile every discovered Shoko file against the Sonarr inventory in one pass.

    Exact correspondence means: the translated Shoko absolute path equals the Sonarr
    file ``path`` (full normalized path equality; basename equality alone never
    matches) **and** the byte sizes are equal **and** every location of the Shoko
    file resolves to that same single Sonarr identity. Multiple Shoko files matching
    one Sonarr file are all reported as matches with a ``shoko_duplicate_size``
    warning (Shoko-side duplicates/variations of the same media).
    """
    if not discovery.verdict.complete:
        return ShokoMatchResult(
            eligible=False,
            matches=(),
            exclusions=(),
            identities=(),
            warnings=("reconciliation refused: Shoko snapshot incomplete",),
        )
    if not inventory_complete:
        return ShokoMatchResult(
            eligible=False,
            matches=(),
            exclusions=(),
            identities=(),
            warnings=("reconciliation refused: Sonarr inventory incomplete",),
        )
    index = index_sonarr_files(inventory_files)
    matches: list[FileMatch] = []
    exclusions: list[ShokoExclusion] = []
    identities: list[ShokoFileIdentity] = []
    for item in discovery.files:
        gate = _candidate_gate(item)
        if gate is not None:
            exclusions.append(ShokoExclusion(item.id, gate[0], gate[1]))
            identities.append(build_identity(item))
            continue
        translated, failure = _ownership(item, discovery.folders, settings)
        identities.append(build_identity(item, translated))
        if failure is not None:
            exclusions.append(ShokoExclusion(item.id, failure[0], failure[1]))
            continue
        assert translated
        sonarr_file, correspondence = _correspond(translated, item.size, index)
        if correspondence is not None:
            exclusions.append(ShokoExclusion(item.id, correspondence[0], correspondence[1]))
            continue
        assert sonarr_file is not None
        matches.append(
            FileMatch(
                shoko_file_id=item.id,
                sonarr_file=sonarr_file,
                shoko_paths=tuple(sorted(set(translated))),
                hashes=item.hashes,
            )
        )
    matches.sort(key=lambda match: match.shoko_file_id)
    exclusions.sort(key=lambda exclusion: (exclusion.shoko_file_id, exclusion.reason.value))
    identities.sort(key=lambda identity: identity.shoko_file_id)
    return ShokoMatchResult(
        eligible=True,
        matches=tuple(matches),
        exclusions=tuple(exclusions),
        identities=tuple(identities),
        warnings=_duplicate_warnings(matches),
    )


# ---------------------------------------------------------------------------- I/O


def collect_shoko_discovery(
    endpoint: ApiEndpoint,
    *,
    page_size: int = PAGE_SIZE,
    max_pages: int = MAX_FILE_PAGES,
) -> ShokoDiscovery:
    """Fetch import folders once, then page through ``include_only=Unrecognized`` files.

    Every request is a ``GET`` through :func:`shoko_linker.http.resolve_and_send`.
    Stability rules: page envelopes must be ``{Total, List}``, ``Total`` must not
    change between pages, file IDs must not repeat, a short page must reach the
    reported total, and ``include`` honored means every location carries
    ``AbsolutePath`` and every file carries ``SeriesIDs``. Violations are recorded
    as verdict reasons — never raised, never guessed around.
    """
    reasons: list[SnapshotReason] = []
    details: list[str] = []
    effective_size = _effective_page_size(page_size, details)

    folders, folder_reasons, folder_details = _fetch_import_folders(endpoint)
    reasons.extend(folder_reasons)
    details.extend(folder_details)

    files, pages_fetched, total = _walk_file_pages(
        endpoint, effective_size, max_pages, reasons, details
    )

    if total is not None and len(files) > total:
        # The server returned more rows than its own Total: pageSize honored
        # means this cannot happen, so an overshoot is a contract violation.
        _append(reasons, SnapshotReason.PAGE_OVERSHOOT)
        details.append(
            f"collected {len(files)} files exceeds the reported Total {total}: page shape "
            "violated the pagination contract"
        )
    deduped: list[SnapshotReason] = []
    for reason in reasons:
        _append(deduped, reason)
    return ShokoDiscovery(
        files=tuple(files),
        folders=folders,
        verdict=ShokoVerdict(complete=not deduped, reasons=tuple(deduped), details=tuple(details)),
        pages_fetched=pages_fetched,
        total_reported=total,
        page_size=effective_size,
    )


def _walk_file_pages(
    endpoint: ApiEndpoint,
    effective_size: int,
    max_pages: int,
    reasons: list[SnapshotReason],
    details: list[str],
) -> tuple[list[ShokoFile], int, int | None]:
    """Page the File listing until the total, a cap, or a stability failure."""
    files: list[ShokoFile] = []
    seen: set[int] = set()
    total: int | None = None
    pages_fetched = 0
    absolute_paths_missing = False
    xrefs_missing = False
    page = 1
    while True:
        if pages_fetched >= max_pages:
            _append(reasons, SnapshotReason.MAX_PAGES_EXCEEDED)
            details.append(
                f"page cap of {max_pages} reached with {len(files)} of "
                f"{total if total is not None else '?'} files collected"
            )
            break
        outcome = _fetch_page(endpoint, effective_size, page)
        pages_fetched += 1
        if outcome.rows is None:
            _append(reasons, outcome.reason or SnapshotReason.TRANSPORT_FAILURE)
            details.append(outcome.detail)
            break
        if total is None:
            total = outcome.total
        elif outcome.total != total:
            _append(reasons, SnapshotReason.TOTAL_CHANGED)
            details.append(
                f"Total changed from {total} to {outcome.total} on page {page} "
                "(Shoko recomputes the total per request: snapshot unstable)"
            )
            break
        parsed, entry_reasons, entry_details, paths_flag, xrefs_flag = parse_file_entries(
            outcome.rows, page
        )
        reasons.extend(entry_reasons)
        details.extend(entry_details)
        absolute_paths_missing |= paths_flag
        xrefs_missing |= xrefs_flag
        _merge_page(parsed, page, seen, files, reasons, details)
        if total is not None and len(files) >= total:
            break
        if not outcome.rows:
            _append(reasons, SnapshotReason.PAGES_EXHAUSTED)
            details.append(
                f"page {page} returned no rows with {len(files)} of {total} files collected "
                "(missing ids: pages short before the total)"
            )
            break
        page += 1
    if total is not None and len(files) < total and not reasons:
        # Defensive: every loop exit that under-fills already records a reason.
        _append(reasons, SnapshotReason.PAGES_EXHAUSTED)
        details.append(f"pagination ended with {len(files)} of {total} files collected")
    _include_flags_verdict(absolute_paths_missing, xrefs_missing, reasons, details)
    return files, pages_fetched, total


# --------------------------------------------------------------------------- helpers


def _include_flags_verdict(
    absolute_paths_missing: bool,
    xrefs_missing: bool,
    reasons: list[SnapshotReason],
    details: list[str],
) -> None:
    """A server that ignored the requested includes yields an unverifiable snapshot."""
    if absolute_paths_missing:
        _append(reasons, SnapshotReason.ABSOLUTE_PATHS_MISSING)
        details.append(
            "at least one location carries no AbsolutePath: include=AbsolutePaths was not "
            "honored, so ownership cannot be verified (incomplete snapshot, never guessed)"
        )
    if xrefs_missing:
        _append(reasons, SnapshotReason.XREFS_MISSING)
        details.append(
            "at least one file carries no SeriesIDs: include=XRefs was not honored, so "
            "unrecognized status cannot be verified (incomplete snapshot, never guessed)"
        )


def _merge_page(
    parsed: Sequence[ShokoFile],
    page: int,
    seen: set[int],
    files: list[ShokoFile],
    reasons: list[SnapshotReason],
    details: list[str],
) -> None:
    """Append page results, keeping duplicate IDs once and flagging the snapshot."""
    for item in parsed:
        if item.id in seen:
            _append(reasons, SnapshotReason.DUPLICATE_FILE_IDS)
            details.append(
                f"file id {item.id} reported on page {page} and an earlier page; "
                "kept once, snapshot unstable"
            )
            continue
        seen.add(item.id)
        files.append(item)


def _effective_page_size(page_size: int, details: list[str]) -> int:
    """Clamp the client-side page size to the Shoko v5.3.3 maximum of 1000."""
    if page_size > PAGE_SIZE:
        details.append(
            f"requested pageSize {page_size} exceeds the Shoko v5.3.3 maximum; "
            f"clamped to {PAGE_SIZE} (pageSize 0/unlimited is refused: paging needs "
            "bounded pages)"
        )
        return PAGE_SIZE
    if page_size <= 0:
        details.append(
            f"requested pageSize {page_size} is refused client-side; pageSize=0 means "
            f"'unlimited' on Shoko and breaks page-stability checks: using {PAGE_SIZE}"
        )
        return PAGE_SIZE
    return page_size


@dataclass(frozen=True)
class _PageOutcome:
    total: int | None
    rows: list[Any] | None
    reason: SnapshotReason | None
    detail: str


def _fetch_page(endpoint: ApiEndpoint, page_size: int, page: int) -> _PageOutcome:
    outcome = _fetch_json(
        endpoint,
        FILE_PATH,
        {
            "include_only": _UNRECOGNIZED_FILTER,
            "include": _INCLUDE_FLAGS,
            "pageSize": page_size,
            "page": page,
        },
    )
    if outcome.payload is None:
        return _PageOutcome(None, None, outcome.reason, outcome.detail)
    total, rows, problem = _validate_list_result(outcome.payload)
    if problem:
        return _PageOutcome(
            None, None, SnapshotReason.MALFORMED_ENVELOPE, f"page {page}: {problem}"
        )
    assert total is not None
    assert rows is not None
    return _PageOutcome(total, rows, None, "")


def _fetch_import_folders(
    endpoint: ApiEndpoint,
) -> tuple[dict[int, ImportFolder], list[SnapshotReason], list[str]]:
    outcome = _fetch_json(endpoint, IMPORT_FOLDER_PATH)
    if outcome.payload is None:
        reason = outcome.reason or SnapshotReason.MALFORMED_ENVELOPE
        return {}, [reason], [f"cannot enumerate import folders: {outcome.detail}"]
    payload = outcome.payload
    if not isinstance(payload, list):
        return (
            {},
            [SnapshotReason.MALFORMED_ENVELOPE],
            ["api/v3/ImportFolder: expected a JSON array"],
        )
    return parse_import_folders(payload)


def _fetch_json(
    endpoint: ApiEndpoint, path: str, params: Mapping[str, Any] | None = None
) -> _JsonOutcome:
    try:
        response = resolve_and_send(endpoint, "GET", path, params=params)
    except (HttpPolicyError, httpx.HTTPError) as exc:
        return _JsonOutcome(None, SnapshotReason.TRANSPORT_FAILURE, f"{path}: {exc}")
    try:
        if not MIN_SUCCESS_STATUS <= response.status_code < MAX_SUCCESS_STATUS:
            return _JsonOutcome(
                None, SnapshotReason.TRANSPORT_FAILURE, f"{path}: HTTP {response.status_code}"
            )
        try:
            payload = response.json()
        except ValueError as exc:
            return _JsonOutcome(
                None, SnapshotReason.MALFORMED_ENVELOPE, f"{path}: invalid JSON ({exc})"
            )
    finally:
        response.close()
    return _JsonOutcome(payload, None, "")


@dataclass(frozen=True)
class _JsonOutcome:
    payload: Any | None
    reason: SnapshotReason | None
    detail: str


def _validate_list_result(payload: Any) -> tuple[int | None, list[Any] | None, str]:
    """Shoko ``ListResult`` must carry exactly ``Total`` (int) and ``List`` (array)."""
    if not isinstance(payload, dict):
        return None, None, "expected a ListResult object with exactly Total and List"
    entry = cast("Mapping[str, Any]", payload)
    keys = set(entry)
    if keys != LIST_RESULT_KEYS:
        return None, None, f"ListResult keys must be exactly Total/List, got {sorted(keys)}"
    total = entry.get("Total")
    if isinstance(total, bool) or not isinstance(total, int) or total < 0:
        return None, None, "ListResult Total must be a non-negative integer"
    rows = entry.get("List")
    if not isinstance(rows, list):
        return None, None, "ListResult List must be a JSON array"
    return total, rows, ""


def _candidate_gate(item: ShokoFile) -> tuple[ExclusionReason, str] | None:
    """Non-candidate states that keep the snapshot complete (no guessing)."""
    gate: tuple[ExclusionReason, str] | None = None
    if item.xref_count > 0:
        gate = (
            ExclusionReason.XREF_PRESENT,
            f"file {item.id} carries {item.xref_count} episode cross-reference(s): "
            "no longer unrecognized",
        )
    elif item.is_variation:
        gate = (ExclusionReason.VARIATION, f"file {item.id} is a variation file")
    elif item.is_ignored:
        gate = (
            ExclusionReason.IGNORED,
            f"file {item.id} is marked IsIgnored: never requested, never a link candidate",
        )
    elif not item.locations:
        gate = (ExclusionReason.NO_LOCATIONS, f"file {item.id} reports no locations")
    elif _has_no_hash(item):
        gate = (
            ExclusionReason.NO_HASHES,
            f"file {item.id} reports no settled hash identity: identity incomplete "
            "(hashing not settled), never a link candidate",
        )
    return gate


def _ownership(
    item: ShokoFile,
    folders: Mapping[int, ImportFolder],
    settings: Settings,
) -> tuple[tuple[str, ...], tuple[ExclusionReason, str] | None]:
    """Check **every** location strictly; any failure drops the whole Shoko file."""
    translated: list[str] = []
    problems: list[tuple[ExclusionReason, str]] = []
    for location in sorted(
        item.locations, key=lambda loc: (loc.import_folder_id, loc.relative_path, loc.place_id)
    ):
        path, failure = _check_location(location, folders, settings)
        if failure is not None:
            problems.append(failure)
        if path is not None:
            translated.append(path)
    if problems:
        reason, detail = problems[0]
        if translated:
            # Some locations are Sonarr-owned and others are not: the whole
            # file is dropped (mixed ownership), never a convenient first path.
            return tuple(translated), (
                ExclusionReason.MIXED_OWNERSHIP,
                f"file {item.id} has in-scope locations and failing locations "
                f"({reason.value}: {detail}); all-location ownership drops the file",
            )
        return tuple(translated), (reason, detail)
    return tuple(translated), None


def _check_location(
    location: ShokoLocation,
    folders: Mapping[int, ImportFolder],
    settings: Settings,
) -> tuple[str | None, tuple[ExclusionReason, str] | None]:
    folder, failure = _location_access(location, folders)
    absolute = location.absolute_path
    translated: str | None = None
    if failure is None:
        assert folder is not None
        failure, absolute = _location_paths(location, folder, absolute, settings)
    if failure is None:
        translated, failure = _location_mapping(absolute, settings)
    return translated, failure


def _location_access(
    location: ShokoLocation,
    folders: Mapping[int, ImportFolder],
) -> tuple[ImportFolder | None, tuple[ExclusionReason, str] | None]:
    folder = folders.get(location.import_folder_id)
    failure: tuple[ExclusionReason, str] | None = None
    if not location.is_accessible:
        failure = (
            ExclusionReason.INACCESSIBLE_LOCATION,
            f"location {location.place_id} is not accessible",
        )
    elif folder is None:
        failure = (
            ExclusionReason.UNKNOWN_IMPORT_FOLDER,
            f"location {location.place_id} references unknown ImportFolderID "
            f"{location.import_folder_id}",
        )
    elif not folder.path_usable:
        failure = (
            ExclusionReason.UNKNOWN_IMPORT_FOLDER,
            f"import folder {folder.id} has an unusable path {folder.path!r}",
        )
    elif folder.drop_folder_type != DROP_TYPE_NONE:
        failure = (
            ExclusionReason.DROP_FOLDER_TYPE,
            f"import folder {folder.id} ({folder.name!r}) is a drop folder "
            f"(DropFolderType={folder.drop_folder_type!r})",
        )
    return folder, failure


def _location_paths(
    location: ShokoLocation,
    folder: ImportFolder,
    absolute: str,
    settings: Settings,
) -> tuple[tuple[ExclusionReason, str] | None, str]:
    failure: tuple[ExclusionReason, str] | None = None
    normalized = absolute
    joined = ""
    try:
        normalized = normalize_posix(absolute)
    except PathError as exc:
        failure = (ExclusionReason.INVALID_LOCATION_PATH, f"location {location.place_id}: {exc}")
    if failure is None:
        try:
            joined = join_location_path(folder.path, location.relative_path)
        except PathError as exc:
            failure = (
                ExclusionReason.LOCATION_PATH_MISMATCH,
                f"location {location.place_id}: {exc}",
            )
    if failure is None and joined != normalized:
        failure = (
            ExclusionReason.LOCATION_PATH_MISMATCH,
            f"location {location.place_id}: {folder.path!r} + {location.relative_path!r} = "
            f"{joined!r} does not match AbsolutePath {normalized!r}",
        )
    if failure is None and not any(
        is_within_root(normalized, root) for root in settings.shoko.allowed_roots
    ):
        failure = (
            ExclusionReason.OUTSIDE_ALLOWED_ROOT,
            f"location {normalized!r} is outside every configured shoko.allowed_roots entry",
        )
    return failure, normalized


def _location_mapping(
    absolute: str,
    settings: Settings,
) -> tuple[str | None, tuple[ExclusionReason, str] | None]:
    failure: tuple[ExclusionReason, str] | None = None
    translated: str | None = None
    mapping: PathMapping | None = None
    try:
        mapping = select_reverse_mapping(absolute, settings.path_mappings)
    except PathError as exc:
        failure = (ExclusionReason.AMBIGUOUS_MAPPING, str(exc))
    if failure is None and mapping is None:
        failure = (
            ExclusionReason.UNMAPPED_LOCATION,
            f"location {absolute!r} is covered by no paths.mappings entry",
        )
    if failure is None and mapping is not None:
        try:
            translated = translate_to_sonarr(absolute, mapping)
        except PathError as exc:
            failure = (ExclusionReason.UNMAPPED_LOCATION, str(exc))
    return translated, failure


def _correspond(
    translated_paths: Sequence[str],
    size: int,
    index: Mapping[str, list[SonarrFile]],
) -> tuple[SonarrFile | None, tuple[ExclusionReason, str] | None]:
    """Exact correspondence: full normalized path equality **and** byte size.

    Every translated location must resolve to one and the same Sonarr identity: a
    location whose path is absent from the inventory is ``multi_location_divergence``,
    a size that matches at only some locations is ``no_size_match``, and size-matching
    distinct Sonarr identities is ``ambiguous_multiple_sonarr_candidates``.
    """
    unique_paths = sorted(set(translated_paths))
    missing = [path for path in unique_paths if not index.get(path)]
    if len(missing) == len(unique_paths):
        return None, (
            ExclusionReason.NOT_IN_INVENTORY,
            f"no Sonarr file exists at the exact path {unique_paths[0]!r} "
            "(basename equality alone never matches)",
        )
    if missing:
        return None, (
            ExclusionReason.MULTI_LOCATION_DIVERGENCE,
            f"locations map to Sonarr paths {unique_paths} where {missing} is not in the "
            "inventory: no single file identity",
        )
    candidates: dict[tuple[int, int], SonarrFile] = {}
    size_hits: list[list[SonarrFile]] = []
    for path in unique_paths:
        entries = index[path]
        hits = sorted(
            (item for item in entries if item.size == size),
            key=lambda item: (item.series_id, item.episode_file_id),
        )
        size_hits.append(hits)
        for item in hits:
            candidates[(item.series_id, item.episode_file_id)] = item
    if not all(size_hits):
        if len(unique_paths) == 1:
            sizes = sorted({item.size for item in index[unique_paths[0]]})
            return None, (
                ExclusionReason.SIZE_MISMATCH,
                f"Sonarr file at {unique_paths[0]!r} has size(s) {sizes}, Shoko reports {size}",
            )
        return None, (
            ExclusionReason.NO_SIZE_MATCH,
            f"Shoko size {size} matches the inventory at only some of the locations "
            f"{unique_paths}: no consistent file identity",
        )
    if len(candidates) > 1:
        return None, (
            ExclusionReason.AMBIGUOUS_MULTIPLE_SONARR_CANDIDATES,
            f"paths {unique_paths} at size {size} resolve to Sonarr files "
            f"{sorted(candidates)}: ambiguous",
        )
    key = next(iter(candidates))
    return candidates[key], None


def _duplicate_warnings(matches: Sequence[FileMatch]) -> tuple[str, ...]:
    """Shoko-side duplicates: several Shoko files matching one Sonarr file."""
    by_sonarr: dict[tuple[int, int], list[int]] = {}
    for match in matches:
        key = (match.sonarr_file.series_id, match.sonarr_file.episode_file_id)
        by_sonarr.setdefault(key, []).append(match.shoko_file_id)
    warnings: list[str] = []
    for key in sorted(by_sonarr):
        ids = sorted(by_sonarr[key])
        if len(ids) > 1:
            warnings.append(
                f"shoko_duplicate_size: Shoko files {ids} all match Sonarr file "
                f"{key[1]} of series {key[0]} at the same path and size "
                "(Shoko-side duplicates)"
            )
    return tuple(sorted(warnings))


def _parse_hashes(entry: Mapping[str, Any]) -> tuple[ShokoHashes, bool, str]:
    detail = ""
    if "Hashes" not in entry:
        return ShokoHashes(None, None, None, None), True, "Hashes key absent"
    raw = entry.get("Hashes")
    if raw is None:
        return ShokoHashes(None, None, None, None), True, "Hashes is null"
    hashes_entry = _as_mapping(raw)
    if hashes_entry is None:
        return ShokoHashes(None, None, None, None), False, ""
    values: list[str | int | None] = []
    for key in ("ED2K", "SHA1", "CRC32", "MD5"):
        value = hashes_entry.get(key)
        if value is None:
            values.append(None)
        elif isinstance(value, bool) or not isinstance(value, str | int):
            return ShokoHashes(None, None, None, None), False, ""
        else:
            values.append(cast("str | int", value))
    return ShokoHashes(values[0], values[1], values[2], values[3]), True, detail


def _parse_series_ids(entry: Mapping[str, Any]) -> tuple[int, bool]:
    """``(xref_count, include_honored)``; a missing key means the include was ignored."""
    if "SeriesIDs" not in entry:
        return 0, False
    raw = entry.get("SeriesIDs")
    if raw is None:
        return 0, True
    if not isinstance(raw, list):
        return 0, False
    return len(raw), True


def _parse_locations(
    entry: Mapping[str, Any], file_id: int | None
) -> tuple[list[ShokoLocation], bool, bool]:
    """``(locations, ok, absolute_paths_missing)``; shapes are validated, never guessed."""
    if "Locations" not in entry:
        return [], False, False
    raw = entry.get("Locations")
    if not isinstance(raw, list):
        return [], False, False
    locations: list[ShokoLocation] = []
    absolute_paths_missing = False
    for item in raw:
        parsed = _parse_location(item, file_id)
        if parsed is None:
            return [], False, absolute_paths_missing
        location, present = parsed
        absolute_paths_missing |= not present
        locations.append(location)
    return locations, True, absolute_paths_missing


def _parse_location(item: Any, file_id: int | None) -> tuple[ShokoLocation, bool] | None:
    """Parse one location; returns ``(location, AbsolutePath_present)`` or None."""
    location = _as_mapping(item)
    if location is None:
        return None
    place_id = _required_int(location, "ID")
    owner_id = _required_int(location, "FileID")
    folder_id = _required_int(location, "ImportFolderID")
    relative = _required_str(location, "RelativePath")
    accessible = _required_bool(location, "IsAccessible")
    usable = (
        place_id is not None
        and place_id > 0
        and owner_id is not None
        and owner_id > 0
        and (file_id is None or owner_id == file_id)
        and folder_id is not None
        and folder_id > 0
        and relative is not None
        and accessible is not None
    )
    if not usable:
        return None
    assert place_id is not None
    assert owner_id is not None
    assert folder_id is not None
    assert relative is not None
    assert accessible is not None
    present = "AbsolutePath" in location
    raw_absolute = location.get("AbsolutePath")
    absolute = raw_absolute if isinstance(raw_absolute, str) else ""
    return (
        ShokoLocation(
            place_id=place_id,
            file_id=owner_id,
            import_folder_id=folder_id,
            relative_path=relative,
            absolute_path=absolute,
            is_accessible=accessible,
        ),
        present,
    )


def has_available_hashes(item: ShokoFile) -> bool:
    """True when Shoko reported a **settled, usable** hash identity (the link-eligibility gate).

    Shoko 5.3.3 creates the file record and exposes an **empty** ``ED2K`` string
    while the file is still being hashed, so "at least one non-null hash value" is
    not identity evidence and must never admit a link candidate. Shoko's ``Hashes``
    object carries the raw hexadecimal digests AniDB and Shoko match on, **not** an
    ``ed2k://`` URI. Settled means the ED2K identity is present and complete — a
    non-empty string of **exactly 32 hexadecimal characters** (the AniDB ED2K base
    hash, i.e. the MD5 of the file content, stored uppercase by Shoko) — **and**
    every other *populated* field matches its wire shape: ``sha1`` a 40-hex string,
    ``md5`` a 32-hex string, ``crc32`` an 8-hex string. Unreported (null/absent)
    ``SHA1``/``MD5``/``CRC32`` are permitted and never fail a file. Empty and
    whitespace-only strings, non-hex characters, truncated or oversized values, and
    values that are not strings all mean *not settled*. ``Imported=null`` is
    irrelevant here: a fully hashed file stays eligible. Values are trimmed for
    validation only, never rewritten, so identity comparisons use the exact bytes
    Shoko reported (case-sensitive).
    """
    return _hashes_settled(item.hashes)


def canonical_ed2k(value: str | int | None) -> str | None:
    """Return a raw 32-hex ED2K in comparison form, or None when it is malformed.

    F21 treats ED2K as Shoko's frozen file-content identity. The raw spelling stays
    untouched in diagnostic/snapshot records; only this comparison value is lowercased.
    Unlike the broader candidacy check, this requires the actual wire value to be exactly
    32 hexadecimal characters, without trimming or accepting numeric substitutes.
    """
    if not isinstance(value, str) or not _ED2K_HEX_RE.fullmatch(value):
        return None
    return value.lower()


def _hashes_settled(hashes: ShokoHashes) -> bool:
    """Single source of truth for "hashing settled" (snapshot and preflight gates)."""
    if not _valid_ed2k(hashes.ed2k):
        return False
    return (
        _settled_field(hashes.sha1, _SHA1_HEX_RE)
        and _settled_field(hashes.md5, _MD5_HEX_RE)
        and _settled_field(hashes.crc32, _CRC32_HEX_RE)
    )


def _valid_ed2k(value: str | int | None) -> bool:
    """Shoko's ED2K field is the raw AniDB base hash: a 32-hex string (upper or lower)."""
    if not isinstance(value, str):
        return False
    return bool(_ED2K_HEX_RE.fullmatch(value.strip()))


def _settled_field(value: str | int | None, pattern: re.Pattern[str]) -> bool:
    """Unreported (``None``) is fine; a populated value must be a plausible hex string.

    F16: Shoko's wire hash fields are strings, so any non-string populated value
    (int, bool, …) is malformed and means *not settled* (fail closed).
    """
    if value is None:
        return True
    if not isinstance(value, str):
        return False
    return bool(pattern.fullmatch(value.strip()))


def _has_no_hash(item: ShokoFile) -> bool:
    return not has_available_hashes(item)


def _file_problems(
    file_id: int | None,
    size: int | None,
    is_ignored: bool | None,
    is_variation: bool | None,
    imported_ok: bool,
) -> list[str]:
    problems: list[str] = []
    if file_id is None or file_id <= 0:
        problems.append("missing/invalid ID")
    if size is None or size < 0:
        problems.append("missing/invalid Size")
    if is_ignored is None:
        problems.append("missing/invalid IsIgnored")
    if is_variation is None:
        problems.append("missing/invalid IsVariation")
    if not imported_ok:
        problems.append("invalid Imported")
    return problems


def _hash_tuple(identity: ShokoFileIdentity) -> tuple[str | int | None, ...]:
    return (identity.ed2k, identity.sha1, identity.crc32, identity.md5)


def _safe_path(path: str) -> str:
    try:
        return normalize_posix(path)
    except PathError:
        return path


def _append[T](items: list[T], item: T) -> None:
    if item not in items:
        items.append(item)


def _as_mapping(raw: Any) -> Mapping[str, Any] | None:
    if isinstance(raw, dict):
        return cast("Mapping[str, Any]", raw)
    return None


def _required_int(entry: Mapping[str, Any], key: str) -> int | None:
    value = entry.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _required_str(entry: Mapping[str, Any], key: str) -> str | None:
    value = entry.get(key)
    return value if isinstance(value, str) else None


def _required_bool(entry: Mapping[str, Any], key: str) -> bool | None:
    value = entry.get(key)
    return value if isinstance(value, bool) else None


def _optional_str(entry: Mapping[str, Any], key: str) -> tuple[str | None, bool]:
    """``(value, ok)``: missing/None is ``(None, True)``; a present non-string is not ok."""
    if key not in entry:
        return None, True
    value = entry[key]
    if value is None:
        return None, True
    if not isinstance(value, str):
        return None, False
    return value, True
