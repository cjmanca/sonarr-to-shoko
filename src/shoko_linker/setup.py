"""Read-only Sonarr setup discovery and fail-closed anime-scope decisions."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any

import httpx

from .errors import HttpPolicyError, PathError
from .http import ApiEndpoint, BoundedJsonError, get_bounded_json
from .paths import (
    PathMapping,
    apply_mapping,
    is_within_root,
    normalize_posix,
    path_parts,
    validate_mappings,
)
from .shoko import (
    ImportFolder,
    ShokoDiscovery,
    ShokoFile,
    has_available_hashes,
    join_location_path,
)
from .sonarr import (
    ROOTFOLDER_PATH,
    SERIES_PATH,
    SonarrFile,
    SonarrInventory,
    SonarrSeries,
    parse_series_entries,
)

MIN_SUCCESS_STATUS = 200
MAX_SUCCESS_STATUS = 300
SONARR_VERSION_COMPONENT_COUNT = 3
SUPPORTED_SONARR_MAJOR_VERSIONS = frozenset({4, 5, 6})


class ScopeStatus(Enum):
    """The only setup outcomes that can be shown to the user."""

    BLOCKED = "blocked"
    NO_ANIME = "no_anime"
    CONFIRM_SCOPE = "confirm_scope"
    SELECT_SCOPE = "select_scope"


class ScopeReadStatus(Enum):
    """Whether the Sonarr scope inventory was read and parsed completely."""

    COMPLETE = "complete"
    UNAVAILABLE = "unavailable"
    INCOMPLETE = "incomplete"


@dataclass(frozen=True)
class RootFolder:
    """A validated Sonarr root folder and its reported accessibility."""

    path: str
    accessible: bool


@dataclass(frozen=True)
class AnimeRootCandidate:
    """One registered Sonarr root containing positively identified anime series."""

    path: str
    series_ids: tuple[int, ...]


@dataclass(frozen=True)
class AnimeScopeDiscovery:
    """Read-only result; no result itself authorizes a persistent scope or a write."""

    status: ScopeStatus
    candidates: tuple[AnimeRootCandidate, ...]
    message: str
    details: tuple[str, ...] = ()
    read_status: ScopeReadStatus = ScopeReadStatus.COMPLETE


@dataclass(frozen=True)
class ApiCompatibility:
    """Version and documented endpoint availability, without raw upstream payloads."""

    sonarr_version: str | None
    shoko_version: str | None
    sonarr_supported: bool
    shoko_supported: bool
    capabilities: tuple[str, ...]
    messages: tuple[str, ...]

    @property
    def compatible(self) -> bool:
        """Metadata probes succeeded; operational response contracts are checked at use."""
        return self.sonarr_supported and self.shoko_supported


class SetupReadinessStage(Enum):
    """Next safe onboarding action; none of these states authorizes a write."""

    BLOCKED = "blocked"
    NO_ANIME = "no_anime"
    CONFIRM_SCOPE = "confirm_scope"
    SELECT_SCOPE = "select_scope"
    DISCOVER_PATHS = "discover_paths"
    CONFIRM_PATH_MAPPING = "confirm_path_mapping"
    SAFETY_CHECKS = "safety_checks"


@dataclass(frozen=True)
class SetupReadiness:
    """Current read-only setup stage with one user-actionable plain-language message."""

    stage: SetupReadinessStage
    message: str


class MappingDiscoveryStatus(Enum):
    """Whether observed exact file evidence is enough to establish path mappings."""

    BLOCKED = "blocked"
    NEEDS_USER_MAPPING = "needs_user_mapping"
    READY = "ready"


@dataclass(frozen=True)
class PathMappingEvidence:
    """Exact unrecognized Shoko files that support one root-to-root translation."""

    mapping: PathMapping
    shoko_file_ids: tuple[int, ...]


@dataclass(frozen=True)
class PathMappingDiscovery:
    """Validated path mappings or a fail-closed request for a user decision."""

    status: MappingDiscoveryStatus
    mappings: tuple[PathMapping, ...]
    evidence: tuple[PathMappingEvidence, ...]
    message: str
    details: tuple[str, ...] = ()


@dataclass(frozen=True)
class _JsonRead:
    status: ScopeReadStatus
    payload: Any = None


def _get_json(
    endpoint: ApiEndpoint,
    path: str,
    *,
    deadline: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> _JsonRead:
    """GET one documented setup endpoint and preserve read completeness evidence."""
    try:
        status_code, payload = get_bounded_json(endpoint, path, deadline=deadline, clock=clock)
    except BoundedJsonError as exc:
        status = (
            ScopeReadStatus.INCOMPLETE
            if exc.status_code is not None
            and MIN_SUCCESS_STATUS <= exc.status_code < MAX_SUCCESS_STATUS
            else ScopeReadStatus.UNAVAILABLE
        )
        return _JsonRead(status)
    except (HttpPolicyError, httpx.HTTPError):
        return _JsonRead(ScopeReadStatus.UNAVAILABLE)
    if not MIN_SUCCESS_STATUS <= status_code < MAX_SUCCESS_STATUS:
        return _JsonRead(ScopeReadStatus.UNAVAILABLE)
    return _JsonRead(ScopeReadStatus.COMPLETE, payload)


def _sonarr_version_supported(version: str) -> bool:
    parts = version.split(".")
    if len(parts) < SONARR_VERSION_COMPONENT_COUNT or any(not part.isdigit() for part in parts):
        return False
    return int(parts[0]) in SUPPORTED_SONARR_MAJOR_VERSIONS


def inspect_api_compatibility(
    sonarr: ApiEndpoint,
    shoko: ApiEndpoint,
    *,
    deadline: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> ApiCompatibility:
    """Read metadata through the required v3 endpoints without pinning a Shoko release.

    Sonarr's `/api/v3/system/status` and Shoko's `/api/v3/Init/Version` expose only the
    version/capability metadata needed here. Shoko's settings endpoint is deliberately
    not used: it is admin-only and returns secret-bearing server configuration.
    """
    sonarr_payload = _get_json(
        sonarr, "api/v3/system/status", deadline=deadline, clock=clock
    ).payload
    sonarr_version_raw = sonarr_payload.get("version") if isinstance(sonarr_payload, dict) else None
    sonarr_version = (
        sonarr_version_raw.strip()
        if isinstance(sonarr_version_raw, str) and sonarr_version_raw.strip()
        else None
    )
    sonarr_supported = (
        False if sonarr_version is None else _sonarr_version_supported(sonarr_version)
    )

    shoko_payload = _get_json(shoko, "api/v3/Init/Version", deadline=deadline, clock=clock).payload
    server = shoko_payload.get("Server") if isinstance(shoko_payload, dict) else None
    shoko_version_raw = server.get("Version") if isinstance(server, dict) else None
    shoko_version = (
        shoko_version_raw.strip()
        if isinstance(shoko_version_raw, str) and shoko_version_raw.strip()
        else None
    )
    # A release label is not an API capability. Keep the observed version for consent
    # drift detection; discovery, queue, resolution and preflight parsers independently
    # refuse unavailable endpoints or incompatible response shapes before any link.
    shoko_supported = shoko_version is not None

    capabilities: list[str] = []
    messages: list[str] = []
    if sonarr_version is not None:
        capabilities.append("sonarr_api_v3_system_status")
    if shoko_version is not None:
        capabilities.append("shoko_api_v3_init_version")
    if sonarr_version is None:
        messages.append("Cannot verify Sonarr compatibility. Check its connection and API version.")
    elif not sonarr_supported:
        messages.append(
            f"Sonarr {sonarr_version} is not supported; use a verified Sonarr API v3 version "
            "from 4.x through 6.x."
        )
    if shoko_version is None:
        messages.append("Cannot verify Shoko compatibility. Check its connection and API version.")
    return ApiCompatibility(
        sonarr_version=sonarr_version,
        shoko_version=shoko_version,
        sonarr_supported=sonarr_supported,
        shoko_supported=shoko_supported,
        capabilities=tuple(capabilities),
        messages=tuple(messages),
    )


def build_setup_readiness(
    compatibility: ApiCompatibility,
    scope: AnimeScopeDiscovery,
    path_mappings: PathMappingDiscovery | None = None,
    *,
    scope_confirmed: bool = False,
) -> SetupReadiness:
    """Summarize the next safe setup action; safety authorization is a later gate."""
    stage = SetupReadinessStage.BLOCKED
    message = ""
    if scope.read_status is not ScopeReadStatus.COMPLETE:
        message = "Cannot completely verify the current Sonarr anime scope. Check its connection."
    elif not compatibility.compatible:
        message = (
            compatibility.messages[0]
            if compatibility.messages
            else "Required Sonarr/Shoko API compatibility could not be verified."
        )
    elif scope.status is ScopeStatus.BLOCKED:
        message = scope.message
    elif scope.status is ScopeStatus.NO_ANIME:
        stage = SetupReadinessStage.NO_ANIME
        message = scope.message
    elif not scope_confirmed:
        stage = (
            SetupReadinessStage.SELECT_SCOPE
            if scope.status is ScopeStatus.SELECT_SCOPE
            else SetupReadinessStage.CONFIRM_SCOPE
        )
        message = scope.message
    elif path_mappings is None:
        stage = SetupReadinessStage.DISCOVER_PATHS
        message = "Checking exact file paths to infer how Sonarr and Shoko folders correspond."
    elif path_mappings.status is MappingDiscoveryStatus.BLOCKED:
        message = path_mappings.message
    elif path_mappings.status is MappingDiscoveryStatus.NEEDS_USER_MAPPING:
        stage = SetupReadinessStage.CONFIRM_PATH_MAPPING
        message = path_mappings.message
    else:
        stage = SetupReadinessStage.SAFETY_CHECKS
        message = "Folder correspondence is ready. Checking Shoko safety before automatic linking."
    return SetupReadiness(stage, message)


def _parse_root_folders(raw: Any) -> tuple[RootFolder, ...] | None:
    if not isinstance(raw, list):
        return None
    folders: list[RootFolder] = []
    seen_paths: set[str] = set()
    for entry in raw:
        if not isinstance(entry, dict):
            return None
        path = entry.get("path")
        accessible = entry.get("accessible")
        if not isinstance(path, str) or not isinstance(accessible, bool):
            return None
        try:
            normalized = normalize_posix(path)
        except PathError:
            return None
        if normalized in seen_paths:
            return None
        seen_paths.add(normalized)
        folders.append(RootFolder(normalized, accessible))
    return tuple(sorted(folders, key=lambda folder: folder.path))


def _blocked(
    message: str,
    details: Sequence[str] = (),
    *,
    read_status: ScopeReadStatus = ScopeReadStatus.COMPLETE,
) -> AnimeScopeDiscovery:
    return AnimeScopeDiscovery(ScopeStatus.BLOCKED, (), message, tuple(details), read_status)


def select_anime_roots(
    series: Sequence[SonarrSeries], root_folders: Sequence[RootFolder]
) -> AnimeScopeDiscovery:
    """Select root candidates only from `seriesType=anime` within accessible registered roots."""
    anime_series = tuple(item for item in series if item.series_type == "anime")
    unknown_type = tuple(item for item in series if item.series_type is None)
    if not anime_series:
        if unknown_type:
            return _blocked(
                "Sonarr did not identify supported anime series types; check its API version "
                "and series metadata."
            )
        return AnimeScopeDiscovery(
            ScopeStatus.NO_ANIME,
            (),
            "No Sonarr anime series were found. Check the series types in Sonarr.",
        )

    problems: list[str] = []
    series_by_root: dict[str, list[int]] = {}
    for item in anime_series:
        try:
            series_root = normalize_posix(item.root_folder_path)
        except PathError:
            problems.append(f"anime series {item.id} has an unusable root-folder path")
            continue
        matches = [folder for folder in root_folders if is_within_root(series_root, folder.path)]
        if not matches:
            problems.append(
                f"anime series {item.id} is outside every registered Sonarr root folder"
            )
            continue
        most_specific_depth = max(len(path_parts(folder.path)) for folder in matches)
        most_specific = tuple(
            folder for folder in matches if len(path_parts(folder.path)) == most_specific_depth
        )
        if len(most_specific) != 1:
            problems.append(f"anime series {item.id} matches ambiguous Sonarr root folders")
            continue
        folder = most_specific[0]
        if not folder.accessible:
            problems.append(f"anime root folder {folder.path} is not accessible")
            continue
        series_by_root.setdefault(folder.path, []).append(item.id)

    if problems:
        return _blocked(
            "Cannot safely discover anime folders from Sonarr. Root folders must be accessible "
            "and scope data must be unambiguous.",
            problems,
        )

    candidates = tuple(
        AnimeRootCandidate(path, tuple(sorted(ids))) for path, ids in sorted(series_by_root.items())
    )
    if len(candidates) == 1:
        return AnimeScopeDiscovery(
            ScopeStatus.CONFIRM_SCOPE,
            candidates,
            f"Confirm this Sonarr anime folder: {candidates[0].path}",
        )
    if len(candidates) > 1:
        return AnimeScopeDiscovery(
            ScopeStatus.SELECT_SCOPE,
            candidates,
            "Which Sonarr folder contains your anime?",
        )
    return _blocked("No accessible registered Sonarr root contains the detected anime series.")


def discover_sonarr_scope(  # noqa: PLR0911 - ordered fail-closed API read validation
    endpoint: ApiEndpoint,
    *,
    deadline: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> AnimeScopeDiscovery:
    """Fetch only the Sonarr series/root-folder inventories and determine safe anime roots."""
    series_read = _get_json(endpoint, SERIES_PATH, deadline=deadline, clock=clock)
    if series_read.status is not ScopeReadStatus.COMPLETE:
        return _blocked(
            "Cannot safely read Sonarr series. Check the address, API key, and API version.",
            read_status=series_read.status,
        )
    raw_series = series_read.payload
    if not isinstance(raw_series, list):
        return _blocked(
            "Cannot safely read Sonarr series. Check the address, API key, and API version.",
            read_status=ScopeReadStatus.INCOMPLETE,
        )
    series_result = parse_series_entries(raw_series)
    if series_result.reasons:
        return _blocked(
            "Sonarr returned incomplete series data; automatic scope discovery is paused.",
            series_result.details,
            read_status=ScopeReadStatus.INCOMPLETE,
        )

    root_read = _get_json(endpoint, ROOTFOLDER_PATH, deadline=deadline, clock=clock)
    if root_read.status is not ScopeReadStatus.COMPLETE:
        return _blocked(
            "Cannot safely read Sonarr root folders. Check the address, API key, and API version.",
            read_status=root_read.status,
        )
    raw_roots = root_read.payload
    root_folders = _parse_root_folders(raw_roots)
    if root_folders is None:
        return _blocked(
            "Cannot safely read Sonarr root folders. Check the address, API key, and API version.",
            read_status=ScopeReadStatus.INCOMPLETE,
        )
    discovery = select_anime_roots(series_result.series, root_folders)
    if any(item.series_type is None for item in series_result.series):
        return AnimeScopeDiscovery(
            discovery.status,
            discovery.candidates,
            discovery.message,
            discovery.details,
            ScopeReadStatus.INCOMPLETE,
        )
    return discovery


def _eligible_shoko_file(file: ShokoFile) -> bool:
    return (
        file.xref_count == 0
        and not file.is_ignored
        and not file.is_variation
        and bool(file.locations)
        and has_available_hashes(file)
    )


type _SonarrPathIndex = dict[tuple[tuple[str, ...], int], list[tuple[SonarrFile, str]]]


def _blocked_mapping(message: str, detail: str | None = None) -> PathMappingDiscovery:
    details = () if detail is None else (detail,)
    return PathMappingDiscovery(MappingDiscoveryStatus.BLOCKED, (), (), message, details)


def infer_path_mappings(
    inventory: SonarrInventory, discovery: ShokoDiscovery
) -> PathMappingDiscovery:
    """Infer unique root mappings from exact relative paths and equal byte sizes.

    Every Shoko location for one file must be accessible, in a non-drop import folder,
    internally consistent, and resolve to the same uniquely identified anime Sonarr file.
    Filenames alone, incomplete snapshots, ambiguous identities, and conflicting root
    mappings never produce a mapping.
    """
    if not inventory.verdict.complete:
        return _blocked_mapping("Cannot infer paths from an incomplete Sonarr inventory.")
    if not discovery.verdict.complete:
        return _blocked_mapping("Cannot infer paths from an incomplete Shoko snapshot.")

    roots_by_series, root_error = _anime_roots_by_series(inventory)
    if root_error is not None:
        return _blocked_mapping(root_error)
    if not roots_by_series:
        return _blocked_mapping("No positively identified Sonarr anime series are in scope.")

    sonarr_index, index_error = _index_sonarr_paths(inventory, roots_by_series)
    if index_error is not None:
        return _blocked_mapping(index_error)
    assert sonarr_index is not None

    supported, ambiguous_files = _collect_mapping_evidence(discovery, sonarr_index)
    return _finish_mapping_discovery(supported, ambiguous_files)


def _anime_roots_by_series(
    inventory: SonarrInventory,
) -> tuple[dict[int, str], str | None]:
    roots: dict[int, str] = {}
    for eligible in inventory.eligible_series:
        if eligible.series.series_type != "anime":
            continue
        try:
            root = normalize_posix(eligible.root)
        except PathError:
            return {}, "Sonarr anime root is not a usable POSIX path."
        previous = roots.get(eligible.series.id)
        if previous is not None and previous != root:
            return {}, "Sonarr reported conflicting roots for an anime series."
        roots[eligible.series.id] = root
    return roots, None


def _index_sonarr_paths(
    inventory: SonarrInventory, roots_by_series: dict[int, str]
) -> tuple[_SonarrPathIndex | None, str | None]:
    index: _SonarrPathIndex = {}
    for sonarr_file in inventory.files:
        root_for_series = roots_by_series.get(sonarr_file.series_id)
        if root_for_series is None:
            continue
        try:
            root_components = path_parts(root_for_series)
            file_components = path_parts(sonarr_file.path)
        except PathError:
            return None, "Sonarr inventory contains an unusable file path."
        if file_components[: len(root_components)] != root_components:
            return None, "Sonarr file escaped its selected anime root."
        relative = file_components[len(root_components) :]
        if not relative:
            return None, "Sonarr file path does not identify a file below its anime root."
        index.setdefault((relative, sonarr_file.size), []).append((sonarr_file, root_for_series))
    return index, None


def _mapping_for_shoko_file(
    shoko_file: ShokoFile,
    folders: Mapping[int, ImportFolder],
    sonarr_index: _SonarrPathIndex,
) -> tuple[tuple[PathMapping, ...], bool]:
    """Return per-location mappings when every location reaches one Sonarr identity."""
    matches: list[tuple[SonarrFile, PathMapping]] = []
    locations_valid = True
    ambiguous = False
    for location in shoko_file.locations:
        folder = folders.get(location.import_folder_id)
        if (
            location.file_id != shoko_file.id
            or not location.is_accessible
            or folder is None
            or not folder.path_usable
            or folder.drop_folder_type != "None"
        ):
            locations_valid = False
            break
        try:
            folder_path = normalize_posix(folder.path)
            absolute_path = join_location_path(folder_path, location.relative_path)
            reported_path = normalize_posix(location.absolute_path)
            folder_components = path_parts(folder_path)
            absolute_components = path_parts(absolute_path)
        except PathError:
            locations_valid = False
            break
        if reported_path != absolute_path:
            locations_valid = False
            break
        relative = absolute_components[len(folder_components) :]
        candidates = sonarr_index.get((relative, shoko_file.size), [])
        if len(candidates) != 1:
            locations_valid = False
            ambiguous = len(candidates) > 1
            break
        sonarr_file, sonarr_root = candidates[0]
        mapping = PathMapping(sonarr_root, folder_path)
        try:
            translated_path = apply_mapping(sonarr_file.path, mapping)
        except PathError:
            locations_valid = False
            break
        if translated_path != reported_path:
            locations_valid = False
            break
        matches.append((sonarr_file, mapping))

    identities = {
        (sonarr_file.series_id, sonarr_file.episode_file_id) for sonarr_file, _mapping in matches
    }
    if not locations_valid or len(matches) != len(shoko_file.locations) or len(identities) != 1:
        return (), ambiguous
    mappings = tuple(
        sorted(
            {mapping for _sonarr_file, mapping in matches},
            key=lambda item: (item.sonarr, item.shoko),
        )
    )
    return mappings, False


def _collect_mapping_evidence(
    discovery: ShokoDiscovery, sonarr_index: _SonarrPathIndex
) -> tuple[dict[PathMapping, set[int]], list[int]]:
    supported: dict[PathMapping, set[int]] = {}
    ambiguous_files: list[int] = []
    for shoko_file in discovery.files:
        if not _eligible_shoko_file(shoko_file):
            continue
        mappings, ambiguous = _mapping_for_shoko_file(shoko_file, discovery.folders, sonarr_index)
        if ambiguous:
            ambiguous_files.append(shoko_file.id)
            continue
        for mapping in mappings:
            supported.setdefault(mapping, set()).add(shoko_file.id)
    return supported, ambiguous_files


def _finish_mapping_discovery(
    supported: dict[PathMapping, set[int]], ambiguous_files: Sequence[int]
) -> PathMappingDiscovery:
    if not supported:
        if ambiguous_files:
            details = tuple(
                f"Shoko file {file_id} has multiple exact Sonarr candidates"
                for file_id in sorted(set(ambiguous_files))
            )
            return PathMappingDiscovery(
                MappingDiscoveryStatus.NEEDS_USER_MAPPING,
                (),
                (),
                "Several Sonarr files match the same Shoko path and size; "
                "confirm the folder mapping.",
                details,
            )
        return PathMappingDiscovery(
            MappingDiscoveryStatus.NEEDS_USER_MAPPING,
            (),
            (),
            "Cannot confidently infer the Sonarr and Shoko folder mapping. "
            "Confirm the matching folders.",
        )

    candidate_mappings = tuple(
        sorted(supported, key=lambda mapping: (mapping.sonarr, mapping.shoko))
    )
    try:
        validated = validate_mappings(candidate_mappings)
    except PathError as exc:
        evidence = tuple(
            PathMappingEvidence(mapping, tuple(sorted(supported[mapping])))
            for mapping in candidate_mappings
        )
        return PathMappingDiscovery(
            MappingDiscoveryStatus.NEEDS_USER_MAPPING,
            (),
            evidence,
            "Observed files suggest conflicting folder mappings. Confirm the folder pairs.",
            (str(exc),),
        )

    evidence = tuple(
        PathMappingEvidence(mapping, tuple(sorted(supported[mapping]))) for mapping in validated
    )
    return PathMappingDiscovery(
        MappingDiscoveryStatus.READY,
        validated,
        evidence,
        f"Inferred {len(validated)} unique path mapping(s) from exact file paths and sizes.",
    )
