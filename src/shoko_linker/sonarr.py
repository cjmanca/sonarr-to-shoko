"""Read-only Sonarr v3 inventory, eligibility filtering, and baseline identity.

Only series whose ``rootFolderPath`` lies component-aware inside a configured
``sonarr.allowed_roots`` entry are eligible. The normal service additionally requires
the verified Sonarr ``seriesType == "anime"`` field; legacy advanced YAML can retain
the previous explicit-root-only behavior. Only eligible series trigger
``api/v3/episode`` and ``api/v3/episodefile`` reads — an excluded series produces
zero secondary requests.

All I/O goes through :func:`shoko_linker.http.resolve_and_send` with relative API
paths, so urlBase preservation, the manual same-origin redirect walk, the no-retry
policy and origin binding stay enforced. Decision logic (selection, parsing,
indexing, root validation, baselining, comparison) is pure and I/O-free;
:func:`collect_sonarr_inventory` reads the inventory and
:func:`check_current_file_relationship` revalidates one chosen source file immediately
before an apply write.

An inventory is ``complete`` only when the series enumeration and every eligible
series' episode/file responses succeeded and were internally consistent.
``auto_apply_eligible`` is False on any incomplete snapshot. Malformed files fail
closed with a recorded reason; files escaping their series' allowed root are
rejected, never indexed. An episode whose file reference cannot be parsed taints
the whole series file set (partial groups are never trusted).
:func:`compare_baseline` separates identity discontinuity from ordinary churn:
a previously seen series root change, episode season/number change, or file
**size** change means the source was replaced (e.g. Sonarr database restore) and
requires a fresh baseline before any automatic write; renames/moves (same file
id and size, new path) and title-only changes are reported as drift/churn.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any, Final, cast

import httpx

from .config import Settings
from .errors import HttpPolicyError, PathError
from .http import ApiEndpoint, resolve_and_send
from .paths import is_within_root, normalize_posix

SERIES_PATH: Final[str] = "api/v3/series"
EPISODE_PATH: Final[str] = "api/v3/episode"
EPISODE_FILE_PATH: Final[str] = "api/v3/episodefile"
ROOTFOLDER_PATH: Final[str] = "api/v3/rootfolder"
MIN_SUCCESS_STATUS: Final[int] = 200
MAX_SUCCESS_STATUS: Final[int] = 300
SERIES_CHILD_RESPONSE_COUNT: Final[int] = 2

ROOTFOLDER_UNAVAILABLE_DETAIL: Final[str] = "root folder listing unavailable"
ROOTFOLDER_NOT_LISTED_SUFFIX: Final[str] = "is not listed by Sonarr /rootfolder"
ROOTFOLDER_OUTSIDE_SUFFIX: Final[str] = "is outside the configured anime allowlist (not eligible)"


class VerdictReason(Enum):
    """Machine-readable reasons an inventory snapshot is not complete."""

    SERIES_ENUMERATION_FAILED = "series_enumeration_failed"
    TRANSPORT_FAILURE = "transport_failure"
    MALFORMED_RESPONSE = "malformed_response"
    FILE_ID_COLLISION = "file_id_collision"
    INCONSISTENT_ENUMERATION = "inconsistent_enumeration"
    INCOMPLETE_SNAPSHOT = "incomplete_snapshot"


class FileRejectReason(Enum):
    """Machine-readable reasons a Sonarr episode file was not indexed."""

    MISSING_REQUIRED_FIELD = "missing_required_field"
    SERIES_MISMATCH = "series_mismatch"
    DUPLICATE_FILE_ID = "duplicate_file_id"
    INVALID_PATH = "invalid_path"
    OUTSIDE_ALLOWED_ROOT = "outside_allowed_root"
    MALFORMED_EPISODE_DATA = "malformed_episode_data"


class RebaselineReason(Enum):
    """Machine-readable identity discontinuities between two baseline snapshots."""

    SERIES_IDENTITY_CHANGED = "series_identity_changed"
    EPISODE_IDENTITY_CHANGED = "episode_identity_changed"
    FILE_IDENTITY_CHANGED = "file_identity_changed"


@dataclass(frozen=True)
class SonarrSeries:
    """One Sonarr series resource; an absent ``series_type`` is never anime evidence."""

    id: int
    title: str
    root_folder_path: str
    tvdb_id: int | None
    series_type: str | None = None


@dataclass(frozen=True)
class SonarrEpisode:
    """One episode resource; ``episode_file_id`` 0 means the episode has no file."""

    id: int
    series_id: int
    season_number: int
    episode_number: int
    title: str
    absolute_episode_number: int | None
    tvdb_id: int | None
    episode_file_id: int


@dataclass(frozen=True)
class SonarrFileRecord:
    """A structurally valid ``episodefile`` entry before scope and grouping checks."""

    id: int
    series_id: int
    path: str
    size: int


@dataclass(frozen=True)
class SonarrFile:
    """An indexed episode file: exact normalized path, byte size, complete episode set."""

    series_id: int
    episode_file_id: int
    path: str
    size: int
    episodes: tuple[SonarrEpisode, ...]


@dataclass(frozen=True)
class RejectedSonarrFile:
    """A file record that was refused (with a reason) instead of guessed into the index."""

    series_id: int
    episode_file_id: int | None
    path: str | None
    size: int | None
    reason: FileRejectReason


@dataclass(frozen=True)
class EligibleSeries:
    """An eligible series plus the exact allowlist root that admitted it."""

    series: SonarrSeries
    root: str


@dataclass(frozen=True)
class SeriesParseResult:
    series: tuple[SonarrSeries, ...]
    reasons: tuple[VerdictReason, ...]
    details: tuple[str, ...]


@dataclass(frozen=True)
class EpisodeParseResult:
    """Parsed episodes.

    ``tainted_file_ids`` marks files whose episode sets are untrustworthy;
    ``file_reference_tainted`` marks a series whose episodes contained an
    unparseable file reference, so no file group in that series can be
    trusted (partial groups are never indexed).
    """

    episodes: tuple[SonarrEpisode, ...]
    tainted_file_ids: frozenset[int]
    reasons: tuple[VerdictReason, ...]
    details: tuple[str, ...]
    file_reference_tainted: bool = False


@dataclass(frozen=True)
class FileParseResult:
    records: tuple[SonarrFileRecord, ...]
    rejected: tuple[RejectedSonarrFile, ...]
    reasons: tuple[VerdictReason, ...]
    details: tuple[str, ...]


@dataclass(frozen=True)
class FileIndexResult:
    files: tuple[SonarrFile, ...]
    rejected: tuple[RejectedSonarrFile, ...]
    unlinked: tuple[tuple[int, int], ...]
    reasons: tuple[VerdictReason, ...]
    details: tuple[str, ...]


@dataclass(frozen=True)
class InventoryVerdict:
    """Baseline validity: complete snapshots are the only ones safe for auto-apply."""

    complete: bool
    auto_apply_eligible: bool
    reasons: tuple[VerdictReason, ...]
    details: tuple[str, ...]


class SourceRelationshipStatus(Enum):
    """Outcome of a fresh pre-write check of one Sonarr file relationship."""

    CURRENT = "current"
    CHANGED = "changed"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class SourceRelationshipVerdict:
    status: SourceRelationshipStatus
    detail: str


@dataclass(frozen=True)
class SonarrInventory:
    """One read-only pass over the eligible Sonarr collection."""

    eligible_series: tuple[EligibleSeries, ...]
    excluded_series: tuple[SonarrSeries, ...]
    episodes: tuple[SonarrEpisode, ...]
    files: tuple[SonarrFile, ...]
    rejected_files: tuple[RejectedSonarrFile, ...]
    unlinked_file_ids: tuple[tuple[int, int], ...]
    warnings: tuple[str, ...]
    verdict: InventoryVerdict
    series_manifest_hash: str | None = None
    series_cursor: int = 0
    swept_series_ids: frozenset[int] = frozenset()
    refreshed_series_ids: frozenset[int] = frozenset()
    fetched_series_children: Mapping[int, tuple[tuple[Any, ...], tuple[Any, ...]]] | None = None
    refresh_sweep_complete: bool = False
    invalidated_series_ids: frozenset[int] = frozenset()


@dataclass(frozen=True)
class _SeriesChildrenSnapshot:
    manifest_hash: str
    cursor: int
    cache: Mapping[int, tuple[tuple[Any, ...], tuple[Any, ...]]]
    fetched: Mapping[int, tuple[tuple[Any, ...], tuple[Any, ...]]]
    swept_series_ids: frozenset[int]
    refreshed_series_ids: frozenset[int]
    complete: bool
    invalidated_series_ids: frozenset[int]


@dataclass(frozen=True)
class _IndexedSeriesChildren:
    episodes: tuple[SonarrEpisode, ...]
    files: tuple[SonarrFile, ...]
    rejected: tuple[RejectedSonarrFile, ...]
    unlinked: tuple[tuple[int, int], ...]


@dataclass(frozen=True)
class SeriesIdentity:
    """Series placement identity: the series' own ``rootFolderPath``, not the allowlist root."""

    title: str
    root_folder_path: str


@dataclass(frozen=True)
class EpisodeIdentity:
    title: str
    season_number: int
    episode_number: int


@dataclass(frozen=True)
class FileIdentity:
    path: str
    size: int


@dataclass(frozen=True)
class BaselineSnapshot:
    """Identity of one observed inventory, keyed by Sonarr IDs."""

    complete: bool
    auto_apply_eligible: bool
    reasons: tuple[VerdictReason, ...]
    details: tuple[str, ...]
    series_identities: Mapping[int, SeriesIdentity]
    episode_identities: Mapping[tuple[int, int], EpisodeIdentity]
    file_identities: Mapping[tuple[int, int], FileIdentity]


@dataclass(frozen=True)
class RebaselineVerdict:
    """True when a source identity discontinuity means the baseline must be rebuilt."""

    requires_rebaseline: bool
    reasons: tuple[RebaselineReason, ...]
    details: tuple[str, ...]


# ------------------------------------------------------------------ pure decisions


def select_eligible_series(
    series: Sequence[SonarrSeries],
    allowed_roots: Sequence[str],
    *,
    anime_only: bool = True,  # noqa: ARG001 - compatibility; scope cannot be widened
) -> tuple[tuple[EligibleSeries, ...], tuple[SonarrSeries, ...]]:
    """Split series by exact component-aware root containment.

    Only the positively identified Sonarr type ``anime`` is admitted, even when a
    live-action series shares the same allowed root. ``anime_only`` is retained for
    caller compatibility and cannot disable this invariant. A series whose root is unusable
    as a POSIX path (or absent from the allowlist) is excluded. Output order is by series
    ID, independent of API enumeration order.
    """
    eligible: list[EligibleSeries] = []
    excluded: list[SonarrSeries] = []
    for item in series:
        if item.series_type != "anime":
            excluded.append(item)
            continue
        root = _matching_root(item.root_folder_path, allowed_roots)
        if root is None:
            excluded.append(item)
        else:
            eligible.append(EligibleSeries(series=item, root=root))
    eligible.sort(key=lambda item: item.series.id)
    excluded.sort(key=lambda item: item.id)
    return tuple(eligible), tuple(excluded)


def parse_series_entries(raw: Sequence[Any]) -> SeriesParseResult:
    """Parse ``/api/v3/series`` entries; a malformed required field fails that entry closed."""
    series: list[SonarrSeries] = []
    reasons: list[VerdictReason] = []
    details: list[str] = []
    for index, item in enumerate(raw):
        entry = _as_mapping(item)
        if entry is None:
            reasons.append(VerdictReason.MALFORMED_RESPONSE)
            details.append(f"series entry {index} is not an object")
            continue
        series_id = _required_int(entry, "id")
        title = _required_str(entry, "title")
        root = _required_str(entry, "rootFolderPath")
        tvdb_id, tvdb_ok = _optional_int(entry, "tvdbId")
        series_type, series_type_ok = _optional_text(entry, "seriesType")
        problems = _series_problems(series_id, title, root, tvdb_ok, series_type_ok)
        if problems:
            reasons.append(VerdictReason.MALFORMED_RESPONSE)
            details.append(f"series entry {index} malformed ({', '.join(problems)})")
            continue
        assert series_id is not None
        assert title is not None
        assert root is not None
        series.append(
            SonarrSeries(
                id=series_id,
                title=title,
                root_folder_path=root,
                tvdb_id=tvdb_id,
                series_type=series_type,
            )
        )
    return SeriesParseResult(tuple(series), tuple(reasons), tuple(details))


def parse_episode_entries(raw: Sequence[Any], series_id: int) -> EpisodeParseResult:
    """Parse ``/api/v3/episode`` entries for one series.

    A malformed episode taints the episode set of the file it references (that
    file then fails closed), so a partial episode group is never trusted. An
    unparseable ``episodeFileId`` value (present, unusable) taints the entire
    series file set: the reference target is unresolvable, so no group
    in that series can be proven complete. Duplicate episode IDs mark the
    enumeration inconsistent and taint the referenced file.
    """
    episodes: list[SonarrEpisode] = []
    seen_ids: set[int] = set()
    tainted: set[int] = set()
    reasons: list[VerdictReason] = []
    details: list[str] = []
    file_reference_tainted = False
    for index, item in enumerate(raw):
        entry = _as_mapping(item)
        if entry is None:
            reasons.append(VerdictReason.MALFORMED_RESPONSE)
            details.append(f"episode entry {index} is not an object")
            continue
        episode_id = _required_int(entry, "id")
        owner_id = _required_int(entry, "seriesId")
        season_number = _required_int(entry, "seasonNumber")
        episode_number = _required_int(entry, "episodeNumber")
        title = _required_str(entry, "title")
        file_id, file_ok = _episode_file_id(entry)
        absolute, absolute_ok = _optional_int(entry, "absoluteEpisodeNumber")
        tvdb_id, tvdb_ok = _optional_int(entry, "tvdbId")
        problems = _episode_problems(
            episode_id,
            owner_id,
            series_id,
            season_number,
            episode_number,
            title,
            file_ok,
            absolute_ok,
            tvdb_ok,
        )
        if problems:
            reasons.append(VerdictReason.MALFORMED_RESPONSE)
            details.append(f"episode entry {index} malformed ({', '.join(problems)})")
            if not file_ok:
                file_reference_tainted = True
            elif file_id > 0:
                tainted.add(file_id)
            continue
        assert episode_id is not None
        assert season_number is not None
        assert episode_number is not None
        assert title is not None
        if episode_id in seen_ids:
            reasons.append(VerdictReason.INCONSISTENT_ENUMERATION)
            details.append(f"episode entry {index} repeats episode id {episode_id}")
            if file_ok and file_id > 0:
                tainted.add(file_id)
            continue
        seen_ids.add(episode_id)
        episodes.append(
            SonarrEpisode(
                id=episode_id,
                series_id=series_id,
                season_number=season_number,
                episode_number=episode_number,
                title=title,
                absolute_episode_number=absolute,
                tvdb_id=tvdb_id,
                episode_file_id=file_id,
            )
        )
    return EpisodeParseResult(
        tuple(episodes),
        frozenset(tainted),
        tuple(reasons),
        tuple(details),
        file_reference_tainted,
    )


def parse_file_entries(raw: Sequence[Any], series_id: int) -> FileParseResult:
    """Parse ``/api/v3/episodefile`` entries for one series; malformed records reject."""
    records: list[SonarrFileRecord] = []
    rejected: list[RejectedSonarrFile] = []
    reasons: list[VerdictReason] = []
    details: list[str] = []
    for index, item in enumerate(raw):
        entry = _as_mapping(item)
        if entry is None:
            reasons.append(VerdictReason.MALFORMED_RESPONSE)
            details.append(f"file entry {index} is not an object")
            continue
        file_id = _required_int(entry, "id")
        owner_id = _required_int(entry, "seriesId")
        path = _required_str(entry, "path")
        size = _required_int(entry, "size")
        problems = _file_problems(file_id, owner_id, path, size)
        if problems:
            reasons.append(VerdictReason.MALFORMED_RESPONSE)
            details.append(f"file entry {index} malformed ({', '.join(problems)})")
            rejected.append(
                RejectedSonarrFile(
                    series_id=series_id,
                    episode_file_id=file_id,
                    path=path,
                    size=size,
                    reason=FileRejectReason.MISSING_REQUIRED_FIELD,
                )
            )
            continue
        assert file_id is not None
        assert path is not None
        assert size is not None
        if owner_id != series_id:
            reasons.append(VerdictReason.INCONSISTENT_ENUMERATION)
            details.append(f"file entry {index} reports foreign seriesId {owner_id}")
            rejected.append(
                RejectedSonarrFile(
                    series_id=series_id,
                    episode_file_id=file_id,
                    path=path,
                    size=size,
                    reason=FileRejectReason.SERIES_MISMATCH,
                )
            )
            continue
        records.append(SonarrFileRecord(id=file_id, series_id=series_id, path=path, size=size))
    return FileParseResult(tuple(records), tuple(rejected), tuple(reasons), tuple(details))


def index_series_files(
    item: EligibleSeries,
    episodes: EpisodeParseResult,
    files: FileParseResult,
) -> FileIndexResult:
    """Index one eligible series' files: grouping, collisions, root escape, dangling refs.

    Every accepted file carries the complete set of episodes sharing its
    ``episodeFileId`` (season 0 included). Files no episode references are
    returned flagged in ``unlinked`` with an empty episode set; episodes with
    no file are simply not files. When the series' episode list contained
    an unparseable file reference, **no** file is indexed: its group membership
    is unprovable, so every record is refused with ``MALFORMED_EPISODE_DATA``.
    """
    series_id = item.series.id
    reasons: list[VerdictReason] = []
    details: list[str] = []
    rejected: list[RejectedSonarrFile] = list(files.rejected)
    by_id, collided, collisions, collision_reasons, collision_details = _deduplicate_records(
        files.records, series_id
    )
    rejected.extend(collisions)
    reasons.extend(collision_reasons)
    details.extend(collision_details)
    if episodes.file_reference_tainted:
        for file_id in sorted(by_id):
            rejected.append(_reject_record(by_id[file_id], FileRejectReason.MALFORMED_EPISODE_DATA))
        _append(reasons, VerdictReason.MALFORMED_RESPONSE)
        details.append(
            f"series {series_id}: an episode file reference was unparseable; "
            "no file group in this series can be proven complete"
        )
        rejected.sort(key=_reject_key)
        return FileIndexResult((), tuple(rejected), (), tuple(reasons), tuple(details))
    episodes_by_file: dict[int, list[SonarrEpisode]] = {}
    for episode in episodes.episodes:
        if episode.episode_file_id > 0:
            episodes_by_file.setdefault(episode.episode_file_id, []).append(episode)
    files_out: list[SonarrFile] = []
    unlinked: list[tuple[int, int]] = []
    for file_id in sorted(by_id):
        record = by_id[file_id]
        if file_id in episodes.tainted_file_ids:
            rejected.append(_reject_record(record, FileRejectReason.MALFORMED_EPISODE_DATA))
            _append(reasons, VerdictReason.MALFORMED_RESPONSE)
            details.append(
                f"series {series_id}: file {file_id} has a malformed episode reference; "
                "its episode set cannot be trusted"
            )
            continue
        rejection = _path_rejection(record.path, item.root)
        if rejection is not None:
            rejected.append(_reject_record(record, rejection))
            details.append(f"series {series_id}: file {file_id} rejected ({rejection.value})")
            continue
        group = tuple(sorted(episodes_by_file.get(file_id, []), key=lambda episode: episode.id))
        files_out.append(
            SonarrFile(
                series_id=series_id,
                episode_file_id=file_id,
                path=_normalize(record.path),
                size=record.size,
                episodes=group,
            )
        )
        if not group:
            unlinked.append((series_id, file_id))
    for file_id in sorted(episodes_by_file):
        if file_id not in by_id and file_id not in collided:
            _append(reasons, VerdictReason.INCONSISTENT_ENUMERATION)
            details.append(
                f"series {series_id}: episodes reference missing episodeFileId {file_id}"
            )
    rejected.sort(key=_reject_key)
    return FileIndexResult(
        tuple(files_out), tuple(rejected), tuple(unlinked), tuple(reasons), tuple(details)
    )


def validate_root_folders(configured: Sequence[str], observed: Sequence[str]) -> tuple[str, ...]:
    """Compare configured anime roots with Sonarr's reported root folders.

    A mismatch is a recorded warning, never a silent expansion of scope: root
    folders Sonarr reports outside the allowlist stay ineligible.
    """
    warnings: list[str] = []
    normalized: list[str] = []
    for item in observed:
        try:
            normalized.append(normalize_posix(item))
        except PathError:
            warnings.append(f"Sonarr reported an unusable root folder path {item!r}")
    normalized.sort()
    for root in configured:
        if not any(is_within_root(root, seen) for seen in normalized):
            warnings.append(f"configured root {root!r} {ROOTFOLDER_NOT_LISTED_SUFFIX}")
    for seen in normalized:
        if not any(is_within_root(seen, root) for root in configured):
            warnings.append(f"Sonarr root folder {seen!r} {ROOTFOLDER_OUTSIDE_SUFFIX}")
    return tuple(warnings)


def build_baseline(inventory: SonarrInventory) -> BaselineSnapshot:
    """Reduce an inventory to comparable per-ID identities (pure)."""
    series_identities = {
        item.series.id: SeriesIdentity(item.series.title, item.series.root_folder_path)
        for item in inventory.eligible_series
    }
    episode_identities = {
        (episode.series_id, episode.id): EpisodeIdentity(
            episode.title, episode.season_number, episode.episode_number
        )
        for episode in inventory.episodes
    }
    file_identities = {
        (file.series_id, file.episode_file_id): FileIdentity(file.path, file.size)
        for file in inventory.files
    }
    return BaselineSnapshot(
        complete=inventory.verdict.complete,
        auto_apply_eligible=inventory.verdict.auto_apply_eligible,
        reasons=inventory.verdict.reasons,
        details=inventory.verdict.details,
        series_identities=dict(series_identities),
        episode_identities=dict(episode_identities),
        file_identities=dict(file_identities),
    )


def compare_baseline(previous: BaselineSnapshot, current: BaselineSnapshot) -> RebaselineVerdict:
    """Separate source identity discontinuity from ordinary Sonarr churn.

    Discontinuity (source replaced, e.g. Sonarr database restore — automatic
    writes must stop until a fresh baseline is established):
    a previously seen series ID with a changed **root**, an episode ID with
    changed season/episode numbers, or a file ID with a changed **size**
    (replacement). Ordinary churn is reported in details without forcing a
    rebaseline: renames/moves (same file id and size, new path) and
    title-only edits. Added and removed IDs are churn as well.
    """
    reasons: list[RebaselineReason] = []
    details: list[str] = []
    for series_id, series_identity in sorted(previous.series_identities.items()):
        current_series = current.series_identities.get(series_id)
        if current_series is None:
            continue
        if current_series.root_folder_path != series_identity.root_folder_path:
            _append(reasons, RebaselineReason.SERIES_IDENTITY_CHANGED)
            details.append(
                f"series {series_id} root changed from {series_identity.root_folder_path!r} "
                f"to {current_series.root_folder_path!r}"
            )
        elif current_series.title != series_identity.title:
            details.append(f"series {series_id} title changed (metadata churn)")
    for key, episode_identity in sorted(previous.episode_identities.items()):
        current_episode = current.episode_identities.get(key)
        if current_episode is None:
            continue
        if (
            current_episode.season_number != episode_identity.season_number
            or current_episode.episode_number != episode_identity.episode_number
        ):
            _append(reasons, RebaselineReason.EPISODE_IDENTITY_CHANGED)
            details.append(
                f"episode {key[1]} of series {key[0]} renumbered from "
                f"S{episode_identity.season_number}E{episode_identity.episode_number} to "
                f"S{current_episode.season_number}E{current_episode.episode_number}"
            )
        elif current_episode.title != episode_identity.title:
            details.append(f"episode {key[1]} of series {key[0]} title changed (metadata churn)")
    for key, file_identity in sorted(previous.file_identities.items()):
        current_file = current.file_identities.get(key)
        if current_file is None:
            continue
        if current_file.size != file_identity.size:
            _append(reasons, RebaselineReason.FILE_IDENTITY_CHANGED)
            details.append(
                f"file {key[1]} of series {key[0]} changed size "
                f"{file_identity.size} -> {current_file.size} at {current_file.path!r} "
                "(replacement signal)"
            )
        elif current_file.path != file_identity.path:
            details.append(
                f"file {key[1]} of series {key[0]} renamed from "
                f"{file_identity.path!r} to {current_file.path!r} (path drift churn)"
            )
    return RebaselineVerdict(bool(reasons), tuple(reasons), tuple(details))


# ------------------------------------------------------------------------------ I/O


def collect_sonarr_inventory(
    endpoint: ApiEndpoint,
    settings: Settings | None = None,
    *,
    allowed_roots: Sequence[str] | None = None,
    anime_only: bool | None = None,
    cached_manifest_hash: str | None = None,
    cached_series_children: Mapping[int, tuple[Sequence[Any], Sequence[Any]]] | None = None,
    series_cursor: int = 0,
    swept_series_ids: frozenset[int] = frozenset(),
    series_deadline: float | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    on_progress: Callable[[int, int], None] | None = None,
) -> SonarrInventory:
    """Read the eligible Sonarr collection once (series, episodes, files, rootfolders).

    Requests use relative API paths through :func:`shoko_linker.http.resolve_and_send`.
    Transport or shape failures are recorded as verdict reasons, never raised,
    so one broken series response cannot crash the inventory; per-series fetches
    stop at that series.
    """
    if settings is None:
        effective_roots = _setup_inventory_roots(allowed_roots, anime_only)
    else:
        effective_roots = (
            settings.sonarr.allowed_roots
            if allowed_roots is None
            else tuple(normalize_posix(root) for root in allowed_roots)
        )
    warnings: list[str] = []
    reasons: list[VerdictReason] = []
    details: list[str] = []
    outcome = _fetch_json_list(endpoint, SERIES_PATH)
    if outcome.payload is None:
        _append(reasons, VerdictReason.SERIES_ENUMERATION_FAILED)
        details.append(f"cannot enumerate Sonarr series: {outcome.detail}")
        return SonarrInventory((), (), (), (), (), (), (), make_verdict(reasons, details))

    series_result = parse_series_entries(outcome.payload)
    reasons.extend(series_result.reasons)
    details.extend(series_result.details)
    manifest_hash = _inventory_manifest_hash(endpoint, effective_roots, series_result.series)
    eligible, excluded = select_eligible_series(
        series_result.series,
        effective_roots,
        anime_only=True,
    )

    warnings.extend(_collect_root_folder_warnings(endpoint, effective_roots))
    children = _collect_series_children(
        endpoint,
        eligible,
        manifest_hash=manifest_hash,
        cached_manifest_hash=cached_manifest_hash,
        cached_series_children=cached_series_children,
        series_cursor=series_cursor,
        swept_series_ids=swept_series_ids,
        series_deadline=series_deadline,
        monotonic=monotonic,
        reasons=reasons,
        details=details,
        on_progress=on_progress,
    )
    indexed = _index_series_children(eligible, children.cache, reasons, details)
    _mark_incomplete_inventory(eligible, children, reasons, details)

    episodes = tuple(sorted(indexed.episodes, key=lambda episode: (episode.series_id, episode.id)))
    files = tuple(sorted(indexed.files, key=lambda item: (item.series_id, item.episode_file_id)))
    rejected = tuple(sorted(indexed.rejected, key=_reject_key))
    unlinked = tuple(sorted(indexed.unlinked))
    return SonarrInventory(
        eligible_series=eligible,
        excluded_series=excluded,
        episodes=episodes,
        files=files,
        rejected_files=rejected,
        unlinked_file_ids=unlinked,
        warnings=tuple(warnings),
        verdict=make_verdict(reasons, details),
        series_manifest_hash=children.manifest_hash,
        series_cursor=children.cursor,
        swept_series_ids=children.swept_series_ids,
        refreshed_series_ids=children.refreshed_series_ids,
        fetched_series_children=children.fetched,
        refresh_sweep_complete=children.complete,
        invalidated_series_ids=children.invalidated_series_ids,
    )


def check_current_file_relationship(  # noqa: PLR0911, PLR0912
    endpoint: ApiEndpoint,
    *,
    series_id: int,
    episode_file_id: int,
    path: str,
    size: int,
    episode_identities: Sequence[tuple[int, int | None, int, int, int | None]],
    tvdb_series_id: int,
    allowed_roots: Sequence[str],
    deadline: float | None = None,
    monotonic: Callable[[], float] = time.monotonic,
) -> SourceRelationshipVerdict:
    """Freshly verify one candidate's Sonarr series, file, and episode relationship.

    These uncached reads are intentionally made for only the already-selected series
    immediately before a write, even when its resumable inventory fragment is still fresh.
    The candidate must still be the same positively identified anime series in the authorized
    root, and the frozen file/episode relationship must still match. A changed source is
    distinct from an unavailable/malformed read: the former must be quarantined against its
    frozen targets, while the latter is a retryable no-write deferral.
    """
    if deadline is not None and monotonic() >= deadline:
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.UNAVAILABLE,
            "Sonarr source check deferred by the apply time budget",
        )
    series = _check_current_series_identity(
        endpoint,
        series_id=series_id,
        path=path,
        tvdb_series_id=tvdb_series_id,
        allowed_roots=allowed_roots,
        deadline=deadline,
        monotonic=monotonic,
    )
    if series.status is not SourceRelationshipStatus.CURRENT:
        return series

    if deadline is not None and monotonic() >= deadline:
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.UNAVAILABLE,
            "Sonarr source check deferred before episode read by the apply time budget",
        )
    episodes_outcome = _fetch_json_list(endpoint, EPISODE_PATH, {"seriesId": series_id})
    if episodes_outcome.payload is None:
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.UNAVAILABLE,
            f"current Sonarr episode list unavailable: {episodes_outcome.detail}",
        )
    if deadline is not None and monotonic() >= deadline:
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.UNAVAILABLE,
            "Sonarr source check deferred before episode-file read by the apply time budget",
        )
    files_outcome = _fetch_json_list(endpoint, EPISODE_FILE_PATH, {"seriesId": series_id})
    if files_outcome.payload is None:
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.UNAVAILABLE,
            f"current Sonarr episode-file list unavailable: {files_outcome.detail}",
        )
    if deadline is not None and monotonic() >= deadline:
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.UNAVAILABLE,
            "Sonarr source check exceeded the apply time budget",
        )

    parsed_episodes = parse_episode_entries(episodes_outcome.payload, series_id)
    parsed_files = parse_file_entries(files_outcome.payload, series_id)
    if parsed_episodes.reasons or parsed_files.reasons or parsed_episodes.file_reference_tainted:
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.UNAVAILABLE,
            "current Sonarr source relationship is malformed or inconsistent",
        )
    if episode_file_id in parsed_episodes.tainted_file_ids:
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.UNAVAILABLE,
            f"current Sonarr episode membership for file {episode_file_id} is malformed",
        )

    current_files = [item for item in parsed_files.records if item.id == episode_file_id]
    if not current_files:
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.CHANGED,
            f"Sonarr no longer reports episode file {episode_file_id} in series {series_id}",
        )
    if len(current_files) != 1:
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.UNAVAILABLE,
            f"Sonarr reports duplicate episode-file id {episode_file_id}",
        )
    current_file = current_files[0]
    try:
        current_path = _normalize(current_file.path)
        expected_path = _normalize(path)
    except PathError:
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.UNAVAILABLE,
            f"Sonarr episode-file path for file {episode_file_id} is invalid",
        )
    if current_path != expected_path or current_file.size != size:
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.CHANGED,
            f"Sonarr episode-file path or size changed for file {episode_file_id}",
        )

    expected = tuple(sorted(episode_identities))
    current = tuple(
        sorted(
            (
                item.id,
                item.tvdb_id,
                item.season_number,
                item.episode_number,
                item.absolute_episode_number,
            )
            for item in parsed_episodes.episodes
            if item.episode_file_id == episode_file_id
        )
    )
    if current != expected:
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.CHANGED,
            f"Sonarr episode membership changed for file {episode_file_id}",
        )
    return SourceRelationshipVerdict(SourceRelationshipStatus.CURRENT, "")


def _check_current_series_identity(  # noqa: PLR0911
    endpoint: ApiEndpoint,
    *,
    series_id: int,
    path: str,
    tvdb_series_id: int,
    allowed_roots: Sequence[str],
    deadline: float | None,
    monotonic: Callable[[], float],
) -> SourceRelationshipVerdict:
    """Verify that this exact current series is still eligible for the frozen source file."""
    series_outcome = _fetch_json_list(endpoint, SERIES_PATH)
    if series_outcome.payload is None:
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.UNAVAILABLE,
            f"current Sonarr series list unavailable: {series_outcome.detail}",
        )
    if deadline is not None and monotonic() >= deadline:
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.UNAVAILABLE,
            "Sonarr source check exceeded the apply time budget after series read",
        )
    parsed_series = parse_series_entries(series_outcome.payload)
    if parsed_series.reasons:
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.UNAVAILABLE,
            "current Sonarr series list is malformed or inconsistent",
        )
    matching_series = [item for item in parsed_series.series if item.id == series_id]
    if not matching_series:
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.CHANGED,
            f"Sonarr no longer reports series {series_id}",
        )
    if len(matching_series) != 1:
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.UNAVAILABLE,
            f"Sonarr reports duplicate series id {series_id}",
        )
    current_series = matching_series[0]
    if current_series.series_type != "anime":
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.CHANGED,
            f"Sonarr series {series_id} is no longer positively identified as anime",
        )
    if current_series.tvdb_id != tvdb_series_id:
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.CHANGED,
            f"Sonarr series {series_id} TVDB identity changed from {tvdb_series_id} "
            f"to {current_series.tvdb_id}",
        )
    eligible, _excluded = select_eligible_series((current_series,), allowed_roots)
    if not eligible:
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.CHANGED,
            f"Sonarr series {series_id} root is no longer within an authorized anime root",
        )
    try:
        current_root = _normalize(current_series.root_folder_path)
        expected_path = _normalize(path)
    except PathError:
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.CHANGED,
            f"Sonarr series {series_id} root or frozen file path is invalid",
        )
    if not is_within_root(expected_path, current_root):
        return SourceRelationshipVerdict(
            SourceRelationshipStatus.CHANGED,
            f"frozen Sonarr file path is outside current series {series_id} root",
        )
    return SourceRelationshipVerdict(SourceRelationshipStatus.CURRENT, "")


def _setup_inventory_roots(
    allowed_roots: Sequence[str] | None, anime_only: bool | None
) -> tuple[str, ...]:
    if not allowed_roots or anime_only is not True:
        raise ValueError("setup inventory requires explicit roots and anime-only selection")
    try:
        roots = tuple(normalize_posix(root) for root in allowed_roots)
    except (PathError, TypeError):
        raise ValueError("setup inventory roots are invalid") from None
    if len(set(roots)) != len(roots):
        raise ValueError("setup inventory roots must be unique")
    return roots


def _collect_series_children(
    endpoint: ApiEndpoint,
    eligible: Sequence[EligibleSeries],
    *,
    manifest_hash: str,
    cached_manifest_hash: str | None,
    cached_series_children: Mapping[int, tuple[Sequence[Any], Sequence[Any]]] | None,
    series_cursor: int,
    swept_series_ids: frozenset[int],
    series_deadline: float | None,
    monotonic: Callable[[], float],
    reasons: list[VerdictReason],
    details: list[str],
    on_progress: Callable[[int, int], None] | None = None,
) -> _SeriesChildrenSnapshot:
    cache_is_current = cached_manifest_hash == manifest_hash
    cache = {
        series_id: (tuple(children[0]), tuple(children[1]))
        for series_id, children in (cached_series_children or {}).items()
        if cache_is_current and len(children) == SERIES_CHILD_RESPONSE_COUNT
    }
    cursor = series_cursor if cache_is_current else 0
    swept = set(swept_series_ids if cache_is_current else ())
    fetched: dict[int, tuple[tuple[Any, ...], tuple[Any, ...]]] = {}
    refreshed: set[int] = set()
    invalidated: set[int] = set()
    eligible_ids = {item.series.id for item in eligible}
    ordered = sorted(eligible, key=lambda item: (item.series.id <= cursor, item.series.id))
    if on_progress is not None:
        on_progress(0, len(ordered))
    for index, item in enumerate(ordered, start=1):
        if series_deadline is not None and monotonic() >= series_deadline:
            break
        cursor = item.series.id
        episodes, files = _fetch_series_children(
            endpoint,
            item,
            reasons,
            details,
            deadline=series_deadline,
            monotonic=monotonic,
        )
        if on_progress is not None:
            on_progress(index, len(ordered))
        if episodes is None or files is None:
            cache.pop(item.series.id, None)
            swept.discard(item.series.id)
            invalidated.add(item.series.id)
            continue
        children = (tuple(episodes), tuple(files))
        cache[item.series.id] = children
        fetched[item.series.id] = children
        refreshed.add(item.series.id)
        swept.add(item.series.id)
        if eligible_ids.issubset(swept):
            break
    return _SeriesChildrenSnapshot(
        manifest_hash=manifest_hash,
        cursor=cursor,
        cache=cache,
        fetched=fetched,
        swept_series_ids=frozenset(swept),
        refreshed_series_ids=frozenset(refreshed),
        complete=eligible_ids.issubset(swept),
        invalidated_series_ids=frozenset(invalidated),
    )


def _index_series_children(
    eligible: Sequence[EligibleSeries],
    cache: Mapping[int, tuple[tuple[Any, ...], tuple[Any, ...]]],
    reasons: list[VerdictReason],
    details: list[str],
) -> _IndexedSeriesChildren:
    episodes_all: list[SonarrEpisode] = []
    files_all: list[SonarrFile] = []
    rejected_all: list[RejectedSonarrFile] = []
    unlinked_all: list[tuple[int, int]] = []
    for item in eligible:
        payloads = cache.get(item.series.id)
        if payloads is None:
            continue
        episode_result = parse_episode_entries(payloads[0], item.series.id)
        file_result = parse_file_entries(payloads[1], item.series.id)
        index = index_series_files(item, episode_result, file_result)
        for result in (episode_result, file_result, index):
            reasons.extend(result.reasons)
            details.extend(result.details)
        episodes_all.extend(episode_result.episodes)
        files_all.extend(index.files)
        rejected_all.extend(index.rejected)
        unlinked_all.extend(index.unlinked)
    return _IndexedSeriesChildren(
        tuple(episodes_all), tuple(files_all), tuple(rejected_all), tuple(unlinked_all)
    )


def _mark_incomplete_inventory(
    eligible: Sequence[EligibleSeries],
    children: _SeriesChildrenSnapshot,
    reasons: list[VerdictReason],
    details: list[str],
) -> None:
    eligible_ids = {item.series.id for item in eligible}
    missing = eligible_ids - set(children.cache)
    if missing:
        details.append(f"series child data unavailable for {len(missing)} eligible series")
    if not children.complete:
        pending = len(eligible_ids - children.swept_series_ids)
        details.append(f"full inventory refresh pending for {pending} eligible series")
    if missing or not children.complete:
        _append(reasons, VerdictReason.INCOMPLETE_SNAPSHOT)


def _inventory_manifest_hash(
    endpoint: ApiEndpoint, allowed_roots: Sequence[str], series: Sequence[SonarrSeries]
) -> str:
    """Bind cached children to the exact Sonarr scope and relevant series identities."""
    scope_series = (
        item
        for item in series
        if item.series_type == "anime"
        or _matching_root(item.root_folder_path, allowed_roots) is not None
    )
    manifest = {
        "endpoint": str(endpoint.base_url).rstrip("/"),
        "roots": sorted(allowed_roots),
        "series": [
            (item.id, item.root_folder_path, item.tvdb_id, item.series_type)
            for item in sorted(scope_series, key=lambda item: item.id)
        ],
    }
    encoded = json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def make_verdict(reasons: Sequence[VerdictReason], details: Sequence[str]) -> InventoryVerdict:
    """Completeness is the absence of recorded reasons; auto-apply requires a complete snapshot."""
    deduped: list[VerdictReason] = []
    for reason in reasons:
        _append(deduped, reason)
    complete = not deduped
    return InventoryVerdict(
        complete=complete,
        auto_apply_eligible=complete,
        reasons=tuple(deduped),
        details=tuple(details),
    )


# --------------------------------------------------------------------------- helpers


def _deduplicate_records(
    records: Sequence[SonarrFileRecord], series_id: int
) -> tuple[
    dict[int, SonarrFileRecord],
    set[int],
    list[RejectedSonarrFile],
    list[VerdictReason],
    list[str],
]:
    """Split records into unique-by-id and fully rejected collisions (both copies refused)."""
    by_id: dict[int, SonarrFileRecord] = {}
    collided: set[int] = set()
    rejected: list[RejectedSonarrFile] = []
    reasons: list[VerdictReason] = []
    details: list[str] = []
    for record in records:
        previous = by_id.get(record.id)
        if previous is None and record.id not in collided:
            by_id[record.id] = record
            continue
        collided.add(record.id)
        if previous is not None:
            del by_id[record.id]
            rejected.append(_reject_record(previous, FileRejectReason.DUPLICATE_FILE_ID))
        rejected.append(_reject_record(record, FileRejectReason.DUPLICATE_FILE_ID))
        _append(reasons, VerdictReason.FILE_ID_COLLISION)
        details.append(f"series {series_id}: episodeFileId {record.id} reported more than once")
    return by_id, collided, rejected, reasons, details


def _collect_root_folder_warnings(
    endpoint: ApiEndpoint, allowed_roots: Sequence[str]
) -> tuple[str, ...]:
    outcome = _fetch_json_list(endpoint, ROOTFOLDER_PATH)
    if outcome.payload is None:
        return (f"{ROOTFOLDER_UNAVAILABLE_DETAIL}: {outcome.detail}",)
    observed = _root_folder_paths(outcome.payload)
    if observed is None:
        return (f"{ROOTFOLDER_UNAVAILABLE_DETAIL}: {ROOTFOLDER_PATH} entries are not objects",)
    return validate_root_folders(allowed_roots, observed)


def _fetch_series_children(
    endpoint: ApiEndpoint,
    item: EligibleSeries,
    reasons: list[VerdictReason],
    details: list[str],
    *,
    deadline: float | None = None,
    monotonic: Callable[[], float] = time.monotonic,
) -> tuple[list[Any] | None, list[Any] | None]:
    series_id = item.series.id
    episodes = _fetch_json_list(endpoint, EPISODE_PATH, {"seriesId": series_id})
    if episodes.payload is None:
        _append(reasons, episodes.reason or VerdictReason.TRANSPORT_FAILURE)
        details.append(f"series {series_id} episodes: {episodes.detail}")
        # The series is already unusable; skip the file read it cannot be joined with.
        return None, None
    if deadline is not None and monotonic() >= deadline:
        _append(reasons, VerdictReason.INCOMPLETE_SNAPSHOT)
        details.append(f"series {series_id} episode-file fetch deferred by inventory time budget")
        return None, None
    files = _fetch_json_list(endpoint, EPISODE_FILE_PATH, {"seriesId": series_id})
    if files.payload is None:
        _append(reasons, files.reason or VerdictReason.TRANSPORT_FAILURE)
        details.append(f"series {series_id} episode files: {files.detail}")
    return episodes.payload, files.payload


@dataclass(frozen=True)
class _FetchOutcome:
    payload: list[Any] | None
    reason: VerdictReason | None
    detail: str


def _fetch_json_list(
    endpoint: ApiEndpoint,
    path: str,
    params: Mapping[str, Any] | None = None,
) -> _FetchOutcome:
    """GET one relative API path requiring a JSON array body; failures are recorded, not raised."""
    try:
        response = resolve_and_send(endpoint, "GET", path, params=params)
    except (HttpPolicyError, httpx.HTTPError) as exc:
        return _FetchOutcome(None, VerdictReason.TRANSPORT_FAILURE, f"{path}: {exc}")
    try:
        if not MIN_SUCCESS_STATUS <= response.status_code < MAX_SUCCESS_STATUS:
            return _FetchOutcome(
                None,
                VerdictReason.TRANSPORT_FAILURE,
                f"{path}: HTTP {response.status_code}",
            )
        try:
            payload = response.json()
        except ValueError as exc:
            return _FetchOutcome(
                None, VerdictReason.MALFORMED_RESPONSE, f"{path}: invalid JSON ({exc})"
            )
    finally:
        response.close()
    if not isinstance(payload, list):
        return _FetchOutcome(
            None, VerdictReason.MALFORMED_RESPONSE, f"{path}: expected a JSON array"
        )
    return _FetchOutcome(payload, None, "")


def _root_folder_paths(payload: Sequence[Any]) -> list[str] | None:
    paths: list[str] = []
    for item in payload:
        entry = _as_mapping(item)
        if entry is None:
            return None
        path = _required_str(entry, "path")
        if path is not None:
            paths.append(path)
    return paths


def _matching_root(path: str, allowed_roots: Sequence[str]) -> str | None:
    for root in allowed_roots:
        if is_within_root(path, root):
            return root
    return None


def _path_rejection(path: str, root: str) -> FileRejectReason | None:
    try:
        normalized = normalize_posix(path)
    except PathError:
        return FileRejectReason.INVALID_PATH
    if not is_within_root(normalized, root):
        return FileRejectReason.OUTSIDE_ALLOWED_ROOT
    return None


def _normalize(path: str) -> str:
    """normalize_posix after _path_rejection accepted the path."""
    return normalize_posix(path)


def _reject_record(record: SonarrFileRecord, reason: FileRejectReason) -> RejectedSonarrFile:
    return RejectedSonarrFile(
        series_id=record.series_id,
        episode_file_id=record.id,
        path=record.path,
        size=record.size,
        reason=reason,
    )


def _reject_key(rejected: RejectedSonarrFile) -> tuple[int, int, str, str]:
    file_id = -1 if rejected.episode_file_id is None else rejected.episode_file_id
    path = "" if rejected.path is None else rejected.path
    return (rejected.series_id, file_id, rejected.reason.value, path)


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


def _optional_int(entry: Mapping[str, Any], key: str) -> tuple[int | None, bool]:
    """``(value, ok)``: missing/None is ``(None, True)``; a present non-int is ``(None, False)``."""
    if key not in entry:
        return None, True
    value = entry[key]
    if value is None:
        return None, True
    if isinstance(value, bool) or not isinstance(value, int):
        return None, False
    return value, True


def _optional_text(entry: Mapping[str, Any], key: str) -> tuple[str | None, bool]:
    """``(value, ok)``: missing/null is unknown; a present value must be non-empty text."""
    if key not in entry or entry[key] is None:
        return None, True
    value = entry[key]
    if not isinstance(value, str) or not value.strip():
        return None, False
    return value.strip().lower(), True


def _episode_file_id(entry: Mapping[str, Any]) -> tuple[int, bool]:
    """``(file_id, ok)``: missing/None/0 all mean no file; negative/non-int is malformed."""
    file_id = entry.get("episodeFileId")
    if file_id is None:
        return 0, True
    if isinstance(file_id, bool) or not isinstance(file_id, int) or file_id < 0:
        return 0, False
    return file_id, True


def _series_problems(
    series_id: int | None,
    title: str | None,
    root: str | None,
    tvdb_ok: bool,
    series_type_ok: bool,
) -> list[str]:
    problems: list[str] = []
    if series_id is None or series_id <= 0:
        problems.append("missing/invalid id")
    if title is None:
        problems.append("missing/invalid title")
    if root is None:
        problems.append("missing/invalid rootFolderPath")
    if not tvdb_ok:
        problems.append("invalid tvdbId")
    if not series_type_ok:
        problems.append("invalid seriesType")
    return problems


def _episode_problems(
    episode_id: int | None,
    owner_id: int | None,
    series_id: int,
    season_number: int | None,
    episode_number: int | None,
    title: str | None,
    file_ok: bool,
    absolute_ok: bool,
    tvdb_ok: bool,
) -> list[str]:
    problems: list[str] = []
    if episode_id is None or episode_id <= 0:
        problems.append("missing/invalid id")
    if owner_id is None or owner_id <= 0:
        problems.append("missing/invalid seriesId")
    elif owner_id != series_id:
        problems.append(f"foreign seriesId {owner_id}")
    if season_number is None or season_number < 0:
        problems.append("missing/invalid seasonNumber")
    if episode_number is None or episode_number <= 0:
        problems.append("missing/invalid episodeNumber")
    if title is None:
        problems.append("missing/invalid title")
    if not file_ok:
        problems.append("invalid episodeFileId")
    if not absolute_ok:
        problems.append("invalid absoluteEpisodeNumber")
    if not tvdb_ok:
        problems.append("invalid tvdbId")
    return problems


def _file_problems(
    file_id: int | None,
    owner_id: int | None,
    path: str | None,
    size: int | None,
) -> list[str]:
    problems: list[str] = []
    if file_id is None or file_id <= 0:
        problems.append("missing/invalid id")
    if owner_id is None or owner_id <= 0:
        problems.append("missing/invalid seriesId")
    if path is None:
        problems.append("missing/invalid path")
    if size is None or size < 0:
        problems.append("missing/invalid size")
    return problems
