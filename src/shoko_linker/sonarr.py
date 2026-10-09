"""Read-only Sonarr v3 inventory, eligibility filtering, and baseline identity.

Only series whose ``rootFolderPath`` lies component-aware inside a configured
``sonarr.allowed_roots`` entry are eligible; ``seriesType`` is deliberately never
consulted, so live-action TV is excluded by root scope, never by guess. Only
eligible series trigger ``api/v3/episode`` and ``api/v3/episodefile`` reads —
an excluded series produces zero secondary requests.

All I/O goes through :func:`shoko_linker.http.resolve_and_send` with relative API
paths, so urlBase preservation, the manual same-origin redirect walk, the no-retry
policy and origin binding stay enforced. Decision logic (selection, parsing,
indexing, root validation, baselining, comparison) is pure and I/O-free;
:func:`collect_sonarr_inventory` is the only function that talks to Sonarr.

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

from collections.abc import Mapping, Sequence
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
    """One Sonarr series resource (``seriesType`` intentionally not modeled)."""

    id: int
    title: str
    root_folder_path: str
    tvdb_id: int | None


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
) -> tuple[tuple[EligibleSeries, ...], tuple[SonarrSeries, ...]]:
    """Split series by exact component-aware root containment.

    ``seriesType`` is never consulted. A series whose root is unusable as a POSIX
    path (or absent from the allowlist) is excluded, so scope always fails closed.
    Output order is by series ID, independent of API enumeration order.
    """
    eligible: list[EligibleSeries] = []
    excluded: list[SonarrSeries] = []
    for item in series:
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
        problems = _series_problems(series_id, title, root, tvdb_ok)
        if problems:
            reasons.append(VerdictReason.MALFORMED_RESPONSE)
            details.append(f"series entry {index} malformed ({', '.join(problems)})")
            continue
        assert series_id is not None
        assert title is not None
        assert root is not None
        series.append(
            SonarrSeries(id=series_id, title=title, root_folder_path=root, tvdb_id=tvdb_id)
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


def collect_sonarr_inventory(endpoint: ApiEndpoint, settings: Settings) -> SonarrInventory:
    """Read the eligible Sonarr collection once (series, episodes, files, rootfolders).

    Requests use relative API paths through :func:`shoko_linker.http.resolve_and_send`.
    Transport or shape failures are recorded as verdict reasons, never raised,
    so one broken series response cannot crash the inventory; per-series fetches
    stop at that series.
    """
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
    eligible, excluded = select_eligible_series(series_result.series, settings.sonarr.allowed_roots)

    warnings.extend(_collect_root_folder_warnings(endpoint, settings.sonarr.allowed_roots))

    episodes_all: list[SonarrEpisode] = []
    files_all: list[SonarrFile] = []
    rejected_all: list[RejectedSonarrFile] = []
    unlinked_all: list[tuple[int, int]] = []
    for item in eligible:
        episodes, files = _fetch_series_children(endpoint, item, reasons, details)
        if episodes is None or files is None:
            continue
        episode_result = parse_episode_entries(episodes, item.series.id)
        file_result = parse_file_entries(files, item.series.id)
        index = index_series_files(item, episode_result, file_result)
        for result in (episode_result, file_result, index):
            reasons.extend(result.reasons)
            details.extend(result.details)
        episodes_all.extend(episode_result.episodes)
        files_all.extend(index.files)
        rejected_all.extend(index.rejected)
        unlinked_all.extend(index.unlinked)

    episodes_all.sort(key=lambda episode: (episode.series_id, episode.id))
    files_all.sort(key=lambda indexed: (indexed.series_id, indexed.episode_file_id))
    rejected_all.sort(key=_reject_key)
    unlinked_all.sort()
    return SonarrInventory(
        eligible_series=eligible,
        excluded_series=excluded,
        episodes=tuple(episodes_all),
        files=tuple(files_all),
        rejected_files=tuple(rejected_all),
        unlinked_file_ids=tuple(unlinked_all),
        warnings=tuple(warnings),
        verdict=make_verdict(reasons, details),
    )


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
) -> tuple[list[Any] | None, list[Any] | None]:
    series_id = item.series.id
    episodes = _fetch_json_list(endpoint, EPISODE_PATH, {"seriesId": series_id})
    if episodes.payload is None:
        _append(reasons, episodes.reason or VerdictReason.TRANSPORT_FAILURE)
        details.append(f"series {series_id} episodes: {episodes.detail}")
        # The series is already unusable; skip the file read it cannot be joined with.
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
