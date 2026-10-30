"""M5: Shoko internal episode-ID resolution, guarded link plan, single POST, verification.

This module implements the mutation milestone against the **pinned Shoko v5.3.3**
contract in ``docs/compatibility.md`` §2 — every endpoint, field, and semantic used
is cited there; nothing is invented. The wire format is PascalCase with nulls
included. Every request goes
through :func:`shoko_linker.http.resolve_and_send` (manual same-origin redirect
walk, no auto-follow, no retries, origin-bound auth headers); the Shoko API key
goes out only as the ``apikey`` header and never appears in any structure,
fingerprint, detail string, ``repr``, or persisted field defined here.

Write-safety model (all of it, because the endpoint's own semantics demand it):

* ``POST api/v3/File/{id}/Link`` takes **Shoko internal AnimeEpisodeIDs** (never
  AniDB ids), **replaces all User-source XRefs**, refuses when any AniDB-source
  XRef exists, and enqueues a ``ManualLinkJob`` **per episode** (async; the job
  chain can delete/move media server-side per docs §2 — late side effects such as
  renames and plugin reactions are **out of scope here and belong to deployment
  monitoring**). HTTP 2xx means **accepted, not verified**.
* Therefore: a plan is frozen with a canonical fingerprint; a **fresh**
  ``GET File/{id}?include=XRefs,AbsolutePaths`` runs immediately before the single
  POST (zero XRefs required — *any* XRef, User or AniDB, blocks; size/location
  identity and all-location ownership re-checked; resolved internal episode IDs
  re-verified against their AniDB identities; mapping-generation digests
  revalidated; caller-supplied safety attestation / apply gate / queue inputs
  enforced). The race against other Shoko clients remains documented and is
  not closable from the client side.
* Exactly **one delivery attempt** per plan: ``max_redirects=0`` (a redirect
  on a mutation is ambiguous — ``AMBIGUOUS_SEND``), **no retry on any failure**.
  Post-submit verification polls the XRef set for the **exact full set** of planned
  identities; ``VERIFIED`` only on the exact set; an unexpected/extra set, or
  a file left Unrecognized past the deadline, is ``CONTRADICTION`` — leaving
  Unrecognized does not prove success.

Dry-run discipline: the default orchestration path (``reconcile_link_plan``
with ``dry_run=True``) performs **only GETs** and short-circuits before the single
POST call site (:func:`submit_link`). Queue access here is read-only
(:func:`fetch_queue`); ``Queue/Pause|Resume|Clear``, ``Settings``, and ``Action``
endpoints are never called.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any, Final, cast

import httpx

from .config import Settings
from .errors import ConfigError, HttpPolicyError, PathError
from .http import (
    ApiEndpoint,
    BoundedJsonError,
    get_bounded_json,
    read_bounded_json,
    resolve_and_send,
)
from .logging_setup import redact
from .mappings import EpisodeType, MappingIndex, ResolutionStatus
from .mappings import resolve as resolve_mapping
from .paths import is_within_root, normalize_posix
from .policies import RunPolicy
from .queue_inventory import Presence, QueueJobKind, fetch_queue_inventory
from .shoko import (
    DROP_TYPE_NONE,
    FileMatch,
    ImportFolder,
    ShokoFile,
    ShokoLocation,
    canonical_ed2k,
    has_available_hashes,
    join_location_path,
    parse_file_entries,
    select_reverse_mapping,
    translate_to_sonarr,
)
from .sonarr import SonarrEpisode

MIN_SUCCESS_STATUS: Final[int] = 200
MAX_SUCCESS_STATUS: Final[int] = 300
NOT_FOUND_STATUS: Final[int] = 404
CLIENT_ERROR_MIN: Final[int] = 400
CLIENT_ERROR_MAX: Final[int] = 500
# ProblemDetails contains short validation messages, never an episode inventory.
MAX_LINK_PROBLEM_BYTES: Final[int] = 64 * 1024
PLAN_VERSION: Final[str] = "2"
FILE_INCLUDE_FLAGS: Final[str] = "XRefs,AbsolutePaths"
VERIFY_INCLUDE_FLAGS: Final[str] = "XRefs"
DEFAULT_RESOLVE_TTL_SECONDS: Final[float] = 300.0
XREF_SOURCES: Final[frozenset[str]] = frozenset({"AniDB", "User"})
EPISODE_TYPE_WIRE_NAMES: Final[frozenset[str]] = frozenset(
    {"Unknown", "Other", "Episode", "Special", "Trailer", "Credits", "Parody"}
)
_DIGEST_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{64}$")


class EpisodeResolutionStatus(Enum):
    """Outcome of the AniDB→Shoko internal-ID resolution chain."""

    RESOLVED = "resolved"
    PENDING_METADATA = "pending_metadata"
    ANIME_NOT_CACHED = "anime_not_cached"
    EPISODE_NOT_FOUND = "episode_not_found"
    LIST_TRUNCATED = "list_truncated"
    MISMATCH = "mismatch"
    TRANSPORT_FAILURE = "transport_failure"
    MALFORMED = "malformed"
    INVALID_TYPE = "invalid_type"


class PlanStatus(Enum):
    """Frozen plan linkability state."""

    LINKABLE = "linkable"
    PENDING_TARGETS = "pending_targets"


class PreflightReason(Enum):
    """Machine-readable reasons the pre-POST gate refuses the single POST."""

    BLOCKED_SAFETY = "blocked_safety"
    QUEUE_UNKNOWN = "queue_unknown"
    QUEUE_BLOCKED = "queue_blocked"
    FILE_NOT_FOUND = "file_not_found"
    TRANSPORT_FAILURE = "transport_failure"
    MALFORMED = "malformed"
    NOT_CANDIDATE = "not_candidate"
    EXISTING_LINK = "existing_link"
    IDENTITY_CHANGED = "identity_changed"
    OWNERSHIP_FAILED = "ownership_failed"
    TARGET_DRIFT = "target_drift"
    TARGET_UNAVAILABLE = "target_unavailable"
    MAPPING_CHANGED = "mapping_changed"
    CANDIDATE_SET_CHANGED = "candidate_set_changed"


# A preflight may inspect several frozen targets and collect several reasons. Deferral is safe
# only when every reason independently permits retry; in particular, a missing target response
# must never mask drift or malformed data from another target. BLOCKED_SAFETY is explicitly
# retryable because the preflight uses it only for transient authorization/gate state.
RETRYABLE_PREFLIGHT_REASONS: Final[frozenset[PreflightReason]] = frozenset(
    {
        PreflightReason.BLOCKED_SAFETY,
        PreflightReason.QUEUE_UNKNOWN,
        PreflightReason.QUEUE_BLOCKED,
        PreflightReason.TRANSPORT_FAILURE,
        PreflightReason.TARGET_UNAVAILABLE,
    }
)


def preflight_reasons_are_retryable(reasons: Sequence[PreflightReason]) -> bool:
    """Defer only a nonempty preflight verdict whose complete reason set is retryable."""
    return bool(reasons) and all(reason in RETRYABLE_PREFLIGHT_REASONS for reason in reasons)


def primary_hard_preflight_reason(
    reasons: Sequence[PreflightReason],
) -> PreflightReason | None:
    """Return the first non-retryable reason, so a preceding wait cannot hide hard evidence."""
    return next(
        (reason for reason in reasons if reason not in RETRYABLE_PREFLIGHT_REASONS),
        None,
    )


class SendOutcome(Enum):
    """Delivery state of the single guarded POST (one attempt, never retried)."""

    ACCEPTED_UNVERIFIED = "accepted_unverified"
    REJECTED = "rejected"
    AMBIGUOUS_SEND = "ambiguous_send"
    REFUSED = "refused"


class VerificationVerdict(Enum):
    """Post-submit XRef verification state."""

    PENDING = "pending"
    VERIFIED = "verified"
    PARTIAL = "partial"
    CONTRADICTION = "contradiction"
    UNKNOWN = "unknown"


class ReconcileStage(Enum):
    """Where :func:`reconcile_link_plan` stopped (the dry-run boundary)."""

    SKIPPED = "skipped"
    PENDING_TARGETS = "pending_targets"
    PREFLIGHT_BLOCKED = "preflight_blocked"
    DRY_RUN = "dry_run"
    SUBMITTED = "submitted"


# ------------------------------------------------------------------ data records


@dataclass(frozen=True)
class QueueSnapshot:
    """One read-only ``GET api/v3/Queue`` observation (no Pause/Resume/Clear ever)."""

    waiting_count: int
    blocked_count: int
    total_count: int


@dataclass(frozen=True)
class ResolvedEpisode:
    """Result of one ``(AniDB anime id, type, AniDB episode number)`` resolution chain."""

    anidb_anime_id: int
    episode_type: str
    anidb_episode_number: int
    anidb_episode_id: int | None
    shoko_episode_id: int | None
    status: EpisodeResolutionStatus
    detail: str


@dataclass(frozen=True)
class SonarrEpisodeIdentity:
    """Source Sonarr episode identity recorded in a frozen plan."""

    episode_id: int
    tvdb_episode_id: int | None
    season_number: int
    episode_number: int
    absolute_episode_number: int | None


@dataclass(frozen=True)
class MappedEpisodeTarget:
    """Verified mapping identity, independent of Shoko's current metadata cache."""

    source: SonarrEpisodeIdentity
    tvdb_series: int
    anidb_anime_id: int
    episode_type: EpisodeType
    anidb_episode_number: int
    source_rule: str

    @property
    def identity(self) -> tuple[int, EpisodeType, int]:
        return self.anidb_anime_id, self.episode_type, self.anidb_episode_number


@dataclass(frozen=True)
class MetadataNeed:
    """A typed, read-only observation of missing anime or internal episode metadata."""

    target: MappedEpisodeTarget
    missing: EpisodeResolutionStatus
    known_anidb_episode_id: int | None


@dataclass(frozen=True)
class MetadataDemand:
    """Complete eligible file demand; never a partially resolved link mutation.

    The fingerprint deliberately excludes needs and resolved IDs. A fresh pass
    rebuilds the entire LinkPlan when metadata arrives, rather than patching this record.
    """

    policy: str
    scope_fingerprint: str
    sonarr_series_id: int
    sonarr_episode_file_id: int
    sonarr_path: str
    sonarr_size: int
    shoko_file_id: int
    shoko_size: int
    shoko_ed2k: str
    shoko_paths: tuple[str, ...]
    episodes: tuple[SonarrEpisodeIdentity, ...]
    mapped_targets: tuple[MappedEpisodeTarget, ...]
    needs: tuple[MetadataNeed, ...]
    targets_digest: str
    structure_digest: str
    mapping_revision: str
    overrides_active: bool
    fingerprint: str
    # Display only: never changes equality, fingerprints, or source authorization.
    series_name: str = field(default="", compare=False, repr=False)

    @property
    def required_targets(self) -> frozenset[tuple[int, EpisodeType, int]]:
        return frozenset(target.identity for target in self.mapped_targets)

    @property
    def missing_targets(self) -> frozenset[tuple[int, EpisodeType, int]]:
        return frozenset(need.target.identity for need in self.needs)


@dataclass(frozen=True)
class LinkTarget:
    """One fully resolved link target: mapping provenance + resolved Shoko internal ID."""

    tvdb_series: int
    season_number: int
    episode_number: int
    absolute_episode_number: int | None
    anidb_anime_id: int
    anidb_episode_number: int
    episode_type: str
    source_rule: str
    anidb_episode_id: int
    shoko_episode_id: int


@dataclass(frozen=True)
class PendingTarget:
    """Mapping-resolved target whose Shoko internal ID is pending metadata (404)."""

    tvdb_series: int
    season_number: int
    episode_number: int
    absolute_episode_number: int | None
    anidb_anime_id: int
    anidb_episode_number: int
    episode_type: str
    source_rule: str
    anidb_episode_id: int


@dataclass(frozen=True)
class LinkPlan:
    """Canonical, credential-free preview of one guarded link mutation.

    No timestamps and no queue fields are stored: the fingerprint is stable across
    replanning at different times. ``fingerprint`` is derived from the canonical
    lines and is itself excluded from the hashed material.
    """

    plan_version: str
    policy: str
    sonarr_series_id: int
    sonarr_episode_file_id: int
    sonarr_path: str
    sonarr_size: int
    shoko_file_id: int
    shoko_size: int
    shoko_ed2k: str
    shoko_paths: tuple[str, ...]
    episodes: tuple[SonarrEpisodeIdentity, ...]
    targets: tuple[LinkTarget, ...]
    pending_targets: tuple[PendingTarget, ...]
    episode_ids: tuple[int, ...]
    targets_digest: str
    structure_digest: str
    mapping_revision: str
    overrides_active: bool
    status: PlanStatus
    fingerprint: str


@dataclass(frozen=True)
class PlanSkip:
    """A file that must not be linked, with a machine-readable reason."""

    reason: str
    detail: str


@dataclass(frozen=True)
class EpisodeTargetResolution:
    """Join of the M4 mapping answer and the M5 ID chain for one Sonarr episode."""

    episode: SonarrEpisode
    tvdb_series: int
    status: ResolutionStatus
    anidb_anime_id: int | None = None
    anidb_episode_number: int | None = None
    episode_type: str | None = None
    source_rule: str | None = None
    anidb_episode_id: int | None = None
    shoko_episode_id: int | None = None
    pending_metadata: bool = False
    detail: str = ""
    metadata_status: EpisodeResolutionStatus | None = None


@dataclass(frozen=True)
class PreflightVerdict:
    """Outcome of the fresh pre-POST gate; ``ok=False`` forbids the POST."""

    ok: bool
    reasons: tuple[PreflightReason, ...]
    details: tuple[str, ...]


@dataclass(frozen=True)
class SendResult:
    """Structured intent record the caller persists (M6). No credential material."""

    outcome: SendOutcome
    shoko_file_id: int
    episode_ids: tuple[int, ...]
    plan_fingerprint: str
    request_fingerprint: str
    attempted_at: float
    http_status: int | None
    retry_forbidden: bool
    detail: str


@dataclass(frozen=True)
class XrefEpisode:
    """One ``EpisodeCrossReferenceIDs`` entry from a fresh XRefs read."""

    shoko_episode_id: int | None
    anidb_episode_id: int
    source: str


@dataclass(frozen=True)
class FileXRef:
    """One ``FileCrossReference`` entry: AniDB series identity + episode cross-refs."""

    series_anidb_id: int
    episodes: tuple[XrefEpisode, ...]


@dataclass(frozen=True)
class ShokoFileXRefs:
    """Fresh ``GET File/{id}?include=XRefs`` view used for verification."""

    file_id: int
    size: int
    xrefs: tuple[FileXRef, ...]


@dataclass(frozen=True)
class ReconcileResult:
    """One orchestration pass; carries every decision record the caller must persist."""

    stage: ReconcileStage
    plan: LinkPlan | None
    skip: PlanSkip | None
    preflight: PreflightVerdict | None
    send: SendResult | None
    verification: VerificationVerdict | None


# ------------------------------------------------------------- resolution chain


@dataclass(frozen=True)
class _CacheEntry:
    value: ResolvedEpisode
    resolved_at: float


@dataclass(frozen=True)
class _GetOutcome:
    payload: Any | None
    status: int | None
    detail: str


class EpisodeIdResolver:
    """Two-step AniDB→Shoko ID chain with a caller-clock TTL cache.

    Cache keyed ``(anime_id, type, number)``; a pass-level instance is the
    throttling unit: one chain per unique key, reusing anime/type listings across
    different episode numbers for their TTL. Negative outcomes (404 pending-metadata,
    identity mismatch, transport failure) are **not** cached: the key is
    invalidated so a later pass re-reads. Chain identities are validated at both
    hops (``AnimeID``, ``EpisodeNumber``/``Type``, ``IDs.AniDB``); a mismatch
    blocks and is never guessed around.
    """

    def __init__(
        self,
        endpoint: ApiEndpoint,
        *,
        clock: Callable[[], float] = time.monotonic,
        ttl_seconds: float = DEFAULT_RESOLVE_TTL_SECONDS,
        deadline: float | None = None,
    ) -> None:
        if deadline is not None and not math.isfinite(deadline):
            raise ValueError("episode resolution deadline must be finite")
        self._endpoint = endpoint
        self._clock = clock
        self._ttl = ttl_seconds
        self._deadline = deadline
        self._cache: dict[tuple[int, str, int], _CacheEntry] = {}
        self._listings: dict[tuple[int, str], tuple[_GetOutcome, float]] = {}

    def resolve(self, anime_id: int, episode_type: str, number: int) -> ResolvedEpisode:
        """Resolve one ``(anime_id, type, number)`` triple to a Shoko internal episode ID."""
        key = (anime_id, episode_type, number)
        now = self._clock()
        cached = self._cache.get(key)
        if (
            cached is not None
            and now - cached.resolved_at < self._ttl
            and (self._deadline is None or now < self._deadline)
        ):
            return cached.value
        result = self._chain(key, now)
        if result.status is EpisodeResolutionStatus.RESOLVED:
            self._cache[key] = _CacheEntry(result, now)
        else:
            self._cache.pop(key, None)
            self._listings.pop((anime_id, episode_type), None)
        return result

    def invalidate(self, anime_id: int, episode_type: str | EpisodeType, number: int) -> None:
        """Drop one cache entry so the next resolve performs a fresh chain."""
        type_name = episode_type.value if isinstance(episode_type, EpisodeType) else episode_type
        self._cache.pop((anime_id, type_name, number), None)
        self._listings.pop((anime_id, type_name), None)

    def _chain(self, key: tuple[int, str, int], now: float) -> ResolvedEpisode:
        anime_id, type_name, number = key
        anidb_episode_id: int | None = None
        shoko_episode_id: int | None = None
        status = EpisodeResolutionStatus.INVALID_TYPE
        detail = ""
        if type_name in EPISODE_TYPE_WIRE_NAMES:
            listing_key = (anime_id, type_name)
            cached = self._listings.get(listing_key)
            if cached is not None and now - cached[1] < self._ttl:
                listing = cached[0]
            else:
                listing = self._read(
                    f"api/v3/Series/AniDB/{anime_id}/Episode",
                    {"type": type_name},
                )
                self._listings[listing_key] = (listing, now)
            anidb_episode_id, status, detail = _chain_first(listing, anime_id, type_name, number)
            if anidb_episode_id is not None:
                episode = self._read(f"api/v3/Episode/AniDB/{anidb_episode_id}/Episode")
                shoko_episode_id, status, detail = _chain_second(episode, anidb_episode_id)
                if status is EpisodeResolutionStatus.RESOLVED:
                    detail = (
                        f"AniDB {anime_id} {type_name} {number} -> AniDB episode "
                        f"{anidb_episode_id} -> Shoko episode {shoko_episode_id}"
                    )
        return ResolvedEpisode(
            anidb_anime_id=anime_id,
            episode_type=type_name,
            anidb_episode_number=number,
            anidb_episode_id=anidb_episode_id,
            shoko_episode_id=shoko_episode_id,
            status=status,
            detail=detail,
        )

    def _read(self, path: str, params: Mapping[str, Any] | None = None) -> _GetOutcome:
        if self._deadline is None:
            return _get(self._endpoint, path, params)
        remaining = self._deadline - self._clock()
        if remaining <= 0:
            return _GetOutcome(None, None, "episode resolution deadline")
        result = _get(self._endpoint, path, params, deadline=self._deadline, clock=self._clock)
        if self._clock() >= self._deadline:
            return _GetOutcome(None, None, "episode resolution deadline")
        return result


def _chain_first(
    listing: _GetOutcome, anime_id: int, type_name: str, number: int
) -> tuple[int | None, EpisodeResolutionStatus, str]:
    """Step one: ``GET Series/AniDB/{id}/Episode?type={Type}`` -> the AniDB episode ID."""
    if listing.payload is None:
        if listing.status == NOT_FOUND_STATUS:
            return (
                None,
                EpisodeResolutionStatus.ANIME_NOT_CACHED,
                f"Shoko has no AniDB series entry cached for anime {anime_id}",
            )
        return None, EpisodeResolutionStatus.TRANSPORT_FAILURE, listing.detail
    found = _match_anidb_episode(listing.payload, anime_id, type_name, number)
    if found is None:
        return None, EpisodeResolutionStatus.MALFORMED, ""
    if isinstance(found, EpisodeResolutionStatus):
        if found is EpisodeResolutionStatus.LIST_TRUNCATED:
            return (
                None,
                found,
                f"Series/AniDB/{anime_id}/Episode listing is page-truncated (Total exceeds the "
                f"returned List): absence of AniDB episode {type_name} {number} cannot be "
                "concluded; fail closed",
            )
        return None, found, ""
    return found, EpisodeResolutionStatus.RESOLVED, ""


def _chain_second(
    episode: _GetOutcome, anidb_episode_id: int
) -> tuple[int | None, EpisodeResolutionStatus, str]:
    """Step two: ``GET Episode/AniDB/{anidbEpID}/Episode`` -> the Shoko internal ID."""
    if episode.payload is None:
        if episode.status == NOT_FOUND_STATUS:
            return (
                None,
                EpisodeResolutionStatus.PENDING_METADATA,
                f"AniDB episode {anidb_episode_id} is not materialized in Shoko yet "
                "(pending-metadata; campaign-eligible later, not linkable now)",
            )
        return None, EpisodeResolutionStatus.TRANSPORT_FAILURE, episode.detail
    shoko_id, anidb_identity, problem = _parse_shoko_episode(episode.payload)
    if problem:
        return None, EpisodeResolutionStatus.MALFORMED, ""
    if anidb_identity != anidb_episode_id:
        return (
            None,
            EpisodeResolutionStatus.MISMATCH,
            f"Shoko episode identity AniDB {anidb_identity} does not match the queried "
            f"AniDB episode {anidb_episode_id} (blocked, never linked)",
        )
    assert shoko_id is not None
    return shoko_id, EpisodeResolutionStatus.RESOLVED, ""


def resolve_episode_ids(
    endpoint: ApiEndpoint,
    targets: Sequence[tuple[int, str, int]],
    *,
    clock: Callable[[], float] = time.monotonic,
    ttl_seconds: float = DEFAULT_RESOLVE_TTL_SECONDS,
) -> dict[tuple[int, str, int], ResolvedEpisode]:
    """Resolve every unique ``(anime_id, type, number)`` target once for the pass."""
    resolver = EpisodeIdResolver(endpoint, clock=clock, ttl_seconds=ttl_seconds)
    return {key: resolver.resolve(*key) for key in dict.fromkeys(targets)}


# --------------------------------------------------------------- plan freeze


def freeze_metadata_demand(
    match: FileMatch,
    resolutions: Sequence[EpisodeTargetResolution],
    *,
    scope_fingerprint: str,
    policy: RunPolicy,
    targets_digest: str,
    structure_digest: str,
    shoko_paths: Sequence[str],
    mapping_revision: str,
    overrides_active: bool,
) -> MetadataDemand | PlanSkip:
    """Freeze complete mappings and source identity without inventing episode IDs."""
    for name, digest in (
        ("scope_fingerprint", scope_fingerprint),
        ("targets_digest", targets_digest),
        ("structure_digest", structure_digest),
    ):
        _require_digest(digest, name)
    validated = _validate_resolution_set(match, resolutions)
    if isinstance(validated, PlanSkip):
        return validated
    ed2k = canonical_ed2k(match.hashes.ed2k)
    paths = tuple(sorted({normalize_posix(path) for path in shoko_paths}))
    if ed2k is None or not paths:
        return PlanSkip(
            "metadata_identity_invalid", "metadata demand requires ED2K and Shoko paths"
        )
    targets: list[MappedEpisodeTarget] = []
    needs: dict[tuple[int, EpisodeType, int], MetadataNeed] = {}
    for item in resolutions:
        target = _metadata_mapping_target(item)
        if isinstance(target, PlanSkip):
            return target
        targets.append(target)
        if item.metadata_status is not None:
            needs.setdefault(
                target.identity, MetadataNeed(target, item.metadata_status, item.anidb_episode_id)
            )
    if not needs:
        return PlanSkip("metadata_not_missing", "all required metadata is already available")
    ordered = tuple(
        sorted(
            targets,
            key=lambda target: (
                target.source.episode_id,
                target.anidb_anime_id,
                target.episode_type.value,
                target.anidb_episode_number,
                target.source_rule,
            ),
        )
    )
    episodes = tuple(dict.fromkeys(target.source for target in ordered))
    draft = MetadataDemand(
        policy=policy.value,
        scope_fingerprint=scope_fingerprint,
        sonarr_series_id=match.sonarr_file.series_id,
        sonarr_episode_file_id=match.sonarr_file.episode_file_id,
        sonarr_path=normalize_posix(match.sonarr_file.path),
        sonarr_size=match.sonarr_file.size,
        shoko_file_id=match.shoko_file_id,
        shoko_size=match.sonarr_file.size,
        shoko_ed2k=ed2k,
        shoko_paths=paths,
        episodes=episodes,
        mapped_targets=ordered,
        needs=tuple(
            needs[key] for key in sorted(needs, key=lambda key: (key[0], key[1].value, key[2]))
        ),
        targets_digest=targets_digest,
        structure_digest=structure_digest,
        mapping_revision=mapping_revision,
        overrides_active=overrides_active,
        fingerprint="",
    )
    return replace(draft, fingerprint=metadata_demand_fingerprint(draft))


def _metadata_mapping_target(item: EpisodeTargetResolution) -> MappedEpisodeTarget | PlanSkip:
    if (
        item.status is not ResolutionStatus.RESOLVED
        or type(item.anidb_anime_id) is not int
        or item.anidb_anime_id <= 0
        or type(item.anidb_episode_number) is not int
        or item.anidb_episode_number <= 0
        or not item.source_rule
        or item.episode_type not in {kind.value for kind in EpisodeType}
    ):
        return PlanSkip("target_provenance_invalid", "metadata demand requires complete mappings")
    missing = item.metadata_status
    if missing is EpisodeResolutionStatus.ANIME_NOT_CACHED:
        valid = item.anidb_episode_id is None and item.shoko_episode_id is None
    elif missing is EpisodeResolutionStatus.PENDING_METADATA:
        valid = (
            type(item.anidb_episode_id) is int
            and item.anidb_episode_id > 0
            and item.shoko_episode_id is None
        )
    elif missing is None:
        valid = (
            type(item.anidb_episode_id) is int
            and item.anidb_episode_id > 0
            and type(item.shoko_episode_id) is int
            and item.shoko_episode_id > 0
        )
    else:
        valid = False
    if not valid:
        return PlanSkip(
            "target_resolution_invalid", "missing or contradictory metadata observation"
        )
    episode = item.episode
    return MappedEpisodeTarget(
        SonarrEpisodeIdentity(
            episode.id,
            episode.tvdb_id,
            episode.season_number,
            episode.episode_number,
            episode.absolute_episode_number,
        ),
        item.tvdb_series,
        item.anidb_anime_id,
        EpisodeType(item.episode_type),
        item.anidb_episode_number,
        item.source_rule,
    )


def metadata_demand_fingerprint(demand: MetadataDemand) -> str:
    """Canonical whole-file identity, stable as a Shoko metadata queue progresses."""
    payload = (
        "metadata_demand_v1",
        demand.policy,
        demand.scope_fingerprint,
        demand.sonarr_series_id,
        demand.sonarr_episode_file_id,
        demand.sonarr_path,
        demand.sonarr_size,
        demand.shoko_file_id,
        demand.shoko_size,
        demand.shoko_ed2k,
        demand.shoko_paths,
        tuple(
            (
                target.source.episode_id,
                target.source.tvdb_episode_id,
                target.source.season_number,
                target.source.episode_number,
                target.source.absolute_episode_number,
                target.tvdb_series,
                target.anidb_anime_id,
                target.episode_type.value,
                target.anidb_episode_number,
                target.source_rule,
            )
            for target in demand.mapped_targets
        ),
        demand.targets_digest,
        demand.structure_digest,
        demand.mapping_revision,
        demand.overrides_active,
    )
    return hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode("utf-8")).hexdigest()


def freeze_plan(
    match: FileMatch,
    resolutions: Sequence[EpisodeTargetResolution],
    *,
    policy: RunPolicy,
    targets_digest: str,
    structure_digest: str,
    shoko_paths: Sequence[str],
    mapping_revision: str = "",
    overrides_active: bool = False,
) -> LinkPlan | PlanSkip:
    """Freeze the canonical link plan for one matched file, applying the complete-target rule.

    ``shoko_paths`` are the **Shoko-side canonical absolute location paths** (the
    ``Locations[].AbsolutePath`` values as Shoko reports them, normalized), NOT
    ``FileMatch.shoko_paths`` (which is the Sonarr-translated set). Preflight
    compares the fresh Shoko read against these paths; passing the translated side
    fails every apply closed.
    **Every** Sonarr episode of the file must carry a ``RESOLVED`` mapping answer;
    any ``AMBIGUOUS``/``UNMAPPED``/``NO_EQUIVALENT``/``CONFLICT`` skips the whole
    file with a reason. ``PENDING_METADATA`` targets mark the plan
    ``PENDING_TARGETS`` (campaign-eligible later, not linkable now). The deduped
    sorted Shoko internal IDs are the exact ``POST .../Link`` body.
    """
    _require_digest(targets_digest, "targets_digest")
    _require_digest(structure_digest, "structure_digest")
    if canonical_ed2k(match.hashes.ed2k) is None:
        return PlanSkip(
            "invalid_ed2k",
            f"Shoko file {match.shoko_file_id} has missing or malformed raw ED2K; "
            "a file-content identity is required before freezing a link plan",
        )
    validated = _validate_resolution_set(match, resolutions)
    if isinstance(validated, PlanSkip):
        return validated
    _, file_episodes = validated
    built = _build_targets(resolutions)
    if isinstance(built, PlanSkip):
        return built
    targets, pending = built
    shoko = tuple(sorted({normalize_posix(path) for path in shoko_paths}))
    if not shoko:
        return PlanSkip("no_shoko_paths", "the plan carries no canonical Shoko location paths")
    episodes = tuple(
        SonarrEpisodeIdentity(
            episode_id=episode.id,
            tvdb_episode_id=episode.tvdb_id,
            season_number=episode.season_number,
            episode_number=episode.episode_number,
            absolute_episode_number=episode.absolute_episode_number,
        )
        for episode in file_episodes
    )
    status = PlanStatus.PENDING_TARGETS if pending else PlanStatus.LINKABLE
    episode_ids = () if pending else tuple(sorted({target.shoko_episode_id for target in targets}))
    draft = LinkPlan(
        plan_version=PLAN_VERSION,
        policy=policy.value,
        sonarr_series_id=match.sonarr_file.series_id,
        sonarr_episode_file_id=match.sonarr_file.episode_file_id,
        sonarr_path=match.sonarr_file.path,
        sonarr_size=match.sonarr_file.size,
        shoko_file_id=match.shoko_file_id,
        shoko_size=match.sonarr_file.size,
        shoko_ed2k=cast(str, match.hashes.ed2k),
        shoko_paths=shoko,
        episodes=episodes,
        targets=tuple(sorted(targets, key=_target_key)),
        pending_targets=tuple(sorted(pending, key=_pending_key)),
        episode_ids=episode_ids,
        targets_digest=targets_digest,
        structure_digest=structure_digest,
        mapping_revision=mapping_revision,
        overrides_active=overrides_active,
        status=status,
        fingerprint="",
    )
    return replace(draft, fingerprint=plan_fingerprint(draft))


def plan_fingerprint(plan: LinkPlan) -> str:
    """SHA-256 over the plan's canonical sorted lines (no timestamps, no credentials)."""
    lines = [
        f"v|{plan.plan_version}",
        f"p|{plan.policy}",
        f"sn|{plan.sonarr_series_id}|{plan.sonarr_episode_file_id}|{plan.sonarr_path}"
        f"|{plan.sonarr_size}",
        f"sh|{plan.shoko_file_id}|{plan.shoko_size}|{plan.shoko_ed2k.lower()}",
        *(f"shp|{path}" for path in plan.shoko_paths),
        *(
            f"ep|{item.episode_id}|{_fmt(item.tvdb_episode_id)}|{item.season_number}"
            f"|{item.episode_number}|{_fmt(item.absolute_episode_number)}"
            for item in plan.episodes
        ),
        *(
            f"tg|{target.tvdb_series}|{target.season_number}|{target.episode_number}"
            f"|{_fmt(target.absolute_episode_number)}|{target.anidb_anime_id}"
            f"|{target.anidb_episode_number}|{target.episode_type}|{target.source_rule}"
            for target in plan.targets
        ),
        *(
            f"pt|{item.tvdb_series}|{item.season_number}|{item.episode_number}"
            f"|{_fmt(item.absolute_episode_number)}|{item.anidb_anime_id}"
            f"|{item.anidb_episode_number}|{item.episode_type}|{item.source_rule}"
            f"|{item.anidb_episode_id}"
            for item in plan.pending_targets
        ),
        *(
            f"id|{target.anidb_anime_id}|{target.anidb_episode_number}|{target.episode_type}"
            f"|{target.anidb_episode_id}|{target.shoko_episode_id}"
            for target in plan.targets
        ),
        f"d|targets|{plan.targets_digest}",
        f"d|structure|{plan.structure_digest}",
        f"r|{plan.mapping_revision}",
        f"o|{int(plan.overrides_active)}",
        *(f"i|{episode_id}" for episode_id in plan.episode_ids),
        f"st|{plan.status.value}",
    ]
    canonical = "\n".join(sorted(lines))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def request_fingerprint(plan: LinkPlan) -> str:
    """SHA-256 of the exact mutation delivery: method, file id, sorted EpisodeIDs."""
    lines = [
        "method|POST",
        f"path|api/v3/File/{plan.shoko_file_id}/Link",
        f"body|{plan.episode_ids}",
        f"plan|{plan.fingerprint}",
    ]
    canonical = "\n".join(sorted(lines))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _validate_resolution_set(
    match: FileMatch,
    resolutions: Sequence[EpisodeTargetResolution],
) -> PlanSkip | tuple[dict[int, EpisodeTargetResolution], tuple[SonarrEpisode, ...]]:
    """Check the resolution set covers exactly the matched file's episode group."""
    file_episodes = tuple(sorted(match.sonarr_file.episodes, key=lambda episode: episode.id))
    problems: list[tuple[str, str]] = []
    by_episode: dict[int, EpisodeTargetResolution] = {}
    if not file_episodes:
        problems.append(("no_episodes", "the matched Sonarr file has no episode group"))
    for item in resolutions:
        if item.episode.id in by_episode:
            prior = [entry for entry in resolutions if entry.episode.id == item.episode.id]
            identities = {
                (entry.anidb_anime_id, entry.episode_type, entry.anidb_episode_number)
                for entry in prior
            }
            if len(identities) != len(prior):
                problems.append(
                    ("duplicate_resolution", f"episode {item.episode.id} target resolved twice")
                )
        by_episode[item.episode.id] = item
    missing = [episode.id for episode in file_episodes if episode.id not in by_episode]
    if missing:
        problems.append(
            ("incomplete_resolution_set", f"episodes {missing} have no mapping resolution record")
        )
    foreign = sorted(
        episode_id
        for episode_id, item in by_episode.items()
        if not any(episode.id == episode_id for episode in file_episodes)
    )
    if foreign:
        problems.append(
            (
                "foreign_resolution",
                f"resolutions reference episodes {foreign} outside the matched file's "
                "episode group",
            )
        )
    series_ids = {item.tvdb_series for item in by_episode.values()}
    if len(series_ids) != 1:
        problems.append(("mixed_tvdb_series", f"resolutions span TVDB series {sorted(series_ids)}"))
    if problems:
        reason, detail = problems[0]
        return PlanSkip(reason, detail)
    tvdb_series = next(iter(series_ids))
    if tvdb_series <= 0:
        return PlanSkip("invalid_tvdb_series", f"TVDB series id {tvdb_series} is not positive")
    return by_episode, file_episodes


def _build_targets(
    resolutions: Sequence[EpisodeTargetResolution],
) -> PlanSkip | tuple[list[LinkTarget], list[PendingTarget]]:
    """Apply the complete-target rule per episode (PROJ: never partial links)."""
    targets: list[LinkTarget] = []
    pending: list[PendingTarget] = []
    for item in sorted(resolutions, key=lambda item: item.episode.id):
        episode_id = item.episode.id
        if item.status is not ResolutionStatus.RESOLVED:
            return PlanSkip(
                f"target_{item.status.value}",
                f"episode {episode_id} (S{item.episode.season_number}"
                f"E{item.episode.episode_number}): {item.detail or item.status.value}",
            )
        provenance = _target_provenance(item)
        if isinstance(provenance, str):
            return PlanSkip("target_provenance_invalid", f"episode {episode_id}: {provenance}")
        if item.pending_metadata:
            pending.append(
                PendingTarget(
                    tvdb_series=provenance.tvdb_series,
                    season_number=provenance.season_number,
                    episode_number=provenance.episode_number,
                    absolute_episode_number=provenance.absolute_episode_number,
                    anidb_anime_id=provenance.anidb_anime_id,
                    anidb_episode_number=provenance.anidb_episode_number,
                    episode_type=provenance.episode_type,
                    source_rule=provenance.source_rule,
                    anidb_episode_id=provenance.anidb_episode_id,
                )
            )
            continue
        shoko_id = item.shoko_episode_id
        if shoko_id is None or shoko_id <= 0:
            return PlanSkip(
                "target_unresolved_id",
                f"episode {episode_id} resolved to AniDB {item.anidb_anime_id} "
                f"episode {item.anidb_episode_number} with no Shoko internal episode ID",
            )
        targets.append(
            LinkTarget(
                tvdb_series=provenance.tvdb_series,
                season_number=provenance.season_number,
                episode_number=provenance.episode_number,
                absolute_episode_number=provenance.absolute_episode_number,
                anidb_anime_id=provenance.anidb_anime_id,
                anidb_episode_number=provenance.anidb_episode_number,
                episode_type=provenance.episode_type,
                source_rule=provenance.source_rule,
                anidb_episode_id=provenance.anidb_episode_id,
                shoko_episode_id=shoko_id,
            )
        )
    return targets, pending


# --------------------------------------------------------------------- preflight


def fetch_queue(
    endpoint: ApiEndpoint,
    *,
    deadline: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> QueueSnapshot | None:
    """Read-only queue occupancy (`GET api/v3/Queue`); None on any failure (fail closed)."""
    outcome = _get(endpoint, "api/v3/Queue", deadline=deadline, clock=clock)
    if outcome.payload is None:
        return None
    entry = _as_mapping(outcome.payload)
    if entry is None:
        return None
    counts: list[int] = []
    for key in ("WaitingCount", "BlockedCount", "TotalCount"):
        value = entry.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            return None
        counts.append(value)
    return QueueSnapshot(counts[0], counts[1], counts[2])


def _authorization_refusals(
    *,
    service_mode: bool,
    service_authorized: bool | None,
    attestation_valid: bool | None,
    apply_gate_open: bool | None,
) -> tuple[tuple[PreflightReason, str], ...]:
    """Return only the explicit authorization lane's missing safety predicates."""
    if service_mode:
        if service_authorized is not True:
            return (
                (
                    PreflightReason.BLOCKED_SAFETY,
                    "current automatic-link onboarding authorization is missing or invalid",
                ),
            )
        return ()

    refusals: list[tuple[PreflightReason, str]] = []
    if attestation_valid is not True:
        refusals.append(
            (
                PreflightReason.BLOCKED_SAFETY,
                "safety attestation is missing/invalid: the gate stays closed",
            )
        )
    if apply_gate_open is not True:
        refusals.append(
            (
                PreflightReason.BLOCKED_SAFETY,
                "the apply gate is closed (sync.apply_changes and the operator opt-in are both "
                "required; dry-run never reaches this point in apply mode)",
            )
        )
    return tuple(refusals)


def preflight(
    endpoint: ApiEndpoint,
    plan: LinkPlan,
    settings: Settings,
    folders: Mapping[int, ImportFolder],
    *,
    attestation_valid: bool | None,
    apply_gate_open: bool | None,
    queue: QueueSnapshot | None,
    active_targets_digest: str,
    active_structure_digest: str,
    blocked_over: int | None = None,
    mapping_index: MappingIndex | None = None,
    service_mode: bool = False,
    service_authorized: bool | None = None,
    check_file: bool = True,
    check_targets: bool = True,
) -> PreflightVerdict:
    """The fresh gate immediately before the single POST; every check fails closed.

    Order: authorization inputs first (legacy attestation/gate or service onboarding sends
    **zero** requests when blocked),
    then the queue anomaly input, then the **pure** generation checks (mapping digests and
    active-index candidate set), and only then fresh target network reads (one per unique AniDB
    episode ID). The protected-link ``File/{id}?include=XRefs,AbsolutePaths`` read is last. Send
    callers with additional source/authorization network checks pass ``check_file=False``
    and ``check_targets=False``. After those checks they call
    :func:`preflight_link_jobs_and_targets`, then :func:`preflight_file`; the protected
    file read must be the final network guard before the durable send intent.
    """
    reasons: list[PreflightReason] = []
    details: list[str] = []

    def add(reason: PreflightReason, detail: str) -> None:
        if reason not in reasons:
            reasons.append(reason)
        details.append(detail)

    for reason, detail in _authorization_refusals(
        service_mode=service_mode,
        service_authorized=service_authorized,
        attestation_valid=attestation_valid,
        apply_gate_open=apply_gate_open,
    ):
        add(reason, detail)
    if not reasons:
        if queue is None:
            add(
                PreflightReason.QUEUE_UNKNOWN,
                "no queue snapshot supplied: occupancy cannot be assessed (fail closed)",
            )
        elif blocked_over is not None and queue.blocked_count > blocked_over:
            add(
                PreflightReason.QUEUE_BLOCKED,
                f"queue BlockedCount {queue.blocked_count} exceeds the threshold {blocked_over} "
                "(ManualLinkJob delivery cannot be trusted on a blocked queue)",
            )
    if not reasons and plan.status is not PlanStatus.LINKABLE:
        add(
            PreflightReason.MALFORMED,
            f"plan status {plan.status.value} is not linkable: preflight refuses",
        )
    if not reasons and (
        active_targets_digest != plan.targets_digest
        or active_structure_digest != plan.structure_digest
    ):
        add(
            PreflightReason.MAPPING_CHANGED,
            "the active mapping generation no longer matches the plan's frozen "
            "targets/structure digests: replan before any POST",
        )
    if not reasons and mapping_index is not None:
        _preflight_candidates(plan, mapping_index, add)
    if not reasons:
        if check_targets:
            _preflight_targets(endpoint, plan, add)
        if not reasons and check_file:
            _preflight_file(endpoint, plan, settings, folders, add)
    return PreflightVerdict(not reasons, tuple(reasons), tuple(details))


def preflight_link_jobs_and_targets(
    endpoint: ApiEndpoint,
    plan: LinkPlan,
    *,
    deadline: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> PreflightVerdict:
    """Fresh job absence, THEN target identities; caller reads protected file last.

    Never reuse an absence observation as authority for another submission. An
    external refresh may recreate episode IDs, so reads made before this queue
    observation cannot authorize delivery. Foreign schedulers can still race us.
    """
    inventory = fetch_queue_inventory(
        endpoint, deadline=deadline if deadline is not None else clock() + 30.0, clock=clock
    )
    subjects = [(QueueJobKind.MANUAL_LINK, plan.shoko_file_id)] + [
        (QueueJobKind.ANIME_METADATA, anime_id)
        for anime_id in sorted({target.anidb_anime_id for target in plan.targets})
    ]
    details: list[str] = []
    reasons: list[PreflightReason] = []
    for kind, subject_id in subjects:
        observed = inventory.lookup(kind, subject_id)
        if observed.presence is Presence.ABSENT:
            continue
        reason = (
            PreflightReason.QUEUE_BLOCKED
            if observed.presence is Presence.PRESENT
            else PreflightReason.QUEUE_UNKNOWN
        )
        if reason not in reasons:
            reasons.append(reason)
        details.append(
            f"{kind.value} {subject_id}: {observed.presence.value} "
            f"({observed.reason.value}); waiting for fresh queue absence"
        )
    if not reasons:

        def add(reason: PreflightReason, detail: str) -> None:
            if reason not in reasons:
                reasons.append(reason)
            details.append(detail)

        _preflight_targets(endpoint, plan, add, deadline=deadline, clock=clock)
    return PreflightVerdict(not reasons, tuple(reasons), tuple(details))


def preflight_file(
    endpoint: ApiEndpoint,
    plan: LinkPlan | MetadataDemand,
    settings: Settings,
    folders: Mapping[int, ImportFolder],
    *,
    deadline: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> PreflightVerdict:
    """Perform the final fresh protected-link/file identity read, with no later network checks."""
    reasons: list[PreflightReason] = []
    details: list[str] = []

    def add(reason: PreflightReason, detail: str) -> None:
        if reason not in reasons:
            reasons.append(reason)
        details.append(detail)

    _preflight_file(endpoint, plan, settings, folders, add, deadline=deadline, clock=clock)
    return PreflightVerdict(not reasons, tuple(reasons), tuple(details))


type AddReason = Callable[[PreflightReason, str], None]


def _preflight_file(
    endpoint: ApiEndpoint,
    plan: LinkPlan | MetadataDemand,
    settings: Settings,
    folders: Mapping[int, ImportFolder],
    add: AddReason,
    *,
    deadline: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """Fresh file read: existence, zero XRefs, unchanged identity, all-location ownership."""
    outcome = _get(
        endpoint,
        f"api/v3/File/{plan.shoko_file_id}",
        {"include": FILE_INCLUDE_FLAGS},
        deadline=deadline,
        clock=clock,
    )
    if outcome.payload is None:
        if outcome.status == NOT_FOUND_STATUS:
            add(
                PreflightReason.FILE_NOT_FOUND,
                f"file {plan.shoko_file_id} no longer exists: the plan is stale, no POST",
            )
        else:
            add(PreflightReason.TRANSPORT_FAILURE, outcome.detail)
        return
    files, parse_reasons, parse_details, absolute_missing, xrefs_missing = parse_file_entries(
        [outcome.payload], 1
    )
    if parse_reasons or not files:
        add(
            PreflightReason.MALFORMED,
            f"fresh file read malformed: {'; '.join(parse_details) or 'no parsed file'}",
        )
        return
    item = files[0]
    if item.id != plan.shoko_file_id:
        add(
            PreflightReason.MALFORMED,
            f"the fresh read echoes file {item.id} for a request naming {plan.shoko_file_id}: "
            "untrustworthy response, no POST",
        )
        return
    disqualifier: str | None = None
    if item.is_ignored or item.is_variation:
        disqualifier = (
            f"(IsIgnored={item.is_ignored}, IsVariation={item.is_variation}): "
            "the M3 candidacy gate must pass on the fresh read too — never a link target"
        )
    elif not has_available_hashes(item):
        disqualifier = (
            "with no settled hash identity: identity incomplete (hashing not settled), "
            "never a link target"
        )
    if disqualifier is not None:
        add(
            PreflightReason.NOT_CANDIDATE,
            f"file {plan.shoko_file_id} is no longer a qualifying candidate {disqualifier}",
        )
        return
    if absolute_missing or xrefs_missing:
        add(
            PreflightReason.MALFORMED,
            "fresh file read ignored include=XRefs/AbsolutePaths: identity cannot be verified",
        )
        return
    if item.xref_count > 0:
        add(
            PreflightReason.EXISTING_LINK,
            f"file {plan.shoko_file_id} carries {item.xref_count} cross-reference(s) "
            "(User or AniDB source): the link endpoint replaces User links and refuses "
            "AniDB links — the file is protected, never linked",
        )
        return
    if item.size != plan.sonarr_size:
        add(
            PreflightReason.IDENTITY_CHANGED, f"file size changed {plan.sonarr_size} -> {item.size}"
        )
    _preflight_ed2k(item, plan, add)
    fresh_paths = tuple(sorted({_safe_path(loc.absolute_path) for loc in item.locations}))
    if fresh_paths != plan.shoko_paths:
        add(
            PreflightReason.IDENTITY_CHANGED,
            f"Shoko locations changed {list(plan.shoko_paths)} -> {list(fresh_paths)} "
            "(added/removed/moved locations)",
        )
    _preflight_ownership(item, plan, settings, folders, add)


def _preflight_ed2k(item: ShokoFile, plan: LinkPlan | MetadataDemand, add: AddReason) -> None:
    """Refuse fresh content identity drift, comparing ED2K only (optional hashes are metadata)."""
    if canonical_ed2k(item.hashes.ed2k) != plan.shoko_ed2k.lower():
        add(
            PreflightReason.IDENTITY_CHANGED,
            f"Shoko ED2K changed {plan.shoko_ed2k!r} -> {item.hashes.ed2k!r}; "
            "file-content identity drift, no POST",
        )


def _preflight_ownership(
    item: ShokoFile,
    plan: LinkPlan | MetadataDemand,
    settings: Settings,
    folders: Mapping[int, ImportFolder],
    add: AddReason,
) -> None:
    """Every location must pass ownership and translate to the plan's single Sonarr path."""
    failures: list[str] = []
    for location in sorted(
        item.locations, key=lambda loc: (loc.import_folder_id, loc.relative_path, loc.place_id)
    ):
        translated, failure = _check_location(location, folders, settings)
        if failure is not None:
            failures.append(failure)
        elif translated != plan.sonarr_path:
            failures.append(
                f"location {location.place_id} translates to {translated!r}, not the planned "
                f"Sonarr identity {plan.sonarr_path!r}"
            )
    if failures:
        add(
            PreflightReason.OWNERSHIP_FAILED,
            f"all-location ownership failed for file {plan.shoko_file_id}: " + "; ".join(failures),
        )


def _check_location(
    location: ShokoLocation,
    folders: Mapping[int, ImportFolder],
    settings: Settings,
) -> tuple[str | None, str | None]:
    """Minimal re-implementation of the M3 all-location ownership check via public helpers."""
    folder, failure = _location_access(location, folders)
    if failure is not None:
        return None, failure
    assert folder is not None
    normalized, failure = _location_identity(location, folder, settings)
    if failure is not None:
        return None, failure
    assert normalized is not None
    return _location_mapping_check(normalized, settings)


def _location_access(
    location: ShokoLocation,
    folders: Mapping[int, ImportFolder],
) -> tuple[ImportFolder | None, str | None]:
    if not location.is_accessible:
        return None, f"location {location.place_id} is not accessible"
    folder = folders.get(location.import_folder_id)
    if folder is None:
        return None, (
            f"location {location.place_id} references unknown ImportFolderID "
            f"{location.import_folder_id}"
        )
    if not folder.path_usable:
        return None, f"import folder {folder.id} has an unusable path {folder.path!r}"
    if folder.drop_folder_type != DROP_TYPE_NONE:
        return None, (
            f"import folder {folder.id} ({folder.name!r}) is a drop folder "
            f"(DropFolderType={folder.drop_folder_type!r})"
        )
    return folder, None


def _location_identity(
    location: ShokoLocation,
    folder: ImportFolder,
    settings: Settings,
) -> tuple[str | None, str | None]:
    try:
        normalized = normalize_posix(location.absolute_path)
    except PathError as exc:
        return None, f"location {location.place_id}: {exc}"
    try:
        joined = join_location_path(folder.path, location.relative_path)
    except PathError as exc:
        return None, f"location {location.place_id}: {exc}"
    if joined != normalized:
        return None, (
            f"location {location.place_id}: {folder.path!r} + {location.relative_path!r} "
            f"= {joined!r} does not match AbsolutePath {normalized!r}"
        )
    if not any(is_within_root(normalized, root) for root in settings.shoko.allowed_roots):
        return None, (
            f"location {normalized!r} is outside every configured shoko.allowed_roots entry"
        )
    return normalized, None


def _location_mapping_check(normalized: str, settings: Settings) -> tuple[str | None, str | None]:
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


def _preflight_targets(  # noqa: PLR0912 - verify the full identity chain before any write
    endpoint: ApiEndpoint,
    plan: LinkPlan,
    add: AddReason,
    *,
    deadline: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """Freshly revalidate mapped AniDB identities and their Shoko internal IDs."""
    source_targets: dict[tuple[int, str], dict[int, set[int]]] = {}
    source_failed = False
    for target in plan.targets:
        anime_id = _positive_int(target.anidb_anime_id)
        episode_number = _positive_int(target.anidb_episode_number)
        anidb_episode_id = _positive_int(target.anidb_episode_id)
        if (
            anime_id is None
            or episode_number is None
            or anidb_episode_id is None
            or not isinstance(target.episode_type, str)
            or target.episode_type not in EPISODE_TYPE_WIRE_NAMES
        ):
            source_failed = True
            add(
                PreflightReason.TARGET_DRIFT,
                "the frozen AniDB source identity is malformed; no POST",
            )
            continue
        source_targets.setdefault((anime_id, target.episode_type), {}).setdefault(
            episode_number, set()
        ).add(anidb_episode_id)

    for (anime_id, type_name), episodes in sorted(source_targets.items()):
        outcome = _get(
            endpoint,
            f"api/v3/Series/AniDB/{anime_id}/Episode",
            {"type": type_name},
            deadline=deadline,
            clock=clock,
        )
        if outcome.payload is None:
            source_failed = True
            reason, detail = _target_read_unavailable(
                outcome,
                f"Shoko AniDB episode listing for anime {anime_id} type {type_name}",
                missing_detail="is not available in Shoko yet",
            )
            add(reason, detail)
            continue
        for number, expected_ids in sorted(episodes.items()):
            if len(expected_ids) != 1:
                source_failed = True
                add(
                    PreflightReason.TARGET_DRIFT,
                    f"the frozen AniDB identity {anime_id}/{type_name}/{number} has conflicting "
                    "episode IDs; no POST",
                )
                continue
            expected_id = next(iter(expected_ids))
            found = _match_anidb_episode(outcome.payload, anime_id, type_name, number)
            if not isinstance(found, int) or isinstance(found, bool) or found != expected_id:
                source_failed = True
                add(
                    PreflightReason.TARGET_DRIFT,
                    f"Shoko AniDB identity {anime_id}/{type_name}/{number} no longer resolves "
                    f"to episode {expected_id}; no POST",
                )
    if source_failed:
        return

    for target_anidb_id, expected_pairs in _group_expected_targets(plan):
        outcome = _get(
            endpoint,
            f"api/v3/Episode/AniDB/{target_anidb_id}/Episode",
            deadline=deadline,
            clock=clock,
        )
        if outcome.payload is None:
            reason, detail = _target_read_unavailable(
                outcome,
                f"Shoko episode metadata for AniDB episode {target_anidb_id}",
                missing_detail="is not materialized in Shoko yet",
            )
            add(reason, detail)
            continue
        shoko_id, anidb_identity, problem = _parse_shoko_episode(outcome.payload)
        if problem:
            add(
                PreflightReason.TARGET_DRIFT,
                f"Shoko episode re-read malformed for AniDB {target_anidb_id}",
            )
        elif anidb_identity != target_anidb_id:
            add(
                PreflightReason.TARGET_DRIFT,
                f"Shoko episode AniDB identity changed to {anidb_identity} (expected "
                f"{target_anidb_id})",
            )
        elif shoko_id not in expected_pairs:
            add(
                PreflightReason.TARGET_DRIFT,
                f"Shoko internal ID {shoko_id} does not match the planned "
                f"{sorted(expected_pairs)} for AniDB episode {target_anidb_id}",
            )


def _target_read_unavailable(
    outcome: _GetOutcome,
    description: str,
    *,
    missing_detail: str,
) -> tuple[PreflightReason, str]:
    """Classify a failed fresh target read without claiming the frozen identity changed."""
    if outcome.status == NOT_FOUND_STATUS:
        detail = (
            f"{description} {missing_detail} (HTTP 404); target identity cannot be confirmed, "
            "retry after metadata is available; no POST"
        )
    elif outcome.status is not None and MIN_SUCCESS_STATUS <= outcome.status < MAX_SUCCESS_STATUS:
        detail = (
            f"{description} returned a malformed response (HTTP {outcome.status}); target "
            "identity cannot be confirmed, operator review required; no POST"
        )
        return PreflightReason.MALFORMED, detail
    elif outcome.status is not None:
        detail = (
            f"{description} could not be re-read (HTTP {outcome.status}); target identity cannot "
            "be confirmed, retry later; no POST"
        )
    else:
        detail = (
            f"{description} could not be re-read because Shoko was temporarily unreachable; "
            "target identity cannot be confirmed, retry later; no POST"
        )
    return PreflightReason.TARGET_UNAVAILABLE, detail


def _group_expected_targets(plan: LinkPlan) -> list[tuple[int, set[int]]]:
    """One re-read per unique AniDB episode id (dedup for multi-episode files)."""
    grouped: dict[int, set[int]] = {}
    for target in plan.targets:
        grouped.setdefault(target.anidb_episode_id, set()).add(target.shoko_episode_id)
    return sorted(grouped.items())


def _preflight_candidates(
    plan: LinkPlan,
    mapping_index: MappingIndex,
    add: AddReason,
) -> None:
    """The active mapping index must answer each planned source episode identically."""
    grouped: dict[tuple[int, int, int, int | None], set[tuple[int, int, str, str]]] = {}
    for target in plan.targets:
        key = (
            target.tvdb_series,
            target.season_number,
            target.episode_number,
            target.absolute_episode_number,
        )
        grouped.setdefault(key, set()).add(
            (
                target.anidb_anime_id,
                target.anidb_episode_number,
                target.episode_type,
                target.source_rule,
            )
        )
    for key, expected in grouped.items():
        resolution = resolve_mapping(
            mapping_index,
            *key,
        )
        match = (
            resolution.status is ResolutionStatus.RESOLVED
            and {
                (
                    item.anidb_id,
                    item.anidb_episode_number,
                    item.episode_type.value,
                    item.source_rule.value,
                )
                for item in resolution.targets
            }
            == expected
        )
        if not match:
            add(
                PreflightReason.CANDIDATE_SET_CHANGED,
                f"active mapping answers TVDB {key[0]} S{key[1]}"
                f"E{key[2]} as {resolution.status.value} "
                f"({resolution.detail}), not the planned target {expected} "
                "(replan before any POST)",
            )


# ------------------------------------------------------------------- single POST


def submit_link(
    endpoint: ApiEndpoint,
    plan: LinkPlan,
    *,
    clock: Callable[[], float] = time.monotonic,
    deadline: float | None = None,
) -> SendResult:
    """The single, isolated POST call site: exactly one delivery attempt, never retried.

    ``max_redirects=0`` — a redirect on a mutation is ambiguous, so it is recorded as
    ``AMBIGUOUS_SEND`` with ``retry_forbidden=True`` and an inspection instruction;
    timeouts, connection failures, and 5xx are recorded the same way. 2xx =
    ``ACCEPTED_UNVERIFIED`` (the endpoint enqueues async jobs and returns before they
    run); bounded, valid 4xx ProblemDetails = ``REJECTED`` (server-side validation
    refuses with zero writes per the pinned endpoint docs). Unusable refusal bodies
    remain ambiguous. Every field needed for M6 intent persistence is exposed;
    this function performs no disk I/O.
    """
    attempted_at = clock()
    outcome = SendOutcome.REFUSED
    http_status: int | None = None
    detail = f"plan status {plan.status.value} is not linkable: the POST is never sent"
    if plan.status is PlanStatus.LINKABLE:
        if not plan.episode_ids:
            detail = "empty EpisodeIDs are never POSTed"
        else:
            outcome, http_status, detail = _deliver(endpoint, plan, deadline=deadline, clock=clock)
    return SendResult(
        outcome=outcome,
        shoko_file_id=plan.shoko_file_id,
        episode_ids=plan.episode_ids,
        plan_fingerprint=plan.fingerprint,
        request_fingerprint=request_fingerprint(plan),
        attempted_at=attempted_at,
        http_status=http_status,
        retry_forbidden=True,
        detail=redact(detail),
    )


def _deliver(
    endpoint: ApiEndpoint,
    plan: LinkPlan,
    *,
    deadline: float | None,
    clock: Callable[[], float],
) -> tuple[SendOutcome, int | None, str]:
    """Exactly one delivery attempt; redirects/timeouts/5xx are ambiguous, never retried."""
    path = f"api/v3/File/{plan.shoko_file_id}/Link"
    body = {"EpisodeIDs": list(plan.episode_ids)}
    inspect = (
        "delivery state is ambiguous: re-read GET File/"
        f"{plan.shoko_file_id}?include=XRefs (verify_link) and inspect Shoko's state "
        "before considering anything further; automatic retries are forbidden"
    )
    try:
        response = resolve_and_send(
            endpoint,
            "POST",
            path,
            json=body,
            max_redirects=0,
            stream=True,
            headers={"Accept-Encoding": "identity"},
            deadline=deadline,
            clock=clock,
        )
    except HttpPolicyError as exc:
        return (
            SendOutcome.AMBIGUOUS_SEND,
            None,
            f"redirect policy stopped the POST: {type(exc).__name__}. {inspect}",
        )
    except httpx.HTTPError as exc:
        return (
            SendOutcome.AMBIGUOUS_SEND,
            None,
            f"transport failed mid-delivery: {type(exc).__name__}. {inspect}",
        )
    try:
        status = response.status_code
        if MIN_SUCCESS_STATUS <= status < MAX_SUCCESS_STATUS:
            return (
                SendOutcome.ACCEPTED_UNVERIFIED,
                status,
                "Shoko accepted the link request; ManualLinkJob runs asynchronously, so this "
                "is NOT verified (poll with verify_link)",
            )
        if CLIENT_ERROR_MIN <= status < CLIENT_ERROR_MAX:
            try:
                problem = _extract_problem(
                    read_bounded_json(
                        response, max_bytes=MAX_LINK_PROBLEM_BYTES, deadline=deadline, clock=clock
                    ),
                    status,
                )
            except (BoundedJsonError, httpx.HTTPError, httpx.StreamError):
                problem = None
            if problem is None:
                return (
                    SendOutcome.AMBIGUOUS_SEND,
                    status,
                    f"HTTP {status} with an unusable refusal response. {inspect}",
                )
            return (
                SendOutcome.REJECTED,
                status,
                f"Shoko refused the request: {problem} (zero writes assumed "
                "per the pinned endpoint contract)",
            )
        return SendOutcome.AMBIGUOUS_SEND, status, f"HTTP {status} during delivery. {inspect}"
    finally:
        response.close()


# ------------------------------------------------------------- XRefs + verify


def parse_file_xrefs(payload: Any) -> ShokoFileXRefs | None:
    """Parse a fresh ``GET File/{id}?include=XRefs`` body; malformed shapes yield None."""
    entry = _as_mapping(payload)
    xrefs: list[FileXRef] = []
    if entry is None:
        return None
    file_id = _positive_int(entry.get("ID"))
    size = entry.get("Size")
    raw_xrefs = entry.get("SeriesIDs")
    if (
        file_id is None
        or isinstance(size, bool)
        or not isinstance(size, int)
        or size < 0
        or not isinstance(raw_xrefs, list)
    ):
        return None
    for raw in raw_xrefs:
        parsed = _parse_xref(raw)
        if parsed is None:
            return None
        xrefs.append(parsed)
    return ShokoFileXRefs(file_id, size, tuple(xrefs))


def _parse_xref(raw: Any) -> FileXRef | None:
    """Parse one ``FileCrossReference``; any unusable field fails the whole read closed."""
    xref = _as_mapping(raw)
    series = _as_mapping(xref.get("SeriesID")) if xref is not None else None
    series_anidb = _positive_int(series.get("AniDB")) if series is not None else None
    raw_episodes = xref.get("EpisodeIDs") if xref is not None else None
    episodes: list[XrefEpisode] = []
    ok = series_anidb is not None and isinstance(raw_episodes, list)
    if ok:
        assert raw_episodes is not None
        for raw_episode in raw_episodes:
            episode = _as_mapping(raw_episode)
            anidb_episode = _positive_int(episode.get("AniDB")) if episode is not None else None
            source = episode.get("Source") if episode is not None else None
            if anidb_episode is None or not isinstance(source, str) or source not in XREF_SOURCES:
                ok = False
                break
            assert episode is not None
            episodes.append(XrefEpisode(_positive_int(episode.get("ID")), anidb_episode, source))
    if not ok:
        return None
    assert series_anidb is not None
    return FileXRef(series_anidb, tuple(episodes))


def verify_link(
    fetch: Callable[[], ShokoFileXRefs | None],
    plan: LinkPlan,
    *,
    clock: Callable[[], float] = time.monotonic,
    deadline: float,
    poll_interval: float = 0.0,
    advance: Callable[[float], None] | None = None,
    max_polls: int = 100,
) -> VerificationVerdict:
    """Poll the XRef set for the **exact full set** of planned identities.

    ``fetch`` is caller-supplied (in tests, scripted responses; in production it
    wraps ``GET File/{id}?include=XRefs``); ``clock`` and ``advance`` are injected so
    polling is deterministic offline. For every planned target the file must carry an
    XRef with ``SeriesID.AniDB`` equal to the intended anime id, an ``EpisodeIDs``
    entry whose ``AniDB`` is the intended AniDB episode id and whose internal ``ID``
    is the resolved Shoko id. Subset before the deadline -> ``PENDING``; exact set ->
    ``VERIFIED``; non-empty subset past the deadline -> ``PARTIAL``; extra/unexpected
    identities, or a file left Unrecognized (zero XRefs) past the deadline ->
    ``CONTRADICTION`` — leaving Unrecognized does not prove success. A fetch failure
    is ``UNKNOWN`` (never assume).
    """
    if plan.status is not PlanStatus.LINKABLE:
        return VerificationVerdict.UNKNOWN
    expected = _expected_identities(plan)
    last = VerificationVerdict.PENDING
    for _ in range(max(1, max_polls)):
        try:
            observed = fetch()
        except Exception:
            return VerificationVerdict.UNKNOWN
        if observed is None:
            return VerificationVerdict.UNKNOWN
        last = _evaluate_xrefs(observed, plan, expected, clock(), deadline)
        if last is not VerificationVerdict.PENDING:
            return last
        if advance is not None:
            advance(poll_interval)
    return last


# ---------------------------------------------------------------- orchestration


def reconcile_link_plan(
    endpoint: ApiEndpoint,
    plan: LinkPlan,
    settings: Settings,
    folders: Mapping[int, ImportFolder],
    *,
    active_targets_digest: str,
    active_structure_digest: str,
    dry_run: bool = True,
    attestation_valid: bool = False,
    apply_gate_open: bool = False,
    queue: QueueSnapshot | None = None,
    blocked_over: int | None = None,
    mapping_index: MappingIndex | None = None,
    clock: Callable[[], float] = time.monotonic,
    verify_fetch: Callable[[], ShokoFileXRefs | None] | None = None,
    verify_deadline: float | None = None,
    verify_poll_interval: float = 0.0,
    verify_advance: Callable[[float], None] | None = None,
) -> ReconcileResult:
    """One orchestration pass: gate, (dry-run) short-circuit, single POST, verification.

    ``dry_run=True`` (the default) performs only GETs: preflight runs, then the flow
    stops **before** :func:`submit_link` — the isolated POST call site is never
    reached. The apply path reaches ``submit_link`` only when preflight is
    OK; verification runs only after an accepted send.
    """
    if plan.status is not PlanStatus.LINKABLE:
        return ReconcileResult(
            stage=ReconcileStage.PENDING_TARGETS,
            plan=plan,
            skip=None,
            preflight=None,
            send=None,
            verification=None,
        )
    verdict = preflight(
        endpoint,
        plan,
        settings,
        folders,
        attestation_valid=attestation_valid,
        apply_gate_open=apply_gate_open,
        queue=queue,
        active_targets_digest=active_targets_digest,
        active_structure_digest=active_structure_digest,
        blocked_over=blocked_over,
        mapping_index=mapping_index,
        check_file=dry_run,
        check_targets=dry_run,
    )
    if verdict.ok and not dry_run:
        verdict = preflight_link_jobs_and_targets(endpoint, plan, clock=clock)
        if verdict.ok:
            verdict = preflight_file(endpoint, plan, settings, folders)
    if not verdict.ok:
        return ReconcileResult(
            stage=ReconcileStage.PREFLIGHT_BLOCKED,
            plan=plan,
            skip=None,
            preflight=verdict,
            send=None,
            verification=None,
        )
    if dry_run:
        return ReconcileResult(
            stage=ReconcileStage.DRY_RUN,
            plan=plan,
            skip=None,
            preflight=verdict,
            send=None,
            verification=None,
        )
    send = submit_link(endpoint, plan, clock=clock)
    verification: VerificationVerdict | None = None
    if (
        send.outcome is SendOutcome.ACCEPTED_UNVERIFIED
        and verify_fetch is not None
        and verify_deadline is not None
    ):
        verification = verify_link(
            verify_fetch,
            plan,
            clock=clock,
            deadline=verify_deadline,
            poll_interval=verify_poll_interval,
            advance=verify_advance,
        )
    return ReconcileResult(
        stage=ReconcileStage.SUBMITTED,
        plan=plan,
        skip=None,
        preflight=verdict,
        send=send,
        verification=verification,
    )


# ----------------------------------------------------------------------- helpers


@dataclass(frozen=True)
class _Frozen:
    tvdb_series: int
    season_number: int
    episode_number: int
    absolute_episode_number: int | None
    anidb_anime_id: int
    anidb_episode_number: int
    episode_type: str
    source_rule: str
    anidb_episode_id: int


def _target_provenance(item: EpisodeTargetResolution) -> _Frozen | str:
    """Validate one mapping-provenance record; returns the frozen tuple or a problem string."""
    target = (item.anidb_anime_id, item.anidb_episode_number, item.episode_type, item.source_rule)
    if any(value is None for value in target) or item.anidb_episode_id is None:
        return f"resolved mapping record lacks provenance fields: {target} "
    anidb_anime_id = item.anidb_anime_id
    anidb_episode_number = item.anidb_episode_number
    episode_type = item.episode_type
    source_rule = item.source_rule
    anidb_episode_id = item.anidb_episode_id
    assert anidb_anime_id is not None
    assert anidb_episode_number is not None
    assert episode_type is not None
    assert source_rule is not None
    assert anidb_episode_id is not None
    if anidb_anime_id <= 0 or anidb_episode_number <= 0 or anidb_episode_id <= 0:
        return "mapping provenance carries non-positive AniDB identifiers"
    if episode_type not in EPISODE_TYPE_WIRE_NAMES:
        return f"episode type {episode_type!r} is not a wired EpisodeType name"
    return _Frozen(
        tvdb_series=item.tvdb_series,
        season_number=item.episode.season_number,
        episode_number=item.episode.episode_number,
        absolute_episode_number=item.episode.absolute_episode_number,
        anidb_anime_id=anidb_anime_id,
        anidb_episode_number=anidb_episode_number,
        episode_type=episode_type,
        source_rule=source_rule,
        anidb_episode_id=anidb_episode_id,
    )


def _match_anidb_episode(
    payload: Any, anime_id: int, type_name: str, number: int
) -> int | EpisodeResolutionStatus | None:
    """Find the unique AniDB episode for (EpisodeNumber, Type); None = malformed envelope.

    A ``Total`` above the returned ``List`` length means the server page was
    truncated: absence is NOT conclusive, so this answers ``LIST_TRUNCATED``
    (fail closed) instead of ``EPISODE_NOT_FOUND``.
    """
    rows = _anidb_rows(_as_mapping(payload))
    if rows is None:
        return None
    total, entries = rows
    if total > len(entries):
        return EpisodeResolutionStatus.LIST_TRUNCATED
    if total < len(entries):
        return EpisodeResolutionStatus.MALFORMED
    return _match_anidb_rows(entries, anime_id, type_name, number)


def _anidb_rows(envelope: Mapping[str, Any] | None) -> tuple[int, list[Any]] | None:
    if envelope is None:
        return None
    rows = envelope.get("List")
    total = envelope.get("Total")
    if (
        not isinstance(rows, list)
        or isinstance(total, bool)
        or not isinstance(total, int)
        or total < 0
    ):
        return None
    return total, rows


def _match_anidb_rows(
    entries: Sequence[Any], anime_id: int, type_name: str, number: int
) -> int | EpisodeResolutionStatus:
    matches: list[int] = []
    seen_ids: set[int] = set()
    seen_identities: set[tuple[int, str, int]] = set()
    for raw in entries:
        row = _as_mapping(raw)
        if row is None:
            return EpisodeResolutionStatus.MALFORMED
        row_anime_id = _positive_int(row.get("AnimeID"))
        row_number = _positive_int(row.get("EpisodeNumber"))
        row_id = _positive_int(row.get("ID"))
        row_type = row.get("Type")
        if (
            row_anime_id is None
            or row_number is None
            or row_id is None
            or not isinstance(row_type, str)
            or row_type not in EPISODE_TYPE_WIRE_NAMES
        ):
            return EpisodeResolutionStatus.MALFORMED
        identity = (row_anime_id, row_type, row_number)
        if row_id in seen_ids or identity in seen_identities:
            return EpisodeResolutionStatus.MALFORMED
        seen_ids.add(row_id)
        seen_identities.add(identity)
        if row_anime_id != anime_id:
            continue
        if row_number != number or row_type != type_name:
            continue
        matches.append(row_id)
    if not matches:
        return EpisodeResolutionStatus.EPISODE_NOT_FOUND
    if len(set(matches)) > 1:
        return EpisodeResolutionStatus.MALFORMED
    return matches[0]


def _parse_shoko_episode(payload: Any) -> tuple[int | None, int | None, str]:
    """Parse Shoko ``Episode.IDs``: ``(internal_id, anidb_episode_id, problem)``."""
    entry = _as_mapping(payload)
    ids = _as_mapping(entry.get("IDs")) if entry is not None else None
    if ids is None:
        return None, None, "episode response has no valid IDs object"
    internal = _positive_int(ids.get("ID"))
    anidb = _positive_int(ids.get("AniDB"))
    if internal is None:
        return None, anidb, "episode IDs.ID (internal AnimeEpisodeID) is missing/invalid"
    if anidb is None:
        return internal, None, "episode IDs.AniDB is missing/invalid"
    return internal, anidb, ""


def _get(
    endpoint: ApiEndpoint,
    path: str,
    params: Mapping[str, Any] | None = None,
    *,
    deadline: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> _GetOutcome:
    try:
        status, payload = get_bounded_json(endpoint, path, params, deadline=deadline, clock=clock)
    except BoundedJsonError as exc:
        return _GetOutcome(None, exc.status_code, f"{path}: {exc}")
    except (HttpPolicyError, httpx.HTTPError) as exc:
        return _GetOutcome(None, None, f"{path}: {type(exc).__name__}")
    if not MIN_SUCCESS_STATUS <= status < MAX_SUCCESS_STATUS:
        return _GetOutcome(None, status, f"{path}: HTTP {status}")
    return _GetOutcome(payload, status, "")


def _expected_identities(plan: LinkPlan) -> frozenset[tuple[int, int, int]]:
    return frozenset(
        (target.anidb_anime_id, target.anidb_episode_id, target.shoko_episode_id)
        for target in plan.targets
    )


def _evaluate_xrefs(
    observed: ShokoFileXRefs,
    plan: LinkPlan,
    expected: frozenset[tuple[int, int, int]],
    now: float,
    deadline: float,
) -> VerificationVerdict:
    if observed.file_id != plan.shoko_file_id:
        return VerificationVerdict.CONTRADICTION
    if observed.size != plan.shoko_size:
        return VerificationVerdict.UNKNOWN
    actual = {
        (xref.series_anidb_id, episode.anidb_episode_id, episode.shoko_episode_id)
        for xref in observed.xrefs
        for episode in xref.episodes
    }
    actual_identities = {(anime, anidb) for anime, anidb, _ in actual}
    expected_identities = {(anime, anidb) for anime, anidb, _ in expected}
    # A materialized link to any internal episode other than the resolved one is a
    # wrong-set replacement; only a null internal ID stays "unverified".
    wrong_links = {item for item in actual if item[2] is not None and item not in expected}
    verdict = VerificationVerdict.VERIFIED
    if actual != expected:
        if not actual_identities <= expected_identities or wrong_links:
            verdict = VerificationVerdict.CONTRADICTION
        else:
            # Leaving the file Unrecognized past the deadline does not prove success.
            verdict = _subset_verdict(now, deadline, empty=not actual)
    return verdict


def _subset_verdict(now: float, deadline: float, *, empty: bool) -> VerificationVerdict:
    if now < deadline:
        return VerificationVerdict.PENDING
    return VerificationVerdict.CONTRADICTION if empty else VerificationVerdict.PARTIAL


def _extract_problem(payload: Any, status: int) -> str | None:
    """Minimal, credential-free extraction of a Shoko refusal message (ProblemDetails)."""
    message = ""
    entry = _as_mapping(payload)
    if entry is None:
        return None
    if "status" in entry and (type(entry["status"]) is not int or entry["status"] != status):
        return None
    if any(
        key in entry and not isinstance(entry[key], str)
        for key in ("title", "detail", "type", "instance")
    ):
        return None
    if "errors" in entry:
        errors = _as_mapping(entry["errors"])
        if errors is None or any(
            not isinstance(value, str)
            and not (isinstance(value, list) and all(isinstance(item, str) for item in value))
            for value in errors.values()
        ):
            return None
    parts: list[str] = []
    for key in ("title", "detail"):
        value = entry.get(key)
        if isinstance(value, str) and value.strip():
            parts.append(value.strip())
    errors = _as_mapping(entry.get("errors"))
    if errors is not None:
        for field_name in sorted(key for key in errors if isinstance(key, str)):
            value = errors[field_name]
            messages = value if isinstance(value, list) else [value]
            texts = [item.strip() for item in messages if isinstance(item, str) and item.strip()]
            if texts:
                parts.append(f"{field_name}: {'; '.join(texts[:3])}")
    message = " | ".join(parts)
    return redact(message)[:200] if message else None


def _require_digest(value: str, name: str) -> None:
    if not isinstance(value, str) or not _DIGEST_RE.fullmatch(value):
        raise ConfigError(f"link plan requires a SHA-256 hex {name} (64 lowercase hex characters)")


def _target_key(target: LinkTarget) -> tuple[int, int, int, int, str, str]:
    return (
        target.tvdb_series,
        target.season_number,
        target.episode_number,
        target.anidb_anime_id,
        target.episode_type,
        target.source_rule,
    )


def _pending_key(item: PendingTarget) -> tuple[int, int, int, int, str, str]:
    return (
        item.tvdb_series,
        item.season_number,
        item.episode_number,
        item.anidb_anime_id,
        item.episode_type,
        item.source_rule,
    )


def _fmt(value: int | None) -> str:
    return "-" if value is None else str(value)


def _positive_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return cast(int, value)


def _safe_path(path: str) -> str:
    try:
        return normalize_posix(path)
    except PathError:
        return path


def _as_mapping(raw: Any) -> Mapping[str, Any] | None:
    if isinstance(raw, dict):
        return cast("Mapping[str, Any]", raw)
    return None
