"""Anime-Lists XML parsing, reverse TVDB→AniDB inversion, overrides, provenance digests.

Pure and offline: consumes XML text/bytes and produces an immutable
:class:`MappingIndex` a caller can atomically activate. No network I/O happens
here; the runtime download/refresh layer is a separate module.

Contract source: ``docs/compatibility.md`` section 3 (Anime-Lists mapping contract).
Semantics implemented verbatim:

* Offsets are AniDB→TVDB additions (``tvdb = anidb + offset``), reversed as
  ``anidb = tvdb - offset``.
* Precedence is applied to forward AniDB identities before inversion: individual
  ``;a-b;`` pairs > mapping range ``offset`` > entry ``episodeoffset``;
  ``start``/``end`` without ``offset`` is inert. A higher-precedence declaration
  suppresses lower-precedence reverse positions for the same AniDB identity.
* TVDB ``0`` in a target position means "AniDB episode has no TVDB counterpart"
  and is never registered as a reverse key.
* ``;1-1+2;`` registers both TVDB keys for one AniDB episode; ``;1-1;2-1;``
  resolves one TVDB key to the complete AniDB parts declared in that rule; ``;1+2-1;`` is
  invalid and excludes the whole entry.
* ``defaulttvdbseason="a"`` means absolute identity (Sonarr absolute == AniDB
  number) only for episodes the entry's mapping-lists do not cover;
  ``a`` + ``episodeoffset`` is UNVERIFIED and fails closed.
* Season 0 never resolves by season alone: only explicit season-0 mappings
  qualify, and only a unique hit for the episode resolves.
* Split-season entries sharing one TVDB season partition it by their offsets: a
  queried episode resolves to the entry with the greatest offset strictly below
  it (AoT TVDB 267440 season 4: offsets 0/16/28 partition episodes 1-16/17-28/
  29+). Equal offsets collide → ambiguous.
 * Episode type follows the mapping source: regular entry-offset, absolute-identity,
   and ``anidbseason=1`` range rules remain ``Episode`` regardless of number;
   ``anidbseason=0`` range rules remain ``Special`` regardless of number. Explicit
   ``;a-b;`` pairs decode documented T/O number encodings first (``201-400`` →
   Trailer numbers 1-200, ``>=401`` → Other numbers 1+), then use season 0 as
   ``Special`` or season 1 as ``Episode``. Season-0 explicit 101-103 identities
   have unverified types and fence only their TVDB positions.
   Credits/Parody are not expressible by Anime-Lists and are never produced.

Recorded fail-closed interpretations (reported as problems, never guessed):

* A malformed or unsupported ``<mapping>`` declaration invalidates its entire
  ``<anime>`` entry, so an entry offset cannot silently replace the rejected
  mapping. This includes missing/invalid season attributes, unknown mapping
  attributes, malformed pair tokens/ranges, and ``anidbseason`` ``2`` or higher
  (UNVERIFIED). ``start``/``end`` without ``offset`` remains the documented inert
  declaration. Such an entry contributes no targets and fences its known TVDB
  series; if its TVDB ID cannot be identified, all queries fail closed. Other valid
  entries for unrelated series remain usable for offline inspection. Download refresh
  rejects any snapshot with an excluded entry, preserving the last-known-good generation.
* A reverse query for TVDB episode ``0`` answers ``NO_EQUIVALENT`` (the
  Anime-Lists no-counterpart sentinel), never ``UNMAPPED``.
* A mapping body above ``MAX_PAIRS_PER_RULE`` excludes the entry: a partially
  applied pair set is never trusted.
* Operator override **adds** never authorize a target that upstream declares as
  having no TVDB counterpart (``;a-0;``): such adds are refused and reported as
  ``add-no-counterpart`` conflicts — upstream explicit declarations outrank
  operator additions for the exact declared identity.

The reverse map is not 1:1: explicit composite parts resolve together, while
competing declarations answer ``AMBIGUOUS`` (writes prohibited). Candidate
ordering is deterministic: sorted by AniDB id, then episode number, then type.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from enum import Enum
from types import MappingProxyType
from typing import Any, Final, cast
from xml.etree.ElementTree import Element, ParseError

import yaml  # type: ignore[import-untyped]
from defusedxml import DefusedXmlException  # type: ignore[import-untyped]
from defusedxml import ElementTree as SafeElementTree

from .errors import ConfigError, LinkerError

ROOT_TAG: Final[str] = "anime-list"
DEFAULT_MAX_ENTRIES: Final[int] = 50_000
MAX_PAIRS_PER_RULE: Final[int] = 2_000
MAX_RANGE_SPAN: Final[int] = 2_000
MAX_AGGREGATE_RANGE_KEYS: Final[int] = 2_000_000
UNVERIFIED_ANIDBSEASON: Final[int] = 2
SPECIAL_SEASON: Final[int] = 0
REGULAR_SEASON: Final[int] = 1
TRAILER_MIN: Final[int] = 201
TRAILER_MAX: Final[int] = 400
OTHER_MIN: Final[int] = 401
UNVERIFIED_MIN: Final[int] = 101
UNVERIFIED_MAX: Final[int] = 103
_UNVERIFIED_SEASON: Final[int] = 2

_ANIME_ENTRY_ATTRIBUTES: Final[frozenset[str]] = frozenset(
    {"anidbid", "tvdbid", "defaulttvdbseason", "episodeoffset"}
)
_ANIME_UNUSED_ATTRIBUTES: Final[frozenset[str]] = frozenset(
    {"tmdbtv", "tmdbseason", "tmdboffset", "tmdbid", "imdbid"}
)
_MAPPING_ATTRIBUTES: Final[frozenset[str]] = frozenset(
    {"anidbseason", "tvdbseason", "tmdbseason", "start", "end", "offset"}
)
_METADATA_CHILDREN: Final[frozenset[str]] = frozenset(
    {
        "name",
        "supplemental-info",
        "studio",
        "genre",
        "fanart",
        "thumb",
        "banner",
        "background",
        "credits",
        "director",
        "webmaster",
    }
)
_PAIR_RE: Final[re.Pattern[str]] = re.compile(r"(\d+)-(\d+(?:\+\d+)*)")
_LEFT_MULTI_RE: Final[re.Pattern[str]] = re.compile(r"\d+\+\d+")
_INT_RE: Final[re.Pattern[str]] = re.compile(r"\d+")
_SIGNED_INT_RE: Final[re.Pattern[str]] = re.compile(r"-?\d+")


class MappingError(LinkerError):
    """The document as a whole is unusable: root, XML syntax, entry cap, defused XML threats."""


class EntryIssueKind(Enum):
    """Structured severity for a recorded per-entry parser issue."""

    MALFORMED = "malformed"
    KNOWN_UNSUPPORTED = "known_unsupported"
    WARNING = "warning"


class EntryIssueCode(Enum):
    """Stable machine-readable categories; diagnostics are never parsed as control flow."""

    MALFORMED_ENTRY = "malformed_entry"
    ABSOLUTE_SEASON_WITH_EPISODE_OFFSET = "absolute_season_with_episode_offset"
    UNVERIFIED_SEASON_ZERO_EPISODE_TYPE = "unverified_season_zero_episode_type"
    ENTRY_WARNING = "entry_warning"


@dataclass(frozen=True)
class EntryParseIssue:
    """One typed parser finding alongside its human-readable explanation."""

    kind: EntryIssueKind
    code: EntryIssueCode
    detail: str


class EpisodeType(Enum):
    """Derivable episode types; Credits/Parody are not expressible by Anime-Lists."""

    EPISODE = "Episode"
    SPECIAL = "Special"
    TRAILER = "Trailer"
    OTHER = "Other"


_OVERRIDABLE_TYPES: Final[Mapping[str, EpisodeType]] = MappingProxyType(
    {member.value: member for member in EpisodeType}
)


class SourceRule(Enum):
    """Provenance of one mapping target, ordered by precedence rank."""

    OVERRIDE = "override"
    EXPLICIT_PAIR = "explicit_pair"
    MAPPING_RANGE_OFFSET = "mapping_range_offset"
    ENTRY_OFFSET = "entry_offset"
    ABSOLUTE_IDENTITY = "absolute_identity"


_PRECEDENCE_RANK: Final[Mapping[SourceRule, int]] = MappingProxyType(
    {
        SourceRule.OVERRIDE: 0,
        SourceRule.EXPLICIT_PAIR: 1,
        SourceRule.MAPPING_RANGE_OFFSET: 2,
        SourceRule.ENTRY_OFFSET: 3,
        SourceRule.ABSOLUTE_IDENTITY: 4,
    }
)


class ResolutionStatus(Enum):
    """Outcome of one reverse (TVDB→AniDB) query."""

    RESOLVED = "resolved"
    AMBIGUOUS = "ambiguous"
    UNMAPPED = "unmapped"
    NO_EQUIVALENT = "no_equivalent"
    CONFLICT = "conflict"


@dataclass(frozen=True)
class EpisodePair:
    """One ``;anidbEp-tvdbEp;`` declaration; empty ``tvdb_episodes`` is the TVDB-0 marker."""

    anidb_episode: int
    tvdb_episodes: tuple[int, ...]


@dataclass(frozen=True)
class MappingRule:
    """One parsed ``<mapping>`` element from ``<mapping-list>`` or ``<before>``."""

    origin: str
    anidbseason: int
    tvdbseason: int
    start: int | None
    end: int | None
    offset: int | None
    pairs: tuple[EpisodePair, ...]


@dataclass(frozen=True)
class AnimeEntry:
    """One structurally valid ``<anime>`` element."""

    index: int
    anidbid: int
    tvdbid: int
    defaulttvdbseason: int | str
    episodeoffset: int
    rules: tuple[MappingRule, ...]
    unknown_attributes: tuple[str, ...]


@dataclass(frozen=True)
class EntryProblem:
    """Recorded issues for one ``<anime>`` element; excluded entries contribute no targets."""

    index: int
    anidbid: int | None
    tvdbid: int | None
    excluded: bool
    problems: tuple[str, ...]
    issues: tuple[EntryParseIssue, ...]
    fenced_tvdb_keys: tuple[QueryKey, ...] = ()

    @property
    def known_unsupported(self) -> bool:
        """True only for the documented unsupported case, never for malformed declarations."""
        return any(
            issue.kind is EntryIssueKind.KNOWN_UNSUPPORTED for issue in self.issues
        ) and not any(issue.kind is EntryIssueKind.MALFORMED for issue in self.issues)


@dataclass(frozen=True)
class ParsedMappings:
    """Parse result: usable entries plus every recorded problem."""

    entries: tuple[AnimeEntry, ...]
    entry_problems: tuple[EntryProblem, ...]
    document_problems: tuple[str, ...]
    anime_elements: int

    @property
    def excluded_entries(self) -> int:
        return sum(1 for problem in self.entry_problems if problem.excluded)

    @property
    def invalid_entries(self) -> int:
        """Excluded entries other than the one explicitly recognized unsupported construct."""
        return sum(
            1
            for problem in self.entry_problems
            if problem.excluded and not problem.known_unsupported
        )

    @property
    def problem_count(self) -> int:
        entry_total = sum(len(problem.problems) for problem in self.entry_problems)
        return len(self.document_problems) + entry_total


@dataclass(frozen=True)
class MappingTarget:
    """One resolved reverse-mapping candidate with provenance."""

    anidb_id: int
    anidb_episode_number: int
    episode_type: EpisodeType
    source_rule: SourceRule
    precedence_rank: int
    origin: str

    # Only explicit parts declared together authorize a composite destination set.
    composite_claim: int | None = None

    @property
    def identity(self) -> tuple[int, int, EpisodeType]:
        return (self.anidb_id, self.anidb_episode_number, self.episode_type)


TargetIdentity = tuple[int, int, EpisodeType]
QueryKey = tuple[int, int, int]
SeasonKey = tuple[int, int]


@dataclass(frozen=True)
class Resolution:
    """Answer to one reverse query; ``AMBIGUOUS``/``CONFLICT`` answers prohibit writes."""

    status: ResolutionStatus
    target: MappingTarget | None
    candidates: tuple[MappingTarget, ...]
    detail: str

    @property
    def targets(self) -> tuple[MappingTarget, ...]:
        """Complete authorized destinations; rejected candidates are never targets."""
        return self.candidates if self.status is ResolutionStatus.RESOLVED else ()


@dataclass(frozen=True)
class OverrideAdd:
    """Operator-added mapping; ranks above upstream explicit pairs (operator intent)."""

    tvdb_series: int
    season: int
    episode: int
    anidb_id: int
    anidb_episode: int
    episode_type: EpisodeType


@dataclass(frozen=True)
class OverrideRemove:
    """Operator removal; ``anidb_id=None`` removes every target of the key."""

    tvdb_series: int
    season: int
    episode: int
    anidb_id: int | None


@dataclass(frozen=True)
class Overrides:
    """Validated operator overrides (content of ``settings.mappings.overrides_file``)."""

    adds: tuple[OverrideAdd, ...]
    removes: tuple[OverrideRemove, ...]
    notes: str | None


@dataclass(frozen=True)
class OverrideConflict:
    """An override that contradicts upstream data: applied, but reported."""

    kind: str
    tvdb_series: int
    season: int
    episode: int
    detail: str


@dataclass(frozen=True)
class _RangeRule:
    anidb_id: int
    anidbseason: int
    start: int
    end: int
    offset: int
    origin: str


@dataclass(frozen=True)
class _OffsetClaim:
    anidb_id: int
    offset: int
    entry_index: int


@dataclass(frozen=True)
class _AbsoluteClaim:
    anidb_id: int
    covered: frozenset[tuple[int, EpisodeType]]
    intervals: tuple[tuple[int, int, int], ...]
    entry_index: int


@dataclass(frozen=True)
class _ForwardTarget:
    tvdb_series: int
    tvdb_season: int
    tvdb_episodes: tuple[int, ...]


@dataclass(frozen=True)
class MappingIndex:
    """Immutable reverse index over one Anime-Lists snapshot plus applied overrides."""

    explicit: Mapping[QueryKey, tuple[MappingTarget, ...]]
    season_zero: Mapping[QueryKey, tuple[MappingTarget, ...]]
    ranges: Mapping[SeasonKey, tuple[_RangeRule, ...]]
    entry_offsets: Mapping[SeasonKey, tuple[_OffsetClaim, ...]]
    absolute: Mapping[int, tuple[_AbsoluteClaim, ...]]
    no_counterpart: Mapping[SeasonKey, frozenset[TargetIdentity]]
    forward: Mapping[TargetIdentity, tuple[_ForwardTarget, ...]]
    forward_precedence: Mapping[TargetIdentity, int]
    override_adds: Mapping[QueryKey, tuple[MappingTarget, ...]]
    override_removes: Mapping[QueryKey, frozenset[int] | None]
    override_conflicts: tuple[OverrideConflict, ...]
    unsafe_tvdb_ids: frozenset[int]
    unsafe_tvdb_keys: frozenset[QueryKey]
    unsafe_all: bool
    entries_indexed: int
    entries_excluded: int
    entries_with_problems: int
    mapping_rules: int
    explicit_pairs: int
    no_counterpart_pairs: int
    range_rules: int
    absolute_entries: int


# --------------------------------------------------------------------- parsing


def parse_anime_lists(
    xml_text: str | bytes | bytearray, *, max_entries: int = DEFAULT_MAX_ENTRIES
) -> ParsedMappings:
    """Parse Anime-Lists XML; a malformed entry is recorded and excluded, never guessed.

    Whole-document failures (bad root, syntax, DOCTYPE/entity threats, entry-count
    cap) raise :class:`MappingError`. Per-entry defects land in ``entry_problems``.
    """
    root = _parse_root(xml_text)
    if root.tag != ROOT_TAG:
        raise MappingError(f"root element must be <{ROOT_TAG}> (got {root.tag!r})")
    document_problems: list[str] = []
    anime_elements: list[Element] = []
    for child in root:
        if child.tag == "anime":
            anime_elements.append(child)
        else:
            document_problems.append(f"unknown element <{child.tag}> under <{ROOT_TAG}> (ignored)")
    if max_entries > 0 and len(anime_elements) > max_entries:
        raise MappingError(
            f"{len(anime_elements)} <anime> entries exceed max_entries={max_entries}"
        )
    entries: list[AnimeEntry] = []
    entry_problems: list[EntryProblem] = []
    declared_anidbids: dict[int, list[int]] = {}
    for index, element in enumerate(anime_elements):
        raw_anidbid = element.attrib.get("anidbid")
        if raw_anidbid is not None and _INT_RE.fullmatch(raw_anidbid):
            anidbid = int(raw_anidbid)
            if anidbid > 0:
                declared_anidbids.setdefault(anidbid, []).append(index)
    duplicate_anidbids = {
        anidbid: tuple(indices)
        for anidbid, indices in declared_anidbids.items()
        if len(indices) > 1
    }
    for index, element in enumerate(anime_elements):
        entry, problem = _parse_anime_element(index, element, duplicate_anidbids)
        if entry is not None:
            entries.append(entry)
        if problem is not None:
            entry_problems.append(problem)
    return ParsedMappings(
        entries=tuple(entries),
        entry_problems=tuple(entry_problems),
        document_problems=tuple(document_problems),
        anime_elements=len(anime_elements),
    )


def _parse_root(xml_text: str | bytes | bytearray) -> Element:
    try:
        # ``defusedxml`` accepts any bytes-like buffer at runtime, although its bundled
        # annotation only lists ``bytes``. Keeping the capped bytearray avoids a second
        # full-size copy before parsing the streamed source.
        return cast("Element", SafeElementTree.fromstring(cast(bytes, xml_text)))
    except DefusedXmlException as exc:
        raise MappingError(f"Anime-Lists XML rejected as unsafe XML: {exc}") from exc
    except (ParseError, ValueError, SyntaxError) as exc:
        raise MappingError(f"Anime-Lists XML is not well-formed XML: {exc}") from exc
    except Exception as exc:
        raise MappingError(f"Anime-Lists XML rejected as unsafe XML: {exc}") from exc


def _parse_anime_element(
    index: int, element: Element, duplicate_anidbids: Mapping[int, tuple[int, ...]]
) -> tuple[AnimeEntry | None, EntryProblem | None]:
    fatal: list[str] = []
    problems: list[str] = []
    known_unsupported: list[str] = []
    fenced_tvdb_keys: set[QueryKey] = set()
    issues: list[EntryParseIssue] = []
    attributes = dict(element.attrib)
    allowed = _ANIME_ENTRY_ATTRIBUTES | _ANIME_UNUSED_ATTRIBUTES
    unknown = tuple(name for name in sorted(attributes) if name not in allowed)
    for name in unknown:
        fatal.append(f"unknown attribute {name!r} on <anime> (entry excluded, not guessed)")

    anidbid = _positive_int(attributes.get("anidbid"), "anidbid", fatal)
    tvdbid = _positive_int(attributes.get("tvdbid"), "tvdbid", fatal)
    season = _season_attr(attributes.get("defaulttvdbseason"), fatal)
    episodeoffset = _offset_attr(attributes.get("episodeoffset"), fatal)

    unsupported_absolute_offset = season == "a" and episodeoffset is not None
    unsupported_detail = (
        "defaulttvdbseason='a' with episodeoffset is UNVERIFIED; TVDB ownership is fenced"
    )
    if unsupported_absolute_offset:
        problems.append(unsupported_detail)

    rules = _parse_entry_rules(
        element,
        problems,
        fatal,
        known_unsupported,
        fenced_tvdb_keys,
        tvdbid=tvdbid,
    )

    if anidbid is not None and anidbid in duplicate_anidbids:
        entries = ", ".join(str(item) for item in duplicate_anidbids[anidbid])
        fatal.append(f"anidbid {anidbid} is duplicated by entries {entries}")

    if fatal:
        issues = [
            EntryParseIssue(
                EntryIssueKind.MALFORMED, EntryIssueCode.MALFORMED_ENTRY, "; ".join(fatal)
            )
        ]
        if known_unsupported:
            issues.append(
                EntryParseIssue(
                    EntryIssueKind.KNOWN_UNSUPPORTED,
                    EntryIssueCode.UNVERIFIED_SEASON_ZERO_EPISODE_TYPE,
                    "; ".join(known_unsupported),
                )
            )
        if unsupported_absolute_offset:
            issues.append(
                EntryParseIssue(
                    EntryIssueKind.KNOWN_UNSUPPORTED,
                    EntryIssueCode.ABSOLUTE_SEASON_WITH_EPISODE_OFFSET,
                    unsupported_detail,
                )
            )
        return None, EntryProblem(
            index,
            anidbid,
            tvdbid,
            True,
            tuple(fatal + problems + known_unsupported),
            tuple(issues),
            tuple(sorted(fenced_tvdb_keys)),
        )
    if anidbid is None or tvdbid is None or not isinstance(season, int | str):
        detail = "; ".join(problems) or "entry identity or season is incomplete"
        return None, EntryProblem(
            index,
            anidbid,
            tvdbid,
            True,
            tuple(fatal + problems + known_unsupported),
            (EntryParseIssue(EntryIssueKind.MALFORMED, EntryIssueCode.MALFORMED_ENTRY, detail),),
            tuple(sorted(fenced_tvdb_keys)),
        )
    if unsupported_absolute_offset:
        issue = EntryParseIssue(
            EntryIssueKind.KNOWN_UNSUPPORTED,
            EntryIssueCode.ABSOLUTE_SEASON_WITH_EPISODE_OFFSET,
            unsupported_detail,
        )
        issues = [issue]
        if known_unsupported:
            issues.append(
                EntryParseIssue(
                    EntryIssueKind.KNOWN_UNSUPPORTED,
                    EntryIssueCode.UNVERIFIED_SEASON_ZERO_EPISODE_TYPE,
                    "; ".join(known_unsupported),
                )
            )
        return None, EntryProblem(
            index,
            anidbid,
            tvdbid,
            True,
            tuple(problems + known_unsupported),
            tuple(issues),
            tuple(sorted(fenced_tvdb_keys)),
        )
    entry = AnimeEntry(
        index=index,
        anidbid=anidbid,
        tvdbid=tvdbid,
        defaulttvdbseason=season,
        episodeoffset=episodeoffset if episodeoffset is not None else 0,
        rules=rules,
        unknown_attributes=unknown,
    )
    if problems:
        issues.append(
            EntryParseIssue(
                EntryIssueKind.WARNING, EntryIssueCode.ENTRY_WARNING, "; ".join(problems)
            )
        )
    if known_unsupported:
        issues.append(
            EntryParseIssue(
                EntryIssueKind.KNOWN_UNSUPPORTED,
                EntryIssueCode.UNVERIFIED_SEASON_ZERO_EPISODE_TYPE,
                "; ".join(known_unsupported),
            )
        )
    if issues:
        return entry, EntryProblem(
            index,
            anidbid,
            tvdbid,
            False,
            tuple(problems + known_unsupported),
            tuple(issues),
            tuple(sorted(fenced_tvdb_keys)),
        )
    return entry, None


def _positive_int(raw: str | None, name: str, fatal: list[str]) -> int | None:
    if raw is None:
        fatal.append(f"missing {name}")
        return None
    if _INT_RE.fullmatch(raw) is None:
        fatal.append(f"invalid {name} {raw!r}")
        return None
    value = int(raw)
    if value <= 0:
        fatal.append(f"{name} must be positive (got {value})")
        return None
    return value


def _season_attr(raw: str | None, fatal: list[str]) -> int | str | None:
    if raw is None:
        fatal.append("missing defaulttvdbseason")
        return None
    if raw == "a":
        return "a"
    if _INT_RE.fullmatch(raw) is None:
        fatal.append(f"invalid defaulttvdbseason {raw!r}")
        return None
    return int(raw)


def _offset_attr(raw: str | None, fatal: list[str]) -> int | None:
    if raw is None:
        return None
    if _SIGNED_INT_RE.fullmatch(raw) is None:
        fatal.append(f"invalid episodeoffset {raw!r}")
        return None
    return int(raw)


def _parse_entry_rules(
    element: Element,
    problems: list[str],
    fatal: list[str],
    known_unsupported: list[str],
    fenced_tvdb_keys: set[QueryKey],
    *,
    tvdbid: int | None,
) -> tuple[MappingRule, ...]:
    rules: list[MappingRule] = []
    for child in element:
        tag = child.tag
        if tag in _METADATA_CHILDREN:
            continue
        if tag in ("mapping-list", "before"):
            rules.extend(
                _parse_rule_container(
                    tag,
                    child,
                    problems,
                    fatal,
                    known_unsupported,
                    fenced_tvdb_keys,
                    tvdbid=tvdbid,
                )
            )
        elif isinstance(tag, str) and "mapping" in tag.casefold():
            detail = f"unsupported mapping element <{tag}> inside <anime> (entry excluded)"
            fatal.append(detail)
        else:
            problems.append(f"unknown element <{tag}> inside <anime> (ignored)")
    return tuple(rules)


def _parse_rule_container(
    origin: str,
    container: Element,
    problems: list[str],
    fatal: list[str],
    known_unsupported: list[str],
    fenced_tvdb_keys: set[QueryKey],
    *,
    tvdbid: int | None,
) -> Iterable[MappingRule]:
    if container.attrib:
        detail = f"unexpected attributes on <{origin}> (entry excluded)"
        fatal.append(detail)
    if container.text and container.text.strip():
        detail = f"unmapped text inside <{origin}> (entry excluded)"
        fatal.append(detail)
    for mapping in container:
        if mapping.tail and mapping.tail.strip():
            detail = f"unmapped text inside <{origin}> (entry excluded)"
            fatal.append(detail)
        if not isinstance(mapping.tag, str) or mapping.tag != "mapping":
            detail = f"non-<mapping> element inside <{origin}> (entry excluded)"
            fatal.append(detail)
            continue
        rule = _parse_mapping_element(
            origin,
            mapping,
            problems,
            fatal,
            known_unsupported,
            fenced_tvdb_keys,
            tvdbid=tvdbid,
        )
        if rule is not None:
            yield rule


def _parse_mapping_element(
    origin: str,
    mapping: Element,
    problems: list[str],
    fatal: list[str],
    known_unsupported: list[str],
    fenced_tvdb_keys: set[QueryKey],
    *,
    tvdbid: int | None,
) -> MappingRule | None:
    attributes = dict(mapping.attrib)
    rule_problems: list[str] = []
    invalid = False
    unknown = sorted(set(attributes) - _MAPPING_ATTRIBUTES)
    for name in unknown:
        rule_problems.append(
            f"unknown attribute {name!r} on <mapping> (entry excluded, not guessed)"
        )
    invalid = bool(unknown)
    if len(mapping) > 0:
        rule_problems.append("nested element inside <mapping> (entry excluded)")
        invalid = True

    anidbseason = _mapping_int(attributes.get("anidbseason"), "anidbseason", rule_problems)
    tvdbseason = _mapping_int(attributes.get("tvdbseason"), "tvdbseason", rule_problems)
    start = _optional_mapping_int(attributes.get("start"), "start", rule_problems)
    end = _optional_mapping_int(attributes.get("end"), "end", rule_problems)
    offset = _mapping_signed_int(attributes.get("offset"), "offset", rule_problems)

    if anidbseason is None or tvdbseason is None:
        invalid = True
    if ("start" in attributes and start is None) or ("end" in attributes and end is None):
        invalid = True
    if "offset" in attributes and offset is None:
        invalid = True

    pairs, pair_fatal, unsupported_pairs = _parse_pair_body(
        "".join(mapping.itertext()), rule_problems, anidbseason=anidbseason
    )
    if pair_fatal:
        invalid = True

    _record_unsupported_pair_fences(
        unsupported_pairs,
        known_unsupported,
        fenced_tvdb_keys,
        tvdbid=tvdbid,
        tvdbseason=tvdbseason,
    )

    bounds = _validated_range(anidbseason, start, end, offset, rule_problems)
    if anidbseason is not None and anidbseason >= UNVERIFIED_ANIDBSEASON:
        rule_problems.append(
            f"mapping anidbseason={anidbseason} semantics are UNVERIFIED (entry excluded)"
        )
        invalid = True
    if anidbseason is not None and anidbseason < 0:
        invalid = True
    if offset is not None and bounds == (None, None, None):
        invalid = True

    if invalid:
        fatal.extend(rule_problems)
        fatal.append(
            f"<mapping> in <{origin}> has a malformed or unsupported declaration (entry excluded)"
        )
        return None
    start_ok, end_ok, offset_ok = bounds
    problems.extend(rule_problems)
    if anidbseason is None or tvdbseason is None:
        return None
    if anidbseason >= UNVERIFIED_ANIDBSEASON:
        return None
    return MappingRule(
        origin=origin,
        anidbseason=anidbseason,
        tvdbseason=tvdbseason,
        start=start_ok,
        end=end_ok,
        offset=offset_ok,
        pairs=pairs,
    )


def _record_unsupported_pair_fences(
    pairs: Sequence[EpisodePair],
    known_unsupported: list[str],
    fenced_tvdb_keys: set[QueryKey],
    *,
    tvdbid: int | None,
    tvdbseason: int | None,
) -> None:
    for pair in pairs:
        known_unsupported.append(
            f"AniDB season 0 episode {pair.anidb_episode} has unverified type; "
            "only its explicit TVDB positions are fenced"
        )
        if tvdbid is not None and tvdbseason is not None:
            fenced_tvdb_keys.update(
                (tvdbid, tvdbseason, tvdb_episode)
                for tvdb_episode in pair.tvdb_episodes
                if tvdb_episode >= 1
            )


def _validated_range(
    anidbseason: int | None,
    start: int | None,
    end: int | None,
    offset: int | None,
    problems: list[str],
) -> tuple[int | None, int | None, int | None]:
    if anidbseason is None:
        return None, None, None
    if anidbseason == _UNVERIFIED_SEASON:
        problems.append("mapping anidbseason='2' semantics are UNVERIFIED (range excluded)")
        return None, None, None
    if anidbseason < 0:
        problems.append("negative anidbseason is not valid (range excluded)")
        return None, None, None
    if offset is None:
        if start is not None or end is not None:
            problems.append("mapping start/end without offset is inert (range ignored)")
        return None, None, None
    bounds_ok, reason = _bounds_ok(start, end)
    if not bounds_ok:
        problems.append(reason)
        return None, None, None
    return start, end, offset


def _bounds_ok(start: int | None, end: int | None) -> tuple[bool, str]:
    if start is None or end is None:
        return False, "mapping offset without [start,end] bounds is unbounded (range excluded)"
    if start < 1:
        return False, f"mapping start {start} must be >= 1 (range excluded)"
    if end < start:
        return False, f"mapping range end {end} < start {start} (range excluded)"
    if end - start > MAX_RANGE_SPAN:
        span = end - start
        return False, f"mapping span {span} exceeds cap {MAX_RANGE_SPAN} (range excluded)"
    return True, ""


def _mapping_int(raw: str | None, name: str, problems: list[str]) -> int | None:
    if raw is None:
        problems.append(f"mapping missing {name}")
        return None
    if _INT_RE.fullmatch(raw) is None:
        problems.append(f"mapping invalid {name} {raw!r}")
        return None
    return int(raw)


def _optional_mapping_int(raw: str | None, name: str, problems: list[str]) -> int | None:
    if raw is None:
        return None
    if _INT_RE.fullmatch(raw) is None:
        problems.append(f"mapping invalid {name} {raw!r}")
        return None
    return int(raw)


def _mapping_signed_int(raw: str | None, name: str, problems: list[str]) -> int | None:
    if raw is None:
        return None
    if _SIGNED_INT_RE.fullmatch(raw) is None:
        problems.append(f"mapping invalid {name} {raw!r}")
        return None
    return int(raw)


def _parse_pair_body(
    body: str, problems: list[str], *, anidbseason: int | None
) -> tuple[tuple[EpisodePair, ...], bool, tuple[EpisodePair, ...]]:
    pairs: list[EpisodePair] = []
    unsupported: list[EpisodePair] = []
    seen: set[EpisodePair] = set()
    tokens = [item.strip() for item in body.split(";") if item.strip()]
    if len(tokens) > MAX_PAIRS_PER_RULE:
        problems.append(
            f"mapping body has {len(tokens)} pairs, above the cap of {MAX_PAIRS_PER_RULE} "
            "(entry excluded: a partially applied pair set is never trusted)"
        )
        return (), True, ()
    for token in tokens:
        left_side = token.partition("-")[0]
        if _LEFT_MULTI_RE.fullmatch(left_side):
            problems.append(f"pair {token!r}: AniDB-side multi-episode invalid (entry rejected)")
            return tuple(pairs), True, tuple(unsupported)
        pair = _parse_pair_token(token, problems)
        if pair is None:
            return tuple(pairs), True, tuple(unsupported)
        if pair in seen:
            continue
        seen.add(pair)
        if anidbseason == SPECIAL_SEASON and UNVERIFIED_MIN <= pair.anidb_episode <= UNVERIFIED_MAX:
            unsupported.append(pair)
            continue
        pairs.append(pair)
    return tuple(pairs), False, tuple(unsupported)


def _parse_pair_token(token: str, problems: list[str]) -> EpisodePair | None:
    match = _PAIR_RE.fullmatch(token)
    if match is None:
        problems.append(f"invalid pair token {token!r} (entry excluded)")
        return None
    anidb = int(match.group(1))
    parts = [int(item) for item in match.group(2).split("+")]
    if anidb < 1:
        problems.append(f"pair {token!r}: AniDB episode below 1 (entry excluded)")
        return None
    if len(parts) > 1 and 0 in parts:
        problems.append(
            f"pair {token!r}: TVDB 0 cannot combine with other episodes (entry excluded)"
        )
        return None
    tvdb = () if parts == [0] else tuple(sorted(set(parts)))
    return EpisodePair(anidb, tvdb)


# ------------------------------------------------------------------- index build


def build_mapping_index(parsed: ParsedMappings, overrides: Overrides | None = None) -> MappingIndex:
    """Invert parsed entries into the immutable reverse index, then apply operator overrides.

    Overrides are applied after upstream inversion: added pairs rank above
    upstream explicit pairs, removals subtract targets. Overrides contradicting
    upstream data are applied **and** reported in ``override_conflicts``.
    """
    explicit: dict[QueryKey, list[MappingTarget]] = {}
    season_zero: dict[QueryKey, list[MappingTarget]] = {}
    ranges: dict[SeasonKey, list[_RangeRule]] = {}
    entry_offsets: dict[SeasonKey, list[_OffsetClaim]] = {}
    absolute: dict[int, list[_AbsoluteClaim]] = {}
    no_counterpart: dict[SeasonKey, set[TargetIdentity]] = {}
    forward: dict[TargetIdentity, list[_ForwardTarget]] = {}
    forward_precedence: dict[TargetIdentity, int] = {}
    counters = {
        "rules": 0,
        "pairs": 0,
        "no_counterpart": 0,
        "ranges": 0,
        "absolute": 0,
        "range_keys": 0,
    }
    for entry in parsed.entries:
        _index_entry(
            entry,
            explicit,
            season_zero,
            ranges,
            entry_offsets,
            absolute,
            no_counterpart,
            forward,
            forward_precedence,
            counters,
        )
    if counters["range_keys"] > MAX_AGGREGATE_RANGE_KEYS:
        raise MappingError(
            f"indexed range rules expand {counters['range_keys']} enumerable keys, above the "
            f"aggregate cap of {MAX_AGGREGATE_RANGE_KEYS} (fail closed)"
        )

    index = MappingIndex(
        explicit=MappingProxyType({key: tuple(value) for key, value in explicit.items()}),
        season_zero=MappingProxyType({key: tuple(value) for key, value in season_zero.items()}),
        ranges=MappingProxyType({key: tuple(value) for key, value in ranges.items()}),
        entry_offsets=MappingProxyType({key: tuple(value) for key, value in entry_offsets.items()}),
        absolute=MappingProxyType({key: tuple(value) for key, value in absolute.items()}),
        no_counterpart=MappingProxyType(
            {key: frozenset(value) for key, value in no_counterpart.items()}
        ),
        forward=MappingProxyType({key: tuple(value) for key, value in forward.items()}),
        forward_precedence=MappingProxyType(dict(forward_precedence)),
        override_adds=MappingProxyType({}),
        override_removes=MappingProxyType({}),
        override_conflicts=(),
        unsafe_tvdb_ids=frozenset(
            problem.tvdbid
            for problem in parsed.entry_problems
            if problem.excluded and problem.tvdbid is not None
        ),
        unsafe_tvdb_keys=frozenset(
            key for problem in parsed.entry_problems for key in problem.fenced_tvdb_keys
        ),
        unsafe_all=any(
            problem.excluded and problem.tvdbid is None for problem in parsed.entry_problems
        ),
        entries_indexed=len(parsed.entries),
        entries_excluded=parsed.excluded_entries,
        entries_with_problems=len(parsed.entry_problems),
        mapping_rules=counters["rules"],
        explicit_pairs=counters["pairs"],
        no_counterpart_pairs=counters["no_counterpart"],
        range_rules=counters["ranges"],
        absolute_entries=counters["absolute"],
    )
    if overrides is None:
        return index
    return _apply_overrides(index, overrides)


def _index_entry(
    entry: AnimeEntry,
    explicit: dict[QueryKey, list[MappingTarget]],
    season_zero: dict[QueryKey, list[MappingTarget]],
    ranges: dict[SeasonKey, list[_RangeRule]],
    entry_offsets: dict[SeasonKey, list[_OffsetClaim]],
    absolute: dict[int, list[_AbsoluteClaim]],
    no_counterpart: dict[SeasonKey, set[TargetIdentity]],
    forward: dict[TargetIdentity, list[_ForwardTarget]],
    forward_precedence: dict[TargetIdentity, int],
    counters: dict[str, int],
) -> None:
    covered: set[tuple[int, EpisodeType]] = set()
    intervals: list[tuple[int, int, int]] = []
    for rule in entry.rules:
        counters["rules"] += 1
        _index_rule_pairs(
            entry,
            rule,
            explicit,
            season_zero,
            no_counterpart,
            forward,
            forward_precedence,
            covered,
            counters,
        )
        if rule.offset is not None and rule.start is not None and rule.end is not None:
            counters["ranges"] += 1
            counters["range_keys"] += rule.end - rule.start + 1
            intervals.append((rule.start, rule.end, rule.anidbseason))
            ranges.setdefault((entry.tvdbid, rule.tvdbseason), []).append(
                _RangeRule(
                    anidb_id=entry.anidbid,
                    anidbseason=rule.anidbseason,
                    start=rule.start,
                    end=rule.end,
                    offset=rule.offset,
                    origin=(
                        f"entry {entry.index} <{rule.origin}> range [{rule.start},{rule.end}] "
                        f"offset {rule.offset}"
                    ),
                )
            )
            for anidb in range(rule.start, rule.end + 1):
                tvdb = anidb + rule.offset
                episode_type = _episode_type(rule.anidbseason, anidb)
                if tvdb >= 1 and episode_type is not None:
                    _record_forward_precedence(
                        forward_precedence,
                        (entry.anidbid, anidb, episode_type),
                        _PRECEDENCE_RANK[SourceRule.MAPPING_RANGE_OFFSET],
                    )
                    forward.setdefault((entry.anidbid, anidb, episode_type), []).append(
                        _ForwardTarget(entry.tvdbid, rule.tvdbseason, (tvdb,))
                    )

    if entry.defaulttvdbseason == "a":
        counters["absolute"] += 1
        absolute.setdefault(entry.tvdbid, []).append(
            _AbsoluteClaim(
                anidb_id=entry.anidbid,
                covered=frozenset(covered),
                intervals=tuple(intervals),
                entry_index=entry.index,
            )
        )
        return
    season = entry.defaulttvdbseason
    if not isinstance(season, int):
        return
    entry_offsets.setdefault((entry.tvdbid, season), []).append(
        _OffsetClaim(anidb_id=entry.anidbid, offset=entry.episodeoffset, entry_index=entry.index)
    )


def _index_rule_pairs(
    entry: AnimeEntry,
    rule: MappingRule,
    explicit: dict[QueryKey, list[MappingTarget]],
    season_zero: dict[QueryKey, list[MappingTarget]],
    no_counterpart: dict[SeasonKey, set[TargetIdentity]],
    forward: dict[TargetIdentity, list[_ForwardTarget]],
    forward_precedence: dict[TargetIdentity, int],
    covered: set[tuple[int, EpisodeType]],
    counters: dict[str, int],
) -> None:
    for pair in rule.pairs:
        counters["pairs"] += 1
        target = _target(
            entry.anidbid,
            pair.anidb_episode,
            rule.anidbseason,
            SourceRule.EXPLICIT_PAIR,
            f"entry {entry.index} <{rule.origin}> pair ;{pair.anidb_episode}-"
            f"{'+'.join(str(ep) for ep in pair.tvdb_episodes)};",
        )
        if target is None:
            continue
        identity = target.identity
        covered.add((target.anidb_episode_number, target.episode_type))
        _record_forward_precedence(
            forward_precedence,
            identity,
            _PRECEDENCE_RANK[SourceRule.EXPLICIT_PAIR],
        )
        if not pair.tvdb_episodes:
            counters["no_counterpart"] += 1
            no_counterpart.setdefault((entry.tvdbid, rule.tvdbseason), set()).add(identity)
            continue
        target = replace(target, composite_claim=counters["rules"])
        for tvdb_episode in pair.tvdb_episodes:
            key = (entry.tvdbid, rule.tvdbseason, tvdb_episode)
            explicit.setdefault(key, []).append(target)
            if rule.tvdbseason == SPECIAL_SEASON and rule.anidbseason == SPECIAL_SEASON:
                season_zero.setdefault(key, []).append(target)
        forward.setdefault(identity, []).append(
            _ForwardTarget(entry.tvdbid, rule.tvdbseason, pair.tvdb_episodes)
        )


def _record_forward_precedence(
    precedence: dict[TargetIdentity, int], identity: TargetIdentity, rank: int
) -> None:
    previous = precedence.get(identity)
    if previous is None or rank < previous:
        precedence[identity] = rank


# ------------------------------------------------------------------- querying


def resolve(
    index: MappingIndex,
    tvdb_series_id: int,
    season: int,
    episode: int,
    absolute: int | None = None,
) -> Resolution:
    """Reverse-query one TVDB position; ``AMBIGUOUS``/``CONFLICT`` prohibit writes."""
    return _resolve(index, (tvdb_series_id, season, episode), absolute, apply_overrides=True)


def _resolve(
    index: MappingIndex, key: QueryKey, absolute: int | None, apply_overrides: bool
) -> Resolution:
    tvdb_series, season, episode = key
    if index.unsafe_all or tvdb_series in index.unsafe_tvdb_ids:
        scope = "all TVDB series" if index.unsafe_all else f"TVDB {tvdb_series}"
        return Resolution(
            status=ResolutionStatus.UNMAPPED,
            target=None,
            candidates=(),
            detail=f"{scope} is fenced because an excluded Anime-Lists entry may own its mapping",
        )
    if key in index.unsafe_tvdb_keys:
        return Resolution(
            status=ResolutionStatus.UNMAPPED,
            target=None,
            candidates=(),
            detail=(
                f"TVDB {tvdb_series} S{season}E{episode} is fenced because its explicit "
                "Anime-Lists source episode has an unverified type"
            ),
        )
    if episode == 0:
        declared = index.no_counterpart.get((tvdb_series, season), frozenset())
        listed = ", ".join(
            f"AniDB {anidb} ep {ep} ({episode_type.value})"
            for anidb, ep, episode_type in sorted(declared, key=lambda item: (item[0], item[1]))
        )
        return Resolution(
            status=ResolutionStatus.NO_EQUIVALENT,
            target=None,
            candidates=(),
            detail=(
                f"TVDB {tvdb_series} S{season}E0 is the no-counterpart sentinel; "
                f"declared counterpart-less AniDB episodes: {listed or 'none'}"
            ),
        )

    notes: list[str] = []
    candidates = _upstream_candidates(index, key, absolute, notes)
    kept_candidates = [
        target
        for target in candidates
        if target.precedence_rank
        <= index.forward_precedence.get(target.identity, target.precedence_rank)
    ]
    if len(kept_candidates) < len(candidates):
        notes.append("lower-precedence reverse position suppressed by forward AniDB mapping")
    candidates = kept_candidates
    declared = index.no_counterpart.get((tvdb_series, season), frozenset())
    if declared:
        suppressed = set(declared)
        candidates = [target for target in candidates if target.identity not in suppressed]
    if apply_overrides:
        candidates.extend(index.override_adds.get(key, ()))
        if key in index.override_removes:
            removed = index.override_removes[key]
            if removed is None:
                candidates = []
            else:
                candidates = [target for target in candidates if target.anidb_id not in removed]
            notes.append("operator removals applied")

    return _resolve_candidates(key, candidates, notes)


def _resolve_candidates(
    key: QueryKey, candidates: Sequence[MappingTarget], notes: Sequence[str]
) -> Resolution:
    tvdb_series, season, episode = key
    if not candidates:
        suffix = f" ({'; '.join(notes)})" if notes else ""
        return Resolution(
            status=ResolutionStatus.UNMAPPED,
            target=None,
            candidates=(),
            detail=f"no mapping resolves TVDB {tvdb_series} S{season}E{episode}{suffix}",
        )
    best = min(target.precedence_rank for target in candidates)
    deduped: dict[TargetIdentity, MappingTarget] = {}
    for target in sorted(
        (target for target in candidates if target.precedence_rank == best),
        key=lambda target: (
            target.anidb_id,
            target.anidb_episode_number,
            target.episode_type.value,
            target.origin,
        ),
    ):
        deduped.setdefault(target.identity, target)
    kept = tuple(deduped.values())
    if len(kept) == 1:
        target = kept[0]
        return Resolution(
            status=ResolutionStatus.RESOLVED,
            target=target,
            candidates=(target,),
            detail=f"{target.source_rule.value} via {target.origin}",
        )
    # Preserve the real Episode/Special contradiction for one AniDB number. Trailer/Other
    # and Special/Trailer number collisions are type-local and resolve only when one explicit
    # source declaration authorizes the complete typed composite.
    same_number_identities = {(target.anidb_id, target.anidb_episode_number) for target in kept}
    episode_types = {target.episode_type for target in kept}
    if (
        len(same_number_identities) == 1
        and EpisodeType.EPISODE in episode_types
        and EpisodeType.SPECIAL in episode_types
    ):
        return Resolution(
            status=ResolutionStatus.CONFLICT,
            target=None,
            candidates=tuple(kept),
            detail=(
                f"TVDB {tvdb_series} S{season}E{episode} resolves to AniDB {kept[0].anidb_id} "
                f"episode {kept[0].anidb_episode_number} with contradicting types "
                f"{[target.episode_type.value for target in kept]}"
            ),
        )
    if _is_explicit_composite(candidates, kept, best):
        return Resolution(
            status=ResolutionStatus.RESOLVED,
            target=None,
            candidates=kept,
            detail="explicit composite parts via " + "; ".join(t.origin for t in kept),
        )
    return Resolution(
        status=ResolutionStatus.AMBIGUOUS,
        target=None,
        candidates=tuple(kept),
        detail=(
            f"TVDB {tvdb_series} S{season}E{episode} has {len(kept)} unresolvable candidates "
            f"at precedence rank {best}; writes prohibited"
        ),
    )


def _is_explicit_composite(
    candidates: Sequence[MappingTarget], kept: Sequence[MappingTarget], best: int
) -> bool:
    claims: dict[int, set[TargetIdentity]] = {}
    for candidate in candidates:
        if candidate.precedence_rank != best:
            continue
        if candidate.composite_claim is None:
            return False
        claims.setdefault(candidate.composite_claim, set()).add(candidate.identity)
    # Independent declarations must agree on the ENTIRE typed composite, not merely
    # overlap. Cross-anime and competing type-local claims remain ambiguous.
    return (
        bool(claims)
        and all(values == {target.identity for target in kept} for values in claims.values())
        and len({target.anidb_id for target in kept}) == 1
        and len({target.identity for target in kept}) == len(kept)
    )


def _upstream_candidates(
    index: MappingIndex, key: QueryKey, absolute: int | None, notes: list[str]
) -> list[MappingTarget]:
    tvdb_series, season, episode = key
    if season == SPECIAL_SEASON:
        return _season_zero_candidates(index, key)
    candidates: list[MappingTarget] = list(index.explicit.get(key, ()))
    for rule in index.ranges.get((tvdb_series, season), ()):
        anidb = episode - rule.offset
        if rule.start <= anidb <= rule.end:
            target = _target(
                rule.anidb_id,
                anidb,
                rule.anidbseason,
                SourceRule.MAPPING_RANGE_OFFSET,
                rule.origin,
            )
            if target is not None:
                candidates.append(target)
    if episode >= 1:
        claims = index.entry_offsets.get((tvdb_series, season), ())
        candidates.extend(_partition_entry_offset(claims, episode, notes))
    if absolute is not None and absolute >= 1:
        candidates.extend(_absolute_candidates(index, tvdb_series, absolute, notes))
    return candidates


def _season_zero_candidates(index: MappingIndex, key: QueryKey) -> list[MappingTarget]:
    """Season 0 resolves only from explicit season-0 mapping-list rules (anidbseason 0).

    OAD-style rules that project regular AniDB episodes into TVDB season 0
    (``anidbseason=1``, ``tvdbseason=0``) are recorded as explicit targets for
    other seasons but never resolve a season-0 query on their own; a season-0
    query needs a unique hit, otherwise it fails closed.
    """
    candidates: list[MappingTarget] = list(index.season_zero.get(key, ()))
    tvdb_series, season, episode = key
    for rule in index.ranges.get((tvdb_series, season), ()):
        if rule.anidbseason != SPECIAL_SEASON:
            continue
        anidb = episode - rule.offset
        if rule.start <= anidb <= rule.end:
            target = _target(
                rule.anidb_id,
                anidb,
                rule.anidbseason,
                SourceRule.MAPPING_RANGE_OFFSET,
                rule.origin,
            )
            if target is not None:
                candidates.append(target)
    return candidates


def _partition_entry_offset(
    claims: Sequence[_OffsetClaim], episode: int, notes: list[str]
) -> list[MappingTarget]:
    """Split-season partition: the entry with the greatest offset strictly below the queried
    TVDB episode owns it (AoT season 4 offsets 0/16/28 partition episodes 1-16/17-28/29+)."""
    eligible = [claim for claim in claims if claim.offset < episode and episode - claim.offset >= 1]
    if not eligible:
        return []
    top = max(claim.offset for claim in eligible)
    owners = sorted(
        (claim for claim in eligible if claim.offset == top), key=lambda claim: claim.anidb_id
    )
    if len(owners) > 1:
        notes.append(
            f"season partition tie: entries {[c.anidb_id for c in owners]} share offset {top}"
        )
    targets: list[MappingTarget] = []
    for claim in owners:
        target = _target(
            claim.anidb_id,
            episode - claim.offset,
            1,
            SourceRule.ENTRY_OFFSET,
            f"entry {claim.entry_index} episodeoffset {claim.offset} (season partition owner)",
        )
        if target is not None:
            targets.append(target)
    return targets


def _absolute_candidates(
    index: MappingIndex, tvdb_series: int, absolute: int, notes: list[str]
) -> list[MappingTarget]:
    targets: list[MappingTarget] = []
    for claim in index.absolute.get(tvdb_series, ()):
        fallback_type = _episode_type(REGULAR_SEASON, absolute)
        if fallback_type is not None and _covers(claim, absolute, fallback_type):
            notes.append(
                f"absolute identity for entry {claim.entry_index} suppressed by its mapping-list"
            )
            continue
        target = _target(
            claim.anidb_id,
            absolute,
            1,
            SourceRule.ABSOLUTE_IDENTITY,
            f"entry {claim.entry_index} defaulttvdbseason='a' absolute identity",
        )
        if target is not None:
            targets.append(target)
    return targets


def _covers(claim: _AbsoluteClaim, anidb_episode: int, episode_type: EpisodeType) -> bool:
    if (anidb_episode, episode_type) in claim.covered:
        return True
    return any(
        start <= anidb_episode <= end and _episode_type(anidbseason, anidb_episode) is episode_type
        for start, end, anidbseason in claim.intervals
    )


def _target(
    anidb_id: int,
    anidb_episode: int,
    anidbseason: int,
    source_rule: SourceRule,
    origin: str,
) -> MappingTarget | None:
    identity = _episode_identity(
        anidbseason,
        anidb_episode,
        explicit_pair=source_rule is SourceRule.EXPLICIT_PAIR,
    )
    if identity is None:
        return None
    episode_number, episode_type = identity
    return MappingTarget(
        anidb_id=anidb_id,
        anidb_episode_number=episode_number,
        episode_type=episode_type,
        source_rule=source_rule,
        precedence_rank=_PRECEDENCE_RANK[source_rule],
        origin=origin,
    )


def _episode_identity(
    anidbseason: int, anidb_episode: int, *, explicit_pair: bool = False
) -> tuple[int, EpisodeType] | None:
    if anidb_episode < 1 or anidbseason not in (SPECIAL_SEASON, REGULAR_SEASON):
        return None
    if explicit_pair and anidb_episode >= OTHER_MIN:
        return anidb_episode - OTHER_MIN + 1, EpisodeType.OTHER
    if explicit_pair and TRAILER_MIN <= anidb_episode <= TRAILER_MAX:
        return anidb_episode - TRAILER_MIN + 1, EpisodeType.TRAILER
    if anidbseason == SPECIAL_SEASON:
        if explicit_pair and UNVERIFIED_MIN <= anidb_episode <= UNVERIFIED_MAX:
            return None
        return anidb_episode, EpisodeType.SPECIAL
    return anidb_episode, EpisodeType.EPISODE


def _episode_type(
    anidbseason: int, anidb_episode: int, *, explicit_pair: bool = False
) -> EpisodeType | None:
    """Return the Shoko type while keeping the documented T/O encoding source-aware."""
    identity = _episode_identity(anidbseason, anidb_episode, explicit_pair=explicit_pair)
    return None if identity is None else identity[1]


# ------------------------------------------------------------------- overrides


def load_overrides(yaml_text: str) -> Overrides:
    """Validate operator override YAML (``safe_load`` only); malformed documents raise
    ConfigError.
    """
    try:
        raw = yaml.safe_load(yaml_text)
    except yaml.YAMLError as exc:
        raise ConfigError(f"overrides file is not valid YAML: {exc}") from exc
    if raw is None:
        return Overrides((), (), None)
    if not isinstance(raw, dict):
        raise ConfigError("overrides: top level must be a mapping with add/remove/notes")
    document = cast("dict[str, Any]", raw)
    for key in document:
        if not isinstance(key, str) or key not in {"add", "remove", "notes"}:
            raise ConfigError(f"overrides: unknown setting {key!r}")
    adds = _override_adds(document.get("add"))
    removes = _override_removes(document.get("remove"))
    notes = document.get("notes")
    if notes is not None and not isinstance(notes, str):
        raise ConfigError("overrides.notes: must be a string")
    return Overrides(adds, removes, notes)


def _override_adds(raw: Any) -> tuple[OverrideAdd, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise ConfigError("overrides.add: must be a list")
    adds: list[OverrideAdd] = []
    for position, item in enumerate(raw):
        field = f"overrides.add[{position}]"
        if not isinstance(item, dict):
            raise ConfigError(f"{field}: expected a mapping with all six fields")
        entry = cast("dict[str, Any]", item)
        _reject_unknown_keys(
            entry, {"tvdb_series", "season", "episode", "anidb_id", "anidb_episode", "type"}, field
        )
        adds.append(
            OverrideAdd(
                tvdb_series=_override_int(entry, "tvdb_series", field, minimum=1),
                season=_override_int(entry, "season", field, minimum=0),
                episode=_override_int(entry, "episode", field, minimum=1),
                anidb_id=_override_int(entry, "anidb_id", field, minimum=1),
                anidb_episode=_override_int(entry, "anidb_episode", field, minimum=1),
                episode_type=_override_type(entry, field),
            )
        )
    return tuple(adds)


def _override_removes(raw: Any) -> tuple[OverrideRemove, ...]:
    if raw is None:
        return ()
    if not isinstance(raw, list):
        raise ConfigError("overrides.remove: must be a list")
    removes: list[OverrideRemove] = []
    for position, item in enumerate(raw):
        field = f"overrides.remove[{position}]"
        if not isinstance(item, dict):
            raise ConfigError(f"{field}: expected a mapping")
        entry = cast("dict[str, Any]", item)
        _reject_unknown_keys(entry, {"tvdb_series", "season", "episode", "anidb_id"}, field)
        anidb_id = entry.get("anidb_id")
        removes.append(
            OverrideRemove(
                tvdb_series=_override_int(entry, "tvdb_series", field, minimum=1),
                season=_override_int(entry, "season", field, minimum=0),
                episode=_override_int(entry, "episode", field, minimum=1),
                anidb_id=_checked_positive(anidb_id, f"{field}.anidb_id")
                if anidb_id is not None
                else None,
            )
        )
    return tuple(removes)


def _reject_unknown_keys(entry: Mapping[str, Any], allowed: Iterable[str], field: str) -> None:
    allowed_keys = frozenset(allowed)
    for key in entry:
        if key not in allowed_keys:
            raise ConfigError(f"{field}: unknown setting {key!r}")


def _override_int(entry: Mapping[str, Any], name: str, field: str, *, minimum: int) -> int:
    raw = entry.get(name)
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise ConfigError(f"{field}.{name}: required whole number")
    if raw < minimum:
        raise ConfigError(f"{field}.{name}: must be at least {minimum}")
    return raw


def _checked_positive(raw: Any, field: str) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 1:
        raise ConfigError(f"{field}: must be a whole number of at least 1")
    return int(raw)


def _override_type(entry: Mapping[str, Any], field: str) -> EpisodeType:
    raw = entry.get("type")
    if not isinstance(raw, str) or raw not in _OVERRIDABLE_TYPES:
        allowed = ", ".join(sorted(_OVERRIDABLE_TYPES))
        raise ConfigError(
            f"{field}.type: must be one of {allowed} (Credits/Parody are not mappable targets)"
        )
    return _OVERRIDABLE_TYPES[raw]


def _apply_overrides(index: MappingIndex, overrides: Overrides) -> MappingIndex:
    adds: dict[QueryKey, list[MappingTarget]] = {}
    removes: dict[QueryKey, frozenset[int] | None] = {}
    conflicts: list[OverrideConflict] = []

    for position, add in enumerate(overrides.adds):
        key = (add.tvdb_series, add.season, add.episode)
        target = MappingTarget(
            anidb_id=add.anidb_id,
            anidb_episode_number=add.anidb_episode,
            episode_type=add.episode_type,
            source_rule=SourceRule.OVERRIDE,
            precedence_rank=_PRECEDENCE_RANK[SourceRule.OVERRIDE],
            origin=f"operator override add #{position}",
        )
        declared = index.no_counterpart.get(key[:2], frozenset())
        if (add.anidb_id, add.anidb_episode, add.episode_type) in declared:
            conflicts.append(
                OverrideConflict(
                    kind="add-no-counterpart",
                    tvdb_series=key[0],
                    season=key[1],
                    episode=key[2],
                    detail=(
                        f"override add #{position} targets {target.identity}, which upstream "
                        "declares as having no TVDB counterpart: refused (fail closed)"
                    ),
                )
            )
            continue
        adds.setdefault(key, []).append(target)
        upstream = _upstream_identities(index, key)
        if not upstream:
            conflicts.append(
                OverrideConflict(
                    kind="add-gap",
                    tvdb_series=key[0],
                    season=key[1],
                    episode=key[2],
                    detail=f"override add #{position} fills unmapped {key[0]} S{key[1]}E{key[2]}",
                )
            )
        elif upstream != {target.identity}:
            conflicts.append(
                OverrideConflict(
                    kind="add",
                    tvdb_series=key[0],
                    season=key[1],
                    episode=key[2],
                    detail=(
                        f"override add #{position} targets {target.identity}; "
                        f"upstream resolves {_format_identities(upstream)}; operator intent applied"
                    ),
                )
            )

    for position, remove in enumerate(overrides.removes):
        key = (remove.tvdb_series, remove.season, remove.episode)
        if remove.anidb_id is None:
            removes[key] = None
        elif key not in removes:
            removes[key] = frozenset({remove.anidb_id})
        elif removes[key] is not None:
            removes[key] = frozenset({remove.anidb_id}) | cast("frozenset[int]", removes[key])
        upstream = _upstream_identities(index, key)
        matched = [
            identity
            for identity in upstream
            if remove.anidb_id is None or identity[0] == remove.anidb_id
        ]
        if not matched:
            names = "all targets" if remove.anidb_id is None else f"AniDB {remove.anidb_id}"
            conflicts.append(
                OverrideConflict(
                    kind="remove-noop",
                    tvdb_series=key[0],
                    season=key[1],
                    episode=key[2],
                    detail=f"remove #{position} {names} of S{key[1]}E{key[2]} absent upstream",
                )
            )

    return MappingIndex(
        explicit=index.explicit,
        season_zero=index.season_zero,
        ranges=index.ranges,
        entry_offsets=index.entry_offsets,
        absolute=index.absolute,
        no_counterpart=index.no_counterpart,
        forward=index.forward,
        forward_precedence=index.forward_precedence,
        override_adds=MappingProxyType({key: tuple(value) for key, value in adds.items()}),
        override_removes=MappingProxyType(dict(removes)),
        override_conflicts=tuple(conflicts),
        unsafe_tvdb_ids=index.unsafe_tvdb_ids,
        unsafe_tvdb_keys=index.unsafe_tvdb_keys,
        unsafe_all=index.unsafe_all,
        entries_indexed=index.entries_indexed,
        entries_excluded=index.entries_excluded,
        entries_with_problems=index.entries_with_problems,
        mapping_rules=index.mapping_rules,
        explicit_pairs=index.explicit_pairs,
        no_counterpart_pairs=index.no_counterpart_pairs,
        range_rules=index.range_rules,
        absolute_entries=index.absolute_entries,
    )


def _upstream_identities(index: MappingIndex, key: QueryKey) -> frozenset[TargetIdentity]:
    resolution = _resolve(index, key, absolute=None, apply_overrides=False)
    if resolution.status in (
        ResolutionStatus.RESOLVED,
        ResolutionStatus.AMBIGUOUS,
        ResolutionStatus.CONFLICT,
    ):
        return frozenset(target.identity for target in resolution.candidates)
    return frozenset()


def _format_identities(identities: Iterable[TargetIdentity]) -> str:
    ordered = sorted(identities, key=lambda item: (item[0], item[1], item[2].value))
    return (
        "{"
        + ", ".join(f"{anidb}:{episode}:{type_.value}" for anidb, episode, type_ in ordered)
        + "}"
    )


# ------------------------------------------------------------------ digests/stats


def effective_targets(index: MappingIndex) -> Mapping[int, frozenset[TargetIdentity]]:
    """Canonical effective resolved target set per TVDB series (change-detection input).

    Only enumerable keys participate: explicit pairs, bounded range offsets, and
    operator additions. Unbounded identity rules (entry offsets, absolute
    identity) resolve at query time and are reported by :func:`index_stats`, not
    in the target digest. ``AMBIGUOUS``/``CONFLICT`` keys contribute no target.
    """
    keys = _enumerable_keys(index)
    per_series: dict[int, set[TargetIdentity]] = {}
    for key in sorted(keys):
        resolution = _resolve(index, key, absolute=None, apply_overrides=True)
        if resolution.status is not ResolutionStatus.RESOLVED:
            continue
        for target in resolution.candidates:
            per_series.setdefault(key[0], set()).add(target.identity)
    return MappingProxyType(
        {series: frozenset(identities) for series, identities in per_series.items()}
    )


def targets_digest(index: MappingIndex) -> str:
    """SHA-256 of canonical source→target semantics (including composite parts)."""
    lines: list[str] = []
    for key in sorted(_enumerable_keys(index)):
        for target in resolve(index, *key).targets:
            lines.append(
                f"{key[0]}|{key[1]}|{key[2]}|{target.anidb_id}|"
                f"{target.anidb_episode_number}|{target.episode_type.value}"
            )
    lines.extend(f"X|{tvdb_series}" for tvdb_series in sorted(index.unsafe_tvdb_ids))
    lines.extend(
        f"F|{tvdb_series}|{season}|{episode}"
        for tvdb_series, season, episode in sorted(index.unsafe_tvdb_keys)
    )
    if index.unsafe_all:
        lines.append("X|ALL")
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


def effective_source_targets(index: MappingIndex) -> Mapping[int, frozenset[str]]:
    """Enumerable source→target semantics per series, retaining source positions."""
    result: dict[int, set[str]] = {}
    for key in sorted(_enumerable_keys(index)):
        for target in resolve(index, *key).targets:
            result.setdefault(key[0], set()).add(
                f"{key[1]}|{key[2]}|{target.anidb_id}|"
                f"{target.anidb_episode_number}|{target.episode_type.value}"
            )
    return MappingProxyType({series: frozenset(lines) for series, lines in result.items()})


def legacy_targets_digest(index: MappingIndex) -> str:
    """Validate existing cache metadata written before source-aware digests.

    The old algorithm excluded composites and discarded source positions. It is
    accepted only as cache provenance alongside the full content/structure checks;
    drift decisions use source-aware semantics and immutable file destinations.
    """
    per_series: dict[int, set[TargetIdentity]] = {}
    for key in _enumerable_keys(index):
        answer = resolve(index, *key)
        if len(answer.targets) == 1:
            per_series.setdefault(key[0], set()).add(answer.targets[0].identity)
    lines = [
        f"{series}|{anime}|{episode}|{episode_type.value}"
        for series, identities in sorted(per_series.items())
        for anime, episode, episode_type in sorted(
            identities, key=lambda item: (item[0], item[1], item[2].value)
        )
    ]
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


def content_digest(xml_bytes: bytes | bytearray) -> str:
    """SHA-256 of raw mapping bytes (source provenance, not target semantics)."""
    return hashlib.sha256(xml_bytes).hexdigest()


def structure_digest(index: MappingIndex) -> str:
    """SHA-256 over the canonical rule structure, including unbounded identity rules.

    :func:`targets_digest` covers only enumerable resolved targets; entry-offset
    and absolute-identity claims are unbounded and resolve at query time, so a
    refresh that changes an offset (e.g. a split-season ``episodeoffset``) moves
    this digest even when the enumerable target set is unchanged.
    """
    lines = _structure_explicit_lines(index)
    lines.extend(_structure_range_lines(index))
    lines.extend(_structure_identity_lines(index))
    lines.extend(_structure_declaration_lines(index))
    lines.extend(_structure_override_lines(index))
    return hashlib.sha256("\n".join(sorted(lines)).encode("utf-8")).hexdigest()


def _structure_explicit_lines(index: MappingIndex) -> list[str]:
    lines: list[str] = []
    for key, targets in index.explicit.items():
        for target in targets:
            lines.append(
                f"E|{key[0]}|{key[1]}|{key[2]}|{target.anidb_id}"
                f"|{target.anidb_episode_number}|{target.episode_type.value}"
            )
    return lines


def _structure_range_lines(index: MappingIndex) -> list[str]:
    lines: list[str] = []
    for (tvdb_series, season), rules in index.ranges.items():
        for rule in rules:
            lines.append(
                f"R|{tvdb_series}|{season}|{rule.anidb_id}|{rule.anidbseason}"
                f"|{rule.start}|{rule.end}|{rule.offset}"
            )
    return lines


def _structure_identity_lines(index: MappingIndex) -> list[str]:
    lines: list[str] = []
    for (tvdb_series, season), offset_claims in index.entry_offsets.items():
        for offset_claim in offset_claims:
            lines.append(f"O|{tvdb_series}|{season}|{offset_claim.anidb_id}|{offset_claim.offset}")
    for tvdb_series, absolute_claims in index.absolute.items():
        for absolute_claim in absolute_claims:
            covered = ",".join(
                f"{episode}:{episode_type.value}"
                for episode, episode_type in sorted(
                    absolute_claim.covered, key=lambda item: (item[0], item[1].value)
                )
            )
            spans = ",".join(
                f"{start}-{end}@{anidbseason}"
                for start, end, anidbseason in sorted(absolute_claim.intervals)
            )
            lines.append(f"A|{tvdb_series}|{absolute_claim.anidb_id}|{covered}|{spans}")
    return lines


def _structure_declaration_lines(index: MappingIndex) -> list[str]:
    lines: list[str] = []
    for (tvdb_series, season), declared in index.no_counterpart.items():
        ordered = sorted(declared, key=lambda item: (item[0], item[1], item[2].value))
        for anidb_id, episode, episode_type in ordered:
            lines.append(f"N|{tvdb_series}|{season}|{anidb_id}|{episode}|{episode_type.value}")
    lines.extend(f"X|{tvdb_series}" for tvdb_series in sorted(index.unsafe_tvdb_ids))
    lines.extend(
        f"F|{tvdb_series}|{season}|{episode}"
        for tvdb_series, season, episode in sorted(index.unsafe_tvdb_keys)
    )
    if index.unsafe_all:
        lines.append("X|ALL")
    return lines


def _structure_override_lines(index: MappingIndex) -> list[str]:
    lines: list[str] = []
    for key, targets in index.override_adds.items():
        for target in targets:
            lines.append(
                f"+|{key[0]}|{key[1]}|{key[2]}|{target.anidb_id}"
                f"|{target.anidb_episode_number}|{target.episode_type.value}"
            )
    for key, removed in index.override_removes.items():
        names = "all" if removed is None else ",".join(str(item) for item in sorted(removed))
        lines.append(f"-|{key[0]}|{key[1]}|{key[2]}|{names}")
    return lines


@dataclass(frozen=True)
class IndexStats:
    """Counts for provenance reporting and change detection."""

    entries_indexed: int
    entries_excluded: int
    entries_with_problems: int
    mapping_rules: int
    explicit_pairs: int
    no_counterpart_pairs: int
    range_rules: int
    absolute_entries: int
    entry_offset_season_keys: int
    tvdb_series_with_targets: int
    resolved_target_keys: int
    ambiguous_keys: int
    conflict_keys: int
    override_adds: int
    override_removes: int
    override_conflicts: int


def index_stats(index: MappingIndex) -> IndexStats:
    """Summarize an index: entry/key counts, ambiguity, conflicts, override activity."""
    keys = _enumerable_keys(index)
    resolved = 0
    ambiguous = 0
    conflicts = 0
    series_with_targets: set[int] = set()
    for key in sorted(keys):
        status = _resolve(index, key, absolute=None, apply_overrides=True).status
        if status is ResolutionStatus.RESOLVED:
            resolved += 1
            series_with_targets.add(key[0])
        elif status is ResolutionStatus.AMBIGUOUS:
            ambiguous += 1
        elif status is ResolutionStatus.CONFLICT:
            conflicts += 1
    return IndexStats(
        entries_indexed=index.entries_indexed,
        entries_excluded=index.entries_excluded,
        entries_with_problems=index.entries_with_problems,
        mapping_rules=index.mapping_rules,
        explicit_pairs=index.explicit_pairs,
        no_counterpart_pairs=index.no_counterpart_pairs,
        range_rules=index.range_rules,
        absolute_entries=index.absolute_entries,
        entry_offset_season_keys=len(index.entry_offsets),
        tvdb_series_with_targets=len(series_with_targets),
        resolved_target_keys=resolved,
        ambiguous_keys=ambiguous,
        conflict_keys=conflicts,
        override_adds=sum(len(items) for items in index.override_adds.values()),
        override_removes=len(index.override_removes),
        override_conflicts=len(index.override_conflicts),
    )


def _enumerable_keys(index: MappingIndex) -> set[QueryKey]:
    keys: set[QueryKey] = set(index.explicit)
    for (tvdb_series, season), rules in index.ranges.items():
        for rule in rules:
            for anidb in range(rule.start, rule.end + 1):
                tvdb_episode = anidb + rule.offset
                if tvdb_episode >= 1:
                    keys.add((tvdb_series, season, tvdb_episode))
    keys.update(index.override_adds)
    return keys
