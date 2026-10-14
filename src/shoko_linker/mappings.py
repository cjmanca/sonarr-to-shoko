"""Anime-Lists XML parsing, reverse TVDB→AniDB inversion, overrides, provenance digests.

Pure and offline: consumes XML text/bytes and produces an immutable
:class:`MappingIndex` a caller can atomically activate. No network I/O happens
here; the runtime download/refresh layer is a separate module.

Contract source: ``docs/compatibility.md`` section 3 (Anime-Lists mapping contract).
Semantics implemented verbatim:

* Offsets are AniDB→TVDB additions (``tvdb = anidb + offset``), reversed as
  ``anidb = tvdb - offset``.
* Precedence: individual ``;a-b;`` pairs > mapping range ``offset`` > entry
  ``episodeoffset``; ``start``/``end`` without ``offset`` is inert.
* TVDB ``0`` in a target position means "AniDB episode has no TVDB counterpart"
  and is never registered as a reverse key.
* ``;1-1+2;`` registers both TVDB keys for one AniDB episode; ``;1-1;2-1;``
  registers one TVDB key with two candidate AniDB episodes; ``;1+2-1;`` is
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
* Episode type is derived (number wins over season): ``>=401`` → Other,
  ``201-400`` → Trailer, ``anidbseason 1`` → Episode, ``0`` → Special;
  ``101-103`` is UNVERIFIED and fails closed; Credits/Parody are not
  expressible by Anime-Lists and are never produced.

Recorded fail-closed interpretations (reported as problems, never guessed):

* A ``<mapping>`` without ``tvdbseason``/``anidbseason`` has no defined season
  pair → rule excluded. ``offset`` without ``[start,end]`` is unbounded →
  excluded. ``anidbseason`` ``2`` **or higher** is UNVERIFIED → the whole rule
  (pairs and range) is excluded. Pair numbers in ``101-103`` are excluded.
  Unknown attributes are recorded, never interpreted.
* A reverse query for TVDB episode ``0`` answers ``NO_EQUIVALENT`` (the
  Anime-Lists no-counterpart sentinel), never ``UNMAPPED``.
* A mapping body above ``MAX_PAIRS_PER_RULE`` excludes the entry: a partially
  applied pair set is never trusted.
* Operator override **adds** never authorize a target that upstream declares as
  having no TVDB counterpart (``;a-0;``): such adds are refused and reported as
  ``add-no-counterpart`` conflicts — upstream explicit declarations outrank
  operator additions for the exact declared identity.

The reverse map is not 1:1: candidate lists are retained; more than one
unresolvable candidate answers ``AMBIGUOUS`` (writes prohibited). Candidate
ordering is deterministic: sorted by AniDB id, then episode number, then type.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
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
_PAIR_RE: Final[re.Pattern[str]] = re.compile(r"^(\d+)-(\d+(?:\+\d+)*)$")
_LEFT_MULTI_RE: Final[re.Pattern[str]] = re.compile(r"^\d+\+\d+")
_INT_RE: Final[re.Pattern[str]] = re.compile(r"^\d+$")
_SIGNED_INT_RE: Final[re.Pattern[str]] = re.compile(r"^-?\d+$")


class MappingError(LinkerError):
    """The document as a whole is unusable: root, XML syntax, entry cap, defused XML threats."""


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
    """Recorded problems for one ``<anime>`` element; ``excluded`` entries contribute nothing."""

    index: int
    anidbid: int | None
    tvdbid: int | None
    excluded: bool
    problems: tuple[str, ...]


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
    covered: frozenset[int]
    intervals: tuple[tuple[int, int], ...]
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
    forward: Mapping[tuple[int, int, int], tuple[_ForwardTarget, ...]]
    override_adds: Mapping[QueryKey, tuple[MappingTarget, ...]]
    override_removes: Mapping[QueryKey, frozenset[int] | None]
    override_conflicts: tuple[OverrideConflict, ...]
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
    seen_anidbid: dict[int, int] = {}
    for index, element in enumerate(anime_elements):
        entry, problem = _parse_anime_element(index, element, seen_anidbid)
        if entry is not None:
            entries.append(entry)
            seen_anidbid[entry.anidbid] = index
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
    index: int, element: Element, seen_anidbid: Mapping[int, int]
) -> tuple[AnimeEntry | None, EntryProblem | None]:
    fatal: list[str] = []
    problems: list[str] = []
    attributes = dict(element.attrib)
    allowed = _ANIME_ENTRY_ATTRIBUTES | _ANIME_UNUSED_ATTRIBUTES
    unknown = tuple(name for name in sorted(attributes) if name not in allowed)
    for name in unknown:
        problems.append(f"unknown attribute {name!r} on <anime> (recorded, not used)")

    anidbid = _positive_int(attributes.get("anidbid"), "anidbid", fatal)
    tvdbid = _positive_int(attributes.get("tvdbid"), "tvdbid", fatal)
    season = _season_attr(attributes.get("defaulttvdbseason"), fatal)
    episodeoffset = _offset_attr(attributes.get("episodeoffset"), fatal)

    if season == "a" and "episodeoffset" in attributes:
        fatal.append("defaulttvdbseason='a' with episodeoffset is UNVERIFIED (fail closed)")

    rules = _parse_entry_rules(element, problems, fatal)

    if anidbid is not None and anidbid in seen_anidbid:
        fatal.append(f"anidbid {anidbid} already claimed by entry {seen_anidbid[anidbid]}")

    if fatal:
        return None, EntryProblem(index, anidbid, tvdbid, True, tuple(fatal + problems))
    if anidbid is None or tvdbid is None or not isinstance(season, int | str):
        return None, EntryProblem(index, anidbid, tvdbid, True, tuple(fatal + problems))
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
        return entry, EntryProblem(index, anidbid, tvdbid, False, tuple(problems))
    return entry, None


def _positive_int(raw: str | None, name: str, fatal: list[str]) -> int | None:
    if raw is None:
        fatal.append(f"missing {name}")
        return None
    if not _INT_RE.match(raw):
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
    if not _INT_RE.match(raw):
        fatal.append(f"invalid defaulttvdbseason {raw!r}")
        return None
    return int(raw)


def _offset_attr(raw: str | None, fatal: list[str]) -> int | None:
    if raw is None:
        return None
    if not _SIGNED_INT_RE.match(raw):
        fatal.append(f"invalid episodeoffset {raw!r}")
        return None
    return int(raw)


def _parse_entry_rules(
    element: Element, problems: list[str], fatal: list[str]
) -> tuple[MappingRule, ...]:
    rules: list[MappingRule] = []
    for child in element:
        tag = child.tag
        if tag in _METADATA_CHILDREN:
            continue
        if tag in ("mapping-list", "before"):
            rules.extend(_parse_rule_container(tag, child, problems, fatal))
        else:
            problems.append(f"unknown element <{tag}> inside <anime> (ignored)")
    return tuple(rules)


def _parse_rule_container(
    origin: str, container: Element, problems: list[str], fatal: list[str]
) -> Iterable[MappingRule]:
    for mapping in container:
        if not isinstance(mapping.tag, str) or mapping.tag != "mapping":
            problems.append(f"non-<mapping> element inside <{origin}> (ignored)")
            continue
        rule = _parse_mapping_element(origin, mapping, problems, fatal)
        if rule is not None:
            yield rule


def _parse_mapping_element(
    origin: str, mapping: Element, problems: list[str], fatal: list[str]
) -> MappingRule | None:
    attributes = dict(mapping.attrib)
    rule_problems: list[str] = []
    unknown = sorted(set(attributes) - _MAPPING_ATTRIBUTES)
    for name in unknown:
        rule_problems.append(
            f"unknown attribute {name!r} on <mapping> (rule excluded, not guessed)"
        )
    if len(mapping) > 0:
        rule_problems.append("nested element inside <mapping> (rule excluded)")

    anidbseason = _mapping_int(attributes.get("anidbseason"), "anidbseason", rule_problems)
    tvdbseason = _mapping_int(attributes.get("tvdbseason"), "tvdbseason", rule_problems)
    start = _optional_mapping_int(attributes.get("start"), "start", rule_problems)
    end = _optional_mapping_int(attributes.get("end"), "end", rule_problems)
    offset = _mapping_signed_int(attributes.get("offset"), "offset", rule_problems)

    pairs, pair_fatal = _parse_pair_body("".join(mapping.itertext()), rule_problems)
    if pair_fatal:
        fatal.extend(rule_problems)
        fatal.append(f"<mapping> in <{origin}> has an invalid pair (entry excluded)")
        return None

    bounds = _validated_range(anidbseason, start, end, offset, rule_problems)
    problems.extend(rule_problems)
    if unknown or anidbseason is None or tvdbseason is None:
        return None
    if anidbseason >= UNVERIFIED_ANIDBSEASON:
        problems.append(
            f"mapping anidbseason={anidbseason} semantics are UNVERIFIED (whole rule excluded)"
        )
        return None
    start_ok, end_ok, offset_ok = bounds
    return MappingRule(
        origin=origin,
        anidbseason=anidbseason,
        tvdbseason=tvdbseason,
        start=start_ok,
        end=end_ok,
        offset=offset_ok,
        pairs=pairs,
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
    if not _INT_RE.match(raw):
        problems.append(f"mapping invalid {name} {raw!r}")
        return None
    return int(raw)


def _optional_mapping_int(raw: str | None, name: str, problems: list[str]) -> int | None:
    if raw is None:
        return None
    if not _INT_RE.match(raw):
        problems.append(f"mapping invalid {name} {raw!r}")
        return None
    return int(raw)


def _mapping_signed_int(raw: str | None, name: str, problems: list[str]) -> int | None:
    if raw is None:
        return None
    if not _SIGNED_INT_RE.match(raw):
        problems.append(f"mapping invalid {name} {raw!r}")
        return None
    return int(raw)


def _parse_pair_body(body: str, problems: list[str]) -> tuple[tuple[EpisodePair, ...], bool]:
    pairs: list[EpisodePair] = []
    seen: set[EpisodePair] = set()
    tokens = [item.strip() for item in body.split(";") if item.strip()]
    if len(tokens) > MAX_PAIRS_PER_RULE:
        problems.append(
            f"mapping body has {len(tokens)} pairs, above the cap of {MAX_PAIRS_PER_RULE} "
            "(entry excluded: a partially applied pair set is never trusted)"
        )
        return (), True
    for token in tokens:
        if _LEFT_MULTI_RE.match(token):
            problems.append(f"pair {token!r}: AniDB-side multi-episode invalid (entry rejected)")
            return tuple(pairs), True
        pair = _parse_pair_token(token, problems)
        if pair is None or pair in seen:
            continue
        seen.add(pair)
        pairs.append(pair)
    return tuple(pairs), False


def _parse_pair_token(token: str, problems: list[str]) -> EpisodePair | None:
    match = _PAIR_RE.fullmatch(token)
    if match is None:
        problems.append(f"invalid pair token {token!r} (ignored)")
        return None
    anidb = int(match.group(1))
    parts = [int(item) for item in match.group(2).split("+")]
    if anidb < 1:
        problems.append(f"pair {token!r}: AniDB episode below 1 (ignored)")
        return None
    if UNVERIFIED_MIN <= anidb <= UNVERIFIED_MAX:
        problems.append(f"pair {token!r}: AniDB {anidb} in 101-103 range (UNVERIFIED, excluded)")
        return None
    if len(parts) > 1 and 0 in parts:
        problems.append(f"pair {token!r}: TVDB 0 cannot combine with other episodes (ignored)")
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
    forward: dict[tuple[int, int, int], list[_ForwardTarget]] = {}
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
        override_adds=MappingProxyType({}),
        override_removes=MappingProxyType({}),
        override_conflicts=(),
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
    forward: dict[tuple[int, int, int], list[_ForwardTarget]],
    counters: dict[str, int],
) -> None:
    covered: set[int] = set()
    intervals: list[tuple[int, int]] = []
    for rule in entry.rules:
        counters["rules"] += 1
        _index_rule_pairs(
            entry, rule, explicit, season_zero, no_counterpart, forward, covered, counters
        )
        if rule.offset is not None and rule.start is not None and rule.end is not None:
            counters["ranges"] += 1
            counters["range_keys"] += rule.end - rule.start + 1
            intervals.append((rule.start, rule.end))
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
                if tvdb >= 1 and _episode_type(rule.anidbseason, anidb) is not None:
                    forward.setdefault((entry.anidbid, rule.anidbseason, anidb), []).append(
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
    forward: dict[tuple[int, int, int], list[_ForwardTarget]],
    covered: set[int],
    counters: dict[str, int],
) -> None:
    for pair in rule.pairs:
        counters["pairs"] += 1
        covered.add(pair.anidb_episode)
        if not pair.tvdb_episodes:
            counters["no_counterpart"] += 1
            no_counterpart.setdefault((entry.tvdbid, rule.tvdbseason), set()).add(
                (
                    entry.anidbid,
                    pair.anidb_episode,
                    _episode_type(rule.anidbseason, pair.anidb_episode) or EpisodeType.EPISODE,
                )
            )
            continue
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
        for tvdb_episode in pair.tvdb_episodes:
            key = (entry.tvdbid, rule.tvdbseason, tvdb_episode)
            explicit.setdefault(key, []).append(target)
            if rule.tvdbseason == SPECIAL_SEASON and rule.anidbseason == SPECIAL_SEASON:
                season_zero.setdefault(key, []).append(target)
        forward.setdefault((entry.anidbid, rule.anidbseason, pair.anidb_episode), []).append(
            _ForwardTarget(entry.tvdbid, rule.tvdbseason, pair.tvdb_episodes)
        )


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
    declared = index.no_counterpart.get((tvdb_series, season), frozenset())
    if declared:
        suppressed = {(anidb, episode) for anidb, episode, _ in declared}
        candidates = [
            target
            for target in candidates
            if (target.anidb_id, target.anidb_episode_number) not in suppressed
        ]
    if apply_overrides:
        candidates.extend(index.override_adds.get(key, ()))
        if key in index.override_removes:
            removed = index.override_removes[key]
            if removed is None:
                candidates = []
            else:
                candidates = [target for target in candidates if target.anidb_id not in removed]
            notes.append("operator removals applied")

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
    identities = {(target.anidb_id, target.anidb_episode_number) for target in kept}
    if len(kept) == 1:
        target = kept[0]
        return Resolution(
            status=ResolutionStatus.RESOLVED,
            target=target,
            candidates=(target,),
            detail=f"{target.source_rule.value} via {target.origin}",
        )
    if len(identities) == 1:
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
    return Resolution(
        status=ResolutionStatus.AMBIGUOUS,
        target=None,
        candidates=tuple(kept),
        detail=(
            f"TVDB {tvdb_series} S{season}E{episode} has {len(kept)} unresolvable candidates "
            f"at precedence rank {best}; writes prohibited"
        ),
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
        if _covers(claim, absolute):
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


def _covers(claim: _AbsoluteClaim, anidb_episode: int) -> bool:
    if anidb_episode in claim.covered:
        return True
    return any(start <= anidb_episode <= end for start, end in claim.intervals)


def _target(
    anidb_id: int,
    anidb_episode: int,
    anidbseason: int,
    source_rule: SourceRule,
    origin: str,
) -> MappingTarget | None:
    episode_type = _episode_type(anidbseason, anidb_episode)
    if episode_type is None:
        return None
    return MappingTarget(
        anidb_id=anidb_id,
        anidb_episode_number=anidb_episode,
        episode_type=episode_type,
        source_rule=source_rule,
        precedence_rank=_PRECEDENCE_RANK[source_rule],
        origin=origin,
    )


def _episode_type(anidbseason: int, anidb_episode: int) -> EpisodeType | None:
    if UNVERIFIED_MIN <= anidb_episode <= UNVERIFIED_MAX:
        return None
    if anidb_episode >= OTHER_MIN:
        return EpisodeType.OTHER
    if TRAILER_MIN <= anidb_episode <= TRAILER_MAX:
        return EpisodeType.TRAILER
    if anidbseason == 1:
        return EpisodeType.EPISODE
    if anidbseason == 0:
        return EpisodeType.SPECIAL
    return None


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
        override_adds=MappingProxyType({key: tuple(value) for key, value in adds.items()}),
        override_removes=MappingProxyType(dict(removes)),
        override_conflicts=tuple(conflicts),
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
    """SHA-256 of the canonical effective target set; formatting-only changes never move it."""
    lines: list[str] = []
    for series, identities in sorted(effective_targets(index).items()):
        for anidb_id, episode, episode_type in sorted(
            identities, key=lambda item: (item[0], item[1], item[2].value)
        ):
            lines.append(f"{series}|{anidb_id}|{episode}|{episode_type.value}")
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
            covered = ",".join(str(item) for item in sorted(absolute_claim.covered))
            spans = ",".join(f"{start}-{end}" for start, end in sorted(absolute_claim.intervals))
            lines.append(f"A|{tvdb_series}|{absolute_claim.anidb_id}|{covered}|{spans}")
    return lines


def _structure_declaration_lines(index: MappingIndex) -> list[str]:
    lines: list[str] = []
    for (tvdb_series, season), declared in index.no_counterpart.items():
        ordered = sorted(declared, key=lambda item: (item[0], item[1], item[2].value))
        for anidb_id, episode, episode_type in ordered:
            lines.append(f"N|{tvdb_series}|{season}|{anidb_id}|{episode}|{episode_type.value}")
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
