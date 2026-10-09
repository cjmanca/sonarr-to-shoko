"""Anime-Lists parsing, reverse inversion, overrides, digests (M4-a).

All offline: fixtures are committed files or inline strings; no test performs
network access. ``tests/fixtures/aot_real_entries.xml`` is a verbatim excerpt of
the live Anime-Lists XML (source URL, fetch date, full-file SHA-256, attribution
recorded in its header comment) and is the real-data regression anchor from
docs/compatibility.md section 3. ``tests/fixtures/gate_mappings.xml`` and
``gate_mappings_reordered.xml`` are synthetic gate fixtures authored for these
tests.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from shoko_linker.errors import ConfigError
from shoko_linker.mappings import (
    MAX_PAIRS_PER_RULE,
    UNVERIFIED_MAX,
    UNVERIFIED_MIN,
    EpisodeType,
    IndexStats,
    MappingError,
    MappingIndex,
    Overrides,
    ResolutionStatus,
    SourceRule,
    build_mapping_index,
    content_digest,
    effective_targets,
    index_stats,
    load_overrides,
    parse_anime_lists,
    resolve,
    structure_digest,
    targets_digest,
)

FIXTURES = Path(__file__).resolve().parent / "fixtures"
AOT_XML = FIXTURES / "aot_real_entries.xml"
GATE_XML = FIXTURES / "gate_mappings.xml"
GATE_REORDERED_XML = FIXTURES / "gate_mappings_reordered.xml"

RANK_OVERRIDE = 0
RANK_PAIR = 1
RANK_RANGE = 2
RANK_ENTRY_OFFSET = 3
RANK_ABSOLUTE = 4
SINGLE_CANDIDATE = 1
TWO_CANDIDATES = 2
SHA256_HEX_LENGTH = 64
SMALL_MAX_ENTRIES = 1
TWO_ELEMENTS = 2
NO_CONFLICTS = 0

SPLIT_TVDB = 1000
SPLIT_FIRST_ANIDB = 100
SPLIT_SECOND_ANIDB = 101
SPLIT_MISS_EPISODE = 1
PAIR_TVDB = 1100
PAIR_ANIDB = 110
PAIR_EPISODE = 9
PAIR_ANIDB_EPISODE = 3
PAIR_OFFSET_EPISODE = 5
RANGE_TVDB = 1200
RANGE_ANIDB = 120
RANGE_EPISODE = 25
RANGE_ANIDB_EPISODE = 5
RANGE_LOW_MISS = 15
RANGE_HIGH_MISS = 31
RANGE_ENTRY_TVDB = 1250
ZERO_TVDB = 1300
ZERO_ANIDB = 130
ZERO_SUPPRESSED_EPISODE = 5
ZERO_REGISTERED_EPISODE = 7
ZERO_REGISTERED_ANIDB_EPISODE = 6
TRAILER_TVDB = 1400
TRAILER_EPISODE = 1
TRAILER_ANIDB_EPISODE = 201
OTHER_EPISODE = 3
OTHER_ANIDB_EPISODE = 401
MULTI_TVDB = 1500
MULTI_ANIDB = 150
MULTI_SECOND_EPISODE = 2
PARTS_TVDB = 1600
PARTS_EPISODE = 1
COLLISION_TVDB = 1700
COLLISION_EPISODE = 4
COLLISION_FIRST_ANIDB = 170
COLLISION_SECOND_ANIDB = 171
COLLISION_SECOND_EPISODE = 9
SEASON0_UNIQUE_TVDB = 1800
SEASON0_UNIQUE_ANIDB = 180
SEASON0_UNIQUE_EPISODE = 3
SEASON0_UNIQUE_ANIDB_EPISODE = 1
SEASON0_MULTI_TVDB = 1900
SEASON0_MULTI_EPISODE = 2
SEASON0_MULTI_MISS = 3
SEASON0_OAD_ANIDB = 192
SEASON0_OAD_EPISODE = 4
SEASON0_OAD_ANIDB_EPISODE = 3
ABSOLUTE_TVDB = 2000
ABSOLUTE_ANIDB = 200
ABSOLUTE_EPISODE = 4
COVERED_TVDB = 2100
COVERED_ANIDB = 210
COVERED_EPISODE = 301
COVERED_ANIDB_EPISODE = 1
COVERED_FREE_EPISODE = 9
COVERED_FREE_ABSOLUTE = 2
ABS_OFFSET_ANIDB = 220
INVALID_PAIR_ANIDB = 230
UNVERIFIED_RANGE_TVDB = 2400
UNVERIFIED_RANGE_ANIDB = 240
UNVERIFIED_RANGE_EPISODE = 2
BEFORE_TVDB = 2500
BEFORE_ANIDB = 250
BEFORE_EPISODE = 1
BEFORE_ANIDB_EPISODE = 7
BEFORE_OFFSET_EPISODE = 2
NEGATIVE_TVDB = 2600
NEGATIVE_ANIDB = 260
NEGATIVE_EPISODE = 26
NEGATIVE_ANIDB_EPISODE = 36
INERT_TVDB = 2700
INERT_ANIDB = 270
INERT_EPISODE = 5
INERT_ANIDB_EPISODE = 1
SEASON0_ALONE_TVDB = 2800
SEASON0_ALONE_EPISODE = 1
UNBOUNDED_TVDB = 2900
UNBOUNDED_ANIDB = 290
UNBOUNDED_EPISODE = 3
SEASON2_TVDB = 2910
SEASON2_ANIDB = 291
SEASON2_EPISODE = 3

GATE_ENTRIES_INDEXED = 25
GATE_ENTRIES_EXCLUDED = 2
GATE_ENTRIES_WITH_PROBLEMS = 7
GATE_MAPPING_RULES = 18  # season-2 rules are now excluded at rule level (review fix)
GATE_RANGE_RULES = 2
GATE_ABSOLUTE_ENTRIES = 2

AOT_TVDB = 267440
AOT_ANCHOR_SEASON = 4
AOT_ANCHOR_EPISODE = 26
AOT_ANCHOR_ABSOLUTE = 85
AOT_ANCHOR_ANIDB = 16177
AOT_ANCHOR_ANIDB_EPISODE = 10
AOT_ENTRIES = 8
AOT_MAPPING_RULES = 4
AOT_EXPLICIT_PAIRS = 32
AOT_NO_COUNTERPART_PAIRS = 14
LIVE_FILE_SHA256 = "492b080c17777ce43a51094fb4d892b0ea0e9fac8fe8c8b655dc27ec5e22eb17"

OVERRIDE_ADD_CONFLICT = """
add:
  - {tvdb_series: 1700, season: 1, episode: 4, anidb_id: 999, anidb_episode: 7, type: Episode}
"""
OVERRIDE_ADD_GAP = """
add:
  - {tvdb_series: 1200, season: 2, episode: 15, anidb_id: 120, anidb_episode: 9, type: Episode}
"""
OVERRIDE_REMOVE_ONE = """
remove:
  - {tvdb_series: 1700, season: 1, episode: 4, anidb_id: 170}
"""
OVERRIDE_REMOVE_ALL = """
remove:
  - {tvdb_series: 1700, season: 1, episode: 4}
"""
OVERRIDE_REMOVE_NOOP = """
remove:
  - {tvdb_series: 1100, season: 1, episode: 5, anidb_id: 424242}
"""
OVERRIDE_ADD_REDUNDANT = """
add:
  - {tvdb_series: 1100, season: 1, episode: 9, anidb_id: 110, anidb_episode: 3, type: Episode}
"""
OVERRIDE_REMOVE_KEY_1300 = """
remove:
  - {tvdb_series: 1300, season: 1, episode: 7}
"""

ROOT_MALFORMED = "<mappings><anime anidbid='1' tvdbid='2' defaulttvdbseason='1'/></mappings>"
ENTRY_UNCLOSED = "<anime-list><anime anidbid='1' tvdbid='2' defaulttvdbseason='1'></anime-list>"
MAPPING_UNKNOWN_ATTR = (
    "<anime anidbid='1' tvdbid='2' defaulttvdbseason='1'>"
    "<mapping-list><mapping anidbseason='1' tvdbseason='1' type='ep'>;3-9;</mapping></mapping-list>"
    "</anime>"
)
AOT_S1_ANIDB = 9541
AOT_FINAL_ANIDB = 14977
AOT_FINAL_PART2_ANIDB = AOT_ANCHOR_ANIDB
AOT_FINAL_PART3_ANIDB = 17303
AOT_S3_ANIDB = 13241
AOT_S3_PART2_ANIDB = 14444
AOT_OAD_ANIDB = 9826
OVERRIDE_TARGET_ANIDB = 999
OVERRIDE_TARGET_EPISODE = 7
GAP_TARGET_EPISODE = 9
MALFORMED_ADD_NO_TYPE = (
    "add:\n  - {tvdb_series: 1, season: 1, episode: 1, anidb_id: 1, anidb_episode: 1}\n"
)
MALFORMED_ADD_NO_EPISODE = (
    "add:\n  - {tvdb_series: 1, season: 1, episode: 1, anidb_id: 1, type: Episode}\n"
)
MALFORMED_ADD_NO_SERIES = (
    "add:\n  - {season: 1, episode: 1, anidb_id: 1, anidb_episode: 1, type: Episode}\n"
)
MALFORMED_ADD_CREDITS = (
    "add:\n  - tvdb_series: 1\n    season: 1\n    episode: 1\n"
    "    anidb_id: 1\n    anidb_episode: 1\n    type: Credits\n"
)
MALFORMED_ADD_EPISODE_ZERO = (
    "add:\n  - tvdb_series: 1\n    season: 1\n    episode: 0\n"
    "    anidb_id: 1\n    anidb_episode: 1\n    type: Episode\n"
)


def doc(entries: str) -> str:
    return f"<anime-list>{entries}</anime-list>"


def index_of(xml: str, overrides: Overrides | None = None) -> MappingIndex:
    return build_mapping_index(parse_anime_lists(xml), overrides)


def gate_index(overrides: Overrides | None = None) -> MappingIndex:
    return index_of(GATE_XML.read_text(encoding="utf-8"), overrides)


def aot_index() -> MappingIndex:
    return index_of(AOT_XML.read_text(encoding="utf-8"))


# ------------------------------------------------------------------ document parsing


def test_root_element_required() -> None:
    with pytest.raises(MappingError, match="root element"):
        parse_anime_lists(ROOT_MALFORMED)


def test_malformed_xml_raises_document_error() -> None:
    with pytest.raises(MappingError, match="not well-formed"):
        parse_anime_lists(ENTRY_UNCLOSED)


def test_doctype_and_external_entities_rejected() -> None:
    entity = (
        "<!DOCTYPE anime-list [<!ENTITY x 'evil'>]>"
        "<anime-list><anime anidbid='1' tvdbid='2' defaulttvdbseason='1'>&x;</anime></anime-list>"
    )
    external = (
        "<!DOCTYPE anime-list [<!ENTITY x SYSTEM 'file:///etc/passwd'>]>"
        "<anime-list><anime anidbid='1' tvdbid='2' defaulttvdbseason='1'>&x;</anime></anime-list>"
    )
    with pytest.raises(MappingError, match="unsafe XML"):
        parse_anime_lists(entity)
    with pytest.raises(MappingError, match="unsafe XML"):
        parse_anime_lists(external)


def test_entry_count_cap() -> None:
    xml = doc(
        "<anime anidbid='1' tvdbid='2' defaulttvdbseason='1'/>"
        "<anime anidbid='2' tvdbid='3' defaulttvdbseason='1'/>"
    )
    with pytest.raises(MappingError, match="max_entries"):
        parse_anime_lists(xml, max_entries=SMALL_MAX_ENTRIES)
    parsed = parse_anime_lists(xml, max_entries=TWO_ELEMENTS)
    assert parsed.anime_elements == TWO_ELEMENTS
    assert len(parsed.entries) == TWO_ELEMENTS


def test_parser_accepts_bytes_and_str() -> None:
    xml = doc("<anime anidbid='1' tvdbid='2' defaulttvdbseason='1'/>")
    assert len(parse_anime_lists(xml.encode("utf-8")).entries) == SINGLE_CANDIDATE
    assert len(parse_anime_lists(xml).entries) == SINGLE_CANDIDATE


def test_document_problems_recorded() -> None:
    parsed = parse_anime_lists(
        "<anime-list><other/><anime anidbid='1' tvdbid='2' defaulttvdbseason='1'/></anime-list>"
    )
    assert any("unknown element <other>" in problem for problem in parsed.document_problems)


# ------------------------------------------------------------------ entry parsing


def test_bad_anidbid_type_excludes_entry_only() -> None:
    parsed = parse_anime_lists(
        doc(
            "<anime anidbid='abc' tvdbid='2' defaulttvdbseason='1'/>"
            "<anime anidbid='3' tvdbid='4' defaulttvdbseason='1'/>"
        )
    )
    assert len(parsed.entries) == SINGLE_CANDIDATE
    assert parsed.excluded_entries == SINGLE_CANDIDATE
    assert parsed.entry_problems[0].excluded
    assert any("invalid anidbid" in text for text in parsed.entry_problems[0].problems)


def test_unknown_attributes_recorded_never_guessed() -> None:
    parsed = parse_anime_lists(
        doc("<anime anidbid='1' tvdbid='2' defaulttvdbseason='1' startingEpisodeNumber='9'/>")
    )
    assert len(parsed.entries) == SINGLE_CANDIDATE
    assert parsed.entries[0].unknown_attributes == ("startingEpisodeNumber",)
    assert any("unknown attribute" in text for text in parsed.entry_problems[0].problems)


def test_left_multi_pair_rejects_entry() -> None:
    parsed = parse_anime_lists(
        doc(
            "<anime anidbid='1' tvdbid='2' defaulttvdbseason='1'>"
            "<mapping-list><mapping anidbseason='1' tvdbseason='1'>;1+2-1;</mapping></mapping-list>"
            "</anime>"
        )
    )
    assert parsed.excluded_entries == SINGLE_CANDIDATE
    assert any("AniDB-side multi-episode" in text for text in parsed.entry_problems[0].problems)
    index = build_mapping_index(parsed)
    assert resolve(index, 2, 1, 1).status is ResolutionStatus.UNMAPPED


def test_absolute_with_episodeoffset_fails_closed() -> None:
    parsed = parse_anime_lists(
        doc("<anime anidbid='1' tvdbid='2' defaulttvdbseason='a' episodeoffset='3'/>")
    )
    assert parsed.excluded_entries == SINGLE_CANDIDATE
    assert any("UNVERIFIED" in text for text in parsed.entry_problems[0].problems)
    index = build_mapping_index(parsed)
    assert resolve(index, 2, 1, 5, absolute=5).status is ResolutionStatus.UNMAPPED


def test_unverified_101_103_pairs_excluded_pair_survives() -> None:
    index = gate_index()
    parsed = parse_anime_lists(GATE_XML.read_text(encoding="utf-8"))
    problem = next(p for p in parsed.entry_problems if p.anidbid == UNVERIFIED_RANGE_ANIDB)
    assert not problem.excluded
    assert any("101-103" in text for text in problem.problems)
    surviving = resolve(index, UNVERIFIED_RANGE_TVDB, 1, UNVERIFIED_RANGE_EPISODE)
    assert surviving.status is ResolutionStatus.RESOLVED
    entry_offset = resolve(index, UNVERIFIED_RANGE_TVDB, 1, SEASON0_ALONE_EPISODE)
    assert entry_offset.status is ResolutionStatus.RESOLVED
    assert entry_offset.target is not None
    assert entry_offset.target.anidb_id == UNVERIFIED_RANGE_ANIDB


def test_start_end_without_offset_inert_pairs_work() -> None:
    index = gate_index()
    parsed = parse_anime_lists(GATE_XML.read_text(encoding="utf-8"))
    entry = next(p for p in parsed.entry_problems if p.anidbid == INERT_ANIDB)
    assert any("inert" in text for text in entry.problems)
    resolved = resolve(index, INERT_TVDB, 1, INERT_EPISODE)
    assert resolved.target is not None
    assert resolved.target.anidb_id == INERT_ANIDB
    assert resolved.target.anidb_episode_number == INERT_ANIDB_EPISODE
    assert resolved.target.source_rule is SourceRule.EXPLICIT_PAIR


def test_offset_without_bounds_excluded() -> None:
    parsed = parse_anime_lists(GATE_XML.read_text(encoding="utf-8"))
    entry = next(p for p in parsed.entry_problems if p.anidbid == UNBOUNDED_ANIDB)
    assert any("unbounded" in text for text in entry.problems)
    index = build_mapping_index(parsed)
    resolved = resolve(index, UNBOUNDED_TVDB, 1, UNBOUNDED_EPISODE)
    assert resolved.target is not None
    assert resolved.target.source_rule is SourceRule.ENTRY_OFFSET


def test_anidbseason_2_excluded_fail_closed() -> None:
    index = gate_index()
    parsed = parse_anime_lists(GATE_XML.read_text(encoding="utf-8"))
    entry = next(p for p in parsed.entry_problems if p.anidbid == SEASON2_ANIDB)
    assert any("UNVERIFIED" in text for text in entry.problems)
    resolved = resolve(index, SEASON2_TVDB, 1, SEASON2_EPISODE)
    assert resolved.target is not None
    assert resolved.target.source_rule is SourceRule.ENTRY_OFFSET


def test_duplicate_anidbid_second_entry_excluded() -> None:
    parsed = parse_anime_lists(
        doc(
            "<anime anidbid='7' tvdbid='2' defaulttvdbseason='1'/>"
            "<anime anidbid='7' tvdbid='3' defaulttvdbseason='1'/>"
        )
    )
    assert len(parsed.entries) == SINGLE_CANDIDATE
    assert parsed.excluded_entries == SINGLE_CANDIDATE
    assert any("already claimed" in text for text in parsed.entry_problems[0].problems)


def test_unknown_mapping_attribute_excludes_rule() -> None:
    parsed = parse_anime_lists(doc(MAPPING_UNKNOWN_ATTR))
    assert len(parsed.entries) == SINGLE_CANDIDATE
    assert any("unknown attribute 'type'" in text for text in parsed.entry_problems[0].problems)
    index = build_mapping_index(parsed)
    assert index.mapping_rules == NO_CONFLICTS
    resolved = resolve(index, 2, 1, 9)
    assert resolved.target is not None
    assert resolved.target.source_rule is SourceRule.ENTRY_OFFSET


# --------------------------------------------------------------------- gate behaviors


def test_split_seasons_resolve_to_their_own_entries() -> None:
    index = gate_index()
    first = resolve(index, SPLIT_TVDB, 1, RANGE_ANIDB_EPISODE)
    assert first.status is ResolutionStatus.RESOLVED
    assert first.target is not None
    assert first.target.anidb_id == SPLIT_FIRST_ANIDB
    second = resolve(index, SPLIT_TVDB, 2, SEASON0_UNIQUE_ANIDB_EPISODE)
    assert second.target is not None
    assert second.target.anidb_id == SPLIT_SECOND_ANIDB
    missing = resolve(index, SPLIT_TVDB, 3, SPLIT_MISS_EPISODE)
    assert missing.status is ResolutionStatus.UNMAPPED


def test_explicit_pair_beats_entry_offset() -> None:
    index = gate_index()
    resolved = resolve(index, PAIR_TVDB, 1, PAIR_EPISODE)
    assert resolved.status is ResolutionStatus.RESOLVED
    assert resolved.target is not None
    assert resolved.target.anidb_id == PAIR_ANIDB
    assert resolved.target.anidb_episode_number == PAIR_ANIDB_EPISODE
    assert resolved.target.source_rule is SourceRule.EXPLICIT_PAIR
    assert resolved.target.precedence_rank == RANK_PAIR
    other = resolve(index, PAIR_TVDB, 1, PAIR_OFFSET_EPISODE)
    assert other.target is not None
    assert other.target.source_rule is SourceRule.ENTRY_OFFSET


def test_mapping_range_offset_beats_entry_offset() -> None:
    index = gate_index()
    resolved = resolve(index, RANGE_ENTRY_TVDB, 1, PAIR_OFFSET_EPISODE)
    assert resolved.target is not None
    assert resolved.target.anidb_episode_number == PAIR_OFFSET_EPISODE
    assert resolved.target.source_rule is SourceRule.MAPPING_RANGE_OFFSET
    assert resolved.target.precedence_rank == RANK_RANGE


def test_range_bounds_enforced() -> None:
    index = gate_index()
    resolved = resolve(index, RANGE_TVDB, 2, RANGE_EPISODE)
    assert resolved.status is ResolutionStatus.RESOLVED
    assert resolved.target is not None
    assert resolved.target.anidb_id == RANGE_ANIDB
    assert resolved.target.anidb_episode_number == RANGE_ANIDB_EPISODE
    low = resolve(index, RANGE_TVDB, 2, RANGE_LOW_MISS)
    assert low.status is ResolutionStatus.UNMAPPED
    high = resolve(index, RANGE_TVDB, 2, RANGE_HIGH_MISS)
    assert high.status is ResolutionStatus.UNMAPPED


def test_tvdb_zero_never_registered_and_suppresses_identity() -> None:
    index = gate_index()
    suppressed = resolve(index, ZERO_TVDB, 1, ZERO_SUPPRESSED_EPISODE)
    assert suppressed.status is ResolutionStatus.UNMAPPED
    resolved = resolve(index, ZERO_TVDB, 1, ZERO_REGISTERED_EPISODE)
    assert resolved.target is not None
    assert resolved.target.anidb_episode_number == ZERO_REGISTERED_ANIDB_EPISODE


def test_no_equivalent_sentinel_lists_declarations() -> None:
    index = gate_index()
    sentinel = resolve(index, ZERO_TVDB, 1, NO_CONFLICTS)
    assert sentinel.status is ResolutionStatus.NO_EQUIVALENT
    assert f"AniDB {ZERO_ANIDB} ep {ZERO_SUPPRESSED_EPISODE}" in sentinel.detail


def test_trailer_and_other_number_types() -> None:
    index = gate_index()
    trailer = resolve(index, TRAILER_TVDB, 1, TRAILER_EPISODE)
    assert trailer.target is not None
    assert trailer.target.episode_type is EpisodeType.TRAILER
    assert trailer.target.anidb_episode_number == TRAILER_ANIDB_EPISODE
    other = resolve(index, TRAILER_TVDB, 1, OTHER_EPISODE)
    assert other.target is not None
    assert other.target.episode_type is EpisodeType.OTHER
    assert other.target.anidb_episode_number == OTHER_ANIDB_EPISODE


def test_multi_episode_forward_pair_registers_both_keys() -> None:
    index = gate_index()
    first = resolve(index, MULTI_TVDB, 1, PARTS_EPISODE)
    second = resolve(index, MULTI_TVDB, 1, MULTI_SECOND_EPISODE)
    assert first.status is ResolutionStatus.RESOLVED
    assert second.status is ResolutionStatus.RESOLVED
    assert first.target is not None
    assert second.target is not None
    assert first.target.anidb_episode_number == PARTS_EPISODE
    assert second.target.anidb_episode_number == PARTS_EPISODE


def test_multi_episode_reverse_parts_ambiguous_candidates() -> None:
    index = gate_index()
    resolution = resolve(index, PARTS_TVDB, 1, PARTS_EPISODE)
    assert resolution.status is ResolutionStatus.AMBIGUOUS
    assert resolution.target is None
    assert [target.anidb_episode_number for target in resolution.candidates] == [
        PARTS_EPISODE,
        SEASON0_UNIQUE_ANIDB_EPISODE + 1,
    ]


def test_two_entries_colliding_on_same_key_ambiguous() -> None:
    index = gate_index()
    resolution = resolve(index, COLLISION_TVDB, 1, COLLISION_EPISODE)
    assert resolution.status is ResolutionStatus.AMBIGUOUS
    assert [target.anidb_id for target in resolution.candidates] == [
        COLLISION_FIRST_ANIDB,
        COLLISION_SECOND_ANIDB,
    ]
    assert "writes prohibited" in resolution.detail


def test_season0_unique_explicit_mapping_resolves() -> None:
    index = gate_index()
    resolved = resolve(index, SEASON0_UNIQUE_TVDB, NO_CONFLICTS, SEASON0_UNIQUE_EPISODE)
    assert resolved.status is ResolutionStatus.RESOLVED
    assert resolved.target is not None
    assert resolved.target.anidb_id == SEASON0_UNIQUE_ANIDB
    assert resolved.target.episode_type is EpisodeType.SPECIAL


def test_season0_multi_claimed_fails_closed() -> None:
    index = gate_index()
    collision = resolve(index, SEASON0_MULTI_TVDB, NO_CONFLICTS, SEASON0_MULTI_EPISODE)
    assert collision.status is ResolutionStatus.AMBIGUOUS
    assert len(collision.candidates) == TWO_CANDIDATES
    miss = resolve(index, SEASON0_MULTI_TVDB, NO_CONFLICTS, SEASON0_MULTI_MISS)
    assert miss.status is ResolutionStatus.UNMAPPED


def test_season0_never_resolved_by_season_alone() -> None:
    index = gate_index()
    resolution = resolve(index, SEASON0_ALONE_TVDB, NO_CONFLICTS, SEASON0_ALONE_EPISODE)
    assert resolution.status is ResolutionStatus.UNMAPPED


def test_season0_oad_style_rule_never_resolves() -> None:
    index = gate_index()
    oad_target = resolve(index, SEASON0_MULTI_TVDB, NO_CONFLICTS, SEASON0_OAD_EPISODE)
    assert oad_target.status is ResolutionStatus.UNMAPPED
    oad_source = resolve(index, SEASON0_MULTI_TVDB, 1, SEASON0_OAD_ANIDB_EPISODE)
    assert oad_source.status is ResolutionStatus.RESOLVED
    assert oad_source.target is not None
    assert oad_source.target.anidb_id == SEASON0_OAD_ANIDB


def test_absolute_identity_only_with_absolute_number() -> None:
    index = gate_index()
    with_absolute = resolve(index, ABSOLUTE_TVDB, 1, ABSOLUTE_EPISODE, absolute=ABSOLUTE_EPISODE)
    assert with_absolute.status is ResolutionStatus.RESOLVED
    assert with_absolute.target is not None
    assert with_absolute.target.anidb_id == ABSOLUTE_ANIDB
    assert with_absolute.target.source_rule is SourceRule.ABSOLUTE_IDENTITY
    assert with_absolute.target.precedence_rank == RANK_ABSOLUTE
    without = resolve(index, ABSOLUTE_TVDB, 1, ABSOLUTE_EPISODE)
    assert without.status is ResolutionStatus.UNMAPPED


def test_absolute_identity_suppressed_by_mapping_list() -> None:
    index = gate_index()
    covered = resolve(index, COVERED_TVDB, 1, COVERED_EPISODE, absolute=COVERED_ANIDB_EPISODE)
    assert covered.status is ResolutionStatus.RESOLVED
    assert covered.target is not None
    assert covered.target.source_rule is SourceRule.EXPLICIT_PAIR
    assert covered.target.anidb_episode_number == COVERED_ANIDB_EPISODE
    suppressed = resolve(
        index, COVERED_TVDB, 1, COVERED_FREE_EPISODE, absolute=COVERED_ANIDB_EPISODE
    )
    assert suppressed.status is ResolutionStatus.UNMAPPED
    assert "suppressed by its mapping-list" in suppressed.detail
    free = resolve(index, COVERED_TVDB, 1, COVERED_FREE_EPISODE, absolute=COVERED_FREE_ABSOLUTE)
    assert free.target is not None
    assert free.target.anidb_id == COVERED_ANIDB
    assert free.target.source_rule is SourceRule.ABSOLUTE_IDENTITY


def test_absolute_identity_not_used_for_season_zero() -> None:
    index = gate_index()
    resolution = resolve(
        index, ABSOLUTE_TVDB, NO_CONFLICTS, ABSOLUTE_EPISODE, absolute=ABSOLUTE_EPISODE
    )
    assert resolution.status is ResolutionStatus.UNMAPPED


def test_before_element_pairs_apply_like_explicit_pairs() -> None:
    index = gate_index()
    before = resolve(index, BEFORE_TVDB, 1, BEFORE_EPISODE)
    assert before.target is not None
    assert before.target.anidb_id == BEFORE_ANIDB
    assert before.target.anidb_episode_number == BEFORE_ANIDB_EPISODE
    assert before.target.source_rule is SourceRule.EXPLICIT_PAIR
    assert "before" in before.target.origin
    plain = resolve(index, BEFORE_TVDB, 1, BEFORE_OFFSET_EPISODE)
    assert plain.target is not None
    assert plain.target.anidb_episode_number == BEFORE_OFFSET_EPISODE
    assert plain.target.source_rule is SourceRule.ENTRY_OFFSET


def test_negative_entry_offset_reverses() -> None:
    index = gate_index()
    resolved = resolve(index, NEGATIVE_TVDB, 1, NEGATIVE_EPISODE)
    assert resolved.target is not None
    assert resolved.target.anidb_id == NEGATIVE_ANIDB
    assert resolved.target.anidb_episode_number == NEGATIVE_ANIDB_EPISODE


def test_candidates_sorted_deterministically() -> None:
    index = gate_index()
    resolution = resolve(index, COLLISION_TVDB, 1, COLLISION_EPISODE)
    ordered = [(target.anidb_id, target.anidb_episode_number) for target in resolution.candidates]
    assert ordered == sorted(ordered)


# ------------------------------------------------------------------- AoT real anchor


def test_aot_anchor_s04e26_abs85_resolves_to_16177_episode_10() -> None:
    index = aot_index()
    resolved = resolve(
        index, AOT_TVDB, AOT_ANCHOR_SEASON, AOT_ANCHOR_EPISODE, absolute=AOT_ANCHOR_ABSOLUTE
    )
    assert resolved.status is ResolutionStatus.RESOLVED
    assert resolved.target is not None
    assert resolved.target.anidb_id == AOT_ANCHOR_ANIDB
    assert resolved.target.anidb_episode_number == AOT_ANCHOR_ANIDB_EPISODE
    assert resolved.target.episode_type is EpisodeType.EPISODE
    assert resolved.target.source_rule is SourceRule.ENTRY_OFFSET
    assert resolved.target.precedence_rank == RANK_ENTRY_OFFSET
    assert len(resolved.candidates) == SINGLE_CANDIDATE


def test_aot_season_partition_resolves_without_cross_entry_collisions() -> None:
    index = aot_index()
    partition = {
        (AOT_ANCHOR_SEASON, 10): (14977, 10),
        (AOT_ANCHOR_SEASON, 16): (14977, 16),
        (AOT_ANCHOR_SEASON, 17): (AOT_ANCHOR_ANIDB, 1),
        (AOT_ANCHOR_SEASON, 28): (AOT_ANCHOR_ANIDB, 12),
        (AOT_ANCHOR_SEASON, 29): (17303, 1),
        (AOT_ANCHOR_SEASON, 40): (17303, 12),
        (3, 12): (13241, 12),
        (3, 13): (14444, 1),
        (3, 24): (14444, 12),
    }
    for (season, episode), (anidb, anidb_episode) in partition.items():
        resolved = resolve(index, AOT_TVDB, season, episode)
        assert resolved.status is ResolutionStatus.RESOLVED, (season, episode, resolved.detail)
        assert resolved.target is not None
        observed = (resolved.target.anidb_id, resolved.target.anidb_episode_number)
        assert observed == (anidb, anidb_episode)
        assert len(resolved.candidates) == SINGLE_CANDIDATE


def test_aot_season_zero_explicit_mappings_resolve() -> None:
    index = aot_index()
    season_zero = {
        (NO_CONFLICTS, 1): (AOT_S1_ANIDB, 1, EpisodeType.SPECIAL),
        (NO_CONFLICTS, 8): (AOT_S1_ANIDB, 7, EpisodeType.SPECIAL),
        (NO_CONFLICTS, 11): (AOT_S1_ANIDB, 10, EpisodeType.SPECIAL),
    }
    for (season, episode), (anidb, anidb_episode, episode_type) in season_zero.items():
        resolved = resolve(index, AOT_TVDB, season, episode)
        assert resolved.status is ResolutionStatus.RESOLVED, (season, episode, resolved.detail)
        assert resolved.target is not None
        observed = (resolved.target.anidb_id, resolved.target.anidb_episode_number)
        assert observed == (anidb, anidb_episode)
        assert resolved.target.episode_type is episode_type
    for episode in (7, 12, 13, 15, 17, 20, 22, 23):
        oad_only = resolve(index, AOT_TVDB, NO_CONFLICTS, episode)
        assert oad_only.status is ResolutionStatus.UNMAPPED, (episode, oad_only.detail)


def test_aot_no_counterpart_declarations_never_resolve() -> None:
    index = aot_index()
    sentinel = resolve(index, AOT_TVDB, NO_CONFLICTS, NO_CONFLICTS)
    assert sentinel.status is ResolutionStatus.NO_EQUIVALENT
    declared = index.no_counterpart.get((AOT_TVDB, NO_CONFLICTS), frozenset())
    assert (9541, 11, EpisodeType.SPECIAL) in declared
    assert (13241, 5, EpisodeType.SPECIAL) in declared
    special_11 = resolve(index, AOT_TVDB, NO_CONFLICTS, 11)
    assert special_11.status is ResolutionStatus.RESOLVED
    assert special_11.target is not None
    assert special_11.target.anidb_id == AOT_S1_ANIDB


def test_aot_index_stats_and_digests() -> None:
    parsed = parse_anime_lists(AOT_XML.read_text(encoding="utf-8"))
    index = build_mapping_index(parsed)
    stats = index_stats(index)
    assert stats.entries_indexed == AOT_ENTRIES
    assert stats.entries_excluded == NO_CONFLICTS
    assert stats.entries_with_problems == NO_CONFLICTS
    assert stats.mapping_rules == AOT_MAPPING_RULES
    assert stats.explicit_pairs == AOT_EXPLICIT_PAIRS
    assert stats.no_counterpart_pairs == AOT_NO_COUNTERPART_PAIRS
    assert stats.range_rules == NO_CONFLICTS
    assert stats.absolute_entries == NO_CONFLICTS
    raw = AOT_XML.read_bytes()
    assert len(content_digest(raw)) == SHA256_HEX_LENGTH
    assert LIVE_FILE_SHA256 in raw.decode("utf-8")
    digest = targets_digest(index)
    assert len(digest) == SHA256_HEX_LENGTH
    assert targets_digest(aot_index()) == digest


# ------------------------------------------------------------------- overrides


def test_override_add_ranks_above_upstream_explicit_pair() -> None:
    index = gate_index(load_overrides(OVERRIDE_ADD_CONFLICT))
    resolved = resolve(index, COLLISION_TVDB, 1, COLLISION_EPISODE)
    assert resolved.status is ResolutionStatus.RESOLVED
    assert resolved.target is not None
    assert resolved.target.anidb_id == OVERRIDE_TARGET_ANIDB
    assert resolved.target.source_rule is SourceRule.OVERRIDE
    assert resolved.target.precedence_rank == RANK_OVERRIDE
    conflicts = [conflict for conflict in index.override_conflicts if conflict.kind == "add"]
    assert len(conflicts) == SINGLE_CANDIDATE
    assert "upstream resolves" in conflicts[0].detail


def test_override_add_fills_unmapped_gap() -> None:
    index = gate_index(load_overrides(OVERRIDE_ADD_GAP))
    resolved = resolve(index, RANGE_TVDB, 2, RANGE_LOW_MISS)
    assert resolved.status is ResolutionStatus.RESOLVED
    assert resolved.target is not None
    assert resolved.target.anidb_id == RANGE_ANIDB
    assert any(conflict.kind == "add-gap" for conflict in index.override_conflicts)


def test_override_remove_subtracts_one_candidate() -> None:
    index = gate_index(load_overrides(OVERRIDE_REMOVE_ONE))
    resolved = resolve(index, COLLISION_TVDB, 1, COLLISION_EPISODE)
    assert resolved.status is ResolutionStatus.RESOLVED
    assert resolved.target is not None
    assert resolved.target.anidb_id == COLLISION_SECOND_ANIDB


def test_override_remove_all_targets_of_key() -> None:
    index = gate_index(load_overrides(OVERRIDE_REMOVE_ALL))
    resolution = resolve(index, COLLISION_TVDB, 1, COLLISION_EPISODE)
    assert resolution.status is ResolutionStatus.UNMAPPED
    assert "operator removals applied" in resolution.detail


def test_override_remove_absent_target_reported_as_noop() -> None:
    index = gate_index(load_overrides(OVERRIDE_REMOVE_NOOP))
    conflicts = [
        conflict for conflict in index.override_conflicts if conflict.kind == "remove-noop"
    ]
    assert len(conflicts) == SINGLE_CANDIDATE
    resolved = resolve(index, PAIR_TVDB, 1, PAIR_OFFSET_EPISODE)
    assert resolved.status is ResolutionStatus.RESOLVED


def test_override_matching_upstream_is_not_a_conflict() -> None:
    index = gate_index(load_overrides(OVERRIDE_ADD_REDUNDANT))
    assert len(index.override_conflicts) == NO_CONFLICTS
    resolved = resolve(index, PAIR_TVDB, 1, PAIR_EPISODE)
    assert resolved.target is not None
    assert resolved.target.source_rule is SourceRule.OVERRIDE


def test_override_removal_moves_targets_digest() -> None:
    plain = gate_index()
    removed = gate_index(load_overrides(OVERRIDE_REMOVE_KEY_1300))
    plain_targets = effective_targets(plain)[ZERO_TVDB]
    removed_target = (ZERO_ANIDB, ZERO_REGISTERED_ANIDB_EPISODE, EpisodeType.EPISODE)
    assert targets_digest(plain) != targets_digest(removed)
    assert removed_target in plain_targets
    survivor = effective_targets(removed).get(ZERO_TVDB, frozenset())
    assert (ZERO_ANIDB, ZERO_REGISTERED_ANIDB_EPISODE, EpisodeType.EPISODE) not in survivor


def test_load_overrides_notes_and_empty_documents() -> None:
    empty = load_overrides("")
    assert empty.adds == ()
    assert empty.removes == ()
    documented = load_overrides("notes: manual fixes\n")
    assert documented.notes == "manual fixes"
    assert documented.adds == ()


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (MALFORMED_ADD_NO_TYPE, "type"),
        (MALFORMED_ADD_NO_EPISODE, "anidb_episode"),
        (MALFORMED_ADD_NO_SERIES, "tvdb_series"),
        (MALFORMED_ADD_CREDITS, "Credits/Parody"),
        (MALFORMED_ADD_EPISODE_ZERO, "episode"),
        ("add: not-a-list\n", "must be a list"),
        ("unexpected: 1\n", "unknown setting"),
        ("notes: 5\n", "must be a string"),
        ("remove:\n  - {tvdb_series: 1, season: 1, episode: 1, anidb_id: -3}\n", "anidb_id"),
        ("[1, 2]\n", "top level"),
        ("add: [\n", "valid YAML"),
    ],
)
def test_load_overrides_rejects_malformed(text: str, expected: str) -> None:
    with pytest.raises(ConfigError, match=expected):
        load_overrides(text)


# ------------------------------------------------------------------ digests/stats


def test_targets_digest_ignores_formatting_only_changes() -> None:
    plain = gate_index()
    reordered = index_of(GATE_REORDERED_XML.read_text(encoding="utf-8"))
    assert targets_digest(plain) == targets_digest(reordered)
    assert content_digest(GATE_XML.read_bytes()) != content_digest(GATE_REORDERED_XML.read_bytes())
    assert effective_targets(plain) == effective_targets(reordered)


def test_structure_digest_stable_and_offset_sensitive() -> None:
    plain = gate_index()
    reordered = index_of(GATE_REORDERED_XML.read_text(encoding="utf-8"))
    assert structure_digest(plain) == structure_digest(reordered)
    base = doc(
        "<anime anidbid='100' tvdbid='1000' defaulttvdbseason='1' episodeoffset='0'/>"
        "<anime anidbid='101' tvdbid='1000' defaulttvdbseason='2'/>"
    )
    shifted = doc(
        "<anime anidbid='100' tvdbid='1000' defaulttvdbseason='1' episodeoffset='3'/>"
        "<anime anidbid='101' tvdbid='1000' defaulttvdbseason='2'/>"
    )
    plain_index = index_of(base)
    shifted_index = index_of(shifted)
    assert structure_digest(plain_index) != structure_digest(shifted_index)
    assert targets_digest(plain_index) == targets_digest(shifted_index)


def test_targets_digest_tracks_semantic_change() -> None:
    plain = gate_index()
    dropped = index_of(
        doc(
            "<anime anidbid='100' tvdbid='1000' defaulttvdbseason='1'/>"
            "<anime anidbid='101' tvdbid='1000' defaulttvdbseason='2'/>"
            "<anime anidbid='110' tvdbid='1100' defaulttvdbseason='1'/>"
        )
    )
    assert targets_digest(plain) != targets_digest(dropped)
    assert PAIR_TVDB in effective_targets(plain)
    assert PAIR_TVDB not in effective_targets(dropped)


def test_effective_targets_are_canonical_and_hashable() -> None:
    index = gate_index()
    targets = effective_targets(index)
    assert all(isinstance(identities, frozenset) for identities in targets.values())
    assert (PAIR_ANIDB, PAIR_ANIDB_EPISODE, EpisodeType.EPISODE) in targets[PAIR_TVDB]
    assert COLLISION_TVDB not in targets
    assert PARTS_TVDB not in targets
    declared = frozenset({(ZERO_ANIDB, ZERO_REGISTERED_ANIDB_EPISODE, EpisodeType.EPISODE)})
    assert declared <= targets[ZERO_TVDB]
    assert isinstance(hash(tuple(sorted(targets.items()))), int)


def test_index_stats_report_gate_counts() -> None:
    index = gate_index()
    stats: IndexStats = index_stats(index)
    assert stats.entries_indexed == GATE_ENTRIES_INDEXED
    assert stats.entries_excluded == GATE_ENTRIES_EXCLUDED
    assert stats.entries_with_problems == GATE_ENTRIES_WITH_PROBLEMS
    assert stats.mapping_rules == GATE_MAPPING_RULES
    assert stats.absolute_entries == GATE_ABSOLUTE_ENTRIES
    assert stats.range_rules == GATE_RANGE_RULES
    assert stats.ambiguous_keys >= TWO_CANDIDATES
    assert stats.conflict_keys == NO_CONFLICTS
    assert stats.override_adds == NO_CONFLICTS
    assert stats.tvdb_series_with_targets >= GATE_RANGE_RULES * 5


# ------------------------------------------------- review-driven regressions (M4-a)

CAP_TVDB = 3000
CAP_ANIDB = 300
SEASON3_TVDB = 3100
SEASON3_ANIDB = 310
SEASON3_EPISODE = 4
TYPE_CONFLICT_TVDB = 3200
TYPE_CONFLICT_ANIDB = 320
TYPE_CONFLICT_ANIDB_EPISODE = 5
TYPE_CONFLICT_TVDB_EPISODE = 9
S0_RANGE_TVDB = 3300
S0_RANGE_ANIDB = 330
S0_RANGE_EPISODE = 5
S0_RANGE_MISS = 1
SUPPRESS_TVDB = 3400
SUPPRESS_ANIDB = 340
SUPPRESS_TVDB_EPISODE = 7
RANGE_101_TVDB = 3500
RANGE_101_ANIDB = 350
OVERRIDE_NO_COUNTERPART_ANIDB = 130
OVERRIDE_NO_COUNTERPART_ANIDB_EPISODE = 5
OVERRIDE_NO_COUNTERPART_EPISODE = 11
UNVERIFIED_EPISODES = (101, 102, 103)

OVERRIDE_ADD_NO_COUNTERPART = (
    "add:\n"
    f"  - {{tvdb_series: {ZERO_TVDB}, season: 1, episode: {OVERRIDE_NO_COUNTERPART_EPISODE}, "
    f"anidb_id: {OVERRIDE_NO_COUNTERPART_ANIDB}, "
    f"anidb_episode: {OVERRIDE_NO_COUNTERPART_ANIDB_EPISODE}, type: Episode}}\n"
)


def test_pair_body_over_cap_excludes_the_entry() -> None:
    body = "".join(f";{n}-{n + 50};" for n in range(1, 2 * MAX_PAIRS_PER_RULE))
    xml = doc(
        f"<anime anidbid='{CAP_ANIDB}' tvdbid='{CAP_TVDB}' defaulttvdbseason='1'>"
        f"<mapping-list><mapping anidbseason='1' tvdbseason='1'>{body}</mapping></mapping-list>"
        f"</anime>"
    )
    parsed = parse_anime_lists(xml)
    assert parsed.entries == ()
    assert parsed.entry_problems[0].excluded is True
    assert any("cap" in problem for problem in parsed.entry_problems[0].problems)
    assert build_mapping_index(parsed).entries_indexed == 0


def test_pair_cap_overflow_is_fatal_before_later_token_validation() -> None:
    body = "".join(f";{n}-{n + 50};" for n in range(1, 2 * MAX_PAIRS_PER_RULE)) + ";1+2-1;"
    xml = doc(
        f"<anime anidbid='{CAP_ANIDB}' tvdbid='{CAP_TVDB}' defaulttvdbseason='1'>"
        f"<mapping-list><mapping anidbseason='1' tvdbseason='1'>{body}</mapping></mapping-list>"
        f"</anime>"
    )
    parsed = parse_anime_lists(xml)
    assert parsed.entries == ()
    assert parsed.entry_problems[0].excluded is True


def test_anidbseason_above_one_excludes_whole_rule_with_note() -> None:
    xml = doc(
        f"<anime anidbid='{SEASON3_ANIDB}' tvdbid='{SEASON3_TVDB}' defaulttvdbseason='1'>"
        f"<mapping-list><mapping anidbseason='3' tvdbseason='1'>;{SEASON3_EPISODE}-"
        f"{SEASON3_EPISODE};</mapping></mapping-list>"
        f"</anime>"
    )
    parsed = parse_anime_lists(xml)
    assert any("UNVERIFIED" in problem for problem in parsed.entry_problems[0].problems)
    index = build_mapping_index(parsed)
    resolution = resolve(index, SEASON3_TVDB, 1, SEASON3_EPISODE)
    # The unverified rule contributed nothing; only the entry default remains.
    assert resolution.target is not None
    assert resolution.target.source_rule is SourceRule.ENTRY_OFFSET


def test_identical_target_with_contradicting_types_is_conflict() -> None:
    xml = doc(
        f"<anime anidbid='{TYPE_CONFLICT_ANIDB}' tvdbid='{TYPE_CONFLICT_TVDB}' "
        "defaulttvdbseason='1'>"
        "<mapping-list>"
        f"<mapping anidbseason='1' tvdbseason='1'>;{TYPE_CONFLICT_ANIDB_EPISODE}-"
        f"{TYPE_CONFLICT_TVDB_EPISODE};</mapping>"
        f"<mapping anidbseason='0' tvdbseason='1'>;{TYPE_CONFLICT_ANIDB_EPISODE}-"
        f"{TYPE_CONFLICT_TVDB_EPISODE};</mapping>"
        "</mapping-list></anime>"
    )
    resolution = resolve(index_of(xml), TYPE_CONFLICT_TVDB, 1, TYPE_CONFLICT_TVDB_EPISODE)
    assert resolution.status is ResolutionStatus.CONFLICT
    assert len(resolution.candidates) == TWO_CANDIDATES


def test_season_zero_range_offset_path_resolves_special() -> None:
    xml = doc(
        f"<anime anidbid='{S0_RANGE_ANIDB}' tvdbid='{S0_RANGE_TVDB}' defaulttvdbseason='1'>"
        "<mapping-list><mapping anidbseason='0' tvdbseason='0' start='1' end='8' offset='2'/>"
        "</mapping-list></anime>"
    )
    index = index_of(xml)
    resolution = resolve(index, S0_RANGE_TVDB, 0, S0_RANGE_EPISODE)
    assert resolution.status is ResolutionStatus.RESOLVED
    assert resolution.target is not None
    assert resolution.target.episode_type is EpisodeType.SPECIAL
    assert resolution.target.source_rule is SourceRule.MAPPING_RANGE_OFFSET
    assert resolve(index, S0_RANGE_TVDB, 0, S0_RANGE_MISS).status is ResolutionStatus.UNMAPPED


def test_no_counterpart_declaration_suppresses_same_identity_from_all_rules() -> None:
    xml = doc(
        f"<anime anidbid='{SUPPRESS_ANIDB}' tvdbid='{SUPPRESS_TVDB}' defaulttvdbseason='1' "
        "episodeoffset='2'>"
        "<mapping-list><mapping anidbseason='1' tvdbseason='1'>;5-0;5-7;</mapping></mapping-list>"
        "</anime>"
    )
    index = index_of(xml)
    assert resolve(index, SUPPRESS_TVDB, 1, SUPPRESS_TVDB_EPISODE).status is (
        ResolutionStatus.UNMAPPED
    )


def test_range_derived_unverified_numbers_unmapped_and_104_is_episode() -> None:
    xml = doc(
        f"<anime anidbid='{RANGE_101_ANIDB}' tvdbid='{RANGE_101_TVDB}' defaulttvdbseason='1'>"
        "<mapping-list><mapping anidbseason='1' tvdbseason='1' start='100' end='105' "
        "offset='0'/></mapping-list></anime>"
    )
    index = index_of(xml)
    for episode in UNVERIFIED_EPISODES:
        assert resolve(index, RANGE_101_TVDB, 1, episode).status is ResolutionStatus.UNMAPPED
    resolution = resolve(index, RANGE_101_TVDB, 1, 104)
    assert resolution.status is ResolutionStatus.RESOLVED
    assert resolution.target is not None
    assert resolution.target.episode_type is EpisodeType.EPISODE


def test_forward_map_registers_pairs_without_unverified_numbers() -> None:
    index = gate_index()
    assert (PAIR_ANIDB, 1, PAIR_ANIDB_EPISODE) in index.forward
    entries = index.forward[(PAIR_ANIDB, 1, PAIR_ANIDB_EPISODE)]
    assert any(
        item.tvdb_series == PAIR_TVDB and PAIR_EPISODE in item.tvdb_episodes for item in entries
    )
    unverified = {key for key in index.forward if UNVERIFIED_MIN <= key[2] <= UNVERIFIED_MAX}
    assert unverified == set()


def test_override_add_cannot_authorize_declared_no_counterpart_identity() -> None:
    index = gate_index(load_overrides(OVERRIDE_ADD_NO_COUNTERPART))
    kinds = [conflict.kind for conflict in index.override_conflicts]
    assert "add-no-counterpart" in kinds
    resolution = resolve(index, ZERO_TVDB, 1, OVERRIDE_NO_COUNTERPART_EPISODE)
    # The refused add never becomes a candidate: only the legitimate upstream
    # entry-default mapping remains for this key.
    assert resolution.target is not None
    assert resolution.target.source_rule is SourceRule.ENTRY_OFFSET
    assert resolution.target.anidb_episode_number == OVERRIDE_NO_COUNTERPART_EPISODE


# ------------------------------------------- M4 review-driven regressions

AGGREGATE_TVDB_BASE = 5000
AGGREGATE_ANIDB_BASE = 9000
AGGREGATE_RULE_COUNT = 1002


def test_aggregate_range_key_cap_fails_closed() -> None:
    rules = "".join(
        "<mapping anidbseason='1' tvdbseason='1' start='1' end='2000' offset='0'>;</mapping>"
        for _ in range(AGGREGATE_RULE_COUNT)
    )
    xml = doc(
        "".join(
            f"<anime anidbid='{AGGREGATE_ANIDB_BASE + i}' tvdbid='{AGGREGATE_TVDB_BASE + i}' "
            f"defaulttvdbseason='1'><mapping-list>{rules}</mapping-list></anime>"
            for i in range(1)
        )
    )
    parsed = parse_anime_lists(xml)
    with pytest.raises(MappingError, match="aggregate cap"):
        build_mapping_index(parsed)


DUP_PAIR_ANIDB = 600
DUP_PAIR_TVDB = 6000


def test_identical_pair_in_both_containers_is_not_a_conflict() -> None:
    xml = doc(
        f"<anime anidbid='{DUP_PAIR_ANIDB}' tvdbid='{DUP_PAIR_TVDB}' defaulttvdbseason='1'>"
        "<mapping-list><mapping anidbseason='1' tvdbseason='1'>;3-3;</mapping></mapping-list>"
        "<before><mapping anidbseason='1' tvdbseason='1'>;3-3;</mapping></before>"
        "</anime>"
    )
    resolution = resolve(index_of(xml), DUP_PAIR_TVDB, 1, 3)
    assert resolution.status is ResolutionStatus.RESOLVED
    assert resolution.target is not None
    assert resolution.target.anidb_id == DUP_PAIR_ANIDB
