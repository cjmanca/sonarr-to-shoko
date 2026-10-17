"""Component-aware path behavior: root allowlisting and Docker mappings."""

from __future__ import annotations

import pytest

from shoko_linker.errors import PathError
from shoko_linker.paths import (
    PathMapping,
    apply_mapping,
    is_within_root,
    normalize_posix,
    path_parts,
    select_mapping,
    validate_mappings,
)

ANIME_TO_SHOKO = PathMapping(sonarr="/data/media/anime", shoko="/anime")


def test_normalize_posix_collapses_and_strips() -> None:
    assert normalize_posix("/data//media/./anime/") == "/data/media/anime"


def test_normalize_posix_preserves_meaningful_component_whitespace() -> None:
    path = "/anime/Series / file.mkv "
    assert normalize_posix(path) == path


def test_normalize_posix_rejects_relative_root_and_backslashes() -> None:
    with pytest.raises(PathError):
        normalize_posix("data/media/anime")
    with pytest.raises(PathError):
        normalize_posix("/")
    with pytest.raises(PathError):
        normalize_posix(r"\data\media\anime")
    with pytest.raises(PathError):
        normalize_posix("")


def test_path_parts_is_case_sensitive() -> None:
    assert path_parts("/anime/Season 01") == ("anime", "Season 01")


def test_is_within_root_exact_and_descendant() -> None:
    assert is_within_root("/anime", "/anime") is True
    assert is_within_root("/anime/", "/anime") is True
    assert is_within_root("/anime//Season 01/x.mkv", "/anime") is True
    assert is_within_root("/anime/Season 01", "/anime/Season 01") is True


def test_is_within_root_rejects_lookalike_prefix() -> None:
    # The classic prefix-string bug: /anime-old must NOT be inside /anime.
    assert is_within_root("/anime-old/x.mkv", "/anime") is False
    assert is_within_root("/animeold", "/anime") is False
    assert is_within_root("/anime", "/anime/season") is False
    assert is_within_root("/other/anime/x.mkv", "/anime") is False


def test_is_within_root_is_case_sensitive() -> None:
    assert is_within_root("/ANIME/x.mkv", "/anime") is False


def test_is_within_root_fails_closed_on_escape_and_junk() -> None:
    assert is_within_root("/anime/../secret/x.mkv", "/anime") is False
    assert is_within_root("/anime/../../etc/passwd", "/anime") is False
    assert is_within_root("anime/x.mkv", "/anime") is False
    assert is_within_root("", "/anime") is False
    assert is_within_root("/anime/x", "") is False


def test_apply_mapping_replaces_component_boundary() -> None:
    assert apply_mapping("/data/media/anime/Show/S01E01.mkv", ANIME_TO_SHOKO) == (
        "/anime/Show/S01E01.mkv"
    )
    assert apply_mapping("/data/media/anime", ANIME_TO_SHOKO) == "/anime"


def test_apply_mapping_preserves_meaningful_component_whitespace() -> None:
    path = "/data/media/anime/Series /file.mkv "
    assert apply_mapping(path, ANIME_TO_SHOKO) == "/anime/Series /file.mkv "


def test_apply_mapping_rejects_outside_paths_and_escapes() -> None:
    with pytest.raises(PathError):
        apply_mapping("/data/media/other/x.mkv", ANIME_TO_SHOKO)
    with pytest.raises(PathError):
        apply_mapping("/data/media", ANIME_TO_SHOKO)
    with pytest.raises(PathError):
        apply_mapping("/data/media/anime/../x.mkv", ANIME_TO_SHOKO)


def test_select_mapping_prefers_most_specific() -> None:
    # Built directly: validate_mappings refuses nested sets, select_mapping only resolves
    # the most specific match and reports equal-specificity ties as ambiguous.
    mappings = [
        ANIME_TO_SHOKO,
        PathMapping(sonarr="/data/media", shoko="/media-root"),
        PathMapping(sonarr="/data/media/anime/Specials", shoko="/anime-specials"),
    ]
    assert select_mapping("/data/media/anime/Show/x.mkv", mappings) == ANIME_TO_SHOKO
    assert select_mapping("/data/media/anime/Specials/x.mkv", mappings) == PathMapping(
        sonarr="/data/media/anime/Specials", shoko="/anime-specials"
    )
    # /data/media/anime-old is inside /data/media (component-aware) but not inside
    # /data/media/anime, so the broad mapping resolves it.
    assert select_mapping("/data/media/anime-old/x.mkv", mappings) == PathMapping(
        sonarr="/data/media", shoko="/media-root"
    )
    assert select_mapping("/other/x.mkv", mappings) is None


def test_select_mapping_reports_equal_specificity_ties() -> None:
    mappings = [PathMapping("/a", "/x"), PathMapping("/a", "/y")]
    with pytest.raises(PathError, match="ambiguous"):
        select_mapping("/a/file.mkv", mappings)


def test_validate_mappings_rejects_nested_sources() -> None:
    nested = [
        PathMapping(sonarr="/data/media", shoko="/media-root"),
        PathMapping(sonarr="/data/media/anime", shoko="/anime"),
    ]
    with pytest.raises(PathError, match="nested"):
        validate_mappings(nested)


def test_validate_mappings_rejects_empty_and_conflicting() -> None:
    with pytest.raises(PathError, match="at least one"):
        validate_mappings([])
    conflicting = [
        PathMapping(sonarr="/data/media/anime", shoko="/anime"),
        PathMapping(sonarr="/data/media/anime", shoko="/shoko-anime"),
    ]
    with pytest.raises(PathError, match="conflicting"):
        validate_mappings(conflicting)


def test_validate_mappings_deduplicates_and_orders_by_depth() -> None:
    ordered = validate_mappings(
        [
            PathMapping(sonarr="/data/media/anime", shoko="/anime"),
            PathMapping(sonarr="/data/media/anime", shoko="/anime"),
            PathMapping(sonarr="/anime", shoko="/shoko-anime"),
        ]
    )
    assert [m.sonarr for m in ordered] == ["/anime", "/data/media/anime"]
