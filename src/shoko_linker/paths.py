"""Component-aware POSIX path utilities for root allowlists and Docker path mappings.

Media paths in this deployment are container-internal POSIX paths (for example
``/data/media/anime``). They are compared **case-sensitively** (``/ANIME`` is not
``/anime``) and always by whole path components, never by string prefix, so
``/anime-old`` is never inside ``/anime``.

All functions here are pure: no filesystem access, no I/O, no module state.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import PurePosixPath

from .errors import PathError


@dataclass(frozen=True)
class PathMapping:
    """One explicit Sonarr -> Shoko container path mapping."""

    sonarr: str
    shoko: str


def normalize_posix(path: str) -> str:
    """Return ``path`` in normalized absolute POSIX form.

    Collapses ``//`` and ``.``, drops the trailing slash, and rejects relative paths,
    backslash separators and ``..`` segments (fail closed on escape attempts).
    """
    if not path.strip():
        raise PathError("path must be a non-empty string")
    candidate = path.strip()
    if "\\" in candidate:
        raise PathError(f"path must use POSIX separators, not backslashes: {candidate!r}")
    if not candidate.startswith("/"):
        raise PathError(f"path must be an absolute POSIX path starting with '/': {candidate!r}")
    pure = PurePosixPath(candidate)
    if ".." in pure.parts:
        raise PathError(f"path must not contain '..' segments: {candidate!r}")
    normalized = str(pure)
    if normalized == "/":
        raise PathError("path must name a directory below the filesystem root")
    return normalized


def path_parts(path: str) -> tuple[str, ...]:
    """Split a POSIX media path into components, case-sensitively.

    Raises:
        PathError: when the path is not a usable absolute POSIX path.
    """
    return PurePosixPath(normalize_posix(path)).parts[1:]


def is_within_root(path: str, root: str) -> bool:
    """True when ``path`` is ``root`` itself or a component-aware descendant of ``root``.

    ``/anime`` is within ``/anime``; ``/anime/x`` is within ``/anime``; ``/anime-old``
    is **not** within ``/anime``. Unusable paths (relative, empty, containing ``..``)
    are reported as out of scope rather than raising, so scope checks always fail closed.
    """
    try:
        root_parts = path_parts(root)
        candidate_parts = path_parts(path)
    except PathError:
        return False
    if len(candidate_parts) < len(root_parts):
        return False
    return candidate_parts[: len(root_parts)] == root_parts


def apply_mapping(path: str, mapping: PathMapping) -> str:
    """Translate ``path`` from the mapping's Sonarr side to its Shoko side.

    The source must match on an exact component boundary.

    Raises:
        PathError: when ``path`` is not inside ``mapping.sonarr``, or is not a usable
            POSIX path.
    """
    source = path_parts(mapping.sonarr)
    target = path_parts(mapping.shoko)
    candidate = path_parts(path)
    if len(candidate) < len(source) or candidate[: len(source)] != source:
        raise PathError(f"path {path!r} is not inside mapping source {mapping.sonarr!r}")
    translated = (*target, *candidate[len(source) :])
    return "/" + "/".join(translated)


def select_mapping(path: str, mappings: Sequence[PathMapping]) -> PathMapping | None:
    """Return the unique most specific mapping covering ``path``, or None when unmapped.

    Raises:
        PathError: when more than one mapping matches with equal specificity.
    """
    matches = [mapping for mapping in mappings if is_within_root(path, mapping.sonarr)]
    if not matches:
        return None
    best_depth = max(len(path_parts(mapping.sonarr)) for mapping in matches)
    winners = [mapping for mapping in matches if len(path_parts(mapping.sonarr)) == best_depth]
    if len(winners) > 1:
        sources = ", ".join(mapping.sonarr for mapping in winners)
        raise PathError(f"ambiguous path mappings for {path!r}: {sources}")
    return winners[0]


def validate_mappings(mappings: Sequence[PathMapping]) -> tuple[PathMapping, ...]:
    """Normalize a mapping set and reject nested or conflicting definitions.

    Returns the mappings ordered from least to most specific source.

    Raises:
        PathError: when the set is empty, a path is unusable, one source maps to
            several targets, or one source is nested inside another (the nesting makes
            coverage ambiguous, so it is refused instead of guessed).
    """
    if not mappings:
        raise PathError("must contain at least one {sonarr, shoko} mapping")
    normalized = [
        PathMapping(normalize_posix(m.sonarr), normalize_posix(m.shoko)) for m in mappings
    ]
    by_source: dict[tuple[str, ...], PathMapping] = {}
    for mapping in normalized:
        key = path_parts(mapping.sonarr)
        previous = by_source.get(key)
        if previous is None:
            by_source[key] = mapping
            continue
        if previous.shoko != mapping.shoko:
            raise PathError(
                f"conflicting Shoko targets for Sonarr path {mapping.sonarr!r}: "
                f"{previous.shoko!r} and {mapping.shoko!r}"
            )
    ordered = sorted(by_source.values(), key=lambda m: len(path_parts(m.sonarr)))
    for outer_index, outer in enumerate(ordered):
        for inner in ordered[outer_index + 1 :]:
            if is_within_root(inner.sonarr, outer.sonarr):
                raise PathError(
                    f"nested mappings are ambiguous: {outer.sonarr!r} contains {inner.sonarr!r}"
                )
    return tuple(ordered)
