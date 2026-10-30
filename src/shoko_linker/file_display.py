"""Pure display projection from retained, structured file paths and confirmed scope."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

from .errors import PathError
from .logging_setup import diagnostic_text
from .paths import PathMapping, apply_mapping, is_within_root, normalize_posix, path_parts

if TYPE_CHECKING:
    from .state import ServiceFileExample

MAX_DISPLAY_PATH_CHARS = 8192
MAX_DISPLAY_LOCATIONS = 16


@dataclass(frozen=True)
class FileDisplay:
    filename: str | None = None
    relative_path: str | None = None
    library: str | None = None


def safe_display_path(value: str) -> str | None:
    """A redacted or truncated path is display text, never identity evidence."""
    if not value or len(value) > MAX_DISPLAY_PATH_CHARS:
        return None
    try:
        normalized = normalize_posix(value)
    except PathError:
        return None
    return (
        normalized
        if diagnostic_text(normalized, limit=MAX_DISPLAY_PATH_CHARS) == normalized
        else None
    )


def file_display(
    sonarr_path: str,
    shoko_paths: Sequence[str],
    roots: Sequence[str],
    mappings: Sequence[PathMapping],
    *,
    paths_complete: bool = True,
) -> FileDisplay:
    """Select a library only from unique structured correspondence, never prose/names."""
    source = safe_display_path(sonarr_path)
    locations = tuple(safe_display_path(path) for path in shoko_paths[:MAX_DISPLAY_LOCATIONS])
    complete = paths_complete and len(shoko_paths) <= MAX_DISPLAY_LOCATIONS and all(locations)
    candidates: set[str] = set()
    if complete:
        for location in locations:
            if location is None:
                continue
            applicable = [
                mapping for mapping in mappings if is_within_root(location, mapping.shoko)
            ]
            if len(applicable) != 1:
                complete = False
                break
            translated = apply_mapping(
                location, PathMapping(applicable[0].shoko, applicable[0].sonarr)
            )
            candidates.add(translated)
    if source is not None:
        candidates.add(source)
    elif complete and len(candidates) == 1:
        source = next(iter(candidates))
    filename = PurePosixPath(source).name if source is not None else None
    if filename is None and len(locations) == 1 and locations[0] is not None:
        filename = PurePosixPath(locations[0]).name
    if not complete or source is None or candidates != {source}:
        return FileDisplay(filename=filename)
    safe_roots = {root for value in roots if (root := safe_display_path(value)) is not None}
    covering = [root for root in safe_roots if is_within_root(source, root) and source != root]
    if not covering:
        return FileDisplay(filename=filename)
    depth = max(len(path_parts(root)) for root in covering)
    winners = [root for root in covering if len(path_parts(root)) == depth]
    if len(winners) != 1:
        return FileDisplay(filename=filename)
    library = winners[0]
    relative = PurePosixPath(source).relative_to(library)
    return FileDisplay(filename, str(relative.parent), library)


def tracked_file_display(
    example: ServiceFileExample, roots: Sequence[str], mappings: Sequence[PathMapping]
) -> FileDisplay:
    """Preserve the tracked projector's proof-loss marker after diagnostic sanitization."""
    return file_display(
        example.sonarr_path,
        example.shoko_paths,
        roots,
        mappings,
        paths_complete=example.display_paths_complete,
    )
