"""Pure display projection from retained, structured file paths and confirmed scope."""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any

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


MAX_DISPLAY_DOCUMENT_CHARS = 65536
MAX_DISPLAY_TARGETS = 256
FROZEN_TARGET_PARTS = 7
RESOLVED_TARGET_PARTS = 2
SEND_TARGET_PARTS = 3
MAX_ANIDB_ID = 2**63 - 1
SERIES_NAME_UNAVAILABLE = "Series name unavailable"


def series_display_name(value: object) -> str:
    """Untrusted labels never participate in identity or authorization."""
    if not isinstance(value, str) or len(value) > MAX_DISPLAY_DOCUMENT_CHARS:
        return ""
    return diagnostic_text(value, limit=512).strip()


def display_document(raw: object) -> Any:
    if not isinstance(raw, str) or len(raw) > MAX_DISPLAY_DOCUMENT_CHARS or "\0" in raw:
        return None
    try:
        return json.loads(raw)
    except (ValueError, RecursionError):
        return None


def display_anidb_ids(raw: object, *, frozen: bool = False) -> tuple[int, ...]:
    """Strict bounded projection of immutable send triples or frozen mapping keys."""
    document = display_document(raw)
    ids: list[int] = []
    if frozen:
        if not isinstance(document, dict) or len(document) > MAX_DISPLAY_TARGETS:
            return ()
        for key, resolved in document.items():
            target = display_document(key)
            if (
                not isinstance(target, list)
                or len(target) != FROZEN_TARGET_PARTS
                or not isinstance(resolved, list)
                or len(resolved) != RESOLVED_TARGET_PARTS
                or any(
                    value is not None and (type(value) is not int or value <= 0)
                    for value in resolved
                )
                or type(target[0]) is not int
                or target[0] <= 0
                or type(target[1]) is not int
                or target[1] < 0
                or type(target[2]) is not int
                or target[2] <= 0
                or (target[3] is not None and (type(target[3]) is not int or target[3] <= 0))
                or type(target[4]) is not int
                or not 0 < target[4] <= MAX_ANIDB_ID
                or type(target[5]) is not int
                or target[5] <= 0
                or target[6] not in ("Episode", "Special", "Trailer", "Other")
            ):
                return ()
            ids.append(target[4])
    else:
        if not isinstance(document, list) or len(document) > MAX_DISPLAY_TARGETS:
            return ()
        for target in document:
            if (
                not isinstance(target, list)
                or len(target) != SEND_TARGET_PARTS
                or any(type(value) is not int or not 0 < value <= MAX_ANIDB_ID for value in target)
            ):
                return ()
            ids.append(target[0])
    return tuple(sorted(set(ids)))
