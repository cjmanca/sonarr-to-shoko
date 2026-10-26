"""Prepare container state ownership, then exec the CLI without root privileges."""

# ruff: noqa: B009 -- POSIX-only functions are resolved dynamically for Windows tooling.

from __future__ import annotations

import os
import re
import stat
import sys
from pathlib import Path

STATE_DIRECTORY = Path("/data")
MAX_ID = 2**32 - 2


def _identity(name: str, default: int) -> int:
    value = os.environ.get(name, str(default))
    if not re.fullmatch(r"[0-9]{1,10}", value) or not 0 < int(value) <= MAX_ID:
        raise ValueError(f"{name} must be a positive numeric ID (root ID 0 is not allowed)")
    return int(value)


def _prepare_state(uid: int, gid: int) -> None:
    if STATE_DIRECTORY.is_symlink():
        raise ValueError("/data must be a directory, not a symbolic link")
    STATE_DIRECTORY.mkdir(parents=True, exist_ok=True)

    def own(path: Path) -> None:
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode):
            return
        if info.st_uid != uid or info.st_gid != gid:
            getattr(os, "chown")(path, uid, gid, follow_symlinks=False)

    def fail(error: OSError) -> None:
        raise error

    own(STATE_DIRECTORY)
    for directory, dirs, files in os.walk(STATE_DIRECTORY, followlinks=False, onerror=fail):
        for name in (*dirs, *files):
            own(Path(directory) / name)


def main() -> None:
    try:
        current_uid, current_gid = getattr(os, "getuid")(), getattr(os, "getgid")()
        uid = _identity("PUID", current_uid or 1000)
        gid = _identity("PGID", current_gid or 1000)
        if current_uid == 0:
            _prepare_state(uid, gid)
            getattr(os, "setgroups")([])
            getattr(os, "setgid")(gid)
            getattr(os, "setuid")(uid)
        elif (uid, gid) != (current_uid, current_gid):
            raise ValueError("PUID/PGID must match the runtime user when using Docker --user")
        # Exec the installed CLI so it receives container signals directly.
        os.execvp(  # noqa: S606 -- exec after dropping privileges
            "shoko-sonarr-linker",  # noqa: S607 -- fixed command in the image's PATH
            ["shoko-sonarr-linker", *sys.argv[1:]],
        )
    except (OSError, ValueError) as exc:
        print(f"Container startup failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
