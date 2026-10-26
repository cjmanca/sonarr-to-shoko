"""Run inside a disposable image as root: python - < scripts/smoke_container_identity.py."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

CHILD = """
import os
from pathlib import Path
from shoko_linker import container_entrypoint as entrypoint
from shoko_linker.state import StateStore

def verify(program, args):
    expected = (int(os.environ.get('PUID', '1000')), int(os.environ.get('PGID', '1000')))
    assert (os.getuid(), os.getgid()) == expected
    assert os.getgroups() == []
    assert program == 'shoko-sonarr-linker'
    assert args == ['shoko-sonarr-linker', 'service']
    store = StateStore('/data/state.sqlite3')
    assert store.connection.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
    store.connection.execute('CREATE TABLE IF NOT EXISTS smoke_marker (value TEXT)')
    store.connection.execute("INSERT INTO smoke_marker VALUES ('preserved')")
    store.close()
    print(f'non-root {expected}: database opened successfully', flush=True)

os.execvp = verify
entrypoint.sys.argv = ['entrypoint', 'service']
entrypoint.main()
"""


def main() -> None:
    outside = Path("/tmp/identity-smoke-outside")  # noqa: S108 -- disposable container only
    outside.mkdir()
    protected = outside / "protected"
    protected.write_text("unchanged", encoding="utf-8")
    Path("/data/outside-link").symlink_to(outside, target_is_directory=True)
    environment = {key: value for key, value in os.environ.items() if key not in {"PUID", "PGID"}}
    identities = ({}, {"PUID": "99", "PGID": "100"}, {"PUID": "1234", "PGID": "1234"})
    for overrides in identities:
        subprocess.run(  # noqa: S603 -- fixed interpreter and test code
            [sys.executable, "-c", CHILD], env=environment | overrides, check=True
        )
        assert protected.stat().st_uid == 0
        assert protected.read_text(encoding="utf-8") == "unchanged"
    import sqlite3

    with sqlite3.connect("/data/state.sqlite3") as connection:
        assert connection.execute("SELECT COUNT(*) FROM smoke_marker").fetchone()[0] == len(
            identities
        )
    for overrides in ({"PUID": "0"}, {"PGID": "-1"}, {"PUID": "invalid"}):
        result = subprocess.run(  # noqa: S603 -- fixed interpreter and test code
            [sys.executable, "-m", "shoko_linker.container_entrypoint", "--help"],
            env=environment | overrides,
            check=False,
        )
        assert result.returncode == 1
    print(
        "Identity changes preserved SQLite state; symlinks stayed untouched; invalid IDs refused."
    )


if __name__ == "__main__":
    main()
