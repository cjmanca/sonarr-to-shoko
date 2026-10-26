"""Container bootstrap validates identity and drops privileges before the CLI."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import Mock, call

import pytest

from shoko_linker import container_entrypoint as entrypoint


@pytest.mark.parametrize("value", ["0", "-1", "", "abc", "1:2", " 99", "4294967295"])
@pytest.mark.parametrize("name", ["PUID", "PGID"])
def test_invalid_identity_never_prepares_state_or_executes(
    monkeypatch: pytest.MonkeyPatch, name: str, value: str
) -> None:
    monkeypatch.setattr(os, "getuid", lambda: 0, raising=False)
    monkeypatch.setattr(os, "getgid", lambda: 0, raising=False)
    monkeypatch.delenv("PUID", raising=False)
    monkeypatch.delenv("PGID", raising=False)
    monkeypatch.setenv(name, value)
    prepare, execute = Mock(), Mock()
    monkeypatch.setattr(entrypoint, "_prepare_state", prepare)
    monkeypatch.setattr(os, "execvp", execute)
    with pytest.raises(SystemExit) as error:
        entrypoint.main()
    assert error.value.code == 1
    prepare.assert_not_called()
    execute.assert_not_called()


@pytest.mark.parametrize("configured", [False, True])
def test_root_drops_groups_then_gid_then_uid_before_exec(
    monkeypatch: pytest.MonkeyPatch, configured: bool
) -> None:
    calls = Mock()
    monkeypatch.setattr(os, "getuid", lambda: 0, raising=False)
    monkeypatch.setattr(os, "getgid", lambda: 0, raising=False)
    for name, value in (("PUID", "99"), ("PGID", "100")):
        monkeypatch.delenv(name, raising=False)
        if configured:
            monkeypatch.setenv(name, value)
    monkeypatch.setattr(entrypoint, "_prepare_state", calls.prepare)
    for name in ("setgroups", "setgid", "setuid", "execvp"):
        monkeypatch.setattr(os, name, getattr(calls, name), raising=False)
    monkeypatch.setattr(sys, "argv", ["entrypoint", "validate", "--offline"])
    entrypoint.main()
    uid, gid = (99, 100) if configured else (1000, 1000)
    assert calls.mock_calls == [
        call.prepare(uid, gid),
        call.setgroups([]),
        call.setgid(gid),
        call.setuid(uid),
        call.execvp("shoko-sonarr-linker", ["shoko-sonarr-linker", "validate", "--offline"]),
    ]


@pytest.mark.parametrize("mismatch", [False, True])
def test_explicit_nonroot_user_skips_ownership_and_rejects_mismatched_ids(
    monkeypatch: pytest.MonkeyPatch, mismatch: bool
) -> None:
    monkeypatch.setattr(os, "getuid", lambda: 99, raising=False)
    monkeypatch.setattr(os, "getgid", lambda: 100, raising=False)
    monkeypatch.setenv("PUID", "1000" if mismatch else "99")
    monkeypatch.setenv("PGID", "100")
    prepare, execute = Mock(), Mock()
    monkeypatch.setattr(entrypoint, "_prepare_state", prepare)
    monkeypatch.setattr(os, "execvp", execute)
    if mismatch:
        with pytest.raises(SystemExit):
            entrypoint.main()
        execute.assert_not_called()
    else:
        entrypoint.main()
        execute.assert_called_once()
    prepare.assert_not_called()


def test_ownership_failure_prevents_exec(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(os, "getuid", lambda: 0, raising=False)
    monkeypatch.setattr(os, "getgid", lambda: 0, raising=False)
    monkeypatch.delenv("PUID", raising=False)
    monkeypatch.delenv("PGID", raising=False)
    monkeypatch.setattr(entrypoint, "_prepare_state", Mock(side_effect=PermissionError("denied")))
    execute = Mock()
    monkeypatch.setattr(os, "execvp", execute)
    with pytest.raises(SystemExit):
        entrypoint.main()
    execute.assert_not_called()


def test_ownership_covers_existing_state(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    state = tmp_path / "data"
    cache = state / "mappings"
    cache.mkdir(parents=True)
    database = state / "state.sqlite3"
    database.write_text("existing state", encoding="utf-8")
    snapshot = cache / "snapshot.xml"
    snapshot.write_text("existing mappings", encoding="utf-8")
    monkeypatch.setattr(entrypoint, "STATE_DIRECTORY", state)
    chown = Mock()
    monkeypatch.setattr(os, "chown", chown, raising=False)
    entrypoint._prepare_state(12345, 12346)
    assert {args.args[0] for args in chown.call_args_list} == {state, cache, database, snapshot}
    assert all(args.kwargs == {"follow_symlinks": False} for args in chown.call_args_list)
    assert database.read_text(encoding="utf-8") == "existing state"
