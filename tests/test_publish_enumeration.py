"""Offline gate tests for scripts/enumerate_introduced.py (M8).

Synthetic git repos in tmp_path; the tag set is injected (known_tags), so no
network is needed. Covers the PLAN M8 enumeration cases: single/multi-commit
push, first push with first-buildable boundary, feature-branch merge-base,
force-push non-ancestor, backfill gap, and max_shas overflow reporting.
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "enumerate_introduced.py"
NONPOSITIVE_CAP_EXIT = 2  # CLI exit code for a rejected --max-shas


def _load_enumerator() -> Any:
    spec = importlib.util.spec_from_file_location("enumerate_introduced", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # required for dataclass annotation resolution under 3.14
    spec.loader.exec_module(module)
    return module


enum = _load_enumerator()


def _git(repo: Path, *args: str) -> str:
    env = os.environ.copy()
    env.update(
        {
            "GIT_AUTHOR_NAME": "test",
            "GIT_AUTHOR_EMAIL": "test@example.invalid",
            "GIT_COMMITTER_NAME": "test",
            "GIT_COMMITTER_EMAIL": "test@example.invalid",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_COUNT": "1",
            "GIT_CONFIG_KEY_0": "commit.gpgsign",
            "GIT_CONFIG_VALUE_0": "false",
        }
    )
    proc = subprocess.run(  # noqa: S603 - fixed argv to the git CLI fixture
        ["git", *args],  # noqa: S607 - git CLI fixture under test
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        msg = f"git {' '.join(args)} failed: {proc.stderr.strip()}"
        raise RuntimeError(msg)
    return proc.stdout.strip()


def _init_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    return repo


def _commit(repo: Path, label: str) -> str:
    (repo / f"{label}.txt").write_text(label, encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", label)
    return _git(repo, "rev-parse", "HEAD")


def test_single_commit_push(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    _commit(repo, "a")
    b = _commit(repo, "b")
    result = enum.enumerate_introduced(repo, after=b, before=_git(repo, "rev-parse", "HEAD~1"))
    assert result.shas == [b]
    assert result.overflow == []
    assert result.skipped == []


def test_multi_commit_push_is_chronological_and_complete(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    a = _commit(repo, "a")
    b = _commit(repo, "b")
    c = _commit(repo, "c")
    d = _commit(repo, "d")
    result = enum.enumerate_introduced(repo, after=d, before=a)
    assert result.shas == [b, c, d]
    assert result.overflow == []


def test_first_push_excludes_pre_boundary_history(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    p1 = _commit(repo, "p1")
    p2 = _commit(repo, "p2")
    boundary = _commit(repo, "boundary")
    c3 = _commit(repo, "c3")
    result = enum.enumerate_introduced(
        repo, after=c3, before=None, first_buildable=boundary, max_shas=100
    )
    assert result.shas == [boundary, c3]
    assert {s.sha for s in result.skipped} == {p1, p2}
    assert all(s.reason == "pre-boundary" for s in result.skipped)
    assert any("boundary" in note for note in result.notes)


def test_first_push_without_boundary_enumerates_full_history(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    a = _commit(repo, "a")
    b = _commit(repo, "b")
    result = enum.enumerate_introduced(repo, after=b, before=None, max_shas=100)
    assert result.shas == [a, b]
    assert result.skipped == []
    assert any("no first-buildable boundary" in note for note in result.notes)


def test_feature_branch_uses_merge_base_and_skips_shared_history(
    tmp_path: Path,
) -> None:
    repo = _init_repo(tmp_path)
    _commit(repo, "a")
    _commit(repo, "b")
    _git(repo, "checkout", "-q", "-b", "feature")
    c = _commit(repo, "c")
    d = _commit(repo, "d")
    result = enum.enumerate_introduced(
        repo, after=d, before=None, ref="refs/heads/feature", max_shas=100
    )
    assert result.shas == [c, d]
    assert any("merge-base" in note for note in result.notes)
    assert any("not re-tagged" in note for note in result.notes)


def test_force_push_non_ancestor_falls_back_to_merge_base(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    m = _commit(repo, "m")
    x1 = _commit(repo, "x1")
    _git(repo, "reset", "--hard", "-q", m)
    x2 = _commit(repo, "x2")
    result = enum.enumerate_introduced(repo, after=x2, before=x1)
    assert result.shas == [x2]
    assert any("non-ancestor" in note for note in result.notes)
    assert any(m[:12] in note for note in result.notes)


def test_empty_push_before_equals_after(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    a = _commit(repo, "a")
    result = enum.enumerate_introduced(repo, after=a, before=a)
    assert result.shas == []
    assert any("empty push" in note for note in result.notes)


def test_enumerate_missing_returns_exactly_untagged_shas(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    a = _commit(repo, "a")
    b = _commit(repo, "b")
    c = _commit(repo, "c")
    known = {f"sha-{a}", f"sha-{b}", "latest"}
    result = enum.enumerate_missing(repo, upto=c, known_tags=known, max_shas=100)
    assert result.shas == [c]
    assert {s.sha for s in result.skipped} == {a, b}
    assert all(s.reason == "already-tagged" for s in result.skipped)


def test_enumerate_missing_tolerates_ls_remote_tag_forms(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    a = _commit(repo, "a")
    b = _commit(repo, "b")
    known = {f"refs/tags/sha-{a}", f"refs/tags/sha-{a}^{{}}", "refs/tags/latest"}
    result = enum.enumerate_missing(repo, upto=b, known_tags=known, max_shas=100)
    assert result.shas == [b]


def test_enumerate_missing_since_bounds_the_range_gap(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    a = _commit(repo, "a")
    b = _commit(repo, "b")
    c = _commit(repo, "c")
    d = _commit(repo, "d")
    result = enum.enumerate_missing(repo, upto=d, since=a, known_tags=set(), max_shas=100)
    assert result.shas == [b, c, d]
    result2 = enum.enumerate_missing(repo, upto=d, since=c, known_tags=set(), max_shas=100)
    assert result2.shas == [d]


def test_max_shas_overflow_is_reported_not_dropped_oldest_first(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    a = _commit(repo, "a")
    b = _commit(repo, "b")
    c = _commit(repo, "c")
    d = _commit(repo, "d")
    e = _commit(repo, "e")
    result = enum.enumerate_introduced(repo, after=e, before=a, max_shas=2)
    assert result.shas == [b, c]
    assert result.overflow == [d, e]
    assert any("overflow" in note for note in result.notes)
    missing = enum.enumerate_missing(repo, upto=e, known_tags=set(), max_shas=2)
    assert missing.shas == [a, b]
    assert missing.overflow == [c, d, e]


def test_zero_and_negative_max_shas_fail_closed(tmp_path: Path) -> None:
    """M8 review F4: a non-positive cap must abort, not silently bypass the cap."""
    repo = _init_repo(tmp_path)
    a = _commit(repo, "a")
    b = _commit(repo, "b")
    for bad in (0, -3):
        with pytest.raises(ValueError, match="positive integer"):
            enum.enumerate_introduced(repo, after=b, before=a, max_shas=bad)
        with pytest.raises(ValueError, match="positive integer"):
            enum.enumerate_missing(repo, upto=b, known_tags=set(), max_shas=bad)


def test_cli_rejects_nonpositive_max_shas(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    a = _commit(repo, "a")
    b = _commit(repo, "b")
    for bad in ("0", "-5"):
        proc = subprocess.run(  # noqa: S603 - fixed argv to the CLI under test
            [
                sys.executable,
                str(SCRIPT),
                "introduced",
                "--repo",
                str(repo),
                "--before",
                a,
                "--after",
                b,
                "--max-shas",
                bad,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert proc.returncode == NONPOSITIVE_CAP_EXIT
        assert "positive" in proc.stderr
        assert proc.stdout == ""


def test_zero_sha_before_selects_first_push_path(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    boundary = _commit(repo, "boundary")
    c2 = _commit(repo, "c2")
    result = enum.enumerate_introduced(
        repo,
        after=c2,
        before="0" * 40,
        first_buildable=boundary,
        max_shas=100,
    )
    assert result.shas == [boundary, c2]


def test_cli_json_contract(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    a = _commit(repo, "a")
    b = _commit(repo, "b")
    tags_file = tmp_path / "tags.txt"
    tags_file.write_text(f"sha-{a}\nlatest\n", encoding="utf-8")
    proc = subprocess.run(  # noqa: S603
        [
            sys.executable,
            str(SCRIPT),
            "introduced",
            "--repo",
            str(repo),
            "--before",
            a,
            "--after",
            b,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0
    data = json.loads(proc.stdout)
    assert data["shas"] == [b]
    proc2 = subprocess.run(  # noqa: S603
        [
            sys.executable,
            str(SCRIPT),
            "missing",
            "--repo",
            str(repo),
            "--upto",
            b,
            "--known-tags-file",
            str(tags_file),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc2.returncode == 0
    assert json.loads(proc2.stdout)["shas"] == [b]


def test_git_error_on_unknown_revision(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    _commit(repo, "a")
    with pytest.raises(RuntimeError, match="rev-parse"):
        enum.enumerate_introduced(repo, after="deadbeef" * 5)
