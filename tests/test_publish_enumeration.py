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


def test_first_feature_push_uses_remote_tracking_default_branch(
    tmp_path: Path,
) -> None:
    repo = _init_repo(tmp_path)
    for i in range(12):
        main_tip = _commit(repo, f"main-{i}")
    _git(repo, "update-ref", "refs/remotes/origin/main", main_tip)
    _git(repo, "checkout", "-q", "-b", "feature")
    feature_1 = _commit(repo, "feature-1")
    feature_2 = _commit(repo, "feature-2")
    _git(repo, "branch", "-D", "main")

    result = enum.enumerate_introduced(
        repo,
        after=feature_2,
        before=None,
        ref="refs/heads/feature",
        default_branch="main",
        max_shas=10,
    )

    assert result.shas == [feature_1, feature_2]
    assert result.overflow == []
    assert any("merge-base" in note for note in result.notes)


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


def test_force_push_fresh_clone_recovers_without_before_object(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    pre_boundary = _commit(repo, "pre-boundary")
    boundary = _commit(repo, "boundary")
    old_tip = _commit(repo, "rewritten-away")
    _git(repo, "reset", "--hard", "-q", boundary)
    replacement = _commit(repo, "replacement")
    newest = _commit(repo, "newest")
    clone = tmp_path / "runner"
    # Transport clone, not a local object-directory copy: only reachable objects.
    _git(tmp_path, "clone", "--no-local", str(repo), str(clone))
    with pytest.raises(enum.GitError):
        enum._resolve_commit(clone, old_tip)

    result = enum.enumerate_introduced(
        clone, before=old_tip, after=newest, first_buildable=boundary, max_shas=2
    )

    assert result.shas == [boundary, replacement]
    assert result.overflow == [newest]
    assert [(item.sha, item.reason) for item in result.skipped] == [(pre_boundary, "pre-boundary")]
    assert any("degraded-history" in note and old_tip in note for note in result.notes)


def test_push_event_revision_survives_later_branch_rewrite(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    before = _commit(repo, "base")
    intermediate = _commit(repo, "event-intermediate")
    event_after = _commit(repo, "event-after")
    # Model checkout's event-SHA ref: branch advancement must not change HEAD.
    _git(repo, "checkout", "--detach", event_after)
    _git(repo, "update-ref", "refs/heads/main", before)
    assert _git(repo, "rev-parse", "main") == before

    result = enum.enumerate_introduced(repo, before=before, after=event_after)

    assert result.shas == [intermediate, event_after]


@pytest.mark.parametrize("retain_boundary_tag", [False, True])
def test_rewritten_boundary_in_clone_skips_unbuildable_history_and_recovers(
    tmp_path: Path, retain_boundary_tag: bool
) -> None:
    repo = _init_repo(tmp_path)
    prehistory = [_commit(repo, f"pre-build-{i}") for i in range(4)]
    boundary = _commit(repo, "old-boundary")
    if retain_boundary_tag:
        _git(repo, "tag", "old-buildable", boundary)
    old_tip = _commit(repo, "old-tip")
    _git(repo, "reset", "--hard", "-q", prehistory[-1])
    for name in ("Dockerfile", "pyproject.toml", "uv.lock"):
        (repo / name).write_text("synthetic input", encoding="utf-8")
    for name in ("src", "tests"):
        (repo / name).mkdir()
        (repo / name / "example.py").write_text("", encoding="utf-8")
    replacements = [_commit(repo, f"replacement-{i}") for i in range(3)]
    clone = tmp_path / "runner"
    _git(tmp_path, "clone", "--no-local", str(repo), str(clone))
    if retain_boundary_tag:
        assert enum._resolve_commit(clone, boundary) == boundary
        assert not enum._is_ancestor(clone, boundary, replacements[-1])
    else:
        with pytest.raises(enum.GitError):
            enum._resolve_commit(clone, boundary)
    with pytest.raises(enum.GitError):
        enum._resolve_commit(clone, old_tip)

    push = enum.enumerate_introduced(
        clone, before=old_tip, after=replacements[-1], first_buildable=boundary, max_shas=2
    )
    scheduled = enum.enumerate_missing(
        clone, upto=replacements[-1], first_buildable=boundary, known_tags=set(), max_shas=2
    )
    for result in (push, scheduled):
        assert result.shas == replacements[:2]
        assert result.overflow == replacements[2:]
        reasons = {item.sha: item.reason for item in result.skipped}
        expected = "boundary-not-ancestor" if retain_boundary_tag else "boundary-unavailable"
        assert reasons[boundary] == expected
        assert all(reasons[sha] == "missing-historical-build-inputs" for sha in prehistory)
        assert any("without guessing a cutoff" in note for note in result.notes)

    recovered = enum.enumerate_missing(
        clone,
        upto=replacements[-1],
        first_buildable=boundary,
        known_tags={f"sha-{sha}" for sha in scheduled.shas},
        max_shas=2,
    )
    assert recovered.shas == replacements[2:]
    assert recovered.overflow == []


def test_unavailable_boundary_with_no_build_inputs_has_explicit_skip_reasons(
    tmp_path: Path,
) -> None:
    repo = _init_repo(tmp_path)
    after = _commit(repo, "not-buildable")
    result = enum.enumerate_missing(repo, upto=after, first_buildable="f" * 40, known_tags=set())
    assert result.shas == []
    assert [(item.sha, item.reason) for item in result.skipped] == [
        ("f" * 40, "boundary-unavailable"),
        (after, "missing-historical-build-inputs"),
    ]


def test_malformed_unavailable_boundary_is_not_replaced_with_history_scan(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    after = _commit(repo, "after")
    with pytest.raises(enum.GitError):
        enum.enumerate_introduced(repo, after=after, before="e" * 40, first_buildable="bad")
    with pytest.raises(enum.GitError):
        enum.enumerate_missing(repo, upto=after, known_tags=set(), first_buildable="bad")


@pytest.mark.parametrize("before", ["not-a-sha", "f" * 39, "F" * 40, "--all"])
def test_unresolvable_malformed_before_never_uses_recovery(tmp_path: Path, before: str) -> None:
    repo = _init_repo(tmp_path)
    after = _commit(repo, "after")
    with pytest.raises(enum.GitError):
        enum.enumerate_introduced(repo, before=before, after=after)


def test_missing_event_after_never_uses_before_recovery(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    _commit(repo, "base")
    with pytest.raises(enum.GitError):
        enum.enumerate_introduced(repo, before="e" * 40, after="f" * 40)


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
    result = enum.enumerate_missing(
        repo, upto=c, known_tags=known, max_shas=100, first_buildable=None
    )
    assert result.shas == [c]
    assert {s.sha for s in result.skipped} == {a, b}
    assert all(s.reason == "already-tagged" for s in result.skipped)


def test_default_backfill_starts_at_inclusive_buildable_boundary_and_progresses(
    tmp_path: Path,
) -> None:
    repo = _init_repo(tmp_path)
    earlier = [_commit(repo, f"pre-boundary-{index}") for index in range(2)]
    boundary = _commit(repo, "boundary")
    buildable = [_commit(repo, f"buildable-{index}") for index in range(4)]

    first = enum.enumerate_missing(
        repo,
        upto=buildable[-1],
        known_tags=set(),
        max_shas=2,
        first_buildable=boundary,
    )

    assert first.shas == [boundary, buildable[0]]
    assert first.overflow == buildable[1:]
    assert {item.sha for item in first.skipped if item.reason == "pre-boundary"} == set(earlier)
    assert all(sha not in first.shas + first.overflow for sha in earlier)
    assert any("first-buildable boundary" in note and "inclusive" in note for note in first.notes)

    second = enum.enumerate_missing(
        repo,
        upto=buildable[-1],
        known_tags={f"sha-{sha}" for sha in first.shas},
        max_shas=2,
        first_buildable=boundary,
    )
    assert second.shas == buildable[1:3]
    assert second.overflow == [buildable[3]]


def test_explicit_since_range_remains_exclusive_and_overrides_default_boundary(
    tmp_path: Path,
) -> None:
    repo = _init_repo(tmp_path)
    since = _commit(repo, "explicit-since")
    before_boundary = _commit(repo, "explicit-pre-boundary")
    boundary = _commit(repo, "boundary")
    after = _commit(repo, "after")

    result = enum.enumerate_missing(
        repo,
        upto=after,
        since=since,
        known_tags=set(),
        max_shas=10,
        first_buildable=boundary,
    )

    assert result.shas == [before_boundary, boundary, after]
    assert not any(item.reason == "pre-boundary" for item in result.skipped)
    assert any("explicit backfill range" in note for note in result.notes)


def test_default_backfill_audits_unbuildable_trees_when_boundary_is_unrelated(
    tmp_path: Path,
) -> None:
    repo = _init_repo(tmp_path)
    main_tip = _commit(repo, "main")
    _git(repo, "checkout", "--orphan", "unrelated")
    unrelated_tip = _commit(repo, "unrelated")

    result = enum.enumerate_missing(
        repo,
        upto=unrelated_tip,
        known_tags=set(),
        first_buildable=main_tip,
    )

    assert result.shas == []
    assert result.overflow == []
    assert any(item.reason == "boundary-not-ancestor" for item in result.skipped)
    assert any(
        item.sha == unrelated_tip and item.reason == "missing-historical-build-inputs"
        for item in result.skipped
    )
    assert any("without guessing a cutoff" in note for note in result.notes)


def test_enumerate_missing_tolerates_ls_remote_tag_forms(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    a = _commit(repo, "a")
    b = _commit(repo, "b")
    known = {f"refs/tags/sha-{a}", f"refs/tags/sha-{a}^{{}}", "refs/tags/latest"}
    result = enum.enumerate_missing(
        repo, upto=b, known_tags=known, max_shas=100, first_buildable=None
    )
    assert result.shas == [b]


def test_enumerate_missing_registry_tag_format_zero_reschedule(tmp_path: Path) -> None:
    """F3: a repeated backfill against an unchanged registry schedules nothing."""
    repo = _init_repo(tmp_path)
    a = _commit(repo, "a")
    b = _commit(repo, "b")
    c = _commit(repo, "c")
    known = {f"sha-{a}", f"sha-{b}", f"sha-{c}", "latest"}  # ghcr_tags.py plain tag names
    result = enum.enumerate_missing(
        repo, upto=c, known_tags=known, max_shas=100, first_buildable=None
    )
    assert result.shas == []
    assert {s.sha for s in result.skipped} == {a, b, c}
    assert all(s.reason == "already-tagged" for s in result.skipped)


def test_enumerate_missing_accepts_mixed_tag_formats(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    a = _commit(repo, "a")
    b = _commit(repo, "b")
    c = _commit(repo, "c")
    known = {f"refs/tags/sha-{a}", f"refs/tags/sha-{a}^{{}}", f"sha-{b}", "SHA-B", "latest"}
    result = enum.enumerate_missing(
        repo, upto=c, known_tags=known, max_shas=100, first_buildable=None
    )
    assert result.shas == [c]
    assert {s.sha for s in result.skipped} == {a, b}


def test_enumerate_missing_unverified_provenance_reschedules(tmp_path: Path) -> None:
    """F3: an unverified sha- tag is NOT trusted as published; the SHA is re-scheduled."""
    repo = _init_repo(tmp_path)
    a = _commit(repo, "a")
    b = _commit(repo, "b")
    c = _commit(repo, "c")
    known = {f"sha-{a}", f"sha-{b}", f"sha-{c}"}
    result = enum.enumerate_missing(
        repo,
        upto=c,
        known_tags=known,
        unverified={a},
        max_shas=100,
        first_buildable=None,
    )
    assert result.shas == [a]
    reasons = {s.sha: s.reason for s in result.skipped}
    assert reasons[b] == "already-tagged"
    assert reasons[c] == "already-tagged"
    assert reasons[a] == "tag-provenance-unverified"
    assert any("provenance" in note for note in result.notes)


def test_cli_missing_accepts_unverified_file(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    a = _commit(repo, "a")
    b = _commit(repo, "b")
    tags_file = tmp_path / "tags.txt"
    tags_file.write_text(f"sha-{a}\nsha-{b}\nlatest\n", encoding="utf-8")
    unverified_file = tmp_path / "unverified.txt"
    unverified_file.write_text(f"{a}\n", encoding="utf-8")
    proc = subprocess.run(  # noqa: S603
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
            "--first-buildable",
            a,
            "--unverified-file",
            str(unverified_file),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0
    data = json.loads(proc.stdout)
    assert data["shas"] == [a]
    assert {"sha": a, "reason": "tag-provenance-unverified"} in data["skipped"]


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
    missing = enum.enumerate_missing(
        repo, upto=e, known_tags=set(), max_shas=2, first_buildable=None
    )
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
            "--first-buildable",
            a,
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
