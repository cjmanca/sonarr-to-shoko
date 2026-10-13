#!/usr/bin/env python3
"""Enumerate commit SHAs introduced by a git push, for GHCR per-commit publishing.

Standard library + git CLI plumbing only. The publish workflow calls this (via
its ``introduced`` / ``missing`` subcommands) to decide exactly which commits a
push introduced, so *every* commit inside a multi-commit push gets its own
immutable ``sha-<fullsha>`` image, with no silent gaps.

Enumeration semantics (see AGENTS/PROJECT §6 delivery policy):

* Normal push (``before`` is an ancestor of ``after``): ``before..after``,
  chronological oldest-first.
* First push to the default branch (no usable ``before``): enumerate from the
  recorded first-buildable boundary (boundary inclusive); commits *before* the
  boundary are reported as skipped ``pre-boundary`` history, never published.
* New feature-branch push (``ref`` != default branch): enumerate from
  ``merge-base(default_branch, after)`` to ``after``. Shared history that
  predates the merge-base is intentionally excluded — it was (or will be)
  covered by default-branch enumeration and must not be re-tagged.
* Force-push / non-ancestor (``before`` not an ancestor of ``after``):
  fall back to ``merge-base(before, after)..after`` with a ``non-ancestor``
  note; the rewritten commits are new SHAs and are published as such.
* Backfill (``enumerate_missing``): SHAs reachable from ``upto`` (optionally
  after ``since``) that lack a ``sha-<fullsha>`` tag, given the tag set the
  caller fetched from the GHCR registry (``scripts/ghcr_tags.py``; the
  historical ``git ls-remote`` / ``gh`` forms are still accepted). Injected as
  ``known_tags`` so tests need no network. ``unverified`` lists SHAs whose
  ``sha-`` tag exists in the registry but whose provenance could not be
  verified: they are re-scheduled (a ``sha-`` tag is content-addressed, so a
  re-push is safe) instead of being skipped as already published.

Ordering and capping: results are chronological, **oldest first**. When
``max_shas`` caps the list, the oldest prefix is kept and the remainder is
returned in ``overflow`` — never silently dropped. ``max_shas`` must be
positive: zero/negative values fail closed (CLI exit 2, library ``ValueError``)
rather than bypassing the cap. Rationale: oldest-first
retention keeps the published tag chain contiguous with already-published
history (a gap is healed in one forward sweep by backfill); the
newest-first alternative is deliberately rejected because it leaves the
older hole open longer and complicates digest audits.

Tag immutability: ``sha-`` tags are named by full commit SHA, so one tag
name maps to exactly one commit. The workflow never republishes an SHA it
can see already tagged (``enumerate_missing`` filters on ``known_tags``),
so published ``sha-`` tags are not overwritten in practice.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_MAX_SHAS = 10
TAG_PREFIX = "sha-"
ZERO_SHA = "0" * 40


class GitError(RuntimeError):
    """A git plumbing command failed."""


@dataclass(frozen=True)
class SkippedCommit:
    sha: str
    reason: str


@dataclass
class EnumerationResult:
    """Enumerated SHAs (chronological, oldest first) plus audit trail."""

    shas: list[str] = field(default_factory=list)
    skipped: list[SkippedCommit] = field(default_factory=list)
    overflow: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, object]:
        return {
            "shas": list(self.shas),
            "skipped": [{"sha": s.sha, "reason": s.reason} for s in self.skipped],
            "overflow": list(self.overflow),
            "notes": list(self.notes),
        }


def _run(repo_dir: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603 - fixed argv to the git CLI, no shell, no user input in argv[0]
        ["git", "-C", str(repo_dir), *args],  # noqa: S607 - git CLI is the intended plumbing tool
        capture_output=True,
        text=True,
        check=False,
    )


def _git(repo_dir: Path, *args: str) -> str:
    proc = _run(repo_dir, *args)
    if proc.returncode != 0:
        msg = f"git {' '.join(args)} failed (rc={proc.returncode}): {proc.stderr.strip()}"
        raise GitError(msg)
    return proc.stdout.strip()


def _resolve_commit(repo_dir: Path, rev: str) -> str:
    return _git(repo_dir, "rev-parse", "--verify", f"{rev}^{{commit}}")


def _is_ancestor(repo_dir: Path, a: str, b: str) -> bool:
    return _run(repo_dir, "merge-base", "--is-ancestor", a, b).returncode == 0


def _merge_base(repo_dir: Path, a: str, b: str) -> str | None:
    proc = _run(repo_dir, "merge-base", a, b)
    if proc.returncode != 0:
        return None
    return proc.stdout.strip() or None


def _rev_list(repo_dir: Path, *range_args: str) -> list[str]:
    out = _git(repo_dir, "rev-list", "--reverse", *range_args)
    return out.split() if out else []


def _apply_cap(result: EnumerationResult, max_shas: int) -> EnumerationResult:
    """Cap to ``max_shas`` keeping the oldest prefix; overflow is reported, never dropped.

    ``max_shas`` must be positive: a zero/negative cap fails closed (``ValueError``)
    instead of silently bypassing the cap, so a malformed dispatch input can
    never widen the per-run publish matrix.
    """
    if max_shas <= 0:
        msg = (
            f"max_shas must be a positive integer (got {max_shas}); "
            "refusing to enumerate under a non-positive cap"
        )
        raise ValueError(msg)
    if len(result.shas) > max_shas:
        result.overflow = result.shas[max_shas:]
        result.shas = result.shas[:max_shas]
        result.notes.append(
            f"overflow: {len(result.overflow)} sha(s) beyond max_shas={max_shas}; "
            "kept oldest-first prefix for contiguity; recover via backfill "
            "(workflow_dispatch backfill=true / weekly schedule)"
        )
    return result


def _enumerate_range(
    repo_dir: Path, before_full: str, after_full: str, result: EnumerationResult
) -> None:
    if before_full == after_full:
        result.notes.append("empty push: before == after; nothing introduced")
        return
    if _is_ancestor(repo_dir, before_full, after_full):
        result.shas = _rev_list(repo_dir, f"{before_full}..{after_full}")
        result.notes.append(f"fast-forward: {before_full[:12]}..{after_full[:12]}")
        return
    mb = _merge_base(repo_dir, before_full, after_full)
    if mb is None:
        result.shas = _rev_list(repo_dir, after_full)
        result.notes.append(
            "non-ancestor: before shares no history with after (disjoint roots); "
            "enumerated full history of after"
        )
        return
    result.shas = _rev_list(repo_dir, f"{mb}..{after_full}")
    result.notes.append(
        f"non-ancestor: force-push, enumerated from merge-base {mb[:12]} "
        "(rewritten commits are new SHAs and are published as such)"
    )


def _enumerate_from_boundary(
    repo_dir: Path, after_full: str, first_buildable: str | None, result: EnumerationResult
) -> None:
    if not first_buildable:
        result.shas = _rev_list(repo_dir, after_full)
        result.notes.append(
            "first push: no first-buildable boundary recorded; enumerated full history"
        )
        return
    boundary_full = _resolve_commit(repo_dir, first_buildable)
    if not _is_ancestor(repo_dir, boundary_full, after_full):
        result.shas = _rev_list(repo_dir, after_full)
        result.skipped.append(SkippedCommit(boundary_full, "boundary-not-ancestor"))
        result.notes.append(
            "first push: boundary is not an ancestor of after; enumerated full history"
        )
        return
    ancestors = _rev_list(repo_dir, boundary_full)
    result.skipped = [
        SkippedCommit(sha, "pre-boundary") for sha in ancestors if sha != boundary_full
    ]
    result.shas = [boundary_full, *_rev_list(repo_dir, f"{boundary_full}..{after_full}")]
    result.notes.append(
        "first push to default branch: enumerated from first-buildable boundary "
        "(inclusive); older history skipped as pre-boundary, never published"
    )


def _enumerate_new_branch(
    repo_dir: Path, after_full: str, default_branch: str, result: EnumerationResult
) -> None:
    default = _run(
        repo_dir,
        "rev-parse",
        "--verify",
        f"refs/remotes/origin/{default_branch}^{{commit}}",
    )
    if default.returncode != 0:
        default = _run(repo_dir, "rev-parse", "--verify", f"refs/heads/{default_branch}^{{commit}}")
    if default.returncode != 0:
        result.shas = _rev_list(repo_dir, after_full)
        result.notes.append(
            f"new branch push: default branch '{default_branch}' not found as a remote-tracking "
            "or local ref; "
            "enumerated full history"
        )
        return
    mb = _merge_base(repo_dir, default.stdout.strip(), after_full)
    if mb is None:
        result.shas = _rev_list(repo_dir, after_full)
        result.notes.append(
            "new branch push: no merge-base with default branch; enumerated full history"
        )
        return
    result.shas = _rev_list(repo_dir, f"{mb}..{after_full}")
    result.notes.append(
        f"new branch push: enumerated from merge-base {mb[:12]} with '{default_branch}'; "
        "shared history predates the merge-base and is not re-tagged; branch pushes are "
        "published as sha tags, latest promotion is default-branch-only"
    )


def enumerate_introduced(
    repo_dir: str | Path,
    *,
    after: str,
    before: str | None = None,
    ref: str = "refs/heads/main",
    default_branch: str = "main",
    first_buildable: str | None = None,
    max_shas: int = DEFAULT_MAX_SHAS,
) -> EnumerationResult:
    """SHAs introduced by a push (chronological, oldest first) with audit trail.

    ``before``/``after`` are git revisions (empty-string/zero-SHA
    ``before`` means "new ref" and selects the first-push / new-branch path).
    ``ref`` is the pushed ref (``refs/heads/<name>``). ``first_buildable`` is
    the recorded first-buildable boundary included by the first-push path.
    ``max_shas`` caps the published list; the tail lands in ``overflow``.
    """
    repo = Path(repo_dir)
    result = EnumerationResult()
    after_full = _resolve_commit(repo, after)
    if before is not None and before != ZERO_SHA:
        before_full = _resolve_commit(repo, before)
        _enumerate_range(repo, before_full, after_full, result)
    elif ref in (f"refs/heads/{default_branch}", default_branch):
        _enumerate_from_boundary(repo, after_full, first_buildable, result)
    else:
        _enumerate_new_branch(repo, after_full, default_branch, result)
    return _apply_cap(result, max_shas)


def enumerate_missing(
    repo_dir: str | Path,
    *,
    upto: str,
    known_tags: set[str],
    tag_prefix: str = TAG_PREFIX,
    since: str | None = None,
    max_shas: int = DEFAULT_MAX_SHAS,
    unverified: set[str] | None = None,
) -> EnumerationResult:
    """SHAs reachable from ``upto`` lacking a verified ``sha-<fullsha>`` tag.

    ``known_tags`` is the set of tag *names*. Registry-format names
    (``sha-<fullsha>`` from ``scripts/ghcr_tags.py``) and the historical
    ``git ls-remote`` forms (``refs/tags/`` prefixes, ``^{}`` peel refs) are
    both accepted; injection keeps this offline-testable.

    ``unverified`` carries SHAs whose registry tag exists but whose provenance
    could not be confirmed. Those SHAs are scheduled for publication instead of
    being skipped, so backfill never trusts an unverified tag as "published".
    ``since`` (exclusive) restricts the scan to ``since..upto`` for targeted
    range-gap backfill; default scans the whole history up to ``upto``.
    """
    repo = Path(repo_dir)
    upto_full = _resolve_commit(repo, upto)
    published = {
        name.strip().lower().removeprefix("refs/tags/").removeprefix(tag_prefix).split("^")[0]
        for name in known_tags
    }
    unverified_set = {sha.strip().lower() for sha in (unverified or set())}
    scan = f"{_resolve_commit(repo, since)}..{upto_full}" if since else upto_full
    result = EnumerationResult()
    for sha in _rev_list(repo, scan):
        if sha in published and sha not in unverified_set:
            result.skipped.append(SkippedCommit(sha, "already-tagged"))
        elif sha in unverified_set:
            result.shas.append(sha)
            result.skipped.append(SkippedCommit(sha, "tag-provenance-unverified"))
        else:
            result.shas.append(sha)
    result.notes.append(
        f"backfill scan {scan}: {len(result.shas)} scheduled sha(s), "
        f"{sum(1 for item in result.skipped if item.reason == 'already-tagged')} verified-published"
    )
    if unverified_set:
        result.notes.append(
            f"provenance: {len(unverified_set)} registry tag(s) unverified; re-scheduled"
        )
    return _apply_cap(result, max_shas)


def _add_common(sub: argparse.ArgumentParser) -> None:
    sub.add_argument("--repo", default=".", help="repository directory (default: cwd)")
    sub.add_argument("--max-shas", type=int, default=DEFAULT_MAX_SHAS)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    introduced = sub.add_parser("introduced", help="SHAs introduced by a push")
    _add_common(introduced)
    introduced.add_argument("--after", required=True)
    introduced.add_argument("--before", default=None)
    introduced.add_argument("--ref", default="refs/heads/main")
    introduced.add_argument("--default-branch", default="main")
    introduced.add_argument("--first-buildable", default=None)

    missing = sub.add_parser("missing", help="untagged SHAs reachable from --upto (backfill)")
    _add_common(missing)
    missing.add_argument("--upto", required=True)
    missing.add_argument("--since", default=None)
    missing.add_argument("--known-tags-file", required=True, help="file of tag names, one per line")
    missing.add_argument(
        "--unverified-file",
        default=None,
        help="file of SHAs whose registry tag is provenance-unverified (re-scheduled)",
    )
    missing.add_argument("--tag-prefix", default=TAG_PREFIX)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.max_shas <= 0:
        print(
            f"error: --max-shas must be a positive integer (got {args.max_shas})",
            file=sys.stderr,
        )
        return 2
    if args.command == "introduced":
        result = enumerate_introduced(
            args.repo,
            after=args.after,
            before=args.before,
            ref=args.ref,
            default_branch=args.default_branch,
            first_buildable=args.first_buildable,
            max_shas=args.max_shas,
        )
    else:
        known = Path(args.known_tags_file).read_text(encoding="utf-8").splitlines()
        unverified: set[str] = set()
        if args.unverified_file:
            unverified = Path(args.unverified_file).read_text(encoding="utf-8").splitlines()
        result = enumerate_missing(
            args.repo,
            upto=args.upto,
            known_tags=set(known),
            tag_prefix=args.tag_prefix,
            since=args.since,
            max_shas=args.max_shas,
            unverified=set(unverified),
        )
    json.dump(result.to_dict(), sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
