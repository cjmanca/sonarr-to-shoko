#!/usr/bin/env python3
"""Workflow-side validator for historical GHCR publish legs (M8 F3).

Each publish matrix leg checks out its own candidate SHA. Commits that
predate ``scripts/ci_checks.sh`` (introduced at M8) cannot run the shared check
list from their own tree, so the plan job uploads THIS validator as a pinned
artifact and the leg runs it against the historical working tree. It validates
the historical SOURCE — it never substitutes the current commit's checks:

* ``Dockerfile`` exists (the image for this SHA must be buildable)
* ``pyproject.toml`` and ``uv.lock`` exist (the leg installs the historical
  dev group with ``uv sync --frozen`` against this commit's own lock)
* ``compileall`` over ``src`` and ``tests`` (``scripts`` tolerated: it did not
  exist at the first-buildable boundary)
* ``config.example.yaml`` parses when present (PyYAML comes from the leg env)

Reports exactly which checks ran. Exit 0 = passed; nonzero = failed.
"""

from __future__ import annotations

import argparse
import compileall
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any


def _require_file(tree: Path, name: str, checks: list[str], failures: list[str]) -> None:
    if (tree / name).is_file():
        checks.append(f"{name} present")
    else:
        failures.append(f"{name} missing")


def _compile_dir(
    tree: Path, name: str, checks: list[str], failures: list[str], *, required: bool
) -> None:
    directory = tree / name
    if not directory.is_dir():
        if required:
            failures.append(f"{name}/ directory missing")
        else:
            checks.append(f"{name}/ absent at this commit (tolerated)")
        return
    if compileall.compile_dir(str(directory), quiet=2, legacy=False):
        checks.append(f"compileall {name}/")
    else:
        failures.append(f"compileall {name}/ failed")


def _check_config_example(tree: Path, checks: list[str], failures: list[str]) -> None:
    example = tree / "config.example.yaml"
    if not example.is_file():
        checks.append("config.example.yaml absent at this commit (tolerated)")
        return
    try:
        import yaml
    except ImportError:
        checks.append("config.example.yaml present (PyYAML unavailable: parse skipped)")
        return
    try:
        loaded = yaml.safe_load(example.read_text(encoding="utf-8"))
    except Exception as exc:
        # Keep this pinned artifact standalone: old trees need not contain our package.
        # Parser messages, marks and ReaderError text can expose source credentials.
        detail = type(exc).__name__
        if isinstance(exc, yaml.MarkedYAMLError) and exc.problem_mark is not None:
            mark = exc.problem_mark
            detail += f" at line {mark.line + 1}, column {mark.column + 1}"
        failures.append(f"config.example.yaml failed to parse: {detail}")
        return
    if not isinstance(loaded, dict):
        failures.append("config.example.yaml does not parse to a mapping")
    else:
        checks.append("config.example.yaml parses")


def validate_historical(root: str | Path = ".") -> dict[str, Any]:
    """Run the bounded historical sanity checks for the tree at ``root``."""
    tree = Path(root)
    checks: list[str] = []
    failures: list[str] = []

    _require_file(tree, "Dockerfile", checks, failures)
    _require_file(tree, "pyproject.toml", checks, failures)
    _require_file(tree, "uv.lock", checks, failures)
    _compile_dir(tree, "src", checks, failures, required=True)
    _compile_dir(tree, "tests", checks, failures, required=True)
    _compile_dir(tree, "scripts", checks, failures, required=False)
    _check_config_example(tree, checks, failures)

    return {"ok": not failures, "checks": checks, "failures": failures}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--repo", default=".", help="historical working tree to validate (default: cwd)"
    )
    args = parser.parse_args(argv)
    report = validate_historical(args.repo)
    json.dump(report, sys.stdout, indent=2)
    sys.stdout.write("\n")
    if report["ok"]:
        return 0
    print("historical validation failed: " + "; ".join(report["failures"]), file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
