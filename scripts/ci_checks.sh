#!/usr/bin/env bash
# Shared CI check list (M8): the exact checks the CI test job and the
# per-SHA publish matrix run against a candidate commit. Keeping one list
# guarantees per-SHA publish verification is real (run per commit, no
# cross-commit caching), not a cached pass from the test job.
# Requires: uv (setup-uv), git; run from the repository root.
set -euo pipefail

uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pytest -q
git diff --check
