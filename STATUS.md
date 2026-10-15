# STATUS — F34 Campaign Capacity Warning

**Run:** Implement F34 from `PLAN.md`.
**Approved PLAN.md SHA-256:** `D43140F0DF399F09EDFE5B2D9A851F4266D7293BA6DD02D06605B7FE981A20AB`.
**Baseline HEAD:** `7d848a30edf4518fe8af5d9528c5eefb86034399`.
**Validated implementation commit:** `a387e0dbeb3ba73774217c9c6b25419608495961` (`fix: warn when campaign capacity is disabled`).
**Branch:** `main` (user requested direct work without a worktree).
**Execution mode:** Offline and mocked tests. No live Sonarr/Shoko requests or writes, deployment, or publishing.

## Scope and implementation

- The current F34 `PLAN.md` was present in the working tree at task start and was adopted by the explicit user request. Its SHA-256 was recorded above; `PLAN.md` was not edited during implementation.
- `campaign_deadline_warning()` now reports disabled capacity for `max_new_per_cycle=0` and `max_pending_episode_jobs=0`, includes all zero-valued limits when combined, and retains the existing rolling-only warning and positive-capacity rolling-window calculation.
- Campaign preparation passes the configured limits to the warning helper. Warning generation does not change campaign plan fields, the requested deadline, the approval digest, configured limits, or remote-write behavior.
- Regression tests cover all-positive limits, each disabled limit, combined disabled limits, rolling horizon warnings, large insufficient/sufficient deadlines, JSON and text output, unchanged plan/deadline/digest, and zero Shoko POSTs.

## Verification and review

- New regression cases failed before implementation as expected for per-cycle and pending-job limits.
- `uv run pytest -q tests/test_campaign.py` → passed through **100%** (mocked/offline).
- `uv run pytest -q` → passed through **100%** (mocked/offline; run before the final docstring-only review correction).
- After the review correction, focused campaign preparation tests → **14 passed**.
- `uv run ruff check .` → passed; targeted Ruff check also passed after the review correction.
- `uv run ruff format --check .` → **51 files already formatted**; targeted format check also passed after the review correction.
- `uv run mypy` → passed (**47 source files**); targeted mypy also passed after the review correction.
- `git diff --check` → no whitespace errors. Git emitted its Windows CRLF-to-LF working-copy warning for `tests/test_campaign.py`.
- A sequential read-only review found no behavior issues and one P3 docstring accuracy issue; the docstring was corrected and focused tests/static checks rerun. The reviewer could not run tests in its separate environment; no reviewer test success is claimed.
- No live Sonarr/Shoko requests or writes occurred.
- Implementation commit `a387e0dbeb3ba73774217c9c6b25419608495961` and verification record commit `ae631e3fb0e00691349864686f376413a8754f4b` were pushed to `origin/main`. The remote ref was verified at `ae631e3fb0e00691349864686f376413a8754f4b` after the push.

## Next action

No further offline implementation work remains. Live integration, deployment, and production write safety remain external and unverified.
