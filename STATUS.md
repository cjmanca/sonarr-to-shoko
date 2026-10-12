# STATUS — shoko-sonarr-linker execution ledger

**Run:** Third independent review remediation (F15–F20) per PLAN.md.
**PLAN.md SHA-256:** `7215769B11D8D94B57CAAE048D6E43370CF1A3835D2ACD7C892919359F3EAD38` (immutable this run)
**Baseline HEAD:** `721689f22c34879302386fa0d67017aef333517f`
**Environment:** Windows / Python 3.14.0 / uv 0.9.18 / Docker 29.7.2

## Ledger

| Milestone | Status | Notes |
| --- | --- | --- |
| Phase 0 orient | complete | Baseline suite green locally (see Baseline); upstream Shoko 5.3.3 facts verified at pinned commit 892461188e5f1cc3ccf1b9b6eb1d4dfd31bea6f8 |
| F15 (P0) file-quality-filter attestation hard refusal | **complete** | Implemented + reviewed (see Phase 1). Defect was: attestation.py `build_record`/`_parse_settings_summary` validated flag presence/type only |
| F16 (P1) ED2K field format vs Shoko v3 API | **complete** | Implemented + reviewed (see Phase 1). Defect was: `_valid_ed2k` required `ed2k://` URI; fixtures built synthetic URIs |
| F17 (P1) SQLite v1→v2 migration | pending | state.py `SCHEMA_VERSION = 1`, empty migration registry; new drain columns in fresh schema |
| F18 (P1/P2) budget cooldown vs permanent pause | pending | daemon zero-progress breaker pauses on `budgets_exhausted` |
| F19 (P2) canary authorization history | pending | `_SEND_PROGRESS_STATES` excludes `waiting`; REJECTED/REFUSED map to `waiting` |
| F20 (P2) plan-drift test fixture | pending | Confirmed: `write_plan_file` writes compact JSON (`"max_submissions":5`), so the test's space-form replace is a no-op; GitHub runs 38199820749/38199820737 failed, local suite green — discrepancy to reconcile |

## Baseline evidence (2026-10-12)

- `uv run pytest -q` at HEAD 721689f: **all tests pass locally** (exit 0, ~970 tests, offline mocked).
- `tests/test_campaign.py::test_campaign_apply_refuses_authorized_plan_drift_with_zero_posts` passes locally, fails on GitHub runner (per PLAN F20). Root fixture defect confirmed (no-op text replace); why the local pass occurs is to be reconciled during F20 work.
- GitHub Actions runs 38199820749 (CI) and 38199820737 (GHCR): **failing at test phase** (per PLAN; external, unverified locally beyond reproducing the defect).

## Decisions & constraints

- No pushes/publishing/deploy/live Sonarr-Shoko writes without authorization. Local milestone commits after gates pass.
- PLAN.md immutable; STATUS.md (this file) is the mutable ledger.
- Subagents receive self-contained briefs; they do not read PLAN.md/STATUS.md/ORCHESTRATION.md.

## Next action

Phase 1: verify upstream Shoko 5.3.3 source for F15/F16 facts, then implement F15 (attestation hard refusal) per brief.
