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
| F17 (P1) SQLite v1→v2 migration | **complete** | Implemented + reviewed (see Phase 2). Defect was: `SCHEMA_VERSION = 1` with drain columns only in fresh-install schema; empty registry; v1 DBs crashed on campaign queries |
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

Phase 3: F18 (recoverable-wait vs permanent pause in the daemon zero-progress breaker) + F19 (canary authorization from durable send history).

## Phase 1 — F15 + F16 (complete, adversarial review ACCEPTABLE)

- F15: `_refuse_enabled_file_quality_filter()` shared by `build_record()` and `_parse_settings_summary()`; refusal text names `FileQualityFilterEnabled=true` + ManualLinkJob delete risk. 6 attestation tests + 2 campaign lease tests (record refusal, load invalid, daemon/manual-apply/drain zero-POST scoped per lease duality).
- F16: `_valid_ed2k` = raw 32-hex (upper/lower); SHA1/MD5/CRC32 validated as 40/32/8-hex **strings**, null/absent optionals permitted; `Imported=null` non-excluding; values preserved verbatim for identity comparisons (case-sensitive `HASH_CHANGED` drift tested); fixtures rebuilt to real Shoko wire shape (no `ed2k://` anywhere in test data).
- Upstream verified at pinned commit 89246118: File.cs `HashesDict` + ctor L141; HashFileJob L327 `ToUpperInvariant`, `string.Empty` seeds; Hasher MD4-tree ED2K.
- Review: ACCEPTABLE, no P0/P1/P2. Accepted P3 tradeoffs: whitespace-**padded** valid hex passes strip-then-validate (fail-closed drift only, upstream never pads); int-typed hash values excluded as NO_HASHES (fail closed); e2e harness builds ShokoHashes directly (wire-shape covered by test_shoko/test_linking JSON fixtures).
- Checks: full suite 974 pass; ruff/format/mypy clean; reviewer independently reproduced.

## Phase 2 — F17 (complete, migration review ACCEPTABLE)

- `SCHEMA_VERSION = 2`; registry entry `{1: _migrate_v1_to_v2}`; `_init_schema` routes existing `0 < user_version < 2` through `migrate()` in place; `> 2` refused fail closed; fresh installs direct to v2.
- Migration: PRAGMA table_info-guarded `ALTER TABLE campaigns ADD COLUMN` (NULL/NULL/''), so migrated campaigns are never drain-authorized; interim v1-with-columns upgrades cleanly preserving grants; step + version bump + audit row in one `BEGIN IMMEDIATE` transaction; induced failure → full rollback, file intact at v1, recoverable `StateError`, reopen after fault removal succeeds.
- Tests: v1 DB built from the **verbatim** `_SCHEMA_SQL` of commit 6d4183b (fixture `tests/state_v1_fixture.py`); baseline/files/campaign/verified+indeterminate sends/rolling budgets/apply-pause populated; 9 required verifications pass (data byte-compared, no auto-auth, no send replay, budget intact, reopen idempotent, v3 refused, failure recoverable); guard test re-extracts the commit schema via git and asserts sha256 equality with the fixture (skips only if git unavailable); drain-columns/schema sync test.
- Review evidence: mutation harness (7 mutations → targeted failures; control green); `PRAGMA user_version` transactionality empirically confirmed; WAL + synchronous=FULL.
- **Deferred P2s (pre-existing, outside F17 upgrade gate, reviewer recommends ship):** (1) fresh-init `executescript` not atomic (mid-script failure can leave half-created v0 store); (2) concurrent-open migration lock race escapes raw `sqlite3.OperationalError: database is locked` → traceback/exit 1 instead of clean `StateError`/exit 5 (migration data safety itself unaffected); (3) cryptic fail-closed tracebacks for v0-with-tables / v1-missing-schema_version-table cases (data safe, operator UX only); (4) connection FD leak on failed open.
- Checks: full suite 985 pass; ruff/format/mypy clean (orchestrator re-ran independently).

## Commits

- `7a2e2bf` fix: remediation F15+F16 (incl. third-review PLAN.md baseline + STATUS.md)
- (next) fix: remediation F17 — v1→v2 forward-only migration
