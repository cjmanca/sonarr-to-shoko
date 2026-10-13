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
| F18 (P1/P2) budget cooldown vs permanent pause | **complete** | Implemented + reviewed + fix round (see Phase 3) |
| F19 (P2) canary authorization history | **complete** | Implemented + reviewed (see Phase 3) |
| F20 (P2) plan-drift test fixture | **complete** | Fixed + root-caused (see Phase 4) |

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
- `2374b8b` fix: remediation F17 — v1→v2 forward-only migration
- `45cfebd` fix: remediation F18+F19 — recoverable-wait deferral + send-ledger canary authorization
- `35b9583` test: remediation F20 — structural plan-drift tamper
- (next) chore: remediation F15–F20 final review polish (P3 nits)

## Phase 4 — F20 (complete)

- Root cause PROVEN: space-form replace is a no-op against compact canonical JSON; Windows `Path.write_text` translates trailing `\n`→`\r\n`, changing bytes ⇒ digest mismatch ⇒ drift fires locally. Under simulated Linux semantics (`newline=""`) the ORIGINAL test fails exactly like CI (extra POSTs, no drift); the repaired test passes under the same simulation.
- Repair: `_tamper_plan_file_canonical()` structurally lowers `totals.max_submissions` (5→4) WITH the bound file set (schema binds files==max_submissions), writes byte-exact canonical bytes via `write_bytes`; pre-cycle asserts: bytes changed, canonical file digest changed, original digest == stored approval digest, tampered digest != approval digest, canonical re-serialization stability guard (catches future newline regressions). Post-cycle: paused `plan_drift`, zero additional POSTs, canary file VERIFIED untouched, send history length unchanged, `total_sent` unchanged, drain selection `status_paused`, `select_next_daemon_campaign` None, second cycle zero POSTs. Reviewer mutation: neutered tamper IS caught by the guard.
- CI reconciliation: local-pass artifact explained; GitHub green remains EXTERNAL/UNVERIFIED until Actions run on the corrected commit (no push authorized).

## Phase 5 — Final integration (complete)

- `uv run pytest -q` → 998 passed (orchestrator + final reviewer, independently). `ruff check`/`format --check`/`mypy`/`git diff --check` clean (reviewer re-ran all).
- `docker build -t shoko-sonarr-linker:review .` → success (image sha256:940e8aa5…).
- Default-deny CLI smoke (CI pattern, built image): `--help` rc=0; `validate --offline config.example.yaml` rc=0; `once --apply config.example.yaml` rc=3 with REFUSED message, zero writes.
- Targeted integration mapping: every PLAN Phase-5 item maps to named passing tests (Shoko-style ED2K payloads, Imported=null pipeline, unsafe filter leases ×3 apply boundaries, F17 migration suite incl. verbatim-commit guard, multi-window F18 progression + restart, rejected/indeterminate canaries, digest-change/drain invalidation, restart/no-replay trio, existing-link protection, multi-episode verification, shutdown flow) — final reviewer verified no item lacks coverage.
- Final adversarial review (every mutation path, 10-point checklist): **all 10 PASS**; verdict **ACCEPTABLE**; single Shoko POST site (`linking.py:1160`) gated by apply gate + live lease + digest + status/auth + fresh preflight + budget reservation + apply lock; intent committed before transmit; no blind replay (recovery → HOLD); XRef protection intact; no out-of-scope linking.
- P3 polish committed with this entry: dead `allow_int` branch removed from `_settled_field` (non-string now fail-closed as not-settled), `state.py` F7 docstring folded into the F17 v2 framing, stale test-name citation in `test_campaign.py:2530` corrected.

## Open items / external state

- GitHub Actions (CI + GHCR) green status: **EXTERNAL/UNVERIFIED** — expected to pass on pushed commits (no push performed; publishing not authorized this run).
- Deferred P2s from F17 review (pre-existing, out of upgrade-gate): fresh-init `executescript` non-atomicity; concurrent-open migration lock traceback vs clean StateError; cryptic fail-closed messages (v0-with-tables, v1-missing-schema_version); FD leak on failed open. Recommend as a future bounded task.
- Deferred P3s: campaign metas retained through auto-pause (bounded, cleared on terminal transitions); F18 stall-cooldown span fixed at 5 intervals (no new config key by design).
- Live Sonarr/Shoko validation: not performed (no credentials/access; all evidence is mocked/offline).

## Completion audit

F15–F20: all six corrected with source/test evidence (none disproved). Required targeted regression tests: all present and passing (mapping table in Phase 5). Full suite: 998 green. Docker + default-deny smoke: green. Independent reviews (Phase 1 adversarial, Phase 2 migration, Phase 3 campaign, final 10-point): all ACCEPTABLE, no unresolved P0/P1, no acceptance-blocking P2 (F18 head-of-line P2 fixed within phase). SQLite upgrade safety: proven against verbatim 6d4183b schema. Multi-window unattended progression: proven. PLAN.md digest unchanged (`7215769B…` — verified at this audit: `git diff PLAN.md` empty since baseline commit 7a2e2bf). STATUS.md: this ledger. External validations marked UNVERIFIED, not claimed.

## Phase 3 — F18 + F19 (complete, review ACCEPTABLE after fix round)

- F18: rolling-budget exhaustion and recoverable stalls (pending Shoko metadata, verifying async jobs, queue congestion, batch capacity) now DEFER via durable meta `campaign_deferred_until:<id>` (schema v2 unchanged); campaign stays APPROVED with drain authorization intact; auto-resumes (rolling-slot expiry when budget-caused; otherwise a 5-cycle-interval yield span that re-arms each stall — chosen over 1 interval after review showed 1 interval is inert under production sleep-after-cycle cadence); head-of-line stalled campaigns yield the queue to later authorized campaigns (two-campaign tests, restart tests); zero-progress breaker reduced to genuinely-unexpected stalls only (`no_queueable_candidates` for blocked_safety/conflict/unqueueable rows, pauses at 3, proven); all hard stops intact (lease, plan_drift, revoked, mapping drift, source discontinuity, writes_disabled, deadline, max_submissions_reached, indeterminate no-replay); no ceilings/rate-limits changed; latent `_budget_headroom` false-exhausted wedge fixed (peek now mirrors `_reserve_tx` rolling reset; reporting rollup shared).
- F19: `authorize_campaign_drain` gates on the durable sends ledger — every attempted (non-voided) campaign send's latest row must be outcome `verified` AND file VERIFIED; refusals list outstanding sends visibly (file id + outcome + state); rejected→waiting canary sends cannot silently pass; resubmit+verified clears with earlier rejected rows visible in `attempted_sends` audit; confirm+note gate unchanged; authorization persists across restart. Documented in `DRAIN_AUTHORIZATION_RULE` + PROJECT.md §3.
- Review (adversarial, 12 probes): ACCEPTABLE; P2 head-of-line starvation finding remediated (5-cycle cooldown + cadence-accurate tests); 4 P3s fixed (family labels, reporting peek, structured budget-refusal matching, `+N more` display); 1 P3 deferred (cosmetic docstring name in test_campaign.py:2529).
- Checks: full suite 998 pass; ruff/format/mypy clean (orchestrator re-ran independently). Tests adapted to 5-interval semantics with assertions strengthened (zero-POST per cycle, no-replay ledgers, no-pause every cycle, breaker-not-burned preserved verbatim).
