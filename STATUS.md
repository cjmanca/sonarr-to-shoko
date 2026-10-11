# Status Ledger — shoko-sonarr-linker (Second Review Remediation F8–F14)

Mutable ledger maintained by the main orchestrator. PLAN.md is immutable.

## Baseline

- Branch: `main`
- Baseline HEAD at round start: `6d4183b408ee9835c65d035b0e9fb7ec6df9d962` (matches PLAN.md reviewed commit)
- PLAN.md SHA-256: `b427ae7e909442d0638bb79ac37c0e47b0ed7456fba112c7c47046fc0529fab6` (frozen v2 — second independent review handoff, 2026-10-10)
- Prior round (F1–F7 remediation) evidence is preserved in git history (commits `db2f852..6d4183b` and the previous STATUS.md content at `HEAD~`).
- Working tree at startup contained only the new PLAN.md (replacing frozen v9 plan) and this emptied STATUS.md.

## Findings status

| Finding | Severity | Status |
| --- | --- | --- |
| F8 — daemon/campaign attestation scope mutual exclusion | P1 | PASS (pending final gate) |
| F9 — campaign submission budgets never reset between batches | P1 | PASS (pending final gate) |
| F10 — canary authorization inferred from attempted submissions | P1 | TODO |
| F11 — campaign batches reuse stale safety/timing decisions | P1 | TODO |
| F12 — GHCR provenance fetches manifests as blobs | P2 | TODO |
| F13 — empty hashes pass file-readiness check | P2 | TODO |
| F14 — deployment docs inconsistent campaign instructions | P2 | TODO |
| Final validation (full checks + simulated campaign + adversarial review) | Gate | TODO |

## F8 — PASS (local commit this round)

**Design:** Daemon lease stays at `safety.attestation_file`; each campaign lease is a deterministic sibling `<stem>.campaign-<id>.yaml` in the same directory (`attestation.campaign_lease_path`). CLI `attestation record/status/invalidate --scope daemon|campaign:<id>` targets the scope's own file (writes never clobber the other scope; destructive invalidate refuses when the target is absent, honoring a legacy campaign record at the base path only on exact scope match). Pause bookkeeping is scoped: daemon lease transition → global write pause `attestation_expired` + meta `attestation_gate`; campaign lease transition → campaign status PAUSED + meta `campaign_attestation_gate:<id>` (never global). Campaign sends bypass the global pause ONLY when origin-gated (`reason == attestation_expired` AND `attestation_gate == "paused"`); every other pause reason (churn, source break, queue anomaly, operator pause, writes_disabled) gates all scopes. CLI refuses the reserved reason string for operator `pause-writes`. Per-submission lease freshness: campaign `_send_one` and daemon `_process_candidate` re-evaluate the applicable lease before each POST; transient blocked-safety preflight defers non-destructively (never quarantines). `validate_scope` rejects hostile/over-long campaign ids.

**Review history (sequential read-only reviewers):** round 1 BLOCKING (2 P1: invalidate scope clobber via read fallback; reserved-reason bypass of operator pause; 2 P2: stale campaign gate meta leaving campaign approved/head-of-line; hardcoded `attestation_valid=True` per-file; 3 P3) → all fixed. Round 2 BLOCKING (P2: mid-batch transient BLOCKED_SAFETY quarantined files destructively; P2: daemon cycle-start lease snapshot rode mid-cycle expiry; P3 charset; P3 cosmetic reason) → all fixed. Final round: **ACCEPTABLE**, zero blocking findings.

**Deferred P3s:** test seams in e2e_harness/test_attestation patch the path resolver to one file (real resolver covered by DualEnv real-file tests + reviewer CLI probes); campaign in-batch `apply_gate` reads in-memory settings (config-file gate flip mid-batch not observed in flight — pre-existing property, F11 will extend); daemon gate transition overwrites a preexisting operator pause reason (cosmetic, pre-existing).

**Evidence:** `uv run pytest -q` 829 passed (exit 0); `uv run ruff check .` pass; `uv run ruff format --check .` 47 files clean; `uv run mypy` clean (44 files); `git diff --check` clean. Real-file scenario tests cover: daemon-only, campaign-only, both valid, each expired independently, both expired, endpoint mismatch, stale version, scope mismatch fail-closed, scoped invalidate, backward-compat legacy records, restart-equivalence (fresh loader read), mid-batch/mid-cycle lease expiry (zero POSTs after expiry, non-destructive deferral), resume→expiry→re-pause. Offline only; no live Shoko/Sonarr writes.

## F9 — PASS (local commit this round)

**Design:** `StateStore.begin_cycle(prefix, iso)` allocates durable window keys `<label>-<run_id>-<seq>` from persisted meta `budget_run_id` (per-install uuid nonce) + `budget_cycle_seq` (monotonic, advanced before submissions; corruption-safe seed via floor scan of existing window rows). Daemon cycles use `cycle-<run>-<seq>`; every campaign batch (daemon drain + manual CLI apply) allocates one fresh `campaign-<short>-<run>-<seq>` key per batch, threaded through prepare_send/verify/mark_completed. Completed per-cycle budget rows pruned to newest 64 at cycle start (global `_global` rolling row + current window never touched; pending accounting lives only on `_global` via send-row release funnels; `sends`-row request-fingerprint dedup untouched → no replay, no lost intent). Campaign batch loop bounded by an attempts counter (incl. resolve-chain scans) and a monotonic wall-time cap (`max_cycle_seconds`, clock starts before the sweep). Campaign-wide budget refusals (paused / pending_files_exhausted / cycle_new_exhausted / rolling_new_exhausted) break the batch and defer the remainder non-destructively (queueable, never quarantined); per-file `pending_episode_jobs_exhausted` is a per-file deferral that never starves healthy files. Zero-progress breaker keyed on REAL progress (`report.progress = submitted+verified+quarantined`), not scan counts: per-file job refusals and non-materializing pending targets trip campaign auto-pause (`no_budget_progress` / `no_queueable_candidates`) in 3 cycles with resume working. `plan_complete` reports derive only from persisted `campaign_send_progress >= len(plan.files)` — an all-refused batch is never reported complete.

**Review history:** round 1 BLOCKING (P2 preflight GET amplification via processed-keyed guard + no wall-time cap; P3 corrupt seq) → fixed (attempts bound, time cap, corruption floor). Round 2 BLOCKING (P2 resolver-walk amplification relocated to `_resolve_and_rebuild`; P2 per-file job-ceiling refusal broke whole batch → head-of-line starvation) → fixed (scan-attempt counting, `_CAMPAIGN_WIDE_BUDGET_REFUSALS` classification). Round 3 BLOCKING (P1 false `plan_complete` from scan counter; P2 breaker bypassed by deferrals) → fixed (persisted-progress completion, `report.progress`). Round 4: **ACCEPTABLE**, remaining findings P3 only.

**Deferred P3s:** `report.progress` sums submitted+verified+quarantined so verified sends double-count the operator-facing metric (sole functional use is the `==0` breaker check); mass-drift quarantine resolves are cap-neutral (amortized, wall-time bounded); zero-progress reason label imprecise for mixed budget+pending deferrals.

**Evidence:** `uv run pytest -q` 847 passed; `uv run ruff check .` pass; `uv run ruff format --check .` clean; `uv run mypy` clean (44 files); `git diff --check` clean. 17+ `test_f9_*` regression tests incl: 100-file campaign / per-cycle cap 5 progressing past 5/10/20/50 across daemon batches; restart key-collision freedom; repeated manual applies fresh keys; rolling exhaustion stop/resume; outstanding-job cap across batches; budget-blocked zero-progress auto-pause; crash-mid-batch single-completion (no replay); bounded preflight/resolve calls (spy counts ≤ batch size); wall-time bound; all-refused ≠ plan_complete. Offline only; no live writes.

## Constraints

- Sequential model agents only (single-slot local inference endpoint).
- No push, publish, deploy, or live Shoko/Sonarr writes.
- Subagents never read/modify PLAN.md, STATUS.md, ORCHESTRATION.md.
- All prior file-protection, scope-isolation, and no-overwrite safeguards must be preserved.

## Next action

Implement F10 (explicit operator canary confirmation for automatic drain authorization) via a self-contained delegated brief.
