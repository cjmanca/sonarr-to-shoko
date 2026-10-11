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
| F9 — campaign submission budgets never reset between batches | P1 | TODO |
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

## Constraints

- Sequential model agents only (single-slot local inference endpoint).
- No push, publish, deploy, or live Shoko/Sonarr writes.
- Subagents never read/modify PLAN.md, STATUS.md, ORCHESTRATION.md.
- All prior file-protection, scope-isolation, and no-overwrite safeguards must be preserved.

## Next action

Implement F9 (unique durable budget-cycle identifiers) via a self-contained delegated brief.
