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
| F8 — daemon/campaign attestation scope mutual exclusion | P1 | TODO |
| F9 — campaign submission budgets never reset between batches | P1 | TODO |
| F10 — canary authorization inferred from attempted submissions | P1 | TODO |
| F11 — campaign batches reuse stale safety/timing decisions | P1 | TODO |
| F12 — GHCR provenance fetches manifests as blobs | P2 | TODO |
| F13 — empty hashes pass file-readiness check | P2 | TODO |
| F14 — deployment docs inconsistent campaign instructions | P2 | TODO |
| Final validation (full checks + simulated campaign + adversarial review) | Gate | TODO |

## Constraints

- Sequential model agents only (single-slot local inference endpoint).
- No push, publish, deploy, or live Shoko/Sonarr writes.
- Subagents never read/modify PLAN.md, STATUS.md, ORCHESTRATION.md.
- All prior file-protection, scope-isolation, and no-overwrite safeguards must be preserved.

## Next action

Implement F8 (independent daemon/campaign safety attestations) via a self-contained delegated brief.
