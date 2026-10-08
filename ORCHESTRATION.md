# Project Orchestration — shoko-sonarr-linker

**Scope:** Harness-independent instructions for the **main project orchestrator** during a project-wide implementation run. These rules apply whether the orchestrator runs in Claude Code, another coding-agent harness, or directly. They do **not** apply to delegated implementation/review subagents.

## Document roles and access

| Document | Purpose | Main orchestrator | Delegated agents |
| --- | --- | --- | --- |
| [PROJECT.md](PROJECT.md) | Authoritative product requirements, constraints, safety invariants, and acceptance criteria | Read as needed | Read relevant sections or use excerpts in assigned brief |
| [AGENTS.md](AGENTS.md) | General implementation/review procedures and safety rules | Follow | Follow |
| [PLAN.md](PLAN.md) | **Immutable** milestone sequence, dependencies, and acceptance gates | Read; **never edit** during execution | **Do not read or edit** |
| [STATUS.md](STATUS.md) | **Mutable** milestone ledger, verification evidence, blockers, and recovery checkpoint | Read and update | **Do not read or edit** |
| **ORCHESTRATION.md** | Main-orchestrator coordination, delegation, and recovery procedures | Read and follow | Not required; do not include in delegated briefs |

Only the orchestrator reads or writes PLAN.md and STATUS.md. Subagents receive a self-contained assignment, not the project-wide execution ledger. A harness-specific bootstrap file may point to this document, but must not become a second source of orchestration policy.

**Authority:** PROJECT.md defines **what** must be built, AGENTS.md **how assigned engineering work is performed**, PLAN.md **which milestones and gates must be met**, and STATUS.md **what has actually been done**. When they conflict, identify the category and record the unresolved disagreement rather than silently weakening a safety requirement.

## Main orchestration workflow

1. **Orient.** Read ORCHESTRATION.md, AGENTS.md, PROJECT.md, immutable PLAN.md, and STATUS.md; inspect `git status`, branch/HEAD, relevant code/tests, and any uncommitted changes. Do not trust a checkpoint without comparing it to the repository.
2. **Select work.** Using STATUS.md, choose the earliest incomplete **actionable** slice and consult the matching PLAN.md gate. Keep work units bounded; continue independent safe work when external/live prerequisites are unavailable.
3. **Delegate carefully.** Provide each implementation or review subagent with a self-contained brief covering the goal, relevant PROJECT.md requirements, safety/API facts, file boundaries, expected outputs, acceptance checks, and review focus. **Never assign the subagent to inspect or update PLAN.md, STATUS.md, or ORCHESTRATION.md.** A returning subagent supplies code/results/findings; the orchestrator owns the project-wide decisions.
4. **Run sequentially.** The local inference endpoint may permit just one model request at a time. Do not run parallel subagents/reviewers unless the runtime is explicitly known to support it. If a separate reviewer is unavailable, perform an explicit, distinct read-only second-pass review.
5. **Verify and review each slice.** Inspect changed files and actual diffs; run relevant tests/linters/build checks. Conduct a separate adversarial review focused on wrong episode links, excluded roots, Shoko side effects, async/restart replay, mapping validity, and CI publishing correctness. Findings need severity, file/line, impact, and proposed fix. Fix P0/P1 and gate-blocking P2, then retest and re-review.
6. **Accept and checkpoint.** Decide a gate only with observed evidence. Update **STATUS.md**, not PLAN.md, after each accepted slice/review-fix cycle, milestone decision, and local commit. Record commands/results, open findings, blockers, current branch/commit, and **one concrete next action**. Commit locally only after the relevant gate passes; keep commits focused and, after the Docker bootstrap, buildable.
7. **Continue unattended.** Do not request routine approval between milestones. After two substantially identical tool failures, diagnose; after three unproductive fix/review attempts, record a reproducible blocker and proceed with independent safe work. Never call a mock/offline check a live verification.

## Immutability and permissions

- **PLAN.md is frozen:** do not edit it to mark completion, resolve blockers, or rewrite acceptance gates. Check its SHA-256 against the digest recorded in STATUS.md at startup and following context loss. On mismatch, stop plan-dependent work, record the discrepancy, and seek approval for a new plan version. Do not silently adopt a changed plan.
- When an actual plan change is necessary, enter a **proposed change** and impact in STATUS.md. Continue unrelated safe work while it awaits user approval. Update product requirements in PROJECT.md only when legitimately changed; keep generic engineering rules in AGENTS.md.
- Permitted during the assigned implementation run: offline code/document edits, mock tests, local builds, and local milestone commits after passing checks. **Not implicitly authorized:** pushing, publishing packages/images directly, creating/changing remote repositories or GitHub settings, deployment, real Sonarr/Shoko write calls, or approving Shoko's physical-media safety gate. CI workflow code for later authorized publishing is in scope.
- Shoko's metadata link jobs can trigger server-side deletion/renaming and external hooks. Never infer a lack of media risk from the linker's own Docker mounts. See PROJECT.md and PLAN.md for apply-time safety requirements; the orchestrator may not self-attest operator-only checks.

## Context compaction, interruption, and rehydration

**Treat compaction as context loss, not as a completed checkpoint.** A compacted session summary is only a hint; it does not replace these files or live repository evidence. Apply this protocol on automatic/manual compaction, a resumed session, agent migration to a different harness, or any comparable loss of working context.

**Before a deliberate context reset:** Update STATUS.md with the active milestone/slice, accepted local commit, branch/HEAD, working-tree changes, exact checks/results, unresolved review findings, blockers, any in-flight or uncertain write state, and **one concrete next action**. Mark incomplete work as incomplete; do not transfer large logs or subagent transcripts into the ledger. Never write execution state into PLAN.md.

**After compaction or resumption, before editing, delegating, committing, or declaring success:**

1. **Restore orchestration policy:** Re-read **ORCHESTRATION.md** and **AGENTS.md**; if the active harness has a bootstrap file, read it only for harness-specific startup rules.
2. **Restore product boundaries:** Re-read **PROJECT.md**, emphasizing prohibited media effects, scope restrictions, and requirements relevant to the active slice.
3. **Restore the frozen plan:** Read **PLAN.md** for the milestone and gate; compute SHA-256 and compare it with STATUS.md. Record and resolve an unexpected mismatch before plan-dependent work proceeds.
4. **Restore execution state:** Read **STATUS.md** for the latest verified milestones, unresolved findings, blockers, and next action.
5. **Reconcile reality:** Inspect `git status`, HEAD/branch, changed files, relevant code/tests and available actual test output. Correct STATUS.md if it disagrees with evidence. Missing evidence is **UNVERIFIED**, not PASS.
6. **Resume precisely:** Continue the earliest actionable incomplete slice, avoiding duplicate implementation or blind replay of any uncertain operation. Reissue a fresh, self-contained brief to resumed/new subagents; do not send them to PLAN.md or STATUS.md.

If a reset happens without a reliable checkpoint, reconstruct status from source, git history/diffs, and actual test outputs; document uncertainty in STATUS.md. Do not automatically replay requests that might already have reached Shoko.

## Completion and handoff

The project-wide implementation goal is complete only when PLAN.md's offline acceptance gates have evidence in STATUS.md, critical/high defects are resolved, and the final checks actually ran. Mark live integration, GHCR publishing, Unraid deployment, and production write safety as **EXTERNAL/UNVERIFIED** unless directly observed with authorization. The final handoff states accomplished milestones, checks, remaining risks/blockers, last accepted local commit, and the next operator action.
