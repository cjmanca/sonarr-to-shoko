# Execution Status — shoko-sonarr-linker

**Owner:** Main project orchestrator only (independent of coding-agent harness). Delegated implementation/review agents receive scoped task briefs and report findings to the orchestrator; they must not read or edit this file or [PLAN.md](PLAN.md).

**Frozen plan SHA-256:** `7f231f3ad6d63a61e920ddb981bd18750a613abb70db23a2744890675c0c45e5`. If the digest changes unexpectedly, stop treating the edited file as approved; record the discrepancy here and request user authorization before replacing the plan.

**Authority:** [PROJECT.md](PROJECT.md) = requirements; [AGENTS.md](AGENTS.md) = general agent procedures; [ORCHESTRATION.md](ORCHESTRATION.md) = harness-independent main-orchestrator workflow; **[PLAN.md](PLAN.md) = frozen milestone sequence and acceptance gates**; **STATUS.md = the only mutable execution/progress record**. Never modify PLAN.md to record progress. Proposed plan changes require user approval before replacing the frozen plan.

## Current checkpoint

- **Goal status:** `TODO` (valid: `TODO`, `ACTIVE`, `PASS`, `PARTIAL/BLOCKED`).
- **Active milestone / slice:** M0 — discovery / not started.
- **Next concrete action:** Inspect git status and repository contents; verify versioned Sonarr/Shoko API and Anime-Lists mapping contracts against primary sources. Then begin M1's first buildable Python/Docker commit.
- **Last accepted local commit:** None recorded.
- **Working tree / uncommitted work:** Not inspected.
- **Last observed branch / HEAD:** Not inspected.
- **Last verified checks (command → result):** None yet.
- **Open P0/P1 or gate-blocking P2 findings:** None reported yet (not a completed review).
- **Current blockers / assumptions:** Production Sonarr/Shoko versions, credentials, mounts, safety settings, mapping coverage, and GitHub package permissions have not been verified.
- **Offline vs live evidence:** No offline gates have been executed. Live Sonarr/Shoko, Plex, GHCR, and Unraid checks are `EXTERNAL/UNVERIFIED`.
- **Proposed changes to frozen plan:** None. Any proposal belongs here until explicitly approved by the user.

## Milestone ledger

*Only the orchestrator changes statuses or enters acceptance evidence. Never promote a milestone to PASS based on a subagent claim alone; inspect code/diff and actual test output. Each row refers to the acceptance gate under the same ID in the immutable PLAN.md.*

| Milestone | Status | Evidence (commands/results, review, commit) / blocker |
| --- | --- | --- |
| M0 | TODO | — |
| M1 | TODO | — |
| M2 | TODO | — |
| M3 | TODO | — |
| M4 | TODO | — |
| M5 | TODO | — |
| M6 | TODO | — |
| M7 | TODO | — |
| M8 | TODO | — |
| M9 | TODO | — |

**Milestone states:** `TODO`, `ACTIVE`, `PASS`, `BLOCKED`; a milestone can `PASS` its offline gate while explicitly labeling external/live validation `EXTERNAL/UNVERIFIED`. A blocked offline gate is never a pass.

## Latest implementation / review checkpoint

- **Slice / scoped assignment:** Not started.
- **Files changed:** —
- **Implementation summary:** —
- **Targeted checks and exact results:** —
- **Independent read-only review:** Not performed. Include severity, path/line, impact, and fix or disposition for each finding.
- **Fixes and retests:** —
- **Decision:** Not accepted / pending.
- **Commit SHA:** —
- **Next task brief (self-contained for subagents):** —

## Blockers, deferred findings, and plan change proposals

| Type / severity | Milestone | Reproducer or justification | Resolution / next action |
| --- | --- | --- | --- |
| None recorded | — | — | — |

## External validation tracker

*Unavailable live evidence is not a test pass. Do not request write credentials merely to complete offline gates.*

| External requirement | Status | Evidence / remaining action |
| --- | --- | --- |
| Deployed Sonarr/Shoko version and live API response compatibility | UNVERIFIED | Inspect in approved live read-only preflight |
| Shoko Sonarr media effectively read-only; file-quality filter disabled; plugin/renamer/MyList review | UNVERIFIED | Operator-only filesystem/settings safety inspection |
| Live apply canary, Shoko queue settling, media integrity, Plex result | UNVERIFIED | Requires explicit user authorization and backup |
| GHCR per-commit push and `latest` behavior | UNVERIFIED | Requires authorized push and observed GitHub runner |
| Unraid Docker Compose and persistent state | UNVERIFIED | Requires authorized deployment |

## Checkpoint maintenance rules (orchestrator only)

After each bounded slice and review/fix cycle, update this document **once**, replacing the current checkpoint and relevant milestone row with concise verified evidence. Keep only the most recent detailed slice; record durable milestone decisions in the ledger. Record changed paths, exact checks/statuses, P0–P3 issues and dispositions, last commit, blockers, and the next concrete action. Before intentional compaction, record the current branch/HEAD, incomplete edits, real check outcomes, outstanding review findings and **one concrete next action**. On resumption or after automatic compaction, the **main orchestrator** must follow the document rehydration order in [ORCHESTRATION.md](ORCHESTRATION.md), verify the immutable plan digest and repository state, then continue at the earliest *actionable* unpassed milestone without relying solely on a compacted summary.

Do not paste large tool logs or full subagent transcripts. Do not include secrets, API keys, personal server credentials, or unredacted sensitive Shoko settings in this file. Treat a frozen-plan change as a proposal requiring user approval—not as an edit to PLAN.md.
