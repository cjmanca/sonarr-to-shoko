# Agent Instructions — shoko-sonarr-linker

**Scope:** Entire repository, including delegated implementers and reviewers. This file defines **how to perform assigned work**, not the project plan, task scheduler, or progress ledger.

## Requirements and task boundaries

- **[PROJECT.md](PROJECT.md)** is authoritative for product behavior, architecture, safety constraints, integrations, and acceptance criteria. Read the relevant sections when needed to understand a substantive change. The delegating orchestrator should supply a self-contained task brief with scope and acceptance checks.
- **AGENTS.md** defines engineering procedure, testing, review, and reporting. Source code, tests, and upstream documentation describe current behavior, not permission to weaken PROJECT.md.
- For delegated tasks, work from the **assigned brief** and the relevant source files. Do not independently select additional milestones or browse orchestration/progress artifacts. Return findings and evidence to the delegating agent, which owns project-wide decisions and bookkeeping.
- For a **top-level project orchestration** session (including `/goal`), read and follow [ORCHESTRATION.md](ORCHESTRATION.md) at startup and after context compaction. That document, not this file, contains the portable orchestration policy. Delegated agents work from their scoped brief and `AGENTS.md`.
- When requirements conflict or evidence is missing, report the discrepancy; do not silently override product safety rules. Keep new product requirements in PROJECT.md and reusable agent procedures here. Avoid duplicating specifications between documents.

## Working contract

- Complete assigned, bounded work without routine approval requests. Make conservative, reversible decisions within the brief and report assumptions that matter.
- **No fabricated success:** State only checks actually run, concrete findings, and observed results. Distinguish mocked/offline tests from live validation. Never invent credentials, upstream API replies, CI status, or deployment evidence.
- If Sonarr/Shoko, GitHub publishing permissions, or optional tooling are unavailable, finish independent offline work and report precise blockers. Diagnose after two similar failures; after three unproductive fix/review attempts, stop looping and report the root cause or reproducer.
- The local inference endpoint may support only **one request at a time**. Do not start parallel subagents, concurrent reviews, or background model tasks. When independent review is available, run it sequentially and read-only; otherwise perform a distinct second-pass review.
- Keep changes small and focused. Preserve user work; never force-reset/clean or delete unrelated files. Do not modify documentation or tracked files outside the assigned scope merely to make checks pass.

## Context compaction and delegated-task recovery

- After compaction or a resumed assignment, **re-read `AGENTS.md`**, the current **self-contained task brief**, **`PROJECT.md`**, and the source/tests needed for that task. A compaction summary is a hint, not an authoritative replacement for instructions or code evidence.
- Re-establish the assigned file boundaries, safety constraints, outstanding defects, and acceptance checks before continuing. Inspect affected files and `git diff` so interrupted edits are not repeated or assumed complete; rerun only checks whose results cannot be verified.
- Before an intentional context reset, preserve the concrete changes, checks/results, remaining review findings, and next step in a concise handoff **to the orchestrator**. If the brief was lost or is incomplete, request a refreshed scoped brief rather than inferring project-wide next steps.
- Do not perform project-wide progress bookkeeping or redefine the assigned milestone during recovery. The main orchestrator owns that coordination.

## Permissions and safety boundaries

- Unless the task brief authorizes otherwise, edit only assigned project files and run local tests/builds. The main orchestrator normally handles progress records, acceptance decisions, and local commits. Do not create commits or edit orchestration tracking unless explicitly delegated.
- **Not implicitly authorized:** Pushes, remote repository creation, force-pushes, publishing, GitHub settings/secrets changes, production deployments, live Sonarr/Shoko writes, or enabling production apply mode. Future CI workflow code and mocked write tests are allowed within assigned scope.
- Never rename, move, delete, or rewrite media files. Shoko API metadata operations may indirectly trigger server-side file deletion/relocation; no live writes without the explicitly approved Shoko safety preflight described by the product requirements.
- Never expose API keys or credentials in files, fixtures, logs, reports, or commits. Reject missing/ambiguous root allowlists, path translations, episode mappings, or preexisting Shoko associations rather than guessing.

## Bounded implementation and review loop

1. **Inspect:** Understand the assigned requirement, relevant existing files and behavior, version-specific API assumptions, edge cases, and failure paths. Check for more specific instructions in directories being edited.
2. **Micro-plan:** Identify the smallest complete change and its acceptance checks. Don't over-plan trivial local changes.
3. **Implement:** Change only necessary code/docs. Separate external I/O, mapping decisions, and persistence when useful; avoid speculative abstractions and infrastructure.
4. **Verify:** Run focused tests and applicable static checks; inspect actual output. Never weaken assertions or disable safety controls to turn failures green.
5. **Review:** Inspect the actual diff against the assigned requirements and PROJECT.md. A read-only reviewer should report concrete **severity, path/line, impact, reproduction, and proposed fix**, not edit the code. The reviewer MUST be a different sub-agent from implementation.
6. **Fix and recheck:** Correct P0/P1 and acceptance-blocking P2 findings, then retest and re-review affected behavior. State any deferred P3 issue and justification.
7. **Return evidence:** Report changed paths, exact commands/outcomes, open findings, limitations, and next recommendation to the orchestrator. Do not claim milestone completion or update central execution records yourself.

Severity: **P0** data loss/security/scope escape; **P1** wrong episode linking, destructive writes, broken core behavior; **P2** meaningful reliability/edge-case defect; **P3** cosmetic or optional improvement. Critical/high issues block acceptance. A clean review requires evidence, not a declaration.

## Engineering and testing discipline

- Favor readable, typed Python, small dependencies, and existing project tooling. No Redis/Celery, messaging system, web UI, speculative compatibility framework, or tests for static tables without a demonstrated need.
- Prefer verified identifiers and explicit AniDB↔TVDB mapping precedence to fuzzy names. Never invent API endpoint fields, mapping semantics, or guarantees about Shoko's asynchronous jobs.
- Test changed **behavior** and meaningful failures: root isolation, Docker path boundaries, sizes, mapping precedence and split seasons, multi-episode files, existing-link protection, dry-run zero remote writes, async verification, and restart idempotency. Use focused fixtures and mocked HTTP; CI must not need production access.
- Avoid tests for trivial data holders, static fixtures, or low-value implementation details covered by consumer tests. Do not create a custom testing framework without justification.
- Run targeted checks while editing, then relevant configured checks. Examples once available: `python -m pytest -q`, `ruff check .`, `ruff format --check .`, configured type checker, `git diff --check`, Docker build/smoke tests. Report exact results and explicitly mark unavailable checks unverified.
- For GHCR workflow work, validate multi-commit push coverage, least-privilege permissions, no publication from untrusted PRs, SHA provenance, `latest` race protection, and recovery of missed builds. See [PROJECT.md §6](PROJECT.md#6-delivery-and-deployment).

## Handoff

A delegated task ends with a concise evidence-based handoff. The orchestrator decides whether its acceptance gate passes and which bounded task follows. An unresolved high-severity defect must be reported, not quietly deferred. Avoid long repeated summaries or speculative next steps.
