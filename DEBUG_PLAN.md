# Iterative debug session plan

Created 2026-10-26 before bug hunting. This is a session execution plan, not new product requirements. PROJECT.md remains authoritative. DEBUG_SESSION.md holds evidence and the resumable lane state. Existing PLAN.md, STATUS.md and ORCHESTRATION.md are reserved; this delegated planning task did not read or modify them.

## Rehydration and delegation contract

The root agent only orchestrates and reads necessary documentation. Delegate all information gathering, option evaluation, implementation, diff inspection, reviews and tests/builds in small, self-contained tasks. Run exactly one sub-agent task at a time because the local inference endpoint may permit only one request. Do not start concurrent reviewers, background model tasks or parallel implementers. Await completion before starting the next task. Do not put an entire project audit or several unrelated fixes into one brief.

At startup/compaction root re-reads AGENTS.md, ORCHESTRATION.md, PROJECT.md, this plan and session memory. Delegate repository status/diff recovery rather than inspecting source personally. Each delegated agent re-reads AGENTS.md, its current brief, relevant PROJECT.md sections and scoped source/tests; check nested instructions. Supply actual known exclusions, safety constraints, acceptance checks and file boundaries. Delegates do not browse orchestration records or select project milestones. Memory updates can be explicitly delegated to a bounded bookkeeping agent. Preserve existing work.

Every loop step starts by issuing the following exact user prompt, followed by the bounded lane/task brief and the current non-actionable ignore list:

```text
Triple check for more bugs, missing implementations, and security holes. If you find a bug, you fix it, not matter whether it's in scope. If no bugs found, you can just say it looks good. Think of 3 ways to solve each of these, considering the documentation. Select the best option to implement. API breaking changes and broad refactors (even across slices and significant churn) are acceptable if they improve code quality significantly. Don't overengineer. Only propose options that are an improvement over the current code, avoid "sidegrades". After each issue is solved, keep repeating this pattern of looking for more issues and starting sub-agents to fix them until no more bugs are found.
```

The orchestrator implements this prompt through sequential bounded assignments: a hunter reports a concrete issue, an options task evaluates three improving solutions (or the hunter includes that bounded evaluation), an implementer receives the selected fix and acceptance checks, then a different read-only reviewer assesses the actual diff. Cross-lane issues are retained and assigned to their proper lane, never discarded as out of scope. Broad refactors/API changes are acceptable when materially better for current or documented future needs; hypothetical complexity and sidegrades are not. If three genuinely improving alternatives do not exist, report that limitation rather than inventing sidegrades.

No agent runs full validation during hunting/fixing/review loops. Use focused tests for the changed behavior and its failure cases, scoped lint/type checks when useful, and diff hygiene. Every brief explicitly says: targeted tests only; do not run full validation. Root delegates full-suite/build validation only at the end. A reviewer must differ from the implementer and remain read-only, returning severity, path/line, impact, reproduction and proposed fix. Fix P0/P1 and acceptance-blocking P2 findings, rerun affected checks and obtain re-review.

During validation, determine whether each fixed defect repeats elsewhere and can be prevented by a practical refactor. If so, root delegates a separate bounded refactor, targeted regression checks and independent read-only review. Do not refactor merely for theoretical reuse.

Keep future-milestone/non-actionable findings in DEBUG_SESSION.md with evidence, rationale, revisit condition and explicit ignore-list wording. Pass that list to every later hunt; it is not permission to ignore actionable security or safety regressions. Report blockers to the user and move to independent unblocked lanes. Diagnose after two similar failures; after three unproductive attempts stop that loop and report root cause/reproducer. Blocked lanes cannot be marked clean or used to claim unverified success.

No commits, pushes, remote creation, publishing, deployments, GitHub settings/secrets changes, live Sonarr/Shoko writes, production apply, credentials exposure or media modifications. Mocked write tests/local builds are permitted. No raw Shoko Settings documents in logs/reports. Never relax product safety rules or tests to obtain green checks.

## Sequential audit lanes

Paths below are ownership boundaries for the initial hunt, not a ban on reporting issues elsewhere. Consult adjacent consumer contracts read-only when necessary; delegate cross-lane edits explicitly. The project inventory is a Python service with its dashboard in the same package, not separate server/shared/client packages. Do not invent nonexistent builds.

| ID | Initial bounded component | Relevant focused checks (select cases as needed) |
| --- | --- | --- |
| L1 | `config.py`, `paths.py`, `policies.py`: configuration, root/path boundaries, mapping ambiguity, validation | `test_config.py`, `test_paths.py`, `test_policies.py` |
| L2 | `http.py`, `sonarr.py`, `shoko.py`, `errors.py`: transport, API contracts, response validation, source isolation | `test_http.py`, `test_sonarr.py`, `test_shoko.py` |
| L3 | `mappings.py`, `mapping_store.py`: translation precedence, split/special/multi-target identity, safe refresh/cache | `test_mappings.py`, `test_mapping_fixes.py`, `test_mapping_store.py` |
| L4 | `linking.py`, `audit.py`: exact identity, multi-episode completeness, protected links, dry-run/async verification | `test_linking.py`, `test_audit.py`, selected `test_e2e.py` cases |
| L5 | `state.py`: persistence/migrations, durable sends/budgets, restart idempotency and locking | selected `test_state.py`, `test_apply_lock.py`, `test_batch_progress.py` cases |
| L6 | `setup.py`, `service_setup.py`, `onboarding.py`, `safety_check.py`, `attestation.py`: discovery, consent, digest invalidation, physical safety | selected corresponding `test_setup.py`, `test_service_setup.py`, `test_onboarding.py`, `test_safety_check.py`, `test_attestation.py` cases |
| L7 | `service.py`, `rate_control.py`: normal queue, canary, adaptive hard limits, recovery and source/mapping drift | selected `test_service.py`, `test_service_runtime.py`, `test_rate_control.py`, `test_service_e2e.py` cases |
| L8 | `campaign.py`, `daemon.py`: advanced campaign authorization, durable canary, deferred retry/terminal stop behavior | selected `test_campaign.py`, `test_daemon.py`, `test_f_campaign_e2e.py` cases |
| L9 | `dashboard.py`, `service_settings.py`: auth, enrollment/status/control routes, CSRF/session risks, persisted settings, dashboard UI behavior | selected `test_dashboard.py`, `test_service_settings.py`, `test_service_e2e.py` cases |
| L10 | `cli.py`, `bootstrap.py`, `reporting.py`, `logging_setup.py`, `__init__.py`, `scripts/validate_historical.py`: entry points, modes, errors/reports, secret redaction | selected `test_cli.py`, `test_cli_m7.py`, `test_bootstrap.py`, `test_logging.py`, `test_validate_historical.py` cases |
| L11 | `container_entrypoint.py`, `Dockerfile`, Compose/Unraid examples, packaging/lock config and deployment docs: runtime identity, persistence, installation | `test_container_entrypoint.py`, `test_unraid_template.py`; focused offline packaging checks only before final validation |
| L12 | `.github/workflows/ci.yml`, `publish.yml`, `scripts/ci_checks.sh`, `enumerate_introduced.py`, `ghcr_plan.py`, `ghcr_tags.py`: per-SHA provenance, permissions, PR isolation, latest races and recovery | `test_workflows.py`, `test_publish_enumeration.py`, `test_ghcr_tags.py` |

Source paths are relative to `src/shoko_linker/` unless otherwise stated; test paths are relative to `tests/`. Subdivide a large lane into narrower sequential briefs; do not send its entire file list as an unbounded implementation task. Read README/docs relevant to each contract without rewriting product requirements. Cross-cutting test helpers are inspected only when behavior evidence requires them.

## Loop and retirement rules

1. Select the next active lane from session memory and issue the exact prompt plus its bounded brief. No bug hunting occurs before this plan exists.
2. Hunt three distinct ways: requirement-to-code completeness/safety, edge-case/failure/restart analysis, and existing-test/diff evidence. Return concrete reproducible findings and targeted evidence; absence of a test alone is not a bug.
3. Handle each actionable issue regardless of original lane scope. Evaluate three improvements against documentation and select the simplest complete solution. Delegate one coherent fix at a time, targeted tests, independent read-only review and needed repair/recheck. Record evidence and changed interfaces.
4. A lane with findings or changes remains active. Only a later hunt reporting clean AND no changes retires it. Store the pass, reviewed revision/diff context, command evidence and retirement reason. Retired lanes are skipped during ordinary loops until every lane is retired clean/no-change.
5. When every lane is retired, perform a final all-lane cross-change sweep sequentially, including retired lanes and their interfaces. Start each step with the exact prompt. If findings/changes occur, reactivate affected lanes and resume ordinary looping; unaffected clean lanes retain evidence. Once all lanes retire again, repeat the all-lane final sweep.
6. Only after a final all-lane sweep is clean/no-change, assign a dedicated final full-validation-and-fixes agent. Explicitly tell it not to check in regularly; this may run a long time. Root waits patiently and reports only meaningful state changes/blockers, without repeatedly interrupting the agent.
7. Final agent runs the complete configured quality gate (`uv run ruff check .`, `uv run ruff format --check .`, `uv run mypy`, `uv run pytest -q`, `git diff --check`), package validation and applicable Docker build/smokes from CI, verifying available tools first. Use the required Python 3.12-compatible locked environment; host default Python is not proof of target compatibility. No publishing/live access. Report exact commands/results, tool versions and unavailable checks separately. Do not invent server/shared/client builds; verify the actual Python/dashboard/container components.
8. Full-validation failures receive concrete bounded fixes and independent read-only review (the final agent may return a scoped implementation brief to root). Assess repeated patterns and delegate justified preventive refactors. Any fix reopens affected hunt lanes and requires renewed all-lane cross-change sweep plus final full validation. Finish only when no actionable findings remain in completed audits and final checks pass, or explicitly report a blocker without claiming clean completion.

## Required evidence and recovery state

Record lane/pass state, findings with severity/path/reproduction, three options and selection, implementer/reviewer identities, changed paths, targeted commands with observed results, repeated-pattern decision/refactor, retirement evidence, final-sweep state, final validation, exclusions and blockers in DEBUG_SESSION.md. Keep concise handoffs before compaction. Do not claim exhaustive proof of absence of bugs or live validation from mocked tests.

Suggested first assignment: L1 hunter, read-only, `config.py` and `paths.py` first (policies only if their consumers require it), PROJECT.md scope/path/config requirements, matching selected tests, current exclusion list. Return reproducible actionable issues and three improving options per issue, or evidence-based clean/no-change. Targeted tests only, no full validation, no implementation until root assigns a bounded fix.
