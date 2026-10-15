# Sonarr-to-Shoko — Seventh Independent Review Handoff

Repository: https://github.com/cjmanca/sonarr-to-shoko

Reviewed commit: `7d848a30edf4518fe8af5d9528c5eefb86034399`

## Review outcome

The F31–F33 implementation passed independent source review.

No outstanding P0/P1 or acceptance-blocking P2 findings were identified.

One non-blocking P3 issue remains.

## F34 — Campaign deadline feasibility warning ignores disabled submission capacity

### Affected files

* `src/shoko_linker/campaign.py`
* `src/shoko_linker/cli.py`
* Relevant campaign preparation tests

### Issue

`campaign_deadline_warning()` estimates the optimistic completion horizon using the rolling submission limit.

It explicitly handles `rolling_new_limit=0`.

However, the configured limits also permit:

* `max_new_per_cycle=0`
* `max_pending_episode_jobs=0`

Either setting prevents all link submissions.

The preparation command can therefore omit its feasibility warning even when the campaign cannot make progress.

### Required correction

Extend the existing warning mechanism to recognize disabled submission capacity.

At minimum:

1. Warn when `max_new_per_cycle=0`.
2. Warn when `max_pending_episode_jobs=0`.
3. Preserve the existing rolling-window deadline calculation.
4. Keep the warning informational.
5. Do not modify the approved campaign deadline.
6. Do not increase or bypass submission limits.
7. Preserve both text and JSON warning output.

Avoid implementing a sophisticated scheduling simulator. A simple check for impossible configurations is sufficient.

### Regression tests

Verify campaign preparation with:

* All submission limits positive.
* Rolling limit zero.
* Per-cycle limit zero.
* Pending episode-job limit zero.
* Multiple limits simultaneously zero.
* A large campaign with an insufficient deadline.
* A sufficiently long deadline with all limits positive.

Assert that warnings are accurate and that the generated campaign plan and approval digest remain unaffected.

## Execution

Follow `ORCHESTRATION.md` and `AGENTS.md`.

Keep `PLAN.md` immutable.

Record implementation and verification results in `STATUS.md`.

Implement the smallest complete fix, add focused regression tests, run the relevant existing checks, and perform an independent read-only review.

Do not introduce additional infrastructure or expand the test framework.

Commit validated changes locally.

Do not push, deploy, publish, or perform live Sonarr/Shoko writes without authorization.

## Completion

F34 is complete when disabled submission capacity is consistently reported during campaign preparation and all relevant tests pass.

This issue is P3 and does not need to block read-only production integration testing.
