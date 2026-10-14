# Sonarr-to-Shoko — Sixth Independent Review Handoff

**Repository:** https://github.com/cjmanca/sonarr-to-shoko

**Reviewed commit:** `d5124c74b94a3c2b6cb3aecade3915d15b05d80e`

**Review date:** 2026-10-14

## Objective

Investigate and correct the remaining issues identified after implementation of F26–F30.

The previous remediation is substantially successful.

Preserve the current architecture and avoid unnecessary new infrastructure, broad refactoring, or disproportionate test-framework expansion.

## Operating instructions

Follow the existing project orchestration framework.

* `PROJECT.md`: requirements.
* `AGENTS.md`: engineering instructions.
* `ORCHESTRATION.md`: main orchestrator workflow.
* `PLAN.md`: immutable.
* `STATUS.md`: mutable execution ledger.

Only the main orchestrator interacts with PLAN.md and STATUS.md.

Subagents receive self-contained task briefs.

For each finding:

1. Verify the issue against current HEAD.
2. Reproduce the problematic behavior with a focused regression test.
3. Implement the smallest complete correction.
4. Run relevant existing tests.
5. Conduct independent read-only review.
6. Correct any blocking review findings.
7. Update STATUS.md.
8. Commit validated changes locally.
9. Continue to the next finding.

Do not push, publish, deploy, or perform production Sonarr/Shoko writes without authorization.

---

# F31 — P2: Mapping generation activation is not crash-atomic

## Affected files

* `src/shoko_linker/mapping_store.py`
* `tests/test_mapping_store.py`
* Related mapping recovery tests

## Evidence

The existing mapping cache uses three related files:

* `active.xml`
* `active.meta.json`
* `state.json`

Each file is written atomically, but the complete mapping generation is not committed atomically as one unit.

In `mapping_store.py`, `_activate()` currently performs:

```python
_atomic_write(store.active_path, body)
_atomic_write(store.meta_path, _json_bytes(asdict(meta)))
```

The state is then updated separately by `_record_success()`.

This creates an interruption window.

### Reproduction scenario

1. Source A has successfully activated mapping generation A.
2. `active.xml`, `active.meta.json`, and `state.json` describe A.
3. A scheduled refresh downloads valid generation B.
4. The downloader validates B successfully.
5. `_activate()` replaces `active.xml` with B.
6. The process crashes before replacing `active.meta.json`.

On restart:

* `active.xml` contains generation B.
* `active.meta.json` describes generation A.
* `state.json` still describes the previous successful refresh.
* The checksum comparison fails.
* `load_active()` returns `None`.

The application correctly refuses to use inconsistent data.

However, **generation A's previously valid XML has already been overwritten**.

The last-known-good mapping is no longer available.

The F27 recovery mechanism can download a fresh copy, but that recovery depends on the external source remaining available.

This violates the intended last-known-good and atomic-activation guarantees.

## Required correction

Make generation activation recoverable across interruption at every filesystem write boundary.

Preserve the current single logical mapping cache.

A reasonable implementation could retain a previous verified generation until the replacement generation is committed.

Alternatively, use immutable generation files and an atomic active-generation pointer.

Choose the simplest approach compatible with the existing storage model.

Requirements:

* Fully validate a new generation before committing it.
* Never destroy the last-known-good generation before the replacement is safely committed.
* Ensure the active XML and metadata always identify the same generation.
* Detect interrupted activation on restart.
* Recover the most recent complete verified generation.
* Preserve existing source-URL provenance checks.
* Do not authorize cached data from a different configured source.
* Preserve refresh timing and bounded failure backoff.
* Avoid unnecessary duplication of large mapping data beyond what crash safety requires.

Do not merely suppress checksum failures or trust mismatched metadata.

### Required regression tests

Create an initial verified generation A.

Attempt activation of generation B.

Inject failures at these points:

1. Before writing the new XML.
2. During staging of new XML.
3. After the new XML is staged.
4. Before publishing new metadata.
5. During metadata publication.
6. Before final activation bookkeeping completes.
7. After successful activation.

Reopen the store after each simulated interruption.

Verify:

* No partially written generation becomes active.
* The previous complete generation remains recoverable.
* A fully committed B becomes the active generation.
* Metadata and content digests agree.
* Source identity remains enforced.
* Restart does not create a refresh retry loop.
* An interrupted source change never authorizes another source's XML.

Also verify that normal formatting-only mapping updates remain efficient.

**Acceptance:** An interrupted activation cannot leave the application without its previously verified last-known-good mapping solely because the replacement was partially committed.

---

# F32 — P2: Unclaimed campaign quarantine markers can bypass batch accounting

## Affected files

* `src/shoko_linker/campaign.py`
* `src/shoko_linker/state.py`
* `src/shoko_linker/daemon.py`
* `tests/test_campaign.py`

## Evidence

The F29 remediation correctly replaced:

```python
counters.attempts - counters.quarantined
```

with direct work-attempt accounting.

New quarantine decisions now increment `counters.attempts`.

However, a separate path remains in `_entry_gate()`.

For previously quarantined campaign records, it invokes:

```python
newly_reported = store.claim_campaign_quarantine_progress(
    file_id,
    campaign.campaign_id,
)
```

When a record still carries the pending-progress marker, this method updates its persisted state detail.

The branch then increments:

```python
counters.quarantined += 1
```

It does **not** increment:

```python
counters.attempts
```

Consequently, the batch-size guard:

```python
if counters.attempts >= size:
```

does not limit how many previously created but unclaimed quarantine records are updated and credited during that invocation.

### Reproduction scenario

The daemon's ordinary reconciliation can run without an active campaign drain.

A DRY_RUN reconciliation may therefore discover and persist campaign identity-drift quarantines before a subsequent `campaign apply` invocation.

Example:

1. Prepare and approve a campaign containing 100 files.
2. Change the observed ED2K identity for all 100 files.
3. Run an ordinary DRY_RUN reconciliation with no campaign quarantine limit.
4. The reconciliation records 100 `campaign_quarantine` states containing pending-progress markers.
5. Run `campaign apply --batch-size 1`.

The candidate loop encounters the existing quarantined records.

Each pending marker can be claimed and updated without consuming `counters.attempts`.

The invocation can therefore credit many quarantines and perform many SQLite updates despite the requested one-file batch size.

This is different from the F29 test where files become quarantined inside the bounded candidate loop.

### Required correction

Distinguish three cases:

**A. New quarantine decision**

A newly detected quarantine requiring a state transition must consume the batch's work allowance.

**B. Existing quarantine already accounted**

An already processed quarantine should not consume the allowance again.

It should not generate new progress.

**C. Existing quarantine awaiting first progress attribution**

Its first persistent progress claim must be handled in a bounded manner.

Either:

* Charge the first claim against the batch's meaningful-work allowance, or
* Process pending claims through a separate explicitly bounded accounting path.

Do not allow unlimited state-detail updates simply because the original quarantine decision happened in an earlier reconciliation.

Preserve the existing single-counting guarantee.

### Required tests

Use 100 campaign files and `--batch-size 1`.

Test:

1. All 100 files are newly drifted inside the campaign apply loop.
2. All 100 files were quarantined by an earlier DRY_RUN reconciliation.
3. All 100 files carry unclaimed quarantine-progress markers.
4. All 100 files have already had their quarantine progress recorded.
5. A mixture of pending markers, previously counted quarantines, and valid candidates.
6. Restart between quarantine creation and progress attribution.

Verify:

* No more than one unit of new meaningful work is accounted in a one-file batch.
* No unlimited marker mutations occur.
* The same quarantine is never counted twice.
* Previously accounted quarantines do not starve later valid candidates.
* Multiple bounded invocations eventually process outstanding accounting work.
* Submission and rolling-budget counters remain unchanged by quarantine bookkeeping.
* No extra Shoko POSTs occur.

**Acceptance:** The batch-size limit applies consistently to first-time progress attribution as well as new quarantine decisions, without repeatedly charging already processed records.

---

# F33 — P3: Large campaign deadline is incompatible with default submission limits

## Affected files

* `src/shoko_linker/cli.py`
* `src/shoko_linker/campaign.py`
* `README.md`
* `docs/deployment.md`
* Related campaign-preparation tests

## Evidence

The default configuration provides:

```yaml
sync:
  rolling_new_limit: 200
  rolling_window_hours: 168
```

The `campaign prepare` CLI defaults to:

```text
--deadline-hours 168
```

The application supports campaigns containing up to 7,000 files.

However, processing 7,000 files at a maximum of 200 new submissions per seven-day rolling window requires at least 35 submission windows.

Even under ideal conditions, the final window cannot begin until roughly 238 days after the first one.

The default campaign deadline is only seven days.

Therefore, a large campaign prepared with default arguments will expire well before its approved backlog is completed.

The F18 implementation correctly supports automatic processing across multiple rolling-budget windows, but the default campaign deadline prevents such processing unless the operator explicitly selects a longer deadline.

Additionally, the safety attestation lease has its own expiration policy. A long campaign may require periodic operator reinspection and renewal even when its campaign deadline is sufficiently long.

That safety restriction should be preserved.

## Required correction

Do not silently extend campaign approvals or safety leases.

Instead, improve campaign preparation and documentation so the operator can make an informed decision.

Recommended behavior:

1. Calculate an optimistic minimum completion horizon from the approved file count and configured rolling limit.
2. Compare it with the requested campaign deadline.
3. Show a clear warning when the campaign cannot plausibly finish before its deadline under the configured limits.
4. Explain that transient failures, pending Shoko jobs, and verification delays can increase completion time.
5. Explain that safety-attestation renewal remains separately required.
6. Document how to set an appropriate `--deadline-hours` for a large campaign.

Do not automatically increase rate limits to meet a campaign deadline.

A deliberately short deadline may be appropriate for a canary or limited campaign, so avoid rejecting it unless explicitly required by existing policy.

### Required tests

Test campaign preparation with:

* 10 files, 200/week.
* 200 files, 200/week.
* 201 files, 200/week.
* 7,000 files, 200/week.
* A deliberately short deadline.
* A sufficient long deadline.
* A zero submission limit.

Verify the warning uses the configured limits and does not accidentally authorize additional submissions.

**Acceptance:** The operator is informed when the approved campaign cannot finish within its deadline under the configured rate limits.

---

# Implementation sequence

## Phase 1 — Mapping crash consistency

Investigate and fix F31.

This is the highest-priority reliability issue.

Use fault injection against the actual activation write sequence.

Avoid replacing the existing mapping architecture unless the current one cannot provide crash consistency.

## Phase 2 — Campaign accounting

Investigate and fix F32.

Ensure the initial creation and later attribution of quarantines remain independently safe and bounded.

Preserve the F29 protections for genuinely new quarantine decisions.

## Phase 3 — Operational deadline guidance

Implement F33.

Prefer documentation and a targeted preparation-time warning over additional scheduling infrastructure.

## Phase 4 — Validation

Run:

```bash
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
uv run mypy
git diff --check
docker build -t shoko-sonarr-linker:review .
```

Run the relevant focused regression tests before the full suite.

Perform independent read-only review covering:

* Crash-consistent mapping activation.
* Source provenance.
* Last-known-good recovery.
* Campaign quarantine attribution.
* Batch-size enforcement.
* Submission budget integrity.
* No-replay guarantees.
* Existing-link protection.

Update `STATUS.md` with verified findings, actual test results, review results, and local commit SHAs.

Keep `PLAN.md` unchanged.

## Completion criteria

Work is complete when:

1. Mapping activation remains recoverable after interruption at every commit boundary.
2. No partial mapping generation becomes active.
3. First-time quarantine attribution obeys the batch-work limit.
4. Already-accounted quarantines remain free to skip.
5. Large-campaign deadline limitations are clearly communicated.
6. The previous F26–F30 protections remain intact.
7. Targeted and complete tests pass.
8. Docker build and smoke tests pass.
9. Independent review finds no unresolved P0/P1 or acceptance-blocking P2 issues.
10. STATUS.md records actual verification evidence.
11. PLAN.md remains immutable.

Do not perform production Shoko writes, deploy, or push without authorization.

Continue through the phases without requesting routine approval.
