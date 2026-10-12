# Sonarr-to-Shoko — Third Independent Review Handoff

**Repository:** https://github.com/cjmanca/sonarr-to-shoko

**Reviewed HEAD:** `721689f22c34879302386fa0d67017aef333517f`

**Review date:** 2026-10-11

## Objective

Correct the remaining defects discovered during an independent review of the F8–F14 remediation.

The previous fixes are largely implemented and should be preserved. Focus on the confirmed issues below rather than redesigning working components.

The application must remain default-deny, preserve existing Shoko associations, avoid media modifications, and never submit a potentially duplicated link request.

## Operating instructions

Follow the repository's existing orchestration framework.

* `PROJECT.md` defines product requirements.
* `AGENTS.md` defines engineering procedures.
* `ORCHESTRATION.md` defines main-orchestrator responsibilities.
* `PLAN.md` is immutable during execution.
* `STATUS.md` is the mutable execution ledger.

Only the main orchestrator reads or updates `PLAN.md` and `STATUS.md`. Subagents receive self-contained task briefs and must not inspect those orchestration files.

For every issue:

1. Verify the finding against current HEAD.
2. Identify affected implementation and test paths.
3. Implement the smallest complete correction.
4. Add focused regression tests.
5. Run relevant checks.
6. Conduct a separate read-only review.
7. Fix blocking findings and retest.
8. Update `STATUS.md`.
9. Commit locally after validation.
10. Continue without requesting routine approval.

Do not push, publish packages, deploy, modify production configuration, or perform live Sonarr/Shoko writes without authorization.

---

# F15 — P0: Unsafe Shoko file-quality-filter settings can pass attestation

## Affected files

* `src/shoko_linker/attestation.py`
* `src/shoko_linker/cli.py`
* `tests/test_attestation.py`
* Related apply-safety tests

## Evidence

In `attestation.py`, `build_record()` validates that all four settings flags are present and Boolean.

It does not enforce that `FileQualityFilterEnabled` is false.

The loader also accepts a correctly structured record containing:

```yaml
settings_summary:
  FileQualityFilterEnabled: true
  RenameOnImport: false
  MoveOnImport: false
  MyListAddFiles: false
```

`load_attestation()` checks the endpoint, scope, version, and expiration, but does not reject this unsafe settings value.

`AttestationVerdict.valid_for_apply` returns true whenever the status is `valid`.

Consequently, the application can accept an operator attestation that explicitly states the file-quality filter is enabled.

This contradicts the production safety requirements.

### Why this matters

In Shoko Server 5.3.3:

`Shoko.Server/Scheduling/Jobs/Shoko/ManualLinkJob.cs`

`ManualLinkJob.Process()` calls `ProcessFileQualityFilter()`.

That function checks:

```csharp
if (!_settings.FileQualityFilterEnabled) return;
```

When the filter is enabled, it can ultimately call:

```csharp
RemoveRecordAndDeletePhysicalFile(...)
```

The affected files can include media in Shoko's independently writable manual collection.

Making Sonarr's mounts read-only does not necessarily protect those files.

## Required correction

Treat `FileQualityFilterEnabled=true` as a hard apply-safety failure.

Enforce this at both boundaries:

1. When creating an attestation.
2. When loading an existing attestation.

A manually edited or previously generated unsafe attestation must not become valid merely because it parses successfully.

Return a clear refusal explaining which setting is unsafe.

Do not require all renamer/MyList flags to be false without examining their separate risk and deployment requirements. The file-quality filter is the independently established hard prohibition.

Preserve the operator-controlled mount/plugin checklist.

## Required tests

Verify:

* `FileQualityFilterEnabled=false` permits an otherwise valid attestation.
* `FileQualityFilterEnabled=true` is rejected when recording.
* An existing YAML lease containing `true` is rejected when loaded.
* Daemon apply performs zero POSTs with an unsafe lease.
* Manual campaign apply performs zero POSTs.
* Automatic campaign draining performs zero POSTs.
* Other unrelated safety flags retain their documented behavior.
* Missing or malformed flags remain invalid.

**Acceptance:** No Shoko link request is authorized by an attestation explicitly reporting an enabled file-quality filter.

---

# F16 — P1: ED2K validation rejects Shoko's actual hash format

## Affected files

* `src/shoko_linker/shoko.py`
* `src/shoko_linker/linking.py`
* `tests/hash_fixtures.py`
* `tests/test_shoko.py`
* `tests/test_linking.py`
* `tests/e2e_harness.py`
* Other fixtures using ED2K hashes

## Evidence

The F13 implementation introduced `_valid_ed2k()`.

It requires the ED2K value to begin with:

```text
ed2k://
```

It then validates a URI containing a filename, file size, and hash segments.

However, **Shoko's v3 File API does not return ED2K as a URI**.

In Shoko Server 5.3.3:

`Shoko.Server/API/v3/Models/Shoko/File.cs`

The response is populated with:

```csharp
Hashes = new()
{
    ED2K = file.Hash,
    MD5 = file.MD5,
    CRC32 = file.CRC32,
    SHA1 = file.SHA1
};
```

And in:

`Shoko.Server/Scheduling/Jobs/Shoko/HashFileJob.cs`

Shoko stores:

```csharp
vlocal.Hash = hashes.ED2K.ToUpperInvariant();
```

The ED2K value is the raw hexadecimal hash, normally 32 hexadecimal characters.

An actual response resembles:

```json
{
  "Hashes": {
    "ED2K": "0123456789ABCDEF0123456789ABCDEF",
    "SHA1": null,
    "MD5": null,
    "CRC32": null
  }
}
```

It does not contain a filename or URI prefix.

The current `_valid_ed2k()` rejects this value.

Because discovery and fresh preflight both use `has_available_hashes()`, valid files can be rejected as `NO_HASHES`.

This potentially blocks the entire real backlog.

### Why the tests did not detect it

`tests/hash_fixtures.py` constructs synthetic `ed2k://` URIs.

Those fixtures are then reused across discovery, linking, and end-to-end tests.

The tests consistently exercise an incorrect API representation.

## Required correction

Replace URI validation with validation of Shoko's actual ED2K field.

For Shoko 5.3.3:

* Require a string.
* Require exactly 32 hexadecimal characters.
* Accept uppercase and lowercase.
* Reject empty, whitespace-only, and malformed values.
* Preserve the original value for identity comparisons.
* Validate other populated hash fields according to their actual wire formats.
* Permit absent optional hashes.
* Do not use `Imported=null` as an exclusion.

Verify the field contract against upstream Shoko source before implementation.

Correct the shared test fixtures so they model actual Shoko API responses.

Do not keep URI-based synthetic fixtures simply to preserve passing tests.

### Required tests

Test:

1. Valid uppercase 32-character ED2K.
2. Valid lowercase 32-character ED2K.
3. `Imported=null` with valid ED2K.
4. Empty ED2K.
5. Whitespace-only ED2K.
6. Truncated ED2K.
7. Nonhexadecimal ED2K.
8. Missing ED2K.
9. Malformed optional hashes.
10. Existing XRefs.
11. Correct file identity across discovery and fresh preflight.

Add an end-to-end backlog fixture containing actual Shoko-style hash values.

**Acceptance:** A fully hashed, unrecognized Shoko file with a normal hexadecimal ED2K value can progress through discovery, mapping, planning, and protected preflight.

---

# F17 — P1: Existing SQLite v1 databases cannot be upgraded safely

## Affected files

* `src/shoko_linker/state.py`
* `tests/test_state.py`
* Relevant campaign migration tests

## Evidence

The current state module still declares:

```python
SCHEMA_VERSION = 1
```

The initial SQL schema has been modified to add campaign fields:

```sql
drain_authorized_iso TEXT,
drain_authorized_digest TEXT,
drain_invalidated_reason TEXT NOT NULL DEFAULT ''
```

However, the previous implementation already created schema-v1 databases without these columns.

The migration registry is still empty.

In `_init_schema()`, an existing database with `user_version=1` is treated as current, and initialization returns without applying any changes.

Meanwhile, `get_campaign()` and `list_campaigns()` now select the new columns.

Opening an existing v1 database can therefore produce:

```text
sqlite3.OperationalError:
no such column: drain_authorized_iso
```

This is an upgrade compatibility defect.

The affected state may contain important historical submissions, approvals, budgets, and indeterminate write outcomes.

It must not be discarded or silently recreated.

## Required correction

Implement a genuine forward-only schema migration.

Suggested approach:

* Bump `SCHEMA_VERSION` to 2.
* Keep the new fields in the fresh-install schema.
* Add a migration from v1 to v2.
* Add missing campaign columns with appropriate defaults.
* Preserve all existing data.
* Treat existing campaigns as **not drain-authorized** after migration.
* Ensure migration failure leaves a recoverable database state.
* Refuse newer unsupported schema versions.

Do not solve this by deleting the state database.

Do not rewrite existing submission histories.

Avoid migrations that automatically authorize old campaigns.

## Required tests

Create a database using the actual schema from commit:

`6d4183b408ee9835c65d035b0e9fb7ec6df9d962`

Populate it with:

* A baseline.
* File records.
* An existing campaign.
* A verified send.
* An indeterminate send.
* Rolling-budget state.
* A stored apply-pause reason.

Then open it with the corrected implementation.

Verify:

1. Migration succeeds.
2. Schema version advances.
3. All existing records survive.
4. Campaign queries work.
5. Old campaigns have no automatic-drain authorization.
6. Pending sends are not replayed.
7. Budget accounting remains correct.
8. Reopening the database does not repeat the migration.
9. A future unsupported schema version is refused.
10. Migration failure does not destroy the database.

**Acceptance:** An existing installation can upgrade without losing state or requiring manual database reconstruction.

---

# F18 — P1/P2: Normal budget cooldown permanently pauses unattended campaigns

## Affected files

* `src/shoko_linker/daemon.py`
* `src/shoko_linker/campaign.py`
* `src/shoko_linker/state.py`
* `tests/test_daemon.py`
* `tests/test_campaign.py`
* End-to-end campaign tests

## Evidence

The daemon implements a three-cycle zero-progress breaker.

When campaign apply is refused because of:

```text
budgets_exhausted
```

the daemon increments its zero-progress counter.

After three such cycles, it pauses the campaign with:

```text
no_budget_progress
```

Paused campaigns are excluded from automatic-drain selection.

This creates a problem with normal rolling budgets.

For example, the existing default limits include:

```text
rolling_new_limit = 200
rolling_window_hours = 168
```

After 200 submissions, the weekly rolling budget is exhausted.

The application should stop submitting until the rolling window permits more work.

Instead, subsequent daemon cycles can permanently pause the campaign.

When the rolling budget eventually resets, the campaign remains paused and requires operator intervention.

For a 7,000-file backlog, this defeats unattended processing across multiple budget windows.

### Related issue

The same zero-progress logic can pause campaigns waiting for:

* Shoko metadata.
* Pending asynchronous jobs.
* Temporary queue congestion.
* Other expected, recoverable conditions.

A few consecutive zero-progress cycles are not proof of permanent failure.

## Required correction

Distinguish **recoverable waiting** from **permanent failure**.

Normal rolling-budget exhaustion should produce a deferred state with an expected retry time.

When the rolling window permits more submissions, processing should resume automatically.

The application should similarly wait through legitimate metadata and async-job delays without immediately requiring operator intervention.

Preserve hard stops for:

* Invalid safety authorization.
* Revoked approval.
* Mapping conflicts requiring review.
* Source identity discontinuity.
* Unsafe Shoko settings.
* Unexpected existing links.
* Other genuinely unsafe states.

Do not remove rate limits or submission ceilings to make the campaign progress.

The goal is correct scheduling, not faster unsafe writes.

### Required tests

Use a fake clock and a campaign larger than the rolling limit.

Example:

* 100 approved files.
* Per-cycle cap: 5.
* Rolling cap: 10.
* Rolling window: 1 hour.

Verify:

1. The campaign submits at most 5 per cycle.
2. It stops after 10 during the rolling window.
3. It remains approved while waiting.
4. No submissions occur before the window permits them.
5. Processing resumes automatically after the window.
6. The campaign completes across multiple windows.
7. Restart during cooldown preserves the schedule.
8. Pending Shoko jobs do not cause premature permanent pauses.
9. Genuine safety failures still stop writes.

**Acceptance:** An approved campaign can progress across multiple rolling-budget windows without repeated manual intervention.

---

# F19 — P2: Canary verification ignores attempted sends that return to WAITING

## Affected files

* `src/shoko_linker/campaign.py`
* `src/shoko_linker/state.py`
* `tests/test_campaign.py`

## Evidence

`authorize_campaign_drain()` checks submitted campaign files by filtering their current states against `_SEND_PROGRESS_STATES`.

That set includes:

* `request_may_have_been_sent`
* `verifying`
* `verified`
* `partial`
* `indeterminate_write`
* `conflict`

It excludes `waiting`.

However, `StateStore.record_send_outcome()` maps `REJECTED` and `REFUSED` outcomes to `FileState.WAITING`.

The send is still recorded in the submission ledger.

Consider a manually executed canary batch:

1. File A is submitted and verified.
2. File B is submitted but Shoko returns a rejection.
3. File B transitions to `waiting`.
4. The operator invokes `campaign authorize-drain`.

The authorization check can see only File A among its filtered submitted files.

It may conclude that every submitted canary file was verified, even though File B was not.

## Required correction

Base canary completion checks on the durable send history, not solely the current file state.

The operator authorization should account for every send attempted as part of the canary.

Do not confuse a request rejected with documented zero-write semantics with a verified episode association.

If rejected canary requests are permitted as an exceptional outcome, require explicit, visible handling rather than silently excluding them from the completeness check.

Preserve the requirement for operator confirmation of physical-media safety.

## Required tests

* One verified canary: eligible for explicit operator confirmation.
* One verifying canary: refused.
* One indeterminate canary: refused.
* One rejected canary: refused or explicitly handled according to the documented policy.
* One verified plus one rejected: cannot silently pass.
* Previously attempted file now in `waiting`: remains visible in the canary audit.
* Successful explicit confirmation after safe resolution persists across restart.

**Acceptance:** Drain authorization cannot silently omit attempted canary sends from its verification decision.

---

# F20 — P2: Current GitHub CI and publishing are failing

## Affected files

* `tests/test_campaign.py`
* Related campaign-plan tests
* `STATUS.md` (orchestrator only)

## Evidence

The latest GitHub Actions runs failed:

CI:
https://github.com/cjmanca/sonarr-to-shoko/actions/runs/38199820749

GHCR:
https://github.com/cjmanca/sonarr-to-shoko/actions/runs/38199820737

Both workflows fail during the test phase.

The failing test is:

```text
test_campaign_apply_refuses_authorized_plan_drift_with_zero_posts
```

The test attempts to modify an approved plan using:

```python
plan_text.replace(
    '"max_submissions": 5',
    '"max_submissions": 4',
)
```

However, campaign plan files are written using compact canonical JSON:

```python
json.dumps(
    document,
    sort_keys=True,
    separators=(",", ":"),
)
```

The actual JSON therefore contains:

```json
"max_submissions":5
```

rather than a space after the colon.

The replacement does nothing.

The plan file remains unchanged, so the application's digest validation correctly permits the next batch.

The test then incorrectly expects plan drift and fails.

GitHub's runner confirmed that additional mocked submissions occurred before the assertion failed.

This is primarily a **test-fixture defect**, not evidence that the production digest guard can be bypassed.

Nevertheless, the intended plan-drift regression is currently not being tested correctly.

## Required correction

Modify the plan structurally rather than using whitespace-sensitive text replacement.

For example:

```python
document = json.loads(plan_path.read_text())
document["totals"]["max_submissions"] = 4

plan_path.write_text(
    json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
    ) + "\n"
)
```

Verify that the chosen field exists and that its original value differs.

Before running the daemon, explicitly assert:

* The file contents changed.
* The canonical digest changed.
* The new digest differs from the stored approval digest.

Then verify:

* The campaign pauses with `plan_drift`.
* No additional POST is issued.
* Existing verified canary associations remain untouched.
* Automatic drain authorization does not bypass the changed plan.

Do not remove the test or weaken its assertions merely to restore CI.

## Required validation

Run the focused test first, then the complete test suite.

Check that the locally passing results in `STATUS.md` reconcile with the actual GitHub runner failure.

Actual publishing success for the latest commit remains unverified until GitHub Actions completes successfully on a corrected commit.

**Acceptance:** The plan-drift test genuinely modifies the approved plan and proves that the application refuses further submissions.

---

# Implementation sequence

## Phase 1 — Critical safety and API compatibility

Fix F15 and F16 first.

Verify the relevant Shoko Server 5.3.3 source directly.

The work is incomplete if the tests pass only because they use synthetic API formats that Shoko does not return.

Conduct a separate adversarial review of the safety-attestation and Shoko file-readiness paths.

## Phase 2 — Database upgrade safety

Implement F17.

Test against a database created by the previous implementation, not merely against a schema reconstructed from the latest source.

Preserve durable submission history.

## Phase 3 — Campaign correctness

Fix F18 and F19.

Use the existing campaign, daemon, and SQLite mechanisms.

Do not introduce a separate scheduler, task queue, or database.

Verify unattended progress across multiple real budget windows and comprehensive canary authorization.

## Phase 4 — Repair CI validation

Fix F20.

Make the failing test actually exercise plan drift.

Run the relevant checks and distinguish local evidence from GitHub runner results.

Do not push unless authorized.

## Phase 5 — Final integration and adversarial review

Run:

```bash
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
uv run mypy
git diff --check
docker build -t shoko-sonarr-linker:review .
```

Also run targeted integration tests for:

* Actual Shoko-style ED2K payloads.
* `Imported=null` with completed hashing.
* Explicitly unsafe file-quality-filter settings.
* Existing SQLite schema migration.
* Campaign progression across multiple rolling-budget windows.
* Rejected and indeterminate canary submissions.
* Campaign approval digest changes.
* Container restart and send recovery.
* Existing-link protection.
* Multi-episode file verification.
* Normal daemon shutdown.

Perform a separate read-only review of every mutation path.

The final review must establish that:

1. Every real Shoko POST is protected by fresh safety checks.
2. The state machine persists intent before transmitting.
3. An uncertain request is never blindly replayed.
4. A previously linked Shoko file cannot be intentionally overwritten.
5. No out-of-scope Sonarr file is linked.
6. Attestations explicitly reporting unsafe settings are rejected.
7. Existing installations can upgrade without losing SQLite state.
8. An approved backlog can progress unattended across normal budget resets.
9. The plan-drift regression exercises a genuine change.
10. Tests use realistic, upstream-verified API payloads.

Record actual evidence in `STATUS.md`.

Keep `PLAN.md` immutable.

Commit the corrections locally after successful validation.

Do not perform production writes, deployments, or GitHub publishing without authorization.

# Completion criteria

This remediation is complete when:

* F15–F20 are corrected or conclusively disproved with source/test evidence.
* All required targeted regression tests pass.
* The full configured test suite passes.
* Docker builds and the default-deny CLI smoke tests pass.
* Independent review finds no unresolved P0/P1 or acceptance-blocking P2 defects.
* Existing SQLite data survives upgrades.
* Campaigns can complete across budget windows without repeated operator intervention.
* `STATUS.md` contains accurate results and commits.
* `PLAN.md` remains unchanged.
* External validations are not represented as completed until actually performed.

Continue through the phases without routine approval requests. Preserve the existing architecture and all previously implemented safety controls.
