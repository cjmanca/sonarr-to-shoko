# Sonarr-to-Shoko — Second Independent Review Handoff

Repository: https://github.com/cjmanca/sonarr-to-shoko

Reviewed commit: `6d4183b408ee9835c65d035b0e9fb7ec6df9d962`

Review date: 2026-10-10

## Objective

Address the remaining defects discovered after implementation of remediation F1–F7.

The previous remediation is substantially successful. Do not rewrite working subsystems or repeat completed implementation unnecessarily.

Focus on the findings below, particularly automatic backlog campaigns and production write safety.

## Operating instructions

The main orchestrator must follow `ORCHESTRATION.md`.

Document responsibilities remain unchanged:

* `PROJECT.md`: authoritative product requirements.
* `PLAN.md`: immutable implementation plan. Never modify.
* `STATUS.md`: mutable progress and verification evidence.
* `AGENTS.md`: general engineering procedures.
* `ORCHESTRATION.md`: main orchestrator workflow.

Subagents receive self-contained assignments. They must not read or modify `PLAN.md`, `STATUS.md`, or `ORCHESTRATION.md`.

For each finding:

1. Verify the root cause against current HEAD.
2. Implement the smallest complete correction.
3. Add focused regression tests.
4. Run relevant tests.
5. Perform an independent read-only review.
6. Fix blocking findings and retest.
7. Update `STATUS.md`.
8. Commit locally.
9. Continue to the next finding.

Run model agents sequentially.

Do not push, publish, deploy, or perform live Shoko/Sonarr writes without explicit authorization.

---

# F8 — P1: Daemon and campaign safety attestations are mutually exclusive

## Affected files

* `src/shoko_linker/attestation.py`
* `src/shoko_linker/cli.py`
* `src/shoko_linker/daemon.py`
* `src/shoko_linker/campaign.py`
* Related configuration and tests

## Root cause

The application currently stores one safety attestation in the path configured by:

`safety.attestation_file`

That record contains a scope:

* `daemon`
* `campaign:<campaign-id>`

The attestation loader requires an exact scope match.

However, the automatic campaign daemon uses two independent authorization paths.

First, the daemon computes its ordinary apply eligibility:

`daemon.py`, approximately lines 1683–1719.

This requires a valid daemon-scoped attestation before `apply_running` becomes true.

Second, automatic campaign processing uses:

`_campaign_scoped_deps()`

This requires a campaign-scoped attestation.

Both lookups use the same configured attestation file.

Consequently:

* A daemon-scoped attestation cannot authorize the campaign.
* A campaign-scoped attestation cannot authorize the daemon's outer apply cycle.
* Recording a new attestation in the same file does not allow both scopes to remain valid simultaneously.

The resulting configuration can prevent automatic campaign processing entirely.

The problem is compounded by the global attestation pause mechanism: an invalid daemon attestation can pause writes before the valid campaign attestation is considered.

## Required correction

Support independent, simultaneously valid safety authorizations for daemon and campaign operations.

Choose a simple design that preserves the existing operator-controlled safety model.

For example, use separately persisted, scope-specific attestation records under the configured safety directory.

Alternatively, separate deployment-wide media-safety certification from campaign-specific authorization.

Do not weaken the requirement for a valid operator-recorded safety lease.

The daemon must distinguish:

1. Permission to submit newly arriving episodes.
2. Permission to process an explicitly approved backlog campaign.

A missing daemon-scope lease must not invalidate an otherwise authorized campaign solely because the service runs inside the daemon process.

Similarly, an invalid campaign lease must not disable unrelated, independently authorized new-arrival processing.

Avoid using one global pause marker for unrelated scope failures unless it represents a genuinely deployment-wide safety issue.

Maintain backward compatibility with existing attestation files where practical.

## Regression tests

Test with real temporary attestation files, not only injected Boolean callbacks.

Required scenarios:

* Valid daemon lease only.
* Valid campaign lease only.
* Both leases valid simultaneously.
* Daemon lease expired, campaign lease valid.
* Campaign lease expired, daemon lease valid.
* Both leases expired.
* Different Shoko endpoint.
* Different Shoko version.
* Explicit operator invalidation.
* Container restart with persisted attestations.

**Acceptance:** An approved, canary-authorized campaign can drain automatically under its own valid safety authorization without disabling unrelated daemon processing.

---

# F9 — P1: Campaign submission budgets never reset between batches

## Affected files

* `src/shoko_linker/campaign.py`
* `src/shoko_linker/state.py`
* `src/shoko_linker/daemon.py`
* `src/shoko_linker/cli.py`

## Root cause

`campaign.py`, approximately line 1383, submits requests using:

```python
cycle_key=f"campaign-{_short(campaign_id)}"
```

This key is identical across every batch of the same campaign.

However, `StateStore._reserve_tx()` retrieves the existing `cycle_new_submissions` count using that key.

It increments the stored count after every submission.

The per-cycle limit is checked against the accumulated count.

The default configuration permits:

`sync.max_new_per_cycle = 20`

Consequently, once a campaign has submitted 20 files, every subsequent batch can encounter:

`cycle_new_exhausted`

Because the cycle key never changes, waiting for another daemon cycle does not reset the count.

This can permanently stall a campaign intended to process thousands of files.

A related issue exists in the daemon's cycle identifiers.

The daemon uses identifiers such as:

`cycle-1`, `cycle-2`, etc.

These identifiers restart at `cycle-1` whenever the process restarts.

Old persisted budget rows can therefore be reused accidentally by a new process.

## Required correction

Give every actual reconciliation cycle and campaign batch a distinct durable budget-cycle identity.

Requirements:

* Unique across process restarts.
* Stable for one executing cycle/batch.
* Consistent for every submission belonging to that cycle.
* No collision with previous campaign batches.
* Does not reset global rolling budgets.
* Does not permit crash recovery to submit the same request twice.

A persisted run identifier combined with a sequence number, or a UUID-based cycle identifier, would be suitable.

Keep the existing durable send fingerprints and idempotence protections.

Consider bounded cleanup of old completed cycle-budget rows so thousands of campaigns do not cause unlimited SQLite metadata growth.

Also examine zero-progress handling.

Currently, an attempted candidate can count as `processed` even when its submission was refused by the budget.

This can prevent the zero-progress circuit breaker from detecting a campaign that is permanently stuck.

## Regression tests

Simulate:

1. A campaign with 100 approved files.
2. A per-cycle submission cap of 5.
3. Several automatic daemon batches.
4. Successful progression beyond 5, 10, 20, and 50 total submissions.
5. Daemon restart halfway through.
6. Repeated one-shot invocations.
7. Rolling-budget exhaustion and later recovery.
8. Pending asynchronous jobs.
9. Zero successful submissions across several cycles.

Verify that the per-cycle budget resets appropriately while rolling and outstanding-job limits remain enforced.

**Acceptance:** A 7,000-file campaign can progress across arbitrarily many bounded cycles without becoming permanently blocked by an old cycle identifier.

---

# F10 — P1: Canary authorization is inferred from attempted submissions

## Affected files

* `src/shoko_linker/campaign.py`
* `src/shoko_linker/state.py`
* `src/shoko_linker/cli.py`
* `src/shoko_linker/daemon.py`

## Root cause

`campaign.py`, approximately lines 1540–1562, selects campaigns for automatic processing when:

```python
campaign_send_progress(store, campaign.campaign_id) >= 1
```

However, `campaign_send_progress()` counts several states, including:

* `request_may_have_been_sent`
* `verifying`
* `verified`
* `partial`
* `indeterminate_write`
* `conflict`

Therefore, the daemon can consider the canary complete after a single attempted submission.

The canary might still be:

* Unverified.
* Partially linked.
* Indeterminate.
* Incorrectly linked.
* Awaiting asynchronous Shoko jobs.

More importantly, the software has no evidence that the operator inspected the resulting media files for unexpected renames, deletions, or other Shoko-side effects.

The original safety requirement is that the operator reviews a completed canary before approving broader production writes.

Counting an attempted request does not establish that approval.

## Required correction

Introduce a durable, explicit operator confirmation that the canary has passed.

Suggested workflow:

1. Approve the finite campaign.
2. Manually execute a small canary batch.
3. Wait for Shoko's asynchronous jobs to settle.
4. Verify the exact resulting episode links.
5. Confirm the physical-media safety checks.
6. Explicitly authorize automatic draining.

The final step should be represented by an operator-only CLI command or equivalent confirmed action.

For example:

`campaign authorize-drain --campaign-id <id> --confirm --note "canary inspected"`

The command name is illustrative.

Bind the authorization to the approved campaign identity/digest.

Persist the authorization across restarts.

Do not permit automatic draining when the canary is pending, partial, indeterminate, or conflicting.

A verified Shoko association alone is insufficient to establish that the operator inspected physical-media side effects.

Preserve the current manual batch commands.

## Regression tests

Test that:

* A newly approved campaign cannot auto-drain.
* One attempted but unverified send does not authorize draining.
* An indeterminate canary does not authorize draining.
* A verified canary without operator confirmation does not authorize draining.
* Explicit confirmation after verification enables bounded automatic processing.
* Revocation prevents further submissions.
* A changed plan digest invalidates automatic-drain authorization.
* Restart preserves valid confirmation.

**Acceptance:** The daemon cannot begin mass backlog processing before the operator has explicitly confirmed that the canary passed.

---

# F11 — P1: Campaign batches reuse stale safety and timing decisions

## Affected files

* `src/shoko_linker/campaign.py`
* `src/shoko_linker/daemon.py`
* `src/shoko_linker/linking.py`

## Root cause

`apply_campaign()` checks the write gate and safety attestation once before entering its batch loop.

It then processes multiple files.

The initial timestamp is reused throughout the batch.

More importantly, `_send_one()` calls `preflight()` with:

```python
attestation_valid=True
apply_gate_open=True
```

These values are hardcoded because the batch previously passed its outer safety checks.

However, each file may require substantial time for Shoko verification.

A campaign processing several files can continue well beyond its original attestation inspection time.

Consequently, an attestation or campaign deadline may expire while the batch is still active.

The next file can be submitted using the earlier authorization.

A related shutdown concern exists: a SIGTERM received during a long batch is not checked before every subsequent submission.

The daemon checks shutdown after the entire reconciliation cycle, which now includes automatic campaign processing.

This can result in additional POST requests after shutdown has been requested.

## Required correction

Immediately before each new Shoko POST, re-evaluate the safety inputs that can become stale.

At minimum:

* Current attestation validity.
* Applicable safety scope.
* Campaign deadline.
* Campaign status.
* Operator write gate.
* Shutdown request.
* Mapping generation consistency.
* Existing-link protection.
* Shoko file identity.

Keep the existing fresh file/XRef and target-identity preflight.

Use the actual current time for submission timestamps and rolling-budget calculations instead of reusing an old batch-start timestamp.

Ensure the process stops starting new submissions when shutdown has been requested.

An already submitted request must remain durably tracked and recoverable.

Keep verification bounded and avoid busy polling.

Do not bypass the original safety gate merely because the candidate belongs to an approved campaign.

## Regression tests

Using a fake clock:

1. Begin a batch with a valid attestation.
2. Submit and verify the first file.
3. Advance beyond the attestation expiration.
4. Confirm the next file is not submitted.

Repeat with campaign deadline expiration.

Also test:

* Operator pause or revoke between files.
* SIGTERM after the first request.
* Mapping changes between candidates.
* Long verification delays.
* Correct pending-send recovery after interruption.

**Acceptance:** No new Shoko POST begins after its applicable authorization expires or after shutdown has been requested.

---

# F12 — P2: GHCR provenance verification fetches manifests as blobs

## Affected files

* `scripts/ghcr_tags.py`
* `.github/workflows/publish.yml`
* `tests/test_ghcr_tags.py`

## Root cause

The new registry discovery implementation correctly queries GHCR instead of Git tags.

However, the OCI index provenance code contains an endpoint error.

In `scripts/ghcr_tags.py`, approximately lines 410–440, `_index_revision()` extracts the digest of a child manifest.

It then fetches that manifest using:

```python
f"{REGISTRY}/v2/{owner}/{package}/blobs/{digest}"
```

The OCI Distribution API distinguishes manifests from blobs.

A referenced image manifest must be fetched through:

`GET /v2/<name>/manifests/<digest>`

The `/blobs/<digest>` endpoint is for blobs such as image configurations and layers.

This can cause provenance verification to fail for OCI image indexes produced by Docker Buildx.

Because the workflow enables provenance attestations, OCI indexes are particularly relevant.

The verifier may classify an already-published image as `unknown_provenance`.

The backfill workflow can then repeatedly schedule that SHA for publication.

## Required correction

Fetch child manifest descriptors through the manifests endpoint.

Fetch the referenced image configuration through the blobs endpoint.

Handle OCI indexes containing multiple descriptors.

Select an actual runnable image manifest rather than assuming the first descriptor is always the image.

For example, an index may contain an image manifest and a separate attestation manifest.

Preserve digest verification.

Also correct the publication policy for existing tags with unknown provenance.

An existing but unverifiable SHA tag should not automatically be overwritten merely because its name contains a commit SHA.

A tag name derived from a commit is not automatically an immutable image reference.

If an existing tag cannot be verified, report the problem and require safe reconciliation instead of silently overwriting an unknown image.

## Regression tests

Use a representative OCI index containing:

* A runnable image manifest.
* An attestation manifest.
* Image configuration labels.
* Valid SHA-256 digests.

Assert the exact registry request paths.

Also test:

* Docker manifest schema 2.
* OCI single-image manifests.
* OCI indexes.
* Manifest ordering variations.
* Missing revision labels.
* Mismatched revision labels.
* Missing child manifests.
* Registry authentication failure.
* Repeated backfill after successful publication.

**Acceptance:** Successfully published images are recognized reliably, and backfill does not overwrite an existing unverified SHA tag.

OCI reference: https://github.com/opencontainers/distribution-spec/blob/main/spec.md

---

# F13 — P2: Empty hashes can pass the file-readiness check

## Affected files

* `src/shoko_linker/shoko.py`
* `src/shoko_linker/linking.py`
* Related tests

## Root cause

The new `has_available_hashes()` helper uses:

```python
return any(value is not None for value in _hash_tuple_values(item.hashes))
```

This considers an empty string a valid hash.

For example:

```python
ShokoHashes(
    ed2k="",
    sha1=None,
    crc32=None,
    md5=None,
)
```

passes the check.

However, Shoko can use an empty ED2K string while preparing a new file record before hashing has completed.

A non-null value is not necessarily a usable hash.

## Required correction

Validate that the required hash is nonempty and structurally valid.

For Shoko 5.3.3, investigate requiring a complete ED2K identity before linking.

Do not assume that a partially populated hash object means hashing has completed.

Preserve the corrected behavior allowing fully hashed files with `Imported=null`.

Tests must distinguish:

* `Imported=null`, valid complete ED2K: eligible.
* `Imported=null`, empty ED2K: ineligible.
* Whitespace-only ED2K: ineligible.
* Malformed ED2K: ineligible.
* Missing hashes: ineligible.
* Complete hashes, no XRefs: eligible.

**Acceptance:** The `Imported=null` fix remains functional without admitting files whose hashing state is incomplete.

---

# F14 — P2: Deployment documentation contains inconsistent campaign instructions

## Affected files

* `docs/deployment.md`
* `README.md`

## Evidence

The deployment guide currently prepares a campaign using:

`campaign prepare --limit 1`

It then explains that the daemon will process the remaining campaign automatically.

However, the approved plan contains only one file.

There are no additional approved files for the daemon to process.

The same document also contains an older warning about stopping after the first campaign batch because of a known source-continuity defect.

This contradicts the newly implemented multi-batch behavior.

## Required correction

Update the deployment instructions to prepare the complete intended campaign first.

The operator should then apply only a small canary subset from that approved campaign.

After verification and explicit operator confirmation, the daemon may process the remaining approved files automatically.

Remove obsolete warnings about defects that have been fixed.

Ensure the documentation accurately explains the separate daemon and campaign safety attestations after F8 is resolved.

Cross-check example commands against actual CLI arguments.

**Acceptance:** Following the deployment guide produces a finite, fully approved backlog campaign that can proceed safely after the canary.

---

# Implementation priority

Address findings in this order:

**Phase A — Core campaign correctness**

1. F8: Independent safety attestations.
2. F9: Unique budget-cycle identifiers.
3. F10: Explicit canary confirmation.
4. F11: Fresh per-submission authorization and shutdown checks.

These four must be fixed before production automatic campaign processing.

**Phase B — Registry reliability**

5. F12: Correct OCI manifest traversal and tag-provenance handling.

**Phase C — File readiness and documentation**

6. F13: Validate actual hash contents.
7. F14: Correct deployment documentation.

Avoid adding new infrastructure or unnecessarily restructuring the existing application.

---

# Final validation requirements

Run the project's configured checks:

```bash
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
uv run mypy
git diff --check
docker build -t shoko-sonarr-linker:review .
```

Add a realistic simulated campaign containing at least 100 files.

Use deliberately small budgets so the test crosses several per-cycle boundaries.

Verify that automatic processing:

* Continues beyond the per-cycle cap across later cycles.
* Survives process restart.
* Never widens the approved file or episode set.
* Requires explicit canary confirmation.
* Requires valid safety authorization for each submission.
* Stops new submissions after revocation or shutdown.
* Preserves uncertain sends without replaying POST requests.
* Handles mapping updates and Shoko queue anomalies safely.

Run independent regression tests for the GHCR OCI registry operations.

Perform a separate adversarial review of all changes affecting Shoko POST submission, campaign authorization, and durable SQLite state.

Record findings, actual test results, and local commit SHAs in `STATUS.md`.

Keep `PLAN.md` unchanged.

Do not perform live writes or push the resulting commits without authorization.

## Completion criteria

Remediation is complete when:

1. Automatic campaigns operate with valid independent safety attestations.
2. Campaigns progress through multiple real budget cycles without stalling.
3. A canary cannot authorize automatic draining until explicitly confirmed by the operator.
4. Expired safety authorization prevents further POST requests.
5. Graceful shutdown prevents additional submissions after the current protected operation.
6. GHCR provenance verification handles OCI manifests and indexes correctly.
7. Hash-readiness checks reject empty or incomplete hashes.
8. Deployment instructions accurately represent the implemented behavior.
9. All targeted and full regression tests pass.
10. Independent review finds no outstanding P0/P1 or acceptance-blocking P2 defects.
11. `STATUS.md` contains accurate evidence.
12. `PLAN.md` remains immutable.

Continue through the remediation without routine approval requests.

Preserve all existing file-protection, scope-isolation, and no-overwrite safeguards.
