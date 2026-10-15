# STATUS — Sixth Independent Review Remediation (F31–F33)

**Run:** Implement the approved sixth independent review handoff in `PLAN.md`.
**Approved PLAN.md SHA-256:** `0C543E28DD12FA124E845110681F11F9281D4D7057A53CC9F22984A900C64AEB`.
**Reviewed baseline HEAD:** `4ef92355fdc03b41499731e0180a18821d6b9d51`.
**Branch:** `main` (user selected the current checkout).
**Execution mode:** Offline and mocked tests only. No push, deployment, production write, or live integration is authorized.

## Baseline and recovery

- `PLAN.md` matched the tracked file at baseline HEAD. `STATUS.md` was empty at task start, so the digest above establishes the frozen plan version for this run. `PLAN.md` remains unchanged.
- Baseline `uv run pytest -q` completed through 100% and exited successfully.
- F31 reproduction: `uv run pytest -q tests/test_mapping_store.py::test_interrupted_active_xml_replacement_recovers_previous_generation` failed as expected. Injecting an exception after replacing `active.xml` caused reopened `load_active()` to return `None` instead of recovering generation A.
- Preserve the F26–F30 protections, especially source provenance, source-transition refusal, bounded recovery backoff, campaign accounting, and streamed-download behavior.

## Milestone ledger

| Milestone | Status | Evidence / notes |
| --- | --- | --- |
| Baseline and frozen plan | complete | Clean tracked baseline at `4ef9235`; plan SHA-256 recorded above. Full baseline pytest run passed. |
| F31 — Mapping crash consistency | complete | Immutable digest-identified generation files are selected by an atomic pointer. Source-transition and scheduler snapshots, epoch/attempt fencing, verified legacy migration, and lock-fenced reads/cleanup preserve last-known-good data across interrupted activation and state loss. |
| F32 — Campaign quarantine accounting | complete | First-time pending progress claims consume the batch allowance; previously recorded claims are skipped without recounting. Mocked tests cover 100-file DRY_RUN quarantine staging, `--batch-size 1`, restart before attribution, no extra POSTs, and unchanged submission/rolling-budget counters. |
| F33 — Operational deadline guidance | pending | — |
| Final validation and independent review | pending | Project-wide validation and final review remain for the end of F33. |

## F31 — Mapping crash consistency (complete)

- Replaced in-place XML/meta activation with immutable `generations/<id>/active.xml` + `active.meta.json`, selected by atomic `active.generation.json`. Candidate XML is parsed/indexed and staged metadata/content digests are checked before pointer publication; staged and migrated XML digest checks stream from disk.
- Pointer snapshots preserve scheduler state. `source-transition.json` and pointer pending-state fences preserve source epochs and retry state, including source switchbacks, state loss, initial default-source failures, and legacy flat caches.
- State transitions, activation, recovery, status snapshots, and active-generation reads share the generation lock. Attempt IDs prevent older responses from changing a newer refresh schedule. Daemon freshness identity checks are digest-only and include the source epoch; campaign quarantine uses the lock-held identity path.
- Legacy migration validates upstream-derived counts and effective digests when applicable. Override-aware CLI status, report, health, and daemon generation views use the configured overrides.
- Fault-injection regression coverage exercises XML staging, metadata staging, pointer publication, bookkeeping, and post-activation interruptions; additional tests cover source provenance, state/backoff recovery, formatting-only efficiency, lock contention, concurrent status/read cleanup, and stale refresh responses.

### F31 verification evidence

- `uv run pytest -q tests/test_campaign.py tests/test_mapping_store.py tests/test_daemon.py tests/test_cli_m7.py tests/test_e2e.py` → completed through **100%**, exit 0 (mocked/offline integrations only).
- `uv run ruff check .` → passed.
- `uv run ruff format --check .` → **51 files already formatted**.
- `uv run mypy` → passed (**47 source files**).
- `git diff --check` → no whitespace errors. Git reported the existing Windows CRLF/LF working-copy warning for `tests/test_campaign.py`.
- Final sequential read-only review → no P0/P1/P2/P3 findings.
- Local F31 implementation commit: `f31fc88` (`fix: make mapping activation crash-safe`).
- No live Sonarr/Shoko requests or writes occurred; no push, publish, or deployment was performed.

## F32 — Campaign quarantine accounting (complete)

- `_entry_gate()` now claims only a pending progress marker. A successful first claim increments both `attempts` and `quarantined`, so the existing batch guard bounds persistent attribution work. Already-claimed or unmarked quarantines defer without consuming the allowance or adding quarantine progress.
- Regression coverage creates 100 drift quarantines during ordinary DRY_RUN, closes the state/dependency handles, then applies with batch size one. It verifies exactly one marker is claimed, the other 99 remain pending, and no Shoko POST, campaign `total_sent`, or rolling submission budget changes. The parameterized case verifies all 100 already-recorded markers are not recounted. A mixed-case test confirms an already-counted quarantine does not starve a valid candidate; a repeated bounded-invocation test confirms remaining claims progress one per invocation.
- Sequential read-only acceptance review of the gate, atomic state claim, candidate loop guard, test coverage, and diff found no P0/P1/P2/P3 findings. F29 new-quarantine behavior remains covered by the existing 100-file one-file-canary and related campaign regressions.

### F32 verification evidence

- `uv run pytest -q tests/test_campaign.py::test_batch_one_bounds_attribution_of_preexisting_quarantine_markers tests/test_campaign.py::test_batch_one_skips_accounted_quarantine_before_valid_candidate_and_pending_marker tests/test_campaign.py::test_pending_quarantine_attribution_completes_across_bounded_batches tests/test_campaign.py::test_one_file_canary_bounds_reconciliation_sweep_quarantines tests/test_campaign.py::test_previously_quarantined_candidate_does_not_consume_batch_allowance tests/test_campaign.py::test_repeated_ed2k_campaign_quarantine_is_deferred_and_counts_as_zero_progress` → passed (**7 cases**, including parameterized values).
- `uv run pytest -q tests/test_campaign.py` → passed through **100%** (mocked/offline integrations only).
- `uv run ruff check src/shoko_linker/campaign.py tests/test_campaign.py` → passed.
- `uv run ruff format --check src/shoko_linker/campaign.py tests/test_campaign.py` → **2 files already formatted**.
- `uv run mypy src/shoko_linker/campaign.py src/shoko_linker/state.py tests/test_campaign.py` → passed.
- `git diff --check` → no whitespace errors (Git emitted the working-copy CRLF/LF normalization warning for `tests/test_campaign.py`).
- Local F32 implementation commit: `ac157eb` (`fix: bound campaign quarantine attribution`).
- No live Sonarr/Shoko requests or writes occurred; no push, publish, or deployment was performed.

## Next action

Start F33: inspect campaign preparation, configured rolling limits, and operator documentation; add the requested optimistic completion-horizon warning without changing deadlines, safety leases, or submission limits.
