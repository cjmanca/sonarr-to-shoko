# STATUS — Fifth Independent Review Remediation (F26–F30)

**Run:** Implement the approved fifth independent review handoff in `PLAN.md`.
**Approved PLAN.md SHA-256:** `CE5993C0E5F1DF5E2F33F18344E2A96C35E88B9817C9F554CA395B6E913CD272`.
**Reviewed baseline HEAD:** `665568dfab1a6101bcb62bbf82ed4650ba9e27bd`.
**Branch:** `main`.
**Execution mode:** Offline and mocked tests only. Local commits to `main` are authorized. No push, production write, or deployment is authorized.

## Baseline and approved design

- The worktree plan differs from the prior committed ledger's digest; the user explicitly approved adopting the current worktree plan as the immutable baseline. Its reviewed commit matches current `HEAD`. Keep `PLAN.md` unchanged and verify the approved digest at completion.
- At task start, `PLAN.md` was already modified and `STATUS.md` was empty in the worktree. The approved plan content is treated as user work and is not rewritten.
- Keep the existing single mapping cache and bind active authorization, ETag, and refresh scheduling to the normalized full source URL. Retain old bytes after source changes only as non-authorizing recovery data; make the first fetch immediate and persist bounded backoff after failure.
- Share the daemon's existing unique SQLite budget key between arrival and automatic-campaign sends. Manual `campaign apply` continues using its own key. Preserve rolling and pending-job limits.
- Bound campaign work across both the reconciliation sweep and candidate loop. Count meaningful work directly; do not reduce work as quarantines increase. Already-processed quarantine rows must not starve later entries.
- Add opt-in HTTP streaming for mapping downloads only. Preserve the existing default buffered behavior for other callers and close mapping responses on every terminal path.

## Milestone ledger

| Milestone | Status | Evidence / notes |
| --- | --- | --- |
| Adopt approved baseline and design | complete | User approved plan digest above and recommended design, including the F29 `--batch-size 1` sweep-quarantine acceptance check. |
| F26/F27 — Mapping source provenance and recovery | complete | Active authorization and persisted refresh state are now bound to the normalized full source URL. Source transitions immediately clear validators/schedule and remain non-authorizing through failures/backoff until a successful fetch activates that source. Invalid active generations trigger immediate recovery; failed recovery honors persisted backoff across restart. Sequential read-only review found no remaining mapping-provenance/recovery issue. |
| F28 — Shared daemon-cycle budget | complete | Automatic campaign sends now reserve against the daemon's existing durable cycle key; manual apply still allocates a unique batch key. An integration test proved five completed arrival sends leave zero pending slots but still prevent five more campaign submissions in the same cycle; the deferred campaign uses the next cycle's fresh cap. |
| F29 — Bounded campaign processing | complete | Direct work-attempt accounting replaces subtraction. Reconciliation defers campaign identity quarantines to the bounded candidate loop in both manual and automatic apply paths. A 100-file `--batch-size 1` canary quarantines one file; a 100-file all-mapping-drift batch of five quarantines five. Mixed sends/quarantines and preexisting quarantine cases pass. |
| F30 — Streamed mapping download | complete | Mapping refresh uses HTTPX streaming, reads capped raw chunks into one bytearray without a second body-sized copy, and closes responses on success, overflow, errors, 304, and interruption. Focused streaming tests and related source-wiring tests pass. |
| Final checks and read-only review | complete | Full configured tests, quality checks, Docker build/smoke, and sequential read-only review passed. Approved PLAN.md digest rechecked; all implementation slices committed locally on main. |

## F26/F27 — Mapping source provenance and recovery (complete)

- Verified the findings at the approved baseline: `load_active()` did not compare the cache's stored source URL with the configured URL, while `due()` trusted only `next_due_iso`; request headers could send the old source's ETag after reconfiguration.
- `load_active()` now checks normalized metadata and scheduler source identities and refuses a pending source transition. The transition marker also prevents an old generation from becoming active if configuration changes back before a successful refresh.
- Source changes reset source-specific validators, last-success state, and schedule before the request. A fresh successful response activates with current provenance even if mapping targets match; failures retain old XML bytes only as non-authorizing recovery data and persist bounded retry backoff.
- Missing, corrupt, checksum-mismatched, malformed, or unusable active caches trigger a refresh independent of the normal schedule. Stale ETags are omitted/cleared, and repeated failed recovery attempts wait until the persisted backoff expires, including after reopening the store.
- Regression coverage includes same-host path changes, valid source-B activation, failed B without fallback, switching back to A after failed B, intact source-A restart, deleted/corrupt cache metadata and XML, successful recovery, daemon startup recovery, and restart during recovery backoff. The pre-fix focused run reproduced **9 failures**.

### F26/F27 verification evidence

- `uv run --offline --group dev pytest -o addopts="" -q tests/test_mapping_store.py tests/test_daemon.py::test_daemon_recovers_missing_active_cache_before_weekly_due_time tests/test_daemon.py::test_restart_does_not_disable_refresh_or_tight_loop tests/test_daemon.py::test_mapping_activated_reevaluates_backlog_without_posts` → **50 passed**.
- Focused `ruff check` → passed; focused `ruff format --check` → **4 files already formatted**.
- `uv run --offline --group dev mypy` → passed (**47 source files**).
- Focused `git diff --check` → no whitespace errors (Git emitted the existing Windows CRLF/LF working-copy warning for `tests/test_daemon.py`).
- Local F26/F27 implementation commit: `b2b920d` (`fix: bind mapping cache to configured source`).
- All service requests were mocked/offline; no live Sonarr/Shoko call or write occurred.

## F28 — Shared daemon-cycle submission budget (complete)

- Confirmed the intended scope in `PROJECT.md` §3: new-arrival and authorized automatic-campaign work use the same per-cycle ceiling. Clarified the `README.md` budget description: manual `campaign apply` invocations receive their own bounded cycle budget.
- Automatic drain now receives the unique budget key allocated for its enclosing daemon cycle and passes it to `apply_campaign()`. Manual campaign invocations continue calling `StateStore.begin_cycle()` for a new unique key; SQLite remains authoritative, with no parallel counter.
- The F28 regression completes verification for five new arrivals before the campaign drain. It proves the released pending slots do not reset `cycle_new_submissions`, the same-cycle campaign sends zero, the candidates remain eligible after the recoverable cooldown, and the next outer cycle submits within a fresh cap. It also checks rolling totals, zero outstanding jobs, and no duplicate file sends.
- Sequential read-only review confirmed the outer CLI allocates restart-unique keys, only `_drain_campaign` uses the shared-key path, manual batches still allocate independently, and existing rolling/pending reservation/release funnels were not changed. No unresolved F28 issue.

### F28 verification evidence

- Pre-fix `test_daemon_arrivals_and_campaign_share_one_cycle_submission_budget` reproduced **10 total requests** with a configured cap of 5.
- Focused F9/manual/restart plus F28 integration checks → **5 passed**.
- `uv run --offline --group dev pytest -o addopts="" -q tests/test_campaign.py tests/test_f_campaign_e2e.py` → **149 passed**.
- Focused `ruff check` and `ruff format --check` → passed; `uv run --offline --group dev mypy` → passed (**47 source files**); scoped `git diff --check` → no whitespace errors.
- All service requests were mocked/offline; no live Sonarr/Shoko call or write occurred.

## F29 — Bounded campaign processing (complete)

- Replaced `attempts - quarantined` with a direct meaningful-work count: pending-target resolution attempts, send attempts, and every new campaign quarantine decision count; already verified or previously quarantined rows do not consume new-work allowance.
- The reconciliation sweep preceding campaign apply no longer mutates campaign identity-drift quarantines outside the candidate loop. It carries the drift details into the frozen sweep result so the candidate loop can apply the same fail-closed quarantine with full provenance and count it once. Automatic daemon cycles also defer outer-sweep identity quarantines to their bounded campaign loop.
- A read-only path audit found `_quarantine_source_identity_drift` as the daemon reconciliation quarantine path and `_campaign_quarantine` as the apply-loop quarantine funnel; both are now covered by the bounded path. Protected Shoko associations and send-recovery states remain guarded.
- Regression cases cover 100 frozen-identity drifts with `--batch-size 1` (the pre-fix run made **100** quarantines), 100 mapping-target drifts with batch size five, mixed quarantine/send work, a previously quarantined head followed by an eligible candidate, an already-verified canary at the plan head, pending-target bounds, restart, and automatic-daemon outer+inner sweeps.
- Sequential read-only review found no remaining acceptance-blocking finding. All Sonarr/Shoko requests were mocked; no live write occurred.

### F29 verification evidence

- Focused F29 regression matrix → **12 passed**.
- `uv run --offline --group dev pytest -o addopts="" -q tests/test_campaign.py tests/test_f_campaign_e2e.py tests/test_daemon.py` → **234 passed**.
- `uv run --offline --group dev ruff check .` → passed; `uv run --offline --group dev ruff format --check .` → **51 files already formatted**.
- `uv run --offline --group dev mypy` → passed (**47 source files**); `git diff --check` → no whitespace errors (Windows CRLF/LF working-copy warning only).
- Local combined F28/F29 implementation commit: `053bdaa` (`fix: enforce campaign cycle and batch limits`).

## F30 — Streamed mapping download (complete)

- Added opt-in `stream=True` to the shared HTTP sender, preserving its buffered default for unrelated Sonarr/Shoko callers. Mapping refresh alone selects streaming and retains the existing source-origin binding, identity encoding, no-redirect policy, timeouts, ETag/304 behavior, validation, and atomic activation.
- `_read_capped()` now iterates raw bytes using chunks no larger than 64 KiB (or `max_bytes + 1` for smaller caps), appends only bytes within the cap to a single `bytearray`, stops at first overflow, and closes in `finally`. The parser/hash/activation path accepts that bytearray without making a second full-size body copy.
- Refresh converts streaming `httpx.HTTPError` into the existing transport-failure/backoff outcome after the response is closed. Interruptions propagate while the `finally` still closes the response. Last-known-good data remains active on same-source read failures and is never replaced by oversize or malformed content.
- Source-response test fixtures now use HTTPX byte streams. A focused sender test confirms opt-in streaming leaves the body unconsumed on return and default callers remain buffered. Mapping tests cover below/exact/over limits, one-chunk early stop without generating the oversized tail, response closure for oversize/non-200/304/read-error/interruption, unchanged LKG data, and successful mapping parsing.
- Sequential read-only review checked the sender flag reaches `Client.send(stream=True)`, mapping refresh is the only production opt-in, redirects still close/refuse at `max_redirects=0`, all response terminal paths close, and raw-byte accounting plus bytearray retention avoid whole-response buffering. No unresolved finding.

### F30 verification evidence

- `uv run --offline --group dev pytest -o addopts="" -q tests/test_mapping_store.py tests/test_http.py` → **75 passed**.
- Related source-wiring checks: daemon **80 passed**; CLI/attestation **62 passed**; end-to-end **28 passed** and F campaign e2e **22 passed**.
- Full Ruff check/format → passed (**51 files already formatted**); `uv run --offline --group dev mypy` → passed (**47 source files**); `git diff --check` → no whitespace errors (Windows CRLF/LF working-copy warnings only).
- Local F30 implementation commit: `84e4752` (`fix: stream and cap mapping downloads`).
- All requests remained mocked/offline; no live Sonarr/Shoko requests or writes occurred.

## Final verification and handoff

- Full configured suite: `uv run pytest -q` completed through **100%** and exited successfully.
- `uv run ruff check .` → passed; `uv run ruff format --check .` → **51 files already formatted**; `uv run mypy` → passed (**47 source files**); `git diff --check` → no whitespace errors (Git reports Windows CRLF/LF working-copy warnings for changed files).
- `docker build -t shoko-sonarr-linker:review .` → passed. Container `--help` passed; `validate --offline /app/config.example.yaml` passed with placeholder API-key environment values and made no Sonarr/Shoko requests; runtime UID/GID is `1000:1000`.
- Final sequential read-only review across mapping provenance/recovery, combined budget reservations/restarts, reconciliation plus candidate-loop batch bounds/protected associations, streaming byte limits/response closure, and no-replay behavior found no unresolved P0/P1 or acceptance-blocking P2 issue.
- Live Sonarr/Shoko integration, production writes, GitHub Actions/GHCR, and deployment remain **EXTERNAL/UNVERIFIED**; none were performed.
- Approved `PLAN.md` SHA-256 rechecked as `CE5993C0E5F1DF5E2F33F18344E2A96C35E88B9817C9F554CA395B6E913CD272`. It remains an uncommitted pre-existing user change and is excluded from implementation commits.
- Local commits: `b2b920d` (F26/F27 implementation), `c6a408b` (F26/F27 evidence), `053bdaa` (F28/F29 implementation), `d6f7901` (F28/F29 evidence), and `84e4752` (F30 implementation).
- Changed implementation/test/documentation files: `README.md`, `src/shoko_linker/campaign.py`, `src/shoko_linker/daemon.py`, `src/shoko_linker/http.py`, `src/shoko_linker/mapping_store.py`, `src/shoko_linker/mappings.py`, `tests/e2e_harness.py`, `tests/test_attestation.py`, `tests/test_audit.py`, `tests/test_campaign.py`, `tests/test_cli_m7.py`, `tests/test_daemon.py`, `tests/test_f_campaign_e2e.py`, `tests/test_http.py`, and `tests/test_mapping_store.py`.

## Next action

No implementation work remains. Live integration and deployment checks are external operator work and remain unverified.
