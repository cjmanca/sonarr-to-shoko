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
| F28 — Shared daemon-cycle budget | pending | |
| F29 — Bounded campaign processing | pending | Must include sweep mutations and candidate-loop work in the same bound. |
| F30 — Streamed mapping download | pending | |
| Final checks and read-only review | pending | |

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
- All service requests were mocked/offline; no live Sonarr/Shoko call or write occurred.

## Next action

Add a regression that combines new-arrival and automatic-campaign sends under one daemon-cycle key, including verification that releases pending slots do not reset the shared per-cycle count.
