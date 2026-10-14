# STATUS — Fourth Independent Review Remediation (F21–F25)

**Run:** Fourth independent review handoff per `PLAN.md`.
**PLAN.md SHA-256:** `A77D0FF4410ACFFDC73083878F3629727661CBEB99347BAB173E045A3E3DD028` (adopted working-plan baseline; PLAN.md remains immutable).
**Baseline HEAD:** `4e0f6f0693c089e8dd5694404298777123944518` (matches the reviewed commit named in the current PLAN.md).
**Branch:** `main`.
**Environment:** Windows / Python 3.14.0 / uv 0.9.18 / Docker 29.7.2; `jq` unavailable.

## Orientation and plan baseline

- The current working PLAN.md differs from the previous execution ledger's digest. The user explicitly requested implementation of the current working PLAN.md, which identifies baseline HEAD `4e0f6f0`; that is treated as authorization to adopt this plan version. Its current SHA-256 is recorded above. PLAN.md must not be edited or included in implementation commits.
- At task start, `PLAN.md` was modified in the worktree and `STATUS.md` was empty (the prior ledger content had been removed in the worktree). These are pre-existing user changes; the current ledger records the adopted baseline and execution evidence.
- No production Sonarr/Shoko API writes or deployment are authorized. The user later explicitly authorized pushing `main`, including the GHCR publishing workflow that push triggers; no live API writes occurred. All Shoko/Sonarr behavior validation is offline/mocked.

## Ledger

| Milestone | Status | Notes |
| --- | --- | --- |
| Orient and adopt plan baseline | complete | Current PLAN.md hash captured; HEAD matches its reviewed commit. |
| F23 — GHCR workflow planning and latest gate | complete | Shared offline-tested planner; safe per-SHA publication/rechecks; branch-tip backfill; strict provenance/media validation; latest gated on verified current main head. Independent spec and code-quality review accepted after fixes/adjudication. Local implementation commit: `22ecaff86266c88152a2f7b45cb01745098a1870`. |
| F21 — Frozen Shoko file identity | complete | ED2K is bound to link plans, fingerprints, durable campaign approval, discovery continuity, preflight, and async verification. Legacy campaign plans require re-prepare/re-approval. Campaign drift quarantine accounting is persisted and counts once. Independent sequential review accepted after two P2 fixes. Local implementation commit: `5eff64ee6bdaf89faad05993d62da005fc7010c4`. |
| F22 — Incomplete reconciliation handling | pending |  |
| F24 — Candidate-level mapping drift | pending |  |
| F25 — Cross-scope write reporting | pending |  |
| Final checks and adversarial review | pending |  |

## F23 — Restore automated publication (complete)

- Added `scripts/ghcr_plan.py` as the shared executable path for validated tag extraction, candidate membership, bounded provenance classification, promotion verification, and overflow-safe `max_shas` parsing. It ignores empty tag-file lines, rejects malformed SHA tags and contradictory `package_missing` responses, and rotates the 10-tag provenance window across workflow run numbers.
- Replaced the three failing jq constructions with the shared planner: newline parsing no longer applies string functions to null, candidate membership is explicit, and provenance reports parse JSON structurally. Published SHAs are skipped, only recognized absence responses are publishable, and unknown provenance fails closed.
- Serialized publish legs by exact SHA and rechecked the immutable tag under the lock. A verified existing image is skipped; a proven-absent tag may build/push; unknown state fails closed. Historical matrix checkouts receive trusted current verifier tools through same-run artifacts. Removed unnecessary Actions API permissions and workflow-level per-ref concurrency that could replace a pending unique push.
- `promote-latest` now runs only for push/schedule/main-backfill events, requires successful test/plan and publish success or an empty matrix, validates the exact run SHA’s immutable image provenance, serializes promotions, and checks current main immediately before promotion. README notes the out-of-band registry-writer race limitation; GHCR offers no conditional tag update in this workflow.
- Branch dispatch backfill is restricted to an in-repository `refs/heads/*` ref, checks out trusted planner code from main, fetches the selected branch tip, validates `to_sha` and `from_sha` ancestry, and can recover capped overflow on an unmerged branch. Feature-branch backfills cannot promote `latest`.
- Registry discovery now requires an explicit tags array, classifies `package_missing` only for recognized `NAME_UNKNOWN` responses, and fails closed on other 401/404 responses. Link pagination handles repeated headers, parameter ordering, empty list members and relative URLs while rejecting malformed/offsite targets. Manifest, child-manifest, descriptor, and config media types/digests are validated before revision labels are trusted. Cross-origin redirects strip Authorization/Proxy-Authorization, and HTTPS downgrades are rejected.
- Tests cover registry/tag edge cases, cap rotation and backfill reruns, manifest/config media mismatches, duplicate headers, redirect credential handling, branch-ref dispatch bounds, main-only latest gates, and workflow permissions. The available `jq` executable is unavailable locally; shared Python behavior is tested directly, and the workflow’s remaining jq uses are structurally checked. All tests are offline/mocked.

### F23 verification evidence

- `uv run --offline --group dev pytest -o addopts="" -q` → **1089 passed** (2026-10-13).
- `uv run --offline --group dev ruff check .` → passed.
- `uv run --offline --group dev ruff format --check .` → passed (51 files already formatted).
- `uv run --offline --group dev mypy` → passed (47 source files).
- `uv run --offline --group dev mypy scripts/ghcr_plan.py scripts/ghcr_tags.py` → passed (2 source files).
- `git diff --check` → passed (no output).
- Local F23 implementation commit: `22ecaff86266c88152a2f7b45cb01745098a1870` (`fix: harden GHCR publishing workflow`).
- Regression tests were developed test-first; initial runs reproduced the original jq/provenance/gate defects and each fix passed the focused tests before the complete suite.
- Independent spec-compliance and code-quality review found no unresolved acceptance-blocking defect after fixes. The `NAME_UNKNOWN` bootstrap interpretation was reviewed against GitHub’s documented GITHUB_TOKEN package-access rules; a token that cannot access an unlinked package cannot successfully push it. Exact GHCR response behavior remains live-unverified.
- No GitHub Actions workflow run, GHCR publication, Docker build, deployment, or production Sonarr/Shoko write was performed. Live GHCR/Actions state remains **EXTERNAL/UNVERIFIED**. An out-of-band GHCR writer can race after the per-SHA preflight; this is documented and is outside the workflow’s serialization guarantee.

## F21 — Freeze Shoko file identity (complete)

- `LinkPlan` now captures the raw Shoko ED2K, validates it as exactly 32 hexadecimal characters, and includes its normalized value in the plan fingerprint. Fresh preflight compares the current ED2K to the frozen value before POST; optional SHA1/MD5/CRC32 changes do not invalidate the explicit ED2K identity contract.
- The first-seen Shoko identity snapshot is compared on each discovery and remains unchanged across restart. A reused Shoko file ID with different content is not replanned. Files in possibly-sent recovery states remain held for polling; XRef verification requires the persisted original ED2K to match, so changed content cannot be marked verified or trigger a replay.
- Campaign plan schema `/2` persists ED2K in each approved file binding. Schema `/1` plans are rejected with an explicit re-prepare/re-approve instruction. Candidate and pending-target drift is quarantined before POST.
- Sequential read-only review found two P2 campaign accounting gaps and both were fixed: a quarantine discovered during a dry-run/no-drain cycle now persists a one-time progress marker that is atomically claimed on apply, including after restart; campaign-bound `WAITING` rows advanced to `MATCHED` during reconciliation also retain that quarantine marker. Repeated quarantines remain zero progress, and `MATCHED`/`WAITING` are not made submission-queueable. Final review found no remaining findings.
- All tests are offline/mocked; no live Sonarr/Shoko calls or writes were performed.

### F21 verification evidence

- `uv run --offline --group dev pytest -o addopts="" -q tests/test_linking.py tests/test_daemon.py tests/test_campaign.py` → **294 passed**.
- `uv run --offline --group dev pytest -o addopts="" -q` → **1105 passed** (2026-10-13).
- `uv run --offline --group dev ruff check .` → passed.
- `uv run --offline --group dev ruff format --check .` → passed (51 files already formatted).
- `uv run --offline --group dev mypy` → passed (47 source files).
- `git diff --check` → passed (no whitespace errors).
- Local implementation commit: `5eff64e` (`fix: bind Shoko ED2K to approved link plans`). `PLAN.md` SHA-256 remains `A77D0FF4410ACFFDC73083878F3629727661CBEB99347BAB173E045A3E3DD028`.

## Next action

Begin Phase 3 (F22): require complete reconciliation snapshots before manual or daemon campaign candidate-drift decisions; incomplete observations must defer without quarantine or approval mutation.
