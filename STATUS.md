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
| F22 — Incomplete reconciliation handling | complete | Manual apply and the daemon's inner campaign sweep now require `reconciliation_ready` before campaign drift decisions. Incomplete snapshots defer with stable source reason codes, zero POSTs, unchanged approvals/progress, and no zero-progress breaker update. Sequential read-only review accepted. Local implementation commit: `70628e8f4229540afc5335f7d7e13a6dc8924bea`. |
| F24 — Candidate-level mapping drift | complete | Candidate-level mapping continuity, generation freshness, origin-bound pause handling, and durable ambiguity provenance. Sequential adversarial review findings fixed. Local implementation commit: `9ebf445`. |
| F25 — Cross-scope write reporting | complete | Cycle/health/report output now separates new-arrival and campaign eligibility, submissions, total link requests, and path/all-path suppression. Sequential read-only review found no remaining findings. Local implementation commit: `b0bfe47`. |
| Final checks and adversarial review | complete | The final review's mapping-activation/quarantine race was fixed with a cross-process generation lock, full checks and a second review passed, and the follow-up CI/GHCR workflows succeeded for code head `d45e21ea5cd353e094f550458068fa3993ff0b92`. The only unrelated worktree modification remains the pre-existing `PLAN.md` change. |

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

## F22 — Defer incomplete campaign reconciliations (complete)

- Campaign apply now refuses candidate and mapping-drift decisions unless its inner dry-run sweep reports `reconciliation_ready=True`. On an incomplete sweep it raises a dedicated deferral error before eligibility/mapping changes or candidate processing. Manual apply reports the source reason; daemon drains surface `campaign_deferred:<id>:reconciliation_incomplete` and source reason codes, then return without touching the zero-progress breaker.
- Daemon cycle reports now include stable `sonarr_inventory_incomplete:<reason>` and `shoko_snapshot_incomplete:<reason>` codes. Existing outer-cycle readiness gating remains in place; the inner-sweep exception covers the case where the outer daemon snapshot was complete but the campaign's fresh sweep is not.
- Mocked regression coverage includes an incomplete/short Shoko page, inconsistent Shoko `Total`, malformed Shoko file field, transient failure on a later Shoko page, incomplete Sonarr episode-file inventory, and a daemon cycle whose outer snapshot is complete but inner campaign sweep is incomplete. Tests verify zero additional POSTs, unchanged campaign status/digest/bindings/progress, no quarantines, and normal processing after a subsequent complete snapshot. Existing complete-snapshot ED2K drift coverage remains passing.
- All validation is offline/mocked; no production Sonarr/Shoko calls or writes were performed.

### F22 verification evidence

- `uv run --offline --group dev pytest -o addopts="" -q tests/test_campaign.py tests/test_daemon.py tests/test_shoko.py tests/test_sonarr.py` → **338 passed**.
- `uv run --offline --group dev pytest -o addopts="" -q` → **1111 passed** (2026-10-14).
- `uv run --offline --group dev ruff check .` → passed.
- `uv run --offline --group dev ruff format --check .` → passed (51 files already formatted).
- `uv run --offline --group dev mypy` → passed (47 source files).
- `git diff --check` → passed (no whitespace errors).
- Sequential read-only F22 review found no remaining findings. Local implementation commit: `70628e8f4229540afc5335f7d7e13a6dc8924bea`. `PLAN.md` SHA-256 remains `A77D0FF4410ACFFDC73083878F3629727661CBEB99347BAB173E045A3E3DD028`.

## F24 — Preserve approved targets across unrelated mapping updates (complete)

- Campaign mapping authorization is now compared against each approved file's immutable AniDB target identities. Unrelated Anime-Lists generation changes no longer invalidate or expand an approved file set; genuine target drift is quarantined per candidate with approved/current identities, mapping provenance, and revision context. Historical verified-link drift is reported without modifying existing XRefs.
- Campaign batches bind to the exact mapping snapshot from their reconciliation sweep, recheck freshness after the Shoko queue read and before drift quarantines, and retain only a compact generation key in cycle history. A regression covers mapping-generation changes during pending-target resolution and Shoko episode-ID drift.
- Daemon-origin mapping-churn pauses can defer new-arrival writes while an authorized campaign continues through per-candidate target checks. Pause origin is atomically installed, operator pauses cannot be relabeled for campaign bypass, and attestation recovery uses an atomic expected-reason/origin compare-and-clear so a concurrent operator pause remains in effect.
- Mapping ambiguity reasons now compact/group candidate provenance within the durable detail limit and explicitly count omitted candidates. Regressions cover 32 competing candidates and persistence of provenance in the campaign quarantine detail.
- Tests use mocked/offline Sonarr and Shoko services; no production calls or writes occurred. Sequential read-only adversarial review found and fixed: P1 concurrent operator-pause clearing; P2 stale Shoko-ID-drift quarantine after a mapping-generation swap; and provenance clipping in durable campaign details. No unresolved F24 blocking findings remain.

### F24 verification evidence

- `uv run --offline --group dev pytest -o addopts="" -q` → **1125 passed** (2026-10-14).
- Focused regressions for bounded ambiguity provenance, persisted candidate provenance, mapping-generation deferral on Shoko-ID drift, and attestation recovery preserving an operator pause → **4 passed**.
- `uv run --offline --group dev ruff check .` → passed.
- `uv run --offline --group dev ruff format --check .` → passed (51 files already formatted).
- `uv run --offline --group dev mypy` → passed (47 source files).
- `git diff --check` → passed (only expected Windows CRLF-to-LF warnings from Git).
- `docker build -t shoko-sonarr-linker:review .` → passed; built image runs as UID/GID `1000:1000`.
- Docker smoke: `validate --offline config.example.yaml` passed with zero Sonarr/Shoko requests; `once --apply config.example.yaml` refused with exit **3** under the default-deny config and reported zero remote writes.
- Adopted `PLAN.md` SHA-256 rechecked as `A77D0FF4410ACFFDC73083878F3629727661CBEB99347BAB173E045A3E3DD028`; the pre-existing plan modification remains uncommitted and unchanged.
- Local F24 implementation commit: `9ebf445` (`fix: preserve campaign approval across mapping updates`). Live Sonarr/Shoko integration remains unverified; F24 push-triggered CI/GHCR results are included in the final acceptance record below.
- Independent follow-up review found a P2 TOCTOU window between the campaign mapping freshness check and quarantine persistence. The fix uses a shared cross-process `generation.lock`: candidate quarantine holds it through a fresh generation check and state commit, while mapping activation uses the same lock. If a campaign decision owns the lock, refresh returns `FAILED_LOCKED` and follows the existing bounded retry schedule; if refresh owns it, campaign quarantine defers. The forced-interleaving regression and an after-check stale-generation regression both pass.
- Follow-up full verification after the race fix: `uv run --offline --group dev pytest -o addopts="" -q` → **1128 passed**; Ruff check/format, mypy, and `git diff --check` passed. Docker build and default-deny smoke passed. The review fix commit is `9363209` (`fix: serialize campaign quarantine with mapping updates`); it was pushed and its workflow results are in the final acceptance record below.

## F25 — Report cross-scope write activity accurately (complete)

- `CycleReport` now names new-arrival eligibility/submissions and campaign drain selection/eligibility/submissions separately. `total_link_requests_attempted` is the sum of actual `submit_link` attempts across both cycle paths. The legacy `apply_eligible` and `counts.submitted` remain new-arrival scoped; `writes_suppressed` now means both mutation paths were gated, with per-path suppression fields retained alongside it.
- Cycle JSON, daemon summaries, human report output, and persisted `health`/`report` metadata include the scoped values. The health/report payload returns unknown (`null`) for new metrics when reading older state that predates those metadata keys.
- Regression coverage exercises both leases valid, daemon lease invalid with campaign drain valid, daemon lease valid with campaign lease invalid, both leases invalid, explicit operator global pause, and eligible new-arrival processing with zero requests. It checks the actual request sum and verifies the same cycle activity reaches both `health` and `report` payloads.
- F25 read-only second-pass review found no unresolved findings. All service integration remains mocked/offline; no production calls or writes occurred.

### F25 verification evidence

- `uv run --offline --group dev pytest -o addopts="" -q` → **1127 passed** (2026-10-14).
- Targeted lease/global-pause/zero-request matrix and payload regressions → **6 passed**.
- `uv run --offline --group dev ruff check .` → passed.
- `uv run --offline --group dev ruff format --check .` → passed (51 files already formatted).
- `uv run --offline --group dev mypy` → passed (47 source files).
- `git diff --check` → passed (only expected Windows CRLF-to-LF warnings from Git).
- `docker build -t shoko-sonarr-linker:review .` → passed; runtime user verified as UID/GID `1000:1000`.
- Docker smoke: CLI help passed; `validate --offline config.example.yaml` passed with no Sonarr/Shoko requests; `once --apply config.example.yaml` refused with exit **3** under default-deny configuration and reported no remote writes.
- `PLAN.md` SHA-256 rechecked as `A77D0FF4410ACFFDC73083878F3629727661CBEB99347BAB173E045A3E3DD028`; the pre-existing plan modification remains uncommitted and unchanged.
- Local F25 implementation commit: `b0bfe47` (`fix: report campaign and arrival writes separately`). Live Sonarr/Shoko behavior remains unverified; push-triggered CI/GHCR results are included below.

## Final acceptance gate (complete)

- The final independent read-only review identified the F24 TOCTOU P2 described above. It is fixed; the reviewer rechecked the shared lock/activation/quarantine paths and found no remaining finding.
- The F24/F25 implementation-and-evidence push advanced `main` to `e42780581280adc77e437fad06bcc0182ee92aca`. The four commits were F24 implementation `9ebf4454986ca2df8da572c6164af3af760a8a3f`, F24 evidence `4039f295f7c27f2d792ff9423e299258cce801cd`, F25 implementation `b0bfe47a6f1c67868d21fd80dbcef108fd3a4844`, and F25 evidence `e42780581280adc77e437fad06bcc0182ee92aca`.
- GitHub Actions CI succeeded for head `e42780581280adc77e437fad06bcc0182ee92aca`: [run 38522668978](https://github.com/cjmanca/sonarr-to-shoko/actions/runs/38522668978). Docker build, shared quality checks, CLI help, offline validation, and default-deny smoke all passed.
- GHCR publishing succeeded for run [38522668956](https://github.com/cjmanca/sonarr-to-shoko/actions/runs/38522668956). All four pushed commit SHAs were built/published by the matrix; immutable-SHA provenance and current-main checks passed, and `latest` promotion succeeded.
- Follow-up review-fix CI succeeded for head `d45e21ea5cd353e094f550458068fa3993ff0b92`: [run 38527082285](https://github.com/cjmanca/sonarr-to-shoko/actions/runs/38527082285). Follow-up GHCR run [38527082206](https://github.com/cjmanca/sonarr-to-shoko/actions/runs/38527082206) published the mapping-lock fix `93632095c9e6b5691eef6254ebbece9bf2c7b2ef` and final ledger commit `d45e21ea5cd353e094f550458068fa3993ff0b92`; immutable provenance, current-main validation, and `latest` promotion passed.
- The adopted `PLAN.md` SHA-256 remains `A77D0FF4410ACFFDC73083878F3629727661CBEB99347BAB173E045A3E3DD028`. The pre-existing modified `PLAN.md` remains uncommitted and was not included in any push.
- No production Sonarr/Shoko calls or writes, Unraid deployment, or other production deployment occurred. Those live integration/deployment gates remain **EXTERNAL/UNVERIFIED**.

## Next action

No additional implementation action remains. Live Sonarr/Shoko integration and deployment remain external operator work.
