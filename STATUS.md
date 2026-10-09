# Execution Status — shoko-sonarr-linker

**Owner:** Main project orchestrator only (independent of coding-agent harness). Delegated implementation/review agents receive scoped task briefs and report findings to the orchestrator; they must not read or edit this file or [PLAN.md](PLAN.md).

**Frozen plan SHA-256:** `7f231f3ad6d63a61e920ddb981bd18750a613abb70db23a2744890675c0c45e5`. If the digest changes unexpectedly, stop treating the edited file as approved; record the discrepancy here and request user authorization before replacing the plan.

**Authority:** [PROJECT.md](PROJECT.md) = requirements; [AGENTS.md](AGENTS.md) = general agent procedures; [ORCHESTRATION.md](ORCHESTRATION.md) = harness-independent main-orchestrator workflow; **[PLAN.md](PLAN.md) = frozen milestone sequence and acceptance gates**; **STATUS.md = the only mutable execution/progress record**. Never modify PLAN.md to record progress. Proposed plan changes require user approval before replacing the frozen plan.

## Current checkpoint

- **Goal status:** `ACTIVE` (orchestration goal; plan persisted in goal tool, phases M1→M9).
- **Active milestone / slice:** **M2 PASS (offline gate)** — Sonarr inventory/baseline module implemented, adversarially reviewed (no P0/P1), all P2/P3 findings fixed, 149 tests green, Docker buildable. M1 **PASS** (boundary `aa6b85e`). M0 **PASS** (`d0c9196`).
- **Next concrete action:** Record M2 commit SHA at next ledger touch; prepare M3 brief (Shoko discovery, pagination stability, multi-location ownership, Docker path translation, path+size identity stability, linear reconciliation; include symlink-resolution obligation).
- **Last accepted local commit:** M2 commit (created with this checkpoint, SHA at next touch); M1 boundary `aa6b85e`; prior `d0c9196`.
- **Working tree / uncommitted work:** M2 slice: `src/shoko_linker/sonarr.py`, `tests/test_sonarr.py` (new, all checks green) + this STATUS.md update; committed as the M2 commit created with this checkpoint.
- **Last observed branch / HEAD:** `main` (ahead of `origin/main`; pushes NOT authorized).
- **Last verified checks (orchestrator-run, command → result):** PLAN.md SHA-256 matches ledger (`7f231f3a…0c45e5`). M2 suite: `uv run pytest -o addopts="" -q` → **exit 0, 149 passed** (addopts `-q` + CLI `-q` = `-qq` hides the footer; use `-o addopts=""`). `ruff check` / `ruff format --check` (17 files) / `mypy` → 0. `git diff --check` → 0. `docker build -t shoko-sonarr-linker:m2 .` → 0; container `--help` 0, offline `validate` 0. M1 gate matrix re-verified at boundary: `--help` 0; `validate --offline config.example.yaml` 0 (dummy env keys); `once --apply config.example.yaml` → **3 REFUSED** with and without `SHOKO_LINKER_ALLOW_APPLY=1`; container smoke 0/0/3.
- **Open P0/P1 or gate-blocking P2 findings:** **None open.** All M1+M2 review findings fixed and test-verified (see ledger rows + blockers table). Adjudication recorded: rename = churn, size change = rebaseline (M2 ledger).
- **Deferred P3s (reason):** `normalize_posix("//x")` keeps double-slash root (cosmetic; containment correct). `is_within_root` is lexical — filesystem symlinks inside allowlisted roots are out of lexical scope by design; **M2/M3 briefs must handle symlink resolution explicitly** (reviewer note, not an M1 defect).
- **Current blockers / assumptions:** Production Sonarr/Shoko versions, credentials, mounts, safety settings, mapping coverage, and GitHub package permissions remain unverified (EXTERNAL/UNVERIFIED). Prior harness tooling outage did not recur this session; all checks ran.
- **Offline vs live evidence:** All M1 gate items are offline evidence (mock HTTP, local Docker). Live Sonarr/Shoko, Plex, GHCR, Unraid checks are `EXTERNAL/UNVERIFIED`.
- **Proposed changes to frozen plan:** None.

## Milestone ledger

*Only the orchestrator changes statuses or enters acceptance evidence. Never promote a milestone to PASS based on a subagent claim alone; inspect code/diff and actual test output. Each row refers to the acceptance gate under the same ID in the immutable PLAN.md.*

| Milestone | Status | Evidence (commands/results, review, commit) / blocker |
| --- | --- | --- |
| M0 | **PASS** (offline gate) | `docs/compatibility.md` @ `d0c9196`. Sonarr v3 + Shoko v5.3.3 (blob-SHA-verified tag `89246118…`) + Anime-Lists contracts; orchestrator re-verified every load-bearing claim against raw sources (README quotes verbatim; ManualLinkJob chain; `apikey` auth; PascalCase naming; `ListResult{Total,List}`; ETag-only conditional fetch — 304 measured; license null → no bundling; AoT S04E26→AniDB 16177 ep10 from live XML). Plan-nuance recorded: `RelocateOnImport` IS serialized (Newtonsoft) — read `RenameOnImport`/`MoveOnImport` directly. Live checks remain EXTERNAL/UNVERIFIED (§5 of doc). |
| M1 | **PASS** (offline gate; first-buildable boundary `aa6b85e`) | Boundary commit recorded at next checkpoint update (rides M2 commit). Foundation package (`src/shoko_linker/` 8 modules + `py.typed`), 7 test modules, example config, Dockerfile, `.dockerignore`, Compose example, basic CI, README. Evidence (orchestrator-observed): 111 tests passed; ruff/format/mypy exit 0; CLI exit matrix (help 0, validate 0, `once --apply` 3 REFUSED both gate combos); `docker build` exit 0; container smoke 0/0/3. Two prior P1s (traceback redaction, urlBase join) fixed + tested. Independent read-only adversarial review: no P0/P1; 2 P2 + 6 P3 findings fixed/tested above. M8 publish workflow intentionally NOT here (CI is lint/test/build only, `contents: read`). Live/production items EXTERNAL/UNVERIFIED. |
| M2 | **PASS** (offline gate) | `src/shoko_linker/sonarr.py` + `tests/test_sonarr.py` (37 cases). Gate evidence (orchestrator-observed): 149 tests passed; ruff/format/mypy 0; git diff --check 0; `docker build` 0 + container smoke 0/0. Excluded series → zero secondary requests (request-log asserted); `/anime` never admits `/anime-old` (case-sensitive, Windows-style roots excluded); per-file episode groups complete (S01E01-E02, season 0, unlinked files flagged); malformed fields/collisions/transport failures → `complete=False`, `auto_apply_eligible=False`. Independent adversarial review: no P0/P1; 2 P2 fixed — (1) unparseable `episodeFileId` now taints the whole series file set (partial groups never indexed), (2) rename-churn adjudication: file **size** change = replacement → rebaseline; same-size rename = reported path-drift churn; series **root** change → rebaseline; title-only edits = churn; added/removed IDs = churn. P3s fixed: positive-ID/number validation, duplicate episode-ID detection, deterministic ordering (series by ID, rejects keyed by path, rootfolder warnings sorted), skipped doomed file fetch. Reviewer also caught `build_baseline` storing the allowlist root instead of the series' own `rootFolderPath` (masked series moves) — fixed + regression-tested. Live Sonarr checks EXTERNAL/UNVERIFIED. |
| M3 | TODO | — |
| M4 | TODO | — |
| M5 | TODO | — |
| M6 | TODO | — |
| M7 | TODO | — |
| M8 | TODO | — |
| M9 | TODO | — |

**Milestone states:** `TODO`, `ACTIVE`, `PASS`, `BLOCKED`; a milestone can `PASS` its offline gate while explicitly labeling external/live validation `EXTERNAL/UNVERIFIED`. A blocked offline gate is never a pass.

## Latest implementation / review checkpoint

- **Slice / scoped assignment:** M2 — read-only Sonarr v3 inventory + eligibility filtering + baseline identity rules (delegated implementer brief, fully specified in-brief; implementer reported 129 tests; orchestrator re-observed green).
- **Files changed:** `src/shoko_linker/sonarr.py` (new), `tests/test_sonarr.py` (new; 37 cases). No existing modules modified.
- **Implementation summary:** Series fetched once, filtered by exact component-aware containment in `sonarr.allowed_roots` (never `seriesType`); only eligible series fetch episodes/files. Files indexed by `(series_id, episode_file_id)` with normalized path + byte size + complete episode group (season 0, multi-episode files; unlinked files flagged). Malformed records/fields, collisions, cross-series references, transport failures → recorded reasons; any reason makes the snapshot incomplete and `auto_apply_eligible=False`. Rootfolder check = warnings only, never expands scope. `build_baseline`/`compare_baseline`: root change / season-episode renumber / file size change → `requires_rebaseline` (source-restore signal); renames (same size, new path), title edits, added/removed IDs → churn details. All I/O via `http.resolve_and_send` relative paths (307 urlBase walk tested).
- **Targeted checks and exact results (orchestrator-run):** `uv run pytest -o addopts="" -q` → 0 (149 passed); `ruff check` → 0; `ruff format --check` → 0 (17 files); `mypy` → 0; `git diff --check` → 0; `docker build` → 0; container `--help` 0, offline `validate` 0.
- **Independent read-only review:** Adversarial subagent (sequential, read-only): 0 P0/P1; probes confirmed core invariants (exclusion zero-requests, fail-closed parsing, cross-series leak detection, 7,000-file perf 0.12 s). Findings and dispositions in ledger row M2 + blockers table. Reviewer flagged the `build_baseline` root-field defect — fixed with regression test.
- **Fixes and retests:** Post-fix re-run: 149 passed; ruff/format/mypy/diffcheck 0; Docker rebuild + smoke 0/0/0.
- **Decision:** M2 accepted at the offline gate.
- **Commit SHA:** created with this checkpoint (SHA recorded at next ledger touch, riding the M3 commit).
- **Next task brief (self-contained for subagents):** M3-a — Shoko discovery: paginated unrecognized files + import folders (stability/complete pagination, changing totals, duplicate/missing IDs), multi-location ownership (all locations within approved DropFolderType.None Sonarr-owned paths, same canonical Sonarr file identity), component-aware Sonarr↔Shoko Docker path translation, exact path+size match, identity stability across two spaced observations, linear index reconciliation. Symlink/realpath obligation from M1 review must be honored. Zero writes.

## Blockers, deferred findings, and plan change proposals

| Type / severity | Milestone | Reproducer or justification | Resolution / next action |
| --- | --- | --- | --- |
| P2 (resolved) | M2 | `sonarr.py:333` + `793-800`: episode with present-but-unparseable `episodeFileId` (e.g. `"30"`) → no taint attributed, file indexed with provably partial group (reproduced by reviewer probe). Auto-apply stayed safe via snapshot-incomplete, invariant letter violated. | Resolved: unparseable file reference sets series-level `file_reference_tainted`; `index_series_files` refuses all files in that series (`MALFORMED_EPISODE_DATA`). Regression test: no files indexed, snapshot incomplete. |
| P2 (adjudicated) | M2 | `compare_baseline` flagged any title/path/size change → Sonarr renames chronically block auto-apply (PLAN treats renames as churn: "distinguish new file identity from renamed/moved/replaced records"). | Adjudicated (fail-safe split): file **size** change = replacement → requires rebaseline; same-size rename = reported path-drift churn; series **root** change → rebaseline; title-only edits + added/removed IDs = churn. Documented in module docstring + tests. No spec change to PLAN needed. |
| P2 (resolved) | M2 | Reviewer probe: `build_baseline` stored the matched allowlist root, masking series root movement in `compare_baseline`. | Resolved: identity now stores the series' own `rootFolderPath`; regression test (root change → rebaseline). |
| P3 (resolved) | M2 | Negative/zero IDs, bool ids, float ids, non-positive `episodeNumber` accepted into index/baseline keys; collision reject order followed API order; eligible/excluded order = API enumeration order; duplicate episode IDs undetected; doomed episodefile GET after episode failure. | Resolved: positive-ID/number validation, dup-episode-id → `INCONSISTENT_ENUMERATION`, deterministic sorting (series by ID, reject key includes path, rootfolder warnings sorted), fetch skip on episode failure; parametrized oddity tests added. |
| P3 (deferred) | M1→M2 | `normalize_posix("//x")` keeps the double-slash root form. Cosmetic; containment/mapping correct. | Defer (no functional impact); normalize opportunistically in M3 path work. |
| P3→obligation (deferred) | M2/M3 | `is_within_root` is lexical by design: symlinks inside an allowlisted root can point outside it. Not an M1 defect. | M2/M3 briefs must resolve/verify real paths; PLAN M3 symlink uncertainty gate item covers it. |
| P3 (resolved) | M1-b | Example Compose ran stub `daemon` (exit 4) under `restart: unless-stopped` → crash loop. | Resolved: prominent note (drop restart / use real daemon image when it ships). |

## External validation tracker

*Unavailable live evidence is not a test pass. Do not request write credentials merely to complete offline gates.*

| External requirement | Status | Evidence / remaining action |
| --- | --- | --- |
| Deployed Sonarr/Shoko version and live API response compatibility | UNVERIFIED | Inspect in approved live read-only preflight |
| Shoko Sonarr media effectively read-only; file-quality filter disabled; plugin/renamer/MyList review | UNVERIFIED | Operator-only filesystem/settings safety inspection |
| Live apply canary, Shoko queue settling, media integrity, Plex result | UNVERIFIED | Requires explicit user authorization and backup |
| GHCR per-commit push and `latest` behavior | UNVERIFIED | Requires authorized push and observed GitHub runner |
| Unraid Docker Compose and persistent state | UNVERIFIED | Requires authorized deployment |

## Checkpoint maintenance rules (orchestrator only)

After each bounded slice and review/fix cycle, update this document **once**, replacing the current checkpoint and relevant milestone row with concise verified evidence. Keep only the most recent detailed slice; record durable milestone decisions in the ledger. Record changed paths, exact checks/statuses, P0–P3 issues and dispositions, last commit, blockers, and the next concrete action. Before intentional compaction, record the current branch/HEAD, incomplete edits, real check outcomes, outstanding review findings and **one concrete next action**. On resumption or after automatic compaction, the **main orchestrator** must follow the document rehydration order in [ORCHESTRATION.md](ORCHESTRATION.md), verify the immutable plan digest and repository state, then continue at the earliest *actionable* unpassed milestone without relying solely on a compacted summary.

Do not paste large tool logs or full subagent transcripts. Do not include secrets, API keys, personal server credentials, or unredacted sensitive Shoko settings in this file. Treat a frozen-plan change as a proposal requiring user approval—not as an edit to PLAN.md.
