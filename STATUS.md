# Execution Status — shoko-sonarr-linker

**Owner:** Main project orchestrator only (independent of coding-agent harness). Delegated implementation/review agents receive scoped task briefs and report findings to the orchestrator; they must not read or edit this file or [PLAN.md](PLAN.md).

**Frozen plan SHA-256:** `7f231f3ad6d63a61e920ddb981bd18750a613abb70db23a2744890675c0c45e5`. If the digest changes unexpectedly, stop treating the edited file as approved; record the discrepancy here and request user authorization before replacing the plan.

**Authority:** [PROJECT.md](PROJECT.md) = requirements; [AGENTS.md](AGENTS.md) = general agent procedures; [ORCHESTRATION.md](ORCHESTRATION.md) = harness-independent main-orchestrator workflow; **[PLAN.md](PLAN.md) = frozen milestone sequence and acceptance gates**; **STATUS.md = the only mutable execution/progress record**. Never modify PLAN.md to record progress. Proposed plan changes require user approval before replacing the frozen plan.

## Current checkpoint

- **Goal status:** `ACTIVE` (orchestration goal; plan persisted in goal tool, phases M1→M9).
- **Active milestone / slice:** **M3 PASS (offline gate)** — Shoko discovery/ownership/matching implemented, adversarially reviewed (no P0/P1; 1 P2 fixed), 221 tests green, Docker buildable. M2 **PASS** (`bb7356b`). M1 **PASS** (boundary `aa6b85e`). M0 **PASS** (`d0c9196`).
- **Next concrete action:** Record M3 commit SHA at next ledger touch; prepare M4 brief (Anime-Lists runtime fetch/refresh + AniDB→TVDB inversion + atomic activation; real AoT S04E26 check against live XML; synthetic fixtures per gate).
- **Last accepted local commit:** M3 commit (created with this checkpoint; SHA at next touch); M2 `bb7356b`; M1 boundary `aa6b85e`.
- **Working tree / uncommitted work:** M3 slice: `src/shoko_linker/shoko.py`, `tests/test_shoko.py` (new, all checks green) + this STATUS.md update; committed as the M3 commit created with this checkpoint.
- **Last observed branch / HEAD:** `main` (ahead of `origin/main`; pushes NOT authorized).
- **Last verified checks (orchestrator-run, command → result):** PLAN.md SHA-256 matches ledger (`7f231f3a…0c45e5`). M3 suite: `uv run pytest -o addopts="" -q` → **exit 0, 221 passed**. `ruff check` / `ruff format --check` (19 files) / `mypy` → 0. `git diff --check` → 0. `docker build -t shoko-sonarr-linker:m3 .` → 0; container `--help` 0, offline `validate` 0. Prior milestones' gate matrices remain green in-suite (149 → 221 includes all of them).
- **Open P0/P1 or gate-blocking P2 findings:** **None open.** All M1–M3 review findings fixed and test-verified (see ledger rows + blockers table).
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
| M2 | **PASS** (offline gate, commit `bb7356b`) | `src/shoko_linker/sonarr.py` + `tests/test_sonarr.py` (37 cases). Gate evidence (orchestrator-observed): 149 tests passed; ruff/format/mypy 0; git diff --check 0; `docker build` 0 + container smoke 0/0. Excluded series → zero secondary requests (request-log asserted); `/anime` never admits `/anime-old` (case-sensitive, Windows-style roots excluded); per-file episode groups complete (S01E01-E02, season 0, unlinked files flagged); malformed fields/collisions/transport failures → `complete=False`, `auto_apply_eligible=False`. Independent adversarial review: no P0/P1; 2 P2 fixed — (1) unparseable `episodeFileId` now taints the whole series file set (partial groups never indexed), (2) rename-churn adjudication: file **size** change = replacement → rebaseline; same-size rename = reported path-drift churn; series **root** change → rebaseline; title-only edits = churn; added/removed IDs = churn. P3s fixed: positive-ID/number validation, duplicate episode-ID detection, deterministic ordering (series by ID, rejects keyed by path, rootfolder warnings sorted), skipped doomed file fetch. Reviewer also caught `build_baseline` storing the allowlist root instead of the series' own `rootFolderPath` (masked series moves) — fixed + regression-tested. Live Sonarr checks EXTERNAL/UNVERIFIED. |
| M3 | **PASS** (offline gate; commit at next ledger touch) | `src/shoko_linker/shoko.py` + `tests/test_shoko.py` (72 cases). Gate evidence (orchestrator-observed): 221 tests; ruff/format/mypy/diffcheck 0; `docker build` 0 + container smoke 0/0. All gate items asserted: changing `Total`/duplicate IDs/short pages/transport/envelope/cap → snapshot incomplete; `match_shoko_files` refuses incomplete snapshots (Shoko-side **and** Sonarr-incomplete); include-not-honored (`AbsolutePath`/`SeriesIDs` absent) → incomplete, never guessed; all-location ownership (accessibility, known folder, `DropFolderType=="None"`, folder+relative == absolute cross-check, component-aware roots, case-sensitive; any failing location drops whole file); reverse-mapping ties → `ambiguous_mapping`; exact full-path+size correspondence (basename never matches; divergence/no-size/ambiguous-size; two Shoko files → both match + duplicate warning); identity primitives (size/hash/locations add/remove/change/stable); 7,000 files in 0.63 s with 8 GET requests, zero writes proved by request log. Independent adversarial review (23 probes): **no P0/P1**; P2 fixed — identity now pinned to canonical Shoko path space (`sonarr_paths` context-only) so ownership flips cannot fake location drift; P3s fixed — page-overshoot contract violation flag, dead-net annotation, M6 docstring note; 9 new gap tests (case-sensitivity, slash forms, null absolute path, composite drop-type string, negative pageSize, overshoot, multi-episode passthrough, ownership-flip stability). Symlink lexical-scope obligation carried to M5/M6 preflight (documented). Live Shoko response shapes EXTERNAL/UNVERIFIED. |
| M4 | TODO | — |
| M5 | TODO | — |
| M6 | TODO | — |
| M7 | TODO | — |
| M8 | TODO | — |
| M9 | TODO | — |

**Milestone states:** `TODO`, `ACTIVE`, `PASS`, `BLOCKED`; a milestone can `PASS` its offline gate while explicitly labeling external/live validation `EXTERNAL/UNVERIFIED`. A blocked offline gate is never a pass.

## Latest implementation / review checkpoint

- **Slice / scoped assignment:** M3 — read-only Shoko v5.3.3 discovery (paginated unrecognized listing + import folders), strict all-location ownership, Shoko→Sonarr path translation, exact path+size correspondence, identity primitives (delegated implementer brief; 213 reported, orchestrator re-observed, then review + fixes to 221).
- **Files changed:** `src/shoko_linker/shoko.py` (new, ~1270 lines), `tests/test_shoko.py` (new, 72 cases). No existing modules modified.
- **Implementation summary:** GETs only via `resolve_and_send`; `{Total,List}` exact envelope; total-change/duplicate-ID/short-page/cap/transport/malformed → verdict reasons → `match_shoko_files` refuses. `include_only=Unrecognized` semantics honored (XRefs ⇒ not candidate; IsVariation/IsIgnored/Imported=null/no-locations categorized, snapshot stays complete). Ownership: every location accessible + known folder + `DropFolderType=="None"` + normalized folder+relative == AbsolutePath + within `shoko.allowed_roots`; any failure drops the whole file. Reverse mapping most-specific with tie refusal; correspondence = exact normalized full path + byte size; multi-location divergence / partial size / ambiguous candidates refused; Shoko duplicates warned. `ShokoFileIdentity`/`compare_identity` canonical Shoko-path space for M5/M6 two-observation checks. Deterministic sorting throughout.
- **Targeted checks and exact results (orchestrator-run):** `uv run pytest -o addopts="" -q` → 0 (221 passed); ruff/format(19 files)/mypy → 0; `git diff --check` → 0; `docker build` → 0; container `--help` 0, `validate` 0.
- **Independent read-only review:** Adversarial subagent (23 MockTransport probes): zero writes structurally provable (single hardcoded-GET call site); all injected instabilities refused; contract fidelity vs docs §2 high. 0 P0/P1; P2: identity space mixing (ownership flip faked `LOCATIONS_CHANGED`) — fixed with canonical Shoko-path `paths` + context-only `sonarr_paths` + regression tests (ownership-flip stability, reconcile-level). P3s: overshoot flag (added, tested), dead safety net (annotated defensive), identities-dropped-on-refused (documented as design), reason-naming cosmetic (deferred, test-pinned).
- **Fixes and retests:** Post-fix: 221 passed; ruff/format/mypy/diffcheck 0; Docker rebuild + smoke 0/0/0.
- **Decision:** M3 accepted at the offline gate.
- **Commit SHA:** created with this checkpoint (SHA recorded at next ledger touch).
- **Next task brief (self-contained for subagents):** M4-a — Anime-Lists mapping module: runtime download/conditional refresh (ETag/If-None-Match only, per docs §3), staged parse + structural validation + atomic activation, last-known-good retention, size/time limits, defusedxml, refresh scheduler state, inversion engine (defaulttvdbseason/episodeoffset/mapping-lists/explicit pairs/`;a-b+c;`/`0`-exclusion/absolute `a`/T-O ranges, explicit > range-offset > entry-offset precedence, split-series via tvdbid+season, season-0 ambiguity fail-closed), overrides file, provenance digests, effective-target-set comparison. Gate fixtures per PLAN (frozen synthetic XMLs; AoT real-data check).

## Blockers, deferred findings, and plan change proposals

| Type / severity | Milestone | Reproducer or justification | Resolution / next action |
| --- | --- | --- | --- |
| P2 (resolved) | M3 | `shoko.py:431–455/537/540`: `ShokoFileIdentity.paths` stored translated Sonarr paths when translation succeeded, raw Shoko paths otherwise → ownership-status flip on an unchanged file reported fake `LOCATIONS_CHANGED` (reviewer probe P8); M5/M6 two-observation gates would misfire. | Resolved: `paths` pinned to canonical Shoko-side space (the only compared location field); translated paths moved to non-compared `sonarr_paths`; regression tests: translated/raw observations compare stable, and end-to-end ownership flip (folder becomes drop-type Source) stays identity-stable while the file is refused. |
| P3 (resolved) | M3 | Server overshoot (rows beyond reported `Total`, probe P7) accepted as complete. | Resolved: `SnapshotReason.PAGE_OVERSHOOT` → incomplete; test added. |
| P3 (resolved) | M3 | Post-loop under-fill safety net unreachable (all exits already record reasons); identities dropped on refused passes. | Resolved: net annotated as defensive; refused-pass identity behavior documented as design (drift only on complete passes; M6 note in `ShokoMatchResult` docstring). |
| P3 (deferred) | M3 | Known import folder with unusable path reports reason name `UNKNOWN_IMPORT_FOLDER` (test-pinned, descriptive detail). | Cosmetic naming; defer. |
| P3 (deferred) | M1→M2 | `normalize_posix("//x")` keeps the double-slash root form. Cosmetic; containment/mapping correct (M3 slash-form cross-check test passes). | Defer (no functional impact). |
| P3→obligation (deferred) | M2/M3 | `is_within_root` is lexical by design: symlinks inside an allowlisted root can point outside it. Not an M1 defect. | M5/M6 preflight must resolve/verify real paths; PLAN M3/M5 gate items cover live symlink checks (EXTERNAL/UNVERIFIED). |
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
