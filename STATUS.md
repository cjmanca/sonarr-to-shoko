# Execution Status — shoko-sonarr-linker

**Owner:** Main project orchestrator only (independent of coding-agent harness). Delegated implementation/review agents receive scoped task briefs and report findings to the orchestrator; they must not read or edit this file or [PLAN.md](PLAN.md).

**Frozen plan SHA-256:** `7f231f3ad6d63a61e920ddb981bd18750a613abb70db23a2744890675c0c45e5`. If the digest changes unexpectedly, stop treating the edited file as approved; record the discrepancy here and request user authorization before replacing the plan.

**Authority:** [PROJECT.md](PROJECT.md) = requirements; [AGENTS.md](AGENTS.md) = general agent procedures; [ORCHESTRATION.md](ORCHESTRATION.md) = harness-independent main-orchestrator workflow; **[PLAN.md](PLAN.md) = frozen milestone sequence and acceptance gates**; **STATUS.md = the only mutable execution/progress record**. Never modify PLAN.md to record progress. Proposed plan changes require user approval before replacing the frozen plan.

## Current checkpoint

- **Goal status:** `ACTIVE` (orchestration goal; plan persisted in goal tool, phases M1→M9).
- **Active milestone / slice:** **M1 PASS (offline gate)** — first-buildable boundary commit created on this checkpoint (SHA recorded at next ledger touch, riding the M2 commit). M0 **PASS** (offline gate, `d0c9196`).
- **Next concrete action:** Prepare the M2 brief (Sonarr eligibility filter + file/episode inventory + baseline cohort rules, offline fixture gate) and delegate to an implementer; M1 P3 deferrals listed below must be honored in M2/M3 briefs.
- **Last accepted local commit:** M1 boundary commit (this checkpoint); prior accepted `d0c9196` (M0 docs).
- **Working tree / uncommitted work:** STATUS.md checkpoint update only (application tree fully committed in the boundary commit).
- **Last observed branch / HEAD:** `main` (ahead of `origin/main`; pushes NOT authorized).
- **Last verified checks (orchestrator-run, command → result):** PLAN.md SHA-256 matches ledger. `uv run pytest -o addopts="" -q` → **exit 0, 111 passed** (addopts `-q` + CLI `-q` = `-qq` suppresses the summary footer; use `-o addopts=""` to see counts). `uv run ruff check .` → 0; `uv run ruff format --check .` → 0 (15 files); `uv run mypy` → 0 (15 source files); `git diff --check` → 0. CLI smoke: `--help` 0; `validate --offline config.example.yaml` 0 (dummy env keys); `once --apply config.example.yaml` → **3 REFUSED** both with and without `SHOKO_LINKER_ALLOW_APPLY=1` (config gate false). `docker build -t shoko-sonarr-linker:m1-boundary .` → **exit 0**. Container smoke: `--help` 0, offline validate 0, `once --apply` → 3 REFUSED (gate text observed).
- **Independent review (M1, read-only adversarial subagent):** **No P0/P1.** Findings triaged below; reviewer independently re-ran suite, CLI matrix, `docker build`, container smoke (all green) and verified non-root uid/gid, writable `/data`, `/app` root-owned, no source tree in image layers.
- **Open P0/P1 or gate-blocking P2 findings:** **None open.** P2s fixed + regression-tested: (1) `_number` accepted NaN/inf and crashed on >float ints → finite/range checks (`must be a finite number` / `out of range`); (2) non-UTF-8 config and huge-int values raised raw tracebacks (exit 1) → `ConfigError` exit 2 paths. P3s closed: http docstring now matches POST replay semantics (301/302/307/308 same-origin walk, one delivery per target; pinned by parametrized test), `resolve_and_send` refuses foreign-origin absolute URLs before any request, `make_client` warns (length only) when a key is below the 8-char redaction floor, dead CLI handler-lookup branch removed, tests strengthened (`retries=0` transport pin, Sonarr key in `repr` check, short-key warning asserts value absent), compose example notes daemon-stub restart loop.
- **Deferred P3s (reason):** `normalize_posix("//x")` keeps double-slash root (cosmetic; containment correct). `is_within_root` is lexical — filesystem symlinks inside allowlisted roots are out of lexical scope by design; **M2/M3 briefs must handle symlink resolution explicitly** (reviewer note, not an M1 defect).
- **Current blockers / assumptions:** Production Sonarr/Shoko versions, credentials, mounts, safety settings, mapping coverage, and GitHub package permissions remain unverified (EXTERNAL/UNVERIFIED). Prior harness tooling outage did not recur this session; all checks ran.
- **Offline vs live evidence:** All M1 gate items are offline evidence (mock HTTP, local Docker). Live Sonarr/Shoko, Plex, GHCR, Unraid checks are `EXTERNAL/UNVERIFIED`.
- **Proposed changes to frozen plan:** None.

## Milestone ledger

*Only the orchestrator changes statuses or enters acceptance evidence. Never promote a milestone to PASS based on a subagent claim alone; inspect code/diff and actual test output. Each row refers to the acceptance gate under the same ID in the immutable PLAN.md.*

| Milestone | Status | Evidence (commands/results, review, commit) / blocker |
| --- | --- | --- |
| M0 | **PASS** (offline gate) | `docs/compatibility.md` @ `d0c9196`. Sonarr v3 + Shoko v5.3.3 (blob-SHA-verified tag `89246118…`) + Anime-Lists contracts; orchestrator re-verified every load-bearing claim against raw sources (README quotes verbatim; ManualLinkJob chain; `apikey` auth; PascalCase naming; `ListResult{Total,List}`; ETag-only conditional fetch — 304 measured; license null → no bundling; AoT S04E26→AniDB 16177 ep10 from live XML). Plan-nuance recorded: `RelocateOnImport` IS serialized (Newtonsoft) — read `RenameOnImport`/`MoveOnImport` directly. Live checks remain EXTERNAL/UNVERIFIED (§5 of doc). |
| M1 | **PASS** (offline gate; first-buildable boundary) | Boundary commit recorded at next checkpoint update (rides M2 commit). Foundation package (`src/shoko_linker/` 8 modules + `py.typed`), 7 test modules, example config, Dockerfile, `.dockerignore`, Compose example, basic CI, README. Evidence (orchestrator-observed): 111 tests passed; ruff/format/mypy exit 0; CLI exit matrix (help 0, validate 0, `once --apply` 3 REFUSED both gate combos); `docker build` exit 0; container smoke 0/0/3. Two prior P1s (traceback redaction, urlBase join) fixed + tested. Independent read-only adversarial review: no P0/P1; 2 P2 + 6 P3 findings fixed/tested above. M8 publish workflow intentionally NOT here (CI is lint/test/build only, `contents: read`). Live/production items EXTERNAL/UNVERIFIED. |
| M2 | TODO | — |
| M3 | TODO | — |
| M4 | TODO | — |
| M5 | TODO | — |
| M6 | TODO | — |
| M7 | TODO | — |
| M8 | TODO | — |
| M9 | TODO | — |

**Milestone states:** `TODO`, `ACTIVE`, `PASS`, `BLOCKED`; a milestone can `PASS` its offline gate while explicitly labeling external/live validation `EXTERNAL/UNVERIFIED`. A blocked offline gate is never a pass.

## Latest implementation / review checkpoint

- **Slice / scoped assignment:** M1 close-out (orchestrator): re-verify recorded evidence, independent adversarial review, fix findings, retest, Docker verification, boundary commit.
- **Files changed (this slice):** `src/shoko_linker/config.py` (finite/range numeric validation, non-UTF-8 file → ConfigError), `src/shoko_linker/http.py` (accurate POST-redirect docstring, hop-0 origin binding, short-key redaction warning), `src/shoko_linker/logging_setup.py` (`register_secret` returns registration result; public `MIN_SECRET_LENGTH`), `src/shoko_linker/cli.py` (dead branch removed), `tests/test_config.py` (+7 cases), `tests/test_http.py` (+6 cases), `tests/test_logging.py` (registration-result pin), `README.md` (warning note), `compose.example.yaml` (stub restart-loop note).
- **Implementation summary:** Fail-closed numeric and file-encoding validation; redirect walk documented and tested for exactly one delivery per target across 301/302/307/308 with 303+POST refusal; foreign-origin requests refused before transport; sub-floor key registration now warns without emitting the value; example Compose warns that the daemon is a stub in this build.
- **Targeted checks and exact results:** See "Last verified checks" above (111 passed; lint/format/type/CLI/Docker/container all exit-0, gate refusal exit 3).
- **Independent read-only review:** Full-diff adversarial pass (read-only subagent, sequential): commands independently run, invariants verified clean (gate composition, secret handling, path containment, redirect policy, Docker posture, CI least privilege + rc capture). Findings: 0 P0/P1, 2 P2, 6 P3 — all fixed this slice except two documented deferrals.
- **Fixes and retests:** Full suite re-run after fixes → green (above), then Docker rebuild + container smoke re-run → green.
- **Decision:** M1 accepted at the offline gate; boundary commit created.
- **Commit SHA:** Boundary commit = created with this checkpoint (SHA recorded at next ledger touch).
- **Next task brief (self-contained for subagents):** M2-a — Sonarr v3 inventory module: fetch `/api/v3/series`, filter by exact configured anime `rootFolderPath` before any per-series episode/file requests; index episode files by series/file ID + exact path + byte size + all episodes sharing `episodeFileId`; season 0 and multi-episode files; component-aware root containment; baseline cohort rules (complete snapshot, ID-reuse/source-restore detection stops auto-apply); offline MockTransport fixtures. M2 brief must include the symlink-handling obligation noted above.

## Blockers, deferred findings, and plan change proposals

| Type / severity | Milestone | Reproducer or justification | Resolution / next action |
| --- | --- | --- | --- |
| P2 (resolved) | M1 | `config.py:_number` accepted `.nan`/`.inf` (`interval_seconds: .nan` → accepted; daemon `time.sleep(nan)` crash) and raised uncaught `OverflowError` on `10**400`. | Resolved: finiteness + range `ConfigError`s; parametrized regression tests; suite green. |
| P2 (resolved) | M1 | Non-UTF-8 config file → uncaught `UnicodeDecodeError` traceback exit 1 (documented contract: exit 2, never raises for expected failures). | Resolved: `load_settings` catches decode → `ConfigError`; regression test; suite green. |
| P3 (resolved) | M1 | `http.py` docstring claimed POST replay only for 307/308; implementation (spec-legal) also walks 301/302. No test pinned statuses. | Resolved: accurate docstring + parametrized 301/302/307/308 walk test (one delivery per target). |
| P3 (resolved) | M1 | `resolve_and_send` accepted foreign absolute URLs → auth header to any host (no M1 caller; future-engine risk). | Resolved: hop-0 origin binding refuses foreign origins before any request; test added. |
| P3 (resolved) | M1 | API keys shorter than 8 chars silently unregistered (unredactable) — e.g. `SHOKO_API_KEY=abc`. | Resolved: `make_client` logs length-only warning; `register_secret` returns bool; tests pin warning text and value absence. |
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
