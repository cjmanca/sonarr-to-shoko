# User Experience Redesign Implementation Plan

> **For agentic workers:** Follow this plan task-by-task inline on `main`, without a worktree. Preserve `PLAN.md` unchanged. Follow `AGENTS.md` and record evidence in `STATUS.md` at each accepted gate.

**Goal:** Deliver the six milestones in `PLAN.md`: a safe, set-and-forget Sonarr-to-Shoko service for ordinary Docker/Unraid users, while retaining the established guarded engine, SQLite history, and advanced CLI.

**Architecture:** Add a consumer-facing environment/bootstrap and onboarding layer around the existing `Settings`, discovery, mapping, state, and link submission modules. Automatic operation gets a dedicated durable authorization/queue path; it must not synthesize legacy attestations or operator campaign approvals. A small authenticated dashboard provides setup, status, and controls, while the existing advanced CLI remains available.

**Tech Stack:** Python 3.12, existing `httpx`/SQLite/PyYAML code, standard-library HTTP server and password hashing, pytest with mocked HTTP, Docker/Compose.

---

## Repository boundaries and file map

| Path | Responsibility in this redesign |
| --- | --- |
| `PROJECT.md` | Updated product behavior, onboarding, safety, and dashboard acceptance criteria. |
| `src/shoko_linker/bootstrap.py` (new) | Parse the four required service environment settings without storing key values in settings/report objects; locate persistent `/data` state and optional legacy YAML. |
| `src/shoko_linker/config.py` | Adapt bootstrap input/discovered scope into the existing typed `Settings`; preserve strict validation for legacy YAML. |
| `src/shoko_linker/setup.py` (new) | Read-only setup discovery, evidence-backed scope/path mapping, and compatibility/capability results. |
| `src/shoko_linker/sonarr.py` | Parse verified Sonarr `seriesType`, root folders, status/version as needed; fetch child inventory only after scope is explicit. |
| `src/shoko_linker/shoko.py` | Reuse strict import-folder/file discovery; expose read-only compatibility/safety facts only from verified supported API shapes. |
| `src/shoko_linker/onboarding.py` (new) | Persist selected scope, one-time physical-safety/automatic-linking consent, readiness, pause state, and invalidation identity. |
| `src/shoko_linker/state.py` | Add forward-only SQLite migration and transactional methods for onboarding, authentication metadata, and adaptive rate state; do not rewrite historical rows. |
| `src/shoko_linker/daemon.py` | Add an explicitly authorized automatic backlog+arrival policy and adaptive bounded batch selection; preserve preflight, immutable targets, sends, verification, and recovery. |
| `src/shoko_linker/rate_control.py` (new) | Deterministic persisted ramp-up/backoff decisions based on queue/jobs, verification, transport outcomes, and anomalies. |
| `src/shoko_linker/dashboard.py` (new) | Minimal server-rendered setup/login/status interface, authenticated and CSRF-protected Pause/Resume, no external frontend. |
| `src/shoko_linker/cli.py` | Keep every advanced command; add simple `service`, `status`, `pause`, and `resume` routes that do not require a YAML file. |
| `Dockerfile`, `compose.example.yaml`, `unraid.xml` (new) | Start the service by default; keep non-root and persistent `/data`; provide Unraid fields for URLs/keys and an optional private dashboard port; no media mount, socket, or privileged mode. |
| `README.md`, `docs/advanced.md` (new), `docs/deployment.md` | Consumer-first quick start, migration, practical Unraid example, and relocated operator detail. |
| `tests/test_bootstrap.py` (new) | Environment parsing, missing/invalid settings, no secret leakage, and legacy YAML selection. |
| `tests/test_setup.py` (new) | Anime filtering, root choice, path-evidence inference, API compatibility, and fail-closed ambiguous discovery. |
| `tests/test_onboarding.py` (new) | Safety inspection, one-time consent binding/invalidation, pause/resume, and additive migration. |
| `tests/test_rate_control.py` (new) | Persisted cautious ramp-up, capacity ceilings, recovery after restart, and immediate slowdown/stop signals. |
| `tests/test_dashboard.py` (new) | First-user setup, password hashing, authenticated controls, CSRF/refusal behavior, and safe status text. |
| `tests/test_service_e2e.py` (new) | Fresh-install MockTransport journey through setup, auto-link, verification, restart, and skip/refusal cases. |
| Existing tests under `tests/` | Extend consumer-facing behavior without reducing existing safety and compatibility coverage. |

Do not add a dependency or invent API fields. Consult `docs/compatibility.md`, the deployed/versioned Swagger or upstream source before implementing API reads. `GET /api/v3/Settings` is admin-only and includes secrets: if it is available, extract only named safety booleans and never log/store/serialize its raw payload; if it is unavailable, require the setup confirmation for the unverifiable facts. Do not call Shoko `PATCH /Settings`, queue mutation endpoints, or import/action endpoints.

## Execution rules

- Run tests before implementation for each behavior; retain the failing output as TDD evidence, then implement and rerun the focused test.
- Keep the existing advanced YAML and CLI behavior as compatibility paths. The Docker default `service` path is the new ordinary-user path.
- Automatic writes require durable explicit onboarding authorization bound to the selected endpoint, Shoko version, root scope, mappings, and safety facts. This is not a generated `attestation` or legacy operator campaign approval. The manual `once --apply` and campaign paths retain their existing gates.
- No media mounts, media-file writes, live Sonarr/Shoko writes, production apply, deployment, GHCR publication from local commands, or enabling real-service writes during this implementation.
- The first dashboard account is created in the first-visit wizard, as selected by the user. The setup wizard performs account creation only; safety/scope confirmation is a separate explicit action. All later dashboard controls require authentication. Do not publish a host port automatically; the Unraid template makes an operator-selected local port mapping explicit.
- Do not stop for routine milestone approval. Update `STATUS.md` after each verified gate with command output, local commit, branch/HEAD, open findings, and one next action.

## M0 — Establish the redesign requirements and execution checkpoint

### Task 0.1: Replace the old product description in `PROJECT.md`

**Files:** Modify `PROJECT.md`; update `STATUS.md`.

- [ ] Update the problem/goal and ordinary operating mode to environment-based automatic service behavior.
- [ ] Keep authoritative safety invariants: exact anime scope; complete path/size/file identity; no existing-link overwrite; no ambiguous mapping; no uncertain POST replay; no intentional media change; no settings writes; fail closed on unknown Shoko safety.
- [ ] Document that a user’s one-time setup confirmation explicitly authorizes automatic linking only for the selected scope, is bound to observed safety/version identity, and is invalidated on relevant changes. Existing approvals never widen into this authorization.
- [ ] Move campaigns, expiring leases, digest approvals, and low-level operator procedures to the advanced-tooling section; retain them for supported legacy workflows.
- [ ] Specify dashboard status/control behavior, first-user setup, authentication, and no automatically published Internet-facing port.
- [ ] Record the selected working `PLAN.md` SHA-256 and its supersession of the historical F34 digest in `STATUS.md`; keep `PLAN.md` unchanged. Record the explicit user `/goal` request as authority for using the current handoff.
- [ ] After verifying the selected hash, include the user-provided `PLAN.md` bytes unchanged in the M0 baseline commit so the new frozen plan version is durable and available with the implementation. Do not stage any later edits to `PLAN.md`.
- [ ] Review the product text against every safety and compatibility requirement in current `PLAN.md` and relevant existing `PROJECT.md` sections; reconcile contradictions explicitly.

**Gate:** `PROJECT.md` states the new product requirements without weakening PROJECT safety invariants; `PLAN.md` is unchanged; `STATUS.md` records the selected plan hash and next action.

## M1 — Zero-configuration startup

### Task 1.1: Parse connection-only environment settings

**Files:** Create `src/shoko_linker/bootstrap.py`, `tests/test_bootstrap.py`; modify `config.py`, `tests/test_config.py`.

- [ ] Add tests for `SONARR_URL`, `SONARR_API_KEY`, `SHOKO_URL`, and `SHOKO_API_KEY` as the only required service settings; reject missing/blank values, malformed URLs, and URLs with embedded credentials.
- [ ] Assert errors identify a variable/field without printing any key value; assert repr, JSON, logs, and status objects contain only secret variable names or redacted values.
- [ ] Parse URLs through the same validated URL policy as `config.py`; store API keys as `SecretRef` names and resolve their values only when creating the HTTP endpoints.
- [ ] Supply `/data/state.sqlite3` and mapping-cache defaults without requiring a config mount; treat optional advanced YAML as an explicit override, never merge partially validated roots/mappings into a widened scope.
- [ ] Run `uv run pytest -q tests/test_bootstrap.py tests/test_config.py` and retain the existing config tests unchanged in intent.

**Gate:** Four connection settings load without a YAML file; invalid/missing fields fail with safe, actionable messages; existing YAML loading remains compatible.

### Task 1.2: Add container-default service startup

**Files:** Modify `src/shoko_linker/cli.py`, `src/shoko_linker/__init__.py` only if needed, `Dockerfile`, `compose.example.yaml`; add focused CLI tests.

- [ ] Add a normal `service` command that reads bootstrap settings, starts the long-running service path, and routes expected setup/network failures to plain-language messages without tracebacks or secrets.
- [ ] Preserve explicit `daemon <config>` and all advanced handlers unchanged in behavior.
- [ ] Change the image default command to `service`; prove an explicit `CMD`/CLI command still overrides it.
- [ ] Keep the runtime non-root, `/data` persistent, and the image without media mounts, exposed host port, Docker socket, or privileged mode.
- [ ] Run the focused CLI tests and `uv run ruff check src/shoko_linker/bootstrap.py src/shoko_linker/cli.py tests/test_bootstrap.py`.

**Gate:** Container startup does not require a config YAML or subcommand; legacy command invocation still works; no remote mutation is enabled by missing setup state.

### Task 1.3: Supply an Unraid-friendly container template

**Files:** Create `unraid.xml`; modify `compose.example.yaml`, `tests/test_workflows.py` or add `tests/test_unraid_template.py`.

- [ ] Define image/name/repository, restart policy, non-root-compatible `/data` mapping, Sonarr/Shoko URLs, and masked API-key environment fields.
- [ ] Make the dashboard port a deliberate user mapping with no automatically exposed port; include no media-volume, Docker-socket, or privileged mapping.
- [ ] Validate XML parsing, every required field, secret environment names (not secret values), persistent state, and absence of unsafe mounts/privileges.
- [ ] Run the focused template/Compose checks.

**M1 gate:** A mocked fresh container with only service connection settings reaches setup without YAML; missing connections fail safely; default Docker command is `service`; existing advanced configuration/CLI tests pass; Unraid template is structurally valid.

## M2 — Automatic environment discovery

### Task 2.1: Add evidence-backed anime/root discovery

**Files:** Create `src/shoko_linker/setup.py`, `tests/test_setup.py`; modify `src/shoko_linker/sonarr.py`, `tests/test_sonarr.py`, `src/shoko_linker/mappings.py` only if a pure lookup helper is needed.

- [ ] Add failing fixtures with anime, live-action, missing-ID, and multiple-root Sonarr series. Parse `seriesType` from the verified v3 response; allow only positively identified anime for automatic scope.
- [ ] Discover Sonarr root folders and API status/version through verified v3 paths, preserving URL-base and header-only API-key rules.
- [ ] Discover Anime-Lists mappings from the existing mapping store; use verified series IDs, not titles, to confirm anime candidates. Do not use fuzzy name matching.
- [ ] If exactly one safe anime root is established, present it for confirmation as part of setup scope; when multiple roots or uncertain series remain, ask one plain-language selection and persist only the selected exact roots.
- [ ] Do not fetch episodes/files outside the selected anime roots. Missing root data, unavailable APIs, malformed entries, unsupported compatibility, or empty selection stays setup-blocked.
- [ ] Run `uv run pytest -q tests/test_setup.py tests/test_sonarr.py tests/test_mappings.py`.

### Task 2.2: Infer only unique Sonarr↔Shoko path mappings

**Files:** Modify `src/shoko_linker/setup.py`, `src/shoko_linker/paths.py`, `src/shoko_linker/shoko.py`; extend `tests/test_setup.py`, `tests/test_paths.py`, `tests/test_shoko.py`.

- [ ] Collect complete Sonarr eligible-file and Shoko unrecognized-file snapshots using existing strict parsers.
- [ ] Use only `DropFolderType=None` Shoko import folders that correspond to the selected Sonarr anime root; reject manual/source/destination/both folders.
- [ ] Infer prefix mappings only when normalized relative path structure and exact byte-size/file identity produce a unique consistent mapping. Reject ties, duplicate paths, conflicting prefixes, partial snapshots, and files outside selected roots.
- [ ] Feed only validated mappings into the existing component-boundary validator and matcher; no filename-only/basename/fuzzy fallback.
- [ ] If safe evidence is insufficient, persist a single setup question with candidates and reasons; do not silently expand roots or require routine Docker path translation.
- [ ] Run `uv run pytest -q tests/test_setup.py tests/test_paths.py tests/test_shoko.py tests/test_sonarr.py`.

### Task 2.3: Expose compatibility and setup readiness

**Files:** Modify `src/shoko_linker/setup.py`, `src/shoko_linker/reporting.py`; add/extend setup tests.

- [ ] Record verified Sonarr/Shoko API versions and supported capabilities using documented endpoints only.
- [ ] Return separate connection, inventory, mapping, path, and safety readiness results; do not call readiness a pass if a required snapshot is incomplete.
- [ ] Render actionable setup messages without API keys, raw settings payloads, or internal exception/state names.
- [ ] Run all M2-focused tests and `git diff --check`.

**M2 gate:** Anime-only scope and exact paths are established or setup is blocked with one clear decision; manual Shoko collections and live-action roots cannot enter the eligible set.

## M3 — Simplified, persistent safety model

### Task 3.1: Inspect supported Shoko safety facts

**Files:** Create `src/shoko_linker/safety_check.py`, `tests/test_safety_check.py`; modify `docs/compatibility.md` only with evidence-backed findings.

- [ ] Build tests from the verified Shoko v5.3.3 schemas for `GET /api/v3/Settings`, `GET /api/v3/Queue`, and `GET /api/v3/ImportFolder`; include standard-user 401/403 responses.
- [ ] Parse only the explicit required safety booleans and queue/folder facts. Never log, return, or persist the raw settings document; include a fixture containing sentinel secrets and assert they never escape.
- [ ] Reject unsafe file-quality deletion, renamer/move, duplicate-deletion, MyList, plugin, or drop-folder settings according to evidence-backed side-effect rules; missing, malformed, unsupported, or conflicting values are `unknown`, not safe.
- [ ] If `GET /Settings` is unavailable to a normal key, require the one-time physical setup confirmation for each fact the API cannot prove. Do not request admin credentials as an undocumented new requirement.
- [ ] Never issue `PATCH /Settings`, queue mutation, import, or file-system calls.
- [ ] Run `uv run pytest -q tests/test_safety_check.py tests/test_attestation.py tests/test_shoko.py`.

### Task 3.2: Persist onboarding consent and version/scope identity

**Files:** Create `src/shoko_linker/onboarding.py`, `tests/test_onboarding.py`; modify `src/shoko_linker/state.py`, `tests/test_state.py`, `tests/state_v1_fixture.py`.

- [ ] Extend the migration chain from schema v2 with additive onboarding/auth/rate-control storage; test an actual v2 fixture with sends, budgets, campaigns, completed links, and unresolved sends before/after migration.
- [ ] Store setup stage, selected roots/path mappings, user-selected scope digest, endpoint identity, observed Shoko version/capabilities, safety digest, explicit automatic-linking consent, pause state, and timestamps transactionally.
- [ ] Persist the consent as application onboarding state, not as an `attestation record`, generated note, pre-approved campaign, or fabricated operator action. Keep old leases/approval digests untouched and non-authorizing for the new service path.
- [ ] Re-evaluate observable safety and identity every startup and before every write. Endpoint/version/scope/safety changes invalidate authorization and pause new writes while read-only monitoring continues.
- [ ] Do not expire the user confirmation on an arbitrary seven-day timer. Require a fresh explicit confirmation only after a material identity/safety/scope change or user revocation.
- [ ] Make Pause durable across restart; Resume clears only the user pause and re-enters every safety/discovery gate.
- [ ] Run `uv run pytest -q tests/test_onboarding.py tests/test_state.py tests/test_attestation.py`.

### Task 3.3: Enforce service-only authorization without weakening advanced gates

**Files:** Modify `src/shoko_linker/daemon.py`, `src/shoko_linker/config.py`, `src/shoko_linker/cli.py`; extend daemon/CLI/onboarding tests.

- [ ] Add an explicit service-mode eligibility input sourced from validated persisted onboarding consent; do not implement it by setting `SHOKO_LINKER_ALLOW_APPLY=1` in-process or globally changing `apply_gate`.
- [ ] Keep `once --apply`, legacy daemon, and campaign operations under their current explicit gate and expiring attestation behavior.
- [ ] Before each service write, re-check scope/safety identity and the existing file-level unrecognized/XRef preflight, immutable target, queue guard, budget, apply lock, and uncertain-send ordering.
- [ ] Assert service mode before consent sends zero POSTs; after valid setup it can send only within the selected roots; stale/changed identity or any unsafe/unknown condition sends zero POSTs and persists a clear pause reason.
- [ ] Run focused tests and `uv run mypy src/shoko_linker/config.py src/shoko_linker/daemon.py src/shoko_linker/cli.py`.

**M3 gate:** No unattended write occurs before explicit setup consent and current safe conditions. Existing advanced write gates and lease tests remain green; API-invisible facts require real one-time user confirmation in the product flow, not a test-created production approval.

## M4 — Automatic backlog and arrival processing

### Task 4.1: Add a durable automatic backlog path

**Files:** Create or modify `src/shoko_linker/daemon.py` and `src/shoko_linker/state.py`; extend `tests/test_daemon.py`, `tests/test_state.py`, and `tests/test_f_campaign_e2e.py` only where shared guards are exercised.

- [ ] Add an explicit service policy that queues both backlog and arrivals after onboarding authorization; do not auto-send backlog under the legacy `NEW_ARRIVAL` policy.
- [ ] Reuse `FileIdentitySnapshot`, frozen mapping target digests, current state transitions, `sends`, SQLite budget slots, apply lock, protected-link preflight, asynchronous verification, and restart recovery.
- [ ] Reconsider only affected unresolved files when the Anime-Lists cache activates a mapping update; quarantine any changed previously resolved target and leave verified Shoko associations untouched.
- [ ] Persist internal queue progress, first-batch/canary verification, and rate-controller state; do not create an operator-approved campaign row, fake approval digest grant, or deadline-bound queue.
- [ ] First automatic batch is one submission. Do not ramp until its entire expected target set is verified; any rejected, uncertain, partial, conflicting, stale, or anomalous outcome pauses automatic expansion.
- [ ] After success, process other verified-safe files independently; ambiguous/conflicting files get durable reasons and do not stop unrelated candidates. Never partially link a multi-episode file.
- [ ] Existing/manual-linked and out-of-scope files stay unchanged; an existing link or uncertain send cannot be retried by the service.
- [ ] Simulate process restart after prepare, after `request_may_have_been_sent`, and during async verification; assert exactly zero replay POSTs.
- [ ] Run `uv run pytest -q tests/test_daemon.py tests/test_state.py tests/test_linking.py tests/test_f_campaign_e2e.py`.

### Task 4.2: Use practical, feedback-controlled throughput

**Files:** Create `src/shoko_linker/rate_control.py`, `tests/test_rate_control.py`; modify `src/shoko_linker/config.py`, `src/shoko_linker/daemon.py`, `src/shoko_linker/state.py`.

- [ ] Keep hard per-cycle, rolling-window, pending-file, pending-episode-job, wall-time, and recovery ceilings durable. Raise the default rolling ceiling from 200/week to a documented practical ceiling of 5,000/week; retain a bounded cycle ceiling (20 submissions) and pending episode-job ceiling (10) unless measured code/API constraints require a stricter value.
- [ ] Start with a one-submission canary. Persist successful streak and current cycle cap; increase gradually (1, 2, 4, 8, 16, then configured hard ceiling) only when queue is healthy, sends verify, outstanding jobs stay within budget, and no API/throttle/anomaly signal is present.
- [ ] Immediately hold or lower the cap for queue blockage, outstanding-job pressure, verification delay/failure, API error/throttle, source/mapping anomaly, and safety refusal. Keep uncertain-send and durable rolling-budget protections independent of the adaptive cap.
- [ ] Make the controller deterministic under an injected clock and persist every state transition so a restart cannot reset the cap and burst submissions.
- [ ] Test every signal individually, combined limits, disabled safety, persistence/restart, and queue-health ramp sequence; assert at most 5,000 rolling submissions/week and 20 per cycle by defaults.
- [ ] Run `uv run pytest -q tests/test_rate_control.py tests/test_daemon.py tests/test_config.py`.

**M4 gate:** Backlog and new arrivals progress without campaign preparation/approval/canary/drain CLI work; verified links and previous history persist; failures reduce or stop throughput; no deadline expiry or uncertain POST replay.

## M5 — Minimal status dashboard and ordinary controls

### Task 5.1: Create first-user setup and authenticated dashboard

**Files:** Create `src/shoko_linker/dashboard.py`, `tests/test_dashboard.py`; modify `src/shoko_linker/onboarding.py`, `src/shoko_linker/state.py`.

- [ ] Use a small standard-library HTTP server and HTML forms; serve status categories and plain-language actionable messages, plus an advanced diagnostics section with internal IDs/details only after login.
- [ ] First visit atomically creates the dashboard username/password; store only a salted, slow password hash and a separate random signing key in `/data`, with restrictive file permissions. Never put passwords, session cookies, or setup secrets in logs or reports.
- [ ] Require authenticated, expiring same-site session cookies for status details, Pause, Resume, safety confirmation, and scope selection after enrollment. Use constant-time credential checks and CSRF tokens on state-changing forms. The setup endpoint becomes unavailable immediately after the first account is created.
- [ ] Serve only from the explicitly configured dashboard listener; do not auto-publish a Docker host port. Compose/Unraid makes local port publication explicit. Do not add CORS, remote management proxies, or an unauthenticated control API.
- [ ] Pause/Resume use the same durable onboarding state as the service. Resume does not authorize writes unless all safety/scope checks currently pass.
- [ ] Add bounded request-body/field sizes and generic login errors; test malformed, overlong, replayed-CSRF, unauthenticated, and wrong-password requests.
- [ ] Run `uv run pytest -q tests/test_dashboard.py tests/test_onboarding.py`.

### Task 5.2: Run dashboard with worker and expose ordinary operations

**Files:** Modify `src/shoko_linker/cli.py`, `src/shoko_linker/bootstrap.py`, `src/shoko_linker/dashboard.py`; extend CLI/service tests.

- [ ] Start the dashboard and background service under one managed process with graceful SIGINT/SIGTERM shutdown; no detached/untracked worker threads or competing SQLite writers.
- [ ] Serialize dashboard state access with the service runtime: dashboard handlers must not concurrently use the worker's thread-affine SQLite connection; send mutations through the runtime's serialized control path and use an independently opened read-only connection for dashboard queries if needed.
- [ ] Add `status`, `pause`, and `resume` CLI equivalents using the same state APIs; retain all advanced command parsers/handlers and make the ordinary top-level help list the short operations first with advanced commands grouped under a clear help surface.
- [ ] Display connection state, verified linked count, waiting count, skipped/matching count, worker running/paused state, mapping-cache status, and the next user action. Map errors to approved plain-language messages and keep secrets/internal state names out of ordinary output.
- [ ] Test HTTP and CLI status parity, authentication boundaries, control persistence, worker shutdown, and no writes before setup authorization.
- [ ] Run focused dashboard/service/CLI tests and `uv run ruff check src/shoko_linker/dashboard.py src/shoko_linker/onboarding.py src/shoko_linker/cli.py`.

**M5 gate:** The ordinary user can inspect operation and pause/resume without learning campaign/attestation commands; no unauthenticated dashboard control path exists after first-account setup; port publication remains explicit.

## M6 — Migration, docs, integration, and final review

### Task 6.1: Finish compatibility and migration behavior

**Files:** Modify `src/shoko_linker/state.py`, `src/shoko_linker/config.py`, `docs/advanced.md`; extend migration/config/CLI tests.

- [ ] Test opening existing schema-v2 state containing verified, pending, blocked, campaign, and unresolved-send history; verify row counts/identities and immutable fingerprints are unchanged after migration.
- [ ] Test existing full YAML config and each existing advanced CLI command still parses and retains its prior safety gate.
- [ ] Add an upgrade path that enters read-only setup mode by default; never treat old `apply_changes`, `SHOKO_LINKER_ALLOW_APPLY`, leases, campaigns, digests, or drain authorizations as consent to the broader automatic scope.
- [ ] Document how the user can stop/rollback the container while preserving `/data`; no state reset or migration that discards unresolved history.

### Task 6.2: Complete the fresh-install end-to-end acceptance test

**Files:** Create `tests/test_service_e2e.py`; modify `tests/e2e_harness.py` only for shared API behavior.

- [ ] Fresh `/data` and only Sonarr/Shoko URLs/API keys: mocked service connects, identifies anime, fetches mapping, infers an unambiguous path mapping, enters setup, and makes zero writes before separate authenticated safety/scope authorization.
- [ ] After simulated physical-safety consent and valid API-observed conditions, verify a one-file automatic canary posts once and reaches exact Shoko XRef verification; only then does the rate controller expand.
- [ ] Restart the service and prove verified files remain complete, queued files continue, pending/uncertain sends are inspected but never replayed, and prior links remain unchanged.
- [ ] Include live-action series, Shoko manual drop folder, ambiguous roots/mappings, invalid API keys, incomplete snapshots, unsupported versions/capabilities, inaccessible settings, unsafe settings, existing links, one conflicting file plus one safe file, and temporary mapping/API failures.
- [ ] In the mapping-failure scenario, retain the active last-known-good XML, retry on schedule without a tight loop, and automatically reconsider an unresolved file once a later valid generation adds its mapping.
- [ ] Assert each refusal has a user-actionable status and zero unsafe POSTs; unrelated safe files continue when an individual file alone is ambiguous.
- [ ] Run `uv run pytest -q tests/test_service_e2e.py` before the full suite.

### Task 6.3: Rewrite ordinary-user and Unraid documentation

**Files:** Modify `README.md`, `docs/deployment.md`, `compose.example.yaml`, `unraid.xml`; create `docs/advanced.md`.

- [ ] Opening README explains what the service does and presents the four-step install/API-key/start/safety-ready flow.
- [ ] Document only exceptional setup choices (multiple anime roots, ambiguous paths, Shoko settings unavailable to normal keys, physical side-effect confirmation) in plain language.
- [ ] Provide a practical Unraid installation/template walkthrough, local dashboard port choice, first-login creation, and migration procedure.
- [ ] Move detailed campaign, attestation, digest, fingerprint, mapping internals, SQLite recovery, low-level CLI, and operator safety procedures into advanced documentation; keep advanced commands documented, not deleted.
- [ ] Search ordinary README/setup/dashboard wording to ensure campaign, attestation, digest, reconciliation, exception class, and state-machine jargon do not leak into the normal path.

### Task 6.4: Run all configured checks and adversarial review

**Files:** No code edits until findings are recorded; review every changed file and the final diff.

- [ ] Run `uv run pytest -q`.
- [ ] Run `uv run ruff check .`.
- [ ] Run `uv run ruff format --check .`.
- [ ] Run `uv run mypy`.
- [ ] Run `git diff --check`.
- [ ] Run `docker build -t shoko-sonarr-linker:local .` and a local offline/container smoke test that confirms startup, signal shutdown, non-root identity, no host port publication, and `/data` persistence. Report Docker checks unavailable if the runtime is not installed; never claim them passed without output.
- [ ] Perform a distinct read-only review of scope/root isolation, path mapping, API facts/secret handling, Shoko file side effects, one-time consent, campaign compatibility, async verification, restart replay, dashboard auth/CSRF, migrations, and throughput ceilings. Record severity/path/impact/reproducer/fix for each finding.
- [ ] Fix P0/P1 and acceptance-blocking P2 findings and rerun affected focused tests plus all checks required by those changes.
- [ ] Update `STATUS.md` with each M gate’s actual evidence; label live Sonarr/Shoko, production write safety, deployment, and GHCR state **EXTERNAL/UNVERIFIED**.

**Final gate:** A fresh mocked installation from four connection settings completes safe setup and continuously processes backlog/arrivals; all safety refusals and compatibility checks above are observed; full checks and Docker smoke pass; independent review has no unresolved P0/P1/blocking P2.

## Commit and push

- [ ] Commit each accepted implementation slice with focused files and a concise repository-style message; do not include unrelated or secret-bearing files.
- [ ] Preserve the user-provided current `PLAN.md` content unchanged. Commit the redesign implementation plan and updated `PROJECT.md` as task documents.
- [ ] Before final push, inspect `git status`, `git diff`, `git log --oneline -10`, current branch, and `git diff origin/main...HEAD`; push the intended commits to `origin/main` only after all offline gates pass.
- [ ] Verify the remote `main` ref contains the pushed commit(s) and report exact evidence. Do not publish GHCR images, deploy, or perform live Shoko writes.
