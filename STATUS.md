# STATUS — F35/F36 Ninth Review Handoff

**Run:** Implement the user-selected F35/F36 handoff in `PLAN.md`.
**Selected PLAN.md SHA-256:** `FA35235ABDCF10D9A78F7741DE51E3CC8160E15CD688A67789EB94412EBACB07`.
**Plan selection:** At startup, the working tree already contained the replacement F35/F36 `PLAN.md` and an empty `STATUS.md`. The user's request explicitly selected the current handoff. Its contents were preserved unchanged during implementation and this digest records the selected version.
**Baseline HEAD:** `e78efc8` (`docs: finalize redesign acceptance record`); branch `main`, initially aligned with `origin/main`.
**Latest accepted implementation commit:** `8bdf12bb0c8b14f7957e3c0ebc7d27bb9625ca55` (`fix: keep dashboard responsive and use published image`).
**Mode:** Offline/mock validation only. No live Sonarr/Shoko requests or writes, production deployment, or direct GHCR publication.

## Completed work

- **F35 — dashboard responsiveness:** `run_dashboard_worker()` now handles HTTP requests on the caller thread and executes worker ticks on a dedicated non-daemon thread. Worker exceptions are logged and retried. Shutdown closes the listener before joining the worker and does not forcibly terminate an in-flight operation. Dashboard and worker setup use separate API clients; status snapshots are synchronized; each request/worker operation opens its own SQLite connection. Existing worker-only submission, per-submission authorization/pause checks, OS apply lock, durable send/no-replay, existing-link protection, and recovery paths remain in place.
- **F36 — install experience:** Compose defaults to `ghcr.io/cjmanca/shoko-sonarr-linker:latest`, uses a shared external Docker network, and keeps host ports unpublished with a persistent `/data` volume. The README documents the four required environment variables, `.env` handling, pull/start steps, private dashboard exposure, mutable `latest` updates, immutable SHA pinning, Unraid parity, and the developer-only local build. `.env` is ignored by Git. The Unraid template has the same image and a 360-second stop timeout.
- Added regression coverage for dashboard status/account/pause while the worker is blocked, pauses during preflight and verification, worker exception retry, graceful SIGTERM/start-stop lifecycle, Compose defaults, and Unraid settings. The service end-to-end fixture now uses a separate dashboard setup controller.

## Verification

- `uv run pytest -q` — completed at 100% with no failures; one test skipped. The earlier 120-second-limited attempt did not finish; its README sequence failure was fixed and the full suite rerun successfully.
- `uv run pytest -o addopts='' -q tests/test_service.py tests/test_daemon.py::test_dashboard_pause_during_slow_service_request_prevents_next_post tests/test_service_runtime.py tests/test_cli.py tests/test_service_e2e.py tests/test_unraid_template.py` — 35 passed.
- `uv run ruff check .` — passed.
- `uv run ruff format --check .` — 70 files already formatted.
- `uv run mypy` — no issues in 66 source files.
- `git diff --check` — exited successfully; Git emitted a CRLF-to-LF normalization warning for `tests/test_daemon.py`.
- `docker build -t shoko-sonarr-linker:review .` — passed; local image digest `sha256:b056cc4ab2d0b1811ef0d212cc24f581575255c6fdaff09a1b1c69d5dbee0b4e`.
- `docker compose -f compose.example.yaml config --quiet` — passed with temporary example environment values.
- Distinct read-only review found no unresolved P0/P1 or acceptance-blocking P2 findings.

## External validation / next action

- **EXTERNAL/UNVERIFIED:** live Sonarr/Shoko behavior and physical-safety inspection, actual pull of the published GHCR image, post-push CI/GHCR publishing, and an Unraid installation.
- Delivery commits `8bdf12bb0c8b14f7957e3c0ebc7d27bb9625ca55` and `c5a7a4f63fe39bc3641903a8b0cba55e304ae5ba` were pushed with `git push origin main`.
- Immediately after that push, `git rev-parse HEAD` and `git ls-remote origin refs/heads/main` both returned `c5a7a4f63fe39bc3641903a8b0cba55e304ae5ba`; the working tree was clean.
- Next: no repository implementation work remains. External validations above require the target services/operator environment.

## First-use account verification fix — 2026-10-26

- Bounded follow-up to the reported account-creation failure, on `main` at `5124fc5`. Local changes remain uncommitted in `src/shoko_linker/dashboard.py` and `tests/test_dashboard.py`, plus this record.
- Changed the response Referrer-Policy to `same-origin`; retained rejection of missing, null, and cross-site origins and all session/CSRF checks.
- `uv run pytest -o addopts='' -q tests/test_dashboard.py` — 22 passed, 1 skipped. Focused Ruff lint/format, focused mypy, and `git diff --check` passed. Independent read-only reviewer reran the dashboard tests with the same result and reported no findings.
- `uv run --with playwright python -` with installed headless Edge and temporary local state reproduced the native form failure under the old policy (`Origin: null`, POST 403). Under the fixed policy the browser sent its origin, received POST 303, and reached authenticated `/status`. No production services were contacted; Playwright was an ephemeral tool dependency.
- Next: deliver the local fix through an authorized image build/release, then reload the setup page and retry account creation. The installed container remains unverified; no push, publication, or deployment was performed.

## Dashboard appearance refresh — 2026-10-26

- Added a shared branded layout, inline styling, status cards, connection/control panels, and responsive account/setup forms in `src/shoko_linker/dashboard.py`. Preserved authentication, CSRF, scope consent, and the first-use form verification fix above.
- `uv run pytest tests/test_dashboard.py -q` passed (one existing skip). `uv run ruff check .`, `uv run ruff format --check .` (75 files), `uv run mypy` (70 source files), and `git diff --check` passed.
- An independent read-only reviewer reported no blocking findings. Local fixture previews were inspected in Chrome at desktop and 390px phone widths; status and setup pages had no horizontal overflow. No live Sonarr/Shoko operations were performed.
- User authorized committing and pushing the UI refresh and existing form-security fix. Next: push the accepted local change, then verify the resulting image workflow; installed-container appearance remains unverified.
