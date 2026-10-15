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
