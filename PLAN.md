# Sonarr-to-Shoko — Ninth Independent Review Handoff

Repository: https://github.com/cjmanca/sonarr-to-shoko

Reviewed commit: `e78efc86dc8671dd75c401fc64c9d72aa35748b3`

## Review summary

The user-experience redesign is substantially implemented.

Verified components include:

* Environment-based startup.
* Sonarr anime-scope discovery.
* Sonarr/Shoko path-mapping discovery.
* Shoko safety inspection.
* Persistent onboarding confirmation.
* Automatic backlog processing.
* Adaptive submission controls.
* Authenticated dashboard.
* Legacy CLI compatibility.
* Updated beginner documentation.

The reviewed commit passed GitHub CI and GHCR publishing.

Two issues remain.

## F35 — P1: Dashboard blocks during background processing

### Affected files

* `src/shoko_linker/service.py`
* `src/shoko_linker/cli.py`
* `src/shoko_linker/dashboard.py`
* Related service/dashboard tests

### Root cause

`run_dashboard_worker()` runs the linking worker synchronously on the same thread responsible for accepting dashboard requests.

Each iteration executes:

1. `worker_tick()`
2. `server.handle_request()`

A long-running reconciliation or verification cycle prevents the dashboard from accepting new requests.

The normal service invokes the existing daemon machinery, which can perform lengthy API operations.

This is incompatible with the intended always-available management interface.

### Required correction

Decouple dashboard request handling from the background linking worker.

Prefer a minimal implementation using existing Python standard-library facilities.

A reasonable approach is:

* One background thread for the linking worker.
* One thread for the HTTP server.
* SQLite transactions for durable shared state.
* A small, thread-safe status snapshot for dashboard display.

Alternatively, run the HTTP server on the main thread and the linking worker on a dedicated background thread.

Avoid introducing Celery, Redis, a message broker, or another scheduling framework.

### Safety requirements

Preserve the existing single-writer guarantees.

The worker must remain the only component performing automatic Shoko link submissions.

Dashboard actions should modify durable control state rather than directly invoking linking operations.

The worker must recheck pause and authorization state before subsequent POST requests.

Preserve:

* OS apply locking.
* SQLite transaction boundaries.
* Durable submission fingerprints.
* No-replay semantics.
* Existing-link protection.
* Per-submission authorization checks.
* Safe startup and shutdown.

### Threading requirements

Avoid sharing a SQLite connection across threads.

Each thread should obtain its own appropriately scoped state connection.

Any in-memory status exchanged between threads must be synchronized or represented by an immutable snapshot.

Network clients must not be shared unsafely.

The dashboard should remain responsive even when the linking worker is blocked on a slow API request.

### Shutdown

Ensure SIGTERM:

1. Stops accepting new linking work.
2. Signals the background worker to stop.
3. Allows the current protected operation to finish within the configured shutdown grace period when possible.
4. Closes the HTTP listener.
5. Releases resources safely.

Do not forcibly terminate a thread while a Shoko submission is in flight.

### Regression tests

Simulate:

* A reconciliation cycle blocked on a network request.
* A verification operation taking several minutes.
* Dashboard status access during worker activity.
* A pause request while a worker cycle is executing.
* A resume request.
* Dashboard account creation while the worker is idle.
* Worker exceptions.
* Graceful SIGTERM.
* Repeated start/stop cycles.

Verify that:

* Dashboard status requests remain responsive.
* Pause requests can be accepted while the worker is busy.
* The next submission observes the persisted pause.
* No additional POST is started after the worker observes the pause.
* Existing submissions remain recoverable.
* No SQLite threading errors occur.
* Shutdown does not corrupt durable state.

**Acceptance:** A long-running linking operation cannot make the dashboard unavailable.

---

## F36 — P2: Docker Compose quick-start requires an undocumented local build

### Affected files

* `compose.example.yaml`
* `README.md`
* Docker/installation tests

### Root cause

The main Compose example currently contains:

```yaml
image: shoko-sonarr-linker:local
```

That image is not automatically available to ordinary users.

However, the README presents Docker Compose as a normal installation method.

The Unraid template already references:

```text
ghcr.io/cjmanca/shoko-sonarr-linker:latest
```

The two installation methods are inconsistent.

### Required correction

Use the published GHCR image as the default Compose image.

Prefer a pinned version or immutable SHA for reproducible deployments.

If `latest` is used for convenience, document the update implications and retain the option to pin a SHA.

Keep local source builds as an explicitly documented developer option.

Do not require normal users to install Git, Python, uv, or build the Docker image themselves.

### Additional usability review

Check the installation instructions end to end from the perspective of someone with minimal Docker knowledge.

Verify that:

* All required environment variables are clearly identified.
* Users can obtain the image without building it.
* The persistent `/data` volume is configured.
* The dashboard port can be deliberately exposed for local access.
* The container starts the normal service automatically.
* No YAML configuration file is required.
* No advanced CLI commands are necessary for first-use setup.

Keep the dashboard private by default and explain how to expose it safely on a trusted local network.

### Regression tests

Validate the Compose structure and Unraid template.

Verify:

* The default image resolves to the published GHCR repository.
* Required environment variables are passed.
* Persistent storage is configured.
* No unnecessary media mounts exist.
* No privileged mode or Docker socket mount is required.
* The normal service is the default container command.

**Acceptance:** Users can install the published container using the documented Compose procedure without building source code.

---

## Implementation sequence

### Phase 1 — Dashboard responsiveness

Implement F35 first.

This is the primary production-usability issue.

Use the existing service and SQLite architecture rather than adding external infrastructure.

### Phase 2 — Installation experience

Implement F36.

Make Docker Compose and Unraid installation instructions consistent with the actual published image.

### Phase 3 — Validation

Run:

```bash
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
uv run mypy
git diff --check
docker build -t shoko-sonarr-linker:review .
```

Conduct independent read-only review focused on:

* Worker/dashboard concurrency.
* SQLite connection ownership.
* Persistent pause and resume controls.
* Per-submission authorization.
* Thread shutdown and resource cleanup.
* Container startup.
* Beginner installation documentation.

Record results in `STATUS.md`.

Follow the repository's orchestration rules and keep the active implementation plan immutable.

Do not perform production Shoko writes, deploy, or push without authorization.

## Completion criteria

1. Dashboard remains responsive during long worker operations.
2. Pausing during a running cycle prevents further submissions once the pause is observed.
3. SQLite state remains consistent across concurrent dashboard and worker activity.
4. Worker shutdown preserves in-flight submission recovery guarantees.
5. Docker Compose uses a published image by default.
6. Installation requires no local source build.
7. Existing authorization and linking safeguards remain intact.
8. Targeted tests pass.
9. Full configured checks pass.
10. Independent review finds no unresolved P0/P1
