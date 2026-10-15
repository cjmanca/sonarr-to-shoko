# STATUS — User Experience Redesign

**Run:** Implement the current user-selected `PLAN.md` redesign.
**Current PLAN.md SHA-256:** `587b920f90062092599c178ac3619ce830c66764f95fd71043158d2610dbc733`.
**Historical approved PLAN.md SHA-256:** `D43140F0DF399F09EDFE5B2D9A851F4266D7293BA6DD02D06605B7FE981A20AB` (the previous F34 handoff).
**Plan selection:** The current redesign handoff was already present in the working tree when `/goal` was invoked, and the user explicitly requested implementing that `PLAN.md` handoff. It is adopted for this run and remains unchanged; the old F34 digest does not describe the selected redesign.
**Baseline HEAD:** `5f9d4a3` (`docs: add user experience redesign design`).
**Latest accepted local commit:** `e43b656` (`docs: record redesign baseline`).
**Branch/worktree:** `main`; one worktree at the repository root, as requested.
**Mode:** Local/offline implementation only. No live Sonarr/Shoko requests or writes, production deployment, or GHCR publishing.

## Plan and design

- Design: `docs/superpowers/specs/2026-10-14-user-experience-redesign-design.md` (committed as `5f9d4a3`). The user selected first-visit browser creation of a dashboard username/password.
- Separate implementation plan: `docs/superpowers/plans/2026-10-14-user-experience-redesign.md` (created and self-reviewed; implementation has not started).
- Product requirements update: `PROJECT.md` now describes the new service while retaining exact scope, guarded linking, uncertain-send, and media-safety invariants.
- API fact confirmed in `docs/compatibility.md`: Shoko `GET /api/v3/Settings` is admin-only and returns secrets. Only verified safety booleans may be parsed; inaccessible settings require explicit one-time physical confirmation, not fabricated attestations or undocumented admin credentials.

## Progress and verification

- M0 — product requirements and execution checkpoint: **accepted**. `PROJECT.md` now makes environment startup, one-time onboarding authorization, automatic backlog processing, dashboard privacy/login, advanced CLI compatibility, and updated acceptance criteria authoritative. `PLAN.md` still matches its selected digest.
- M1 — zero-configuration startup: **in progress**.
- M1.1 — environment bootstrap: **accepted**. Added connection-only `BootstrapSettings`, fixed `/data/state.sqlite3` default, `SecretRef`-only API key fields, and shared safe URL validation; legacy YAML still uses that validator.
- M1.2 — container-default service: **next**. M1.3 and M2–M6: **not started**.
- Design-doc check: staged `git diff --cached --check` passed before commit; no runtime/code test was applicable to the design-only commit.
- M0 verification: `git diff --check` and staged `git diff --cached --check` returned no whitespace errors; `PLAN.md` SHA-256 remained `587b920f90062092599c178ac3619ce830c66764f95fd71043158d2610dbc733`; the current plan version, `PROJECT.md`, `STATUS.md`, and separate implementation plan were committed as `8919789`.
- M1.1 TDD: `uv run pytest -q tests/test_bootstrap.py` first failed on the missing bootstrap module as expected; a subsequent regression case for port `0` failed before its validator fix. `uv run pytest -q tests/test_bootstrap.py tests/test_config.py` then passed.
- M1.1 static checks: targeted `uv run ruff check ...` passed; targeted `uv run ruff format --check ...` reported all 3 files formatted; targeted `uv run mypy ...` reported no issues; `git diff --check` returned no whitespace errors.
- No product behavior or Docker/test verification has been claimed.

## Open risks / next action

- The working plan-version mismatch is resolved for this run by the user's explicit `/goal` instruction selecting the current handoff; do not edit `PLAN.md`.
- Shoko's safety settings may be unavailable to the supplied non-admin key. Automatic writes must remain paused until the user completes the physical-safety confirmation for facts that cannot be read.
- **Next action:** commit the verified M1.1 files, then add the failing test for container-default `service` startup while preserving explicit legacy CLI commands.
