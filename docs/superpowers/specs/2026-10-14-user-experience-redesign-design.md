# User Experience Redesign — Design

**Date:** 2026-10-14
**Scope:** The product redesign in the current, user-selected `PLAN.md` handoff.

## Goal

Make the ordinary installation a continuously running background service that can safely link eligible Sonarr-managed anime without routine CLI work. Preserve the existing guarded linker and advanced operator tooling; this is a consumer-facing layer and workflow redesign, not a rewrite of the matching, mapping, SQLite, or submission engine.

## Operating model

1. The container starts the service from environment-based Sonarr/Shoko URLs and API keys. Existing YAML configurations and advanced CLI invocations remain supported.
2. Startup reads the existing Anime-Lists mapping source and the Sonarr/Shoko APIs. It selects anime scope only from verified identifiers and exact, corroborated file/path evidence. It infers path mappings only when evidence is unique. Missing or conflicting evidence pauses at one plain-language setup decision; it never expands scope to all Sonarr series or Shoko folders.
3. Before any write, the service validates every safety fact exposed by a supported Shoko API and blocks on unsafe or unverifiable conditions. Facts that cannot be verified remotely require an explicit one-time physical-safety confirmation during setup. This confirmation is durable and bound to the endpoint/version/scope; a Shoko version or relevant safety change invalidates it. The service never changes Shoko settings or fabricates consent, and old approvals do not imply consent to the redesigned automatic scope.
4. After onboarding and safety gates pass, one persistent background worker handles both the existing backlog and newly arriving eligible files. It reuses the frozen identities, target mappings, SQLite send ledger, protected-link preflight, durable budgets, asynchronous verification, and restart recovery. It never overwrites existing links, guesses mappings, repeats uncertain POSTs, or intentionally modifies media. Per-file ambiguity is reported and skipped while unrelated safe work continues.
5. Adaptive throughput starts conservatively and responds to Shoko queue state, pending jobs, verification, API failures/throttling, and anomalies. Existing durable submission limits and uncertain-send protections remain authoritative.
6. A lightweight dashboard reports connection/link/queue/matching status and plain-language intervention needs, with authenticated Pause/Resume and advanced diagnostics. Per the user decision, the first visit offers a username/password setup wizard; after enrollment all dashboard operations require authentication. Do not publish the dashboard port automatically or expose an unauthenticated management surface to the Internet.

## Components and compatibility

- An environment-settings adapter feeds the existing typed `Settings` and HTTP clients. Advanced YAML can supply optional settings and remains backward compatible.
- A setup/discovery coordinator uses the existing Sonarr inventory, Shoko discovery, mapping index/store, and path validation. API/version/capability assumptions must be verified against supported upstream schemas before use.
- A safety/onboarding gate owns explicit scope selection, one-time physical confirmation, and persistent pause/resume state. It must not weaken existing low-level link guards.
- The automatic worker owns an internal durable backlog/arrival queue and reuses existing send/verification/recovery operations. Campaign and attestation mechanics stay available in the advanced CLI but are hidden from ordinary users.
- The dashboard is a small server-rendered local interface with authenticated controls; avoid a frontend framework and new infrastructure. Persist credentials as salted password hashes, never plaintext or logs. Keep the HTTP listener private unless an operator deliberately publishes it.
- SQLite migrations are additive and restart-safe. Existing submissions, completed links, unresolved sends, and configuration files survive upgrade; legacy approvals are never silently broadened.

## User-facing delivery

The README leads with what the service does and gives a short Docker/Unraid path: install, enter service URLs/API keys, complete only the setup choices that cannot be inferred, and start. Campaign, attestation, fingerprint, mapping, and recovery procedures move to advanced documentation without deleting the tools themselves. Container metadata/template and examples expose only the necessary app state and configuration; never mount the media library or Docker socket.

## Validation

Use the current offline test stack and Docker smoke checks. The central test uses a fresh persistent volume with only the service connection settings, mocked Sonarr/Shoko/mapping APIs, and exercises discovery, exact path/size correspondence, mapping resolution, safe onboarding, automatic link submission and verification, restart recovery, and dashboard Pause/Resume authentication. Add focused refusal coverage for ambiguous roots/mappings, invalid credentials, unsupported API capability/version, unsafe or unverifiable safety, existing links, and uncertain sends. Verify that a single problematic file does not prevent unrelated safe work. Run configured pytest, Ruff, mypy, formatting, `git diff --check`, and the Docker build/smoke path. All mocked results remain distinct from live integration evidence.
