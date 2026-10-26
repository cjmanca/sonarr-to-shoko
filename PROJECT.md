# Shoko–Sonarr Linker — Project Requirements

> **Purpose of this file:** Explain what this project must do and why. `AGENTS.md` contains development rules; this file describes the product requirements and expected behavior. Keep it accurate as decisions change.

## 1. Product goal and normal user experience

Shoko Server usually recognizes anime files through AniDB file hashes. Many files in the existing library are unknown to AniDB even though **Sonarr already knows exactly which TV episodes those files contain**. Manually linking approximately **7,000** unrecognized episodes in Shoko is not viable, and Shoko's GUI filename matcher can select incorrect episode numbers.

Build **`shoko-sonarr-linker`**, a continuously running **Python 3.12+** service that uses Sonarr's file-to-episode records and verified TVDB-to-AniDB mappings to associate unrecognized Shoko files with the correct Shoko episodes. The normal Docker installation discovers its environment and, after safe onboarding, handles both the existing backlog and newly arriving anime without routine CLI operation.

The ordinary user supplies Sonarr and Shoko URLs/API keys, starts the container, and completes only the first-use dashboard account and any scope or physical-safety decision that cannot be established automatically. The application starts its background service automatically. YAML configuration, campaigns, attestations, approval digests, and recovery commands remain available as advanced interfaces, not normal setup requirements.

The end result is better Shoko metadata and Shoko Relay integration with Plex. **This service does not manage Plex or media files.**

## 2. Existing environment and ownership

| Component | Responsibility |
| --- | --- |
| **Sonarr** | Source of truth for managed series, episode identities, episode-file relationships, and paths; also owns downloading, renaming, and moving files. |
| **Shoko Server** | Imports/indexes anime files and holds anime/episode associations; may also independently manage a separate manual drop-folder collection. |
| **Anime-Lists** | Supplies verified cross-references and episode-number translations between TVDB and AniDB. |
| **This service** | Reconciles *unrecognized Sonarr-managed anime files* into Shoko associations only. |
| **Plex + Shoko Relay** | Consume the resulting Shoko metadata; no direct integration is required for v1. |

The environment is Docker-based on Unraid. Sonarr and Shoko can use different **container-internal media paths** and must be reachable over the network; Docker service names work only where network configuration permits.

**Scope isolation is critical:** the same Sonarr instance also manages live-action TV. The service selects only positively identified anime series under an explicitly confirmed, persisted set of anime roots. Shoko's manually managed folders and unrelated files are out of scope. Missing or ambiguous discovery must pause for one clear user decision; it must never silently widen scope.

## 3. Required behavior

### Initial reconciliation and continuous operation

- **Normal service mode:** start automatically in the container, remain running, discover new or still-unrecognized Shoko files, and process them after Sonarr/Shoko indexing and all setup safety gates have passed.
- **One-shot/advanced mode:** retain read-only dry-run as the default; explicit apply retains its existing independent config/environment/attestation gates.
- **Incremental operation:** avoid redoing expensive lookups or resubmitting links on every poll.
- **Full reconciliation:** occasionally rescan eligible files so missed changes, restarts, renames, and mapping updates are recovered.
- **Restart recovery:** use persisted state to resume incomplete work and verify previously submitted link jobs rather than blindly sending them again.
- **Automatic-write authorization:** the normal service may write only after the user has explicitly confirmed the selected scope and one-time safety/setup consent. API keys alone, old config flags, an old lease, or a campaign approval do not imply consent to broader automatic linking. Before consent, setup and monitoring are read-only.
- **Dry-run invariant:** any advanced dry-run makes no remote changes, including series creation or refresh requests.

### Eligible-series and file selection

1. Discover Sonarr series and select only positively identified anime series (verified `seriesType`/identifiers) under a confirmed, nonempty root allowlist. If multiple plausible anime roots exist, ask the user to select the intended scope.
2. Fetch episodes and episode files **only for selected anime series**; also verify each file path is within an eligible root.
3. Fetch Shoko's unrecognized files and import-folder metadata. Limit Shoko candidates to selected Sonarr-owned anime paths, excluding Shoko-managed drop folders.
4. Infer Sonarr↔Shoko Docker path mappings only from sufficient unique exact-path/relative-structure and file-size/identity evidence. Persist the inferred mapping; allow explicit user mapping input only when automatic discovery genuinely fails.
5. Match files using their **complete normalized paths** and confirm **file-size equality**. Reject duplicates, unexpected locations, conflicts, and ambiguous matches.
6. Support **several Sonarr episodes associated with one media file** (e.g., an `S01E01-E02` file).

A root match must be **path-component-aware**: `/anime` must not accidentally include `/anime-old`. Missing root allowlists, unsupported series types, incomplete snapshots, or invalid/ambiguous mappings must fail closed, never expand to the entire Sonarr library.

### Episode identity translation

For each matched file, use **Sonarr episode IDs/metadata**, not a filename guess, to obtain its TVDB series, season, and episode identifiers. Resolve these through the community-maintained **Anime-Lists** mapping files into:

`AniDB anime ID + AniDB episode type + AniDB episode number`

Then resolve that identity to **Shoko's internal episode ID** before linking. These ID spaces are distinct and must not be conflated.

Anime-Lists mappings may include explicit episode maps, season offsets, episode ranges, absolute numbering, split AniDB series, and specials. Respect their actual precedence and semantics. Build a reverse TVDB-to-AniDB lookup from the source mapping data. Treat a source that maps to multiple destinations according to its documented semantics; reject genuinely ambiguous mappings.

**Example filename:**

```text
Attack on Titan (2013) - S04E26 - 085 - Traitor [HDTV-1080p][AAC 2.0][JA][x265 Judas]-Judas.mkv
```

Here `S04E26` is Sonarr/TVDB numbering and `085` is an absolute number. The matching AniDB entry has its **own** episode numbering. Neither `26` nor `85` should be used as an AniDB episode number without an explicit, validated mapping. Keep this case as a regression test.

If a file contains multiple Sonarr episodes, **all** must map successfully; link the complete deduplicated Shoko episode-ID set or skip the entire file. Never silently create partial links.

### Keeping Anime-Lists mappings current

- **Automatically refresh the Anime-Lists XML in the long-running daemon**, weekly by default (`refresh_hours: 168`, configurable). Check at startup when the cache is missing or a refresh is due. This must work without rebuilding or restarting the Docker image and without configuring an external cron job.
- Support an on-demand **`mappings refresh [--force]`** and a **`mappings status`** CLI command. Show the active source revision/checksum and last/next check time; do not require manual download/replacement of the XML.
- Keep a locally persisted last-known-good snapshot. Download via HTTPS using conditional requests when supported; validate new XML and activate it atomically. A missing, failed or malformed update must not corrupt the active mapping, cause a tight retry loop, or produce unsafe fallback guesses. With no valid cached/downloaded mapping, linking is unavailable.
- When refreshed data adds previously missing mappings, automatically reconsider affected unresolved files without waiting for an unrelated full rescan. **Do not silently add preexisting-backlog files to a previously approved advanced campaign.** Changes to previously resolved target mappings must be quarantined for review; existing Shoko associations are never overwritten automatically.

### Shoko linking and verification

- Use **Shoko's supported API**, not direct database changes. Verify endpoint schemas against the deployed Shoko version.
- Link only Shoko files that still qualify as unrecognized and **have no existing protected episode associations**.
- Re-read the current file associations immediately before writing; the linking endpoint may replace existing manual links.
- Shoko may process link requests asynchronously: **HTTP success means accepted, not completed**. Poll/re-read the resulting associations and record success only after verification.
- Do not modify, replace, or “repair” existing links automatically. Report conflicts for review.
- Where the required Shoko anime/episode metadata is missing, optionally create/refresh the series through supported Shoko APIs in **apply mode** and retry once available. Never do this in dry-run mode.

### Normal automatic processing

- The ordinary service maintains a persistent queue for both the existing backlog and new arrivals. It reuses frozen file identities, immutable mapping targets, durable SQLite sends/budgets, protected-link preflight, asynchronous verification, restart recovery, and duplicate-submission prevention.
- After explicit setup authorization and all current safety checks pass, the service submits an initial one-file batch and verifies the complete target association before increasing throughput. No manual campaign preparation, approval digest, canary command, drain authorization, or deadline calculation is required in normal service mode.
- Work does not expire merely because it takes longer than seven days. Individual ambiguous, conflicting, or out-of-scope files are skipped with durable reasons while unrelated safe work continues.
- Keep durable per-cycle, rolling-window, pending-file, pending-episode-job, wall-time, and recovery ceilings. Start at one submission and increase gradually only when Shoko's queue, pending jobs, verification, API responses, and anomaly signals show healthy operation. Reduce or stop on congestion, failure, throttling, mapping/source drift, or safety uncertainty. The initial practical ceilings are 20 submissions per cycle, 5,000 per rolling week, and 10 pending episode jobs; all remain hard limits, never bypassed by adaptation.
- When mapping refresh adds a missing mapping, reconsider affected unresolved files automatically. A previously resolved target change is quarantined for review; verified Shoko associations are never overwritten.

### Advanced campaign compatibility (operator CLI)

The existing digest-approved campaign workflow remains available for advanced operation. The following rules apply to that workflow only; they are not required for the normal onboarding-authorized service queue.

- The advanced daemon campaign drain runs only with the operator's explicit, digest-bound `campaign authorize-drain` confirmation of a **settled canary, judged from the durable send ledger**: every send ever attempted for the campaign must be terminal **VERIFIED**. A rejected/refused request (its file back in `waiting`), an in-flight send, and an indeterminate/partial/conflict outcome are *not* canary completion; the authorization visibly refuses and lists every outstanding send (file id, outcome, state). A rejected file must be resubmitted and verified through the normal bounded flow before the grant succeeds.
- Recoverable waits must not permanently pause a campaign: normal rolling/cycle budget exhaustion, pending Shoko metadata, pending async verification jobs, and transient queue congestion mark the campaign **deferred** with a persisted retry time (the earliest rolling-slot expiry when the rolling budget blocks, otherwise a fixed daemon yield span of five cycle intervals that re-arms on every stall and never pauses the campaign). The campaign stays approved and drain-authorized, is skipped while the window is closed, and resumes automatically once the window permits — bounded by the same per-cycle cap and rolling ceilings. The zero-progress breaker pauses only genuinely-unexpected stalls (remaining rows parked in operator-review safety states).
- Hard stops stay terminal: invalid/expired attestation lease, revoked approval, plan/mapping drift, source discontinuity, unexpected existing links, deadline expiry, and the `max_submissions` ceiling (plan complete) keep pausing writes for operator action.

**This service must never intentionally rename, move, delete, rewrite, or open media files for modification.** Shoko's linking jobs can indirectly invoke server-side deletion, renaming, plugins, or external actions; validate these hazards before any write. No media-directory Docker mount is necessary.

## 4. Configuration and operations

### Normal Docker configuration

The standard container installation requires only these four service settings:

```text
SONARR_URL
SONARR_API_KEY
SHOKO_URL
SHOKO_API_KEY
```

API key values are read at runtime and never stored in settings, files, reports, or logs. The first dashboard visit creates the dashboard username/password; those dashboard credentials are not Sonarr/Shoko API keys. The container starts the background `service` mode without a mounted YAML file. The user only needs an additional setup decision when safe anime scope/path mapping or a physical-safety fact cannot be established automatically.

Advanced users may use an optional human-readable YAML configuration. The schema below is an advanced example; its roots and mappings remain explicit and fail-closed:

```yaml
sonarr:
  url: http://sonarr:8989
  api_key_env: SONARR_API_KEY
  allowed_roots:
    - /data/media/anime

shoko:
  url: http://shoko:8111
  api_key_env: SHOKO_API_KEY
  allowed_roots:
    - /anime

paths:
  mappings:
    - sonarr: /data/media/anime
      shoko: /anime

mappings:
  source: anime-lists
  refresh_hours: 168
  overrides_file: /config/overrides.yaml

sync:
  interval_seconds: 300
  full_reconcile_interval_hours: 24
  new_file_grace_period_seconds: 300
  apply_changes: false  # Safe default
  max_concurrent_links: 1

database:
  path: /data/state.sqlite3
```

All paths above are **examples**, not assumptions about the deployed Unraid directory layout. Advanced YAML configuration must validate that every eligible path is mapped unambiguously.

### Safety onboarding and authorization

- Setup and normal service startup begin read-only. The service validates every safety fact it can observe through supported Shoko APIs before enabling automatic writes.
- Shoko `GET /api/v3/Settings` is admin-only and returns server secrets. If the configured API key can access it, parse only required safety booleans and never log, persist, or return the raw document. If it cannot be read, ask the user to inspect the relevant Shoko safety settings and physical/container facts once during setup. Do not require undocumented extra API credentials.
- The setup confirmation explicitly authorizes automatic linking only for the displayed, selected scope after the user verifies any facts that cannot be observed through the API. Persist a confirmation digest bound to the Sonarr/Shoko endpoints, Shoko version/capabilities, roots, path mappings, and observed safety facts. Re-check observable facts at startup and before each write; a relevant change invalidates authorization and pauses automatic writes until the user confirms again.
- Do not change Shoko configuration, invent physical-media facts, create fictitious attestation records, or reinterpret `sync.apply_changes`, `SHOKO_LINKER_ALLOW_APPLY`, old leases, campaigns, approval digests, or drain grants as consent for this service mode.
- Legacy one-shot apply and campaign CLI commands retain their existing gate (`sync.apply_changes`, `SHOKO_LINKER_ALLOW_APPLY=1`, and the applicable valid attestation/approval). Automatic service authorization is a separate durable onboarding state.

Provide CLI entry points equivalent to:

```text
shoko-sonarr-linker service
shoko-sonarr-linker status
shoko-sonarr-linker pause
shoko-sonarr-linker resume
```

Keep the advanced CLI entry points available:

```text
shoko-sonarr-linker validate
shoko-sonarr-linker once --dry-run
shoko-sonarr-linker once --apply
shoko-sonarr-linker daemon
shoko-sonarr-linker report
shoko-sonarr-linker mappings status
shoko-sonarr-linker mappings refresh --force
```

Use **SQLite** in a persistent volume to track identity, attempts, submitted operations, verification, retry scheduling, and unresolved reasons. Cache the downloaded Anime-Lists data locally, automatically refresh it on the configured schedule, persist update timing/source version, retain its last known-good version on download/validation failure, and retry mapping failures when mapping data changes. Support optional user-maintained mapping overrides without editing the upstream XML.

Use bounded API concurrency, timeouts, exponential backoff, and sensible rate limits. Polling plus periodic full reconciliation is sufficient; webhooks, queues, and extra infrastructure are not required.

## 5. Results, observability, and failure handling

Every run should distinguish at least:

- Successfully verified new links.
- Already-linked files (left unchanged).
- Pending Shoko import, hashing, or series metadata.
- Missing/ambiguous TVDB-to-AniDB mappings.
- Mapping cache age, last successful refresh, failed download, and newly resolved mapping counts.
- Path or size mismatches and files outside the allowed roots.
- Retryable API failures and permanent skips.

Provide human-readable logs and machine-readable reports of unresolved records, with their source identifiers, target candidates (where known), and actionable reasons. Do not log API keys. One bad file must not stall the rest of the collection.

The lightweight dashboard also shows connection health, verified-link count, pending work, skipped/unmatched files, whether automatic linking is running/paused, and the next user action. Update dashboard data automatically without page reloads, preserve in-progress setup choices, and show an animated setup progress bar with an explicitly estimated percentage based on discovery stages and observed series/file totals. Automatic status updates must reuse discovery results rather than repeatedly scanning the entire library. Use plain language for ordinary status (for example, connection failures, waiting for Shoko, or skipped ambiguous matches); keep internal identifiers/state details in authenticated advanced diagnostics. Provide authenticated Pause and Resume. The first visit provides the user-selected username/password account setup wizard; after enrollment every status/control route requires authentication. Do not automatically publish a host port, expose an unauthenticated Internet-facing management route, or mount the Docker socket.

Provide an authenticated Settings area with routine service controls and a collapsed “Show advanced” section for the remaining supported configuration. Persist non-secret preferences across restarts and validate them through the existing configuration rules and automatic-service hard limits. Allow users to review and change selected anime libraries and folder mappings; saving a scope change must pause linking and require fresh, digest-bound scope/safety confirmation, including after a restart. Connection URL changes require a restart and new setup confirmation; API key values remain in runtime environment variables. Persistent database relocation requires an explicit migration, and container networking remains deployment configuration.

**Safety principle:** a skipped file with a clear reason is better than attaching it to an incorrect anime episode.

## 6. Delivery and deployment

- Package as an installable Python project with a simple CLI and focused tests.
- Include a **non-root**, restart-safe Docker image, Unraid-friendly template/Compose example, and persisted `/data` state. No privileged mode, Docker socket, or media-library mounts are needed. Do not publish the dashboard host port automatically; if the operator chooses a local port mapping, dashboard authentication is required after first-use setup.
- Add **GitHub Actions** for lint/tests/build validation and automatic publishing to **GitHub Container Registry (GHCR)**.
- Publish an **immutable SHA-tagged image for each pushed commit**, including commits within multi-commit pushes; publish `latest` only for the newest successfully built default-branch revision. Prevent stale workflows from overwriting a newer `latest`.
- Do not publish containers from untrusted pull requests. Tests must pass before publishing; use least-privilege `GITHUB_TOKEN` permissions.

Core tests should cover root filtering, path boundaries, Anime-Lists rule precedence, split seasons/specials, multi-episode files, existing-link protection, no-op dry runs, async verification, and restart idempotence. Keep the test suite proportional; don't build a large testing framework.

## 7. Out of scope for v1

Media management or downloading; modifying Sonarr; automatically replacing existing Shoko links; filename-only or fuzzy-title-based authoritative matching; automatic AniDB uploads; Plex management; distributed workers; Redis/Celery; and unsolicited file-system scanning.

## 8. Acceptance criteria

The project is ready for deployment when it can:

1. Start the normal container with only the four Sonarr/Shoko connection settings and no required YAML or CLI command; give plain-language errors for invalid/missing connections.
2. Safely discover anime scope and exact file correspondences, ask one clear decision when discovery is ambiguous, and exclude all live-action series and Shoko manual/drop-folder files.
3. Inspect verifiable Shoko safety facts, request explicit one-time user confirmation for nonobservable physical facts, and refuse unsafe/unknown configurations without changing Shoko settings or fabricating consent.
4. After authorization, automatically process a one-file verified canary then safely progress through the existing backlog and new arrivals, with exact target verification, durable budgets, adaptive rate control, restart recovery, no existing-link changes, and no uncertain request replay.
5. Keep working indefinitely without a campaign deadline, handle temporary API failures and mapping updates, retry previously unresolved mappings, and skip one ambiguous/conflicting file without blocking unrelated safe work.
6. Provide a private-by-default authenticated dashboard with status and Pause/Resume, while preserving advanced CLI/YAML behavior and existing SQLite history across upgrade.
7. Run under Docker Compose/Unraid with non-root execution and persistent state, pass the project's checks and Docker smoke tests, and retain the specified GHCR workflow policy.

## 9. Authoritative references and validation notes

- Sonarr API v3: https://sonarr.tv/docs/api/
- Shoko Server source/API: https://github.com/ShokoAnime/ShokoServer (API docs also available from the deployed instance's `/swagger/`)
- Anime-Lists mapping source: https://github.com/Anime-Lists/anime-lists
- GitHub Actions container publishing: https://docs.github.com/en/actions/tutorials/publish-packages/publish-docker-images

**Versions, API shapes, authentication, Anime-Lists interpretation, and asynchronous behavior must be verified against the actual target installations.** This document states desired behavior; it is not evidence that any specific implementation or endpoint already works.
