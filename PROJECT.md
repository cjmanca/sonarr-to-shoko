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

**Scope isolation is critical:** the same Sonarr instance also manages live-action TV. The service selects series under an explicitly confirmed, persisted set of library roots. Sonarr's `Anime`, `Standard`, and `Daily` series types are all supported: the type controls numbering and is not proof that a series is anime. Verified TVDB-to-AniDB episode mappings remain mandatory before linking. Unselected roots, Shoko's manually managed folders, and unrelated files are out of scope. Missing or ambiguous discovery must pause for one clear user decision; it must never silently widen scope.

## 3. Required behavior

### Initial reconciliation and continuous operation

- **Normal service mode:** start automatically in the container, remain running, discover new or still-unrecognized Shoko files, and process them after Sonarr/Shoko indexing and all setup safety gates have passed.
- **One-shot/advanced mode:** retain read-only dry-run as the default; explicit apply retains its existing independent config/environment/attestation gates.
- **Incremental operation:** avoid redoing expensive lookups or resubmitting links on every poll.
- **Full reconciliation:** occasionally rescan eligible files so missed changes, restarts, renames, and mapping updates are recovered.
- **Restart recovery:** use persisted state to resume incomplete work and verify previously submitted link jobs rather than blindly sending them again.
- **Source continuity holds:** retain the first discontinuity's bounded evidence atomically with the hold, including prior/current counts and landed-link accounting. Later polls and restarts must preserve that cause while separately tracking the latest complete observation; legacy holds must honestly report unknown original evidence. The normal dashboard recovery workflow must provide explicit review and confirmation bound to the active hold and fresh complete evidence, then reassess safety before resuming. Local prepare/confirm primitives bind a single-use, expiring review to the hold, full normalized inventory content (including paths and relationships), landed accounting, current consent/authorization/preferences/scope, fresh effective settings and mapping identities, and authenticated account/session ownership. Acceptance atomically changes only the continuity baseline and records a bounded local audit; it grants no write authorization and clears no other pause, canary, uncertainty, or safety gate. External facts require independent trusted controller reassessment and cannot be compared atomically by SQLite. Durable evidence, local acceptance primitives, fresh worker-owned collection, and the normal dashboard review/confirmation workflow are implemented; live deployment validation remains separate.
- **Automatic-write authorization:** the normal service may write only after the user has explicitly confirmed the selected scope and one-time safety/setup consent. API keys alone, old config flags, an old lease, or a campaign approval do not imply consent to broader automatic linking. Before consent, setup and monitoring are read-only.
- **Dry-run invariant:** any advanced dry-run makes no remote changes, including series creation or refresh requests.

### Eligible-series and file selection

1. Discover Sonarr series with known `seriesType` values (`anime`, `standard`, or `daily`) and select them only under a confirmed, nonempty root allowlist. Offer roots containing any supported type for explicit selection; never auto-approve an unselected root. Unknown, missing, or malformed types remain excluded. If multiple plausible roots exist, ask the user to select the intended scope.
2. Fetch episodes and episode files **only for selected series**; also verify each file path is within an eligible root. Every file still requires valid Sonarr episode identities, a positive TVDB series ID, and complete verified TVDB-to-AniDB mappings before linking; do not infer identities from filenames or air dates.
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
- Shoko compatibility is determined by the required API v3 endpoints and response contracts, not an exact server release allowlist. Accept version labels such as `5.3.3.0` and other releases when those contracts remain compatible; reject missing or incompatible API responses before writing. Continue recording the observed version in setup consent so a changed server installation prompts fresh confirmation.
- Link only Shoko files that still qualify as unrecognized and **have no existing protected episode associations**.
- Re-read the current file associations immediately before writing; the linking endpoint may replace existing manual links.
- Shoko may process link requests asynchronously: **HTTP success means accepted, not completed**. Poll/re-read the resulting associations and record success only after verification.
- The linker must not directly modify, replace, or “repair” existing links automatically. Report conflicts for review. Explicit optional metadata-maintenance consent is the only normal-service exception for Shoko-native same-anime maintenance; protected existing-association checks on file linking remain mandatory.
- Where required Shoko anime/episode metadata is missing, normal service may request supported Shoko metadata retrieval only when the ordinary `allow_missing_metadata_refresh` preference is enabled (default **off**) and fresh scope-bound consent explicitly accepts Shoko-native maintenance. Fully verify selected Sonarr file demand and mappings before requesting metadata; a missing-cache skip alone is insufficient. This may repair existing database episode/file associations and run rename/move/processing jobs, plugins or hooks for the same anime, including the separate manual collection. Read-only mounts protect media, not database associations. The linker still never directly modifies media or bypasses existing-link protection.
- Missing-metadata permission is separate from physical-safety confirmations and bound into consent as `missing_metadata_maintenance_v1`. Changing the preference in either direction atomically saves the choice and requires fresh setup consent. A true preference alone cannot authorize requests; stale forms/checks cannot restore permission or clear a newer hold. Disabling does not cancel accepted Shoko jobs. Off leaves unrelated already-resolved safe linking available.
- Request outstanding missing-series metadata together in a bounded batch, only after a complete queue observation verifies that all Shoko jobs (including unrelated waiting, running and blocked jobs) are absent. Check emptiness once immediately before admitting the batch; accepted requests from that batch may enqueue work without stopping its remaining requests. Metadata uses the configured hard cycle allowance remaining after ready file links, independently of the adaptive file-link throughput cap and the absence of a file canary. Existing pending/failed canary safety holds still apply. Shared durable rolling/pending budgets, deadlines, current demand/mapping/consent checks and uncertain-delivery protection remain mandatory; stop a batch on uncertain/refused delivery or a lost safety guard. Work beyond these bounds waits for a later service cycle; mapping XML refresh scheduling does not schedule metadata retries.
- Deduplicate supported metadata work against verified queued/running Shoko jobs and durable local request state. Unknown queue visibility refuses new metadata requests. HTTP acceptance is not completion: await verified target metadata and never blindly replay uncertain requests. No global exactly-once guarantee is made across unrelated clients.
- Advanced dry-run/apply and campaign authorization remain independent; the normal-service preference does not enable advanced metadata writes or relax existing gates. Never create/retrieve metadata during dry-run.

### Normal automatic processing

- The ordinary service maintains a persistent queue for both the existing backlog and new arrivals. It reuses frozen file identities, immutable mapping targets, durable SQLite sends/budgets, protected-link preflight, asynchronous verification, restart recovery, and duplicate-submission prevention.
- After explicit setup authorization and all current safety checks pass, the service submits an initial one-file batch and verifies the complete target association before increasing throughput. No manual campaign preparation, approval digest, canary command, drain authorization, or deadline calculation is required in normal service mode.
- Work does not expire merely because it takes longer than seven days. Individual ambiguous, conflicting, or out-of-scope files are skipped with durable reasons while unrelated safe work continues.
- Keep durable per-cycle, rolling-window, pending-file, pending-episode-job, wall-time, and recovery ceilings. Start file-link throughput at one submission and increase gradually only when Shoko's queue, pending jobs, verification, API responses, and anomaly signals show healthy operation. Reduce or stop on congestion, failure, throttling, mapping/source drift, or safety uncertainty. The initial practical ceilings are 20 submissions per cycle, 5,000 per rolling week, and 10 pending episode jobs; all remain hard limits, never bypassed by adaptation.
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
- Normal service supports a separate Shoko-managed manual collection with global `MoveOnImport`, `RenameOnImport`, or `RelocateOnImport` enabled. Every selected Sonarr target must belong unambiguously to a usable Shoko import folder with `Drop Type: None` (excluded from renaming/moving), with complete, known import-folder context and no overlapping ownership. Every location of a candidate file must still be in an eligible, mapped `None` folder. The exclusion behavior is source-verified against Shoko 5.3.3; it is not an exact-release allowlist or a claim of live validation on other releases (see [compatibility evidence](docs/compatibility.md#excluded-folders-and-mixed-collections)).
- `FileQualityFilterEnabled` and `AniDb.MyList_AddFiles` remain independent blockers when enabled: folder exclusion does not prevent deletion of other episode files or external MyList writes. Missing settings require explicit manual confirmation: the selected folders are excluded from renaming/moving for unavailable renamer flags, and each unavailable hard-blocking setting is disabled.
- Setup must explicitly require the operator to confirm that selected Sonarr media is mounted read-only **inside Shoko**, with no writable aliases or writable hardlink exposure, and that plugins/external hooks cannot rename, move, delete, or rewrite **any media on link events**. The separate manual import workflow may remain enabled. The API cannot verify these physical/hook facts; a read-only linker container or a read-only source mount alone does not prove safety.
- The setup confirmation explicitly authorizes automatic linking only for the displayed, selected scope after the user verifies any facts that cannot be observed through the API. Persist a confirmation digest bound to the Sonarr/Shoko endpoints, Shoko version/capabilities, roots, path mappings, safety-policy revision, sanitized observed settings, and selected Shoko folder IDs, normalized paths, and types. Re-check observable facts at startup and before each write; selected-folder drift, settings drift, or older consent invalidates authorization and pauses automatic writes until the user confirms again. Unrelated valid manual-folder changes must not invalidate consent when selected ownership remains unambiguous.
- Do not change Shoko configuration, invent physical-media facts, create fictitious attestation records, or reinterpret `sync.apply_changes`, `SHOKO_LINKER_ALLOW_APPLY`, old leases, campaigns, approval digests, or drain grants as consent for this service mode.
- Legacy one-shot apply and campaign CLI commands retain their existing gate (`sync.apply_changes`, `SHOKO_LINKER_ALLOW_APPLY=1`, and the applicable valid attestation/approval). Automatic service authorization is a separate durable onboarding state.
- The scoped folder policy above applies to normal service only. Advanced one-shot/campaign apply retains its independent offline operator-attestation gate: the lease requires boolean `FileQualityFilterEnabled`, `RenameOnImport`, `MoveOnImport`, and `MyListAddFiles` fields, and explicitly refuses `FileQualityFilterEnabled=true`; it does not require the other three recorded flags to be false or record `RelocateOnImport`. A valid lease is not automatic verification of Shoko settings or physical safety. The [advanced deployment safety checklist](docs/advanced-deployment.md) still requires the operator to disable global renaming/moving and MyList writes and verify mounts and plugins before attesting.

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

Dashboard Waiting and Needs review counters describe durable local linker work across scans, not Shoko's job queue or its current unrecognized-file total. Their paginated filename/path rows must use the same classification as their counters, show plain reasons, and state the displayed range and full group total. Latest scan exclusions are a separate scan result, with their own total and bounded examples. Show the last completed scan's observation interval, Shoko-reported total when available, unique observed files, selected-Sonarr matches, exclusions, and the outside-scope subset of exclusions. Observed files partition into matches and exclusions; a Sonarr match is not a verified link. A paged scan may observe a different number than Shoko reports while its list changes. Suppress scan totals when the current source/scope cannot be established; missing scan evidence is unavailable, never an invented zero. Ordinary status reads reuse persisted evidence and perform no remote reads or mutations.

Recent linker actions show bounded retained request history, with its full retained total, plain outcomes, recorded timestamps, and available filename/path context. Requested is the retained attempt time, or local preparation time while not attempted; it never proves delivery. Updated is the latest recorded event for that request, including inspection or operator resolution, independent of later file activity. Legacy records show only retained per-request times; unavailable dates remain unavailable. Each newly prepared metadata claim has its own preparation time, preserving archived generations. Include retained server sources and modes explicitly: the file request ledger does not retain an immutable endpoint identity, and displayed paths are current retained context, not historical snapshots. Distinguish locally prepared, possibly sent, accepted, verified, rejected, uncertain, and operator-resolved records without claiming delivery or success from preparation or an HTTP response alone. Verified metadata availability does not prove the request caused it or that a file was linked. External queue observations are not linker requests. This history is not a Shoko filesystem, plugin, or hook audit; absence of request records does not prove absence of those effects. Preserve independently opened and closed file/request disclosures during automatic status updates without changing setup choices.

Across the dashboard, structured timestamps within the previous or upcoming 24 hours show only `HH:mm` (24-hour clock); times exactly 24 hours away or farther show `YYYY-MM-DD`. Use the browser's local timezone with a labelled UTC server-rendered fallback. Keep the full timestamp and timezone accessible in timestamp details, update compact dates during normal polling, and leave machine output and raw technical diagnostics unchanged.

**Safety principle:** a skipped file with a clear reason is better than attaching it to an incorrect anime episode.

Use authenticated server-rendered routes with a persistent left navigation: Overview (`/status`) for health, progress and next action; Issues (`/issues`) for tracked review files with a separate latest-scan-exclusions view; Verified (`/files/verified`) and Waiting (`/files/waiting`) for their exact tracked groups; History (`/history`) for recorded mutating requests; and Settings (`/settings`). History must not substitute verified-file inventory. The shared header has compact, colored, labelled links to Verified, Waiting and Needs review and exactly one Pause or Resume icon button, determined by the persisted pause flag rather than inferred from worker activity. Distinguish user pauses from service pauses in the status text. Resume remains available for recovered transient service pauses; every Resume still reevaluates existing authorization and may refuse a safety/setup hold without changing its ownership. Table rows and their header counts share the same local snapshot. Tables provide native pagination and accessible type icons, filenames, relative directories and confirmed root paths as library names; unavailable scope/path evidence remains unavailable. Reject unsupported, duplicate or oversized table query parameters. Routes and pagination work without JavaScript; automatic refresh uses the current route and query. Keep setup choices and existing authentication, CSRF, redaction, and private deployment boundaries.

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
2. Safely discover the explicitly selected Sonarr library scope and exact file correspondences across Anime, Standard, and Daily numbering types, ask one clear decision when discovery is ambiguous, and exclude unselected roots, unmapped series, and Shoko manual/drop-folder files.
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
