# Shoko–Sonarr Linker — Project Requirements

> **Purpose of this file:** Explain what this project must do and why. `AGENTS.md` contains development rules; this file describes the product requirements and expected behavior. Keep it accurate as decisions change.

## 1. Problem and goal

Shoko Server usually recognizes anime files through AniDB file hashes. Many files in the existing library are unknown to AniDB even though **Sonarr already knows exactly which TV episodes those files contain**. Manually linking approximately **7,000** unrecognized episodes in Shoko is not viable, and Shoko's GUI filename matcher can select incorrect episode numbers.

Build **`shoko-sonarr-linker`**, a small, continuously running **Python 3.12+** service that uses Sonarr's file-to-episode records and verified TVDB-to-AniDB mappings to associate unrecognized Shoko files with the correct Shoko episodes. It must handle both the initial backlog and newly arriving anime automatically.

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

**Scope isolation is critical:** the same Sonarr instance also manages live-action TV. Only series under explicitly configured **anime root directories** are eligible. Shoko's manually managed folders and unrelated files are out of scope.

## 3. Required behavior

### Initial reconciliation and continuous operation

- **One-shot mode:** inspect the full eligible collection, report proposed links, and optionally apply them.
- **Daemon mode:** remain running, periodically discover new or still-unrecognized Shoko files, and process them once Sonarr and Shoko have finished indexing.
- **Incremental operation:** avoid redoing expensive lookups or resubmitting links on every poll.
- **Full reconciliation:** occasionally rescan eligible files so missed changes, restarts, renames, and mapping updates are recovered.
- **Restart recovery:** use persisted state to resume incomplete work and verify previously submitted link jobs rather than blindly sending them again.
- **Dry-run by default:** applying link changes requires explicit opt-in; dry-run must not make *any* remote changes, including series creation or refresh requests.

### Eligible-series and file selection

1. Fetch Sonarr series and select only those whose configured root folder is in a nonempty **allowlist**.
2. Fetch episodes and episode files **only for those series**; also verify each file path is within an eligible root.
3. Fetch Shoko's unrecognized files and import-folder metadata. Limit Shoko candidates to configured **anime paths**, excluding Shoko-managed drop folders.
4. Translate Sonarr and Shoko paths through explicit, configurable **Docker path mappings**.
5. Match files using their **complete normalized paths** and confirm **file-size equality**. Reject duplicates, unexpected locations, conflicts, and ambiguous matches.
6. Support **several Sonarr episodes associated with one media file** (e.g., an `S01E01-E02` file).

A root match must be **path-component-aware**: `/anime` must not accidentally include `/anime-old`. Missing root allowlists or invalid mappings must fail closed, never expand to the entire Sonarr library.

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
- When refreshed data adds previously missing mappings, automatically reconsider affected unresolved files without waiting for an unrelated full rescan. **Do not silently add preexisting-backlog files to a previously approved campaign.** Changes to previously resolved target mappings must be quarantined for review; existing Shoko associations are never overwritten automatically.

### Shoko linking and verification

- Use **Shoko's supported API**, not direct database changes. Verify endpoint schemas against the deployed Shoko version.
- Link only Shoko files that still qualify as unrecognized and **have no existing protected episode associations**.
- Re-read the current file associations immediately before writing; the linking endpoint may replace existing manual links.
- Shoko may process link requests asynchronously: **HTTP success means accepted, not completed**. Poll/re-read the resulting associations and record success only after verification.
- Do not modify, replace, or “repair” existing links automatically. Report conflicts for review.
- Where the required Shoko anime/episode metadata is missing, optionally create/refresh the series through supported Shoko APIs in **apply mode** and retry once available. Never do this in dry-run mode.

**No media files may be renamed, moved, deleted, rewritten, or opened for modification.** No media-directory Docker mount is necessary.

## 4. Configuration and operations

Configuration should be human-readable YAML with secrets supplied via environment variables. The exact schema is an implementation detail; it must support at least:

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

All paths above are **examples**, not assumptions about the deployed Unraid directory layout. Configuration must validate that every eligible path is mapped unambiguously.

Provide CLI entry points equivalent to:

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

**Safety principle:** a skipped file with a clear reason is better than attaching it to an incorrect anime episode.

## 6. Delivery and deployment

- Package as an installable Python project with a simple CLI and focused tests.
- Include a **non-root**, restart-safe Docker image, Compose example for Unraid, and persisted `/data` state. No privileged mode, Docker socket, exposed web port, or media-library mounts are needed.
- Add **GitHub Actions** for lint/tests/build validation and automatic publishing to **GitHub Container Registry (GHCR)**.
- Publish an **immutable SHA-tagged image for each pushed commit**, including commits within multi-commit pushes; publish `latest` only for the newest successfully built default-branch revision. Prevent stale workflows from overwriting a newer `latest`.
- Do not publish containers from untrusted pull requests. Tests must pass before publishing; use least-privilege `GITHUB_TOKEN` permissions.

Core tests should cover root filtering, path boundaries, Anime-Lists rule precedence, split seasons/specials, multi-episode files, existing-link protection, no-op dry runs, async verification, and restart idempotence. Keep the test suite proportional; don't build a large testing framework.

## 7. Out of scope for v1

Media management or downloading; modifying Sonarr; automatically replacing existing Shoko links; filename-only or fuzzy-title-based authoritative matching; automatic AniDB uploads; Plex management; a web dashboard; distributed workers; Redis/Celery; and unsolicited file-system scanning.

## 8. Acceptance criteria

The project is ready for deployment when it can:

1. Reconcile the existing eligible backlog in **dry-run**, with an understandable explanation for every unresolved file.
2. Safely apply a controlled batch, verify the created Shoko episode links, and leave existing associations and physical media unchanged.
3. Exclude **all** Sonarr live-action series and Shoko manual-drop-folder files by configured scope.
4. Continuously process newly imported eligible anime, refresh Anime-Lists XML automatically on a configurable schedule, retry previously unresolved entries when new mappings become available, and recover correctly from network errors, mapping updates, and container restarts.
5. Run under Docker Compose on Unraid with persistent state and publish tested GHCR images using the specified commit-tag policy.

## 9. Authoritative references and validation notes

- Sonarr API v3: https://sonarr.tv/docs/api/
- Shoko Server source/API: https://github.com/ShokoAnime/ShokoServer (API docs also available from the deployed instance's `/swagger/`)
- Anime-Lists mapping source: https://github.com/Anime-Lists/anime-lists
- GitHub Actions container publishing: https://docs.github.com/en/actions/tutorials/publish-packages/publish-docker-images

**Versions, API shapes, authentication, Anime-Lists interpretation, and asynchronous behavior must be verified against the actual target installations.** This document states desired behavior; it is not evidence that any specific implementation or endpoint already works.
