# Compatibility and contract notes (M0)

Short, evidence-backed record of upstream contracts this project depends on.
Confidence labels: **CONFIRMED** = verified against a cited primary source at the
stated ref; **UNVERIFIED** = installation-specific, not proven here. No speculative
API shapes are encoded.

## 1. Sonarr API v3 (reads only)

Verified refs: `Sonarr/Sonarr` branch heads `v5-develop` (`da990630…`) and `develop`
(`cab419ad…`), historical commit `7297c1b8e4` (2017 v3-era), plus the committed
`src/Sonarr.Api.V3/openapi.json` at both heads. The v3 surface is identical at both
heads for every endpoint we use (162 paths, same params/schemas).

### Auth and base URL

- API key via header `X-Api-Key: <key>`; query `?apikey=<key>` also accepted, checked
  **before** the header; `Authorization: Bearer <key>` fallback. 401 on failure.
  API auth is mandatory (fallback policy on scheme `"API"`).
  Source: `src/Sonarr.Http/Authentication/AuthenticationBuilderExtensions.cs`,
  `ApiKeyAuthenticationHandler.cs`, `src/NzbDrone.Host/Startup.cs` (v5-develop).
- URLs are **lowercase** and relative to an optional URL base: with `urlBase=/sonarr`,
  the endpoint is `/sonarr/api/v3/series`. Requests to the bare path without the base
  get **HTTP 307** redirect to `{urlBase}{path}`.
  Source: `Startup.cs:68`, `src/Sonarr.Http/Middleware/UrlBaseMiddleware.cs:18-29`.
- Instance identity/config is discoverable at `GET /api/v3/system/status`
  (`version`, `urlBase`, `instanceName`, `isDocker`, `branch`) — use this for instance
  identity, not an invented endpoint. `rootFolderPath` is **computed** server-side
  (never stored), so it is always present on reads.

### JSON conventions (parsing-relevant)

Source: `src/NzbDrone.Common/Serializer/System.Text.Json/STJson.cs` + `Startup.cs` (v5-develop):

- camelCase keys; case-insensitive on read.
- **Null properties are omitted** (`DefaultIgnoreCondition = WhenWritingNull`) —
  absence of `absoluteEpisodeNumber`, `mediaInfo`, etc. means null.
- Enums serialize as camelCase strings (`seriesType: "anime"`, `releaseType: "multiEpisode"`).
- `id` is omitted when `0` (`RestResource`, `JsonIgnore WhenWritingDefault`).

### Endpoints used

| Endpoint | Returns | Notes |
| --- | --- | --- |
| `GET /api/v3/series` | full JSON array of `SeriesResource` (no paging) | Fields we rely on: `id`, `title`, `path`, `rootFolderPath`, `tvdbId` (TVDB **series** id), `seriesType` (`standard`/`daily`/`anime`), `seasons[].seasonNumber`, `statistics`, `tags`, `monitored`. List and detail share the shape. |
| `GET /api/v3/episode?seriesId={id}` | full array of `EpisodeResource` (no paging) | Requires `seriesId` or `episodeIds` (else 400). Per-episode fields: `id`, `seriesId`, `tvdbId` (TVDB **episode** id, added 2022-02-27 commit `b24bea41…`), `episodeFileId` (0 when fileless), `seasonNumber`, `episodeNumber`, `absoluteEpisodeNumber` (nullable; anime-relevant), `title`, `hasFile`, `monitored`. |
| `GET /api/v3/episode?episodeFileId={id}` | all episodes of one file | **This is how file→episode groups are resolved.** `EpisodeFileResource` has **no `episodeIds`** field (verified in `EpisodeFileResource.cs` v5-develop, develop, 2017 file, v3 OpenAPI, V5 API, UI type `Episode.ts`; controller `EpisodeController.GetEpisodes` handles `episodeFileId` via `GetEpisodesByFileId`). |
| `GET /api/v3/episodefile?seriesId={id}` | full array of `EpisodeFileResource` (no paging) | Fields: `id` (= `episode.episodeFileId` join key), `seriesId`, `seasonNumber`, `relativePath`, `path` (server-computed `series.path + relativePath`), `size` (int64), `dateAdded`, `releaseType` (`singleEpisode`/`multiEpisode`/`seasonPack`), `quality`, `mediaInfo`. Requires `seriesId` or `episodeFileIds` (else 400). |
| `GET /api/v3/rootfolder` | array of `RootFolderResource` | `id`, `path`, `accessible`, `freeSpace`, `unmappedFolders`. Used to validate configured allowed roots. (No `/api/v3/root` endpoint.) |

**Multi-episode files:** one file ↔ many episodes is confirmed at the data layer
(`EpisodeFile.Episodes` lazy-load by `Episode.EpisodeFileId == EpisodeFile.Id`,
`TableMapping.cs:121-133`). The association values are **episode record `id`s** —
not TVDB ids, not a separate "episodeId". A grouped record has no single `tvdbId`:
each episode row carries its own. **No `episodes` sub-array exists** on the v3
episode resource (verified across refs + UI type).

**No pagination** on `/series`, `/episode`, `/episodefile`, `/rootfolder`,
`/calendar` — they return complete arrays. No server-side rate limiting found
(Startup pipeline, csproj greps, spec: no 429/throttle). Deployment-level
limits are **UNVERIFIED**.

**Version sensitivity:** v3 applies across Sonarr 4.x–6.x for the endpoints above.
Sonarr now also exposes `/api/v5/*` (renamed include flags, `aniListIds`/`malIds`,
dropped `languageProfileId`); we target **v3** and treat v5 as out of scope.
Minor drift: `episode.endTime`/`grabDate` present on `develop` C#, absent on
`v5-develop` C# (spec lists them) — runtime presence is **UNVERIFIED**.

## 2. Shoko API v3 (v5.3.3)

Verified refs: `ShokoAnime/ShokoServer` tag **v5.3.3** = commit
`892461188e5f1cc3ccf1b9b6eb1d4dfd31bea6f8`; every cited file was fetched at that
ref and **blob-SHA-verified** against the tree; orchestrator independently re-checked
`ManualLinkJob.cs`, `ListResult.cs`, `APIExtensions.cs`, `CustomAuthHandler.cs`.
Zero network calls to any live Shoko; zero writes.

### Auth, base path, wire format

- API key via **header `apikey`** or **query `?apikey=`** (exact name `apikey`;
  header and query union-checked). Valid key → `user` role; `admin` role iff
  `user.IsAdmin == 1`. Fail → 401. Base path `/api/v3/...`
  (`CustomAuthHandler.cs:46-71`, controllers' `[Route("/api/v{version}/[controller]")]`).
- **JSON wire names are PascalCase = C# property names** (Newtonsoft
  `DefaultContractResolver` + `DefaultNamingStrategy`; `NullValueHandling.Include`;
  `DefaultValueHandling.Populate`; ISO-8601 dates; enums stringified by
  **per-property** `[JsonConverter(StringEnumConverter)]`)
  (`APIExtensions.cs:182-191`). Opposite convention from Sonarr — parsers must be
  per-service.

### Version and startup compatibility

- At the pinned v5.3.3 ref, **`GET /api/v3/Init/Version`** is implemented by
  `InitController.GetVersion` and returns `ComponentVersionSet.Server.Version`
  (with optional `ReleaseChannel`, `Commit`, `Tag`, and `ReleaseDate`). The response
  shape is `Server.Version`; do not assume a top-level version field. This read-only
  endpoint provides Shoko's server version without requesting the secret-bearing
  admin-only `/Settings` document. Source: `InitController.cs` at
  [`892461188e5f1cc3ccf1b9b6eb1d4dfd31bea6f8`](https://github.com/ShokoAnime/ShokoServer/blob/892461188e5f1cc3ccf1b9b6eb1d4dfd31bea6f8/Shoko.Server/API/v3/Controllers/InitController.cs),
  `ComponentVersion.cs`, and `ComponentVersionSet.cs` at the same ref.
- **`GET /api/v3/Init/Status`** returns `ServerStatus` with startup state, startup
  message, uptime, and database-blocked state. It is a read-only availability signal;
  do not call `POST /api/v3/Init/StartServer` from the linker.
- The source audit below establishes the **v5.3.3** contract baseline, not a runtime
  release allowlist. Setup accepts any nonempty `Server.Version` reported by the
  required v3 metadata endpoint, including `5.3.3.0`. Discovery, queue, episode
  resolution and protected preflight independently validate their response contracts
  and refuse missing/incompatible data before writing. This does not claim that
  every release has been live-tested or that metadata alone proves all capabilities.
  The observed version remains bound to setup consent; version changes require fresh
  confirmation. Missing safety settings still require the existing operator checks.

### `GET /api/v3/File` — listing unrecognized files

- Params: `pageSize` (default **100**, max 1000, `0`=unlimited), `page` (default 1),
  `include` (`Ignored, MediaInfo, XRefs, AbsolutePaths, ImportLimbo`),
  `exclude` (`Watched, Variations, Duplicates, Unrecognized, ManualLinks`),
  `include_only` (`Watched, Variations, Duplicates, Unrecognized, ManualLinks,
  Ignored, ImportLimbo`), `sortOrder` (list, `-` prefix reverses),
  `includeDataFrom` (`None, AniDB, TvDB, TMDB, Trakt, MAL, AniList, Animeshon,
  Kitsu, Shoko`).
- **`include_only=Unrecognized` means exactly: file has zero episode cross-references**
  (`ModelHelper.cs:536-537`). Ignored and import-**limbo** files (cross-refs with
  unresolvable AniDB anime/episode) are **excluded by default**; re-include via
  `include=Ignored` / `include=ImportLimbo`. Limbo detection for us:
  `xrefs.Count>0 && any xref AniDB anime/episode null`.
- **Paging envelope = `ListResult`** with exactly **`Total`** + **`List`** (no
  Page/PageSize fields; `ListResult.cs:46-51`). `Total` is recomputed per request via
  offset `Skip/Take` (`ModelHelper.cs:48-105`) — **no snapshot isolation**: concurrent
  inserts (including our own links) shift rows between pages. Default file sort:
  `ImportFolderName`, `RelativePath`. Duplicate/missing-ID and changing-`Total`
  detection are our responsibility (M3).
- Hash lookup exists: `GET /api/v3/File/Hash/ED2K?hash=&size=` (+CRC32/MD5/SHA1) —
  useful for exact size/identity cross-checks.

### `File` model (API v3 `Shoko.Server/API/v3/Models/Shoko/File.cs`)

Top-level: `ID` (int = `VideoLocalID`), `Size` (long), `IsVariation`, `IsIgnored`,
`Hashes{ED2K,SHA1,CRC32,MD5}`, `Locations[]`, `AVDump`, `Resolution`, `Duration`,
`ResumePosition`, `Viewed`/`Watched`, **`Imported`** (`DateTime?`; null = never
imported), `Created`, `Updated`, `SeriesIDs[]` (**present only with `include=XRefs`**),
`AniDB` (only `includeDataFrom=AniDB`), `MediaInfo` (only `include=MediaInfo`).
**Absent at v5.3.3 (legacy v1/v2 names):** `Hash`, `Priority`, `SufficientlyHashed`,
`LastFileImported`, tolerant-path/drop-folder location fields — do not use them.

- **`Imported` (`DateTimeImported`) is diagnostic only.** `ProcessFileJob` sets it once a
  file is successfully identified through AniDB, so a fully hashed file whose hashes are
  unknown to AniDB keeps `Imported=null` **indefinitely** — hashed, stable, zero
  cross-references. That is exactly the unrecognized backlog this linker links, so
  **candidacy never depends on `Imported`** (a null is *not* "incomplete", and a
  non-string/non-null garbage value stays a malformed-file failure at parse time).
  Eligibility = zero episode cross-references + at least one available hash (an absent
  `Hashes` key / null hashes are a recorded detail and exclude the file as
  `no_hashes` — identity not settled; a present-but-garbage `Hashes` object is a
  parse-time malformed-file failure) + all locations passing strict
  ownership + exact Sonarr path+size
  correspondence + a valid AniDB→Shoko episode mapping + complete/consistent snapshots
  (and, at write time, fresh preflight re-checks these; grace periods apply).

- `Locations[]` element: `ID` (`VideoLocal_Place_ID`), `FileID`, `ImportFolderID`,
  `RelativePath` (relative to import folder), `AbsolutePath` (requires
  `include=AbsolutePaths`), `IsAccessible`. **One file = N locations** confirmed
  (one `VideoLocal` per ED2K+size with N `VideoLocal_Place` rows;
  `SVR_VideoLocal.cs:78`) — **every** location must pass ownership checks (M3).
- `SeriesIDs[]` element = `FileCrossReference`: `SeriesID{ID (int? — Shoko internal
  `AnimeSeriesID`, **null if no local series entry**), `AniDB` (int, AniDB **anime**
  ID), `TMDB}`; `EpisodeIDs[]` = `EpisodeCrossReferenceIDs{ID (int? — Shoko internal
  `AnimeEpisodeID`, **null when the AniDB episode has no Shoko metadata yet** —
  confirmed), `AniDB` (int = **AniDB episode ID**), `TMDB`, `ReleaseGroup`, `ED2K`,
  `FileSize`, `Percentage{Start,End,Size,Group}`, `Source` (`"AniDB"|"User"`).
  XRefs carry **no AniDB type/number** — resolve via AniDB episode endpoints below.

### `GET /api/v3/File/{fileID}`

- Accepted `include` at v5.3.3: **`XRefs`, `MediaInfo`, `AbsolutePaths` only** —
  **no `All` flag**. Preflight for a link = `File/{id}?include=XRefs,AbsolutePaths`.

### `POST /api/v3/File/{fileID}/Link` (the only write endpoint we use)

- Body: **`{"EpisodeIDs":[<int>...]}`** — **Shoko internal episode
  IDs** (`AnimeEpisodeID`); NOT AniDB anime/episode IDs (`FileController.cs:991-1031`).
- Auth: any valid apikey (class-level `[Authorize]`; not admin-gated).
- Pre-check `CheckXRefsForFile`: file with **any AniDB-source xref → ValidationProblem,
  zero writes** (preexisting AniDB associations protected).
- **Replace semantics:** `RemoveXRefsForFile` deletes **all User-source xrefs** and
  resets `DateTimeImported` before linking — so an existing *manual* link is
  replaced. Never call when *any* existing link is present (fresh
  `include=XRefs` read immediately before the POST; race against other Shoko clients
  remains documented, not closable).
- Enqueues `ManualLinkJob` **per episode** (`StartJobNow`, priority 10) and
  returns `Ok()` **before jobs run** — 2xx = accepted, not verified.
- Siblings we do NOT use: `POST File/{id}/LinkFromSeries`, `POST File/Link`
  (multi-file partial links w/ Percentage), `DELETE File/{id}/Link`.

### `ManualLinkJob` side effects (v5.3.3; ordered; **media-risk chain**)

1. Writes User-source `CrossRef_File_Episode` {Hash, FileName, FileSize, AnimeID,
   EpisodeID=**AniDB episode ID**, Percentage, EpisodeOrder=1}.
2. **FileQualityFilter** (gated `FileQualityFilterEnabled`, **default false**): ranks
   all files of the episode, keeps top `FileQualityPreferences.MaxNumberOfFilesToKeep`
   (default 1), **protects the just-linked file**, then **physically deletes** the
   other files' places (`RemoveRecordAndDeletePhysicalFile`). Files with missing AniDB
   info are kept unless `AllowDeletingFilesWithMissingInfo`.
3. Sets `DateTimeImported=now` on the VideoLocal (moves file out of "never imported").
4. Sets `EpisodeAddedDate` on the series + all `AllGroupsAbove`; queues stats update.
5. **`OnFileMatched` plugin event** → third-party plugins can rename/upload/react.
6. **AniDB MyList write** if `AniDb.MyList_AddFiles` (**default TRUE** — live
   external AniDB write on every link!).
7. **Physical rename/move** via `RenameMoveFileJob` if
   `Plugins.Renamer.RelocateOnImport` (= `MoveOnImport || RenameOnImport`,
   defaults false). `RelocateOnImport` **is serialized** by the API/Newtonsoft (the
   `[JsonIgnore]` is System.Text.Json and ineffective here) — read
   `RenameOnImport`/`MoveOnImport` directly; do not rely on its absence.

`ImportAsNewEp`: **absent at v5.3.3.**

### Excluded folders and mixed collections

The normal service permits global `MoveOnImport`, `RenameOnImport`, and
`RelocateOnImport` for a separate manual collection when all selected Sonarr
targets belong unambiguously to usable `DropFolderType=None` folders. Evidence
from Shoko **5.3.3**:

- The API maps a folder with both drop flags false to `None`
  ([ImportFolder.cs](https://github.com/ShokoAnime/ShokoServer/blob/v5.3.3/Shoko.Server/API/v3/Models/Shoko/ImportFolder.cs#L53-L74)).
  This represents the renamer's excluded folder type;
  [RenameFileService.cs](https://github.com/ShokoAnime/ShokoServer/blob/v5.3.3/Shoko.Server/Renamer/RenameFileService.cs#L38-L60)
  returns for excluded folders before invoking the renamer.
- [RenameMoveFileJob.cs](https://github.com/ShokoAnime/ShokoServer/blob/v5.3.3/Shoko.Server/Scheduling/Jobs/Shoko/RenameMoveFileJob.cs#L45-L50)
  handles **all places** of the file. Therefore every candidate location must
  still be in an eligible, mapped `None` folder; one excluded location is insufficient.
- [VideoLocal_PlaceService.cs](https://github.com/ShokoAnime/ShokoServer/blob/v5.3.3/Shoko.Server/Services/VideoLocal_PlaceService.cs#L130-L151)
  also guards excluded folders. Other move branches can delete a destination
  before a move from a read-only source fails
  ([lines 267–281](https://github.com/ShokoAnime/ShokoServer/blob/v5.3.3/Shoko.Server/Services/VideoLocal_PlaceService.cs#L267-L281)).
  Read-only mounts alone are not a universal bypass for folder exclusion.
- Folder exclusion does not contain all
  [ManualLinkJob side effects](https://github.com/ShokoAnime/ShokoServer/blob/v5.3.3/Shoko.Server/Scheduling/Jobs/Shoko/ManualLinkJob.cs#L78-L122):
  quality filtering can delete other files and plugins can react to link events.
  `FileQualityFilterEnabled` and `AniDb.MyList_AddFiles` remain independent blockers
  when enabled, regardless of folder type or mount protection.

Normal setup requires complete, known import-folder context, no overlapping selected
ownership, and explicit confirmation that selected media is read-only **inside
Shoko**, without writable aliases/hardlink exposure, and that plugins/external hooks
cannot modify any media on link events. The separate manual import workflow may
remain enabled. These physical and hook facts cannot be API-verified. Unavailable
renamer settings require manual confirmation of selected-folder exclusion;
unavailable quality-filter/MyList settings require confirmation that they are disabled.

Consent binds the safety-policy revision, sanitized settings, and selected folder
IDs, normalized paths, and types alongside the existing scope/version binding.
Selected-folder or settings drift and older consent require renewed confirmation;
unrelated valid manual-folder changes do not when selected ownership stays
unambiguous. This is a source-verified baseline, not a release allowlist or live
validation of arbitrary versions. Advanced one-shot/campaign apply retains its
independent offline operator-attestation gate. Its lease requires boolean
`FileQualityFilterEnabled`, `RenameOnImport`, `MoveOnImport`, and `MyListAddFiles`
fields and refuses `FileQualityFilterEnabled=true`; it accepts either boolean for
the other three flags and has no `RelocateOnImport` field. Lease validity does not
automatically verify settings or physical safety. Operators must still follow the
[advanced deployment safety checklist](advanced-deployment.md), including disabling
global renaming/moving and MyList writes and verifying mounts and plugins.

### Settings / Queue — read-only safety inputs

- `GET /api/v3/Settings`: **admin+init only**; returns **full server config including
  secrets** (`AniDb.Username/Password/AVDumpKey`, TMDB/Trakt keys, DB settings) —
  **parse only required booleans, never log/store raw**. Safety-relevant serialized
  names: `FileQualityFilterEnabled`, `FileQualityPreferences.MaxNumberOfFilesToKeep`/
  `.AllowDeletingFilesWithMissingInfo`/`.AllowDeletionOfImportedFiles`,
  `Plugins.Renamer.RenameOnImport`/`.MoveOnImport`/`.RelocateOnImport`/
  `.AllowRelocationInsideDestinationOnImport`, `AniDb.MyList_AddFiles`,
  `Import.AutomaticallyDeleteDuplicatesOnImport`, `Import.MaxAutoScanAttemptsPerFile`,
  `Import.RunOnStart`/`ScanDropFoldersOnStart`. `PATCH /Settings` (writes) —
  **never**.
- Queue (read-only): `GET /Queue` (any user: `WaitingCount, BlockedCount, TotalCount,
  ThreadCount, CurrentlyExecuting[]`), `GET /Queue/Types` (type→count map),
  `GET /Queue/Items` (**admin**; `QueueItem{Key,Type,Title,Details,IsRunning,
  StartTime,IsBlocked}`). **Never call `POST Queue/Pause|Resume|Clear`.** Poll link
  completion via `Queue/Types` counts + `File/{id}?include=XRefs`; job history is not
  persisted.
- **Never invoke `Action` endpoints** (`RunImport`, `ImportNewFiles`,
  `RemoveMissingFiles`, …) — they trigger imports/deletions.

### Series/episode identity resolution (Shoko internal IDs)

- `GET /api/v3/Series/AniDB/{anidbID}/Series` → 404 when Shoko has no series entry
  (requires AniDB anime cached). `GET /api/v3/Series/AniDB/{anidbID}` →
  `AnidbAnime{ID, ShokoID (int? = internal series ID if materialized), Type, Title,
  EpisodeCount,…}`.
- `GET /api/v3/Series/AniDB/{id}/Episode` → `ListResult<AnidbEpisode>` (no Shoko
  series entry required). `AnidbEpisode{ID (AniDB episode ID), AnimeID, Type,
  EpisodeNumber, AirDate, Title, Titles, Description, Rating}` — **contains no Shoko
  IDs** (the "ShokoSeriesID/ShokoEpisodeID fields" claim is UNVERIFIED → absent).
- At the pinned [v5.3.3 `AnidbEpisode` DTO](https://github.com/ShokoAnime/ShokoServer/blob/v5.3.3/Shoko.Server/API/v3/Models/AniDB/AnidbEpisode.cs),
  `Type` is copied from `ep.AbstractEpisodeType.ToV3Dto()` and `EpisodeNumber` from
  `ep.EpisodeNumber`; the [AniDB series listing route](https://github.com/ShokoAnime/ShokoServer/blob/v5.3.3/Shoko.Server/API/v3/Controllers/SeriesController.cs)
  filters by the requested wire `type`. Therefore Anime-Lists' synthetic `T1=201`
  and `O1=401` codes are **not** Shoko `EpisodeNumber` values: resolve them as
  `(Trailer, 1)` and `(Other, 1)`, respectively.
- `EpisodeType` wire names (v3): **`Unknown, Other, Episode, Special, Trailer,
  Credits, Parody`** (NOT AniDB UDP codes `ep/cred/par/spe`).
- Resolution chain `(AniDB anime ID, type, number) → Shoko internal episode ID`:
  `Series/AniDB/{animeID}/Episode?type={Type}` → match `EpisodeNumber==n && Type==t`
  → take `ID` (AniDB ep ID) → `GET /api/v3/Episode/AniDB/{anidbEpisodeID}/Episode` →
  Shoko `Episode.IDs{ID = internal AnimeEpisodeID, …}`. **404 ⇒ episode not
  materialized in Shoko yet** (pending-metadata state; M5/M7 campaign semantics).
- `GET /api/v3/Series/{seriesID}/Episode` → `ListResult<Episode>` with filters
  (`includeMissing/includeUnaired/includeHidden/includeVoted/includeWatched/
  includeManuallyLinked` as `false|true|only`, `type`, `includeFiles/includeXRefs/
  includeMediaInfo/includeAbsolutePaths`, pageSize default **20**, max 1000).

### Import folders / drop types

- `GET /api/v3/ImportFolder` (any user): `ID, Name, Path, WatchForNewFiles,
  DropFolderType, Size, FileSize`. **`DropFolderType` = `None | Source |
  Destination | Both`** (flags) — legacy `Manual/Video/Archive` naming absent.
  A file location's drop type = its `ImportFolderID`'s folder type; locations carry
  no drop fields of their own (M3 ownership: all locations' folders must be
  `None`-type Sonarr-owned paths).

### Pagination/limits/absence facts

- No rate limiting, no `Retry-After`, no server-side API timeouts in the Shoko API
  pipeline; outbound AniDB rate limits apply to Shoko, not us. Client-side
  timeouts/backoff are ours.
- No `API_ImportQueueEntry` model in v3 (v2-only). Import pipeline is Quartz jobs
  visible via `Queue/Types` (`ImportJob/DiscoverFileJob/HashFileJob/ProcessFileJob`).

### Version sensitivity

- Current `master` (`80f1a7a6…`) **diverges**: `GET File` loses `includeDataFrom`;
  **`POST File/{id}/Link` no longer enqueues `ManualLinkJob`** (routes through the
  release pipeline; `ManualLinkJob.cs` deleted; `LinkFromSeries` `[Obsolete]`).
  Master's link side-effect profile is **UNVERIFIED** — this project pins **v5.3.3**
  semantics and must re-verify before supporting newer servers.
- Shoko v6 (`Shokodisc`) is a separate rewrite; out of scope.

## 3. Anime-Lists mapping contract

README semantics and the pinned real-data examples below were rechecked against
`Anime-Lists/anime-lists` commit
[`996d6a8b98c0c8ba94378875746a83b66dc590d4`](https://github.com/Anime-Lists/anime-lists/commit/996d6a8b98c0c8ba94378875746a83b66dc590d4)
(2026-10-14). Other XML inventory and HTTP measurements below were recorded on
2026-10-08 for `anime-list-full.xml` (1,715,347 bytes, SHA-256
`492B080C17…`, 10,771 `<anime>`).

### Source, cadence, provenance, fetch

- Canonical programmatic URL:
  `https://raw.githubusercontent.com/Anime-Lists/anime-lists/master/anime-list-full.xml`
  (branch is **`master`**; no `main`). Commit-pinned URLs work for reproducible
  snapshots: `…/anime-lists/<sha>/anime-list-full.xml`.
- Regeneration is CI-driven: every merged PR triggers `Generate lists`
  (`generate-lists.yml`, auto-commit by `actions-user`); weekly Sunday 01:00 cron
  `Update to latest animetitles.xml`. Measured cadence on the full file: multiple
  regenerations per week. **No releases, no tags, no changelog** (API: empty).
  **The XML embeds no comments, timestamp, or commit SHA** → provenance (fetch
  timestamp + last-touching commit SHA via commits API) is ours to record.
- **Conditional fetch (measured):** `ETag` present (strong for `identity`, weak
  `W/"…"` for gzip), **no `Last-Modified`** → only `If-None-Match` works.
  `Vary: Accept-Encoding` ⇒ **cache the validator together with the
  exact `Accept-Encoding`** (cross-encoding reuse yields 200, not 304).
  `Cache-Control: max-age=300`. Verified: matching ETag+encoding → **304**.
  ETag ≠ file SHA-256 — treat as opaque validator.
- **Licensing: no LICENSE file, GitHub `license: null`, README silent** on
  attribution/redistribution → **shipping a bundled XML copy is UNVERIFIED
  (no granted permission)**: fetch at runtime, cache locally, attribute the source.
  (`animetitles.xml` is a copy of AniDB's; upstream terms not established here.)

### Structure (measured; no DTD/XSD exists)

- Root `<anime-list>`. `<anime>` attributes: `anidbid`, `tvdbid`,
  `defaulttvdbseason`, `episodeoffset`, `tmdbtv`, `tmdbseason`, `tmdboffset`,
  `tmdbid`, `imdbid`. Children: `<name>`, `<mapping-list>`, `<before>`,
  `<supplemental-info replace="">`, studio/genre/fanart/thumb/credits/director.
- **Confirmed ABSENT from this project's format** (0 occurrences in file/README/
  history incl. 2012 initial commit): `startingEpisodeNumber`,
  `endingEpisodeNumber`, `startingAniDBEpisdoeNumber`, `epoffset`, `a`/`b` anchors
  on `<anime>`, `type` on `<mapping>`, `tvdbid` on `<mapping>`, `<mappingList>`,
  `airedStart/airedEnd`, `startEpisode/endEpisode`, `newAnimeEpisode`. Parsers must
  target the real attribute set only.
- `<mapping>` attributes (complete set): `anidbseason` (0=specials,1=regular,2 seen
  once), `tvdbseason`, `tmdbseason`, `start`, `end`, `offset`. Body: individual
  pairs ` ;anidbEp-tvdbEp; … ; `.

### Provider and presentation metadata validation

The [upstream README](https://github.com/Anime-Lists/anime-lists#format) identifies
non-TVDB titles with AniDB type markers in `tvdbid`: `movie`, `hentai`, `OVA`/`ova`,
`music video`, `tv special`, `web`, and `other`. These records need no
`defaulttvdbseason` and produce no TVDB targets. They still participate in AniDB
ownership/duplicate validation; unknown markers and malformed structures are rejected.
The [full-list transform](https://github.com/Anime-Lists/anime-lists/blob/master/transforms/create-anime-list-full.xsl)
removes empty attributes, so their absence is legitimate.

`<before>;1-26;</before>` orders AniDB specials for presentation. It is validated
as text-only positive episode pairs and never supplies TVDB identity translations.
Nested `<mapping>` elements inside `<before>` are invalid.

Mapping rules containing only `tmdbseason` are validated against their numeric
`tmdbtv` owner and the provider's attribute/pair syntax, then omitted from the
TVDB index. They cannot suppress TVDB entry offsets or absolute fallback, nor create
TVDB targets. Their ignored offsets are never expanded; this does not implement
open-ended TVDB offset semantics. Rules containing both provider seasons validate
both seasons and retain only their TVDB meaning.

### Semantics (README verbatim, all orchestrator-quoted)

- Offsets are **AniDB→TVDB additions**: `tvdb_ep = anidb_ep + offset` ⇒ reverse:
  `anidb_ep = tvdb_ep − offset`. `episodeoffset` = entry-level (regular eps in
  `defaulttvdbseason`); mapping `offset` applies within `[start,end]` of its
  `anidbseason`.
- Precedence: "The offset and/or episodeoffset will be ignored in favour of an
  individual mapping." ⇒ **individual `;a-b;` pairs > mapping range `offset` >
  entry `episodeoffset`**. `start`/`end` without `offset` (35 nodes): undefined → inert.
- `defaulttvdbseason="a"`: "may be marked as `a` if the absolute episode numbering
  is defined and matches AniDB.net" ⇒ Sonarr absolute number == AniDB number (74
  entries; 20 plain identity, 53 with mapping-lists). **`a`+`episodeoffset`
  combination UNVERIFIED** (2 entries, both also carry range mappings) → fail closed.
- **TVDB `0` in target position** = "AniDB episode has no TVDB counterpart"
  ("mapped to 0 if and only if there's a conflict"; 739 `;-0;` pairs). Never
  register as a TVDB episode key.
- Multi-episode: `;1-1+2;` = one AniDB ep covers TVDB 1 and 2 (valid, 28 bodies);
  `;1-1;2-1;` = AniDB eps 1+2 are parts of TVDB ep 1 (valid; 103 measured keys);
  **`;1+2-1;` IS NOT VALID** → reject at parse.
- T/O encodings (README): "trailer episodes with prefix T … >= 201, T1 = 201";
  "Other … >= 401, O1 = 401". Decode explicit pair values to Shoko's type-local
  numbers first: 201→`Trailer` 1, 400→`Trailer` 200, 401→`Other` 1. This applies
  before the season-0 `Special` rule. These encoded values apply only to individual
  pair declarations; entry offsets and range rules keep their declared regular or
  special type and number.
- Explicit regular `anidbseason="1"` pairs 101–103 are valid regular `Episode`
  identities, not an unsupported range. Pinned examples in the commit above:
  Ranma 1/2 Nettou Hen (AniDB 193, TVDB 76932) has regular 103→TVDB S6E10 in
  its 95–118 offset range ([source lines 948–956](https://github.com/Anime-Lists/anime-lists/blob/996d6a8b98c0c8ba94378875746a83b66dc590d4/anime-list-full.xml#L948-L956));
  Kirarin Revolution (AniDB 4237, TVDB 103071) explicitly maps 101, 102 and 103
  ([source lines 11614–11621](https://github.com/Anime-Lists/anime-lists/blob/996d6a8b98c0c8ba94378875746a83b66dc590d4/anime-list-full.xml#L11614-L11621)).
- Season-0 explicit values 101–103 also occur (for example AniDB 1431 / TVDB
  79032 maps them to S0E3–E5, alongside T1 and O1 at S0E1–E2; [source lines
  5323–5328](https://github.com/Anime-Lists/anime-lists/blob/996d6a8b98c0c8ba94378875746a83b66dc590d4/anime-list-full.xml#L5323-L5328)).
  Their type is undocumented: do not guess `Special`. Record a structured
  unsupported-type reason and fence only the explicit affected TVDB positions;
  retain supported mappings in the same entry and allow unrelated mappings and
  the valid upstream snapshot to proceed. The pinned minimal excerpts are kept in
  `tests/fixtures/anime_lists_legacy_number_examples.xml`.
- Split series: the pattern is **many `<anime>` entries sharing one `tvdbid`**
  (1,303 numeric tvdbids multi-claimed; AoT = 8 entries on TVDB 267440) and
  **zero shared `anidbid`** (10,771/10,771 distinct) — season→entry resolution uses
  `defaulttvdbseason` equality. Season 0 is structurally ambiguous (447/6,337
  `(tvdbid,season)` keys multi-claimed; 421 of them season 0) → **fail closed**,
  never resolve season 0 by season alone.
- **No AniDB episode type tag in the data** (no `ep/spe/cred/par`): `anidbseason`
  0/1 plus explicit-pair T/O encodings are the available type signals. Shoko-side
  type (wire `Episode/Special/Trailer/Other/Credits/Parody`) is source-aware:
  regular entry offsets, absolute identity, and `anidbseason=1` ranges are
  `Episode` at every positive number; `anidbseason=0` ranges are `Special` at
  every positive number. Explicit pair codes `201–400` and `>=401` are T/O in
  either season; regular explicit 101–103 remain `Episode`, while season-0 explicit
  101–103 are fenced because their type is unverified. Credits/Parody are not
  expressible by Anime-Lists → not mappable targets.

### Activating snapshots with isolated semantic defects

The default public snapshot inspected during the setup investigation (SHA-256
`29abeffcdbd9911c4b230a1565f841c5e2ed4b51b37273da9be83c502e744b89`)
contains eight invalid rows: six TVDB mapping offsets without both range bounds,
one unverified AniDB season 2 rule, and one contradictory `40-75+0` pair. Two
additional rows use the already unsupported absolute-season plus entry-offset form.
These declarations remain uninterpreted and retain their parser severity/counts.
They must not prevent unrelated, verified series from becoming available.

Activation uses explicit parser issue codes, never diagnostic-string matching. Only
these recognized semantic defects with strictly positive, fully validated AniDB and
TVDB owners may be isolated. The entire affected TVDB series is fenced, including
otherwise valid sibling entries and operator override additions, before any fallback
or override lookup. Fences affect generation digests, survive cache reload/restart,
and enter the existing per-file/campaign mapping-drift checks. Previously frozen
targets do not become writable merely because the source is later repaired.

Malformed XML, unknown schema/attributes, missing or ambiguous ownership, duplicate
AniDB identities, resource-limit failures, and snapshots with no unfenced usable
entries still reject the whole update, preserve the last-known-good cache, and keep
the invalid-source write hold and bounded retry behavior. A successful accepted
activation is required to clear that hold. Minimal excerpts are in
`tests/fixtures/anime_lists_owner_quarantine.xml`; the full public file is not vendored.
This is offline public-source and mocked-transport evidence, not live Sonarr/Shoko
validation.

### Real-data regression anchor (M4 gate)

Attack on Titan: TVDB series **267440** (not 262461), split entries — 9541 (S1),
9826 (OAD, s0), 10944 (S2), 13241 (S3), 14444 (S3 Part 2, `episodeoffset="12"`),
14977 (Final, `defaulttvdbseason="4"`), **16177 (Final Part 2, `defaulttvdbseason="4"
episodeoffset="16"`)**, 17303 (Final Part 3, `episodeoffset="28"`).
**S04E26 (abs 085, "Traitor") → AniDB 16177, AniDB episode 10**
(26 − 16 = 10; abs: 25+12+12+10+16 = 75; 85 − 75 = 10). Verified from the live XML
(entry carries no mapping-list overrides).

## 4. Cross-service integration facts (M0 decisions)

- **Sonarr grouping:** `episodefile` responses carry no episode
  links; build `episodeFileId → [episodes]` from the per-series
  `GET /api/v3/episode?seriesId=` payload (one request per eligible series — no
  per-file requests; keeps M2/M3 index construction ~linear for ~7,000 files).
- **Case conventions:** Sonarr = camelCase + null-omission; Shoko =
  PascalCase + nulls-included. Per-service parsing; never share assumptions.
- **Instance identity (PLAN M0 decision):** Sonarr exposes
  `GET /api/v3/system/status` (`version`, `instanceName`, `urlBase`, `branch`,
  `isDocker`) — use as identity signal; Shoko v3 exposes no public stable
  instance ID (settings is admin-only and secret-laden) → use **configured
  instance identity + conservative sampled identity-change detection**
  (series/episode ID reuse, root-list drift, inventory discontinuity).
  No invented endpoints.
- **URL bases:** Sonarr may sit behind a `urlBase` (307 redirect if omitted);
  Shoko v3 base is `/api/v3` with no urlBase middleware — keep per-service base
  handling; never attach API keys to cross-origin redirect targets (both services
  send keys; Sonarr's key is also readable from its own config endpoint —
  **never log it**).
- **Key placement:** Sonarr prefers `?apikey=` (query, first) then `X-Api-Key`;
  Shoko accepts `apikey` header/query. **Prefer headers** in our clients to keep
  keys out of URLs/proxy logs.

## 5. Live-only checks (EXTERNAL/UNVERIFIED — never claimed offline)

- Deployed Sonarr/Shoko versions, exact response shapes under installed configs
  (urlBase, auth mode), settings values (`FileQualityFilterEnabled=false`,
  renamer flags, `AniDb.MyList_AddFiles`), mount read-only-ness, plugin inventory,
  live AniDB episode titles (e.g., "Traitor" title check), queue behavior.
- Shoko `master`-branch link semantics; Shoko v6 API.

### Optional missing-metadata maintenance (Shoko 5.3.3)

The normal-service option defaults off and requires separate scope-bound consent; see [requirements](../PROJECT.md#shoko-linking-and-verification). The source-verified request is `POST /api/v3/Series/AniDB/{id}/Refresh` with `force=false`, `downloadRelations=false`, `createSeriesEntry=true`, `immediate=false`, `cacheOnly=false`, and `skipTmdbUpdate=true`. JSON `false` means queued, not complete or rejected. This is normal Shoko maintenance, not a pure metadata/no-hook operation: same-anime associations and processing may change. Different job flags can produce different queue keys for the same anime. Queue inspection requires administrator visibility; unknown visibility refuses new requests. Queue snapshots plus local durable guards do not promise globally exactly-once scheduling across Shoko clients. No live write is claimed.

New file-link submissions require fresh, complete Shoko job visibility to avoid duplicating same-file ManualLink jobs or using episode IDs while a target anime refresh is pending. In Shoko 5.3.3, `GET /api/v3/Queue/Items` requires an administrator API key. A forbidden, incomplete, or unavailable inventory safely defers new links in both normal and advanced modes; verification of already submitted links remains available. Unrelated jobs do not block linking. This read-and-submit guard cannot prevent another client from scheduling a job after the observation.

Each fresh inventory is bounded to 20,000 jobs, 20 pages of at most 1,000 jobs,
and 8 MB of combined response bodies (including both running-job summaries),
within the caller's remaining deadline and configured HTTP timeouts. These bounds
cover the tested 20,000-job unrelated backlog; larger or more verbose responses
defer new submissions rather than treating an incomplete inventory as empty.
