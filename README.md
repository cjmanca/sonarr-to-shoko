# Shoko–Sonarr Linker

This service uses Sonarr's episode records and verified Anime-Lists mappings to
link eligible, unrecognized anime files to episodes in Shoko. It runs in the
background and keeps track of new arrivals, verified links, and work that needs
review. It does not rename, move, or edit media files.

## Quick start

1. Install the example Compose file. Download [`compose.example.yaml`](compose.example.yaml)
   and save it in a new folder on the Docker host.

2. Enter the four connection settings by creating a `.env` file beside the Compose
   file and adding the values below. Use URLs and API keys that the container can
   reach; the example URLs work only when the services share Docker DNS on the same
   network.

   ```text
   SONARR_URL=http://sonarr:8989
   SONARR_API_KEY=replace-with-your-sonarr-api-key
   SHOKO_URL=http://shoko:8111
   SHOKO_API_KEY=replace-with-your-shoko-api-key
   ```

   These are the only required service settings. Keep `.env` private; do not commit
   it or share the API keys.

   In `compose.example.yaml`, set the `media.name` value to the existing Docker
   network shared by Sonarr and Shoko; check the containers' network settings on
   the Docker host to find its name. `media` is only an example. The Compose file
   joins that existing network but does not create or reconfigure it. If you use
   host/IP addresses instead of Docker service names, make sure those addresses
   are reachable from the container.

3. Start the published image from that folder:

   ```text
   docker compose -f compose.example.yaml pull
   docker compose -f compose.example.yaml up -d
   ```

   Compose uses `ghcr.io/cjmanca/shoko-sonarr-linker:latest`; it does not build the
   source. The named `/data` volume persists the SQLite state and mapping cache
   across container upgrades. The container starts the normal background service
   automatically, with no YAML configuration file or media-library mount.

4. Open the dashboard after deliberately exposing its port. By default, Compose
   does not publish a dashboard port. To open it on the Docker host, uncomment the
   `ports` lines under the service in the Compose file, keeping the
   `127.0.0.1:8080:8080` binding. Re-run the `up -d` command, then visit
   `http://127.0.0.1:8080`. To access it from another trusted LAN device, bind the
   host's LAN IP instead and restrict access with your network firewall; do not
   expose the dashboard to the public Internet. Create the dashboard account on
   first visit, then review the anime scope and any safety facts the service cannot
   verify itself. Automatic linking starts only after setup is confirmed.

The [Unraid template](unraid.xml) uses the same published `latest` image, requires
the same four connection settings, persists `/data`, and leaves dashboard port
publishing optional.

The dashboard's **Waiting** count is local linker work, including planning,
missing metadata, and confirmation of earlier requests; Shoko's queue can be empty
while files are waiting. **Needs review** lists a separate group of tracked files
with safety, mapping, conflict, or uncertain-result problems. Each group has
a paginated filename/path table with plain reasons. **Latest scan exclusions** is
independent of those tracked groups. **Last completed scan** explains the observed
unrecognized files and selected Sonarr matches, including its observation times;
these historical counts need not equal Shoko's current unrecognized-file total.

**History** shows retained linking and metadata request records,
including their recorded time and outcome. It includes all retained server
sources and modes; file request records do not preserve the historical server
endpoint, and displayed paths are current retained context. Prepared or possibly
sent records do not prove delivery, and available metadata does not prove a file
was linked. This is request history, not an audit of Shoko's media changes or hooks.

Use the left navigation to switch between Overview, Issues, Verified, Waiting,
History and Settings. Overview focuses on health and setup; Issues separates
tracked review files from latest scan exclusions. The header totals link to their
file tables. The single Pause/Resume button reflects the saved pause state,
including service pauses; the status text identifies user and service pauses separately.
Resume still checks authorization and cannot bypass a safety hold. File tables show paginated
filenames, relative directories and the confirmed library root when known.

The application runs as `PUID:PGID`, defaulting to `1000:1000`. For Unraid,
set `PUID=99` and `PGID=100` (the template includes these defaults). Compose
users can add these optional values to `.env`. Startup briefly uses root to
adjust ownership of `/data` and its contents, then drops privileges before
starting the application. Mount only a dedicated appdata directory at `/data`.
Existing installations must add these variables when updating their container.

`latest` is a moving tag. To stay on the newest successful default-branch image,
run `docker compose -f compose.example.yaml pull` before restarting after an update.
For reproducible deployments, replace `latest` in the Compose `image` line with a
published immutable `sha-<40-character-commit>` tag from the GHCR package versions.
Unraid users can select the same SHA-tagged image in the template's repository field.

Sonarr's **Anime**, **Standard**, and **Daily** series types are supported within
the folders you explicitly select. A series still needs verified TVDB-to-AniDB
episode mappings before any file can be linked. The service does not infer episode
identities from filenames or air dates, or include unselected folders.

If Sonarr has several library roots, select the folders to include. If the service
cannot prove how a Sonarr folder corresponds to a Shoko folder, it will ask you to
choose the match. Unsafe or unavailable Shoko settings and physical/plugin safety
facts require review before linking can start. Uncertain matches are left for
review rather than guessed.

### Separate Sonarr and Shoko-managed collections

You can keep Shoko's global move/rename options enabled for a separate manual
import source and destination. Set the selected Sonarr import folder in Shoko to
**Drop Type: None** and mount its media **read-only inside the Shoko container**.
Setup checks that the selected paths belong unambiguously to excluded folders;
every location of each linked file must also be eligible and mapped. This folder
exclusion behavior was verified in Shoko 5.3.3 source; see the
[compatibility evidence](docs/compatibility.md#excluded-folders-and-mixed-collections).

During setup, confirm that the selected media has no writable aliases or writable
hardlink exposure in Shoko, and that plugins or external hooks cannot modify any
media on link events. These facts cannot be checked through Shoko's API. Mounting
media read-only only in the linker container does not provide this protection.
`FileQualityFilterEnabled` and `AniDb.MyList_AddFiles` must still be disabled;
folder exclusion does not contain their side effects. If settings cannot be read,
setup asks you to confirm the applicable facts manually. The service never changes
Shoko settings.

After upgrading to this safety policy, review and confirm setup again. Changes to
selected Shoko folder IDs, paths or types, safety settings, or the observed server
version also pause linking for confirmation. Changes to unrelated valid manual
folders do not require confirmation while selected ownership remains unambiguous.

## Dashboard and controls

The authenticated dashboard shows connection health, verified and waiting files,
files needing review, episode-mapping availability, and whether automatic linking
is running or paused. Use its **Pause** and **Resume** controls, or run the same
local service controls in the container:

```text
shoko-sonarr-linker status
shoko-sonarr-linker pause
shoko-sonarr-linker resume
```

Status updates automatically without reloading the page. Setup shows its current
discovery stage and an estimated progress percentage, including series/file counts.
Open **Settings** to change service timing, throughput, connection URLs, and selected
anime libraries or folder mappings. **Show advanced** reveals the remaining service
configuration. Saving a library change pauses linking until you confirm its scope
and safety facts again. Connection URL changes require a container restart and new
confirmation. API keys stay in the container environment; change them there.

The container listens on port `8080` internally. Map a host port only when you
want dashboard access, and keep that mapping on a trusted network. Authentication
is required after first-use account creation.

### Missing anime or episode metadata

In dashboard **Settings**, **Let Shoko retrieve missing anime and episode metadata**
is off by default. Enabling requires fresh setup confirmation accepting Shoko's
normal same-anime maintenance, including possible database association changes and
processing of the manual collection. Disabling requires fresh setup confirmation
and prevents new requests; it does not cancel jobs Shoko already accepted.

The Overview's **Missing episode metadata** panel shows the latest local
observations with affected filenames, Sonarr/Shoko paths, and AniDB anime IDs.
These are recent examples, not a total or percentage of pending work. **Accepted**
means Shoko accepted a request; **metadata ready** means the service can check the
complete file plan again. Only **Verified files** confirms completed file links.
Uncertain deliveries receive read-only checks without automatically replaying the
request. Observation time is not a promised completion or next-check time.

If queue visibility is incomplete, check that the configured Shoko API key has
administrator access to the queue inventory (`Queue/Items` in Shoko 5.3.3). New
requests wait when the service cannot establish the queue state safely. Known
queued/running/blocked work is awaited without submitting another request.

## Existing installations

Keep the existing `/data` volume when upgrading. The application migrates SQLite
state in place and does not treat old approvals or apply settings as consent for
normal-service linking. To roll back, stop the container and preserve `/data`;
do not delete or replace the database or mapping cache.

## Advanced operation

Existing YAML configuration and operator commands remain available for support
and troubleshooting. See the [advanced CLI and recovery reference](docs/advanced.md)
and [advanced deployment safety runbook](docs/advanced-deployment.md) when those
tools are needed.

## Development checks

Normal installation does not require Git, Python, `uv`, or a local image build.
For development, clone the repository, build the local image with
`docker build -t shoko-sonarr-linker:local .`, and use that tag in a local copy of
the Compose file.

```powershell
uv sync
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
uv run mypy
```
