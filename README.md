# Shoko–Sonarr Linker

This service uses Sonarr's episode records and verified Anime-Lists mappings to
link eligible, unrecognized anime files to episodes in Shoko. It runs in the
background and keeps track of new arrivals, verified links, and work that needs
review. It does not rename, move, or edit media files.

## Quick start

1. Install the container using [Docker Compose](compose.example.yaml) or the
   [Unraid template](unraid.xml). Connect it to the Docker network used by Sonarr
   and Shoko, and provide a persistent `/data` volume.
2. Enter the Sonarr URL/API key and Shoko URL/API key in the container settings.
   These are the only required service settings; no YAML file or media-library
   mount is required.

   ```text
   SONARR_URL
   SONARR_API_KEY
   SHOKO_URL
   SHOKO_API_KEY
   ```

3. Start the container. It begins with read-only discovery and does not publish a
   dashboard port automatically.
4. Open the dashboard on a port you deliberately map, create its username and
   password, then review the anime scope and any safety facts the service cannot
   verify itself. Automatic linking starts only after setup is confirmed.

If Sonarr has several anime roots, select the folders to include. If the service
cannot prove how a Sonarr folder corresponds to a Shoko folder, it will ask you to
choose the match. Unsafe or unavailable Shoko settings and physical/plugin safety
facts require review before linking can start. Uncertain matches are left for
review rather than guessed.

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

The container listens on port `8080` internally. Map a host port only when you
want dashboard access, and keep that mapping on a trusted network. Authentication
is required after first-use account creation.

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

```powershell
uv sync
uv run pytest -q
uv run ruff check .
uv run ruff format --check .
uv run mypy
```
