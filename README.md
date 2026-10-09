# shoko-sonarr-linker

Links unrecognized Shoko files to the correct Shoko episodes using Sonarr's
file-to-episode records and Anime-Lists TVDB↔AniDB mappings. Read-only against
Sonarr; Shoko writes are gated. See [PROJECT.md](PROJECT.md) for requirements and
[AGENTS.md](AGENTS.md) for development rules.

**Current state: M1 foundation.** Configuration, path-safety utilities, HTTP client
policy, CLI skeleton, run-policy representation and offline tests. The reconciliation,
linking and mapping-refresh engines are not implemented yet.

## Install

```powershell
uv sync            # creates/syncs .venv with runtime and dev dependencies
```

## Run

```powershell
$env:SONARR_API_KEY = "<key>"   # API key values live only in the environment
$env:SHOKO_API_KEY  = "<key>"   # the config file stores only the variable NAMES
uv run shoko-sonarr-linker validate config.example.yaml
uv run shoko-sonarr-linker config config.example.yaml --check
uv run shoko-sonarr-linker once --dry-run config.example.yaml
uv run python -m pytest -q
```

Configuration is YAML (`config.example.yaml` is the full example). Exit codes:
`0` ok, `2` configuration invalid (the message names the failed field, never a secret),
`3` apply-gate refusal, `4` not implemented in this build.

## Safety defaults

- **Dry-run is the default.** `once` proposes nothing writable and makes no remote change.
- **Live writes are default-deny** and open only through `shoko_linker.config.apply_gate`:
  both `sync.apply_changes: true` in the config **and** the operator environment variable
  `SHOKO_LINKER_ALLOW_APPLY=1`. `once --apply` refuses with exit `3` otherwise; no CLI flag
  enables writes by itself.
- **Secrets are environment-only.** Config holds the variable name; values are read at
  access time, never stored in settings, reports, `repr`, and scrubbed from logs by a
  redaction filter **and** a redacting formatter (tracebacks included) registered at
  client creation. Redaction is literal-substring scrubbing of registered values:
  values shorter than 8 characters are not registered (scrubbing them would erase
  ordinary words) — client creation logs a warning naming only the length — real
  Sonarr/Shoko keys are long, and request logging must keep keys out of URLs entirely.
- **Scope fails closed.** `sonarr.allowed_roots`, `shoko.allowed_roots` and
  `paths.mappings` must be non-empty. Root matching is path-component-aware
  (`/anime` never matches `/anime-old`), paths are compared case-sensitively, and nested
  or conflicting mappings are rejected at load time.
- **No automatic retries.** Clients are built with `follow_redirects=False`,
  `retries=0`, and explicit timeouts. Redirects are followed only by
  `http.resolve_and_send`, which caps hops, and drops auth headers when a redirect
  target's origin differs. Non-idempotent requests are never replayed (HTTP 303 after a
  POST is refused).
- **No media file access.** This tool never opens, renames, moves, or deletes media.

## Docker

```powershell
docker build -t shoko-sonarr-linker:local .
docker run --rm shoko-sonarr-linker:local --help
docker run --rm -e SONARR_API_KEY=<key> -e SHOKO_API_KEY=<key> shoko-sonarr-linker:local validate --offline config.example.yaml
```

The image runs as a non-root user, needs only a writable `/data` volume
(SQLite state + mapping cache), mounts no media, exposes no ports, and uses
`init: true` (see [compose.example.yaml](compose.example.yaml)) for signal-safe
shutdown.

## Stub commands

`validate --live-read-only`, `daemon`, `report`, `mappings status`, and
`mappings refresh` are declared in the CLI and refuse with exit `4` and an explicit
"not implemented" message. They make no network calls and write nothing.
