# Docker and Unraid deployment

The normal installation needs four environment variables, persistent local state,
and network access to Sonarr and Shoko. It starts the background service
automatically. No media-library mounts or YAML configuration are required.

## Docker Compose

Copy `compose.example.yaml`, connect the service to the network where Sonarr and
Shoko are reachable, and set these variables in the deployment environment or a
protected environment file:

```text
SONARR_URL
SONARR_API_KEY
SHOKO_URL
SHOKO_API_KEY
```

Keep the named `/data` volume. SQLite state, the mapping cache, dashboard account,
and setup consent live there. Do not place `/data` on SMB/NFS storage; SQLite
locking is not reliable on network shares. The container runs as UID/GID 1000 and
does not need the Docker socket or media mounts.

Start the container with `docker compose up -d`. The service checks connections
read-only, and the first dashboard visit creates the dashboard account and opens
the setup flow.

## Dashboard access

The application listens on container port `8080`, but Compose publishes no host
port by default. To enable access only from the Docker host, add this explicit
mapping to the service:

```yaml
ports:
  - "127.0.0.1:8080:8080"
```

For other local-network arrangements, publish the port only on a trusted network.
The dashboard requires authentication after the first account is created; do not
put it behind an unauthenticated reverse proxy.

## Unraid

1. Add the `unraid.xml` template in the Docker template editor.
2. Set Sonarr and Shoko URLs and API keys. API keys are masked fields and are read
   from the container environment at runtime.
3. Set the `/data` host path to a persistent local appdata directory owned by
   UID/GID 1000. Keep the optional advanced YAML mount empty for the normal setup.
4. Join the Docker network used by Sonarr and Shoko.
5. Leave **Dashboard Port (optional)** empty to publish no host port. If you want
   dashboard access, assign a trusted local host port to container port `8080`.
6. Apply the template and start the container. Open the dashboard on the selected
   local port, create a username/password, and complete the setup decisions shown.

The dashboard and worker use the same serialized service loop. Restarting the
container preserves verified links, pending verification, pause state, consent,
and the last-known-good episode mapping cache.

## Upgrade and rollback

Keep the existing `/data` volume when upgrading. SQLite migrations add onboarding
and dashboard state without deleting existing submissions or unresolved sends.
Old `sync.apply_changes`, `SHOKO_LINKER_ALLOW_APPLY`, attestations, and campaigns
do not authorize normal automatic linking; complete first-use setup instead.

To roll back, stop the container and preserve `/data`. Do not delete, replace, or
manually edit the database, mapping cache, or pending-send state. Existing advanced
YAML and CLI workflows remain documented in [advanced guidance](advanced.md).

## Image and runtime verification

For a local image build and smoke test, see the repository's development workflow
and Dockerfile. Live Sonarr/Shoko connections, Unraid installation, and Shoko
physical-side-effect safety remain deployment-specific checks.
