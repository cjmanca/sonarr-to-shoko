# shoko-sonarr-linker — non-root, restart-safe container.
#
# Deliberate constraints (product safety):
#  * No media-library mounts, no privileged mode, no Docker socket.
#  * The service talks only to Sonarr/Shoko over the network; keys come from
#    the environment at runtime, never from the image or the build context.
#  * /data is the only writable path (SQLite state + mapping cache), owned by the
#    non-root runtime user.
#  * init: true in Compose provides a PID-1 signal handler (container-init), so the
#    Python process receives SIGTERM for graceful shutdown.

FROM python:3.12-slim

# Reasonable pin: python:3.12-slim (digest pinning is enforced at GHCR publish
# time — see the publishing workflow milestone).

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN groupadd --gid 1000 linker \
    && useradd --uid 1000 --gid 1000 --create-home --shell /usr/sbin/nologin linker

WORKDIR /app

# Build the wheel from source, install it, then drop build scaffolding so the
# runtime layer carries no source tree and no build caches.
COPY pyproject.toml README.md config.example.yaml ./
COPY src ./src
RUN pip install --no-cache-dir . \
    && pip check \
    && rm -rf src dist *.egg-info

# Writable persistent state directory (mount a volume here in Compose).
RUN mkdir -p /data && chown -R 1000:1000 /data && chmod 0755 /data

USER 1000:1000

ENTRYPOINT ["shoko-sonarr-linker"]
CMD ["--help"]
