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
#
# Provenance pins (M8 security review F8), fetched 2026-10-09:
#  * Base pinned by digest, python:3.12-slim linux/amd64 manifest
#    sha256:2b4f19dae3a777dfc3b76730bda1e82e1f66ab2a2686fa93ca78edbfb4f04ffe
#    (the tag's multi-arch manifest-list digest is
#     sha256:a6e34c598f2467ed0e9a8d349809fcd8b5c603269512df273a0bb1784edc11b1;
#     source: Docker Hub API library/python tags/3.12-slim, amd64 image digest).
#  * Builder tool uv pinned by release tag ghcr.io/astral-sh/uv:0.13.0
#    (uv 0.13.0 release, 2026-10-09); deliberately NOT the floating :latest tag.
#  * Install is lockfile-driven: uv sync --frozen refuses to build when uv.lock
#    is stale vs pyproject.toml, pins exact versions, and verifies the sha256
#    hashes recorded in the lock for every artifact; --no-dev keeps pytest/ruff/
#    mypy out of the image, --no-editable keeps the source tree out of runtime.

FROM python@sha256:2b4f19dae3a777dfc3b76730bda1e82e1f66ab2a2686fa93ca78edbfb4f04ffe AS base
# = python:3.12-slim (linux/amd64, digest-pinned)

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

FROM base AS builder
# uv binary from the pinned official uv image (binary only; the uv image itself
# never reaches the runtime layer).
COPY --from=ghcr.io/astral-sh/uv:0.13.0 /uv /usr/local/bin/uv
ENV UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_PREFERENCE=only-system
WORKDIR /app

# Dependency layer first (cache-friendly): resolves strictly from uv.lock.
COPY pyproject.toml README.md uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
# Project layer last: build + install the wheel, non-editable.
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable \
    && uv pip check

FROM base AS runtime

RUN groupadd --gid 1000 linker \
    && useradd --uid 1000 --gid 1000 --create-home --shell /usr/sbin/nologin linker

# Runtime layer: installed venv + example config only — no source tree, no
# build caches, no uv.
COPY --from=builder /app/.venv /app/.venv
COPY config.example.yaml /app/config.example.yaml
ENV PATH="/app/.venv/bin:$PATH"
WORKDIR /app

# Writable persistent state directory (mount a volume here in Compose).
RUN mkdir -p /data && chown -R 1000:1000 /data && chmod 0755 /data

USER 1000:1000

ENTRYPOINT ["shoko-sonarr-linker"]
CMD ["service"]
