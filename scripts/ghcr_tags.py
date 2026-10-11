#!/usr/bin/env python3
"""GHCR (OCI) registry discovery and ``sha-`` tag provenance verification.

Standard library only (urllib, json, base64, hashlib). Used by
``.github/workflows/publish.yml`` to learn the ACTUAL publication state of the
GHCR package: ``git ls-remote --tags`` reports git refs, which are INDEPENDENT
of container tags — publishing ``sha-<fullsha>`` as an image creates no git
tag, so backfill must read the registry, not the git tag list.

Explicit outcome classes — failure is never conflated with absence:

``ok``                 tag list collected across all pages (exit 0).
``package_missing``    the tags call answered 401/404 with ``NAME_UNKNOWN``
                       semantics AFTER a successful token exchange. GitHub's
                       GHCR hides a non-existent (or not-yet-created, private)
                       package from pull traffic with 401 + ``NAME_UNKNOWN``
                       (404 on public registries); on a fresh repository with
                       no published image this is the documented first-run
                       state, so it means "nothing published yet" (exit 0).
``auth_failure``       the token endpoint answered 401 (token invalid/expired).
``permission_failure`` a tags/manifest call answered 403 (authenticated, not
                       permitted). Conservative: the state is UNKNOWN, not
                       absent.
``network_failure``    DNS / connect / timeout / transport errors.
``unavailable``        5xx, malformed/unparsable responses, or the pagination
                       limit was reached.

Any status outside {ok, package_missing} exits NONZERO, and the plan step must
ABORT the run on that outcome — never publish against unknown registry state.

Provenance verification (``verify``) is best-effort and bounded. A ``sha-``
tag counts as published ONLY when the manifest/config chain carries the OCI
annotation ``org.opencontainers.image.revision`` equal to the full SHA. A tag
that the registry answers 404 for is ``not_published`` (absent, safe to
publish). Any fetch/parse failure yields ``unknown_provenance``: the tag
EXISTS and its content cannot be verified, so the CONSUMER must NOT re-push or
overwrite it — a ``sha-`` tag NAME derived from a commit is not proof that the
existing image is the right or immutable one. Publication policy (skip /
publish / reconcile) is owned by ``.github/workflows/publish.yml``.

Per the OCI Distribution Spec, manifests are read through
``GET /v2/<name>/manifests/<reference>`` and blobs through
``GET /v2/<name>/blobs/<digest>``. buildx publishes attestation-enabled
images as OCI image INDEXES whose descriptors mix runnable image
manifests with in-toto ATTESTATION manifests, so index children are fetched
from ``/manifests/`` (never ``/blobs/``) and the runnable descriptor is
selected by platform presence (attestation descriptors carry no usable
platform and/or attestation annotations). Every digest-addressed document is
verified strictly: the digest string must be ``sha256:<64 lowercase hex>``
(non-conformant digests such as ``sha512:``/``notadigest``/empty fail closed
to ``unknown_provenance``) and the fetched body must hash to it.

The token is read from an environment variable (``--token-env``) and reaches
the registry ONLY as a Basic ``username:password`` header (password = token);
it never appears in a URL. The username part is ignored by GHCR for
authentication, so a fixed placeholder is used.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Sequence
from typing import Any, Protocol, cast

REGISTRY = "https://ghcr.io"
PACKAGE = "shoko-sonarr-linker"
USER_AGENT = "shoko-sonarr-linker-ghcr-tags/1.0"
NAME_SEGMENT = re.compile(r"^[a-z0-9]+(?:[._-][a-z0-9]+)*$")
FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
DIGEST_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
LINK_NEXT = re.compile(r'^<([^>]+)>\s*;\s*rel\s*=\s*"?next"?$', flags=re.IGNORECASE)

STATUS_OK = "ok"
STATUS_PACKAGE_MISSING = "package_missing"
STATUS_AUTH_FAILURE = "auth_failure"
STATUS_PERMISSION_FAILURE = "permission_failure"
STATUS_NETWORK_FAILURE = "network_failure"
STATUS_UNAVAILABLE = "unavailable"
SUCCESS_STATUSES = frozenset({STATUS_OK, STATUS_PACKAGE_MISSING})

PROVENANCE_PUBLISHED = "published"
PROVENANCE_NOT_PUBLISHED = "not_published"
PROVENANCE_UNKNOWN = "unknown_provenance"
REVISION_ANNOTATION = "org.opencontainers.image.revision"

HTTP_OK = 200
HTTP_UNAUTHORIZED = 401
HTTP_FORBIDDEN = 403
HTTP_NOT_FOUND = 404
HTTP_SERVER_ERROR_MIN = 500
HTTP_SERVER_ERROR_MAX = 599
SHA_LENGTH = 40

DEFAULT_TIMEOUT = 30.0
MAX_PAGES = 100
VERIFY_CAP = 10
_MANIFEST_MEDIA_TYPES = (
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
)
ACCEPT_MANIFESTS = ", ".join(_MANIFEST_MEDIA_TYPES)
_INDEX_TYPES = frozenset(
    {
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
    }
)
_IMAGE_MANIFEST_TYPES = frozenset(
    {
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    }
)
ACCEPT_CONFIG = ", ".join(
    (
        "application/vnd.oci.image.config.v1+json",
        "application/vnd.docker.container.image.v1+json",
    )
)
# Descriptor-annotation markers that identify an in-toto ATTESTATION manifest
# (buildx attaches these to the attestation descriptors of an index).
_ATTESTATION_ANNOTATION_KEY_MARKERS = ("attestation", "in-toto.io/")
_ATTESTATION_ANNOTATION_VALUE_MARKERS = ("attestation", "in-toto")
# Deterministic runnable-descriptor preference when an index carries several
# platform-bearing image manifests: bare linux/amd64 (EMPTY variant, so
# linux/amd64/v4 is not the preferred rank) first, then (os, architecture,
# variant) alphabetical, then list order — so the chosen chain is stable.
_PREFERRED_PLATFORM = ("linux", "amd64", "")
MAX_INDEX_DEPTH = 4


class AuthFailureError(RuntimeError):
    """The token endpoint rejected the credential (401)."""


class PermissionFailureError(RuntimeError):
    """Authenticated but not permitted (403): state unknown, not absent."""


class PackageMissingError(RuntimeError):
    """The package does not exist yet (401/404 NAME_UNKNOWN after token exchange)."""


class NetworkFailureError(RuntimeError):
    """Transport-level failure (DNS/connect/timeout/protocol)."""


class RegistryUnavailableError(RuntimeError):
    """5xx, malformed response, or pagination limit."""


class PlanAbortedError(RuntimeError):
    """A registry discovery outcome forbids publishing against unknown state."""


class Response(Protocol):
    """The subset of ``http.client.HTTPResponse`` the transport depends on."""

    def read(self) -> bytes: ...

    def getheaders(self) -> list[tuple[str, str]]: ...

    @property
    def status(self) -> int: ...

    def __enter__(self) -> Response: ...

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None: ...


def _open(request: urllib.request.Request, timeout: float) -> Response:
    """Single seam for the HTTP opener so tests can monkeypatch it."""
    response = urllib.request.urlopen(request, timeout=timeout)  # noqa: S310 - ghcr.io https only
    return cast(Response, response)  # URL host/scheme enforced by _registry_url


class Transport(Protocol):
    def get(
        self, url: str, headers: dict[str, str], timeout: float
    ) -> tuple[int, dict[str, str], bytes]: ...


class UrllibTransport:
    """GET-only OCI transport with HTTP error bodies preserved."""

    def get(
        self, url: str, headers: dict[str, str], timeout: float
    ) -> tuple[int, dict[str, str], bytes]:
        request = urllib.request.Request(url, headers=headers, method="GET")  # noqa: S310 - ghcr.io https only
        try:
            with _open(request, timeout) as response:
                body = response.read()
                status = int(response.status)
                raw_headers = dict(response.getheaders())
        except urllib.error.HTTPError as exc:
            body = exc.read()
            status = int(exc.code)
            raw_headers = dict(exc.headers.items())
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            msg = f"transport error for {url}: {exc}"
            raise NetworkFailureError(msg) from exc
        lowered = {key.lower(): value for key, value in raw_headers.items()}
        return status, lowered, body


def validate_namespace(owner: str, package: str) -> None:
    """Reject anything that cannot be a valid lowercase GHCR ``owner/package`` pair."""
    if not NAME_SEGMENT.match(owner):
        msg = f"owner {owner!r} is not a valid lowercase GHCR namespace segment"
        raise ValueError(msg)
    if not NAME_SEGMENT.match(package):
        msg = f"package {package!r} is not a valid lowercase GHCR namespace segment"
        raise ValueError(msg)


def _registry_url(path: str) -> str:
    parsed = urllib.parse.urlsplit(path)
    if parsed.scheme != "https" or parsed.netloc != "ghcr.io":
        msg = f"refusing non-ghcr.io URL: {path!r}"
        raise RegistryUnavailableError(msg)
    return path


def _exchange_token(
    transport: Transport, owner: str, package: str, token: str, timeout: float
) -> str:
    scope = f"repository:{owner}/{package}:pull"
    query = urllib.parse.urlencode({"service": "ghcr.io", "scope": scope})
    url = f"{REGISTRY}/token?{query}"
    basic = base64.b64encode(f"oauth2:{token}".encode()).decode("ascii")
    headers = {
        "Authorization": f"Basic {basic}",
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }
    status, _, body = transport.get(url, headers, timeout)
    if status == HTTP_UNAUTHORIZED:
        msg = "token endpoint returned 401: GITHUB_TOKEN is invalid, expired, or unpublishable"
        raise AuthFailureError(msg)
    if status == HTTP_FORBIDDEN:
        msg = "token endpoint returned 403: GITHUB_TOKEN is authenticated but not permitted"
        raise PermissionFailureError(msg)
    if status != HTTP_OK:
        msg = f"token endpoint returned {status}"
        raise RegistryUnavailableError(msg)
    try:
        document = json.loads(body)
    except (ValueError, UnicodeDecodeError) as exc:
        msg = f"token endpoint response is not JSON: {exc}"
        raise RegistryUnavailableError(msg) from exc
    if (
        not isinstance(document, dict)
        or not isinstance(document.get("token"), str)
        or not document["token"]
    ):
        raise RegistryUnavailableError(
            "token endpoint response is missing a non-empty 'token' field"
        )
    return str(document["token"])


def _oci_error_detail(body: bytes) -> str:
    try:
        document = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return "<non-JSON error body>"
    if isinstance(document, dict) and isinstance(document.get("errors"), list):
        codes = [str(item.get("code")) for item in document["errors"] if isinstance(item, dict)]
        return ", ".join(codes) or "<empty errors>"
    return "<no OCI error codes>"


def _next_page_url(headers: dict[str, str]) -> str | None:
    link = headers.get("link")
    if not link:
        return None
    for part in link.split(","):
        match = LINK_NEXT.match(part.strip())
        if match:
            return _registry_url(match.group(1))
    return None


def _tag_names(body: bytes) -> list[str]:
    try:
        document = json.loads(body)
    except (ValueError, UnicodeDecodeError) as exc:
        msg = f"tags/list response is not JSON: {exc}"
        raise RegistryUnavailableError(msg) from exc
    if not isinstance(document, dict):
        raise RegistryUnavailableError("tags/list response is not a JSON object")
    tags = document.get("tags", [])
    if not isinstance(tags, list) or not all(isinstance(tag, str) for tag in tags):
        raise RegistryUnavailableError("tags/list 'tags' field is not a list of strings")
    return [str(tag) for tag in tags]


def _result(status: str, tags: list[str], detail: str) -> dict[str, Any]:
    return {"status": status, "tags": tags, "detail": detail}


def _tags_page_outcome(
    transport: Transport, url: str, bearer: str, timeout: float
) -> tuple[list[str], str | None]:
    """Return (tags, next_url) for a page; classified failures RAISE, never return None."""
    headers = {
        "Authorization": f"Bearer {bearer}",
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }
    status, response_headers, body = transport.get(url, headers, timeout)
    if status == HTTP_OK:
        return _tag_names(body), _next_page_url(response_headers)
    if status in (HTTP_UNAUTHORIZED, HTTP_NOT_FOUND):
        msg = f"tags/list returned {status} ({_oci_error_detail(body)}) after token exchange"
        raise PackageMissingError(msg)
    if status == HTTP_FORBIDDEN:
        msg = f"tags/list returned 403 ({_oci_error_detail(body)}): state is unknown, not absent"
        raise PermissionFailureError(msg)
    if HTTP_SERVER_ERROR_MIN <= status <= HTTP_SERVER_ERROR_MAX:
        msg = f"tags/list returned {status} ({_oci_error_detail(body)})"
        raise RegistryUnavailableError(msg)
    msg = f"tags/list returned unexpected status {status}"
    raise RegistryUnavailableError(msg)


def list_tags(
    owner: str,
    package: str = PACKAGE,
    token: str = "",
    *,
    transport: Transport | None = None,
    timeout: float = DEFAULT_TIMEOUT,
    max_pages: int = MAX_PAGES,
) -> dict[str, Any]:
    """Collect every image tag of ``owner/package`` from the GHCR OCI registry."""
    validate_namespace(owner, package)
    http = transport or UrllibTransport()
    try:
        bearer = _exchange_token(http, owner, package, token, timeout)
        url = f"{REGISTRY}/v2/{owner}/{package}/tags/list"
        tags: list[str] = []
        pages = 0
        while pages < max_pages:
            pages += 1
            names, following = _tags_page_outcome(http, url, bearer, timeout)
            tags.extend(names)
            if following is None:
                detail = f"collected {len(tags)} tag(s) over {pages} page(s)"
                return _result(STATUS_OK, tags, detail)
            url = following
        raise RegistryUnavailableError(
            f"pagination exceeded max_pages={max_pages}; registry state is unbounded"
        )
    except AuthFailureError as exc:
        return _result(STATUS_AUTH_FAILURE, [], str(exc))
    except PackageMissingError as exc:
        return _result(STATUS_PACKAGE_MISSING, [], str(exc))
    except PermissionFailureError as exc:
        return _result(STATUS_PERMISSION_FAILURE, [], str(exc))
    except NetworkFailureError as exc:
        return _result(STATUS_NETWORK_FAILURE, [], str(exc))
    except RegistryUnavailableError as exc:
        return _result(STATUS_UNAVAILABLE, [], str(exc))


def _fetch_json(
    transport: Transport,
    url: str,
    bearer: str,
    timeout: float,
    accept: str = ACCEPT_MANIFESTS,
) -> tuple[int, dict[str, str], Any, bytes]:
    headers = {
        "Authorization": f"Bearer {bearer}",
        "Accept": accept,
        "User-Agent": USER_AGENT,
    }
    status, response_headers, body = transport.get(url, headers, timeout)
    if status != HTTP_OK:
        return status, response_headers, None, b""
    try:
        return status, response_headers, json.loads(body), body
    except (ValueError, UnicodeDecodeError) as exc:
        msg = f"non-JSON document from {url}: {exc}"
        raise RegistryUnavailableError(msg) from exc


def _require_digest_format(digest: str) -> None:
    """Fail closed on any digest that cannot be verified as sha256.

    ``/blobs/`` and child-manifest URLs are built from registry-supplied
    digest strings, and provenance trust must never rest on an unverified
    document: ``sha512:...``, empty, uppercase-hex, or free-form strings are
    REJECTED (RegistryUnavailableError -> ``unknown_provenance``), not silently
    accepted. Matching is case-sensitive lowercase hex.
    """
    if not DIGEST_SHA256.fullmatch(digest):
        msg = f"digest {digest!r} is not a verifiable sha256 digest"
        raise RegistryUnavailableError(msg)


def _require_digest(digest: str, body: bytes) -> None:
    """Bounded integrity check for digest-addressed blobs and child manifests."""
    _require_digest_format(digest)
    if hashlib.sha256(body).hexdigest() != digest.removeprefix("sha256:"):
        raise RegistryUnavailableError(f"blob digest {digest} does not match its content")


def _annotation_revision(document: dict[str, Any]) -> str | None:
    """Read ``org.opencontainers.image.revision`` from a manifest's ``annotations``."""
    annotations = document.get("annotations")
    if isinstance(annotations, dict) and isinstance(annotations.get(REVISION_ANNOTATION), str):
        return str(annotations[REVISION_ANNOTATION])
    return None


def _config_label_revision(config_document: dict[str, Any]) -> str | None:
    """Docker-schema2 shape: labels live under ``config.config.Labels``."""
    inner = config_document.get("config")
    if isinstance(inner, dict):
        labels = inner.get("Labels")
        if isinstance(labels, dict) and isinstance(labels.get(REVISION_ANNOTATION), str):
            return str(labels[REVISION_ANNOTATION])
    return None


def _fetch_config_revision(
    transport: Transport,
    owner: str,
    package: str,
    bearer: str,
    document: dict[str, Any],
    timeout: float,
) -> str | None:
    """Follow the bounded manifest -> config chain to the revision annotation."""
    config = document.get("config")
    if not isinstance(config, dict) or not isinstance(config.get("digest"), str):
        raise RegistryUnavailableError("manifest is missing config.digest")
    digest = str(config["digest"])
    # Format is validated BEFORE the URL is built: an unverifiable digest must
    # never become a request path, and never yield provenance trust.
    _require_digest_format(digest)
    url = f"{REGISTRY}/v2/{owner}/{package}/blobs/{digest}"
    blob_headers = {
        "Authorization": f"Bearer {bearer}",
        "Accept": ACCEPT_CONFIG,
        "User-Agent": USER_AGENT,
    }
    status, _, body = transport.get(url, blob_headers, timeout)
    if status != HTTP_OK:
        msg = f"config blob fetch returned {status}"
        raise RegistryUnavailableError(msg)
    _require_digest(digest, body)
    try:
        config_document = json.loads(body)
    except (ValueError, UnicodeDecodeError) as exc:
        msg = f"config blob is not JSON: {exc}"
        raise RegistryUnavailableError(msg) from exc
    if not isinstance(config_document, dict):
        raise RegistryUnavailableError("config blob is not a JSON object")
    revision = _annotation_revision(config_document)
    if revision is None:
        revision = _config_label_revision(config_document)
    return revision


def _descriptor_platform(descriptor: dict[str, Any]) -> tuple[str, str, str] | None:
    """Runnable-image signature: a usable ``platform`` object in the index descriptor.

    Attestation manifests carry ``os``/``architecture`` of ``unknown`` (or no
    platform at all), so they never produce a usable platform tuple.
    """
    platform = descriptor.get("platform")
    if not isinstance(platform, dict):
        return None
    os_name = platform.get("os")
    architecture = platform.get("architecture")
    if not isinstance(os_name, str) or not isinstance(architecture, str):
        return None
    os_name = os_name.strip().lower()
    architecture = architecture.strip().lower()
    if not os_name or not architecture:
        return None
    if os_name == "unknown" or architecture == "unknown":
        return None
    variant = platform.get("variant")
    variant = variant.strip().lower() if isinstance(variant, str) else ""
    return os_name, architecture, variant


def _is_attestation_descriptor(descriptor: dict[str, Any]) -> bool:
    """True when the descriptor is annotated as an in-toto attestation manifest."""
    annotations = descriptor.get("annotations")
    if not isinstance(annotations, dict):
        return False
    for key, value in annotations.items():
        if not isinstance(key, str):
            continue
        lowered_key = key.lower()
        if any(marker in lowered_key for marker in _ATTESTATION_ANNOTATION_KEY_MARKERS):
            return True
        if isinstance(value, str):
            lowered_value = value.lower()
            if any(marker in lowered_value for marker in _ATTESTATION_ANNOTATION_VALUE_MARKERS):
                return True
    return False


def _select_runnable_descriptor(manifests: list[Any]) -> dict[str, Any] | None:
    """Pick the runnable image manifest descriptor of an index (attestations skipped).

    Candidates must carry a usable platform and (when mediaType is present) a
    manifest media type. Ranking is ``(preference, platform, list-order)``:

    * ``linux/amd64`` is preferred only in its bare form — ``_PREFERRED_PLATFORM``
      has an EMPTY variant, so ``linux/amd64/v4`` is NOT the preferred rank and
      falls into the alphabetical group (the build host image is plain amd64).
    * Remaining candidates sort by ``(os, architecture, variant)`` alphabetically,
      then by list index, so the chosen chain is stable across runs.
    * A descriptor whose mediaType is present but is NOT an image-manifest type is
      skipped — this includes nested INDEX media types, so an index-of-index with
      typed descriptors has no candidate and fails closed (``unknown_provenance``)
      rather than trusting a non-image child. Untyped (mediaType-less) descriptors
      that resolve to a nested index are recursed into by ``_index_revision``.
    """
    ranked: list[tuple[tuple[int, tuple[str, str, str], int], dict[str, Any]]] = []
    for index, descriptor in enumerate(manifests):
        if not isinstance(descriptor, dict) or not isinstance(descriptor.get("digest"), str):
            continue
        media_type = descriptor.get("mediaType")
        if (
            isinstance(media_type, str)
            and media_type.strip()
            and media_type not in _IMAGE_MANIFEST_TYPES
        ):
            # Non-image media types (nested index, artifact/attestation-only types)
            # are never treated as the runnable image.
            continue
        if _is_attestation_descriptor(descriptor):
            continue
        platform = _descriptor_platform(descriptor)
        if platform is None:
            continue
        rank = (0 if platform == _PREFERRED_PLATFORM else 1, platform, index)
        ranked.append((rank, descriptor))
    if not ranked:
        return None
    ranked.sort(key=lambda item: item[0])
    return ranked[0][1]


def _child_media_type(headers: dict[str, str], document: dict[str, Any]) -> str:
    content_type = str(headers.get("content-type", "")).split(";")[0].strip()
    return content_type or str(document.get("mediaType", ""))


def _index_revision(
    transport: Transport,
    owner: str,
    package: str,
    bearer: str,
    document: dict[str, Any],
    timeout: float,
    depth: int = 0,
) -> str | None:
    """Resolve the revision of an image index (annotations, else the runnable manifest chain)."""
    revision = _annotation_revision(document)
    if revision is not None:
        return revision
    if depth >= MAX_INDEX_DEPTH:
        raise RegistryUnavailableError(
            f"image index nesting exceeds the depth bound {MAX_INDEX_DEPTH}"
        )
    manifests = document.get("manifests")
    if not isinstance(manifests, list) or not manifests:
        raise RegistryUnavailableError("image index carries no manifests")
    descriptor = _select_runnable_descriptor(manifests)
    if descriptor is None:
        raise RegistryUnavailableError(
            "image index carries no identifiable runnable image manifest (attestation-only?)"
        )
    digest = str(descriptor["digest"])
    # Validated BEFORE the URL is built: an unverifiable descriptor
    # digest is a fail-closed (unknown_provenance), never a request path.
    _require_digest_format(digest)
    # OCI Distribution Spec: child MANIFESTS come from /manifests/, not /blobs/.
    manifest_url = f"{REGISTRY}/v2/{owner}/{package}/manifests/{digest}"
    inner_status, inner_headers, inner, inner_body = _fetch_json(
        transport, manifest_url, bearer, timeout
    )
    if inner_status != HTTP_OK:
        msg = f"nested manifest fetch returned {inner_status}"
        raise RegistryUnavailableError(msg)
    if not isinstance(inner, dict):
        raise RegistryUnavailableError("nested manifest fetch is not a JSON object")
    _require_digest(digest, inner_body)
    revision = _annotation_revision(inner)
    if revision is not None:
        return revision
    if _child_media_type(inner_headers, inner) in _INDEX_TYPES:
        return _index_revision(transport, owner, package, bearer, inner, timeout, depth + 1)
    return _fetch_config_revision(transport, owner, package, bearer, inner, timeout)


def verify_sha_tag(
    full_sha: str,
    owner: str,
    package: str = PACKAGE,
    token: str = "",
    *,
    transport: Transport | None = None,
    timeout: float = DEFAULT_TIMEOUT,
) -> str:
    """Best-effort, bounded provenance check of one ``sha-<fullsha>`` tag."""
    if not FULL_SHA.match(full_sha):
        msg = f"--shas entries must be 40-char lowercase hex SHAs (got {full_sha!r})"
        raise ValueError(msg)
    validate_namespace(owner, package)
    http = transport or UrllibTransport()
    tag = f"sha-{full_sha}"
    try:
        bearer = _exchange_token(http, owner, package, token, timeout)
        # Trust boundary: the tag-addressed top document is the registry's own
        # answer (TLS/auth-bound, no client-side digest to check). Every
        # digest-addressed document it references — child manifests and config
        # blobs — is format-validated and hash-verified before use.
        manifest_url = f"{REGISTRY}/v2/{owner}/{package}/manifests/{tag}"
        status, headers, document, _raw = _fetch_json(http, manifest_url, bearer, timeout)
        if status == HTTP_NOT_FOUND:
            return PROVENANCE_NOT_PUBLISHED
        if status != HTTP_OK:
            msg = f"manifest fetch returned {status}"
            raise RegistryUnavailableError(msg)
        if not isinstance(document, dict):
            raise RegistryUnavailableError("manifest document is not a JSON object")
        content_type = str(headers.get("content-type", "")).split(";")[0].strip()
        media_type = content_type or str(document.get("mediaType", ""))
        if media_type in _INDEX_TYPES:
            revision = _index_revision(http, owner, package, bearer, document, timeout)
        else:
            revision = _annotation_revision(document)
            if revision is None:
                revision = _fetch_config_revision(http, owner, package, bearer, document, timeout)
        if revision is not None and revision.lower() == full_sha:
            return PROVENANCE_PUBLISHED
        msg = f"revision annotation {revision!r} does not match {full_sha}"
        raise RegistryUnavailableError(msg)
    except (
        AuthFailureError,
        PermissionFailureError,
        PackageMissingError,
        NetworkFailureError,
        RegistryUnavailableError,
    ):
        # Conservative: an unverifiable tag is NOT counted as published.
        # The tag EXISTS and its content is unknown, so the consumer must not
        # re-push/overwrite it — operator reconciliation is required (see the
        # publication policy in .github/workflows/publish.yml).
        return PROVENANCE_UNKNOWN


def plan_sha_tags(document: dict[str, Any]) -> list[str]:
    """Plan-consumer helper: SHA tag names, aborting on unknown registry state."""
    status = document.get("status")
    if status not in SUCCESS_STATUSES:
        detail = document.get("detail", "")
        msg = f"registry status {status!r} ({detail}); refusing to publish against unknown state"
        raise PlanAbortedError(msg)
    tags = document.get("tags", [])
    if not isinstance(tags, list):
        raise PlanAbortedError("registry discovery document has a non-list tags field")
    return [str(tag) for tag in tags if str(tag).startswith("sha-")]


def _read_token(token_env: str) -> str:
    token = os.environ.get(token_env, "").strip()
    if not token:
        msg = f"token env {token_env!r} unset/empty (plan wires GITHUB_TOKEN via env indirection)"
        raise AuthFailureError(msg)
    return token


def _emit(payload: dict[str, Any]) -> None:
    json.dump(payload, sys.stdout, indent=2)
    sys.stdout.write("\n")


def _run_list(args: argparse.Namespace) -> int:
    try:
        token = _read_token(args.token_env)
    except AuthFailureError as exc:
        _emit(_result(STATUS_AUTH_FAILURE, [], str(exc)))
        return 1
    try:
        document = list_tags(args.owner, args.package, token, timeout=args.timeout)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    _emit(document)
    return 0 if document["status"] in SUCCESS_STATUSES else 1


def _run_verify(args: argparse.Namespace) -> int:
    shas = [item.strip() for item in args.shas.split(",") if item.strip()]
    if not shas:
        print("error: --shas must list at least one 40-hex SHA", file=sys.stderr)
        return 2
    if len(shas) > VERIFY_CAP:
        print(f"error: --shas exceeds the bounded provenance cap of {VERIFY_CAP}", file=sys.stderr)
        return 2
    try:
        validate_namespace(args.owner, args.package)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    try:
        token = _read_token(args.token_env)
    except AuthFailureError as exc:
        # Emit the classified JSON so the plan step can abort with
        # a clear message instead of choking on an empty response document.
        _emit({"status": STATUS_AUTH_FAILURE, "results": {}, "detail": str(exc)})
        return 1
    results: dict[str, str] = {}
    for sha in shas:
        try:
            results[sha] = verify_sha_tag(
                sha, args.owner, args.package, token, timeout=args.timeout
            )
        except ValueError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
    published = sum(1 for outcome in results.values() if outcome == PROVENANCE_PUBLISHED)
    detail = (
        f"provenance over {len(results)} tag(s): {published} published, "
        f"{sum(1 for o in results.values() if o == PROVENANCE_NOT_PUBLISHED)} not published, "
        f"{sum(1 for o in results.values() if o == PROVENANCE_UNKNOWN)} unverified"
    )
    _emit({"status": STATUS_OK, "results": results, "detail": detail})
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    listing = sub.add_parser("list", help="collect GHCR image tags for owner/package")
    listing.add_argument("--owner", required=True, help="lowercase GHCR namespace owner")
    listing.add_argument("--package", default=PACKAGE)
    listing.add_argument("--token-env", default="GHCR_TOKEN", help="env var carrying GITHUB_TOKEN")
    listing.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)

    verify = sub.add_parser("verify", help="bounded provenance check of sha- tags")
    verify.add_argument("--owner", required=True)
    verify.add_argument("--package", default=PACKAGE)
    verify.add_argument(
        "--shas", required=True, help=f"comma-separated 40-hex SHAs (cap {VERIFY_CAP})"
    )
    verify.add_argument("--token-env", default="GHCR_TOKEN")
    verify.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "list":
        return _run_list(args)
    return _run_verify(args)


if __name__ == "__main__":
    raise SystemExit(main())
