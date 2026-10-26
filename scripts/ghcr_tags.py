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
that the registry answers 404 for, or a manifest 401 carrying the OCI error
code ``NAME_UNKNOWN`` after token exchange, is ``not_published`` (absent, safe
to publish). Other auth/fetch/parse failures yield ``unknown_provenance``:
registry/tag state or provenance could not be verified, so the CONSUMER must
NOT publish or overwrite until reconciled — a ``sha-`` tag NAME derived from
a commit is not proof that registry state or image provenance is known.
Publication policy (skip /
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
from collections.abc import Iterable, Sequence
from typing import Any, Protocol, cast

REGISTRY = "https://ghcr.io"
PACKAGE = "shoko-sonarr-linker"
USER_AGENT = "shoko-sonarr-linker-ghcr-tags/1.0"
NAME_SEGMENT = re.compile(r"^[a-z0-9]+(?:[._-][a-z0-9]+)*$")
FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
DIGEST_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")
LINK_PARAM_TOKEN = re.compile(r"[!#$%&'*+\-.^_`|~0-9A-Za-z]+")

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
_SUPPORTED_MANIFEST_MEDIA_TYPES = frozenset(
    media_type.lower() for media_type in _MANIFEST_MEDIA_TYPES
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
_SUPPORTED_CONFIG_MEDIA_TYPES = frozenset(
    {
        "application/vnd.oci.image.config.v1+json",
        "application/vnd.docker.container.image.v1+json",
    }
)
_GENERIC_CONFIG_MEDIA_TYPE = "application/octet-stream"
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


def _https_origin(url: str) -> tuple[str, str, int]:
    parsed = urllib.parse.urlsplit(url)
    hostname = parsed.hostname
    if parsed.scheme.lower() != "https" or hostname is None:
        raise ValueError("redirect URL must have an HTTPS origin")
    port = parsed.port
    return "https", hostname.lower(), 443 if port is None else port


class _AuthorizationSafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Restrict redirects to HTTPS and credentials to their original origin."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> urllib.request.Request | None:
        try:
            source_origin = _https_origin(req.full_url)
            destination_origin = _https_origin(newurl)
        except ValueError as exc:
            raise urllib.error.HTTPError(
                req.full_url,
                code,
                "refusing redirect without a valid HTTPS origin",
                headers,
                fp,
            ) from exc

        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected is not None and source_origin != destination_origin:
            protected_headers = {"authorization", "proxy-authorization"}
            for header_map in (redirected.headers, redirected.unredirected_hdrs):
                for name in list(header_map):
                    if name.lower() in protected_headers:
                        del header_map[name]
        return redirected


def _open(request: urllib.request.Request, timeout: float) -> Response:
    """Single seam for the HTTP opener so tests can monkeypatch it."""
    opener = urllib.request.build_opener(_AuthorizationSafeRedirectHandler())
    response = opener.open(request, timeout=timeout)
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
                raw_headers = response.getheaders()
        except urllib.error.HTTPError as exc:
            body = exc.read()
            status = int(exc.code)
            raw_headers = exc.headers.items()
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            msg = f"transport error for {url}: {exc}"
            raise NetworkFailureError(msg) from exc
        lowered = _normalize_headers(raw_headers)
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


def _normalize_headers(headers: Iterable[tuple[str, str]]) -> dict[str, str]:
    """Lowercase response header names and combine repeated Link fields."""
    normalized: dict[str, str] = {}
    link_values: list[str] = []
    for name, value in headers:
        key = name.lower()
        if key == "link":
            link_values.append(value.strip())
        elif key == "content-type":
            if key in normalized and normalized[key] != value:
                raise RegistryUnavailableError("conflicting duplicate Content-Type headers")
            normalized[key] = value
        else:
            normalized[key] = value
    if link_values:
        normalized["link"] = ", ".join(link_values)
    return normalized


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


def _has_oci_error_code(body: bytes, code: str) -> bool:
    """Return whether an OCI error response explicitly carries ``code``."""
    try:
        document = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return False
    errors = document.get("errors") if isinstance(document, dict) else None
    return isinstance(errors, list) and any(
        isinstance(item, dict) and item.get("code") == code for item in errors
    )


def _advance_link_state(
    char: str, in_target: bool, in_quote: bool, escaped: bool
) -> tuple[bool, bool, bool, bool]:
    """Advance Link-field scanning state; the final item marks a list comma."""
    separator = False
    if in_quote:
        if escaped:
            escaped = False
        elif char == "\\":
            escaped = True
        elif char == '"':
            in_quote = False
    elif in_target:
        if char == ">":
            in_target = False
        elif char in '<"':
            raise RegistryUnavailableError("malformed Link header: invalid URI-reference")
    elif char == '"':
        in_quote = True
    elif char == "<":
        in_target = True
    elif char == ">":
        raise RegistryUnavailableError("malformed Link header: unmatched '>'")
    elif char == ",":
        separator = True
    return in_target, in_quote, escaped, separator


def _split_link_values(header: str) -> list[str]:
    """Split a Link field at commas outside targets and quoted parameter values."""
    values: list[str] = []
    start = 0
    in_target = False
    in_quote = False
    escaped = False
    for index, char in enumerate(header):
        in_target, in_quote, escaped, separator = _advance_link_state(
            char, in_target, in_quote, escaped
        )
        if separator:
            value = header[start:index].strip()
            if value:
                values.append(value)
            start = index + 1
    if in_quote or escaped or in_target:
        raise RegistryUnavailableError("malformed Link header: unterminated quote or URI-reference")
    final_value = header[start:].strip()
    if final_value:
        values.append(final_value)
    return values


def _skip_link_whitespace(value: str, position: int) -> int:
    while position < len(value) and value[position] in " \t":
        position += 1
    return position


def _parse_quoted_link_parameter(value: str, position: int) -> tuple[str, int]:
    parsed: list[str] = []
    position += 1
    while position < len(value):
        char = value[position]
        position += 1
        if char == "\\":
            if position == len(value):
                raise RegistryUnavailableError("malformed Link header: invalid quoted escape")
            parsed.append(value[position])
            position += 1
        elif char == '"':
            return "".join(parsed), position
        else:
            parsed.append(char)
    raise RegistryUnavailableError("malformed Link header: unterminated quoted value")


def _parse_link_parameter(value: str, position: int) -> tuple[str, str | None, int]:
    position = _skip_link_whitespace(value, position)
    name_match = LINK_PARAM_TOKEN.match(value, position)
    if name_match is None:
        raise RegistryUnavailableError("malformed Link header: invalid parameter name")
    name = name_match.group(0).lower()
    position = _skip_link_whitespace(value, name_match.end())
    if position == len(value) or value[position] != "=":
        return name, None, position
    position = _skip_link_whitespace(value, position + 1)
    if position == len(value):
        raise RegistryUnavailableError("malformed Link header: parameter has no value")
    if value[position] == '"':
        parameter_value, position = _parse_quoted_link_parameter(value, position)
        return name, parameter_value, position

    value_start = position
    while position < len(value) and value[position] not in "; \t":
        if value[position] in '<">':
            raise RegistryUnavailableError("malformed Link header: invalid parameter value")
        position += 1
    if position == value_start:
        raise RegistryUnavailableError("malformed Link header: parameter has no value")
    return name, value[value_start:position], position


def _parse_link_value(value: str) -> tuple[str, list[str]]:
    """Parse one Link value and collect its relation types."""
    value = value.strip()
    if not value.startswith("<"):
        raise RegistryUnavailableError("malformed Link header: link-value must start with '<'")
    target_end = value.find(">")
    if target_end <= 1:
        raise RegistryUnavailableError("malformed Link header: empty or unterminated target")
    target = value[1:target_end]
    if any(char.isspace() for char in target):
        raise RegistryUnavailableError("malformed Link header: whitespace in URI-reference")

    relations: list[str] | None = None
    rel_seen = False
    position = target_end + 1
    while position < len(value):
        position = _skip_link_whitespace(value, position)
        if position == len(value):
            break
        if value[position] != ";":
            raise RegistryUnavailableError("malformed Link header: expected ';' parameter")
        name, parameter_value, position = _parse_link_parameter(value, position + 1)
        if name == "rel":
            if rel_seen:
                raise RegistryUnavailableError("malformed Link header: duplicate rel parameter")
            rel_seen = True
            if parameter_value is None:
                raise RegistryUnavailableError("malformed Link header: rel parameter has no value")
            relations = parameter_value.split()
            if not relations:
                raise RegistryUnavailableError("malformed Link header: rel parameter has no value")
    if relations is None:
        raise RegistryUnavailableError("malformed Link header: missing rel parameter")
    return target, [relation.lower() for relation in relations]


def _next_page_url(headers: dict[str, str], current_url: str) -> str | None:
    link = headers.get("link")
    if link is None:
        return None
    if not link.strip():
        raise RegistryUnavailableError("malformed Link header: empty field value")
    targets: list[str] = []
    for link_value in _split_link_values(link):
        target, relations = _parse_link_value(link_value)
        if "next" in relations:
            resolved = urllib.parse.urljoin(current_url, target)
            targets.append(_registry_url(resolved))
    if len(targets) > 1:
        raise RegistryUnavailableError("malformed Link header: multiple rel=next targets")
    return targets[0] if targets else None


def _tag_names(body: bytes) -> list[str]:
    try:
        document = json.loads(body)
    except (ValueError, UnicodeDecodeError) as exc:
        msg = f"tags/list response is not JSON: {exc}"
        raise RegistryUnavailableError(msg) from exc
    if not isinstance(document, dict):
        raise RegistryUnavailableError("tags/list response is not a JSON object")
    if "tags" not in document:
        raise RegistryUnavailableError("tags/list response is missing the required 'tags' array")
    tags = document["tags"]
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
        return _tag_names(body), _next_page_url(response_headers, url)
    if status in (HTTP_UNAUTHORIZED, HTTP_NOT_FOUND):
        detail = _oci_error_detail(body)
        if _has_oci_error_code(body, "NAME_UNKNOWN"):
            raise PackageMissingError(
                f"tags/list returned {status} ({detail}) after token exchange"
            )
        if status == HTTP_UNAUTHORIZED:
            raise AuthFailureError(f"tags/list returned 401 ({detail}) after token exchange")
        raise RegistryUnavailableError(f"tags/list returned 404 ({detail}) after token exchange")
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
        return status, response_headers, None, body
    try:
        return status, response_headers, json.loads(body), body
    except (ValueError, UnicodeDecodeError) as exc:
        msg = f"non-JSON document from {url}: {exc}"
        raise RegistryUnavailableError(msg) from exc


def _validated_manifest_media_type(headers: dict[str, str], document: dict[str, Any]) -> str:
    """Require a supported image manifest/index type and consistent declarations."""
    header_media_type: str | None = None
    if "content-type" in headers:
        raw_header_type = headers["content-type"]
        if not isinstance(raw_header_type, str):
            raise RegistryUnavailableError("manifest Content-Type is not a string")
        header_media_type = raw_header_type.split(";", 1)[0].strip().lower()
        if not header_media_type:
            raise RegistryUnavailableError("manifest Content-Type is empty")

    body_media_type: str | None = None
    if "mediaType" in document:
        raw_body_type = document["mediaType"]
        if not isinstance(raw_body_type, str):
            raise RegistryUnavailableError("manifest mediaType field is not a string")
        body_media_type = raw_body_type.split(";", 1)[0].strip().lower()
        if not body_media_type:
            raise RegistryUnavailableError("manifest mediaType field is empty")

    if (
        header_media_type is not None
        and body_media_type is not None
        and header_media_type != body_media_type
    ):
        raise RegistryUnavailableError(
            f"manifest Content-Type {header_media_type!r} disagrees with body mediaType "
            f"{body_media_type!r}"
        )
    media_type = header_media_type or body_media_type
    if media_type not in _SUPPORTED_MANIFEST_MEDIA_TYPES:
        raise RegistryUnavailableError(
            f"unsupported or missing image manifest media type {media_type!r}"
        )
    return media_type


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


def _validate_config_content_type(headers: dict[str, str], descriptor_media_type: str) -> None:
    if "content-type" not in headers:
        return
    raw_response_type = headers["content-type"]
    if not isinstance(raw_response_type, str):
        raise RegistryUnavailableError("config blob Content-Type is not a string")
    response_media_type = raw_response_type.split(";", 1)[0].strip().lower()
    if not response_media_type:
        raise RegistryUnavailableError("config blob Content-Type is empty")
    if response_media_type not in (_SUPPORTED_CONFIG_MEDIA_TYPES | {_GENERIC_CONFIG_MEDIA_TYPE}):
        raise RegistryUnavailableError(
            f"unsupported config blob Content-Type {response_media_type!r}"
        )
    if response_media_type not in {_GENERIC_CONFIG_MEDIA_TYPE, descriptor_media_type}:
        raise RegistryUnavailableError(
            f"config blob Content-Type {response_media_type!r} disagrees with "
            f"descriptor mediaType {descriptor_media_type!r}"
        )


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
    if not isinstance(config, dict):
        raise RegistryUnavailableError("manifest is missing config descriptor")
    raw_media_type = config.get("mediaType")
    if not isinstance(raw_media_type, str):
        raise RegistryUnavailableError("manifest config descriptor is missing mediaType")
    descriptor_media_type = raw_media_type.strip().lower()
    if descriptor_media_type not in _SUPPORTED_CONFIG_MEDIA_TYPES:
        raise RegistryUnavailableError(
            f"unsupported config descriptor mediaType {descriptor_media_type!r}"
        )
    if not isinstance(config.get("digest"), str):
        raise RegistryUnavailableError("manifest config descriptor is missing digest")
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
    status, response_headers, body = transport.get(url, blob_headers, timeout)
    if status != HTTP_OK:
        msg = f"config blob fetch returned {status}"
        raise RegistryUnavailableError(msg)
    _validate_config_content_type(response_headers, descriptor_media_type)
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
            and media_type.strip().lower() not in _IMAGE_MANIFEST_TYPES
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


def _index_revision(
    transport: Transport,
    owner: str,
    package: str,
    bearer: str,
    document: dict[str, Any],
    headers: dict[str, str],
    timeout: float,
    depth: int = 0,
) -> str | None:
    """Resolve the revision of an image index (annotations, else the runnable manifest chain)."""
    media_type = _validated_manifest_media_type(headers, document)
    if media_type not in _INDEX_TYPES:
        raise RegistryUnavailableError(f"expected an image index, got media type {media_type!r}")
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
    inner_media_type = _validated_manifest_media_type(inner_headers, inner)
    if "mediaType" in descriptor:
        descriptor_media_type = descriptor["mediaType"]
        if not isinstance(descriptor_media_type, str) or not descriptor_media_type.strip():
            raise RegistryUnavailableError("index descriptor mediaType is malformed")
        normalized_descriptor_type = descriptor_media_type.strip().lower()
        if normalized_descriptor_type != inner_media_type:
            raise RegistryUnavailableError(
                f"index descriptor mediaType {normalized_descriptor_type!r} disagrees with "
                f"fetched child media type {inner_media_type!r}"
            )
    _require_digest(digest, inner_body)
    revision = _annotation_revision(inner)
    if revision is not None:
        return revision
    if inner_media_type in _INDEX_TYPES:
        return _index_revision(
            transport, owner, package, bearer, inner, inner_headers, timeout, depth + 1
        )
    return _fetch_config_revision(transport, owner, package, bearer, inner, timeout)


def verify_sha_tag(  # noqa: PLR0912 -- keep the fail-closed registry/digest checks explicit
    full_sha: str,
    owner: str,
    package: str = PACKAGE,
    token: str = "",
    *,
    transport: Transport | None = None,
    timeout: float = DEFAULT_TIMEOUT,
    digest: str | None = None,
) -> str:
    """Check a SHA tag, or its resolved digest for race-free image promotion."""
    if not FULL_SHA.match(full_sha):
        msg = f"--shas entries must be 40-char lowercase hex SHAs (got {full_sha!r})"
        raise ValueError(msg)
    validate_namespace(owner, package)
    http = transport or UrllibTransport()
    tag = f"sha-{full_sha}"
    try:
        if digest is not None:
            _require_digest_format(digest)
        bearer = _exchange_token(http, owner, package, token, timeout)
        # Trust boundary: the tag-addressed top document is the registry's own
        # answer (TLS/auth-bound, no client-side digest to check). Every
        # digest-addressed document it references — child manifests and config
        # blobs — is format-validated and hash-verified before use.
        manifest_url = f"{REGISTRY}/v2/{owner}/{package}/manifests/{digest or tag}"
        status, headers, document, raw = _fetch_json(http, manifest_url, bearer, timeout)
        if status == HTTP_NOT_FOUND:
            if _has_oci_error_code(raw, "MANIFEST_UNKNOWN") or _has_oci_error_code(
                raw, "NAME_UNKNOWN"
            ):
                return PROVENANCE_NOT_PUBLISHED
            raise RegistryUnavailableError(
                "manifest fetch returned 404 without a recognized absence code "
                f"({_oci_error_detail(raw)})"
            )
        if status == HTTP_UNAUTHORIZED and _has_oci_error_code(raw, "NAME_UNKNOWN"):
            return PROVENANCE_NOT_PUBLISHED
        if status != HTTP_OK:
            msg = f"manifest fetch returned {status}"
            raise RegistryUnavailableError(msg)
        if digest is not None:
            _require_digest(digest, raw)
        if not isinstance(document, dict):
            raise RegistryUnavailableError("manifest document is not a JSON object")
        media_type = _validated_manifest_media_type(headers, document)
        if media_type in _INDEX_TYPES:
            revision = _index_revision(http, owner, package, bearer, document, headers, timeout)
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
        # Conservative: registry/tag state or provenance is not verified, so
        # the consumer must not publish or overwrite (see workflow policy).
        return PROVENANCE_UNKNOWN


def plan_sha_tags(document: dict[str, Any]) -> list[str]:
    """Plan-consumer helper: SHA tag names, aborting on unknown registry state."""
    status = document.get("status")
    if status not in SUCCESS_STATUSES:
        detail = document.get("detail", "")
        msg = f"registry status {status!r} ({detail}); refusing to publish against unknown state"
        raise PlanAbortedError(msg)
    raw_tags = document.get("tags")
    if not isinstance(raw_tags, list) or any(not isinstance(tag, str) for tag in raw_tags):
        raise PlanAbortedError("registry discovery document must contain a list of string tags")
    if status == STATUS_PACKAGE_MISSING and raw_tags:
        raise PlanAbortedError("package_missing registry response must have an empty tags list")
    sha_tags: list[str] = []
    for tag in raw_tags:
        if tag.startswith("sha-"):
            sha_value = tag.removeprefix("sha-")
        elif tag.lstrip().lower().startswith("sha-"):
            raise PlanAbortedError(
                f"invalid sha tag {tag!r}; expected sha- followed by 40 lowercase hex digits"
            )
        else:
            continue
        if FULL_SHA.fullmatch(sha_value) is None:
            raise PlanAbortedError(
                f"invalid sha tag {tag!r}; expected sha- followed by 40 lowercase hex digits"
            )
        sha_tags.append(tag)
    return sha_tags


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
        if args.digest is not None and len(shas) != 1:
            raise ValueError("--digest requires exactly one SHA")
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
                sha, args.owner, args.package, token, timeout=args.timeout, digest=args.digest
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
    verify.add_argument(
        "--digest", help="verify a resolved manifest digest instead of a mutable tag"
    )
    verify.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "list":
        return _run_list(args)
    return _run_verify(args)


if __name__ == "__main__":
    raise SystemExit(main())
