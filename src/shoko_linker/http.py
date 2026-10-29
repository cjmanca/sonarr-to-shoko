"""Thin httpx client factories with explicit, auditable request behavior.

Policy (deliberate, not incidental):

* ``follow_redirects=False``. Redirects are followed only by :func:`resolve_and_send`,
  which walks same-origin 3xx targets up to ``MAX_REDIRECTS`` hops. This exists because
  Sonarr answers requests to a bare path with HTTP 307 when a ``urlBase`` is configured.
* Auth headers are attached per request from a factory, so key values are read from the
  environment at access time. When a redirect target has a **different origin** the auth
  headers are dropped: an API key is never sent to a host that did not issue it.
* **No automatic retries for any request**, and explicitly none for non-idempotent ones
  (``retries=0`` transport, no backoff loop). A POST is replayed at most once per hop
  as part of the manual same-origin redirect walk (301/302/307/308 redirect to a new
  target — each original request is delivered exactly once), which is the
  redirect contract, not a retry. 303 after a non-idempotent method is refused, never
  replayed. The walk is capped at ``MAX_REDIRECTS`` hops.
* :func:`resolve_and_send` binds the first hop to the client base URL's origin and
  refuses an absolute ``url`` from a foreign origin: service auth headers only ever
  target the configured endpoint.
"""

from __future__ import annotations

import json as json_module
import logging
import math
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any, Final

import httpx

from .config import Settings
from .errors import HttpPolicyError
from .logging_setup import MIN_SECRET_LENGTH, register_secret

_logger = logging.getLogger(__name__)

type HeadersFactory = Callable[[], dict[str, str]]

AUTH_HEADER_NAMES: Final[frozenset[str]] = frozenset(
    {"x-api-key", "apikey", "authorization", "proxy-authorization"}
)
REDIRECT_STATUSES: Final[frozenset[int]] = frozenset({301, 302, 303, 307, 308})
_SEE_OTHER: Final[int] = 303
MAX_REDIRECTS: Final[int] = 5
_MIN_HTTP_STATUS: Final[int] = 100
_MAX_HTTP_STATUS: Final[int] = 599
# About 18 KiB per episode for the supported 7,000-episode library; most
# inventories are per-series or paged and substantially smaller.
MAX_INVENTORY_JSON_BYTES: Final[int] = 128 * 1024 * 1024
_IDEMPOTENT_METHODS: Final[frozenset[str]] = frozenset(
    {"GET", "HEAD", "OPTIONS", "TRACE", "DELETE", "PUT"}
)


class ApiEndpoint:
    """A service client plus the factory that supplies its auth headers."""

    def __init__(self, client: httpx.Client, headers_factory: HeadersFactory) -> None:
        self.client = client
        self.headers_factory = headers_factory

    @property
    def base_url(self) -> httpx.URL:
        return self.client.base_url


def make_timeout(connect_seconds: float, read_seconds: float) -> httpx.Timeout:
    """Explicit timeout: ``read`` as the general value, ``connect`` for connection setup."""
    return httpx.Timeout(read_seconds, connect=connect_seconds)


def make_client(
    base_url: str,
    headers_factory: HeadersFactory | None = None,
    timeout: httpx.Timeout | None = None,
    *,
    secret_values: Sequence[str] = (),
) -> ApiEndpoint:
    """Build an endpoint whose client never auto-follows redirects and never retries.

    Auth headers come from ``headers_factory`` at request time (see module docstring)
    and are registered for log redaction. The client holds no key material itself.

    The base URL is stored with a trailing slash: URL resolution is relative to
    the base, and a base like ``http://host/sonarr/`` keeps its ``urlBase`` prefix for
    every request. API paths are passed *relative* (no leading slash) —
    a leading slash is RFC-absolute and would drop the base path.
    """
    client = httpx.Client(
        base_url=base_url.rstrip("/") + "/",
        timeout=timeout if timeout is not None else make_timeout(10.0, 30.0),
        follow_redirects=False,
        transport=httpx.HTTPTransport(retries=0),
    )
    for value in secret_values:
        if not register_secret(value):
            _logger.warning(
                "an API key of %d characters is below the %d-character redaction floor; "
                "log redaction cannot scrub it — rotate to a longer key (the value itself "
                "is never logged)",
                len(value),
                MIN_SECRET_LENGTH,
            )
    return ApiEndpoint(client, headers_factory if headers_factory is not None else _no_headers)


def sonarr_headers(api_key: str) -> dict[str, str]:
    """Sonarr v3 auth header. Header is preferred over ``?apikey=`` to keep keys out of URLs."""
    return {"X-Api-Key": api_key}


def shoko_headers(api_key: str) -> dict[str, str]:
    """Shoko API v3 auth header (``apikey``). Header is preferred over ``?apikey=``."""
    return {"apikey": api_key}


def make_sonarr_client(settings: Settings) -> ApiEndpoint:
    """Endpoint for the configured Sonarr instance (reads only, by project policy)."""
    api_key = settings.sonarr.api_key.resolve()
    return make_client(
        settings.sonarr.url,
        _factory_with_secret(sonarr_headers, api_key),
        _timeout(settings),
        secret_values=(api_key,),
    )


def make_shoko_client(settings: Settings) -> ApiEndpoint:
    """Endpoint for the configured Shoko instance."""
    api_key = settings.shoko.api_key.resolve()
    return make_client(
        settings.shoko.url,
        _factory_with_secret(shoko_headers, api_key),
        _timeout(settings),
        secret_values=(api_key,),
    )


def strip_auth_headers(headers: Mapping[str, str]) -> dict[str, str]:
    """Return ``headers`` without any authentication header."""
    return {name: value for name, value in headers.items() if name.lower() not in AUTH_HEADER_NAMES}


def same_origin(first: httpx.URL, second: httpx.URL) -> bool:
    """True when scheme, host and port match exactly."""
    return (
        first.scheme.lower() == second.scheme.lower()
        and first.host.lower() == second.host.lower()
        and first.port == second.port
    )


def resolve_and_send(
    endpoint: ApiEndpoint,
    method: str,
    url: str,
    *,
    params: Mapping[str, Any] | None = None,
    json: Any | None = None,
    max_redirects: int = MAX_REDIRECTS,
    stream: bool = False,
    timeout: httpx.Timeout | None = None,
    headers: Mapping[str, str] | None = None,
    same_origin_only: bool = False,
    deadline: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> httpx.Response:
    """Send one request, following same-origin redirects manually.

    Cross-origin redirect targets are requested **without** auth headers. The query of
    the initial request is not re-applied to redirect targets: a ``Location`` carries the
    full replacement URI, and re-appending parameters would duplicate it.

    Raises:
        HttpPolicyError: when the redirect cap is exceeded, when a 303 follows a
            non-idempotent method, or when a redirect carries no usable ``Location``.

    When ``stream=True``, the returned body is unconsumed; the caller must close the response.
    ``timeout`` optionally narrows the client's timeout for a bounded read operation.
    An absolute ``deadline`` checks time before each redirect hop and further narrows
    that hop's timeouts. It cannot interrupt in-flight I/O beyond those timeouts.
    Extra ``headers`` cannot replace endpoint auth. ``same_origin_only`` refuses
    foreign redirect targets before issuing any request to them.
    """
    if deadline is not None and not math.isfinite(deadline):
        raise ValueError("request deadline must be finite")
    verb = method.upper()
    request_headers = dict(headers or {})
    request_headers.update(endpoint.headers_factory())
    origin = _resolve(endpoint.client, url)
    if not same_origin(endpoint.client.base_url, origin):
        raise HttpPolicyError(
            f"refusing {verb} to {str(origin)!r}: its origin does not match the client "
            f"base {str(endpoint.client.base_url)!r} (auth headers stay with their endpoint)"
        )
    target = origin
    for hop in range(max_redirects + 1):
        hop_timeout = timeout if timeout is not None else endpoint.client.timeout
        if deadline is not None:
            remaining = deadline - clock()
            if remaining <= 0:
                raise httpx.TimeoutException("request deadline exceeded")
            hop_timeout = httpx.Timeout(
                **{
                    name: remaining if value is None else min(value, remaining)
                    for name, value in hop_timeout.as_dict().items()
                }
            )
        if not same_origin(origin, target):
            # The key belongs to the origin that issued it; cross-origin hops are anonymous.
            request_headers = strip_auth_headers(request_headers)
        response = endpoint.client.send(
            endpoint.client.build_request(
                verb,
                target,
                params=params if hop == 0 else None,
                json=json,
                headers=request_headers,
                timeout=hop_timeout,
            ),
            stream=stream,
        )
        location = response.headers.get("location")
        if response.status_code not in REDIRECT_STATUSES:
            return response
        if not location:
            response.close()
            raise HttpPolicyError(
                f"HTTP {response.status_code} from {response.request.url} has no Location header"
            )
        next_url = response.request.url.join(location)
        if not next_url.is_absolute_url:
            response.close()
            raise HttpPolicyError(
                f"redirect from {response.request.url} has no usable target "
                f"(HTTP {response.status_code})"
            )
        if response.status_code == _SEE_OTHER and verb not in _IDEMPOTENT_METHODS:
            response.close()
            raise HttpPolicyError(
                f"refusing to replay {verb} after HTTP {_SEE_OTHER} to {next_url} "
                "(no non-idempotent replays)"
            )
        response.close()
        if same_origin_only and not same_origin(origin, next_url):
            raise HttpPolicyError("refusing cross-origin API redirect")
        target = next_url
    raise HttpPolicyError(
        f"redirect limit of {max_redirects} hops exceeded for {url!r} (last target {str(target)!r})"
    )


class BoundedJsonError(ValueError):
    """A response cannot safely be treated as a complete JSON inventory."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = (
            status_code
            if type(status_code) is int and _MIN_HTTP_STATUS <= status_code <= _MAX_HTTP_STATUS
            else None
        )


def get_bounded_json(
    endpoint: ApiEndpoint,
    path: str,
    params: Mapping[str, Any] | None = None,
    *,
    max_bytes: int = MAX_INVENTORY_JSON_BYTES,
    deadline: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> tuple[int, Any]:
    """GET identity JSON without buffering/decompressing an unbounded response.

    Non-success statuses return no payload without consuming their bodies. Errors
    contain no response data. The optional absolute deadline bounds body reads
    between chunks and narrows each configured timeout; a blocking read still
    depends on that HTTP timeout. Callers own any wider workflow deadline.
    """
    if max_bytes <= 0 or (deadline is not None and not math.isfinite(deadline)):
        raise ValueError("invalid bounded JSON limits")

    def remaining() -> float | None:
        if deadline is None:
            return None
        value = deadline - clock()
        if value <= 0:
            raise BoundedJsonError("JSON response deadline exceeded")
        return value

    budget = remaining()
    timeout = None
    if budget is not None:
        timeout = httpx.Timeout(
            **{
                name: budget if value is None else min(value, budget)
                for name, value in endpoint.client.timeout.as_dict().items()
            }
        )
    response = resolve_and_send(
        endpoint,
        "GET",
        path,
        params=params,
        stream=True,
        timeout=timeout,
        headers={"Accept-Encoding": "identity"},
        same_origin_only=True,
        deadline=deadline,
        clock=clock,
    )
    try:
        if not response.is_success:
            remaining()
            return response.status_code, None
        return response.status_code, read_bounded_json(
            response, max_bytes=max_bytes, deadline=deadline, clock=clock
        )
    finally:
        response.close()


def read_bounded_json(
    response: httpx.Response,
    *,
    max_bytes: int,
    deadline: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> Any:
    """Read identity JSON from an already streamed response; caller owns closure.

    Mutation callers may use this only for diagnostics that are actually needed.
    This never sends a request, retries, or uses a decompression iterator.
    """
    if max_bytes <= 0 or (deadline is not None and not math.isfinite(deadline)):
        raise ValueError("invalid bounded JSON limits")

    def check_deadline() -> None:
        if deadline is not None and clock() >= deadline:
            raise BoundedJsonError(
                "JSON response deadline exceeded", status_code=response.status_code
            )

    check_deadline()
    if response.headers.get("content-encoding", "identity").strip().lower() != "identity":
        raise BoundedJsonError(
            "unsupported JSON response encoding", status_code=response.status_code
        )
    body = bytearray()
    # Preloaded mock responses have already consumed their stream. Real
    # streamed responses use iter_raw, never httpx's decompression iterator.
    chunks = (response.content,) if response.is_stream_consumed else response.iter_raw()
    for chunk in chunks:
        check_deadline()
        if len(body) + len(chunk) > max_bytes:
            raise BoundedJsonError(
                "JSON response exceeds size limit", status_code=response.status_code
            )
        body.extend(chunk)
    check_deadline()
    try:
        payload = json_module.loads(body)
    except (ValueError, RecursionError):
        raise BoundedJsonError("invalid JSON response", status_code=response.status_code) from None
    check_deadline()
    return payload


def _resolve(client: httpx.Client, url: str) -> httpx.URL:
    """Resolve an API path against the client base **preserving any urlBase**.

    A leading slash is stripped (paths are relative to the base, not RFC-absolute):
    ``http://host/sonarr/`` + ``/api/v3/series`` must become
    ``http://host/sonarr/api/v3/series``, not ``http://host/api/v3/series``.
    """
    return client.base_url.join(url.lstrip("/"))


def _no_headers() -> dict[str, str]:
    return {}


def _factory_with_secret(func: Callable[[str], dict[str, str]], api_key: str) -> HeadersFactory:
    def factory() -> dict[str, str]:
        return func(api_key)

    return factory


def _timeout(settings: Settings) -> httpx.Timeout:
    return make_timeout(settings.sync.connect_timeout_seconds, settings.sync.read_timeout_seconds)
