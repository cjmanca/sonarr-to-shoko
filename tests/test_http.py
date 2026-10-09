"""HTTP client policy: explicit timeouts, manual redirects, no cross-origin key leak.

All requests go through httpx.MockTransport: nothing in this module touches the network.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import httpx
import pytest

from shoko_linker.config import (
    DEFAULT_CONNECT_TIMEOUT_SECONDS,
    DEFAULT_READ_TIMEOUT_SECONDS,
    load_settings,
)
from shoko_linker.errors import HttpPolicyError
from shoko_linker.http import (
    MAX_REDIRECTS,
    ApiEndpoint,
    make_client,
    make_shoko_client,
    make_sonarr_client,
    make_timeout,
    resolve_and_send,
    same_origin,
    shoko_headers,
    sonarr_headers,
    strip_auth_headers,
)
from shoko_linker.logging_setup import registered_secret_count

EXAMPLE_CONFIG = Path(__file__).resolve().parents[1] / "config.example.yaml"
FAKE_KEY = "fake-key-never-a-real-one-0000"
HOPS_AFTER_ONE_REDIRECT = 2
REGISTERED_KEY_COUNT = 2
Handler = Callable[[httpx.Request], httpx.Response]


def endpoint_for(handler: Handler, base_url: str = "http://shoko:8111") -> ApiEndpoint:
    client = httpx.Client(
        base_url=base_url,
        transport=httpx.MockTransport(handler),
        follow_redirects=False,
        timeout=make_timeout(1.0, 1.0),
    )
    return ApiEndpoint(client, lambda: shoko_headers(FAKE_KEY))


def test_make_client_disables_redirects_and_sets_timeouts() -> None:
    endpoint = make_client("http://shoko:8111", None, make_timeout(10.0, 30.0))
    assert endpoint.client.follow_redirects is False
    assert endpoint.client.timeout.read == DEFAULT_READ_TIMEOUT_SECONDS
    assert endpoint.client.timeout.connect == DEFAULT_CONNECT_TIMEOUT_SECONDS
    # The base is stored with a trailing slash so path joins stay inside it.
    assert str(endpoint.client.base_url) == "http://shoko:8111/"
    endpoint.client.close()


def test_make_client_transport_disables_retries() -> None:
    endpoint = make_client("http://shoko:8111")
    transport = endpoint.client._transport
    assert isinstance(transport, httpx.HTTPTransport)
    # Policy pin: the real transport must never retry a request on its own.
    pool = getattr(transport, "_pool", None)
    assert getattr(pool, "_retries", None) == 0
    endpoint.client.close()


def test_urlbase_is_preserved_for_api_paths() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={}, request=request)

    client = httpx.Client(
        base_url="http://host/sonarr/",
        transport=httpx.MockTransport(handler),
        follow_redirects=False,
        timeout=make_timeout(1.0, 1.0),
    )
    endpoint = ApiEndpoint(client, lambda: sonarr_headers(FAKE_KEY))
    # Leading-slash API paths must not drop the urlBase segment.
    resolve_and_send(endpoint, "GET", "/api/v3/series")
    resolve_and_send(endpoint, "GET", "api/v3/episodefile")
    assert str(seen[0].url) == "http://host/sonarr/api/v3/series"
    assert str(seen[1].url) == "http://host/sonarr/api/v3/episodefile"
    endpoint.client.close()


def test_auth_header_names_match_the_documented_contracts() -> None:
    assert sonarr_headers(FAKE_KEY) == {"X-Api-Key": FAKE_KEY}
    assert shoko_headers(FAKE_KEY) == {"apikey": FAKE_KEY}


def test_strip_auth_headers_keeps_non_auth_headers() -> None:
    stripped = strip_auth_headers(
        {
            "X-Api-Key": FAKE_KEY,
            "apikey": FAKE_KEY,
            "Authorization": "Bearer x",
            "Accept": "application/json",
        }
    )
    assert stripped == {"Accept": "application/json"}


def test_same_origin_compares_scheme_host_and_port() -> None:
    base = httpx.URL("http://shoko:8111/api/v3/File")
    assert same_origin(base, httpx.URL("http://shoko:8111/other")) is True
    assert same_origin(base, httpx.URL("https://shoko:8111/api/v3/File")) is False
    assert same_origin(base, httpx.URL("http://other:8111/api/v3/File")) is False
    assert same_origin(base, httpx.URL("http://shoko:9999/api/v3/File")) is False


def test_same_origin_redirect_keeps_the_auth_header() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if "pageSize" not in request.url.params:
            return httpx.Response(
                307, headers={"location": "/api/v3/File?pageSize=100"}, request=request
            )
        return httpx.Response(200, json={"Total": 1}, request=request)

    response = resolve_and_send(endpoint_for(handler), "GET", "/api/v3/File")
    assert response.status_code == httpx.codes.OK
    assert len(seen) == HOPS_AFTER_ONE_REDIRECT
    assert seen[1].headers["apikey"] == FAKE_KEY
    assert seen[1].url.params["pageSize"] == "100"


def test_redirect_target_query_is_not_duplicated() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.params.get("page") == "1":
            return httpx.Response(307, headers={"location": "/api/v3/File?page=2"}, request=request)
        return httpx.Response(200, json={}, request=request)

    resolve_and_send(endpoint_for(handler), "GET", "/api/v3/File", params={"page": "1"})
    assert seen[0].url.params["page"] == "1"
    assert seen[1].url.params["page"] == "2"


def test_cross_origin_redirect_strips_the_auth_header() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.host == "shoko":
            return httpx.Response(
                302, headers={"location": "http://cdn.example/file"}, request=request
            )
        return httpx.Response(200, json={}, request=request)

    endpoint = endpoint_for(handler)
    response = resolve_and_send(endpoint, "GET", "/File")
    assert response.status_code == httpx.codes.OK
    assert seen[0].headers["apikey"] == FAKE_KEY
    assert "apikey" not in seen[1].headers
    assert seen[1].url.host == "cdn.example"


def test_redirect_cap_is_enforced() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(307, headers={"location": "/api/v3/File"}, request=request)

    with pytest.raises(HttpPolicyError, match="redirect limit"):
        resolve_and_send(endpoint_for(handler), "GET", "/api/v3/File", max_redirects=MAX_REDIRECTS)


def test_redirect_without_location_is_reported() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(307, json={}, request=request)

    with pytest.raises(HttpPolicyError, match="no Location header"):
        resolve_and_send(endpoint_for(handler), "GET", "/api/v3/File")


def test_post_is_never_replayed_after_303() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(303, headers={"location": "/api/v3/File/1"}, request=request)

    with pytest.raises(HttpPolicyError, match="refusing to replay POST"):
        resolve_and_send(
            endpoint_for(handler), "POST", "/api/v3/File/1/Link", json={"EpisodeIDs": [1]}
        )
    assert len(seen) == 1


@pytest.mark.parametrize("status", [301, 302, 307, 308])
def test_post_redirect_walk_delivers_exactly_once_per_target(status: int) -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/api/v3/File/1/Link":
            return httpx.Response(
                status, headers={"location": "/api/v3/File/2/Link"}, request=request
            )
        return httpx.Response(200, json={}, request=request)

    response = resolve_and_send(
        endpoint_for(handler), "POST", "/api/v3/File/1/Link", json={"EpisodeIDs": [1]}
    )
    assert response.status_code == httpx.codes.OK
    # Each original POST body is delivered once, re-pointed at the redirect target:
    # redirect contract, not retry. Two targets, two requests, no repeats.
    assert [str(r.url) for r in seen] == [
        "http://shoko:8111/api/v3/File/1/Link",
        "http://shoko:8111/api/v3/File/2/Link",
    ]
    assert all(r.method == "POST" for r in seen)


def test_foreign_origin_absolute_url_is_refused_before_any_request() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={}, request=request)

    with pytest.raises(HttpPolicyError, match="does not match the client base"):
        resolve_and_send(endpoint_for(handler), "GET", "https://evil.example/api/v3/series")
    assert not seen


def test_short_key_below_redaction_floor_warns_without_naming_value(
    caplog: pytest.LogCaptureFixture,
) -> None:
    short_key = "abc"
    endpoint = make_client("http://shoko:8111", None, None, secret_values=(short_key,))
    assert registered_secret_count() == 0
    assert "redaction floor" in caplog.text
    assert short_key not in caplog.text
    endpoint.client.close()


def test_non_idempotent_method_gets_exactly_one_attempt() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(500, json={}, request=request)

    response = resolve_and_send(
        endpoint_for(handler), "POST", "/api/v3/File/1/Link", json={"EpisodeIDs": [1]}
    )
    assert response.status_code == httpx.codes.INTERNAL_SERVER_ERROR
    assert len(seen) == 1


def test_setting_built_clients_use_the_configured_urls_and_headers() -> None:
    settings = load_settings(EXAMPLE_CONFIG)
    sonarr = make_sonarr_client(settings)
    shoko = make_shoko_client(settings)
    assert sonarr.client.follow_redirects is False
    assert str(sonarr.base_url).rstrip("/") == settings.sonarr.url
    assert str(shoko.base_url).rstrip("/") == settings.shoko.url
    assert list(sonarr.headers_factory()) == ["X-Api-Key"]
    assert list(shoko.headers_factory()) == ["apikey"]
    assert sonarr.client.timeout.connect == settings.sync.connect_timeout_seconds
    assert shoko.client.timeout.read == settings.sync.read_timeout_seconds
    # Both keys were registered for redaction at client creation.
    assert registered_secret_count() == REGISTERED_KEY_COUNT
    sonarr.client.close()
    shoko.client.close()
