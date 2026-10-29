"""Absolute cycle budgets cover the production authorization and queue read chains."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest
import test_service_setup as ts
import test_setup as setup_fixtures

from shoko_linker import service_setup
from shoko_linker.http import ApiEndpoint
from shoko_linker.linking import fetch_queue
from shoko_linker.service_setup import ServiceSetupController
from shoko_linker.setup import inspect_api_compatibility
from shoko_linker.shoko import collect_import_folders
from shoko_linker.state import open_state


class SlowBody(httpx.SyncByteStream):
    def __init__(self, payload: bytes, clock: list[float]) -> None:
        self.payload = payload
        self.clock = clock
        self.closed = False
        self.chunks = 0

    def __iter__(self) -> Iterator[bytes]:
        for offset in range(0, len(self.payload), 5):
            self.clock[0] += 0.5
            self.chunks += 1
            yield self.payload[offset : offset + 5]

    def close(self) -> None:
        self.closed = True


READS = (
    "/api/v3/system/status",
    "/api/v3/Init/Version",
    "/api/v3/series",
    "/api/v3/rootfolder",
    "/api/v3/Settings",
    "/api/v3/ImportFolder",
)


@pytest.mark.parametrize("late_path", [None, *READS])
@pytest.mark.parametrize("paused", [False, True])
def test_current_context_entire_read_chain_shares_deadline_without_invalidating_consent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, late_path: str | None, paused: bool
) -> None:
    controller, form, _, _, _, sonarr, shoko = ts._ready_setup_form(tmp_path, monkeypatch)
    assert "Setup saved." in controller.action("confirm", form)
    sonarr.client.close()
    shoko.client.close()
    clock = [100.0 if late_path is None else 99.0]
    requests: list[httpx.Request] = []
    bodies: list[SlowBody] = []
    payloads: dict[str, object] = {
        READS[0]: {"version": "4.0.0"},
        READS[1]: {"Server": {"Version": "5.3.3"}},
        READS[2]: [setup_fixtures.series_entry(41, ts.ANIME_ROOT)],
        READS[3]: [setup_fixtures.root_entry(ts.ANIME_ROOT)],
        READS[4]: {"FileQualityFilterEnabled": False},
        READS[5]: [{"ID": 1, "Name": "Anime", "Path": ts.SHOKO_ROOT, "DropFolderType": 0}],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        payload = payloads[request.url.path]
        if request.url.path == late_path:
            body = SlowBody(json.dumps(payload).encode(), clock)
            bodies.append(body)
            return httpx.Response(200, stream=body, request=request)
        return httpx.Response(200, json=payload, request=request)

    timeout = httpx.Timeout(connect=0.25, read=30.0, write=0.75, pool=2.0)
    with (
        httpx.Client(
            base_url="http://sonarr:8989", transport=httpx.MockTransport(handler), timeout=timeout
        ) as sonarr_client,
        httpx.Client(
            base_url="http://shoko:8111", transport=httpx.MockTransport(handler), timeout=timeout
        ) as shoko_client,
        open_state(controller.state_path) as store,
    ):
        controller.sonarr = ApiEndpoint(sonarr_client, lambda: {})
        controller.shoko = ApiEndpoint(shoko_client, lambda: {})
        monkeypatch.setattr(
            controller, "_environment", ServiceSetupController._environment.__get__(controller)
        )
        monkeypatch.setattr(service_setup, "collect_import_folders", collect_import_folders)
        if paused:
            store.pause_service_for_user("2026-10-29T00:00:00Z")
        pause = store.get_service_pause()
        consent = store.get_onboarding_json()
        revision = store.get_service_authorization_revision()
        assert controller.current_context(store, deadline=100.0, clock=lambda: clock[0]) is None
        assert store.get_onboarding_json() == consent
        assert store.get_service_authorization_revision() == revision
        assert not controller.onboarding(store).reconfirmation_required()
        assert store.get_service_pause() == pause
    if late_path is None:
        assert requests == []
    else:
        assert [request.url.path for request in requests] == list(
            READS[: READS.index(late_path) + 1]
        )
        assert all(
            request.extensions["timeout"]
            == {"connect": 0.25, "read": 1.0, "write": 0.75, "pool": 1.0}
            for request in requests
        )
        assert len(bodies) == 1
        assert bodies[0].chunks == 2
        assert bodies[0].closed
        assert clock[0] == 100.0


@pytest.mark.parametrize("expired", [False, True])
def test_aggregate_queue_expiry_is_unknown_and_does_not_start_later_io(expired: bool) -> None:
    clock = [100.0 if expired else 99.0]
    body = SlowBody(b'{"WaitingCount":0,"BlockedCount":0,"TotalCount":0}', clock)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, stream=body, request=request)

    with httpx.Client(
        base_url="http://shoko:8111",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(connect=0.25, read=30.0, write=0.75, pool=2.0),
    ) as client:
        endpoint = ApiEndpoint(client, lambda: {})
        assert fetch_queue(endpoint, deadline=100.0, clock=lambda: clock[0]) is None
        before = len(requests)
        assert fetch_queue(endpoint, deadline=100.0, clock=lambda: clock[0]) is None
        assert len(requests) == before
    assert len(requests) == (0 if expired else 1)
    if not expired:
        assert body.chunks == 2
        assert body.closed
        assert requests[0].extensions["timeout"] == {
            "connect": 0.25,
            "read": 1.0,
            "write": 0.75,
            "pool": 1.0,
        }


def test_authorization_version_redirects_share_the_whole_chain_deadline() -> None:
    clock = [99.0]
    requests: list[httpx.Request] = []
    body = SlowBody(b'{"version":"4.0.0"}', clock)

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/api/v3/system/status":
            clock[0] += 0.5
            return httpx.Response(302, headers={"Location": "/version-result"}, request=request)
        assert request.url.path == "/version-result"
        return httpx.Response(200, stream=body, request=request)

    with httpx.Client(
        base_url="http://sonarr:8989",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(connect=0.25, read=30.0, write=0.75, pool=2.0),
    ) as client:
        endpoint = ApiEndpoint(client, lambda: {})
        compatibility = inspect_api_compatibility(
            endpoint, endpoint, deadline=100.0, clock=lambda: clock[0]
        )
    assert not compatibility.compatible
    assert [request.url.path for request in requests] == [
        "/api/v3/system/status",
        "/version-result",
    ]
    assert requests[0].extensions["timeout"] == {
        "connect": 0.25,
        "read": 1.0,
        "write": 0.75,
        "pool": 1.0,
    }
    assert requests[1].extensions["timeout"] == {
        "connect": 0.25,
        "read": 0.5,
        "write": 0.5,
        "pool": 0.5,
    }
    assert body.closed
    assert body.chunks == 1
    assert clock[0] == 100.0
