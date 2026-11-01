"""A separate malformed Sonarr row cannot stall confirmed normal-service work."""

from dataclasses import replace
from pathlib import Path

import httpx
import test_daemon as td
import test_normal_source_policy as normal

from shoko_linker.setup import ScopeReadStatus, discover_sonarr_scope


def test_normal_cycle_uses_fresh_scoped_inventory_despite_separate_bad_title(
    tmp_path: Path,
) -> None:
    deps, shoko, _clock = normal.confirmed_deps(tmp_path)
    stub = td.SonarrStub(list(shoko.files.values()))

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v3/series":
            stub.requests.append(request)
            return httpx.Response(
                200,
                json=[
                    *stub._series(),
                    {
                        "id": 43,
                        "rootFolderPath": "/unrelated/tv",
                        "seriesType": "standard",
                        "tvdbId": 100,
                    },
                ],
                request=request,
            )
        if request.url.path == "/api/v3/rootfolder":
            return httpx.Response(
                200,
                json=[
                    {"path": td.SONARR_ROOT, "accessible": True},
                    {"path": normal.SECOND_ROOT, "accessible": True},
                    {"path": "/unrelated/tv", "accessible": True},
                ],
                request=request,
            )
        return stub.handler(request)

    original = deps.sonarr
    deps = replace(deps, sonarr=td.endpoint_for(handler, td.SONARR_BASE))
    consent = deps.store.get_onboarding_json()
    try:
        scope = discover_sonarr_scope(
            deps.sonarr, selected_roots=deps.settings.sonarr.allowed_roots
        )
        assert scope.read_status is ScopeReadStatus.COMPLETE
        report = normal.normal_cycle(deps)
        assert report.new_arrival_submitted == 1, report.reasons
        assert shoko.posts_count() == 1
        assert deps.store.get_onboarding_json() == consent
        assert all(request.url.params.get("seriesId") != "43" for request in stub.requests)
    finally:
        original.client.close()
        normal.close(deps)
