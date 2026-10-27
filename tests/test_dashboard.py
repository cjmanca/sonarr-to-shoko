"""M5.1 first-user enrollment, login/session security, and authenticated controls."""

from __future__ import annotations

import hashlib
import os
import re
import stat
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from shoko_linker import cli
from shoko_linker.bootstrap import BootstrapSettings
from shoko_linker.config import SecretRef
from shoko_linker.dashboard import (
    DASHBOARD_SESSION_SECONDS,
    MAX_DASHBOARD_REQUEST_BYTES,
    DashboardApplication,
    DashboardSetupView,
    DashboardStatus,
    _b64decode,
    create_dashboard_server,
    hash_password,
    verify_password,
)
from shoko_linker.http import ApiEndpoint
from shoko_linker.onboarding import (
    OnboardingContext,
    OnboardingManager,
    OnboardingStatus,
    ScopeBinding,
)
from shoko_linker.paths import PathMapping
from shoko_linker.safety_check import (
    SafetyAssessment,
    assess_settings_payload,
    evaluate_service_safety,
    inspect_shoko_safety_settings,
)
from shoko_linker.service_settings import ServiceSettingsController
from shoko_linker.service_setup import ServiceSetupController
from shoko_linker.setup import (
    AnimeRootCandidate,
    AnimeScopeDiscovery,
    ApiCompatibility,
    MappingDiscoveryStatus,
    PathMappingDiscovery,
    ScopeReadStatus,
    ScopeStatus,
)
from shoko_linker.shoko import ImportFolder, ShokoDiscovery, ShokoVerdict
from shoko_linker.sonarr import InventoryVerdict, SonarrInventory
from shoko_linker.state import open_state

T0 = datetime(2026, 10, 15, tzinfo=UTC)
USERNAME = "library-admin"
PASSWORD = "a long and unique dashboard password"
API_SECRET_SENTINEL = "dashboard-must-not-show-api-secret"


@dataclass
class MutableClock:
    value: datetime = T0

    def __call__(self) -> datetime:
        return self.value


def safe_assessment() -> SafetyAssessment:
    return assess_settings_payload(
        {
            "FileQualityFilterEnabled": False,
            "Plugins": {
                "Renamer": {
                    "RenameOnImport": False,
                    "MoveOnImport": False,
                    "RelocateOnImport": False,
                }
            },
            "AniDb": {"MyList_AddFiles": False, "Password": API_SECRET_SENTINEL},
            "Database": {"Password": API_SECRET_SENTINEL},
        }
    )


def scope_binding(assessment: SafetyAssessment) -> ScopeBinding:
    return ScopeBinding(
        sonarr_url="http://sonarr:8989",
        shoko_url="http://shoko:8111",
        sonarr_version="6.0.0.0",
        shoko_version="5.3.3",
        capabilities=("sonarr_api_v3_system_status", "shoko_api_v3_init_version"),
        roots=("/data/media/anime",),
        path_mappings=(PathMapping("/data/media/anime", "/anime"),),
        safety_digest=assessment.digest,
        unverifiable_facts=assessment.unverifiable_facts,
    )


def make_app(
    tmp_path: Path,
    *,
    clock: MutableClock | None = None,
    onboarding: bool = False,
) -> DashboardApplication:
    assessment = safe_assessment()
    binding = scope_binding(assessment)
    path = tmp_path / "state.sqlite3"
    if onboarding:
        with open_state(path) as store:
            record = OnboardingManager(store).confirm_setup(
                binding,
                assessment,
                confirmed_facts=(assessment.unverifiable_facts + assessment.unknown_settings),
                automatic_link_consent=True,
                iso=T0.isoformat(),
            )
    return DashboardApplication(
        path,
        clock=clock or MutableClock(),
        onboarding_context=lambda: OnboardingContext(
            binding, assessment, "" if not onboarding else record.to_json()
        ),
        status_provider=lambda: DashboardStatus(
            connection_message="Sonarr and Shoko are connected.",
            linked_files=12,
            waiting_files=4,
            review_files=2,
            automatic_linking=True,
            next_action="No action is needed.",
            diagnostics=("internal trace sentinel",),
            mapping_cache_status="Episode mappings are available.",
            excluded_files=3,
            scope_excluded_files=1,
            exclusion_counts=(("outside_allowed_root", 1), ("size_mismatch", 2)),
        ),
    )


def start_server(app: DashboardApplication) -> tuple[Any, threading.Thread, str]:
    server = create_dashboard_server("127.0.0.1", 0, app)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, f"http://127.0.0.1:{server.server_port}"


def stop_server(server: Any, thread: threading.Thread) -> None:
    server.shutdown()
    thread.join(timeout=5)
    server.server_close()


def same_origin(base_url: str) -> dict[str, str]:
    return {"Origin": base_url}


def setup_account(client: httpx.Client, base_url: str) -> httpx.Response:
    return client.post(
        f"{base_url}/setup",
        data={"username": USERNAME, "password": PASSWORD},
        headers=same_origin(base_url),
    )


def csrf_from(html: str) -> str:
    match = re.search(r'name="csrf_token" value="([^"]+)"', html)
    assert match is not None
    return match.group(1)


def setup_digest_from(html: str) -> str:
    match = re.search(r'name="setup_digest" value="([0-9a-f]{64})"', html)
    assert match is not None
    return match.group(1)


def test_first_user_setup_is_atomic_and_persists_only_password_hash_and_signing_key(
    tmp_path: Path,
) -> None:
    app = make_app(tmp_path)
    server, thread, base_url = start_server(app)
    try:
        with httpx.Client(follow_redirects=False) as client:
            first_visit = client.get(base_url)
            assert first_visit.status_code == 200
            assert "Create your dashboard account" in first_visit.text
            # Native browser form POSTs need an origin-preserving policy.
            assert first_visit.headers["referrer-policy"] == "same-origin"
            created = setup_account(client, base_url)
            assert created.status_code == 303
            assert created.headers["location"] == "/status"
            assert "HttpOnly" in created.headers["set-cookie"]
            assert "SameSite=Strict" in created.headers["set-cookie"]
            assert "session" not in created.text.lower()
            assert setup_account(client, base_url).status_code == 410
        with open_state(tmp_path / "state.sqlite3") as store:
            account = store.get_dashboard_account()
            assert store.get_onboarding_json() is None
        assert account is not None
        assert account.username == USERNAME
        assert PASSWORD not in account.password_hash
        assert PASSWORD not in repr(account)
        assert len(account.signing_key) == 32
    finally:
        stop_server(server, thread)


def test_password_hash_uses_a_random_salt_and_verifies_without_plaintext_storage() -> None:
    first = hash_password(PASSWORD)
    second = hash_password(PASSWORD)
    assert first != second
    assert PASSWORD not in first
    assert verify_password(PASSWORD, first) is True
    assert verify_password("a different password", first) is False


def test_login_is_generic_and_status_requires_authentication(tmp_path: Path) -> None:
    app = make_app(tmp_path)
    server, thread, base_url = start_server(app)
    try:
        with httpx.Client(follow_redirects=False) as owner:
            assert setup_account(owner, base_url).status_code == 303
            page = owner.get(f"{base_url}/status")
            assert page.status_code == 200
            assert "12" in page.text
            assert "4" in page.text
            assert "2" in page.text
            assert "No action is needed." in page.text
            assert "Episode mappings are available." in page.text
            assert "Latest cycle exclusions" in page.text
            assert "Excluded outside scope" in page.text
            assert "size_mismatch: 2" in page.text
            assert "internal trace sentinel" in page.text
            assert API_SECRET_SENTINEL not in page.text
        with httpx.Client(follow_redirects=False) as visitor:
            unauthenticated = visitor.get(f"{base_url}/status")
            assert unauthenticated.status_code == 303
            assert unauthenticated.headers["location"] == "/"
            assert API_SECRET_SENTINEL not in unauthenticated.text
            wrong = visitor.post(
                f"{base_url}/login",
                data={"username": "someone-else", "password": "incorrect password"},
                headers=same_origin(base_url),
            )
        with httpx.Client(follow_redirects=False) as existing_user:
            password_only = existing_user.post(
                f"{base_url}/login",
                data={"username": USERNAME, "password": "incorrect password"},
                headers=same_origin(base_url),
            )
        with httpx.Client(follow_redirects=False) as returning_user:
            successful_login = returning_user.post(
                f"{base_url}/login",
                data={"username": USERNAME, "password": PASSWORD},
                headers=same_origin(base_url),
            )
            assert successful_login.status_code == 303
            assert returning_user.get(f"{base_url}/status").status_code == 200
        assert wrong.status_code == password_only.status_code == 401
        assert wrong.text == password_only.text
        assert API_SECRET_SENTINEL not in wrong.text
    finally:
        stop_server(server, thread)


def test_sessions_expire_and_tampered_cookie_is_rejected(tmp_path: Path) -> None:
    clock = MutableClock()
    app = make_app(tmp_path, clock=clock)
    server, thread, base_url = start_server(app)
    try:
        with httpx.Client(follow_redirects=False) as client:
            assert setup_account(client, base_url).status_code == 303
            cookie = client.cookies.get("shoko_linker_session")
            assert cookie
            assert client.get(f"{base_url}/status").status_code == 200
            tampered = cookie[:-1] + ("A" if cookie[-1] != "A" else "B")
            assert (
                client.get(
                    f"{base_url}/status",
                    headers={"Cookie": f"shoko_linker_session={tampered}"},
                ).status_code
                == 303
            )
            clock.value += timedelta(seconds=DASHBOARD_SESSION_SECONDS + 1)
            assert client.get(f"{base_url}/status").status_code == 303
    finally:
        stop_server(server, thread)


def test_dashboard_cookie_base64_rejects_noncanonical_padding_bits() -> None:
    with pytest.raises(ValueError, match="non-canonical"):
        _b64decode("AB")


def test_pause_resume_require_one_time_csrf_and_resume_rechecks_onboarding(
    tmp_path: Path,
) -> None:
    app = make_app(tmp_path, onboarding=True)
    server, thread, base_url = start_server(app)
    try:
        with httpx.Client(follow_redirects=False) as client:
            assert setup_account(client, base_url).status_code == 303
            old_cookie = client.cookies.get("shoko_linker_session")
            assert old_cookie
            page = client.get(f"{base_url}/status")
            token = csrf_from(page.text)
            paused = client.post(
                f"{base_url}/pause",
                data={"csrf_token": token},
                headers=same_origin(base_url),
            )
            assert paused.status_code == 303
            with open_state(tmp_path / "state.sqlite3") as store:
                assert store.get_service_pause() == (True, "user_paused")

            replay = client.post(
                f"{base_url}/pause",
                data={"csrf_token": token},
                headers={**same_origin(base_url), "Cookie": f"shoko_linker_session={old_cookie}"},
            )
            assert replay.status_code == 403

            resumed_page = client.get(f"{base_url}/status")
            resumed = client.post(
                f"{base_url}/resume",
                data={"csrf_token": csrf_from(resumed_page.text)},
                headers=same_origin(base_url),
            )
            assert resumed.status_code == 303
            with open_state(tmp_path / "state.sqlite3") as store:
                assert store.get_service_pause() == (False, "")
                assert OnboardingManager(store).record() is not None
    finally:
        stop_server(server, thread)


@pytest.mark.parametrize(
    "condition",
    ["unsafe_shoko_settings", "setup_changed", "connection_url", "user_paused", "running"],
)
def test_authenticated_status_and_poll_preserve_runtime_next_action(
    tmp_path: Path, condition: str
) -> None:
    app = make_app(tmp_path, onboarding=True)
    binding = scope_binding(safe_assessment())
    bootstrap = BootstrapSettings(
        binding.sonarr_url,
        SecretRef("SONARR_API_KEY"),
        binding.shoko_url,
        SecretRef("SHOKO_API_KEY"),
        tmp_path / "state.sqlite3",
    )
    with httpx.Client(
        transport=httpx.MockTransport(lambda _: pytest.fail("Unexpected remote I/O"))
    ) as remote:
        endpoint = ApiEndpoint(remote, lambda: {})
        runtime = cli._NormalServiceRuntime(bootstrap, endpoint, endpoint, cli.StopState())
        runtime._update_latest(
            authorized=True, next_action="Automatic linking is processing eligible files."
        )
        if condition == "connection_url":
            preferences = ServiceSettingsController(bootstrap, lambda: None)
            view = preferences.view()
            form = {
                **{name: value for name, _label, value in view.values},
                "advanced_yaml": view.advanced_yaml,
                "settings_digest": view.digest,
            }
            form["sonarr_url"] = "http://changed-sonarr:8989"
            assert preferences.save(form).startswith("Settings saved.")
            runtime.worker_tick()
            expected = "Restart the container to use saved connection URLs, then confirm setup."
        elif condition == "running":
            expected = "Automatic linking is processing eligible files."
        else:
            with open_state(bootstrap.database_path) as store:
                manager = OnboardingManager(store)
                if condition == "user_paused":
                    manager.pause(iso=T0.isoformat())
                    expected = "Automatic linking is paused. Resume when you are ready."
                else:
                    expected = (
                        "Fix Shoko safety settings and confirm setup again."
                        if condition == "unsafe_shoko_settings"
                        else "Confirm the changed scope and safety facts."
                    )
                    manager.pause_for_review(condition, expected, iso=T0.isoformat())
                    runtime._update_latest(authorized=False, next_action=expected)
        assert runtime.status().next_action == expected
        app.status_provider = runtime.status
        server, thread, base_url = start_server(app)
        try:
            with httpx.Client(follow_redirects=False) as client:
                assert client.get(f"{base_url}/status").status_code == 303
                assert setup_account(client, base_url).status_code == 303
                for path in ("/status", "/settings"):
                    # Polling fetches these same HTML routes without a page reload.
                    for headers in ({}, {"Cache-Control": "no-cache"}):
                        page = client.get(f"{base_url}{path}", headers=headers)
                        assert page.status_code == 200
                        assert f'<p class="next-action">{expected}</p>' in page.text
                        badge = "Running" if condition == "running" else "Paused"
                        assert f"{badge}</span>" in page.text
        finally:
            stop_server(server, thread)


def test_resume_refusal_overrides_cached_runtime_guidance(tmp_path: Path) -> None:
    app = make_app(tmp_path)
    app.onboarding_context = None
    server, thread, base_url = start_server(app)
    try:
        with httpx.Client(follow_redirects=False) as client:
            assert setup_account(client, base_url).status_code == 303
            page = client.get(f"{base_url}/status")
            response = client.post(
                f"{base_url}/resume",
                data={"csrf_token": csrf_from(page.text)},
                headers=same_origin(base_url),
            )
            assert response.status_code == 200
            assert (
                '<p class="next-action">Complete setup before automatic linking can resume.</p>'
                in response.text
            )
            assert "Paused</span>" in response.text
    finally:
        stop_server(server, thread)


def test_setup_acknowledgement_expires_on_poll_and_worker_activity_is_live(tmp_path: Path) -> None:
    app = make_app(tmp_path)
    current = app.status_provider()
    current = replace(current, automatic_linking=False, worker_activity="Checking safety.")
    app.status_provider = lambda: current
    app.setup_action = lambda _action, _form: "Setup saved."
    server, thread, base_url = start_server(app)
    try:
        with httpx.Client(follow_redirects=False) as client:
            assert setup_account(client, base_url).status_code == 303
            page = client.get(f"{base_url}/status")
            confirmed = client.post(
                f"{base_url}/setup/confirm",
                data={"csrf_token": csrf_from(page.text)},
                headers=same_origin(base_url),
            )
            assert confirmed.status_code == 200
            assert (
                '<div data-live="acknowledgement" role="status"><p>Setup saved.</p>'
                in confirmed.text
            )
            assert '<p class="next-action">No action is needed.</p>' in confirmed.text
            assert '<p class="worker-activity" role="status">Checking safety.</p>' in confirmed.text
            # A completed worker stage and a genuine block both appear on normal polling.
            for activity, automatic, action in (
                ("Processing eligible files.", True, "Automatic linking is running."),
                ("", False, "Fix Shoko safety settings and confirm setup again."),
                ("<script>unsafe</script>", False, "Waiting for connections."),
            ):
                current = replace(
                    current,
                    worker_activity=activity,
                    automatic_linking=automatic,
                    next_action=action,
                )
                page = client.get(f"{base_url}/status")
                assert 'data-live="acknowledgement" role="status"></div>' in page.text
                assert "Setup saved." not in page.text
                assert f'<p class="next-action">{action}</p>' in page.text
                assert "<script>unsafe</script>" not in page.text
                assert ('class="worker-activity"' in page.text) == bool(activity)
            assert "&lt;script&gt;unsafe&lt;/script&gt;" in page.text
            # The successful POST consumed its CSRF token even though only the notice changed.
            replay = client.post(
                f"{base_url}/setup/confirm",
                data={"csrf_token": csrf_from(confirmed.text)},
                headers=same_origin(base_url),
            )
            assert replay.status_code == 200
            assert (
                client.post(
                    f"{base_url}/setup/confirm",
                    data={"csrf_token": csrf_from(confirmed.text)},
                    headers=same_origin(base_url),
                ).status_code
                == 403
            )
    finally:
        stop_server(server, thread)


def test_setup_scope_and_safety_controls_are_authenticated_and_csrf_protected(
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, dict[str, str]]] = []
    selected_roots: list[str] = []
    discovering = True

    def setup_view() -> DashboardSetupView:
        if discovering:
            return ServiceSetupController._loading_view()
        return DashboardSetupView(
            message="Choose the Sonarr folder and review safety before automatic linking.",
            root_options=tuple(f"/anime/{index}" for index in range(12)),
            shoko_root_options=("/shoko/anime",),
            selected_roots=tuple(selected_roots),
            inferred_mappings=(("/anime/9", "/shoko/anime"),) if selected_roots else (),
            required_confirmations=("Confirm plugin side effects",),
            snapshot_digest="a" * 64,
        )

    def save_setup(action: str, form: Mapping[str, str]) -> str:
        calls.append((action, dict(form)))
        if action == "scope":
            selected_roots.extend(value for key, value in form.items() if key.startswith("root_"))
        return "Setup saved."

    app = DashboardApplication(
        tmp_path / "state.sqlite3",
        clock=MutableClock(),
        setup_provider=setup_view,
        setup_action=save_setup,
    )
    server, thread, base_url = start_server(app)
    try:
        with httpx.Client(follow_redirects=False) as client:
            assert setup_account(client, base_url).status_code == 303
            page = client.get(f"{base_url}/status")
            assert '<meta http-equiv="refresh"' not in page.text
            assert "fetch(statusUrl" in page.text
            assert "script-src 'sha256-" in page.headers["content-security-policy"]
            assert "connect-src 'self'" in page.headers["content-security-policy"]
            assert 'role="progressbar"' in page.text
            assert 'aria-valuenow="0"' in page.text
            assert "Setup checks start automatically" in page.text
            assert 'action="/setup/scope"' not in page.text
            assert 'action="/setup/confirm"' not in page.text
            assert page.text.index("Set up automatic linking") < page.text.index("Verified files")
            discovering = False
            page = client.get(f"{base_url}/status")
            assert "Choose the Sonarr folder" in page.text
            assert "/anime/9" in page.text
            assert '<meta http-equiv="refresh"' not in page.text
            assert 'role="progressbar"' not in page.text
            assert "fetch(statusUrl" in page.text
            assert 'action="/setup/scope"' in page.text
            first_token = csrf_from(page.text)
            selected = client.post(
                f"{base_url}/setup/scope",
                data={
                    "csrf_token": first_token,
                    "setup_digest": setup_digest_from(page.text),
                    **{f"root_{index}": f"/anime/{index}" for index in range(12)},
                },
                headers=same_origin(base_url),
            )
            assert selected.status_code == 200
            confirm_page = client.get(f"{base_url}/status")
            assert "Confirm plugin side effects" in confirm_page.text
            assert hashlib.sha256(b"Confirm plugin side effects").hexdigest() in confirm_page.text
            confirmed = client.post(
                f"{base_url}/setup/confirm",
                data={
                    "csrf_token": csrf_from(confirm_page.text),
                    "setup_digest": setup_digest_from(confirm_page.text),
                    "confirm_0": "1",
                    "automatic_link_consent": "1",
                },
                headers=same_origin(base_url),
            )
            assert confirmed.status_code == 200
        assert calls[0][0] == "scope"
        assert len(calls[0][1]) == 14
        assert calls[0][1]["root_11"] == "/anime/11"
        assert calls[1][0] == "confirm"
        assert calls[1][1]["automatic_link_consent"] == "1"
    finally:
        stop_server(server, thread)


def test_setup_provider_cache_miss_does_not_block_status_pause_or_resume(  # noqa: PLR0915
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Full setup correspondence discovery runs off the dashboard's single request thread."""
    assessment = safe_assessment()
    binding = scope_binding(assessment)
    compatibility = ApiCompatibility(
        binding.sonarr_version,
        binding.shoko_version,
        True,
        True,
        binding.capabilities,
        (),
    )
    scope = AnimeScopeDiscovery(
        ScopeStatus.CONFIRM_SCOPE,
        (AnimeRootCandidate(binding.roots[0], (41,)),),
        "Confirm the Sonarr anime folder.",
    )
    inventory = SonarrInventory((), (), (), (), (), (), (), InventoryVerdict(True, True, (), ()))
    discovery = ShokoDiscovery(
        (),
        {1: ImportFolder(1, "Anime", "/anime", "None", True)},
        ShokoVerdict(True, (), ()),
        1,
        0,
        1000,
    )
    path_result = PathMappingDiscovery(
        MappingDiscoveryStatus.READY,
        binding.path_mappings,
        (),
        "Exact file paths and sizes establish this folder match.",
    )
    bootstrap = BootstrapSettings(
        binding.sonarr_url,
        SecretRef("SONARR_API_KEY"),
        binding.shoko_url,
        SecretRef("SHOKO_API_KEY"),
        tmp_path / "state.sqlite3",
    )
    sonarr = ApiEndpoint(httpx.Client(), lambda: {})
    shoko = ApiEndpoint(httpx.Client(), lambda: {})
    controller = ServiceSetupController(bootstrap, sonarr, shoko, tmp_path / "state.sqlite3")
    refresh_started = threading.Event()
    release_refresh = threading.Event()
    refresh_finished = threading.Event()
    child_gets = 0

    monkeypatch.setattr(controller, "_environment", lambda: (compatibility, scope, assessment))

    def slow_inventory(
        roots: tuple[str, ...],
    ) -> tuple[SonarrInventory, ShokoDiscovery, PathMappingDiscovery]:
        nonlocal child_gets
        refresh_started.set()
        try:
            assert release_refresh.wait(timeout=3)
            child_gets += 320
            return inventory, discovery, path_result
        finally:
            refresh_finished.set()

    monkeypatch.setattr(controller, "_inventory", slow_inventory)
    app = make_app(tmp_path, onboarding=True)
    scoped_assessment = evaluate_service_safety(
        assessment, binding.path_mappings, discovery.folders
    )
    current_binding = replace(
        binding,
        safety_digest=scoped_assessment.digest,
        unverifiable_facts=scoped_assessment.unverifiable_facts,
        settings_observation=tuple(scoped_assessment.observed_settings.items()),
        selected_folders_observation=((1, "/anime", "None"),),
    )
    with open_state(bootstrap.database_path) as store:
        record = OnboardingManager(store).confirm_setup(
            current_binding,
            scoped_assessment,
            confirmed_facts=scoped_assessment.unverifiable_facts
            + scoped_assessment.unknown_settings,
            automatic_link_consent=True,
            iso=T0.isoformat(),
        )
    app.onboarding_context = lambda: OnboardingContext(
        current_binding, scoped_assessment, record.to_json()
    )
    monkeypatch.setattr(
        "shoko_linker.service_setup.collect_import_folders",
        lambda *_: tuple(discovery.folders.values()),
    )
    app.setup_provider = controller.view
    app.setup_action = controller.action
    server, server_thread, base_url = start_server(app)
    client = httpx.Client(follow_redirects=False)
    status_done = threading.Event()
    status_result: dict[str, Any] = {}

    def request_status() -> None:
        status_result["response"] = client.get(f"{base_url}/status")
        status_done.set()

    try:
        assert setup_account(client, base_url).status_code == 303
        status_thread = threading.Thread(target=request_status)
        status_thread.start()
        status_responsive = status_done.wait(timeout=0.5)
        background_started = refresh_started.wait(timeout=1)
        pause_responsive = False
        resume_responsive = False
        if status_responsive:
            page = status_result["response"]
            assert page.status_code == 200
            assert "Checking current Sonarr and Shoko setup" in page.text
            assert 'role="progressbar"' in page.text
            assert "fetch(statusUrl" in page.text
            pause = client.post(
                f"{base_url}/pause",
                data={"csrf_token": csrf_from(page.text)},
                headers=same_origin(base_url),
            )
            pause_responsive = pause.status_code == 303
            paused_page = client.get(f"{base_url}/status")
            resumed = client.post(
                f"{base_url}/resume",
                data={"csrf_token": csrf_from(paused_page.text)},
                headers=same_origin(base_url),
            )
            resume_responsive = resumed.status_code == 303
        assert child_gets == 0
        release_refresh.set()
        status_thread.join(timeout=2)
        assert status_responsive
        assert background_started
        assert pause_responsive
        assert resume_responsive
        assert refresh_finished.wait(timeout=2)
        assert child_gets == 320
    finally:
        release_refresh.set()
        client.close()
        stop_server(server, server_thread)
        sonarr.client.close()
        shoko.client.close()


@pytest.mark.parametrize("drift", ["mapping_options", "scope", "safety"])
def test_authenticated_stale_setup_form_cannot_authorize_reinterpreted_choices(  # noqa: PLR0915
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, drift: str
) -> None:
    assessment = safe_assessment()
    assessment_state = {"value": assessment}
    binding = scope_binding(assessment)
    compatibility = ApiCompatibility(
        binding.sonarr_version,
        binding.shoko_version,
        True,
        True,
        binding.capabilities,
        (),
    )
    scope_state = {
        "value": AnimeScopeDiscovery(
            ScopeStatus.CONFIRM_SCOPE,
            (AnimeRootCandidate(binding.roots[0], (41,)),),
            "Confirm the Sonarr anime folder.",
        )
    }
    folders_state = {"paths": ["/anime"]}
    inventory = SonarrInventory((), (), (), (), (), (), (), InventoryVerdict(True, True, (), ()))
    path_result = PathMappingDiscovery(
        MappingDiscoveryStatus.NEEDS_USER_MAPPING,
        (),
        (),
        "Choose a safe folder mapping.",
    )
    bootstrap = BootstrapSettings(
        binding.sonarr_url,
        SecretRef("SONARR_API_KEY"),
        binding.shoko_url,
        SecretRef("SHOKO_API_KEY"),
        tmp_path / "state.sqlite3",
    )
    sonarr = ApiEndpoint(httpx.Client(), lambda: {"X-Test-Secret": API_SECRET_SENTINEL})
    shoko = ApiEndpoint(httpx.Client(), lambda: {"X-Test-Secret": API_SECRET_SENTINEL})
    controller = ServiceSetupController(bootstrap, sonarr, shoko, bootstrap.database_path)

    def current_environment() -> tuple[ApiCompatibility, AnimeScopeDiscovery, SafetyAssessment]:
        return compatibility, scope_state["value"], assessment_state["value"]

    def current_inventory(
        _roots: tuple[str, ...],
    ) -> tuple[SonarrInventory, ShokoDiscovery, PathMappingDiscovery]:
        folders = tuple(
            ImportFolder(index, path.rsplit("/", 1)[-1], path, "None", True)
            for index, path in enumerate(folders_state["paths"], start=1)
        )
        discovery = ShokoDiscovery(
            (),
            {folder.id: folder for folder in folders},
            ShokoVerdict(True, (), ()),
            len(folders),
            0,
            1000,
        )
        return inventory, discovery, path_result

    monkeypatch.setattr(controller, "_environment", current_environment)
    monkeypatch.setattr(controller, "_inventory", current_inventory)
    monkeypatch.setattr(
        "shoko_linker.service_setup.collect_import_folders",
        lambda *_: current_inventory(binding.roots)[1].folders.values(),
    )

    refresh_finished = threading.Event()
    original_refresh = controller.refresh_view

    def tracked_refresh() -> DashboardSetupView | None:
        result = original_refresh()
        refresh_finished.set()
        return result

    monkeypatch.setattr(controller, "refresh_view", tracked_refresh)
    app = DashboardApplication(
        bootstrap.database_path,
        setup_provider=controller.view,
        setup_action=controller.action,
    )
    server, server_thread, base_url = start_server(app)
    client = httpx.Client(follow_redirects=False)

    def wait_for_page(predicate: Any) -> httpx.Response:
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            page = client.get(f"{base_url}/status")
            if predicate(page.text):
                return page
            time.sleep(0.01)
        pytest.fail("setup discovery did not publish the expected view")

    def wait_for_background_view(predicate: Any) -> None:
        controller._invalidate_view()
        refresh_finished.clear()
        loading = controller.view()
        assert loading is not None
        assert not loading.confirmation_enabled
        assert refresh_finished.wait(timeout=3)
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline:
            view = controller.view()
            if predicate(view):
                return
            time.sleep(0.01)
        pytest.fail("background setup refresh did not publish the updated choices")

    try:
        assert setup_account(client, base_url).status_code == 303
        scope_page = wait_for_page(lambda text: 'name="root_0"' in text)
        assert API_SECRET_SENTINEL not in scope_page.text
        scope_result = client.post(
            f"{base_url}/setup/scope",
            data={
                "csrf_token": csrf_from(scope_page.text),
                "setup_digest": setup_digest_from(scope_page.text),
                "root_0": binding.roots[0],
            },
            headers=same_origin(base_url),
        )
        assert scope_result.status_code == 200
        confirm_page = wait_for_page(lambda text: 'name="mapping_0"' in text)
        old_view = controller.view()
        assert old_view is not None
        assert old_view.confirmation_enabled
        assert old_view.mapping_options == ((binding.roots[0], "/anime"),)
        old_form = {
            "csrf_token": csrf_from(confirm_page.text),
            "setup_digest": setup_digest_from(confirm_page.text),
            "mapping_0": "0",
            "automatic_link_consent": "1",
            **{
                f"confirm_{index}": hashlib.sha256(fact.encode("utf-8")).hexdigest()
                for index, fact in enumerate(old_view.required_confirmations)
            },
        }

        if drift == "mapping_options":
            # Both targets remain safe. Sorting inserts a new option at index zero, so an
            # unbound old mapping_0=0 would now silently select /aaa instead of /anime.
            folders_state["paths"] = ["/anime", "/aaa"]
        elif drift == "scope":
            scope_state["value"] = AnimeScopeDiscovery(
                ScopeStatus.CONFIRM_SCOPE,
                (
                    AnimeRootCandidate("/data/media/aaa-anime", (42,)),
                    AnimeRootCandidate(binding.roots[0], (41,)),
                ),
                "Confirm the Sonarr anime folder.",
            )
        else:
            assessment_state["value"] = assess_settings_payload({})

        wait_for_background_view(lambda view: view is not None and view != old_view)
        stale = client.post(
            f"{base_url}/setup/confirm",
            data=old_form,
            headers=same_origin(base_url),
        )
        assert stale.status_code == 200
        assert "Setup was not saved" in stale.text
        assert API_SECRET_SENTINEL not in stale.text
        with open_state(bootstrap.database_path) as store:
            assert controller.onboarding(store).record() is None

        fresh_page = wait_for_page(lambda text: 'name="mapping_0"' in text)
        fresh_view = controller.view()
        assert fresh_view is not None
        assert fresh_view.confirmation_enabled
        target_index = fresh_view.mapping_options.index((binding.roots[0], "/anime"))
        fresh_form = {
            "csrf_token": csrf_from(fresh_page.text),
            "setup_digest": setup_digest_from(fresh_page.text),
            "mapping_0": str(target_index),
            "automatic_link_consent": "1",
            **{
                f"confirm_{index}": hashlib.sha256(fact.encode("utf-8")).hexdigest()
                for index, fact in enumerate(fresh_view.required_confirmations)
            },
        }
        confirmed = client.post(
            f"{base_url}/setup/confirm",
            data=fresh_form,
            headers=same_origin(base_url),
        )
        assert confirmed.status_code == 200
        assert "Setup saved" in confirmed.text
        assert API_SECRET_SENTINEL not in fresh_page.text + confirmed.text
        with open_state(bootstrap.database_path) as store:
            record = controller.onboarding(store).record()
            assert record is not None
            assert record.binding.roots == (binding.roots[0],)
            assert record.binding.path_mappings == (PathMapping(binding.roots[0], "/anime"),)
    finally:
        client.close()
        stop_server(server, server_thread)
        sonarr.client.close()
        shoko.client.close()


@pytest.mark.parametrize(
    "failure",
    [
        "unavailable",
        "scope_unavailable",
        "folders_unavailable",
        "safety_unavailable",
        "drift",
        "drift_then_outage",
    ],
)
def test_authenticated_recovery_keeps_system_review_actionable(  # noqa: PLR0915 - full auth flow
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    assessment = safe_assessment()
    binding = scope_binding(assessment)
    compatibility_state = {
        "value": ApiCompatibility(
            binding.sonarr_version,
            binding.shoko_version,
            True,
            True,
            binding.capabilities,
            (),
        )
    }
    scope = AnimeScopeDiscovery(
        ScopeStatus.CONFIRM_SCOPE,
        (AnimeRootCandidate(binding.roots[0], (41,)),),
        "Confirm the Sonarr anime folder.",
    )
    scope_state = {"value": scope}
    assessment_state = {"value": assessment}
    import_folder = ImportFolder(1, "Anime", "/anime", "None", True)
    scoped_assessment = evaluate_service_safety(
        assessment, binding.path_mappings, {import_folder.id: import_folder}
    )
    binding = replace(
        scope_binding(scoped_assessment),
        settings_observation=tuple(scoped_assessment.observed_settings.items()),
        selected_folders_observation=((import_folder.id, import_folder.path, "None"),),
    )
    folder_state: dict[str, tuple[ImportFolder, ...] | None] = {"value": (import_folder,)}
    discovery = ShokoDiscovery(
        (),
        {import_folder.id: import_folder},
        ShokoVerdict(True, (), ()),
        1,
        0,
        1000,
    )
    inventory = SonarrInventory((), (), (), (), (), (), (), InventoryVerdict(True, True, (), ()))
    path_result = PathMappingDiscovery(
        MappingDiscoveryStatus.READY,
        binding.path_mappings,
        (),
        "Exact paths establish this mapping.",
    )
    state_path = tmp_path / "state.sqlite3"
    bootstrap = BootstrapSettings(
        binding.sonarr_url,
        SecretRef("SONARR_API_KEY"),
        binding.shoko_url,
        SecretRef("SHOKO_API_KEY"),
        state_path,
    )
    sonarr = ApiEndpoint(httpx.Client(), lambda: {})
    shoko = ApiEndpoint(httpx.Client(), lambda: {})
    controller = ServiceSetupController(bootstrap, sonarr, shoko, state_path)
    monkeypatch.setattr(
        controller,
        "_environment",
        lambda: (
            compatibility_state["value"],
            scope_state["value"],
            assessment_state["value"],
        ),
    )
    monkeypatch.setattr(
        controller,
        "_inventory",
        lambda _roots: (inventory, discovery, path_result),
    )
    monkeypatch.setattr(
        "shoko_linker.service_setup.collect_import_folders",
        lambda *_: folder_state["value"],
    )
    with open_state(state_path) as store:
        OnboardingManager(store).confirm_setup(
            binding,
            scoped_assessment,
            confirmed_facts=(
                scoped_assessment.unverifiable_facts + scoped_assessment.unknown_settings
            ),
            automatic_link_consent=True,
            iso=T0.isoformat(),
        )

    if failure == "unavailable":
        compatibility_state["value"] = ApiCompatibility(
            None,
            binding.shoko_version,
            False,
            True,
            (),
            ("Cannot verify Sonarr compatibility.",),
        )
        expected_pause = "setup_unavailable"
        expected_status = OnboardingStatus.PAUSED
    elif failure == "scope_unavailable":
        scope_state["value"] = replace(scope, read_status=ScopeReadStatus.UNAVAILABLE)
        expected_pause = "setup_unavailable"
        expected_status = OnboardingStatus.PAUSED
    elif failure == "folders_unavailable":
        folder_state["value"] = None
        expected_pause = "setup_unavailable"
        expected_status = OnboardingStatus.PAUSED
    elif failure == "safety_unavailable":
        unavailable_client = httpx.Client(
            base_url="http://shoko:8111/",
            transport=httpx.MockTransport(lambda request: httpx.Response(503, request=request)),
        )
        try:
            assessment_state["value"] = inspect_shoko_safety_settings(
                ApiEndpoint(unavailable_client, lambda: {})
            )
        finally:
            unavailable_client.close()
        expected_pause = "setup_unavailable"
        expected_status = OnboardingStatus.PAUSED
    else:
        compatibility_state["value"] = ApiCompatibility(
            binding.sonarr_version,
            "5.3.4",
            True,
            False,
            binding.capabilities,
            ("Shoko version needs review.",),
        )
        expected_pause = "setup_changed"
        expected_status = OnboardingStatus.NEEDS_CONFIRMATION
    with open_state(state_path) as store:
        assert controller.current_context(store) is None
        assert store.get_service_pause() == (True, expected_pause)
        assert controller.onboarding(store).reconfirmation_required() is (
            failure in {"drift", "drift_then_outage"}
        )

    if failure == "drift_then_outage":
        compatibility_state["value"] = ApiCompatibility(
            None,
            binding.shoko_version,
            False,
            True,
            (),
            ("Cannot verify Sonarr compatibility.",),
        )
        with open_state(state_path) as store:
            assert controller.current_context(store) is None
            assert store.get_service_pause() == (True, "setup_changed")
            assert controller.onboarding(store).reconfirmation_required()

    compatibility_state["value"] = ApiCompatibility(
        binding.sonarr_version,
        binding.shoko_version,
        True,
        True,
        binding.capabilities,
        (),
    )
    scope_state["value"] = scope
    folder_state["value"] = (import_folder,)
    assessment_state["value"] = assessment
    with open_state(state_path) as store:
        current = controller.current_context(store)
        assert current is not None
        decision = OnboardingManager(store).evaluate_context(current, iso=T0.isoformat())
        assert decision.status is expected_status
        assert store.get_service_pause() == (True, expected_pause)
    recovered = controller.refresh_view()
    if failure not in {"drift", "drift_then_outage"}:
        assert recovered is None
    else:
        assert recovered is not None
        assert recovered.confirmation_enabled
        assert recovered.snapshot_digest

    def current_context() -> OnboardingContext | None:
        with open_state(state_path) as store:
            return controller.current_context(store)

    app = DashboardApplication(
        state_path,
        clock=MutableClock(),
        onboarding_context=current_context,
        setup_provider=controller.view,
        setup_action=controller.action,
    )
    server, server_thread, base_url = start_server(app)
    client = httpx.Client(follow_redirects=False)
    try:
        assert setup_account(client, base_url).status_code == 303
        page = client.get(f"{base_url}/status")
        assert page.status_code == 200
        if failure in {"drift", "drift_then_outage"}:
            assert "Confirm setup" in page.text
            held = client.post(
                f"{base_url}/resume",
                data={"csrf_token": csrf_from(page.text)},
                headers=same_origin(base_url),
            )
            assert held.status_code == 200
            assert "Setup must be confirmed again" in held.text
            page = client.get(f"{base_url}/status")
            assert "Confirm setup" in page.text
            view = controller.view()
            assert view is not None
            assert view.confirmation_enabled
            form = {
                "csrf_token": csrf_from(page.text),
                "setup_digest": setup_digest_from(page.text),
                "automatic_link_consent": "1",
                **{
                    f"confirm_{index}": hashlib.sha256(fact.encode("utf-8")).hexdigest()
                    for index, fact in enumerate(view.required_confirmations)
                },
            }
            confirmed = client.post(
                f"{base_url}/setup/confirm",
                data=form,
                headers=same_origin(base_url),
            )
            assert confirmed.status_code == 200
            assert "Setup saved" in confirmed.text
            page = confirmed
            with open_state(state_path) as store:
                assert store.get_service_pause() == (False, "")
        else:
            assert "Resume" in page.text
            assert "Confirm setup" not in page.text

        resumed = client.post(
            f"{base_url}/resume",
            data={"csrf_token": csrf_from(page.text)},
            headers=same_origin(base_url),
        )
        assert resumed.status_code == 303
        with open_state(state_path) as store:
            assert store.get_service_pause() == (False, "")
    finally:
        client.close()
        stop_server(server, server_thread)
        sonarr.client.close()
        shoko.client.close()


@pytest.mark.parametrize("origin", [None, "null", "http://attacker.invalid"])
def test_setup_rejects_unverifiable_origins(tmp_path: Path, origin: str | None) -> None:
    app = make_app(tmp_path)
    server, thread, base_url = start_server(app)
    try:
        with httpx.Client() as client:
            response = client.post(
                f"{base_url}/setup",
                data={"username": USERNAME, "password": PASSWORD},
                headers={} if origin is None else {"Origin": origin},
            )
            assert response.status_code == 403
        with open_state(tmp_path / "state.sqlite3") as store:
            assert store.get_dashboard_account() is None
    finally:
        stop_server(server, thread)


def test_dashboard_rejects_unbounded_or_malformed_setup_fields(tmp_path: Path) -> None:
    app = make_app(tmp_path)
    server, thread, base_url = start_server(app)
    try:
        with httpx.Client(follow_redirects=False) as client:
            cross_origin = client.post(
                f"{base_url}/setup",
                data={"username": USERNAME, "password": PASSWORD},
                headers={"Origin": "http://attacker.invalid"},
            )
            assert cross_origin.status_code == 403
            too_long = client.post(
                f"{base_url}/setup",
                data={"username": USERNAME, "password": "x" * 300},
                headers=same_origin(base_url),
            )
            assert too_long.status_code == 400
            malformed = client.post(
                f"{base_url}/setup",
                content=b"username=%ZZ&password=some-password-long-enough",
                headers={
                    **same_origin(base_url),
                    "Content-Type": "application/x-www-form-urlencoded",
                },
            )
            assert malformed.status_code == 400
            oversized = client.post(
                f"{base_url}/setup",
                content=b"x" * (MAX_DASHBOARD_REQUEST_BYTES + 1),
                headers=same_origin(base_url),
            )
            assert oversized.status_code == 413
        with open_state(tmp_path / "state.sqlite3") as store:
            assert store.get_dashboard_account() is None
    finally:
        stop_server(server, thread)


def test_disconnected_partial_body_is_not_dispatched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import socket
    from unittest.mock import Mock

    app = make_app(tmp_path)
    handle = Mock(wraps=app.handle)
    monkeypatch.setattr(app, "handle", handle)
    server = create_dashboard_server("127.0.0.1", 0, app)
    thread = threading.Thread(target=server.handle_request)
    thread.start()
    try:
        with socket.create_connection(("127.0.0.1", server.server_port), timeout=1) as client:
            client.sendall(b"POST /setup HTTP/1.1\r\nContent-Length: 100\r\n\r\na")
            client.shutdown(socket.SHUT_WR)
            assert client.recv(1024) == b""
        thread.join(timeout=2)
        assert not thread.is_alive()
        handle.assert_not_called()
    finally:
        server.server_close()
        thread.join(timeout=2)


@pytest.mark.skipif(os.name == "nt", reason="POSIX mode bits are not available on Windows")
def test_dashboard_credentials_use_owner_only_database_permissions(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_state(path) as store:
        store.create_dashboard_account(
            USERNAME, "scrypt$16384$8$1$salt$hash", b"k" * 32, T0.isoformat()
        )
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    for sidecar in (Path(f"{path}-wal"), Path(f"{path}-shm")):
        if sidecar.exists():
            assert stat.S_IMODE(sidecar.stat().st_mode) == 0o600
