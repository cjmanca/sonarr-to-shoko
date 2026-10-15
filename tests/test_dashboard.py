"""M5.1 first-user enrollment, login/session security, and authenticated controls."""

from __future__ import annotations

import os
import re
import stat
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from shoko_linker.dashboard import (
    DASHBOARD_SESSION_SECONDS,
    MAX_DASHBOARD_REQUEST_BYTES,
    DashboardApplication,
    DashboardStatus,
    create_dashboard_server,
    hash_password,
    verify_password,
)
from shoko_linker.onboarding import OnboardingManager, ScopeBinding
from shoko_linker.paths import PathMapping
from shoko_linker.safety_check import SafetyAssessment, assess_settings_payload
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
            OnboardingManager(store).confirm_setup(
                binding,
                assessment,
                confirmed_facts=(assessment.unverifiable_facts + assessment.unknown_settings),
                automatic_link_consent=True,
                iso=T0.isoformat(),
            )
    return DashboardApplication(
        path,
        clock=clock or MutableClock(),
        onboarding_context=lambda: (binding, assessment),
        status_provider=lambda: DashboardStatus(
            connection_message="Sonarr and Shoko are connected.",
            linked_files=12,
            waiting_files=4,
            review_files=2,
            automatic_linking=True,
            next_action="No action is needed.",
            diagnostics=("internal trace sentinel",),
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
