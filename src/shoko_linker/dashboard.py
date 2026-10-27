"""Small authenticated standard-library dashboard for setup status and ordinary controls."""

from __future__ import annotations

import base64
import hashlib
import hmac
import html
import json
import logging
import re
import secrets
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from http.cookies import CookieError, SimpleCookie
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from socket import SHUT_RDWR
from threading import Timer
from typing import Final
from urllib.parse import parse_qs, urlsplit

from .onboarding import (
    OnboardingContext,
    OnboardingDecision,
    OnboardingError,
    OnboardingManager,
    OnboardingStatus,
)
from .state import (
    DASHBOARD_SIGNING_KEY_BYTES,
    MAX_DASHBOARD_USERNAME_CHARS,
    BudgetLimits,
    DashboardAccount,
    StateError,
    StateStore,
    open_state,
)

_logger = logging.getLogger(__name__)

DASHBOARD_SESSION_SECONDS: Final[int] = 12 * 60 * 60
MAX_DASHBOARD_REQUEST_BYTES: Final[int] = 8192
MAX_DASHBOARD_DRAIN_BYTES: Final[int] = 65_536
DASHBOARD_REQUEST_TIMEOUT_SECONDS: Final[float] = 5.0
MIN_DASHBOARD_PASSWORD_CHARS: Final[int] = 12
MAX_DASHBOARD_PASSWORD_CHARS: Final[int] = 256
MAX_DASHBOARD_FORM_FIELDS: Final[int] = 64
MAX_DASHBOARD_COOKIE_CHARS: Final[int] = 2048
MAX_DASHBOARD_BASE64_CHARS: Final[int] = 256
SCRYPT_FIELD_COUNT: Final[int] = 6
_COOKIE_NAME: Final[str] = "shoko_linker_session"
_SCRYPT_N: Final[int] = 1 << 14
_SCRYPT_R: Final[int] = 8
_SCRYPT_P: Final[int] = 1
_SCRYPT_KEY_BYTES: Final[int] = 32
_SCRYPT_SALT_BYTES: Final[int] = 16
_BAD_ESCAPE_RE: Final[re.Pattern[str]] = re.compile(r"%(?![0-9A-Fa-f]{2})")
_DUMMY_SALT: Final[bytes] = b"shoko-linker-dummy-salt"
_DUMMY_HASH: Final[bytes] = bytes(_SCRYPT_KEY_BYTES)


@dataclass(frozen=True)
class DashboardStatus:
    """Plain-language status facts, plus details shown only in the authenticated view."""

    connection_message: str
    linked_files: int
    waiting_files: int
    review_files: int
    automatic_linking: bool
    next_action: str
    diagnostics: tuple[str, ...] = ()
    mapping_cache_status: str = "Waiting for setup."
    excluded_files: int = 0
    scope_excluded_files: int = 0
    exclusion_counts: tuple[tuple[str, int], ...] = ()
    worker_activity: str = ""
    skipped_file_details: tuple[str, ...] = ()


@dataclass(frozen=True)
class DashboardSetupView:
    """Current safe scope/safety choices shown only after dashboard authentication."""

    message: str
    root_options: tuple[str, ...]
    shoko_root_options: tuple[str, ...] = ()
    selected_roots: tuple[str, ...] = ()
    inferred_mappings: tuple[tuple[str, str], ...] = ()
    mapping_options: tuple[tuple[str, str], ...] = ()
    mapping_required: bool = False
    required_confirmations: tuple[str, ...] = ()
    confirmation_enabled: bool = True
    snapshot_digest: str = ""
    loading: bool = False
    progress_percent: int = 0


@dataclass(frozen=True)
class DashboardSettingsView:
    values: tuple[tuple[str, str, str], ...]
    advanced_yaml: str
    digest: str
    message: str
    database_path: str
    secret_env_vars: tuple[str, str]
    selected_roots: tuple[str, ...] = ()
    path_mappings: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class DashboardResponse:
    status: int
    body: bytes
    headers: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class _SessionToken:
    session_id: str
    session_id_hash: str
    csrf_token: str
    expires_epoch: int


class _DashboardInputError(ValueError):
    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


def _now_utc(clock: Callable[[], datetime]) -> datetime:
    value = clock()
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def hash_password(password: str) -> str:
    """Create the fixed-cost scrypt representation; plaintext is never persisted."""
    salt = secrets.token_bytes(_SCRYPT_SALT_BYTES)
    digest = hashlib.scrypt(
        password.encode("utf-8"),
        salt=salt,
        n=_SCRYPT_N,
        r=_SCRYPT_R,
        p=_SCRYPT_P,
        dklen=_SCRYPT_KEY_BYTES,
        maxmem=64 * 1024 * 1024,
    )
    return f"scrypt${_SCRYPT_N}${_SCRYPT_R}${_SCRYPT_P}${_b64encode(salt)}${_b64encode(digest)}"


def verify_password(password: str, encoded: str | None) -> bool:
    """Verify in constant time and do the same scrypt work for an unknown user."""
    salt = _DUMMY_SALT
    expected = _DUMMY_HASH
    valid_record = False
    n, r, p = _SCRYPT_N, _SCRYPT_R, _SCRYPT_P
    if encoded is not None:
        parts = encoded.split("$")
        if len(parts) == SCRYPT_FIELD_COUNT and parts[0] == "scrypt":
            try:
                parsed_n, parsed_r, parsed_p = (int(parts[1]), int(parts[2]), int(parts[3]))
                candidate_salt = _b64decode(parts[4])
                candidate_hash = _b64decode(parts[5])
            except (ValueError, TypeError):
                pass
            else:
                if (
                    (parsed_n, parsed_r, parsed_p) == (_SCRYPT_N, _SCRYPT_R, _SCRYPT_P)
                    and len(candidate_salt) == _SCRYPT_SALT_BYTES
                    and len(candidate_hash) == _SCRYPT_KEY_BYTES
                ):
                    salt = candidate_salt
                    expected = candidate_hash
                    valid_record = True
        else:
            n, r, p = _SCRYPT_N, _SCRYPT_R, _SCRYPT_P
    else:
        n, r, p = _SCRYPT_N, _SCRYPT_R, _SCRYPT_P
    actual = hashlib.scrypt(
        password.encode("utf-8"),
        salt=salt,
        n=n,
        r=r,
        p=p,
        dklen=_SCRYPT_KEY_BYTES,
        maxmem=64 * 1024 * 1024,
    )
    return valid_record and hmac.compare_digest(actual, expected)


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64decode(value: str) -> bytes:
    if not value or len(value) > MAX_DASHBOARD_BASE64_CHARS:
        raise ValueError("invalid base64 field")
    decoded = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    if _b64encode(decoded) != value:
        raise ValueError("non-canonical base64 field")
    return decoded


def _decode_session_cookie(signing_key: bytes, cookie_header: str) -> _SessionToken | None:
    """Authenticate and decode a signed cookie without revealing parsing failures."""
    token: _SessionToken | None = None
    try:
        cookie = SimpleCookie()
        cookie.load(cookie_header)
        morsel = cookie.get(_COOKIE_NAME)
        if morsel is None or len(morsel.value) > MAX_DASHBOARD_COOKIE_CHARS:
            raise ValueError("cookie is absent or oversized")
        payload_text, signature_text = morsel.value.split(".", 1)
        payload = _b64decode(payload_text)
        signature = _b64decode(signature_text)
        expected = hmac.new(signing_key, payload, hashlib.sha256).digest()
        if not hmac.compare_digest(signature, expected):
            raise ValueError("cookie signature does not match")
        document = json.loads(payload)
        if not isinstance(document, dict) or set(document) != {"sid", "exp", "csrf"}:
            raise ValueError("cookie payload is malformed")
        session_id = document["sid"]
        expires_epoch = document["exp"]
        csrf_token = document["csrf"]
        if (
            not isinstance(session_id, str)
            or not isinstance(expires_epoch, int)
            or isinstance(expires_epoch, bool)
            or not isinstance(csrf_token, str)
        ):
            raise ValueError("cookie payload fields are malformed")
        token = _SessionToken(
            session_id,
            hashlib.sha256(session_id.encode("ascii")).hexdigest(),
            csrf_token,
            expires_epoch,
        )
    except (CookieError, ValueError, TypeError, UnicodeError):
        pass
    return token


class DashboardApplication:
    """Request handler independent of the listener; opens short-lived SQLite views per request."""

    def __init__(
        self,
        state_path: str | Path,
        *,
        budget_limits: BudgetLimits | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        status_provider: Callable[[], DashboardStatus] | None = None,
        onboarding_context: Callable[[], OnboardingContext | None] | None = None,
        setup_provider: Callable[[], DashboardSetupView | None] | None = None,
        setup_action: Callable[[str, Mapping[str, str]], str] | None = None,
        settings_provider: Callable[[], DashboardSettingsView] | None = None,
        settings_action: Callable[[Mapping[str, str]], str] | None = None,
        control_changed: Callable[[], None] = lambda: None,
    ) -> None:
        self.state_path = Path(state_path)
        self.budget_limits = budget_limits
        self.clock = clock
        self.status_provider = status_provider or self._default_status
        self.onboarding_context = onboarding_context
        self.setup_provider = setup_provider
        self.setup_action = setup_action
        self.settings_provider = settings_provider
        self.settings_action = settings_action
        self.control_changed = control_changed

    @staticmethod
    def _default_status() -> DashboardStatus:
        return DashboardStatus(
            connection_message="Waiting for Sonarr and Shoko connection checks.",
            linked_files=0,
            waiting_files=0,
            review_files=0,
            automatic_linking=False,
            next_action="Complete setup before automatic linking can begin.",
        )

    def handle(
        self,
        method: str,
        target: str,
        headers: Mapping[str, str],
        body: bytes = b"",
    ) -> DashboardResponse:
        normalized_headers = {key.lower(): value for key, value in headers.items()}
        try:
            path = urlsplit(target).path or "/"
        except ValueError:
            return self._error(HTTPStatus.BAD_REQUEST, "Invalid request target.")
        refusal = self._request_refusal(method, normalized_headers, body)
        if refusal is not None:
            return refusal
        try:
            with open_state(self.state_path, budget_limits=self.budget_limits) as store:
                if method == "GET":
                    response = self._get(store, path, normalized_headers)
                else:
                    response = self._post(store, path, normalized_headers, body)
        except _DashboardInputError as exc:
            response = self._error(exc.status, str(exc))
        except OnboardingError:
            response = self._error(
                HTTPStatus.CONFLICT, "Setup needs to be reviewed before this action."
            )
        except (StateError, OSError):
            _logger.error("Dashboard request failed while accessing local state.")
            response = self._error(
                HTTPStatus.INTERNAL_SERVER_ERROR, "The dashboard is temporarily unavailable."
            )
        except Exception:
            _logger.error("Dashboard request failed.")
            response = self._error(
                HTTPStatus.INTERNAL_SERVER_ERROR, "The dashboard is temporarily unavailable."
            )
        return response

    def _request_refusal(
        self, method: str, headers: Mapping[str, str], body: bytes
    ) -> DashboardResponse | None:
        if len(body) > MAX_DASHBOARD_REQUEST_BYTES:
            return self._error(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "Request is too large.")
        if method not in {"GET", "POST"}:
            return self._error(HTTPStatus.METHOD_NOT_ALLOWED, "Method not allowed.")
        if method == "POST" and not self._same_origin(headers):
            return self._error(HTTPStatus.FORBIDDEN, "Request could not be verified.")
        return None

    def _get(self, store: StateStore, path: str, headers: Mapping[str, str]) -> DashboardResponse:
        account = store.get_dashboard_account()
        if path not in {"/", "/status", "/settings"}:
            return self._error(HTTPStatus.NOT_FOUND, "Page not found.")
        if account is None:
            return (
                self._html(HTTPStatus.OK, self._setup_page())
                if path == "/"
                else self._redirect("/")
            )
        session = self._read_session(store, account, headers.get("cookie", ""))
        if session is None:
            return (
                self._html(HTTPStatus.OK, self._login_page())
                if path == "/"
                else self._redirect("/")
            )
        paused, _reason = store.get_service_pause()
        status = self.status_provider()
        setup = None if self.setup_provider is None else self.setup_provider()
        settings = (
            self.settings_provider()
            if path == "/settings" and self.settings_provider is not None
            else None
        )
        page = self._status_page(
            status, session.csrf_token, paused=paused, setup=setup, settings=settings
        )
        return self._html(HTTPStatus.OK, page)

    def _post(
        self, store: StateStore, path: str, headers: Mapping[str, str], body: bytes
    ) -> DashboardResponse:
        if path == "/setup" and store.get_dashboard_account() is not None:
            return self._error(HTTPStatus.GONE, "Dashboard setup is already complete.")
        try:
            form = self._parse_form(headers, body)
        except _DashboardInputError as exc:
            return self._error(exc.status, str(exc))
        if path == "/setup":
            return self._setup(store, form)
        if path == "/login":
            return self._login(store, form)
        control_paths = {
            "/pause",
            "/resume",
            "/setup/scope",
            "/setup/confirm",
            "/setup/edit",
            "/settings",
        }
        setup_action_available = not path.startswith("/setup/") or self.setup_action is not None
        if path in control_paths and setup_action_available:
            return self._control(store, path, headers.get("cookie", ""), form)
        return self._error(HTTPStatus.NOT_FOUND, "Page not found.")

    def _setup(self, store: StateStore, form: Mapping[str, str]) -> DashboardResponse:
        if store.get_dashboard_account() is not None:
            return self._error(HTTPStatus.GONE, "Dashboard setup is already complete.")
        username = self._validate_username(form.get("username", ""))
        password = self._validate_password(form.get("password", ""))
        now = _now_utc(self.clock)
        signing_key = secrets.token_bytes(DASHBOARD_SIGNING_KEY_BYTES)
        if not store.create_dashboard_account(
            username, hash_password(password), signing_key, now.isoformat()
        ):
            return self._error(HTTPStatus.GONE, "Dashboard setup is already complete.")
        cookie = self._new_session(store, signing_key, now)
        if cookie is None:
            return self._error(HTTPStatus.SERVICE_UNAVAILABLE, "Please sign in to continue.")
        return self._redirect("/status", set_cookie=cookie)

    def _login(self, store: StateStore, form: Mapping[str, str]) -> DashboardResponse:
        account = store.get_dashboard_account()
        username = form.get("username", "")
        password = form.get("password", "")
        if (
            len(username) > MAX_DASHBOARD_USERNAME_CHARS
            or len(password) > MAX_DASHBOARD_PASSWORD_CHARS
        ):
            account = None
        password_ok = verify_password(password, None if account is None else account.password_hash)
        username_ok = account is not None and hmac.compare_digest(
            username.encode("utf-8"), account.username.encode("utf-8")
        )
        if account is None or not username_ok or not password_ok:
            return self._html(
                HTTPStatus.UNAUTHORIZED,
                self._login_page("Sign-in failed. Check your username and password."),
            )
        cookie = self._new_session(store, account.signing_key, _now_utc(self.clock))
        if cookie is None:
            return self._error(HTTPStatus.SERVICE_UNAVAILABLE, "Please try signing in again.")
        return self._redirect("/status", set_cookie=cookie)

    def _control(  # noqa: PLR0911 - authenticated controls retain distinct refusal responses
        self,
        store: StateStore,
        path: str,
        cookie_header: str,
        form: Mapping[str, str],
    ) -> DashboardResponse:
        account = store.get_dashboard_account()
        session = None if account is None else self._read_session(store, account, cookie_header)
        if account is None or session is None:
            return self._error(HTTPStatus.FORBIDDEN, "Sign in to continue.")
        token = form.get("csrf_token", "")
        supplied_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        if not token or not hmac.compare_digest(token, session.csrf_token):
            return self._error(HTTPStatus.FORBIDDEN, "Request could not be verified.")
        now = _now_utc(self.clock)
        next_csrf = secrets.token_urlsafe(32)
        if not store.rotate_dashboard_session_csrf(
            session.session_id_hash,
            supplied_hash,
            hashlib.sha256(next_csrf.encode("utf-8")).hexdigest(),
            int(now.timestamp()),
        ):
            return self._error(HTTPStatus.FORBIDDEN, "Request could not be verified.")
        cookie = self._encode_session_cookie(
            account.signing_key, session.session_id, session.expires_epoch, next_csrf
        )
        manager = OnboardingManager(store)
        if path == "/pause":
            manager.pause(iso=now.isoformat())
        elif path == "/resume":
            context = None if self.onboarding_context is None else self.onboarding_context()
            if context is None:
                decision = OnboardingDecision(
                    OnboardingStatus.NOT_CONFIGURED,
                    "Complete setup before automatic linking can resume.",
                )
            else:
                decision = manager.resume_context(context, iso=now.isoformat())
            if decision.status is not OnboardingStatus.AUTHORIZED:
                return self._html(
                    HTTPStatus.OK,
                    self._status_page(
                        self.status_provider(), next_csrf, paused=True, next_action=decision.message
                    ),
                    set_cookie=cookie,
                )
        elif path == "/settings":
            if self.settings_action is None or self.settings_provider is None:
                return self._error(HTTPStatus.NOT_FOUND, "Settings are unavailable.")
            message = self.settings_action(form)
            return self._html(
                HTTPStatus.OK,
                self._status_page(
                    self.status_provider(),
                    next_csrf,
                    paused=store.get_service_pause()[0],
                    acknowledgement=message,
                    setup=None if self.setup_provider is None else self.setup_provider(),
                    settings=self.settings_provider(),
                ),
                set_cookie=cookie,
            )
        else:
            action = {"/setup/scope": "scope", "/setup/confirm": "confirm", "/setup/edit": "edit"}[
                path
            ]
            assert self.setup_action is not None
            message = self.setup_action(action, form)
            self.control_changed()
            setup = None if self.setup_provider is None else self.setup_provider()
            return self._html(
                HTTPStatus.OK,
                self._status_page(
                    self.status_provider(),
                    next_csrf,
                    paused=store.get_service_pause()[0],
                    acknowledgement=message,
                    setup=setup,
                ),
                set_cookie=cookie,
            )
        self.control_changed()
        return self._redirect("/status", set_cookie=cookie)

    def _new_session(self, store: StateStore, signing_key: bytes, now: datetime) -> str | None:
        session_id = secrets.token_urlsafe(32)
        csrf_token = secrets.token_urlsafe(32)
        expires_epoch = int(now.timestamp()) + DASHBOARD_SESSION_SECONDS
        session_hash = hashlib.sha256(session_id.encode("ascii")).hexdigest()
        csrf_hash = hashlib.sha256(csrf_token.encode("ascii")).hexdigest()
        created = store.create_dashboard_session(
            session_hash, csrf_hash, expires_epoch, now.isoformat()
        )
        if not created:
            return None
        return self._encode_session_cookie(signing_key, session_id, expires_epoch, csrf_token)

    def _read_session(
        self, store: StateStore, account: DashboardAccount, cookie_header: str
    ) -> _SessionToken | None:
        token = _decode_session_cookie(account.signing_key, cookie_header)
        if token is None:
            return None
        now_epoch = int(_now_utc(self.clock).timestamp())
        if token.expires_epoch <= now_epoch:
            return None
        record = store.get_dashboard_session(token.session_id_hash, now_epoch)
        if record is None or record.expires_epoch != token.expires_epoch:
            return None
        csrf_hash = hashlib.sha256(token.csrf_token.encode("ascii")).hexdigest()
        if not hmac.compare_digest(record.csrf_hash, csrf_hash):
            return None
        return token

    @staticmethod
    def _encode_session_cookie(
        signing_key: bytes, session_id: str, expires_epoch: int, csrf_token: str
    ) -> str:
        payload = json.dumps(
            {"sid": session_id, "exp": expires_epoch, "csrf": csrf_token},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        payload_text = _b64encode(payload)
        signature = _b64encode(hmac.new(signing_key, payload, hashlib.sha256).digest())
        return (
            f"{_COOKIE_NAME}={payload_text}.{signature}; Path=/; HttpOnly; SameSite=Strict; "
            f"Max-Age={DASHBOARD_SESSION_SECONDS}"
        )

    @staticmethod
    def _validate_username(username: str) -> str:
        normalized = username.strip()
        if (
            not normalized
            or len(normalized) > MAX_DASHBOARD_USERNAME_CHARS
            or not normalized.isprintable()
        ):
            raise _DashboardInputError(HTTPStatus.BAD_REQUEST, "Choose a valid username.")
        return normalized

    @staticmethod
    def _validate_password(password: str) -> str:
        if (
            not MIN_DASHBOARD_PASSWORD_CHARS <= len(password) <= MAX_DASHBOARD_PASSWORD_CHARS
            or not password.isprintable()
        ):
            raise _DashboardInputError(
                HTTPStatus.BAD_REQUEST,
                f"Choose a password between {MIN_DASHBOARD_PASSWORD_CHARS} and "
                f"{MAX_DASHBOARD_PASSWORD_CHARS} characters.",
            )
        return password

    @staticmethod
    def _same_origin(headers: Mapping[str, str]) -> bool:
        origin = headers.get("origin")
        host = headers.get("host")
        if not origin or not host:
            return False
        try:
            parsed = urlsplit(origin)
        except ValueError:
            return False
        return parsed.scheme == "http" and parsed.netloc.lower() == host.lower()

    @staticmethod
    def _parse_form(headers: Mapping[str, str], body: bytes) -> Mapping[str, str]:
        content_type = headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if content_type != "application/x-www-form-urlencoded":
            raise _DashboardInputError(HTTPStatus.BAD_REQUEST, "Invalid form submission.")
        try:
            text = body.decode("utf-8")
            if _BAD_ESCAPE_RE.search(text):
                raise ValueError("invalid percent escape")
            parsed = parse_qs(
                text,
                keep_blank_values=True,
                strict_parsing=True,
                max_num_fields=MAX_DASHBOARD_FORM_FIELDS,
                encoding="utf-8",
                errors="strict",
            )
        except (UnicodeError, ValueError):
            raise _DashboardInputError(HTTPStatus.BAD_REQUEST, "Invalid form submission.") from None
        if any(len(values) != 1 for values in parsed.values()):
            raise _DashboardInputError(HTTPStatus.BAD_REQUEST, "Invalid form submission.")
        return {key: values[0] for key, values in parsed.items()}

    @staticmethod
    def _setup_page(error: str = "") -> str:
        return _page(
            "Create dashboard account",
            '<div class="eyebrow">Welcome to your library</div>'
            "<h1>Create your dashboard account</h1>"
            "<p>This account protects setup and controls.</p>"
            f'{_error(error)}<form method="post" action="/setup">'
            f'<label>Username <input name="username" '
            f'maxlength="{MAX_DASHBOARD_USERNAME_CHARS}" autocomplete="username" required></label>'
            f'<label>Password <input name="password" type="password" '
            f'minlength="{MIN_DASHBOARD_PASSWORD_CHARS}" '
            f'maxlength="{MAX_DASHBOARD_PASSWORD_CHARS}" autocomplete="new-password" '
            'aria-describedby="password-hint" required></label>'
            f'<p id="password-hint" class="hint">Use at least '
            f"{MIN_DASHBOARD_PASSWORD_CHARS} characters.</p>"
            '<button>Create account <span aria-hidden="true">&rarr;</span></button></form>',
            compact=True,
        )

    @staticmethod
    def _login_page(error: str = "") -> str:
        return _page(
            "Sign in",
            '<div class="eyebrow">Welcome back</div><h1>Sign in</h1>'
            "<p>Your library, connected. Sign in to manage automatic linking.</p>"
            f'{_error(error)}<form method="post" action="/login">'
            f'<label>Username <input name="username" maxlength="{MAX_DASHBOARD_USERNAME_CHARS}" '
            'autocomplete="username" required></label>'
            f'<label>Password <input name="password" type="password" '
            f'maxlength="{MAX_DASHBOARD_PASSWORD_CHARS}" '
            'autocomplete="current-password" required></label>'
            "<button>Sign in</button></form>",
            compact=True,
        )

    @staticmethod
    def _status_page(
        status: DashboardStatus,
        csrf_token: str,
        *,
        paused: bool,
        next_action: str | None = None,
        acknowledgement: str = "",
        setup: DashboardSetupView | None = None,
        settings: DashboardSettingsView | None = None,
    ) -> str:
        active = status.automatic_linking and not paused
        action = next_action or status.next_action
        diagnostics = "".join(f"<li>{html.escape(item)}</li>" for item in status.diagnostics)
        exclusion_categories = (
            ", ".join(
                f"{html.escape(reason)}: {count}"
                for reason, count in status.exclusion_counts
                if count
            )
            or "None"
        )
        skipped_files = "".join(
            f"<li>{html.escape(item)}</li>" for item in status.skipped_file_details
        )
        controls = (
            f'<form method="post" action="/pause">'
            f'<input type="hidden" name="csrf_token" value="{html.escape(csrf_token, quote=True)}">'
            '<button class="secondary">Pause</button></form>'
            f'<form method="post" action="/resume">'
            f'<input type="hidden" name="csrf_token" value="{html.escape(csrf_token, quote=True)}">'
            "<button>Resume</button></form>"
        )
        return _page(
            "Service settings" if settings else "Automatic linking status",
            '<header class="page-heading" data-live="heading"><div>'
            '<div class="eyebrow">Library overview</div>'
            f"<h1>{'Settings' if settings else 'Automatic linking'}</h1>"
            '<p>Keep your anime library in sync.</p></div>'
            f'<span class="status-badge {"running" if active else "paused"}">'
            f'<span aria-hidden="true" class="status-dot"></span>'
            f"{'Running' if active else 'Paused'}</span></header>"
            '<nav class="section-nav" aria-label="Dashboard navigation">'
            f'<a href="/status"{" aria-current=\"page\"" if not settings else ""}>Overview</a>'
            f'<a href="/settings"{" aria-current=\"page\"" if settings else ""}>Settings</a></nav>'
            + '<div data-live="acknowledgement" role="status">'
            + (f"<p>{html.escape(acknowledgement)}</p>" if acknowledgement else "")
            + "</div>"
            + (DashboardApplication._settings_section(settings, csrf_token) if settings else "")
            + '<p id="update-status" role="status" hidden></p>'
            '<div data-live="setup">'
            f"{DashboardApplication._setup_section(setup, csrf_token)}"
            '</div><dl class="metrics" data-live="metrics">'
            '<div class="metric verified"><dt>Verified files</dt>'
            f'<dd>{status.linked_files:,}</dd><dd class="metric-note">Confirmed in Shoko</dd></div>'
            '<div class="metric waiting"><dt>Waiting</dt>'
            f'<dd>{status.waiting_files:,}</dd><dd class="metric-note">'
            "Preparing or confirming links"
            '</dd></div><div class="metric review"><dt>Need review</dt>'
            f'<dd>{status.review_files:,}</dd><dd class="metric-note">Files needing attention'
            '</dd></div></dl><div class="overview-grid" data-live="overview">'
            '<section class="panel"><div class="eyebrow">Service health</div>'
            "<h2>Connections &amp; mappings</h2>"
            f'<p class="connection-message">{html.escape(status.connection_message)}</p>'
            + (
                '<p class="worker-activity" role="status">'
                f"{html.escape(status.worker_activity)}</p>"
                if status.worker_activity
                else ""
            )
            + '<dl class="facts"><div><dt>Episode mappings</dt>'
            f"<dd>{html.escape(status.mapping_cache_status)}</dd></div>"
            f"<div><dt>Latest cycle exclusions</dt><dd>{status.excluded_files}</dd></div>"
            f"<div><dt>Excluded outside scope</dt><dd>{status.scope_excluded_files}</dd></div>"
            f"<div><dt>Exclusion categories</dt><dd>{exclusion_categories}</dd></div>"
            "</dl></section>"
            '<section class="panel control-panel"><div class="eyebrow">Your next step</div>'
            f'<h2>Linking controls</h2><p class="next-action">{html.escape(action)}</p>'
            f'<nav aria-label="Linking controls">{controls}</nav>'
            '<p class="hint">Waiting includes work being prepared or waiting for Shoko '
            "to confirm a link. Partial or uncertain link results need attention; "
            "conflicts and safety issues are included.</p></section></div>"
            + '<section class="panel" data-live="skipped-files"><h2>Files skipped</h2>'
            + (
                "<p>Recent examples from the latest scan. Other eligible files can continue.</p>"
                f"<ul>{skipped_files}</ul>"
                if skipped_files
                else "<p>No skipped-file examples from the latest scan.</p>"
            )
            + "</section>"
            + '<details class="panel diagnostics" data-live="diagnostics">'
            "<summary>Advanced diagnostics</summary>"
            f"<ul>{diagnostics}</ul></details>",
            live=True,
        )

    @staticmethod
    def _settings_section(settings: DashboardSettingsView, csrf_token: str) -> str:
        fields = "".join(
            f'<label>{html.escape(label)}<input name="{html.escape(name, quote=True)}" '
            f'type="{"url" if name.endswith("_url") else "number"}" step="any" '
            f'value="{html.escape(value, quote=True)}" required></label>'
            for name, label, value in settings.values
        )
        token = html.escape(csrf_token, quote=True)
        return (
            '<section class="panel settings-panel"><div class="eyebrow">Preferences</div>'
            '<h2>Service settings</h2>'
            f'<p>{html.escape(settings.message)}</p>'
            '<form method="post" action="/settings">'
            f'<input type="hidden" name="csrf_token" value="{token}">'
            f'<input type="hidden" name="settings_digest" value="{settings.digest}">'
            f'{fields}<details><summary>Show advanced</summary>'
            '<p>Advanced service configuration (YAML). Safety and campaign fields apply to '
            'compatibility operations; normal automatic linking still requires scope consent. '
            'Hard safety limits cannot be increased here.</p>'
            '<label>Advanced configuration<textarea name="advanced_yaml" rows="24">'
            f'{html.escape(settings.advanced_yaml)}</textarea></label>'
            '<p>API keys are supplied by the container environment: '
            f'{html.escape(", ".join(settings.secret_env_vars))}. '
            'Change them in your container configuration and restart. '
            f'Persistent database: {html.escape(settings.database_path)}. '
            'Changing its location requires an explicit data migration.</p></details>'
            '<button>Save settings</button></form>'
            '<h2>Automatically linked libraries</h2>'
            '<div data-live="settings-scope"><ul>'
            + "".join(f"<li>{html.escape(root)}</li>" for root in settings.selected_roots)
            + "</ul>"
            "<ul>"
            + "".join(
                f"<li>{html.escape(source)} → {html.escape(target)}</li>"
                for source, target in settings.path_mappings
            )
            + "</ul></div>"
            "<p>Review anime folders and their Shoko folder mappings. Saving a changed "
            "selection pauses linking until you confirm the new scope and safety facts.</p>"
            '<form method="post" action="/setup/edit">'
            f'<input type="hidden" name="csrf_token" value="{token}">'
            '<button class="secondary">Change libraries and folder mappings</button></form>'
            "</section>"
        )

    @staticmethod
    def _setup_section(setup: DashboardSetupView | None, csrf_token: str) -> str:
        if setup is None:
            return ""
        root_choices = "".join(
            f'<label><input type="checkbox" name="root_{index}" '
            f'value="{html.escape(root, quote=True)}"'
            f'{" checked" if root in setup.selected_roots else ""}>'
            f"{html.escape(root)}</label>"
            for index, root in enumerate(setup.root_options)
        )
        scope_form = (
            '<form method="post" action="/setup/scope">'
            f'<input type="hidden" name="csrf_token" value="{html.escape(csrf_token, quote=True)}">'
            '<input type="hidden" name="setup_digest" '
            f'value="{html.escape(setup.snapshot_digest, quote=True)}">'
            f"{root_choices}<button>Continue</button></form>"
            if setup.root_options
            else ""
        )
        mapping_summary = "".join(
            f"<li>{html.escape(sonarr)} → {html.escape(shoko)}</li>"
            for sonarr, shoko in setup.inferred_mappings
        )
        mapping_fields = ""
        if setup.mapping_required and setup.selected_roots:
            fields: list[str] = []
            for index, root in enumerate(setup.selected_roots):
                options = "".join(
                    f'<option value="{option_index}">{html.escape(source)} → '
                    f"{html.escape(target)}</option>"
                    for option_index, (source, target) in enumerate(setup.mapping_options)
                    if source == root
                )
                fields.append(
                    f"<label>Folder match for {html.escape(root)} "
                    f'<select name="mapping_{index}" required>{options}</select></label>'
                )
            mapping_fields = "".join(fields)
        facts = "".join(
            f'<label><input type="checkbox" name="confirm_{index}" '
            f'value="{hashlib.sha256(fact.encode("utf-8")).hexdigest()}">'
            f"{html.escape(fact)}</label>"
            for index, fact in enumerate(setup.required_confirmations)
        )
        confirm_form = ""
        if setup.selected_roots and setup.confirmation_enabled:
            confirm_form = (
                '<form method="post" action="/setup/confirm">'
                '<input type="hidden" name="csrf_token" '
                f'value="{html.escape(csrf_token, quote=True)}">'
                '<input type="hidden" name="setup_digest" '
                f'value="{html.escape(setup.snapshot_digest, quote=True)}">'
                f"<ul>{mapping_summary}</ul>{mapping_fields}{facts}"
                '<label><input type="checkbox" name="automatic_link_consent" value="1" required>'
                "I authorize automatic linking for this scope.</label>"
                "<button>Confirm setup</button></form>"
            )
        return (
            '<section class="panel setup-panel" '
            f'data-setup-digest="{html.escape(setup.snapshot_digest, quote=True)}" '
            f'data-loading="{str(setup.loading).lower()}">'
            '<div class="eyebrow">Finish setup</div>'
            "<h2>Set up automatic linking</h2>"
            f"<p>{html.escape(setup.message)}</p>"
            + (
                '<div class="setup-progress" role="progressbar" '
                'aria-label="Estimated setup progress" aria-valuemin="0" aria-valuemax="100" '
                f'aria-valuenow="{setup.progress_percent}">'
                f'<span style="width:{setup.progress_percent}%"></span></div>'
                f'<p class="progress-label">About {setup.progress_percent}% complete</p>'
                '<p role="status">Setup checks start automatically. Large libraries can '
                "take several minutes. Keep this page open; choices appear automatically. "
                "Automatic linking waits for your confirmation.</p>"
                if setup.loading
                else ""
            )
            + f"{scope_form}{confirm_form}</section>"
        )

    @staticmethod
    def _redirect(location: str, *, set_cookie: str | None = None) -> DashboardResponse:
        headers = [("Location", location)]
        if set_cookie is not None:
            headers.append(("Set-Cookie", set_cookie))
        return DashboardResponse(HTTPStatus.SEE_OTHER, b"", tuple(headers))

    @staticmethod
    def _html(
        status: int,
        document: str,
        *,
        set_cookie: str | None = None,
    ) -> DashboardResponse:
        headers = [("Content-Type", "text/html; charset=utf-8")]
        if set_cookie is not None:
            headers.append(("Set-Cookie", set_cookie))
        return DashboardResponse(status, document.encode("utf-8"), tuple(headers))

    @staticmethod
    def _error(status: int, message: str) -> DashboardResponse:
        return DashboardApplication._html(
            status, _page("Dashboard", f"<h1>{html.escape(message)}</h1>")
        )


def _error(message: str) -> str:
    return "" if not message else f'<p role="alert">{html.escape(message)}</p>'


_DASHBOARD_JS: Final[str] = """
(() => {
  let stopped = false;
  const notice = document.getElementById('update-status');
  async function update() {
    try {
      const statusUrl = window.location.pathname === '/settings' ? '/settings' : '/status';
      const response = await fetch(statusUrl, {
        credentials: 'same-origin', cache: 'no-store', signal: AbortSignal.timeout(10000)
      });
      if (!response.ok) throw new Error('Status unavailable');
      const next = new DOMParser().parseFromString(await response.text(), 'text/html');
      if (!next.querySelector('[data-live="metrics"]')) {
        stopped = true;
        notice.hidden = false;
        notice.textContent = 'Your session expired. ';
        const login = document.createElement('a');
        login.href = '/'; login.textContent = 'Sign in again'; notice.append(login);
        return;
      }
      let changedSetup = false;
      const currentToken = next.querySelector('input[name="csrf_token"]');
      if (currentToken) document.querySelectorAll('input[name="csrf_token"]').forEach(
        input => { input.value = currentToken.value; });
      for (const current of document.querySelectorAll('[data-live]')) {
        const fresh = next.querySelector('[data-live="' + current.dataset.live + '"]');
        if (!fresh) continue;
        if (current.dataset.live === 'setup') {
          const oldPanel = current.querySelector('[data-setup-digest]');
          const newPanel = fresh.querySelector('[data-setup-digest]');
          // Retain choices during cache refreshes and while the same facts are displayed.
          if (oldPanel && newPanel && oldPanel.dataset.loading !== 'true' &&
              (newPanel.dataset.loading === 'true' ||
               oldPanel.dataset.setupDigest === newPanel.dataset.setupDigest)) {
            const checking = newPanel.dataset.loading === 'true';
            current.querySelectorAll('button').forEach(button => { button.disabled = checking; });
            current.querySelector('.setup-recheck')?.remove();
            if (checking) {
              const progress = document.createElement('div');
              progress.className = 'setup-recheck';
              const explanation = document.createElement('p');
              explanation.textContent = 'Rechecking setup. Your choices are kept; '
                + 'submission will be available when checks finish.';
              progress.append(explanation);
              for (const element of newPanel.querySelectorAll('.setup-progress, .progress-label')) {
                progress.append(element.cloneNode(true));
              }
              current.append(progress);
            }
            const token = next.querySelector('input[name="csrf_token"]');
            if (token) current.querySelectorAll('input[name="csrf_token"]').forEach(
              input => { input.value = token.value; });
            continue;
          }
          changedSetup = Boolean(oldPanel && newPanel &&
            oldPanel.dataset.loading !== 'true' && newPanel.dataset.loading !== 'true');
        }
        if (current.tagName === 'DETAILS') fresh.open = current.open;
        if (current.outerHTML !== fresh.outerHTML) {
          current.replaceWith(fresh);
        }
      }
      notice.textContent = changedSetup
        ? 'Setup facts changed. Review the updated choices and confirmations.'
        : '';
      notice.hidden = !changedSetup;
    } catch (_) {
      notice.hidden = false;
      notice.textContent = 'Live updates interrupted. Retrying automatically…';
    } finally {
      if (!stopped) window.setTimeout(update, 2000);
    }
  }
  window.setTimeout(update, 2000);
})();
"""
_DASHBOARD_SCRIPT_HASH: Final[str] = base64.b64encode(
    hashlib.sha256(_DASHBOARD_JS.encode("utf-8")).digest()
).decode("ascii")


_DASHBOARD_CSS: Final[str] = """
:root {
  color-scheme: light;
  --ink: #18243b; --muted: #58677e; --line: #e1e7f0;
  --accent: #5146cf; --accent-hover: #4036b0; --surface: #fff;
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  background: #f4f6fb; color: var(--ink); font-size: 15px; line-height: 1.6;
}
* { box-sizing: border-box; }
body { margin: 0; min-height: 100vh; }
.site-header { background: var(--surface); border-bottom: 1px solid var(--line); }
.header-inner {
  max-width: 1160px; margin: auto; padding: 21px 32px; display: flex;
  align-items: center; justify-content: space-between; gap: 20px;
}
.brand { display: flex; align-items: center; gap: 12px; font-size: 17px;
  font-weight: 750; letter-spacing: -.4px; }
.brand-mark {
  display: grid; place-items: center; width: 38px; height: 38px; border-radius: 12px;
  background: var(--accent); color: white; font-size: 25px; line-height: 1;
  box-shadow: 0 4px 10px #5146cf26;
}
.header-caption { color: var(--muted); font-size: 13px; }
main { max-width: 1160px; margin: auto; padding: 44px 32px 24px; }
h1, h2, p, dl { margin-top: 0; }
h1 { font-size: clamp(26px, 4vw, 34px); line-height: 1.2;
  letter-spacing: -1.1px; margin-bottom: 12px; }
h2 { font-size: 18px; line-height: 1.4; letter-spacing: -.35px; margin-bottom: 18px; }
p { color: var(--muted); margin-bottom: 20px; }
.eyebrow { font-size: 11px; text-transform: uppercase; letter-spacing: 1.6px;
  font-weight: 750; color: var(--accent); margin-bottom: 10px; }
.page-heading { display: flex; justify-content: space-between; align-items: center;
  gap: 20px; margin-bottom: 30px; }
.page-heading p { margin-bottom: 0; }
.status-badge { display: inline-flex; align-items: center; gap: 8px;
  border: 1px solid; border-radius: 100px; padding: 6px 14px;
  font-size: 13px; font-weight: 650; white-space: nowrap; }
.running { color: #176546; background: #edf9f2; border-color: #c9e8d7; }
.paused { color: #85570c; background: #fff7e7; border-color: #efdcaf; }
.status-dot { width: 7px; height: 7px; border-radius: 50%; background: currentColor; }
.metrics { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 20px; margin-bottom: 24px; }
.metric, .panel { background: var(--surface); border: 1px solid var(--line);
  border-radius: 16px; box-shadow: 0 3px 12px #18243b04; }
.metric { padding: 24px; border-top: 3px solid var(--accent); }
.metric dt { color: var(--muted); font-size: 13px; font-weight: 650; }
dd { margin: 0; }
.metric dd { font-size: 38px; font-weight: 700; line-height: 1.5;
  letter-spacing: -1.5px; font-variant-numeric: tabular-nums; overflow-wrap: anywhere; }
.metric .metric-note { font-size: 12px; font-weight: 400; letter-spacing: 0;
  color: var(--muted); }
.verified { border-top-color: #339877; }
.verified > dd:first-of-type { color: #207657; }
.review { border-top-color: #d4a140; }
.review > dd:first-of-type { color: #966311; }
.overview-grid { display: grid; grid-template-columns: minmax(0, 1.35fr) minmax(0, 1fr);
  gap: 24px; margin-bottom: 24px; }
.panel { padding: 26px; min-width: 0; overflow-wrap: anywhere; }
.connection-message { padding: 12px 16px; background: #f4f6fb; border-radius: 9px;
  color: var(--ink); font-size: 14px; }
.facts { margin-bottom: 0; font-size: 13px; }
.facts > div { display: flex; justify-content: space-between; gap: 20px;
  padding: 12px 0; border-top: 1px solid var(--line); }
.facts dt { color: var(--muted); flex: 1; }
.facts dd { flex: 1; text-align: right; font-weight: 550; }
.next-action { color: var(--ink); }
nav { display: flex; flex-wrap: wrap; gap: 10px; margin: 24px 0; }
nav form { margin: 0; }
.section-nav { display: inline-flex; gap: 4px; margin: 0 0 28px; padding: 5px;
  background: #eeedf8; border: 1px solid #e1def3; border-radius: 12px; }
.section-nav a { padding: 8px 20px; border-radius: 8px; text-decoration: none;
  color: var(--muted); font-size: 14px; font-weight: 650; }
.section-nav a:hover { color: var(--accent); background: #ffffff80; }
.section-nav a[aria-current="page"] { color: var(--accent); background: var(--surface);
  box-shadow: 0 2px 6px #18243b0d; }
#update-status:not([hidden]) { padding: 12px 16px; background: #fff7e7;
  color: #85570c; border: 1px solid #efdcaf; border-radius: 9px; }
button, .button { background: var(--accent); color: white; border: 1px solid var(--accent);
  border-radius: 9px; padding: 11px 20px; font: inherit; font-size: 14px;
  font-weight: 650; cursor: pointer; transition: background .15s, box-shadow .15s; }
button:hover { background: var(--accent-hover); box-shadow: 0 3px 8px #5146cf22; }
.button { display: inline-block; text-decoration: none; }
button.secondary, .button.secondary { background: white; color: var(--ink);
  border-color: #cbd3e1; }
button.secondary:hover { background: #f0f2f8; }
:focus-visible { outline: 3px solid #8277ef; outline-offset: 3px; }
button:disabled { opacity: .55; cursor: not-allowed; }
.hint { font-size: 12px; color: var(--muted); margin: 12px 0 0; }
.setup-panel { margin-bottom: 24px; }
.setup-progress { height: 10px; overflow: hidden; border-radius: 10px;
  background: #e8e5fb; margin: 20px 0; }
.setup-progress span { display: block; min-width: 3%; height: 100%; border-radius: inherit;
  background: linear-gradient(90deg, #5146cf, #9187f1, #5146cf);
  background-size: 200% 100%; animation: setup-progress 1.5s linear infinite;
  transition: width .4s ease; }
@keyframes setup-progress { to { background-position: -200% 0; } }
label { display: block; font-size: 14px; font-weight: 550; margin: 18px 0; }
input:not([type="hidden"]):not([type="checkbox"]), select, textarea {
  display: block; width: 100%; margin-top: 7px; padding: 11px 13px;
  border: 1px solid #cbd3e1; border-radius: 9px; background: #fff;
  color: var(--ink); font: inherit; min-width: 0;
}
input[type="checkbox"] { accent-color: var(--accent); width: 17px; height: 17px;
  margin: 0 10px 0 0; vertical-align: middle; }
.setup-panel form + form { margin-top: 24px; padding-top: 6px;
  border-top: 1px solid var(--line); }
.settings-panel { margin-bottom: 24px; }
.settings-panel h2:not(:first-child) { margin-top: 24px; }
textarea { resize: vertical; font-family: ui-monospace, monospace; }
.diagnostics { padding: 18px 24px; }
summary { cursor: pointer; font-size: 13px; font-weight: 650; color: var(--muted); }
details[open] summary { margin-bottom: 16px; }
details ul { font-family: ui-monospace, monospace; font-size: 12px; padding-left: 20px; }
[role="alert"] { color: #9c293d; background: #fff0f2; border: 1px solid #f1cbd2;
  border-radius: 9px; padding: 12px 16px; }
.compact { max-width: 520px; margin: 58px auto 32px; padding: 0 24px; }
.auth-card { background: white; border: 1px solid var(--line); border-radius: 20px;
  padding: 36px; box-shadow: 0 12px 40px #18243b08; }
.compact h1 { font-size: 28px; letter-spacing: -.8px; }
.compact form button { width: 100%; margin-top: 24px; }
footer { max-width: 1160px; padding: 24px 32px 32px; margin: auto;
  color: var(--muted); font-size: 12px; text-align: center; }
@media (max-width: 760px) {
  .overview-grid { grid-template-columns: 1fr; }
  .metrics { gap: 12px; }
  .metric { padding: 18px; }
  .header-caption { display: none; }
}
@media (max-width: 480px) {
  .header-inner { padding: 16px 20px; }
  main { padding: 28px 20px 12px; }
  .page-heading { align-items: flex-start; flex-direction: column; gap: 14px; }
  .metrics { grid-template-columns: 1fr; }
  .metric { padding: 18px 22px; }
  .metric dd { font-size: 32px; }
  .panel { padding: 22px; }
  .facts > div { flex-direction: column; gap: 4px; }
  .facts dd { text-align: left; }
  .compact { margin-top: 28px; padding: 0 20px; }
  .auth-card { padding: 26px; }
}
@media (prefers-reduced-motion: reduce) {
  button { transition: none; }
  .setup-progress span { animation: none; transition: none; }
}
"""


def _page(title: str, body: str, *, compact: bool = False, live: bool = False) -> str:
    main_class = ' class="compact"' if compact else ""
    content = f'<div class="auth-card">{body}</div>' if compact else body
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f"<title>{html.escape(title)} · Shoko&ndash;Sonarr Linker</title>"
        f"<style>{_DASHBOARD_CSS}</style></head><body>"
        '<header class="site-header"><div class="header-inner">'
        '<div class="brand"><span class="brand-mark" aria-hidden="true">&#8644;</span>'
        'Shoko&ndash;Sonarr Linker</div><span class="header-caption">Your anime library, connected'
        "</span></div></header>"
        f"<main{main_class}>{content}</main>"
        "<footer>Sonarr metadata. Shoko connections. One organized library.</footer>"
        + (f"<script>{_DASHBOARD_JS}</script>" if live else "")
        + "</body></html>"
    )


def create_dashboard_server(host: str, port: int, application: DashboardApplication) -> HTTPServer:
    """Create an explicitly bound, single-request-at-a-time local HTTP server."""

    class DashboardHandler(BaseHTTPRequestHandler):
        server_version = "ShokoSonarrLinker"
        sys_version = ""

        def handle(self) -> None:
            # Bound the entire accepted request, including trickled headers/body.
            # Socket inactivity timeouts alone reset whenever another byte arrives.
            self.connection.settimeout(DASHBOARD_REQUEST_TIMEOUT_SECONDS)
            deadline = Timer(DASHBOARD_REQUEST_TIMEOUT_SECONDS, self._expire_connection)
            self._read_deadline = deadline
            deadline.daemon = True
            deadline.start()
            try:
                super().handle()
            except OSError:
                # Timeouts, EOF/reset, and disconnected response readers are ordinary
                # client failures; never emit request data or tracebacks.
                self.close_connection = True
            finally:
                deadline.cancel()

        def _expire_connection(self) -> None:
            # The request may already have completed/disconnected.
            with suppress(OSError):
                self.connection.shutdown(SHUT_RDWR)

        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler hook
            self._dispatch()

        def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler hook
            raw_length = self.headers.get("Content-Length", "")
            try:
                length = int(raw_length)
            except ValueError:
                self._send(application._error(HTTPStatus.BAD_REQUEST, "Invalid request body."))
                return
            if length < 0:
                self._send(application._error(HTTPStatus.BAD_REQUEST, "Invalid request body."))
                return
            if length > MAX_DASHBOARD_REQUEST_BYTES:
                if length <= MAX_DASHBOARD_DRAIN_BYTES:
                    self.rfile.read(length)
                else:
                    self.close_connection = True
                self._send(
                    application._error(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "Request is too large.")
                )
                return
            if self.headers.get("Transfer-Encoding") is not None:
                self._send(application._error(HTTPStatus.BAD_REQUEST, "Invalid request body."))
                return
            body = self.rfile.read(length)
            if len(body) != length:
                self.close_connection = True
                return
            self._dispatch(body)

        def _dispatch(self, body: bytes = b"") -> None:
            # The deadline bounds client input, not local password hashing or setup work.
            self._read_deadline.cancel()
            response = application.handle(self.command, self.path, dict(self.headers.items()), body)
            self._send(response)

        def _send(self, response: DashboardResponse) -> None:
            self._read_deadline.cancel()
            self.send_response(response.status)
            self.send_header("Content-Length", str(len(response.body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            # no-referrer makes native form POSTs send Origin: null, which our
            # same-origin verification must reject. Keep origin on local forms.
            self.send_header("Referrer-Policy", "same-origin")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; "
                f"script-src 'sha256-{_DASHBOARD_SCRIPT_HASH}'; connect-src 'self'; "
                "base-uri 'none'; frame-ancestors 'none'",
            )
            for name, value in response.headers:
                self.send_header(name, value)
            self.end_headers()
            if response.body:
                self.wfile.write(response.body)

        def log_message(self, _format: str, *_args: object) -> None:
            # Request paths/forms may carry credentials; the management surface is log-silent.
            return

    class LocalDashboardServer(HTTPServer):
        allow_reuse_address = True

    return LocalDashboardServer((host, port), DashboardHandler)
