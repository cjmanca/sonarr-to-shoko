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
from dataclasses import dataclass
from datetime import UTC, datetime
from http import HTTPStatus
from http.cookies import CookieError, SimpleCookie
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Final
from urllib.parse import parse_qs, urlsplit

from .onboarding import (
    OnboardingDecision,
    OnboardingError,
    OnboardingManager,
    OnboardingStatus,
    ScopeBinding,
)
from .safety_check import SafetyAssessment
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
        onboarding_context: Callable[[], tuple[ScopeBinding, SafetyAssessment] | None]
        | None = None,
        setup_provider: Callable[[], DashboardSetupView | None] | None = None,
        setup_action: Callable[[str, Mapping[str, str]], str] | None = None,
    ) -> None:
        self.state_path = Path(state_path)
        self.budget_limits = budget_limits
        self.clock = clock
        self.status_provider = status_provider or self._default_status
        self.onboarding_context = onboarding_context
        self.setup_provider = setup_provider
        self.setup_action = setup_action

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
        if path not in {"/", "/status"}:
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
        page = self._status_page(status, session.csrf_token, paused=paused, setup=setup)
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
        control_paths = {"/pause", "/resume", "/setup/scope", "/setup/confirm"}
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

    def _control(
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
                binding, assessment = context
                decision = manager.resume(binding, assessment, iso=now.isoformat())
            if decision.status is not OnboardingStatus.AUTHORIZED:
                return self._html(
                    HTTPStatus.OK,
                    self._status_page(
                        self.status_provider(), next_csrf, paused=True, next_action=decision.message
                    ),
                    set_cookie=cookie,
                )
        else:
            action = "scope" if path == "/setup/scope" else "confirm"
            assert self.setup_action is not None
            message = self.setup_action(action, form)
            setup = None if self.setup_provider is None else self.setup_provider()
            return self._html(
                HTTPStatus.OK,
                self._status_page(
                    self.status_provider(),
                    next_csrf,
                    paused=store.get_service_pause()[0],
                    next_action=message,
                    setup=setup,
                ),
                set_cookie=cookie,
            )
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
            "<h1>Create your dashboard account</h1>"
            "<p>This account protects setup and controls.</p>"
            f'{_error(error)}<form method="post" action="/setup">'
            f'<label>Username <input name="username" '
            f'maxlength="{MAX_DASHBOARD_USERNAME_CHARS}" required></label>'
            f'<label>Password <input name="password" type="password" '
            f'minlength="{MIN_DASHBOARD_PASSWORD_CHARS}" '
            f'maxlength="{MAX_DASHBOARD_PASSWORD_CHARS}" required></label>'
            "<button>Create account</button></form>",
        )

    @staticmethod
    def _login_page(error: str = "") -> str:
        return _page(
            "Sign in",
            f'<h1>Sign in</h1>{_error(error)}<form method="post" action="/login">'
            f'<label>Username <input name="username" maxlength="{MAX_DASHBOARD_USERNAME_CHARS}" '
            'autocomplete="username" required></label>'
            f'<label>Password <input name="password" type="password" '
            f'maxlength="{MAX_DASHBOARD_PASSWORD_CHARS}" '
            'autocomplete="current-password" required></label>'
            "<button>Sign in</button></form>",
        )

    @staticmethod
    def _status_page(
        status: DashboardStatus,
        csrf_token: str,
        *,
        paused: bool,
        next_action: str | None = None,
        setup: DashboardSetupView | None = None,
    ) -> str:
        active = status.automatic_linking and not paused
        action = next_action or ("Automatic linking is paused." if paused else status.next_action)
        diagnostics = "".join(f"<li>{html.escape(item)}</li>" for item in status.diagnostics)
        controls = (
            f'<form method="post" action="/pause">'
            f'<input type="hidden" name="csrf_token" value="{html.escape(csrf_token, quote=True)}">'
            "<button>Pause</button></form>"
            f'<form method="post" action="/resume">'
            f'<input type="hidden" name="csrf_token" value="{html.escape(csrf_token, quote=True)}">'
            "<button>Resume</button></form>"
        )
        return _page(
            "Automatic linking status",
            "<h1>Automatic linking</h1>"
            f"<p>{html.escape(status.connection_message)}</p>"
            f"<dl><dt>Verified files</dt><dd>{status.linked_files}</dd>"
            f"<dt>Waiting</dt><dd>{status.waiting_files}</dd>"
            f"<dt>Need review</dt><dd>{status.review_files}</dd>"
            f"<dt>Episode mappings</dt><dd>{html.escape(status.mapping_cache_status)}</dd>"
            f"<dt>Automatic linking</dt><dd>{'Running' if active else 'Paused'}</dd></dl>"
            f"<p>{html.escape(action)}</p><nav>{controls}</nav>"
            f"{DashboardApplication._setup_section(setup, csrf_token)}"
            f"<details><summary>Advanced diagnostics</summary><ul>{diagnostics}</ul></details>",
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
                f"<ul>{mapping_summary}</ul>{mapping_fields}{facts}"
                '<label><input type="checkbox" name="automatic_link_consent" value="1" required>'
                "I authorize automatic linking for this scope.</label>"
                "<button>Confirm setup</button></form>"
            )
        return (
            "<section><h2>Set up automatic linking</h2>"
            f"<p>{html.escape(setup.message)}</p>{scope_form}{confirm_form}</section>"
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


def _page(title: str, body: str) -> str:
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f"<title>{html.escape(title)}</title></head><body>{body}</body></html>"
    )


def create_dashboard_server(host: str, port: int, application: DashboardApplication) -> HTTPServer:
    """Create an explicitly bound, single-request-at-a-time local HTTP server."""

    class DashboardHandler(BaseHTTPRequestHandler):
        server_version = "ShokoSonarrLinker"
        sys_version = ""

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
            self._dispatch(self.rfile.read(length))

        def _dispatch(self, body: bytes = b"") -> None:
            response = application.handle(self.command, self.path, dict(self.headers.items()), body)
            self._send(response)

        def _send(self, response: DashboardResponse) -> None:
            self.send_response(response.status)
            self.send_header("Content-Length", str(len(response.body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; "
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
