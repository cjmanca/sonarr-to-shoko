"""CLI behavior: exit codes, refusal messages, stub honesty. Offline only, no network."""

from __future__ import annotations

import signal
import sqlite3
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pytest
import state_v1_fixture as v1fx

from shoko_linker import cli
from shoko_linker.bootstrap import BootstrapSettings
from shoko_linker.cli import (
    EXIT_APPLY_GATE_REFUSAL,
    EXIT_CONFIG_INVALID,
    EXIT_OK,
    EXIT_RUNTIME,
    main,
)
from shoko_linker.config import SecretRef
from shoko_linker.http import ApiEndpoint
from shoko_linker.service import SERVICE_POLL_SECONDS, ConnectionReport
from shoko_linker.state import SCHEMA_VERSION, StateStore

EXAMPLE_CONFIG = Path(__file__).resolve().parents[1] / "config.example.yaml"
DUMMY_SHOKO_KEY = "dummy-shoko-key-value-00000000"


def test_service_command_requires_no_yaml_path() -> None:
    try:
        args = cli.build_parser().parse_args(["service"])
    except SystemExit as exc:
        pytest.fail(f"service must be a normal command with no required config path: {exc}")
    assert args.command == "service"


def test_ordinary_control_commands_require_no_yaml_path() -> None:
    for command in ("status", "pause", "resume"):
        try:
            args = cli.build_parser().parse_args([command])
        except SystemExit as exc:
            pytest.fail(f"{command} must be a normal command with no YAML path: {exc}")
        assert args.command == command


def test_ordinary_controls_persist_pause_and_refuse_resume_without_setup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    state_path = tmp_path / "state.sqlite3"
    monkeypatch.setattr(cli, "DEFAULT_DATABASE_PATH", state_path)

    assert main(["pause"]) == EXIT_OK
    assert "paused" in capsys.readouterr().out
    assert main(["status"]) == EXIT_OK
    status = capsys.readouterr().out
    assert "Automatic linking: paused" in status

    assert main(["resume"]) == cli.EXIT_STATE
    assert "Complete dashboard setup" in capsys.readouterr().out
    store = StateStore(state_path)
    try:
        assert store.get_service_pause() == (True, "user_paused")
    finally:
        store.close()


def test_status_opens_stopped_legacy_database_without_remote_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    state_path = tmp_path / "legacy-state.sqlite3"
    v1fx.build_v1_database(state_path)
    connection = sqlite3.connect(state_path)
    try:
        connection.executemany(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            (("writes_disabled", "1"), ("writes_disabled_reason", "operator repair required")),
        )
        connection.commit()
    finally:
        connection.close()
    monkeypatch.setattr(cli, "DEFAULT_DATABASE_PATH", state_path)
    monkeypatch.setattr(
        cli,
        "bootstrap_endpoints",
        lambda *_args, **_kwargs: pytest.fail("offline status must not contact Sonarr or Shoko"),
    )

    assert main(["status"]) == EXIT_OK
    output = capsys.readouterr().out
    assert "Automatic linking: waiting for setup confirmation" in output

    store = StateStore(state_path)
    try:
        assert store.schema_version() == SCHEMA_VERSION
        assert store.writes_disabled is True
        assert store.get_meta("writes_disabled_reason") == "operator repair required"
    finally:
        store.close()


def test_service_reports_missing_connection_setting_without_secret_values(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("SONARR_URL", raising=False)
    monkeypatch.setenv("SONARR_API_KEY", "sonarr-test-key-never-a-real-secret")
    monkeypatch.setenv("SHOKO_URL", "http://shoko:8111")
    monkeypatch.setenv("SHOKO_API_KEY", "shoko-test-key-never-a-real-secret")

    code = main(["service"])
    captured = capsys.readouterr()

    assert code == EXIT_CONFIG_INVALID
    assert "SONARR_URL" in captured.err
    assert "sonarr-test-key-never-a-real-secret" not in captured.err
    assert "shoko-test-key-never-a-real-secret" not in captured.err


def test_service_command_starts_managed_dashboard_worker_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SONARR_URL", "http://sonarr:8989")
    monkeypatch.setenv("SONARR_API_KEY", "sonarr-test-key-never-a-real-secret")
    monkeypatch.setenv("SHOKO_URL", "http://shoko:8111")
    monkeypatch.setenv("SHOKO_API_KEY", "shoko-test-key-never-a-real-secret")
    sonarr = ApiEndpoint(httpx.Client(), lambda: {})
    shoko = ApiEndpoint(httpx.Client(), lambda: {})
    invoked: dict[str, Any] = {}
    monkeypatch.setattr(
        cli, "bootstrap_endpoints", lambda _settings: (sonarr, shoko), raising=False
    )

    class FakeServer:
        def server_close(self) -> None:
            invoked["closed"] = True

    server = FakeServer()
    monkeypatch.setattr(
        cli,
        "create_dashboard_server",
        lambda host, port, app: invoked.update(host=host, port=port, app=app) or server,
    )
    monkeypatch.setattr(
        cli,
        "run_dashboard_worker",
        lambda server_arg, **kwargs: invoked.update(server=server_arg, **kwargs),
    )
    monkeypatch.setattr(cli, "make_signal_handlers", lambda _state: ())

    try:
        assert main(["service"]) == EXIT_OK
    finally:
        sonarr.client.close()
        shoko.client.close()

    assert invoked["host"] == "0.0.0.0"  # noqa: S104
    assert invoked["port"] == 8080
    assert invoked["server"] is server
    assert invoked["worker_interval_seconds"] == SERVICE_POLL_SECONDS
    assert invoked["app"].setup_provider is not None
    assert invoked["app"].setup_action is not None
    assert invoked["closed"] is True


def test_service_worker_never_builds_daemon_dependencies_before_onboarding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SONARR_URL", "http://sonarr:8989")
    monkeypatch.setenv("SONARR_API_KEY", "sonarr-test-key-never-a-real-secret")
    monkeypatch.setenv("SHOKO_URL", "http://shoko:8111")
    monkeypatch.setenv("SHOKO_API_KEY", "shoko-test-key-never-a-real-secret")
    monkeypatch.setattr(
        cli,
        "load_bootstrap_settings",
        lambda: BootstrapSettings(
            "http://sonarr:8989",
            SecretRef("SONARR_API_KEY"),
            "http://shoko:8111",
            SecretRef("SHOKO_API_KEY"),
            tmp_path / "state.sqlite3",
        ),
    )
    sonarr = ApiEndpoint(httpx.Client(), lambda: {})
    shoko = ApiEndpoint(httpx.Client(), lambda: {})
    monkeypatch.setattr(cli, "bootstrap_endpoints", lambda _settings: (sonarr, shoko))
    monkeypatch.setattr(cli, "make_signal_handlers", lambda _state: ())
    monkeypatch.setattr(
        cli,
        "probe_connections",
        lambda *_: ConnectionReport(True, True, ()),
    )

    class FakeController:
        def __init__(self, *_args: Any) -> None:
            pass

        def current_context(self, _store: Any) -> None:
            return None

        def view(self) -> None:
            return None

        def action(self, _action: str, _form: Any) -> str:
            return "Complete setup first."

    monkeypatch.setattr(cli, "ServiceSetupController", FakeController)

    class FakeServer:
        def server_close(self) -> None:
            return None

    server = FakeServer()
    invoked: dict[str, Any] = {}
    monkeypatch.setattr(
        cli,
        "create_dashboard_server",
        lambda _host, _port, app: invoked.update(app=app) or server,
    )

    def worker_loop(_server: Any, **kwargs: Any) -> None:
        kwargs["worker_tick"]()
        invoked["status"] = kwargs

    monkeypatch.setattr(cli, "run_dashboard_worker", worker_loop)
    monkeypatch.setattr(
        cli,
        "build_daemon_deps",
        lambda *_args, **_kwargs: pytest.fail("daemon must remain unconfigured before consent"),
    )

    try:
        assert main(["service"]) == EXIT_OK
        message = invoked["app"].status_provider().next_action
        assert "Complete or review scope and safety setup" in message
    finally:
        sonarr.client.close()
        shoko.client.close()


def test_help_exits_zero(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["--help"])
    assert excinfo.value.code == EXIT_OK
    text = capsys.readouterr().out
    assert "validate" in text
    assert "mappings" in text
    assert "SHOKO_LINKER_ALLOW_APPLY" in text


def test_validate_offline_accepts_example_config(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["validate", "--offline", str(EXAMPLE_CONFIG)])
    out = capsys.readouterr().out
    assert code == EXIT_OK
    assert "Configuration is valid" in out
    assert DUMMY_SHOKO_KEY not in out


def test_validate_refuses_live_read_only_without_network(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code = main(["validate", "--offline", "--live-read-only", str(EXAMPLE_CONFIG)])
    out = capsys.readouterr().out
    assert code == EXIT_RUNTIME
    assert "not implemented" in out
    assert "no network call was made" in out


def test_validate_offline_is_the_default(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["validate", str(EXAMPLE_CONFIG)]) == EXIT_OK


def test_validate_reports_invalid_config(
    base_document: dict[str, Any],
    write_document: Callable[[dict[str, Any]], Path],
    capsys: pytest.CaptureFixture[str],
) -> None:
    base_document["shoko"]["allowed_roots"] = []
    code = main(["validate", str(write_document(base_document))])
    captured = capsys.readouterr()
    assert code == EXIT_CONFIG_INVALID
    assert "config invalid" in captured.err
    assert "shoko.allowed_roots" in captured.err
    assert DUMMY_SHOKO_KEY not in captured.err


def test_validate_reports_missing_secret(
    base_document: dict[str, Any],
    write_document: Callable[[dict[str, Any]], Path],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("SHOKO_API_KEY")
    code = main(["validate", str(write_document(base_document))])
    captured = capsys.readouterr()
    assert code == EXIT_CONFIG_INVALID
    assert "SHOKO_API_KEY" in captured.err
    assert DUMMY_SHOKO_KEY not in captured.err


def test_once_apply_without_operator_opt_in_refuses(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code = main(["once", "--apply", str(EXAMPLE_CONFIG)])
    out = capsys.readouterr().out
    assert code == EXIT_APPLY_GATE_REFUSAL
    assert "REFUSED" in out
    assert "SHOKO_LINKER_ALLOW_APPLY" in out
    assert DUMMY_SHOKO_KEY not in out


def test_once_apply_with_config_default_refuses(
    base_document: dict[str, Any],
    write_document: Callable[[dict[str, Any]], Path],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    # Operator opt-in present, config still says dry-run: the gate stays shut.
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    code = main(["once", "--apply", str(write_document(base_document))])
    out = capsys.readouterr().out
    assert code == EXIT_APPLY_GATE_REFUSAL
    assert "sync.apply_changes" in out


def test_once_refuses_an_unknown_policy_name(
    base_document: dict[str, Any],
    write_document: Callable[[dict[str, Any]], Path],
) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["once", "--policy", "not-a-policy", str(write_document(base_document))])
    assert excinfo.value.code == EXIT_CONFIG_INVALID


def test_config_path_command_resolves(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    code = main(["config", str(tmp_path / "cfg.yaml")])
    out = capsys.readouterr().out
    assert code == EXIT_OK
    assert "config path:" in out
    assert "not requested" in out


def test_config_path_check_validates(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code = main(["config", str(EXAMPLE_CONFIG), "--check"])
    out = capsys.readouterr().out
    assert code == EXIT_OK
    assert "sonarr: http://sonarr:8989" in out
    assert "live writes=disabled" in out


def test_config_path_check_reports_invalid(
    base_document: dict[str, Any],
    write_document: Callable[[dict[str, Any]], Path],
    capsys: pytest.CaptureFixture[str],
) -> None:
    del base_document["database"]
    code = main(["config", str(write_document(base_document)), "--check"])
    captured = capsys.readouterr()
    assert code == EXIT_CONFIG_INVALID
    assert "database: section is required" in captured.err


def test_main_returns_int_for_a_valid_run() -> None:
    assert main(["validate", str(EXAMPLE_CONFIG)]) == EXIT_OK


def test_stop_handler_sets_flag_and_wakes_event(monkeypatch: pytest.MonkeyPatch) -> None:
    """F6: the async-signal-safe handler marks stopped AND sets the shutdown event so an
    interruptible sleep wakes promptly."""
    monkeypatch.setattr(cli, "_SHUTDOWN_WAKE", threading.Event())
    state = cli.StopState()
    assert not state.should_stop()
    assert not state.shutdown_event.is_set()
    handler = cli._stop_handler(state)
    handler(int(signal.SIGINT), None)
    assert state.should_stop()
    assert state.shutdown_event.is_set()
    assert cli._SHUTDOWN_WAKE.is_set()


def test_make_signal_handlers_installs_sigint_that_wakes_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F6: the SIGINT (Ctrl+C) path — installed handler sets the flag and the event."""
    monkeypatch.setattr(cli, "_SHUTDOWN_WAKE", threading.Event())
    state = cli.StopState()
    original = signal.getsignal(signal.SIGINT)
    try:
        installed = cli.make_signal_handlers(state)
        assert int(signal.SIGINT) in installed
        assert not state.shutdown_event.is_set()
        handler = signal.getsignal(signal.SIGINT)
        assert callable(handler)
        handler(int(signal.SIGINT), None)
        assert state.should_stop()
        assert state.shutdown_event.is_set()
        assert cli._SHUTDOWN_WAKE.is_set()
    finally:
        signal.signal(signal.SIGINT, original)


def test_in_cycle_sleep_pacing_survives_shutdown_event(monkeypatch: pytest.MonkeyPatch) -> None:
    """F6 review: the shutdown event targets ONLY the inter-cycle sleep. In-cycle
    ``sleep`` (advance seam for verification polls) keeps its plain pacing after a wake,
    so a shutdown mid-verification never turns remaining polls into a GET burst."""
    wake = threading.Event()
    wake.set()
    monkeypatch.setattr(cli, "_SHUTDOWN_WAKE", wake)
    calls: list[float] = []
    monkeypatch.setattr(time, "sleep", lambda seconds: calls.append(seconds))
    cli.sleep(5.0)
    assert calls == [5.0]


def test_interruptible_sleep_wakes_immediately_when_event_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F6: a pre-set shutdown event short-circuits the interval — no wall-clock wait."""
    wake = threading.Event()
    wake.set()
    monkeypatch.setattr(cli, "_SHUTDOWN_WAKE", wake)
    started = time.monotonic()
    cli.interruptible_sleep(3600.0)
    assert time.monotonic() - started < 1.0
