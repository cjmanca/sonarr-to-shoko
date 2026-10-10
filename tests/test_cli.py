"""CLI behavior: exit codes, refusal messages, stub honesty. Offline only, no network."""

from __future__ import annotations

import signal
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from shoko_linker import cli
from shoko_linker.cli import (
    EXIT_APPLY_GATE_REFUSAL,
    EXIT_CONFIG_INVALID,
    EXIT_OK,
    EXIT_RUNTIME,
    main,
)

EXAMPLE_CONFIG = Path(__file__).resolve().parents[1] / "config.example.yaml"
DUMMY_SHOKO_KEY = "dummy-shoko-key-value-00000000"


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
