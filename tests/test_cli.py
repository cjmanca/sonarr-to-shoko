"""CLI behavior: exit codes, refusal messages, stub honesty. Offline only, no network."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from shoko_linker.cli import (
    EXIT_APPLY_GATE_REFUSAL,
    EXIT_CONFIG_INVALID,
    EXIT_NOT_IMPLEMENTED,
    EXIT_OK,
    main,
)

EXAMPLE_CONFIG = Path(__file__).resolve().parents[1] / "config.example.yaml"
DUMMY_SHOKO_KEY = "dummy-shoko-key-value-00000000"
STUB_COMMAND_COUNT = 3


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
    assert code == EXIT_NOT_IMPLEMENTED
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


def test_once_defaults_to_dry_run(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["once", str(EXAMPLE_CONFIG)])
    out = capsys.readouterr().out
    assert code == EXIT_NOT_IMPLEMENTED
    assert "dry-run" in out
    assert "not implemented" in out


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


def test_once_apply_opens_gate_then_stops_before_writes(
    base_document: dict[str, Any],
    write_document: Callable[[dict[str, Any]], Path],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    base_document["sync"]["apply_changes"] = True
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    code = main(["once", "--apply", str(write_document(base_document))])
    out = capsys.readouterr().out
    # Gate opened, and the build has no write path: the engine stub refuses.
    assert code == EXIT_NOT_IMPLEMENTED
    assert "live writes=ALLOWED" in out
    assert "not implemented" in out


def test_once_reports_the_selected_policy(
    base_document: dict[str, Any],
    write_document: Callable[[dict[str, Any]], Path],
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = str(write_document(base_document))
    assert main(["once", "--policy", "new_arrival", path]) == EXIT_NOT_IMPLEMENTED
    assert "policy: new arrival" in capsys.readouterr().out


def test_once_refuses_an_unknown_policy_name(
    base_document: dict[str, Any],
    write_document: Callable[[dict[str, Any]], Path],
) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["once", "--policy", "not-a-policy", str(write_document(base_document))])
    assert excinfo.value.code == EXIT_CONFIG_INVALID


def test_daemon_stub_prints_policy_state(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["daemon", str(EXAMPLE_CONFIG)])
    out = capsys.readouterr().out
    assert code == EXIT_NOT_IMPLEMENTED
    assert "requested policy: new arrival" in out
    assert "sonarr: http://sonarr:8989" in out


def test_report_and_mapping_commands_are_honest_stubs(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["report", str(EXAMPLE_CONFIG)]) == EXIT_NOT_IMPLEMENTED
    assert main(["mappings", "status", str(EXAMPLE_CONFIG)]) == EXIT_NOT_IMPLEMENTED
    assert main(["mappings", "refresh", "--force", str(EXAMPLE_CONFIG)]) == EXIT_NOT_IMPLEMENTED
    out = capsys.readouterr().out
    assert "anime-lists" in out
    assert "refresh interval: 168 hours" in out
    assert out.count("not implemented") == STUB_COMMAND_COUNT


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
