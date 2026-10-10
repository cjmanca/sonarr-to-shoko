"""Offline tests for scripts/validate_historical.py (M8 F3).

Synthetic historical trees in tmp_path — no git, no network, no Docker.
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "validate_historical.py"


def _load_module() -> Any:
    spec = importlib.util.spec_from_file_location("validate_historical", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


validator = _load_module()


def _synthetic_commit(
    tmp_path: Path, *, with_dockerfile: bool = True, with_config: bool = True
) -> Path:
    tree = tmp_path / "commit"
    tree.mkdir()
    if with_dockerfile:
        (tree / "Dockerfile").write_text("FROM python:3.12-slim\n", encoding="utf-8")
    (tree / "pyproject.toml").write_text(
        '[project]\nname = "shoko-sonarr-linker"\n', encoding="utf-8"
    )
    (tree / "uv.lock").write_text(
        'version = 1\n\n[[package]]\nname = "shoko-sonarr-linker"\n', encoding="utf-8"
    )
    src = tree / "src" / "shoko_linker"
    src.mkdir(parents=True)
    (src / "__init__.py").write_text("VALUE = 1\n", encoding="utf-8")
    tests = tree / "tests"
    tests.mkdir()
    (tests / "test_smoke.py").write_text(
        "def test_smoke() -> None:\n    assert True\n", encoding="utf-8"
    )
    if with_config:
        (tree / "config.example.yaml").write_text(
            "sync:\n  apply_changes: false\n", encoding="utf-8"
        )
    return tree


def test_synthetic_historical_tree_passes(tmp_path: Path) -> None:
    tree = _synthetic_commit(tmp_path)
    report = validator.validate_historical(tree)
    assert report["ok"] is True
    assert report["failures"] == []
    joined = "\n".join(report["checks"])
    assert "Dockerfile present" in joined
    assert "pyproject.toml present" in joined
    assert "uv.lock present" in joined
    assert "compileall src/" in joined
    assert "compileall tests/" in joined
    assert "config.example.yaml parses" in joined
    assert "scripts/ absent" in joined


def test_missing_dockerfile_fails(tmp_path: Path) -> None:
    tree = _synthetic_commit(tmp_path, with_dockerfile=False)
    report = validator.validate_historical(tree)
    assert report["ok"] is False
    assert any("Dockerfile" in failure for failure in report["failures"])


def test_missing_lock_and_source_fail(tmp_path: Path) -> None:
    tree = _synthetic_commit(tmp_path, with_config=False)
    (tree / "uv.lock").unlink()
    report = validator.validate_historical(tree)
    assert report["ok"] is False
    assert any("uv.lock" in failure for failure in report["failures"])
    assert any("config.example.yaml absent" in check for check in report["checks"])


def test_broken_source_fails_compileall(tmp_path: Path) -> None:
    tree = _synthetic_commit(tmp_path)
    (tree / "src" / "shoko_linker" / "broken.py").write_text("def f(:\n", encoding="utf-8")
    report = validator.validate_historical(tree)
    assert report["ok"] is False
    assert any("compileall src/" in failure for failure in report["failures"])


def test_malformed_config_fails(tmp_path: Path) -> None:
    tree = _synthetic_commit(tmp_path)
    (tree / "config.example.yaml").write_text("only: a: string: mapping: [\n", encoding="utf-8")
    report = validator.validate_historical(tree)
    assert report["ok"] is False
    assert any("config.example.yaml" in failure for failure in report["failures"])


def test_scripts_dir_tolerated_and_compiled(tmp_path: Path) -> None:
    tree = _synthetic_commit(tmp_path)
    scripts = tree / "scripts"
    scripts.mkdir()
    (scripts / "tool.py").write_text("print('ok')\n", encoding="utf-8")
    report = validator.validate_historical(tree)
    assert report["ok"] is True
    assert any("compileall scripts/" in check for check in report["checks"])


def test_cli_exit_codes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tree = _synthetic_commit(tmp_path)
    out = io.StringIO()
    monkeypatch.setattr(sys, "stdout", out)
    assert validator.main(["--repo", str(tree)]) == 0
    assert json.loads(out.getvalue())["ok"] is True

    (tree / "Dockerfile").unlink()
    out2 = io.StringIO()
    err = io.StringIO()
    monkeypatch.setattr(sys, "stdout", out2)
    monkeypatch.setattr(sys, "stderr", err)
    assert validator.main(["--repo", str(tree)]) == 1
    assert "Dockerfile" in err.getvalue()
