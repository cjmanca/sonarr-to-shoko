"""Shared offline fixtures. No test in this suite performs network access."""

from __future__ import annotations

import copy
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml  # type: ignore[import-untyped]

from shoko_linker import logging_setup

REPO_ROOT: Path = Path(__file__).resolve().parents[1]
EXAMPLE_CONFIG: Path = REPO_ROOT / "config.example.yaml"

DUMMY_SONARR_KEY = "dummy-sonarr-key-value-00000000"
DUMMY_SHOKO_KEY = "dummy-shoko-key-value-00000000"

_VALID_DOCUMENT: dict[str, Any] = {
    "sonarr": {
        "url": "http://sonarr:8989",
        "api_key_env": "SONARR_API_KEY",
        "allowed_roots": ["/data/media/anime"],
    },
    "shoko": {
        "url": "http://shoko:8111",
        "api_key_env": "SHOKO_API_KEY",
        "allowed_roots": ["/anime"],
    },
    "paths": {"mappings": [{"sonarr": "/data/media/anime", "shoko": "/anime"}]},
    "mappings": {"source": "anime-lists", "refresh_hours": 168},
    "sync": {
        "interval_seconds": 300,
        "apply_changes": False,
        "max_concurrent_links": 1,
    },
    "database": {"path": "/data/state.sqlite3"},
}


@pytest.fixture(autouse=True)
def _isolated_environment(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Dummy key values present, write opt-in absent, no leaked registrations."""
    monkeypatch.setenv("SONARR_API_KEY", DUMMY_SONARR_KEY)
    monkeypatch.setenv("SHOKO_API_KEY", DUMMY_SHOKO_KEY)
    monkeypatch.delenv("SHOKO_LINKER_ALLOW_APPLY", raising=False)
    logging_setup.clear_registered_secrets()
    yield
    logging_setup.clear_registered_secrets()


@pytest.fixture
def base_document() -> dict[str, Any]:
    """A valid configuration document, safe to mutate per test."""
    return copy.deepcopy(_VALID_DOCUMENT)


@pytest.fixture
def write_text(tmp_path: Path) -> Callable[[str], Path]:
    def _write(text: str) -> Path:
        path = tmp_path / "config.yaml"
        path.write_text(text, encoding="utf-8")
        return path

    return _write


@pytest.fixture
def write_document(
    tmp_path: Path,
) -> Callable[[dict[str, Any]], Path]:
    def _write(document: dict[str, Any]) -> Path:
        path = tmp_path / "config.yaml"
        path.write_text(yaml.safe_dump(document), encoding="utf-8")
        return path

    return _write
