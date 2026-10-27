"""YAML consumers must never expose source text through parse failures."""

from __future__ import annotations

import traceback
from collections.abc import Callable
from pathlib import Path

import pytest

from shoko_linker.attestation import AttestationError, load_attestation, parse_attestation_document
from shoko_linker.config import parse_settings
from shoko_linker.errors import ConfigError
from shoko_linker.mappings import load_overrides


@pytest.mark.parametrize(
    ("parse", "error", "prefix"),
    [
        (parse_settings, ConfigError, "configuration is not valid YAML"),
        (load_overrides, ConfigError, "overrides file is not valid YAML"),
        (parse_attestation_document, AttestationError, "attestation file is not valid YAML/JSON"),
    ],
    ids=["configuration", "overrides", "attestation"],
)
@pytest.mark.parametrize(
    ("source", "detail"),
    [
        ("value: [http://user:synthetic-password@host\n", "ParserError at line 2, column 1"),
        ("value: !synthetic-password example\n", "ConstructorError at line 1, column 8"),
        ("value: *synthetic-password\n", "ComposerError at line 1, column 8"),
        ("value: synthetic-password\x00\n", "ReaderError"),
    ],
    ids=["snippet", "tag", "alias", "reader"],
)
def test_yaml_errors_are_source_free(
    parse: Callable[[str], object], error: type[Exception], prefix: str, source: str, detail: str
) -> None:
    with pytest.raises(error, match=prefix) as caught:
        parse(source)

    assert "synthetic-password" not in str(caught.value)
    assert "synthetic-password" not in "".join(traceback.format_exception(caught.value))
    assert caught.value.__cause__ is None
    assert caught.value.__suppress_context__ is True
    assert str(caught.value) == f"{prefix}: {detail}"


def test_invalid_attestation_verdict_does_not_expose_yaml_source(tmp_path: Path) -> None:
    path = tmp_path / "lease.yaml"
    path.write_text("endpoint: !synthetic-password example\n", encoding="utf-8")

    verdict = load_attestation(
        path, shoko_url="http://shoko:8111", now_iso="2026-01-01T00:00:00+00:00", scope="daemon"
    )

    assert verdict.status == "invalid"
    assert not verdict.valid_for_apply
    assert "synthetic-password" not in verdict.reason
    assert verdict.reason == (
        "attestation file is not valid YAML/JSON: ConstructorError at line 1, column 11"
    )
