"""Redaction behavior: registered secret values never reach log output. Offline only."""

from __future__ import annotations

import io
import logging
from collections.abc import Iterator
from pathlib import Path

import pytest

from shoko_linker import logging_setup
from shoko_linker.config import load_settings
from shoko_linker.http import make_sonarr_client
from shoko_linker.logging_setup import REDACTED, RedactionFilter, configure_logging

API_KEY_VALUE = "super-secret-api-key-value-9999"
DUMMY_SONARR_KEY = "dummy-sonarr-key-value-00000000"
EXAMPLE_CONFIG = Path(__file__).resolve().parents[1] / "config.example.yaml"


@pytest.fixture
def logging_stream() -> Iterator[io.StringIO]:
    """Root logger writing to a captured stream, restored afterwards."""
    saved_handlers = list(logging.root.handlers)
    saved_filters = list(logging.root.filters)
    stream = io.StringIO()
    configure_logging(logging.INFO, stream=stream)
    yield stream
    logging.root.handlers[:] = saved_handlers
    logging.root.filters[:] = saved_filters
    logging_setup.clear_registered_secrets()


def test_secret_value_is_scrubbed_from_formatted_output(logging_stream: io.StringIO) -> None:
    logging_setup.register_secret(API_KEY_VALUE)
    logging.getLogger("shoko_linker.test").info("link request failed with key %s", API_KEY_VALUE)
    output = logging_stream.getvalue()
    assert API_KEY_VALUE not in output
    assert REDACTED in output
    assert "link request failed with key" in output


def test_secret_inside_interpolated_message_is_scrubbed(logging_stream: io.StringIO) -> None:
    logging_setup.register_secret(API_KEY_VALUE)
    message = f"retrying after error: token={API_KEY_VALUE}"
    logging.getLogger("shoko_linker.test").warning(message)
    output = logging_stream.getvalue()
    assert API_KEY_VALUE not in output
    assert REDACTED in output


def test_environment_variable_names_are_not_scrubbed(logging_stream: io.StringIO) -> None:
    # Names are configuration, not secrets; only values are scrubbed.
    logging_setup.register_secret(API_KEY_VALUE)
    logging.getLogger("shoko_linker.test").info("set SHOKO_API_KEY in the environment")
    output = logging_stream.getvalue()
    assert "SHOKO_API_KEY" in output
    assert API_KEY_VALUE not in output


def test_client_creation_registers_the_key_for_redaction(logging_stream: io.StringIO) -> None:
    settings = load_settings(EXAMPLE_CONFIG)
    endpoint = make_sonarr_client(settings)
    assert logging_setup.registered_secret_count() == 1
    api_key = settings.sonarr.api_key.resolve()
    logging.getLogger("shoko_linker.test").info("sonarr request failed with key %s", api_key)
    output = logging_stream.getvalue()
    assert DUMMY_SONARR_KEY not in output
    assert REDACTED in output
    endpoint.client.close()


def test_exception_traceback_is_redacted(logging_stream: io.StringIO) -> None:
    # P1 regression: exc_text is appended by the formatter AFTER filters run; only
    # RedactionFormatter scrubs it. A secret inside an exception must never reach
    # the traceback in output.
    logging_setup.register_secret(API_KEY_VALUE)

    def _boom() -> None:
        raise RuntimeError(f"request to /api/v3/File failed; key was {API_KEY_VALUE}")

    logger = logging.getLogger("shoko_linker.test")
    try:
        _boom()
    except RuntimeError:
        logger.exception("link request failed")
    output = logging_stream.getvalue()
    assert API_KEY_VALUE not in output
    assert REDACTED in output
    assert "RuntimeError" in output


def test_exception_args_are_redacted(logging_stream: io.StringIO) -> None:
    logging_setup.register_secret(API_KEY_VALUE)
    exc = ValueError(API_KEY_VALUE)
    logging.getLogger("shoko_linker.test").error("validation failed: %s", exc)
    output = logging_stream.getvalue()
    assert API_KEY_VALUE not in output
    assert REDACTED in output


def test_redaction_formatter_scrubs_final_string() -> None:
    # Direct formatter-level check (the layer filters cannot reach).
    logging_setup.register_secret(API_KEY_VALUE)
    formatter = logging_setup.RedactionFormatter("%(message)s")
    record = logging.LogRecord(
        name="t",
        level=logging.ERROR,
        pathname=__file__,
        lineno=1,
        msg="header apikey=%s",
        args=(API_KEY_VALUE,),
        exc_info=None,
    )
    assert API_KEY_VALUE not in formatter.format(record)
    assert REDACTED in formatter.format(record)


def test_short_values_are_not_registered() -> None:
    assert logging_setup.register_secret("ab") is False
    assert logging_setup.registered_secret_count() == 0
    assert logging_setup.register_secret(API_KEY_VALUE) is True
    assert logging_setup.registered_secret_count() == 1


def test_redact_helper_scrubs_any_text() -> None:
    logging_setup.register_secret(API_KEY_VALUE)
    assert logging_setup.redact(f"header: {API_KEY_VALUE}") == f"header: {REDACTED}"


def test_filter_scrubs_records_captured_by_pytest(caplog: pytest.LogCaptureFixture) -> None:
    # Redaction lives on handlers, so an externally installed capture handler
    # needs the filter too. This is the documented requirement for every logging path.
    saved = [(handler, list(handler.filters)) for handler in logging.root.handlers]
    logging_setup.register_secret(API_KEY_VALUE)
    caplog.set_level(logging.INFO)
    for handler in logging.root.handlers:
        handler.addFilter(RedactionFilter())
    try:
        logging.getLogger("shoko_linker.test").info("value in message: %s", API_KEY_VALUE)
    finally:
        for handler, filters in saved:
            handler.filters[:] = filters
    assert API_KEY_VALUE not in caplog.text
    assert REDACTED in caplog.text
