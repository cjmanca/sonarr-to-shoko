"""Root logging setup with a redaction filter.

Secret values (API keys) are registered when a client is created and scrubbed from
every formatted log record before it reaches a handler, so a value cannot reach output
even if a caller formats it into a message. This package's own log records name
configuration only (environment-variable names), never key values; the filter is the
backstop that makes the values unprintable.

Redaction happens at TWO layers, and embedders must replicate both on every handler:

* :class:`RedactionFilter` scrubs the formatted *message* (``record.getMessage()``,
  which includes ``%``-interpolation of arguments).
* :class:`RedactionFormatter` scrubs the **fully formatted output**, which is
  where ``exc_info`` tracebacks (``record.exc_text``) and ``stack_info`` are appended
  by the logging machinery *after* filters have run. Exception reprs are a foreseeable
  leak path (M2+ logs httpx errors whose reprs contain request URLs), so a
  filter alone is not sufficient.

Design rules for later milestones:

* ``redact()`` is a literal-substring scrubber. A secret transformed before logging
  (truncated, re-encoded) is not caught. Request logging must **never** emit raw keys;
  authentication uses headers, so keys stay out of URLs, and
  full URLs must be redacted before being logged.
* Registered values shorter than :data:`MIN_SECRET_LENGTH` are not scrubbed (scrubbing
  short strings would erase ordinary words). API keys in Sonarr/Shoko are long;
  short values are rejected at registration, a warning naming only the length is
  logged, and they must never be logged themselves.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from threading import Lock
from typing import Final, TextIO

REDACTED: Final[str] = "***REDACTED***"

# Values shorter than this would scrub ordinary words from messages; real API
# keys are long, and short values are rejected for registration (never logged).
MIN_SECRET_LENGTH: Final[int] = 8

_secrets: set[str] = set()
_secrets_lock = Lock()


def register_secret(value: str) -> bool:
    """Register a live secret value so the redaction filter scrubs it from output.

    Returns ``False`` when the value is below :data:`MIN_SECRET_LENGTH` (scrubbing
    short strings would erase ordinary words); such values are never registered.
    """
    if len(value) < MIN_SECRET_LENGTH:
        return False
    with _secrets_lock:
        _secrets.add(value)
    return True


def redact(text: str) -> str:
    """Return ``text`` with every registered secret value replaced by a marker."""
    with _secrets_lock:
        values = sorted(_secrets, key=len, reverse=True)
    for value in values:
        if value in text:
            text = text.replace(value, REDACTED)
    return text


def diagnostic_text(text: str, *, limit: int = 512) -> str:
    """Bound untrusted diagnostic metadata after redacting secrets and URL payloads."""
    text = redact(text)
    text = re.sub(r"(?i)https?://\S+", "[URL omitted]", text)
    text = "".join(" " if unicodedata.category(char).startswith("C") else char for char in text)
    return text if len(text) <= limit else text[: limit - 1] + "…"


def registered_secret_count() -> int:
    """How many secrets are registered. Values are never exposed by this function."""
    with _secrets_lock:
        return len(_secrets)


def clear_registered_secrets() -> None:
    """Drop all registered secrets (used by tests and process teardown)."""
    with _secrets_lock:
        _secrets.clear()


class RedactionFilter(logging.Filter):
    """Scrub registered secrets from the formatted message of every record."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = redact(record.getMessage())
        record.args = ()
        return True


class RedactionFormatter(logging.Formatter):
    """Formatter that redacts the FINAL formatted output.

    ``logging`` appends ``exc_text`` (tracebacks) and ``stack_info`` inside
    ``Formatter.format``, after handler filters have run, so message-level redaction
    alone leaks secrets embedded in exceptions. Scrubbing the complete formatted
    string is the only reliable layer for ``exc_info=True`` logging.
    """

    def format(self, record: logging.LogRecord) -> str:
        return redact(super().format(record))


def configure_logging(
    level: int = logging.INFO,
    *,
    stream: TextIO | None = None,
) -> logging.Logger:
    """Install a single, redacting stream handler on the root logger.

    Both layers are attached to the **handler**: `logging` applies logger-level filters
    only to records logged on that exact logger (propagated records need handler
    filters), and tracebacks/``stack_info`` are appended by the formatter after
    filters, so the formatter must redact too. Any handler installed by an embedder
    (JSON handlers, test capture handlers, …) must get its own :class:`RedactionFilter`
    AND :class:`RedactionFormatter` — do the same in every logging setup path, because
    an unfiltered handler leaks registered values.
    """
    root = logging.getLogger()
    root.setLevel(level)
    handler = logging.StreamHandler(stream)
    handler.setLevel(level)
    handler.setFormatter(RedactionFormatter("%(asctime)s %(levelname)-8s %(name)s: %(message)s"))
    handler.addFilter(RedactionFilter())
    for existing in list(root.handlers):
        root.removeHandler(existing)
    root.addHandler(handler)
    return root
