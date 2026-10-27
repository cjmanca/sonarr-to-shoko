"""Source-free details for YAML parse errors at operator-facing boundaries."""

from __future__ import annotations

import yaml  # type: ignore[import-untyped]


def yaml_error_detail(exc: yaml.YAMLError) -> str:
    """Return only the error class and one-based location, never parser/source text.

    Even parser problem descriptions can contain secrets through tags or aliases.
    Callers must also suppress the raw exception chain with ``raise ... from None``.
    """
    detail = type(exc).__name__
    if isinstance(exc, yaml.MarkedYAMLError) and exc.problem_mark is not None:
        mark = exc.problem_mark
        detail += f" at line {mark.line + 1}, column {mark.column + 1}"
    return detail
