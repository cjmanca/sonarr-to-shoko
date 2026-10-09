"""Exception types shared by the package.

Messages are written for operators: they name the offending field or path and never
contain secret values.
"""

from __future__ import annotations


class LinkerError(Exception):
    """Base class for all shoko-sonarr-linker errors."""


class ConfigError(LinkerError):
    """Configuration is missing, malformed, unsafe, or a secret is unavailable."""


class PathError(LinkerError):
    """A path is not a usable POSIX media path, or a mapping set is ambiguous."""


class PolicyError(LinkerError):
    """A run policy request is unknown or violates policy rules."""


class HttpPolicyError(LinkerError):
    """A redirect/transport situation the client policy refuses to handle."""
