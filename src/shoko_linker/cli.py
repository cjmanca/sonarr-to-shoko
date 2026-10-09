"""Command line interface.

``main(argv)`` returns an exit code; it never raises for expected failures.

Exit codes:
    0  success
    2  configuration/request invalid (message names the failed field, never a secret value)
    3  apply-gate refusal (live writes requested without the default-deny gate opening)
    4  command not implemented in this build (no network calls, no writes)
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Final, cast

from .config import (
    APPLY_ENV_VAR,
    DEFAULT_CONFIG_PATH,
    Settings,
    apply_gate_status,
    load_settings,
)
from .errors import ConfigError, LinkerError, PolicyError
from .logging_setup import configure_logging
from .policies import PolicyRequest, RunPolicy, parse_policy

type CommandHandler = Callable[[argparse.Namespace], int]

EXIT_OK: Final[int] = 0
EXIT_CONFIG_INVALID: Final[int] = 2
EXIT_APPLY_GATE_REFUSAL: Final[int] = 3
EXIT_NOT_IMPLEMENTED: Final[int] = 4

_POLICY_CHOICES: Final[list[str]] = [policy.value for policy in RunPolicy]


def build_parser() -> argparse.ArgumentParser:
    """Construct the full command tree (all subcommands are wired, most are stubs)."""
    parser = argparse.ArgumentParser(
        prog="shoko-sonarr-linker",
        description=(
            "Link unrecognized Shoko files to Shoko episodes using Sonarr episode metadata "
            "and Anime-Lists mappings."
        ),
        epilog=(
            "Safety defaults: dry-run only. Live writes require BOTH 'sync.apply_changes: true' "
            f"in the config AND the operator opt-in {APPLY_ENV_VAR}=1 in the environment; no "
            "command-line flag enables writes by itself.\n"
            "Exit codes: 0 ok, 2 config invalid, 3 apply-gate refusal, 4 not implemented in "
            "this build."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="debug-level logging")
    subparsers = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")
    _add_validate(subparsers)
    _add_once(subparsers)
    _add_daemon(subparsers)
    _add_report(subparsers)
    _add_mappings(subparsers)
    _add_config(subparsers)
    return parser


def _add_validate(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("validate", help="Validate a configuration file (offline).")
    _add_config_argument(parser)
    parser.add_argument(
        "--offline",
        action="store_true",
        default=True,
        help="validate without network access (default)",
    )
    parser.add_argument(
        "--live-read-only",
        action="store_true",
        help="NOT IMPLEMENTED in this build: refuses immediately and makes no network call",
    )


def _add_once(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("once", help="Run one reconciliation pass (dry-run by default).")
    _add_config_argument(parser)
    parser.add_argument(
        "--dry-run", action="store_true", help="report proposals only (the default)"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help=f"request live writes; allowed only by the gate ({APPLY_ENV_VAR}=1 + config opt-in)",
    )
    _add_policy_argument(parser, RunPolicy.BACKLOG)


def _add_daemon(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("daemon", help="NOT IMPLEMENTED: prints policy state and exits.")
    _add_config_argument(parser)
    _add_policy_argument(parser, RunPolicy.NEW_ARRIVAL)


def _add_report(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("report", help="NOT IMPLEMENTED in this build.")
    _add_config_argument(parser)


def _add_mappings(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("mappings", help="Anime-Lists mapping cache commands.")
    mapping_sub = parser.add_subparsers(
        dest="mappings_command", required=True, metavar="SUBCOMMAND"
    )
    status = mapping_sub.add_parser(
        "status", help="NOT IMPLEMENTED: prints configured mapping source only."
    )
    _add_config_argument(status)
    refresh = mapping_sub.add_parser("refresh", help="NOT IMPLEMENTED in this build.")
    _add_config_argument(refresh)
    refresh.add_argument(
        "--force", action="store_true", help="ignored in this build (no fetch implemented)"
    )


def _add_config(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    parser = subparsers.add_parser("config", help="Report the resolved configuration file path.")
    parser.add_argument(
        "path",
        nargs="?",
        default=DEFAULT_CONFIG_PATH,
        help=f"configuration file (default: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument("--check", action="store_true", help="load and validate the file")


def _add_config_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "config",
        nargs="?",
        default=DEFAULT_CONFIG_PATH,
        help=f"configuration file (default: {DEFAULT_CONFIG_PATH})",
    )


def _add_policy_argument(parser: argparse.ArgumentParser, default: RunPolicy) -> None:
    parser.add_argument(
        "--policy",
        default=default.value,
        choices=_POLICY_CHOICES,
        help=f"run policy for this invocation (default: {default.value})",
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Parse arguments, run the selected command, return its exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if cast(bool, args.verbose):
        configure_logging(logging.DEBUG)
    command = cast(str, args.command)
    handler = _COMMAND_HANDLERS[command]
    try:
        return handler(args)
    except ConfigError as exc:
        return _report_config_invalid(exc)
    except PolicyError as exc:
        print(f"request invalid: {exc}", file=sys.stderr)
        return EXIT_CONFIG_INVALID
    except LinkerError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_CONFIG_INVALID


def _run_validate(args: argparse.Namespace) -> int:
    if cast(bool, args.live_read_only):
        return _not_implemented(
            "validate --live-read-only",
            "live read-only checks arrive in a later milestone; no network call was made",
        )
    settings = load_settings(cast(str, args.config))
    print_config_summary(settings)
    print("Configuration is valid. Offline checks only: no Sonarr or Shoko request was made.")
    return EXIT_OK


def _run_once(args: argparse.Namespace) -> int:
    settings = load_settings(cast(str, args.config))
    request = PolicyRequest(parse_policy(cast(str, args.policy))).validated()
    apply_requested = cast(bool, args.apply)
    mode = "dry-run (no remote changes, no writes)"
    if apply_requested:
        mode = "apply requested"
    print(f"policy: {request.policy.label}")
    print(f"mode: {mode}")
    if apply_requested:
        status = apply_gate_status(settings)
        print(f"apply gate: {status.describe()}")
        if not status.allowed:
            print(
                "REFUSED: live writes stay disabled. Enabling them requires "
                f"'sync.apply_changes: true' in the config AND {APPLY_ENV_VAR}=1 set by the "
                "operator. Nothing was sent to Sonarr or Shoko."
            )
            return EXIT_APPLY_GATE_REFUSAL
    return _not_implemented(
        "once", "the reconciliation/linking engine arrives in a later milestone"
    )


def _run_daemon(args: argparse.Namespace) -> int:
    settings = load_settings(cast(str, args.config))
    request = PolicyRequest(parse_policy(cast(str, args.policy))).validated()
    print_config_summary(settings)
    print(f"requested policy: {request.policy.label}")
    return _not_implemented("daemon", "the long-running loop arrives in a later milestone")


def _run_report(args: argparse.Namespace) -> int:
    settings = load_settings(cast(str, args.config))
    print(f"database: {settings.database.path}")
    return _not_implemented("report", "reports need the sync state written by later milestones")


def _run_mappings(args: argparse.Namespace) -> int:
    settings = load_settings(cast(str, args.config))
    command = cast(str, args.mappings_command)
    print(f"configured mapping source: {settings.mappings.source}")
    print(f"configured refresh interval: {settings.mappings.refresh_hours:g} hours")
    print(f"configured overrides file: {settings.mappings.overrides_file or '<none>'}")
    return _not_implemented(
        f"mappings {command}",
        "the mapping cache, downloads and overrides loading arrive in later milestones",
    )


def _run_config(args: argparse.Namespace) -> int:
    resolved = Path(cast(str, args.path)).expanduser().resolve()
    print(f"config path: {resolved}")
    if not cast(bool, args.check):
        print("check: not requested (pass --check to load and validate the file)")
        return EXIT_OK
    settings = load_settings(resolved)
    print_config_summary(settings)
    return EXIT_OK


def print_config_summary(settings: Settings) -> None:
    """Human-readable configuration summary. Secret values are never included."""
    print(
        f"config file: {settings.config_path if settings.config_path is not None else '<string>'}"
    )
    print(f"  sonarr: {settings.sonarr.url}")
    _print_roots(settings.sonarr.allowed_roots)
    print(f"  shoko: {settings.shoko.url}")
    _print_roots(settings.shoko.allowed_roots)
    print(f"  path mappings ({len(settings.path_mappings)}):")
    for mapping in settings.path_mappings:
        print(f"    {mapping.sonarr} -> {mapping.shoko}")
    print(
        f"  mappings: source={settings.mappings.source}, "
        f"refresh_hours={settings.mappings.refresh_hours:g}, "
        f"overrides={settings.mappings.overrides_file or '<none>'}"
    )
    sync = settings.sync
    print(
        f"  sync: interval_seconds={sync.interval_seconds:g}, "
        f"full_reconcile_interval_hours={sync.full_reconcile_interval_hours:g}, "
        f"new_file_grace_period_seconds={sync.new_file_grace_period_seconds:g}, "
        f"max_concurrent_links={sync.max_concurrent_links}"
    )
    print(
        f"  timeouts: connect={sync.connect_timeout_seconds:g}s, "
        f"read={sync.read_timeout_seconds:g}s"
    )
    print(f"  database: {settings.database.path}")
    print(
        f"  api keys: read from the environment at access time "
        f"({', '.join(settings.secret_env_vars)}); values are never stored, printed, or logged"
    )
    print(f"  writes: {apply_gate_status(settings).describe()}")


def _print_roots(roots: Sequence[str]) -> None:
    print(f"    allowed_roots ({len(roots)}): {', '.join(roots)}")


def _report_config_invalid(exc: ConfigError) -> int:
    print(f"config invalid: {exc}", file=sys.stderr)
    return EXIT_CONFIG_INVALID


def _not_implemented(command: str, reason: str) -> int:
    print(f"{command}: not implemented — {reason}")
    return EXIT_NOT_IMPLEMENTED


_COMMAND_HANDLERS: Final[dict[str, CommandHandler]] = {
    "validate": _run_validate,
    "once": _run_once,
    "daemon": _run_daemon,
    "report": _run_report,
    "mappings": _run_mappings,
    "config": _run_config,
}


if __name__ == "__main__":
    sys.exit(main())
