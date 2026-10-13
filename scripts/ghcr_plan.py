#!/usr/bin/env python3
"""Normalize GHCR SHA tags and make bounded, provenance-aware publish decisions."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

MAX_SHAS_CAP = 10
VERIFY_CAP = 10
FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
SHA_TAG = re.compile(r"^sha-([0-9a-f]{40})$")
PROVENANCE_STATES = frozenset({"published", "not_published", "unknown_provenance"})


class PlanError(ValueError):
    """Input does not support a safe GHCR publication decision."""


def _require_object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise PlanError(f"{label} must be a JSON object")
    return value


def _validated_sha(value: Any, label: str) -> str:
    if not isinstance(value, str) or FULL_SHA.fullmatch(value) is None:
        raise PlanError(f"{label} must be a 40-character lowercase hexadecimal SHA")
    return value


def _tag_shas(tag_text: str) -> list[str]:
    """Read newline-separated tag names, ignoring empty lines and rejecting malformed sha tags."""
    shas: list[str] = []
    seen: set[str] = set()
    for line in tag_text.splitlines():
        candidate = line.strip()
        if not candidate:
            continue
        if not candidate.lower().startswith("sha-"):
            continue
        match = SHA_TAG.fullmatch(line)
        if match is None:
            raise PlanError(
                f"invalid sha tag {line!r}; expected sha- followed by 40 lowercase hex digits"
            )
        sha = match.group(1)
        if sha not in seen:
            shas.append(sha)
            seen.add(sha)
    return shas


def normalize_tag_lines(tag_text: str) -> set[str]:
    """Return full SHAs from valid SHA tags; a trailing newline is harmless."""
    return set(_tag_shas(tag_text))


def registry_tags_to_text(document: Mapping[str, Any]) -> str:
    """Validate a ghcr_tags.py list result and emit canonical SHA tags for the enumerator."""
    status = document.get("status")
    if status not in ("ok", "package_missing"):
        detail = document.get("detail", "")
        raise PlanError(
            f"registry status {status!r} ({detail}); refusing to plan against unknown state"
        )
    tags = document.get("tags")
    if not isinstance(tags, list) or any(not isinstance(tag, str) for tag in tags):
        raise PlanError("registry tags must be a list of strings")
    if status == "package_missing":
        if tags:
            raise PlanError("package_missing registry response must have an empty tags list")
        return ""
    return "".join(f"sha-{sha}\n" for sha in _tag_shas("\n".join(tags)))


def parse_max_shas(value: str | None) -> int:
    """Parse a positive decimal matrix cap without converting unbounded integers."""
    if value is None or value == "":
        return MAX_SHAS_CAP
    if not isinstance(value, str) or re.fullmatch(r"[0-9]+", value) is None:
        raise PlanError("max_shas must be a positive decimal integer")
    significant = value.lstrip("0")
    if not significant:
        raise PlanError("max_shas must be a positive decimal integer")
    if len(significant) > len(str(MAX_SHAS_CAP)) or int(significant) > MAX_SHAS_CAP:
        return MAX_SHAS_CAP
    return int(significant)


def _max_verify(value: int) -> int:
    if value < 1 or value > VERIFY_CAP:
        raise PlanError(f"max_verify must be between 1 and {VERIFY_CAP}")
    return value


def _rotation(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise PlanError("rotation must be a non-negative integer")
    return value


def _plan_shas(plan: Mapping[str, Any]) -> list[str]:
    raw_shas = plan.get("shas")
    if not isinstance(raw_shas, list):
        raise PlanError("plan.shas must be a list")
    shas = [_validated_sha(sha, "plan SHA") for sha in raw_shas]
    skipped = plan.get("skipped", [])
    if not isinstance(skipped, list):
        raise PlanError("plan.skipped must be a list")
    for item in skipped:
        if not isinstance(item, dict):
            raise PlanError("plan.skipped entries must be objects")
        if item.get("reason") == "already-tagged":
            sha = _validated_sha(item.get("sha"), "already-tagged plan SHA")
            if sha not in shas:
                shas.append(sha)
    return shas


def prepare_plan(
    plan: Mapping[str, Any],
    known_tags_text: str,
    *,
    max_verify: int = VERIFY_CAP,
    rotation: int = 0,
) -> dict[str, Any]:
    """Select a bounded, rotating window of existing candidate tags for verification."""
    cap = _max_verify(max_verify)
    rotation = _rotation(rotation)
    known_shas = normalize_tag_lines(known_tags_text)
    existing = [sha for sha in _plan_shas(plan) if sha in known_shas]
    if existing:
        start = rotation % len(existing)
        window_indices = {
            (start + index) % len(existing) for index in range(min(cap, len(existing)))
        }
        verify = [sha for index, sha in enumerate(existing) if index in window_indices]
        over_cap = [sha for index, sha in enumerate(existing) if index not in window_indices]
    else:
        verify = []
        over_cap = []
    return {
        "verify_shas": verify,
        "over_cap_shas": over_cap,
        "tagged_candidate_shas": existing,
        "rotation": rotation,
    }


def reconciliation_lines(provenance: Mapping[str, Any], shas: Sequence[str]) -> list[str]:
    """Render a parsed verifier response as operator reconciliation guidance."""
    results = provenance.get("results")
    if not isinstance(results, dict):
        results = {}
    lines: list[str] = []
    for raw_sha in shas:
        sha = _validated_sha(raw_sha, "unverified SHA")
        state = results.get(sha, "unknown_provenance")
        if not isinstance(state, str) or state not in PROVENANCE_STATES:
            state = "unknown_provenance"
        lines.append(
            f"- sha-{sha}: registry tag state {state} — reconcile the existing tag "
            "(inspect/pin/delete), then re-run backfill"
        )
    return lines


def _validate_prepared(
    plan: Mapping[str, Any],
    known_tags_text: str,
    prepared: Mapping[str, Any],
    max_verify: int,
    rotation: int,
) -> dict[str, Any]:
    rotation = _rotation(rotation)
    prepared_rotation = prepared.get("rotation")
    if (
        not isinstance(prepared_rotation, int)
        or isinstance(prepared_rotation, bool)
        or prepared_rotation != rotation
    ):
        raise PlanError("prepared verification rotation does not match the requested rotation")
    expected = prepare_plan(plan, known_tags_text, max_verify=max_verify, rotation=rotation)
    if prepared.get("verify_shas") != expected["verify_shas"]:
        raise PlanError(
            "prepared verification list does not match the current plan and registry tags"
        )
    if prepared.get("over_cap_shas") != expected["over_cap_shas"]:
        raise PlanError("prepared over-cap list does not match the current plan and registry tags")
    return expected


def _provenance_states(
    expected: Mapping[str, Any], provenance: Mapping[str, Any]
) -> dict[str, str]:
    if provenance.get("status") != "ok":
        raise PlanError(
            f"provenance verification failed ({provenance.get('status')}: "
            f"{provenance.get('detail', '')})"
        )
    results = provenance.get("results")
    if not isinstance(results, dict):
        raise PlanError("provenance results must be a JSON object")

    states: dict[str, str] = {}
    for sha in expected["verify_shas"]:
        state = results.get(sha)
        states[sha] = (
            state if isinstance(state, str) and state in PROVENANCE_STATES else "unknown_provenance"
        )
    return states


def _apply_provenance(
    plan: Mapping[str, Any],
    expected: Mapping[str, Any],
    states: Mapping[str, str],
) -> tuple[list[str], list[str], list[str], list[str]]:
    original_shas = [_validated_sha(sha, "plan SHA") for sha in plan["shas"]]
    final_shas = list(original_shas)
    absent: list[str] = []
    published: list[str] = []
    unknown: list[str] = []
    over_cap = expected["over_cap_shas"]

    for sha in expected["tagged_candidate_shas"]:
        if sha in over_cap:
            if sha in final_shas:
                final_shas.remove(sha)
            continue
        state = states[sha]
        if state == "published":
            published.append(sha)
            if sha in final_shas:
                final_shas.remove(sha)
        elif state == "not_published":
            absent.append(sha)
        else:
            unknown.append(sha)
            if sha in final_shas:
                final_shas.remove(sha)
    return final_shas, absent, published, unknown


def finalize_plan(
    plan: Mapping[str, Any],
    known_tags_text: str,
    prepared: Mapping[str, Any],
    provenance: Mapping[str, Any],
    *,
    max_verify: int = VERIFY_CAP,
    rotation: int = 0,
) -> dict[str, Any]:
    """Filter published/capped tags and identify absence or reconciliation states."""
    expected = _validate_prepared(plan, known_tags_text, prepared, max_verify, rotation)
    states = _provenance_states(expected, provenance)
    final_shas, absent, published, unknown = _apply_provenance(plan, expected, states)

    final_plan = dict(plan)
    final_plan["shas"] = final_shas
    return {
        "status": "reconciliation_required" if unknown else "ok",
        "plan": final_plan,
        "verify_shas": expected["verify_shas"],
        "published_shas": published,
        "absent_shas": absent,
        "unknown_shas": unknown,
        "over_cap_shas": expected["over_cap_shas"],
        "reconciliation": reconciliation_lines(provenance, unknown),
    }


def promotion_provenance_is_valid(provenance: Mapping[str, Any], sha: str) -> bool:
    """Require a successful verifier result for this exact immutable revision."""
    try:
        full_sha = _validated_sha(sha, "promotion SHA")
    except PlanError:
        return False
    results = provenance.get("results")
    return (
        provenance.get("status") == "ok"
        and isinstance(results, dict)
        and results.get(full_sha) == "published"
    )


def publish_provenance_action(provenance: Mapping[str, Any], sha: str) -> str:
    """Decide whether a locked per-SHA publisher skips or may build/push."""
    full_sha = _validated_sha(sha, "publish SHA")
    if provenance.get("status") != "ok":
        raise PlanError(
            f"provenance verification failed ({provenance.get('status')}: "
            f"{provenance.get('detail', '')})"
        )
    results = provenance.get("results")
    if not isinstance(results, dict):
        raise PlanError("provenance results must be a JSON object")
    if full_sha not in results:
        raise PlanError(f"provenance response omitted expected SHA {full_sha}")
    state = results[full_sha]
    if state == "published":
        return "skip"
    if state == "not_published":
        return "publish"
    if state == "unknown_provenance":
        raise PlanError(f"sha-{full_sha} has unknown provenance; refusing to publish")
    raise PlanError(f"provenance result for {full_sha} is malformed: {state!r}")


def _read_json(path: str) -> dict[str, Any]:
    with Path(path).open(encoding="utf-8") as file:
        return _require_object(json.load(file), path)


def _emit(document: Mapping[str, Any]) -> None:
    json.dump(document, sys.stdout, indent=2)
    sys.stdout.write("\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    tags = commands.add_parser("tags", help="validate registry state and emit canonical sha tags")
    tags.add_argument("--registry", required=True)

    max_shas = commands.add_parser("max-shas", help="safely clamp the publish matrix size")
    max_shas.add_argument("--value", required=True)

    prepare = commands.add_parser("prepare", help="choose bounded provenance checks")
    prepare.add_argument("--plan", required=True)
    prepare.add_argument("--known-tags-file", required=True)
    prepare.add_argument("--max-verify", type=int, default=VERIFY_CAP)
    prepare.add_argument("--rotation", type=int, default=0)

    finalize = commands.add_parser("finalize", help="apply provenance outcomes to the publish plan")
    finalize.add_argument("--plan", required=True)
    finalize.add_argument("--known-tags-file", required=True)
    finalize.add_argument("--prepared", required=True)
    finalize.add_argument("--provenance", required=True)
    finalize.add_argument("--max-verify", type=int, default=VERIFY_CAP)
    finalize.add_argument("--rotation", type=int, default=0)

    absent = commands.add_parser("absent", help="emit SHA values proven absent by verification")
    absent.add_argument("--decision", required=True)

    promotion = commands.add_parser(
        "verify-promotion", help="require verified provenance for one SHA"
    )
    promotion.add_argument("--provenance", required=True)
    promotion.add_argument("--sha", required=True)

    verify_publish = commands.add_parser(
        "verify-publish", help="decide whether a locked SHA leg should skip or publish"
    )
    verify_publish.add_argument("--provenance", required=True)
    verify_publish.add_argument("--sha", required=True)
    return parser


def _run_tags(args: argparse.Namespace) -> int:
    document = _read_json(args.registry)
    sys.stdout.write(registry_tags_to_text(document))
    return 0


def _run_prepare(args: argparse.Namespace) -> int:
    plan = _read_json(args.plan)
    known = Path(args.known_tags_file).read_text(encoding="utf-8")
    _emit(prepare_plan(plan, known, max_verify=args.max_verify, rotation=args.rotation))
    return 0


def _run_max_shas(args: argparse.Namespace) -> int:
    sys.stdout.write(f"{parse_max_shas(args.value)}\n")
    return 0


def _run_finalize(args: argparse.Namespace) -> int:
    plan = _read_json(args.plan)
    known = Path(args.known_tags_file).read_text(encoding="utf-8")
    prepared = _read_json(args.prepared)
    provenance = _read_json(args.provenance)
    _emit(
        finalize_plan(
            plan,
            known,
            prepared,
            provenance,
            max_verify=args.max_verify,
            rotation=args.rotation,
        )
    )
    return 0


def _run_absent(args: argparse.Namespace) -> int:
    decision = _read_json(args.decision)
    if decision.get("status") != "ok":
        raise PlanError("cannot schedule absent tags while reconciliation is required")
    absent_shas = decision.get("absent_shas")
    if not isinstance(absent_shas, list):
        raise PlanError("decision.absent_shas must be a list")
    for sha in absent_shas:
        sys.stdout.write(f"{_validated_sha(sha, 'absent SHA')}\n")
    return 0


def _run_verify_promotion(args: argparse.Namespace) -> int:
    provenance = _read_json(args.provenance)
    if promotion_provenance_is_valid(provenance, args.sha):
        return 0
    print(
        f"error: immutable sha-{args.sha} is missing or lacks verified revision provenance; "
        "refusing to promote latest",
        file=sys.stderr,
    )
    return 1


def _run_verify_publish(args: argparse.Namespace) -> int:
    provenance = _read_json(args.provenance)
    sys.stdout.write(f"{publish_provenance_action(provenance, args.sha)}\n")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    handlers = {
        "tags": _run_tags,
        "max-shas": _run_max_shas,
        "prepare": _run_prepare,
        "finalize": _run_finalize,
        "absent": _run_absent,
        "verify-promotion": _run_verify_promotion,
        "verify-publish": _run_verify_publish,
    }
    try:
        return handlers[args.command](args)
    except (OSError, json.JSONDecodeError, PlanError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
