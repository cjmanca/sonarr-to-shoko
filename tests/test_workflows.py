"""Structural tests for the CI + GHCR publishing workflows (M8).

PyYAML is already a runtime dependency (config loader), so these parse the
real YAML files — no new dependency. These guard the security/acceptance
properties: least-privilege permissions, PR isolation, immutable sha- tag
naming, latest serialization, matrix cap parity, and the shared check list.
"""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml  # type: ignore[import-untyped]

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
CI_TEXT = (WORKFLOWS / "ci.yml").read_text(encoding="utf-8")
PUBLISH_TEXT = (WORKFLOWS / "publish.yml").read_text(encoding="utf-8")
CHECKS_TEXT = (ROOT / "scripts" / "ci_checks.sh").read_text(encoding="utf-8")

CANONICAL_CHECKS = [
    "uv run ruff check .",
    "uv run ruff format --check .",
    "uv run mypy",
    "uv run pytest -q",
    "git diff --check",
]
SHA_HEX_LEN = 40


def _load(name: str, text: str) -> Any:
    data = yaml.safe_load(text)
    assert isinstance(data, dict)
    # YAML 1.1 parses the bare `on:` key as boolean True.
    triggers = data.get("on", data.get(True))
    assert isinstance(triggers, dict)
    data["_triggers"] = triggers
    return data


def _load_enumerator() -> Any:
    script = ROOT / "scripts" / "enumerate_introduced.py"
    spec = importlib.util.spec_from_file_location("enumerate_introduced", script)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclass annotation resolution under 3.14
    spec.loader.exec_module(module)
    return module


def _load_ghcr_planner() -> Any:
    script = ROOT / "scripts" / "ghcr_plan.py"
    assert script.is_file(), "the workflow's shared GHCR planner must exist"
    spec = importlib.util.spec_from_file_location("ghcr_plan", script)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


publish = _load("publish", PUBLISH_TEXT)
ci = _load("ci", CI_TEXT)
enum = _load_enumerator()


def _steps(job: dict[str, Any]) -> str:
    return json.dumps(job.get("steps", []))


def test_publish_permissions_are_least_privilege() -> None:
    assert publish["permissions"] == {"contents": "read"}
    assert publish["jobs"]["publish"]["permissions"] == {
        "contents": "read",
        "packages": "write",
    }
    assert publish["jobs"]["promote-latest"]["permissions"] == {
        "contents": "read",
        "packages": "write",
    }
    # Registry discovery is pull-only. Same-run artifact handoff needs no
    # additional Actions API permission; only publisher jobs need packages:write.
    assert publish["jobs"]["plan"]["permissions"] == {
        "contents": "read",
        "packages": "read",
    }
    assert "actions" not in publish["jobs"]["plan"]["permissions"]
    assert "actions" not in publish["jobs"]["publish"]["permissions"]
    assert "packages" not in json.dumps(publish["jobs"]["test"].get("permissions", {}))
    for name in ("test", "plan"):
        assert "packages: write" not in json.dumps(publish["jobs"][name].get("permissions", {}))


def test_ci_workflow_has_no_publish_permission() -> None:
    assert ci["permissions"] == {"contents": "read"}
    for job in ci["jobs"].values():
        assert "packages" not in json.dumps(job.get("permissions", {}))


def test_publish_gates_exclude_pull_requests() -> None:
    publish_if = str(publish["jobs"]["publish"]["if"])
    plan_if = str(publish["jobs"]["plan"]["if"])
    promote_if = str(publish["jobs"]["promote-latest"]["if"])
    for cond in (publish_if, plan_if, promote_if):
        assert "pull_request" not in cond
    assert "push" in publish_if
    assert "workflow_dispatch" in publish_if
    assert "backfill" in publish_if
    assert "refs/heads/main" in promote_if


def test_latest_only_in_promote_and_sha_tag_pattern_in_publish() -> None:
    publish_dump = _steps(publish["jobs"]["publish"])
    assert "sha-${{ matrix.sha }}" in publish_dump
    assert "latest" not in publish_dump
    promote_dump = _steps(publish["jobs"]["promote-latest"])
    assert "shoko-sonarr-linker:latest" in promote_dump


def test_promote_latest_concurrency_is_serialized() -> None:
    concurrency = publish["jobs"]["promote-latest"]["concurrency"]
    assert concurrency["group"] == "latest-promotion"
    assert concurrency["cancel-in-progress"] is False
    assert "git ls-remote" in _steps(publish["jobs"]["promote-latest"])


def test_latest_concurrency_comment_documents_pending_replacement_and_head_guard() -> None:
    assert "a newer run may replace an older pending promotion" in PUBLISH_TEXT
    assert "head check immediately before the latest push rejects stale candidates" in PUBLISH_TEXT
    assert "never cancel a queued promotion" not in PUBLISH_TEXT


def test_no_repository_secrets_in_workflows() -> None:
    secret_refs = set(re.findall(r"secrets\.[A-Za-z0-9_]+", PUBLISH_TEXT)) | set(
        re.findall(r"secrets\.[A-Za-z0-9_]+", CI_TEXT)
    )
    assert secret_refs == {"secrets.GITHUB_TOKEN"}
    assert "SONARR" not in PUBLISH_TEXT
    assert "SHOKO" not in PUBLISH_TEXT


def test_triggers_declared_dispatch_inputs_and_schedule() -> None:
    triggers = publish["_triggers"]
    assert triggers["push"]["branches"] == ["**"]
    dispatch_inputs = triggers["workflow_dispatch"]["inputs"]
    assert set(dispatch_inputs) == {"from_sha", "to_sha", "backfill", "max_shas"}
    assert dispatch_inputs["backfill"]["type"] == "boolean"
    assert triggers["schedule"], "weekly backfill schedule must be declared"


def test_dispatch_input_descriptions_explain_selected_branch_recovery() -> None:
    dispatch_inputs = publish["_triggers"]["workflow_dispatch"]["inputs"]
    assert "selected refs/heads/* branch" in dispatch_inputs["to_sha"]["description"]
    assert "fetched head" in dispatch_inputs["to_sha"]["description"]
    assert "selected repository branch" in dispatch_inputs["backfill"]["description"]
    assert "each run remains capped" in dispatch_inputs["backfill"]["description"]


def test_matrix_cap_matches_script_constant() -> None:
    expected_cap = 10  # published matrix size guidance (plan enforces per run)
    assert expected_cap == enum.DEFAULT_MAX_SHAS
    planner = _load_ghcr_planner()
    assert expected_cap == planner.MAX_SHAS_CAP
    plan_dump = _steps(publish["jobs"]["plan"])
    assert "--max-shas" in plan_dump
    assert "$MAX" in plan_dump
    assert "ghcr_plan.py max-shas" in plan_dump
    assert "MAX_SHAS_INPUT" in plan_dump
    assert "ghcr_plan.py prepare" in plan_dump
    assert "ghcr_plan.py finalize" in plan_dump
    assert "--max-verify 10" in plan_dump
    assert '[ "$MAX" -gt 10 ]' not in plan_dump
    run_missing = re.search(r"(?ms)^[ \t]*run_missing\(\)[ \t]*\{(.*?)^[ \t]*\}", _plan_run())
    assert run_missing is not None
    assert '--max-shas "$MAX"' in run_missing.group(1)


def test_plan_verification_rotation_uses_same_run_number_for_prepare_and_finalize() -> None:
    plan_run = _plan_run()
    prepare = re.search(r"(?ms)^python3 scripts/ghcr_plan.py prepare(.*?)^VERIFY_SHAS=", plan_run)
    finalize = re.search(
        r"(?ms)^python3 scripts/ghcr_plan.py finalize(.*?)^DECISION_STATUS=", plan_run
    )
    assert prepare is not None
    assert finalize is not None
    assert '--rotation "$GITHUB_RUN_NUMBER"' in prepare.group(0)
    assert '--rotation "$GITHUB_RUN_NUMBER"' in finalize.group(0)


def test_shared_check_list_parity() -> None:
    for command in CANONICAL_CHECKS:
        assert command in CHECKS_TEXT
    assert "scripts/ci_checks.sh" in _steps(publish["jobs"]["test"])
    assert "scripts/ci_checks.sh" in _steps(publish["jobs"]["publish"])
    assert "scripts/ci_checks.sh" not in _steps(publish["jobs"]["promote-latest"])
    assert "scripts/ci_checks.sh" in _steps(ci["jobs"]["quality"])
    # ci.yml must not hand-roll a divergent check list
    assert "uv run ruff check ." not in CI_TEXT
    assert "uv run pytest -q" not in CI_TEXT


def test_publish_matrix_wired_to_plan_and_isolated() -> None:
    job = publish["jobs"]["publish"]
    assert job["needs"] == ["test", "plan"]
    matrix = job["strategy"]["matrix"]
    assert matrix["sha"] == "${{ fromJSON(needs.plan.outputs.shas) }}"
    assert job["strategy"]["fail-fast"] is False
    assert "continue-on-error" not in job
    assert job["concurrency"] == {
        "group": "publish-sha-${{ matrix.sha }}",
        "cancel-in-progress": False,
    }


def test_successful_test_job_records_the_exact_checked_revision_after_checks() -> None:
    job = publish["jobs"]["test"]
    steps = job["steps"]
    checks = next(step for step in steps if "bash scripts/ci_checks.sh" in str(step.get("run", "")))
    record = next(step for step in steps if step.get("id") == "validated_revision")
    assert steps.index(checks) < steps.index(record)
    assert record.get("if") is None  # Default success gate: no output after failed checks.
    assert job["outputs"]["validated_sha"] == "${{ steps.validated_revision.outputs.sha }}"
    assert record["env"]["EXPECTED_SHA"] == "${{ github.sha }}"
    assert "git rev-parse HEAD" in record["run"]
    assert record["run"].index('test "$SHA" = "$EXPECTED_SHA"') < record["run"].index(
        'echo "sha=$SHA" >> "$GITHUB_OUTPUT"'
    )


def test_publish_reuses_checks_only_for_the_same_runs_identical_revision() -> None:
    job = publish["jobs"]["publish"]
    assert "test" in job["needs"]
    steps = job["steps"]
    guarded = [
        step
        for step in steps
        if str(step.get("uses", "")).startswith("astral-sh/setup-uv@")
        or step.get("with", {}).get("name") == "historical-validator"
        or "uv sync --frozen" in str(step.get("run", ""))
        or "bash scripts/ci_checks.sh" in str(step.get("run", ""))
    ]
    assert len(guarded) == 4
    for step in guarded:
        # Missing output or a different matrix SHA falls back to its own checks.
        assert step["if"] == "${{ needs.test.outputs.validated_sha != matrix.sha }}"
    verify = next(step for step in guarded if "ci_checks.sh" in str(step.get("run", "")))
    assert "historical-validator/validate_historical.py --repo ." in verify["run"]
    reuse = next(step for step in steps if step.get("name", "").startswith("Reuse successful"))
    assert reuse["if"] == "${{ needs.test.outputs.validated_sha == matrix.sha }}"
    registry = next(step for step in steps if step.get("id") == "registry_check")
    assert registry.get("if") is None  # Reusing tests never skips the registry safety gate.


def test_publish_sha_is_reverified_under_lock_before_docker_steps() -> None:
    job = publish["jobs"]["publish"]
    steps = job["steps"]
    verify_index = next(
        index
        for index, step in enumerate(steps)
        if "ghcr_tags.py verify" in str(step.get("run", ""))
    )
    verify = steps[verify_index]
    assert verify["id"] == "registry_check"
    assert verify["env"]["MATRIX_SHA"] == "${{ matrix.sha }}"
    assert "ghcr_plan.py verify-publish" in verify["run"]
    assert "ghcr-publish-tools/ghcr_tags.py" in verify["run"]
    assert "ghcr-publish-tools/ghcr_plan.py" in verify["run"]
    assert "needs.plan.outputs.owner" in verify["env"]["GHCR_OWNER"]

    docker_steps = [
        step
        for step in steps
        if str(step.get("uses", "")).startswith(
            ("docker/setup-buildx-action@", "docker/login-action@", "docker/build-push-action@")
        )
    ]
    assert docker_steps
    for step in docker_steps:
        assert step["if"] == "${{ steps.registry_check.outputs.action == 'publish' }}"
        assert steps.index(step) > verify_index
    image_validation = next(
        step for step in steps if str(step.get("name", "")).startswith("Validate image reference")
    )
    assert image_validation["if"] == "${{ steps.registry_check.outputs.action == 'publish' }}"

    tool_download = next(
        index
        for index, step in enumerate(steps)
        if str(step.get("uses", "")).startswith("actions/download-artifact@")
        and step.get("with", {}).get("name") == "ghcr-publish-tools"
    )
    assert tool_download < verify_index


def test_publish_build_uses_labels_and_scoped_cache() -> None:
    dump = _steps(publish["jobs"]["publish"])
    assert "org.opencontainers.image.source" in dump
    assert "org.opencontainers.image.revision" in dump
    assert "scope=sha-${{ matrix.sha }}" in dump
    assert json.dumps(publish["jobs"]["publish"]["steps"][-2]["with"]).count('"push": true') == 1


def _run_bodies(data: dict[str, Any]) -> list[str]:
    bodies: list[str] = []
    for job in data["jobs"].values():
        for step in job.get("steps", []):
            run = step.get("run")
            if isinstance(run, str):
                bodies.append(run)
    return bodies


def test_run_blocks_contain_no_expression_interpolation() -> None:
    """F1: `${{ }}` must never reach a shell script body (env: indirection only)."""
    for label, data in (("publish.yml", publish), ("ci.yml", ci)):
        for body in _run_bodies(data):
            assert "${{" not in body, f"{label}: run block interpolates ${{{{ }}}}:\n{body}"


def _uses_lines(text: str) -> list[str]:
    return [
        line.strip().removeprefix("-").strip()
        for line in text.splitlines()
        if re.match(r"^\s*-\s+uses:", line) or line.strip().startswith("uses:")
    ]


def test_every_uses_ref_is_sha_pinned_in_both_workflows() -> None:
    """F9: every `uses:` line pins a full 40-hex commit SHA and names its tag."""
    pattern = re.compile(r"^uses: [A-Za-z0-9._/-]+@([0-9a-f]{40}) # v\d+\.\d+\.\d+$")
    for label, text in (("publish.yml", PUBLISH_TEXT), ("ci.yml", CI_TEXT)):
        lines = _uses_lines(text)
        assert lines, f"{label}: expected uses: steps"
        for line in lines:
            match = pattern.fullmatch(line)
            assert match is not None, f"{label}: not SHA-pinned: {line}"
            assert len(match.group(1)) == SHA_HEX_LEN


def test_pinned_action_shas_match_tag_header_records() -> None:
    """The pin table in each header comment must match the `uses:` SHAs."""
    for text in (PUBLISH_TEXT, CI_TEXT):
        for line in _uses_lines(text):
            sha = line.split("@", 1)[1].split(" # ", 1)[0]
            tag = line.split(" # ", 1)[1]
            assert sha in text
            assert f"{tag} " in text


def test_publish_workflow_does_not_drop_pushes_and_keeps_job_locks() -> None:
    """Avoid workflow-level pending-run replacement; retain SHA/latest serialization."""
    assert "concurrency" not in publish
    publish_concurrency = publish["jobs"]["publish"]["concurrency"]
    assert publish_concurrency["group"] == "publish-sha-${{ matrix.sha }}"
    assert publish_concurrency["cancel-in-progress"] is False
    latest_concurrency = publish["jobs"]["promote-latest"]["concurrency"]
    assert latest_concurrency["group"] == "latest-promotion"
    assert latest_concurrency["cancel-in-progress"] is False


def test_plan_dispatch_inputs_flow_only_through_env() -> None:
    """F1: github.*/inputs.* reach the plan shell only as quoted env vars."""
    plan = publish["jobs"]["plan"]
    env = plan["env"]
    assert env["TO_SHA_INPUT"] == "${{ inputs.to_sha }}"
    assert env["FROM_SHA_INPUT"] == "${{ inputs.from_sha }}"
    assert env["MAX_SHAS_INPUT"] == "${{ inputs.max_shas }}"
    assert env["PUSH_SHA"] == "${{ github.sha }}"
    assert env["PUSH_BEFORE"] == "${{ github.event.before }}"
    assert env["PUSH_REF"] == "${{ github.ref }}"
    assert env["EVENT_NAME"] == "${{ github.event_name }}"
    run = next(step for step in plan["steps"] if step.get("id") == "plan")["run"]
    interpolated = ("inputs.", "github.sha", "github.event.before", "github.ref")
    for token in (*interpolated, "github.event_name"):
        assert token not in run


def test_plan_validates_dispatch_shas_and_gates_range() -> None:
    """F1 hex validation + selected-branch ancestry gate for dispatch/backfills."""
    run = next(step for step in publish["jobs"]["plan"]["steps"] if step.get("id") == "plan")["run"]
    assert "not-40-hex" in run
    assert "{40}" in run  # 40-hex regex guard on from_sha/to_sha inputs
    assert "merge-base --is-ancestor" in run
    assert "not-ancestor-of-selected-branch-head" in run
    assert "not-ancestor-of-to-sha" in run
    assert "refs/heads/*" in run


def test_dispatch_uses_trusted_plan_checkout_and_fetches_selected_branch() -> None:
    plan_checkout = next(
        step
        for step in publish["jobs"]["plan"]["steps"]
        if str(step.get("uses", "")).startswith("actions/checkout@")
    )
    assert plan_checkout.get("with", {}).get("ref") == (
        "${{ github.event_name == 'workflow_dispatch' && 'refs/heads/main' || github.ref }}"
    )
    assert plan_checkout["with"]["fetch-depth"] == 0

    run = _plan_run()
    dispatch = run.find('if [ "$EVENT_NAME" = "workflow_dispatch" ]; then')
    branch_guard = run.find('case "$PUSH_REF" in', dispatch)
    fetch_selected = run.find('git fetch --no-tags origin "$PUSH_REF"', branch_guard)
    resolve_branch = run.find("BRANCH_HEAD=$(git rev-parse FETCH_HEAD)", fetch_selected)
    default_branch = run.find('if [ -z "$TO" ]; then TO="$BRANCH_HEAD"; fi', resolve_branch)
    validate_to = run.find('if [ -n "$TO" ] && ! is_full_sha "$TO";', default_branch)
    ancestry = run.find('git merge-base --is-ancestor "$TO" "$BRANCH_HEAD"', validate_to)
    enumerate_missing = run.find("run_missing\n", ancestry)

    assert (
        min(
            dispatch,
            branch_guard,
            fetch_selected,
            resolve_branch,
            default_branch,
            validate_to,
            ancestry,
            enumerate_missing,
        )
        >= 0
    )
    assert (
        dispatch
        < branch_guard
        < fetch_selected
        < resolve_branch
        < default_branch
        < validate_to
        < ancestry
        < enumerate_missing
    )
    assert "refs/heads/*)" in run
    assert '*) die "workflow_dispatch ref must be a branch' in run
    assert "git fetch --no-tags origin refs/heads/main" not in run
    assert 'if [ -z "$TO" ]; then TO="$PUSH_SHA"; fi' in run


def test_backfill_from_sha_must_be_ancestor_of_to_sha_before_enumeration() -> None:
    run = _plan_run()
    validate_from = run.find('if [ -n "$FROM" ] && ! is_full_sha "$FROM";')
    from_boundary = run.find('git merge-base --is-ancestor "$FROM" "$TO"', validate_from)
    enumerate_missing = run.find("run_missing\n", from_boundary)

    assert validate_from >= 0
    assert from_boundary > validate_from
    assert enumerate_missing > from_boundary
    assert "not-ancestor-of-to-sha" in run


def test_schedule_and_default_manual_backfills_share_inclusive_buildable_boundary() -> None:
    run = _plan_run()
    run_missing = re.search(r"(?ms)^[ \t]*run_missing\(\)[ \t]*\{(.*?)^[ \t]*\}", run)
    assert run_missing is not None
    assert '--first-buildable "$FIRST_BUILDABLE"' in run_missing.group(1)
    assert 'if [ -n "$FROM" ]; then args+=(--since "$FROM"); fi' in run_missing.group(1)
    assert "run_missing absent.txt" in run  # the verified-absent retry uses the same boundary
    assert "FIRST_BUILDABLE=aa6b85ea71192d9c94c7f3c8a58d715c6c37db42" in run
    assert enum.DEFAULT_FIRST_BUILDABLE == "aa6b85ea71192d9c94c7f3c8a58d715c6c37db42"
    assert publish["_triggers"]["schedule"]
    assert "workflow_dispatch" in str(publish["jobs"]["plan"]["if"])
    assert 'if [ -z "$TO" ]; then TO="$PUSH_SHA"; fi' in run  # scheduled default tip
    assert 'if [ -z "$TO" ]; then TO="$BRANCH_HEAD"; fi' in run  # manual default tip
    assert run.count("run_missing\n") == 1  # schedule and manual paths share this call site


def test_failed_sha_backfill_comment_describes_registry_recheck() -> None:
    assert re.search(
        r"later backfill\s+#\s+rechecks registry provenance for the failed SHA",
        PUBLISH_TEXT,
    )
    assert "The missing SHA stays untagged for the next backfill" not in PUBLISH_TEXT


def test_plan_cap_fails_closed_on_nonpositive_value() -> None:
    """F4: Python validates/clamps the decimal input before shell integer operations."""
    run = next(step for step in publish["jobs"]["plan"]["steps"] if step.get("id") == "plan")["run"]
    assert 'MAX=$(python3 scripts/ghcr_plan.py max-shas --value "${MAX_SHAS_INPUT}")' in run
    assert '[ "$MAX" -le 0 ]' not in run
    assert '[ "$MAX" -gt 10 ]' not in run


def test_plan_handles_branch_deletion_push() -> None:
    """F6: all-zero after (branch deletion) yields an empty matrix, not a crash."""
    run = next(step for step in publish["jobs"]["plan"]["steps"] if step.get("id") == "plan")["run"]
    assert "branch-deleted" in run
    zero = "0" * 40
    assert zero in run
    assert 'echo "shas=[]"' in run


def test_promote_gate_requires_successful_publish_or_empty_matrix_skip() -> None:
    """A failed per-SHA publish blocks latest; an empty matrix still has a registry gate."""
    promote_if = str(publish["jobs"]["promote-latest"]["if"])
    assert publish["jobs"]["promote-latest"]["needs"] == ["test", "plan", "publish"]
    assert "needs.publish.result == 'success'" in promote_if
    assert "needs.publish.result == 'skipped'" in promote_if
    assert "needs.publish.result == 'failure'" not in promote_if
    assert "needs.test.result == 'success'" in promote_if
    assert "needs.plan.result == 'success'" in promote_if
    assert "!cancelled()" in promote_if
    promote = publish["jobs"]["promote-latest"]
    steps = promote["steps"]
    assert "scripts/ci_checks.sh" not in _steps(promote)
    verify = next(
        i for i, step in enumerate(steps) if "ghcr_tags.py verify" in str(step.get("run", ""))
    )
    push = next(
        i
        for i, step in enumerate(steps)
        if "docker buildx imagetools create" in str(step.get("run", ""))
    )
    assert verify < push
    verify_run = steps[verify]["run"]
    assert "ghcr_plan.py verify-promotion" in verify_run
    assert "set -euo pipefail" in verify_run
    assert "needs.plan.result == 'success'" in promote_if
    assert "RUN_SHA" in steps[verify]["env"]
    assert "git ls-remote" in _steps(promote)


def test_latest_allows_safe_main_backfill_events() -> None:
    promote = publish["jobs"]["promote-latest"]
    promote_if = str(promote["if"])
    assert "github.event_name == 'push'" in promote_if
    assert "github.event_name == 'schedule'" in promote_if
    assert "(github.event_name == 'workflow_dispatch' && inputs.backfill)" in promote_if
    assert "github.ref == 'refs/heads/main'" in promote_if
    assert "needs.test.result == 'success'" in promote_if
    assert "needs.plan.result == 'success'" in promote_if
    assert "needs.publish.result == 'success'" in promote_if
    assert "needs.publish.result == 'skipped'" in promote_if
    assert promote_if.count("github.ref == 'refs/heads/main'") == 1
    assert "PUSH_REF" not in promote_if

    steps = promote["steps"]
    assert "scripts/ci_checks.sh" not in _steps(promote)
    verify = next(step for step in steps if "ghcr_tags.py verify" in str(step.get("run", "")))
    assert verify["env"]["RUN_SHA"] == "${{ github.sha }}"
    head_check = next(
        index
        for index, step in enumerate(steps)
        if "git ls-remote origin refs/heads/main" in str(step.get("run", ""))
    )
    latest_push = next(
        index
        for index, step in enumerate(steps)
        if "docker buildx imagetools create" in str(step.get("run", ""))
    )
    assert head_check + 1 == latest_push


def test_skipped_empty_publish_matrix_still_requires_exact_sha_verification() -> None:
    publish_job = publish["jobs"]["publish"]
    promote_if = str(publish["jobs"]["promote-latest"]["if"])
    assert publish_job["strategy"]["fail-fast"] is False
    assert "needs.publish.result == 'skipped'" in promote_if

    steps = publish["jobs"]["promote-latest"]["steps"]
    verify = next(step for step in steps if "ghcr_tags.py verify" in str(step.get("run", "")))
    assert verify.get("if") is None
    assert verify["env"]["RUN_SHA"] == "${{ github.sha }}"
    assert '--shas "${RUN_SHA}"' in verify["run"]
    assert "ghcr_plan.py verify-promotion" in verify["run"]


def test_latest_copies_the_verified_digest_without_rebuilding_or_retesting() -> None:
    steps = publish["jobs"]["promote-latest"]["steps"]
    assert not any(
        str(step.get("uses", "")).startswith(("docker/build-push-action@", "astral-sh/setup-uv@"))
        for step in steps
    )
    assert "uv sync" not in _steps(publish["jobs"]["promote-latest"])
    source = next(step for step in steps if step.get("id") == "source_image")
    assert source["env"]["SOURCE_TAG"] == "${{ needs.plan.outputs.image }}:sha-${{ github.sha }}"
    assert "docker buildx imagetools inspect" in source["run"]
    assert "^sha256:[0-9a-f]{64}$" in source["run"]
    verify = next(step for step in steps if "ghcr_tags.py verify" in str(step.get("run", "")))
    copy = next(
        step for step in steps if "docker buildx imagetools create" in str(step.get("run", ""))
    )
    assert steps.index(source) < steps.index(verify) < steps.index(copy)
    for step in (verify, copy):
        assert step["env"]["SOURCE_DIGEST"] == "${{ steps.source_image.outputs.digest }}"
    assert '--digest "${SOURCE_DIGEST}"' in verify["run"]
    assert "--prefer-index=false" in copy["run"]
    assert '"${IMAGE_NAME}@${SOURCE_DIGEST}"' in copy["run"]
    assert ":sha-" not in copy["run"]  # Never resolve the tag again after verification.


def _plan_run() -> str:
    plan_steps = publish["jobs"]["plan"]["steps"]
    step = next(step for step in plan_steps if step.get("id") == "plan")
    return str(step["run"])


def _plan_all_runs() -> str:
    return "\n".join(str(step.get("run", "")) for step in publish["jobs"]["plan"]["steps"])


def _image_name_run() -> str:
    step = next(step for step in publish["jobs"]["plan"]["steps"] if step.get("id") == "image_name")
    return str(step["run"])


# --- F2: image reference correctness -------------------------------------------------


def test_image_reference_uses_snake_case_owner_and_is_lowercased() -> None:
    """F2: `github.repositoryOwner` resolves to "" and produced ghcr.io//name tags."""
    assert "github.repository_owner" in PUBLISH_TEXT
    assert "repositoryOwner" not in PUBLISH_TEXT
    plan = publish["jobs"]["plan"]
    assert plan["env"]["REPO_OWNER"] == "${{ github.repository_owner }}"
    image_run = _image_name_run()
    assert "tr 'A-Z' 'a-z'" in image_run  # GHCR namespaces must be lowercase
    assert "ghcr.io/" in image_run
    assert "${{ inputs" not in image_run  # owner reaches the shell only via env


def test_image_reference_is_validated_before_any_push() -> None:
    """Empty owner and invalid references must fail loudly, pre-build-push."""
    image_run = _image_name_run()
    assert '[ -n "${REPO_OWNER}" ]' in image_run  # owner non-empty guard
    assert "github.repository_owner is empty" in image_run
    assert "^[a-z0-9]+(-[a-z0-9]+)*$" in image_run  # GHCR namespace segment rule
    assert r"ghcr\.io/[a-z0-9._-]+/shoko-sonarr-linker" in image_run
    plan = publish["jobs"]["plan"]
    outputs = plan["outputs"]
    assert outputs["image"] == "${{ steps.image_name.outputs.image }}"
    for job_name in ("publish", "promote-latest"):
        steps = publish["jobs"][job_name]["steps"]
        gate = next(
            i
            for i, s in enumerate(steps)
            if str(s.get("name", "")).startswith("Validate image reference")
        )
        push = next(
            i
            for i, s in enumerate(steps)
            if str(s.get("uses", "")).startswith("docker/build-push-action@")
            or "docker buildx imagetools create" in str(s.get("run", ""))
        )
        assert gate < push  # the gate runs before docker/build-push-action can see the tag
        gate_run = steps[gate]["run"]
        assert "sha-[0-9a-f]{40}" in gate_run if job_name == "publish" else ":latest$" in gate_run
        assert steps[gate]["env"]["IMAGE_TAG"].startswith("${{ needs.plan.outputs.image }}")


def test_publish_tags_flow_from_plan_output() -> None:
    """Build inputs and promotion environment use the validated plan image name."""
    build = next(
        step
        for step in publish["jobs"]["publish"]["steps"]
        if str(step.get("uses", "")).startswith("docker/build-push-action@")
    )
    assert build["with"]["tags"] == "${{ needs.plan.outputs.image }}:sha-${{ matrix.sha }}"
    promote = next(
        step
        for step in publish["jobs"]["promote-latest"]["steps"]
        if "docker buildx imagetools create" in str(step.get("run", ""))
    )
    assert promote["env"]["IMAGE_NAME"] == "${{ needs.plan.outputs.image }}"
    assert '--tag "${IMAGE_NAME}:latest"' in promote["run"]
    plan = publish["jobs"]["plan"]
    assert plan["env"]["REPO_OWNER"] == "${{ github.repository_owner }}"
    assert plan["env"]["GHCR_PACKAGE"] == "shoko-sonarr-linker"
    assert plan["env"]["GHCR_TOKEN"] == "${{ secrets.GITHUB_TOKEN }}"
    image_run = _image_name_run()
    assert '"${REPO_OWNER}"' in image_run  # owner reaches the shell only via env indirection


# --- F3: registry is the publication state -------------------------------------------


def test_plan_discovers_registry_tags_not_git_tags() -> None:
    """F3: `git ls-remote --tags` asserted the OLD defective design (git refs are
    independent of GHCR container tags); registry discovery replaces it."""
    all_runs = _plan_all_runs()
    assert "ghcr_tags.py list" in all_runs
    assert "known_tags.txt" in all_runs
    registry_step = next(
        step
        for step in publish["jobs"]["plan"]["steps"]
        if "ghcr_tags.py list" in str(step.get("run", ""))
    )
    assert registry_step["env"]["GHCR_OWNER"] == "${{ steps.image_name.outputs.owner }}"
    assert "refusing to publish against unknown registry state" in registry_step["run"]
    assert "ghcr_plan.py tags --registry registry.json" in registry_step["run"]
    assert "jq -r '.tags[]'" not in registry_step["run"]
    assert (
        "package_missing" in registry_step["run"]
    )  # fresh-package state is explicit, not a failure
    # No run body queries git tags (header prose may document the replacement):
    for body in _run_bodies(publish):
        assert "git ls-remote --tags" not in body  # registry query, not git refs
    # promote-latest keeps its head-verification ls-remote (refs/heads/main only)
    assert "refs/heads/main" in _steps(publish["jobs"]["promote-latest"])


def test_plan_verifies_tag_provenance_before_trusting_it() -> None:
    """A registry sha- tag counts as published only when the revision annotation
    verifies. F12 policy: a verified-published tag is SKIPPED, a tag proven
    ABSENT is publishable, and an EXISTING unverifiable tag is reported for
    operator reconciliation and ABORTS the plan — never silently overwritten
    (a sha- tag name derived from a commit is not proof of immutability)."""
    plan_run = _plan_run()
    assert "ghcr_tags.py verify" in plan_run
    assert "ghcr_plan.py prepare" in plan_run
    assert "ghcr_plan.py finalize" in plan_run
    assert "ghcr_plan.py absent" in plan_run
    assert "provenance verification failed" in plan_run
    assert "reconciliation" in plan_run
    assert '--unverified-file "$extra_unverified"' in plan_run
    assert "run_missing absent.txt" in plan_run  # only proven-absent SHAs re-enter the set
    assert "over_cap_shas" in plan_run
    assert "Provenance reconciliation required" in plan_run
    assert "refusing to publish while registry state or provenance is unverified" in plan_run
    assert "--rawfile res provenance.json" not in plan_run
    assert "select($pub | index(.))" not in plan_run
    assert plan_run.count("ghcr_tags.py verify") == 1  # common planner path for push and backfill


def test_unknown_provenance_wording_does_not_assert_tag_exists() -> None:
    tag_tool_text = (ROOT / "scripts" / "ghcr_tags.py").read_text(encoding="utf-8")
    assert "unknown_provenance  -> registry/tag state or provenance could not be" in PUBLISH_TEXT
    assert "unknown_provenance  -> tag EXISTS" not in PUBLISH_TEXT
    assert "registry/tag state or provenance could not be verified" in tag_tool_text
    assert "The tag EXISTS and its content is unknown" not in tag_tool_text


def test_plan_uploads_and_publish_downloads_historical_validator() -> None:
    """F3: pre-M8 commits have no scripts/ci_checks.sh in their own tree."""
    upload = next(
        step
        for step in publish["jobs"]["plan"]["steps"]
        if "upload-artifact" in str(step.get("uses", ""))
    )
    assert upload["with"]["name"] == "historical-validator"
    assert upload["with"]["path"] == "scripts/validate_historical.py"
    assert upload["with"]["if-no-files-found"] == "error"
    publish_dump = _steps(publish["jobs"]["publish"])
    download = next(
        step
        for step in publish["jobs"]["publish"]["steps"]
        if "download-artifact" in str(step.get("uses", ""))
    )
    assert download["with"]["name"] == "historical-validator"
    assert "historical-validator" in publish_dump
    leg_run = next(
        step["run"]
        for step in publish["jobs"]["publish"]["steps"]
        if "ci_checks.sh" in str(step.get("run"))
    )
    assert "[ -f scripts/ci_checks.sh ]" in leg_run  # per-commit tooling choice, not a blind run
    assert "validate_historical.py" in leg_run
    assert (ROOT / "scripts" / "validate_historical.py").is_file()
    assert (ROOT / "scripts" / "ghcr_tags.py").is_file()


def test_plan_uploads_and_publish_downloads_current_ghcr_tools() -> None:
    upload = next(
        step
        for step in publish["jobs"]["plan"]["steps"]
        if str(step.get("uses", "")).startswith("actions/upload-artifact@")
        and step.get("with", {}).get("name") == "ghcr-publish-tools"
    )
    assert "scripts/ghcr_tags.py" in upload["with"]["path"]
    assert "scripts/ghcr_plan.py" in upload["with"]["path"]

    download = next(
        step
        for step in publish["jobs"]["publish"]["steps"]
        if str(step.get("uses", "")).startswith("actions/download-artifact@")
        and step.get("with", {}).get("name") == "ghcr-publish-tools"
    )
    assert download["with"]["path"] == "ghcr-publish-tools"


def _sha(number: int) -> str:
    return f"{number:040x}"


def test_registry_tag_normalization_handles_empty_single_multiple_and_newlines() -> None:
    planner = _load_ghcr_planner()

    assert planner.registry_tags_to_text({"status": "package_missing", "tags": []}) == ""
    assert planner.registry_tags_to_text({"status": "ok", "tags": []}) == ""
    assert planner.normalize_tag_lines("") == set()
    assert planner.normalize_tag_lines("\n\n") == set()
    one = f"sha-{_sha(1)}"
    two = f"sha-{_sha(2)}"
    with pytest.raises(planner.PlanError, match="package_missing.*empty tags"):
        planner.registry_tags_to_text({"status": "package_missing", "tags": [one]})
    assert planner.normalize_tag_lines(one) == {_sha(1)}
    assert planner.normalize_tag_lines(f"{one}\n") == {_sha(1)}
    assert planner.normalize_tag_lines(f"{one}\n\n{two}\nlatest\n") == {
        _sha(1),
        _sha(2),
    }
    assert planner.registry_tags_to_text({"status": "ok", "tags": [one, two, "latest"]}) == (
        f"{one}\n{two}\n"
    )


def test_malformed_sha_tag_fails_closed() -> None:
    planner = _load_ghcr_planner()
    for malformed in ("sha-not-a-full-sha", f"SHA-{_sha(1)}", f"sha-{_sha(1)} "):
        with pytest.raises(planner.PlanError, match="invalid sha tag"):
            planner.normalize_tag_lines(malformed)


def test_planner_membership_skips_only_published_candidate_and_keeps_missing() -> None:
    planner = _load_ghcr_planner()
    a, b, c = _sha(1), _sha(2), _sha(3)
    plan = {"shas": [a, b, c], "skipped": [], "overflow": [], "notes": []}
    known_tags = f"sha-{b}\n"

    prepared = planner.prepare_plan(plan, known_tags, max_verify=10)
    assert prepared["verify_shas"] == [b]

    decision = planner.finalize_plan(
        plan,
        known_tags,
        prepared,
        {"status": "ok", "results": {b: "published"}},
        max_verify=10,
    )
    assert decision["status"] == "ok"
    assert decision["plan"]["shas"] == [a, c]
    assert decision["published_shas"] == [b]
    assert decision["absent_shas"] == []


def test_absent_and_unverified_existing_tags_have_distinct_safe_outcomes() -> None:
    planner = _load_ghcr_planner()
    a, b = _sha(4), _sha(5)
    plan = {"shas": [a, b], "skipped": [], "overflow": [], "notes": []}
    known_tags = f"sha-{a}\nsha-{b}\n"
    prepared = planner.prepare_plan(plan, known_tags)

    decision = planner.finalize_plan(
        plan,
        known_tags,
        prepared,
        {
            "status": "ok",
            "results": {a: "not_published", b: "unknown_provenance"},
        },
    )
    assert decision["status"] == "reconciliation_required"
    assert decision["plan"]["shas"] == [a]
    assert decision["absent_shas"] == [a]
    assert decision["unknown_shas"] == [b]
    assert decision["reconciliation"] == planner.reconciliation_lines(
        {"status": "ok", "results": {b: "unknown_provenance"}}, [b]
    )
    assert f"sha-{b}" in decision["reconciliation"][0]


def test_omitted_expected_provenance_is_unknown_and_never_publishable() -> None:
    planner = _load_ghcr_planner()
    sha = _sha(6)
    plan = {"shas": [sha], "skipped": [], "overflow": [], "notes": []}
    known_tags = f"sha-{sha}\n"
    prepared = planner.prepare_plan(plan, known_tags)

    decision = planner.finalize_plan(
        plan,
        known_tags,
        prepared,
        {"status": "ok", "results": {}},
    )
    assert decision["status"] == "reconciliation_required"
    assert decision["unknown_shas"] == [sha]
    assert decision["plan"]["shas"] == []
    assert decision["reconciliation"] == planner.reconciliation_lines(
        {"status": "ok", "results": {}}, [sha]
    )


def test_finalized_backfill_absent_skipped_sha_does_not_grow_capped_plan() -> None:
    planner = _load_ghcr_planner()
    shas = [_sha(number) for number in range(30, 40)]
    skipped_sha = _sha(40)
    plan = {
        "shas": shas,
        "skipped": [{"sha": skipped_sha, "reason": "already-tagged"}],
        "overflow": [],
        "notes": [],
    }
    known_tags = f"sha-{skipped_sha}\n"
    prepared = planner.prepare_plan(plan, known_tags, max_verify=10)
    decision = planner.finalize_plan(
        plan,
        known_tags,
        prepared,
        {"status": "ok", "results": {skipped_sha: "not_published"}},
        max_verify=10,
    )

    assert decision["plan"]["shas"] == shas
    assert len(decision["plan"]["shas"]) == 10
    assert decision["absent_shas"] == [skipped_sha]


def test_verification_cap_excludes_all_existing_tags_beyond_ten() -> None:
    planner = _load_ghcr_planner()
    shas = [_sha(i) for i in range(10, 22)]
    plan = {
        "shas": [],
        "skipped": [{"sha": sha, "reason": "already-tagged"} for sha in shas],
        "overflow": [],
        "notes": [],
    }
    known_tags = "".join(f"sha-{sha}\n" for sha in shas)

    prepared = planner.prepare_plan(plan, known_tags, max_verify=10)
    assert prepared["verify_shas"] == shas[:10]
    assert prepared["over_cap_shas"] == shas[10:]
    decision = planner.finalize_plan(
        plan,
        known_tags,
        prepared,
        {"status": "ok", "results": {sha: "published" for sha in shas[:10]}},
        max_verify=10,
    )
    assert decision["plan"]["shas"] == []
    assert decision["over_cap_shas"] == shas[10:]
    assert not set(decision["plan"]["shas"]) & set(shas[10:])


def test_verification_window_rotates_across_existing_candidates_and_finalize_matches() -> None:
    planner = _load_ghcr_planner()
    shas = [_sha(number) for number in range(10, 30)]
    plan = {"shas": shas, "skipped": [], "overflow": [], "notes": []}
    known_tags = "".join(f"sha-{sha}\n" for sha in shas)

    first = planner.prepare_plan(plan, known_tags, max_verify=10, rotation=0)
    second = planner.prepare_plan(plan, known_tags, max_verify=10, rotation=10)

    assert first["verify_shas"] == shas[:10]
    assert second["verify_shas"] == shas[10:]
    assert not set(first["verify_shas"]) & set(second["verify_shas"])
    assert set(first["verify_shas"]) | set(second["verify_shas"]) == set(shas)
    assert first["over_cap_shas"] == shas[10:]
    assert second["over_cap_shas"] == shas[:10]

    with pytest.raises(planner.PlanError, match="rotation"):
        planner.finalize_plan(
            plan,
            known_tags,
            first,
            {"status": "ok", "results": {sha: "published" for sha in shas[:10]}},
            max_verify=10,
            rotation=10,
        )

    decision = planner.finalize_plan(
        plan,
        known_tags,
        second,
        {"status": "ok", "results": {sha: "not_published" for sha in shas[10:]}},
        max_verify=10,
        rotation=10,
    )
    assert decision["verify_shas"] == shas[10:]
    assert decision["over_cap_shas"] == shas[:10]
    assert decision["absent_shas"] == shas[10:]
    assert decision["plan"]["shas"] == shas[10:]
    assert not set(decision["plan"]["shas"]) & set(shas[:10])


def test_rotation_wraps_while_preserving_candidate_order() -> None:
    planner = _load_ghcr_planner()
    shas = [_sha(number) for number in range(20, 40)]
    plan = {
        "shas": [],
        "skipped": [{"sha": sha, "reason": "already-tagged"} for sha in shas],
        "overflow": [],
        "notes": [],
    }
    known_tags = "".join(f"sha-{sha}\n" for sha in shas)

    prepared = planner.prepare_plan(plan, known_tags, max_verify=10, rotation=15)

    assert prepared["verify_shas"] == shas[:5] + shas[15:]
    assert prepared["over_cap_shas"] == shas[5:15]


def test_rotation_cli_accepts_nonnegative_value_and_rejects_negative(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    planner = _load_ghcr_planner()
    shas = [_sha(number) for number in range(40, 60)]
    plan_path = tmp_path / "plan.json"
    known_tags_path = tmp_path / "known-tags.txt"
    plan_path.write_text(
        json.dumps(
            {
                "shas": [],
                "skipped": [{"sha": sha, "reason": "already-tagged"} for sha in shas],
            }
        ),
        encoding="utf-8",
    )
    known_tags_path.write_text("".join(f"sha-{sha}\n" for sha in shas), encoding="utf-8")

    assert (
        planner.main(
            [
                "prepare",
                "--plan",
                str(plan_path),
                "--known-tags-file",
                str(known_tags_path),
                "--rotation",
                "10",
            ]
        )
        == 0
    )
    prepared = json.loads(capsys.readouterr().out)
    assert prepared["rotation"] == 10
    assert prepared["verify_shas"] == shas[10:]

    prepared_path = tmp_path / "prepared.json"
    provenance_path = tmp_path / "provenance.json"
    prepared_path.write_text(json.dumps(prepared), encoding="utf-8")
    provenance_path.write_text(
        json.dumps({"status": "ok", "results": {sha: "published" for sha in shas[10:]}}),
        encoding="utf-8",
    )
    assert (
        planner.main(
            [
                "finalize",
                "--plan",
                str(plan_path),
                "--known-tags-file",
                str(known_tags_path),
                "--prepared",
                str(prepared_path),
                "--provenance",
                str(provenance_path),
                "--rotation",
                "10",
            ]
        )
        == 0
    )
    decision = json.loads(capsys.readouterr().out)
    assert decision["verify_shas"] == shas[10:]

    assert (
        planner.main(
            [
                "prepare",
                "--plan",
                str(plan_path),
                "--known-tags-file",
                str(known_tags_path),
                "--rotation",
                "-1",
            ]
        )
        == 2
    )
    assert "rotation" in capsys.readouterr().err

    assert (
        planner.main(
            [
                "finalize",
                "--plan",
                str(plan_path),
                "--known-tags-file",
                str(known_tags_path),
                "--prepared",
                str(prepared_path),
                "--provenance",
                str(provenance_path),
                "--rotation",
                "-1",
            ]
        )
        == 2
    )
    assert "rotation" in capsys.readouterr().err


def test_planner_rejects_negative_rotation_for_prepare_and_finalize() -> None:
    planner = _load_ghcr_planner()
    sha = _sha(61)
    plan = {"shas": [sha], "skipped": [], "overflow": [], "notes": []}
    known_tags = f"sha-{sha}\n"

    with pytest.raises(planner.PlanError, match="rotation"):
        planner.prepare_plan(plan, known_tags, rotation=-1)

    prepared = planner.prepare_plan(plan, known_tags)
    with pytest.raises(planner.PlanError, match="rotation"):
        planner.finalize_plan(
            plan,
            known_tags,
            prepared,
            {"status": "ok", "results": {sha: "published"}},
            rotation=-1,
        )


def test_provenance_report_uses_parsed_json_results() -> None:
    planner = _load_ghcr_planner()
    sha = _sha(23)
    provenance = json.loads(json.dumps({"status": "ok", "results": {sha: "unknown_provenance"}}))
    assert planner.reconciliation_lines(provenance, [sha]) == [
        f"- sha-{sha}: registry tag state unknown_provenance — reconcile the existing tag "
        "(inspect/pin/delete), then re-run backfill"
    ]


def test_latest_requires_exact_verified_immutable_sha_provenance() -> None:
    planner = _load_ghcr_planner()
    sha = _sha(24)
    assert planner.promotion_provenance_is_valid(
        {"status": "ok", "results": {sha: "published"}}, sha
    )
    for provenance in (
        {"status": "ok", "results": {sha: "not_published"}},
        {"status": "ok", "results": {sha: "unknown_provenance"}},
        {"status": "ok", "results": {_sha(25): "published"}},
        {"status": "auth_failure", "results": {sha: "published"}},
    ):
        assert not planner.promotion_provenance_is_valid(provenance, sha)


def test_publish_provenance_recheck_skips_published_and_allows_absent() -> None:
    planner = _load_ghcr_planner()
    decide = getattr(planner, "publish_provenance_action", None)
    assert callable(decide), "the workflow's publish decision must be shared and executable"
    sha = _sha(26)
    assert decide({"status": "ok", "results": {sha: "published"}}, sha) == "skip"
    assert decide({"status": "ok", "results": {sha: "not_published"}}, sha) == "publish"


def test_publish_provenance_recheck_fails_closed_on_unknown_or_malformed_results() -> None:
    planner = _load_ghcr_planner()
    decide = getattr(planner, "publish_provenance_action", None)
    assert callable(decide), "the workflow's publish decision must be shared and executable"
    sha = _sha(27)
    for provenance in (
        {"status": "ok", "results": {sha: "unknown_provenance"}},
        {"status": "ok", "results": {}},
        {"status": "ok", "results": {sha: "unexpected"}},
        {"status": "network_failure", "results": {sha: "published"}},
        {"status": "ok", "results": []},
    ):
        with pytest.raises(planner.PlanError):
            decide(provenance, sha)


def test_max_shas_parser_validates_and_clamps_without_integer_overflow() -> None:
    planner = _load_ghcr_planner()
    parse_max = getattr(planner, "parse_max_shas", None)
    assert callable(parse_max), "max_shas must be parsed through the shared safe planner"

    assert parse_max("") == 10
    assert parse_max("1") == 1
    assert parse_max("10") == 10
    assert parse_max("0007") == 7
    assert parse_max("00010") == 10
    assert parse_max("999999999999999999999999999999999999999999999999") == 10
    for invalid in ("0", "0000", "-1", "1.5", " 2", "abc"):
        with pytest.raises(planner.PlanError, match="positive decimal"):
            parse_max(invalid)
