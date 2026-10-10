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
        "actions": "read",
    }
    assert publish["jobs"]["promote-latest"]["permissions"] == {
        "contents": "read",
        "packages": "write",
    }
    # M8 F3 update: plan legitimately carries packages:read (registry tag
    # discovery is pull-only) and actions:write (historical-validator artifact);
    # only publish/promote-latest get packages:write, and the test job keeps none.
    assert publish["jobs"]["plan"]["permissions"] == {
        "contents": "read",
        "packages": "read",
        "actions": "write",
    }
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


def test_matrix_cap_matches_script_constant() -> None:
    expected_cap = 10  # published matrix size guidance (plan enforces per run)
    assert expected_cap == enum.DEFAULT_MAX_SHAS
    plan_dump = _steps(publish["jobs"]["plan"])
    assert f"MAX={expected_cap}" in plan_dump
    assert "--max-shas" in plan_dump
    assert "$MAX" in plan_dump
    cap = re.search(r"MAX=(\d+)", plan_dump)
    assert cap is not None
    assert int(cap.group(1)) == enum.DEFAULT_MAX_SHAS == expected_cap
    assert "-gt 10" in plan_dump  # dispatch max_shas is hard-clamped to the cap
    assert ".shas -=" in plan_dump  # already-tagged SHAs dropped (tag immutability)


def test_shared_check_list_parity() -> None:
    for command in CANONICAL_CHECKS:
        assert command in CHECKS_TEXT
    assert "scripts/ci_checks.sh" in _steps(publish["jobs"]["test"])
    assert "scripts/ci_checks.sh" in _steps(publish["jobs"]["publish"])
    assert "scripts/ci_checks.sh" in _steps(publish["jobs"]["promote-latest"])
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
    assert job["continue-on-error"] is True


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


def test_publish_workflow_concurrency_never_cancels_queued_sha_runs() -> None:
    """F2: cancel-in-progress true on publish-* silently drops SHA tags."""
    concurrency = publish["concurrency"]
    assert concurrency["group"] == "publish-${{ github.ref }}"
    assert concurrency["cancel-in-progress"] is False


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
    """F1 hex validation + F3 ancestry gate for dispatch/backfill ranges."""
    run = next(step for step in publish["jobs"]["plan"]["steps"] if step.get("id") == "plan")["run"]
    assert "not-40-hex" in run
    assert "{40}" in run  # 40-hex regex guard on from_sha/to_sha inputs
    assert "merge-base --is-ancestor" in run
    assert "not-ancestor-of-main" in run
    assert "refs/heads/main" in run  # ancestry checked against the current main head


def test_plan_cap_fails_closed_on_nonpositive_value() -> None:
    """F4: max_shas <= 0 must abort the plan, never bypass the matrix cap."""
    run = next(step for step in publish["jobs"]["plan"]["steps"] if step.get("id") == "plan")["run"]
    assert '[ "$MAX" -le 0 ]' in run
    assert "positive" in run
    assert "-gt 10" in run  # clamp ceiling retained


def test_plan_handles_branch_deletion_push() -> None:
    """F6: all-zero after (branch deletion) yields an empty matrix, not a crash."""
    run = next(step for step in publish["jobs"]["plan"]["steps"] if step.get("id") == "plan")["run"]
    assert "branch-deleted" in run
    zero = "0" * 40
    assert zero in run
    assert 'echo "shas=[]"' in run


def test_promote_gate_does_not_rely_on_publish_result() -> None:
    """F5: continue-on-error makes needs.publish.result vacuous; promote re-checks fully."""
    promote_if = str(publish["jobs"]["promote-latest"]["if"])
    assert "needs.publish.result" not in promote_if
    assert "needs.test.result == 'success'" in promote_if
    assert "!cancelled()" in promote_if
    steps_dump = _steps(publish["jobs"]["promote-latest"])
    assert "scripts/ci_checks.sh" in steps_dump  # the real gate: full per-SHA re-check
    assert "git ls-remote" in steps_dump


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
        )
        assert gate < push  # the gate runs before docker/build-push-action can see the tag
        gate_run = steps[gate]["run"]
        assert "sha-[0-9a-f]{40}" in gate_run if job_name == "publish" else ":latest$" in gate_run
        assert steps[gate]["env"]["IMAGE_TAG"].startswith("${{ needs.plan.outputs.image }}")


def test_publish_tags_flow_from_plan_output_not_shell() -> None:
    """`tags:` is a GitHub-evaluated input; the tag string never reaches a shell body."""
    build = next(
        step
        for step in publish["jobs"]["publish"]["steps"]
        if str(step.get("uses", "")).startswith("docker/build-push-action@")
    )
    assert build["with"]["tags"] == "${{ needs.plan.outputs.image }}:sha-${{ matrix.sha }}"
    promote = next(
        step
        for step in publish["jobs"]["promote-latest"]["steps"]
        if str(step.get("uses", "")).startswith("docker/build-push-action@")
    )
    assert promote["with"]["tags"] == "${{ needs.plan.outputs.image }}:latest"
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
    verifies; unverified tags are re-scheduled, never trusted."""
    plan_run = _plan_run()
    assert "ghcr_tags.py verify" in plan_run
    assert "provenance verification failed" in plan_run
    assert 'select(.value != "published")' in plan_run
    assert "unverified.txt" in plan_run
    assert "--unverified-file unverified.txt" in plan_run
    assert plan_run.count("ghcr_tags.py verify") == 2  # push path + backfill path


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
