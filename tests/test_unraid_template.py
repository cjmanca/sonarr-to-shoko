"""The Unraid template exposes required connection settings and safe opt-in ports."""

from __future__ import annotations

from pathlib import Path

from defusedxml import ElementTree  # type: ignore[import-untyped]

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_PATH = REPOSITORY_ROOT / "unraid.xml"


def test_unraid_template_keeps_dashboard_private_and_state_persistent() -> None:
    assert TEMPLATE_PATH.is_file(), "the Unraid template must be present"
    root = ElementTree.parse(TEMPLATE_PATH).getroot()
    configs = root.findall("Config")
    by_name = {config.attrib["Name"]: config for config in configs}
    by_target = {config.attrib["Target"]: config for config in configs}

    assert root.tag == "Container"
    assert root.attrib["version"] == "2"
    assert root.findtext("Privileged") == "false"
    assert root.findtext("Repository") == "ghcr.io/cjmanca/shoko-sonarr-linker:latest"
    assert root.findtext("ExtraParams") == "--restart unless-stopped"
    assert by_name["Application Data"].attrib["Target"] == "/data"
    assert by_name["Application Data"].attrib["Required"] == "true"
    assert by_name["Application Data"].attrib["Mode"] == "rw"
    assert 'USER 1000:1000' in (REPOSITORY_ROOT / "Dockerfile").read_text(encoding="utf-8")

    for variable in ("SONARR_URL", "SONARR_API_KEY", "SHOKO_URL", "SHOKO_API_KEY"):
        assert variable in by_target
        assert by_target[variable].attrib["Type"] == "Variable"
        assert by_target[variable].attrib["Required"] == "true"
    assert by_target["SONARR_API_KEY"].attrib["Mask"] == "true"
    assert by_target["SHOKO_API_KEY"].attrib["Mask"] == "true"
    assert by_target["SONARR_API_KEY"].attrib["Default"] == ""
    assert by_target["SHOKO_API_KEY"].attrib["Default"] == ""

    dashboard_port = by_target["8080"]
    assert dashboard_port.attrib["Type"] == "Port"
    assert dashboard_port.attrib["Required"] == "false"
    assert dashboard_port.attrib["Default"] == ""
    assert not any("media" in config.attrib["Target"].lower() for config in configs)
    assert not any("docker.sock" in config.attrib["Target"] for config in configs)
