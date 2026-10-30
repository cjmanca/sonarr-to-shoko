"""Exercise production resize/poll transitions with Node DOM boundary doubles, not a browser."""

import base64
import hashlib
import json
import re
import shutil
import subprocess
from pathlib import Path

import httpx
import pytest
from test_dashboard import start_server, stop_server
from test_dashboard_routes import login, request_headers, runtime_app

from shoko_linker.dashboard import _DASHBOARD_JS


@pytest.mark.parametrize("storage", ["normal", "corrupt", "denied", "oversized", "hostile"])
def test_resize_and_poll_state(storage: str) -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node is unavailable for the dashboard JavaScript regression")
    fixture = Path(__file__).with_name("dashboard_resize.cjs").read_text(encoding="utf-8")
    result = subprocess.run(  # noqa: S603 - resolved Node, fixed fixture, no shell
        [node, "-"],
        input="const production = "
        + json.dumps(_DASHBOARD_JS)
        + ";\n"
        + "const storageMode = "
        + json.dumps(storage)
        + ";\n"
        + fixture,
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_rendered_resize_handles_and_script_match_csp(tmp_path: Path) -> None:
    app, requests, endpoint = runtime_app(tmp_path)
    server, thread, base_url = start_server(app)
    try:
        response = httpx.get(base_url + "/files/waiting", headers=request_headers(login(app)))
        assert response.status_code == 200
        text = response.text
        for key, label in (
            ("type", "Type"),
            ("filename", "Filename"),
            ("path", "Relative path"),
            ("library", "Library"),
            ("status", "Status"),
        ):
            assert f'data-resize="{key}" tabindex="0" role="separator"' in text
            assert f'aria-label="Resize {label} column"' in text
        script = re.search(r"<script>(.*?)</script>", text, re.DOTALL)
        assert script is not None
        digest = base64.b64encode(hashlib.sha256(script[1].encode()).digest()).decode()
        assert f"script-src 'sha256-{digest}'" in dict(response.headers)["content-security-policy"]
        assert requests == []
    finally:
        stop_server(server, thread)
        endpoint.client.close()
