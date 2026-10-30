"""History diagnostics must not turn sanitized text into library identity."""

from dataclasses import replace
from pathlib import Path

import pytest
import test_dashboard as dashboard
from test_dashboard_routes import runtime_app
from test_state import LIMITS, prepare, walk_to_planned

from shoko_linker.logging_setup import diagnostic_text, register_secret
from shoko_linker.onboarding import OnboardingManager
from shoko_linker.paths import PathMapping
from shoko_linker.state import open_state

_SECRET = "history-path-proof-secret"


@pytest.mark.parametrize(
    ("raw_path", "root", "complete"),
    [
        ("/ani me/Show/Ep.mkv", "/ani me", True),
        ("/ani\x00me/Show/Ep.mkv", "/ani me", False),
        ("/ani\x1fme/Show/Ep.mkv", "/ani me", False),
        ("/ani\nme/Show/Ep.mkv", "/ani me", False),
        ("/ani\u202eme/Show/Ep.mkv", "/ani me", False),
        (f"/{_SECRET}/Show/Ep.mkv", "secret-root", False),
        ("/ani me/" + "x" * 8200 + "/Ep.mkv", "/ani me", False),
        (b"/ani me/Show/Ep.mkv", "/ani me", False),
        ("", "/ani me", False),
        ("ani me/Show/Ep.mkv", "/ani me", False),
        ("/ani me/../ani me/Show/Ep.mkv", "/ani me", False),
        ("/ani me\\Show\\Ep.mkv", "/ani me", False),
    ],
    ids=[
        "valid",
        "nul",
        "control",
        "newline",
        "bidi",
        "secret",
        "oversized",
        "nontext",
        "empty",
        "relative",
        "traversal",
        "backslashes",
    ],
)
def test_runtime_history_table_preserves_original_path_proof(
    tmp_path: Path, raw_path: str | bytes, root: str, complete: bool
) -> None:
    app, requests, endpoint = runtime_app(tmp_path)
    register_secret(_SECRET)
    if root == "secret-root":
        root = diagnostic_text(f"/{_SECRET}")
    try:
        with open_state(app.state_path, budget_limits=LIMITS) as store:
            assessment = dashboard.safe_assessment()
            binding = replace(
                dashboard.scope_binding(assessment),
                roots=(root,),
                path_mappings=(PathMapping(root, "/anime"),),
            )
            OnboardingManager(store).confirm_setup(
                binding,
                assessment,
                confirmed_facts=assessment.unverifiable_facts + assessment.unknown_settings,
                automatic_link_consent=True,
                iso=dashboard.T0.isoformat(),
            )
            walk_to_planned(store, file_id=201)
            prepare(store, file_id=201)
            store.connection.execute(
                "UPDATE files SET sonarr_path=? WHERE shoko_file_id=201", (raw_path,)
            )
            before = tuple(store.connection.iterdump())
            assert app.table_provider is not None
            table = app.table_provider("history", 1, 50)
            assert tuple(store.connection.iterdump()) == before
        assert table.history is not None
        assert len(table.history.rows) == len(table.displays) == 1
        row, display = table.history.rows[0], table.displays[0]
        assert row.current_path_complete is complete
        if complete:
            assert display.library == root
            assert display.relative_path == "Show"
        else:
            assert display.library is None
            assert display.relative_path is None
        if isinstance(raw_path, str) and "\x00" in raw_path:
            assert row.current_path == ""  # NUL paths are rejected before hydration.
        assert _SECRET not in repr(table.history)
        assert requests == []
    finally:
        endpoint.client.close()
