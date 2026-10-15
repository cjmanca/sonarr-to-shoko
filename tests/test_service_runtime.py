"""Managed service runtime gates cycles on current durable onboarding consent."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path, PurePosixPath
from typing import Any, cast

import httpx
import pytest

from shoko_linker import cli
from shoko_linker.bootstrap import BootstrapSettings
from shoko_linker.config import SecretRef, Settings, load_settings
from shoko_linker.daemon import CycleMode, DaemonDeps
from shoko_linker.http import ApiEndpoint
from shoko_linker.mapping_store import MappingStore
from shoko_linker.onboarding import OnboardingManager, ScopeBinding
from shoko_linker.paths import PathMapping
from shoko_linker.policies import RunPolicy
from shoko_linker.safety_check import assess_settings_payload
from shoko_linker.service import ConnectionReport
from shoko_linker.state import StateStore, open_state

EXAMPLE_CONFIG = Path(__file__).resolve().parents[1] / "config.example.yaml"


def test_worker_runs_service_policy_only_with_current_durable_consent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assessment = assess_settings_payload(
        {
            "FileQualityFilterEnabled": False,
            "Plugins": {
                "Renamer": {
                    "RenameOnImport": False,
                    "MoveOnImport": False,
                    "RelocateOnImport": False,
                }
            },
            "AniDb": {"MyList_AddFiles": False},
        }
    )
    bootstrap = BootstrapSettings(
        "http://sonarr:8989",
        SecretRef("SONARR_API_KEY"),
        "http://shoko:8111",
        SecretRef("SHOKO_API_KEY"),
        cast(Path, PurePosixPath("/data/state.sqlite3")),
    )
    binding = ScopeBinding(
        bootstrap.sonarr_url,
        bootstrap.shoko_url,
        "4.0.0",
        "5.3.3",
        ("sonarr_api_v3_system_status", "shoko_api_v3_init_version"),
        ("/data/media/anime",),
        (PathMapping("/data/media/anime", "/anime"),),
        assessment.digest,
        assessment.unverifiable_facts,
    )
    state_path = tmp_path / "state.sqlite3"
    with open_state(state_path) as store:
        OnboardingManager(store).confirm_setup(
            binding,
            assessment,
            confirmed_facts=assessment.unverifiable_facts,
            automatic_link_consent=True,
            iso="2026-10-15T00:00:00Z",
        )

    runtime_settings = load_settings(EXAMPLE_CONFIG)
    sonarr = ApiEndpoint(httpx.Client(), lambda: {})
    shoko = ApiEndpoint(httpx.Client(), lambda: {})
    mapping_source = ApiEndpoint(httpx.Client(), lambda: {})
    monkeypatch.setattr(cli, "probe_connections", lambda *_: ConnectionReport(True, True, ()))
    monkeypatch.setattr(cli, "settings_for_scope", lambda *_args, **_kwargs: runtime_settings)
    monkeypatch.setattr(cli, "open_mapping_store", lambda _settings: object())
    monkeypatch.setattr(cli, "read_overrides_text", lambda _settings: None)

    def build_dependencies(
        settings: Settings,
        *,
        store: StateStore,
        mapping_store: Any,
        **_kwargs: Any,
    ) -> DaemonDeps:
        return DaemonDeps(
            settings,
            store,
            cast(MappingStore, mapping_store),
            sonarr,
            shoko,
            mapping_source,
        )

    monkeypatch.setattr(cli, "build_daemon_deps", build_dependencies)
    captured: dict[str, Any] = {}

    def run_one_cycle(deps: DaemonDeps, **kwargs: Any) -> object:
        captured["deps"] = deps
        captured.update(kwargs)
        captured["authorized_initially"] = deps.service_authorized()
        current_setup.available = False
        captured["authorized_after_revocation"] = deps.service_authorized()
        return object()

    monkeypatch.setattr(cli, "run_daemon", run_one_cycle)
    runtime = cli._NormalServiceRuntime(
        replace(bootstrap, database_path=state_path),
        sonarr,
        shoko,
        cli.StopState(),
    )

    class CurrentSetup:
        available = True

        def current_context(self, _store: StateStore) -> tuple[ScopeBinding, Any] | None:
            return (binding, assessment) if self.available else None

    current_setup = CurrentSetup()
    runtime.setup = cast(Any, current_setup)
    try:
        runtime.worker_tick()
        assert captured["policy"] is RunPolicy.SERVICE
        assert captured["mode"] is CycleMode.APPLY
        assert captured["stop_after_cycles"] == 1
        deps = cast(DaemonDeps, captured["deps"])
        assert deps.service_mode is True
        assert captured["authorized_initially"] is True
        assert captured["authorized_after_revocation"] is False
    finally:
        sonarr.client.close()
        shoko.client.close()
        mapping_source.client.close()
