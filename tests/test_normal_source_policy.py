"""Normal fresh-source authorization stays independent of advanced inventory approval."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
import test_daemon as td
import test_onboarding as to

from shoko_linker import cli, daemon
from shoko_linker.bootstrap import BootstrapSettings
from shoko_linker.config import SecretRef
from shoko_linker.onboarding import (
    OnboardingContext,
    OnboardingManager,
    required_confirmation_facts,
)
from shoko_linker.paths import PathMapping
from shoko_linker.policies import RunPolicy
from shoko_linker.rate_control import RateState, encode_rate_state
from shoko_linker.shoko import collect_shoko_discovery
from shoko_linker.sonarr import collect_sonarr_inventory
from shoko_linker.state import (
    META_NORMAL_SOURCE_OWNERSHIP,
    META_SERVICE_AUTHORIZATION_REVISION,
    META_SERVICE_SETTINGS,
    CampaignBinding,
    Cohort,
    FileState,
    SendToken,
    SourceObservation,
    SourceRebaselineAssessment,
    SourceRebaselineBindings,
    StateError,
    StateStore,
)

SECOND_ROOT = "/sonarr/second-anime"


def confirmed_deps(tmp_path: Path) -> tuple[daemon.DaemonDeps, td.ShokoStub, list[float]]:
    files = [
        td.SpecFile(series_id=1, file_id=10, shoko_id=500),
        td.SpecFile(series_id=1, file_id=11, shoko_id=501),
    ]
    td.activate_mapping(tmp_path, td.mapping_xml())
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, files)
    settings = td.make_settings(tmp_path)
    settings = replace(
        settings,
        sonarr=replace(settings.sonarr, allowed_roots=(td.SONARR_ROOT, SECOND_ROOT)),
        path_mappings=(*settings.path_mappings, PathMapping(SECOND_ROOT, "/shoko/second-anime")),
        shoko=replace(settings.shoko, allowed_roots=(td.SHOKO_ROOT, "/shoko/second-anime")),
    )
    shoko = td.ShokoStub(files)
    clock = [0.0]
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=files,
        shoko_files=files,
        xml=td.mapping_xml(),
        settings=settings,
        shoko=shoko,
        monotonic=lambda: clock[0],
        advance=lambda _: None,
        now=lambda: td.T1,
    )
    assessment = to.safe_assessment()
    binding = replace(
        to.make_binding(assessment),
        sonarr_url=settings.sonarr.url,
        shoko_url=settings.shoko.url,
        roots=tuple(sorted(settings.sonarr.allowed_roots)),
        path_mappings=settings.path_mappings,
    )
    record = OnboardingManager(store).confirm_setup(
        binding,
        assessment,
        confirmed_facts=required_confirmation_facts(binding, assessment),
        automatic_link_consent=True,
        iso=td.T0,
    )
    context = OnboardingContext(
        binding,
        assessment,
        record.to_json(),
        store.get_service_authorization_revision(),
        settings_json=store.get_meta(META_SERVICE_SETTINGS),
    )
    deps = replace(
        deps, service_mode=True, service_authorized=lambda: True, service_context=lambda: context
    )
    return deps, shoko, clock


def close(deps: daemon.DaemonDeps) -> None:
    deps.store.close()
    for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
        endpoint.client.close()


def normal_cycle(deps: daemon.DaemonDeps, iso: str = td.T1) -> daemon.CycleReport:
    return daemon.run_cycle(
        deps,
        now_iso=iso,
        mode=daemon.CycleMode.APPLY,
        cycle_key=deps.store.begin_cycle("normal", iso),
        policy=RunPolicy.SERVICE,
    )


def complete_proof(deps: daemon.DaemonDeps) -> daemon.DaemonDeps:
    return daemon._bind_normal_source_proof(
        deps,
        collect_sonarr_inventory(deps.sonarr, deps.settings),
        collect_shoko_discovery(deps.shoko),
        deadline=100.0,
    )


def test_count_76_to_69_to_8124_needs_no_reapproval_and_preserves_advanced_hold(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deps, shoko, _clock = confirmed_deps(tmp_path)
    store = deps.store
    bootstrap = BootstrapSettings(
        td.SONARR_BASE,
        SecretRef("SONARR_API_KEY"),
        td.SHOKO_BASE,
        SecretRef("SHOKO_API_KEY"),
        store.path,
    )
    runtime = cli._NormalServiceRuntime(bootstrap, deps.sonarr, deps.shoko, cli.StopState())
    consent = store.get_onboarding_json()
    source = daemon._source_identity(deps.settings)
    store.record_source_observation(
        source,
        "a" * 64,
        iso=td.T0,
        roots_fingerprint=daemon.roots_fingerprint(deps.settings),
        discovery_total=76,
    )
    anchor = store.get_meta("source_last_discovery_total")
    try:
        # Independent manual collection changes are genuinely enumerated over HTTP pages.
        for ident in range(1000, 1067):
            shoko.files[ident] = td.SpecFile(series_id=999, file_id=ident, shoko_id=ident)
        monkeypatch.setattr(cli, "current_iso", lambda: td.T1)
        runtime._run_daemon_cycle(deps, store, deps.settings)
        assert runtime.status().automatic_linking
        assert shoko.posts_count() == 1
        assert store.get_meta("requires_rebaseline") == "1"
        hold = store.get_meta("source_continuity_hold_v1")
        ownership = store.get_meta(META_NORMAL_SOURCE_OWNERSHIP)
        shoko.files = {
            501: shoko.files[501],
            **{
                ident: td.SpecFile(series_id=999, file_id=ident, shoko_id=ident)
                for ident in range(1000, 9123)
            },
        }
        monkeypatch.setattr(cli, "current_iso", lambda: td.T2)
        runtime._run_daemon_cycle(deps, store, deps.settings)
        assert runtime.status().automatic_linking
        assert shoko.posts_count() == 2
        assert store.get_onboarding_json() == consent
        assert store.get_meta("source_continuity_hold_v1") == hold
        assert store.get_meta("source_last_discovery_total") == anchor
        assert store.get_meta(META_NORMAL_SOURCE_OWNERSHIP) == ownership
        assert not daemon.compute_eligibility(replace(deps, service_mode=False), td.T2).eligible
        assert not daemon.compute_eligibility(deps, td.T2, campaign=True).eligible
        # Durable marker and saved consent survive reopening without a content approval.
        path = store.path
        store.close()
        reopened = StateStore(path, budget_limits=store.budget_limits)
        deps = replace(deps, store=reopened)
        assert reopened.get_meta(META_NORMAL_SOURCE_OWNERSHIP) == ownership
        assert normal_cycle(deps, td.T2).apply_eligible
    finally:
        close(deps)


@pytest.mark.parametrize(
    "change", ["consent", "revision", "settings", "ownership", "pause", "expiry"]
)
def test_source_proof_fails_closed_when_its_local_binding_changes(
    tmp_path: Path, change: str
) -> None:
    deps, _shoko, clock = confirmed_deps(tmp_path)
    try:
        deps.store.set_meta("requires_rebaseline", "1")
        proven = complete_proof(deps)
        assert daemon._normal_source_current(proven)
        assert "source_unstable" in daemon.compute_eligibility(proven, td.T1, campaign=True).reasons
        if change == "consent":
            deps.store.save_onboarding_json("{}", td.T1)
        elif change == "revision":
            deps.store.set_meta(META_SERVICE_AUTHORIZATION_REVISION, "a" * 32)
        elif change == "settings":
            deps.store.set_meta(META_SERVICE_SETTINGS, "{}")
        elif change == "ownership":
            deps.store.set_meta(
                META_NORMAL_SOURCE_OWNERSHIP,
                json.dumps({"source_identity": "other", "send_floor": 0}),
            )
        elif change == "pause":
            OnboardingManager(deps.store).pause(iso=td.T1)
        else:
            clock[0] = 100.0
        assert not daemon._normal_source_current(proven)
        assert not daemon.compute_eligibility(proven, td.T1).eligible
    finally:
        close(deps)


def test_missing_context_or_incomplete_inventory_never_grants_normal_proof(tmp_path: Path) -> None:
    deps, _shoko, _clock = confirmed_deps(tmp_path)
    try:
        deps.store.set_meta("requires_rebaseline", "1")
        assert not daemon.compute_eligibility(deps, td.T1).eligible
        assert (
            complete_proof(replace(deps, service_context=lambda: None)).normal_source_proof is None
        )
        inventory = collect_sonarr_inventory(deps.sonarr, deps.settings)
        discovery = collect_shoko_discovery(deps.shoko)
        incomplete = replace(discovery, verdict=replace(discovery.verdict, complete=False))
        assert (
            daemon._bind_normal_source_proof(
                deps, inventory, incomplete, deadline=100.0
            ).normal_source_proof
            is None
        )
    finally:
        close(deps)


def test_unknown_legacy_attempts_are_fenced_before_recovery_without_trapping_fresh_work(
    tmp_path: Path,
) -> None:
    deps, shoko, _clock = confirmed_deps(tmp_path)
    try:
        # A pre-marker process could have sent this to an unidentified endpoint.
        shoko.link_on_post = False
        legacy = replace(deps, service_context=None)
        assert normal_cycle(legacy).new_arrival_submitted == 1
        retained = deps.store.get_file(500)
        assert retained is not None
        assert retained.state is FileState.VERIFYING
        assert deps.store.get_service_canary() == (500, "pending")
        shoko.requests.clear()
        deps.store.set_meta("source_instance", "unidentified-legacy-server")
        assert daemon._normal_source_context(deps) is not None
        assert deps.store.get_service_canary() is None
        reasons = daemon._recovery_phase(deps, td.T1, 0.0)
        assert "recovery_source_ownership_unknown:500" in reasons
        assert not any(request.url.path == "/api/v3/File/500" for request in shoko.requests)
        retained = deps.store.get_file(500)
        assert retained is not None
        assert retained.state is FileState.VERIFYING
        assert deps.store.budget_view().pending_file_requests == 1
        shoko.link_on_post = True
        current = normal_cycle(deps, td.T2)
        assert current.new_arrival_submitted == 1
        assert deps.store.get_service_canary() == (501, "verified")
        retained = deps.store.get_file(500)
        assert retained is not None
        assert retained.state is FileState.VERIFYING
        assert deps.store.budget_view().pending_file_requests == 1
    finally:
        close(deps)


def test_normal_runtime_status_does_not_require_advanced_count_acceptance(tmp_path: Path) -> None:
    deps, _shoko, _clock = confirmed_deps(tmp_path)
    bootstrap = BootstrapSettings(
        td.SONARR_BASE,
        SecretRef("SONARR_API_KEY"),
        td.SHOKO_BASE,
        SecretRef("SHOKO_API_KEY"),
        tmp_path / "state.sqlite3",
    )
    runtime = cli._NormalServiceRuntime(bootstrap, deps.sonarr, deps.shoko, cli.StopState())
    try:
        deps.store.set_meta("requires_rebaseline", "1")
        allowed, message = runtime._operational_status(
            deps.store,
            authorized=True,
            next_action="Automatic linking is processing eligible files.",
        )
        assert allowed
        assert "Review source" not in message
    finally:
        close(deps)


def test_endpoint_transition_fences_old_attempt_and_starts_a_new_canary(tmp_path: Path) -> None:
    deps, shoko, _clock = confirmed_deps(tmp_path)
    try:
        shoko.link_on_post = False
        assert normal_cycle(deps).new_arrival_submitted == 1
        old_marker = deps.store.get_meta(META_NORMAL_SOURCE_OWNERSHIP)
        assert deps.store.get_service_canary() == (500, "pending")
        changed_settings = replace(
            deps.settings, shoko=replace(deps.settings.shoko, url="http://new-shoko:8111")
        )
        old_endpoint = deps.shoko
        deps = replace(
            deps,
            settings=changed_settings,
            shoko=td.endpoint_for(shoko.handler, "http://new-shoko:8111"),
        )
        old_endpoint.client.close()
        # Endpoint settings alone cannot authorize a transition.
        assert daemon._normal_source_context(deps) is None
        assert deps.store.get_meta(META_NORMAL_SOURCE_OWNERSHIP) == old_marker
        assessment = to.safe_assessment()
        old_record = OnboardingManager(deps.store).record()
        assert old_record is not None
        binding = replace(old_record.binding, shoko_url="http://new-shoko:8111")
        record = OnboardingManager(deps.store).confirm_setup(
            binding,
            assessment,
            confirmed_facts=required_confirmation_facts(binding, assessment),
            automatic_link_consent=True,
            iso=td.T1,
        )
        context = OnboardingContext(
            binding,
            assessment,
            record.to_json(),
            deps.store.get_service_authorization_revision(),
            settings_json=deps.store.get_meta(META_SERVICE_SETTINGS),
        )
        deps = replace(deps, service_context=lambda: context)
        assert daemon._normal_source_context(deps) is not None
        assert deps.store.get_service_canary() is None
        shoko.requests.clear()
        reasons = daemon._recovery_phase(deps, td.T1, 0.0)
        assert "recovery_source_ownership_unknown:500" in reasons
        assert not any(request.url.path == "/api/v3/File/500" for request in shoko.requests)
        shoko.link_on_post = True
        assert normal_cycle(deps, td.T2).new_arrival_submitted == 1
        assert deps.store.get_service_canary() == (501, "verified")
        retained = deps.store.get_file(500)
        assert retained is not None
        assert retained.state is FileState.VERIFYING
        assert deps.store.budget_view().pending_file_requests == 1
        assert deps.store.get_meta(META_NORMAL_SOURCE_OWNERSHIP) != old_marker
    finally:
        close(deps)


@pytest.mark.parametrize("boundary", ["prepare_send", "mark_sent", "protect_transport"])
def test_normal_file_proof_is_rechecked_inside_each_send_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    deps, shoko, _clock = confirmed_deps(tmp_path)
    try:
        deps.store.set_meta("requires_rebaseline", "1")
        original: Callable[..., Any] = getattr(deps.store, boundary)

        def pause_before_transaction(*args: Any, **kwargs: Any) -> Any:
            OnboardingManager(deps.store).pause(iso=td.T1)
            return original(*args, **kwargs)

        monkeypatch.setattr(deps.store, boundary, pause_before_transaction)
        with pytest.raises(StateError, match="normal source authorization changed"):
            normal_cycle(deps)
        assert shoko.posts_count() == 0
        assert deps.store.get_meta("requires_rebaseline") == "1"
    finally:
        close(deps)


def test_scope_change_uses_existing_onboarding_confirmation_once(tmp_path: Path) -> None:
    deps, shoko, _clock = confirmed_deps(tmp_path)
    store = deps.store
    try:
        source = daemon._source_identity(deps.settings)
        store.record_source_observation(
            source,
            "a" * 64,
            iso=td.T0,
            roots_fingerprint=daemon.roots_fingerprint(deps.settings),
            discovery_total=2,
        )
        assert daemon._normal_source_context(deps) is not None
        marker = store.get_meta(META_NORMAL_SOURCE_OWNERSHIP)
        assessment = to.safe_assessment()
        record = OnboardingManager(store).record()
        assert record is not None
        settings = replace(
            deps.settings,
            sonarr=replace(deps.settings.sonarr, allowed_roots=(td.SONARR_ROOT,)),
            path_mappings=tuple(
                m for m in deps.settings.path_mappings if m.sonarr == td.SONARR_ROOT
            ),
            shoko=replace(deps.settings.shoko, allowed_roots=(td.SHOKO_ROOT,)),
        )
        binding = replace(
            record.binding, roots=(td.SONARR_ROOT,), path_mappings=settings.path_mappings
        )
        record = OnboardingManager(store).confirm_setup(
            binding,
            assessment,
            confirmed_facts=required_confirmation_facts(binding, assessment),
            automatic_link_consent=True,
            iso=td.T1,
        )
        context = OnboardingContext(
            binding,
            assessment,
            record.to_json(),
            store.get_service_authorization_revision(),
            settings_json=store.get_meta(META_SERVICE_SETTINGS),
        )
        deps = replace(deps, settings=settings, service_context=lambda: context)
        assert normal_cycle(deps).new_arrival_submitted == 1
        assert normal_cycle(deps, td.T2).new_arrival_submitted == 1
        assert store.get_meta("requires_rebaseline") == "1"
        assert store.get_meta(META_NORMAL_SOURCE_OWNERSHIP) == marker
        assert store.get_onboarding_json() == record.to_json()
        assert shoko.posts_count() == 2
    finally:
        close(deps)


def test_same_source_legacy_count_hold_fences_unknown_attempt_but_allows_fresh_work(
    tmp_path: Path,
) -> None:
    deps, shoko, _clock = confirmed_deps(tmp_path)
    try:
        shoko.link_on_post = False
        assert normal_cycle(replace(deps, service_context=None)).new_arrival_submitted == 1
        source = daemon._source_identity(deps.settings)
        deps.store.record_source_observation(source, "a" * 64, iso=td.T0, discovery_total=76)
        deps.store.check_source_continuity(source, "b" * 64, iso=td.T1, discovery_total=69)
        saved_consent = deps.store.get_onboarding_json()
        hold = deps.store.get_meta("source_continuity_hold_v1")
        assert daemon._normal_source_context(deps) is not None
        assert not deps.store.normal_source_send_owned(500)
        assert deps.store.get_service_canary() is None
        shoko.materialize(500)
        shoko.requests.clear()
        reasons = daemon._recovery_phase(deps, td.T1, 0.0)
        assert "recovery_source_ownership_unknown:500" in reasons
        assert not any(request.url.path == "/api/v3/File/500" for request in shoko.requests)
        shoko.link_on_post = True
        assert normal_cycle(deps, td.T2).new_arrival_submitted == 1
        assert deps.store.get_service_canary() == (501, "verified")
        retained = deps.store.get_file(500)
        assert retained is not None
        assert retained.state is FileState.VERIFYING
        assert deps.store.budget_view().pending_file_requests == 1
        assert deps.store.get_onboarding_json() == saved_consent
        assert deps.store.get_meta("source_continuity_hold_v1") == hold
        assert deps.store.get_meta("requires_rebaseline") == "1"
    finally:
        close(deps)


def test_legacy_boolean_service_authorization_cannot_bypass_advanced_hold(tmp_path: Path) -> None:
    deps, shoko, _clock = confirmed_deps(tmp_path)
    try:
        deps.store.set_meta("requires_rebaseline", "1")
        legacy = replace(deps, service_context=None, service_authorized=lambda: True)
        report = normal_cycle(legacy)
        assert not report.apply_eligible
        assert report.new_arrival_submitted == 0
        assert "source_unstable" in report.reasons
        assert shoko.posts_count() == 0
        assert deps.store.get_meta("requires_rebaseline") == "1"
        assert deps.store.get_meta(META_NORMAL_SOURCE_OWNERSHIP) is None
    finally:
        close(deps)


def _clear_advanced_transition_hold(deps: daemon.DaemonDeps, source: str, clear_path: str) -> None:
    if clear_path == "clear_requires_rebaseline":
        deps.store.clear_requires_rebaseline(td.T1)
    elif clear_path == "record_source_observation":
        deps.store.record_source_observation(
            source,
            "b" * 64,
            iso=td.T1,
            discovery_total=2,
            roots_fingerprint=daemon.roots_fingerprint(deps.settings),
        )
    else:
        deps.store.create_dashboard_account("operator", "scrypt$test", b"s" * 32, td.T0)
        deps.store.create_dashboard_session("a" * 64, "b" * 64, 2_000_000_000, td.T0)
        observation = SourceObservation(
            source, daemon.roots_fingerprint(deps.settings), "b" * 64, 2
        )
        baseline_assessment = SourceRebaselineAssessment(observation, "c" * 64, td.T1, td.T1)
        bindings = SourceRebaselineBindings(
            "d" * 64, "e" * 64, deps.store.get_source_rebaseline_local_binding()
        )
        hold = deps.store.get_source_continuity_review().hold
        assert hold is not None
        prepared = deps.store.prepare_source_rebaseline(
            baseline_assessment,
            expected_hold_id=hold.hold_id,
            bindings=bindings,
            owner_session_sha256="a" * 64,
            iso=td.T1,
            expires_iso=(td._parse(td.T1) + timedelta(seconds=120)).isoformat(),
        )
        fresh = (td._parse(td.T1) + timedelta(seconds=1)).isoformat()
        deps.store.confirm_source_rebaseline(
            prepared.token,
            replace(baseline_assessment, started_iso=fresh, completed_iso=fresh),
            bindings=bindings,
            owner_session_sha256="a" * 64,
            iso=fresh,
        )


@pytest.mark.parametrize(
    "clear_path",
    ["clear_requires_rebaseline", "record_source_observation", "confirm_source_rebaseline"],
)
@pytest.mark.parametrize("legacy_verified", [False, True])
def test_cleared_advanced_transition_cannot_adopt_pre_marker_attempts(
    tmp_path: Path,
    clear_path: str,
    legacy_verified: bool,
) -> None:
    deps, shoko, _clock = confirmed_deps(tmp_path)
    try:
        shoko.link_on_post = legacy_verified
        assert normal_cycle(replace(deps, service_context=None)).new_arrival_submitted == 1
        before = deps.store.connection.execute(
            "SELECT outcome,verification,slot_released FROM sends WHERE shoko_file_id=500"
        ).fetchone()
        changed_settings = replace(
            deps.settings, shoko=replace(deps.settings.shoko, url="http://new-shoko:8111")
        )
        old_endpoint = deps.shoko
        deps = replace(
            deps,
            settings=changed_settings,
            shoko=td.endpoint_for(shoko.handler, "http://new-shoko:8111"),
        )
        old_endpoint.client.close()
        source = daemon._source_identity(deps.settings)
        deps.store.check_source_continuity(
            source,
            "b" * 64,
            iso=td.T1,
            discovery_total=2,
            roots_fingerprint=daemon.roots_fingerprint(deps.settings),
        )
        assert deps.store.get_meta("requires_rebaseline") == "1"
        _clear_advanced_transition_hold(deps, source, clear_path)
        assert deps.store.get_meta("requires_rebaseline") == "0"
        assessment = to.safe_assessment()
        record = OnboardingManager(deps.store).record()
        assert record is not None
        binding = replace(record.binding, shoko_url="http://new-shoko:8111")
        record = OnboardingManager(deps.store).confirm_setup(
            binding,
            assessment,
            confirmed_facts=required_confirmation_facts(binding, assessment),
            automatic_link_consent=True,
            iso=td.T1,
        )
        context = OnboardingContext(
            binding,
            assessment,
            record.to_json(),
            deps.store.get_service_authorization_revision(),
            settings_json=deps.store.get_meta(META_SERVICE_SETTINGS),
        )
        deps = replace(deps, service_context=lambda: context)
        deps.store.save_service_rate_state_json(
            encode_rate_state(
                RateState(
                    cap=20,
                    successful_streak=5,
                    updated_iso=td.T1,
                    last_reason="old endpoint success",
                )
            ),
            td.T1,
        )
        assert daemon._normal_source_context(deps) is not None
        marker = deps.store.get_meta(META_NORMAL_SOURCE_OWNERSHIP)
        deps.store.record_source_observation(source, "b" * 64, iso=td.T1, discovery_total=2)
        assert daemon._normal_source_context(deps) is not None
        assert deps.store.get_meta(META_NORMAL_SOURCE_OWNERSHIP) == marker
        assert daemon._load_service_rate_state(deps, td.T1).cap == 1
        assert not deps.store.normal_source_send_owned(500)
        assert deps.store.get_service_canary() is None
        shoko.requests.clear()
        daemon._recovery_phase(deps, td.T1, 0.0)
        assert not any(request.url.path == "/api/v3/File/500" for request in shoko.requests)
        assert (
            deps.store.connection.execute(
                "SELECT outcome,verification,slot_released FROM sends WHERE shoko_file_id=500"
            ).fetchone()
            == before
        )
        shoko.link_on_post = True
        assert normal_cycle(deps, td.T2).new_arrival_submitted == 1
        assert not any(request.url.path == "/api/v3/File/500" for request in shoko.requests)
        assert deps.store.get_service_canary() == (501, "verified")
        assert (
            deps.store.connection.execute(
                "SELECT outcome,verification,slot_released FROM sends WHERE shoko_file_id=500"
            ).fetchone()
            == before
        )
        assert deps.store.budget_view().pending_file_requests == (0 if legacy_verified else 1)
    finally:
        close(deps)


def test_established_normal_marker_keeps_current_attempt_ownership_across_restart(
    tmp_path: Path,
) -> None:
    deps, shoko, _clock = confirmed_deps(tmp_path)
    try:
        shoko.link_on_post = False
        assert normal_cycle(deps).new_arrival_submitted == 1
        marker = deps.store.get_meta(META_NORMAL_SOURCE_OWNERSHIP)
        assert deps.store.normal_source_send_owned(500)
        old_store = deps.store
        path = old_store.path
        limits = old_store.budget_limits
        old_store.close()
        deps = replace(deps, store=StateStore(path, budget_limits=limits))
        assert daemon._normal_source_context(deps) is not None
        assert deps.store.get_meta(META_NORMAL_SOURCE_OWNERSHIP) == marker
        assert deps.store.normal_source_send_owned(500)
        shoko.materialize(500)
        reasons = daemon._recovery_phase(deps, td.T1, 0.0)
        assert "recovery_poll:500:verified" in reasons
        assert deps.store.get_service_canary() == (500, "pending")
        assert daemon._refresh_service_canary(deps, td.T1) == (500, "verified")
        assert deps.store.budget_view().pending_file_requests == 0
    finally:
        close(deps)


def test_legacy_unsent_plans_migrate_without_inventory_acceptance(tmp_path: Path) -> None:
    deps, shoko, _clock = confirmed_deps(tmp_path)
    try:
        daemon.run_cycle(
            replace(deps, service_context=None),
            now_iso=td.T0,
            mode=daemon.CycleMode.DRY_RUN,
            cycle_key=deps.store.begin_cycle("dry", td.T0),
            policy=RunPolicy.SERVICE,
        )
        planned = deps.store.get_file(500)
        assert planned is not None
        assert planned.state is FileState.PLANNED
        assert deps.store.connection.execute("SELECT COUNT(*) FROM sends").fetchone()[0] == 0
        saved_consent = deps.store.get_onboarding_json()
        deps.store.set_meta("requires_rebaseline", "1")
        assert normal_cycle(deps).new_arrival_submitted == 1
        assert deps.store.get_service_canary() == (500, "verified")
        assert deps.store.get_onboarding_json() == saved_consent
        assert deps.store.get_meta("requires_rebaseline") == "1"
        assert shoko.posts_count() == 1
    finally:
        close(deps)


def test_advanced_future_send_at_other_endpoint_never_becomes_normal_owned(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    deps, shoko, _clock = confirmed_deps(tmp_path)
    advanced_endpoints = []
    try:
        assert daemon._normal_source_context(deps) is not None
        marker = deps.store.get_meta(META_NORMAL_SOURCE_OWNERSHIP)
        added = td.SpecFile(series_id=1, file_id=12, shoko_id=599)
        shoko.files[599] = added
        sonarr = td.SonarrStub([*shoko.files.values()])
        advanced_sonarr = td.endpoint_for(sonarr.handler, td.SONARR_BASE)
        advanced_shoko = td.endpoint_for(shoko.handler, "http://advanced-shoko:8111")
        advanced_endpoints = [advanced_sonarr, advanced_shoko]
        advanced = replace(
            deps,
            service_mode=False,
            service_context=None,
            normal_source_proof=None,
            sonarr=advanced_sonarr,
            shoko=advanced_shoko,
            settings=replace(
                deps.settings, shoko=replace(deps.settings.shoko, url="http://advanced-shoko:8111")
            ),
        )
        shoko.link_on_post = False
        report = daemon.run_cycle(
            advanced,
            now_iso=td.T1,
            mode=daemon.CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key=deps.store.begin_cycle("advanced", td.T1),
        )
        assert report.new_arrival_submitted == 1, report.reasons
        send = deps.store.active_send_details(599)
        assert send is not None
        assert send.send_id > json.loads(marker or "{}")["send_floor"]
        assert deps.store.get_meta(f"normal_send_owner_v1:{send.send_id}") is None
        assert not deps.store.normal_source_send_owned(599)
        assert daemon._normal_source_context(deps) is not None
        shoko.requests.clear()
        reasons = daemon._recovery_phase(deps, td.T1, 0.0)
        assert "recovery_source_ownership_unknown:599" in reasons
        assert not any(request.url.path == "/api/v3/File/599" for request in shoko.requests)
        assert deps.store.get_service_canary() is None
        assert deps.store.budget_view().pending_file_requests == 1
        shoko.link_on_post = True
        assert normal_cycle(deps, td.T2).new_arrival_submitted == 1
        assert deps.store.normal_source_send_owned(500)
        assert not deps.store.normal_source_send_owned(599)
        assert not any(request.url.path == "/api/v3/File/599" for request in shoko.requests)
        assert deps.store.get_meta("requires_rebaseline") == "1"
    finally:
        for endpoint in advanced_endpoints:
            endpoint.client.close()
        close(deps)


def _seed_planned_send_file(store: StateStore) -> None:
    store.record_file(
        599,
        sonarr_series_id=1,
        sonarr_file_id=12,
        sonarr_path=f"{td.SONARR_ROOT}/series1/599.mkv",
        sonarr_size=td.FILE_SIZE,
        cohort=Cohort.NEW_ARRIVAL,
        iso=td.T0,
    )
    for state in (FileState.MATCHED, FileState.MAPPED, FileState.PLANNED):
        store.advance(599, state, "ownership fixture", iso=td.T0)


def test_future_campaign_send_has_no_normal_owner_tag(tmp_path: Path) -> None:
    deps, _shoko, _clock = confirmed_deps(tmp_path)
    try:
        assert daemon._normal_source_context(deps) is not None
        _seed_planned_send_file(deps.store)
        deps.store.create_campaign(
            "advanced",
            "a" * 64,
            max_submissions=1,
            deadline_iso=None,
            attestation_scope="campaign:advanced",
            approval_iso=td.T0,
            iso=td.T0,
            bindings=(CampaignBinding(599, "a" * 64, "b" * 64),),
        )
        cycle_key = deps.store.begin_cycle("campaign", td.T1)
        send = deps.store.prepare_send(
            599,
            "a" * 64,
            "b" * 64,
            targets_digest="c" * 64,
            structure_digest="d" * 64,
            mapping_revision="1",
            cycle_key=cycle_key,
            campaign_id="advanced",
            iso=td.T1,
        )
        assert isinstance(send, SendToken)
        deps.store.mark_sent(send, iso=td.T1)
        assert deps.store.get_meta(f"normal_send_owner_v1:{send.send_id}") is None
        assert not deps.store.normal_source_send_owned(599)
        with pytest.raises(StateError, match="current normal-owned send"):
            deps.store.record_service_canary(599, td.T1, send_id=send.send_id)
    finally:
        close(deps)


@pytest.mark.parametrize("damage", ["missing", "corrupt", "wrong_epoch"])
def test_damaged_normal_send_owner_blocks_recovery_and_canary_credit(
    tmp_path: Path, damage: str
) -> None:
    deps, shoko, _clock = confirmed_deps(tmp_path)
    try:
        shoko.link_on_post = False
        assert normal_cycle(deps).new_arrival_submitted == 1
        send = deps.store.active_send_details(500)
        assert send is not None
        key = f"normal_send_owner_v1:{send.send_id}"
        assert deps.store.get_meta(key) is not None
        if damage == "missing":
            deps.store.connection.execute("DELETE FROM meta WHERE key=?", (key,))
            deps.store.connection.commit()
        else:
            deps.store.set_meta(key, "not-a-digest" if damage == "corrupt" else "f" * 64)
        assert not deps.store.normal_source_send_owned(500)
        assert deps.store.get_service_canary() is None
        shoko.materialize(500)
        shoko.requests.clear()
        reasons = daemon._recovery_phase(deps, td.T1, 0.0)
        assert "recovery_source_ownership_unknown:500" in reasons
        assert not any(request.url.path == "/api/v3/File/500" for request in shoko.requests)
        assert deps.store.budget_view().pending_file_requests == 1
    finally:
        close(deps)


def test_failed_prepare_rolls_back_normal_send_owner_tag_and_budget(tmp_path: Path) -> None:
    deps, _shoko, _clock = confirmed_deps(tmp_path)
    try:
        proven = complete_proof(deps)
        _seed_planned_send_file(deps.store)
        key = deps.store.begin_cycle("rollback", td.T1)
        deps.store.connection.execute(
            "CREATE TRIGGER reject_prepared BEFORE UPDATE ON files "
            "WHEN NEW.state='prepared_not_sent' BEGIN SELECT RAISE(ABORT,'forced rollback'); END"
        )
        deps.store.connection.commit()
        with pytest.raises(StateError):
            deps.store.prepare_send(
                599,
                "a" * 64,
                "b" * 64,
                targets_digest="c" * 64,
                structure_digest="d" * 64,
                mapping_revision="1",
                cycle_key=key,
                iso=td.T1,
                normal_source_proof=proven.normal_source_proof,
            )
        assert deps.store.connection.execute("SELECT COUNT(*) FROM sends").fetchone()[0] == 0
        assert (
            deps.store.connection.execute(
                "SELECT COUNT(*) FROM meta WHERE key LIKE 'normal_send_owner_v1:%'"
            ).fetchone()[0]
            == 0
        )
        assert deps.store.budget_view().pending_file_requests == 0
    finally:
        close(deps)


@pytest.mark.parametrize("attempted", [False, True])
def test_endpoint_a_b_a_uses_distinct_epoch_and_never_reuses_old_tags_or_rate(
    tmp_path: Path,
    attempted: bool,
) -> None:
    deps, shoko, _clock = confirmed_deps(tmp_path)

    def transition(url: str) -> None:
        nonlocal deps
        old_endpoint = deps.shoko
        record = OnboardingManager(deps.store).record()
        assert record is not None
        assessment = to.safe_assessment()
        binding = replace(record.binding, shoko_url=url)
        record = OnboardingManager(deps.store).confirm_setup(
            binding,
            assessment,
            confirmed_facts=required_confirmation_facts(binding, assessment),
            automatic_link_consent=True,
            iso=td.T1,
        )
        context = OnboardingContext(
            binding,
            assessment,
            record.to_json(),
            deps.store.get_service_authorization_revision(),
            settings_json=deps.store.get_meta(META_SERVICE_SETTINGS),
        )
        deps = replace(
            deps,
            shoko=td.endpoint_for(shoko.handler, url),
            settings=replace(deps.settings, shoko=replace(deps.settings.shoko, url=url)),
            service_context=lambda: context,
        )
        old_endpoint.client.close()
        assert daemon._normal_source_context(deps) is not None

    try:
        assert daemon._normal_source_context(deps) is not None
        if attempted:
            shoko.link_on_post = False
            assert normal_cycle(deps).new_arrival_submitted == 1
            assert deps.store.normal_source_send_owned(500)
        original = deps.store.get_meta(META_NORMAL_SOURCE_OWNERSHIP)
        daemon._load_service_rate_state(deps, td.T1)
        deps.store.save_service_rate_state_json(
            encode_rate_state(
                RateState(
                    cap=20, successful_streak=5, updated_iso=td.T1, last_reason="old epoch success"
                )
            ),
            td.T1,
        )
        transition("http://new-shoko:8111")
        transition(td.SHOKO_BASE)
        assert deps.store.get_meta(META_NORMAL_SOURCE_OWNERSHIP) != original
        assert daemon._load_service_rate_state(deps, td.T1).cap == 1
        assert deps.store.get_service_canary() is None
        if attempted:
            assert not deps.store.normal_source_send_owned(500)
            shoko.requests.clear()
            reasons = daemon._recovery_phase(deps, td.T1, 0.0)
            assert "recovery_source_ownership_unknown:500" in reasons
            assert not any(request.url.path == "/api/v3/File/500" for request in shoko.requests)
            assert deps.store.budget_view().pending_file_requests == 1
    finally:
        close(deps)


@pytest.mark.parametrize("boundary", ["mark_sent", "protect_transport"])
@pytest.mark.parametrize("proof_present", [False, True])
def test_normal_send_tag_is_required_again_at_mark_and_transport(
    tmp_path: Path,
    boundary: str,
    proof_present: bool,
) -> None:
    deps, _shoko, _clock = confirmed_deps(tmp_path)
    try:
        proven = complete_proof(deps)
        _seed_planned_send_file(deps.store)
        key = deps.store.begin_cycle("owned", td.T1)
        send = deps.store.prepare_send(
            599,
            "a" * 64,
            "b" * 64,
            targets_digest="c" * 64,
            structure_digest="d" * 64,
            mapping_revision="1",
            cycle_key=key,
            iso=td.T1,
            normal_source_proof=proven.normal_source_proof,
        )
        assert isinstance(send, SendToken)
        assert deps.store.normal_source_send_owned(599)
        if boundary == "protect_transport":
            deps.store.mark_sent(send, iso=td.T1, normal_source_proof=proven.normal_source_proof)
        if proof_present:
            deps.store.set_meta(f"normal_send_owner_v1:{send.send_id}", "f" * 64)
        final_proof = proven.normal_source_proof if proof_present else None

        def reach_boundary() -> None:
            if boundary == "mark_sent":
                deps.store.mark_sent(send, iso=td.T1, normal_source_proof=final_proof)
            else:
                with deps.store.protect_transport(send, normal_source_proof=final_proof):
                    pytest.fail("wrong-owner send reached transport")

        with pytest.raises(StateError, match="endpoint ownership"):
            reach_boundary()
    finally:
        close(deps)


def test_advanced_recovery_cannot_inspect_normal_tagged_attempt_at_other_endpoint(
    tmp_path: Path,
) -> None:
    deps, shoko, _clock = confirmed_deps(tmp_path)
    extra_endpoint = None
    try:
        shoko.link_on_post = False
        assert normal_cycle(deps).new_arrival_submitted == 1
        assert deps.store.normal_source_send_tagged(500)
        extra_endpoint = td.endpoint_for(shoko.handler, "http://advanced-shoko:8111")
        advanced = replace(
            deps,
            service_mode=False,
            service_context=None,
            normal_source_proof=None,
            shoko=extra_endpoint,
            settings=replace(
                deps.settings, shoko=replace(deps.settings.shoko, url="http://advanced-shoko:8111")
            ),
        )
        shoko.requests.clear()
        reasons, processed = daemon._file_recovery_phase(advanced, td.T1, 0.0, max_rows=1)
        assert processed == 1
        assert "recovery_source_ownership_unknown:500" in reasons
        assert not any(request.url.path == "/api/v3/File/500" for request in shoko.requests)
        assert deps.store.budget_view().pending_file_requests == 1
    finally:
        if extra_endpoint is not None:
            extra_endpoint.client.close()
        close(deps)
