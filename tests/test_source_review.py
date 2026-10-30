"""Controller/SQLite tests. The trusted collector is a fake, not live HTTP proof."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from test_source_rebaseline import OBSERVATION, SESSION, T1, bindings, digest, poll, setup, tables

from shoko_linker.source_review import (
    SourceReviewController,
    SourceReviewEvidence,
    SourceReviewSummary,
    SourceReviewWork,
)
from shoko_linker.state import (
    META_SERVICE_AUTHORIZATION_REVISION,
    META_SOURCE_CONTINUITY_HOLD,
    SourceRebaselineAssessment,
    SourceRebaselineBindings,
    StateError,
    StateStore,
    open_state,
)

OTHER_SESSION = "d" * 64


@dataclass
class Clock:
    now: datetime = field(
        default_factory=lambda: datetime.fromisoformat(T1) + timedelta(seconds=10)
    )
    mono: float = 100.0
    stopped: bool = False

    def advance(self, seconds: int = 1) -> None:
        self.now += timedelta(seconds=seconds)
        self.mono += seconds


@dataclass
class Collector:
    clock: Clock
    capture_calls: int = 0
    works: list[SourceReviewWork] = field(default_factory=list)
    first_progress: list[object | None] = field(default_factory=list)
    deadlines: list[float] = field(default_factory=list)
    slices: int = 1
    content: str = "full paths and episode relationships"
    effective: str = "effective endpoints settings scope"
    mapping: str = "mapping content"
    action: Callable[[StateStore, SourceReviewWork], None] | None = None
    transform: Callable[[SourceReviewEvidence], SourceReviewEvidence] | None = None

    def capture_bindings(self, store: StateStore, deadline: float) -> SourceRebaselineBindings:
        self.capture_calls += 1
        self.deadlines.append(deadline)
        return replace(
            bindings(store),
            effective_identity_sha256=digest(self.effective),
            mapping_identity_sha256=digest(self.mapping),
        )

    def collect(
        self, store: StateStore, work: SourceReviewWork, deadline: float
    ) -> SourceReviewEvidence | None:
        self.deadlines.append(deadline)
        self.works.append(work)
        self.first_progress.append(work.progress)
        page = 0 if work.progress is None else int(str(work.progress))
        work.progress = page + 1
        if self.action is not None:
            self.action(store, work)
        if page + 1 < self.slices:
            return None
        result = SourceReviewEvidence(
            SourceRebaselineAssessment(
                OBSERVATION, digest(self.content), work.started_iso, self.clock.now.isoformat()
            ),
            frozenset(range(1, 57)),
            SourceReviewSummary(1, 20, 30, 56, 2),
            True,
            True,
            True,
            True,
        )
        return result if self.transform is None else self.transform(result)


@dataclass
class Harness:
    path: Path
    clock: Clock
    collector: Collector
    controller: SourceReviewController
    wakes: list[bool]

    def tick(self) -> bool:
        return bool(self.controller.worker_tick(self.clock.mono + 60))

    def ready(self) -> str:
        self.controller.request_assessment(SESSION)
        assert self.tick()
        view = self.controller.view(SESSION)
        assert view.status == "ready"
        assert view.token is not None
        return view.token

    def confirm(self, token: str) -> None:
        assert self.controller.request_confirmation(token, SESSION).status == "confirming"
        self.clock.advance()
        assert self.tick()


@pytest.fixture
def harness(tmp_path: Path) -> Harness:
    path = tmp_path / "state.sqlite3"
    with open_state(path) as store:
        setup(store)
        store.create_dashboard_session(OTHER_SESSION, "e" * 64, 2_000_000_000, T1)
    clock = Clock()
    collector = Collector(clock)
    wakes: list[bool] = []
    controller = SourceReviewController(
        path,
        collector,
        wake=lambda: wakes.append(True),
        shutdown_requested=lambda: clock.stopped,
        now=lambda: clock.now,
        monotonic=lambda: clock.mono,
    )
    return Harness(path, clock, collector, controller, wakes)


def test_views_never_schedule_collect_or_mutate_even_when_expired(harness: Harness) -> None:
    h = harness
    with open_state(h.path) as store:
        before = tables(store)
    for _ in range(5):
        assert h.controller.view(SESSION).status == "idle"
    assert not h.tick()
    assert not h.wakes
    assert h.collector.capture_calls == 0
    with open_state(h.path) as store:
        assert tables(store) == before
    h.ready()
    h.clock.advance(901)
    with open_state(h.path) as store:
        before = tables(store)
    calls, wakes = h.collector.capture_calls, len(h.wakes)
    for _ in range(5):
        view = h.controller.view(SESSION)
        assert view.status == "error"
        assert view.token is None
    assert h.collector.capture_calls == calls
    assert len(h.wakes) == wakes
    assert not h.tick()
    with open_state(h.path) as store:
        assert tables(store) == before


def test_explicit_requests_coalesce_and_confirmation_starts_empty(harness: Harness) -> None:
    h = harness
    for _ in range(5):
        assert h.controller.request_assessment(SESSION).status == "queued"
    assert h.wakes == [True]
    assert not h.collector.works
    assert not h.collector.capture_calls
    assert h.tick()
    token = h.controller.view(SESSION).token
    assert token is not None
    for _ in range(5):
        assert h.controller.request_assessment(SESSION).token == token
    for _ in range(5):
        assert h.controller.request_confirmation(token, SESSION).status == "confirming"
        assert h.controller.request_assessment(SESSION).status == "confirming"
    assert h.wakes == [True, True]
    # Exact equality to the request timestamp is NOT fresh proof.
    assert h.tick()
    assert len(h.collector.works) == 1
    h.clock.advance()
    assert h.tick()
    assert h.controller.view(SESSION).status == "confirmed"
    assert len(h.collector.works) == 2
    assert h.collector.works[0] is not h.collector.works[1]
    assert h.collector.first_progress == [None, None]
    assert h.collector.works[0].started_iso < h.collector.works[1].started_iso
    assert not h.tick()
    with pytest.raises(StateError, match="unavailable"):
        h.controller.request_confirmation(token, SESSION)


def test_review_owned_progress_resumes_and_second_sweep_is_independent(harness: Harness) -> None:
    h = harness
    h.collector.slices = 3
    h.controller.request_assessment(SESSION)
    for _ in range(2):
        assert h.tick()
        assert h.controller.view(SESSION).status == "assessing"
    assert h.tick()
    token = h.controller.view(SESSION).token
    assert token is not None
    assert h.collector.works[0] is h.collector.works[2]
    h.controller.request_confirmation(token, SESSION)
    h.clock.advance()
    for _ in range(3):
        assert h.tick()
    assert h.controller.view(SESSION).status == "confirmed"
    assert h.collector.first_progress == [None, 1, 2, None, 1, 2]
    assert h.collector.works[0] is not h.collector.works[3]
    assert all(deadline <= 161 for deadline in h.collector.deadlines)


def test_changed_full_content_requires_another_explicit_acceptance(harness: Harness) -> None:
    h = harness
    first = h.ready()
    h.collector.content = "same counts; different full paths and episode associations"
    h.confirm(first)
    changed = h.controller.view(SESSION)
    assert changed.status == "changed"
    assert changed.token is not None
    assert changed.token != first
    with open_state(h.path) as store:
        assert store.get_source_continuity_review().requires_rebaseline
        assert store.get_source_rebaseline_review().last_audit is None
    with pytest.raises(StateError):
        h.controller.request_confirmation(first, SESSION)
    h.confirm(changed.token)
    assert h.controller.view(SESSION).status == "confirmed"
    assert len(h.collector.works) == 3


@pytest.mark.parametrize(
    "flag", ["sonarr_complete", "sonarr_sweep_complete", "shoko_complete", "shoko_sweep_complete"]
)
def test_incomplete_evidence_never_becomes_ready(harness: Harness, flag: str) -> None:
    h = harness
    h.collector.transform = lambda evidence: replace(
        evidence,
        sonarr_complete=flag != "sonarr_complete",
        sonarr_sweep_complete=flag != "sonarr_sweep_complete",
        shoko_complete=flag != "shoko_complete",
        shoko_sweep_complete=flag != "shoko_sweep_complete",
    )
    h.controller.request_assessment(SESSION)
    assert h.tick()
    assert h.controller.view(SESSION).status == "error"
    with open_state(h.path) as store:
        assert store.get_source_rebaseline_review().prepared is None
        assert store.get_source_continuity_review().requires_rebaseline


@pytest.mark.parametrize("change", ["roots", "total", "started", "future", "digest"])
def test_invalid_assessment_rejected(harness: Harness, change: str) -> None:
    h = harness

    def transform(evidence: SourceReviewEvidence) -> SourceReviewEvidence:
        assessment = evidence.assessment
        if change == "roots":
            return replace(evidence, summary=replace(evidence.summary, selected_roots=0))
        if change == "total":
            assessment = replace(assessment, observation=replace(OBSERVATION, discovery_total=None))
        elif change == "started":
            assessment = replace(assessment, started_iso=T1)
        elif change == "future":
            assessment = replace(
                assessment, completed_iso=(h.clock.now + timedelta(seconds=1)).isoformat()
            )
        else:
            assessment = replace(assessment, content_sha256="bad")
        return replace(evidence, assessment=assessment)

    h.collector.transform = transform
    h.controller.request_assessment(SESSION)
    assert h.tick()
    assert h.controller.view(SESSION).status == "error"
    with open_state(h.path) as store:
        assert store.get_source_rebaseline_review().prepared is None


@pytest.mark.parametrize("change", ["effective", "mapping", "local"])
def test_identity_drift_during_io_aborts_before_prepare(harness: Harness, change: str) -> None:
    h = harness

    def drift(store: StateStore, _work: SourceReviewWork) -> None:
        if change == "local":
            store.set_meta(META_SERVICE_AUTHORIZATION_REVISION, "f" * 32)
        else:
            setattr(h.collector, change, "changed")

    h.collector.action = drift
    h.controller.request_assessment(SESSION)
    assert h.tick()
    assert h.controller.view(SESSION).status == "error"
    with open_state(h.path) as store:
        assert store.get_source_rebaseline_review().prepared is None


def test_changed_bindings_before_confirm_skip_collection(harness: Harness) -> None:
    h = harness
    token = h.ready()
    h.collector.mapping = "new mappings"
    h.confirm(token)
    assert h.controller.view(SESSION).status == "error"
    assert len(h.collector.works) == 1


@pytest.mark.parametrize("change", ["deadline", "shutdown", "timeout"])
def test_late_or_aborted_collection_never_prepares(harness: Harness, change: str) -> None:
    h = harness

    def abort(_store: StateStore, _work: SourceReviewWork) -> None:
        if change == "deadline":
            h.clock.advance(60)
        elif change == "shutdown":
            h.clock.stopped = True
        else:
            raise TimeoutError("upstream secret response must not be exposed")

    h.collector.action = abort
    h.controller.request_assessment(SESSION)
    assert h.tick()
    view = h.controller.view(SESSION)
    assert view.status == "error"
    assert "secret" not in view.message
    with open_state(h.path) as store:
        assert store.get_source_rebaseline_review().prepared is None


def test_expired_queued_job_never_calls_collector(harness: Harness) -> None:
    h = harness
    h.controller.request_assessment(SESSION)
    h.clock.advance(901)
    assert h.tick()
    assert h.controller.view(SESSION).status == "error"
    assert not h.collector.capture_calls


def test_owner_is_private_and_other_session_cannot_replace_active_job(harness: Harness) -> None:
    h = harness
    h.controller.request_assessment(SESSION)
    with pytest.raises(StateError, match="another session"):
        h.controller.request_assessment(OTHER_SESSION)
    assert h.controller.view(OTHER_SESSION).token is None
    assert h.tick()
    token = h.controller.view(SESSION).token
    assert token is not None
    other = h.controller.view(OTHER_SESSION)
    assert other.token is None
    assert other.summary is None
    with pytest.raises(StateError, match="another session"):
        h.controller.request_confirmation(token, OTHER_SESSION)
    assert len(h.wakes) == 1


def test_owner_expiry_during_io_aborts_before_commit(harness: Harness) -> None:
    h = harness
    with open_state(h.path) as store:
        # Update expiry through a fixture SQL write; no session-revocation API exists.
        store.connection.execute(
            "UPDATE dashboard_sessions SET expires_epoch = ? WHERE session_id_hash = ?",
            (int(h.clock.now.timestamp()) + 1, SESSION),
        )
        store.connection.commit()
    h.collector.action = lambda _store, _work: h.clock.advance(2)
    h.controller.request_assessment(SESSION)
    assert h.tick()
    with pytest.raises(StateError, match="session is unavailable"):
        h.controller.view(SESSION)
    with open_state(h.path) as store:
        assert store.get_source_rebaseline_review().prepared is None


def test_csrf_rotation_keeps_review_owner_stable(harness: Harness) -> None:
    h = harness
    token = h.ready()
    with open_state(h.path) as store:
        assert store.rotate_dashboard_session_csrf(
            SESSION, "b" * 64, "f" * 64, int(h.clock.now.timestamp())
        )
    h.confirm(token)
    assert h.controller.view(SESSION).status == "confirmed"


def test_confirmation_preserves_user_pause_consent_and_all_nonmeta_tables(harness: Harness) -> None:
    h = harness
    with open_state(h.path) as store:
        store.pause_service_for_user(h.clock.now.isoformat())
        consent = store.get_onboarding_json()
        before = tables(store)
    h.confirm(h.ready())
    view = h.controller.view(SESSION)
    assert view.status == "confirmed"
    assert view.service_pause == (True, "user_paused")
    with open_state(h.path) as store:
        assert not store.get_source_continuity_review().requires_rebaseline
        assert store.get_onboarding_json() == consent
        after = tables(store)
        assert {k: v for k, v in before.items() if k != "meta"} == {
            k: v for k, v in after.items() if k != "meta"
        }
        assert store.get_source_rebaseline_review().last_audit is not None


def test_storage_hold_blocks_prepare_without_clearing_hold(harness: Harness) -> None:
    h = harness
    h.controller.request_assessment(SESSION)
    with open_state(h.path) as store:
        store.disable_writes("storage check failed")
    assert h.tick()
    assert h.controller.view(SESSION).status == "error"
    with open_state(h.path) as store:
        assert store.writes_disabled
        assert store.get_source_continuity_review().requires_rebaseline


def test_competing_prepared_review_aborts_queued_confirmation(harness: Harness) -> None:
    h = harness
    token = h.ready()
    h.controller.request_confirmation(token, SESSION)
    with open_state(h.path) as store:
        prepared = store.get_source_rebaseline_review().prepared
        assert prepared is not None
        store.prepare_source_rebaseline(
            SourceRebaselineAssessment(OBSERVATION, digest(h.collector.content), T1, T1),
            expected_hold_id=prepared.hold_id,
            bindings=bindings(store),
            owner_session_sha256=OTHER_SESSION,
            iso=h.clock.now.isoformat(),
            expires_iso=(h.clock.now + timedelta(seconds=600)).isoformat(),
        )
    h.clock.advance()
    assert h.tick()
    assert h.controller.view(SESSION).status == "error"
    assert len(h.collector.works) == 1


def test_running_job_reentrant_requests_cannot_replace_progress(harness: Harness) -> None:
    h = harness

    def rerequest(_store: StateStore, _work: SourceReviewWork) -> None:
        assert h.controller.request_assessment(SESSION).status == "assessing"
        assert h.controller.worker_tick(h.clock.mono + 60)

    h.collector.action = rerequest
    h.ready()
    assert len(h.collector.works) == 1
    assert len(h.wakes) == 1


@pytest.mark.parametrize("failure", ["incomplete", "shutdown", "timeout", "local_drift"])
def test_failed_second_collection_preserves_baseline_hold_and_no_audit(
    harness: Harness, failure: str
) -> None:
    h = harness
    token = h.ready()

    def fail(store: StateStore, _work: SourceReviewWork) -> None:
        if failure == "shutdown":
            h.clock.stopped = True
        elif failure == "timeout":
            raise TimeoutError("private upstream response")
        elif failure == "local_drift":
            store.set_meta(META_SERVICE_AUTHORIZATION_REVISION, "f" * 32)

    h.collector.action = fail
    if failure == "incomplete":
        h.collector.transform = lambda evidence: replace(evidence, shoko_sweep_complete=False)
    h.confirm(token)
    assert h.controller.view(SESSION).status == "error"
    assert len(h.collector.works) == 2
    with open_state(h.path) as store:
        assert store.get_source_continuity_review().requires_rebaseline
        assert store.get_source_rebaseline_review().last_audit is None
        prepared = store.get_source_rebaseline_review().prepared
        assert prepared is not None
        assert prepared.token == token


def test_complete_empty_library_with_selected_roots_is_valid(harness: Harness) -> None:
    h = harness
    h.collector.transform = lambda evidence: replace(
        evidence,
        assessment=replace(
            evidence.assessment,
            observation=replace(OBSERVATION, discovery_total=0),
        ),
        discovered_file_ids=frozenset(),
        summary=SourceReviewSummary(1, 0, 0, 0, 2),
    )
    h.confirm(h.ready())
    assert h.controller.view(SESSION).status == "confirmed"


def test_legacy_hold_remains_honestly_unknown_during_review(harness: Harness) -> None:
    h = harness
    with open_state(h.path) as store:
        store.connection.execute("DELETE FROM meta WHERE key = ?", (META_SOURCE_CONTINUITY_HOLD,))
        store.connection.commit()
    view = h.controller.view(SESSION)
    assert view.continuity.evidence_status == "legacy_unknown"
    assert view.continuity.hold is None
    h.ready()
    hold = h.controller.view(SESSION).continuity.hold
    assert hold is not None
    assert hold.detected_iso is None
    assert hold.prior is None


def test_restart_does_not_automatically_recollect_or_accept_old_token(harness: Harness) -> None:
    h = harness
    token = h.ready()
    restarted = SourceReviewController(
        h.path,
        h.collector,
        wake=lambda: h.wakes.append(True),
        shutdown_requested=lambda: False,
        now=lambda: h.clock.now,
        monotonic=lambda: h.clock.mono,
    )
    assert restarted.view(SESSION).status == "idle"
    assert restarted.view(SESSION).token is None
    with pytest.raises(StateError, match="unavailable"):
        restarted.request_confirmation(token, SESSION)
    assert not restarted.worker_tick(h.clock.mono + 60)
    assert len(h.collector.works) == 1
    assert restarted.request_assessment(SESSION).status == "queued"


@pytest.mark.parametrize("previous", ["confirmed", "ready", "error", "assessing"])
def test_new_hold_never_displays_previous_review_lifecycle(harness: Harness, previous: str) -> None:
    h = harness
    token = h.ready()
    if previous == "confirmed":
        h.confirm(token)
    elif previous == "error":
        h.collector.mapping = "changed"
        h.confirm(token)
    elif previous == "assessing":
        h.controller.request_confirmation(token, SESSION)
        h.clock.advance()
        h.collector.slices = 3
        assert h.tick()
    with open_state(h.path) as store:
        old_hold = store.get_source_continuity_review().hold
        audit = store.get_source_rebaseline_review().last_audit
        if previous != "confirmed":
            # Another local process accepts the old hold; this controller cannot
            # transfer its ready, failed or in-progress display to the next hold.
            store.set_meta("requires_rebaseline", "0")
            store.connection.execute(
                "DELETE FROM meta WHERE key = ?", (META_SOURCE_CONTINUITY_HOLD,)
            )
            store.connection.commit()
        poll(store, total=55, at=h.clock.now.isoformat())
        current = store.get_source_continuity_review()
        assert current.hold is not None
        assert current.hold.current is not None
        assert current.hold.current.discovery_total == 55
        if old_hold is not None:
            assert current.hold.hold_id != old_hold.hold_id
        before = tables(store)
        before_meta = store.connection.execute(
            "SELECT key, value FROM meta ORDER BY key"
        ).fetchall()
    calls, wakes = h.collector.capture_calls, len(h.wakes)
    for _ in range(2):
        view = h.controller.view(SESSION)
        assert view.status == "idle"
        assert view.continuity == current
        assert view.token is None
        assert view.summary is None
        assert view.started_iso is None
        assert view.completed_iso is None
        assert view.message == ""
    assert h.collector.capture_calls == calls
    assert len(h.wakes) == wakes
    with open_state(h.path) as store:
        assert tables(store) == before
        assert (
            store.connection.execute("SELECT key, value FROM meta ORDER BY key").fetchall()
            == before_meta
        )
        assert store.get_source_rebaseline_review().last_audit == audit
    # A former owner/token cannot reserve the new hold against another session.
    replacement = h.controller.request_assessment(OTHER_SESSION)
    assert replacement.status == "queued"
    assert replacement.token is None
    with pytest.raises(StateError, match="another session"):
        h.controller.request_assessment(SESSION)
    with open_state(h.path) as store:
        assert store.get_source_continuity_review() == current


def test_another_controller_accepting_new_hold_cannot_revive_old_confirmation(
    harness: Harness,
) -> None:
    h = harness
    h.confirm(h.ready())
    with open_state(h.path) as store:
        poll(store, total=55, at=h.clock.now.isoformat())
    other = SourceReviewController(
        h.path,
        h.collector,
        wake=lambda: None,
        shutdown_requested=lambda: False,
        now=lambda: h.clock.now,
        monotonic=lambda: h.clock.mono,
    )
    other.request_assessment(SESSION)
    assert other.worker_tick(h.clock.mono + 60)
    token = other.view(SESSION).token
    assert token is not None
    other.request_confirmation(token, SESSION)
    h.clock.advance()
    assert other.worker_tick(h.clock.mono + 60)
    assert other.view(SESSION).status == "confirmed"
    previous = h.controller.view(SESSION)
    assert previous.status == "idle"
    assert previous.summary is None
    assert previous.token is None


def test_ready_token_monotonic_expiry_survives_wall_clock_rollback(harness: Harness) -> None:
    h = harness
    token = h.ready()
    calls, wakes = h.collector.capture_calls, len(h.wakes)
    h.clock.mono += 901
    h.clock.now += timedelta(seconds=1)  # Wall time is far behind elapsed monotonic time.
    with open_state(h.path) as store:
        before = tables(store)
    for _ in range(3):
        view = h.controller.view(SESSION)
        assert view.status == "error"
        assert view.token is None
    with pytest.raises(StateError, match="unavailable"):
        h.controller.request_confirmation(token, SESSION)
    assert not h.tick()
    assert h.collector.capture_calls == calls
    assert len(h.wakes) == wakes
    with open_state(h.path) as store:
        assert tables(store) == before
        assert store.get_source_continuity_review().requires_rebaseline
    # A new explicit assessment may create a genuinely new token.
    assert h.controller.request_assessment(SESSION).status == "queued"
    assert h.tick()
    replacement = h.controller.view(SESSION)
    assert replacement.status == "ready"
    assert replacement.token != token


def test_confirmation_queued_near_expiry_uses_only_original_remaining_budget(
    harness: Harness,
) -> None:
    h = harness
    token = h.ready()  # Original monotonic deadline is 100 + 900 = 1000.
    first_deadlines = len(h.collector.deadlines)
    h.clock.mono += 890
    h.clock.now += timedelta(seconds=1)
    h.confirm(token)  # Fresh collection starts at 991, with 9 seconds remaining.
    assert h.controller.view(SESSION).status == "confirmed"
    second_deadlines = h.collector.deadlines[first_deadlines:]
    assert second_deadlines
    assert all(deadline == 1000 for deadline in second_deadlines)


def test_original_token_expiry_during_second_collection_preserves_hold(harness: Harness) -> None:
    h = harness
    token = h.ready()
    h.clock.mono += 890
    h.clock.now += timedelta(seconds=1)

    def finish_at_original_expiry(_store: StateStore, _work: SourceReviewWork) -> None:
        h.clock.mono = 1000

    h.collector.action = finish_at_original_expiry
    h.confirm(token)
    assert len(h.collector.works) == 2
    assert h.controller.view(SESSION).status == "error"
    with open_state(h.path) as store:
        assert store.get_source_continuity_review().requires_rebaseline
        assert store.get_source_rebaseline_review().last_audit is None
        prepared = store.get_source_rebaseline_review().prepared
        assert prepared is not None
        assert prepared.token == token


def changed_token_near_original_expiry(h: Harness) -> tuple[str, str]:
    token = h.ready()
    h.clock.mono += 890
    h.clock.now += timedelta(seconds=1)
    h.collector.content = "changed full paths and relationships"
    h.confirm(token)  # New content prepares a new token at monotonic 991.
    changed = h.controller.view(SESSION)
    assert changed.status == "changed"
    assert changed.token is not None
    assert changed.token != token
    return token, changed.token


def test_changed_token_gets_its_own_deadline_without_reviving_old_token(harness: Harness) -> None:
    h = harness
    old, new = changed_token_near_original_expiry(h)
    # The original deadline was 1000; the NEW token's deadline is 991 + 900 = 1891.
    h.clock.mono = 1880
    h.clock.now += timedelta(seconds=1)
    assert h.controller.view(SESSION).token == new
    with pytest.raises(StateError, match="unavailable"):
        h.controller.request_confirmation(old, SESSION)
    previous_deadlines = len(h.collector.deadlines)
    h.confirm(new)
    assert h.controller.view(SESSION).status == "confirmed"
    assert all(deadline == 1891 for deadline in h.collector.deadlines[previous_deadlines:])


def test_changed_token_also_expires_monotonically_despite_wall_rollback(harness: Harness) -> None:
    h = harness
    _old, new = changed_token_near_original_expiry(h)
    h.clock.mono = 1891
    h.clock.now += timedelta(seconds=1)
    calls = h.collector.capture_calls
    view = h.controller.view(SESSION)
    assert view.status == "error"
    assert view.token is None
    with pytest.raises(StateError, match="unavailable"):
        h.controller.request_confirmation(new, SESSION)
    assert h.collector.capture_calls == calls
    with open_state(h.path) as store:
        assert store.get_source_continuity_review().requires_rebaseline
        assert store.get_source_rebaseline_review().last_audit is None
