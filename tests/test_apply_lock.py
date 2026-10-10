"""F5 (P1) apply-ownership fence: real OS advisory locks across processes.

These tests use ``multiprocessing.get_context("spawn")`` with a real SQLite state
directory on ``tmp_path``. Child processes open their OWN store (no shared fds); the
kernel releases the OS advisory lock when a child exits, so crash recovery is proven
end to end. Timers use generous margins so the assertions are deterministic on
Windows and POSIX. HTTP is never touched (offline).
"""

from __future__ import annotations

import multiprocessing
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from shoko_linker.state import (
    BudgetLimits,
    Cohort,
    FileState,
    LockHeldError,
    SendToken,
    StateStore,
)

NOW_ISO = "2026-01-01T00:00:00+00:00"
LEASE_SECONDS = 1.0
HOLD_SECONDS = 6.0
MIN_ATTEMPTS = 3
SPAWN = "spawn"

PLAN_FP = "a" * 64
REQ_FP = "b" * 64
TARGETS = "d" * 64
STRUCTURE = "e" * 64
WIDE_LIMITS = BudgetLimits(
    max_pending_files=10,
    max_pending_episode_jobs=20,
    per_cycle_new=10,
    rolling_new=10,
    rolling_window_hours=24.0,
)


def _iso_plus(iso: str, seconds: float) -> str:
    return (datetime.fromisoformat(iso) + timedelta(seconds=seconds)).isoformat()


def _open(path: Path) -> StateStore:
    return StateStore(path, budget_limits=WIDE_LIMITS)


def _seed_uncertain_send(store: StateStore, iso: str) -> None:
    """Durable ``request_may_have_been_sent`` row: prepared + mark_sent committed."""
    store.record_file(
        101,
        sonarr_series_id=1,
        sonarr_file_id=10,
        sonarr_path="/data/media/anime/show/s01e01.mkv",
        sonarr_size=1234,
        cohort=Cohort.NEW_ARRIVAL,
        iso=iso,
    )
    for target in (FileState.MATCHED, FileState.MAPPED, FileState.PLANNED):
        store.advance(101, target, "walk", iso=iso)
    token = store.prepare_send(
        101,
        PLAN_FP,
        REQ_FP,
        targets_digest=TARGETS,
        structure_digest=STRUCTURE,
        mapping_revision="7",
        cycle_key="cycle-1",
        iso=iso,
    )
    assert isinstance(token, SendToken)
    store.mark_sent(token, iso=iso)


# ---------------------------------------------------------------- child entry points


def _hold_ownership(path_str: str, queue: multiprocessing.Queue[Any], hold_seconds: float) -> None:
    """Acquire apply ownership, report the pid, then hold the OS lock for the full duration."""
    store = _open(Path(path_str))
    try:
        lease = _iso_plus(NOW_ISO, LEASE_SECONDS)
        store.acquire_apply_ownership(os.getpid(), NOW_ISO, lease)
        queue.put(("acquired", os.getpid()))
        time.sleep(hold_seconds)
    except BaseException as exc:  # pragma: no cover - reported to the parent
        queue.put(("error", repr(exc)))
    finally:
        store.close()


def _crash_holder(path_str: str, queue: multiprocessing.Queue[Any]) -> None:
    """Acquire ownership, commit a durable uncertain send, report, then block until killed."""
    store = _open(Path(path_str))
    lease = _iso_plus(NOW_ISO, LEASE_SECONDS)
    store.acquire_apply_ownership(os.getpid(), NOW_ISO, lease)
    _seed_uncertain_send(store, NOW_ISO)
    queue.put(("sent", os.getpid()))
    time.sleep(60)  # the parent terminates us well before this; the kernel frees the lock
    store.close()  # pragma: no cover - never reached


def _start(ctx: Any, target: Any, args: tuple[Any, ...]) -> Any:
    process = ctx.Process(target=target, args=args)
    process.start()
    return process


def _await(queue: multiprocessing.Queue[Any], kind: str) -> int:
    message, pid = queue.get(timeout=20)
    assert message == kind, f"expected {kind!r}, got {message!r}"
    return int(pid)


# ------------------------------------------------------------------- cross-process


def test_live_owner_is_contended_past_meta_lease_expiry(tmp_path: Path) -> None:
    """Req 1: a live apply owner never loses ownership when the fixed lease expires.

    P1 holds the OS lock far past its meta lease expiry; P2's acquire is CONTENDED with
    zero meta mutation and zero submissions.
    """
    ctx = multiprocessing.get_context(SPAWN)
    queue: multiprocessing.Queue[Any] = ctx.Queue()
    path = tmp_path / "state.sqlite3"
    proc = _start(ctx, _hold_ownership, (str(path), queue, HOLD_SECONDS))
    try:
        holder_pid = _await(queue, "acquired")
        # Let the meta lease expire while the owner is still alive.
        time.sleep(LEASE_SECONDS + 1.0)
        store = _open(path)
        try:
            with pytest.raises(LockHeldError) as excinfo:
                store.acquire_apply_ownership(
                    os.getpid(), _iso_plus(NOW_ISO, 60.0), _iso_plus(NOW_ISO, 120.0)
                )
            recorded = excinfo.value.holder
            assert recorded is not None
            assert recorded.owner_pid == holder_pid
            # Zero meta mutation: the foreign lease record is untouched.
            current = store.lock_holder()
            assert current is not None
            assert current.owner_pid == holder_pid
            assert current.acquired_iso == NOW_ISO
            # Zero submissions: no send rows exist.
            assert store.get_file(101) is None
        finally:
            store.close()
    finally:
        proc.terminate()
        proc.join(timeout=20)


def test_crash_recovery_kernel_releases_and_owner_steals(tmp_path: Path) -> None:
    """Req 2: a killed owner's OS lock is freed by the kernel; a new owner takes over.

    The durable ``request_may_have_been_sent`` row is preserved and stays
    resubmit-forbidden after the takeover (no blind resubmit of an uncertain send).
    """
    ctx = multiprocessing.get_context(SPAWN)
    queue: multiprocessing.Queue[Any] = ctx.Queue()
    path = tmp_path / "state.sqlite3"
    holder = _start(ctx, _crash_holder, (str(path), queue))
    try:
        holder_pid = _await(queue, "sent")
        # Let the (short) meta lease expire, then hard-kill the owner.
        time.sleep(LEASE_SECONDS + 1.0)
        holder.terminate()
        holder.join(timeout=20)
        assert holder.exitcode is not None

        store = _open(path)
        try:
            acq = store.acquire_apply_ownership(
                os.getpid(), _iso_plus(NOW_ISO, 120.0), _iso_plus(NOW_ISO, 600.0)
            )
            assert acq.acquired is True
            assert acq.holder.owner_pid == os.getpid()
            # Steal of the DEAD owner is audit-recorded.
            steals = store.get_meta("apply_lock_steals")
            assert steals is not None
            import json

            audit = json.loads(steals)
            assert any(entry["from_pid"] == holder_pid for entry in audit)
            # Recovery classification preserved: the uncertain row is held, never resubmitted.
            plan = store.recover_after_restart()
            assert [row.shoko_file_id for row in plan.resubmit_forbidden] == [101]
            assert "never resubmit" in plan.resubmit_forbidden[0].instruction
        finally:
            store.close()
    finally:
        if holder.is_alive():  # pragma: no cover - defensive cleanup
            holder.terminate()
            holder.join(timeout=20)


def test_owner_holds_across_a_long_batch_and_refuses_all_racers(tmp_path: Path) -> None:
    """Req 3: a multi-file batch that exceeds the lease is refused for the whole duration."""
    ctx = multiprocessing.get_context(SPAWN)
    queue: multiprocessing.Queue[Any] = ctx.Queue()
    path = tmp_path / "state.sqlite3"
    # A batch longer than the lease: the owner holds ownership the entire time.
    holder = _start(ctx, _hold_ownership, (str(path), queue, HOLD_SECONDS))
    try:
        holder_pid = _await(queue, "acquired")
        attempts = 0
        deadline = time.monotonic() + HOLD_SECONDS - 2.0
        while time.monotonic() < deadline:
            store = _open(path)
            try:
                with pytest.raises(LockHeldError) as excinfo:
                    store.acquire_apply_ownership(
                        os.getpid(), _iso_plus(NOW_ISO, 60.0), _iso_plus(NOW_ISO, 120.0)
                    )
                assert excinfo.value.holder is not None
                assert excinfo.value.holder.owner_pid == holder_pid
            finally:
                store.close()
            attempts += 1
            time.sleep(0.4)
        # Several attempts straddle the lease expiry — all refused.
        assert attempts >= MIN_ATTEMPTS
    finally:
        holder.terminate()
        holder.join(timeout=20)


def test_dead_owner_free_os_lock_takeover_is_audited(tmp_path: Path) -> None:
    """Req 4 (process view): an expired lease with a FREE OS lock is a legitimate takeover."""
    ctx = multiprocessing.get_context(SPAWN)
    queue: multiprocessing.Queue[Any] = ctx.Queue()
    path = tmp_path / "state.sqlite3"
    holder = _start(ctx, _hold_ownership, (str(path), queue, 0.2))
    holder_pid = _await(queue, "acquired")
    holder.join(timeout=20)  # exit releases the OS lock; the meta lease is now stale.
    assert holder.exitcode == 0

    store = _open(path)
    try:
        acq = store.acquire_apply_ownership(
            os.getpid(), _iso_plus(NOW_ISO, 60.0), _iso_plus(NOW_ISO, 120.0)
        )
        assert acq.acquired is True
        import json

        audit = json.loads(str(store.get_meta("apply_lock_steals")))
        assert any(entry["from_pid"] == holder_pid for entry in audit)
    finally:
        store.close()
