"""M7-a reporting payloads: nonsecret JSON/human serializations for the CLI.

Every payload here is built from local data only — the state DB (read-only queries),
the mapping store's on-disk view, and validated settings. **API key values are never
included**: settings expose only the environment-variable *names*. Human-readable
rendering lives in :mod:`shoko_linker.cli`; this module produces the data structures
(``--json`` serializes them with :func:`json_payload`).

Cycle-summary meta keys (``last_cycle_*``) are the CLI's contract for persisting
one cycle's observability levels; :mod:`shoko_linker.cli` writes them, :func:`report_payload`
reads them.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any, Final

from .audit import AuditReport
from .campaign import CampaignApplyReport
from .config import Settings, apply_gate_status
from .daemon import CycleReport, DaemonReport
from .mapping_store import MappingStatus, RefreshOutcome
from .state import (
    META_APPLY_LOCK,
    META_ATTESTATION_GATE,
    META_BASELINE_COMPLETED_ISO,
    META_BUDGET_CYCLE_KEY,
    META_LAST_CYCLE_END_ISO,
    META_LAST_CYCLE_START_ISO,
    META_LAST_FULL_RECONCILE_ISO,
    META_LAST_REFUSAL,
    META_NEXT_CYCLE_DUE_ISO,
    META_NEXT_FULL_RECONCILE_ISO,
    META_REQUIRES_REBASELINE,
    META_WRITES_DISABLED_REASON,
    Baseline,
    BudgetLimits,
    Cohort,
    EligibilityVerdict,
    FileState,
    LockHolder,
    StateStore,
)

META_CYCLE_ALIVE: Final[str] = "last_cycle_alive"
META_CYCLE_RECONCILIATION_READY: Final[str] = "last_cycle_reconciliation_ready"
META_CYCLE_APPLY_ELIGIBLE: Final[str] = "last_cycle_apply_eligible"
META_CYCLE_MODE: Final[str] = "last_cycle_mode"
META_CYCLE_KEY: Final[str] = "last_cycle_key"
META_CYCLE_FAILED: Final[str] = "last_cycle_failed"
META_CYCLE_SUBMITTED: Final[str] = "last_cycle_submitted"
META_OPERATOR_ACTIONS: Final[str] = "operator_actions"

CYCLE_META_KEYS: Final[tuple[str, ...]] = (
    META_CYCLE_KEY,
    META_CYCLE_MODE,
    META_CYCLE_ALIVE,
    META_CYCLE_RECONCILIATION_READY,
    META_CYCLE_APPLY_ELIGIBLE,
    META_CYCLE_FAILED,
    META_CYCLE_SUBMITTED,
    META_LAST_CYCLE_START_ISO,
    META_LAST_CYCLE_END_ISO,
    META_NEXT_CYCLE_DUE_ISO,
    META_LAST_FULL_RECONCILE_ISO,
    META_NEXT_FULL_RECONCILE_ISO,
    META_BASELINE_COMPLETED_ISO,
    META_BUDGET_CYCLE_KEY,
    META_REQUIRES_REBASELINE,
    META_LAST_REFUSAL,
    META_WRITES_DISABLED_REASON,
    META_APPLY_LOCK,
    META_ATTESTATION_GATE,
)

ATTESTATION_LABEL: Final[str] = "attestation: absent (M7-b populates the safety lease)"
TOP_REASON_LIMIT: Final[int] = 3
DETAIL_ROLLUP_LIMIT: Final[int] = 25
INDICATOR_TRUE: Final[str] = "1"

QUEUED_STATES: Final[frozenset[FileState]] = frozenset(
    {
        FileState.DISCOVERED,
        FileState.MATCHED,
        FileState.MAPPED,
        FileState.PLANNED,
        FileState.PREPARED_NOT_SENT,
        FileState.VERIFYING,
        FileState.WAITING,
    }
)
INDETERMINATE_STATES: Final[frozenset[FileState]] = frozenset(
    {FileState.REQUEST_MAY_HAVE_BEEN_SENT, FileState.INDETERMINATE_WRITE}
)


def json_payload(payload: Mapping[str, Any]) -> str:
    """Deterministic JSON text for a payload (sorted keys, no secret values by construction)."""
    return json.dumps(payload, indent=2, sort_keys=True)


def _iso(moment: datetime) -> str:
    return (moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)).isoformat()


def _as_int(value: Any) -> int:
    return int(value)


def _parse_meta_iso(raw: str | None) -> datetime | None:
    if raw is None:
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def recovery_deferred(reasons: Sequence[str]) -> int:
    """The ``recovery_deferred:<n>`` figure carried in cycle reasons (0 when absent)."""
    for reason in reasons:
        if reason.startswith("recovery_deferred:"):
            raw = reason.split(":", 1)[1]
            return int(raw) if raw.isdigit() else 0
    return 0


# ------------------------------------------------------------------ cycle payloads


def cycle_report_payload(
    report: CycleReport,
    *,
    eligibility: EligibilityVerdict | None = None,
    started_iso: str | None = None,
    ended_iso: str | None = None,
) -> dict[str, Any]:
    """Full nonsecret serialization of one :class:`~shoko_linker.daemon.CycleReport`."""
    payload: dict[str, Any] = {
        "cycle_key": report.cycle_key,
        "mode": report.mode.value,
        "alive": report.alive,
        "reconciliation_ready": report.reconciliation_ready,
        "apply_eligible": report.apply_eligible,
        "writes_suppressed": report.writes_suppressed,
        "mapping_outcome": report.mapping_outcome.value if report.mapping_outcome else None,
        "mapping_revision": report.mapping_revision,
        "counts": asdict(report.counts),
        "reasons": list(report.reasons),
        "top_reasons": list(report.reasons[:TOP_REASON_LIMIT]),
        "drift": list(report.drift),
        "existing_link_drift": list(report.existing_link_drift),
        "deferred_planned": report.deferred_planned,
        "recovery_deferred": recovery_deferred(report.reasons),
        "failed": report.failed,
        "live_held_sends": report.live_held_sends,
        "live_held_slots": report.live_held_slots,
        "started_iso": started_iso,
        "ended_iso": ended_iso,
    }
    if eligibility is not None:
        payload["eligibility"] = {
            "eligible": eligibility.eligible,
            "reasons": list(eligibility.reasons),
        }
    return payload


def daemon_report_payload(
    report: DaemonReport,
    *,
    cycles: Sequence[tuple[CycleReport, str | None, str | None]],
) -> dict[str, Any]:
    """Aggregate daemon view: bounded cycle summaries plus totals and the exit cause.

    ``cycles_run`` is the TRUE executed count (previously the history length). ``total_failed``
    counts failed cycles, ``history_retained`` is the bounded-history size, and ``last_cycle_key``
    names the most recent completed cycle (``None`` if none ran). ``cycle_summaries`` covers
    only the bounded history, not every executed cycle.
    """
    return {
        "cycles_run": report.total_cycles,
        "total_failed": report.total_failed,
        "history_retained": len(report.cycles),
        "last_cycle_key": report.last_cycle.cycle_key if report.last_cycle is not None else None,
        "exit_reason": report.exit_reason,
        "shutdown_requested": report.shutdown_requested,
        "cycle_summaries": [
            cycle_report_payload(cycle, started_iso=start_iso, ended_iso=end_iso)
            for cycle, start_iso, end_iso in cycles
        ],
    }


def cycle_summary_line(report: CycleReport) -> str:
    """One human-readable cycle summary line (no secrets, no key values)."""
    counts = report.counts
    return (
        f"cycle {report.cycle_key} [{report.mode.value}]: "
        f"discovered={counts.discovered} matched={counts.matched} mapped={counts.mapped} "
        f"planned={counts.planned} submitted={counts.submitted} verified={counts.verified} "
        f"pending={counts.pending} indeterminate={counts.indeterminate} blocked={counts.blocked} "
        f"backlog={counts.backlog} new_arrival={counts.new_arrival} | "
        f"mapping={report.mapping_outcome.value if report.mapping_outcome else 'none'}"
        f" rev={report.mapping_revision} deferred={report.deferred_planned}"
        f" recovery_deferred={recovery_deferred(report.reasons)}"
        f" live_held(sends={report.live_held_sends}, slots={report.live_held_slots})"
        f" apply_eligible={report.apply_eligible}"
        f" reconciliation_ready={report.reconciliation_ready}"
        f"{' FAILED' if report.failed else ''}"
    )


# ------------------------------------------------------------------ mapping payloads


def mapping_status_payload(
    status: MappingStatus,
    *,
    now: datetime,
    refresh_hours: float,
    affected: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    """On-disk mapping generation view (no network): digests, age, schedule, availability."""
    due_now = _parse_meta_iso(status.next_due_iso)
    payload: dict[str, Any] = {
        "source_url": status.source_url,
        "generation_active": status.active_present,
        "revision": status.revision,
        "content_sha256": status.content_sha256,
        "targets_digest": status.targets_digest,
        "structure_digest": status.structure_digest,
        "activated_iso": status.activated_iso,
        "active_age_seconds": status.active_age_seconds,
        "active_age_hours": (
            round(status.active_age_seconds / 3600.0, 3)
            if status.active_age_seconds is not None
            else None
        ),
        "last_attempted_iso": status.last_attempted_iso,
        "last_success_iso": status.last_success_iso,
        "next_due_iso": status.next_due_iso,
        "next_due_now": due_now is None or due_now <= now,
        "refresh_hours": refresh_hours,
        "last_outcome": status.last_outcome.value if status.last_outcome is not None else None,
        "consecutive_failures": status.consecutive_failures,
        "source_available_last_attempt": status.last_outcome
        not in (
            RefreshOutcome.FAILED_TRANSPORT,
            RefreshOutcome.NO_ACTIVE_BUT_FAILED,
        ),
        "entries_indexed": status.entries_indexed,
        "mapping_rules": status.mapping_rules,
        "explicit_pairs": status.explicit_pairs,
    }
    if affected is not None:
        payload["mapping_affected_candidates"] = dict(affected)
    return payload


def refresh_payload(
    outcome: RefreshOutcome,
    status: MappingStatus,
    *,
    forced: bool,
    refresh_hours: float,
    now: datetime,
) -> dict[str, Any]:
    """Outcome of one ``mappings refresh`` run plus the resulting generation view."""
    return {
        "outcome": outcome.value,
        "success": outcome in REFRESH_SUCCESS_OUTCOMES,
        "forced": forced,
        "generation": mapping_status_payload(status, now=now, refresh_hours=refresh_hours),
    }


REFRESH_SUCCESS_OUTCOMES: Final[frozenset[RefreshOutcome]] = frozenset(
    {RefreshOutcome.NOT_DUE, RefreshOutcome.UNCHANGED, RefreshOutcome.ACTIVATED}
)


# ------------------------------------------------------------------ state rollups


def _state_counts(store: StateStore) -> dict[str, int]:
    counts = {state.value: 0 for state in FileState}
    for row in store.connection.execute(
        "SELECT state, COUNT(*) FROM files GROUP BY state", ()
    ).fetchall():
        counts[str(row[0])] = int(row[1])
    return counts


def _cohort_counts(store: StateStore) -> dict[str, int]:
    counts = {cohort.value: 0 for cohort in Cohort}
    for row in store.connection.execute(
        "SELECT cohort, COUNT(*) FROM files GROUP BY cohort", ()
    ).fetchall():
        counts[str(row[0])] = _as_int(row[1])
    return counts


def _detail_rollup(store: StateStore) -> list[dict[str, Any]]:
    rows = store.connection.execute(
        "SELECT state_detail, COUNT(*) AS n FROM files"
        " GROUP BY state_detail ORDER BY n DESC, state_detail LIMIT ?",
        (DETAIL_ROLLUP_LIMIT,),
    ).fetchall()
    return [{"detail": str(row[0]), "count": _as_int(row[1])} for row in rows]


def _baseline_payload(baseline: Baseline | None) -> dict[str, Any] | None:
    return None if baseline is None else asdict(baseline)


def _lock_payload(holder: LockHolder | None) -> dict[str, Any]:
    if holder is None:
        return {"held": False}
    return {
        "held": True,
        "owner_pid": holder.owner_pid,
        "acquired_iso": holder.acquired_iso,
        "lease_expires_iso": holder.lease_expires_iso,
    }


def _limits_payload(limits: BudgetLimits | None) -> dict[str, Any] | None:
    return None if limits is None else asdict(limits)


def budgets_ok(store: StateStore) -> bool:
    """Non-mutating budget-headroom peek mirroring the daemon's eligibility input."""
    view = store.budget_view()
    if view.paused_reason is not None:
        return False
    limits = store.budget_limits
    if limits is None:
        return True
    if view.pending_file_requests >= limits.max_pending_files:
        return False
    if view.pending_episode_jobs >= limits.max_pending_episode_jobs:
        return False
    return view.rolling_new_submissions < limits.rolling_new


def state_rollup(store: StateStore) -> dict[str, Any]:
    """Read-only rollup of the state DB: counts, cohorts, details, budgets, lock, meta."""
    by_state = _state_counts(store)
    queued = sum(by_state[state.value] for state in QUEUED_STATES)
    indeterminate = sum(by_state[state.value] for state in INDETERMINATE_STATES)
    view = store.budget_view()
    meta = {key: store.get_meta(key) for key in CYCLE_META_KEYS}
    return {
        "present": True,
        "path": str(store.path),
        "counts_by_state": by_state,
        "counts_queued": queued,
        "counts_verified": by_state[FileState.VERIFIED.value],
        "counts_indeterminate": indeterminate,
        "counts_by_cohort": _cohort_counts(store),
        "first_run_baseline": _baseline_payload(store.latest_baseline()),
        "detail_rollup": _detail_rollup(store),
        "mapping_conflicts": by_state[FileState.CONFLICT.value],
        "budget_view": asdict(view),
        "budget_limits": _limits_payload(store.budget_limits),
        "apply_lock": _lock_payload(store.lock_holder()),
        "writes_disabled": store.writes_disabled,
        "live_send_counts": dict(
            zip(("live_held_sends", "live_held_slots"), store.live_send_counts(), strict=True)
        ),
        "meta": meta,
    }


def _coverage_payload(rollup: Mapping[str, Any]) -> dict[str, Any]:
    tracked = sum(int(count) for count in rollup["counts_by_state"].values())
    verified = int(rollup["counts_verified"])
    percent = None if tracked == 0 else round(100.0 * verified / tracked, 1)
    return {
        "files_tracked_in_state": tracked,
        "files_verified": verified,
        "coverage_percent": percent,
        "denominator": "files tracked in the state DB (all recorded states)",
        "note": "based on last cycle inventory: coverage reflects the state written by the "
        "most recent cycle(s), not a live inventory re-scan",
    }


def _mapping_affected(store: StateStore) -> dict[str, int]:
    affected = {
        state.value: 0
        for state in (FileState.WAITING, FileState.AMBIGUOUS, FileState.INDETERMINATE_WRITE)
    }
    for row in store.connection.execute(
        "SELECT state, COUNT(*) FROM files WHERE state IN (?, ?, ?) GROUP BY state",
        (
            FileState.WAITING.value,
            FileState.AMBIGUOUS.value,
            FileState.INDETERMINATE_WRITE.value,
        ),
    ).fetchall():
        affected[str(row[0])] = _as_int(row[1])
    return affected


def _eligibility_inputs(
    store: StateStore, settings: Settings, mapping_active: bool, attestation_status: str
) -> dict[str, Any]:
    gate = apply_gate_status(settings)
    return {
        "apply_gate": gate.describe(),
        "attestation": attestation_status,
        "attestation_gate": store.get_meta(META_ATTESTATION_GATE),
        "mapping_generation_active": mapping_active,
        "budgets_ok": budgets_ok(store),
        "source_stable": store.get_meta(META_REQUIRES_REBASELINE) != INDICATOR_TRUE,
        "writes_disabled": store.writes_disabled,
    }


def _attestation_status(attestation: Mapping[str, Any] | None) -> str:
    return "absent" if attestation is None else str(attestation.get("status", "absent"))


def _attestation_digest(attestation: Mapping[str, Any] | None) -> str | None:
    return None if attestation is None else attestation.get("digest")


def _attestation_expires_iso(attestation: Mapping[str, Any] | None) -> str | None:
    if attestation is None:
        return None
    record = attestation.get("record")
    return None if not isinstance(record, dict) else record.get("expires_iso")


def campaign_rollup(store: StateStore) -> list[dict[str, Any]]:
    """Read-only campaign rows for the report (nonsecret digest-bound summaries)."""
    rows: list[dict[str, Any]] = []
    for campaign in store.list_campaigns():
        rows.append(
            {
                "campaign_id": campaign.campaign_id,
                "digest": campaign.digest,
                "status": campaign.status,
                "status_reason": campaign.status_reason,
                "max_submissions": campaign.max_submissions,
                "deadline_iso": campaign.deadline_iso,
                "attestation_scope": campaign.attestation_scope,
                "approval_iso": campaign.approval_iso,
                "total_sent": campaign.total_sent,
                "last_batch_iso": campaign.last_batch_iso,
                "approved_files": len(campaign.bindings),
                "progress": store.campaign_progress(campaign.campaign_id),
            }
        )
    return rows


def report_payload(
    store: StateStore | None,
    mapping_status: MappingStatus,
    settings: Settings,
    *,
    now: datetime,
    refresh_hours: float,
    attestation: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """The ``report`` command payload: rollup + coverage + 3-level observability.

    ``attestation`` is the verdict payload from
    :meth:`shoko_linker.attestation.AttestationVerdict.payload` (``None`` = absent).
    """
    attestation_status = _attestation_status(attestation)
    mapping = mapping_status_payload(
        mapping_status,
        now=now,
        refresh_hours=refresh_hours,
        affected=None if store is None else _mapping_affected(store),
    )
    if store is None:
        return {
            "state": {"present": False, "path": settings.database.path},
            "mapping": mapping,
            "coverage": {
                "files_tracked_in_state": 0,
                "files_verified": 0,
                "coverage_percent": None,
                "denominator": "files tracked in the state DB (no state rows yet)",
                "note": "based on last cycle inventory: no cycle has written state yet",
            },
            "safety": {"attestation": attestation_status},
            "levels": {
                "alive": False,
                "reconciliation_ready": False,
                "apply_eligible": False,
                "last_cycle": {},
                "eligibility_inputs": {
                    "apply_gate": apply_gate_status(settings).describe(),
                    "attestation": attestation_status,
                    "attestation_gate": None,
                    "mapping_generation_active": mapping_status.active_present,
                    "budgets_ok": True,
                    "source_stable": True,
                    "writes_disabled": False,
                },
            },
            "generated_iso": _iso(now),
        }
    rollup = state_rollup(store)
    meta = rollup["meta"]
    return {
        "state": {"present": True, "path": str(store.path)},
        "counts_by_state": rollup["counts_by_state"],
        "counts_queued": rollup["counts_queued"],
        "counts_verified": rollup["counts_verified"],
        "counts_indeterminate": rollup["counts_indeterminate"],
        "cohorts": rollup["counts_by_cohort"],
        "first_run_baseline": rollup["first_run_baseline"],
        "detail_rollup": rollup["detail_rollup"],
        "mapping": mapping,
        "mapping_conflicts": rollup["mapping_conflicts"],
        "safety": {
            "attestation": attestation_status,
            "attestation_note": f"attestation: {attestation_status}",
            "attestation_digest": _attestation_digest(attestation),
            "attestation_expires_iso": _attestation_expires_iso(attestation),
            "apply_lock": rollup["apply_lock"],
            "writes_disabled": rollup["writes_disabled"],
            "writes_disabled_reason": meta[META_WRITES_DISABLED_REASON],
            "paused_reason": rollup["budget_view"]["paused_reason"],
            "last_refusal": meta[META_LAST_REFUSAL],
            "requires_rebaseline": meta[META_REQUIRES_REBASELINE] == INDICATOR_TRUE,
        },
        "campaigns": campaign_rollup(store),
        "outstanding_jobs": {
            "live_held_sends": rollup["live_send_counts"]["live_held_sends"],
            "live_held_slots": rollup["live_send_counts"]["live_held_slots"],
            "pending_file_requests": rollup["budget_view"]["pending_file_requests"],
            "pending_episode_jobs": rollup["budget_view"]["pending_episode_jobs"],
            "budget_view": rollup["budget_view"],
            "budget_limits": rollup["budget_limits"],
        },
        "coverage": _coverage_payload(rollup),
        "levels": {
            "alive": meta[META_CYCLE_ALIVE] == INDICATOR_TRUE,
            "reconciliation_ready": meta[META_CYCLE_RECONCILIATION_READY] == INDICATOR_TRUE,
            "apply_eligible": meta[META_CYCLE_APPLY_ELIGIBLE] == INDICATOR_TRUE,
            "last_cycle": {
                key: meta[key]
                for key in (
                    META_CYCLE_KEY,
                    META_CYCLE_MODE,
                    META_CYCLE_FAILED,
                    META_CYCLE_SUBMITTED,
                    META_LAST_CYCLE_START_ISO,
                    META_LAST_CYCLE_END_ISO,
                    META_NEXT_CYCLE_DUE_ISO,
                    META_LAST_FULL_RECONCILE_ISO,
                    META_NEXT_FULL_RECONCILE_ISO,
                )
            },
            "eligibility_inputs": _eligibility_inputs(
                store, settings, mapping_status.active_present, attestation_status
            ),
        },
        "generated_iso": _iso(now),
    }


def health_payload(
    store: StateStore | None,
    mapping_status: MappingStatus,
) -> dict[str, Any]:
    """Cheap liveness view from state/meta and the mapping store only (no network)."""
    age_hours = (
        round(mapping_status.active_age_seconds / 3600.0, 3)
        if mapping_status.active_age_seconds is not None
        else None
    )
    if store is None:
        return {
            "alive": False,
            "last_cycle_iso": None,
            "cycle_ok": False,
            "mapping_age_hours": age_hours,
            "mapping_generation_active": mapping_status.active_present,
            "state_present": False,
            "counts": {},
        }
    by_state = _state_counts(store)
    return {
        "alive": store.get_meta(META_CYCLE_ALIVE) == INDICATOR_TRUE,
        "last_cycle_iso": store.get_meta(META_LAST_CYCLE_END_ISO),
        "cycle_ok": store.get_meta(META_CYCLE_FAILED) != INDICATOR_TRUE,
        "mapping_age_hours": age_hours,
        "mapping_generation_active": mapping_status.active_present,
        "state_present": True,
        "counts": {
            "tracked": sum(by_state.values()),
            "verified": by_state[FileState.VERIFIED.value],
            "queued": sum(by_state[state.value] for state in QUEUED_STATES),
            "indeterminate": sum(by_state[state.value] for state in INDETERMINATE_STATES),
        },
    }


def operator_action_payload(
    command: str, note: str, *, iso: str, result: Mapping[str, Any]
) -> dict[str, Any]:
    """Nonsecret record of one operator mutation for ``--json`` output and the audit meta."""
    return {
        "command": command,
        "note": note,
        "iso": iso,
        "result": dict(result),
    }


def campaign_apply_payload(report: CampaignApplyReport) -> dict[str, Any]:
    """Nonsecret serialization of one ``campaign apply`` batch."""
    return {
        "campaign_id": report.campaign_id,
        "processed": report.processed,
        "submitted": report.submitted,
        "verified": report.verified,
        "quarantined": report.quarantined,
        "deferred": report.deferred,
        "total_sent": report.total_sent,
        "remaining": report.remaining,
        "stopped": report.stopped,
        "reasons": list(report.reasons),
    }


def campaign_apply_summary_line(report: CampaignApplyReport) -> str:
    """One human-readable ``campaign apply`` summary line (no secrets)."""
    return (
        f"campaign {report.campaign_id}: processed={report.processed} submitted={report.submitted} "
        f"verified={report.verified} quarantined={report.quarantined} deferred={report.deferred} "
        f"total_sent={report.total_sent} remaining={report.remaining}"
        f"{f' stopped={report.stopped}' if report.stopped else ''}"
    )


def audit_payload(report: AuditReport) -> dict[str, Any]:
    """Nonsecret serialization of one existing-link audit pass."""
    return {
        "examined": report.examined,
        "linked_examined": report.linked_examined,
        "requests_made": report.requests_made,
        "max_requests": report.max_requests,
        "truncated": report.truncated,
        "failures": list(report.failures),
        "counts": dict(report.counts),
        "findings": [
            {
                "shoko_file_id": finding.shoko_file_id,
                "classification": finding.classification.value,
                "sonarr_series_id": finding.sonarr_series_id,
                "sonarr_file_id": finding.sonarr_file_id,
                "linked_identities": [list(item) for item in finding.linked_identities],
                "target_identities": [list(item) for item in finding.target_identities],
                "detail": finding.detail,
            }
            for finding in report.findings
        ],
    }


def audit_summary_line(report: AuditReport) -> str:
    """One human-readable audit summary line (no secrets)."""
    counts = ", ".join(f"{name}={count}" for name, count in sorted(report.counts.items()) if count)
    return (
        f"audit: examined={report.examined} requests={report.requests_made}/{report.max_requests} "
        f"{'TRUNCATED ' if report.truncated else ''}{counts or 'no linked files examined'}"
    )


def operator_audit_entries(store: StateStore) -> list[dict[str, Any]]:
    """The persisted operator-action audit list (best-effort JSON meta list)."""
    raw = store.get_meta(META_OPERATOR_ACTIONS)
    if raw is None:
        return []
    try:
        document = json.loads(raw)
    except ValueError:
        return []
    if not isinstance(document, list):
        return []
    return [item for item in document if isinstance(item, dict)]
