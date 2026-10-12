"""F17 fixtures: the exact schema DDL shipped by the previous (v1) implementation.

``_V1_SCHEMA_SQL`` is the **verbatim** ``_SCHEMA_SQL`` body from commit
``6d4183b408ee9835c65d035b0e9fb7ec6df9d962`` (schema v1: the campaigns table has
13 columns and NO F10 drain-authorization columns). Do not edit it: the F17
migration tests build real v1-era databases from this text and compare every row
before/after the upgrade. Helpers create/populate such a database with raw SQL that
mirrors the v1 column set (no dependence on the current store API).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

V1_COMMIT: str = "6d4183b408ee9835c65d035b0e9fb7ec6df9d962"
V1_USER_VERSION: int = 1

_V1_SCHEMA_SQL = """
CREATE TABLE schema_version (
    version INTEGER NOT NULL,
    applied_iso TEXT NOT NULL,
    description TEXT NOT NULL
);
CREATE TABLE meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE baselines (
    baseline_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_iso TEXT NOT NULL,
    complete INTEGER NOT NULL CHECK (complete IN (0, 1)),
    auto_apply_eligible INTEGER NOT NULL CHECK (auto_apply_eligible IN (0, 1)),
    series_snapshot_hash TEXT NOT NULL DEFAULT '',
    reason TEXT NOT NULL DEFAULT ''
);
CREATE TABLE baseline_files (
    baseline_id INTEGER NOT NULL REFERENCES baselines (baseline_id),
    sonarr_series_id INTEGER NOT NULL,
    sonarr_file_id INTEGER NOT NULL,
    path TEXT NOT NULL,
    size INTEGER NOT NULL,
    PRIMARY KEY (baseline_id, sonarr_series_id, sonarr_file_id)
);
CREATE TABLE files (
    shoko_file_id INTEGER PRIMARY KEY,
    sonarr_series_id INTEGER,
    sonarr_file_id INTEGER,
    sonarr_path TEXT NOT NULL DEFAULT '',
    sonarr_size INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL,
    cohort TEXT NOT NULL CHECK (cohort IN ('backlog', 'new_arrival', 'campaign')),
    plan_fingerprint TEXT,
    request_fingerprint TEXT,
    mapping_targets_digest TEXT,
    mapping_structure_digest TEXT,
    mapping_revision TEXT,
    policy TEXT,
    campaign_id TEXT,
    state_detail TEXT NOT NULL DEFAULT '',
    updated_iso TEXT NOT NULL,
    first_seen_iso TEXT NOT NULL,
    last_verified_iso TEXT,
    identity_hash_json TEXT
);
CREATE TABLE sends (
    send_id INTEGER PRIMARY KEY AUTOINCREMENT,
    shoko_file_id INTEGER NOT NULL REFERENCES files (shoko_file_id),
    request_fingerprint TEXT NOT NULL,
    plan_fingerprint TEXT NOT NULL,
    attempted_iso TEXT NOT NULL,
    http_status INTEGER,
    outcome TEXT NOT NULL,
    verified_iso TEXT,
    verification TEXT,
    episode_jobs INTEGER NOT NULL DEFAULT 1,
    target_ids_json TEXT NOT NULL DEFAULT '[]',
    slot_released INTEGER NOT NULL DEFAULT 0 CHECK (slot_released IN (0, 1))
);
CREATE TABLE campaigns (
    campaign_id TEXT PRIMARY KEY,
    digest TEXT NOT NULL,
    plan_fingerprint_set TEXT NOT NULL DEFAULT '[]',
    max_submissions INTEGER NOT NULL,
    deadline_iso TEXT,
    attestation_scope TEXT NOT NULL DEFAULT '',
    approval_iso TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'approved'
        CHECK (status IN ('approved', 'paused', 'revoked')),
    status_reason TEXT NOT NULL DEFAULT '',
    total_sent INTEGER NOT NULL DEFAULT 0,
    last_batch_iso TEXT,
    created_iso TEXT NOT NULL,
    plan_path TEXT
);
CREATE TABLE budgets (
    window_key TEXT PRIMARY KEY,
    cycle_new_submissions INTEGER NOT NULL DEFAULT 0,
    rolling_new_submissions INTEGER,
    rolling_window_start_iso TEXT,
    pending_file_requests INTEGER,
    pending_episode_jobs INTEGER,
    paused_reason TEXT,
    completed_submissions INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX idx_files_state ON files (state);
CREATE INDEX idx_files_cohort ON files (cohort);
CREATE INDEX idx_files_campaign ON files (campaign_id);
CREATE INDEX idx_files_path ON files (sonarr_path);
CREATE INDEX idx_files_series_file ON files (sonarr_series_id, sonarr_file_id);
CREATE INDEX idx_sends_request_fingerprint ON sends (request_fingerprint);
CREATE INDEX idx_sends_file ON sends (shoko_file_id);
CREATE INDEX idx_baseline_files_identity ON baseline_files (sonarr_series_id, sonarr_file_id);
"""


def create_v1_database(path: Path | str) -> None:
    """Create a schema-v1 database exactly as the previous implementation did."""
    conn = sqlite3.connect(str(path))
    try:
        conn.executescript(_V1_SCHEMA_SQL)
        conn.execute(f"PRAGMA user_version={V1_USER_VERSION}")
        conn.commit()
    finally:
        conn.close()


T0 = "2026-01-01T00:00:00+00:00"
T1 = "2026-01-01T01:00:00+00:00"
T2 = "2026-01-02T01:00:00+00:00"
T3 = "2026-01-03T00:00:00+00:00"

PLAN_FP = "a" * 64
REQ_FP = "b" * 64
REQ_FP_2 = "c" * 64
REQ_FP_3 = "d" * 64
CAMPAIGN_DIGEST = "e" * 64
TARGETS = "f" * 64
STRUCTURE = "0" * 64
CAMPAIGN_ID = "camp-f17"
CYCLE_KEY = "daemon-run1-3"
PAUSE_REASON = "apply-pause: rolling submission ceiling hit"


def populate_v1_database(path: Path | str) -> None:
    """Fill a v1 database with durable history using raw v1-column SQL.

    Mirrors the real v1 columns: a completed baseline, a verified backlog
    file, a campaign with an approved row and two bound files (one
    request_may_have_been_sent, one indeterminate_write), the matching send
    history, the rolling ``_global`` budget row (with its stored apply-pause
    reason) and the per-cycle budget row plus budget meta keys.
    """
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            "INSERT INTO baselines (created_iso, complete, auto_apply_eligible,"
            " series_snapshot_hash, reason) VALUES (?, 1, 1, 'snapshot-hash', 'initial scan')",
            (T0,),
        )
        conn.execute(
            "INSERT INTO baseline_files (baseline_id, sonarr_series_id, sonarr_file_id,"
            " path, size) VALUES (1, 7, 70, '/data/media/anime/show/s01e01.mkv', 1234)",
        )
        for file_id, state, campaign_id, req_fp in (
            (101, "verified", None, REQ_FP),
            (102, "request_may_have_been_sent", CAMPAIGN_ID, REQ_FP_2),
            (103, "indeterminate_write", CAMPAIGN_ID, REQ_FP_3),
        ):
            conn.execute(
                "INSERT INTO files (shoko_file_id, sonarr_series_id, sonarr_file_id,"
                " sonarr_path, sonarr_size, state, cohort, plan_fingerprint,"
                " request_fingerprint, mapping_targets_digest, mapping_structure_digest,"
                " mapping_revision, policy, campaign_id, state_detail, updated_iso,"
                " first_seen_iso, last_verified_iso)"
                " VALUES (?, 1, ?, ?, 1234, ?, ?, ?, ?, ?, ?, '7', 'auto', ?, ?, ?, ?, ?)",
                (
                    file_id,
                    file_id + 100,
                    f"/data/media/anime/show/s01e{file_id - 100}.mkv",
                    state,
                    "backlog" if campaign_id is None else "campaign",
                    PLAN_FP,
                    req_fp,
                    TARGETS,
                    STRUCTURE,
                    campaign_id,
                    f"v1-{state}",
                    T2,
                    T0,
                    T2 if state == "verified" else None,
                ),
            )
        conn.execute(
            "INSERT INTO sends (shoko_file_id, request_fingerprint, plan_fingerprint,"
            " attempted_iso, http_status, outcome, verified_iso, verification,"
            " episode_jobs, target_ids_json, slot_released)"
            " VALUES (101, ?, ?, ?, 202, 'accepted_unverified', ?, 'verified', 1,"
            " '[[1, 2, 3]]', 1)",
            (REQ_FP, PLAN_FP, T1, T2),
        )
        conn.execute(
            "INSERT INTO sends (shoko_file_id, request_fingerprint, plan_fingerprint,"
            " attempted_iso, http_status, outcome, episode_jobs, target_ids_json,"
            " slot_released) VALUES (102, ?, ?, ?, 202, 'accepted_unverified', 1,"
            " '[[4, 5, 6]]', 0)",
            (REQ_FP_2, PLAN_FP, T1),
        )
        conn.execute(
            "INSERT INTO sends (shoko_file_id, request_fingerprint, plan_fingerprint,"
            " attempted_iso, outcome, episode_jobs, target_ids_json, slot_released)"
            " VALUES (103, ?, ?, ?, 'ambiguous_send', 1, '[[7, 8, 9]]', 0)",
            (REQ_FP_3, PLAN_FP, T1),
        )
        conn.execute(
            "INSERT INTO campaigns (campaign_id, digest, plan_fingerprint_set,"
            " max_submissions, deadline_iso, attestation_scope, approval_iso, status,"
            " status_reason, total_sent, last_batch_iso, created_iso, plan_path)"
            " VALUES (?, ?, ?, 10, ?, 'campaign:canary', ?, 'approved', '', 2, ?, ?, ?)",
            (
                CAMPAIGN_ID,
                CAMPAIGN_DIGEST,
                f'[[102, "{PLAN_FP}", "{REQ_FP_2}"], [103, "{PLAN_FP}", "{REQ_FP_3}"]]',
                T3,
                T1,
                T2,
                T0,
                "/data/plans/campaign.json",
            ),
        )
        conn.execute(
            "INSERT INTO budgets (window_key, cycle_new_submissions,"
            " rolling_new_submissions, rolling_window_start_iso, pending_file_requests,"
            " pending_episode_jobs, paused_reason, completed_submissions)"
            " VALUES ('_global', 0, 5, ?, 2, 2, ?, 4)",
            (T0, PAUSE_REASON),
        )
        conn.execute(
            "INSERT INTO budgets (window_key, cycle_new_submissions, completed_submissions)"
            " VALUES (?, 2, 1)",
            (CYCLE_KEY,),
        )
        for key, value in (
            ("budget_last_cycle_key", CYCLE_KEY),
            ("budget_run_id", "run1"),
            ("budget_cycle_seq", "3"),
        ):
            conn.execute("INSERT INTO meta (key, value) VALUES (?, ?)", (key, value))
        conn.execute(
            "INSERT INTO schema_version (version, applied_iso, description)"
            " VALUES (1, ?, 'initial v1 schema')",
            (T0,),
        )
        conn.commit()
    finally:
        conn.close()


_TABLE_ORDER: tuple[str, ...] = (
    "schema_version",
    "meta",
    "baselines",
    "baseline_files",
    "files",
    "sends",
    "campaigns",
    "budgets",
)


def snapshot_state(path: Path | str) -> dict[str, list[tuple[object, ...]]]:
    """Every persisted row per table (stable order) for before/after comparison."""
    conn = sqlite3.connect(str(path))
    try:
        return {
            table: conn.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()  # noqa: S608
            for table in _TABLE_ORDER
        }
    finally:
        conn.close()


def build_v1_database(path: Path | str) -> dict[str, list[tuple[object, ...]]]:
    """Create + populate a v1-era database and return its pre-migration snapshot."""
    create_v1_database(path)
    populate_v1_database(path)
    return snapshot_state(path)
