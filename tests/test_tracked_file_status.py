"""Local tracked-file totals and examples share a consistent, bounded projection."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from shoko_linker.file_display import tracked_file_display
from shoko_linker.logging_setup import register_secret
from shoko_linker.paths import PathMapping
from shoko_linker.reporting import discovery_exclusion_payload
from shoko_linker.state import (
    META_LAST_DISCOVERY_EXCLUSIONS,
    Cohort,
    FileIdentitySnapshot,
    FileState,
    ServiceFileCounts,
    StateStore,
    open_state,
)

ISO = "2026-10-29T10:00:00+00:00"


def seed(store: StateStore, file_id: int, state: FileState, detail: str = "") -> None:
    store.record_file(
        file_id,
        sonarr_series_id=1,
        sonarr_file_id=file_id,
        sonarr_path=f"/sonarr/Anime/Episode {file_id}.mkv",
        sonarr_size=100,
        cohort=Cohort.BACKLOG,
        iso=ISO,
        state=state,
        identity=FileIdentitySnapshot(shoko_paths=(f"/shoko/Anime/Episode {file_id}.mkv",)),
    )
    store.connection.execute(
        "UPDATE files SET state_detail = ? WHERE shoko_file_id = ?", (detail, file_id)
    )


def test_waiting_review_and_exclusions_are_distinct_durable_populations(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    with open_state(path) as store:
        for file_id in range(1, 37):
            seed(store, file_id, FileState.WAITING, "pending_metadata")
        for file_id in range(37, 45):
            seed(store, file_id, FileState.CONFLICT, "protected_link")
        seed(store, 45, FileState.VERIFIED)
        store.set_meta(
            META_LAST_DISCOVERY_EXCLUSIONS,
            json.dumps(
                {
                    "version": 1,
                    "total": 18,
                    "counts": {"outside_scope": 18},
                    "scope_excluded": 18,
                    "examples": [],
                }
            ),
        )
        before = store.connection.total_changes
        expected = store.service_file_status(example_limit=5)
        assert expected.counts == store.service_file_counts()
        assert expected.waiting.total == 36
        assert expected.review.total == 8
        assert expected.counts.verified_files == 1
        assert expected.waiting.reason_counts == (("metadata", 36),)
        assert len(expected.waiting.examples) == len(expected.review.examples) == 5
        assert [row.shoko_file_id for row in expected.waiting.examples] == [1, 2, 3, 4, 5]
        assert discovery_exclusion_payload(store)["total"] == 18
        assert store.connection.total_changes == before
        assert not store.connection.in_transaction
    with open_state(path) as restarted:
        assert restarted.service_file_status(example_limit=5) == expected


def test_all_states_agree_with_totals_and_wait_reasons(tmp_path: Path) -> None:
    with open_state(tmp_path / "state.sqlite3") as store:
        for file_id, state in enumerate(FileState, 1):
            seed(store, file_id, state)
        seed(store, 100, FileState.WAITING, "pending_metadata")
        status = store.service_file_status()
        assert status.counts == store.service_file_counts()
        assert status.waiting.total == len(status.waiting.examples)
        assert status.review.total == len(status.review.examples)
        assert sum(count for _, count in status.waiting.reason_counts) == status.waiting.total
        assert sum(count for _, count in status.review.reason_counts) == status.review.total
        assert status.counts.verified_files + status.waiting.total + status.review.total == 15
        waiting = {row.state: row for row in status.waiting.examples}
        assert waiting["planned"].reason == "pre_send"
        assert "Not submitted" in waiting["planned"].message
        assert waiting["verifying"].reason == "verification"
        assert waiting["request_may_have_been_sent"].reason == "verification"
        assert status.waiting.examples[-1].reason == "metadata"
        assert "may not be queued" in status.waiting.examples[-1].message
        assert store.service_file_status(example_limit=0).waiting.examples == ()


def test_examples_show_stored_paths_and_sanitize_before_deriving_filename(tmp_path: Path) -> None:
    secret = "tracked_status_secret_very_long"
    register_secret(secret)
    with open_state(tmp_path / "state.sqlite3") as store:
        seed(store, 1, FileState.PLANNED)
        seed(store, 2, FileState.CONFLICT)
        store.connection.execute(
            "UPDATE files SET sonarr_path = ?, state_detail = ?, identity_hash_json = ?"
            " WHERE shoko_file_id = 2",
            (
                f"https://host/path?key={secret}\n",
                f"failed {secret}\x00 https://secret/key",
                json.dumps({"paths": [f"/anime/{secret}\n.mkv"]}),
            ),
        )
        status = store.service_file_status()
        normal = status.waiting.examples[0]
        assert normal.filename == "Episode 1.mkv"
        assert normal.sonarr_path == "/sonarr/Anime/Episode 1.mkv"
        assert normal.shoko_paths == ("/shoko/Anime/Episode 1.mkv",)
        unsafe = status.review.examples[0]
        assert store.service_file_page("review").rows == status.review.examples
        assert secret not in repr(unsafe)
        assert "https://" not in repr(unsafe)
        assert "\x00" not in unsafe.detail
        store.connection.execute(
            "UPDATE files SET sonarr_path = '', identity_hash_json = NULL WHERE shoko_file_id = 2"
        )
        assert store.service_file_status().review.examples[0].filename == "Filename unavailable"


def test_totals_and_examples_share_snapshot_during_worker_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "state.sqlite3"
    with open_state(path) as store, open_state(path) as worker:
        seed(store, 1, FileState.WAITING, "pending_metadata")
        original = store.service_file_counts

        def change_after_count() -> ServiceFileCounts:
            counts = original()
            worker.connection.execute("UPDATE files SET state = 'verified'")
            return counts

        monkeypatch.setattr(store, "service_file_counts", change_after_count)
        status = store.service_file_status()
        assert status.waiting.total == 1
        assert status.waiting.reason_counts == (("metadata", 1),)
        assert status.waiting.examples[0].state == "waiting"
        assert original().verified_files == 1


@pytest.mark.parametrize("limit", [-1, 21, True])
def test_example_limits_are_bounded(tmp_path: Path, limit: int) -> None:
    with (
        open_state(tmp_path / "state.sqlite3") as store,
        pytest.raises(ValueError, match="example_limit"),
    ):
        store.service_file_status(example_limit=limit)


@pytest.mark.parametrize("group", ["verified", "waiting", "review"])
def test_tracked_pages_traverse_exact_membership_and_restart(tmp_path: Path, group: str) -> None:
    path = tmp_path / "pages.sqlite3"
    with open_state(path) as store:
        states = {
            "verified": FileState.VERIFIED,
            "waiting": FileState.WAITING,
            "review": FileState.CONFLICT,
        }
        for file_id in range(1, 208):
            seed(store, file_id, states[group], "pending_metadata")
        before = store.connection.total_changes
        pages = [store.service_file_page(group, page=n, page_size=100) for n in range(1, 4)]
        assert [len(p.rows) for p in pages] == [100, 100, 7]
        assert [r.shoko_file_id for p in pages for r in p.rows] == list(range(1, 208))
        assert [(p.pagination.start, p.pagination.end) for p in pages] == [
            (1, 100),
            (101, 200),
            (201, 207),
        ]
        last = store.service_file_page(group, page=2**63 - 1, page_size=100)
        assert last.rows == pages[-1].rows
        assert last.pagination.page == 3
        assert last.pagination.requested_page == 2**63 - 1
        assert last.pagination.total == 207
        assert all(p.counts == store.service_file_counts() for p in pages)
        if group == "verified":
            assert all(r.reason == "verified" and "were verified" in r.message for r in last.rows)
        assert store.connection.total_changes == before
        assert not store.connection.in_transaction
    with open_state(path) as store:
        assert store.service_file_page(group, page=3, page_size=100) == pages[-1]


def test_page_classification_and_snapshot_match_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "pages.sqlite3"
    with open_state(path) as store, open_state(path) as worker:
        for file_id, state in enumerate(FileState, 1):
            seed(store, file_id, state)
        seed(store, 100, FileState.WAITING, "pending_metadata")
        status = store.service_file_status()
        assert store.service_file_page("waiting").rows == status.waiting.examples
        assert store.service_file_page("review").rows == status.review.examples
        original = store.service_file_counts

        def change_after_count() -> ServiceFileCounts:
            counts = original()
            worker.connection.execute("UPDATE files SET state='verified'")
            return counts

        monkeypatch.setattr(store, "service_file_counts", change_after_count)
        page = store.service_file_page("waiting")
        assert page.pagination.total == status.waiting.total
        assert page.rows == status.waiting.examples
        assert page.counts == status.counts
        assert original().waiting_files == 0


@pytest.mark.parametrize(
    "kwargs",
    [
        {"page": True},
        {"page": 0},
        {"page": -1},
        {"page": 2**63},
        {"page": "1"},
        {"page_size": True},
        {"page_size": 0},
        {"page_size": -1},
        {"page_size": 101},
        {"page_size": 2**90},
        {"page_size": "50"},
    ],
)
def test_page_parameters_rejected(tmp_path: Path, kwargs: dict[str, Any]) -> None:
    from shoko_linker.action_history import action_history_page

    with open_state(tmp_path / "pages.sqlite3") as store:
        for call in (
            lambda: store.service_file_page("waiting", **kwargs),
            lambda: action_history_page(store, **kwargs),
        ):
            with pytest.raises(ValueError, match="page"):
                call()


@pytest.mark.parametrize("group", ["other", "WAITING", "waiting' OR 1=1", True, [], None])
def test_page_group_rejected(tmp_path: Path, group: object) -> None:
    with open_state(tmp_path / "pages.sqlite3") as store, pytest.raises(ValueError, match="group"):
        store.service_file_page(group)  # type: ignore[arg-type]


def test_empty_pages_are_explicit(tmp_path: Path) -> None:
    from shoko_linker.action_history import action_history_page

    with open_state(tmp_path / "pages.sqlite3") as store:
        for result in (
            store.service_file_page("verified", page=9),
            action_history_page(store, page=9),
        ):
            assert result.rows == ()
            assert result.pagination.requested_page == 9
            assert result.pagination.page == result.pagination.page_count == 1
            assert result.pagination.start == result.pagination.end == result.pagination.total == 0


def test_tracked_page_does_not_decode_off_page_identity(tmp_path: Path) -> None:
    with open_state(tmp_path / "pages.sqlite3") as store:
        for file_id in range(1, 130):
            seed(store, file_id, FileState.WAITING)
        evaluated: list[str] = []

        def measured_length(value: str | bytes | None) -> int | None:
            if isinstance(value, str) and value.startswith("/sonarr/"):
                evaluated.append(value)
            return None if value is None else len(value)

        store.connection.create_function("length", 1, measured_length)
        result = store.service_file_page("waiting", page=2)
        assert [row.shoko_file_id for row in result.rows] == list(range(51, 101))
        assert evaluated == [
            f"/sonarr/Anime/Episode {i}.mkv" for i in range(51, 101) for _ in range(2)
        ]


def test_paged_paths_keep_bounded_full_context_for_library_projection(tmp_path: Path) -> None:
    with open_state(tmp_path / "pages.sqlite3") as store:
        seed(store, 1, FileState.VERIFIED)
        path = "/media/" + "long-directory/" * 60 + "Episode.mkv"
        store.connection.execute("UPDATE files SET sonarr_path=?", (path,))
        row = store.service_file_page("verified").rows[0]
        assert row.sonarr_path == path
        assert row.filename == "Episode.mkv"


@pytest.mark.parametrize("api", ["status", "page"])
def test_discarded_oversized_identity_cannot_prove_display_library(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, api: str
) -> None:
    # The conflicting location must not disappear into apparently complete empty evidence.
    raw = json.dumps({"paths": ["/outside/E.mkv"], "padding": "x" * 65536})
    assert len(raw) > 65536
    with open_state(tmp_path / "oversized.sqlite3") as store:
        seed(store, 1, FileState.WAITING)
        store.connection.execute("UPDATE files SET identity_hash_json=?", (raw,))
        original_loads = json.loads
        decoded: list[str] = []

        def bounded_loads(value: str) -> Any:
            assert len(value) <= 65536, "oversized identity hydrated into Python"
            decoded.append(value)
            return original_loads(value)

        monkeypatch.setattr(json, "loads", bounded_loads)
        before = store.connection.total_changes
        row = (
            store.service_file_status().waiting.examples[0]
            if api == "status"
            else store.service_file_page("waiting").rows[0]
        )
        assert row.display_paths_complete is False
        assert row.shoko_paths == ()
        assert decoded == []
        display = tracked_file_display(row, ("/sonarr/Anime",), (PathMapping("/sonarr", "/shoko"),))
        assert display.filename == "Episode 1.mkv"
        assert display.relative_path is None
        assert display.library is None
        assert store.connection.total_changes == before


@pytest.mark.parametrize("api", ["status", "page"])
@pytest.mark.parametrize("length", [65535, 65536, 65537])
@pytest.mark.parametrize("padding", ["x", "\u00e9"])
def test_identity_display_limit_uses_sqlite_character_boundary(
    tmp_path: Path, api: str, length: int, padding: str
) -> None:
    base = json.dumps({"paths": ["/shoko/Anime/Episode 1.mkv"], "padding": ""}, ensure_ascii=False)
    raw = base.replace('"padding": ""', '"padding": "' + padding * (length - len(base)) + '"')
    assert len(raw) == length
    with open_state(tmp_path / "boundary.sqlite3") as store:
        seed(store, 1, FileState.WAITING)
        store.connection.execute("UPDATE files SET identity_hash_json=?", (raw,))
        row = (
            store.service_file_status().waiting.examples[0]
            if api == "status"
            else store.service_file_page("waiting").rows[0]
        )
        display = tracked_file_display(row, ("/sonarr/Anime",), (PathMapping("/sonarr", "/shoko"),))
        if length <= 65536:
            assert row.display_paths_complete is True
            assert row.shoko_paths == ("/shoko/Anime/Episode 1.mkv",)
            assert display.library == "/sonarr/Anime"
            assert display.relative_path == "."
        else:
            assert row.display_paths_complete is False
            assert row.shoko_paths == ()
            assert display.library is display.relative_path is None


@pytest.mark.parametrize("api", ["status", "page"])
@pytest.mark.parametrize("identity", [None, "", "{broken", "null"])
def test_missing_identity_remains_distinct_from_malformed_identity(
    tmp_path: Path, api: str, identity: str | None
) -> None:
    with open_state(tmp_path / "missing.sqlite3") as store:
        seed(store, 1, FileState.WAITING)
        store.connection.execute("UPDATE files SET identity_hash_json=?", (identity,))
        row = (
            store.service_file_status().waiting.examples[0]
            if api == "status"
            else store.service_file_page("waiting").rows[0]
        )
        assert row.display_paths_complete is (identity is None)
        display = tracked_file_display(row, ("/sonarr/Anime",), (PathMapping("/sonarr", "/shoko"),))
        assert display.library == ("/sonarr/Anime" if identity is None else None)


@pytest.mark.parametrize("api", ["status", "page"])
@pytest.mark.parametrize("length", [8191, 8192, 8193])
@pytest.mark.parametrize("padding", ["x", "\u00e9"])
def test_discarded_source_preserves_combined_display_proof(
    tmp_path: Path, api: str, length: int, padding: str
) -> None:
    # A valid Shoko location cannot hide contradictory source evidence omitted by SQL.
    template = "/outside/Other.mkv"
    source = "/outside/" + padding * (length - len(template)) + "Other.mkv"
    assert len(source) == length
    with open_state(tmp_path / "source.sqlite3") as store:
        seed(store, 1, FileState.WAITING)
        store.connection.execute("UPDATE files SET sonarr_path=?", (source,))
        row = (
            store.service_file_status().waiting.examples[0]
            if api == "status"
            else store.service_file_page("waiting").rows[0]
        )
        assert row.sonarr_path == (source if length <= 8192 else "")
        assert row.display_paths_complete is (length <= 8192)
        assert row.shoko_paths == ("/shoko/Anime/Episode 1.mkv",)
        display = tracked_file_display(row, ("/sonarr/Anime",), (PathMapping("/sonarr", "/shoko"),))
        assert display.library is display.relative_path is None


@pytest.mark.parametrize("api", ["status", "page"])
@pytest.mark.parametrize(
    "source",
    [
        b"/outside/Other.mkv",
        "/outside/" + "long/" * 1700 + "Other.mkv",
        "relative/Other.mkv",
        "/outside/Bad\x00Name.mkv",
        "/outside/Bad\nName.mkv",
    ],
)
def test_invalid_source_does_not_enable_shoko_fallback(
    tmp_path: Path, api: str, source: str | bytes
) -> None:
    with open_state(tmp_path / "source.sqlite3") as store:
        seed(store, 1, FileState.WAITING)
        store.connection.execute("UPDATE files SET sonarr_path=?", (source,))
        row = (
            store.service_file_status().waiting.examples[0]
            if api == "status"
            else store.service_file_page("waiting").rows[0]
        )
        assert row.display_paths_complete is False
        display = tracked_file_display(row, ("/sonarr/Anime",), (PathMapping("/sonarr", "/shoko"),))
        assert display.library is display.relative_path is None


@pytest.mark.parametrize("api", ["status", "page"])
def test_genuinely_empty_source_allows_complete_shoko_fallback(tmp_path: Path, api: str) -> None:
    with open_state(tmp_path / "source.sqlite3") as store:
        seed(store, 1, FileState.WAITING)
        store.connection.execute("UPDATE files SET sonarr_path=''")
        row = (
            store.service_file_status().waiting.examples[0]
            if api == "status"
            else store.service_file_page("waiting").rows[0]
        )
        assert row.display_paths_complete is True
        display = tracked_file_display(row, ("/sonarr/Anime",), (PathMapping("/sonarr", "/shoko"),))
        assert display.library == "/sonarr/Anime"
        assert display.relative_path == "."
        assert display.filename == "Episode 1.mkv"


@pytest.mark.parametrize("api", ["status", "page"])
@pytest.mark.parametrize("bad_source", [False, True])
@pytest.mark.parametrize("bad_identity", [False, True])
def test_display_completeness_combines_all_identity_inputs(
    tmp_path: Path, api: str, bad_source: bool, bad_identity: bool
) -> None:
    source = "/outside/" + "x" * 8192 if bad_source else "/sonarr/Anime/Episode 1.mkv"
    identity = json.dumps(
        {"paths": ["/shoko/Anime/Episode 1.mkv"], "padding": "x" * (65536 if bad_identity else 0)}
    )
    with open_state(tmp_path / "combined.sqlite3") as store:
        seed(store, 1, FileState.WAITING)
        store.connection.execute(
            "UPDATE files SET sonarr_path=?, identity_hash_json=?", (source, identity)
        )
        row = (
            store.service_file_status().waiting.examples[0]
            if api == "status"
            else store.service_file_page("waiting").rows[0]
        )
        complete = not bad_source and not bad_identity
        assert row.display_paths_complete is complete
        display = tracked_file_display(row, ("/sonarr/Anime",), (PathMapping("/sonarr", "/shoko"),))
        assert display.library == ("/sonarr/Anime" if complete else None)
        assert display.relative_path == ("." if complete else None)
