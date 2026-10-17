"""Offline behavioral regressions for mapping permutations, activation and composites."""

import json
import sqlite3
from dataclasses import replace
from pathlib import Path
from threading import Event, Thread

import httpx
import pytest
import test_daemon as td

from shoko_linker import mapping_store as cache_module
from shoko_linker.campaign import (
    CampaignEligibilityRefusalError,
    apply_campaign,
    approve_campaign,
    authorize_campaign_drain,
    prepare_campaign,
    write_plan_file,
)
from shoko_linker.daemon import CycleMode, DaemonDeps, run_cycle
from shoko_linker.linking import LinkPlan, SendOutcome, request_fingerprint
from shoko_linker.mapping_store import RefreshOutcome
from shoko_linker.mappings import build_mapping_index, parse_anime_lists, resolve, targets_digest
from shoko_linker.policies import RunPolicy
from shoko_linker.state import (
    BudgetRefusal,
    Cohort,
    FileState,
    SendToken,
    StateError,
    StateStore,
    WriteDisabledError,
)


def _refresh_mapping_source(deps: DaemonDeps, body: bytes, now_iso: str) -> RefreshOutcome:
    endpoint = td.source_endpoint(
        lambda request: httpx.Response(
            200,
            headers={"ETag": '"invalid-source-test"'},
            stream=httpx.ByteStream(body),
            request=request,
        )
    )
    try:
        return cache_module.refresh(
            deps.mapping_store,
            endpoint,
            now=td._parse(now_iso),
            refresh_hours=deps.settings.mappings.refresh_hours,
            force=True,
        )
    finally:
        endpoint.client.close()


def explicit_xml(body: str) -> str:
    return (
        '<anime-list><anime anidbid="1234" tvdbid="267440" defaulttvdbseason="1">'
        '<mapping-list><mapping anidbseason="1" tvdbseason="1">'
        f"{body}</mapping></mapping-list></anime></anime-list>"
    )


def test_permutation_changes_semantic_digest() -> None:
    old = build_mapping_index(parse_anime_lists(explicit_xml(";1-1;2-2;")))
    new = build_mapping_index(parse_anime_lists(explicit_xml(";1-2;2-1;")))
    assert targets_digest(old) != targets_digest(new)


def test_upgrade_retains_last_known_good_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Exact digest of the old destination-only algorithm for this two-pair fixture.
    legacy = "30792f7c90eceed98a1d2c335c92a0e26116bc3e568576839ad54dfef3309dd4"
    with monkeypatch.context() as patch:
        patch.setattr(cache_module, "targets_digest", lambda _: legacy)
        td.activate_mapping(tmp_path, explicit_xml(";1-1;2-2;"))
    active = cache_module.load_active(cache_module.MappingStore(tmp_path / "mappings"))
    assert active is not None


@pytest.mark.parametrize("external", [False, True])
@pytest.mark.parametrize("restart", [False, True])
def test_prior_targets_survive_activation_and_restart(
    tmp_path: Path, external: bool, restart: bool
) -> None:
    old = explicit_xml(";1-1;2-2;")
    new = explicit_xml(";1-2;2-1;")
    files = [td.SpecFile(1, 10, 500), td.SpecFile(1, 11, 501, ((1, 2, 5502, 778),))]
    td.activate_mapping(tmp_path, old)
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, files)
    shoko = td.ShokoStub(files)
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=files,
        shoko_files=files,
        xml=old,
        monotonic=td.Clock([100.0]),
        advance=lambda _: None,
        shoko=shoko,
    )
    deps = replace(deps, service_mode=True, service_authorized=lambda: True, max_submissions=0)
    try:
        first = run_cycle(
            deps, now_iso=td.T0, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="before"
        )
        assert first.plans[500].episode_ids == (777,)
        if external:
            td._activate_new_generation(deps, new, td.T1)
        if restart:
            store.close()
            store = td.open_tmp_store(tmp_path)
            deps = replace(deps, store=store)
        later = td.T2 if external else td._due_iso()
        deps = replace(
            deps, max_submissions=1, mapping_source=td.source_endpoint(td.source_handler(new))
        )
        second = run_cycle(
            deps, now_iso=later, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="after"
        )
        assert second.mapping_outcome is (
            RefreshOutcome.NOT_DUE if external else RefreshOutcome.ACTIVATED
        )
        assert shoko.posts == []
        assert store.get_file(500).state is FileState.CONFLICT  # type: ignore[union-attr]
        third = run_cycle(
            deps, now_iso=later, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="retry"
        )
        assert third.counts.submitted == 0
        assert shoko.posts == []
    finally:
        store.close()


def test_explicit_composite_is_resolved() -> None:
    index = build_mapping_index(parse_anime_lists(explicit_xml(";1-1;2-1;")))
    answer = resolve(index, 267440, 1, 1)
    assert answer.status.value == "resolved"
    assert [target.anidb_episode_number for target in answer.targets] == [1, 2]


def test_type_local_composite_keeps_complete_unique_targets_through_service_preflight(
    tmp_path: Path,
) -> None:
    xml = explicit_xml(";201-1;401-1;")
    source = td.SpecFile(1, 10, 500)
    metadata = replace(source, episodes=((1, 1, 5601, 780), (1, 1, 5602, 781)))

    class TypeLocalShokoStub(td.ShokoStub):
        def _chain_first(self, request: httpx.Request) -> httpx.Response:
            episode_type = request.url.params["type"]
            episode_id = {"Trailer": 5601, "Other": 5602}[episode_type]
            row = self._anidb_episode(episode_id, source.anime_id, 1)
            row["Type"] = episode_type
            return self._json(request, {"Total": 1, "List": [row]})

    td.activate_mapping(tmp_path, xml)
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [source])
    shoko = TypeLocalShokoStub([metadata])
    preflight_xrefs: list[list[dict[str, object]]] = []
    shoko.on_post = lambda: preflight_xrefs.append(list(shoko.linked.get(source.shoko_id, [])))
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[source],
        shoko_files=[metadata],
        xml=xml,
        monotonic=td.Clock([100.0]),
        advance=lambda _: None,
        shoko=shoko,
    )
    deps = replace(deps, service_mode=True, service_authorized=lambda: True)
    try:
        report = run_cycle(
            deps,
            now_iso=td.T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="typed-composite",
        )

        plan = report.plans[source.shoko_id]
        assert report.counts.verified == 1
        assert {(target.episode_type, target.anidb_episode_number) for target in plan.targets} == {
            ("Trailer", 1),
            ("Other", 1),
        }
        assert {
            (target.tvdb_series, target.season_number, target.episode_number, target.source_rule)
            for target in plan.targets
        } == {(267440, 1, 1, "explicit_pair")}
        assert set(plan.episode_ids) == {780, 781}
        assert len(plan.episode_ids) == len(set(plan.episode_ids)) == 2
        assert preflight_xrefs == [[]]
        assert len(shoko.posts) == 1
        assert len(shoko.posts[0]["EpisodeIDs"]) == 2
        assert set(shoko.posts[0]["EpisodeIDs"]) == {780, 781}
        assert {
            request.url.params.get("type")
            for request in shoko.requests
            if request.url.path == f"/api/v3/Series/AniDB/{source.anime_id}/Episode"
        } == {"Trailer", "Other"}
        linked_ids = {item["ID"] for item in shoko.linked[source.shoko_id][0]["EpisodeIDs"]}
        assert len(shoko.linked[source.shoko_id][0]["EpisodeIDs"]) == 2
        assert linked_ids == {780, 781}
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()


def test_malformed_pair_cannot_fallback_to_entry_offset_and_post(
    tmp_path: Path,
) -> None:
    xml = (
        '<anime-list><anime anidbid="1234" tvdbid="267440" defaulttvdbseason="1" '
        'episodeoffset="0"><mapping-list><mapping anidbseason="1" tvdbseason="1">'
        ";1-2oops;</mapping></mapping-list></anime></anime-list>"
    )
    source = td.SpecFile(1, 10, 500, episodes=((1, 2, 5502, 777),))
    td.activate_mapping(tmp_path, xml)
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [source])
    shoko = td.ShokoStub([source])
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[source],
        shoko_files=[source],
        xml=xml,
        monotonic=td.Clock([100.0]),
        advance=lambda _: None,
        shoko=shoko,
    )
    deps = replace(deps, service_mode=True, service_authorized=lambda: True)
    try:
        report = run_cycle(
            deps,
            now_iso=td.T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="malformed-mapping",
        )

        assert report.counts.submitted == 0
        assert report.counts.verified == 0
        assert shoko.posts == []
    finally:
        store.close()


@pytest.mark.parametrize(
    ("cached_xml", "source_xml", "anime_id"),
    [
        (
            '<anime-list><anime anidbid="100" tvdbid="267440" '
            'defaulttvdbseason="1" episodeoffset="0"/></anime-list>',
            '<anime-list><anime anidbid="100" tvdbid="267440" '
            'defaulttvdbseason="1" episodeoffset="0"/>'
            '<anime anidbid="200" tvdbid="267440" defaulttvdbseason="1" '
            'episodeoffset="16"><mapping-list><mapping anidbseason="1" '
            'tvdbseason="1">;1-2oops;</mapping></mapping-list></anime></anime-list>',
            100,
        ),
        (
            '<anime-list><anime anidbid="200" tvdbid="267440" '
            'defaulttvdbseason="1" episodeoffset="0"/></anime-list>',
            '<anime-list><anime anidbid="200" tvdbid="267440" '
            'defaulttvdbseason="1" episodeoffse="16"/></anime-list>',
            200,
        ),
    ],
    ids=["invalid-split-owner", "unknown-entry-attribute"],
)
def test_invalid_refresh_keeps_lkg_but_service_makes_zero_posts(
    tmp_path: Path, cached_xml: str, source_xml: str, anime_id: int
) -> None:
    source = td.SpecFile(
        1,
        10,
        500,
        episodes=((1, 17, 17, 777),),
        tvdb_series=267440,
        anime_id=anime_id,
    )
    td.activate_mapping(tmp_path, cached_xml)
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [source])
    shoko = td.ShokoStub([source])
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[source],
        shoko_files=[source],
        xml=source_xml,
        monotonic=td.Clock([100.0]),
        advance=lambda _: None,
        shoko=shoko,
    )
    deps = replace(deps, service_mode=True, service_authorized=lambda: True)
    try:
        report = run_cycle(
            deps,
            now_iso=td._due_iso(),
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="invalid-mapping-refresh",
        )

        active = cache_module.load_active(deps.mapping_store)
        assert report.mapping_outcome is RefreshOutcome.FAILED_INVALID
        assert "mapping_source_invalid" in report.reasons
        assert active is not None
        assert active.revision == 1
        assert report.counts.submitted == 0
        assert shoko.posts == []

        retry = run_cycle(
            deps,
            now_iso=td._due_iso(hours=168.5),
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="invalid-mapping-refresh-backoff",
        )
        assert retry.mapping_outcome is RefreshOutcome.NOT_DUE
        assert "mapping_source_invalid" in retry.reasons
        assert retry.counts.submitted == 0
        assert shoko.posts == []
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()


def test_competing_explicit_rules_are_not_a_composite() -> None:
    xml = explicit_xml(";1-1;2-1;").replace(
        ";1-1;2-1;", ';1-1;</mapping><mapping anidbseason="1" tvdbseason="1">;2-1;'
    )
    answer = resolve(build_mapping_index(parse_anime_lists(xml)), 267440, 1, 1)
    assert answer.status.value == "ambiguous"
    assert answer.targets == ()


@pytest.mark.parametrize("legacy_state", [FileState.AMBIGUOUS, FileState.WAITING])
def test_external_activation_resolves_previously_missing_mapping(
    tmp_path: Path, legacy_state: FileState
) -> None:
    source = td.SpecFile(2, 20, 600, tvdb_series=999, anime_id=1001)
    td.activate_mapping(tmp_path, td.mapping_xml())
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [source])
    shoko = td.ShokoStub([source])
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[source],
        shoko_files=[source],
        xml=td.mapping_xml(),
        monotonic=td.Clock([100.0]),
        advance=lambda _: None,
        shoko=shoko,
    )
    deps = replace(deps, service_mode=True, service_authorized=lambda: True)
    try:
        first = run_cycle(
            deps, now_iso=td.T0, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="missing"
        )
        assert not first.plans
        if legacy_state is FileState.WAITING:
            store.advance(600, FileState.WAITING, "target_unmapped", iso=td.T0)
        # A genuinely unresolved pre-upgrade row has neither a target snapshot nor
        # a reconciled generation marker; unlike a drift wait, it remains retryable.
        assert store.get_meta("file_mapping_targets:v1:600") is None
        store.clear_meta("reconciled_mapping_semantics:v1")
        td._activate_new_generation(deps, td.mapping_xml_mapped((999, 1001)), td.T1)
        store.close()
        store = td.open_tmp_store(tmp_path)
        deps = replace(deps, store=store)
        second = run_cycle(
            deps,
            now_iso=td.T2,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="resolved",
        )
        assert second.mapping_outcome is RefreshOutcome.NOT_DUE
        assert second.counts.verified == 1
        assert shoko.posts == [{"EpisodeIDs": [777]}]
    finally:
        store.close()


def test_resolved_shoko_id_change_without_mapping_refresh_is_quarantined(tmp_path: Path) -> None:
    source = td.SpecFile(1, 10, 500)
    xml = td.mapping_xml()
    td.activate_mapping(tmp_path, xml)
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [source])
    shoko = td.ShokoStub([source])
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[source],
        shoko_files=[source],
        xml=xml,
        monotonic=td.Clock([100.0]),
        advance=lambda _: None,
        shoko=shoko,
    )
    deps = replace(deps, service_mode=True, service_authorized=lambda: True, max_submissions=0)
    try:
        run_cycle(
            deps, now_iso=td.T0, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="planned"
        )
        original = store.get_meta("file_mapping_targets:v1:500")
        shoko.files[500] = replace(source, episodes=((1, 1, 5501, 778),))
        store.close()
        store = td.open_tmp_store(tmp_path)
        deps = replace(deps, store=store, max_submissions=1)
        run_cycle(
            deps, now_iso=td.T1, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="changed"
        )
        assert shoko.posts == []
        assert store.get_meta("file_mapping_targets:v1:500") == original
        assert store.get_file(500).state is FileState.CONFLICT  # type: ignore[union-attr]
    finally:
        store.close()


def test_target_snapshot_batch_rolls_back_on_invalid_persisted_state(tmp_path: Path) -> None:
    store = td.open_tmp_store(tmp_path)
    try:
        for file_id in (500, 501):
            store.record_file(
                file_id,
                sonarr_series_id=1,
                sonarr_file_id=file_id,
                sonarr_path=f"/anime/{file_id}.mkv",
                sonarr_size=1,
                cohort=Cohort.NEW_ARRIVAL,
                iso=td.T0,
            )
        store.set_meta("file_mapping_targets:v1:501", "malformed")
        with pytest.raises(StateError, match="malformed"):
            store.remember_mapping_targets_bulk(
                {500: {"source": (5501, 777)}, 501: {"source": (5502, 778)}}
            )
        assert store.get_meta("file_mapping_targets:v1:500") is None
        assert store.get_meta("file_mapping_targets:v1:501") == "malformed"
    finally:
        store.close()


def test_external_mapping_change_reports_verified_file_after_restart(tmp_path: Path) -> None:
    source = td.SpecFile(1, 10, 500)
    xml = td.mapping_xml()
    td.activate_mapping(tmp_path, xml)
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [source])
    shoko = td.MappingChangeShokoStub([source])
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[source],
        shoko_files=[source],
        xml=xml,
        monotonic=td.Clock([100.0]),
        advance=lambda _: None,
        shoko=shoko,
    )
    deps = replace(deps, service_mode=True, service_authorized=lambda: True)
    try:
        first = run_cycle(
            deps,
            now_iso=td.T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="verified",
        )
        assert first.counts.verified == 1
        associations = list(shoko.linked[500])
        changed = "<anime-list>" + td._entry(td.TVDB_SERIES, td.CHANGED_ANIME_ID) + "</anime-list>"
        td._activate_new_generation(deps, changed, td.T1)
        store.close()
        store = td.open_tmp_store(tmp_path)
        deps = replace(deps, store=store)
        second = run_cycle(
            deps, now_iso=td.T2, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="drift"
        )
        assert second.existing_link_drift
        assert store.get_service_mapping_drifts()
        assert store.get_file(500).state is FileState.VERIFIED  # type: ignore[union-attr]
        assert shoko.linked[500] == associations
        assert len(shoko.posts) == 1
    finally:
        store.close()


def test_entry_offset_201_resolves_as_episode_and_type_drift_never_relinks(
    tmp_path: Path,
) -> None:
    source = td.SpecFile(1, 10, 500, episodes=((1, 1, 201, 777),))
    offset_xml = (
        "<anime-list><anime anidbid='1234' tvdbid='267440' defaulttvdbseason='1' "
        "episodeoffset='-200'/></anime-list>"
    )
    explicit_xml = (
        "<anime-list><anime anidbid='1234' tvdbid='267440' defaulttvdbseason='1'>"
        "<mapping-list><mapping anidbseason='1' tvdbseason='1'>;201-1;</mapping>"
        "</mapping-list></anime></anime-list>"
    )

    class NumberedEpisodeShokoStub(td.ShokoStub):
        def _chain_first(self, request: httpx.Request) -> httpx.Response:
            anime_id = int(request.url.path.split("/")[5])
            episode_type = request.url.params.get("type", "Episode")
            row = {
                "ID": 201,
                "AnimeID": anime_id,
                "Type": episode_type,
                "EpisodeNumber": 201,
                "AirDate": None,
                "Title": "Episode 201",
            }
            return self._json(request, {"Total": 1, "List": [row]})

    td.activate_mapping(tmp_path, offset_xml)
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [source])
    shoko = NumberedEpisodeShokoStub([source])
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[source],
        shoko_files=[source],
        xml=offset_xml,
        monotonic=td.Clock([100.0]),
        advance=lambda _: None,
        shoko=shoko,
    )
    deps = replace(deps, service_mode=True, service_authorized=lambda: True)
    try:
        first = run_cycle(
            deps,
            now_iso=td.T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="regular-201",
        )

        assert first.counts.verified == 1
        assert first.plans[500].targets[0].episode_type == "Episode"
        assert first.plans[500].targets[0].anidb_episode_number == 201
        episode_type_requests = [
            request.url.params.get("type")
            for request in shoko.requests
            if request.url.path == "/api/v3/Series/AniDB/1234/Episode"
        ]
        assert episode_type_requests
        assert set(episode_type_requests) == {"Episode"}
        linked_before_refresh = list(shoko.linked[500])
        old_targets_digest = first.plans[500].targets_digest

        td._activate_new_generation(deps, explicit_xml, td.T1)
        store.close()
        store = td.open_tmp_store(tmp_path)
        deps = replace(deps, store=store)
        second = run_cycle(
            deps,
            now_iso=td.T2,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="explicit-trailer-drift",
        )

        assert second.existing_link_drift
        refreshed_target = resolve(
            build_mapping_index(parse_anime_lists(explicit_xml)), 267440, 1, 1
        ).target
        assert refreshed_target is not None
        assert (refreshed_target.episode_type.value, refreshed_target.anidb_episode_number) == (
            "Trailer",
            1,
        )
        assert store.get_file(500).state is FileState.VERIFIED  # type: ignore[union-attr]
        assert store.get_service_mapping_drifts()
        assert store.get_service_mapping_drifts()[0].previous_targets_digest == old_targets_digest
        assert store.get_service_mapping_drifts()[0].active_targets_digest != old_targets_digest
        assert shoko.linked[500] == linked_before_refresh
        assert len(shoko.posts) == 1
    finally:
        store.close()


def test_composite_partial_async_result_is_never_verified_or_resent(tmp_path: Path) -> None:
    xml = explicit_xml(";1-1;2-1;")
    source = td.SpecFile(1, 10, 500)
    metadata = replace(source, episodes=((1, 1, 5501, 777), (1, 2, 5502, 778)))
    td.activate_mapping(tmp_path, xml)
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [source])
    shoko = td.ShokoStub([metadata])
    shoko.link_on_post = False
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[source],
        shoko_files=[metadata],
        xml=xml,
        monotonic=td.Clock([100.0]),
        advance=lambda _: None,
        shoko=shoko,
    )
    deps = replace(deps, service_mode=True, service_authorized=lambda: True)
    try:
        first = run_cycle(
            deps,
            now_iso=td.T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="accepted",
        )
        assert first.counts.verified == 0
        shoko.linked[500] = [shoko._xref(source.anime_id, source.episodes)]
        store.close()
        store = td.open_tmp_store(tmp_path)
        deps = replace(deps, store=store)
        second = run_cycle(
            deps, now_iso=td.T1, mode=CycleMode.APPLY, policy=RunPolicy.SERVICE, cycle_key="partial"
        )
        assert second.counts.verified == 0
        assert store.get_file(500).state is FileState.PARTIAL  # type: ignore[union-attr]
        assert shoko.posts == [{"EpisodeIDs": [777, 778]}]
    finally:
        store.close()


def seed_legacy_resolution_evidence(store: StateStore, plan: LinkPlan, evidence: str) -> None:
    if evidence in ("planned", "mapping_drift", "mapping_changed"):
        if evidence != "planned":
            store.advance(500, FileState.WAITING, evidence, iso=td.T0)
        return
    fingerprint = request_fingerprint(plan)
    token = store.prepare_send(
        500,
        plan.fingerprint,
        fingerprint,
        targets_digest=plan.targets_digest,
        structure_digest=plan.structure_digest,
        mapping_revision=plan.mapping_revision,
        cycle_key="legacy-send",
        iso=td.T0,
        target_ids=((1234, 5501, 777),),
    )
    assert isinstance(token, SendToken)
    if evidence.endswith("voided_send"):
        store.void_prepared_send(500, fingerprint, iso=td.T0, reason="old preflight")
        store.advance(500, FileState.WAITING, "retry deferred", iso=td.T0)
        if evidence == "conflict_voided_send":
            store.advance(500, FileState.CONFLICT, "legacy_conflict", iso=td.T0)
        elif evidence == "ambiguous_voided_send":
            store.advance(500, FileState.AMBIGUOUS, "legacy_ambiguity", iso=td.T0)
        return
    store.mark_sent(token, iso=td.T0)
    store.record_send_outcome(token, SendOutcome.REJECTED, iso=td.T0)
    if evidence == "history_only":
        # The ledger must suffice without denormalized fields on the legacy file.
        with sqlite3.connect(store.path) as connection:
            connection.execute(
                "UPDATE files SET plan_fingerprint = NULL, request_fingerprint = NULL,"
                " mapping_targets_digest = NULL, mapping_structure_digest = NULL,"
                " mapping_revision = NULL WHERE shoko_file_id = 500"
            )


@pytest.mark.parametrize(
    "evidence",
    [
        "planned",
        "mapping_drift",
        "mapping_changed",
        "rejected_send",
        "voided_send",
        "history_only",
        "conflict_voided_send",
        "ambiguous_voided_send",
    ],
)
def test_legacy_resolved_file_cannot_silently_freeze_new_targets(
    tmp_path: Path, evidence: str
) -> None:
    source = td.SpecFile(1, 10, 500)
    xml = explicit_xml(";1-1;2-2;")
    metadata = replace(source, episodes=((1, 1, 5501, 777), (1, 2, 5502, 778)))
    td.activate_mapping(tmp_path, xml)
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [source])
    shoko = td.ShokoStub([metadata])
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[source],
        shoko_files=[metadata],
        xml=xml,
        monotonic=td.Clock([100.0]),
        advance=lambda _: None,
        shoko=shoko,
    )
    deps = replace(deps, service_mode=True, service_authorized=lambda: True, max_submissions=0)
    try:
        first = run_cycle(
            deps,
            now_iso=td.T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="old-plan",
        )
        plan = first.plans[500]
        assert plan.episode_ids == (777,)
        seed_legacy_resolution_evidence(store, plan, evidence)
        # Pre-upgrade databases have planned state but no persisted destination identities.
        store.clear_meta("file_mapping_targets:v1:500")
        store.clear_meta("reconciled_mapping_semantics:v1")
        td._activate_new_generation(deps, explicit_xml(";1-2;2-1;"), td.T1)
        store.close()
        store = td.open_tmp_store(tmp_path)
        deps = replace(deps, store=store, max_submissions=1)
        second = run_cycle(
            deps,
            now_iso=td.T2,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="upgraded",
        )
        assert second.mapping_outcome is RefreshOutcome.NOT_DUE
        assert shoko.posts == []
        assert second.counts.submitted == 0
        assert store.get_file(500).state is FileState.CONFLICT  # type: ignore[union-attr]
        assert store.mapping_target_quarantined(500)
        first_snapshot = store.get_meta("file_mapping_targets:v1:500")
        # A later semantic change must not reopen the quarantine, even when this
        # pass found an already-CONFLICT legacy row and a new process handles it.
        later = td._due_iso()
        td._activate_new_generation(deps, explicit_xml(";1-2;2-1;3-3;"), later)
        store.close()
        store = td.open_tmp_store(tmp_path)
        deps = replace(deps, store=store)
        run_cycle(
            deps,
            now_iso=later,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="still-quarantined",
        )
        assert shoko.posts == []
        assert first_snapshot is None
        assert store.get_meta("file_mapping_targets:v1:500") is None
    finally:
        store.close()


@pytest.mark.parametrize(
    "state",
    [
        FileState.DISCOVERED,
        FileState.MATCHED,
        FileState.MAPPED,
        FileState.PLANNED,
        FileState.WAITING,
        FileState.AMBIGUOUS,
        FileState.CONFLICT,
    ],
)
def test_quarantine_is_independent_of_lifecycle_state_and_detail(
    tmp_path: Path, state: FileState
) -> None:
    store = td.open_tmp_store(tmp_path)
    try:
        store.record_file(
            500,
            sonarr_series_id=1,
            sonarr_file_id=10,
            sonarr_path="/anime/500.mkv",
            sonarr_size=1,
            cohort=Cohort.NEW_ARRIVAL,
            iso=td.T0,
            state=state,
        )
        # Simulate legacy resolution evidence independently of the lifecycle state.
        with sqlite3.connect(store.path) as connection:
            connection.execute("UPDATE files SET state_detail = 'mapping_drift'")
        assert store.quarantine_unfrozen_mapping_targets(iso=td.T1) == frozenset({500})
        record = store.get_file(500)
        assert record is not None
        assert record.state is FileState.CONFLICT
        assert record.state_detail == "service_mapping_target_changed"
        original_marker = store.get_meta("file_mapping_quarantine:v1:500")
        assert original_marker is not None
        assert json.loads(original_marker)["state"] == state.value
        assert json.loads(original_marker)["detail"] == "mapping_drift"
        store.advance(500, FileState.WAITING, "unrelated retry", iso=td.T1)
        store.close()
        store = td.open_tmp_store(tmp_path)
        # Even if another path changes the presentation, persistence must reject
        # new targets BEFORE relying on a subsequent daemon reconciliation.
        assert store.remember_mapping_targets_bulk({500: {"new-target": (5502, 778)}}) == {500}
        assert store.get_meta("file_mapping_targets:v1:500") is None
        for next_state in (FileState.MATCHED, FileState.MAPPED, FileState.PLANNED):
            store.advance(500, next_state, "another lifecycle update", iso=td.T1)
        refused = store.prepare_send(
            500,
            "a" * 64,
            "b" * 64,
            targets_digest="c" * 64,
            structure_digest="d" * 64,
            mapping_revision="2",
            cycle_key="blocked",
            iso=td.T2,
        )
        assert isinstance(refused, BudgetRefusal)
        assert refused.reason == "mapping_target_quarantined"
        assert store.active_send_details(500) is None
        assert store.quarantine_unfrozen_mapping_targets(iso=td.T2) == {500}
        assert store.get_meta("file_mapping_quarantine:v1:500") == original_marker
        assert store.get_file(500).state is FileState.CONFLICT  # type: ignore[union-attr]
    finally:
        store.close()


def test_quarantine_marker_and_same_state_detail_update_roll_back_together(tmp_path: Path) -> None:
    store = td.open_tmp_store(tmp_path)
    try:
        for file_id, state in ((500, FileState.CONFLICT), (501, FileState.WAITING)):
            store.record_file(
                file_id,
                sonarr_series_id=1,
                sonarr_file_id=file_id,
                sonarr_path=f"/anime/{file_id}.mkv",
                sonarr_size=1,
                cohort=Cohort.NEW_ARRIVAL,
                iso=td.T0,
                state=state,
            )
        with sqlite3.connect(store.path) as connection:
            connection.execute("UPDATE files SET state_detail = 'mapping_drift'")
            connection.execute(
                "CREATE TRIGGER fail_quarantine BEFORE UPDATE ON files"
                " WHEN NEW.shoko_file_id = 501 AND NEW.state = 'conflict'"
                " BEGIN SELECT RAISE(ABORT, 'simulated quarantine persistence failure'); END"
            )
        with pytest.raises(WriteDisabledError, match="simulated quarantine persistence failure"):
            store.quarantine_unfrozen_mapping_targets(iso=td.T1)
        for file_id in (500, 501):
            assert not store.mapping_target_quarantined(file_id)
            assert store.get_file(file_id).state_detail == "mapping_drift"  # type: ignore[union-attr]
        assert store.get_file(500).state is FileState.CONFLICT  # type: ignore[union-attr]
        assert store.get_file(501).state is FileState.WAITING  # type: ignore[union-attr]
        store.close()
        store = StateStore(tmp_path / "state.sqlite3")
        assert store.writes_disabled
        assert not store.mapping_target_quarantined(500)
    finally:
        store.close()


@pytest.mark.parametrize("pending", [False, True])
def test_composite_links_complete_set_or_waits(tmp_path: Path, pending: bool) -> None:
    xml = explicit_xml(";1-1;2-1;")
    source = td.SpecFile(1, 10, 500)
    metadata = replace(source, episodes=((1, 1, 5501, 777), (1, 2, 5502, 778)))
    td.activate_mapping(tmp_path, xml)
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [source])
    shoko = td.ShokoStub([metadata])
    if pending:
        shoko.unmaterialized.add(5502)
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[source],
        shoko_files=[metadata],
        xml=xml,
        monotonic=td.Clock([100.0]),
        advance=lambda _: None,
        shoko=shoko,
    )
    deps = replace(deps, service_mode=True, service_authorized=lambda: True)
    try:
        report = run_cycle(
            deps,
            now_iso=td.T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="composite",
        )
        if pending:
            assert shoko.posts == []
            assert report.plans[500].episode_ids == ()
            shoko.unmaterialized.clear()
            report = run_cycle(
                deps,
                now_iso=td.T1,
                mode=CycleMode.APPLY,
                policy=RunPolicy.SERVICE,
                cycle_key="ready",
            )
        assert shoko.posts == [{"EpisodeIDs": [777, 778]}]
        assert report.plans[500].episode_ids == (777, 778)
        assert store.get_file(500).state is FileState.VERIFIED  # type: ignore[union-attr]
    finally:
        store.close()


def test_invalid_source_suppresses_advanced_new_arrival_apply_in_failure_cycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file = td.SpecFile(1, 10, 500)
    td.activate_mapping(tmp_path, td.mapping_xml())
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [])
    shoko = td.ShokoStub([file])
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=td.mapping_xml(),
        monotonic=td.Clock([100.0]),
        advance=lambda _: None,
        shoko=shoko,
        max_submissions=1,
    )
    invalid_source = td.source_endpoint(
        lambda request: httpx.Response(
            200,
            stream=httpx.ByteStream(b'{"unexpected": "JSON is not Anime-Lists XML"}'),
            request=request,
        )
    )
    deps = replace(deps, mapping_source=invalid_source)
    try:
        report = run_cycle(
            deps,
            now_iso=td._due_iso(),
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="advanced-invalid-source",
        )
        sends = store._conn.execute("SELECT COUNT(*) FROM sends").fetchone()
        budget = store.budget_view()
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()

    assert report.mapping_outcome is RefreshOutcome.FAILED_INVALID
    assert "mapping_source_invalid" in report.reasons
    assert report.counts.submitted == 0
    assert shoko.posts == []
    assert int(sends[0]) == 0
    assert budget.rolling_new_submissions == 0


@pytest.mark.parametrize("service_mode", [False, True], ids=["advanced", "service"])
def test_invalid_source_hold_survives_transport_failure_restart_and_not_due(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, service_mode: bool
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file = td.SpecFile(1, 10, 500)
    td.activate_mapping(tmp_path, td.mapping_xml())
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [])
    shoko = td.ShokoStub([file])
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=td.mapping_xml(),
        monotonic=td.Clock([100.0]),
        advance=lambda _: None,
        shoko=shoko,
        max_submissions=1,
    )
    deps = replace(deps, service_mode=service_mode, service_authorized=lambda: True)
    invalid_source = td.source_endpoint(
        lambda request: httpx.Response(
            200,
            stream=httpx.ByteStream(b'{"unexpected": "invalid source document"}'),
            request=request,
        )
    )
    deps = replace(deps, mapping_source=invalid_source)
    invalid_at = td._due_iso()
    policy = RunPolicy.SERVICE if service_mode else RunPolicy.NEW_ARRIVAL
    try:
        invalid = run_cycle(
            deps,
            now_iso=invalid_at,
            mode=CycleMode.APPLY,
            policy=policy,
            cycle_key="service-invalid-source",
        )
        assert invalid.mapping_outcome is RefreshOutcome.FAILED_INVALID
        assert shoko.posts == []

        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()
        store = td.open_tmp_store(tmp_path)

        transport_source = td.source_endpoint(lambda request: httpx.Response(503, request=request))
        deps = td.build_deps(
            tmp_path,
            store=store,
            sonarr_files=[file],
            shoko_files=[file],
            xml=td.mapping_xml(),
            monotonic=td.Clock([100.0]),
            advance=lambda _: None,
            shoko=shoko,
            max_submissions=1,
        )
        deps = replace(
            deps,
            mapping_source=transport_source,
            service_mode=service_mode,
            service_authorized=lambda: True,
        )
        failed_transport = run_cycle(
            deps,
            now_iso=td._due_iso(invalid_at, hours=1.0),
            mode=CycleMode.APPLY,
            policy=policy,
            cycle_key="service-invalid-then-transport",
        )
        not_due = run_cycle(
            deps,
            now_iso=td._due_iso(invalid_at, hours=1.5),
            mode=CycleMode.APPLY,
            policy=policy,
            cycle_key="service-invalid-then-not-due",
        )
        sends = store._conn.execute("SELECT COUNT(*) FROM sends").fetchone()
        budget = store.budget_view()
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()

    assert failed_transport.mapping_outcome is RefreshOutcome.FAILED_TRANSPORT
    assert not_due.mapping_outcome is RefreshOutcome.NOT_DUE
    assert "mapping_source_invalid" in not_due.reasons
    assert not_due.counts.submitted == 0
    assert shoko.posts == []
    assert int(sends[0]) == 0
    assert budget.rolling_new_submissions == 0


def test_invalid_source_refresh_during_final_shoko_get_blocks_send_intent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file = td.SpecFile(1, 10, 500)
    td.activate_mapping(tmp_path, td.mapping_xml())
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [])
    shoko = td.ShokoStub([file])
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=td.mapping_xml(),
        monotonic=td.Clock([100.0]),
        advance=lambda _: None,
        shoko=shoko,
        max_submissions=1,
    )
    cli_mapping_store = cache_module.MappingStore(
        deps.mapping_store.directory, source_url=td.SOURCE_URL
    )
    invalid_source = td.source_endpoint(
        lambda request: httpx.Response(
            200,
            stream=httpx.ByteStream(b'{"invalid": "refresh from another CLI process"}'),
            request=request,
        )
    )
    refreshed = {"value": False}

    def invalidate_during_final_read(path: str) -> None:
        if path != "/api/v3/File/500" or refreshed["value"]:
            return
        refreshed["value"] = True
        assert (
            cache_module.refresh(
                cli_mapping_store,
                invalid_source,
                now=td._parse(td._due_iso()),
                refresh_hours=deps.settings.mappings.refresh_hours,
                force=True,
            )
            is RefreshOutcome.FAILED_INVALID
        )

    shoko.on_get = invalidate_during_final_read
    try:
        report = run_cycle(
            deps,
            now_iso=td.T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.NEW_ARRIVAL,
            cycle_key="mapping-invalidated-during-preflight",
        )
        sends = store._conn.execute("SELECT COUNT(*) FROM sends").fetchone()
        budget = store.budget_view()
        record = store.get_file(500)
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source, invalid_source):
            endpoint.client.close()
        store.close()

    assert refreshed["value"]
    assert report.counts.submitted == 0
    assert "mapping_source_invalid" in report.reasons
    assert shoko.posts == []
    assert int(sends[0]) == 0
    assert budget.pending_file_requests == 0
    assert budget.rolling_new_submissions == 0
    assert record is not None
    assert record.state is FileState.PLANNED


@pytest.mark.parametrize("operation", ["service", "advanced", "campaign"])
def test_invalid_refresh_cannot_commit_between_final_guard_and_send(  # noqa: PLR0915
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, operation: str
) -> None:
    """The generation write lease orders a concurrent invalid refresh after the POST."""
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file = td.SpecFile(1, 10, 500)
    td.activate_mapping(tmp_path, td.mapping_xml())
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [file] if operation == "campaign" else [])
    shoko = td.ShokoStub([file])
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=td.mapping_xml(),
        monotonic=td.Clock([100.0]),
        advance=lambda _: None,
        shoko=shoko,
        max_submissions=1,
    )
    cli_mapping_store = cache_module.MappingStore(
        deps.mapping_store.directory, source_url=td.SOURCE_URL
    )
    invalid_at = td._due_iso()
    failure_entered = Event()
    release_failure = Event()
    hold_persisted = Event()
    refresh_finished = Event()
    refresh_outcomes: list[RefreshOutcome] = []
    refresh_errors: list[BaseException] = []
    invalid_source = td.source_endpoint(
        lambda request: httpx.Response(
            200,
            stream=httpx.ByteStream(b'{"invalid": "concurrent refresh"}'),
            request=request,
        )
    )
    original_record_failure = cache_module._record_failure
    original_record_failure_locked = cache_module._record_failure_locked

    def paused_failure(*args: object, **kwargs: object) -> RefreshOutcome:
        failure_entered.set()
        if not release_failure.wait(5.0):
            raise AssertionError("test did not release the invalid refresh")
        return original_record_failure(*args, **kwargs)  # type: ignore[arg-type]

    def observe_invalid_commit(*args: object, **kwargs: object) -> RefreshOutcome:
        result = original_record_failure_locked(*args, **kwargs)  # type: ignore[arg-type]
        if args[3] is RefreshOutcome.FAILED_INVALID:
            hold_persisted.set()
        return result

    monkeypatch.setattr(cache_module, "GENERATION_LOCK_REFRESH_WAIT_SECONDS", 5.0)
    monkeypatch.setattr(cache_module, "_record_failure", paused_failure)
    monkeypatch.setattr(cache_module, "_record_failure_locked", observe_invalid_commit)

    def refresh_from_other_store() -> None:
        try:
            refresh_outcomes.append(
                cache_module.refresh(
                    cli_mapping_store,
                    invalid_source,
                    now=td._parse(invalid_at),
                    refresh_hours=deps.settings.mappings.refresh_hours,
                    force=True,
                )
            )
        except BaseException as exc:  # propagate a worker failure into the test thread
            refresh_errors.append(exc)
        finally:
            refresh_finished.set()

    refresh_thread = Thread(target=refresh_from_other_store, daemon=True)
    refresh_started = False

    def begin_refresh_during_final_read(path: str) -> None:
        nonlocal refresh_started
        if path != "/api/v3/File/500" or refresh_started:
            return
        refresh_started = True
        refresh_thread.start()
        assert failure_entered.wait(5.0)

    def check_post_before_hold_commit() -> None:
        release_failure.set()
        assert not hold_persisted.wait(0.5), "invalid-source hold committed before the POST"
        # Durable evidence is visible immediately, but the generation lease keeps the
        # already-authorized POST intact until its protected section completes.
        assert cli_mapping_store.invalid_source_hold()

    plan_path = tmp_path / "concurrent-refresh-campaign.json"
    try:
        if operation == "campaign":
            plan, digest = prepare_campaign(
                deps, deps.settings, store, td.T0, limit=1, deadline_hours=168.0
            )
            write_plan_file(plan_path, plan)
            approve_campaign(store, plan_path, digest, td.T0)
            shoko.on_get = begin_refresh_during_final_read
            shoko.on_post = check_post_before_hold_commit
            campaign_report = apply_campaign(
                deps, deps.settings, store, plan_path, td.T0, batch_size=1
            )
            submitted = campaign_report.submitted
        else:
            service_mode = operation == "service"
            deps = replace(
                deps,
                service_mode=service_mode,
                service_authorized=lambda: True,
                service_authorized_local=lambda: True,
            )
            policy = RunPolicy.SERVICE if service_mode else RunPolicy.NEW_ARRIVAL
            shoko.on_get = begin_refresh_during_final_read
            shoko.on_post = check_post_before_hold_commit
            cycle_report = run_cycle(
                deps,
                now_iso=td.T0,
                mode=CycleMode.APPLY,
                policy=policy,
                cycle_key=f"concurrent-refresh-{operation}",
            )
            submitted = cycle_report.counts.submitted
        assert refresh_started
        assert submitted == 1
        assert shoko.posts == [{"EpisodeIDs": [td.SHOKO_EP]}]
    finally:
        release_failure.set()
        if refresh_started:
            refresh_thread.join(timeout=8.0)
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source, invalid_source):
            endpoint.client.close()
        store.close()

    assert not refresh_thread.is_alive()
    assert refresh_errors == []
    assert refresh_finished.is_set()
    assert refresh_outcomes == [RefreshOutcome.FAILED_INVALID]
    assert hold_persisted.is_set()
    assert cache_module.MappingStore(tmp_path / "mappings").invalid_source_hold()


def test_service_post_keeps_lease_proof_while_invalid_refresh_latches(  # noqa: PLR0915
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    files = [td.SpecFile(1, 10, 500), td.SpecFile(2, 11, 501)]
    td.activate_mapping(tmp_path, td.mapping_xml())
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [])
    shoko = td.ShokoStub(files)
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=files,
        shoko_files=files,
        xml=td.mapping_xml(),
        monotonic=td.Clock([100.0]),
        advance=lambda _: None,
        shoko=shoko,
        max_submissions=1,
    )
    deps = replace(deps, service_mode=True, service_authorized=lambda: True)
    refresh_store = cache_module.MappingStore(
        deps.mapping_store.directory, source_url=td.SOURCE_URL
    )
    response_requested = Event()
    release_response = Event()
    refresh_finished = Event()
    refresh_outcomes: list[RefreshOutcome] = []
    refresh_errors: list[BaseException] = []
    invalid_source = td.source_endpoint(
        lambda request: _delayed_invalid_mapping_response(
            request, response_requested, release_response
        )
    )

    def refresh_invalid_source() -> None:
        try:
            refresh_outcomes.append(
                cache_module.refresh(
                    refresh_store,
                    invalid_source,
                    now=td._parse(td.T0),
                    refresh_hours=deps.settings.mappings.refresh_hours,
                    force=True,
                )
            )
        except BaseException as exc:
            refresh_errors.append(exc)
        finally:
            refresh_finished.set()

    refresh_thread = Thread(target=refresh_invalid_source, daemon=True)
    refresh_started = False
    post_observed_hold: list[bool] = []

    def begin_invalid_refresh_during_final_read(path: str) -> None:
        nonlocal refresh_started
        if path != "/api/v3/File/500" or refresh_started:
            return
        refresh_started = True
        refresh_thread.start()
        assert response_requested.wait(5.0)

    def finish_authorized_post() -> None:
        release_response.set()
        refresh_thread.join(timeout=5.0)
        assert not refresh_thread.is_alive()
        post_observed_hold.append(refresh_store.invalid_source_hold())

    shoko.on_get = begin_invalid_refresh_during_final_read
    shoko.on_post = finish_authorized_post
    try:
        report = run_cycle(
            deps,
            now_iso=td.T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="invalid-refresh-during-protected-post",
        )
        first_record = store.get_file(500)
        pending_record = store.get_file(501)
        assert refresh_finished.is_set()
        assert refresh_errors == []
        assert refresh_outcomes == [RefreshOutcome.FAILED_LOCKED]
        assert post_observed_hold == [True]
        assert report.counts.submitted == 1
        assert report.counts.verified == 1
        assert "mapping_source_invalid" not in report.reasons
        assert shoko.posts == [{"EpisodeIDs": [td.SHOKO_EP]}]
        assert first_record is not None
        assert first_record.state is FileState.VERIFIED
        assert pending_record is not None
        assert pending_record.state is FileState.PLANNED

        held = run_cycle(
            deps,
            now_iso=td.T1,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="invalid-source-fences-next-post",
        )
        assert held.counts.submitted == 0
        assert "mapping_source_invalid" in held.reasons
        assert shoko.posts == [{"EpisodeIDs": [td.SHOKO_EP]}]
        assert refresh_store.invalid_source_hold()
    finally:
        release_response.set()
        if refresh_started:
            refresh_thread.join(timeout=5.0)
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source, invalid_source):
            endpoint.client.close()
        store.close()


def _delayed_invalid_mapping_response(
    request: httpx.Request, requested: Event, release: Event
) -> httpx.Response:
    requested.set()
    assert release.wait(5.0)
    return httpx.Response(
        200,
        stream=httpx.ByteStream(b'{"invalid": "not Anime-Lists XML"}'),
        request=request,
    )


def test_validated_source_clears_hold_but_changed_frozen_targets_stay_quarantined(
    tmp_path: Path,
) -> None:
    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    changed = td.SpecFile(1, 10, 500, tvdb_series=1, anime_id=1001)
    safe_new = td.SpecFile(2, 11, 501)
    old_xml = td.mapping_xml_mapped((1, 1001))
    new_xml = td.mapping_xml_mapped((1, 2001))
    td.activate_mapping(tmp_path, old_xml)
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [changed])
    shoko = td.ShokoStub([changed, safe_new])
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[changed, safe_new],
        shoko_files=[changed, safe_new],
        xml=old_xml,
        monotonic=td.Clock([100.0]),
        advance=lambda _: None,
        shoko=shoko,
        max_submissions=0,
    )
    deps = replace(deps, service_mode=True, service_authorized=lambda: True)
    try:
        initial = run_cycle(
            deps,
            now_iso=td.T0,
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="freeze-old-targets",
        )
        assert initial.counts.submitted == 0
        assert store.get_file(changed.shoko_id).state is FileState.PLANNED  # type: ignore[union-attr]

        assert (
            _refresh_mapping_source(deps, b'{"invalid": true}', td.T1)
            is RefreshOutcome.FAILED_INVALID
        )
        assert cache_module.status(deps.mapping_store, td._parse(td.T1)).invalid_source_hold
        assert _refresh_mapping_source(deps, new_xml.encode(), td.T2) is RefreshOutcome.ACTIVATED
        assert not cache_module.status(deps.mapping_store, td._parse(td.T2)).invalid_source_hold

        deps = replace(deps, max_submissions=1)
        resumed = run_cycle(
            deps,
            now_iso=td._due_iso(td.T2, hours=1),
            mode=CycleMode.APPLY,
            policy=RunPolicy.SERVICE,
            cycle_key="validated-source-resumed",
        )
        changed_record = store.get_file(changed.shoko_id)
        assert resumed.counts.submitted == 1
        assert shoko.posts == [{"EpisodeIDs": [td.SHOKO_EP]}]
        assert changed_record is not None
        assert changed_record.state is FileState.CONFLICT
        assert store.mapping_target_quarantined(changed.shoko_id)
        assert store.get_file(safe_new.shoko_id).state is FileState.VERIFIED  # type: ignore[union-attr]
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()
        monkeypatch.undo()


def test_approved_campaign_is_held_without_post_or_reservation_on_invalid_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    file = td.SpecFile(1, 10, 500)
    td.activate_mapping(tmp_path, td.mapping_xml())
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, [file])
    shoko = td.ShokoStub([file])
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=[file],
        shoko_files=[file],
        xml=td.mapping_xml(),
        monotonic=td.Clock([100.0]),
        advance=lambda _: None,
        shoko=shoko,
        max_submissions=1,
    )
    plan_path = tmp_path / "approved-campaign.json"
    try:
        plan, digest = prepare_campaign(
            deps, deps.settings, store, td.T0, limit=1, deadline_hours=168.0
        )
        write_plan_file(plan_path, plan)
        approve_campaign(store, plan_path, digest, td.T0)
        assert (
            _refresh_mapping_source(deps, b'{"invalid": true}', td.T1)
            is RefreshOutcome.FAILED_INVALID
        )
        before = store.budget_view()

        with pytest.raises(CampaignEligibilityRefusalError, match="mapping_source_invalid"):
            apply_campaign(deps, deps.settings, store, plan_path, td.T2, batch_size=1)

        after = store.budget_view()
        sends = store._conn.execute("SELECT COUNT(*) FROM sends").fetchone()
        campaign = store.get_campaign(plan.campaign_id)
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()

    assert shoko.posts == []
    assert int(sends[0]) == 0
    assert after.pending_file_requests == before.pending_file_requests == 0
    assert after.rolling_new_submissions == before.rolling_new_submissions == 0
    assert campaign is not None
    assert campaign.status == "approved"


def test_daemon_campaign_drain_is_held_before_send_intent_on_invalid_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SHOKO_LINKER_ALLOW_APPLY", "1")
    files = [td.SpecFile(1, 10, 500), td.SpecFile(2, 11, 501)]
    td.activate_mapping(tmp_path, td.mapping_xml())
    store = td.open_tmp_store(tmp_path)
    td.seed_baseline(store, files)
    shoko = td.ShokoStub(files)
    settings = td._drain_settings(tmp_path, batch=2)
    deps = td.build_deps(
        tmp_path,
        store=store,
        sonarr_files=files,
        shoko_files=files,
        xml=td.mapping_xml(),
        monotonic=td.Clock([100.0]),
        advance=lambda _: None,
        settings=settings,
        max_submissions=1,
        shoko=shoko,
    )
    plan_path = tmp_path / "drain-held-campaign.json"
    try:
        plan, digest = prepare_campaign(deps, settings, store, td.T0, limit=2, deadline_hours=168.0)
        write_plan_file(plan_path, plan)
        approve_campaign(store, plan_path, digest, td.T0)
        canary = apply_campaign(deps, settings, store, plan_path, td.T0, batch_size=1)
        assert canary.submitted == 1
        authorize_campaign_drain(store, plan.campaign_id, td.T0, note="canary inspected")

        assert (
            _refresh_mapping_source(deps, b'{"invalid": true}', td.T1)
            is RefreshOutcome.FAILED_INVALID
        )
        before = store.budget_view()
        sends_before = store._conn.execute("SELECT COUNT(*) FROM sends").fetchone()
        posts_before = len(shoko.posts)

        report = run_cycle(
            deps,
            now_iso=td.T1,
            mode=CycleMode.APPLY,
            policy=RunPolicy.BACKLOG,
            cycle_key="daemon-drain-invalid-source",
        )

        after = store.budget_view()
        sends_after = store._conn.execute("SELECT COUNT(*) FROM sends").fetchone()
        campaign = store.get_campaign(plan.campaign_id)
    finally:
        for endpoint in (deps.sonarr, deps.shoko, deps.mapping_source):
            endpoint.client.close()
        store.close()

    assert report.campaign_drain_selected
    assert not report.campaign_apply_eligible
    assert report.campaign_submitted == 0
    assert "mapping_source_invalid" in report.reasons
    assert len(shoko.posts) == posts_before
    assert int(sends_after[0]) == int(sends_before[0])
    assert after.pending_file_requests == before.pending_file_requests
    assert after.pending_episode_jobs == before.pending_episode_jobs
    assert after.rolling_new_submissions == before.rolling_new_submissions
    assert campaign is not None
    assert campaign.status == "approved"
    assert campaign.drain_authorized_iso == td.T0
    assert campaign.drain_authorized_digest == campaign.digest
