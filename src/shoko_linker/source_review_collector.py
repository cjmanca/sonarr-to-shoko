"""Fresh, review-owned source collection for explicit continuity recovery.

No daemon/setup inventory cache, remote mutation, or write authorization is used here.
Only the worker calls this adapter; HTTP status/control handlers use its controller.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from http import HTTPStatus
from pathlib import Path
from typing import Any

from .bootstrap import BootstrapSettings
from .config import Settings
from .daemon import inventory_snapshot_hash, roots_fingerprint
from .http import ApiEndpoint, get_bounded_json
from .linking import parse_file_xrefs
from .mapping_store import DEFAULT_MAX_BYTES, MappingStore
from .mappings import (
    build_mapping_index,
    legacy_targets_digest,
    load_overrides,
    parse_anime_lists,
    structure_digest,
    targets_digest,
)
from .onboarding import OnboardingContext, OnboardingManager, OnboardingStatus
from .paths import normalize_posix
from .service import settings_for_scope
from .service_settings import apply_service_settings, load_service_settings
from .service_setup import ServiceSetupController
from .shoko import (
    ImportFolder,
    ShokoDiscovery,
    ShokoDiscoveryProgress,
    SnapshotReason,
    collect_import_folders,
    collect_shoko_discovery,
)
from .sonarr import (
    SonarrInventory,
    collect_sonarr_inventory,
    parse_file_entries,
    parse_series_entries,
    select_eligible_series,
)
from .source_review import SourceReviewEvidence, SourceReviewSummary, SourceReviewWork
from .state import (
    SourceObservation,
    SourceRebaselineAssessment,
    SourceRebaselineBindings,
    StateError,
    StateStore,
)


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


@dataclass
class _Progress:
    context_digest: str
    series: tuple[tuple[object, ...], ...]
    manifest: str | None = None
    children: dict[int, tuple[tuple[Any, ...], tuple[Any, ...]]] = field(default_factory=dict)
    cursor: int = 0
    swept: frozenset[int] = frozenset()
    inventory: SonarrInventory | None = None
    shoko: ShokoDiscoveryProgress | None = None
    discovery: ShokoDiscovery | None = None


class FreshSourceReviewCollector:
    """A bounded sweep with private progress, discarded on any identity drift."""

    def __init__(
        self,
        bootstrap: BootstrapSettings,
        sonarr: ApiEndpoint,
        shoko: ApiEndpoint,
        setup: ServiceSetupController,
        *,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        monotonic: Callable[[], float] = time.monotonic,
        shutdown_requested: Callable[[], bool] = lambda: False,
    ) -> None:
        self.bootstrap = bootstrap
        self.sonarr = sonarr
        self.shoko = shoko
        self.setup = setup
        self.now = now
        self.clock = monotonic
        self.shutdown_requested = shutdown_requested

    def _check(self, deadline: float, work: SourceReviewWork | None = None) -> None:
        if self.clock() >= deadline:
            raise TimeoutError("source review deadline exceeded")
        if self.shutdown_requested() or (work is not None and work.shutdown_requested()):
            raise StateError("source review stopped")

    def _settings(self, store: StateStore, deadline: float) -> Settings:
        self._check(deadline)
        document = load_service_settings(store, self.bootstrap)
        if (
            document["sonarr_url"] != self.bootstrap.sonarr_url
            or document["shoko_url"] != self.bootstrap.shoko_url
            or str(self.sonarr.base_url).rstrip("/") != self.bootstrap.sonarr_url.rstrip("/")
            or str(self.shoko.base_url).rstrip("/") != self.bootstrap.shoko_url.rstrip("/")
        ):
            raise StateError("source review requires current runtime endpoints")
        manager = OnboardingManager(store)
        record = manager.record()
        if record is None or not record.automatic_link_consent or manager.reconfirmation_required():
            raise StateError("source review requires confirmed scope")
        return apply_service_settings(
            settings_for_scope(
                self.bootstrap, roots=record.binding.roots, mappings=record.binding.path_mappings
            ),
            document,
        )

    def _read(self, path: Path, deadline: float) -> bytes:
        chunks: list[bytes] = []
        total = 0
        with path.open("rb") as stream:
            while True:
                self._check(deadline)
                chunk = stream.read(64 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > DEFAULT_MAX_BYTES:
                    raise StateError("source review mapping input exceeds limit")
                chunks.append(chunk)
        self._check(deadline)
        return b"".join(chunks)

    def _mapping_identity(self, settings: Settings, deadline: float) -> str:
        mapping = MappingStore.from_settings(settings)
        # Never wait indefinitely behind activation, and never refresh or repair a cache.
        with mapping.generation_lock(wait_seconds=0) as acquired:
            if not acquired or mapping.invalid_source_hold():
                raise StateError("current source review mappings unavailable")
            self._check(deadline)
            # Bound the XML before the existing identity verifier reads it. Activated
            # generations are immutable under this same lock.
            body = self._read(mapping.active_path, deadline)
            identity = mapping.active_identity_locked()
            if identity is None:
                raise StateError("current source review mappings unavailable")
            self._check(deadline)
            override_path = settings.mappings.overrides_file
            overrides_body = (
                None if override_path is None else self._read(Path(override_path), deadline)
            )
            overrides = (
                None if overrides_body is None else load_overrides(overrides_body.decode("utf-8"))
            )
            parsed = parse_anime_lists(body)
            index = build_mapping_index(parsed, overrides)
            self._check(deadline)
            if (
                not parsed.activation_eligible
                or hashlib.sha256(body).hexdigest() != identity[1]
                or identity[2] not in (targets_digest(index), legacy_targets_digest(index))
                or structure_digest(index) != identity[3]
            ):
                raise StateError("current source review mappings invalid")
            return _digest(
                (
                    identity,
                    None if overrides_body is None else hashlib.sha256(overrides_body).hexdigest(),
                )
            )

    def capture_bindings(self, store: StateStore, deadline: float) -> SourceRebaselineBindings:
        settings = self._settings(store, deadline)
        mapping = self._mapping_identity(settings, deadline)
        self._check(deadline)
        return SourceRebaselineBindings(
            _digest(asdict(settings)), mapping, store.get_source_rebaseline_local_binding()
        )

    def _context(
        self, store: StateStore, deadline: float, work: SourceReviewWork
    ) -> OnboardingContext:
        self._check(deadline, work)
        context = self.setup.current_context(
            store, deadline=deadline, clock=lambda: self._work_clock(work)
        )
        self._check(deadline, work)
        if (
            context is None
            or OnboardingManager(store)
            .evaluate_source_review_context(context, iso=self.now().isoformat())
            .status
            is not OnboardingStatus.AUTHORIZED
        ):
            raise StateError("source review current consent unavailable")
        return context

    def _series(
        self, settings: Settings, deadline: float, work: SourceReviewWork
    ) -> tuple[tuple[object, ...], ...]:
        self._check(deadline, work)
        status, raw = get_bounded_json(
            self.sonarr, "api/v3/series", deadline=deadline, clock=lambda: self._work_clock(work)
        )
        self._check(deadline, work)
        if not HTTPStatus.OK <= status < HTTPStatus.MULTIPLE_CHOICES or not isinstance(raw, list):
            raise StateError("source review series unavailable")
        parsed = parse_series_entries(raw)
        if parsed.reasons:
            raise StateError("source review series invalid")
        selected, _ = select_eligible_series(parsed.series, settings.sonarr.allowed_roots)
        selected_ids = {item.series.id for item in selected}
        result: list[tuple[object, ...]] = []
        for entry in raw:
            self._check(deadline, work)
            if entry["id"] in selected_ids:
                result.append(
                    (
                        entry["id"],
                        entry.get("seriesType"),
                        entry.get("tvdbId"),
                        normalize_posix(entry["path"]),
                        normalize_posix(entry["rootFolderPath"]),
                    )
                )
        return tuple(sorted(result))

    def collect(
        self, store: StateStore, work: SourceReviewWork, deadline: float
    ) -> SourceReviewEvidence | None:
        self._check(deadline, work)
        settings = self._settings(store, deadline)
        context = self._context(store, deadline, work)
        context_digest = context.binding.digest
        if work.progress is None:
            work.progress = _Progress(context_digest, self._series(settings, deadline, work))
        if not isinstance(work.progress, _Progress):
            raise StateError("invalid source review progress")
        progress = work.progress
        if progress.context_digest != context_digest:
            raise StateError("source review context changed")
        if progress.inventory is None:
            inventory = collect_sonarr_inventory(
                self.sonarr,
                settings,
                cached_manifest_hash=progress.manifest,
                cached_series_children=progress.children,
                series_cursor=progress.cursor,
                swept_series_ids=progress.swept,
                deadline=deadline,
                monotonic=lambda: self._work_clock(work),
            )
            if (
                progress.manifest is not None
                and inventory.series_manifest_hash != progress.manifest
            ):
                raise StateError("source review manifest changed")
            progress.manifest = inventory.series_manifest_hash
            progress.children.update(inventory.fetched_series_children or {})
            progress.cursor = inventory.series_cursor
            progress.swept = inventory.swept_series_ids
            if not inventory.verdict.complete or not inventory.refresh_sweep_complete:
                return None
            progress.inventory = inventory
        self._check(deadline, work)
        if progress.discovery is None:
            discovery = collect_shoko_discovery(
                self.shoko,
                cached_progress=progress.shoko,
                endpoint_identity=str(self.shoko.base_url).rstrip("/"),
                deadline=deadline,
                monotonic=lambda: self._work_clock(work),
                observed_at=self.now().isoformat(),
            )
            if {SnapshotReason.TOTAL_CHANGED, SnapshotReason.PAGE_CHANGED} & set(
                discovery.verdict.reasons
            ) or any(
                "restarting sweep" in item or "invalidated" in item
                for item in discovery.verdict.details
            ):
                raise StateError("source review Shoko inventory changed")
            progress.shoko = discovery.progress
            if not discovery.verdict.complete or not discovery.refresh_sweep_complete:
                return None
            if discovery.total_reported is None:
                raise StateError("source review Shoko total unavailable")
            progress.discovery = discovery
        self._check(deadline, work)
        # Re-read current environment and series placement after all inventory I/O.
        final = self._context(store, deadline, work)
        if (
            final.binding.digest != progress.context_digest
            or self._series(settings, deadline, work) != progress.series
        ):
            raise StateError("source review environment changed")
        self._check_folders(progress.discovery, deadline, work)
        if self.capture_bindings(store, deadline) != work.bindings:
            raise StateError("source review bindings changed")
        inventory, discovery = progress.inventory, progress.discovery
        content = self._content(progress, work, deadline)
        self._check(deadline, work)
        completed = self.now().isoformat()
        observation = SourceObservation(
            f"sonarr={settings.sonarr.url};shoko={settings.shoko.url}",
            roots_fingerprint(settings),
            inventory_snapshot_hash(inventory, discovery),
            discovery.total_reported,
        )
        return SourceReviewEvidence(
            SourceRebaselineAssessment(observation, content, work.started_iso, completed),
            frozenset(item.id for item in discovery.files),
            SourceReviewSummary(
                len(settings.sonarr.allowed_roots),
                len(inventory.eligible_series),
                len(inventory.files),
                len(discovery.files),
                len(discovery.folders),
            ),
            True,
            True,
            True,
            True,
        )

    def _check_folders(
        self, discovery: ShokoDiscovery, deadline: float, work: SourceReviewWork
    ) -> None:
        folders = collect_import_folders(
            self.shoko, deadline=deadline, monotonic=lambda: self._work_clock(work)
        )
        self._check(deadline, work)
        if folders is None or self._folder_identity(folders) != self._folder_identity(
            discovery.folders.values()
        ):
            raise StateError("source review folders changed")

    def _work_clock(self, work: SourceReviewWork) -> float:
        if work.shutdown_requested():
            raise StateError("source review stopped")
        return self.clock()

    @staticmethod
    def _folder_identity(folders: Iterable[ImportFolder]) -> list[tuple[int, str, str, bool]]:
        return sorted(
            (folder.id, normalize_posix(folder.path), folder.drop_folder_type, folder.path_usable)
            for folder in folders
        )

    def _content(self, progress: _Progress, work: SourceReviewWork, deadline: float) -> str:
        inventory, discovery = progress.inventory, progress.discovery
        assert inventory is not None
        assert discovery is not None
        if discovery.progress is None:
            raise StateError("source review association proof unavailable")
        refs: dict[int, list[Any]] = {}
        for rows in discovery.progress.pages.values():
            for raw in rows:
                self._check(deadline, work)
                parsed = parse_file_xrefs(raw)
                if parsed is None or any(
                    episode.shoko_episode_id is None
                    for xref in parsed.xrefs
                    for episode in xref.episodes
                ):
                    raise StateError("source review association proof invalid")
                # The pinned File DTO populates SeriesIDs from EpisodeCrossReferences.
                # Preserve all association data after validating the protected IDs with
                # the existing parser; sort both relationship levels canonically.
                refs[parsed.file_id] = sorted(
                    (
                        dict(xref) | {"EpisodeIDs": sorted(xref["EpisodeIDs"], key=_digest)}
                        for xref in raw["SeriesIDs"]
                    ),
                    key=_digest,
                )
        if set(refs) != {item.id for item in discovery.files}:
            raise StateError("source review association proof incomplete")
        records: list[tuple[int, int, str, int]] = []
        for series_id, (_, raw_files) in sorted(progress.children.items()):
            self._check(deadline, work)
            parsed_files = parse_file_entries(raw_files, series_id)
            if parsed_files.reasons or parsed_files.rejected:
                raise StateError("source review file identity incomplete")
            records.extend(
                (item.id, item.series_id, normalize_posix(item.path), item.size)
                for item in parsed_files.records
            )
        return _digest(
            {
                "file_records": sorted(records),
                "bindings": asdict(work.bindings),
                "context": progress.context_digest,
                "series": progress.series,
                "episodes": [
                    (
                        ep.id,
                        ep.series_id,
                        ep.season_number,
                        ep.episode_number,
                        ep.absolute_episode_number,
                        ep.tvdb_id,
                        ep.episode_file_id,
                    )
                    for ep in inventory.episodes
                ],
                "files": [
                    (
                        item.series_id,
                        item.episode_file_id,
                        item.path,
                        item.size,
                        sorted(ep.id for ep in item.episodes),
                    )
                    for item in inventory.files
                ],
                "rejected": [
                    asdict(item) | {"reason": item.reason.value}
                    for item in inventory.rejected_files
                ],
                "unlinked": inventory.unlinked_file_ids,
                "shoko": [
                    {
                        "id": item.id,
                        "size": item.size,
                        "ignored": item.is_ignored,
                        "variation": item.is_variation,
                        "hashes": asdict(item.hashes),
                        "locations": sorted(
                            (
                                loc.place_id,
                                loc.file_id,
                                loc.import_folder_id,
                                normalize_posix("/" + loc.relative_path.lstrip("/")),
                                normalize_posix(loc.absolute_path),
                                loc.is_accessible,
                            )
                            for loc in item.locations
                        ),
                        "associations": refs[item.id],
                    }
                    for item in sorted(discovery.files, key=lambda item: item.id)
                ],
                "folders": sorted(
                    (
                        folder.id,
                        normalize_posix(folder.path),
                        folder.drop_folder_type,
                        folder.path_usable,
                    )
                    for folder in discovery.folders.values()
                ),
            }
        )
