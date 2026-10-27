"""Durable scope-bound onboarding consent for the normal automatic service."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Final, NamedTuple

from .config import validate_base_url
from .errors import ConfigError, PathError
from .paths import PathMapping, is_within_root, normalize_posix, validate_mappings
from .safety_check import SAFETY_SETTING_PATHS, SafetyAssessment, SafetyStatus
from .state import (
    META_ONBOARDING_RECONFIRMATION_REQUIRED,
    SYSTEM_PAUSE_REASONS,
    SYSTEM_RECONFIRMATION_PAUSE_REASONS,
    StateStore,
)

ONBOARDING_DOCUMENT_SCHEMA: Final[str] = "shoko-sonarr-linker/onboarding/1"
_SELECTED_FOLDER_FACT_COUNT: Final[int] = 3
_HEX64_RE: Final[re.Pattern[str]] = re.compile(r"^[0-9a-f]{64}$")


class OnboardingError(ValueError):
    """Onboarding input or persisted consent is invalid; automatic writes stay closed."""


class OnboardingStatus(Enum):
    """Normal-service authorization outcomes, separate from legacy leases/campaigns."""

    NOT_CONFIGURED = "not_configured"
    NEEDS_CONFIRMATION = "needs_confirmation"
    AUTHORIZED = "authorized"
    PAUSED = "paused"
    UNSAFE = "unsafe"


def _validated_folder_observation(raw: Any) -> tuple[tuple[int, str, str], ...]:
    """Validate only selected, approved folder IDs/normalized paths/exclusion types."""
    if not isinstance(raw, list | tuple) or not raw:
        raise OnboardingError("selected folder observation must be a nonempty list")
    selected: list[tuple[int, str, str]] = []
    for item in raw:
        if (
            not isinstance(item, list | tuple)
            or len(item) != _SELECTED_FOLDER_FACT_COUNT
            or not isinstance(item[0], int)
            or isinstance(item[0], bool)
            or item[0] <= 0
            or not isinstance(item[1], str)
            or item[2] != "None"
        ):
            raise OnboardingError("selected folder observation is malformed")
        try:
            path = normalize_posix(item[1])
        except PathError:
            raise OnboardingError("selected folder observation has an invalid path") from None
        selected.append((item[0], path, "None"))
    if len({item[0] for item in selected}) != len(selected):
        raise OnboardingError("selected folder observation has duplicate IDs")
    return tuple(sorted(selected))


@dataclass(frozen=True)
class ScopeBinding:
    """Non-secret facts that define one authorized Sonarr/Shoko scope."""

    sonarr_url: str
    shoko_url: str
    sonarr_version: str
    shoko_version: str
    capabilities: tuple[str, ...]
    roots: tuple[str, ...]
    path_mappings: tuple[PathMapping, ...]
    safety_digest: str
    unverifiable_facts: tuple[str, ...]
    settings_observation: tuple[tuple[str, bool | None], ...] | None = None
    selected_folders_observation: tuple[tuple[int, str, str], ...] | None = None

    def __post_init__(self) -> None:
        try:
            object.__setattr__(self, "sonarr_url", validate_base_url(self.sonarr_url, "sonarr.url"))
            object.__setattr__(self, "shoko_url", validate_base_url(self.shoko_url, "shoko.url"))
            roots = tuple(sorted({normalize_posix(root) for root in self.roots}))
            mappings = validate_mappings(self.path_mappings)
        except (ConfigError, PathError, ValueError) as exc:
            raise OnboardingError(
                "setup scope contains an invalid URL, root, or path mapping"
            ) from exc
        if not roots:
            raise OnboardingError("setup scope must include at least one confirmed anime root")
        if not isinstance(self.sonarr_version, str) or not self.sonarr_version.strip():
            raise OnboardingError("sonarr.version must be a verified non-empty value")
        if not isinstance(self.shoko_version, str) or not self.shoko_version.strip():
            raise OnboardingError("shoko.version must be a verified non-empty value")
        if not _HEX64_RE.fullmatch(self.safety_digest):
            raise OnboardingError("safety_digest must be a lowercase SHA-256 value")
        if any(
            not any(is_within_root(mapping.sonarr, root) for root in roots) for mapping in mappings
        ):
            raise OnboardingError("path mapping source must stay inside a confirmed anime root")
        if self.settings_observation is not None:
            observations = dict(self.settings_observation)
            if (
                len(observations) != len(self.settings_observation)
                or set(observations) != set(SAFETY_SETTING_PATHS)
                or any(
                    value is not None and not isinstance(value, bool)
                    for value in observations.values()
                )
            ):
                raise OnboardingError(
                    "setup settings observation must contain only safety booleans"
                )
            object.__setattr__(self, "settings_observation", tuple(sorted(observations.items())))
        if self.selected_folders_observation is not None:
            object.__setattr__(
                self,
                "selected_folders_observation",
                _validated_folder_observation(self.selected_folders_observation),
            )
        capabilities = tuple(sorted(set(self.capabilities)))
        facts = tuple(sorted(set(self.unverifiable_facts)))
        if any(not item.strip() for item in (*capabilities, *facts)):
            raise OnboardingError("capability and confirmation fact labels must not be empty")
        object.__setattr__(self, "sonarr_version", self.sonarr_version.strip())
        object.__setattr__(self, "shoko_version", self.shoko_version.strip())
        object.__setattr__(self, "capabilities", capabilities)
        object.__setattr__(self, "roots", roots)
        object.__setattr__(self, "path_mappings", mappings)
        object.__setattr__(self, "unverifiable_facts", facts)

    def document(self) -> dict[str, Any]:
        """Canonical non-secret representation used only for durable digest binding."""
        document: dict[str, Any] = {
            "sonarr_url": self.sonarr_url,
            "shoko_url": self.shoko_url,
            "sonarr_version": self.sonarr_version,
            "shoko_version": self.shoko_version,
            "capabilities": list(self.capabilities),
            "roots": list(self.roots),
            "path_mappings": [
                {"sonarr": mapping.sonarr, "shoko": mapping.shoko} for mapping in self.path_mappings
            ],
            "safety_digest": self.safety_digest,
            "unverifiable_facts": list(self.unverifiable_facts),
        }

        # Omit the new optional baseline for legacy documents to preserve their digest.
        if self.settings_observation is not None:
            document["settings_observation"] = dict(self.settings_observation)
        if self.selected_folders_observation is not None:
            document["selected_folders_observation"] = [
                list(folder) for folder in self.selected_folders_observation
            ]
        return document

    @property
    def digest(self) -> str:
        canonical = json.dumps(self.document(), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @classmethod
    def from_document(cls, raw: Any) -> ScopeBinding:
        if not isinstance(raw, dict):
            raise OnboardingError("persisted setup binding is malformed")
        expected = {
            "sonarr_url",
            "shoko_url",
            "sonarr_version",
            "shoko_version",
            "capabilities",
            "roots",
            "path_mappings",
            "safety_digest",
            "unverifiable_facts",
        }
        optional = {"settings_observation", "selected_folders_observation"}
        if not expected <= set(raw) or set(raw) - expected - optional:
            raise OnboardingError("persisted setup binding has an unsupported schema")
        text_fields = (
            "sonarr_url",
            "shoko_url",
            "sonarr_version",
            "shoko_version",
            "safety_digest",
        )
        if not all(isinstance(raw[name], str) for name in text_fields):
            raise OnboardingError("persisted setup binding contains invalid text values")
        list_fields = ("capabilities", "roots", "path_mappings", "unverifiable_facts")
        if not all(isinstance(raw[name], list) for name in list_fields):
            raise OnboardingError("persisted setup binding contains invalid list values")
        mappings: list[PathMapping] = []
        for item in raw["path_mappings"]:
            if not isinstance(item, dict) or set(item) != {"sonarr", "shoko"}:
                raise OnboardingError("persisted setup path mapping is malformed")
            if not isinstance(item["sonarr"], str) or not isinstance(item["shoko"], str):
                raise OnboardingError("persisted setup path mapping is malformed")
            mappings.append(PathMapping(item["sonarr"], item["shoko"]))
        list_values = (*raw["capabilities"], *raw["roots"], *raw["unverifiable_facts"])
        if not all(isinstance(item, str) for item in list_values):
            raise OnboardingError("persisted setup binding contains invalid list entries")
        observation = raw.get("settings_observation")
        if "settings_observation" in raw and (
            not isinstance(observation, dict)
            or set(observation) != set(SAFETY_SETTING_PATHS)
            or any(
                value is not None and not isinstance(value, bool) for value in observation.values()
            )
        ):
            raise OnboardingError("persisted setup settings observation is malformed")
        selected = (
            _validated_folder_observation(raw["selected_folders_observation"])
            if "selected_folders_observation" in raw
            else None
        )
        return cls(
            sonarr_url=raw["sonarr_url"],
            shoko_url=raw["shoko_url"],
            sonarr_version=raw["sonarr_version"],
            shoko_version=raw["shoko_version"],
            capabilities=tuple(raw["capabilities"]),
            roots=tuple(raw["roots"]),
            path_mappings=tuple(mappings),
            safety_digest=raw["safety_digest"],
            unverifiable_facts=tuple(raw["unverifiable_facts"]),
            settings_observation=None if observation is None else tuple(observation.items()),
            selected_folders_observation=selected,
        )


@dataclass(frozen=True)
class OnboardingRecord:
    """An explicit human confirmation; it is never inferred from old campaign state."""

    binding: ScopeBinding
    binding_digest: str
    confirmed_facts: tuple[str, ...]
    automatic_link_consent: bool
    consented_iso: str

    def to_json(self) -> str:
        payload = {
            "schema": ONBOARDING_DOCUMENT_SCHEMA,
            "binding": self.binding.document(),
            "binding_digest": self.binding_digest,
            "confirmed_facts": list(self.confirmed_facts),
            "automatic_link_consent": self.automatic_link_consent,
            "consented_iso": self.consented_iso,
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_json(cls, encoded: str) -> OnboardingRecord:
        try:
            payload = json.loads(encoded)
        except ValueError:
            raise OnboardingError("persisted onboarding record is malformed") from None
        expected = {
            "schema",
            "binding",
            "binding_digest",
            "confirmed_facts",
            "automatic_link_consent",
            "consented_iso",
        }
        if not isinstance(payload, dict) or set(payload) != expected:
            raise OnboardingError("persisted onboarding record has an unsupported schema")
        if payload["schema"] != ONBOARDING_DOCUMENT_SCHEMA:
            raise OnboardingError("persisted onboarding record has an unsupported schema")
        if not isinstance(payload["binding_digest"], str) or not _HEX64_RE.fullmatch(
            payload["binding_digest"]
        ):
            raise OnboardingError("persisted onboarding binding digest is malformed")
        if not isinstance(payload["confirmed_facts"], list) or not all(
            isinstance(item, str) for item in payload["confirmed_facts"]
        ):
            raise OnboardingError("persisted onboarding confirmations are malformed")
        if payload["automatic_link_consent"] is not True:
            raise OnboardingError("persisted onboarding consent is not explicit")
        if not isinstance(payload["consented_iso"], str):
            raise OnboardingError("persisted onboarding timestamp is malformed")
        _validate_iso(payload["consented_iso"])
        binding = ScopeBinding.from_document(payload["binding"])
        if binding.digest != payload["binding_digest"]:
            raise OnboardingError("persisted onboarding binding digest does not match")
        return cls(
            binding=binding,
            binding_digest=payload["binding_digest"],
            confirmed_facts=tuple(payload["confirmed_facts"]),
            automatic_link_consent=True,
            consented_iso=payload["consented_iso"],
        )


class OnboardingContext(NamedTuple):
    """Remote facts tied to the exact consent captured before collecting them."""

    binding: ScopeBinding
    assessment: SafetyAssessment
    consent_json: str


@dataclass(frozen=True)
class OnboardingDecision:
    status: OnboardingStatus
    message: str
    binding_digest: str | None = None


def _validate_iso(value: str) -> None:
    try:
        datetime.fromisoformat(value)
    except ValueError:
        raise OnboardingError("timestamp must be a valid ISO-8601 value") from None


class OnboardingManager:
    """Persist explicit automatic-link consent separately from legacy operator approvals."""

    def __init__(self, store: StateStore) -> None:
        self.store = store

    def record(self) -> OnboardingRecord | None:
        encoded = self.store.get_onboarding_json()
        return None if encoded is None else OnboardingRecord.from_json(encoded)

    def confirm_setup(
        self,
        binding: ScopeBinding,
        assessment: SafetyAssessment,
        *,
        confirmed_facts: tuple[str, ...] | list[str],
        automatic_link_consent: bool,
        iso: str,
    ) -> OnboardingRecord:
        _validate_iso(iso)
        if assessment.status is SafetyStatus.UNSAFE:
            raise OnboardingError("unsafe Shoko settings must be fixed before setup confirmation")
        if binding.settings_observation is not None and dict(binding.settings_observation) != dict(
            assessment.observed_settings
        ):
            raise OnboardingError("setup settings observation does not match current safety facts")
        if binding.safety_digest != assessment.digest:
            raise OnboardingError("safety settings changed during setup; review them again")
        if set(binding.unverifiable_facts) != set(assessment.unverifiable_facts):
            raise OnboardingError("setup binding does not cover the current physical-safety facts")
        if automatic_link_consent is not True:
            raise OnboardingError("explicit consent to automatic linking is required")

        required = set(assessment.unverifiable_facts) | set(assessment.unknown_settings)
        confirmed = set(confirmed_facts)
        missing = sorted(required - confirmed)
        extra = sorted(confirmed - required)
        if missing:
            raise OnboardingError(
                f"setup confirmation is missing required facts: {', '.join(missing)}"
            )
        if extra:
            raise OnboardingError(f"setup confirmation contains unknown facts: {', '.join(extra)}")

        record = OnboardingRecord(
            binding=binding,
            binding_digest=binding.digest,
            confirmed_facts=tuple(sorted(confirmed)),
            automatic_link_consent=True,
            consented_iso=iso,
        )
        self.store.confirm_onboarding_json(record.to_json(), iso)
        return record

    def reconfirmation_required(self) -> bool:
        """Return the durable hard-consent hold, including pre-latch legacy pause reasons."""
        if self.store.get_meta(META_ONBOARDING_RECONFIRMATION_REQUIRED) is not None:
            return True
        paused, reason = self.store.get_service_pause()
        return paused and reason in SYSTEM_RECONFIRMATION_PAUSE_REASONS

    def evaluate(
        self,
        binding: ScopeBinding,
        assessment: SafetyAssessment,
        *,
        iso: str,
        expected_consent: str | None = None,
    ) -> OnboardingDecision:
        _validate_iso(iso)
        return self._evaluate(
            binding,
            assessment,
            iso=iso,
            respect_user_pause=True,
            expected_consent=expected_consent,
        )

    def evaluate_context(self, context: OnboardingContext, *, iso: str) -> OnboardingDecision:
        return self.evaluate(
            context.binding, context.assessment, iso=iso, expected_consent=context.consent_json
        )

    def resume_context(self, context: OnboardingContext, *, iso: str) -> OnboardingDecision:
        return self.resume(
            context.binding, context.assessment, iso=iso, expected_consent=context.consent_json
        )

    def pause(self, *, iso: str) -> OnboardingDecision:
        _validate_iso(iso)
        self.store.pause_service_for_user(iso)
        return OnboardingDecision(OnboardingStatus.PAUSED, "Automatic linking is paused.")

    def pause_for_review(self, reason: str, message: str, *, iso: str) -> OnboardingDecision:
        """Persist a system hold for changed or temporarily unverifiable setup facts."""
        _validate_iso(iso)
        if reason not in SYSTEM_PAUSE_REASONS:
            raise OnboardingError("unsupported automatic-service review pause reason")
        self._pause_for_system_change(reason, iso)
        return OnboardingDecision(OnboardingStatus.NEEDS_CONFIRMATION, message)

    def observe_review(self, record: OnboardingRecord, reason: str, *, iso: str) -> bool:
        """Persist an observed hard hold only if its approved consent is still current."""
        _validate_iso(iso)
        if reason not in SYSTEM_RECONFIRMATION_PAUSE_REASONS:
            raise OnboardingError("unsupported observed setup review reason")
        encoded = self.store.get_onboarding_json()
        if encoded is None or OnboardingRecord.from_json(encoded) != record:
            return False
        return self.store.pause_onboarding_if_current(encoded, reason, iso)

    def resume(
        self,
        binding: ScopeBinding,
        assessment: SafetyAssessment,
        *,
        iso: str,
        expected_consent: str | None = None,
    ) -> OnboardingDecision:
        _validate_iso(iso)
        if expected_consent is None:
            expected_consent = self.store.get_onboarding_json()
        decision = self._evaluate(
            binding,
            assessment,
            iso=iso,
            respect_user_pause=False,
            expected_consent=expected_consent,
        )
        if decision.status is OnboardingStatus.AUTHORIZED:
            if expected_consent is not None and self.store.resume_onboarding_if_current(
                expected_consent, iso
            ):
                return decision
            return OnboardingDecision(
                OnboardingStatus.NEEDS_CONFIRMATION,
                "Setup changed while resuming. Review the current scope and safety facts.",
                binding.digest,
            )
        return decision

    def _pause_for_system_change(self, reason: str, iso: str) -> None:
        self.store.pause_service_for_review(reason, iso)

    def _evaluate(
        self,
        binding: ScopeBinding,
        assessment: SafetyAssessment,
        *,
        iso: str,
        respect_user_pause: bool,
        expected_consent: str | None = None,
    ) -> OnboardingDecision:
        encoded = self.store.get_onboarding_json()
        if expected_consent is not None and encoded != expected_consent:
            return OnboardingDecision(
                OnboardingStatus.NEEDS_CONFIRMATION,
                "Setup changed during safety checks. Refresh setup before resuming.",
                binding.digest,
            )
        record = None if encoded is None else OnboardingRecord.from_json(encoded)
        status = OnboardingStatus.AUTHORIZED
        message = "Setup is confirmed and current safety checks pass."
        binding_digest: str | None = None
        if assessment.status is SafetyStatus.UNSAFE:
            held = self.store.pause_onboarding_if_current(encoded, "unsafe_shoko_settings", iso)
            status = OnboardingStatus.UNSAFE if held else OnboardingStatus.NEEDS_CONFIRMATION
            message = (
                assessment.message
                if held
                else "Setup changed during safety checks. Refresh setup before resuming."
            )
        elif record is None:
            paused, _reason = self.store.get_service_pause()
            status = OnboardingStatus.PAUSED if paused else OnboardingStatus.NOT_CONFIGURED
            message = (
                "Automatic linking is paused."
                if paused
                else "Complete scope and safety setup before automatic linking."
            )
        else:
            binding_digest = binding.digest
            required_facts = set(assessment.unverifiable_facts) | set(assessment.unknown_settings)
            current_facts_match = required_facts <= set(record.confirmed_facts)
            if (
                binding.digest != record.binding_digest
                or binding.safety_digest != assessment.digest
                or set(binding.unverifiable_facts) != set(assessment.unverifiable_facts)
                or not current_facts_match
            ):
                held = self.observe_review(record, "setup_changed", iso=iso)
                status = OnboardingStatus.NEEDS_CONFIRMATION
                message = (
                    "Setup or Shoko safety changed. Confirm the current scope and safety facts."
                    if held
                    else "Setup changed during safety checks. Refresh setup before resuming."
                )
            elif self.reconfirmation_required():
                status = OnboardingStatus.NEEDS_CONFIRMATION
                message = "Setup must be confirmed again before automatic linking can resume."
            elif not record.automatic_link_consent:
                status = OnboardingStatus.NEEDS_CONFIRMATION
                message = "Explicit consent to automatic linking is required."
            else:
                paused, reason = self.store.get_service_pause()
                if paused and respect_user_pause:
                    status = OnboardingStatus.PAUSED
                    message = "Automatic linking is paused."
                elif paused and reason == "setup_unavailable":
                    message = (
                        "Current setup checks pass. Resume automatic linking to clear the "
                        "temporary connection hold."
                    )
                elif paused and reason != "user_paused":
                    status = OnboardingStatus.NEEDS_CONFIRMATION
                    message = "Setup must be confirmed again before automatic linking can resume."
        if status is OnboardingStatus.AUTHORIZED and self.store.get_onboarding_json() != encoded:
            status = OnboardingStatus.NEEDS_CONFIRMATION
            message = "Setup changed during safety checks. Refresh setup before resuming."
        return OnboardingDecision(status, message, binding_digest)
