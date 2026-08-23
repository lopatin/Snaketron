"""One resumable production state machine invoked by Hermes."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import io
import json
import math
import os
import re
import socket
import time
import uuid
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from PIL import Image, ImageDraw
from pydantic import BaseModel

from .assets import AssetProcessor, ForgeBundle, ForgeVariant
from .calibration import (
    BUILD_JUDGE_RUBRIC,
    PROTOTYPE_JUDGE_RUBRIC,
    JudgeCalibrationService,
    judge_evaluator_version,
)
from .config import FactoryConfig
from .db import Database, canonical_json
from .domain import (
    ArtifactKind,
    AssetPlan,
    ConceptProposal,
    Disposition,
    GateResult,
    GateVerdict,
    ImplementationPlan,
    InlineArtifact,
    ModifierPlan,
    OperationStatus,
    PrototypeManifest,
    ProviderError,
    ProviderFailureKind,
    ProviderResult,
    Purpose,
    Stage,
    VisualJudgment,
    WorkerRequest,
    WorkerResult,
    current_worker_result_json_schema,
)
from .draft_automation import (
    DraftInbox,
    DraftInboxError,
    DraftMediaPreplan,
    DraftVideoIntent,
    draft_attempt_metadata,
    is_draft_attempt,
)
from .fal_media import (
    PIXVERSE_TRANSITION_CAPABILITY,
    FalPixVerseTransitionAdapter,
    FalQueuePending,
    FalQueueTicket,
    PixVerseTransitionOptions,
    validate_pixverse_video_result,
)
from .gates import GateRunner
from .lama import LamaRuntimeError, lama_bundle_manifest, lama_bundle_sha
from .objects import ObjectStore
from .operations import ExistingOperation, OperationJournal
from .outbox import OutboxDispatcher
from .persistence import SUPPORTED_IMAGE_MEDIA_TYPES, ResultPersistence
from .prototype_projection import (
    PROTOTYPE_PROJECTION_VERSION,
    PrototypeProjectionError,
    project_prototype_body,
)
from .providers import ProviderRegistry
from .recovery import (
    RecoveredResultError,
    validate_recovered_result,
    validate_registration_result,
    validate_skin_authority_readback,
)
from .renderer import (
    BrowserRenderer,
    RendererDrift,
    renderer_bundle_manifest_sha,
    renderer_execution_config,
    renderer_execution_config_sha,
)
from .snaketron_api import SnaketronApi
from .video_frames import (
    MatteEndpointRequest,
    VideoFrameExtractionConfig,
    VideoFrameExtractionError,
    VideoFrameExtractionRequest,
    extract_rgba_frame_sheet,
    validate_matte_endpoint,
    video_toolchain_identity,
)
from .worker import SkillBundle, WorkerAdapter, build_worker
from .worker_validation import (
    WorkerContractError,
    assert_resolved_document,
    asset_row_texels,
    expected_materialized_catalog_record,
    validate_plan_resource_limits,
    validate_worker_handoff,
)


class BudgetExceeded(RuntimeError):
    pass


class RunWallTimeExceeded(BudgetExceeded):
    """The current leased tick exhausted its wall-time allowance."""


class BehaviorDrift(RuntimeError):
    """An in-flight Attempt no longer matches its immutable execution inputs."""


_PROVIDER_IMAGE_ASPECTS = {
    "1:1": 1.0,
    "4:3": 4 / 3,
    "3:4": 3 / 4,
    "16:9": 16 / 9,
    "9:16": 9 / 16,
}
# A direct, no-crop resize corrects at most fifteen percent of aspect drift.
# Anything taller is divided into independently journaled temporal slices.
_PROVIDER_SLICE_ASPECT_LOG_TOLERANCE = math.log(1.15)
_PROTOTYPE_IMAGE_PROMPT_VERSION = "prototype-image-prompt-v2"
_PROTOTYPE_IMAGE_RULES_START = "<!-- PROTOTYPE_IMAGE_RULES:START -->"
_PROTOTYPE_IMAGE_RULES_END = "<!-- PROTOTYPE_IMAGE_RULES:END -->"


class Factory:
    def __init__(
        self,
        config: FactoryConfig,
        *,
        database: Database | None = None,
        objects: ObjectStore | None = None,
        providers: ProviderRegistry | None = None,
        worker: WorkerAdapter | None = None,
        api: SnaketronApi | None = None,
        renderer: BrowserRenderer | None = None,
        fal_media: FalPixVerseTransitionAdapter | None = None,
    ) -> None:
        self.config = config
        self.database = database or Database(config.paths.database)
        self.database.migrate()
        self.objects = objects or ObjectStore(config.paths.objects)
        self.providers = providers or ProviderRegistry(config)
        self.worker = worker or build_worker(config)
        self.api = api or SnaketronApi(config)
        self.renderer = renderer or BrowserRenderer(config)
        self.fal_media = fal_media or FalPixVerseTransitionAdapter(
            api_key_env=config.draft_automation.fal_api_key_env,
            fallback_api_key_envs=(config.draft_automation.fal_api_key_fallback_env,),
            queue_base_url=config.draft_automation.fal_queue_base_url,
            timeout_seconds=min(60, config.draft_automation.fal_transition_timeout_seconds),
            poll_deadline_seconds=config.draft_automation.fal_transition_timeout_seconds,
            max_status_polls=1,
            poll_interval_seconds=0,
        )
        self.assets = AssetProcessor(config)
        self.gates = GateRunner(config)
        self.journal = OperationJournal(self.database)
        self.persistence = ResultPersistence(self.objects)
        self.calibration = JudgeCalibrationService(self.database, config)
        self._lease_token: str | None = None
        self._run_deadline: float | None = None

    async def close(self) -> None:
        for owner in (self.providers, self.worker, self.api, self.fal_media):
            close = getattr(owner, "close", None)
            if close:
                result = close()
                if hasattr(result, "__await__"):
                    await result

    def behavior_snapshot(self) -> dict[str, Any]:
        skill, skill_git_ref, skill_git_sha = self.active_skill_bundle()
        automation_bundle: SkillBundle | None = None
        automation_bundle_object = None
        if self.config.draft_automation.enabled:
            automation_bundle = SkillBundle.load(self.config.draft_automation.skill_dir)
            automation_bundle_object = self.objects.put(canonical_json(automation_bundle.files).encode("utf-8"))
        direction = self.config.paths.direction.read_bytes()
        guideline_text = self._skill_guidelines(skill)
        prototype_geometry, prototype_guide, _ = self._current_prototype_geometry()
        capability = self.config.paths.capability_manifest.read_bytes()
        gates = self.config.paths.gate_manifest.read_bytes()
        model_config = self._model_config_bytes()
        direction_object = self.objects.put(direction)
        guideline_object = self.objects.put(guideline_text)
        prototype_geometry_object = self.objects.put(prototype_geometry)
        prototype_guide_object = self.objects.put(prototype_guide)
        capability_object = self.objects.put(capability)
        gate_object = self.objects.put(gates)
        model_config_object = self.objects.put(model_config)
        renderer_config = renderer_execution_config(self.config)
        lama_bundle = lama_bundle_manifest(self.config)
        snapshot = {
            "snapshot_version": 7,
            "direction_sha": direction_object.sha256,
            "direction_ref": direction_object.uri,
            "design_guidelines_sha": guideline_object.sha256,
            "design_guidelines_ref": guideline_object.uri,
            "prototype_geometry_sha": prototype_geometry_object.sha256,
            "prototype_geometry_ref": prototype_geometry_object.uri,
            "prototype_guide_sha": prototype_guide_object.sha256,
            "prototype_guide_ref": prototype_guide_object.uri,
            "skill_sha": skill.sha256,
            "skill_git_ref": skill_git_ref,
            "skill_git_sha": skill_git_sha,
            "capability_sha": capability_object.sha256,
            "capability_ref": capability_object.uri,
            "gate_sha": gate_object.sha256,
            "gate_ref": gate_object.uri,
            "model_config_sha": model_config_object.sha256,
            "model_config_ref": model_config_object.uri,
            "config_sha": self.config.version_sha256,
            "mode": self.config.mode,
            "models": self.config.public_snapshot()["models"],
            "runtime_sha": _runtime_sha(lama_bundle),
            "renderer_sha": self._current_renderer_sha(),
            "renderer_config": renderer_config,
            "renderer_config_sha": renderer_execution_config_sha(renderer_config),
            "lama_bundle": lama_bundle,
            "lama_bundle_sha": lama_bundle_sha(lama_bundle),
        }
        if automation_bundle is not None and automation_bundle_object is not None:
            snapshot["draft_automation_skill_sha"] = automation_bundle.sha256
            snapshot["draft_automation_skill_ref"] = automation_bundle_object.uri
        return snapshot

    def pinned_draft_automation_bundle(self, attempt: dict[str, Any]) -> SkillBundle:
        """Load the exact orchestration skill retained in this Attempt snapshot."""

        behavior = json.loads(attempt["behavior_json"])
        reference = behavior.get("draft_automation_skill_ref")
        expected = behavior.get("draft_automation_skill_sha")
        if not isinstance(reference, str) or not isinstance(expected, str):
            raise FileNotFoundError("draft Attempt has no retained automation skill bundle")
        try:
            files = self.persistence.load_json(reference)
        except (FileNotFoundError, RuntimeError, ValueError) as error:
            raise FileNotFoundError("retained draft automation skill bundle is unreadable") from error
        if not isinstance(files, dict) or not all(
            isinstance(name, str) and isinstance(body, str) for name, body in files.items()
        ):
            raise FileNotFoundError("retained draft automation skill bundle is malformed")
        bundle = SkillBundle.from_files(files)
        if bundle.sha256 != expected:
            raise FileNotFoundError("retained draft automation skill bundle hash differs")
        return bundle

    def control_attempt(self, namespace: str = "optimizer") -> dict[str, Any]:
        """Return a terminal current-behavior coordinator for meta-model calls.

        Feedback routing and technique mining consume historical evidence, but
        must execute with the current model/runtime snapshot. The complete
        behavior digest gives each upgrade one durable coordinator and keeps it
        outside the production/build/publication state machine.
        """

        allowed = "abcdefghijklmnopqrstuvwxyz0123456789-_"
        if not namespace or any(character not in allowed for character in namespace):
            raise ValueError("control Attempt namespace must be lowercase ASCII")
        behavior = self.behavior_snapshot()
        identity = hashlib.sha256(canonical_json(behavior).encode()).hexdigest()
        key = f"control:{namespace}:{identity}"
        existing = self.database.find_attempt_by_key(key)
        if existing is not None:
            if existing["purpose"] != Purpose.CONTROL or existing["disposition"] != Disposition.EXPERIMENT_COMPLETE:
                raise RuntimeError("optimizer control idempotency key belongs to a non-control Attempt")
            return existing
        concept = self.database.create_concept(
            name=f"{namespace} control {identity[:12]}",
            brief="Terminal non-production coordinator for behavior-pinned optimizer model calls.",
            seed=identity,
            source="optimizer-control",
            tags=["control", namespace],
        )
        return self.database.create_attempt(
            concept_id=concept["id"],
            purpose=Purpose.CONTROL,
            stage=Stage.COMPLETE,
            idempotency_key=key,
            behavior=behavior,
            direction_sha=behavior["direction_sha"],
            skill_sha=behavior["skill_sha"],
            capability_sha=behavior["capability_sha"],
            gate_sha=behavior["gate_sha"],
            model_config_sha=behavior["model_config_sha"],
            restart_stage=f"control:{namespace}",
            experiment_split="control",
            disposition=Disposition.EXPERIMENT_COMPLETE,
        )

    def _model_config_bytes(self) -> bytes:
        public = self.config.public_snapshot()
        return canonical_json(
            {
                "models": public["models"],
                "worker": public["worker"],
                "snaketron_resolved_model_pattern": self.config.service.resolved_model_pattern,
            }
        ).encode("utf-8")

    def _current_renderer_sha(self) -> str | None:
        resolver = getattr(self.renderer, "renderer_sha", None)
        if resolver is None or not callable(resolver):
            return None
        try:
            value = resolver()
        except Exception:
            if isinstance(self.renderer, BrowserRenderer):
                raise
            # Some development fixtures are not Git worktrees. Production's
            # browser renderer exposes a tree SHA and is checked below.
            return None
        return str(value) if value else None

    def _snapshot_bytes(
        self,
        attempt: dict[str, Any],
        name: str,
        expected_sha: str,
        legacy_path: Path,
    ) -> bytes:
        behavior = json.loads(attempt["behavior_json"])
        reference = behavior.get(f"{name}_ref")
        if int(behavior.get("snapshot_version", 0)) >= 2:
            if not isinstance(reference, str):
                raise FileNotFoundError(f"pinned {name} snapshot reference is absent")
            payload = self.objects.get(reference)
        else:
            payload = legacy_path.read_bytes()
        actual = hashlib.sha256(payload).hexdigest()
        if actual != expected_sha:
            raise FileNotFoundError(f"pinned {name} snapshot {expected_sha} differs from retained bytes {actual}")
        return payload

    def pinned_direction(self, attempt: dict[str, Any]) -> bytes:
        return self._snapshot_bytes(
            attempt,
            "direction",
            str(attempt["direction_sha"]),
            self.config.paths.direction,
        )

    @staticmethod
    def _skill_guidelines(skill: SkillBundle) -> bytes:
        text = skill.files.get("references/design-guidelines.md")
        if not isinstance(text, str) or not text.strip():
            raise FileNotFoundError("canonical authoring skill lacks references/design-guidelines.md")
        return text.encode("utf-8")

    def pinned_design_guidelines(self, attempt: dict[str, Any]) -> bytes:
        behavior = json.loads(attempt["behavior_json"])
        if int(behavior.get("snapshot_version", 0)) < 6:
            return b""
        return self._snapshot_bytes(
            attempt,
            "design_guidelines",
            str(behavior.get("design_guidelines_sha", "")),
            self.config.paths.skill_dir / "references" / "design-guidelines.md",
        )

    def pinned_prototype_geometry(self, attempt: dict[str, Any]) -> tuple[dict[str, Any], bytes] | None:
        behavior = json.loads(attempt["behavior_json"])
        if int(behavior.get("snapshot_version", 0)) < 6:
            return None
        contract_payload = self._snapshot_bytes(
            attempt,
            "prototype_geometry",
            str(behavior.get("prototype_geometry_sha", "")),
            self.config.paths.prototype_geometry,
        )
        guide_ref = behavior.get("prototype_guide_ref")
        expected_guide_sha = behavior.get("prototype_guide_sha")
        if not isinstance(guide_ref, str) or not isinstance(expected_guide_sha, str):
            raise FileNotFoundError("pinned prototype geometry guide reference is absent")
        guide = self.objects.get(guide_ref)
        actual = hashlib.sha256(guide).hexdigest()
        if actual != expected_guide_sha:
            raise FileNotFoundError(f"pinned prototype guide {expected_guide_sha} differs from retained bytes {actual}")
        return self._validate_prototype_geometry(contract_payload, guide), guide

    def _current_prototype_geometry(self) -> tuple[bytes, bytes, dict[str, Any]]:
        contract_path = self.config.paths.prototype_geometry
        contract_payload = contract_path.read_bytes()
        try:
            contract = json.loads(contract_payload)
            relative_guide = contract["guide"]
        except (KeyError, TypeError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError(f"prototype geometry contract is malformed: {error}") from error
        if not isinstance(relative_guide, str) or not relative_guide:
            raise ValueError("prototype geometry contract has no guide path")
        guide_path = (contract_path.parent / relative_guide).resolve()
        try:
            guide_path.relative_to(self.config.paths.repo_root.resolve())
        except ValueError as error:
            raise ValueError("prototype geometry guide must remain inside the repository") from error
        guide = guide_path.read_bytes()
        return contract_payload, guide, self._validate_prototype_geometry(contract_payload, guide)

    @staticmethod
    def _validate_prototype_geometry(contract_payload: bytes, guide: bytes) -> dict[str, Any]:
        try:
            contract = json.loads(contract_payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError(f"prototype geometry contract is not exact UTF-8 JSON: {error}") from error
        if not isinstance(contract, dict) or contract.get("schema_version") != 1:
            raise ValueError("prototype geometry contract must use schema_version 1")
        expected = contract.get("guide_sha256")
        actual = hashlib.sha256(guide).hexdigest()
        if not isinstance(expected, str) or expected != actual:
            raise ValueError(f"prototype geometry guide hash {actual} differs from contract {expected!r}")
        canvas = contract.get("guide_canvas")
        if not isinstance(canvas, dict):
            raise ValueError("prototype geometry contract has no guide_canvas")
        try:
            with Image.open(io.BytesIO(guide)) as opened:
                opened.load()
                image_format = opened.format
                image_size = opened.size
        except Exception as error:
            raise ValueError(f"prototype geometry guide is not a decodable image: {error}") from error
        expected_size = (canvas.get("width_px"), canvas.get("height_px"))
        if image_format != "PNG" or image_size != expected_size:
            raise ValueError(
                f"prototype geometry guide must be PNG {expected_size[0]}x{expected_size[1]}, "
                f"got {image_format} {image_size[0]}x{image_size[1]}"
            )
        return contract

    def pinned_gates(self, attempt: dict[str, Any]) -> GateRunner:
        if not isinstance(self.gates, GateRunner):
            # Explicit test/in-process adapters remain injectable. Installed
            # production always owns the concrete GateRunner initialized above.
            return self.gates  # type: ignore[return-value]
        capability = self._snapshot_bytes(
            attempt,
            "capability",
            str(attempt["capability_sha"]),
            self.config.paths.capability_manifest,
        )
        gates = self._snapshot_bytes(
            attempt,
            "gate",
            str(attempt["gate_sha"]),
            self.config.paths.gate_manifest,
        )
        return GateRunner(
            self.config,
            capability_payload=capability,
            gate_payload=gates,
        )

    def _behavior_drift_reason(
        self,
        attempt: dict[str, Any],
        stage: Stage | str,
    ) -> str | None:
        try:
            behavior = json.loads(attempt["behavior_json"])
        except (TypeError, ValueError):
            return "Attempt behavior snapshot is malformed"
        if int(behavior.get("snapshot_version", 0)) < 2:
            return None
        try:
            retained_model_config = self._snapshot_bytes(
                attempt,
                "model_config",
                str(attempt["model_config_sha"]),
                self.config.source_path or Path("factory.yaml"),
            )
        except (FileNotFoundError, RuntimeError, ValueError) as error:
            return str(error)
        if retained_model_config != self._model_config_bytes():
            return "model or task-worker configuration changed during an in-flight Attempt"
        if int(behavior.get("snapshot_version", 0)) >= 6:
            try:
                current_geometry, current_guide, _ = self._current_prototype_geometry()
            except (FileNotFoundError, OSError, RuntimeError, ValueError) as error:
                return str(error)
            if hashlib.sha256(current_geometry).hexdigest() != behavior.get("prototype_geometry_sha"):
                return "prototype geometry contract changed during an in-flight Attempt"
            if hashlib.sha256(current_guide).hexdigest() != behavior.get("prototype_guide_sha"):
                return "prototype geometry guide changed during an in-flight Attempt"
        current_lama: dict[str, Any] | None = None
        if int(behavior.get("snapshot_version", 0)) >= 5:
            pinned_lama = behavior.get("lama_bundle")
            pinned_lama_sha = behavior.get("lama_bundle_sha")
            if not isinstance(pinned_lama, dict) or not isinstance(pinned_lama_sha, str):
                return "pinned LaMa runtime bundle is absent"
            if lama_bundle_sha(pinned_lama) != pinned_lama_sha:
                return "pinned LaMa runtime bundle hash is invalid"
            try:
                current_lama = lama_bundle_manifest(self.config)
            except LamaRuntimeError as error:
                return str(error)
            if current_lama != pinned_lama:
                return "LaMa dependency lock, offline loader, or model changed during an in-flight Attempt"
        if behavior.get("runtime_sha") != _runtime_sha(current_lama):
            return "factory runtime changed during an in-flight Attempt"
        if behavior.get("renderer_sha"):
            current_renderer = self._current_renderer_sha()
            if current_renderer != behavior["renderer_sha"]:
                return "repository/renderer tree changed during an in-flight Attempt"
        if int(behavior.get("snapshot_version", 0)) >= 3:
            pinned_renderer_config = behavior.get("renderer_config")
            pinned_renderer_config_sha = behavior.get("renderer_config_sha")
            if not isinstance(pinned_renderer_config, dict) or not isinstance(pinned_renderer_config_sha, str):
                return "pinned renderer configuration is absent"
            if renderer_execution_config_sha(pinned_renderer_config) != pinned_renderer_config_sha:
                return "pinned renderer configuration hash is invalid"
            if int(behavior.get("snapshot_version", 0)) >= 4:
                pinned_bundle = pinned_renderer_config.get("browser_bundle")
                pinned_bundle_sha = pinned_renderer_config.get("browser_bundle_sha256")
                if not isinstance(pinned_bundle, dict) or not isinstance(pinned_bundle_sha, str):
                    return "pinned cached renderer bundle is absent"
                if renderer_bundle_manifest_sha(pinned_bundle) != pinned_bundle_sha:
                    return "pinned cached renderer bundle hash is invalid"
            current_renderer_config = renderer_execution_config(self.config)
            if current_renderer_config != pinned_renderer_config:
                return "renderer config SHA, endpoints, capture command, or timeout changed during an in-flight Attempt"
        return None

    def active_skill_bundle(self) -> tuple[SkillBundle, str, str]:
        active = self.database.active_behavior("author-skin")
        if active is not None:
            bundle = SkillBundle.load_git(
                self.config.paths.repo_root,
                active["sha"],
                self.config.paths.skill_dir,
            )
            return bundle, active["git_ref"], active["sha"]
        bundle = SkillBundle.load(self.config.paths.skill_dir)
        return bundle, "HEAD", _git_commit(self.config.paths.repo_root)

    def pinned_skill_bundle(self, attempt: dict[str, Any]) -> SkillBundle:
        behavior = json.loads(attempt["behavior_json"])
        git_sha = behavior.get("skill_git_sha")
        if git_sha:
            try:
                bundle = SkillBundle.load_git(
                    self.config.paths.repo_root,
                    git_sha,
                    self.config.paths.skill_dir,
                )
                if bundle.sha256 == attempt["skill_sha"]:
                    return bundle
            except FileNotFoundError:
                pass
        # This fallback supports a clean pre-commit development checkout while
        # retaining exact hash binding. Installed production versions always
        # resolve from their immutable Git commit/tag.
        bundle = SkillBundle.load(self.config.paths.skill_dir)
        if bundle.sha256 != attempt["skill_sha"]:
            raise FileNotFoundError(f"pinned skill bundle {attempt['skill_sha']} is not reachable from Git or checkout")
        return bundle

    async def run_once(self) -> dict[str, Any]:
        self.database.migrate()
        self.database.assert_file_permissions()
        self.objects.assert_permissions()
        owner = f"service:{socket.gethostname()}:{os.getpid()}"
        token = self.database.acquire_lease("production", owner, self.config.lease_seconds)
        self._lease_token = token
        started = time.monotonic()
        self._run_deadline = started + self.config.budgets.wall_seconds_per_run
        report: dict[str, Any] = {"owner": owner, "advanced": [], "halt": None}
        try:
            if self.config.draft_automation.enabled:
                report["draft_candidate_budget"] = self.config.draft_candidate_budget_report()
            report["calibration"] = self.calibration.refresh_all()
            report["judge_routing"] = {
                kind: self.calibration.routing_status(kind).as_report() for kind in ("prototype", "build")
            }
            report["outbox"] = await OutboxDispatcher(self.database, self.objects, self.config).dispatch_due()
            unresolved = self.database.unresolved_operations()
            if unresolved:
                report["halt"] = {
                    "reason": "reconciliation_required",
                    "operations": [operation["id"] for operation in unresolved],
                }
                return report
            if self.config.draft_automation.enabled:
                try:
                    report["admin_review_reconciliation"] = await self._reconcile_admin_reviews()
                except ProviderError as error:
                    report["halt"] = {
                        "reason": "admin_review_reconciliation_unavailable",
                        "detail": f"{error.kind}: {error}",
                    }
                    return report
            attempt = self.database.next_active_attempt()
            if attempt is None:
                if self.config.draft_automation.enabled:
                    admission_halt = self._draft_generation_halt_detail()
                    if admission_halt is not None:
                        report["halt"] = admission_halt
                    else:
                        pending_admin = self.database.count_attempts(disposition=Disposition.AWAITING_ADMIN_REVIEW)
                        if pending_admin >= self.config.draft_automation.max_pending_admin_reviews:
                            report["halt"] = {
                                "reason": "admin_review_wip_cap",
                                "pending": pending_admin,
                            }
                        else:
                            try:
                                attempt = DraftInbox(self.config.draft_automation.inbox).import_next(self)
                            except DraftInboxError as error:
                                report["halt"] = {"reason": "draft_inbox_invalid", "detail": str(error)}
                            if attempt is None and report["halt"] is None:
                                report["halt"] = {"reason": "draft_inbox_empty"}
                            elif attempt is not None:
                                report["advanced"].append(
                                    {
                                        "attempt": attempt["id"],
                                        "stage": Stage.PROTOTYPE,
                                        "source": "draft_inbox",
                                    }
                                )
                else:
                    reason = self._generation_halt()
                    if reason:
                        report["halt"] = {"reason": reason}
                    else:
                        attempt = self._create_seed_attempt()
                        report["advanced"].append({"attempt": attempt["id"], "stage": Stage.CONCEPT})

            while attempt is not None and (
                attempt["disposition"] == Disposition.ACTIVE
                and time.monotonic() - started < self.config.budgets.wall_seconds_per_run
            ):
                if not is_draft_attempt(attempt) and attempt["stage"] in {
                    Stage.CONCEPT,
                    Stage.PROTOTYPE,
                    Stage.PROTOTYPE_TRIAGE,
                }:
                    prototype_pending = self.database.count_attempts(
                        disposition=Disposition.NEEDS_HUMAN,
                        review_kind="prototype",
                    )
                    if prototype_pending >= self.config.budgets.max_pending_prototype_reviews:
                        report["halt"] = {"reason": "prototype_review_wip_cap"}
                        break
                if not is_draft_attempt(attempt) and attempt["stage"] in {
                    Stage.AUTHOR,
                    Stage.ASSETS,
                    Stage.BUILD_GATE,
                    Stage.REGISTER,
                    Stage.RENDER,
                    Stage.BUILD_TRIAGE,
                }:
                    final_pending = self.database.count_attempts(
                        disposition=Disposition.NEEDS_HUMAN, review_kind="final"
                    )
                    if final_pending >= self.config.budgets.max_pending_final_reviews:
                        report["halt"] = {"reason": "final_review_wip_cap"}
                        break
                before = (attempt["stage"], attempt["version"], attempt["disposition"])
                failure_detail: str | None = None
                scheduled_pending: FalQueuePending | None = None
                try:
                    attempt = await self._advance(attempt)
                except FalQueuePending as pending:
                    # The paid submit ticket and this repeatable read are both
                    # durable. Ordinary queue progress is a scheduler yield,
                    # not a failed skin: retain ACTIVE/AUTHOR and let the next
                    # Hermes tick allocate one new read key.
                    scheduled_pending = pending
                    attempt = self.database.get_attempt(attempt["id"])
                    report["halt"] = {
                        "reason": "scheduled_provider_pending",
                        "provider": "fal.ai",
                        "request_id": pending.request_id,
                        "status": pending.status,
                        "polls_this_tick": pending.polls,
                    }
                except ProviderError as error:
                    if not error.outcome_known or error.kind == ProviderFailureKind.UNKNOWN_OUTCOME:
                        unresolved = self.database.unresolved_operations()
                        report["halt"] = {
                            "reason": "reconciliation_required",
                            "operations": [operation["id"] for operation in unresolved],
                        }
                        return report
                    failure_detail = f"known external failure at {before[0]}: {error.kind}: {error}"
                    attempt = self._block_attempt(
                        attempt,
                        failure_detail,
                        program_halt="provider_model_mismatch" if error.halt_generation else None,
                    )
                except ExistingOperation as error:
                    unresolved = self.database.unresolved_operations()
                    if unresolved:
                        report["halt"] = {
                            "reason": "reconciliation_required",
                            "operations": [operation["id"] for operation in unresolved],
                        }
                        return report
                    failure_detail = f"external operation cannot advance safely: {error}"
                    attempt = self._block_attempt(attempt, failure_detail)
                after = (attempt["stage"], attempt["version"], attempt["disposition"])
                advancement = {
                    "attempt": attempt["id"],
                    "from": before[0],
                    "to": after[0],
                    "state": after[2],
                }
                if failure_detail is not None:
                    advancement["failure"] = failure_detail
                if scheduled_pending is not None:
                    advancement["pending"] = {
                        "provider": "fal.ai",
                        "request_id": scheduled_pending.request_id,
                        "status": scheduled_pending.status,
                    }
                report["advanced"].append(advancement)
                if scheduled_pending is not None:
                    break
                if after == before:
                    break
                self.database.renew_lease("production", token, self.config.lease_seconds)
            if self._wall_time_exhausted():
                report["halt"] = {"reason": "run_wall_time_budget"}

            # Optimizer readiness is part of this same scheduled command. The
            # optimizer module advances at most one resumable job and is loaded
            # lazily so ordinary production does not require DSPy import time.
            if (
                self.config.optimizer.enabled
                and not self.config.draft_automation.enabled
                and not self._wall_time_exhausted()
            ):
                from .optimizer import Optimizer
                from .techniques import TechniqueMiner

                try:
                    report["optimizer"] = await Optimizer(self).advance_if_ready()
                    if report["optimizer"].get("state") == "not_ready":
                        report["techniques"] = await TechniqueMiner(self).advance_if_ready()
                except (ProviderError, ExistingOperation):
                    unresolved = self.database.unresolved_operations()
                    if not unresolved:
                        raise
                    report["halt"] = {
                        "reason": "reconciliation_required",
                        "operations": [operation["id"] for operation in unresolved],
                    }
            return report
        except RunWallTimeExceeded as error:
            report["halt"] = {"reason": "run_wall_time_budget", "detail": str(error)}
            return report
        except BudgetExceeded as error:
            report["halt"] = {"reason": "budget", "detail": str(error)}
            return report
        finally:
            self.database.release_lease("production", token)
            self._lease_token = None
            self._run_deadline = None

    async def _reconcile_admin_reviews(self) -> dict[str, Any]:
        """Bound local WIP to live exact Admin requests, not historical rows."""

        result: dict[str, Any] = {"still_pending": [], "published": [], "rejected": [], "superseded": []}
        for retained in self.database.attempts_by_disposition(Disposition.AWAITING_ADMIN_REVIEW):
            skin_id = retained.get("production_skin_id")
            revision = retained.get("production_revision")
            content_ref = retained.get("production_content_hash")
            if not skin_id or not revision or not content_ref:
                current = self.database.get_attempt(retained["id"])
                self.database.update_attempt(
                    current["id"],
                    current["version"],
                    disposition=Disposition.BLOCKED,
                    failure_json={
                        "reason": "awaiting Admin review without exact private revision authority",
                        "stage": Stage.COMPLETE,
                    },
                )
                result["superseded"].append(retained["id"])
                continue
            response = await self._with_lease_heartbeat(self.api.get_skin_authority(skin_id))
            authority = response.value
            if not isinstance(authority, dict):
                raise ProviderError(
                    ProviderFailureKind.INVALID_OUTPUT,
                    "Snaketron Admin reconciliation returned a non-object authority",
                    request_id=response.request_id,
                    resolved_model=response.resolved_model,
                )
            pending = authority.get("pendingRevision", authority.get("pending_revision"))
            published = authority.get("publishedRevision", authority.get("published_revision"))
            if str(pending) == str(revision):
                result["still_pending"].append(retained["id"])
                continue
            current = self.database.get_attempt(retained["id"])
            # ``contentRef`` is viewer-scoped: for the factory creator it is
            # the current private head, which may advance after Admin publishes
            # this exact immutable revision. The authenticated revision number
            # is the publication authority; registration already bound that
            # revision row to ``content_ref`` and verified exact readback.
            if str(published) == str(revision):
                self.database.update_attempt(
                    current["id"],
                    current["version"],
                    disposition=Disposition.PUBLISHED,
                    review_kind=None,
                )
                result["published"].append(retained["id"])
            elif pending is None:
                # The exact request disappeared and its immutable revision did
                # not become public: Admin rejected or explicitly withdrew it.
                self.database.update_attempt(
                    current["id"],
                    current["version"],
                    disposition=Disposition.HUMAN_REJECTED,
                    review_kind=None,
                )
                result["rejected"].append(retained["id"])
            else:
                self.database.update_attempt(
                    current["id"],
                    current["version"],
                    disposition=Disposition.BLOCKED,
                    review_kind=None,
                    failure_json={
                        "reason": "Admin queue now names a different exact revision",
                        "expected_revision": int(revision),
                        "observed_pending_revision": pending,
                        "stage": Stage.COMPLETE,
                    },
                )
                result["superseded"].append(retained["id"])
        return result

    def _generation_halt(self) -> str | None:
        detail = self._generation_halt_detail()
        return str(detail["reason"]) if detail is not None else None

    def _draft_generation_halt_detail(self) -> dict[str, Any] | None:
        """Safety and spend admission shared with generation, minus local review/target policy."""

        resume = self.database.latest_generation_resume()
        after = str(resume["created_at"]) if resume is not None else None
        explicit = self.database.unresolved_program_halt(after=after)
        if explicit is not None:
            return {
                "reason": f"program_halt:{explicit['program_halt']}:{explicit['id']}",
                "evidence_at": explicit["updated_at"],
                "attempt_id": explicit["id"],
                "acknowledgeable": True,
            }
        gate_cluster = self.database.repeated_blocking_gate_failure(
            window=self.config.halts.deterministic_failure_window,
            threshold=self.config.halts.repeated_blocking_gate_limit,
            after=after,
        )
        if gate_cluster is not None:
            return {
                "reason": f"repeated_blocking_gate:{gate_cluster['gate_name']}",
                "evidence_at": gate_cluster["latest_at"],
                "acknowledgeable": True,
            }
        root_cause = self.database.repeated_root_cause_after_promotion(
            target="authoring_playbook",
            min_confidence=self.config.optimizer.feedback_min_confidence,
            threshold=self.config.halts.repeated_root_cause_after_promotion_limit,
            after=after,
        )
        if root_cause is not None:
            return {
                "reason": f"repeated_root_cause_after_promotion:{root_cause['signature']}",
                "evidence_at": root_cause["latest_at"],
                "acknowledgeable": True,
            }
        report = self.config.draft_candidate_budget_report()
        # A new durable draft must be able to finish its complete conservative
        # pipeline under the remaining daily/program envelope. Admitting only
        # the first candidate creates an unattended half-built Attempt that
        # can never reach Admin review when prior work consumed the balance.
        pipeline_reservation = int(report["full_pipeline_reservation_micros"])
        try:
            self._check_budget(None, pipeline_reservation)
        except BudgetExceeded as error:
            return {
                "reason": "budget_preflight",
                "detail": str(error),
                "required_reservation_micros": pipeline_reservation,
                "acknowledgeable": False,
            }
        return None

    def _generation_halt_detail(self) -> dict[str, Any] | None:
        """Return the exact current generation pause and its acknowledgement boundary."""

        resume = self.database.latest_generation_resume()
        after = str(resume["created_at"]) if resume is not None else None
        explicit = self.database.unresolved_program_halt(after=after)
        if explicit is not None:
            return {
                "reason": f"program_halt:{explicit['program_halt']}:{explicit['id']}",
                "evidence_at": explicit["updated_at"],
                "attempt_id": explicit["id"],
                "acknowledgeable": True,
            }
        if self.database.published_concept_count() >= self.config.program.target_published_skins:
            return {"reason": "published_target_reached", "acknowledgeable": False}
        gate_cluster = self.database.repeated_blocking_gate_failure(
            window=self.config.halts.deterministic_failure_window,
            threshold=self.config.halts.repeated_blocking_gate_limit,
            after=after,
        )
        if gate_cluster is not None:
            return {
                "reason": f"repeated_blocking_gate:{gate_cluster['gate_name']}",
                "evidence_at": gate_cluster["latest_at"],
                "acknowledgeable": True,
            }
        root_cause = self.database.repeated_root_cause_after_promotion(
            target="authoring_playbook",
            min_confidence=self.config.optimizer.feedback_min_confidence,
            threshold=self.config.halts.repeated_root_cause_after_promotion_limit,
            after=after,
        )
        if root_cause is not None:
            return {
                "reason": f"repeated_root_cause_after_promotion:{root_cause['signature']}",
                "evidence_at": root_cause["latest_at"],
                "acknowledgeable": True,
            }
        prototype_pending = self.database.count_attempts(disposition=Disposition.NEEDS_HUMAN, review_kind="prototype")
        if prototype_pending >= self.config.budgets.max_pending_prototype_reviews:
            return {"reason": "prototype_review_wip_cap", "acknowledgeable": False}
        if self.database.count_attempts(disposition=Disposition.ACTIVE) >= (
            self.config.budgets.max_concurrent_attempts
        ):
            return {"reason": "active_attempt_cap", "acknowledgeable": False}
        self._check_budget(None, 0)
        return None

    def _create_seed_attempt(self) -> dict[str, Any]:
        nonce = uuid.uuid4().hex
        concept = self.database.create_concept(
            name=f"Unscored idea {nonce[:8]}",
            brief="Retained before ideation scoring; the smart-text operation fills this proposal.",
            seed=nonce,
            source="factory",
            tags=["unscored"],
        )
        behavior = self.behavior_snapshot()
        return self.database.create_attempt(
            concept_id=concept["id"],
            purpose=Purpose.PRODUCTION,
            stage=Stage.CONCEPT,
            idempotency_key=f"initial:{concept['id']}",
            behavior=behavior,
            direction_sha=behavior["direction_sha"],
            skill_sha=behavior["skill_sha"],
            capability_sha=behavior["capability_sha"],
            gate_sha=behavior["gate_sha"],
            model_config_sha=behavior["model_config_sha"],
        )

    async def _advance(self, attempt: dict[str, Any]) -> dict[str, Any]:
        stage = Stage(attempt["stage"])
        authority_failure = self._shared_prototype_authority_failure(attempt, stage)
        if authority_failure is not None:
            return self._block_attempt(attempt, authority_failure)
        drift = self._behavior_drift_reason(attempt, stage)
        if drift is not None:
            return self._block_attempt(attempt, drift)
        if stage == Stage.CONCEPT:
            return await self._ideate(attempt)
        if stage == Stage.PROTOTYPE:
            return await self._prototype(attempt)
        if stage == Stage.PROTOTYPE_TRIAGE:
            return await self._prototype_triage(attempt)
        if stage == Stage.AUTHOR:
            return await self._author(attempt)
        if stage == Stage.ASSETS:
            return await self._build_assets(attempt)
        if stage == Stage.BUILD_GATE:
            return await self._build_gate(attempt)
        if stage == Stage.REGISTER:
            return await self._register(attempt)
        if stage == Stage.RENDER:
            return await self._render(attempt)
        if stage == Stage.BUILD_TRIAGE:
            return await self._build_triage(attempt)
        return attempt

    def _shared_prototype_authority_failure(
        self,
        attempt: dict[str, Any],
        stage: Stage,
    ) -> str | None:
        """Fail closed before legacy geometry can spend or drive a build."""

        if attempt["purpose"] != Purpose.PRODUCTION:
            return None
        behavior = json.loads(attempt["behavior_json"])
        snapshot_version = int(behavior.get("snapshot_version", 0))
        if snapshot_version < 7 and stage in {
            Stage.CONCEPT,
            Stage.PROTOTYPE,
            Stage.PROTOTYPE_TRIAGE,
            Stage.AUTHOR,
            Stage.ASSETS,
            Stage.BUILD_GATE,
            Stage.REGISTER,
            Stage.RENDER,
            Stage.BUILD_TRIAGE,
        }:
            return "legacy production Attempt must retry under the shared design and geometry contract"
        if stage not in {
            Stage.AUTHOR,
            Stage.ASSETS,
            Stage.BUILD_GATE,
            Stage.REGISTER,
            Stage.RENDER,
            Stage.BUILD_TRIAGE,
        }:
            return None
        approved_hash = attempt.get("approved_prototype_hash")
        if not approved_hash:
            return None
        if snapshot_version < 7:
            return "legacy approved prototype cannot drive a build under the shared geometry contract"
        prototype = self._find_lineage_artifact(
            attempt,
            ArtifactKind.PROTOTYPE,
            content_hash=str(approved_hash),
        )
        if prototype is None:
            return "approved prototype bytes are not retained"
        manifest_artifact = self._prototype_manifest_for(attempt, prototype)
        if manifest_artifact is None:
            return "approved prototype has no contract-bound manifest"
        try:
            manifest = self.persistence.load_json(manifest_artifact["object_ref"])
        except (FileNotFoundError, RuntimeError, ValueError) as error:
            return f"approved prototype manifest is unreadable: {error}"
        required = {
            "design_guidelines_sha256": behavior.get("design_guidelines_sha"),
            "prototype_geometry_sha256": behavior.get("prototype_geometry_sha"),
            "prototype_guide_sha256": behavior.get("prototype_guide_sha"),
        }
        if any(
            not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None for value in required.values()
        ):
            return "Attempt behavior lacks exact shared design and geometry authority hashes"
        if any(manifest.get(key) != value for key, value in required.items()):
            return "approved prototype manifest does not bind the pinned design and geometry contract"
        source_hash = manifest.get("source_image_sha256")
        if (
            manifest.get("geometry_projection") != PROTOTYPE_PROJECTION_VERSION
            or not isinstance(source_hash, str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", source_hash) is None
        ):
            return "approved prototype manifest lacks the exact renderer-mask projection provenance"
        source = self._find_lineage_artifact(
            attempt,
            ArtifactKind.PROVIDER_RESPONSE,
            content_hash=source_hash,
        )
        if source is None or source["stage"] != Stage.PROTOTYPE or not str(source["media_type"]).startswith("image/"):
            return "approved prototype raw provider source is not retained"
        if is_draft_attempt(attempt):
            selection_id = attempt.get("prototype_selection_id")
            if not selection_id or attempt.get("prototype_decision_id"):
                return "draft submission lacks an exclusive retained selection record"
            try:
                selection = self.database.get_artifact(str(selection_id))
                payload = self.persistence.load_json(selection["object_ref"])
            except (FileNotFoundError, RuntimeError, ValueError) as error:
                return f"draft selection record is unreadable: {error}"
            if (
                selection["attempt_id"] != attempt["id"]
                or selection["kind"] != ArtifactKind.PROTOTYPE_SELECTION
                or payload.get("mode") != "draft_submission"
                or payload.get("selected_artifact_sha256") != approved_hash
                or payload.get("human_approval") is not False
                or payload.get("maximum_driver_action") != "request_admin_review"
            ):
                return "draft selection record does not authorize the exact retained prototype"
            return None
        decision_id = attempt.get("prototype_decision_id")
        approval = next(
            (
                decision
                for candidate in self._lineage(attempt)
                for decision in self.database.decisions_for_attempt(candidate["id"])
                if decision["id"] == decision_id and decision["action"] == "prototype_approval"
            ),
            None,
        )
        if approval is None or approval["content_hash"] != approved_hash:
            return "prototype approval decision cannot be verified"
        return None

    async def _ideate(self, attempt: dict[str, Any]) -> dict[str, Any]:
        concept = self.database.get_concept(attempt["concept_id"])
        try:
            direction = self.pinned_direction(attempt).decode("utf-8")
        except (FileNotFoundError, RuntimeError, UnicodeDecodeError, ValueError) as error:
            return self._block_attempt(attempt, str(error))
        recent = self._ideation_context(concept["id"])
        request = {
            "direction": direction,
            "retained_concepts": recent,
            "seed": concept["seed"],
            "scoring_contract": {
                "novelty_score": "0..1 against every retained concept and its human feedback",
                "direction_score": "0..1 against repository-canonical direction",
                "scores_rank_but_never_delete": True,
            },
        }
        provider = self.providers.role("smart_text")
        operation, result = await self._provider_call(
            attempt=attempt,
            stage=Stage.CONCEPT,
            key=f"{attempt['id']}:ideate:v2",
            role="smart_text",
            side_effect="generate_concept",
            request=request,
            invoke=lambda: provider.generate_structured(
                system=(
                    "Propose one original Snaketron skin direction. It must obey the canonical "
                    "direction, differ from published and rejected retained concepts, learn from literal "
                    "human feedback without copying it, and be implementable as SkinDoc v2. Return "
                    "honest direction and novelty scores with a short comparison rationale."
                ),
                prompt=canonical_json(request),
                schema=ConceptProposal,
                temperature=0.8,
            ),
        )
        proposal = self._model_result(operation, result, ConceptProposal)
        similarity = self._concept_similarity(proposal, recent)
        rank_score = round(
            0.4 * proposal.novelty_score + 0.4 * proposal.direction_score + 0.2 * (1.0 - similarity["score"]),
            6,
        )
        selected = rank_score >= self.config.ideation.minimum_rank_score
        concept = self.database.get_concept(concept["id"])
        if attempt["purpose"] == Purpose.PRODUCTION and concept["current_attempt_id"] == attempt["id"]:
            concept = self.database.update_concept(
                concept["id"],
                concept["version"],
                name=proposal.name,
                brief=proposal.brief,
                seed=proposal.seed,
                tags_json=proposal.tags,
                source="gemini_ideation",
            )
        concept_artifact = self._store_json_artifact(
            attempt,
            Stage.CONCEPT,
            ArtifactKind.CONCEPT_BRIEF,
            {
                **proposal.model_dump(mode="json"),
                "ranking": {
                    "version": "concept-rank-v1",
                    "rank_score": rank_score,
                    "minimum_rank_score": self.config.ideation.minimum_rank_score,
                    "max_text_similarity": similarity["score"],
                    "most_similar_concept_id": similarity["concept_id"],
                    "most_similar_name": similarity["name"],
                    "selected_for_prototype": selected,
                    "context_concepts": len(recent),
                },
            },
            metadata={"operation_id": operation["id"], "rank_score": rank_score},
        )
        self.database.add_evaluation(
            artifact_id=concept_artifact["id"],
            attempt_id=attempt["id"],
            evaluator="smart_text+deterministic_similarity",
            result=GateResult(
                gate="concept_rank",
                gate_version=f"{operation['resolved_model']}+concept-rank-v1",
                blocking=False,
                verdict=GateVerdict.CANDIDATE if selected else GateVerdict.MACHINE_REJECTED,
                reasons=[
                    (
                        "ranked for prototype generation"
                        if selected
                        else "retained below the configured prototype-spend threshold"
                    ),
                    proposal.novelty_rationale,
                ],
                measurements={
                    "rank_score": rank_score,
                    "minimum_rank_score": self.config.ideation.minimum_rank_score,
                    "gemini_novelty_score": proposal.novelty_score,
                    "gemini_direction_score": proposal.direction_score,
                    "max_text_similarity": similarity["score"],
                    "most_similar_concept_id": similarity["concept_id"],
                    "context_dispositions": self._disposition_counts(recent),
                },
            ),
        )
        current = self.database.get_attempt(attempt["id"])
        if not selected:
            return self.database.update_attempt(
                current["id"],
                current["version"],
                stage=Stage.COMPLETE,
                disposition=Disposition.MACHINE_REJECTED,
                review_kind=None,
            )
        return self.database.update_attempt(current["id"], current["version"], stage=Stage.PROTOTYPE)

    def _ideation_context(self, current_concept_id: str) -> list[dict[str, Any]]:
        """Return bounded, categorized retained directions with literal feedback."""

        limit = self.config.ideation.context_limit
        rows = self.database.list_gallery("all", limit=min(500, limit * 6))
        by_concept: dict[str, dict[str, Any]] = {}
        for row in rows:
            if row["concept_id"] == current_concept_id or row["purpose"] != Purpose.PRODUCTION:
                continue
            if row["concept_id"] not in by_concept and len(by_concept) >= limit:
                continue
            item = by_concept.setdefault(
                row["concept_id"],
                {
                    "concept_id": row["concept_id"],
                    "name": row["concept_name"],
                    "brief": row["concept_brief"],
                    "tags": json.loads(row["tags_json"]),
                    "disposition": row["disposition"],
                    "human_feedback": [],
                },
            )
            # The newest Attempt supplies the current disposition, while all
            # literal feedback in its retained lineage remains useful context.
            feedback = item["human_feedback"]
            for decision in self.database.decisions_for_attempt(row["id"]):
                value = str(decision["feedback"]).strip()
                if value and value not in feedback:
                    feedback.append(value)
        return list(by_concept.values())[:limit]

    @staticmethod
    def _concept_similarity(proposal: ConceptProposal, context: list[dict[str, Any]]) -> dict[str, Any]:
        def tokens(value: str) -> set[str]:
            return {token for token in re.findall(r"[a-z0-9]+", value.lower()) if len(token) > 2}

        proposed = tokens(" ".join([proposal.name, proposal.brief, *proposal.tags]))
        best = {"score": 0.0, "concept_id": None, "name": None}
        for item in context:
            retained = tokens(" ".join([item["name"], item["brief"], *item["tags"]]))
            union = proposed | retained
            score = len(proposed & retained) / len(union) if union else 0.0
            if score > best["score"]:
                best = {
                    "score": round(score, 6),
                    "concept_id": item["concept_id"],
                    "name": item["name"],
                }
        return best

    @staticmethod
    def _disposition_counts(context: list[dict[str, Any]]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for item in context:
            key = str(item["disposition"])
            counts[key] = counts.get(key, 0) + 1
        return counts

    async def _prototype(self, attempt: dict[str, Any]) -> dict[str, Any]:
        existing = self.database.artifacts_for_attempt(
            attempt["id"], stage=Stage.PROTOTYPE, kind=ArtifactKind.PROTOTYPE
        )
        by_index: dict[int, dict[str, Any]] = {}
        for item in existing:
            index = json.loads(item["metadata_json"]).get("prototype_index")
            if not isinstance(index, int):
                return self._block_attempt(attempt, "retained prototype has no integer prototype_index")
            if index in by_index and by_index[index]["content_hash"] != item["content_hash"]:
                return self._block_attempt(attempt, f"prototype index {index} names multiple retained images")
            by_index[index] = item
        completed_indices: set[int] = set()
        prototypes_by_id = {item["id"]: item for item in existing}
        for manifest in self.database.artifacts_for_attempt(
            attempt["id"], stage=Stage.PROTOTYPE, kind=ArtifactKind.PROTOTYPE_MANIFEST
        ):
            metadata = json.loads(manifest["metadata_json"])
            index = metadata.get("prototype_index")
            image_id = metadata.get("image_artifact_id")
            if not isinstance(index, int) or image_id not in prototypes_by_id:
                return self._block_attempt(attempt, "retained prototype manifest has invalid slot linkage")
            completed_indices.add(index)
            by_index[index] = prototypes_by_id[image_id]
        concept = self.database.get_concept(attempt["concept_id"])
        provider = self.providers.role("image_generator")
        prototype_count = (
            self.config.draft_automation.candidates_per_prompt
            if is_draft_attempt(attempt)
            else self.config.budgets.prototypes_per_attempt
        )
        for index in range(prototype_count):
            if index in completed_indices:
                continue
            if index in by_index:
                # Artifact rows and their companion manifests are two durable
                # writes. A crash between them must complete the manifest from
                # the retained exact image instead of skipping the index or
                # spending on another provider call.
                if self._prototype_manifest_for(attempt, by_index[index]) is None:
                    self._store_prototype_manifest(attempt, concept, by_index[index], index)
                continue
            try:
                prompt = self._prototype_prompt(attempt, concept, index)
                geometry = self.pinned_prototype_geometry(attempt)
            except (FileNotFoundError, RuntimeError, UnicodeDecodeError, ValueError) as error:
                return self._block_attempt(attempt, str(error))
            references: list[tuple[str, bytes]] = []
            request: dict[str, Any] = {"prompt": prompt, "aspect_ratio": "16:9", "image_size": "2K"}
            if geometry is not None:
                contract, guide = geometry
                behavior = json.loads(attempt["behavior_json"])
                prompt_contract = {
                    "id": _PROTOTYPE_IMAGE_PROMPT_VERSION,
                    "design_guidelines_sha256": behavior["design_guidelines_sha"],
                    "prototype_geometry_sha256": behavior["prototype_geometry_sha"],
                    "prototype_guide_sha256": behavior["prototype_guide_sha"],
                }
                references = [("image/png", guide)]
                request["prompt_contract"] = prompt_contract
                request["references"] = [
                    {
                        "role": "strict_snake_body_geometry_guide",
                        "media_type": "image/png",
                        "contract_id": contract["id"],
                        "contract_sha256": behavior["prototype_geometry_sha"],
                        "content_hash": f"sha256:{behavior['prototype_guide_sha']}",
                        "object_ref": behavior["prototype_guide_ref"],
                    }
                ]
                request["geometry_projection"] = {
                    "version": PROTOTYPE_PROJECTION_VERSION,
                    "contract_sha256": behavior["prototype_geometry_sha"],
                    "guide_content_hash": f"sha256:{behavior['prototype_guide_sha']}",
                    "raw_provider_image_policy": "retained_audit_only",
                    "projected_image_policy": "sole_review_and_authoring_authority",
                }
            operation, result = await self._provider_call(
                attempt=self.database.get_attempt(attempt["id"]),
                stage=Stage.PROTOTYPE,
                key=f"{attempt['id']}:prototype:{index}:{_PROTOTYPE_IMAGE_PROMPT_VERSION}",
                role="image_generator",
                side_effect="generate_prototype_image",
                request=request,
                invoke=lambda prompt=prompt, references=references: provider.generate_image(
                    prompt=prompt,
                    references=references or None,
                    aspect_ratio="16:9",
                    image_size="2K",
                ),
            )
            image, media_type = self._image_result(operation, result)
            source_artifact = self._store_bytes_artifact(
                attempt,
                Stage.PROTOTYPE,
                ArtifactKind.PROVIDER_RESPONSE,
                image,
                media_type,
                metadata={
                    "prototype_index": index,
                    "prompt": prompt,
                    "provider_output_role": "prototype_material_source",
                    "review_authority": False,
                    "geometry_projection": PROTOTYPE_PROJECTION_VERSION,
                    "prototype_geometry_sha256": request.get("references", [{}])[0].get("contract_sha256"),
                    "prototype_guide_content_hash": request.get("references", [{}])[0].get("content_hash"),
                    "prompt_contract": request.get("prompt_contract"),
                },
                provenance={
                    "operation_id": operation["id"],
                    "provider_role": "image_generator",
                    "resolved_model": operation["resolved_model"],
                    "references": request.get("references", []),
                    "prompt_contract": request.get("prompt_contract"),
                },
                occurrence_key=f"prototype-source:{index}",
            )
            if geometry is None:
                return self._block_attempt(
                    attempt,
                    "current prototype generation has no pinned renderer geometry authority",
                )
            try:
                projection = project_prototype_body(
                    image,
                    contract=contract,
                    geometry_guide=guide,
                )
            except PrototypeProjectionError as error:
                self.database.add_evaluation(
                    artifact_id=source_artifact["id"],
                    attempt_id=attempt["id"],
                    evaluator="deterministic_prototype_projection",
                    result=GateResult(
                        gate="prototype_geometry_projection",
                        gate_version=PROTOTYPE_PROJECTION_VERSION,
                        blocking=True,
                        verdict=GateVerdict.MACHINE_REJECTED,
                        reasons=[str(error)],
                        measurements={
                            "prototype_index": index,
                            "source_image_sha256": source_artifact["content_hash"],
                            "projection_version": PROTOTYPE_PROJECTION_VERSION,
                        },
                    ),
                )
                continue
            stored = self.objects.put(projection.png_bytes)
            image_artifact = self.database.add_artifact(
                attempt_id=attempt["id"],
                stage=Stage.PROTOTYPE,
                kind=ArtifactKind.PROTOTYPE,
                content_hash=stored.uri,
                object_ref=stored.uri,
                media_type="image/png",
                size_bytes=stored.size,
                metadata={
                    "prototype_index": index,
                    "prompt": prompt,
                    "source_artifact_id": source_artifact["id"],
                    "source_image_sha256": source_artifact["content_hash"],
                    "source_media_type": media_type,
                    "source_bbox": list(projection.source_bbox),
                    "source_size": list(projection.source_size),
                    "geometry_projection": projection.version,
                    "prototype_geometry_sha256": request.get("references", [{}])[0].get("contract_sha256"),
                    "prototype_guide_content_hash": request.get("references", [{}])[0].get("content_hash"),
                    "prompt_contract": request.get("prompt_contract"),
                },
                provenance={
                    "operation_id": operation["id"],
                    "provider_role": "image_generator",
                    "resolved_model": operation["resolved_model"],
                    "reference_policy": "repository_owned_renderer_geometry_guide",
                    "source_artifact_id": source_artifact["id"],
                    "source_image_sha256": source_artifact["content_hash"],
                    "geometry_projection": projection.version,
                    "references": request.get("references", []),
                    "prompt_contract": request.get("prompt_contract"),
                },
            )
            self.database.add_evaluation(
                artifact_id=image_artifact["id"],
                attempt_id=attempt["id"],
                evaluator="deterministic_prototype_projection",
                result=GateResult(
                    gate="prototype_geometry_projection",
                    gate_version=projection.version,
                    blocking=True,
                    verdict=GateVerdict.CANDIDATE,
                    reasons=["raw provider material was projected through the exact renderer-owned body mask"],
                    measurements={
                        "prototype_index": index,
                        "source_image_sha256": source_artifact["content_hash"],
                        "projected_image_sha256": image_artifact["content_hash"],
                        "source_bbox": list(projection.source_bbox),
                        "source_size": list(projection.source_size),
                        "projection_version": projection.version,
                    },
                ),
            )
            # Content-addressed artifacts intentionally deduplicate identical
            # provider bytes.  A duplicate output is still a completed paid
            # slot, so retain a distinct manifest that binds the exact prompt,
            # operation and slot to the shared immutable image.
            artifact_metadata = json.loads(image_artifact["metadata_json"])
            self._store_prototype_manifest(
                attempt,
                concept,
                image_artifact,
                index,
                prompt=prompt,
                operation_id=operation["id"],
                source_image_sha256=source_artifact["content_hash"],
                geometry_projection=projection.version,
                duplicate_of_index=(
                    artifact_metadata.get("prototype_index")
                    if artifact_metadata.get("prototype_index") != index
                    else None
                ),
            )
        projected = self.database.artifacts_for_attempt(
            attempt["id"], stage=Stage.PROTOTYPE, kind=ArtifactKind.PROTOTYPE
        )
        if not projected:
            return self._reject_attempt(
                attempt,
                "no provider prototype could be deterministically projected into real snake geometry",
            )
        current = self.database.get_attempt(attempt["id"])
        return self.database.update_attempt(current["id"], current["version"], stage=Stage.PROTOTYPE_TRIAGE)

    def _store_prototype_manifest(
        self,
        attempt: dict[str, Any],
        concept: dict[str, Any],
        image_artifact: dict[str, Any],
        index: int,
        *,
        prompt: str | None = None,
        operation_id: str | None = None,
        source_image_sha256: str | None = None,
        geometry_projection: str | None = None,
        duplicate_of_index: int | None = None,
    ) -> dict[str, Any]:
        metadata = json.loads(image_artifact["metadata_json"])
        retained_prompt = metadata.get("prompt") if prompt is None else prompt
        if prompt is None and metadata.get("prototype_index") != index:
            raise RuntimeError(f"retained prototype index {index} lacks its exact generation prompt")
        if not isinstance(retained_prompt, str) or not retained_prompt:
            raise RuntimeError(f"retained prototype index {index} lacks its exact generation prompt")
        retained_source_sha = source_image_sha256 or metadata.get("source_image_sha256")
        retained_projection = geometry_projection or metadata.get("geometry_projection")
        if (
            not isinstance(retained_source_sha, str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", retained_source_sha) is None
        ):
            raise RuntimeError(f"retained prototype index {index} lacks its exact provider source hash")
        if retained_projection != PROTOTYPE_PROJECTION_VERSION:
            raise RuntimeError(f"retained prototype index {index} lacks the current geometry projection")
        behavior = json.loads(attempt["behavior_json"])
        image_config = behavior.get("models", {}).get("image_generator")
        if not isinstance(image_config, dict):
            # Legacy development attempts predate retained public model
            # snapshots. In-flight production attempts always take this value
            # from behavior_snapshot and are drift-checked before advancing.
            image_config = self.config.public_snapshot()["models"]["image_generator"]
        manifest = PrototypeManifest(
            brief=concept["brief"],
            palette_intent=self._concept_field(attempt, "palette_intent", "role-aware palette"),
            motion_intent=self._concept_field(attempt, "motion_intent", "subtle authored motion"),
            implementation_hint=self._concept_field(attempt, "implementation_hint", "layers"),
            hint_rationale=self._concept_field(
                attempt,
                "implementation_rationale",
                "The implementation route is advisory until the authoring probe.",
            ),
            prompt=retained_prompt,
            provider_config=canonical_json(image_config),
            image_sha256=image_artifact["content_hash"],
            source_image_sha256=retained_source_sha,
            geometry_projection=retained_projection,
            design_guidelines_sha256=behavior.get("design_guidelines_sha"),
            prototype_geometry_sha256=behavior.get("prototype_geometry_sha"),
            prototype_guide_sha256=behavior.get("prototype_guide_sha"),
        )
        return self._store_json_artifact(
            attempt,
            Stage.PROTOTYPE,
            ArtifactKind.PROTOTYPE_MANIFEST,
            manifest.model_dump(mode="json", by_alias=True),
            metadata={
                "prototype_index": index,
                "image_artifact_id": image_artifact["id"],
                "operation_id": operation_id,
                "duplicate_of_index": duplicate_of_index,
            },
        )

    async def _prototype_triage(self, attempt: dict[str, Any]) -> dict[str, Any]:
        only_artifact: str | None = None
        if str(attempt.get("restart_stage") or "").startswith("re_evaluate:"):
            only_artifact = str(attempt["restart_stage"]).split(":", 1)[1]
        if only_artifact:
            target = self.database.get_artifact(only_artifact)
            prototypes = [target] if target["kind"] == ArtifactKind.PROTOTYPE else []
        else:
            prototypes = self.database.artifacts_for_attempt(
                attempt["id"], stage=Stage.PROTOTYPE, kind=ArtifactKind.PROTOTYPE
            )
        if not prototypes:
            return self._reject_attempt(attempt, "no retained prototype image is available for triage")
        concept = self.database.get_concept(attempt["concept_id"])
        judgments: list[VisualJudgment] = []
        evaluated: list[tuple[dict[str, Any], GateResult]] = []
        safety_evaluated: list[tuple[dict[str, Any], GateResult]] = []
        geometry = self.pinned_prototype_geometry(attempt)
        behavior = json.loads(attempt["behavior_json"])
        geometry_comparison = (
            {
                "media_type": "image/png",
                "object_ref": behavior["prototype_guide_ref"],
                "content_hash": f"sha256:{behavior['prototype_guide_sha']}",
            }
            if geometry is not None
            else None
        )
        for artifact in prototypes:
            judgment, operation = await self._judge(
                attempt,
                artifact,
                system=PROTOTYPE_JUDGE_RUBRIC,
                context={
                    "brief": concept["brief"],
                    "prototype_contract": geometry[0] if geometry is not None else "legacy horizontal head-body-tail",
                    "image_order": (
                        ["pinned_blank_geometry_guide", "candidate_prototype"]
                        if geometry is not None
                        else ["candidate_prototype"]
                    ),
                },
                key=(
                    f"{attempt['id']}:prototype-judge:{artifact['content_hash']}:v2"
                    if geometry is not None
                    else f"{attempt['id']}:prototype-judge:{artifact['content_hash']}:v1"
                ),
                comparison=geometry_comparison,
            )
            if judgment.review_flags and judgment.verdict != "machine_rejected":
                judgment = judgment.model_copy(update={"verdict": "machine_rejected"})
            judgments.append(judgment)
            result = GateResult(
                gate="visual_fidelity",
                gate_version=judge_evaluator_version(
                    self.config,
                    "prototype",
                    resolved_model=operation["resolved_model"],
                ),
                blocking=False,
                verdict=GateVerdict(judgment.verdict),
                reasons=judgment.reasons,
                measurements={
                    **judgment.model_dump(mode="json", exclude={"verdict", "reasons"}),
                    "resolved_model": operation["resolved_model"],
                },
            )
            evaluated.append((artifact, result))
            safety_evaluated.append(
                (
                    artifact,
                    self.gates.manifest.result(
                        "safety_ip",
                        not judgment.review_flags,
                        reasons=[f"judge flagged {flag}" for flag in judgment.review_flags],
                        measurements={
                            "review_flags": judgment.review_flags,
                            "resolved_model": operation["resolved_model"],
                            "scope": "prototype_exact_pixels",
                        },
                    ),
                )
            )
        current = self.database.get_attempt(attempt["id"])
        evaluator_versions = {result.gate_version for _, result in evaluated}
        actual_evaluator = evaluator_versions.pop() if len(evaluator_versions) == 1 else "mixed-evaluator-versions"
        routing = self.calibration.routing_status("prototype", evaluator_version=actual_evaluator)
        all_rejected = all(judgment.verdict == "machine_rejected" for judgment in judgments)
        safety_failed = any(result.verdict == GateVerdict.FAIL for _, result in safety_evaluated)
        sampled = (
            not safety_failed
            and routing.enabled
            and all_rejected
            and self.calibration.should_sample_reject(attempt["id"], "prototype")
        )
        for artifact, result in evaluated:
            self.database.add_evaluation(
                artifact_id=artifact["id"],
                attempt_id=attempt["id"],
                evaluator="visual_judge",
                result=result,
                hidden_until_label=not routing.enabled or sampled,
            )
        for artifact, result in safety_evaluated:
            self.database.add_evaluation(
                artifact_id=artifact["id"],
                attempt_id=attempt["id"],
                evaluator="visual_judge_safety_ip",
                result=result,
                hidden_until_label=False,
            )
        if is_draft_attempt(attempt):
            ranked: list[tuple[float, str, dict[str, Any], VisualJudgment, GateResult]] = []
            retained_candidates: list[dict[str, Any]] = []
            for artifact, judgment, (_, safety) in zip(
                prototypes,
                judgments,
                safety_evaluated,
                strict=True,
            ):
                score = round(
                    (
                        judgment.fidelity
                        + judgment.readability
                        + judgment.role_clarity
                        + judgment.animation_quality
                        + judgment.craft
                    )
                    / 5,
                    6,
                )
                metadata = json.loads(artifact["metadata_json"] or "{}")
                eligible = (
                    safety.verdict != GateVerdict.FAIL
                    and judgment.verdict != "machine_rejected"
                    and not judgment.review_flags
                )
                retained_candidates.append(
                    {
                        "artifact_id": artifact["id"],
                        "artifact_sha256": artifact["content_hash"],
                        "prototype_index": metadata.get("prototype_index"),
                        "score": score,
                        "verdict": judgment.verdict,
                        "eligible": eligible,
                        "review_flags": judgment.review_flags,
                    }
                )
                if eligible:
                    # Descending score and then ascending immutable hash makes
                    # the same retained bytes choose the same authority.
                    ranked.append((score, artifact["content_hash"], artifact, judgment, safety))
            if not ranked:
                return self._reject_attempt(
                    current,
                    "draft automation found no safe non-rejected prototype candidate",
                )
            ranked.sort(key=lambda value: (-value[0], value[1]))
            selected_score, _, selected, selected_judgment, _ = ranked[0]
            queue = draft_attempt_metadata(attempt)
            rationale = (
                f"Deterministic safe-candidate rank selected {selected['content_hash']} "
                f"with mean game-scale score {selected_score:.6f}; "
                f"verdict={selected_judgment.verdict}."
            )
            selection = self._store_json_artifact(
                attempt,
                Stage.PROTOTYPE_TRIAGE,
                ArtifactKind.PROTOTYPE_SELECTION,
                {
                    "schema_version": 1,
                    "mode": "draft_submission",
                    "queue_id": queue["queue_id"],
                    "queue_request_sha256": queue["request_sha256"],
                    "selected_artifact_id": selected["id"],
                    "selected_artifact_sha256": selected["content_hash"],
                    "selection_rationale": rationale,
                    "ranking": "mean-v1:fidelity,readability,role_clarity,animation_quality,craft;hash-tiebreak",
                    "candidates": retained_candidates,
                    "human_approval": False,
                    "maximum_driver_action": "request_admin_review",
                },
                metadata={
                    "selected_artifact_id": selected["id"],
                    "selected_artifact_sha256": selected["content_hash"],
                },
                occurrence_key="draft-selection-v1",
            )
            current = self.database.get_attempt(attempt["id"])
            return self.database.update_attempt(
                current["id"],
                current["version"],
                approved_prototype_hash=selected["content_hash"],
                prototype_selection_id=selection["id"],
                stage=Stage.AUTHOR,
                disposition=Disposition.ACTIVE,
                review_kind=None,
            )
        routed = not safety_failed and (not routing.enabled or not all_rejected)
        disposition = Disposition.NEEDS_HUMAN if routed else Disposition.MACHINE_REJECTED
        review_kind = "prototype" if routed else ("prototype_label" if sampled else None)
        transition = {
            "stage": Stage.PROTOTYPE_REVIEW,
            "disposition": disposition,
            "review_kind": review_kind,
        }
        updated = (
            self._transition_to_review(current, **transition)
            if routed or sampled
            else self.database.update_attempt(current["id"], current["version"], **transition)
        )
        return updated

    async def _author(self, attempt: dict[str, Any]) -> dict[str, Any]:
        draft_submission = is_draft_attempt(attempt)
        media_catalog_artifact: dict[str, Any] | None = None
        if draft_submission:
            preplan_or_attempt = await self._draft_media_preplan(attempt)
            if isinstance(preplan_or_attempt, dict) and "disposition" in preplan_or_attempt:
                return preplan_or_attempt
            preplan = preplan_or_attempt
            assert isinstance(preplan, DraftMediaPreplan)
            if preplan.decision == "platform_gap":
                assert preplan.failure is not None
                return self._block_attempt(
                    self.database.get_attempt(attempt["id"]),
                    "draft media platform_gap: " + preplan.failure.reason,
                )
            media_catalog_artifact = await self._materialize_draft_media(attempt, preplan)
        if not attempt["approved_prototype_hash"]:
            return self._block_attempt(attempt, "authoring lacks an exact retained prototype selection")
        if draft_submission:
            if not attempt.get("prototype_selection_id") or attempt.get("prototype_decision_id"):
                return self._block_attempt(attempt, "draft authoring lacks an exclusive selection record")
        elif not attempt["prototype_decision_id"] or attempt.get("prototype_selection_id"):
            return self._block_attempt(attempt, "authoring lacks an exact human prototype approval")
        prototype = self._find_lineage_artifact(
            attempt, ArtifactKind.PROTOTYPE, content_hash=attempt["approved_prototype_hash"]
        )
        if prototype is None:
            return self._block_attempt(attempt, "approved prototype bytes are not retained")
        prototype_manifest_artifact = self._prototype_manifest_for(attempt, prototype)
        if prototype_manifest_artifact is None:
            return self._block_attempt(attempt, "approved prototype has no matching manifest")
        prototype_manifest = self.persistence.load_json(prototype_manifest_artifact["object_ref"])
        attempt_behavior = json.loads(attempt["behavior_json"])
        if int(attempt_behavior.get("snapshot_version", 0)) >= 6:
            required_manifest_authority = {
                "design_guidelines_sha256": attempt_behavior.get("design_guidelines_sha"),
                "prototype_geometry_sha256": attempt_behavior.get("prototype_geometry_sha"),
                "prototype_guide_sha256": attempt_behavior.get("prototype_guide_sha"),
            }
            if any(prototype_manifest.get(key) != value for key, value in required_manifest_authority.items()):
                return self._block_attempt(
                    attempt,
                    "approved prototype manifest does not bind the pinned design and geometry contract",
                )
        approval: dict[str, Any] | None = None
        selection_payload: dict[str, Any] | None = None
        selection_artifact: dict[str, Any] | None = None
        if draft_submission:
            try:
                selection_artifact = self.database.get_artifact(str(attempt["prototype_selection_id"]))
                selection_payload = self.persistence.load_json(selection_artifact["object_ref"])
            except (FileNotFoundError, RuntimeError, ValueError) as error:
                return self._block_attempt(attempt, f"draft selection record is unreadable: {error}")
            if (
                selection_artifact["attempt_id"] != attempt["id"]
                or selection_artifact["kind"] != ArtifactKind.PROTOTYPE_SELECTION
                or selection_payload.get("mode") != "draft_submission"
                or selection_payload.get("selected_artifact_sha256") != prototype["content_hash"]
                or selection_payload.get("human_approval") is not False
            ):
                return self._block_attempt(attempt, "draft selection record cannot be verified")
        else:
            approval = next(
                (
                    decision
                    for candidate in self._lineage(attempt)
                    for decision in self.database.decisions_for_attempt(candidate["id"])
                    if decision["id"] == attempt["prototype_decision_id"] and decision["action"] == "prototype_approval"
                ),
                None,
            )
            if approval is None or approval["content_hash"] != prototype["content_hash"]:
                return self._block_attempt(attempt, "prototype approval decision cannot be verified")
        try:
            direction_bytes = self.pinned_direction(attempt)
            pinned_gates = self.pinned_gates(attempt)
            guideline_bytes = self.pinned_design_guidelines(attempt)
            prototype_geometry = self.pinned_prototype_geometry(attempt)
        except (FileNotFoundError, RuntimeError, ValueError) as error:
            return self._block_attempt(attempt, str(error))
        direction_object = self.objects.put(direction_bytes)

        # Experimental attempts are self-contained: their retained candidate
        # bundle is the authority and must be verified before any lookup of the
        # checkout's currently promoted package. This also makes a trial
        # resumable after the active Git ref moves.
        if attempt["purpose"] != Purpose.PRODUCTION:
            candidate = self._find_lineage_artifact(attempt, ArtifactKind.OPTIMIZER_CANDIDATE)
            if candidate is None:
                return self._block_attempt(attempt, "experimental authoring has no retained candidate bundle")
            try:
                payload = self.persistence.load_json(candidate["object_ref"])
                files = payload["skill_files"]
                declared_sha = payload["skill_sha256"]
                if not isinstance(files, dict) or not all(
                    isinstance(name, str) and isinstance(body, str) for name, body in files.items()
                ):
                    raise ValueError("candidate skill_files must map paths to UTF-8 text")
                bundle = SkillBundle.from_files(files)
            except (FileNotFoundError, KeyError, TypeError, ValueError, RuntimeError) as error:
                return self._block_attempt(attempt, f"optimizer candidate bundle is invalid: {error}")
            if bundle.sha256 != declared_sha:
                return self._block_attempt(attempt, "optimizer candidate bundle hash differs")
        else:
            try:
                bundle = self.pinned_skill_bundle(attempt)
            except FileNotFoundError as error:
                return self._block_attempt(attempt, str(error))
        if bundle.sha256 != attempt["skill_sha"]:
            return self._block_attempt(
                attempt,
                f"pinned skill {attempt['skill_sha']} is unavailable; checkout changed to {bundle.sha256}",
            )
        if guideline_bytes and self._skill_guidelines(bundle) != guideline_bytes:
            return self._block_attempt(attempt, "pinned shared design guidelines differ from the authoring skill")
        try:
            implementation_plan_output_schema = json.loads(bundle.files["schemas/implementation-plan.schema.json"])
        except (KeyError, TypeError, json.JSONDecodeError):
            return self._block_attempt(
                attempt,
                "pinned authoring bundle predates the current implementation-plan schema; "
                "retry under the current package",
            )
        behavior = json.loads(attempt["behavior_json"])
        feedback = self._lineage_feedback(attempt)
        request_identity = canonical_json(
            {
                "attempt_id": attempt["id"],
                "approved_prototype_hash": attempt["approved_prototype_hash"],
                "skill_sha": bundle.sha256,
                "direction_sha": attempt["direction_sha"],
                "capability_sha": attempt["capability_sha"],
                "gate_sha": attempt["gate_sha"],
                "design_guidelines_sha": behavior.get("design_guidelines_sha"),
                "prototype_geometry_sha": behavior.get("prototype_geometry_sha"),
                "prototype_guide_sha": behavior.get("prototype_guide_sha"),
            }
        )
        prototype_input_name = "selected_prototype" if draft_submission else "approved_prototype"
        artifact_refs = {
            prototype_input_name: prototype["object_ref"],
            "prototype_manifest": prototype_manifest_artifact["object_ref"],
            "canonical_direction": direction_object.uri,
        }
        materialized_modifier_catalog: dict[str, Any] | None = None
        if media_catalog_artifact is not None:
            artifact_refs["materialized_modifier_catalog"] = media_catalog_artifact["object_ref"]
            materialized_modifier_catalog = self.persistence.load_json(media_catalog_artifact["object_ref"])
        inline_artifacts = {
            prototype_input_name: InlineArtifact(
                content_hash=prototype["content_hash"],
                media_type=prototype["media_type"],
                base64_data=base64.b64encode(self.objects.get(prototype["object_ref"])).decode("ascii"),
            )
        }
        geometry_input: dict[str, Any] | None = None
        if prototype_geometry is not None:
            geometry_contract, geometry_guide = prototype_geometry
            artifact_refs.update(
                {
                    "prototype_geometry": str(behavior["prototype_geometry_ref"]),
                    "prototype_geometry_guide": str(behavior["prototype_guide_ref"]),
                }
            )
            inline_artifacts["prototype_geometry_guide"] = InlineArtifact(
                content_hash=f"sha256:{behavior['prototype_guide_sha']}",
                media_type="image/png",
                base64_data=base64.b64encode(geometry_guide).decode("ascii"),
            )
            geometry_input = {
                "contract": geometry_contract,
                "contract_sha256": behavior["prototype_geometry_sha"],
                "guide_sha256": behavior["prototype_guide_sha"],
            }
        if draft_submission:
            assert selection_payload is not None and selection_artifact is not None
            prototype_authority_input = {
                "selection_record_artifact_id": selection_artifact["id"],
                "selection_record_sha256": selection_artifact["content_hash"],
                "selected_artifact_id": selection_payload["selected_artifact_id"],
                "artifact_hash": selection_payload["selected_artifact_sha256"],
                "selection_rationale": selection_payload["selection_rationale"],
                "human_approval": False,
            }
            trusted_input_authority = {
                "mode": "draft_submission",
                "artifact_sha256": prototype_manifest["image_sha256"],
                "authority_record_sha256": selection_artifact["content_hash"],
                "human_approval_decision_id": None,
                "selection_rationale": selection_payload["selection_rationale"],
                "maximum_driver_action": "request_admin_review",
            }
        else:
            assert approval is not None
            prototype_authority_input = {
                "decision_id": approval["id"],
                "artifact_id": approval["artifact_id"],
                "artifact_hash": approval["content_hash"],
                "attempt_version": approval["attempt_version"],
                "actor": approval["actor"],
            }
            trusted_input_authority = {
                "mode": "approved_prototype",
                "artifact_sha256": prototype_manifest["image_sha256"],
                "authority_record_sha256": "sha256:"
                + hashlib.sha256(canonical_json(prototype_authority_input).encode()).hexdigest(),
                "human_approval_decision_id": approval["id"],
                "selection_rationale": None,
                "maximum_driver_action": "register_private_revision",
            }
        request = WorkerRequest(
            request_id=f"worker_{hashlib.sha256(request_identity.encode()).hexdigest()}",
            attempt_id=attempt["id"],
            purpose=Purpose(attempt["purpose"]),
            skill_sha256=bundle.sha256,
            skill_files=bundle.files,
            capability_manifest=pinned_gates.capabilities,
            artifact_refs=artifact_refs,
            authoring_inputs={
                "prototype_manifest": prototype_manifest,
                ("prototype_selection" if draft_submission else "prototype_approval"): prototype_authority_input,
                "input_authority": trusted_input_authority,
                "host_capabilities": (
                    {
                        "authority_modes": [trusted_input_authority["mode"]],
                        "operations": ["bind_materialized_modifier", "forge_asset"],
                        "raster_overhang_px_max": 4,
                        "max_non_endpoint_generated_assets": 0,
                    }
                    if draft_submission
                    else {
                        "authority_modes": [trusted_input_authority["mode"]],
                        "operations": ["generate_asset"],
                        "raster_overhang_px_max": 0,
                    }
                ),
                "direction": {
                    "sha256": direction_object.uri,
                    "text": direction_bytes.decode("utf-8"),
                },
                "capabilities": {
                    "expected_sha256": attempt["capability_sha"],
                    "manifest": pinned_gates.capabilities,
                },
                "gates": {"expected_sha256": attempt["gate_sha"]},
                "design_guidelines": {
                    "sha256": behavior.get("design_guidelines_sha"),
                    "text": guideline_bytes.decode("utf-8") if guideline_bytes else None,
                },
                "prototype_geometry": geometry_input,
                "materialized_modifier_catalog": materialized_modifier_catalog,
            },
            inline_artifacts=inline_artifacts,
            pure_tools=["color_math", "schema_lookup"],
            budget={
                "max_layers": pinned_gates.capabilities["limits"]["max_flattened_layers"],
                "max_texture_refs": pinned_gates.capabilities["limits"]["max_texture_refs"],
            },
            output_schemas={
                "implementation_plan": implementation_plan_output_schema,
                "worker_result": current_worker_result_json_schema(),
            },
            feedback=feedback,
        )
        operation, result = await self._provider_call(
            attempt=attempt,
            stage=Stage.AUTHOR,
            key=f"{attempt['id']}:author:{attempt['skill_sha']}",
            role="task_worker",
            side_effect="task_worker_rollout",
            request=request,
            invoke=lambda: self.worker.execute(request),
        )
        worker_result = self._model_result(operation, result, WorkerResult)
        try:
            validate_worker_handoff(
                worker_result,
                bundle.files,
                pinned_gates.capabilities,
                trusted_authority=request.authoring_inputs["input_authority"],
                materialized_modifier_catalog=materialized_modifier_catalog,
                allow_direct_generation=not draft_submission,
            )
        except WorkerContractError as error:
            return self._reject_attempt(
                self.database.get_attempt(attempt["id"]),
                f"task worker violated the pinned authoring contract: {error}",
            )
        self._store_json_artifact(
            attempt,
            Stage.AUTHOR,
            ArtifactKind.IMPLEMENTATION_PLAN,
            worker_result.implementation_plan.model_dump(mode="json"),
            metadata={"operation_id": operation["id"]},
        )
        self._store_json_artifact(
            attempt,
            Stage.AUTHOR,
            ArtifactKind.SKIN_DOCUMENT,
            worker_result.skin_document,
            metadata={"phase": "worker_draft", "operation_id": operation["id"]},
        )
        self._store_json_artifact(
            attempt,
            Stage.AUTHOR,
            ArtifactKind.WORKER_TRACE,
            {
                "trace": worker_result.trace,
                "usage": worker_result.usage,
                "tool_requests": [item.model_dump(mode="json") for item in worker_result.tool_requests],
            },
            metadata={
                "operation_id": operation["id"],
                "novelty_candidate": any(
                    isinstance(item, dict)
                    and (item.get("novelty_candidate") is True or item.get("technique_candidate") is True)
                    for item in worker_result.trace
                ),
            },
        )
        current = self.database.get_attempt(attempt["id"])
        next_stage = Stage.ASSETS if worker_result.implementation_plan.asset_plan else Stage.BUILD_GATE
        return self.database.update_attempt(current["id"], current["version"], stage=next_stage)

    async def _draft_media_preplan(
        self,
        attempt: dict[str, Any],
    ) -> DraftMediaPreplan | dict[str, Any]:
        retained = self._find_lineage_artifact(attempt, ArtifactKind.DRAFT_MEDIA_PREPLAN)
        if retained is not None:
            try:
                return DraftMediaPreplan.model_validate(self.persistence.load_json(retained["object_ref"]))
            except (FileNotFoundError, RuntimeError, ValueError) as error:
                return self._block_attempt(attempt, f"retained draft media preplan is invalid: {error}")
        try:
            bundle = self.pinned_draft_automation_bundle(attempt)
            schema = json.loads(bundle.files["schemas/draft-media-preplan.schema.json"])
            guideline_bytes = self.pinned_design_guidelines(attempt)
            pinned_gates = self.pinned_gates(attempt)
        except (FileNotFoundError, KeyError, RuntimeError, ValueError, json.JSONDecodeError) as error:
            return self._block_attempt(attempt, f"draft media preplanning authority is unavailable: {error}")
        prototype = self._find_lineage_artifact(
            attempt,
            ArtifactKind.PROTOTYPE,
            content_hash=attempt.get("approved_prototype_hash"),
        )
        concept_artifact = self._find_lineage_artifact(attempt, ArtifactKind.CONCEPT_BRIEF)
        if prototype is None or concept_artifact is None:
            return self._block_attempt(attempt, "draft media preplanning lacks retained prototype/concept bytes")
        concept = self.persistence.load_json(concept_artifact["object_ref"])
        behavior = json.loads(attempt["behavior_json"])
        identity = canonical_json(
            {
                "attempt_id": attempt["id"],
                "automation_skill_sha": bundle.sha256,
                "prototype": prototype["content_hash"],
                "concept": concept_artifact["content_hash"],
                "capability": attempt["capability_sha"],
            }
        )
        request = WorkerRequest(
            request_id=f"draft_media_{hashlib.sha256(identity.encode()).hexdigest()}",
            attempt_id=attempt["id"],
            purpose=Purpose.PRODUCTION,
            skill_sha256=bundle.sha256,
            skill_files=bundle.files,
            capability_manifest=pinned_gates.capabilities,
            artifact_refs={
                "selected_prototype": prototype["object_ref"],
                "concept_brief": concept_artifact["object_ref"],
            },
            authoring_inputs={
                "task": "draft_media_preplan",
                "concept": concept,
                "design_guidelines": {
                    "sha256": behavior.get("design_guidelines_sha"),
                    "text": guideline_bytes.decode("utf-8"),
                },
                "host_capabilities": {
                    "operations": [
                        "generate_endpoint_image",
                        "fal_transition_submit",
                        "fal_transition_result",
                        "deterministic_video_frame_extraction",
                        "forge_asset",
                    ],
                    "fal": {
                        **self.fal_media.capability_manifest(),
                        "duration_seconds": self.config.draft_automation.fal_transition_duration_seconds,
                        "resolution": self.config.draft_automation.fal_transition_resolution,
                        "aspect_ratio": self.config.draft_automation.fal_transition_aspect_ratio,
                    },
                    "max_video_intents": self.config.draft_automation.max_video_intents,
                    "logical_cell_px": 16,
                    "max_body_columns": 63,
                    "raster_overhang_px_max": 4,
                    "endpoint_images_per_video_intent": 2,
                    "max_non_endpoint_generated_assets": 0,
                },
            },
            inline_artifacts={
                "selected_prototype": InlineArtifact(
                    content_hash=prototype["content_hash"],
                    media_type=prototype["media_type"],
                    base64_data=base64.b64encode(self.objects.get(prototype["object_ref"])).decode("ascii"),
                )
            },
            pure_tools=["color_math", "schema_lookup"],
            budget={
                "max_video_intents": self.config.draft_automation.max_video_intents,
                "max_frame_rows": int(pinned_gates.capabilities["limits"]["max_sprite_frame_rows"]),
            },
            output_schemas={"worker_result": schema, "draft_media_preplan": schema},
        )
        execute_typed = getattr(self.worker, "execute_typed", None)
        if execute_typed is None:
            return self._block_attempt(attempt, "task worker lacks typed draft media preplanning support")
        operation, result = await self._provider_call(
            attempt=attempt,
            stage=Stage.AUTHOR,
            key=f"{attempt['id']}:draft-media-preplan:{bundle.sha256}",
            role="task_worker",
            side_effect="draft_media_preplan",
            request=request,
            invoke=lambda: execute_typed(
                request,
                DraftMediaPreplan,
                system=(
                    "Plan private Snaketron draft media only. Follow the supplied automate-skin-drafts "
                    "bundle exactly. You have no tools or side effects and must not invent hashes or "
                    "claim completed provider work. Return only the requested JSON schema."
                ),
            ),
        )
        preplan = self._model_result(operation, result, DraftMediaPreplan)
        if len(preplan.video_intents) > self.config.draft_automation.max_video_intents:
            return self._block_attempt(
                self.database.get_attempt(attempt["id"]),
                "draft media preplan exceeds the advertised video-intent cap",
            )
        self._store_json_artifact(
            attempt,
            Stage.AUTHOR,
            ArtifactKind.DRAFT_MEDIA_PREPLAN,
            preplan.model_dump(mode="json"),
            metadata={"operation_id": operation["id"], "automation_skill_sha": bundle.sha256},
            occurrence_key="draft-media-preplan-v1",
        )
        return preplan

    async def _materialize_draft_media(
        self,
        attempt: dict[str, Any],
        preplan: DraftMediaPreplan,
    ) -> dict[str, Any]:
        retained = self._find_lineage_artifact(attempt, ArtifactKind.MEDIA_CATALOG)
        if retained is not None:
            return retained
        if not preplan.video_intents:
            return self._store_json_artifact(
                attempt,
                Stage.AUTHOR,
                ArtifactKind.MEDIA_CATALOG,
                {"schema_version": 1, "modifiers": []},
                occurrence_key="draft-media-catalog-v1",
            )
        materialized: list[dict[str, Any]] = []
        for intent in preplan.video_intents:
            start = await self._draft_media_endpoint(attempt, intent, endpoint="start")
            end = await self._draft_media_endpoint(attempt, intent, endpoint="end")
            video = await self._draft_media_video(attempt, intent, start=start, end=end)
            materialized.append(
                await self._draft_media_frame_sheet(
                    attempt,
                    intent,
                    start=start,
                    end=end,
                    video=video,
                )
            )
        return self._store_json_artifact(
            attempt,
            Stage.AUTHOR,
            ArtifactKind.MEDIA_CATALOG,
            {"schema_version": 1, "modifiers": materialized},
            provenance={
                "preplan_sha256": "sha256:"
                + hashlib.sha256(canonical_json(preplan.model_dump(mode="json")).encode()).hexdigest()
            },
            occurrence_key="draft-media-catalog-v1",
        )

    async def _draft_media_endpoint(
        self,
        attempt: dict[str, Any],
        intent: DraftVideoIntent,
        *,
        endpoint: Literal["start", "end"],
    ) -> dict[str, Any]:
        retained = self._find_current_artifact(
            attempt["id"],
            ArtifactKind.MEDIA_ENDPOINT,
            metadata_match={"intent_id": intent.intent_id, "endpoint": endpoint},
        )
        if retained is not None:
            self._verify_draft_media_endpoint_evidence(
                attempt,
                intent,
                endpoint=endpoint,
                endpoint_artifact=retained,
            )
            return retained
        prototype = self._find_lineage_artifact(
            attempt,
            ArtifactKind.PROTOTYPE,
            content_hash=attempt.get("approved_prototype_hash"),
        )
        if prototype is None:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "draft media endpoint generation lacks selected prototype bytes",
                halt_generation=True,
            )
        base_prompt = intent.start_frame_prompt if endpoint == "start" else intent.end_frame_prompt
        extraction_config = VideoFrameExtractionConfig(
            ffmpeg_path=self.config.draft_automation.ffmpeg_path,
            ffprobe_path=self.config.draft_automation.ffprobe_path,
            total_timeout_seconds=self.config.draft_automation.video_extraction_timeout_seconds,
        )
        geometry = self._draft_media_endpoint_geometry(
            intent,
            source_apron_px=32,
            output_apron_px=1,
            max_aspect_scale=extraction_config.max_source_aspect_scale,
            max_bbox_area_fraction=extraction_config.max_source_bbox_area_fraction,
            max_upscale=extraction_config.max_source_upscale,
        )
        geometry_guide = self._draft_media_endpoint_geometry_guide(intent)
        geometry_guide_object = self.objects.put(geometry_guide)
        prompt = f"""Create the exact {endpoint} endpoint for one animated Snaketron modifier.
Use the first supplied image only as the visual-style reference. Use the second
supplied blank white snake as the mandatory geometry reference.
Render exactly one {intent.component_key} component, with a static orthographic
camera, inside a 3:2 reserved empty arena. The arena background is a single
flat matte RGB(127,127,127). Keep at least 32 pixels of matte around every
visible object. No text, labels, shadows, scenery, gradients, extra objects,
border, or transparency checkerboard.
Geometry is mandatory:
- Draw exactly {intent.body_columns} consecutive square 16x16 logical cells in one straight horizontal row.
- The cell count is structural: make one continuous thin round snake body with no gaps or disconnected square chunks.
- The logical body core is exactly {geometry["body_core_width_px"]}x16 pixels. The retained native row is
  exactly {geometry["stored_native_width_px"]}x{geometry["stored_native_row_height_px"]} pixels, including at most
  {intent.raster_overhang_px} pixels of authored bleed above and below the 16-pixel round body.
- Target a visible foreground bounding-box aspect of {geometry["visible_bbox_aspect_target"]:.6f}.
  The deterministic gate rejects aspects below {geometry["visible_bbox_aspect_min"]:.6f} or above
  {geometry["visible_bbox_aspect_max"]:.6f}, and rejects a foreground bbox above
  {geometry["max_source_bbox_area_fraction"]:.6f} of the arena area.
- The source object may require at most {geometry["max_source_upscale"]:.6f}x enlargement to fit
  the retained native row; do not draw a tiny icon, dot, line, or distant subject.
- Use no perspective, foreshortening, zoom, or camera padding inside the object. The only empty
  framing is the external flat matte arena. Do not crop the first or last cell.
Endpoint direction:
{base_prompt}
Return one image only."""
        provider = self.providers.role("image_generator")
        request = {
            "schema_version": 1,
            "operation": "generate_draft_media_endpoint",
            "intent_id": intent.intent_id,
            "endpoint": endpoint,
            "prompt": prompt,
            "prototype_sha256": prototype["content_hash"],
            "geometry_guide_sha256": geometry_guide_object.uri,
            "references": [
                {
                    "role": "visual_style",
                    "content_ref": prototype["content_hash"],
                    "media_type": prototype["media_type"],
                },
                {
                    "role": "blank_native_geometry",
                    "content_ref": geometry_guide_object.uri,
                    "media_type": "image/png",
                },
            ],
            "geometry": geometry,
            "arena": {"width_px": 1080, "height_px": 720, "matte_rgb": [127, 127, 127]},
            "aspect_ratio": "3:2",
            "image_size": "2K",
        }
        operation, result = await self._provider_call(
            attempt=self.database.get_attempt(attempt["id"]),
            stage=Stage.AUTHOR,
            key=f"{attempt['id']}:draft-media:{intent.intent_id}:endpoint:{endpoint}",
            role="image_generator",
            side_effect="generate_draft_media_endpoint_image",
            request=request,
            invoke=lambda: provider.generate_image(
                prompt=prompt,
                references=[
                    (prototype["media_type"], self.objects.get(prototype["object_ref"])),
                    ("image/png", geometry_guide),
                ],
                aspect_ratio="3:2",
                image_size="2K",
            ),
        )
        raw, media_type = self._image_result(operation, result)
        try:
            with Image.open(io.BytesIO(raw)) as opened:
                opened.load()
                source = opened.convert("RGB")
            decoded = io.BytesIO()
            # Preserve the provider's complete decoded pixel field. No padding,
            # resize, crop, or synthetic apron may occur before segmentation.
            source.save(decoded, format="PNG", optimize=False, compress_level=9)
            provider_pixels = decoded.getvalue()
            source.close()
        except (OSError, ValueError) as error:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                f"draft media {endpoint} endpoint cannot be decoded exactly: {error}",
                request_id=operation.get("provider_request_id"),
                resolved_model=operation.get("resolved_model"),
            ) from error
        provider_artifact = self._store_bytes_artifact(
            attempt,
            Stage.AUTHOR,
            ArtifactKind.MEDIA_ENDPOINT_PROVIDER_OUTPUT,
            provider_pixels,
            "image/png",
            metadata={
                "intent_id": intent.intent_id,
                "endpoint": endpoint,
                "operation_id": operation["id"],
                "provider_media_type": media_type,
                "normalization": "decoded_pixels_only_no_crop_resize_or_padding",
            },
            provenance={"provider_result_hash": operation["result_hash"]},
            occurrence_key=f"draft-media:{intent.intent_id}:endpoint:{endpoint}:provider-pixels",
        )
        request = MatteEndpointRequest(
            frame_sha256=provider_artifact["content_hash"],
            body_columns=intent.body_columns,
            texels_per_cell=16,
            raster_overhang_px=intent.raster_overhang_px,
            matte_rgb=tuple(intent.matte_rgb),
            source_apron_px=32,
            output_apron_px=1,
        )
        try:
            validated = await asyncio.to_thread(
                validate_matte_endpoint,
                provider_pixels,
                request=request,
                config=extraction_config,
                label=f"{intent.intent_id} {endpoint} endpoint",
            )
        except VideoFrameExtractionError as error:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                f"draft media {endpoint} endpoint failed pre-spend matte validation ({error.code}): {error}",
            ) from error
        source = self._store_bytes_artifact(
            attempt,
            Stage.AUTHOR,
            ArtifactKind.MEDIA_ENDPOINT_SOURCE_RGBA,
            validated.source_rgba_png,
            "image/png",
            metadata={
                "intent_id": intent.intent_id,
                "endpoint": endpoint,
                "provider_pixels_sha256": provider_artifact["content_hash"],
            },
            provenance={"matte_validation_report_sha256": validated.report["report_sha256"]},
            occurrence_key=f"draft-media:{intent.intent_id}:endpoint:{endpoint}:source-rgba",
        )
        native = self._store_bytes_artifact(
            attempt,
            Stage.AUTHOR,
            ArtifactKind.MEDIA_ENDPOINT_NATIVE_RGBA,
            validated.native_rgba_png,
            "image/png",
            metadata={
                "intent_id": intent.intent_id,
                "endpoint": endpoint,
                "provider_pixels_sha256": provider_artifact["content_hash"],
                "body_columns": intent.body_columns,
                "texels_per_cell": 16,
                "raster_overhang_px": intent.raster_overhang_px,
            },
            provenance={"matte_validation_report_sha256": validated.report["report_sha256"]},
            occurrence_key=f"draft-media:{intent.intent_id}:endpoint:{endpoint}:native-rgba",
        )
        if (
            validated.report.get("source", {}).get("source_rgba_png_sha256") != source["content_hash"]
            or validated.report.get("native", {}).get("native_rgba_png_sha256") != native["content_hash"]
        ):
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                f"draft media {endpoint} endpoint validator returned inconsistent evidence",
                halt_generation=True,
            )
        self._store_json_artifact(
            attempt,
            Stage.AUTHOR,
            ArtifactKind.MEDIA_ENDPOINT_VALIDATION_REPORT,
            validated.report,
            metadata={
                "intent_id": intent.intent_id,
                "endpoint": endpoint,
                "provider_pixels_sha256": provider_artifact["content_hash"],
                "source_rgba_sha256": source["content_hash"],
                "native_rgba_sha256": native["content_hash"],
            },
            occurrence_key=f"draft-media:{intent.intent_id}:endpoint:{endpoint}:validation-report",
        )
        try:
            endpoint_bytes = self._compose_draft_media_endpoint(
                validated.native_rgba_png,
                tuple(intent.matte_rgb),
            )
        except (OSError, ValueError) as error:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                f"draft media {endpoint} native object cannot be composed exactly: {error}",
                halt_generation=True,
            ) from error
        retained = self._store_bytes_artifact(
            attempt,
            Stage.AUTHOR,
            ArtifactKind.MEDIA_ENDPOINT,
            endpoint_bytes,
            "image/png",
            metadata={
                "intent_id": intent.intent_id,
                "endpoint": endpoint,
                "operation_id": operation["id"],
                "provider_media_type": media_type,
                "provider_pixels_sha256": provider_artifact["content_hash"],
                "source_rgba_sha256": source["content_hash"],
                "native_rgba_sha256": native["content_hash"],
                "validation_report_sha256": validated.report["report_sha256"],
                "width_px": 1080,
                "height_px": 720,
                "matte_rgb": list(intent.matte_rgb),
                "normalization": "validated_native_rgba_centered_without_resampling",
            },
            provenance={
                "provider_result_hash": operation["result_hash"],
                "provider_pixels_sha256": provider_artifact["content_hash"],
            },
            occurrence_key=f"draft-media:{intent.intent_id}:endpoint:{endpoint}",
        )
        self._verify_draft_media_endpoint_evidence(
            attempt,
            intent,
            endpoint=endpoint,
            endpoint_artifact=retained,
        )
        return retained

    def _verify_draft_media_endpoint_evidence(
        self,
        attempt: dict[str, Any],
        intent: DraftVideoIntent,
        *,
        endpoint: Literal["start", "end"],
        endpoint_artifact: dict[str, Any],
    ) -> None:
        """Verify retained raw segmentation evidence before a paid Fal submit."""

        match = {"intent_id": intent.intent_id, "endpoint": endpoint}
        report_artifact = self._find_current_artifact(
            attempt["id"], ArtifactKind.MEDIA_ENDPOINT_VALIDATION_REPORT, metadata_match=match
        )
        provider_artifact = self._find_current_artifact(
            attempt["id"], ArtifactKind.MEDIA_ENDPOINT_PROVIDER_OUTPUT, metadata_match=match
        )
        source = self._find_current_artifact(
            attempt["id"], ArtifactKind.MEDIA_ENDPOINT_SOURCE_RGBA, metadata_match=match
        )
        native = self._find_current_artifact(
            attempt["id"], ArtifactKind.MEDIA_ENDPOINT_NATIVE_RGBA, metadata_match=match
        )
        if any(item is None for item in (report_artifact, provider_artifact, source, native)):
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                f"retained {endpoint} endpoint lacks raw pre-spend segmentation evidence",
                halt_generation=True,
            )
        assert report_artifact is not None
        assert provider_artifact is not None
        assert source is not None
        assert native is not None
        report = self.persistence.load_json(report_artifact["object_ref"])
        metadata = json.loads(endpoint_artifact["metadata_json"])
        if (
            report.get("source", {}).get("input_png_sha256") != provider_artifact["content_hash"]
            or report.get("source", {}).get("source_rgba_png_sha256") != source["content_hash"]
            or report.get("native", {}).get("native_rgba_png_sha256") != native["content_hash"]
            or metadata.get("provider_pixels_sha256") != provider_artifact["content_hash"]
            or metadata.get("source_rgba_sha256") != source["content_hash"]
            or metadata.get("native_rgba_sha256") != native["content_hash"]
            or metadata.get("validation_report_sha256") != report.get("report_sha256")
        ):
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                f"retained {endpoint} endpoint segmentation evidence is not hash-bound",
                halt_generation=True,
            )
        try:
            exact_endpoint = self._compose_draft_media_endpoint(
                self.objects.get(native["object_ref"]),
                tuple(intent.matte_rgb),
            )
        except (OSError, ValueError) as error:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                f"retained {endpoint} native endpoint evidence cannot be recomposed: {error}",
                halt_generation=True,
            ) from error
        if (
            f"sha256:{hashlib.sha256(exact_endpoint).hexdigest()}" != endpoint_artifact["content_hash"]
            or self.objects.get(endpoint_artifact["object_ref"]) != exact_endpoint
        ):
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                f"retained {endpoint} Fal arena differs from exact validated native pixels",
                halt_generation=True,
            )

    @staticmethod
    def _draft_media_endpoint_geometry(
        intent: DraftVideoIntent,
        *,
        source_apron_px: int,
        output_apron_px: int,
        max_aspect_scale: float,
        max_bbox_area_fraction: float,
        max_upscale: float,
    ) -> dict[str, int | float | bool]:
        body_width = intent.body_columns * 16
        row_height = 16 + 2 * intent.raster_overhang_px
        drawable_width = body_width - 2 * output_apron_px
        drawable_height = row_height - 2 * output_apron_px
        aspect = drawable_width / drawable_height
        return {
            "logical_body_cells": intent.body_columns,
            "logical_cell_width_px": 16,
            "logical_cell_height_px": 16,
            "body_core_width_px": body_width,
            "body_core_height_px": 16,
            "stored_native_width_px": body_width,
            "stored_native_row_height_px": row_height,
            "raster_overhang_px_each_transverse_side": intent.raster_overhang_px,
            "native_output_apron_px": output_apron_px,
            "native_drawable_width_px": drawable_width,
            "native_drawable_height_px": drawable_height,
            "visible_bbox_aspect_target": round(aspect, 6),
            "visible_bbox_aspect_min": round(aspect / max_aspect_scale, 6),
            "visible_bbox_aspect_max": round(aspect * max_aspect_scale, 6),
            "max_source_bbox_area_fraction": max_bbox_area_fraction,
            "max_source_upscale": max_upscale,
            "source_matte_apron_px": source_apron_px,
            "straight_horizontal_row": True,
            "no_internal_camera_padding": True,
        }

    @staticmethod
    def _draft_media_endpoint_geometry_guide(intent: DraftVideoIntent) -> bytes:
        """Return an exact blank snake silhouette for the endpoint image model."""

        width = intent.body_columns * 16
        height = 16
        left = (1080 - width) // 2
        top = (720 - height) // 2
        guide = Image.new("RGB", (1080, 720), tuple(intent.matte_rgb))
        ImageDraw.Draw(guide).rounded_rectangle(
            (left, top, left + width - 1, top + height - 1),
            radius=height // 2,
            fill=(255, 255, 255),
        )
        output = io.BytesIO()
        guide.save(output, format="PNG", optimize=False, compress_level=9)
        guide.close()
        return output.getvalue()

    @staticmethod
    def _compose_draft_media_endpoint(
        native_rgba_png: bytes,
        matte_rgb: tuple[int, int, int],
    ) -> bytes:
        with Image.open(io.BytesIO(native_rgba_png)) as opened:
            opened.load()
            native_image = opened.convert("RGBA")
        if native_image.width > 1016 or native_image.height > 656:
            native_image.close()
            raise ValueError("native endpoint object cannot fit the 32px Fal arena apron")
        arena = Image.new("RGBA", (1080, 720), (*matte_rgb, 255))
        arena.alpha_composite(
            native_image,
            ((1080 - native_image.width) // 2, (720 - native_image.height) // 2),
        )
        output = io.BytesIO()
        arena.convert("RGB").save(output, format="PNG", optimize=False, compress_level=9)
        native_image.close()
        arena.close()
        return output.getvalue()

    async def _draft_media_video(
        self,
        attempt: dict[str, Any],
        intent: DraftVideoIntent,
        *,
        start: dict[str, Any],
        end: dict[str, Any],
    ) -> dict[str, Any]:
        retained = self._find_current_artifact(
            attempt["id"],
            ArtifactKind.MEDIA_VIDEO,
            metadata_match={"intent_id": intent.intent_id},
        )
        if retained is not None:
            return retained
        # These lookups are a second fail-closed boundary immediately before
        # the paid submit. Endpoint generation alone never authorizes Fal.
        for endpoint_name, endpoint_artifact in (("start", start), ("end", end)):
            self._verify_draft_media_endpoint_evidence(
                attempt,
                intent,
                endpoint=endpoint_name,
                endpoint_artifact=endpoint_artifact,
            )
        period = float(intent.common_period_ms)
        fps = float(intent.desired_fps)
        derived_rows = max(2, math.ceil(period * fps / 1_000))
        row_texels = 16 + 2 * intent.raster_overhang_px
        effective_cap = min(
            120,
            2_048 // row_texels,
            16_777_216 // (intent.body_columns * 16 * row_texels * 4),
        )
        if derived_rows > effective_cap:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                f"draft media intent {intent.intent_id} needs {derived_rows} rows; exact cap is {effective_cap}",
                halt_generation=True,
            )
        start_bytes = self.objects.get(start["object_ref"])
        end_bytes = self.objects.get(end["object_ref"])
        transition_prompt = self._draft_fal_transition_prompt(intent)
        media_request = {
            "schema_version": 1,
            "request_id": f"{attempt['id']}_{intent.intent_id}".replace("-", "_")[:200],
            "operation": "generate_video",
            "capability_id": self.config.draft_automation.fal_transition_capability_id,
            "input_artifacts": [start["content_hash"], end["content_hash"]],
            "logical_key": intent.logical_key,
            "component_key": intent.component_key,
            "prompt": transition_prompt,
            "model_transition_prompt_sha256": "sha256:"
            + hashlib.sha256(intent.transition_prompt.encode("utf-8")).hexdigest(),
            "output_kind": "video",
            "journal": {
                "retain_inputs": True,
                "retain_provider_output": True,
                "retain_output": True,
                "retain_reports": True,
            },
            "extraction": None,
            "video": {
                "start_frame_sha256": start["content_hash"],
                "end_frame_sha256": end["content_hash"],
                "source_video_sha256": None,
                "common_period_ms": period,
                "desired_fps": fps,
                "derived_frame_rows": derived_rows,
                "body_columns": intent.body_columns,
                "texels_per_cell": 16,
                "raster_overhang_px": intent.raster_overhang_px,
                "row_texels": row_texels,
                "effective_frame_row_cap": effective_cap,
                "frame_extraction": "deterministic_uniform_full_period",
                "row_zero": "resting_and_reduced_motion",
                "alpha_matte_verification": "fail_closed",
                "loop_closure": "true_final_to_zero",
                "max_frame_rows": 120,
            },
            "reuse": None,
        }
        options = PixVerseTransitionOptions(
            seed=int(intent.seed),
            reservation_micros=self.config.draft_automation.fal_transition_reservation_micros(),
            duration_seconds=self.config.draft_automation.fal_transition_duration_seconds,
            resolution=self.config.draft_automation.fal_transition_resolution,
            aspect_ratio=self.config.draft_automation.fal_transition_aspect_ratio,
        )
        submit_request = self.fal_media.submit_journal_request(
            media_request,
            start_frame=start_bytes,
            start_media_type="image/png",
            end_frame=end_bytes,
            end_media_type="image/png",
            options=options,
        )
        submit_operation, submit_result = await self._provider_call(
            attempt=self.database.get_attempt(attempt["id"]),
            stage=Stage.AUTHOR,
            key=f"{attempt['id']}:draft-media:{intent.intent_id}:fal-submit",
            role="fal_pixverse_transition",
            side_effect="fal_transition_submit",
            request=submit_request,
            invoke=lambda: self.fal_media.submit_transition(
                media_request,
                start_frame=start_bytes,
                start_media_type="image/png",
                end_frame=end_bytes,
                end_media_type="image/png",
                options=options,
            ),
        )
        ticket = FalQueueTicket.from_value(self._json_result(submit_operation, submit_result))
        poll_operation, poll_result = await self._draft_fal_transition_result(
            self.database.get_attempt(attempt["id"]),
            intent_id=intent.intent_id,
            ticket=ticket,
            submit_operation=submit_operation,
        )
        video_bytes = (
            poll_result.value if poll_result is not None else self.objects.get(str(poll_operation["result_hash"]))
        )
        if not isinstance(video_bytes, bytes):
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "Fal result operation did not retain exact MP4 bytes",
                request_id=ticket.request_id,
                resolved_model=PIXVERSE_TRANSITION_CAPABILITY,
            )
        return self._store_bytes_artifact(
            attempt,
            Stage.AUTHOR,
            ArtifactKind.MEDIA_VIDEO,
            video_bytes,
            "video/mp4",
            metadata={
                "intent_id": intent.intent_id,
                "submit_operation_id": submit_operation["id"],
                "poll_operation_id": poll_operation["id"],
                "provider_request_id": ticket.request_id,
                "media_request": media_request,
            },
            provenance={
                "start_frame_sha256": start["content_hash"],
                "end_frame_sha256": end["content_hash"],
            },
            occurrence_key=f"draft-media:{intent.intent_id}:video",
        )

    async def _draft_fal_transition_result(
        self,
        attempt: dict[str, Any],
        *,
        intent_id: str,
        ticket: FalQueueTicket,
        submit_operation: dict[str, Any],
    ) -> tuple[dict[str, Any], ProviderResult | None]:
        """Perform at most one scheduled GET/read for a retained paid ticket."""

        poll_request = self.fal_media.poll_journal_request(ticket)
        expected_hash = self.journal.request_hash(poll_request)
        prefix = f"{attempt['id']}:draft-media:{intent_id}:fal-result:{ticket.request_id}:read:"
        with self.database.connect() as connection:
            rows = [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM operation WHERE attempt_id=? AND side_effect=? ORDER BY created_at, id",
                    (attempt["id"], "fal_transition_result"),
                ).fetchall()
                if str(row["idempotency_key"]).startswith(prefix)
            ]
        for row in rows:
            if row["request_hash"] != expected_hash:
                raise ProviderError(
                    ProviderFailureKind.INVALID_OUTPUT,
                    "retained Fal result-read key names a changed ticket request",
                    request_id=ticket.request_id,
                    resolved_model=PIXVERSE_TRANSITION_CAPABILITY,
                    halt_generation=True,
                )
        succeeded = next(
            (row for row in reversed(rows) if row["status"] == OperationStatus.SUCCEEDED),
            None,
        )
        if succeeded is not None:
            return await self._provider_call(
                attempt=attempt,
                stage=Stage.AUTHOR,
                key=succeeded["idempotency_key"],
                role="fal_pixverse_transition",
                side_effect="fal_transition_result",
                request=poll_request,
                invoke=lambda: self.fal_media.poll_transition(ticket),
                validate_result_extra=validate_pixverse_video_result,
                provider_retries_override=0,
            )
        failed_terminal = next(
            (
                row
                for row in reversed(rows)
                if row["status"] in {OperationStatus.FAILED_TERMINAL, OperationStatus.RECONCILIATION_REQUIRED}
            ),
            None,
        )
        if failed_terminal is not None:
            raise ExistingOperation(
                f"Fal result read {failed_terminal['id']} is terminal; inspect its retained evidence"
            )
        try:
            submitted_at = datetime.fromisoformat(str(submit_operation["created_at"]).replace("Z", "+00:00"))
            if submitted_at.tzinfo is None:
                submitted_at = submitted_at.replace(tzinfo=UTC)
        except (TypeError, ValueError) as error:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "retained Fal submit operation has no valid ticket timestamp",
                request_id=ticket.request_id,
                resolved_model=PIXVERSE_TRANSITION_CAPABILITY,
                halt_generation=True,
            ) from error
        age_seconds = (datetime.now(UTC) - submitted_at.astimezone(UTC)).total_seconds()
        maximum_age = self.config.draft_automation.fal_transition_ticket_max_age_seconds
        if age_seconds > maximum_age:
            raise ProviderError(
                ProviderFailureKind.UNAVAILABLE,
                "Fal queue ticket expired before a validated result was retained; "
                f"request_id={ticket.request_id}; age={int(age_seconds)}s; max={maximum_age}s; "
                "operator must reconcile the retained ticket and may not resubmit blindly",
                request_id=ticket.request_id,
                resolved_model=PIXVERSE_TRANSITION_CAPABILITY,
                halt_generation=True,
            )
        resumable = next(
            (row for row in reversed(rows) if row["status"] in {OperationStatus.INTENT, OperationStatus.RUNNING}),
            None,
        )
        if resumable is not None:
            key = str(resumable["idempotency_key"])
        else:
            indexes: list[int] = []
            for row in rows:
                try:
                    indexes.append(int(str(row["idempotency_key"]).removeprefix(prefix)))
                except ValueError:
                    raise ProviderError(
                        ProviderFailureKind.INVALID_OUTPUT,
                        "retained Fal result-read key has an invalid sequence number",
                        request_id=ticket.request_id,
                        resolved_model=PIXVERSE_TRANSITION_CAPABILITY,
                        halt_generation=True,
                    ) from None
            key = f"{prefix}{max(indexes, default=-1) + 1}"
        return await self._provider_call(
            attempt=attempt,
            stage=Stage.AUTHOR,
            key=key,
            role="fal_pixverse_transition",
            side_effect="fal_transition_result",
            request=poll_request,
            invoke=lambda: self.fal_media.poll_transition(ticket),
            validate_result_extra=validate_pixverse_video_result,
            provider_retries_override=0,
        )

    async def _draft_media_frame_sheet(
        self,
        attempt: dict[str, Any],
        intent: DraftVideoIntent,
        *,
        start: dict[str, Any],
        end: dict[str, Any],
        video: dict[str, Any],
    ) -> dict[str, Any]:
        retained_sheet = self._find_current_artifact(
            attempt["id"],
            ArtifactKind.MEDIA_FRAME_SHEET,
            metadata_match={"intent_id": intent.intent_id},
        )
        retained_manifest = self._find_current_artifact(
            attempt["id"],
            ArtifactKind.MODIFIER_MANIFEST,
            metadata_match={"intent_id": intent.intent_id},
        )
        if retained_sheet is not None and retained_manifest is not None:
            payload = self.persistence.load_json(retained_manifest["object_ref"])
            record = dict(payload["catalog_record"])
            record["modifier_manifest_sha256"] = retained_manifest["content_hash"]
            return record

        start_bytes = self.objects.get(start["object_ref"])
        end_bytes = self.objects.get(end["object_ref"])
        video_bytes = self.objects.get(video["object_ref"])
        frame_rows = max(2, math.ceil(intent.common_period_ms * intent.desired_fps / 1_000))
        extraction_request = VideoFrameExtractionRequest(
            source_video_sha256=video["content_hash"],
            start_frame_sha256=start["content_hash"],
            end_frame_sha256=end["content_hash"],
            body_columns=intent.body_columns,
            texels_per_cell=16,
            raster_overhang_px=intent.raster_overhang_px,
            frame_rows=frame_rows,
            desired_fps=float(intent.desired_fps),
            common_period_ms=float(intent.common_period_ms),
            matte_rgb=tuple(intent.matte_rgb),
            source_apron_px=32,
            output_apron_px=1,
        )
        extraction_config = VideoFrameExtractionConfig(
            ffmpeg_path=self.config.draft_automation.ffmpeg_path,
            ffprobe_path=self.config.draft_automation.ffprobe_path,
            total_timeout_seconds=self.config.draft_automation.video_extraction_timeout_seconds,
        )
        operation_request = {
            "schema_version": 1,
            "operation": "deterministic_video_to_rgba_frame_sheet",
            "intent_id": intent.intent_id,
            "tools": video_toolchain_identity(extraction_config),
            "inputs": {
                "video": video["content_hash"],
                "start": start["content_hash"],
                "end": end["content_hash"],
            },
            "request": asdict(extraction_request),
            "config": asdict(extraction_config),
        }

        async def invoke_extractor() -> ProviderResult:
            try:
                extracted = await asyncio.to_thread(
                    extract_rgba_frame_sheet,
                    video_bytes,
                    start_frame_png=start_bytes,
                    end_frame_png=end_bytes,
                    request=extraction_request,
                    config=extraction_config,
                )
            except VideoFrameExtractionError as error:
                # A paid PixVerse output can deterministically fail matte,
                # geometry, codec, camera, or loop checks without indicating
                # that the configured program behavior changed. Those failures
                # terminalize only this Attempt. Escalate globally only when
                # exact retained bytes moved or the admitted media toolchain
                # disappeared between identity capture and execution.
                invariant_drift = error.code in {"hash_mismatch", "tool_unavailable"}
                raise ProviderError(
                    ProviderFailureKind.INVALID_OUTPUT,
                    f"deterministic video extraction failed closed ({error.code}): {error}",
                    resolved_model="deterministic-video-frame-extractor-v1",
                    halt_generation=invariant_drift,
                ) from error
            return ProviderResult(
                value={"image": extracted.sheet_png, "media_type": "image/png"},
                request_id=f"local-{intent.intent_id}",
                resolved_model="deterministic-video-frame-extractor-v1",
                sanitized_metadata={"extraction_report": extracted.report},
                usage={"cost_micros": 0, "usage_complete": True},
            )

        self._assert_wall_time_before_spend(
            required_seconds=extraction_config.total_timeout_seconds,
            boundary="deterministic video frame extraction",
        )
        operation, result = await self._provider_call(
            attempt=self.database.get_attempt(attempt["id"]),
            stage=Stage.AUTHOR,
            key=f"{attempt['id']}:draft-media:{intent.intent_id}:extract-sheet",
            role="deterministic_video_extractor",
            side_effect="extract_rgba_frame_sheet",
            request=operation_request,
            invoke=invoke_extractor,
        )
        sheet_bytes, media_type = self._image_result(operation, result)
        if media_type != "image/png":
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "deterministic frame extractor did not retain a PNG sheet",
                resolved_model="deterministic-video-frame-extractor-v1",
            )
        operation_metadata = json.loads(operation.get("metadata_json") or "{}")
        report = operation_metadata.get("extraction_report")
        if not isinstance(report, dict):
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "deterministic frame extractor operation omitted its exact report",
                resolved_model="deterministic-video-frame-extractor-v1",
            )
        if report.get("tools") != operation_request["tools"]:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "deterministic extractor tool identity changed after journal admission",
                resolved_model="deterministic-video-frame-extractor-v1",
                halt_generation=True,
            )
        expected_sheet_hash = report.get("output", {}).get("sheet_png_sha256")
        actual_sheet_hash = f"sha256:{hashlib.sha256(sheet_bytes).hexdigest()}"
        if expected_sheet_hash != actual_sheet_hash:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "deterministic extractor report differs from retained sheet bytes",
                resolved_model="deterministic-video-frame-extractor-v1",
                halt_generation=True,
            )
        row_height = 16 + 2 * intent.raster_overhang_px
        frame_bytes: list[bytes] = []
        try:
            with Image.open(io.BytesIO(sheet_bytes)) as opened:
                opened.load()
                sheet_image = opened.convert("RGBA")
            if sheet_image.size != (intent.body_columns * 16, frame_rows * row_height):
                raise ValueError("sheet dimensions differ from the exact authored grid")
            for index in range(frame_rows):
                frame = sheet_image.crop((0, index * row_height, sheet_image.width, (index + 1) * row_height))
                output = io.BytesIO()
                frame.save(output, format="PNG", optimize=False, compress_level=9)
                frame.close()
                frame_bytes.append(output.getvalue())
            sheet_image.close()
        except (OSError, ValueError) as error:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                f"retained extracted sheet cannot be split exactly: {error}",
                resolved_model="deterministic-video-frame-extractor-v1",
            ) from error
        expected_frames = report.get("output", {}).get("frame_png_sha256")
        actual_frames = [f"sha256:{hashlib.sha256(value).hexdigest()}" for value in frame_bytes]
        if expected_frames != actual_frames:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "retained frame rows differ from extractor report hashes",
                resolved_model="deterministic-video-frame-extractor-v1",
                halt_generation=True,
            )
        report_artifact = self._store_json_artifact(
            attempt,
            Stage.AUTHOR,
            ArtifactKind.MEDIA_EXTRACTION_REPORT,
            report,
            metadata={"intent_id": intent.intent_id, "operation_id": operation["id"]},
            occurrence_key=f"draft-media:{intent.intent_id}:extraction-report",
        )
        frame_artifacts = [
            self._store_bytes_artifact(
                attempt,
                Stage.AUTHOR,
                ArtifactKind.MEDIA_EXTRACTED_FRAME,
                value,
                "image/png",
                metadata={"intent_id": intent.intent_id, "frame_index": index},
                provenance={"extraction_operation_id": operation["id"]},
                occurrence_key=f"draft-media:{intent.intent_id}:frame:{index}",
            )
            for index, value in enumerate(frame_bytes)
        ]
        sheet_artifact = self._store_bytes_artifact(
            attempt,
            Stage.AUTHOR,
            ArtifactKind.MEDIA_FRAME_SHEET,
            sheet_bytes,
            "image/png",
            metadata={
                "intent_id": intent.intent_id,
                "width_px": intent.body_columns * 16,
                "height_px": frame_rows * row_height,
                "frame_rows": frame_rows,
                "body_columns": intent.body_columns,
                "texels_per_cell": 16,
                "raster_overhang_px": intent.raster_overhang_px,
                "row_texels": row_height,
                "alpha_verified": True,
                "operation_id": operation["id"],
                "report_artifact_id": report_artifact["id"],
            },
            provenance={
                "video_sha256": video["content_hash"],
                "start_frame_sha256": start["content_hash"],
                "end_frame_sha256": end["content_hash"],
                "frame_sha256": [item["content_hash"] for item in frame_artifacts],
            },
            occurrence_key=f"draft-media:{intent.intent_id}:frame-sheet",
        )
        provenance_payload = {
            "schema_version": 1,
            "lineage_id": attempt["concept_id"],
            "intent_id": intent.intent_id,
            "endpoint_artifact_ids": [start["id"], end["id"]],
            "video_artifact_id": video["id"],
            "sheet_artifact_id": sheet_artifact["id"],
            "report_artifact_id": report_artifact["id"],
            "operation_ids": {
                "extract": operation["id"],
                "fal_submit": json.loads(video["metadata_json"])["submit_operation_id"],
                "fal_result": json.loads(video["metadata_json"])["poll_operation_id"],
            },
        }
        provenance_artifact = self._store_json_artifact(
            attempt,
            Stage.AUTHOR,
            ArtifactKind.MEDIA_PROVENANCE,
            provenance_payload,
            metadata={"intent_id": intent.intent_id},
            occurrence_key=f"draft-media:{intent.intent_id}:provenance",
        )
        extraction_evidence = {
            "source_arena": "reserved_empty",
            "alpha_contract": "exact_mask_matte",
            "background_removal": "required",
            "matte_policy": "fail_closed",
            "cropped_object_retained": True,
        }
        video_evidence = {
            "start_frame_sha256": start["content_hash"],
            "end_frame_sha256": end["content_hash"],
            "source_video_sha256": video["content_hash"],
            "extracted_sheet_sha256": sheet_artifact["content_hash"],
            "common_period_ms": float(intent.common_period_ms),
            "desired_fps": float(intent.desired_fps),
            "derived_frame_rows": frame_rows,
            "effective_frame_row_cap": min(
                120,
                2_048 // row_height,
                16_777_216 // (intent.body_columns * 16 * row_height * 4),
            ),
            "frame_extraction": "deterministic_uniform_full_period",
            "alpha_matte_verification": "fail_closed",
            "loop_closure": "true_final_to_zero",
            "retained_inputs_and_output": True,
        }
        asset = AssetPlan(
            kind="sheet",
            natural_length_cells=intent.body_columns,
            frames=frame_rows,
            desired_fps=float(intent.desired_fps),
            texels_per_cell=16,
            raster_overhang_px=intent.raster_overhang_px,
            anchor=intent.anchor,
            fit="tile" if intent.anchor == "whole" else "clip",
            tile_phase_origin="tail" if intent.anchor == "whole" else None,
            fade="none" if intent.anchor == "whole" else ("trailing" if intent.anchor == "head" else "leading"),
            transverse_edge_policy="fail_closed_transparent_effect",
            prompt=(
                f"Bind exact retained RGBA sheet {sheet_artifact['content_hash']} for "
                f"{intent.logical_key}; do not regenerate or repaint it."
            ),
        )
        catalog_record = {
            "logical_key": intent.logical_key,
            "component_key": intent.component_key,
            "texture_name": intent.texture_name,
            "source_mode": "video_frames",
            "source_object_sha256": sheet_artifact["content_hash"],
            "modifier_manifest_sha256": None,
            "provenance_sha256": provenance_artifact["content_hash"],
            # License/provenance authority comes from the driver's retained
            # current-concept provider lineage.  The planning model is never
            # allowed to confer a license on generated media.
            "license_id": "provider-generated-current-concept-v1",
            "authorized_lineage_ids": [attempt["concept_id"]],
            "extraction": extraction_evidence,
            "video": video_evidence,
            "asset": asset.model_dump(mode="json"),
        }
        modifier_manifest_payload = {
            "schema_version": 1,
            "modifier_id": (
                f"modifier:{attempt['concept_id']}:{sheet_artifact['content_hash'].removeprefix('sha256:')}"
            ),
            "intent_id": intent.intent_id,
            "catalog_record": catalog_record,
            "extraction_report_sha256": report_artifact["content_hash"],
        }
        manifest_artifact = self._store_json_artifact(
            attempt,
            Stage.AUTHOR,
            ArtifactKind.MODIFIER_MANIFEST,
            modifier_manifest_payload,
            metadata={"intent_id": intent.intent_id, "source_object_sha256": sheet_artifact["content_hash"]},
            occurrence_key=f"draft-media:{intent.intent_id}:modifier-manifest",
        )
        catalog_record["modifier_manifest_sha256"] = manifest_artifact["content_hash"]
        return catalog_record

    def _retain_forge_bundle(
        self,
        *,
        attempt: dict[str, Any],
        asset_index: int,
        generation: int,
        bundle: ForgeBundle,
        normalized: dict[str, Any],
        provider_artifact_id: str | None,
        re_evaluation_target_id: str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """Retain all exact forge outputs before any accept/upload decision."""

        gate_accepted = self.assets.accepted(bundle)
        evidence_payload = {
            **(
                bundle.manifest
                or {
                    "schema_version": 1,
                    "accepted": False,
                    "rejection": [reason for gate in bundle.gate_results for reason in gate.reasons],
                }
            ),
            # This is factory evidence, not the exact upload manifest.  Keeping
            # it distinct prevents the later authenticated manifest from CAS-
            # deduplicating into an immutable ``uploaded: false`` row.
            "factory_gate_accepted": gate_accepted,
        }
        evidence = self._store_json_artifact(
            attempt,
            Stage.ASSETS,
            ArtifactKind.FORGE_MANIFEST,
            evidence_payload,
            metadata={
                "asset_index": asset_index,
                "generation": generation,
                "uploaded": False,
                "normalized_artifact_id": normalized["id"],
                "provider_artifact_id": provider_artifact_id,
                "repair_methods": list(bundle.repair_methods),
                "gate_accepted": gate_accepted,
                "re_evaluation_target_id": re_evaluation_target_id,
            },
            occurrence_key=f"asset:{asset_index}:generation:{generation}:forge-evidence",
        )
        for gate in bundle.gate_results:
            self.database.add_evaluation(
                artifact_id=evidence["id"],
                attempt_id=attempt["id"],
                evaluator="deterministic",
                result=gate,
            )

        common_metadata = {
            "asset_index": asset_index,
            "generation": generation,
            "uploaded": False,
            "gate_accepted": gate_accepted,
            "normalized_artifact_id": normalized["id"],
            "forge_manifest_artifact_id": evidence["id"],
            "repair_methods": list(bundle.repair_methods),
            "re_evaluation_target_id": re_evaluation_target_id,
        }
        common_provenance = {
            "provider_artifact_id": provider_artifact_id,
            "normalized_artifact_id": normalized["id"],
            "forge_manifest_artifact_id": evidence["id"],
            "repair_methods": list(bundle.repair_methods),
            "re_evaluation_target_id": re_evaluation_target_id,
        }
        for variant_index, variant in enumerate(bundle.variants):
            self._store_bytes_artifact(
                attempt,
                Stage.ASSETS,
                ArtifactKind.TEXTURE_VARIANT,
                variant.data,
                "image/png",
                metadata={
                    **common_metadata,
                    "phase": "forge_ladder_candidate",
                    "variant_index": variant_index,
                    "content_ref": variant.content_ref,
                    "width_px": variant.width_px,
                    "height_px": variant.height_px,
                    "bytes": variant.bytes,
                    "texels_per_cell": variant.texels_per_cell,
                },
                provenance=common_provenance,
                occurrence_key=(f"asset:{asset_index}:generation:{generation}:variant:{variant_index}"),
            )
        if bundle.rejected_output is not None:
            rejected = bundle.rejected_output
            self._store_bytes_artifact(
                attempt,
                Stage.ASSETS,
                ArtifactKind.SOURCE_ASSET,
                rejected.data,
                "image/png",
                metadata={
                    **common_metadata,
                    "phase": "forge_rejected_output",
                    "content_ref": rejected.content_ref,
                    "width_px": rejected.width_px,
                    "height_px": rejected.height_px,
                    "bytes": rejected.bytes,
                    "texels_per_cell": rejected.texels_per_cell,
                },
                provenance={**common_provenance, "post_repair": bundle.repaired},
                occurrence_key=f"asset:{asset_index}:generation:{generation}:rejected-output",
            )
        return evidence, gate_accepted

    async def _re_evaluate_asset(
        self,
        attempt: dict[str, Any],
        plan: ImplementationPlan,
    ) -> dict[str, Any]:
        """Run current deterministic gates over the selected immutable bytes."""

        target_id = str(attempt.get("restart_stage") or "").removeprefix("re_evaluate:")
        target = self.database.get_artifact(target_id)
        supported = {
            ArtifactKind.SOURCE_ASSET,
            ArtifactKind.FORGE_MANIFEST,
            ArtifactKind.TEXTURE_VARIANT,
        }
        if target["attempt_id"] != attempt["id"] or target["kind"] not in supported:
            return self._block_attempt(attempt, "asset re-evaluation does not bind a retained asset artifact")
        metadata = json.loads(target["metadata_json"])
        asset_index = metadata.get("asset_index")
        if not isinstance(asset_index, int) or not 0 <= asset_index < len(plan.asset_plan):
            return self._block_attempt(attempt, "asset re-evaluation has no valid retained plan index")
        asset = plan.asset_plan[asset_index]

        def variant_from_artifact(artifact: dict[str, Any]) -> ForgeVariant:
            exact = self.objects.get(artifact["object_ref"])
            item_metadata = json.loads(artifact["metadata_json"])
            try:
                with Image.open(io.BytesIO(exact)) as opened:
                    width, height = opened.size
            except Exception:
                width = int(item_metadata.get("width_px", 0))
                height = int(item_metadata.get("height_px", 0))
            texels = int(item_metadata.get("texels_per_cell", asset.texels_per_cell))
            return ForgeVariant(
                content_ref=artifact["content_hash"],
                url=f"/artifacts/{artifact['id']}",
                width_px=int(item_metadata.get("width_px", width)),
                height_px=int(item_metadata.get("height_px", height)),
                bytes=int(item_metadata.get("bytes", len(exact))),
                texels_per_cell=texels,
                data=exact,
            )

        selected: list[dict[str, Any]] = []
        if target["kind"] in {ArtifactKind.SOURCE_ASSET, ArtifactKind.TEXTURE_VARIANT}:
            selected = [target]
        else:
            manifest = self.persistence.load_json(target["object_ref"])
            refs = {
                item.get("content_ref")
                for item in manifest.get("descriptor", {}).get("variants", [])
                if isinstance(item, dict) and isinstance(item.get("content_ref"), str)
            }
            generation = metadata.get("generation")
            for lineage_attempt in self._lineage(attempt):
                for candidate in self.database.artifacts_for_attempt(
                    lineage_attempt["id"], stage=Stage.ASSETS, kind=ArtifactKind.TEXTURE_VARIANT
                ):
                    candidate_metadata = json.loads(candidate["metadata_json"])
                    if (
                        candidate["content_hash"] in refs
                        and candidate_metadata.get("asset_index") == asset_index
                        and (generation is None or candidate_metadata.get("generation") == generation)
                    ):
                        selected.append(candidate)
            if not selected:
                original_manifest_id = metadata.get("re_evaluates_artifact_id", target["id"])
                for lineage_attempt in self._lineage(attempt):
                    for candidate in self.database.artifacts_for_attempt(
                        lineage_attempt["id"], stage=Stage.ASSETS, kind=ArtifactKind.SOURCE_ASSET
                    ):
                        candidate_metadata = json.loads(candidate["metadata_json"])
                        if (
                            candidate_metadata.get("phase") == "forge_rejected_output"
                            and candidate_metadata.get("forge_manifest_artifact_id") == original_manifest_id
                        ):
                            selected.append(candidate)
        # Preserve one occurrence of each selected row. Equal content hashes
        # may legitimately belong to different asset indices/generations.
        selected = list({artifact["id"]: artifact for artifact in selected}.values())
        if not selected:
            return self._block_attempt(attempt, "asset re-evaluation target has no retained exact pixels")
        variants = tuple(variant_from_artifact(artifact) for artifact in selected)
        self._assert_wall_time_before_spend(
            required_seconds=120 * len(variants),
            boundary="exact asset re-evaluation",
        )
        results = await asyncio.to_thread(self.assets.re_evaluate_exact, asset, variants)
        for result in results:
            self.database.add_evaluation(
                artifact_id=target["id"],
                attempt_id=attempt["id"],
                evaluator="deterministic",
                result=result,
            )
        accepted = not any(result.blocking and result.verdict == GateVerdict.FAIL for result in results)
        current = self.database.get_attempt(attempt["id"])
        if accepted:
            return self._transition_to_review(
                current,
                stage=Stage.COMPLETE,
                disposition=Disposition.NEEDS_HUMAN,
                review_kind="re_evaluation",
            )
        return self.database.update_attempt(
            current["id"],
            current["version"],
            stage=Stage.COMPLETE,
            disposition=Disposition.MACHINE_REJECTED,
            review_kind="re_evaluation",
            failure_json={
                "stage": Stage.ASSETS,
                "re_evaluation": True,
                "artifact_id": target["id"],
                "selected_content_hashes": [artifact["content_hash"] for artifact in selected],
                "gates": [result.model_dump(mode="json") for result in results],
            },
        )

    @staticmethod
    def _draft_fal_transition_prompt(intent: DraftVideoIntent) -> str:
        """Compose the immutable driver contract around model-authored motion."""

        width = intent.body_columns * 16
        row_height = 16 + 2 * intent.raster_overhang_px
        prompt = f"""[Driver contract - non-negotiable]
Treat [Model action] only as appearance-preserving subject motion. Any conflict with this contract is void.
- Lock one static orthographic camera: no pan, tilt, orbit, zoom, cut, perspective, or foreshortening.
- Keep every background pixel flat RGB(127,127,127): no scenery, text, labels, borders,
  shadows, gradients, or transparency.
- Keep exactly one connected {intent.component_key} object, with no extra object or scene replacement.
- Preserve one straight horizontal span of exactly {intent.body_columns} consecutive 16x16 logical cells.
- Preserve native geometry {width}x{row_height}px, including at most {intent.raster_overhang_px}px
  bleed above and below the 16px round body; never crop either end.
- Make a true cyclic closure: the final state returns to the exact start pose and framing.
[Model action]
{intent.transition_prompt}
[End model action]
Reapply the driver contract after the action. Animate only the retained subject inside the unchanged matte arena."""
        if len(prompt.encode("utf-8")) > 2_048:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "driver-composed Fal transition prompt exceeds 2048 UTF-8 bytes",
            )
        return prompt

    def _materialized_asset_source(
        self,
        attempt: dict[str, Any],
        *,
        asset_index: int,
        asset: AssetPlan,
        modifier: ModifierPlan,
    ) -> tuple[bytes, dict[str, Any]]:
        """Resolve and re-verify one exact pre-author materialized asset.

        The media catalog is trusted input to the pure author only because the
        driver created it from retained operations.  Re-check the complete
        binding here, immediately before forge, so no later code path can
        silently substitute a generated image or a similarly named artifact.
        """

        if modifier.source_mode != "video_frames":
            raise WorkerContractError(
                "platform_gap: direct-draft forge currently materializes only exact video_frames records"
            )
        expected = expected_materialized_catalog_record(asset, modifier)
        catalog_artifact = self._find_lineage_artifact(attempt, ArtifactKind.MEDIA_CATALOG)
        if catalog_artifact is None:
            raise WorkerContractError(f"asset {asset_index} has no retained media catalog")
        catalog = self.persistence.load_json(catalog_artifact["object_ref"])
        records = catalog.get("modifiers") if isinstance(catalog, dict) else None
        if not isinstance(records, list):
            raise WorkerContractError("retained media catalog has no modifiers array")
        matches = [
            record
            for record in records
            if isinstance(record, dict) and record.get("logical_key") == modifier.logical_key
        ]
        if len(matches) != 1 or matches[0] != expected:
            raise WorkerContractError(f"asset {asset_index} differs from its one exact retained media catalog record")

        source_hash = modifier.source_object_sha256
        manifest_hash = modifier.modifier_manifest_sha256
        if source_hash is None or manifest_hash is None or modifier.video is None:
            raise WorkerContractError(f"asset {asset_index} has incomplete video-frame evidence")
        source_artifact = self._find_lineage_artifact(
            attempt,
            ArtifactKind.MEDIA_FRAME_SHEET,
            content_hash=source_hash,
        )
        manifest_artifact = self._find_lineage_artifact(
            attempt,
            ArtifactKind.MODIFIER_MANIFEST,
            content_hash=manifest_hash,
        )
        provenance_artifact = self._find_lineage_artifact(
            attempt,
            ArtifactKind.MEDIA_PROVENANCE,
            content_hash=modifier.provenance_sha256,
        )
        if source_artifact is None or manifest_artifact is None or provenance_artifact is None:
            raise WorkerContractError(
                f"asset {asset_index} is missing its exact sheet, modifier manifest, or provenance"
            )
        if source_artifact["media_type"] != "image/png":
            raise WorkerContractError(f"asset {asset_index} materialized sheet is not a retained PNG")

        manifest = self.persistence.load_json(manifest_artifact["object_ref"])
        manifest_record = dict(expected)
        # A content-addressed document cannot contain its own hash.  The
        # outer catalog binds that hash; the immutable manifest retains the
        # otherwise-complete record with this one field deliberately empty.
        manifest_record["modifier_manifest_sha256"] = None
        if not isinstance(manifest, dict) or manifest.get("catalog_record") != manifest_record:
            raise WorkerContractError(f"asset {asset_index} modifier manifest differs from its catalog")
        report_hash = manifest.get("extraction_report_sha256")
        if (
            not isinstance(report_hash, str)
            or self._find_lineage_artifact(
                attempt,
                ArtifactKind.MEDIA_EXTRACTION_REPORT,
                content_hash=report_hash,
            )
            is None
        ):
            raise WorkerContractError(f"asset {asset_index} has no exact extraction report")

        for kind, content_hash, label, media_type in (
            (
                ArtifactKind.MEDIA_ENDPOINT,
                modifier.video.start_frame_sha256,
                "start endpoint",
                "image/png",
            ),
            (
                ArtifactKind.MEDIA_ENDPOINT,
                modifier.video.end_frame_sha256,
                "end endpoint",
                "image/png",
            ),
            (
                ArtifactKind.MEDIA_VIDEO,
                modifier.video.source_video_sha256,
                "source video",
                "video/mp4",
            ),
        ):
            evidence = self._find_lineage_artifact(attempt, kind, content_hash=content_hash)
            if evidence is None or evidence["media_type"] != media_type:
                raise WorkerContractError(f"asset {asset_index} has no exact retained {label}")

        metadata = json.loads(source_artifact["metadata_json"])
        expected_geometry = {
            "width_px": asset.natural_length_cells * asset.texels_per_cell,
            "height_px": asset.frames * asset_row_texels(asset),
            "frame_rows": asset.frames,
            "body_columns": asset.natural_length_cells,
            "texels_per_cell": asset.texels_per_cell,
            "raster_overhang_px": asset.raster_overhang_px,
            "row_texels": asset_row_texels(asset),
            "alpha_verified": True,
        }
        if any(metadata.get(field) != value for field, value in expected_geometry.items()):
            raise WorkerContractError(f"asset {asset_index} sheet geometry differs from its exact plan")
        source = self.objects.get(source_artifact["object_ref"])
        if f"sha256:{hashlib.sha256(source).hexdigest()}" != source_hash:
            raise WorkerContractError(f"asset {asset_index} sheet bytes differ from their content hash")
        try:
            with Image.open(io.BytesIO(source)) as opened:
                opened.load()
                if opened.format != "PNG" or opened.mode != "RGBA":
                    raise ValueError("sheet must remain exact RGBA PNG")
                if opened.size != (
                    expected_geometry["width_px"],
                    expected_geometry["height_px"],
                ):
                    raise ValueError("decoded sheet dimensions differ")
        except (OSError, ValueError) as error:
            raise WorkerContractError(f"asset {asset_index} materialized sheet failed exact decode: {error}") from error
        return source, source_artifact

    async def _build_assets(self, attempt: dict[str, Any]) -> dict[str, Any]:
        try:
            pinned_gates = self.pinned_gates(attempt)
        except (FileNotFoundError, RuntimeError, ValueError) as error:
            return self._block_attempt(attempt, str(error))
        self.assets.gates = pinned_gates.manifest
        self.assets.runtime_gates = pinned_gates
        plan_artifact = self._find_lineage_artifact(attempt, ArtifactKind.IMPLEMENTATION_PLAN)
        if not plan_artifact:
            return self._block_attempt(attempt, "asset stage is missing an implementation plan")
        plan = ImplementationPlan.model_validate(self.persistence.load_json(plan_artifact["object_ref"]))
        try:
            validate_plan_resource_limits(plan, pinned_gates.capabilities)
            self._validate_asset_image_call_budget(plan)
        except WorkerContractError as error:
            return self._reject_attempt(
                self.database.get_attempt(attempt["id"]),
                f"retained asset plan exceeds pinned capabilities: {error}",
            )
        except ValueError as error:
            return self._reject_attempt(
                self.database.get_attempt(attempt["id"]),
                f"retained asset plan exceeds provider image-call bounds: {error}",
            )
        if str(attempt.get("restart_stage") or "").startswith("re_evaluate:"):
            return await self._re_evaluate_asset(attempt, plan)
        remaining_image_reservation = self._planned_asset_image_reservation(
            plan,
            attempt=attempt,
        )
        try:
            self._check_budget(attempt, remaining_image_reservation)
        except BudgetExceeded as error:
            # An active scheduler must not retry the same unaffordable plan
            # forever. Retain the exact plan and make the operator action
            # explicit; no asset-provider operation has started here.
            return self._block_attempt(
                self.database.get_attempt(attempt["id"]),
                "retained asset plan cannot fit its worst-case remaining provider reservation: "
                f"{remaining_image_reservation} micros ({error})",
            )

        document_artifact = self._find_lineage_artifact(attempt, ArtifactKind.SKIN_DOCUMENT)
        prototype = self._find_lineage_artifact(
            attempt, ArtifactKind.PROTOTYPE, content_hash=attempt["approved_prototype_hash"]
        )
        if not plan_artifact or not document_artifact or not prototype:
            return self._block_attempt(attempt, "asset stage is missing plan, document, or prototype")
        document = self.persistence.load_json(document_artifact["object_ref"])
        prototype_bytes = self.objects.get(prototype["object_ref"])
        human_feedback = self._stage_feedback(attempt, "assets")
        trace = self._find_lineage_artifact(attempt, ArtifactKind.WORKER_TRACE)
        tool_requests = self.persistence.load_json(trace["object_ref"]).get("tool_requests", []) if trace else []
        generated_textures: list[dict[str, Any]] = []
        modifiers = {item.asset_index: item for item in plan.modifier_plan}

        for index, asset in enumerate(plan.asset_plan):
            uploaded = self._find_current_artifact(
                attempt["id"],
                ArtifactKind.FORGE_MANIFEST,
                metadata_match={"asset_index": index, "uploaded": True},
            )
            if uploaded:
                generated_textures.append(self.persistence.load_json(uploaded["object_ref"]))
                continue
            accepted: ForgeBundle | None = None
            accepted_evidence: dict[str, Any] | None = None
            accepted_normalized: dict[str, Any] | None = None
            accepted_generation: int | None = None
            rejection_feedback = ""
            modifier = modifiers.get(index)
            materialized = modifier is not None and modifier.source_mode != "direct_generate"
            if materialized:
                try:
                    assert modifier is not None
                    raw, raw_artifact = self._materialized_asset_source(
                        attempt,
                        asset_index=index,
                        asset=asset,
                        modifier=modifier,
                    )
                except WorkerContractError as error:
                    return self._reject_attempt(
                        self.database.get_attempt(attempt["id"]),
                        f"asset {index} materialized evidence is invalid: {error}",
                    )
                self._assert_wall_time_before_spend(required_seconds=900, boundary="local forge")
                bundle = await self._with_lease_heartbeat(asyncio.to_thread(self.assets.forge, raw, asset))
                normalized = self._store_bytes_artifact(
                    attempt,
                    Stage.ASSETS,
                    ArtifactKind.SOURCE_ASSET,
                    bundle.normalized_source,
                    "image/png",
                    metadata={
                        "asset_index": index,
                        "generation": 0,
                        "phase": "normalized_materialized_forge_input",
                        "materialized_source_artifact_id": raw_artifact["id"],
                        "materialized_source_sha256": raw_artifact["content_hash"],
                        "source_mode": modifier.source_mode,
                    },
                    provenance={
                        "materialized_source_artifact_id": raw_artifact["id"],
                        "modifier_manifest_sha256": modifier.modifier_manifest_sha256,
                    },
                    occurrence_key=f"asset:{index}:materialized:normalized-input",
                )
                evidence, gate_accepted = self._retain_forge_bundle(
                    attempt=attempt,
                    asset_index=index,
                    generation=0,
                    bundle=bundle,
                    normalized=normalized,
                    provider_artifact_id=None,
                )
                if not gate_accepted:
                    reasons = "; ".join(reason for gate in bundle.gate_results for reason in gate.reasons)
                    return self._reject_attempt(
                        self.database.get_attempt(attempt["id"]),
                        f"asset {index} exact materialized bytes failed strict forge; "
                        f"regeneration is forbidden: {reasons}",
                    )
                accepted = bundle
                accepted_evidence = evidence
                accepted_normalized = normalized
                accepted_generation = 0
            else:
                request_spec = next(
                    (
                        request
                        for request in tool_requests
                        if request.get("kind") == "generate_asset"
                        and int(request.get("arguments", {}).get("asset_index", -1)) == index
                    ),
                    None,
                )
                base_prompt = (
                    request_spec.get("arguments", {}).get("prompt") if request_spec else None
                ) or asset.prompt
                if not base_prompt:
                    base_prompt = f"Create the final {asset.kind} art faithful to the approved snake prototype."
                for generation in range(self.config.budgets.provider_retries + 1):
                    prompt = self._asset_prompt(asset, base_prompt, rejection_feedback, human_feedback)
                    try:
                        raw, raw_artifact = await self._generate_asset_provider_source(
                            attempt=attempt,
                            asset=asset,
                            asset_index=index,
                            generation=generation,
                            prompt=prompt,
                            prototype=prototype,
                            prototype_bytes=prototype_bytes,
                        )
                    except ProviderError as error:
                        if error.outcome_known and error.kind == ProviderFailureKind.INVALID_OUTPUT:
                            # The journal retained/quarantined the exact paid
                            # response. Continue only at the next bounded
                            # generation key; never replay the failed operation.
                            rejection_feedback = f"provider output failed exact validation: {error}"
                            continue
                        raise
                    self._assert_wall_time_before_spend(required_seconds=900, boundary="local forge")
                    bundle = await self._with_lease_heartbeat(asyncio.to_thread(self.assets.forge, raw, asset))
                    normalized = self._store_bytes_artifact(
                        attempt=attempt,
                        stage=Stage.ASSETS,
                        kind=ArtifactKind.SOURCE_ASSET,
                        data=bundle.normalized_source,
                        media_type="image/png",
                        metadata={
                            "asset_index": index,
                            "generation": generation,
                            "phase": "normalized_forge_input",
                            "provider_artifact_id": raw_artifact["id"],
                        },
                        occurrence_key=f"asset:{index}:generation:{generation}:normalized-input",
                    )
                    evidence, gate_accepted = self._retain_forge_bundle(
                        attempt=attempt,
                        asset_index=index,
                        generation=generation,
                        bundle=bundle,
                        normalized=normalized,
                        provider_artifact_id=raw_artifact["id"],
                    )
                    if gate_accepted:
                        accepted = bundle
                        accepted_evidence = evidence
                        accepted_normalized = normalized
                        accepted_generation = generation
                        break
                    rejection_feedback = "; ".join(reason for gate in bundle.gate_results for reason in gate.reasons)
            if accepted is None:
                return self._reject_attempt(
                    self.database.get_attempt(attempt["id"]),
                    f"asset {index} failed strict forge after bounded regeneration: {rejection_feedback}",
                )

            upload_operation, _ = await self._provider_call(
                attempt=self.database.get_attempt(attempt["id"]),
                stage=Stage.ASSETS,
                key=f"{attempt['id']}:asset:{index}:upload:{accepted.manifest['content_ref']}",
                role="snaketron_api",
                side_effect="upload_exact_forge_ladder",
                request=accepted.manifest,
                invoke=lambda accepted=accepted: self.api.upload_forge_bundle(accepted),
            )
            # Read-after-write compares exact bytes and prevents a revision from
            # naming server-generated/re-encoded derivatives.
            self._assert_wall_time_before_spend(
                required_seconds=(self.config.service.request_timeout_seconds * max(1, len(accepted.variants))),
                boundary="forge readback",
            )
            await self._with_lease_heartbeat(self.api.verify_forge_bundle(accepted))
            assert accepted_evidence is not None
            assert accepted_normalized is not None
            assert accepted_generation is not None
            persisted_manifest = self._store_json_artifact(
                attempt,
                Stage.ASSETS,
                ArtifactKind.FORGE_MANIFEST,
                accepted.manifest,
                metadata={
                    "asset_index": index,
                    "generation": accepted_generation,
                    "uploaded": True,
                    "operation_id": upload_operation["id"],
                    "normalized_artifact_id": accepted_normalized["id"],
                    "source_forge_manifest_artifact_id": accepted_evidence["id"],
                    "repair_methods": list(accepted.repair_methods),
                },
                occurrence_key=(f"asset:{index}:generation:{accepted_generation}:uploaded-manifest"),
            )
            _ = persisted_manifest
            generated_textures.append(accepted.manifest)

        try:
            final_document = self._bind_textures(document, plan, tool_requests, generated_textures)
            assert_resolved_document(final_document)
        except ValueError as error:
            return self._reject_attempt(
                self.database.get_attempt(attempt["id"]),
                f"could not bind generated assets: {error}",
            )
        self._store_json_artifact(
            attempt,
            Stage.ASSETS,
            ArtifactKind.SKIN_DOCUMENT,
            final_document,
            metadata={"phase": "exact_asset_descriptors"},
        )
        current = self.database.get_attempt(attempt["id"])
        return self.database.update_attempt(current["id"], current["version"], stage=Stage.BUILD_GATE)

    async def _build_gate(self, attempt: dict[str, Any]) -> dict[str, Any]:
        try:
            pinned_gates = self.pinned_gates(attempt)
        except (FileNotFoundError, RuntimeError, ValueError) as error:
            return self._block_attempt(attempt, str(error))
        re_evaluation = str(attempt.get("restart_stage") or "").startswith("re_evaluate:")
        document_artifact: dict[str, Any] | None
        if re_evaluation:
            target_id = str(attempt["restart_stage"]).split(":", 1)[1]
            target = self.database.get_artifact(target_id)
            document_artifact = target if target["kind"] == ArtifactKind.SKIN_DOCUMENT else None
        else:
            document_artifact = self._find_lineage_artifact(attempt, ArtifactKind.SKIN_DOCUMENT)
        plan_artifact = self._find_lineage_artifact(attempt, ArtifactKind.IMPLEMENTATION_PLAN)
        if not document_artifact or not plan_artifact:
            return self._block_attempt(attempt, "build gate has no document or implementation plan")
        document = self.persistence.load_json(document_artifact["object_ref"])
        try:
            assert_resolved_document(document)
        except WorkerContractError as error:
            return self._reject_attempt(attempt, str(error))
        plan = ImplementationPlan.model_validate(self.persistence.load_json(plan_artifact["object_ref"]))
        self._assert_wall_time_before_spend(
            required_seconds=120,
            boundary="deterministic document gates",
        )
        results = pinned_gates.validate_document(document, plan)
        results.append(self._ownership_gate(attempt, document, plan, pinned_gates))
        for result in results:
            self.database.add_evaluation(
                artifact_id=document_artifact["id"],
                attempt_id=attempt["id"],
                evaluator="deterministic",
                result=result,
            )
        current = self.database.get_attempt(attempt["id"])
        if pinned_gates.blocking_failure(results):
            return self.database.update_attempt(
                current["id"],
                current["version"],
                disposition=Disposition.MACHINE_REJECTED,
                failure_json={
                    "stage": Stage.BUILD_GATE,
                    "gates": [result.model_dump(mode="json") for result in results],
                },
            )
        if re_evaluation:
            updated = self._transition_to_review(
                current,
                stage=Stage.COMPLETE,
                disposition=Disposition.NEEDS_HUMAN,
                review_kind="re_evaluation",
            )
            return updated
        return self.database.update_attempt(current["id"], current["version"], stage=Stage.REGISTER)

    def _ownership_gate(
        self,
        attempt: dict[str, Any],
        document: dict[str, Any],
        plan: ImplementationPlan,
        gates: GateRunner,
    ) -> GateResult:
        """Bind every generated document ref to its authenticated upload intent."""

        document_refs: set[str] = set()
        for texture in document.get("textures", []):
            descriptor = texture.get("descriptor")
            if not isinstance(descriptor, dict):
                continue
            for variant in descriptor.get("variants", []):
                ref = variant.get("content_ref")
                if isinstance(ref, str):
                    document_refs.add(ref)

        uploaded_refs: set[str] = set()
        upload_operations: list[str] = []
        reasons: list[str] = []
        for candidate in self._lineage(attempt):
            for artifact in self.database.artifacts_for_attempt(candidate["id"], kind=ArtifactKind.FORGE_MANIFEST):
                metadata = json.loads(artifact["metadata_json"])
                if metadata.get("uploaded") is not True:
                    continue
                operation_id = metadata.get("operation_id")
                if not isinstance(operation_id, str):
                    reasons.append(f"uploaded forge manifest {artifact['id']} has no operation authority")
                    continue
                operation = self.database.get_operation(operation_id)
                if not (
                    operation["status"] == OperationStatus.SUCCEEDED
                    and operation["provider_role"] == "snaketron_api"
                    and operation["side_effect"] == "upload_exact_forge_ladder"
                ):
                    reasons.append(f"forge manifest {artifact['id']} does not name a successful authenticated upload")
                    continue
                upload_operations.append(operation_id)
                payload = self.persistence.load_json(artifact["object_ref"])
                for variant in payload.get("descriptor", {}).get("variants", []):
                    ref = variant.get("content_ref")
                    if isinstance(ref, str):
                        uploaded_refs.add(ref)

        expected_assets = bool(plan.asset_plan)
        if expected_assets and not upload_operations:
            reasons.append("asset-backed document has no successful authenticated forge upload")
        missing = sorted(document_refs - uploaded_refs)
        extra = sorted(uploaded_refs - document_refs)
        if missing:
            reasons.append(f"document refs lack owned upload authority: {missing}")
        if not expected_assets and document_refs:
            reasons.append("layers-only plan unexpectedly names generated upload refs")
        return gates.manifest.result(
            "ownership",
            not reasons,
            reasons=reasons,
            measurements={
                "authenticated_identity": "snaketron_factory_service",
                "document_content_refs": sorted(document_refs),
                "uploaded_content_refs": sorted(uploaded_refs),
                "retained_but_unreferenced_content_refs": extra,
                "upload_operation_ids": sorted(upload_operations),
                "applicable": expected_assets,
            },
        )

    async def _register(self, attempt: dict[str, Any]) -> dict[str, Any]:
        document_artifact = self._find_lineage_artifact(attempt, ArtifactKind.SKIN_DOCUMENT)
        if not document_artifact:
            return self._block_attempt(attempt, "registration has no exact SkinDoc artifact")
        document = self.persistence.load_json(document_artifact["object_ref"])
        concept = self.database.get_concept(attempt["concept_id"])
        previous = (
            self.database.latest_registered_attempt(concept["id"]) if attempt["purpose"] == Purpose.PRODUCTION else None
        )
        if previous is None:
            evaluation_only = attempt["purpose"] != Purpose.PRODUCTION
            skin_name = concept["name"] if not evaluation_only else f"Trial {attempt['id'][-8:]} {concept['name']}"[:40]
            idempotency_key = (
                f"factory-concept:{concept['id']}" if not evaluation_only else f"factory-trial:{attempt['id']}"
            )
            request = {
                "name": skin_name,
                "document": document,
                "idempotency_key": idempotency_key,
                "evaluation_only": evaluation_only,
            }
            side_effect = "create_private_skin_revision"
            operation, result = await self._provider_call(
                attempt=attempt,
                stage=Stage.REGISTER,
                key=f"{attempt['id']}:create-skin:{document_artifact['content_hash']}",
                role="snaketron_api",
                side_effect=side_effect,
                request=request,
                invoke=lambda: self.api.create_skin(
                    name=skin_name,
                    document=document,
                    idempotency_key=idempotency_key,
                    evaluation_only=evaluation_only,
                ),
            )
        else:
            request = {
                "skin_id": previous["production_skin_id"],
                "expected_head_revision": int(previous["production_revision"]),
                "document": document,
            }
            side_effect = "append_private_skin_revision"
            operation, result = await self._provider_call(
                attempt=attempt,
                stage=Stage.REGISTER,
                key=f"{attempt['id']}:append-skin:{document_artifact['content_hash']}",
                role="snaketron_api",
                side_effect=side_effect,
                request=request,
                invoke=lambda: self.api.append_revision(
                    skin_id=previous["production_skin_id"],
                    document=document,
                    expected_head_revision=int(previous["production_revision"]),
                ),
            )
        response = self._json_result(operation, result)
        try:
            validate_registration_result(
                side_effect=side_effect,
                request=request,
                response=response,
            )
        except (RecoveredResultError, TypeError, ValueError) as error:
            return self._block_attempt(
                self.database.get_attempt(attempt["id"]),
                f"Snaketron registration response violated exact authority: {error}",
            )
        skin_id = response.get("skinId") or response.get("skin_id")
        revision = response.get("headRevision") or response.get("head_revision")
        content_ref = response.get("contentRef") or response.get("content_ref")
        if skin_id is None or revision is None or not content_ref:
            return self._block_attempt(
                self.database.get_attempt(attempt["id"]),
                "Snaketron registration response omitted skinId/headRevision/contentRef",
            )
        expected_document_bytes = canonical_json(document).encode("utf-8")
        try:
            self._assert_wall_time_before_spend(
                required_seconds=self.config.service.request_timeout_seconds,
                boundary="private SkinDoc readback",
            )
            stored_document = await self._with_lease_heartbeat(self.api.get_skin_document(str(content_ref)))
        except AttributeError:
            return self._block_attempt(
                self.database.get_attempt(attempt["id"]),
                "Snaketron API adapter cannot perform authenticated private SkinDoc readback",
            )
        if (
            not isinstance(stored_document, bytes)
            or stored_document != expected_document_bytes
            or f"sha256:{hashlib.sha256(stored_document).hexdigest()}" != str(content_ref)
        ):
            return self._block_attempt(
                self.database.get_attempt(attempt["id"]),
                "Snaketron private SkinDoc readback differs from the exact registered canonical bytes",
            )
        concept = self.database.get_concept(concept["id"])
        if attempt["purpose"] == Purpose.PRODUCTION and concept["stable_skin_id"] is None:
            self.database.update_concept(concept["id"], concept["version"], stable_skin_id=str(skin_id))
        current = self.database.get_attempt(attempt["id"])
        return self.database.update_attempt(
            current["id"],
            current["version"],
            stage=Stage.RENDER,
            production_skin_id=str(skin_id),
            production_revision=str(revision),
            production_content_hash=str(content_ref),
        )

    async def _render(self, attempt: dict[str, Any]) -> dict[str, Any]:
        drift = self._behavior_drift_reason(attempt, Stage.RENDER)
        if drift is not None:
            return self._block_attempt(attempt, drift)
        if not attempt["production_content_hash"]:
            return self._block_attempt(attempt, "render stage has no registered content ref")
        behavior = json.loads(attempt["behavior_json"])
        pinned_renderer = behavior.get("renderer_sha")
        pinned_renderer_config = behavior.get("renderer_config_sha")
        try:
            self._assert_wall_time_before_spend(
                required_seconds=self.config.browser.timeout_seconds,
                boundary="browser capture",
            )
            if isinstance(self.renderer, BrowserRenderer):
                capture = asyncio.to_thread(
                    self.renderer.capture,
                    attempt["production_content_hash"],
                    expected_renderer_sha=pinned_renderer,
                    expected_config_sha=pinned_renderer_config,
                )
            else:
                capture = asyncio.to_thread(self.renderer.capture, attempt["production_content_hash"])
            evidence = await self._with_lease_heartbeat(capture)
        except RendererDrift as error:
            return self._block_attempt(attempt, str(error), program_halt="renderer_drift")
        if pinned_renderer and evidence.renderer_sha != pinned_renderer:
            return self._block_attempt(
                attempt,
                "browser evidence was produced by a renderer tree other than the pinned tree",
                program_halt="renderer_drift",
            )
        if (
            isinstance(self.renderer, BrowserRenderer)
            and pinned_renderer_config
            and evidence.renderer_config_sha != pinned_renderer_config
        ):
            return self._block_attempt(
                attempt,
                "browser evidence was produced with renderer configuration other than the pinned snapshot",
                program_halt="renderer_drift",
            )
        if evidence.contact_sheet:
            rendered = self._store_bytes_artifact(
                attempt,
                Stage.RENDER,
                ArtifactKind.CONTACT_SHEET,
                evidence.contact_sheet,
                "image/png",
                metadata={"renderer_sha": evidence.renderer_sha, "evidence": evidence.manifest},
            )
        else:
            rendered = self._store_json_artifact(
                attempt,
                Stage.RENDER,
                ArtifactKind.RENDER_EVIDENCE,
                evidence.manifest,
                metadata={"renderer_sha": evidence.renderer_sha, "capture_failed": True},
            )
        if evidence.animation:
            self._store_bytes_artifact(
                attempt,
                Stage.RENDER,
                ArtifactKind.ANIMATION_CAPTURE,
                evidence.animation,
                "video/webm",
                metadata={"renderer_sha": evidence.renderer_sha},
            )
        self.database.add_evaluation(
            artifact_id=rendered["id"],
            attempt_id=attempt["id"],
            evaluator="real_browser",
            result=evidence.gate_result,
        )
        current = self.database.get_attempt(attempt["id"])
        if evidence.gate_result.verdict == GateVerdict.FAIL:
            return self._block_attempt(
                current,
                "required real-browser evidence gate failed: " + "; ".join(evidence.gate_result.reasons),
                program_halt="required_browser_evidence",
                evidence={"gate": evidence.gate_result.model_dump(mode="json")},
            )
        return self.database.update_attempt(current["id"], current["version"], stage=Stage.BUILD_TRIAGE)

    async def _build_triage(self, attempt: dict[str, Any]) -> dict[str, Any]:
        only_artifact: str | None = None
        if str(attempt.get("restart_stage") or "").startswith("re_evaluate:"):
            only_artifact = str(attempt["restart_stage"]).split(":", 1)[1]
        re_evaluation = only_artifact is not None
        render = (
            self.database.get_artifact(only_artifact)
            if only_artifact
            else self._find_lineage_artifact(attempt, ArtifactKind.CONTACT_SHEET)
        )
        prototype = self._find_lineage_artifact(
            attempt, ArtifactKind.PROTOTYPE, content_hash=attempt["approved_prototype_hash"]
        )
        if not render or not prototype:
            return self._block_attempt(attempt, "visual build triage needs render and prototype bytes")
        judgment, operation = await self._judge(
            attempt,
            render,
            comparison=prototype,
            system=BUILD_JUDGE_RUBRIC,
            context={"selected_prototype": prototype["content_hash"]},
            key=f"{attempt['id']}:build-judge:{render['content_hash']}:v1",
        )
        result = GateResult(
            gate="visual_fidelity",
            gate_version=judge_evaluator_version(
                self.config,
                "build",
                resolved_model=operation["resolved_model"],
            ),
            blocking=False,
            verdict=GateVerdict(judgment.verdict),
            reasons=judgment.reasons,
            measurements={
                **judgment.model_dump(mode="json", exclude={"verdict", "reasons"}),
                "resolved_model": operation["resolved_model"],
            },
        )
        safety_result = self.gates.manifest.result(
            "safety_ip",
            not judgment.review_flags,
            reasons=[f"judge flagged {flag}" for flag in judgment.review_flags],
            measurements={
                "review_flags": judgment.review_flags,
                "resolved_model": operation["resolved_model"],
                "scope": "completed_build_exact_browser_pixels",
            },
        )
        current = self.database.get_attempt(attempt["id"])
        if re_evaluation:
            self.database.add_evaluation(
                artifact_id=render["id"],
                attempt_id=attempt["id"],
                evaluator="visual_judge",
                result=result,
                hidden_until_label=False,
            )
            self.database.add_evaluation(
                artifact_id=render["id"],
                attempt_id=attempt["id"],
                evaluator="visual_judge_safety_ip",
                result=safety_result,
                hidden_until_label=False,
            )
            updated = self._transition_to_review(
                current,
                stage=Stage.COMPLETE,
                disposition=Disposition.NEEDS_HUMAN,
                review_kind="re_evaluation",
            )
            return updated
        if attempt["purpose"] != Purpose.PRODUCTION:
            self.database.add_evaluation(
                artifact_id=render["id"],
                attempt_id=attempt["id"],
                evaluator="visual_judge",
                result=result,
                hidden_until_label=False,
            )
            self.database.add_evaluation(
                artifact_id=render["id"],
                attempt_id=attempt["id"],
                evaluator="visual_judge_safety_ip",
                result=safety_result,
                hidden_until_label=False,
            )
            return self.database.update_attempt(
                current["id"],
                current["version"],
                stage=Stage.COMPLETE,
                disposition=Disposition.EXPERIMENT_COMPLETE,
                review_kind=None,
            )
        if is_draft_attempt(attempt):
            self.database.add_evaluation(
                artifact_id=render["id"],
                attempt_id=attempt["id"],
                evaluator="visual_judge",
                result=result,
                hidden_until_label=False,
            )
            self.database.add_evaluation(
                artifact_id=render["id"],
                attempt_id=attempt["id"],
                evaluator="visual_judge_safety_ip",
                result=safety_result,
                hidden_until_label=False,
            )
            if safety_result.verdict == GateVerdict.FAIL or judgment.verdict == "machine_rejected":
                return self._reject_attempt(
                    self.database.get_attempt(attempt["id"]),
                    "direct draft failed completed-build safety or visual triage; private revision retained",
                )
            current = self.database.get_attempt(attempt["id"])
            skin_id = current["production_skin_id"]
            revision = current["production_revision"]
            content_ref = current["production_content_hash"]
            if not skin_id or not revision or not content_ref:
                return self._block_attempt(
                    current,
                    "Admin review cannot open without exact registered private revision authority",
                )
            request = {
                "skin_id": skin_id,
                "revision": int(revision),
                "content_ref": content_ref,
            }
            await self._provider_call(
                attempt=current,
                stage=Stage.BUILD_TRIAGE,
                key=f"{attempt['id']}:request-admin-review:{skin_id}:{revision}:{content_ref}",
                role="snaketron_api",
                side_effect="request_exact_publication_review",
                request=request,
                invoke=lambda: self.api.request_publication_exact(
                    skin_id=skin_id,
                    revision=int(revision),
                    content_ref=content_ref,
                ),
            )
            current = self.database.get_attempt(attempt["id"])
            return self.database.update_attempt(
                current["id"],
                current["version"],
                stage=Stage.COMPLETE,
                disposition=Disposition.AWAITING_ADMIN_REVIEW,
                review_kind=None,
            )
        routing = self.calibration.routing_status("build", evaluator_version=result.gate_version)
        safety_failed = safety_result.verdict == GateVerdict.FAIL
        rejected = judgment.verdict == "machine_rejected"
        sampled = (
            not safety_failed
            and routing.enabled
            and rejected
            and self.calibration.should_sample_reject(attempt["id"], "build")
        )
        self.database.add_evaluation(
            artifact_id=render["id"],
            attempt_id=attempt["id"],
            evaluator="visual_judge",
            result=result,
            hidden_until_label=not routing.enabled or sampled,
        )
        self.database.add_evaluation(
            artifact_id=render["id"],
            attempt_id=attempt["id"],
            evaluator="visual_judge_safety_ip",
            result=safety_result,
            hidden_until_label=False,
        )
        routed = not safety_failed and (not routing.enabled or not rejected)
        transition = {
            "stage": Stage.FINAL_REVIEW,
            "disposition": Disposition.NEEDS_HUMAN if routed else Disposition.MACHINE_REJECTED,
            "review_kind": "final" if routed else ("build_label" if sampled else None),
        }
        if routed:
            current = self.database.get_attempt(attempt["id"])
            skin_id = current["production_skin_id"]
            revision = current["production_revision"]
            content_ref = current["production_content_hash"]
            if not skin_id or not revision or not content_ref:
                return self._block_attempt(
                    current,
                    "final review cannot open without exact registered revision authority",
                )
            request = {
                "skin_id": skin_id,
                "revision": int(revision),
                "content_ref": content_ref,
            }
            await self._provider_call(
                attempt=current,
                stage=Stage.BUILD_TRIAGE,
                key=f"{attempt['id']}:request-publication:{skin_id}:{revision}:{content_ref}",
                role="snaketron_api",
                side_effect="request_exact_publication_review",
                request=request,
                invoke=lambda: self.api.request_publication_exact(
                    skin_id=skin_id,
                    revision=int(revision),
                    content_ref=content_ref,
                ),
            )
            # The durable operation advances attempt.version. The human-review
            # outbox transition must bind the new version, never the snapshot
            # from before the external request.
            current = self.database.get_attempt(attempt["id"])
        updated = (
            self._transition_to_review(current, **transition)
            if routed or sampled
            else self.database.update_attempt(current["id"], current["version"], **transition)
        )
        return updated

    async def _provider_call(
        self,
        *,
        attempt: dict[str, Any],
        stage: Stage,
        key: str,
        role: str,
        side_effect: str,
        request: Any,
        invoke: Any,
        validate_result_extra: Any | None = None,
        provider_retries_override: int | None = None,
    ) -> tuple[dict[str, Any], ProviderResult | None]:
        drift = self._behavior_drift_reason(attempt, stage)
        if drift is not None:
            raise BehaviorDrift(drift)
        reserve = self._reservation(role, side_effect)
        safe_failures = {
            ProviderFailureKind.AUTHENTICATION,
            ProviderFailureKind.TIMEOUT,
            ProviderFailureKind.UNAVAILABLE,
            ProviderFailureKind.QUOTA,
        }
        request_hash = self.journal.request_hash(request)
        request_object = self.objects.put(self.journal.request_payload(request))
        retries = (
            self.config.budgets.provider_retries if provider_retries_override is None else provider_retries_override
        )
        if retries < 0:
            raise ValueError("provider_retries_override cannot be negative")

        def validate_result(result: ProviderResult) -> None:
            self._validate_resolved_model(attempt, role, result)
            if validate_result_extra is not None:
                validate_result_extra(result)

        repeatable_provider_read = role == "fal_pixverse_transition" and side_effect == "fal_transition_result"
        repeatable_local_computation = (
            role == "deterministic_video_extractor" and side_effect == "extract_rgba_frame_sheet"
        )

        async def run_journal(operation_key: str, retry: int):
            operation, result = await self._with_lease_heartbeat(
                self.journal.run_provider(
                    attempt_id=attempt["id"],
                    stage=stage,
                    idempotency_key=operation_key,
                    side_effect=side_effect,
                    provider_role=role,
                    request=request,
                    reserve_micros=reserve,
                    invoke=invoke,
                    persist_result=self.persistence,
                    validate_result=validate_result,
                    metadata={
                        "config_sha": self.config.version_sha256,
                        "retry": retry,
                        "request_ref": request_object.uri,
                        "request_sha256": request_object.sha256,
                    },
                    repeatable_read=repeatable_provider_read,
                    repeatable_local=repeatable_local_computation,
                )
            )
            operation_metadata = json.loads(operation.get("metadata_json") or "{}")
            readback_effects = {
                "create_private_skin_revision",
                "append_private_skin_revision",
                "request_exact_publication_review",
            }
            if (
                result is None
                and side_effect in readback_effects
                and isinstance(operation_metadata, dict)
                and isinstance(operation_metadata.get("recovery"), dict)
            ):
                authority_request = dict(request)
                skin_id = authority_request.get("skin_id")
                if skin_id is None:
                    recovered_value = self.persistence.load_json(operation["result_hash"])
                    if isinstance(recovered_value, dict):
                        skin_id = recovered_value.get("skinId", recovered_value.get("skin_id"))
                        authority_request["skin_id"] = skin_id
                if skin_id is None:
                    raise ProviderError(
                        ProviderFailureKind.INVALID_OUTPUT,
                        f"recovered {side_effect} result omitted exact skin authority",
                        halt_generation=True,
                    )
                authority = await self._with_lease_heartbeat(self.api.get_skin_authority(skin_id))
                try:
                    validate_skin_authority_readback(
                        side_effect=side_effect,
                        request=authority_request,
                        authority=authority.value,
                    )
                except RecoveredResultError as error:
                    operation_metadata["recovery_validation"] = {
                        "status": "failed_terminal",
                        "message": str(error),
                        "result_hash": operation.get("result_hash"),
                        "authority": "authenticated_snaketron_readback",
                    }
                    self.database.transition_operation(
                        operation["id"],
                        OperationStatus.SUCCEEDED,
                        OperationStatus.FAILED_TERMINAL,
                        retry_class="terminal",
                        metadata_json=operation_metadata,
                        failure_json={
                            "kind": ProviderFailureKind.INVALID_OUTPUT,
                            "message": str(error),
                            "quarantined_result_hash": operation.get("result_hash"),
                            "operator_action": "reconcile the exact Snaketron revision/request state",
                        },
                    )
                    raise ProviderError(
                        ProviderFailureKind.INVALID_OUTPUT,
                        f"recovered {side_effect} failed authenticated Snaketron readback: {error}",
                        request_id=authority.request_id,
                        resolved_model=authority.resolved_model,
                        halt_generation=True,
                    ) from error
            return operation, result

        for retry in range(retries + 1):
            operation_key = key if retry == 0 else f"{key}:retry:{retry}"
            with self.database.connect() as connection:
                existing_row = connection.execute(
                    "SELECT * FROM operation WHERE idempotency_key=?", (operation_key,)
                ).fetchone()
            existing = dict(existing_row) if existing_row is not None else None
            retryable_existing = existing is not None and (
                existing["status"] == OperationStatus.FAILED_RETRYABLE
                or (existing["status"] == OperationStatus.RESOLVED and existing["retry_class"] == "retry_safe")
            )
            if retryable_existing:
                assert existing is not None
                if existing["request_hash"] != request_hash:
                    # Let the journal raise its standard conditional-write
                    # conflict rather than silently treating a changed request
                    # as the next retry.
                    return await run_journal(operation_key, retry)
                if retry < retries:
                    # Either the adapter proved this call did not execute or
                    # an authenticated operator did so with immutable
                    # evidence. Resume at the next deterministic numbered key
                    # without invoking the completed operation again.
                    continue
                raise ExistingOperation(f"known-safe provider retries exhausted for operation {existing['id']}")
            if existing is not None:
                # Its reservation already committed. Re-entering the journal
                # either reconstructs success, safely starts an INTENT, or
                # fails closed for RUNNING/reconciliation/terminal states.
                if existing["status"] == OperationStatus.SUCCEEDED:
                    self._validate_replayed_operation_model(attempt, role, existing)
                if existing["status"] == OperationStatus.INTENT or (
                    existing["status"] == OperationStatus.RUNNING
                    and (repeatable_provider_read or repeatable_local_computation)
                ):
                    self._assert_wall_time_before_spend(role=role)
                return await run_journal(operation_key, retry)

            self._assert_wall_time_before_spend(role=role)
            self._check_budget(self.database.get_attempt(attempt["id"]), reserve)
            try:
                return await run_journal(operation_key, retry)
            except ProviderError as error:
                if error.outcome_known and error.kind in safe_failures and retry < retries:
                    continue
                raise
        raise AssertionError("provider retry loop did not return or raise")

    def _wall_time_exhausted(self) -> bool:
        return self._run_deadline is not None and time.monotonic() >= self._run_deadline

    def _assert_wall_time_before_spend(
        self,
        *,
        role: str | None = None,
        required_seconds: float = 0,
        boundary: str = "external spend",
    ) -> None:
        """Admit a boundary only when its configured timeout fits this tick.

        Checking only that the deadline has not *yet* elapsed lets a 600-second
        image request start in the final second of a tick. Hermes would then
        kill the no-agent script before the adapter's own timeout, converting
        an avoidable late call into an unknown external outcome. Model and API
        roles therefore reserve their full configured timeout before creating
        an operation intent. Exact successful replay remains available even
        after the deadline because it performs no external work.
        """

        if role is not None:
            config_role_name = "task_worker" if role == "task_worker" else role
            role_config = getattr(self.config.models, config_role_name, None)
            if role_config is not None:
                required_seconds = max(required_seconds, float(role_config.timeout_seconds))
            elif role == "snaketron_api":
                required_seconds = max(required_seconds, float(self.config.service.request_timeout_seconds))
            elif role == "git_promotion":
                # GitPromoter owns a single absolute deadline across its
                # validator, signed commit/tag, both pushes, remote check, and
                # clean-clone verification. Reserve its bounded cleanup too.
                required_seconds = max(
                    required_seconds,
                    float(self.config.optimizer.promotion_timeout_seconds + 30),
                )
            elif role == "fal_pixverse_transition":
                required_seconds = max(
                    required_seconds,
                    float(self.config.draft_automation.fal_transition_timeout_seconds),
                )
            boundary = role
        if self._run_deadline is None:
            return
        remaining = self._run_deadline - time.monotonic()
        # One second is deliberately kept for persisting the terminal result,
        # renewing/releasing the process lease, and serializing the run report.
        required_with_settlement = max(0.0, float(required_seconds)) + 1.0
        if remaining < required_with_settlement:
            raise RunWallTimeExceeded(
                f"run wall-time cap leaves {max(0.0, remaining):.3f}s before {boundary}; "
                f"requires {required_with_settlement:.3f}s including settlement"
            )

    def _validate_resolved_model(
        self,
        attempt: dict[str, Any],
        role: str,
        result: ProviderResult,
    ) -> None:
        """Reject provider fallback before its output can become a successful operation.

        A configured alias may explicitly allow a dated immutable provider id.
        The first accepted id becomes the exact per-role pin for the Attempt;
        subsequent calls cannot move even within the allowed family.
        """

        if role == "deterministic_video_extractor":
            if result.resolved_model != "deterministic-video-frame-extractor-v1":
                raise ProviderError(
                    ProviderFailureKind.INVALID_OUTPUT,
                    "video extractor resolved identity differs from the pinned deterministic implementation",
                    request_id=result.request_id,
                    resolved_model=result.resolved_model,
                    halt_generation=True,
                )
            return
        if role == "fal_pixverse_transition":
            resolved = result.resolved_model
            expected = self.config.draft_automation.fal_transition_capability_id
            if resolved != expected or resolved != PIXVERSE_TRANSITION_CAPABILITY:
                raise ProviderError(
                    ProviderFailureKind.INVALID_OUTPUT,
                    f"Fal resolved capability {resolved!r} differs from pinned {expected!r}",
                    request_id=result.request_id,
                    resolved_model=resolved,
                    halt_generation=True,
                )
            return
        config_role_name = "task_worker" if role == "task_worker" else role
        role_config = getattr(self.config.models, config_role_name, None)
        if role_config is None:
            # Snaketron storage/API and human-operator calls are versioned
            # external services, not content-model roles.
            return
        resolved = result.resolved_model
        if not role_config.accepts_resolved_model(resolved):
            expected = role_config.resolved_model_pattern or role_config.model
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                f"{role} resolved model {resolved!r} violates pinned identity {expected!r}",
                request_id=result.request_id,
                resolved_model=resolved,
                halt_generation=True,
            )
        with self.database.connect() as connection:
            rows = connection.execute(
                "SELECT DISTINCT resolved_model FROM operation WHERE attempt_id=? "
                "AND provider_role=? AND status=? AND resolved_model IS NOT NULL",
                (attempt["id"], role, OperationStatus.SUCCEEDED),
            ).fetchall()
        prior = {str(row[0]) for row in rows}
        if prior and prior != {resolved}:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                f"{role} resolved model changed within Attempt from {sorted(prior)!r} to {resolved!r}",
                request_id=result.request_id,
                resolved_model=resolved,
                halt_generation=True,
            )

    def _validate_replayed_operation_model(
        self,
        attempt: dict[str, Any],
        role: str,
        operation: dict[str, Any],
    ) -> None:
        """Validate replay authority and terminally quarantine bad recovery.

        An authenticated recovery is still untrusted provider output. It may
        become replayable only while its retained bytes, media/structured
        contract, role, and exact model identity all remain valid.
        """

        metadata: dict[str, Any] = {}
        try:
            parsed_metadata = json.loads(operation.get("metadata_json") or "{}")
            if not isinstance(parsed_metadata, dict):
                raise RecoveredResultError("operation metadata is not a JSON object")
            metadata = parsed_metadata
            resolved = operation.get("resolved_model")
            config_role_name = "task_worker" if role == "task_worker" else role
            if hasattr(self.config.models, config_role_name) or role in {
                "fal_pixverse_transition",
                "deterministic_video_extractor",
            }:
                if not isinstance(resolved, str) or not resolved:
                    raise RecoveredResultError(f"replayed {role} operation omitted its resolved model identity")
                self._validate_resolved_model(
                    attempt,
                    role,
                    ProviderResult(
                        value={},
                        request_id=operation.get("provider_request_id"),
                        resolved_model=resolved,
                    ),
                )
            recovery = metadata.get("recovery")
            if recovery is None:
                return
            if not isinstance(recovery, dict):
                raise RecoveredResultError("authenticated recovery metadata is malformed")
            result_hash = operation.get("result_hash")
            if not isinstance(result_hash, str):
                raise RecoveredResultError("authenticated recovery omitted its retained result hash")
            if not isinstance(resolved, str) or not resolved:
                raise RecoveredResultError("authenticated recovery omitted its resolved model identity")
            recovered_result_metadata = recovery.get("result_metadata")
            video_metadata = (
                {"video": recovered_result_metadata.get("video")}
                if isinstance(recovered_result_metadata, dict)
                and recovered_result_metadata.get("result") == {"kind": "video", "media_type": "video/mp4"}
                else None
            )
            validate_recovered_result(
                config=self.config,
                operation=operation,
                database=self.database,
                objects=self.objects,
                result_hash=result_hash,
                resolved_model=resolved,
                media_type=recovery.get("media_type"),
                provider_request_id=operation.get("provider_request_id"),
                result_metadata=video_metadata,
            )
        except (ProviderError, RecoveredResultError, TypeError, ValueError) as error:
            evidence_ref = None
            recovered_metadata = metadata.get("recovery")
            if isinstance(recovered_metadata, dict):
                evidence_ref = recovered_metadata.get("evidence_ref")
            metadata["recovery_validation"] = {
                "status": "failed_terminal",
                "message": str(error),
                "result_hash": operation.get("result_hash"),
            }
            message = f"replayed operation {operation['id']} failed exact recovery validation: {error}"
            self.database.transition_operation(
                operation["id"],
                OperationStatus.SUCCEEDED,
                OperationStatus.FAILED_TERMINAL,
                retry_class="terminal",
                metadata_json=metadata,
                failure_json={
                    "kind": ProviderFailureKind.INVALID_OUTPUT,
                    "message": message,
                    "quarantined_result_hash": operation.get("result_hash"),
                    "recovery_evidence_ref": evidence_ref,
                    "operator_action": (
                        "inspect the retained CAS object and provider audit evidence; the invalid result "
                        "will never be replayed"
                    ),
                },
            )
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                message,
                request_id=operation.get("provider_request_id"),
                resolved_model=operation.get("resolved_model"),
                halt_generation=True,
            ) from error

    async def _with_lease_heartbeat(self, awaitable: Any) -> Any:
        """Keep the process lease alive around a potentially long boundary."""

        if self._lease_token is None:
            return await awaitable
        self.database.renew_lease("production", self._lease_token, self.config.lease_seconds)

        async def heartbeat() -> None:
            interval = max(1.0, self.config.lease_seconds / 3)
            while True:
                await asyncio.sleep(interval)
                assert self._lease_token is not None
                self.database.renew_lease("production", self._lease_token, self.config.lease_seconds)

        operation_task = asyncio.ensure_future(awaitable)
        heartbeat_task = asyncio.create_task(heartbeat())
        done, _pending = await asyncio.wait({operation_task, heartbeat_task}, return_when=asyncio.FIRST_COMPLETED)
        if heartbeat_task in done:
            operation_task.cancel()
            await asyncio.gather(operation_task, return_exceptions=True)
            # A heartbeat task only completes by raising or cancellation.
            return heartbeat_task.result()
        heartbeat_task.cancel()
        await asyncio.gather(heartbeat_task, return_exceptions=True)
        self.database.renew_lease("production", self._lease_token, self.config.lease_seconds)
        return operation_task.result()

    def _reservation(self, role: str, side_effect: str) -> int:
        if role == "fal_pixverse_transition":
            if side_effect == "fal_transition_submit":
                return self.config.draft_automation.fal_transition_reservation_micros()
            if side_effect == "fal_transition_result":
                return 0
            raise ValueError(f"unsupported Fal journal side effect {side_effect!r}")
        if not hasattr(self.config.models, role):
            return 0
        model = getattr(self.config.models, role)
        text_reservation = (
            model.max_input_tokens * model.cost_per_million_input_micros
            + model.max_output_tokens * model.cost_per_million_output_micros
        ) // 1_000_000
        if "image" in side_effect or "asset" in side_effect:
            return model.cost_per_image_micros + text_reservation
        # Reserve the pinned model's full context/output ceilings. Exact,
        # complete provider usage replaces it after the response; absent or
        # malformed usage leaves the conservative reservation charged.
        return text_reservation

    def _check_budget(self, attempt: dict[str, Any] | None, reserve: int) -> None:
        budgets = self.config.budgets
        if (
            attempt
            and self.database.cost_exposure(attempt_id=attempt["id"]) + reserve > budgets.max_cost_micros_per_attempt
        ):
            raise BudgetExceeded("per-attempt cost cap reached")
        today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        if self.database.cost_exposure(since=today) + reserve > budgets.max_cost_micros_per_day:
            raise BudgetExceeded("daily cost cap reached")
        if self.database.cost_exposure() + reserve > budgets.max_cost_micros_program:
            raise BudgetExceeded("program cost cap reached")

    def _model_result(
        self,
        operation: dict[str, Any],
        result: ProviderResult | None,
        model: type[BaseModel],
    ) -> Any:
        if result is not None:
            value = result.value
            if isinstance(value, model):
                return value
        else:
            value = self.persistence.load_json(operation["result_hash"])
        return model.model_validate(value)

    def _json_result(self, operation: dict[str, Any], result: ProviderResult | None) -> dict[str, Any]:
        value = result.value if result is not None else self.persistence.load_json(operation["result_hash"])
        if hasattr(value, "model_dump"):
            value = value.model_dump(mode="json", by_alias=True)
        if not isinstance(value, dict):
            raise ValueError(f"operation {operation['id']} did not return a JSON object")
        return value

    def _image_result(self, operation: dict[str, Any], result: ProviderResult | None) -> tuple[bytes, str]:
        try:
            metadata = json.loads(operation["metadata_json"])
            media_type = metadata["result"]["media_type"]
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError(f"operation {operation['id']} has no retained image media type") from error
        if media_type not in SUPPORTED_IMAGE_MEDIA_TYPES:
            raise ValueError(f"operation {operation['id']} retained unsupported image media type {media_type!r}")
        if result is None:
            return self.objects.get(operation["result_hash"]), media_type
        value = result.value
        if not isinstance(value, dict) or not isinstance(value.get("image"), bytes):
            raise ValueError(f"operation {operation['id']} did not return image bytes")
        if value.get("media_type") != media_type:
            raise ValueError(f"operation {operation['id']} image media type differs from retained metadata")
        return value["image"], media_type

    async def _judge(
        self,
        attempt: dict[str, Any],
        artifact: dict[str, Any],
        *,
        system: str,
        context: dict[str, Any],
        key: str,
        comparison: dict[str, Any] | None = None,
    ) -> tuple[VisualJudgment, dict[str, Any]]:
        images = [(artifact["media_type"], self.objects.get(artifact["object_ref"]))]
        if comparison:
            images.insert(0, (comparison["media_type"], self.objects.get(comparison["object_ref"])))
        provider = self.providers.role("visual_judge")
        operation, result = await self._provider_call(
            attempt=self.database.get_attempt(attempt["id"]),
            stage=Stage(attempt["stage"]),
            key=key,
            role="visual_judge",
            side_effect="visual_judgment",
            request={
                "artifact": artifact["content_hash"],
                "comparison": comparison["content_hash"] if comparison else None,
                "context": context,
            },
            invoke=lambda: provider.generate_structured(
                system=system,
                prompt=canonical_json(context),
                schema=VisualJudgment,
                images=images,
                temperature=0.2,
            ),
        )
        judgment = self._model_result(operation, result, VisualJudgment)
        self._store_json_artifact(
            attempt,
            Stage(attempt["stage"]),
            ArtifactKind.PROVIDER_RESPONSE,
            judgment.model_dump(mode="json"),
            metadata={"operation_id": operation["id"], "artifact_id": artifact["id"]},
        )
        return judgment, operation

    def _store_json_artifact(
        self,
        attempt: dict[str, Any],
        stage: Stage | str,
        kind: ArtifactKind,
        value: Any,
        *,
        metadata: dict[str, Any] | None = None,
        provenance: dict[str, Any] | None = None,
        occurrence_key: str | None = None,
    ) -> dict[str, Any]:
        if hasattr(value, "model_dump"):
            value = value.model_dump(mode="json", by_alias=True)
        data = canonical_json(value).encode("utf-8")
        return self._store_bytes_artifact(
            attempt,
            stage,
            kind,
            data,
            "application/json",
            metadata=metadata,
            provenance=provenance,
            occurrence_key=occurrence_key,
        )

    def _store_bytes_artifact(
        self,
        attempt: dict[str, Any],
        stage: Stage | str,
        kind: ArtifactKind,
        data: bytes,
        media_type: str,
        *,
        metadata: dict[str, Any] | None = None,
        provenance: dict[str, Any] | None = None,
        occurrence_key: str | None = None,
    ) -> dict[str, Any]:
        stored = self.objects.put(data)
        return self.database.add_artifact(
            attempt_id=attempt["id"],
            stage=str(stage),
            kind=kind,
            content_hash=stored.uri,
            object_ref=stored.uri,
            media_type=media_type,
            size_bytes=stored.size,
            metadata=metadata,
            provenance=provenance,
            occurrence_key=occurrence_key,
        )

    def _lineage(self, attempt: dict[str, Any]) -> list[dict[str, Any]]:
        lineage = [attempt]
        seen = {attempt["id"]}
        while lineage[-1].get("parent_attempt_id"):
            parent_id = lineage[-1]["parent_attempt_id"]
            if parent_id in seen:
                raise RuntimeError("attempt lineage contains a cycle")
            parent = self.database.get_attempt(parent_id)
            lineage.append(parent)
            seen.add(parent_id)
            if len(lineage) > 100:
                raise RuntimeError("attempt lineage exceeds safety bound")
        return lineage

    def _find_lineage_artifact(
        self,
        attempt: dict[str, Any],
        kind: ArtifactKind,
        *,
        content_hash: str | None = None,
    ) -> dict[str, Any] | None:
        for candidate in self._lineage(attempt):
            artifacts = self.database.artifacts_for_attempt(candidate["id"], kind=kind)
            for artifact in reversed(artifacts):
                if content_hash is None or artifact["content_hash"] == content_hash:
                    return artifact
        return None

    def _find_current_artifact(
        self,
        attempt_id: str,
        kind: ArtifactKind,
        *,
        metadata_match: dict[str, Any],
    ) -> dict[str, Any] | None:
        for artifact in reversed(self.database.artifacts_for_attempt(attempt_id, kind=kind)):
            metadata = json.loads(artifact["metadata_json"])
            if all(metadata.get(key) == value for key, value in metadata_match.items()):
                return artifact
        return None

    def _prototype_manifest_for(self, attempt: dict[str, Any], prototype: dict[str, Any]) -> dict[str, Any] | None:
        for candidate in self._lineage(attempt):
            manifests = self.database.artifacts_for_attempt(candidate["id"], kind=ArtifactKind.PROTOTYPE_MANIFEST)
            for manifest in reversed(manifests):
                metadata = json.loads(manifest["metadata_json"])
                if metadata.get("image_artifact_id") == prototype["id"]:
                    return manifest
                payload = self.persistence.load_json(manifest["object_ref"])
                if payload.get("image_sha256") == prototype["content_hash"]:
                    return manifest
        return None

    def _lineage_feedback(self, attempt: dict[str, Any]) -> list[str]:
        feedback: list[str] = []
        for candidate in reversed(self._lineage(attempt)):
            for decision in self.database.decisions_for_attempt(candidate["id"]):
                if decision["feedback"].strip():
                    feedback.append(decision["feedback"].strip())
        return feedback[-20:]

    def _stage_feedback(self, attempt: dict[str, Any], target: str) -> list[str]:
        """Select literal human corrections relevant to one regenerated stage."""

        if target not in {"prototype", "assets"}:
            raise ValueError(f"unknown feedback target {target}")
        values: list[str] = []
        for candidate in reversed(self._lineage(attempt)):
            for decision in self.database.decisions_for_attempt(candidate["id"]):
                value = str(decision["feedback"]).strip()
                if not value:
                    continue
                action = str(decision["action"])
                tags = {str(tag) for tag in json.loads(decision["tags_json"])}
                relevant = False
                if action == "retry":
                    relevant = (
                        "from:prototype" in tags
                        if target == "prototype"
                        else bool(tags & {"from:assets", "from:build"})
                    )
                elif target == "prototype":
                    relevant = action in {
                        "prototype_label",
                        "human_rejection",
                        "soft_triage_override",
                        "feedback_only",
                    }
                else:
                    relevant = action in {
                        "build_quality_label",
                        "human_rejection",
                        "soft_triage_override",
                        "feedback_only",
                    }
                if relevant and value not in values:
                    values.append(value)
        return values[-12:]

    def _concept_field(self, attempt: dict[str, Any], name: str, fallback: str) -> str:
        artifact = self._find_lineage_artifact(attempt, ArtifactKind.CONCEPT_BRIEF)
        if artifact:
            value = self.persistence.load_json(artifact["object_ref"])
            if isinstance(value.get(name), str):
                return value[name]
        return fallback

    @staticmethod
    def _prototype_image_rules(guidelines: str) -> str:
        if guidelines.count(_PROTOTYPE_IMAGE_RULES_START) != 1 or guidelines.count(_PROTOTYPE_IMAGE_RULES_END) != 1:
            raise ValueError("pinned design guidelines must have one prototype-image rules boundary")
        start = guidelines.index(_PROTOTYPE_IMAGE_RULES_START) + len(_PROTOTYPE_IMAGE_RULES_START)
        end = guidelines.index(_PROTOTYPE_IMAGE_RULES_END)
        if start >= end:
            raise ValueError("pinned prototype-image rules boundary is malformed")
        rules = guidelines[start:end].strip()
        if not rules:
            raise ValueError("pinned prototype-image rules are empty")
        return rules

    @staticmethod
    def _prototype_geometry_prompt_values(contract: dict[str, Any]) -> dict[str, int | str]:
        def mapping(value: Any, name: str) -> dict[str, Any]:
            if not isinstance(value, dict):
                raise ValueError(f"pinned prototype geometry has no {name}")
            return value

        def positive_integer(value: Any, name: str) -> int:
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"pinned prototype geometry {name} must be a positive integer")
            return value

        contract_id = contract.get("id")
        if not isinstance(contract_id, str) or not contract_id:
            raise ValueError("pinned prototype geometry has no id")
        source = mapping(contract.get("renderer_source"), "renderer_source")
        body_cells = positive_integer(source.get("body_cells"), "renderer_source.body_cells")
        native_cell_px = positive_integer(source.get("native_cell_px"), "renderer_source.native_cell_px")
        if source.get("head_direction") != "right":
            raise ValueError("pinned prototype geometry head_direction must be right")
        transform = mapping(contract.get("presentation_transform"), "presentation_transform")
        if transform.get("type") != "nearest_neighbor_integer_upscale":
            raise ValueError("pinned prototype geometry presentation transform must be nearest-neighbor")
        presentation_scale = positive_integer(transform.get("scale"), "presentation_transform.scale")
        projection = mapping(contract.get("prototype_projection"), "prototype_projection")
        projection_mapping = mapping(projection.get("mapping"), "prototype_projection.mapping")
        body_bbox = projection_mapping.get("native_body_bbox_px")
        if (
            not isinstance(body_bbox, list)
            or len(body_bbox) != 4
            or any(isinstance(value, bool) or not isinstance(value, int) for value in body_bbox)
        ):
            raise ValueError("pinned prototype geometry native_body_bbox_px must contain four integers")
        native_body_width = body_bbox[2] - body_bbox[0]
        native_body_height = body_bbox[3] - body_bbox[1]
        if (native_body_width, native_body_height) != (body_cells * native_cell_px, native_cell_px):
            raise ValueError("pinned prototype geometry body box is not exactly body_cells by one square cell")
        return {
            "contract_id": contract_id,
            "body_cells": body_cells,
            "native_cell_px": native_cell_px,
            "native_body_width": native_body_width,
            "native_body_height": native_body_height,
            "presentation_scale": presentation_scale,
            "presentation_cell_px": native_cell_px * presentation_scale,
            "presentation_body_width": native_body_width * presentation_scale,
            "presentation_body_height": native_body_height * presentation_scale,
        }

    def _prototype_prompt(
        self,
        attempt: dict[str, Any],
        concept: dict[str, Any],
        index: int,
    ) -> str:
        guidelines = self.pinned_design_guidelines(attempt).decode("utf-8")
        geometry = self.pinned_prototype_geometry(attempt)
        if geometry is None:
            raise ValueError("prototype image generation requires pinned renderer geometry and guide bytes")
        values = self._prototype_geometry_prompt_values(geometry[0])
        rules = self._prototype_image_rules(guidelines)
        behavior = json.loads(attempt["behavior_json"])
        design_sha = behavior.get("design_guidelines_sha")
        geometry_sha = behavior.get("prototype_geometry_sha")
        guide_sha = behavior.get("prototype_guide_sha")
        for name, digest in (
            ("design guidelines", design_sha),
            ("prototype geometry", geometry_sha),
            ("prototype guide", guide_sha),
        ):
            if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                raise ValueError(f"pinned {name} authority hash is malformed")
        feedback = self._stage_feedback(attempt, "prototype")
        feedback_block = (
            "\nHUMAN RETRY CORRECTIONS\n"
            "These are literal JSON strings. Apply them only when they do not conflict with GEOMETRY.\n"
            f"{canonical_json(feedback)}\n"
            if feedback
            else ""
        )
        return f"""GEOMETRY — HIGHEST PRIORITY
The attached PNG is the strict repository-owned body guide. Paint the existing
white capsule; do not redesign its outline. Geometry contract
`{values["contract_id"]}` is pinned as sha256:{geometry_sha}; the attached guide
is pinned as sha256:{guide_sha}.

- The body occupies an invisible grid of exactly {values["body_cells"]} columns x 1 row of square occupancy cells.
- Every logical cell is square: {values["native_cell_px"]}x{values["native_cell_px"]} px in the native renderer.
- The complete native body is exactly {values["native_body_width"]}x{values["native_body_height"]} px.
- In the attached {values["presentation_scale"]}x reference, every cell is
  {values["presentation_cell_px"]}x{values["presentation_cell_px"]} px and the body is exactly
  {values["presentation_body_width"]}x{values["presentation_body_height"]} px.
- A cell is a measurement only. Do not draw the grid or turn cells into visible
  squares, plates, panels, diamonds, joints, or separate segments.
- Paint one continuous, flat, right-facing capsule. The rightmost one-cell
  position is the rounded head; the left endpoint is the rounded tail.
- Keep the small centered head-core disc exactly where the guide puts it. Do not
  enlarge the head beyond one cell or point the tail.
- Paint only inside the white body. No gaps, articulated modules, perspective,
  3D geometry, shadows outside the body, or other silhouettes.
- This is the fixed prototype review pose. Use exactly this length and
  silhouette. Do not depict any alternative live length.

CREATIVE BRIEF
Create prototype variation {index + 1} for this Snaketron skin. Geometry wins
over every creative instruction if they conflict.
Concept brief (literal JSON string):
{canonical_json(concept["brief"])}
SHARED SKIN DESIGN RULES
The following is the exact image-relevant excerpt of the shared rules used by
the downstream skin author. Pinned authority: sha256:{design_sha}.
{rules}
{feedback_block}
RETURN ONE PROTOTYPE IMAGE
Apply the brief and shared rules to the exact guide. Return the image only.
This is a constrained small-scale skin reference, not poster art."""

    @staticmethod
    def _nearest_provider_aspect(width: int, height: int) -> tuple[str, float]:
        ratio = width / height
        aspect = min(
            _PROVIDER_IMAGE_ASPECTS,
            key=lambda key: abs(math.log(ratio / _PROVIDER_IMAGE_ASPECTS[key])),
        )
        return aspect, abs(math.log(ratio / _PROVIDER_IMAGE_ASPECTS[aspect]))

    @classmethod
    def _asset_image_slices(cls, asset: AssetPlan) -> list[dict[str, Any]]:
        """Map a tall sheet to contiguous provider-native frame ranges.

        The supported image APIs expose a small fixed aspect-ratio set.  A
        full sheet uses one call when it maps within the bounded correction;
        otherwise the largest close temporal slice is chosen repeatedly.
        """

        total_rows = asset.frames if asset.kind == "sheet" else 1
        width = asset.natural_length_cells
        full_aspect, full_error = cls._nearest_provider_aspect(width, total_rows)
        if asset.kind != "sheet" or full_error <= _PROVIDER_SLICE_ASPECT_LOG_TOLERANCE:
            return [
                {
                    "slice_index": 0,
                    "start_frame": 0,
                    "end_frame": total_rows,
                    "frame_rows": total_rows,
                    "aspect_ratio": full_aspect,
                }
            ]

        # Vertical slicing cannot improve an already-too-wide grid.  Retain
        # the ordinary single-call behavior for that uncommon case.
        if width / total_rows > max(_PROVIDER_IMAGE_ASPECTS.values()):
            return [
                {
                    "slice_index": 0,
                    "start_frame": 0,
                    "end_frame": total_rows,
                    "frame_rows": total_rows,
                    "aspect_ratio": full_aspect,
                }
            ]

        slices: list[dict[str, Any]] = []
        start = 0
        while start < total_rows:
            remaining = total_rows - start
            candidates: list[tuple[int, str, float]] = []
            for rows in range(1, remaining + 1):
                aspect, error = cls._nearest_provider_aspect(width, rows)
                if error <= _PROVIDER_SLICE_ASPECT_LOG_TOLERANCE:
                    candidates.append((rows, aspect, error))
            if not candidates:
                # This can only be a short, too-wide tail.  Keeping it intact
                # is deterministic and never increases the number of calls.
                rows = remaining
                aspect, _error = cls._nearest_provider_aspect(width, rows)
            else:
                rows, aspect, _error = max(candidates, key=lambda candidate: candidate[0])
            end = start + rows
            slices.append(
                {
                    "slice_index": len(slices),
                    "start_frame": start,
                    "end_frame": end,
                    "frame_rows": rows,
                    "aspect_ratio": aspect,
                }
            )
            start = end
        return slices

    def _validate_asset_image_call_budget(self, plan: ImplementationPlan) -> None:
        direct_indexes = (
            {item.asset_index for item in plan.modifier_plan if item.source_mode == "direct_generate"}
            if plan.modifier_plan
            else set(range(len(plan.asset_plan)))
        )
        slice_counts = [
            (index, len(self._asset_image_slices(asset)))
            for index, asset in enumerate(plan.asset_plan)
            if index in direct_indexes
        ]
        per_asset = self.config.budgets.max_image_slices_per_asset
        for index, count in slice_counts:
            if count > per_asset:
                raise ValueError(f"asset {index} needs {count} slices; configured maximum is {per_asset}")
        # The same configured retry count bounds both known-safe transport
        # retries and image regeneration after a strict deterministic reject.
        rounds = self.config.budgets.provider_retries + 1
        worst_case_calls = sum(count for _index, count in slice_counts) * rounds * rounds
        maximum = self.config.budgets.max_asset_image_calls_per_attempt
        if worst_case_calls > maximum:
            raise ValueError(f"worst-case image calls {worst_case_calls} exceed configured attempt maximum {maximum}")

    def _planned_asset_image_reservation(
        self,
        plan: ImplementationPlan,
        *,
        attempt: dict[str, Any] | None = None,
    ) -> int:
        """Conservatively price every still-possible direct image call."""

        direct_indexes = (
            {item.asset_index for item in plan.modifier_plan if item.source_mode == "direct_generate"}
            if plan.modifier_plan
            else set(range(len(plan.asset_plan)))
        )
        if attempt is not None:
            direct_indexes = {
                index
                for index in direct_indexes
                if self._find_current_artifact(
                    attempt["id"],
                    ArtifactKind.FORGE_MANIFEST,
                    metadata_match={"asset_index": index, "uploaded": True},
                )
                is None
            }
        slice_count = sum(
            len(self._asset_image_slices(asset))
            for index, asset in enumerate(plan.asset_plan)
            if index in direct_indexes
        )
        rounds = self.config.budgets.provider_retries + 1
        worst_case_calls = slice_count * rounds * rounds
        per_call = self._reservation("image_generator", "generate_build_asset")
        return worst_case_calls * per_call

    async def _generate_asset_provider_source(
        self,
        *,
        attempt: dict[str, Any],
        asset: AssetPlan,
        asset_index: int,
        generation: int,
        prompt: str,
        prototype: dict[str, Any],
        prototype_bytes: bytes,
    ) -> tuple[bytes, dict[str, Any]]:
        """Generate one exact forge input, batching only extreme tall sheets."""

        slices = self._asset_image_slices(asset)
        maximum = self.config.budgets.max_image_slices_per_asset
        if len(slices) > maximum:
            raise ValueError(f"asset {asset_index} needs {len(slices)} slices; configured maximum is {maximum}")
        provider = self.providers.role("image_generator")
        # Static coats/overlays keep the original one-call path. Every sheet,
        # including an ordinary one-slice sheet, uses the no-crop temporal
        # normalizer below; only extreme tall sheets multiply provider calls.
        if asset.kind != "sheet":
            aspect = str(slices[0]["aspect_ratio"])
            request = {
                "asset_index": asset_index,
                "generation": generation,
                "prompt": prompt,
                "prototype": prototype["content_hash"],
                "aspect_ratio": aspect,
                "image_size": "2K",
            }

            operation, result = await self._provider_call(
                attempt=self.database.get_attempt(attempt["id"]),
                stage=Stage.ASSETS,
                key=f"{attempt['id']}:asset:{asset_index}:generation:{generation}",
                role="image_generator",
                side_effect="generate_build_asset",
                request=request,
                invoke=lambda: provider.generate_image(
                    prompt=prompt,
                    references=[(prototype["media_type"], prototype_bytes)],
                    aspect_ratio=aspect,
                    image_size="2K",
                ),
            )
            raw, media_type = self._image_result(operation, result)
            artifact = self._store_bytes_artifact(
                attempt,
                Stage.ASSETS,
                ArtifactKind.SOURCE_ASSET,
                raw,
                media_type,
                metadata={
                    "asset_index": asset_index,
                    "generation": generation,
                    "phase": "provider_output",
                    "prompt": prompt,
                    "aspect_ratio": aspect,
                    "image_size": "2K",
                    "slice_count": 1,
                },
                provenance={"operation_id": operation["id"]},
                occurrence_key=f"asset:{asset_index}:generation:{generation}:provider-output",
            )
            return raw, artifact

        normalized_slices: list[bytes] = []
        normalized_artifacts: list[dict[str, Any]] = []
        raw_artifacts: list[dict[str, Any]] = []
        operation_ids: list[str] = []
        for spec in slices:
            slice_index = int(spec["slice_index"])
            start = int(spec["start_frame"])
            end = int(spec["end_frame"])
            frame_rows = int(spec["frame_rows"])
            aspect = str(spec["aspect_ratio"])
            continuity_artifacts: list[dict[str, Any]] = []
            if normalized_artifacts:
                continuity_artifacts.append(normalized_artifacts[-1])
            if end == asset.frames and normalized_artifacts:
                first = normalized_artifacts[0]
                if all(candidate["id"] != first["id"] for candidate in continuity_artifacts):
                    continuity_artifacts.append(first)
            continuity_refs = [candidate["content_hash"] for candidate in continuity_artifacts]
            slice_prompt = self._asset_slice_prompt(
                prompt,
                asset=asset,
                slice_index=slice_index,
                slice_count=len(slices),
                start_frame=start,
                end_frame=end,
                has_previous=bool(normalized_artifacts),
            )
            request = {
                "asset_index": asset_index,
                "generation": generation,
                "prompt": slice_prompt,
                "prototype": prototype["content_hash"],
                "slice": {
                    **spec,
                    "slice_count": len(slices),
                    "full_frame_rows": asset.frames,
                    "body_columns": asset.natural_length_cells,
                },
                "continuity_refs": continuity_refs,
                "aspect_ratio": aspect,
                "image_size": "2K",
            }
            references = [(prototype["media_type"], prototype_bytes)] + [
                ("image/png", self.objects.get(candidate["object_ref"])) for candidate in continuity_artifacts
            ]

            operation, result = await self._provider_call(
                attempt=self.database.get_attempt(attempt["id"]),
                stage=Stage.ASSETS,
                key=(f"{attempt['id']}:asset:{asset_index}:generation:{generation}:slice:{slice_index}:{start}-{end}"),
                role="image_generator",
                side_effect="generate_build_asset_slice",
                request=request,
                invoke=lambda slice_prompt=slice_prompt, references=references, aspect=aspect: provider.generate_image(
                    prompt=slice_prompt,
                    references=references,
                    aspect_ratio=aspect,
                    image_size="2K",
                ),
            )
            raw, media_type = self._image_result(operation, result)
            raw_artifact = self._store_bytes_artifact(
                attempt,
                Stage.ASSETS,
                ArtifactKind.SOURCE_ASSET,
                raw,
                media_type,
                metadata={
                    "asset_index": asset_index,
                    "generation": generation,
                    "phase": "provider_slice_output",
                    "prompt": slice_prompt,
                    "slice": request["slice"],
                    "continuity_refs": continuity_refs,
                    "aspect_ratio": aspect,
                    "image_size": "2K",
                },
                provenance={"operation_id": operation["id"]},
                occurrence_key=(
                    f"asset:{asset_index}:generation:{generation}:slice:{slice_index}:{start}-{end}:provider-output"
                ),
            )
            normalized = self.assets.normalize_sheet_slice(
                raw,
                body_columns=asset.natural_length_cells,
                frame_rows=frame_rows,
                texels_per_cell=asset.texels_per_cell,
                raster_overhang_px=asset.raster_overhang_px,
            )
            normalized_artifact = self._store_bytes_artifact(
                attempt,
                Stage.ASSETS,
                ArtifactKind.SOURCE_ASSET,
                normalized,
                "image/png",
                metadata={
                    "asset_index": asset_index,
                    "generation": generation,
                    "phase": "normalized_provider_slice",
                    "slice": request["slice"],
                    "provider_artifact_id": raw_artifact["id"],
                    "target_width_px": asset.natural_length_cells * asset.texels_per_cell,
                    "target_height_px": frame_rows * asset_row_texels(asset),
                    "normalization": "no_crop_direct_resize",
                },
                provenance={"operation_id": operation["id"], "provider_artifact_id": raw_artifact["id"]},
                occurrence_key=(
                    f"asset:{asset_index}:generation:{generation}:slice:{slice_index}:{start}-{end}:normalized"
                ),
            )
            raw_artifacts.append(raw_artifact)
            normalized_artifacts.append(normalized_artifact)
            normalized_slices.append(normalized)
            operation_ids.append(operation["id"])

        self._assert_wall_time_before_spend()
        assembled = self.assets.assemble_sheet_slices(normalized_slices, asset)
        assembled_artifact = self._store_bytes_artifact(
            attempt,
            Stage.ASSETS,
            ArtifactKind.SOURCE_ASSET,
            assembled,
            "image/png",
            metadata={
                "asset_index": asset_index,
                "generation": generation,
                "phase": "assembled_provider_output",
                "prompt": prompt,
                "slice_count": len(slices),
                "frame_ranges": [[spec["start_frame"], spec["end_frame"]] for spec in slices],
                "provider_artifact_ids": [artifact["id"] for artifact in raw_artifacts],
                "normalized_slice_artifact_ids": [artifact["id"] for artifact in normalized_artifacts],
                "width_px": asset.natural_length_cells * asset.texels_per_cell,
                "height_px": asset.frames * asset_row_texels(asset),
                "assembly": "vertical_exact_no_crop",
            },
            provenance={
                "operation_ids": operation_ids,
                "provider_slice_refs": [artifact["content_hash"] for artifact in raw_artifacts],
                "normalized_slice_refs": [artifact["content_hash"] for artifact in normalized_artifacts],
            },
            occurrence_key=f"asset:{asset_index}:generation:{generation}:assembled-provider-output",
        )
        return assembled, assembled_artifact

    @staticmethod
    def _asset_slice_prompt(
        prompt: str,
        *,
        asset: AssetPlan,
        slice_index: int,
        slice_count: int,
        start_frame: int,
        end_frame: int,
        has_previous: bool,
    ) -> str:
        row_count = end_frame - start_frame
        continuity = (
            "The immediately preceding normalized time slice is supplied after the prototype; "
            "its last row must flow directly into this slice's first row."
            if has_previous
            else "Global row 0 is the valid resting and reduced-motion frame."
        )
        loop = (
            "This is the final slice: its last row must flow cleanly back into global row 0, "
            "whose first slice is also supplied when distinct from the preceding slice."
            if end_frame == asset.frames
            else "Its last row must anticipate the next global frame without a jump."
        )
        return f"""{prompt}

Bounded sprite generation call {slice_index + 1} of {slice_count}. For this call,
emit only the {asset.natural_length_cells}-column by {row_count}-row cell grid for
global animation frames [{start_frame}, {end_frame}) out of [0, {asset.frames}).
Do not emit, miniaturize, stack, label, border, or summarize any frames outside
that exact half-open range. {continuity} {loop} Preserve the same snake,
palette, lighting, cell registration, and motion phase across every slice."""

    @staticmethod
    def _asset_prompt(asset: Any, base: str, rejection: str, human_feedback: list[str]) -> str:
        grid = (
            f"The output is an X-by-Y sprite grid with X={asset.natural_length_cells} body "
            f"cells and Y={asset.frames} animation rows. Row zero is a valid resting and "
            "reduced-motion frame."
            if asset.kind == "sheet"
            else f"The natural authored body length is {asset.natural_length_cells or 1} cells."
        )
        seams = []
        if asset.kind == "sheet":
            seams.append("vertical/time")
        if asset.fit == "tile":
            seams.append("horizontal/body")
        retry = f"\nPrevious strict-gate rejection to correct: {rejection}" if rejection else ""
        corrections = (
            "\nLiteral human corrections for this retry (JSON strings; preserve meaning): "
            + canonical_json(human_feedback)
            if human_feedback
            else ""
        )
        return f"""{base}

Generate only the final texture pixels, without labels or UI. {grid}
The exact forge density is {asset.texels_per_cell} texels per cell. Required
seamless axes: {", ".join(seams) if seams else "none"}. Preserve cell
alignment, temporal continuity, a clean loop, and fidelity to the supplied
approved prototype. The deterministic driver will crop/resample to the exact
grid and then measure the bytes.{corrections}{retry}"""

    @staticmethod
    def _asset_aspect(asset: Any) -> str:
        width = asset.natural_length_cells or 1
        height = asset.frames if asset.kind == "sheet" else 1
        return Factory._nearest_provider_aspect(width, height)[0]

    @staticmethod
    def _bind_textures(
        document: dict[str, Any],
        plan: ImplementationPlan,
        tool_requests: list[dict[str, Any]],
        manifests: list[dict[str, Any]],
    ) -> dict[str, Any]:
        cloned = json.loads(json.dumps(document))
        textures = cloned.get("textures", [])
        if len(manifests) != len(plan.asset_plan):
            raise ValueError("generated manifest count differs from asset plan")
        claimed: set[int] = set()
        modifiers = {item.asset_index: item for item in plan.modifier_plan}
        for index, (asset, manifest) in enumerate(zip(plan.asset_plan, manifests, strict=True)):
            request = next(
                (
                    item
                    for item in tool_requests
                    if item.get("kind") == "generate_asset"
                    and int(item.get("arguments", {}).get("asset_index", -1)) == index
                ),
                None,
            )
            modifier = modifiers.get(index)
            texture_name = request.get("arguments", {}).get("texture_name") if request else None
            if texture_name is None and modifier is not None:
                texture_name = modifier.texture_name
            if texture_name:
                matches = [i for i, item in enumerate(textures) if item.get("name") == texture_name]
            else:
                matches = [i for i, item in enumerate(textures) if i not in claimed and item.get("kind") == asset.kind]
            if not matches:
                raise ValueError(f"asset {index} has no matching SkinDoc texture entry")
            texture_index = matches[0]
            claimed.add(texture_index)
            textures[texture_index]["ref"] = manifest["content_ref"]
            textures[texture_index]["kind"] = asset.kind
            textures[texture_index]["descriptor"] = manifest["descriptor"]
        return cloned

    def _transition_to_review(self, attempt: dict[str, Any], **fields: Any) -> dict[str, Any]:
        """Commit review state and its delivery intent in one SQLite transaction."""

        review_kind = fields.get("review_kind")
        if not review_kind:
            raise ValueError("review transition requires a review_kind")
        payload = {
            "attempt_id": attempt["id"],
            "concept_id": attempt["concept_id"],
            "review_kind": review_kind,
        }
        stored = self.objects.put(canonical_json(payload).encode())
        updated, _message = self.database.update_attempt_with_outbox(
            attempt["id"],
            attempt["version"],
            outbox_idempotency_key=f"review:{attempt['id']}:{review_kind}",
            outbox_destination="review_gallery",
            outbox_event_ref=attempt["id"],
            outbox_payload_ref=stored.uri,
            outbox_payload_hash=stored.sha256,
            **fields,
        )
        return updated

    def _reject_attempt(self, attempt: dict[str, Any], reason: str) -> dict[str, Any]:
        current = self.database.get_attempt(attempt["id"])
        return self.database.update_attempt(
            current["id"],
            current["version"],
            disposition=Disposition.MACHINE_REJECTED,
            failure_json={"stage": current["stage"], "reason": reason},
            review_kind=None,
        )

    def _block_attempt(
        self,
        attempt: dict[str, Any],
        reason: str,
        *,
        program_halt: str | None = None,
        evidence: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        current = self.database.get_attempt(attempt["id"])
        failure: dict[str, Any] = {"stage": current["stage"], "reason": reason}
        if program_halt is not None:
            failure["program_halt"] = program_halt
        if evidence:
            failure["evidence"] = evidence
        return self.database.update_attempt(
            current["id"],
            current["version"],
            disposition=Disposition.BLOCKED,
            failure_json=failure,
            review_kind=None,
        )


def _runtime_sha(lama_bundle: dict[str, Any] | None = None) -> str:
    """Hash factory sources and, for v5 snapshots, the local repair bundle."""

    root = Path(__file__).resolve().parent
    digest = hashlib.sha256()
    for path in sorted(root.glob("*.py")):
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    if lama_bundle is not None:
        digest.update(b"lama-bundle\0")
        digest.update(lama_bundle_sha(lama_bundle).encode("ascii"))
        digest.update(b"\0")
    return digest.hexdigest()


def _git_commit(repo: Path) -> str:
    import subprocess

    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        capture_output=True,
        text=True,
        timeout=10,
        check=True,
    )
    return completed.stdout.strip()
