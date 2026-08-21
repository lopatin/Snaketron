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
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from PIL import Image
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
)
from .gates import GateRunner
from .lama import LamaRuntimeError, lama_bundle_manifest, lama_bundle_sha
from .objects import ObjectStore
from .operations import ExistingOperation, OperationJournal
from .outbox import OutboxDispatcher
from .persistence import SUPPORTED_IMAGE_MEDIA_TYPES, ResultPersistence
from .providers import ProviderRegistry
from .recovery import RecoveredResultError, validate_recovered_result, validate_skin_authority_readback
from .renderer import (
    BrowserRenderer,
    RendererDrift,
    renderer_bundle_manifest_sha,
    renderer_execution_config,
    renderer_execution_config_sha,
)
from .snaketron_api import SnaketronApi
from .worker import SkillBundle, WorkerAdapter, build_worker
from .worker_validation import (
    WorkerContractError,
    assert_resolved_document,
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
    ) -> None:
        self.config = config
        self.database = database or Database(config.paths.database)
        self.database.migrate()
        self.objects = objects or ObjectStore(config.paths.objects)
        self.providers = providers or ProviderRegistry(config)
        self.worker = worker or build_worker(config)
        self.api = api or SnaketronApi(config)
        self.renderer = renderer or BrowserRenderer(config)
        self.assets = AssetProcessor(config)
        self.gates = GateRunner(config)
        self.journal = OperationJournal(self.database)
        self.persistence = ResultPersistence(self.objects)
        self.calibration = JudgeCalibrationService(self.database, config)
        self._lease_token: str | None = None
        self._run_deadline: float | None = None

    async def close(self) -> None:
        for owner in (self.providers, self.worker, self.api):
            close = getattr(owner, "close", None)
            if close:
                result = close()
                if hasattr(result, "__await__"):
                    await result

    def behavior_snapshot(self) -> dict[str, Any]:
        skill, skill_git_ref, skill_git_sha = self.active_skill_bundle()
        direction = self.config.paths.direction.read_bytes()
        capability = self.config.paths.capability_manifest.read_bytes()
        gates = self.config.paths.gate_manifest.read_bytes()
        model_config = self._model_config_bytes()
        direction_object = self.objects.put(direction)
        capability_object = self.objects.put(capability)
        gate_object = self.objects.put(gates)
        model_config_object = self.objects.put(model_config)
        renderer_config = renderer_execution_config(self.config)
        lama_bundle = lama_bundle_manifest(self.config)
        return {
            "snapshot_version": 5,
            "direction_sha": direction_object.sha256,
            "direction_ref": direction_object.uri,
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
            attempt = self.database.next_active_attempt()
            if attempt is None:
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
                if attempt["stage"] in {
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
                if attempt["stage"] in {
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
                try:
                    attempt = await self._advance(attempt)
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
                report["advanced"].append(advancement)
                if after == before:
                    break
                self.database.renew_lease("production", token, self.config.lease_seconds)
            if self._wall_time_exhausted():
                report["halt"] = {"reason": "run_wall_time_budget"}

            # Optimizer readiness is part of this same scheduled command. The
            # optimizer module advances at most one resumable job and is loaded
            # lazily so ordinary production does not require DSPy import time.
            if self.config.optimizer.enabled and not self._wall_time_exhausted():
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

    def _generation_halt(self) -> str | None:
        detail = self._generation_halt_detail()
        return str(detail["reason"]) if detail is not None else None

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
        for index in range(self.config.budgets.prototypes_per_attempt):
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
            except (FileNotFoundError, RuntimeError, UnicodeDecodeError, ValueError) as error:
                return self._block_attempt(attempt, str(error))
            request = {"prompt": prompt, "aspect_ratio": "16:9", "image_size": "2K"}
            operation, result = await self._provider_call(
                attempt=self.database.get_attempt(attempt["id"]),
                stage=Stage.PROTOTYPE,
                key=f"{attempt['id']}:prototype:{index}:v1",
                role="image_generator",
                side_effect="generate_prototype_image",
                request=request,
                invoke=lambda prompt=prompt: provider.generate_image(
                    prompt=prompt, aspect_ratio="16:9", image_size="2K"
                ),
            )
            image, media_type = self._image_result(operation, result)
            stored = self.objects.put(image)
            image_artifact = self.database.add_artifact(
                attempt_id=attempt["id"],
                stage=Stage.PROTOTYPE,
                kind=ArtifactKind.PROTOTYPE,
                content_hash=stored.uri,
                object_ref=stored.uri,
                media_type=media_type,
                size_bytes=stored.size,
                metadata={"prototype_index": index, "prompt": prompt},
                provenance={
                    "operation_id": operation["id"],
                    "provider_role": "image_generator",
                    "resolved_model": operation["resolved_model"],
                    # The default prototype path supplies no external image
                    # references. Never fabricate a licensing attestation;
                    # future reference assets must add their own immutable
                    # owner/license provenance here.
                    "reference_policy": "prompt_only_no_external_reference_assets",
                    "references": [],
                },
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
                duplicate_of_index=(
                    artifact_metadata.get("prototype_index")
                    if artifact_metadata.get("prototype_index") != index
                    else None
                ),
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
        duplicate_of_index: int | None = None,
    ) -> dict[str, Any]:
        metadata = json.loads(image_artifact["metadata_json"])
        retained_prompt = metadata.get("prompt") if prompt is None else prompt
        if prompt is None and metadata.get("prototype_index") != index:
            raise RuntimeError(f"retained prototype index {index} lacks its exact generation prompt")
        if not isinstance(retained_prompt, str) or not retained_prompt:
            raise RuntimeError(f"retained prototype index {index} lacks its exact generation prompt")
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
        for artifact in prototypes:
            judgment, operation = await self._judge(
                attempt,
                artifact,
                system=PROTOTYPE_JUDGE_RUBRIC,
                context={
                    "brief": concept["brief"],
                    "prototype_contract": "horizontal head-body-tail",
                },
                key=f"{attempt['id']}:prototype-judge:{artifact['content_hash']}:v1",
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
        if not attempt["approved_prototype_hash"] or not attempt["prototype_decision_id"]:
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
        feedback = self._lineage_feedback(attempt)
        request_identity = canonical_json(
            {
                "attempt_id": attempt["id"],
                "approved_prototype_hash": attempt["approved_prototype_hash"],
                "skill_sha": bundle.sha256,
                "direction_sha": attempt["direction_sha"],
                "capability_sha": attempt["capability_sha"],
                "gate_sha": attempt["gate_sha"],
            }
        )
        request = WorkerRequest(
            request_id=f"worker_{hashlib.sha256(request_identity.encode()).hexdigest()}",
            attempt_id=attempt["id"],
            purpose=Purpose(attempt["purpose"]),
            skill_sha256=bundle.sha256,
            skill_files=bundle.files,
            capability_manifest=pinned_gates.capabilities,
            artifact_refs={
                "approved_prototype": prototype["object_ref"],
                "prototype_manifest": prototype_manifest_artifact["object_ref"],
                "canonical_direction": direction_object.uri,
            },
            authoring_inputs={
                "prototype_manifest": prototype_manifest,
                "prototype_approval": {
                    "decision_id": approval["id"],
                    "artifact_id": approval["artifact_id"],
                    "artifact_hash": approval["content_hash"],
                    "attempt_version": approval["attempt_version"],
                    "actor": approval["actor"],
                },
                "direction": {
                    "sha256": direction_object.uri,
                    "text": direction_bytes.decode("utf-8"),
                },
                "capabilities": {
                    "expected_sha256": attempt["capability_sha"],
                    "manifest": pinned_gates.capabilities,
                },
                "gates": {"expected_sha256": attempt["gate_sha"]},
            },
            inline_artifacts={
                "approved_prototype": InlineArtifact(
                    content_hash=prototype["content_hash"],
                    media_type=prototype["media_type"],
                    base64_data=base64.b64encode(self.objects.get(prototype["object_ref"])).decode("ascii"),
                )
            },
            pure_tools=["color_math", "schema_lookup"],
            budget={
                "max_layers": pinned_gates.capabilities["limits"]["max_flattened_layers"],
                "max_texture_refs": pinned_gates.capabilities["limits"]["max_texture_refs"],
            },
            output_schemas={
                "implementation_plan": ImplementationPlan.model_json_schema(),
                "worker_result": WorkerResult.model_json_schema(),
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
            validate_worker_handoff(worker_result, bundle.files, pinned_gates.capabilities)
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

        for index, asset in enumerate(plan.asset_plan):
            uploaded = self._find_current_artifact(
                attempt["id"],
                ArtifactKind.FORGE_MANIFEST,
                metadata_match={"asset_index": index, "uploaded": True},
            )
            if uploaded:
                generated_textures.append(self.persistence.load_json(uploaded["object_ref"]))
                continue
            request_spec = next(
                (
                    request
                    for request in tool_requests
                    if request.get("kind") == "generate_asset"
                    and int(request.get("arguments", {}).get("asset_index", -1)) == index
                ),
                None,
            )
            base_prompt = (request_spec.get("arguments", {}).get("prompt") if request_spec else None) or asset.prompt
            if not base_prompt:
                base_prompt = f"Create the final {asset.kind} art faithful to the approved snake prototype."
            rejection_feedback = ""
            accepted: ForgeBundle | None = None
            accepted_evidence: dict[str, Any] | None = None
            accepted_normalized: dict[str, Any] | None = None
            accepted_generation: int | None = None
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
                    attempt,
                    Stage.ASSETS,
                    ArtifactKind.SOURCE_ASSET,
                    bundle.normalized_source,
                    "image/png",
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
            operation, result = await self._provider_call(
                attempt=attempt,
                stage=Stage.REGISTER,
                key=f"{attempt['id']}:create-skin:{document_artifact['content_hash']}",
                role="snaketron_api",
                side_effect="create_private_skin_revision",
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
            operation, result = await self._provider_call(
                attempt=attempt,
                stage=Stage.REGISTER,
                key=f"{attempt['id']}:append-skin:{document_artifact['content_hash']}",
                role="snaketron_api",
                side_effect="append_private_skin_revision",
                request=request,
                invoke=lambda: self.api.append_revision(
                    skin_id=previous["production_skin_id"],
                    document=document,
                    expected_head_revision=int(previous["production_revision"]),
                ),
            )
        response = self._json_result(operation, result)
        skin_id = response.get("skinId") or response.get("skin_id")
        revision = response.get("headRevision") or response.get("head_revision")
        content_ref = response.get("contentRef") or response.get("content_ref")
        if skin_id is None or revision is None or not content_ref:
            return self._block_attempt(
                self.database.get_attempt(attempt["id"]),
                "Snaketron registration response omitted skinId/headRevision/contentRef",
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
        retries = self.config.budgets.provider_retries

        def validate_result(result: ProviderResult) -> None:
            self._validate_resolved_model(attempt, role, result)

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
                if existing["status"] == OperationStatus.INTENT:
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
            if hasattr(self.config.models, config_role_name):
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
            validate_recovered_result(
                config=self.config,
                operation=operation,
                database=self.database,
                objects=self.objects,
                result_hash=result_hash,
                resolved_model=resolved,
                media_type=recovery.get("media_type"),
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

    def _prototype_prompt(
        self,
        attempt: dict[str, Any],
        concept: dict[str, Any],
        index: int,
    ) -> str:
        direction = self.pinned_direction(attempt).decode("utf-8")
        feedback = self._stage_feedback(attempt, "prototype")
        feedback_block = (
            "\nLiteral human corrections for this retry (JSON strings; preserve meaning, do not invent more):\n"
            + canonical_json(feedback)
            if feedback
            else ""
        )
        return f"""Create prototype variation {index + 1} for this Snaketron skin.

Brief: {concept["brief"]}
Canonical direction:
{direction}
{feedback_block}

Depict exactly one medium-length snake in a horizontal head + representative
body + tail strip. The head and tail must be visibly distinct. Use a neutral or
transparent background. Keep it legible at actual game scale. Do not include
UI chrome, words, labels, scenery, framing, or alternate concepts. This is a
visual implementation reference, not poster art."""

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
        slice_counts = [len(self._asset_image_slices(asset)) for asset in plan.asset_plan]
        per_asset = self.config.budgets.max_image_slices_per_asset
        for index, count in enumerate(slice_counts):
            if count > per_asset:
                raise ValueError(f"asset {index} needs {count} slices; configured maximum is {per_asset}")
        # The same configured retry count bounds both known-safe transport
        # retries and image regeneration after a strict deterministic reject.
        rounds = self.config.budgets.provider_retries + 1
        worst_case_calls = sum(slice_counts) * rounds * rounds
        maximum = self.config.budgets.max_asset_image_calls_per_attempt
        if worst_case_calls > maximum:
            raise ValueError(f"worst-case image calls {worst_case_calls} exceed configured attempt maximum {maximum}")

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
                    "target_height_px": frame_rows * asset.texels_per_cell,
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
                "height_px": asset.frames * asset.texels_per_cell,
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
            texture_name = request.get("arguments", {}).get("texture_name") if request else None
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
