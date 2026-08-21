"""Human-only review transitions, recovery actions, and exact publication."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Sequence
from typing import Any

from .db import Database, VersionConflict, canonical_json
from .domain import (
    ArtifactKind,
    Disposition,
    OperationStatus,
    ProviderError,
    ProviderFailureKind,
    Purpose,
    Stage,
)
from .operations import ExistingOperation, OperationJournal
from .persistence import ResultPersistence
from .snaketron_api import SnaketronApi


class ReviewService:
    def __init__(
        self,
        database: Database,
        journal: OperationJournal,
        api: SnaketronApi,
        persistence: ResultPersistence,
        behavior_snapshot: Callable[[], dict[str, Any]],
        provider_retries: int = 2,
    ) -> None:
        if provider_retries < 0:
            raise ValueError("provider_retries must be non-negative")
        self.database = database
        self.journal = journal
        self.api = api
        self.persistence = persistence
        self.behavior_snapshot = behavior_snapshot
        self.provider_retries = provider_retries

    @staticmethod
    def _child_request_key(
        *,
        action: str,
        actor: str,
        parent_attempt_id: str,
        exact_input: dict[str, str],
        idempotency_key: str | None,
    ) -> str:
        """Hash one immutable human request without exposing its bearer key."""

        supplied = (idempotency_key or "").strip() or "implicit-single-request"
        payload = canonical_json(
            {
                "version": 1,
                "action": action,
                "actor": actor,
                "parent_attempt_id": parent_attempt_id,
                "exact_input": exact_input,
                "client_idempotency_key": supplied,
            }
        )
        return hashlib.sha256(payload.encode()).hexdigest()

    async def _journaled_call(
        self,
        *,
        attempt_id: str,
        stage: Stage,
        base_key: str,
        side_effect: str,
        request: dict[str, Any],
        invoke: Callable[[], Any],
    ) -> tuple[dict[str, Any], Any]:
        """Run one exact review effect with bounded, durable safe retries."""

        safe_failures = {
            ProviderFailureKind.TIMEOUT,
            ProviderFailureKind.UNAVAILABLE,
            ProviderFailureKind.QUOTA,
        }
        request_hash = self.journal.request_hash(request)

        async def run(key: str) -> tuple[dict[str, Any], Any]:
            return await self.journal.run_provider(
                attempt_id=attempt_id,
                stage=stage,
                idempotency_key=key,
                side_effect=side_effect,
                provider_role="human_operator",
                request=request,
                reserve_micros=0,
                invoke=invoke,
                persist_result=self.persistence,
            )

        for retry in range(self.provider_retries + 1):
            key = base_key if retry == 0 else f"{base_key}:retry:{retry}"
            with self.database.connect() as connection:
                row = connection.execute("SELECT * FROM operation WHERE idempotency_key=?", (key,)).fetchone()
            existing = dict(row) if row is not None else None
            retryable = existing is not None and (
                existing["status"] == OperationStatus.FAILED_RETRYABLE
                or (existing["status"] == OperationStatus.RESOLVED and existing["retry_class"] == "retry_safe")
            )
            if retryable:
                assert existing is not None
                if existing["request_hash"] != request_hash:
                    return await run(key)
                if retry < self.provider_retries:
                    continue
                raise ExistingOperation(f"known-safe review retries exhausted for operation {existing['id']}")
            if existing is not None:
                return await run(key)
            try:
                return await run(key)
            except ProviderError as error:
                if error.outcome_known and error.kind in safe_failures and retry < self.provider_retries:
                    continue
                raise
        raise AssertionError("review provider retry loop did not return or raise")

    @staticmethod
    def _human(actor: str) -> None:
        if not actor.startswith("human:") or not actor.removeprefix("human:").strip():
            raise PermissionError("this transition requires an authenticated human actor")

    def label(
        self,
        *,
        attempt_id: str,
        artifact_id: str,
        kind: str,
        outcome: str,
        feedback: str,
        tags: Sequence[str],
        actor: str,
    ) -> dict[str, Any]:
        self._human(actor)
        if kind not in {"prototype_label", "build_quality_label"}:
            raise ValueError("labels are prototype_label or build_quality_label")
        if outcome not in {"accept", "reject"}:
            raise ValueError("blind-label outcome must be accept or reject")
        attempt = self.database.get_attempt(attempt_id)
        artifact = self.database.get_artifact(artifact_id)
        if artifact["attempt_id"] != attempt_id:
            raise VersionConflict("label artifact does not belong to attempt")
        expected_kind = {
            "prototype_label": ArtifactKind.PROTOTYPE,
            "build_quality_label": ArtifactKind.CONTACT_SHEET,
        }[kind]
        if artifact["kind"] != expected_kind:
            raise ValueError(f"{kind} must label a retained {expected_kind} artifact")
        return self.database.add_human_decision(
            artifact_id=artifact_id,
            attempt_id=attempt_id,
            action=kind,
            feedback=feedback,
            tags=[
                *(tag for tag in tags if not str(tag).lower().startswith("outcome:")),
                f"outcome:{outcome}",
            ],
            actor=actor,
            attempt_version=attempt["version"],
            content_hash=artifact["content_hash"],
        )

    def approve_prototype(
        self,
        *,
        attempt_id: str,
        artifact_id: str,
        content_hash: str,
        feedback: str,
        actor: str,
    ) -> dict[str, Any]:
        self._human(actor)
        attempt = self.database.get_attempt(attempt_id)
        if attempt["purpose"] != Purpose.PRODUCTION:
            raise PermissionError("optimizer and technique artifacts cannot enter production")
        artifact = self.database.get_artifact(artifact_id)
        if artifact["attempt_id"] != attempt_id or artifact["kind"] != ArtifactKind.PROTOTYPE:
            raise VersionConflict("approval must name a prototype from the exact attempt")
        if artifact["content_hash"] != content_hash:
            raise VersionConflict("prototype approval hash differs from retained bytes")
        existing = self.database.find_exact_decision(
            attempt_id=attempt_id,
            action="prototype_approval",
            content_hash=content_hash,
        )
        if existing is not None and existing["artifact_id"] != artifact_id:
            raise VersionConflict("prototype approval replay names a different artifact")
        if attempt["stage"] != Stage.PROTOTYPE_REVIEW:
            if (
                existing is not None
                and attempt["approved_prototype_hash"] == content_hash
                and attempt["prototype_decision_id"] == existing["id"]
            ):
                return {"decision": existing, "attempt": attempt}
            raise VersionConflict("new prototype approval requires the prototype review state")
        if attempt["disposition"] != Disposition.NEEDS_HUMAN or attempt["review_kind"] not in {None, "prototype"}:
            raise VersionConflict("machine soft rejects require an explicit triage override before approval")
        if attempt["approved_prototype_hash"] or attempt["prototype_decision_id"]:
            raise VersionConflict("prototype review already binds a different approval")
        decision = existing or self.database.add_human_decision(
            artifact_id=artifact_id,
            attempt_id=attempt_id,
            action="prototype_approval",
            feedback=feedback,
            tags=[],
            actor=actor,
            attempt_version=attempt["version"],
            content_hash=content_hash,
        )
        updated = self.database.update_attempt(
            attempt_id,
            attempt["version"],
            approved_prototype_hash=content_hash,
            prototype_decision_id=decision["id"],
            stage=Stage.AUTHOR,
            disposition=Disposition.ACTIVE,
            review_kind=None,
        )
        return {"decision": decision, "attempt": updated}

    async def override_triage(
        self,
        *,
        attempt_id: str,
        artifact_id: str,
        feedback: str,
        actor: str,
    ) -> dict[str, Any]:
        """Route an exact soft-rejected artifact back to human authority."""

        self._human(actor)
        attempt = self.database.get_attempt(attempt_id)
        if attempt["purpose"] != Purpose.PRODUCTION:
            raise PermissionError("optimizer and technique triage cannot enter production review")
        expected = {
            Stage.PROTOTYPE_REVIEW: (ArtifactKind.PROTOTYPE, "prototype"),
            Stage.FINAL_REVIEW: (ArtifactKind.CONTACT_SHEET, "final"),
        }.get(attempt["stage"])
        if expected is None:
            raise VersionConflict("triage override requires a prototype or final soft-reject stage")
        artifact = self.database.get_artifact(artifact_id)
        if artifact["attempt_id"] != attempt_id or artifact["kind"] != expected[0]:
            raise VersionConflict(f"triage override must name the exact {expected[0]} from this attempt")
        existing = self.database.find_exact_decision(
            attempt_id=attempt_id,
            action="soft_triage_override",
            content_hash=artifact["content_hash"],
        )
        if attempt["disposition"] == Disposition.NEEDS_HUMAN:
            if existing is not None and attempt["review_kind"] == expected[1]:
                return {"decision": existing, "attempt": attempt}
            raise VersionConflict("attempt is already routed to a different human review")
        if attempt["disposition"] != Disposition.MACHINE_REJECTED:
            raise VersionConflict("only a machine soft reject can be overridden")

        operation: dict[str, Any] | None = None
        if attempt["stage"] == Stage.FINAL_REVIEW:
            skin_id = attempt["production_skin_id"]
            revision = attempt["production_revision"]
            content_hash = attempt["production_content_hash"]
            if not skin_id or not revision or not content_hash:
                raise VersionConflict("final override requires the exact registered revision authority")
            request = {
                "skin_id": skin_id,
                "revision": int(revision),
                "content_ref": content_hash,
                "operator": True,
            }
            operation, _ = await self._journaled_call(
                attempt_id=attempt_id,
                stage=Stage.FINAL_REVIEW,
                base_key=(f"override-publish-request:{attempt_id}:{skin_id}:{revision}:{content_hash}"),
                side_effect="request_exact_publication_review_after_override",
                request=request,
                invoke=lambda: self.api.request_publication_exact(
                    skin_id=skin_id,
                    revision=int(revision),
                    content_ref=content_hash,
                    operator=True,
                ),
            )
            attempt = self.database.get_attempt(attempt_id)
            if not (
                attempt["stage"] == Stage.FINAL_REVIEW
                and attempt["disposition"] == Disposition.MACHINE_REJECTED
                and attempt["production_skin_id"] == skin_id
                and str(attempt["production_revision"]) == str(revision)
                and attempt["production_content_hash"] == content_hash
            ):
                raise VersionConflict("final soft reject changed while opening exact review authority")

        decision = existing or self.database.add_human_decision(
            artifact_id=artifact_id,
            attempt_id=attempt_id,
            action="soft_triage_override",
            feedback=feedback,
            tags=[],
            actor=actor,
            attempt_version=attempt["version"],
            content_hash=artifact["content_hash"],
        )
        updated = self.database.update_attempt(
            attempt_id,
            attempt["version"],
            disposition=Disposition.NEEDS_HUMAN,
            review_kind=expected[1],
        )
        result = {"decision": decision, "attempt": updated}
        if operation is not None:
            result["operation"] = operation
        return result

    async def reject(
        self,
        *,
        attempt_id: str,
        artifact_id: str | None,
        feedback: str,
        tags: Sequence[str],
        actor: str,
    ) -> dict[str, Any]:
        self._human(actor)
        attempt = self.database.get_attempt(attempt_id)
        if attempt["purpose"] != Purpose.PRODUCTION:
            raise PermissionError("optimizer and technique artifacts cannot receive production review decisions")
        expected_review = {
            Stage.PROTOTYPE_REVIEW: "prototype",
            Stage.FINAL_REVIEW: "final",
        }.get(attempt["stage"])
        if (
            attempt["disposition"] != Disposition.NEEDS_HUMAN
            or expected_review is None
            or attempt["review_kind"] != expected_review
        ):
            raise VersionConflict("human rejection requires an attempt waiting in prototype or final review")
        artifact = self.database.get_artifact(artifact_id) if artifact_id else None
        if artifact is not None and artifact["attempt_id"] != attempt_id:
            raise VersionConflict("rejection artifact does not belong to attempt")
        operation: dict[str, Any] | None = None
        if attempt["stage"] == Stage.FINAL_REVIEW:
            skin_id = attempt["production_skin_id"]
            revision = attempt["production_revision"]
            content_hash = attempt["production_content_hash"]
            if not skin_id or not revision or not content_hash:
                raise VersionConflict("final rejection requires the exact registered revision authority")
            request = {
                "skin_id": skin_id,
                "revision": int(revision),
                "content_ref": content_hash,
            }
            operation, _ = await self._journaled_call(
                attempt_id=attempt_id,
                stage=Stage.FINAL_REVIEW,
                base_key=(f"cancel-publish-request:{attempt_id}:{skin_id}:{revision}:{content_hash}"),
                side_effect="cancel_exact_publication_request",
                request=request,
                invoke=lambda: self.api.cancel_publication_request_exact(
                    skin_id=skin_id,
                    revision=int(revision),
                    content_ref=content_hash,
                ),
            )
            # Operation journaling deliberately advances the attempt version.
            # Bind the local human decision to that new version, while
            # re-checking that no concurrent local review transition won.
            current = self.database.get_attempt(attempt_id)
            if not (
                current["stage"] == Stage.FINAL_REVIEW
                and current["disposition"] == Disposition.NEEDS_HUMAN
                and current["review_kind"] == "final"
                and current["production_skin_id"] == skin_id
                and str(current["production_revision"]) == str(revision)
                and current["production_content_hash"] == content_hash
            ):
                raise VersionConflict("final review changed while cancelling its exact server request")
            attempt = current
        decision = self.database.add_human_decision(
            artifact_id=artifact_id,
            attempt_id=attempt_id,
            action="human_rejection",
            feedback=feedback,
            tags=tags,
            actor=actor,
            attempt_version=attempt["version"],
            content_hash=artifact["content_hash"] if artifact else None,
        )
        updated = self.database.update_attempt(
            attempt_id,
            attempt["version"],
            disposition=Disposition.HUMAN_REJECTED,
            review_kind=None,
        )
        result = {"decision": decision, "attempt": updated}
        if operation is not None:
            result["operation"] = operation
        return result

    def retry(
        self,
        *,
        attempt_id: str,
        from_stage: str,
        feedback: str,
        actor: str,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        self._human(actor)
        if from_stage not in {"prototype", "assets", "build"}:
            raise ValueError("retry stage must be prototype, assets, or build")
        parent = self.database.get_attempt(attempt_id)
        if from_stage == "prototype":
            stage = Stage.PROTOTYPE
            approved_hash = None
            approval_id = None
        elif from_stage == "assets":
            stage = Stage.ASSETS
            approved_hash = parent["approved_prototype_hash"]
            approval_id = parent["prototype_decision_id"]
        else:
            stage = Stage.AUTHOR
            approved_hash = parent["approved_prototype_hash"]
            approval_id = parent["prototype_decision_id"]
        if from_stage != "prototype" and (not approved_hash or not approval_id):
            raise VersionConflict("asset/build retry has no exact prototype authority to inherit")
        request_key = self._child_request_key(
            action="retry",
            actor=actor,
            parent_attempt_id=attempt_id,
            exact_input={"from_stage": from_stage},
            idempotency_key=idempotency_key,
        )
        snapshot = self.behavior_snapshot()
        decision, child, linked = self.database.create_review_child(
            parent_attempt_id=attempt_id,
            expected_parent_version=parent["version"],
            decision_idempotency_key=f"review-decision:{request_key}",
            child_idempotency_key=f"review-child:{request_key}",
            action="retry",
            decision_artifact_id=None,
            decision_content_hash=None,
            feedback=feedback,
            tags=[f"from:{from_stage}"],
            actor=actor,
            stage=stage,
            restart_stage=from_stage,
            behavior=snapshot,
            direction_sha=snapshot["direction_sha"],
            skill_sha=snapshot["skill_sha"],
            capability_sha=snapshot["capability_sha"],
            gate_sha=snapshot["gate_sha"],
            model_config_sha=snapshot["model_config_sha"],
            approved_prototype_hash=approved_hash,
            prototype_decision_id=approval_id,
        )
        assert linked is None
        return {"decision": decision, "attempt": child}

    def re_evaluate(
        self,
        *,
        attempt_id: str,
        artifact_id: str,
        feedback: str,
        actor: str,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        self._human(actor)
        parent = self.database.get_attempt(attempt_id)
        artifact = self.database.get_artifact(artifact_id)
        if artifact["attempt_id"] != attempt_id:
            raise VersionConflict("re-evaluation artifact must belong to the named attempt")
        evaluation_artifact = artifact
        if artifact["kind"] == ArtifactKind.PROTOTYPE_MANIFEST:
            metadata = json.loads(artifact["metadata_json"])
            image_artifact_id = metadata.get("image_artifact_id")
            if not image_artifact_id:
                raise VersionConflict("prototype manifest does not bind retained image bytes")
            image = self.database.get_artifact(image_artifact_id)
            if image["attempt_id"] != attempt_id or image["kind"] != ArtifactKind.PROTOTYPE:
                raise VersionConflict("prototype manifest image binding is invalid")
            evaluation_artifact = image
            stage = Stage.PROTOTYPE_TRIAGE
        elif artifact["kind"] == ArtifactKind.PROTOTYPE:
            stage = Stage.PROTOTYPE_TRIAGE
        elif artifact["kind"] == ArtifactKind.SKIN_DOCUMENT:
            stage = Stage.BUILD_GATE
        elif artifact["kind"] in {ArtifactKind.CONTACT_SHEET, ArtifactKind.ANIMATION_CAPTURE}:
            stage = Stage.BUILD_TRIAGE
        else:
            raise ValueError(f"artifact kind {artifact['kind']} has no re-evaluation gate")
        request_key = self._child_request_key(
            action="re_evaluate",
            actor=actor,
            parent_attempt_id=attempt_id,
            exact_input={
                "artifact_id": artifact_id,
                "content_hash": str(artifact["content_hash"]),
            },
            idempotency_key=idempotency_key,
        )
        snapshot = self.behavior_snapshot()
        metadata = json.loads(evaluation_artifact["metadata_json"])
        provenance = json.loads(evaluation_artifact["provenance_json"])
        decision, child, linked = self.database.create_review_child(
            parent_attempt_id=attempt_id,
            expected_parent_version=parent["version"],
            decision_idempotency_key=f"review-decision:{request_key}",
            child_idempotency_key=f"review-child:{request_key}",
            action="re_evaluate",
            decision_artifact_id=artifact_id,
            decision_content_hash=artifact["content_hash"],
            feedback=feedback,
            tags=[],
            actor=actor,
            stage=stage,
            restart_stage="re_evaluate",
            behavior=snapshot,
            direction_sha=snapshot["direction_sha"],
            skill_sha=snapshot["skill_sha"],
            capability_sha=snapshot["capability_sha"],
            gate_sha=snapshot["gate_sha"],
            model_config_sha=snapshot["model_config_sha"],
            approved_prototype_hash=parent["approved_prototype_hash"],
            prototype_decision_id=parent["prototype_decision_id"],
            linked_source_artifact_id=evaluation_artifact["id"],
            linked_metadata={**metadata, "re_evaluates_artifact_id": evaluation_artifact["id"]},
            linked_provenance={**provenance, "linked_from_attempt_id": attempt_id},
        )
        assert linked is not None
        return {"decision": decision, "attempt": child, "artifact": linked}

    async def publish(
        self,
        *,
        attempt_id: str,
        revision: str,
        content_hash: str,
        feedback: str,
        actor: str,
    ) -> dict[str, Any]:
        self._human(actor)
        attempt = self.database.get_attempt(attempt_id)
        if attempt["purpose"] != Purpose.PRODUCTION:
            raise PermissionError("trial skins are non-publishable")
        if str(attempt["production_revision"]) != str(revision) or attempt["production_content_hash"] != content_hash:
            raise VersionConflict("publication must bind the exact reviewed revision and content hash")
        existing = self.database.find_exact_decision(
            attempt_id=attempt_id,
            action="publish_approval",
            content_hash=content_hash,
            revision=str(revision),
        )
        if existing is None and not (
            attempt["stage"] == Stage.FINAL_REVIEW
            and attempt["disposition"] == Disposition.NEEDS_HUMAN
            and attempt["review_kind"] == "final"
        ):
            raise VersionConflict("publication requires the exact final review state")
        decision = existing or self.database.add_human_decision(
            artifact_id=None,
            attempt_id=attempt_id,
            action="publish_approval",
            feedback=feedback,
            tags=[],
            actor=actor,
            attempt_version=attempt["version"],
            revision=str(revision),
            content_hash=content_hash,
        )
        reason = decision["feedback"] or "Skin Factory human approval"
        request = {
            "skin_id": attempt["production_skin_id"],
            "revision": int(revision),
            "content_ref": content_hash,
            "reason": reason,
        }
        operation, result = await self._journaled_call(
            attempt_id=attempt_id,
            stage=Stage.FINAL_REVIEW,
            base_key=f"publish:{decision['id']}:{revision}:{content_hash}",
            side_effect="publish_exact_revision",
            request=request,
            invoke=lambda: self.api.publish_exact(
                skin_id=attempt["production_skin_id"],
                revision=int(revision),
                content_ref=content_hash,
                reason=reason,
            ),
        )
        current = self.database.get_attempt(attempt_id)
        if current["disposition"] != Disposition.PUBLISHED:
            current = self.database.update_attempt(
                attempt_id,
                current["version"],
                stage=Stage.COMPLETE,
                disposition=Disposition.PUBLISHED,
                review_kind=None,
            )
        return {"decision": decision, "operation": operation, "attempt": current, "result": result}
