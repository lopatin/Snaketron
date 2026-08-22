"""Human-only review transitions, recovery actions, and exact publication."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Sequence
from typing import Any, Literal

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
from .prototype_projection import PROTOTYPE_PROJECTION_VERSION
from .recovery import validate_skin_authority_readback
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
        mode: Literal["shadow", "production"] = "production",
    ) -> None:
        if provider_retries < 0:
            raise ValueError("provider_retries must be non-negative")
        self.database = database
        self.journal = journal
        self.api = api
        self.persistence = persistence
        self.behavior_snapshot = behavior_snapshot
        self.provider_retries = provider_retries
        self.mode = mode

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
            ProviderFailureKind.AUTHENTICATION,
            ProviderFailureKind.TIMEOUT,
            ProviderFailureKind.UNAVAILABLE,
            ProviderFailureKind.QUOTA,
        }
        request_hash = self.journal.request_hash(request)
        request_object = self.persistence.objects.put(self.journal.request_payload(request))

        async def run(key: str) -> tuple[dict[str, Any], Any]:
            operation, result = await self.journal.run_provider(
                attempt_id=attempt_id,
                stage=stage,
                idempotency_key=key,
                side_effect=side_effect,
                provider_role="human_operator",
                request=request,
                reserve_micros=0,
                invoke=invoke,
                persist_result=self.persistence,
                metadata={
                    "request_ref": request_object.uri,
                    "request_sha256": request_object.sha256,
                },
            )
            metadata = json.loads(operation.get("metadata_json") or "{}")
            if result is None and isinstance(metadata, dict) and isinstance(metadata.get("recovery"), dict):
                skin_id = request.get("skin_id")
                if skin_id is None:
                    raise ValueError(f"recovered {side_effect} operation omitted exact skin_id authority")
                authority = await self.api.get_skin_authority(skin_id, operator=True)
                try:
                    validate_skin_authority_readback(
                        side_effect=side_effect,
                        request=request,
                        authority=authority.value,
                    )
                except ValueError as error:
                    metadata["recovery_validation"] = {
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
                        metadata_json=metadata,
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
                    ) from error
            return operation, result

        # A 401/403 is known not to have executed. Preserve the failed bounded
        # chain, but let a later *human action* after credential refresh open a
        # new audited reauthentication epoch for the same immutable request.
        # Other terminal/exhausted failures remain closed.
        epoch = 0
        while True:
            prefix = base_key if epoch == 0 else f"{base_key}:reauth:{epoch}"
            keys = [prefix, *(f"{prefix}:retry:{retry}" for retry in range(1, self.provider_retries + 1))]
            with self.database.connect() as connection:
                rows = connection.execute(
                    f"SELECT * FROM operation WHERE idempotency_key IN ({','.join('?' for _ in keys)})",
                    keys,
                ).fetchall()
            existing_by_key = {str(row["idempotency_key"]): dict(row) for row in rows}
            success = next(
                (
                    operation
                    for operation in existing_by_key.values()
                    if operation["status"] == OperationStatus.SUCCEEDED and operation["request_hash"] == request_hash
                ),
                None,
            )
            if success is not None:
                return await run(str(success["idempotency_key"]))
            if len(existing_by_key) != len(keys):
                break
            authentication_chain = True
            for operation in existing_by_key.values():
                try:
                    failure = json.loads(operation.get("failure_json") or "{}")
                except json.JSONDecodeError:
                    failure = {}
                authentication_chain = authentication_chain and (
                    operation["status"] == OperationStatus.FAILED_RETRYABLE
                    and failure.get("kind") == ProviderFailureKind.AUTHENTICATION
                )
            if not authentication_chain:
                break
            epoch += 1

        for retry in range(self.provider_retries + 1):
            key = prefix if retry == 0 else f"{prefix}:retry:{retry}"
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

    async def _cancel_final_review_request(
        self,
        attempt: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any] | None]:
        """Close one exact server review request before forking its Attempt.

        Production Attempts in final review own a server-side pending revision.
        A retry/re-evaluation must release that authority before its child can
        eventually request a different exact revision. The same journal key is
        shared with rejection, so known success, 401 reauthentication epochs,
        and authenticated unknown-outcome recovery remain idempotent.
        """

        if attempt["stage"] != Stage.FINAL_REVIEW or attempt["purpose"] != Purpose.PRODUCTION:
            return attempt, None
        skin_id = attempt["production_skin_id"]
        revision = attempt["production_revision"]
        content_hash = attempt["production_content_hash"]
        if not skin_id or not revision or not content_hash:
            raise VersionConflict("final review child requires the exact registered revision authority")
        request = {
            "skin_id": skin_id,
            "revision": int(revision),
            "content_ref": content_hash,
        }
        operation, _ = await self._journaled_call(
            attempt_id=attempt["id"],
            stage=Stage.FINAL_REVIEW,
            base_key=(f"cancel-publish-request:{attempt['id']}:{skin_id}:{revision}:{content_hash}"),
            side_effect="cancel_exact_publication_request",
            request=request,
            invoke=lambda: self.api.cancel_publication_request_exact(
                skin_id=skin_id,
                revision=int(revision),
                content_ref=content_hash,
            ),
        )
        current = self.database.get_attempt(attempt["id"])
        stable_fields = (
            "stage",
            "purpose",
            "disposition",
            "review_kind",
            "production_skin_id",
            "production_revision",
            "production_content_hash",
            "approved_prototype_hash",
            "prototype_decision_id",
        )
        if any(str(current.get(field)) != str(attempt.get(field)) for field in stable_fields):
            raise VersionConflict("final review changed while cancelling its exact server request")
        return current, operation

    def _assert_no_blocking_failure(self, attempt_id: str, artifact_id: str | None = None) -> None:
        failures = [
            evaluation
            for evaluation in self.database.evaluations_for_attempt(attempt_id, reveal=True)
            if bool(evaluation["blocking"])
            and evaluation["verdict"] == "fail"
            and (artifact_id is None or evaluation["artifact_id"] == artifact_id)
        ]
        if failures:
            gates = sorted({str(evaluation["gate_name"]) for evaluation in failures})
            raise PermissionError("blocking gates cannot be overridden or published: " + ", ".join(gates))

    def _assert_shared_prototype_authority(
        self,
        attempt: dict[str, Any],
        *,
        content_hash: str,
        require_approval_decision: bool,
    ) -> None:
        behavior = json.loads(attempt["behavior_json"])
        if int(behavior.get("snapshot_version", 0)) < 7:
            raise VersionConflict(
                "legacy prototype cannot authorize a build; retry from prototype under the shared geometry rules"
            )
        expected = {
            "design_guidelines_sha256": behavior.get("design_guidelines_sha"),
            "prototype_geometry_sha256": behavior.get("prototype_geometry_sha"),
            "prototype_guide_sha256": behavior.get("prototype_guide_sha"),
        }
        if any(
            not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None for value in expected.values()
        ):
            raise VersionConflict("prototype approval behavior lacks exact shared design and geometry authority hashes")

        lineage: list[dict[str, Any]] = []
        current = attempt
        seen: set[str] = set()
        manifest_payload: dict[str, Any] | None = None
        while current["id"] not in seen:
            lineage.append(current)
            seen.add(current["id"])
            for manifest in self.database.artifacts_for_attempt(
                current["id"],
                stage=Stage.PROTOTYPE,
                kind=ArtifactKind.PROTOTYPE_MANIFEST,
            ):
                payload = self.persistence.load_json(manifest["object_ref"])
                if payload.get("image_sha256") == content_hash:
                    manifest_payload = payload
                    break
            if manifest_payload is not None:
                break
            parent_id = current.get("parent_attempt_id")
            if not parent_id:
                break
            current = self.database.get_attempt(parent_id)
        if manifest_payload is None:
            raise VersionConflict("prototype authority requires its exact contract-bound manifest")
        if any(manifest_payload.get(key) != value for key, value in expected.items()):
            raise VersionConflict(
                "legacy or mismatched prototype cannot authorize a build; retry from prototype under current rules"
            )
        source_hash = manifest_payload.get("source_image_sha256")
        if (
            manifest_payload.get("geometry_projection") != PROTOTYPE_PROJECTION_VERSION
            or not isinstance(source_hash, str)
            or re.fullmatch(r"sha256:[0-9a-f]{64}", source_hash) is None
        ):
            raise VersionConflict("prototype authority lacks exact renderer-mask projection provenance")
        retained_source = next(
            (
                artifact
                for candidate in lineage
                for artifact in self.database.artifacts_for_attempt(
                    candidate["id"],
                    stage=Stage.PROTOTYPE,
                    kind=ArtifactKind.PROVIDER_RESPONSE,
                )
                if artifact["content_hash"] == source_hash and str(artifact["media_type"]).startswith("image/")
            ),
            None,
        )
        if retained_source is None:
            raise VersionConflict("prototype authority lacks its retained raw provider source")
        if not require_approval_decision:
            return
        decision_id = attempt.get("prototype_decision_id")
        approval = next(
            (
                decision
                for candidate in lineage
                for decision in self.database.decisions_for_attempt(candidate["id"])
                if decision["id"] == decision_id and decision["action"] == "prototype_approval"
            ),
            None,
        )
        if approval is None or approval["content_hash"] != content_hash:
            raise VersionConflict("final review lacks the exact shared-contract prototype approval decision")

    def _assert_publication_authority_has_no_blocking_failure(self, attempt: dict[str, Any]) -> None:
        """Check only artifacts that authorize this exact immutable revision.

        Failed prototype siblings and failed asset generations stay retained,
        but cannot poison a later selected prototype or accepted regeneration.
        The exact selected prototype, registered document, and current render
        evidence remain non-overrideable.
        """

        lineage: list[dict[str, Any]] = []
        current: dict[str, Any] | None = attempt
        seen: set[str] = set()
        while current is not None and current["id"] not in seen:
            lineage.append(current)
            seen.add(current["id"])
            parent_id = current.get("parent_attempt_id")
            current = self.database.get_attempt(parent_id) if parent_id else None

        authoritative: set[str] = set()
        for lineage_attempt in lineage:
            for artifact in self.database.artifacts_for_attempt(lineage_attempt["id"]):
                if artifact["kind"] == ArtifactKind.PROTOTYPE and artifact["content_hash"] == attempt.get(
                    "approved_prototype_hash"
                ):
                    authoritative.add(artifact["id"])
                if artifact["kind"] == ArtifactKind.SKIN_DOCUMENT and artifact["content_hash"] == attempt.get(
                    "production_content_hash"
                ):
                    authoritative.add(artifact["id"])
        # Final browser evidence is produced on the publishing Attempt. Older
        # retry renders in ancestors are historical, not publication inputs.
        for kind in (ArtifactKind.CONTACT_SHEET, ArtifactKind.ANIMATION_CAPTURE):
            artifacts = self.database.artifacts_for_attempt(attempt["id"], kind=kind)
            if artifacts:
                authoritative.add(artifacts[-1]["id"])

        failures: list[dict[str, Any]] = []
        for lineage_attempt in lineage:
            failures.extend(
                evaluation
                for evaluation in self.database.evaluations_for_attempt(lineage_attempt["id"], reveal=True)
                if evaluation["artifact_id"] in authoritative
                and bool(evaluation["blocking"])
                and evaluation["verdict"] == "fail"
            )
        if failures:
            gates = sorted({str(evaluation["gate_name"]) for evaluation in failures})
            raise PermissionError("blocking gates cannot be overridden or published: " + ", ".join(gates))

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
        artifact = self.database.get_artifact(artifact_id)
        if artifact["attempt_id"] != attempt_id:
            raise VersionConflict("label artifact does not belong to attempt")
        expected_kind = {
            "prototype_label": ArtifactKind.PROTOTYPE,
            "build_quality_label": ArtifactKind.CONTACT_SHEET,
        }[kind]
        if artifact["kind"] != expected_kind:
            raise ValueError(f"{kind} must label a retained {expected_kind} artifact")
        return self.database.add_blind_human_label(
            artifact_id=artifact_id,
            attempt_id=attempt_id,
            action=kind,
            feedback=feedback,
            tags=[
                *(tag for tag in tags if not str(tag).lower().startswith("outcome:")),
                f"outcome:{outcome}",
            ],
            actor=actor,
            content_hash=artifact["content_hash"],
        )

    def _assert_blind_prototype_label(self, artifact_id: str) -> None:
        if not self.database.has_hidden_visual_evaluation(artifact_id):
            return
        if (
            self.database.authoritative_blind_label(
                artifact_id=artifact_id,
                action="prototype_label",
            )
            is None
        ):
            raise VersionConflict(
                "prototype approval requires one blind label bound to its hidden visual_judge evaluation"
            )

    def _assert_blind_build_label(self, attempt_id: str) -> None:
        contact_sheets = self.database.artifacts_for_attempt(attempt_id, kind=ArtifactKind.CONTACT_SHEET)
        if not contact_sheets:
            return
        contact_sheet = contact_sheets[-1]
        if not self.database.has_hidden_visual_evaluation(contact_sheet["id"]):
            return
        if (
            self.database.authoritative_blind_label(
                artifact_id=contact_sheet["id"],
                action="build_quality_label",
            )
            is None
        ):
            raise VersionConflict(
                "publication requires one blind label bound to the exact contact sheet's hidden visual_judge evaluation"
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
        self._assert_shared_prototype_authority(
            attempt,
            content_hash=content_hash,
            require_approval_decision=False,
        )
        self._assert_no_blocking_failure(attempt_id, artifact_id)
        self._assert_blind_prototype_label(artifact_id)
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
        self._assert_no_blocking_failure(attempt_id, artifact_id)
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
            approved_hash = attempt.get("approved_prototype_hash")
            if not isinstance(approved_hash, str) or not approved_hash:
                raise VersionConflict("final override lacks an exact approved prototype authority")
            self._assert_shared_prototype_authority(
                attempt,
                content_hash=approved_hash,
                require_approval_decision=True,
            )
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

    def annotate_reject(
        self,
        *,
        attempt_id: str,
        artifact_id: str,
        content_hash: str,
        feedback: str,
        tags: Sequence[str],
        actor: str,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """Attach literal feedback to exact retained reject bytes without changing state."""

        self._human(actor)
        attempt = self.database.get_attempt(attempt_id)
        if attempt["purpose"] != Purpose.PRODUCTION:
            raise PermissionError("optimizer and technique artifacts cannot receive production annotations")
        if attempt["disposition"] not in {Disposition.MACHINE_REJECTED, Disposition.HUMAN_REJECTED}:
            raise VersionConflict("feedback-only annotation requires a retained machine or human reject")
        if not feedback.strip():
            raise ValueError("feedback-only annotation requires nonempty literal feedback")
        artifact = self.database.get_artifact(artifact_id)
        if artifact["attempt_id"] != attempt_id:
            raise VersionConflict("annotation artifact does not belong to attempt")
        if artifact["content_hash"] != content_hash:
            raise VersionConflict("annotation content hash does not name the exact retained artifact")
        request_key = self._child_request_key(
            action="feedback_only",
            actor=actor,
            parent_attempt_id=attempt_id,
            exact_input={"artifact_id": artifact_id, "content_hash": content_hash},
            idempotency_key=idempotency_key,
        )
        decision = self.database.add_human_decision(
            artifact_id=artifact_id,
            attempt_id=attempt_id,
            action="feedback_only",
            feedback=feedback,
            tags=tags,
            actor=actor,
            attempt_version=attempt["version"],
            content_hash=content_hash,
            idempotency_key=f"review-decision:{request_key}",
        )
        return {"decision": decision, "attempt": self.database.get_attempt(attempt_id)}

    async def retry(
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
        parent, cancellation = await self._cancel_final_review_request(parent)
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
        result = {"decision": decision, "attempt": child}
        if cancellation is not None:
            result["operation"] = cancellation
        return result

    def _asset_re_evaluation_source(
        self,
        *,
        attempt_id: str,
        artifact: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """Validate and preserve the exact artifact selected by the human."""

        del attempt_id
        metadata = json.loads(artifact["metadata_json"])
        asset_index = metadata.get("asset_index")
        if not isinstance(asset_index, int) or asset_index < 0:
            raise VersionConflict("asset re-evaluation artifact has no valid asset index")
        return artifact, {
            **metadata,
            "asset_index": asset_index,
            "re_evaluation_requested_artifact_id": artifact["id"],
            "re_evaluation_requested_kind": artifact["kind"],
        }

    async def re_evaluate(
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
        asset_metadata: dict[str, Any] | None = None
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
        elif artifact["kind"] in {
            ArtifactKind.SOURCE_ASSET,
            ArtifactKind.FORGE_MANIFEST,
            ArtifactKind.TEXTURE_VARIANT,
        }:
            evaluation_artifact, asset_metadata = self._asset_re_evaluation_source(
                attempt_id=attempt_id,
                artifact=artifact,
            )
            stage = Stage.ASSETS
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
        if artifact["kind"] in {
            ArtifactKind.SOURCE_ASSET,
            ArtifactKind.FORGE_MANIFEST,
            ArtifactKind.TEXTURE_VARIANT,
        }:
            assert asset_metadata is not None
            metadata = asset_metadata
        else:
            metadata = json.loads(evaluation_artifact["metadata_json"])
        provenance = json.loads(evaluation_artifact["provenance_json"])
        parent, cancellation = await self._cancel_final_review_request(parent)
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
        result = {"decision": decision, "attempt": child, "artifact": linked}
        if cancellation is not None:
            result["operation"] = cancellation
        return result

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
        self._assert_publication_authority_has_no_blocking_failure(attempt)
        self._assert_blind_build_label(attempt_id)
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
        approved_hash = attempt.get("approved_prototype_hash")
        if not isinstance(approved_hash, str) or not approved_hash:
            raise VersionConflict("final review lacks an exact approved prototype authority")
        self._assert_shared_prototype_authority(
            attempt,
            content_hash=approved_hash,
            require_approval_decision=True,
        )
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
