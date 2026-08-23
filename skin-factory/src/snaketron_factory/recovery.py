"""Fail-closed validation for operator-recovered external results."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ValidationError

from .config import FactoryConfig
from .db import Database, canonical_json
from .domain import ConceptProposal, ProviderError, ProviderResult, VisualJudgment, WorkerResult
from .fal_media import (
    PIXVERSE_TRANSITION_CAPABILITY,
    FalQueueTicket,
    validate_pixverse_video_result,
)
from .objects import ObjectStore
from .operations import OperationJournal, validate_exact_image_bytes


class RecoveredResultError(ValueError):
    """Exact retained recovery evidence violates its operation contract."""


@dataclass(frozen=True)
class ValidatedRecoveredResult:
    value: Any
    metadata: dict[str, Any]
    size_bytes: int


_EXPECTED_ROLES: dict[str, str] = {
    "generate_concept": "smart_text",
    "generate_prototype_image": "image_generator",
    "task_worker_rollout": "task_worker",
    "upload_exact_forge_ladder": "snaketron_api",
    "create_private_skin_revision": "snaketron_api",
    "append_private_skin_revision": "snaketron_api",
    "request_exact_publication_review": "snaketron_api",
    "visual_judgment": "visual_judge",
    "generate_build_asset": "image_generator",
    "generate_build_asset_slice": "image_generator",
    "fal_transition_submit": "fal_pixverse_transition",
    "fal_transition_result": "fal_pixverse_transition",
    "classify_feedback_route": "smart_text",
    "gepa_reflective_proposal": "smart_text",
    "gepa_reflective_mutation": "smart_text",
    "evaluate_fixture_expected_properties": "visual_judge",
    "promote_authoring_playbook": "git_promotion",
    "mine_novel_authoring_technique": "smart_text",
    "promote_animation_technique": "git_promotion",
    "request_exact_publication_review_after_override": "human_operator",
    "cancel_exact_publication_request": "human_operator",
    "publish_exact_revision": "human_operator",
}

_REQUEST_BOUND_EFFECTS = frozenset(
    {
        "upload_exact_forge_ladder",
        "create_private_skin_revision",
        "append_private_skin_revision",
        "request_exact_publication_review",
        "promote_authoring_playbook",
        "promote_animation_technique",
        "request_exact_publication_review_after_override",
        "cancel_exact_publication_request",
        "publish_exact_revision",
    }
)


def validate_recovered_result(
    *,
    config: FactoryConfig,
    operation: dict[str, Any],
    database: Database,
    objects: ObjectStore,
    result_hash: str,
    resolved_model: str,
    media_type: str | None,
    provider_request_id: str | None = None,
    result_metadata: dict[str, Any] | None = None,
) -> ValidatedRecoveredResult:
    """Authenticate a recovered CAS object against one immutable operation.

    This function performs no database mutation. Callers validate first and
    only then record ``executed_result_recovered`` as a semantic success.
    """

    side_effect = str(operation.get("side_effect") or "")
    role = str(operation.get("provider_role") or "")
    expected_role = _EXPECTED_ROLES.get(side_effect)
    if expected_role is not None and role != expected_role:
        raise RecoveredResultError(
            f"operation {operation.get('id')} side effect {side_effect!r} requires role {expected_role!r}, not {role!r}"
        )
    _validate_model_identity(config, operation, role, resolved_model)
    try:
        exact = objects.get(result_hash)
    except (OSError, RuntimeError, ValueError) as error:
        raise RecoveredResultError(f"recovered result is not a valid retained object: {error}") from error
    # Recovery authority consists of both immutable halves of the operation:
    # the exact request that was sent and the exact bytes claimed as its
    # result.  Even a valid image or schema-conforming JSON object is not
    # replayable when its request evidence is missing or has been tampered.
    request = _load_exact_request(operation, objects)

    if side_effect == "fal_transition_result":
        if media_type != "video/mp4":
            raise RecoveredResultError("recovered Fal result requires exact media_type video/mp4")
        if not isinstance(result_metadata, dict):
            raise RecoveredResultError("recovered Fal result requires bounded provider result metadata")
        if not isinstance(provider_request_id, str) or not provider_request_id:
            raise RecoveredResultError("recovered Fal result requires its exact retained request_id")
        if request.get("request_id") != provider_request_id:
            raise RecoveredResultError("recovered Fal result request_id differs from its exact poll request")
        recovered = ProviderResult(
            value=exact,
            request_id=provider_request_id,
            resolved_model=resolved_model,
            sanitized_metadata=result_metadata,
            usage={"usage_complete": False},
        )
        try:
            validate_pixverse_video_result(recovered)
        except ProviderError as error:
            raise RecoveredResultError(str(error)) from error
        return ValidatedRecoveredResult(
            value=exact,
            metadata={
                "result": {"kind": "video", "media_type": "video/mp4"},
                "video": recovered.sanitized_metadata["video"],
            },
            size_bytes=len(exact),
        )

    if role in {"image_generator", "image_editor"}:
        if media_type is None:
            raise RecoveredResultError("recovered image result requires its exact media type")
        try:
            trusted = validate_exact_image_bytes(exact, media_type)
        except ProviderError as error:
            raise RecoveredResultError(str(error)) from error
        return ValidatedRecoveredResult(
            value=exact,
            metadata={"result": trusted},
            size_bytes=len(exact),
        )

    if media_type is not None:
        raise RecoveredResultError(f"recovered structured role {role!r} does not accept a media type")
    if result_metadata is not None:
        raise RecoveredResultError(f"recovered structured role {role!r} does not accept result metadata")
    try:
        value = json.loads(exact)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RecoveredResultError(f"recovered structured result is not exact UTF-8 JSON: {error}") from error
    if not isinstance(value, dict):
        raise RecoveredResultError("recovered structured result must be a JSON object")
    if side_effect == "fal_transition_submit":
        try:
            ticket = FalQueueTicket.from_value(value)
        except ProviderError as error:
            raise RecoveredResultError(str(error)) from error
        if provider_request_id is not None and provider_request_id != ticket.request_id:
            raise RecoveredResultError("recovered Fal submit request_id differs from its queue ticket")
        return ValidatedRecoveredResult(
            value=ticket.as_dict(),
            metadata={"result": {"kind": "structured", "contract": "fal_queue_ticket_v1"}},
            size_bytes=len(exact),
        )
    model = _structured_contract(side_effect)
    if model is not None:
        try:
            model.model_validate(value)
        except ValidationError as error:
            raise RecoveredResultError(
                f"recovered result violates the {side_effect} structured contract: {error}"
            ) from error
    _validate_external_result_shape(side_effect, value, request=request, database=database)
    return ValidatedRecoveredResult(
        value=value,
        metadata={"result": {"kind": "structured", "contract": side_effect or "generic_object"}},
        size_bytes=len(exact),
    )


def _validate_model_identity(
    config: FactoryConfig,
    operation: dict[str, Any],
    role: str,
    resolved_model: str,
) -> None:
    if not resolved_model.strip():
        raise RecoveredResultError("recovered result requires the exact resolved model identity")
    config_role_name = "task_worker" if role == "task_worker" else role
    role_config = getattr(config.models, config_role_name, None)
    if role_config is not None and not role_config.accepts_resolved_model(resolved_model):
        expected = role_config.resolved_model_pattern or role_config.model
        raise RecoveredResultError(
            f"recovered {role} model {resolved_model!r} violates pinned identity {expected!r} "
            f"for operation {operation.get('id')}"
        )
    if role in {"snaketron_api", "human_operator"} and not config.service.accepts_resolved_model(resolved_model):
        raise RecoveredResultError(
            f"recovered Snaketron API model {resolved_model!r} violates pinned identity "
            f"{config.service.resolved_model_pattern!r} for operation {operation.get('id')}"
        )
    if role == "git_promotion" and resolved_model != "git-promotion-v1":
        raise RecoveredResultError(f"recovered Git promotion model {resolved_model!r} is not exact 'git-promotion-v1'")
    if role == "fal_pixverse_transition" and resolved_model != PIXVERSE_TRANSITION_CAPABILITY:
        raise RecoveredResultError(
            f"recovered Fal model {resolved_model!r} is not exact {PIXVERSE_TRANSITION_CAPABILITY!r}"
        )


def _structured_contract(side_effect: str) -> type[BaseModel] | None:
    if side_effect == "generate_concept":
        return ConceptProposal
    if side_effect == "task_worker_rollout":
        return WorkerResult
    if side_effect == "visual_judgment":
        return VisualJudgment
    if side_effect in {
        "classify_feedback_route",
        "gepa_reflective_proposal",
        "gepa_reflective_mutation",
        "evaluate_fixture_expected_properties",
    }:
        # Imported lazily because optimizer imports Factory for orchestration.
        from .optimizer import ExpectedPropertyJudgment, FeedbackClassification, GepaPopulation

        return {
            "classify_feedback_route": FeedbackClassification,
            "gepa_reflective_proposal": GepaPopulation,
            "gepa_reflective_mutation": GepaPopulation,
            "evaluate_fixture_expected_properties": ExpectedPropertyJudgment,
        }[side_effect]
    if side_effect == "mine_novel_authoring_technique":
        # Techniques likewise imports optimizer/Factory at orchestration time.
        from .techniques import TechniqueProposal

        return TechniqueProposal
    return None


def _load_exact_request(operation: dict[str, Any], objects: ObjectStore) -> dict[str, Any]:
    try:
        metadata = json.loads(operation.get("metadata_json") or "{}")
        request_ref = metadata["request_ref"]
        expected_sha = metadata["request_sha256"]
    except (KeyError, TypeError, ValueError) as error:
        raise RecoveredResultError("recovered external result has no immutable journaled request") from error
    if not isinstance(request_ref, str) or not isinstance(expected_sha, str):
        raise RecoveredResultError("recovered external result has malformed request authority")
    if request_ref != f"sha256:{expected_sha}":
        raise RecoveredResultError("journaled request reference and SHA disagree")
    try:
        request = json.loads(objects.get(request_ref))
    except (OSError, RuntimeError, UnicodeDecodeError, ValueError, json.JSONDecodeError) as error:
        raise RecoveredResultError(f"journaled request payload is not valid retained JSON: {error}") from error
    if not isinstance(request, dict):
        raise RecoveredResultError("journaled request payload is not a JSON object")
    if OperationJournal.request_hash(request) != operation.get("request_hash"):
        raise RecoveredResultError("journaled request payload does not match the operation request hash")
    return request


def _value_alias(value: dict[str, Any], camel: str, snake: str) -> Any:
    return value[camel] if camel in value else value.get(snake)


def _content_ref(document: Any) -> str:
    return "sha256:" + hashlib.sha256(canonical_json(document).encode("utf-8")).hexdigest()


def validate_registration_result(
    *,
    side_effect: str,
    request: dict[str, Any],
    response: dict[str, Any],
) -> None:
    """Apply the exact registration contract to a live or recovered response."""

    if side_effect not in {"create_private_skin_revision", "append_private_skin_revision"}:
        raise ValueError("registration validation requires a create or append side effect")
    skin_id = _value_alias(response, "skinId", "skin_id")
    revision = _value_alias(response, "headRevision", "head_revision")
    content_ref = _value_alias(response, "contentRef", "content_ref")
    if skin_id is None or revision is None or not isinstance(content_ref, str) or not content_ref:
        raise RecoveredResultError("registration result omitted skinId/headRevision/contentRef")
    if content_ref != _content_ref(request.get("document")):
        raise RecoveredResultError("registration contentRef does not name the exact requested document")
    if side_effect == "create_private_skin_revision" and int(revision) != 1:
        raise RecoveredResultError("initial skin response did not create revision 1")
    if side_effect == "append_private_skin_revision":
        if str(skin_id) != str(request.get("skin_id")):
            raise RecoveredResultError("append response names a different skin")
        expected_revision = request.get("expected_head_revision")
        if not isinstance(expected_revision, int) or int(revision) != expected_revision + 1:
            raise RecoveredResultError("append response is not the requested next revision")


def _validate_external_result_shape(
    side_effect: str,
    value: dict[str, Any],
    *,
    request: dict[str, Any] | None,
    database: Database,
) -> None:
    if side_effect in _REQUEST_BOUND_EFFECTS and request is None:
        raise RecoveredResultError(f"recovered {side_effect} result has no exact request authority")
    if side_effect in {"create_private_skin_revision", "append_private_skin_revision"}:
        assert request is not None
        validate_registration_result(side_effect=side_effect, request=request, response=value)
    if side_effect == "upload_exact_forge_ladder":
        assert request is not None
        response_ref = _value_alias(value, "contentRef", "content_ref")
        requested_ref = request.get("content_ref")
        if not isinstance(requested_ref, str) or response_ref != requested_ref:
            raise RecoveredResultError("recovered forge upload response does not bind the exact manifest contentRef")
    if side_effect in {
        "request_exact_publication_review",
        "request_exact_publication_review_after_override",
    }:
        assert request is not None
        if value.get("accepted") is not True and value.get("pending") is not True:
            raise RecoveredResultError("recovered publication-request response did not confirm acceptance")
        _validate_optional_publication_identity(value, request)
    if side_effect == "cancel_exact_publication_request":
        assert request is not None
        if value.get("cancelled") is not True:
            raise RecoveredResultError("recovered publication cancellation did not confirm cancellation")
        _validate_optional_publication_identity(value, request)
    if side_effect == "publish_exact_revision":
        assert request is not None
        if value.get("publication") != "published":
            raise RecoveredResultError("recovered publication response did not confirm published state")
        _validate_required_publication_identity(value, request)
    if side_effect in {"promote_authoring_playbook", "promote_animation_technique"} and (
        not isinstance(value.get("git_ref"), str) or not isinstance(value.get("sha"), str)
    ):
        raise RecoveredResultError("recovered promotion result omitted git_ref/sha")
    if side_effect in {"promote_authoring_playbook", "promote_animation_technique"}:
        assert request is not None
        identity = request.get("run_id") or request.get("candidate_id")
        expected_ref = f"refs/tags/skin-authoring/{identity}"
        expected_branch = f"bot/skin-authoring/{identity}"
        if not isinstance(identity, str) or value.get("git_ref") != expected_ref:
            raise RecoveredResultError("recovered promotion ref does not match the exact optimization request")
        if value.get("branch") != expected_branch:
            raise RecoveredResultError("recovered promotion branch does not match the exact optimization request")
        active = database.active_behavior("author-skin")
        if active is None or active.get("git_ref") != value.get("git_ref") or active.get("sha") != value.get("sha"):
            raise RecoveredResultError(
                "recovered Git promotion is not the exact already-committed active behavior; "
                "verify the signed remote ref and reconcile the active behavior pointer first"
            )


def _validate_optional_publication_identity(value: dict[str, Any], request: dict[str, Any]) -> None:
    aliases = {
        "skin_id": ("skin_id", "skinId"),
        "revision": ("revision", "pendingRevision", "publishedRevision"),
        "content_ref": ("content_ref", "contentRef"),
    }
    for request_name, response_names in aliases.items():
        supplied = next((value[name] for name in response_names if name in value), None)
        if supplied is not None and str(supplied) != str(request.get(request_name)):
            raise RecoveredResultError(f"recovered publication response changed exact {request_name}")


def _validate_required_publication_identity(value: dict[str, Any], request: dict[str, Any]) -> None:
    skin_id = _value_alias(value, "skinId", "skin_id")
    revision = value.get("publishedRevision", value.get("revision"))
    content_ref = _value_alias(value, "contentRef", "content_ref")
    if str(skin_id) != str(request.get("skin_id")):
        raise RecoveredResultError("recovered publication response names a different skin")
    if str(revision) != str(request.get("revision")):
        raise RecoveredResultError("recovered publication response names a different revision")
    if content_ref != request.get("content_ref"):
        raise RecoveredResultError("recovered publication response names a different contentRef")


def validate_skin_authority_readback(
    *,
    side_effect: str,
    request: dict[str, Any],
    authority: dict[str, Any],
) -> None:
    """Prove server state before a replay advances local review/registration."""

    skin_id = _value_alias(authority, "skinId", "skin_id")
    expected_skin = request.get("skin_id")
    if expected_skin is not None and str(skin_id) != str(expected_skin):
        raise RecoveredResultError("Snaketron readback names a different skin")
    if side_effect in {"create_private_skin_revision", "append_private_skin_revision"}:
        expected_revision = 1
        if side_effect == "append_private_skin_revision":
            previous = request.get("expected_head_revision")
            if not isinstance(previous, int):
                raise RecoveredResultError("append readback has no requested prior revision")
            expected_revision = previous + 1
        revision = _value_alias(authority, "headRevision", "head_revision")
        content_ref = _value_alias(authority, "contentRef", "content_ref")
        try:
            actual_revision = int(revision)
        except (TypeError, ValueError) as error:
            raise RecoveredResultError("Snaketron registration readback omitted its head revision") from error
        if actual_revision != expected_revision or content_ref != _content_ref(request.get("document")):
            raise RecoveredResultError("Snaketron registration readback does not match the exact document/revision")
        return
    revision = request.get("revision")
    if side_effect in {
        "request_exact_publication_review",
        "request_exact_publication_review_after_override",
    }:
        pending = _value_alias(authority, "pendingRevision", "pending_revision")
        if str(pending) != str(revision):
            raise RecoveredResultError("Snaketron readback does not retain the exact pending revision")
        return
    if side_effect == "cancel_exact_publication_request":
        pending = _value_alias(authority, "pendingRevision", "pending_revision")
        if str(pending) == str(revision):
            raise RecoveredResultError("Snaketron readback still retains the cancelled exact revision")
        if "publishedRevision" not in authority and "published_revision" not in authority:
            raise RecoveredResultError(
                "Snaketron cancellation readback omitted publishedRevision and cannot prove "
                "the exact revision is unpublished"
            )
        published = _value_alias(authority, "publishedRevision", "published_revision")
        if str(published) == str(revision):
            raise RecoveredResultError(
                "Snaketron cancellation readback shows the supposedly cancelled exact revision is already published"
            )
        return
    if side_effect == "publish_exact_revision":
        publication = authority.get("publication")
        published = _value_alias(authority, "publishedRevision", "published_revision")
        content_ref = _value_alias(authority, "contentRef", "content_ref")
        if publication != "published" or str(published) != str(revision) or content_ref != request.get("content_ref"):
            raise RecoveredResultError("Snaketron readback does not prove the exact published revision/contentRef")
