from __future__ import annotations

import hashlib
import io
import json
import struct

import pytest
from PIL import Image

from snaketron_factory.db import canonical_json
from snaketron_factory.domain import OperationStatus, ProviderResult, Stage
from snaketron_factory.factory import Factory
from snaketron_factory.fal_media import PIXVERSE_TRANSITION_CAPABILITY
from snaketron_factory.operations import OperationJournal
from snaketron_factory.recovery import (
    RecoveredResultError,
    validate_recovered_result,
    validate_skin_authority_readback,
)


def _operation(database, objects, attempt, *, side_effect: str, role: str, request: dict):
    retained = objects.put(OperationJournal.request_payload(request))
    operation, _ = database.begin_operation(
        attempt_id=attempt["id"],
        stage=Stage.COMPLETE,
        idempotency_key=f"recovery-contract:{side_effect}",
        side_effect=side_effect,
        provider_role=role,
        request_hash=OperationJournal.request_hash(request),
        cost_reserved_micros=0,
        metadata={"request_ref": retained.uri, "request_sha256": retained.sha256},
    )
    return database.transition_operation(
        operation["id"],
        OperationStatus.INTENT,
        OperationStatus.RECONCILIATION_REQUIRED,
    )


def _validate(factory_config, database, objects, operation, payload, *, model="snaketron-api"):
    retained = objects.put(canonical_json(payload).encode())
    return validate_recovered_result(
        config=factory_config,
        operation=operation,
        database=database,
        objects=objects,
        result_hash=retained.uri,
        resolved_model=model,
        media_type=None,
    )


def _validate_image(factory_config, database, objects, operation, payload: bytes):
    retained = objects.put(payload)
    return validate_recovered_result(
        config=factory_config,
        operation=operation,
        database=database,
        objects=objects,
        result_hash=retained.uri,
        resolved_model="gemini-3-pro-image",
        media_type="image/png",
    )


def _corrupt_request_authority(operation: dict) -> dict:
    metadata = json.loads(operation["metadata_json"])
    missing_sha = "0" * 64
    metadata.update(
        {
            "request_ref": f"sha256:{missing_sha}",
            "request_sha256": missing_sha,
        }
    )
    return {**operation, "metadata_json": canonical_json(metadata)}


def _fal_video() -> tuple[bytes, dict]:
    def box(kind: bytes, payload: bytes = b"") -> bytes:
        return struct.pack(">I4s", len(payload) + 8, kind) + payload

    value = box(b"ftyp", b"isom\x00\x00\x02\x00isommp42") + box(b"mdat", b"pixels") + box(b"moov")
    return value, {
        "video": {
            "byte_limit": 1_000,
            "content_sha256": "sha256:" + hashlib.sha256(value).hexdigest(),
            "bytes": len(value),
            "reported_file": {
                "content_type_valid_string": True,
                "content_type": "video/mp4",
                "file_size_valid_integer": True,
                "file_size": len(value),
                "file_name_valid": True,
            },
            "download": {
                "content_type": "video/mp4",
                "content_length_valid_integer": True,
                "content_length": len(value),
            },
        }
    }


def test_recovered_structured_result_requires_exact_retained_request(
    factory_config, database, objects, make_attempt
) -> None:
    operation = _operation(
        database,
        objects,
        make_attempt(stage=Stage.CONCEPT),
        side_effect="generate_concept",
        role="smart_text",
        request={"prompt": "the immutable concept prompt"},
    )
    proposal = {
        "name": "Recovered authority",
        "brief": "A complete recovered concept used to prove exact request retention.",
        "tags": ["recovery"],
        "seed": "exact-request",
        "palette_intent": "high contrast",
        "motion_intent": "steady",
        "implementation_hint": "layers",
        "implementation_rationale": "Layered geometry is sufficient for the bounded design.",
        "novelty_score": 0.8,
        "direction_score": 0.9,
        "novelty_rationale": "The design is distinct enough for this request-authority regression.",
    }

    with pytest.raises(RecoveredResultError, match="no immutable journaled request"):
        _validate(
            factory_config,
            database,
            objects,
            {**operation, "metadata_json": "{}"},
            proposal,
            model="gemini-3.7-flash",
        )
    with pytest.raises(RecoveredResultError, match="not valid retained JSON"):
        _validate(
            factory_config,
            database,
            objects,
            _corrupt_request_authority(operation),
            proposal,
            model="gemini-3.7-flash",
        )


def test_recovered_image_requires_exact_retained_request(factory_config, database, objects, make_attempt) -> None:
    operation = _operation(
        database,
        objects,
        make_attempt(stage=Stage.PROTOTYPE),
        side_effect="generate_prototype_image",
        role="image_generator",
        request={"prompt": "the immutable prototype prompt"},
    )
    output = io.BytesIO()
    Image.new("RGBA", (3, 2), (20, 40, 60, 255)).save(output, format="PNG")
    png = output.getvalue()

    with pytest.raises(RecoveredResultError, match="no immutable journaled request"):
        _validate_image(
            factory_config,
            database,
            objects,
            {**operation, "metadata_json": "{}"},
            png,
        )
    with pytest.raises(RecoveredResultError, match="not valid retained JSON"):
        _validate_image(
            factory_config,
            database,
            objects,
            _corrupt_request_authority(operation),
            png,
        )


def test_recovered_fal_submit_requires_exact_queue_ticket_contract(
    factory_config, database, objects, make_attempt
) -> None:
    operation = _operation(
        database,
        objects,
        make_attempt(stage=Stage.AUTHOR),
        side_effect="fal_transition_submit",
        role="fal_pixverse_transition",
        request={"operation": "submit_transition", "request_id": "draft-request-1"},
    )
    ticket = {
        "schema_version": 1,
        "capability_id": PIXVERSE_TRANSITION_CAPABILITY,
        "request_id": "fal-ticket-123",
    }
    retained = objects.put(canonical_json(ticket).encode())
    validated = validate_recovered_result(
        config=factory_config,
        operation=operation,
        database=database,
        objects=objects,
        result_hash=retained.uri,
        resolved_model=PIXVERSE_TRANSITION_CAPABILITY,
        media_type=None,
        provider_request_id="fal-ticket-123",
    )
    assert validated.value == ticket
    with pytest.raises(RecoveredResultError, match="differs from its queue ticket"):
        validate_recovered_result(
            config=factory_config,
            operation=operation,
            database=database,
            objects=objects,
            result_hash=retained.uri,
            resolved_model=PIXVERSE_TRANSITION_CAPABILITY,
            media_type=None,
            provider_request_id="different-ticket",
        )


def test_recovered_publication_result_binds_exact_request_response_and_server_identity(
    factory_config, database, objects, make_attempt
) -> None:
    attempt = make_attempt(stage=Stage.FINAL_REVIEW)
    request = {
        "skin_id": "skin-17",
        "revision": 4,
        "content_ref": "sha256:" + "4" * 64,
        "reason": "exact approval",
    }
    operation = _operation(
        database,
        objects,
        attempt,
        side_effect="publish_exact_revision",
        role="human_operator",
        request=request,
    )
    exact = {
        "skinId": "skin-17",
        "publication": "published",
        "publishedRevision": 4,
        "contentRef": request["content_ref"],
    }
    assert _validate(factory_config, database, objects, operation, exact).value == exact

    with pytest.raises(RecoveredResultError, match="different skin"):
        _validate(factory_config, database, objects, operation, {**exact, "skinId": "skin-18"})
    with pytest.raises(RecoveredResultError, match=r"publishedRevision|different revision"):
        _validate(factory_config, database, objects, operation, {**exact, "publishedRevision": 5})
    with pytest.raises(RecoveredResultError, match="pinned identity"):
        _validate(factory_config, database, objects, operation, exact, model="operator-typed-arbitrary-server")


@pytest.mark.parametrize(
    ("side_effect", "role", "payload"),
    [
        ("request_exact_publication_review", "snaketron_api", {"accepted": True}),
        ("request_exact_publication_review_after_override", "human_operator", {"accepted": True}),
        ("cancel_exact_publication_request", "human_operator", {"cancelled": True}),
    ],
)
def test_recovered_publication_request_and_cancel_require_exact_confirmation(
    factory_config, database, objects, make_attempt, side_effect, role, payload
) -> None:
    request = {
        "skin_id": "skin-22",
        "revision": 6,
        "content_ref": "sha256:" + "6" * 64,
    }
    operation = _operation(
        database,
        objects,
        make_attempt(stage=Stage.FINAL_REVIEW),
        side_effect=side_effect,
        role=role,
        request=request,
    )
    assert _validate(factory_config, database, objects, operation, payload).value == payload
    with pytest.raises(RecoveredResultError, match="confirm"):
        _validate(factory_config, database, objects, operation, {})


def test_recovered_registration_binds_document_skin_and_next_revision(
    factory_config, database, objects, make_attempt
) -> None:
    document = {"schema_version": 2, "name": "Exact", "layers": []}
    content_ref = "sha256:" + hashlib.sha256(canonical_json(document).encode()).hexdigest()
    request = {
        "skin_id": "skin-9",
        "expected_head_revision": 7,
        "document": document,
    }
    operation = _operation(
        database,
        objects,
        make_attempt(stage=Stage.REGISTER),
        side_effect="append_private_skin_revision",
        role="snaketron_api",
        request=request,
    )
    response = {"skinId": "skin-9", "headRevision": 8, "contentRef": content_ref}
    assert _validate(factory_config, database, objects, operation, response).value == response
    with pytest.raises(RecoveredResultError, match="exact requested document"):
        _validate(factory_config, database, objects, operation, {**response, "contentRef": "sha256:" + "0" * 64})
    with pytest.raises(RecoveredResultError, match="different skin"):
        _validate(factory_config, database, objects, operation, {**response, "skinId": "skin-10"})


def test_recovered_git_promotion_requires_exact_request_and_committed_active_pointer(
    factory_config, database, objects, make_attempt
) -> None:
    run_id = "opt-run-1"
    request = {
        "run_id": run_id,
        "candidate": "winner",
        "candidate_skill_sha": "candidate-sha",
        "expected_active_sha": "a" * 40,
    }
    operation = _operation(
        database,
        objects,
        make_attempt(stage=Stage.COMPLETE),
        side_effect="promote_authoring_playbook",
        role="git_promotion",
        request=request,
    )
    response = {
        "git_ref": f"refs/tags/skin-authoring/{run_id}",
        "sha": "b" * 40,
        "branch": f"bot/skin-authoring/{run_id}",
    }
    with pytest.raises(RecoveredResultError, match="active behavior pointer"):
        _validate(factory_config, database, objects, operation, response, model="git-promotion-v1")
    database.set_active_behavior("author-skin", response["git_ref"], response["sha"])
    assert (
        _validate(
            factory_config,
            database,
            objects,
            operation,
            response,
            model="git-promotion-v1",
        ).value
        == response
    )
    with pytest.raises(RecoveredResultError, match="optimization request"):
        _validate(
            factory_config,
            database,
            objects,
            operation,
            {**response, "git_ref": "refs/tags/skin-authoring/someone-else"},
            model="git-promotion-v1",
        )


def test_authenticated_skin_readback_is_exact_for_pending_cancelled_and_published_authority() -> None:
    request = {
        "skin_id": "17",
        "revision": 4,
        "content_ref": "sha256:" + "4" * 64,
    }
    validate_skin_authority_readback(
        side_effect="request_exact_publication_review",
        request=request,
        authority={"skinId": 17, "pendingRevision": 4},
    )
    validate_skin_authority_readback(
        side_effect="cancel_exact_publication_request",
        request=request,
        authority={"skinId": 17, "pendingRevision": None, "publishedRevision": None},
    )
    validate_skin_authority_readback(
        side_effect="publish_exact_revision",
        request=request,
        authority={
            "skinId": 17,
            "publication": "published",
            "publishedRevision": 4,
            "contentRef": request["content_ref"],
        },
    )
    with pytest.raises(RecoveredResultError, match="pending revision"):
        validate_skin_authority_readback(
            side_effect="request_exact_publication_review",
            request=request,
            authority={"skinId": 17, "pendingRevision": 5},
        )
    with pytest.raises(RecoveredResultError, match="omitted publishedRevision"):
        validate_skin_authority_readback(
            side_effect="cancel_exact_publication_request",
            request=request,
            authority={"skinId": 17, "pendingRevision": None},
        )
    with pytest.raises(RecoveredResultError, match="already published"):
        validate_skin_authority_readback(
            side_effect="cancel_exact_publication_request",
            request=request,
            authority={"skinId": 17, "pendingRevision": None, "publishedRevision": 4},
        )


@pytest.mark.asyncio
async def test_recovered_fal_video_is_validated_again_on_exact_factory_replay(
    factory_config,
    database,
    objects,
    make_attempt,
    monkeypatch,
) -> None:
    attempt = make_attempt(stage=Stage.AUTHOR)
    request = {"operation": "poll_transition", "request_id": "fal-ticket-123"}
    operation = _operation(
        database,
        objects,
        attempt,
        side_effect="fal_transition_result",
        role="fal_pixverse_transition",
        request=request,
    )
    video, metadata = _fal_video()
    retained = objects.put(video)
    validated = validate_recovered_result(
        config=factory_config,
        operation=operation,
        database=database,
        objects=objects,
        result_hash=retained.uri,
        resolved_model=PIXVERSE_TRANSITION_CAPABILITY,
        media_type="video/mp4",
        provider_request_id="fal-ticket-123",
        result_metadata=metadata,
    )
    database.resolve_operation(
        operation_id=operation["id"],
        resolution="executed_result_recovered",
        evidence_ref="fal-dashboard:audit-123",
        result_hash=retained.uri,
        resolved_model=PIXVERSE_TRANSITION_CAPABILITY,
        provider_request_id="fal-ticket-123",
        media_type="video/mp4",
        result_metadata=validated.metadata,
        actor="human:test-operator",
    )
    assert database.get_operation(operation["id"])["status"] == OperationStatus.SUCCEEDED

    factory = Factory(factory_config, database=database, objects=objects)
    monkeypatch.setattr(factory, "_behavior_drift_reason", lambda *_args, **_kwargs: None)

    async def must_not_invoke() -> ProviderResult:
        raise AssertionError("an authenticated recovered result must replay without provider I/O")

    replayed, result = await factory._provider_call(
        attempt=database.get_attempt(attempt["id"]),
        stage=Stage.AUTHOR,
        key="recovery-contract:fal_transition_result",
        role="fal_pixverse_transition",
        side_effect="fal_transition_result",
        request=request,
        invoke=must_not_invoke,
    )
    assert replayed["id"] == operation["id"]
    assert result is None
    assert database.get_operation(operation["id"])["status"] == OperationStatus.SUCCEEDED
    await factory.close()
