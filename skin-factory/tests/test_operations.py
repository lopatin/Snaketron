from __future__ import annotations

import io
import json

import pytest
from PIL import Image

from snaketron_factory.db import VersionConflict
from snaketron_factory.domain import (
    OperationStatus,
    ProviderError,
    ProviderFailureKind,
    ProviderResult,
    Stage,
)
from snaketron_factory.objects import ObjectStore
from snaketron_factory.operations import ExistingOperation, OperationJournal
from snaketron_factory.persistence import ResultPersistence

EXTERNAL_EFFECT_BOUNDARY_MATRIX = [
    ("generate_concept", Stage.CONCEPT, "smart_text"),
    ("generate_prototype_image", Stage.PROTOTYPE, "image_generator"),
    ("visual_judgment", Stage.PROTOTYPE_TRIAGE, "visual_judge"),
    ("task_worker_rollout", Stage.AUTHOR, "task_worker"),
    ("generate_build_asset", Stage.ASSETS, "image_generator"),
    ("generate_build_asset_slice", Stage.ASSETS, "image_generator"),
    ("upload_exact_forge_ladder", Stage.ASSETS, "snaketron_api"),
    ("create_private_skin_revision", Stage.REGISTER, "snaketron_api"),
    ("append_private_skin_revision", Stage.REGISTER, "snaketron_api"),
    ("request_exact_publication_review", Stage.BUILD_TRIAGE, "snaketron_api"),
    ("request_exact_publication_review_after_override", Stage.FINAL_REVIEW, "human_operator"),
    ("cancel_exact_publication_request", Stage.FINAL_REVIEW, "human_operator"),
    ("publish_exact_revision", Stage.FINAL_REVIEW, "human_operator"),
    ("classify_feedback_route", Stage.COMPLETE, "smart_text"),
    ("gepa_reflective_proposal", Stage.COMPLETE, "smart_text"),
    ("gepa_reflective_mutation", Stage.COMPLETE, "smart_text"),
    ("evaluate_fixture_expected_properties", Stage.COMPLETE, "visual_judge"),
    ("promote_authoring_playbook", Stage.COMPLETE, "git"),
    ("mine_novel_authoring_technique", Stage.COMPLETE, "smart_text"),
    ("promote_animation_technique", Stage.COMPLETE, "git"),
]


@pytest.mark.asyncio
async def test_success_persists_result_before_committing_operation_and_charges_actual_cost(
    database, objects: ObjectStore, make_attempt
) -> None:
    attempt = make_attempt()
    journal = OperationJournal(database)
    persistence = ResultPersistence(objects)
    observed_statuses: list[str] = []

    def persist(result: ProviderResult) -> str:
        with database.connect() as connection:
            observed_statuses.append(
                connection.execute("SELECT status FROM operation WHERE idempotency_key='success-key'").fetchone()[0]
            )
        return persistence(result)

    operation, result = await journal.run_provider(
        attempt_id=attempt["id"],
        stage=Stage.CONCEPT,
        idempotency_key="success-key",
        side_effect="generate_concept",
        provider_role="smart_text",
        request={"prompt": "one"},
        reserve_micros=1_000,
        invoke=lambda: ProviderResult(
            value={"answer": 42},
            request_id="provider-123",
            resolved_model="gemini-pinned",
            sanitized_metadata={"safe": True},
            usage={"cost_micros": 321, "input_tokens": 12},
        ),
        persist_result=persist,
    )

    assert observed_statuses == [OperationStatus.RUNNING]
    assert operation["status"] == OperationStatus.SUCCEEDED
    assert operation["provider_request_id"] == "provider-123"
    assert operation["resolved_model"] == "gemini-pinned"
    assert operation["cost_charged_micros"] == 321
    assert json.loads(objects.get(operation["result_hash"])) == {"answer": 42}
    assert result is not None and result.value == {"answer": 42}
    current = database.get_attempt(attempt["id"])
    assert current["cost_reserved_micros"] == 1_000
    assert current["cost_charged_micros"] == 321
    assert database.total_cost() == 321

    replay, replay_result = await journal.run_provider(
        attempt_id=attempt["id"],
        stage=Stage.CONCEPT,
        idempotency_key="success-key",
        side_effect="generate_concept",
        provider_role="smart_text",
        request={"prompt": "one"},
        reserve_micros=1_000,
        invoke=lambda: pytest.fail("a successful operation must never execute twice"),
        persist_result=persist,
    )
    assert replay["id"] == operation["id"]
    assert replay_result is None


@pytest.mark.asyncio
async def test_success_without_complete_provider_usage_keeps_full_reservation_charged(
    database, objects: ObjectStore, make_attempt
) -> None:
    attempt = make_attempt()
    journal = OperationJournal(database)
    persistence = ResultPersistence(objects)

    operation, _ = await journal.run_provider(
        attempt_id=attempt["id"],
        stage=Stage.PROTOTYPE,
        idempotency_key="missing-provider-usage",
        side_effect="generate_prototype_image",
        provider_role="image_generator",
        request={"prompt": "bounded"},
        reserve_micros=987_654,
        invoke=lambda: ProviderResult(
            value={"answer": "retained"},
            resolved_model="gemini-image-pinned",
            usage={
                "input_tokens": 0,
                "output_tokens": 0,
                "usage_complete": False,
                "known_image_cost_micros": 10_000,
            },
        ),
        persist_result=persistence,
    )

    assert operation["status"] == OperationStatus.SUCCEEDED
    assert operation["cost_charged_micros"] == 987_654
    assert database.get_attempt(attempt["id"])["cost_charged_micros"] == 987_654
    assert database.total_cost() == 987_654


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "outcome_known", "expected_status", "retry_class", "charge"),
    [
        (ProviderFailureKind.AUTHENTICATION, True, OperationStatus.FAILED_RETRYABLE, "safe_new_key", 0),
        (ProviderFailureKind.TIMEOUT, True, OperationStatus.FAILED_RETRYABLE, "safe_new_key", 0),
        (ProviderFailureKind.UNAVAILABLE, True, OperationStatus.FAILED_RETRYABLE, "safe_new_key", 0),
        (ProviderFailureKind.QUOTA, True, OperationStatus.FAILED_RETRYABLE, "safe_new_key", 0),
        (ProviderFailureKind.TIMEOUT, False, OperationStatus.RECONCILIATION_REQUIRED, "unknown", 700),
        (ProviderFailureKind.UNAVAILABLE, False, OperationStatus.RECONCILIATION_REQUIRED, "unknown", 700),
        (ProviderFailureKind.QUOTA, False, OperationStatus.RECONCILIATION_REQUIRED, "unknown", 700),
        (ProviderFailureKind.REFUSAL, True, OperationStatus.FAILED_TERMINAL, "terminal", 700),
        (ProviderFailureKind.INVALID_OUTPUT, True, OperationStatus.FAILED_TERMINAL, "terminal", 700),
        (ProviderFailureKind.UNKNOWN_OUTCOME, False, OperationStatus.RECONCILIATION_REQUIRED, "unknown", 700),
    ],
)
async def test_provider_failures_have_explicit_retry_and_cost_semantics(
    database,
    make_attempt,
    kind: ProviderFailureKind,
    outcome_known: bool,
    expected_status: OperationStatus,
    retry_class: str,
    charge: int,
) -> None:
    attempt = make_attempt()
    journal = OperationJournal(database)

    def fail() -> ProviderResult:
        raise ProviderError(
            kind,
            "injected",
            outcome_known=outcome_known,
            request_id="request-failure",
        )

    with pytest.raises(ProviderError) as captured:
        await journal.run_provider(
            attempt_id=attempt["id"],
            stage=Stage.PROTOTYPE,
            idempotency_key=f"failure:{kind}",
            side_effect="generate_image",
            provider_role="image_generator",
            request={"prompt": "x"},
            reserve_micros=700,
            invoke=fail,
        )
    assert captured.value.kind == kind
    with database.connect() as connection:
        operation = dict(
            connection.execute("SELECT * FROM operation WHERE idempotency_key=?", (f"failure:{kind}",)).fetchone()
        )
    assert operation["status"] == expected_status
    assert operation["retry_class"] == retry_class
    assert operation["cost_charged_micros"] == charge
    assert operation["provider_request_id"] == "request-failure"
    assert database.get_attempt(attempt["id"])["cost_charged_micros"] == charge


@pytest.mark.asyncio
async def test_returned_invalid_image_is_retained_as_quarantined_terminal_evidence(
    database, objects, make_attempt
) -> None:
    attempt = make_attempt(stage=Stage.PROTOTYPE)
    journal = OperationJournal(database)
    persistence = ResultPersistence(objects)
    exact = b"paid image bytes with an invalid declared type"

    with pytest.raises(ProviderError, match="supported media_type"):
        await journal.run_provider(
            attempt_id=attempt["id"],
            stage=Stage.PROTOTYPE,
            idempotency_key="quarantined-invalid-image",
            side_effect="generate_prototype_image",
            provider_role="image_generator",
            request={"prompt": "one paid result"},
            reserve_micros=700,
            invoke=lambda: ProviderResult(
                value={"image": exact, "media_type": "image/gif"},
                request_id="paid-request-1",
                resolved_model="unexpected-image-model",
                usage={"cost_micros": 456},
            ),
            persist_result=persistence,
        )

    with database.connect() as connection:
        operation = dict(
            connection.execute("SELECT * FROM operation WHERE idempotency_key='quarantined-invalid-image'").fetchone()
        )
    assert operation["status"] == OperationStatus.FAILED_TERMINAL
    assert operation["result_hash"].startswith("sha256:")
    assert objects.get(operation["result_hash"]) == exact
    assert operation["provider_request_id"] == "paid-request-1"
    assert operation["resolved_model"] == "unexpected-image-model"
    assert operation["cost_charged_micros"] == 456
    assert json.loads(operation["metadata_json"])["result"] == {
        "kind": "image",
        "reported_media_type": "image/gif",
    }
    assert json.loads(operation["metadata_json"])["quarantined"] is True


@pytest.mark.asyncio
async def test_corrupt_supported_image_replay_never_repeats_spend_and_keeps_exact_bytes(
    database, objects, make_attempt
) -> None:
    attempt = make_attempt(stage=Stage.PROTOTYPE)
    journal = OperationJournal(database)
    persistence = ResultPersistence(objects)
    corrupt = b"\x89PNG\r\n\x1a\ntruncated-after-valid-magic"
    calls = 0

    def invoke() -> ProviderResult:
        nonlocal calls
        calls += 1
        return ProviderResult(
            value={"image": corrupt, "media_type": "image/png"},
            request_id="corrupt-gemini-response",
            resolved_model="gemini-3-pro-image-20260801",
            usage={"cost_micros": 555},
        )

    request = {"prompt": "one exact corrupted Gemini fixture"}
    with pytest.raises(ProviderError, match="decode validation"):
        await journal.run_provider(
            attempt_id=attempt["id"],
            stage=Stage.PROTOTYPE,
            idempotency_key="corrupt-supported-image",
            side_effect="generate_prototype_image",
            provider_role="image_generator",
            request=request,
            reserve_micros=700,
            invoke=invoke,
            persist_result=persistence,
        )
    assert calls == 1
    with database.connect() as connection:
        operation = dict(
            connection.execute("SELECT * FROM operation WHERE idempotency_key='corrupt-supported-image'").fetchone()
        )
    assert operation["status"] == OperationStatus.FAILED_TERMINAL
    assert objects.get(operation["result_hash"]) == corrupt
    assert operation["cost_charged_micros"] == 555

    with pytest.raises(ExistingOperation, match="already terminal"):
        await journal.run_provider(
            attempt_id=attempt["id"],
            stage=Stage.PROTOTYPE,
            idempotency_key="corrupt-supported-image",
            side_effect="generate_prototype_image",
            provider_role="image_generator",
            request=request,
            reserve_micros=700,
            invoke=invoke,
            persist_result=persistence,
        )
    assert calls == 1


@pytest.mark.asyncio
async def test_arbitrary_adapter_exception_is_unknown_and_never_blindly_replayed(database, make_attempt) -> None:
    attempt = make_attempt()
    journal = OperationJournal(database)

    def disconnect_after_send() -> ProviderResult:
        raise RuntimeError("socket vanished after request body")

    with pytest.raises(ProviderError) as captured:
        await journal.run_provider(
            attempt_id=attempt["id"],
            stage=Stage.PROTOTYPE,
            idempotency_key="unknown-call",
            side_effect="generate_image",
            provider_role="image_generator",
            request={"prompt": "x"},
            reserve_micros=900,
            invoke=disconnect_after_send,
        )
    assert captured.value.kind == ProviderFailureKind.UNKNOWN_OUTCOME
    assert not captured.value.outcome_known
    operation = database.unresolved_operations()[0]
    assert operation["status"] == OperationStatus.RECONCILIATION_REQUIRED
    assert operation["cost_charged_micros"] == 900

    with pytest.raises(ExistingOperation, match="requires authenticated reconciliation"):
        await journal.run_provider(
            attempt_id=attempt["id"],
            stage=Stage.PROTOTYPE,
            idempotency_key="unknown-call",
            side_effect="generate_image",
            provider_role="image_generator",
            request={"prompt": "x"},
            reserve_micros=900,
            invoke=lambda: pytest.fail("must not replay"),
        )


@pytest.mark.asyncio
async def test_restart_at_intent_is_safe_but_restart_at_running_requires_reconciliation(database, make_attempt) -> None:
    attempt = make_attempt()
    journal = OperationJournal(database)
    request = {"prompt": "same"}
    digest = journal.request_hash(request)

    intent, _ = database.begin_operation(
        attempt_id=attempt["id"],
        stage=Stage.CONCEPT,
        idempotency_key="crash-before-call",
        side_effect="generate",
        provider_role="smart_text",
        request_hash=digest,
        cost_reserved_micros=100,
    )
    resumed, result = await journal.run_provider(
        attempt_id=attempt["id"],
        stage=Stage.CONCEPT,
        idempotency_key="crash-before-call",
        side_effect="generate",
        provider_role="smart_text",
        request=request,
        reserve_micros=100,
        invoke=lambda: ProviderResult(value={"ok": True}, resolved_model="test"),
    )
    assert resumed["id"] == intent["id"]
    assert resumed["status"] == OperationStatus.SUCCEEDED
    assert result is not None

    running, _ = database.begin_operation(
        attempt_id=attempt["id"],
        stage=Stage.PROTOTYPE,
        idempotency_key="crash-during-call",
        side_effect="generate",
        provider_role="image_generator",
        request_hash=digest,
        cost_reserved_micros=200,
    )
    database.transition_operation(running["id"], OperationStatus.INTENT, OperationStatus.RUNNING)
    with pytest.raises(ExistingOperation, match="interrupted call"):
        await journal.run_provider(
            attempt_id=attempt["id"],
            stage=Stage.PROTOTYPE,
            idempotency_key="crash-during-call",
            side_effect="generate",
            provider_role="image_generator",
            request=request,
            reserve_micros=200,
            invoke=lambda: pytest.fail("must not invoke after RUNNING crash"),
        )
    reconciled = database.get_operation(running["id"])
    assert reconciled["status"] == OperationStatus.RECONCILIATION_REQUIRED
    assert reconciled["cost_charged_micros"] == 200


@pytest.mark.asyncio
@pytest.mark.parametrize(("side_effect", "stage", "role"), EXTERNAL_EFFECT_BOUNDARY_MATRIX)
async def test_every_external_effect_has_crash_safe_intent_call_and_result_boundaries(
    database,
    objects,
    make_attempt,
    side_effect: str,
    stage: Stage,
    role: str,
) -> None:
    """The M2 boundary matrix covers every production/meta external effect."""

    attempt = make_attempt(stage=stage)
    journal = OperationJournal(database)
    persistence = ResultPersistence(objects)
    request = {"side_effect": side_effect, "fixture": "exact-boundary-v1"}
    digest = journal.request_hash(request)

    # Crash before the adapter call: the durable INTENT is safe to resume.
    intent_key = f"boundary:{side_effect}:intent"
    intent, _ = database.begin_operation(
        attempt_id=attempt["id"],
        stage=stage,
        idempotency_key=intent_key,
        side_effect=side_effect,
        provider_role=role,
        request_hash=digest,
        cost_reserved_micros=10,
    )
    completed, returned = await journal.run_provider(
        attempt_id=attempt["id"],
        stage=stage,
        idempotency_key=intent_key,
        side_effect=side_effect,
        provider_role=role,
        request=request,
        reserve_micros=10,
        invoke=lambda: ProviderResult(
            value={"effect": side_effect, "exact": True},
            resolved_model=f"fixture-{role}",
            usage={"cost_micros": 7},
        ),
        persist_result=persistence,
    )
    assert completed["id"] == intent["id"]
    assert returned is not None
    assert persistence.load_json(completed["result_hash"]) == {
        "effect": side_effect,
        "exact": True,
    }

    # Crash after result persistence/stage write: exact replay consumes CAS
    # evidence and never repeats the external effect.
    replay, replay_result = await journal.run_provider(
        attempt_id=attempt["id"],
        stage=stage,
        idempotency_key=intent_key,
        side_effect=side_effect,
        provider_role=role,
        request=request,
        reserve_micros=10,
        invoke=lambda: pytest.fail(f"{side_effect} replayed after durable success"),
        persist_result=persistence,
    )
    assert replay["result_hash"] == completed["result_hash"]
    assert replay_result is None

    # Crash while the call is in flight: effect outcome is never guessed or
    # blindly retried; authenticated reconciliation owns the next transition.
    running_key = f"boundary:{side_effect}:running"
    running, _ = database.begin_operation(
        attempt_id=attempt["id"],
        stage=stage,
        idempotency_key=running_key,
        side_effect=side_effect,
        provider_role=role,
        request_hash=digest,
        cost_reserved_micros=10,
    )
    database.transition_operation(running["id"], OperationStatus.INTENT, OperationStatus.RUNNING)
    with pytest.raises(ExistingOperation, match="interrupted call"):
        await journal.run_provider(
            attempt_id=attempt["id"],
            stage=stage,
            idempotency_key=running_key,
            side_effect=side_effect,
            provider_role=role,
            request=request,
            reserve_micros=10,
            invoke=lambda: pytest.fail(f"{side_effect} retried with an unknown outcome"),
            persist_result=persistence,
        )
    assert database.get_operation(running["id"])["status"] == OperationStatus.RECONCILIATION_REQUIRED


def test_reconciliation_is_human_only_immutable_and_has_exact_cost_semantics(database, make_attempt) -> None:
    attempt = make_attempt()
    operation, _ = database.begin_operation(
        attempt_id=attempt["id"],
        stage=Stage.PROTOTYPE,
        idempotency_key="resolve-me",
        side_effect="image",
        provider_role="image_generator",
        request_hash="d" * 64,
        cost_reserved_micros=500,
    )
    operation = database.transition_operation(
        operation["id"],
        OperationStatus.INTENT,
        OperationStatus.RECONCILIATION_REQUIRED,
        retry_class="unknown",
    )
    with pytest.raises(PermissionError):
        database.resolve_operation(
            operation_id=operation["id"],
            resolution="confirmed_not_executed",
            evidence_ref="ticket:1",
            actor="service:hermes",
        )

    resolution = database.resolve_operation(
        operation_id=operation["id"],
        resolution="confirmed_not_executed",
        evidence_ref="provider-audit:absent",
        actor="human:operator",
    )
    assert resolution["resolution"] == "confirmed_not_executed"
    resolved = database.get_operation(operation["id"])
    assert resolved["status"] == OperationStatus.RESOLVED
    assert resolved["retry_class"] == "retry_safe"
    assert resolved["cost_charged_micros"] == 0
    with pytest.raises(VersionConflict):
        database.resolve_operation(
            operation_id=operation["id"],
            resolution="confirmed_not_executed",
            evidence_ref="duplicate",
            actor="human:operator",
        )

    lost, _ = database.begin_operation(
        attempt_id=attempt["id"],
        stage=Stage.PROTOTYPE,
        idempotency_key="lost-output",
        side_effect="image",
        provider_role="image_generator",
        request_hash="e" * 64,
        cost_reserved_micros=600,
    )
    database.transition_operation(lost["id"], OperationStatus.INTENT, OperationStatus.RECONCILIATION_REQUIRED)
    database.resolve_operation(
        operation_id=lost["id"],
        resolution="executed_output_lost",
        evidence_ref="provider-audit:charged",
        actor="human:operator",
    )
    assert database.get_operation(lost["id"])["cost_charged_micros"] == 600
    current = database.get_attempt(attempt["id"])
    assert current["disposition"] == "blocked"
    assert json.loads(current["failure_json"])["operation"] == lost["id"]


@pytest.mark.asyncio
async def test_recovered_result_becomes_semantic_success_and_replays_exact_cas_without_provider(
    database, objects, make_attempt
) -> None:
    attempt = make_attempt(stage=Stage.CONCEPT)
    journal = OperationJournal(database)
    persistence = ResultPersistence(objects)
    request = {"prompt": "recover me"}

    def unknown() -> ProviderResult:
        raise ProviderError(
            ProviderFailureKind.UNKNOWN_OUTCOME,
            "provider accepted but response was lost",
            outcome_known=False,
            request_id="lost-response",
        )

    with pytest.raises(ProviderError):
        await journal.run_provider(
            attempt_id=attempt["id"],
            stage=Stage.CONCEPT,
            idempotency_key="recovered-result",
            side_effect="generate_concept",
            provider_role="smart_text",
            request=request,
            reserve_micros=400,
            invoke=unknown,
        )
    operation = database.unresolved_operations()[0]
    recovered_value = {
        "name": "Recovered Aurora",
        "brief": "A complete retained concept recovered from the provider audit response.",
        "tags": ["aurora"],
        "seed": "recovered-seed",
        "palette_intent": "cool cyan and violet",
        "motion_intent": "slow luminous pulse",
        "implementation_hint": "layers",
        "implementation_rationale": "The effect is expressible with bounded authored layers.",
        "novelty_score": 0.8,
        "direction_score": 0.9,
        "novelty_rationale": "It differs from retained directions through its polar-light motion.",
    }
    recovered_ref = persistence(ProviderResult(value=recovered_value, resolved_model="operator-recovery"))
    with pytest.raises(ValueError, match="requires a valid sha256"):
        database.resolve_operation(
            operation_id=operation["id"],
            resolution="executed_result_recovered",
            evidence_ref="provider:audit",
            actor="human:operator",
        )
    with pytest.raises(ValueError, match="does not accept"):
        database.resolve_operation(
            operation_id=operation["id"],
            resolution="confirmed_not_executed",
            evidence_ref="provider:audit",
            result_hash=recovered_ref,
            actor="human:operator",
        )

    resolution = database.resolve_operation(
        operation_id=operation["id"],
        resolution="executed_result_recovered",
        evidence_ref="provider:audit:result-123",
        result_hash=recovered_ref,
        resolved_model="gemini-3.7-flash-20260801",
        provider_request_id="provider-result-123",
        actor="human:operator",
    )
    assert resolution["result_hash"] == recovered_ref
    succeeded = database.get_operation(operation["id"])
    assert succeeded["status"] == OperationStatus.SUCCEEDED
    assert succeeded["retry_class"] == "complete"
    assert succeeded["result_hash"] == recovered_ref
    assert succeeded["resolved_model"] == "gemini-3.7-flash-20260801"
    assert succeeded["provider_request_id"] == "provider-result-123"
    assert json.loads(succeeded["metadata_json"])["recovery"]["evidence_ref"] == "provider:audit:result-123"
    assert succeeded["cost_charged_micros"] == 400
    assert database.get_attempt(attempt["id"])["cost_charged_micros"] == 400

    replay, result = await journal.run_provider(
        attempt_id=attempt["id"],
        stage=Stage.CONCEPT,
        idempotency_key="recovered-result",
        side_effect="generate_concept",
        provider_role="smart_text",
        request=request,
        reserve_micros=400,
        invoke=lambda: pytest.fail("recovered success must not call provider"),
    )
    assert replay["id"] == operation["id"]
    assert result is None
    assert persistence.load_json(replay["result_hash"]) == recovered_value


@pytest.mark.asyncio
async def test_recovered_image_records_exact_media_model_and_request_metadata(database, objects, make_attempt) -> None:
    attempt = make_attempt(stage=Stage.PROTOTYPE)
    journal = OperationJournal(database)
    request = {"prompt": "recover exact image"}

    with pytest.raises(ProviderError):
        await journal.run_provider(
            attempt_id=attempt["id"],
            stage=Stage.PROTOTYPE,
            idempotency_key="recovered-image",
            side_effect="generate_prototype_image",
            provider_role="image_generator",
            request=request,
            reserve_micros=750_000,
            invoke=lambda: (_ for _ in ()).throw(
                ProviderError(
                    ProviderFailureKind.UNKNOWN_OUTCOME,
                    "provider accepted before response loss",
                    outcome_known=False,
                )
            ),
        )
    operation = database.unresolved_operations()[0]
    buffer = io.BytesIO()
    Image.new("RGB", (3, 2), (12, 34, 56)).save(buffer, format="WEBP", lossless=True)
    exact_webp = buffer.getvalue()
    recovered = objects.put(exact_webp)
    with pytest.raises(ValueError, match="require media_type"):
        database.resolve_operation(
            operation_id=operation["id"],
            resolution="executed_result_recovered",
            evidence_ref="provider:audit:image-1",
            result_hash=recovered.uri,
            resolved_model="gemini-3-pro-image-20260801",
            actor="human:operator",
        )
    database.resolve_operation(
        operation_id=operation["id"],
        resolution="executed_result_recovered",
        evidence_ref="provider:audit:image-1",
        result_hash=recovered.uri,
        resolved_model="gemini-3-pro-image-20260801",
        provider_request_id="image-request-1",
        media_type="image/webp",
        actor="human:operator",
    )

    replay, result = await journal.run_provider(
        attempt_id=attempt["id"],
        stage=Stage.PROTOTYPE,
        idempotency_key="recovered-image",
        side_effect="generate_prototype_image",
        provider_role="image_generator",
        request=request,
        reserve_micros=750_000,
        invoke=lambda: pytest.fail("recovered image must not call the provider"),
    )
    assert result is None
    assert replay["resolved_model"] == "gemini-3-pro-image-20260801"
    assert replay["provider_request_id"] == "image-request-1"
    assert json.loads(replay["metadata_json"])["result"] == {
        "kind": "image",
        "media_type": "image/webp",
    }
    assert objects.get(replay["result_hash"]) == exact_webp


def test_request_hash_is_canonical_and_does_not_embed_binary_payloads() -> None:
    first = OperationJournal.request_hash({"z": b"secret bytes", "a": [1, 2]})
    second = OperationJournal.request_hash({"a": [1, 2], "z": b"secret bytes"})
    changed = OperationJournal.request_hash({"a": [1, 2], "z": b"other"})
    assert first == second
    assert first != changed
