from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

from snaketron_factory.calibration import (
    PROTOTYPE_JUDGE_RUBRIC,
    JudgeCalibrationService,
    judge_evaluator_version,
    wilson_interval,
)
from snaketron_factory.config import FactoryConfig, load_config
from snaketron_factory.db import Database, VersionConflict
from snaketron_factory.domain import (
    ArtifactKind,
    Disposition,
    GateResult,
    GateVerdict,
    OperationStatus,
    Purpose,
    Stage,
    VisualJudgment,
)
from snaketron_factory.factory import Factory
from snaketron_factory.objects import ObjectStore
from snaketron_factory.outbox import (
    DeliveryResult,
    LocalGalleryDestination,
    OutboxDispatcher,
    WebhookDestination,
)

PROTOTYPE_EVALUATOR_VERSION = (
    "gemini-3.7-flash+rubric:"
    + hashlib.sha256((PROTOTYPE_JUDGE_RUBRIC + "\nvisual-judgment-schema-v1").encode()).hexdigest()[:16]
)


def _config(tmp_path: Path, *, mode: str = "production") -> FactoryConfig:
    root = Path(__file__).resolve().parents[1]
    config = load_config(root / "config" / "factory.yaml")
    config.mode = mode  # type: ignore[assignment]
    config.paths.database = tmp_path / "factory.sqlite3"
    config.paths.objects = tmp_path / "objects"
    return config


def _attempt(db: Database) -> dict[str, object]:
    concept = db.create_concept(name="calibration", brief="calibration fixture", seed="1", source="test", tags=[])
    attempt = db.create_attempt(
        concept_id=concept["id"],
        purpose=Purpose.PRODUCTION,
        stage=Stage.PROTOTYPE_REVIEW,
        idempotency_key="calibration-attempt",
        behavior={},
        direction_sha="d",
        skill_sha="s",
        capability_sha="c",
        gate_sha="g",
        model_config_sha="m",
    )
    return db.update_attempt(
        attempt["id"],
        attempt["version"],
        disposition=Disposition.NEEDS_HUMAN,
        review_kind="prototype",
    )


def _labeled_evaluation(
    db: Database,
    attempt: dict[str, object],
    index: int,
    *,
    verdict: GateVerdict,
    human_accepts: bool,
    evaluator_version: str = PROTOTYPE_EVALUATOR_VERSION,
    actor: str = "human:calibrator",
) -> dict[str, object]:
    digest = hashlib.sha256(f"artifact-{index}".encode()).hexdigest()
    artifact = db.add_artifact(
        attempt_id=str(attempt["id"]),
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        content_hash=digest,
        object_ref=f"sha256:{digest}",
        media_type="image/png",
        size_bytes=1,
    )
    db.add_evaluation(
        artifact_id=artifact["id"],
        attempt_id=str(attempt["id"]),
        evaluator="visual_judge",
        result=GateResult(
            gate="visual_fidelity",
            gate_version=evaluator_version,
            blocking=False,
            verdict=verdict,
        ),
        hidden_until_label=True,
    )
    label = {
        "artifact_id": artifact["id"],
        "attempt_id": str(attempt["id"]),
        "action": "prototype_label",
        "feedback": "independent blind label",
        "tags": [f"outcome:{'accept' if human_accepts else 'reject'}"],
        "actor": actor,
        "content_hash": digest,
    }
    if actor.startswith("human:"):
        db.add_blind_human_label(**label)
    else:
        db.add_human_decision(
            **label,
            attempt_version=int(attempt["version"]),
        )
    return artifact


def test_wilson_interval_is_fail_closed_for_empty_denominator() -> None:
    assert wilson_interval(0, 0, 0.95) == (0.0, 1.0)
    lower, upper = wilson_interval(0, 40, 0.95)
    assert lower == 0
    assert 0.08 < upper < 0.09


def test_calibration_uses_blind_human_labels_and_computes_confusion_bounds(tmp_path: Path) -> None:
    config = _config(tmp_path)
    db = Database(config.paths.database)
    db.migrate()
    attempt = _attempt(db)
    for index in range(18):
        _labeled_evaluation(db, attempt, index, verdict=GateVerdict.CANDIDATE, human_accepts=True)
    for index in range(18, 20):
        _labeled_evaluation(db, attempt, index, verdict=GateVerdict.MACHINE_REJECTED, human_accepts=True)
    _labeled_evaluation(db, attempt, 20, verdict=GateVerdict.UNCERTAIN, human_accepts=False)
    for index in range(21, 40):
        _labeled_evaluation(db, attempt, index, verdict=GateVerdict.MACHINE_REJECTED, human_accepts=False)
    # Even a correctly shaped row authored by a service identity is not ground truth.
    _labeled_evaluation(
        db,
        attempt,
        99,
        verdict=GateVerdict.CANDIDATE,
        human_accepts=True,
        actor="service:forbidden-labeler",
    )

    metrics = JudgeCalibrationService(db, config).refresh("prototype")

    assert metrics.sample_size == 40
    assert (metrics.true_positive, metrics.true_negative) == (18, 19)
    assert (metrics.false_positive, metrics.false_negative) == (1, 2)
    assert metrics.uncertain_count == 1
    assert metrics.precision == pytest.approx(18 / 19)
    assert metrics.recall == pytest.approx(18 / 20)
    assert metrics.false_approve_rate == pytest.approx(1 / 20)
    assert metrics.false_reject_rate == pytest.approx(2 / 20)
    assert metrics.reversal_rate == pytest.approx(2 / 21)
    assert metrics.uncertainty_rate == pytest.approx(1 / 40)
    persisted = db.judge_calibration("prototype")
    assert persisted is not None
    assert persisted["sample_size"] == 40
    assert persisted["latest_label_at"]


def test_blind_reveal_and_calibration_are_per_artifact_and_use_first_label(tmp_path: Path) -> None:
    config = _config(tmp_path)
    db = Database(config.paths.database)
    db.migrate()
    attempt = _attempt(db)
    first = _labeled_evaluation(db, attempt, 1, verdict=GateVerdict.CANDIDATE, human_accepts=True)
    second_digest = hashlib.sha256(b"artifact-2").hexdigest()
    second = db.add_artifact(
        attempt_id=str(attempt["id"]),
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        content_hash=second_digest,
        object_ref=f"sha256:{second_digest}",
        media_type="image/png",
        size_bytes=1,
    )
    db.add_evaluation(
        artifact_id=second["id"],
        attempt_id=str(attempt["id"]),
        evaluator="visual_judge",
        result=GateResult(
            gate="visual_fidelity",
            gate_version=PROTOTYPE_EVALUATOR_VERSION,
            blocking=False,
            verdict=GateVerdict.MACHINE_REJECTED,
        ),
        hidden_until_label=True,
    )
    visible = db.evaluations_for_attempt(str(attempt["id"]))
    assert [row["artifact_id"] for row in visible] == [first["id"]]
    assert db.has_hidden_unlabeled_evaluations(str(attempt["id"]))

    # A later, now-unblinded opinion cannot become a second label for the
    # artifact or replace its first calibration authority.
    with pytest.raises(VersionConflict, match="already exists"):
        db.add_human_decision(
            artifact_id=str(first["id"]),
            attempt_id=str(attempt["id"]),
            action="prototype_label",
            feedback="later non-blind disagreement",
            tags=["outcome:reject"],
            actor="human:second-reviewer",
            attempt_version=int(attempt["version"]),
            content_hash=str(first["content_hash"]),
        )
    metrics = JudgeCalibrationService(db, config).refresh("prototype")
    assert metrics.sample_size == 1
    assert metrics.true_positive == 1
    assert metrics.false_positive == 0


def test_calibration_ignores_human_rows_without_transactional_blind_authority(tmp_path: Path) -> None:
    config = _config(tmp_path)
    db = Database(config.paths.database)
    db.migrate()
    attempt = _attempt(db)
    digest = hashlib.sha256(b"prejudged-label-order").hexdigest()
    artifact = db.add_artifact(
        attempt_id=str(attempt["id"]),
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        content_hash=digest,
        object_ref=f"sha256:{digest}",
        media_type="image/png",
        size_bytes=1,
    )
    # This immutable legacy/direct row was written before there was any judge
    # evidence. A later hidden evaluation must not retroactively make it blind
    # or eligible optimizer/calibration ground truth.
    db.add_human_decision(
        artifact_id=artifact["id"],
        attempt_id=str(attempt["id"]),
        action="prototype_label",
        feedback="saw no judge result, but arrived too early",
        tags=["outcome:accept"],
        actor="human:legacy",
        attempt_version=int(attempt["version"]),
        content_hash=digest,
    )
    db.add_evaluation(
        artifact_id=artifact["id"],
        attempt_id=str(attempt["id"]),
        evaluator="visual_judge",
        result=GateResult(
            gate="visual_fidelity",
            gate_version=PROTOTYPE_EVALUATOR_VERSION,
            blocking=False,
            verdict=GateVerdict.CANDIDATE,
        ),
        hidden_until_label=True,
    )

    metrics = JudgeCalibrationService(db, config).refresh("prototype")
    assert metrics.sample_size == 0
    assert db.evaluations_for_attempt(str(attempt["id"])) == []


def test_production_routing_requires_current_low_reversal_calibration(tmp_path: Path) -> None:
    config = _config(tmp_path)
    db = Database(config.paths.database)
    db.migrate()
    attempt = _attempt(db)
    # Enough positives and negatives are required because each conditional
    # false-rate Wilson interval fails closed on an empty denominator.
    for index in range(40):
        _labeled_evaluation(db, attempt, index, verdict=GateVerdict.CANDIDATE, human_accepts=True)
    for index in range(40, 80):
        _labeled_evaluation(db, attempt, index, verdict=GateVerdict.MACHINE_REJECTED, human_accepts=False)
    service = JudgeCalibrationService(db, config)
    service.refresh("prototype")

    status = service.routing_status("prototype")
    assert status.enabled
    assert status.reasons == ()

    stale = service.routing_status("prototype", at=datetime.now(UTC) + timedelta(days=31))
    assert not stale.enabled
    assert "calibration_stale" in stale.reasons
    changed = service.routing_status(
        "prototype",
        evaluator_version=judge_evaluator_version(
            config,
            "prototype",
            resolved_model="gemini-3.7-flash-20260801",
        ),
    )
    assert not changed.enabled
    assert "evaluator_version_changed" in changed.reasons


def test_current_dated_gemini_evaluator_is_authoritative_over_configured_alias(tmp_path: Path) -> None:
    config = _config(tmp_path)
    db = Database(config.paths.database)
    db.migrate()
    attempt = _attempt(db)
    dated = judge_evaluator_version(
        config,
        "prototype",
        resolved_model="gemini-3.7-flash-20260801",
    )
    for index in range(40):
        _labeled_evaluation(
            db,
            attempt,
            index,
            verdict=GateVerdict.CANDIDATE,
            human_accepts=True,
            evaluator_version=dated,
        )
    for index in range(40, 80):
        _labeled_evaluation(
            db,
            attempt,
            index,
            verdict=GateVerdict.MACHINE_REJECTED,
            human_accepts=False,
            evaluator_version=dated,
        )
    service = JudgeCalibrationService(db, config)
    report = service.refresh_all()

    assert service.active_evaluator_version("prototype") == dated
    assert report["prototype"]["active_evaluator_version"] == dated
    assert report["prototype"]["evaluator_version"] == dated
    assert service.quality_status("prototype").enabled


def test_production_routing_falls_back_to_shadow_on_sample_or_reversal_bound(tmp_path: Path) -> None:
    config = _config(tmp_path)
    db = Database(config.paths.database)
    db.migrate()
    attempt = _attempt(db)
    for index in range(10):
        _labeled_evaluation(db, attempt, index, verdict=GateVerdict.CANDIDATE, human_accepts=True)
    service = JudgeCalibrationService(db, config)
    service.refresh("prototype")
    status = service.routing_status("prototype")
    assert not status.enabled
    assert "insufficient_labeled_sample" in status.reasons
    assert "false_approve_bound" in status.reasons  # no negative denominator fails closed

    # Add an ample but badly reversed reject set. The next refresh remains
    # blocked specifically by the machine-reject reversal confidence bound.
    for index in range(10, 45):
        _labeled_evaluation(db, attempt, index, verdict=GateVerdict.MACHINE_REJECTED, human_accepts=True)
    for index in range(45, 80):
        _labeled_evaluation(db, attempt, index, verdict=GateVerdict.MACHINE_REJECTED, human_accepts=False)
    service.refresh("prototype")
    status = service.routing_status("prototype")
    assert not status.enabled
    assert "machine_reject_reversal_bound" in status.reasons


def test_shadow_mode_never_enables_machine_routing_and_sampling_is_deterministic(tmp_path: Path) -> None:
    config = _config(tmp_path, mode="shadow")
    config.review.sampled_reject_rate = 1
    db = Database(config.paths.database)
    db.migrate()
    service = JudgeCalibrationService(db, config)
    assert not service.routing_status("build").enabled
    assert service.should_sample_reject("attempt", "build")
    config.review.sampled_reject_rate = 0
    assert not service.should_sample_reject("attempt", "build")


@pytest.mark.asyncio
async def test_factory_production_config_routes_as_shadow_without_calibration(tmp_path: Path) -> None:
    config = _config(tmp_path)
    db = Database(config.paths.database)
    db.migrate()
    attempt = _attempt(db)
    _add_unlabeled_prototype(db, attempt, 1)
    factory = Factory(config, database=db, objects=ObjectStore(config.paths.objects))

    async def reject(*args: object, **kwargs: object):
        del args, kwargs
        return _rejection(), {"resolved_model": "gemini-3.7-flash"}

    factory._judge = reject  # type: ignore[method-assign]
    updated = await factory._prototype_triage(attempt)

    assert updated["disposition"] == Disposition.NEEDS_HUMAN
    evaluation = db.evaluations_for_attempt(str(attempt["id"]), reveal=True)[0]
    assert evaluation["hidden_until_label"] == 1


@pytest.mark.asyncio
async def test_factory_samples_calibrated_machine_reject_without_promoting_it(tmp_path: Path) -> None:
    config = _config(tmp_path)
    config.review.sampled_reject_rate = 1
    db = Database(config.paths.database)
    db.migrate()
    attempt = _attempt(db)
    _add_unlabeled_prototype(db, attempt, 1)
    db.set_judge_calibration(
        kind="prototype",
        evaluator_version=PROTOTYPE_EVALUATOR_VERSION,
        sample_size=100,
        true_positive=50,
        true_negative=50,
        false_positive=0,
        false_negative=0,
        lower_confidence=0.96,
        upper_confidence=1,
        stale=False,
        precision=1,
        recall=1,
        false_approve_upper=0.08,
        false_reject_upper=0.08,
        reversal_upper=0.08,
        uncertainty_upper=0.08,
        latest_label_at=datetime.now(UTC).isoformat(),
    )
    factory = Factory(config, database=db, objects=ObjectStore(config.paths.objects))

    async def reject(*args: object, **kwargs: object):
        del args, kwargs
        return _rejection(), {"resolved_model": "gemini-3.7-flash"}

    factory._judge = reject  # type: ignore[method-assign]
    updated = await factory._prototype_triage(attempt)

    assert updated["disposition"] == Disposition.MACHINE_REJECTED
    assert updated["review_kind"] == "prototype_label"
    evaluation = db.evaluations_for_attempt(str(attempt["id"]), reveal=True)[0]
    assert evaluation["hidden_until_label"] == 1
    assert len(db.pending_outbox()) == 1
    assert db.decisions_for_attempt(str(attempt["id"])) == []


@pytest.mark.asyncio
async def test_run_once_dispatches_outbox_even_while_provider_work_is_halted(tmp_path: Path) -> None:
    config = _config(tmp_path)
    config.optimizer.enabled = False
    db = Database(config.paths.database)
    db.migrate()
    attempt = _attempt(db)
    operation, _ = db.begin_operation(
        attempt_id=str(attempt["id"]),
        stage=Stage.PROTOTYPE,
        idempotency_key="unknown-provider-call",
        side_effect="generate_image",
        provider_role="image_generator",
        request_hash="a" * 64,
        cost_reserved_micros=1,
    )
    db.transition_operation(
        operation["id"],
        OperationStatus.INTENT,
        OperationStatus.RECONCILIATION_REQUIRED,
    )
    objects = ObjectStore(config.paths.objects)
    message = _enqueue(db, objects)
    factory = Factory(config, database=db, objects=objects)

    report = await factory.run_once()
    await factory.close()

    assert report["halt"]["reason"] == "reconciliation_required"
    assert report["outbox"]["delivered"] == 1
    assert db.get_outbox(str(message["id"]))["status"] == "delivered"


def _add_unlabeled_prototype(db: Database, attempt: dict[str, object], index: int) -> dict[str, object]:
    digest = hashlib.sha256(f"unlabeled-{index}".encode()).hexdigest()
    return db.add_artifact(
        attempt_id=str(attempt["id"]),
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        content_hash=digest,
        object_ref=f"sha256:{digest}",
        media_type="image/png",
        size_bytes=1,
    )


def _rejection() -> VisualJudgment:
    return VisualJudgment(
        verdict="machine_rejected",
        reasons=["fixture reject"],
        fidelity=0.1,
        readability=0.1,
        role_clarity=0.1,
        animation_quality=0.1,
        craft=0.1,
    )


class _FailingDestination:
    def __init__(self, *, permanent: bool = False) -> None:
        self.permanent = permanent
        self.calls = 0

    async def deliver(self, message: dict[str, object], payload: bytes) -> DeliveryResult:
        del message, payload
        self.calls += 1
        return DeliveryResult(False, permanent=self.permanent, error="injected failure")


def _enqueue(db: Database, objects: ObjectStore, *, destination: str = "review_gallery") -> dict[str, object]:
    stored = objects.put(b'{"event":"review"}')
    return db.enqueue_outbox(
        idempotency_key="review:one",
        destination=destination,
        event_ref="attempt-one",
        payload_ref=stored.uri,
        payload_hash=stored.sha256,
    )


def test_review_transition_and_outbox_intent_commit_atomically(tmp_path: Path) -> None:
    config = _config(tmp_path)
    db = Database(config.paths.database)
    db.migrate()
    attempt = _attempt(db)
    objects = ObjectStore(config.paths.objects)
    stored = objects.put(b'{"review":"prototype"}')

    updated, message = db.update_attempt_with_outbox(
        str(attempt["id"]),
        int(attempt["version"]),
        outbox_idempotency_key="review:atomic:prototype",
        outbox_destination="review_gallery",
        outbox_event_ref=str(attempt["id"]),
        outbox_payload_ref=stored.uri,
        outbox_payload_hash=stored.sha256,
        stage=Stage.PROTOTYPE_REVIEW,
        disposition=Disposition.NEEDS_HUMAN,
        review_kind="prototype",
    )

    assert updated["disposition"] == Disposition.NEEDS_HUMAN
    assert message["status"] == "pending"
    assert db.pending_outbox()[0]["event_ref"] == attempt["id"]


def test_outbox_conflict_rolls_back_atomic_review_transition(tmp_path: Path) -> None:
    config = _config(tmp_path)
    db = Database(config.paths.database)
    db.migrate()
    attempt = _attempt(db)
    objects = ObjectStore(config.paths.objects)
    first = objects.put(b'{"review":"first"}')
    second = objects.put(b'{"review":"different"}')
    db.enqueue_outbox(
        idempotency_key="review:conflict",
        destination="review_gallery",
        event_ref=str(attempt["id"]),
        payload_ref=first.uri,
        payload_hash=first.sha256,
    )

    with pytest.raises(VersionConflict, match="outbox key reused"):
        db.update_attempt_with_outbox(
            str(attempt["id"]),
            int(attempt["version"]),
            outbox_idempotency_key="review:conflict",
            outbox_destination="review_gallery",
            outbox_event_ref=str(attempt["id"]),
            outbox_payload_ref=second.uri,
            outbox_payload_hash=second.sha256,
            stage=Stage.PROTOTYPE_REVIEW,
            disposition=Disposition.NEEDS_HUMAN,
            review_kind="prototype",
        )

    unchanged = db.get_attempt(str(attempt["id"]))
    assert unchanged["version"] == attempt["version"]
    assert unchanged["stage"] == attempt["stage"]
    assert unchanged["disposition"] == attempt["disposition"]


@pytest.mark.parametrize("changed", ["destination", "event_ref", "payload_ref", "payload_hash"])
def test_outbox_idempotency_key_rejects_any_changed_identity(tmp_path: Path, changed: str) -> None:
    config = _config(tmp_path)
    db = Database(config.paths.database)
    db.migrate()
    values = {
        "destination": "review_gallery",
        "event_ref": "attempt-one",
        "payload_ref": "sha256:" + "a" * 64,
        "payload_hash": "a" * 64,
    }
    db.enqueue_outbox(idempotency_key="review:identity", **values)
    values[changed] = "different"

    with pytest.raises(VersionConflict, match="outbox key reused"):
        db.enqueue_outbox(idempotency_key="review:identity", **values)


@pytest.mark.asyncio
async def test_local_outbox_delivery_is_acknowledged_with_immutable_history(tmp_path: Path) -> None:
    config = _config(tmp_path)
    db = Database(config.paths.database)
    db.migrate()
    objects = ObjectStore(config.paths.objects)
    message = _enqueue(db, objects)

    report = await OutboxDispatcher(
        db,
        objects,
        config,
        destinations={"review_gallery": LocalGalleryDestination()},
    ).dispatch_due()

    assert report["delivered"] == 1
    current = db.get_outbox(str(message["id"]))
    assert current["status"] == "delivered"
    assert current["delivered_at"]
    assert db.outbox_delivery_attempts(str(message["id"]))[0]["outcome"] == "delivered"
    assert db.pending_outbox() == []


@pytest.mark.asyncio
async def test_outbox_retries_with_backoff_then_dead_letters_at_bound(tmp_path: Path) -> None:
    config = _config(tmp_path)
    config.outbox.max_attempts = 2
    config.outbox.initial_backoff_seconds = 10
    config.outbox.max_backoff_seconds = 20
    db = Database(config.paths.database)
    db.migrate()
    objects = ObjectStore(config.paths.objects)
    message = _enqueue(db, objects, destination="injected")
    destination = _FailingDestination()
    start = datetime.now(UTC)

    first = await OutboxDispatcher(db, objects, config, destinations={"injected": destination}).dispatch_due(at=start)
    assert first["retried"] == 1
    current = db.get_outbox(str(message["id"]))
    assert current["status"] == "retry"
    assert current["next_attempt_at"] == (start + timedelta(seconds=10)).isoformat(timespec="microseconds")
    assert db.pending_outbox(at=(start + timedelta(seconds=9)).isoformat()) == []

    second = await OutboxDispatcher(db, objects, config, destinations={"injected": destination}).dispatch_due(
        at=start + timedelta(seconds=10)
    )
    assert second["dead_lettered"] == 1
    current = db.get_outbox(str(message["id"]))
    assert current["status"] == "dead_letter"
    assert current["attempts"] == 2
    assert current["dead_lettered_at"]
    history = db.outbox_delivery_attempts(str(message["id"]))
    assert [row["outcome"] for row in history] == ["transient_failure", "attempts_exhausted"]


@pytest.mark.asyncio
async def test_outbox_permanent_destination_or_payload_failure_dead_letters_without_retry(tmp_path: Path) -> None:
    config = _config(tmp_path)
    db = Database(config.paths.database)
    db.migrate()
    objects = ObjectStore(config.paths.objects)
    unknown = _enqueue(db, objects, destination="not-configured")

    report = await OutboxDispatcher(db, objects, config, destinations={}).dispatch_due()

    assert report["dead_lettered"] == 1
    current = db.get_outbox(str(unknown["id"]))
    assert current["status"] == "dead_letter"
    assert "unknown outbox destination" in current["last_error"]


@pytest.mark.asyncio
async def test_webhook_destination_sends_idempotency_identity_and_accepts_duplicate_ack() -> None:
    captured: list[httpx.Request] = []

    def duplicate(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(409, text="already consumed")

    async with httpx.AsyncClient(transport=httpx.MockTransport(duplicate)) as client:
        destination = WebhookDestination(
            "https://notifications.test/review",
            token="secret",
            timeout_seconds=5,
            client=client,
        )
        result = await destination.deliver(
            {"idempotency_key": "review:one", "event_ref": "attempt:one"},
            b'{"attempt":"one"}',
        )

    assert result.delivered
    assert result.response_code == 409
    assert captured[0].headers["idempotency-key"] == "review:one"
    assert captured[0].headers["x-snaketron-event-ref"] == "attempt:one"
    assert captured[0].headers["authorization"] == "Bearer secret"
