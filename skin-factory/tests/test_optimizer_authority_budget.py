from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from snaketron_factory.calibration import judge_evaluator_version
from snaketron_factory.config import BudgetConfig, load_config
from snaketron_factory.db import Database
from snaketron_factory.domain import (
    ArtifactKind,
    Disposition,
    GateResult,
    GateVerdict,
    OperationStatus,
    ProviderError,
    ProviderFailureKind,
    ProviderResult,
    Purpose,
    Stage,
)
from snaketron_factory.factory import Factory
from snaketron_factory.optimizer import (
    EvaluatorVersionDrift,
    FeedbackClassification,
    Optimizer,
)
from snaketron_factory.techniques import TechniqueMiner


def _complete_operation(
    database: Database,
    attempt_id: str,
    *,
    key: str,
    reserve: int,
    charge: int,
) -> None:
    operation, _ = database.begin_operation(
        attempt_id=attempt_id,
        stage=Stage.PROTOTYPE,
        idempotency_key=key,
        side_effect=key,
        provider_role="test",
        request_hash=hashlib.sha256(key.encode()).hexdigest(),
        cost_reserved_micros=reserve,
    )
    operation = database.transition_operation(operation["id"], OperationStatus.INTENT, OperationStatus.RUNNING)
    database.transition_operation(
        operation["id"],
        OperationStatus.RUNNING,
        OperationStatus.SUCCEEDED,
        cost_charged_micros=charge,
    )


def _metric(version: str, score: float = 0.9) -> dict[str, Any]:
    resolved_model = version.rsplit("+rubric:", 1)[0]
    return {
        "evaluator": "visual_judge",
        "evaluator_version": version,
        "gate_name": "visual_fidelity",
        "blocking": 0,
        "verdict": "candidate",
        "measurements_json": json.dumps(
            {
                **{name: score for name in ("fidelity", "readability", "role_clarity", "animation_quality", "craft")},
                "resolved_model": resolved_model,
            }
        ),
    }


def test_optimizer_examples_require_authoring_route_at_configured_confidence(
    factory_config, database, objects, make_attempt
) -> None:
    def labeled(target: str | None, confidence: float) -> str:
        attempt = make_attempt(stage=Stage.FINAL_REVIEW, disposition=Disposition.NEEDS_HUMAN)
        stored = objects.put(f"prototype:{attempt['id']}".encode())
        database.add_artifact(
            attempt_id=attempt["id"],
            stage=Stage.PROTOTYPE,
            kind=ArtifactKind.PROTOTYPE,
            content_hash=stored.uri,
            object_ref=stored.uri,
            media_type="image/png",
            size_bytes=stored.size,
        )
        attempt = database.update_attempt(
            attempt["id"],
            attempt["version"],
            approved_prototype_hash=stored.uri,
            prototype_decision_id=f"approval:{attempt['id']}",
            review_kind="final",
        )
        contact_bytes = objects.put(f"contact:{attempt['id']}".encode())
        contact = database.add_artifact(
            attempt_id=attempt["id"],
            stage=Stage.RENDER,
            kind=ArtifactKind.CONTACT_SHEET,
            content_hash=contact_bytes.uri,
            object_ref=contact_bytes.uri,
            media_type="image/png",
            size_bytes=contact_bytes.size,
        )
        database.add_evaluation(
            artifact_id=contact["id"],
            attempt_id=attempt["id"],
            evaluator="visual_judge",
            result=GateResult(
                gate="visual_fidelity",
                gate_version="judge-v1",
                blocking=False,
                verdict=GateVerdict.CANDIDATE,
            ),
            hidden_until_label=True,
        )
        decision = database.add_blind_human_label(
            artifact_id=contact["id"],
            attempt_id=attempt["id"],
            action="build_quality_label",
            feedback="literal retained feedback",
            tags=["outcome:accept"],
            actor="human:reviewer",
            content_hash=contact["content_hash"],
        )
        if target is not None:
            database.add_feedback_route(
                decision_id=decision["id"],
                target=target,
                signature=f"{target} issue",
                confidence=confidence,
                classifier_version="resolved-router-v1",
                evidence={"decision": decision["id"]},
            )
        return attempt["id"]

    accepted = labeled("authoring_playbook", 0.8)
    labeled("authoring_playbook", 0.799)
    labeled("direction", 0.99)
    labeled("deterministic_gate", 0.99)
    labeled("platform", 0.99)
    labeled(None, 0)

    # A plausible human row plus a high-confidence route is still excluded
    # when it was not atomically bound to hidden pre-existing judge evidence.
    accepted_attempt = database.get_attempt(accepted)
    prototype = database.artifacts_for_attempt(accepted, kind=ArtifactKind.PROTOTYPE)[0]
    untrusted = database.add_human_decision(
        artifact_id=prototype["id"],
        attempt_id=accepted,
        action="build_quality_label",
        feedback="unblinded direct row",
        tags=["outcome:accept"],
        actor="human:legacy",
        attempt_version=accepted_attempt["version"],
        content_hash=prototype["content_hash"],
    )
    database.add_feedback_route(
        decision_id=untrusted["id"],
        target="authoring_playbook",
        signature="must not train",
        confidence=1.0,
        classifier_version="resolved-router-v1",
        evidence={"authority": False},
    )

    factory = SimpleNamespace(database=database, config=factory_config)
    optimizer = Optimizer(factory)

    assert [row["attempt_id"] for row in optimizer._eligible_examples()] == [accepted]
    assert factory_config.optimizer.feedback_min_confidence == 0.8


def test_technique_sources_also_require_high_confidence_authoring_authority(database, objects, make_attempt) -> None:
    def source(target: str, confidence: float) -> str:
        attempt = make_attempt(stage=Stage.FINAL_REVIEW, disposition=Disposition.NEEDS_HUMAN)
        attempt = database.update_attempt(
            attempt["id"],
            attempt["version"],
            review_kind="final",
            production_skin_id=f"skin-{attempt['id']}",
            production_revision="1",
            production_content_hash="sha256:" + "c" * 64,
        )
        stored = objects.put(f"trace:{attempt['id']}".encode())
        artifact = database.add_artifact(
            attempt_id=attempt["id"],
            stage=Stage.AUTHOR,
            kind=ArtifactKind.WORKER_TRACE,
            content_hash=stored.uri,
            object_ref=stored.uri,
            media_type="application/json",
            size_bytes=stored.size,
            metadata={"novelty_candidate": True},
        )
        contact_bytes = objects.put(f"contact:{attempt['id']}".encode())
        contact = database.add_artifact(
            attempt_id=attempt["id"],
            stage=Stage.RENDER,
            kind=ArtifactKind.CONTACT_SHEET,
            content_hash=contact_bytes.uri,
            object_ref=contact_bytes.uri,
            media_type="image/png",
            size_bytes=contact_bytes.size,
        )
        database.add_evaluation(
            artifact_id=contact["id"],
            attempt_id=attempt["id"],
            evaluator="visual_judge",
            result=GateResult(
                gate="visual_fidelity",
                gate_version="judge-v1",
                blocking=False,
                verdict=GateVerdict.CANDIDATE,
            ),
            hidden_until_label=True,
        )
        decision = database.add_blind_human_label(
            artifact_id=contact["id"],
            attempt_id=attempt["id"],
            action="build_quality_label",
            feedback="A novel animation technique succeeded.",
            tags=["outcome:accept"],
            actor="human:reviewer",
            content_hash=contact["content_hash"],
        )
        database.add_feedback_route(
            decision_id=decision["id"],
            target=target,
            signature="novel animation effect",
            confidence=confidence,
            classifier_version="resolved-router-v1",
            evidence={"decision": decision["id"]},
        )
        return artifact["id"]

    accepted = source("authoring_playbook", 0.8)
    rejected = {
        source("authoring_playbook", 0.79),
        source("direction", 0.99),
        source("platform", 0.99),
    }

    found = database.successful_novel_traces(min_confidence=0.8)
    assert [row["id"] for row in found] == [accepted]
    assert rejected.isdisjoint(row["id"] for row in found)


def test_control_attempt_is_terminal_current_and_upgrade_idempotent(
    factory_config, database, objects, monkeypatch
) -> None:
    factory = Factory(factory_config, database=database, objects=objects)
    snapshot = {
        "direction_sha": "1" * 64,
        "skill_sha": "2" * 64,
        "capability_sha": "3" * 64,
        "gate_sha": "4" * 64,
        "model_config_sha": "5" * 64,
        "runtime_sha": "runtime-a",
        "models": {"smart_text": {"model": "resolved-a"}},
    }
    monkeypatch.setattr(factory, "behavior_snapshot", lambda: dict(snapshot))

    first = factory.control_attempt("optimizer")
    again = factory.control_attempt("optimizer")

    assert first["id"] == again["id"]
    assert first["purpose"] == Purpose.CONTROL
    assert first["stage"] == Stage.COMPLETE
    assert first["disposition"] == Disposition.EXPERIMENT_COMPLETE
    assert database.next_active_attempt() is None
    assert database.get_concept(first["concept_id"])["current_attempt_id"] is None

    snapshot["runtime_sha"] = "runtime-b"
    upgraded = factory.control_attempt("optimizer")
    assert upgraded["id"] != first["id"]
    assert factory.control_attempt("optimizer")["id"] == upgraded["id"]


@pytest.mark.asyncio
async def test_historical_feedback_routes_on_current_control_attempt(factory_config) -> None:
    decision = {
        "id": "decision-old",
        "attempt_id": "production-stale",
        "action": "build_quality_label",
        "feedback": "The stripe cadence needs more breathing room.",
        "tags_json": "[]",
    }
    captured: dict[str, Any] = {}
    control = {
        "id": "control-current",
        "stage": Stage.COMPLETE,
        "purpose": Purpose.CONTROL,
        "disposition": Disposition.EXPERIMENT_COMPLETE,
    }

    class DatabaseStub:
        def unlabeled_feedback_routes(self, *, limit: int):
            assert limit == 1
            return [decision]

        def add_feedback_route(self, **fields: Any):
            captured["route"] = fields
            return {"id": "route", **fields}

    async def provider_call(**fields: Any):
        captured["call"] = fields
        return (
            {"id": "operation", "resolved_model": "router-resolved-v1"},
            ProviderResult(
                value=FeedbackClassification(
                    target="authoring_playbook",
                    signature="stripe spacing",
                    confidence=0.93,
                    evidence=["literal spacing feedback"],
                ),
                resolved_model="router-resolved-v1",
            ),
        )

    factory = SimpleNamespace(
        database=DatabaseStub(),
        config=factory_config,
        control_attempt=lambda namespace: control if namespace == "optimizer" else None,
        providers=SimpleNamespace(role=lambda _role: SimpleNamespace()),
        _provider_call=provider_call,
        _model_result=lambda _operation, result, _model: result.value,
    )

    routed = await Optimizer(factory)._route_one_feedback()

    assert routed is not None
    assert captured["call"]["attempt"] is control
    assert "production-stale" not in captured["call"]["key"]
    assert "control-current" in captured["call"]["key"]
    assert captured["route"]["classifier_version"] == "router-resolved-v1"


@pytest.mark.asyncio
async def test_known_feedback_classifier_failure_is_durably_skipped(factory_config) -> None:
    decision = {
        "id": "decision-failing-router",
        "action": "build_quality_label",
        "feedback": "Retain this literal feedback even when routing fails.",
        "tags_json": "[]",
    }
    captured: dict[str, Any] = {}

    class DatabaseStub:
        def unlabeled_feedback_routes(self, *, limit: int):
            assert limit == 1
            return [decision]

        def unresolved_operations(self):
            return []

        def add_feedback_route(self, **fields: Any):
            captured.update(fields)
            return {"id": "route-failed", **fields}

    async def provider_call(**_fields: Any):
        raise ProviderError(
            ProviderFailureKind.REFUSAL,
            "classifier refused known input",
            outcome_known=True,
        )

    factory = SimpleNamespace(
        database=DatabaseStub(),
        config=factory_config,
        control_attempt=lambda _namespace: {
            "id": "control-current",
            "stage": Stage.COMPLETE,
        },
        providers=SimpleNamespace(role=lambda _role: SimpleNamespace()),
        _provider_call=provider_call,
    )

    routed = await Optimizer(factory)._route_one_feedback()

    assert routed["id"] == "route-failed"
    assert captured["decision_id"] == decision["id"]
    assert captured["target"] == "platform"
    assert captured["confidence"] == 0
    assert captured["evidence"]["routed_for_optimization"] is False


@pytest.mark.asyncio
async def test_known_optimizer_and_technique_failures_terminalize_their_owning_records(
    factory_config,
    monkeypatch,
) -> None:
    run = {"id": "optimization-failure", "state": "queued", "version": 3}
    candidate = {"id": "technique-failure", "disposition": "trials_running", "version": 4}

    class DatabaseStub:
        def unlabeled_feedback_routes(self, *, limit: int):
            assert limit == 1
            return []

        def ready_optimization_run(self):
            return run if run["state"] == "queued" else None

        def get_optimization_run(self, _run_id: str):
            return dict(run)

        def update_optimization_run(self, _run_id: str, expected_version: int, **fields: Any):
            assert expected_version == run["version"]
            run.update(fields)
            run["version"] += 1
            return dict(run)

        def ready_technique_candidate(self):
            return candidate if candidate["disposition"] == "trials_running" else None

        def update_technique_candidate(
            self,
            _candidate_id: str,
            expected_version: int,
            *,
            disposition: str,
            trial_results: Any,
        ):
            assert expected_version == candidate["version"]
            candidate.update(disposition=disposition, trial_results_json=json.dumps(trial_results))
            candidate["version"] += 1
            return dict(candidate)

        def unresolved_operations(self):
            return []

    factory = SimpleNamespace(database=DatabaseStub(), config=factory_config)
    optimizer = Optimizer(factory)

    async def fail_optimizer(_run: dict[str, Any]):
        raise ProviderError(ProviderFailureKind.INVALID_OUTPUT, "bad reflection", outcome_known=True)

    monkeypatch.setattr(optimizer, "_advance", fail_optimizer)
    optimizer_result = await optimizer.advance_if_ready()
    assert optimizer_result["state"] == "evaluation_failed"
    assert run["dev_metrics_json"]["error"] == "known_external_failure"

    miner = TechniqueMiner(factory)

    async def fail_technique(_candidate: dict[str, Any]):
        raise ProviderError(ProviderFailureKind.REFUSAL, "technique refused", outcome_known=True)

    monkeypatch.setattr(miner, "_advance", fail_technique)
    technique_result = await miner.advance_if_ready()
    assert technique_result["state"] == "rejected"
    assert json.loads(candidate["trial_results_json"])["reason"] == "known_external_failure"


def test_metric_scoring_rejects_mixed_or_drifted_actual_evaluator_versions(factory_config) -> None:
    old = judge_evaluator_version(factory_config, "build", resolved_model="judge-2026-08-01")
    new = judge_evaluator_version(factory_config, "build", resolved_model="judge-2026-08-15")
    attempts = [
        {
            "id": "baseline",
            "experiment_split": "development",
            "disposition": Disposition.EXPERIMENT_COMPLETE,
        },
        {
            "id": "candidate",
            "experiment_split": "development",
            "disposition": Disposition.EXPERIMENT_COMPLETE,
        },
    ]
    evaluations = {"baseline": [_metric(old)], "candidate": [_metric(new)]}
    database = SimpleNamespace(evaluations_for_attempt=lambda attempt_id, **_kwargs: evaluations[attempt_id])
    optimizer = Optimizer(SimpleNamespace(database=database, config=factory_config))

    with pytest.raises(EvaluatorVersionDrift, match="mixed"):
        optimizer._scoring_evaluator_version(attempts, split="development")

    evaluations["candidate"] = [_metric(old)]
    assert optimizer._scoring_evaluator_version(attempts, split="development") == old
    with pytest.raises(EvaluatorVersionDrift, match="drifted"):
        optimizer._scoring_evaluator_version(attempts, split="development", expected=new)


@pytest.mark.asyncio
async def test_technique_scoring_rejects_mixed_evaluator_versions_before_calibration(factory_config) -> None:
    old = judge_evaluator_version(factory_config, "build", resolved_model="judge-old")
    new = judge_evaluator_version(factory_config, "build", resolved_model="judge-new")
    attempts = [
        {
            "id": "baseline",
            "concept_id": "concept",
            "experiment_candidate": "baseline",
            "experiment_split": "technique",
            "disposition": Disposition.EXPERIMENT_COMPLETE,
        },
        {
            "id": "candidate",
            "concept_id": "concept",
            "experiment_candidate": "technique",
            "experiment_split": "technique",
            "disposition": Disposition.EXPERIMENT_COMPLETE,
        },
    ]
    updated: dict[str, Any] = {}

    class DatabaseStub:
        def experiment_attempts(self, _run_id: str):
            return attempts

        def evaluations_for_attempt(self, attempt_id: str, *, reveal: bool):
            assert reveal is True
            return [_metric(old if attempt_id == "baseline" else new)]

        def update_technique_candidate(self, _candidate_id: str, _version: int, **fields: Any):
            updated.update(fields)
            return {"id": "technique", **fields}

    factory = SimpleNamespace(database=DatabaseStub(), config=factory_config)
    miner = TechniqueMiner(factory)

    result = await miner._advance({"id": "technique", "version": 1, "disposition": "trials_running"})

    assert result["state"] == "rejected"
    assert updated["trial_results"]["reason"] == "evaluator_version_drift"


def test_budget_exposure_releases_completed_reservations_and_admits_default_asset_path(
    factory_config, database, objects, make_attempt
) -> None:
    production = load_config(Path(__file__).parents[1] / "config" / "factory.yaml")
    production = production.model_copy(
        update={
            "paths": factory_config.paths,
            "budgets": BudgetConfig(),
        }
    )
    factory = Factory(production, database=database, objects=objects)
    attempt = make_attempt(stage=Stage.PROTOTYPE)
    smart_reserve = factory._reservation("smart_text", "generate_concept")
    image_reserve = factory._reservation("image_generator", "generate_prototype_image")
    judge_reserve = factory._reservation("visual_judge", "judge_prototype")

    _complete_operation(database, attempt["id"], key="concept", reserve=smart_reserve, charge=90)
    for index in range(3):
        _complete_operation(
            database,
            attempt["id"],
            key=f"prototype-{index}",
            reserve=image_reserve,
            charge=764_134,
        )
        _complete_operation(
            database,
            attempt["id"],
            key=f"prototype-judge-{index}",
            reserve=judge_reserve,
            charge=90,
        )

    audit = database.get_attempt(attempt["id"])
    assert int(audit["cost_reserved_micros"]) + image_reserve > production.budgets.max_cost_micros_per_attempt
    assert database.cost_exposure(attempt_id=attempt["id"]) == 3 * 764_134 + 4 * 90

    # The first raster asset is admitted from actual completed charges plus
    # its outstanding reservation, even though cumulative audit reservations
    # intentionally remain above the cap.
    factory._check_budget(audit, image_reserve)
    operation, _ = database.begin_operation(
        attempt_id=attempt["id"],
        stage=Stage.ASSETS,
        idempotency_key="asset-0",
        side_effect="generate_asset_image",
        provider_role="image_generator",
        request_hash=hashlib.sha256(b"asset-0").hexdigest(),
        cost_reserved_micros=image_reserve,
    )
    assert operation["status"] == OperationStatus.INTENT
    assert database.cost_exposure(attempt_id=attempt["id"]) <= production.budgets.max_cost_micros_per_attempt
    assert database.total_cost() == 3 * 764_134 + 4 * 90
