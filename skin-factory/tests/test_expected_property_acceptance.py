from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest

from snaketron_factory.calibration import judge_evaluator_version
from snaketron_factory.domain import Disposition, GateVerdict, ProviderResult
from snaketron_factory.optimizer import (
    EXPECTED_PROPERTY_RUBRIC_SHA,
    ExpectedPropertyContract,
    ExpectedPropertyJudgment,
    ExpectedPropertyResult,
    Optimizer,
    expected_property_evaluator_version,
    freeze_expected_property_contract,
)
from snaketron_factory.techniques import TechniqueMiner


def _contract(concept_id: str, *, technique: bool = False) -> dict[str, Any]:
    return freeze_expected_property_contract(
        {
            "id": f"decision-{concept_id}",
            "concept_id": concept_id,
            "prototype_hash": "sha256:" + concept_id.encode().hex().ljust(64, "0")[:64],
            "prototype_decision_id": f"prototype-decision-{concept_id}",
            "feedback": f"Keep the {concept_id} rhythm legible at game scale.",
            "root_cause": f"{concept_id} rhythm clarity",
        },
        additional_properties=(["traveling glint remains continuous across segment boundaries"] if technique else None),
    )


def _attempt(
    attempt_id: str,
    concept_id: str,
    candidate: str,
    split: str,
    contract: dict[str, Any],
) -> dict[str, Any]:
    return {
        "id": attempt_id,
        "concept_id": concept_id,
        "experiment_candidate": candidate,
        "experiment_split": split,
        "approved_prototype_hash": contract["prototype_hash"],
        "behavior_json": json.dumps({"expected_property_contract": contract}),
        "disposition": Disposition.EXPERIMENT_COMPLETE,
    }


def _visual(version: str, score: float) -> dict[str, Any]:
    model = version.rsplit("+rubric:", 1)[0]
    return {
        "evaluator": "visual_judge",
        "evaluator_version": version,
        "gate_name": "visual_fidelity",
        "blocking": 0,
        "verdict": "candidate",
        "measurements_json": json.dumps(
            {
                **{name: score for name in ("fidelity", "readability", "role_clarity", "animation_quality", "craft")},
                "resolved_model": model,
            }
        ),
    }


def _property_evaluation(
    contract: dict[str, Any],
    *,
    model: str,
    scores: list[float],
) -> dict[str, Any]:
    properties = contract["properties"]
    assert len(properties) == len(scores)
    results = [
        {
            "property_id": item["property_id"],
            "satisfied": score >= 0.75,
            "score": score,
            "evidence": [f"retained evidence for {item['property_id']}"],
        }
        for item, score in zip(properties, scores, strict=True)
    ]
    passed = all(item["satisfied"] for item in results)
    return {
        "evaluator": "expected_property_judge",
        "evaluator_version": expected_property_evaluator_version(model, contract["contract_sha256"]),
        "gate_name": "fixture_expected_properties",
        "blocking": 1,
        "verdict": GateVerdict.PASS if passed else GateVerdict.FAIL,
        "reasons_json": json.dumps(["fixture-specific property proof"]),
        "measurements_json": json.dumps(
            {
                "contract_version": contract["contract_version"],
                "contract_sha256": contract["contract_sha256"],
                "evaluator_rubric_sha256": EXPECTED_PROPERTY_RUBRIC_SHA,
                "resolved_model": model,
                "judgment_verdict": "pass" if passed else "fail",
                "minimum_score": 0.75,
                "property_results": results,
                "mean_score": sum(scores) / len(scores),
            }
        ),
    }


def test_fixture_contract_is_content_hashed_versioned_and_fixture_specific() -> None:
    first = _contract("river")
    same = _contract("river")
    other_fixture = _contract("ember")
    technique = _contract("river", technique=True)

    assert ExpectedPropertyContract.model_validate(first).contract_version == "fixture-expected-properties-v1"
    assert first == same
    assert first["contract_sha256"] != other_fixture["contract_sha256"]
    assert first["contract_sha256"] != technique["contract_sha256"]
    assert len(technique["properties"]) == len(first["properties"]) + 1


def test_promotion_authority_rejects_unpaired_fixtures_and_contract_drift(factory_config) -> None:
    river = _contract("river")
    ember = _contract("ember")

    def evidence(contract: dict[str, Any]) -> dict[str, Any]:
        return {
            "contract_sha256": contract["contract_sha256"],
            "passed": True,
            "by_property": {item["property_id"]: {"score": 0.95} for item in contract["properties"]},
        }

    baseline_river = evidence(river)
    candidate_river = evidence(river)
    candidate_river["contract_sha256"] = "sha256:" + "f" * 64
    scores = {
        "baseline": {
            "visual_by_concept": {"river": 0.8, "ember": 0.8},
            "expected_properties_by_concept": {
                "river": baseline_river,
                "ember": evidence(ember),
            },
        },
        "candidate": {
            "visual_by_concept": {"river": 0.9},
            "expected_properties_by_concept": {"river": candidate_river},
        },
    }
    factory = SimpleNamespace(config=factory_config, database=SimpleNamespace())

    reasons = Optimizer(factory)._candidate_metric_authority_reasons(scores, "candidate")

    assert "candidate fixture set differs from the paired baseline" in reasons
    assert "fixture river contract differs from the paired baseline" in reasons


@pytest.mark.asyncio
async def test_expected_property_evaluator_records_exact_blocking_results(factory_config) -> None:
    contract_payload = _contract("river")
    contract = ExpectedPropertyContract.model_validate(contract_payload)
    attempt = _attempt("candidate-river", "river", "candidate", "holdout", contract_payload)
    prototype = {
        "id": "prototype",
        "content_hash": contract.prototype_hash,
        "object_ref": "sha256:" + "1" * 64,
        "media_type": "image/png",
    }
    render = {
        "id": "render",
        "content_hash": "sha256:" + "2" * 64,
        "object_ref": "sha256:" + "3" * 64,
        "media_type": "image/png",
    }
    judgment = ExpectedPropertyJudgment(
        verdict="pass",
        reasons=["all frozen properties are visible"],
        results=[
            ExpectedPropertyResult(
                property_id=item.property_id,
                satisfied=True,
                score=0.9,
                evidence=["visible retained evidence"],
            )
            for item in contract.properties
        ],
    )
    captured: dict[str, Any] = {}

    class DatabaseStub:
        def get_attempt(self, _attempt_id: str):
            return attempt

        def add_evaluation(self, **fields: Any):
            captured["evaluation"] = fields
            return fields

    async def provider_call(**fields: Any):
        captured["provider_call"] = fields
        return (
            {"id": "operation", "resolved_model": "gemini-3.7-flash-20260815"},
            ProviderResult(value=judgment, resolved_model="gemini-3.7-flash-20260815"),
        )

    def lineage(_attempt: dict[str, Any], kind: str, **_kwargs: Any):
        return prototype if str(kind) == "prototype" else render

    factory = SimpleNamespace(
        database=DatabaseStub(),
        config=factory_config,
        providers=SimpleNamespace(role=lambda _role: SimpleNamespace()),
        objects=SimpleNamespace(get=lambda _ref: b"pixels"),
        _find_lineage_artifact=lineage,
        _provider_call=provider_call,
        _model_result=lambda _operation, result, _model: result.value,
    )
    optimizer = Optimizer(factory)

    await optimizer._evaluate_expected_properties(attempt, contract)

    assert captured["provider_call"]["role"] == "visual_judge"
    evaluation = captured["evaluation"]
    assert evaluation["evaluator"] == "expected_property_judge"
    assert evaluation["result"].blocking is True
    assert evaluation["result"].verdict == GateVerdict.PASS
    assert evaluation["result"].gate_version == expected_property_evaluator_version(
        "gemini-3.7-flash-20260815",
        contract.contract_sha256,
    )
    assert [item["property_id"] for item in evaluation["result"].measurements["property_results"]] == [
        item.property_id for item in contract.properties
    ]


@pytest.mark.asyncio
async def test_holdout_cannot_promote_when_expected_property_fails_despite_high_visual_score(
    factory_config,
) -> None:
    model = "gemini-3.7-flash-20260815"
    visual_version = judge_evaluator_version(factory_config, "build", resolved_model=model)
    attempts: list[dict[str, Any]] = []
    evaluations: dict[str, list[dict[str, Any]]] = {}
    for concept in ("river", "ember", "aurora"):
        contract = _contract(concept)
        for candidate, visual_score, property_score in (
            ("baseline", 0.40, 0.90),
            ("candidate", 0.98, 0.70 if concept == "ember" else 0.95),
        ):
            attempt_id = f"{candidate}-{concept}"
            attempts.append(_attempt(attempt_id, concept, candidate, "holdout", contract))
            evaluations[attempt_id] = [
                _visual(visual_version, visual_score),
                _property_evaluation(
                    contract,
                    model=model,
                    scores=[property_score] * len(contract["properties"]),
                ),
            ]
    run = {
        "id": "holdout-run",
        "version": 3,
        "state": "evaluating_holdout",
        "dev_metrics_json": json.dumps({"winner": "candidate"}),
        "train_metrics_json": json.dumps({"evaluator_version": visual_version}),
        "candidate_refs_json": "[]",
    }
    updated: dict[str, Any] = {}

    class DatabaseStub:
        def experiment_attempts(self, _run_id: str):
            return attempts

        def evaluations_for_attempt(self, attempt_id: str, *, reveal: bool):
            assert reveal is True
            return evaluations[attempt_id]

        def get_optimization_run(self, _run_id: str):
            return run

        def update_optimization_run(self, _run_id: str, _version: int, **fields: Any):
            updated.update(fields)
            return {**run, **fields}

    calibration = SimpleNamespace(
        enabled=True,
        reasons=[],
        as_report=lambda: {"enabled": True, "evaluator_version": visual_version},
    )
    config = factory_config.model_copy(
        update={
            "optimizer": factory_config.optimizer.model_copy(
                update={"promotion_min_pairs": 3, "promotion_min_effect": 0.01}
            )
        }
    )
    factory = SimpleNamespace(
        database=DatabaseStub(),
        config=config,
        calibration=SimpleNamespace(quality_status=lambda _kind, *, evaluator_version: calibration),
    )

    result = await Optimizer(factory)._finish_holdout(run)

    assert result["state"] == "evaluated_not_promoted"
    assert any("did not pass every expected property" in reason for reason in result["reasons"])
    assert any("expected-property non-inferiority" in reason for reason in result["reasons"])
    assert "candidate has a blocking-gate regression" in result["reasons"]
    metrics = updated["holdout_metrics_json"]
    ember = metrics["scores"]["candidate"]["expected_properties_by_concept"]["ember"]
    assert ember["passed"] is False
    assert ember["contract_sha256"] == _contract("ember")["contract_sha256"]
    assert ember["evaluator_version"] == expected_property_evaluator_version(
        model,
        ember["contract_sha256"],
    )


@pytest.mark.asyncio
async def test_technique_candidate_requires_fixture_property_proof(factory_config) -> None:
    model = "gemini-3.7-flash-20260815"
    visual_version = judge_evaluator_version(factory_config, "build", resolved_model=model)
    attempts: list[dict[str, Any]] = []
    evaluations: dict[str, list[dict[str, Any]]] = {}
    for concept in ("river", "ember"):
        contract = _contract(concept, technique=True)
        for candidate, property_score in (
            ("baseline", 0.85),
            ("technique", 0.70 if concept == "ember" else 0.95),
        ):
            attempt_id = f"{candidate}-{concept}"
            attempts.append(_attempt(attempt_id, concept, candidate, "technique", contract))
            evaluations[attempt_id] = [
                _visual(visual_version, 0.95),
                _property_evaluation(
                    contract,
                    model=model,
                    scores=[property_score] * len(contract["properties"]),
                ),
            ]
    updated: dict[str, Any] = {}

    class DatabaseStub:
        def experiment_attempts(self, _run_id: str):
            return attempts

        def evaluations_for_attempt(self, attempt_id: str, *, reveal: bool):
            assert reveal is True
            return evaluations[attempt_id]

        def update_technique_candidate(self, _candidate_id: str, _version: int, **fields: Any):
            updated.update(fields)
            return {"id": "technique", **fields}

    calibration = SimpleNamespace(enabled=True, as_report=lambda: {"enabled": True})
    config = factory_config.model_copy(
        update={"optimizer": factory_config.optimizer.model_copy(update={"technique_min_fixtures": 2})}
    )
    factory = SimpleNamespace(
        database=DatabaseStub(),
        config=config,
        calibration=SimpleNamespace(quality_status=lambda _kind, *, evaluator_version: calibration),
    )

    result = await TechniqueMiner(factory)._advance({"id": "technique", "version": 1, "disposition": "trials_running"})

    assert result["state"] == "rejected"
    trial_results = updated["trial_results"]
    assert trial_results["passed"] is False
    assert any(
        "did not pass every expected property" in reason
        for reason in trial_results["expected_property_authority_reasons"]
    )
    assert trial_results["paired_scores"]["technique"]["expected_properties_by_concept"]["ember"]["passed"] is False
