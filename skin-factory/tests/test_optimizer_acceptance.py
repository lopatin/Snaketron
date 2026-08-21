from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any

import pytest

from snaketron_factory.calibration import judge_evaluator_version
from snaketron_factory.db import LeaseBusy, VersionConflict
from snaketron_factory.domain import ArtifactKind, Disposition, Purpose, Stage
from snaketron_factory.factory import Factory
from snaketron_factory.gallery import VIEW_MAP
from snaketron_factory.optimizer import Optimizer, _pareto_winner
from snaketron_factory.techniques import TechniqueMiner, TechniqueProposal
from snaketron_factory.worker import SkillBundle


def _visual_metric(version: str, score: float = 0.9) -> dict[str, Any]:
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


def test_holdout_ledger_is_idempotent_but_never_reuses_a_concept_in_an_epoch(database, make_attempt) -> None:
    first = make_attempt()
    second = make_attempt()
    run = database.create_optimization_run(
        target="authoring_playbook",
        dataset_version="sha256:" + "1" * 64,
        teacher_config={"model": "gemini-3.7-flash"},
        student_config={"model": "student"},
    )
    rows = [
        {
            "concept_id": first["concept_id"],
            "prototype_hash": "sha256:" + "a" * 64,
        }
    ]

    initial = database.reserve_holdout(
        target="authoring_playbook",
        holdout_epoch="v1",
        optimization_run_id=run["id"],
        dataset_version=run["dataset_version"],
        rows=rows,
    )
    replay = database.reserve_holdout(
        target="authoring_playbook",
        holdout_epoch="v1",
        optimization_run_id=run["id"],
        dataset_version=run["dataset_version"],
        rows=rows,
    )
    assert replay == initial
    assert database.used_holdout_concepts(target="authoring_playbook", holdout_epoch="v1") == {first["concept_id"]}

    next_run = database.create_optimization_run(
        target="authoring_playbook",
        dataset_version="sha256:" + "2" * 64,
        teacher_config={},
        student_config={},
    )
    with pytest.raises(VersionConflict, match="already queried"):
        database.reserve_holdout(
            target="authoring_playbook",
            holdout_epoch="v1",
            optimization_run_id=next_run["id"],
            dataset_version=next_run["dataset_version"],
            rows=rows,
        )

    # A reviewed epoch rotation is explicit and leaves the old audit row intact.
    rotated = database.reserve_holdout(
        target="authoring_playbook",
        holdout_epoch="v2",
        optimization_run_id=next_run["id"],
        dataset_version=next_run["dataset_version"],
        rows=[
            {
                "concept_id": second["concept_id"],
                "prototype_hash": "sha256:" + "b" * 64,
            }
        ],
    )
    assert rotated[0]["holdout_epoch"] == "v2"


def test_dataset_excludes_consumed_holdouts_and_epoch_changes_partition(factory_config) -> None:
    config = factory_config.model_copy(
        update={"optimizer": factory_config.optimizer.model_copy(update={"holdout_epoch": "epoch-a"})}
    )
    optimizer = Optimizer(SimpleNamespace(database=None, config=config))
    examples = [
        {
            "concept_id": f"concept-{index}",
            "attempt_id": f"attempt-{index}",
            "approved_prototype_hash": f"sha256:{index:064x}",
            "prototype_decision_id": f"decision-{index}",
            "feedback": "",
            "tags_json": "[]",
            "target": None,
            "signature": None,
            "confidence": None,
        }
        for index in range(20)
    ]
    first = optimizer._freeze_dataset(examples, excluded_concepts={"concept-0"})
    assert "concept-0" not in {row["concept_id"] for row in first["rows"]}
    assert first["holdout_epoch"] == "epoch-a"

    config.optimizer.holdout_epoch = "epoch-b"
    second = optimizer._freeze_dataset(examples, excluded_concepts=set())
    first_holdout = {row["concept_id"] for row in first["rows"] if row["split"] == "holdout"}
    second_holdout = {row["concept_id"] for row in second["rows"] if row["split"] == "holdout"}
    assert first_holdout != second_holdout


def test_pareto_selector_keeps_specialists_and_prefers_baseline_on_exact_tie() -> None:
    winner, frontier = _pareto_winner(
        {
            "baseline": {"mean": 0.5, "by_concept": {"a": 0.5, "b": 0.5}},
            "specialist-a": {"mean": 0.55, "by_concept": {"a": 0.9, "b": 0.2}},
            "specialist-b": {"mean": 0.55, "by_concept": {"a": 0.2, "b": 0.9}},
            "dominated": {"mean": 0.1, "by_concept": {"a": 0.1, "b": 0.1}},
        }
    )
    assert winner in {"specialist-a", "specialist-b"}
    assert set(frontier) == {"baseline", "specialist-a", "specialist-b"}

    tied, _ = _pareto_winner(
        {
            "baseline": {"mean": 0.8, "by_concept": {"a": 0.8}},
            "candidate": {"mean": 0.8, "by_concept": {"a": 0.8}},
        }
    )
    assert tied == "baseline"


@pytest.mark.asyncio
async def test_completed_development_generation_durably_schedules_reflective_mutations(
    factory_config, monkeypatch
) -> None:
    candidates = [
        {"id": "baseline", "skill_sha": "base", "object_ref": "cas:base"},
        {"id": "gepa-g1-1", "skill_sha": "g1", "object_ref": "cas:g1"},
    ]
    run = {
        "id": "optimization-1",
        "state": "evaluating_development",
        "version": 2,
        "dataset_version": "cas:dataset",
        "candidate_refs_json": json.dumps(candidates),
        "train_metrics_json": json.dumps(
            {
                "algorithm": "durable-gepa-v1",
                "generation": 1,
                "development_concepts": ["concept-a"],
                "history": [],
            }
        ),
    }
    attempts = [
        {
            "id": "attempt-base",
            "concept_id": "concept-a",
            "experiment_candidate": "baseline",
            "experiment_split": "development",
            "disposition": Disposition.EXPERIMENT_COMPLETE,
        },
        {
            "id": "attempt-g1",
            "concept_id": "concept-a",
            "experiment_candidate": "gepa-g1-1",
            "experiment_split": "development",
            "disposition": Disposition.EXPERIMENT_COMPLETE,
        },
    ]

    class DatabaseStub:
        def experiment_attempts(self, _: str):
            return attempts

        def get_optimization_run(self, _: str):
            return run

        def evaluations_for_attempt(self, _attempt_id: str, *, reveal: bool):
            assert reveal is True
            return [_visual_metric(judge_evaluator_version(config, "build", resolved_model="judge-v1"))]

        def update_optimization_run(self, _run_id: str, _version: int, **fields: Any):
            run.update(fields)
            run["version"] += 1
            for name, value in list(fields.items()):
                if name.endswith("_json") and not isinstance(value, str):
                    run[name] = json.dumps(value)
            return run

    config = factory_config.model_copy(
        update={
            "optimizer": factory_config.optimizer.model_copy(
                update={
                    "gepa_generations": 2,
                    "gepa_candidates_per_generation": 2,
                    "promotion_min_pairs": 3,
                    "max_metric_calls": 20,
                }
            )
        }
    )
    factory = SimpleNamespace(
        database=DatabaseStub(),
        config=config,
        persistence=SimpleNamespace(
            load_json=lambda _: {
                "rows": [
                    {
                        "concept_id": "concept-a",
                        "attempt_id": "parent-a",
                        "prototype_hash": "sha256:" + "a" * 64,
                        "prototype_decision_id": "approval-a",
                        "feedback": "retain a clearer motion rhythm",
                        "split": "development",
                    }
                ]
            }
        ),
    )
    optimizer = Optimizer(factory)

    async def expected_properties(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(optimizer, "_ensure_expected_property_evaluations", expected_properties)
    monkeypatch.setattr(optimizer, "_candidate_metric_authority_reasons", lambda *_args: [])
    monkeypatch.setattr(
        optimizer,
        "_scores",
        lambda *_args, **_kwargs: {
            "baseline": {"mean": 0.5, "by_concept": {"concept-a": 0.5}},
            "gepa-g1-1": {"mean": 0.7, "by_concept": {"concept-a": 0.7}},
        },
    )

    async def reflect(**_: Any):
        return [{"id": "gepa-g2-1", "skill_sha": "g2", "object_ref": "cas:g2"}]

    scheduled: list[tuple[list[dict[str, Any]], list[dict[str, Any]]]] = []
    monkeypatch.setattr(optimizer, "_reflect_from_rollouts", reflect)
    monkeypatch.setattr(
        optimizer,
        "_schedule_trials",
        lambda _run, proposed, rows: scheduled.append((proposed, rows)),
    )

    result = await optimizer._finish_development(run)

    assert result["generation"] == 2
    assert result["parent_candidate"] == "gepa-g1-1"
    assert result["new_trials"] == 1
    assert scheduled[0][0][0]["id"] == "gepa-g2-1"
    retained_metrics = json.loads(run["train_metrics_json"])
    assert retained_metrics["history"][0]["winner"] == "gepa-g1-1"
    assert retained_metrics["evaluator_version"] == judge_evaluator_version(
        config,
        "build",
        resolved_model="judge-v1",
    )
    assert run["state"] == "evaluating_development"


def test_novel_trace_mining_requires_positive_completed_build_and_versioned_evidence(
    database, objects, make_attempt
) -> None:
    def retained_trace(*, outcome: str, complete: bool, marker: bool):
        attempt = make_attempt(stage=Stage.FINAL_REVIEW)
        if complete:
            attempt = database.update_attempt(
                attempt["id"],
                attempt["version"],
                production_skin_id=f"skin-{attempt['id']}",
                production_revision="1",
                production_content_hash="sha256:" + "c" * 64,
            )
        stored = objects.put(b'{"trace":[{"phase":"novel-effect"}]}')
        artifact = database.add_artifact(
            attempt_id=attempt["id"],
            stage=Stage.AUTHOR,
            kind=ArtifactKind.WORKER_TRACE,
            content_hash=stored.uri,
            object_ref=stored.uri,
            media_type="application/json",
            size_bytes=stored.size,
            metadata={"novelty_candidate": marker},
        )
        decision = database.add_human_decision(
            artifact_id=artifact["id"],
            attempt_id=attempt["id"],
            action="build_quality_label",
            feedback="The motion has unusually effective craft.",
            tags=[f"outcome:{outcome}"],
            actor="human:alex",
            attempt_version=attempt["version"],
        )
        return attempt, artifact, decision

    accepted_attempt, accepted, decision = retained_trace(outcome="accept", complete=True, marker=False)
    database.add_feedback_route(
        decision_id=decision["id"],
        target="authoring_playbook",
        signature="novel animation cadence",
        confidence=0.91,
        classifier_version="gemini-3.7-flash-versioned",
        evidence={"decision": decision["id"]},
    )
    _rejected_attempt, rejected, _ = retained_trace(outcome="reject", complete=True, marker=True)
    _incomplete_attempt, incomplete, _ = retained_trace(outcome="accept", complete=False, marker=True)

    found = database.successful_novel_traces()
    assert [row["id"] for row in found] == [accepted["id"]]
    assert found[0]["attempt_id"] == accepted_attempt["id"]
    assert found[0]["route_signature"] == "novel animation cadence"
    assert rejected["id"] not in {row["id"] for row in found}
    assert incomplete["id"] not in {row["id"] for row in found}


def test_technique_trials_always_pair_candidate_with_pinned_baseline(factory_config, monkeypatch) -> None:
    skill_root = factory_config.paths.skill_dir
    references = skill_root / "references"
    references.mkdir(exist_ok=True)
    playbook = "before\n<!-- GEPA:PLAYBOOK:START -->\ncurrent recipe\n<!-- GEPA:PLAYBOOK:END -->\nafter\n"
    (references / "playbook.md").write_text(playbook, encoding="utf-8")
    (skill_root / "optimization-boundary.json").write_text(
        json.dumps(
            {
                "target": "authoring_playbook",
                "editable_path": "references/playbook.md",
                "start_marker": "<!-- GEPA:PLAYBOOK:START -->",
                "end_marker": "<!-- GEPA:PLAYBOOK:END -->",
            }
        ),
        encoding="utf-8",
    )
    bundle = SkillBundle.load(skill_root)
    factory = SimpleNamespace(
        database=SimpleNamespace(),
        config=factory_config,
        active_skill_bundle=lambda: (bundle, "HEAD", "commit"),
    )
    miner = TechniqueMiner(factory)
    monkeypatch.setattr(
        miner.optimizer,
        "_candidate_bundle",
        lambda *, candidate_id, **_kwargs: {
            "id": candidate_id,
            "skill_sha": candidate_id,
            "object_ref": f"cas:{candidate_id}",
        },
    )
    captured: dict[str, Any] = {}

    def schedule(run, candidates, rows, *, purpose):
        captured.update(run=run, candidates=candidates, rows=rows, purpose=purpose)

    monkeypatch.setattr(miner.optimizer, "_schedule_trials", schedule)
    fixtures = [
        {
            "concept_id": f"concept-{index}",
            "attempt_id": f"attempt-{index}",
            "approved_prototype_hash": f"sha256:{index:064x}",
            "prototype_decision_id": f"approval-{index}",
        }
        for index in range(3)
    ]
    proposal = TechniqueProposal(
        title="Layered traveling glint",
        recipe_markdown="Use two bounded effect layers with offset time phases. " * 3,
        applicability=["procedural bands", "slow motion"],
        expected_properties=["stable topology", "readable resting frame"],
        novelty_evidence=["retained successful trace"],
    )

    miner._schedule({"id": "technique-1"}, proposal, fixtures)

    assert [item["id"] for item in captured["candidates"]] == ["baseline", "technique"]
    assert captured["purpose"] == Purpose.TECHNIQUE
    assert len(captured["rows"]) == 3


@pytest.mark.asyncio
async def test_technique_promotion_rejects_a_paired_blocking_regression(factory_config, monkeypatch) -> None:
    attempts = [
        {
            "id": "baseline-a",
            "concept_id": "a",
            "experiment_candidate": "baseline",
            "experiment_split": "technique",
            "disposition": Disposition.EXPERIMENT_COMPLETE,
        },
        {
            "id": "technique-a",
            "concept_id": "a",
            "experiment_candidate": "technique",
            "experiment_split": "technique",
            "disposition": Disposition.EXPERIMENT_COMPLETE,
        },
        {
            "id": "baseline-b",
            "concept_id": "b",
            "experiment_candidate": "baseline",
            "experiment_split": "technique",
            "disposition": Disposition.EXPERIMENT_COMPLETE,
        },
        {
            "id": "technique-b",
            "concept_id": "b",
            "experiment_candidate": "technique",
            "experiment_split": "technique",
            "disposition": Disposition.EXPERIMENT_COMPLETE,
        },
    ]
    updated: dict[str, Any] = {}

    class DatabaseStub:
        def experiment_attempts(self, _run_id):
            return attempts

        def evaluations_for_attempt(self, attempt_id, *, reveal):
            assert reveal is True
            rows = [_visual_metric(judge_evaluator_version(factory_config, "build", resolved_model="judge-v1"))]
            if attempt_id == "technique-b":
                rows.append({"blocking": 1, "verdict": "fail", "gate_name": "temporal_loop"})
            return rows

        def update_technique_candidate(self, _id, _version, **fields):
            updated.update(fields)
            return {"id": "technique-record", **fields}

    calibration = SimpleNamespace(enabled=True, as_report=lambda: {"enabled": True})
    factory = SimpleNamespace(
        database=DatabaseStub(),
        config=factory_config.model_copy(
            update={"optimizer": factory_config.optimizer.model_copy(update={"technique_min_fixtures": 2})}
        ),
        calibration=SimpleNamespace(quality_status=lambda _kind, **_kwargs: calibration),
    )
    miner = TechniqueMiner(factory)
    scores = {
        "baseline-a": 0.8,
        "technique-a": 0.9,
        "baseline-b": 0.8,
        "technique-b": 0.9,
    }
    miner.optimizer._trial_score = lambda attempt, **_kwargs: scores[attempt["id"]]  # type: ignore[method-assign]

    async def expected_properties(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(miner.optimizer, "_ensure_expected_property_evaluations", expected_properties)
    monkeypatch.setattr(miner.optimizer, "_candidate_metric_authority_reasons", lambda *_args: [])
    monkeypatch.setattr(
        miner.optimizer,
        "_scores",
        lambda *_args, **_kwargs: {
            "baseline": {"visual_by_concept": {"a": 0.8, "b": 0.8}},
            "technique": {"visual_by_concept": {"a": 0.9, "b": 0.9}},
        },
    )

    result = await miner._advance({"id": "technique-record", "version": 1, "disposition": "trials_running"})

    assert result["state"] == "rejected"
    assert updated["trial_results"]["passed"] is False
    assert updated["trial_results"]["blocking_regressions"] == [
        {"concept_id": "b", "attempt_id": "technique-b", "gate": "temporal_loop"}
    ]


def test_needs_review_gallery_aggregates_both_inboxes_and_re_evaluation_outcomes(database, make_attempt) -> None:
    attempts = []
    for review_kind in ("prototype", "final", "re_evaluation"):
        attempt = make_attempt(stage=Stage.COMPLETE, disposition=Disposition.NEEDS_HUMAN)
        attempts.append(database.update_attempt(attempt["id"], attempt["version"], review_kind=review_kind))
    assert VIEW_MAP["needs-review"] == ("Needs review", "needs_review")
    assert {row["id"] for row in database.list_gallery("needs_review")} == {row["id"] for row in attempts}


@pytest.mark.asyncio
async def test_generation_wip_halt_still_dispatches_optimizer_from_one_run_once(
    factory_config, database, objects, make_attempt, monkeypatch
) -> None:
    waiting = make_attempt(stage=Stage.PROTOTYPE_REVIEW, disposition=Disposition.NEEDS_HUMAN)
    database.update_attempt(waiting["id"], waiting["version"], review_kind="prototype")
    config = factory_config.model_copy(
        update={
            "optimizer": factory_config.optimizer.model_copy(update={"enabled": True}),
            "budgets": factory_config.budgets.model_copy(update={"max_pending_prototype_reviews": 1}),
        }
    )
    factory = Factory(config, database=database, objects=objects)

    async def advance(_self):
        return {"state": "queued", "run_id": "optimization-ready"}

    monkeypatch.setattr(Optimizer, "advance_if_ready", advance)
    report = await factory.run_once()
    await factory.close()

    assert report["halt"] == {"reason": "prototype_review_wip_cap"}
    assert report["optimizer"] == {
        "state": "queued",
        "run_id": "optimization-ready",
    }
    assert report["advanced"] == []


@pytest.mark.asyncio
async def test_long_boundary_heartbeats_lease_before_another_runner_can_acquire(
    factory_config, database, objects
) -> None:
    config = factory_config.model_copy(update={"lease_seconds": 60})
    # Use a short runtime value to exercise the clock without weakening the
    # validated production config minimum.
    config.lease_seconds = 1
    factory = Factory(config, database=database, objects=objects)
    token = database.acquire_lease("production", "service:first", 1)
    factory._lease_token = token

    async def boundary() -> str:
        await asyncio.sleep(1.4)
        return "complete"

    task = asyncio.create_task(factory._with_lease_heartbeat(boundary()))
    await asyncio.sleep(1.15)
    with pytest.raises(LeaseBusy):
        database.acquire_lease("production", "service:second", 1)
    assert await task == "complete"

    database.release_lease("production", token)
    factory._lease_token = None
    await factory.close()


@pytest.mark.asyncio
async def test_gepa_holdout_cannot_promote_with_any_blocking_gate_regression(
    factory_config,
    monkeypatch,
) -> None:
    attempts = []
    for concept in ("a", "b", "c"):
        attempts.extend(
            [
                {
                    "id": f"baseline-{concept}",
                    "concept_id": concept,
                    "experiment_candidate": "baseline",
                    "experiment_split": "holdout",
                    "disposition": Disposition.EXPERIMENT_COMPLETE,
                },
                {
                    "id": f"candidate-{concept}",
                    "concept_id": concept,
                    "experiment_candidate": "gepa-g2-1",
                    "experiment_split": "holdout",
                    "disposition": Disposition.EXPERIMENT_COMPLETE,
                },
            ]
        )
    run = {
        "id": "optimization-blocking",
        "version": 3,
        "state": "evaluating_holdout",
        "dev_metrics_json": json.dumps({"winner": "gepa-g2-1"}),
        "train_metrics_json": json.dumps(
            {
                "evaluator_version": judge_evaluator_version(
                    factory_config,
                    "build",
                    resolved_model="judge-v1",
                )
            }
        ),
        "candidate_refs_json": json.dumps(
            [
                {"id": "baseline", "object_ref": "cas:baseline", "skill_sha": "base"},
                {"id": "gepa-g2-1", "object_ref": "cas:candidate", "skill_sha": "candidate"},
            ]
        ),
    }
    updated: dict[str, Any] = {}

    class DatabaseStub:
        def experiment_attempts(self, _run_id):
            return attempts

        def evaluations_for_attempt(self, attempt_id, *, reveal):
            assert reveal is True
            rows = [_visual_metric(judge_evaluator_version(factory_config, "build", resolved_model="judge-v1"))]
            if attempt_id == "candidate-b":
                rows.append({"blocking": 1, "verdict": "fail", "gate_name": "seam"})
            return rows

        def get_optimization_run(self, _run_id):
            return run

        def update_optimization_run(self, _run_id, _version, **fields):
            updated.update(fields)
            return {**run, **fields}

    calibration = SimpleNamespace(
        enabled=True,
        reasons=[],
        as_report=lambda: {"enabled": True},
    )
    calibration_calls: list[str] = []

    def quality_status(_kind: str, *, evaluator_version: str):
        calibration_calls.append(evaluator_version)
        return calibration

    config = factory_config.model_copy(
        update={
            "optimizer": factory_config.optimizer.model_copy(
                update={
                    "promotion_min_pairs": 3,
                    "promotion_min_effect": 0.01,
                }
            )
        }
    )
    factory = SimpleNamespace(
        database=DatabaseStub(),
        config=config,
        calibration=SimpleNamespace(quality_status=quality_status),
    )
    optimizer = Optimizer(factory)

    async def expected_properties(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(optimizer, "_ensure_expected_property_evaluations", expected_properties)
    monkeypatch.setattr(optimizer, "_candidate_metric_authority_reasons", lambda *_args: [])
    optimizer._scores = lambda *_args, **_kwargs: {  # type: ignore[method-assign]
        "baseline": {"mean": 0.4, "by_concept": {key: 0.4 for key in ("a", "b", "c")}},
        "gepa-g2-1": {"mean": 0.9, "by_concept": {key: 0.9 for key in ("a", "b", "c")}},
    }

    result = await optimizer._finish_holdout(run)

    assert result["state"] == "evaluated_not_promoted"
    assert "candidate has a blocking-gate regression" in result["reasons"]
    assert updated["holdout_metrics_json"]["blocking_regressions"] == [
        {"concept_id": "b", "attempt_id": "candidate-b", "gate": "seam"}
    ]
    assert calibration_calls == [judge_evaluator_version(factory_config, "build", resolved_model="judge-v1")]
