from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest

from snaketron_factory.calibration import judge_evaluator_version
from snaketron_factory.domain import ArtifactKind, Disposition, Purpose, Stage
from snaketron_factory.factory import Factory
from snaketron_factory.optimizer import freeze_expected_property_contract
from snaketron_factory.techniques import (
    TechniqueMiner,
    TechniqueProposal,
    fixture_structural_signature,
)
from snaketron_factory.worker import SkillBundle


def _structure(kind: str, provenance: str) -> dict[str, Any]:
    if kind == "formula":
        plan = {"path": "layers", "asset_plan": []}
        document = {
            "textures": [],
            "layers": [
                {
                    "type": "span",
                    "region": "body",
                    "source": {
                        "type": "gradient",
                        "axis": "along_body",
                        "stops": [
                            {"offset": "tri(time)", "alpha": 1},
                            {"offset": 1, "alpha": 0},
                        ],
                    },
                }
            ],
        }
    elif kind == "texture":
        plan = {"path": "texture", "asset_plan": [{"kind": "coat"}]}
        document = {
            "textures": [{"name": "coat", "kind": "coat"}],
            "layers": [
                {
                    "type": "span",
                    "region": "body",
                    "source": {
                        "type": "image",
                        "texture": "coat",
                        "fit": {"type": "tile"},
                        "drift_cells": 0,
                    },
                }
            ],
        }
    elif kind == "sprite":
        plan = {"path": "sprite_sheet", "asset_plan": [{"kind": "sheet"}]}
        document = {
            "textures": [{"name": "sheet", "kind": "sheet"}],
            "layers": [
                {
                    "type": "span",
                    "region": "body",
                    "source": {
                        "type": "image",
                        "texture": "sheet",
                        "fit": {"type": "clip"},
                        "drift_cells": 0,
                    },
                }
            ],
        }
    else:  # pragma: no cover - test helper guard
        raise AssertionError(kind)
    return fixture_structural_signature(
        plan,
        document,
        plan_content_hash=f"sha256:plan-{provenance}",
        document_content_hash=f"sha256:document-{provenance}",
    )


def _fixture(index: int, structure: dict[str, Any]) -> dict[str, Any]:
    return {
        "concept_id": f"concept-{index}",
        "attempt_id": f"attempt-{index}",
        "approved_prototype_hash": f"sha256:{index:064x}",
        "prototype_decision_id": f"approval-{index}",
        "fixture_structure": structure,
    }


def _configured(factory_config, *, minimum_fixtures: int = 3, minimum_structures: int = 3):
    return factory_config.model_copy(
        update={
            "optimizer": factory_config.optimizer.model_copy(
                update={
                    "technique_min_fixtures": minimum_fixtures,
                    "technique_min_structural_signatures": minimum_structures,
                }
            )
        }
    )


@pytest.mark.asyncio
async def test_distinct_concepts_with_one_structure_are_not_ready(
    factory_config,
    monkeypatch,
) -> None:
    config = _configured(factory_config)
    database = SimpleNamespace(
        ready_technique_candidate=lambda: None,
        successful_novel_traces=lambda **_kwargs: [{"id": "trace"}],
        technique_source_exists=lambda _source_id: False,
    )
    miner = TechniqueMiner(SimpleNamespace(database=database, config=config))
    fixtures = [_fixture(index, _structure("formula", str(index))) for index in range(5)]
    monkeypatch.setattr(miner.optimizer, "_eligible_examples", lambda: fixtures)

    report = await miner.advance_if_ready()

    assert report["state"] == "not_ready"
    assert report["reason"] == "not enough distinct fixture structures"
    assert report["available_fixtures_with_structure"] == 5
    assert report["available_structural_signatures"] == 1
    assert report["required_structural_signatures"] == 3


def test_varied_approved_fixtures_schedule_paired_trials(factory_config, monkeypatch) -> None:
    config = _configured(factory_config)
    skill_root = config.paths.skill_dir
    references = skill_root / "references"
    references.mkdir(exist_ok=True)
    playbook = "before\n<!-- GEPA:PLAYBOOK:START -->\ncurrent\n<!-- GEPA:PLAYBOOK:END -->\nafter\n"
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
        config=config,
        active_skill_bundle=lambda: (bundle, "HEAD", "commit"),
    )
    miner = TechniqueMiner(factory)
    fixtures = [
        _fixture(0, _structure("formula", "a")),
        _fixture(1, _structure("texture", "b")),
        _fixture(2, _structure("sprite", "c")),
    ]
    selected, report = miner._select_structurally_varied_fixtures(fixtures)
    assert report["selected_structural_signatures"] == 3
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
    monkeypatch.setattr(
        miner.optimizer,
        "_schedule_trials",
        lambda run, candidates, rows, *, purpose: captured.update(
            run=run,
            candidates=candidates,
            rows=rows,
            purpose=purpose,
        ),
    )
    proposal = TechniqueProposal(
        title="Cross-topology traveling glint",
        recipe_markdown="Use bounded evidence from each topology and preserve the fixture contract. " * 3,
        applicability=["procedural layers", "generated assets"],
        expected_properties=["stable topology", "readable resting frame"],
        novelty_evidence=["retained successful trace"],
    )

    miner._schedule({"id": "technique-varied"}, proposal, selected)

    assert [item["id"] for item in captured["candidates"]] == ["baseline", "technique"]
    assert captured["purpose"] == Purpose.TECHNIQUE
    assert len(captured["rows"]) == 3
    assert len({row["fixture_structure"]["structural_signature"] for row in captured["rows"]}) == 3


@pytest.mark.asyncio
async def test_derived_structure_is_persisted_in_each_real_trial_behavior(
    factory_config,
    database,
    objects,
    make_attempt,
) -> None:
    factory = Factory(factory_config, database=database, objects=objects)
    bundle = SkillBundle.load(factory_config.paths.skill_dir)
    factory.active_skill_bundle = lambda: (bundle, "HEAD", "test-skill-commit")  # type: ignore[method-assign]
    parent = make_attempt(stage=Stage.COMPLETE)
    plan = {"path": "layers", "asset_plan": []}
    document = {
        "textures": [],
        "layers": [
            {
                "type": "span",
                "region": "body",
                "source": {
                    "type": "gradient",
                    "axis": "along_body",
                    "stops": [{"offset": "tri(time)", "alpha": 1}],
                },
            }
        ],
    }
    factory._store_json_artifact(parent, Stage.AUTHOR, ArtifactKind.IMPLEMENTATION_PLAN, plan)
    factory._store_json_artifact(parent, Stage.BUILD_GATE, ArtifactKind.SKIN_DOCUMENT, document)
    miner = TechniqueMiner(factory)
    fixture = {
        "concept_id": parent["concept_id"],
        "attempt_id": parent["id"],
        "approved_prototype_hash": "sha256:" + "a" * 64,
        "prototype_decision_id": "approval-a",
    }
    structure = miner._fixture_structure(fixture)
    assert structure is not None
    assert structure["implementation_path"] == "layers"
    assert structure["animation_mechanisms"] == ["formula", "formula:tri"]
    candidate_payload = objects.put(json.dumps({"playbook": "candidate"}).encode())
    contract = freeze_expected_property_contract(
        {
            **fixture,
            "prototype_hash": fixture["approved_prototype_hash"],
            "feedback": "preserve the moving gradient",
        }
    )

    miner.optimizer._schedule_trials(
        {"id": "technique-persistence"},
        [
            {
                "id": "baseline",
                "skill_sha": "skill-sha",
                "object_ref": candidate_payload.uri,
            }
        ],
        [
            {
                "concept_id": parent["concept_id"],
                "attempt_id": parent["id"],
                "prototype_hash": fixture["approved_prototype_hash"],
                "prototype_decision_id": fixture["prototype_decision_id"],
                "split": "technique",
                "expected_property_contract": contract,
                "fixture_structure": structure,
            }
        ],
        purpose=Purpose.TECHNIQUE,
    )

    [trial] = database.experiment_attempts("technique-persistence")
    behavior = json.loads(trial["behavior_json"])
    assert behavior["fixture_structural_signature"] == structure
    await factory.close()


def _metric(config) -> dict[str, Any]:
    version = judge_evaluator_version(config, "build", resolved_model="judge-v1")
    return {
        "evaluator": "visual_judge",
        "evaluator_version": version,
        "gate_name": "visual_fidelity",
        "blocking": 0,
        "verdict": "candidate",
        "measurements_json": json.dumps(
            {
                **{
                    name: 0.9
                    for name in (
                        "fidelity",
                        "readability",
                        "role_clarity",
                        "animation_quality",
                        "craft",
                    )
                },
                "resolved_model": "judge-v1",
            }
        ),
    }


async def _promotion_result(
    factory_config,
    monkeypatch,
    *,
    expected: dict[str, dict[str, Any]],
    observed: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    config = _configured(factory_config, minimum_fixtures=2, minimum_structures=2)
    attempts = [
        {
            "id": f"{candidate_id}-{concept_id}",
            "concept_id": concept_id,
            "experiment_candidate": candidate_id,
            "experiment_split": "technique",
            "disposition": Disposition.EXPERIMENT_COMPLETE,
            "behavior_json": json.dumps({"fixture_structural_signature": observed[(concept_id, candidate_id)]}),
        }
        for concept_id in expected
        for candidate_id in ("baseline", "technique")
    ]
    updated: dict[str, Any] = {}

    class DatabaseStub:
        def experiment_attempts(self, _run_id: str):
            return attempts

        def evaluations_for_attempt(self, _attempt_id: str, *, reveal: bool):
            assert reveal is True
            return [_metric(config)]

        def update_technique_candidate(self, _candidate_id: str, _version: int, **fields: Any):
            updated.update(fields)
            return {"id": "technique-record", **fields}

    calibration = SimpleNamespace(enabled=True, as_report=lambda: {"enabled": True})
    factory = SimpleNamespace(
        database=DatabaseStub(),
        config=config,
        persistence=SimpleNamespace(
            load_json=lambda ref: {"fixture_structures": expected} if ref == "cas:recipe" else None
        ),
        calibration=SimpleNamespace(quality_status=lambda _kind, **_kwargs: calibration),
    )
    miner = TechniqueMiner(factory)

    async def expected_properties(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(miner.optimizer, "_ensure_expected_property_evaluations", expected_properties)
    monkeypatch.setattr(miner.optimizer, "_candidate_metric_authority_reasons", lambda *_args: [])
    monkeypatch.setattr(
        miner.optimizer,
        "_scores",
        lambda *_args, **_kwargs: {
            "baseline": {"visual_by_concept": {concept: 0.85 for concept in expected}},
            "technique": {"visual_by_concept": {concept: 0.9 for concept in expected}},
        },
    )
    candidate = {
        "id": "technique-record",
        "version": 1,
        "disposition": "trials_running",
        "recipe_ref": "cas:recipe",
        "trial_results_json": json.dumps({"fixture_structures": expected}),
    }
    result = await miner._advance(candidate)
    assert result["state"] == "rejected"
    return updated["trial_results"]


@pytest.mark.asyncio
async def test_promotion_rejects_fixture_structural_signature_drift(
    factory_config,
    monkeypatch,
) -> None:
    first = _structure("formula", "a")
    second = _structure("texture", "b")
    expected = {"concept-a": first, "concept-b": second}
    observed = {
        (concept, candidate): signature
        for concept, signature in expected.items()
        for candidate in ("baseline", "technique")
    }
    observed[("concept-b", "technique")] = first

    results = await _promotion_result(
        factory_config,
        monkeypatch,
        expected=expected,
        observed=observed,
    )

    assert results["passed"] is False
    assert "fixture concept-b structural signature drifted" in results["fixture_structure_reasons"]
    assert results["fixture_structure_evidence"]["trial_structures"]["technique-concept-b"] == first


@pytest.mark.asyncio
async def test_promotion_rejects_missing_structural_diversity(factory_config, monkeypatch) -> None:
    # Separate retained artifact provenance is not structural diversity.
    first = _structure("formula", "a")
    second = _structure("formula", "b")
    assert first["structural_signature"] == second["structural_signature"]
    expected = {"concept-a": first, "concept-b": second}
    observed = {
        (concept, candidate): signature
        for concept, signature in expected.items()
        for candidate in ("baseline", "technique")
    }

    results = await _promotion_result(
        factory_config,
        monkeypatch,
        expected=expected,
        observed=observed,
    )

    assert results["passed"] is False
    assert (
        "fixture structural diversity is below the configured promotion minimum" in results["fixture_structure_reasons"]
    )
    evidence = results["fixture_structure_evidence"]
    assert evidence["required_distinct_signatures"] == 2
    assert len(evidence["observed_distinct_signatures"]) == 1
