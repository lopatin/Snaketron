"""Novel trace → experimental recipe → varied trials → bounded promotion."""

from __future__ import annotations

import json
import statistics
import subprocess
from pathlib import Path
from typing import Any

from pydantic import Field

from .db import canonical_json
from .domain import Disposition, ProviderError, Purpose, Stage, StrictModel
from .operations import ExistingOperation
from .optimizer import (
    EvaluatorVersionDrift,
    ExpectedPropertyError,
    Optimizer,
    _extract_section,
    _promotion_provider_result,
    _replace_section,
    freeze_expected_property_contract,
    known_meta_failure,
)
from .promotion import GitPromoter


class TechniqueProposal(StrictModel):
    title: str = Field(min_length=3, max_length=100)
    recipe_markdown: str = Field(min_length=100, max_length=8_000)
    applicability: list[str] = Field(min_length=2)
    expected_properties: list[str] = Field(min_length=2, max_length=8)
    novelty_evidence: list[str] = Field(min_length=1)


class TechniqueMiner:
    def __init__(self, factory: Any) -> None:
        self.factory = factory
        self.database = factory.database
        self.config = factory.config
        self.optimizer = Optimizer(factory)

    async def advance_if_ready(self) -> dict[str, Any]:
        active = self.database.ready_technique_candidate()
        if active:
            try:
                return await self._advance(active)
            except (ProviderError, ExistingOperation) as error:
                if not known_meta_failure(self.database, error):
                    raise
                current = self.database.ready_technique_candidate()
                if current is None or current["id"] != active["id"]:
                    current = active
                updated = self.database.update_technique_candidate(
                    current["id"],
                    current["version"],
                    disposition="rejected",
                    trial_results={
                        "passed": False,
                        "reason": "known_external_failure",
                        "detail": str(error),
                    },
                )
                return {"state": updated["disposition"], "candidate_id": updated["id"]}
        source = next(
            (
                row
                for row in self.database.successful_novel_traces(
                    min_confidence=self.config.optimizer.feedback_min_confidence
                )
                if not self.database.technique_source_exists(row["id"])
            ),
            None,
        )
        if source is None:
            return {"state": "not_ready"}
        fixtures = self.optimizer._eligible_examples()
        distinct: dict[str, dict[str, Any]] = {}
        for fixture in fixtures:
            distinct.setdefault(fixture["concept_id"], fixture)
        selected = list(distinct.values())[: self.config.optimizer.technique_min_fixtures]
        if len(selected) < self.config.optimizer.technique_min_fixtures:
            return {
                "state": "not_ready",
                "reason": "not enough varied human-approved fixtures",
            }
        # Mine historical evidence with the current behavior snapshot; the
        # source production Attempt may be deliberately stale after an upgrade.
        attempt = self.factory.control_attempt("optimizer")
        provider = self.factory.providers.role("smart_text")
        trace = self.factory.persistence.load_json(source["object_ref"])
        request = {
            "successful_trace": trace,
            "human_feedback": source["feedback"],
            "quick_tags": json.loads(source["tags_json"]),
            "constraint": "recipe text only; no fixture, gate, schema, or code changes",
        }
        try:
            operation, result = await self.factory._provider_call(
                attempt=attempt,
                stage=Stage(attempt["stage"]),
                key=f"technique-mine:{source['id']}:{attempt['id']}:v1",
                role="smart_text",
                side_effect="mine_novel_authoring_technique",
                request=request,
                invoke=lambda: provider.generate_structured(
                    system=(
                        "Extract one repeatable animation or asset-authoring technique from the "
                        "successful trace. It must be safe to add as an experimental playbook recipe. "
                        "Do not propose code, gate, schema, or fixture self-modification."
                    ),
                    prompt=canonical_json(request),
                    schema=TechniqueProposal,
                    temperature=0.3,
                ),
            )
        except (ProviderError, ExistingOperation) as error:
            if not known_meta_failure(self.database, error):
                raise
            failure = self.factory.objects.put(
                canonical_json(
                    {
                        "schema_version": 1,
                        "reason": "known_external_failure",
                        "detail": str(error),
                        "source_artifact_id": source["id"],
                    }
                ).encode()
            )
            candidate = self.database.create_technique_candidate(
                source_artifact_id=source["id"],
                recipe_ref=failure.uri,
                recipe_sha=failure.sha256,
                fixture_refs=[row["prototype_hash"] for row in selected],
            )
            candidate = self.database.update_technique_candidate(
                candidate["id"],
                candidate["version"],
                disposition="rejected",
                trial_results={
                    "passed": False,
                    "reason": "known_external_failure",
                    "detail": str(error),
                },
            )
            return {"state": candidate["disposition"], "candidate_id": candidate["id"]}
        proposal = self.factory._model_result(operation, result, TechniqueProposal)
        fixture_contracts = {
            row["concept_id"]: freeze_expected_property_contract(
                {
                    **row,
                    "prototype_hash": row["approved_prototype_hash"],
                    "root_cause": row.get("signature"),
                },
                additional_properties=proposal.expected_properties,
            )
            for row in selected
        }
        recipe_payload = {
            "schema_version": 2,
            "proposal": proposal.model_dump(mode="json"),
            "fixture_contracts": fixture_contracts,
        }
        recipe = self.factory.objects.put(canonical_json(recipe_payload).encode())
        candidate = self.database.create_technique_candidate(
            source_artifact_id=source["id"],
            recipe_ref=recipe.uri,
            recipe_sha=recipe.sha256,
            fixture_refs=[row["prototype_hash"] for row in selected],
        )
        self._schedule(candidate, proposal, selected, fixture_contracts)
        candidate = self.database.update_technique_candidate(
            candidate["id"],
            candidate["version"],
            disposition="trials_running",
            trial_results={"operation_id": operation["id"], "title": proposal.title},
        )
        return {"state": candidate["disposition"], "candidate_id": candidate["id"]}

    def _schedule(
        self,
        candidate: dict[str, Any],
        proposal: TechniqueProposal,
        fixtures: list[dict[str, Any]],
        fixture_contracts: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        fixture_contracts = fixture_contracts or {
            item["concept_id"]: freeze_expected_property_contract(
                {
                    **item,
                    "prototype_hash": item["approved_prototype_hash"],
                },
                additional_properties=proposal.expected_properties,
            )
            for item in fixtures
        }
        skill, _skill_ref, _skill_commit = self.factory.active_skill_bundle()
        boundary = json.loads((self.config.paths.skill_dir / "optimization-boundary.json").read_text())
        path = boundary["editable_path"]
        playbook = skill.files[path]
        current = _extract_section(playbook, boundary["start_marker"], boundary["end_marker"])
        addition = (
            f"\n\n## Experimental recipe: {proposal.title}\n\n{proposal.recipe_markdown.strip()}\n\n"
            f"Applicability: {', '.join(proposal.applicability)}.\n"
            f"Expected evidence: {', '.join(proposal.expected_properties)}."
        )
        full = _replace_section(playbook, boundary["start_marker"], boundary["end_marker"], current + addition)
        baseline = self.optimizer._candidate_bundle(
            candidate_id="baseline",
            skill=skill,
            path=path,
            full_playbook=playbook,
            rationale="Pinned production baseline for paired technique evaluation",
        )
        technique = self.optimizer._candidate_bundle(
            candidate_id="technique",
            skill=skill,
            path=path,
            full_playbook=full,
            rationale=f"Experimental technique: {proposal.title}",
        )
        rows = [
            {
                "concept_id": item["concept_id"],
                "attempt_id": item["attempt_id"],
                "prototype_hash": item["approved_prototype_hash"],
                "prototype_decision_id": item["prototype_decision_id"],
                "split": "technique",
                "expected_property_contract": fixture_contracts[item["concept_id"]],
            }
            for item in fixtures
        ]
        self.optimizer._schedule_trials(
            {"id": candidate["id"]},
            [baseline, technique],
            rows,
            purpose=Purpose.TECHNIQUE,
        )

    async def _advance(self, candidate: dict[str, Any]) -> dict[str, Any]:
        attempts = self.database.experiment_attempts(candidate["id"])
        active = [row for row in attempts if row["disposition"] == Disposition.ACTIVE]
        if active:
            return {
                "state": candidate["disposition"],
                "candidate_id": candidate["id"],
                "remaining": len(active),
            }
        try:
            evaluator_version = self.optimizer._scoring_evaluator_version(
                attempts,
                split="technique",
            )
        except EvaluatorVersionDrift as error:
            results = {
                "passed": False,
                "reason": "evaluator_version_drift",
                "detail": str(error),
                "trials": [row["id"] for row in attempts],
            }
            updated = self.database.update_technique_candidate(
                candidate["id"],
                candidate["version"],
                disposition="rejected",
                trial_results=results,
            )
            return {"state": updated["disposition"], "candidate_id": candidate["id"]}
        try:
            await self.optimizer._ensure_expected_property_evaluations(
                attempts,
                split="technique",
            )
        except ExpectedPropertyError as error:
            results = {
                "passed": False,
                "reason": "expected_property_evidence",
                "detail": str(error),
                "trials": [row["id"] for row in attempts],
            }
            updated = self.database.update_technique_candidate(
                candidate["id"],
                candidate["version"],
                disposition="rejected",
                trial_results=results,
            )
            return {"state": updated["disposition"], "candidate_id": candidate["id"]}
        try:
            scores = self.optimizer._scores(
                attempts,
                split="technique",
                evaluator_version=evaluator_version,
            )
        except ExpectedPropertyError as error:
            results = {
                "passed": False,
                "reason": "expected_property_evidence",
                "detail": str(error),
                "trials": [row["id"] for row in attempts],
            }
            updated = self.database.update_technique_candidate(
                candidate["id"],
                candidate["version"],
                disposition="rejected",
                trial_results=results,
            )
            return {"state": updated["disposition"], "candidate_id": candidate["id"]}
        blocking_regressions: list[dict[str, str]] = []
        for attempt in attempts:
            candidate_name = str(attempt["experiment_candidate"])
            if candidate_name != "technique":
                continue
            for evaluation in self.database.evaluations_for_attempt(attempt["id"], reveal=True):
                if evaluation["blocking"] and evaluation["verdict"] == "fail":
                    blocking_regressions.append(
                        {
                            "concept_id": attempt["concept_id"],
                            "attempt_id": attempt["id"],
                            "gate": evaluation["gate_name"],
                        }
                    )
        baseline_by_concept = scores.get("baseline", {}).get("visual_by_concept", {})
        technique_by_concept = scores.get("technique", {}).get("visual_by_concept", {})
        paired_concepts = sorted(set(baseline_by_concept).intersection(technique_by_concept))
        baseline_scores = [baseline_by_concept[key] for key in paired_concepts]
        technique_scores = [technique_by_concept[key] for key in paired_concepts]
        deltas = [candidate - baseline for baseline, candidate in zip(baseline_scores, technique_scores, strict=True)]
        authority_reasons = self.optimizer._candidate_metric_authority_reasons(
            scores,
            "technique",
        )
        calibration = self.factory.calibration.quality_status(
            "build",
            evaluator_version=evaluator_version,
        )
        passed = (
            len(paired_concepts) >= self.config.optimizer.technique_min_fixtures
            and not blocking_regressions
            and not authority_reasons
            and min(technique_scores, default=0) >= 0.7
            and statistics.fmean(technique_scores) >= 0.78
            and min(deltas, default=-1) >= -0.05
            and statistics.fmean(deltas) >= 0
            and calibration.enabled
        )
        results = {
            "baseline_scores": baseline_scores,
            "technique_scores": technique_scores,
            "paired_deltas": deltas,
            "paired_concepts": paired_concepts,
            "blocking_regressions": blocking_regressions,
            "expected_property_authority_reasons": authority_reasons,
            "paired_scores": scores,
            "passed": passed,
            "trials": [row["id"] for row in attempts],
            "evaluator_version": evaluator_version,
            "judge_calibration": calibration.as_report(),
        }
        if not passed:
            updated = self.database.update_technique_candidate(
                candidate["id"],
                candidate["version"],
                disposition="rejected",
                trial_results=results,
            )
            return {"state": updated["disposition"], "candidate_id": candidate["id"]}
        technique_attempts = [row for row in attempts if row["experiment_candidate"] == "technique"]
        trial_candidate = self.factory._find_lineage_artifact(technique_attempts[0], self._candidate_artifact_kind())
        if trial_candidate is None:
            raise RuntimeError("technique trial lost its candidate bundle")
        payload = self.factory.persistence.load_json(trial_candidate["object_ref"])
        active_behavior = self.database.active_behavior("author-skin")
        if active_behavior is None:
            head = _git_head(self.config.paths.repo_root)
            self.database.set_active_behavior("author-skin", "HEAD", head)
            active_behavior = self.database.active_behavior("author-skin")
        assert active_behavior is not None
        operation, provider_result = await self.factory._provider_call(
            attempt=technique_attempts[0],
            stage=Stage.COMPLETE,
            key=f"technique:{candidate['id']}:promote:{active_behavior['sha']}",
            role="git_promotion",
            side_effect="promote_animation_technique",
            request={
                "candidate_id": candidate["id"],
                "recipe_sha": candidate["recipe_sha"],
                "expected_active_sha": active_behavior["sha"],
            },
            invoke=lambda: _promotion_provider_result(
                GitPromoter(self.config, self.database).promote_playbook(
                    candidate_playbook=payload["playbook"],
                    run_id=candidate["id"],
                    expected_head=active_behavior["sha"],
                    expected_active_sha=active_behavior["sha"],
                )
            ),
        )
        promotion = self.factory._json_result(operation, provider_result)
        results.update({"promoted_ref": promotion["git_ref"], "promoted_sha": promotion["sha"]})
        updated = self.database.update_technique_candidate(
            candidate["id"],
            candidate["version"],
            disposition="promoted",
            trial_results=results,
        )
        return {
            "state": updated["disposition"],
            "candidate_id": candidate["id"],
            "git_ref": promotion["git_ref"],
        }

    @staticmethod
    def _candidate_artifact_kind():
        from .domain import ArtifactKind

        return ArtifactKind.OPTIMIZER_CANDIDATE


def _git_head(repo: Path) -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo,
        text=True,
        capture_output=True,
        check=True,
        timeout=10,
    )
    return completed.stdout.strip()
