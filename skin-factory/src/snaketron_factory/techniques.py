"""Novel trace → experimental recipe → varied trials → bounded promotion."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import statistics
import subprocess
from collections import Counter
from pathlib import Path
from typing import Any

from pydantic import Field

from .db import canonical_json
from .domain import ArtifactKind, Disposition, ProviderError, Purpose, Stage, StrictModel
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

STRUCTURAL_SIGNATURE_VERSION = "fixture-structure-v1"
_IMPLEMENTATION_PATHS = {"layers", "texture", "sprite_sheet", "hybrid"}
_ASSET_KINDS = {"coat", "overlay", "sheet"}
_TIME_TOKEN = re.compile(r"(?<![A-Za-z0-9_])time(?![A-Za-z0-9_])")
_CALL = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(")


def _expression_mechanisms(value: Any) -> set[str]:
    """Return value-independent animation mechanisms used by one expression."""

    if not isinstance(value, str) or _TIME_TOKEN.search(value) is None:
        return set()
    calls = {match.lower() for match in _CALL.findall(value)}
    return {"formula", *(f"formula:{call}" for call in calls or {"direct"})}


def _source_structure(source: Any, texture_kinds: dict[str, str]) -> tuple[str, set[str]]:
    if not isinstance(source, dict):
        return "missing", set()
    source_type = source.get("type")
    mechanisms: set[str] = set()
    if source_type == "solid":
        return "solid", mechanisms
    if source_type == "gradient":
        stops = source.get("stops")
        if not isinstance(stops, list):
            stops = []
        animated_sites: set[str] = set()
        for stop in stops:
            if not isinstance(stop, dict):
                continue
            for field in ("offset", "alpha"):
                found = _expression_mechanisms(stop.get(field))
                if found:
                    animated_sites.add(field)
                    mechanisms.update(found)
        return (
            "gradient:"
            f"{source.get('axis', 'unknown')}:stops={len(stops)}:"
            f"animated={','.join(sorted(animated_sites)) or 'none'}",
            mechanisms,
        )
    if source_type == "band":
        animated_sites = set()
        for field in ("half_width", "t_center", "alpha"):
            found = _expression_mechanisms(source.get(field))
            if found:
                animated_sites.add(field)
                mechanisms.update(found)
        return f"band:animated={','.join(sorted(animated_sites)) or 'none'}", mechanisms
    if source_type == "image":
        fit = source.get("fit")
        fit_type = fit.get("type", "unknown") if isinstance(fit, dict) else fit
        texture = source.get("texture")
        texture_kind = texture_kinds.get(str(texture), "unknown")
        drift = _expression_mechanisms(source.get("drift_cells"))
        mechanisms.update(drift)
        return (
            f"image:{texture_kind}:fit={fit_type or 'unknown'}:animated_drift={'yes' if drift else 'no'}",
            mechanisms,
        )
    if source_type == "text":
        return "text", mechanisms
    return f"unknown:{source_type}", mechanisms


def _layer_structure(layers: Any, texture_kinds: dict[str, str]) -> tuple[list[str], set[str]]:
    """Describe ordered effect topology while excluding style/content values."""

    if not isinstance(layers, list):
        return [], set()
    structure: list[str] = []
    mechanisms: set[str] = set()

    def visit(layer: Any) -> None:
        if not isinstance(layer, dict):
            structure.append("invalid")
            return
        layer_type = str(layer.get("type", "unknown"))
        animated_sites: set[str] = set()
        found = _expression_mechanisms(layer.get("opacity"))
        if found:
            animated_sites.add("opacity")
            mechanisms.update(found)
        transform = layer.get("transform")
        if isinstance(transform, dict):
            for field, value in sorted(transform.items()):
                found = _expression_mechanisms(value)
                if found:
                    animated_sites.add(f"transform.{field}")
                    mechanisms.update(found)
        if layer_type == "group":
            structure.append(
                f"group:animated={','.join(sorted(animated_sites)) or 'none'}:"
                f"boost_only={'yes' if layer.get('boost_only') else 'no'}"
            )
            children = layer.get("layers")
            if isinstance(children, list):
                for child in children:
                    visit(child)
            return
        suffix = ""
        if layer_type == "ribbon":
            suffix = f":region={layer.get('region', 'unknown')}"
        elif layer_type == "span":
            source, source_mechanisms = _source_structure(layer.get("source"), texture_kinds)
            mechanisms.update(source_mechanisms)
            suffix = (
                f":region={layer.get('region', 'unknown')}:clip={layer.get('clip', 'silhouette')}:"
                f"corner={layer.get('corner', 'fan')}:source={source}"
            )
        elif layer_type == "head_disc":
            found = _expression_mechanisms(layer.get("radius_ratio"))
            if found:
                animated_sites.add("radius_ratio")
                mechanisms.update(found)
        structure.append(
            f"{layer_type}{suffix}:animated={','.join(sorted(animated_sites)) or 'none'}:"
            f"boost_only={'yes' if layer.get('boost_only') else 'no'}"
        )

    for item in layers:
        visit(item)
    return structure, mechanisms


def fixture_structural_signature(
    plan: dict[str, Any],
    document: dict[str, Any],
    *,
    plan_content_hash: str,
    document_content_hash: str,
) -> dict[str, Any]:
    """Build a stable semantic hash for an approved production fixture.

    Exact artifact hashes are retained as provenance, but deliberately do not
    influence the signature: two builds using different colors or bytes still
    exercise the same authoring structure when their topology is identical.
    """

    path = plan.get("path")
    if path not in _IMPLEMENTATION_PATHS:
        raise ValueError("fixture implementation plan has no supported path")
    plan_assets = plan.get("asset_plan")
    if not isinstance(plan_assets, list):
        raise ValueError("fixture implementation plan has no asset_plan")
    plan_counts: Counter[str] = Counter()
    for asset in plan_assets:
        kind = asset.get("kind") if isinstance(asset, dict) else None
        if kind not in _ASSET_KINDS:
            raise ValueError("fixture implementation plan has an unsupported asset kind")
        plan_counts[str(kind)] += 1
    textures = document.get("textures", [])
    if not isinstance(textures, list):
        raise ValueError("fixture skin document textures are malformed")
    texture_kinds: dict[str, str] = {}
    document_counts: Counter[str] = Counter()
    for texture in textures:
        if not isinstance(texture, dict) or texture.get("kind") not in _ASSET_KINDS:
            raise ValueError("fixture skin document has an unsupported texture kind")
        kind = str(texture["kind"])
        texture_kinds[str(texture.get("name", ""))] = kind
        document_counts[kind] += 1
    counts = Counter(
        {
            kind: max(plan_counts[kind], document_counts[kind])
            for kind in sorted(set(plan_counts).union(document_counts))
        }
    )
    asset_kinds = sorted(kind for kind, count in counts.items() for _ in range(count))
    effect_structure, mechanisms = _layer_structure(document.get("layers", []), texture_kinds)
    if "sheet" in counts:
        mechanisms.add("sprite_sheet")
    if not mechanisms:
        mechanisms.add("static")
    structural_payload = {
        "contract_version": STRUCTURAL_SIGNATURE_VERSION,
        "implementation_path": path,
        "asset_kinds": asset_kinds,
        "animation_mechanisms": sorted(mechanisms),
        "effect_structure": effect_structure,
    }
    signature = "sha256:" + hashlib.sha256(canonical_json(structural_payload).encode()).hexdigest()
    return {
        **structural_payload,
        "structural_signature": signature,
        "source": {
            "implementation_plan_hash": plan_content_hash,
            "skin_document_hash": document_content_hash,
        },
    }


def _validated_structural_signature(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("fixture structural signature is missing")
    required = {
        "contract_version",
        "implementation_path",
        "asset_kinds",
        "animation_mechanisms",
        "effect_structure",
        "structural_signature",
        "source",
    }
    if set(value) != required or value.get("contract_version") != STRUCTURAL_SIGNATURE_VERSION:
        raise ValueError("fixture structural signature contract is malformed or drifted")
    if value.get("implementation_path") not in _IMPLEMENTATION_PATHS:
        raise ValueError("fixture structural signature has an unsupported implementation path")
    assets = value.get("asset_kinds")
    mechanisms = value.get("animation_mechanisms")
    effects = value.get("effect_structure")
    if (
        not isinstance(assets, list)
        or not all(isinstance(item, str) for item in assets)
        or assets != sorted(assets)
        or any(item not in _ASSET_KINDS for item in assets)
        or not isinstance(mechanisms, list)
        or not mechanisms
        or not all(isinstance(item, str) and item for item in mechanisms)
        or mechanisms != sorted(set(mechanisms))
        or not isinstance(effects, list)
        or not all(isinstance(item, str) and item for item in effects)
    ):
        raise ValueError("fixture structural signature topology is malformed")
    structural_payload = {name: value[name] for name in required if name not in {"structural_signature", "source"}}
    expected = "sha256:" + hashlib.sha256(canonical_json(structural_payload).encode()).hexdigest()
    if value.get("structural_signature") != expected:
        raise ValueError("fixture structural signature hash is invalid")
    source = value.get("source")
    if (
        not isinstance(source, dict)
        or set(source) != {"implementation_plan_hash", "skin_document_hash"}
        or not all(isinstance(item, str) and item for item in source.values())
    ):
        raise ValueError("fixture structural signature provenance is missing")
    return json.loads(canonical_json(value))


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
        selected, diversity = self._select_structurally_varied_fixtures(list(distinct.values()))
        if not selected:
            return {
                "state": "not_ready",
                **diversity,
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
                fixture_refs=[self._prototype_hash(row) for row in selected],
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
            "schema_version": 3,
            "proposal": proposal.model_dump(mode="json"),
            "fixture_contracts": fixture_contracts,
            "fixture_structures": {row["concept_id"]: row["fixture_structure"] for row in selected},
        }
        recipe = self.factory.objects.put(canonical_json(recipe_payload).encode())
        candidate = self.database.create_technique_candidate(
            source_artifact_id=source["id"],
            recipe_ref=recipe.uri,
            recipe_sha=recipe.sha256,
            fixture_refs=[self._prototype_hash(row) for row in selected],
        )
        self._schedule(candidate, proposal, selected, fixture_contracts)
        candidate = self.database.update_technique_candidate(
            candidate["id"],
            candidate["version"],
            disposition="trials_running",
            trial_results={
                "operation_id": operation["id"],
                "title": proposal.title,
                "fixture_structures": {row["concept_id"]: row["fixture_structure"] for row in selected},
                "minimum_structural_signatures": (self.config.optimizer.technique_min_structural_signatures),
            },
        )
        return {"state": candidate["disposition"], "candidate_id": candidate["id"]}

    @staticmethod
    def _prototype_hash(fixture: dict[str, Any]) -> str:
        value = fixture.get("prototype_hash") or fixture.get("approved_prototype_hash")
        if not isinstance(value, str) or not value:
            raise ValueError("technique fixture has no approved prototype hash")
        return value

    def _fixture_structure(self, fixture: dict[str, Any]) -> dict[str, Any] | None:
        try:
            attempt = self.database.get_attempt(fixture["attempt_id"])
            plan_artifact = self.factory._find_lineage_artifact(
                attempt,
                ArtifactKind.IMPLEMENTATION_PLAN,
            )
            document_artifact = self.factory._find_lineage_artifact(
                attempt,
                ArtifactKind.SKIN_DOCUMENT,
            )
            if plan_artifact is None or document_artifact is None:
                return None
            plan = self.factory.persistence.load_json(plan_artifact["object_ref"])
            document = self.factory.persistence.load_json(document_artifact["object_ref"])
            if not isinstance(plan, dict) or not isinstance(document, dict):
                return None
            return fixture_structural_signature(
                plan,
                document,
                plan_content_hash=plan_artifact["content_hash"],
                document_content_hash=document_artifact["content_hash"],
            )
        except (AttributeError, KeyError, TypeError, ValueError):
            return None

    def _select_structurally_varied_fixtures(
        self,
        fixtures: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        evidenced: list[dict[str, Any]] = []
        missing_structure = 0
        for fixture in fixtures:
            structure = fixture.get("fixture_structure") or self._fixture_structure(fixture)
            if structure is None:
                missing_structure += 1
                continue
            try:
                structure = _validated_structural_signature(structure)
            except ValueError:
                missing_structure += 1
                continue
            evidenced.append({**fixture, "fixture_structure": structure})
        by_signature: dict[str, list[dict[str, Any]]] = {}
        for fixture in evidenced:
            by_signature.setdefault(fixture["fixture_structure"]["structural_signature"], []).append(fixture)
        minimum_fixtures = self.config.optimizer.technique_min_fixtures
        minimum_signatures = self.config.optimizer.technique_min_structural_signatures
        report = {
            "required_fixtures": minimum_fixtures,
            "available_fixtures_with_structure": len(evidenced),
            "required_structural_signatures": minimum_signatures,
            "available_structural_signatures": len(by_signature),
            "fixtures_missing_structure": missing_structure,
        }
        if len(evidenced) < minimum_fixtures:
            return [], {
                **report,
                "reason": "not enough structurally evidenced human-approved fixtures",
            }
        if len(by_signature) < minimum_signatures:
            return [], {
                **report,
                "reason": "not enough distinct fixture structures",
            }
        # First take one representative per required topology, then fill the
        # paired sample in original fixture order. This prevents an early run
        # of near-identical concepts from crowding out the varied corpus.
        selected = [group[0] for group in list(by_signature.values())[:minimum_signatures]]
        selected_ids = {item["concept_id"] for item in selected}
        required_count = max(minimum_fixtures, minimum_signatures)
        for fixture in evidenced:
            if len(selected) >= required_count:
                break
            if fixture["concept_id"] not in selected_ids:
                selected.append(fixture)
                selected_ids.add(fixture["concept_id"])
        return selected, {
            **report,
            "selected_fixtures": len(selected),
            "selected_structural_signatures": len(
                {item["fixture_structure"]["structural_signature"] for item in selected}
            ),
        }

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
                    "prototype_hash": self._prototype_hash(item),
                },
                additional_properties=proposal.expected_properties,
            )
            for item in fixtures
        }
        fixture_structures: dict[str, dict[str, Any]] = {}
        for item in fixtures:
            structure = item.get("fixture_structure") or self._fixture_structure(item)
            fixture_structures[item["concept_id"]] = _validated_structural_signature(structure)
        distinct_structures = {item["structural_signature"] for item in fixture_structures.values()}
        if len(fixtures) < self.config.optimizer.technique_min_fixtures:
            raise ValueError("technique scheduling requires the configured fixture minimum")
        if len(distinct_structures) < self.config.optimizer.technique_min_structural_signatures:
            raise ValueError("technique scheduling requires structurally varied fixtures")
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
                "prototype_hash": self._prototype_hash(item),
                "prototype_decision_id": item["prototype_decision_id"],
                "split": "technique",
                "expected_property_contract": fixture_contracts[item["concept_id"]],
                "fixture_structure": fixture_structures[item["concept_id"]],
            }
            for item in fixtures
        ]
        self.optimizer._schedule_trials(
            {"id": candidate["id"]},
            [baseline, technique],
            rows,
            purpose=Purpose.TECHNIQUE,
        )

    def _trial_structure_evidence(
        self,
        candidate: dict[str, Any],
        attempts: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], list[str]]:
        """Revalidate immutable recipe evidence against every paired trial."""

        reasons: list[str] = []
        expected: dict[str, dict[str, Any]] = {}
        try:
            recipe = self.factory.persistence.load_json(candidate["recipe_ref"])
            raw_expected = recipe["fixture_structures"]
            if not isinstance(raw_expected, dict):
                raise ValueError("recipe fixture_structures is not an object")
            expected = {
                str(concept_id): _validated_structural_signature(signature)
                for concept_id, signature in raw_expected.items()
            }
        except (AttributeError, KeyError, TypeError, ValueError):
            reasons.append("candidate recipe has no valid frozen fixture structures")

        checkpoint: dict[str, Any] = {}
        try:
            raw_checkpoint = candidate["trial_results_json"]
            parsed_checkpoint = json.loads(raw_checkpoint) if isinstance(raw_checkpoint, str) else raw_checkpoint
            raw_structures = parsed_checkpoint["fixture_structures"]
            if not isinstance(raw_structures, dict):
                raise ValueError("candidate checkpoint fixture_structures is not an object")
            checkpoint = {
                str(concept_id): _validated_structural_signature(signature)
                for concept_id, signature in raw_structures.items()
            }
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            reasons.append("candidate trial checkpoint has no valid fixture structures")
        if expected and checkpoint != expected:
            reasons.append("candidate trial checkpoint fixture structures drifted from the immutable recipe")

        observed_by_attempt: dict[str, dict[str, Any] | None] = {}
        observed_by_concept: dict[str, list[dict[str, Any]]] = {}
        candidates_by_concept: dict[str, set[str]] = {}
        for attempt in attempts:
            if attempt.get("experiment_split") != "technique":
                continue
            concept_id = str(attempt.get("concept_id", ""))
            candidate_id = str(attempt.get("experiment_candidate", ""))
            candidates_by_concept.setdefault(concept_id, set()).add(candidate_id)
            try:
                behavior = json.loads(attempt["behavior_json"])
                signature = _validated_structural_signature(behavior["fixture_structural_signature"])
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                signature = None
                reasons.append(f"trial {attempt.get('id')} has no valid fixture structural signature")
            observed_by_attempt[str(attempt.get("id"))] = signature
            if signature is not None:
                observed_by_concept.setdefault(concept_id, []).append(signature)

        if expected and set(observed_by_concept) != set(expected):
            reasons.append("trial fixture set differs from the immutable recipe")
        for concept_id, signature in expected.items():
            observed = observed_by_concept.get(concept_id, [])
            if not observed or any(item != signature for item in observed):
                reasons.append(f"fixture {concept_id} structural signature drifted")
            if not {"baseline", "technique"}.issubset(candidates_by_concept.get(concept_id, set())):
                reasons.append(f"fixture {concept_id} is missing a paired structural trial")

        distinct_signatures = sorted({item["structural_signature"] for item in expected.values()})
        minimum_signatures = self.config.optimizer.technique_min_structural_signatures
        if len(distinct_signatures) < minimum_signatures:
            reasons.append("fixture structural diversity is below the configured promotion minimum")
        if len(expected) < self.config.optimizer.technique_min_fixtures:
            reasons.append("fixture sample is below the configured technique minimum")
        # Deduplicate without hiding which independent invariant failed.
        reasons = list(dict.fromkeys(reasons))
        evidence = {
            "contract_version": STRUCTURAL_SIGNATURE_VERSION,
            "required_fixtures": self.config.optimizer.technique_min_fixtures,
            "required_distinct_signatures": minimum_signatures,
            "observed_distinct_signatures": distinct_signatures,
            "immutable_recipe_structures": expected,
            "checkpoint_structures": checkpoint,
            "trial_structures": observed_by_attempt,
            "reasons": reasons,
        }
        return evidence, reasons

    async def _advance(self, candidate: dict[str, Any]) -> dict[str, Any]:
        attempts = self.database.experiment_attempts(candidate["id"])
        active = [row for row in attempts if row["disposition"] == Disposition.ACTIVE]
        if active:
            return {
                "state": candidate["disposition"],
                "candidate_id": candidate["id"],
                "remaining": len(active),
            }
        structure_evidence, structure_reasons = self._trial_structure_evidence(candidate, attempts)
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
                "fixture_structure_evidence": structure_evidence,
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
                "fixture_structure_evidence": structure_evidence,
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
                "fixture_structure_evidence": structure_evidence,
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
            and not structure_reasons
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
            "fixture_structure_reasons": structure_reasons,
            "fixture_structure_evidence": structure_evidence,
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

        async def promote_candidate() -> Any:
            promoted = await asyncio.to_thread(
                GitPromoter(self.config, self.database).promote_playbook,
                candidate_playbook=payload["playbook"],
                run_id=candidate["id"],
                expected_head=active_behavior["sha"],
                expected_active_sha=active_behavior["sha"],
            )
            return _promotion_provider_result(promoted)

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
            invoke=promote_candidate,
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
