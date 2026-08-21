from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import UTC, datetime
from typing import Any

import pytest

from snaketron_factory.calibration import JudgeCalibrationService, judge_evaluator_version
from snaketron_factory.db import Database, canonical_json
from snaketron_factory.domain import (
    ArtifactKind,
    Disposition,
    GateResult,
    GateVerdict,
    ImplementationPlan,
    ProviderResult,
    Purpose,
    Stage,
    WorkerResult,
)
from snaketron_factory.factory import BehaviorDrift, Factory
from snaketron_factory.objects import ObjectStore
from snaketron_factory.renderer import BrowserEvidence, BrowserRenderer
from snaketron_factory.worker import FakeWorker, SkillBundle


def _attempt_values(concept_id: str, key: str, purpose: Purpose) -> dict[str, Any]:
    return {
        "concept_id": concept_id,
        "purpose": purpose,
        "stage": Stage.CONCEPT,
        "idempotency_key": key,
        "behavior": {},
        "direction_sha": "1" * 64,
        "skill_sha": "2" * 64,
        "capability_sha": "3" * 64,
        "gate_sha": "4" * 64,
        "model_config_sha": "5" * 64,
    }


def test_only_current_production_attempt_can_change_canonical_concept(database: Database) -> None:
    concept = database.create_concept(
        name="Canonical",
        brief="The production identity must not be overwritten by experimental state.",
        seed="canonical",
        source="test",
        tags=[],
    )
    first = database.create_attempt(**_attempt_values(concept["id"], "production-first", Purpose.PRODUCTION))
    current = database.create_attempt(**_attempt_values(concept["id"], "production-current", Purpose.PRODUCTION))
    experiment = database.create_attempt(**_attempt_values(concept["id"], "experiment", Purpose.OPTIMIZER))

    database.update_attempt(first["id"], first["version"], disposition=Disposition.MACHINE_REJECTED)
    database.update_attempt(
        experiment["id"],
        experiment["version"],
        disposition=Disposition.EXPERIMENT_COMPLETE,
    )
    concept_after_trials = database.get_concept(concept["id"])
    assert concept_after_trials["current_attempt_id"] == current["id"]
    assert concept_after_trials["current_disposition"] == Disposition.ACTIVE

    experiment = database.get_attempt(experiment["id"])
    database.update_attempt_with_outbox(
        experiment["id"],
        experiment["version"],
        outbox_idempotency_key="experiment-review",
        outbox_destination="review_gallery",
        outbox_event_ref=experiment["id"],
        outbox_payload_ref="sha256:" + "a" * 64,
        outbox_payload_hash="a" * 64,
        disposition=Disposition.NEEDS_HUMAN,
        review_kind="re_evaluation",
    )
    assert database.get_concept(concept["id"])["current_disposition"] == Disposition.ACTIVE

    database.update_attempt(current["id"], current["version"], disposition=Disposition.PUBLISHED)
    assert database.get_concept(concept["id"])["current_disposition"] == Disposition.PUBLISHED


def test_latest_registered_attempt_excludes_trials_and_wrong_stable_id(database: Database) -> None:
    concept = database.create_concept(
        name="Stable",
        brief="Only production revisions of the canonical skin may be appended.",
        seed="stable",
        source="test",
        tags=[],
    )
    production = database.create_attempt(**_attempt_values(concept["id"], "registered-production", Purpose.PRODUCTION))
    production = database.update_attempt(
        production["id"],
        production["version"],
        production_skin_id="skin-canonical",
        production_revision="2",
        production_content_hash="sha256:" + "b" * 64,
    )
    concept = database.get_concept(concept["id"])
    database.update_concept(concept["id"], concept["version"], stable_skin_id="skin-canonical")

    trial = database.create_attempt(**_attempt_values(concept["id"], "registered-trial", Purpose.TECHNIQUE))
    database.update_attempt(
        trial["id"],
        trial["version"],
        production_skin_id="skin-canonical",
        production_revision="99",
        production_content_hash="sha256:" + "c" * 64,
    )
    wrong = database.create_attempt(**_attempt_values(concept["id"], "registered-wrong-id", Purpose.PRODUCTION))
    database.update_attempt(
        wrong["id"],
        wrong["version"],
        production_skin_id="skin-other",
        production_revision="100",
        production_content_hash="sha256:" + "d" * 64,
    )

    assert database.latest_registered_attempt(concept["id"])["id"] == production["id"]


def _blind_label(
    database: Database,
    objects: ObjectStore,
    attempt: dict[str, Any],
    *,
    index: int,
    evaluator_version: str,
) -> None:
    stored = objects.put(f"pixels-{index}".encode())
    artifact = database.add_artifact(
        attempt_id=attempt["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        content_hash=stored.uri,
        object_ref=stored.uri,
        media_type="image/png",
        size_bytes=stored.size,
    )
    database.add_evaluation(
        artifact_id=artifact["id"],
        attempt_id=attempt["id"],
        evaluator="visual_judge",
        result=GateResult(
            gate="visual_fidelity",
            gate_version=evaluator_version,
            blocking=False,
            verdict=GateVerdict.CANDIDATE,
        ),
        hidden_until_label=True,
    )
    database.add_blind_human_label(
        artifact_id=artifact["id"],
        attempt_id=attempt["id"],
        action="prototype_label",
        feedback="blind calibration label",
        tags=["outcome:accept"],
        actor="human:calibrator",
        content_hash=artifact["content_hash"],
    )


def test_calibration_accumulates_each_resolved_model_and_rubric_identity(
    factory_config, database: Database, objects: ObjectStore
) -> None:
    concept = database.create_concept(
        name="Calibration versions",
        brief="Dated provider versions need independent calibration histories.",
        seed="versions",
        source="test",
        tags=[],
    )
    attempt = database.create_attempt(**_attempt_values(concept["id"], "calibration-versions", Purpose.PRODUCTION))
    attempt = database.update_attempt(
        attempt["id"],
        attempt["version"],
        stage=Stage.PROTOTYPE_REVIEW,
        disposition=Disposition.NEEDS_HUMAN,
        review_kind="prototype",
    )
    old = judge_evaluator_version(factory_config, "prototype", resolved_model="gemini-3.7-flash-20260801")
    new = judge_evaluator_version(factory_config, "prototype", resolved_model="gemini-3.7-flash-20260815")
    _blind_label(database, objects, attempt, index=1, evaluator_version=old)
    _blind_label(database, objects, attempt, index=2, evaluator_version=new)

    report = JudgeCalibrationService(database, factory_config).refresh_all(at=datetime.now(UTC))

    assert report["prototype"]["observed_evaluator_versions"] == [old, new]
    assert database.judge_calibration("prototype", old)["sample_size"] == 1
    assert database.judge_calibration("prototype", new)["sample_size"] == 1
    assert len(report["prototype"]["calibrations"]) == 3


def _author_result() -> WorkerResult:
    return WorkerResult(
        implementation_plan=ImplementationPlan(
            path="layers",
            rationale="The retained candidate authors a procedural skin.",
            fidelity_features=["clear silhouette"],
            layer_plan=["base"],
            asset_plan=[],
            animation_plan=["time-based highlight"],
            required_wrap_axes=[],
            risks=[],
        ),
        skin_document={
            "schema_version": 2,
            "name": "Retained Candidate",
            "period_ms": 1000,
            "textures": [],
            "layers": [],
        },
    )


@pytest.mark.asyncio
async def test_experiment_author_uses_retained_candidate_before_git_and_resumes_same_request(
    factory_config, database: Database, objects: ObjectStore
) -> None:
    worker = FakeWorker([_author_result()], model="worker-test")
    factory = Factory(
        factory_config,
        database=database,
        objects=objects,
        worker=worker,
    )
    concept = database.create_concept(
        name="Candidate",
        brief="A retained optimizer candidate must survive active Git movement.",
        seed="candidate",
        source="test",
        tags=[],
    )
    direction_sha = hashlib.sha256(factory_config.paths.direction.read_bytes()).hexdigest()
    capability_sha = hashlib.sha256(factory_config.paths.capability_manifest.read_bytes()).hexdigest()
    gate_sha = hashlib.sha256(factory_config.paths.gate_manifest.read_bytes()).hexdigest()
    parent = database.create_attempt(
        concept_id=concept["id"],
        purpose=Purpose.PRODUCTION,
        stage=Stage.PROTOTYPE_REVIEW,
        idempotency_key="candidate-parent",
        behavior={},
        direction_sha=direction_sha,
        skill_sha="parent-skill",
        capability_sha=capability_sha,
        gate_sha=gate_sha,
        model_config_sha="legacy",
    )
    prototype_bytes = objects.put(b"approved prototype pixels")
    prototype = database.add_artifact(
        attempt_id=parent["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        content_hash=prototype_bytes.uri,
        object_ref=prototype_bytes.uri,
        media_type="image/png",
        size_bytes=prototype_bytes.size,
    )
    manifest = objects.put(
        canonical_json(
            {
                "brief": concept["brief"],
                "implementation_hint": "layers",
                "image_sha256": prototype["content_hash"],
            }
        ).encode()
    )
    database.add_artifact(
        attempt_id=parent["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE_MANIFEST,
        content_hash=manifest.uri,
        object_ref=manifest.uri,
        media_type="application/json",
        size_bytes=manifest.size,
        metadata={"image_artifact_id": prototype["id"]},
    )
    approval = database.add_human_decision(
        artifact_id=prototype["id"],
        attempt_id=parent["id"],
        action="prototype_approval",
        feedback="exact approved pixels",
        tags=[],
        actor="human:reviewer",
        attempt_version=parent["version"],
        content_hash=prototype["content_hash"],
    )
    files = {
        "SKILL.md": "# Candidate authoring contract\n",
        "schemas/asset-request.schema.json": json.dumps({"type": "object"}),
    }
    candidate_bundle = SkillBundle.from_files(files)
    child = database.create_attempt(
        concept_id=concept["id"],
        purpose=Purpose.OPTIMIZER,
        parent_attempt_id=parent["id"],
        stage=Stage.AUTHOR,
        idempotency_key="candidate-child",
        behavior={},
        direction_sha=direction_sha,
        skill_sha=candidate_bundle.sha256,
        capability_sha=capability_sha,
        gate_sha=gate_sha,
        model_config_sha="legacy",
        approved_prototype_hash=prototype["content_hash"],
        prototype_decision_id=approval["id"],
    )
    factory._store_json_artifact(
        child,
        Stage.AUTHOR,
        ArtifactKind.OPTIMIZER_CANDIDATE,
        {
            "skill_sha256": candidate_bundle.sha256,
            "skill_files": files,
        },
    )

    def forbidden_git_lookup(_attempt: dict[str, Any]) -> SkillBundle:
        raise AssertionError("experimental authoring consulted the active Git bundle")

    factory.pinned_skill_bundle = forbidden_git_lookup  # type: ignore[method-assign]
    first = await factory._author(child)
    assert first["stage"] == Stage.BUILD_GATE
    assert len(worker.requests) == 1
    first_request_id = worker.requests[0].request_id

    replay = database.update_attempt(first["id"], first["version"], stage=Stage.AUTHOR)
    resumed = await factory._author(replay)
    assert resumed["stage"] == Stage.BUILD_GATE
    assert len(worker.requests) == 1
    assert first_request_id.startswith("worker_") and len(first_request_id) == 71
    await factory.close()


class _PinnedRenderer:
    def __init__(self) -> None:
        self.sha = "renderer-a"
        self.capture_calls = 0

    def renderer_sha(self) -> str:
        return self.sha

    def capture(self, content_ref: str) -> BrowserEvidence:
        self.capture_calls += 1
        return BrowserEvidence(
            contact_sheet=b"sheet",
            animation=b"animation",
            manifest={"content_ref": content_ref},
            gate_result=GateResult(
                gate="browser_pixels_ready",
                gate_version="test-v1",
                blocking=True,
                verdict=GateVerdict.PASS,
            ),
            renderer_sha=self.sha,
        )


def _snapshot_factory(factory_config, database, objects, *, renderer=None) -> Factory:
    factory = Factory(
        factory_config,
        database=database,
        objects=objects,
        renderer=renderer,
    )
    bundle = SkillBundle.load(factory_config.paths.skill_dir)
    factory.active_skill_bundle = lambda: (bundle, "refs/tags/test", "git-sha")  # type: ignore[method-assign]
    return factory


@pytest.mark.asyncio
async def test_snapshots_survive_repo_file_changes_and_model_drift_fails_before_call(
    factory_config, database: Database, objects: ObjectStore
) -> None:
    factory = _snapshot_factory(factory_config, database, objects, renderer=_PinnedRenderer())
    snapshot = factory.behavior_snapshot()
    concept = database.create_concept(
        name="Pinned inputs",
        brief="Immutable execution inputs are loaded from retained object bytes.",
        seed="pinned",
        source="test",
        tags=[],
    )
    attempt = database.create_attempt(
        concept_id=concept["id"],
        purpose=Purpose.PRODUCTION,
        stage=Stage.CONCEPT,
        idempotency_key="pinned-inputs",
        behavior=snapshot,
        direction_sha=snapshot["direction_sha"],
        skill_sha=snapshot["skill_sha"],
        capability_sha=snapshot["capability_sha"],
        gate_sha=snapshot["gate_sha"],
        model_config_sha=snapshot["model_config_sha"],
    )
    original_direction = factory.pinned_direction(attempt)
    original_capabilities = factory.pinned_gates(attempt).capabilities
    factory_config.paths.direction.write_text("changed direction\n", encoding="utf-8")
    factory_config.paths.capability_manifest.write_text("{}", encoding="utf-8")
    factory_config.paths.gate_manifest.write_text("version: 99\ngates: []\n", encoding="utf-8")
    assert factory.pinned_direction(attempt) == original_direction
    assert factory.pinned_gates(attempt).capabilities == original_capabilities

    factory_config.models.task_worker.model = "worker-moved"
    invoked = False

    async def invoke() -> ProviderResult:
        nonlocal invoked
        invoked = True
        return ProviderResult(value={}, resolved_model="worker-moved")

    with pytest.raises(BehaviorDrift, match="configuration changed"):
        await factory._provider_call(
            attempt=attempt,
            stage=Stage.CONCEPT,
            key="must-not-run",
            role="smart_text",
            side_effect="generate_concept",
            request={},
            invoke=invoke,
        )
    assert not invoked
    with database.connect() as connection:
        assert (
            connection.execute("SELECT count(*) FROM operation WHERE attempt_id=?", (attempt["id"],)).fetchone()[0] == 0
        )
    await factory.close()


@pytest.mark.asyncio
async def test_repository_tree_drift_blocks_pre_render_execution(
    factory_config, database: Database, objects: ObjectStore
) -> None:
    renderer = _PinnedRenderer()
    factory = _snapshot_factory(
        factory_config,
        database,
        objects,
        renderer=renderer,
    )
    snapshot = factory.behavior_snapshot()
    concept = database.create_concept(
        name="Renderer pin",
        brief="Browser evidence must come from the snapshotted renderer tree.",
        seed="renderer",
        source="test",
        tags=[],
    )
    attempt = database.create_attempt(
        concept_id=concept["id"],
        purpose=Purpose.PRODUCTION,
        stage=Stage.BUILD_GATE,
        idempotency_key="renderer-pin",
        behavior=snapshot,
        direction_sha=snapshot["direction_sha"],
        skill_sha=snapshot["skill_sha"],
        capability_sha=snapshot["capability_sha"],
        gate_sha=snapshot["gate_sha"],
        model_config_sha=snapshot["model_config_sha"],
    )
    renderer.sha = "renderer-b"

    blocked = await factory._advance(attempt)

    assert blocked["disposition"] == Disposition.BLOCKED
    assert renderer.capture_calls == 0
    await factory.close()


@pytest.mark.asyncio
async def test_uncommitted_renderer_content_drift_fails_closed_before_capture(
    factory_config, database: Database, objects: ObjectStore, monkeypatch
) -> None:
    renderer_file = factory_config.paths.repo_root / "renderer-source.js"
    renderer_file.write_text("export const renderer = 'v1';\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q"], cwd=factory_config.paths.repo_root, check=True)
    subprocess.run(["git", "add", "."], cwd=factory_config.paths.repo_root, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Skin Factory Test",
            "-c",
            "user.email=skin-factory@example.invalid",
            "commit",
            "-qm",
            "renderer fixture",
        ],
        cwd=factory_config.paths.repo_root,
        check=True,
    )
    renderer = BrowserRenderer(factory_config)
    factory = _snapshot_factory(factory_config, database, objects, renderer=renderer)
    snapshot = factory.behavior_snapshot()
    concept = database.create_concept(
        name="Dirty renderer pin",
        brief="Uncommitted renderer bytes must invalidate browser evidence authority.",
        seed="dirty-renderer",
        source="test",
        tags=[],
    )
    attempt = database.create_attempt(
        concept_id=concept["id"],
        purpose=Purpose.PRODUCTION,
        stage=Stage.RENDER,
        idempotency_key="dirty-renderer-pin",
        behavior=snapshot,
        direction_sha=snapshot["direction_sha"],
        skill_sha=snapshot["skill_sha"],
        capability_sha=snapshot["capability_sha"],
        gate_sha=snapshot["gate_sha"],
        model_config_sha=snapshot["model_config_sha"],
    )
    attempt = database.update_attempt(
        attempt["id"],
        attempt["version"],
        production_skin_id="skin-evaluation",
        production_revision="1",
        production_content_hash="sha256:" + "8" * 64,
    )
    capture_calls = 0

    def capture(*_args, **_kwargs):
        nonlocal capture_calls
        capture_calls += 1
        raise AssertionError("dirty renderer must be rejected before capture")

    monkeypatch.setattr(renderer, "capture", capture)
    renderer_file.write_text("export const renderer = 'uncommitted-v2';\n", encoding="utf-8")

    blocked = await factory._advance(attempt)

    assert blocked["disposition"] == Disposition.BLOCKED
    assert "renderer tree changed" in json.loads(blocked["failure_json"])["reason"]
    assert capture_calls == 0
    await factory.close()


@pytest.mark.asyncio
async def test_ignored_cached_browser_bundle_drift_fails_closed_before_capture(
    factory_config, database: Database, objects: ObjectStore, monkeypatch
) -> None:
    repo = factory_config.paths.repo_root
    (repo / ".gitignore").write_text("client/web/dist/\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(
        [
            "git",
            "-c",
            "user.name=Skin Factory Test",
            "-c",
            "user.email=skin-factory@example.invalid",
            "commit",
            "-qm",
            "renderer bundle fixture",
        ],
        cwd=repo,
        check=True,
    )
    renderer = BrowserRenderer(factory_config)
    factory = _snapshot_factory(factory_config, database, objects, renderer=renderer)
    snapshot = factory.behavior_snapshot()
    pinned_worktree_sha = renderer.renderer_sha()
    concept = database.create_concept(
        name="Cached renderer bundle pin",
        brief="Ignored built assets must remain bound to the exact attempt snapshot.",
        seed="cached-renderer-bundle",
        source="test",
        tags=[],
    )
    attempt = database.create_attempt(
        concept_id=concept["id"],
        purpose=Purpose.PRODUCTION,
        stage=Stage.RENDER,
        idempotency_key="cached-renderer-bundle-pin",
        behavior=snapshot,
        direction_sha=snapshot["direction_sha"],
        skill_sha=snapshot["skill_sha"],
        capability_sha=snapshot["capability_sha"],
        gate_sha=snapshot["gate_sha"],
        model_config_sha=snapshot["model_config_sha"],
    )
    attempt = database.update_attempt(
        attempt["id"],
        attempt["version"],
        production_skin_id="skin-evaluation",
        production_revision="1",
        production_content_hash="sha256:" + "7" * 64,
    )
    capture_calls = 0

    def capture(*_args, **_kwargs):
        nonlocal capture_calls
        capture_calls += 1
        raise AssertionError("mutated cached renderer bundle must be rejected before capture")

    monkeypatch.setattr(renderer, "capture", capture)
    (repo / "client" / "web" / "dist" / "main.js").write_text(
        "export const renderer = 'ignored-but-mutated';\n",
        encoding="utf-8",
    )
    assert renderer.renderer_sha() == pinned_worktree_sha

    blocked = await factory._advance(attempt)

    assert blocked["disposition"] == Disposition.BLOCKED
    assert "renderer config SHA, endpoints, capture command" in json.loads(blocked["failure_json"])["reason"]
    assert capture_calls == 0
    await factory.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    ["config_sha", "browser_endpoint", "service_endpoint", "capture_command"],
)
async def test_renderer_execution_config_drift_fails_closed_before_capture(
    factory_config, database: Database, objects: ObjectStore, mutation: str
) -> None:
    renderer = _PinnedRenderer()
    factory = _snapshot_factory(factory_config, database, objects, renderer=renderer)
    snapshot = factory.behavior_snapshot()
    concept = database.create_concept(
        name=f"Renderer config pin {mutation}",
        brief="The browser capture boundary must use the exact snapshotted configuration.",
        seed=mutation,
        source="test",
        tags=[],
    )
    attempt = database.create_attempt(
        concept_id=concept["id"],
        purpose=Purpose.PRODUCTION,
        stage=Stage.RENDER,
        idempotency_key=f"renderer-config-{mutation}",
        behavior=snapshot,
        direction_sha=snapshot["direction_sha"],
        skill_sha=snapshot["skill_sha"],
        capability_sha=snapshot["capability_sha"],
        gate_sha=snapshot["gate_sha"],
        model_config_sha=snapshot["model_config_sha"],
    )
    attempt = database.update_attempt(
        attempt["id"],
        attempt["version"],
        production_skin_id="skin-evaluation",
        production_revision="1",
        production_content_hash="sha256:" + "9" * 64,
    )
    if mutation == "config_sha":
        factory_config.version_sha256 = "0" * 64
    elif mutation == "browser_endpoint":
        factory_config.browser.base_url = "https://mutated-client.test"
    elif mutation == "service_endpoint":
        factory_config.service.base_url = "https://mutated-service.test"
    else:
        factory_config.browser.capture_command = ["false", "mutated-capture"]

    blocked = await factory._advance(attempt)

    assert blocked["disposition"] == Disposition.BLOCKED
    assert "renderer config SHA, endpoints, capture command" in json.loads(blocked["failure_json"])["reason"]
    assert renderer.capture_calls == 0
    await factory.close()


@pytest.mark.asyncio
async def test_renderer_source_config_file_drift_fails_closed_before_capture(
    factory_config, database: Database, objects: ObjectStore
) -> None:
    assert factory_config.source_path is not None
    factory_config.source_path.write_text("browser: original\n", encoding="utf-8")
    renderer = _PinnedRenderer()
    factory = _snapshot_factory(factory_config, database, objects, renderer=renderer)
    snapshot = factory.behavior_snapshot()
    concept = database.create_concept(
        name="Renderer source config pin",
        brief="The loaded factory file checksum must remain exact until capture.",
        seed="source-config",
        source="test",
        tags=[],
    )
    attempt = database.create_attempt(
        concept_id=concept["id"],
        purpose=Purpose.PRODUCTION,
        stage=Stage.RENDER,
        idempotency_key="renderer-source-config",
        behavior=snapshot,
        direction_sha=snapshot["direction_sha"],
        skill_sha=snapshot["skill_sha"],
        capability_sha=snapshot["capability_sha"],
        gate_sha=snapshot["gate_sha"],
        model_config_sha=snapshot["model_config_sha"],
    )
    attempt = database.update_attempt(
        attempt["id"],
        attempt["version"],
        production_skin_id="skin-evaluation",
        production_revision="1",
        production_content_hash="sha256:" + "a" * 64,
    )
    factory_config.source_path.write_text("browser: mutated\n", encoding="utf-8")

    blocked = await factory._advance(attempt)

    assert blocked["disposition"] == Disposition.BLOCKED
    assert renderer.capture_calls == 0
    await factory.close()
