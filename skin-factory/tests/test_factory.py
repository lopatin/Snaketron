from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

import pytest
from conftest import add_artifact

from snaketron_factory.db import Database, VersionConflict
from snaketron_factory.domain import (
    ArtifactKind,
    ConceptProposal,
    Disposition,
    GateResult,
    GateVerdict,
    ImplementationPlan,
    OperationStatus,
    ProviderError,
    ProviderFailureKind,
    ProviderResult,
    Purpose,
    Stage,
    VisualJudgment,
    WorkerResult,
)
from snaketron_factory.factory import BudgetExceeded, Factory
from snaketron_factory.objects import ObjectStore
from snaketron_factory.operations import ExistingOperation, OperationJournal
from snaketron_factory.providers import FakeProvider
from snaketron_factory.renderer import (
    RENDERER_BUNDLE_MANIFEST_ENV,
    RENDERER_BUNDLE_SHA_ENV,
    BrowserEvidence,
    BrowserRenderer,
    RendererDrift,
)
from snaketron_factory.review import ReviewService
from snaketron_factory.worker import FakeWorker, SkillBundle


class FakeRegistry:
    def __init__(self, roles: dict[str, FakeProvider]) -> None:
        self.roles = roles

    def role(self, name: str) -> FakeProvider:
        return self.roles[name]

    async def close(self) -> None:
        return None


class FakeApi:
    def __init__(self) -> None:
        self.create_calls: list[dict[str, Any]] = []
        self.append_calls: list[dict[str, Any]] = []
        self.publication_request_calls: list[dict[str, Any]] = []
        self.publish_calls: list[dict[str, Any]] = []

    async def close(self) -> None:
        return None

    async def create_skin(self, **request: Any) -> ProviderResult:
        self.create_calls.append(request)
        return ProviderResult(
            value={
                "skinId": "skin-stable",
                "headRevision": 1,
                "contentRef": "sha256:" + "9" * 64,
            },
            request_id="create-1",
            resolved_model="snaketron-api",
        )

    async def append_revision(self, **request: Any) -> ProviderResult:
        self.append_calls.append(request)
        return ProviderResult(
            value={
                "skinId": request["skin_id"],
                "headRevision": request["expected_head_revision"] + 1,
                "contentRef": "sha256:" + "8" * 64,
            },
            request_id="append-1",
            resolved_model="snaketron-api",
        )

    async def request_publication_exact(self, **request: Any) -> ProviderResult:
        self.publication_request_calls.append(request)
        return ProviderResult(
            value={"accepted": True, **request},
            request_id="request-publication-1",
            resolved_model="snaketron-api",
        )

    async def publish_exact(self, **request: Any) -> ProviderResult:
        self.publish_calls.append(request)
        return ProviderResult(
            value={"publication": "published"},
            request_id="publish-1",
            resolved_model="snaketron-api",
        )


@dataclass(frozen=True)
class FakeGateManifest:
    sha256: str = "gate-test-sha"


class PassingGates:
    capability_sha256 = "capability-test-sha"
    manifest = FakeGateManifest()
    capabilities: ClassVar[dict[str, Any]] = {
        "limits": {
            "max_flattened_layers": 32,
            "max_texture_refs": 8,
        }
    }

    @staticmethod
    def validate_document(document: dict[str, Any], plan: ImplementationPlan) -> list[GateResult]:
        assert document["schema_version"] == 2
        assert plan.path == "layers"
        return [
            GateResult(
                gate="document_schema",
                gate_version="fake-v1",
                blocking=True,
                verdict=GateVerdict.PASS,
                measurements={"real_driver_boundary": True},
            )
        ]

    @staticmethod
    def blocking_failure(results: list[GateResult]) -> bool:
        return any(result.blocking and result.verdict == GateVerdict.FAIL for result in results)


@pytest.mark.asyncio
async def test_optimizer_registration_uses_server_enforced_evaluation_namespace(
    factory_config,
    database,
    objects,
    make_attempt,
) -> None:
    api = FakeApi()
    factory = Factory(factory_config, database=database, objects=objects, api=api)  # type: ignore[arg-type]
    attempt = make_attempt(stage=Stage.REGISTER, purpose=Purpose.OPTIMIZER)
    add_artifact(
        database,
        objects,
        attempt["id"],
        stage=Stage.BUILD_GATE,
        kind=ArtifactKind.SKIN_DOCUMENT,
        value=json.dumps(
            {"schema_version": 2, "name": "Isolated trial", "period_ms": 1000, "textures": [], "layers": []}
        ).encode(),
        media_type="application/json",
    )

    registered = await factory._register(attempt)

    assert registered["stage"] == Stage.RENDER
    assert api.create_calls == [
        {
            "name": f"Trial {attempt['id'][-8:]} Concept 1",
            "document": {
                "schema_version": 2,
                "name": "Isolated trial",
                "period_ms": 1000,
                "textures": [],
                "layers": [],
            },
            "idempotency_key": f"factory-trial:{attempt['id']}",
            "evaluation_only": True,
        }
    ]
    assert database.get_concept(attempt["concept_id"])["stable_skin_id"] is None
    assert database.latest_registered_attempt(attempt["concept_id"]) is None
    assert api.publication_request_calls == []
    with database.connect() as connection:
        operation = connection.execute(
            "SELECT * FROM operation WHERE attempt_id=? AND side_effect='create_private_skin_revision'",
            (attempt["id"],),
        ).fetchone()
    assert operation is not None
    assert operation["request_hash"] == OperationJournal.request_hash(api.create_calls[0])
    await factory.close()


class PassingRenderer:
    def __init__(self) -> None:
        self.refs: list[str] = []

    def capture(self, content_ref: str) -> BrowserEvidence:
        self.refs.append(content_ref)
        return BrowserEvidence(
            contact_sheet=b"real-browser-contact-sheet",
            animation=b"real-browser-animation",
            manifest={"asset_status": {"requested": 0, "failed": 0}},
            gate_result=GateResult(
                gate="browser_pixels_ready",
                gate_version="browser-v1",
                blocking=True,
                verdict=GateVerdict.PASS,
            ),
            renderer_sha="renderer-tree-sha",
        )


class FailingRenderer:
    def capture(self, content_ref: str) -> BrowserEvidence:
        return BrowserEvidence(
            contact_sheet=b"",
            animation=b"",
            manifest={
                "schema_version": 1,
                "content_ref": content_ref,
                "renderer_exit": 2,
                "stderr": "injected browser failure",
            },
            gate_result=GateResult(
                gate="browser_pixels_ready",
                gate_version="browser-v1",
                blocking=True,
                verdict=GateVerdict.FAIL,
                reasons=["injected browser failure"],
                measurements={"exit": 2},
            ),
            renderer_sha="renderer-tree-sha",
        )


def author_result() -> WorkerResult:
    return WorkerResult(
        implementation_plan=ImplementationPlan(
            path="layers",
            rationale="The approved pure pattern is easiest to animate as layers.",
            fidelity_features=["distinct head", "clean highlight"],
            layer_plan=["base", "highlight"],
            asset_plan=[],
            animation_plan=["time-based highlight"],
            required_wrap_axes=[],
            risks=["highlight contrast"],
        ),
        skin_document={
            "schema_version": 2,
            "name": "River Glass",
            "period_ms": 1200,
            "textures": [],
            "layers": [],
        },
        tool_requests=[],
        trace=[{"phase": "authored"}],
        usage={"tokens": 100},
    )


def configure_skill(factory: Factory) -> SkillBundle:
    bundle = SkillBundle.load(factory.config.paths.skill_dir)
    factory.active_skill_bundle = lambda: (bundle, "refs/tags/test-skill", "test-git-sha")  # type: ignore[method-assign]
    factory.pinned_skill_bundle = lambda _: bundle  # type: ignore[method-assign]
    return bundle


@pytest.mark.asyncio
async def test_prototype_resume_completes_missing_manifest_without_another_paid_call(
    factory_config, database, objects, monkeypatch
) -> None:
    image = FakeProvider(
        "gemini-3-pro-image",
        [{"image": b"one exact prototype", "media_type": "image/png", "text": "done"}],
    )
    factory = Factory(
        factory_config,
        database=database,
        objects=objects,
        providers=FakeRegistry({"image_generator": image}),  # type: ignore[arg-type]
    )
    configure_skill(factory)
    attempt = factory._create_seed_attempt()
    attempt = database.update_attempt(attempt["id"], attempt["version"], stage=Stage.PROTOTYPE)

    original = factory._store_json_artifact

    def crash_before_manifest(*args: Any, **kwargs: Any) -> dict[str, Any]:
        if args[2] == ArtifactKind.PROTOTYPE_MANIFEST:
            raise RuntimeError("injected crash after image artifact")
        return original(*args, **kwargs)

    monkeypatch.setattr(factory, "_store_json_artifact", crash_before_manifest)
    with pytest.raises(RuntimeError, match="injected crash"):
        await factory._prototype(attempt)
    prototypes = database.artifacts_for_attempt(attempt["id"], stage=Stage.PROTOTYPE, kind=ArtifactKind.PROTOTYPE)
    assert len(prototypes) == 1
    assert image.calls and len(image.calls) == 1
    assert not database.artifacts_for_attempt(
        attempt["id"], stage=Stage.PROTOTYPE, kind=ArtifactKind.PROTOTYPE_MANIFEST
    )

    monkeypatch.setattr(factory, "_store_json_artifact", original)
    resumed = await factory._prototype(database.get_attempt(attempt["id"]))
    assert resumed["stage"] == Stage.PROTOTYPE_TRIAGE
    assert len(image.calls) == 1
    manifests = database.artifacts_for_attempt(
        attempt["id"], stage=Stage.PROTOTYPE, kind=ArtifactKind.PROTOTYPE_MANIFEST
    )
    assert len(manifests) == 1
    assert factory.persistence.load_json(manifests[0]["object_ref"])["image_sha256"] == prototypes[0]["content_hash"]
    await factory.close()


@pytest.mark.asyncio
async def test_linked_child_prototype_re_evaluation_is_consumed_and_remains_exactly_approvable(
    factory_config, database, objects, make_attempt
) -> None:
    judge = FakeProvider(
        "gemini-3.7-flash",
        [
            VisualJudgment(
                verdict="candidate",
                reasons=["current rubric accepts the retained direction"],
                fidelity=0.8,
                readability=0.9,
                role_clarity=0.9,
                animation_quality=0.7,
                craft=0.8,
            )
        ],
    )
    factory = Factory(
        factory_config,
        database=database,
        objects=objects,
        providers=FakeRegistry({"visual_judge": judge}),  # type: ignore[arg-type]
    )
    configure_skill(factory)
    factory.gates = PassingGates()  # type: ignore[assignment]
    parent = make_attempt(stage=Stage.PROTOTYPE_REVIEW, disposition=Disposition.MACHINE_REJECTED)
    prototype = add_artifact(
        database,
        objects,
        parent["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        value=b"retained rejected pixels",
        media_type="image/png",
        metadata={"prototype_index": 0},
    )
    manifest = add_artifact(
        database,
        objects,
        parent["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE_MANIFEST,
        value=json.dumps({"image_sha256": prototype["content_hash"]}).encode(),
        media_type="application/json",
        metadata={"prototype_index": 0, "image_artifact_id": prototype["id"]},
    )
    review = ReviewService(
        database,
        factory.journal,
        factory.api,
        factory.persistence,
        factory.behavior_snapshot,
    )
    reevaluated = review.re_evaluate(
        attempt_id=parent["id"],
        artifact_id=prototype["id"],
        feedback="apply the current rubric to these exact pixels",
        actor="human:alex",
    )
    child = reevaluated["attempt"]
    linked = reevaluated["artifact"]
    assert child["restart_stage"] == f"re_evaluate:{linked['id']}"
    assert linked["attempt_id"] == child["id"]
    assert linked["content_hash"] == prototype["content_hash"]

    routed = await factory._prototype_triage(child)
    assert routed["stage"] == Stage.PROTOTYPE_REVIEW
    assert routed["disposition"] == Disposition.NEEDS_HUMAN
    evaluation = database.evaluations_for_attempt(child["id"], reveal=True)[0]
    assert evaluation["artifact_id"] == linked["id"]
    assert factory._prototype_manifest_for(routed, linked)["id"] == manifest["id"]

    approved = review.approve_prototype(
        attempt_id=child["id"],
        artifact_id=linked["id"],
        content_hash=linked["content_hash"],
        feedback="approve the re-evaluated exact artifact",
        actor="human:alex",
    )["attempt"]
    assert approved["stage"] == Stage.AUTHOR
    assert approved["approved_prototype_hash"] == prototype["content_hash"]
    await factory.close()


@pytest.mark.asyncio
async def test_build_re_evaluations_record_current_results_but_never_gain_publish_authority(
    factory_config, database, objects, make_attempt
) -> None:
    judge = FakeProvider(
        "gemini-3.7-flash",
        [
            VisualJudgment(
                verdict="candidate",
                reasons=["the retained real render now clears the rubric"],
                fidelity=0.85,
                readability=0.9,
                role_clarity=0.9,
                animation_quality=0.8,
                craft=0.85,
            )
        ],
    )
    api = FakeApi()
    factory = Factory(
        factory_config,
        database=database,
        objects=objects,
        providers=FakeRegistry({"visual_judge": judge}),  # type: ignore[arg-type]
        api=api,  # type: ignore[arg-type]
    )
    configure_skill(factory)
    factory.gates = PassingGates()  # type: ignore[assignment]
    review = ReviewService(
        database,
        factory.journal,
        api,  # type: ignore[arg-type]
        factory.persistence,
        factory.behavior_snapshot,
    )

    document_parent = make_attempt(stage=Stage.BUILD_GATE, disposition=Disposition.MACHINE_REJECTED)
    plan = author_result().implementation_plan
    add_artifact(
        database,
        objects,
        document_parent["id"],
        stage=Stage.AUTHOR,
        kind=ArtifactKind.IMPLEMENTATION_PLAN,
        value=plan.model_dump_json().encode(),
        media_type="application/json",
    )
    document = add_artifact(
        database,
        objects,
        document_parent["id"],
        stage=Stage.BUILD_GATE,
        kind=ArtifactKind.SKIN_DOCUMENT,
        value=json.dumps(author_result().skin_document).encode(),
        media_type="application/json",
    )
    document_result = review.re_evaluate(
        attempt_id=document_parent["id"],
        artifact_id=document["id"],
        feedback="rerun current hard gates only",
        actor="human:alex",
    )
    document_child = await factory._build_gate(document_result["attempt"])
    assert document_child["stage"] == Stage.COMPLETE
    assert document_child["disposition"] == Disposition.NEEDS_HUMAN
    assert document_child["review_kind"] == "re_evaluation"
    hard_results = database.evaluations_for_attempt(document_child["id"], reveal=True)
    assert len(hard_results) == 1
    assert hard_results[0]["artifact_id"] == document_result["artifact"]["id"]
    assert hard_results[0]["evaluator"] == "deterministic"
    assert judge.calls == []

    # Even if stale server identifiers are later attached by an operator, this
    # outcome is not the final-review authority required by publication.
    document_child = database.update_attempt(
        document_child["id"],
        document_child["version"],
        production_skin_id="skin-stale",
        production_revision="4",
        production_content_hash="sha256:" + "4" * 64,
    )
    with pytest.raises(VersionConflict, match="final review"):
        await review.publish(
            attempt_id=document_child["id"],
            revision="4",
            content_hash=document_child["production_content_hash"],
            feedback="must not publish a re-evaluation",
            actor="human:alex",
        )
    assert api.publish_calls == []

    visual_parent = make_attempt(stage=Stage.PROTOTYPE_REVIEW, disposition=Disposition.NEEDS_HUMAN)
    prototype = add_artifact(
        database,
        objects,
        visual_parent["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        value=b"approved prototype",
        media_type="image/png",
    )
    visual_parent = review.approve_prototype(
        attempt_id=visual_parent["id"],
        artifact_id=prototype["id"],
        content_hash=prototype["content_hash"],
        feedback="approval",
        actor="human:alex",
    )["attempt"]
    visual_parent = database.update_attempt(
        visual_parent["id"],
        visual_parent["version"],
        stage=Stage.BUILD_TRIAGE,
        disposition=Disposition.MACHINE_REJECTED,
    )
    contact = add_artifact(
        database,
        objects,
        visual_parent["id"],
        stage=Stage.RENDER,
        kind=ArtifactKind.CONTACT_SHEET,
        value=b"retained real-browser contact sheet",
        media_type="image/png",
    )
    visual_result = review.re_evaluate(
        attempt_id=visual_parent["id"],
        artifact_id=contact["id"],
        feedback="judge the exact old render with the current rubric",
        actor="human:alex",
    )
    visual_child = await factory._build_triage(visual_result["attempt"])
    assert visual_child["stage"] == Stage.COMPLETE
    assert visual_child["disposition"] == Disposition.NEEDS_HUMAN
    assert visual_child["review_kind"] == "re_evaluation"
    visual_evaluations = database.evaluations_for_attempt(visual_child["id"], reveal=True)
    assert len(visual_evaluations) == 1
    assert visual_evaluations[0]["artifact_id"] == visual_result["artifact"]["id"]
    assert visual_evaluations[0]["verdict"] == GateVerdict.CANDIDATE
    assert [row["id"] for row in factory._lineage(visual_child)] == [
        visual_child["id"],
        visual_parent["id"],
    ]
    all_ids = {row["id"] for row in database.list_gallery("all")}
    assert {document_child["id"], visual_child["id"]} <= all_ids
    assert document_child["id"] not in {row["id"] for row in database.list_gallery("published")}
    assert visual_child["id"] not in {row["id"] for row in database.list_gallery("final_review")}
    await factory.close()


@pytest.mark.asyncio
async def test_full_state_machine_requires_exact_human_gates_and_retains_every_artifact(
    factory_config, database: Database, objects: ObjectStore
) -> None:
    smart = FakeProvider(
        "gemini-3.7-flash",
        [
            ConceptProposal(
                name="River Glass",
                brief="A crystalline river pattern with a distinct bright head and tapered tail.",
                tags=["water", "glass"],
                seed="river-glass-seed",
                palette_intent="cyan and white",
                motion_intent="a restrained traveling glint",
                implementation_hint="layers",
                implementation_rationale="The regular shine is formula-driven and easy to tune.",
            )
        ],
    )
    image = FakeProvider(
        "gemini-3-pro-image",
        [{"image": b"prototype-image-pixels", "media_type": "image/png", "text": "done"}],
    )
    judge = FakeProvider(
        "gemini-3.7-flash",
        [
            VisualJudgment(
                verdict="candidate",
                reasons=["clear direction"],
                fidelity=0.9,
                readability=0.9,
                role_clarity=0.9,
                animation_quality=0.8,
                craft=0.9,
            ),
            VisualJudgment(
                verdict="candidate",
                reasons=["faithful real render"],
                fidelity=0.9,
                readability=0.9,
                role_clarity=0.9,
                animation_quality=0.9,
                craft=0.9,
            ),
        ],
    )
    providers = FakeRegistry({"smart_text": smart, "image_generator": image, "visual_judge": judge})
    worker = FakeWorker([author_result()])
    api = FakeApi()
    renderer = PassingRenderer()
    factory = Factory(
        factory_config,
        database=database,
        objects=objects,
        providers=providers,  # type: ignore[arg-type]
        worker=worker,
        api=api,  # type: ignore[arg-type]
        renderer=renderer,  # type: ignore[arg-type]
    )
    configure_skill(factory)
    factory.gates = PassingGates()  # type: ignore[assignment]

    prototype_report = await factory.run_once()
    assert prototype_report["halt"] is None
    attempts = database.list_gallery("all")
    assert len(attempts) == 1
    attempt = database.get_attempt(attempts[0]["id"])
    assert attempt["stage"] == Stage.PROTOTYPE_REVIEW
    assert attempt["disposition"] == Disposition.NEEDS_HUMAN
    assert attempt["review_kind"] == "prototype"
    kinds = [row["kind"] for row in database.artifacts_for_attempt(attempt["id"])]
    assert kinds[:3] == [
        ArtifactKind.CONCEPT_BRIEF,
        ArtifactKind.PROTOTYPE,
        ArtifactKind.PROTOTYPE_MANIFEST,
    ]
    assert kinds[3:] == [ArtifactKind.PROVIDER_RESPONSE]
    assert worker.requests == []
    assert api.create_calls == []

    prototype = database.artifacts_for_attempt(attempt["id"], stage=Stage.PROTOTYPE, kind=ArtifactKind.PROTOTYPE)[0]
    review = ReviewService(
        database,
        factory.journal,
        api,  # type: ignore[arg-type]
        factory.persistence,
        factory.behavior_snapshot,
    )
    # A blind training label alone leaves the attempt stopped.
    review.label(
        attempt_id=attempt["id"],
        artifact_id=prototype["id"],
        kind="prototype_label",
        outcome="accept",
        feedback="blind label",
        tags=[],
        actor="human:alex",
    )
    assert database.get_attempt(attempt["id"])["stage"] == Stage.PROTOTYPE_REVIEW
    approved = review.approve_prototype(
        attempt_id=attempt["id"],
        artifact_id=prototype["id"],
        content_hash=prototype["content_hash"],
        feedback="build this exact direction",
        actor="human:alex",
    )["attempt"]
    assert approved["stage"] == Stage.AUTHOR

    build_report = await factory.run_once()
    assert build_report["halt"] is None
    built = database.get_attempt(attempt["id"])
    assert built["stage"] == Stage.FINAL_REVIEW
    assert built["disposition"] == Disposition.NEEDS_HUMAN
    assert built["review_kind"] == "final"
    assert built["production_skin_id"] == "skin-stable"
    assert built["production_revision"] == "1"
    assert built["production_content_hash"] == "sha256:" + "9" * 64
    assert len(worker.requests) == 1
    request = worker.requests[0]
    assert request.inline_artifacts["approved_prototype"].content_hash == prototype["content_hash"]
    assert request.authoring_inputs["prototype_approval"]["decision_id"] == approved["prototype_decision_id"]
    assert api.create_calls[0]["idempotency_key"].startswith("factory-concept:")
    assert api.create_calls[0]["evaluation_only"] is False
    assert api.publication_request_calls == [
        {
            "skin_id": built["production_skin_id"],
            "revision": 1,
            "content_ref": built["production_content_hash"],
        }
    ]
    assert renderer.refs == [built["production_content_hash"]]

    retained = database.artifacts_for_attempt(attempt["id"])
    retained_kinds = [row["kind"] for row in retained]
    assert ArtifactKind.IMPLEMENTATION_PLAN in retained_kinds
    assert ArtifactKind.SKIN_DOCUMENT in retained_kinds
    assert ArtifactKind.WORKER_TRACE in retained_kinds
    assert ArtifactKind.CONTACT_SHEET in retained_kinds
    assert ArtifactKind.ANIMATION_CAPTURE in retained_kinds
    assert all(objects.exists(row["object_ref"]) for row in retained)
    deterministic = [
        row
        for row in database.evaluations_for_attempt(attempt["id"], reveal=True)
        if row["evaluator"] == "deterministic"
    ]
    browser = [
        row
        for row in database.evaluations_for_attempt(attempt["id"], reveal=True)
        if row["evaluator"] == "real_browser"
    ]
    assert deterministic and deterministic[0]["verdict"] == GateVerdict.PASS
    assert browser and browser[0]["verdict"] == GateVerdict.PASS

    published = await review.publish(
        attempt_id=built["id"],
        revision=built["production_revision"],
        content_hash=built["production_content_hash"],
        feedback="approved exact real render",
        actor="human:alex",
    )
    assert published["attempt"]["disposition"] == Disposition.PUBLISHED
    assert api.publish_calls[0]["revision"] == 1
    assert api.publish_calls[0]["content_ref"] == built["production_content_hash"]
    assert database.list_gallery("published")[0]["id"] == built["id"]
    await factory.close()


@pytest.mark.asyncio
async def test_known_safe_provider_failure_retries_with_numbered_operation_key(
    factory_config, database, objects
) -> None:
    factory = Factory(factory_config, database=database, objects=objects)
    attempt = database.create_concept(
        name="retry",
        brief="A detailed retry concept used to exercise known-safe failure handling.",
        seed="retry",
        source="test",
        tags=["test"],
    )
    behavior = {
        "direction_sha": "a",
        "skill_sha": "b",
        "capability_sha": "c",
        "gate_sha": "d",
        "model_config_sha": "e",
    }
    row = database.create_attempt(
        concept_id=attempt["id"],
        purpose="production",
        stage=Stage.PROTOTYPE,
        idempotency_key="retry-attempt",
        behavior=behavior,
        direction_sha="a",
        skill_sha="b",
        capability_sha="c",
        gate_sha="d",
        model_config_sha="e",
    )
    calls = 0

    def invoke() -> ProviderResult:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ProviderError(ProviderFailureKind.UNAVAILABLE, "connect failed", outcome_known=True)
        return ProviderResult(value={"ok": True}, resolved_model="retry-model", usage={"cost_micros": 0})

    operation, result = await factory._provider_call(
        attempt=row,
        stage=Stage.PROTOTYPE,
        key="numbered-provider-call",
        role="image_generator",
        side_effect="generate_prototype_image",
        request={"prompt": "same request"},
        invoke=invoke,
    )
    assert calls == 2
    assert result is not None and result.value == {"ok": True}
    assert operation["idempotency_key"] == "numbered-provider-call:retry:1"
    with database.connect() as connection:
        operations = [
            dict(item)
            for item in connection.execute(
                "SELECT * FROM operation WHERE attempt_id=? ORDER BY created_at", (row["id"],)
            )
        ]
    assert [item["status"] for item in operations] == [
        OperationStatus.FAILED_RETRYABLE,
        OperationStatus.SUCCEEDED,
    ]
    assert operations[0]["cost_charged_micros"] == 0
    await factory.close()


@pytest.mark.asyncio
async def test_successful_image_replay_recovers_exact_non_png_media_type(
    factory_config, database, objects, make_attempt
) -> None:
    factory = Factory(factory_config, database=database, objects=objects)
    attempt = make_attempt(stage=Stage.PROTOTYPE)
    request = {"prompt": "retain the exact WebP result"}
    exact = b"RIFF\x10\x00\x00\x00WEBPexact"

    operation, result = await factory._provider_call(
        attempt=attempt,
        stage=Stage.PROTOTYPE,
        key="webp-image",
        role="image_generator",
        side_effect="generate_prototype_image",
        request=request,
        invoke=lambda: ProviderResult(
            value={"image": exact, "media_type": "image/webp"},
            resolved_model="image-model",
        ),
    )
    assert factory._image_result(operation, result) == (exact, "image/webp")
    assert json.loads(operation["metadata_json"])["result"] == {
        "kind": "image",
        "media_type": "image/webp",
    }

    replay, replay_result = await factory._provider_call(
        attempt=database.get_attempt(attempt["id"]),
        stage=Stage.PROTOTYPE,
        key="webp-image",
        role="image_generator",
        side_effect="generate_prototype_image",
        request=request,
        invoke=lambda: pytest.fail("successful image operation must not call the provider again"),
    )
    assert replay_result is None
    assert factory._image_result(replay, replay_result) == (exact, "image/webp")
    invalid = {**replay, "metadata_json": json.dumps({"result": {"media_type": "image/gif"}})}
    with pytest.raises(ValueError, match="unsupported image media type"):
        factory._image_result(invalid, None)
    await factory.close()


@pytest.mark.asyncio
async def test_restart_skips_durable_known_safe_failure_and_uses_next_numbered_key(
    factory_config, database, objects
) -> None:
    factory = Factory(factory_config, database=database, objects=objects)
    concept = database.create_concept(
        name="restart",
        brief="A detailed restart concept used to prove durable retry behavior.",
        seed="restart",
        source="test",
        tags=["test"],
    )
    attempt = database.create_attempt(
        concept_id=concept["id"],
        purpose="production",
        stage=Stage.CONCEPT,
        idempotency_key="restart-attempt",
        behavior={},
        direction_sha="a",
        skill_sha="b",
        capability_sha="c",
        gate_sha="d",
        model_config_sha="e",
    )
    request = {"prompt": "durable request"}
    with pytest.raises(ProviderError):
        await factory.journal.run_provider(
            attempt_id=attempt["id"],
            stage=Stage.CONCEPT,
            idempotency_key="restart-call",
            side_effect="generate_concept",
            provider_role="smart_text",
            request=request,
            reserve_micros=factory._reservation("smart_text", "generate_concept"),
            invoke=lambda: (_ for _ in ()).throw(
                ProviderError(ProviderFailureKind.TIMEOUT, "connect timeout", outcome_known=True)
            ),
        )

    invocations = 0

    def recovered_in_new_process() -> ProviderResult:
        nonlocal invocations
        invocations += 1
        return ProviderResult(value={"resumed": True}, resolved_model="smart")

    operation, _ = await factory._provider_call(
        attempt=database.get_attempt(attempt["id"]),
        stage=Stage.CONCEPT,
        key="restart-call",
        role="smart_text",
        side_effect="generate_concept",
        request=request,
        invoke=recovered_in_new_process,
    )
    assert invocations == 1
    assert operation["idempotency_key"] == "restart-call:retry:1"
    assert database.get_attempt(attempt["id"])["cost_reserved_micros"] == (
        factory._reservation("smart_text", "generate_concept") * 2
    )
    await factory.close()


@pytest.mark.asyncio
async def test_authenticated_not_executed_resolution_resumes_same_request_at_next_key(
    factory_config, database, objects, make_attempt
) -> None:
    factory = Factory(factory_config, database=database, objects=objects)
    attempt = make_attempt(stage=Stage.PROTOTYPE)
    request = {"prompt": "same exact request"}
    reserve = factory._reservation("image_generator", "generate_prototype_image")

    with pytest.raises(ProviderError):
        await factory._provider_call(
            attempt=attempt,
            stage=Stage.PROTOTYPE,
            key="operator-resume",
            role="image_generator",
            side_effect="generate_prototype_image",
            request=request,
            invoke=lambda: (_ for _ in ()).throw(
                ProviderError(
                    ProviderFailureKind.UNKNOWN_OUTCOME,
                    "response lost after send",
                    outcome_known=False,
                )
            ),
        )
    unknown = database.unresolved_operations()[0]
    assert database.get_attempt(attempt["id"])["cost_charged_micros"] == reserve
    database.resolve_operation(
        operation_id=unknown["id"],
        resolution="confirmed_not_executed",
        evidence_ref="provider:audit:not-found",
        actor="human:operator",
    )
    assert database.get_operation(unknown["id"])["retry_class"] == "retry_safe"
    assert database.get_attempt(attempt["id"])["cost_charged_micros"] == 0

    invocations = 0

    def invoke() -> ProviderResult:
        nonlocal invocations
        invocations += 1
        return ProviderResult(
            value={"image": b"pixels", "media_type": "image/png"},
            resolved_model="image-model",
        )

    with pytest.raises(VersionConflict):
        await factory._provider_call(
            attempt=database.get_attempt(attempt["id"]),
            stage=Stage.PROTOTYPE,
            key="operator-resume",
            role="image_generator",
            side_effect="generate_prototype_image",
            request={"prompt": "changed request"},
            invoke=invoke,
        )
    assert invocations == 0

    resumed, result = await factory._provider_call(
        attempt=database.get_attempt(attempt["id"]),
        stage=Stage.PROTOTYPE,
        key="operator-resume",
        role="image_generator",
        side_effect="generate_prototype_image",
        request=request,
        invoke=invoke,
    )
    assert invocations == 1
    assert resumed["idempotency_key"] == "operator-resume:retry:1"
    assert resumed["status"] == OperationStatus.SUCCEEDED
    assert result is not None
    await factory.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind",
    [ProviderFailureKind.TIMEOUT, ProviderFailureKind.UNAVAILABLE, ProviderFailureKind.QUOTA],
)
async def test_unknown_provider_outcome_is_never_retried(factory_config, database, objects, kind) -> None:
    factory = Factory(factory_config, database=database, objects=objects)
    concept = database.create_concept(
        name="unknown",
        brief="A detailed unknown-outcome concept for the reconciliation test.",
        seed="unknown",
        source="test",
        tags=["test"],
    )
    attempt = database.create_attempt(
        concept_id=concept["id"],
        purpose="production",
        stage=Stage.PROTOTYPE,
        idempotency_key="unknown-attempt",
        behavior={},
        direction_sha="a",
        skill_sha="b",
        capability_sha="c",
        gate_sha="d",
        model_config_sha="e",
    )
    calls = 0

    def invoke() -> ProviderResult:
        nonlocal calls
        calls += 1
        raise ProviderError(
            kind,
            "ambiguous response after send",
            outcome_known=False,
        )

    with pytest.raises(ProviderError):
        await factory._provider_call(
            attempt=attempt,
            stage=Stage.PROTOTYPE,
            key="unknown-no-retry",
            role="image_generator",
            side_effect="generate_prototype_image",
            request={"prompt": "x"},
            invoke=invoke,
        )
    assert calls == 1
    assert database.unresolved_operations()[0]["idempotency_key"] == "unknown-no-retry"
    await factory.close()


@pytest.mark.asyncio
async def test_exhausted_known_safe_retries_are_not_replayed_after_restart(
    factory_config, database, objects, make_attempt
) -> None:
    factory = Factory(factory_config, database=database, objects=objects)
    attempt = make_attempt(stage=Stage.CONCEPT)
    calls = 0

    def unavailable() -> ProviderResult:
        nonlocal calls
        calls += 1
        raise ProviderError(ProviderFailureKind.UNAVAILABLE, "still offline", outcome_known=True)

    with pytest.raises(ProviderError) as exhausted:
        await factory._provider_call(
            attempt=attempt,
            stage=Stage.CONCEPT,
            key="exhausted-retries",
            role="smart_text",
            side_effect="generate_concept",
            request={"prompt": "unchanged"},
            invoke=unavailable,
        )
    assert exhausted.value.kind == ProviderFailureKind.UNAVAILABLE
    assert calls == factory.config.budgets.provider_retries + 1

    calls = 0
    with pytest.raises(ExistingOperation, match="retries exhausted"):
        await factory._provider_call(
            attempt=database.get_attempt(attempt["id"]),
            stage=Stage.CONCEPT,
            key="exhausted-retries",
            role="smart_text",
            side_effect="generate_concept",
            request={"prompt": "unchanged"},
            invoke=unavailable,
        )
    assert calls == 0
    await factory.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "retryable"),
    [
        (ProviderFailureKind.REFUSAL, False),
        (ProviderFailureKind.UNAVAILABLE, True),
    ],
)
async def test_run_once_terminalizes_known_provider_failure_instead_of_stranding_oldest_attempt(
    factory_config,
    database,
    objects,
    make_attempt,
    monkeypatch,
    kind,
    retryable,
) -> None:
    factory = Factory(factory_config, database=database, objects=objects)
    attempt = make_attempt(stage=Stage.CONCEPT)
    calls = 0

    monkeypatch.setattr(factory, "_behavior_drift_reason", lambda *_: None)

    async def failing_advance(current: dict[str, Any]) -> dict[str, Any]:
        nonlocal calls

        def invoke() -> ProviderResult:
            nonlocal calls
            calls += 1
            raise ProviderError(kind, "known provider failure", outcome_known=True)

        await factory._provider_call(
            attempt=current,
            stage=Stage.CONCEPT,
            key=f"known-failure:{kind}",
            role="smart_text",
            side_effect="generate_concept",
            request={"prompt": "exact retained request"},
            invoke=invoke,
        )
        raise AssertionError("known failure unexpectedly returned")

    monkeypatch.setattr(factory, "_advance", failing_advance)
    report = await factory.run_once()

    terminal = database.get_attempt(attempt["id"])
    assert terminal["disposition"] == Disposition.BLOCKED
    assert database.next_active_attempt() is None
    assert report["advanced"][-1]["attempt"] == attempt["id"]
    assert report["advanced"][-1]["state"] == Disposition.BLOCKED
    assert "known external failure" in report["advanced"][-1]["failure"]
    expected_calls = factory.config.budgets.provider_retries + 1 if retryable else 1
    assert calls == expected_calls
    with database.connect() as connection:
        statuses = [
            row[0]
            for row in connection.execute(
                "SELECT status FROM operation WHERE attempt_id=? ORDER BY created_at",
                (attempt["id"],),
            )
        ]
    expected_status = (
        OperationStatus.FAILED_TERMINAL if kind == ProviderFailureKind.REFUSAL else OperationStatus.FAILED_RETRYABLE
    )
    assert statuses == [expected_status] * expected_calls
    await factory.close()


def charge_operation(database: Database, attempt: dict[str, Any], amount: int, key: str) -> None:
    operation, _ = database.begin_operation(
        attempt_id=attempt["id"],
        stage=Stage.CONCEPT,
        idempotency_key=key,
        side_effect="charged",
        provider_role="smart_text",
        request_hash=hashlib.sha256(key.encode()).hexdigest(),
        cost_reserved_micros=amount,
    )
    operation = database.transition_operation(operation["id"], OperationStatus.INTENT, OperationStatus.RUNNING)
    database.transition_operation(
        operation["id"],
        OperationStatus.RUNNING,
        OperationStatus.SUCCEEDED,
        cost_charged_micros=amount,
    )


def test_budget_checks_cover_attempt_day_and_program_caps(factory_config, database, objects, make_attempt) -> None:
    factory = Factory(factory_config, database=database, objects=objects)
    attempt = make_attempt()
    factory.config.budgets.max_cost_micros_per_attempt = 100
    charge_operation(database, attempt, 95, "attempt-cost")
    with pytest.raises(BudgetExceeded, match="per-attempt"):
        factory._check_budget(database.get_attempt(attempt["id"]), 6)

    factory.config.budgets.max_cost_micros_per_attempt = 1_000
    factory.config.budgets.max_cost_micros_per_day = 100
    with pytest.raises(BudgetExceeded, match="daily"):
        factory._check_budget(None, 6)

    factory.config.budgets.max_cost_micros_per_day = 1_000
    factory.config.budgets.max_cost_micros_program = 100
    with pytest.raises(BudgetExceeded, match="program"):
        factory._check_budget(None, 6)


@pytest.mark.asyncio
async def test_failed_browser_capture_retains_json_evidence_not_an_empty_png(
    factory_config, database, objects, make_attempt
) -> None:
    factory = Factory(
        factory_config,
        database=database,
        objects=objects,
        renderer=FailingRenderer(),  # type: ignore[arg-type]
    )
    attempt = make_attempt(stage=Stage.RENDER)
    attempt = database.update_attempt(
        attempt["id"],
        attempt["version"],
        production_skin_id="skin-private",
        production_revision="1",
        production_content_hash="sha256:" + "6" * 64,
    )
    result = await factory._render(attempt)
    assert result["disposition"] == Disposition.MACHINE_REJECTED
    artifacts = database.artifacts_for_attempt(attempt["id"])
    assert [item["kind"] for item in artifacts] == [ArtifactKind.RENDER_EVIDENCE]
    assert artifacts[0]["media_type"] == "application/json"
    evidence = json.loads(objects.get(artifacts[0]["object_ref"]))
    assert evidence["renderer_exit"] == 2
    assert evidence["stderr"] == "injected browser failure"
    evaluations = database.evaluations_for_attempt(attempt["id"], reveal=True)
    assert evaluations[0]["artifact_id"] == artifacts[0]["id"]
    assert evaluations[0]["verdict"] == GateVerdict.FAIL
    await factory.close()


def test_browser_renderer_failure_manifest_contains_replayable_diagnostics(factory_config, monkeypatch) -> None:
    def run(*_: Any, **__: Any):
        return type(
            "Completed",
            (),
            {"returncode": 17, "stdout": "capture stdout", "stderr": "capture stderr"},
        )()

    monkeypatch.setattr("snaketron_factory.renderer.subprocess.run", run)
    renderer = BrowserRenderer(factory_config)
    monkeypatch.setattr(renderer, "renderer_sha", lambda: "renderer-sha")
    evidence = renderer.capture("sha256:" + "5" * 64)
    assert evidence.contact_sheet == b""
    assert evidence.animation == b""
    assert evidence.manifest == {
        "schema_version": 1,
        "content_ref": "sha256:" + "5" * 64,
        "renderer_exit": 17,
        "stdout": "capture stdout",
        "stderr": "capture stderr",
    }
    assert evidence.gate_result.verdict == GateVerdict.FAIL
    assert evidence.renderer_sha == "renderer-sha"


def _served_renderer_attestation(
    renderer: BrowserRenderer,
    *,
    changed_path: str | None = None,
) -> dict[str, Any]:
    execution = renderer.execution_config()
    assets = []
    for path in ("index.html", "main.js", "client_bg.wasm"):
        expected = execution["browser_bundle"]["assets"][path]
        assets.append(
            {
                "path": path,
                "kind": path.rsplit(".", 1)[-1],
                "sha256": "0" * 64 if path == changed_path else expected["sha256"],
                "size_bytes": expected["size_bytes"],
            }
        )
    return {
        "schema_version": 1,
        "bundle_manifest_sha256": execution["browser_bundle_sha256"],
        "assets": assets,
        "errors": [],
    }


def _write_successful_browser_capture(output: Path, attestation: dict[str, Any]) -> None:
    output.mkdir(parents=True)
    sheet = b"attested contact sheet"
    animation = b"attested animation"
    (output / "contact-sheet.png").write_bytes(sheet)
    (output / "animation.webm").write_bytes(animation)
    (output / "renderer-attestation.json").write_text(json.dumps(attestation), encoding="utf-8")
    (output / "evidence.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "asset_status": {"requested": 0, "failed": 0},
                "served_renderer": attestation,
                "contact_sheet": {"sha256": hashlib.sha256(sheet).hexdigest()},
                "animation": {"sha256": hashlib.sha256(animation).hexdigest()},
            }
        ),
        encoding="utf-8",
    )


def test_browser_renderer_passes_pinned_bundle_to_capture_and_verifies_served_bytes(
    factory_config, monkeypatch
) -> None:
    renderer = BrowserRenderer(factory_config)
    monkeypatch.setattr(renderer, "renderer_sha", lambda: "renderer-sha")
    attestation = _served_renderer_attestation(renderer)

    def run(command: list[str], **kwargs: Any):
        raw_manifest = kwargs["env"][RENDERER_BUNDLE_MANIFEST_ENV]
        assert hashlib.sha256(raw_manifest.encode()).hexdigest() == kwargs["env"][RENDERER_BUNDLE_SHA_ENV]
        _write_successful_browser_capture(Path(command[-1]), attestation)
        return type("Completed", (), {"returncode": 0, "stdout": "", "stderr": ""})()

    monkeypatch.setattr("snaketron_factory.renderer.subprocess.run", run)

    evidence = renderer.capture("sha256:" + "5" * 64)

    assert evidence.gate_result.verdict == GateVerdict.PASS
    assert evidence.manifest["served_renderer"] == attestation


@pytest.mark.parametrize("changed_path", ["main.js", "client_bg.wasm"])
def test_browser_renderer_fails_closed_when_served_executable_bytes_differ(
    factory_config, monkeypatch, changed_path: str
) -> None:
    renderer = BrowserRenderer(factory_config)
    monkeypatch.setattr(renderer, "renderer_sha", lambda: "renderer-sha")
    attestation = _served_renderer_attestation(renderer, changed_path=changed_path)

    def run(command: list[str], **_kwargs: Any):
        _write_successful_browser_capture(Path(command[-1]), attestation)
        return type("Completed", (), {"returncode": 0, "stdout": "", "stderr": ""})()

    monkeypatch.setattr("snaketron_factory.renderer.subprocess.run", run)

    with pytest.raises(RendererDrift, match=rf"served bytes differ for {changed_path}"):
        renderer.capture("sha256:" + "5" * 64)


@pytest.mark.parametrize("pin", ["renderer", "config"])
def test_browser_renderer_refuses_changed_pin_before_capture_process(factory_config, monkeypatch, pin) -> None:
    renderer = BrowserRenderer(factory_config)
    monkeypatch.setattr(renderer, "renderer_sha", lambda: "renderer-sha")
    monkeypatch.setattr(
        "snaketron_factory.renderer.subprocess.run",
        lambda *_args, **_kwargs: pytest.fail("capture process must not start after pin drift"),
    )
    expected_renderer = "other-renderer" if pin == "renderer" else "renderer-sha"
    expected_config = "0" * 64 if pin == "config" else renderer.execution_config_sha(renderer.execution_config())

    with pytest.raises(RendererDrift, match=pin):
        renderer.capture(
            "sha256:" + "5" * 64,
            expected_renderer_sha=expected_renderer,
            expected_config_sha=expected_config,
        )
