from __future__ import annotations

import hashlib
import io
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar

import pytest
from conftest import add_artifact
from PIL import Image

from snaketron_factory.assets import ForgeBundle, ForgeVariant
from snaketron_factory.db import Database, VersionConflict, canonical_json
from snaketron_factory.domain import (
    ArtifactKind,
    AssetPlan,
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
        self.skin_authority: dict[str, Any] | None = None

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

    async def get_skin_authority(self, skin_id: str | int, *, operator: bool = False) -> ProviderResult:
        assert operator is False
        return ProviderResult(
            value=self.skin_authority
            or {
                "skinId": skin_id,
                "publication": "private",
                "publishedRevision": None,
                "pendingRevision": None,
            },
            resolved_model="snaketron-api",
        )


def retained_request_metadata(objects: ObjectStore, request: Any) -> dict[str, str]:
    retained = objects.put(OperationJournal.request_payload(request))
    return {"request_ref": retained.uri, "request_sha256": retained.sha256}


@dataclass(frozen=True)
class FakeGateManifest:
    sha256: str = "gate-test-sha"

    @staticmethod
    def result(
        name: str,
        passed: bool,
        *,
        reasons: list[str] | None = None,
        measurements: dict[str, Any] | None = None,
    ) -> GateResult:
        return GateResult(
            gate=name,
            gate_version="fake-v1",
            blocking=True,
            verdict=GateVerdict.PASS if passed else GateVerdict.FAIL,
            reasons=reasons or [],
            measurements=measurements or {},
        )


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
            design_guidelines={
                "artistic_direction": "One glass pattern direction.",
                "concept_twist": "Original glass-flow treatment.",
                "structure": "pattern",
                "body_strategy": "Reads at four cells and across early growth, turns, and overlap.",
                "head_zone": "light_field_dark_core",
                "asset_strategy": "Procedural layers require no raster seams.",
            },
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
async def test_prototype_retry_sends_literal_human_feedback_in_journaled_prompt(
    factory_config, database, objects, make_attempt
) -> None:
    image = FakeProvider(
        "gemini-3-pro-image",
        [{"image": b"corrected prototype", "media_type": "image/png", "text": "done"}],
    )
    factory = Factory(
        factory_config,
        database=database,
        objects=objects,
        providers=FakeRegistry({"image_generator": image}),  # type: ignore[arg-type]
    )
    configure_skill(factory)
    parent = make_attempt(stage=Stage.PROTOTYPE_REVIEW, disposition=Disposition.MACHINE_REJECTED)
    review = ReviewService(
        database,
        factory.journal,
        factory.api,
        factory.persistence,
        factory.behavior_snapshot,
    )
    correction = "Keep the concept, but make the head brighter and remove the muddy border."
    child = (
        await review.retry(
            attempt_id=parent["id"],
            from_stage="prototype",
            feedback=correction,
            actor="human:alex",
        )
    )["attempt"]

    updated = await factory._prototype(child)
    assert updated["stage"] == Stage.PROTOTYPE_TRIAGE
    prompt = image.calls[0]["prompt"]
    assert canonical_json([correction]) in prompt
    assert "Locked Skin Design Guidelines" in prompt
    assert "Keep one continuous thin rounded snake body." in prompt
    assert '"id":"prototype-geometry-test-v1"' in prompt
    assert "Treat its white capsule as the only paintable snake" in prompt
    assert "body. Fill that body with the proposed skin" in prompt
    assert "Do not create gaps, articulated modules, diamonds, plates" in prompt
    with database.connect() as connection:
        operation = dict(
            connection.execute(
                "SELECT * FROM operation WHERE attempt_id=? ORDER BY created_at",
                (child["id"],),
            ).fetchone()
        )
    operation_metadata = json.loads(operation["metadata_json"])
    retained_request = json.loads(objects.get(operation_metadata["request_ref"]))
    assert operation["request_hash"] == factory.journal.request_hash(retained_request)
    assert retained_request["prompt"] == prompt
    assert retained_request["aspect_ratio"] == "16:9"
    assert retained_request["image_size"] == "2K"
    assert retained_request["references"] == [
        {
            "content_hash": f"sha256:{json.loads(child['behavior_json'])['prototype_guide_sha']}",
            "contract_id": "prototype-geometry-test-v1",
            "contract_sha256": json.loads(child["behavior_json"])["prototype_geometry_sha"],
            "media_type": "image/png",
            "object_ref": json.loads(child["behavior_json"])["prototype_guide_ref"],
            "role": "strict_snake_body_geometry_guide",
        }
    ]
    assert image.calls[0]["references"] == [("image/png", objects.get(retained_request["references"][0]["object_ref"]))]
    await factory.close()


def test_asset_retry_prompt_safely_delimits_literal_human_feedback() -> None:
    asset = AssetPlan(
        kind="sheet",
        natural_length_cells=20,
        frames=32,
        desired_fps=32,
        texels_per_cell=16,
        anchor="whole",
        fit="tile",
        fade="none",
        prompt="Generate the approved glass texture.",
    )
    feedback = ["Make it brighter; keep the apostrophe in Alex's note."]

    prompt = Factory._asset_prompt(asset, asset.prompt, "fix the y seam", feedback)

    assert canonical_json(feedback) in prompt
    assert "fix the y seam" in prompt


@pytest.mark.asyncio
async def test_extreme_tall_sheet_is_journaled_in_no_crop_slices_and_resumes_before_forge(
    factory_config, database, objects, make_attempt
) -> None:
    asset = AssetPlan(
        kind="sheet",
        natural_length_cells=4,
        frames=60,
        desired_fps=60,
        texels_per_cell=16,
        anchor="whole",
        fit="clip",
        fade="none",
        prompt="Animate a continuous electric pulse along the approved snake.",
    )
    slices = Factory._asset_image_slices(asset)
    assert 1 < len(slices) <= factory_config.budgets.max_image_slices_per_asset
    assert [(item["start_frame"], item["end_frame"]) for item in slices] == [
        (0, 8),
        (8, 16),
        (16, 24),
        (24, 32),
        (32, 40),
        (40, 48),
        (48, 56),
        (56, 60),
    ]
    # A common, less-tall sheet remains a single paid call.
    ordinary = asset.model_copy(update={"natural_length_cells": 20, "frames": 32, "desired_fps": 32})
    assert len(Factory._asset_image_slices(ordinary)) == 1

    def slice_png(start: int, end: int) -> bytes:
        rows = end - start
        # Match the provider-native requested aspect rather than the final
        # grid. Narrow edge markers prove normalization resizes the full
        # source instead of center-cropping it to 4:rows.
        width, height = (90, 160) if rows == 8 else (96, 96)
        image = Image.new("RGB", (width, height))
        for local, frame in enumerate(range(start, end)):
            color = ((frame * 3 + 1) % 256, (frame * 5 + 2) % 256, (frame * 7 + 3) % 256)
            top = local * height // rows
            bottom = (local + 1) * height // rows
            image.paste(color, (0, top, width, bottom))
        image.paste((255, 0, 0), (0, 0, 3, height))
        image.paste((0, 0, 255), (width - 3, 0, width, height))
        output = io.BytesIO()
        image.save(output, format="PNG", optimize=True)
        return output.getvalue()

    provider_bytes = [slice_png(item["start_frame"], item["end_frame"]) for item in slices]
    image_provider = FakeProvider(
        "gemini-3-pro-image",
        [{"image": payload, "media_type": "image/png", "text": "slice"} for payload in provider_bytes],
    )
    factory = Factory(
        factory_config,
        database=database,
        objects=objects,
        providers=FakeRegistry({"image_generator": image_provider}),  # type: ignore[arg-type]
    )
    factory.pinned_gates = lambda _: factory.gates  # type: ignore[method-assign]
    prototype_bytes = _exact_png((21, 42, 84), width=320, height=64)
    prototype_ref = "sha256:" + hashlib.sha256(prototype_bytes).hexdigest()
    attempt = make_attempt(stage=Stage.ASSETS, approved_prototype_hash=prototype_ref)
    prototype_artifact = add_artifact(
        database,
        objects,
        attempt["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        value=prototype_bytes,
        media_type="image/png",
    )
    plan = ImplementationPlan(
        path="sprite_sheet",
        rationale="The approved pulse needs a time-sampled sheet.",
        fidelity_features=["continuous electric pulse"],
        layer_plan=["sprite"],
        asset_plan=[asset],
        animation_plan=["sample the loop into sixty frame rows"],
        required_wrap_axes=["y"],
        risks=["temporal seam"],
        design_guidelines={
            "artistic_direction": "One electric pulse direction.",
            "concept_twist": "Original pulse treatment.",
            "structure": "sprite",
            "body_strategy": "Reads at four cells and across growth, turns, and headward overlap.",
            "head_zone": "light_field_dark_core",
            "asset_strategy": "Tall fast rows loop seamlessly on y.",
        },
    )
    add_artifact(
        database,
        objects,
        attempt["id"],
        stage=Stage.AUTHOR,
        kind=ArtifactKind.IMPLEMENTATION_PLAN,
        value=plan.model_dump_json().encode(),
        media_type="application/json",
    )
    add_artifact(
        database,
        objects,
        attempt["id"],
        stage=Stage.AUTHOR,
        kind=ArtifactKind.SKIN_DOCUMENT,
        value=json.dumps(
            {
                "schema_version": 2,
                "name": "Tall Pulse",
                "period_ms": 1000,
                "textures": [{"name": "pulse", "kind": "sheet", "ref": "pending:asset:0"}],
                "layers": [],
            }
        ).encode(),
        media_type="application/json",
    )

    class ReachedForge(RuntimeError):
        pass

    forge_inputs: list[bytes] = []

    def stop_at_forge(source: bytes, planned: AssetPlan) -> ForgeBundle:
        assert planned == asset
        forge_inputs.append(source)
        raise ReachedForge

    factory.assets.forge = stop_at_forge  # type: ignore[method-assign]
    with pytest.raises(ReachedForge):
        await factory._build_assets(attempt)

    assert len(image_provider.calls) == len(slices)
    assert len(forge_inputs) == 1
    with Image.open(io.BytesIO(forge_inputs[0])) as assembled:
        assert assembled.size == (64, 960)
        for frame in range(60):
            expected = ((frame * 3 + 1) % 256, (frame * 5 + 2) % 256, (frame * 7 + 3) % 256)
            assert assembled.getpixel((32, frame * 16 + 8)) == expected
            left = assembled.getpixel((0, frame * 16 + 8))
            right = assembled.getpixel((63, frame * 16 + 8))
            assert left[0] > 200 and left[1] < 30 and left[2] < 30
            assert right[2] > 200 and right[0] < 30 and right[1] < 30

    retained = database.artifacts_for_attempt(attempt["id"], stage=Stage.ASSETS, kind=ArtifactKind.SOURCE_ASSET)
    by_phase: dict[str, list[dict[str, Any]]] = {}
    for artifact in retained:
        metadata = json.loads(artifact["metadata_json"])
        by_phase.setdefault(str(metadata.get("phase")), []).append(artifact)
    assert len(by_phase["provider_slice_output"]) == len(slices)
    assert len(by_phase["normalized_provider_slice"]) == len(slices)
    assert len(by_phase["assembled_provider_output"]) == 1
    assert {objects.get(row["object_ref"]) for row in by_phase["provider_slice_output"]} == set(provider_bytes)
    assembled_metadata = json.loads(by_phase["assembled_provider_output"][0]["metadata_json"])
    assert assembled_metadata["frame_ranges"] == [[item["start_frame"], item["end_frame"]] for item in slices]
    assert assembled_metadata["assembly"] == "vertical_exact_no_crop"
    normalized_metadata = [json.loads(row["metadata_json"]) for row in by_phase["normalized_provider_slice"]]
    assert all(item["normalization"] == "no_crop_direct_resize" for item in normalized_metadata)
    assert sum(item["target_height_px"] for item in normalized_metadata) == 960

    with database.connect() as connection:
        operations = [
            dict(row)
            for row in connection.execute(
                "SELECT * FROM operation WHERE attempt_id=? ORDER BY idempotency_key",
                (attempt["id"],),
            ).fetchall()
        ]
    assert len(operations) == len(slices)
    assert all(row["status"] == OperationStatus.SUCCEEDED for row in operations)
    assert all(row["side_effect"] == "generate_build_asset_slice" for row in operations)
    assert {objects.get(row["result_hash"]) for row in operations} == set(provider_bytes)
    requests = []
    for operation in operations:
        operation_metadata = json.loads(operation["metadata_json"])
        request = json.loads(objects.get(operation_metadata["request_ref"]))
        assert operation["request_hash"] == factory.journal.request_hash(request)
        requests.append(request)
    requests.sort(key=lambda request: request["slice"]["start_frame"])
    assert [request["slice"]["end_frame"] for request in requests] == [8, 16, 24, 32, 40, 48, 56, 60]
    assert "resting and reduced-motion frame" in requests[0]["prompt"]
    assert "preceding normalized time slice" in requests[1]["prompt"]
    assert "flow cleanly back into global row 0" in requests[-1]["prompt"]
    assert requests[0]["continuity_refs"] == []
    assert len(requests[-1]["continuity_refs"]) == 2
    assert all("out of [0, 60)" in request["prompt"] for request in requests)

    # Re-entering after a crash at the forge boundary reconstructs all slice
    # results from the operation journal and creates no new paid calls.
    with pytest.raises(ReachedForge):
        await factory._build_assets(database.get_attempt(attempt["id"]))
    assert len(image_provider.calls) == len(slices)
    assert len(forge_inputs) == 2
    assert forge_inputs[1] == forge_inputs[0]
    with database.connect() as connection:
        operation_count = connection.execute(
            "SELECT COUNT(*) FROM operation WHERE attempt_id=?", (attempt["id"],)
        ).fetchone()[0]
    assert operation_count == len(slices)
    rounds = factory_config.budgets.provider_retries + 1
    assert len(slices) * rounds * rounds <= factory_config.budgets.max_asset_image_calls_per_attempt

    # Geometry that would exceed the configured slice ceiling is rejected by
    # the pre-spend resource check, not partially generated.
    too_tall = asset.model_copy(update={"natural_length_cells": 1, "frames": 120, "desired_fps": 120})
    too_tall_plan = plan.model_copy(update={"asset_plan": [too_tall]})
    with pytest.raises(ValueError, match="needs 60 slices"):
        factory._validate_asset_image_call_budget(too_tall_plan)
    assert len(image_provider.calls) == len(slices)

    # The no-crop sheet path also covers ordinary geometry, but it remains one
    # provider call and one exact assembled forge input.
    image_provider.responses.append(
        {
            "image": _exact_png((90, 45, 180), width=320, height=512),
            "media_type": "image/png",
            "text": "ordinary sheet",
        }
    )
    ordinary_source, ordinary_artifact = await factory._generate_asset_provider_source(
        attempt=database.get_attempt(attempt["id"]),
        asset=ordinary,
        asset_index=1,
        generation=0,
        prompt=Factory._asset_prompt(ordinary, ordinary.prompt, "", []),
        prototype=prototype_artifact,
        prototype_bytes=prototype_bytes,
    )
    assert len(image_provider.calls) == len(slices) + 1
    with Image.open(io.BytesIO(ordinary_source)) as ordinary_image:
        assert ordinary_image.size == (320, 512)
    assert json.loads(ordinary_artifact["metadata_json"])["slice_count"] == 1
    assert image_provider.calls[-1]["aspect_ratio"] == "9:16"
    await factory.close()


def _exact_png(color: tuple[int, int, int], *, width: int = 256, height: int = 64) -> bytes:
    output = io.BytesIO()
    Image.new("RGB", (width, height), color).save(output, format="PNG")
    return output.getvalue()


def _asset_plan() -> ImplementationPlan:
    return ImplementationPlan(
        path="texture",
        rationale="The exact retained coat is the implementation under test.",
        fidelity_features=["exact retained pixels"],
        layer_plan=["fallback", "coat"],
        asset_plan=[
            AssetPlan(
                kind="coat",
                natural_length_cells=4,
                frames=1,
                texels_per_cell=64,
                fit="tile",
                prompt="Paint the exact coat.",
            )
        ],
        animation_plan=[],
        required_wrap_axes=["x"],
        risks=["seam"],
        design_guidelines={
            "artistic_direction": "One exact coat direction.",
            "concept_twist": "Original retained texture fixture.",
            "structure": "pattern",
            "body_strategy": "Reads at four cells and across growth, turns, and overlap.",
            "head_zone": "light_field_dark_core",
            "asset_strategy": "The coat tiles seamlessly on x.",
        },
    )


def test_rejected_forge_retains_every_identical_rung_occurrence_and_repaired_pixels(
    factory_config, database, objects, make_attempt
) -> None:
    factory = Factory(factory_config, database=database, objects=objects)
    attempt = make_attempt(stage=Stage.ASSETS)
    normalized_bytes = _exact_png((10, 20, 30))
    normalized = factory._store_bytes_artifact(
        attempt,
        Stage.ASSETS,
        ArtifactKind.SOURCE_ASSET,
        normalized_bytes,
        "image/png",
        metadata={"asset_index": 0, "generation": 0, "phase": "normalized_forge_input"},
        occurrence_key="asset:0:generation:0:normalized-input",
    )
    rung_bytes = _exact_png((40, 50, 60))
    rung_ref = "sha256:" + hashlib.sha256(rung_bytes).hexdigest()
    rejected_bytes = _exact_png((211, 17, 99))
    rejected_ref = "sha256:" + hashlib.sha256(rejected_bytes).hexdigest()
    variants = tuple(
        ForgeVariant(
            content_ref=rung_ref,
            url=f"/api/textures/variants/{rung_ref}.png",
            width_px=256,
            height_px=64,
            bytes=len(rung_bytes),
            texels_per_cell=texels,
            data=rung_bytes,
        )
        for texels in (64, 32)
    )
    rejected_output = ForgeVariant(
        content_ref=rejected_ref,
        url=f"/factory/rejected/{rejected_ref}.png",
        width_px=256,
        height_px=64,
        bytes=len(rejected_bytes),
        texels_per_cell=64,
        data=rejected_bytes,
    )
    failure = GateResult(
        gate="detail_density",
        gate_version="exact-v1",
        blocking=True,
        verdict=GateVerdict.FAIL,
        reasons=["injected strict rejection"],
    )
    bundle = ForgeBundle(
        manifest={
            "schema_version": 1,
            "descriptor": {
                "kind": "coat",
                "body_columns": 4,
                "frame_rows": None,
                "variants": [
                    {
                        "content_ref": rung_ref,
                        "url": variants[0].url,
                        "width_px": 256,
                        "height_px": 64,
                        "bytes": len(rung_bytes),
                        "texels_per_cell": item.texels_per_cell,
                    }
                    for item in variants
                ],
            },
        },
        descriptor={},
        variants=variants,
        gate_results=(failure,),
        repaired=True,
        normalized_source=normalized_bytes,
        repair_methods=("tx_t:x",),
        rejected_output=rejected_output,
    )

    evidence, accepted = factory._retain_forge_bundle(
        attempt=attempt,
        asset_index=0,
        generation=0,
        bundle=bundle,
        normalized=normalized,
        provider_artifact_id=None,
    )

    assert not accepted
    retained = database.artifacts_for_attempt(attempt["id"], stage=Stage.ASSETS, kind=ArtifactKind.TEXTURE_VARIANT)
    assert len(retained) == 2
    assert retained[0]["id"] != retained[1]["id"]
    assert {row["content_hash"] for row in retained} == {rung_ref}
    assert {row["object_ref"] for row in retained} == {rung_ref}
    assert {json.loads(row["metadata_json"])["variant_index"] for row in retained} == {0, 1}
    assert all(json.loads(row["metadata_json"])["gate_accepted"] is False for row in retained)
    rejected = next(
        row
        for row in database.artifacts_for_attempt(attempt["id"], stage=Stage.ASSETS, kind=ArtifactKind.SOURCE_ASSET)
        if json.loads(row["metadata_json"]).get("phase") == "forge_rejected_output"
    )
    assert objects.get(rejected["object_ref"]) == rejected_bytes
    assert json.loads(rejected["provenance_json"])["post_repair"] is True
    assert json.loads(rejected["metadata_json"])["forge_manifest_artifact_id"] == evidence["id"]


@pytest.mark.asyncio
async def test_asset_re_evaluation_measures_selected_hash_without_provider_lama_or_upload(
    factory_config, database, objects, make_attempt
) -> None:
    image_provider = FakeProvider("gemini-3-pro-image", [])
    api = FakeApi()
    factory = Factory(
        factory_config,
        database=database,
        objects=objects,
        providers=FakeRegistry({"image_generator": image_provider}),  # type: ignore[arg-type]
        api=api,  # type: ignore[arg-type]
    )
    configure_skill(factory)
    factory.pinned_gates = lambda _: factory.gates  # type: ignore[method-assign]
    parent = make_attempt(stage=Stage.ASSETS, disposition=Disposition.MACHINE_REJECTED)
    plan = _asset_plan()
    add_artifact(
        database,
        objects,
        parent["id"],
        stage=Stage.AUTHOR,
        kind=ArtifactKind.IMPLEMENTATION_PLAN,
        value=plan.model_dump_json().encode(),
        media_type="application/json",
    )
    exact = _exact_png((61, 122, 183))
    selected = factory._store_bytes_artifact(
        parent,
        Stage.ASSETS,
        ArtifactKind.TEXTURE_VARIANT,
        exact,
        "image/png",
        metadata={
            "asset_index": 0,
            "generation": 0,
            "phase": "forge_ladder_candidate",
            "content_ref": "sha256:" + hashlib.sha256(exact).hexdigest(),
            "width_px": 256,
            "height_px": 64,
            "bytes": len(exact),
            "texels_per_cell": 64,
            "uploaded": False,
            "gate_accepted": False,
        },
        occurrence_key="asset:0:generation:0:variant:0",
    )
    review = ReviewService(
        database,
        factory.journal,
        api,  # type: ignore[arg-type]
        factory.persistence,
        factory.behavior_snapshot,
    )
    reevaluated = await review.re_evaluate(
        attempt_id=parent["id"],
        artifact_id=selected["id"],
        feedback="measure these exact bytes with the promoted gates",
        actor="human:alex",
    )
    linked = reevaluated["artifact"]
    measured: list[bytes] = []

    def inspect(asset: AssetPlan, variants: tuple[ForgeVariant, ...]):
        assert asset == plan.asset_plan[0]
        measured.extend(item.data for item in variants)
        assert [item.content_ref for item in variants] == [selected["content_hash"]]
        return (
            GateResult(
                gate="asset_exact_hash",
                gate_version="promoted-exact-v2",
                blocking=True,
                verdict=GateVerdict.PASS,
                measurements={"content_refs": [selected["content_hash"]]},
            ),
        )

    factory.assets.re_evaluate_exact = inspect  # type: ignore[method-assign]
    factory.assets.forge = lambda *_args, **_kwargs: pytest.fail("re-evaluation must not invoke forge/LaMa")  # type: ignore[method-assign]

    result = await factory._build_assets(reevaluated["attempt"])

    assert result["stage"] == Stage.COMPLETE
    assert result["disposition"] == Disposition.NEEDS_HUMAN
    assert result["review_kind"] == "re_evaluation"
    assert measured == [exact]
    assert linked["content_hash"] == selected["content_hash"]
    assert linked["object_ref"] == selected["object_ref"]
    assert image_provider.calls == []
    assert api.create_calls == []
    assert api.append_calls == []
    assert api.publication_request_calls == []
    assert api.publish_calls == []
    evaluations = database.evaluations_for_attempt(result["id"], reveal=True)
    assert len(evaluations) == 1
    assert evaluations[0]["artifact_id"] == linked["id"]
    assert json.loads(evaluations[0]["measurements_json"])["content_refs"] == [selected["content_hash"]]
    await factory.close()


@pytest.mark.asyncio
async def test_manifest_and_failed_repair_re_evaluation_measure_only_their_retained_pixels(
    factory_config, database, objects, make_attempt
) -> None:
    api = FakeApi()
    factory = Factory(factory_config, database=database, objects=objects, api=api)  # type: ignore[arg-type]
    configure_skill(factory)
    factory.pinned_gates = lambda _: factory.gates  # type: ignore[method-assign]
    parent = make_attempt(stage=Stage.ASSETS, disposition=Disposition.MACHINE_REJECTED)
    plan = _asset_plan()
    add_artifact(
        database,
        objects,
        parent["id"],
        stage=Stage.AUTHOR,
        kind=ArtifactKind.IMPLEMENTATION_PLAN,
        value=plan.model_dump_json().encode(),
        media_type="application/json",
    )
    normalized_bytes = _exact_png((5, 15, 25))
    normalized = factory._store_bytes_artifact(
        parent,
        Stage.ASSETS,
        ArtifactKind.SOURCE_ASSET,
        normalized_bytes,
        "image/png",
        metadata={"asset_index": 0, "generation": 0, "phase": "normalized_forge_input"},
        occurrence_key="asset:0:generation:0:normalized-input",
    )
    rung_bytes = _exact_png((35, 45, 55))
    rung_ref = "sha256:" + hashlib.sha256(rung_bytes).hexdigest()
    rungs = tuple(
        ForgeVariant(
            content_ref=rung_ref,
            url=f"/api/textures/variants/{rung_ref}.png",
            width_px=256,
            height_px=64,
            bytes=len(rung_bytes),
            texels_per_cell=texels,
            data=rung_bytes,
        )
        for texels in (64, 32)
    )
    failed_bytes = _exact_png((199, 31, 87))
    failed_ref = "sha256:" + hashlib.sha256(failed_bytes).hexdigest()
    failed = ForgeVariant(
        content_ref=failed_ref,
        url=f"/factory/rejected/{failed_ref}.png",
        width_px=256,
        height_px=64,
        bytes=len(failed_bytes),
        texels_per_cell=64,
        data=failed_bytes,
    )
    bundle = ForgeBundle(
        manifest={
            "schema_version": 1,
            "descriptor": {
                "kind": "coat",
                "body_columns": 4,
                "frame_rows": None,
                "variants": [
                    {
                        "content_ref": rung.content_ref,
                        "url": rung.url,
                        "width_px": rung.width_px,
                        "height_px": rung.height_px,
                        "bytes": rung.bytes,
                        "texels_per_cell": rung.texels_per_cell,
                    }
                    for rung in rungs
                ],
            },
        },
        descriptor={},
        variants=rungs,
        gate_results=(
            GateResult(
                gate="detail_density",
                gate_version="old-v1",
                blocking=True,
                verdict=GateVerdict.FAIL,
                reasons=["old gate rejected this ladder"],
            ),
        ),
        repaired=True,
        normalized_source=normalized_bytes,
        repair_methods=("roll:x",),
        rejected_output=failed,
    )
    evidence, _accepted = factory._retain_forge_bundle(
        attempt=parent,
        asset_index=0,
        generation=0,
        bundle=bundle,
        normalized=normalized,
        provider_artifact_id=None,
    )
    failed_artifact = next(
        row
        for row in database.artifacts_for_attempt(parent["id"], kind=ArtifactKind.SOURCE_ASSET)
        if json.loads(row["metadata_json"]).get("phase") == "forge_rejected_output"
    )
    review = ReviewService(
        database,
        factory.journal,
        api,  # type: ignore[arg-type]
        factory.persistence,
        factory.behavior_snapshot,
    )
    captured: list[list[bytes]] = []

    def inspect(_asset: AssetPlan, variants: tuple[ForgeVariant, ...]):
        captured.append([item.data for item in variants])
        return (
            GateResult(
                gate="asset_exact_hash",
                gate_version="current-v2",
                blocking=True,
                verdict=GateVerdict.PASS,
            ),
        )

    factory.assets.re_evaluate_exact = inspect  # type: ignore[method-assign]
    factory.assets.forge = lambda *_args, **_kwargs: pytest.fail("exact re-evaluation must not run LaMa")  # type: ignore[method-assign]
    for target in (evidence, failed_artifact):
        child = (
            await review.re_evaluate(
                attempt_id=parent["id"],
                artifact_id=target["id"],
                feedback="inspect retained pixels only",
                actor="human:alex",
            )
        )["attempt"]
        result = await factory._build_assets(child)
        assert result["stage"] == Stage.COMPLETE
        assert result["disposition"] == Disposition.NEEDS_HUMAN

    assert captured == [[rung_bytes, rung_bytes], [failed_bytes]]
    assert api.create_calls == []
    assert api.append_calls == []
    await factory.close()


@pytest.mark.asyncio
async def test_asset_byte_gates_use_attempt_pinned_runner_not_checkout_runner(
    factory_config, database, objects, make_attempt
) -> None:
    factory = Factory(factory_config, database=database, objects=objects)
    pinned = PassingGates()
    factory.pinned_gates = lambda _: pinned  # type: ignore[method-assign]
    attempt = make_attempt(stage=Stage.ASSETS)

    blocked = await factory._build_assets(attempt)

    assert blocked["disposition"] == Disposition.BLOCKED
    assert factory.assets.runtime_gates is pinned
    assert factory.assets.gates is pinned.manifest
    await factory.close()


def test_ownership_gate_requires_exact_authenticated_upload_authority(
    factory_config, database, objects, make_attempt
) -> None:
    factory = Factory(factory_config, database=database, objects=objects)
    attempt = make_attempt(stage=Stage.BUILD_GATE)
    ref = "sha256:" + "a" * 64
    plan = ImplementationPlan(
        path="texture",
        rationale="A painterly texture is required.",
        fidelity_features=["painted scales"],
        layer_plan=["fallback", "texture"],
        asset_plan=[
            AssetPlan(
                kind="coat",
                natural_length_cells=4,
                frames=1,
                texels_per_cell=64,
                fit="tile",
                prompt="Paint exact scales.",
            )
        ],
        animation_plan=[],
        required_wrap_axes=["x"],
        risks=["seams"],
        design_guidelines={
            "artistic_direction": "One painterly scale direction.",
            "concept_twist": "Original scale treatment.",
            "structure": "pattern",
            "body_strategy": "Reads at four cells and across growth, turns, and overlap.",
            "head_zone": "light_field_dark_core",
            "asset_strategy": "The exact coat tiles seamlessly on x.",
        },
    )
    document = {
        "textures": [
            {
                "name": "coat",
                "kind": "coat",
                "descriptor": {"variants": [{"content_ref": ref}]},
            }
        ]
    }

    missing = factory._ownership_gate(attempt, document, plan, factory.gates)
    assert missing.verdict == GateVerdict.FAIL
    assert "no successful authenticated forge upload" in missing.reasons[0]

    operation, _ = database.begin_operation(
        attempt_id=attempt["id"],
        stage=Stage.ASSETS,
        idempotency_key="owned-upload",
        side_effect="upload_exact_forge_ladder",
        provider_role="snaketron_api",
        request_hash="b" * 64,
        cost_reserved_micros=0,
    )
    operation = database.transition_operation(operation["id"], OperationStatus.INTENT, OperationStatus.RUNNING)
    operation = database.transition_operation(
        operation["id"],
        OperationStatus.RUNNING,
        OperationStatus.SUCCEEDED,
        resolved_model="snaketron-api",
        result_hash="sha256:" + "c" * 64,
        cost_charged_micros=0,
    )
    add_artifact(
        database,
        objects,
        attempt["id"],
        stage=Stage.ASSETS,
        kind=ArtifactKind.FORGE_MANIFEST,
        value=canonical_json({"descriptor": {"variants": [{"content_ref": ref}]}}).encode(),
        media_type="application/json",
        metadata={"uploaded": True, "operation_id": operation["id"]},
    )

    passed = factory._ownership_gate(attempt, document, plan, factory.gates)
    assert passed.verdict == GateVerdict.PASS
    assert passed.measurements["document_content_refs"] == [ref]


@pytest.mark.asyncio
async def test_ideation_ranks_against_published_and_rejected_feedback_before_image_spend(
    factory_config, database, objects
) -> None:
    retained = [
        ("Published Prism", Disposition.PUBLISHED, "Keep the tiny-cell contrast."),
        ("Rejected River", Disposition.HUMAN_REJECTED, "The blue river motif felt derivative."),
        ("Machine Moss", Disposition.MACHINE_REJECTED, ""),
    ]
    for index, (name, disposition, feedback) in enumerate(retained):
        concept = database.create_concept(
            name=name,
            brief=f"A retained {name.lower()} snake direction with enough descriptive detail.",
            seed=f"retained-{index}",
            source="test",
            tags=["retained", name.split()[1].lower()],
        )
        prior = database.create_attempt(
            concept_id=concept["id"],
            purpose=Purpose.PRODUCTION,
            stage=Stage.COMPLETE,
            idempotency_key=f"retained-{index}",
            behavior={},
            direction_sha="d",
            skill_sha="s",
            capability_sha="c",
            gate_sha="g",
            model_config_sha="m",
        )
        prior = database.update_attempt(prior["id"], prior["version"], disposition=disposition)
        if feedback:
            database.add_human_decision(
                artifact_id=None,
                attempt_id=prior["id"],
                action="build_quality_label",
                feedback=feedback,
                tags=["outcome:reject"],
                actor=f"human:reviewer-{index}",
                attempt_version=prior["version"],
            )

    smart = FakeProvider(
        "gemini-3.7-flash",
        [
            ConceptProposal(
                name="Another River",
                brief="A blue river motif that closely repeats an already rejected retained direction.",
                tags=["river", "blue"],
                seed="low-ranked-river",
                palette_intent="blue",
                motion_intent="flow",
                implementation_hint="layers",
                implementation_rationale="Procedural bands can express the flow.",
                novelty_score=0.1,
                direction_score=0.2,
                novelty_rationale="This proposal is knowingly close to the rejected river example.",
            )
        ],
    )
    factory = Factory(
        factory_config,
        database=database,
        objects=objects,
        providers=FakeRegistry({"smart_text": smart}),  # type: ignore[arg-type]
    )
    configure_skill(factory)
    attempt = factory._create_seed_attempt()
    updated = await factory._ideate(attempt)

    assert updated["stage"] == Stage.COMPLETE
    assert updated["disposition"] == Disposition.MACHINE_REJECTED
    request = json.loads(smart.calls[0]["prompt"])
    by_name = {item["name"]: item for item in request["retained_concepts"]}
    assert by_name["Published Prism"]["disposition"] == Disposition.PUBLISHED
    assert by_name["Rejected River"]["disposition"] == Disposition.HUMAN_REJECTED
    assert by_name["Rejected River"]["human_feedback"] == ["The blue river motif felt derivative."]
    brief = database.artifacts_for_attempt(attempt["id"], kind=ArtifactKind.CONCEPT_BRIEF)[0]
    payload = factory.persistence.load_json(brief["object_ref"])
    assert payload["ranking"]["selected_for_prototype"] is False
    assert payload["ranking"]["context_concepts"] == 3
    evaluation = database.evaluations_for_attempt(attempt["id"], reveal=True)[0]
    assert evaluation["gate_name"] == "concept_rank"
    assert evaluation["verdict"] == GateVerdict.MACHINE_REJECTED

    review = ReviewService(
        database,
        factory.journal,
        factory.api,
        factory.persistence,
        factory.behavior_snapshot,
    )
    retried = (
        await review.retry(
            attempt_id=updated["id"],
            from_stage="prototype",
            feedback="Keep the idea but replace the river with a novel stained-glass rhythm.",
            actor="human:alex",
        )
    )["attempt"]
    assert retried["stage"] == Stage.PROTOTYPE
    assert retried["parent_attempt_id"] == updated["id"]
    await factory.close()


@pytest.mark.asyncio
async def test_reject_annotation_flows_through_lineage_into_a_later_retry(
    factory_config, database, objects, make_attempt
) -> None:
    factory = Factory(factory_config, database=database, objects=objects)
    parent = make_attempt(stage=Stage.PROTOTYPE_REVIEW, disposition=Disposition.MACHINE_REJECTED)
    artifact = add_artifact(
        database,
        objects,
        parent["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        value=b"rejected direction pixels",
    )
    review = ReviewService(
        database,
        factory.journal,
        factory.api,
        factory.persistence,
        lambda: {
            "direction_sha": "a" * 64,
            "skill_sha": "b" * 64,
            "capability_sha": "c" * 64,
            "gate_sha": "d" * 64,
            "model_config_sha": "e" * 64,
        },
    )
    feedback = "Preserve the clean head shape, but remove the noisy body bands."
    review.annotate_reject(
        attempt_id=parent["id"],
        artifact_id=artifact["id"],
        content_hash=artifact["content_hash"],
        feedback=feedback,
        tags=["readability"],
        actor="human:alex",
        idempotency_key="annotate-before-retry",
    )
    child = (
        await review.retry(
            attempt_id=parent["id"],
            from_stage="prototype",
            feedback="Try this retained direction again.",
            actor="human:alex",
            idempotency_key="retry-after-annotation",
        )
    )["attempt"]

    assert feedback in factory._lineage_feedback(child)
    assert feedback in factory._stage_feedback(child, "prototype")
    await factory.close()


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
async def test_duplicate_prototype_bytes_complete_every_paid_slot_without_duplicate_storage(
    factory_config, database, objects
) -> None:
    duplicate = {"image": b"identical provider output", "media_type": "image/png", "text": "done"}
    image = FakeProvider(
        "gemini-3-pro-image",
        [duplicate.copy() for _ in range(factory_config.budgets.prototypes_per_attempt)],
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

    advanced = await factory._prototype(attempt)

    assert advanced["stage"] == Stage.PROTOTYPE_TRIAGE
    assert len(image.calls) == factory_config.budgets.prototypes_per_attempt
    prototypes = database.artifacts_for_attempt(attempt["id"], stage=Stage.PROTOTYPE, kind=ArtifactKind.PROTOTYPE)
    manifests = database.artifacts_for_attempt(
        attempt["id"], stage=Stage.PROTOTYPE, kind=ArtifactKind.PROTOTYPE_MANIFEST
    )
    assert len(prototypes) == 1
    assert len(manifests) == factory_config.budgets.prototypes_per_attempt
    manifest_metadata = [json.loads(item["metadata_json"]) for item in manifests]
    assert {item["prototype_index"] for item in manifest_metadata} == set(
        range(factory_config.budgets.prototypes_per_attempt)
    )
    assert {item["image_artifact_id"] for item in manifest_metadata} == {prototypes[0]["id"]}
    assert sum(item["duplicate_of_index"] is not None for item in manifest_metadata) == (
        factory_config.budgets.prototypes_per_attempt - 1
    )
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
    current_contract = factory.behavior_snapshot()
    manifest = add_artifact(
        database,
        objects,
        parent["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE_MANIFEST,
        value=json.dumps(
            {
                "image_sha256": prototype["content_hash"],
                "design_guidelines_sha256": current_contract["design_guidelines_sha"],
                "prototype_geometry_sha256": current_contract["prototype_geometry_sha"],
                "prototype_guide_sha256": current_contract["prototype_guide_sha"],
            }
        ).encode(),
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
    reevaluated = await review.re_evaluate(
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

    review.label(
        attempt_id=child["id"],
        artifact_id=linked["id"],
        kind="prototype_label",
        outcome="accept",
        feedback="blind label the re-evaluated exact pixels",
        tags=[],
        actor="human:alex",
    )
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
async def test_prototype_safety_flag_is_a_retained_non_overrideable_blocking_gate(
    factory_config, database, objects, make_attempt
) -> None:
    judge = FakeProvider(
        "gemini-3.7-flash",
        [
            VisualJudgment(
                verdict="candidate",
                reasons=["The visual direction is readable but contains a protected mark."],
                fidelity=0.9,
                readability=0.9,
                role_clarity=0.9,
                animation_quality=0.8,
                craft=0.9,
                review_flags=["protected_mark"],
            )
        ],
    )
    factory = Factory(
        factory_config,
        database=database,
        objects=objects,
        providers=FakeRegistry({"visual_judge": judge}),  # type: ignore[arg-type]
    )
    attempt = make_attempt(stage=Stage.PROTOTYPE_TRIAGE)
    artifact = add_artifact(
        database,
        objects,
        attempt["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        value=b"flagged prototype pixels",
        media_type="image/png",
    )

    routed = await factory._prototype_triage(attempt)

    assert routed["stage"] == Stage.PROTOTYPE_REVIEW
    assert routed["disposition"] == Disposition.MACHINE_REJECTED
    assert routed["review_kind"] is None
    evaluations = database.evaluations_for_attempt(attempt["id"], reveal=True)
    safety = next(item for item in evaluations if item["gate_name"] == "safety_ip")
    assert safety["artifact_id"] == artifact["id"]
    assert safety["blocking"] == 1
    assert safety["verdict"] == GateVerdict.FAIL
    assert json.loads(safety["measurements_json"])["review_flags"] == ["protected_mark"]
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
    document_result = await review.re_evaluate(
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
    assert {item["gate_name"] for item in hard_results} == {"document_schema", "ownership"}
    assert {item["artifact_id"] for item in hard_results} == {document_result["artifact"]["id"]}
    assert {item["evaluator"] for item in hard_results} == {"deterministic"}
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
    visual_result = await review.re_evaluate(
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
    assert {item["gate_name"] for item in visual_evaluations} == {"visual_fidelity", "safety_ip"}
    assert {item["artifact_id"] for item in visual_evaluations} == {visual_result["artifact"]["id"]}
    visual_fidelity = next(item for item in visual_evaluations if item["gate_name"] == "visual_fidelity")
    assert visual_fidelity["verdict"] == GateVerdict.CANDIDATE
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
                novelty_score=0.92,
                direction_score=0.94,
                novelty_rationale="It differs from retained water skins through its refracted geometric rhythm.",
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
    worker = FakeWorker([author_result()], model="worker-test")
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
    behavior = json.loads(attempt["behavior_json"])
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
    manifest_artifact = database.artifacts_for_attempt(
        attempt["id"], stage=Stage.PROTOTYPE, kind=ArtifactKind.PROTOTYPE_MANIFEST
    )[0]
    manifest = json.loads(objects.get(manifest_artifact["object_ref"]))
    assert manifest["design_guidelines_sha256"] == behavior["design_guidelines_sha"]
    assert manifest["prototype_geometry_sha256"] == behavior["prototype_geometry_sha"]
    assert manifest["prototype_guide_sha256"] == behavior["prototype_guide_sha"]
    assert judge.calls[0]["images"] == [
        ("image/png", objects.get(behavior["prototype_guide_ref"])),
        (prototype["media_type"], objects.get(prototype["object_ref"])),
    ]
    assert judge.calls[0]["system"].startswith("The first image is the pinned blank Snaketron geometry guide")
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
    assert request.inline_artifacts["prototype_geometry_guide"].content_hash == (
        f"sha256:{behavior['prototype_guide_sha']}"
    )
    assert request.artifact_refs["prototype_geometry"] == behavior["prototype_geometry_ref"]
    assert request.artifact_refs["prototype_geometry_guide"] == behavior["prototype_guide_ref"]
    assert request.authoring_inputs["design_guidelines"] == {
        "sha256": behavior["design_guidelines_sha"],
        "text": objects.get(behavior["design_guidelines_ref"]).decode("utf-8"),
    }
    assert request.authoring_inputs["prototype_geometry"]["contract_sha256"] == behavior["prototype_geometry_sha"]
    assert request.authoring_inputs["prototype_geometry"]["guide_sha256"] == behavior["prototype_guide_sha"]
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

    contact_sheet = database.artifacts_for_attempt(built["id"], stage=Stage.RENDER, kind=ArtifactKind.CONTACT_SHEET)[-1]
    review.label(
        attempt_id=built["id"],
        artifact_id=contact_sheet["id"],
        kind="build_quality_label",
        outcome="accept",
        feedback="blind label the exact completed build",
        tags=[],
        actor="human:alex",
    )
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
@pytest.mark.parametrize(
    ("role", "resolved_model"),
    [
        ("smart_text", "claude-fallback"),
        ("visual_judge", "gemini-3.6-flash"),
        ("image_generator", "gemini-2.5-flash-image"),
        ("task_worker", "different-worker"),
    ],
)
async def test_provider_result_model_mismatch_fails_before_success_or_consumption(
    factory_config,
    database,
    objects,
    role: str,
    resolved_model: str,
) -> None:
    factory = Factory(factory_config, database=database, objects=objects)
    configure_skill(factory)
    attempt = factory._create_seed_attempt()

    with pytest.raises(ProviderError, match="violates pinned identity") as caught:
        await factory._provider_call(
            attempt=attempt,
            stage=Stage.CONCEPT,
            key=f"{attempt['id']}:model-mismatch:{role}",
            role=role,
            side_effect="model_identity_probe",
            request={"role": role},
            invoke=lambda: ProviderResult(
                value={"must_not": "be consumed"},
                request_id=f"request-{role}",
                resolved_model=resolved_model,
            ),
        )

    assert caught.value.kind == ProviderFailureKind.INVALID_OUTPUT
    with database.connect() as connection:
        operation = dict(
            connection.execute(
                "SELECT * FROM operation WHERE idempotency_key=?",
                (f"{attempt['id']}:model-mismatch:{role}",),
            ).fetchone()
        )
    assert operation["status"] == OperationStatus.FAILED_TERMINAL
    assert operation["resolved_model"] == resolved_model
    assert operation["result_hash"].startswith("sha256:")
    assert json.loads(objects.get(operation["result_hash"])) == {"must_not": "be consumed"}
    assert json.loads(operation["metadata_json"])["quarantined"] is True
    assert json.loads(operation["failure_json"])["quarantined_result_hash"] == operation["result_hash"]
    await factory.close()


@pytest.mark.asyncio
async def test_authenticated_recovered_result_cannot_bypass_attempt_model_pin(
    factory_config, database, objects, make_attempt
) -> None:
    factory = Factory(factory_config, database=database, objects=objects)
    attempt = make_attempt(stage=Stage.CONCEPT)
    request = {"prompt": "recover exact structured output"}

    with pytest.raises(ProviderError):
        await factory.journal.run_provider(
            attempt_id=attempt["id"],
            stage=Stage.CONCEPT,
            idempotency_key="recovered-wrong-model",
            side_effect="generate_concept",
            provider_role="smart_text",
            request=request,
            reserve_micros=100,
            metadata=retained_request_metadata(objects, request),
            invoke=lambda: (_ for _ in ()).throw(
                ProviderError(
                    ProviderFailureKind.UNKNOWN_OUTCOME,
                    "accepted before response loss",
                    outcome_known=False,
                )
            ),
        )
    operation = database.unresolved_operations()[0]
    recovered = objects.put(json.dumps({"fallback": "must remain quarantined"}).encode())
    database.resolve_operation(
        operation_id=operation["id"],
        resolution="executed_result_recovered",
        evidence_ref="provider:audit:wrong-model",
        result_hash=recovered.uri,
        resolved_model="claude-fallback",
        provider_request_id="fallback-request",
        actor="human:operator",
    )

    invoked = False

    def invoke() -> ProviderResult:
        nonlocal invoked
        invoked = True
        return ProviderResult(value={"wrong": True}, resolved_model="gemini-3.7-flash")

    with pytest.raises(ProviderError, match="violates pinned identity") as caught:
        await factory._provider_call(
            attempt=database.get_attempt(attempt["id"]),
            stage=Stage.CONCEPT,
            key="recovered-wrong-model",
            role="smart_text",
            side_effect="generate_concept",
            request=request,
            invoke=invoke,
        )
    assert caught.value.halt_generation is True
    assert invoked is False
    assert objects.get(recovered.uri) == json.dumps({"fallback": "must remain quarantined"}).encode()
    terminal = database.get_operation(operation["id"])
    assert terminal["status"] == OperationStatus.FAILED_TERMINAL
    failure = json.loads(terminal["failure_json"])
    assert failure["quarantined_result_hash"] == recovered.uri
    assert failure["recovery_evidence_ref"] == "provider:audit:wrong-model"
    assert "will never be replayed" in failure["operator_action"]
    await factory.close()


@pytest.mark.asyncio
async def test_authenticated_recovered_structured_result_is_terminally_quarantined_before_consumption(
    factory_config, database, objects, make_attempt
) -> None:
    factory = Factory(factory_config, database=database, objects=objects)
    attempt = make_attempt(stage=Stage.CONCEPT)
    request = {"prompt": "recover a concept"}

    with pytest.raises(ProviderError):
        await factory.journal.run_provider(
            attempt_id=attempt["id"],
            stage=Stage.CONCEPT,
            idempotency_key="recovered-wrong-schema",
            side_effect="generate_concept",
            provider_role="smart_text",
            request=request,
            reserve_micros=100,
            metadata=retained_request_metadata(objects, request),
            invoke=lambda: (_ for _ in ()).throw(
                ProviderError(
                    ProviderFailureKind.UNKNOWN_OUTCOME,
                    "accepted before response loss",
                    outcome_known=False,
                )
            ),
        )
    operation = database.unresolved_operations()[0]
    recovered = objects.put(json.dumps({"name": "only one field"}).encode())
    database.resolve_operation(
        operation_id=operation["id"],
        resolution="executed_result_recovered",
        evidence_ref="provider:audit:wrong-schema",
        result_hash=recovered.uri,
        resolved_model="gemini-3.7-flash",
        actor="human:operator",
    )

    with pytest.raises(ProviderError, match="structured contract") as caught:
        await factory._provider_call(
            attempt=database.get_attempt(attempt["id"]),
            stage=Stage.CONCEPT,
            key="recovered-wrong-schema",
            role="smart_text",
            side_effect="generate_concept",
            request=request,
            invoke=lambda: pytest.fail("invalid recovery must not call the provider"),
        )
    assert caught.value.kind == ProviderFailureKind.INVALID_OUTPUT
    assert caught.value.halt_generation is True
    terminal = database.get_operation(operation["id"])
    assert terminal["status"] == OperationStatus.FAILED_TERMINAL
    assert objects.get(recovered.uri) == json.dumps({"name": "only one field"}).encode()

    # The bad semantic success is no longer replayable on a later tick. The
    # exact evidence remains, and the journal reports one terminal boundary.
    with pytest.raises(ExistingOperation, match="already terminal"):
        await factory._provider_call(
            attempt=database.get_attempt(attempt["id"]),
            stage=Stage.CONCEPT,
            key="recovered-wrong-schema",
            role="smart_text",
            side_effect="generate_concept",
            request=request,
            invoke=lambda: pytest.fail("terminal recovery must not call the provider"),
        )
    await factory.close()


@pytest.mark.asyncio
async def test_authenticated_recovered_corrupt_image_is_terminally_quarantined_before_consumption(
    factory_config, database, objects, make_attempt
) -> None:
    factory = Factory(factory_config, database=database, objects=objects)
    attempt = make_attempt(stage=Stage.PROTOTYPE)
    request = {"prompt": "recover exact pixels"}

    with pytest.raises(ProviderError):
        await factory.journal.run_provider(
            attempt_id=attempt["id"],
            stage=Stage.PROTOTYPE,
            idempotency_key="recovered-corrupt-image",
            side_effect="generate_prototype_image",
            provider_role="image_generator",
            request=request,
            reserve_micros=100,
            metadata=retained_request_metadata(objects, request),
            invoke=lambda: (_ for _ in ()).throw(
                ProviderError(
                    ProviderFailureKind.UNKNOWN_OUTCOME,
                    "accepted before response loss",
                    outcome_known=False,
                )
            ),
        )
    operation = database.unresolved_operations()[0]
    corrupt = b"\x89PNG\r\n\x1a\ntruncated-paid-response"
    recovered = objects.put(corrupt)
    database.resolve_operation(
        operation_id=operation["id"],
        resolution="executed_result_recovered",
        evidence_ref="provider:audit:corrupt-image",
        result_hash=recovered.uri,
        resolved_model="gemini-3-pro-image",
        media_type="image/png",
        actor="human:operator",
    )

    with pytest.raises(ProviderError, match="exact decode validation"):
        await factory._provider_call(
            attempt=database.get_attempt(attempt["id"]),
            stage=Stage.PROTOTYPE,
            key="recovered-corrupt-image",
            role="image_generator",
            side_effect="generate_prototype_image",
            request=request,
            invoke=lambda: pytest.fail("corrupt recovery must not call the provider"),
        )
    terminal = database.get_operation(operation["id"])
    assert terminal["status"] == OperationStatus.FAILED_TERMINAL
    assert objects.get(recovered.uri) == corrupt
    assert json.loads(terminal["failure_json"])["quarantined_result_hash"] == recovered.uri
    await factory.close()


@pytest.mark.asyncio
async def test_recovered_service_publication_request_requires_authenticated_exact_server_readback(
    factory_config, database, objects, make_attempt
) -> None:
    api = FakeApi()
    factory = Factory(factory_config, database=database, objects=objects, api=api)  # type: ignore[arg-type]
    attempt = make_attempt(stage=Stage.BUILD_TRIAGE)
    request = {
        "skin_id": "skin-service-recovery",
        "revision": 3,
        "content_ref": "sha256:" + "3" * 64,
    }
    with pytest.raises(ProviderError):
        await factory._provider_call(
            attempt=attempt,
            stage=Stage.BUILD_TRIAGE,
            key="service-request-recovery",
            role="snaketron_api",
            side_effect="request_exact_publication_review",
            request=request,
            invoke=lambda: (_ for _ in ()).throw(
                ProviderError(
                    ProviderFailureKind.UNKNOWN_OUTCOME,
                    "request response was lost",
                    outcome_known=False,
                )
            ),
        )
    operation = database.unresolved_operations()[0]
    recovered = objects.put(b'{"accepted":true}')
    database.resolve_operation(
        operation_id=operation["id"],
        resolution="executed_result_recovered",
        evidence_ref="provider:audit:pending-3",
        result_hash=recovered.uri,
        resolved_model="snaketron-api",
        actor="human:operator",
    )
    api.skin_authority = {
        "skinId": "skin-service-recovery",
        "publication": "private",
        "pendingRevision": 4,
    }

    with pytest.raises(ProviderError, match="authenticated Snaketron readback"):
        await factory._provider_call(
            attempt=database.get_attempt(attempt["id"]),
            stage=Stage.BUILD_TRIAGE,
            key="service-request-recovery",
            role="snaketron_api",
            side_effect="request_exact_publication_review",
            request=request,
            invoke=lambda: pytest.fail("recovered request must not invoke the provider"),
        )
    terminal = database.get_operation(operation["id"])
    assert terminal["status"] == OperationStatus.FAILED_TERMINAL
    assert objects.get(recovered.uri) == b'{"accepted":true}'
    await factory.close()


@pytest.mark.asyncio
async def test_first_allowed_resolved_alias_becomes_exact_attempt_role_pin(factory_config, database, objects) -> None:
    factory_config.models.smart_text.resolved_model_pattern = r"gemini-3\.7-flash(?:-[0-9]{8})?"
    factory = Factory(factory_config, database=database, objects=objects)
    configure_skill(factory)
    attempt = factory._create_seed_attempt()

    first_model = "gemini-3.7-flash-20260801"
    await factory._provider_call(
        attempt=attempt,
        stage=Stage.CONCEPT,
        key=f"{attempt['id']}:first-resolution",
        role="smart_text",
        side_effect="model_identity_probe",
        request={"probe": 1},
        invoke=lambda: ProviderResult(value={"ok": 1}, resolved_model=first_model),
    )
    with pytest.raises(ProviderError, match="changed within Attempt"):
        await factory._provider_call(
            attempt=database.get_attempt(attempt["id"]),
            stage=Stage.CONCEPT,
            key=f"{attempt['id']}:second-resolution",
            role="smart_text",
            side_effect="model_identity_probe",
            request={"probe": 2},
            invoke=lambda: ProviderResult(
                value={"ok": 2},
                resolved_model="gemini-3.7-flash-20260815",
            ),
        )
    await factory.close()


@pytest.mark.asyncio
async def test_provider_model_mismatch_durably_halts_new_generation_until_human_resume(
    factory_config, database, objects
) -> None:
    proposal = ConceptProposal(
        name="Fallback concept",
        brief="A detailed concept whose value must never be consumed from a fallback model.",
        tags=["fallback"],
        seed="fallback",
        palette_intent="bright",
        motion_intent="none",
        implementation_hint="layers",
        implementation_rationale="This payload exists only to prove model identity enforcement.",
        novelty_score=0.9,
        direction_score=0.9,
        novelty_rationale="The response is structurally valid but came from the wrong provider model.",
    )
    smart = FakeProvider("claude-fallback", [proposal])
    factory = Factory(
        factory_config,
        database=database,
        objects=objects,
        providers=FakeRegistry({"smart_text": smart}),  # type: ignore[arg-type]
    )
    configure_skill(factory)

    first = await factory.run_once()
    halted_attempt = database.list_gallery("blocked")[0]
    assert first["advanced"][-1]["state"] == Disposition.BLOCKED
    assert json.loads(halted_attempt["failure_json"])["program_halt"] == "provider_model_mismatch"
    with database.connect() as connection:
        concepts_before = connection.execute("SELECT count(*) FROM concept").fetchone()[0]

    second = await factory.run_once()
    assert second["halt"]["reason"] == f"program_halt:provider_model_mismatch:{halted_attempt['id']}"
    assert len(smart.calls) == 1
    with database.connect() as connection:
        assert connection.execute("SELECT count(*) FROM concept").fetchone()[0] == concepts_before

    resumed = database.resume_program_halt(
        attempt_id=halted_attempt["id"],
        actor="human:alex",
        reason="Provider configuration was inspected and corrected.",
    )
    assert resumed["action"] == "human_resume"
    assert factory._generation_halt() is None
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
        return ProviderResult(value={"ok": True}, resolved_model="gemini-3-pro-image", usage={"cost_micros": 0})

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
    output = io.BytesIO()
    Image.new("RGB", (3, 2), (10, 90, 180)).save(output, format="WEBP", lossless=True)
    exact = output.getvalue()

    operation, result = await factory._provider_call(
        attempt=attempt,
        stage=Stage.PROTOTYPE,
        key="webp-image",
        role="image_generator",
        side_effect="generate_prototype_image",
        request=request,
        invoke=lambda: ProviderResult(
            value={"image": exact, "media_type": "image/webp"},
            resolved_model="gemini-3-pro-image",
        ),
    )
    assert factory._image_result(operation, result) == (exact, "image/webp")
    assert json.loads(operation["metadata_json"])["result"] == {
        "decoded_format": "WEBP",
        "height_px": 2,
        "kind": "image",
        "media_type": "image/webp",
        "width_px": 3,
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
        return ProviderResult(value={"resumed": True}, resolved_model="gemini-3.7-flash")

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
            value={"image": _exact_png((22, 44, 66), width=4, height=4), "media_type": "image/png"},
            resolved_model="gemini-3-pro-image",
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


def test_generation_halts_at_configured_published_skin_target(factory_config, database, objects, make_attempt) -> None:
    factory_config.program.target_published_skins = 1
    factory = Factory(factory_config, database=database, objects=objects)

    assert factory._generation_halt() is None
    make_attempt(disposition=Disposition.PUBLISHED)
    assert factory._generation_halt() == "published_target_reached"


@pytest.mark.asyncio
async def test_wall_time_cap_is_rechecked_immediately_before_new_provider_spend(
    factory_config, database, objects, make_attempt, monkeypatch
) -> None:
    attempt = make_attempt(stage=Stage.PROTOTYPE)
    factory = Factory(factory_config, database=database, objects=objects)
    factory.config.models.image_generator.timeout_seconds = 600
    # The tick has not expired, but only the provider's nominal 600-second
    # timeout remains. Admission also needs one second to persist the result
    # and release the process lease, so this paid call must not start.
    factory._run_deadline = 610.0
    monkeypatch.setattr("snaketron_factory.factory.time.monotonic", lambda: 10.0)
    invoked = False

    def invoke() -> ProviderResult:
        nonlocal invoked
        invoked = True
        return ProviderResult(value={"never": "spent"}, resolved_model="gemini-3-pro-image")

    with pytest.raises(BudgetExceeded, match=r"requires 601\.000s"):
        await factory._provider_call(
            attempt=attempt,
            stage=Stage.PROTOTYPE,
            key="wall-cap-before-spend",
            role="image_generator",
            side_effect="generate_prototype_image",
            request={"prompt": "must remain unspent"},
            invoke=invoke,
        )

    assert invoked is False
    with database.connect() as connection:
        assert (
            connection.execute("SELECT 1 FROM operation WHERE idempotency_key='wall-cap-before-spend'").fetchone()
            is None
        )
    await factory.close()


@pytest.mark.asyncio
async def test_late_git_promotion_is_refused_before_operation_or_push(
    factory_config, database, objects, make_attempt, monkeypatch
) -> None:
    attempt = make_attempt(stage=Stage.COMPLETE, disposition=Disposition.EXPERIMENT_COMPLETE)
    factory = Factory(factory_config, database=database, objects=objects)
    factory.config.optimizer.promotion_timeout_seconds = 1200
    factory._run_deadline = 1240.0
    monkeypatch.setattr("snaketron_factory.factory.time.monotonic", lambda: 10.0)
    invoked = False

    def invoke() -> ProviderResult:
        nonlocal invoked
        invoked = True
        return ProviderResult(value={"sha": "never-pushed"}, resolved_model="git")

    with pytest.raises(BudgetExceeded, match=r"requires 1231\.000s"):
        await factory._provider_call(
            attempt=attempt,
            stage=Stage.COMPLETE,
            key="late-git-promotion",
            role="git_promotion",
            side_effect="promote_authoring_playbook",
            request={"candidate": "must remain local"},
            invoke=invoke,
        )

    assert invoked is False
    with database.connect() as connection:
        assert (
            connection.execute("SELECT 1 FROM operation WHERE idempotency_key='late-git-promotion'").fetchone() is None
        )
    await factory.close()


@pytest.mark.asyncio
async def test_full_prototype_inbox_pauses_active_retry_before_image_spend(
    factory_config, database, objects, make_attempt
) -> None:
    factory_config.budgets.max_pending_prototype_reviews = 1
    waiting = make_attempt(stage=Stage.PROTOTYPE_REVIEW, disposition=Disposition.NEEDS_HUMAN)
    database.update_attempt(waiting["id"], waiting["version"], review_kind="prototype")
    retry = make_attempt(stage=Stage.PROTOTYPE)
    image = FakeProvider("gemini-3-pro-image")
    factory = Factory(
        factory_config,
        database=database,
        objects=objects,
        providers=FakeRegistry({"image_generator": image}),  # type: ignore[arg-type]
    )

    report = await factory.run_once()

    assert report["halt"] == {"reason": "prototype_review_wip_cap"}
    assert report["advanced"] == []
    assert database.get_attempt(retry["id"])["stage"] == Stage.PROTOTYPE
    assert image.calls == []
    await factory.close()


@pytest.mark.asyncio
async def test_full_prototype_inbox_pauses_resumed_machine_triage(
    factory_config, database, objects, make_attempt
) -> None:
    factory_config.budgets.max_pending_prototype_reviews = 1
    waiting = make_attempt(stage=Stage.PROTOTYPE_REVIEW, disposition=Disposition.NEEDS_HUMAN)
    database.update_attempt(waiting["id"], waiting["version"], review_kind="prototype")
    active = make_attempt(stage=Stage.PROTOTYPE_TRIAGE)
    judge = FakeProvider("gemini-3.7-flash")
    factory = Factory(
        factory_config,
        database=database,
        objects=objects,
        providers=FakeRegistry({"visual_judge": judge}),  # type: ignore[arg-type]
    )

    report = await factory.run_once()

    assert report["halt"] == {"reason": "prototype_review_wip_cap"}
    assert database.get_attempt(active["id"])["stage"] == Stage.PROTOTYPE_TRIAGE
    assert judge.calls == []
    await factory.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "stage",
    [Stage.AUTHOR, Stage.ASSETS, Stage.BUILD_GATE, Stage.REGISTER, Stage.RENDER, Stage.BUILD_TRIAGE],
)
async def test_full_final_inbox_pauses_every_resumed_pre_review_stage(
    factory_config, database, objects, make_attempt, stage: Stage
) -> None:
    factory_config.budgets.max_pending_final_reviews = 1
    waiting = make_attempt(stage=Stage.FINAL_REVIEW, disposition=Disposition.NEEDS_HUMAN)
    database.update_attempt(waiting["id"], waiting["version"], review_kind="final")
    active = make_attempt(stage=stage)
    factory = Factory(factory_config, database=database, objects=objects)

    report = await factory.run_once()

    assert report["halt"] == {"reason": "final_review_wip_cap"}
    assert database.get_attempt(active["id"])["stage"] == stage
    assert report["advanced"] == []
    await factory.close()


def test_generation_halts_on_repeated_blocking_platform_gate(factory_config, database, objects, make_attempt) -> None:
    factory_config.halts.repeated_blocking_gate_limit = 3
    factory_config.halts.deterministic_failure_window = 6
    factory = Factory(factory_config, database=database, objects=objects)

    for index in range(3):
        attempt = make_attempt(stage=Stage.BUILD_GATE, disposition=Disposition.MACHINE_REJECTED)
        artifact = add_artifact(
            database,
            objects,
            attempt["id"],
            stage=Stage.BUILD_GATE,
            kind=ArtifactKind.SKIN_DOCUMENT,
            value=f'{{"fixture":{index}}}'.encode(),
            media_type="application/json",
        )
        database.add_evaluation(
            artifact_id=artifact["id"],
            attempt_id=attempt["id"],
            evaluator="deterministic",
            result=GateResult(
                gate="renderer_conformance",
                gate_version="test-v1",
                blocking=True,
                verdict=GateVerdict.FAIL,
                reasons=["same platform regression"],
            ),
        )

    assert factory._generation_halt() == "repeated_blocking_gate:renderer_conformance"
    detail = factory._generation_halt_detail()
    assert detail is not None and detail["acknowledgeable"] is True
    database.record_generation_resume(
        halt_key=detail["reason"],
        evidence_at=detail["evidence_at"],
        actor="human:platform-owner",
        reason="Renderer regression was repaired and its exact bundle was redeployed.",
    )
    assert factory._generation_halt() is None


def test_generation_halts_when_root_cause_repeats_after_skill_promotion(
    factory_config, database, objects, make_attempt
) -> None:
    factory_config.halts.repeated_root_cause_after_promotion_limit = 2
    factory = Factory(factory_config, database=database, objects=objects)
    database.set_active_behavior("author-skin", "refs/tags/skin-authoring-v2", "a" * 40)

    for index in range(2):
        attempt = make_attempt(stage=Stage.COMPLETE, disposition=Disposition.HUMAN_REJECTED)
        artifact = add_artifact(
            database,
            objects,
            attempt["id"],
            stage=Stage.COMPLETE,
            kind=ArtifactKind.CONTACT_SHEET,
            value=f"render-{index}".encode(),
            media_type="image/png",
        )
        decision = database.add_human_decision(
            artifact_id=artifact["id"],
            attempt_id=attempt["id"],
            action="build_quality_label",
            feedback="The tail still loses the approved silhouette.",
            tags=["tail"],
            actor=f"human:reviewer-{index}",
            attempt_version=attempt["version"],
        )
        database.add_feedback_route(
            decision_id=decision["id"],
            target="authoring_playbook",
            signature="tail-silhouette-loss",
            confidence=0.95,
            classifier_version="gemini-3.7-flash+rubric:test",
            evidence={"artifact_id": artifact["id"]},
        )

    assert factory._generation_halt() == "repeated_root_cause_after_promotion:tail-silhouette-loss"
    detail = factory._generation_halt_detail()
    assert detail is not None and detail["acknowledgeable"] is True
    database.record_generation_resume(
        halt_key=detail["reason"],
        evidence_at=detail["evidence_at"],
        actor="human:authoring-owner",
        reason="The ineffective promotion was reviewed and replaced.",
    )
    assert factory._generation_halt() is None


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
    assert result["disposition"] == Disposition.BLOCKED
    failure = json.loads(result["failure_json"])
    assert failure["program_halt"] == "required_browser_evidence"
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
