from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import TypeAdapter, ValidationError

from snaketron_factory.domain import (
    AssetPlan,
    ImplementationPlan,
    InputAuthorityEvidence,
    ModifierPlan,
    WorkerResult,
    current_worker_result_json_schema,
)
from snaketron_factory.skin_document import SkinDocument
from snaketron_factory.worker_validation import (
    WorkerContractError,
    _fallback_covers_image,
    _validate_modifier_layer,
    validate_plan_resource_limits,
    validate_worker_handoff,
)

REPO = Path(__file__).resolve().parents[2]


def _authority() -> InputAuthorityEvidence:
    return InputAuthorityEvidence(
        mode="approved_prototype",
        artifact_sha256="sha256:" + "a" * 64,
        authority_record_sha256="sha256:" + "b" * 64,
        human_approval_decision_id="decision-exact",
        selection_rationale=None,
        maximum_driver_action="register_private_revision",
    )


def _layer_result(authority: InputAuthorityEvidence | None = None) -> WorkerResult:
    document = json.loads((REPO / "skills/author-skin/templates/skin-v2.template.json").read_text())
    return WorkerResult(
        implementation_plan=ImplementationPlan(
            input_authority=authority or _authority(),
            path="layers",
            rationale="Procedural fixture.",
            design_guidelines={
                "artistic_direction": "One direction.",
                "concept_twist": "Original fixture.",
                "structure": "pattern",
                "body_strategy": "Reads at four cells, grows, and turns.",
                "head_zone": "light_field_dark_core",
                "asset_strategy": "No raster assets.",
            },
            common_period_ms=document["period_ms"],
            fidelity_features=["clear body"],
            layer_plan=["body"],
            asset_plan=[],
            modifier_plan=[],
            animation_plan=[],
            required_wrap_axes=[],
            risks=[],
        ),
        skin_document=document,
    )


def test_current_handoff_requires_trusted_authority() -> None:
    with pytest.raises(WorkerContractError, match="requires trusted input authority"):
        validate_worker_handoff(_layer_result(), {}, {})


def test_worker_authority_mismatch_is_rejected_before_asset_work() -> None:
    trusted = _authority().model_dump(mode="json")
    trusted["authority_record_sha256"] = "sha256:" + "c" * 64
    with pytest.raises(WorkerContractError, match="differs from trusted WorkerRequest authority"):
        validate_worker_handoff(_layer_result(), {}, {}, trusted_authority=trusted)


def test_factory_handoff_accepts_exact_draft_submission_authority() -> None:
    draft = InputAuthorityEvidence(
        mode="draft_submission",
        artifact_sha256="sha256:" + "a" * 64,
        authority_record_sha256="sha256:" + "b" * 64,
        human_approval_decision_id=None,
        selection_rationale="Exact candidate selected for private admin review.",
        maximum_driver_action="request_admin_review",
    )
    skill_files = {
        name: (REPO / "skills/author-skin" / name).read_text(encoding="utf-8")
        for name in (
            "schemas/implementation-plan.schema.json",
            "schemas/asset-request.schema.json",
        )
    }
    validate_worker_handoff(
        _layer_result(draft),
        skill_files,
        {},
        trusted_authority=draft.model_dump(mode="json"),
    )


def test_draft_author_cannot_ignore_a_nonempty_materialized_modifier_catalog() -> None:
    draft = InputAuthorityEvidence(
        mode="draft_submission",
        artifact_sha256="sha256:" + "a" * 64,
        authority_record_sha256="sha256:" + "b" * 64,
        human_approval_decision_id=None,
        selection_rationale="Exact candidate selected for private admin review.",
        maximum_driver_action="request_admin_review",
    )
    skill_files = {
        name: (REPO / "skills/author-skin" / name).read_text(encoding="utf-8")
        for name in (
            "schemas/implementation-plan.schema.json",
            "schemas/asset-request.schema.json",
        )
    }

    with pytest.raises(WorkerContractError, match="exact complete materialized modifier catalog"):
        validate_worker_handoff(
            _layer_result(draft),
            skill_files,
            {},
            trusted_authority=draft.model_dump(mode="json"),
            materialized_modifier_catalog={
                "schema_version": 1,
                "modifiers": [{"logical_key": "paid_clockwork_wave"}],
            },
            allow_direct_generation=False,
        )


def _modifier(anchor: str, mode: str, limit: float | None) -> tuple[AssetPlan, ModifierPlan]:
    asset = AssetPlan(
        kind="overlay",
        natural_length_cells=6,
        frames=1,
        desired_fps=None,
        texels_per_cell=64,
        raster_overhang_px=4,
        anchor=anchor,
        fit="clip",
        tile_phase_origin=None,
        fade="trailing" if anchor == "head" else "leading",
        transverse_edge_policy="fail_closed_transparent_effect",
        prompt="fixture",
    )
    modifier = ModifierPlan(
        asset_index=0,
        logical_key="fixture_component",
        component_key="H" if anchor == "head" else "T1",
        texture_name="fixture_texture",
        image_layer_name="Fixture image",
        fallback_layer_name="Fixture fallback",
        span_limit_mode=mode,
        span_limit_value=limit,
        source_mode="direct_generate",
        source_object_sha256=None,
        modifier_manifest_sha256=None,
        provenance_sha256="sha256:" + "d" * 64,
        license_id="owned-original",
        authorized_lineage_ids=["concept-fixture"],
        required_capabilities=["image_generator"],
        extraction=None,
        video=None,
    )
    return asset, modifier


def test_head_and_tail_modifier_claims_match_actual_layer_spans() -> None:
    head_asset, head_modifier = _modifier("head", "head_cells", 6)
    head_layer = {
        "span": {"from": "head", "natural": 6},
        "source": {
            "type": "image",
            "fit": "clip",
            "fade": {"lead_cells": 0, "trail_cells": 1, "steps": 2},
        },
    }
    _validate_modifier_layer(0, head_asset, head_modifier, head_layer)
    head_layer["span"]["natural"] = 7
    with pytest.raises(WorkerContractError, match="at most cell 6"):
        _validate_modifier_layer(0, head_asset, head_modifier, head_layer)

    tail_asset, tail_modifier = _modifier("tail", "tail_fraction", 0.5)
    tail_layer = {
        "span": {"from": {"fraction": {"fraction": 0.5}}},
        "source": {
            "type": "image",
            "fit": "clip",
            "fade": {"lead_cells": 1, "trail_cells": 0, "steps": 2},
        },
    }
    _validate_modifier_layer(0, tail_asset, tail_modifier, tail_layer)
    tail_layer["span"]["from"] = "whole"
    with pytest.raises(WorkerContractError, match="final planned fraction"):
        _validate_modifier_layer(0, tail_asset, tail_modifier, tail_layer)


def test_more_than_four_modest_assets_follow_pinned_capability_limit() -> None:
    assets = [
        AssetPlan(
            kind="overlay",
            natural_length_cells=2,
            frames=1,
            texels_per_cell=64,
            raster_overhang_px=0,
            anchor="whole",
            fit="clip",
            fade="none",
            transverse_edge_policy="not_applicable_opaque_fill",
            prompt=f"modest independent asset {index}",
        )
        for index in range(5)
    ]
    plan = type("Plan", (), {"asset_plan": assets})()
    limits = {
        "max_texture_refs": 8,
        "max_texture_dimension_px": 2048,
        "max_sprite_frame_rows": 120,
        "max_texture_decoded_bytes": 16_777_216,
        "max_skin_texture_decoded_bytes": 67_108_864,
        "max_skin_texture_compressed_bytes": 8_388_608,
    }
    validate_plan_resource_limits(plan, {"limits": limits})
    limits["max_texture_refs"] = 4
    with pytest.raises(WorkerContractError, match="pinned limit is 4"):
        validate_plan_resource_limits(plan, {"limits": limits})


def test_typed_skin_document_exposes_phase_origin_and_bounded_overhang() -> None:
    document = json.loads((REPO / "skills/author-skin/fixtures/texture/skin.skin.json").read_text(encoding="utf-8"))
    parsed = TypeAdapter(SkinDocument).validate_python(document)
    fit = parsed["layers"][2]["source"]["fit"]
    descriptor = parsed["textures"][0]["descriptor"]
    assert fit["phase_origin"] == "head"
    assert descriptor["raster_overhang_px"] == 0

    document["textures"][0]["descriptor"]["raster_overhang_px"] = 5
    with pytest.raises(ValidationError):
        TypeAdapter(SkinDocument).validate_python(document)


def test_factory_overhang_platform_gap_is_rejected_before_asset_provider_spend() -> None:
    plan = json.loads(
        (REPO / "skills/author-skin/fixtures/worker-drafts/implementation-plan.json").read_text(encoding="utf-8")
    )
    plan["asset_plan"][0]["raster_overhang_px"] = 4
    document = json.loads(
        (REPO / "skills/author-skin/fixtures/worker-drafts/skin-draft.valid.json").read_text(encoding="utf-8")
    )
    requests = json.loads(
        (REPO / "skills/author-skin/fixtures/worker-drafts/tool-requests.json").read_text(encoding="utf-8")
    )
    result = WorkerResult(
        implementation_plan=plan,
        skin_document=document,
        tool_requests=requests,
    )
    skill_files = {
        "schemas/implementation-plan.schema.json": (
            REPO / "skills/author-skin/schemas/implementation-plan.schema.json"
        ).read_text(encoding="utf-8"),
        "schemas/asset-request.schema.json": (REPO / "skills/author-skin/schemas/asset-request.schema.json").read_text(
            encoding="utf-8"
        ),
    }
    capabilities = {
        "limits": {
            "max_texture_refs": 8,
            "max_texture_dimension_px": 2048,
            "max_sprite_frame_rows": 120,
            "max_texture_decoded_bytes": 16_777_216,
            "max_skin_texture_decoded_bytes": 67_108_864,
            "max_skin_texture_compressed_bytes": 8_388_608,
        }
    }
    provider_calls: list[object] = []
    with pytest.raises(WorkerContractError, match="platform_gap: raster_overhang_px"):
        validate_worker_handoff(
            result,
            skill_files,
            capabilities,
            trusted_authority=plan["input_authority"],
        )
    assert provider_calls == []


def test_direct_modifier_cannot_claim_only_half_of_a_bound_object() -> None:
    _, modifier = _modifier("head", "head_cells", 6)
    payload = modifier.model_dump(mode="json")
    payload["source_object_sha256"] = "sha256:" + "e" * 64
    with pytest.raises(ValidationError, match="direct generation cannot claim"):
        ModifierPlan.model_validate(payload)


def test_video_modifier_binds_source_object_to_extracted_sheet() -> None:
    plan = json.loads(
        (REPO / "skills/author-skin/fixtures/modifier-workflow/implementation-plan.json").read_text(encoding="utf-8")
    )
    modifier = next(item for item in plan["modifier_plan"] if item["source_mode"] == "video_frames")
    modifier["video"]["extracted_sheet_sha256"] = "sha256:" + "f" * 64
    with pytest.raises(ValidationError, match="exact extracted sheet bytes"):
        ImplementationPlan.model_validate(plan)


def test_fallback_must_cover_normal_single_cell_and_non_boost_rendering() -> None:
    image = {
        "type": "span",
        "region": "body",
        "clip": "silhouette",
        "span": {"from": "head", "natural": 6},
        "source": {"type": "image", "texture": "fixture", "fit": "clip"},
    }
    fallback = {
        "name": "Fixture fallback",
        "type": "ribbon",
        "region": "body",
        "color": {"slot": "fill"},
    }
    assert _fallback_covers_image(fallback, image)
    for field, value in (("opacity", 0), ("boost_only", True), ("omit_on_single_cell", True)):
        unsafe = {**fallback, field: value}
        assert not _fallback_covers_image(unsafe, image)

    span_fallback = {
        "type": "span",
        "region": "body",
        "clip": "silhouette",
        "span": {"from": "head", "natural": 6},
        "source": {"type": "solid", "color": {"slot": "fill"}},
    }
    assert _fallback_covers_image(span_fallback, image)
    span_fallback["span"] = {"from": "whole"}
    assert not _fallback_covers_image(span_fallback, image)


def test_current_worker_schema_requires_explicit_current_contract_fields() -> None:
    schema = current_worker_result_json_schema()
    for definition in (
        "ImplementationPlan",
        "AssetPlan",
        "InputAuthorityEvidence",
        "ModifierPlan",
    ):
        entry = schema["$defs"][definition]
        assert set(entry["required"]) == set(entry["properties"])


def test_asset_plan_preflight_enforces_aggregate_skin_budgets() -> None:
    limits = {
        "max_texture_refs": 8,
        "max_texture_dimension_px": 2048,
        "max_sprite_frame_rows": 120,
        "max_texture_decoded_bytes": 16_777_216,
        "max_skin_texture_decoded_bytes": 67_108_864,
        "max_skin_texture_compressed_bytes": 8_388_608,
    }
    individually_valid = [
        AssetPlan(
            kind="sheet",
            natural_length_cells=128,
            frames=80,
            desired_fps=40,
            texels_per_cell=16,
            fit="clip",
            prompt=f"bounded sheet {index}",
        )
        for index in range(7)
    ]
    decoded_plan = type("Plan", (), {"asset_plan": individually_valid})()
    with pytest.raises(WorkerContractError, match="per-skin limit"):
        validate_plan_resource_limits(decoded_plan, {"limits": limits})

    compressed_plan = type(
        "Plan",
        (),
        {
            "asset_plan": [
                AssetPlan(
                    kind="sheet",
                    natural_length_cells=64,
                    frames=42,
                    desired_fps=21,
                    texels_per_cell=16,
                    fit="clip",
                    prompt=f"compressed-bound sheet {index}",
                )
                for index in range(4)
            ]
        },
    )()
    with pytest.raises(WorkerContractError, match="conservative PNG upper bound"):
        validate_plan_resource_limits(compressed_plan, {"limits": limits})


def test_live_handoff_enforces_pinned_plan_route_conditionals() -> None:
    plan = json.loads(
        (REPO / "skills/author-skin/fixtures/sprite-sheet/implementation-plan.json").read_text(encoding="utf-8")
    )
    plan["path"] = "texture"
    document = json.loads(
        (REPO / "skills/author-skin/fixtures/sprite-sheet/skin.skin.json").read_text(encoding="utf-8")
    )
    result = WorkerResult(implementation_plan=plan, skin_document=document)
    skill_files = {
        name: (REPO / "skills/author-skin" / name).read_text(encoding="utf-8")
        for name in (
            "schemas/implementation-plan.schema.json",
            "schemas/asset-request.schema.json",
        )
    }
    capabilities = json.loads((REPO / "skin-schema/capabilities-v2.json").read_text())
    with pytest.raises(WorkerContractError, match="implementation plan"):
        validate_worker_handoff(
            result,
            skill_files,
            capabilities,
            trusted_authority=plan["input_authority"],
        )


def test_live_handoff_derives_global_wrap_axes_from_assets() -> None:
    plan = json.loads(
        (REPO / "skills/author-skin/fixtures/worker-drafts/implementation-plan.json").read_text(encoding="utf-8")
    )
    plan["required_wrap_axes"] = []
    document = json.loads(
        (REPO / "skills/author-skin/fixtures/worker-drafts/skin-draft.valid.json").read_text(encoding="utf-8")
    )
    requests = json.loads(
        (REPO / "skills/author-skin/fixtures/worker-drafts/tool-requests.json").read_text(encoding="utf-8")
    )
    result = WorkerResult(implementation_plan=plan, skin_document=document, tool_requests=requests)
    skill_files = {
        name: (REPO / "skills/author-skin" / name).read_text(encoding="utf-8")
        for name in (
            "schemas/implementation-plan.schema.json",
            "schemas/asset-request.schema.json",
        )
    }
    capabilities = json.loads((REPO / "skin-schema/capabilities-v2.json").read_text())
    with pytest.raises(WorkerContractError, match="required_wrap_axes differ"):
        validate_worker_handoff(
            result,
            skill_files,
            capabilities,
            trusted_authority=plan["input_authority"],
        )


def test_live_handoff_rejects_required_transparency_without_edge_proof() -> None:
    plan = json.loads(
        (REPO / "skills/author-skin/fixtures/worker-drafts/implementation-plan.json").read_text(encoding="utf-8")
    )
    document = json.loads(
        (REPO / "skills/author-skin/fixtures/worker-drafts/skin-draft.valid.json").read_text(encoding="utf-8")
    )
    requests = json.loads(
        (REPO / "skills/author-skin/fixtures/worker-drafts/tool-requests.json").read_text(encoding="utf-8")
    )
    requests[0]["arguments"]["transparency"] = "required"
    result = WorkerResult(implementation_plan=plan, skin_document=document, tool_requests=requests)
    skill_files = {
        name: (REPO / "skills/author-skin" / name).read_text(encoding="utf-8")
        for name in (
            "schemas/implementation-plan.schema.json",
            "schemas/asset-request.schema.json",
        )
    }
    capabilities = json.loads((REPO / "skin-schema/capabilities-v2.json").read_text())
    with pytest.raises(WorkerContractError, match="transverse_policy"):
        validate_worker_handoff(
            result,
            skill_files,
            capabilities,
            trusted_authority=plan["input_authority"],
        )
