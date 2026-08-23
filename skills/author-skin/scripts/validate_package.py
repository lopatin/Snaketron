#!/usr/bin/env python3
"""Validate the canonical author-skin package and its executable fixtures."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

PACKAGE = Path(__file__).resolve().parents[1]
REPO = PACKAGE.parents[1]
HASH = re.compile(r"^sha256:[a-f0-9]{64}$")
PLAIN_HASH = re.compile(r"^[a-f0-9]{64}$")
ROUTES = {"layers", "texture", "sprite-sheet", "hybrid"}
DRAFT_FIXTURE_DIR = "worker-drafts"
MODIFIER_FIXTURE_DIR = "modifier-workflow"
MAX_TEXTURE_REFS = 8
MAX_SPRITE_FPS = 60
PLAN_PATHS = {"layers", "texture", "sprite_sheet", "hybrid"}
ASSET_KINDS = {"coat", "sheet", "overlay"}
AXES = {"x", "y"}
DESIGN_GUIDELINE_START = "<!-- SKIN_DESIGN_LOCKED:START -->"
DESIGN_GUIDELINE_END = "<!-- SKIN_DESIGN_LOCKED:END -->"
PROTOTYPE_IMAGE_RULES_START = "<!-- PROTOTYPE_IMAGE_RULES:START -->"
PROTOTYPE_IMAGE_RULES_END = "<!-- PROTOTYPE_IMAGE_RULES:END -->"
DESIGN_GUIDELINE_KEYS = {
    "artistic_direction",
    "concept_twist",
    "structure",
    "body_strategy",
    "head_zone",
    "asset_strategy",
}
DESIGN_GUIDELINE_TEXT_LIMITS = {
    "artistic_direction": 240,
    "concept_twist": 240,
    "body_strategy": 320,
    "asset_strategy": 320,
}
PROTOTYPE_AUTHORITY_KEYS = {
    "design_guidelines_sha256",
    "prototype_geometry_sha256",
    "prototype_guide_sha256",
}
PROTOTYPE_GEOMETRY_PROJECTION = "prototype-body-mask-v1"
BAND_LANE_INVARIANT = "max_frame(abs(t_center)) + max_frame(abs(half_width)) <= 0.5"
BAND_LANE_SAFE_EXAMPLE = "t_center = 0.3 * tri(time)"
PROTOTYPE_PROJECTION_PROMPT = (
    "prototype-body-mask-v1 deterministically projects the retained source material through the exact native "
    "renderer body mask before review; image_sha256 is the projected authority and source_image_sha256 is "
    "audit-only raw provider material."
)
PROTOTYPE_MANIFEST_KEYS = {
    "brief",
    "palette_intent",
    "motion_intent",
    "implementation_hint",
    "hint_rationale",
    "prompt",
    "model_config",
    "image_sha256",
    "source_image_sha256",
    "geometry_projection",
} | PROTOTYPE_AUTHORITY_KEYS
PROTOTYPE_PROMPT_TERMS = (
    "exact pinned blank geometry guide",
    "flat right-facing continuous 16-cell-by-1-cell capsule",
    "one-cell rounded head",
    "small centered dark core",
    "rounded tail",
    "no gaps, detached plates, perspective, or outside paint",
    PROTOTYPE_PROJECTION_PROMPT,
)
PIXVERSE_TRANSITION_CAPABILITY = "fal-ai/pixverse/v6/transition"
PIXVERSE_PROMPT_SECTIONS = (
    "[Cinematography]",
    "[Subject]",
    "[Action / Transition]",
    "[Context]",
    "[Style & Ambiance]",
)

REQUIRED_FILES = {
    "SKILL.md",
    "agents/openai.yaml",
    "optimization-boundary.json",
    "references/contract.md",
    "references/design-guidelines.md",
    "references/integration.md",
    "references/layers-effects.md",
    "references/modifiers-video.md",
    "references/playbook.md",
    "references/prototypes.md",
    "references/textures-sprites.md",
    "references/validation.md",
    "schemas/asset-request.schema.json",
    "schemas/implementation-plan.schema.json",
    "schemas/media-operation-request.schema.json",
    "schemas/modifier-manifest.schema.json",
    "schemas/prototype-manifest.schema.json",
    "templates/asset-request.json",
    "templates/implementation-plan.json",
    "templates/media-operation-request.json",
    "templates/modifier-manifest.json",
    "templates/skin-v2.template.json",
    "fixtures/worker-drafts/skin-anchor.externally-tagged.valid.json",
    "fixtures/worker-drafts/skin-anchor.flattened.invalid.json",
    "fixtures/modifier-workflow/draft-selection.json",
    "fixtures/modifier-workflow/implementation-plan.json",
    "fixtures/modifier-workflow/media-requests.json",
    "fixtures/modifier-workflow/T1.modifier-manifest.json",
    "fixtures/modifier-workflow/T2.modifier-manifest.json",
    "fixtures/modifier-workflow/B1.modifier-manifest.json",
    "fixtures/modifier-workflow/H.modifier-manifest.json",
}

PLAN_KEYS = {
    "input_authority",
    "path",
    "rationale",
    "design_guidelines",
    "common_period_ms",
    "fidelity_features",
    "layer_plan",
    "asset_plan",
    "modifier_plan",
    "animation_plan",
    "required_wrap_axes",
    "risks",
}

ASSET_KEYS = {
    "kind",
    "natural_length_cells",
    "frames",
    "desired_fps",
    "texels_per_cell",
    "raster_overhang_px",
    "anchor",
    "fit",
    "tile_phase_origin",
    "fade",
    "transverse_edge_policy",
    "prompt",
}

INPUT_AUTHORITY_KEYS = {
    "mode",
    "artifact_sha256",
    "authority_record_sha256",
    "human_approval_decision_id",
    "selection_rationale",
    "maximum_driver_action",
}

MODIFIER_KEYS = {
    "asset_index",
    "logical_key",
    "component_key",
    "texture_name",
    "image_layer_name",
    "fallback_layer_name",
    "span_limit_mode",
    "span_limit_value",
    "source_mode",
    "source_object_sha256",
    "modifier_manifest_sha256",
    "provenance_sha256",
    "license_id",
    "authorized_lineage_ids",
    "required_capabilities",
    "extraction",
    "video",
}


def read_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def is_hash(value: Any) -> bool:
    return isinstance(value, str) and HASH.fullmatch(value) is not None


def is_plain_hash(value: Any) -> bool:
    return isinstance(value, str) and PLAIN_HASH.fullmatch(value) is not None


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical_prototype_authorities() -> dict[str, str]:
    contract = REPO / "skin-schema" / "prototype-geometry-v1.json"
    geometry = read_json(contract)
    guide = contract.parent / geometry["guide"]
    return {
        "design_guidelines_sha256": sha256_file(PACKAGE / "references/design-guidelines.md"),
        "prototype_geometry_sha256": sha256_file(contract),
        "prototype_guide_sha256": sha256_file(guide),
    }


def numeric_constant(value: Any) -> bool:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return True
    if isinstance(value, str):
        try:
            float(value)
        except ValueError:
            return False
        return True
    return False


def add(errors: list[str], condition: bool, message: str) -> None:
    if not condition:
        errors.append(message)


def validate_schema_instance(instance: Any, schema: dict[str, Any], label: str, errors: list[str]) -> None:
    """Dependency-free closed top-level schema check for package fixtures."""

    if not isinstance(instance, dict):
        errors.append(f"{label}: instance must be an object")
        return
    required = set(schema.get("required", []))
    properties = set(schema.get("properties", {}))
    add(errors, required <= set(instance), f"{label}: missing required top-level fields")
    if schema.get("additionalProperties") is False:
        add(errors, set(instance) <= properties, f"{label}: unexpected top-level fields")


def validate_media_request(request: dict[str, Any], label: str, errors: list[str]) -> None:
    add(
        errors,
        request.get("journal")
        == {
            "retain_inputs": True,
            "retain_provider_output": True,
            "retain_output": True,
            "retain_reports": True,
        },
        f"{label}: media inputs/provider output/result/reports must all be journaled",
    )
    extraction = request.get("extraction")
    if extraction is not None:
        add(
            errors,
            extraction.get("source_arena") == "reserved_empty"
            and extraction.get("visible_object_count") == 1
            and extraction.get("alpha_contract") in {"transparent_rgba", "exact_mask_matte"}
            and extraction.get("background_removal") == "required"
            and extraction.get("matte_policy") == "fail_closed"
            and extraction.get("crop_after_verification") is True
            and extraction.get("fail_on_edge_contact") is True,
            f"{label}: extraction must fail closed before cropping one verified RGBA/matte object",
        )
    video = request.get("video")
    if isinstance(video, dict):
        period = video.get("common_period_ms")
        fps = video.get("desired_fps")
        if isinstance(period, (int, float)) and isinstance(fps, (int, float)):
            derived = max(2, math.ceil(period * fps / 1_000))
            add(errors, video.get("derived_frame_rows") == derived, f"{label}: derived rows must equal ceil(period*fps/1000)")
        columns = video.get("body_columns")
        tpc = video.get("texels_per_cell")
        overhang = video.get("raster_overhang_px")
        if all(isinstance(value, int) for value in (columns, tpc, overhang)) and (tpc * overhang) % 16 == 0:
            row_texels = tpc + 2 * (tpc * overhang // 16)
            cap = min(120, 2048 // row_texels, 16_777_216 // (columns * tpc * row_texels * 4))
            add(errors, video.get("row_texels") == row_texels, f"{label}: video row_texels is wrong")
            add(errors, video.get("effective_frame_row_cap") == cap, f"{label}: video effective row cap is wrong")
            add(errors, video.get("derived_frame_rows", 121) <= cap, f"{label}: video rows exceed effective cap")
        add(errors, video.get("frame_extraction") == "deterministic_uniform_full_period", f"{label}: video timestamps must be deterministic")
        add(errors, video.get("loop_closure") == "true_final_to_zero", f"{label}: true final-to-zero loop is required")
        add(errors, video.get("alpha_matte_verification") == "fail_closed", f"{label}: video alpha/matte must fail closed")
        add(
            errors,
            isinstance(video.get("desired_fps"), (int, float))
            and not isinstance(video.get("desired_fps"), bool)
            and video["desired_fps"] <= MAX_SPRITE_FPS,
            f"{label}: video desired_fps exceeds the pinned renderer ceiling",
        )
    if request.get("operation") == "generate_video" and isinstance(video, dict):
        add(
            errors,
            request.get("input_artifacts")
            == [video.get("start_frame_sha256"), video.get("end_frame_sha256")],
            f"{label}: transition inputs must bind ordered start/end frame hashes",
        )
        add(
            errors,
            video.get("source_video_sha256") is None,
            f"{label}: transition generation cannot claim provider output bytes before generation",
        )
        if request.get("capability_id") == PIXVERSE_TRANSITION_CAPABILITY:
            prompt = request.get("prompt", "")
            section_positions = [prompt.find(section) for section in PIXVERSE_PROMPT_SECTIONS]
            add(
                errors,
                all(position >= 0 for position in section_positions)
                and section_positions == sorted(section_positions)
                and all(prompt.count(section) == 1 for section in PIXVERSE_PROMPT_SECTIONS),
                f"{label}: PixVerse prompt must contain the five literal sections exactly once and in order",
            )
            for term in (
                "Static orthographic camera",
                "flat 2D",
                "exact supplied",
                "Only the B1 component animates",
                "reserved matte arena",
                "background completely static",
                "true cyclic final-to-zero closure",
                "exact colors",
                "alpha/matte boundaries",
            ):
                add(errors, term in prompt, f"{label}: PixVerse prompt is missing required constraint: {term}")
    if request.get("operation") == "extract_video_frames" and isinstance(video, dict):
        add(
            errors,
            request.get("input_artifacts") == [video.get("source_video_sha256")],
            f"{label}: frame extraction must bind the exact source video hash",
        )
    if request.get("operation") == "reuse_modifier":
        reuse = request.get("reuse", {})
        add(
            errors,
            request.get("input_artifacts") == [reuse.get("modifier_manifest_sha256")],
            f"{label}: reuse must bind one exact modifier manifest",
        )
        add(errors, reuse.get("require_authorized_lineage") is True, f"{label}: reuse must enforce lineage scope")
        add(errors, bool(reuse.get("target_lineage_id")), f"{label}: reuse target lineage is required")


def validate_modifier_manifest(manifest: dict[str, Any], label: str, errors: list[str]) -> None:
    for field in (
        "content_sha256",
        "forge_manifest_sha256",
        "descriptor_sha256",
        "provenance_sha256",
        "created_by_operation_sha256",
    ):
        add(errors, is_hash(manifest.get(field)), f"{label}: {field} must be an exact hash")
    add(
        errors,
        isinstance(manifest.get("authorized_lineage_ids"), list)
        and bool(manifest["authorized_lineage_ids"]),
        f"{label}: authorized lineage scope is required",
    )
    identifier = manifest.get("modifier_id")
    match = (
        re.fullmatch(r"modifier:([a-z0-9_-]+):(sha256:[a-f0-9]{64})", identifier)
        if isinstance(identifier, str)
        else None
    )
    add(errors, match is not None, f"{label}: modifier id is malformed")
    if match is not None:
        add(
            errors,
            match.group(2) == manifest.get("content_sha256"),
            f"{label}: modifier id must bind the exact content hash",
        )
        add(
            errors,
            match.group(1) in manifest.get("authorized_lineage_ids", []),
            f"{label}: modifier id lineage is not authorized",
        )
    add(
        errors,
        isinstance(manifest.get("raster_overhang_px"), int)
        and 0 <= manifest["raster_overhang_px"] <= 4,
        f"{label}: raster overhang must be 0..4",
    )
    verification = manifest.get("verification", {})
    add(errors, verification.get("background_contamination") == "pass", f"{label}: background contamination failed")
    add(errors, verification.get("edge_fringes") == "pass", f"{label}: alpha/matte edge fringes failed")
    add(
        errors,
        verification.get("outer_frame_edge") in {"pass_transparent_effect", "not_applicable_opaque_fill"},
        f"{label}: outer frame edge verification is required",
    )
    add(errors, verification.get("frame_gutters") == "pass", f"{label}: frame/repeat gutters are forbidden")
    add(errors, verification.get("immutable_bytes") is True, f"{label}: modifier bytes must be immutable")
    alpha_matte = verification.get("alpha_matte")
    outer_frame_edge = verification.get("outer_frame_edge")
    if alpha_matte in {"transparent_rgba_verified", "exact_mask_matte_verified"}:
        add(
            errors,
            outer_frame_edge == "pass_transparent_effect",
            f"{label}: transparent or matte modifier must prove the outer frame edge is clear",
        )
    elif alpha_matte == "opaque_not_applicable":
        add(
            errors,
            outer_frame_edge == "not_applicable_opaque_fill",
            f"{label}: opaque modifier must use the explicit outer-edge exemption",
        )


def validate_modifier_workflow_links(
    plan: dict[str, Any],
    requests: list[dict[str, Any]],
    manifests: dict[str, tuple[str, dict[str, Any]]],
    label: str,
    errors: list[str],
) -> None:
    """Bind every planned component to one retained manifest and its media path."""

    modifiers = plan.get("modifier_plan", [])
    assets = plan.get("asset_plan", [])
    planned_components = [modifier.get("component_key") for modifier in modifiers]
    add(
        errors,
        set(manifests) == set(planned_components) and len(manifests) == len(modifiers),
        f"{label}: every planned component needs exactly one immutable modifier manifest",
    )
    used_request_ids: set[str] = set()
    for modifier in modifiers:
        component = modifier.get("component_key")
        record = manifests.get(component)
        if record is None:
            continue
        manifest_sha256, manifest = record
        asset_index = modifier.get("asset_index")
        if not isinstance(asset_index, int) or not 0 <= asset_index < len(assets):
            continue
        asset = assets[asset_index]
        for field in (
            "logical_key",
            "component_key",
            "provenance_sha256",
            "license_id",
            "authorized_lineage_ids",
        ):
            add(
                errors,
                manifest.get(field) == modifier.get(field),
                f"{label}: {component} manifest {field} differs from its plan",
            )
        add(
            errors,
            manifest.get("content_sha256") == modifier.get("source_object_sha256"),
            f"{label}: {component} manifest content differs from the planned immutable object",
        )
        add(
            errors,
            modifier.get("modifier_manifest_sha256") == f"sha256:{manifest_sha256}",
            f"{label}: {component} plan does not bind the exact manifest bytes",
        )
        add(
            errors,
            manifest.get("raster_overhang_px") == asset.get("raster_overhang_px"),
            f"{label}: {component} manifest overhang differs from its asset",
        )
        component_requests = [
            request
            for request in requests
            if request.get("component_key") == component
            and request.get("logical_key") == modifier.get("logical_key")
        ]
        for request in component_requests:
            if isinstance(request.get("request_id"), str):
                used_request_ids.add(request["request_id"])
            add(
                errors,
                request.get("capability_id") in modifier.get("required_capabilities", []),
                f"{label}: {component} request uses an unplanned capability",
            )
        mode = modifier.get("source_mode")
        if mode == "extracted_rgba":
            add(
                errors,
                [request.get("operation") for request in component_requests] == ["extract_object"],
                f"{label}: {component} extraction path must contain one exact extract_object request",
            )
            add(
                errors,
                is_hash(manifest.get("extraction_report_sha256"))
                and manifest.get("video_report_sha256") is None,
                f"{label}: {component} manifest needs extraction-only verification",
            )
        elif mode == "reused_object":
            add(
                errors,
                [request.get("operation") for request in component_requests] == ["reuse_modifier"],
                f"{label}: {component} reuse path must contain one exact reuse request",
            )
            if len(component_requests) == 1:
                reuse = component_requests[0].get("reuse", {})
                add(
                    errors,
                    reuse.get("modifier_manifest_sha256") == modifier.get("modifier_manifest_sha256"),
                    f"{label}: {component} reuse request does not bind the planned manifest",
                )
                add(
                    errors,
                    reuse.get("target_lineage_id") in modifier.get("authorized_lineage_ids", []),
                    f"{label}: {component} reuse target is outside the authorized lineage",
                )
        elif mode == "video_frames":
            operations = [request.get("operation") for request in component_requests]
            add(
                errors,
                operations == ["generate_video", "extract_video_frames"],
                f"{label}: {component} video path must retain ordered generation and extraction requests",
            )
            add(
                errors,
                is_hash(manifest.get("extraction_report_sha256"))
                and is_hash(manifest.get("video_report_sha256")),
                f"{label}: {component} manifest needs video and extraction verification",
            )
            video = modifier.get("video", {})
            for request in component_requests:
                request_video = request.get("video", {})
                for field in (
                    "start_frame_sha256",
                    "end_frame_sha256",
                    "common_period_ms",
                    "desired_fps",
                    "derived_frame_rows",
                ):
                    add(
                        errors,
                        request_video.get(field) == video.get(field),
                        f"{label}: {component} media request {field} differs from the plan",
                    )
                if request.get("operation") == "extract_video_frames":
                    add(
                        errors,
                        request_video.get("source_video_sha256") == video.get("source_video_sha256"),
                        f"{label}: {component} extraction request differs from the planned source video",
                    )
        if mode != "reused_object":
            retained_inputs = {
                artifact
                for request in component_requests
                for artifact in request.get("input_artifacts", [])
            }
            add(
                errors,
                retained_inputs <= set(manifest.get("source_artifacts", [])),
                f"{label}: {component} manifest does not retain every media input",
            )
    add(
        errors,
        used_request_ids == {request.get("request_id") for request in requests},
        f"{label}: media requests must map one-to-one into planned component workflows",
    )


def validate_frontmatter(path: Path, errors: list[str]) -> None:
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    add(errors, len(lines) >= 4 and lines[0] == "---", f"{path}: missing frontmatter")
    if len(lines) < 4 or lines[0] != "---":
        return
    try:
        end = lines.index("---", 1)
    except ValueError:
        errors.append(f"{path}: unclosed frontmatter")
        return
    frontmatter = "\n".join(lines[1:end])
    add(errors, "name: author-skin" in frontmatter, f"{path}: wrong skill name")
    add(errors, "description:" in frontmatter, f"{path}: missing description")


def validate_boundaries(errors: list[str]) -> None:
    boundary = read_json(PACKAGE / "optimization-boundary.json")
    playbook = (PACKAGE / boundary["editable_path"]).read_text(encoding="utf-8")
    contract = (PACKAGE / "references/contract.md").read_text(encoding="utf-8")
    guidelines_path = "references/design-guidelines.md"
    guidelines = (PACKAGE / guidelines_path).read_text(encoding="utf-8")
    skill = (PACKAGE / "SKILL.md").read_text(encoding="utf-8")
    layers_reference = (PACKAGE / "references/layers-effects.md").read_text(encoding="utf-8")
    prototypes_reference = (PACKAGE / "references/prototypes.md").read_text(encoding="utf-8")
    validation_reference = (PACKAGE / "references/validation.md").read_text(encoding="utf-8")
    start = boundary["start_marker"]
    end = boundary["end_marker"]
    add(
        errors,
        playbook.count(start) == 1,
        "playbook must have exactly one GEPA start marker",
    )
    add(
        errors,
        playbook.count(end) == 1,
        "playbook must have exactly one GEPA end marker",
    )
    add(
        errors,
        playbook.find(start) < playbook.find(end),
        "GEPA markers are out of order",
    )
    add(
        errors,
        contract.count("<!-- FACTORY_LOCKED:START -->") == 1 and contract.count("<!-- FACTORY_LOCKED:END -->") == 1,
        "contract must have one locked boundary",
    )
    add(
        errors,
        guidelines.count(PROTOTYPE_IMAGE_RULES_START) == 1
        and guidelines.count(PROTOTYPE_IMAGE_RULES_END) == 1,
        "design guidelines must have one prototype-image rules boundary",
    )
    rules_start = guidelines.find(PROTOTYPE_IMAGE_RULES_START)
    rules_end = guidelines.find(PROTOTYPE_IMAGE_RULES_END)
    add(errors, rules_start < rules_end, "prototype-image rules markers are out of order")
    rules = guidelines[rules_start + len(PROTOTYPE_IMAGE_RULES_START) : rules_end].strip()
    add(errors, bool(rules), "prototype-image rules must not be empty")
    locked_start = guidelines.find(DESIGN_GUIDELINE_START)
    locked_end = guidelines.find(DESIGN_GUIDELINE_END)
    add(
        errors,
        locked_start < rules_start < rules_end < locked_end,
        "prototype-image rules must remain inside the locked design guidelines",
    )
    for required_safety_term in (
        "protected marks",
        "public-figure likeness",
        "unsafe content",
        "unlicensed references",
        "blocking `safety_ip`",
        "non-waivable",
    ):
        add(
            errors,
            required_safety_term in contract,
            f"locked contract is missing safety invariant: {required_safety_term}",
        )
    for required_band_term in (
        BAND_LANE_INVARIANT,
        BAND_LANE_SAFE_EXAMPLE,
        "every baked animation frame",
    ):
        add(
            errors,
            required_band_term in contract,
            f"locked contract is missing band lane invariant: {required_band_term}",
        )
    for label, reference in (
        ("layer guidance", layers_reference),
        ("validation guidance", validation_reference),
    ):
        add(
            errors,
            BAND_LANE_INVARIANT in reference,
            f"locked {label} is missing the combined band lane invariant",
        )
    add(
        errors,
        BAND_LANE_SAFE_EXAMPLE in layers_reference,
        "locked layer guidance is missing the safe animated band example",
    )
    add(
        errors,
        guidelines.count(DESIGN_GUIDELINE_START) == 1,
        "design guidelines must have exactly one locked start marker",
    )
    add(
        errors,
        guidelines.count(DESIGN_GUIDELINE_END) == 1,
        "design guidelines must have exactly one locked end marker",
    )
    add(
        errors,
        guidelines.find(DESIGN_GUIDELINE_START) < guidelines.find(DESIGN_GUIDELINE_END),
        "design-guideline markers are out of order",
    )
    if guidelines.count(DESIGN_GUIDELINE_START) == 1 and guidelines.count(DESIGN_GUIDELINE_END) == 1:
        locked_guidelines = guidelines.split(DESIGN_GUIDELINE_START, 1)[1].split(DESIGN_GUIDELINE_END, 1)[0]
        for required_design_term in (
            "fun, weird, or silly",
            "one-cell-wide path that continuously",
            "5\u201315 CSS px",
            "four cells",
            "Six- and seven-cell snakes",
            "compressed head, turn, and tail points",
            "head-ward run",
            "cannot paint arbitrary pixels outside it",
            "`TextureDescriptorV2.raster_overhang_px`",
            "0 through 4 authored bleed pixels per transverse side",
            "unchanged",
            "16×16 logical body cell",
            "does not relax the longitudinal head and tail caps",
            "neighboring snake",
            "not a freeform canvas",
            "Pattern",
            "Sprite",
            "production quality",
            "seamless",
            "first 1.5 cells",
            "light_field_dark_core",
            "dark_field_light_disc_dark_core",
            "Do not set a white `head_core`",
            "`artifact_refs.prototype_geometry`",
            "`artifact_refs.prototype_geometry_guide`",
            "`authoring_inputs.prototype_geometry`",
            "exact inline guide bytes",
            "`contract_sha256`",
            "`guide_sha256`",
            "`prototype_geometry_sha256`",
            "`prototype_guide_sha256`",
        ):
            add(
                errors,
                required_design_term in locked_guidelines,
                f"locked design guidelines are missing invariant: {required_design_term}",
            )
    for required_geometry_term in (
        "exact pinned `prototype_geometry` contract and guide",
        "`prototype_geometry` and `prototype_geometry_guide` artifact",
        "`artifact_refs` and `authoring_inputs`",
        "exact inline guide bytes",
        "`contract_sha256`",
        "`guide_sha256`",
        "`prototype_geometry_sha256`",
        "`prototype_guide_sha256`",
        "`source_image_sha256`",
        "`geometry_projection`",
        "`prototype-body-mask-v1`",
        "only authoring input",
        "`invalid_input`",
    ):
        add(
            errors,
            required_geometry_term in prototypes_reference,
            f"prototype reference is missing pinned geometry handoff: {required_geometry_term}",
        )
    add(
        errors,
        guidelines_path in boundary["locked_paths"],
        "design guidelines must be in the optimizer's locked paths",
    )
    add(
        errors,
        "[Skin Design Guidelines](references/design-guidelines.md)" in skill and "completely and apply" in skill,
        "SKILL.md must require reading and applying the shared design guidelines",
    )
    add(
        errors,
        "`design_guidelines` object" in validation_reference,
        "validation reference must gate the design-guideline evidence",
    )
    add(
        errors,
        boundary["editable_path"] not in boundary["locked_paths"],
        "the GEPA target cannot also be locked",
    )
    for path in boundary["locked_paths"]:
        add(errors, (PACKAGE / path).exists(), f"locked path does not exist: {path}")


def validate_playbook_candidate(candidate: str) -> list[str]:
    """Prove that an optimizer changed only the marked playbook body."""
    errors: list[str] = []
    boundary = read_json(PACKAGE / "optimization-boundary.json")
    current = (PACKAGE / boundary["editable_path"]).read_text(encoding="utf-8")
    start = boundary["start_marker"]
    end = boundary["end_marker"]
    if candidate.count(start) != 1 or candidate.count(end) != 1:
        return ["candidate must preserve exactly one pair of GEPA markers"]
    current_prefix, current_rest = current.split(start, 1)
    _, current_suffix = current_rest.split(end, 1)
    candidate_prefix, candidate_rest = candidate.split(start, 1)
    _, candidate_suffix = candidate_rest.split(end, 1)
    add(
        errors,
        candidate_prefix == current_prefix,
        "candidate changed locked playbook prefix",
    )
    add(
        errors,
        candidate_suffix == current_suffix,
        "candidate changed locked playbook suffix",
    )
    return errors


def validate_manifest(manifest: dict[str, Any], label: str, errors: list[str]) -> None:
    add(
        errors,
        set(manifest) == PROTOTYPE_MANIFEST_KEYS,
        f"{label}: prototype manifest fields drifted",
    )
    add(
        errors,
        manifest.get("implementation_hint") in PLAN_PATHS,
        f"{label}: invalid implementation hint",
    )
    add(errors, is_hash(manifest.get("image_sha256")), f"{label}: invalid image hash")
    add(
        errors,
        is_hash(manifest.get("source_image_sha256")),
        f"{label}: source_image_sha256 is required and must be a prefixed SHA-256 digest",
    )
    add(
        errors,
        manifest.get("geometry_projection") == PROTOTYPE_GEOMETRY_PROJECTION,
        f"{label}: geometry_projection must be exactly {PROTOTYPE_GEOMETRY_PROJECTION}",
    )
    add(
        errors,
        isinstance(manifest.get("model_config"), str) and bool(manifest["model_config"]),
        f"{label}: model_config must name a stored configuration",
    )
    for authority in sorted(PROTOTYPE_AUTHORITY_KEYS):
        add(
            errors,
            is_plain_hash(manifest.get(authority)),
            f"{label}: {authority} is required and must be a SHA-256 digest",
        )
    expected_authorities = canonical_prototype_authorities()
    for authority, expected in expected_authorities.items():
        add(
            errors,
            manifest.get(authority) == expected,
            f"{label}: {authority} differs from the canonical retained authority",
        )
    prompt = manifest.get("prompt")
    for term in PROTOTYPE_PROMPT_TERMS:
        add(
            errors,
            isinstance(prompt, str) and term in prompt,
            f"{label}: prototype prompt is missing geometry invariant: {term}",
        )


def validate_prototype_manifest_schema(schema: dict[str, Any], errors: list[str]) -> None:
    add(
        errors,
        schema.get("additionalProperties") is False,
        "prototype-manifest schema must forbid extra fields",
    )
    add(
        errors,
        set(schema.get("required", [])) == PROTOTYPE_MANIFEST_KEYS,
        "prototype-manifest schema must require every field and authority hash",
    )
    properties = schema.get("properties", {})
    add(
        errors,
        set(properties) == PROTOTYPE_MANIFEST_KEYS,
        "prototype-manifest schema fields drifted",
    )
    for authority in sorted(PROTOTYPE_AUTHORITY_KEYS):
        definition = properties.get(authority, {})
        add(
            errors,
            definition.get("type") == "string" and definition.get("pattern") == "^[a-f0-9]{64}$",
            f"prototype-manifest schema must require a non-null SHA-256: {authority}",
        )
    source_definition = properties.get("source_image_sha256", {})
    add(
        errors,
        source_definition.get("type") == "string"
        and source_definition.get("pattern") == r"^sha256:[a-f0-9]{64}$",
        "prototype-manifest schema must require a prefixed source image SHA-256",
    )
    projection_definition = properties.get("geometry_projection", {})
    add(
        errors,
        projection_definition.get("type") == "string"
        and projection_definition.get("const") == PROTOTYPE_GEOMETRY_PROJECTION,
        f"prototype-manifest schema must pin geometry_projection to {PROTOTYPE_GEOMETRY_PROJECTION}",
    )


def validate_design_guidelines(evidence: Any, label: str, errors: list[str]) -> None:
    if not isinstance(evidence, dict):
        errors.append(f"{label}: design_guidelines must be an object")
        return
    add(
        errors,
        set(evidence) == DESIGN_GUIDELINE_KEYS,
        f"{label}: design_guidelines fields drifted",
    )
    for field, maximum in DESIGN_GUIDELINE_TEXT_LIMITS.items():
        value = evidence.get(field)
        add(
            errors,
            isinstance(value, str) and bool(value.strip()),
            f"{label}: design_guidelines.{field} must be a non-empty string",
        )
        if isinstance(value, str):
            add(
                errors,
                len(value) <= maximum,
                f"{label}: design_guidelines.{field} exceeds {maximum} characters",
            )
    add(
        errors,
        evidence.get("structure") in {"pattern", "sprite"},
        f"{label}: design_guidelines.structure is invalid",
    )
    add(
        errors,
        evidence.get("head_zone") in {"light_field_dark_core", "dark_field_light_disc_dark_core"},
        f"{label}: design_guidelines.head_zone is invalid",
    )


def validate_implementation_plan_schema(schema: dict[str, Any], errors: list[str]) -> None:
    add(
        errors,
        "design_guidelines" in schema.get("required", []),
        "implementation-plan schema must require design_guidelines",
    )
    definition = schema.get("$defs", {}).get("designGuidelines", {})
    add(
        errors,
        definition.get("additionalProperties") is False,
        "design-guideline schema must forbid extra fields",
    )
    add(
        errors,
        set(definition.get("required", [])) == DESIGN_GUIDELINE_KEYS
        and set(definition.get("properties", {})) == DESIGN_GUIDELINE_KEYS,
        "design-guideline schema fields drifted",
    )
    properties = definition.get("properties", {})
    for field, maximum in DESIGN_GUIDELINE_TEXT_LIMITS.items():
        field_schema = properties.get(field, {})
        add(
            errors,
            field_schema.get("type") == "string"
            and field_schema.get("minLength") == 1
            and field_schema.get("maxLength") == maximum,
            f"design-guideline schema bound drifted: {field}",
        )
    add(
        errors,
        set(properties.get("structure", {}).get("enum", [])) == {"pattern", "sprite"},
        "design-guideline structure enum drifted",
    )
    add(
        errors,
        set(properties.get("head_zone", {}).get("enum", []))
        == {"light_field_dark_core", "dark_field_light_disc_dark_core"},
        "design-guideline head-zone enum drifted",
    )
    plan_properties = schema.get("properties", {})
    add(
        errors,
        plan_properties.get("asset_plan", {}).get("maxItems") == MAX_TEXTURE_REFS
        and plan_properties.get("modifier_plan", {}).get("maxItems") == MAX_TEXTURE_REFS,
        "implementation-plan schema must admit the current eight reference parser ceiling",
    )
    for field in ("layer_plan", "animation_plan"):
        description = plan_properties.get(field, {}).get("description", "")
        add(
            errors,
            BAND_LANE_INVARIANT in description,
            f"implementation-plan schema {field} must preserve the combined band lane invariant",
        )
    add(
        errors,
        BAND_LANE_SAFE_EXAMPLE in plan_properties.get("animation_plan", {}).get("description", ""),
        "implementation-plan schema animation_plan must preserve the safe animated band example",
    )
    for required in ("input_authority", "common_period_ms", "modifier_plan"):
        add(errors, required in schema.get("required", []), f"implementation-plan schema must require {required}")
    authority = schema.get("$defs", {}).get("inputAuthority", {})
    add(
        errors,
        authority.get("additionalProperties") is False
        and set(authority.get("required", [])) == INPUT_AUTHORITY_KEYS,
        "input-authority schema must be closed and require every evidence field",
    )
    modifier = schema.get("$defs", {}).get("modifier", {})
    add(
        errors,
        modifier.get("additionalProperties") is False
        and set(modifier.get("required", [])) == MODIFIER_KEYS,
        "modifier-plan schema must be closed and require every provenance/placement field",
    )
    add(
        errors,
        modifier.get("properties", {}).get("asset_index", {}).get("maximum")
        == MAX_TEXTURE_REFS - 1,
        "modifier asset index must match the current parser ceiling",
    )
    asset = schema.get("$defs", {}).get("asset", {})
    asset_properties = asset.get("properties", {})
    add(
        errors,
        asset_properties.get("raster_overhang_px", {}).get("maximum") == 4,
        "asset schema must cap raster_overhang_px at 4 around the unchanged 16x16 body cell",
    )
    add(
        errors,
        set(asset_properties.get("tile_phase_origin", {}).get("enum", [])) == {"head", "tail", None},
        "asset schema must expose head/tail tile phase plus null for non-tile fits",
    )


def validate_asset_request(request: dict[str, Any], label: str, errors: list[str]) -> None:
    required = {
        "schema_version",
        "request_id",
        "asset_index",
        "texture_name",
        "capability_role",
        "operation",
        "kind",
        "input_artifacts",
        "modifier",
        "prompt",
        "width_px",
        "height_px",
        "grid",
        "required_wrap_axes",
        "tile_phase_origin",
        "transparency",
        "row_zero",
        "edge_contract",
        "retention",
        "repair",
    }
    add(errors, set(request) == required, f"{label}: asset-request fields drifted")
    add(errors, request.get("schema_version") == 2, f"{label}: asset schema must be 2")
    add(
        errors,
        isinstance(request.get("asset_index"), int)
        and 0 <= request["asset_index"] < MAX_TEXTURE_REFS,
        f"{label}: asset index exceeds the current parser limit",
    )
    add(
        errors,
        isinstance(request.get("texture_name"), str)
        and bool(re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_-]*", request["texture_name"])),
        f"{label}: invalid SkinDoc texture name",
    )
    operation = request.get("operation")
    role = request.get("capability_role")
    inputs = request.get("input_artifacts", [])
    add(
        errors,
        all(is_hash(value) for value in inputs),
        f"{label}: invalid input artifact",
    )
    if operation == "generate":
        add(
            errors,
            role == "image_generator",
            f"{label}: generate needs image_generator",
        )
        add(errors, not inputs, f"{label}: generation cannot claim edit inputs")
        add(
            errors,
            request.get("repair") == {"method": "none", "mask_artifact": None},
            f"{label}: generation is not a repair",
        )
    elif operation == "edit":
        add(errors, role == "image_editor", f"{label}: edit needs image_editor")
        add(errors, bool(inputs), f"{label}: edit needs an input artifact")
    else:
        errors.append(f"{label}: invalid operation")
    grid = request.get("grid", {})
    axes = request.get("required_wrap_axes", [])
    add(
        errors,
        set(axes) <= AXES and len(axes) == len(set(axes)),
        f"{label}: invalid axes",
    )
    add(
        errors,
        request.get("width_px") == grid.get("body_columns", 0) * grid.get("texels_per_cell", 0),
        f"{label}: width must encode independent body columns",
    )
    tpc = grid.get("texels_per_cell")
    overhang = grid.get("raster_overhang_px")
    scaled_side = None
    if isinstance(tpc, int) and isinstance(overhang, int) and (tpc * overhang) % 16 == 0:
        scaled_side = tpc * overhang // 16
    row_texels = tpc + 2 * scaled_side if isinstance(tpc, int) and scaled_side is not None else None
    add(
        errors,
        row_texels == grid.get("row_texels"),
        f"{label}: row_texels must scale the bleed aprons around the unchanged 16x16 body",
    )
    add(
        errors,
        request.get("height_px") == grid.get("frame_rows", 0) * (row_texels or 0),
        f"{label}: height must encode independent frame rows with scaled overhang",
    )
    add(
        errors,
        isinstance(request.get("width_px"), int) and 1 <= request["width_px"] <= 2048,
        f"{label}: width exceeds the current capability bound",
    )
    add(
        errors,
        isinstance(request.get("height_px"), int) and 1 <= request["height_px"] <= 2048,
        f"{label}: height exceeds the current capability bound",
    )
    add(
        errors,
        isinstance(grid.get("texels_per_cell"), int) and 4 <= grid["texels_per_cell"] <= 128,
        f"{label}: invalid texels per cell",
    )
    expected_tpc = 16 if request.get("kind") == "sheet" else 64
    add(
        errors,
        grid.get("texels_per_cell") == expected_tpc,
        f"{label}: current forge requires {expected_tpc} texels per cell",
    )
    if request.get("kind") == "sheet":
        add(
            errors,
            grid.get("frame_rows", 0) >= 2,
            f"{label}: sheet needs multiple rows",
        )
        add(errors, "y" in axes, f"{label}: sheet needs y wrap")
        add(
            errors,
            request.get("row_zero") == "resting_and_reduced_motion",
            f"{label}: sheet row zero must be a resting frame",
        )
    else:
        add(errors, grid.get("frame_rows") == 1, f"{label}: static image needs one row")
        add(
            errors,
            request.get("row_zero") == "not_applicable",
            f"{label}: static row zero is N/A",
        )
    repair = request.get("repair", {})
    if repair.get("method") in {"tx_t_inpaint", "roll_and_repair"}:
        add(
            errors,
            is_hash(repair.get("mask_artifact")),
            f"{label}: inpaint repair needs a mask artifact",
        )
    add(
        errors,
        isinstance(overhang, int) and 0 <= overhang <= 4,
        f"{label}: raster_overhang_px must be 0..4",
    )
    phase = request.get("tile_phase_origin")
    if "x" in axes:
        add(errors, phase in {"head", "tail"}, f"{label}: tiled asset needs head/tail phase origin")
    else:
        add(errors, phase is None, f"{label}: non-tiled asset phase origin must be null")
    modifier = request.get("modifier", {})
    add(
        errors,
        set(modifier)
        == {"logical_key", "component_key", "provenance_sha256", "license_id", "authorized_lineage_ids"},
        f"{label}: modifier intent fields drifted",
    )
    add(errors, is_hash(modifier.get("provenance_sha256")), f"{label}: modifier provenance must be hashed")
    add(
        errors,
        isinstance(modifier.get("authorized_lineage_ids"), list) and bool(modifier["authorized_lineage_ids"]),
        f"{label}: modifier needs an authorized lineage scope",
    )
    retention = request.get("retention", {})
    add(
        errors,
        retention
        == {"retain_inputs": True, "retain_provider_output": True, "retain_forged_output": True},
        f"{label}: asset inputs/provider/forge outputs must all be retained",
    )
    edge = request.get("edge_contract", {})
    add(errors, edge.get("no_frame_or_repeat_gutters") is True, f"{label}: frame/repeat gutters are forbidden")
    policy = edge.get("transverse_policy")
    add(
        errors,
        policy in {"fail_closed_transparent_effect", "not_applicable_opaque_fill"},
        f"{label}: invalid transverse edge policy",
    )
    if request.get("transparency") == "required":
        add(errors, policy == "fail_closed_transparent_effect", f"{label}: transparent effect must fail on edge contact")
    if policy == "not_applicable_opaque_fill":
        add(errors, request.get("transparency") == "opaque", f"{label}: opaque edge exemption requires opaque bytes")


def validate_video_contract(
    video: Any,
    asset: dict[str, Any],
    common_period_ms: Any,
    label: str,
    errors: list[str],
) -> None:
    if not isinstance(video, dict):
        errors.append(f"{label}: video contract is required")
        return
    for field in (
        "start_frame_sha256",
        "end_frame_sha256",
        "source_video_sha256",
        "extracted_sheet_sha256",
    ):
        add(errors, is_hash(video.get(field)), f"{label}: video {field} must be an exact hash")
    period = video.get("common_period_ms")
    fps = video.get("desired_fps")
    add(errors, period == common_period_ms, f"{label}: video must use the plan common period")
    add(errors, fps == asset.get("desired_fps"), f"{label}: video desired_fps differs from asset")
    add(
        errors,
        isinstance(fps, (int, float)) and not isinstance(fps, bool) and fps <= MAX_SPRITE_FPS,
        f"{label}: video desired_fps exceeds the pinned renderer ceiling",
    )
    if not isinstance(period, (int, float)) or not isinstance(fps, (int, float)):
        return
    derived = max(2, math.ceil(period * fps / 1_000))
    add(errors, video.get("derived_frame_rows") == derived, f"{label}: video rows must equal ceil(period*fps/1000)")
    tpc = asset.get("texels_per_cell")
    overhang = asset.get("raster_overhang_px")
    columns = asset.get("natural_length_cells")
    if not all(isinstance(value, int) for value in (tpc, overhang, columns)) or (tpc * overhang) % 16:
        return
    row_texels = tpc + 2 * (tpc * overhang // 16)
    width = columns * tpc
    cap = min(120, 2048 // row_texels, 16_777_216 // (width * row_texels * 4))
    add(errors, video.get("effective_frame_row_cap") == cap, f"{label}: video effective row cap is wrong")
    add(errors, derived <= cap and asset.get("frames") == derived, f"{label}: video cadence exceeds effective row cap")
    add(errors, video.get("frame_extraction") == "deterministic_uniform_full_period", f"{label}: video extraction is not deterministic")
    add(errors, video.get("alpha_matte_verification") == "fail_closed", f"{label}: video alpha/matte must fail closed")
    add(errors, video.get("loop_closure") == "true_final_to_zero", f"{label}: video must verify true final-to-zero")
    add(errors, video.get("retained_inputs_and_output") is True, f"{label}: video inputs/output must be retained")


def validate_plan(
    plan: dict[str, Any],
    manifest: dict[str, Any],
    approval: dict[str, Any],
    label: str,
    errors: list[str],
) -> None:
    add(errors, set(plan) == PLAN_KEYS, f"{label}: implementation-plan fields drifted")
    path = plan.get("path")
    add(errors, path in PLAN_PATHS, f"{label}: invalid plan path")
    authority = plan.get("input_authority", {})
    add(errors, set(authority) == INPUT_AUTHORITY_KEYS, f"{label}: input_authority fields drifted")
    mode = authority.get("mode")
    add(errors, mode in {"approved_prototype", "draft_submission"}, f"{label}: invalid input authority mode")
    add(errors, is_hash(authority.get("authority_record_sha256")), f"{label}: authority record must be hashed")
    add(
        errors,
        authority.get("artifact_sha256") == manifest.get("image_sha256"),
        f"{label}: authority and manifest must bind the same image",
    )
    if mode == "approved_prototype":
        add(errors, approval.get("action") == "prototype_approval", f"{label}: wrong approval action")
        add(
            errors,
            manifest.get("image_sha256") == approval.get("artifact_sha256"),
            f"{label}: approval and manifest must bind the same image",
        )
        add(
            errors,
            authority.get("human_approval_decision_id") == approval.get("decision_id")
            and bool(approval.get("decision_id")),
            f"{label}: exact human approval decision id is required",
        )
        add(errors, authority.get("selection_rationale") is None, f"{label}: approval cannot claim selection rationale")
        add(
            errors,
            authority.get("maximum_driver_action") == "register_private_revision",
            f"{label}: approved mode action ceiling drifted",
        )
    elif mode == "draft_submission":
        add(errors, authority.get("human_approval_decision_id") is None, f"{label}: draft cannot claim human approval")
        add(errors, bool(authority.get("selection_rationale")), f"{label}: draft needs literal selection rationale")
        add(
            errors,
            authority.get("maximum_driver_action") == "request_admin_review",
            f"{label}: draft action ceiling must be request_admin_review",
        )
    add(
        errors,
        isinstance(plan.get("common_period_ms"), (int, float))
        and not isinstance(plan.get("common_period_ms"), bool)
        and 120 <= plan["common_period_ms"] <= 60_000,
        f"{label}: common_period_ms is invalid",
    )
    add(errors, bool(plan.get("rationale")), f"{label}: route rationale is required")
    validate_design_guidelines(plan.get("design_guidelines"), label, errors)

    axes = plan.get("required_wrap_axes", [])
    add(
        errors,
        set(axes) <= AXES and len(axes) == len(set(axes)),
        f"{label}: invalid wrap axes",
    )
    assets = plan.get("asset_plan", [])
    add(
        errors,
        len(assets) <= MAX_TEXTURE_REFS,
        f"{label}: asset plan exceeds max texture refs",
    )
    if path == "layers":
        add(errors, not assets, f"{label}: layers path cannot request image assets")
    elif path == "texture":
        add(errors, bool(assets), f"{label}: texture path needs an asset")
        add(
            errors,
            all(asset.get("kind") in {"coat", "overlay"} for asset in assets),
            f"{label}: texture path cannot contain a sheet",
        )
    elif path == "sprite_sheet":
        add(
            errors,
            any(asset.get("kind") == "sheet" for asset in assets),
            f"{label}: sprite_sheet path needs a sheet",
        )
    elif path == "hybrid":
        add(errors, bool(assets), f"{label}: hybrid path needs an image asset")

    derived_axes: set[str] = set()
    aggregate_decoded = 0
    aggregate_png_upper = 0
    for index, asset in enumerate(assets):
        asset_label = f"{label}: asset_plan[{index}]"
        add(errors, set(asset) == ASSET_KEYS, f"{asset_label}: fields drifted")
        kind = asset.get("kind")
        add(errors, kind in ASSET_KINDS, f"{asset_label}: invalid kind")
        add(
            errors,
            isinstance(asset.get("natural_length_cells"), int) and 1 <= asset["natural_length_cells"] <= 128,
            f"{asset_label}: invalid independent X body cells",
        )
        add(
            errors,
            isinstance(asset.get("frames"), int) and 1 <= asset["frames"] <= 120,
            f"{asset_label}: invalid independent Y frame count",
        )
        tpc = asset.get("texels_per_cell")
        overhang = asset.get("raster_overhang_px")
        scaled_side = None
        if isinstance(tpc, int) and isinstance(overhang, int) and (tpc * overhang) % 16 == 0:
            scaled_side = tpc * overhang // 16
        row_texels = tpc + 2 * scaled_side if isinstance(tpc, int) and scaled_side is not None else None
        width = None
        if isinstance(asset.get("natural_length_cells"), int) and isinstance(tpc, int):
            width = asset["natural_length_cells"] * tpc
            add(
                errors,
                width <= 2048,
                f"{asset_label}: width exceeds the current capability bound",
            )
        height = None
        if isinstance(asset.get("frames"), int) and row_texels is not None:
            rows = asset["frames"] if kind == "sheet" else 1
            height = rows * row_texels
            add(
                errors,
                height <= 2048,
                f"{asset_label}: height exceeds the current capability bound",
            )
        if width is not None and height is not None:
            decoded = width * height * 4
            add(
                errors,
                decoded <= 16_777_216,
                f"{asset_label}: decoded bytes exceed the current capability bound",
            )
            aggregate_decoded += decoded
            scanline_bytes = decoded + height
            aggregate_png_upper += scanline_bytes + ((scanline_bytes + 65_534) // 65_535) * 5 + 1_024
        add(
            errors,
            isinstance(asset.get("texels_per_cell"), int) and 4 <= asset["texels_per_cell"] <= 128,
            f"{asset_label}: invalid texels per cell",
        )
        expected_tpc = 16 if kind == "sheet" else 64
        add(
            errors,
            asset.get("texels_per_cell") == expected_tpc,
            f"{asset_label}: current forge requires {expected_tpc} texels per cell",
        )
        add(
            errors,
            asset.get("anchor") in {"whole", "head", "tail"},
            f"{asset_label}: invalid anchor",
        )
        add(
            errors,
            asset.get("fit") in {"tile", "clip", "stretch", "cutout"},
            f"{asset_label}: invalid fit",
        )
        add(
            errors,
            asset.get("fade") in {"none", "leading", "trailing", "both"},
            f"{asset_label}: invalid fade",
        )
        add(
            errors,
            isinstance(overhang, int) and 0 <= overhang <= 4,
            f"{asset_label}: raster_overhang_px must be 0..4",
        )
        if asset.get("fit") == "tile":
            add(
                errors,
                asset.get("tile_phase_origin") in {"head", "tail"},
                f"{asset_label}: tile fit needs head/tail phase origin",
            )
        else:
            add(errors, asset.get("tile_phase_origin") is None, f"{asset_label}: non-tile phase must be null")
        add(
            errors,
            asset.get("transverse_edge_policy")
            in {"fail_closed_transparent_effect", "not_applicable_opaque_fill"},
            f"{asset_label}: invalid transverse edge policy",
        )
        add(
            errors,
            bool(asset.get("prompt")),
            f"{asset_label}: generation prompt is required",
        )
        if asset.get("fit") == "tile":
            derived_axes.add("x")
        if kind == "sheet":
            add(
                errors,
                asset["frames"] >= 2,
                f"{asset_label}: sheet needs multiple frames",
            )
            add(
                errors,
                isinstance(asset.get("desired_fps"), (int, float))
                and not isinstance(asset.get("desired_fps"), bool)
                and 1 <= asset["desired_fps"] <= 120,
                f"{asset_label}: sheet needs a bounded desired_fps",
            )
            derived_axes.add("y")
        else:
            add(
                errors,
                asset.get("frames") == 1,
                f"{asset_label}: static asset has one frame",
            )
            add(
                errors,
                asset.get("desired_fps") is None,
                f"{asset_label}: static asset desired_fps must be null",
            )
    add(
        errors,
        aggregate_decoded <= 67_108_864,
        f"{label}: decoded asset plan exceeds the current per-skin bound",
    )
    add(
        errors,
        aggregate_png_upper <= 8_388_608,
        f"{label}: conservative PNG asset plan exceeds the current per-skin compressed bound",
    )
    add(
        errors,
        set(axes) == derived_axes,
        f"{label}: wrap axes must be derived from kind and fit",
    )
    modifiers = plan.get("modifier_plan", [])
    add(errors, len(modifiers) == len(assets), f"{label}: modifier plan must map one-to-one to assets")
    indexes: list[int] = []
    for index, modifier in enumerate(modifiers):
        modifier_label = f"{label}: modifier_plan[{index}]"
        add(errors, set(modifier) == MODIFIER_KEYS, f"{modifier_label}: fields drifted")
        asset_index = modifier.get("asset_index")
        indexes.append(asset_index)
        add(errors, asset_index == index, f"{modifier_label}: asset index must be exact and ordered")
        if not isinstance(asset_index, int) or not 0 <= asset_index < len(assets):
            continue
        asset = assets[asset_index]
        add(errors, is_hash(modifier.get("provenance_sha256")), f"{modifier_label}: provenance must be hashed")
        add(
            errors,
            isinstance(modifier.get("authorized_lineage_ids"), list)
            and bool(modifier["authorized_lineage_ids"]),
            f"{modifier_label}: authorized lineage scope is required",
        )
        expected_span = {"whole": "whole", "head": "head_cells", "tail": "tail_fraction"}.get(asset.get("anchor"))
        add(errors, modifier.get("span_limit_mode") == expected_span, f"{modifier_label}: span limit differs from anchor")
        value = modifier.get("span_limit_value")
        if expected_span == "whole":
            add(errors, value is None, f"{modifier_label}: whole span limit must be null")
        elif expected_span == "head_cells":
            add(errors, isinstance(value, (int, float)) and 0 < value <= 6, f"{modifier_label}: head span exceeds cell 6")
        elif expected_span == "tail_fraction":
            add(errors, isinstance(value, (int, float)) and 0 < value <= 0.5, f"{modifier_label}: tail span exceeds half")
        source_mode = modifier.get("source_mode")
        add(
            errors,
            source_mode in {"direct_generate", "extracted_rgba", "reused_object", "video_frames"},
            f"{modifier_label}: invalid source mode",
        )
        source_bound = is_hash(modifier.get("source_object_sha256"))
        manifest_bound = is_hash(modifier.get("modifier_manifest_sha256"))
        bound = source_bound and manifest_bound
        if source_mode == "direct_generate":
            add(errors, not source_bound and not manifest_bound
                and modifier.get("extraction") is None and modifier.get("video") is None,
                f"{modifier_label}: direct generation cannot claim bound media evidence")
        else:
            add(errors, bound, f"{modifier_label}: retained object and manifest hashes are required")
        if source_mode == "reused_object":
            add(
                errors,
                modifier.get("extraction") is None and modifier.get("video") is None,
                f"{modifier_label}: reused object cannot claim extraction/video evidence",
            )
        if source_mode == "extracted_rgba":
            add(
                errors,
                modifier.get("video") is None,
                f"{modifier_label}: extracted RGBA cannot claim video evidence",
            )
        if source_mode in {"extracted_rgba", "video_frames"}:
            extraction = modifier.get("extraction", {})
            add(
                errors,
                extraction
                in (
                    {
                        "source_arena": "reserved_empty",
                        "alpha_contract": "transparent_rgba",
                        "background_removal": "required",
                        "matte_policy": "fail_closed",
                        "cropped_object_retained": True,
                    },
                    {
                        "source_arena": "reserved_empty",
                        "alpha_contract": "exact_mask_matte",
                        "background_removal": "required",
                        "matte_policy": "fail_closed",
                        "cropped_object_retained": True,
                    },
                ),
                f"{modifier_label}: extraction must verify and retain exact alpha/matte bytes",
            )
        if source_mode == "video_frames":
            validate_video_contract(modifier.get("video"), asset, plan.get("common_period_ms"), modifier_label, errors)
            add(
                errors,
                modifier.get("video", {}).get("extracted_sheet_sha256")
                == modifier.get("source_object_sha256"),
                f"{modifier_label}: source object must be the exact extracted video sheet bytes",
            )
    add(errors, indexes == list(range(len(assets))), f"{label}: modifier indexes must be unique and contiguous")


def asset_wrap_axes(asset: dict[str, Any]) -> set[str]:
    axes: set[str] = set()
    if asset.get("kind") == "sheet":
        axes.add("y")
    if asset.get("fit") == "tile":
        axes.add("x")
    return axes


def validate_worker_draft(
    document: dict[str, Any],
    plan: dict[str, Any],
    tool_requests: list[dict[str, Any]],
    label: str,
    errors: list[str],
    *,
    allow_pending: bool = True,
) -> None:
    """Validate the one-pass worker-to-driver unresolved texture handoff."""
    assets = plan.get("asset_plan", [])
    generated = [request for request in tool_requests if request.get("kind") == "generate_asset"]
    add(
        errors,
        len(generated) == len(tool_requests),
        f"{label}: initial worker draft only supports generate_asset requests",
    )
    add(
        errors,
        len(generated) == len(assets),
        f"{label}: needs exactly one generate request per planned asset",
    )

    requests_by_index: dict[int, dict[str, Any]] = {}
    for request_number, request in enumerate(generated):
        add(
            errors,
            set(request) == {"kind", "arguments"},
            f"{label}: tool request {request_number} fields drifted",
        )
        arguments = request.get("arguments", {})
        validate_asset_request(arguments, f"{label}: request {request_number}", errors)
        index = arguments.get("asset_index")
        if not isinstance(index, int):
            continue
        add(
            errors,
            index not in requests_by_index,
            f"{label}: duplicate asset index {index}",
        )
        requests_by_index[index] = arguments

    textures = document.get("textures", [])
    texture_names = [texture.get("name") for texture in textures]
    add(
        errors,
        len(texture_names) == len(set(texture_names)),
        f"{label}: duplicate texture name",
    )
    layers = flatten_layers(document.get("layers", []))

    for index, asset in enumerate(assets):
        arguments = requests_by_index.get(index)
        add(errors, arguments is not None, f"{label}: missing request for asset {index}")
        if arguments is None:
            continue
        expected_rows = asset.get("frames") if asset.get("kind") == "sheet" else 1
        grid = arguments.get("grid", {})
        add(
            errors,
            arguments.get("operation") == "generate",
            f"{label}: asset {index} is not a generation request",
        )
        add(
            errors,
            arguments.get("kind") == asset.get("kind"),
            f"{label}: asset {index} kind differs from plan",
        )
        add(
            errors,
            grid.get("body_columns") == asset.get("natural_length_cells"),
            f"{label}: asset {index} X differs from plan",
        )
        add(
            errors,
            grid.get("frame_rows") == expected_rows,
            f"{label}: asset {index} Y differs from plan",
        )
        add(
            errors,
            grid.get("texels_per_cell") == asset.get("texels_per_cell"),
            f"{label}: asset {index} texel density differs from plan",
        )
        add(
            errors,
            set(arguments.get("required_wrap_axes", [])) == asset_wrap_axes(asset),
            f"{label}: asset {index} wrap axes differ from use",
        )

        texture_name = arguments.get("texture_name")
        matches = [texture for texture in textures if texture.get("name") == texture_name]
        add(
            errors,
            len(matches) == 1,
            f"{label}: asset {index} needs one named draft texture",
        )
        if len(matches) != 1:
            continue
        texture = matches[0]
        sentinel = f"pending:asset:{index}"
        add(
            errors,
            texture.get("ref") == sentinel,
            f"{label}: asset {index} must use {sentinel}",
        )
        add(
            errors,
            texture.get("kind") == asset.get("kind"),
            f"{label}: asset {index} draft texture kind differs",
        )
        add(
            errors,
            "descriptor" not in texture,
            f"{label}: asset {index} draft cannot fabricate a descriptor",
        )
        add(
            errors,
            any(
                layer.get("source", {}).get("type") == "image"
                and layer.get("source", {}).get("texture") == texture_name
                for layer in layers
            ),
            f"{label}: asset {index} texture is not used by an image layer",
        )

    pending_refs = [
        texture.get("ref")
        for texture in textures
        if isinstance(texture.get("ref"), str) and texture["ref"].startswith("pending:")
    ]
    add(
        errors,
        len(pending_refs) == len(assets),
        f"{label}: pending refs must correspond one-to-one with planned assets",
    )
    if not allow_pending:
        add(errors, not pending_refs, f"{label}: pending ref survived exact binding")


def flatten_layers(layers: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for layer in layers:
        if layer.get("type") == "group":
            result.extend(flatten_layers(layer.get("layers", [])))
        else:
            result.append(layer)
    return result


def fallback_covers_image(fallback: dict[str, Any], image: dict[str, Any]) -> bool:
    if fallback.get("region") != image.get("region"):
        return False
    if fallback.get("boost_only", False) or fallback.get("omit_on_single_cell", False):
        return False
    if fallback.get("opacity", 1) != 1:
        return False
    default_transform = {
        "translate_s": 0,
        "translate_t": 0,
        "scale_s": 1,
        "scale_t": 1,
        "rotate_turns": 0,
    }

    def normalized_transform(layer: dict[str, Any]) -> dict[str, Any]:
        return {**default_transform, **layer.get("transform", {})}

    if fallback.get("type") == "ribbon":
        return normalized_transform(fallback) == default_transform
    if fallback.get("type") != "span" or fallback.get("source", {}).get("type") != "solid":
        return False
    span_defaults = {"natural": None, "min": 0, "priority": 0}
    return (
        {**span_defaults, **fallback.get("span", {})}
        == {**span_defaults, **image.get("span", {})}
        and fallback.get("clip", "silhouette") == image.get("clip", "silhouette")
        and normalized_transform(fallback) == normalized_transform(image)
    )


def validate_document(
    document: dict[str, Any],
    plan: dict[str, Any],
    label: str,
    errors: list[str],
) -> None:
    add(
        errors,
        document.get("schema_version") == 2,
        f"{label}: fixture is not SkinDoc v2",
    )
    layers = flatten_layers(document.get("layers", []))
    textures = {entry.get("name"): entry for entry in document.get("textures", [])}
    assets = plan.get("asset_plan", [])
    modifiers = {entry.get("texture_name"): entry for entry in plan.get("modifier_plan", [])}
    add(errors, document.get("period_ms") == plan.get("common_period_ms"), f"{label}: document period differs from plan")
    image_count = 0
    procedural_count = 0

    for index, layer in enumerate(layers):
        source = layer.get("source", {})
        source_type = source.get("type")
        if source_type and source_type != "image":
            procedural_count += 1
        if source_type != "image":
            continue
        image_count += 1
        texture_name = source.get("texture")
        add(errors, texture_name in textures, f"{label}: image names an absent texture")
        if texture_name not in textures:
            continue
        modifier = modifiers.get(texture_name)
        add(errors, modifier is not None, f"{label}: image texture has no exact modifier record")
        if modifier is None:
            continue
        add(errors, layer.get("name") == modifier.get("image_layer_name"), f"{label}: modifier image layer name differs")
        add(
            errors,
            numeric_constant(source.get("drift_cells", 0)),
            f"{label}: image drift must stay constant until capability enables it",
        )
        texture = textures[texture_name]
        descriptor = texture.get("descriptor")
        asset_index = modifier.get("asset_index")
        matching_assets = [assets[asset_index]] if isinstance(asset_index, int) and 0 <= asset_index < len(assets) else []
        add(
            errors,
            len(matching_assets) == 1,
            f"{label}: image needs one matching asset plan",
        )
        if not matching_assets:
            continue
        asset = matching_assets[0]
        fallback_candidates = [
            candidate
            for candidate in layers[:index]
            if candidate.get("name") == modifier.get("fallback_layer_name")
            and fallback_covers_image(candidate, layer)
        ]
        add(
            errors,
            bool(fallback_candidates),
            f"{label}: ordinary fallback must precede image",
        )
        add(
            errors,
            texture.get("kind") == asset["kind"],
            f"{label}: texture kind differs from plan",
        )
        add(
            errors,
            isinstance(descriptor, dict),
            f"{label}: generated texture needs a descriptor",
        )
        if not isinstance(descriptor, dict):
            continue
        add(
            errors,
            descriptor.get("kind") == asset["kind"],
            f"{label}: descriptor kind differs",
        )
        add(
            errors,
            descriptor.get("body_columns") == asset["natural_length_cells"],
            f"{label}: descriptor X differs from plan",
        )
        add(
            errors,
            descriptor.get("raster_overhang_px", 0) == asset["raster_overhang_px"],
            f"{label}: descriptor raster overhang differs from plan",
        )
        expected_rows = asset["frames"] if asset["kind"] == "sheet" else None
        add(
            errors,
            descriptor.get("frame_rows") == expected_rows,
            f"{label}: descriptor Y differs from plan",
        )
        variants = descriptor.get("variants", [])
        add(errors, bool(variants), f"{label}: descriptor has no variants")
        add(
            errors,
            any(variant.get("content_ref") == texture.get("ref") for variant in variants),
            f"{label}: canonical ref is not an exact served variant",
        )
        for variant in variants:
            content_ref = variant.get("content_ref")
            add(errors, is_hash(content_ref), f"{label}: invalid variant hash")
            add(
                errors,
                variant.get("url") == f"/api/textures/variants/{content_ref}.png",
                f"{label}: variant URL is not addressed by its own hash",
            )
            tpc = variant.get("texels_per_cell")
            if isinstance(tpc, int) and (tpc * asset["raster_overhang_px"]) % 16 == 0:
                row_texels = tpc + 2 * (tpc * asset["raster_overhang_px"] // 16)
                rows = asset["frames"] if asset["kind"] == "sheet" else 1
                add(
                    errors,
                    variant.get("height_px") == rows * row_texels,
                    f"{label}: variant height does not include scaled raster overhang",
                )

        span = layer.get("span", {})
        mode = modifier.get("span_limit_mode")
        if mode == "whole":
            add(errors, span.get("from") == "whole" and span.get("natural") is None, f"{label}: whole modifier span differs")
        elif mode == "head_cells":
            natural = span.get("natural")
            add(
                errors,
                span.get("from") == "head"
                and isinstance(natural, (int, float))
                and 0 < natural <= min(6, modifier.get("span_limit_value")),
                f"{label}: head modifier does not stop by cell 6",
            )
        elif mode == "tail_fraction":
            origin = span.get("from")
            fraction = origin.get("fraction", {}).get("fraction") if isinstance(origin, dict) else None
            limit = modifier.get("span_limit_value")
            add(
                errors,
                isinstance(fraction, (int, float))
                and isinstance(limit, (int, float))
                and 1 - limit <= fraction < 1
                and span.get("natural") is None,
                f"{label}: tail modifier exceeds half the current snake",
            )
        fit = source.get("fit")
        if asset.get("fit") == "tile":
            add(
                errors,
                isinstance(fit, dict)
                and fit.get("type") == "tile"
                and fit.get("phase_origin", "head") == asset.get("tile_phase_origin"),
                f"{label}: tile phase origin differs from plan",
            )

    path = plan.get("path")
    if path == "layers":
        add(
            errors,
            image_count == 0,
            f"{label}: layers fixture unexpectedly uses images",
        )
    else:
        add(errors, image_count > 0, f"{label}: image route has no image layer")
    if path == "hybrid":
        add(
            errors,
            procedural_count > 0,
            f"{label}: hybrid fixture needs procedural composition",
        )


def validate_package() -> list[str]:
    errors: list[str] = []
    for relative in sorted(REQUIRED_FILES):
        add(
            errors,
            (PACKAGE / relative).is_file(),
            f"required package file is missing: {relative}",
        )

    validate_frontmatter(PACKAGE / "SKILL.md", errors)
    validate_boundaries(errors)

    schemas = {schema.name: read_json(schema) for schema in (PACKAGE / "schemas").glob("*.json")}
    validate_implementation_plan_schema(schemas.get("implementation-plan.schema.json", {}), errors)
    validate_prototype_manifest_schema(schemas.get("prototype-manifest.schema.json", {}), errors)
    add(
        errors,
        schemas.get("asset-request.schema.json", {})
        .get("properties", {})
        .get("asset_index", {})
        .get("maximum")
        == MAX_TEXTURE_REFS - 1,
        "asset-request schema index must match the current parser ceiling",
    )
    asset_template = read_json(PACKAGE / "templates/asset-request.json")
    validate_schema_instance(asset_template, schemas.get("asset-request.schema.json", {}), "asset template schema", errors)
    validate_asset_request(asset_template, "asset template", errors)
    validate_schema_instance(
        read_json(PACKAGE / "templates/implementation-plan.json"),
        schemas.get("implementation-plan.schema.json", {}),
        "implementation-plan template schema",
        errors,
    )
    media_template = read_json(PACKAGE / "templates/media-operation-request.json")
    validate_schema_instance(
        media_template,
        schemas.get("media-operation-request.schema.json", {}),
        "media-operation template schema",
        errors,
    )
    validate_media_request(media_template, "media-operation template", errors)
    validate_schema_instance(
        read_json(PACKAGE / "templates/modifier-manifest.json"),
        schemas.get("modifier-manifest.schema.json", {}),
        "modifier-manifest template schema",
        errors,
    )
    validate_modifier_manifest(read_json(PACKAGE / "templates/modifier-manifest.json"), "modifier-manifest template", errors)
    for required_prompt_term in (
        "Production-quality",
        "one confident style",
        "seamless axes",
        "no gutters",
        "row zero",
    ):
        add(
            errors,
            required_prompt_term in asset_template.get("prompt", ""),
            f"asset template prompt is missing design invariant: {required_prompt_term}",
        )
    validate_plan(
        read_json(PACKAGE / "templates/implementation-plan.json"),
        {
            "implementation_hint": "layers",
            "image_sha256": "sha256:" + "a" * 64,
        },
        {
            "action": "prototype_approval",
            "artifact_sha256": "sha256:" + "a" * 64,
            "decision_id": "decision_replace_me",
        },
        "template",
        errors,
    )

    fixture_root = PACKAGE / "fixtures"
    fixture_directories = {entry.name for entry in fixture_root.iterdir() if entry.is_dir()}
    add(
        errors,
        fixture_directories == ROUTES | {DRAFT_FIXTURE_DIR, MODIFIER_FIXTURE_DIR},
        "fixtures must cover four routes, worker handoff, and modifier workflow",
    )
    for route in sorted(ROUTES):
        directory = fixture_root / route
        manifest = read_json(directory / "prototype-manifest.json")
        approval = read_json(directory / "approval.json")
        plan = read_json(directory / "implementation-plan.json")
        document = read_json(directory / "skin.skin.json")
        validate_manifest(manifest, route, errors)
        validate_plan(plan, manifest, approval, route, errors)
        validate_schema_instance(plan, schemas.get("implementation-plan.schema.json", {}), f"{route} plan schema", errors)
        for asset_index, asset in enumerate(plan.get("asset_plan", [])):
            if asset.get("kind") != "sheet" or not isinstance(asset.get("desired_fps"), (int, float)):
                continue
            derived_rows = max(2, math.ceil(document["period_ms"] * asset["desired_fps"] / 1_000))
            add(
                errors,
                asset.get("frames") == derived_rows,
                f"{route}: asset_plan[{asset_index}] frames must derive from period_ms and desired_fps",
            )
        expected_path = route.replace("-", "_")
        add(
            errors,
            plan.get("path") == expected_path,
            f"{route}: fixture path does not match directory",
        )
        validate_document(document, plan, route, errors)

    draft_directory = fixture_root / DRAFT_FIXTURE_DIR
    draft_plan = read_json(draft_directory / "implementation-plan.json")
    draft_requests = read_json(draft_directory / "tool-requests.json")
    draft = read_json(draft_directory / "skin-draft.valid.json")
    validate_plan(
        draft_plan,
        {"image_sha256": "sha256:" + "a" * 64},
        {
            "action": "prototype_approval",
            "artifact_sha256": "sha256:" + "a" * 64,
            "decision_id": "fixture_worker_draft",
        },
        "worker draft plan",
        errors,
    )
    validate_worker_draft(draft, draft_plan, draft_requests, "worker draft", errors)
    validate_schema_instance(draft_plan, schemas.get("implementation-plan.schema.json", {}), "worker draft plan schema", errors)
    for index, request in enumerate(draft_requests):
        validate_schema_instance(
            request.get("arguments", {}),
            schemas.get("asset-request.schema.json", {}),
            f"worker draft request {index} schema",
            errors,
        )

    modifier_directory = fixture_root / MODIFIER_FIXTURE_DIR
    selection_path = modifier_directory / "draft-selection.json"
    modifier_plan = read_json(modifier_directory / "implementation-plan.json")
    add(
        errors,
        modifier_plan.get("input_authority", {}).get("authority_record_sha256")
        == f"sha256:{sha256_file(selection_path)}",
        "modifier workflow must bind the exact retained draft-selection record",
    )
    validate_plan(
        modifier_plan,
        {"image_sha256": "sha256:" + "8" * 64},
        {},
        "modifier workflow",
        errors,
    )
    validate_schema_instance(
        modifier_plan,
        schemas.get("implementation-plan.schema.json", {}),
        "modifier workflow plan schema",
        errors,
    )
    component_keys = [item.get("component_key") for item in modifier_plan.get("modifier_plan", [])]
    add(errors, component_keys == ["T1", "T2", "B1", "H"], "modifier workflow must keep T1/T2/B1/H as four distinct records")
    add(
        errors,
        len({item.get("texture_name") for item in modifier_plan.get("modifier_plan", [])}) == 4
        and len({item.get("image_layer_name") for item in modifier_plan.get("modifier_plan", [])}) == 4,
        "modifier workflow must keep one texture and image layer per component",
    )
    media_requests = read_json(modifier_directory / "media-requests.json")
    for index, request in enumerate(media_requests):
        validate_schema_instance(
            request,
            schemas.get("media-operation-request.schema.json", {}),
            f"modifier media request {index} schema",
            errors,
        )
        validate_media_request(request, f"modifier media request {index}", errors)
    modifier_manifests: dict[str, tuple[str, dict[str, Any]]] = {}
    for component in ("T1", "T2", "B1", "H"):
        manifest_path = modifier_directory / f"{component}.modifier-manifest.json"
        modifier_manifest = read_json(manifest_path)
        validate_schema_instance(
            modifier_manifest,
            schemas.get("modifier-manifest.schema.json", {}),
            f"{component} modifier manifest fixture schema",
            errors,
        )
        validate_modifier_manifest(modifier_manifest, f"{component} modifier manifest fixture", errors)
        modifier_manifests[component] = (sha256_file(manifest_path), modifier_manifest)
    validate_modifier_workflow_links(
        modifier_plan,
        media_requests,
        modifier_manifests,
        "modifier workflow",
        errors,
    )

    fabricated_errors: list[str] = []
    validate_worker_draft(
        read_json(draft_directory / "skin-draft.fabricated.invalid.json"),
        draft_plan,
        draft_requests,
        "fabricated draft",
        fabricated_errors,
    )
    add(
        errors,
        any("must use pending:asset:0" in error for error in fabricated_errors)
        and any("cannot fabricate a descriptor" in error for error in fabricated_errors),
        "negative draft fixture must prove fabricated refs/descriptors are rejected",
    )

    wrong_index_errors: list[str] = []
    validate_worker_draft(
        read_json(draft_directory / "skin-draft.wrong-index.invalid.json"),
        draft_plan,
        draft_requests,
        "wrong-index draft",
        wrong_index_errors,
    )
    add(
        errors,
        any("must use pending:asset:0" in error for error in wrong_index_errors),
        "negative draft fixture must prove mismatched sentinels are rejected",
    )

    final_errors: list[str] = []
    validate_worker_draft(
        draft,
        draft_plan,
        draft_requests,
        "unbound final",
        final_errors,
        allow_pending=False,
    )
    add(
        errors,
        any("survived exact binding" in error for error in final_errors),
        "worker draft fixture must be rejected as an unbound final document",
    )

    for wrapper in (
        REPO / ".claude/skills/author-skin/SKILL.md",
        REPO / ".agents/skills/author-skin/SKILL.md",
        REPO / "hermes/skills/author-skin/SKILL.md",
    ):
        add(
            errors,
            wrapper.is_file(),
            f"discovery wrapper is missing: {wrapper.relative_to(REPO)}",
        )
        if wrapper.is_file():
            validate_frontmatter(wrapper, errors)
            add(
                errors,
                "../../../skills/author-skin/SKILL.md" in wrapper.read_text(encoding="utf-8"),
                f"wrapper does not point at canonical package: {wrapper.relative_to(REPO)}",
            )

    add(
        errors,
        not (REPO / ".claude/skills/author-skin/templates/custom_skin.rs.tmpl").exists(),
        "stale Rust escalation template still exists",
    )
    return errors


def run_cargo_validation() -> int:
    paths = [PACKAGE / "templates/skin-v2.template.json"]
    paths.extend(sorted((PACKAGE / "fixtures").glob("*/skin.skin.json")))
    paths.append(PACKAGE / "fixtures/worker-drafts/skin-anchor.externally-tagged.valid.json")
    command = [
        "cargo",
        "run",
        "-q",
        "-p",
        "skin-schema",
        "--bin",
        "validate-skin",
        "--",
        *(str(path) for path in paths),
    ]
    accepted = subprocess.run(command, cwd=REPO, check=False)
    if accepted.returncode != 0:
        return accepted.returncode

    invalid = PACKAGE / "fixtures/worker-drafts/skin-anchor.flattened.invalid.json"
    rejected = subprocess.run(
        [
            "cargo",
            "run",
            "-q",
            "-p",
            "skin-schema",
            "--bin",
            "validate-skin",
            "--",
            str(invalid),
        ],
        cwd=REPO,
        check=False,
        capture_output=True,
        text=True,
    )
    if rejected.returncode != 1 or "expected struct variant" not in rejected.stderr:
        print(
            "validate-skin did not reject flattened numeric anchors at the Rust enum boundary",
            file=sys.stderr,
        )
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cargo", action="store_true", help="also validate fixture SkinDocs")
    parser.add_argument("--json", action="store_true", help="emit a machine-readable result")
    parser.add_argument(
        "--candidate-playbook",
        type=Path,
        help="verify that a GEPA candidate changes only the marked playbook body",
    )
    args = parser.parse_args()

    errors = validate_package()
    if args.candidate_playbook:
        errors.extend(validate_playbook_candidate(args.candidate_playbook.read_text(encoding="utf-8")))
    if not errors and args.cargo and run_cargo_validation() != 0:
        errors.append("cargo SkinDoc validation failed")

    if args.json:
        print(json.dumps({"ok": not errors, "errors": errors}, indent=2))
    elif errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
    else:
        print("author-skin package: ok")
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
