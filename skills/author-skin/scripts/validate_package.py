#!/usr/bin/env python3
"""Validate the canonical author-skin package and its executable fixtures."""

from __future__ import annotations

import argparse
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
ROUTES = {"layers", "texture", "sprite-sheet", "hybrid"}
DRAFT_FIXTURE_DIR = "worker-drafts"
PLAN_PATHS = {"layers", "texture", "sprite_sheet", "hybrid"}
ASSET_KINDS = {"coat", "sheet", "overlay"}
AXES = {"x", "y"}

REQUIRED_FILES = {
    "SKILL.md",
    "agents/openai.yaml",
    "optimization-boundary.json",
    "references/contract.md",
    "references/integration.md",
    "references/layers-effects.md",
    "references/playbook.md",
    "references/prototypes.md",
    "references/textures-sprites.md",
    "references/validation.md",
    "schemas/asset-request.schema.json",
    "schemas/implementation-plan.schema.json",
    "schemas/prototype-manifest.schema.json",
    "templates/asset-request.json",
    "templates/implementation-plan.json",
    "templates/skin-v2.template.json",
}

PLAN_KEYS = {
    "path",
    "rationale",
    "fidelity_features",
    "layer_plan",
    "asset_plan",
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
    "anchor",
    "fit",
    "fade",
    "prompt",
}


def read_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def is_hash(value: Any) -> bool:
    return isinstance(value, str) and HASH.fullmatch(value) is not None


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
        contract.count("<!-- FACTORY_LOCKED:START -->") == 1
        and contract.count("<!-- FACTORY_LOCKED:END -->") == 1,
        "contract must have one locked boundary",
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
    required = {
        "brief",
        "palette_intent",
        "motion_intent",
        "implementation_hint",
        "hint_rationale",
        "prompt",
        "model_config",
        "image_sha256",
    }
    add(
        errors, set(manifest) == required, f"{label}: prototype manifest fields drifted"
    )
    add(
        errors,
        manifest.get("implementation_hint") in PLAN_PATHS,
        f"{label}: invalid implementation hint",
    )
    add(errors, is_hash(manifest.get("image_sha256")), f"{label}: invalid image hash")
    add(
        errors,
        isinstance(manifest.get("model_config"), str)
        and bool(manifest["model_config"]),
        f"{label}: model_config must name a stored configuration",
    )


def validate_asset_request(
    request: dict[str, Any], label: str, errors: list[str]
) -> None:
    required = {
        "schema_version",
        "request_id",
        "asset_index",
        "texture_name",
        "capability_role",
        "operation",
        "kind",
        "input_artifacts",
        "prompt",
        "width_px",
        "height_px",
        "grid",
        "required_wrap_axes",
        "transparency",
        "row_zero",
        "repair",
    }
    add(errors, set(request) == required, f"{label}: asset-request fields drifted")
    add(errors, request.get("schema_version") == 1, f"{label}: asset schema must be 1")
    add(
        errors,
        isinstance(request.get("asset_index"), int) and request["asset_index"] >= 0,
        f"{label}: invalid asset index",
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
        request.get("width_px")
        == grid.get("body_columns", 0) * grid.get("texels_per_cell", 0),
        f"{label}: width must encode independent body columns",
    )
    add(
        errors,
        request.get("height_px")
        == grid.get("frame_rows", 0) * grid.get("texels_per_cell", 0),
        f"{label}: height must encode independent frame rows",
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
        isinstance(grid.get("texels_per_cell"), int)
        and 4 <= grid["texels_per_cell"] <= 128,
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
    add(
        errors,
        approval.get("action") == "prototype_approval",
        f"{label}: wrong approval action",
    )
    add(
        errors,
        manifest.get("image_sha256") == approval.get("artifact_sha256"),
        f"{label}: approval and manifest must bind the same image",
    )
    add(
        errors,
        bool(approval.get("decision_id")),
        f"{label}: approval decision id is required",
    )
    add(errors, bool(plan.get("rationale")), f"{label}: route rationale is required")

    axes = plan.get("required_wrap_axes", [])
    add(
        errors,
        set(axes) <= AXES and len(axes) == len(set(axes)),
        f"{label}: invalid wrap axes",
    )
    assets = plan.get("asset_plan", [])
    add(errors, len(assets) <= 4, f"{label}: asset plan exceeds max texture refs")
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
    for index, asset in enumerate(assets):
        asset_label = f"{label}: asset_plan[{index}]"
        add(errors, set(asset) == ASSET_KEYS, f"{asset_label}: fields drifted")
        kind = asset.get("kind")
        add(errors, kind in ASSET_KINDS, f"{asset_label}: invalid kind")
        add(
            errors,
            isinstance(asset.get("natural_length_cells"), int)
            and 1 <= asset["natural_length_cells"] <= 128,
            f"{asset_label}: invalid independent X body cells",
        )
        add(
            errors,
            isinstance(asset.get("frames"), int) and 1 <= asset["frames"] <= 120,
            f"{asset_label}: invalid independent Y frame count",
        )
        if isinstance(asset.get("natural_length_cells"), int) and isinstance(asset.get("texels_per_cell"), int):
            add(
                errors,
                asset["natural_length_cells"] * asset["texels_per_cell"] <= 2048,
                f"{asset_label}: width exceeds the current capability bound",
            )
        if isinstance(asset.get("frames"), int) and isinstance(asset.get("texels_per_cell"), int):
            rows = asset["frames"] if kind == "sheet" else 1
            add(
                errors,
                rows * asset["texels_per_cell"] <= 2048,
                f"{asset_label}: height exceeds the current capability bound",
            )
        add(
            errors,
            isinstance(asset.get("texels_per_cell"), int)
            and 4 <= asset["texels_per_cell"] <= 128,
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
        set(axes) == derived_axes,
        f"{label}: wrap axes must be derived from kind and fit",
    )


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
    generated = [
        request for request in tool_requests if request.get("kind") == "generate_asset"
    ]
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
        add(
            errors, arguments is not None, f"{label}: missing request for asset {index}"
        )
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
        matches = [
            texture for texture in textures if texture.get("name") == texture_name
        ]
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
        add(
            errors,
            numeric_constant(source.get("drift_cells", 0)),
            f"{label}: image drift must stay constant until capability enables it",
        )
        texture = textures[texture_name]
        descriptor = texture.get("descriptor")
        matching_assets = [
            asset
            for asset in assets
            if asset.get("kind") == texture.get("kind")
            and isinstance(descriptor, dict)
            and descriptor.get("body_columns") == asset.get("natural_length_cells")
            and descriptor.get("frame_rows")
            == (asset.get("frames") if asset.get("kind") == "sheet" else None)
        ]
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
            if candidate.get("region") == layer.get("region")
            and candidate.get("type") == "ribbon"
            and candidate.get("source", {}).get("type") != "image"
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
            any(
                variant.get("content_ref") == texture.get("ref") for variant in variants
            ),
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

    for schema in (PACKAGE / "schemas").glob("*.json"):
        read_json(schema)
    validate_asset_request(
        read_json(PACKAGE / "templates/asset-request.json"),
        "asset template",
        errors,
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
    fixture_directories = {
        entry.name for entry in fixture_root.iterdir() if entry.is_dir()
    }
    add(
        errors,
        fixture_directories == ROUTES | {DRAFT_FIXTURE_DIR},
        "fixtures must cover four routes plus the worker draft handoff",
    )
    for route in sorted(ROUTES):
        directory = fixture_root / route
        manifest = read_json(directory / "prototype-manifest.json")
        approval = read_json(directory / "approval.json")
        plan = read_json(directory / "implementation-plan.json")
        document = read_json(directory / "skin.skin.json")
        validate_manifest(manifest, route, errors)
        validate_plan(plan, manifest, approval, route, errors)
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
        and any(
            "cannot fabricate a descriptor" in error for error in fabricated_errors
        ),
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
                "../../../skills/author-skin/SKILL.md"
                in wrapper.read_text(encoding="utf-8"),
                f"wrapper does not point at canonical package: {wrapper.relative_to(REPO)}",
            )

    add(
        errors,
        not (
            REPO / ".claude/skills/author-skin/templates/custom_skin.rs.tmpl"
        ).exists(),
        "stale Rust escalation template still exists",
    )
    return errors


def run_cargo_validation() -> int:
    paths = [PACKAGE / "templates/skin-v2.template.json"]
    paths.extend(sorted((PACKAGE / "fixtures").glob("*/skin.skin.json")))
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
    return subprocess.run(command, cwd=REPO, check=False).returncode


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cargo", action="store_true", help="also validate fixture SkinDocs"
    )
    parser.add_argument(
        "--json", action="store_true", help="emit a machine-readable result"
    )
    parser.add_argument(
        "--candidate-playbook",
        type=Path,
        help="verify that a GEPA candidate changes only the marked playbook body",
    )
    args = parser.parse_args()

    errors = validate_package()
    if args.candidate_playbook:
        errors.extend(
            validate_playbook_candidate(
                args.candidate_playbook.read_text(encoding="utf-8")
            )
        )
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
