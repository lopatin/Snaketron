"""Semantic validation of the pure worker-to-driver handoff.

The task worker may describe desired external work, but it cannot mint texture
hashes or descriptors.  This module validates the complete draft before the
driver stores it or performs any asset/provider operation.
"""

from __future__ import annotations

import json
import math
from typing import Any

from jsonschema import Draft202012Validator

from .domain import AssetPlan, WorkerResult


class WorkerContractError(ValueError):
    """The worker returned structurally valid JSON that violates the skill contract."""


def validate_worker_handoff(
    result: WorkerResult,
    skill_files: dict[str, str],
    capabilities: dict[str, Any],
) -> None:
    if result.failure is not None:
        raise WorkerContractError(f"worker reported failure: {result.failure}")
    document = result.skin_document
    if document.get("schema_version") != 2:
        raise WorkerContractError("worker draft must be a SkinDoc v2 document")
    validate_plan_resource_limits(
        result.implementation_plan,
        capabilities,
        period_ms=document.get("period_ms"),
    )

    try:
        schema = json.loads(skill_files["schemas/asset-request.schema.json"])
    except (KeyError, json.JSONDecodeError) as error:
        raise WorkerContractError("pinned skill bundle has no valid asset-request schema") from error
    validator = Draft202012Validator(schema)

    requests = result.tool_requests
    if any(request.kind != "generate_asset" for request in requests):
        raise WorkerContractError(
            "initial worker draft supports only generate_asset; repair/edit operations belong to the driver"
        )
    if len(requests) != len(result.implementation_plan.asset_plan):
        raise WorkerContractError("worker must emit exactly one generate request per planned asset")

    textures = document.get("textures", [])
    if not isinstance(textures, list):
        raise WorkerContractError("SkinDoc textures must be an array")
    names = [texture.get("name") for texture in textures if isinstance(texture, dict)]
    if len(names) != len(set(names)):
        raise WorkerContractError("SkinDoc draft texture names must be unique")
    layers = _flatten(document.get("layers", []))

    by_index: dict[int, dict[str, Any]] = {}
    for request in requests:
        arguments = request.arguments
        errors = sorted(validator.iter_errors(arguments), key=lambda item: list(item.path))
        if errors:
            first = errors[0]
            location = ".".join(str(part) for part in first.absolute_path) or "$"
            raise WorkerContractError(f"asset request {location}: {first.message}")
        index = arguments["asset_index"]
        if index in by_index:
            raise WorkerContractError(f"duplicate asset request index {index}")
        by_index[index] = arguments

    expected_pending: set[str] = set()
    for index, asset in enumerate(result.implementation_plan.asset_plan):
        arguments = by_index.get(index)
        if arguments is None:
            raise WorkerContractError(f"missing asset request index {index}")
        _validate_asset(index, asset, arguments)
        texture_name = arguments["texture_name"]
        matches = [texture for texture in textures if texture.get("name") == texture_name]
        if len(matches) != 1:
            raise WorkerContractError(f"asset {index} must name one unique SkinDoc texture, found {len(matches)}")
        texture = matches[0]
        sentinel = f"pending:asset:{index}"
        expected_pending.add(sentinel)
        if texture.get("ref") != sentinel:
            raise WorkerContractError(f"asset {index} draft must use exact ref {sentinel}")
        if texture.get("kind") != asset.kind:
            raise WorkerContractError(f"asset {index} draft texture kind differs from plan")
        if "descriptor" in texture:
            raise WorkerContractError(f"asset {index} draft must not fabricate a descriptor")
        if not any(
            layer.get("source", {}).get("type") == "image" and layer.get("source", {}).get("texture") == texture_name
            for layer in layers
        ):
            raise WorkerContractError(f"asset {index} draft texture is not used by an image layer")

    actual_pending = {
        texture.get("ref")
        for texture in textures
        if isinstance(texture.get("ref"), str) and texture["ref"].startswith("pending:")
    }
    if actual_pending != expected_pending:
        raise WorkerContractError(f"pending texture refs differ from planned assets: {sorted(actual_pending)}")


def effective_sheet_frame_rows(asset: AssetPlan, period_ms: Any, capabilities: dict[str, Any]) -> int:
    """Derive Y from requested motion rate, period, and immutable runtime bounds."""

    if asset.kind != "sheet" or asset.desired_fps is None:
        raise WorkerContractError("sheet frame derivation requires a sheet asset with desired_fps")
    try:
        period = float(period_ms)
        limits = capabilities["limits"]
        max_fps = float(limits["max_sprite_frame_rate_fps"])
        max_rows = int(limits["max_sprite_frame_rows"])
        max_dimension = int(limits["max_texture_dimension_px"])
        max_decoded = int(limits["max_texture_decoded_bytes"])
    except (KeyError, TypeError, ValueError) as error:
        raise WorkerContractError("pinned capabilities do not declare sprite sampling limits") from error
    if not math.isfinite(period) or period <= 0:
        raise WorkerContractError("SkinDoc period_ms must be positive for sprite sampling")
    if float(asset.desired_fps) > max_fps:
        raise WorkerContractError(f"sheet desired_fps {asset.desired_fps:g} exceeds pinned renderer limit {max_fps:g}")
    width_px = asset.natural_length_cells * asset.texels_per_cell
    decoded_per_row = width_px * asset.texels_per_cell * 4
    bounded_rows = min(
        max_rows,
        max_dimension // asset.texels_per_cell,
        max_decoded // decoded_per_row,
    )
    if bounded_rows < 2:
        raise WorkerContractError("pinned image limits cannot hold a two-frame sprite sheet")
    requested_rows = max(2, math.ceil(period * float(asset.desired_fps) / 1_000))
    return min(requested_rows, bounded_rows)


def validate_plan_resource_limits(
    plan: Any,
    capabilities: dict[str, Any],
    *,
    period_ms: Any | None = None,
) -> None:
    """Reject dangerous image geometry before a provider call or allocation."""

    assets = plan.asset_plan
    if not assets:
        return
    try:
        limits = capabilities["limits"]
        max_refs = int(limits["max_texture_refs"])
        max_dimension = int(limits["max_texture_dimension_px"])
        max_rows = int(limits["max_sprite_frame_rows"])
        max_decoded = int(limits["max_texture_decoded_bytes"])
    except (KeyError, TypeError, ValueError) as error:
        raise WorkerContractError("pinned capabilities do not declare image resource limits") from error

    if len(assets) > max_refs:
        raise WorkerContractError(f"asset plan has {len(assets)} textures; pinned limit is {max_refs}")
    for index, asset in enumerate(assets):
        rows = asset.frames if asset.kind == "sheet" else 1
        width = asset.natural_length_cells * asset.texels_per_cell
        height = rows * asset.texels_per_cell
        if asset.kind == "sheet" and rows > max_rows:
            raise WorkerContractError(f"asset {index} has {rows} frame rows; pinned limit is {max_rows}")
        if asset.kind == "sheet" and period_ms is not None:
            derived = effective_sheet_frame_rows(asset, period_ms, capabilities)
            if rows != derived:
                raise WorkerContractError(
                    f"asset {index} declares {rows} frame rows; desired_fps={asset.desired_fps:g} "
                    f"and period_ms={float(period_ms):g} derive {derived}"
                )
        if width > max_dimension or height > max_dimension:
            raise WorkerContractError(
                f"asset {index} grid is {width}x{height}px; pinned per-axis limit is {max_dimension}px"
            )
        decoded = width * height * 4
        if decoded > max_decoded:
            raise WorkerContractError(f"asset {index} requires {decoded} decoded bytes; pinned limit is {max_decoded}")


def assert_resolved_document(document: dict[str, Any]) -> None:
    """Refuse publication/register if any worker placeholder survives binding."""

    for texture in document.get("textures", []):
        reference = texture.get("ref")
        if isinstance(reference, str) and reference.startswith("pending:"):
            raise WorkerContractError(f"unresolved texture placeholder survived: {reference}")
        if reference and (not isinstance(reference, str) or not reference.startswith("sha256:")):
            raise WorkerContractError(f"texture has non-content-addressed ref: {reference!r}")
        if reference and not isinstance(texture.get("descriptor"), dict):
            raise WorkerContractError("generated texture has no immutable descriptor")


def _validate_asset(index: int, asset: AssetPlan, arguments: dict[str, Any]) -> None:
    grid = arguments["grid"]
    expected_rows = asset.frames if asset.kind == "sheet" else 1
    expected_axes = ({"y"} if asset.kind == "sheet" else set()) | ({"x"} if asset.fit == "tile" else set())
    checks = {
        "operation": (arguments["operation"], "generate"),
        "capability_role": (arguments["capability_role"], "image_generator"),
        "kind": (arguments["kind"], asset.kind),
        "body_columns": (grid["body_columns"], asset.natural_length_cells),
        "frame_rows": (grid["frame_rows"], expected_rows),
        "texels_per_cell": (grid["texels_per_cell"], asset.texels_per_cell),
        "width_px": (arguments["width_px"], asset.natural_length_cells * asset.texels_per_cell),
        "height_px": (arguments["height_px"], expected_rows * asset.texels_per_cell),
    }
    for field, (actual, expected) in checks.items():
        if actual != expected:
            raise WorkerContractError(f"asset {index} {field} differs from plan: {actual!r} != {expected!r}")
    if set(arguments["required_wrap_axes"]) != expected_axes:
        raise WorkerContractError(f"asset {index} wrap axes differ from kind/fit usage")


def _flatten(layers: Any) -> list[dict[str, Any]]:
    if not isinstance(layers, list):
        return []
    flattened: list[dict[str, Any]] = []
    for layer in layers:
        if not isinstance(layer, dict):
            continue
        if layer.get("type") == "group":
            flattened.extend(_flatten(layer.get("layers", [])))
        else:
            flattened.append(layer)
    return flattened
