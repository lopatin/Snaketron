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

from .domain import AssetPlan, ModifierPlan, WorkerResult


class WorkerContractError(ValueError):
    """The worker returned structurally valid JSON that violates the skill contract."""


def expected_materialized_catalog_record(
    asset: AssetPlan,
    modifier: ModifierPlan,
) -> dict[str, Any]:
    """Return the complete hash-bound record a materialized plan must reuse.

    Keeping this construction shared between the worker boundary and the
    executing driver prevents the author from validating one set of fields
    while the forge later consumes a looser subset.
    """

    if modifier.source_mode == "direct_generate":
        raise WorkerContractError("direct-generated modifiers have no materialized catalog record")
    return {
        "logical_key": modifier.logical_key,
        "component_key": modifier.component_key,
        "texture_name": modifier.texture_name,
        "source_mode": modifier.source_mode,
        "source_object_sha256": modifier.source_object_sha256,
        "modifier_manifest_sha256": modifier.modifier_manifest_sha256,
        "provenance_sha256": modifier.provenance_sha256,
        "license_id": modifier.license_id,
        "authorized_lineage_ids": modifier.authorized_lineage_ids,
        "extraction": modifier.extraction.model_dump(mode="json") if modifier.extraction else None,
        "video": modifier.video.model_dump(mode="json") if modifier.video else None,
        "asset": asset.model_dump(mode="json"),
    }


def validate_worker_handoff(
    result: WorkerResult,
    skill_files: dict[str, str],
    capabilities: dict[str, Any],
    trusted_authority: dict[str, Any] | None = None,
    materialized_modifier_catalog: dict[str, Any] | None = None,
    allow_direct_generation: bool = True,
    *,
    allow_legacy: bool = False,
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
    derived_wrap_axes = {
        axis
        for asset in result.implementation_plan.asset_plan
        for axis in (("y",) if asset.kind == "sheet" else ()) + (("x",) if asset.fit == "tile" else ())
    }
    if set(result.implementation_plan.required_wrap_axes) != derived_wrap_axes:
        raise WorkerContractError("implementation required_wrap_axes differ from asset kind/fit usage")
    authority = result.implementation_plan.input_authority
    if trusted_authority is None and not allow_legacy:
        raise WorkerContractError("current worker handoff requires trusted input authority from WorkerRequest")
    if authority is None and not allow_legacy:
        raise WorkerContractError("current worker plan must declare exact input_authority")
    if authority is not None and authority.mode not in {"approved_prototype", "draft_submission"}:
        raise WorkerContractError("worker input authority mode is unsupported")
    if (
        authority is not None
        and trusted_authority is not None
        and authority.model_dump(mode="json") != trusted_authority
    ):
        raise WorkerContractError("implementation input_authority differs from trusted WorkerRequest authority")
    if result.implementation_plan.common_period_ms is None and not allow_legacy:
        raise WorkerContractError("current worker plan must declare common_period_ms")
    if result.implementation_plan.common_period_ms is not None:
        try:
            document_period = float(document.get("period_ms"))
        except (TypeError, ValueError) as error:
            raise WorkerContractError("SkinDoc period_ms is invalid") from error
        if document_period != float(result.implementation_plan.common_period_ms):
            raise WorkerContractError("implementation common_period_ms differs from SkinDoc period_ms")
    if not allow_legacy:
        try:
            plan_schema = json.loads(skill_files["schemas/implementation-plan.schema.json"])
        except (KeyError, json.JSONDecodeError) as error:
            raise WorkerContractError("pinned skill bundle has no valid current implementation-plan schema") from error
        plan_errors = sorted(
            Draft202012Validator(plan_schema).iter_errors(result.implementation_plan.model_dump(mode="json")),
            key=lambda item: list(item.path),
        )
        if plan_errors:
            first = plan_errors[0]
            location = ".".join(str(part) for part in first.absolute_path) or "$"
            raise WorkerContractError(f"implementation plan {location}: {first.message}")
    materialized_records = (
        materialized_modifier_catalog.get("modifiers", []) if isinstance(materialized_modifier_catalog, dict) else []
    )
    if not isinstance(materialized_records, list):
        raise WorkerContractError("materialized modifier catalog must contain a modifiers array")
    materialized_by_key = {
        item.get("logical_key"): item
        for item in materialized_records
        if isinstance(item, dict) and isinstance(item.get("logical_key"), str)
    }
    if len(materialized_by_key) != len(materialized_records):
        raise WorkerContractError("materialized modifier catalog keys must be unique exact records")
    if authority is not None and authority.mode == "draft_submission":
        planned_materialized_keys = [
            item.logical_key
            for item in result.implementation_plan.modifier_plan
            if item.source_mode != "direct_generate"
        ]
        if len(set(planned_materialized_keys)) != len(planned_materialized_keys):
            raise WorkerContractError("draft materialized modifier logical keys must be unique")
        if set(planned_materialized_keys) != set(materialized_by_key):
            raise WorkerContractError("draft author must bind the exact complete materialized modifier catalog")
    materialized_indexes = {
        item.asset_index for item in result.implementation_plan.modifier_plan if item.source_mode != "direct_generate"
    }
    if any(
        asset.raster_overhang_px > 0 and index not in materialized_indexes
        for index, asset in enumerate(result.implementation_plan.asset_plan)
    ):
        raise WorkerContractError(
            "platform_gap: raster_overhang_px is supported by the renderer/direct-host contract, "
            "but only an exact materialized modifier path can preserve it"
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
    direct_indexes = {
        item.asset_index for item in result.implementation_plan.modifier_plan if item.source_mode == "direct_generate"
    }
    if allow_legacy and not result.implementation_plan.modifier_plan:
        direct_indexes = set(range(len(result.implementation_plan.asset_plan)))
    if direct_indexes and not allow_direct_generation:
        raise WorkerContractError("platform_gap: this host forbids non-endpoint direct asset generation")
    if len(requests) != len(direct_indexes):
        raise WorkerContractError("worker must emit one generate request only for each direct-generated asset")

    textures = document.get("textures", [])
    if not isinstance(textures, list):
        raise WorkerContractError("SkinDoc textures must be an array")
    names = [texture.get("name") for texture in textures if isinstance(texture, dict)]
    if len(names) != len(set(names)):
        raise WorkerContractError("SkinDoc draft texture names must be unique")
    layers = _flatten(document.get("layers", []))
    modifiers = {item.asset_index: item for item in result.implementation_plan.modifier_plan}
    if not allow_legacy and len(modifiers) != len(result.implementation_plan.asset_plan):
        raise WorkerContractError("modifier plan must map one-to-one to planned assets")

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
        modifier = modifiers.get(index)
        if modifier is None:
            if not allow_legacy:
                raise WorkerContractError(f"asset {index} has no exact modifier plan")
            if arguments is None:
                raise WorkerContractError(f"missing legacy asset request index {index}")
            _validate_asset(index, asset, arguments)
        elif modifier.source_mode == "direct_generate":
            if arguments is None:
                raise WorkerContractError(f"missing direct asset request index {index}")
            _validate_asset(index, asset, arguments)
            expected_modifier = {
                "logical_key": modifier.logical_key,
                "component_key": modifier.component_key,
                "provenance_sha256": modifier.provenance_sha256,
                "license_id": modifier.license_id,
                "authorized_lineage_ids": modifier.authorized_lineage_ids,
            }
            if arguments.get("modifier") != expected_modifier:
                raise WorkerContractError(f"asset {index} modifier intent differs from plan")
        else:
            if arguments is not None:
                raise WorkerContractError(f"materialized asset {index} cannot request a new provider generation")
            retained = materialized_by_key.get(modifier.logical_key)
            if retained is None:
                raise WorkerContractError(f"asset {index} is absent from the trusted materialized catalog")
            expected_catalog = expected_materialized_catalog_record(asset, modifier)
            for field, expected in expected_catalog.items():
                if retained.get(field) != expected:
                    raise WorkerContractError(
                        f"asset {index} catalog {field} differs from the exact implementation plan"
                    )
            arguments = {
                "texture_name": modifier.texture_name,
            }
        if modifier is not None:
            expected_span_mode = {
                "whole": "whole",
                "head": "head_cells",
                "tail": "tail_fraction",
            }[asset.anchor]
            if modifier.span_limit_mode != expected_span_mode:
                raise WorkerContractError(f"asset {index} modifier span limit differs from anchor")
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
        if modifier is not None:
            matches = [
                (position, layer)
                for position, layer in enumerate(layers)
                if layer.get("name") == modifier.image_layer_name
                and layer.get("source", {}).get("type") == "image"
                and layer.get("source", {}).get("texture") == texture_name
            ]
            if len(matches) != 1:
                raise WorkerContractError(f"asset {index} must own one exact named image layer")
            position, image_layer = matches[0]
            _validate_modifier_layer(index, asset, modifier, image_layer)
            fallback_positions = [
                candidate_position
                for candidate_position, candidate in enumerate(layers[:position])
                if candidate.get("name") == modifier.fallback_layer_name
                and _fallback_covers_image(candidate, image_layer)
            ]
            if not fallback_positions:
                raise WorkerContractError(f"asset {index} named fallback must precede its image layer")

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
    row_texels = asset_row_texels(asset)
    decoded_per_row = width_px * row_texels * 4
    bounded_rows = min(
        max_rows,
        max_dimension // row_texels,
        max_decoded // decoded_per_row,
    )
    if bounded_rows < 2:
        raise WorkerContractError("pinned image limits cannot hold a two-frame sprite sheet")
    requested_rows = max(2, math.ceil(period * float(asset.desired_fps) / 1_000))
    return min(requested_rows, bounded_rows)


def asset_row_texels(asset: AssetPlan) -> int:
    """Return body texels plus the scaled per-side 16-grid overhang."""

    scaled = asset.texels_per_cell * asset.raster_overhang_px
    if scaled % 16:
        raise WorkerContractError("raster overhang does not scale exactly at this texture rung")
    return asset.texels_per_cell + 2 * (scaled // 16)


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
        max_skin_decoded = int(limits["max_skin_texture_decoded_bytes"])
        max_skin_compressed = int(limits["max_skin_texture_compressed_bytes"])
    except (KeyError, TypeError, ValueError) as error:
        raise WorkerContractError("pinned capabilities do not declare image resource limits") from error

    if len(assets) > max_refs:
        raise WorkerContractError(f"asset plan has {len(assets)} textures; pinned limit is {max_refs}")
    aggregate_decoded = 0
    aggregate_png_upper = 0
    for index, asset in enumerate(assets):
        rows = asset.frames if asset.kind == "sheet" else 1
        width = asset.natural_length_cells * asset.texels_per_cell
        height = rows * asset_row_texels(asset)
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
        aggregate_decoded += decoded
        scanline_bytes = decoded + height
        stored_block_overhead = ((scanline_bytes + 65_534) // 65_535) * 5
        aggregate_png_upper += scanline_bytes + stored_block_overhead + 1_024
    if aggregate_decoded > max_skin_decoded:
        raise WorkerContractError(
            f"asset plan requires {aggregate_decoded} conservatively bounded decoded bytes; "
            f"pinned per-skin limit is {max_skin_decoded}"
        )
    if aggregate_png_upper > max_skin_compressed:
        raise WorkerContractError(
            f"asset plan has a {aggregate_png_upper}-byte conservative PNG upper bound; "
            f"pinned per-skin compressed limit is {max_skin_compressed}"
        )


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
        "raster_overhang_px": (grid["raster_overhang_px"], asset.raster_overhang_px),
        "row_texels": (grid["row_texels"], asset_row_texels(asset)),
        "height_px": (arguments["height_px"], expected_rows * asset_row_texels(asset)),
        "tile_phase_origin": (arguments["tile_phase_origin"], asset.tile_phase_origin),
        "transverse_edge_policy": (
            arguments["edge_contract"]["transverse_policy"],
            asset.transverse_edge_policy,
        ),
    }
    for field, (actual, expected) in checks.items():
        if actual != expected:
            raise WorkerContractError(f"asset {index} {field} differs from plan: {actual!r} != {expected!r}")
    if set(arguments["required_wrap_axes"]) != expected_axes:
        raise WorkerContractError(f"asset {index} wrap axes differ from kind/fit usage")
    if (
        arguments["transparency"] == "required"
        and arguments["edge_contract"]["transverse_policy"] != "fail_closed_transparent_effect"
    ):
        raise WorkerContractError(f"asset {index} transparent effect must fail closed on transverse edge contact")
    if (
        arguments["edge_contract"]["transverse_policy"] == "not_applicable_opaque_fill"
        and arguments["transparency"] != "opaque"
    ):
        raise WorkerContractError(f"asset {index} opaque edge exemption requires opaque bytes")


def _validate_modifier_layer(index: int, asset: AssetPlan, modifier: Any, layer: dict[str, Any]) -> None:
    span = layer.get("span", {})
    origin = span.get("from")
    if modifier.span_limit_mode == "whole":
        if origin != "whole" or span.get("natural") is not None:
            raise WorkerContractError(f"asset {index} whole modifier layer has a bounded or different span")
    elif modifier.span_limit_mode == "head_cells":
        natural = span.get("natural")
        if origin != "head" or not isinstance(natural, (int, float)) or not (0 < natural <= 6):
            raise WorkerContractError(f"asset {index} head modifier must span from head through at most cell 6")
        if float(natural) > float(modifier.span_limit_value):
            raise WorkerContractError(f"asset {index} head modifier exceeds its planned span limit")
    else:
        fraction = origin.get("fraction", {}).get("fraction") if isinstance(origin, dict) else None
        limit = float(modifier.span_limit_value)
        if not isinstance(fraction, (int, float)) or not (1 - limit <= float(fraction) < 1):
            raise WorkerContractError(f"asset {index} tail modifier must occupy at most the final planned fraction")
        if span.get("natural") is not None:
            raise WorkerContractError(f"asset {index} tail-fraction modifier cannot add a fixed natural span")

    source = layer.get("source", {})
    fit = source.get("fit")
    if asset.fit == "tile":
        if not isinstance(fit, dict) or fit.get("type") != "tile":
            raise WorkerContractError(f"asset {index} layer fit differs from tile plan")
        if fit.get("phase_origin", "head") != asset.tile_phase_origin:
            raise WorkerContractError(f"asset {index} tile phase origin differs from plan")
    elif fit != asset.fit and not (isinstance(fit, dict) and fit.get("type") == asset.fit):
        raise WorkerContractError(f"asset {index} layer fit differs from plan")

    fade = source.get("fade")
    lead = fade.get("lead_cells", 0) if isinstance(fade, dict) else 0
    trail = fade.get("trail_cells", 0) if isinstance(fade, dict) else 0
    expected = {
        "none": (False, False),
        "leading": (True, False),
        "trailing": (False, True),
        "both": (True, True),
    }[asset.fade]
    if (lead > 0, trail > 0) != expected:
        raise WorkerContractError(f"asset {index} image fade differs from plan")


def _fallback_covers_image(fallback: dict[str, Any], image: dict[str, Any]) -> bool:
    """Conservatively prove an earlier non-image layer remains readable."""

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
    if fallback.get("type") != "span":
        return False
    source = fallback.get("source", {})
    if source.get("type") != "solid":
        return False
    span_defaults = {"natural": None, "min": 0, "priority": 0}
    fallback_span = {**span_defaults, **fallback.get("span", {})}
    image_span = {**span_defaults, **image.get("span", {})}
    return (
        fallback_span == image_span
        and fallback.get("clip", "silhouette") == image.get("clip", "silhouette")
        and normalized_transform(fallback) == normalized_transform(image)
    )


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
