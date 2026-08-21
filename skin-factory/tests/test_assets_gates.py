from __future__ import annotations

import io
import json
from pathlib import Path

import pytest
from PIL import Image
from pydantic import ValidationError

from snaketron_factory.assets import AssetProcessor
from snaketron_factory.config import FactoryConfig
from snaketron_factory.domain import AssetPlan, GateVerdict, ImplementationPlan
from snaketron_factory.gates import GateRunner
from snaketron_factory.worker_validation import WorkerContractError, validate_plan_resource_limits

REPO_ROOT = Path(__file__).resolve().parents[2]


def _png(width: int = 96, height: int = 96) -> bytes:
    output = io.BytesIO()
    Image.new("RGB", (width, height), (40, 160, 220)).save(output, format="PNG")
    return output.getvalue()


def _asset(*, kind: str = "sheet", fit: str = "tile", frames: int = 60) -> AssetPlan:
    return AssetPlan(
        kind=kind,
        natural_length_cells=7,
        frames=frames if kind == "sheet" else 1,
        texels_per_cell=16 if kind == "sheet" else 64,
        fit=fit,
        prompt="A deterministic test texture",
    )


def _plan(asset: AssetPlan, *, axes: list[str] | None = None) -> ImplementationPlan:
    return ImplementationPlan(
        path="sprite_sheet" if asset.kind == "sheet" else "texture",
        rationale="Exercise the exact generated image contract.",
        fidelity_features=["clean silhouette"],
        layer_plan=["solid fallback", "image layer"],
        asset_plan=[asset],
        animation_plan=["loop sprite rows"] if asset.kind == "sheet" else [],
        required_wrap_axes=axes if axes is not None else AssetProcessor._asset_axes(asset),
        risks=[],
    )


def _document(*, frames: int = 60, fit: object | None = None, drift: object = "2") -> dict:
    digest = "a" * 64
    return {
        "schema_version": 2,
        "period_ms": 1_000,
        "textures": [
            {
                "name": "motion",
                "kind": "sheet",
                "ref": f"sha256:{digest}",
                "descriptor": {
                    "kind": "sheet",
                    "body_columns": 7,
                    "frame_rows": frames,
                    "variants": [
                        {
                            "content_ref": f"sha256:{digest}",
                            "url": f"/api/textures/variants/sha256:{digest}.png",
                            "width_px": 112,
                            "height_px": frames * 16,
                            "bytes": 1_024,
                            "texels_per_cell": 16,
                        }
                    ],
                },
            }
        ],
        "layers": [
            {
                "name": "fallback",
                "type": "paint",
                "region": "body",
                "source": {"type": "solid", "color": "#205080"},
            },
            {
                "name": "motion",
                "type": "paint",
                "region": "body",
                "source": {
                    "type": "image",
                    "texture": "motion",
                    "fit": fit if fit is not None else {"type": "tile"},
                    "drift_cells": drift,
                },
            },
        ],
    }


def _result(results, gate: str):
    return next(result for result in results if result.gate == gate)


def test_asset_plan_locks_current_forge_texel_density() -> None:
    with pytest.raises(ValidationError, match="sheet requires 16 texels_per_cell"):
        AssetPlan.model_validate(_asset().model_dump() | {"texels_per_cell": 64})


def test_asset_plan_is_capability_bounded_before_image_allocation() -> None:
    capabilities = json.loads((REPO_ROOT / "skin-schema/capabilities-v2.json").read_text(encoding="utf-8"))
    oversized = _plan(
        AssetPlan(
            kind="coat",
            natural_length_cells=33,
            frames=1,
            texels_per_cell=64,
            fit="clip",
            prompt="This request must be refused before generation.",
        )
    )

    with pytest.raises(WorkerContractError, match="2112x64px"):
        validate_plan_resource_limits(oversized, capabilities)


def test_asset_plan_obeys_pinned_row_limit_not_only_parser_ceiling() -> None:
    capabilities = json.loads((REPO_ROOT / "skin-schema/capabilities-v2.json").read_text(encoding="utf-8"))
    capabilities["limits"]["max_sprite_frame_rows"] = 32

    with pytest.raises(WorkerContractError, match="60 frame rows"):
        validate_plan_resource_limits(_plan(_asset(frames=60)), capabilities)


def test_normalize_keeps_body_columns_and_frame_rows_independent() -> None:
    normalized = AssetProcessor.normalize(_png(), _asset(frames=45))
    with Image.open(io.BytesIO(normalized)) as image:
        assert image.size == (7 * 16, 45 * 16)


@pytest.mark.parametrize(
    ("kind", "fit", "expected"),
    [
        ("sheet", "tile", ["y", "x"]),
        ("sheet", "clip", ["y"]),
        ("coat", "tile", ["x"]),
        ("coat", "clip", []),
        ("overlay", "clip", []),
    ],
)
def test_seam_axes_are_derived_from_usage(kind: str, fit: str, expected: list[str]) -> None:
    assert AssetProcessor._asset_axes(_asset(kind=kind, fit=fit)) == expected


def test_forge_packages_the_exact_ladder_and_verified_axes(factory_config: FactoryConfig) -> None:
    processor = AssetProcessor(factory_config)
    processor.forge_script = REPO_ROOT / "client/design/tools/forge.py"
    processor.config.paths.repo_root = REPO_ROOT

    bundle = processor.forge(_png(128, 64), _asset(kind="coat", fit="tile"))

    assert processor.accepted(bundle)
    assert bundle.manifest["seam_axes"] == ["x"]
    assert [variant.texels_per_cell for variant in bundle.variants] == [64, 32, 16]
    assert bundle.manifest["content_ref"] == bundle.variants[0].content_ref
    for variant in bundle.variants:
        assert variant.url == f"/api/textures/variants/{variant.content_ref}.png"


def test_image_contract_accepts_object_fit_and_numeric_constant(
    factory_config: FactoryConfig,
) -> None:
    runner = GateRunner(factory_config)
    results = runner._image_contract(_document(), _plan(_asset()))

    assert all(result.verdict == GateVerdict.PASS for result in results)
    assert _result(results, "asset_dimensions").measurements["required_wrap_axes"] == ["x", "y"]


@pytest.mark.parametrize(
    ("document", "gate", "reason"),
    [
        (
            {**_document(), "layers": _document()["layers"][1:]},
            "renderer_conformance",
            "no ordinary fallback",
        ),
        (_document(drift="time"), "asset_dimensions", "nonconstant drift"),
        (_document(frames=61), "asset_dimensions", "only 60 are reachable"),
    ],
)
def test_image_contract_rejects_unrenderable_generated_assets(
    factory_config: FactoryConfig,
    document: dict,
    gate: str,
    reason: str,
) -> None:
    runner = GateRunner(factory_config)
    plan = _plan(_asset(frames=int(document["textures"][0]["descriptor"]["frame_rows"])))
    result = _result(runner._image_contract(document, plan), gate)

    assert result.verdict == GateVerdict.FAIL
    assert any(reason in item for item in result.reasons)


def test_image_contract_rejects_plan_axes_that_do_not_match_document_usage(
    factory_config: FactoryConfig,
) -> None:
    runner = GateRunner(factory_config)
    result = _result(runner._image_contract(_document(), _plan(_asset(), axes=["y"])), "asset_dimensions")

    assert result.verdict == GateVerdict.FAIL
    assert "differ from usage" in result.reasons[0]
