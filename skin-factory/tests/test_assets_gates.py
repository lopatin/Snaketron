from __future__ import annotations

import hashlib
import io
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest
from PIL import Image
from pydantic import ValidationError

from snaketron_factory.assets import AssetProcessor, ForgeVariant
from snaketron_factory.config import FactoryConfig
from snaketron_factory.domain import AssetPlan, GateVerdict, ImplementationPlan
from snaketron_factory.gates import (
    ASSET_BYTE_GATE_NAMES,
    ASSET_RUNTIME_GATE_NAMES,
    RUNTIME_GATE_PRODUCERS,
    GateManifest,
    GateRunner,
)
from snaketron_factory.worker_validation import WorkerContractError, validate_plan_resource_limits

REPO_ROOT = Path(__file__).resolve().parents[2]


def _png(width: int = 96, height: int = 96) -> bytes:
    output = io.BytesIO()
    Image.new("RGB", (width, height), (40, 160, 220)).save(output, format="PNG")
    return output.getvalue()


def _variant(image: Image.Image, texels_per_cell: int) -> ForgeVariant:
    output = io.BytesIO()
    image.save(output, format="PNG", optimize=True)
    data = output.getvalue()
    content_ref = f"sha256:{hashlib.sha256(data).hexdigest()}"
    return ForgeVariant(
        content_ref=content_ref,
        url=f"/api/textures/variants/{content_ref}.png",
        width_px=image.width,
        height_px=image.height,
        bytes=len(data),
        texels_per_cell=texels_per_cell,
        data=data,
    )


def _moving_band_sheet(columns: int, rows: int, texels_per_cell: int) -> Image.Image:
    width = columns * texels_per_cell
    pixels = np.full((rows * texels_per_cell, width, 3), (12, 24, 36), dtype=np.uint8)
    band_width = max(2, width // rows)
    x = np.arange(width)
    for frame in range(rows):
        selected = ((x - frame * width // rows) % width) < band_width
        top = frame * texels_per_cell
        pixels[top : top + texels_per_cell, selected] = (245, 210, 32)
    return Image.fromarray(pixels, mode="RGB")


def _checkerboard(columns: int, texels_per_cell: int) -> Image.Image:
    height = texels_per_cell
    width = columns * texels_per_cell
    y, x = np.indices((height, width))
    selected = ((x // 4) + (y // 4)) % 2 == 0
    pixels = np.empty((height, width, 3), dtype=np.uint8)
    pixels[selected] = (255, 0, 200)
    pixels[~selected] = (0, 220, 255)
    return Image.fromarray(pixels, mode="RGB")


def _asset(*, kind: str = "sheet", fit: str = "tile", frames: int = 60) -> AssetPlan:
    return AssetPlan(
        kind=kind,
        natural_length_cells=7,
        frames=frames if kind == "sheet" else 1,
        desired_fps=float(frames) if kind == "sheet" else None,
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
        design_guidelines={
            "artistic_direction": "One clear deterministic test direction.",
            "concept_twist": "Original fixture with no popular basis.",
            "structure": "sprite" if asset.kind == "sheet" else "pattern",
            "body_strategy": "Reads at four cells, early six to seven cells, turns, growth, and headward overlap.",
            "head_zone": "light_field_dark_core",
            "asset_strategy": "Uses exact seamless axes and a tall row cadence when animated.",
        },
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


def _layers_plan() -> ImplementationPlan:
    return ImplementationPlan(
        path="layers",
        rationale="Exercise the procedural renderer contract.",
        fidelity_features=["clean silhouette"],
        layer_plan=["body", "animated band"],
        asset_plan=[],
        animation_plan=["move the band across the body"],
        required_wrap_axes=[],
        risks=["band may leave the body"],
        design_guidelines={
            "artistic_direction": "One clear deterministic test direction.",
            "concept_twist": "Original fixture with no popular basis.",
            "structure": "pattern",
            "body_strategy": "Reads at four cells, early growth, turns, and overlap.",
            "head_zone": "light_field_dark_core",
            "asset_strategy": "Procedural layers require no raster assets.",
        },
    )


def test_asset_plan_locks_current_forge_texel_density() -> None:
    with pytest.raises(ValidationError, match="sheet requires 16 texels_per_cell"):
        AssetPlan.model_validate(_asset().model_dump() | {"texels_per_cell": 64})


def test_renderer_conformance_runs_exact_client_compiler_before_registration(
    factory_config: FactoryConfig,
) -> None:
    factory_config.paths.repo_root = REPO_ROOT
    document = json.loads(
        (REPO_ROOT / "skills/author-skin/fixtures/layers/skin.skin.json").read_text(encoding="utf-8")
    )
    source = document["layers"][2]["source"]
    source["half_width"] = 0.15
    source["t_center"] = "tri(time)"

    result = _result(GateRunner(factory_config).validate_document(document, _layers_plan()), "renderer_conformance")

    assert result.verdict == GateVerdict.FAIL
    assert result.measurements == {
        "procedural_fallbacks_required": 0,
        "client_compiler_exit": 1,
        "client_compiler": "client::skin::registry runtime compiler",
        "pre_register": True,
    }
    assert any("|t_center| + half_width may not exceed 0.5" in reason for reason in result.reasons)


@pytest.mark.parametrize(
    ("error", "kind", "reason"),
    [
        (subprocess.TimeoutExpired(["cargo"], 120), "timeout", "timed out after 120 seconds"),
        (FileNotFoundError(2, "No such file or directory", "cargo"), "missing_executable", "could not run"),
    ],
)
def test_renderer_compiler_unavailability_fails_closed_before_registration(
    factory_config: FactoryConfig,
    monkeypatch: pytest.MonkeyPatch,
    error: BaseException,
    kind: str,
    reason: str,
) -> None:
    def fail_to_run(*_args, **_kwargs):
        raise error

    monkeypatch.setattr("snaketron_factory.gates.subprocess.run", fail_to_run)

    result = GateRunner(factory_config)._client_renderer_compile({"schema_version": 2})

    assert result.blocking is True
    assert result.verdict == GateVerdict.FAIL
    assert result.measurements == {
        "client_compiler_exit": None,
        "client_compiler": "client::skin::registry runtime compiler",
        "client_compiler_error": kind,
        "pre_register": True,
    }
    assert reason in result.reasons[0]


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
    assert {result.gate for result in bundle.gate_results} == set(ASSET_RUNTIME_GATE_NAMES)
    for variant in bundle.variants:
        assert variant.url == f"/api/textures/variants/{variant.content_ref}.png"


def test_asset_processor_returns_exact_failed_post_repair_png(
    factory_config: FactoryConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failed = _png(7 * 64, 64)

    def run(command, **_kwargs):
        output = Path(command[command.index("--out-dir") + 1])
        output.mkdir(parents=True)
        failed_path = output / "rejected.png"
        failed_path.write_bytes(failed)
        (output / "manifest.json").write_text(
            json.dumps(
                {
                    "kind": "coat",
                    "accepted": False,
                    "width_px": 7 * 64,
                    "height_px": 64,
                    "body_columns": 7,
                    "frame_rows": None,
                    "seam_axes": ["x"],
                    "horizontal_ratio": 2.0,
                    "vertical_ratio": 0.0,
                    "repaired": True,
                    "repair_methods": ["tx_t:x"],
                    "rungs": [],
                    "failed_output": {
                        "texels_per_cell": 64,
                        "width_px": 7 * 64,
                        "height_px": 64,
                        "bytes": len(failed),
                        "sha256": hashlib.sha256(failed).hexdigest(),
                        "path": str(failed_path),
                    },
                    "rejection": "still structurally wrong after repair",
                }
            ),
            encoding="utf-8",
        )
        return SimpleNamespace(returncode=1, stderr="")

    monkeypatch.setattr("snaketron_factory.assets.subprocess.run", run)
    bundle = AssetProcessor(factory_config).forge(_png(), _asset(kind="coat", fit="tile"))

    assert not AssetProcessor.accepted(bundle)
    assert bundle.repaired
    assert bundle.repair_methods == ("tx_t:x",)
    assert bundle.rejected_output is not None
    assert bundle.rejected_output.data == failed
    assert bundle.manifest["failed_output"]["content_ref"] == bundle.rejected_output.content_ref


def test_exact_asset_re_evaluation_uses_read_only_inspector_and_selected_hash(
    factory_config: FactoryConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected = _variant(_checkerboard(7, 64), 64)

    def run(command, **_kwargs):
        assert "--inspect-existing" in command
        assert command[command.index("--texels-per-cell") + 1] == "64"
        source = Path(command[command.index("--in") + 1])
        assert source.read_bytes() == selected.data
        output = Path(command[command.index("--out-dir") + 1])
        output.mkdir(parents=True)
        (output / "manifest.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "mode": "inspect_existing",
                    "accepted": True,
                    "reasons": [],
                    "measurements": [{"axis": "x", "cleared": True}],
                }
            ),
            encoding="utf-8",
        )
        return SimpleNamespace(returncode=0, stderr="")

    monkeypatch.setattr("snaketron_factory.assets.subprocess.run", run)
    processor = AssetProcessor(factory_config)
    results = processor.re_evaluate_exact(_asset(kind="coat", fit="tile"), (selected,))

    seam = _result(results, "seam")
    exact = _result(results, "asset_exact_hash")
    assert seam.verdict == GateVerdict.PASS
    assert seam.measurements["repair_attempted"] is False
    assert seam.measurements["variants"][0]["content_ref"] == selected.content_ref
    assert exact.verdict == GateVerdict.PASS
    assert exact.measurements["content_refs"] == [selected.content_ref]
    for result in results[2:]:
        assert result.measurements["byte_scope"] == "selected-content-addressed-retained-bytes"
        assert result.measurements["re_evaluation"] is True


def test_every_configured_runtime_gate_has_an_explicit_producer() -> None:
    manifest = GateManifest(REPO_ROOT / "skin-factory/config/gates.yaml")

    assert set(manifest.policies) == set(RUNTIME_GATE_PRODUCERS)
    assert set(ASSET_RUNTIME_GATE_NAMES).issubset(manifest.policies)
    assert all(manifest.policy(name).version.startswith("exact-pixels-") for name in ASSET_BYTE_GATE_NAMES)


def test_exact_asset_byte_gates_measure_every_shipping_rung(factory_config: FactoryConfig) -> None:
    runner = GateRunner(factory_config)
    asset = _asset(frames=4)
    variants = [
        _variant(_moving_band_sheet(7, 4, 16), 16),
        _variant(_moving_band_sheet(7, 4, 8), 8),
    ]

    results = runner.validate_asset_bytes(asset, variants)

    assert [result.gate for result in results] == list(ASSET_BYTE_GATE_NAMES)
    assert _result(results, "sprite_grid").verdict == GateVerdict.PASS
    assert _result(results, "temporal_loop").verdict == GateVerdict.PASS
    expected_refs = {variant.content_ref for variant in variants}
    for result in results:
        measured_refs = {item["content_ref"] for item in result.measurements["variants"]}
        assert measured_refs == expected_refs
        assert result.measurements["byte_scope"] == "content-addressed-shipping-png"
    loops = _result(results, "temporal_loop").measurements["loops"]
    assert all(item["measured_frame_rows"] == 4 for item in loops)
    assert all(item["distinct_frame_cells"] == 4 for item in loops)
    assert all(item["changed_frame_transitions"] == 4 for item in loops)


def test_temporal_gate_accepts_horizontal_motion_with_constant_row_means(
    factory_config: FactoryConfig,
) -> None:
    runner = GateRunner(factory_config)
    asset = _asset(frames=4)
    image = _moving_band_sheet(7, 4, 16)
    profile = np.asarray(image, dtype=np.float64).mean(axis=(1, 2))

    assert np.allclose(profile, profile[0])
    temporal = _result(runner.validate_asset_bytes(asset, [_variant(image, 16)]), "temporal_loop")
    assert temporal.verdict == GateVerdict.PASS
    assert temporal.measurements["loops"][0]["distinct_frame_cells"] == 4


def test_exact_temporal_gate_uses_grid_rows_and_rejects_static_frame_cells(
    factory_config: FactoryConfig,
) -> None:
    runner = GateRunner(factory_config)
    asset = _asset(frames=64)
    variant = _variant(Image.new("RGB", (7 * 16, 64 * 16), (20, 40, 60)), 16)

    results = runner.validate_asset_bytes(asset, [variant])

    assert _result(results, "sprite_grid").verdict == GateVerdict.PASS
    temporal = _result(results, "temporal_loop")
    assert temporal.verdict == GateVerdict.FAIL
    assert "no measurable frame-to-frame animation" in temporal.reasons[0]
    assert temporal.measurements["loops"][0]["declared_frame_rows"] == 64
    assert temporal.measurements["loops"][0]["measured_frame_rows"] == 64
    assert temporal.measurements["loops"][0]["distinct_frame_cells"] == 1
    assert temporal.measurements["loops"][0]["changed_frame_transitions"] == 0


def test_exact_temporal_gate_measures_full_colour_loop_discontinuity(factory_config: FactoryConfig) -> None:
    runner = GateRunner(factory_config)
    asset = _asset(frames=4)
    pixels = np.asarray(_moving_band_sheet(7, 4, 16)).copy()
    pixels[-16:, :, :] = 255
    variant = _variant(Image.fromarray(pixels, mode="RGB"), 16)

    temporal = _result(runner.validate_asset_bytes(asset, [variant]), "temporal_loop")

    assert temporal.verdict == GateVerdict.FAIL
    assert any("loop frame discontinuity" in reason for reason in temporal.reasons)
    loop = temporal.measurements["loops"][0]
    assert loop["loop_frame_mae"] > loop["loop_frame_allowance"]


def test_exact_grid_gate_rejects_a_dimensionally_malformed_rung(factory_config: FactoryConfig) -> None:
    runner = GateRunner(factory_config)
    asset = _asset(kind="coat", fit="clip")
    malformed = _variant(Image.new("RGB", (7 * 64 - 1, 64), (20, 40, 60)), 64)

    result = _result(runner.validate_asset_bytes(asset, [malformed]), "sprite_grid")

    assert result.verdict == GateVerdict.FAIL
    assert "declared grid requires 448x64px" in result.reasons[0]


def test_chroma_and_detail_loss_are_blocking_on_exact_ladder_bytes(factory_config: FactoryConfig) -> None:
    runner = GateRunner(factory_config)
    asset = _asset(kind="coat", fit="clip")
    variants = [
        _variant(_checkerboard(7, 64), 64),
        _variant(Image.new("RGB", (7 * 16, 16), (128, 128, 128)), 16),
    ]

    results = runner.validate_asset_bytes(asset, variants)
    palette = _result(results, "palette_chroma")
    detail = _result(results, "detail_density")

    assert palette.blocking and palette.verdict == GateVerdict.FAIL
    assert detail.blocking and detail.verdict == GateVerdict.FAIL
    assert palette.measurements["palette"][1]["chroma_retention"] == 0.0
    assert detail.measurements["detail"][1]["detail_retention"] == 0.0


def test_contrast_and_scale_diagnostics_name_exact_bytes_at_each_scale(
    factory_config: FactoryConfig,
) -> None:
    runner = GateRunner(factory_config)
    asset = _asset(kind="coat", fit="clip")
    variants = [
        _variant(_checkerboard(7, 64), 64),
        _variant(_checkerboard(7, 16), 16),
    ]

    results = runner.validate_asset_bytes(asset, variants)
    contrast = _result(results, "contrast_diagnostic")
    scale = _result(results, "detail_retention_diagnostic")

    expected_refs = [variant.content_ref for variant in variants]
    assert [item["content_ref"] for item in contrast.measurements["contrast"]] == expected_refs
    assert [item["content_ref"] for item in scale.measurements["scales"]] == expected_refs
    assert all("contrast_ratio" in item for item in contrast.measurements["contrast"])
    assert all("scale_readability" in item for item in scale.measurements["scales"])


def test_image_contract_accepts_object_fit_and_numeric_constant(
    factory_config: FactoryConfig,
) -> None:
    runner = GateRunner(factory_config)
    results = runner._image_contract(_document(), _plan(_asset()))

    assert all(result.verdict == GateVerdict.PASS for result in results)
    assert _result(results, "asset_dimensions").measurements["required_wrap_axes"] == ["x", "y"]


def test_sheet_rows_are_derived_from_desired_fps_and_period(
    factory_config: FactoryConfig,
) -> None:
    runner = GateRunner(factory_config)
    document = _document(frames=30)
    asset = _asset(frames=30)

    result = _result(runner._image_contract(document, _plan(asset)), "asset_dimensions")

    assert result.verdict == GateVerdict.PASS
    assert result.measurements["animation_sampling"] == [
        {
            "texture": "motion",
            "desired_fps": 30.0,
            "period_ms": 1_000,
            "derived_frame_rows": 30,
            "effective_fps": 30.0,
            "renderer_max_fps": 60,
        }
    ]


def test_reusing_one_sheet_in_multiple_layers_consumes_one_plan_asset(
    factory_config: FactoryConfig,
) -> None:
    runner = GateRunner(factory_config)
    document = _document(frames=30)
    document["layers"].append(
        {
            "name": "second motion use",
            "type": "paint",
            "region": "body",
            "source": {
                "type": "image",
                "texture": "motion",
                "fit": {"type": "tile"},
                "drift_cells": 0,
            },
        }
    )

    results = runner._image_contract(document, _plan(_asset(frames=30)))

    assert all(result.verdict == GateVerdict.PASS for result in results)
    assert len(_result(results, "asset_dimensions").measurements["animation_sampling"]) == 1


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
