"""Strict forge execution and exact-byte artifact packaging."""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from PIL import Image, ImageOps

from .config import FactoryConfig
from .domain import AssetPlan, GateResult, GateVerdict
from .gates import GateRunner
from .lama import lama_bundle_manifest, lama_python, lama_subprocess_environment
from .worker_validation import asset_row_texels


def _has_alpha(image: Image.Image) -> bool:
    return "A" in image.getbands() or "transparency" in image.info


@dataclass(frozen=True)
class ForgeVariant:
    content_ref: str
    url: str
    width_px: int
    height_px: int
    bytes: int
    texels_per_cell: int
    data: bytes


@dataclass(frozen=True)
class ForgeBundle:
    manifest: dict[str, Any]
    descriptor: dict[str, Any]
    variants: tuple[ForgeVariant, ...]
    gate_results: tuple[GateResult, ...]
    repaired: bool
    normalized_source: bytes = b""
    repair_methods: tuple[str, ...] = ()
    rejected_output: ForgeVariant | None = None


class AssetProcessor:
    def __init__(self, config: FactoryConfig) -> None:
        self.config = config
        self.runtime_gates = GateRunner(config)
        self.gates = self.runtime_gates.manifest
        self.forge_script = config.paths.repo_root / "client/design/tools/forge.py"
        self.forge_python = str(lama_python(config))

    def forge(self, source: bytes, asset: AssetPlan) -> ForgeBundle:
        # Hash before every forge so a swapped model can never become shipping
        # pixels merely because doctor succeeded earlier in the deployment.
        lama_bundle_manifest(self.config)
        source = self.normalize(source, asset)
        with tempfile.TemporaryDirectory(prefix="skin-factory-forge-") as directory:
            root = Path(directory)
            source_path = root / "source.png"
            output = root / "out"
            source_path.write_bytes(source)
            command = [
                self.forge_python,
                str(self.forge_script),
                "--kind",
                asset.kind,
                "--axes",
                ",".join(self._asset_axes(asset)) or "none",
                "--in",
                str(source_path),
                "--out-dir",
                str(output),
            ]
            if asset.natural_length_cells is not None:
                command[4:4] = ["--body-columns", str(asset.natural_length_cells)]
            if asset.kind == "sheet" and asset.frames is not None:
                command[4:4] = ["--frame-rows", str(asset.frames)]
            command.extend(["--raster-overhang-px", str(asset.raster_overhang_px)])
            completed = subprocess.run(
                command,
                cwd=self.config.paths.repo_root,
                capture_output=True,
                text=True,
                timeout=900,
                check=False,
                env=lama_subprocess_environment(self.config),
            )
            manifest_path = output / "manifest.json"
            if not manifest_path.is_file():
                result = self.gates.result(
                    "seam",
                    False,
                    reasons=[
                        "forge did not emit manifest.json",
                        completed.stderr[-2_000:],
                    ],
                    measurements={"exit": completed.returncode},
                )
                return ForgeBundle({}, {}, (), (result,), False, source)
            raw_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            accepted = completed.returncode == 0 and raw_manifest.get("accepted") is True
            variants: list[ForgeVariant] = []
            hash_failures: list[str] = []

            def exact_png(record: dict[str, Any], *, label: str) -> ForgeVariant | None:
                try:
                    path = Path(record["path"]).resolve(strict=True)
                    path.relative_to(output.resolve())
                except (KeyError, OSError, ValueError) as error:
                    hash_failures.append(f"{label}: invalid forge output path: {error}")
                    return None
                data = path.read_bytes()
                digest = hashlib.sha256(data).hexdigest()
                content_ref = f"sha256:{digest}"
                try:
                    with Image.open(path) as image:
                        width, height = image.size
                        image.verify()
                except Exception as error:  # Pillow has multiple typed decode errors.
                    hash_failures.append(f"{label}: cannot decode exact PNG: {error}")
                    return None
                if digest != record.get("sha256"):
                    hash_failures.append(f"{label}: manifest hash differs from bytes")
                if len(data) != record.get("bytes"):
                    hash_failures.append(f"{label}: manifest byte count differs")
                if width != record.get("width_px") or height != record.get("height_px"):
                    hash_failures.append(f"{label}: manifest dimensions differ")
                try:
                    texels_per_cell = int(record["texels_per_cell"])
                except (KeyError, TypeError, ValueError):
                    hash_failures.append(f"{label}: missing texels_per_cell")
                    return None
                return ForgeVariant(
                    content_ref=content_ref,
                    url=f"/api/textures/variants/{content_ref}.png",
                    width_px=width,
                    height_px=height,
                    bytes=len(data),
                    texels_per_cell=texels_per_cell,
                    data=data,
                )

            for index, rung in enumerate(raw_manifest.get("rungs", [])):
                variant = exact_png(rung, label=f"rung {index}")
                if variant is not None:
                    variants.append(variant)
            rejected_output_record = raw_manifest.get("failed_output")
            rejected_output = (
                exact_png(rejected_output_record, label="rejected output")
                if isinstance(rejected_output_record, dict)
                else None
            )
            seam = self.gates.result(
                "seam",
                accepted,
                reasons=[] if accepted else [raw_manifest.get("rejection") or completed.stderr[-2_000:]],
                measurements={
                    "horizontal_ratio": raw_manifest.get("horizontal_ratio"),
                    "vertical_ratio": raw_manifest.get("vertical_ratio"),
                    "required_axes": self._asset_axes(asset),
                    "forge_exit": completed.returncode,
                    "repair_methods": raw_manifest.get("repair_methods", []),
                    "rejected_output_ref": rejected_output.content_ref if rejected_output else None,
                },
            )
            exact = self.gates.result(
                "asset_exact_hash",
                (bool(variants) if accepted else rejected_output is not None) and not hash_failures,
                reasons=hash_failures
                or ([] if variants or rejected_output else ["forge emitted no retained PNG evidence"]),
                measurements={
                    "variants": len(variants),
                    "rejected_output_ref": rejected_output.content_ref if rejected_output else None,
                },
            )
            descriptor = {
                "kind": asset.kind,
                "body_columns": asset.natural_length_cells,
                "frame_rows": asset.frames if asset.kind == "sheet" else None,
                "raster_overhang_px": asset.raster_overhang_px,
                "variants": [
                    {
                        "content_ref": item.content_ref,
                        "url": item.url,
                        "width_px": item.width_px,
                        "height_px": item.height_px,
                        "bytes": item.bytes,
                        "texels_per_cell": item.texels_per_cell,
                    }
                    for item in variants
                ],
            }
            if not accepted:
                retained_manifest = {
                    key: value for key, value in raw_manifest.items() if key not in {"rungs", "failed_output"}
                }
                retained_manifest["descriptor"] = descriptor
                retained_manifest["rungs"] = descriptor["variants"]
                if rejected_output is not None:
                    retained_manifest["failed_output"] = {
                        "content_ref": rejected_output.content_ref,
                        "width_px": rejected_output.width_px,
                        "height_px": rejected_output.height_px,
                        "bytes": rejected_output.bytes,
                        "texels_per_cell": rejected_output.texels_per_cell,
                    }
                return ForgeBundle(
                    retained_manifest,
                    descriptor,
                    tuple(variants),
                    (seam, exact),
                    bool(raw_manifest.get("repaired")),
                    source,
                    tuple(raw_manifest.get("repair_methods", [])),
                    rejected_output,
                )

            byte_gate_results = self.runtime_gates.validate_asset_bytes(asset, variants)
            canonical = variants[0].content_ref if variants else ""
            upload_manifest = {
                "schema_version": 1,
                "content_ref": canonical,
                "descriptor": descriptor,
                "seam_axes": raw_manifest.get("seam_axes", self._asset_axes(asset)),
                "shareable": False,
            }
            return ForgeBundle(
                manifest=upload_manifest,
                descriptor=descriptor,
                variants=tuple(variants),
                gate_results=(seam, exact, *byte_gate_results),
                repaired=bool(raw_manifest.get("repaired")),
                normalized_source=source,
                repair_methods=tuple(raw_manifest.get("repair_methods", [])),
                rejected_output=rejected_output,
            )

    def re_evaluate_exact(
        self,
        asset: AssetPlan,
        variants: list[ForgeVariant] | tuple[ForgeVariant, ...],
    ) -> tuple[GateResult, ...]:
        """Run current deterministic gates without changing retained pixels."""

        exact_failures: list[str] = []
        seam_failures: list[str] = []
        inspections: list[dict[str, Any]] = []
        for index, variant in enumerate(variants):
            actual_ref = f"sha256:{hashlib.sha256(variant.data).hexdigest()}"
            label = actual_ref
            if actual_ref != variant.content_ref:
                exact_failures.append(f"variant {index}: declared ref {variant.content_ref} differs from {actual_ref}")
            if len(variant.data) != variant.bytes:
                exact_failures.append(
                    f"{label}: declared {variant.bytes} bytes differs from exact {len(variant.data)} bytes"
                )
            try:
                with Image.open(io.BytesIO(variant.data)) as opened:
                    opened.load()
                    width, height = opened.size
                    image_format = opened.format
            except Exception as error:  # Pillow exposes several decoder-specific exceptions.
                exact_failures.append(f"{label}: cannot decode exact image: {error}")
                continue
            if image_format != "PNG":
                exact_failures.append(f"{label}: exact retained bytes are {image_format}, not PNG")
            if (width, height) != (variant.width_px, variant.height_px):
                exact_failures.append(
                    f"{label}: decoded size {width}x{height}px differs from declared "
                    f"{variant.width_px}x{variant.height_px}px"
                )

            with tempfile.TemporaryDirectory(prefix="skin-factory-inspect-") as directory:
                root = Path(directory)
                source_path = root / "retained"
                output = root / "out"
                source_path.write_bytes(variant.data)
                command = [
                    self.forge_python,
                    str(self.forge_script),
                    "--kind",
                    asset.kind,
                    "--axes",
                    ",".join(self._asset_axes(asset)) or "none",
                    "--in",
                    str(source_path),
                    "--out-dir",
                    str(output),
                    "--inspect-existing",
                    "--texels-per-cell",
                    str(variant.texels_per_cell),
                ]
                command.extend(["--body-columns", str(asset.natural_length_cells)])
                if asset.kind == "sheet":
                    command.extend(["--frame-rows", str(asset.frames)])
                command.extend(["--raster-overhang-px", str(asset.raster_overhang_px)])
                completed = subprocess.run(
                    command,
                    cwd=self.config.paths.repo_root,
                    capture_output=True,
                    text=True,
                    timeout=120,
                    check=False,
                    env=lama_subprocess_environment(self.config),
                )
                manifest_path = output / "manifest.json"
                if not manifest_path.is_file():
                    seam_failures.append(
                        f"{label}: exact seam inspector emitted no manifest "
                        f"(exit {completed.returncode}): {completed.stderr[-1_000:]}"
                    )
                    continue
                inspection = json.loads(manifest_path.read_text(encoding="utf-8"))
                inspection["content_ref"] = actual_ref
                inspection["served_url"] = variant.url
                inspections.append(inspection)
                seam_failures.extend(f"{label}: {reason}" for reason in inspection.get("reasons", []))

        exact = self.gates.result(
            "asset_exact_hash",
            bool(variants) and not exact_failures,
            reasons=exact_failures or ([] if variants else ["no exact retained asset bytes were selected"]),
            measurements={
                "byte_scope": "selected-content-addressed-retained-bytes",
                "content_refs": [item.content_ref for item in variants],
            },
        )
        seam = self.gates.result(
            "seam",
            bool(inspections) and not seam_failures,
            reasons=seam_failures or ([] if inspections else ["no exact retained asset bytes were inspectable"]),
            measurements={
                "byte_scope": "selected-content-addressed-retained-bytes",
                "variants": inspections,
                "repair_attempted": False,
            },
        )
        byte_results = tuple(
            result.model_copy(
                update={
                    "measurements": {
                        **result.measurements,
                        "byte_scope": "selected-content-addressed-retained-bytes",
                        "re_evaluation": True,
                    }
                }
            )
            for result in self.runtime_gates.validate_asset_bytes(asset, variants)
        )
        return (seam, exact, *byte_results)

    @staticmethod
    def normalize(source: bytes, asset: AssetPlan) -> bytes:
        """Normalize model output to the authored X-by-Y texel grid.

        Gemini controls composition but provider image sizes are a small fixed
        set. The factory owns the exact grid: it crops to the requested aspect
        and resamples once before any seam measurement or repair. That input is
        retained separately from the provider bytes.
        """
        canonical_texels = {"coat": 64, "overlay": 64, "sheet": 16}[asset.kind]
        with Image.open(io.BytesIO(source)) as opened:
            mode = "RGBA" if asset.kind == "overlay" or asset.raster_overhang_px > 0 or _has_alpha(opened) else "RGB"
            image = opened.convert(mode)
            width = image.width if asset.natural_length_cells is None else asset.natural_length_cells * canonical_texels
            if asset.kind == "sheet":
                assert asset.frames is not None
                height = asset.frames * asset_row_texels(asset)
            elif asset.natural_length_cells is None:
                height = image.height
            else:
                height = asset_row_texels(asset)
            normalized = ImageOps.fit(
                image,
                (width, height),
                method=Image.Resampling.LANCZOS,
                centering=(0.5, 0.5),
            )
            output = io.BytesIO()
            normalized.save(output, format="PNG", optimize=True)
            return output.getvalue()

    @staticmethod
    def normalize_sheet_slice(
        source: bytes,
        *,
        body_columns: int,
        frame_rows: int,
        texels_per_cell: int = 16,
        raster_overhang_px: int = 0,
    ) -> bytes:
        """Resize one provider-native time slice without discarding pixels.

        The caller selects a provider aspect ratio close to this slice's cell
        grid.  A direct resize introduces only the bounded aspect correction;
        unlike ``ImageOps.fit`` it cannot crop away generated frames.
        """

        if body_columns < 1 or frame_rows < 1 or texels_per_cell < 1:
            raise ValueError("sheet slice dimensions must be positive")
        if not 0 <= raster_overhang_px <= 4:
            raise ValueError("raster_overhang_px must be from 0 through 4")
        scaled_overhang = texels_per_cell * raster_overhang_px
        if scaled_overhang % 16:
            raise ValueError("raster overhang must scale exactly from the fixed 16-texel body grid")
        row_texels = texels_per_cell + 2 * (scaled_overhang // 16)
        target = (body_columns * texels_per_cell, frame_rows * row_texels)
        with Image.open(io.BytesIO(source)) as opened:
            mode = "RGBA" if raster_overhang_px > 0 or _has_alpha(opened) else "RGB"
            image = opened.convert(mode)
            normalized = image.resize(target, resample=Image.Resampling.LANCZOS)
            output = io.BytesIO()
            normalized.save(output, format="PNG", optimize=True)
            return output.getvalue()

    @staticmethod
    def assemble_sheet_slices(slices: list[bytes], asset: AssetPlan) -> bytes:
        """Vertically concatenate exact normalized slices into the full grid."""

        if asset.kind != "sheet":
            raise ValueError("only sheet assets can be assembled from time slices")
        expected_width = asset.natural_length_cells * asset.texels_per_cell
        expected_height = asset.frames * asset_row_texels(asset)
        decoded: list[Image.Image] = []
        total_height = 0
        use_alpha = asset.raster_overhang_px > 0
        try:
            for source in slices:
                opened = Image.open(io.BytesIO(source))
                source_has_alpha = _has_alpha(opened)
                use_alpha = use_alpha or source_has_alpha
                image = opened.convert("RGBA" if asset.raster_overhang_px > 0 or source_has_alpha else "RGB")
                opened.close()
                if image.width != expected_width:
                    image.close()
                    raise ValueError(f"sheet slice width {image.width}px differs from expected {expected_width}px")
                decoded.append(image)
                total_height += image.height
            if total_height != expected_height:
                raise ValueError(f"sheet slice height total {total_height}px differs from expected {expected_height}px")
            assembled = Image.new("RGBA" if use_alpha else "RGB", (expected_width, expected_height))
            offset = 0
            for image in decoded:
                assembled.paste(image, (0, offset))
                offset += image.height
            output = io.BytesIO()
            assembled.save(output, format="PNG", optimize=True)
            assembled.close()
            return output.getvalue()
        finally:
            for image in decoded:
                image.close()

    @staticmethod
    def _asset_axes(asset: AssetPlan) -> list[str]:
        axes: list[str] = []
        if asset.kind == "sheet":
            axes.append("y")
        if asset.fit == "tile":
            axes.append("x")
        return axes

    @staticmethod
    def accepted(bundle: ForgeBundle) -> bool:
        return bool(bundle.variants) and not any(
            result.blocking and result.verdict == GateVerdict.FAIL for result in bundle.gate_results
        )
