"""Blocking deterministic gates and diagnostic measurements."""

from __future__ import annotations

import hashlib
import io
import json
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import yaml
from PIL import Image

from .config import FactoryConfig
from .domain import AssetPlan, GateResult, GateVerdict, ImplementationPlan
from .worker_validation import WorkerContractError, effective_sheet_frame_rows

# This registry is intentionally code-owned.  The manifest controls versions
# and blocking policy, while this list makes it impossible to add a configured
# gate with no runtime producer and accidentally treat its absence as a pass.
RUNTIME_GATE_PRODUCERS = {
    "document_schema": "GateRunner.validate_document",
    "reference_integrity": "GateRunner.validate_document",
    "ownership": "Factory._ownership_gate",
    "safety_ip": "Factory._prototype_triage/Factory._build_triage",
    "asset_dimensions": "GateRunner.validate_document",
    "asset_exact_hash": "AssetProcessor.forge",
    "seam": "AssetProcessor.forge",
    "sprite_grid": "GateRunner.validate_asset_bytes",
    "temporal_loop": "GateRunner.validate_asset_bytes",
    "palette_chroma": "GateRunner.validate_asset_bytes",
    "detail_density": "GateRunner.validate_asset_bytes",
    "operation_budget": "GateRunner.validate_document",
    "renderer_conformance": "GateRunner.validate_document",
    "browser_pixels_ready": "BrowserRenderer.capture",
    "contrast_diagnostic": "GateRunner.validate_asset_bytes",
    "detail_retention_diagnostic": "GateRunner.validate_asset_bytes",
    "visual_fidelity": "SkinFactory",
}

ASSET_BYTE_GATE_NAMES = (
    "sprite_grid",
    "temporal_loop",
    "palette_chroma",
    "detail_density",
    "contrast_diagnostic",
    "detail_retention_diagnostic",
)

ASSET_RUNTIME_GATE_NAMES = ("seam", "asset_exact_hash", *ASSET_BYTE_GATE_NAMES)


@dataclass(frozen=True)
class _ExactVariant:
    content_ref: str
    declared_content_ref: str
    served_url: str
    width_px: int
    height_px: int
    texels_per_cell: int
    pixels: np.ndarray

    def identity(self) -> dict[str, Any]:
        return {
            "content_ref": self.content_ref,
            "declared_content_ref": self.declared_content_ref,
            "served_url": self.served_url,
            "width_px": self.width_px,
            "height_px": self.height_px,
            "texels_per_cell": self.texels_per_cell,
        }


@dataclass(frozen=True)
class GatePolicy:
    name: str
    version: str
    blocking: bool


class GateManifest:
    def __init__(self, path: Path, *, payload: bytes | None = None) -> None:
        self.path = path
        payload = path.read_bytes() if payload is None else payload
        self.sha256 = hashlib.sha256(payload).hexdigest()
        raw = yaml.safe_load(payload)
        self.version = int(raw["version"])
        self.policies = {
            entry["name"]: GatePolicy(
                name=entry["name"],
                version=str(entry["version"]),
                blocking=bool(entry["blocking"]),
            )
            for entry in raw["gates"]
        }

    def policy(self, name: str) -> GatePolicy:
        try:
            return self.policies[name]
        except KeyError as error:
            raise RuntimeError(f"gate {name!r} is not declared in {self.path}") from error

    def result(
        self,
        name: str,
        passed: bool,
        *,
        reasons: list[str] | None = None,
        measurements: dict[str, Any] | None = None,
    ) -> GateResult:
        policy = self.policy(name)
        return GateResult(
            gate=name,
            gate_version=policy.version,
            blocking=policy.blocking,
            verdict=GateVerdict.PASS if passed else GateVerdict.FAIL,
            reasons=reasons or [],
            measurements=measurements or {},
        )


class GateRunner:
    def __init__(
        self,
        config: FactoryConfig,
        *,
        capability_payload: bytes | None = None,
        gate_payload: bytes | None = None,
    ) -> None:
        self.config = config
        self.manifest = GateManifest(config.paths.gate_manifest, payload=gate_payload)
        payload = config.paths.capability_manifest.read_bytes() if capability_payload is None else capability_payload
        self.capability_sha256 = hashlib.sha256(payload).hexdigest()
        self.capabilities = json.loads(payload)

    def validate_document(self, document: dict[str, Any], plan: ImplementationPlan) -> list[GateResult]:
        results = [self._rust_schema(document)]
        client_compile = self._client_renderer_compile(document)
        for result in self._image_contract(document, plan):
            if result.gate != "renderer_conformance":
                results.append(result)
                continue
            reasons = [*client_compile.reasons, *result.reasons]
            results.append(
                self.manifest.result(
                    "renderer_conformance",
                    client_compile.verdict == GateVerdict.PASS and result.verdict == GateVerdict.PASS,
                    reasons=reasons,
                    measurements={
                        **result.measurements,
                        **client_compile.measurements,
                    },
                )
            )
        results.append(self._operation_budget(document))
        return results

    def validate_asset_bytes(self, asset: AssetPlan, variants: list[Any] | tuple[Any, ...]) -> list[GateResult]:
        """Measure every asset gate on the immutable PNG bytes that ship.

        The variant content references are hashes of these same bytes.  The
        upload path subsequently performs an exact read-after-write comparison,
        so these measurements apply to the bytes returned by the public texture
        endpoint rather than to a provider preview or an in-memory precursor.
        """
        exact, byte_errors = self._decode_exact_variants(variants)
        identities = [item.identity() for item in exact]
        common = {
            "asset_kind": asset.kind,
            "declared_body_columns": asset.natural_length_cells,
            "declared_frame_rows": asset.frames if asset.kind == "sheet" else None,
            "byte_scope": "content-addressed-shipping-png",
            "variants": identities,
        }

        grid_errors = list(byte_errors)
        grid_measurements: list[dict[str, Any]] = []
        for item in exact:
            expected_width = asset.natural_length_cells * item.texels_per_cell
            expected_height = (asset.frames if asset.kind == "sheet" else 1) * item.texels_per_cell
            grid_measurements.append(
                {
                    "content_ref": item.content_ref,
                    "body_columns": item.width_px / item.texels_per_cell,
                    "frame_rows": item.height_px / item.texels_per_cell,
                    "expected_width_px": expected_width,
                    "expected_height_px": expected_height,
                    "actual_width_px": item.width_px,
                    "actual_height_px": item.height_px,
                }
            )
            if item.width_px != expected_width or item.height_px != expected_height:
                grid_errors.append(
                    f"{item.content_ref}: exact grid is {item.width_px}x{item.height_px}px; "
                    f"declared grid requires {expected_width}x{expected_height}px"
                )
        if not exact:
            grid_errors.append("no exact shipping PNG variants were available for grid measurement")
        grid = self.manifest.result(
            "sprite_grid",
            not grid_errors,
            reasons=grid_errors,
            measurements={**common, "grid": grid_measurements},
        )

        temporal = self._temporal_loop_result(asset, exact, byte_errors, common, grid_errors)
        pixel_metrics = [self._pixel_metrics(item) for item in exact]
        palette = self._palette_chroma_result(pixel_metrics, byte_errors, common)
        detail = self._detail_density_result(pixel_metrics, byte_errors, common)
        contrast = self._contrast_result(pixel_metrics, byte_errors, common)
        scale = self._scale_result(pixel_metrics, byte_errors, common)
        return [grid, temporal, palette, detail, contrast, scale]

    @staticmethod
    def _decode_exact_variants(variants: list[Any] | tuple[Any, ...]) -> tuple[list[_ExactVariant], list[str]]:
        decoded: list[_ExactVariant] = []
        errors: list[str] = []
        for index, variant in enumerate(variants):
            label = f"variant {index}"
            try:
                data = bytes(variant.data)
                declared_ref = str(variant.content_ref)
                served_url = str(variant.url)
                declared_width = int(variant.width_px)
                declared_height = int(variant.height_px)
                texels_per_cell = int(variant.texels_per_cell)
            except (AttributeError, TypeError, ValueError) as error:
                errors.append(f"{label}: malformed exact variant metadata: {error}")
                continue
            actual_ref = f"sha256:{hashlib.sha256(data).hexdigest()}"
            label = actual_ref
            if actual_ref != declared_ref:
                errors.append(f"{label}: declared content ref is {declared_ref}")
            if texels_per_cell <= 0:
                errors.append(f"{label}: texels_per_cell must be positive")
                continue
            try:
                with Image.open(io.BytesIO(data)) as opened:
                    opened.load()
                    if opened.format != "PNG":
                        errors.append(f"{label}: exact shipping bytes are {opened.format}, not PNG")
                    image = opened.convert("RGBA")
            except Exception as error:  # Pillow exposes several decoder-specific exceptions.
                errors.append(f"{label}: cannot decode exact shipping PNG: {error}")
                continue
            if image.width != declared_width or image.height != declared_height:
                errors.append(
                    f"{label}: decoded size {image.width}x{image.height}px differs from "
                    f"declared {declared_width}x{declared_height}px"
                )
            decoded.append(
                _ExactVariant(
                    content_ref=actual_ref,
                    declared_content_ref=declared_ref,
                    served_url=served_url,
                    width_px=image.width,
                    height_px=image.height,
                    texels_per_cell=texels_per_cell,
                    pixels=np.asarray(image, dtype=np.uint8).copy(),
                )
            )
        return decoded, errors

    def _temporal_loop_result(
        self,
        asset: AssetPlan,
        exact: list[_ExactVariant],
        byte_errors: list[str],
        common: dict[str, Any],
        grid_errors: list[str],
    ) -> GateResult:
        if asset.kind != "sheet":
            return self.manifest.result(
                "temporal_loop",
                True,
                measurements={**common, "applicable": False, "loops": []},
            )

        reasons = list(byte_errors)
        if grid_errors:
            reasons.extend(item for item in grid_errors if item not in reasons)
        loops: list[dict[str, Any]] = []
        if not exact:
            reasons.append("no exact shipping PNG variants were available for temporal measurement")
        for item in exact:
            if item.height_px != asset.frames * item.texels_per_cell:
                continue
            luma = _premultiplied_luma(item.pixels)
            raw_frames = item.pixels.reshape(
                asset.frames,
                item.texels_per_cell,
                item.width_px,
                4,
            )
            frame_pixels = _premultiplied_channels(item.pixels).reshape(
                asset.frames,
                item.texels_per_cell,
                item.width_px,
                4,
            )
            frame_steps = [
                float(np.mean(np.abs(frame_pixels[(index + 1) % asset.frames] - frame_pixels[index])) / 255.0)
                for index in range(asset.frames)
            ]
            edge_steps = [
                float(
                    np.mean(np.abs(frame_pixels[index, -1, :, :] - frame_pixels[(index + 1) % asset.frames, 0, :, :]))
                    / 255.0
                )
                for index in range(asset.frames)
            ]
            internal_frame = frame_steps[:-1]
            internal_edge = edge_steps[:-1]
            change_floor = 1.0 / 1_024.0
            changed_steps = [step for step in frame_steps if step > change_floor]
            material_internal_frames = [step for step in internal_frame if step > change_floor]
            material_internal_edges = [step for step in internal_edge if step > change_floor]
            frame_baseline = float(np.median(material_internal_frames)) if material_internal_frames else 0.0
            edge_baseline = float(np.median(material_internal_edges)) if material_internal_edges else 0.0
            loop_frame = frame_steps[-1]
            loop_edge = edge_steps[-1]
            frame_allowance = max(0.02, frame_baseline * 2.5)
            edge_allowance = max(0.02, edge_baseline * 2.5)
            translation_px = _frame_translation(luma, asset.frames)
            translation_limit = max(1, item.texels_per_cell // 8)
            distinct_frames = len({hashlib.sha256(frame.tobytes()).digest() for frame in raw_frames})
            loops.append(
                {
                    "content_ref": item.content_ref,
                    "declared_frame_rows": asset.frames,
                    "measured_frame_rows": item.height_px // item.texels_per_cell,
                    "distinct_frame_cells": distinct_frames,
                    "changed_frame_transitions": len(changed_steps),
                    "minimum_change_mae": _rounded(change_floor),
                    "frame_step_mae": [_rounded(step) for step in frame_steps],
                    "loop_frame_mae": _rounded(loop_frame),
                    "median_internal_frame_mae": _rounded(frame_baseline),
                    "loop_frame_allowance": _rounded(frame_allowance),
                    "loop_edge_mae": _rounded(loop_edge),
                    "median_internal_edge_mae": _rounded(edge_baseline),
                    "loop_edge_allowance": _rounded(edge_allowance),
                    "median_frame_translation_px": translation_px,
                    "frame_translation_limit_px": translation_limit,
                }
            )
            if distinct_frames < 2 or len(changed_steps) < 2:
                reasons.append(
                    f"{item.content_ref}: declared frame cells contain no measurable frame-to-frame animation"
                )
            if loop_frame > frame_allowance:
                reasons.append(
                    f"{item.content_ref}: loop frame discontinuity {_rounded(loop_frame)} exceeds "
                    f"{_rounded(frame_allowance)}"
                )
            if loop_edge > edge_allowance:
                reasons.append(
                    f"{item.content_ref}: loop edge discontinuity {_rounded(loop_edge)} exceeds "
                    f"{_rounded(edge_allowance)}"
                )
            if abs(translation_px) > translation_limit:
                reasons.append(
                    f"{item.content_ref}: frames translate {translation_px}px; limit is {translation_limit}px"
                )
        return self.manifest.result(
            "temporal_loop",
            not reasons,
            reasons=reasons,
            measurements={**common, "applicable": True, "loops": loops},
        )

    @staticmethod
    def _pixel_metrics(item: _ExactVariant) -> dict[str, Any]:
        rgba = item.pixels.astype(np.float32) / 255.0
        alpha = rgba[..., 3]
        visible = alpha > (1.0 / 255.0)
        rgb = rgba[..., :3]
        visible_rgb = rgb[visible]
        if visible_rgb.size:
            visible_luma = visible_rgb @ np.array([0.2126, 0.7152, 0.0722], dtype=np.float32)
            chroma = np.max(visible_rgb, axis=1) - np.min(visible_rgb, axis=1)
            maximum = np.max(visible_rgb, axis=1)
            saturation = np.divide(chroma, maximum, out=np.zeros_like(chroma), where=maximum > 0)
            p05, p50, p95 = np.percentile(visible_luma, [5, 50, 95])
            quantized = np.floor(visible_rgb * 7.999).astype(np.uint16)
            packed = (quantized[:, 0] << 6) | (quantized[:, 1] << 3) | quantized[:, 2]
            palette_bins = int(np.unique(packed).size)
            mean_chroma = float(np.mean(chroma))
            p90_chroma = float(np.percentile(chroma, 90))
            mean_saturation = float(np.mean(saturation))
        else:
            p05 = p50 = p95 = 0.0
            palette_bins = 0
            mean_chroma = p90_chroma = mean_saturation = 0.0

        luma = _premultiplied_luma(item.pixels) / 255.0
        dx = np.abs(np.diff(luma, axis=1))
        dy = np.abs(np.diff(luma, axis=0))
        mean_gradient = float((np.mean(dx) + np.mean(dy)) / 2.0)
        detail_per_cell = mean_gradient * item.texels_per_cell
        dynamic_range = float(p95 - p05)
        contrast_ratio = float((p95 + 0.05) / (p05 + 0.05))
        return {
            "content_ref": item.content_ref,
            "texels_per_cell": item.texels_per_cell,
            "visible_fraction": _rounded(float(np.mean(visible))),
            "palette_bins_3bit": palette_bins,
            "mean_chroma": _rounded(mean_chroma),
            "p90_chroma": _rounded(p90_chroma),
            "mean_saturation": _rounded(mean_saturation),
            "luminance_p05": _rounded(float(p05)),
            "luminance_p50": _rounded(float(p50)),
            "luminance_p95": _rounded(float(p95)),
            "dynamic_range": _rounded(dynamic_range),
            "contrast_ratio": _rounded(contrast_ratio),
            "mean_gradient": _rounded(mean_gradient),
            "detail_per_cell": _rounded(detail_per_cell),
        }

    def _palette_chroma_result(
        self,
        metrics: list[dict[str, Any]],
        byte_errors: list[str],
        common: dict[str, Any],
    ) -> GateResult:
        threshold = 0.55
        reasons = list(byte_errors)
        enriched = _with_retention(metrics, "mean_chroma", "chroma_retention", floor=0.03)
        for item in enriched[1:]:
            if item["chroma_retention"] < threshold:
                reasons.append(
                    f"{item['content_ref']}: chroma retention {item['chroma_retention']} is below {threshold}"
                )
        if not enriched:
            reasons.append("no exact shipping PNG variants were available for chroma measurement")
        return self.manifest.result(
            "palette_chroma",
            not reasons,
            reasons=reasons,
            measurements={**common, "minimum_retention": threshold, "palette": enriched},
        )

    def _detail_density_result(
        self,
        metrics: list[dict[str, Any]],
        byte_errors: list[str],
        common: dict[str, Any],
    ) -> GateResult:
        threshold = 0.20
        reasons = list(byte_errors)
        enriched = _with_retention(metrics, "detail_per_cell", "detail_retention", floor=0.02)
        for item in enriched[1:]:
            if item["detail_retention"] < threshold:
                reasons.append(
                    f"{item['content_ref']}: per-cell detail retention {item['detail_retention']} is below {threshold}"
                )
        if not enriched:
            reasons.append("no exact shipping PNG variants were available for detail measurement")
        return self.manifest.result(
            "detail_density",
            not reasons,
            reasons=reasons,
            measurements={**common, "minimum_retention": threshold, "detail": enriched},
        )

    def _contrast_result(
        self,
        metrics: list[dict[str, Any]],
        byte_errors: list[str],
        common: dict[str, Any],
    ) -> GateResult:
        minimum_range = 0.08
        reasons = list(byte_errors)
        for item in metrics:
            if item["dynamic_range"] < minimum_range:
                reasons.append(
                    f"{item['content_ref']}: luminance range {item['dynamic_range']} is below {minimum_range}"
                )
        if not metrics:
            reasons.append("no exact shipping PNG variants were available for contrast measurement")
        return self.manifest.result(
            "contrast_diagnostic",
            not reasons,
            reasons=reasons,
            measurements={**common, "minimum_dynamic_range": minimum_range, "contrast": metrics},
        )

    def _scale_result(
        self,
        metrics: list[dict[str, Any]],
        byte_errors: list[str],
        common: dict[str, Any],
    ) -> GateResult:
        threshold = 0.50
        reasons = list(byte_errors)
        detail = _with_retention(metrics, "detail_per_cell", "detail_retention", floor=0.02)
        contrast = _with_retention(detail, "dynamic_range", "contrast_retention", floor=0.08)
        for item in contrast[1:]:
            item["scale_readability"] = _rounded(min(item["detail_retention"], item["contrast_retention"]))
            if item["scale_readability"] < threshold:
                reasons.append(
                    f"{item['content_ref']}: scale readability {item['scale_readability']} is below {threshold}"
                )
        if contrast:
            contrast[0]["scale_readability"] = 1.0
        else:
            reasons.append("no exact shipping PNG variants were available for scale measurement")
        return self.manifest.result(
            "detail_retention_diagnostic",
            not reasons,
            reasons=reasons,
            measurements={**common, "minimum_scale_readability": threshold, "scales": contrast},
        )

    def _rust_schema(self, document: dict[str, Any]) -> GateResult:
        with tempfile.TemporaryDirectory(prefix="skin-factory-schema-") as directory:
            path = Path(directory) / "skin.skin.json"
            path.write_text(json.dumps(document, sort_keys=True), encoding="utf-8")
            command = [
                "cargo",
                "run",
                "-q",
                "-p",
                "skin-schema",
                "--bin",
                "validate-skin",
                "--",
                str(path),
            ]
            completed = subprocess.run(
                command,
                cwd=self.config.paths.repo_root,
                text=True,
                capture_output=True,
                timeout=120,
                check=False,
            )
        output = "\n".join(part.strip() for part in (completed.stdout, completed.stderr) if part.strip())
        return self.manifest.result(
            "document_schema",
            completed.returncode == 0,
            reasons=[] if completed.returncode == 0 else [output[:4_000]],
            measurements={"validator_exit": completed.returncode, "schema_version": 2},
        )

    def _client_renderer_compile(self, document: dict[str, Any]) -> GateResult:
        """Run the exact side-effect-free compiler used by client registration."""

        compiler = "client::skin::registry runtime compiler"
        timeout_seconds = 120

        def unavailable(kind: str, reason: str) -> GateResult:
            return self.manifest.result(
                "renderer_conformance",
                False,
                reasons=[reason],
                measurements={
                    "client_compiler_exit": None,
                    "client_compiler": compiler,
                    "client_compiler_error": kind,
                    "pre_register": True,
                },
            )

        try:
            with tempfile.TemporaryDirectory(prefix="skin-factory-client-compile-") as directory:
                path = Path(directory) / "skin.skin.json"
                path.write_text(json.dumps(document, sort_keys=True), encoding="utf-8")
                command = [
                    "cargo",
                    "run",
                    "-q",
                    "-p",
                    "client",
                    "--bin",
                    "validate-renderer-skin",
                    "--",
                    str(path),
                ]
                completed = subprocess.run(
                    command,
                    cwd=self.config.paths.repo_root,
                    text=True,
                    capture_output=True,
                    timeout=timeout_seconds,
                    check=False,
                )
        except subprocess.TimeoutExpired:
            return unavailable(
                "timeout",
                f"client renderer compiler timed out after {timeout_seconds} seconds before registration",
            )
        except OSError as error:
            kind = "missing_executable" if isinstance(error, FileNotFoundError) else "os_error"
            detail = error.strerror or error.__class__.__name__
            return unavailable(
                kind,
                f"client renderer compiler could not run before registration ({kind}: {detail})",
            )
        output = "\n".join(part.strip() for part in (completed.stdout, completed.stderr) if part.strip())
        return self.manifest.result(
            "renderer_conformance",
            completed.returncode == 0,
            reasons=[] if completed.returncode == 0 else [output[:4_000]],
            measurements={
                "client_compiler_exit": completed.returncode,
                "client_compiler": compiler,
                "pre_register": True,
            },
        )

    @staticmethod
    def _flatten(layers: list[dict[str, Any]]) -> list[dict[str, Any]]:
        flattened: list[dict[str, Any]] = []
        for layer in layers:
            if layer.get("type") == "group":
                flattened.extend(GateRunner._flatten(layer.get("layers", [])))
            else:
                flattened.append(layer)
        return flattened

    def _image_contract(self, document: dict[str, Any], plan: ImplementationPlan) -> list[GateResult]:
        layers = self._flatten(document.get("layers", []))
        textures = {item.get("name"): item for item in document.get("textures", [])}
        failures: list[str] = []
        reference_failures: list[str] = []
        shape_failures: list[str] = []
        required_axes: set[str] = set()
        image_count = 0
        limits = self.capabilities["limits"]
        sheet_plans = [asset for asset in plan.asset_plan if asset.kind == "sheet"]
        used_sheet_plans: set[int] = set()
        processed_sheet_textures: set[str] = set()
        animation_sampling: list[dict[str, Any]] = []

        for index, layer in enumerate(layers):
            source = layer.get("source", {})
            if source.get("type") != "image":
                continue
            image_count += 1
            texture_name = source.get("texture")
            texture = textures.get(texture_name)
            if not texture:
                reference_failures.append(f"layer {layer.get('name', index)!r} names absent texture")
                continue
            kind = texture.get("kind")
            descriptor = texture.get("descriptor")
            if not isinstance(descriptor, dict):
                reference_failures.append(f"generated texture {texture_name!r} has no descriptor")
                continue
            if descriptor.get("kind") != kind:
                reference_failures.append(f"texture {texture_name!r} descriptor kind differs")
            variants = descriptor.get("variants", [])
            if not variants:
                reference_failures.append(f"texture {texture_name!r} has no immutable variants")
            for variant in variants:
                content_ref = variant.get("content_ref", "")
                expected = f"/api/textures/variants/{content_ref}.png"
                if variant.get("url") != expected:
                    reference_failures.append(f"texture {texture_name!r} URL is not addressed by variant hash")
                if not _content_ref(content_ref):
                    reference_failures.append(f"texture {texture_name!r} has malformed variant hash")
                width = int(variant.get("width_px", 0))
                height = int(variant.get("height_px", 0))
                size = int(variant.get("bytes", 0))
                if width <= 0 or height <= 0 or max(width, height) > limits["max_texture_dimension_px"]:
                    shape_failures.append(f"texture {texture_name!r} variant dimensions exceed limit")
                if size <= 0 or size > limits["max_texture_variant_bytes"]:
                    shape_failures.append(f"texture {texture_name!r} variant byte size exceeds limit")

            body_columns = descriptor.get("body_columns")
            frame_rows = descriptor.get("frame_rows")
            if kind in {"coat", "sheet"} and not isinstance(body_columns, int):
                shape_failures.append(f"{kind} {texture_name!r} needs body_columns")
            if kind == "sheet":
                required_axes.add("y")
                if not isinstance(frame_rows, int) or frame_rows < 2:
                    shape_failures.append(f"sheet {texture_name!r} needs at least two frame rows")
                elif frame_rows > limits["max_sprite_frame_rows"]:
                    shape_failures.append(f"sheet {texture_name!r} exceeds frame row limit")
                else:
                    period_ms = float(document.get("period_ms", 0))
                    reachable = min(
                        int(limits["max_sprite_frame_rows"]),
                        int(-(-period_ms * float(limits["max_sprite_frame_rate_fps"]) // 1000)),
                    )
                    if frame_rows > reachable:
                        shape_failures.append(
                            f"sheet {texture_name!r} declares {frame_rows} rows but only {reachable} "
                            "are reachable for this period"
                        )
                    if texture_name not in processed_sheet_textures:
                        candidates = [
                            (candidate_index, candidate)
                            for candidate_index, candidate in enumerate(sheet_plans)
                            if candidate_index not in used_sheet_plans
                            and candidate.natural_length_cells == body_columns
                        ]
                        if not candidates:
                            shape_failures.append(f"sheet {texture_name!r} has no matching implementation plan asset")
                            processed_sheet_textures.add(str(texture_name))
                        else:
                            candidate_index, asset = next(
                                (
                                    (candidate_index, candidate)
                                    for candidate_index, candidate in candidates
                                    if candidate.frames == frame_rows
                                ),
                                candidates[0],
                            )
                            used_sheet_plans.add(candidate_index)
                            processed_sheet_textures.add(str(texture_name))
                            try:
                                derived_rows = effective_sheet_frame_rows(
                                    asset,
                                    document.get("period_ms"),
                                    self.capabilities,
                                )
                            except WorkerContractError as error:
                                shape_failures.append(f"sheet {texture_name!r}: {error}")
                            else:
                                animation_sampling.append(
                                    {
                                        "texture": texture_name,
                                        "desired_fps": asset.desired_fps,
                                        "period_ms": document.get("period_ms"),
                                        "derived_frame_rows": derived_rows,
                                        "effective_fps": round(derived_rows * 1_000 / float(document["period_ms"]), 6),
                                        "renderer_max_fps": limits["max_sprite_frame_rate_fps"],
                                    }
                                )
                                if frame_rows != derived_rows or asset.frames != derived_rows:
                                    shape_failures.append(
                                        f"sheet {texture_name!r} declares {frame_rows} rows but desired_fps="
                                        f"{asset.desired_fps:g} and period_ms={float(document['period_ms']):g} "
                                        f"derive {derived_rows}"
                                    )
            elif frame_rows is not None:
                shape_failures.append(f"non-sheet texture {texture_name!r} declares frame_rows")

            fit = source.get("fit")
            fit_type = fit.get("type") if isinstance(fit, dict) else fit
            if fit_type == "tile":
                required_axes.add("x")
            drift = source.get("drift_cells", 0)
            if not _numeric_constant(drift):
                shape_failures.append(f"image layer {layer.get('name', index)!r} has nonconstant drift")

            # The closest ordinary layer below in the flattened painter order
            # must cover the same region. It remains visible while the atlas is
            # pending or unavailable.
            fallback = next(
                (
                    candidate
                    for candidate in reversed(layers[:index])
                    if candidate.get("source", {}).get("type") != "image"
                    and candidate.get("region") == layer.get("region")
                ),
                None,
            )
            if fallback is None:
                failures.append(f"image layer {layer.get('name', index)!r} has no ordinary fallback below it")

        expected_assets = plan.path in {"texture", "sprite_sheet", "hybrid"}
        if expected_assets and image_count == 0:
            failures.append(f"{plan.path} plan produced no image layers")
        if not expected_assets and image_count:
            failures.append("layers plan unexpectedly produced image layers")
        if set(plan.required_wrap_axes) != required_axes:
            shape_failures.append(
                f"plan wrap axes {sorted(plan.required_wrap_axes)} differ from usage {sorted(required_axes)}"
            )
        if len(used_sheet_plans) != len(sheet_plans):
            shape_failures.append("one or more planned sheet assets are absent from the resolved SkinDoc")

        return [
            self.manifest.result(
                "reference_integrity",
                not reference_failures,
                reasons=reference_failures,
                measurements={"textures": len(textures), "image_layers": image_count},
            ),
            self.manifest.result(
                "asset_dimensions",
                not shape_failures,
                reasons=shape_failures,
                measurements={
                    "required_wrap_axes": sorted(required_axes),
                    "animation_sampling": animation_sampling,
                },
            ),
            self.manifest.result(
                "renderer_conformance",
                not failures,
                reasons=failures,
                measurements={"procedural_fallbacks_required": image_count},
            ),
        ]

    def _operation_budget(self, document: dict[str, Any]) -> GateResult:
        flat = self._flatten(document.get("layers", []))
        maximum = int(self.capabilities["limits"]["max_flattened_layers"])
        passed = len(flat) <= maximum
        return self.manifest.result(
            "operation_budget",
            passed,
            reasons=[] if passed else [f"{len(flat)} flattened layers exceeds {maximum}"],
            measurements={"flattened_layers": len(flat), "maximum": maximum},
        )

    @staticmethod
    def blocking_failure(results: list[GateResult]) -> bool:
        return any(result.blocking and result.verdict == GateVerdict.FAIL for result in results)


def _content_ref(value: Any) -> bool:
    if not isinstance(value, str) or not value.startswith("sha256:") or len(value) != 71:
        return False
    return all(character in "0123456789abcdef" for character in value[7:])


def _numeric_constant(value: Any) -> bool:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return True
    if isinstance(value, str):
        try:
            float(value)
        except ValueError:
            return False
        return True
    return False


def _premultiplied_luma(rgba: np.ndarray) -> np.ndarray:
    channels = _premultiplied_channels(rgba)
    return channels[..., :3] @ np.array([0.2126, 0.7152, 0.0722], dtype=np.float32)


def _premultiplied_channels(rgba: np.ndarray) -> np.ndarray:
    pixels = rgba.astype(np.float32)
    alpha = pixels[..., 3:4] / 255.0
    return np.concatenate((pixels[..., :3] * alpha, pixels[..., 3:4]), axis=2)


def _frame_translation(luma: np.ndarray, rows: int) -> int:
    frame_height = luma.shape[0] // rows
    if frame_height < 4:
        return 0
    shifts: list[int] = []
    for row in range(rows):
        first = luma[row * frame_height : (row + 1) * frame_height]
        next_start = ((row + 1) % rows) * frame_height
        second = luma[next_start : next_start + frame_height]
        if second.shape[0] != frame_height:
            continue
        a = first.mean(axis=1) - first.mean()
        b = second.mean(axis=1) - second.mean()
        if float(np.max(np.abs(a))) <= 1e-5 or float(np.max(np.abs(b))) <= 1e-5:
            continue
        correlation = np.fft.irfft(np.fft.rfft(b) * np.conj(np.fft.rfft(a)), n=frame_height)
        lag = int(np.argmax(correlation))
        shifts.append(lag - frame_height if lag > frame_height // 2 else lag)
    if not shifts:
        return 0
    return int(np.median(shifts))


def _with_retention(
    metrics: list[dict[str, Any]],
    value_name: str,
    retention_name: str,
    *,
    floor: float,
) -> list[dict[str, Any]]:
    ordered = sorted((dict(item) for item in metrics), key=lambda item: int(item["texels_per_cell"]), reverse=True)
    if not ordered:
        return []
    baseline = float(ordered[0][value_name])
    for item in ordered:
        value = float(item[value_name])
        retention = 1.0 if baseline < floor else value / baseline
        item[retention_name] = _rounded(retention)
    return ordered


def _rounded(value: float) -> float:
    return round(float(value), 6)
