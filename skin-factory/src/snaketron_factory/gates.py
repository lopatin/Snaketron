"""Blocking deterministic gates and diagnostic measurements."""

from __future__ import annotations

import hashlib
import json
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .config import FactoryConfig
from .domain import GateResult, GateVerdict, ImplementationPlan


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
        results.extend(self._image_contract(document, plan))
        results.append(self._operation_budget(document))
        return results

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
                measurements={"required_wrap_axes": sorted(required_axes)},
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
