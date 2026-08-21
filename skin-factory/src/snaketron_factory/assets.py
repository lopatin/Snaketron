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
from .gates import GateManifest
from .lama import lama_bundle_manifest, lama_python, lama_subprocess_environment


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


class AssetProcessor:
    def __init__(self, config: FactoryConfig) -> None:
        self.config = config
        self.gates = GateManifest(config.paths.gate_manifest)
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
                },
            )
            if not accepted:
                return ForgeBundle(
                    raw_manifest,
                    {},
                    (),
                    (seam,),
                    bool(raw_manifest.get("repaired")),
                    source,
                    tuple(raw_manifest.get("repair_methods", [])),
                )

            variants: list[ForgeVariant] = []
            hash_failures: list[str] = []
            for rung in raw_manifest.get("rungs", []):
                path = Path(rung["path"])
                data = path.read_bytes()
                digest = hashlib.sha256(data).hexdigest()
                content_ref = f"sha256:{digest}"
                try:
                    with Image.open(path) as image:
                        width, height = image.size
                        image.verify()
                except Exception as error:  # Pillow has multiple typed decode errors.
                    hash_failures.append(f"{path.name}: cannot decode exact PNG: {error}")
                    continue
                if digest != rung.get("sha256"):
                    hash_failures.append(f"{path.name}: manifest hash differs from bytes")
                if len(data) != rung.get("bytes"):
                    hash_failures.append(f"{path.name}: manifest byte count differs")
                if width != rung.get("width_px") or height != rung.get("height_px"):
                    hash_failures.append(f"{path.name}: manifest dimensions differ")
                variants.append(
                    ForgeVariant(
                        content_ref=content_ref,
                        url=f"/api/textures/variants/{content_ref}.png",
                        width_px=width,
                        height_px=height,
                        bytes=len(data),
                        texels_per_cell=int(rung["texels_per_cell"]),
                        data=data,
                    )
                )
            exact = self.gates.result(
                "asset_exact_hash",
                bool(variants) and not hash_failures,
                reasons=hash_failures or ([] if variants else ["forge emitted no ladder rungs"]),
                measurements={"variants": len(variants)},
            )
            descriptor = {
                "kind": asset.kind,
                "body_columns": asset.natural_length_cells,
                "frame_rows": asset.frames if asset.kind == "sheet" else None,
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
                gate_results=(seam, exact),
                repaired=bool(raw_manifest.get("repaired")),
                normalized_source=source,
                repair_methods=tuple(raw_manifest.get("repair_methods", [])),
            )

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
            mode = "RGBA" if asset.kind == "overlay" else "RGB"
            image = opened.convert(mode)
            width = image.width if asset.natural_length_cells is None else asset.natural_length_cells * canonical_texels
            if asset.kind == "sheet":
                assert asset.frames is not None
                height = asset.frames * canonical_texels
            elif asset.natural_length_cells is None:
                height = image.height
            else:
                height = canonical_texels
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
