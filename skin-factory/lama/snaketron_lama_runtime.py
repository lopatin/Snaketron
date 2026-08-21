"""Load only the preinstalled, hash-authorized Big-LaMa TorchScript model."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

import sitecustomize

if not getattr(sitecustomize, "OFFLINE_GUARD_INSTALLED", False):
    raise RuntimeError("the Snaketron LaMa socket guard is not installed")

_MANIFEST = json.loads(Path(__file__).with_name("manifest.json").read_text(encoding="utf-8"))
MODEL_SHA256 = _MANIFEST["model"]["sha256"]
MODEL_SIZE_BYTES = _MANIFEST["model"]["size_bytes"]


def verified_model_path() -> Path:
    raw = os.environ.get("LAMA_MODEL")
    if not raw:
        raise RuntimeError("LAMA_MODEL must name the preinstalled Big-LaMa model")
    path = Path(raw)
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise RuntimeError("LAMA_MODEL must be an absolute, regular, non-symlink file")
    if path.stat().st_size != MODEL_SIZE_BYTES:
        raise RuntimeError(f"Big-LaMa model size differs: expected {MODEL_SIZE_BYTES}, got {path.stat().st_size}")
    digest_builder = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest_builder.update(chunk)
    digest = digest_builder.hexdigest()
    if digest != MODEL_SHA256:
        raise RuntimeError(f"Big-LaMa model digest differs: expected {MODEL_SHA256}, got {digest}")
    return path


def load_lama():
    path = verified_model_path()
    # simple-lama downloads only when LAMA_MODEL is absent. Reasserting the
    # verified absolute path keeps that code path unreachable even if the
    # caller inherited a hostile LAMA_MODEL_URL.
    os.environ["LAMA_MODEL"] = str(path)
    os.environ.pop("LAMA_MODEL_URL", None)
    from simple_lama_inpainting import SimpleLama

    return SimpleLama()


def smoke_report(*, infer: bool = True) -> dict[str, Any]:
    path = verified_model_path()
    lama = load_lama()
    result: dict[str, Any] = {
        "model_sha256": MODEL_SHA256,
        "model_size_bytes": MODEL_SIZE_BYTES,
        "model_path": str(path),
        "loaded": True,
    }
    if infer:
        from PIL import Image

        image = Image.new("RGB", (32, 32), (31, 47, 63))
        mask = Image.new("L", (32, 32), 0)
        for x in range(12, 20):
            for y in range(12, 20):
                mask.putpixel((x, y), 255)
        output = lama(image, mask)
        if output.size != image.size:
            raise RuntimeError(f"LaMa smoke output shape differs: {output.size}")
        result["smoke_output_size"] = list(output.size)
    return result


if __name__ == "__main__":
    print(json.dumps(smoke_report(), sort_keys=True))
