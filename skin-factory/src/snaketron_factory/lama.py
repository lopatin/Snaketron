"""Pinned, preloaded, offline Big-LaMa runtime authority."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any

from .config import FactoryConfig
from .db import canonical_json


class LamaRuntimeError(RuntimeError):
    """The installed repair runtime differs from its checked-in authority."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _regular_private_file(path: Path, *, private: bool = False) -> None:
    if path.is_symlink() or not path.is_file():
        raise LamaRuntimeError(f"required LaMa file is missing or not regular: {path}")
    if private and path.stat().st_mode & 0o077:
        raise LamaRuntimeError(f"LaMa model must be owner-only (chmod 400 or 600): {path}")


def lama_bundle_manifest(config: FactoryConfig, *, verify_model: bool = True) -> dict[str, Any]:
    """Hash the dependency lock, offline loader, and exact model together."""

    manifest_path = config.paths.lama_manifest
    _regular_private_file(manifest_path)
    try:
        raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise LamaRuntimeError(f"invalid LaMa manifest: {error}") from error
    if (
        not isinstance(raw, dict)
        or set(raw) != {"schema_version", "dependency_lock", "model", "runtime_files"}
        or raw["schema_version"] != 1
    ):
        raise LamaRuntimeError("LaMa manifest schema differs from version 1")
    project = manifest_path.parent.resolve()

    def member(raw_name: object) -> Path:
        if not isinstance(raw_name, str) or not raw_name or Path(raw_name).is_absolute():
            raise LamaRuntimeError("LaMa manifest members must be nonempty relative paths")
        path = (project / raw_name).resolve()
        if project not in path.parents:
            raise LamaRuntimeError(f"LaMa manifest member escapes its project: {raw_name}")
        _regular_private_file(path)
        return path

    lock = member(raw["dependency_lock"])
    pyproject = project / "pyproject.toml"
    _regular_private_file(pyproject)
    runtime_names = raw["runtime_files"]
    if (
        not isinstance(runtime_names, list)
        or not runtime_names
        or not all(isinstance(name, str) for name in runtime_names)
        or len(set(runtime_names)) != len(runtime_names)
    ):
        raise LamaRuntimeError("LaMa runtime_files must be a unique nonempty list")
    runtime_files = {str(name): sha256_file(member(name)) for name in runtime_names}

    model = raw["model"]
    if not isinstance(model, dict) or set(model) != {"filename", "sha256", "size_bytes", "url"}:
        raise LamaRuntimeError("LaMa model manifest fields differ")
    filename = model["filename"]
    expected_sha = model["sha256"]
    expected_size = model["size_bytes"]
    url = model["url"]
    if (
        not isinstance(filename, str)
        or Path(filename).name != filename
        or not isinstance(expected_sha, str)
        or len(expected_sha) != 64
        or any(character not in "0123456789abcdef" for character in expected_sha)
        or not isinstance(expected_size, int)
        or expected_size <= 0
        or not isinstance(url, str)
        or not url.startswith("https://")
    ):
        raise LamaRuntimeError("LaMa model identity is malformed")
    if config.paths.lama_model.name != filename:
        raise LamaRuntimeError("configured LaMa model path does not match the pinned filename")
    if verify_model:
        _regular_private_file(config.paths.lama_model, private=True)
        actual_size = config.paths.lama_model.stat().st_size
        if actual_size != expected_size:
            raise LamaRuntimeError(f"LaMa model size differs: expected {expected_size}, got {actual_size}")
        actual_sha = sha256_file(config.paths.lama_model)
        if actual_sha != expected_sha:
            raise LamaRuntimeError(f"LaMa model digest differs: expected {expected_sha}, got {actual_sha}")

    return {
        "schema_version": 1,
        "manifest_sha256": sha256_file(manifest_path),
        "pyproject_sha256": sha256_file(pyproject),
        "dependency_lock_sha256": sha256_file(lock),
        "runtime_files": runtime_files,
        "model": {
            "filename": filename,
            "sha256": expected_sha,
            "size_bytes": expected_size,
            "url": url,
        },
    }


def lama_bundle_sha(manifest: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(manifest).encode("utf-8")).hexdigest()


def lama_python(config: FactoryConfig) -> Path:
    return config.paths.lama_python


def lama_subprocess_environment(config: FactoryConfig) -> dict[str, str]:
    """Return a minimal, secret-free and network-disabled helper environment."""

    runtime_dir = config.paths.lama_manifest.parent.resolve()
    cache = config.paths.data_dir / "lama" / "torch-cache"
    if cache.is_symlink():
        raise LamaRuntimeError(f"LaMa cache cannot be a symlink: {cache}")
    cache = cache.resolve()
    cache.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(cache, 0o700)
    python = lama_python(config).resolve()
    return {
        "PATH": os.pathsep.join((str(python.parent), "/usr/bin", "/bin")),
        "LANG": "C.UTF-8",
        "PYTHONNOUSERSITE": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPATH": str(runtime_dir),
        "LAMA_MODEL": str(config.paths.lama_model.resolve()),
        "SNAKETRON_LAMA_OFFLINE": "1",
        "HF_HUB_OFFLINE": "1",
        "TRANSFORMERS_OFFLINE": "1",
        "TORCH_HOME": str(cache),
    }
