from __future__ import annotations

import io
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from PIL import Image

from snaketron_factory.assets import AssetProcessor
from snaketron_factory.config import load_config
from snaketron_factory.doctor import FactoryDoctor
from snaketron_factory.domain import AssetPlan
from snaketron_factory.factory import Factory
from snaketron_factory.lama import (
    LamaRuntimeError,
    lama_bundle_manifest,
    lama_bundle_sha,
    lama_subprocess_environment,
)
from snaketron_factory.worker import SkillBundle


def test_loaded_config_preserves_virtualenv_python_entrypoint_symlink(tmp_path: Path) -> None:
    source = Path(__file__).resolve().parents[1]
    package = tmp_path / "skin-factory"
    config_path = package / "config/factory.yaml"
    config_path.parent.mkdir(parents=True)
    config_path.write_bytes((source / "config/factory.yaml").read_bytes())
    entrypoint = package / "var/lama-venv/bin/python"
    entrypoint.parent.mkdir(parents=True)
    entrypoint.symlink_to(sys.executable)

    config = load_config(config_path)

    assert config.paths.lama_python == entrypoint
    assert config.paths.lama_python != entrypoint.resolve()


def test_lama_bundle_hashes_lock_loader_and_private_model(factory_config) -> None:
    bundle = lama_bundle_manifest(factory_config)

    assert bundle["dependency_lock_sha256"]
    assert set(bundle["runtime_files"]) == {"sitecustomize.py", "snaketron_lama_runtime.py"}
    assert bundle["model"]["sha256"]
    assert len(lama_bundle_sha(bundle)) == 64


def test_production_lama_project_pins_direct_and_transitive_closure() -> None:
    project = Path(__file__).resolve().parents[1] / "lama"
    pyproject = (project / "pyproject.toml").read_text(encoding="utf-8")
    lock = (project / "uv.lock").read_text(encoding="utf-8")
    manifest = json.loads((project / "manifest.json").read_text(encoding="utf-8"))

    for requirement in (
        "simple-lama-inpainting==0.1.2",
        "numpy==1.26.4",
        "opencv-python==4.11.0.86",
        "pillow==9.5.0",
        "torch==2.2.2",
        "torchvision==0.17.2",
    ):
        assert requirement in pyproject
    assert 'requires-python = "==3.11.*"' in lock
    assert 'name = "simple-lama-inpainting"' in lock
    assert 'version = "0.1.2"' in lock
    assert 'hash = "sha256:' in lock
    assert manifest["model"] == {
        "filename": "big-lama-v0.1.0.pt",
        "sha256": "7ba7aa7ac37a4d41fdbbeba3a2af7ead18058552997e3a3cd1a3b2210c9e6b4c",
        "size_bytes": 205_803_670,
        "url": "https://github.com/enesmsahin/simple-lama-inpainting/releases/download/v0.1.0/big-lama.pt",
    }


@pytest.mark.parametrize("failure", ["missing", "wrong-size", "wrong-hash", "public", "symlink"])
def test_lama_bundle_rejects_missing_wrong_or_nonprivate_weights(factory_config, failure: str, tmp_path: Path) -> None:
    model = factory_config.paths.lama_model
    expected = model.read_bytes()
    if failure == "missing":
        model.unlink()
    elif failure == "wrong-size":
        model.chmod(0o600)
        model.write_bytes(expected + b"x")
    elif failure == "wrong-hash":
        model.chmod(0o600)
        model.write_bytes(b"x" * len(expected))
    elif failure == "public":
        model.chmod(0o644)
    else:
        replacement = tmp_path / "replacement.pt"
        replacement.write_bytes(expected)
        model.unlink()
        model.symlink_to(replacement)

    with pytest.raises(LamaRuntimeError):
        lama_bundle_manifest(factory_config)


def test_lama_subprocess_environment_is_minimal_exact_and_offline(factory_config, monkeypatch) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "must-not-reach-forge")
    monkeypatch.setenv("HTTPS_PROXY", "http://must-not-reach-forge")
    monkeypatch.setenv("LAMA_MODEL_URL", "https://must-not-reach-forge.test/model")

    environment = lama_subprocess_environment(factory_config)

    assert environment["LAMA_MODEL"] == str(factory_config.paths.lama_model.resolve())
    assert environment["SNAKETRON_LAMA_OFFLINE"] == "1"
    assert environment["HF_HUB_OFFLINE"] == "1"
    assert environment["TRANSFORMERS_OFFLINE"] == "1"
    assert "GEMINI_API_KEY" not in environment
    assert "HTTPS_PROXY" not in environment
    assert "LAMA_MODEL_URL" not in environment
    assert set(environment) == {
        "PATH",
        "LANG",
        "PYTHONNOUSERSITE",
        "PYTHONDONTWRITEBYTECODE",
        "PYTHONPATH",
        "LAMA_MODEL",
        "SNAKETRON_LAMA_OFFLINE",
        "HF_HUB_OFFLINE",
        "TRANSFORMERS_OFFLINE",
        "TORCH_HOME",
    }


def test_forge_process_receives_only_exact_offline_lama_environment(factory_config, monkeypatch) -> None:
    received: dict[str, str] = {}

    def run(_command, *, env, **_kwargs):
        received.update(env)
        return subprocess.CompletedProcess([], 1, "", "refused in fixture")

    monkeypatch.setattr("snaketron_factory.assets.subprocess.run", run)
    pixels = io.BytesIO()
    Image.new("RGB", (64, 64), (20, 40, 60)).save(pixels, format="PNG")
    AssetProcessor(factory_config).forge(
        pixels.getvalue(),
        AssetPlan(
            kind="coat",
            natural_length_cells=1,
            frames=1,
            texels_per_cell=64,
            fit="tile",
            prompt="test exact offline helper environment",
        ),
    )

    assert received == lama_subprocess_environment(factory_config)


def test_offline_sitecustomize_denies_socket_connect() -> None:
    project = Path(__file__).resolve().parents[1] / "lama"
    environment = {
        "PATH": os.pathsep.join((str(Path(sys.executable).parent), "/usr/bin", "/bin")),
        "PYTHONPATH": str(project),
        "PYTHONNOUSERSITE": "1",
        "SNAKETRON_LAMA_OFFLINE": "1",
    }
    completed = subprocess.run(
        [sys.executable, "-c", "import socket; socket.getaddrinfo('example.test', 443)"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
        env=environment,
    )

    assert completed.returncode != 0
    assert "network access is disabled" in completed.stderr


def test_runtime_requires_preloaded_model_before_importing_simple_lama(tmp_path: Path) -> None:
    source = Path(__file__).resolve().parents[1] / "lama"
    runtime = tmp_path / "lama"
    runtime.mkdir()
    for name in ("sitecustomize.py", "snaketron_lama_runtime.py"):
        (runtime / name).write_bytes((source / name).read_bytes())
    (runtime / "manifest.json").write_text(
        json.dumps(
            {
                "model": {
                    "sha256": "0" * 64,
                    "size_bytes": 1,
                }
            }
        ),
        encoding="utf-8",
    )
    # If upstream is imported, the sentinel makes the failure distinguishable
    # from the intended preloaded-model refusal.
    (runtime / "simple_lama_inpainting.py").write_text(
        "raise RuntimeError('upstream import/download branch was reached')\n",
        encoding="utf-8",
    )
    environment = {
        "PATH": os.pathsep.join((str(Path(sys.executable).parent), "/usr/bin", "/bin")),
        "PYTHONPATH": str(runtime),
        "PYTHONNOUSERSITE": "1",
        "SNAKETRON_LAMA_OFFLINE": "1",
    }

    completed = subprocess.run(
        [sys.executable, "-m", "snaketron_lama_runtime"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
        env=environment,
    )

    assert completed.returncode != 0
    assert "LAMA_MODEL must name the preinstalled" in completed.stderr
    assert "upstream import/download branch" not in completed.stderr


def test_behavior_drift_detects_lama_lock_change(factory_config) -> None:
    class Renderer:
        @staticmethod
        def renderer_sha() -> None:
            return None

    factory = Factory(factory_config, renderer=Renderer())  # type: ignore[arg-type]
    bundle = SkillBundle.load(factory_config.paths.skill_dir)
    factory.active_skill_bundle = lambda: (bundle, "HEAD", "a" * 40)  # type: ignore[method-assign]
    attempt = factory._create_seed_attempt()
    lock = factory_config.paths.lama_manifest.parent / "uv.lock"
    lock.write_text(lock.read_text(encoding="utf-8") + "# drift\n", encoding="utf-8")

    assert factory._behavior_drift_reason(attempt, attempt["stage"]) == (
        "LaMa dependency lock, offline loader, or model changed during an in-flight Attempt"
    )


def test_doctor_checks_frozen_offline_environment_and_real_smoke(factory_config, monkeypatch) -> None:
    calls: list[tuple[list[str], dict[str, str]]] = []
    monkeypatch.setattr("snaketron_factory.doctor.shutil.which", lambda name: "/usr/bin/uv" if name == "uv" else None)

    def run(command, *, env, **_kwargs):
        calls.append((command, env))
        if command[0] == "/usr/bin/uv":
            return subprocess.CompletedProcess(command, 0, "environment is synchronized", "")
        report = {
            "loaded": True,
            "model_sha256": lama_bundle_manifest(factory_config)["model"]["sha256"],
            "smoke_output_size": [32, 32],
        }
        return subprocess.CompletedProcess(command, 0, json.dumps(report), "")

    monkeypatch.setattr("snaketron_factory.doctor.subprocess.run", run)
    check = FactoryDoctor(factory_config)._lama_check()

    assert check.ok is True
    assert "bundle sha256:" in check.detail
    assert calls[0][0][1:3] == ["sync", "--project"]
    assert {"--frozen", "--offline", "--check", "--no-python-downloads"}.issubset(calls[0][0])
    assert calls[0][1]["UV_OFFLINE"] == "1"
    assert calls[1][0][-2:] == ["-m", "snaketron_lama_runtime"]
    assert calls[1][1]["SNAKETRON_LAMA_OFFLINE"] == "1"
