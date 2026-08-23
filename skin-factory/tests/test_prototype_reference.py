from __future__ import annotations

import importlib.util
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SCRIPT = REPO / "skin-factory" / "scripts" / "build-prototype-reference.py"


def _module():
    spec = importlib.util.spec_from_file_location("build_prototype_reference", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_checked_in_renderer_reference_is_byte_exact() -> None:
    _module().check(REPO)


def test_external_capture_tools_receive_no_factory_or_provider_secrets() -> None:
    module = _module()
    environment = module._subprocess_environment(
        {
            "HOME": "/tmp/runtime-home",
            "PATH": "/usr/bin:/bin",
            "TMPDIR": "/tmp",
            "GEMINI_API_KEY": "provider-secret",
            "SNAKETRON_FACTORY_SERVICE_TOKEN": "private-skin-secret",
            "SNAKETRON_FACTORY_OPERATOR_TOKEN": "review-secret",
            "AWS_SECRET_ACCESS_KEY": "cloud-secret",
            "GITHUB_TOKEN": "git-secret",
            "CUSTOM_WORKER_CREDENTIAL": "worker-secret",
        }
    )
    assert environment == {
        "HOME": "/tmp/runtime-home",
        "PATH": "/usr/bin:/bin",
        "TMPDIR": "/tmp",
    }
    assert not any("TOKEN" in name or "SECRET" in name or "GEMINI" in name for name in environment)
