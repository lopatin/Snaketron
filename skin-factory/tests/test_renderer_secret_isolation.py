from __future__ import annotations

import os
from typing import Any

import pytest

from snaketron_factory.config import FactoryConfig
from snaketron_factory.renderer import (
    RENDERER_BUNDLE_MANIFEST_ENV,
    RENDERER_BUNDLE_SHA_ENV,
    RENDERER_RUNTIME_ENV_ALLOWLIST,
    RENDERER_SERVICE_TOKEN_ENV,
    BrowserRenderer,
    RendererDrift,
)


def _config_with_custom_secret_names(factory_config: FactoryConfig) -> FactoryConfig:
    raw = factory_config.model_dump()
    raw["service"].update(
        {
            "service_token_env": "CUSTOM_SNAKETRON_FETCH_SECRET",
            "operator_token_env": "CUSTOM_SNAKETRON_OPERATOR_SECRET",
        }
    )
    raw["review"]["operator_secret_env"] = "CUSTOM_REVIEW_OPERATOR_SECRET"
    raw["worker"]["api_key_env"] = "CUSTOM_WORKER_SECRET"
    raw["outbox"].update(
        {
            "webhook_url_env": "CUSTOM_WEBHOOK_URL_SECRET",
            "webhook_token_env": "CUSTOM_WEBHOOK_TOKEN_SECRET",
        }
    )
    for index, role in enumerate(raw["models"].values()):
        role["api_key_env"] = f"CUSTOM_PROVIDER_SECRET_{index}"
    config = FactoryConfig.model_validate(raw)
    config.source_path = factory_config.source_path
    config.version_sha256 = factory_config.version_sha256
    return config


def _capture_environment(renderer: BrowserRenderer) -> dict[str, str]:
    execution = renderer.execution_config()
    return renderer._capture_environment(
        execution["browser_bundle"],
        execution["browser_bundle_sha256"],
    )


def test_renderer_node_environment_is_an_allowlist_with_only_the_configured_fetch_token(
    factory_config: FactoryConfig,
    monkeypatch,
) -> None:
    config = _config_with_custom_secret_names(factory_config)
    configured_secrets = {name: f"secret-value-for-{name}" for name in config.credential_environment_names()}
    for name, value in configured_secrets.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("UNDECLARED_AMBIENT_SECRET", "ambient-secret-value")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "aws-secret-value")
    monkeypatch.setenv("HOME", "/tmp/renderer-home")
    monkeypatch.setenv("PATH", os.environ.get("PATH", "/usr/bin:/bin"))

    renderer = BrowserRenderer(config)
    monkeypatch.setattr(renderer, "renderer_sha", lambda: "renderer-test-sha")
    captured_environment: dict[str, str] = {}

    def run(*_args: Any, **kwargs: Any):
        captured_environment.update(kwargs["env"])
        return type(
            "Completed",
            (),
            {"returncode": 17, "stdout": "", "stderr": "injected capture stop"},
        )()

    monkeypatch.setattr("snaketron_factory.renderer.subprocess.run", run)
    evidence = renderer.capture("sha256:" + "a" * 64)
    environment = captured_environment

    child_only = {
        RENDERER_SERVICE_TOKEN_ENV,
        RENDERER_BUNDLE_MANIFEST_ENV,
        RENDERER_BUNDLE_SHA_ENV,
    }
    assert set(environment).issubset(RENDERER_RUNTIME_ENV_ALLOWLIST | child_only)
    assert environment[RENDERER_SERVICE_TOKEN_ENV] == configured_secrets[config.service.service_token_env]
    assert environment["HOME"] == "/tmp/renderer-home"
    assert environment[RENDERER_BUNDLE_MANIFEST_ENV]
    assert environment[RENDERER_BUNDLE_SHA_ENV]
    assert evidence.manifest["renderer_exit"] == 17

    # The configured source name is normalized to the one capture-only key;
    # every other service and human-authority capability is completely absent.
    assert not config.credential_environment_names().intersection(environment)
    assert "UNDECLARED_AMBIENT_SECRET" not in environment
    assert "AWS_SECRET_ACCESS_KEY" not in environment
    leaked_values = set(environment.values()).intersection(configured_secrets.values())
    assert leaked_values == {configured_secrets[config.service.service_token_env]}


def test_configured_secret_name_overrides_runtime_allowlist(
    factory_config: FactoryConfig,
    monkeypatch,
) -> None:
    raw: dict[str, Any] = factory_config.model_dump()
    raw["worker"]["api_key_env"] = "HOME"
    raw["service"]["service_token_env"] = "CUSTOM_FETCH_TOKEN"
    config = FactoryConfig.model_validate(raw)
    config.source_path = factory_config.source_path
    config.version_sha256 = factory_config.version_sha256
    monkeypatch.setenv("HOME", "worker-secret-in-runtime-name")
    monkeypatch.setenv("CUSTOM_FETCH_TOKEN", "private-fetch-token")

    environment = _capture_environment(BrowserRenderer(config))

    assert "HOME" not in environment
    assert "worker-secret-in-runtime-name" not in environment.values()
    assert environment[RENDERER_SERVICE_TOKEN_ENV] == "private-fetch-token"


def test_renderer_rejects_service_token_source_shared_with_provider_secret(
    factory_config: FactoryConfig,
    monkeypatch,
) -> None:
    raw: dict[str, Any] = factory_config.model_dump()
    raw["service"]["service_token_env"] = "SHARED_PROVIDER_AND_FETCH_SECRET"
    raw["models"]["smart_text"]["api_key_env"] = "SHARED_PROVIDER_AND_FETCH_SECRET"
    config = FactoryConfig.model_validate(raw)
    config.source_path = factory_config.source_path
    config.version_sha256 = factory_config.version_sha256
    monkeypatch.setenv("SHARED_PROVIDER_AND_FETCH_SECRET", "ambiguous-secret")

    with pytest.raises(RendererDrift, match="overlaps another credential"):
        _capture_environment(BrowserRenderer(config))
