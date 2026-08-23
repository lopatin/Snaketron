"""Least-privilege environment boundaries for services and subprocesses."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from pathlib import Path

from .config import FactoryConfig


def read_private_environment(path: Path) -> dict[str, str]:
    """Read literal JSON from an owner-private file without shell evaluation."""

    mode = path.stat().st_mode & 0o777
    if mode & 0o077:
        raise PermissionError(f"environment file must be private (chmod 600): {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("environment file must be a JSON object")
    result: dict[str, str] = {}
    for name, value in payload.items():
        if not isinstance(name, str) or not name or not name.replace("_", "A").isalnum() or name[0].isdigit():
            raise ValueError(f"invalid environment variable name: {name!r}")
        if not isinstance(value, str):
            raise ValueError(f"environment value for {name} must be a string")
        result[name] = value
    return result


def apply_environment(payload: Mapping[str, str]) -> None:
    """Make an explicit JSON mapping authoritative over inherited values."""

    for name, value in payload.items():
        os.environ[name] = value


def load_service_environment(config: FactoryConfig, path: Path | None) -> None:
    """Install one explicit, config-bounded service identity.

    Human authority is checked before any scrubbing so a direct invocation
    from an operator shell fails closed. Provider and service values inherited
    from a daemon or login shell are then removed, and only the private JSON
    values are installed.
    """

    inherited_authority = sorted(name for name in config.human_authority_environment_names() if name in os.environ)
    if inherited_authority:
        raise PermissionError(
            "service command cannot inherit human operator authority: " + ", ".join(inherited_authority)
        )
    if path is None:
        raise PermissionError("service commands require an explicit private --env-file JSON")
    payload = read_private_environment(path)
    forbidden = sorted(config.human_authority_environment_names().intersection(payload))
    if forbidden:
        raise PermissionError("service environment must not contain operator credentials: " + ", ".join(forbidden))
    unknown = sorted(set(payload).difference(config.service_environment_names()))
    if unknown:
        raise PermissionError("service environment contains undeclared names: " + ", ".join(unknown))
    missing = sorted(name for name in config.required_service_environment_names() if not payload.get(name))
    if config.draft_automation.enabled:
        primary_name = config.draft_automation.fal_api_key_env
        fallback_name = config.draft_automation.fal_api_key_fallback_env
        primary = payload.get(primary_name, "")
        fallback = payload.get(fallback_name, "")
        if not primary and not fallback:
            missing.append(f"{primary_name}|{fallback_name}")
        if primary and fallback and primary != fallback:
            raise PermissionError("service environment contains conflicting Fal credential aliases")
    if missing:
        raise PermissionError("service environment is missing required credentials: " + ", ".join(missing))
    for name in config.credential_environment_names():
        os.environ.pop(name, None)
    apply_environment(payload)


def scrubbed_subprocess_environment(
    config: FactoryConfig,
    *,
    overrides: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Copy process state without any config-declared service/human secret."""

    environment = os.environ.copy()
    for name in config.credential_environment_names():
        environment.pop(name, None)
    if overrides:
        environment.update(overrides)
    return environment
