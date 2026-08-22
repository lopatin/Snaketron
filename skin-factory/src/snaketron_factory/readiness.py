"""Behavior-bound proof that an explicit paid smoke completed."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .db import canonical_json
from .factory import Factory
from .snaketron_api import validate_service_capabilities


class ReadinessError(RuntimeError):
    """A paid-smoke marker is absent, unsafe, or stale."""


_SKILL_PIN_FIELDS = ("skill_sha256", "skill_git_ref", "skill_git_sha")


def current_readiness_pin(factory: Factory) -> dict[str, Any]:
    snapshot = factory.behavior_snapshot()
    digest = hashlib.sha256(canonical_json(snapshot).encode("utf-8")).hexdigest()
    # GEPA is allowed to promote only the bounded author-skin package through
    # the verified Git/journal flow.  Keep a second digest with those three
    # fields removed so the scheduled guard can distinguish that one designed
    # transition from config, direction, gates, capabilities, models,
    # renderer, runtime, or LaMa drift.
    static_snapshot = {
        key: value for key, value in snapshot.items() if key not in {"skill_sha", "skill_git_ref", "skill_git_sha"}
    }
    static_digest = hashlib.sha256(canonical_json(static_snapshot).encode("utf-8")).hexdigest()
    return {
        "version": 3,
        "behavior_sha256": digest,
        "static_behavior_sha256": static_digest,
        "config_sha256": snapshot["config_sha"],
        "skill_sha256": snapshot["skill_sha"],
        "skill_git_ref": snapshot["skill_git_ref"],
        "skill_git_sha": snapshot["skill_git_sha"],
        "model_config_sha256": snapshot["model_config_sha"],
        "renderer_sha256": snapshot["renderer_config_sha"],
        "lama_sha256": snapshot["lama_bundle_sha"],
    }


def marker_path(factory: Factory) -> Path:
    return factory.config.paths.data_dir / "hermes-paid-smoke.json"


def record_paid_smoke(factory: Factory, service_capabilities: dict[str, Any]) -> dict[str, Any]:
    """Record behavior and the stable service account proven by the paid run.

    The credential id is deliberately not retained: rotating a dedicated
    ``snk_factory_v1`` credential for the same account must not invalidate a
    successful paid smoke.  The account id is stable and prevents a different
    least-privilege account from inheriting that smoke's authority.
    """

    pin = current_readiness_pin(factory)
    service_user_id = _service_user_id(service_capabilities)
    value = {
        **pin,
        "factory_service_user_id": service_user_id,
        "kind": "explicit-paid-run-once",
        "recorded_at": datetime.now(UTC).isoformat(),
    }
    path = marker_path(factory)
    if path.is_symlink():
        raise ReadinessError(f"paid-smoke marker cannot be a symlink: {path}")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path.parent, 0o700)
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        prefix=".hermes-paid-smoke-",
        dir=path.parent,
        delete=False,
    ) as handle:
        temporary = Path(handle.name)
        json.dump(value, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return value


def check_paid_smoke(factory: Factory, service_capabilities: dict[str, Any]) -> dict[str, Any]:
    """Verify behavior and require the exact paid-smoke service account."""

    path = marker_path(factory)
    if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077:
        raise ReadinessError("explicit paid-smoke marker is missing or not owner-private")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise ReadinessError(f"explicit paid-smoke marker is invalid: {error}") from error
    expected_fields = {
        "version",
        "behavior_sha256",
        "static_behavior_sha256",
        "config_sha256",
        "skill_sha256",
        "skill_git_ref",
        "skill_git_sha",
        "model_config_sha256",
        "renderer_sha256",
        "lama_sha256",
        "factory_service_user_id",
        "kind",
        "recorded_at",
    }
    if not isinstance(value, dict) or set(value) != expected_fields:
        raise ReadinessError("explicit paid-smoke marker schema differs")
    if value.get("version") != 3:
        raise ReadinessError("explicit paid-smoke marker version differs")
    if value.get("kind") != "explicit-paid-run-once":
        raise ReadinessError("explicit paid-smoke marker kind differs")
    current = current_readiness_pin(factory)
    static_keys = sorted(set(current).difference({"behavior_sha256", *_SKILL_PIN_FIELDS}))
    stale = [key for key in static_keys if value.get(key) != current[key]]
    if stale:
        raise ReadinessError("explicit paid-smoke marker is stale for: " + ", ".join(stale))

    skill_changed = any(value.get(key) != current[key] for key in _SKILL_PIN_FIELDS)
    if skill_changed:
        if not _verified_automatic_skill_promotion(factory, current):
            raise ReadinessError("author-skin behavior changed without an exact verified automatic promotion")
    elif value.get("behavior_sha256") != current["behavior_sha256"]:
        # A future snapshot field must fail closed until it is deliberately
        # classified above; matching named fields are not enough.
        raise ReadinessError("explicit paid-smoke marker is stale for: behavior_sha256")
    current_user_id = _service_user_id(service_capabilities)
    if value.get("factory_service_user_id") != current_user_id:
        raise ReadinessError("factory service account differs from the account proven by the explicit paid smoke")
    return value


def _service_user_id(service_capabilities: dict[str, Any]) -> int:
    capabilities = validate_service_capabilities(service_capabilities)
    user_id = capabilities["identity"].get("userId")
    if isinstance(user_id, bool) or not isinstance(user_id, int) or user_id <= 0:
        raise PermissionError("factory service capability identity must have a positive integer userId")
    return user_id


def _verified_automatic_skill_promotion(factory: Factory, current: dict[str, Any]) -> bool:
    """Accept only the exact append-only promotion path the optimizer owns.

    `GitPromoter` moves `active_behavior` only after validating the package,
    creating a signed immutable tag, pushing it, checking the remote peeled
    SHA, and verifying a clean clone.  The operation journal then retains that
    exact result.  Requiring both records also covers the narrow crash window
    where the external promotion succeeded but `optimization_run.state` has
    not yet advanced to `promoted`.
    """

    active = factory.database.active_behavior("author-skin")
    if active is None:
        return False
    git_ref = str(active.get("git_ref") or "")
    git_sha = str(active.get("sha") or "")
    prefix = "refs/tags/skin-authoring/"
    if (
        not git_ref.startswith(prefix)
        or not git_ref.removeprefix(prefix)
        or current.get("skill_git_ref") != git_ref
        or current.get("skill_git_sha") != git_sha
    ):
        return False
    run_id = git_ref.removeprefix(prefix)
    try:
        run = factory.database.get_optimization_run(run_id)
    except Exception:
        return False
    if run.get("target") != "authoring_playbook" or run.get("state") not in {
        "evaluating_holdout",
        "promoted",
    }:
        return False
    if run.get("state") == "promoted" and (run.get("promoted_ref") != git_ref or run.get("promoted_sha") != git_sha):
        return False

    with factory.database.connect() as connection:
        operations = connection.execute(
            "SELECT idempotency_key,result_hash FROM operation "
            "WHERE side_effect=? AND provider_role=? AND status=? ORDER BY created_at DESC",
            (
                "promote_authoring_playbook",
                "git_promotion",
                "succeeded",
            ),
        ).fetchall()
    for operation in operations:
        if not str(operation["idempotency_key"]).startswith(f"gepa:{run_id}:promote:"):
            continue
        result_hash = operation["result_hash"]
        if not result_hash:
            continue
        try:
            result = factory.persistence.load_json(str(result_hash))
        except Exception:
            continue
        if isinstance(result, dict) and result.get("git_ref") == git_ref and result.get("sha") == git_sha:
            return True
    return False
