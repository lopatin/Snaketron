"""Provider-neutral operator CLI used by agents, humans, and Hermes cron."""

# Typer intentionally declares command metadata in function defaults.
# ruff: noqa: B008

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import typer

from .calibration import JudgeCalibrationService
from .config import FactoryConfig, load_config
from .db import Database, VersionConflict
from .doctor import FactoryDoctor
from .environment import apply_environment, load_service_environment, read_private_environment
from .factory import Factory
from .gallery import _prevalidate_bulk_retry, build_review_service, create_app
from .objects import ObjectStore

app = typer.Typer(
    name="factory",
    no_args_is_help=True,
    pretty_exceptions_enable=False,
    help="Run and review the retained Snaketron skin factory.",
)

CONFIG_OPTION = typer.Option(Path("config/factory.yaml"), "--config", exists=True, dir_okay=False)
ENV_OPTION = typer.Option(None, "--env-file", exists=True, dir_okay=False, help="JSON secret env file")
JSON_OPTION = typer.Option(False, "--json", help="Emit one machine-readable JSON document")


def load_environment(path: Path | None) -> None:
    """Load a non-executable JSON environment file without evaluating shell."""

    if path is None:
        return
    apply_environment(read_private_environment(path))


def _load(
    config_path: Path,
    env_file: Path | None,
    *,
    service_command: str | None = None,
) -> FactoryConfig:
    settings = load_config(config_path)
    if service_command is not None:
        _require_service_identity(settings, command=service_command)
        load_service_environment(settings, env_file)
    else:
        load_environment(env_file)
    return settings


def _human(config: FactoryConfig, actor: str | None) -> str:
    if not os.environ.get(config.review.operator_secret_env):
        raise PermissionError(f"{config.review.operator_secret_env} is required for human operator commands")
    selected = actor or os.environ.get("SKIN_FACTORY_REVIEW_ACTOR", "")
    if not selected.startswith("human:") or not selected.removeprefix("human:").strip():
        raise PermissionError("--actor must be a nonempty human: identity")
    return selected


def _require_service_identity(config: FactoryConfig, *, command: str = "run-once") -> None:
    """Fail closed if a scheduled cycle possesses any human-only authority."""

    present = sorted(name for name in config.human_authority_environment_names() if name in os.environ)
    if present:
        raise PermissionError(f"{command} cannot inherit human operator authority: " + ", ".join(present))


def _feedback(value: str, path: Path | None) -> str:
    if value and path:
        raise ValueError("use either --feedback or --feedback-file, not both")
    return path.read_text(encoding="utf-8") if path else value


def _jsonable(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def _emit(value: Any, machine: bool) -> None:
    typer.echo(json.dumps(_jsonable(value), sort_keys=machine, indent=None if machine else 2))


def _fail(error: Exception, machine: bool) -> None:
    payload = {"ok": False, "error": error.__class__.__name__, "detail": str(error)}
    typer.echo(json.dumps(payload) if machine else f"error: {payload['detail']}", err=True)
    raise typer.Exit(1)


async def _close(factory: Factory) -> None:
    await factory.close()


@app.command()
def doctor(
    config: Path = CONFIG_OPTION,
    env_file: Path | None = ENV_OPTION,
    identity: Literal["service", "operator", "all"] = typer.Option("service", "--identity"),
    offline: bool = typer.Option(False, "--offline", help="Skip network probes"),
    json_output: bool = JSON_OPTION,
) -> None:
    """Verify configuration, credentials, storage, models, renderer, and LaMa."""

    async def run() -> Any:
        settings = _load(
            config,
            env_file,
            service_command="service doctor" if identity == "service" else None,
        )
        checker = FactoryDoctor(settings)
        try:
            return await checker.run(identity=identity, offline=offline)
        finally:
            await checker.close()

    try:
        report = asyncio.run(run())
        _emit(report.model_dump(mode="json"), json_output)
        if not report.ok:
            raise typer.Exit(1)
    except typer.Exit:
        raise
    except Exception as error:
        _fail(error, json_output)


@app.command("run-once")
def run_once(
    config: Path = CONFIG_OPTION,
    env_file: Path | None = ENV_OPTION,
    json_output: bool = JSON_OPTION,
) -> None:
    """Advance the single leased, resumable production/optimization cycle."""

    async def run() -> Any:
        settings = _load(config, env_file, service_command="run-once")
        factory = Factory(settings)
        try:
            return await factory.run_once()
        finally:
            await factory.close()

    try:
        _emit(asyncio.run(run()), json_output)
    except Exception as error:
        _fail(error, json_output)


@app.command()
def status(
    config: Path = CONFIG_OPTION,
    env_file: Path | None = ENV_OPTION,
    json_output: bool = JSON_OPTION,
) -> None:
    """Show durable queue, spend, reconciliation, and calibration state."""

    try:
        settings = _load(config, env_file)
        database = Database(settings.paths.database)
        database.migrate()
        with database.connect() as connection:
            dispositions = {
                row[0]: row[1]
                for row in connection.execute("SELECT disposition,count(*) FROM attempt GROUP BY disposition")
            }
            stages = {row[0]: row[1] for row in connection.execute("SELECT stage,count(*) FROM attempt GROUP BY stage")}
            optimizations = [
                dict(row)
                for row in connection.execute(
                    "SELECT id,target,state,promoted_ref,created_at,updated_at "
                    "FROM optimization_run ORDER BY created_at DESC LIMIT 20"
                ).fetchall()
            ]
            techniques = [
                dict(row)
                for row in connection.execute(
                    "SELECT id,disposition,run_id,created_at,updated_at FROM technique_candidate "
                    "ORDER BY created_at DESC LIMIT 20"
                ).fetchall()
            ]
            outbox = {
                row[0]: row[1]
                for row in connection.execute("SELECT status,count(*) FROM outbox_message GROUP BY status")
            }
        prototype_calibration = database.judge_calibration("prototype")
        build_calibration = database.judge_calibration("build")
        calibration = JudgeCalibrationService(database, settings)
        result = {
            "ok": not database.integrity_check(),
            "mode": settings.mode,
            "database": str(database.path),
            "dispositions": dispositions,
            "stages": stages,
            "pending": {
                "prototype": database.count_attempts(disposition="needs_human", review_kind="prototype"),
                "final": database.count_attempts(disposition="needs_human", review_kind="final"),
            },
            "unresolved_operations": database.unresolved_operations(),
            "cost_micros": {"program": database.total_cost()},
            "active_authoring": database.active_behavior("author-skin"),
            "judge_calibration": {
                "prototype": prototype_calibration,
                "build": build_calibration,
            },
            "judge_routing": {
                "prototype": calibration.routing_status("prototype").as_report(),
                "build": calibration.routing_status("build").as_report(),
            },
            "outbox": {
                "by_status": outbox,
                "dead_letter": outbox.get("dead_letter", 0),
            },
            "optimizations": optimizations,
            "techniques": techniques,
        }
        _emit(result, json_output)
    except Exception as error:
        _fail(error, json_output)


@app.command()
def serve(
    config: Path = CONFIG_OPTION,
    env_file: Path | None = ENV_OPTION,
    host: str | None = typer.Option(None, "--host"),
    port: int | None = typer.Option(None, "--port", min=1, max=65535),
) -> None:
    """Serve the authenticated human review gallery."""

    try:
        settings = _load(config, env_file)
        if not os.environ.get(settings.review.operator_secret_env):
            raise PermissionError(f"{settings.review.operator_secret_env} is required")
        import uvicorn

        uvicorn.run(
            create_app(settings),
            host=host or settings.review.bind,
            port=port or settings.review.port,
            access_log=False,
        )
    except Exception as error:
        _fail(error, False)


async def _review_runtime(settings: FactoryConfig, action: Any) -> Any:
    factory = Factory(settings)
    factory.database.migrate()
    try:
        result = action(build_review_service(factory), factory)
        if hasattr(result, "__await__"):
            result = await result
        return result
    finally:
        await factory.close()


@app.command()
def label(
    attempt_id: str,
    artifact_id: str,
    kind: Literal["prototype_label", "build_quality_label"] = typer.Option(..., "--kind"),
    outcome: Literal["accept", "reject"] = typer.Option(..., "--outcome"),
    feedback: str = typer.Option("", "--feedback"),
    feedback_file: Path | None = typer.Option(None, "--feedback-file", exists=True),
    tag: list[str] = typer.Option([], "--tag"),
    actor: str | None = typer.Option(None, "--actor"),
    config: Path = CONFIG_OPTION,
    env_file: Path | None = ENV_OPTION,
    json_output: bool = JSON_OPTION,
) -> None:
    """Record a blind training label; this never approves or publishes."""

    try:
        settings = _load(config, env_file)
        human = _human(settings, actor)
        result = asyncio.run(
            _review_runtime(
                settings,
                lambda review, _: review.label(
                    attempt_id=attempt_id,
                    artifact_id=artifact_id,
                    kind=kind,
                    outcome=outcome,
                    feedback=_feedback(feedback, feedback_file),
                    tags=tag,
                    actor=human,
                ),
            )
        )
        _emit(result, json_output)
    except Exception as error:
        _fail(error, json_output)


@app.command("approve-prototype")
def approve_prototype(
    attempt_id: str,
    artifact_id: str,
    content_hash: str = typer.Option(..., "--content-hash"),
    feedback: str = typer.Option("", "--feedback"),
    feedback_file: Path | None = typer.Option(None, "--feedback-file", exists=True),
    actor: str | None = typer.Option(None, "--actor"),
    config: Path = CONFIG_OPTION,
    env_file: Path | None = ENV_OPTION,
    json_output: bool = JSON_OPTION,
) -> None:
    """Approve an exact retained prototype hash and authorize its build."""

    try:
        settings = _load(config, env_file)
        human = _human(settings, actor)
        result = asyncio.run(
            _review_runtime(
                settings,
                lambda review, _: review.approve_prototype(
                    attempt_id=attempt_id,
                    artifact_id=artifact_id,
                    content_hash=content_hash,
                    feedback=_feedback(feedback, feedback_file),
                    actor=human,
                ),
            )
        )
        _emit(result, json_output)
    except Exception as error:
        _fail(error, json_output)


@app.command()
def reject(
    attempt_id: str,
    artifact_id: str | None = typer.Option(None, "--artifact-id"),
    feedback: str = typer.Option("", "--feedback"),
    feedback_file: Path | None = typer.Option(None, "--feedback-file", exists=True),
    tag: list[str] = typer.Option([], "--tag"),
    actor: str | None = typer.Option(None, "--actor"),
    config: Path = CONFIG_OPTION,
    env_file: Path | None = ENV_OPTION,
    json_output: bool = JSON_OPTION,
) -> None:
    """Retain an attempt as human rejected with literal feedback."""

    try:
        settings = _load(config, env_file)
        human = _human(settings, actor)
        result = asyncio.run(
            _review_runtime(
                settings,
                lambda review, _: review.reject(
                    attempt_id=attempt_id,
                    artifact_id=artifact_id,
                    feedback=_feedback(feedback, feedback_file),
                    tags=tag,
                    actor=human,
                ),
            )
        )
        _emit(result, json_output)
    except Exception as error:
        _fail(error, json_output)


@app.command("re-evaluate")
def re_evaluate(
    artifact_id: str,
    attempt_id: str | None = typer.Option(None, "--attempt-id"),
    feedback: str = typer.Option("", "--feedback"),
    feedback_file: Path | None = typer.Option(None, "--feedback-file", exists=True),
    actor: str | None = typer.Option(None, "--actor"),
    idempotency_key: str | None = typer.Option(None, "--idempotency-key"),
    config: Path = CONFIG_OPTION,
    env_file: Path | None = ENV_OPTION,
    json_output: bool = JSON_OPTION,
) -> None:
    """Append current gates/judges against existing retained bytes."""

    try:
        settings = _load(config, env_file)
        human = _human(settings, actor)

        def action(review: Any, factory: Factory) -> Any:
            artifact = factory.database.get_artifact(artifact_id)
            selected_attempt = attempt_id or artifact["attempt_id"]
            if artifact["attempt_id"] != selected_attempt:
                raise VersionConflict("artifact does not belong to --attempt-id")
            return review.re_evaluate(
                attempt_id=selected_attempt,
                artifact_id=artifact_id,
                feedback=_feedback(feedback, feedback_file),
                actor=human,
                idempotency_key=idempotency_key,
            )

        _emit(asyncio.run(_review_runtime(settings, action)), json_output)
    except Exception as error:
        _fail(error, json_output)


@app.command()
def retry(
    attempt_id: str,
    from_stage: Literal["prototype", "assets", "build"] = typer.Option(..., "--from"),
    feedback: str = typer.Option("", "--feedback"),
    feedback_file: Path | None = typer.Option(None, "--feedback-file", exists=True),
    actor: str | None = typer.Option(None, "--actor"),
    idempotency_key: str | None = typer.Option(None, "--idempotency-key"),
    config: Path = CONFIG_OPTION,
    env_file: Path | None = ENV_OPTION,
    json_output: bool = JSON_OPTION,
) -> None:
    """Create a linked child using current behavior from a chosen stage."""

    try:
        settings = _load(config, env_file)
        human = _human(settings, actor)
        result = asyncio.run(
            _review_runtime(
                settings,
                lambda review, _: review.retry(
                    attempt_id=attempt_id,
                    from_stage=from_stage,
                    feedback=_feedback(feedback, feedback_file),
                    actor=human,
                    idempotency_key=idempotency_key,
                ),
            )
        )
        _emit(result, json_output)
    except Exception as error:
        _fail(error, json_output)


@app.command("bulk-retry")
def bulk_retry(
    attempt_ids: list[str] = typer.Argument(...),
    from_stage: Literal["prototype", "assets", "build"] = typer.Option(..., "--from"),
    feedback: str = typer.Option("", "--feedback"),
    feedback_file: Path | None = typer.Option(None, "--feedback-file", exists=True),
    actor: str | None = typer.Option(None, "--actor"),
    idempotency_key: str | None = typer.Option(None, "--idempotency-key"),
    config: Path = CONFIG_OPTION,
    env_file: Path | None = ENV_OPTION,
    json_output: bool = JSON_OPTION,
) -> None:
    """Create linked retries for a prevalidated set of rejected attempts."""

    try:
        settings = _load(config, env_file)
        human = _human(settings, actor)

        def action(review: Any, factory: Factory) -> Any:
            _prevalidate_bulk_retry(factory.database, attempt_ids, from_stage)
            literal = _feedback(feedback, feedback_file)
            return {
                "results": [
                    review.retry(
                        attempt_id=item,
                        from_stage=from_stage,
                        feedback=literal,
                        actor=human,
                        idempotency_key=idempotency_key,
                    )
                    for item in attempt_ids
                ]
            }

        _emit(asyncio.run(_review_runtime(settings, action)), json_output)
    except Exception as error:
        _fail(error, json_output)


@app.command()
def publish(
    attempt_id: str,
    revision: str = typer.Option(..., "--revision"),
    content_hash: str = typer.Option(..., "--content-hash"),
    feedback: str = typer.Option("", "--feedback"),
    feedback_file: Path | None = typer.Option(None, "--feedback-file", exists=True),
    actor: str | None = typer.Option(None, "--actor"),
    config: Path = CONFIG_OPTION,
    env_file: Path | None = ENV_OPTION,
    json_output: bool = JSON_OPTION,
) -> None:
    """Publish the exact reviewed revision/hash immediately."""

    try:
        settings = _load(config, env_file)
        human = _human(settings, actor)
        if not os.environ.get(settings.service.operator_token_env):
            raise PermissionError(f"{settings.service.operator_token_env} is required")
        result = asyncio.run(
            _review_runtime(
                settings,
                lambda review, _: review.publish(
                    attempt_id=attempt_id,
                    revision=revision,
                    content_hash=content_hash,
                    feedback=_feedback(feedback, feedback_file),
                    actor=human,
                ),
            )
        )
        _emit(result, json_output)
    except Exception as error:
        _fail(error, json_output)


@app.command("resolve-operation")
def resolve_operation(
    operation_id: str,
    resolution: Literal[
        "confirmed_not_executed",
        "executed_result_recovered",
        "executed_output_lost",
        "indeterminate",
    ]
    | None = typer.Option(None, "--resolution"),
    evidence_ref: str | None = typer.Option(None, "--evidence-ref"),
    result_hash: str | None = typer.Option(None, "--result-hash"),
    resolved_model: str | None = typer.Option(None, "--resolved-model"),
    provider_request_id: str | None = typer.Option(None, "--provider-request-id"),
    media_type: str | None = typer.Option(None, "--media-type"),
    resolution_file: Path | None = typer.Option(None, "--resolution-file", exists=True),
    actor: str | None = typer.Option(None, "--actor"),
    config: Path = CONFIG_OPTION,
    env_file: Path | None = ENV_OPTION,
    json_output: bool = JSON_OPTION,
) -> None:
    """Record one authenticated, evidenced outcome for an unknown operation."""

    try:
        settings = _load(config, env_file)
        human = _human(settings, actor)
        if resolution_file:
            if any((resolution, evidence_ref, result_hash, resolved_model, provider_request_id, media_type)):
                raise ValueError("--resolution-file cannot be combined with outcome options")
            payload = json.loads(resolution_file.read_text(encoding="utf-8"))
            resolution = payload.get("resolution")
            evidence_ref = payload.get("evidence_ref")
            result_hash = payload.get("result_hash")
            resolved_model = payload.get("resolved_model")
            provider_request_id = payload.get("provider_request_id")
            media_type = payload.get("media_type")
        if not resolution or not evidence_ref:
            raise ValueError("resolution and evidence reference are required")
        database = Database(settings.paths.database)
        database.migrate()
        if resolution == "executed_result_recovered":
            if not result_hash:
                raise ValueError("executed_result_recovered requires a result hash")
            if not resolved_model:
                raise ValueError("executed_result_recovered requires the exact resolved model")
            ObjectStore(settings.paths.objects).get(result_hash)
        result = database.resolve_operation(
            operation_id=operation_id,
            resolution=resolution,
            evidence_ref=evidence_ref,
            result_hash=result_hash,
            resolved_model=resolved_model,
            provider_request_id=provider_request_id,
            media_type=media_type,
            actor=human,
        )
        _emit(result, json_output)
    except Exception as error:
        _fail(error, json_output)


@app.command()
def optimize(
    if_ready: bool = typer.Option(False, "--if-ready"),
    target: str = typer.Option("authoring-playbook", "--target"),
    config: Path = CONFIG_OPTION,
    env_file: Path | None = ENV_OPTION,
    json_output: bool = JSON_OPTION,
) -> None:
    """Advance at most one eligible resumable optimizer/technique job."""

    async def run() -> Any:
        if not if_ready:
            raise ValueError("optimization is guarded; pass --if-ready")
        if target != "authoring-playbook":
            raise ValueError("the first bounded optimizer target is authoring-playbook")
        factory = Factory(_load(config, env_file))
        factory.database.migrate()
        try:
            from .optimizer import Optimizer

            return await Optimizer(factory).advance_if_ready()
        finally:
            await factory.close()

    try:
        _emit(asyncio.run(run()), json_output)
    except Exception as error:
        _fail(error, json_output)


@app.command()
def backup(
    target: Path | None = typer.Option(None, "--target", file_okay=False),
    config: Path = CONFIG_OPTION,
    env_file: Path | None = ENV_OPTION,
    json_output: bool = JSON_OPTION,
) -> None:
    """Create a consistent SQLite plus immutable-object backup directory."""

    try:
        settings = _load(config, env_file)
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        destination = (target or settings.paths.data_dir / "backups" / timestamp).resolve()
        if destination.exists():
            raise FileExistsError(f"backup target already exists: {destination}")
        objects_root = settings.paths.objects.resolve()
        if destination == objects_root or objects_root in destination.parents:
            raise ValueError("backup target cannot be inside the object store")
        destination.mkdir(parents=True, mode=0o700)
        database = Database(settings.paths.database)
        database.migrate()
        errors = database.integrity_check()
        if errors:
            raise RuntimeError(f"database integrity failed: {errors}")
        database_target = destination / "factory.sqlite3"
        database.backup(database_target)
        os.chmod(database_target, 0o600)
        objects_target = destination / "objects"
        if settings.paths.objects.exists():
            shutil.copytree(settings.paths.objects, objects_target, copy_function=shutil.copy2)
        else:
            objects_target.mkdir()
        object_files = [path for path in objects_target.rglob("*") if path.is_file()]
        manifest = {
            "version": 1,
            "created_at": datetime.now(UTC).isoformat(),
            "config_sha256": settings.version_sha256,
            "database_sha256": hashlib.sha256(database_target.read_bytes()).hexdigest(),
            "object_count": len(object_files),
            "object_bytes": sum(path.stat().st_size for path in object_files),
        }
        (destination / "manifest.json").write_text(
            json.dumps(manifest, sort_keys=True, indent=2) + "\n", encoding="utf-8"
        )
        _emit({"ok": True, "target": str(destination), "manifest": manifest}, json_output)
    except Exception as error:
        _fail(error, json_output)


if __name__ == "__main__":
    app()
