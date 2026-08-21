from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from snaketron_factory.cli import app, load_environment
from snaketron_factory.config import FactoryConfig, load_config
from snaketron_factory.db import Database, canonical_json
from snaketron_factory.domain import (
    ArtifactKind,
    Disposition,
    GateResult,
    GateVerdict,
    OperationStatus,
    Purpose,
    Stage,
)
from snaketron_factory.environment import load_service_environment
from snaketron_factory.factory import Factory
from snaketron_factory.gallery import create_app
from snaketron_factory.objects import ObjectStore
from snaketron_factory.operations import OperationJournal

ROOT = Path(__file__).resolve().parents[1]


def make_config(tmp_path: Path) -> FactoryConfig:
    base = load_config(ROOT / "config/factory.yaml")
    paths = base.paths.model_copy(
        update={
            "data_dir": tmp_path / "var",
            "database": tmp_path / "var/factory.sqlite3",
            "objects": tmp_path / "var/objects",
        }
    )
    worker = base.worker.model_copy(update={"adapter": "fake"})
    return base.model_copy(update={"paths": paths, "worker": worker})


def seeded_factory(tmp_path: Path) -> tuple[Factory, dict, dict, bytes]:
    config = make_config(tmp_path)
    database = Database(config.paths.database)
    database.migrate()
    objects = ObjectStore(config.paths.objects)
    factory = Factory(config, database=database, objects=objects)
    behavior = factory.behavior_snapshot()
    concept = database.create_concept(
        name="Blind test",
        brief="A retained prototype used to prove blind human labeling.",
        seed="seed",
        source="test",
        tags=["fixture"],
    )
    attempt = database.create_attempt(
        concept_id=concept["id"],
        purpose=Purpose.PRODUCTION,
        stage=Stage.PROTOTYPE_REVIEW,
        idempotency_key=f"test:{concept['id']}",
        behavior=behavior,
        direction_sha="direction",
        skill_sha="skill",
        capability_sha="capability",
        gate_sha="gates",
        model_config_sha="models",
    )
    attempt = database.update_attempt(
        attempt["id"],
        attempt["version"],
        disposition=Disposition.NEEDS_HUMAN,
        review_kind="prototype",
    )
    image = b"not-a-decoded-image-but-exact-retained-bytes"
    stored = objects.put(image)
    artifact = database.add_artifact(
        attempt_id=attempt["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        content_hash=stored.uri,
        object_ref=stored.uri,
        media_type="image/png",
        size_bytes=len(image),
    )
    manifest = canonical_json(
        {
            "image_sha256": artifact["content_hash"],
            "design_guidelines_sha256": behavior["design_guidelines_sha"],
            "prototype_geometry_sha256": behavior["prototype_geometry_sha"],
            "prototype_guide_sha256": behavior["prototype_guide_sha"],
        }
    ).encode()
    manifest_stored = objects.put(manifest)
    database.add_artifact(
        attempt_id=attempt["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE_MANIFEST,
        content_hash=manifest_stored.uri,
        object_ref=manifest_stored.uri,
        media_type="application/json",
        size_bytes=len(manifest),
        metadata={"image_artifact_id": artifact["id"]},
    )
    database.add_evaluation(
        artifact_id=artifact["id"],
        attempt_id=attempt["id"],
        evaluator="visual_judge",
        result=GateResult(
            gate="visual_fidelity",
            gate_version="judge-v1",
            blocking=False,
            verdict=GateVerdict.MACHINE_REJECTED,
            reasons=["hidden reason"],
            measurements={"score": 0.2},
        ),
        hidden_until_label=True,
    )
    return factory, attempt, artifact, image


def headers() -> dict[str, str]:
    return {
        "authorization": "Bearer review-secret-123456789",
        "x-review-actor": "human:alice",
    }


def test_gallery_requires_auth_hides_judge_until_blind_label_and_streams_bytes(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("SKIN_FACTORY_REVIEW_TOKEN", "review-secret-123456789")
    factory, attempt, artifact, image = seeded_factory(tmp_path)
    second_bytes = b"second retained prototype"
    second_stored = factory.objects.put(second_bytes)
    second = factory.database.add_artifact(
        attempt_id=attempt["id"],
        stage=Stage.PROTOTYPE,
        kind=ArtifactKind.PROTOTYPE,
        content_hash=second_stored.uri,
        object_ref=second_stored.uri,
        media_type="image/png",
        size_bytes=len(second_bytes),
    )
    factory.database.add_evaluation(
        artifact_id=second["id"],
        attempt_id=attempt["id"],
        evaluator="visual_judge",
        result=GateResult(
            gate="visual_fidelity",
            gate_version="judge-v1",
            blocking=False,
            verdict=GateVerdict.CANDIDATE,
            reasons=["second hidden reason"],
        ),
        hidden_until_label=True,
    )
    application = create_app(factory.config, factory=factory)

    with TestClient(application) as client:
        assert client.get(f"/api/attempts/{attempt['id']}").status_code == 401
        blind = client.get(f"/api/attempts/{attempt['id']}", headers=headers())
        assert blind.status_code == 200
        assert blind.json()["evaluations_blind"] is True
        assert blind.json()["evaluations"] == []

        streamed = client.get(f"/artifacts/{artifact['id']}", headers=headers())
        assert streamed.status_code == 200
        assert streamed.content == image
        assert streamed.headers["etag"] == f'"{artifact["content_hash"].removeprefix("sha256:")}"'

        labeled = client.post(
            "/actions/label",
            headers={**headers(), "accept": "application/json"},
            data={
                "attempt_id": attempt["id"],
                "artifact_id": artifact["id"],
                "kind": "prototype_label",
                "outcome": "reject",
                "feedback": "Direction is not readable",
                "tags": "readability",
            },
        )
        assert labeled.status_code == 200
        after = client.get(f"/api/attempts/{attempt['id']}", headers=headers()).json()
        assert after["evaluations_blind"] is True
        assert len(after["evaluations"]) == 1
        assert after["evaluations"][0]["reasons"] == ["hidden reason"]
        assert "outcome:reject" in after["decisions"][0]["tags"]

        second_label = client.post(
            "/actions/label",
            headers={**headers(), "accept": "application/json"},
            data={
                "attempt_id": attempt["id"],
                "artifact_id": second["id"],
                "kind": "prototype_label",
                "outcome": "accept",
                "feedback": "Readable direction",
            },
        )
        assert second_label.status_code == 200
        fully_labeled = client.get(f"/api/attempts/{attempt['id']}", headers=headers()).json()
        assert fully_labeled["evaluations_blind"] is False
        assert len(fully_labeled["evaluations"]) == 2


def test_exact_prototype_approval_rejects_wrong_hash(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("SKIN_FACTORY_REVIEW_TOKEN", "review-secret-123456789")
    factory, attempt, artifact, _ = seeded_factory(tmp_path)
    application = create_app(factory.config, factory=factory)
    form = {
        "attempt_id": attempt["id"],
        "artifact_id": artifact["id"],
        "content_hash": "sha256:" + "0" * 64,
        "feedback": "",
    }
    with TestClient(application) as client:
        wrong = client.post(
            "/actions/approve-prototype",
            headers={**headers(), "accept": "application/json"},
            data=form,
        )
        assert wrong.status_code == 409
        labeled = client.post(
            "/actions/label",
            headers={**headers(), "accept": "application/json"},
            data={
                "attempt_id": attempt["id"],
                "artifact_id": artifact["id"],
                "kind": "prototype_label",
                "outcome": "accept",
                "feedback": "blind first",
            },
        )
        assert labeled.status_code == 200
        form["content_hash"] = artifact["content_hash"]
        accepted = client.post(
            "/actions/approve-prototype",
            headers={**headers(), "accept": "application/json"},
            data=form,
        )
        assert accepted.status_code == 200
        assert accepted.json()["attempt"]["approved_prototype_hash"] == artifact["content_hash"]
        assert accepted.json()["attempt"]["stage"] == "author"


def test_environment_file_is_private_json_not_shell(tmp_path: Path, monkeypatch) -> None:
    environment = tmp_path / "service.json"
    environment.write_text(
        json.dumps({"SAFE_TEST_VALUE": "literal $(touch should-not-run); $HOME"}),
        encoding="utf-8",
    )
    os.chmod(environment, 0o600)
    monkeypatch.setenv("SAFE_TEST_VALUE", "inherited-value-must-lose")
    load_environment(environment)
    assert os.environ["SAFE_TEST_VALUE"] == "literal $(touch should-not-run); $HOME"
    assert not (tmp_path / "should-not-run").exists()
    os.chmod(environment, 0o644)
    try:
        load_environment(environment)
    except PermissionError:
        pass
    else:
        raise AssertionError("world-readable secret file was accepted")


def test_explicit_service_json_wins_and_scrubs_custom_configured_credentials(tmp_path: Path, monkeypatch) -> None:
    config = make_config(tmp_path)
    custom_provider = "CUSTOM_PROVIDER_CREDENTIAL"
    custom_worker = "CUSTOM_WORKER_CREDENTIAL"
    custom_service = "CUSTOM_SNAKETRON_SERVICE_CREDENTIAL"
    custom_webhook = "CUSTOM_WEBHOOK_CREDENTIAL"
    models = config.models.model_copy(
        update={
            "task_worker": config.models.task_worker.model_copy(update={"api_key_env": custom_worker}),
            "smart_text": config.models.smart_text.model_copy(update={"api_key_env": custom_provider}),
            "visual_judge": config.models.visual_judge.model_copy(update={"api_key_env": custom_provider}),
            "image_generator": config.models.image_generator.model_copy(update={"api_key_env": custom_provider}),
        }
    )
    config = config.model_copy(
        update={
            "models": models,
            "service": config.service.model_copy(update={"service_token_env": custom_service}),
            "outbox": config.outbox.model_copy(update={"webhook_token_env": custom_webhook}),
        }
    )
    for name in (custom_provider, custom_worker, custom_service, custom_webhook):
        monkeypatch.setenv(name, f"inherited-{name}")
    environment = tmp_path / "explicit-service.json"
    environment.write_text(
        json.dumps(
            {
                custom_provider: "explicit-provider-value",
                custom_service: "explicit-service-value",
            }
        ),
        encoding="utf-8",
    )
    environment.chmod(0o600)

    load_service_environment(config, environment)

    # Provider/service consumers read only the explicit file; optional
    # configured capabilities absent from it are removed rather than inherited.
    assert config.models.smart_text.secret() == "explicit-provider-value"
    assert os.environ[custom_service] == "explicit-service-value"
    assert custom_worker not in os.environ
    assert custom_webhook not in os.environ


def test_service_environment_rejects_inherited_custom_human_authority(tmp_path: Path, monkeypatch) -> None:
    config = make_config(tmp_path)
    config = config.model_copy(
        update={"review": config.review.model_copy(update={"operator_secret_env": "CUSTOM_REVIEW_AUTHORITY"})}
    )
    monkeypatch.setenv("CUSTOM_REVIEW_AUTHORITY", "never-report-this-value")
    environment = tmp_path / "service.json"
    environment.write_text(
        json.dumps(
            {
                "TEST_GEMINI_KEY": "explicit-provider",
                config.service.service_token_env: "explicit-service",
            }
        ),
        encoding="utf-8",
    )
    environment.chmod(0o600)

    with pytest.raises(PermissionError, match="CUSTOM_REVIEW_AUTHORITY") as raised:
        load_service_environment(config, environment)
    assert "never-report-this-value" not in str(raised.value)


def test_run_once_uses_authoritative_explicit_service_json(tmp_path: Path, monkeypatch) -> None:
    config = make_config(tmp_path)
    custom_provider = "CUSTOM_RUN_PROVIDER_KEY"
    custom_worker = "CUSTOM_RUN_WORKER_KEY"
    custom_service = "CUSTOM_RUN_SERVICE_TOKEN"
    models = config.models.model_copy(
        update={
            "task_worker": config.models.task_worker.model_copy(update={"api_key_env": custom_worker}),
            "smart_text": config.models.smart_text.model_copy(update={"api_key_env": custom_provider}),
            "visual_judge": config.models.visual_judge.model_copy(update={"api_key_env": custom_provider}),
            "image_generator": config.models.image_generator.model_copy(update={"api_key_env": custom_provider}),
        }
    )
    config = config.model_copy(
        update={
            "models": models,
            "service": config.service.model_copy(update={"service_token_env": custom_service}),
        }
    )
    raw = config.model_dump(mode="json", exclude={"source_path", "version_sha256"})
    generated = tmp_path / "factory-service.yaml"
    generated.write_text(yaml.safe_dump(raw), encoding="utf-8")
    environment = tmp_path / "service.json"
    environment.write_text(
        json.dumps(
            {
                custom_provider: "explicit-run-provider",
                custom_service: "explicit-run-service",
            }
        ),
        encoding="utf-8",
    )
    environment.chmod(0o600)
    monkeypatch.setenv(custom_provider, "inherited-provider-must-lose")
    monkeypatch.setenv(custom_worker, "inherited-worker-must-be-removed")
    monkeypatch.setenv(custom_service, "inherited-service-must-lose")
    captured: dict[str, str | None] = {}

    class FakeFactory:
        def __init__(self, settings: FactoryConfig) -> None:
            captured["provider"] = settings.models.smart_text.secret()
            captured["worker"] = os.environ.get(custom_worker)
            captured["service"] = os.environ.get(custom_service)

        async def run_once(self):
            return {"ok": True}

        async def close(self) -> None:
            return None

    monkeypatch.setattr("snaketron_factory.cli.Factory", FakeFactory)
    result = CliRunner().invoke(
        app,
        [
            "run-once",
            "--config",
            str(generated),
            "--env-file",
            str(environment),
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured == {
        "provider": "explicit-run-provider",
        "worker": None,
        "service": "explicit-run-service",
    }


def test_service_doctor_requires_explicit_private_json(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    raw = config.model_dump(mode="json", exclude={"source_path", "version_sha256"})
    generated = tmp_path / "factory-doctor.yaml"
    generated.write_text(yaml.safe_dump(raw), encoding="utf-8")

    result = CliRunner().invoke(
        app,
        ["doctor", "--config", str(generated), "--identity", "service", "--offline", "--json"],
    )

    assert result.exit_code == 1
    assert "require an explicit private --env-file JSON" in result.output


@pytest.mark.parametrize(
    "inherited_name",
    ["CUSTOM_FACTORY_REVIEW_SECRET", "CUSTOM_SNAKETRON_ADMIN_TOKEN", "SKIN_FACTORY_REVIEW_ACTOR"],
)
def test_run_once_rejects_configured_or_actor_human_authority(tmp_path: Path, monkeypatch, inherited_name: str) -> None:
    config = make_config(tmp_path)
    config = config.model_copy(
        update={
            "service": config.service.model_copy(update={"operator_token_env": "CUSTOM_SNAKETRON_ADMIN_TOKEN"}),
            "review": config.review.model_copy(update={"operator_secret_env": "CUSTOM_FACTORY_REVIEW_SECRET"}),
        }
    )
    raw = config.model_dump(mode="json", exclude={"source_path", "version_sha256"})
    generated = tmp_path / "factory-custom-authority.yaml"
    generated.write_text(yaml.safe_dump(raw), encoding="utf-8")
    monkeypatch.setenv(inherited_name, "present-but-never-reported")

    result = CliRunner().invoke(
        app,
        ["run-once", "--config", str(generated), "--json"],
    )

    assert result.exit_code == 1
    assert "run-once cannot inherit human operator authority" in result.output
    assert inherited_name in result.output
    assert "present-but-never-reported" not in result.output


def test_cli_status_backup_and_command_surface(tmp_path: Path) -> None:
    factory, _attempt, artifact, _image = seeded_factory(tmp_path)
    config = factory.config
    # The command uses the checked-in behavior files but a test DB is supplied
    # through a minimal copied config.
    raw = config.model_dump(mode="json", exclude={"source_path", "version_sha256"})
    generated = tmp_path / "factory.yaml"
    import yaml

    generated.write_text(yaml.safe_dump(raw), encoding="utf-8")
    runner = CliRunner()
    status_result = runner.invoke(app, ["status", "--config", str(generated), "--json"])
    assert status_result.exit_code == 0, status_result.output
    status = json.loads(status_result.stdout)
    assert status["ok"] is True
    assert status["program"] == {
        "published_concepts": 0,
        "target_published_skins": 100,
        "target_reached": False,
    }
    assert status["generation_halt"] is None

    backup_target = tmp_path / "backup"
    backup_result = runner.invoke(
        app,
        [
            "backup",
            "--config",
            str(generated),
            "--target",
            str(backup_target),
            "--json",
        ],
    )
    assert backup_result.exit_code == 0, backup_result.output
    assert (backup_target / "factory.sqlite3").is_file()
    manifest = json.loads((backup_target / "manifest.json").read_text())
    assert manifest["version"] == 2
    assert manifest["referenced_object_count"] >= 1
    assert manifest["objects_sha256"]

    verified = runner.invoke(
        app,
        ["verify-backup", "--source", str(backup_target), "--json"],
    )
    assert verified.exit_code == 0, verified.output
    assert json.loads(verified.stdout)["ok"] is True

    # An omitted DB-referenced object is detected by both the signed inventory
    # and the restored database authority graph, even when totals could be
    # forged to look superficially plausible.
    digest = artifact["object_ref"].removeprefix("sha256:")
    (backup_target / "objects/sha256" / digest[:2] / digest).unlink()
    corrupt = runner.invoke(
        app,
        ["verify-backup", "--source", str(backup_target), "--json"],
    )
    assert corrupt.exit_code == 1
    assert "inventory differs" in corrupt.output or "missing" in corrupt.output

    help_result = runner.invoke(app, ["--help"])
    for command in (
        "doctor",
        "run-once",
        "serve",
        "label",
        "approve-prototype",
        "re-evaluate",
        "bulk-retry",
        "publish",
        "resolve-operation",
        "resume-generation",
        "optimize",
        "backup",
        "verify-backup",
        "readiness-pin",
    ):
        assert command in help_result.output


def test_manual_optimizer_cannot_overlap_the_scheduled_production_lease(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    raw = config.model_dump(mode="json", exclude={"source_path", "version_sha256"})
    generated = tmp_path / "factory.yaml"
    generated.write_text(yaml.safe_dump(raw), encoding="utf-8")
    database = Database(config.paths.database)
    database.migrate()
    token = database.acquire_lease("production", "service:scheduled-run", 60)
    try:
        result = CliRunner().invoke(
            app,
            [
                "optimize",
                "--if-ready",
                "--target",
                "authoring-playbook",
                "--config",
                str(generated),
                "--json",
            ],
        )
    finally:
        database.release_lease("production", token)

    assert result.exit_code == 1
    payload = json.loads(result.stderr)
    assert payload["ok"] is False
    assert payload["error"] == "LeaseBusy"
    assert "production" in payload["detail"]


def test_cli_recovery_validates_exact_payload_and_model_before_database_success(tmp_path: Path, monkeypatch) -> None:
    config = make_config(tmp_path)
    raw = config.model_dump(mode="json", exclude={"source_path", "version_sha256"})
    generated = tmp_path / "factory.yaml"
    generated.write_text(yaml.safe_dump(raw), encoding="utf-8")
    database = Database(config.paths.database)
    database.migrate()
    concept = database.create_concept(
        name="Recovery boundary",
        brief="A retained control record for authenticated recovery validation.",
        seed="recovery",
        source="test",
        tags=["recovery"],
    )
    attempt = database.create_attempt(
        concept_id=concept["id"],
        purpose=Purpose.PRODUCTION,
        stage=Stage.CONCEPT,
        idempotency_key=f"recovery:{concept['id']}",
        behavior={},
        direction_sha="direction",
        skill_sha="skill",
        capability_sha="capability",
        gate_sha="gates",
        model_config_sha="models",
    )
    objects = ObjectStore(config.paths.objects)
    request = {"probe": "CLI recovery"}
    retained_request = objects.put(OperationJournal.request_payload(request))
    operation, _ = database.begin_operation(
        attempt_id=attempt["id"],
        stage=Stage.CONCEPT,
        idempotency_key="cli-recovery",
        side_effect="generic_structured_probe",
        provider_role="smart_text",
        request_hash=OperationJournal.request_hash(request),
        cost_reserved_micros=10,
        metadata={
            "request_ref": retained_request.uri,
            "request_sha256": retained_request.sha256,
        },
    )
    database.transition_operation(
        operation["id"],
        OperationStatus.INTENT,
        OperationStatus.RECONCILIATION_REQUIRED,
    )
    invalid = objects.put(b"not-json")
    valid = objects.put(b'{"recovered":true}')
    monkeypatch.setenv(config.review.operator_secret_env, "test-review-secret-at-least-16")

    def resolve(result_hash: str, model: str):
        return CliRunner().invoke(
            app,
            [
                "resolve-operation",
                operation["id"],
                "--resolution",
                "executed_result_recovered",
                "--evidence-ref",
                "provider:audit:cli",
                "--result-hash",
                result_hash,
                "--resolved-model",
                model,
                "--actor",
                "human:operator",
                "--config",
                str(generated),
                "--json",
            ],
        )

    bad_payload = resolve(invalid.uri, "gemini-3.7-flash")
    assert bad_payload.exit_code == 1
    assert database.get_operation(operation["id"])["status"] == OperationStatus.RECONCILIATION_REQUIRED
    bad_model = resolve(valid.uri, "unapproved-fallback")
    assert bad_model.exit_code == 1
    assert database.get_operation(operation["id"])["status"] == OperationStatus.RECONCILIATION_REQUIRED
    accepted = resolve(valid.uri, "gemini-3.7-flash")
    assert accepted.exit_code == 0, accepted.output
    assert database.get_operation(operation["id"])["status"] == OperationStatus.SUCCEEDED
