from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from PIL import Image
from test_gallery_cli import seeded_factory
from typer.testing import CliRunner

from snaketron_factory.cli import app
from snaketron_factory.config import ModelRole, load_config
from snaketron_factory.db import Database, canonical_json
from snaketron_factory.doctor import FactoryDoctor, _safe_error
from snaketron_factory.domain import (
    DoctorCheck,
    GateResult,
    GateVerdict,
    ImplementationPlan,
    OperationStatus,
    ProviderResult,
    Stage,
    WorkerRequest,
    WorkerResult,
)
from snaketron_factory.factory import Factory
from snaketron_factory.promotion import GitPromoter, _run
from snaketron_factory.readiness import ReadinessError, check_paid_smoke, record_paid_smoke
from snaketron_factory.worker import SkillBundle


def test_repository_config_pins_lm_studio_resolved_worker_identity() -> None:
    config = load_config(Path(__file__).resolve().parents[1] / "config/factory.yaml")

    assert config.models.task_worker.model == "qwen/qwen3.8-27b"
    assert config.worker.endpoint == "http://localhost:1234/v1"


def _write_executable(path: Path, source: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")
    path.chmod(0o700)


def _factory_capabilities(
    *,
    user_id: int = 41,
    credential_id: str = "0123456789abcdef0123456789abcdef",
) -> dict[str, object]:
    return {
        "schemaVersion": 1,
        "identity": {
            "userId": user_id,
            "username": "skin-factory",
            "registeredAccount": True,
            "isGuest": False,
            "isAdmin": False,
        },
        "credential": {
            "credentialType": "factoryService",
            "credentialId": credential_id,
            "revocable": True,
            "expiresAt": None,
        },
        "capabilities": {
            "createPrivateSkins": True,
            "createEvaluationSkins": True,
            "uploadPrivateForgeTextures": True,
            "requestPublicationReview": True,
            "publishSkins": False,
            "administerSkins": False,
        },
    }


@pytest.mark.asyncio
async def test_service_doctor_does_not_require_operator_authority(tmp_path: Path, monkeypatch) -> None:
    factory, _, _, _ = seeded_factory(tmp_path)
    monkeypatch.setenv("GEMINI_API_KEY", "service-gemini-value")
    monkeypatch.setenv("SNAKETRON_FACTORY_SERVICE_TOKEN", "private-skin-token")
    monkeypatch.delenv("SKIN_FACTORY_REVIEW_TOKEN", raising=False)
    monkeypatch.delenv("SNAKETRON_FACTORY_OPERATOR_TOKEN", raising=False)
    checker = FactoryDoctor(factory.config, factory=factory)
    checker._lama_check = lambda: DoctorCheck(name="lama_helper", ok=True, detail="test")

    async def browser(*, offline: bool) -> DoctorCheck:
        assert offline is True
        return DoctorCheck(name="playwright_chromium", ok=True, detail="test")

    checker._browser_check = browser
    report = await checker.run(identity="service", offline=True)
    names = {item.name for item in report.checks}
    assert report.ok is True
    assert "cached_renderer_bundle" in names
    assert "prototype_geometry_contract" in names
    assert "prototype_geometry" in names
    assert "credential:SKIN_FACTORY_REVIEW_TOKEN" not in names
    assert "credential:SNAKETRON_FACTORY_OPERATOR_TOKEN" not in names
    assert "credential:SNAKETRON_FACTORY_SERVICE_TOKEN" in names


@pytest.mark.asyncio
async def test_doctor_fails_closed_when_the_blank_prototype_guide_differs_from_its_contract(
    factory_config,
) -> None:
    factory = Factory(factory_config)
    contract = json.loads(factory_config.paths.prototype_geometry.read_text(encoding="utf-8"))
    guide = factory_config.paths.prototype_geometry.parent / contract["guide"]
    guide.write_bytes(guide.read_bytes() + b"drift")

    check = FactoryDoctor(factory_config, factory=factory)._prototype_geometry_check()
    assert check.name == "prototype_geometry"
    assert check.ok is False
    assert "guide hash" in check.detail
    await factory.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("identity_update", "capability_update", "expected"),
    [
        ({}, {}, True),
        ({"isAdmin": True}, {"publishSkins": True, "administerSkins": True}, False),
        ({"isGuest": True, "registeredAccount": False}, {}, False),
        ({}, {"uploadPrivateForgeTextures": False}, False),
    ],
)
async def test_doctor_requires_a_useful_non_admin_snaketron_service_identity(
    factory_config,
    identity_update: dict[str, object],
    capability_update: dict[str, object],
    expected: bool,
) -> None:
    identity = {
        "userId": 41,
        "username": "skin-factory",
        "registeredAccount": True,
        "isGuest": False,
        "isAdmin": False,
        **identity_update,
    }
    capabilities = {
        "createPrivateSkins": True,
        "createEvaluationSkins": True,
        "uploadPrivateForgeTextures": True,
        "requestPublicationReview": True,
        "publishSkins": False,
        "administerSkins": False,
        **capability_update,
    }
    credential = {
        "credentialType": "factoryService",
        "credentialId": "0123456789abcdef0123456789abcdef",
        "revocable": True,
        "expiresAt": None,
    }

    class Api:
        @staticmethod
        async def service_capabilities():
            return {
                "schemaVersion": 1,
                "identity": identity,
                "credential": credential,
                "capabilities": capabilities,
            }

    factory = Factory(factory_config)
    await factory.api.close()
    factory.api = Api()  # type: ignore[assignment]
    check = await FactoryDoctor(factory_config, factory=factory)._api_capability_check()
    await factory.close()

    assert check.name == "snaketron_service_capabilities"
    assert check.ok is expected
    if expected:
        assert "publish/admin authority absent" in check.detail
    else:
        assert "non-admin factory identity" in check.detail


@pytest.mark.asyncio
async def test_content_model_preflight_accepts_configurable_openai_image_provider(tmp_path: Path) -> None:
    factory, _, _, _ = seeded_factory(tmp_path)
    factory.config.models.image_generator = ModelRole.model_validate(
        {
            "provider": "openai_compatible",
            "model": "operator-image-v1",
            "base_url": "https://images.example.test/v1",
        }
    )

    class Description:
        def __init__(self, value: dict[str, str]) -> None:
            self.value = value

        async def describe_model(self) -> dict[str, str]:
            return self.value

    descriptions = {
        "smart_text": Description({"name": "models/gemini-3.7-flash"}),
        "visual_judge": Description({"name": "models/gemini-3.7-flash"}),
        "image_generator": Description({"id": "operator-image-v1"}),
    }

    class Providers:
        def role(self, name: str) -> Description:
            return descriptions[name]

    factory.providers = Providers()  # type: ignore[assignment]
    check = await FactoryDoctor(factory.config, factory=factory)._content_models_check()
    await factory.close()

    assert check.name == "content_models"
    assert check.ok is True
    assert "image_generator(openai_compatible)=operator-image-v1" in check.detail


@pytest.mark.asyncio
async def test_online_doctor_executes_side_effect_free_worker_contract(factory_config, monkeypatch) -> None:
    monkeypatch.setattr("snaketron_factory.doctor.secrets.choice", lambda _alphabet: "8")
    received: list[WorkerRequest] = []

    class Worker:
        async def execute(self, request: WorkerRequest) -> ProviderResult:
            received.append(request)
            return ProviderResult(
                value=WorkerResult(
                    implementation_plan=ImplementationPlan(
                        path="layers",
                        rationale="A procedural fixture requires no external assets.",
                        fidelity_features=["clear stripe"],
                        layer_plan=["base stripe"],
                        asset_plan=[],
                        animation_plan=[],
                        required_wrap_axes=[],
                        risks=[],
                        design_guidelines={
                            "artistic_direction": "One procedural conformance direction.",
                            "concept_twist": "Original doctor fixture.",
                            "structure": "pattern",
                            "body_strategy": "Reads at four cells and across growth and turns.",
                            "head_zone": "light_field_dark_core",
                            "asset_strategy": "Procedural layers require no raster seams.",
                        },
                    ),
                    skin_document={
                        "schema_version": 2,
                        "name": "Doctor Visual 888888",
                        "palette": {
                            "friendly": [{"fill": "#25c776", "outline": "#155c3d", "accent": "#d9fff0"}],
                            "enemy": [{"fill": "#e45d6a", "outline": "#7c2832", "accent": "#ffd0d5"}],
                            "free_for_all": [{"fill": "#25c776", "outline": "#155c3d", "accent": "#d9fff0"}],
                        },
                        "period_ms": 1000,
                        "head_core": {"ratio": 0.38, "color": "#155c3d"},
                        "textures": [],
                        "layers": [
                            {
                                "name": "Swatch body",
                                "type": "ribbon",
                                "region": "body",
                                "color": {"slot": "fill"},
                                "extra_px": 0,
                                "joints": True,
                                "tail_cap": True,
                            }
                        ],
                    },
                    tool_requests=[],
                ),
                resolved_model="worker-test",
                usage={"cost_micros": 0},
            )

    factory = Factory(factory_config)
    factory.worker = Worker()  # type: ignore[assignment]

    class ConformanceGates:
        @staticmethod
        def validate_document(_document, _plan):
            return [
                GateResult(
                    gate="document_schema",
                    gate_version="doctor-test-v1",
                    blocking=True,
                    verdict=GateVerdict.PASS,
                )
            ]

        @staticmethod
        def blocking_failure(results):
            return any(result.blocking and result.verdict == GateVerdict.FAIL for result in results)

    factory.gates = ConformanceGates()  # type: ignore[assignment]
    check = await FactoryDoctor(factory_config, factory=factory)._worker_conformance_check()
    await factory.close()

    assert check.ok is True
    assert "exact model 'worker-test'" in check.detail
    assert len(received) == 1
    request = received[0]
    assert request.request_id == "doctor-worker-conformance-v2"
    assert request.attempt_id == "doctor-side-effect-free-fixture"
    assert request.artifact_refs == {}
    assert set(request.inline_artifacts) == {"approved_prototype"}
    card = request.inline_artifacts["approved_prototype"]
    assert card.media_type == "image/png"
    card_bytes = base64.b64decode(card.base64_data, validate=True)
    assert card.content_hash == f"sha256:{hashlib.sha256(card_bytes).hexdigest()}"
    with Image.open(io.BytesIO(card_bytes)) as image:
        assert image.format == "PNG"
        assert image.size == (768, 320)
    assert "888888" not in json.dumps(request.authoring_inputs, sort_keys=True)
    assert request.pure_tools == []


@pytest.mark.asyncio
async def test_paid_smoke_marker_is_owner_private_and_behavior_bound(factory_config) -> None:
    factory = Factory(factory_config)
    bundle = SkillBundle.load(factory_config.paths.skill_dir)
    factory.active_skill_bundle = lambda: (bundle, "HEAD", "a" * 40)  # type: ignore[method-assign]
    factory._current_renderer_sha = lambda: "renderer-test-sha"  # type: ignore[method-assign]

    capabilities = _factory_capabilities()
    marker = record_paid_smoke(factory, capabilities)
    path = factory_config.paths.data_dir / "hermes-paid-smoke.json"
    assert path.stat().st_mode & 0o777 == 0o600
    assert marker["version"] == 3
    assert marker["factory_service_user_id"] == 41
    assert check_paid_smoke(factory, capabilities) == marker

    factory_config.paths.direction.write_text("changed behavior\n", encoding="utf-8")
    with pytest.raises(ReadinessError, match="stale"):
        check_paid_smoke(factory, capabilities)
    await factory.close()


@pytest.mark.asyncio
async def test_paid_smoke_pins_account_identity_but_allows_credential_rotation(factory_config) -> None:
    factory = Factory(factory_config)
    bundle = SkillBundle.load(factory_config.paths.skill_dir)
    factory.active_skill_bundle = lambda: (bundle, "HEAD", "a" * 40)  # type: ignore[method-assign]
    factory._current_renderer_sha = lambda: "renderer-test-sha"  # type: ignore[method-assign]
    original = _factory_capabilities(
        user_id=41,
        credential_id="0123456789abcdef0123456789abcdef",
    )
    marker = record_paid_smoke(factory, original)

    rotated = _factory_capabilities(
        user_id=41,
        credential_id="fedcba9876543210fedcba9876543210",
    )
    assert check_paid_smoke(factory, rotated) == marker
    assert "0123456789abcdef0123456789abcdef" not in json.dumps(marker)

    substituted = _factory_capabilities(
        user_id=42,
        credential_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    with pytest.raises(ReadinessError, match="account differs"):
        check_paid_smoke(factory, substituted)
    await factory.close()


def test_readiness_cli_rechecks_pinned_account_before_enable_or_tick(factory_config, monkeypatch) -> None:
    factory_config.source_path.write_text("config_version: 1\n", encoding="utf-8")
    live = {"capabilities": _factory_capabilities(user_id=41)}

    class DatabaseFixture:
        @staticmethod
        def migrate() -> None:
            return None

    class ApiFixture:
        @staticmethod
        async def service_capabilities() -> dict[str, object]:
            return live["capabilities"]

    class FactoryFixture:
        def __init__(self, config) -> None:
            self.config = config
            self.database = DatabaseFixture()
            self.api = ApiFixture()

        @staticmethod
        def behavior_snapshot() -> dict[str, str]:
            return {
                "config_sha": "config-sha",
                "skill_sha": "skill-sha",
                "skill_git_ref": "HEAD",
                "skill_git_sha": "a" * 40,
                "model_config_sha": "model-sha",
                "renderer_config_sha": "renderer-sha",
                "lama_bundle_sha": "lama-sha",
            }

        @staticmethod
        async def close() -> None:
            return None

    monkeypatch.setattr("snaketron_factory.cli._load", lambda *_args, **_kwargs: factory_config)
    monkeypatch.setattr("snaketron_factory.cli.Factory", FactoryFixture)
    runner = CliRunner()
    common = ["--config", str(factory_config.source_path), "--json"]

    recorded = runner.invoke(app, ["readiness-pin", "--record-paid-smoke", *common])
    assert recorded.exit_code == 0, recorded.output
    assert json.loads(recorded.stdout)["marker"]["factory_service_user_id"] == 41

    live["capabilities"] = _factory_capabilities(
        user_id=41,
        credential_id="fedcba9876543210fedcba9876543210",
    )
    rotated = runner.invoke(app, ["readiness-pin", "--check-paid-smoke", *common])
    assert rotated.exit_code == 0, rotated.output
    assert json.loads(rotated.stdout)["service_identity"]["credential_id"] == ("fedcba9876543210fedcba9876543210")

    live["capabilities"] = _factory_capabilities(
        user_id=42,
        credential_id="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    substituted = runner.invoke(app, ["readiness-pin", "--check-paid-smoke", *common])
    assert substituted.exit_code == 1
    assert json.loads(substituted.stderr)["error"] == "ReadinessError"
    assert "account differs" in substituted.stderr


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["config", "model", "renderer", "lama"])
async def test_paid_smoke_pin_rejects_runtime_drift_before_a_scheduled_run(factory_config, drift: str) -> None:
    factory = Factory(factory_config)
    bundle = SkillBundle.load(factory_config.paths.skill_dir)
    factory.active_skill_bundle = lambda: (bundle, "HEAD", "a" * 40)  # type: ignore[method-assign]
    factory._current_renderer_sha = lambda: "renderer-test-sha"  # type: ignore[method-assign]
    capabilities = _factory_capabilities()
    record_paid_smoke(factory, capabilities)

    if drift == "config":
        factory_config.version_sha256 = "changed-config-sha"
    elif drift == "model":
        factory_config.models.image_generator.model = "changed-image-model"
    elif drift == "renderer":
        factory_config.browser.timeout_seconds += 1
    else:
        runtime = factory_config.paths.lama_manifest.parent / "sitecustomize.py"
        runtime.write_text("# changed pinned LaMa runtime\n", encoding="utf-8")

    with pytest.raises(ReadinessError, match="stale"):
        check_paid_smoke(factory, capabilities)
    await factory.close()


@pytest.mark.asyncio
async def test_paid_smoke_pin_allows_only_an_exact_journaled_author_skill_promotion(factory_config) -> None:
    factory = Factory(factory_config)
    original = SkillBundle.load(factory_config.paths.skill_dir)
    factory.active_skill_bundle = lambda: (original, "HEAD", "a" * 40)  # type: ignore[method-assign]
    factory._current_renderer_sha = lambda: "renderer-test-sha"  # type: ignore[method-assign]
    capabilities = _factory_capabilities()
    marker = record_paid_smoke(factory, capabilities)

    coordinator = factory.control_attempt("readiness-promotion")
    run = factory.database.create_optimization_run(
        target="authoring_playbook",
        dataset_version="sha256:" + "d" * 64,
        teacher_config={"model": "gemini-3.7-flash"},
        student_config={"model": "worker-test"},
    )
    git_ref = f"refs/tags/skin-authoring/{run['id']}"
    git_sha = "b" * 40
    factory.database.set_active_behavior("author-skin", git_ref, git_sha)
    factory_config.paths.skill_dir.joinpath("playbook.md").write_text(
        "Automatically promoted, gate-validated playbook.\n",
        encoding="utf-8",
    )
    promoted = SkillBundle.load(factory_config.paths.skill_dir)
    factory.active_skill_bundle = lambda: (promoted, git_ref, git_sha)  # type: ignore[method-assign]

    result = factory.objects.put(
        canonical_json({"git_ref": git_ref, "sha": git_sha, "branch": f"bot/skin-authoring/{run['id']}"}).encode()
    )
    operation, created = factory.database.begin_operation(
        attempt_id=coordinator["id"],
        stage=Stage.COMPLETE,
        idempotency_key=f"gepa:{run['id']}:promote:winner:{'a' * 40}",
        side_effect="promote_authoring_playbook",
        provider_role="git_promotion",
        request_hash="request-sha",
        cost_reserved_micros=0,
    )
    assert created
    operation = factory.database.transition_operation(
        operation["id"],
        OperationStatus.INTENT,
        OperationStatus.RUNNING,
    )
    factory.database.transition_operation(
        operation["id"],
        OperationStatus.RUNNING,
        OperationStatus.SUCCEEDED,
        result_hash=result.uri,
        retry_class="complete",
        cost_charged_micros=0,
    )
    current_run = factory.database.get_optimization_run(run["id"])
    factory.database.update_optimization_run(
        run["id"],
        current_run["version"],
        state="promoted",
        promoted_ref=git_ref,
        promoted_sha=git_sha,
    )

    assert check_paid_smoke(factory, capabilities) == marker

    # Repointing the same bounded package without a matching promotion result
    # is arbitrary behavior drift, not an optimizer promotion.
    unverified_sha = "c" * 40
    factory.database.set_active_behavior("author-skin", "refs/tags/skin-authoring/unverified", unverified_sha)
    factory.active_skill_bundle = lambda: (promoted, "refs/tags/skin-authoring/unverified", unverified_sha)  # type: ignore[method-assign]
    with pytest.raises(ReadinessError, match="without an exact verified automatic promotion"):
        check_paid_smoke(factory, capabilities)
    await factory.close()


def test_doctor_redacts_secret_values(monkeypatch) -> None:
    monkeypatch.setenv("DOCTOR_TEST_SECRET", "sufficiently-long-secret-value")
    detail = _safe_error(RuntimeError("failed with sufficiently-long-secret-value"))
    assert "sufficiently-long-secret-value" not in detail
    assert "<redacted>" in detail


def test_hermes_scripts_enforce_locked_no_agent_service_only_install() -> None:
    root = Path(__file__).resolve().parents[1]
    installer = (root / "scripts/install-hermes.sh").read_text(encoding="utf-8")
    wrapper = (root / "scripts/hermes-run-once.sh").read_text(encoding="utf-8")
    renderer_builder = (root / "scripts/build-renderer-bundle.sh").read_text(encoding="utf-8")
    assert 'sync --project "$package" --frozen --no-dev --extra production' in installer
    assert '"$package/.venv/bin/playwright" install chromium' in installer
    assert 'UV_PROJECT_ENVIRONMENT="$lama_dir" "$uv_bin" sync' in installer
    assert "factory data directory cannot be a symlink" in installer
    assert "LaMa environment directory cannot be a symlink" in installer
    assert '--project "$package/lama"' in installer
    assert "--no-install-project" in installer
    assert "--no-python-downloads" in installer
    assert "verify_lama_model" in installer
    assert "--proto '=https'" in installer
    assert 'chmod 400 "$lama_model"' in installer
    assert "lama-requirements.txt" not in installer
    assert '"$package/scripts/build-renderer-bundle.sh"' in installer
    assert "wasm-pack" in renderer_builder
    assert '"$npm_bin" ci' in renderer_builder
    assert 'SNAKETRON_FACTORY_RENDERER_BUILD=true "$npm_bin" run build:prod' in renderer_builder
    assert '--script "$installed_name"' in installer
    assert "--no-agent" in installer
    assert '--workdir "$repo_root"' in installer
    assert "HERMES_CRON_SCRIPT_TIMEOUT" in installer
    assert '"$hermes_bin" gateway restart' in installer
    assert '"$hermes_bin" gateway status' in installer
    assert "forbidden" in installer
    assert "SKIN_FACTORY_REVIEW_TOKEN" in installer
    assert "SNAKETRON_FACTORY_OPERATOR_TOKEN" in installer
    assert "source " not in wrapper
    assert "eval " not in wrapper
    assert "--check-paid-smoke" in wrapper
    assert "run-once" in wrapper


def test_service_example_contains_no_human_credentials() -> None:
    root = Path(__file__).resolve().parents[1]
    value = json.loads((root / "scripts/factory.service-env.example.json").read_text(encoding="utf-8"))
    assert set(value) == {"GEMINI_API_KEY", "SNAKETRON_FACTORY_SERVICE_TOKEN"}
    assert "SKIN_FACTORY_REVIEW_TOKEN" not in value
    assert "SNAKETRON_FACTORY_OPERATOR_TOKEN" not in value


def test_service_environment_manifest_matches_configured_capabilities() -> None:
    root = Path(__file__).resolve().parents[1]
    config = load_config(root / "config/factory.yaml")
    manifest = json.loads((root / "config/service-env.json").read_text(encoding="utf-8"))
    entries = [*manifest["required"], *manifest["optional"]]
    assert {entry["name"] for entry in entries} == config.credential_environment_names()
    assert {entry["name"] for entry in manifest["required"]} == config.required_service_environment_names()
    assert {
        entry["name"] for entry in entries if entry.get("identity") == "human_operator_only"
    } == config.human_authority_environment_names()


def test_promotion_subprocess_environment_scrubs_custom_service_secrets_and_keeps_git_gpg(
    factory_config, database: Database, monkeypatch, tmp_path: Path
) -> None:
    custom_provider = "CUSTOM_PROMOTION_PROVIDER_KEY"
    custom_worker = "CUSTOM_PROMOTION_WORKER_KEY"
    custom_service = "CUSTOM_PROMOTION_SERVICE_TOKEN"
    custom_operator = "CUSTOM_PROMOTION_OPERATOR_TOKEN"
    custom_review = "CUSTOM_PROMOTION_REVIEW_TOKEN"
    custom_webhook = "CUSTOM_PROMOTION_WEBHOOK_TOKEN"
    models = factory_config.models.model_copy(
        update={
            "task_worker": factory_config.models.task_worker.model_copy(update={"api_key_env": custom_worker}),
            "smart_text": factory_config.models.smart_text.model_copy(update={"api_key_env": custom_provider}),
            "visual_judge": factory_config.models.visual_judge.model_copy(update={"api_key_env": custom_provider}),
            "image_generator": factory_config.models.image_generator.model_copy(
                update={"api_key_env": custom_provider}
            ),
        }
    )
    config = factory_config.model_copy(
        update={
            "models": models,
            "service": factory_config.service.model_copy(
                update={
                    "service_token_env": custom_service,
                    "operator_token_env": custom_operator,
                }
            ),
            "review": factory_config.review.model_copy(update={"operator_secret_env": custom_review}),
            "outbox": factory_config.outbox.model_copy(update={"webhook_token_env": custom_webhook}),
        }
    )
    for name in config.credential_environment_names():
        monkeypatch.setenv(name, f"secret-for-{name}")
    monkeypatch.setenv("GIT_SSH_COMMAND", "ssh -F /safe/git-config")
    monkeypatch.setenv("GNUPGHOME", str(tmp_path / "gnupg"))
    monkeypatch.setenv("GPG_TTY", "/dev/ttysafe")

    promoter = GitPromoter(config, database)
    assert config.credential_environment_names().isdisjoint(promoter._subprocess_env)
    assert promoter._subprocess_env["GIT_SSH_COMMAND"] == "ssh -F /safe/git-config"
    assert promoter._subprocess_env["GNUPGHOME"] == str(tmp_path / "gnupg")
    assert promoter._subprocess_env["GPG_TTY"] == "/dev/ttysafe"

    received: list[dict[str, str]] = []

    def fake_run(*_args, env: dict[str, str], **_kwargs):
        received.append(env)
        return subprocess.CompletedProcess(["git"], 0, "", "")

    monkeypatch.setattr("snaketron_factory.promotion.subprocess.run", fake_run)
    _run(tmp_path, "git", "status", env=promoter._subprocess_env)
    assert received == [promoter._subprocess_env]
    with pytest.raises(TypeError, match="env"):
        _run(tmp_path, "git", "status")  # type: ignore[call-arg]


def test_promotion_subprocess_timeout_is_one_shared_absolute_deadline(tmp_path: Path, monkeypatch) -> None:
    observed: list[float] = []

    def fake_run(*_args, timeout: float, **_kwargs):
        observed.append(timeout)
        return subprocess.CompletedProcess(["git"], 0, "", "")

    monotonic = iter([100.0, 103.5])
    monkeypatch.setattr("snaketron_factory.promotion.time.monotonic", lambda: next(monotonic))
    monkeypatch.setattr("snaketron_factory.promotion.subprocess.run", fake_run)

    _run(tmp_path, "git", "status", env={}, deadline=110.0)
    _run(tmp_path, "git", "status", env={}, deadline=110.0)

    assert observed == [10.0, 6.5]


def test_no_agent_wrapper_uses_explicit_workdir_env_and_one_command(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    package = tmp_path / "skin-factory"
    executable = package / ".venv/bin/factory"
    executable.parent.mkdir(parents=True)
    (package / "pyproject.toml").write_text("[project]\nname='fixture'\n", encoding="utf-8")
    (package / ".factory.env").write_text("{}\n", encoding="utf-8")
    executable.write_text(
        """#!/bin/sh
set -eu
test -z "${SKIN_FACTORY_REVIEW_TOKEN:-}"
test -z "${SKIN_FACTORY_REVIEW_ACTOR:-}"
test -z "${SNAKETRON_FACTORY_OPERATOR_TOKEN:-}"
test -z "${GEMINI_API_KEY:-}"
test -z "${SNAKETRON_FACTORY_SERVICE_TOKEN:-}"
if [ "${1:-}" = readiness-pin ] && [ "${FAIL_READINESS_PIN:-}" = 1 ]; then
  exit 1
fi
if [ "${1:-}" = run-once ] && [ -n "${RUN_ONCE_SENTINEL:-}" ]; then
  : > "$RUN_ONCE_SENTINEL"
fi
printf '%s\n' "$@"
""",
        encoding="utf-8",
    )
    os.chmod(executable, 0o700)
    environment = os.environ.copy()
    environment.update(
        {
            "SKIN_FACTORY_REVIEW_TOKEN": "inherited-review-authority",
            "SKIN_FACTORY_REVIEW_ACTOR": "human:inherited-operator",
            "SNAKETRON_FACTORY_OPERATOR_TOKEN": "inherited-publish-authority",
            "GEMINI_API_KEY": "inherited-provider-authority",
            "SNAKETRON_FACTORY_SERVICE_TOKEN": "inherited-service-authority",
        }
    )
    completed = subprocess.run(
        [str(root / "scripts/hermes-run-once.sh")],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    arguments = completed.stdout.splitlines()
    assert arguments == [
        "run-once",
        "--config",
        str(package / "config/factory.yaml"),
        "--env-file",
        str(package / ".factory.env"),
        "--json",
    ]

    sentinel = tmp_path / "provider-spend-started"
    environment["FAIL_READINESS_PIN"] = "1"
    environment["RUN_ONCE_SENTINEL"] = str(sentinel)
    refused = subprocess.run(
        [str(root / "scripts/hermes-run-once.sh")],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert refused.returncode != 0
    assert not sentinel.exists(), "run-once/provider spend started after readiness drift"


def test_installer_rejects_operator_credentials_before_install(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    environment = tmp_path / "bad-service.json"
    environment.write_text(
        json.dumps(
            {
                "GEMINI_API_KEY": "service-key",
                "SNAKETRON_FACTORY_SERVICE_TOKEN": "service-token",
                "SKIN_FACTORY_REVIEW_TOKEN": "must-not-reach-cron",
            }
        ),
        encoding="utf-8",
    )
    completed = subprocess.run(
        [str(root / "scripts/install-hermes.sh"), str(environment)],
        capture_output=True,
        text=True,
        timeout=20,
        check=False,
    )
    assert completed.returncode != 0
    assert "must not contain operator credentials" in completed.stderr


def test_disposable_hermes_install_status_smoke_run_once_and_rollback(tmp_path: Path) -> None:
    """Exercise the scheduler lifecycle without reading or mutating real Hermes state."""

    source = Path(__file__).resolve().parents[1]
    repo = tmp_path / "repo"
    package = repo / "skin-factory"
    shutil.copytree(
        source,
        package,
        ignore=shutil.ignore_patterns(".venv", ".factory.env", "var", "__pycache__"),
    )
    manifest_path = package / "config/service-env.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["optional"].append(
        {
            "name": "CUSTOM_INHERITED_PROVIDER_CREDENTIAL",
            "consumer": ["adversarial_install_test"],
            "secret": True,
        }
    )
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    fake_lama_model = tmp_path / "fake-big-lama.pt"
    fake_lama_model.write_bytes(b"bounded installer model fixture")
    lama_manifest_path = package / "lama/manifest.json"
    lama_manifest = json.loads(lama_manifest_path.read_text(encoding="utf-8"))
    lama_manifest["model"].update(
        {
            "sha256": hashlib.sha256(fake_lama_model.read_bytes()).hexdigest(),
            "size_bytes": fake_lama_model.stat().st_size,
            "url": "https://models.test/big-lama.pt",
        }
    )
    lama_manifest_path.write_text(json.dumps(lama_manifest), encoding="utf-8")
    client = repo / "client"
    web = client / "web"
    web.mkdir(parents=True)
    (client / "Cargo.toml").write_text("[package]\nname='renderer-fixture'\n", encoding="utf-8")
    (web / "package-lock.json").write_text("{}\n", encoding="utf-8")

    fake_bin = tmp_path / "bin"
    uv_log = tmp_path / "uv.log"
    hermes_log = tmp_path / "hermes.log"
    hermes_state = tmp_path / "hermes-cron-state"
    factory_log = tmp_path / "factory.log"
    renderer_build_log = tmp_path / "renderer-build.log"
    home = tmp_path / "home"
    hermes_home = tmp_path / "isolated-hermes"
    home.mkdir()

    _write_executable(
        fake_bin / "uv",
        """#!/bin/sh
set -eu
test -z "${SKIN_FACTORY_REVIEW_TOKEN:-}"
test -z "${SKIN_FACTORY_REVIEW_ACTOR:-}"
test -z "${SNAKETRON_FACTORY_OPERATOR_TOKEN:-}"
test -z "${GEMINI_API_KEY:-}"
test -z "${LMSTUDIO_API_KEY:-}"
test -z "${SNAKETRON_FACTORY_SERVICE_TOKEN:-}"
test -z "${CUSTOM_INHERITED_PROVIDER_CREDENTIAL:-}"
printf '%s\n' "$*" >> "$FAKE_UV_LOG"
if [ -n "${UV_PROJECT_ENVIRONMENT:-}" ]; then
  mkdir -p "$UV_PROJECT_ENVIRONMENT/bin"
  ln -sf "$FAKE_LAMA_PYTHON" "$UV_PROJECT_ENVIRONMENT/bin/python"
fi
""",
    )
    _write_executable(
        fake_bin / "curl",
        """#!/bin/sh
set -eu
test -z "${SKIN_FACTORY_REVIEW_TOKEN:-}"
test -z "${SKIN_FACTORY_REVIEW_ACTOR:-}"
test -z "${SNAKETRON_FACTORY_OPERATOR_TOKEN:-}"
test -z "${GEMINI_API_KEY:-}"
test -z "${LMSTUDIO_API_KEY:-}"
test -z "${SNAKETRON_FACTORY_SERVICE_TOKEN:-}"
test -z "${CUSTOM_INHERITED_PROVIDER_CREDENTIAL:-}"
target=''
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--output" ]; then
    target="$2"
    shift 2
  else
    shift
  fi
done
test -n "$target"
cp "$FAKE_LAMA_MODEL_SOURCE" "$target"
""",
    )
    _write_executable(
        fake_bin / "git",
        """#!/bin/sh
set -eu
test -z "${SKIN_FACTORY_REVIEW_TOKEN:-}"
test -z "${SKIN_FACTORY_REVIEW_ACTOR:-}"
test -z "${SNAKETRON_FACTORY_OPERATOR_TOKEN:-}"
test -z "${GEMINI_API_KEY:-}"
test -z "${LMSTUDIO_API_KEY:-}"
test -z "${SNAKETRON_FACTORY_SERVICE_TOKEN:-}"
test -z "${CUSTOM_INHERITED_PROVIDER_CREDENTIAL:-}"
printf '%s\n' "$*" >> "$FAKE_GIT_LOG"
""",
    )
    _write_executable(
        fake_bin / "wasm-pack",
        """#!/bin/sh
set -eu
test -z "${SKIN_FACTORY_REVIEW_TOKEN:-}"
test -z "${SKIN_FACTORY_REVIEW_ACTOR:-}"
test -z "${SNAKETRON_FACTORY_OPERATOR_TOKEN:-}"
test -z "${GEMINI_API_KEY:-}"
test -z "${LMSTUDIO_API_KEY:-}"
test -z "${SNAKETRON_FACTORY_SERVICE_TOKEN:-}"
test -z "${CUSTOM_INHERITED_PROVIDER_CREDENTIAL:-}"
printf 'wasm-pack %s\n' "$*" >> "$FAKE_RENDERER_BUILD_LOG"
""",
    )
    _write_executable(
        fake_bin / "npm",
        """#!/bin/sh
set -eu
test -z "${SKIN_FACTORY_REVIEW_TOKEN:-}"
test -z "${SKIN_FACTORY_REVIEW_ACTOR:-}"
test -z "${SNAKETRON_FACTORY_OPERATOR_TOKEN:-}"
test -z "${GEMINI_API_KEY:-}"
test -z "${LMSTUDIO_API_KEY:-}"
test -z "${SNAKETRON_FACTORY_SERVICE_TOKEN:-}"
test -z "${CUSTOM_INHERITED_PROVIDER_CREDENTIAL:-}"
printf 'npm %s\n' "$*" >> "$FAKE_RENDERER_BUILD_LOG"
if [ "${1:-} ${2:-}" = "run build:prod" ]; then
  mkdir -p dist
  printf '%s\n' '<!doctype html><script src="/main.js"></script>' > dist/index.html
  printf '%s\n' 'export const renderer = true;' > dist/main.js
  : > dist/client_bg.wasm
fi
""",
    )
    _write_executable(
        fake_bin / "hermes",
        """#!/bin/sh
set -eu
test -z "${SKIN_FACTORY_REVIEW_TOKEN:-}"
test -z "${SKIN_FACTORY_REVIEW_ACTOR:-}"
test -z "${SNAKETRON_FACTORY_OPERATOR_TOKEN:-}"
test -z "${GEMINI_API_KEY:-}"
test -z "${LMSTUDIO_API_KEY:-}"
test -z "${SNAKETRON_FACTORY_SERVICE_TOKEN:-}"
test -z "${CUSTOM_INHERITED_PROVIDER_CREDENTIAL:-}"
printf '%s\n' "$*" >> "$FAKE_HERMES_LOG"
case "${1:-} ${2:-} ${3:-}" in
  "cron list --all")
    if [ -f "$FAKE_HERMES_STATE" ]; then
      printf '%s\n' 'abc123 snaketron-skin-factory active'
    fi
    ;;
  "cron create "*)
    printf '%s\n' active > "$FAKE_HERMES_STATE"
    printf '%s\n' 'Created job: abc123'
    ;;
  "cron status ")
    printf '%s\n' 'scheduler: healthy'
    ;;
  "cron remove "*)
    unlink "$FAKE_HERMES_STATE"
    printf '%s\n' "Removed job: ${3:-}"
    ;;
esac
""",
    )

    venv = package / ".venv/bin"
    venv.mkdir(parents=True)
    (venv / "python").symlink_to(sys.executable)
    _write_executable(
        venv / "factory",
        """#!/bin/sh
set -eu
test -z "${SKIN_FACTORY_REVIEW_TOKEN:-}"
test -z "${SKIN_FACTORY_REVIEW_ACTOR:-}"
test -z "${SNAKETRON_FACTORY_OPERATOR_TOKEN:-}"
test -z "${GEMINI_API_KEY:-}"
test -z "${LMSTUDIO_API_KEY:-}"
test -z "${SNAKETRON_FACTORY_SERVICE_TOKEN:-}"
test -z "${CUSTOM_INHERITED_PROVIDER_CREDENTIAL:-}"
printf '%s\n' "$*" >> "$FAKE_FACTORY_LOG"
case "${1:-}" in
  doctor)
    case " $* " in
      *" --offline "*) ;;
      *)
        if [ "${FAKE_WORKER_CONFORMANCE_READY:-}" != 1 ]; then
          printf '%s\n' '{"ok":false,"checks":[{"name":"task_worker_conformance","ok":false}]}'
          exit 1
        fi
        ;;
    esac
    ;;
  run-once)
    if [ "${FAKE_PAID_SMOKE_FAIL:-}" = 1 ]; then
      printf '%s\n' '{"advanced":[{"failure":"provider failed","state":"blocked","to":"prototype"}],"halt":null}'
    else
      printf '%s\n' \
        '{"advanced":[{"from":"prototype_triage","state":"needs_human","to":"prototype_review"}],"halt":null}'
    fi
    exit 0
    ;;
  readiness-pin)
    case "$*" in
      *--record-paid-smoke*) printf '%s\n' ready > "$FAKE_READY_MARKER" ;;
      *--check-paid-smoke*) test -f "$FAKE_READY_MARKER" ;;
    esac
    ;;
esac
printf '%s\n' '{"ok":true}'
""",
    )
    _write_executable(
        venv / "playwright",
        """#!/bin/sh
set -eu
test -z "${SKIN_FACTORY_REVIEW_TOKEN:-}"
test -z "${SKIN_FACTORY_REVIEW_ACTOR:-}"
test -z "${SNAKETRON_FACTORY_OPERATOR_TOKEN:-}"
test -z "${GEMINI_API_KEY:-}"
test -z "${LMSTUDIO_API_KEY:-}"
test -z "${SNAKETRON_FACTORY_SERVICE_TOKEN:-}"
test -z "${CUSTOM_INHERITED_PROVIDER_CREDENTIAL:-}"
printf 'playwright %s\n' "$*" >> "$FAKE_RENDERER_BUILD_LOG"
""",
    )

    service_env = tmp_path / "service.json"
    service_env.write_text(
        json.dumps(
            {
                "GEMINI_API_KEY": "synthetic-nonsecret-gemini-value",
                "SNAKETRON_FACTORY_SERVICE_TOKEN": "synthetic-nonsecret-service-value",
            }
        ),
        encoding="utf-8",
    )
    service_env.chmod(0o600)

    environment = os.environ.copy()
    environment.update(
        {
            "HOME": str(home),
            "HERMES_HOME": str(hermes_home),
            "PATH": f"{fake_bin}{os.pathsep}{environment['PATH']}",
            "FAKE_UV_LOG": str(uv_log),
            "FAKE_GIT_LOG": str(tmp_path / "git.log"),
            "FAKE_HERMES_LOG": str(hermes_log),
            "FAKE_HERMES_STATE": str(hermes_state),
            "FAKE_FACTORY_LOG": str(factory_log),
            "FAKE_READY_MARKER": str(tmp_path / "paid-smoke-ready"),
            "FAKE_RENDERER_BUILD_LOG": str(renderer_build_log),
            "FAKE_LAMA_PYTHON": "/usr/bin/true",
            "FAKE_LAMA_MODEL_SOURCE": str(fake_lama_model),
            # Simulate install and Hermes being launched by a credentialed
            # operator shell. No child process may inherit these capabilities.
            "SKIN_FACTORY_REVIEW_TOKEN": "inherited-review-authority",
            "SKIN_FACTORY_REVIEW_ACTOR": "human:inherited-operator",
            "SNAKETRON_FACTORY_OPERATOR_TOKEN": "inherited-publish-authority",
            "GEMINI_API_KEY": "inherited-provider-must-not-reach-installer-children",
            "LMSTUDIO_API_KEY": "inherited-worker-must-not-reach-installer-children",
            "SNAKETRON_FACTORY_SERVICE_TOKEN": "inherited-service-must-not-reach-installer-children",
            "CUSTOM_INHERITED_PROVIDER_CREDENTIAL": "custom-inherited-must-not-reach-children",
        }
    )

    def run(script: str, *arguments: str, cwd: Path = repo) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [str(package / "scripts" / script), *arguments],
            cwd=cwd,
            env=environment,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )

    prepared = run("install-hermes.sh", str(service_env), "every 17m")
    assert prepared.returncode == 0, prepared.stderr
    assert "Prepared Skin Factory without a live cron" in prepared.stdout
    assert not hermes_state.exists()

    copied_env = package / ".factory.env"
    hermes_env = hermes_home / ".env"
    wrapper = hermes_home / "scripts/snaketron-skin-factory.sh"
    locator = hermes_home / "scripts/snaketron-skin-factory.workdir"
    job_id = package / "var/hermes-job-id"
    lama_python = package / "var/lama-venv/bin/python"
    assert copied_env.stat().st_mode & 0o777 == 0o600
    assert json.loads(copied_env.read_text(encoding="utf-8")) == json.loads(service_env.read_text(encoding="utf-8"))
    assert hermes_env.stat().st_mode & 0o777 == 0o600
    assert hermes_env.read_text(encoding="utf-8") == "HERMES_CRON_SCRIPT_TIMEOUT=1920\n"
    timeout_pin = package / "var/hermes-script-timeout-seconds"
    assert timeout_pin.stat().st_mode & 0o777 == 0o600
    assert timeout_pin.read_text(encoding="utf-8") == "1920\n"
    assert wrapper.stat().st_mode & 0o777 == 0o700
    assert locator.stat().st_mode & 0o777 == 0o600
    assert locator.read_text(encoding="utf-8").strip() == str(repo.resolve())
    assert not job_id.exists()
    assert lama_python.is_symlink()
    installed_lama = package / "var/lama/big-lama-v0.1.0.pt"
    assert installed_lama.read_bytes() == fake_lama_model.read_bytes()
    assert installed_lama.stat().st_mode & 0o777 == 0o400
    assert renderer_build_log.read_text(encoding="utf-8").splitlines() == [
        "playwright install chromium",
        "wasm-pack build --target web --out-dir pkg --release -- --locked",
        "npm ci",
        "npm run build:prod",
    ]

    uv_calls = uv_log.read_text(encoding="utf-8").splitlines()
    assert uv_calls == [
        f"sync --project {package} --frozen --no-dev --extra production",
        (
            f"sync --project {package / 'lama'} --frozen --no-dev --no-install-project "
            "--python 3.11 --no-python-downloads"
        ),
    ]
    wrapped = subprocess.run(
        [str(wrapper)],
        cwd=hermes_home / "scripts",
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert wrapped.returncode != 0
    assert factory_log.read_text(encoding="utf-8").splitlines()[-1] == (
        f"readiness-pin --config {package / 'config/factory.yaml'} --env-file {copied_env} --check-paid-smoke --json"
    )

    refused_enable = run("install-hermes.sh", str(service_env), "every 17m", "--enable-cron")
    assert refused_enable.returncode != 0
    assert not hermes_state.exists()
    assert not job_id.exists()
    assert "task_worker_conformance" in refused_enable.stdout

    # Status/smoke/rollback are explicit operator commands rather than the
    # installed no-agent path under test, so return to an uncredentialed shell.
    environment.pop("SKIN_FACTORY_REVIEW_TOKEN")
    environment.pop("SKIN_FACTORY_REVIEW_ACTOR")
    environment.pop("SNAKETRON_FACTORY_OPERATOR_TOKEN")
    environment["FAKE_WORKER_CONFORMANCE_READY"] = "1"
    environment.pop("GEMINI_API_KEY")
    environment.pop("LMSTUDIO_API_KEY")
    environment.pop("SNAKETRON_FACTORY_SERVICE_TOKEN")
    environment.pop("CUSTOM_INHERITED_PROVIDER_CREDENTIAL")

    status = run("hermes-status.sh")
    assert status.returncode == 0, status.stderr
    assert "scheduler: healthy" in status.stdout
    smoke = run("hermes-smoke.sh")
    assert smoke.returncode == 0, smoke.stderr
    environment["FAKE_PAID_SMOKE_FAIL"] = "1"
    failed_paid_smoke = run("hermes-smoke.sh", "--run-once", cwd=repo)
    assert failed_paid_smoke.returncode != 0
    assert not Path(environment["FAKE_READY_MARKER"]).exists()
    environment.pop("FAKE_PAID_SMOKE_FAIL")
    paid_smoke = run("hermes-smoke.sh", "--run-once", cwd=repo)
    assert paid_smoke.returncode == 0, paid_smoke.stderr
    assert Path(environment["FAKE_READY_MARKER"]).is_file()

    wrapped = subprocess.run(
        [str(wrapper)],
        cwd=hermes_home / "scripts",
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert wrapped.returncode == 0, wrapped.stderr
    assert json.loads(wrapped.stdout)["advanced"][0]["to"] == "prototype_review"

    environment.pop("FAKE_WORKER_CONFORMANCE_READY")
    refused_without_worker = run("install-hermes.sh", str(service_env), "every 17m", "--enable-cron")
    assert refused_without_worker.returncode != 0
    assert "task_worker_conformance" in refused_without_worker.stdout
    assert not hermes_state.exists()
    assert not job_id.exists()
    environment["FAKE_WORKER_CONFORMANCE_READY"] = "1"

    enabled = run("install-hermes.sh", str(service_env), "every 17m", "--enable-cron")
    assert enabled.returncode == 0, enabled.stderr
    assert "Installed one behavior-gated no-agent Hermes job abc123" in enabled.stdout
    assert job_id.read_text(encoding="utf-8").strip() == "abc123"
    create_call = next(
        call for call in hermes_log.read_text(encoding="utf-8").splitlines() if call.startswith("cron create")
    )
    assert "--name snaketron-skin-factory" in create_call
    assert "--script snaketron-skin-factory.sh" in create_call
    assert "--no-agent" in create_call
    assert f"--workdir {repo}" in create_call
    hermes_calls = hermes_log.read_text(encoding="utf-8").splitlines()
    assert hermes_calls.index("gateway restart") < hermes_calls.index("gateway status")
    assert hermes_calls.index("gateway status") < hermes_calls.index(create_call)
    calls = factory_log.read_text(encoding="utf-8").splitlines()
    assert sum(call.startswith("run-once ") for call in calls) == 3
    assert any(call.startswith("doctor ") and "--identity service" in call for call in calls)
    assert any(call.startswith("status ") for call in calls)

    rolled_back = run("hermes-rollback.sh")
    assert rolled_back.returncode == 0, rolled_back.stderr
    assert "Factory data, backups, env, and virtual environments were preserved" in rolled_back.stdout
    assert not wrapper.exists()
    assert not locator.exists()
    assert not job_id.exists()
    assert not hermes_state.exists()
    assert copied_env.exists()
    assert lama_python.exists()
