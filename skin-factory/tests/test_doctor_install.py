from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from test_gallery_cli import seeded_factory

from snaketron_factory.config import ModelRole, load_config
from snaketron_factory.db import Database
from snaketron_factory.doctor import FactoryDoctor, _safe_error
from snaketron_factory.domain import DoctorCheck
from snaketron_factory.promotion import GitPromoter, _run


def _write_executable(path: Path, source: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")
    path.chmod(0o700)


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
    assert "credential:SKIN_FACTORY_REVIEW_TOKEN" not in names
    assert "credential:SNAKETRON_FACTORY_OPERATOR_TOKEN" not in names
    assert "credential:SNAKETRON_FACTORY_SERVICE_TOKEN" in names


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
    assert 'run --project "$package" --frozen --no-dev playwright install chromium' in installer
    assert 'UV_PROJECT_ENVIRONMENT="$lama_dir" "$uv_bin" sync' in installer
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
    assert '--script "$installed"' in installer
    assert "--no-agent" in installer
    assert '--workdir "$repo_root"' in installer
    assert "forbidden" in installer
    assert "SKIN_FACTORY_REVIEW_TOKEN" in installer
    assert "SNAKETRON_FACTORY_OPERATOR_TOKEN" in installer
    assert "source " not in wrapper
    assert "eval " not in wrapper
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
  ln -s "$FAKE_LAMA_PYTHON" "$UV_PROJECT_ENVIRONMENT/bin/python"
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
printf '%s\n' '{"ok":true}'
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

    installed = run("install-hermes.sh", str(service_env), "every 17m")
    assert installed.returncode == 0, installed.stderr
    assert "Installed one no-agent Hermes job abc123" in installed.stdout

    copied_env = package / ".factory.env"
    wrapper = hermes_home / "scripts/snaketron-skin-factory.sh"
    job_id = package / "var/hermes-job-id"
    lama_python = package / "var/lama-venv/bin/python"
    assert copied_env.stat().st_mode & 0o777 == 0o600
    assert json.loads(copied_env.read_text(encoding="utf-8")) == json.loads(service_env.read_text(encoding="utf-8"))
    assert wrapper.stat().st_mode & 0o777 == 0o700
    assert job_id.read_text(encoding="utf-8").strip() == "abc123"
    assert lama_python.is_symlink()
    installed_lama = package / "var/lama/big-lama-v0.1.0.pt"
    assert installed_lama.read_bytes() == fake_lama_model.read_bytes()
    assert installed_lama.stat().st_mode & 0o777 == 0o400
    assert renderer_build_log.read_text(encoding="utf-8").splitlines() == [
        "wasm-pack build --target web --out-dir pkg --release -- --locked",
        "npm ci",
        "npm run build:prod",
    ]

    uv_calls = uv_log.read_text(encoding="utf-8").splitlines()
    assert uv_calls == [
        f"sync --project {package} --frozen --no-dev --extra production",
        f"run --project {package} --frozen --no-dev playwright install chromium",
        (
            f"sync --project {package / 'lama'} --frozen --no-dev --no-install-project "
            "--python 3.11 --no-python-downloads"
        ),
    ]
    create_call = next(
        call for call in hermes_log.read_text(encoding="utf-8").splitlines() if call.startswith("cron create")
    )
    assert "--name snaketron-skin-factory" in create_call
    assert f"--script {wrapper}" in create_call
    assert "--no-agent" in create_call
    assert f"--workdir {repo}" in create_call

    wrapped = subprocess.run(
        [str(wrapper)],
        cwd=repo,
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert wrapped.returncode == 0, wrapped.stderr
    assert factory_log.read_text(encoding="utf-8").splitlines()[-1] == (
        f"run-once --config {package / 'config/factory.yaml'} --env-file {copied_env} --json"
    )

    # Status/smoke/rollback are explicit operator commands rather than the
    # installed no-agent path under test, so return to an uncredentialed shell.
    environment.pop("SKIN_FACTORY_REVIEW_TOKEN")
    environment.pop("SKIN_FACTORY_REVIEW_ACTOR")
    environment.pop("SNAKETRON_FACTORY_OPERATOR_TOKEN")
    environment.pop("GEMINI_API_KEY")
    environment.pop("LMSTUDIO_API_KEY")
    environment.pop("SNAKETRON_FACTORY_SERVICE_TOKEN")
    environment.pop("CUSTOM_INHERITED_PROVIDER_CREDENTIAL")

    status = run("hermes-status.sh")
    assert status.returncode == 0, status.stderr
    assert "scheduler: healthy" in status.stdout
    smoke = run("hermes-smoke.sh")
    assert smoke.returncode == 0, smoke.stderr
    paid_smoke = run("hermes-smoke.sh", "--run-once", cwd=repo)
    assert paid_smoke.returncode == 0, paid_smoke.stderr
    calls = factory_log.read_text(encoding="utf-8").splitlines()
    assert sum(call.startswith("run-once ") for call in calls) == 2
    assert any(call.startswith("doctor ") and "--identity service" in call for call in calls)
    assert any(call.startswith("status ") for call in calls)

    rolled_back = run("hermes-rollback.sh")
    assert rolled_back.returncode == 0, rolled_back.stderr
    assert "Factory data, backups, env, and virtual environments were preserved" in rolled_back.stdout
    assert not wrapper.exists()
    assert not job_id.exists()
    assert not hermes_state.exists()
    assert copied_env.exists()
    assert lama_python.exists()
