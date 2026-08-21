from __future__ import annotations

import importlib.util
import json
import stat
import subprocess
import urllib.error
from pathlib import Path
from types import ModuleType

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "skin-factory" / "scripts" / "local-calibration.py"


def load_launcher() -> ModuleType:
    spec = importlib.util.spec_from_file_location("local_calibration", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def private_json(path: Path, value: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")
    path.chmod(0o600)


def test_runtime_state_is_checkout_isolated_stable_and_private(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    launcher = load_launcher()
    repository = tmp_path / "repo"
    repository.mkdir()
    state_dir = tmp_path / "var" / "local-runtime"
    monkeypatch.setattr(launcher, "REPO", repository)
    monkeypatch.setattr(launcher, "STATE_DIR", state_dir)
    monkeypatch.setattr(launcher, "STATE_FILE", state_dir / "state.json")

    first = launcher.runtime_state(71)
    second = launcher.runtime_state(None)

    assert first == second
    assert first["admin_user_ids"] == "71"
    assert first["project"].startswith("snaketron-factory-")
    assert first["dynamodb_prefix"] != "snaketron"
    assert first["texture_bucket"] != "snaketron-textures-dev"
    assert first["jwt_secret"]
    assert stat.S_IMODE((state_dir / "state.json").stat().st_mode) == 0o600
    assert stat.S_IMODE(state_dir.stat().st_mode) == 0o700

    environment = launcher.compose_environment(first)
    assert environment["SNAKETRON_HTTP_HOST_PORT"] == "18080"
    assert environment["SNAKETRON_REDIS_HOST_PORT"] == "16379"
    assert environment["SNAKETRON_LOCALSTACK_PERSISTENCE"] == "1"
    assert environment["SNAKETRON_ADMIN_USER_IDS"] == "71"


def test_wait_http_retries_task_warming_until_readiness_is_2xx(monkeypatch: pytest.MonkeyPatch) -> None:
    launcher = load_launcher()
    attempts = 0

    class ReadyResponse:
        status = 200

        def __enter__(self) -> ReadyResponse:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    def urlopen(_url: str, timeout: int) -> ReadyResponse:
        nonlocal attempts
        assert timeout == 2
        attempts += 1
        if attempts < 3:
            raise urllib.error.HTTPError(
                "http://127.0.0.1:18080/health/ready",
                503,
                "task is warming",
                {},
                None,
            )
        return ReadyResponse()

    monkeypatch.setattr(launcher.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(launcher.time, "sleep", lambda _seconds: None)

    launcher.wait_http(
        "http://127.0.0.1:18080/health/ready",
        timeout=2,
        description="restarted server",
    )

    assert attempts == 3


def test_server_recreate_waits_for_application_readiness_before_returning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    launcher = load_launcher()
    waits: list[tuple[str, float, str]] = []
    commands: list[list[str]] = []
    monkeypatch.setattr(launcher, "verify_container_ownership", lambda _state: None)
    monkeypatch.setattr(launcher, "docker_compose", lambda: ["docker", "compose"])
    monkeypatch.setattr(launcher, "compose_environment", lambda _state: {})
    monkeypatch.setattr(
        launcher.subprocess,
        "run",
        lambda command, **_kwargs: commands.append(command),
    )
    monkeypatch.setattr(
        launcher,
        "wait_http",
        lambda url, *, timeout, description: waits.append((url, timeout, description)),
    )

    launcher.recreate_server({})

    assert commands == [["docker", "compose", "up", "--detach", "--no-deps", "--force-recreate", "server"]]
    assert waits == [
        (
            "http://127.0.0.1:18080/health/ready",
            120,
            "restarted isolated Snaketron server application",
        )
    ]


def test_gallery_start_probes_unauthenticated_health_endpoint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    launcher = load_launcher()
    operator_path = tmp_path / "operator.json"
    private_json(operator_path, {launcher.OPERATOR_TOKEN: "operator-session"})
    waits: list[tuple[str, float, str]] = []
    starts: list[str] = []
    monkeypatch.setattr(launcher.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(
        launcher,
        "start_owned_process",
        lambda name, *_args, **_kwargs: starts.append(name),
    )
    monkeypatch.setattr(
        launcher,
        "wait_http",
        lambda url, *, timeout, description: waits.append((url, timeout, description)),
    )

    launcher.start_renderer_and_gallery(operator_path)

    assert starts == ["renderer", "gallery"]
    assert waits == [
        ("http://127.0.0.1:13000/", 20, "pinned renderer"),
        ("http://127.0.0.1:18765/healthz", 20, "review gallery"),
    ]


def test_gallery_fingerprint_changes_when_operator_session_refreshes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    launcher = load_launcher()
    operator_path = tmp_path / "operator.json"
    private_json(operator_path, {launcher.OPERATOR_TOKEN: "first-session"})
    fingerprints: list[tuple[str, str]] = []
    monkeypatch.setattr(launcher.shutil, "which", lambda name: f"/usr/bin/{name}")
    monkeypatch.setattr(
        launcher,
        "start_owned_process",
        lambda name, *_args, **kwargs: fingerprints.append((name, kwargs["fingerprint"])),
    )
    monkeypatch.setattr(launcher, "wait_http", lambda *_args, **_kwargs: None)

    launcher.start_renderer_and_gallery(operator_path)
    private_json(operator_path, {launcher.OPERATOR_TOKEN: "refreshed-session"})
    launcher.start_renderer_and_gallery(operator_path)

    renderer = [value for name, value in fingerprints if name == "renderer"]
    gallery = [value for name, value in fingerprints if name == "gallery"]
    assert renderer[0] == renderer[1]
    assert gallery[0] != gallery[1]


def test_stale_owned_process_fingerprint_is_restarted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    launcher = load_launcher()
    state_dir = tmp_path / "local-runtime"
    logs = state_dir / "logs"
    logs.mkdir(parents=True)
    process_file = state_dir / "processes.json"
    private_json(
        process_file,
        {
            "gallery": {
                "pid": 101,
                "needle": "factory serve",
                "port": 18765,
                "log": str(logs / "gallery.log"),
                "fingerprint": "old-source",
            }
        },
    )
    monkeypatch.setattr(launcher, "STATE_DIR", state_dir)
    monkeypatch.setattr(launcher, "PROCESS_FILE", process_file)
    running = {101: True}
    signals: list[tuple[int, int]] = []

    monkeypatch.setattr(
        launcher,
        "process_command",
        lambda pid: "factory serve --config local" if running.get(pid, False) else "",
    )
    monkeypatch.setattr(launcher, "port_is_open", lambda _port: running.get(101, False))

    def kill(pid: int, sent: int) -> None:
        signals.append((pid, sent))
        running[pid] = False

    class Child:
        pid = 202

    monkeypatch.setattr(launcher.os, "kill", kill)
    monkeypatch.setattr(launcher.subprocess, "Popen", lambda *_args, **_kwargs: Child())

    launcher.start_owned_process(
        "gallery",
        ["factory", "serve"],
        needle="factory serve",
        port=18765,
        environment={},
        fingerprint="new-source",
    )

    assert signals == [(101, launcher.signal.SIGTERM)]
    assert launcher.read_processes()["gallery"]["pid"] == 202
    assert launcher.read_processes()["gallery"]["fingerprint"] == "new-source"


def test_matching_owned_process_fingerprint_is_reused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    launcher = load_launcher()
    state_dir = tmp_path / "local-runtime"
    state_dir.mkdir()
    process_file = state_dir / "processes.json"
    private_json(
        process_file,
        {
            "gallery": {
                "pid": 101,
                "needle": "factory serve",
                "port": 18765,
                "log": str(state_dir / "gallery.log"),
                "fingerprint": "same-source",
            }
        },
    )
    monkeypatch.setattr(launcher, "STATE_DIR", state_dir)
    monkeypatch.setattr(launcher, "PROCESS_FILE", process_file)
    monkeypatch.setattr(launcher, "process_command", lambda _pid: "factory serve --config local")
    monkeypatch.setattr(launcher, "port_is_open", lambda _port: True)
    monkeypatch.setattr(
        launcher.os,
        "kill",
        lambda *_args: pytest.fail("matching process must not be stopped"),
    )
    monkeypatch.setattr(
        launcher.subprocess,
        "Popen",
        lambda *_args, **_kwargs: pytest.fail("matching process must not be restarted"),
    )

    launcher.start_owned_process(
        "gallery",
        ["factory", "serve"],
        needle="factory serve",
        port=18765,
        environment={},
        fingerprint="same-source",
    )

    assert launcher.read_processes()["gallery"]["pid"] == 101


def test_gemini_import_is_atomic_private_and_preserves_unrelated_keys(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    launcher = load_launcher()
    service = tmp_path / "private" / "service.json"
    private_json(service, {"LMSTUDIO_API_KEY": "local-worker-key"})
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-secret-never-printed")

    value = launcher.import_gemini_key(service)

    assert value == {
        "GEMINI_API_KEY": "gemini-secret-never-printed",
        "LMSTUDIO_API_KEY": "local-worker-key",
    }
    assert stat.S_IMODE(service.stat().st_mode) == 0o600
    assert "gemini-secret-never-printed" not in capsys.readouterr().out


def test_absent_service_token_runs_four_phase_local_bootstrap(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    launcher = load_launcher()
    service_path = tmp_path / "service.json"
    operator_path = tmp_path / "operator.json"
    accounts_path = tmp_path / "accounts.json"
    private_json(
        service_path,
        {
            "GEMINI_API_KEY": "key",
            launcher.SERVICE_TOKEN: "replace-via-manage-service-credential.py-provision",
        },
    )
    initial_state = {"admin_user_ids": ""}
    calls: list[tuple[str, bool]] = []
    recreated: list[dict[str, str]] = []
    probed: list[str] = []

    def run_step(command: list[str], **_kwargs: object) -> None:
        action = command[2]
        calls.append((action, "--require-admin" in command))
        if action == "bootstrap-local-accounts":
            private_json(
                accounts_path,
                {
                    launcher.LOCAL_ADMIN_USER_ID: "71",
                    launcher.LOCAL_FACTORY_USER_ID: "72",
                },
            )
            operator = launcher.read_private_json(operator_path)
            operator[launcher.OPERATOR_TOKEN] = "local-admin-jwt"
            launcher.write_private_json(operator_path, operator)
        else:
            service = launcher.read_private_json(service_path)
            service[launcher.SERVICE_TOKEN] = "snk_factory_v1." + "a" * 32 + "." + "b" * 43
            launcher.write_private_json(service_path, service)

    monkeypatch.setattr(launcher, "run_credential_step", run_step)
    monkeypatch.setattr(
        launcher,
        "runtime_state",
        lambda admin_id=None: {"admin_user_ids": "" if admin_id is None else str(admin_id)},
    )
    monkeypatch.setattr(launcher, "recreate_server", lambda state: recreated.append(state))
    monkeypatch.setattr(launcher, "probe_service_capabilities", lambda token: probed.append(token))

    state, service, operator = launcher.bootstrap_local_credentials(
        initial_state,
        accounts_path=accounts_path,
        operator_path=operator_path,
        service_path=service_path,
    )

    assert calls == [
        ("bootstrap-local-accounts", False),
        ("bootstrap-local-accounts", True),
        ("provision", False),
    ]
    assert state["admin_user_ids"] == "71"
    assert recreated == [{"admin_user_ids": "71"}]
    assert probed == [service[launcher.SERVICE_TOKEN]]
    assert operator[launcher.REVIEW_ACTOR] == "human:local-operator"
    assert operator[launcher.REVIEW_TOKEN]


def test_present_service_token_is_never_replaced(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    launcher = load_launcher()
    service_path = tmp_path / "service.json"
    operator_path = tmp_path / "operator.json"
    accounts_path = tmp_path / "accounts.json"
    token = "snk_factory_v1." + "a" * 32 + "." + "b" * 43
    private_json(service_path, {"GEMINI_API_KEY": "key", launcher.SERVICE_TOKEN: token})
    private_json(
        operator_path,
        {
            launcher.OPERATOR_TOKEN: "operator",
            launcher.REVIEW_TOKEN: "review",
            launcher.REVIEW_ACTOR: "human:operator",
        },
    )
    monkeypatch.setattr(
        launcher,
        "run_credential_step",
        lambda *_args, **_kwargs: pytest.fail("existing token must not be replaced"),
    )
    observed: list[str] = []
    monkeypatch.setattr(launcher, "probe_service_capabilities", lambda value: observed.append(value))

    launcher.bootstrap_local_credentials(
        {},
        accounts_path=accounts_path,
        operator_path=operator_path,
        service_path=service_path,
    )

    assert observed == [token]


def test_present_local_token_refreshes_operator_jwt_without_reprovision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    launcher = load_launcher()
    service_path = tmp_path / "service.json"
    operator_path = tmp_path / "operator.json"
    accounts_path = tmp_path / "accounts.json"
    token = "snk_factory_v1." + "a" * 32 + "." + "b" * 43
    private_json(service_path, {"GEMINI_API_KEY": "key", launcher.SERVICE_TOKEN: token})
    private_json(
        operator_path,
        {
            launcher.OPERATOR_TOKEN: "expired-jwt",
            launcher.REVIEW_TOKEN: "review-token-long-enough",
            launcher.REVIEW_ACTOR: "human:operator",
        },
    )
    private_json(
        accounts_path,
        {launcher.LOCAL_ADMIN_USER_ID: "71", launcher.LOCAL_FACTORY_USER_ID: "72"},
    )
    calls: list[list[str]] = []
    monkeypatch.setattr(
        launcher,
        "run_credential_step",
        lambda command, **_kwargs: calls.append(command),
    )
    monkeypatch.setattr(launcher, "probe_service_capabilities", lambda _token: None)

    launcher.bootstrap_local_credentials(
        {"admin_user_ids": "71"},
        accounts_path=accounts_path,
        operator_path=operator_path,
        service_path=service_path,
    )

    assert len(calls) == 1
    assert calls[0][2] == "bootstrap-local-accounts"
    assert "--require-admin" in calls[0]
    assert "provision" not in calls[0]
    assert launcher.read_private_json(service_path)[launcher.SERVICE_TOKEN] == token


def test_known_operator_placeholders_are_replaced_privately(tmp_path: Path) -> None:
    launcher = load_launcher()
    operator_path = tmp_path / "operator.json"
    private_json(
        operator_path,
        {
            launcher.REVIEW_TOKEN: "replace-with-a-long-random-review-secret",
            launcher.REVIEW_ACTOR: "human:operator-name",
        },
    )

    operator = launcher.ensure_operator_identity(operator_path)

    assert not operator[launcher.REVIEW_TOKEN].startswith("replace-")
    assert operator[launcher.REVIEW_ACTOR] == "human:local-operator"
    assert stat.S_IMODE(operator_path.stat().st_mode) == 0o600


def test_capability_probe_installs_a_no_redirect_handler(monkeypatch: pytest.MonkeyPatch) -> None:
    launcher = load_launcher()
    observed: list[object] = []

    class RefusingOpener:
        def open(self, _request: object, timeout: int) -> None:
            assert timeout == 10
            raise urllib.error.HTTPError(
                "http://127.0.0.1:1/capture",
                302,
                "redirect refused",
                {},
                None,
            )

    def opener(handler: object) -> RefusingOpener:
        observed.append(handler)
        return RefusingOpener()

    monkeypatch.setattr(launcher.urllib.request, "build_opener", opener)
    with pytest.raises(launcher.SetupRequired, match="not valid"):
        launcher.probe_service_capabilities("secret-never-forwarded")
    assert len(observed) == 1
    assert isinstance(observed[0], launcher.NoRedirect)


def test_capability_probe_rejects_admin_or_publication_authority() -> None:
    launcher = load_launcher()
    base = {
        "schemaVersion": 1,
        "identity": {"registeredAccount": True, "isGuest": False, "isAdmin": False},
        "credential": {"credentialType": "factoryService", "revocable": True, "expiresAt": None},
        "capabilities": {
            "createPrivateSkins": True,
            "createEvaluationSkins": True,
            "uploadPrivateForgeTextures": True,
            "requestPublicationReview": True,
            "publishSkins": False,
            "administerSkins": False,
        },
    }
    launcher.validate_capability_envelope(base)

    base["identity"]["isAdmin"] = True
    with pytest.raises(launcher.SetupRequired, match="non-admin"):
        launcher.validate_capability_envelope(base)
    base["identity"]["isAdmin"] = False
    base["capabilities"]["publishSkins"] = True
    with pytest.raises(launcher.SetupRequired, match="least-privilege"):
        launcher.validate_capability_envelope(base)


def test_runtime_config_uses_isolated_mutable_state_and_exact_worker_model(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    launcher = load_launcher()
    runtime = tmp_path / "var" / "local-runtime.yaml"
    monkeypatch.setattr(launcher, "RUNTIME_CONFIG", runtime)

    launcher.write_runtime_config("publisher/exact-worker-id")

    value = yaml.safe_load(runtime.read_text(encoding="utf-8"))
    assert value["service"]["base_url"] == "http://127.0.0.1:18080"
    assert value["browser"]["base_url"] == "http://127.0.0.1:13000"
    assert value["review"]["port"] == 18765
    assert value["models"]["task_worker"]["model"] == "publisher/exact-worker-id"
    assert value["paths"]["data_dir"] == "var/local-runtime/factory-data"
    assert value["paths"]["database"] == "var/local-runtime/factory-data/factory.sqlite3"
    assert value["paths"]["objects"] == "var/local-runtime/factory-data/objects"
    assert value["paths"]["lama_model"] == "var/lama/big-lama-v0.1.0.pt"
    assert stat.S_IMODE(runtime.stat().st_mode) == 0o600


def test_first_paid_cycle_requires_prototype_review_before_recording(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    launcher = load_launcher()
    calls: list[tuple[str, ...]] = []
    monkeypatch.setattr(launcher, "has_current_paid_smoke", lambda _path: False)

    def command(_path: Path, *arguments: str, capture: bool = False) -> subprocess.CompletedProcess[str]:
        calls.append(arguments)
        payload = {"advanced": [{"to": "prototype_review"}], "halt": None} if capture else {}
        return subprocess.CompletedProcess([], 0, json.dumps(payload))

    monkeypatch.setattr(launcher, "factory_command", command)
    launcher.run_paid_cycle(Path("service.json"))

    assert calls == [("run-once",), ("readiness-pin", "--record-paid-smoke")]


def test_stale_paid_marker_is_not_blessed_by_build_stage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    launcher = load_launcher()
    monkeypatch.setattr(launcher, "has_current_paid_smoke", lambda _path: False)
    monkeypatch.setattr(
        launcher,
        "factory_command",
        lambda *_args, **_kwargs: subprocess.CompletedProcess(
            [], 0, json.dumps({"advanced": [{"to": "final_review"}], "halt": None})
        ),
    )

    with pytest.raises(RuntimeError, match="prototype_review"):
        launcher.run_paid_cycle(Path("service.json"))


def test_compose_and_localstack_enable_private_texture_storage() -> None:
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    server_environment = compose["services"]["server"]["environment"]
    localstack_environment = compose["services"]["localstack"]["environment"]
    initializer = (ROOT / "localstack-init.sh").read_text(encoding="utf-8")

    assert "SNAKETRON_TEXTURE_S3_BUCKET" in server_environment
    assert server_environment["SNAKETRON_TEXTURE_S3_FORCE_PATH_STYLE"] == "true"
    assert any("SNAKETRON_TEXTURE_S3_BUCKET" in item for item in localstack_environment)
    assert any("SNAKETRON_LOCALSTACK_PERSISTENCE" in item for item in localstack_environment)
    assert "put-public-access-block" in initializer
    assert 'TEXTURE_BUCKET="${SNAKETRON_TEXTURE_S3_BUCKET' in initializer


def test_installer_accepts_an_explicit_nonsecret_runtime_config() -> None:
    installer = (ROOT / "skin-factory" / "scripts" / "install-hermes.sh").read_text(encoding="utf-8")
    assert 'factory_config="${SKIN_FACTORY_INSTALL_CONFIG:-$package/config/factory.yaml}"' in installer
    assert installer.count('--config "$factory_config"') == 3
