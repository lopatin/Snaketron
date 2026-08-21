#!/usr/bin/env python3
"""Safely prepare and launch one checkout-local real Skin Factory runtime.

``start`` performs only non-generation readiness checks. ``run`` (and the
``--generate`` alias) is the explicit authorization for one paid factory tick.
Operational state is retained under the ignored ``skin-factory/var`` tree.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import http.client
import json
import os
import re
import secrets
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

SCRIPT = Path(__file__).resolve()
PACKAGE = SCRIPT.parent.parent
REPO = PACKAGE.parent
COMPOSE_FILE = REPO / "docker-compose.yml"
STATE_DIR = PACKAGE / "var" / "local-runtime"
STATE_FILE = STATE_DIR / "state.json"
PROCESS_FILE = STATE_DIR / "processes.json"
RUNTIME_CONFIG = PACKAGE / "var" / "local-runtime.yaml"
DEFAULT_SERVICE_ENV = Path.home() / ".config" / "snaketron-skin-factory" / "service.json"
DEFAULT_OPERATOR_ENV = Path.home() / ".config" / "snaketron-skin-factory" / "operator.json"
DEFAULT_ACCOUNTS_ENV = Path.home() / ".config" / "snaketron-skin-factory" / "local-accounts.json"

SERVICE_TOKEN = "SNAKETRON_FACTORY_SERVICE_TOKEN"
OPERATOR_TOKEN = "SNAKETRON_FACTORY_OPERATOR_TOKEN"
REVIEW_TOKEN = "SKIN_FACTORY_REVIEW_TOKEN"
REVIEW_ACTOR = "SKIN_FACTORY_REVIEW_ACTOR"
SERVICE_SECRET_NAMES = {
    "GEMINI_API_KEY",
    "LMSTUDIO_API_KEY",
    SERVICE_TOKEN,
    "SKIN_FACTORY_OUTBOX_WEBHOOK_URL",
    "SKIN_FACTORY_OUTBOX_WEBHOOK_TOKEN",
}
HUMAN_SECRET_NAMES = {OPERATOR_TOKEN, REVIEW_TOKEN, REVIEW_ACTOR}
FACTORY_TOKEN = re.compile(r"^snk_factory_v1\.[0-9a-f]{32}\.[A-Za-z0-9_-]{43}$")
LOCAL_ADMIN_USER_ID = "SNAKETRON_LOCAL_ADMIN_USER_ID"
LOCAL_FACTORY_USER_ID = "SNAKETRON_LOCAL_FACTORY_USER_ID"

HTTP_PORT = 18080
GRPC_PORT = 15051
LOCALSTACK_PORT = 14566
REDIS_PORT = 16379
RENDERER_PORT = 13000
GALLERY_PORT = 18765
WORKER_ENDPOINT = "http://127.0.0.1:1234/v1"


class SetupRequired(RuntimeError):
    """The isolated infrastructure is usable but operator setup is incomplete."""


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """Never forward a local bearer credential to a redirect target."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(newurl, code, "credential API redirects are refused", headers, fp)


def say(message: str) -> None:
    print(message, flush=True)


def ensure_private_directory(path: Path) -> None:
    if path.is_symlink():
        raise RuntimeError(f"private directory cannot be a symlink: {path}")
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(path, 0o700)


def read_private_json(path: Path, *, missing_ok: bool = False) -> dict[str, str]:
    if missing_ok and not path.exists():
        return {}
    details = path.lstat()
    if not stat.S_ISREG(details.st_mode) or details.st_mode & 0o077:
        raise RuntimeError(f"{path} must be a regular owner-private (0600) JSON file")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or not all(
        isinstance(name, str) and isinstance(item, str) for name, item in value.items()
    ):
        raise RuntimeError(f"{path} must contain one JSON object of string values")
    return value


def write_private_json(path: Path, value: dict[str, Any]) -> None:
    ensure_private_directory(path.parent)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
        os.chmod(path, 0o600)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary_name)
        raise


def checkout_key() -> str:
    return hashlib.sha256(str(REPO.resolve()).encode()).hexdigest()[:10]


def runtime_state(admin_user_id: int | None = None) -> dict[str, str]:
    ensure_private_directory(STATE_DIR)
    existing = read_private_json(STATE_FILE, missing_ok=True)
    key = checkout_key()
    if existing and existing.get("checkout") != str(REPO.resolve()):
        raise RuntimeError(f"refusing runtime state owned by another checkout: {STATE_FILE}")
    state = {
        "checkout": str(REPO.resolve()),
        "key": key,
        "project": f"snaketron-factory-{key}",
        "localstack_container": f"snaketron-factory-{key}-localstack",
        "redis_container": f"snaketron-factory-{key}-valkey",
        "server_container": f"snaketron-factory-{key}-server",
        "dynamodb_prefix": f"snaketron-factory-{key}",
        "replay_bucket": f"snaketron-factory-replays-{key}",
        "texture_bucket": f"snaketron-factory-textures-{key}",
        "jwt_secret": existing.get("jwt_secret") or secrets.token_urlsafe(48),
        "admin_user_ids": existing.get("admin_user_ids", ""),
    }
    if admin_user_id is not None:
        if admin_user_id <= 0:
            raise RuntimeError("--admin-user-id must be positive")
        state["admin_user_ids"] = str(admin_user_id)
    write_private_json(STATE_FILE, state)
    for path in (STATE_DIR / "localstack-data", STATE_DIR / "replays", STATE_DIR / "logs"):
        ensure_private_directory(path)
    return state


def scrubbed_environment(*, include_human: bool = False) -> dict[str, str]:
    environment = os.environ.copy()
    for name in SERVICE_SECRET_NAMES | HUMAN_SECRET_NAMES:
        environment.pop(name, None)
    if include_human:
        # Human authority is still loaded only from the explicit JSON file.
        for name in SERVICE_SECRET_NAMES:
            environment.pop(name, None)
    return environment


def compose_environment(state: dict[str, str]) -> dict[str, str]:
    environment = scrubbed_environment()
    environment.update(
        {
            "COMPOSE_PROJECT_NAME": state["project"],
            "SNAKETRON_LOCALSTACK_CONTAINER_NAME": state["localstack_container"],
            "SNAKETRON_REDIS_CONTAINER_NAME": state["redis_container"],
            "SNAKETRON_SERVER_CONTAINER_NAME": state["server_container"],
            "SNAKETRON_LOCALSTACK_HOST_PORT": str(LOCALSTACK_PORT),
            "SNAKETRON_REDIS_HOST_PORT": str(REDIS_PORT),
            "SNAKETRON_HTTP_HOST_PORT": str(HTTP_PORT),
            "SNAKETRON_GRPC_HOST_PORT": str(GRPC_PORT),
            "SNAKETRON_LOCALSTACK_DATA_DIR": str(STATE_DIR / "localstack-data"),
            "SNAKETRON_REPLAY_DATA_DIR": str(STATE_DIR / "replays"),
            "SNAKETRON_DYNAMODB_TABLE_PREFIX": state["dynamodb_prefix"],
            "SNAKETRON_REPLAY_S3_BUCKET": state["replay_bucket"],
            "SNAKETRON_REPLAY_S3_PREFIX": "recordings",
            "SNAKETRON_TEXTURE_S3_BUCKET": state["texture_bucket"],
            "SNAKETRON_TEXTURE_S3_PREFIX": "textures",
            "SNAKETRON_JWT_SECRET": state["jwt_secret"],
            "SNAKETRON_ADMIN_USER_IDS": state["admin_user_ids"],
            "SNAKETRON_LOCALSTACK_PERSISTENCE": "1",
        }
    )
    return environment


def docker_compose() -> list[str]:
    docker = shutil.which("docker")
    if not docker:
        raise RuntimeError("Docker Desktop with the `docker compose` plugin is required")
    result = subprocess.run(
        [docker, "compose", "version"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if result.returncode != 0:
        raise RuntimeError("the `docker compose` plugin is required")
    return [
        docker,
        "compose",
        "--project-name",
        f"snaketron-factory-{checkout_key()}",
        "--project-directory",
        str(REPO),
        "--file",
        str(COMPOSE_FILE),
    ]


def container_details(name: str) -> dict[str, Any] | None:
    docker = shutil.which("docker")
    if not docker:
        return None
    result = subprocess.run(
        [docker, "container", "inspect", name],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    if result.returncode != 0:
        return None
    value = json.loads(result.stdout)
    return value[0] if isinstance(value, list) and len(value) == 1 else None


def verify_container_ownership(state: dict[str, str]) -> None:
    for key in ("localstack_container", "redis_container", "server_container"):
        name = state[key]
        details = container_details(name)
        if details is None:
            continue
        labels = details.get("Config", {}).get("Labels", {}) or {}
        if labels.get("com.docker.compose.project") != state["project"]:
            raise RuntimeError(f"refusing container owned by another Compose project: {name}")
        working_dir = labels.get("com.docker.compose.project.working_dir")
        if working_dir and Path(working_dir).resolve() != REPO.resolve():
            raise RuntimeError(f"refusing container owned by another checkout: {name}")


def wait_http(url: str, *, timeout: float, description: str) -> None:
    deadline = time.monotonic() + timeout
    last_error = "not reachable"
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if 200 <= response.status < 300:
                    return
        except (OSError, urllib.error.URLError, http.client.HTTPException) as error:
            last_error = str(error)
        time.sleep(0.5)
    raise RuntimeError(f"{description} did not become ready: {last_error}")


def start_infrastructure(state: dict[str, str]) -> None:
    verify_container_ownership(state)
    say("Building and starting this checkout's isolated Snaketron, LocalStack, and Valkey...")
    command = [*docker_compose(), "up", "--detach", "--build", "localstack", "redis", "server"]
    subprocess.run(command, check=True, cwd=REPO, env=compose_environment(state))
    verify_container_ownership(state)
    wait_http(
        f"http://127.0.0.1:{HTTP_PORT}/health/ready",
        timeout=180,
        description="isolated Snaketron server application",
    )
    docker = shutil.which("docker")
    assert docker is not None
    for bucket in (state["replay_bucket"], state["texture_bucket"]):
        subprocess.run(
            [
                docker,
                "exec",
                state["localstack_container"],
                "awslocal",
                "s3api",
                "head-bucket",
                "--bucket",
                bucket,
            ],
            check=True,
            stdout=subprocess.DEVNULL,
        )
        subprocess.run(
            [
                docker,
                "exec",
                state["localstack_container"],
                "awslocal",
                "s3api",
                "get-public-access-block",
                "--bucket",
                bucket,
            ],
            check=True,
            stdout=subprocess.DEVNULL,
        )


def recreate_server(state: dict[str, str]) -> None:
    """Recreate only the isolated server after persisting its admin allowlist."""

    verify_container_ownership(state)
    say("Restarting only the isolated Snaketron server with the durable local admin id...")
    subprocess.run(
        [
            *docker_compose(),
            "up",
            "--detach",
            "--no-deps",
            "--force-recreate",
            "server",
        ],
        check=True,
        cwd=REPO,
        env=compose_environment(state),
    )
    verify_container_ownership(state)
    wait_http(
        f"http://127.0.0.1:{HTTP_PORT}/health/ready",
        timeout=120,
        description="restarted isolated Snaketron server application",
    )


def import_gemini_key(service_path: Path) -> dict[str, str]:
    service = read_private_json(service_path, missing_ok=True)
    current = service.get("GEMINI_API_KEY", "")
    placeholder = current.startswith("replace-")
    inherited = os.environ.get("GEMINI_API_KEY", "")
    if not current or placeholder:
        if not inherited:
            raise SetupRequired(
                "GEMINI_API_KEY is absent from both the inherited shell and the private service JSON. "
                "Export/source it in the invoking shell, then rerun; the launcher never prints it."
            )
        service["GEMINI_API_KEY"] = inherited
        write_private_json(service_path, service)
        say(f"Imported GEMINI_API_KEY into owner-private JSON: {service_path}")
    return service


def ensure_operator_identity(operator_path: Path) -> dict[str, str]:
    operator = read_private_json(operator_path, missing_ok=True)
    changed = False
    review_token = operator.get(REVIEW_TOKEN, "")
    if not review_token or review_token.startswith("replace-"):
        operator[REVIEW_TOKEN] = secrets.token_urlsafe(32)
        changed = True
    review_actor = operator.get(REVIEW_ACTOR, "")
    if not review_actor or review_actor == "human:operator-name" or review_actor.startswith("replace-"):
        operator[REVIEW_ACTOR] = "human:local-operator"
        changed = True
    if changed:
        write_private_json(operator_path, operator)
    return operator


def credential_command(
    action: str,
    *,
    accounts_path: Path,
    operator_path: Path,
    service_path: Path | None = None,
    user_id: int | None = None,
    require_admin: bool = False,
) -> list[str]:
    command = [
        sys.executable,
        str(PACKAGE / "scripts" / "manage-service-credential.py"),
        action,
        "--base-url",
        f"http://127.0.0.1:{HTTP_PORT}",
        "--operator-env",
        str(operator_path),
    ]
    if action == "bootstrap-local-accounts":
        command.extend(["--accounts-env", str(accounts_path)])
        if require_admin:
            command.append("--require-admin")
    else:
        assert service_path is not None
        command.extend(["--service-env", str(service_path)])
        if user_id is not None:
            command.extend(["--user-id", str(user_id)])
    return command


def manual_credential_recovery(accounts_path: Path, operator_path: Path, service_path: Path) -> str:
    helper = PACKAGE / "scripts" / "manage-service-credential.py"
    base = f"python3 {helper} bootstrap-local-accounts --base-url http://127.0.0.1:{HTTP_PORT}"
    bootstrap = f"{base} --accounts-env {accounts_path} --operator-env {operator_path}"
    provision = (
        f"python3 {helper} provision --base-url http://127.0.0.1:{HTTP_PORT} "
        f"--operator-env {operator_path} --service-env {service_path} --user-id <factory-user-id>"
    )
    return (
        "Manual recovery (secrets remain in owner-private JSON):\n"
        f"  1. {bootstrap}\n"
        f"  2. {SCRIPT} start --admin-user-id <admin-user-id> --accounts-env {accounts_path} "
        f"--operator-env {operator_path} --service-env {service_path}\n"
        f"  3. {bootstrap} --require-admin\n"
        f"  4. {provision}"
    )


def run_credential_step(command: list[str], *, recovery: str, description: str) -> None:
    try:
        subprocess.run(command, check=True, cwd=PACKAGE, env=scrubbed_environment())
    except subprocess.CalledProcessError as error:
        raise SetupRequired(f"{description} failed.\n{recovery}") from error


def validate_capability_envelope(value: dict[str, Any]) -> None:
    identity = value.get("identity") if isinstance(value, dict) else None
    credential = value.get("credential") if isinstance(value, dict) else None
    capabilities = value.get("capabilities") if isinstance(value, dict) else None
    if (
        value.get("schemaVersion") != 1
        or not isinstance(identity, dict)
        or not isinstance(credential, dict)
        or not isinstance(capabilities, dict)
    ):
        raise SetupRequired("Snaketron returned an unsupported factory capability envelope")
    required_identity = {"registeredAccount": True, "isGuest": False, "isAdmin": False}
    required_credential = {"credentialType": "factoryService", "revocable": True, "expiresAt": None}
    required_capabilities = {
        "createPrivateSkins": True,
        "createEvaluationSkins": True,
        "uploadPrivateForgeTextures": True,
        "requestPublicationReview": True,
        "publishSkins": False,
        "administerSkins": False,
    }
    if any(identity.get(name) is not expected for name, expected in required_identity.items()):
        raise SetupRequired("factory service identity is not a registered non-admin account")
    if any(credential.get(name) != expected for name, expected in required_credential.items()):
        raise SetupRequired("factory service credential is not durable and revocable")
    if any(capabilities.get(name) is not expected for name, expected in required_capabilities.items()):
        raise SetupRequired("factory service capability envelope is not least-privilege or texture-ready")


def probe_service_capabilities(token: str) -> None:
    request = urllib.request.Request(
        f"http://127.0.0.1:{HTTP_PORT}/api/factory/capabilities",
        headers={"Accept": "application/json", "Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.build_opener(NoRedirect()).open(request, timeout=10) as response:
            value = json.load(response)
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as error:
        raise SetupRequired("the factory service credential is not valid on the isolated server") from error
    validate_capability_envelope(value)


def bootstrap_local_credentials(
    state: dict[str, str],
    *,
    accounts_path: Path,
    operator_path: Path,
    service_path: Path,
) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    service = import_gemini_key(service_path)
    ensure_operator_identity(operator_path)
    recovery = manual_credential_recovery(accounts_path, operator_path, service_path)

    # Never replace a syntactically present token, even if it belongs to
    # another server. Only the checked-in replace-* placeholder is considered
    # absent; every other malformed value remains visible for manual recovery.
    present_token = service.get(SERVICE_TOKEN, "")
    token_is_placeholder = present_token.startswith("replace-")
    if not present_token or token_is_placeholder:
        run_credential_step(
            credential_command(
                "bootstrap-local-accounts",
                accounts_path=accounts_path,
                operator_path=operator_path,
            ),
            recovery=recovery,
            description="local account bootstrap",
        )
        accounts = read_private_json(accounts_path)
        try:
            admin_id = int(accounts[LOCAL_ADMIN_USER_ID])
            factory_id = int(accounts[LOCAL_FACTORY_USER_ID])
        except (KeyError, ValueError) as error:
            raise SetupRequired(f"local account IDs were not retained.\n{recovery}") from error
        state = runtime_state(admin_id)
        recreate_server(state)
        run_credential_step(
            credential_command(
                "bootstrap-local-accounts",
                accounts_path=accounts_path,
                operator_path=operator_path,
                require_admin=True,
            ),
            recovery=recovery,
            description="local administrator confirmation",
        )
        run_credential_step(
            credential_command(
                "provision",
                accounts_path=accounts_path,
                operator_path=operator_path,
                service_path=service_path,
                user_id=factory_id,
            ),
            recovery=recovery,
            description="factory credential provisioning",
        )
        service = read_private_json(service_path)

    # Login JWTs expire even though the dedicated service credential does not.
    # A later local start refreshes only human authority and re-proves that the
    # factory account is still non-admin; it never rotates/replaces the service
    # token automatically.
    elif state.get("admin_user_ids") and accounts_path.exists():
        run_credential_step(
            credential_command(
                "bootstrap-local-accounts",
                accounts_path=accounts_path,
                operator_path=operator_path,
                require_admin=True,
            ),
            recovery=recovery,
            description="local administrator JWT refresh",
        )

    token = service.get(SERVICE_TOKEN, "")
    if FACTORY_TOKEN.fullmatch(token) is None:
        raise SetupRequired(f"{SERVICE_TOKEN} is present but malformed; it was not replaced.\n{recovery}")
    operator = read_private_json(operator_path)
    missing = [name for name in (OPERATOR_TOKEN, REVIEW_TOKEN, REVIEW_ACTOR) if not operator.get(name)]
    if missing:
        raise SetupRequired(f"operator JSON is missing {', '.join(missing)}: {operator_path}\n{recovery}")
    if not operator[REVIEW_ACTOR].startswith("human:"):
        raise SetupRequired(f"{REVIEW_ACTOR} must be a nonempty human: identity in {operator_path}")
    probe_service_capabilities(token)
    return state, service, operator


def run_installer(service_path: Path) -> None:
    environment = scrubbed_environment()
    environment["HERMES_HOME"] = str(STATE_DIR / "hermes-home")
    environment["SKIN_FACTORY_INSTALL_CONFIG"] = str(RUNTIME_CONFIG)
    environment["SKIN_FACTORY_INSTALL_ENVIRONMENT"] = str(STATE_DIR / "factory.env")
    environment["SKIN_FACTORY_INSTALL_STATE_DIR"] = str(STATE_DIR)
    say("Preparing frozen Python, renderer, browser, and LaMa dependencies (no cron and no generation)...")
    subprocess.run(
        [str(PACKAGE / "scripts" / "install-hermes.sh"), str(service_path.resolve()), "every 6h"],
        check=True,
        cwd=PACKAGE,
        env=environment,
    )


def prepare_factory_environment() -> None:
    uv = shutil.which("uv")
    if not uv:
        raise RuntimeError("uv is required to prepare the Skin Factory")
    say("Synchronizing the frozen Skin Factory environment...")
    subprocess.run(
        [uv, "sync", "--project", str(PACKAGE), "--frozen", "--no-dev", "--extra", "production"],
        check=True,
        cwd=PACKAGE,
        env=scrubbed_environment(),
    )


def detect_worker_model(service: dict[str, str], requested: str | None) -> str:
    headers = {"Accept": "application/json"}
    if service.get("LMSTUDIO_API_KEY"):
        headers["Authorization"] = f"Bearer {service['LMSTUDIO_API_KEY']}"
    request = urllib.request.Request(f"{WORKER_ENDPOINT}/models", headers=headers)
    try:
        with urllib.request.build_opener(NoRedirect()).open(request, timeout=10) as response:
            payload = json.load(response)
    except (OSError, urllib.error.URLError, json.JSONDecodeError) as error:
        raise SetupRequired(
            f"LM Studio is not ready at {WORKER_ENDPOINT}. Load the task-worker model and start its local server."
        ) from error
    entries = payload.get("data") if isinstance(payload, dict) else None
    identifiers = [item.get("id") for item in entries or [] if isinstance(item, dict)]
    identifiers = [item for item in identifiers if isinstance(item, str) and item]
    if requested:
        if requested not in identifiers:
            raise SetupRequired(f"--worker-model {requested!r} is not advertised by LM Studio: {identifiers}")
        return requested
    preferred = "qwen/qwen3.8-27b"
    if preferred in identifiers:
        return preferred
    suffix_matches = [item for item in identifiers if item.rsplit("/", 1)[-1] == "qwen3.8-27b"]
    if len(suffix_matches) == 1:
        return suffix_matches[0]
    if len(identifiers) == 1:
        return identifiers[0]
    raise SetupRequired(
        "LM Studio must advertise one unambiguous task-worker model; choose one with --worker-model. "
        f"Advertised model ids: {identifiers}"
    )


def write_runtime_config(worker_model: str) -> None:
    python = PACKAGE / ".venv" / "bin" / "python"
    if not python.exists():
        raise RuntimeError("factory virtual environment is missing after installation")
    helper = r"""
import os, pathlib, sys, tempfile, yaml
source = pathlib.Path(sys.argv[1])
target = pathlib.Path(sys.argv[2])
model = sys.argv[3]
value = yaml.safe_load(source.read_text(encoding="utf-8"))
value["service"]["base_url"] = "http://127.0.0.1:18080"
value["browser"]["base_url"] = "http://127.0.0.1:13000"
value["review"]["bind"] = "127.0.0.1"
value["review"]["port"] = 18765
value["models"]["task_worker"]["model"] = model
value["paths"]["data_dir"] = "var/local-runtime/factory-data"
value["paths"]["database"] = "var/local-runtime/factory-data/factory.sqlite3"
value["paths"]["objects"] = "var/local-runtime/factory-data/objects"
# Keep the large frozen local model shared with the install while all mutable
# factory truth remains isolated from another checkout/runtime.
value["paths"]["lama_manifest"] = "lama/manifest.json"
value["paths"]["lama_model"] = "var/lama/big-lama-v0.1.0.pt"
value["paths"]["lama_python"] = "var/lama-venv/bin/python"
target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
descriptor, temporary = tempfile.mkstemp(prefix=".local-runtime.", dir=target.parent)
try:
    os.fchmod(descriptor, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        yaml.safe_dump(value, handle, sort_keys=False)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, target)
    os.chmod(target, 0o600)
finally:
    if os.path.exists(temporary):
        os.unlink(temporary)
"""
    # The helper receives no secret material; only the exact public model id.
    subprocess.run(
        [str(python), "-c", helper, str(PACKAGE / "config" / "factory.yaml"), str(RUNTIME_CONFIG), worker_model],
        check=True,
        cwd=PACKAGE,
        env=scrubbed_environment(),
    )


def port_is_open(port: int) -> bool:
    with socket.socket() as connection:
        connection.settimeout(0.25)
        return connection.connect_ex(("127.0.0.1", port)) == 0


def process_command(pid: int) -> str:
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "command="],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else ""


def read_processes() -> dict[str, Any]:
    if not PROCESS_FILE.exists():
        return {}
    return read_private_json_any(PROCESS_FILE)


def read_private_json_any(path: Path) -> dict[str, Any]:
    details = path.lstat()
    if not stat.S_ISREG(details.st_mode) or details.st_mode & 0o077:
        raise RuntimeError(f"{path} must be a regular owner-private (0600) JSON file")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise RuntimeError(f"{path} must contain one JSON object")
    return value


def process_fingerprint(paths: list[Path]) -> str:
    """Hash the exact non-secret files whose bytes a local process serves."""

    digest = hashlib.sha256()
    files: list[Path] = []
    for path in paths:
        if path.is_dir():
            files.extend(
                candidate
                for candidate in path.rglob("*")
                if candidate.is_file()
                and "__pycache__" not in candidate.parts
                and candidate.suffix not in {".pyc", ".pyo"}
            )
        elif path.is_file():
            files.append(path)
        else:
            raise RuntimeError(f"process fingerprint input is absent: {path}")
    for path in sorted(set(files), key=lambda item: str(item.resolve())):
        digest.update(str(path.resolve()).encode())
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def start_owned_process(
    name: str,
    command: list[str],
    *,
    needle: str,
    port: int,
    environment: dict[str, str],
    fingerprint: str,
) -> None:
    processes = read_processes()
    existing = processes.get(name)
    if isinstance(existing, dict) and isinstance(existing.get("pid"), int):
        running = process_command(existing["pid"])
        if running and needle in running:
            if port_is_open(port):
                if existing.get("fingerprint") == fingerprint:
                    return
                os.kill(existing["pid"], signal.SIGTERM)
                deadline = time.monotonic() + 8
                while time.monotonic() < deadline and process_command(existing["pid"]):
                    time.sleep(0.2)
                if process_command(existing["pid"]):
                    raise RuntimeError(f"stale owned {name} did not exit after SIGTERM")
                processes.pop(name, None)
                running = ""
            else:
                raise RuntimeError(f"owned {name} process exists but port {port} is not ready")
        if running:
            raise RuntimeError(f"refusing reused PID recorded for {name}; inspect {PROCESS_FILE}")
        processes.pop(name, None)
    if port_is_open(port):
        raise RuntimeError(f"refusing to replace an unrelated listener on 127.0.0.1:{port} ({name})")
    log_path = STATE_DIR / "logs" / f"{name}.log"
    descriptor = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        child = subprocess.Popen(
            command,
            cwd=REPO,
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=descriptor,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
    finally:
        os.close(descriptor)
    processes[name] = {
        "pid": child.pid,
        "needle": needle,
        "port": port,
        "log": str(log_path),
        "fingerprint": fingerprint,
    }
    write_private_json(PROCESS_FILE, processes)


def start_renderer_and_gallery(operator_path: Path) -> None:
    node = shutil.which("node")
    if not node:
        raise RuntimeError("Node.js is required to serve the pinned renderer bundle")
    renderer_script = PACKAGE / "scripts" / "serve-renderer-bundle.mjs"
    renderer_fingerprint = process_fingerprint([renderer_script, REPO / "client" / "web" / "dist"])
    start_owned_process(
        "renderer",
        [node, str(renderer_script), str(REPO / "client" / "web" / "dist"), "127.0.0.1", str(RENDERER_PORT)],
        needle=str(renderer_script),
        port=RENDERER_PORT,
        environment=scrubbed_environment(),
        fingerprint=renderer_fingerprint,
    )
    wait_http(
        f"http://127.0.0.1:{RENDERER_PORT}/",
        timeout=20,
        description="pinned renderer",
    )
    factory = PACKAGE / ".venv" / "bin" / "factory"
    gallery_fingerprint = process_fingerprint(
        [
            PACKAGE / "src" / "snaketron_factory",
            PACKAGE / "templates",
            RUNTIME_CONFIG,
            operator_path.resolve(),
        ]
    )
    start_owned_process(
        "gallery",
        [
            str(factory),
            "serve",
            "--config",
            str(RUNTIME_CONFIG),
            "--env-file",
            str(operator_path.resolve()),
        ],
        needle=f"{factory} serve",
        port=GALLERY_PORT,
        environment=scrubbed_environment(include_human=True),
        fingerprint=gallery_fingerprint,
    )
    wait_http(
        f"http://127.0.0.1:{GALLERY_PORT}/healthz",
        timeout=20,
        description="review gallery",
    )


def factory_command(service_path: Path, *arguments: str, capture: bool = False) -> subprocess.CompletedProcess[str]:
    command = [
        str(PACKAGE / ".venv" / "bin" / "factory"),
        *arguments,
        "--config",
        str(RUNTIME_CONFIG),
        "--env-file",
        str(service_path.resolve()),
        "--json",
    ]
    return subprocess.run(
        command,
        check=True,
        cwd=PACKAGE,
        env=scrubbed_environment(),
        text=True,
        stdout=subprocess.PIPE if capture else None,
    )


def online_readiness(service_path: Path) -> None:
    say("Running live non-generation service/model/browser/LaMa readiness checks...")
    factory_command(service_path, "doctor", "--identity", "service")
    factory_command(service_path, "status")


def has_current_paid_smoke(service_path: Path) -> bool:
    command = [
        str(PACKAGE / ".venv" / "bin" / "factory"),
        "readiness-pin",
        "--check-paid-smoke",
        "--config",
        str(RUNTIME_CONFIG),
        "--env-file",
        str(service_path.resolve()),
        "--json",
    ]
    result = subprocess.run(
        command,
        check=False,
        cwd=PACKAGE,
        env=scrubbed_environment(),
        text=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0


def bootstrap(args: argparse.Namespace) -> tuple[dict[str, str], Path, Path]:
    service_path = args.service_env.expanduser().resolve()
    operator_path = args.operator_env.expanduser().resolve()
    accounts_path = args.accounts_env.expanduser().resolve()
    state = runtime_state(args.admin_user_id)
    start_infrastructure(state)
    state, service, _operator = bootstrap_local_credentials(
        state,
        accounts_path=accounts_path,
        operator_path=operator_path,
        service_path=service_path,
    )
    prepare_factory_environment()
    model = detect_worker_model(service, args.worker_model)
    write_runtime_config(model)
    run_installer(service_path)
    start_renderer_and_gallery(operator_path)
    online_readiness(service_path)
    say(f"Ready: {SCRIPT} open  ->  http://127.0.0.1:{GALLERY_PORT}/login (worker model {model})")
    return state, service_path, operator_path


def run_paid_cycle(service_path: Path) -> None:
    current_marker = has_current_paid_smoke(service_path)
    say("Explicit run action accepted: starting one real provider-backed Skin Factory cycle...")
    result = factory_command(service_path, "run-once", capture=True)
    assert result.stdout is not None
    value = json.loads(result.stdout)
    print(json.dumps(value, sort_keys=True), flush=True)
    advanced = value.get("advanced") if isinstance(value, dict) else None
    failures = isinstance(advanced, list) and any(
        isinstance(item, dict) and (item.get("failure") or item.get("state") == "blocked") for item in advanced
    )
    if failures:
        raise RuntimeError("paid cycle halted or failed; readiness marker was not recorded")
    if current_marker:
        return
    reached_prototype_review = isinstance(advanced, list) and any(
        isinstance(item, dict) and item.get("to") == "prototype_review" for item in advanced
    )
    if value.get("halt") is not None or not reached_prototype_review:
        raise RuntimeError(
            "first paid cycle did not prove concept, image, visual judge, and retained prototype_review; "
            "readiness marker was not recorded"
        )
    factory_command(service_path, "readiness-pin", "--record-paid-smoke")


def stop_owned_processes() -> None:
    processes = read_processes()
    retained: dict[str, Any] = {}
    for name, entry in processes.items():
        if not isinstance(entry, dict) or not isinstance(entry.get("pid"), int):
            retained[name] = entry
            continue
        pid = entry["pid"]
        needle = entry.get("needle", "")
        command = process_command(pid)
        if not command:
            continue
        if not isinstance(needle, str) or not needle or needle not in command:
            say(f"Refusing to stop PID {pid}; it no longer matches recorded {name}")
            retained[name] = entry
            continue
        os.kill(pid, signal.SIGTERM)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline and process_command(pid):
            time.sleep(0.2)
        if process_command(pid):
            say(f"Owned {name} did not exit after SIGTERM; leaving it running")
            retained[name] = entry
    write_private_json(PROCESS_FILE, retained)


def stop_runtime(args: argparse.Namespace) -> None:
    state = runtime_state(None)
    verify_container_ownership(state)
    stop_owned_processes()
    say("Stopping only this checkout's containers; retained data is preserved...")
    subprocess.run(
        [*docker_compose(), "stop", "server", "redis", "localstack"],
        check=True,
        cwd=REPO,
        env=compose_environment(state),
    )


def status_runtime() -> None:
    state = runtime_state(None)
    processes = read_processes()
    result = {
        "checkout": str(REPO),
        "runtime_config": str(RUNTIME_CONFIG),
        "containers": {
            key: bool(container_details(state[key]) and container_details(state[key]).get("State", {}).get("Running"))
            for key in ("localstack_container", "redis_container", "server_container")
        },
        "renderer": port_is_open(RENDERER_PORT),
        "gallery": port_is_open(GALLERY_PORT),
        "gallery_url": f"http://127.0.0.1:{GALLERY_PORT}",
        "recorded_processes": sorted(processes),
    }
    print(json.dumps(result, sort_keys=True, indent=2))


def open_gallery() -> None:
    url = f"http://127.0.0.1:{GALLERY_PORT}/login"
    if not port_is_open(GALLERY_PORT):
        raise SetupRequired(f"gallery is not running; start it with {SCRIPT} start")
    opener = shutil.which("open")
    if not opener:
        raise RuntimeError(f"macOS `open` is unavailable; browse to {url}")
    subprocess.run([opener, url], check=True, env=scrubbed_environment())
    say(url)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("action", nargs="?", choices=("start", "run", "open", "status", "stop"), default="start")
    result.add_argument(
        "--generate",
        action="store_true",
        help="alias for the explicit paid `run` action; never implied by `start`",
    )
    result.add_argument("--service-env", type=Path, default=DEFAULT_SERVICE_ENV)
    result.add_argument("--operator-env", type=Path, default=DEFAULT_OPERATOR_ENV)
    result.add_argument("--accounts-env", type=Path, default=DEFAULT_ACCOUNTS_ENV)
    result.add_argument("--admin-user-id", type=int, help="persist the dedicated local admin id across restarts")
    result.add_argument("--worker-model", help="exact LM Studio model id when /models is ambiguous")
    return result


def main() -> int:
    args = parser().parse_args()
    if args.generate:
        if args.action not in {"start", "run"}:
            raise SystemExit("--generate cannot be combined with open, status, or stop")
        args.action = "run"
    try:
        if args.action == "status":
            status_runtime()
            return 0
        if args.action == "open":
            open_gallery()
            return 0
        if args.action == "stop":
            stop_runtime(args)
            return 0
        _state, service_path, _operator_path = bootstrap(args)
        if args.action == "run":
            run_paid_cycle(service_path)
        return 0
    except SetupRequired as error:
        print(f"setup required: {error}", file=sys.stderr)
        return 2
    except subprocess.CalledProcessError as error:
        print(f"local calibration command failed with exit {error.returncode}", file=sys.stderr)
        return error.returncode or 1
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"local calibration failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
