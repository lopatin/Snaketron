from __future__ import annotations

import importlib.util
import json
import stat
import sys
import urllib.error
import urllib.request
from pathlib import Path
from types import SimpleNamespace

import pytest


def _script():
    path = Path(__file__).parents[1] / "scripts/manage-service-credential.py"
    spec = importlib.util.spec_from_file_location("manage_service_credential", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _launcher():
    path = Path(__file__).parents[1] / "scripts/local-calibration.py"
    spec = importlib.util.spec_from_file_location("local_calibration", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_one_time_service_token_is_written_atomically_and_owner_private(tmp_path: Path) -> None:
    module = _script()
    service_path = tmp_path / "private/service.json"
    credential_id = "0123456789abcdef0123456789abcdef"
    raw_token = f"snk_factory_v1.{credential_id}.{'A' * 43}"

    installed_id = module.install_issued_token(
        service_path,
        {"GEMINI_API_KEY": "provider-secret"},
        {
            "token": raw_token,
            "credential": {
                "credentialType": "factoryService",
                "credentialId": credential_id,
                "revocable": True,
                "expiresAt": None,
            },
        },
    )

    assert installed_id == credential_id
    assert stat.S_IMODE(service_path.stat().st_mode) == 0o600
    assert json.loads(service_path.read_text(encoding="utf-8")) == {
        "GEMINI_API_KEY": "provider-secret",
        "SNAKETRON_FACTORY_SERVICE_TOKEN": raw_token,
    }
    assert module.current_credential_id(module.private_json(service_path), None) == credential_id


def test_fal_key_import_uses_fresh_login_protocol_and_never_emits_secret(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _script()
    secret = "fal-secret-value:with-safe-punctuation"
    service_path = tmp_path / "private/service.json"
    monkeypatch.setattr(module.secrets, "token_hex", lambda _size: "a" * 48)

    def login(command, **kwargs):
        assert command[:2] == ["/bin/zsh", "-lic"]
        assert secret not in " ".join(command)
        assert module.FAL_API_KEY not in kwargs["env"]
        assert module.FAL_KEY not in kwargs["env"]
        begin = "__snaketron_fal_begin_" + "a" * 48 + "__"
        end = "__snaketron_fal_end_" + "a" * 48 + "__"
        return SimpleNamespace(returncode=0, stdout=f"{begin}\n{secret}\n{end}\n", stderr="")

    monkeypatch.setattr(module.subprocess, "run", login)
    source = module.import_fal_key(service_path, {})

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""
    assert secret not in captured.out + captured.err
    assert source == "fresh-zsh-login"
    assert stat.S_IMODE(service_path.stat().st_mode) == 0o600
    assert json.loads(service_path.read_text(encoding="utf-8")) == {module.FAL_API_KEY: secret}


def test_fal_key_import_accepts_existing_alias_and_canonicalizes_without_output(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _script()
    secret = "existing-fal-key"
    service_path = tmp_path / "service.json"

    assert module.import_fal_key(service_path, {module.FAL_KEY: secret}) == module.FAL_KEY

    captured = capsys.readouterr()
    assert secret not in captured.out + captured.err
    assert json.loads(service_path.read_text(encoding="utf-8")) == {module.FAL_API_KEY: secret}


def test_fal_login_import_rejects_noisy_or_conflicting_output_without_leaking_it(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _script()
    secret = "must-not-leak"
    monkeypatch.setattr(module.secrets, "token_hex", lambda _size: "b" * 48)

    def noisy(_command, **_kwargs):
        begin = "__snaketron_fal_begin_" + "b" * 48 + "__"
        end = "__snaketron_fal_end_" + "b" * 48 + "__"
        return SimpleNamespace(
            returncode=0,
            stdout=f"startup chatter\n{begin}\n{secret}\n{end}\n",
            stderr="",
        )

    with pytest.raises(SystemExit, match="ambiguous startup output"):
        module.discover_fal_key({}, run_login_shell=noisy)

    captured = capsys.readouterr()
    assert secret not in captured.out + captured.err


def test_service_file_reader_rejects_group_readable_secrets(tmp_path: Path) -> None:
    module = _script()
    path = tmp_path / "service.json"
    path.write_text("{}\n", encoding="utf-8")
    path.chmod(0o640)

    try:
        module.private_json(path)
    except SystemExit as error:
        assert "owner-private (0600)" in str(error)
    else:
        raise AssertionError("group-readable service credentials must be rejected")


@pytest.mark.parametrize(
    ("value", "normalized"),
    [
        ("https://snaketron.example/", "https://snaketron.example"),
        ("https://snaketron.example:8443", "https://snaketron.example:8443"),
        ("http://127.0.0.1:8080/", "http://127.0.0.1:8080"),
        ("http://[::1]:8080", "http://[::1]:8080"),
        ("http://localhost:8080", "http://localhost:8080"),
    ],
)
def test_credential_api_origin_is_tls_or_explicit_loopback(value: str, normalized: str) -> None:
    assert _script().validate_base_url(value) == normalized


@pytest.mark.parametrize(
    "value",
    [
        "http://snaketron.example",
        "https://admin:secret@snaketron.example",
        "https://snaketron.example/api",
        "https://snaketron.example?next=elsewhere",
        "https://snaketron.example#credentials",
    ],
)
def test_credential_api_rejects_unsafe_origins(value: str) -> None:
    with pytest.raises(SystemExit):
        _script().validate_base_url(value)


def test_credential_api_never_forwards_admin_bearer_across_redirects() -> None:
    module = _script()
    request = urllib.request.Request(
        "https://snaketron.example/api/admin/factory-credentials",
        headers={"Authorization": "Bearer must-not-move"},
    )
    with pytest.raises(urllib.error.HTTPError, match="redirects are refused"):
        module.NoRedirect().redirect_request(
            request,
            None,
            307,
            "Temporary Redirect",
            {},
            "https://attacker.example/collect",
        )


def test_local_account_bootstrap_writes_only_owner_private_secrets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _script()
    accounts_path = tmp_path / "private/accounts.json"
    operator_path = tmp_path / "private/operator.json"

    def authenticate(_base_url: str, username: str, password: str):
        assert password
        is_admin = username.startswith("local_admin_")
        return {
            "token": "admin-jwt" if is_admin else "unused-factory-jwt",
            "user": {
                "id": 41 if is_admin else 42,
                "username": username,
                "isGuest": False,
                "isAdmin": is_admin,
            },
        }

    monkeypatch.setattr(module, "register_or_login_local_account", authenticate)

    result = module.bootstrap_local_accounts(
        "http://127.0.0.1:8080",
        accounts_path,
        operator_path,
        require_admin=True,
    )

    assert result == (41, 42, True)
    assert stat.S_IMODE(accounts_path.stat().st_mode) == 0o600
    assert stat.S_IMODE(operator_path.stat().st_mode) == 0o600
    accounts = json.loads(accounts_path.read_text(encoding="utf-8"))
    assert accounts[module.LOCAL_ADMIN_USERNAME].startswith("local_admin_")
    assert accounts[module.LOCAL_FACTORY_USERNAME].startswith("local_factory_")
    assert accounts[module.LOCAL_ADMIN_USER_ID] == "41"
    assert accounts[module.LOCAL_FACTORY_USER_ID] == "42"
    assert json.loads(operator_path.read_text(encoding="utf-8")) == {module.OPERATOR_TOKEN: "admin-jwt"}


def test_local_account_registration_is_retry_safe_on_username_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _script()
    calls: list[tuple[str, dict[str, str], bool]] = []

    def account_request(_base_url, path, payload, *, allow_conflict=False):
        calls.append((path, payload, allow_conflict))
        if path.endswith("register"):
            return 409, {}
        return 200, {
            "token": "fresh-jwt",
            "user": {
                "id": 17,
                "username": payload["username"],
                "isGuest": False,
                "isAdmin": False,
            },
        }

    monkeypatch.setattr(module, "account_request", account_request)

    response = module.register_or_login_local_account(
        "http://localhost:8080", "factory_service", "owner-private-password"
    )

    assert response["token"] == "fresh-jwt"
    assert calls == [
        (
            "/api/auth/register",
            {"username": "factory_service", "password": "owner-private-password"},
            True,
        ),
        (
            "/api/auth/login",
            {"username": "factory_service", "password": "owner-private-password"},
            False,
        ),
    ]


def test_local_bootstrap_requires_restarted_admin_and_rejects_admin_factory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _script()
    accounts_path = tmp_path / "accounts.json"
    accounts_path.write_text(
        json.dumps(
            {
                module.LOCAL_ADMIN_USERNAME: "local_admin",
                module.LOCAL_ADMIN_PASSWORD: "admin-password",
                module.LOCAL_FACTORY_USERNAME: "local_factory",
                module.LOCAL_FACTORY_PASSWORD: "factory-password",
            }
        ),
        encoding="utf-8",
    )
    accounts_path.chmod(0o600)

    def pending_admin(_base_url: str, username: str, _password: str):
        return {
            "token": "jwt",
            "user": {
                "id": 51 if username == "local_admin" else 52,
                "username": username,
                "isGuest": False,
                "isAdmin": False,
            },
        }

    monkeypatch.setattr(module, "register_or_login_local_account", pending_admin)
    with pytest.raises(SystemExit, match="SNAKETRON_ADMIN_USER_IDS=51"):
        module.bootstrap_local_accounts(
            "http://127.0.0.1:8080",
            accounts_path,
            tmp_path / "operator.json",
            require_admin=True,
        )

    def admin_factory(_base_url: str, username: str, _password: str):
        return {
            "token": "jwt",
            "user": {
                "id": 51 if username == "local_admin" else 52,
                "username": username,
                "isGuest": False,
                "isAdmin": True,
            },
        }

    monkeypatch.setattr(module, "register_or_login_local_account", admin_factory)
    with pytest.raises(SystemExit, match="factory account is currently an administrator"):
        module.bootstrap_local_accounts(
            "http://127.0.0.1:8080",
            accounts_path,
            tmp_path / "operator.json",
            require_admin=True,
        )


def test_local_bootstrap_refuses_non_loopback_origins(tmp_path: Path) -> None:
    module = _script()
    with pytest.raises(SystemExit, match="explicit loopback"):
        module.bootstrap_local_accounts(
            "https://snaketron.example",
            tmp_path / "accounts.json",
            tmp_path / "operator.json",
            require_admin=False,
        )


def test_local_bootstrap_cli_prints_only_non_secret_handoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _script()
    accounts_path = tmp_path / "accounts.json"
    operator_path = tmp_path / "operator.json"
    monkeypatch.setattr(module, "bootstrap_local_accounts", lambda *_args, **_kwargs: (71, 72, False))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "manage-service-credential.py",
            "bootstrap-local-accounts",
            "--base-url",
            "http://127.0.0.1:8080",
            "--accounts-env",
            str(accounts_path),
            "--operator-env",
            str(operator_path),
        ],
    )

    module.main()

    assert capsys.readouterr().out.splitlines() == [
        "bootstrapped dedicated local accounts admin_user_id=71 factory_user_id=72",
        "restart the same isolated server with SNAKETRON_ADMIN_USER_IDS=71; "
        "then rerun this action with --require-admin before provisioning",
    ]


def test_launcher_orchestrates_bootstrap_restart_provision_and_probe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _launcher()
    accounts_path = tmp_path / "accounts.json"
    operator_path = tmp_path / "operator.json"
    service_path = tmp_path / "service.json"
    module.write_private_json(service_path, {"GEMINI_API_KEY": "provider-key"})
    token = f"snk_factory_v1.{'1' * 32}.{'A' * 43}"
    actions: list[tuple[str, bool]] = []
    recreated: list[dict[str, str]] = []
    probed: list[str] = []

    def run_step(command, *, recovery, description):
        assert recovery and description
        action = command[2]
        actions.append((action, "--require-admin" in command))
        if action == "bootstrap-local-accounts":
            module.write_private_json(
                accounts_path,
                {
                    module.LOCAL_ADMIN_USER_ID: "81",
                    module.LOCAL_FACTORY_USER_ID: "82",
                },
            )
            operator = module.read_private_json(operator_path)
            operator[module.OPERATOR_TOKEN] = "admin-jwt"
            module.write_private_json(operator_path, operator)
        elif action == "provision":
            assert command[command.index("--user-id") + 1] == "82"
            service = module.read_private_json(service_path)
            service[module.SERVICE_TOKEN] = token
            module.write_private_json(service_path, service)

    monkeypatch.setattr(module, "run_credential_step", run_step)
    monkeypatch.setattr(
        module,
        "runtime_state",
        lambda admin_id=None: {"admin_user_ids": "" if admin_id is None else str(admin_id)},
    )
    monkeypatch.setattr(module, "recreate_server", lambda state: recreated.append(state))
    monkeypatch.setattr(module, "probe_service_capabilities", lambda value: probed.append(value))

    state, service, operator = module.bootstrap_local_credentials(
        {"admin_user_ids": ""},
        accounts_path=accounts_path,
        operator_path=operator_path,
        service_path=service_path,
    )

    assert actions == [
        ("bootstrap-local-accounts", False),
        ("bootstrap-local-accounts", True),
        ("provision", False),
    ]
    assert recreated == [{"admin_user_ids": "81"}]
    assert state["admin_user_ids"] == "81"
    assert service[module.SERVICE_TOKEN] == token
    assert operator[module.OPERATOR_TOKEN] == "admin-jwt"
    assert operator[module.REVIEW_ACTOR] == "human:local-operator"
    assert probed == [token]


def test_launcher_never_replaces_an_existing_service_token(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = _launcher()
    token = f"snk_factory_v1.{'2' * 32}.{'B' * 43}"
    service_path = tmp_path / "service.json"
    operator_path = tmp_path / "operator.json"
    module.write_private_json(
        service_path,
        {"GEMINI_API_KEY": "provider-key", module.SERVICE_TOKEN: token},
    )
    module.write_private_json(operator_path, {module.OPERATOR_TOKEN: "admin-jwt"})
    monkeypatch.setattr(
        module,
        "run_credential_step",
        lambda *_args, **_kwargs: pytest.fail("existing credentials must never be reprovisioned"),
    )
    probed: list[str] = []
    monkeypatch.setattr(module, "probe_service_capabilities", lambda value: probed.append(value))

    _, service, _ = module.bootstrap_local_credentials(
        {"admin_user_ids": "81"},
        accounts_path=tmp_path / "accounts.json",
        operator_path=operator_path,
        service_path=service_path,
    )

    assert service[module.SERVICE_TOKEN] == token
    assert probed == [token]


def test_launcher_refreshes_expiring_admin_jwt_without_rotating_service_token(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _launcher()
    token = f"snk_factory_v1.{'3' * 32}.{'C' * 43}"
    accounts_path = tmp_path / "accounts.json"
    operator_path = tmp_path / "operator.json"
    service_path = tmp_path / "service.json"
    module.write_private_json(accounts_path, {module.LOCAL_ADMIN_USER_ID: "81"})
    module.write_private_json(operator_path, {module.OPERATOR_TOKEN: "expiring-admin-jwt"})
    module.write_private_json(
        service_path,
        {"GEMINI_API_KEY": "provider-key", module.SERVICE_TOKEN: token},
    )
    actions: list[list[str]] = []
    monkeypatch.setattr(module, "run_credential_step", lambda command, **_kwargs: actions.append(command))
    monkeypatch.setattr(module, "probe_service_capabilities", lambda _value: None)

    _, service, _ = module.bootstrap_local_credentials(
        {"admin_user_ids": "81"},
        accounts_path=accounts_path,
        operator_path=operator_path,
        service_path=service_path,
    )

    assert len(actions) == 1
    assert actions[0][2] == "bootstrap-local-accounts"
    assert "--require-admin" in actions[0]
    assert service[module.SERVICE_TOKEN] == token


def test_launcher_replaces_known_placeholders_and_refuses_redirects(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = _launcher()
    operator_path = tmp_path / "operator.json"
    module.write_private_json(
        operator_path,
        {
            module.REVIEW_TOKEN: "replace-with-a-long-random-review-secret",
            module.REVIEW_ACTOR: "human:operator-name",
        },
    )
    operator = module.ensure_operator_identity(operator_path)
    assert not operator[module.REVIEW_TOKEN].startswith("replace-")
    assert operator[module.REVIEW_ACTOR] == "human:local-operator"

    request = urllib.request.Request(
        "http://127.0.0.1:18080/api/factory/capabilities",
        headers={"Authorization": "Bearer must-not-move"},
    )
    with pytest.raises(urllib.error.HTTPError, match="redirects are refused"):
        module.NoRedirect().redirect_request(
            request,
            None,
            307,
            "Temporary Redirect",
            {},
            "https://attacker.example/collect",
        )

    service_path = tmp_path / "service.json"
    module.write_private_json(
        service_path,
        {
            "GEMINI_API_KEY": "provider-key",
            module.SERVICE_TOKEN: "malformed-but-not-a-known-placeholder",
        },
    )
    monkeypatch.setattr(
        module,
        "run_credential_step",
        lambda *_args, **_kwargs: pytest.fail("unknown malformed tokens must not be replaced"),
    )
    with pytest.raises(module.SetupRequired, match="present but malformed"):
        module.bootstrap_local_credentials(
            {},
            accounts_path=tmp_path / "accounts.json",
            operator_path=operator_path,
            service_path=service_path,
        )


def test_launcher_capability_probe_rejects_admin_or_publication_authority() -> None:
    module = _launcher()
    envelope = {
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
    module.validate_capability_envelope(envelope)

    envelope["identity"]["isAdmin"] = True
    envelope["capabilities"]["publishSkins"] = True
    with pytest.raises(module.SetupRequired, match="registered non-admin"):
        module.validate_capability_envelope(envelope)
