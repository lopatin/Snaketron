from __future__ import annotations

import importlib.util
import json
import stat
import urllib.error
import urllib.request
from pathlib import Path

import pytest


def _script():
    path = Path(__file__).parents[1] / "scripts/manage-service-credential.py"
    spec = importlib.util.spec_from_file_location("manage_service_credential", path)
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
