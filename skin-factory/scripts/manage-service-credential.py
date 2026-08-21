#!/usr/bin/env python3
"""Provision, rotate, or revoke the durable Skin Factory credential safely.

The operator JWT is read from an owner-private JSON file. A newly issued raw
service token is written atomically to the owner-private service JSON and is
never printed or placed in a command-line argument.
"""

from __future__ import annotations

import argparse
import contextlib
import ipaddress
import json
import os
import re
import stat
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

OPERATOR_TOKEN = "SNAKETRON_FACTORY_OPERATOR_TOKEN"
SERVICE_TOKEN = "SNAKETRON_FACTORY_SERVICE_TOKEN"
TOKEN_ID = re.compile(r"^snk_factory_v1\.([0-9a-f]{32})\.[A-Za-z0-9_-]{43}$")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """Never forward an administrator bearer to a redirect target."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001, ANN201
        raise urllib.error.HTTPError(newurl, code, "credential API redirects are refused", headers, fp)


def validate_base_url(value: str) -> str:
    parsed = urllib.parse.urlsplit(value)
    if not parsed.hostname or parsed.username is not None or parsed.password is not None:
        raise SystemExit("--base-url must name one origin without userinfo")
    if parsed.query or parsed.fragment or parsed.path not in {"", "/"}:
        raise SystemExit("--base-url must not contain a path, query, or fragment")
    loopback = parsed.hostname == "localhost"
    with contextlib.suppress(ValueError):
        loopback = loopback or ipaddress.ip_address(parsed.hostname).is_loopback
    if parsed.scheme != "https" and not (parsed.scheme == "http" and loopback):
        raise SystemExit("--base-url must use HTTPS (HTTP is allowed only for an explicit loopback host)")
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, "", "", ""))


def private_json(path: Path, *, may_not_exist: bool = False) -> dict[str, str]:
    if may_not_exist and not path.exists():
        return {}
    details = path.stat()
    if not stat.S_ISREG(details.st_mode) or details.st_mode & 0o077:
        raise SystemExit(f"{path} must be a regular owner-private (0600) file")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or not all(
        isinstance(key, str) and isinstance(item, str) for key, item in value.items()
    ):
        raise SystemExit(f"{path} must contain one JSON object of string values")
    return value


def write_private_json(path: Path, value: dict[str, str]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, sort_keys=True, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary)
        raise


def request(
    base_url: str,
    operator_token: str,
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    base_url = validate_base_url(base_url)
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {
        "Authorization": f"Bearer {operator_token}",
        "Accept": "application/json",
    }
    if data is not None:
        headers["Content-Type"] = "application/json"
    try:
        with urllib.request.build_opener(NoRedirect()).open(
            urllib.request.Request(
                f"{base_url}{path}",
                data=data,
                headers=headers,
                method=method,
            ),
            timeout=30,
        ) as response:
            value = json.load(response)
    except urllib.error.HTTPError as error:
        detail = error.read(1_000).decode("utf-8", errors="replace")
        raise SystemExit(f"Snaketron credential API returned HTTP {error.code}: {detail}") from error
    except urllib.error.URLError as error:
        raise SystemExit(f"Snaketron credential API is unavailable: {error.reason}") from error
    if not isinstance(value, dict):
        raise SystemExit("Snaketron credential API returned a non-object response")
    return value


def current_credential_id(service: dict[str, str], explicit: str | None) -> str:
    if explicit:
        if re.fullmatch(r"[0-9a-f]{32}", explicit) is None:
            raise SystemExit("--credential-id must be exactly 32 lowercase hex characters")
        return explicit
    token = service.get(SERVICE_TOKEN, "")
    match = TOKEN_ID.fullmatch(token)
    if match is None:
        raise SystemExit(
            f"{SERVICE_TOKEN} is missing or malformed; supply the non-secret --credential-id"
        )
    return match.group(1)


def install_issued_token(service_path: Path, service: dict[str, str], response: dict[str, Any]) -> str:
    token = response.get("token")
    credential = response.get("credential")
    if not isinstance(token, str) or not isinstance(credential, dict):
        raise SystemExit("Snaketron did not return a one-time credential response")
    match = TOKEN_ID.fullmatch(token)
    credential_id = credential.get("credentialId")
    if match is None or credential_id != match.group(1):
        raise SystemExit("Snaketron returned a malformed or inconsistent service credential")
    service[SERVICE_TOKEN] = token
    write_private_json(service_path, service)
    return credential_id


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("provision", "rotate", "revoke"))
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--operator-env", type=Path, required=True)
    parser.add_argument("--service-env", type=Path, required=True)
    parser.add_argument("--user-id", type=int)
    parser.add_argument("--credential-id")
    args = parser.parse_args()

    operator = private_json(args.operator_env)
    operator_token = operator.get(OPERATOR_TOKEN)
    if not operator_token:
        raise SystemExit(f"{args.operator_env} does not contain {OPERATOR_TOKEN}")
    service = private_json(args.service_env, may_not_exist=True)

    if args.action == "provision":
        if args.user_id is None or args.user_id <= 0:
            raise SystemExit("provision requires a positive --user-id")
        response = request(
            args.base_url,
            operator_token,
            "POST",
            "/api/admin/factory-credentials",
            {"userId": args.user_id},
        )
        credential_id = install_issued_token(args.service_env, service, response)
        print(f"provisioned factory credential {credential_id} into {args.service_env}")
        return

    credential_id = current_credential_id(service, args.credential_id)
    if args.action == "rotate":
        response = request(
            args.base_url,
            operator_token,
            "POST",
            f"/api/admin/factory-credentials/{credential_id}/rotate",
        )
        replacement_id = install_issued_token(args.service_env, service, response)
        print(
            f"rotated factory credential {credential_id} to {replacement_id} in {args.service_env}"
        )
        return

    request(
        args.base_url,
        operator_token,
        "DELETE",
        f"/api/admin/factory-credentials/{credential_id}",
    )
    token = service.get(SERVICE_TOKEN, "")
    if TOKEN_ID.fullmatch(token) is not None and current_credential_id(service, None) == credential_id:
        service.pop(SERVICE_TOKEN)
        write_private_json(args.service_env, service)
    print(f"revoked factory credential {credential_id}")


if __name__ == "__main__":
    main()
