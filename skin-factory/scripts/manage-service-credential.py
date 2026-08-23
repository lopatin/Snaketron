#!/usr/bin/env python3
"""Bootstrap local accounts or manage the durable Skin Factory credential safely.

The operator JWT is read from an owner-private JSON file. A newly issued raw
service token is written atomically to the owner-private service JSON and is
never printed or placed in a command-line argument.

Local account bootstrap is deliberately loopback-only. It stores generated
passwords in a separate owner-private JSON file, registers each account (or
logs it in on an exact retry), and writes only the administrator JWT to the
operator file. The server must then be restarted with the emitted numeric
administrator ID in ``SNAKETRON_ADMIN_USER_IDS`` before provisioning the
least-privilege factory credential.
"""

from __future__ import annotations

import argparse
import contextlib
import ipaddress
import json
import os
import re
import secrets
import stat
import subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

OPERATOR_TOKEN = "SNAKETRON_FACTORY_OPERATOR_TOKEN"
SERVICE_TOKEN = "SNAKETRON_FACTORY_SERVICE_TOKEN"
FAL_API_KEY = "FAL_API_KEY"
FAL_KEY = "FAL_KEY"
LOCAL_ADMIN_USERNAME = "SNAKETRON_LOCAL_ADMIN_USERNAME"
LOCAL_ADMIN_PASSWORD = "SNAKETRON_LOCAL_ADMIN_PASSWORD"
LOCAL_ADMIN_USER_ID = "SNAKETRON_LOCAL_ADMIN_USER_ID"
LOCAL_FACTORY_USERNAME = "SNAKETRON_LOCAL_FACTORY_USERNAME"
LOCAL_FACTORY_PASSWORD = "SNAKETRON_LOCAL_FACTORY_PASSWORD"
LOCAL_FACTORY_USER_ID = "SNAKETRON_LOCAL_FACTORY_USER_ID"
TOKEN_ID = re.compile(r"^snk_factory_v1\.([0-9a-f]{32})\.[A-Za-z0-9_-]{43}$")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """Never forward an administrator bearer to a redirect target."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
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


def validate_loopback_base_url(value: str) -> str:
    """Accept only an explicit loopback origin for account/password traffic."""

    normalized = validate_base_url(value)
    hostname = urllib.parse.urlsplit(normalized).hostname
    loopback = hostname == "localhost"
    with contextlib.suppress(ValueError):
        loopback = loopback or (hostname is not None and ipaddress.ip_address(hostname).is_loopback)
    if not loopback:
        raise SystemExit("local account bootstrap requires an explicit loopback --base-url")
    return normalized


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


def account_request(
    base_url: str,
    path: str,
    payload: dict[str, str],
    *,
    allow_conflict: bool = False,
) -> tuple[int, dict[str, Any]]:
    """Send one loopback-only registration/login request without exposing secrets."""

    base_url = validate_loopback_base_url(base_url)
    data = json.dumps(payload).encode("utf-8")
    try:
        with urllib.request.build_opener(NoRedirect()).open(
            urllib.request.Request(
                f"{base_url}{path}",
                data=data,
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                method="POST",
            ),
            timeout=30,
        ) as response:
            value = json.load(response)
            status_code = response.status
    except urllib.error.HTTPError as error:
        detail = error.read(1_000).decode("utf-8", errors="replace")
        if allow_conflict and error.code == 409:
            return error.code, {}
        raise SystemExit(f"Snaketron account API returned HTTP {error.code}: {detail}") from error
    except urllib.error.URLError as error:
        raise SystemExit(f"Snaketron account API is unavailable: {error.reason}") from error
    if not isinstance(value, dict):
        raise SystemExit("Snaketron account API returned a non-object response")
    return status_code, value


def local_account_secrets(path: Path) -> dict[str, str]:
    """Load or generate the two dedicated local account identities."""

    accounts = private_json(path, may_not_exist=True)
    if not accounts:
        suffix = secrets.token_hex(3)
        accounts = {
            LOCAL_ADMIN_USERNAME: f"local_admin_{suffix}",
            LOCAL_ADMIN_PASSWORD: secrets.token_urlsafe(32),
            LOCAL_FACTORY_USERNAME: f"local_factory_{suffix}",
            LOCAL_FACTORY_PASSWORD: secrets.token_urlsafe(32),
        }
        write_private_json(path, accounts)

    missing = [
        key
        for key in (
            LOCAL_ADMIN_USERNAME,
            LOCAL_ADMIN_PASSWORD,
            LOCAL_FACTORY_USERNAME,
            LOCAL_FACTORY_PASSWORD,
        )
        if not accounts.get(key)
    ]
    if missing:
        raise SystemExit(f"{path} is missing required local account keys: {', '.join(missing)}")
    if accounts[LOCAL_ADMIN_USERNAME] == accounts[LOCAL_FACTORY_USERNAME]:
        raise SystemExit("local administrator and factory usernames must be distinct")
    return accounts


def register_or_login_local_account(
    base_url: str,
    username: str,
    password: str,
) -> dict[str, Any]:
    """Register once, falling back to login so an interrupted bootstrap is retry-safe."""

    status_code, response = account_request(
        base_url,
        "/api/auth/register",
        {"username": username, "password": password},
        allow_conflict=True,
    )
    if status_code == 409:
        _, response = account_request(
            base_url,
            "/api/auth/login",
            {"username": username, "password": password},
        )

    token = response.get("token")
    user = response.get("user")
    if not isinstance(token, str) or not token or not isinstance(user, dict):
        raise SystemExit("Snaketron returned a malformed local account response")
    if (
        user.get("username") != username
        or type(user.get("id")) is not int
        or user["id"] <= 0
        or user.get("isGuest") is not False
        or type(user.get("isAdmin")) is not bool
    ):
        raise SystemExit("Snaketron returned an inconsistent local account identity")
    return {"token": token, "user": user}


def bootstrap_local_accounts(
    base_url: str,
    accounts_path: Path,
    operator_path: Path,
    *,
    require_admin: bool,
) -> tuple[int, int, bool]:
    """Create/login the local admin and non-admin factory identities."""

    base_url = validate_loopback_base_url(base_url)
    accounts = local_account_secrets(accounts_path)
    admin = register_or_login_local_account(
        base_url,
        accounts[LOCAL_ADMIN_USERNAME],
        accounts[LOCAL_ADMIN_PASSWORD],
    )
    factory = register_or_login_local_account(
        base_url,
        accounts[LOCAL_FACTORY_USERNAME],
        accounts[LOCAL_FACTORY_PASSWORD],
    )
    admin_user = admin["user"]
    factory_user = factory["user"]
    admin_user_id = admin_user["id"]
    factory_user_id = factory_user["id"]
    if admin_user_id == factory_user_id:
        raise SystemExit("local administrator and factory accounts resolved to the same user")
    if factory_user["isAdmin"]:
        raise SystemExit(
            "factory account is currently an administrator; remove its ID from "
            "SNAKETRON_ADMIN_USER_IDS before provisioning"
        )

    for key, actual in (
        (LOCAL_ADMIN_USER_ID, admin_user_id),
        (LOCAL_FACTORY_USER_ID, factory_user_id),
    ):
        recorded = accounts.get(key)
        if recorded is not None and recorded != str(actual):
            raise SystemExit(f"{accounts_path} contains a stale {key}")
        accounts[key] = str(actual)

    if require_admin and not admin_user["isAdmin"]:
        raise SystemExit(
            f"local administrator {admin_user_id} is not authorized; restart the same isolated "
            f"server with SNAKETRON_ADMIN_USER_IDS={admin_user_id}, then retry"
        )

    write_private_json(accounts_path, accounts)
    operator = private_json(operator_path, may_not_exist=True)
    operator[OPERATOR_TOKEN] = admin["token"]
    write_private_json(operator_path, operator)
    return admin_user_id, factory_user_id, admin_user["isAdmin"]


def current_credential_id(service: dict[str, str], explicit: str | None) -> str:
    if explicit:
        if re.fullmatch(r"[0-9a-f]{32}", explicit) is None:
            raise SystemExit("--credential-id must be exactly 32 lowercase hex characters")
        return explicit
    token = service.get(SERVICE_TOKEN, "")
    match = TOKEN_ID.fullmatch(token)
    if match is None:
        raise SystemExit(f"{SERVICE_TOKEN} is missing or malformed; supply the non-secret --credential-id")
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


def _validated_fal_key(value: str) -> str:
    if not value or len(value) > 4_096 or any(character.isspace() or ord(character) < 32 for character in value):
        raise SystemExit("Fal credential is missing or malformed")
    return value


def discover_fal_key(
    environment: dict[str, str] | None = None,
    *,
    run_login_shell: Any | None = None,
) -> tuple[str, str]:
    """Read Fal authority without ever writing the secret to user output.

    The fresh login-shell protocol accepts output only when stdout is exactly
    one nonce-delimited value and stderr is empty. Shell startup chatter is an
    error, and its captured bytes are intentionally omitted from diagnostics.
    """

    inherited = os.environ if environment is None else environment
    run_login_shell = run_login_shell or subprocess.run
    primary = inherited.get(FAL_API_KEY, "")
    fallback = inherited.get(FAL_KEY, "")
    if primary or fallback:
        if primary and fallback and primary != fallback:
            raise SystemExit("FAL_API_KEY and FAL_KEY disagree")
        return _validated_fal_key(primary or fallback), (FAL_API_KEY if primary else FAL_KEY)

    nonce = secrets.token_hex(24)
    begin = f"__snaketron_fal_begin_{nonce}__"
    end = f"__snaketron_fal_end_{nonce}__"
    conflict = f"__snaketron_fal_conflict_{nonce}__"
    missing = f"__snaketron_fal_missing_{nonce}__"
    command = (
        "emulate -L zsh; "
        "primary=${FAL_API_KEY-}; fallback=${FAL_KEY-}; "
        f"if [[ -n $primary && -n $fallback && $primary != $fallback ]]; then command printf '%s\\n' '{conflict}'; "
        f"elif [[ -n $primary ]]; then command printf '%s\\n%s\\n%s\\n' '{begin}' \"$primary\" '{end}'; "
        f"elif [[ -n $fallback ]]; then command printf '%s\\n%s\\n%s\\n' '{begin}' \"$fallback\" '{end}'; "
        f"else command printf '%s\\n' '{missing}'; fi"
    )
    child_environment = os.environ.copy()
    child_environment.pop(FAL_API_KEY, None)
    child_environment.pop(FAL_KEY, None)
    try:
        completed = run_login_shell(
            ["/bin/zsh", "-lic", command],
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
            env=child_environment,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise SystemExit("fresh zsh login could not be inspected for a Fal credential") from error
    if completed.returncode != 0 or completed.stderr != "":
        raise SystemExit("fresh zsh login produced ambiguous startup output while reading Fal authority")
    lines = completed.stdout.splitlines()
    if lines == [conflict]:
        raise SystemExit("fresh zsh login defines conflicting FAL_API_KEY and FAL_KEY values")
    if lines == [missing]:
        raise SystemExit("fresh zsh login does not define FAL_API_KEY or FAL_KEY")
    if len(lines) != 3 or lines[0] != begin or lines[2] != end:
        raise SystemExit("fresh zsh login produced ambiguous startup output while reading Fal authority")
    return _validated_fal_key(lines[1]), "fresh-zsh-login"


def import_fal_key(service_path: Path, environment: dict[str, str] | None = None) -> str:
    service = private_json(service_path, may_not_exist=True)
    value, source = discover_fal_key(environment)
    service[FAL_API_KEY] = value
    # The scheduler consumes one canonical name. Keeping a second alias would
    # make later rotation ambiguous.
    service.pop(FAL_KEY, None)
    write_private_json(service_path, service)
    return source


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=("bootstrap-local-accounts", "import-fal", "provision", "rotate", "revoke"),
    )
    parser.add_argument("--base-url")
    parser.add_argument("--operator-env", type=Path)
    parser.add_argument("--service-env", type=Path)
    parser.add_argument("--accounts-env", type=Path)
    parser.add_argument(
        "--require-admin",
        action="store_true",
        help="for local bootstrap, require the restarted server to recognize the admin ID",
    )
    parser.add_argument("--user-id", type=int)
    parser.add_argument("--credential-id")
    args = parser.parse_args()

    if args.action == "import-fal":
        if args.service_env is None:
            raise SystemExit("import-fal requires --service-env")
        if any((args.base_url, args.operator_env, args.accounts_env, args.user_id, args.credential_id)):
            raise SystemExit("import-fal accepts only --service-env")
        if args.require_admin:
            raise SystemExit("--require-admin is valid only with bootstrap-local-accounts")
        source = import_fal_key(args.service_env)
        print(f"imported Fal service authority from {source} into {args.service_env}")
        return

    if args.action == "bootstrap-local-accounts":
        if args.accounts_env is None or args.operator_env is None or args.base_url is None:
            raise SystemExit("bootstrap-local-accounts requires --base-url, --operator-env, and --accounts-env")
        admin_user_id, factory_user_id, is_admin = bootstrap_local_accounts(
            args.base_url,
            args.accounts_env,
            args.operator_env,
            require_admin=args.require_admin,
        )
        print(f"bootstrapped dedicated local accounts admin_user_id={admin_user_id} factory_user_id={factory_user_id}")
        if is_admin:
            print("local administrator authority confirmed")
        else:
            print(
                "restart the same isolated server with "
                f"SNAKETRON_ADMIN_USER_IDS={admin_user_id}; then rerun this action with "
                "--require-admin before provisioning"
            )
        return

    if args.require_admin:
        raise SystemExit("--require-admin is valid only with bootstrap-local-accounts")
    if args.accounts_env is not None:
        raise SystemExit("--accounts-env is valid only with bootstrap-local-accounts")
    if args.service_env is None:
        raise SystemExit(f"{args.action} requires --service-env")
    if args.operator_env is None or args.base_url is None:
        raise SystemExit(f"{args.action} requires --base-url and --operator-env")

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
        print(f"rotated factory credential {credential_id} to {replacement_id} in {args.service_env}")
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
