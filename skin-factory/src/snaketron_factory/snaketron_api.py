"""Exact-hash Snaketron first-class skin and texture API client."""

from __future__ import annotations

import hashlib
import json
import os
import re
from typing import Any

import httpx

from .assets import ForgeBundle
from .config import FactoryConfig
from .domain import ProviderError, ProviderFailureKind, ProviderResult


def validate_service_capabilities(value: dict[str, Any]) -> dict[str, Any]:
    """Require the exact useful-but-non-authoritative factory identity."""

    identity = value.get("identity") if isinstance(value, dict) else None
    credential = value.get("credential") if isinstance(value, dict) else None
    capabilities = value.get("capabilities") if isinstance(value, dict) else None
    if (
        value.get("schemaVersion") != 1
        or not isinstance(identity, dict)
        or not isinstance(credential, dict)
        or not isinstance(capabilities, dict)
    ):
        raise ValueError("Snaketron factory capability envelope has an unsupported schema")
    required_identity = {
        "registeredAccount": True,
        "isGuest": False,
        "isAdmin": False,
    }
    required_capabilities = {
        "createPrivateSkins": True,
        "createEvaluationSkins": True,
        "uploadPrivateForgeTextures": True,
        "requestPublicationReview": True,
        "publishSkins": False,
        "administerSkins": False,
    }
    required_credential = {
        "credentialType": "factoryService",
        "revocable": True,
        "expiresAt": None,
    }
    identity_drift = {
        key: identity.get(key) for key, expected in required_identity.items() if identity.get(key) is not expected
    }
    capability_drift = {
        key: capabilities.get(key)
        for key, expected in required_capabilities.items()
        if capabilities.get(key) is not expected
    }
    credential_drift = {
        key: credential.get(key) for key, expected in required_credential.items() if credential.get(key) != expected
    }
    credential_id = credential.get("credentialId")
    if not isinstance(credential_id, str) or re.fullmatch(r"[0-9a-f]{32}", credential_id) is None:
        credential_drift["credentialId"] = credential_id
    if identity_drift or credential_drift or capability_drift:
        raise PermissionError(
            "service token is not the required registered, non-admin factory identity; "
            f"identity mismatches={sorted(identity_drift)}, credential mismatches={sorted(credential_drift)}, "
            f"capability mismatches={sorted(capability_drift)}"
        )
    return value


class SnaketronApi:
    def __init__(
        self,
        config: FactoryConfig,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.config = config
        self.base_url = config.service.base_url.rstrip("/")
        self.client = client or httpx.AsyncClient(timeout=config.service.request_timeout_seconds)
        self._owns_client = client is None

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    def _token(self, operator: bool = False) -> str:
        name = self.config.service.operator_token_env if operator else self.config.service.service_token_env
        value = os.environ.get(name)
        if not value:
            raise ProviderError(ProviderFailureKind.UNAVAILABLE, f"{name} is not set")
        return value

    def _headers(self, *, operator: bool = False) -> dict[str, str]:
        return {"authorization": f"Bearer {self._token(operator)}"}

    async def health(self) -> dict[str, Any]:
        response = await self._request("GET", "/health", authenticated=False)
        try:
            return response.json()
        except ValueError:
            return {"status": response.text.strip() or "ok"}

    async def service_capabilities(self) -> dict[str, Any]:
        """Read the DB-derived service envelope without creating a canary."""

        response = await self._request(
            "GET",
            "/api/factory/capabilities",
            headers=self._headers(),
        )
        try:
            value = response.json()
        except ValueError as error:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "Snaketron factory capability response is not JSON",
                request_id=self._request_id(response),
            ) from error
        if not isinstance(value, dict):
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "Snaketron factory capability response is not an object",
                request_id=self._request_id(response),
            )
        return value

    async def upload_forge_bundle(self, bundle: ForgeBundle) -> ProviderResult:
        if not bundle.variants:
            raise ValueError("cannot upload an empty forge bundle")
        manifest_text = json.dumps(bundle.manifest, sort_keys=True, separators=(",", ":"))
        files: list[tuple[str, tuple[str, bytes | str, str]]] = [
            ("manifest", ("manifest.json", manifest_text, "application/json"))
        ]
        files.extend(
            (
                "variant",
                (f"{variant.content_ref}.png", variant.data, "image/png"),
            )
            for variant in bundle.variants
        )
        response = await self._request(
            "POST",
            "/api/textures/forge",
            headers=self._headers(),
            files=files,
        )
        value = response.json()
        return self._result(response, value)

    async def verify_forge_bundle(self, bundle: ForgeBundle) -> ProviderResult:
        verified: list[dict[str, Any]] = []
        for variant in bundle.variants:
            response = await self._request(
                "GET", f"/api/textures/variants/{variant.content_ref}.png", authenticated=False
            )
            data = response.content
            digest = f"sha256:{hashlib.sha256(data).hexdigest()}"
            if digest != variant.content_ref:
                raise ProviderError(
                    ProviderFailureKind.INVALID_OUTPUT,
                    f"stored variant {variant.content_ref} returned bytes named {digest}",
                    request_id=self._request_id(response),
                )
            if data != variant.data:
                raise ProviderError(
                    ProviderFailureKind.INVALID_OUTPUT,
                    f"stored variant {variant.content_ref} was re-encoded",
                    request_id=self._request_id(response),
                )
            verified.append({"content_ref": digest, "bytes": len(data)})
        return ProviderResult(
            value={"variants": verified},
            request_id=None,
            resolved_model="snaketron-texture-store-v1",
            sanitized_metadata={},
            usage={"cost_micros": 0},
        )

    async def create_skin(
        self,
        *,
        name: str,
        document: dict[str, Any],
        idempotency_key: str,
        evaluation_only: bool = False,
    ) -> ProviderResult:
        response = await self._request(
            "POST",
            "/api/skins",
            headers=self._headers(),
            json={
                "name": name,
                "kind": "snake",
                "document": document,
                "idempotencyKey": idempotency_key,
                "evaluationOnly": evaluation_only,
            },
        )
        return self._result(response, response.json())

    async def append_revision(
        self,
        *,
        skin_id: str | int,
        document: dict[str, Any],
        expected_head_revision: int,
    ) -> ProviderResult:
        response = await self._request(
            "PUT",
            f"/api/skins/{skin_id}",
            headers=self._headers(),
            json={
                "document": document,
                "expectedHeadRevision": expected_head_revision,
            },
        )
        return self._result(response, response.json())

    async def publish_exact(
        self,
        *,
        skin_id: str | int,
        revision: int,
        content_ref: str,
        reason: str = "Skin Factory human approval",
    ) -> ProviderResult:
        response = await self._request(
            "PUT",
            f"/api/admin/skins/{skin_id}/status",
            headers=self._headers(operator=True),
            json={
                "publication": "published",
                "revision": revision,
                "contentRef": content_ref,
                "reason": reason,
            },
        )
        return self._result(response, response.json())

    async def request_publication_exact(
        self,
        *,
        skin_id: str | int,
        revision: int,
        content_ref: str,
        operator: bool = False,
    ) -> ProviderResult:
        """Open review for one immutable production revision.

        The production loop uses service identity. A human may use operator
        identity to reopen the same exact request after overriding soft triage.
        The server checks the immutable revision/hash in either case.
        """
        response = await self._request(
            "POST",
            f"/api/skins/{skin_id}/publish-request",
            headers=self._headers(operator=operator),
            json={"revision": revision, "contentRef": content_ref},
        )
        return self._result(response, response.json() if response.content else {"accepted": True})

    async def cancel_publication_request_exact(
        self,
        *,
        skin_id: str | int,
        revision: int,
        content_ref: str,
    ) -> ProviderResult:
        """Withdraw only the exact request a human just rejected."""
        response = await self._request(
            "DELETE",
            f"/api/skins/{skin_id}/publish-request",
            # Rejection is an explicit human/operator decision. The scheduler
            # service identity never receives this credential.
            headers=self._headers(operator=True),
            json={"revision": revision, "contentRef": content_ref},
        )
        return self._result(response, response.json() if response.content else {"cancelled": True})

    async def get_skin_authority(
        self,
        skin_id: str | int,
        *,
        operator: bool = False,
    ) -> ProviderResult:
        """Read the server's current immutable revision/review authority."""

        response = await self._request(
            "GET",
            f"/api/skins/{skin_id}",
            headers=self._headers(operator=operator),
        )
        try:
            value = response.json()
        except ValueError as error:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "Snaketron skin authority readback is not JSON",
                request_id=self._request_id(response),
            ) from error
        if not isinstance(value, dict):
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "Snaketron skin authority readback is not an object",
                request_id=self._request_id(response),
            )
        return self._result(response, value)

    async def get_skin_document(self, content_ref: str, *, as_operator: bool = False) -> bytes:
        response = await self._request(
            "GET",
            f"/api/skins/by-ref/{content_ref}",
            headers=self._headers(operator=as_operator),
        )
        return response.content

    async def _request(
        self,
        method: str,
        path: str,
        *,
        authenticated: bool = True,
        **kwargs: Any,
    ) -> httpx.Response:
        if authenticated and "headers" not in kwargs:
            kwargs["headers"] = self._headers()
        safe_method = method.upper() in {"GET", "HEAD", "OPTIONS"}
        try:
            response = await self.client.request(method, f"{self.base_url}{path}", **kwargs)
        except httpx.ConnectError as error:
            raise ProviderError(ProviderFailureKind.UNAVAILABLE, f"Snaketron API connection failed: {error}") from error
        except httpx.TimeoutException as error:
            known = safe_method or isinstance(error, (httpx.ConnectTimeout, httpx.PoolTimeout))
            raise ProviderError(
                ProviderFailureKind.TIMEOUT,
                f"Snaketron API timeout: {error}",
                outcome_known=known,
            ) from error
        except httpx.TransportError as error:
            raise ProviderError(
                ProviderFailureKind.UNAVAILABLE if safe_method else ProviderFailureKind.UNKNOWN_OUTCOME,
                f"Snaketron API transport outcome is unknown: {error}",
                outcome_known=safe_method,
            ) from error
        if response.is_success:
            return response
        request_id = self._request_id(response)
        if response.status_code == 429:
            kind = ProviderFailureKind.QUOTA
        elif response.status_code in {401, 403}:
            # Authentication/authorization rejection proves the requested
            # mutation did not execute. A human may refresh the short-lived
            # operator credential and explicitly retry the exact authority.
            kind = ProviderFailureKind.AUTHENTICATION
        elif response.status_code == 408 or response.status_code >= 500:
            kind = ProviderFailureKind.UNAVAILABLE
        elif response.status_code in {400, 404, 409, 410, 422}:
            kind = ProviderFailureKind.INVALID_OUTPUT
        else:
            kind = ProviderFailureKind.REFUSAL
        raise ProviderError(
            kind,
            f"Snaketron API HTTP {response.status_code}: {response.text[:1_000]}",
            outcome_known=(
                safe_method
                or (response.status_code != 429 and response.status_code != 408 and response.status_code < 500)
            ),
            request_id=request_id,
        )

    @staticmethod
    def _request_id(response: httpx.Response) -> str | None:
        return response.headers.get("x-request-id") or response.headers.get("x-amzn-requestid")

    def _result(self, response: httpx.Response, value: Any) -> ProviderResult:
        return ProviderResult(
            value=value,
            request_id=self._request_id(response),
            resolved_model=response.headers.get("x-snaketron-version", "snaketron-api"),
            sanitized_metadata={
                key.lower(): value
                for key, value in response.headers.items()
                if key.lower() in {"etag", "date", "x-request-id", "x-amzn-requestid"}
            },
            usage={"cost_micros": 0},
        )
