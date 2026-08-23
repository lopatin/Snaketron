"""Exactly-once local intents around at-most-once external effects."""

from __future__ import annotations

import hashlib
import inspect
import io
import json
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, TypeVar

from PIL import Image, UnidentifiedImageError

from .db import Database, canonical_json
from .domain import OperationStatus, ProviderError, ProviderFailureKind, ProviderResult
from .persistence import SUPPORTED_IMAGE_MEDIA_TYPES

T = TypeVar("T")


def validate_exact_image_bytes(value: bytes, media_type: str) -> dict[str, Any]:
    """Decode every retained image byte and return trusted media metadata.

    Both live provider responses and authenticated reconciliation use this
    exact validator. A reported content type is evidence only after Pillow
    verifies and fully decodes the immutable CAS bytes.
    """

    if media_type not in SUPPORTED_IMAGE_MEDIA_TYPES:
        raise ProviderError(
            ProviderFailureKind.INVALID_OUTPUT,
            "provider image result omitted a supported media_type",
        )
    expected_formats = {
        "image/png": "PNG",
        "image/jpeg": "JPEG",
        "image/webp": "WEBP",
    }
    try:
        # ``verify`` checks container integrity without decoding pixels. A
        # second open plus ``load`` proves the complete raster is decodable.
        with Image.open(io.BytesIO(value)) as verified:
            width, height = verified.size
            actual_format = str(verified.format or "").upper()
            if width <= 0 or height <= 0 or width > 8192 or height > 8192 or width * height > 33_554_432:
                raise ValueError(f"unsafe decoded image dimensions {width}x{height}")
            verified.verify()
        with Image.open(io.BytesIO(value)) as decoded:
            decoded.load()
            if decoded.size != (width, height) or str(decoded.format or "").upper() != actual_format:
                raise ValueError("image identity changed between verification and full decode")
    except (OSError, UnidentifiedImageError, ValueError) as error:
        raise ProviderError(
            ProviderFailureKind.INVALID_OUTPUT,
            f"provider image bytes failed exact decode validation: {error}",
        ) from error
    if actual_format != expected_formats[media_type]:
        raise ProviderError(
            ProviderFailureKind.INVALID_OUTPUT,
            f"provider image media type {media_type} does not match decoded {actual_format or 'unknown'} bytes",
        )
    return {
        "kind": "image",
        "media_type": media_type,
        "width_px": width,
        "height_px": height,
        "decoded_format": actual_format,
    }


class ExistingOperation(RuntimeError):
    """An operation exists but has no reusable terminal result."""


class OperationJournal:
    def __init__(self, database: Database) -> None:
        self.database = database

    @staticmethod
    def request_hash(request: Any) -> str:
        if hasattr(request, "model_dump"):
            request = request.model_dump(mode="json")
        request = OperationJournal._hashable(request)
        payload = canonical_json(request).encode()
        return hashlib.sha256(payload).hexdigest()

    @staticmethod
    def request_payload(request: Any) -> bytes:
        """Canonical replay input; binary values are named by exact hash/size."""

        if hasattr(request, "model_dump"):
            request = request.model_dump(mode="json")
        return canonical_json(OperationJournal._hashable(request)).encode("utf-8")

    @staticmethod
    def _hashable(value: Any) -> Any:
        if isinstance(value, bytes):
            return {
                "$bytes_sha256": hashlib.sha256(value).hexdigest(),
                "$bytes_length": len(value),
            }
        if isinstance(value, dict):
            return {str(key): OperationJournal._hashable(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [OperationJournal._hashable(item) for item in value]
        if hasattr(value, "model_dump"):
            return OperationJournal._hashable(value.model_dump(mode="json"))
        return value

    async def run_provider(
        self,
        *,
        attempt_id: str,
        stage: str,
        idempotency_key: str,
        side_effect: str,
        provider_role: str,
        request: Any,
        reserve_micros: int,
        invoke: Callable[[], Awaitable[ProviderResult] | ProviderResult],
        persist_result: Callable[[ProviderResult], str] | None = None,
        validate_result: Callable[[ProviderResult], None] | None = None,
        metadata: Mapping[str, Any] | None = None,
        repeatable_read: bool = False,
        repeatable_local: bool = False,
    ) -> tuple[dict[str, Any], ProviderResult | None]:
        if repeatable_read and repeatable_local:
            raise ValueError("an operation cannot be both a repeatable provider read and local computation")
        repeatable = repeatable_read or repeatable_local
        if repeatable and reserve_micros != 0:
            raise ValueError("repeatable reads/local computations must have a zero cost reservation")
        repeatable_kind = "local_computation" if repeatable_local else "provider_read"
        digest = self.request_hash(request)
        operation, created = self.database.begin_operation(
            attempt_id=attempt_id,
            stage=stage,
            idempotency_key=idempotency_key,
            side_effect=side_effect,
            provider_role=provider_role,
            request_hash=digest,
            cost_reserved_micros=reserve_micros,
            metadata=metadata,
        )
        if not created:
            status = OperationStatus(operation["status"])
            if status == OperationStatus.SUCCEEDED:
                return operation, None
            if status == OperationStatus.INTENT:
                # The transaction committed the reservation, but no adapter
                # call started. Resuming this exact intent is safe.
                created = True
            elif status == OperationStatus.RUNNING and repeatable:
                retained_metadata = json.loads(operation.get("metadata_json") or "{}")
                operation = self.database.transition_operation(
                    operation["id"],
                    OperationStatus.RUNNING,
                    OperationStatus.INTENT,
                    retry_class=f"repeatable_{repeatable_kind}_resume",
                    cost_charged_micros=0,
                    metadata_json={
                        **retained_metadata,
                        "crash_recovery": {
                            "kind": repeatable_kind,
                            "request_hash": digest,
                        },
                    },
                )
                created = True
            elif status == OperationStatus.RUNNING:
                operation = self.database.transition_operation(
                    operation["id"],
                    OperationStatus.RUNNING,
                    OperationStatus.RECONCILIATION_REQUIRED,
                    retry_class="unknown",
                    cost_charged_micros=operation["cost_reserved_micros"],
                    failure_json={
                        "kind": "crash_during_external_call",
                        "message": "process restarted while provider operation was running",
                    },
                )
                raise ExistingOperation(
                    f"operation {operation['id']} requires reconciliation after an interrupted call"
                )
            elif status == OperationStatus.RECONCILIATION_REQUIRED:
                raise ExistingOperation(f"operation {operation['id']} requires authenticated reconciliation")
            elif status == OperationStatus.RESOLVED and operation["retry_class"] != "retry_safe":
                raise ExistingOperation(f"resolved operation {operation['id']} is terminal")
            elif status != OperationStatus.INTENT:
                raise ExistingOperation(
                    f"operation key {idempotency_key} is already terminal; create a numbered retry key"
                )

        operation = self.database.transition_operation(operation["id"], OperationStatus.INTENT, OperationStatus.RUNNING)
        try:
            result = invoke()
            if inspect.isawaitable(result):
                result = await result
            assert isinstance(result, ProviderResult)
        except ProviderError as error:
            if repeatable and error.kind in {
                ProviderFailureKind.AUTHENTICATION,
                ProviderFailureKind.TIMEOUT,
                ProviderFailureKind.UNAVAILABLE,
                ProviderFailureKind.QUOTA,
                ProviderFailureKind.UNKNOWN_OUTCOME,
            }:
                status = OperationStatus.FAILED_RETRYABLE
                retry_class = "safe_new_key"
                charged = 0
            elif not error.outcome_known or error.kind == ProviderFailureKind.UNKNOWN_OUTCOME:
                status = OperationStatus.RECONCILIATION_REQUIRED
                retry_class = "unknown"
                charged = reserve_micros
            elif error.kind in {
                ProviderFailureKind.AUTHENTICATION,
                ProviderFailureKind.TIMEOUT,
                ProviderFailureKind.UNAVAILABLE,
                ProviderFailureKind.QUOTA,
            }:
                status = OperationStatus.FAILED_RETRYABLE
                retry_class = "safe_new_key"
                charged = 0
            else:
                status = OperationStatus.FAILED_TERMINAL
                retry_class = "terminal"
                charged = reserve_micros
            operation = self.database.transition_operation(
                operation["id"],
                OperationStatus.RUNNING,
                status,
                provider_request_id=error.request_id,
                resolved_model=error.resolved_model,
                retry_class=retry_class,
                cost_charged_micros=charged,
                failure_json={"kind": error.kind, "message": str(error)},
            )
            raise
        except BaseException as error:
            if repeatable:
                operation = self.database.transition_operation(
                    operation["id"],
                    OperationStatus.RUNNING,
                    OperationStatus.FAILED_RETRYABLE,
                    retry_class="safe_new_key",
                    cost_charged_micros=0,
                    failure_json={"kind": f"repeatable_{repeatable_kind}_exception", "message": str(error)},
                )
                raise ProviderError(
                    ProviderFailureKind.UNAVAILABLE,
                    f"repeatable {repeatable_kind} failed safely for operation {operation['id']}: {error}",
                    outcome_known=True,
                ) from error
            # Once control entered an adapter, an arbitrary transport/process
            # exception cannot prove the provider did not accept the request.
            # This is the expensive boundary the reconciliation workflow owns.
            operation = self.database.transition_operation(
                operation["id"],
                OperationStatus.RUNNING,
                OperationStatus.RECONCILIATION_REQUIRED,
                retry_class="unknown",
                cost_charged_micros=reserve_micros,
                failure_json={"kind": "unknown_exception", "message": str(error)},
            )
            raise ProviderError(
                ProviderFailureKind.UNKNOWN_OUTCOME,
                f"provider outcome unknown for operation {operation['id']}: {error}",
                outcome_known=False,
            ) from error

        # Persist every returned value before interpreting or accepting it.
        # This includes provider fallbacks and malformed payloads: they remain
        # quarantined failed-operation evidence and can never be consumed, but
        # paid bytes and typed payloads are not silently discarded. A crash
        # before persistence leaves RUNNING and requires reconciliation; a
        # crash after it leaves an exact content hash for operator recovery.
        result_hash = persist_result(result) if persist_result else self.request_hash(result.value)
        charged = self._estimate_charge(result, reserve_micros)
        intent_metadata = json.loads(operation["metadata_json"] or "{}")
        try:
            result_metadata = self._result_metadata(result)
            if validate_result is not None:
                validate_result(result)
        except ProviderError as error:
            operation = self.database.transition_operation(
                operation["id"],
                OperationStatus.RUNNING,
                OperationStatus.FAILED_TERMINAL,
                provider_request_id=result.request_id,
                resolved_model=result.resolved_model,
                retry_class="terminal",
                result_hash=result_hash,
                metadata_json={
                    **intent_metadata,
                    **result.sanitized_metadata,
                    **self._quarantined_result_metadata(result),
                    "usage": result.usage,
                    "quarantined": True,
                },
                failure_json={
                    "kind": error.kind,
                    "message": str(error),
                    "quarantined_result_hash": result_hash,
                },
                cost_charged_micros=charged,
            )
            raise
        operation = self.database.transition_operation(
            operation["id"],
            OperationStatus.RUNNING,
            OperationStatus.SUCCEEDED,
            provider_request_id=result.request_id,
            resolved_model=result.resolved_model,
            retry_class="complete",
            result_hash=result_hash,
            metadata_json={
                **intent_metadata,
                **result.sanitized_metadata,
                **result_metadata,
                "usage": result.usage,
            },
            cost_charged_micros=charged,
        )
        return operation, result

    @staticmethod
    def _quarantined_result_metadata(result: ProviderResult) -> dict[str, Any]:
        """Describe a rejected result without trusting its media contract."""

        value = result.value
        if not isinstance(value, dict) or not isinstance(value.get("image"), bytes):
            return {"result": {"kind": "structured"}}
        media_type = value.get("media_type")
        metadata: dict[str, Any] = {"kind": "image"}
        if isinstance(media_type, str):
            metadata["reported_media_type"] = media_type
        return {"result": metadata}

    @staticmethod
    def _result_metadata(result: ProviderResult) -> dict[str, Any]:
        """Retain the type needed to reconstruct binary provider results."""

        value = result.value
        if not isinstance(value, dict) or not isinstance(value.get("image"), bytes):
            return {}
        media_type = value.get("media_type")
        if media_type not in SUPPORTED_IMAGE_MEDIA_TYPES:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "provider image result omitted a supported media_type",
            )
        # Fake adapters deliberately use symbolic bytes in unit tests. Real
        # adapter results must decode completely before semantic success; the
        # journal already persisted the exact response, so failures below are
        # retained as quarantined terminal evidence.
        if result.sanitized_metadata.get("fake") is True:
            return {"result": {"kind": "image", "media_type": media_type}}
        try:
            return {"result": validate_exact_image_bytes(value["image"], media_type)}
        except ProviderError as error:
            raise ProviderError(
                error.kind,
                str(error),
                request_id=result.request_id,
                resolved_model=result.resolved_model,
            ) from error

    @staticmethod
    def _estimate_charge(result: ProviderResult, reservation: int) -> int:
        reported = result.usage.get("cost_micros")
        if isinstance(reported, int) and reported >= 0:
            return reported
        return reservation
