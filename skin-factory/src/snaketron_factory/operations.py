"""Exactly-once local intents around at-most-once external effects."""

from __future__ import annotations

import hashlib
import inspect
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, TypeVar

from .db import Database, canonical_json
from .domain import OperationStatus, ProviderError, ProviderFailureKind, ProviderResult
from .persistence import SUPPORTED_IMAGE_MEDIA_TYPES

T = TypeVar("T")


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
        metadata: Mapping[str, Any] | None = None,
    ) -> tuple[dict[str, Any], ProviderResult | None]:
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
            result_metadata = self._result_metadata(result)
        except ProviderError as error:
            if not error.outcome_known or error.kind == ProviderFailureKind.UNKNOWN_OUTCOME:
                status = OperationStatus.RECONCILIATION_REQUIRED
                retry_class = "unknown"
                charged = reserve_micros
            elif error.kind in {
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
                retry_class=retry_class,
                cost_charged_micros=charged,
                failure_json={"kind": error.kind, "message": str(error)},
            )
            raise
        except BaseException as error:
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

        # Persist the returned value before committing success. A crash before
        # this point leaves RUNNING and therefore requires reconciliation. A
        # crash after it leaves a content hash from which the next run can
        # reconstruct any not-yet-created Artifact row.
        result_hash = persist_result(result) if persist_result else self.request_hash(result.value)
        charged = self._estimate_charge(result, reserve_micros)
        operation = self.database.transition_operation(
            operation["id"],
            OperationStatus.RUNNING,
            OperationStatus.SUCCEEDED,
            provider_request_id=result.request_id,
            resolved_model=result.resolved_model,
            retry_class="complete",
            result_hash=result_hash,
            metadata_json={**result.sanitized_metadata, **result_metadata, "usage": result.usage},
            cost_charged_micros=charged,
        )
        return operation, result

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
        return {"result": {"kind": "image", "media_type": media_type}}

    @staticmethod
    def _estimate_charge(result: ProviderResult, reservation: int) -> int:
        reported = result.usage.get("cost_micros")
        if isinstance(reported, int) and reported >= 0:
            return reported
        return reservation
