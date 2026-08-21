"""At-least-once transactional-outbox delivery with immutable attempt history."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

import httpx

from .config import FactoryConfig
from .db import Database
from .objects import ObjectStore


@dataclass(frozen=True)
class DeliveryResult:
    delivered: bool
    permanent: bool = False
    response_code: int | None = None
    error: str | None = None


class OutboxDestination(Protocol):
    async def deliver(self, message: Mapping[str, object], payload: bytes) -> DeliveryResult: ...


class LocalGalleryDestination:
    """Gallery data is already committed locally; CAS verification is delivery."""

    async def deliver(self, message: Mapping[str, object], payload: bytes) -> DeliveryResult:
        del message, payload
        return DeliveryResult(delivered=True)


class WebhookDestination:
    def __init__(
        self,
        url: str,
        *,
        token: str | None,
        timeout_seconds: float,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.url = url
        self.token = token
        self.client = client or httpx.AsyncClient(timeout=timeout_seconds)
        self._owns_client = client is None

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def deliver(self, message: Mapping[str, object], payload: bytes) -> DeliveryResult:
        headers = {
            "content-type": "application/json",
            "idempotency-key": str(message["idempotency_key"]),
            "x-snaketron-event-ref": str(message["event_ref"]),
        }
        if self.token:
            headers["authorization"] = f"Bearer {self.token}"
        try:
            response = await self.client.post(self.url, content=payload, headers=headers)
        except (httpx.TimeoutException, httpx.NetworkError) as error:
            return DeliveryResult(False, error=f"{type(error).__name__}: {error}")
        if response.is_success or response.status_code == 409:
            # A receiver may use 409 to report the already-consumed
            # Idempotency-Key; that is a successful at-least-once delivery.
            return DeliveryResult(True, response_code=response.status_code)
        detail = response.text[:2_000]
        transient = response.status_code in {408, 425, 429, 500, 502, 503, 504}
        return DeliveryResult(
            False,
            permanent=not transient,
            response_code=response.status_code,
            error=f"HTTP {response.status_code}: {detail}",
        )


class OutboxDispatcher:
    def __init__(
        self,
        database: Database,
        objects: ObjectStore,
        config: FactoryConfig,
        *,
        destinations: Mapping[str, OutboxDestination] | None = None,
    ) -> None:
        self.database = database
        self.objects = objects
        self.config = config
        self._owned: list[object] = []
        if destinations is not None:
            self.destinations = dict(destinations)
            return
        webhook_url = config.outbox.webhook_url()
        if webhook_url:
            webhook = WebhookDestination(
                webhook_url,
                token=config.outbox.webhook_token(),
                timeout_seconds=config.outbox.request_timeout_seconds,
            )
            self.destinations = {"review_gallery": webhook, "webhook": webhook}
            self._owned.append(webhook)
        else:
            self.destinations = {"review_gallery": LocalGalleryDestination()}

    async def close(self) -> None:
        for destination in self._owned:
            close = getattr(destination, "close", None)
            if close:
                await close()

    async def dispatch_due(self, *, at: datetime | None = None) -> dict[str, object]:
        timestamp = at or datetime.now(UTC)
        report: dict[str, object] = {
            "examined": 0,
            "delivered": 0,
            "retried": 0,
            "dead_lettered": 0,
            "messages": [],
        }
        try:
            for message in self.database.pending_outbox(
                self.config.outbox.batch_size,
                at=timestamp.isoformat(timespec="microseconds"),
            ):
                report["examined"] = int(report["examined"]) + 1
                status = await self._dispatch(message, timestamp)
                key = {
                    "delivered": "delivered",
                    "retry": "retried",
                    "dead_letter": "dead_lettered",
                }[status]
                report[key] = int(report[key]) + 1
                assert isinstance(report["messages"], list)
                report["messages"].append({"id": message["id"], "status": status})
            return report
        finally:
            await self.close()

    async def _dispatch(self, message: dict[str, object], timestamp: datetime) -> str:
        destination = self.destinations.get(str(message["destination"]))
        if destination is None:
            result = DeliveryResult(
                False,
                permanent=True,
                error=f"unknown outbox destination {message['destination']}",
            )
        else:
            try:
                payload = self.objects.get(str(message["payload_ref"]))
                digest = hashlib.sha256(payload).hexdigest()
                if digest != message["payload_hash"]:
                    raise RuntimeError(f"payload hash mismatch: expected {message['payload_hash']}, got {digest}")
            except (OSError, RuntimeError, ValueError) as error:
                result = DeliveryResult(False, permanent=True, error=str(error))
            else:
                try:
                    result = await destination.deliver(message, payload)
                except Exception as error:  # adapters are an isolation boundary
                    result = DeliveryResult(False, error=f"{type(error).__name__}: {error}")

        attempts_after = int(message["attempts"]) + 1
        if result.delivered:
            status = "delivered"
            outcome = "delivered"
            next_attempt_at = None
        elif result.permanent:
            status = "dead_letter"
            outcome = "permanent_failure"
            next_attempt_at = None
        elif attempts_after >= self.config.outbox.max_attempts:
            status = "dead_letter"
            outcome = "attempts_exhausted"
            next_attempt_at = None
        else:
            status = "retry"
            outcome = "transient_failure"
            delay = min(
                self.config.outbox.max_backoff_seconds,
                self.config.outbox.initial_backoff_seconds * 2 ** (attempts_after - 1),
            )
            next_attempt_at = (timestamp + timedelta(seconds=delay)).isoformat(timespec="microseconds")
        self.database.record_outbox_delivery(
            str(message["id"]),
            int(message["version"]),
            status=status,
            outcome=outcome,
            error=result.error,
            response_code=result.response_code,
            next_attempt_at=next_attempt_at,
        )
        return status
