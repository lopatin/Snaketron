"""Exact, journal-friendly fal.ai media operations for interactive skin hosts.

The paid queue submit is intentionally separate from repeatable polling and
download.  Callers persist ``submit_transition`` through ``OperationJournal``
before they start polling, so a poll timeout or process restart never implies
another paid submission.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import inspect
import ipaddress
import json
import math
import os
import re
import socket
import struct
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any, ClassVar, Literal
from urllib.parse import quote, urlsplit

import httpx

from .domain import ProviderError, ProviderFailureKind, ProviderResult
from .operations import validate_exact_image_bytes

PIXVERSE_TRANSITION_CAPABILITY = "fal-ai/pixverse/v6/transition"
_CONTENT_HASH = re.compile(r"^sha256:[a-f0-9]{64}$")
_REQUEST_ID = re.compile(r"^[A-Za-z0-9_-]{1,200}$")
_PROMPT_SECTIONS = (
    "[Cinematography]",
    "[Subject]",
    "[Action / Transition]",
    "[Context]",
    "[Style & Ambiance]",
)
_JOURNAL_CONTRACT = {
    "retain_inputs": True,
    "retain_provider_output": True,
    "retain_output": True,
    "retain_reports": True,
}
_RESOLUTIONS = frozenset({"360p", "540p", "720p", "1080p"})
_ASPECT_RATIOS = frozenset({"16:9", "4:3", "1:1", "3:4", "9:16", "2:3", "3:2", "21:9"})
_SAFE_RESPONSE_HEADERS = frozenset({"date", "etag", "x-fal-request-id", "x-request-id"})
_MAX_FRAME_BYTES = 16 * 1024 * 1024
_DOWNLOAD_DOMAIN = "fal.media"
_MAX_MP4_TOP_LEVEL_BOXES = 4_096
_MAX_RETAINED_BOX_NAMES = 128
_PIXVERSE_V6_NO_AUDIO_MICROS_PER_SECOND = {
    "360p": 25_000,
    "540p": 35_000,
    "720p": 45_000,
    "1080p": 90_000,
}
_PIXVERSE_PRICING_VERSION = "fal-pixverse-v6-2026-08-23"

HostResolver = Callable[[str], Sequence[str] | Awaitable[Sequence[str]]]


@dataclass(frozen=True, slots=True)
class PixVerseTransitionOptions:
    """Provider controls that must be explicit in the operation journal."""

    seed: int
    reservation_micros: int
    duration_seconds: int = 5
    resolution: Literal["360p", "540p", "720p", "1080p"] = "720p"
    aspect_ratio: Literal["16:9", "4:3", "1:1", "3:4", "9:16", "2:3", "3:2", "21:9"] = "3:2"

    def __post_init__(self) -> None:
        if isinstance(self.seed, bool) or not isinstance(self.seed, int) or not 0 <= self.seed <= 2_147_483_647:
            raise ValueError("PixVerse seed must be an integer from 0 through 2147483647")
        if (
            isinstance(self.duration_seconds, bool)
            or not isinstance(self.duration_seconds, int)
            or not 1 <= self.duration_seconds <= 15
        ):
            raise ValueError("PixVerse duration_seconds must be an integer from 1 through 15")
        if self.resolution not in _RESOLUTIONS:
            raise ValueError(f"unsupported PixVerse resolution {self.resolution!r}")
        if self.aspect_ratio not in _ASPECT_RATIOS:
            raise ValueError(f"unsupported PixVerse aspect ratio {self.aspect_ratio!r}")
        required = _PIXVERSE_V6_NO_AUDIO_MICROS_PER_SECOND[self.resolution] * self.duration_seconds
        if (
            isinstance(self.reservation_micros, bool)
            or not isinstance(self.reservation_micros, int)
            or self.reservation_micros < required
        ):
            raise ValueError(
                f"PixVerse reservation_micros must cover at least {required} micros under {_PIXVERSE_PRICING_VERSION}"
            )


@dataclass(frozen=True, slots=True)
class FalQueueTicket:
    """The minimal immutable result of one accepted paid queue submission."""

    schema_version: int
    capability_id: str
    request_id: str

    @classmethod
    def from_value(cls, value: Mapping[str, Any] | FalQueueTicket) -> FalQueueTicket:
        if isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise ProviderError(ProviderFailureKind.INVALID_OUTPUT, "Fal queue ticket must be an object")
        if set(value) != {"schema_version", "capability_id", "request_id"}:
            raise ProviderError(ProviderFailureKind.INVALID_OUTPUT, "Fal queue ticket has unexpected fields")
        ticket = cls(
            schema_version=value.get("schema_version"),
            capability_id=value.get("capability_id"),
            request_id=value.get("request_id"),
        )
        ticket.validate()
        return ticket

    def validate(self) -> None:
        if self.schema_version != 1 or self.capability_id != PIXVERSE_TRANSITION_CAPABILITY:
            raise ProviderError(ProviderFailureKind.INVALID_OUTPUT, "Fal queue ticket has the wrong capability")
        if not isinstance(self.request_id, str) or _REQUEST_ID.fullmatch(self.request_id) is None:
            raise ProviderError(ProviderFailureKind.INVALID_OUTPUT, "Fal queue ticket has an invalid request_id")

    def as_dict(self) -> dict[str, Any]:
        self.validate()
        return asdict(self)


class FalQueuePending(ProviderError):
    """A retained queue ticket is valid but not terminal on this read."""

    def __init__(self, *, request_id: str, status: str, polls: int) -> None:
        super().__init__(
            ProviderFailureKind.TIMEOUT,
            f"Fal transition remains {status} after {polls} scheduled status poll(s)",
            request_id=request_id,
            resolved_model=PIXVERSE_TRANSITION_CAPABILITY,
        )
        self.status = status
        self.polls = polls


@dataclass(frozen=True, slots=True)
class _ValidatedInputs:
    prompt: str
    prompt_sha256: str
    start_sha256: str
    end_sha256: str
    start_media: dict[str, Any]
    end_media: dict[str, Any]


class FalPixVerseTransitionAdapter:
    """One exact adapter for ``fal-ai/pixverse/v6/transition``.

    The adapter never reads a shell startup file.  It reads the configured
    environment variable at call time, preferring ``FAL_API_KEY`` for this
    repository and accepting Fal's documented ``FAL_KEY`` as a fallback.
    """

    capability_id: ClassVar[str] = PIXVERSE_TRANSITION_CAPABILITY

    def __init__(
        self,
        *,
        api_key_env: str = "FAL_API_KEY",
        fallback_api_key_envs: tuple[str, ...] = ("FAL_KEY",),
        queue_base_url: str = "https://queue.fal.run",
        timeout_seconds: float = 60,
        poll_deadline_seconds: float = 900,
        max_status_polls: int = 300,
        poll_interval_seconds: float = 2,
        max_video_bytes: int = 100_000_000,
        api_client: httpx.AsyncClient | None = None,
        download_client: httpx.AsyncClient | None = None,
        host_resolver: HostResolver | None = None,
    ) -> None:
        if not _valid_env_name(api_key_env) or any(not _valid_env_name(name) for name in fallback_api_key_envs):
            raise ValueError("Fal credential environment names must be shell-safe variable names")
        parsed_base = urlsplit(queue_base_url)
        if parsed_base.scheme != "https" or not parsed_base.hostname or parsed_base.username or parsed_base.password:
            raise ValueError("Fal queue_base_url must be an HTTPS origin without credentials")
        if parsed_base.path not in {"", "/"} or parsed_base.query or parsed_base.fragment:
            raise ValueError("Fal queue_base_url must not contain a path, query, or fragment")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if poll_deadline_seconds <= 0 or poll_deadline_seconds > 3_600:
            raise ValueError("poll_deadline_seconds must be from greater than 0 through 3600")
        if isinstance(max_status_polls, bool) or not 1 <= max_status_polls <= 3_600:
            raise ValueError("max_status_polls must be from 1 through 3600")
        if poll_interval_seconds < 0 or poll_interval_seconds > 60:
            raise ValueError("poll_interval_seconds must be from 0 through 60")
        if isinstance(max_video_bytes, bool) or not 1 <= max_video_bytes <= 500_000_000:
            raise ValueError("max_video_bytes must be from 1 through 500000000")

        self.api_key_env = api_key_env
        self.fallback_api_key_envs = tuple(fallback_api_key_envs)
        self.queue_base_url = queue_base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.poll_deadline_seconds = poll_deadline_seconds
        self.max_status_polls = max_status_polls
        self.poll_interval_seconds = poll_interval_seconds
        self.max_video_bytes = max_video_bytes
        self.api_client = api_client
        self.download_client = download_client
        self._owns_api_client = api_client is None
        self._owns_download_client = download_client is None
        self.host_resolver = host_resolver or _system_resolve_host

    async def __aenter__(self) -> FalPixVerseTransitionAdapter:
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owns_api_client and self.api_client is not None:
            await self.api_client.aclose()
        if self._owns_download_client and self.download_client is not None:
            await self.download_client.aclose()

    @property
    def credential_env_candidates(self) -> tuple[str, ...]:
        """Return names only; credential values are deliberately never exposed."""

        return (self.api_key_env, *self.fallback_api_key_envs)

    def credential_env_name(self) -> str | None:
        """Return the first currently populated variable name, never its value."""

        return next((name for name in self.credential_env_candidates if os.environ.get(name)), None)

    def capability_manifest(self) -> dict[str, Any]:
        return {
            "capability_id": self.capability_id,
            "operation": "generate_video",
            "provider": "fal.ai",
            "api_key_env_candidates": list(self.credential_env_candidates),
            "queue_submit_then_repeatable_poll": True,
            "input_transport": "exact_data_uri",
            "prompt_max_utf8_bytes": 2_048,
            "duration_seconds": {"minimum": 1, "maximum": 15},
            "resolutions": sorted(_RESOLUTIONS),
            "aspect_ratios": sorted(_ASPECT_RATIOS),
            "output_media_type": "video/mp4",
            "max_video_bytes": self.max_video_bytes,
            "pricing": {
                "version": _PIXVERSE_PRICING_VERSION,
                "audio": False,
                "micros_per_second": dict(_PIXVERSE_V6_NO_AUDIO_MICROS_PER_SECOND),
                "maximum_supported_call_micros": 1_350_000,
            },
        }

    def submit_journal_request(
        self,
        media_request: Mapping[str, Any],
        *,
        start_frame: bytes,
        start_media_type: str,
        end_frame: bytes,
        end_media_type: str,
        options: PixVerseTransitionOptions,
    ) -> dict[str, Any]:
        """Build the exact secret-free request that ``OperationJournal`` hashes."""

        validated = self._validate_inputs(
            media_request,
            start_frame=start_frame,
            start_media_type=start_media_type,
            end_frame=end_frame,
            end_media_type=end_media_type,
        )
        canonical_request = json.dumps(media_request, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return {
            "schema_version": 1,
            "operation": "submit_transition",
            "capability_id": self.capability_id,
            "request_id": media_request["request_id"],
            "media_request_sha256": f"sha256:{hashlib.sha256(canonical_request.encode()).hexdigest()}",
            "prompt_sha256": validated.prompt_sha256,
            "input_frames": [
                _journal_frame("start", validated.start_sha256, start_frame, start_media_type, validated.start_media),
                _journal_frame("end", validated.end_sha256, end_frame, end_media_type, validated.end_media),
            ],
            "provider": {
                "queue_base_url": self.queue_base_url,
                "model": self.capability_id,
                "credential_env_candidates": list(self.credential_env_candidates),
                "input_transport": "exact_data_uri",
            },
            "options": asdict(options),
        }

    def poll_journal_request(self, ticket: Mapping[str, Any] | FalQueueTicket) -> dict[str, Any]:
        exact_ticket = FalQueueTicket.from_value(ticket)
        return {
            "schema_version": 1,
            "operation": "poll_transition",
            "capability_id": self.capability_id,
            "request_id": exact_ticket.request_id,
            "queue_base_url": self.queue_base_url,
            "max_status_polls": self.max_status_polls,
            "poll_interval_seconds": self.poll_interval_seconds,
            "poll_deadline_seconds": self.poll_deadline_seconds,
            "max_video_bytes": self.max_video_bytes,
        }

    async def submit_transition(
        self,
        media_request: Mapping[str, Any],
        *,
        start_frame: bytes,
        start_media_type: str,
        end_frame: bytes,
        end_media_type: str,
        options: PixVerseTransitionOptions,
    ) -> ProviderResult:
        """Submit one paid transition and return its durable queue ticket only."""

        validated = self._validate_inputs(
            media_request,
            start_frame=start_frame,
            start_media_type=start_media_type,
            end_frame=end_frame,
            end_media_type=end_media_type,
        )
        payload = {
            "prompt": validated.prompt,
            "aspect_ratio": options.aspect_ratio,
            "resolution": options.resolution,
            "duration": options.duration_seconds,
            "seed": options.seed,
            "generate_audio_switch": False,
            "generate_multi_clip_switch": False,
            "thinking_type": "disabled",
            "first_image_url": _data_uri(start_media_type, start_frame),
            "end_image_url": _data_uri(end_media_type, end_frame),
        }
        response = await self._api_request("POST", self._model_path(), json=payload)
        try:
            body = _json_object(response, "Fal queue submit")
        except ProviderError as error:
            # A 2xx response can still represent accepted paid work. Without a
            # parseable ticket, its outcome must be reconciled rather than
            # submitted again under a new key.
            raise ProviderError(
                ProviderFailureKind.UNKNOWN_OUTCOME,
                "Fal accepted the submit but returned an invalid queue ticket",
                outcome_known=False,
            ) from error
        request_id = body.get("request_id")
        if not isinstance(request_id, str) or _REQUEST_ID.fullmatch(request_id) is None:
            # A successful submit response can represent accepted work even if
            # its body is malformed.  Never permit a blind second paid call.
            raise ProviderError(
                ProviderFailureKind.UNKNOWN_OUTCOME,
                "Fal accepted the submit but omitted a valid request_id",
                outcome_known=False,
            )
        ticket = FalQueueTicket(1, self.capability_id, request_id)
        return ProviderResult(
            value=ticket.as_dict(),
            request_id=request_id,
            resolved_model=self.capability_id,
            sanitized_metadata={
                "provider": "fal.ai",
                "capability_id": self.capability_id,
                "operation": "queue_submit",
                "prompt_sha256": validated.prompt_sha256,
                "start_frame_sha256": validated.start_sha256,
                "end_frame_sha256": validated.end_sha256,
                "options": asdict(options),
                "response_headers": _safe_headers(response),
            },
            # Fal's queue response does not report an exact charge.  The
            # operation journal therefore retains its full preflight reserve.
            usage={"usage_complete": False, "duration_seconds": options.duration_seconds},
        )

    async def poll_transition(self, ticket: Mapping[str, Any] | FalQueueTicket) -> ProviderResult:
        """Repeatably poll one retained ticket and return bounded raw result bytes.

        The caller must persist this result before passing it to
        :func:`validate_pixverse_video_result`. Keeping semantic validation out
        of the read preserves malformed paid output as quarantined evidence.
        """

        exact_ticket = FalQueueTicket.from_value(ticket)
        try:
            async with asyncio.timeout(self.poll_deadline_seconds):
                return await self._poll_transition(exact_ticket)
        except TimeoutError as error:
            raise ProviderError(
                ProviderFailureKind.TIMEOUT,
                "Fal transition polling exceeded its total deadline",
                request_id=exact_ticket.request_id,
                resolved_model=self.capability_id,
            ) from error

    async def _poll_transition(self, exact_ticket: FalQueueTicket) -> ProviderResult:
        request_id = exact_ticket.request_id
        last_response: httpx.Response | None = None
        last_pending_status: str | None = None
        for poll_number in range(1, self.max_status_polls + 1):
            last_response = await self._api_request(
                "GET",
                f"{self._request_path(request_id)}/status",
                paid_request_id=request_id,
                params={"logs": "false"},
            )
            status_body = _json_object(last_response, "Fal queue status", request_id=request_id)
            status = status_body.get("status")
            if status == "COMPLETED":
                return await self._download_result(exact_ticket, poll_number, last_response)
            if status in {"IN_QUEUE", "IN_PROGRESS"}:
                last_pending_status = status
                if poll_number < self.max_status_polls and self.poll_interval_seconds:
                    await asyncio.sleep(self.poll_interval_seconds)
                continue
            if status in {"ERROR", "FAILED", "CANCELLED"}:
                raise ProviderError(
                    ProviderFailureKind.REFUSAL,
                    f"Fal transition ended with status {status}",
                    request_id=request_id,
                    resolved_model=self.capability_id,
                )
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "Fal queue status response contained an unknown status",
                request_id=request_id,
                resolved_model=self.capability_id,
            )
        assert last_pending_status is not None
        raise FalQueuePending(
            request_id=request_id,
            status=last_pending_status,
            polls=self.max_status_polls,
        )

    async def _download_result(
        self,
        ticket: FalQueueTicket,
        poll_count: int,
        status_response: httpx.Response,
    ) -> ProviderResult:
        request_id = ticket.request_id
        result_response = await self._api_request(
            "GET",
            self._request_path(request_id),
            paid_request_id=request_id,
        )
        body = _json_object(result_response, "Fal queue result", request_id=request_id)
        video = body.get("video")
        if not isinstance(video, Mapping):
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "Fal completed result omitted its video File",
                request_id=request_id,
                resolved_model=self.capability_id,
            )
        url, reported_file = self._parse_file_record(video, request_id)
        await self._validate_public_download_host(url, request_id)
        video_bytes, download_headers, download_contract = await self._download_video(url, request_id)
        exact_hash = f"sha256:{hashlib.sha256(video_bytes).hexdigest()}"
        parsed_url = urlsplit(url)
        return ProviderResult(
            value=video_bytes,
            request_id=request_id,
            resolved_model=self.capability_id,
            sanitized_metadata={
                "provider": "fal.ai",
                "capability_id": self.capability_id,
                "operation": "queue_result_download",
                "terminal_status": "COMPLETED",
                "status_poll_count": poll_count,
                "video": {
                    "content_sha256": exact_hash,
                    "bytes": len(video_bytes),
                    "byte_limit": self.max_video_bytes,
                    "reported_file": reported_file,
                    "download": download_contract,
                    "download_origin": f"{parsed_url.scheme}://{parsed_url.netloc}",
                    "download_url_sha256": f"sha256:{hashlib.sha256(url.encode()).hexdigest()}",
                },
                "response_headers": {
                    "status": _safe_headers(status_response),
                    "result": _safe_headers(result_response),
                    "download": download_headers,
                },
            },
            usage={"cost_micros": 0, "usage_complete": True},
        )

    def _parse_file_record(self, video: Mapping[str, Any], request_id: str) -> tuple[str, dict[str, Any]]:
        url = video.get("url")
        media_type = video.get("content_type")
        size = video.get("file_size")
        file_name = video.get("file_name")
        if not isinstance(url, str) or not url:
            raise _invalid_video("Fal video File omitted its URL", request_id)
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.fragment
            or (parsed.port is not None and parsed.port != 443)
        ):
            raise _invalid_video("Fal video URL must be credential-free HTTPS", request_id)
        if isinstance(size, int) and not isinstance(size, bool) and size > self.max_video_bytes:
            raise _invalid_video("Fal video File exceeds the configured byte bound", request_id)
        safe_file_name = (
            file_name
            if isinstance(file_name, str)
            and bool(file_name)
            and len(file_name.encode("utf-8")) <= 255
            and "/" not in file_name
            and "\\" not in file_name
            else None
        )
        return url, {
            "content_type": media_type if isinstance(media_type, str) and len(media_type) <= 128 else None,
            "content_type_valid_string": isinstance(media_type, str) and len(media_type) <= 128,
            "file_size": size if isinstance(size, int) and not isinstance(size, bool) else None,
            "file_size_valid_integer": isinstance(size, int) and not isinstance(size, bool),
            "file_name": safe_file_name,
            "file_name_valid": safe_file_name == file_name,
        }

    async def _validate_public_download_host(self, url: str, request_id: str) -> None:
        host = urlsplit(url).hostname
        assert host is not None
        if host != _DOWNLOAD_DOMAIN and not host.endswith(f".{_DOWNLOAD_DOMAIN}"):
            raise _invalid_video("Fal video URL is outside the approved Fal CDN domain", request_id)
        try:
            literal = ipaddress.ip_address(host)
            addresses = [literal]
        except ValueError:
            try:
                resolved = self.host_resolver(host)
                if inspect.isawaitable(resolved):
                    resolved = await resolved
                addresses = [ipaddress.ip_address(value) for value in resolved]
            except (OSError, ValueError) as error:
                raise ProviderError(
                    ProviderFailureKind.UNAVAILABLE,
                    "Fal video download hostname could not be resolved safely",
                    request_id=request_id,
                    resolved_model=self.capability_id,
                ) from error
        if not addresses or any(not address.is_global for address in addresses):
            raise _invalid_video("Fal video URL resolved to a non-public address", request_id)

    async def _download_video(self, url: str, request_id: str) -> tuple[bytes, dict[str, str], dict[str, Any]]:
        try:
            async with asyncio.timeout(self.timeout_seconds):
                return await self._download_video_with_phase_timeout(url, request_id)
        except TimeoutError as error:
            raise ProviderError(
                ProviderFailureKind.TIMEOUT,
                "Fal video download exceeded its total request deadline",
                request_id=request_id,
                resolved_model=self.capability_id,
            ) from error

    async def _download_video_with_phase_timeout(
        self, url: str, request_id: str
    ) -> tuple[bytes, dict[str, str], dict[str, Any]]:
        try:
            async with self._download_http_client().stream("GET", url, follow_redirects=False) as response:
                if response.is_redirect:
                    raise _invalid_video("Fal video download redirected", request_id)
                if not response.is_success:
                    raise ProviderError(
                        ProviderFailureKind.UNAVAILABLE,
                        f"Fal video download returned HTTP {response.status_code}",
                        request_id=request_id,
                        resolved_model=self.capability_id,
                    )
                content_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
                content_length = response.headers.get("content-length")
                parsed_length: int | None = None
                content_length_valid = content_length is None
                if content_length is not None:
                    try:
                        parsed_length = int(content_length)
                        content_length_valid = parsed_length >= 0
                    except ValueError:
                        content_length_valid = False
                    if parsed_length is not None and parsed_length > self.max_video_bytes:
                        raise _invalid_video("Fal video download exceeds the configured byte bound", request_id)
                payload = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(payload) + len(chunk) > self.max_video_bytes:
                        raise _invalid_video("Fal video download exceeded the configured byte bound", request_id)
                    payload.extend(chunk)
                headers = _safe_headers(response)
        except ProviderError:
            raise
        except httpx.TimeoutException as error:
            raise ProviderError(
                ProviderFailureKind.TIMEOUT,
                "Fal video download timed out",
                request_id=request_id,
                resolved_model=self.capability_id,
            ) from error
        except httpx.TransportError as error:
            raise ProviderError(
                ProviderFailureKind.UNAVAILABLE,
                "Fal video download transport failed",
                request_id=request_id,
                resolved_model=self.capability_id,
            ) from error
        return (
            bytes(payload),
            headers,
            {
                "content_type": content_type,
                "content_length": parsed_length,
                "content_length_valid_integer": content_length_valid,
            },
        )

    async def _api_request(
        self,
        method: str,
        path: str,
        *,
        paid_request_id: str | None = None,
        **kwargs: Any,
    ) -> httpx.Response:
        credential = self._credential()
        try:
            async with asyncio.timeout(self.timeout_seconds):
                response = await self._api_http_client().request(
                    method,
                    f"{self.queue_base_url}{path}",
                    headers={"authorization": f"Key {credential}", "content-type": "application/json"},
                    follow_redirects=False,
                    **kwargs,
                )
        except TimeoutError as error:
            raise ProviderError(
                ProviderFailureKind.TIMEOUT,
                "Fal queue request exceeded its total deadline",
                outcome_known=paid_request_id is not None,
                request_id=paid_request_id,
                resolved_model=self.capability_id if paid_request_id else None,
            ) from error
        except httpx.TimeoutException as error:
            if paid_request_id is not None:
                raise ProviderError(
                    ProviderFailureKind.TIMEOUT,
                    "Fal queue read timed out",
                    request_id=paid_request_id,
                    resolved_model=self.capability_id,
                ) from error
            known = isinstance(error, (httpx.ConnectTimeout, httpx.PoolTimeout))
            raise ProviderError(
                ProviderFailureKind.TIMEOUT,
                "Fal queue submit timed out",
                outcome_known=known,
            ) from error
        except httpx.ConnectError as error:
            raise ProviderError(
                ProviderFailureKind.UNAVAILABLE,
                "Fal queue connection failed",
                request_id=paid_request_id,
                resolved_model=self.capability_id if paid_request_id else None,
            ) from error
        except httpx.TransportError as error:
            if paid_request_id is not None:
                raise ProviderError(
                    ProviderFailureKind.UNAVAILABLE,
                    "Fal queue read transport failed",
                    request_id=paid_request_id,
                    resolved_model=self.capability_id,
                ) from error
            raise ProviderError(
                ProviderFailureKind.UNKNOWN_OUTCOME,
                "Fal queue transport outcome is unknown",
                outcome_known=False,
                request_id=paid_request_id,
                resolved_model=self.capability_id if paid_request_id else None,
            ) from error
        if response.is_redirect:
            kind = ProviderFailureKind.UNKNOWN_OUTCOME if method == "POST" else ProviderFailureKind.INVALID_OUTPUT
            raise ProviderError(
                kind,
                "Fal queue control endpoint redirected unexpectedly",
                outcome_known=method != "POST",
                request_id=paid_request_id,
                resolved_model=self.capability_id if paid_request_id else None,
            )
        if response.is_success:
            return response
        if paid_request_id is not None:
            # The paid ticket is already durable; callers may safely retry this
            # read operation without ever resubmitting it.
            kind = (
                ProviderFailureKind.AUTHENTICATION
                if response.status_code in {401, 403}
                else ProviderFailureKind.UNAVAILABLE
            )
            raise ProviderError(
                kind,
                f"Fal queue read returned HTTP {response.status_code}: {_safe_error(response)}",
                request_id=paid_request_id,
                resolved_model=self.capability_id,
            )
        if response.status_code in {401, 403}:
            kind = ProviderFailureKind.AUTHENTICATION
        elif response.status_code == 429:
            kind = ProviderFailureKind.QUOTA
        elif response.status_code in {408} or response.status_code >= 500:
            kind = ProviderFailureKind.UNAVAILABLE
        elif response.status_code in {400, 404, 409, 422}:
            kind = ProviderFailureKind.INVALID_OUTPUT
        else:
            kind = ProviderFailureKind.REFUSAL
        raise ProviderError(
            kind,
            f"Fal queue submit returned HTTP {response.status_code}: {_safe_error(response)}",
            outcome_known=response.status_code not in {408, 429} and response.status_code < 500,
        )

    def _credential(self) -> str:
        for name in self.credential_env_candidates:
            if value := os.environ.get(name):
                return value
        names = ", ".join(self.credential_env_candidates)
        raise ProviderError(
            ProviderFailureKind.AUTHENTICATION,
            f"Fal credential is unavailable; set one of these environment variables: {names}",
        )

    def _api_http_client(self) -> httpx.AsyncClient:
        if self.api_client is None:
            self.api_client = httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=False)
        return self.api_client

    def _download_http_client(self) -> httpx.AsyncClient:
        if self.download_client is None:
            self.download_client = httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=False)
        return self.download_client

    def _model_path(self) -> str:
        return f"/{self.capability_id}"

    def _request_path(self, request_id: str) -> str:
        if _REQUEST_ID.fullmatch(request_id) is None:
            raise ProviderError(ProviderFailureKind.INVALID_OUTPUT, "Fal request_id is unsafe")
        return f"{self._model_path()}/requests/{quote(request_id, safe='')}"

    def _validate_inputs(
        self,
        media_request: Mapping[str, Any],
        *,
        start_frame: bytes,
        start_media_type: str,
        end_frame: bytes,
        end_media_type: str,
    ) -> _ValidatedInputs:
        if not isinstance(media_request, Mapping):
            raise ProviderError(ProviderFailureKind.INVALID_OUTPUT, "media operation request must be an object")
        if (
            media_request.get("schema_version") != 1
            or media_request.get("operation") != "generate_video"
            or media_request.get("capability_id") != self.capability_id
            or media_request.get("output_kind") != "video"
            or media_request.get("extraction") is not None
            or media_request.get("reuse") is not None
            or media_request.get("journal") != _JOURNAL_CONTRACT
        ):
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "media operation request does not advertise the exact journaled PixVerse transition capability",
            )
        request_id = media_request.get("request_id")
        if not isinstance(request_id, str) or _REQUEST_ID.fullmatch(request_id) is None:
            raise ProviderError(ProviderFailureKind.INVALID_OUTPUT, "media operation request_id is invalid")
        prompt = media_request.get("prompt")
        if not isinstance(prompt, str) or not prompt or len(prompt.encode("utf-8")) > 2_048:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "PixVerse prompt must contain 1 through 2048 UTF-8 bytes",
            )
        positions = [prompt.find(section) for section in _PROMPT_SECTIONS]
        if (
            any(position < 0 for position in positions)
            or positions != sorted(positions)
            or any(prompt.count(section) != 1 for section in _PROMPT_SECTIONS)
        ):
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "PixVerse prompt must contain the five contract sections exactly once and in order",
            )
        video = media_request.get("video")
        if not isinstance(video, Mapping):
            raise ProviderError(ProviderFailureKind.INVALID_OUTPUT, "PixVerse request omitted its video contract")
        start_sha = video.get("start_frame_sha256")
        end_sha = video.get("end_frame_sha256")
        if (
            not isinstance(start_sha, str)
            or _CONTENT_HASH.fullmatch(start_sha) is None
            or not isinstance(end_sha, str)
            or _CONTENT_HASH.fullmatch(end_sha) is None
            or media_request.get("input_artifacts") != [start_sha, end_sha]
            or video.get("source_video_sha256") is not None
        ):
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "PixVerse request must bind ordered exact start/end frame hashes",
            )
        _validate_video_contract(video)
        start_media = _validate_frame(start_frame, start_media_type, start_sha, "start")
        end_media = _validate_frame(end_frame, end_media_type, end_sha, "end")
        return _ValidatedInputs(
            prompt=prompt,
            prompt_sha256=f"sha256:{hashlib.sha256(prompt.encode()).hexdigest()}",
            start_sha256=start_sha,
            end_sha256=end_sha,
            start_media=start_media,
            end_media=end_media,
        )


def _valid_env_name(value: str) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) is not None


def _data_uri(media_type: str, value: bytes) -> str:
    return f"data:{media_type};base64,{base64.b64encode(value).decode('ascii')}"


def _validate_frame(value: bytes, media_type: str, expected_hash: str, role: str) -> dict[str, Any]:
    if not isinstance(value, bytes) or not value or len(value) > _MAX_FRAME_BYTES:
        raise ProviderError(ProviderFailureKind.INVALID_OUTPUT, f"PixVerse {role} frame bytes are empty or oversized")
    actual_hash = f"sha256:{hashlib.sha256(value).hexdigest()}"
    if actual_hash != expected_hash:
        raise ProviderError(ProviderFailureKind.INVALID_OUTPUT, f"PixVerse {role} frame bytes do not match their hash")
    try:
        return validate_exact_image_bytes(value, media_type)
    except ProviderError as error:
        raise ProviderError(error.kind, f"PixVerse {role} frame is invalid: {error}") from error


def _validate_video_contract(video: Mapping[str, Any]) -> None:
    period = video.get("common_period_ms")
    fps = video.get("desired_fps")
    columns = video.get("body_columns")
    texels = video.get("texels_per_cell")
    overhang = video.get("raster_overhang_px")
    if (
        isinstance(period, bool)
        or not isinstance(period, (int, float))
        or not 120 <= period <= 60_000
        or isinstance(fps, bool)
        or not isinstance(fps, (int, float))
        or not 1 <= fps <= 60
        or isinstance(columns, bool)
        or not isinstance(columns, int)
        or not 1 <= columns <= 128
        or isinstance(texels, bool)
        or not isinstance(texels, int)
        or not 4 <= texels <= 128
        or isinstance(overhang, bool)
        or not isinstance(overhang, int)
        or not 0 <= overhang <= 4
        or (texels * overhang) % 16 != 0
    ):
        raise ProviderError(ProviderFailureKind.INVALID_OUTPUT, "PixVerse video geometry or cadence is invalid")
    derived = max(2, math.ceil(period * fps / 1_000))
    row_texels = texels + 2 * (texels * overhang // 16)
    effective_cap = min(120, 2_048 // row_texels, 16_777_216 // (columns * texels * row_texels * 4))
    if (
        video.get("derived_frame_rows") != derived
        or video.get("row_texels") != row_texels
        or video.get("effective_frame_row_cap") != effective_cap
        or derived > effective_cap
        or video.get("frame_extraction") != "deterministic_uniform_full_period"
        or video.get("row_zero") != "resting_and_reduced_motion"
        or video.get("alpha_matte_verification") != "fail_closed"
        or video.get("loop_closure") != "true_final_to_zero"
        or video.get("max_frame_rows") != 120
    ):
        raise ProviderError(ProviderFailureKind.INVALID_OUTPUT, "PixVerse video contract is internally inconsistent")


def _journal_frame(
    role: str,
    content_hash: str,
    value: bytes,
    media_type: str,
    media: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "role": role,
        "content_sha256": content_hash,
        "bytes": len(value),
        "media_type": media_type,
        "width_px": media["width_px"],
        "height_px": media["height_px"],
        "decoded_format": media["decoded_format"],
    }


def _json_object(response: httpx.Response, label: str, *, request_id: str | None = None) -> dict[str, Any]:
    try:
        body = response.json()
    except ValueError as error:
        raise ProviderError(
            ProviderFailureKind.INVALID_OUTPUT,
            f"{label} returned invalid JSON",
            request_id=request_id,
            resolved_model=PIXVERSE_TRANSITION_CAPABILITY if request_id else None,
        ) from error
    if not isinstance(body, dict):
        raise ProviderError(
            ProviderFailureKind.INVALID_OUTPUT,
            f"{label} response must be an object",
            request_id=request_id,
            resolved_model=PIXVERSE_TRANSITION_CAPABILITY if request_id else None,
        )
    return body


def _safe_headers(response: httpx.Response) -> dict[str, str]:
    return {key.lower(): value for key, value in response.headers.items() if key.lower() in _SAFE_RESPONSE_HEADERS}


def _safe_error(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return "non-JSON error response"
    if not isinstance(body, Mapping):
        return "malformed error response"
    candidate: Any = body.get("detail")
    error = body.get("error")
    if candidate is None and isinstance(error, Mapping):
        candidate = error.get("message") or error.get("code")
    if not isinstance(candidate, str):
        return "unspecified provider error"
    return " ".join(candidate.split())[:300]


def _invalid_video(message: str, request_id: str) -> ProviderError:
    return ProviderError(
        ProviderFailureKind.INVALID_OUTPUT,
        message,
        request_id=request_id,
        resolved_model=PIXVERSE_TRANSITION_CAPABILITY,
    )


def validate_pixverse_video_result(result: ProviderResult) -> None:
    """Validate bounded paid bytes after their CAS persistence.

    Pass this function as ``OperationJournal.run_provider(validate_result=...)``.
    The journal persists ``result.value`` first, so a malformed paid response is
    retained as quarantined evidence instead of disappearing inside an adapter.
    """

    request_id = result.request_id
    if not isinstance(request_id, str) or _REQUEST_ID.fullmatch(request_id) is None:
        raise ProviderError(ProviderFailureKind.INVALID_OUTPUT, "Fal video result omitted its retained request_id")
    if result.resolved_model != PIXVERSE_TRANSITION_CAPABILITY:
        raise _invalid_video("Fal video result resolved to the wrong capability", request_id)
    value = result.value
    if not isinstance(value, bytes) or not value:
        raise _invalid_video("Fal video result did not contain exact bytes", request_id)
    video = result.sanitized_metadata.get("video")
    if not isinstance(video, dict):
        raise _invalid_video("Fal video result omitted bounded media metadata", request_id)
    byte_limit = video.get("byte_limit")
    if isinstance(byte_limit, bool) or not isinstance(byte_limit, int) or not 1 <= len(value) <= byte_limit:
        raise _invalid_video("Fal video bytes exceed their retained bound", request_id)
    expected_hash = f"sha256:{hashlib.sha256(value).hexdigest()}"
    if video.get("content_sha256") != expected_hash or video.get("bytes") != len(value):
        raise _invalid_video("Fal video retained hash or byte count disagrees", request_id)

    reported = video.get("reported_file")
    if not isinstance(reported, Mapping):
        raise _invalid_video("Fal video omitted provider File metadata", request_id)
    if not reported.get("content_type_valid_string") or reported.get("content_type") != "video/mp4":
        raise _invalid_video("Fal video File must declare video/mp4", request_id)
    if (
        not reported.get("file_size_valid_integer")
        or reported.get("file_size") != len(value)
        or not reported.get("file_name_valid")
    ):
        raise _invalid_video("Fal video File size or name disagrees with retained bytes", request_id)

    download = video.get("download")
    if not isinstance(download, Mapping) or download.get("content_type") != "video/mp4":
        raise _invalid_video("Fal video download did not return video/mp4", request_id)
    if not download.get("content_length_valid_integer"):
        raise _invalid_video("Fal video download has an invalid Content-Length", request_id)
    content_length = download.get("content_length")
    if content_length is not None and content_length != len(value):
        raise _invalid_video("Fal video download Content-Length disagrees with retained bytes", request_id)

    video["container"] = _validate_mp4(value, request_id=request_id)


def _validate_mp4(value: bytes, *, request_id: str) -> dict[str, Any]:
    """Validate a complete bounded ISO-BMFF container without invoking a codec."""

    position = 0
    boxes: list[str] = []
    box_count = 0
    first_box: str | None = None
    saw_moov = False
    saw_mdat = False
    major_brand: str | None = None
    while position < len(value):
        box_count += 1
        if box_count > _MAX_MP4_TOP_LEVEL_BOXES:
            raise _invalid_video("Fal video MP4 has too many top-level boxes", request_id)
        if len(value) - position < 8:
            raise _invalid_video("Fal video MP4 has a truncated box header", request_id)
        size32, raw_type = struct.unpack_from(">I4s", value, position)
        header_size = 8
        if size32 == 1:
            if len(value) - position < 16:
                raise _invalid_video("Fal video MP4 has a truncated extended box header", request_id)
            size = struct.unpack_from(">Q", value, position + 8)[0]
            header_size = 16
        elif size32 == 0:
            size = len(value) - position
        else:
            size = size32
        if size < header_size or position + size > len(value):
            raise _invalid_video("Fal video MP4 box exceeds retained bytes", request_id)
        try:
            box_type = raw_type.decode("ascii")
        except UnicodeDecodeError as error:
            raise _invalid_video("Fal video MP4 has a non-ASCII box type", request_id) from error
        if first_box is None:
            first_box = box_type
        if len(boxes) < _MAX_RETAINED_BOX_NAMES:
            boxes.append(box_type)
        saw_moov = saw_moov or box_type == "moov"
        saw_mdat = saw_mdat or box_type == "mdat"
        if box_type == "ftyp":
            if size < header_size + 8:
                raise _invalid_video("Fal video MP4 has an invalid ftyp box", request_id)
            try:
                major_brand = value[position + header_size : position + header_size + 4].decode("ascii", "strict")
            except UnicodeDecodeError as error:
                raise _invalid_video("Fal video MP4 has a non-ASCII major brand", request_id) from error
        position += size
    if position != len(value) or first_box != "ftyp" or major_brand is None:
        raise _invalid_video("Fal video is not a complete MP4 container", request_id)
    if not saw_moov or not saw_mdat:
        raise _invalid_video("Fal video MP4 omitted moov or mdat", request_id)
    return {
        "format": "ISO-BMFF",
        "major_brand": major_brand,
        "top_level_box_count": box_count,
        "top_level_boxes": boxes,
        "top_level_boxes_truncated": box_count > len(boxes),
    }


async def _system_resolve_host(host: str) -> Sequence[str]:
    def resolve() -> list[str]:
        return sorted({entry[4][0] for entry in socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)})

    return await asyncio.to_thread(resolve)


__all__ = [
    "PIXVERSE_TRANSITION_CAPABILITY",
    "FalPixVerseTransitionAdapter",
    "FalQueuePending",
    "FalQueueTicket",
    "PixVerseTransitionOptions",
    "validate_pixverse_video_result",
]
