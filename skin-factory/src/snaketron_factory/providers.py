"""Role-based provider adapters with typed failures and no silent fallback."""

from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from typing import Any, ClassVar
from urllib.parse import quote

import httpx
from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError
from pydantic import BaseModel

from .config import FactoryConfig, ModelRole
from .domain import ProviderError, ProviderFailureKind, ProviderResult


def _schema_value(schema: Mapping[str, Any] | type[BaseModel]) -> dict[str, Any]:
    return schema.model_json_schema() if isinstance(schema, type) else dict(schema)


def _usage_count(value: Any) -> int:
    """Return a provider usage counter without accepting booleans or negatives."""

    if isinstance(value, bool):
        return 0
    try:
        parsed = int(value or 0)
    except (TypeError, ValueError):
        return 0
    return max(parsed, 0)


def _image_media_type(data: bytes) -> str | None:
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    return None


def _http_outcome_known(method: str, status_code: int) -> bool:
    """Whether an HTTP error proves that no non-idempotent work happened.

    A retry-looking response is not an idempotency guarantee. In particular,
    providers can return a 429 or 5xx after accepting or even completing an
    expensive generation. Only safe HTTP methods may treat those responses as
    known outcomes; generation POSTs must be reconciled before another call.
    """

    if method.upper() in {"GET", "HEAD", "OPTIONS"}:
        return True
    return status_code != 429 and status_code != 408 and status_code < 500


class GeminiProvider:
    def __init__(self, role: ModelRole, *, client: httpx.AsyncClient | None = None) -> None:
        if role.provider != "gemini" or not role.model:
            raise ValueError("GeminiProvider requires a gemini role with a pinned model")
        self.role = role
        self.client = client or httpx.AsyncClient(timeout=role.timeout_seconds)
        self._owns_client = client is None
        self.base_url = (role.base_url or "https://generativelanguage.googleapis.com/v1beta").rstrip("/")

    @property
    def model(self) -> str:
        assert self.role.model is not None
        return self.role.model

    def _headers(self) -> dict[str, str]:
        key = self.role.secret()
        if not key:
            raise ProviderError(
                ProviderFailureKind.UNAVAILABLE,
                f"{self.role.api_key_env or 'Gemini API key'} is not set",
            )
        return {"x-goog-api-key": key, "content-type": "application/json"}

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def describe_model(self) -> dict[str, Any]:
        response = await self._send("GET", f"/models/{quote(self.model, safe='')}")
        return self._json_response(response)

    async def generate_structured(
        self,
        *,
        system: str,
        prompt: str,
        schema: Mapping[str, Any] | type[BaseModel],
        images: list[tuple[str, bytes]] | None = None,
        temperature: float = 0.4,
    ) -> ProviderResult:
        schema_value = _schema_value(schema)
        parts: list[dict[str, Any]] = [{"text": prompt}]
        for media_type, data in images or []:
            parts.append(
                {
                    "inlineData": {
                        "mimeType": media_type,
                        "data": base64.b64encode(data).decode("ascii"),
                    }
                }
            )
        generation: dict[str, Any] = {
            "temperature": temperature,
            "responseMimeType": "application/json",
            "responseJsonSchema": schema_value,
        }
        if self.role.thinking_level:
            generation["thinkingConfig"] = {"thinkingLevel": self.role.thinking_level}
        payload = {
            "systemInstruction": {"parts": [{"text": system}]},
            "contents": [{"role": "user", "parts": parts}],
            "generationConfig": generation,
        }
        response = await self._send("POST", f"/models/{quote(self.model, safe='')}:generateContent", json=payload)
        body = self._json_response(response)
        text = self._text(body)
        try:
            value = json.loads(text)
        except json.JSONDecodeError as error:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                f"Gemini returned invalid JSON: {error}",
                request_id=self._request_id(response),
            ) from error
        try:
            Draft202012Validator.check_schema(schema_value)
            Draft202012Validator(schema_value).validate(value)
        except (JsonSchemaValidationError, SchemaError) as error:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                f"Gemini structured output violated its JSON schema: {error.message}",
                request_id=self._request_id(response),
            ) from error
        return self._result(value, response, body)

    async def generate_image(
        self,
        *,
        prompt: str,
        references: list[tuple[str, bytes]] | None = None,
        aspect_ratio: str = "16:9",
        image_size: str = "2K",
    ) -> ProviderResult:
        parts: list[dict[str, Any]] = [{"text": prompt}]
        for media_type, data in references or []:
            parts.append(
                {
                    "inlineData": {
                        "mimeType": media_type,
                        "data": base64.b64encode(data).decode("ascii"),
                    }
                }
            )
        payload = {
            "contents": [{"role": "user", "parts": parts}],
            "generationConfig": {
                "responseModalities": ["TEXT", "IMAGE"],
                "imageConfig": {"aspectRatio": aspect_ratio, "imageSize": image_size},
            },
        }
        response = await self._send("POST", f"/models/{quote(self.model, safe='')}:generateContent", json=payload)
        body = self._json_response(response)
        for candidate in body.get("candidates", []):
            for part in candidate.get("content", {}).get("parts", []):
                inline = part.get("inlineData") or part.get("inline_data")
                if inline and str(inline.get("mimeType", "")).startswith("image/"):
                    try:
                        image = base64.b64decode(inline["data"], validate=True)
                    except (KeyError, ValueError) as error:
                        raise ProviderError(
                            ProviderFailureKind.INVALID_OUTPUT,
                            f"Gemini image payload is invalid: {error}",
                            request_id=self._request_id(response),
                        ) from error
                    return self._result(
                        {
                            "image": image,
                            "media_type": inline["mimeType"],
                            "text": self._text(body, required=False),
                        },
                        response,
                        body,
                    )
        self._raise_candidate_failure(body, response)
        raise ProviderError(
            ProviderFailureKind.INVALID_OUTPUT,
            "Gemini response did not include an image",
            request_id=self._request_id(response),
        )

    async def _send(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        try:
            response = await self.client.request(method, f"{self.base_url}{path}", headers=self._headers(), **kwargs)
        except httpx.TimeoutException as error:
            # A connect timeout is known not to have executed; a read/write
            # timeout is conservatively unknown to avoid duplicate image spend.
            known = isinstance(error, (httpx.ConnectTimeout, httpx.PoolTimeout))
            raise ProviderError(
                ProviderFailureKind.TIMEOUT,
                f"Gemini timeout: {error}",
                outcome_known=known,
            ) from error
        except httpx.ConnectError as error:
            raise ProviderError(
                ProviderFailureKind.UNAVAILABLE,
                f"Gemini connection failed: {error}",
            ) from error
        except httpx.TransportError as error:
            raise ProviderError(
                ProviderFailureKind.UNKNOWN_OUTCOME,
                f"Gemini transport outcome is unknown: {error}",
                outcome_known=False,
            ) from error
        if response.is_success:
            return response
        request_id = self._request_id(response)
        try:
            message = response.json().get("error", {}).get("message", response.text)
        except ValueError:
            message = response.text
        if response.status_code == 429:
            kind = ProviderFailureKind.QUOTA
        elif response.status_code == 408 or response.status_code >= 500:
            kind = ProviderFailureKind.UNAVAILABLE
        elif response.status_code in {400, 404, 422}:
            kind = ProviderFailureKind.INVALID_OUTPUT
        else:
            kind = ProviderFailureKind.REFUSAL
        raise ProviderError(
            kind,
            f"Gemini HTTP {response.status_code}: {message}",
            outcome_known=_http_outcome_known(method, response.status_code),
            request_id=request_id,
        )

    @staticmethod
    def _request_id(response: httpx.Response) -> str | None:
        return response.headers.get("x-request-id") or response.headers.get("x-goog-request-id")

    @classmethod
    def _json_response(cls, response: httpx.Response) -> dict[str, Any]:
        try:
            body = response.json()
        except ValueError as error:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                f"Gemini returned invalid response JSON: {error}",
                request_id=cls._request_id(response),
            ) from error
        if not isinstance(body, dict):
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "Gemini response JSON must be an object",
                request_id=cls._request_id(response),
            )
        return body

    def _result(self, value: Any, response: httpx.Response, body: Mapping[str, Any]) -> ProviderResult:
        usage = body.get("usageMetadata", {})
        input_tokens = int(usage.get("promptTokenCount", 0))
        output_tokens = int(usage.get("candidatesTokenCount", 0))
        cost = (
            input_tokens * self.role.cost_per_million_input_micros
            + output_tokens * self.role.cost_per_million_output_micros
        ) // 1_000_000
        if isinstance(value, dict) and "image" in value:
            cost += self.role.cost_per_image_micros
        return ProviderResult(
            value=value,
            request_id=self._request_id(response),
            resolved_model=body.get("modelVersion", self.model),
            sanitized_metadata={
                "finish_reasons": [x.get("finishReason") for x in body.get("candidates", [])],
                "response_sha_headers": {
                    key.lower(): value
                    for key, value in response.headers.items()
                    if key.lower() in {"etag", "date", "x-request-id", "x-goog-request-id"}
                },
            },
            usage={
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cost_micros": cost,
            },
        )

    @classmethod
    def _text(cls, body: Mapping[str, Any], *, required: bool = True) -> str:
        chunks: list[str] = []
        for candidate in body.get("candidates", []):
            for part in candidate.get("content", {}).get("parts", []):
                if isinstance(part.get("text"), str):
                    chunks.append(part["text"])
        if not chunks and required:
            cls._raise_candidate_failure(body, None)
            raise ProviderError(ProviderFailureKind.INVALID_OUTPUT, "Gemini response contained no text")
        return "\n".join(chunks)

    @staticmethod
    def _raise_candidate_failure(body: Mapping[str, Any], response: httpx.Response | None) -> None:
        prompt_feedback = body.get("promptFeedback", {})
        block_reason = prompt_feedback.get("blockReason")
        finish = [x.get("finishReason") for x in body.get("candidates", [])]
        if block_reason or any(x in {"SAFETY", "RECITATION", "PROHIBITED_CONTENT"} for x in finish):
            raise ProviderError(
                ProviderFailureKind.REFUSAL,
                f"Gemini refused the request: {block_reason or finish}",
                request_id=GeminiProvider._request_id(response) if response else None,
            )


class OpenAICompatibleProvider:
    """OpenAI-compatible content adapter for text, vision, and image roles.

    The adapter intentionally implements one explicit API surface instead of
    probing alternative endpoints. Structured and vision requests use Chat
    Completions. Image creation uses Images generations, or Images edits when
    exact reference bytes are supplied. A deployment claiming compatibility
    must implement the endpoint it is configured to serve; an endpoint error
    is never turned into a request to another model or provider.
    """

    _RESPONSE_HEADERS: ClassVar[set[str]] = {
        "date",
        "etag",
        "openai-processing-ms",
        "openai-request-id",
        "request-id",
        "x-request-id",
    }

    def __init__(self, role: ModelRole, *, client: httpx.AsyncClient | None = None) -> None:
        if role.provider != "openai_compatible" or not role.model:
            raise ValueError("OpenAICompatibleProvider requires an openai_compatible role with a pinned model")
        if not role.base_url:
            raise ValueError("OpenAICompatibleProvider requires an explicit API base_url")
        self.role = role
        self.client = client or httpx.AsyncClient(timeout=role.timeout_seconds)
        self._owns_client = client is None
        self.base_url = role.base_url.rstrip("/")

    @property
    def model(self) -> str:
        assert self.role.model is not None
        return self.role.model

    def _headers(self, *, json_content: bool = True) -> dict[str, str]:
        headers: dict[str, str] = {}
        if json_content:
            headers["content-type"] = "application/json"
        if self.role.api_key_env:
            key = self.role.secret()
            if not key:
                raise ProviderError(
                    ProviderFailureKind.UNAVAILABLE,
                    f"{self.role.api_key_env} is not set",
                )
            headers["authorization"] = f"Bearer {key}"
        return headers

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def describe_model(self) -> dict[str, Any]:
        response = await self._send("GET", f"/models/{quote(self.model, safe='')}")
        body = self._json_response(response)
        resolved = body.get("id")
        if resolved != self.model:
            raise ProviderError(
                ProviderFailureKind.UNAVAILABLE,
                f"configured model {self.model!r} resolved as {resolved!r}",
                request_id=self._request_id(response),
            )
        return body

    async def generate_structured(
        self,
        *,
        system: str,
        prompt: str,
        schema: Mapping[str, Any] | type[BaseModel],
        images: list[tuple[str, bytes]] | None = None,
        temperature: float = 0.4,
    ) -> ProviderResult:
        schema_json = _schema_value(schema)
        content: list[dict[str, Any]] = [{"type": "text", "text": prompt}]
        for media_type, data in images or []:
            if not media_type.startswith("image/") or not data:
                raise ProviderError(
                    ProviderFailureKind.INVALID_OUTPUT,
                    "OpenAI-compatible multimodal inputs must be non-empty image media",
                )
            encoded = base64.b64encode(data).decode("ascii")
            content.append(
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:{media_type};base64,{encoded}"},
                }
            )
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": content},
            ],
            "temperature": temperature,
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "snaketron_factory_response",
                    "strict": True,
                    "schema": schema_json,
                },
            },
        }
        if self.role.thinking_level:
            payload["reasoning_effort"] = self.role.thinking_level
        response = await self._send("POST", "/chat/completions", json=payload)
        body = self._json_response(response)
        choices = body.get("choices")
        if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "OpenAI-compatible response contained no choices",
                request_id=self._request_id(response),
            )
        choice = choices[0]
        message = choice.get("message")
        if not isinstance(message, dict):
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "OpenAI-compatible response choice contained no message",
                request_id=self._request_id(response),
            )
        refusal = message.get("refusal")
        finish_reason = choice.get("finish_reason")
        if refusal or finish_reason in {"content_filter", "safety"}:
            raise ProviderError(
                ProviderFailureKind.REFUSAL,
                "OpenAI-compatible provider refused the structured request",
                request_id=self._request_id(response),
            )
        if message.get("tool_calls"):
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "OpenAI-compatible provider returned a tool call instead of structured JSON",
                request_id=self._request_id(response),
            )
        text = self._message_text(message.get("content"))
        try:
            value = json.loads(text)
        except json.JSONDecodeError as error:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                f"OpenAI-compatible provider returned invalid JSON: {error}",
                request_id=self._request_id(response),
            ) from error
        try:
            Draft202012Validator.check_schema(schema_json)
            Draft202012Validator(schema_json).validate(value)
        except (JsonSchemaValidationError, SchemaError) as error:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                f"OpenAI-compatible structured output violated its JSON schema: {error.message}",
                request_id=self._request_id(response),
            ) from error
        return self._result(
            value,
            response,
            body,
            metadata={
                "finish_reason": finish_reason,
                "system_fingerprint": body.get("system_fingerprint"),
            },
        )

    async def generate_image(
        self,
        *,
        prompt: str,
        references: list[tuple[str, bytes]] | None = None,
        aspect_ratio: str = "16:9",
        image_size: str = "2K",
    ) -> ProviderResult:
        size = self._image_dimensions(aspect_ratio, image_size)
        if references:
            files: list[tuple[str, tuple[str, bytes, str]]] = []
            for index, (media_type, data) in enumerate(references):
                if not media_type.startswith("image/") or not data:
                    raise ProviderError(
                        ProviderFailureKind.INVALID_OUTPUT,
                        "OpenAI-compatible image references must be non-empty image media",
                    )
                extension = {
                    "image/jpeg": "jpg",
                    "image/png": "png",
                    "image/webp": "webp",
                }.get(media_type, "image")
                files.append(("image[]", (f"reference-{index}.{extension}", data, media_type)))
            response = await self._send(
                "POST",
                "/images/edits",
                headers=self._headers(json_content=False),
                data={
                    "model": self.model,
                    "prompt": prompt,
                    "n": "1",
                    "size": size,
                },
                files=files,
            )
            endpoint = "images/edits"
        else:
            response = await self._send(
                "POST",
                "/images/generations",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "n": 1,
                    "size": size,
                },
            )
            endpoint = "images/generations"
        body = self._json_response(response)
        entries = body.get("data")
        if not isinstance(entries, list) or len(entries) != 1 or not isinstance(entries[0], dict):
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "OpenAI-compatible image response must contain exactly one data item",
                request_id=self._request_id(response),
            )
        encoded = entries[0].get("b64_json")
        if not isinstance(encoded, str):
            # URLs are deliberately not fetched: exact generated bytes must be
            # returned inside the journaled provider operation.
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "OpenAI-compatible image response did not include retained base64 bytes",
                request_id=self._request_id(response),
            )
        try:
            image = base64.b64decode(encoded, validate=True)
        except ValueError as error:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                f"OpenAI-compatible image payload is invalid: {error}",
                request_id=self._request_id(response),
            ) from error
        media_type = _image_media_type(image)
        if media_type is None:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "OpenAI-compatible image payload is not PNG, JPEG, or WebP",
                request_id=self._request_id(response),
            )
        return self._result(
            {"image": image, "media_type": media_type, "text": entries[0].get("revised_prompt")},
            response,
            body,
            image_count=1,
            metadata={
                "endpoint": endpoint,
                "requested_aspect_ratio": aspect_ratio,
                "requested_image_size": image_size,
                "submitted_size": size,
            },
        )

    async def _send(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        headers = kwargs.pop("headers", None)
        if headers is None:
            headers = self._headers()
        try:
            response = await self.client.request(
                method,
                f"{self.base_url}{path}",
                headers=headers,
                **kwargs,
            )
        except httpx.TimeoutException as error:
            known = isinstance(error, (httpx.ConnectTimeout, httpx.PoolTimeout))
            raise ProviderError(
                ProviderFailureKind.TIMEOUT,
                f"OpenAI-compatible provider timeout: {error}",
                outcome_known=known,
            ) from error
        except httpx.ConnectError as error:
            raise ProviderError(
                ProviderFailureKind.UNAVAILABLE,
                f"OpenAI-compatible provider connection failed: {error}",
            ) from error
        except httpx.TransportError as error:
            # A protocol/write/read failure may occur after the server accepted
            # an expensive generation. It must go to reconciliation, not retry.
            raise ProviderError(
                ProviderFailureKind.UNKNOWN_OUTCOME,
                f"OpenAI-compatible provider transport outcome is unknown: {error}",
                outcome_known=False,
            ) from error
        if response.is_success:
            return response
        request_id = self._request_id(response)
        message = self._error_message(response)
        if response.status_code == 429:
            kind = ProviderFailureKind.QUOTA
        elif response.status_code == 408 or response.status_code >= 500:
            kind = ProviderFailureKind.UNAVAILABLE
        elif response.status_code in {400, 404, 409, 422}:
            kind = ProviderFailureKind.INVALID_OUTPUT
        else:
            kind = ProviderFailureKind.REFUSAL
        raise ProviderError(
            kind,
            f"OpenAI-compatible provider HTTP {response.status_code}: {message}",
            outcome_known=_http_outcome_known(method, response.status_code),
            request_id=request_id,
        )

    @staticmethod
    def _message_text(content: Any) -> str:
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            chunks = [
                item.get("text")
                for item in content
                if isinstance(item, dict)
                and item.get("type") in {None, "text", "output_text"}
                and isinstance(item.get("text"), str)
            ]
            if chunks:
                return "\n".join(chunks)
        raise ProviderError(
            ProviderFailureKind.INVALID_OUTPUT,
            "OpenAI-compatible structured response contained no JSON text",
        )

    @staticmethod
    def _image_dimensions(aspect_ratio: str, image_size: str) -> str:
        dimensions = {
            "1K": {
                "1:1": "1024x1024",
                "4:3": "1536x1024",
                "16:9": "1536x1024",
                "3:4": "1024x1536",
                "9:16": "1024x1536",
            },
            "2K": {
                "1:1": "2048x2048",
                "4:3": "2048x1536",
                "16:9": "2048x1152",
                "3:4": "1536x2048",
                "9:16": "1152x2048",
            },
            "4K": {
                "1:1": "2880x2880",
                "4:3": "3264x2448",
                "16:9": "3840x2160",
                "3:4": "2448x3264",
                "9:16": "2160x3840",
            },
        }
        try:
            return dimensions[image_size][aspect_ratio]
        except KeyError as error:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                f"unsupported OpenAI-compatible image dimensions {image_size!r} {aspect_ratio!r}",
            ) from error

    @classmethod
    def _request_id(cls, response: httpx.Response) -> str | None:
        for key in ("x-request-id", "openai-request-id", "request-id"):
            if value := response.headers.get(key):
                return value
        return None

    @classmethod
    def _json_response(cls, response: httpx.Response) -> dict[str, Any]:
        try:
            body = response.json()
        except ValueError as error:
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                f"OpenAI-compatible provider returned invalid response JSON: {error}",
                request_id=cls._request_id(response),
            ) from error
        if not isinstance(body, dict):
            raise ProviderError(
                ProviderFailureKind.INVALID_OUTPUT,
                "OpenAI-compatible provider response JSON must be an object",
                request_id=cls._request_id(response),
            )
        return body

    @staticmethod
    def _error_message(response: httpx.Response) -> str:
        try:
            body = response.json()
        except ValueError:
            return "non-JSON error response"
        if not isinstance(body, dict):
            return "malformed error response"
        error = body.get("error")
        if not isinstance(error, dict):
            return "unspecified provider error"
        code = error.get("code") or error.get("type")
        message = error.get("message")
        safe_message = " ".join(str(message or "unspecified provider error").split())[:300]
        return f"{code}: {safe_message}" if code else safe_message

    def _result(
        self,
        value: Any,
        response: httpx.Response,
        body: Mapping[str, Any],
        *,
        image_count: int = 0,
        metadata: Mapping[str, Any] | None = None,
    ) -> ProviderResult:
        usage = body.get("usage")
        usage = usage if isinstance(usage, Mapping) else {}
        input_tokens = _usage_count(usage.get("prompt_tokens", usage.get("input_tokens", 0)))
        output_tokens = _usage_count(usage.get("completion_tokens", usage.get("output_tokens", 0)))
        cost = (
            input_tokens * self.role.cost_per_million_input_micros
            + output_tokens * self.role.cost_per_million_output_micros
        ) // 1_000_000
        cost += image_count * self.role.cost_per_image_micros
        clean_metadata = {key: item for key, item in dict(metadata or {}).items() if item is not None}
        clean_metadata["response_headers"] = {
            key.lower(): item for key, item in response.headers.items() if key.lower() in self._RESPONSE_HEADERS
        }
        return ProviderResult(
            value=value,
            request_id=self._request_id(response),
            resolved_model=str(body.get("model") or self.model),
            sanitized_metadata=clean_metadata,
            usage={
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cost_micros": cost,
            },
        )


class FakeProvider:
    """Deterministic adapter used by conformance tests and local smoke runs."""

    def __init__(self, model: str, responses: list[Any] | None = None) -> None:
        self.model = model
        self.responses = list(responses or [])
        self.calls: list[dict[str, Any]] = []

    def _next(self, fallback: Any) -> ProviderResult:
        value = self.responses.pop(0) if self.responses else fallback
        return ProviderResult(
            value=value,
            request_id=f"fake-{len(self.calls)}",
            resolved_model=self.model,
            sanitized_metadata={"fake": True},
            usage={"cost_micros": 0},
        )

    async def describe_model(self) -> dict[str, Any]:
        return {"name": self.model, "supportedGenerationMethods": ["generateContent"]}

    async def generate_structured(self, **kwargs: Any) -> ProviderResult:
        self.calls.append(kwargs)
        return self._next({})

    async def generate_image(self, **kwargs: Any) -> ProviderResult:
        self.calls.append(kwargs)
        # A valid 32x8 transparent PNG fixture is supplied by tests when its
        # pixels matter. Empty bytes make accidental real use fail at a gate.
        return self._next({"image": b"", "media_type": "image/png", "text": "fake"})


class ProviderRegistry:
    def __init__(self, config: FactoryConfig) -> None:
        self.config = config
        self._providers: dict[str, GeminiProvider | OpenAICompatibleProvider | FakeProvider] = {}

    def role(self, name: str) -> GeminiProvider | OpenAICompatibleProvider | FakeProvider:
        if name in self._providers:
            return self._providers[name]
        role = getattr(self.config.models, name)
        if role.provider == "gemini":
            provider: GeminiProvider | OpenAICompatibleProvider | FakeProvider = GeminiProvider(role)
        elif role.provider == "openai_compatible":
            provider = OpenAICompatibleProvider(role)
        elif role.provider == "fake":
            provider = FakeProvider(role.model or f"fake-{name}")
        else:
            raise ValueError(f"role {name} uses {role.provider}; it is not a content provider")
        self._providers[name] = provider
        return provider

    async def close(self) -> None:
        for provider in self._providers.values():
            close = getattr(provider, "close", None)
            if close:
                await close()
