from __future__ import annotations

import base64
import json
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from snaketron_factory.config import FactoryConfig, ModelRole
from snaketron_factory.domain import (
    ImplementationPlan,
    InlineArtifact,
    ProviderError,
    ProviderFailureKind,
    Purpose,
    WorkerRequest,
    WorkerResult,
)
from snaketron_factory.providers import (
    GeminiProvider,
    OpenAICompatibleProvider,
    ProviderRegistry,
)
from snaketron_factory.worker import OpenAICompatibleWorker


def test_image_generator_role_is_configurable_while_gemini_remains_the_default(factory_config) -> None:
    payload = factory_config.model_dump(mode="python")
    payload["models"]["image_generator"] = {
        "provider": "openai_compatible",
        "model": "operator-selected-image-model",
        "base_url": "https://images.example.test/v1",
    }

    configured = FactoryConfig.model_validate(payload)

    assert configured.models.image_generator.provider == "openai_compatible"
    assert configured.models.image_generator.model == "operator-selected-image-model"


def openai_role(**overrides: Any) -> ModelRole:
    values: dict[str, Any] = {
        "provider": "openai_compatible",
        "model": "pinned-image-model",
        "base_url": "https://content.example.test/v1",
        "api_key_env": "TEST_OPENAI_COMPATIBLE_KEY",
        "cost_per_million_input_micros": 100,
        "cost_per_million_output_micros": 200,
        "cost_per_image_micros": 10_000,
    }
    values.update(overrides)
    return ModelRole.model_validate(values)


def worker_result() -> WorkerResult:
    return WorkerResult(
        implementation_plan=ImplementationPlan(
            path="layers",
            rationale="Procedural layers best preserve the clean pattern.",
            fidelity_features=["clear head silhouette"],
            layer_plan=["base", "shine"],
            asset_plan=[],
            animation_plan=["time-driven shine"],
            required_wrap_axes=[],
            risks=["small-scale detail"],
        ),
        skin_document={"schema_version": 2, "name": "test", "layers": []},
        tool_requests=[],
        trace=[{"step": "planned"}],
        usage={"reasoning_tokens": 12},
    )


def worker_request(**overrides: Any) -> WorkerRequest:
    values: dict[str, Any] = {
        "request_id": "worker-request-1",
        "attempt_id": "attempt-1",
        "purpose": Purpose.PRODUCTION,
        "skill_sha256": "a" * 64,
        "skill_files": {"SKILL.md": "Do exact authoring."},
        "capability_manifest": {"limits": {"layers": 8}},
        "artifact_refs": {"prototype": "sha256:" + "b" * 64},
        "authoring_inputs": {"approval": {"decision": "human"}},
        "inline_artifacts": {
            "prototype": InlineArtifact(
                content_hash="sha256:" + "b" * 64,
                media_type="image/png",
                base64_data=base64.b64encode(b"exact-pixels").decode(),
            )
        },
        "pure_tools": ["color_math", "schema_lookup"],
        "budget": {"max_layers": 8},
        "output_schemas": {"worker_result": WorkerResult.model_json_schema()},
    }
    values.update(overrides)
    return WorkerRequest.model_validate(values)


@pytest.mark.parametrize("tool", ["shell", "network", "HTTP", "provider", "storage", "git", "publish", "upload"])
def test_worker_request_rejects_every_side_effecting_tool(tool: str) -> None:
    with pytest.raises(ValidationError, match="side-effecting worker tools are forbidden"):
        worker_request(pure_tools=["color_math", tool])


@pytest.mark.asyncio
async def test_openai_worker_sends_no_tools_and_validates_strict_json(
    factory_config,
) -> None:
    captured: dict[str, Any] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured.update(json.loads(request.content))
        return httpx.Response(
            200,
            headers={"x-request-id": "worker-http-1"},
            json={
                "model": "worker-test-resolved",
                "system_fingerprint": "fp",
                "choices": [
                    {
                        "message": {"content": worker_result().model_dump_json()},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 50, "completion_tokens": 25},
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = OpenAICompatibleWorker(factory_config, client=client)
    result = await adapter.execute(worker_request())
    await client.aclose()

    assert captured["tools"] == []
    assert captured["response_format"]["json_schema"]["strict"] is True
    assert captured["response_format"]["json_schema"]["schema"]["additionalProperties"] is False
    assert "no tools" in captured["messages"][0]["content"].lower()
    user_parts = captured["messages"][1]["content"]
    assert any(part.get("image_url", {}).get("url") == "data:image/png;base64,ZXhhY3QtcGl4ZWxz" for part in user_parts)
    request_text = user_parts[0]["text"]
    assert "Do exact authoring." in request_text
    assert "sha256:" + "b" * 64 in request_text
    assert "exact-pixels" not in request_text
    assert isinstance(result.value, WorkerResult)
    assert result.request_id == "worker-http-1"
    assert result.resolved_model == "worker-test-resolved"
    assert result.usage == {"input_tokens": 50, "output_tokens": 25, "cost_micros": 0}


@pytest.mark.asyncio
async def test_openai_worker_rejects_tool_calls_even_when_content_is_valid(factory_config) -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "choices": [
                    {
                        "message": {
                            "content": worker_result().model_dump_json(),
                            "tool_calls": [{"function": {"name": "shell"}}],
                        }
                    }
                ]
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    adapter = OpenAICompatibleWorker(factory_config, client=client)
    with pytest.raises(ProviderError) as captured:
        await adapter.execute(worker_request())
    await client.aclose()
    assert captured.value.kind == ProviderFailureKind.INVALID_OUTPUT
    assert "side-effecting tool call" in str(captured.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("body", "message"),
    [
        ({"choices": []}, "no choices"),
        ({"choices": [{"message": {"content": {"not": "text"}}}]}, "not JSON text"),
        ({"choices": [{"message": {"content": "{}"}}]}, "violated WorkerResult"),
    ],
)
async def test_openai_worker_rejects_malformed_provider_output(factory_config, body, message) -> None:
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, json=body)))
    adapter = OpenAICompatibleWorker(factory_config, client=client)
    with pytest.raises(ProviderError, match=message):
        await adapter.execute(worker_request())
    await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("content", [b"not-json", b"[]"])
async def test_openai_worker_types_invalid_http_json_as_provider_output_failure(factory_config, content) -> None:
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, content=content, headers={"x-request-id": "bad-json"})
        )
    )
    adapter = OpenAICompatibleWorker(factory_config, client=client)
    with pytest.raises(ProviderError) as captured:
        await adapter.execute(worker_request())
    await client.aclose()
    assert captured.value.kind == ProviderFailureKind.INVALID_OUTPUT
    assert captured.value.request_id == "bad-json"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "kind", "outcome_known"),
    [
        (429, ProviderFailureKind.QUOTA, False),
        (500, ProviderFailureKind.UNAVAILABLE, False),
        (422, ProviderFailureKind.INVALID_OUTPUT, True),
    ],
)
async def test_openai_worker_http_failures_are_typed(factory_config, status, kind, outcome_known) -> None:
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(status, text="injected")))
    adapter = OpenAICompatibleWorker(factory_config, client=client)
    with pytest.raises(ProviderError) as captured:
        await adapter.execute(worker_request())
    await client.aclose()
    assert captured.value.kind == kind
    assert captured.value.outcome_known is outcome_known


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "kind", "outcome_known"),
    [
        (httpx.ConnectTimeout("connect timeout"), ProviderFailureKind.TIMEOUT, True),
        (httpx.PoolTimeout("pool timeout"), ProviderFailureKind.TIMEOUT, True),
        (httpx.ReadTimeout("read timeout"), ProviderFailureKind.TIMEOUT, False),
        (httpx.RemoteProtocolError("truncated response"), ProviderFailureKind.UNKNOWN_OUTCOME, False),
    ],
)
async def test_openai_worker_transport_preserves_at_most_once_boundary(
    factory_config, error, kind, outcome_known
) -> None:
    def fail(request: httpx.Request) -> httpx.Response:
        error.request = request
        raise error

    client = httpx.AsyncClient(transport=httpx.MockTransport(fail))
    adapter = OpenAICompatibleWorker(factory_config, client=client)
    with pytest.raises(ProviderError) as captured:
        await adapter.execute(worker_request())
    await client.aclose()
    assert captured.value.kind == kind
    assert captured.value.outcome_known is outcome_known


@pytest.mark.asyncio
async def test_openai_worker_model_probe_requires_the_pinned_model(factory_config) -> None:
    responses = iter(
        [
            httpx.Response(200, json={"data": [{"id": "worker-test", "owned_by": "local"}]}),
            httpx.Response(200, json={"data": [{"id": "different"}]}),
        ]
    )
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: next(responses)))
    adapter = OpenAICompatibleWorker(factory_config, client=client)
    assert (await adapter.describe_model())["id"] == "worker-test"
    with pytest.raises(ProviderError) as captured:
        await adapter.describe_model()
    await client.aclose()
    assert captured.value.kind == ProviderFailureKind.UNAVAILABLE


@pytest.mark.asyncio
async def test_gemini_structured_contract_pins_schema_thinking_model_and_cost(factory_config, monkeypatch) -> None:
    monkeypatch.setenv("TEST_GEMINI_KEY", "gemini-secret")
    captured: dict[str, Any] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["x-goog-api-key"] == "gemini-secret"
        assert request.url.path.endswith("/models/gemini-3.7-flash:generateContent")
        captured.update(json.loads(request.content))
        return httpx.Response(
            200,
            headers={"x-goog-request-id": "gemini-request"},
            json={
                "modelVersion": "gemini-3.7-flash-202608",
                "candidates": [{"content": {"parts": [{"text": '{"answer":"yes"}'}]}, "finishReason": "STOP"}],
                "usageMetadata": {"promptTokenCount": 2_000_000, "candidatesTokenCount": 1_000_000},
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    provider = GeminiProvider(factory_config.models.smart_text, client=client)
    result = await provider.generate_structured(
        system="Judge exactly.",
        prompt="Input",
        schema={"type": "object", "properties": {"answer": {"type": "string"}}},
        images=[("image/png", b"pixels")],
    )
    await client.aclose()

    generation = captured["generationConfig"]
    assert generation["thinkingConfig"] == {"thinkingLevel": "high"}
    assert generation["responseMimeType"] == "application/json"
    assert generation["responseJsonSchema"]["type"] == "object"
    assert captured["contents"][0]["parts"][1]["inlineData"]["data"] == "cGl4ZWxz"
    assert result.value == {"answer": "yes"}
    assert result.request_id == "gemini-request"
    assert result.resolved_model == "gemini-3.7-flash-202608"
    assert result.usage["cost_micros"] == 400


@pytest.mark.asyncio
async def test_gemini_rejects_json_that_violates_the_requested_schema(factory_config, monkeypatch) -> None:
    monkeypatch.setenv("TEST_GEMINI_KEY", "gemini-secret")
    response = httpx.Response(
        200,
        headers={"x-goog-request-id": "gemini-schema-violation"},
        json={
            "modelVersion": "gemini-3.7-flash-202608",
            "candidates": [{"content": {"parts": [{"text": '{"answer":42}'}]}, "finishReason": "STOP"}],
        },
    )
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: response))
    provider = GeminiProvider(factory_config.models.smart_text, client=client)
    with pytest.raises(ProviderError, match="violated its JSON schema") as captured:
        await provider.generate_structured(
            system="Return the requested shape.",
            prompt="Input",
            schema={
                "type": "object",
                "properties": {"answer": {"type": "string"}},
                "required": ["answer"],
                "additionalProperties": False,
            },
        )
    await client.aclose()
    assert captured.value.kind == ProviderFailureKind.INVALID_OUTPUT
    assert captured.value.request_id == "gemini-schema-violation"


@pytest.mark.asyncio
async def test_gemini_image_returns_exact_bytes_and_image_cost(factory_config, monkeypatch) -> None:
    monkeypatch.setenv("TEST_GEMINI_KEY", "gemini-secret")
    exact = b"generated exact pixels"
    response = httpx.Response(
        200,
        json={
            "modelVersion": "gemini-3-pro-image-pinned",
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": "done"},
                            {
                                "inlineData": {
                                    "mimeType": "image/png",
                                    "data": base64.b64encode(exact).decode(),
                                }
                            },
                        ]
                    }
                }
            ],
            "usageMetadata": {},
        },
    )
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: response))
    provider = GeminiProvider(factory_config.models.image_generator, client=client)
    result = await provider.generate_image(
        prompt="snake", references=[("image/png", b"prototype")], aspect_ratio="16:9", image_size="2K"
    )
    await client.aclose()
    assert result.value["image"] == exact
    assert result.value["media_type"] == "image/png"
    assert result.usage["cost_micros"] == 10_000


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "kind", "outcome_known"),
    [
        (429, ProviderFailureKind.QUOTA, False),
        (503, ProviderFailureKind.UNAVAILABLE, False),
        (400, ProviderFailureKind.INVALID_OUTPUT, True),
        (403, ProviderFailureKind.REFUSAL, True),
    ],
)
async def test_gemini_http_failures_are_typed(factory_config, monkeypatch, status, kind, outcome_known) -> None:
    monkeypatch.setenv("TEST_GEMINI_KEY", "gemini-secret")
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(status, json={"error": {"message": "injected"}}))
    )
    provider = GeminiProvider(factory_config.models.smart_text, client=client)
    with pytest.raises(ProviderError) as captured:
        await provider.generate_structured(system="x", prompt="y", schema={"type": "object"})
    await client.aclose()
    assert captured.value.kind == kind
    assert captured.value.outcome_known is outcome_known


@pytest.mark.asyncio
async def test_gemini_requires_credentials_and_types_success_with_invalid_json(factory_config, monkeypatch) -> None:
    monkeypatch.delenv("TEST_GEMINI_KEY", raising=False)
    missing_key = GeminiProvider(
        factory_config.models.smart_text,
        client=httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200))),
    )
    with pytest.raises(ProviderError) as captured:
        await missing_key.describe_model()
    await missing_key.client.aclose()
    assert captured.value.kind == ProviderFailureKind.UNAVAILABLE

    monkeypatch.setenv("TEST_GEMINI_KEY", "gemini-secret")
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                content=b"not-json",
                headers={"x-goog-request-id": "gemini-bad-json"},
            )
        )
    )
    provider = GeminiProvider(factory_config.models.smart_text, client=client)
    with pytest.raises(ProviderError) as invalid:
        await provider.generate_structured(system="x", prompt="y", schema={"type": "object"})
    await client.aclose()
    assert invalid.value.kind == ProviderFailureKind.INVALID_OUTPUT
    assert invalid.value.request_id == "gemini-bad-json"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("body", "message"),
    [
        (
            {"promptFeedback": {"blockReason": "SAFETY"}, "candidates": []},
            "refused",
        ),
        (
            {"candidates": [{"content": {"parts": [{"inlineData": {"mimeType": "image/png", "data": "!!!"}}]}}]},
            "image payload is invalid",
        ),
        ({"candidates": [{"content": {"parts": [{"text": "no pixels"}]}}]}, "did not include an image"),
    ],
)
async def test_gemini_image_rejects_refusal_invalid_base64_and_missing_pixels(
    factory_config, monkeypatch, body, message
) -> None:
    monkeypatch.setenv("TEST_GEMINI_KEY", "gemini-secret")
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, json=body)))
    provider = GeminiProvider(factory_config.models.image_generator, client=client)
    with pytest.raises(ProviderError, match=message) as captured:
        await provider.generate_image(prompt="snake")
    await client.aclose()
    assert captured.value.kind in {ProviderFailureKind.REFUSAL, ProviderFailureKind.INVALID_OUTPUT}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "known"),
    [
        (httpx.ConnectTimeout("connect timeout"), True),
        (httpx.PoolTimeout("pool timeout"), True),
        (httpx.ReadTimeout("read timeout"), False),
        (httpx.RemoteProtocolError("truncated response"), False),
    ],
)
async def test_gemini_timeout_preserves_known_outcome_boundary(factory_config, monkeypatch, error, known) -> None:
    monkeypatch.setenv("TEST_GEMINI_KEY", "gemini-secret")

    def fail(request: httpx.Request) -> httpx.Response:
        error.request = request
        raise error

    client = httpx.AsyncClient(transport=httpx.MockTransport(fail))
    provider = GeminiProvider(factory_config.models.smart_text, client=client)
    with pytest.raises(ProviderError) as captured:
        await provider.generate_structured(system="x", prompt="y", schema={"type": "object"})
    await client.aclose()
    expected = (
        ProviderFailureKind.UNKNOWN_OUTCOME
        if isinstance(error, httpx.RemoteProtocolError)
        else ProviderFailureKind.TIMEOUT
    )
    assert captured.value.kind == expected
    assert captured.value.outcome_known is known


@pytest.mark.asyncio
async def test_openai_content_structured_contract_is_strict_multimodal_and_costed(
    monkeypatch,
) -> None:
    monkeypatch.setenv("TEST_OPENAI_COMPATIBLE_KEY", "compatible-secret")
    captured: dict[str, Any] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/chat/completions"
        assert request.headers["authorization"] == "Bearer compatible-secret"
        captured.update(json.loads(request.content))
        return httpx.Response(
            200,
            headers={"x-request-id": "compatible-request", "x-secret-provider-header": "do-not-retain"},
            json={
                "id": "chatcmpl-test",
                "model": "pinned-image-model-2026-08-21",
                "system_fingerprint": "fp-safe",
                "choices": [
                    {
                        "message": {"role": "assistant", "content": '{"answer":"yes"}'},
                        "finish_reason": "stop",
                    }
                ],
                "usage": {"prompt_tokens": 2_000_000, "completion_tokens": 1_000_000},
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    provider = OpenAICompatibleProvider(openai_role(thinking_level="high"), client=client)
    result = await provider.generate_structured(
        system="Judge exactly.",
        prompt="Input",
        schema={
            "type": "object",
            "properties": {"answer": {"type": "string", "enum": ["yes", "no"]}},
            "required": ["answer"],
            "additionalProperties": False,
        },
        images=[("image/png", b"pixels")],
        temperature=0.2,
    )
    await client.aclose()

    response_format = captured["response_format"]["json_schema"]
    assert response_format["strict"] is True
    assert response_format["schema"]["additionalProperties"] is False
    assert captured["model"] == "pinned-image-model"
    assert captured["temperature"] == 0.2
    assert captured["reasoning_effort"] == "high"
    assert captured["messages"][1]["content"] == [
        {"type": "text", "text": "Input"},
        {"type": "image_url", "image_url": {"url": "data:image/png;base64,cGl4ZWxz"}},
    ]
    assert result.value == {"answer": "yes"}
    assert result.request_id == "compatible-request"
    assert result.resolved_model == "pinned-image-model-2026-08-21"
    assert result.usage == {
        "input_tokens": 2_000_000,
        "output_tokens": 1_000_000,
        "cost_micros": 400,
    }
    assert result.sanitized_metadata == {
        "finish_reason": "stop",
        "system_fingerprint": "fp-safe",
        "response_headers": {"x-request-id": "compatible-request"},
    }
    assert "compatible-secret" not in json.dumps(result.model_dump(mode="json"))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("body", "kind", "message"),
    [
        (
            {"choices": [{"message": {"content": '{"answer":"maybe"}'}, "finish_reason": "stop"}]},
            ProviderFailureKind.INVALID_OUTPUT,
            "violated its JSON schema",
        ),
        (
            {"choices": [{"message": {"content": "{}", "refusal": "no"}, "finish_reason": "stop"}]},
            ProviderFailureKind.REFUSAL,
            "refused",
        ),
        (
            {
                "choices": [
                    {
                        "message": {"content": "{}", "tool_calls": [{"function": {"name": "shell"}}]},
                        "finish_reason": "tool_calls",
                    }
                ]
            },
            ProviderFailureKind.INVALID_OUTPUT,
            "tool call",
        ),
        ({"choices": []}, ProviderFailureKind.INVALID_OUTPUT, "no choices"),
    ],
)
async def test_openai_content_rejects_nonconforming_structured_responses(monkeypatch, body, kind, message) -> None:
    monkeypatch.setenv("TEST_OPENAI_COMPATIBLE_KEY", "compatible-secret")
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, headers={"request-id": "bad-output"}, json=body))
    )
    provider = OpenAICompatibleProvider(openai_role(), client=client)
    with pytest.raises(ProviderError, match=message) as captured:
        await provider.generate_structured(
            system="judge",
            prompt="input",
            schema={
                "type": "object",
                "properties": {"answer": {"enum": ["yes", "no"]}},
                "required": ["answer"],
                "additionalProperties": False,
            },
        )
    await client.aclose()
    assert captured.value.kind == kind
    if body.get("choices"):
        assert captured.value.request_id in {"bad-output", None}


@pytest.mark.asyncio
async def test_openai_content_generation_returns_exact_retained_image(monkeypatch) -> None:
    monkeypatch.setenv("TEST_OPENAI_COMPATIBLE_KEY", "compatible-secret")
    exact = b"\x89PNG\r\n\x1a\nexact generated pixels"
    captured: dict[str, Any] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/images/generations"
        captured.update(json.loads(request.content))
        return httpx.Response(
            200,
            headers={"openai-request-id": "image-request"},
            json={
                "model": "resolved-image-model",
                "data": [
                    {
                        "b64_json": base64.b64encode(exact).decode(),
                        "revised_prompt": "safe revised prompt",
                    }
                ],
                "usage": {"input_tokens": 25, "output_tokens": 5},
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    provider = OpenAICompatibleProvider(openai_role(), client=client)
    result = await provider.generate_image(prompt="snake", aspect_ratio="16:9", image_size="2K")
    await client.aclose()

    assert captured == {
        "model": "pinned-image-model",
        "prompt": "snake",
        "n": 1,
        "size": "2048x1152",
    }
    assert result.value == {
        "image": exact,
        "media_type": "image/png",
        "text": "safe revised prompt",
    }
    assert result.request_id == "image-request"
    assert result.resolved_model == "resolved-image-model"
    assert result.usage["cost_micros"] == 10_000
    assert result.sanitized_metadata["requested_aspect_ratio"] == "16:9"
    assert result.sanitized_metadata["requested_image_size"] == "2K"


@pytest.mark.asyncio
async def test_openai_content_reference_generation_uses_standard_multipart_edit(monkeypatch) -> None:
    monkeypatch.setenv("TEST_OPENAI_COMPATIBLE_KEY", "compatible-secret")
    exact = b"\xff\xd8\xffexact generated jpeg"
    captured: dict[str, Any] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["content_type"] = request.headers["content-type"]
        captured["body"] = request.content
        return httpx.Response(
            200,
            json={"data": [{"b64_json": base64.b64encode(exact).decode()}]},
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    provider = OpenAICompatibleProvider(openai_role(), client=client)
    result = await provider.generate_image(
        prompt="adapt the approved prototype",
        references=[("image/png", b"first exact ref"), ("image/webp", b"second exact ref")],
        aspect_ratio="9:16",
    )
    await client.aclose()

    assert captured["path"] == "/v1/images/edits"
    assert captured["content_type"].startswith("multipart/form-data; boundary=")
    body = captured["body"]
    assert body.count(b'name="image[]"') == 2
    assert b"first exact ref" in body and b"second exact ref" in body
    assert b'name="size"\r\n\r\n1152x2048' in body
    assert result.value["image"] == exact
    assert result.value["media_type"] == "image/jpeg"
    assert result.sanitized_metadata["endpoint"] == "images/edits"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("body", "message"),
    [
        ({"data": [{"url": "https://unretained.example/image.png"}]}, "retained base64"),
        ({"data": [{"b64_json": "!!!"}]}, "payload is invalid"),
        (
            {"data": [{"b64_json": base64.b64encode(b"not an image").decode()}]},
            "not PNG, JPEG, or WebP",
        ),
        ({"data": []}, "exactly one"),
    ],
)
async def test_openai_content_rejects_unretained_or_invalid_image_output(monkeypatch, body, message) -> None:
    monkeypatch.setenv("TEST_OPENAI_COMPATIBLE_KEY", "compatible-secret")
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, json=body)))
    provider = OpenAICompatibleProvider(openai_role(), client=client)
    with pytest.raises(ProviderError, match=message) as captured:
        await provider.generate_image(prompt="snake")
    await client.aclose()
    assert captured.value.kind == ProviderFailureKind.INVALID_OUTPUT


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "kind", "outcome_known"),
    [
        (429, ProviderFailureKind.QUOTA, False),
        (503, ProviderFailureKind.UNAVAILABLE, False),
        (422, ProviderFailureKind.INVALID_OUTPUT, True),
        (401, ProviderFailureKind.REFUSAL, True),
    ],
)
async def test_openai_content_http_failures_are_typed_and_sanitized(monkeypatch, status, kind, outcome_known) -> None:
    monkeypatch.setenv("TEST_OPENAI_COMPATIBLE_KEY", "compatible-secret")
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                status,
                headers={"x-request-id": "failed-request"},
                json={"error": {"type": "injected", "message": "one\n two"}},
            )
        )
    )
    provider = OpenAICompatibleProvider(openai_role(), client=client)
    with pytest.raises(ProviderError, match="injected: one two") as captured:
        await provider.generate_image(prompt="snake")
    await client.aclose()
    assert captured.value.kind == kind
    assert captured.value.request_id == "failed-request"
    assert captured.value.outcome_known is outcome_known


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "kind", "known"),
    [
        (httpx.ConnectTimeout("connect timeout"), ProviderFailureKind.TIMEOUT, True),
        (httpx.PoolTimeout("pool timeout"), ProviderFailureKind.TIMEOUT, True),
        (httpx.ReadTimeout("read timeout"), ProviderFailureKind.TIMEOUT, False),
        (httpx.RemoteProtocolError("truncated response"), ProviderFailureKind.UNKNOWN_OUTCOME, False),
    ],
)
async def test_openai_content_transport_preserves_at_most_once_boundary(monkeypatch, error, kind, known) -> None:
    monkeypatch.setenv("TEST_OPENAI_COMPATIBLE_KEY", "compatible-secret")

    def fail(request: httpx.Request) -> httpx.Response:
        error.request = request
        raise error

    client = httpx.AsyncClient(transport=httpx.MockTransport(fail))
    provider = OpenAICompatibleProvider(openai_role(), client=client)
    with pytest.raises(ProviderError) as captured:
        await provider.generate_image(prompt="snake")
    await client.aclose()
    assert captured.value.kind == kind
    assert captured.value.outcome_known is known


@pytest.mark.asyncio
async def test_openai_content_model_probe_and_credentials_are_explicit(monkeypatch) -> None:
    monkeypatch.delenv("TEST_OPENAI_COMPATIBLE_KEY", raising=False)
    no_credentials = OpenAICompatibleProvider(
        openai_role(),
        client=httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200))),
    )
    with pytest.raises(ProviderError, match="TEST_OPENAI_COMPATIBLE_KEY is not set") as missing:
        await no_credentials.describe_model()
    await no_credentials.client.aclose()
    assert missing.value.kind == ProviderFailureKind.UNAVAILABLE

    monkeypatch.setenv("TEST_OPENAI_COMPATIBLE_KEY", "compatible-secret")
    responses = iter(
        [
            httpx.Response(200, json={"id": "pinned-image-model", "owned_by": "operator"}),
            httpx.Response(200, json={"id": "unexpected-model"}),
        ]
    )
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: next(responses)))
    provider = OpenAICompatibleProvider(openai_role(), client=client)
    assert (await provider.describe_model())["id"] == "pinned-image-model"
    with pytest.raises(ProviderError, match="resolved as") as mismatch:
        await provider.describe_model()
    await client.aclose()
    assert mismatch.value.kind == ProviderFailureKind.UNAVAILABLE


def test_provider_registry_never_silently_falls_back(factory_config) -> None:
    registry = ProviderRegistry(factory_config)
    first = registry.role("smart_text")
    assert first is registry.role("smart_text")
    factory_config.models.image_generator = openai_role(api_key_env=None)
    compatible = registry.role("image_generator")
    assert isinstance(compatible, OpenAICompatibleProvider)
    assert compatible is registry.role("image_generator")
    factory_config.models.image_editor.provider = "local_lama"
    with pytest.raises(ValueError, match="not a content provider"):
        registry.role("image_editor")
