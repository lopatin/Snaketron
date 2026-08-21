from __future__ import annotations

import hashlib
import json

import httpx
import pytest

from snaketron_factory.assets import ForgeBundle, ForgeVariant
from snaketron_factory.config import FactoryConfig
from snaketron_factory.domain import ProviderError, ProviderFailureKind
from snaketron_factory.snaketron_api import SnaketronApi


def _bundle(data: bytes = b"exact canonical png bytes") -> ForgeBundle:
    content_ref = f"sha256:{hashlib.sha256(data).hexdigest()}"
    variant = ForgeVariant(
        content_ref=content_ref,
        url=f"/api/textures/variants/{content_ref}.png",
        width_px=64,
        height_px=64,
        bytes=len(data),
        texels_per_cell=64,
        data=data,
    )
    descriptor = {
        "kind": "coat",
        "body_columns": 1,
        "frame_rows": None,
        "variants": [
            {
                "content_ref": content_ref,
                "url": variant.url,
                "width_px": 64,
                "height_px": 64,
                "bytes": len(data),
                "texels_per_cell": 64,
            }
        ],
    }
    return ForgeBundle(
        manifest={
            "schema_version": 1,
            "content_ref": content_ref,
            "descriptor": descriptor,
            "seam_axes": ["x"],
            "shareable": False,
        },
        descriptor=descriptor,
        variants=(variant,),
        gate_results=(),
        repaired=False,
    )


@pytest.mark.asyncio
async def test_exact_skin_texture_and_publication_http_contracts(
    factory_config: FactoryConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SNAKETRON_FACTORY_SERVICE_TOKEN", "service-secret")
    monkeypatch.setenv("SNAKETRON_FACTORY_OPERATOR_TOKEN", "operator-secret")
    bundle = _bundle()
    document = {"schema_version": 2, "name": "Exact skin", "layers": []}
    calls: list[tuple[str, str]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        calls.append((request.method, path))
        if path == "/health":
            assert "authorization" not in request.headers
            return httpx.Response(200, json={"status": "ok"})
        if path == "/api/textures/forge":
            assert request.headers["authorization"] == "Bearer service-secret"
            assert request.headers["content-type"].startswith("multipart/form-data; boundary=")
            assert b'name="manifest"' in request.content
            assert b'"seam_axes":["x"]' in request.content
            assert f'filename="{bundle.variants[0].content_ref}.png"'.encode() in request.content
            assert bundle.variants[0].data in request.content
            return httpx.Response(
                201,
                headers={"x-request-id": "forge-request", "x-snaketron-version": "server-v2"},
                json={"contentRef": bundle.manifest["content_ref"]},
            )
        if path.startswith("/api/textures/variants/"):
            assert "authorization" not in request.headers
            return httpx.Response(200, content=bundle.variants[0].data)
        if path == "/api/skins" and request.method == "POST":
            assert request.headers["authorization"] == "Bearer service-secret"
            assert json.loads(request.content) == {
                "name": "Exact skin",
                "kind": "snake",
                "document": document,
                "idempotencyKey": "concept:initial",
                "evaluationOnly": False,
            }
            return httpx.Response(201, json={"id": "skin-1", "revision": 1})
        if path == "/api/skins/skin-1" and request.method == "PUT":
            assert request.headers["authorization"] == "Bearer service-secret"
            assert json.loads(request.content) == {
                "document": document,
                "expectedHeadRevision": 1,
            }
            return httpx.Response(200, json={"id": "skin-1", "revision": 2})
        if path == "/api/admin/skins/skin-1/status":
            assert request.headers["authorization"] == "Bearer operator-secret"
            assert json.loads(request.content) == {
                "publication": "published",
                "revision": 2,
                "contentRef": "sha256:" + "f" * 64,
                "reason": "Exact human approval",
            }
            return httpx.Response(200, json={"publication": "published"})
        if path == "/api/skins/skin-1/publish-request":
            expected_token = "service-secret" if request.method == "POST" else "operator-secret"
            assert request.headers["authorization"] == f"Bearer {expected_token}"
            assert json.loads(request.content) == {
                "revision": 2,
                "contentRef": "sha256:" + "f" * 64,
            }
            return httpx.Response(202 if request.method == "POST" else 204)
        if path.startswith("/api/skins/by-ref/"):
            assert request.headers["authorization"] == "Bearer operator-secret"
            return httpx.Response(200, content=b"exact canonical document")
        raise AssertionError(f"unexpected request: {request.method} {path}")

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    api = SnaketronApi(factory_config, client=client)

    assert await api.health() == {"status": "ok"}
    upload = await api.upload_forge_bundle(bundle)
    assert upload.request_id == "forge-request"
    assert upload.resolved_model == "server-v2"
    assert (await api.verify_forge_bundle(bundle)).value["variants"][0]["bytes"] == len(bundle.variants[0].data)
    assert (
        await api.create_skin(
            name="Exact skin",
            document=document,
            idempotency_key="concept:initial",
        )
    ).value["revision"] == 1
    assert (
        await api.append_revision(
            skin_id="skin-1",
            document=document,
            expected_head_revision=1,
        )
    ).value["revision"] == 2
    assert (
        await api.request_publication_exact(
            skin_id="skin-1",
            revision=2,
            content_ref="sha256:" + "f" * 64,
        )
    ).value == {"accepted": True}
    assert (
        await api.cancel_publication_request_exact(
            skin_id="skin-1",
            revision=2,
            content_ref="sha256:" + "f" * 64,
        )
    ).value == {"cancelled": True}
    assert (
        await api.publish_exact(
            skin_id="skin-1",
            revision=2,
            content_ref="sha256:" + "f" * 64,
            reason="Exact human approval",
        )
    ).value["publication"] == "published"
    assert await api.get_skin_document("sha256:" + "f" * 64, as_operator=True) == b"exact canonical document"
    await client.aclose()

    assert calls == [
        ("GET", "/health"),
        ("POST", "/api/textures/forge"),
        ("GET", f"/api/textures/variants/{bundle.variants[0].content_ref}.png"),
        ("POST", "/api/skins"),
        ("PUT", "/api/skins/skin-1"),
        ("POST", "/api/skins/skin-1/publish-request"),
        ("DELETE", "/api/skins/skin-1/publish-request"),
        ("PUT", "/api/admin/skins/skin-1/status"),
        ("GET", "/api/skins/by-ref/sha256:" + "f" * 64),
    ]


@pytest.mark.asyncio
async def test_evaluation_skin_create_sets_the_non_publishable_server_namespace(
    factory_config: FactoryConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SNAKETRON_FACTORY_SERVICE_TOKEN", "service-secret")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/skins"
        assert json.loads(request.content) == {
            "name": "Trial",
            "kind": "snake",
            "document": {"schema_version": 2, "name": "Trial", "layers": []},
            "idempotencyKey": "factory-trial:attempt-1",
            "evaluationOnly": True,
        }
        return httpx.Response(
            201,
            json={
                "skinId": "evaluation-1",
                "headRevision": 1,
                "contentRef": "sha256:" + "e" * 64,
                "namespace": "evaluation",
            },
        )

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    api = SnaketronApi(factory_config, client=client)
    result = await api.create_skin(
        name="Trial",
        document={"schema_version": 2, "name": "Trial", "layers": []},
        idempotency_key="factory-trial:attempt-1",
        evaluation_only=True,
    )
    await client.aclose()

    assert result.value["namespace"] == "evaluation"


@pytest.mark.asyncio
async def test_verify_forge_bundle_rejects_reencoded_or_misnamed_bytes(
    factory_config: FactoryConfig,
) -> None:
    bundle = _bundle()
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, content=b"different bytes")))
    api = SnaketronApi(factory_config, client=client)

    with pytest.raises(ProviderError, match="returned bytes named") as captured:
        await api.verify_forge_bundle(bundle)
    await client.aclose()
    assert captured.value.kind == ProviderFailureKind.INVALID_OUTPUT


@pytest.mark.asyncio
async def test_empty_forge_bundle_is_rejected_before_http(factory_config: FactoryConfig) -> None:
    api = SnaketronApi(
        factory_config,
        client=httpx.AsyncClient(transport=httpx.MockTransport(lambda _: pytest.fail("HTTP called"))),
    )
    empty = ForgeBundle({}, {}, (), (), False)

    with pytest.raises(ValueError, match="empty forge bundle"):
        await api.upload_forge_bundle(empty)
    await api.client.aclose()


@pytest.mark.asyncio
async def test_health_accepts_a_plain_text_probe(factory_config: FactoryConfig) -> None:
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, text="healthy")))
    api = SnaketronApi(factory_config, client=client)
    assert await api.health() == {"status": "healthy"}
    await client.aclose()


@pytest.mark.asyncio
async def test_missing_service_or_operator_tokens_fail_closed(
    factory_config: FactoryConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SNAKETRON_FACTORY_SERVICE_TOKEN", raising=False)
    monkeypatch.delenv("SNAKETRON_FACTORY_OPERATOR_TOKEN", raising=False)
    api = SnaketronApi(factory_config, client=httpx.AsyncClient())

    with pytest.raises(ProviderError, match="SNAKETRON_FACTORY_SERVICE_TOKEN is not set"):
        api._headers()
    with pytest.raises(ProviderError, match="SNAKETRON_FACTORY_OPERATOR_TOKEN is not set"):
        api._headers(operator=True)
    await api.client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "kind"),
    [
        (429, ProviderFailureKind.QUOTA),
        (503, ProviderFailureKind.UNAVAILABLE),
        (409, ProviderFailureKind.INVALID_OUTPUT),
        (403, ProviderFailureKind.REFUSAL),
    ],
)
async def test_http_failures_are_typed_and_keep_the_request_id(
    factory_config: FactoryConfig,
    status: int,
    kind: ProviderFailureKind,
) -> None:
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(status, headers={"x-amzn-requestid": "failed-request"}, text="safe error")
        )
    )
    api = SnaketronApi(factory_config, client=client)

    with pytest.raises(ProviderError, match=f"HTTP {status}: safe error") as captured:
        await api.health()
    await client.aclose()
    assert captured.value.kind == kind
    assert captured.value.request_id == "failed-request"
    assert captured.value.outcome_known is True


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "kind", "known"),
    [
        (httpx.ConnectError("offline"), ProviderFailureKind.UNAVAILABLE, True),
        (httpx.ConnectTimeout("connect timeout"), ProviderFailureKind.TIMEOUT, True),
        (httpx.PoolTimeout("pool timeout"), ProviderFailureKind.TIMEOUT, True),
        (httpx.ReadTimeout("read timeout"), ProviderFailureKind.TIMEOUT, True),
        (httpx.RemoteProtocolError("truncated response"), ProviderFailureKind.UNAVAILABLE, True),
    ],
)
async def test_transport_failures_preserve_the_at_most_once_boundary(
    factory_config: FactoryConfig,
    error: httpx.HTTPError,
    kind: ProviderFailureKind,
    known: bool,
) -> None:
    def fail(request: httpx.Request) -> httpx.Response:
        error.request = request
        raise error

    client = httpx.AsyncClient(transport=httpx.MockTransport(fail))
    api = SnaketronApi(factory_config, client=client)

    with pytest.raises(ProviderError) as captured:
        await api.health()
    await client.aclose()
    assert captured.value.kind == kind
    assert captured.value.outcome_known is known


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "kind"),
    [
        (408, ProviderFailureKind.UNAVAILABLE),
        (429, ProviderFailureKind.QUOTA),
        (503, ProviderFailureKind.UNAVAILABLE),
    ],
)
async def test_write_http_retry_responses_require_reconciliation(
    factory_config: FactoryConfig,
    monkeypatch: pytest.MonkeyPatch,
    status: int,
    kind: ProviderFailureKind,
) -> None:
    monkeypatch.setenv("SNAKETRON_FACTORY_SERVICE_TOKEN", "service-secret")
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(status, text="ambiguous write")))
    api = SnaketronApi(factory_config, client=client)

    with pytest.raises(ProviderError, match="ambiguous write") as captured:
        await api.create_skin(
            name="Ambiguous",
            document={"schema_version": 2, "name": "Ambiguous", "layers": []},
            idempotency_key="ambiguous:create",
        )
    await client.aclose()
    assert captured.value.kind == kind
    assert captured.value.outcome_known is False


@pytest.mark.asyncio
async def test_post_send_write_transport_failure_is_unknown_but_connect_and_pool_are_safe(
    factory_config: FactoryConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SNAKETRON_FACTORY_SERVICE_TOKEN", "service-secret")
    errors: list[tuple[httpx.HTTPError, ProviderFailureKind, bool]] = [
        (httpx.ConnectError("offline"), ProviderFailureKind.UNAVAILABLE, True),
        (httpx.PoolTimeout("pool"), ProviderFailureKind.TIMEOUT, True),
        (httpx.ReadTimeout("after send"), ProviderFailureKind.TIMEOUT, False),
        (httpx.RemoteProtocolError("truncated"), ProviderFailureKind.UNKNOWN_OUTCOME, False),
    ]
    for error, kind, known in errors:

        def fail(request: httpx.Request, *, injected=error) -> httpx.Response:
            injected.request = request
            raise injected

        client = httpx.AsyncClient(transport=httpx.MockTransport(fail))
        api = SnaketronApi(factory_config, client=client)
        with pytest.raises(ProviderError) as captured:
            await api.append_revision(
                skin_id="skin-1",
                document={"schema_version": 2, "name": "Exact", "layers": []},
                expected_head_revision=1,
            )
        await client.aclose()
        assert captured.value.kind == kind
        assert captured.value.outcome_known is known


@pytest.mark.asyncio
async def test_human_override_publication_request_uses_operator_authority(
    factory_config: FactoryConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SNAKETRON_FACTORY_OPERATOR_TOKEN", "operator-secret")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer operator-secret"
        return httpx.Response(202)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    api = SnaketronApi(factory_config, client=client)
    result = await api.request_publication_exact(
        skin_id="skin-operator",
        revision=4,
        content_ref="sha256:" + "4" * 64,
        operator=True,
    )
    await client.aclose()
    assert result.value == {"accepted": True}
