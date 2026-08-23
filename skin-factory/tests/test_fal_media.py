from __future__ import annotations

import asyncio
import hashlib
import io
import json
import struct
from typing import Any

import httpx
import pytest
from PIL import Image

from snaketron_factory.domain import OperationStatus, ProviderError, ProviderFailureKind, ProviderResult, Stage
from snaketron_factory.fal_media import (
    PIXVERSE_TRANSITION_CAPABILITY,
    FalPixVerseTransitionAdapter,
    FalQueueTicket,
    PixVerseTransitionOptions,
    validate_pixverse_video_result,
)
from snaketron_factory.operations import OperationJournal
from snaketron_factory.persistence import ResultPersistence


def png_bytes(color: tuple[int, int, int, int]) -> bytes:
    output = io.BytesIO()
    Image.new("RGBA", (32, 16), color).save(output, format="PNG")
    return output.getvalue()


def content_ref(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def media_request(start: bytes, end: bytes, *, prompt: str | None = None) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "request_id": "generate_b1_video",
        "operation": "generate_video",
        "capability_id": PIXVERSE_TRANSITION_CAPABILITY,
        "input_artifacts": [content_ref(start), content_ref(end)],
        "logical_key": "body_transition",
        "component_key": "B1",
        "prompt": prompt
        or (
            "[Cinematography]\nStatic orthographic camera; flat 2D framing.\n"
            "[Subject]\nPreserve the exact supplied B1 component.\n"
            "[Action / Transition]\nOnly the B1 component animates through a true loop.\n"
            "[Context]\nKeep the reserved matte arena and background completely static.\n"
            "[Style & Ambiance]\nPreserve exact colors and alpha/matte boundaries."
        ),
        "output_kind": "video",
        "journal": {
            "retain_inputs": True,
            "retain_provider_output": True,
            "retain_output": True,
            "retain_reports": True,
        },
        "extraction": None,
        "video": {
            "start_frame_sha256": content_ref(start),
            "end_frame_sha256": content_ref(end),
            "source_video_sha256": None,
            "common_period_ms": 1_000,
            "desired_fps": 2,
            "derived_frame_rows": 2,
            "body_columns": 16,
            "texels_per_cell": 16,
            "raster_overhang_px": 0,
            "row_texels": 16,
            "effective_frame_row_cap": 120,
            "frame_extraction": "deterministic_uniform_full_period",
            "row_zero": "resting_and_reduced_motion",
            "alpha_matte_verification": "fail_closed",
            "loop_closure": "true_final_to_zero",
            "max_frame_rows": 120,
        },
        "reuse": None,
    }


def mp4_box(kind: bytes, payload: bytes = b"") -> bytes:
    return struct.pack(">I4s", len(payload) + 8, kind) + payload


def valid_mp4() -> bytes:
    return b"".join(
        (
            mp4_box(b"ftyp", b"isom\x00\x00\x02\x00isommp42"),
            mp4_box(b"mdat", b"exact-video-payload"),
            mp4_box(b"moov"),
        )
    )


def retained_video_result(value: bytes, *, request_id: str = "retained-video") -> ProviderResult:
    return ProviderResult(
        value=value,
        request_id=request_id,
        resolved_model=PIXVERSE_TRANSITION_CAPABILITY,
        sanitized_metadata={
            "video": {
                "content_sha256": content_ref(value),
                "bytes": len(value),
                "byte_limit": 100_000_000,
                "reported_file": {
                    "content_type": "video/mp4",
                    "content_type_valid_string": True,
                    "file_size": len(value),
                    "file_size_valid_integer": True,
                    "file_name": "output.mp4",
                    "file_name_valid": True,
                },
                "download": {
                    "content_type": "video/mp4",
                    "content_length": len(value),
                    "content_length_valid_integer": True,
                },
            }
        },
    )


def public_resolver(_: str) -> list[str]:
    return ["93.184.216.34"]


@pytest.mark.asyncio
async def test_submit_is_exact_secret_free_and_poll_downloads_validated_mp4(monkeypatch) -> None:
    start = png_bytes((10, 20, 30, 255))
    end = png_bytes((30, 20, 10, 255))
    request = media_request(start, end)
    video = valid_mp4()
    api_calls: list[httpx.Request] = []
    download_calls: list[httpx.Request] = []
    statuses = iter(("IN_QUEUE", "COMPLETED"))

    def api_handler(http_request: httpx.Request) -> httpx.Response:
        api_calls.append(http_request)
        assert http_request.headers["authorization"] == "Key unit-test-fal-secret"
        if http_request.method == "POST":
            assert http_request.url.path == "/fal-ai/pixverse/v6/transition"
            payload = json.loads(http_request.content)
            assert payload["first_image_url"].startswith("data:image/png;base64,")
            assert payload["end_image_url"].startswith("data:image/png;base64,")
            assert payload["generate_audio_switch"] is False
            assert payload["generate_multi_clip_switch"] is False
            assert payload["thinking_type"] == "disabled"
            return httpx.Response(200, json={"request_id": "fal-request-123"}, headers={"x-request-id": "gateway-1"})
        if http_request.url.path.endswith("/status"):
            return httpx.Response(200, json={"status": next(statuses)})
        assert http_request.url.path.endswith("/requests/fal-request-123")
        return httpx.Response(
            200,
            json={
                "video": {
                    "url": "https://v3b.fal.media/output.mp4?signed=yes",
                    "content_type": "video/mp4",
                    "file_name": "output.mp4",
                    "file_size": len(video),
                }
            },
        )

    def download_handler(http_request: httpx.Request) -> httpx.Response:
        download_calls.append(http_request)
        assert "authorization" not in http_request.headers
        return httpx.Response(
            200,
            content=video,
            headers={"content-type": "video/mp4", "content-length": str(len(video)), "etag": "exact-etag"},
        )

    monkeypatch.setenv("FAL_API_KEY", "unit-test-fal-secret")
    monkeypatch.delenv("FAL_KEY", raising=False)
    async with httpx.AsyncClient(transport=httpx.MockTransport(api_handler)) as api_client:  # noqa: SIM117
        async with httpx.AsyncClient(transport=httpx.MockTransport(download_handler)) as download_client:
            adapter = FalPixVerseTransitionAdapter(
                api_client=api_client,
                download_client=download_client,
                host_resolver=public_resolver,
                poll_interval_seconds=0,
            )
            options = PixVerseTransitionOptions(seed=314159, reservation_micros=1_000_000)
            journal_request = adapter.submit_journal_request(
                request,
                start_frame=start,
                start_media_type="image/png",
                end_frame=end,
                end_media_type="image/png",
                options=options,
            )
            serialized = json.dumps(journal_request)
            assert "unit-test-fal-secret" not in serialized
            assert "data:image" not in serialized
            assert start not in OperationJournal.request_payload(journal_request)
            assert adapter.credential_env_name() == "FAL_API_KEY"

            submitted = await adapter.submit_transition(
                request,
                start_frame=start,
                start_media_type="image/png",
                end_frame=end,
                end_media_type="image/png",
                options=options,
            )
            assert submitted.value == {
                "schema_version": 1,
                "capability_id": PIXVERSE_TRANSITION_CAPABILITY,
                "request_id": "fal-request-123",
            }
            assert submitted.request_id == "fal-request-123"

            completed = await adapter.poll_transition(submitted.value)
            validate_pixverse_video_result(completed)

    assert completed.value == video
    assert completed.sanitized_metadata["video"]["content_sha256"] == content_ref(video)
    assert completed.sanitized_metadata["video"]["container"] == {
        "format": "ISO-BMFF",
        "major_brand": "isom",
        "top_level_box_count": 3,
        "top_level_boxes": ["ftyp", "mdat", "moov"],
        "top_level_boxes_truncated": False,
    }
    assert completed.sanitized_metadata["status_poll_count"] == 2
    assert len(api_calls) == 4
    assert len(download_calls) == 1


@pytest.mark.asyncio
async def test_submit_ticket_is_persisted_and_journal_replay_never_resubmits(
    monkeypatch,
    database,
    objects,
    make_attempt,
) -> None:
    start = png_bytes((1, 2, 3, 255))
    end = png_bytes((3, 2, 1, 255))
    request = media_request(start, end)
    calls = 0

    def api_handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"request_id": "durable-fal-ticket"})

    monkeypatch.setenv("FAL_API_KEY", "journal-test-secret")
    attempt = make_attempt(stage=Stage.ASSETS)
    journal = OperationJournal(database)
    persistence = ResultPersistence(objects)
    async with httpx.AsyncClient(transport=httpx.MockTransport(api_handler)) as api_client:
        adapter = FalPixVerseTransitionAdapter(api_client=api_client, poll_interval_seconds=0)
        options = PixVerseTransitionOptions(seed=7, reservation_micros=1_000_000)
        exact_request = adapter.submit_journal_request(
            request,
            start_frame=start,
            start_media_type="image/png",
            end_frame=end,
            end_media_type="image/png",
            options=options,
        )

        async def invoke():
            return await adapter.submit_transition(
                request,
                start_frame=start,
                start_media_type="image/png",
                end_frame=end,
                end_media_type="image/png",
                options=options,
            )

        operation, returned = await journal.run_provider(
            attempt_id=attempt["id"],
            stage=Stage.ASSETS,
            idempotency_key="fal-submit:generate-b1-video",
            side_effect="submit_pixverse_transition",
            provider_role="fal_pixverse_transition",
            request=exact_request,
            reserve_micros=500_000,
            invoke=invoke,
            persist_result=persistence,
        )
        replay, replay_result = await journal.run_provider(
            attempt_id=attempt["id"],
            stage=Stage.ASSETS,
            idempotency_key="fal-submit:generate-b1-video",
            side_effect="submit_pixverse_transition",
            provider_role="fal_pixverse_transition",
            request=exact_request,
            reserve_micros=500_000,
            invoke=lambda: pytest.fail("persisted Fal submit must never run twice"),
            persist_result=persistence,
        )

    assert calls == 1
    assert returned is not None
    assert replay_result is None
    assert replay["id"] == operation["id"]
    assert operation["provider_request_id"] == "durable-fal-ticket"
    assert persistence.load_json(operation["result_hash"]) == returned.value


@pytest.mark.asyncio
async def test_documented_fal_key_is_the_fallback_and_missing_key_never_calls_http(monkeypatch) -> None:
    start = png_bytes((1, 1, 1, 255))
    end = png_bytes((2, 2, 2, 255))
    request = media_request(start, end)
    observed_authorization: list[str] = []

    def handler(http_request: httpx.Request) -> httpx.Response:
        observed_authorization.append(http_request.headers["authorization"])
        return httpx.Response(200, json={"request_id": "fallback-key-request"})

    monkeypatch.delenv("FAL_API_KEY", raising=False)
    monkeypatch.setenv("FAL_KEY", "documented-fallback-secret")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = FalPixVerseTransitionAdapter(api_client=client)
        assert adapter.credential_env_name() == "FAL_KEY"
        await adapter.submit_transition(
            request,
            start_frame=start,
            start_media_type="image/png",
            end_frame=end,
            end_media_type="image/png",
            options=PixVerseTransitionOptions(seed=1, reservation_micros=1_000_000),
        )
    assert observed_authorization == ["Key documented-fallback-secret"]

    calls = 0

    def must_not_call(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    monkeypatch.delenv("FAL_KEY")
    async with httpx.AsyncClient(transport=httpx.MockTransport(must_not_call)) as client:
        adapter = FalPixVerseTransitionAdapter(api_client=client)
        with pytest.raises(ProviderError) as captured:
            await adapter.submit_transition(
                request,
                start_frame=start,
                start_media_type="image/png",
                end_frame=end,
                end_media_type="image/png",
                options=PixVerseTransitionOptions(seed=1, reservation_micros=1_000_000),
            )
    assert captured.value.kind == ProviderFailureKind.AUTHENTICATION
    assert "FAL_API_KEY" in str(captured.value) and "FAL_KEY" in str(captured.value)
    assert calls == 0


@pytest.mark.asyncio
async def test_invalid_hash_prompt_and_capability_fail_before_submit(monkeypatch) -> None:
    start = png_bytes((1, 2, 3, 255))
    end = png_bytes((3, 2, 1, 255))
    calls = 0

    def must_not_call(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    monkeypatch.setenv("FAL_API_KEY", "unused-secret")
    async with httpx.AsyncClient(transport=httpx.MockTransport(must_not_call)) as client:
        adapter = FalPixVerseTransitionAdapter(api_client=client)
        wrong_hash = media_request(start, end)
        wrong_hash["video"]["start_frame_sha256"] = "sha256:" + "0" * 64
        wrong_hash["input_artifacts"][0] = "sha256:" + "0" * 64
        with pytest.raises(ProviderError, match="do not match"):
            await adapter.submit_transition(
                wrong_hash,
                start_frame=start,
                start_media_type="image/png",
                end_frame=end,
                end_media_type="image/png",
                options=PixVerseTransitionOptions(seed=1, reservation_micros=1_000_000),
            )

        huge_prompt = media_request(
            start,
            end,
            prompt="[Cinematography]" + "x" * 2_049 + "[Subject][Action / Transition][Context][Style & Ambiance]",
        )
        with pytest.raises(ProviderError, match="2048 UTF-8 bytes"):
            adapter.submit_journal_request(
                huge_prompt,
                start_frame=start,
                start_media_type="image/png",
                end_frame=end,
                end_media_type="image/png",
                options=PixVerseTransitionOptions(seed=1, reservation_micros=1_000_000),
            )

        wrong_capability = media_request(start, end)
        wrong_capability["capability_id"] = "fal-ai/another-model"
        with pytest.raises(ProviderError, match="exact journaled PixVerse"):
            adapter.submit_journal_request(
                wrong_capability,
                start_frame=start,
                start_media_type="image/png",
                end_frame=end,
                end_media_type="image/png",
                options=PixVerseTransitionOptions(seed=1, reservation_micros=1_000_000),
            )
    assert calls == 0


@pytest.mark.asyncio
async def test_malformed_submit_success_is_unknown_and_cannot_be_blindly_retried(monkeypatch) -> None:
    start = png_bytes((1, 2, 3, 255))
    end = png_bytes((3, 2, 1, 255))
    monkeypatch.setenv("FAL_API_KEY", "test-secret")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"status": "IN_QUEUE"}))
    ) as client:
        adapter = FalPixVerseTransitionAdapter(api_client=client)
        with pytest.raises(ProviderError) as captured:
            await adapter.submit_transition(
                media_request(start, end),
                start_frame=start,
                start_media_type="image/png",
                end_frame=end,
                end_media_type="image/png",
                options=PixVerseTransitionOptions(seed=1, reservation_micros=1_000_000),
            )
    assert captured.value.kind == ProviderFailureKind.UNKNOWN_OUTCOME
    assert captured.value.outcome_known is False

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, content=b"not-json"))
    ) as client:
        adapter = FalPixVerseTransitionAdapter(api_client=client)
        with pytest.raises(ProviderError) as captured:
            await adapter.submit_transition(
                media_request(start, end),
                start_frame=start,
                start_media_type="image/png",
                end_frame=end,
                end_media_type="image/png",
                options=PixVerseTransitionOptions(seed=1, reservation_micros=1_000_000),
            )
    assert captured.value.kind == ProviderFailureKind.UNKNOWN_OUTCOME
    assert captured.value.outcome_known is False


@pytest.mark.asyncio
async def test_poll_timeout_is_repeatable_and_never_submits(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []

    def handler(http_request: httpx.Request) -> httpx.Response:
        calls.append((http_request.method, http_request.url.path))
        return httpx.Response(200, json={"status": "IN_PROGRESS"})

    monkeypatch.setenv("FAL_API_KEY", "test-secret")
    ticket = FalQueueTicket(1, PIXVERSE_TRANSITION_CAPABILITY, "retained-ticket")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = FalPixVerseTransitionAdapter(api_client=client, max_status_polls=1, poll_interval_seconds=0)
        for _ in range(2):
            with pytest.raises(ProviderError) as captured:
                await adapter.poll_transition(ticket)
            assert captured.value.kind == ProviderFailureKind.TIMEOUT
            assert captured.value.request_id == "retained-ticket"
            assert captured.value.outcome_known is True
    assert calls == [
        ("GET", "/fal-ai/pixverse/v6/transition/requests/retained-ticket/status"),
        ("GET", "/fal-ai/pixverse/v6/transition/requests/retained-ticket/status"),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("video_url", "download_body", "download_type", "declared_delta", "error_match"),
    [
        ("https://private.fal.media/output.mp4", valid_mp4(), "video/mp4", 0, "non-public"),
        ("https://v3b.fal.media/output.mp4", b"not-an-mp4", "video/mp4", 0, "MP4"),
        ("https://v3b.fal.media/output.mp4", valid_mp4(), "application/octet-stream", 0, "video/mp4"),
        ("https://v3b.fal.media/output.mp4", valid_mp4(), "video/mp4", 1, "File size"),
    ],
)
async def test_result_download_fails_closed(
    monkeypatch,
    video_url: str,
    download_body: bytes,
    download_type: str,
    declared_delta: int,
    error_match: str,
) -> None:
    declared_size = len(download_body) + declared_delta

    def api_handler(http_request: httpx.Request) -> httpx.Response:
        if http_request.url.path.endswith("/status"):
            return httpx.Response(200, json={"status": "COMPLETED"})
        return httpx.Response(
            200,
            json={
                "video": {
                    "url": video_url,
                    "content_type": "video/mp4",
                    "file_name": "output.mp4",
                    "file_size": declared_size,
                }
            },
        )

    def download_handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=download_body,
            headers={"content-type": download_type, "content-length": str(len(download_body))},
        )

    monkeypatch.setenv("FAL_API_KEY", "test-secret")
    async with httpx.AsyncClient(transport=httpx.MockTransport(api_handler)) as api_client:  # noqa: SIM117
        async with httpx.AsyncClient(transport=httpx.MockTransport(download_handler)) as download_client:
            adapter = FalPixVerseTransitionAdapter(
                api_client=api_client,
                download_client=download_client,
                host_resolver=(lambda _: ["127.0.0.1"]) if "private.fal.media" in video_url else public_resolver,
                max_status_polls=1,
                poll_interval_seconds=0,
            )
            with pytest.raises(ProviderError, match=error_match):
                result = await adapter.poll_transition(
                    FalQueueTicket(1, PIXVERSE_TRANSITION_CAPABILITY, "download-validation-ticket")
                )
                validate_pixverse_video_result(result)


def test_capability_manifest_names_configuration_without_secret(monkeypatch) -> None:
    monkeypatch.setenv("FAL_API_KEY", "must-never-appear")
    adapter = FalPixVerseTransitionAdapter()
    manifest = adapter.capability_manifest()
    serialized = json.dumps(manifest)
    assert manifest["capability_id"] == PIXVERSE_TRANSITION_CAPABILITY
    assert manifest["api_key_env_candidates"] == ["FAL_API_KEY", "FAL_KEY"]
    assert "must-never-appear" not in serialized


@pytest.mark.asyncio
async def test_non_ascii_mp4_major_brand_is_a_typed_invalid_output(monkeypatch) -> None:
    malformed = b"".join(
        (
            mp4_box(b"ftyp", b"\xffsom\x00\x00\x02\x00isommp42"),
            mp4_box(b"mdat", b"payload"),
            mp4_box(b"moov"),
        )
    )

    def api_handler(http_request: httpx.Request) -> httpx.Response:
        if http_request.url.path.endswith("/status"):
            return httpx.Response(200, json={"status": "COMPLETED"})
        return httpx.Response(
            200,
            json={
                "video": {
                    "url": "https://v3b.fal.media/malformed.mp4",
                    "content_type": "video/mp4",
                    "file_name": "malformed.mp4",
                    "file_size": len(malformed),
                }
            },
        )

    def download_handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=malformed,
            headers={"content-type": "video/mp4", "content-length": str(len(malformed))},
        )

    monkeypatch.setenv("FAL_API_KEY", "test-secret")
    async with httpx.AsyncClient(transport=httpx.MockTransport(api_handler)) as api_client:  # noqa: SIM117
        async with httpx.AsyncClient(transport=httpx.MockTransport(download_handler)) as download_client:
            adapter = FalPixVerseTransitionAdapter(
                api_client=api_client,
                download_client=download_client,
                host_resolver=public_resolver,
                max_status_polls=1,
                poll_interval_seconds=0,
            )
            result = await adapter.poll_transition(FalQueueTicket(1, PIXVERSE_TRANSITION_CAPABILITY, "bad-brand"))
            with pytest.raises(ProviderError) as captured:
                validate_pixverse_video_result(result)
    assert captured.value.kind == ProviderFailureKind.INVALID_OUTPUT
    assert "non-ASCII major brand" in str(captured.value)


@pytest.mark.asyncio
async def test_result_rejects_non_fal_download_domain_before_download(monkeypatch) -> None:
    video = valid_mp4()
    download_calls = 0

    def api_handler(http_request: httpx.Request) -> httpx.Response:
        if http_request.url.path.endswith("/status"):
            return httpx.Response(200, json={"status": "COMPLETED"})
        return httpx.Response(
            200,
            json={
                "video": {
                    "url": "https://media.example.test/output.mp4",
                    "content_type": "video/mp4",
                    "file_name": "output.mp4",
                    "file_size": len(video),
                }
            },
        )

    def must_not_download(_: httpx.Request) -> httpx.Response:
        nonlocal download_calls
        download_calls += 1
        return httpx.Response(200, content=video)

    monkeypatch.setenv("FAL_API_KEY", "test-secret")
    async with httpx.AsyncClient(transport=httpx.MockTransport(api_handler)) as api_client:  # noqa: SIM117
        async with httpx.AsyncClient(transport=httpx.MockTransport(must_not_download)) as download_client:
            adapter = FalPixVerseTransitionAdapter(
                api_client=api_client,
                download_client=download_client,
                host_resolver=public_resolver,
                max_status_polls=1,
                poll_interval_seconds=0,
            )
            with pytest.raises(ProviderError, match="approved Fal CDN domain"):
                await adapter.poll_transition(FalQueueTicket(1, PIXVERSE_TRANSITION_CAPABILITY, "outside-domain"))
    assert download_calls == 0


@pytest.mark.asyncio
async def test_repeatable_read_transport_failure_never_requires_reconciliation(monkeypatch) -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        raise httpx.ReadError("injected read failure")

    monkeypatch.setenv("FAL_API_KEY", "test-secret")
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        adapter = FalPixVerseTransitionAdapter(api_client=client, max_status_polls=1, poll_interval_seconds=0)
        with pytest.raises(ProviderError) as captured:
            await adapter.poll_transition(FalQueueTicket(1, PIXVERSE_TRANSITION_CAPABILITY, "repeatable-read"))
    assert captured.value.kind == ProviderFailureKind.UNAVAILABLE
    assert captured.value.outcome_known is True
    assert captured.value.request_id == "repeatable-read"


@pytest.mark.asyncio
async def test_unknown_queue_status_is_typed_invalid_output_not_unknown_outcome(monkeypatch) -> None:
    monkeypatch.setenv("FAL_API_KEY", "test-secret")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json={"status": "SURPRISE"}))
    ) as client:
        adapter = FalPixVerseTransitionAdapter(api_client=client, max_status_polls=1, poll_interval_seconds=0)
        with pytest.raises(ProviderError) as captured:
            await adapter.poll_transition(FalQueueTicket(1, PIXVERSE_TRANSITION_CAPABILITY, "unknown-status"))
    assert captured.value.kind == ProviderFailureKind.INVALID_OUTPUT
    assert captured.value.outcome_known is True


@pytest.mark.asyncio
async def test_submit_and_poll_have_absolute_deadlines(monkeypatch) -> None:
    start = png_bytes((1, 2, 3, 255))
    end = png_bytes((3, 2, 1, 255))

    async def slow_submit(_: httpx.Request) -> httpx.Response:
        await asyncio.sleep(0.05)
        return httpx.Response(200, json={"request_id": "too-late"})

    monkeypatch.setenv("FAL_API_KEY", "test-secret")
    async with httpx.AsyncClient(transport=httpx.MockTransport(slow_submit)) as client:
        adapter = FalPixVerseTransitionAdapter(api_client=client, timeout_seconds=0.001)
        with pytest.raises(ProviderError) as captured:
            await adapter.submit_transition(
                media_request(start, end),
                start_frame=start,
                start_media_type="image/png",
                end_frame=end,
                end_media_type="image/png",
                options=PixVerseTransitionOptions(seed=1, reservation_micros=1_000_000),
            )
    assert captured.value.kind == ProviderFailureKind.TIMEOUT
    assert captured.value.outcome_known is False

    async def always_pending(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "IN_PROGRESS"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(always_pending)) as client:
        adapter = FalPixVerseTransitionAdapter(
            api_client=client,
            timeout_seconds=1,
            poll_deadline_seconds=0.001,
            max_status_polls=100,
            poll_interval_seconds=0.05,
        )
        with pytest.raises(ProviderError) as captured:
            await adapter.poll_transition(FalQueueTicket(1, PIXVERSE_TRANSITION_CAPABILITY, "poll-deadline"))
    assert captured.value.kind == ProviderFailureKind.TIMEOUT
    assert captured.value.outcome_known is True
    assert captured.value.request_id == "poll-deadline"


def test_pricing_reservation_covers_every_allowed_option() -> None:
    with pytest.raises(ValueError, match="1350000"):
        PixVerseTransitionOptions(
            seed=1,
            reservation_micros=1_000_000,
            duration_seconds=15,
            resolution="1080p",
        )
    accepted = PixVerseTransitionOptions(
        seed=1,
        reservation_micros=1_350_000,
        duration_seconds=15,
        resolution="1080p",
    )
    assert accepted.reservation_micros == 1_350_000


def test_mp4_box_scan_and_diagnostics_are_strictly_bounded() -> None:
    payload = b"".join(
        (
            mp4_box(b"ftyp", b"isom\x00\x00\x02\x00isommp42"),
            *(mp4_box(b"free") for _ in range(4_096)),
            mp4_box(b"mdat", b"payload"),
            mp4_box(b"moov"),
        )
    )
    with pytest.raises(ProviderError, match="too many top-level boxes"):
        validate_pixverse_video_result(retained_video_result(payload))


@pytest.mark.asyncio
async def test_paid_malformed_video_is_persisted_before_terminal_quarantine(
    monkeypatch,
    database,
    objects,
    make_attempt,
) -> None:
    malformed = b"paid-but-malformed-mp4"

    def api_handler(http_request: httpx.Request) -> httpx.Response:
        if http_request.url.path.endswith("/status"):
            return httpx.Response(200, json={"status": "COMPLETED"})
        return httpx.Response(
            200,
            json={
                "video": {
                    "url": "https://v3b.fal.media/malformed.mp4",
                    "content_type": "video/mp4",
                    "file_name": "malformed.mp4",
                    "file_size": len(malformed),
                }
            },
        )

    def download_handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=malformed,
            headers={"content-type": "video/mp4", "content-length": str(len(malformed))},
        )

    monkeypatch.setenv("FAL_API_KEY", "test-secret")
    attempt = make_attempt(stage=Stage.ASSETS)
    journal = OperationJournal(database)
    persistence = ResultPersistence(objects)
    ticket = FalQueueTicket(1, PIXVERSE_TRANSITION_CAPABILITY, "paid-malformed")
    async with httpx.AsyncClient(transport=httpx.MockTransport(api_handler)) as api_client:  # noqa: SIM117
        async with httpx.AsyncClient(transport=httpx.MockTransport(download_handler)) as download_client:
            adapter = FalPixVerseTransitionAdapter(
                api_client=api_client,
                download_client=download_client,
                host_resolver=public_resolver,
                max_status_polls=1,
                poll_interval_seconds=0,
            )
            with pytest.raises(ProviderError, match="MP4"):
                await journal.run_provider(
                    attempt_id=attempt["id"],
                    stage=Stage.ASSETS,
                    idempotency_key="fal-result:paid-malformed",
                    side_effect="retrieve_pixverse_transition",
                    provider_role="fal_pixverse_transition",
                    request=adapter.poll_journal_request(ticket),
                    reserve_micros=0,
                    invoke=lambda: adapter.poll_transition(ticket),
                    persist_result=persistence,
                    validate_result=validate_pixverse_video_result,
                )

    with database.connect() as connection:
        operation = dict(
            connection.execute("SELECT * FROM operation WHERE idempotency_key='fal-result:paid-malformed'").fetchone()
        )
    assert operation["status"] == OperationStatus.FAILED_TERMINAL
    assert objects.get(operation["result_hash"]) == malformed
    assert json.loads(operation["failure_json"])["quarantined_result_hash"] == operation["result_hash"]
