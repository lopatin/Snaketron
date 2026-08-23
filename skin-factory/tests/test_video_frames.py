from __future__ import annotations

import hashlib
import io
import json
import shutil
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest
from PIL import Image, ImageDraw

import snaketron_factory.video_frames as video_frames
from snaketron_factory.video_frames import (
    MatteEndpointRequest,
    VideoFrameExtractionConfig,
    VideoFrameExtractionError,
    VideoFrameExtractionRequest,
    extract_rgba_frame_sheet,
    media_tool_environment,
    validate_matte_endpoint,
    video_toolchain_identity,
)

MATTE = (255, 0, 255)


def content_ref(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def png_bytes(image: Image.Image) -> bytes:
    output = io.BytesIO()
    image.save(output, format="PNG", optimize=False, compress_level=9)
    return output.getvalue()


def retained_frame(*, objects: list[tuple[int, int, int, int]], ambiguous_pixel: bool = False) -> Image.Image:
    image = Image.new("RGB", (96, 64), MATTE)
    draw = ImageDraw.Draw(image)
    for bounds in objects:
        draw.rectangle(bounds, fill=(20, 180, 40))
    if ambiguous_pixel:
        image.putpixel((48, 31), (245, 0, 255))
    return image


def extraction_request(video: bytes, start: bytes, end: bytes) -> VideoFrameExtractionRequest:
    return VideoFrameExtractionRequest(
        source_video_sha256=content_ref(video),
        start_frame_sha256=content_ref(start),
        end_frame_sha256=content_ref(end),
        body_columns=4,
        texels_per_cell=16,
        raster_overhang_px=4,
        frame_rows=4,
        desired_fps=4,
        common_period_ms=1_000,
        matte_rgb=MATTE,
        source_apron_px=8,
        output_apron_px=2,
    )


def config(ffmpeg: str = "/bin/false", ffprobe: str = "/bin/false") -> VideoFrameExtractionConfig:
    return VideoFrameExtractionConfig(ffmpeg_path=ffmpeg, ffprobe_path=ffprobe)


def test_media_tool_environment_scrubs_parent_credentials(monkeypatch, tmp_path: Path) -> None:
    secret_variables = {
        "FAL_API_KEY": "fal-secret-value",
        "GEMINI_API_KEY": "gemini-secret-value",
        "SKIN_FACTORY_REVIEW_TOKEN": "review-secret-value",
        "SNAKETRON_FACTORY_SERVICE_TOKEN": "service-secret-value",
        "UNRELATED_SECRET": "unrelated-secret-value",
    }
    for name, value in secret_variables.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("TMPDIR", str(tmp_path))
    tool = tmp_path / "capture-environment"
    tool.write_text(
        f"#!{sys.executable}\n"
        "import json, os\n"
        "print('fake media version ' + json.dumps(dict(os.environ), sort_keys=True))\n"
    )
    tool.chmod(0o755)

    allowed = media_tool_environment()
    identity = video_toolchain_identity(config(str(tool), str(tool)))
    captured = json.loads(identity["ffmpeg"]["version"].removeprefix("fake media version "))

    assert captured == allowed
    assert captured["LC_ALL"] == "C"
    assert captured["LANG"] == "C"
    assert captured["TZ"] == "UTC"
    assert captured["TMPDIR"] == str(tmp_path)
    for name, value in secret_variables.items():
        assert name not in captured
        assert value not in captured.values()


def make_lossless_loop(tmp_path: Path, ffmpeg: str) -> tuple[bytes, bytes, bytes]:
    frames: list[bytes] = []
    for index, inset in enumerate((0, 3, 6, 3, 1, 0)):
        image = retained_frame(objects=[(16, 26, 79, 37)])
        if inset:
            draw = ImageDraw.Draw(image)
            draw.rectangle((16 + inset, 28, 79 - inset, 35), fill=(30, 90 + inset * 10, 220))
        value = png_bytes(image)
        frames.append(value)
        (tmp_path / f"source-{index:03d}.png").write_bytes(value)
    output_path = tmp_path / "loop.mp4"
    completed = subprocess.run(
        [
            ffmpeg,
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-framerate",
            "6",
            "-start_number",
            "0",
            "-i",
            str(tmp_path / "source-%03d.png"),
            "-frames:v",
            "6",
            "-an",
            "-c:v",
            "libx264rgb",
            "-crf",
            "0",
            "-preset",
            "ultrafast",
            "-pix_fmt",
            "rgb24",
            "-movflags",
            "+faststart",
            "-y",
            str(output_path),
        ],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=False,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr.decode("utf-8", "replace")
    return output_path.read_bytes(), frames[0], frames[-1]


def make_lossy_loop(tmp_path: Path, ffmpeg: str) -> tuple[bytes, bytes, bytes]:
    image = Image.new("RGB", (96, 64), MATTE)
    ImageDraw.Draw(image).rounded_rectangle((16, 23, 79, 40), radius=8, fill=(20, 180, 40))
    frame = png_bytes(image)
    for index in range(6):
        (tmp_path / f"lossy-{index:03d}.png").write_bytes(frame)
    output_path = tmp_path / "lossy.mp4"
    completed = subprocess.run(
        [
            ffmpeg,
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-framerate",
            "6",
            "-start_number",
            "0",
            "-i",
            str(tmp_path / "lossy-%03d.png"),
            "-frames:v",
            "6",
            "-an",
            "-c:v",
            "libx264",
            "-crf",
            "23",
            "-preset",
            "medium",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            "-y",
            str(output_path),
        ],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=False,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr.decode("utf-8", "replace")
    return output_path.read_bytes(), frame, frame


def make_lossless_scene_video(tmp_path: Path, ffmpeg: str) -> bytes:
    scene = retained_frame(objects=[(8, 8, 87, 55)])
    frame = png_bytes(scene)
    for index in range(6):
        (tmp_path / f"scene-{index:03d}.png").write_bytes(frame)
    output_path = tmp_path / "scene.mp4"
    completed = subprocess.run(
        [
            ffmpeg,
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-framerate",
            "6",
            "-start_number",
            "0",
            "-i",
            str(tmp_path / "scene-%03d.png"),
            "-frames:v",
            "6",
            "-an",
            "-c:v",
            "libx264rgb",
            "-crf",
            "0",
            "-preset",
            "ultrafast",
            "-pix_fmt",
            "rgb24",
            "-y",
            str(output_path),
        ],
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=False,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr.decode("utf-8", "replace")
    return output_path.read_bytes()


def test_request_derives_exact_rgba_overhang_geometry() -> None:
    request = extraction_request(b"video", b"start", b"end")

    assert request.output_width_px == 64
    assert request.output_row_height_px == 24

    with pytest.raises(ValueError, match="frame_rows must equal"):
        VideoFrameExtractionRequest(
            source_video_sha256=content_ref(b"video"),
            start_frame_sha256=content_ref(b"start"),
            end_frame_sha256=content_ref(b"end"),
            body_columns=4,
            texels_per_cell=16,
            raster_overhang_px=4,
            frame_rows=5,
            desired_fps=4,
            common_period_ms=1_000,
            matte_rgb=MATTE,
            source_apron_px=8,
            output_apron_px=2,
        )


def test_matte_removal_reconstructs_bounded_uncertainty_and_rejects_unbounded_cleanup() -> None:
    request = extraction_request(b"video", b"start", b"end")

    ambiguous = retained_frame(objects=[(16, 26, 79, 37)], ambiguous_pixel=True)
    reconstructed = video_frames._remove_matte(ambiguous, request, config(), "ambiguous")
    assert reconstructed.measurement["uncertain_pixels"] == 1
    assert reconstructed.measurement["foreground_uncertain_pixels"] == 1
    assert reconstructed.measurement["background_cleanup_pixels"] == 0
    assert 0 < reconstructed.rgba[31, 48, 3] < 255
    assert tuple(reconstructed.rgba[31, 48, :3]) != MATTE

    excessive = retained_frame(objects=[(16, 26, 79, 37)])
    ImageDraw.Draw(excessive).rectangle((40, 6, 54, 20), fill=(245, 0, 255))
    with pytest.raises(VideoFrameExtractionError, match="beyond the declared reconstruction radius") as caught:
        video_frames._remove_matte(excessive, request, config(), "excessive")
    assert caught.value.code == "ambiguous_matte"


def test_matte_removal_rejects_disconnected_objects_and_edges() -> None:
    request = extraction_request(b"video", b"start", b"end")

    disconnected = retained_frame(objects=[(16, 26, 35, 37), (60, 26, 79, 37)])
    with pytest.raises(VideoFrameExtractionError, match="disconnected objects") as caught:
        video_frames._remove_matte(disconnected, request, config(), "disconnected")
    assert caught.value.code == "multiple_objects"

    touching = retained_frame(objects=[(0, 26, 79, 37)])
    with pytest.raises(VideoFrameExtractionError, match="source apron") as caught:
        video_frames._remove_matte(touching, request, config(), "touching")
    assert caught.value.code == "edge_contact"


def test_public_endpoint_preflight_retains_source_and_native_rgba() -> None:
    image = Image.new("RGB", (160, 96), MATTE)
    ImageDraw.Draw(image).rectangle((40, 40, 119, 55), fill=(20, 180, 40))
    frame = png_bytes(image)
    request = MatteEndpointRequest(
        frame_sha256=content_ref(frame),
        body_columns=8,
        texels_per_cell=16,
        raster_overhang_px=4,
        matte_rgb=MATTE,
        source_apron_px=32,
        output_apron_px=2,
    )

    result = validate_matte_endpoint(frame, request=request, config=config(), label="start endpoint")

    with Image.open(io.BytesIO(result.source_rgba_png)) as source:
        assert source.mode == "RGBA"
        assert source.size == (160, 96)
        source_pixels = np.asarray(source)
        assert np.all(source_pixels[0, 0] == 0)
        assert np.any(source_pixels[..., 3] == 255)
    with Image.open(io.BytesIO(result.native_rgba_png)) as native:
        assert native.mode == "RGBA"
        assert native.size == (128, 24)
        assert np.all(np.asarray(native)[0, 0] == 0)
    assert result.report["source"]["input_png_sha256"] == content_ref(frame)
    assert result.report["source"]["source_rgba_png_sha256"] == content_ref(result.source_rgba_png)
    assert result.report["native"]["native_rgba_png_sha256"] == content_ref(result.native_rgba_png)
    assert min(result.report["source"]["matte_measurement"]["source_margins_px"]) >= 32
    assert min(result.report["native"]["measurement"]["margins_px"]) >= 2


def test_endpoint_preflight_rejects_scene_sized_and_native_infeasible_foreground() -> None:
    request_kwargs = {
        "body_columns": 16,
        "texels_per_cell": 16,
        "raster_overhang_px": 4,
        "matte_rgb": MATTE,
        "source_apron_px": 32,
        "output_apron_px": 1,
    }
    scene = Image.new("RGB", (1080, 720), MATTE)
    ImageDraw.Draw(scene).rectangle((32, 32, 1047, 687), fill=(20, 180, 40))
    scene_png = png_bytes(scene)
    with pytest.raises(VideoFrameExtractionError, match="foreground bbox occupies") as caught:
        validate_matte_endpoint(
            scene_png,
            request=MatteEndpointRequest(frame_sha256=content_ref(scene_png), **request_kwargs),
            config=config(),
            label="scene endpoint",
        )
    assert caught.value.code == "source_geometry"

    squat = Image.new("RGB", (1080, 720), MATTE)
    ImageDraw.Draw(squat).rectangle((32, 250, 1047, 469), fill=(20, 180, 40))
    squat_png = png_bytes(squat)
    with pytest.raises(VideoFrameExtractionError, match="native scaling") as caught:
        validate_matte_endpoint(
            squat_png,
            request=MatteEndpointRequest(frame_sha256=content_ref(squat_png), **request_kwargs),
            config=config(),
            label="squat endpoint",
        )
    assert caught.value.code == "source_geometry"


def test_probe_rejects_alpha_capable_source_video_format() -> None:
    probe = json.dumps(
        {
            "streams": [
                {
                    "width": 96,
                    "height": 64,
                    "duration": "1.0",
                    "avg_frame_rate": "6/1",
                    "pix_fmt": "yuva420p",
                    "codec_name": "vp9",
                }
            ],
            "format": {"duration": "1.0"},
        }
    ).encode()

    with pytest.raises(VideoFrameExtractionError, match="opaque-format allowlist") as caught:
        video_frames._parse_probe(probe, config())
    assert caught.value.code == "ambiguous_alpha"


def test_real_ffmpeg_extracts_deterministic_rgba_sheet_with_report(tmp_path: Path) -> None:
    ffmpeg = shutil.which("ffmpeg")
    ffprobe = shutil.which("ffprobe")
    if ffmpeg is None or ffprobe is None:
        pytest.skip("real ffmpeg/ffprobe binaries are not installed")
    video, start, end = make_lossless_loop(tmp_path, ffmpeg)
    request = extraction_request(video, start, end)
    extraction_config = config(ffmpeg, ffprobe)

    first = extract_rgba_frame_sheet(
        video,
        start_frame_png=start,
        end_frame_png=end,
        request=request,
        config=extraction_config,
    )
    second = extract_rgba_frame_sheet(
        video,
        start_frame_png=start,
        end_frame_png=end,
        request=request,
        config=extraction_config,
    )

    assert first.sheet_png == second.sheet_png
    assert first.frame_pngs == second.frame_pngs
    assert first.report == second.report
    assert len(first.frame_pngs) == 4
    with Image.open(io.BytesIO(first.sheet_png)) as sheet:
        assert sheet.mode == "RGBA"
        assert sheet.size == (64, 96)
        pixels = np.asarray(sheet)
        assert np.all(pixels[0, 0] == 0)
        assert np.any(pixels[..., 3] > 0)
        assert np.all(pixels[pixels[..., 3] == 0, :3] == 0)

    assert first.report["request"]["matte_rgb"] == [255, 0, 255]
    assert first.report["output"]["width_px"] == 64
    assert first.report["output"]["row_height_px"] == 24
    assert first.report["output"]["frame_rows"] == 4
    assert first.report["output"]["sheet_png_sha256"] == content_ref(first.sheet_png)
    assert first.report["report_sha256"].startswith("sha256:")
    assert first.report["tools"]["ffmpeg"]["version"].startswith("ffmpeg version")
    assert first.report["tools"]["ffprobe"]["version"].startswith("ffprobe version")
    assert first.report["tools"]["ffmpeg"]["binary_sha256"].startswith("sha256:")
    assert len(first.report["decoded_frame_sha256"]) == 4
    assert first.report["final_to_row_zero"]["mean_absolute_channel_delta"] == 0
    assert all("snaketron-video-frames-" not in " ".join(command) for command in first.report["commands"])
    assert first.report["tools"] == video_toolchain_identity(extraction_config)


def test_lossy_yuv420p_matte_uncertainty_is_bounded_and_reconstructed(tmp_path: Path) -> None:
    ffmpeg = shutil.which("ffmpeg")
    ffprobe = shutil.which("ffprobe")
    if ffmpeg is None or ffprobe is None:
        pytest.skip("real ffmpeg/ffprobe binaries are not installed")
    video, start, end = make_lossy_loop(tmp_path, ffmpeg)

    result = extract_rgba_frame_sheet(
        video,
        start_frame_png=start,
        end_frame_png=end,
        request=extraction_request(video, start, end),
        config=config(ffmpeg, ffprobe),
    )

    measurements = result.report["matte"]["frames"]
    assert result.report["probe"]["pixel_format"] == "yuv420p"
    assert all(measurement["uncertain_pixels"] > 0 for measurement in measurements)
    assert all(measurement["foreground_uncertain_pixels"] > 0 for measurement in measurements)
    assert all(measurement["background_cleanup_pixels"] > 0 for measurement in measurements)
    assert all(measurement["unclassified_uncertain_pixels"] == 0 for measurement in measurements)
    with Image.open(io.BytesIO(result.sheet_png)) as sheet:
        pixels = np.asarray(sheet)
        assert sheet.mode == "RGBA"
        assert np.any((pixels[..., 3] > 0) & (pixels[..., 3] < 255))
        assert np.all(pixels[pixels[..., 3] == 0, :3] == 0)


def test_video_scene_replacement_is_rejected_before_sheet_render(tmp_path: Path) -> None:
    ffmpeg = shutil.which("ffmpeg")
    ffprobe = shutil.which("ffprobe")
    if ffmpeg is None or ffprobe is None:
        pytest.skip("real ffmpeg/ffprobe binaries are not installed")
    endpoint = png_bytes(retained_frame(objects=[(16, 26, 79, 37)]))
    video = make_lossless_scene_video(tmp_path, ffmpeg)

    with pytest.raises(VideoFrameExtractionError, match="foreground bbox occupies") as caught:
        extract_rgba_frame_sheet(
            video,
            start_frame_png=endpoint,
            end_frame_png=endpoint,
            request=extraction_request(video, endpoint, endpoint),
            config=config(ffmpeg, ffprobe),
        )
    assert caught.value.code == "source_geometry"
