"""Deterministic, fail-closed video-to-RGBA sprite-sheet extraction."""

from __future__ import annotations

import hashlib
import io
import json
import math
import os
import shutil
import signal
import subprocess
import tempfile
import time
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import asdict, dataclass
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from fractions import Fraction
from pathlib import Path
from typing import Any

import numpy as np
from PIL import Image, UnidentifiedImageError

_CONTENT_HASH_PREFIX = "sha256:"
_MAX_FRAME_ROWS = 120
_MAX_DECODED_SHEET_BYTES = 16_777_216
_MEDIA_TOOL_ENV_ALLOWLIST = (
    "DYLD_FALLBACK_FRAMEWORK_PATH",
    "DYLD_FALLBACK_LIBRARY_PATH",
    "DYLD_FRAMEWORK_PATH",
    "DYLD_LIBRARY_PATH",
    "LD_LIBRARY_PATH",
    "PATH",
    "PATHEXT",
    "SystemRoot",
    "TEMP",
    "TMP",
    "TMPDIR",
    "WINDIR",
)
_OPAQUE_PIXEL_FORMATS = frozenset(
    {
        "bgr24",
        "gbrp",
        "gbrp10le",
        "gbrp12le",
        "gray",
        "gray10le",
        "gray12le",
        "nv12",
        "nv21",
        "p010le",
        "p012le",
        "rgb24",
        "yuv420p",
        "yuv420p10le",
        "yuv420p12le",
        "yuv422p",
        "yuv422p10le",
        "yuv422p12le",
        "yuv444p",
        "yuv444p10le",
        "yuv444p12le",
        "yuvj420p",
        "yuvj422p",
        "yuvj444p",
    }
)


class VideoFrameExtractionError(RuntimeError):
    """A typed local media failure that must stop draft automation."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class VideoFrameExtractionRequest:
    source_video_sha256: str
    start_frame_sha256: str
    end_frame_sha256: str
    body_columns: int
    texels_per_cell: int
    raster_overhang_px: int
    frame_rows: int
    desired_fps: float
    common_period_ms: float
    matte_rgb: tuple[int, int, int]
    source_apron_px: int
    output_apron_px: int

    def __post_init__(self) -> None:
        for label, value in (
            ("source_video_sha256", self.source_video_sha256),
            ("start_frame_sha256", self.start_frame_sha256),
            ("end_frame_sha256", self.end_frame_sha256),
        ):
            if not _valid_content_hash(value):
                raise ValueError(f"{label} must be a sha256 content reference")
        if isinstance(self.body_columns, bool) or not 1 <= self.body_columns <= 128:
            raise ValueError("body_columns must be from 1 through 128")
        if isinstance(self.texels_per_cell, bool) or not 4 <= self.texels_per_cell <= 128:
            raise ValueError("texels_per_cell must be from 4 through 128")
        if isinstance(self.raster_overhang_px, bool) or not 0 <= self.raster_overhang_px <= 4:
            raise ValueError("raster_overhang_px must be from 0 through 4")
        if (self.texels_per_cell * self.raster_overhang_px) % 16:
            raise ValueError("raster overhang must scale exactly at the requested texel density")
        if isinstance(self.frame_rows, bool) or not 2 <= self.frame_rows <= _MAX_FRAME_ROWS:
            raise ValueError("frame_rows must be from 2 through 120")
        if isinstance(self.desired_fps, bool) or not 1 <= self.desired_fps <= 60:
            raise ValueError("desired_fps must be from 1 through 60")
        if isinstance(self.common_period_ms, bool) or not 120 <= self.common_period_ms <= 60_000:
            raise ValueError("common_period_ms must be from 120 through 60000")
        derived_rows = max(2, math.ceil(self.common_period_ms * self.desired_fps / 1_000))
        if self.frame_rows != derived_rows:
            raise ValueError("frame_rows must equal ceil(common_period_ms * desired_fps / 1000)")
        if len(self.matte_rgb) != 3 or any(
            isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 255 for value in self.matte_rgb
        ):
            raise ValueError("matte_rgb must contain three byte values")
        if isinstance(self.source_apron_px, bool) or not 1 <= self.source_apron_px <= 512:
            raise ValueError("source_apron_px must be from 1 through 512")
        if isinstance(self.output_apron_px, bool) or not 1 <= self.output_apron_px <= 64:
            raise ValueError("output_apron_px must be from 1 through 64")
        if self.output_width_px <= 2 * self.output_apron_px or self.output_row_height_px <= 2 * self.output_apron_px:
            raise ValueError("output apron leaves no drawable frame area")
        if self.output_width_px * self.output_row_height_px * self.frame_rows * 4 > _MAX_DECODED_SHEET_BYTES:
            raise ValueError("decoded RGBA sheet exceeds the pinned byte ceiling")

    @property
    def output_width_px(self) -> int:
        return self.body_columns * self.texels_per_cell

    @property
    def output_row_height_px(self) -> int:
        scaled_overhang = self.texels_per_cell * self.raster_overhang_px // 16
        return self.texels_per_cell + 2 * scaled_overhang


@dataclass(frozen=True, slots=True)
class MatteEndpointRequest:
    """Exact retained identity and native geometry for one endpoint arena."""

    frame_sha256: str
    body_columns: int
    texels_per_cell: int
    raster_overhang_px: int
    matte_rgb: tuple[int, int, int]
    source_apron_px: int
    output_apron_px: int

    def __post_init__(self) -> None:
        if not _valid_content_hash(self.frame_sha256):
            raise ValueError("frame_sha256 must be a sha256 content reference")
        if isinstance(self.body_columns, bool) or not 1 <= self.body_columns <= 128:
            raise ValueError("body_columns must be from 1 through 128")
        if isinstance(self.texels_per_cell, bool) or not 4 <= self.texels_per_cell <= 128:
            raise ValueError("texels_per_cell must be from 4 through 128")
        if isinstance(self.raster_overhang_px, bool) or not 0 <= self.raster_overhang_px <= 4:
            raise ValueError("raster_overhang_px must be from 0 through 4")
        if (self.texels_per_cell * self.raster_overhang_px) % 16:
            raise ValueError("raster overhang must scale exactly at the requested texel density")
        if len(self.matte_rgb) != 3 or any(
            isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 255 for value in self.matte_rgb
        ):
            raise ValueError("matte_rgb must contain three byte values")
        if isinstance(self.source_apron_px, bool) or not 1 <= self.source_apron_px <= 512:
            raise ValueError("source_apron_px must be from 1 through 512")
        if isinstance(self.output_apron_px, bool) or not 1 <= self.output_apron_px <= 64:
            raise ValueError("output_apron_px must be from 1 through 64")
        if self.output_width_px <= 2 * self.output_apron_px or self.output_row_height_px <= 2 * self.output_apron_px:
            raise ValueError("output apron leaves no drawable frame area")
        if self.output_width_px * self.output_row_height_px * 4 > _MAX_DECODED_SHEET_BYTES:
            raise ValueError("decoded native RGBA endpoint exceeds the pinned byte ceiling")

    @property
    def output_width_px(self) -> int:
        return self.body_columns * self.texels_per_cell

    @property
    def output_row_height_px(self) -> int:
        scaled_overhang = self.texels_per_cell * self.raster_overhang_px // 16
        return self.texels_per_cell + 2 * scaled_overhang


@dataclass(frozen=True, slots=True)
class VideoFrameExtractionConfig:
    ffmpeg_path: str
    ffprobe_path: str
    matte_tolerance: int = 4
    object_min_distance: int = 20
    uncertainty_radius_px: int = 3
    max_uncertain_pixels: int = 262_144
    max_uncertain_fraction: float = 0.35
    max_background_cleanup_pixels: int = 65_536
    max_background_cleanup_fraction: float = 0.10
    min_object_pixels: int = 8
    minimum_output_alpha: int = 8
    max_source_bbox_area_fraction: float = 0.50
    max_source_aspect_scale: float = 2.0
    max_source_upscale: float = 2.0
    changed_channel_delta: int = 8
    max_endpoint_mean_abs_delta: float = 24.0
    max_endpoint_changed_fraction: float = 0.55
    max_loop_mean_abs_delta: float = 12.0
    max_loop_changed_fraction: float = 0.35
    max_centroid_step_px: float = 12.0
    subprocess_timeout_seconds: float = 30.0
    total_timeout_seconds: float = 300.0
    max_video_bytes: int = 100_000_000
    max_probe_output_bytes: int = 1_000_000
    max_stderr_bytes: int = 262_144
    max_frame_png_bytes: int = 32_000_000
    max_source_dimension_px: int = 4_096

    def __post_init__(self) -> None:
        if not self.ffmpeg_path or not self.ffprobe_path:
            raise ValueError("explicit ffmpeg_path and ffprobe_path are required")
        if not 0 <= self.matte_tolerance < self.object_min_distance <= 255:
            raise ValueError("matte_tolerance must be below the declared object_min_distance")
        if isinstance(self.uncertainty_radius_px, bool) or not 1 <= self.uncertainty_radius_px <= 8:
            raise ValueError("uncertainty_radius_px must be from 1 through 8")
        if isinstance(self.max_uncertain_pixels, bool) or not 1 <= self.max_uncertain_pixels <= 10_000_000:
            raise ValueError("max_uncertain_pixels is invalid")
        if (
            isinstance(self.max_background_cleanup_pixels, bool)
            or not 0 <= self.max_background_cleanup_pixels <= self.max_uncertain_pixels
        ):
            raise ValueError("max_background_cleanup_pixels is invalid")
        if not 0 <= self.max_uncertain_fraction <= 1:
            raise ValueError("max_uncertain_fraction must be from 0 through 1")
        if not 0 <= self.max_background_cleanup_fraction <= self.max_uncertain_fraction:
            raise ValueError("max_background_cleanup_fraction must not exceed max_uncertain_fraction")
        if isinstance(self.min_object_pixels, bool) or not 1 <= self.min_object_pixels <= 10_000_000:
            raise ValueError("min_object_pixels is invalid")
        if isinstance(self.minimum_output_alpha, bool) or not 1 <= self.minimum_output_alpha <= 64:
            raise ValueError("minimum_output_alpha must be from 1 through 64")
        if not 0 < self.max_source_bbox_area_fraction <= 0.75:
            raise ValueError("max_source_bbox_area_fraction must be greater than 0 through 0.75")
        if not 1 <= self.max_source_aspect_scale <= 8:
            raise ValueError("max_source_aspect_scale must be from 1 through 8")
        if not 1 <= self.max_source_upscale <= 2:
            raise ValueError("max_source_upscale must be from 1 through 2")
        if not 0 <= self.changed_channel_delta <= 255:
            raise ValueError("changed_channel_delta must be a byte value")
        for label, value in (
            ("max_endpoint_mean_abs_delta", self.max_endpoint_mean_abs_delta),
            ("max_loop_mean_abs_delta", self.max_loop_mean_abs_delta),
            ("max_centroid_step_px", self.max_centroid_step_px),
        ):
            if value < 0:
                raise ValueError(f"{label} must be nonnegative")
        for label, value in (
            ("max_endpoint_changed_fraction", self.max_endpoint_changed_fraction),
            ("max_loop_changed_fraction", self.max_loop_changed_fraction),
        ):
            if not 0 <= value <= 1:
                raise ValueError(f"{label} must be from 0 through 1")
        if self.subprocess_timeout_seconds <= 0 or self.total_timeout_seconds <= 0:
            raise ValueError("subprocess timeouts must be positive")
        if not 1 <= self.max_video_bytes <= 500_000_000:
            raise ValueError("max_video_bytes is invalid")
        if min(self.max_probe_output_bytes, self.max_stderr_bytes, self.max_frame_png_bytes) <= 0:
            raise ValueError("subprocess output bounds must be positive")
        if not 16 <= self.max_source_dimension_px <= 8_192:
            raise ValueError("max_source_dimension_px is invalid")


@dataclass(frozen=True, slots=True)
class VideoFrameSheetResult:
    sheet_png: bytes
    frame_pngs: tuple[bytes, ...]
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class MatteEndpointResult:
    source_rgba_png: bytes
    native_rgba_png: bytes
    report: dict[str, Any]


@dataclass(frozen=True, slots=True)
class _Probe:
    width_px: int
    height_px: int
    duration_us: int
    average_fps: Fraction
    pixel_format: str
    codec_name: str


@dataclass(frozen=True, slots=True)
class _MatteFrame:
    rgba: np.ndarray[Any, np.dtype[np.uint8]]
    mask: np.ndarray[Any, np.dtype[np.bool_]]
    measurement: dict[str, Any]


def media_tool_environment() -> dict[str, str]:
    """Return the minimal non-secret environment allowed for local media tools."""

    environment = {
        name: value for name in _MEDIA_TOOL_ENV_ALLOWLIST if (value := os.environ.get(name)) and "\x00" not in value
    }
    environment.setdefault("PATH", os.defpath)
    environment.update({"LC_ALL": "C", "LANG": "C", "TZ": "UTC"})
    return environment


def video_toolchain_identity(config: VideoFrameExtractionConfig) -> dict[str, Any]:
    """Resolve and hash the exact local decoder toolchain under one deadline."""

    deadline = time.monotonic() + config.total_timeout_seconds
    return _video_toolchain_identity(config, deadline)


def _video_toolchain_identity(config: VideoFrameExtractionConfig, deadline: float) -> dict[str, Any]:
    ffmpeg = _resolve_tool(config.ffmpeg_path, "ffmpeg")
    ffprobe = _resolve_tool(config.ffprobe_path, "ffprobe")
    return {
        "ffmpeg": _tool_identity(ffmpeg, config, deadline),
        "ffprobe": _tool_identity(ffprobe, config, deadline),
    }


def validate_matte_endpoint(
    frame_png: bytes,
    *,
    request: MatteEndpointRequest,
    config: VideoFrameExtractionConfig,
    label: str,
) -> MatteEndpointResult:
    """Normalize one retained opaque-matte arena and prove native feasibility."""

    if not isinstance(label, str) or not label or len(label.encode("utf-8")) > 128:
        raise ValueError("endpoint label must contain 1 through 128 UTF-8 bytes")
    if not isinstance(frame_png, bytes) or not frame_png or len(frame_png) > config.max_frame_png_bytes:
        raise VideoFrameExtractionError("invalid_frame", f"{label} bytes are empty or exceed the PNG byte bound")
    _require_hash(frame_png, request.frame_sha256, label)
    image = _decode_png(frame_png, label, config.max_source_dimension_px)
    extracted = _remove_matte(image, request, config, label)
    bbox = _mask_bbox(extracted.mask)
    source_feasibility = _verify_source_feasibility(extracted, image.size, request, config, label)
    native = _render_frame(extracted.rgba, bbox, request, config)
    native_measurement = _verify_output_frame(native, request, config, f"{label} native frame")
    source_rgba_png = _png_bytes(Image.fromarray(extracted.rgba, mode="RGBA"))
    native_rgba_png = _png_bytes(Image.fromarray(native, mode="RGBA"))
    if max(len(source_rgba_png), len(native_rgba_png)) > config.max_frame_png_bytes:
        raise VideoFrameExtractionError("frame_too_large", f"{label} normalized PNG exceeds its byte bound")
    request_record = asdict(request)
    request_record["matte_rgb"] = list(request.matte_rgb)
    report: dict[str, Any] = {
        "schema_version": 1,
        "operation": "validate_matte_endpoint",
        "label": label,
        "request": request_record,
        "config": _matte_config_record(config),
        "source": {
            "width_px": image.width,
            "height_px": image.height,
            "input_png_sha256": request.frame_sha256,
            "source_rgba_png_sha256": _content_hash(source_rgba_png),
            "source_rgba_png_bytes": len(source_rgba_png),
            "matte_measurement": extracted.measurement,
            "feasibility": source_feasibility,
        },
        "native": {
            "width_px": request.output_width_px,
            "height_px": request.output_row_height_px,
            "native_rgba_png_sha256": _content_hash(native_rgba_png),
            "native_rgba_png_bytes": len(native_rgba_png),
            "measurement": native_measurement,
        },
    }
    report_payload = json.dumps(report, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    report["report_sha256"] = _content_hash(report_payload)
    return MatteEndpointResult(source_rgba_png, native_rgba_png, report)


def extract_rgba_frame_sheet(
    video_bytes: bytes,
    *,
    start_frame_png: bytes,
    end_frame_png: bytes,
    request: VideoFrameExtractionRequest,
    config: VideoFrameExtractionConfig,
) -> VideoFrameSheetResult:
    """Decode exact timestamps, remove one declared matte, and build one sheet.

    This function performs no provider or network operation. Every subprocess
    is an argv-only local ffmpeg/ffprobe invocation with a shared wall deadline,
    per-process timeout, disk-backed bounded stdout/stderr, and bounded output
    files.
    """

    if not isinstance(video_bytes, bytes) or not video_bytes or len(video_bytes) > config.max_video_bytes:
        raise VideoFrameExtractionError("invalid_video", "source video bytes are empty or exceed the configured bound")
    for label, value in (("start frame", start_frame_png), ("end frame", end_frame_png)):
        if not isinstance(value, bytes) or not value or len(value) > config.max_frame_png_bytes:
            raise VideoFrameExtractionError("invalid_frame", f"{label} bytes are empty or exceed the PNG byte bound")
    _require_hash(video_bytes, request.source_video_sha256, "source video")
    _require_hash(start_frame_png, request.start_frame_sha256, "start frame")
    _require_hash(end_frame_png, request.end_frame_sha256, "end frame")

    deadline = time.monotonic() + config.total_timeout_seconds
    tool_report = _video_toolchain_identity(config, deadline)
    ffmpeg = Path(tool_report["ffmpeg"]["path"])
    ffprobe = Path(tool_report["ffprobe"]["path"])

    commands: list[list[str]] = []
    with tempfile.TemporaryDirectory(prefix="snaketron-video-frames-") as directory:
        workspace = Path(directory)
        input_path = workspace / "input.mp4"
        input_path.write_bytes(video_bytes)
        probe_argv = [
            str(ffprobe),
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_streams",
            "-show_format",
            "-of",
            "json",
            str(input_path),
        ]
        commands.append(_normalized_command(probe_argv, workspace, ffmpeg, ffprobe))
        probe_output = _run_bounded(
            probe_argv,
            config=config,
            deadline=deadline,
            stdout_limit=config.max_probe_output_bytes,
        )
        probe = _parse_probe(probe_output, config)

        start_image = _decode_png(start_frame_png, "start frame", config.max_source_dimension_px)
        end_image = _decode_png(end_frame_png, "end frame", config.max_source_dimension_px)
        expected_size = (probe.width_px, probe.height_px)
        if start_image.size != expected_size or end_image.size != expected_size:
            raise VideoFrameExtractionError(
                "geometry_mismatch", "retained start/end frames must exactly match the decoded video dimensions"
            )

        decoded_pngs: list[bytes] = []
        timestamps_us = _timestamps_us(probe, request.frame_rows)
        for index, timestamp_us in enumerate(timestamps_us):
            output_path = workspace / f"decoded-{index:03d}.png"
            timestamp = f"{timestamp_us / 1_000_000:.6f}"
            argv = [
                str(ffmpeg),
                "-nostdin",
                "-hide_banner",
                "-loglevel",
                "error",
                "-i",
                str(input_path),
                "-ss",
                timestamp,
                "-map",
                "0:v:0",
                "-frames:v",
                "1",
                "-an",
                "-sn",
                "-dn",
                "-vf",
                "format=rgb24",
                "-c:v",
                "png",
                "-compression_level",
                "9",
                "-y",
                str(output_path),
            ]
            commands.append(_normalized_command(argv, workspace, ffmpeg, ffprobe))
            _run_bounded(
                argv,
                config=config,
                deadline=deadline,
                stdout_limit=16_384,
                watched_paths=((output_path, config.max_frame_png_bytes),),
            )
            if not output_path.is_file():
                raise VideoFrameExtractionError("decode_failed", f"ffmpeg did not produce decoded frame {index}")
            value = output_path.read_bytes()
            if not value or len(value) > config.max_frame_png_bytes:
                raise VideoFrameExtractionError("decode_failed", f"decoded frame {index} violates its PNG byte bound")
            decoded_pngs.append(value)

    source_images = [
        _decode_png(value, f"decoded frame {index}", config.max_source_dimension_px)
        for index, value in enumerate(decoded_pngs)
    ]
    for index, image in enumerate(source_images):
        if image.size != (probe.width_px, probe.height_px):
            raise VideoFrameExtractionError("geometry_mismatch", f"decoded frame {index} changed source dimensions")

    retained_start = _remove_matte(start_image, request, config, "retained start frame")
    retained_end = _remove_matte(end_image, request, config, "retained end frame")
    extracted = [
        _remove_matte(image, request, config, f"decoded frame {index}") for index, image in enumerate(source_images)
    ]
    source_feasibility = {
        "retained_start": _verify_source_feasibility(
            retained_start, start_image.size, request, config, "retained start frame"
        ),
        "retained_end": _verify_source_feasibility(retained_end, end_image.size, request, config, "retained end frame"),
        "frames": [
            _verify_source_feasibility(frame, image.size, request, config, f"decoded frame {index}")
            for index, (frame, image) in enumerate(zip(extracted, source_images, strict=True))
        ],
    }
    union_bbox = _union_bbox([retained_start, *extracted, retained_end])

    start_output = _render_frame(retained_start.rgba, union_bbox, request, config)
    end_output = _render_frame(retained_end.rgba, union_bbox, request, config)
    output_frames = [_render_frame(frame.rgba, union_bbox, request, config) for frame in extracted]
    output_measurements = [
        _verify_output_frame(frame, request, config, f"output frame {index}")
        for index, frame in enumerate(output_frames)
    ]

    endpoint_start = _frame_delta(output_frames[0], start_output, config.changed_channel_delta)
    endpoint_end = _frame_delta(output_frames[-1], end_output, config.changed_channel_delta)
    _require_delta(
        endpoint_start,
        config.max_endpoint_mean_abs_delta,
        config.max_endpoint_changed_fraction,
        "decoded row zero does not preserve the retained start frame",
    )
    _require_delta(
        endpoint_end,
        config.max_endpoint_mean_abs_delta,
        config.max_endpoint_changed_fraction,
        "decoded final row does not preserve the retained end frame",
    )

    joins = [
        _frame_delta(output_frames[index], output_frames[index + 1], config.changed_channel_delta)
        for index in range(len(output_frames) - 1)
    ]
    loop = _frame_delta(output_frames[-1], output_frames[0], config.changed_channel_delta)
    _require_delta(
        loop,
        config.max_loop_mean_abs_delta,
        config.max_loop_changed_fraction,
        "actual final-to-row-zero transition is not a valid loop",
    )
    centroids = [_centroid(frame[..., 3] > 0) for frame in output_frames]
    centroid_steps = [
        _distance(centroids[index], centroids[(index + 1) % len(centroids)]) for index in range(len(centroids))
    ]
    if max(centroid_steps, default=0.0) > config.max_centroid_step_px:
        raise VideoFrameExtractionError("camera_translation", "object centroid motion exceeds the declared bound")

    frame_pngs = tuple(_png_bytes(Image.fromarray(frame, mode="RGBA")) for frame in output_frames)
    sheet = Image.new(
        "RGBA", (request.output_width_px, request.output_row_height_px * request.frame_rows), (0, 0, 0, 0)
    )
    for index, frame in enumerate(output_frames):
        sheet.paste(Image.fromarray(frame, mode="RGBA"), (0, index * request.output_row_height_px))
    sheet_png = _png_bytes(sheet)
    if len(sheet_png) > config.max_frame_png_bytes:
        raise VideoFrameExtractionError("sheet_too_large", "encoded frame sheet exceeds its PNG byte bound")

    request_record = asdict(request)
    request_record["matte_rgb"] = list(request.matte_rgb)
    config_record = asdict(config)
    report: dict[str, Any] = {
        "schema_version": 1,
        "operation": "deterministic_video_to_rgba_frame_sheet",
        "request": request_record,
        "config": config_record,
        "tools": tool_report,
        "commands": commands,
        "probe": {
            "width_px": probe.width_px,
            "height_px": probe.height_px,
            "duration_us": probe.duration_us,
            "average_fps": f"{probe.average_fps.numerator}/{probe.average_fps.denominator}",
            "pixel_format": probe.pixel_format,
            "codec_name": probe.codec_name,
        },
        "timestamps_us": timestamps_us,
        "decoded_frame_sha256": [_content_hash(value) for value in decoded_pngs],
        "matte": {
            "rgb": list(request.matte_rgb),
            "algorithm": "bounded-geodesic-uncertainty-v1",
            **_matte_config_record(config),
            "retained_start": retained_start.measurement,
            "retained_end": retained_end.measurement,
            "frames": [frame.measurement for frame in extracted],
        },
        "union_source_bbox_xyxy": list(union_bbox),
        "source_feasibility": source_feasibility,
        "output": {
            "width_px": request.output_width_px,
            "row_height_px": request.output_row_height_px,
            "frame_rows": request.frame_rows,
            "decoded_rgba_bytes": request.output_width_px * request.output_row_height_px * request.frame_rows * 4,
            "frame_measurements": output_measurements,
            "frame_png_sha256": [_content_hash(value) for value in frame_pngs],
            "sheet_png_sha256": _content_hash(sheet_png),
            "sheet_png_bytes": len(sheet_png),
        },
        "endpoint_metrics": {"row_zero_to_start": endpoint_start, "final_to_end": endpoint_end},
        "temporal_join_metrics": joins,
        "final_to_row_zero": loop,
        "centroids_xy": [[round(x, 6), round(y, 6)] for x, y in centroids],
        "centroid_step_px": [round(value, 6) for value in centroid_steps],
    }
    recipe_payload = json.dumps(report, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    report["report_sha256"] = _content_hash(recipe_payload)
    return VideoFrameSheetResult(sheet_png=sheet_png, frame_pngs=frame_pngs, report=report)


def _valid_content_hash(value: Any) -> bool:
    return (
        isinstance(value, str)
        and value.startswith(_CONTENT_HASH_PREFIX)
        and len(value) == 71
        and all(character in "0123456789abcdef" for character in value[7:])
    )


def _content_hash(value: bytes) -> str:
    return f"sha256:{hashlib.sha256(value).hexdigest()}"


def _require_hash(value: bytes, expected: str, label: str) -> None:
    if _content_hash(value) != expected:
        raise VideoFrameExtractionError("hash_mismatch", f"{label} bytes do not match their retained hash")


def _resolve_tool(value: str, label: str) -> Path:
    resolved = shutil.which(value) if os.sep not in value else value
    if not resolved:
        raise VideoFrameExtractionError("tool_unavailable", f"{label} executable is unavailable")
    path = Path(resolved).resolve()
    if not path.is_file() or not os.access(path, os.X_OK):
        raise VideoFrameExtractionError("tool_unavailable", f"{label} executable is not an executable file")
    return path


def _tool_identity(path: Path, config: VideoFrameExtractionConfig, deadline: float) -> dict[str, Any]:
    output = _run_bounded(
        [str(path), "-version"],
        config=config,
        deadline=deadline,
        stdout_limit=config.max_probe_output_bytes,
    )
    first_line = output.decode("utf-8", "strict").splitlines()[0] if output else ""
    if not first_line:
        raise VideoFrameExtractionError("tool_unavailable", f"{path.name} did not report a version")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return {"path": str(path), "binary_sha256": f"sha256:{digest.hexdigest()}", "version": first_line}


def _run_bounded(
    argv: Sequence[str],
    *,
    config: VideoFrameExtractionConfig,
    deadline: float,
    stdout_limit: int,
    watched_paths: Sequence[tuple[Path, int]] = (),
) -> bytes:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise VideoFrameExtractionError("timeout", "video extraction exceeded its total wall deadline")
    process_timeout = min(config.subprocess_timeout_seconds, remaining)
    environment = media_tool_environment()
    with tempfile.TemporaryFile() as stdout_file, tempfile.TemporaryFile() as stderr_file:
        try:
            process = subprocess.Popen(
                list(argv),
                stdin=subprocess.DEVNULL,
                stdout=stdout_file,
                stderr=stderr_file,
                env=environment,
                shell=False,
                start_new_session=True,
            )
        except OSError as error:
            raise VideoFrameExtractionError(
                "tool_unavailable", f"could not start {Path(argv[0]).name}: {error}"
            ) from error
        started = time.monotonic()
        failure: VideoFrameExtractionError | None = None
        while process.poll() is None:
            now = time.monotonic()
            if now - started > process_timeout or now > deadline:
                failure = VideoFrameExtractionError("timeout", f"{Path(argv[0]).name} exceeded its wall deadline")
                break
            if os.fstat(stdout_file.fileno()).st_size > stdout_limit:
                failure = VideoFrameExtractionError("output_limit", f"{Path(argv[0]).name} exceeded stdout bound")
                break
            if os.fstat(stderr_file.fileno()).st_size > config.max_stderr_bytes:
                failure = VideoFrameExtractionError("output_limit", f"{Path(argv[0]).name} exceeded stderr bound")
                break
            if any(path.exists() and path.stat().st_size > limit for path, limit in watched_paths):
                failure = VideoFrameExtractionError("output_limit", f"{Path(argv[0]).name} exceeded output-file bound")
                break
            time.sleep(0.01)
        if failure is not None:
            _kill_process_group(process)
            process.wait(timeout=5)
            raise failure
        return_code = process.wait()
        if (
            os.fstat(stdout_file.fileno()).st_size > stdout_limit
            or os.fstat(stderr_file.fileno()).st_size > config.max_stderr_bytes
        ):
            raise VideoFrameExtractionError("output_limit", f"{Path(argv[0]).name} exceeded retained output bounds")
        stderr_file.seek(0)
        stderr = stderr_file.read(config.max_stderr_bytes).decode("utf-8", "replace")
        if return_code != 0:
            safe_error = " ".join(stderr.split())[:500]
            raise VideoFrameExtractionError(
                "subprocess_failed", f"{Path(argv[0]).name} exited {return_code}: {safe_error or 'no diagnostic'}"
            )
        stdout_file.seek(0)
        return stdout_file.read(stdout_limit)


def _kill_process_group(process: subprocess.Popen[Any]) -> None:
    with suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGKILL)


def _normalized_command(argv: Sequence[str], workspace: Path, ffmpeg: Path, ffprobe: Path) -> list[str]:
    normalized: list[str] = []
    for value in argv:
        if value == str(ffmpeg):
            normalized.append("$FFMPEG")
        elif value == str(ffprobe):
            normalized.append("$FFPROBE")
        elif value.startswith(str(workspace)):
            normalized.append(f"$WORK/{Path(value).name}")
        else:
            normalized.append(value)
    return normalized


def _parse_probe(value: bytes, config: VideoFrameExtractionConfig) -> _Probe:
    try:
        body = json.loads(value)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise VideoFrameExtractionError("probe_failed", "ffprobe returned invalid bounded JSON") from error
    streams = body.get("streams") if isinstance(body, Mapping) else None
    if not isinstance(streams, list) or len(streams) != 1 or not isinstance(streams[0], Mapping):
        raise VideoFrameExtractionError("probe_failed", "video must contain exactly one selected video stream")
    stream = streams[0]
    width = stream.get("width")
    height = stream.get("height")
    if (
        isinstance(width, bool)
        or not isinstance(width, int)
        or isinstance(height, bool)
        or not isinstance(height, int)
        or not 1 <= width <= config.max_source_dimension_px
        or not 1 <= height <= config.max_source_dimension_px
        or width * height > config.max_source_dimension_px**2
    ):
        raise VideoFrameExtractionError("probe_failed", "video dimensions are missing or unsafe")
    duration_value = stream.get("duration")
    if duration_value in {None, "N/A"} and isinstance(body.get("format"), Mapping):
        duration_value = body["format"].get("duration")
    try:
        duration_us = int((Decimal(str(duration_value)) * 1_000_000).to_integral_value(rounding=ROUND_HALF_UP))
    except (InvalidOperation, TypeError, ValueError) as error:
        raise VideoFrameExtractionError("probe_failed", "video duration is missing or invalid") from error
    if not 1 <= duration_us <= 60_000_000:
        raise VideoFrameExtractionError("probe_failed", "video duration is outside the bounded extraction range")
    try:
        average_fps = Fraction(str(stream.get("avg_frame_rate")))
    except (ValueError, ZeroDivisionError) as error:
        raise VideoFrameExtractionError("probe_failed", "video average frame rate is invalid") from error
    if not 0 < average_fps <= 240:
        raise VideoFrameExtractionError("probe_failed", "video average frame rate is outside the bounded range")
    rotation = 0
    tags = stream.get("tags")
    if isinstance(tags, Mapping) and tags.get("rotate") not in {None, "0", 0}:
        rotation = 1
    for entry in stream.get("side_data_list", []) if isinstance(stream.get("side_data_list"), list) else []:
        if isinstance(entry, Mapping) and entry.get("rotation") not in {None, 0, "0"}:
            rotation = 1
    if rotation:
        raise VideoFrameExtractionError("probe_failed", "rotated video streams are not accepted")
    pixel_format = stream.get("pix_fmt")
    codec_name = stream.get("codec_name")
    if not isinstance(pixel_format, str) or not pixel_format or not isinstance(codec_name, str) or not codec_name:
        raise VideoFrameExtractionError("probe_failed", "video codec or pixel format is missing")
    if pixel_format not in _OPAQUE_PIXEL_FORMATS:
        raise VideoFrameExtractionError(
            "ambiguous_alpha",
            f"source video pixel format {pixel_format[:64]!r} is not in the pinned opaque-format allowlist",
        )
    return _Probe(width, height, duration_us, average_fps, pixel_format[:64], codec_name[:64])


def _timestamps_us(probe: _Probe, rows: int) -> list[int]:
    # With output-side ``-ss``, ffmpeg emits the first frame at or after the
    # requested timestamp. Stay one declared frame interval before EOF so the
    # final sample resolves to the final frame instead of an empty decode.
    frame_interval_us = max(1, _round_fraction(Fraction(1_000_000, 1) / probe.average_fps))
    last_timestamp = max(0, probe.duration_us - frame_interval_us)
    timestamps = [_round_fraction(Fraction(index * last_timestamp, rows - 1)) for index in range(rows)]
    if timestamps != sorted(timestamps) or len(set(timestamps)) != rows:
        raise VideoFrameExtractionError(
            "cadence_invalid", "source duration cannot provide distinct requested timestamps"
        )
    return timestamps


def _round_fraction(value: Fraction) -> int:
    return (2 * value.numerator + value.denominator) // (2 * value.denominator)


def _decode_png(value: bytes, label: str, max_dimension_px: int) -> Image.Image:
    try:
        with Image.open(io.BytesIO(value)) as source:
            if (
                source.format != "PNG"
                or source.width <= 0
                or source.height <= 0
                or source.width > max_dimension_px
                or source.height > max_dimension_px
                or source.width * source.height > max_dimension_px**2
            ):
                raise ValueError("not a nonempty PNG")
            source.load()
            return source.convert("RGBA")
    except (OSError, UnidentifiedImageError, ValueError) as error:
        raise VideoFrameExtractionError("invalid_frame", f"{label} failed exact PNG decode: {error}") from error


def _matte_config_record(config: VideoFrameExtractionConfig) -> dict[str, Any]:
    return {
        "matte_tolerance": config.matte_tolerance,
        "object_min_distance": config.object_min_distance,
        "uncertainty_radius_px": config.uncertainty_radius_px,
        "max_uncertain_pixels": config.max_uncertain_pixels,
        "max_uncertain_fraction": config.max_uncertain_fraction,
        "max_background_cleanup_pixels": config.max_background_cleanup_pixels,
        "max_background_cleanup_fraction": config.max_background_cleanup_fraction,
        "min_object_pixels": config.min_object_pixels,
        "minimum_output_alpha": config.minimum_output_alpha,
        "max_source_bbox_area_fraction": config.max_source_bbox_area_fraction,
        "max_source_aspect_scale": config.max_source_aspect_scale,
        "max_source_upscale": config.max_source_upscale,
    }


def _remove_matte(
    image: Image.Image,
    request: VideoFrameExtractionRequest | MatteEndpointRequest,
    config: VideoFrameExtractionConfig,
    label: str,
) -> _MatteFrame:
    rgba = np.asarray(image.convert("RGBA"), dtype=np.uint8)
    if np.any(rgba[..., 3] != 255):
        raise VideoFrameExtractionError("ambiguous_alpha", f"{label} must be an opaque matte frame")
    rgb = rgba[..., :3].astype(np.int16)
    matte = np.asarray(request.matte_rgb, dtype=np.int16)
    distance = np.max(np.abs(rgb - matte), axis=2)
    matte_mask = distance <= config.matte_tolerance
    definite_object = distance >= config.object_min_distance
    ambiguous = ~(matte_mask | definite_object)
    ambiguous_count = int(np.count_nonzero(ambiguous))
    total_pixels = int(distance.size)
    if ambiguous_count > config.max_uncertain_pixels or ambiguous_count > math.floor(
        total_pixels * config.max_uncertain_fraction
    ):
        raise VideoFrameExtractionError(
            "ambiguous_matte",
            f"{label} exceeds the bounded matte uncertainty budget with {ambiguous_count} pixels",
        )
    definite_object_pixels = int(np.count_nonzero(definite_object))
    if definite_object_pixels < config.min_object_pixels:
        raise VideoFrameExtractionError("missing_object", f"{label} does not contain a bounded visible object")

    object_distance = _bounded_geodesic_distance(
        definite_object,
        definite_object | ambiguous,
        config.uncertainty_radius_px,
    )
    matte_distance = _bounded_geodesic_distance(
        matte_mask,
        matte_mask | ambiguous,
        config.uncertainty_radius_px,
    )
    foreground_uncertain = (
        ambiguous & (object_distance >= 0) & ((matte_distance < 0) | (object_distance <= matte_distance))
    )
    background_uncertain = ambiguous & ~foreground_uncertain & (matte_distance >= 0)
    unclassified = ambiguous & ~(foreground_uncertain | background_uncertain)
    unclassified_count = int(np.count_nonzero(unclassified))
    if unclassified_count:
        raise VideoFrameExtractionError(
            "ambiguous_matte",
            f"{label} has {unclassified_count} uncertain pixels beyond the declared reconstruction radius",
        )
    cleanup_count = int(np.count_nonzero(background_uncertain))
    if cleanup_count > config.max_background_cleanup_pixels or cleanup_count > math.floor(
        total_pixels * config.max_background_cleanup_fraction
    ):
        raise VideoFrameExtractionError(
            "ambiguous_matte",
            f"{label} exceeds the bounded background cleanup budget with {cleanup_count} pixels",
        )

    object_mask = definite_object | foreground_uncertain
    object_pixels = int(np.count_nonzero(object_mask))
    components = _component_count(object_mask)
    if components != 1:
        raise VideoFrameExtractionError("multiple_objects", f"{label} contains {components} disconnected objects")
    bbox = _mask_bbox(object_mask)
    left, top, right, bottom = bbox
    margins = [left, top, image.width - 1 - right, image.height - 1 - bottom]
    if min(margins) < request.source_apron_px:
        raise VideoFrameExtractionError("edge_contact", f"{label} violates the declared source apron")
    output = np.zeros_like(rgba)
    output[definite_object, :3] = rgba[definite_object, :3]
    output[definite_object, 3] = 255
    partial_alpha = np.zeros(distance.shape, dtype=np.uint8)
    if np.any(foreground_uncertain):
        denominator = config.object_min_distance - config.matte_tolerance
        partial_alpha[foreground_uncertain] = np.clip(
            ((distance[foreground_uncertain] - config.matte_tolerance) * 255 + denominator // 2) // denominator,
            1,
            254,
        ).astype(np.uint8)
        observed = rgb[foreground_uncertain].astype(np.int32)
        matte_row = matte.astype(np.int32)[None, :]
        alpha = partial_alpha[foreground_uncertain].astype(np.int32)[:, None]
        premultiplied_delta = (observed - matte_row) * 255
        reconstructed_delta = np.sign(premultiplied_delta) * ((np.abs(premultiplied_delta) + alpha // 2) // alpha)
        reconstructed = np.clip(matte_row + reconstructed_delta, 0, 255).astype(np.uint8)
        output[foreground_uncertain, :3] = reconstructed
        output[foreground_uncertain, 3] = partial_alpha[foreground_uncertain]

    foreground_uncertain_count = int(np.count_nonzero(foreground_uncertain))
    partial_values = partial_alpha[foreground_uncertain]
    return _MatteFrame(
        output,
        object_mask,
        {
            "object_pixels": object_pixels,
            "definite_object_pixels": definite_object_pixels,
            "matte_pixels": int(np.count_nonzero(matte_mask)),
            "uncertain_pixels": ambiguous_count,
            "uncertain_fraction": round(ambiguous_count / total_pixels, 8),
            "foreground_uncertain_pixels": foreground_uncertain_count,
            "background_cleanup_pixels": cleanup_count,
            "unclassified_uncertain_pixels": 0,
            "partial_alpha_pixels": foreground_uncertain_count,
            "partial_alpha_minimum": int(partial_values.min(initial=255)),
            "partial_alpha_maximum": int(partial_values.max(initial=0)),
            "maximum_foreground_reconstruction_distance_px": int(object_distance[foreground_uncertain].max(initial=0)),
            "maximum_background_cleanup_distance_px": int(matte_distance[background_uncertain].max(initial=0)),
            "definite_object_component_count": _component_count(definite_object),
            "component_count": components,
            "object_bbox_xyxy": list(bbox),
            "source_margins_px": margins,
            "maximum_matte_distance": int(distance[matte_mask].max(initial=0)),
            "minimum_definite_object_distance": int(distance[definite_object].min(initial=255)),
        },
    )


def _bounded_geodesic_distance(
    seeds: np.ndarray[Any, np.dtype[np.bool_]],
    allowed: np.ndarray[Any, np.dtype[np.bool_]],
    maximum: int,
) -> np.ndarray[Any, np.dtype[np.int16]]:
    distance = np.full(seeds.shape, -1, dtype=np.int16)
    distance[seeds] = 0
    frontier = seeds.copy()
    reached = seeds.copy()
    for step in range(1, maximum + 1):
        frontier = _dilate_eight(frontier) & allowed & ~reached
        if not np.any(frontier):
            break
        distance[frontier] = step
        reached |= frontier
    return distance


def _dilate_eight(mask: np.ndarray[Any, np.dtype[np.bool_]]) -> np.ndarray[Any, np.dtype[np.bool_]]:
    height, width = mask.shape
    padded = np.pad(mask, 1, mode="constant", constant_values=False)
    output = np.zeros_like(mask)
    for y_offset in range(3):
        for x_offset in range(3):
            output |= padded[y_offset : y_offset + height, x_offset : x_offset + width]
    return output


def _component_count(mask: np.ndarray[Any, np.dtype[np.bool_]]) -> int:
    parents: list[int] = []
    previous: list[tuple[int, int, int]] = []

    def make_set() -> int:
        parents.append(len(parents))
        return len(parents) - 1

    def find(value: int) -> int:
        while parents[value] != value:
            parents[value] = parents[parents[value]]
            value = parents[value]
        return value

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parents[right_root] = left_root

    for row in mask:
        padded = np.concatenate((np.array([False]), row, np.array([False])))
        changes = np.flatnonzero(padded[1:] != padded[:-1])
        current: list[tuple[int, int, int]] = []
        previous_index = 0
        for start, end_exclusive in zip(changes[0::2], changes[1::2], strict=True):
            end = int(end_exclusive - 1)
            start_value = int(start)
            while previous_index < len(previous) and previous[previous_index][1] < start_value - 1:
                previous_index += 1
            overlapping: list[int] = []
            scan = previous_index
            while scan < len(previous) and previous[scan][0] <= end + 1:
                overlapping.append(previous[scan][2])
                scan += 1
            label = overlapping[0] if overlapping else make_set()
            for other in overlapping[1:]:
                union(label, other)
            current.append((start_value, end, label))
        previous = current
    return len({find(index) for index in range(len(parents))})


def _mask_bbox(mask: np.ndarray[Any, np.dtype[np.bool_]]) -> tuple[int, int, int, int]:
    ys, xs = np.nonzero(mask)
    if not len(xs):
        raise VideoFrameExtractionError("missing_object", "object mask is empty")
    return int(xs.min()), int(ys.min()), int(xs.max()), int(ys.max())


def _union_bbox(frames: Sequence[_MatteFrame]) -> tuple[int, int, int, int]:
    boxes = [_mask_bbox(frame.mask) for frame in frames]
    return (
        min(box[0] for box in boxes),
        min(box[1] for box in boxes),
        max(box[2] for box in boxes),
        max(box[3] for box in boxes),
    )


def _verify_source_feasibility(
    frame: _MatteFrame,
    source_size: tuple[int, int],
    request: VideoFrameExtractionRequest | MatteEndpointRequest,
    config: VideoFrameExtractionConfig,
    label: str,
) -> dict[str, Any]:
    bbox = _mask_bbox(frame.mask)
    bbox_width = bbox[2] - bbox[0] + 1
    bbox_height = bbox[3] - bbox[1] + 1
    source_width, source_height = source_size
    bbox_area_fraction = bbox_width * bbox_height / (source_width * source_height)
    drawable_width = request.output_width_px - 2 * request.output_apron_px
    drawable_height = request.output_row_height_px - 2 * request.output_apron_px
    source_aspect = bbox_width / bbox_height
    native_aspect = drawable_width / drawable_height
    aspect_scale = max(source_aspect / native_aspect, native_aspect / source_aspect)
    fit_scale = min(drawable_width / bbox_width, drawable_height / bbox_height)
    if bbox_area_fraction > config.max_source_bbox_area_fraction:
        raise VideoFrameExtractionError(
            "source_geometry",
            f"{label} foreground bbox occupies {bbox_area_fraction:.6f} of its arena; "
            f"maximum is {config.max_source_bbox_area_fraction:.6f}",
        )
    if aspect_scale > config.max_source_aspect_scale:
        raise VideoFrameExtractionError(
            "source_geometry",
            f"{label} foreground aspect needs {aspect_scale:.6f}x native scaling; "
            f"maximum is {config.max_source_aspect_scale:.6f}x",
        )
    if fit_scale > config.max_source_upscale:
        raise VideoFrameExtractionError(
            "source_geometry",
            f"{label} foreground needs {fit_scale:.6f}x upscaling to native geometry; "
            f"maximum is {config.max_source_upscale:.6f}x",
        )
    return {
        "source_width_px": source_width,
        "source_height_px": source_height,
        "bbox_width_px": bbox_width,
        "bbox_height_px": bbox_height,
        "bbox_area_fraction": round(bbox_area_fraction, 8),
        "source_bbox_aspect": round(source_aspect, 8),
        "native_drawable_width_px": drawable_width,
        "native_drawable_height_px": drawable_height,
        "native_drawable_aspect": round(native_aspect, 8),
        "aspect_scale": round(aspect_scale, 8),
        "native_fit_scale": round(fit_scale, 8),
        "max_bbox_area_fraction": config.max_source_bbox_area_fraction,
        "max_aspect_scale": config.max_source_aspect_scale,
        "max_upscale": config.max_source_upscale,
    }


def _render_frame(
    rgba: np.ndarray[Any, np.dtype[np.uint8]],
    bbox: tuple[int, int, int, int],
    request: VideoFrameExtractionRequest | MatteEndpointRequest,
    config: VideoFrameExtractionConfig,
) -> np.ndarray[Any, np.dtype[np.uint8]]:
    left, top, right, bottom = bbox
    crop = rgba[top : bottom + 1, left : right + 1]
    available_width = request.output_width_px - 2 * request.output_apron_px
    available_height = request.output_row_height_px - 2 * request.output_apron_px
    scale = min(available_width / crop.shape[1], available_height / crop.shape[0])
    width = max(1, min(available_width, math.floor(crop.shape[1] * scale + 0.5)))
    height = max(1, min(available_height, math.floor(crop.shape[0] * scale + 0.5)))
    resized = _premultiplied_resize(crop, (width, height), config.minimum_output_alpha)
    output = np.zeros((request.output_row_height_px, request.output_width_px, 4), dtype=np.uint8)
    x = (request.output_width_px - width) // 2
    y = (request.output_row_height_px - height) // 2
    output[y : y + height, x : x + width] = resized
    return output


def _premultiplied_resize(
    rgba: np.ndarray[Any, np.dtype[np.uint8]], size: tuple[int, int], minimum_output_alpha: int
) -> np.ndarray[Any, np.dtype[np.uint8]]:
    if (rgba.shape[1], rgba.shape[0]) == size:
        return rgba.copy()
    alpha = rgba[..., 3].astype(np.float32) / 255.0
    premultiplied = rgba[..., :3].astype(np.float32) * alpha[..., None]
    resized_alpha = _resize_float(alpha, size)
    resized_premultiplied = np.stack(
        [_resize_float(premultiplied[..., channel], size) for channel in range(3)],
        axis=2,
    )
    resized_alpha = np.clip(resized_alpha, 0.0, 1.0)
    output = np.zeros((size[1], size[0], 4), dtype=np.uint8)
    visible = resized_alpha >= (minimum_output_alpha / 255.0)
    unpremultiplied = np.zeros_like(resized_premultiplied)
    unpremultiplied[visible] = resized_premultiplied[visible] / resized_alpha[visible, None]
    output[..., :3] = np.clip(np.rint(unpremultiplied), 0, 255).astype(np.uint8)
    output[..., 3] = np.clip(np.rint(resized_alpha * 255.0), 0, 255).astype(np.uint8)
    output[~visible] = 0
    return output


def _resize_float(value: np.ndarray[Any, np.dtype[np.float32]], size: tuple[int, int]) -> np.ndarray[Any, Any]:
    image = Image.fromarray(value, mode="F")
    # Positive bilinear weights cannot create the colored negative lobes that
    # Lanczos introduces around transparent matte edges. The resize remains
    # premultiplied, so this is deterministic antialiasing without RGB halos.
    return np.asarray(image.resize(size, resample=Image.Resampling.BILINEAR), dtype=np.float32)


def _verify_output_frame(
    frame: np.ndarray[Any, np.dtype[np.uint8]],
    request: VideoFrameExtractionRequest | MatteEndpointRequest,
    config: VideoFrameExtractionConfig,
    label: str,
) -> dict[str, Any]:
    alpha = frame[..., 3]
    mask = alpha > 0
    if not np.any(mask):
        raise VideoFrameExtractionError("missing_object", f"{label} became empty")
    components = _component_count(mask)
    if components != 1:
        raise VideoFrameExtractionError("multiple_objects", f"{label} contains {components} output objects")
    bbox = _mask_bbox(mask)
    margins = [
        bbox[0],
        bbox[1],
        request.output_width_px - 1 - bbox[2],
        request.output_row_height_px - 1 - bbox[3],
    ]
    if min(margins) < request.output_apron_px:
        raise VideoFrameExtractionError("edge_contact", f"{label} violates the exact output apron")
    if np.any(frame[alpha == 0, :3] != 0):
        raise VideoFrameExtractionError("alpha_halo", f"{label} has RGB contamination under transparent pixels")
    partial = (alpha > 0) & (alpha < 255)
    if np.any(partial):
        rgb = frame[..., :3].astype(np.int16)
        matte = np.asarray(request.matte_rgb, dtype=np.int16)
        distance = np.max(np.abs(rgb - matte), axis=2)
        if np.any(distance[partial] <= config.matte_tolerance):
            raise VideoFrameExtractionError("alpha_halo", f"{label} has partial-alpha matte fringe pixels")
    return {
        "object_bbox_xyxy": list(bbox),
        "margins_px": margins,
        "component_count": components,
        "visible_pixels": int(np.count_nonzero(mask)),
        "partial_alpha_pixels": int(np.count_nonzero(partial)),
        "transparent_rgb_contamination_pixels": 0,
    }


def _frame_delta(
    left: np.ndarray[Any, np.dtype[np.uint8]],
    right: np.ndarray[Any, np.dtype[np.uint8]],
    threshold: int,
) -> dict[str, Any]:
    difference = np.abs(left.astype(np.int16) - right.astype(np.int16))
    changed = np.any(difference > threshold, axis=2)
    return {
        "mean_absolute_channel_delta": round(float(difference.mean()), 6),
        "maximum_channel_delta": int(difference.max(initial=0)),
        "changed_pixel_fraction": round(float(changed.mean()), 6),
    }


def _require_delta(metrics: Mapping[str, Any], max_mean: float, max_fraction: float, message: str) -> None:
    if metrics["mean_absolute_channel_delta"] > max_mean or metrics["changed_pixel_fraction"] > max_fraction:
        raise VideoFrameExtractionError("temporal_discontinuity", message)


def _centroid(mask: np.ndarray[Any, np.dtype[np.bool_]]) -> tuple[float, float]:
    ys, xs = np.nonzero(mask)
    return float(xs.mean()), float(ys.mean())


def _distance(left: tuple[float, float], right: tuple[float, float]) -> float:
    return math.hypot(left[0] - right[0], left[1] - right[1])


def _png_bytes(image: Image.Image) -> bytes:
    output = io.BytesIO()
    image.save(output, format="PNG", optimize=False, compress_level=9)
    return output.getvalue()


__all__ = [
    "MatteEndpointRequest",
    "MatteEndpointResult",
    "VideoFrameExtractionConfig",
    "VideoFrameExtractionError",
    "VideoFrameExtractionRequest",
    "VideoFrameSheetResult",
    "extract_rgba_frame_sheet",
    "media_tool_environment",
    "validate_matte_endpoint",
    "video_toolchain_identity",
]
