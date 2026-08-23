"""Deterministically project provider artwork through renderer-owned geometry.

The image generator is allowed to propose surface material, but it is never
allowed to define Snaketron geometry.  This module detects the one centered
foreground subject, maps its material into the contract's native paint box,
clips it with the exact retained renderer pixels, restores the system-owned
head core, and only then applies the contract's nearest-neighbour presentation
scale.
"""

from __future__ import annotations

import io
import struct
import zlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
from PIL import Image, ImageOps, UnidentifiedImageError

PROTOTYPE_PROJECTION_VERSION = "prototype-body-mask-v1"
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


class PrototypeProjectionError(ValueError):
    """The candidate or pinned projection authority cannot be projected safely."""


@dataclass(frozen=True, slots=True)
class PrototypeProjection:
    """A deterministic projected review image and its detected source geometry."""

    png_bytes: bytes
    source_bbox: tuple[int, int, int, int]
    source_size: tuple[int, int]
    version: str = PROTOTYPE_PROJECTION_VERSION


@dataclass(frozen=True, slots=True)
class _Config:
    border_fraction: float
    robust_keep_fraction: float
    robust_iterations: int
    max_border_samples: int
    foreground_delta: int
    foreground_chroma_delta: int
    neutral_dark_delta: int
    axis_support_fraction: float
    max_axis_gap_fraction: float
    max_secondary_support_fraction: float
    max_center_offset_fraction: tuple[float, float]
    min_width_fraction: float
    max_height_fraction: float
    min_aspect_ratio: float
    max_input_pixels: int
    native_body_bbox: tuple[int, int, int, int]
    native_head_core_bbox: tuple[int, int, int, int]
    native_head_core_center: tuple[float, float]
    native_head_core_restore_radius: float


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise PrototypeProjectionError(f"{field} must be an object")
    return value


def _integer(value: Any, field: str, *, minimum: int = 1) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise PrototypeProjectionError(f"{field} must be an integer >= {minimum}")
    return value


def _number(value: Any, field: str, *, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PrototypeProjectionError(f"{field} must be a number")
    result = float(value)
    if not np.isfinite(result) or not minimum <= result <= maximum:
        raise PrototypeProjectionError(f"{field} must be between {minimum} and {maximum}")
    return result


def _sequence(value: Any, field: str, length: int) -> Sequence[Any]:
    if not isinstance(value, (list, tuple)) or len(value) != length:
        raise PrototypeProjectionError(f"{field} must contain exactly {length} values")
    return value


def _bbox(value: Any, field: str) -> tuple[int, int, int, int]:
    values = _sequence(value, field, 4)
    result = tuple(_integer(item, f"{field}[{index}]", minimum=0) for index, item in enumerate(values))
    x0, y0, x1, y1 = result
    if x0 >= x1 or y0 >= y1:
        raise PrototypeProjectionError(f"{field} must be a non-empty half-open box")
    return result


def _rgb(value: Any, field: str) -> np.ndarray:
    if not isinstance(value, str) or len(value) != 7 or not value.startswith("#"):
        raise PrototypeProjectionError(f"{field} must be a #rrggbb color")
    try:
        color = bytes.fromhex(value[1:])
    except ValueError as error:
        raise PrototypeProjectionError(f"{field} must be a #rrggbb color") from error
    if len(color) != 3:
        raise PrototypeProjectionError(f"{field} must be a #rrggbb color")
    return np.frombuffer(color, dtype=np.uint8).copy()


def _parse_config(contract: Mapping[str, Any]) -> _Config:
    projection = _mapping(contract.get("prototype_projection"), "prototype_projection")
    if projection.get("version") != PROTOTYPE_PROJECTION_VERSION:
        raise PrototypeProjectionError(f"prototype_projection.version must be {PROTOTYPE_PROJECTION_VERSION!r}")
    detection = _mapping(projection.get("foreground_detection"), "prototype_projection.foreground_detection")
    if detection.get("background_model") != "robust_quadratic_border_rgb":
        raise PrototypeProjectionError("unsupported prototype foreground background model")
    mapping = _mapping(projection.get("mapping"), "prototype_projection.mapping")
    if mapping.get("source_resample") != "center_aligned_bilinear_rgb8":
        raise PrototypeProjectionError("unsupported prototype source resampler")
    output = _mapping(projection.get("output"), "prototype_projection.output")
    required_output = {
        "body_clip": "exact_renderer_reference_alpha",
        "head_core": "exact_renderer_reference_pixels",
        "background": "exact_guide_canvas_background",
        "presentation": "contract_nearest_neighbor_integer_upscale",
        "png": "deterministic_rgb8_filter0_zlib9",
    }
    if dict(output) != required_output:
        raise PrototypeProjectionError("prototype_projection.output is not the supported deterministic policy")

    center_values = _sequence(
        mapping.get("native_head_core_center_px"),
        "prototype_projection.mapping.native_head_core_center_px",
        2,
    )
    center = (
        _number(center_values[0], "native_head_core_center_px[0]", minimum=0, maximum=100_000),
        _number(center_values[1], "native_head_core_center_px[1]", minimum=0, maximum=100_000),
    )
    center_offset_values = _sequence(
        detection.get("max_center_offset_fraction"),
        "prototype_projection.foreground_detection.max_center_offset_fraction",
        2,
    )
    return _Config(
        border_fraction=_number(
            detection.get("border_fraction"), "foreground_detection.border_fraction", minimum=0.01, maximum=0.25
        ),
        robust_keep_fraction=_number(
            detection.get("robust_keep_fraction"),
            "foreground_detection.robust_keep_fraction",
            minimum=0.5,
            maximum=1,
        ),
        robust_iterations=_integer(detection.get("robust_iterations"), "foreground_detection.robust_iterations"),
        max_border_samples=_integer(
            detection.get("max_border_samples"), "foreground_detection.max_border_samples", minimum=64
        ),
        foreground_delta=_integer(detection.get("foreground_delta"), "foreground_detection.foreground_delta"),
        foreground_chroma_delta=_integer(
            detection.get("foreground_chroma_delta"), "foreground_detection.foreground_chroma_delta"
        ),
        neutral_dark_delta=_integer(detection.get("neutral_dark_delta"), "foreground_detection.neutral_dark_delta"),
        axis_support_fraction=_number(
            detection.get("axis_support_fraction"),
            "foreground_detection.axis_support_fraction",
            minimum=0.0001,
            maximum=0.25,
        ),
        max_axis_gap_fraction=_number(
            detection.get("max_axis_gap_fraction"),
            "foreground_detection.max_axis_gap_fraction",
            minimum=0,
            maximum=0.25,
        ),
        max_secondary_support_fraction=_number(
            detection.get("max_secondary_support_fraction"),
            "foreground_detection.max_secondary_support_fraction",
            minimum=0,
            maximum=1,
        ),
        max_center_offset_fraction=(
            _number(center_offset_values[0], "max_center_offset_fraction[0]", minimum=0, maximum=0.5),
            _number(center_offset_values[1], "max_center_offset_fraction[1]", minimum=0, maximum=0.5),
        ),
        min_width_fraction=_number(
            detection.get("min_width_fraction"),
            "foreground_detection.min_width_fraction",
            minimum=0.01,
            maximum=1,
        ),
        max_height_fraction=_number(
            detection.get("max_height_fraction"),
            "foreground_detection.max_height_fraction",
            minimum=0.01,
            maximum=1,
        ),
        min_aspect_ratio=_number(
            detection.get("min_aspect_ratio"),
            "foreground_detection.min_aspect_ratio",
            minimum=1,
            maximum=100,
        ),
        max_input_pixels=_integer(
            detection.get("max_input_pixels"), "foreground_detection.max_input_pixels", minimum=1_024
        ),
        native_body_bbox=_bbox(mapping.get("native_body_bbox_px"), "mapping.native_body_bbox_px"),
        native_head_core_bbox=_bbox(mapping.get("native_head_core_bbox_px"), "mapping.native_head_core_bbox_px"),
        native_head_core_center=center,
        native_head_core_restore_radius=_number(
            mapping.get("native_head_core_restore_radius_px"),
            "mapping.native_head_core_restore_radius_px",
            minimum=0.5,
            maximum=100,
        ),
    )


def _decode_candidate(
    payload: bytes, *, maximum_pixels: int, background: np.ndarray
) -> tuple[np.ndarray, np.ndarray | None]:
    if not isinstance(payload, bytes) or not payload:
        raise PrototypeProjectionError("prototype candidate must be non-empty image bytes")
    try:
        with Image.open(io.BytesIO(payload)) as opened:
            width, height = opened.size
            if width < 16 or height < 16:
                raise PrototypeProjectionError("prototype candidate dimensions are too small")
            if width * height > maximum_pixels:
                raise PrototypeProjectionError(
                    f"prototype candidate has {width * height} pixels, over the {maximum_pixels} pixel limit"
                )
            if getattr(opened, "n_frames", 1) != 1:
                raise PrototypeProjectionError("animated prototype candidates are not supported")
            rgba = ImageOps.exif_transpose(opened).convert("RGBA")
            pixels = np.asarray(rgba, dtype=np.uint8).copy()
    except PrototypeProjectionError:
        raise
    except (Image.DecompressionBombError, UnidentifiedImageError, OSError, ValueError) as error:
        raise PrototypeProjectionError(f"prototype candidate is not a decodable image: {error}") from error

    alpha = pixels[:, :, 3].astype(np.uint16)
    rgb = pixels[:, :, :3].astype(np.uint16)
    composite = (rgb * alpha[:, :, None] + background.astype(np.uint16) * (255 - alpha[:, :, None]) + 127) // 255
    transparency = pixels[:, :, 3] if np.any(pixels[:, :, 3] != 255) else None
    return composite.astype(np.uint8), transparency


def _decode_native_reference(
    payload: bytes, contract: Mapping[str, Any], scale: int, background: np.ndarray
) -> np.ndarray:
    canvas = _mapping(contract.get("guide_canvas"), "guide_canvas")
    width = _integer(canvas.get("width_px"), "guide_canvas.width_px")
    height = _integer(canvas.get("height_px"), "guide_canvas.height_px")
    try:
        with Image.open(io.BytesIO(payload)) as opened:
            if opened.format != "PNG" or opened.mode != "RGB" or opened.size != (width, height):
                raise PrototypeProjectionError(
                    f"prototype guide must be RGB PNG {width}x{height}, got {opened.format} {opened.mode} {opened.size}"
                )
            guide = np.asarray(opened, dtype=np.uint8).copy()
    except PrototypeProjectionError:
        raise
    except (Image.DecompressionBombError, UnidentifiedImageError, OSError, ValueError) as error:
        raise PrototypeProjectionError(f"prototype guide is not a decodable image: {error}") from error

    if width % scale or height % scale:
        raise PrototypeProjectionError("prototype guide dimensions are not divisible by presentation scale")
    native = guide[::scale, ::scale]
    reconstructed = np.repeat(np.repeat(native, scale, axis=0), scale, axis=1)
    if not np.array_equal(reconstructed, guide):
        raise PrototypeProjectionError("prototype guide is not an exact nearest-neighbour native upscale")
    if not np.all(guide[0, 0] == background):
        raise PrototypeProjectionError("prototype guide background disagrees with its contract")
    return native


def _fit_background(image: np.ndarray, config: _Config) -> np.ndarray:
    height, width, _ = image.shape
    band_y = max(2, int(np.ceil(height * config.border_fraction)))
    band_x = max(2, int(np.ceil(width * config.border_fraction)))
    border = np.zeros((height, width), dtype=bool)
    border[:band_y] = True
    border[-band_y:] = True
    border[:, :band_x] = True
    border[:, -band_x:] = True
    sample_y, sample_x = np.nonzero(border)
    if len(sample_x) > config.max_border_samples:
        positions = np.linspace(0, len(sample_x) - 1, config.max_border_samples, dtype=np.int64)
        sample_y = sample_y[positions]
        sample_x = sample_x[positions]
    normalized_x = sample_x.astype(np.float64) * (2 / max(1, width - 1)) - 1
    normalized_y = sample_y.astype(np.float64) * (2 / max(1, height - 1)) - 1
    design = np.column_stack(
        (
            np.ones_like(normalized_x),
            normalized_x,
            normalized_y,
            normalized_x * normalized_y,
            normalized_x * normalized_x,
            normalized_y * normalized_y,
        )
    )
    observations = image[sample_y, sample_x].astype(np.float64)
    retained = np.ones(len(sample_x), dtype=bool)
    coefficients = np.zeros((design.shape[1], 3), dtype=np.float64)
    for iteration in range(config.robust_iterations + 1):
        if int(retained.sum()) < design.shape[1] * 4:
            raise PrototypeProjectionError("prototype background border has too little usable support")
        coefficients = np.linalg.lstsq(design[retained], observations[retained], rcond=None)[0]
        if iteration == config.robust_iterations:
            break
        residual = np.max(np.abs(observations - design @ coefficients), axis=1)
        keep_count = max(design.shape[1] * 4, int(np.ceil(len(residual) * config.robust_keep_fraction)))
        threshold = np.partition(residual, keep_count - 1)[keep_count - 1]
        retained = residual <= threshold

    x = np.linspace(-1, 1, width, dtype=np.float64)
    y = np.linspace(-1, 1, height, dtype=np.float64)
    predicted = np.empty_like(image)
    x_squared = x * x
    # Generate the fitted surface in bounded row chunks. Real Gemini output is
    # 2752x1536; a full float64 RGB surface would otherwise add a >100 MiB
    # transient solely to discover its background.
    for start in range(0, height, 128):
        end = min(start + 128, height)
        chunk_y = y[start:end, None]
        chunk_y_squared = chunk_y * chunk_y
        for channel in range(3):
            surface = (
                coefficients[0, channel]
                + x[None, :] * coefficients[1, channel]
                + chunk_y * coefficients[2, channel]
                + chunk_y * x[None, :] * coefficients[3, channel]
                + x_squared[None, :] * coefficients[4, channel]
                + chunk_y_squared * coefficients[5, channel]
            )
            predicted[start:end, :, channel] = np.clip(np.rint(surface), 0, 255).astype(np.uint8)
    return predicted


def _close_small_gaps(active: np.ndarray, maximum_gap: int) -> np.ndarray:
    result = active.copy()
    if maximum_gap <= 0:
        return result
    false_indices = np.flatnonzero(~result)
    if not len(false_indices):
        return result
    start = 0
    while start < len(false_indices):
        end = start + 1
        while end < len(false_indices) and false_indices[end] == false_indices[end - 1] + 1:
            end += 1
        first = int(false_indices[start])
        last = int(false_indices[end - 1])
        if first > 0 and last + 1 < len(result) and last - first + 1 <= maximum_gap:
            result[first : last + 1] = True
        start = end
    return result


def _runs(active: np.ndarray) -> list[tuple[int, int]]:
    padded = np.pad(active.astype(np.int8), (1, 1))
    transitions = np.diff(padded)
    starts = np.flatnonzero(transitions == 1)
    ends = np.flatnonzero(transitions == -1)
    return [(int(start), int(end)) for start, end in zip(starts, ends, strict=True)]


def _single_supported_run(
    support: np.ndarray,
    *,
    minimum_support: int,
    maximum_gap: int,
    maximum_secondary_fraction: float,
    field: str,
) -> tuple[int, int]:
    active = _close_small_gaps(support >= minimum_support, maximum_gap)
    runs = _runs(active)
    if not runs:
        raise PrototypeProjectionError(f"no supported foreground run was detected on the {field} axis")
    scored = sorted(
        ((int(support[start:end].sum()), end - start, start, end) for start, end in runs),
        reverse=True,
    )
    primary_score, _, primary_start, primary_end = scored[0]
    if len(scored) > 1 and scored[1][0] > primary_score * maximum_secondary_fraction:
        raise PrototypeProjectionError(f"multiple comparable foreground subjects were detected on the {field} axis")
    return primary_start, primary_end


def _foreground_bbox(
    image: np.ndarray, transparency: np.ndarray | None, predicted_background: np.ndarray, config: _Config
) -> tuple[int, int, int, int]:
    signed = image.astype(np.int16) - predicted_background.astype(np.int16)
    absolute_delta = np.max(np.abs(signed), axis=2)
    chroma_delta = np.max(signed, axis=2) - np.min(signed, axis=2)
    channel_sum = np.sum(signed, axis=2, dtype=np.int32)
    primary_foreground = (
        (absolute_delta >= config.foreground_delta) & (chroma_delta >= config.foreground_chroma_delta)
    ) | (channel_sum >= config.foreground_delta * 3)
    if transparency is not None:
        primary_foreground |= transparency >= config.foreground_delta

    height, width = primary_foreground.shape
    minimum_x_support = max(2, int(np.ceil(height * config.axis_support_fraction)))
    # Neutral cast shadows are intentionally absent from the primary mask.
    # A genuinely all-dark, neutral subject remains supported as a fail-closed
    # fallback when chroma, highlights, and alpha provide no *long* subject. A
    # small bright head core in an otherwise dark skin must not hide its body.
    primary_active = _close_small_gaps(
        primary_foreground.sum(axis=0) >= minimum_x_support,
        max(1, int(np.ceil(width * config.max_axis_gap_fraction))),
    )
    has_long_primary = any(end - start >= width * config.min_width_fraction for start, end in _runs(primary_active))
    if has_long_primary:
        foreground = primary_foreground
    else:
        foreground = primary_foreground | (channel_sum <= -config.neutral_dark_delta * 3)
    x0, x1 = _single_supported_run(
        foreground.sum(axis=0),
        minimum_support=minimum_x_support,
        maximum_gap=max(1, int(np.ceil(width * config.max_axis_gap_fraction))),
        maximum_secondary_fraction=config.max_secondary_support_fraction,
        field="horizontal",
    )
    y0, y1 = _single_supported_run(
        foreground[:, x0:x1].sum(axis=1),
        minimum_support=max(2, int(np.ceil((x1 - x0) * config.axis_support_fraction))),
        maximum_gap=max(1, int(np.ceil(height * config.max_axis_gap_fraction))),
        maximum_secondary_fraction=config.max_secondary_support_fraction,
        field="vertical",
    )
    # Re-measure horizontal support inside the selected vertical subject so
    # isolated border noise cannot widen the final material crop.
    x0, x1 = _single_supported_run(
        foreground[y0:y1].sum(axis=0),
        minimum_support=max(2, int(np.ceil((y1 - y0) * config.axis_support_fraction))),
        maximum_gap=max(1, int(np.ceil(width * config.max_axis_gap_fraction))),
        maximum_secondary_fraction=config.max_secondary_support_fraction,
        field="horizontal",
    )

    body_width = x1 - x0
    body_height = y1 - y0
    center_x = (x0 + x1) / 2
    center_y = (y0 + y1) / 2
    if abs(center_x - width / 2) > width * config.max_center_offset_fraction[0]:
        raise PrototypeProjectionError("prototype foreground is not horizontally centered")
    if abs(center_y - height / 2) > height * config.max_center_offset_fraction[1]:
        raise PrototypeProjectionError("prototype foreground is not vertically centered")
    if body_width < width * config.min_width_fraction:
        raise PrototypeProjectionError("prototype foreground is too short to be the centered snake")
    if body_height > height * config.max_height_fraction:
        raise PrototypeProjectionError("prototype foreground is too tall to be the centered snake")
    if body_width / body_height < config.min_aspect_ratio:
        raise PrototypeProjectionError("prototype foreground is not a thin horizontal snake")
    return x0, y0, x1, y1


def _resample_axis(source_length: int, destination_length: int) -> tuple[np.ndarray, np.ndarray, np.ndarray, int]:
    denominator = destination_length * 2
    lower = np.empty(destination_length, dtype=np.int64)
    upper = np.empty(destination_length, dtype=np.int64)
    weight = np.empty(destination_length, dtype=np.int64)
    last = source_length - 1
    for destination in range(destination_length):
        numerator = (destination * 2 + 1) * source_length - destination_length
        if numerator <= 0:
            lower[destination] = upper[destination] = 0
            weight[destination] = 0
        elif numerator >= last * denominator:
            lower[destination] = upper[destination] = last
            weight[destination] = 0
        else:
            index, remainder = divmod(numerator, denominator)
            lower[destination] = index
            upper[destination] = index + 1
            weight[destination] = remainder
    return lower, upper, weight, denominator


def _bilinear_rgb8(source: np.ndarray, width: int, height: int) -> np.ndarray:
    source_height, source_width, _ = source.shape
    x0, x1, x_weight, x_denominator = _resample_axis(source_width, width)
    y0, y1, y_weight, y_denominator = _resample_axis(source_height, height)
    source64 = source.astype(np.int64)
    horizontal = source64[:, x0] * (x_denominator - x_weight)[None, :, None] + source64[:, x1] * x_weight[None, :, None]
    combined = horizontal[y0] * (y_denominator - y_weight)[:, None, None] + horizontal[y1] * y_weight[:, None, None]
    denominator = x_denominator * y_denominator
    return ((combined + denominator // 2) // denominator).astype(np.uint8)


def _validate_geometry(
    contract: Mapping[str, Any], config: _Config, native: np.ndarray
) -> tuple[np.ndarray, np.ndarray, int]:
    source = _mapping(contract.get("renderer_source"), "renderer_source")
    cell = _integer(source.get("native_cell_px"), "renderer_source.native_cell_px")
    body_cells = _integer(source.get("body_cells"), "renderer_source.body_cells")
    native_canvas = _mapping(source.get("native_canvas"), "renderer_source.native_canvas")
    expected_size = (
        _integer(native_canvas.get("width_px"), "renderer_source.native_canvas.width_px"),
        _integer(native_canvas.get("height_px"), "renderer_source.native_canvas.height_px"),
    )
    if native.shape[:2] != (expected_size[1], expected_size[0]):
        raise PrototypeProjectionError("prototype guide native dimensions disagree with renderer_source.native_canvas")
    x0, y0, x1, y1 = config.native_body_bbox
    if x1 > expected_size[0] or y1 > expected_size[1]:
        raise PrototypeProjectionError("native body paint box escapes the renderer canvas")
    if (x1 - x0, y1 - y0) != (body_cells * cell, cell):
        raise PrototypeProjectionError("native body paint box is not exactly body_cells by one cell")
    core_x0, core_y0, core_x1, core_y1 = config.native_head_core_bbox
    if core_x0 < x0 or core_y0 < y0 or core_x1 > x1 or core_y1 > y1:
        raise PrototypeProjectionError("native head-core restore box escapes the body paint box")
    center_x, center_y = config.native_head_core_center
    if not core_x0 <= center_x <= core_x1 or not core_y0 <= center_y <= core_y1:
        raise PrototypeProjectionError("native head-core center escapes its restore box")
    return np.array(config.native_body_bbox), np.array(config.native_head_core_bbox), cell


def _project_native(
    material: np.ndarray,
    reference: np.ndarray,
    config: _Config,
    background: np.ndarray,
    body_color: np.ndarray,
) -> np.ndarray:
    if not np.all(reference[:, :, 0] == reference[:, :, 1]) or not np.all(reference[:, :, 1] == reference[:, :, 2]):
        raise PrototypeProjectionError("prototype renderer reference must use neutral RGB mask pixels")
    if not np.all(background == background[0]) or not np.all(body_color == body_color[0]):
        raise PrototypeProjectionError("prototype renderer mask and background must be neutral RGB colors")
    denominator = int(body_color[0]) - int(background[0])
    if denominator <= 0:
        raise PrototypeProjectionError("prototype body mask must be lighter than its background")

    x0, y0, x1, y1 = config.native_body_bbox
    native_height, native_width, _ = reference.shape
    source_x = np.clip(np.arange(native_width), x0, x1 - 1) - x0
    source_y = np.clip(np.arange(native_height), y0, y1 - 1) - y0
    texture = material[source_y[:, None], source_x[None, :]].astype(np.int32)
    alpha_numerator = np.clip(reference[:, :, 0].astype(np.int32) - int(background[0]), 0, denominator)
    projected = (
        background.astype(np.int32)[None, None, :] * (denominator - alpha_numerator[:, :, None])
        + texture * alpha_numerator[:, :, None]
        + denominator // 2
    ) // denominator
    projected = projected.astype(np.uint8)

    core_x0, core_y0, core_x1, core_y1 = config.native_head_core_bbox
    center_x, center_y = config.native_head_core_center
    yy, xx = np.ogrid[:native_height, :native_width]
    core = (xx + 0.5 - center_x) ** 2 + (yy + 0.5 - center_y) ** 2 <= config.native_head_core_restore_radius**2
    core &= (xx >= core_x0) & (xx < core_x1) & (yy >= core_y0) & (yy < core_y1)
    dark_reference = np.any(reference < background[None, None, :], axis=2)
    if np.any(dark_reference & ~core):
        raise PrototypeProjectionError("prototype head-core restore geometry does not cover every dark reference pixel")
    projected[core] = reference[core]
    return projected


def _chunk(kind: bytes, payload: bytes) -> bytes:
    body = kind + payload
    return struct.pack(">I", len(payload)) + body + struct.pack(">I", zlib.crc32(body) & 0xFFFFFFFF)


def _encode_rgb_png(pixels: np.ndarray) -> bytes:
    height, width, channels = pixels.shape
    if channels != 3 or pixels.dtype != np.uint8:
        raise PrototypeProjectionError("projected pixels must be RGB8")
    scanlines = b"".join(b"\x00" + pixels[row].tobytes(order="C") for row in range(height))
    header = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    return (
        _PNG_SIGNATURE + _chunk(b"IHDR", header) + _chunk(b"IDAT", zlib.compress(scanlines, 9)) + _chunk(b"IEND", b"")
    )


def project_prototype_body(
    candidate: bytes, *, contract: Mapping[str, Any], geometry_guide: bytes
) -> PrototypeProjection:
    """Return the candidate material projected through exact pinned geometry.

    ``source_bbox`` uses half-open source-image coordinates.  The returned PNG
    is always RGB8 at the contract guide dimensions, and its outer pixels,
    silhouette coverage, native antialiasing, and head core come only from the
    pinned guide.
    """

    config = _parse_config(contract)
    canvas = _mapping(contract.get("guide_canvas"), "guide_canvas")
    background = _rgb(canvas.get("background"), "guide_canvas.background")
    body_color = _rgb(canvas.get("body_mask"), "guide_canvas.body_mask")
    transform = _mapping(contract.get("presentation_transform"), "presentation_transform")
    if transform.get("type") != "nearest_neighbor_integer_upscale":
        raise PrototypeProjectionError("unsupported prototype presentation transform")
    scale = _integer(transform.get("scale"), "presentation_transform.scale")
    reference = _decode_native_reference(geometry_guide, contract, scale, background)
    _validate_geometry(contract, config, reference)

    image, transparency = _decode_candidate(
        candidate,
        maximum_pixels=config.max_input_pixels,
        background=background,
    )
    predicted_background = _fit_background(image, config)
    source_bbox = _foreground_bbox(image, transparency, predicted_background, config)
    x0, y0, x1, y1 = source_bbox
    body_x0, body_y0, body_x1, body_y1 = config.native_body_bbox
    material = _bilinear_rgb8(image[y0:y1, x0:x1], body_x1 - body_x0, body_y1 - body_y0)
    native = _project_native(material, reference, config, background, body_color)
    presentation = np.repeat(np.repeat(native, scale, axis=0), scale, axis=1)
    expected_size = (
        _integer(canvas.get("width_px"), "guide_canvas.width_px"),
        _integer(canvas.get("height_px"), "guide_canvas.height_px"),
    )
    if presentation.shape[:2] != (expected_size[1], expected_size[0]):
        raise PrototypeProjectionError("projected presentation dimensions disagree with guide_canvas")
    return PrototypeProjection(
        png_bytes=_encode_rgb_png(presentation),
        source_bbox=source_bbox,
        source_size=(image.shape[1], image.shape[0]),
    )
