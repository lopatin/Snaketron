from __future__ import annotations

import io
import json
from copy import deepcopy
from pathlib import Path

import numpy as np
import pytest
from PIL import Image, ImageDraw, ImageFilter, PngImagePlugin

from snaketron_factory.prototype_projection import (
    PROTOTYPE_PROJECTION_VERSION,
    PrototypeProjectionError,
    project_prototype_body,
)

REPO = Path(__file__).resolve().parents[2]
CONTRACT_PATH = REPO / "skin-schema" / "prototype-geometry-v1.json"
GUIDE_PATH = REPO / "skin-schema" / "fixtures" / "prototype-geometry-guide-v1.png"


def _authority() -> tuple[dict, bytes]:
    return json.loads(CONTRACT_PATH.read_text()), GUIDE_PATH.read_bytes()


def _png(image: Image.Image, *, metadata: str | None = None, compress_level: int = 6) -> bytes:
    stream = io.BytesIO()
    pnginfo = None
    if metadata is not None:
        pnginfo = PngImagePlugin.PngInfo()
        pnginfo.add_text("irrelevant", metadata)
    image.save(stream, "PNG", compress_level=compress_level, pnginfo=pnginfo)
    return stream.getvalue()


def _gradient_noise_shadow_candidate() -> Image.Image:
    height, width = 360, 640
    yy, xx = np.mgrid[:height, :width]
    gradient = 178 + 20 * xx / (width - 1) + 10 * yy / (height - 1) + 6 * (xx / (width - 1)) ** 2
    noise = np.random.default_rng(20_260_821).integers(-5, 6, size=(height, width, 1))
    neutral = np.repeat(np.clip(gradient[:, :, None] + noise, 0, 255).astype(np.uint8), 3, axis=2)
    candidate = Image.fromarray(neutral).convert("RGBA")

    # A deliberately offset, blurred, neutral cast shadow must not become
    # source geometry or survive outside the renderer-owned target mask.
    shadow = Image.new("RGBA", (width, height))
    shadow_draw = ImageDraw.Draw(shadow)
    shadow_draw.rounded_rectangle((87, 148, 573, 226), radius=38, fill=(20, 20, 20, 105))
    candidate = Image.alpha_composite(candidate, shadow.filter(ImageFilter.GaussianBlur(13)))

    body_mask = Image.new("L", (width, height))
    ImageDraw.Draw(body_mask).rounded_rectangle((70, 130, 570, 210), radius=40, fill=255)
    material = Image.new("RGBA", (width, height))
    material_draw = ImageDraw.Draw(material)
    material_draw.rectangle((70, 130, 570, 210), fill=(24, 160, 218, 255))
    for x in range(70, 571, 24):
        material_draw.rectangle((x, 130, min(x + 11, 570), 210), fill=(245, 112, 36, 255))
    material_draw.line((100, 144, 540, 144), fill=(255, 255, 255, 230), width=6)
    material.putalpha(body_mask)
    return Image.alpha_composite(candidate, material).convert("RGB")


def _native(image_bytes: bytes, scale: int) -> np.ndarray:
    with Image.open(io.BytesIO(image_bytes)) as image:
        pixels = np.asarray(image, dtype=np.uint8)
    native = pixels[::scale, ::scale]
    assert np.array_equal(np.repeat(np.repeat(native, scale, axis=0), scale, axis=1), pixels)
    return native


def test_projection_removes_gradient_noise_and_3d_shadow_while_preserving_exact_renderer_geometry() -> None:
    contract, guide = _authority()
    candidate = _gradient_noise_shadow_candidate()
    first = project_prototype_body(
        _png(candidate, metadata="one encoding", compress_level=1),
        contract=contract,
        geometry_guide=guide,
    )
    second = project_prototype_body(
        _png(candidate, metadata="different bytes", compress_level=9),
        contract=contract,
        geometry_guide=guide,
    )

    assert first.version == PROTOTYPE_PROJECTION_VERSION == "prototype-body-mask-v1"
    assert first.png_bytes == second.png_bytes
    assert first.source_size == (640, 360)
    assert first.source_bbox == second.source_bbox == (70, 130, 571, 211)

    scale = contract["presentation_transform"]["scale"]
    projected = _native(first.png_bytes, scale)
    reference = _native(guide, scale)
    background = np.array([102, 102, 102], dtype=np.uint8)
    reference_support = np.any(reference != background, axis=2)

    # No provider background, noise, cast shadow, perspective, or stray pixel
    # can escape the renderer's exact antialiased silhouette.
    assert np.array_equal(np.any(projected != background, axis=2), reference_support)
    assert np.all(projected[~reference_support] == background)

    mapping = contract["prototype_projection"]["mapping"]
    x0, y0, x1, y1 = mapping["native_body_bbox_px"]
    assert (x1 - x0) == 16 * (y1 - y0) == 16 * contract["renderer_source"]["native_cell_px"]
    assert len(np.unique(projected[y0:y1, x0:x1].reshape(-1, 3), axis=0)) > 20
    assert not np.array_equal(projected[y0:y1, x0 : x1 - 15], reference[y0:y1, x0 : x1 - 15])

    core_x0, core_y0, core_x1, core_y1 = mapping["native_head_core_bbox_px"]
    center_x, center_y = mapping["native_head_core_center_px"]
    yy, xx = np.ogrid[: projected.shape[0], : projected.shape[1]]
    core = (xx + 0.5 - center_x) ** 2 + (yy + 0.5 - center_y) ** 2 <= mapping["native_head_core_restore_radius_px"] ** 2
    core &= (xx >= core_x0) & (xx < core_x1) & (yy >= core_y0) & (yy < core_y1)
    assert np.array_equal(projected[core], reference[core])


def test_projection_decodes_realistic_lossy_provider_bytes() -> None:
    contract, guide = _authority()
    candidate = Image.new("RGB", (640, 360), (180, 180, 180))
    ImageDraw.Draw(candidate).rounded_rectangle((70, 130, 570, 210), radius=40, fill=(20, 160, 220))
    stream = io.BytesIO()
    candidate.save(stream, "JPEG", quality=90)

    result = project_prototype_body(stream.getvalue(), contract=contract, geometry_guide=guide)

    assert result.source_bbox == (69, 129, 572, 212)
    with Image.open(io.BytesIO(result.png_bytes)) as projected:
        assert projected.format == "PNG"
        assert projected.mode == "RGB"
        assert projected.size == (1080, 180)


def test_projection_accepts_the_exact_live_gemini_frame_dimensions() -> None:
    contract, guide = _authority()
    candidate = Image.new("RGB", (2752, 1536), (184, 184, 184))
    ImageDraw.Draw(candidate).rounded_rectangle(
        (300, 618, 2452, 918),
        radius=150,
        fill=(28, 148, 220),
    )

    result = project_prototype_body(_png(candidate), contract=contract, geometry_guide=guide)

    assert result.source_size == (2752, 1536)
    assert result.source_bbox == (300, 618, 2453, 919)


def test_projection_supports_a_transparent_candidate_and_a_neutral_dark_skin() -> None:
    contract, guide = _authority()
    transparent = Image.new("RGBA", (400, 240))
    ImageDraw.Draw(transparent).rounded_rectangle((40, 90, 360, 150), radius=30, fill=(32, 28, 40, 255))
    transparent_result = project_prototype_body(
        _png(transparent),
        contract=contract,
        geometry_guide=guide,
    )
    assert transparent_result.source_bbox == (40, 90, 361, 151)

    dark = Image.new("RGB", (400, 240), (190, 190, 190))
    dark_draw = ImageDraw.Draw(dark)
    dark_draw.rounded_rectangle((40, 90, 360, 150), radius=30, fill=(22, 22, 22))
    dark_draw.ellipse((335, 105, 355, 125), fill=(250, 250, 250))
    dark_result = project_prototype_body(_png(dark), contract=contract, geometry_guide=guide)
    assert dark_result.source_bbox == (40, 90, 361, 151)


@pytest.mark.parametrize("case", ["blank", "off_center", "two_subjects"])
def test_projection_fails_closed_when_there_is_not_one_centered_snake(case: str) -> None:
    contract, guide = _authority()
    candidate = Image.new("RGB", (640, 360), (185, 185, 185))
    draw = ImageDraw.Draw(candidate)
    if case == "off_center":
        draw.rounded_rectangle((10, 130, 290, 200), radius=35, fill=(12, 150, 220))
    elif case == "two_subjects":
        draw.rounded_rectangle((70, 80, 570, 140), radius=30, fill=(12, 150, 220))
        draw.rounded_rectangle((70, 220, 570, 280), radius=30, fill=(230, 90, 24))

    with pytest.raises(PrototypeProjectionError):
        project_prototype_body(_png(candidate), contract=contract, geometry_guide=guide)


def test_projection_rejects_unpinned_policy_and_non_nearest_guide_pixels() -> None:
    contract, guide = _authority()
    candidate = _png(_gradient_noise_shadow_candidate())
    unpinned = deepcopy(contract)
    unpinned["prototype_projection"]["version"] = "prototype-body-mask-v2"
    with pytest.raises(PrototypeProjectionError, match=r"prototype_projection\.version"):
        project_prototype_body(candidate, contract=unpinned, geometry_guide=guide)

    with Image.open(io.BytesIO(guide)) as opened:
        altered = np.asarray(opened, dtype=np.uint8).copy()
    altered[0, 1] = (103, 102, 102)
    altered_guide = _png(Image.fromarray(altered))
    with pytest.raises(PrototypeProjectionError, match="not an exact nearest-neighbour"):
        project_prototype_body(candidate, contract=contract, geometry_guide=altered_guide)


@pytest.mark.parametrize("payload", [b"", b"not an image"])
def test_projection_rejects_malformed_candidate_bytes(payload: bytes) -> None:
    contract, guide = _authority()
    with pytest.raises(PrototypeProjectionError):
        project_prototype_body(payload, contract=contract, geometry_guide=guide)
