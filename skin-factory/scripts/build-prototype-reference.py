#!/usr/bin/env python3
"""Build and verify the renderer-owned prototype geometry guide.

The checked-in native fixture is painted by the real browser/WASM skin
renderer at 15 px/cell, the maximum cell size used by the live arena. The
model-facing guide is only a deterministic 4x nearest-neighbour presentation
of those retained pixels; this script never redraws or approximates the snake.

Normal use is read-only::

    python3 skin-factory/scripts/build-prototype-reference.py --check

Maintainers can deliberately refresh the native renderer pixels after a
renderer change. That command rebuilds the WASM package, captures the padded
preview fixture in Chromium, and rewrites both PNGs. It prints the new hashes;
the contract must then be reviewed and updated explicitly before ``--check``
will pass::

    python3 skin-factory/scripts/build-prototype-reference.py --refresh-native
"""

from __future__ import annotations

import argparse
import base64
import functools
import hashlib
import http.server
import json
import os
import socketserver
import struct
import subprocess
import threading
import zlib
from itertools import pairwise
from pathlib import Path
from typing import Any

PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
EXPECTED_CONTRACT_ID = "prototype-geometry-v1"
EXPECTED_GUIDE_PATH = "fixtures/prototype-geometry-guide-v1.png"
EXPECTED_NATIVE_PATH = "fixtures/prototype-geometry-native-v1.png"
EXPECTED_SKIN_PATH = "fixtures/prototype-geometry-blank.skin.json"
EXPECTED_FIXTURE = "prototype_straight_16"
EXPECTED_CELLS = [[16, 1], [1, 1]]
EXPECTED_CANVAS_CELLS = [18, 3]
EXPECTED_LIVE_CELL_SIZES = [5, 10, 15]
EXPECTED_NATIVE_CELL_PX = 15
EXPECTED_PRESENTATION_SCALE = 4
EXPECTED_ROLE = "own"
EXPECTED_HEAD_CORE_RATIO = 0.38
EXPECTED_COLORS = {
    "background": "#666666",
    "body_mask": "#ffffff",
    "system_head_core": "#1c1c1c",
}

# External build/capture tools never need provider, factory, reviewer, worker,
# cloud, or Git credentials. Start from this fixed runtime/build allowlist
# instead of trying to enumerate every possible secret name to remove.
SUBPROCESS_ENVIRONMENT_NAMES = (
    "CARGO_HOME",
    "CARGO_TARGET_DIR",
    "DEVELOPER_DIR",
    "HOME",
    "LANG",
    "LC_ALL",
    "LC_CTYPE",
    "MACOSX_DEPLOYMENT_TARGET",
    "PATH",
    "PLAYWRIGHT_BROWSERS_PATH",
    "RUSTUP_HOME",
    "RUSTUP_TOOLCHAIN",
    "SDKROOT",
    "SSL_CERT_DIR",
    "SSL_CERT_FILE",
    "TEMP",
    "TMP",
    "TMPDIR",
    "TZ",
    "XDG_RUNTIME_DIR",
)


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _subprocess_environment(source: dict[str, str] | os._Environ[str] | None = None) -> dict[str, str]:
    source = os.environ if source is None else source
    return {name: source[name] for name in SUBPROCESS_ENVIRONMENT_NAMES if source.get(name) is not None}


def _chunk(kind: bytes, payload: bytes) -> bytes:
    body = kind + payload
    return struct.pack(">I", len(payload)) + body + struct.pack(">I", zlib.crc32(body) & 0xFFFFFFFF)


def _encode_rgb_png(width: int, height: int, pixels: bytes) -> bytes:
    expected = width * height * 3
    if len(pixels) != expected:
        raise ValueError(f"RGB payload has {len(pixels)} bytes, expected {expected}")
    stride = width * 3
    scanlines = b"".join(b"\x00" + pixels[row * stride : (row + 1) * stride] for row in range(height))
    header = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    return PNG_SIGNATURE + _chunk(b"IHDR", header) + _chunk(b"IDAT", zlib.compress(scanlines, 9)) + _chunk(b"IEND", b"")


def _decode_rgb_png(payload: bytes) -> tuple[int, int, bytes]:
    """Decode the deliberately tiny PNG subset emitted above.

    Keeping this local and strict makes byte-exact checks independent of Pillow
    and refuses a silently re-encoded fixture with a different colour model,
    interlace mode, or row filter.
    """

    if not payload.startswith(PNG_SIGNATURE):
        raise ValueError("fixture is not a PNG")
    cursor = len(PNG_SIGNATURE)
    width = height = None
    compressed = bytearray()
    saw_end = False
    while cursor < len(payload):
        if cursor + 12 > len(payload):
            raise ValueError("PNG chunk is truncated")
        length = struct.unpack(">I", payload[cursor : cursor + 4])[0]
        kind = payload[cursor + 4 : cursor + 8]
        body_start = cursor + 8
        body_end = body_start + length
        crc_end = body_end + 4
        if crc_end > len(payload):
            raise ValueError("PNG chunk payload is truncated")
        body = payload[body_start:body_end]
        expected_crc = struct.unpack(">I", payload[body_end:crc_end])[0]
        actual_crc = zlib.crc32(kind + body) & 0xFFFFFFFF
        if expected_crc != actual_crc:
            raise ValueError(f"PNG {kind!r} CRC does not match")
        if kind == b"IHDR":
            if len(body) != 13:
                raise ValueError("PNG IHDR has the wrong length")
            width, height, depth, color, compression, filtering, interlace = struct.unpack(">IIBBBBB", body)
            if (depth, color, compression, filtering, interlace) != (8, 2, 0, 0, 0):
                raise ValueError("fixture must be non-interlaced 8-bit RGB PNG")
        elif kind == b"IDAT":
            compressed.extend(body)
        elif kind == b"IEND":
            saw_end = True
            if crc_end != len(payload):
                raise ValueError("bytes follow PNG IEND")
        cursor = crc_end
    if width is None or height is None or not compressed or not saw_end:
        raise ValueError("PNG lacks IHDR, IDAT, or IEND")
    rows = zlib.decompress(bytes(compressed))
    stride = width * 3
    if len(rows) != height * (stride + 1):
        raise ValueError("PNG decompressed size does not match IHDR")
    pixels = bytearray()
    for row in range(height):
        start = row * (stride + 1)
        if rows[start] != 0:
            raise ValueError("fixture PNG must use filter 0 on every row")
        pixels.extend(rows[start + 1 : start + 1 + stride])
    return width, height, bytes(pixels)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _contract_path(repo: Path) -> Path:
    return repo / "skin-schema" / "prototype-geometry-v1.json"


def _load_contract(repo: Path) -> tuple[dict[str, Any], bytes]:
    payload = _contract_path(repo).read_bytes()
    try:
        contract = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"prototype geometry contract is not UTF-8 JSON: {error}") from error
    if not isinstance(contract, dict):
        raise ValueError("prototype geometry contract must be an object")
    return contract, payload


def _resolve_schema_path(repo: Path, relative: Any, field: str) -> Path:
    if not isinstance(relative, str) or not relative or Path(relative).is_absolute():
        raise ValueError(f"{field} must be a non-empty relative path")
    root = (repo / "skin-schema").resolve()
    resolved = (root / relative).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as error:
        raise ValueError(f"{field} escapes skin-schema") from error
    return resolved


def _require_equal(actual: Any, expected: Any, field: str) -> None:
    if actual != expected:
        raise ValueError(f"{field} must be {expected!r}, got {actual!r}")


def _require_sha(value: Any, field: str, *, allow_pending: bool) -> None:
    if allow_pending and value == "PENDING":
        return
    if not isinstance(value, str) or len(value) != 64:
        raise ValueError(f"{field} must be a lowercase SHA-256 hex digest")
    if any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{field} must be a lowercase SHA-256 hex digest")


def _validate_contract_facts(
    repo: Path, contract: dict[str, Any], *, allow_pending: bool = False
) -> tuple[Path, Path, Path]:
    _require_equal(contract.get("schema_version"), 1, "schema_version")
    _require_equal(contract.get("id"), EXPECTED_CONTRACT_ID, "id")
    _require_equal(contract.get("guide"), EXPECTED_GUIDE_PATH, "guide")
    _require_equal(
        contract.get("live_cell_sizes_px"),
        EXPECTED_LIVE_CELL_SIZES,
        "live_cell_sizes_px",
    )
    _require_sha(contract.get("guide_sha256"), "guide_sha256", allow_pending=allow_pending)

    source = contract.get("renderer_source")
    if not isinstance(source, dict):
        raise ValueError("renderer_source must be an object")
    _require_equal(source.get("fixture"), EXPECTED_FIXTURE, "renderer_source.fixture")
    _require_equal(
        source.get("compressed_cells_head_first"),
        EXPECTED_CELLS,
        "renderer_source.compressed_cells_head_first",
    )
    _require_equal(source.get("role"), EXPECTED_ROLE, "renderer_source.role")
    _require_equal(
        source.get("native_cell_px"),
        EXPECTED_NATIVE_CELL_PX,
        "renderer_source.native_cell_px",
    )
    _require_equal(
        source.get("canvas_cells"),
        EXPECTED_CANVAS_CELLS,
        "renderer_source.canvas_cells",
    )
    _require_equal(source.get("native_image"), EXPECTED_NATIVE_PATH, "renderer_source.native_image")
    _require_equal(source.get("skin_document"), EXPECTED_SKIN_PATH, "renderer_source.skin_document")
    _require_equal(source.get("head_direction"), "right", "renderer_source.head_direction")
    _require_equal(
        source.get("head_core_ratio"),
        EXPECTED_HEAD_CORE_RATIO,
        "renderer_source.head_core_ratio",
    )
    _require_sha(
        source.get("native_sha256"),
        "renderer_source.native_sha256",
        allow_pending=allow_pending,
    )
    _require_sha(
        source.get("skin_document_sha256"),
        "renderer_source.skin_document_sha256",
        allow_pending=allow_pending,
    )

    occupied_cells = 1
    for headward, tailward in pairwise(EXPECTED_CELLS):
        occupied_cells += abs(headward[0] - tailward[0]) + abs(headward[1] - tailward[1])
    _require_equal(source.get("body_cells"), occupied_cells, "renderer_source.body_cells")
    if EXPECTED_CELLS[0][0] <= EXPECTED_CELLS[-1][0] or EXPECTED_CELLS[0][1] != EXPECTED_CELLS[-1][1]:
        raise ValueError("expected compressed cells do not describe a straight right-facing body")

    native_width = EXPECTED_CANVAS_CELLS[0] * EXPECTED_NATIVE_CELL_PX
    native_height = EXPECTED_CANVAS_CELLS[1] * EXPECTED_NATIVE_CELL_PX
    _require_equal(
        source.get("native_canvas"),
        {"width_px": native_width, "height_px": native_height},
        "renderer_source.native_canvas",
    )

    transform = contract.get("presentation_transform")
    if not isinstance(transform, dict):
        raise ValueError("presentation_transform must be an object")
    _require_equal(
        transform.get("type"),
        "nearest_neighbor_integer_upscale",
        "presentation_transform.type",
    )
    _require_equal(
        transform.get("scale"),
        EXPECTED_PRESENTATION_SCALE,
        "presentation_transform.scale",
    )

    canvas = contract.get("guide_canvas")
    if not isinstance(canvas, dict):
        raise ValueError("guide_canvas must be an object")
    _require_equal(
        (canvas.get("width_px"), canvas.get("height_px")),
        (native_width * EXPECTED_PRESENTATION_SCALE, native_height * EXPECTED_PRESENTATION_SCALE),
        "guide_canvas dimensions",
    )
    for field, expected in EXPECTED_COLORS.items():
        _require_equal(canvas.get(field), expected, f"guide_canvas.{field}")

    guide_path = _resolve_schema_path(repo, contract["guide"], "guide")
    native_path = _resolve_schema_path(repo, source["native_image"], "renderer_source.native_image")
    skin_path = _resolve_schema_path(repo, source["skin_document"], "renderer_source.skin_document")
    return guide_path, native_path, skin_path


def _nearest_upscale(width: int, height: int, pixels: bytes, scale: int) -> bytes:
    stride = width * 3
    output = bytearray()
    for row in range(height):
        source_row = pixels[row * stride : (row + 1) * stride]
        enlarged_row = b"".join(source_row[column : column + 3] * scale for column in range(0, stride, 3))
        for _ in range(scale):
            output.extend(enlarged_row)
    return bytes(output)


def _guide_from_native(contract: dict[str, Any], native_payload: bytes) -> bytes:
    source = contract["renderer_source"]
    width, height, pixels = _decode_rgb_png(native_payload)
    native_canvas = source["native_canvas"]
    _require_equal(
        (width, height),
        (native_canvas["width_px"], native_canvas["height_px"]),
        "native PNG dimensions",
    )
    scale = contract["presentation_transform"]["scale"]
    enlarged = _nearest_upscale(width, height, pixels, scale)
    return _encode_rgb_png(width * scale, height * scale, enlarged)


def _validate_blank_document(payload: bytes, contract: dict[str, Any]) -> None:
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"blank renderer SkinDoc is not UTF-8 JSON: {error}") from error
    _require_equal(document.get("schema_version"), 2, "blank SkinDoc schema_version")
    _require_equal(document.get("head_core"), {"ratio": 0.38, "color": "#1c1c1c"}, "blank SkinDoc head_core")
    _require_equal(document.get("literals", {}).get("blank"), "#ffffff", "blank SkinDoc body mask")
    layers = document.get("layers")
    if not isinstance(layers, list) or len(layers) != 1:
        raise ValueError("blank SkinDoc must contain exactly one body-mask layer")
    layer = layers[0]
    for field, expected in {
        "type": "ribbon",
        "region": "body",
        "color": {"literal": "blank"},
        "extra_px": 0,
        "joints": True,
        "tail_cap": True,
    }.items():
        _require_equal(layer.get(field), expected, f"blank SkinDoc layer.{field}")
    canvas = contract["guide_canvas"]
    _require_equal(canvas["body_mask"], document["literals"]["blank"], "body mask color agreement")
    _require_equal(canvas["system_head_core"], document["head_core"]["color"], "head core color agreement")


def _assert_pixel_facts(contract: dict[str, Any], native_payload: bytes) -> None:
    width, height, pixels = _decode_rgb_png(native_payload)

    def pixel(x: int, y: int) -> tuple[int, int, int]:
        offset = (y * width + x) * 3
        return tuple(pixels[offset : offset + 3])  # type: ignore[return-value]

    background = tuple(bytes.fromhex(contract["guide_canvas"]["background"][1:]))
    body = tuple(bytes.fromhex(contract["guide_canvas"]["body_mask"][1:]))
    core = tuple(bytes.fromhex(contract["guide_canvas"]["system_head_core"][1:]))
    _require_equal(pixel(0, 0), background, "native top-left background pixel")
    _require_equal(pixel(width - 1, height - 1), background, "native bottom-right background pixel")
    # Cell origins are top-left. Tail centre is (1.5, 1.5), head centre is
    # (16.5, 1.5); sampling the lower integer pixel for each half-pixel centre
    # lands well inside the solid renderer fills.
    cell = contract["renderer_source"]["native_cell_px"]
    tail_x = int((EXPECTED_CELLS[-1][0] + 0.5) * cell)
    center_y = int((EXPECTED_CELLS[0][1] + 0.5) * cell)
    head_x = int((EXPECTED_CELLS[0][0] + 0.5) * cell)
    _require_equal(pixel(tail_x, center_y), body, "native tail-centre body pixel")
    _require_equal(pixel(head_x, center_y), core, "native head-centre core pixel")


class _QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, _format: str, *_args: object) -> None:
        return


class _ReusableServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


def _capture_native(repo: Path, contract: dict[str, Any], skin_path: Path) -> bytes:
    subprocess_env = _subprocess_environment()
    subprocess.run(
        ["wasm-pack", "build", "--target", "web", "--out-dir", "pkg"],
        cwd=repo / "client",
        check=True,
        env=subprocess_env,
    )
    handler = functools.partial(_QuietHandler, directory=str(repo))
    with _ReusableServer(("127.0.0.1", 0), handler) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = server.server_address[1]
            helper = repo / "client" / "web" / "tests" / "render-prototype-reference.mjs"
            source = contract["renderer_source"]
            completed = subprocess.run(
                [
                    "node",
                    str(helper),
                    f"http://127.0.0.1:{port}",
                    str(skin_path),
                    source["fixture"],
                    source["role"],
                    str(source["native_cell_px"]),
                    str(source["native_canvas"]["width_px"]),
                    str(source["native_canvas"]["height_px"]),
                    contract["guide_canvas"]["background"],
                ],
                cwd=repo / "client" / "web",
                check=True,
                stdout=subprocess.PIPE,
                text=True,
                env=subprocess_env,
            )
        finally:
            server.shutdown()
            thread.join()
    try:
        result = json.loads(completed.stdout)
        width = int(result["width"])
        height = int(result["height"])
        pixels = base64.b64decode(result["rgb_base64"], validate=True)
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError(f"renderer helper returned malformed pixels: {error}") from error
    expected = source["native_canvas"]
    _require_equal((width, height), (expected["width_px"], expected["height_px"]), "captured dimensions")
    return _encode_rgb_png(width, height, pixels)


def _report_hashes(contract_payload: bytes, skin_payload: bytes, native: bytes, guide: bytes) -> None:
    print(f"contract_sha256={_sha256(contract_payload)}")
    print(f"skin_document_sha256={_sha256(skin_payload)}")
    print(f"native_sha256={_sha256(native)}")
    print(f"guide_sha256={_sha256(guide)}")


def check(repo: Path) -> None:
    contract, contract_payload = _load_contract(repo)
    guide_path, native_path, skin_path = _validate_contract_facts(repo, contract)
    skin_payload = skin_path.read_bytes()
    native_payload = native_path.read_bytes()
    guide_payload = guide_path.read_bytes()
    _validate_blank_document(skin_payload, contract)
    _require_equal(
        _sha256(skin_payload),
        contract["renderer_source"]["skin_document_sha256"],
        "blank SkinDoc SHA-256",
    )
    _require_equal(
        _sha256(native_payload),
        contract["renderer_source"]["native_sha256"],
        "native renderer PNG SHA-256",
    )
    # Our retained native image has one deterministic representation. A PNG
    # optimizer or screenshot re-encoder must not be able to change the bytes
    # while leaving the pixels and contract apparently untouched.
    width, height, pixels = _decode_rgb_png(native_payload)
    _require_equal(
        _encode_rgb_png(width, height, pixels),
        native_payload,
        "native renderer PNG byte representation",
    )
    _assert_pixel_facts(contract, native_payload)
    expected_guide = _guide_from_native(contract, native_payload)
    _require_equal(guide_payload, expected_guide, "checked-in guide bytes")
    _require_equal(_sha256(guide_payload), contract["guide_sha256"], "guide SHA-256")
    _report_hashes(contract_payload, skin_payload, native_payload, guide_payload)


def refresh_native(repo: Path) -> None:
    contract, contract_payload = _load_contract(repo)
    guide_path, native_path, skin_path = _validate_contract_facts(repo, contract, allow_pending=True)
    skin_payload = skin_path.read_bytes()
    _validate_blank_document(skin_payload, contract)
    native_payload = _capture_native(repo, contract, skin_path)
    _assert_pixel_facts(contract, native_payload)
    guide_payload = _guide_from_native(contract, native_payload)
    native_path.parent.mkdir(parents=True, exist_ok=True)
    native_path.write_bytes(native_payload)
    guide_path.parent.mkdir(parents=True, exist_ok=True)
    guide_path.write_bytes(guide_payload)
    _report_hashes(contract_payload, skin_payload, native_payload, guide_payload)


def rebuild_guide(repo: Path) -> None:
    contract, contract_payload = _load_contract(repo)
    guide_path, native_path, skin_path = _validate_contract_facts(repo, contract, allow_pending=True)
    skin_payload = skin_path.read_bytes()
    native_payload = native_path.read_bytes()
    guide_payload = _guide_from_native(contract, native_payload)
    guide_path.write_bytes(guide_payload)
    _report_hashes(contract_payload, skin_payload, native_payload, guide_payload)


def main() -> int:
    parser = argparse.ArgumentParser()
    action = parser.add_mutually_exclusive_group()
    action.add_argument("--check", action="store_true", help="verify all retained bytes and hashes")
    action.add_argument(
        "--refresh-native",
        action="store_true",
        help="rebuild WASM, capture real 15 px/cell renderer pixels, and regenerate the guide",
    )
    args = parser.parse_args()
    repo = _repo_root()
    if args.check:
        check(repo)
    elif args.refresh_native:
        refresh_native(repo)
    else:
        rebuild_guide(repo)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
