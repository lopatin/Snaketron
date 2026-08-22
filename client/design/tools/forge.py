"""Turn one generated or uploaded image into a shippable texture, or refuse it
with evidence.

This is the pixel half of texture generation. The server owns the job, the
provider calls and the money; this owns only the question "is this image
actually usable, and if not, exactly how not". Keeping that boundary means the
gates can be exercised on a file from a shell without a queue, a key or a
database, which is how they were developed and how they stay debuggable.

It reuses ``sprite_sheet``'s measurements rather than restating them. Those
numbers were tuned against real art and their subtleties are load-bearing —
in particular ``acceptance`` is the single verdict, and its multi-scale
structural check exists because a one-pixel metric will happily certify its
own repair.

Usage::

    python forge.py --kind sheet --rows 20 --in generated.png --out-dir out/
    python forge.py --kind coat --in coat.png --out-dir out/ --report

Writes ``out/manifest.json`` plus one PNG per ladder rung. Exit status is 0
when the texture is shippable and 1 when it is not; the manifest explains
which, either way, so a caller never has to parse log output.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path

import numpy as np
from PIL import Image

import sprite_sheet as sheets

# Texels per cell in the canonical variant, and the rungs below it. These
# mirror `server/src/texture.rs`; a disagreement would mean the server accepts
# a shape this refuses to build, so they are asserted against each other in
# `test_forge.py` rather than trusted.
LADDERS: dict[str, tuple[int, tuple[int, ...]]] = {
    "coat": (64, (32, 16)),
    "sheet": (16, (8,)),
    "overlay": (64, (32, 16)),
}

# Which axes have to join. A coat repeats along the body, a sheet repeats in
# time, an overlay is drawn once and repeats in neither.
SEAMED_AXES: dict[str, tuple[int, ...]] = {
    # axis 1 is x (along the body), axis 0 is y (time).
    "coat": (1,),
    "sheet": (0,),
    "overlay": (),
}

AXIS_NAMES = {0: "vertical", 1: "horizontal"}


@dataclass
class Rung:
    texels_per_cell: int
    width_px: int
    height_px: int
    bytes: int
    sha256: str
    path: str


@dataclass
class Manifest:
    kind: str
    accepted: bool
    width_px: int
    height_px: int
    rows: int | None
    horizontal_ratio: float
    vertical_ratio: float
    repaired: bool
    rungs: list[Rung] = field(default_factory=list)
    # Present only on a refusal, and written to be read by a person: which
    # axis, by how much, and against what limit.
    rejection: str | None = None


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def measure(image: Image.Image, axis: int) -> tuple[float, str | None]:
    """Ratio for one axis, plus a structural complaint if there is one.

    ``acceptance`` wants a band to look at; the correlation length is the same
    first guess ``process`` uses, so a texture measured here and a texture
    measured by the shipping pipeline are measured the same way.
    """
    # RGB, not luminance: every measurement in `sprite_sheet` does its own
    # luminance conversion, so handing it a pre-converted plane both fails to
    # broadcast and would have measured something subtly different if it had.
    pixels = np.asarray(image.convert("RGB"), dtype=np.uint8)

    extent = pixels.shape[axis]
    band = max(8, min(sheets.correlation_length(pixels, axis), extent // 8 or 8))
    verdict = sheets.acceptance(pixels, axis, extent, 0, band)
    return float(verdict["ratio"]), verdict.get("why")


def repair(image: Image.Image, axis: int, lama) -> tuple[Image.Image, bool]:
    """Move the join to the middle, inpaint a thin slice, put it back.

    Deliberately not a cross-fade of the two ends: that averages two different
    pieces of texture into a ghost which then repeats as a visible blotch every
    tile. It reads as a smudge rather than a seam, which is exactly why it
    survives review.
    """
    rolled = sheets.roll_half(image)
    repaired, _band = sheets.repair_centre(lama, rolled, axis)
    return sheets.unroll_half(repaired), True


def build_ladder(image: Image.Image, kind: str, out_dir: Path) -> list[Rung]:
    """Write the canonical image and every rung beneath it.

    Resizing is wrap-aware — a plain resize samples past the edge and puts a
    soft join into art that had none, which would undo the repair one step
    earlier at exactly the sizes players see most.
    """
    canonical, rungs_below = LADDERS[kind]
    out_dir.mkdir(parents=True, exist_ok=True)
    rungs: list[Rung] = []

    for texels in (canonical, *rungs_below):
        if texels == canonical:
            scaled = image
        else:
            scale = texels / canonical
            # `wrapped_resize` takes height then width as scalars, and resizes
            # on a 3x3 tiling so the edges keep the neighbours they will
            # actually have — a plain resize clamps at the border and puts back
            # the very seam the repair removed.
            scaled = sheets.wrapped_resize(
                image,
                max(1, round(image.height * scale)),
                max(1, round(image.width * scale)),
            )

        path = out_dir / f"{texels}.png"
        scaled.save(path, format="PNG", optimize=True)
        data = path.read_bytes()
        rungs.append(
            Rung(
                texels_per_cell=texels,
                width_px=scaled.width,
                height_px=scaled.height,
                bytes=len(data),
                sha256=digest(data),
                path=str(path),
            )
        )
    return rungs


def forge(image: Image.Image, kind: str, rows: int | None, out_dir: Path) -> Manifest:
    """Measure, repair if needed, measure again, and build the ladder."""
    ratios = {0: 0.0, 1: 0.0}
    repaired = False
    lama = None

    for axis in SEAMED_AXES[kind]:
        ratio, structural = measure(image, axis)
        if ratio <= sheets.REPAIRED_RATIO and structural is None:
            ratios[axis] = ratio
            continue

        # A structurally disagreeing source is re-generated, never
        # force-repaired: inpainting a join between two pieces of texture that
        # were never meant to meet produces mush, and mush passes a one-pixel
        # metric.
        if structural is not None:
            return Manifest(
                kind=kind,
                accepted=False,
                width_px=image.width,
                height_px=image.height,
                rows=rows,
                horizontal_ratio=ratios[1],
                vertical_ratio=ratios[0],
                repaired=False,
                rejection=(
                    f"the {AXIS_NAMES[axis]} join is structurally wrong ({structural}); "
                    "this needs generating again rather than repairing"
                ),
            )

        if lama is None:
            lama = sheets.load_lama()
        image, repaired = repair(image, axis, lama)
        ratio, structural = measure(image, axis)
        ratios[axis] = ratio

        if ratio > sheets.REPAIRED_RATIO or structural is not None:
            return Manifest(
                kind=kind,
                accepted=False,
                width_px=image.width,
                height_px=image.height,
                rows=rows,
                horizontal_ratio=ratios[1],
                vertical_ratio=ratios[0],
                repaired=True,
                rejection=(
                    f"the {AXIS_NAMES[axis]} join is still {ratio:.2f}x after repair, "
                    f"and the limit is {sheets.REPAIRED_RATIO:.2f}x"
                ),
            )

    # A sheet whose frames slide reads as the snake rotating rather than the
    # pattern animating, which is why the shipping tooling makes it a build
    # error rather than a warning.
    if kind == "sheet" and rows:
        try:
            sheets.frames_from_period(image, rows, sheets.CELL, image.width // sheets.CELL)
        except ValueError as error:
            return Manifest(
                kind=kind,
                accepted=False,
                width_px=image.width,
                height_px=image.height,
                rows=rows,
                horizontal_ratio=ratios[1],
                vertical_ratio=ratios[0],
                repaired=repaired,
                rejection=f"the frames travel rather than animating in place: {error}",
            )

    return Manifest(
        kind=kind,
        accepted=True,
        width_px=image.width,
        height_px=image.height,
        rows=rows,
        horizontal_ratio=ratios[1],
        vertical_ratio=ratios[0],
        repaired=repaired,
        rungs=build_ladder(image, kind, out_dir),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", required=True, choices=sorted(LADDERS))
    parser.add_argument("--in", dest="source", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--rows", type=int, default=None)
    parser.add_argument("--report", action="store_true", help="print the manifest")
    args = parser.parse_args()

    if args.kind == "sheet" and not args.rows:
        parser.error("--rows is required for a sheet")

    image = Image.open(args.source).convert("RGBA" if args.kind == "overlay" else "RGB")
    manifest = forge(image, args.kind, args.rows, args.out_dir)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.out_dir / "manifest.json").write_text(json.dumps(asdict(manifest), indent=2))

    if args.report or not manifest.accepted:
        print(json.dumps(asdict(manifest), indent=2))

    # The exit status is the verdict, so a caller that ignores stdout still
    # knows. The manifest says why either way.
    return 0 if manifest.accepted else 1


if __name__ == "__main__":
    sys.exit(main())
