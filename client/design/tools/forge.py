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

    python forge.py --kind sheet --body-columns 64 --frame-rows 64 --axes y \
      --in generated.png --out-dir out/
    python forge.py --kind coat --body-columns 12 --axes x \
      --in coat.png --out-dir out/ --report

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

AXIS_NAMES = {0: "vertical", 1: "horizontal"}
AXIS_IDS = {"x": 1, "y": 0}


def parse_axes(value: str) -> tuple[int, ...]:
    """Parse the explicit, usage-derived seam axes from the CLI."""
    value = value.strip().lower()
    if value == "none":
        return ()

    names = [name.strip() for name in value.split(",")]
    if not names or any(not name for name in names):
        raise argparse.ArgumentTypeError("--axes must be x, y, x,y, or none")
    unknown = [name for name in names if name not in AXIS_IDS]
    if unknown:
        raise argparse.ArgumentTypeError(
            f"unknown seam axis {unknown[0]!r}; use x, y, x,y, or none"
        )
    if len(set(names)) != len(names):
        raise argparse.ArgumentTypeError("each seam axis may be named only once")
    return tuple(AXIS_IDS[name] for name in names)


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
    body_columns: int | None
    frame_rows: int | None
    seam_axes: list[str]
    horizontal_ratio: float
    vertical_ratio: float
    repaired: bool
    repair_methods: list[str] = field(default_factory=list)
    rungs: list[Rung] = field(default_factory=list)
    # A refusal after repair must not discard the exact pixels that were
    # measured.  This is deliberately separate from ``rungs``: it is evidence,
    # not a shippable ladder member.
    failed_output: Rung | None = None
    # Present only on a refusal, and written to be read by a person: which
    # axis, by how much, and against what limit.
    rejection: str | None = None


@dataclass(frozen=True)
class Measurement:
    ratio: float
    rank: float
    cleared: bool
    complaint: str | None
    structural: str | None


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def measure(image: Image.Image, axis: int) -> Measurement:
    """The forge's single seam verdict for one axis.

    ``acceptance`` wants a band to look at; the correlation length is the same
    first guess ``process`` uses, so a texture measured here and a texture
    measured by the shipping pipeline are measured the same way.
    """
    # RGB, not luminance: every measurement in `sprite_sheet` does its own
    # luminance conversion, so handing it a pre-converted plane both fails to
    # broadcast and would have measured something subtly different if it had.
    if "A" in image.getbands():
        rgba = np.asarray(image.convert("RGBA"), dtype=np.float64)
        alpha = rgba[..., 3:4] / 255.0
        # Measure what the overlay contributes on screen, plus the alpha edge
        # itself. Transparent RGB garbage must not dominate, while an opacity
        # discontinuity must not disappear from the gate.
        pixels = np.concatenate((rgba[..., :3] * alpha, rgba[..., 3:4]), axis=2).astype(
            np.uint8
        )
    else:
        pixels = np.asarray(image.convert("RGB"), dtype=np.uint8)

    extent = pixels.shape[axis]
    band = max(8, min(sheets.correlation_length(pixels, axis), extent // 8 or 8))
    verdict = sheets.acceptance(pixels, axis, extent, 0, band)
    structural = sheets.structural_verdict(
        sheets.seam_scales(pixels, axis),
        sheets.alignment_anomaly(pixels, axis),
        extent,
    )
    return Measurement(
        ratio=float(verdict["ratio"]),
        rank=float(verdict["rank"]),
        cleared=bool(verdict["cleared"]),
        complaint=verdict.get("why"),
        structural=structural,
    )


def repair(image: Image.Image, axis: int, lama) -> tuple[Image.Image, bool]:
    """Move the join to the middle, inpaint a thin slice, put it back.

    Deliberately not a cross-fade of the two ends: that averages two different
    pieces of texture into a ghost which then repeats as a visible blotch every
    tile. It reads as a smudge rather than a seam, which is exactly why it
    survives review.
    """
    rolled = sheets.roll_half(image)
    repaired, _band = sheets.repair_centre(lama, rolled, axis)
    result = sheets.unroll_half(repaired)
    if "A" in image.getbands():
        result.putalpha(image.getchannel("A"))
    return result, True


def repair_crop_tx_t(
    image: Image.Image,
    axis: int,
    lama,
    gap_fraction: float = 0.15,
) -> Image.Image:
    """Bridge crop ends as ``[T, X, T]``, keep ``[T, X]``, then fit back.

    Unlike a thin rolled repair, this is appropriate when a provider image was
    cropped to the requested grid and its ends were never neighbors. Both
    junctions in the retained repeat were therefore painted in their real
    context. The exact T copies are restored before wrap-aware resampling.
    """

    pixels = np.asarray(image.convert("RGB"), dtype=np.uint8)
    working = np.swapaxes(pixels, 0, 1) if axis == 0 else pixels
    height, width, _ = working.shape
    gap = max(8, int(round(width * gap_fraction)))
    canvas = np.empty((height, width * 2 + gap, 3), dtype=np.uint8)
    canvas[:, :width] = working
    canvas[:, width + gap :] = working
    repeats = (gap + width - 1) // width
    canvas[:, width : width + gap] = np.tile(working[:, ::-1], (1, repeats, 1))[:, :gap]
    mask = np.zeros((height, width * 2 + gap), dtype=np.uint8)
    mask[:, width : width + gap] = 255
    filled = np.asarray(lama(Image.fromarray(canvas), Image.fromarray(mask)), dtype=np.uint8)[
        :height, : width * 2 + gap
    ]
    combined = canvas.copy()
    combined[:, width : width + gap] = filled[:, width : width + gap]
    retained = combined[:, : width + gap]
    retained = np.swapaxes(retained, 0, 1) if axis == 0 else retained
    result = sheets.wrapped_resize(
        Image.fromarray(retained),
        image.height,
        image.width,
    )
    if "A" in image.getbands():
        # LaMa is RGB-only. Preserve the original opacity field for rolled
        # repair and bridge this genuinely new gap with a bounded edge blend.
        alpha = np.asarray(image.getchannel("A"), dtype=np.uint8)
        alpha_working = np.swapaxes(alpha, 0, 1) if axis == 0 else alpha
        left = alpha_working[:, -1:].astype(np.float64)
        right = alpha_working[:, :1].astype(np.float64)
        weights = np.arange(1, gap + 1, dtype=np.float64)[None, :] / (gap + 1)
        alpha_gap = np.rint(left * (1.0 - weights) + right * weights).astype(np.uint8)
        alpha_retained = np.concatenate((alpha_working, alpha_gap), axis=1)
        alpha_retained = np.swapaxes(alpha_retained, 0, 1) if axis == 0 else alpha_retained
        alpha_result = sheets.wrapped_resize(
            Image.fromarray(alpha_retained, mode="L"),
            image.height,
            image.width,
        )
        result.putalpha(alpha_result)
    return result


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


def rejected_manifest(
    image: Image.Image,
    kind: str,
    body_columns: int | None,
    frame_rows: int | None,
    seam_axes: tuple[int, ...],
    ratios: dict[int, float],
    repaired: bool,
    rejection: str,
    out_dir: Path,
    repair_methods: list[str] | None = None,
    rungs: list[Rung] | None = None,
) -> Manifest:
    out_dir.mkdir(parents=True, exist_ok=True)
    failed_path = out_dir / "rejected.png"
    image.save(failed_path, format="PNG", optimize=True)
    failed_data = failed_path.read_bytes()
    return Manifest(
        kind=kind,
        accepted=False,
        width_px=image.width,
        height_px=image.height,
        body_columns=body_columns,
        frame_rows=frame_rows,
        seam_axes=["y" if axis == 0 else "x" for axis in seam_axes],
        horizontal_ratio=ratios[1],
        vertical_ratio=ratios[0],
        repaired=repaired,
        repair_methods=list(repair_methods or []),
        rungs=list(rungs or []),
        failed_output=Rung(
            texels_per_cell=LADDERS[kind][0],
            width_px=image.width,
            height_px=image.height,
            bytes=len(failed_data),
            sha256=digest(failed_data),
            path=str(failed_path),
        ),
        rejection=rejection,
    )


def shape_problem(
    image: Image.Image,
    kind: str,
    body_columns: int | None,
    frame_rows: int | None,
    texels_per_cell: int | None = None,
) -> str | None:
    """Validate X body cells and Y animation frames independently."""
    canonical = texels_per_cell or LADDERS[kind][0]
    if body_columns is not None and body_columns <= 0:
        return "body columns must be positive"
    if frame_rows is not None and frame_rows <= 0:
        return "frame rows must be positive"
    if kind in {"coat", "sheet"} and body_columns is None:
        return f"{kind} art must declare body columns"
    if kind == "sheet" and frame_rows is None:
        return "sheet art must declare frame rows"
    if kind != "sheet" and frame_rows is not None:
        return "only sheet art has frame rows"
    if body_columns is not None and image.width != body_columns * canonical:
        return (
            f"{body_columns} body columns at {canonical}px require "
            f"{body_columns * canonical}px width, not {image.width}px"
        )
    if kind == "sheet" and image.height != frame_rows * canonical:
        return (
            f"{frame_rows} frame rows at {canonical}px require "
            f"{frame_rows * canonical}px height, not {image.height}px"
        )
    if kind == "coat" and image.height != canonical:
        return f"a coat is one {canonical}px body cell tall, not {image.height}px"
    if kind == "overlay" and image.height % canonical != 0:
        return f"an overlay height must contain whole {canonical}px cells"
    return None


def inspect_existing(
    image: Image.Image,
    kind: str,
    body_columns: int | None,
    frame_rows: int | None,
    seam_axes: tuple[int, ...],
    texels_per_cell: int,
) -> dict[str, object]:
    """Measure exact retained pixels without repair, resize, or generation."""

    reasons: list[str] = []
    ratios = {0: 0.0, 1: 0.0}
    measurements: list[dict[str, object]] = []
    if texels_per_cell <= 0:
        reasons.append("texels_per_cell must be positive")
    if len(set(seam_axes)) != len(seam_axes) or any(axis not in AXIS_NAMES for axis in seam_axes):
        reasons.append("seam axes must be a unique subset of x and y")
    if kind == "sheet" and 0 not in seam_axes:
        reasons.append("a sheet must check the y seam between its first and last frame")
    problem = shape_problem(image, kind, body_columns, frame_rows, texels_per_cell)
    if problem:
        reasons.append(problem)
    if not reasons:
        for axis in seam_axes:
            measured = measure(image, axis)
            ratios[axis] = measured.ratio
            measurements.append(
                {
                    "axis": "y" if axis == 0 else "x",
                    "ratio": measured.ratio,
                    "rank": measured.rank,
                    "cleared": measured.cleared,
                    "complaint": measured.complaint,
                    "structural": measured.structural,
                }
            )
            if not measured.cleared:
                reasons.append(
                    f"the exact retained {AXIS_NAMES[axis]} join fails at "
                    f"{measured.rank:.2f} percentile / {measured.ratio:.2f}x "
                    f"({measured.complaint or 'not cleared'})"
                )
        if kind == "sheet" and frame_rows:
            temporal = temporal_problem(image, frame_rows)
            if temporal:
                reasons.append(f"the temporal cells are invalid: {temporal}")
    return {
        "schema_version": 1,
        "mode": "inspect_existing",
        "accepted": not reasons,
        "kind": kind,
        "width_px": image.width,
        "height_px": image.height,
        "body_columns": body_columns,
        "frame_rows": frame_rows,
        "texels_per_cell": texels_per_cell,
        "seam_axes": ["y" if axis == 0 else "x" for axis in seam_axes],
        "horizontal_ratio": ratios[1],
        "vertical_ratio": ratios[0],
        "measurements": measurements,
        "reasons": reasons,
    }


def temporal_problem(image: Image.Image, frame_rows: int) -> str | None:
    """Validate the declared 2-D frame cells without inferring their count.

    Frame count is a grid fact established by ``shape_problem``.  A vertical
    mean profile is not a frame-count measurement: horizontal motion preserves
    that mean exactly.  Temporal validation therefore compares the full RGBA
    cells, their last-to-first step, and unwanted translation around the
    one-cell body cross-section.
    """
    frame_height = image.height // frame_rows
    rgba = np.asarray(image.convert("RGBA"), dtype=np.float64)
    alpha = rgba[..., 3:4] / 255.0
    pixels = np.concatenate((rgba[..., :3] * alpha, rgba[..., 3:4]), axis=2)
    frames = pixels.reshape(frame_rows, frame_height, image.width, 4)
    steps = [
        float(np.mean(np.abs(frames[(index + 1) % frame_rows] - frames[index])) / 255.0)
        for index in range(frame_rows)
    ]
    change_floor = 1.0 / 1_024.0
    material = [step for step in steps if step > change_floor]
    if len(material) < 2:
        return "declared frame cells contain no measurable frame-to-frame animation"

    internal = [step for step in steps[:-1] if step > change_floor]
    baseline = float(np.median(internal)) if internal else 0.0
    allowance = max(0.02, baseline * 2.5)
    if steps[-1] > allowance:
        return (
            f"last-to-first frame discontinuity {steps[-1]:.4f} exceeds "
            f"{allowance:.4f}"
        )

    drift = sheets.frame_translation(image, frame_rows)
    drift_limit = max(1, frame_height // 8)
    if abs(drift) > drift_limit:
        return (
            f"frames translate {drift}px around the body cross-section; "
            f"limit is {drift_limit}px"
        )
    return None


def forge(
    image: Image.Image,
    kind: str,
    body_columns: int | None,
    frame_rows: int | None,
    seam_axes: tuple[int, ...],
    out_dir: Path,
) -> Manifest:
    """Measure, repair if needed, measure again, and build the ladder."""
    ratios = {0: 0.0, 1: 0.0}
    repaired = False
    repair_methods: list[str] = []
    lama = None

    if len(set(seam_axes)) != len(seam_axes) or any(
        axis not in AXIS_NAMES for axis in seam_axes
    ):
        return rejected_manifest(
            image,
            kind,
            body_columns,
            frame_rows,
            tuple(axis for axis in seam_axes if axis in AXIS_NAMES),
            ratios,
            False,
            "seam axes must be a unique subset of x and y",
            out_dir,
        )
    if kind == "sheet" and 0 not in seam_axes:
        return rejected_manifest(
            image,
            kind,
            body_columns,
            frame_rows,
            seam_axes,
            ratios,
            False,
            "a sheet must check the y seam between its first and last frame",
            out_dir,
        )

    problem = shape_problem(image, kind, body_columns, frame_rows)
    if problem:
        return rejected_manifest(
            image,
            kind,
            body_columns,
            frame_rows,
            seam_axes,
            ratios,
            False,
            problem,
            out_dir,
        )

    for axis in seam_axes:
        measured = measure(image, axis)
        ratios[axis] = measured.ratio
        if measured.cleared:
            continue

        # A provider crop whose ends never met gets one bounded [T,X,T]
        # attempt. Unlike smoothing the border, this paints both real
        # junctions. It still has to clear the same multi-scale structural gate
        # afterward; otherwise the source is regenerated.
        if measured.structural is not None:
            if lama is None:
                lama = sheets.load_lama()
            image = repair_crop_tx_t(image, axis, lama)
            repaired = True
            repair_methods.append(f"tx_t:{'y' if axis == 0 else 'x'}")
            measured_after = measure(image, axis)
            ratios[axis] = measured_after.ratio
            if measured_after.cleared:
                continue
            return rejected_manifest(
                image,
                kind,
                body_columns,
                frame_rows,
                seam_axes,
                ratios,
                True,
                (
                    f"the {AXIS_NAMES[axis]} join remains structurally wrong after one "
                    f"[T,X,T] repair ({measured.structural}; "
                    f"{measured_after.complaint or 'not cleared'}); regenerate the source"
                ),
                out_dir,
                repair_methods,
            )

        if lama is None:
            lama = sheets.load_lama()
        image, repaired = repair(image, axis, lama)
        repair_methods.append(f"roll:{'y' if axis == 0 else 'x'}")
        measured = measure(image, axis)
        ratios[axis] = measured.ratio

        if not measured.cleared:
            return rejected_manifest(
                image,
                kind,
                body_columns,
                frame_rows,
                seam_axes,
                ratios,
                True,
                (
                    f"the {AXIS_NAMES[axis]} join is still "
                    f"{measured.rank:.2f} percentile / {measured.ratio:.2f}x "
                    f"after repair ({measured.complaint or 'not cleared'})"
                ),
                out_dir,
                repair_methods,
            )

    # Repairing the second axis can disturb the first one near the corners.
    # Recheck the final canonical pixels on every required join rather than
    # accepting the state that happened to exist midway through the loop.
    for axis in seam_axes:
        measured = measure(image, axis)
        ratios[axis] = measured.ratio
        if not measured.cleared:
            return rejected_manifest(
                image,
                kind,
                body_columns,
                frame_rows,
                seam_axes,
                ratios,
                repaired,
                (
                    f"the final {AXIS_NAMES[axis]} join regressed to "
                    f"{measured.rank:.2f} percentile / {measured.ratio:.2f}x "
                    f"({measured.complaint or 'not cleared'})"
                ),
                out_dir,
                repair_methods,
            )

    # Validate the actual declared frame cells.  Frame count was already proven
    # by shape_problem; reconstructing frames from a 1-D vertical profile would
    # misclassify legitimate horizontal motion as a one-frame sheet.
    if kind == "sheet" and frame_rows:
        problem = temporal_problem(image, frame_rows)
        if problem:
            return rejected_manifest(
                image,
                kind,
                body_columns,
                frame_rows,
                seam_axes,
                ratios,
                repaired,
                f"the temporal cells are invalid: {problem}",
                out_dir,
                repair_methods,
            )

    rungs = build_ladder(image, kind, out_dir)
    # The lower rungs are real shipping bytes, not previews. Wrap-aware resize
    # should preserve the join, but the gate verifies that claim on each PNG
    # rather than inferring it from the canonical source.
    for rung in rungs:
        with Image.open(rung.path) as opened:
            exact = opened.convert("RGBA" if kind == "overlay" else "RGB")
        for axis in seam_axes:
            measured = measure(exact, axis)
            if not measured.cleared:
                return rejected_manifest(
                    image,
                    kind,
                    body_columns,
                    frame_rows,
                    seam_axes,
                    ratios,
                    repaired,
                    (
                        f"the shipping {rung.texels_per_cell}-texel rung fails "
                        f"the {AXIS_NAMES[axis]} join at {measured.rank:.2f} "
                        f"percentile / {measured.ratio:.2f}x "
                        f"({measured.complaint or 'not cleared'})"
                    ),
                    out_dir,
                    repair_methods,
                    rungs,
                )

    return Manifest(
        kind=kind,
        accepted=True,
        width_px=image.width,
        height_px=image.height,
        body_columns=body_columns,
        frame_rows=frame_rows,
        seam_axes=["y" if axis == 0 else "x" for axis in seam_axes],
        horizontal_ratio=ratios[1],
        vertical_ratio=ratios[0],
        repaired=repaired,
        repair_methods=repair_methods,
        rungs=rungs,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", required=True, choices=sorted(LADDERS))
    parser.add_argument("--in", dest="source", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument(
        "--body-columns",
        type=int,
        default=None,
        help="number of independent snake-body cells across the image",
    )
    parser.add_argument(
        "--frame-rows",
        type=int,
        default=None,
        help="number of independent animation frames down a sheet",
    )
    parser.add_argument(
        "--axes",
        required=True,
        type=parse_axes,
        metavar="x|y|x,y|none",
        help="joins exposed by the eventual SkinDoc usage",
    )
    parser.add_argument("--report", action="store_true", help="print the manifest")
    parser.add_argument(
        "--inspect-existing",
        action="store_true",
        help="measure the exact input without repair, resampling, or ladder creation",
    )
    parser.add_argument(
        "--texels-per-cell",
        type=int,
        help="exact retained rung density (required by --inspect-existing)",
    )
    args = parser.parse_args()

    if args.kind in {"coat", "sheet"} and args.body_columns is None:
        parser.error(f"--body-columns is required for {args.kind} art")
    if args.kind == "sheet" and args.frame_rows is None:
        parser.error("--frame-rows is required for a sheet")
    if args.kind != "sheet" and args.frame_rows is not None:
        parser.error("--frame-rows is only valid for a sheet")
    if args.kind == "sheet" and 0 not in args.axes:
        parser.error("a sheet must include y in --axes")
    if args.inspect_existing and args.texels_per_cell is None:
        parser.error("--texels-per-cell is required by --inspect-existing")

    image = Image.open(args.source).convert("RGBA" if args.kind == "overlay" else "RGB")
    if args.inspect_existing:
        inspected = inspect_existing(
            image,
            args.kind,
            args.body_columns,
            args.frame_rows,
            args.axes,
            args.texels_per_cell,
        )
        args.out_dir.mkdir(parents=True, exist_ok=True)
        (args.out_dir / "manifest.json").write_text(json.dumps(inspected, indent=2) + "\n")
        if args.report or not inspected["accepted"]:
            print(json.dumps(inspected, indent=2))
        return 0 if inspected["accepted"] else 1
    manifest = forge(
        image,
        args.kind,
        args.body_columns,
        args.frame_rows,
        args.axes,
        args.out_dir,
    )

    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.out_dir / "manifest.json").write_text(
        json.dumps(asdict(manifest), indent=2) + "\n"
    )

    if args.report or not manifest.accepted:
        print(json.dumps(asdict(manifest), indent=2))

    # The exit status is the verdict, so a caller that ignores stdout still
    # knows. The manifest says why either way.
    return 0 if manifest.accepted else 1


if __name__ == "__main__":
    sys.exit(main())
