#!/usr/bin/env python3
"""Turn a sourced image into a Snaketron sprite sheet, and prove it wraps.

Usage
    python3 client/design/tools/sprite_sheet.py client/design/sprites/*.png
    python3 client/design/tools/sprite_sheet.py IMAGE --rows 20 --name flag
    python3 client/design/tools/sprite_sheet.py IMAGE --dry-run     # measure only
Deps
    pillow, numpy, and — only when a repair is actually needed —
    `pip install simple-lama-inpainting opencv-python-headless`

--------------------------------------------------------------------------
What a sprite sheet is

A square image, read as `rows` rows. **`y` is time and `x` is the body**: one
row is a whole snake's worth of skin, laid out from the head, and playing the
rows in order animates the skin in place. `skin::atlas::AtlasRegion::sheet`
does the reading; this script is what makes the pixels fit to be read.

Two consequences that drive everything below:

- The sheet must wrap in **x** if it will be repeated along a long body, and in
  **y** always — row `n-1` is followed by row `0`, so a discontinuity there is a
  visible jolt once per animation cycle rather than a static seam.
- Rows have to land on whole pixels. A sheet whose height is not divisible by
  its row count samples across row boundaries on every frame, which reads as a
  permanently slightly-blurred skin and looks like bad art.

--------------------------------------------------------------------------
The repair, and why it is a roll rather than a crop

A generated "seamless" image is usually seamless-ish. Finding out where it is
not is awkward while the join sits at the image border, because a border has no
neighbourhood to compare against.

Rolling the image by half its width and half its height moves both wrap joins
to the centre lines — and, crucially, moves genuinely-interior pixels to the
borders, where they wrap correctly *by construction* because they were adjacent
before the roll. So after the roll:

- the two centre lines carry whatever discontinuity the source had, out in the
  open, with ordinary texture either side of them to measure against;
- the borders are already correct and must not be touched.

Repair is then an ordinary inpainting job on a thin slice down each centre line,
and rolling back puts the repaired join at the border where the renderer needs
it. Nothing else in the image moves.
"""
import argparse
import json
import os
import sys

import numpy as np
from PIL import Image

# One row of the shipped sheet, in texels, and how many colours it keeps.
#
# **One cell of body is 16x16 texels, full stop.** A row of the sheet is one
# cell tall, so a twenty-row sheet is 320x320 and that is the whole file. This
# is a deliberate ceiling on the art rather than a compression setting: the
# arena draws cells at 15px and below, so 16 texels is roughly one texel per
# pixel, and the skins read as pixel art rather than as photographs shrunk down.
#
# **The resize happens first, before anything measures or repairs.** That
# ordering is the point. Measuring a 1024px source tells you about a seam that
# will not survive the downsample, and repairing at that size hands LaMa a job
# whose result is mostly thrown away — while the join that actually ships is
# created by the *resize*, after every check has already passed. Downsampling
# first makes the pixels under test the pixels that ship.
CELL = 16
COLORS = 128

DEFAULT_ROWS = 20

# How much of the image the model is asked to invent, across the seam. The
# lesson from `build_coat_textures` holds here: LaMa is a removal model, and a
# wide gap comes back as a blurry desaturated mean. Thin is the whole point.
SLICE_FRACTION = 0.04

# However wide the estimate or the sweep asks for, never more than this. Past
# it LaMa is not repairing a join, it is repainting the sheet.
MAX_SLICE_FRACTION = 0.12

# A seam has to fail **both** of these. The percentile is the primary metric —
# it is the one that works across textures of wildly different busyness — but on
# a very smooth image the interior steps are all tiny and tightly bunched, so an
# utterly ordinary boundary can still rank at 0.91 simply by being a hair above
# its neighbours. Measured on a synthetic probe with a known seam in `x` only:
# the innocent `y` axis ranked 0.906 at a ratio of 0.98, while the real seam
# ranked 1.0 at a ratio of 22.4. Requiring the centre step to actually exceed
# the interior 95th percentile separates the two by an order of magnitude and
# costs nothing on a real seam.
#
# Being wrong in this direction is not free: an unnecessary repair replaces a
# thin slice of real texture with invented pixels.
SEAM_PERCENTILE = 0.90
SEAM_RATIO = 1.0

# After a repair, the same measurement has to come back under these. A seam that
# survives inpainting is not going to be fixed by inpainting it again.
REPAIRED_PERCENTILE = 0.97
REPAIRED_RATIO = 1.5

# Scales at and above which a seam is **structural** rather than a join, and the
# ratio that counts as one. A blend cannot reach this far: averaging sixteen
# pixels into one leaves a stripe's lateral displacement exactly where it was.
#
# This is the check that was missing, and it is a *pre*-repair gate as much as a
# post-repair one. A source whose halves genuinely disagree is not a tileable
# image with a flaw, it is two images — and inpainting it does not fix it, it
# hides it well enough to pass a pixel-scale metric. Measured on the case that
# prompted this: 2.11 at scale 1 rising to 4.05 at scale 8, repaired to a
# beautiful 0.73 at scale 1 while scale 16 still read 2.65.
STRUCTURAL_SCALES = (4, 8, 16)
STRUCTURAL_RATIO = 1.5

# What the width search *aims* for, as against what acceptance *allows*.
#
# A limit and a target are different numbers on purpose. Bisecting straight to
# the limit lands on the narrowest slice that merely scrapes past it — measured
# on the zebra, 22px at 1.42 against a limit of 1.50 — when 128px reached 1.06
# for three hundredths of texture. Sitting on the threshold is precisely how the
# first zebra shipped, so the search targets the tighter number and falls back
# to the limit only if nothing reaches it.
TARGET_RATIO = 1.2

# The join's sideways shift has to be both statistically odd *and* actually
# large. Requiring only the first flags a 3px drift on a texture with no drift
# at all, because the robust spread is then near zero.
ALIGNMENT_MAD = 6.0
ALIGNMENT_FRACTION = 0.015


def steps_along(pixels, axis):
    """Mean absolute neighbour difference along `axis`, per boundary."""
    other = 1 - axis
    return np.abs(np.diff(pixels.astype(np.float64), axis=axis)).mean(
        axis=(other, 2)
    )


def structural_verdict(scales, alignment, extent, ratio=STRUCTURAL_RATIO):
    """Why this axis is unusable, or `None` if it is fine.

    Kept separate from the measuring so the same rule runs before a repair is
    attempted and after one is made. Before, it decides whether repairing is
    even the right idea; after, it decides whether the repair worked or merely
    flattered the metric.
    """
    worst = max(
        (
            (scale, marks["ratio"])
            for scale, marks in scales.items()
            if scale in STRUCTURAL_SCALES
        ),
        key=lambda pair: pair[1],
        default=(0, 0.0),
    )
    if worst[1] >= ratio:
        return (
            f"the halves disagree at scale {worst[0]} ({worst[1]:.2f}x) — a "
            f"structural mismatch no thin fill can repair"
        )
    deviation, lag = alignment
    if deviation >= ALIGNMENT_MAD and abs(lag) >= extent * ALIGNMENT_FRACTION:
        return (
            f"the two sides align best {lag:+d}px apart ({deviation:.1f} MAD from "
            f"the texture's own drift) — the marks do not continue across the join"
        )
    return None


def centre_seam(pixels, axis):
    """How badly the centre line of `axis` stands out, as `(rank, ratio)`.

    `rank` is where the centre boundary falls among all the image's own
    boundaries on that axis, `0..1` — the same percentile the coat pipeline
    reports, and for the same reason: the obvious ratio is not diagnostic across
    textures, because a flat print buries a real seam in its mean while a busy
    one makes an ordinary edge look alarming. `ratio` against the 95th
    percentile is carried alongside purely because it is the number a human can
    picture.
    """
    steps = steps_along(pixels, axis)
    middle = pixels.shape[axis] // 2 - 1
    if middle < 0 or middle >= steps.size:
        return 0.0, 0.0
    centre = steps[middle]
    others = np.delete(steps, middle)
    if others.size == 0:
        return 0.0, 0.0
    rank = float((others < centre).mean())
    ratio = float(centre / max(np.percentile(others, 95), 1e-9))
    return rank, ratio


def downsample(pixels, factor):
    """Box-average by `factor`. The join stays on a boundary at every scale."""
    height, width = pixels.shape[0] // factor, pixels.shape[1] // factor
    return (
        pixels[: height * factor, : width * factor]
        .astype(np.float64)
        .reshape(height, factor, width, factor, -1)
        .mean(axis=(1, 3))
    )


def seam_scales(pixels, axis, scales=(1, 2, 4, 8, 16)):
    """`centre_seam` at a ladder of scales. The gate that was missing.

    A one-pixel step metric measures exactly the boundary an inpainter is about
    to smooth, so it certifies its own repair: LaMa blends a 28px band, the
    adjacent-pixel difference collapses, and the sheet passes while the marks on
    either side still plainly disagree. That is not a tuning error — the metric
    is blind to it by construction, because the defect does not live at one
    pixel.

    Downsampling moves the same measurement up to the scale the defect lives at.
    A blend cannot survive it: averaging sixteen pixels into one leaves the
    lateral displacement of a stripe exactly where it was, and the coarse
    boundary still steps. So a seam that shows only at scale 1 is a genuine
    join to repair, and a seam that persists at scale 8 or 16 is two textures
    that do not belong together.
    """
    out = {}
    for scale in scales:
        small = pixels.astype(np.float64) if scale == 1 else downsample(pixels, scale)
        # Below this there is not enough left to have a distribution.
        if small.shape[axis] < 24:
            break
        rank, ratio = centre_seam(small, axis)
        out[scale] = {"rank": round(rank, 4), "ratio": round(ratio, 3)}
    return out


def alignment_anomaly(pixels, axis, band=12, stride=4):
    """How unlike its neighbours the join's own sideways shift is.

    The direct test for "the stripes do not line up". Collapse a band either
    side of a boundary to a one-dimensional profile and find the circular shift
    that best aligns them; for texture that truly continues, that shift is
    whatever the texture is already doing.

    Measured against the rest of the image rather than against zero, because a
    non-zero shift is *normal*: diagonal stripes are laterally offset from one
    row to the next by definition, and demanding zero would reject every
    diagonal texture ever made. What matters is whether the join's shift is one
    the texture would have produced anyway. Reported in robust deviations (MAD)
    from the interior median, so it is comparable across textures.
    """
    grey = pixels.astype(np.float64) @ np.array([0.2126, 0.7152, 0.0722])
    if axis == 1:
        grey = grey.T
    extent, across = grey.shape
    if extent < band * 4 or across < 16:
        return 0.0, 0

    def shift_at(position):
        before = grey[position - band : position].mean(axis=0)
        after = grey[position : position + band].mean(axis=0)
        before = before - before.mean()
        after = after - after.mean()
        if not np.any(before) or not np.any(after):
            return 0
        # Circular cross-correlation; the texture wraps, so this is exact.
        correlation = np.fft.irfft(
            np.fft.rfft(after) * np.conj(np.fft.rfft(before)), n=across
        )
        lag = int(np.argmax(correlation))
        return lag - across if lag > across // 2 else lag

    centre = extent // 2
    join = shift_at(centre)
    others = [
        shift_at(position)
        for position in range(band, extent - band, stride)
        if abs(position - centre) > band
    ]
    if not others:
        return 0.0, join
    others = np.array(others, dtype=np.float64)
    median = np.median(others)
    spread = np.median(np.abs(others - median))
    # A texture with no lateral drift at all has zero spread, and then any
    # deviation is significant; the floor keeps that from dividing by nothing.
    return float(abs(join - median) / max(spread, 0.5)), join


def correlation_length(pixels, axis, floor=1.0 / np.e):
    """Lag at which the texture stops resembling itself, along `axis`.

    This is the principled first guess at a slice width, and it beats a fixed
    percentage because it is a property of *this* texture. A slice narrower than
    one correlation length cannot bridge a single feature — it is being asked to
    join two halves of a stripe it cannot see the ends of — and a slice much
    wider is throwing away texture the model will have to invent back.

    Computed on the luminance, wrapped, via the autocorrelation theorem: the
    inverse transform of the power spectrum. The image is periodic to begin
    with, which is exactly the assumption an FFT makes, so this is the natural
    way round rather than a shortcut.
    """
    grey = pixels.astype(np.float64) @ np.array([0.2126, 0.7152, 0.0722])
    grey = grey - grey.mean()
    # Correlate along `axis`, averaging over the other one.
    spectrum = np.fft.rfft(grey, axis=axis)
    power = np.fft.irfft((spectrum * np.conj(spectrum)).real, n=grey.shape[axis], axis=axis)
    profile = power.mean(axis=1 - axis)
    if profile[0] <= 0:
        return 8
    profile = profile / profile[0]
    # First lag under the floor, searching only the first half: past that the
    # wrap brings the curve back up and the crossing is an artefact.
    half = max(2, profile.size // 2)
    below = np.nonzero(profile[:half] < floor)[0]
    return int(below[0]) if below.size else half


def band_detail(pixels, axis, start, band):
    """Texture inside the repaired band, over texture outside it.

    The metric that catches LaMa's one failure mode. Asked to invent too much,
    it returns a blurry, desaturated local mean — which fixes the seam perfectly
    and leaves a soft stripe down the sheet that tiles into a recurring blemish.
    Near 1.0 means the fill has the same busyness as its surroundings; much
    below means mush.

    Used as a **floor** rather than a band, deliberately. Values well above 1.0
    do occur — a narrow fill across a violent discontinuity can come back busier
    than its surroundings — but that combination never survives `choose_width`
    anyway, because a slice too narrow to bridge the join has not cleared the
    seam either. Mush is the failure that passes every other check.
    """
    grey = pixels.astype(np.float64) @ np.array([0.2126, 0.7152, 0.0722])
    # Gradient across the seam is the direction the fill has to reconstruct.
    gradient = np.abs(np.diff(grey, axis=axis))
    inside = (
        gradient[start : start + band, :] if axis == 0 else gradient[:, start : start + band]
    )
    mask = np.ones(gradient.shape[axis], dtype=bool)
    mask[start : start + band] = False
    outside = gradient[mask, :] if axis == 0 else gradient[:, mask]
    if outside.size == 0 or outside.mean() <= 0:
        return 1.0
    return float(inside.mean() / outside.mean())


def acceptance(pixels, axis, extent, start, band):
    """The single verdict a repair is judged by, and the numbers behind it.

    Deliberately the *same* rule that judges a source, so "did the repair work"
    and "was this ever tileable" are one question asked twice rather than two
    thresholds that can disagree.
    """
    scales = seam_scales(pixels, axis)
    alignment = alignment_anomaly(pixels, axis)
    rank, ratio = centre_seam(pixels, axis)
    verdict = structural_verdict(scales, alignment, extent)
    fine_ok = rank < REPAIRED_PERCENTILE or ratio < REPAIRED_RATIO
    on_target = (
        structural_verdict(scales, alignment, extent, TARGET_RATIO) is None and fine_ok
    )
    return {
        "cleared": verdict is None and fine_ok,
        "on_target": on_target,
        "why": verdict or (None if fine_ok else f"fine seam still {ratio:.2f}x"),
        "scales": {str(k): v["ratio"] for k, v in scales.items()},
        "rank": round(rank, 3),
        "ratio": round(ratio, 3),
        "detail": round(band_detail(pixels, axis, start, band), 3),
    }


def search_width(lama, image, axis, extent, low, high, tolerance=None, log=print):
    """Bisect for the **narrowest slice that clears**, one LaMa pass at a time.

    Closed loop, not a fixed ladder: each pass decides what to try next, and the
    search stops as soon as the bracket is tight. That is worth doing rather
    than sweeping because the two things being traded move in opposite
    directions and both are monotone in width — measured on a zebra whose
    halves genuinely disagreed:

        width   16    32    64    96   128   192   256   320
        seam  1.63  1.38  1.28  1.12  1.06  1.29  1.10  0.69   (clears from 32)
        detail 0.87  0.84  0.81  0.79  0.80  0.77  0.74  0.73   (falls throughout)

    So "cleared" is a monotone predicate — narrow fails, wide succeeds — which
    is exactly what bisection needs, and the narrowest clearing width is
    *simultaneously* the one that keeps the most real texture. There is no
    trade-off left to weigh once the boundary is found.

    Two early exits, both cheap. The widest allowed slice is tried **first**: if
    that cannot clear the seam, nothing narrower will, and the sheet is rejected
    after one pass instead of six. Then the narrowest is tried, which ends it
    immediately for the common case of an ordinary join.
    """
    # Proportional, because the sheets are now 320px: a fixed 8px bracket is
    # 2.5% of the image and stops the search well short of the boundary.
    tolerance = tolerance or max(2, extent // 80)
    attempts = {}

    def attempt(width):
        width = int(max(8, min(width, high)))
        if width in attempts:
            return attempts[width]
        repaired, band = repair_centre(lama, image, axis, slice_fraction=width / extent)
        pixels = np.asarray(repaired, dtype=np.uint8)
        record = acceptance(pixels, axis, extent, extent // 2 - band // 2, band)
        record.update(width=band, image=repaired)
        attempts[width] = record
        log(
            f"          tried {band:4d}px  seam {record['ratio']:5.2f}  "
            f"detail {record['detail']:.2f}  "
            f"{'cleared' if record['cleared'] else record['why'][:40]}"
        )
        return record

    widest = attempt(high)
    if not widest["cleared"]:
        # Nothing narrower can do better, so stop paying for passes.
        return widest, "nothing cleared, even at the widest slice allowed", attempts

    # Aim for the target; settle for the limit. If even the widest slice cannot
    # reach the target there is no point bisecting on it.
    goal = "on_target" if widest["on_target"] else "cleared"
    narrowest = attempt(low)
    if narrowest[goal]:
        return narrowest, f"{goal.replace('_', ' ')} at the narrowest slice tried", attempts

    # Invariant: `low` fails the goal, `high` meets it. Squeeze until splitting
    # further is not worth another model pass.
    while high - low > tolerance:
        middle = (low + high) // 2
        if attempt(middle)[goal]:
            high = middle
        else:
            low = middle
    why = (
        "narrowest comfortably clear, by bisection"
        if goal == "on_target"
        else "narrowest that cleared at all — the target was out of reach"
    )
    return attempt(high), why, attempts


def roll_half(image):
    """Bring both wrap joins to the centre lines."""
    pixels = np.asarray(image.convert("RGB"), dtype=np.uint8)
    height, width, _ = pixels.shape
    return Image.fromarray(np.roll(pixels, (height // 2, width // 2), axis=(0, 1)))


def unroll_half(image):
    """The inverse. Not the same call: an odd dimension does not round-trip."""
    pixels = np.asarray(image.convert("RGB"), dtype=np.uint8)
    height, width, _ = pixels.shape
    return Image.fromarray(
        np.roll(pixels, (-(height // 2), -(width // 2)), axis=(0, 1))
    )


def repair_centre(lama, image, axis, slice_fraction=SLICE_FRACTION):
    """Inpaint a thin slice down the centre line of `axis`.

    Everything outside the slice is restored byte-for-byte. LaMa returns a
    whole re-rendered image, and keeping its version of the untouched texture
    would quietly soften the entire sheet to fix a one-pixel line.
    """
    pixels = np.asarray(image.convert("RGB"), dtype=np.uint8)
    height, width, _ = pixels.shape
    extent = height if axis == 0 else width
    band = max(8, int(round(extent * slice_fraction)))
    start = extent // 2 - band // 2

    mask = np.zeros((height, width), dtype=np.uint8)
    if axis == 0:
        mask[start : start + band, :] = 255
    else:
        mask[:, start : start + band] = 255

    filled = np.asarray(
        lama(Image.fromarray(pixels), Image.fromarray(mask)), dtype=np.uint8
    )
    # LaMa pads its input up to a multiple of eight and hands the padding back.
    filled = filled[:height, :width]

    out = pixels.copy()
    if axis == 0:
        out[start : start + band, :] = filled[start : start + band, :]
    else:
        out[:, start : start + band] = filled[:, start : start + band]
    return Image.fromarray(out), band


# Cells one mark should span before it reads as a mark rather than as a pixel.
# Below about two, a stripe field turns into a barcode keyed to the grid; much
# above three and an animal's coat looks like wallpaper.
CELLS_PER_MARK = 2.2


def vertical_period(image):
    """The lag at which the source starts repeating itself downwards, in pixels.

    Reported because the sheet's `y` axis is **time**, so its whole height is
    one loop of the animation — and a source that stacks fifteen copies of a
    picture makes that loop fifteen pictures long. The flag showed this
    plainly: one row of twenty spanned 63 source pixels, which is nine stripes
    and part of a canton crammed into a single cell of body *width*. Drawn on a
    snake that reads as the picture wrapped around a cylinder, and playing the
    rows rolls it.

    Returned rather than acted on. A homogeneous texture has a period too — the
    fur grain, eight pixels on both animals here — and cropping to it would be
    absurd. Only the author knows whether a period is a *picture* repeating or
    a weave.
    """
    grey = np.asarray(image.convert("RGB"), dtype=np.float64) @ np.array(
        [0.2126, 0.7152, 0.0722]
    )
    profile = grey.mean(axis=1)
    profile = profile - profile.mean()
    if not np.any(profile):
        return image.height
    spectrum = np.fft.rfft(profile)
    correlation = np.fft.irfft((spectrum * np.conj(spectrum)).real, n=profile.size)
    correlation = correlation / correlation[0]
    low = max(8, profile.size // 200)
    high = max(low + 1, profile.size // 2)
    return low + int(np.argmax(correlation[low:high]))


def crop_to_period(image, period):
    """Keep `period` pixels of height, so the sheet spans exactly one loop."""
    period = int(min(max(period, 8), image.height))
    top = (image.height - period) // 2
    return image.crop((0, top, image.width, top + period))


def frames_from_period(image, rows, cell, cells_long):
    """Build a sheet whose every row is **one whole repeat** of the source.

    For a *picture* that tiles down the image — a flag, not fur — the ordinary
    reading is wrong. The sheet's height is one loop of the animation, so
    spanning fifteen stacked flags puts 0.7 of a flag in every row, and a row is
    one cell of body *width*: the whole picture ends up crammed across the
    snake, which reads as wrapped round a cylinder.

    Here each row is instead one full repeat, sampled at a different phase down
    the source. The source's own stack of copies is already a sequence of wave
    phases, so playing the rows animates the picture rather than scrolling
    through duplicates of it — and each frame is a complete, undistorted flag.

    Rows are as tall as the repeat's real aspect demands, which for a 14.7:1
    flag is a little over one cell. `Fit::Cutout` draws that at authored scale
    and the body clips the slivers.
    """
    period = vertical_period(image)
    width = cells_long * cell
    # Uniform scale in both axes — the whole point is that nothing is squashed.
    scale = width / image.width
    row_height = max(1, int(round(period * scale)))
    sheet = Image.new("RGB", (width, row_height * rows))
    for row in range(rows):
        # Evenly spaced phases through the source's stack of repeats.
        top = int(round(row * (image.height - period) / max(1, rows - 1)))
        window = image.crop((0, top, image.width, top + period))
        sheet.paste(
            window.resize((width, row_height), Image.Resampling.LANCZOS),
            (0, row * row_height),
        )
    return sheet, row_height


def rotate_field(image, degrees):
    """Turn the texture, sampling from a tiling so no corner comes up empty.

    Rotation has to happen to the **pixels**, here, and cannot be a paint-time
    transform. A row is drawn as a one-cell-tall strip along the body, so
    rotating that quad would turn the snake's silhouette rather than the pattern
    inside it. Baking it means the renderer stays unaware, which is also why it
    costs nothing per frame.

    The source is tiled three-by-three before turning, so the corners of the
    result are real texture instead of the empty wedges a plain rotate leaves.
    Tileability does not survive — the lattice no longer lines up with the crop
    — and that is what the repair pass downstream is for.
    """
    if not degrees % 360:
        return image
    wide = Image.new("RGB", (image.width * 3, image.height * 3))
    for row in range(3):
        for column in range(3):
            wide.paste(image, (column * image.width, row * image.height))
    turned = wide.rotate(degrees, resample=Image.Resampling.BICUBIC)
    left, top = image.width, image.height
    return turned.crop((left, top, left + image.width, top + image.height))


def mark_count(image, axis=1):
    """How many marks the source has across `axis`, robustly.

    The number that decides everything downstream, and it is measured rather
    than guessed. Counted as light/dark crossings on many parallel bands and
    then taken as the median, so one atypical band cannot set the scale.
    """
    pixels = np.asarray(image.convert("RGB"), dtype=np.float64) @ np.array(
        [0.2126, 0.7152, 0.0722]
    )
    if axis == 0:
        pixels = pixels.T
    counts = []
    height = pixels.shape[0]
    for start in range(0, height - 8, max(1, height // 24)):
        band = pixels[start : start + 8].mean(axis=0)
        crossings = np.diff((band > band.mean()).astype(int))
        counts.append(np.count_nonzero(crossings) / 2.0)
    return max(1.0, float(np.median(counts)))


def cells_for(image, rows, cell):
    """Row length in cells, so each mark spans `CELLS_PER_MARK` of body.

    **This is the dial, and cropping was the wrong one.** Both the repeat length
    and the mark size come out of it, and neither costs any source: a hide with
    eighteen stripes wants about forty cells, which makes each stripe a bit over
    two cells *and* makes the repeat longer than most snakes. Squeezing the same
    eighteen stripes into twenty cells gives a barcode; cropping to nine stripes
    to fix that throws away half the texture and makes the repeat *more* visible,
    not less, because there is less in it to be unique.

    The source's own aspect is irrelevant here. `y` is time, not space — the
    vertical axis is already being reinterpreted as frames — so the sheet is
    free to be as wide as the art needs.
    """
    wanted = mark_count(image) * CELLS_PER_MARK
    # Whole cells, at least the row count, and never so wide that the file
    # outgrows the coats it sits beside.
    return int(min(max(round(wanted), rows), rows * 4))


def wrapped_resize(image, size, width=None):
    """Resize on a three-by-three tiling and keep the middle.

    Any resample reads a neighbourhood, and at the image's own edges Pillow
    invents one by clamping — which silently undoes the seam this whole script
    exists to create. Filtering a tiling gives every edge pixel the neighbours
    it will actually have. This cost the coat pipeline a seam ratio of 2.7
    versus 1.0 when it was first left out, so it is not theoretical.
    """
    width = width or size
    wide = Image.new("RGB", (image.width * 3, image.height * 3))
    for row in range(3):
        for column in range(3):
            wide.paste(image, (column * image.width, row * image.height))
    out = wide.resize((width * 3, size * 3), Image.Resampling.LANCZOS)
    return out.crop((width, size, width * 2, size * 2))


def give_up(
    report, source, before_rolled, rolled, state_dir, report_dir, name, problems,
    status,
):
    """Log it, save the evidence, and let the batch carry on.

    The state is saved in the *rolled* frame, because handing back the unrolled
    image puts the join at the border again — exactly as hard to see as it was
    before any of this started.
    """
    os.makedirs(state_dir, exist_ok=True)
    rolled.save(os.path.join(state_dir, f"{name}-rolled.png"))
    source.save(os.path.join(state_dir, f"{name}-source.png"))
    if report_dir:
        os.makedirs(report_dir, exist_ok=True)
        report["report"] = render_report(
            name, source, before_rolled, rolled, report,
            os.path.join(report_dir, f"{name}-wrap-check.png"),
        )
    report["status"] = status
    report["problems"] = problems
    with open(os.path.join(state_dir, f"{name}.json"), "w") as handle:
        json.dump(report, handle, indent=2)
    print(f"  REJECTED ({status}) {name}:")
    for problem in problems:
        print(f"    - {problem}")
    print(f"  state saved to {state_dir}; moving on")
    return report


def process(
    path,
    rows,
    out_dir,
    state_dir,
    name=None,
    dry_run=False,
    lama=None,
    report_dir=None,
    axes="xy",
    cell=CELL,
    colors=COLORS,
    resize_first=True,
    cells=None,
    rotate=0.0,
    period=None,
):
    """One image, end to end. Returns a report dict; never raises on a bad seam.

    A sheet that cannot be repaired is **logged, saved, and skipped** rather
    than allowed to fail the batch. A run over a dozen sourced images should
    tell you about all of them at once, not stop at the first bad one.
    """
    name = name or os.path.splitext(os.path.basename(path))[0]
    # Which axes this sheet actually has to wrap on. `y` is never optional —
    # rows are animation frames, so row n-1 is followed by row 0 whatever the
    # sheet is worn as. `x` is required only when the sheet **repeats** along
    # the body: a head-pinned sprite drawn once has no wrap in x to be wrong,
    # and demanding one would reject perfectly good art. The flag is exactly
    # this case — its canton meets its stripes at the horizontal wrap, which is
    # a structural mismatch that does not matter because it is never seen.
    wants = {0} if axes == "y" else {1} if axes == "x" else {0, 1}
    original = Image.open(path).convert("RGB")
    # Down to shipping size **first**, wrap-aware. See `CELL` for why the order
    # matters; the resize is wrap-aware because a plain one invents the edge
    # neighbourhood by clamping, which would either fabricate a join that is not
    # in the source or hide one that is.
    source = rotate_field(original, rotate)
    detected = vertical_period(source)
    cropped = None
    if period:
        source = crop_to_period(source, detected if period == "auto" else int(period))
        cropped = source.height
    cells = cells or cells_for(source, rows, cell)
    width = cells * cell
    source = wrapped_resize(source, rows * cell, width) if resize_first else source
    report = {
        "name": name,
        "source": path,
        "source_size": [original.width, original.height],
        "working_size": [source.width, source.height],
        "rows": rows,
        "rotation_degrees": rotate,
        "vertical_period": detected,
        "cropped_to_period": cropped,
        "repaired": [],
        "status": "ok",
    }

    if original.width != original.height:
        # Not fatal — the renderer derives cells from the aspect either way —
        # but a non-square sheet is almost always a sourcing mistake, and it
        # silently changes how many cells of body one row covers.
        report["warnings"] = [
            f"{original.width}x{original.height} is not square, so one row is "
            f"{original.width / original.height:.1f} cells rather than {rows}"
        ]

    rolled = roll_half(source)
    before_rolled = rolled
    axes = {0: "y (rows / time)", 1: "x (body length)"}
    report["before"] = {}
    report["estimate"] = {}
    pixels = np.asarray(rolled, dtype=np.uint8)
    report["scales"] = {}
    report["alignment"] = {}
    structural = []
    for axis, label in axes.items():
        rank, ratio = centre_seam(pixels, axis)
        report["before"][label] = {"rank": round(rank, 4), "ratio": round(ratio, 3)}
        extent = source.height if axis == 0 else source.width
        scales = seam_scales(pixels, axis)
        alignment = alignment_anomaly(pixels, axis)
        report["scales"][label] = {str(k): v["ratio"] for k, v in scales.items()}
        report["alignment"][label] = {
            "mad": round(alignment[0], 2),
            "lag": alignment[1],
        }
        verdict = structural_verdict(scales, alignment, extent)
        if verdict and axis in wants:
            # Not a rejection. A disagreement at scale needs a *wider* fill —
            # wide enough for the model to redraw the region rather than blend
            # across it — and refusing to try was this gate's own bug. Measured
            # on the zebra it first rejected: 16px left it broken, 32px cleared
            # it, and the pre-repair veto meant no width was ever attempted.
            structural.append(f"{label}: {verdict}")
        elif verdict:
            report.setdefault("ignored", []).append(f"{label}: {verdict}")
        # The width to try first, from the texture rather than from a constant.
        estimate = correlation_length(pixels, axis)
        report["estimate"][label] = int(
            min(max(estimate, 8), round(extent * MAX_SLICE_FRACTION))
        )

    if dry_run:
        report["structural"] = structural
        return report

    report["structural"] = structural

    for axis, label in axes.items():
        marks = report["before"][label]
        if axis not in wants:
            continue
        needs_repair = marks["rank"] >= SEAM_PERCENTILE and marks["ratio"] >= SEAM_RATIO
        structural_here = any(problem.startswith(label) for problem in structural)
        if not needs_repair and not structural_here:
            continue
        if lama is None:
            lama = load_lama()
        extent = source.height if axis == 0 else source.width
        estimate = report["estimate"][label]
        # A join needs about a correlation length. A structural disagreement
        # needs whatever it needs, so the bracket opens all the way to the cap
        # and the search finds the boundary itself.
        low = 8 if structural_here else max(8, estimate // 3)
        high = int(extent * MAX_SLICE_FRACTION)
        print(f"      searching {label} in {low}..{high}px:")
        best, why, attempts = search_width(lama, rolled, axis, extent, low, high)
        report.setdefault("searches", {})[label] = {
            "why": why,
            "chosen": best["width"],
            "passes": len(attempts),
            "tried": sorted(
                (
                    {
                        key: record[key]
                        for key in ("width", "ratio", "detail", "cleared")
                    }
                    for record in attempts.values()
                ),
                key=lambda record: record["width"],
            ),
        }
        if not best["cleared"]:
            return give_up(
                report, source, before_rolled, rolled, state_dir, report_dir, name,
                [f"{label}: {why} ({best['why']})"], status="not-tileable",
            )
        rolled, band = best["image"], best["width"]
        report["repaired"].append({"axis": label, "band_px": band})

    report["after"] = {}
    unresolved = []
    repaired = np.asarray(rolled, dtype=np.uint8)
    for axis, label in axes.items():
        rank, ratio = centre_seam(repaired, axis)
        report["after"][label] = {"rank": round(rank, 4), "ratio": round(ratio, 3)}
        extent = source.height if axis == 0 else source.width
        scales = seam_scales(repaired, axis)
        alignment = alignment_anomaly(repaired, axis)
        report["scales"][label] = {str(k): v["ratio"] for k, v in scales.items()}
        report["alignment"][label] = {
            "mad": round(alignment[0], 2),
            "lag": alignment[1],
        }
        if axis not in wants:
            continue
        if rank >= REPAIRED_PERCENTILE and ratio >= REPAIRED_RATIO:
            unresolved.append(f"{label} still ranks {rank:.2f} at {ratio:.1f}x")
        # The same rule that would have rejected the source. A fill that closed
        # the pixel-scale join without bringing the halves into agreement is
        # exactly the failure this whole check exists for.
        verdict = structural_verdict(scales, alignment, extent)
        if verdict:
            unresolved.append(f"{label} after repair: {verdict}")

    if unresolved:
        return give_up(
            report, source, before_rolled, rolled, state_dir, report_dir, name,
            unresolved, status="unresolved",
        )

    if report_dir:
        os.makedirs(report_dir, exist_ok=True)
        report["report"] = render_report(
            name,
            source,
            before_rolled,
            rolled,
            report,
            os.path.join(report_dir, f"{name}-wrap-check.png"),
        )

    sheet = unroll_half(rolled)
    if not resize_first:
        sheet = wrapped_resize(sheet, rows * cell, width)
    if colors:
        sheet = sheet.quantize(colors=colors, dither=Image.Dither.FLOYDSTEINBERG)
    os.makedirs(out_dir, exist_ok=True)
    destination = os.path.join(out_dir, f"{name}.v1.png")
    sheet.save(destination, optimize=True)
    report["output"] = destination
    report["output_size"] = [sheet.width, sheet.height]
    report["repeat_cells"] = round(sheet.width / cell, 2)
    return report


def wrap_view(image, size=300):
    """The image tiled two-by-two, so both joins meet in the middle.

    Deliberately unannotated. A line drawn on the join hides the thing it is
    pointing at, and a join is only ever judged by whether the marks carry
    across it — so the numbers go in the caption and the picture is left alone.
    """
    two = Image.new("RGB", (image.width * 2, image.height * 2))
    for row in range(2):
        for column in range(2):
            two.paste(image, (column * image.width, row * image.height))
    return two.resize((size, size), Image.Resampling.LANCZOS)


def join_strip(rolled, axis, across=140, along=520, height=110):
    """A close crop straddling one join, in rolled space where it is central.

    The most diagnostic view there is: at this magnification a seam is a line
    you cannot miss, and its absence is equally unmistakable.
    """
    pixels = np.asarray(rolled.convert("RGB"), dtype=np.uint8)
    tall, wide, _ = pixels.shape
    if axis == 1:
        left = max(0, wide // 2 - across // 2)
        top = max(0, tall // 2 - along // 2)
        crop = pixels[top : top + along, left : left + across]
        crop = np.rot90(crop)  # lay the join horizontally so it reads as a strip
    else:
        top = max(0, tall // 2 - across // 2)
        left = max(0, wide // 2 - along // 2)
        crop = pixels[top : top + across, left : left + along]
    view = Image.fromarray(np.ascontiguousarray(crop))
    return view.resize(
        (along, height), Image.Resampling.NEAREST if view.width < along else Image.Resampling.LANCZOS
    )


def render_report(name, source, before_rolled, after_rolled, report, path):
    """Before and after, at two magnifications, with the numbers in the text."""
    from PIL import ImageDraw

    pad, gap = 10, 24
    # Only show an "after" when something was actually done. A sheet rejected
    # before any repair has no after, and rendering one anyway shows the same
    # picture twice under a caption of zeroes — which reads as a repair that
    # perfected the seam rather than as a repair that never ran.
    panels = [("before", before_rolled, report.get("before", {}))]
    if report.get("after"):
        panels.append(("after", after_rolled, report["after"]))
    strips = []
    for label, rolled, marks in panels:
        for axis, axis_label in ((0, "y (rows / time)"), (1, "x (body length)")):
            scores = marks.get(axis_label, {})
            strips.append(
                (
                    f"{name} {label} — {axis_label}: rank {scores.get('rank', 0):.2f} "
                    f"at {scores.get('ratio', 0):.2f}x",
                    join_strip(rolled, axis),
                )
            )

    width = max(520 + pad * 2, 300 * len(panels) + pad * (len(panels) + 1))
    height = pad + 14 + 300 + gap + len(strips) * (110 + gap) + pad
    canvas = Image.new("RGB", (width, height), (255, 255, 255))
    draw = ImageDraw.Draw(canvas)

    estimate = report.get("estimate", {})
    draw.text(
        (pad, pad),
        f"{name} — tiled 2x2 (both joins meet in the centre); "
        f"estimated slice y {estimate.get('y (rows / time)', '-')}px, "
        f"x {estimate.get('x (body length)', '-')}px",
        fill=(0, 0, 0),
    )
    y = pad + 16
    canvas.paste(wrap_view(unroll_half(before_rolled)), (pad, y))
    if len(panels) > 1:
        canvas.paste(wrap_view(unroll_half(after_rolled)), (pad * 2 + 300, y))
    y += 300 + gap

    for label, strip in strips:
        draw.text((pad, y - 14), label, fill=(0, 0, 0))
        canvas.paste(strip, (pad, y))
        y += strip.height + gap

    canvas.save(path)
    return path


def load_lama():
    from simple_lama_inpainting import SimpleLama

    return SimpleLama()


def describe(report):
    line = f"  {report['name']}: {report['source_size'][0]}x{report['source_size'][1]}"
    for label, scales in report.get("scales", {}).items():
        ladder = "  ".join(f"s{s}:{r:5.2f}" for s, r in scales.items())
        align = report.get("alignment", {}).get(label, {})
        line += (
            f"\n      {label:20s} {ladder}   align {align.get('mad', 0):.1f} MAD "
            f"(lag {align.get('lag', 0):+d}px)"
        )
    if report.get("vertical_period"):
        line += (
            f"\n      source repeats every {report['vertical_period']}px down"
            f"{' — cropped to one' if report.get('cropped_to_period') else ''}"
        )
    if report.get("rotation_degrees"):
        line += f"\n      rotated {report['rotation_degrees']:g} degrees before measuring"
    for note in report.get("ignored", []):
        line += f"\n      not required to wrap here — {note}"
    for label, width in report.get("estimate", {}).items():
        line += f"\n      estimated slice for {label}: {width}px"
    for label, search in report.get("searches", {}).items():
        line += (
            f"\n      {label}: {search['chosen']}px in {search['passes']} passes "
            f"({search['why']})"
        )
    for repair in report["repaired"]:
        line += f"\n      repaired {repair['axis']} over {repair['band_px']}px"
    for warning in report.get("warnings", []):
        line += f"\n      note: {warning}"
    if report.get("report"):
        line += f"\n      wrap check {report['report']}"
    if report.get("output"):
        size = report["output_size"]
        line += (
            f"\n      wrote {report['output']} ({size[0]}x{size[1]}, "
            f"repeat {report['repeat_cells']:g} cells)"
        )
    return line


if __name__ == "__main__":
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    parser = argparse.ArgumentParser()
    parser.add_argument("images", nargs="+")
    parser.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    parser.add_argument("--cell", type=int, default=CELL, help="texels per row")
    parser.add_argument(
        "--period",
        default=None,
        help="crop the source's height to one loop: 'auto', or a pixel count. "
        "Use when the source stacks copies of a picture; leave off for a "
        "homogeneous texture, whose period is just its grain",
    )
    parser.add_argument(
        "--rotate",
        type=float,
        default=0.0,
        help="turn the texture by this many degrees before anything else",
    )
    parser.add_argument(
        "--cells",
        type=int,
        default=None,
        help="row length in cells (the repeat). Default: measured from the "
        "source so each mark spans about 2.2 cells",
    )
    parser.add_argument(
        "--colors", type=int, default=COLORS, help="palette size; 0 keeps full RGB"
    )
    parser.add_argument("--name", default=None, help="output name for a single image")
    parser.add_argument(
        "--out-dir", default=os.path.join(root, "web", "public", "images", "skins")
    )
    parser.add_argument(
        "--state-dir",
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), ".sprite-state"),
        help="where unrepairable sheets are saved for inspection",
    )
    parser.add_argument("--dry-run", action="store_true", help="measure, write nothing")
    parser.add_argument(
        "--axes",
        default="xy",
        choices=("xy", "y", "x"),
        help="axes that must wrap. 'y' for a sprite drawn once, which never "
        "wraps along the body (default: xy, for a repeating coat)",
    )
    parser.add_argument(
        "--report-dir", default=None, help="write a before/after wrap check here"
    )
    args = parser.parse_args()

    if args.name and len(args.images) > 1:
        parser.error("--name applies to a single image")

    lama = None
    reports = []
    for path in args.images:
        print(f"\n{path}")
        report = process(
            path,
            rows=args.rows,
            out_dir=args.out_dir,
            state_dir=args.state_dir,
            name=args.name,
            dry_run=args.dry_run,
            lama=lama,
            report_dir=args.report_dir,
            axes=args.axes,
            cell=args.cell,
            colors=args.colors,
            cells=args.cells,
            rotate=args.rotate,
            period=args.period,
        )
        reports.append(report)
        print(describe(report))

    bad = [report for report in reports if report["status"] != "ok"]
    verb = "measured" if args.dry_run else "ready"
    print(f"\n{len(reports) - len(bad)}/{len(reports)} sheets {verb}")
    if bad:
        print(f"unresolved: {', '.join(report['name'] for report in bad)}")
        print(f"state in {args.state_dir}")
    # A bad sheet is reported, not fatal: the batch still produced the others.
    sys.exit(0)
