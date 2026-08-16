#!/usr/bin/env python3
"""Build the tiling coat textures for the animal skin family.

Writes  client/web/public/images/skins/{zebra,tiger,jaguar}[-print].v1.png
Usage   python3 client/design/tools/build_coat_textures.py [--preview DIR]
Deps    pillow, numpy, torch, simple-lama-inpainting, opencv-python-headless
        (`pip install simple-lama-inpainting opencv-python-headless`; install
        it with `--no-deps` if pip tries to build an ancient Pillow), plus
        network access on the first run for the sources and the LaMa weights.

Never hand-edit the PNGs. They are build output, like the rank icons, and this
file plus the three third-party sources are the source. Licensing and
attribution live in `client/web/THIRD_PARTY_ASSETS.md`; the one CC-BY source
there is a real obligation, not a courtesy.

--------------------------------------------------------------------------
Where the coats come from

**Game textures, not wildlife photographs.** Two rounds were tried and thrown
away before this one, and both failures are worth recording because they are
the two obvious things to try:

- *Drawing the patterns procedurally* produces regular, evenly-weighted marks
  that read as scratches on a snake rather than as an animal. The irregularity
  that makes a coat look like a coat is exactly what a generator has to invent
  and cannot.
- *Cropping a wildlife photograph* produces something real but muddy: shot
  under directional light, in an animal's own local colour, at a contrast
  nothing on a white arena at fifteen pixels a cell can survive.

What works is art already made to be a texture — flat-lit, saturated, tiling.
All three come from OpenGameArt. Originals are fetched once into a local cache
(not committed) and only the finished strips ship.

--------------------------------------------------------------------------
Making it tile: the [T, X, T] trick

A crop out of a texture does not tile. Its right edge meets its left edge at
whatever those two happen to be, and on a snake that shows up as a hard break
or — worse, because it is easy to miss in a still — a *repeating blotch* every
few cells.

Cross-fading the two ends together was the first fix and it is a bad one: the
overlap is a ghost of two different pieces of coat averaged together, and it
tiles that ghost. It shipped, and it is what the zebra's visible blemish was.

The fix that works is to **let an inpainting model paint the join**:

    [T, X, T]   put a gap between two copies of the tile and inpaint it
    [T, X]      keep one copy and the gap; that pair is the new tile

Tiling `[T, X]` gives `… T X T X T …`, and every junction in that sequence —
`T→X` and `X→T` — is one the model generated the gap against. So the join is
seamless by construction rather than by blending. `T` is kept byte-identical to
what the model saw, which is what makes the guarantee hold.

**The gap is narrow, and the length comes from the source.** This is the part
that took measuring. LaMa is a *removal* model: asked to invent a gap as wide as
its own context it regresses to a blurry, desaturated mean — detail 0.59 and
chroma 0.23 against the source, and it looks exactly that bad. Iterating a
narrow gap to reach the same length is better but not enough, because each pass
paints over the last one's output and the blur compounds: five passes reached
2.0x at detail 0.61, and the generated half was visibly smeared.

So the gap stays at [`GAP_FRACTION`] and runs [`GROWTH_PASSES`] time. A long
repeat is still wanted — a pattern a player sees come round is a pattern they
notice — but it is bought by **cropping a longer band of real texture**, not by
asking a model to invent one. The shipped strips are 7 to 10 cells with ~13%
generated, at detail 0.82-0.98: the join is invisible and nothing else moved.

`--report` prints the three numbers this was tuned on. `seam` is the wrap
discontinuity over an ordinary column step, so 1.0 means the join is
indistinguishable from anywhere else in the texture; `detail` and `chroma`
compare the second half of the strip against the first, and catch the mush.

--------------------------------------------------------------------------
What each PNG is

**One repeat of a coat, laid out along the snake**: `x` runs down the body from
the head, `y` runs across it, and the full image height is exactly one cell.
A repeat is many cells long, which is the point — a pattern repeating every cell
reads as a grid.

They are **opaque**. The coat replaces the body fill rather than tinting it, so
a tiger is a tiger's colours and not a team colour with stripes drawn on. The
friend/foe reading moves to the contour, which the skin keeps role-coloured; see
`client/src/skin/animal.rs`.

Each animal ships twice: the texture itself, and a **print** — the same coat
posterised to two or three flat tones. Fur and fabric are different things to
wear, and the print keeps a real animal's geometry while dropping everything
else.
"""
import argparse
import io
import os
import urllib.request

import numpy as np
from PIL import Image

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUT_DIR = os.path.join(ROOT, "web", "public", "images", "skins")
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".texture-cache")

# One cell of body, in texels, in the shipped strip. The arena walks cell sizes
# 15 down to 5 and a retina capture doubles that, so 64 keeps every real size
# supersampled.
CELL = 64

# The height everything is inpainted at. LaMa was trained on images of a few
# hundred pixels; feeding it the source's full resolution measurably degrades
# the fill, and 128 is still twice the shipped strip.
WORK_HEIGHT = 128

# How much of the tile's own width the model is asked to invent, and how many
# times. See the module docstring: a wide gap comes out mush and repeating a
# narrow one compounds that mush, so the gap is narrow and runs once.
GAP_FRACTION = 0.05
GROWTH_PASSES = 1

# Source pixels that become one cell of body. Fixes the scale of the marks, and
# lets the window search below choose the crop's *length* freely.
SOURCE_PIXELS_PER_CELL = 100

USER_AGENT = "Snaketron coat texture build (https://github.com/lopatin/Snaketron)"


class Source:
    """One third-party texture, and the band of it that becomes a coat."""

    def __init__(
        self,
        name,
        url,
        band,
        repeat_cells,
        cells_range=(8, 12),
        pieces=1,
        print_bands=2,
        seamless=False,
        note="",
    ):
        self.name = name
        self.url = url
        # `(left, right, centre_y)` in fractions of the source. The band's
        # height is *derived* from the repeat, so the crop already has the
        # aspect ratio the finished strip needs and the final resize never
        # stretches the marks.
        self.band = band
        # Cells covered by the crop of real texture. The shipped repeat is
        # this plus the inpainted join.
        self.repeat_cells = repeat_cells
        # How long a crop the window search may consider, in cells.
        self.cells_range = cells_range
        # How many different windows of the source to stitch into one repeat.
        # A seamless 2D tile has several: bands at different heights through it
        # are different arrangements of the same marks, each seamless in x on
        # its own, and stitching them multiplies the repeat without inventing
        # anything.
        self.pieces = pieces
        self.chosen_cells = repeat_cells
        self.chosen_join = 0.0
        self.print_bands = print_bands
        # A source that already tiles is sliced rather than cropped. It still
        # goes through the join pass: it costs almost nothing, keeps one code
        # path, and the extra sliver breaks up a small tile's own rhythm.
        self.seamless = seamless
        self.note = note

    def pieces_at_work_height(self):
        """The windows that make up one repeat, in order."""
        if not self.seamless or self.pieces == 1:
            return [self.band_at_work_height()]
        image = self.fetch()
        self.chosen_cells = self.repeat_cells
        side = int(self.repeat_cells * WORK_HEIGHT)
        wide = Image.new(image.mode, (image.width * 3, image.height))
        for index in range(3):
            wide.paste(image, (index * image.width, 0))
        scaled = wide.resize((side * 3, side), Image.Resampling.LANCZOS).crop(
            (side, 0, side * 2, side)
        )
        # Evenly spaced bands through the tile. Each is seamless in x by
        # itself; the joins *between* them are what the stitch paints.
        return [
            scaled.crop((0, top, side, top + WORK_HEIGHT))
            for top in (
                int(index * (side - WORK_HEIGHT) / max(1, self.pieces - 1))
                for index in range(self.pieces)
            )
        ]

    @property
    def shipped_cells(self):
        # Whole cells, because the strip is resized to exactly this many and
        # `skin::animal` asserts the PNG's aspect equals the repeat it declares.
        # A seamless source needs no join, so it ships exactly what it cropped.
        if self.seamless and self.pieces == 1:
            return round(self.chosen_cells)
        return round(self.chosen_cells * self.pieces * (1.0 + GAP_FRACTION))

    def choose_window(self, image):
        """Pick the crop that tiles best and is sharpest, by search.

        Two things ruin a coat, and both are properties of *where* the crop was
        taken rather than of anything done to it afterwards:

        - **A join the source never meant to make.** Cropping an arbitrary width
          leaves two unrelated columns meeting. Widths near the texture's own
          period leave columns that nearly match, and the difference between the
          best and worst candidate here is a factor of ten — far more than any
          amount of blending afterwards can recover.
        - **A soft patch.** These sources are photographs; parts of them are out
          of focus. A window containing one tiles that softness, and it reads as
          a recurring smudge, which is exactly the artifact this search was
          written to kill.

        So: score every window on both, reject the ones with a soft patch in
        them, and take the best join among what is left.
        """
        width, height = image.size
        _, _, centre_y = self.band
        grey = np.asarray(image.convert("L"), dtype=np.float64)

        # Sharpness per column, smoothed, over the strip the crop will use.
        band = grey[
            max(0, int(centre_y * height) - 64) : int(centre_y * height) + 64
        ]
        detail_per_column = np.convolve(
            np.abs(np.diff(band, axis=1)).mean(axis=0), np.ones(25) / 25, mode="same"
        )
        sharp_enough = detail_per_column.mean() * 0.45

        candidates = []
        for cells in range(self.cells_range[0], self.cells_range[1] + 1):
            span = cells * SOURCE_PIXELS_PER_CELL
            if span > width:
                continue
            tall = min(span / cells, float(height))
            for x0 in range(0, width - span + 1, 16):
                window = detail_per_column[x0 : x0 + span]
                # The softest stretch anywhere in the window, as a fraction of
                # the source's own average sharpness.
                softest = np.convolve(window, np.ones(64) / 64, mode="valid").min()
                if softest < sharp_enough:
                    continue
                y0 = int(max(0, min(centre_y * height - tall / 2.0, height - tall)))
                strip = grey[y0 : y0 + int(tall), x0 : x0 + span]
                join = float(np.abs(strip[:, -1] - strip[:, 0]).mean())
                candidates.append((join, -softest, cells, x0, y0, span, int(tall)))

        if not candidates:
            raise SystemExit(
                f"{self.name}: no crop of {self.cells_range} cells avoids the "
                "soft parts of this source — widen cells_range or move band's "
                "centre_y"
            )
        candidates.sort()
        # Among windows that join about as well as the best one, take the
        # longest. A better join is worth almost nothing once it is invisible,
        # and a longer repeat is worth a lot: it is the difference between a
        # player seeing the pattern come round on one snake and not.
        reachable = candidates[0][0] * 1.6 + 2.0
        join, _, cells, x0, y0, span, tall = max(
            (c for c in candidates if c[0] <= reachable),
            key=lambda c: (c[2], -c[0]),
        )
        self.chosen_cells = cells
        self.chosen_join = join
        return (x0, y0, x0 + span, y0 + tall)

    def fetch(self):
        os.makedirs(CACHE_DIR, exist_ok=True)
        path = os.path.join(CACHE_DIR, f"{self.name}-source.jpg")
        if not os.path.exists(path):
            print(f"fetching {self.name} source…")
            request = urllib.request.Request(self.url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(request, timeout=180) as response:
                data = response.read()
            Image.open(io.BytesIO(data)).convert("RGB").save(path, quality=95)
        return Image.open(path).convert("RGB")

    def band_at_work_height(self):
        """The crop, at working resolution, with no seam allowance at all."""
        image = self.fetch()
        if self.seamless:
            # Scale the tile so one of its own repeats is the band's length,
            # then take a one-cell slice through the middle. A tile that is
            # seamless in both axes stays seamless in x under any such slice.
            self.chosen_cells = self.repeat_cells
            side = int(self.repeat_cells * WORK_HEIGHT)
            # Resampling reads past the edges and Pillow invents what is there
            # by clamping, which quietly destroys the one property this source
            # was chosen for. Scale a three-up tiling and keep the middle.
            wide = Image.new(image.mode, (image.width * 3, image.height))
            for index in range(3):
                wide.paste(image, (index * image.width, 0))
            scaled = wide.resize((side * 3, side), Image.Resampling.LANCZOS)
            scaled = scaled.crop((side, 0, side * 2, side))
            top = (side - WORK_HEIGHT) // 2
            return scaled.crop((0, top, side, top + WORK_HEIGHT))
        box = self.choose_window(image)
        return image.crop(box).resize(
            (int(self.chosen_cells * WORK_HEIGHT), WORK_HEIGHT),
            Image.Resampling.LANCZOS,
        )


SOURCES = [
    Source(
        "zebra",
        # OpenGameArt, "Fur of Tiger, Giraffe and Zebra", CC0 — a game texture
        # rather than a wildlife photograph, which is the difference between a
        # coat that reads at fifteen pixels a cell and one that does not.
        "https://opengameart.org/sites/default/files/oga-textures/"
        "publicdomainpictures.net-zebra-texture-11297063007kge.jpg",
        band=(0.0, 1.0, 0.5),
        repeat_cells=9.0,
        # Black and white, and nothing between them.
        print_bands=2,
        note="high-contrast black and white bands",
    ),
    Source(
        "tiger",
        # Same CC0 pack: saturated orange, heavy black stripes, flat lighting.
        "https://opengameart.org/sites/default/files/oga-textures/"
        "publicdomainpictures.net-1-1230580077r3k7.jpg",
        # The upper flank, where the ground is brightest: the middle of this
        # texture is nearly all stripe and comes out as embers on black.
        band=(0.0, 1.0, 0.22),
        repeat_cells=9.0,
        # Black stripe, orange ground, and the deep amber between them.
        print_bands=3,
        note="saturated orange with heavy black stripes",
    ),
    Source(
        "jaguar",
        # OpenGameArt, "Leopard print" by diamond-sparrow, CC-BY 4.0. Already a
        # seamless tile, so it is sliced rather than cropped.
        #
        # It is a spotted-cat print rather than a jaguar specifically — a real
        # jaguar's rosettes carry spots inside them and a leopard's do not, and
        # no free game texture makes that distinction. Named for the animal it
        # dresses the snake as; recorded honestly here and in
        # THIRD_PARTY_ASSETS.md.
        "https://opengameart.org/sites/default/files/leopard_128.jpg",
        band=(0.0, 1.0, 0.5),
        repeat_cells=6.0,
        seamless=True,
        pieces=2,
        # Rosette, tawny ground, and the shadow inside each rosette.
        print_bands=3,
        note="tawny ground with black rosettes",
    ),
]


# ---------------------------------------------------------------------------
# Making it tile


def inpaint_wrap(lama, tile, gap_fraction=GAP_FRACTION):
    """`[T, X, T]` in, `[T, X]` out — a tile that wraps onto itself.

    The two `T` halves are restored byte-for-byte afterwards. That is not
    tidiness: the whole guarantee is that every junction in `… T X T X …` is one
    the model saw, and it only holds if `T` is exactly what it saw.
    """
    array = np.asarray(tile.convert("RGB"), dtype=np.uint8)
    height, width, _ = array.shape
    gap = max(8, int(round(width * gap_fraction)))

    canvas = np.empty((height, width * 2 + gap, 3), dtype=np.uint8)
    canvas[:, :width] = array
    canvas[:, width + gap :] = array
    # Seed the gap with a mirror of the tile. The mask is what decides what the
    # model may keep, but handing it black would waste part of the fill on
    # getting away from black.
    canvas[:, width : width + gap] = np.tile(array[:, ::-1], (1, 2, 1))[:, :gap]

    mask = np.zeros((height, width * 2 + gap), dtype=np.uint8)
    mask[:, width : width + gap] = 255

    filled = np.asarray(
        lama(Image.fromarray(canvas), Image.fromarray(mask)), dtype=np.uint8
    )
    # LaMa pads its input up to a multiple of eight and hands the padding back.
    filled = filled[:height, : width * 2 + gap]

    out = canvas.copy()
    out[:, width : width + gap] = filled[:, width : width + gap]
    return Image.fromarray(out[:, : width + gap])


def stitch(lama, pieces, gap_fraction=GAP_FRACTION):
    """The same trick, generalised to a cycle of several pieces.

    `[A, X, B, X, C, X, A]` in, `[A, X, B, X, C, X]` out. The canvas ends with a
    second copy of `A` so the last gap is generated against the piece it will
    actually meet, which is what makes the whole thing close on itself.

    This is how a repeat gets *long* without asking the model to invent length.
    Each piece is a different window of real texture, and only the joins between
    them are generated — so a three-piece stitch is a repeat three times as long
    with the same fraction of invented pixels as a one-piece wrap.
    """
    if len(pieces) == 1:
        return inpaint_wrap(lama, pieces[0], gap_fraction)

    arrays = [np.asarray(p.convert("RGB"), dtype=np.uint8) for p in pieces]
    height = arrays[0].shape[0]
    gap = max(8, int(round(np.mean([a.shape[1] for a in arrays]) * gap_fraction)))

    order = arrays + [arrays[0]]
    total = sum(a.shape[1] for a in order) + gap * len(arrays)
    canvas = np.empty((height, total, 3), dtype=np.uint8)
    mask = np.zeros((height, total), dtype=np.uint8)

    keep_width = 0
    cursor = 0
    for index, piece in enumerate(order):
        canvas[:, cursor : cursor + piece.shape[1]] = piece
        cursor += piece.shape[1]
        if index < len(arrays):
            # Seed each gap by mirroring what precedes it, for the same reason
            # `inpaint_wrap` does: black would waste part of the fill.
            canvas[:, cursor : cursor + gap] = piece[:, ::-1][:, :gap]
            mask[:, cursor : cursor + gap] = 255
            cursor += gap
            keep_width = cursor

    filled = np.asarray(
        lama(Image.fromarray(canvas), Image.fromarray(mask)), dtype=np.uint8
    )[:height, :total]
    out = np.where(mask[..., None] > 0, filled, canvas)
    return Image.fromarray(out[:, :keep_width])


def grow(lama, tile, passes=GROWTH_PASSES):
    """Apply the trick repeatedly. Every intermediate result already tiles."""
    for _ in range(passes):
        tile = inpaint_wrap(lama, tile)
    return tile


def wrapped(image, operation):
    """Run `operation` on three copies of the tile and keep the middle one.

    Every filter after the inpainting — the downsample to shipping size, the
    blur inside the print — reads a neighbourhood, and at the image's own edges
    Pillow invents that neighbourhood by clamping. That quietly undoes the seam
    the whole [T, X, T] pass exists to create: the left edge gets filtered
    against a repeat of itself while the right edge gets filtered against a
    repeat of *itself*, and the two stop meeting.

    Filtering a three-up tiling and keeping the middle gives every edge pixel
    the neighbours it will actually have. It measurably matters: the prints came
    out at a seam ratio of 2.7 without it and 1.0 with it.
    """
    wide = Image.new(image.mode, (image.width * 3, image.height))
    for index in range(3):
        wide.paste(image, (index * image.width, 0))
    out = operation(wide)
    third = out.width // 3
    return out.crop((third, 0, third * 2, out.height))


# ---------------------------------------------------------------------------
# Measurements the tuning was done against


def seam_ratio(image):
    """Where the wrap join ranks among the strip's own column steps, 0..1.

    Reported as a **percentile**, not a ratio. The obvious ratio — the join's
    step over the mean interior step — is not diagnostic on the strips it has to
    sign off, and an adversarial review is what established that: a posterised
    print is mostly flat, so its mean interior step is tiny and one legitimate
    stripe edge landing on the join reads as 1.95; a high-contrast hide has a
    large mean that can bury a real misalignment. Both directions were measured
    on shipped files.

    A percentile asks the only question that matters — *is this join unusual for
    this texture?* — and answers it in the texture's own units. 0.5 means the
    join is a perfectly ordinary column boundary. Anything above ~0.9 is a join
    that stands out from its own texture and will be visible when it repeats.
    """
    a = np.asarray(image.convert("RGB"), dtype=np.float64)
    join = np.abs(a[:, 0] - a[:, -1]).mean()
    interior = np.abs(np.diff(a, axis=1)).mean(axis=(0, 2))
    return float((interior < join).mean())


def detail(pixels):
    grey = np.asarray(pixels, dtype=np.float64) @ np.array([0.2126, 0.7152, 0.0722])
    return float(
        np.abs(np.diff(grey, axis=1)).mean() + np.abs(np.diff(grey, axis=0)).mean()
    )


def chroma(pixels):
    a = np.asarray(pixels, dtype=np.float64)
    return float((a.max(axis=2) - a.min(axis=2)).mean())


# ---------------------------------------------------------------------------
# The print variant


def posterize(pixels, bands, width, height, punch=0.16):
    """Flatten a coat into a printed animal-print.

    The second half of the family. A drawn print and a photographed hide are
    genuinely different things to wear, and the honest way to get the drawn one
    is to keep the *geometry* of a real animal and throw away everything else.
    Inventing the geometry instead is what produced marks that read as
    scratches rather than as a coat.

    **Done at output scale, not source scale.** The whole operation is "lose the
    hair, keep the marks", and which is which is a question about the finished
    strip. Working at twice the output size puts a mark at tens of pixels and a
    hair below one, so a small blur cleanly keeps the first and erases the
    second — and the final halving is what leaves the edges looking inked rather
    than aliased.
    """
    supersample = 2
    work = Image.fromarray((pixels * 255).astype(np.uint8)).resize(
        (width * supersample, height * supersample), Image.Resampling.LANCZOS
    )
    tones = np.asarray(work, dtype=np.float64) / 255.0

    # A box blur that wraps horizontally, rather than a resize round-trip.
    #
    # The round-trip version was subtly wrong and it took an adversarial review
    # to catch: it downsampled by three, so unless the tile's period happened to
    # be divisible by three, each repeat was blurred at a *different sub-pixel
    # phase*. The blur then feeds a hard k-means threshold, so a fraction of a
    # level of drift flipped whole band labels — measured at 26% of pixels on
    # the tiger. The three-up wrapper could not save it, because the error was
    # inside the operation rather than at its edges. Padding with `wrap` makes
    # the blur exactly periodic by construction, whatever the period is.
    radius = max(1, (width * supersample) // 300)
    grey = tones @ np.array([0.2126, 0.7152, 0.0722])
    padded = np.pad(grey, ((radius, radius), (radius, radius)), mode="wrap")
    window = 2 * radius + 1
    kernel = np.ones(window) / window
    smoothed = np.apply_along_axis(
        lambda row: np.convolve(row, kernel, mode="valid"), 1, padded
    )
    luminance = np.apply_along_axis(
        lambda col: np.convolve(col, kernel, mode="valid"), 0, smoothed
    )

    # One-dimensional k-means, seeded on quantiles so it is deterministic.
    centres = np.quantile(luminance, np.linspace(0.10, 0.90, bands))
    for _ in range(24):
        labels = np.argmin(np.abs(luminance[..., None] - centres[None, None, :]), axis=2)
        for band in range(bands):
            members = luminance[labels == band]
            if members.size:
                centres[band] = members.mean()
        centres = np.sort(centres)

    labels = np.argmin(np.abs(luminance[..., None] - centres[None, None, :]), axis=2)
    flat = np.zeros_like(tones)
    for band in range(bands):
        mask = labels == band
        if not mask.any():
            continue
        # The band's own colour, taken from the coat, then pushed apart from its
        # neighbours the way ink is and light is not.
        colour = np.median(tones[mask], axis=0)
        lift = (band / max(1, bands - 1) - 0.5) * 2.0 * punch
        flat[mask] = np.clip(colour + lift, 0.0, 1.0)

    return (
        np.asarray(
            Image.fromarray((flat * 255).astype(np.uint8)).resize(
                (width, height), Image.Resampling.LANCZOS
            ),
            dtype=np.float64,
        )
        / 255.0
    )


# ---------------------------------------------------------------------------


def emit(image, source, suffix, note, report=False, seed=None):
    path = os.path.join(OUT_DIR, f"{source.name}{suffix}.v1.png")
    os.makedirs(OUT_DIR, exist_ok=True)
    image.save(path, optimize=True)

    line = (
        f"{os.path.relpath(path, ROOT):50s} {image.width}x{image.height}  "
        f"repeat {source.shipped_cells:g} cells  — {note}"
    )
    if report:
        pixels = np.asarray(image.convert("RGB"))
        grown_from = int(round(pixels.shape[1] / 2))
        line += (
            f"\n{'':50s} seam {seam_ratio(image):.2f}"
            f"   detail {detail(pixels[:, grown_from:]) / max(detail(pixels[:, :grown_from]), 1e-9):.2f}"
            f"   chroma {chroma(pixels[:, grown_from:]) / max(chroma(pixels[:, :grown_from]), 1e-9):.2f}"
            + (f"   (before: {seed:.2f})" if seed is not None else "")
        )
    print(line)
    return image


def build(source, lama, preview_dir=None, report=False):
    pieces = source.pieces_at_work_height()
    before = seam_ratio(pieces[0])
    if source.seamless and len(pieces) == 1:
        # Already wraps exactly. Inpainting could only add a band that was not
        # there — the very artifact this pass exists to remove.
        grown = pieces[0]
    else:
        grown = stitch(lama, pieces)

    width = int(source.shipped_cells * CELL)
    hide = emit(
        wrapped(grown, lambda wide: wide.resize((width * 3, CELL), Image.Resampling.LANCZOS)),
        source,
        "",
        source.note,
        report,
        before,
    )
    printed = emit(
        wrapped(
            grown,
            lambda wide: Image.fromarray(
                (
                    posterize(
                        np.asarray(wide.convert("RGB"), dtype=np.float64) / 255.0,
                        source.print_bands,
                        width * 3,
                        CELL,
                    )
                    * 255
                )
                .round()
                .astype(np.uint8)
            ),
        ),
        source,
        "-print",
        f"{source.print_bands}-tone print of the same coat",
        report,
    )

    if preview_dir:
        os.makedirs(preview_dir, exist_ok=True)
        for suffix, strip in (("", hide), ("-print", printed)):
            sheet = Image.new("RGB", (strip.width * 3, CELL), (255, 255, 255))
            for index in range(3):
                sheet.paste(strip, (index * strip.width, 0))
            sheet.save(os.path.join(preview_dir, f"{source.name}{suffix}-tiled.png"))
    return hide, printed


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--preview", help="also write three-up tiling previews here")
    parser.add_argument(
        "--report", action="store_true", help="print seam, detail and chroma"
    )
    args = parser.parse_args()

    try:
        import torch
        from simple_lama_inpainting import SimpleLama
    except ImportError as missing:  # pragma: no cover - a developer's first run
        raise SystemExit(
            f"{missing}. This build needs LaMa to make the coats tile:\n"
            "  pip install simple-lama-inpainting opencv-python-headless\n"
            "(add --no-deps to the first if pip tries to build an ancient Pillow)"
        ) from missing

    device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
    print(f"inpainting on {device}")
    lama = SimpleLama(device=device)

    for source in SOURCES:
        build(source, lama, args.preview, args.report)
