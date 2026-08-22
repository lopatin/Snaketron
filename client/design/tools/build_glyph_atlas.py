"""Build the glyph atlas a `text` skin layer draws from.

A skin can wear a word — one character per body cell — and the letters have to
come from somewhere. This builds that somewhere: a single strip of glyphs,
bundled like any other skin art.

**A bitmap rather than a font**, and the choice is not about effort. Canvas
`fillText` would give crisper, scalable letters and no asset at all, but it
would resolve against whatever fonts the viewer's machine happens to have — so
two players in one match could see the same snake wearing two different
alphabets. That is the same class of bug `skin_schema::expr`'s noise function
avoids by hashing rather than sampling a table: a cosmetic that differs per
machine is a desync nobody thinks to look for. Bitmap glyphs are identical
everywhere, which for a competitive game is worth more than kerning.

The font is 5x7 in a 16x16 cell — deliberately small and blocky, because a body
cell is 15 px at the arena's largest zoom and a letter drawn finer than that
turns to mush the moment the arena scales down.

Usage::

    python build_glyph_atlas.py

Writes `client/web/public/images/skins/glyphs.v1.png` and prints the charset
so `skin_schema::v2::TEXT_CHARSET` can be checked against it.
"""

from __future__ import annotations

from pathlib import Path

from PIL import Image

# One 16x16 cell per glyph, with a 5x7 face centred in it. The order here *is*
# the atlas order, and `TEXT_CHARSET` in `skin-schema/src/v2.rs` has to match it
# character for character — a test asserts they do, because an off-by-one here
# would silently shift every letter a skin paints.
CELL = 16
FACE_WIDTH = 5
FACE_HEIGHT = 7

# Each glyph is seven rows of five, `#` on and `.` off. Written out rather than
# generated so what a letter looks like is visible in the diff that changes it.
FONT: dict[str, tuple[str, ...]] = {
    " ": (".....", ".....", ".....", ".....", ".....", ".....", "....."),
    "A": (".###.", "#...#", "#...#", "#####", "#...#", "#...#", "#...#"),
    "B": ("####.", "#...#", "####.", "#...#", "#...#", "#...#", "####."),
    "C": (".###.", "#...#", "#....", "#....", "#....", "#...#", ".###."),
    "D": ("####.", "#...#", "#...#", "#...#", "#...#", "#...#", "####."),
    "E": ("#####", "#....", "####.", "#....", "#....", "#....", "#####"),
    "F": ("#####", "#....", "####.", "#....", "#....", "#....", "#...."),
    "G": (".###.", "#...#", "#....", "#.###", "#...#", "#...#", ".###."),
    "H": ("#...#", "#...#", "#####", "#...#", "#...#", "#...#", "#...#"),
    "I": ("#####", "..#..", "..#..", "..#..", "..#..", "..#..", "#####"),
    "J": ("....#", "....#", "....#", "....#", "#...#", "#...#", ".###."),
    "K": ("#...#", "#..#.", "#.#..", "##...", "#.#..", "#..#.", "#...#"),
    "L": ("#....", "#....", "#....", "#....", "#....", "#....", "#####"),
    "M": ("#...#", "##.##", "#.#.#", "#...#", "#...#", "#...#", "#...#"),
    "N": ("#...#", "##..#", "#.#.#", "#..##", "#...#", "#...#", "#...#"),
    "O": (".###.", "#...#", "#...#", "#...#", "#...#", "#...#", ".###."),
    "P": ("####.", "#...#", "#...#", "####.", "#....", "#....", "#...."),
    "Q": (".###.", "#...#", "#...#", "#...#", "#.#.#", "#..#.", ".##.#"),
    "R": ("####.", "#...#", "#...#", "####.", "#.#..", "#..#.", "#...#"),
    "S": (".####", "#....", "#....", ".###.", "....#", "....#", "####."),
    "T": ("#####", "..#..", "..#..", "..#..", "..#..", "..#..", "..#.."),
    "U": ("#...#", "#...#", "#...#", "#...#", "#...#", "#...#", ".###."),
    "V": ("#...#", "#...#", "#...#", "#...#", "#...#", ".#.#.", "..#.."),
    "W": ("#...#", "#...#", "#...#", "#.#.#", "#.#.#", "##.##", "#...#"),
    "X": ("#...#", "#...#", ".#.#.", "..#..", ".#.#.", "#...#", "#...#"),
    "Y": ("#...#", "#...#", ".#.#.", "..#..", "..#..", "..#..", "..#.."),
    "Z": ("#####", "....#", "...#.", "..#..", ".#...", "#....", "#####"),
    "0": (".###.", "#...#", "#..##", "#.#.#", "##..#", "#...#", ".###."),
    "1": ("..#..", ".##..", "..#..", "..#..", "..#..", "..#..", ".###."),
    "2": (".###.", "#...#", "....#", "...#.", "..#..", ".#...", "#####"),
    "3": ("####.", "....#", "....#", ".###.", "....#", "....#", "####."),
    "4": ("#...#", "#...#", "#...#", "#####", "....#", "....#", "....#"),
    "5": ("#####", "#....", "####.", "....#", "....#", "#...#", ".###."),
    "6": (".###.", "#....", "#....", "####.", "#...#", "#...#", ".###."),
    "7": ("#####", "....#", "...#.", "..#..", ".#...", ".#...", ".#..."),
    "8": (".###.", "#...#", "#...#", ".###.", "#...#", "#...#", ".###."),
    "9": (".###.", "#...#", "#...#", ".####", "....#", "....#", ".###."),
    ".": (".....", ".....", ".....", ".....", ".....", ".##..", ".##.."),
    "!": ("..#..", "..#..", "..#..", "..#..", "..#..", ".....", "..#.."),
    "?": (".###.", "#...#", "....#", "...#.", "..#..", ".....", "..#.."),
    "-": (".....", ".....", ".....", "#####", ".....", ".....", "....."),
}

# The order the atlas lays glyphs out in, and therefore the order a lowering
# indexes them by. Space first so the empty glyph is index zero — a text source
# that somehow asks for an unknown character lands on blank rather than on a
# letter nobody chose.
CHARSET = " ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.!?-"


# The two inks the strip carries, light row first.
#
# Two rows rather than one white row the renderer tints, because canvas cannot
# tint a `drawImage`: `fillStyle` does not touch a blit, and the alternatives —
# a composite operation, or baking a tinted copy per colour — need either a
# per-snake isolation buffer or an offscreen canvas, neither of which this
# engine has and both of which would be invisible to the recorder that verifies
# every other skin.
#
# So the ink is baked, and a document's colour reference *chooses between these
# two* by luminance rather than becoming one. That is a smaller promise than
# arbitrary colour and it is the promise that matters: a word has to be legible
# on a light snake and on a dark one, and these are those two cases. Picking the
# row is a source-rect offset — a paint argument — so it can vary per role
# without changing a single op.
INKS = ((255, 255, 255, 255), (16, 20, 26, 255))


def build() -> Image.Image:
    """Two rows of glyph cells: light ink above, dark ink below."""
    atlas = Image.new(
        "RGBA", (CELL * len(CHARSET), CELL * len(INKS)), (255, 255, 255, 0)
    )
    pixels = atlas.load()

    # The face sits centred, and one row lower than centre: descenders are not
    # in this font, so optical centre is above geometric centre.
    left = (CELL - FACE_WIDTH) // 2
    top = (CELL - FACE_HEIGHT) // 2

    for ink_index, ink in enumerate(INKS):
        for index, character in enumerate(CHARSET):
            rows = FONT.get(character)
            if rows is None:
                raise SystemExit(f"no glyph drawn for {character!r}")
            origin = index * CELL
            base = ink_index * CELL
            for y, row in enumerate(rows):
                for x, cell in enumerate(row):
                    if cell == "#":
                        pixels[origin + left + x, base + top + y] = ink
    return atlas


def main() -> int:
    missing = sorted(set(CHARSET) - set(FONT))
    if missing:
        raise SystemExit(f"charset has characters with no glyph: {missing}")

    out = (
        Path(__file__).resolve().parents[2]
        / "web"
        / "public"
        / "images"
        / "skins"
        / "glyphs.v1.png"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    build().save(out, format="PNG", optimize=True)

    print(f"wrote {out} ({len(CHARSET)} glyphs x {len(INKS)} inks, {CELL}px cells)")
    print(f"TEXT_CHARSET must be exactly: {CHARSET!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
