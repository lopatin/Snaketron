#!/usr/bin/env python3
"""Build per-division rank badges (design system v3 — owner's family spec).

Families:
  Metals (bronze / silver / gold, divisions 1-3)
      1 = the badge's single top chevron alone
      2 = the current two-chevron art, untouched
      3 = a true triple stack, tapered: the third chevron is a scaled-down
          copy of the second, tucked under it the same way 2 tucks under 1
  Plat (platinum, divisions 1-3)
      crown + wings with 0 / 1 / 2 lower bands (same taper rule for the
      added band); 2 = the current art untouched
  Diamond (divisions 1-3)
      the gem with I / II / III as stylized white numeral bars integrated
      into the pavilion center
  Grand Master (divisions 1-3)
      the current art carries exactly two hanging fangs; 1 = a single
      symmetric center fang, 2 = current art, 3 = both side fangs plus the
      center fang

Variants are composed at the raster level from the original art's own
pixels wherever possible and traced with the verified tracer, so color,
bevel, and outline language never drift. Purely additive elements (diamond
numerals, the GM center fang) are built as vectors in sampled tier colors.

Reads   client/web/public/images/{tier}.png
Writes  client/web/components/rankIconDivisionData.ts

Usage:  python3 client/design/tools/build_division_icons.py [preview_dir]
Deps :  pillow, numpy, scipy, scikit-image
"""
import os
import re
import sys
import tempfile
import numpy as np
from PIL import Image

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from trace_rank_icons import ROOT, SRC, trace_icon  # noqa: E402

DST = os.path.join(ROOT, "client", "web", "components", "rankIconDivisionData.ts")

# Cut polylines (left, center, right) hugging the top edge of the lowest
# chevron band; pixels below form the removable / stackable slice.
CUTS = {
    "bronze": ((20, 78), (100, 140), (180, 76)),
    "silver": ((20, 74), (100, 135), (180, 71)),
    "gold": ((20, 74), (100, 135), (180, 70)),
    "platinum": ((20, 86), (100, 148), (180, 86)),
}
TAPER = 0.84  # the added third chevron is this fraction of the one above


def cut_y(cut, x):
    (xl, yl), (xc, yc), (xr, yr) = cut
    if x <= xl:
        return yl
    if x >= xr:
        return yr
    if x <= xc:
        return yl + (yc - yl) * (x - xl) / (xc - xl)
    return yc + (yr - yc) * (x - xc) / (xr - xc)


def below_cut(arr, cut):
    h, w = arr.shape[:2]
    ys = np.arange(h)[:, None]
    line = np.array([cut_y(cut, x) for x in range(w)])[None, :]
    return ys > line


def slice_columns(arr, cut):
    """Columns genuinely owned by the lowest chevron (not outline stubs)."""
    mask = arr[..., 3] >= 128
    w = arr.shape[1]
    depth = np.full(w, -1.0)
    for x in range(w):
        col = np.nonzero(mask[:, x])[0]
        if len(col):
            depth[x] = col.max() - cut_y(cut, x)
    return depth > 25


def erase_slice(im, cut):
    """Division 1: the badge with its lowest chevron removed."""
    arr = np.array(im)
    out = arr.copy()
    kill = below_cut(arr, cut)
    kill[:, ~slice_columns(arr, cut)] = False
    out[kill] = 0
    return Image.fromarray(out)


def add_tapered_slice(im, cut, taper=TAPER):
    """Division 3: a scaled-down copy of the lowest chevron tucked below.

    The copy is scaled about the slice's top-center so it stays centered and
    keeps the chevron angles; the tuck offset is the largest translation
    that leaves no background gap in any column, exactly how the original's
    second chevron tucks under its first.
    """
    arr = np.array(im)
    h, w = arr.shape[:2]
    mask = arr[..., 3] >= 128

    sl = arr.copy()
    keep = below_cut(arr, cut)
    keep[:, ~slice_columns(arr, cut)] = False
    sl[~keep] = 0
    slice_im = Image.fromarray(sl)

    cx, cy = 100.0, cut_y(cut, 100)
    s = taper
    # PIL AFFINE takes the inverse map: out(x,y) <- src(a x + b y + c, ...)
    scaled = slice_im.transform(
        (w, h), Image.AFFINE,
        (1 / s, 0, cx - cx / s, 0, 1 / s, cy - cy / s),
        resample=Image.BICUBIC,
    )
    sarr = np.array(scaled)
    smask = sarr[..., 3] >= 128

    bottom = np.full(w, -1.0)
    for x in range(w):
        col = np.nonzero(mask[:, x])[0]
        if len(col):
            bottom[x] = col.max()
    pitches = []
    for x in range(w):
        col = np.nonzero(smask[:, x])[0]
        if len(col) and bottom[x] > 0:
            pitches.append(bottom[x] - col.min())
    pitch = int(min(pitches)) - 1

    H = h + 45
    canvas = Image.new("RGBA", (w, H), (0, 0, 0, 0))
    canvas.alpha_composite(scaled, (0, pitch))
    canvas.alpha_composite(im, (0, 0))
    return canvas


# ------------------------------------------------------------- grandmaster --

GM_FANG_COLORS = {"outline": "#2b3a3c", "left": "#fcc93a", "right": "#e9a92c"}

# GM division I's lone fang: same trapezoidal body and hang depth as
# division III's center fang, only wider at the shoulder.
#
# Placement is geometry-driven. The crown V narrows as it descends and the
# gaps flanking it are the side-fang slots, so a wide fang whose top sits at
# division III's row (y=148) presents a bare horizontal ledge in open
# background — visible top corners. Tucking the top up into the badge's
# solid wing band (opaque across x[19,181] until y=134) hides the shoulder
# entirely, so only the fang's descending body is ever seen.
GM1_FANG_HW = float(os.environ.get("GM1_FANG_HW", 42.0))
GM1_FANG_TOP = float(os.environ.get("GM1_FANG_TOP", 128.0))

# The badge's own chevron angle, measured off the grandmaster art: the crown
# V's inner edge and both wing outer edges all fit 0.90-0.92 (about 42.4
# degrees). Division I's fang takes its bottom edge from this so the notch
# is parallel to the chevron it hangs from.
BADGE_CHEVRON_SLOPE = 0.9145
# Tip depth of division III's centre fang; division I's fang points to the
# same place, so widening it can never make division I the taller badge.
GM_FANG_TIP_Y = 199.0


def gm_fang_masks(arr):
    """The two side fangs, found as the small bright-gold components that
    are separated from the wing gold by the dark outlines."""
    from scipy import ndimage

    rgb = arr[..., :3].astype(int)
    alpha = arr[..., 3]
    gold = (alpha >= 128) & (rgb.sum(-1) > 330) & (rgb[..., 0] > rgb[..., 2] + 40)
    lab, n = ndimage.label(gold, structure=np.ones((3, 3)))
    fangs = []
    for i in range(1, n + 1):
        comp = lab == i
        area = int(comp.sum())
        ys, xs = np.nonzero(comp)
        # a fang: modest area, centered low, clear of the badge midline zone
        if 250 < area < 1600 and ys.mean() > 110 and abs(xs.mean() - 100) > 25:
            fangs.append(comp)
    if len(fangs) != 2:
        raise RuntimeError(f"expected 2 side fangs, found {len(fangs)}")
    wing_gold = gold & ~np.logical_or.reduce(fangs)
    return fangs, wing_gold


def gm_erase_side_fangs(im):
    from scipy import ndimage

    arr = np.array(im)
    fangs, wing_gold = gm_fang_masks(arr)
    # cover each fang plus its whole outline ring, but protect the full
    # outline thickness (~8px) around the wing/crown gold so the badge's
    # own contour is never nibbled; any leftover sliver of the fang's
    # outline simply merges into the wing outline it was attached to
    erase = ndimage.binary_dilation(np.logical_or.reduce(fangs), iterations=12)
    erase &= ~ndimage.binary_dilation(wing_gold, iterations=8)
    out = arr.copy()
    out[erase] = 0
    return Image.fromarray(out)


def poly_d(pts):
    parts = [f"M{pts[0][0]:.1f} {pts[0][1]:.1f}"]
    parts += [f"L{x:.1f} {y:.1f}" for x, y in pts[1:]]
    return "".join(parts) + "Z"


def gm_center_fang_shapes(cx=100.0, top=148.0, hw=21.0, bottom=185.0, point=14.0,
                          o=6.0, hw_bot=None):
    """A symmetric hanging center fang — deliberately chunkier than the side
    fangs so it holds its own against the big wing chevron above it.

    `hw_bot` tapers the sides inward toward the tip. A wide fang needs the
    taper: with vertical sides a 2x-wide element becomes wider than tall and
    stops reading as a tooth (it reads as a tray). Converging sides plus a
    deep point keep the fang silhouette at any width.

    Prepended BEFORE the traced badge shapes so its top tucks under the
    crown's central tip outline, the same way the side fangs tuck into the
    wing notches.
    """
    c = GM_FANG_COLORS
    hb = hw if hw_bot is None else hw_bot
    # The outline caps the shoulder as well as the sides. Without the top
    # margin a fang wider than the crown V above it exposes an unstroked
    # gold edge — the only unstroked silhouette edge in the whole system.
    outline = [
        (cx - hw - o, top - o), (cx + hw + o, top - o),
        (cx + hb + o, bottom), (cx, bottom + point + o * 0.9), (cx - hb - o, bottom),
    ]
    left = [(cx - hw, top), (cx, top), (cx, bottom + point - o * 0.4), (cx - hb, bottom - o * 0.4)]
    right = [(cx, top), (cx + hw, top), (cx + hb, bottom - o * 0.4), (cx, bottom + point - o * 0.4)]
    return [
        {"d": poly_d(outline), "fill": c["outline"]},
        {"d": poly_d(left), "fill": c["left"]},
        {"d": poly_d(right), "fill": c["right"]},
    ]


# ----------------------------------------------------------------- diamond --

DIA_BAR = {"fill": "#f2fbff", "outline": "#175a86"}


def diamond_bar_shapes(n, cy, cx=100.0, bar_w=16.0, gap=9.5, o=3.5):
    """N white numeral bars integrated into the gem, Black-Ops style.

    Each bar is a vertically symmetric lozenge — the same taper on top as on
    the bottom — sized to survive the 24px leaderboard render. The group
    extends upward toward the gem's ceiling; the middle bar runs the gem's
    full deep center column (~1.5x the original height) while flanking bars
    sit higher on the pavilion slope, shorter than the middle, so the
    numeral's silhouette follows the gem's own contour. The rim-collision
    gate in build_diamond_family enforces the fit.
    """
    # anchored in measured gem geometry: interior center column spans
    # y 42-165, pavilion halfwidth ~36 at y=124 shrinking ~0.83 per row
    ceiling = cy - 56.0
    point = 12.0
    pitch = bar_w + gap
    if n == 1:
        bars = [(cx, ceiling, cy + 39.0)]
    elif n == 2:
        bars = [(cx - pitch / 2, ceiling + 6.0, cy + 28.0),
                (cx + pitch / 2, ceiling + 6.0, cy + 28.0)]
    else:
        bars = [(cx - pitch, ceiling + 20.0, cy + 22.0),
                (cx, ceiling, cy + 42.0),
                (cx + pitch, ceiling + 20.0, cy + 22.0)]
    shapes = []
    for bx, top, bot in bars:
        hw = bar_w / 2

        def lozenge(hw_, top_, bot_, pt):
            return [
                (bx - hw_, top_ + pt), (bx, top_), (bx + hw_, top_ + pt),
                (bx + hw_, bot_ - pt), (bx, bot_), (bx - hw_, bot_ - pt),
            ]

        shapes.append({"d": poly_d(lozenge(hw + o, top - o, bot + o, point)),
                       "fill": DIA_BAR["outline"]})
        shapes.append({"d": poly_d(lozenge(hw, top, bot, point)),
                       "fill": DIA_BAR["fill"]})
    return shapes


# ------------------------------------------------------------------ common --

def trace_pil(im):
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
        im.save(f.name)
        p = f.name
    try:
        return trace_icon(p)
    finally:
        os.unlink(p)


NUM = r"(-?\d+(?:\.\d+)?)"


def transform_def(data, s, tx, ty):
    def tp(x, y):
        return round(x * s + tx, 1), round(y * s + ty, 1)

    def fmt(v):
        t = f"{v:.1f}"
        return t[:-2] if t.endswith(".0") else t

    out = {"gradients": [], "shapes": []}
    for g in data["gradients"]:
        x1, y1 = tp(g["x1"], g["y1"])
        x2, y2 = tp(g["x2"], g["y2"])
        out["gradients"].append({**g, "x1": x1, "y1": y1, "x2": x2, "y2": y2})
    for sh in data["shapes"]:
        d = re.sub(
            rf"([ML]){NUM} {NUM}",
            lambda m: (lambda p: f"{m.group(1)}{fmt(p[0])} {fmt(p[1])}")(
                tp(float(m.group(2)), float(m.group(3)))
            ),
            sh["d"],
        )
        out["shapes"].append({**sh, "d": d})
    return out


def bounds(data):
    xs, ys = [], []
    for sh in data["shapes"]:
        for m in re.finditer(rf"[ML]{NUM} {NUM}", sh["d"]):
            xs.append(float(m.group(1)))
            ys.append(float(m.group(2)))
    return min(xs), min(ys), max(xs), max(ys)


def build_metals_family(tier):
    im = Image.open(os.path.join(SRC, f"{tier}.png")).convert("RGBA")
    cut = CUTS[tier]
    d1 = trace_pil(erase_slice(im, cut))
    d2 = trace_icon(os.path.join(SRC, f"{tier}.png"))
    d3 = trace_pil(add_tapered_slice(im, cut))
    # division 1: recenter the lone chevron vertically; 2 stays exactly put;
    # 3 keeps every chevron at native size and just slides up to fit
    x0, y0, x1, y1 = bounds(d1)
    d1 = transform_def(d1, 1.0, 0.0, round(100 - (y0 + y1) / 2, 1))
    x0, y0, x1, y1 = bounds(d3)
    if y1 > 194:
        d3 = transform_def(d3, 1.0, 0.0, round(194 - y1, 1))
    return {1: d1, 2: d2, 3: d3}


def build_plat_family():
    im = Image.open(os.path.join(SRC, "platinum.png")).convert("RGBA")
    cut = CUTS["platinum"]
    d1 = trace_pil(erase_slice(im, cut))
    d2 = trace_icon(os.path.join(SRC, "platinum.png"))
    d3 = trace_pil(add_tapered_slice(im, cut))
    # platinum has no headroom: one uniform scale for the whole family,
    # sized by division 3, so the emblem is identical across divisions
    x0, y0, x1, y1 = bounds(d3)
    s = min(1.0, 184.0 / (y1 - y0), 184.0 / (x1 - x0))
    out = {}
    for div, d in [(1, d1), (2, d2), (3, d3)]:
        out[div] = transform_def(d, s, round(100 - s * (x0 + x1) / 2, 1), round(8 - s * y0, 1))
    return out


def diamond_interior_mask():
    """Blue facet interior of the gem (excludes the dark rim), eroded 2px."""
    from scipy import ndimage
    from trace_rank_icons import segment

    im = Image.open(os.path.join(SRC, "diamond.png")).convert("RGBA")
    a = np.array(im).astype(float)
    mask = a[..., 3] >= 128
    labels, n = segment(a[..., :3], mask)
    border = ndimage.binary_dilation(~mask, iterations=1) & mask
    ring = np.bincount(labels[border], minlength=n + 1).argmax()
    interior = (labels > 0) & (labels != ring)
    return ndimage.binary_erosion(ndimage.binary_fill_holes(interior), iterations=2)


def assert_bars_inside_gem(shapes, interior):
    """Objective gate: every numeral vertex must sit on gem facet, not rim."""
    for sh in shapes:
        for m in re.finditer(rf"[ML]{NUM} {NUM}", sh["d"]):
            x, y = float(m.group(1)), float(m.group(2))
            if not interior[int(round(y)), int(round(x))]:
                raise RuntimeError(
                    f"diamond numeral vertex ({x}, {y}) collides with the gem rim"
                )


def build_diamond_family():
    base = trace_icon(os.path.join(SRC, "diamond.png"))
    _, y0, _, y1 = bounds(base)
    cy = round((y0 + y1) / 2, 1)
    interior = diamond_interior_mask()
    out = {}
    for n in [1, 2, 3]:
        bars = diamond_bar_shapes(n, cy)
        assert_bars_inside_gem(bars, interior)
        out[n] = {"gradients": base["gradients"], "shapes": base["shapes"] + bars}
    return out


def build_gm_family():
    im = Image.open(os.path.join(SRC, "grandmaster.png")).convert("RGBA")
    base = trace_icon(os.path.join(SRC, "grandmaster.png"))
    solo = trace_pil(gm_erase_side_fangs(im))
    # GM 1's lone fang is GM 3's center fang, only wider: same trapezoidal
    # body, same tip depth (so division I never out-sizes division III),
    # top tucked behind the wing band so no shoulder ever shows. Its bottom
    # notch is cut at the badge's own chevron angle, which puts the outer
    # corners higher than a fixed point depth would — the notch reads as
    # parallel to the chevron above it rather than as a shallow tray.
    solo_point = GM1_FANG_HW * BADGE_CHEVRON_SLOPE
    fang_solo = gm_center_fang_shapes(
        hw=GM1_FANG_HW, top=GM1_FANG_TOP,
        bottom=GM_FANG_TIP_Y - solo_point, point=solo_point,
    )
    fang_trio = gm_center_fang_shapes()
    d1 = {"gradients": solo["gradients"], "shapes": fang_solo + solo["shapes"]}
    d2 = base
    d3 = {"gradients": base["gradients"], "shapes": fang_trio + base["shapes"]}
    # one uniform family scale sized by the union of all three divisions
    boxes = [bounds(d) for d in (d1, d2, d3)]
    x0 = min(b[0] for b in boxes)
    y0 = min(b[1] for b in boxes)
    x1 = max(b[2] for b in boxes)
    y1 = max(b[3] for b in boxes)
    s = min(1.0, 184.0 / (y1 - y0), 190.0 / (x1 - x0))
    out = {}
    for div, d in [(1, d1), (2, d2), (3, d3)]:
        out[div] = transform_def(d, s, round(100 - s * (x0 + x1) / 2, 1), round(8 - s * y0, 1))
    return out


# ---------------------------------------------------------------- emission --

def emit_ts(result):
    lines = [
        "// AUTO-GENERATED by client/design/tools/build_division_icons.py — do not edit by hand.",
        "//",
        "// Per-division rank badges, owner-specified family designs:",
        "// metals (bronze/silver/gold) count 1-3 tapered chevrons; platinum",
        "// counts crown + 1-3 chevrons; diamond integrates I/II/III numeral",
        "// bars into the gem; grandmaster counts 1-3 hanging fangs.",
        "// Division 2 of every raster family is the original tier art.",
        "",
        "import type { RankIconDefinition } from './rankIconData';",
        "",
        "export type RankIconDivisionTier =",
        "  | 'bronze'",
        "  | 'silver'",
        "  | 'gold'",
        "  | 'platinum'",
        "  | 'diamond'",
        "  | 'grandmaster';",
        "",
        "export const RANK_ICON_DIVISION_DATA: Record<",
        "  RankIconDivisionTier,",
        "  Record<number, RankIconDefinition>",
        "> = {",
    ]
    for tier, divs in result.items():
        lines.append(f"  {tier}: {{")
        for division, data in sorted(divs.items()):
            lines.append(f"    {division}: {{")
            lines.append("      gradients: [")
            for g in data["gradients"]:
                stops = ", ".join(
                    f"{{ offset: {s['offset']}, color: '{s['color']}' }}" for s in g["stops"]
                )
                lines.append(
                    f"        {{ id: '{g['id']}', x1: {g['x1']}, y1: {g['y1']}, "
                    f"x2: {g['x2']}, y2: {g['y2']}, stops: [{stops}] }},"
                )
            lines.append("      ],")
            lines.append("      shapes: [")
            for s in data["shapes"]:
                lines.append(f"        {{ d: '{s['d']}', fill: '{s['fill']}' }},")
            lines.append("      ],")
            lines.append("    },")
        lines.append("  },")
    lines.append("};")
    lines.append("")
    return "\n".join(lines)


def to_svg(data):
    defs = []
    for g in data["gradients"]:
        stops = "".join(
            f'<stop offset="{s["offset"]}" stop-color="{s["color"]}"/>' for s in g["stops"]
        )
        defs.append(
            f'<linearGradient id="{g["id"]}" gradientUnits="userSpaceOnUse" '
            f'x1="{g["x1"]}" y1="{g["y1"]}" x2="{g["x2"]}" y2="{g["y2"]}">{stops}</linearGradient>'
        )
    paths = "".join(
        f'<path d="{s["d"]}" fill="{s["fill"]}" stroke="{s["fill"]}" '
        f'stroke-width="0.8" stroke-linejoin="round" paint-order="stroke"/>'
        for s in data["shapes"]
    )
    return (
        '<svg xmlns="http://www.w3.org/2000/svg" width="200" height="200" '
        f'viewBox="0 0 200 200"><defs>{"".join(defs)}</defs>{paths}</svg>'
    )


def main():
    preview_dir = sys.argv[1] if len(sys.argv) > 1 else None
    result = {}
    for tier in ["bronze", "silver", "gold"]:
        result[tier] = build_metals_family(tier)
        print(f"{tier}: ok")
    result["platinum"] = build_plat_family()
    print("platinum: ok")
    result["diamond"] = build_diamond_family()
    print("diamond: ok")
    result["grandmaster"] = build_gm_family()
    print("grandmaster: ok")

    with open(DST, "w") as f:
        f.write(emit_ts(result))
    print(f"wrote {DST}")

    if preview_dir:
        os.makedirs(preview_dir, exist_ok=True)
        for tier, divs in result.items():
            for division, data in divs.items():
                with open(os.path.join(preview_dir, f"{tier}-{division}.svg"), "w") as f:
                    f.write(to_svg(data))
        print(f"previews in {preview_dir}")


if __name__ == "__main__":
    main()
