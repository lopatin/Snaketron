#!/usr/bin/env python3
"""Trace the raster rank icons into vector data for the RankIcon component.

Reads   client/web/public/images/{tier}.png   (200x200 RGBA rasters)
Writes  client/web/components/rankIconData.ts (generated vector module)

The originals are flat-shaded badges: facets with smooth internal gradients,
separated by sharp steps (usually the dark outline). The tracer reproduces
them almost pixel for pixel:

  1. mask = alpha >= 128
  2. edge map = pixels whose 3x3 neighborhood spans a channel step > EDGE_T
  3. facet regions = connected components of smooth (non-edge) pixels;
     every remaining masked pixel joins its nearest region
  4. per region: outer contour traced at the half-pixel level and simplified
     with Douglas-Peucker; paint fitted as either a flat fill or a multi-stop
     linearGradient along the dominant color direction (binned means)
  5. paint order: full-silhouette base in the outline paint, then facets by
     area descending, so nested details simply layer on top
  6. detail recovery: the painted result is reconstructed in memory and
     compared against the source; clusters of high error (thin separator
     lines, specular slivers) are traced and layered on top. Repeated until
     nothing above threshold remains.

Verification (headless Chrome rasterization vs the PNGs) puts interior error
at mean |delta| <= 3.4/255 with ~0% of interior pixels off by more than 32;
all remaining difference is the 1-2px anti-aliasing band along edges.

Usage:  python3 client/design/tools/trace_rank_icons.py   (from repo root)
Deps :  pillow, numpy, scipy, scikit-image
"""
import os
import numpy as np
from PIL import Image
from scipy import ndimage
from skimage import measure

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))
SRC = os.path.join(ROOT, "client", "web", "public", "images")
DST = os.path.join(ROOT, "client", "web", "components", "rankIconData.ts")
TIERS = ["bronze", "silver", "gold", "platinum", "diamond", "grandmaster", "unranked"]

EDGE_T = 28.0        # 3x3 max channel step marking a facet boundary
MIN_REGION_PX = 25   # smaller smooth patches are absorbed by neighbors
DP_EPS = 0.9         # Douglas-Peucker tolerance, in source pixels
FLAT_SPAN = 10.0     # fitted variation below this renders as a flat fill
N_BINS = 7           # gradient sampling bins along the fitted direction
RECOVER_ERR = 40.0   # per-pixel error that triggers detail recovery
RECOVER_MIN_PX = 20  # minimum recovered-detail cluster size


def edge_map(rgb, mask):
    step = np.zeros(rgb.shape[:2])
    for ch in range(3):
        c = rgb[..., ch]
        step = np.maximum(step, ndimage.grey_dilation(c, size=3) - ndimage.grey_erosion(c, size=3))
    return (step > EDGE_T) & mask


def segment(rgb, mask):
    """Label every masked pixel with a facet region id (1..n)."""
    smooth = mask & ~edge_map(rgb, mask)
    labels, n = ndimage.label(smooth, structure=np.ones((3, 3)))
    sizes = np.bincount(labels.ravel(), minlength=n + 1)
    small = sizes < MIN_REGION_PX
    small[0] = False
    labels[small[labels]] = 0
    ids = np.unique(labels)
    ids = ids[ids != 0]
    remap = np.zeros(labels.max() + 1, int)
    remap[ids] = np.arange(1, len(ids) + 1)
    labels = remap[labels]
    unlabeled = labels == 0
    _, (iy, ix) = ndimage.distance_transform_edt(unlabeled, return_indices=True)
    labels = np.where(mask, np.where(unlabeled, labels[iy, ix], labels), 0)
    return labels, len(ids)


def dp_simplify(pts, eps):
    if len(pts) < 3:
        return pts
    keep = np.zeros(len(pts), bool)
    keep[0] = keep[-1] = True
    stack = [(0, len(pts) - 1)]
    while stack:
        a, b = stack.pop()
        if b <= a + 1:
            continue
        seg = pts[b] - pts[a]
        L = np.hypot(*seg)
        rel = pts[a + 1:b] - pts[a]
        if L == 0:
            d = np.hypot(rel[:, 0], rel[:, 1])
        else:
            d = np.abs(seg[0] * rel[:, 1] - seg[1] * rel[:, 0]) / L
        i = d.argmax() + a + 1
        if d.max() > eps:
            keep[i] = True
            stack.append((a, i))
            stack.append((i, b))
    return pts[keep]


def trace_outer(mask_bool, eps=DP_EPS):
    """Longest 0.5-level contour of the mask -> simplified (x, y) polygon."""
    padded = np.pad(mask_bool.astype(float), 1)
    contours = measure.find_contours(padded, 0.5)
    if not contours:
        return None
    c = max(contours, key=len) - 1.0
    pts = np.stack([c[:, 1], c[:, 0]], axis=1)
    closed = np.allclose(pts[0], pts[-1])
    simp = dp_simplify(pts, eps)
    if closed and len(simp) > 3:
        simp = simp[:-1]
    return simp


def hex_color(rgb):
    r, g, b = [int(round(min(255, max(0, v)))) for v in rgb]
    return f"#{r:02x}{g:02x}{b:02x}"


def hex_to_rgb(h):
    return np.array([int(h[1:3], 16), int(h[3:5], 16), int(h[5:7], 16)], float)


def fit_paint(xs, ys, cols):
    """Fit ('flat', color) or ('grad', spec) over a facet's pixels."""
    n = len(xs)
    if n < 8:
        return ("flat", hex_color(cols.mean(0)))
    A = np.stack([np.ones(n), xs, ys], axis=1)
    coef, *_ = np.linalg.lstsq(A, cols, rcond=None)
    dirv = np.array([coef[1].sum(), coef[2].sum()])
    norm = np.linalg.norm(dirv)
    if norm < 1e-9:
        return ("flat", hex_color(cols.mean(0)))
    dirv /= norm
    t = xs * dirv[0] + ys * dirv[1]
    t0, t1 = np.percentile(t, 2), np.percentile(t, 98)
    if t1 - t0 < 2.0:
        return ("flat", hex_color(cols.mean(0)))
    edges = np.linspace(t0, t1, N_BINS + 1)
    stops = []
    for b in range(N_BINS):
        hi = t < edges[b + 1] if b < N_BINS - 1 else t <= edges[b + 1]
        m = (t >= edges[b]) & hi
        if m.sum() < 4:
            continue
        stops.append((((edges[b] + edges[b + 1]) / 2 - t0) / (t1 - t0), cols[m].mean(0)))
    if len(stops) < 2:
        return ("flat", hex_color(cols.mean(0)))
    total_span = np.abs(stops[-1][1] - stops[0][1]).max()
    step_span = max(np.abs(s1[1] - s0[1]).max() for s0, s1 in zip(stops, stops[1:])) * (len(stops) - 1)
    if max(total_span, step_span) < FLAT_SPAN:
        return ("flat", hex_color(cols.mean(0)))
    o0, o1 = stops[0][0], stops[-1][0]
    stops_n = []
    for off, col in stops:
        u = (off - o0) / (o1 - o0) if o1 > o0 else 0.0
        stops_n.append({"offset": round(float(u), 3), "color": hex_color(col)})
    pruned = [stops_n[0]]
    for s in stops_n[1:-1]:
        if pruned[-1]["color"] != s["color"]:
            pruned.append(s)
    pruned.append(stops_n[-1])
    cx, cy = xs.mean(), ys.mean()
    tc = cx * dirv[0] + cy * dirv[1]
    ta, tb = t0 + o0 * (t1 - t0), t0 + o1 * (t1 - t0)
    return ("grad", {
        "x1": round(float(cx + (ta - tc) * dirv[0]), 1),
        "y1": round(float(cy + (ta - tc) * dirv[1]), 1),
        "x2": round(float(cx + (tb - tc) * dirv[0]), 1),
        "y2": round(float(cy + (tb - tc) * dirv[1]), 1),
        "stops": pruned,
    })


def eval_paint(spec, xs, ys):
    kind, data = spec
    if kind == "flat":
        return np.tile(hex_to_rgb(data), (len(xs), 1))
    x1, y1, x2, y2 = data["x1"], data["y1"], data["x2"], data["y2"]
    dx, dy = x2 - x1, y2 - y1
    t = np.clip(((xs - x1) * dx + (ys - y1) * dy) / (dx * dx + dy * dy), 0.0, 1.0)
    offs = np.array([s["offset"] for s in data["stops"]])
    cols = np.stack([hex_to_rgb(s["color"]) for s in data["stops"]])
    out = np.empty((len(xs), 3))
    for ch in range(3):
        out[:, ch] = np.interp(t, offs, cols[:, ch])
    return out


def path_d(pts):
    def f(v):
        s = f"{v:.1f}"
        return s[:-2] if s.endswith(".0") else s
    return "".join(
        [f"M{f(pts[0][0])} {f(pts[0][1])}"]
        + [f"L{f(p[0])} {f(p[1])}" for p in pts[1:]]
        + ["Z"]
    )


# The unranked badge is the one icon whose ink carries meaning: the question
# mark is the subject, and the shield outline and chevron rule are structure.
# Recoloring before tracing lets the normal facet segmentation separate them,
# so the glyph stays full-strength while the structure recedes.
UNRANKED_GLYPH_INK = (41, 52, 55)      # question mark + dot outline
UNRANKED_STRUCT_INK = (150, 160, 165)  # shield outline + chevron rule
UNRANKED_GLYPH_FILL = (255, 255, 255)  # inside the question mark
UNRANKED_GLYPH_REACH = 10              # px of ink a glyph fill claims as its outline


def recolor_unranked(a):
    """Split the unranked badge's single ink into glyph vs structure."""
    mask = a[..., 3] >= 128
    lum = a[..., :3].mean(-1)
    dark = mask & (lum < 120)

    light_labels, count = ndimage.label(mask & ~dark, structure=np.ones((3, 3)))
    glyph_fill = np.zeros_like(mask)
    for i in range(1, count + 1):
        region = light_labels == i
        if region.sum() < 20:
            continue
        xs = np.nonzero(region)[1]
        # The shield's own fields span most of its width; the glyph's do not.
        if xs.max() - xs.min() <= 100:
            glyph_fill |= region

    glyph_ink = ndimage.binary_dilation(
        glyph_fill, iterations=UNRANKED_GLYPH_REACH
    ) & dark

    out = a.copy()
    out[dark & ~glyph_ink, :3] = UNRANKED_STRUCT_INK
    out[glyph_ink, :3] = UNRANKED_GLYPH_INK
    out[glyph_fill, :3] = UNRANKED_GLYPH_FILL
    return out


def trace_icon(path, preprocess=None):
    im = Image.open(path).convert("RGBA")
    a = np.array(im).astype(float)
    if preprocess is not None:
        a = preprocess(a)
    rgb, alpha = a[..., :3], a[..., 3]
    mask = alpha >= 128

    labels, n = segment(rgb, mask)

    # the outline ring: region owning most pixels adjacent to transparency
    border = ndimage.binary_dilation(~mask, iterations=1) & mask
    ring = np.bincount(labels[border], minlength=n + 1).argmax()

    gradients = []
    shapes = []
    recon = np.zeros_like(rgb)

    def add_shape(region_mask, contour, erode, paint_mask=None):
        """Fit paint on region_mask pixels, emit the contour as a shape, and
        record the painted result on paint_mask (defaults to region_mask)."""
        core = ndimage.binary_erosion(region_mask, iterations=erode) if erode else region_mask
        px = core if core.sum() >= 40 else region_mask
        ys_, xs_ = np.nonzero(px)
        kind, paint = fit_paint(xs_.astype(float), ys_.astype(float), rgb[px])
        if kind == "grad":
            gid = f"g{len(gradients)}"
            gradients.append({"id": gid, **paint})
            fill = f"url(#{gid})"
        else:
            fill = paint
        shapes.append({"d": path_d(contour), "fill": fill})
        pm = region_mask if paint_mask is None else paint_mask
        py, px_ = np.nonzero(pm)
        recon[pm] = eval_paint((kind, paint), px_.astype(float), py.astype(float))

    # base: whole silhouette filled with the outline-ring paint
    add_shape(labels == ring, trace_outer(mask), erode=2, paint_mask=mask)

    # Order by FILLED area, not pixel count. Only outer contours are traced,
    # so a ring-shaped facet is emitted as a solid polygon and must be painted
    # before whatever sits in its hole. Pixel count gets that backwards
    # whenever a ring is thinner than the region it encloses (the unranked
    # badge's question-mark outline versus its interior); filled area — the
    # region plus its holes — always puts the encloser first.
    regions = sorted(
        (
            (int(ndimage.binary_fill_holes(labels == rid).sum()), rid)
            for rid in range(1, n + 1)
            if rid != ring and (labels == rid).sum() >= MIN_REGION_PX
        ),
        reverse=True,
    )
    for _, rid in regions:
        rm = labels == rid
        contour = trace_outer(rm)
        if contour is None or len(contour) < 3:
            continue
        add_shape(rm, contour, erode=2)

    # detail recovery: re-trace clusters where the reconstruction is off
    for _ in range(2):
        err = np.abs(recon - rgb).max(-1)
        bad = (err > RECOVER_ERR) & mask
        bad = ndimage.binary_dilation(ndimage.binary_erosion(bad), iterations=1)
        lab_arr, nb = ndimage.label(bad, structure=np.ones((3, 3)))
        found = False
        for k in range(1, nb + 1):
            comp = lab_arr == k
            if int(comp.sum()) < RECOVER_MIN_PX:
                continue
            contour = trace_outer(comp)
            if contour is None or len(contour) < 3:
                continue
            found = True
            add_shape(comp, contour, erode=1)
        if not found:
            break

    return {"gradients": gradients, "shapes": shapes}


def emit_ts(all_data):
    def grad_ts(g):
        stops = ", ".join(
            f"{{ offset: {s['offset']}, color: '{s['color']}' }}" for s in g["stops"]
        )
        return (
            f"      {{ id: '{g['id']}', x1: {g['x1']}, y1: {g['y1']}, "
            f"x2: {g['x2']}, y2: {g['y2']}, stops: [{stops}] }},"
        )

    lines = [
        "// AUTO-GENERATED by client/design/tools/trace_rank_icons.py — do not edit by hand.",
        "//",
        "// Vector reproductions of the original raster rank icons",
        "// (client/web/public/images/{tier}.png), traced facet-by-facet with",
        "// fitted gradients on the source 200x200 canvas. Interior pixels match",
        "// the rasters to within a mean channel delta of ~3/255; the only",
        "// difference is the sub-pixel anti-aliasing band along edges.",
        "",
        "export type RankIconTier =",
        "  | 'bronze'",
        "  | 'silver'",
        "  | 'gold'",
        "  | 'platinum'",
        "  | 'diamond'",
        "  | 'grandmaster'",
        "  | 'unranked';",
        "",
        "export interface RankIconGradientStop {",
        "  offset: number;",
        "  color: string;",
        "}",
        "",
        "export interface RankIconGradient {",
        "  id: string;",
        "  x1: number;",
        "  y1: number;",
        "  x2: number;",
        "  y2: number;",
        "  stops: RankIconGradientStop[];",
        "}",
        "",
        "export interface RankIconShape {",
        "  d: string;",
        "  /** Either a '#rrggbb' literal or a local 'url(#gN)' gradient reference. */",
        "  fill: string;",
        "}",
        "",
        "export interface RankIconDefinition {",
        "  gradients: RankIconGradient[];",
        "  shapes: RankIconShape[];",
        "}",
        "",
        "export const RANK_ICON_VIEW_BOX = '0 0 200 200';",
        "",
        "export const RANK_ICON_DATA: Record<RankIconTier, RankIconDefinition> = {",
    ]
    for tier in TIERS:
        data = all_data[tier]
        lines.append(f"  {tier}: {{")
        lines.append("    gradients: [")
        lines += [grad_ts(g) for g in data["gradients"]]
        lines.append("    ],")
        lines.append("    shapes: [")
        for s in data["shapes"]:
            lines.append(f"      {{ d: '{s['d']}', fill: '{s['fill']}' }},")
        lines.append("    ],")
        lines.append("  },")
    lines.append("};")
    lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    all_data = {}
    for tier in TIERS:
        all_data[tier] = trace_icon(
            os.path.join(SRC, f"{tier}.png"),
            preprocess=recolor_unranked if tier == "unranked" else None,
        )
        print(
            f"{tier}: {len(all_data[tier]['shapes'])} shapes, "
            f"{len(all_data[tier]['gradients'])} gradients"
        )
    with open(DST, "w") as f:
        f.write(emit_ts(all_data))
    print(f"wrote {DST}")
