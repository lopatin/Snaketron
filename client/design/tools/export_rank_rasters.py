#!/usr/bin/env python3
"""Export PNG rasters of every rank badge at the sizes the UI renders.

The web client renders the badges as inline SVG (see RankIcon.tsx); these
rasters exist for surfaces that cannot use inline SVG — Open Graph and
social share cards, Discord/CrazyGames embeds, e-mail, and any native or
offline packaging. Regenerate them whenever the icon generators change.

Sizes come from the actual product usages: 16/24 px in leaderboard rows,
46 px for the post-match reveal medallion, 64 px for the leaderboard
summary badge, plus 128/256 px for share art. Each is emitted at 1x, 2x and
3x so HiDPI surfaces have a matching asset.

Reads   client/web/components/rankIconData.ts
        client/web/components/rankIconDivisionData.ts
Writes  client/web/public/images/ranks/{tier}-{division}@{scale}x.png
        client/web/public/images/ranks/{tier}@{scale}x.png   (tier only)
        client/web/public/images/ranks/manifest.json

Usage:  python3 client/design/tools/export_rank_rasters.py
Deps :  pillow, and Google Chrome (headless) for rasterization
"""
import json
import os
import re
import shutil
import subprocess
import tempfile

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))
COMPONENTS = os.path.join(ROOT, "client", "web", "components")
OUT_DIR = os.path.join(ROOT, "client", "web", "public", "images", "ranks")
CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

# Product sizes get the full DPI ladder; the large share sizes are already
# bigger than any screen renders them, so a single scale is enough there.
SIZE_SCALES = {
    16: [1, 2, 3],    # compact leaderboard / inline chips
    24: [1, 2, 3],    # leaderboard rows
    46: [1, 2, 3],    # post-match reveal medallion
    64: [1, 2, 3],    # leaderboard summary badge
    128: [1],         # share cards
    256: [1],         # share cards / store art
}


GRAD_RE = re.compile(
    r"\{ id: '(?P<id>[^']+)', x1: (?P<x1>[-\d.]+), y1: (?P<y1>[-\d.]+), "
    r"x2: (?P<x2>[-\d.]+), y2: (?P<y2>[-\d.]+), stops: \[(?P<stops>[^\]]*)\] \}"
)
STOP_RE = re.compile(r"\{ offset: ([-\d.]+), color: '([^']+)' \}")
SHAPE_RE = re.compile(r"\{ d: '(?P<d>[^']+)', fill: '(?P<fill>[^']+)' \}")


def block_defs(block):
    gradients = [
        {
            "id": m.group("id"),
            "x1": m.group("x1"), "y1": m.group("y1"),
            "x2": m.group("x2"), "y2": m.group("y2"),
            "stops": STOP_RE.findall(m.group("stops")),
        }
        for m in GRAD_RE.finditer(block)
    ]
    shapes = [{"d": m.group("d"), "fill": m.group("fill")} for m in SHAPE_RE.finditer(block)]
    return {"gradients": gradients, "shapes": shapes}


def iter_blocks(src, indent):
    """Yield (key, body) for every `<indent><key>: {` ... matching `}` block.

    Brace-matched rather than regex-delimited: a regex that anchors on the
    surrounding newlines silently skips whichever block starts a body, which
    is exactly the failure mode this parser must not have.
    """
    pad = " " * indent
    for m in re.finditer(rf"^{pad}(\w+): \{{$", src, re.M):
        key = m.group(1)
        depth, i = 0, m.end() - 1
        while i < len(src):
            if src[i] == "{":
                depth += 1
            elif src[i] == "}":
                depth -= 1
                if depth == 0:
                    break
            i += 1
        yield key, src[m.end():i]


def parse_icon_module(path, kind):
    """Pull the generated icon definitions back out of the TS module.

    The generators emit a fixed, machine-written shape, so parsing them back
    keeps the exporter dependency-free (no node/bundler needed).
    """
    src = open(path).read()
    out = {}
    if kind == "tier":
        for tier, body in iter_blocks(src, 2):
            out[tier] = block_defs(body)
    else:
        for tier, body in iter_blocks(src, 2):
            out[tier] = {
                int(div): block_defs(inner) for div, inner in iter_blocks(body, 4)
            }
    return out


def to_svg(defn, px, uid=""):
    """Render one icon. `uid` namespaces the gradient ids: several icons are
    rasterized on a single page, and identical ids would make every icon
    resolve the first one's gradients."""
    def local(fill):
        return fill.replace("url(#", f"url(#{uid}") if fill.startswith("url(#") else fill

    def gradient(g):
        stops = "".join(f'<stop offset="{o}" stop-color="{c}"/>' for o, c in g["stops"])
        return (
            f'<linearGradient id="{uid}{g["id"]}" gradientUnits="userSpaceOnUse" '
            f'x1="{g["x1"]}" y1="{g["y1"]}" x2="{g["x2"]}" y2="{g["y2"]}">{stops}</linearGradient>'
        )

    defs = "".join(gradient(g) for g in defn["gradients"])
    paths = "".join(
        f'<path d="{s["d"]}" fill="{local(s["fill"])}" stroke="{local(s["fill"])}" '
        f'stroke-width="0.8" stroke-linejoin="round" paint-order="stroke"/>'
        for s in defn["shapes"]
    )
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{px}" height="{px}" '
        f'viewBox="0 0 200 200"><defs>{defs}</defs>{paths}</svg>'
    )


def rasterize(jobs):
    """Rasterize (name, svg, px) jobs, one headless Chrome pass per size."""
    from PIL import Image

    by_size = {}
    for name, svg, px in jobs:
        by_size.setdefault(px, []).append((name, svg))

    for px, items in sorted(by_size.items()):
        with tempfile.TemporaryDirectory() as tmp:
            page = (
                "<!doctype html><style>body{margin:0;background:transparent}"
                "svg{display:block}</style>"
                + "".join(svg for _, svg in items)
            )
            html = os.path.join(tmp, "sheet.html")
            open(html, "w").write(page)
            shot = os.path.join(tmp, "sheet.png")
            subprocess.run(
                [CHROME, "--headless", "--disable-gpu", "--default-background-color=00000000",
                 f"--screenshot={shot}", f"--window-size={px},{px * len(items)}",
                 "--hide-scrollbars", f"file://{html}"],
                check=True, capture_output=True, timeout=300,
            )
            sheet = Image.open(shot).convert("RGBA")
            for i, (name, _) in enumerate(items):
                sheet.crop((0, i * px, px, (i + 1) * px)).save(os.path.join(OUT_DIR, name))
        print(f"  rasterized {len(items)} icons at {px}px")


def main():
    tiers = parse_icon_module(os.path.join(COMPONENTS, "rankIconData.ts"), "tier")
    divs = parse_icon_module(os.path.join(COMPONENTS, "rankIconDivisionData.ts"), "division")

    # Validate the parse rather than trusting it: a partial parse would
    # silently ship an incomplete asset set.
    expected_tiers = {"bronze", "silver", "gold", "platinum", "diamond", "grandmaster", "unranked"}
    if set(tiers) != expected_tiers:
        raise RuntimeError(f"tier parse mismatch: got {sorted(tiers)}")
    if set(divs) != expected_tiers - {"unranked"}:
        raise RuntimeError(f"division tier parse mismatch: got {sorted(divs)}")
    for tier, by_div in divs.items():
        if sorted(by_div) != [1, 2, 3]:
            raise RuntimeError(f"{tier}: expected divisions [1, 2, 3], got {sorted(by_div)}")
    for tier, defn in list(tiers.items()) + [
        (f"{t}-{d}", v) for t, bd in divs.items() for d, v in bd.items()
    ]:
        if not defn["shapes"]:
            raise RuntimeError(f"{tier}: parsed zero shapes")

    if os.path.isdir(OUT_DIR):
        shutil.rmtree(OUT_DIR)
    os.makedirs(OUT_DIR)

    jobs = []
    manifest = {
        "note": (
            "Rasters for surfaces that cannot inline SVG (share cards, embeds). "
            "The web client renders these badges as inline SVG via RankIcon."
        ),
        "sizes": {str(k): v for k, v in SIZE_SCALES.items()},
        "icons": {},
    }
    for tier, defn in tiers.items():
        for base, scales in SIZE_SCALES.items():
            for scale in scales:
                px = base * scale
                name = f"{tier}-{base}@{scale}x.png"
                jobs.append((name, to_svg(defn, px, uid=f"u{len(jobs)}_"), px))
        manifest["icons"][tier] = {"divisions": []}
    for tier, by_div in divs.items():
        for division, defn in sorted(by_div.items()):
            for base, scales in SIZE_SCALES.items():
                for scale in scales:
                    px = base * scale
                    name = f"{tier}-{division}-{base}@{scale}x.png"
                    jobs.append((name, to_svg(defn, px, uid=f"u{len(jobs)}_"), px))
            manifest["icons"].setdefault(tier, {"divisions": []})
            manifest["icons"][tier]["divisions"].append(division)

    print(f"exporting {len(jobs)} rasters into {OUT_DIR}")
    rasterize(jobs)
    with open(os.path.join(OUT_DIR, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)

    total = sum(
        os.path.getsize(os.path.join(OUT_DIR, f)) for f in os.listdir(OUT_DIR)
    )
    print(f"wrote {len(os.listdir(OUT_DIR))} files, {total / 1024:.0f} KB total")


if __name__ == "__main__":
    main()
