#!/usr/bin/env python3
"""Build the endzone banners for the built-in base skins.

Writes  client/web/public/images/bases/<id>.{home,away}.v1.png
Usage   build_base_textures.py --theme dragon --side home --subject "a dragon ..."
        build_base_textures.py --theme dragon --side home --recrop
        build_base_textures.py --check [--theme dragon]
Deps    pillow, numpy; `OPENAI_API_KEY` in the environment, which on a dev box
        means running under `zsh -c 'source ~/.zshrc; ...'` -- generation only.
        `--check` and `--recrop` are offline.

Never hand-edit the PNGs. They are build output and this file is the source.

--------------------------------------------------------------------------
What a base banner is

**One picture, spanning the whole endzone.** Not a tile, not a pattern, not a
repeat -- the renderer lays it along the zone's long axis and cover-fits it
across the depth, and that is the only copy of it on screen.

An endzone is ten cells deep and forty long: a 4:1 strip. The widest thing the
image API will draw is 3:2, so cover-fitting keeps the banner's full length and
crops roughly the top and bottom third away. **Compose for that.** Everything
that matters belongs in a wide band across the middle; the top and bottom of
the frame are sky, ground, atmosphere -- material that can be lost without
taking the subject with it.

The strip is vertical on a landscape screen and horizontal on a portrait one,
and the renderer turns the banner a quarter turn for the first case rather than
squashing it. So the art rides the arena's own rotation, the way paint on a
pitch does.

An earlier version tiled a square four times down each endzone. It worked, and
you could see it working, which was the problem: four identical dragons in a
column read as wallpaper rather than as a banner. Everything that existed to
serve tiling -- the wrap requirement, the half-turn roll, the gradient seam
heal, the quiet-border rule that pushed every subject into a corner -- went
with it. The art got better the moment it stopped having to join to itself.

--------------------------------------------------------------------------
The two ends

Each skin ships **two** banners, home and away, for the end the viewer is
defending and the end they are attacking. They exist so a mirror match does not
have two identical ends: both teams can equip the same skin, and each viewer
then sees home at their own end and away at the other.

`--check` measures that the pair is actually distinguishable. It does **not**
dictate their colours. An earlier version made home cool and away warm, and
seventeen home kits came back as seventeen shades of blue. Which end is which
is carried by the goal wall, which the renderer paints in the viewer's own
colours and no art can override.

--------------------------------------------------------------------------
The one hard constraint

Value. Snakes are drawn on top of these and players score inside them, so a
banner that goes near black or near white swallows the game. `--check` enforces
a mean in 0.10-0.55 with the brightest 2% reaching 0.30.

Read that as a constraint on *value*, never on *saturation* -- and say so in
the prompt, because a model told to stay mid-tone will otherwise hand back
something grey and washed out. Loud, saturated colour at a mid value is exactly
what is wanted.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import urllib.request
from pathlib import Path

import numpy as np
from PIL import Image

# The stored banner. An endzone is about 720 device pixels long on a large
# screen, so 1024 is comfortable headroom and the file stays small enough to
# ship thirty-four of.
BANNER_WIDTH = 1024
BANNER_HEIGHT = 683  # 3:2, the widest the model draws

# The widest size the image API offers. The endzone wants 4:1 and this is 3:2,
# so the renderer crops the rest; see the note on composing for it above.
GENERATION_SIZE = "1536x1024"
MODEL = "gpt-image-2"

ROOT = Path(__file__).resolve().parents[2]
OUT_DIR = ROOT / "web" / "public" / "images" / "bases"

# The value band a picture has to live in. Snakes are painted on top of it and
# players score inside it, so this is a gameplay constraint wearing an
# art-direction hat -- and it is enforced twice, here and in
# `client/src/skin/base_skin.rs`, because only the second one runs in CI.
MIN_MEAN_LUMA = 0.10
MAX_MEAN_LUMA = 0.55
#: Where the brightest 2% of the picture has to reach.
MIN_TOP_LUMA = 0.30
#: Mean OKLCH chroma. A separate gate from value on purpose: told to stay
#: mid-tone, a model will reach for grey, and the first banners came back
#: washed-out blue and washed-out green. Saturation is free -- nothing about a
#: snake being readable requires the background to be drab.
#:
#: Calibrated against the batch that prompted this: the light-cycle grid a
#: player called "washed out blue" measured 0.034 and the dragon 0.056, while
#: the ones that read as having a palette were 0.10 and up.
MIN_CHROMA = 0.070

# Raw generations, kept out of the repository. Healing is cheap and rerunnable;
# generation costs money, so what the model returned is worth keeping around.
CACHE_DIR = Path(__file__).resolve().parent / ".cache" / "bases"

# --------------------------------------------------------------------------
# Prompting

#: The half of the prompt that is about the *surface*, not the subject. Every
#: theme gets it, so no theme has to remember the constraints that make a
#: picture usable as an endzone.
SURFACE_RULES = """
Format: a single wide banner illustration, one picture, edge to edge, no
borders and no framing. It is hung along the end of an arena, like the painted
hoardings behind a stadium goal, and it is the only copy of itself on screen --
so it is a composition, not a tile and not a pattern. Nothing in it repeats.

Composition for the crop: the banner is displayed as a long 4:1 strip, and this
canvas is 3:2, so roughly the top third and the bottom third are cropped away.
Every subject, every face, every readable shape must sit inside a wide band
across the middle of the frame; the top and bottom are sky, ground, water,
smoke -- atmosphere that can be lost without taking anything with it. Spread
the interest along the full width rather than stacking it up the height.

Art direction: this is production-quality game art with a personality, not a
texture swatch. Cartoon, vector, comic, pixel-art, painted, cel-shaded, poster
-- whatever suits the subject, but commit to it. Bold shapes, confident
outlines where the style calls for them, a clear focal subject and a real
sense of place. It is allowed to be funny, weird, loud, or over the top. What
it is not allowed to be is generic, tasteful-and-forgettable, or corporate.

Colour: saturated and characterful. Pick a palette that belongs to this subject
and commit to it -- a real scheme with a dominant hue and an accent that fights
it, not a wash. Do not desaturate, do not haze the picture over, do not render
it in one tint. Washed-out, dusty, muted and pastel are all wrong here.

Value, and this one is a hard constraint rather than a preference: keep the
whole picture in the middle of the value range. Nothing near white and nothing
near black across any large area, and no region so violently high-contrast that
a brightly coloured game piece crossing it would disappear. Bright colours at a
mid value -- saturated is not the same as light, and staying mid-tone is not a
reason to drain the colour out.

No text of any kind, no lettering, no logos, no signature, no watermark, no
UI, no frame, no vignette, no drop shadow under the whole image.
""".strip()

#: What the model is told about the *pair*, rather than about either picture.
#:
#: There is deliberately no palette rule here. An earlier version made home cool
#: and away warm, which enforced the friend/foe read at the cost of seventeen
#: home banners that were seventeen shades of blue -- the theme stopped being
#: the theme. What separates the two ends now is that they are two genuinely
#: different pictures, which is the caller's job to describe in the subject, and
#: the goal wall, which the renderer owns and paints in the viewer's own colours
#: regardless of what the art does.
PAIR_RULE = (
    "This banner is one of a matched pair that dress the two ends of the same "
    "arena. It must share the other one's subject, style, palette family and "
    "level of finish -- recognisably the same skin by the same hand -- while "
    "being obvious at a glance as the other picture rather than a recolour."
)


def build_prompt(subject: str) -> str:
    """One picture's subject, plus the rules every endzone picture obeys."""
    return f"{subject.strip()}\n\n{PAIR_RULE}\n\n{SURFACE_RULES}"


def generate(prompt: str, *, timeout: float = 300.0) -> Image.Image:
    """Ask the model for one picture. Raises with the API's own message."""
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise SystemExit("OPENAI_API_KEY is not set; generation needs it")

    request = urllib.request.Request(
        "https://api.openai.com/v1/images/generations",
        data=json.dumps(
            {
                "model": MODEL,
                "prompt": prompt,
                "size": GENERATION_SIZE,
                "n": 1,
            }
        ).encode(),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.load(response)
    except urllib.error.HTTPError as error:  # pragma: no cover - network
        detail = error.read().decode("utf-8", "replace")[:600]
        raise SystemExit(f"image generation failed ({error.code}): {detail}") from error

    encoded = payload["data"][0]["b64_json"]
    from io import BytesIO

    return Image.open(BytesIO(base64.b64decode(encoded))).convert("RGB")


# --------------------------------------------------------------------------
# Making it tile


def to_banner(image: Image.Image) -> Image.Image:
    """Store what the model returned, at the size the game ships.

    There used to be a seam-healing pass here — a half-turn roll and a
    gradient repair — because the picture was tiled four times down each
    endzone. It is not tiled any more, so there is no join to close and nothing
    to repair: the banner is resized and paletted and that is the whole of it.

    Paletted to 256 colours because thirty-four of these ship and a generated
    image is full of near-duplicate colours nobody can tell apart; undithered,
    because dithering is noise the downscale would only smear.
    """
    resized = image.resize((BANNER_WIDTH, BANNER_HEIGHT), Image.LANCZOS)
    return resized.quantize(colors=256, method=Image.MEDIANCUT, dither=Image.NONE)


# --------------------------------------------------------------------------
# Checks


def chroma(pixels: np.ndarray) -> float:
    """Mean OKLCH chroma, per pixel — how colourful the banner actually is.

    Averaged over pixels rather than taken from the mean colour: a picture that
    is half saturated orange and half saturated teal averages to mud, and it is
    not a drab picture.
    """
    data = pixels.astype(np.float64).reshape(-1, 3) / 255.0
    r, g, b = (_linear(data[:, index]) for index in range(3))
    long = np.cbrt(0.4122214708 * r + 0.5363325363 * g + 0.0514459929 * b)
    medium = np.cbrt(0.2119034982 * r + 0.6806995451 * g + 0.1073969566 * b)
    short = np.cbrt(0.0883024619 * r + 0.2817188376 * g + 0.6299787005 * b)
    axis_a = 1.9779984951 * long - 2.4285922050 * medium + 0.4505937099 * short
    axis_b = 0.0259040371 * long + 0.7827717662 * medium - 0.8086757660 * short
    return float(np.sqrt(axis_a**2 + axis_b**2).mean())


def _linear(channel: np.ndarray) -> np.ndarray:
    return np.where(channel <= 0.04045, channel / 12.92, ((channel + 0.055) / 1.055) ** 2.4)


def mean_oklab(pixels: np.ndarray) -> tuple[float, float, float]:
    """Mean colour in OKLab. Mirrors `skin_schema::color::Rgb::oklab`."""
    data = pixels.astype(np.float64).reshape(-1, 3).mean(axis=0) / 255.0
    r, g, b = (_linear(np.array([c]))[0] for c in data)
    l = np.cbrt(0.4122214708 * r + 0.5363325363 * g + 0.0514459929 * b)
    m = np.cbrt(0.2119034982 * r + 0.6806995451 * g + 0.1073969566 * b)
    s = np.cbrt(0.0883024619 * r + 0.2817188376 * g + 0.6299787005 * b)
    return (
        0.2104542553 * l + 0.7936177850 * m - 0.0040720468 * s,
        1.9779984951 * l - 2.4285922050 * m + 0.4505937099 * s,
        0.0259040371 * l + 0.7827717662 * m - 0.8086757660 * s,
    )


def luminance(pixels: np.ndarray) -> tuple[float, float]:
    """Mean WCAG luminance, and the 98th percentile.

    Two numbers because one is not enough. A mean alone passed a dragon cave
    that was black from corner to corner: at 0.04 it sat inside a 0.01 floor
    that was only ever meant to exclude a pure-black image. The percentile is
    what says the picture has a *bright end* — somewhere in it that a dark
    snake can be seen against — rather than merely averaging above nothing.
    """
    data = pixels.astype(np.float64) / 255.0
    channels = [_linear(data[..., index]) for index in range(3)]
    luma = 0.2126 * channels[0] + 0.7152 * channels[1] + 0.0722 * channels[2]
    return float(luma.mean()), float(np.percentile(luma, 98.0))


def check(out_dir: Path, only: str | None = None) -> list[str]:
    """Hold every committed picture to the rules the renderer relies on.

    The same checks run natively in `client/src/skin/base_skin.rs`; this is the
    copy an author runs while iterating, before a `cargo test` would tell them.
    """
    failures: list[str] = []
    pairs: dict[str, dict[str, np.ndarray]] = {}
    for path in sorted(out_dir.glob("*.v1.png")):
        theme, side, _ = path.name.split(".", 2)
        if only and theme != only:
            continue
        pairs.setdefault(theme, {})[side] = np.asarray(Image.open(path).convert("RGB"))
    if only and not pairs:
        return [f"{only}: nothing committed under {out_dir}"]

    for theme, sides in sorted(pairs.items()):
        for side, pixels in sorted(sides.items()):
            luma, top = luminance(pixels)
            saturation = chroma(pixels)
            print(
                f"{theme:12s} {side:4s}  luma {luma:.2f} top {top:.2f}  "
                f"chroma {saturation:.3f}"
            )
            if saturation < MIN_CHROMA:
                failures.append(
                    f"{theme}.{side}: chroma {saturation:.3f} is below {MIN_CHROMA}; "
                    "the banner has come back washed out"
                )
            if not MIN_MEAN_LUMA <= luma <= MAX_MEAN_LUMA:
                failures.append(
                    f"{theme}.{side}: mean luminance {luma:.2f} is outside "
                    f"{MIN_MEAN_LUMA}-{MAX_MEAN_LUMA}; snakes are drawn on this"
                )
            if top < MIN_TOP_LUMA:
                failures.append(
                    f"{theme}.{side}: even its brightest pixels only reach {top:.2f} "
                    f"(needs {MIN_TOP_LUMA}); the whole picture is in shadow"
                )

        if set(sides) != {"home", "away"}:
            failures.append(f"{theme}: has {sorted(sides)} rather than a home/away pair")
            continue

        # The two ends have to be tellable apart. Which is which is the goal
        # wall's job, not the art's -- see PAIR_RULE.
        home = mean_oklab(sides["home"])
        away = mean_oklab(sides["away"])
        distance = sum((a - b) ** 2 for a, b in zip(home, away)) ** 0.5
        print(f"{theme:12s}       distance {distance:.3f}")
        if distance < 0.10:
            failures.append(f"{theme}: the two ends are only {distance:.3f} apart")
    return failures


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--theme", help="the id this picture belongs to, e.g. `invaders`")
    parser.add_argument("--side", choices=("home", "away"))
    parser.add_argument("--subject", help="what the picture is of, in a sentence or two")
    parser.add_argument("--subject-file", type=Path, help="the same, read from a file")
    parser.add_argument(
        "--recrop",
        action="store_true",
        help="re-render the cached raw generation without asking the model again",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="verify what is committed; --theme narrows it to one pair",
    )
    parser.add_argument("--out", type=Path, default=OUT_DIR)
    parser.add_argument("--print-prompt", action="store_true")
    args = parser.parse_args()

    if args.check:
        failures = check(args.out, args.theme)
        if failures:
            raise SystemExit("\n".join(failures))
        return

    if not args.theme or not args.side:
        parser.error("--theme and --side are required unless --check is given")

    args.out.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    raw_path = CACHE_DIR / f"{args.theme}.{args.side}.raw.png"
    target = args.out / f"{args.theme}.{args.side}.v1.png"

    if args.recrop:
        if not raw_path.exists():
            raise SystemExit(f"no cached generation at {raw_path}")
        raw = Image.open(raw_path).convert("RGB")
    else:
        subject = args.subject
        if args.subject_file:
            subject = args.subject_file.read_text()
        if not subject:
            parser.error("--subject or --subject-file is required to generate")
        prompt = build_prompt(subject)
        if args.print_prompt:
            print(prompt)
        raw = generate(prompt)
        raw.save(raw_path)

    to_banner(raw).save(target, optimize=True)

    pixels = np.asarray(Image.open(target).convert("RGB"))
    luma, top = luminance(pixels)
    print(
        f"{target.relative_to(ROOT.parent)}  luma {luma:.2f} top {top:.2f}  "
        f"chroma {chroma(pixels):.3f}  ({target.stat().st_size // 1024} KiB)",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
