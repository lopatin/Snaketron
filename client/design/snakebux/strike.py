"""Strike the Snaketron letterform into a generated coin blank.

Gemini draws the metal — the gold, the rim, the white glare — and will not
reproduce an exact letterform on request, so the letter is struck here instead,
from the outline traced off the favicon. That keeps the mark identical to the
one the site already uses at every size, which a model redrawing it each time
could not promise.

Two renderings, not one scaled twice. The detailed coin keeps the double rim
and a fine bevel, both of which turn to mud below about forty pixels. The small
one is drawn for twenty: the face is cut out of the blank inside its inner
ring, a single heavy rim is drawn around it, and the letter is thickened, so
what survives the downscale is one gold disc, one dark edge, one white glare
and one bright letter.
"""
import json
import sys
from PIL import Image, ImageDraw, ImageFilter

SUPERSAMPLE = 4


def keyed(path):
    """The coin, cut off the flat magenta the model was asked to sit it on."""
    im = Image.open(path).convert('RGBA')
    pixels = im.load()
    width, height = im.size
    for y in range(height):
        for x in range(width):
            r, g, b, _ = pixels[x, y]
            # Magenta is red and blue high with green low; nothing on the coin
            # comes close, so this separates cleanly without a tolerance dance.
            if r > 150 and b > 120 and g < 110 and (r - g) > 70 and (b - g) > 40:
                pixels[x, y] = (0, 0, 0, 0)
    return im.crop(im.getbbox())


def disc_radius(im):
    """Outer radius of the coin, from the keyed silhouette."""
    return min(im.width, im.height) / 2


def face_only(im, inset):
    """Just the coin's face, cut inside whatever rings the model drew."""
    size = min(im.width, im.height)
    radius = size / 2 * inset
    centre = (im.width / 2, im.height / 2)
    mask = Image.new('L', im.size, 0)
    ImageDraw.Draw(mask).ellipse(
        [centre[0] - radius, centre[1] - radius, centre[0] + radius, centre[1] + radius],
        fill=255)
    out = Image.new('RGBA', im.size, (0, 0, 0, 0))
    out.paste(im, (0, 0), mask)
    return out.crop(out.getbbox())


def letter(size, ring, scale, colour, outline, outline_width):
    """The favicon's S, scaled into a square canvas."""
    xs = [p[0] for p in ring]
    ys = [p[1] for p in ring]
    span = max(max(xs) - min(xs), max(ys) - min(ys))
    factor = size * scale / span
    ox = (size - (max(xs) - min(xs)) * factor) / 2 - min(xs) * factor
    oy = (size - (max(ys) - min(ys)) * factor) / 2 - min(ys) * factor
    points = [(x * factor + ox, y * factor + oy) for x, y in ring]

    layer = Image.new('RGBA', (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(layer)
    if outline_width > 0:
        draw.polygon(points, fill=outline, outline=outline, width=outline_width)
        draw.line(points + [points[0]], fill=outline, width=outline_width, joint='curve')
    draw.polygon(points, fill=colour)
    return layer


def build(blank, out_path, size, *, inset, rim, letter_scale, letter_fill,
          letter_edge, edge_width, glare_boost):
    big = size * SUPERSAMPLE
    coin = keyed(blank)

    if inset is not None:
        coin = face_only(coin, inset)

    coin = coin.resize((big, big), Image.LANCZOS)

    canvas = Image.new('RGBA', (big, big), (0, 0, 0, 0))
    if rim > 0:
        # One rim, drawn rather than inherited: the model puts a second ring
        # inside the first every time, and two dark lines a pixel apart are one
        # muddy line at twenty pixels.
        pad = rim * SUPERSAMPLE
        body = coin.resize((big - pad * 2, big - pad * 2), Image.LANCZOS)
        ring_layer = Image.new('RGBA', (big, big), (0, 0, 0, 0))
        ImageDraw.Draw(ring_layer).ellipse([0, 0, big - 1, big - 1], fill=(20, 18, 14, 255))
        canvas.alpha_composite(ring_layer)
        canvas.alpha_composite(body, (pad, pad))
    else:
        canvas.alpha_composite(coin)

    if glare_boost:
        # Lift the model's own white sweep so it survives the downscale, taken
        # from the coin's bright pixels rather than painted fresh. Done before
        # the letter is struck, not after: the letter is nearly white itself, so
        # a mask built from brightness afterwards selects the letter too and
        # washes out the very thing the glare is there to sit behind.
        base = canvas.convert('RGB')
        pixels = base.load()
        glare = Image.new('L', (big, big), 0)
        gpx = glare.load()
        for y in range(big):
            for x in range(big):
                r, g, b = pixels[x, y]
                if r > 238 and g > 234 and b > 214:
                    gpx[x, y] = 255
        glare = glare.filter(ImageFilter.GaussianBlur(big / 110))
        white = Image.new('RGBA', (big, big), (255, 255, 255, 255))
        lifted = Image.composite(
            white, Image.new('RGBA', (big, big), (255, 255, 255, 0)), glare)
        # Only over the coin, never spilling past its edge.
        silhouette = canvas.getchannel('A')
        lifted.putalpha(Image.composite(
            lifted.getchannel('A'), Image.new('L', (big, big), 0), silhouette))
        canvas.alpha_composite(lifted)

    ring = json.load(open(f'{sys.path[0]}/s-ring.json'))
    mark = letter(big, ring, letter_scale, letter_fill, letter_edge,
                  edge_width * SUPERSAMPLE)
    canvas.alpha_composite(mark)

    canvas.resize((size, size), Image.LANCZOS).save(out_path, optimize=True)
    print(f'wrote {out_path} at {size}px')


if __name__ == '__main__':
    here = sys.path[0]
    build(f'{here}/blank-hi.png', f'{here}/snake-bux.png', 256,
          inset=None, rim=0, letter_scale=0.50,
          letter_fill=(255, 250, 224, 255), letter_edge=(104, 62, 4, 255),
          edge_width=2, glare_boost=False)
    build(f'{here}/blank-lo.png', f'{here}/snake-bux-small.png', 64,
          inset=0.845, rim=3, letter_scale=0.54,
          letter_fill=(255, 253, 238, 255), letter_edge=(88, 52, 2, 255),
          edge_width=2, glare_boost=True)
