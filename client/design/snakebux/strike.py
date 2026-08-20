"""Strike the Snaketron letterform into a generated coin blank.

Gemini draws the metal — the gold and the white glare — and will not reproduce
an exact letterform on request, so the letter is struck here instead, from the
outline traced off the favicon. That keeps the mark identical to the one the
site already uses at every size, which a model redrawing it each time could not
promise.

The rings are drawn here too, for the same reason: asked for a single rim the
model returns a double one every time, and asked for a gold-gradient edge it
returns a black one. Both edges are therefore painted over whatever the model
produced, which also makes them exactly as thick as they need to be.

Two renderings, not one scaled twice. The detailed coin keeps a double rim and a
fine bevel, both of which turn to mud below about forty pixels. The small one is
drawn for twenty: one thin rim, one glare, a thicker brighter letter.
"""
import json
import math
import os
import sys

from PIL import Image, ImageDraw, ImageFilter

SUPERSAMPLE = 4

# Where the model puts its rings, as a fraction of the coin's radius. Measured
# off the blanks rather than guessed; both come out the same because they were
# generated from the same prompt family.
INNER_RING = (0.792, 0.840)
OUTER_EDGE = 0.963


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


def squared(im):
    """Centre the coin in a square canvas so radii mean what they say."""
    side = max(im.size)
    out = Image.new('RGBA', (side, side), (0, 0, 0, 0))
    out.paste(im, ((side - im.width) // 2, (side - im.height) // 2), im)
    return out


def face_only(im, inset):
    """Just the coin's face, cut inside whatever rings the model drew.

    Cropped to the disc it kept, not left in the original canvas: a caller that
    resizes this to fit inside a rim would otherwise be scaling the transparent
    margin too, insetting the face a second time and leaving a rim several
    times thicker than it asked for.
    """
    radius = im.width / 2 * inset
    centre = im.width / 2
    mask = Image.new('L', im.size, 0)
    ImageDraw.Draw(mask).ellipse(
        [centre - radius, centre - radius, centre + radius, centre + radius], fill=255)
    out = Image.new('RGBA', im.size, (0, 0, 0, 0))
    out.paste(im, (0, 0), mask)
    return out.crop(out.getbbox())


def sample(stops, t):
    """Colour at `t` along a list of (position, rgb) stops."""
    if t <= stops[0][0]:
        return stops[0][1]
    for (p0, c0), (p1, c1) in zip(stops, stops[1:]):
        if t <= p1:
            k = (t - p0) / (p1 - p0) if p1 > p0 else 0.0
            return tuple(round(c0[i] + (c1[i] - c0[i]) * k) for i in range(3))
    return stops[-1][1]


def linear_gradient(size, stops, degrees=135.0):
    """A straight ramp across the canvas, first stop first.

    The angle is the direction the ramp travels; 135 degrees runs from the
    top-left to the bottom-right, which is where the model puts its light, so
    an edge painted through this is bright where the coin is lit and deep where
    it is turned away — the thing that makes a flat ring read as a rim.

    Stops rather than two ends, because the two ends here are white and a deep
    brown-gold, and a straight line between those runs through grey. Passing
    the ramp through a saturated gold on the way keeps the edge looking like
    lit metal instead of a pencil line.
    """
    angle = math.radians(degrees)
    dx, dy = math.cos(angle), math.sin(angle)
    ramp = Image.new('RGB', (size, size))
    pixels = ramp.load()
    # Project each pixel onto the ramp direction and normalise to 0..1.
    span = abs(dx) * size + abs(dy) * size
    origin = (size * max(-dx, 0.0) + size * max(-dy, 0.0))
    # One row of the ramp per distinct projection, looked up rather than
    # recomputed per pixel.
    table = [sample(stops, i / 512.0) for i in range(513)]
    for y in range(size):
        for x in range(size):
            t = (x * dx + y * dy + origin) / span
            t = 0.0 if t < 0.0 else 1.0 if t > 1.0 else t
            pixels[x, y] = table[int(t * 512)]
    return ramp


def annulus(size, outer, inner):
    """A mask for the band between two radii, both as fractions of the radius."""
    mask = Image.new('L', (size, size), 0)
    draw = ImageDraw.Draw(mask)
    centre = size / 2
    for radius, ink in ((centre * outer, 255), (centre * inner, 0)):
        draw.ellipse([centre - radius, centre - radius,
                      centre + radius, centre + radius], fill=ink)
    return mask


def paint_ring(canvas, outer, inner, stops, degrees=135.0):
    """Lay a gradient band over the coin between two radii."""
    size = canvas.width
    ramp = linear_gradient(size, stops, degrees).convert('RGBA')
    band = annulus(size, outer, inner)
    # Never outside the coin's own silhouette, so a rim cannot bleed into the
    # transparent corners.
    band = Image.composite(band, Image.new('L', (size, size), 0), canvas.getchannel('A'))
    ramp.putalpha(band)
    canvas.alpha_composite(ramp)


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
        draw.line(points + [points[0]], fill=outline, width=outline_width, joint='curve')
    draw.polygon(points, fill=colour)
    return layer


def lift_glare(canvas, radius_divisor):
    """Strengthen the model's own white sweep so it survives the downscale.

    Taken from the coin's bright pixels rather than painted fresh, and done
    before the letter is struck: the letter is nearly white itself, so a mask
    built from brightness afterwards selects the letter too and washes out the
    very thing the glare is there to sit behind.
    """
    size = canvas.width
    base = canvas.convert('RGB')
    pixels = base.load()
    glare = Image.new('L', (size, size), 0)
    gpx = glare.load()
    for y in range(size):
        for x in range(size):
            r, g, b = pixels[x, y]
            if r > 238 and g > 234 and b > 214:
                gpx[x, y] = 255
    glare = glare.filter(ImageFilter.GaussianBlur(size / radius_divisor))
    white = Image.new('RGBA', (size, size), (255, 255, 255, 255))
    lifted = Image.composite(white, Image.new('RGBA', (size, size), (255, 255, 255, 0)), glare)
    lifted.putalpha(Image.composite(
        lifted.getchannel('A'), Image.new('L', (size, size), 0), canvas.getchannel('A')))
    canvas.alpha_composite(lifted)


def build(blank, out_path, size, *, detailed, letter_scale, letter_fill,
          letter_edge, edge_width, rim_width=None, rim_stops=None,
          inner_stops=None, face_inset=None, glare=None):
    big = size * SUPERSAMPLE
    coin = squared(keyed(blank)).resize((big, big), Image.LANCZOS)

    if detailed:
        canvas = coin
        # The model's inner ring is a hard black line, which at this diameter
        # reads as a gap cut through the coin. Repainted as a lit edge it
        # separates face from rim without punching a hole in the metal.
        paint_ring(canvas, INNER_RING[1], INNER_RING[0], inner_stops)
    else:
        # One rim, drawn rather than inherited: the model puts a second ring
        # inside the first every time, and two dark lines a pixel apart are one
        # muddy line at twenty pixels. Gold rather than black, because a black
        # ring this thick is most of what you see of a small coin.
        pad = rim_width * SUPERSAMPLE
        canvas = Image.new('RGBA', (big, big), (0, 0, 0, 0))
        disc = Image.new('RGBA', (big, big), (0, 0, 0, 0))
        ImageDraw.Draw(disc).ellipse([0, 0, big - 1, big - 1], fill=(255, 255, 255, 255))
        canvas.alpha_composite(disc)
        paint_ring(canvas, 1.0, 0.0, rim_stops)
        body = face_only(coin, face_inset).resize((big - pad * 2, big - pad * 2), Image.LANCZOS)
        canvas.alpha_composite(body, (pad, pad))

    if glare:
        lift_glare(canvas, glare)

    ring = json.load(open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                       's-ring.json')))
    canvas.alpha_composite(letter(big, ring, letter_scale, letter_fill,
                                  letter_edge, edge_width * SUPERSAMPLE))
    canvas.resize((size, size), Image.LANCZOS).save(out_path, optimize=True)
    print(f'wrote {out_path} at {size}px')


if __name__ == '__main__':
    here = os.path.dirname(os.path.abspath(__file__))

    # White where the light falls, through a saturated gold, into a deep gold
    # in the shade. The middle stop is what stops it reading as a grey pencil
    # line across the face.
    LIT_EDGE = [(0.00, (255, 255, 255)),
                (0.34, (255, 214, 92)),
                (0.70, (206, 132, 12)),
                (1.00, (124, 72, 2))]

    build(f'{here}/blank-hi.png', f'{here}/snake-bux.png', 256,
          detailed=True, letter_scale=0.50,
          letter_fill=(255, 250, 224, 255), letter_edge=(104, 62, 4, 255),
          edge_width=2, inner_stops=LIT_EDGE)

    # The small coin's whole edge, so it has to hold the silhouette on both a
    # white page and a dark one. The lit end never goes near white for that
    # reason — it would vanish on paper — and the shaded end never goes black,
    # which is what made the old rim the loudest thing on a twenty-pixel coin.
    build(f'{here}/blank-lo.png', f'{here}/snake-bux-small.png', 64,
          detailed=False, letter_scale=0.54,
          letter_fill=(255, 253, 238, 255), letter_edge=(88, 52, 2, 255),
          edge_width=2, rim_width=2,
          # Weighted toward the shaded end: the lit end has to hold the
          # silhouette against a dark page and the shaded end against a white
          # one, and only one of those is in danger of disappearing. Starting
          # at a mid gold rather than near-white is what keeps the top-left
          # edge from dissolving into paper.
          rim_stops=[(0.00, (255, 198, 62)),
                     (0.28, (226, 150, 20)),
                     (0.62, (150, 88, 6)),
                     (1.00, (86, 48, 2))],
          # Inside the model's own inner ring, not outside it: cut any wider and
          # that ring survives as a dark circle sitting inside the new rim.
          face_inset=INNER_RING[0],
          # No lift here. Cutting the face inside the model's ring enlarges
          # what is left, so its glare is already generous at this diameter;
          # boosting it as well flooded the upper half and swallowed the letter.
          glare=None)
