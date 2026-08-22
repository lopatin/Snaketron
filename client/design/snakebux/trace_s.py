"""Turn the 32x32 favicon into clean high-resolution geometry.

The favicon's S is built from straight edges — flat bars and angled terminals —
so the bitmap is a sampling of a shape, not the shape itself. Collecting the
unit edges where a filled pixel meets an empty one gives that shape's outline
exactly; chaining them and dropping the points that lie on a line recovers the
straight runs, instead of enlarging stair-steps the way an upscale would.
"""
import json
import sys
from PIL import Image, ImageDraw

SIZE = 32


def load_mask(path):
    alpha = Image.open(path).convert('RGBA').getchannel('A')
    return {(x, y) for y in range(SIZE) for x in range(SIZE)
            if alpha.getpixel((x, y)) > 128}


def rings(cells):
    """Closed outlines of the filled region, walked as unit edges."""
    edges = {}
    for (x, y) in cells:
        # Each edge is directed so the filled side is on its left; the four
        # cases together always close into loops.
        if (x, y - 1) not in cells:
            edges[(x, y)] = (x + 1, y)
        if (x + 1, y) not in cells:
            edges[(x + 1, y)] = (x + 1, y + 1)
        if (x, y + 1) not in cells:
            edges[(x + 1, y + 1)] = (x, y + 1)
        if (x - 1, y) not in cells:
            edges[(x, y + 1)] = (x, y)

    found = []
    while edges:
        start = next(iter(edges))
        ring, point = [], start
        while point in edges:
            ring.append(point)
            nxt = edges.pop(point)
            point = nxt
            if point == start:
                break
        if len(ring) >= 4:
            found.append(ring)
    return sorted(found, key=len, reverse=True)


def simplify(ring, tolerance):
    """Douglas-Peucker on a closed ring.

    Split at the point farthest from the start first: run on the ring as a
    single polyline its two ends coincide, every distance is measured against a
    zero-length line, and the whole outline collapses to one point.
    """
    def dp(pts):
        if len(pts) < 3:
            return pts
        (x1, y1), (x2, y2) = pts[0], pts[-1]
        worst, index = 0.0, 0
        for i in range(1, len(pts) - 1):
            px, py = pts[i]
            num = abs((y2 - y1) * px - (x2 - x1) * py + x2 * y1 - y2 * x1)
            den = ((y2 - y1) ** 2 + (x2 - x1) ** 2) ** 0.5 or 1.0
            distance = num / den
            if distance > worst:
                worst, index = distance, i
        if worst <= tolerance:
            return [pts[0], pts[-1]]
        return dp(pts[:index + 1])[:-1] + dp(pts[index:])
    origin = ring[0]
    far = max(range(len(ring)),
              key=lambda i: (ring[i][0] - origin[0]) ** 2 + (ring[i][1] - origin[1]) ** 2)
    first = dp(ring[:far + 1])
    second = dp(ring[far:] + [origin])
    return first[:-1] + second[:-1]


if __name__ == '__main__':
    cells = load_mask('client/web/public/images/snaketron-favicon-32x32.ico')
    outlines = [simplify(ring, 0.6) for ring in rings(cells)]
    print(f'{len(outlines)} ring(s), points: {[len(r) for r in outlines]}',
          file=sys.stderr)
    json.dump(outlines, open(sys.argv[1], 'w'))

    scale, pad = 30, 30
    img = Image.new('RGBA', (SIZE * scale + pad * 2,) * 2, (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    for ring in outlines:
        draw.polygon([(x * scale + pad, y * scale + pad) for x, y in ring],
                     fill=(17, 17, 17, 255))
    img.save(sys.argv[2])
