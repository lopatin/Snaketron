# Textures, overlays, and sprite sheets

Use this reference for every image source. Asset generation is a structured
request to the caller's `image_generator` role. In production configuration the
default is Gemini 3 Pro Image, but requests describe pixels and geometry rather
than naming a vendor. The response must record the resolved provider/model,
request id, prompt, source references, and exact byte hashes.

## Ask for usable source art

State the asset kind, exact grid/dimensions accepted by the capability manifest,
body direction, required wrap axes, transparency, mark scale, palette intent,
and whether the output is a single texture or X-by-Y frame grid. Ask for no
labels, borders, gutters, UI, shadows, perspective, or presentation background.

Use the canonical texel density in the caller's build schema. The current forge
contract is 64 texels per cell for coats/overlays and 16 for sheets; it derives
the smaller delivery rungs automatically. Do not trade X/Y metadata for a
different density or ask the provider to generate the delivery ladder.

For a sheet, X is body cells and Y is derived frames. Set `desired_fps`, then
compute `Y = ceil(period_ms * desired_fps / 1000)` and clamp it to the pinned
row, dimension, and decoded-memory limits. Each row is one complete frame of
the same X-cell snake strip; rows advance down the image. Do not ask for a film
strip with whitespace or a set of independent poses. Row zero is the resting
and reduced-motion art. Prefer tall sheets when more frames are supported and
useful, but never specify a free-standing Y that disagrees with the requested
rate, period, or pinned runtime. X comes from pattern/repeat needs and is
independent of Y. Static coats and overlays use `desired_fps: null`.

Do not squeeze an extreme tall grid into one provider image. The deterministic
driver compares the full X:Y grid with the configured provider aspect ratios.
When they are not close, it divides Y into the largest contiguous frame-row
slices that are close, journals and retains each call, supplies the preceding
slice (and row-zero slice for final loop closure) as continuity references,
normalizes each slice without cropping, and vertically assembles the exact
X-by-Y forge input. Slice count and worst-case image calls are hard-bounded
before spend. The implementation plan still declares one logical sheet; never
fabricate independent texture descriptors for its provider slices.

## Choose the required joins from use

- Any `tile` fit needs `x` continuity; a clipped or stretched coat does not.
- Every looping sprite sheet needs `y` continuity from its final row to row 0.
- A sheet tiled down the body needs both `x` and `y`.
- A clipped head/tail overlay does not need an invisible X join.

Measure all required joins before and after repair at multiple scales. A
one-pixel boundary score can certify a blurry repair while large marks remain
misaligned. Include alignment anomaly and detail/chroma retention; judge the
exact post-resize, post-quantization bytes that will be served.

## Repair a crop with `[T, X, T]`

For a crop whose ends were never adjacent, place a narrow masked gap between
two byte-identical copies: `[T, X, T]`. The current strict forge uses its local
LaMa helper to fill only X, then keeps `[T, X]` as the new repeat and performs
one wrap-aware fit back to the exact authored grid.
Both repeating junctions were painted against their real neighbors. Do not keep
X alone, cross-fade the ends, use a gap so wide it becomes mush, or iteratively
lengthen generated fill.

The current implementation makes one attempt at a 15% gap; it does not run an
editor loop. Compare detail and chroma between original and generated regions.
Keep T exactly as LaMa saw it before the final wrap-aware fit. Any filter after
repair must wrap by construction; ordinary edge clamping silently recreates a
seam. A provider-backed editor remains a future capability until it has the
same exact-mask, exact-byte, journal, budget, and conformance contract.

## Repair an almost-tileable image by rolling

For art already intended to tile, resize wrap-aware to the shipping dimensions
first, then roll each required border join to a center line. Measure it in an
ordinary neighborhood, inpaint the narrowest band that clears multi-scale
seam/alignment checks without loss of detail, restore all unmasked pixels
byte-for-byte, roll back, and remeasure.

Try the maximum permitted band once to establish repairability, then bisect for
the narrowest acceptable band. Aim inside the acceptance threshold rather than
barely on it. A surviving structural mismatch, recurring soft stripe, palette
shift, or excessive detail/chroma loss is rejection—not permission to repeat
inpainting indefinitely.

Wrap-aware resize/filter means periodic padding or an equivalent operation.
Three-up tiling alone is insufficient when resampling phases differ between
copies. Quantize only after repair and re-run every metric afterward.

## Repeat length and mark scale

A clean join can still look repeated. Diagnose repeated distinctive clusters
as a short-repeat problem, not a seam problem. Obtain more genuine source or
generate/stitch multiple varied windows; do not crop tighter, which enlarges
marks while shortening the repeat.

At game scale, a recognizable mark generally needs multiple body cells. Choose
the natural X length so marks remain readable and the repeat is longer than the
body fixtures where possible. Record the measured mark spacing and repeat cells
in the asset report rather than relying on a square sheet's aspect ratio.

If source art must be rotated, bake that rotation into a wrap-aware tiled pixel
operation and repair/recheck afterward. A layer transform rotates the snake's
paint quad and silhouette, not merely the motif inside the bitmap, and applying
both would silently rotate twice. Record baked rotation as provenance only.

## Sprite-specific gates

Verify grid divisibility, independently declared X/Y, every row reachable,
frame order, no unintended whole-picture translation, temporal continuity,
final-to-zero loop, short/median/long placement, bytes/dimensions/palette, and
reduced motion on row zero. A sprite that ends inside the body needs a declared
leading/trailing fade so its bitmap edge does not read as a cut.
