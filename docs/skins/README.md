# Authored built-in skins

Thirty-six built-in skins authored as SkinDoc v2 documents. The first six came
from user-supplied reference strips, retained at `<name>/reference.png`; the
other thirty were briefed as patterns rather than pictures. Each document is
`skin-schema/skins/<name>.skin.json`.

None of these embed a single pixel of raster art. Every one is pure procedural
layers — ribbons, bands, gradients and head treatments — because no
asset-generation capability was advertised in the sessions that authored them,
and because none of the sub-cell detail in the reference strips (turtles, tree
lines, individual rock lumps) survives a 5–15 px game cell anyway. What survives
is rhythm, ramp and one signature mark, and those are formulas.

| Skin | Reference | Ops | Team reading comes from |
| --- | --- | --- | --- |
| Breaker | tropical surf | 178 | aqua water vs warm surf |
| Afterburn | synthwave sunset | 171 | indigo night vs crimson dusk |
| Cinder | lava crust | 189 | seam temperature — blue-hot vs orange |
| Floe | polar pack ice | 181 | polar blue vs ember rust |
| Woodblock | ukiyo-e print | 154 | indigo *aizuri-e* vs sepia *tan-e* ink |
| Bloom | bioluminescent jellyfish | 177 | navy void vs deep plum |

And ten built from a pattern brief, each demonstrating a different primitive:

| Skin | Pattern | Ops | Mechanism |
| --- | --- | --- | --- |
| Slipstream | diagonal racing stripes | 190 | five stacked phase-stepped lanes |
| Harrier | military chevrons | 192 | seven lanes phased out from the keel, plus a counter-chevron at a 4:3 period |
| Argyle | knitted diamond lattice | 198 | two lane sets stepped in mirror-image phase |
| Serpentine | irregular reptile scales | 195 | `noise` in band alpha |
| Voltcore | calm at rest, erupts on Boost | 197 | `boost_only` layers |
| Marquee | lettering on a lit sign | 194 | the `text` source |
| Basalt | staggered stone courses | 198 | high-duty lanes, alternate courses half a period offset |
| Ripple | a travelling standing wave | 181 | `t_center` as a `time` expression, staggered per lane |
| Moth | mirrored wing bands + eyespot | 180 | lane pairs symmetric about `t_center = 0` |
| Prism | iridescent, hue shifting on the clock | 188 | `lighten` at the palette site, per-stop phase |

And twenty more, grouped by shape language:

| Skin | Pattern | Ops |
| --- | --- | --- |
| Houndstooth | broken check with pointed legs | 192 |
| Herringbone | woven tweed, direction flipping course by course | 193 |
| Carbon | 2×2 twill weave under lacquer | 193 |
| Tartan | a highland sett of unequal widths | 189 |
| Loom | woven strip cloth in colour blocks | 185 |
| Pinstripe | fine chalk lines down the length | 186 |
| Circuit | PCB traces, pads and packages | 184 |
| Chrome | a mirrored tube reflecting a horizon | 183 |
| Neon | a lit glass tube with electrode collars | 188 |
| Static | analogue snow and rolling interference | 194 |
| Mosaic | Byzantine tesserae in grout | 192 |
| Geode | sliced agate banding | 194 |
| Amber | fossil resin with inclusions | 186 |
| Rosette | stained glass with black leading | 192 |
| Delft | cobalt brushwork on tin glaze | 190 |
| Coral | reef growth | 195 |
| Timber | bark furrows with lichen | 194 |
| Camo | disruptive pattern at three scales | 191 |
| Peacock | repeating ocelli on a barbed ground | 189 |
| Monarch | veined wing cells with a dotted margin | 191 |

The ceiling is 200 predicted ops per snake; eight snakes share a frame.

## The gate that shapes all of them

`skin-schema/src/sampler.rs` composites each document where the player name and
carried-food digits sit — `s = 1.5, 2.0, 2.5, 3.0` — for all 32 animation steps
and every role, and requires 4.5:1 against the label ink. The ink is *derived
from the role's fill*: a dark fill gets white ink, a light fill gets near-black.

Five of the six reference strips are dark grounds carrying bright features, so
every glowing seam, sun and iceberg lands on the wrong side of that. The
important consequence is that the stretch has to be **consistently** dark or
consistently light across the whole cycle — a body that swings between the two
at the label position cannot pass with either ink. Each skin resolves it by
deciding where along the body brightness is allowed to live, not by dimming:

- **Afterburn** phases the suns to land at cells 4.6, 9.6, 14.6, 19.6 and ramps
  the horizon and lattice in with `smoothstep(s)`, so the neon blazes toward the
  tail — which is where the reference concentrates it too.
- **Floe** keeps white ink and makes the label stretch reliably deep water. The
  ice fades in past cell 2.4, so the head cuts a lead of open water and the pack
  thickens behind it.
- **Cinder** inverts its `Cooling` ramp so the darkest crust is at the head, and
  shortens the head ramp to 1.4 cells — under the first label sample.
- **Bloom** moves its cyan bloom to gradient offset 0.66, past the reach any
  label sample has, so it is bright and free.

## Do not use `transform` — it is a trap

`rotate_turns`, `scale_s` and `scale_t` are in the schema and look like the
obvious way to make diagonal stripes. They are not usable, and this was
established by rendering rather than by reading:

- **`solid` and `band` sources bypass the run frame entirely** — `composite.rs`
  computes their rectangles directly in screen space because "runs are
  axis-aligned". The layer transform is a canvas transform applied around those
  finished coordinates, so a rotated band lands somewhere else and is clipped
  away. It paints nothing at all.
- **`apply_transform` runs once per layer with the CTM still at canvas
  identity**, with no centring translate, so a rotation pivots about the
  top-left of the canvas rather than about the body. Rendering one 0.02-turn
  gradient across the pose corpus gives a different wedge on every pose — a
  corner sliver on the four-cell spawn, a long diagonal on `reversed_travel`,
  nothing on `zigzag`. In a live arena the pattern would change shape every
  frame as the snake moved.

This contradicts `layer.rs`'s own doc comment, which states the composition is
`run_affine ∘ layer_transform ∘ source` with the layer transform "inside" the
run affine. The implementation applies it outside. Fixing it would move op
sequences, so `predict_ops`, the perf census and the `TransformLiar` conformance
case all move with it.

**Diagonals come from stacked lanes instead**, and they look good: several thin
bands at increasing `t_center` with progressively shifted `phase_cells` render
as a clean staircase that reads as a diagonal at a 5–15 px cell. Reverse the
phase progression to flip direction, or fold it back on itself for a chevron.
Slipstream, Harrier and Argyle are all built this way.

## Three techniques worth reusing

Found while building these and cheap to apply anywhere:

- **Raising a band alpha's floor is free.** The sampler's worst label step is
  always the alpha *peak*, so `0.6 + 0.38*sin(...)` measures identically to
  `0.42 + 0.55*sin(...)` while keeping the pattern continuously present instead
  of blinking out for a quarter of the cycle. Prefer the higher floor.
- **Give two lane sets incommensurate periods.** Harrier runs a bone chevron at
  4.2 against a dark counter-chevron at 5.6; the 4:3 ratio means the marking
  differs at every point down a 21-cell body rather than repeating as a row of
  identical shapes. The cheapest way to look designed rather than tiled.
- **Avoid a mid-tone `fill`.** A mid-tone is the one value that bright layers
  push across the 4.5:1 line in *either* ink direction — Harrier measured a rust
  khaki at exactly 4.50 and could only escape by committing dark on one side and
  light on the other. Houndstooth went further and inverted the hostile side's
  polarity outright: near-black check on bone for friendly, bone check on
  charcoal for hostile. It is the same cloth either way, and it made both the
  label gate and the 0.15 side-distance floor self-solving.

## Three primitives that do work, and were unused until now

Verified by rendering, each now carried by one skin:

- **`noise(a, b)`** — legal only at the Cell site, meaning a band's `alpha` or a
  `head_ramp`'s `opacity`. The only organic-texture tool in the language.
- **`text`** — glyphs paint along the body, oriented to the direction of travel,
  from the charset `" ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.!?-"`. About 48 ops.
- **`boost_only: true`** — the layer exists only while the snake is boosting.
  Expressions at the Snake site can also read `boost` directly to fade a layer
  in rather than switch it on.

## Two renderer rules the schema validator does not enforce

Both were caught by `cargo test -p client`, not by `validate-skin`:

1. **`perceptual_distance` between a friendly and a hostile `fill` must reach
   0.15** (`skin::conformance`, `MIN_SIDE_DISTANCE`). Contrast ratio is the
   wrong instrument — it only sees lightness. Cinder's two near-black rocks were
   0.085 apart and Woodblock's two near-neutral papers 0.061.
2. **A v2 document cannot nominate `SideCue::Contour`.** `side_cue()` defaults
   to `Body` and only a Rust skin could override it, so the body fill *is* the
   side cue. Woodblock was designed with a neutral paper and the side carried by
   the ink; the paper had to be tinted apart anyway — a blue *aizuri-e* sheet
   against a warm *tan-e* one, which is at least two real print traditions.

## Reproducing the evidence

```bash
cargo run -p skin-schema --bin validate-skin -- skin-schema/skins/breaker.skin.json
```

Contact sheets are under `docs/screenshots/skins/<name>/`, captured through the
real WASM renderer by `client/web/tests/capture-skin-sheet.mjs` against a dev
server. They are fixed-clock samples; motion is only visible live at
`/qa/skins`.
