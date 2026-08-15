# PRD: The skin shading engine

| Field | Value |
| --- | --- |
| Status | **Implemented** (S0-S9). See section 20 for what was built differently and why. |
| Product | Snaketron client rendering and cosmetics |
| Scope | Replaces the fixed body painter with a declarative layer compositor; adds body space, sprites/textures, procedural sources, and general animation |
| Depends on | `specs/skins-prd.md` (shipped; see its section 15.5 for the one gate it did not deliver) |
| Owners | Product / Client |
| Last updated | 2026-08-15 |

## 1. Executive decision

Replace `paint_body` — one hand-written pass structure with eleven tunable parameters — with a **layer compositor over body space**: a declarative stack of layers, each with a source (solid, gradient, tiled pattern, image, or procedural expression), a placement (a span along the snake), a transform, and animatable parameters.

The design is:

- **Stay in canvas 2D. Bake, don't shade.** Anything per-pixel or expensive is evaluated at registration into small textures; the per-frame path stays a bounded number of canvas ops. GPU shaders would mean a WebGL layer, a second implementation of every skin, and the loss of the recording-based verification the system is built on.
- **Body space `(s, t)`** — arc length from the head in cells, and `-0.5..0.5` across — is the coordinate system everything is expressed in. It is what makes tiling, relative placement, and sprite spans tractable instead of ad-hoc pixel math. A Snaketron body is a polyline of axis-aligned runs, so body space maps to screen through one affine transform per run.
- **A regioned frame, not a free-for-all.** The contour (including the Boost band) lives *outside* the body clip and is the only source of overhang; body layers are clipped to the silhouette and cannot escape it; the head has a small bounded slot. This is what keeps `overhang_px` meaningful and the Boost telegraph unhideable without a pixel validator.
- **Static topology, dynamic parameters.** Layer count and order are fixed at registration. Time may change any numeric or colour parameter and nothing else. This preserves op-count invariance, makes cost knowable before a skin runs, and keeps every existing conformance check meaningful.
- **Images are first class and general** — an atlas with named regions, spans placed by arc length, three-slice (fixed head cap, tiled or stretched middle, fixed tail cap), per-role tinting, and frame-strip animation via a moving source rect. Not a cap-sprite special case.
- **Classic is expressed as layers and proven before the flip.** The compositor ships first as an additional implementation; classic-as-layers is compared against today's painter by rendering both in the same headless browser and diffing pixels; only when it matches within a stated tolerance does classic flip over, and the pre-flip trace is retained forever as a frozen oracle.

The honest headline: the current golden op trace has never been a pixel guarantee, only a very good proxy, and it stops being a proxy at all once `drawImage` enters the op set. This PRD spends part of that proxy and replaces it with a browser-based pixel comparator. That trade is the central decision here, and section 12 is where it is made — including the option of declining the trade entirely.

## 2. Product problem

`BodyStyle` has eleven fields. Every skin Snaketron can express is a point in that space: two colours, two widths, a head core, a gradient, and an optional wave. Tidewave, Voltage and Lantern differ in *parameters*, not in *structure* — which is why they read as three tunings of one skin rather than three skins.

Anything structurally new — a stripe, a scale pattern, art on the head, a texture along the body — currently requires a Rust escalation like `ember.rs`. That path works, but it does not scale past a handful of first-party skins, it cannot be authored by anyone who does not write Rust, and every escalation forecloses the option of ever compiling skins to a different renderer.

We want a skin to be able to say: *these layers, in this order, placed here on the body, blended like this, moving like that* — as data.

## 3. Goals and non-goals

### 3.1 Goals

1. A declarative layer compositor replacing `paint_body`, expressed over body space, with static topology and animatable parameters.
2. Sources covering solid fills, linear and radial gradients, tiled patterns, images/sprites from an atlas, and procedural expressions.
3. A general sprite subsystem: named atlas regions, arc-length spans, three-slice placement, fit modes, per-role tinting, frame strips, and a defined policy for bodies shorter than their art.
4. Animation generalised from three fixed scalars to any numeric parameter, driven by waveform tracks or expressions, baked into a fixed ring of steps.
5. Classic's appearance preserved through the transition, with the guarantee re-established on a stronger footing (browser pixel comparison) rather than quietly spent.
6. No per-frame allocation, no per-frame JSON, and a per-snake op count within a stated multiple of today's.
7. All six shipped skins carried across with no player-visible change: the four catalogue documents converted to v2, and the two Rust skins (`classic@1`, `ember@1`) rebuilt on the compositor.

### 3.2 Non-goals

- **User-submitted skins.** First-party and bundle-embedded only. Section 17 item 4 names what would need rework if this returns.
- **The composite/pixel validator.** Deferred. Section 13 states precisely which guarantees degrade to review controls in its absence — this is the most important thing in this document to read before shipping a non-classic skin.
- **GPU shaders / WebGL.** Section 1.
- **`ctx.filter`.** Disabled by default in shipping Safari. Blur, drop-shadow and colour-matrix are bake-time software operations or they do not exist.
- **Conic gradients.** `web-sys` 0.3.77 exposes no `create_conic_gradient` binding (verified: zero matches). A schema surface with no implementation is worse than an absence.
- **Per-frame blend modes.** Blending happens at bake time; per-frame emission is `source-over` only. Non-source-over per-frame compositing needs a per-snake isolation buffer, which is a real seam in this design but is not built.
- **Anisotropic overhang.** `overhang_px` stays a scalar. Per-side extents would move the twelve pinned roster-layout assertions.
- **Per-skin animation step counts.** Fixed at 32.
- **DPR-scaling the arena canvas.** Out of scope, but bake resolution is derived rather than constant so a future DPR-aware arena does not require a schema change.

## 4. Repository baseline

Verified facts this design builds on:

- The `SnakeSkin` trait (`client/src/skin/mod.rs:281-327`) has one painting entry, `paint_alive`, plus a defaulted `paint_dead`, `base_theme`, `celebration_theme`. **The compositor is a new implementation of that trait, not a new trait.** No trait change is required to land it, which is what makes shipping it alongside `ClassicSkin` possible.
- `paint_body` (`client/src/skin/body.rs:154-379`) emits, in this exact order: **outline layers** (per layer: its strokes, then its joint discs) → **body strokes** → **joint discs** → **tail disc** → **head-proximity gradient rects** → **head disc** → **white head overlay** → **head core**. The order matters and is easy to get wrong: only the *tail* disc precedes the gradient. The head is painted *after* it, which is what stops the ramp from washing out the head. A structurally separate single-cell branch omits the mask, the gradient and the white overlay entirely.
- `SnakePose` (`mod.rs:133-162`) carries the compressed body in whole screen cells, `cell_size` (fractional; the arena walks integers 15→5), `boost_active`, `anim_ms`, `reduced_motion`. **It carries no arc length** — body space must be derived per frame from the polyline, as `walk_cells_from_head` already does.
- `SkinColors<'a>` is four borrowed `&str` (`mod.rs:164-176`), documented as a lookup rather than a build because the arena asks every snake for its colours every frame. A compositor must still answer it with flat hex, which means baking a representative colour per role at registration even when the body is a texture.
- Occlusion is renderer-owned (`paint_alive_with_occlusion`, `mod.rs`), sized from `metrics().overhang_px`, skipped for single-cell bodies.
- The golden trace (`client/src/skin/goldens/classic.trace`, 10,663 lines total) pins classic as **op text**. Counted as recorded ops: `## palette` 2,592, `## alive` 6,066, `## dead` 1,380, `## roster` 324. Only `## alive` and the six roster glyph blocks inside `## roster` are in this work's blast radius; `## palette` and `## dead` are untouched by it.
- Measured per-snake cost today, extracted from that trace: **12–151 ops**, median ~60; a four-corner zigzag is 151; a 21-cell straight snake is 64. Grid dots (~2,300 arcs on a 60×40 arena) dominate the frame regardless.
- The current painter already allocates on the frame path: up to ~11 `String`s, two `Vec`s and a `HashSet` per snake per frame. The compositor is expected to *reduce* this, not merely match it.
- Runs are axis-aligned because `Snake::step_forward` only ever moves one cell along an axis (`common/src/snake.rs`). Consecutive turns produce a **one-cell run**, which no current fixture contains. A live body does not self-cross — the engine kills a snake whose head enters its own body — so `walk_cells_from_head`'s dedup set is defensive rather than load-bearing; it should still be separated from the arc-length counter (section 16), but for clarity, not correctness.

## 5. Coordinate spaces and the regioned frame

### 5.1 Spaces

| Space | Definition | Used for |
| --- | --- | --- |
| **Body** `(s, t)` | `s` = arc length from the head centre in cells; `t` = across, `-0.5..0.5` | Almost everything: spans, tiling, gradients along the snake |
| **Head** `(u, v)` | Head cell local, `-0.5..0.5` both axes, oriented to travel | Eyes, markings, anything anchored to the head |
| **Screen** | Pixels | The rare layer that must not rotate with the body |

`s` is derived from the compressed polyline: accumulate run lengths, with sub-cell positions interpolating within a run. This generalises `walk_cells_from_head`, which today computes a per-cell integer distance and simultaneously uses its `seen` set for paint deduplication — two jobs in one loop that must be separated before self-crossing bodies are handled correctly (section 16).

### 5.2 The regioned frame

Layers are not free to paint anywhere. The frame is:

```
contour region    — outside the body clip; the ONLY source of overhang
  └─ boost band   — present iff boost_active; conformance-checked
body region       — inside one clip per snake, the silhouette
head region       — a bounded slot at s ≈ 0, inside the body clip
```

This is the structural substitute for the deferred pixel validator, and the reason it exists:

- **Only contour layers contribute overhang**, so `overhang_px` stays computable as `max(extra/2)` over active contour layers, and stays honest without measuring pixels.
- **Body layers cannot exceed `|t| ≤ 0.5`**, so no body layer can paint over the Boost band. Without this rule, a body layer with a large opaque source could hide the band and the op-text conformance check would not notice it.
- **The contour region is ordered**: the Boost band is always the outermost contour layer. Without this, a later contour layer would bury the band and the same check would still pass.

### 5.3 Two clip shapes

The body region has **two** clip shapes, and the distinction is load-bearing rather than a nicety:

- **`silhouette`** (default) — the capsule union: run strips with round caps at head and tail, plus a disc at every joint, filled nonzero. This is the snake's visible outline.
- **`cells`** — the union of the body's cell *squares*. Larger than the silhouette at every cap and every outer corner.

Classic's head-proximity ramp needs `cells`. It paints unclipped full-cell rectangles today, deliberately tinting pixels that are inside the body cells but *outside* the rounded silhouette — the surround of the head cap and every joint's outer wedge. At `cell = 15` that is ~24 px² per affected cell, tinted 30% white over whatever the contour painted there. Clipping the ramp to the silhouette would change those pixels, and classic-as-layers would miss the section 12 tolerance for a reason that has nothing to do with the compositor being wrong.

Neither shape exceeds the body cells, so both leave `overhang_px` at zero for body layers. A layer declares `clip: "silhouette" | "cells"`; `silhouette` is the default because it is what a skin author almost always means.

### 5.4 The head core is a generic top layer

The head core is painted by the renderer as an **always-topmost layer in the head region**, after every body layer, using `head_core_ratio` and `head_core_color` from the skin's metrics.

This is what lets an image span `s = 0`. A head sprite may cover the head cell; the core is composited over it afterwards, so the disc the label-contrast rule reasons about is still a flat fill of a known colour. The rule is therefore "image sources never *survive* inside the head-core disc", not "may not be placed there" — an important difference, because placing art on the head is one of the main things a sprite skin wants to do.

## 6. The layer model

```jsonc
{
  "id": "scales",
  "region": "body",            // contour | body | head
  "space":  "body",            // body | head | screen
  "clip":   "silhouette",      // silhouette (default) | cells — see 5.3
  "span":   { "from": "head", "natural": 5, "min": 2, "priority": 10 },
  "source": { "pattern": "scale_tile", "repeat": { "s": 3, "t": 1 } },
  "transform": { "translate": { "s": 0, "t": 0 }, "scale": { "s": 1, "t": 1 }, "rotate": 0 },
  "blend":   "multiply",       // resolved at BAKE time; per-frame is source-over
  "opacity": { "expr": "0.3 + 0.1 * sin(tau * time)" }
}
```

**The rule that everything else depends on: layer count and order are fixed at registration.** A layer that should appear animates its opacity from zero. Time may change numbers and colours; it may not add, remove, reorder, or conditionally skip a layer.

That rule buys: op-count invariance (already a conformance check), a cost knowable before a skin ever paints, and a bake plan computable at registration.

### 6.1 Layer transforms

`transform` was named as a requirement and needs a definition rather than a schema key.

- **Space** — the transform acts in the layer's declared `space` (`body`, `head`, or `screen`), never in screen pixels unless that is the declared space.
- **Origin** — the start of the layer's span in body space; the region centre in head and screen space.
- **Composition** — `run_affine ∘ layer_transform ∘ source`. The per-run affine (section 10) maps body space to screen and is the renderer's; the layer transform sits inside it and is the skin's.
- **Units** — `translate` in cells (body space) or region fractions (head/screen); `scale` unitless; `rotate` in **turns**, matching the animation DSL so a rotation can be driven by a track without a unit conversion.
- **Clipping is unaffected.** A transformed body layer still clips to its declared shape (section 5.3). A transform cannot be used to escape the body.
- **Overhang is unaffected.** Contour layers declare `extra`; a transform on a contour layer does not change `overhang_px`, and the CTM-aware recorder (section 16) is what makes that claim checkable rather than merely stated.

**Spans** place a layer along the body. One grammar, used everywhere in this document:

```jsonc
"span": {
  "from": "head",        // head | tail | { "s": n } | { "fraction": f }
  "natural": 5,          // preferred length, in cells
  "min": 2,              // below this the layer is skipped entirely
  "priority": 10         // higher wins when the body is too short for everything
}
// or, for the middle of a three-slice:
"span": { "between": ["head-art", "tail-art"], "min": 1 }
```

Lengths are always in cells; there is no `unit` field. Spans are clamped to the actual body length, which is where short snakes get decided (section 8.3).

## 7. Sources

| Source | Notes |
| --- | --- |
| `solid` | A colour. The degenerate layer. |
| `linear` / `radial` gradient | Stops in body space. Baked as **data**, not as a browser handle — registration is a process-wide `OnceLock` with no canvas in scope, and it also runs natively in the conformance and golden tests where no context exists at all. The `Web` arm materialises a `CanvasGradient` lazily, cached per context. A gradient that merely *moves* with the body needs no per-step bake (the run affine moves it, since gradient coordinates are user-space); what forces a per-step bake is that colour stops are immutable once added. |
| `pattern` | A tile, itself a small baked layer stack, repeated in `s` and/or `t`. Same rule: baked as data, materialised as a `CanvasPattern` with `setTransform` lazily, cached per context. |
| `image` | An atlas region. Section 8. |
| `procedural` | An expression evaluated into a tile at bake time. Section 9.2. |

## 8. Images, sprites and textures

The product requirement is a general subsystem, not a cap-sprite special case.

### 8.1 Atlas

One atlas per skin, decoded once at registration and held for the process lifetime alongside the compiled skin. Regions are named rectangles:

```jsonc
"atlas": {
  "src": "skins/tidewave/atlas.v3.png",   // versioned URL, first-party, bundle-served
  "filtering": "smooth",                   // smooth | pixelated
  "regions": {
    "head": { "x": 0, "y": 0,   "w": 320, "h": 64 },
    "tail": { "x": 0, "y": 64,  "w": 192, "h": 64 },
    "mid":  { "x": 0, "y": 128, "w": 64,  "h": 64 }
  }
}
```

Delivery is a **versioned relative URL, not `include_bytes!`**. Embedding pixels in the wasm binary inflates every player's initial download with art for skins they are not wearing, and wasm is not compressed as well as PNG already is. The descriptor stays `include_str!`-embedded so the catalogue is known without a fetch; only pixels are fetched, and only for skins actually in the match.

A skin whose atlas has not finished decoding paints its fallback layers (a skin must be legible with no atlas at all — this is a schema requirement, not a nicety, because a mid-match join must not show a blank snake).

### 8.2 Placement

An image layer's `u` maps to its span in `s`, its `v` to `t`. Because runs are axis-aligned, a span crossing a corner splits into per-run slices and each slice is one `drawImage` with a source sub-rectangle:

```
for each run R overlapping the span [s0, s1]:
    [a, b] = intersect(R.arc_range, [s0, s1])
    u0 = (a - s0) / (s1 - s0);  u1 = (b - s0) / (s1 - s0)
    drawImage(atlas,
      region.x + u0*region.w, region.y, (u1-u0)*region.w, region.h,
      (a - R.s0)*cell, -cell/2, (b-a)*cell, cell)
```

One `drawImage` per run per image layer, inside the per-snake clip and the run's transform.

Atlas regions must be **padded by at least one transparent texel** on every side. Fractional source coordinates are unavoidable (arc lengths are not integers) and bilinear sampling will otherwise pull in the neighbouring region's pixels along seams.

Three-slice falls out of spans:

```jsonc
{ "id": "head-art", "source": {"image": "head"}, "span": {"from": "head", "natural": 5, "min": 2, "priority": 10}, "fit": "clip" },
{ "id": "tail-art", "source": {"image": "tail"}, "span": {"from": "tail", "natural": 3, "min": 1, "priority": 10}, "fit": "clip" },
{ "id": "mid",      "source": {"image": "mid", "repeat": {"s": 1}},
                    "span": {"between": ["head-art", "tail-art"], "min": 1}, "fit": "tile" }
```

### 8.3 Short bodies are the common case

Snakes start at three cells. A five-cell head sprite and a three-cell tail sprite do not fit, and they overlap. This is not an edge case and must not be specified as one.

The allocator walks spans in priority order, granting each its `natural` length while the remaining body allows, then degrading toward `min`, then skipping the layer entirely.

What happens to the *art* inside an allocated span is the layer's `fit`:

| `fit` | Behaviour when the span is shorter than `natural` |
| --- | --- |
| `clip` (default for caps) | Draw the source at its natural scale and clip the far end. Art keeps its proportions. |
| `stretch` | Compress the source into the span. Available, not default — it distorts in ways an author cannot predict from the PNG. |
| `tile` | Repeat the source along `s`. The default for middles. |

The earlier draft of this section said "art is never compressed" while section 8.2's placement math unconditionally stretched the region across the span. `fit` is the reconciliation: the math in 8.2 is the `stretch` case, and `clip` maps a *prefix* of the source instead.

A span that cannot get its `min` disappears and the layer beneath shows through, which is why every image skin needs a solid or pattern base layer under its art.

Conformance renders every skin at body lengths 1, 2, 3, 5 and 21 cells, and the QA route's short poses are mandatory in review.

### 8.4 Tinting and frame strips

A grayscale region plus a per-role tint lets one atlas serve friendly and enemy without doubling the art. Tint is a bake-time multiply into a per-role tile, not a per-frame operation.

Frame-strip animation moves the *source rectangle* across the atlas as a function of the animation step. This is one `drawImage` with different arguments — it satisfies op-count invariance exactly, which is why frame strips are the sanctioned way to animate sprite art.

## 9. Animation and expressions

### 9.1 Model

`animation: { period_ms, tracks[], wave? }` generalises to: any numeric parameter, addressed by path, driven by a waveform (`sin`, `tri`, `saw`, `pulse`) or an explicit keyframe list, or by an expression. Compiled into a **fixed ring of 32 steps** at registration, exactly as `ParamSkin` does today.

32 steps is a real constraint, stated rather than hidden: a 900 ms cycle gives ~28 ms per step, below a 60 Hz frame and therefore invisible; a 4,000 ms cycle gives 125 ms plateaus, which is visible as stepping on a slow fade. Lantern has this artifact today. The validator gains a Nyquist rule — flag when `period_ms / 32` exceeds ~40 ms and the animated parameter's per-step delta is perceptible — rather than silently shipping stepped animation.

`reduced_motion` pins step 0. Always.

### 9.2 The expression DSL

Total and non-Turing-complete: no loops, no recursion, no user-defined functions, no unbounded anything.

- **Literals and constants**: decimal numbers, `pi`, `tau`
- **Inputs**: `s`, `t`, `len`, `time` (0..1 turns), `boost` (0/1), `seed` (stable per snake)
- **Arithmetic**: `+ - * /`, unary `-`, parentheses. Division by zero yields 0 rather than a NaN that would propagate into a colour.
- **Functions**: `sin cos saw tri pulse fract floor abs clamp mix smoothstep step min max noise(s,t)`

So the example in section 6 — `0.3 + 0.1 * sin(tau * time)` — is grammatical. It was not under the first draft of this list, which omitted literals, arithmetic and `tau`.

Evaluation tier is derived, not declared:

- **constant** (no `s`, `t`, or `time`) — folded at registration
- **per-step** (uses `time` only) — 32 values, baked
- **per-cell** (uses `s`) — evaluated in the existing cell walk, as `gradient_wave` already is
- **per-texel** (uses `t` or `noise`) — baked into a tile

Totality is being specified as a **sandbox boundary**, not a convenience cap. It must not be relaxed for first-party ergonomics, because relaxing it is exactly what would have to be undone if user submissions return.

## 10. Baking and execution

**Bake** produces, per skin, per animation step: resolved colour tables, gradient and pattern *descriptions*, and RGBA tiles for procedural and tinted-image sources — all plain data, so the recorder still captures them exactly and the comparator can still see them. Browser objects are materialised lazily by the `Web` arm and cached per context (the arena canvas, plus one per roster glyph canvas). Blending between layers that can be pre-composed happens at bake time, so per-frame emission is `source-over` only.

Bake resolution is `BAKE_PX_PER_CELL`, **derived from the largest cell any surface can request — not from the arena alone.** The arena caps at 15 px on a canvas that is not DPR-scaled, but the roster glyph paints through the same `paint_alive` at ~17 px cells on a canvas that *is* DPR-scaled (`render.rs:612-630`, `RosterSnakeCanvas.tsx`), so a DPR-2 display asks for ~34 device px per cell. Deriving from the arena would upsample every roster glyph from day one. Tiles are baked at the maximum across surfaces, with a rebake path if a surface later exceeds it.

Per frame, per snake: build the silhouette path once, `clip` once, then for each layer set its style and emit `1 + 2R` ops for `R` runs. **One clip per snake, never per layer** — per-layer clipping is what would make this expensive.

## 11. Performance

Budget: no per-frame allocation, no per-frame JSON, O(snakes) not O(cells) dispatch — unchanged from `specs/skins-prd.md` section 10.

Grounding: a snake costs **12–151 ops today** (median ~60; four-corner zigzag 151), against ~2,300 grid-dot arcs per frame that dominate regardless.

A classic-equivalent composite skin is roughly **4 layers × (3 + 2R)** plus the clip and the per-cell ramp. At `R = 5` that is ~55 ops plus the ramp — the same order as today, and the honest framing is *comparable, not free*. A rich six-layer image skin roughly doubles a snake's cost, which at eight snakes is still an order of magnitude below the grid.

Gates, measured on a frame with **distinct skins actually resolved** — an all-classic frame is not representative, and per-snake resolution already ships (`render.rs:1895-1903`), so this is a fixture requirement rather than a dependency:

- eight snakes, longest bodies, six distinct skins resolved, no frame over 8 ms p95 on the agreed low-end target
- zero allocations on the per-frame path (the compositor must *reduce* today's ~11 `String`s + 2 `Vec`s + `HashSet` per snake, not match them)
- registration of eight distinct skins within a stated budget, with an eviction policy

## 12. Classic parity and migration

This is the central risk and the reason the staging is shaped as it is.

**The problem.** Classic's appearance is pinned as op text. A compositor lowering the same picture emits a different op stream — clipped blits instead of stroked capsule unions. Op equality cannot survive, and pretending otherwise would mean either constraining the compositor to the old grammar forever, or discovering the loss under schedule pressure and abandoning the guarantee.

**The plan.**

1. Build the **comparator** first — and build it *in the browser*, not as a software rasterizer.

   The obvious design is a Rust rasterizer over the recorded op stream. It does not work. Comparing a stroked capsule union against a clipped blit means comparing antialiased edges, and **20–45% of a snake's pixels sit on an antialiased edge** (measured: 21% for a 21-cell body at 10px cells, 41% at 5px). Any independent rasterizer disagrees with Skia across that whole band — by 5–30 levels, not 1 — so a ≤1/255 tolerance over ≥99.9% of pixels is unreachable by two orders of magnitude. A software rasterizer would be a checker whose own correctness is unverified, quietly failing to prove the one thing it exists to prove.

   Instead: render both lowerings to two canvases **in the same headless browser**, and diff with `getImageData`. Same Skia, same antialiasing, same subpixel rules — so every remaining difference is a real difference. This is the same machinery as Gate 2, which is why the two are one piece of work rather than two.
2. **Fixtures and baselines before anything moves.** Add a one-cell-run pose (consecutive turns — legal, and absent from the current corpus) and a tile-wrapping length, captured from the **unchanged** painter under an additive-only gate: no existing golden line may be removed. In the same stage, capture browser baselines for **all six** shipped skins. Classic gets the elaborate treatment below; the other five get nothing else, and without a baseline captured here, "no visible change" in S9 is not a checkable claim about them.
3. Ship the compositor as an **additional** `SnakeSkin` implementation. Classic keeps painting through `paint_body`. Nothing about the shipped game changes.
4. Express classic as layers. Prove `pixels(classic_layers) ≈ pixels(classic_strokes)` — both rendered by the browser, diffed in place — across the full fixture corpus.
5. **Flip and bless.** Classic switches to the compositor; the `## alive` section **and the six roster glyph blocks** are re-recorded. The pre-flip trace is retained permanently as a frozen oracle.

   The oracle needs one piece of machinery the trace alone does not provide: the comparator consumes `&[PaintOp]`, but the committed artifact is text produced by a one-way serializer. Either commit the pre-flip ops in a re-readable form (serde, alongside the human-readable trace), or commit the pre-flip *rendered PNGs* and compare against those. The second is simpler and is what S1's baselines already produce — so the oracle is the baseline set, and the frozen trace is documentation.
6. `paint_body` and the stroke lowering are deleted once nothing calls them — which means after `ember@1` and the document engine are both rebuilt on the compositor (S9), not at the flip.

**Tolerance.** Byte equality for same-grammar comparisons. For cross-grammar comparison, measured over **the snake's bounding box dilated by `overhang_px`** — not the whole frame, where a 99.9% budget would silently permit hundreds of wrong pixels on a mostly-empty arena: **max per-channel delta ≤ 1 over ≥ 99.9% of that population, and ≤ 4 everywhere**, with a committed diff image. Demanding zero delta between a stroked capsule union and a clipped blit is not achievable and would be quietly dropped the first time it blocked a merge.

**If the job is not funded.** There is a legitimate cheaper plan: **never flip classic.** `ClassicSkin` keeps its hand-written painter and its golden trace untouched and permanent; the compositor serves only new skins. The parity guarantee is preserved by never spending it. Cost: `paint_body` lives indefinitely alongside the compositor, and classic cannot use compositor features. This is the correct choice if browser CI is not going to exist — flipping the look every player sees on the strength of eyeballed contact sheets is not a trade worth making.

**Schema.** Bump to version 2, convert all four catalogue documents plus the test-only `classic-doc@1` in the same commit, and relax the validator from `schema_version != 1` to a supported-set check so a version bump never again silently empties the catalogue. Refs keep `@1` — a ref names the look, not the implementation.

## 13. What conformance still proves — and what it stops proving

The pixel validator is deferred. Three properties therefore degrade from machine-enforced to **review-enforced**. This section exists so the next reader does not assume the checks still mean what they used to.

| Property | Today | Under the compositor, without the pixel validator |
| --- | --- | --- |
| Op-count invariance under animation | Machine | **Machine** — unchanged, and the static-topology rule strengthens it |
| Reduced-motion stillness | Machine | **Machine** — unchanged |
| Flat-hex reported colours | Machine | **Machine** — but see below |
| Overhang honesty | Machine | **Machine only once the recorder tracks the CTM and clip** (S3). Until then, the first transform op silently turns the check into a no-op that still passes. |
| Boost-band visibility | Op-text comparison | **Structural** — body layers cannot reach the band (5.2) and the band is pinned outermost in the contour region. The op-text check passes trivially for an opaque overlay and was never a real defence. |
| Team hue windows | `colors().fill` | **A claim, not an observation.** Once the body is a texture, the reported fill is what the skin says it is, not what it paints. |
| Label contrast | Closed form over a flat fill | **Degrades to a claim.** The number is anchored ~2 cells *behind* the head (`CARRIED_LABEL_OFFSET_CELLS`), not in the head core — so the head-core rule protects the wrong region entirely. The closed form reasons about `colors().fill`, which stops describing the pixels once a body layer is a texture. A skin whose art sits under the label is unenforceable until the pixel validator lands. |

When the content validator eventually lands, it attaches to the **browser comparator's** output (section 12) — the emission seam, after clipping and transforms — not to a bake-time tile. Every property in the table is a property of what actually reaches the canvas, which a tile cannot see. The comparator is therefore shared infrastructure: parity uses it now, content validation uses it later.

## 14. Corners

Derived, not asserted: a cell-wide ribbon turning 90° leaves an unfilled outer wedge of `c² − πc²/4` — **21% of a cell's area, at every cell size**. It does not improve on larger cells; it grows in absolute pixels.

Three policies, default first:

- **`own`** (default) — each run paints its own half of the joint cell. Artifact: a hard 90° orientation flip along the joint-cell boundary, across the full ribbon width. On a solid or a gradient this is invisible; on a directional texture it is a visible break, and it must be described honestly to skin authors rather than glossed.
- **`bisector`** — split the joint along the 45° bisector. Softens the break into two shorter ones.
- **`fan`** — a proper polar fan through the corner. Ships only with a skin that proves it is needed.

**`bisector` and `fan` apply only to sources that can be path-filled** — solid and gradient. A diagonal or polar boundary is not an axis-aligned rectangle, and `drawImage` can only fill one; honouring it for an image or pattern layer would need a per-run clip, which section 10 forbids on cost grounds. **Image and pattern layers are `own`-only**, and the schema rejects any other combination rather than silently ignoring it.

Mitre and fillet are rejected: both change the silhouette, which changes overhang, which moves the roster layout.

## 15. Edge cases

| Case | Required behaviour |
| --- | --- |
| Body shorter than a span's `min` | Layer skipped; the base layer shows through. Never compressed. |
| Two spans overlapping on a short body | Priority order allocates; ties resolve by layer order. |
| One-cell run (consecutive turns) | Two corners in one cell; joint policy applies twice. Fixture required. |
| Self-crossing body | Arc length is monotonic; paint dedup is separate. Fixture required. |
| Single-cell snake | Structurally distinct: no mask, no white head overlay. Explicit in the layer model, not an accident of lowering. |
| Atlas not yet decoded | Fallback layers paint. A skin must be legible with no atlas. |
| Unknown skin ref | Classic, as today. |
| `reduced_motion` | Step 0, always. |
| Animated parameter with a 4 s period | Nyquist rule flags it; 125 ms plateaus are visible stepping. |

## 16. Required fixes to existing code

Landing the compositor requires three corrections to code shipped with the skins work:

1. **`walk_cells_from_head` conflates arc length with paint dedup** (`client/src/skin/geometry.rs:131-154`). The `seen` set both suppresses double-painting and gates the distance increment. A live body cannot self-cross, so this is not a correctness bug today — but body space needs a clean arc-length counter, and entangling it with paint bookkeeping is how it becomes one. Separated in S3, gated on the goldens not moving.
2. **The recorder does not track the CTM or the clip.** The moment a transform op is emitted, `painted_extent` measures pre-transform coordinates and the overhang check becomes a silent no-op that still passes. The recorder must track both, and a `TransformLiar` test skin (one that paints inside its overhang, then translates outside it) must land in the same commit as the first transform op.
3. **~~`RegistryRef` drops three trait methods~~** — fixed while writing this PRD; it silently tested defaults instead of the skin's own `paint_dead`, `base_theme` and `celebration_theme`.
4. **~~The arena resolved every snake to classic~~** — fixed while writing this PRD (`render.rs:1895-1903`); the cosmetic map reached base dressing and celebrations but never the snake bodies.

## 17. Deferred and out of scope

1. **Gate 2 does not exist.** The `@skins` bitmap suite described in `specs/skins-prd.md` section 6.2 was never built — the capture script performs no comparison, there are no baselines, and there is no `skin:approve`. This is new funded work and a **prerequisite** for the flip in section 12, because that is precisely where the op stream stops being a proxy for appearance.
2. **No CI job builds wasm or JS — and this is now a blocker, not an open question.** Native op goldens run in CI, but the comparator in section 12 is browser-based by necessity, so the wasm-pack + Playwright job is a hard prerequisite, built in S2 and gating the S6 flip. The skins PRD left this decision open (its section 15.4); this work closes it: either fund the job, or do not flip classic (section 12, "If the job is not funded").
3. **The composite/pixel validator** — section 13.
4. **User-submitted skins.** If they return, three v2 decisions need rework: the DSL's totality is a sandbox boundary and must not have been relaxed; asset delivery has no content addressing, size cap, or provenance; and `SkinColors` is trusted today but becomes untrusted input.
5. **Anisotropic overhang, per-frame blending, per-skin step counts, DPR scaling, `ctx.filter`, conic gradients** — sections 3.2 and 10.
6. **Unchanged by this work**: `corpse.rs`, the `SnakeSkin` trait shape, `SnakePose`, `SkinIdentity`/`SnakeRole`, `SkinColors`/`SkinMetrics`/`BaseTheme`/`CelebrationTheme`, the occlusion mask, and the `## palette` and `## dead` golden sections.

## 18. Staging

| Stage | Content | Gate |
| --- | --- | --- |
| S0 | Perf smoke harness. **Not** arena skin wiring — that already ships (`render.rs:1895-1903` resolves a skin per snake from `state.skins`). | Baseline numbers on a frame with six distinct skins resolved |
| S1 | New fixtures (one-cell run, tile-wrapping length) from the **unchanged** painter, **plus browser baselines for all six shipped skins** — the only appearance oracle the five non-classic skins will ever have | Additive-only golden diff; six baselines committed |
| S2 | wasm-pack + Playwright CI job; browser comparator (two canvases, one browser, `getImageData` diff). This *is* Gate 2. | Comparator returns zero delta on a known-identical pair and exceeds threshold on a known-different one |
| S3 | CTM/clip tracking in the recorder; `TransformLiar`; separate arc length from the dedup set in `walk_cells_from_head` | Overhang check demonstrably fails for `TransformLiar`; goldens unchanged by the walk refactor |
| S4 | Body space, per-run affine, corner policies, both clip shapes | Ribbon spike; corner artifact reviewed at 5px and 15px |
| S5 | Layer model, layer transforms, solid/gradient sources, bake groups | Classic-as-layers within section 12 tolerance |
| S6 | **Flip**: classic switches, `## alive` and the six roster glyph blocks re-blessed, pre-flip trace frozen as oracle | Section 12 tolerance |
| S7 | Patterns, tiling, the expression DSL | Conformance green |
| S8 | Atlas, spans, three-slice, fit modes, tinting, frame strips | Short-body corpus at 1/2/3/5/21 cells |
| S9 | Convert the four catalogue documents to v2; rebuild `ember@1` on the compositor; **then** delete `paint_body` | No shipped skin differs from its S1 baseline; nothing still calls `paint_body` |

## 19. Acceptance criteria

1. The compositor implements `SnakeSkin`; the trait, `SnakePose`, corpse painting and the occlusion contract are unchanged.
2. Classic renders through the compositor within the section 12 tolerance, with the pre-flip trace committed as a permanently re-checkable oracle.
3. All six shipped skins render through the compositor and none changed visibly, measured against baselines captured before conversion (S1).
4. A skin can place one atlas region over the first N cells, another over the last M, and tile a third between them, tinted per role — with a defined result at 1, 2, 3, 5 and 21 cells.
5. Static topology holds: no skin changes its op sequence with `anim_ms`.
6. Overhang honesty is machine-enforced *through transforms* (CTM/clip-aware recorder plus `TransformLiar`).
7. Perf gates in section 11 pass, measured after S0 wiring.
8. Section 13's degraded properties are documented in-code where a reader would otherwise assume the old guarantee.
9. Gate 2 exists, runs in CI, and gates the S6 flip — or classic was never flipped (section 12).
10. Layer transforms are specified, implemented, and covered by `TransformLiar` (sections 6.1, 16).

## 20. Implementation record

Built in the staging order of section 18. Four things differ from what this
document specified, and each is a decision worth carrying forward rather than a
detail of the build.

### 20.1 Classic's parity was never spent

Section 12 is built on the premise that a compositor must lower a solid body
layer as **clipped blits**, that op equality with the hand-written painter
therefore cannot survive, and that a pixel tolerance has to be budgeted for the
difference. The premise is wrong, and pleasantly so: the cheapest correct
lowering of a solid full-body layer *is* the stroked capsule union the classic
painter already emitted. Choosing that lowering makes classic-as-layers emit the
identical op stream.

The consequences run through the whole plan:

- **`client/src/skin/goldens/classic.trace` was never re-blessed.** Every one of
  its original 10,664 lines survives, in order, through all ten stages. It is
  still the trace recorded from the renderer that existed before the skin system
  did, and it still runs natively in CI.
- **The S6 flip provably changed nothing** — no tolerance, no eyeballing, no
  diff image. `paint_body` was deleted in S9 and the trace did not move.
- **The wasm + Playwright job is still a hard prerequisite**, exactly as section
  17 item 2 says, because it is the only appearance oracle the five non-classic
  skins have and the only way to check an image layer at all. It just is not
  what protects *classic*.

Two emission-order quirks are preserved deliberately to get this: classic sets
its fill style before its strokes, and re-sets it redundantly before the tail
disc. Both are no-ops for the picture. `RibbonPlan` carries them as two booleans
with the reason attached.

### 20.2 Spans are measured over the paintable body, not the arc length

Section 6's span grammar implies coordinates in arc length, which runs centre to
centre. A span over `0 ..= body_len` misses the head and tail caps — half a cell
at each end — so a base layer written that way does not cover the snake, and a
one-cell snake (arc length zero) is not painted at all. Spans run over
`-0.5 ..= body_len + 0.5` instead, which also makes the units what an author
expects: a body of *n* cells has *n* cells of span.

`Anchor::Whole` was added for the same reason. A base layer is not queueing for
room alongside the art; it is what the art sits on. Without it, `Span::WHOLE`
consumed the body at its own priority and every slice above it silently
disappeared — a three-slice skin rendering as a plain one.

### 20.3 Corner policy `fan`, and bake-time tinting, are not built

`fan` is absent as section 14 specifies: it ships with a skin that proves it is
needed, and a schema surface with no implementation is worse than an absence.
`own` and `bisector` are implemented and tested.

**Per-role tinting of a grayscale atlas region (section 8.4) is deferred**, and
this is a genuine reduction in scope rather than a rephrasing. A bake-time
multiply needs a canvas at registration, which the native test path does not
have — so building it would create a browser-only code path that no test in this
system can see, which is the specific thing the whole design exists to avoid.
Until there is a bake surface that works in both, a skin that wants per-role art
declares a region per role, and `ColorSlot::Accent` carries a third per-role
colour for tinting things that *are* path-filled (it is what paints Ember's
head). Frame strips, spans, three-slice, fit modes and placement are all built.

### 20.4 Measured against section 11

| Budget | Result |
| --- | --- |
| Frame time, 8 snakes, 6 distinct skins, all boosting | **p50 0.1 ms, p95 0.3 ms, max 1.4 ms** against an 8 ms gate |
| Per-snake ops | 60-86, unchanged from the stroke painter (`skin::perf`) |
| Per-frame allocation | 268 per 8-snake frame, **unchanged and still not zero** |

The allocation gate is the one section 11 target not met. The cost is the head
ramp's per-cell `rgba(...)` string and the white head overlay's — both preserved
deliberately, because replacing them with `globalAlpha` is what would finally
move the golden trace. It is a provably pixel-identical change (`aC + (1-a)B` by
either route) and the comparator can gate it, but it trades a byte-exact
guarantee for a tolerance-based one and should be a decision, not a side effect.

### 20.5 Three bugs the build surfaced

Recorded because each was invisible until something specific forced it into the
open, and each would have come back:

1. **The comparator reported a snake as differing from itself.** Chrome moves a
   canvas to software rasterization once it decides you are reading it back, so
   the slot every comparison read drifted onto a different rasterizer from the
   slot it was compared against — up to 61 levels across 240 pixels. Every
   canvas in the harness now claims `willReadFrequently` before anything draws.
   The self-check was extended to the full matrix, because the version that only
   compared calm snakes passed throughout.
2. **`drawImage` is not a byte copy.** Building the baseline sheet by
   compositing tiles shifted the Boost band's saturated yellow by up to 4
   levels — inside the tolerance this suite exists to measure. Sheets are built
   with `putImageData`, and baselines are decoded with
   `colorSpaceConversion: 'none'`.
3. **A baseline recorded over a different fixture matrix reads as a pixel
   regression** in every tile after the first missing row. The suite now checks
   the sheet's height against the matrix and says what actually happened.
