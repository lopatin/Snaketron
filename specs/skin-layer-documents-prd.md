# PRD: Layer documents — SkinDoc v2 and the layers-panel Builder

| Field | Value |
| --- | --- |
| Status | Proposed |
| Product | Snaketron skins: document schema, client renderer, Skin Builder |
| Scope | Replaces SkinDoc v1's closed vocabulary with a document form of the layer compositor; replaces the Builder's parameter form (including the entire Animations section) with a layers panel over that model |
| Depends on | `specs/skin-shading-prd.md` (shipped: the compositor, body space, the expression language), `specs/first-class-skins-prd.md` (shipped M0–M5: storage, equip, review, textures, economy) |
| Supersedes | The "SkinDoc v2 layer vocabulary" milestone (M2) sketched in `specs/first-class-skins-prd.md` |
| Owners | Product / Client |
| Last updated | 2026-08-19 |

## 1. Executive decision

The renderer is already generic. `specs/skin-shading-prd.md` built a declarative
layer compositor — regioned frame, body space, spans, gradients, bands, images,
a total expression language with derived evaluation tiers, and a baked
32-step animation ring — and every first-party skin ships on it today. The
**document format never caught up**: `SkinDoc` v1 is a parameter bundle for
exactly one layer stack (classic's seven layers), and the Skin Builder is a
form over that bundle. The Builder's Animations section is the clearest symptom:
it edits a closed list of three modulation targets and one wave shape, because
that is all v1 can say — while one level down, the compositor it compiles into
can animate any layer's opacity, a disc's radius, a band's alpha by expression,
and a texture's drift.

The decision:

- **SkinDoc v2 is the compositor's own model, serialized.** A skin is a stack
  of layers. Each layer has a region, a clip, a kind (ribbon, span+source,
  head disc, head ramp), a transform, and an opacity. There is no separate
  animation vocabulary and no Animations section: **animation is what happens
  when a property's expression reads `time`**. A constant is just a constant
  expression.
- **The Builder becomes a layers panel and a preview — nothing else.** Like
  Photoshop or Sketch with the canvas tools removed: a reorderable stack on
  one side, an inspector for the selected layer on the other, the always-
  animating preview on top. No direct manipulation on the canvas. The
  inspector is generated from the schema descriptor, so the UI cannot drift
  from the object model — the discipline `describe.rs` already enforces,
  extended to the new vocabulary.
- **Every competitive gate survives, enforced on outcomes instead of on
  vocabulary.** v1 could check label contrast analytically because it knew the
  whole structure in advance. v2 cannot, so validation gains a deterministic
  body-space **sampler** that composites the document the same way the
  renderer does and checks the result — every step, every role, natively,
  no browser.

The north-star example, from the brief that motivated this document: a shine.
In v1 it is inexpressible. In v2 it is one layer —

```jsonc
{
  "name": "Shine",
  "type": "span",
  "region": "body",
  "span": { "from": "whole" },
  "source": {
    "type": "gradient",
    "axis": "along_body",
    "stops": [
      { "offset": "saw(time) - 0.15", "color": { "slot": "fill" },     "alpha": "0" },
      { "offset": "saw(time)",        "color": { "literal": "gleam" }, "alpha": "0.55" },
      { "offset": "saw(time) + 0.15", "color": { "slot": "fill" },     "alpha": "0" }
    ]
  },
  "opacity": "1"
}
```

A whole-body gradient whose crest sits at `saw(time)`: a gleam that travels
head-to-tail once per cycle — a gradient whose snake-relative coordinate
depends on time, which is all a "shine" ever was. No track targets, no wave
spec, and no schema change the next time someone wants the shine to breathe
(`"opacity": "0.7 + 0.3 * sin(tau * time)"`) instead of travel.

## 2. Product problem

1. **The vocabulary gap is the real ceiling on first-class skins.** M0–M5
   shipped creation, ownership, publishing, moderation, texture generation and
   the economy — and every skin a player can author is still a re-tint of
   classic's structure. `Source::Image` exists in the renderer and no document
   can name an image. The texture pipeline (M3) can mint a leopard coat and no
   document can wear it.
2. **The Animations section is wrong in kind, not in degree.** It is a UI over
   `AnimationSpec`: three `TrackTarget`s (body lightness, outline lightness,
   gradient opacity) and one `WaveSpec`. Those are not primitives — they are
   the three parameters classic's fixed stack happened to want modulated. Every
   new effect (a travelling shine, a pulsing accent band, a flickering ember)
   would need a new track target, a new bounds constant, a new validator arm
   and a new widget. That is a vocabulary treadmill, and the section exists
   only because v1 has no generic way to say "this property varies with time."
3. **Rust escalation does not scale.** Ember, the checkerboards, and the
   animal family are all layer stacks — written in Rust, because documents
   cannot express layers. Each escalation is a client release, is closed to
   non-programmers, and adds a skin the schema can never describe.

## 3. Goals and non-goals

### 3.1 Goals

1. A v2 document schema that serializes the layer model the renderer actually
   runs: layers with regions, clips, spans, sources (solid, gradient, band,
   image, text), transforms, and expression-valued properties.
2. Animation with no vocabulary of its own: any paint-argument property
   accepts an expression over `s, t, len, time, boost, seed`; the existing
   tier derivation and 32-step bake decide the cost.
3. Textures from M3 usable in documents by content reference, with the
   resolution ladder picked per arena cell size.
4. Validation that keeps every gate v1 enforced — boost telegraphy, team hue
   windows, label and head-core contrast, overhang caps — plus the cost gates
   the open vocabulary makes necessary (op ceiling, bake budget), all
   evaluable natively in CI, in the server, and in wasm.
5. The Builder rebuilt as stack + inspector + preview, generated from the
   descriptor, with the Animations section deleted and nothing authored in v1
   lost (v1 documents open and convert losslessly).
6. Op-count invariance, reduced-motion stillness, `classic.trace`, and the
   200-op ceiling all preserved bit-for-bit — v2 adds lowerings, it does not
   touch classic's.

### 3.2 Non-goals

- **A wysiwyg canvas editor.** The preview is output, never input. Direct
  manipulation is a later product, and nothing here forecloses it.
- **New paint primitives.** v2 reaches the compositor that exists. Blend
  modes stay bake-time; `ctx.filter` stays banned; no WebGL.
- **Exposing classic's parity knobs.** `fill_before_strokes`,
  `refill_before_tail_cap`, `single_pass` exist for byte-exact classic parity
  (`client/src/skin/layer.rs`) and are defaulted for documents, not authored.
- **Changing shipped pixels.** The four catalogue v1 documents and
  `classic-doc@1` keep rendering byte-identically. v1 remains a valid schema
  version forever.
- **User-defined effects for celebrations.** `celebration.effect` stays a
  closed list of first-party renderers.
- **Per-skin animation step counts.** 32, as shipped.

## 4. Repository baseline (verified)

What exists, and the precise gaps between it and a document author:

- **The layer model** (`client/src/skin/layer.rs`): `Layer { id, region, clip,
  kind, transform, boost_only, omit_on_single_cell, opacity_track }`; kinds
  `Ribbon`, `HeadRamp`, `HeadDisc`, `Span`; sources `Solid`,
  `LinearAlongBody`, `RadialFromStart`, `Tiled`, `Image`; fits `Clip`,
  `Stretch`, `Tile`, `Cutout`; `Fade`; `Span`/`Anchor`; `ColorSlot` including
  `Literal(usize)`. Layer count and order fixed at registration; presence a
  function of the pose (`boost_only`, `omit_on_single_cell`), never the clock.
- **The baked ring** (`client/src/skin/composite.rs`): `Frame` carries per-step
  swatches plus three generic tables — `layer_opacity: Vec<f64>`,
  `scalars: Vec<f64>`, `literals: Vec<String>` — indexed by tracks on layers.
  `CompositeSkin::with_atlas` validates the stack at registration
  (`validate_layers` + atlas cross-checks: missing regions, undeclared
  images, zero-pixel regions, non-finite fades).
- **The expression language** (`skin-schema/src/expr.rs`): total, depth-capped
  at 50, no exponent notation, inputs `s, t, len, time, boost, seed`,
  fourteen functions, division-by-zero yields 0, every result finite. Tiers
  **derived** from what an expression reads: `Constant` folds at registration,
  `PerStep` bakes 32 values, `PerCell` rides the cell walk, `PerTexel` bakes
  into a tile. `Source::Tiled.alpha` already takes an `Arc<Expr>`.
- **Conformance** (`client/src/skin/conformance.rs`): boost visibly different;
  painted extent inside reported overhang (with adversarial fixtures that
  lie); flat hex colours; **animation only varies paint arguments** — the op
  *sequence* is compared across clock readings, so anything that changes
  emission count with time fails; reduced motion is actually still; team
  colours stay on their side; document-classic equals reference-classic.
- **Cost** (`client/src/skin/perf.rs`): `PER_SNAKE_CEILING = 200` ops,
  measured by the recorder.
- **The v1 pipeline**: `skin-schema/src/lib.rs` (SCHEMA_VERSION = 1, the
  closed `AnimationSpec`), `client/src/skin/doc.rs` (compiles v1 into
  `document_layers(...)` — the fixed seven-layer classic shape — plus a baked
  `Frame` ring), `skin-schema/src/describe.rs` (the descriptor the Builder
  renders, with the exhaustiveness guard that destructures every document
  struct so a new field cannot ship without a control).
- **Textures** (M3): content-addressed PNG rungs at 64/32/16 texels per cell
  (coats/overlays) and 16/8 (sheets), stored by digest, served through the
  API; `client/src/skin/atlas.rs` loads images by URL and names rectangles.

The gaps, exactly:

1. `Layer.id` is `&'static str` — documents need owned ids.
2. Transforms, gradient stops, band lanes, disc radii are static `f64` — the
   generic animation surface exists only for opacity (`opacity_track`), disc
   radius (`radius_track`), band alpha (expr) and image drift.
3. `Source::Image.region` indexes a Rust-declared atlas — no document can
   declare textures.
4. `SkinDoc` cannot say "layer" at all; `describe.rs` has no tagged-union
   field kind, so even if it could, the Builder couldn't render the choice.

## 5. The v2 document model

### 5.1 Envelope

```jsonc
{
  "schema_version": 2,
  "id": "molten@1",
  "name": "Molten",
  "palette": { /* unchanged from v1: RolePalette */ },
  "labels":  { /* unchanged from v1 */ },
  "base":         { /* optional, unchanged */ },
  "celebration":  { /* optional, unchanged */ },

  "literals": { "gleam": "#fff7d6", "char": "#2b1d16" },
  "textures": [
    { "name": "lava",  "ref": "sha256:9f2c…", "kind": "coat" }
  ],
  "period_ms": 2400,
  "layers": [ /* section 5.3 */ ]
}
```

What survives from v1 unchanged, and why:

- **`palette`** is not a look, it is the team-communication system: two
  friendly shades, two enemy shades, four free-for-all slots, hue-window
  checked. Layers name palette colours through slots, which is what keeps a
  skin readable on both sides of a match without authoring it twice — the
  Own/Enemy preview pair renders one document.
- **`labels`, `base`, `celebration`** are dressings with their own consumers
  (roster, arena, victory screen) and closed rules; nothing about layers
  changes them.

What dissolves: **`head`, `outline`, `animation`**. The head gradient, the
outline ribbon and the body ribbon become ordinary layers in the default
template (section 9.4). `animation` has no v2 form at all — `period_ms` moves
to the envelope (it is the one global the bake needs), and everything else a
track or wave could say is an expression on the layer it affects.

### 5.2 System layers: what an author cannot touch

Two layers are engine-owned, always present, and rendered outside the
document's stack. The Builder shows them as locked rows so the stacking is
honest, but they carry no editable properties beyond what v1 already allowed:

- **Boost band** — outermost contour, pinned to `#fff200` at 6px, painted only
  while boosting. Position in the stack is structural (`Region::Contour`
  ordering), so no document layer can bury it; colour and width are pinned by
  the validator exactly as v1 pins them.
- **Head core** — topmost, `core_ratio` and `core_color` remain document
  properties (the label-contrast and ready-check rules need a flat disc of a
  known colour), but its position is not authorable: it composites over
  everything, which is precisely what lets an image or text layer span `s = 0`.

Everything between those two is the author's.

### 5.3 The layer

```jsonc
{
  "name": "Lava coat",              // author-facing; ids are derived
  "type": "span",                   // ribbon | span | head_disc | head_ramp
  "region": "body",                 // contour | body | head
  "clip": "silhouette",             // silhouette | cells
  "boost_only": false,
  "omit_on_single_cell": false,
  "opacity": "0.8 + 0.2 * sin(tau * time)",   // expression; "1" is opaque
  "transform": {
    "translate_s": "0", "translate_t": "0",
    "scale_s": "1", "scale_t": "1",
    "rotate_turns": "0"
  },
  // type: "span" only —
  "span": { "from": "head", "natural": null, "min": 0, "priority": 0 },
  "corner": "fan",
  "source": { /* section 5.4 */ }
}
```

Kind-specific properties mirror the Rust enums: `ribbon` exposes `color`,
`extra_px` (contour only; the overhang source), `joints`, `tail_cap`;
`head_disc` exposes `paint` and `radius_ratio` (expression); `head_ramp`
exposes `color` and `length_cells`. The parity knobs are defaulted and absent
from the schema.

**Groups** exist in the document as authoring structure only:

```jsonc
{ "type": "group", "name": "Head dressing", "opacity": "1",
  "transform": { … }, "layers": [ … ] }
```

The compiler flattens groups before registration — a group's opacity
multiplies into its children's expressions, its transform composes with
theirs. The compositor never sees a group, so the runtime, the recorder, and
every conformance check are untouched. One level of nesting; a group inside a
group is a validation error (depth buys little and costs the panel its
legibility).

### 5.4 Sources

| `type` | Lowers to | Notes |
| --- | --- | --- |
| `solid` | `Source::Solid` | one colour ref |
| `gradient` | `LinearAlongBody` / `RadialFromStart` | `axis: "along_body" \| "from_start"`; stops carry expression offsets and alphas |
| `band` | `Source::Tiled` | the procedural stripe/dash/checker element |
| `image` | `Source::Image` | names a texture from `textures`; fit `clip \| stretch \| tile \| cutout`; fade; drift |
| `text` | per-cell `DrawImage` from a first-party glyph atlas | section 5.6 |

Colour references are `{ "slot": "fill" | "outline" | "accent" | "head_core" }`
or `{ "literal": "<name>" }`. Slots resolve per role from the palette — this
is the mechanism that keeps one document readable as friend and as enemy.
Literals resolve from the envelope table and are hue-checked by the sampler
(section 7), not by a per-colour window: a gold shine on a blue snake is
legal; a snake that *reads* red on the friendly side is not.

### 5.5 What may animate, and what must not

The conformance suite compares op sequences across clock readings, so the
schema encodes the rule the renderer already lives by: **an expression may
change what an op paints, never how many ops there are.**

Animatable (paint arguments): layer `opacity`; all five `transform` fields;
gradient stop `offset` and `alpha`; band `half_width`, `t_center`, `alpha`;
`head_disc.radius_ratio`; image `drift_cells` rate. All accept expressions;
the bake places each by its derived tier (fold constants; 32-entry tables for
`PerStep`; the cell walk for `PerCell`; baked tiles for `PerTexel`). Every
evaluated result is clamped to the property's legal range before it reaches
canvas — the same posture `expr::eval` already takes with non-finite values,
and load-bearing for stop offsets, where `addColorStop` outside `0..1` throws
rather than clips.

Static (structure and emission count): `type`, `region`, `clip`, span
anchor/lengths/priority, `corner`, band `period_cells` / `duty` /
`phase_cells`, fade lengths and steps, fit parameters (`cells_per_repeat`,
`cells_tall`), text content and spacing, ribbon `extra_px`, `boost_only`,
`omit_on_single_cell`, stop *count*, layer count and order.

The band fields are the instructive case and the reason this split is a
schema-level rule rather than a review note: `Tiled` emission skips repeats
that fall outside the painted window (`composite.rs` — `painted <= 0.0`
returns early; `to <= from` continues), so `period`, `duty` and `phase`
change the *number* of rectangles. Animate them and the skin fails
`skin_conformance_animation_only_varies_paint_arguments` — so the schema
makes them plain numbers and the Builder never offers the fx toggle there.

### 5.6 Text along the body

A `text` source renders a string one character per cell along its span —
the brief's example is a name worn as a skin. Design:

- Glyphs come from a **first-party glyph atlas** bundled like any other skin
  art (A–Z, 0–9, a small punctuation set; one style at launch). A document
  chooses `content`, a colour ref, and per-cell scale. No user fonts — a font
  file is executable-adjacent complexity with none of the sandbox properties
  the rest of this system has.
- Lowering emits **one `DrawImage` per covered cell**, character chosen by
  cell index (the string repeats along the span). The op count is a function
  of the pose, never the clock or the string's length beyond the span —
  identical to how image tiles already behave.
- `content` is validated: charset membership, length ≤ 24. Moderation of the
  *words* is the existing review dimension from M4 — a skin is reviewable
  content whether the word is painted in pixels or in glyphs.

### 5.7 Textures by reference

`textures[]` entries name M3 objects by content digest. At compile time each
becomes an atlas image whose URL is the texture API route for the digest, with
the **ladder rung chosen by arena cell size** (64-texel rungs at cell ≥ 24px,
32 at ≥ 12, else 16 — the thresholds live in one function shared with the
preview so the Builder shows what the arena will show). Registration keeps the
existing failure posture: a texture that 404s/410s leaves the skin unrenderable
and the equip falls back to classic, exactly as an unresolvable content ref
does today.

Validation at save: every `ref` must resolve for the saving user (own texture
or published). Validation of *pixels* happened at forge time (M3); the
document trusts the digest, and the digest is verified end-to-end by the
store.

## 6. Animation worked through

The v1 vocabulary, expressed in v2 — this table is also the conversion the
Builder applies when opening a v1 document (section 10):

| v1 | v2 |
| --- | --- |
| track `body_lightness`, amplitude a, phase p | on the body ribbon: a lightness overlay layer (solid white, `opacity: "a * sin(tau * (time + p))"` clamped ≥ 0) paired with a black twin for the negative half — or, exactly as the v1 lowering does it today, baked swatch shifts; the converter uses the swatch form for fidelity |
| track `outline_lightness` | same, on the outline ribbon |
| track `gradient_opacity` | on the head-ramp layer: `opacity: "base + a * sin(tau * (time + p))"` |
| wave (cells_per_crest c, amplitude a, speed v) | on the head-ramp layer: per-cell opacity term `a * sin(tau * (s / c - v * time))` — `PerCell` tier, riding the walk the ramp already does |

New things that were impossible, now one layer each:

- **Shine** — the section 1 example: a whole-body gradient whose crest
  offset is `saw(time)`. (Deliberately *not* a layer-transform translation:
  a translated source paints in each run's own frame, so it would leave the
  body's path at the first corner. Stop offsets travel along arc length,
  which is what a shine means.)
- **Pulse** — an accent band, `"opacity": "0.5 + 0.5 * sin(tau * time)"`.
- **Ember flicker** — `"opacity": "0.6 + 0.4 * noise(seed, floor(time * 8))"`,
  different per snake because `seed` is per-snake.
- **Boost-reactive coat** — `"opacity": "mix(0.7, 1.0, boost)"` on an image
  layer; legal because presence is not changing, only alpha.

Bounds: `period_ms` keeps its v1 range (120ms–60s). The v1 amplitude cap
(`MAX_ANIMATION_AMPLITUDE = 0.35`) does not carry over as a number — it was a
proxy for "stays readable mid-cycle", and v2 checks readability itself, at
every step, with the sampler.

## 7. Validation: structure, cost, sampler

Three gates, all in `skin-schema` + the shared compiler, so CI, the server
and the browser Builder run the identical judgement.

**7.1 Structure** (exists, extended): region discipline, clip shapes, band
lanes inside the silhouette, contour-only overhang ≤ caps, pinned system
layers, group depth, literal/texture reference resolution, expression parse +
tier legality per property (a `PerCell` expression on a per-step-only property
like layer opacity is an error naming the property and the input that caused
it), static-field enforcement (an expression where a number belongs is a parse
error, not a silent freeze).

**7.2 Cost**: the op count becomes a *predicted* number at registration — a
per-layer closed formula (runs × slices × repeats at the worst-case pose
family: maximum body length and maximum corner density the arena permits)
summed over the stack and required ≤ the existing 200 ceiling, plus: ≤ 24
layers after flattening, ≤ 8 stops per gradient, ≤ 4 texture refs, ≤ 2
`PerTexel` expressions (each bakes a tile per role per step in the worst
case — the budget is memory), bake ≤ 256KB. The formula is property-tested
against the recorder: for a corpus of generated documents, predicted ==
recorded, so the meter in the Builder and the gate in the validator cannot
drift from the truth.

**7.3 The sampler** — the new instrument, and what un-defers (a bounded form
of) the pixel validator the shading PRD deliberately postponed while skins
were first-party-only. Every source is deterministic data in body space —
rectangles, gradients, bands, image pixels we hold, glyphs we bundle — so the
validator can *composite* small patches natively, with no canvas and no
browser, using the same math the renderer uses:

- **Label patch**: the cells behind the head where the name and carried-food
  digits sit. Checked: ink contrast ≥ 4.5:1 against the composited result at
  **all 32 steps × all 8 role slots**, worst case wins. This is v1's lit-label
  rule generalized from "fill + gradient" to "whatever the stack paints
  there".
- **Head patch**: core contrast ≥ 2.0 against the composite surrounding it;
  ready-check ≥ 3.0 on the core (unchanged — the core is still a flat disc).
- **Side reading**: area-weighted mean hue of the composited body per role,
  chroma-weighted so neutrals don't dilute the verdict; must sit in the
  role's window (or read neutral) for friendly, enemy, and the two spectated
  free-for-all slots. This is what replaces per-colour hue checks once
  literals and images enter: it judges what a player actually sees.
- **Boost visibility** stays structural (the band is pinned outermost and
  unoccludable by region rules) — the sampler does not need to prove what the
  frame's geometry already guarantees.

The sampler is conservative by construction (patches are where the rules
bind; steps are exhaustive because animation is a ring, not a continuum) and
it is *not* claimed to be a general art critic — M4's review queue remains the
judgement of taste and abuse. `classic-doc@1` keeps its recorded exemption.

## 8. Server and API impact

Nearly none, by design: revisions are content-addressed blobs and the server
validates with the same `skin-schema` crate, so v2 acceptance ships by
dependency bump. Version gating keeps the shipped rule — a client that does
not understand `schema_version: 2` falls back to classic rather than
guessing. Two additions: the texture-reference resolution check at save time
(section 5.7), and the browse endpoint exposing `schema_version` so old
clients can skip fetching documents they cannot compile.

## 9. The Builder: a layers panel and a preview

### 9.1 Anatomy

```
┌────────────────────────────────────────────────────────────┐
│  PREVIEW — own · enemy · u-turn poses, always animating    │
├──────────────────┬─────────────────────────────────────────┤
│ LAYERS           │ INSPECTOR — the selected layer          │
│ 🔒 Head core     │                                         │
│ ⧉ Head dressing  │  (fields generated from the descriptor: │
│   ◦ Eyes         │   kind-specific properties, colour refs,│
│   ◦ Glow         │   expression fields with fx toggles)    │
│ ◦ Shine          │                                         │
│ ◦ Lava coat      │                                         │
│ ◦ Body           ├─────────────────────────────────────────┤
│ ◦ Outline        │ DOCUMENT — name, palette, period,       │
│ 🔒 Boost band    │ labels, textures, base, celebration     │
│ [+ Add layer]    │ COST — 143/200 ops · bake 96KB          │
└──────────────────┴─────────────────────────────────────────┘
```

- The stack renders top-over-bottom exactly as composited. Drag to reorder;
  locked rows (system layers) don't move and sit where they truly sit. The
  eye toggle is an authoring aid that sets opacity `"0"`/restores — the saved
  document has no hidden-layer concept, because the runtime has none.
- Selecting a row fills the inspector. Groups collapse and select as one.
- `[+ Add layer]` offers the source-level things an author thinks in — Solid,
  Gradient, Band, Image, Text, Ribbon, Head disc, Head ramp — each inserting
  the descriptor's validating default for that variant.
- The cost meter runs the same predicted-ops formula as the validator,
  continuously. Red at the ceiling is the same red the save would produce, so
  authors never discover the budget at publish time.

### 9.2 Expression fields

Every animatable property renders as a **number control with an fx toggle**.
A constant expression *is* a number, so the slider and the expression view are
the same value in two states — flipping to fx pre-fills the current constant;
flipping back is offered when the expression is constant-foldable. In fx
state: free-text entry, caret-precise errors from `ExprError.at`, a chip
naming the derived tier (`constant · per-step · per-cell · per-texel`) so cost
is visible where it is incurred, and input autocompletion for the six inputs
and fourteen functions. A `time`-reading expression animates the preview the
moment it parses.

### 9.3 The descriptor grows four kinds

`describe.rs` gains: `Variant` (tagged union: options, per-option children and
validating defaults — the layer `type` and source `type` switches),
`Expression` (bounds hint + which inputs are legal for this property),
`ColorRef` (slot-or-literal picker fed by the envelope's literal table), and
`LayerStack` (the reorderable list with locked entries). The exhaustiveness
guards extend as shipped: the Rust destructuring covers the new structs, and
the TypeScript `Control` switch ends in `const unhandled: never = kind`, so a
vocabulary addition without a control is a compile error on both sides of the
boundary.

### 9.4 Starting points, and the death of the Animations section

- New skins start from **templates**: Classic (the seven-layer stack v1
  compiles to), Minimal (ribbon + outline), Textured (image coat wired to the
  texture picker). Templates are ordinary v2 documents, not code.
- The texture widget from M2 (list, upload, generate, versions per
  resolution) becomes the payload of an image layer's inspector — same
  component, new home.
- The Animations section is deleted, not relocated. `period_ms` joins the
  document panel. The capabilities the section offered continue to exist as
  properties of the layers they always secretly belonged to (section 6's
  table), which is the entire thesis of this PRD.

## 10. Compatibility and migration

- **v1 validates forever.** `validate` accepts {1, 2}; stored revisions are
  immutable blobs; the four shipped catalogue documents and `classic-doc@1`
  are never rewritten.
- **The Builder edits v2 natively.** Opening a v1 document converts through
  the same lowering `doc.rs` performs today (made a pure
  `SkinDocV1 → SkinDocV2` function and shared), so the conversion is already
  proven by the classic byte-parity test. Saving writes a v2 revision — a new
  content ref, as every edit is.
- **Equip and browse are version-blind**: refs resolve to documents; clients
  compile what they understand and fall back for what they don't. Preview
  crops on the browse page render v2 through the same registry path as v1.

## 11. Renderer changes (scoped, additive)

1. `Layer.id: &'static str` → an interned/owned id (`Cow<'static, str>`), so
   document layers can be named. Mechanical; no behavior change.
2. **Generalized bindings**: the bake grows one `params: Vec<f64>` table per
   frame (subsuming `layer_opacity`/`scalars` rather than adding a third
   mechanism), plus per-cell binding slots evaluated in the existing cell
   walk. Transform fields and gradient stops read through bindings; static
   values compile to the fold, so v1-shaped stacks pay nothing new.
3. **Group flattening and the v1→v2 conversion** in the document compiler.
4. **Text source lowering** and the bundled glyph atlas.
5. **Texture-ladder atlas wiring**: atlas URLs chosen from a manifest by cell
   size, with the existing pending/ready machinery.

Explicitly not changed: `PaintOp` set, `classic.trace`, the recorder, the
occlusion path, `SkinColors` (still four flat hex answers, baked per role).

## 12. Testing

- **Parity**: classic-as-v2 (the converter applied to `classic.skin.json`)
  must emit `classic.trace` byte-for-byte — the same oracle that guarded the
  compositor flip now guards the schema flip.
- **Conformance**: all existing checks run over a set of v2 fixture skins
  (shine, band pulse, textured coat, text, grouped head dressing).
  Adversarial documents join the liar fixtures: an expression smuggled into a
  static field (must fail parse), a band trying to animate `duty` (must be
  unrepresentable), a group nested in a group, a 25th layer, a per-texel
  expression over budget.
- **Cost honesty**: property test — generated documents, predicted ops ==
  recorder-counted ops, across the pose family and the clock.
- **Sampler goldens**: known-bad documents (washed label at step 19, red-ish
  friendly coat, low-contrast core under art) each rejected with the step and
  patch named; known-good documents pass; `classic-doc@1` exempt path pinned.
- **Builder**: the descriptor exhaustiveness tests extended; Playwright flows
  for reorder, variant switch, fx toggle, texture pick, v1 open-and-convert;
  the capture tour gains the layers panel screens.

## 13. Milestones

| # | Deliverable | Gate |
| --- | --- | --- |
| V0 | v2 schema + v1→v2 converter + compiler to the existing model (ids, flattening; no new bindings yet) | classic-as-v2 emits `classic.trace` byte-for-byte |
| V1 | Generalized bindings (transforms, stops, radii), tier-checked; band/gradient/solid fixtures | conformance + cost property test green |
| V2 | Sampler + cost gates in `skin-schema`; server accepts v2 | sampler goldens; server round-trip |
| V3 | Builder layers panel + inspector + descriptor kinds; Animations section removed | Playwright flows; exhaustiveness guards |
| V4 | Image + text sources: texture refs, ladder wiring, glyph atlas | textured + text fixtures through conformance and the browser baselines |
| V5 | Templates, v1 open-in-builder, PR screenshots tour refresh | end-to-end: author → validate → save → equip → arena |

Each milestone is shippable behind the existing content-ref machinery; a v2
document saved before V4 simply cannot name an image yet.

## 14. Risks and open questions

- **Sampler overreach** — the patches check where the rules bind, not
  everything; a document can pass and still be ugly or subtly hostile.
  Mitigation: M4 review is unchanged and remains the publish gate; the
  sampler's job is to make *unreadable* unpublishable, not to certify taste.
- **Expression foot-guns** — authors will write fast flicker
  (`sin(tau * time * 40)` aliases against 32 steps). Mitigation: the bake's
  Nyquist bound is enforced as a validation error on `PerStep` expressions
  whose highest frequency exceeds the ring (detected by sampling the
  expression at 2× ring resolution and comparing), with a message that says
  "this animates faster than the 32-step ring can show".
- **Cost-formula drift** — a renderer change that alters emission without
  updating the formula. Mitigation: the property test is in the same crate as
  the emission; CI fails on divergence.
- **Panel complexity** — a layers panel is a bigger UI than a form.
  Mitigation: templates make the empty state a working skin; system layers
  anchor the mental model; the inspector is still the same schema-driven
  control set the current Builder proved.
- **Open**: whether `screen`-space layers (the third space in the shading
  PRD) are exposed to documents at all in this round — deferred until a
  concrete skin needs one; the schema reserves the field.
- **Open**: literal *tables* per role (different literals for friendly vs
  enemy) — deferred; slots cover the cases we know.
