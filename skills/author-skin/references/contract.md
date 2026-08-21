# Locked authoring contract

<!-- FACTORY_LOCKED:START -->

This file is policy, not an optimization target. Runtime schemas and the
capability manifest generated from the pinned code are authoritative. If this
reference and the manifest disagree, fail `capability_mismatch`; never guess.

## Authority and side effects

- Only an exact human `prototype_approval` authorizes a production build.
- A task worker is pure. It may read its bundle and inputs, reason, and return
  structured output. Provider, shell, storage, Git, registration, review, and
  publication side effects belong to the factory driver and its journal.
- Image generation names the configurable `image_generator` capability role,
  not an SDK or interactive-agent dependency. The first production forge owns
  local LaMa repair; `image_editor` is valid only when a later WorkerRequest
  explicitly advertises an exact-mask, journaled editor phase.
- Output is SkinDoc v2. An expressiveness gap produces `platform_gap`, with the
  missing primitive and evidence. Handwritten Rust is not a production fallback.
- Every intermediate and rejection is an artifact. Never delete, replace, or
  mutate prior bytes to make a retry appear to be the same attempt.

## SkinDoc v2 ownership

The engine owns the outer Boost band and topmost head core. Authors choose the
permitted head-core colour and ratio but cannot duplicate, move, cover, or
weaken those system signals.

The authored stack supports these actual v2 primitives:

- layers: `group`, `ribbon`, `span`, `head_disc`, `head_ramp`;
- span sources: `solid`, `gradient`, `band`, `image`, `text`;
- regions: `contour`, `body`, `head`;
- clips: `silhouette`, `cells`;
- anchors: `whole`, `head`, `tail`, `at`, `fraction`;
- image fits: `clip`, `stretch`, `tile`, `cutout`;
- transforms: `translate_s`, `translate_t`, `scale_s`, `scale_t`,
  `rotate_turns`.

Groups are authoring structure and flatten before paint. They may carry
opacity, but their transform must remain the identity. Follow the capability
manifest for group flag semantics until the pinned implementation proves them.

Use palette slots for role-aware colour: `fill`, `outline`, `accent`, and
`head_core`. Named literals are accents, not a replacement for team signalling.
Friendly must read cool, hostile warm, the within-team shades must be distinct,
the head must remain identifiable, and labels/ready state must retain required
contrast throughout the animation.

## Expressions and animation topology

The expression language is total and bounded. It provides numeric constants,
`s`, `t`, `len`, `time`, `boost`, `seed`, arithmetic, and the functions exposed
at that evaluation site by the pinned manifest's
`expression_functions_by_site` (the current union is `sin`, `cos`, `saw`,
`tri`, `pulse`, `fract`, `floor`, `abs`, `clamp`, `mix`, `smoothstep`, `step`,
`min`, `max`, `noise`). Never invent syntax.

An expression may vary paint arguments, never topology or operation count.
Current evaluation sites are:

| Site | Allowed inputs |
| --- | --- |
| Palette (`ColorRef.lighten`) | `time` |
| Snake (opacity, transforms, gradient stops, image drift) | `time`, `len`, `boost`, `seed` |
| Cell (head-ramp opacity, band alpha) | all six inputs |
| Bounded (band lane and head-disc radius) | `time` |

`noise` is affordable only at the Cell site even though it reads no named
input; do not use it at Palette, Snake, or Bounded sites.

Only these band fields animate: `half_width`, `t_center`, and `alpha`.
`period_cells`, `duty`, and `phase_cells` are static. Layer order, source kind,
regions, clips, span geometry, fit, fade, stop count, text, ribbon overhang,
and structural flags are also static. Unless the capability manifest's
`animation.image_drift` explicitly declares expression support, image
`drift_cells` must be constant; `constant_cells_per_cycle` does not permit a
time expression.

All effects are ordinary layers. A shine can be moving gradient stops; a pulse
can be opacity or lightness; a wave can use `s` and `time` at a cell-capable
site. Multiple effects are multiple fixed layers sharing the document's one
`period_ms`. Reduced motion resolves to step/row zero, which must be a useful
resting appearance.

## Images, fallbacks, and sprite metadata

SkinDoc v2 has no special head/body/tail bitmap slots. Images are sources on
anchored span layers. X is the asset's authored body length, never the live
snake length. Every image plan declares anchor, fit, fade, and behavior on
short, median, and long bodies.

Every image layer must have an earlier ordinary solid/procedural layer that
fully preserves a readable snake when atlas loading is pending or fails. The
fallback is mandatory even when the image is opaque.

During the isolated worker response, an unresolved planned texture uses exactly
`pending:asset:<index>` as its `ref`, the matching planned `kind`, and no
`descriptor`. Its `generate_asset` request uses the same zero-based
`asset_index` plus the exact SkinDoc `texture_name`. This is a draft sentinel,
not a content reference and not a valid registered texture. The driver must
reject any other generated-looking draft ref, any fabricated descriptor,
duplicate/mismatched index or name, and any `pending:` value that survives
binding. Only the exact forge manifest may replace the sentinel before gates.

A sprite sheet is an X-by-Y grid:

- X is the independently chosen number of body cells in each frame;
- Y is the independently chosen number of animation frames;
- each row spans the full X cells from head toward tail;
- row zero is the resting/reduced-motion frame;
- metadata records X, Y, texels per cell, period, and wrap axes separately;
- every row, including the final-to-zero transition, must be reachable and
  verified in the real renderer.

Choose X from repeat length, mark scale, and body fixtures. Choose Y from the
motion and requested frame rate/period, then stay within the pinned dimension,
byte, memory, and runtime-row limits. Never make a sheet square merely to encode
one number twice. A loop always requires `y`; repeating along the body also
requires `x`.

## Security and review invariants

- References must be owned/authorized or explicitly shareable, kind-compatible,
  immutable, and resolved before compilation.
- Record hashes for the exact variants served, not only a canonical source.
- Text uses the admitted glyph set and remains review-gated. Do not expose
  unreviewed text through preview or by-reference paths that other players can
  fetch.
- Never claim pixel validation from native fallback rendering. Image-bearing
  skins require evidence from the real WASM/browser renderer after atlas
  readiness and proof that image pixels, not just fallback pixels, painted.
- Deterministic blocking gates cannot be waived by a visual judge. Visual
  fidelity is soft triage and cannot publish.
- Publication is outside this skill and requires a human approval bound to an
  exact revision/content hash.

## Capability and budget rules

Read limits from the supplied manifest: schema version, expression inputs and
functions by evaluation site, flattened layers, texture references, gradient
stops, operation budget, period range, group behavior, texture kinds, atlas
descriptor version, image-drift support, sprite-row reachability, image sizes,
bytes, measured repair behavior, and evidence fixtures. The factory records its SHA on the
Attempt and passes the pinned manifest in `WorkerRequest`; do not duplicate an
unverified hash inside the artistic plan.

The numbers in repository code are not prompt defaults. Never weaken a limit,
split one build across unjournaled calls, or silently fall back when the pinned
renderer lacks a declared capability.

<!-- FACTORY_LOCKED:END -->
