# Locked authoring contract

<!-- FACTORY_LOCKED:START -->

This file is policy, not an optimization target. Runtime schemas and the
capability manifest generated from the pinned code are authoritative. If this
reference and the manifest disagree, fail `capability_mismatch`; never guess.

## Authority and side effects

- Only an exact human `prototype_approval` authorizes a factory production
  build. Its observable plan mode is `approved_prototype`.
- `draft_submission` is not approval. It binds one agent-selected candidate,
  its literal selection rationale, and the exact retained selection-record
  hash. Only an interactive host that advertises private upload/registration
  and admin-review-request capabilities may execute it, and its maximum action
  is requesting review of a private draft. Otherwise fail `platform_gap`.
- Neither mode authorizes publication. Publication requires a later Snaketron
  admin decision bound to the exact private revision and content reference.
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

Groups exist in the Rust authoring language, but the task-worker result schema
is intentionally the non-recursive flat-layer subset. Emit no `group` node in
`skin_document.layers`: put its concrete children directly in document order,
apply any inherited opacity or structural flags to those children, and use the
implementation plan for human-readable organization. The real Rust gates
remain authoritative after this stricter transport boundary.

Anchor serialization follows Rust's externally tagged enum exactly. The unit
anchors are strings (`"whole"`, `"head"`, or `"tail"`), while numeric variants
are nested objects: `{"at":{"at":1}}` and
`{"fraction":{"fraction":0.5}}`. The tempting flattened forms
`{"at":1}`/`{"fraction":0.5}` do not deserialize and must never be emitted.
Transport integers also keep Rust's portable widths: `span.priority` is signed
32-bit; `fade.steps` is non-negative and must fit the wasm32 client's `usize`;
and texture `body_columns`, `frame_rows`, `width_px`, `height_px`, `bytes`, and
`texels_per_cell` are unsigned 32-bit. The tighter capability and semantic
limits still apply inside those representation bounds.

Use palette slots for role-aware colour: `fill`, `outline`, `accent`, and
`head_core`. Named literals are accents, not a replacement for team signalling.
Friendly must read cool, hostile warm, the within-team shades must be distinct,
the head must remain identifiable, and labels/ready state must retain required
contrast throughout the animation.

Every `ColorRef` is an object: use `{"slot":"fill"}` (or another admitted
slot), or declare a document literal and use `{"literal":"gleam"}`. Never put
a bare hex string at a `ColorRef` site, including ribbon colours, solid/band/text
sources, gradient stops, and head-disc paint. Raw hex strings belong only to
the raw-colour fields shown by the canonical v2 template and fixtures, such as
palette swatches, `literals`, `head_core.color`, and `head_ramp.color`.

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

For every band, bound both lane expressions independently across every baked animation frame.
Require the combined lane invariant
`max_frame(abs(t_center)) + max_frame(abs(half_width)) <= 0.5`. This is stricter
than checking either property alone, or checking only one convenient frame.
For example, `t_center = 0.3 * tri(time)` and `half_width = 0.15` are safe:
the pinned `tri(time)` ranges from zero to one, so
`max_frame(abs(0.3 * tri(time))) + 0.15 = 0.45 <= 0.5`. A band that cannot
prove this bound for the pinned animation ring is invalid; reduce its travel
or width instead of relying on silhouette clipping.

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

The current defensive parser admits at most eight one-to-one raster
asset/modifier pairs so four base parts plus four separable objects can remain
independent. The exact pinned `max_texture_refs` capability is authoritative
at execution time and may impose a lower limit on a retained deployment.

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
- `desired_fps` records the requested motion sampling rate and Y is derived as
  `ceil(period_ms * desired_fps / 1000)`, clamped by the pinned row,
  dimension, and decoded-memory limits;
- each row spans the full X cells from head toward tail;
- row zero is the resting/reduced-motion frame;
- metadata records X, Y, texels per cell, period, and wrap axes separately;
- every row, including the final-to-zero transition, must be reachable and
  verified in the real renderer.

Choose X from repeat length, mark scale, and body fixtures. Choose a
`desired_fps` from the motion, then compute Y from that rate and the document
period using the formula above. Record the derived Y; never freely choose both
FPS and rows. Never make a sheet square merely to encode one number twice. A
loop always requires `y`; repeating along the body also requires `x`.

## Reusable modifier objects

A conceptual decomposition such as `T1`, `T2`, `B1`, and `H` describes
separate visual components, not columns in one bitmap. Each component becomes
its own immutable object asset, texture, and anchored span image layer. The
plan binds a logical component key to its exact manifest/content hash when
available, license, provenance hash, authorized pre-registration lineage ids,
image-layer name,
and earlier fallback-layer name. The driver derives the immutable modifier id
only after exact byte binding. Reuse is authorized within an exact stable
concept/lineage scope; possession of a hash is not permission. Cross-lineage
reuse requires explicit shareability or Snaketron admin authority.

Conceptual keys are not renderer slots. Optional head components anchor at
`head` and extend through at most cell 6. Optional tail components anchor at
`tail` and cover at most one half of the current snake. The plan records these
as `head_cells <= 6` and `tail_fraction <= 0.5`; each remains an independent
span with its own fallback.

Do not pack components into one raster and select columns at render time. That
is valid only if the pinned renderer manifest and supplied SkinDoc schema expose
an exact source-region primitive and the plan names it. Without that advertised
capability, use one object and texture per component or return `platform_gap`.

Extraction happens before composition. Generate or place exactly one object in
a reserved empty source arena, remove the background, and verify transparent
RGBA bytes or an exact retained mask/matte. Reject contaminated backgrounds,
matte-colour fringes, partial-alpha halos, and ambiguous multiple objects.
After verification, crop and retain the object and its extraction report as
new immutable first-class artifacts. Only that retained object may be bound to
an independently anchored layer. The renderer never infers alpha or object
boundaries from a packed raster.

The reserved transparent source margin used to prevent extraction clipping is
provenance, not visible paint authority. Visible bounded rendering is governed
separately by `TextureDescriptorV2.raster_overhang_px`: 0 through 4 authored
bleed pixels per side around the unchanged 16×16 body cell. At the 16-texel
rung, a value of 4 is stored as a 24-texel source row (`4 + 16 + 4`) so apron
art remains distinct; it does not make the cell 24 texels wide. The forge
scales that apron per rung; it never grants unbounded canvas access.
When bleed is nonzero, the exact stored transverse row is at most 24 texels at
the 16-texel rung. For extracted transparent objects/effects, fail closed if visible
alpha touches or is truncated by its outer frame edge. An opaque generic base
texture may intentionally fill the strip and records that exemption. This edge
check is separate from the no-gutter requirement between body repeats or
sprite frames.

Video is a source pipeline, not an implicit provider trick. Use it only when
the host advertises journaled video generation and deterministic frame
extraction. Retain the exact source inputs, provider output video, extracted
frames/sheet, prompts, resolved provider/model, and verification reports.
Sample one common `period_ms` at deterministic timestamps; derive rows as
`ceil(period_ms * desired_fps / 1000)`, clamp to the pinned limit (never more
than 120), verify transparent RGBA or exact matte handling frame by frame, and
test the true final-frame-to-row-zero transition. Prefer ordinary procedural
transforms when they express the motion faithfully.

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
- protected marks, public-figure likeness, unsafe content, and apparently
  unlicensed references are a blocking `safety_ip` failure at both prototype
  and completed-build review. This gate is non-waivable: humans may inspect or
  retry the retained idea, but cannot override it into authoring or publication.
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
