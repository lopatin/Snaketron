# Skin Design Guidelines

<!-- SKIN_DESIGN_LOCKED:START -->

This is the shared creative contract for prototypes and completed skins. Keep
the intent bold; apply it through the actual SkinDoc v2 and renderer contract.
If a requested look needs geometry the pinned capability manifest does not
provide, return `platform_gap` rather than pretending the renderer supports it.

<!-- PROTOTYPE_IMAGE_RULES:START -->

## Creative direction

- Skins may be fun, weird, or silly. Commit confidently to one artistic
  direction per skin instead of blending unrelated rendering styles.
- A recognizable popular concept with one clear, original twist can scale well.
  Borrow the abstract premise or genre, not protected characters, marks,
  likenesses, or trade dress. An original concept does not need a forced
  popular reference.
- A hybrid implementation may combine image and procedural layers without
  becoming a mixed visual style. The layers should serve the same artistic
  direction.
- For the prototype image, apply that one direction only inside the exact
  attached body guide. Return one flat orthographic snake, without UI, text,
  scenery, framing, alternate concepts, or a montage.
- Keep the rightmost 1.5 cells—the head zone—as one readable tonal field that
  preserves the small system head core instead of competing with it.
- Use bold, seamless shapes that survive at native scale. Avoid details, lines,
  or gaps narrower than one fifth of a cell.

<!-- PROTOTYPE_IMAGE_RULES:END -->

## Thin, growing body at real game scale

The pinned `prototype_geometry` authoring input and its exact guide bytes are
the visual authority for prototype/build agreement. The current contract is
repository-owned at `skin-schema/prototype-geometry-v1.json`. Its 16-cell
straight strip is an explicit review pose, not a claim that every live snake
has 16 cells. Preserve its flat continuous capsule topology, right-facing
one-cell head, small centered system core, and rounded tail while translating
the approved art to short, turning, and growing bodies.

Before authoring, require `artifact_refs.prototype_geometry` and
`artifact_refs.prototype_geometry_guide`, the exact inline guide bytes, and
`authoring_inputs.prototype_geometry` with its `contract`, `contract_sha256`,
and `guide_sha256`. Those hashes must match the approved prototype manifest's
`prototype_geometry_sha256` and `prototype_guide_sha256`. If any input is
absent or mismatched, return `invalid_input`; never substitute a local checkout
or a visually similar guide.

The snake is a very thin, round-capped, one-cell-wide path that continuously
grows. Design for that constraint instead of treating it like a wide character
illustration or a fixed-length banner.

Author geometry in normalized snake cells, not fixed screen pixels. The live
arena chooses integer cell sizes from 5–15 CSS px
(`client/web/components/GameArena.tsx`), and renderer conformance pins 5, 10,
and 15 px samples (`client/src/skin/fixtures.rs::CELL_SIZES`). The number 16
describes the canonical texel density of a sprite-sheet cell, not a guaranteed
live cell size; coats and overlays use 64 texels per cell
(`server/src/texture.rs::TextureKind::canonical_texels_per_cell`).

The canonical default spawn is four cells
(`common/src/game_state.rs::DEFAULT_SNAKE_LENGTH` and the renderer's
`starting_length` fixture). Six- and seven-cell snakes are common early growth
states. A head treatment may naturally occupy 6–7 cells or more on a longer
snake, but it must degrade deliberately on the four-cell spawn and on the
shorter one- and two-cell conformance poses. The design must also remain
coherent as the body grows long and turns repeatedly.

`Snake.body` stores compressed head, turn, and tail points, not one entry per
occupied cell (`common/src/snake.rs`). Occupied length is the Manhattan arc
between those points plus one, and a renderer `Segment` is one straight run
(`client/src/skin/geometry.rs`), not a visual cell plate. Author continuous
snake-space material; do not assign artwork by compressed-point index.

Snake space runs from head toward tail. At shared turn cells the head-ward run
owns the cell under the default corner policy; later body material must not be
designed to cover or confuse the head treatment. Inspect straight, short,
cornered, U-turn, and long/tiled fixtures rather than validating only one
illustrated strip.

Authored body spans and image sources are clipped to the round snake
silhouette. They cannot paint arbitrary pixels outside it. The actual bounded
exceptions are contour ribbons and authored head discs; the engine still owns
the outermost Boost band and the topmost head core. Do not imply freeform
outside-body geometry in a prototype or asset prompt.

## Two visual structures

Most designs have one of two primary structures:

- **Pattern:** SkinDoc objects and ordered layers create the identity—ribbons,
  spans, bands, gradients, transformations, and time effects. Pitlane is a
  useful first-party example of a clear pattern idea; reproduce the principle
  with supported v2 primitives rather than depending on its Rust recipe.
- **Sprite:** body art, with optional head- or tail-anchored art, lives in snake
  space. Fills usually come from a texture and may tile or animate. SkinDoc v2
  has no special head/body/tail bitmap slots, so implement these as anchored
  image spans with a complete procedural fallback.

Choose which structure carries the visual identity. A `hybrid` route may add
procedural role cues or effects around sprite art, but it still declares one
primary structure and one artistic direction.

## Texture and sprite production quality

Texture pixels must be production quality. Every join required by actual use
must be seamless on the exact served variants after resize, repair, and
quantization. A clean one-pixel seam score is insufficient when marks,
alignment, detail, or chroma jump across the join.

Sprite animation should use a relatively fast requested frame rate when the
motion benefits from it, producing a tall sheet that can also play cleanly in
slow motion. Choose `desired_fps`, derive frame rows from the document period,
and clamp to the pinned limits; never choose height independently. Row zero is
a complete resting and reduced-motion frame. Fast does not mean flickering:
frame identity, temporal continuity, the last-to-zero join, and every row's
reachability still have to pass.

## Head-zone polarity

Keep roughly the first 1.5 cells as one legible tonal field: largely light or
largely dark, without noisy internal contrast that competes with the head
marker. The marker uses the opposite polarity.

SkinDoc v2's topmost system head core must currently remain dark enough for the
roster's white ready-check (`skin-schema/src/v2.rs` head-core validation). Apply
the intended polarity as follows:

- `light_field_dark_core`: keep the head field largely light and use the dark
  system head core.
- `dark_field_light_disc_dark_core`: keep the head field largely dark and add a
  bounded light authored `head_disc` beneath the still-dark system head core.

Do not set a white `head_core`; the current validator rejects it. If fidelity
requires a white *topmost* system core rather than the supported light authored
disc treatment, report a platform gap. Check the chosen treatment in every
role palette, body length, turn, clock sample, and reduced-motion state.

## Required implementation-plan evidence

Every implementation plan records one compact `design_guidelines` object:

- `artistic_direction`: the single visual direction;
- `concept_twist`: the abstract familiar premise and original twist, or an
  explicit statement that the concept is original;
- `structure`: `pattern` or `sprite`;
- `body_strategy`: behavior at four cells, common early 6–7 cells, long growth,
  turns, and head-over-tail occlusion;
- `head_zone`: one of the two supported polarity treatments above;
- `asset_strategy`: production-quality seam axes and sprite cadence/derived
  rows, or an explicit procedural/no-raster statement.

These fields are design evidence, not a replacement for executable plan data.
`asset_plan`, `required_wrap_axes`, `desired_fps`, layers, and the final document
remain authoritative and must agree with them.

<!-- SKIN_DESIGN_LOCKED:END -->
