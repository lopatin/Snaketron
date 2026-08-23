# Snaketron skin direction

This repository file is the canonical direction for generated skins. Notion or
another planning tool may be imported once, but production never reads it.

## Product character

Skins should feel playful, competitive, immediately legible, and polished at
the scale of the actual game. A strong skin has one memorable visual idea and
executes it consistently. Novel motion should reinforce that idea rather than
make the snake harder to track.

## Readability invariants

- The head remains distinguishable from body and tail in a medium horizontal
  strip and in short, turning, and long in-game poses.
- Own, friend, and enemy roles remain distinguishable under every supported
  palette substitution.
- The outline and head core remain readable against light and dark boards.
- Reduced motion shows a deliberate resting frame, never a broken or empty
  animation state.
- Texture loading failure leaves a complete procedural fallback.

## Craft preference

Prefer SkinDoc v2 layers for geometry, checkerboards, stripes, gradients,
bands, highlights, and time-driven effects. Use textures for painterly or
irregular art, sprite sheets for motion that needs drawn frames, and hybrid
composition when raster detail benefits from procedural contours or role cues.

Avoid unlicensed characters, brands, protected likenesses, illegible micro
detail, poster-like scenery, and effects that create a competitive advantage.

## Prototype contract

Prototype geometry is defined only by the behavior-pinned shared design
guidelines and `skin-schema/prototype-geometry-v1.json`; this direction file
does not create a second silhouette authority. The canonical review view is
one continuous, one-cell-wide rounded capsule with a one-cell head and rounded
tail. Palette, pattern, and compatible effects may vary inside that envelope.
No UI, labels, scenery, alternate concepts, articulated modules, oversized
head, or pointed tail may appear in the image.
