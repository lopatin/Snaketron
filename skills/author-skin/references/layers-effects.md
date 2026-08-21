# Layers and effects

Use this reference when the implementation contains procedural layers.

## Composing the stack

Think bottom to top. A typical stack is a role-readable body/contour foundation,
pattern or image, highlights/effects, and authored head treatment. The engine
inserts its Boost band outside the contour and its head core on top.

- `ribbon` is the efficient whole-body or contour foundation.
- `span` places solid, gradient, band, image, or text sources along an anchored
  body interval.
- `head_ramp` evaluates opacity per cell and is the natural place for a glow
  curve using `s` and `time`.
- `head_disc` adds a bounded authored disc below the system head core.
- `group` organizes one level of layers and may multiply opacity. Put transforms
  on children because group transforms cannot flatten exactly.

Prefer a palette slot when the colour contributes to side reading. A literal
may supply a controlled gleam or material accent, but the composite still has
to pass every role and clock sample.

## Effect recipes

These are patterns, not required designs:

- travelling shine: gradient stop offsets derived from `saw(time)` while stop
  count and layer placement stay fixed;
- pulse: layer opacity or `ColorRef.lighten` derived from a slow sine;
- moving lane: animate band `t_center` or `half_width`, never period/duty/phase;
- head wave: a head-ramp opacity curve combining `s` and `time`;
- boost response: opacity using `boost` at a snake evaluation site, while the
  layer remains present in the op stream;
- per-snake variation: subtle `seed` use only where its evaluation site permits.

Use rates the pinned animation ring can represent. A longer `period_ms` slows
the same sampled cycle; it does not create more expression steps. Validation
must reject aliasing rather than accepting an animation that plays differently
from the expression.

Keep every animated value in the property's valid range at every baked step.
Never animate source kind, number of layers/stops/repeats/slices, fit, span, fade,
clip, region, order, or a structural flag.
