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
- Keep the task-worker `skin_document.layers` flat. Rust understands `group`
  as an authoring convenience, but the strict worker-response grammar excludes
  recursive group nodes; place concrete children in document order and apply
  inherited opacity or flags directly to them.

For a span's `from`, write unit anchors as `"whole"`, `"head"`, or `"tail"`.
Rust's numeric enum variants are externally tagged and therefore intentionally
nested: `{"at":{"at":1}}` or `{"fraction":{"fraction":0.5}}`, never the
flattened shorthand.

Prefer a palette slot when the colour contributes to side reading. A literal
may supply a controlled gleam or material accent, but the composite still has
to pass every role and clock sample.

Write those references in their exact object form: `{"slot":"accent"}` or
`{"literal":"gleam"}`. A bare `"#rrggbb"` is not a `ColorRef`; the canonical
template's raw hex on `head_ramp.color` is a deliberately different field.

## Effect recipes

These are patterns, not required designs:

- travelling shine: gradient stop offsets derived from `saw(time)` while stop
  count and layer placement stay fixed;
- pulse: layer opacity or `ColorRef.lighten` derived from a slow sine;
- moving lane: animate band `t_center` or `half_width`, never period/duty/phase.
  Bound both expressions over every baked frame and require
  `max_frame(abs(t_center)) + max_frame(abs(half_width)) <= 0.5`; neither field's
  individual range proves the combined lane fits. A safe example is
  `t_center = 0.3 * tri(time)` with `half_width = 0.15`: the pinned
  `tri(time)` ranges from zero to one, so the independent maxima sum to
  `0.3 + 0.15 = 0.45`;
- head wave: a head-ramp opacity curve combining `s` and `time`;
- boost response: opacity using `boost` at a snake evaluation site, while the
  layer remains present in the op stream;
- per-snake variation: subtle `seed` use only where its evaluation site permits.

Use rates the pinned animation ring can represent. A longer `period_ms` slows
the same sampled cycle; it does not create more expression steps. Validation
must reject aliasing rather than accepting an animation that plays differently
from the expression.

Keep every animated value in the property's valid range at every baked step.
For a band, this includes proving the combined lane invariant above over the
entire pinned animation ring. Never animate source kind, number of
layers/stops/repeats/slices, fit, span, fade, clip, region, order, or a
structural flag.
