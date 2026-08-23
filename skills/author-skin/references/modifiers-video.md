# Separable modifiers, extraction, reuse, and video

Read this reference only when a design contains independently placeable visual
components, reused object art, source-media extraction, or video-derived
animation.

## Decompose before rendering

Labels such as `T1`, `T2`, `B1`, and `H` are conceptual component keys. Give
each component one logical modifier record, one immutable object/manifest, one
SkinDoc texture, and one anchored span image layer with an earlier ordinary
fallback. The keys do not imply packed columns, fixed semantics, or renderer
fields. Record which component owns each fidelity feature and choose `whole`,
`head`, or `tail` placement from the actual design.

These are placement constraints, not new renderer slots: an optional head
component starts at `head` and may extend through at most cell 6; an optional
tail component starts at `tail` and may cover at most one half of the current
snake. Record `head_cells <= 6` or `tail_fraction <= 0.5` in the modifier plan.
T1/T2/B1/H remain conceptual keys, and every chosen component is still an
independent span.

Do not combine components into one bitmap unless the pinned capability manifest
and supplied schema expose an exact source-region primitive. Without it, the
renderer samples the whole raster; it cannot infer which columns represent a
head, body, or tail. Use distinct assets or return `platform_gap`.

## Extract to an immutable object first

Extraction is a retained preprocessing stage, never an image-layer option:

1. Retain the exact source bytes, license assertion, prompt/recipe, and
   provenance record.
2. Put or generate one component in a reserved empty source arena with enough
   transparent safety margin that no visible pixels touch the crop boundary.
3. Remove the background and produce transparent RGBA bytes. If the operation
   uses a matte, retain the exact mask/matte and prove it maps to alpha without
   contaminating RGB edge pixels.
4. Fail closed on residual background, multiple candidate objects, clipped
   pixels, matte-colour fringes, partial-alpha halos, or an unverifiable alpha
   channel.
5. Crop only after verification. Retain the cropped bytes, extraction report,
   and object manifest under exact SHA-256 references. The cropped object is a
   new immutable first-class asset; the source composite is not.
6. Invoke authoring again with the exact object/manifest catalog. Only then may
   the component become its own anchored image layer.

The empty extraction margin is provenance, not visible overhang. Visible
bounded rendering uses `TextureDescriptorV2.raster_overhang_px`, from 0 through
4 authored bleed pixels per side around the unchanged 16×16 body cell. At the
16-texel rung, a value of 4 is stored as `4 + 16 + 4 = 24` transverse texels;
that is a body row plus two aprons, not a 24-texel cell. The forge scales it per
rung. Keep extraction margin and descriptor overhang separate in plans and
reports.
For an extracted transparent modifier with nonzero overhang, verify that every
visible alpha pixel remains inside the stored body-plus-aprons row (at most 24
transverse texels at the 16-texel rung) and fail closed on edge contact or
truncation. Opaque generic
base textures may intentionally fill that strip and record an explicit
`not_applicable_opaque_fill` policy. This is not a seam-gutter allowance:
repeats and frame rows still have no gutters.

## Reuse safely

Reuse requires an exact immutable modifier manifest, not a prompt name or a
visually similar file. Verify all of these before binding it:

- modifier id and conceptual component key;
- exact object content hash and manifest hash;
- media type, dimensions, alpha/matte report, and kind compatibility;
- license identifier and retained provenance hash;
- an `authorized_lineage_ids` entry for the target pre-registration
  concept/lineage;
- an owned/shareable exact forge manifest or an advertised host operation that
  can create one without changing the source bytes.

The immutable id is `modifier:<authorized-lineage>:<content_sha256>`. Its hash
suffix must equal the exact retained RGBA/sheet bytes, and its lineage segment
must occur in the manifest's authorized lineage set; a merely well-formed but
different digest is invalid.

If any fact is missing, reject the reuse or return `platform_gap`. Do not widen
authorization because two concepts share an owner. Cross-lineage reuse needs
explicit shareability or Snaketron admin authority.

## Video-derived animation

Prefer procedural transforms, opacity, gradients, bands, or image drift when
they reproduce the motion. Use video only when distinct drawn frames carry
important identity and the host advertises journaled video generation plus
deterministic frame extraction.

The host must retain every input, prompt, resolved provider/model, provider
request id, provider output video, extracted frame set/sheet, alpha/matte
report, and operation journal. Use one SkinDoc `period_ms`. Choose
`desired_fps`, then derive rows as
`ceil(period_ms * desired_fps / 1000)` and clamp by the pinned frame-rate,
dimension, decoded-byte, and row limits; rows may never exceed 120. Extract at
deterministic timestamps covering exactly one period. Row zero is the resting
frame. Verify frame order, no unintended camera translation, per-frame alpha or
exact matte, temporal joins, and the actual final-frame-to-row-zero transition.
Transition generation binds ordered exact `start_frame_sha256` and
`end_frame_sha256` roles; an unordered bag of references is invalid. Record the
raw derived row count and the effective cap computed from 120 rows, the 2048px
height limit, and decoded RGBA bytes. Reject a request whose derived rows exceed
that cap rather than silently changing its cadence.

The media-operation v1 schema additionally caps `desired_fps` at the current
pinned renderer ceiling of 60 fps. A host must also apply its exact advertised
ceiling if that is lower; an unadvertised or higher requested cadence is
`platform_gap`, not permission to create unreachable rows.

### Optional PixVerse v6 transition recipe

Use this provider-specific recipe only when the host advertises the exact
`fal-ai/pixverse/v6/transition` capability. This contract does not install or
invoke an adapter. Bind the first input to exact `start_frame_sha256` and the
second to exact `end_frame_sha256`; reversed or unordered references are
invalid. Use a prompt with these literal sections:

```text
[Cinematography]
Static orthographic camera; flat 2D framing; no zoom, pan, tilt, parallax, or perspective.
[Subject]
Preserve the exact supplied component shapes, proportions, edges, and palette.
[Action / Transition]
Only the named component animates from the exact start state to the exact end state; motion remains compatible with a true cyclic final-to-zero closure.
[Context]
Keep the reserved matte arena and its background completely static; add no objects, shadows, text, scenery, or camera motion.
[Style & Ambiance]
Flat 2D production artwork; preserve exact colors and alpha/matte boundaries without restyling, lighting drift, or texture invention.
```

After deterministic extraction, verify the actual final row to row-zero join;
prompt wording is not evidence of loop closure. If the capability or any
required retained-input/output field is absent, return `platform_gap`.

Use [schemas/media-operation-request.schema.json](../schemas/media-operation-request.schema.json)
only when the host advertises the request's exact capability id. The current
factory's initial worker transport supports ordinary `generate_asset` only;
unsupported extraction/reuse/video requests are `platform_gap`, not permission
for direct provider or shell calls.
