# Reading an approved prototype

A valid prototype is one medium-length horizontal snake strip constrained by
the exact pinned `prototype_geometry` contract and guide. It is a flat,
right-facing, continuous one-cell-wide capsule: a rounded one-cell head with a
small centered core, representative body, and rounded tail, on a neutral or
transparent background. Detached plates, gaps, perspective, an oversized
separate head, a pointed tail, UI, labels, scenery, and alternative designs are
not skin pixels.

Verify before implementation:

1. The approval names the exact image SHA-256 and the manifest records the same
   digest. Do not use a visually similar candidate.
2. The manifest preserves the brief, palette and motion intent, prompt, stored
   model-configuration reference, implementation hint, and rationale. The
   immutable Artifact/Attempt metadata separately preserves the resolved model,
   request id, references/licensing provenance, review flags, and approval.
   It must also bind the shared design-guideline SHA, prototype-geometry SHA,
   and exact guide SHA supplied in `artifact_refs` and `authoring_inputs`.
   Require the `prototype_geometry` and `prototype_geometry_guide` artifact
   refs, the exact inline guide bytes, and the authoring input's `contract`,
   `contract_sha256`, and `guide_sha256`. Return `invalid_input` if any value is
   absent or differs from `prototype_geometry_sha256` or
   `prototype_guide_sha256` in the approved manifest; never read replacement
   authority from the current checkout.
3. The image makes head/body/tail and key marks legible when downsampled to the
   game's cell range. Flag a poster-only composition rather than fabricating
   hidden details.
4. Separate authored identity from generator accidents: background shadows,
   presentation glow, perspective, and a fixed illustrated body length are not
   automatically implementation requirements.
5. Translate the fixed strip into live behavior. Declare what happens to marks
   on short, median, and long snakes and at corners. Image X is the natural
   authored/repeat length, not a promise that live snakes have that many cells.

The prototype's `implementation_hint` is advisory. Preserve its rationale in
the plan even when choosing another route so later feedback can distinguish a
bad hint from bad execution.
