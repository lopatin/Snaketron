# Reading an approved prototype

A valid prototype is one medium-length horizontal snake strip: distinct head,
representative body, and tail, on a neutral or transparent background. UI,
labels, scenery, and alternative designs are not skin pixels.

Verify before implementation:

1. The approval names the exact image SHA-256 and the manifest records the same
   digest. Do not use a visually similar candidate.
2. The manifest preserves the brief, palette and motion intent, prompt, stored
   model-configuration reference, implementation hint, and rationale. The
   immutable Artifact/Attempt metadata separately preserves the resolved model,
   request id, references/licensing provenance, review flags, and approval.
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
