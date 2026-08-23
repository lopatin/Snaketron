---
name: automate-skin-drafts
description: Plan and operate unattended Snaketron private-draft creation from the durable Factory inbox through exact Admin review submission. Use when enqueueing or advancing automated skin drafts, or when the Factory requests a hash-free media preplan. Never use this skill to approve or publish a skin.
---

# Automate Snaketron skin drafts

Keep `author-skin` pure. This skill owns only orchestration: queue admission,
media preplanning, exact retained operation sequencing, private revision
registration, and the Admin review request.

When the Factory asks for a media preplan:

1. Inspect the selected prototype, concept fields, shared design rules, pinned
   renderer limits, and the host capability manifest.
2. Prefer procedural motion. Request video only when distinct drawn frames are
   important to the concept and every required host capability is advertised.
3. Return exactly
   `schemas/draft-media-preplan.schema.json`. Do not invent content hashes,
   provider request ids, object manifests, or completed-operation evidence.
4. Give each video intent two explicit endpoint prompts on the required matte
   arena and one five-section transition prompt. Keep the camera and matte
   static. Use the 16-texel logical cell grid and at most four authored bleed
   pixels per side. Use at most 63 body columns; the fixed 1080px provider
   arena must retain its real 32px matte apron before any endpoint spend. The
   five-section model action is at most 1024 UTF-8 bytes because the driver
   adds the non-overridable camera, matte, geometry, and loop contract before
   submitting the final at-most-2048-byte Fal prompt.
5. A video intent is a request, not evidence. The driver must generate and
   retain endpoints before it can construct a hash-bound provider request.

After the driver materializes exact objects, invoke `author-skin` with the
retained modifier catalog. Only that final pass may emit a fully bound
implementation plan and SkinDoc.

For commands, scheduling, admission, recovery, and platform-gap behavior, read
`references/operations.md`.

Never call a provider, shell, upload endpoint, review endpoint, or publication
endpoint from the model worker. Never treat an Admin review request as
publication. Never publish.
