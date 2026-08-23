---
name: author-skin
description: Turn an exact approved prototype or explicitly selected draft candidate into a validated private SkinDoc v2 implementation using layers, textures, sprite sheets, reusable modifiers, or a hybrid. Use for factory builds and capability-gated interactive draft submissions; do not use for prototype approval or publication.
---

# Author a Snaketron skin

Build one exact retained prototype candidate as a SkinDoc v2 revision. This
package is agent-neutral: use the inputs and output schemas supplied by the
caller, not an agent's transcript, private memory, or provider-specific
executable.

## Required inputs

Stop with a typed `invalid_input` result if any item is absent or inconsistent:

- prototype bytes or a readable content-addressed reference;
- its prototype manifest and one exact `input_authority` record in either
  `approved_prototype` or `draft_submission` mode;
- the repository-canonical design direction and literal human feedback;
- the generated capability manifest for the pinned schema/renderer SHA and a
  separate literal host-operation capability record (authority modes,
  operations, and any tighter driver limits);
- output schemas, budgets, pure-tool allowlist, and artifact workspace.

The authority artifact hash must equal the prototype image hash. In
`approved_prototype`, require the exact human `prototype_approval`. In
`draft_submission`, require the agent-selected candidate hash, literal
selection rationale, and retained selection-record hash; never describe that
record as human approval. Never substitute another candidate or silently
regenerate the selected direction.

## Load the contract

Read [references/contract.md](references/contract.md) completely. It is locked
policy. Always read the locked shared
[Skin Design Guidelines](references/design-guidelines.md) completely and apply
them in the implementation plan. Then read only the other craft references
needed for this build:

- Always read [references/prototypes.md](references/prototypes.md) and
  [references/playbook.md](references/playbook.md).
- For procedural layers or effects, read
  [references/layers-effects.md](references/layers-effects.md).
- For any image source, read
  [references/textures-sprites.md](references/textures-sprites.md).
- For separable components, reusable modifier objects, extraction, or video,
  also read [references/modifiers-video.md](references/modifiers-video.md).
- Before returning a result, read
  [references/validation.md](references/validation.md).

## Author in two logical passes

1. Inspect the prototype at game scale and identify the few visual features
   that carry its identity. Treat its `implementation_hint` as advice.
2. Choose `layers`, `texture`, `sprite_sheet`, or `hybrid` using the routing
   rules in the playbook. Prefer layers for patterns and formula-driven motion.
3. Before requesting or generating assets, emit `implementation-plan.json`
   conforming to [schemas/implementation-plan.schema.json](schemas/implementation-plan.schema.json).
   Fill its bounded `design_guidelines` object from the shared guidelines;
   these decisions must agree with the executable layer, asset, wrap, and
   animation plan.
   Record `input_authority`, the common animation period, and every image
   asset's natural X length and derived frame count Y, plus placement and wrap
   axes. Map every asset one-to-one to a modifier record, image layer, and
   earlier fallback. Put the reason for changing the prototype hint in
   `rationale`.
4. Compose the v2 document draft. For each planned image asset, declare one
   matching texture name and use the same `asset_index` and `texture_name` in
   its request. Put a complete ordinary fallback immediately below every image
   layer. The only unresolved reference is `pending:asset:<index>`, with the
   planned kind and no descriptor. Do not fabricate a generated hash,
   descriptor, upload, or successful gate.
5. If ordinary generated assets are needed, emit `generate_asset` tool
   requests whose `arguments`
   conform to
   [schemas/asset-request.schema.json](schemas/asset-request.schema.json) for
   the caller's `image_generator` role. Emit `edit_asset` only in a repair phase
   the caller explicitly advertises; otherwise the deterministic forge owns
   its isolated local LaMa helper. The configured generator default may be Gemini 3
   Pro Image, but the skill never requires Gemini or calls a provider directly
   when running as a factory task worker. Extraction, modifier reuse, and video
   are separate journaled host capabilities: use their request schemas only
   when the caller advertises the exact operation and retained-input/output
   contract. Otherwise return `platform_gap`.
6. Return only the plan, v2-shaped document draft, tool requests, trace, usage,
   or typed failure allowed by the caller's worker-result schema. If the
   prototype cannot be expressed by the
   pinned capabilities, return `platform_gap`; do not create a Rust skin.
7. The driver journals every external operation, generates/repairs assets,
   binds exact served refs and descriptors into the draft, validates the exact
   bytes, registers the private revision, and captures real-browser evidence.
   A later bounded repair request may supply failed artifacts and reports, but
   neither worker nor driver overwrites their history. A pure factory worker
   never uploads, registers, approves, publishes, uses Git, or mutates external
   storage, and the factory accepts only `approved_prototype`. An explicitly
   authorized interactive agent acting as the advertised host may journal and
   execute the declared generation/extraction/binding operations, register the
   exact private revision, and request Snaketron admin review. It may never
   publish; an unadvertised operation is `platform_gap`.

The factory explicitly bundles this directory and records its SHA. Agent
discovery wrappers are conveniences only; see
[references/integration.md](references/integration.md).
