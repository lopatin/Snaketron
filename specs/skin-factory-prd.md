# PRD: Skin Factory v2 — a retained, self-improving skin pipeline

| Field | Value |
| --- | --- |
| Status | Implemented in PR #90; shadow calibration ready after deployment |
| Product | A Hermes-operated process that proposes, prototypes, builds, reviews, publishes, and improves Snaketron skins |
| Supersedes | PR #89's skin-factory plan |
| Depends on | PR #84's first-class skin and SkinDoc v2 work, after the Phase 0 audit in this document |
| Canonical direction | This repository; Notion is an optional import source, not a runtime dependency |
| Human owner | Alex: taste, direction changes, and publication |
| Last updated | 2026-08-21 |

## 1. Decisions

The factory is one resumable state machine, not an LLM-authored collection of
scripts. In production, Hermes schedules one command:

```text
factory run-once
```

The command owns state transitions, leases, budgets, provider calls, artifact
storage, retries, gates, and notifications. Hermes is the production control
plane. It does not calculate metrics, edit ledgers, or improvise the pipeline.

Construction and maintenance are agent-neutral. Codex, Claude Code, or another
coding agent can run the same `factory doctor`, `factory run-once` under
`mode: shadow`, `factory optimize`, and inspection commands. No required step
reads a Claude-specific transcript, directory, subscription, or executable.

Models are selected by capability role in one versioned configuration:

- **Smart text/reflection:** Gemini 3.7 Flash (`gemini-3.7-flash`), with high
  thinking for GEPA, periodic feedback-cluster analysis, and other teacher work.
- **Visual judges:** Gemini 3.7 Flash by default. A cheaper replacement must
  first prove itself in shadow evaluation against human labels.
- **Prototype and production images:** Gemini 3 Pro Image
  (`gemini-3-pro-image`) by default, behind a configurable image-provider
  interface.
- **Task worker:** a cheaper, configurable local or API model invoked by the
  factory. This is the student that executes the authoring skill during
  production and GEPA rollouts.
- **Pixel repair:** deterministic local LaMa inside the strict forge. A
  provider-backed editor is not a selectable production role until it has the
  same exact-byte, mask, budget, journal, and conformance guarantees.

The LaMa role is a versioned local bundle, not a runtime downloader. Its exact
transitive Python closure is frozen in a separate `uv.lock`; the Big-LaMa
TorchScript weight is preloaded over HTTPS, checked against a pinned byte size
and SHA-256, and stored owner-only. Doctor verifies the lock, hash, model load,
and a bounded inference without network access. The forge receives a minimal
environment with the exact `LAMA_MODEL`, offline flags, and a socket-denying
loader. The dependency lock, loader, and model identity are part of each
Attempt's behavior digest, so any drift blocks an in-flight attempt.

Every concept is created in the platform before a machine can reject it. Every
prototype, partial build, valid private skin, browser render, gate report,
judge verdict, and human decision is retained with lineage. Machine rejection
routes an attempt to a browseable archive; it never deletes it.

There is one canonical, agent-neutral skin-authoring skill. Execution consumes
it directly, and GEPA optimizes its bounded playbook sections directly. There
is no second set of derived build prompts that can drift from the skill.

The product and review decisions are explicit:

1. A prototype is a medium-length, horizontal snake strip showing a distinct
   head, representative body, and tail on a neutral or transparent background.
2. Machine triage never starts a build. A human `prototype_approval` selects
   the exact prototype artifact that becomes the build reference.
3. The final human action `publish_approval` publishes a completed skin
   revision immediately. Training labels on prototypes or builds never publish.
   Release cadence is a later policy option, not part of this version.
4. The repository copy of design direction is canonical. Notion may be
   imported once or used as a scratchpad, but production never reads from or
   writes to it.

## 2. What changes from PR #89

PR #89 contains several ideas worth preserving: prototype before build,
separate human and judge raters, deterministic gates, cost/WIP caps, holdout
evaluation, and human-only publication. This version simplifies how those
ideas are delivered.

| PR #89 design | This plan |
| --- | --- |
| Claude Code performs calibration, GEPA, export, and maintenance | Every lifecycle command is provider- and agent-neutral; Gemini 3.7 Flash supplies smart model work |
| A local LLM sequences many deterministic `factory` subcommands | One deterministic, resumable `factory run-once` command |
| Four GEPA programs at launch | Optimize the canonical authoring playbook first; optimize another target only after labels and measured value justify it |
| Canonical authoring skill plus separately tuned build prompts | One canonical skill package, loaded by SHA and optimized directly |
| JSONL operational ledgers, binary prototypes in git, and a commit per tick | Versioned database records plus content-addressed object storage; git only for behavioral artifacts |
| Five images per batch, up to three full batches, seeking a 5/5 clean batch | A configurable small batch, default three; rank it for human prototype selection and iterate only on actionable feedback |
| Judge-killed attempts may remain only in disposable run data | Every attempt is durable and browseable, including machine rejects |
| Five lifecycle phases | Two modes: `shadow` and `production` |

A 5/5 clean batch is not a useful success metric. Even if each image has an
80% chance of passing, only 32.8% of five-image batches are clean. The factory
needs one strong build reference, not proof that an image generator has zero
variance. A human needs one strong direction to select before build spend.

## 3. Goals and non-goals

### Goals

1. Produce unique, readable, performant skins from visual prototypes.
2. Preserve every attempted idea and make machine rejects easy to browse,
   annotate, re-evaluate, and retry.
3. Use SkinDoc v2 layers, textures, sprite sheets, or a hybrid according to
   the selected prototype and measured feasibility.
4. Improve the canonical authoring playbook from human decisions while keeping
   schema, safety, competitive readability, and performance constraints locked.
5. Keep human attention bounded: the machine triages, while humans select the
   build direction and remain the only publication authority.
6. Make every result auditable and replayable from stored inputs/outputs,
   provider request ids, and model, direction, skill, rubric, gate, and code
   versions. Hosted image generation is not assumed to be deterministic.
7. Run until the configurable target is reached (initially 100 published
   skins), a budget is exhausted, or a halt condition fires.

### Non-goals

- No dependency on a particular interactive agent or its conversation history.
- No weight fine-tuning in the first version.
- No automatic publication.
- No production fallback to handwritten Rust skins. A SkinDoc v2 expressiveness
  gap becomes a platform task, not bespoke factory output.
- No release calendar yet.
- No embeddings, pairwise tournaments, or multi-judge panels until simple
  retrieval and one calibrated judge prove insufficient.
- No automatic code or deterministic-gate changes. Those are ordinary tested
  repository changes, even when the factory proposes them.

## 4. Phase 0: make PR #84 a reliable foundation

PR #84 is still open and is a large, moving dependency. The implementation
must record the exact merged schema/renderer commit and run a capability audit
against code and tests. The PRD prose is not the runtime authority.

SkinDoc v2 already exposes the right abstraction: an ordered layer compositor
with ribbons, spans, head discs and ramps; solid, gradient, procedural band,
image, and text sources; transforms; anchored spans; and expressions over
`s`, `t`, `len`, `time`, `boost`, and `seed`. Animation is a property reading
`time`, not a separate animation object. The engine owns the boost layer and
the head core's placement/order; authors still select the permitted head-core
colour and ratio.

Before autonomous builds, Phase 0 must close these verified gaps:

1. Update the skin create/update path to persist the v2 document's texture
   references instead of writing an empty list.
2. Validate each texture reference for existence, declared-kind compatibility,
   and save-time authorization: it must be owned by the skin author/admin or
   explicitly shareable. Runtime bytes remain anonymously fetchable by content
   hash, through a sanitized public descriptor that excludes owner ids and
   generation prompts.
3. Resolve texture metadata before document compilation. Each texture ref must
   identify an immutable descriptor (kind, variant hashes/URLs/dimensions, body
   repeat, and frame rows), or that descriptor must be included in the canonical
   skin envelope whose hash becomes the revision content ref. Fetch and verify
   it before remote registration; do not make a synchronous compiler guess
   while an HTTP manifest is still in flight.
4. Make every served variant URL content-addressed by the variant's own hash.
   A mutable rung at `/by-ref/{canonical_ref}/{rung}.png` breaks the immutability
   promised by a content-addressed revision. Record the exact served hashes on
   the revision and attempt.
5. Remove client hard-coding of `/32.png`, dimensions, and 20 rows. Consume the
   pinned descriptor for the selected ladder rung and sheet shape.
6. Carry sheet body columns and frame rows as independent metadata. Replace the
   fixed 20-frame ceiling with a measured, configuration-backed limit. Derive
   the effective row count from desired frame rate and `period_ms`, then prove
   it against dimensions, bytes, memory, and real-renderer sampling. Sheets may
   be tall, but no declared row may be unreachable.
7. Decouple sprite-row sampling from the 32-step expression animation ring (or
   deliberately raise and prove the shared clock). A 64-row asset is not
   supported if runtime sampling can expose only about 32 rows. Browser tests
   must address every declared row and the loop back to row zero.
8. Either compile nonconstant image drift expressions correctly or reject them
   explicitly; the current v2 compiler silently reduces them to zero.
9. Make post-processing rejection enter the bounded regenerate/repair path
   rather than terminally failing after the first provider result.
10. Run the production forge through the strict manifest-producing entry point.
   A tool that prints a refusal but exits successfully is not a gate.
11. Derive required seam axes from document use: sheet animations always wrap
    on `y`, and an image used with a repeating/tile fit must also pass `x`.
12. Add an actual browser-render evidence step. Native samplers and atlas tests
   do not inspect remote image pixels, so image-bearing skins require contact
   sheets and animation captures from the real WASM renderer. Capture waits for
   atlas readiness and verifies image pixels, rather than recording the fallback
   while decode is still in flight.
13. Require an ordinary solid or procedural fallback underneath every image
    layer. Atlas decode is asynchronous, and an unready or missing image must
    leave a valid, role-readable skin rather than a blank span.
14. Enforce review-gated visibility for text layers. An unapproved private
    revision must not expose unredacted text through by-reference fetches or a
    creator's equipped skin. Either prohibit that exposure or use a separately
    cached/redacted representation with its own ETag; it cannot reuse the final
    document hash and then change after approval. Test an opponent fetch after
    the creator equips the private skin.
15. Rewrite the existing authoring skill for v2 and first-class skin APIs. Its
   valuable texture/seam craft remains, but its v1 document vocabulary, Rust
   escalation matrix, v1 template, compiled-catalog registration, and PR-based
   publication flow are stale.
16. Make revision creation/append idempotent and atomic with an expected head
    and exact document/content hash. A retry may not overwrite an immutable
    revision or leave the head pointing at different content. Make final review
    bind and transactionally publish that exact revision/hash together with its
    audit/publication transition; crash recovery must converge safely.
17. Gate the bytes that clients actually receive. The current upload worker
    re-encodes the submitted PNG and generates ladder variants after offline
    forge checks. Either ingest a complete signed/hashed forge manifest and
    ladder without re-encoding, or run the identical forge in the worker; then
    fetch, hash, and gate every stored variant before creating the revision.
18. Replace ambiguous global content-ref lookup with a deterministic document
    blob and visibility contract. Identical document bytes saved under multiple
    private, disabled, or published skins must not resolve through an arbitrary
    GSI row and leak moderation state or return the wrong 404/410 result.
19. Make group-layer flags honest. The schema accepts `boost_only` and
    `omit_on_single_cell` on groups, while current flattening drops both. Either
    propagate those semantics into every child or reject them on groups, with
    compile/conformance tests for nested boost and one-cell rendering.

Schema and renderer ceilings are read from a generated capability manifest,
not recopied into prompts. Today that includes the operation budget, flattened
layer and texture-reference limits, expression inputs, image kinds, and
animation constraints. An implementation attempt records the manifest SHA.

## 5. Architecture

```text
Hermes cron
    |
    v
factory run-once  -- lease, budgets, state machine, provider dispatch
    |
    +--> concept + attempt created in DB
    +--> Gemini Pro Image prototype strip(s) --> object storage
    +--> Gemini 3.7 Flash soft triage
           +--> machine-rejected archive
           +--> candidate/uncertain --> prototype-review inbox
                                             |
                                  human prototype_approval
                                  (exact artifact hash)
                                             |
    +<----------------------------------------+
    +--> canonical author-skin skill + task worker
    +--> deterministic build/asset/render gates
    +--> valid private Skin revision + browser evidence
    +--> Gemini 3.7 Flash fidelity triage
    +--> final-review inbox OR machine-rejected archive
                                   |
                                   v
                  human label/reject/publish/retry/feedback
                                   |
                   publish_approval publishes immediately
                                   |
                                   v
        run-once dispatches periodic GEPA / re-evaluate / retry
```

The database and object store hold operational truth. Git holds behavioral
truth: design direction, model-role configuration without secrets, the
authoring skill, judge rubrics, fixtures, and promoted optimizer outputs.
Production attempts pin those versions at start, so a promotion between ticks
cannot change an in-flight build.

The local database, content-addressed object tree, and their parent data
directory are owner-only on disk. This includes unreleased images, review-gated
text, provider traces, and human feedback; gallery authentication is not a
substitute for filesystem isolation on a multi-user Hermes host.

The factory uses a process lease and stage idempotency keys. Before each paid or
external side effect, it transactionally records an operation intent and cost
reservation. After the call it records the provider request id, resolved model,
result hash, and retry classification. If a crash leaves the result unknown and
the provider offers neither idempotency nor lookup, the operation enters
`reconciliation_required`; the factory does not silently repeat the call.
Notifications use a transactional outbox. The factory does not pause a shared
repository while an agent edits files, and it does not need a git commit for
every operational event.

An authenticated operator resolves an unknown operation with immutable evidence
using exactly one outcome: `confirmed_not_executed` releases the reservation
and permits retry; `executed_result_recovered` attaches the recovered result and
its exact resolved model, request id when available, and image media type, then
continues only after the exact retained CAS bytes, expected provider role/model,
complete image decode or side-effect-specific structured schema all validate;
`executed_output_lost` or `indeterminate` charges the reservation and leaves the
attempt terminally blocked. Invalid recovery evidence remains immutable but
cannot remain a replayable success: the operator path leaves it unresolved, and
a legacy/direct invalid success is terminally quarantined on first replay with
actionable evidence. Every recovered result, including structured and image
content, additionally requires the exact immutable journaled request CAS and
request hash. Snaketron registration and review/publication
effects require authenticated exact-state readback before local advancement;
recovered Git promotion is accepted only when its deterministic ref/SHA already
matches the committed active-behavior pointer. The cron service identity cannot
create an operation resolution.

At the end of each cycle, `run-once` checks optimizer readiness and enqueues or
advances one budgeted, resumable optimization job when eligible. The operator
command `factory optimize --if-ready` invokes the same path on demand; it is not
a second required cron. Thus the unattended self-improvement loop still has one
Hermes schedule.

## 6. Durable data model

The first implementation may map these records onto PR #84's DynamoDB/S3
patterns, but it must preserve these semantics:

| Record | Required content |
| --- | --- |
| `Concept` | Brief, seed/source, tags, created time, current disposition, current attempt |
| `Attempt` | Concept, purpose (`production|optimizer_rollout|technique_trial`), parent/restart stage, state/review kind, optimistic version/idempotency key, approved prototype hash/decision id, behavior versions, cost, experiment candidate/split link, optional production Skin/revision |
| `Operation` | Attempt/stage, idempotency key, side-effect/provider role, request hash, cost reservation, request id, status/retry class, result hash |
| `OperationResolution` | Operation, resolution, evidence/result reference, authenticated actor, time |
| `Artifact` | Attempt, stage, kind, content hash, object reference, dimensions/manifest, prompt provenance |
| `Evaluation` | Artifact, evaluator and version, hard/soft result, structured reasons, measurements |
| `HumanDecision` | Artifact/attempt, action, literal feedback, quick tags, actor, time |
| `FeedbackRoute` | Human decision, optimizer target, root-cause signature, confidence, classifier version |
| `TechniqueCandidate` | Source trace, experimental recipe, fixture artifacts, varied-trial results, disposition |
| `OptimizationRun` | Target, dataset version, teacher/student configs, candidates, train/eval metrics, promoted version |
| `OutboxMessage` | Idempotency key, destination/event reference, payload hash, delivery status and attempts |

An attempt has a stage and a disposition. The minimal dispositions are
`active`, `needs_human`, `machine_rejected`, `human_rejected`, `published`, and
`blocked`. Concept, Attempt, Operation, OutboxMessage, TechniqueCandidate, and
OptimizationRun are versioned state rows whose transitions use conditional
writes. Each terminal provider/delivery attempt is retained. Artifacts,
evaluations, human decisions, feedback routes, and operation resolutions are
immutable audit history. A retry creates a linked child instead of rewriting an
earlier attempt.

The factory runs under a dedicated service identity that may create and update
its own private skins but cannot approve prototypes, publish, override blocking
gates, or resume a human halt. A Concept acquires one stable private Skin id at
its first valid build; later valid Attempts append immutable revisions to that
Skin. Machine triage belongs to Attempt/Evaluation records, not PR #84's
`pending_revision`.
A final `publish_approval` names the exact reviewed revision and content hash so
a newer head revision can never be published accidentally. A separate human
`prototype_approval` names an exact prototype artifact and attempt version;
that transition is the only authority to enter authoring/build execution.

Only `purpose: production` Attempts may append to the Concept's stable Skin or
enter either human review inbox. Optimizer and technique trials use a separate,
non-publishable evaluation namespace linked to their run/candidate/split. If a
real-render test requires registration, it receives an isolated trial Skin id
that no identity can publish. Trial artifacts remain inspectable in the
Experiments view but never become a production head revision.

All concepts are visible in a factory gallery with these views:

- **Needs review** — separate prototype-direction and final-build inboxes.
- **Machine rejected** — deterministic failures and judge rejects, with the
  failed stage, preview, measurements, and reasons.
- **Human rejected** — human decisions and feedback.
- **Published** — approved skins and their lineage.
- **Experiments** — retained optimizer and technique rollouts, separate from
  production review.
- **All** — every concept and attempt.

Rejected source and build bytes are retained indefinitely by default, with cold
storage allowed. Exact re-evaluation remains supported for their lifetime. A
future purge policy must mark re-evaluation unavailable and leave only
regeneration/retry; it may not silently treat a preview as the original.

The gallery supports two distinct recovery operations:

1. **Re-evaluate** runs current gates and judges against the existing artifact.
   This is cheap and appends a new evaluation; it does not regenerate anything.
2. **Retry from stage** creates a child attempt at `prototype`, `assets`, or
   `build`, using current direction, skill, gates, and models plus the human's
   feedback. A bulk form can retry selected rejects after a skill or gate
   promotion. A prototype retry clears the approval fields and needs a new
   `prototype_approval`; an asset/build retry copies the approved prototype hash
   and decision id into the child and remains authorized only for that exact
   artifact, unless a human selects a different one.

For a production Attempt already in final review, either action first journals
cancellation of the exact pending skin/revision/content hash. A child is
created atomically only after cancellation succeeds or authenticated recovery
readback proves that revision is neither pending nor published. Bulk retry
preflights every selected Attempt, including exact final-revision authority,
before it begins any cancellation.

Humans may also override soft triage by issuing `prototype_approval` for the
exact rejected artifact. Re-evaluation that changes a machine verdict still
does not start a build without this human transition.

Each deterministic gate declares `blocking: true|false` in the versioned gate
manifest. Integrity, ownership, schema, safety, required asset-shape/seam,
performance, and conformance gates block stage advancement and publication and
cannot be overridden.
Diagnostic quality measurements and visual judgments soft-route and may be
overridden. The CLI and gallery consume the same policy.

## 7. Model and provider contract

Models are configured by role, not scattered model names:

```yaml
models:
  task_worker:     { provider: lmstudio, model: configurable }
  smart_text:      { provider: gemini, model: gemini-3.7-flash, thinking_level: high }
  visual_judge:    { provider: gemini, model: gemini-3.7-flash, thinking_level: high }
  image_generator: { provider: gemini, model: gemini-3-pro-image }
  image_editor:    { provider: local_lama }
```

The exact config, requested/resolved model identifier, provider request id, and
sanitized response metadata are stored on every attempt. Changing a pinned
model creates a new config version and requires shadow evaluation; there is no
silent fallback. Provider adapters expose typed refusal, unavailable, timeout,
quota, and invalid-output results.

Secrets enter the factory process through its service environment or secret
store. Cron must not assume an interactive shell has sourced `~/.zshrc`, and
keys are never committed or copied into run artifacts. A non-secret service-env
manifest declares required variables, and `factory doctor --json` verifies
provider roles, DB/object storage, browser capture, and LaMa without printing
credential values. The Snaketron credential is a dedicated opaque 256-bit
factory-service token, not the account's 24-hour login JWT. The server stores
only its hash, reloads its revocation state and account on every request, and
accepts it only on a fixed factory-route allowlist. It has no time-based expiry,
so unattended operation survives ordinary login expiry; administrator-only
provision, atomic rotation, and revocation provide its lifecycle. The online
doctor also calls an authenticated,
side-effect-free Snaketron capability endpoint: the dedicated durable account
must be able to create private and evaluation skins and upload private forge
textures, while its live DB-derived identity has neither administrator nor
publication authority.

Gemini 3.7 Flash is the teacher in GEPA: it reads failed student traces and
proposes improved instructions. The task worker remains the student and runs
the high-volume rollouts. Using the same cheap model to critique and train
itself is not the default design.

Task-model adapters share one typed protocol. `WorkerRequest` carries the
pinned skill bundle and SHA, capability manifest, input artifact references,
read-only pure-tool allowlist, budget, and JSON output schemas. Workers receive
no provider, storage, Git, publication, raw-network, or unrestricted-shell
credentials. `WorkerResult` returns the implementation plan, SkinDoc, structured
asset/tool requests, trace, usage, and typed failure; it performs no external or
mutating side effect itself. Only the factory driver executes those requests,
through the lease, budget checks, and durable `Operation` journal. A
deterministic bundle renderer and adapter conformance suite prevent workers from
depending on filesystem skill discovery or agent-specific tool names. M2 proves
the fake adapter and the configured real adapter against the same fixtures.

## 8. Production pipeline

### 8.1 Ideate and persist

Create the `Concept` before scoring it. Ideation reads repo-canonical direction,
recent published concepts, machine/human rejects, and relevant literal human
feedback. Start with simple name/tag/text similarity and a Gemini score. Add an
embedding service only if duplicate concepts remain a measured problem.

Concept scoring ranks work; it does not erase low-ranked ideas. A concept not
selected in the current tick remains browseable and may be retried or manually
promoted.

### 8.2 Prototype

The prototype contract is exact:

- one medium-length snake, horizontal;
- visibly distinct head, representative body, and tail;
- neutral or transparent background;
- composed to remain legible at game scale, not as poster art;
- no UI chrome, labels, unrelated scenery, or multiple alternatives in one
  image.

Each prototype also has a manifest:

```json
{
  "brief": "...",
  "palette_intent": "...",
  "motion_intent": "...",
  "implementation_hint": "layers|texture|sprite_sheet|hybrid",
  "hint_rationale": "...",
  "prompt": "...",
  "model_config": "...",
  "image_sha256": "..."
}
```

Gemini 3 Pro Image generates a configurable number of independent prototypes,
default three. Gemini 3.7 Flash returns `candidate`, `uncertain`, or
`machine_rejected` for each, with reasons. Candidates and uncertain results are
ranked into the prototype-review inbox; the others remain attached and
browseable. A human `prototype_approval` selects the exact image hash and
manifest that become the build reference. If all are rejected, the attempt
enters the machine-rejected gallery, but a human may still approve one as a soft
triage override or request a retry. No prototype reaches authoring or build
execution without that explicit approval.

Prototype references come only from versioned assets Snaketron owns or has
licensed for this use. Prompts, references, and outputs retain provenance.
Protected marks/likeness and safety concerns are recorded as review flags; a
machine flag can archive an attempt but can never waive the human publication
decision.

Do not automatically spend three more full batches seeking a clean batch.
Iterate only when machine or human feedback identifies a correctable prompt
defect. A human may label a rejected image usable, approve it as the direction,
add feedback, or request a new prototype batch.

### 8.3 Choose the implementation after prototype approval

The human-approved prototype's hint is advisory. The authoring skill must emit
an `implementation-plan.json` before generating assets:

```json
{
  "path": "layers|texture|sprite_sheet|hybrid",
  "rationale": "...",
  "fidelity_features": ["..."],
  "layer_plan": ["..."],
  "asset_plan": [{
    "kind": "sheet",
    "natural_length_cells": 20,
    "frames": 32,
    "anchor": "whole|head|tail",
    "fit": "tile|clip|stretch|cutout",
    "fade": "none|leading|trailing|both"
  }],
  "animation_plan": ["..."],
  "required_wrap_axes": ["x", "y"],
  "risks": ["..."]
}
```

Selection rules:

- **Layers** are preferred for checkerboards, stripes, bands, geometric marks,
  gradients, palette-aware work, and formula-driven animation. They are cheap,
  role-aware, crisp, easy to tune, and visible to deterministic tests.
- **Texture** is for painterly, organic, illustrative, or highly irregular body
  art that the object model would approximate poorly.
- **Sprite sheet** is for motion that needs distinct drawn frames rather than
  parameter changes.
- **Hybrid** composes image sources with ordinary v2 layers for contours,
  role cues, highlights, head/tail treatment, and additional effects.

The worker may revise the prototype hint after an implementation probe. It
records why. If no v2 composition can deliver the prototype, the attempt is
retained and a platform task is proposed; production does not create a one-off
Rust renderer.

SkinDoc v2 has no special head/body/tail image slots. Image assets are ordinary
anchored span layers with a procedural fallback below them. Because live snakes
vary in length, X is the asset's natural authored/repeat length, not the current
snake length; every image plan declares anchor, fit, and fade behavior for
shorter and longer snakes. Browser fixtures cover short, median, and long poses.

### 8.4 Authoring skill

The current `.claude/skills/author-skin` is stale relative to PR #84. Phase 0
moves its canonical content to a neutral package such as:

```text
skills/author-skin/
  SKILL.md                    # short workflow and routing
  references/contract.md     # locked schema, safety, performance invariants
  references/playbook.md     # GEPA-tunable recipes and heuristics
  references/prototypes.md
  references/layers-effects.md
  references/textures-sprites.md
  references/validation.md
  templates/
```

Agent-specific discovery paths are thin links or wrappers. The factory never
depends on discovery: it explicitly loads and records the canonical package
SHA. The package is both the execution program and the optimization target.

Preserve the current skill's measured craft: repeat length and mark scale,
friend/foe cues, `[T, X, T]` inpaint wrapping, roll-and-repair, multi-scale seam
checks, wrap-aware resizing, op-count invariance, real-render inspection, and
contact-sheet review. Rewrite the outdated v1/Rust/compiled-catalog sections
for v2 documents, texture manifests, first-class private revisions, and the
review API.

### 8.5 Assets and animation

Gemini 3 Pro Image is used both for prototypes and for final textures/sprites.
The provider remains configurable and its model/version is recorded.

For generated texture `T`:

1. Ask for the required axes to be seamless and record the intended use.
2. Measure every required join before repair at multiple scales.
3. For a crop, use the `[T, X, T]` inpainting construction and keep `[T, X]`.
   The first implementation executes repair with local LaMa inside the strict
   forge; a failed repair is retained and regenerated through the configured
   image-generator role.
4. For an already-nearly-tileable generated image, roll the join to the center,
   repair the narrowest viable band, restore untouched pixels, and roll back.
5. Resize and filter with wrap-aware operations, then remeasure the exact bytes
   that ship. Reject structural mismatch or excessive detail/chroma loss.

Wrap requirements come from usage: a looping sprite always wraps on `y`; it
wraps on `x` only when repeated along the body.

There are two animation paths:

- **Effect layers.** An effect is authoring shorthand for one or more ordinary
  v2 layers. Time in opacity, transform, gradient stops, band settings, colour
  lightness, or disc radius can create shine, pulse, movement, and stacked
  effects. For bands, only `half_width`, `t_center`, and `alpha` animate;
  `period_cells`, `duty`, and `phase_cells` remain static. Spans, fit, fade
  topology, and layer order are also static, and each property may read only the
  inputs allowed at its evaluation site. Image drift remains constant until
  Phase 0 either implements its expression path or rejects it explicitly. The
  authored layer topology stays fixed over time.
- **Sprite animation.** The generated asset is an X-by-Y grid: X body cells
  cover distance from head toward tail; Y rows are animation frames. Texture
  metadata states X, Y, texels per cell, and wrap axes explicitly; the single
  SkinDoc `period_ms` controls the whole skin, so sprite and effect layers share
  a clock. Animated sheets should generally be tall, with enough frames to
  remain smooth in slow motion. Row zero must be a valid resting and
  reduced-motion frame. Grid alignment, temporal continuity, frame translation,
  loop seam, dimensions, bytes, and palette are gated before upload.
  If the full grid is substantially taller than provider-supported image
  ratios, generate bounded contiguous Y ranges instead of center-cropping one
  portrait image. Journal and retain every slice request/output, bind global
  frame ranges and continuity references in each prompt, normalize without
  cropping, and deterministically assemble the exact full grid before forge.
  Ordinary sheets that map closely to a provider ratio remain one call.

Gemini 3.7 Flash periodically mines successful novel traces and feedback
clusters into `TechniqueCandidate` records. Each candidate materializes an
experimental recipe plus stored fixture artifacts, then runs the task worker on
a configured minimum of structurally different, human-approved prototype
fixtures. These rollouts run in a sandbox through the real gates and renderer;
they never create or append a production Skin revision. A fresh visual fixture
needs the same human `prototype_approval` before use. A recipe is promoted
automatically into the marked playbook section only when blocking gates and the
frozen fidelity metric pass across those varied trials. Fixture or gate *code*
changes remain ordinary reviewed repository changes. This lifecycle is how new
animation and asset techniques become durable authoring knowledge.

### 8.6 Build, render, and triage

The task worker follows the pinned authoring skill and implementation plan.
The deterministic driver invokes asset tools, writes the SkinDoc v2 document,
validates it, uploads textures, registers a private revision, uses a cached
browser/WASM bundle pinned to the renderer SHA, captures browser evidence, and
packages the result. A data-authored skin does not rebuild WASM per attempt.

Gate order:

1. schema, reference integrity, dimensions, budgets, and content hashes;
2. asset seam/detail/chroma/grid/temporal checks;
3. operation-cost and conformance suites;
4. real browser contact sheet and animation capture;
5. Gemini 3.7 Flash comparison of render to the selected build-reference
   prototype, followed by
   readability, role clarity, animation quality, and overall craft.

Blocking deterministic gates halt progression but retain the attempt;
diagnostic gates add evidence for triage. Visual judgment is always soft triage.
The strict forge performs at most one measured LaMa repair for each required
join and rechecks every join afterward. Failed repairs are retained rather than
entering an unbounded editor loop. It never downloads weights at repair time;
missing, public, symlinked, size-mismatched, or hash-mismatched weights are a
blocking installation/runtime failure.

Once all pre-revision blocking gates pass, create or append the exact private
Skin revision before visual triage. A build rejected by the judge therefore
remains a real private skin in the system and can be previewed or re-evaluated
later.

### 8.7 Human prototype review, final review, and publication

There are two explicit human gates. At prototype review, candidates and
uncertain results wait for `prototype_approval`, which binds the exact artifact
hash and authorizes only the build. A prototype `human_rejection` may carry
feedback; a machine reject remains approvable from the archive as a soft-triage
override. At final review, `publish_approval` binds the exact private revision
and content hash and publishes immediately, or `human_rejection` retains it.

In `shadow` mode, machine evaluations are recorded but hidden until independent
human labels are collected for both prototype and completed-build samples. The
training-only actions are `prototype_label` and `build_quality_label`; neither
starts a build or publishes. After blind prototype labeling, the separate
direction-review action may record `prototype_approval`. After blind build
labeling, the separately presented final review may record
`publish_approval`. In `production` mode, machine `candidate` and `uncertain`
prototypes enter the prototype inbox, while candidate and uncertain builds
enter the final inbox; machine rejects enter the browseable archive. A
configured sample of rejects also appears blind in the labeling inbox to
measure false rejects.

Use separate WIP caps for active generation, pending prototype approval, and
pending final review. A full prototype queue stops new ideation/prototyping; a
full final queue stops new builds. Neither accumulates unlimited suspended
batches.

Other human actions are feedback-only, re-evaluate, retry from stage, and
override soft triage. Every decision stores `rater: human`; judge outputs store
their evaluator config and are never silently treated as human labels. The
factory service identity has no permission to create a
`prototype_approval` or `publish_approval`.

## 9. Feedback and self-improvement

Literal feedback is attached immediately to the concept and is retrievable on
the next relevant attempt. Inference and behavioral changes happen in periodic
optimization, not as an uncontrolled edit after every verdict.

During optimizer-readiness analysis, Gemini 3.7 Flash classifies each new human
decision into a versioned `FeedbackRoute`: one target below, a normalized
root-cause signature, confidence, and evidence links. Low-confidence routes do
not trigger automatic changes. These records make repeated clusters and
repeat-after-promotion halts measurable without treating the classifier's
inference as human ground truth.

The routing taxonomy has six targets:

| Target | Example | Promotion rule |
| --- | --- | --- |
| Direction | "No horror themes" | Human-approved repo diff only |
| Prototype prompt | Right concept, consistently wrong visual draft | Optimize after a labeled cluster |
| Authoring playbook | Render fails to implement the selected look | Primary GEPA target |
| Judge rubric | Human repeatedly reverses a judge pattern | Shadow-evaluate candidate rubric |
| Deterministic gate | Human catches a measurable seam/contrast/grid defect | Tested code change through normal PR review |
| Platform | SkinDoc/renderer/tooling cannot express or render it | Normal product/engineering task |

### 9.1 Primary optimization: the authoring playbook

Ten new human-labeled builds or a repeated high-confidence failure cluster may
make the optimizer generate candidates. Automatic promotion has a higher,
configured evidence threshold and never follows from ten labels alone:

1. Freeze a dataset version and group related concepts into train, development,
   and a sealed promotion holdout so close variants cannot cross boundaries.
2. Select one target only, initially `references/playbook.md`.
3. Run the task worker in a sandbox through the real deterministic driver,
   using only exact prototypes that already carry `prototype_approval` or a
   separately human-approved fixture corpus. Rollouts may use isolated assets
   but never create or append production Skin revisions. Gemini 3.7 Flash is
   GEPA's reflection model and proposes candidate playbook edits from traces
   plus textual feedback.
4. Score new rollout outputs with blocking/diagnostic gates, fixture-specific
   expected properties, and a **frozen** visual-fidelity judge previously
   calibrated on human labels. Historical human decisions calibrate this
   callable metric; they do not directly score new pixels.
5. Use development groups to select the candidate, then query the sealed
   promotion holdout once. The teacher and candidate selector never see its
   traces. Enforce query budgets and a human-controlled refresh policy to
   prevent repeated optimization from training on the holdout.
6. Promote only when minimum paired sample, effect/confidence, and
   non-inferiority thresholds pass and no blocking gate regresses. If the
   frozen judge's human calibration is stale, candidates may be recorded but
   not auto-promoted.

GEPA may edit only marked playbook sections. The contract, security rules,
budgets, publication authority, and deterministic gates are locked. This gives
the system automatic craft improvement without allowing prompt optimization to
rewrite its own safety or scoring rules.

Promotion uses a dedicated clean bot worktree and branch, verifies the expected
base SHA, and validates the bounded diff. It commits the candidate, pushes it
under a protected, append-only signed version tag, and verifies that exact SHA
from a clean clone before conditionally updating the database's active-skill SHA
between attempts. Failure before the pointer update leaves the commit inactive;
failure after it is handled by rolling the pointer back. The scheduled Hermes
checkout is never modified, and every active version remains durably reachable
from the canonical repository.

Prototype-prompt or judge-rubric optimization uses the same mechanism later,
one target at a time. Ideation GEPA is deferred until ideation quality is a
measured bottleneck.

### 9.2 Judge calibration and false rejects

Gemini 3.7 Flash begins as the smart visual judge, but starts in shadow mode.
Track precision and recall against human labels separately for prototype and
completed-build decisions. The machine-rejected gallery and sampled-reject
inbox make false rejects observable.

Production triage requires a configurable minimum labeled sample and acceptable
false-approve/false-reject bounds. Do not claim calibration from four holdouts
or ten decisions. Report sample size and confidence alongside rates. When data
go stale or reversal rates exceed the threshold, return to shadow routing
without stopping artifact creation.

A future cheaper/local judge may be trained by prompt optimization with Gemini
3.7 Flash as teacher, then substituted only after it clears the same shadow
evaluation. A judge's own decisions are never its labels.

## 10. Modes, budgets, and halts

### Shadow

- Same pipeline and persistence as production.
- Machine scores and reasons are hidden from the human until the human labels
  the item, preventing anchoring.
- Prototype and completed-build samples receive separate training-only human
  labels; prototype direction approval and publication review remain separate
  actions.
- The human prototype gate still applies: blind labels alone never trigger a
  build.
- No machine verdict prevents human browsing or review.

### Production

- Hermes runs `factory run-once` on schedule.
- Immediately before every scheduled invocation, the fixed wrapper verifies
  the owner-private paid-smoke marker against the current config/model,
  renderer, and LaMa behavior and repeats the live least-privilege Snaketron
  probe. Drift exits before provider spend. An exact journaled, held-out,
  immutable author-playbook promotion is the sole skill-only exception.
- Machine triage routes the prototype inbox, final inbox, and rejected gallery.
- Only a human-approved prototype proceeds to build.
- Human remains the only publication authority.
- Periodic optimization may promote bounded playbook changes between attempts.

Hard caps live in versioned config and are enforced before spend: concurrent
attempts, pending prototype reviews, pending final reviews, images per
prototype, provider retries, wall time, per-attempt cost, daily
cost, and total program cost. Each model role supplies pinned input-context and
output-token ceilings; requests transmit the output ceiling, reservations use
both full ceilings, and billed thinking tokens count as output. Missing usage
keeps the conservative reservation charged. Signed Git promotion has one
overall subprocess deadline plus cleanup budget and cannot start late in a
Hermes tick.

Pause new generation when any of these occurs:

- provider or pinned-model mismatch;
- database lease/state corruption;
- cost cap reached;
- prototype-review or final-review WIP cap reached;
- repeated deterministic failures suggesting a platform regression;
- judge reversal/uncertainty rate exceeds its configured production bound;
- repeated root cause after a playbook promotion;
- required browser evidence cannot be produced.

Existing attempts and rejected-gallery browsing remain available while paused.
Only the affected stage needs to stop; a judge calibration problem returns
routing to shadow rather than disabling deterministic builds.

## 11. Metrics

| Metric | Purpose |
| --- | --- |
| Human publication rate, first attempt and eventual | Overall product quality and recovery |
| Machine-reject reversal rate | The false-reject measure made possible by retaining rejects |
| Prototype-to-valid-build conversion | Whether visual directions are implementable |
| First-pass deterministic gate yield | Authoring execution quality |
| Prototype fidelity on human-labeled evaluation groups | Whether builds honor selected drafts |
| Retry lift by source stage and pipeline version | Whether feedback and improvements actually help |
| Repeated-root-cause rate | Whether assimilation prevents recurrence |
| Prototype/final-review WIP and latency | Whether the factory respects attention and build spend |
| External cost per published skin | Sustainability |

The terminal target is configurable; initially, 100 human-approved and
immediately published skins.

## 12. Delivery order

### M0 — foundation audit and portable authoring skill

- Land/rebase the required PR #84 surface and pin its commit.
- Close all Phase 0 texture/reference/manifest/browser-evidence gaps.
- Move and rewrite `author-skin` as a v2-first neutral package.
- Add repo-canonical direction and owned-anchor manifests.
- Add fixtures proving layers, texture, sprite-sheet, and hybrid routes.

**Exit:** an arbitrary coding agent can take one human-approved usable prototype
through the canonical skill into a valid private revision, with all evidence
captured.

### M1 — durable attempts and review gallery

- Add Concept/Attempt/Operation/OperationResolution/Artifact/Evaluation/
  HumanDecision/FeedbackRoute, TechniqueCandidate, OptimizationRun, and
  transactional-outbox storage.
- Store every stage's artifacts in content-addressed object storage.
- Add prototype review, final review, Machine rejected, Human rejected,
  Published, and All views.
- Add exact-artifact prototype approval, feedback, override, re-evaluate,
  retry-from-stage, and bulk retry.
- Add factory-service versus human-reviewer authorization; the service identity
  has no prototype-approval, publish, gate-override, or human-resume permission.

**Exit:** a machine-rejected prototype and a machine-rejected private build are
both browseable, explainable, and recoverable without altering history, and
only an exact human-approved prototype can enter build execution.

### M2 — provider-neutral factory in shadow mode

- Implement `factory doctor` and `factory run-once`, leases, stage idempotency,
  durable provider-operation intents, the transactional outbox,
  migrations/backups, budgets, and resumability.
- Add failure injection at each provider-call and persistence boundary; an
  unknown non-idempotent call must reconcile or halt rather than repeat spend.
- Add the authenticated operation-resolution workflow; the cron identity cannot
  resolve unknown outcomes.
- Add role-based providers with Gemini 3.7 Flash and Gemini 3 Pro Image defaults.
- Add the typed worker protocol and conformance tests for fake and configured
  real adapters, including proof that workers cannot perform external side
  effects outside the driver journal.
- Run prototype, build, real-render, and visual triage in shadow mode.
- Install Hermes as a non-agent cron wrapper with explicit workdir and service
  environment; test install, status, smoke run, alerting, and rollback. The
  operator skill is not the scheduler's reasoning engine.

**Exit:** Hermes can run unattended while independent human labels are collected
for prototype and build samples with machine verdicts hidden, and build work
starts only after a separate human prototype approval.

### M3 — production routing and self-improvement

- Enable production triage after shadow thresholds are met.
- Add sampled rejects to the human inbox.
- Implement periodic GEPA of the authoring playbook with Gemini 3.7 Flash as
  teacher and the configured task worker as student.
- Promote versioned playbook candidates atomically under held-out evaluation.
- Mine, validate, and promote one new animation or asset recipe through the
  `TechniqueCandidate` lifecycle in the isolated evaluation namespace.

**Exit:** human feedback produces a measured playbook improvement, a rejected
idea succeeds on a linked retry, and `publish_approval` publishes immediately.

## 13. Acceptance criteria

1. Neither setup, calibration, production, nor maintenance requires Claude or
   Claude Code.
2. Production is scheduled by Hermes through one resumable factory command.
3. Gemini 3.7 Flash is the default smart/reflection and visual-judge model;
   Gemini 3 Pro Image is the configurable default for prototypes and build
   assets.
4. The v2-first authoring skill is directly used by execution and is a bounded,
   automatically optimized artifact.
5. Build selection explicitly supports layers, texture, sprite sheet, and
   hybrid, with layers preferred for patterns and formula-driven effects.
6. Texture and sprite assets pass measured seam/grid/temporal gates on the
   exact bytes served to clients.
7. Sprite metadata supports independent X body columns and Y frame rows, with
   tall animation sheets, no hard-coded row count in the client, and a renderer
   test proving every declared row is reachable.
8. Every rejected concept, prototype, and build is durable and browseable.
9. Humans can re-evaluate old artifacts and retry from a chosen stage under the
   latest gates and skill.
10. Judge-authored decisions never become human labels; false rejects remain
    observable through sampling and the archive.
11. A human `prototype_approval` binds the selected artifact hash; neither a
    machine verdict nor a training label can start a build.
12. A human `publish_approval` binds the reviewed revision/content hash and
    publishes it immediately; shadow labels never publish.
13. The repository is the sole canonical source for direction and behavioral
    artifacts.
14. A newly discovered animation/effect technique can move from trace to
    experimental recipe, varied fixtures, and an automatically promoted
    playbook version without allowing fixture or gate code to self-modify.
15. Hermes uses a no-approval/no-resolution/no-publish service identity, and its
    one scheduled command also dispatches eligible optimization work.
16. Paid provider calls have durable intent/result records; crash recovery
    never blindly repeats an operation whose outcome is unknown.
17. Task workers cannot call providers, storage, Git, or publication paths
    directly; only the factory driver performs side effects through budgets and
    the operation journal.
18. Optimizer and technique rollouts use approved fixtures in an isolated,
    non-publishable namespace and never append unpromoted output to production
    skins.
19. Every Hermes tick revalidates its paid-smoke behavior pin and live
    Snaketron service capabilities before spend; config/model, renderer, LaMa,
    administrator-authority, or storage-capability drift fails closed, while an
    exactly retained automatic author-playbook promotion remains runnable.
