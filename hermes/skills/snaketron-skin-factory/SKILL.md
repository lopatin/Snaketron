---
name: snaketron-skin-factory
category: snaketron
description: "Drive the Snaketron skin factory production loop entirely on the local model: ideate skin concepts, build them on deterministic rails with GEPA-tuned prompts, submit to Alex's review queue on Snaketron.io, record and route every decision, and escalate anything judgment-heavy to a Claude Code maintenance session. The production SOP for specs/skin-factory-prd.md."
tags: [snaketron, skins, self-improvement, loop, gepa, reflexion, local-model]
platforms: [macos]
---

# Snaketron Skin Factory — production loop

Everything here runs locally. No Claude, no API LLM calls — the two external
calls are image generation (Gemini) and, only if the export gate recorded that
fallback, the judge model. The craft does not live in you: it lives in the
GEPA-tuned prompts under `skin-factory/prompts/` and in the deterministic
`factory` CLI. **Everything mechanical is a `factory` subcommand** — polling,
cursor management, ledger appends, agreement math, halt evaluation, spend
metering, the per-tick commit. You sequence those calls, run the risk-tiered
retrospectives, and write notification prose. You compute nothing, and you
never open a ledger file yourself. When a task needs judgment about *changing
the stack*, you never do it — you write it to the escalation queue for the
next maintenance session.

The full design is `~/Snaketron/specs/skin-factory-prd.md` (the PRD). Section
references below point there. When this SOP and the PRD disagree, the PRD wins;
flag the disagreement to Alex.

## When to use

- Cron tick (M3 cruise: every 2 hours; M2: on Alex's cadence).
- Alex says: "process the skin queue", "make a skin", "skin factory status",
  "pause/resume the factory", "dry-run a tick" (the export-gate rehearsal).
- Do **not** use for calibration or maintenance — those are interactive Claude
  Code sessions Alex starts in the repo (PRD §11.6, §12.1). If asked, say so
  and report the escalation-queue depth.

## Layout

| Path | What |
| --- | --- |
| `~/Snaketron` | The repo. Builds happen in a dedicated worktree, never on master. |
| `~/Snaketron/skin-factory/config.yaml` | Phase, caps, WIP limits, pinned model configs (chat, judges, embedder — model + version/quant), thresholds. The only file you read for policy. |
| `~/Snaketron/skin-factory/state/` | `catalog.jsonl`, `lessons.jsonl`, `decisions.jsonl`, `escalations.jsonl`, `metrics.json`, `audit.md` (append-only), `loop-state.json`, `prototypes/<concept-id>/` (approved prototypes + alternates — committed, they are build targets and taste data) |
| `~/Snaketron/skin-factory/prompts/` | GEPA-tuned slot prompts (ideation, prototype, build slots). Read-only to you — edited only in maintenance sessions. |
| `~/Snaketron/skin-factory/judges/` | Judge rubrics (concept + prototype + craft) + the pinned judge config. Read-only to you. |
| `~/Snaketron/skin-factory/runs/<id>/` | Per-attempt traces (gitignored): slot inputs/outputs, gate reports, judge outputs |

## Preflight (every invocation)

1. `config.yaml` readable; `phase` is `m2` or `m3` — with exactly one
   carve-out: an explicit **"dry-run a tick"** request from Alex is allowed in
   `m1` (submissions stubbed, no state mutation beyond the audit note). Any
   other invocation outside m2/m3: dormant — report and stop.
2. LM Studio serving the pinned chat model, the pinned embedding model, **and
   the judge model with its vision projector loaded** (config names all three,
   projector file included). Probe the vision path with a trivial image before
   the first judge call of the tick — the prototype and craft judges read
   images, and a projector that silently failed to load produces confident
   text-only guesses instead of an error. A different model/quant than pinned,
   or a missing projector, is a halt — never a substitution.
3. `factory budget` passes — if a cap is exceeded, record a no-op tick in the
   audit log and stop. (The CLI meters every external call itself and refuses
   over-budget ones; this check is the outer layer, not the only one.)
4. `loop-state.json` is neither `halted` nor `paused`. Halted: report why and
   stop — only Alex clears a halt. Paused: a maintenance session or Alex owns
   the workspace right now; report and stop.
5. Platform reachable: submission API + decision feed (PRD §10.2).

## The tick

Order matters: **ingest and route decisions before building**, so feedback is
never left sitting while the factory produces more of the same mistake.

### 1. Ingest — `factory ingest`

Polls the decision feed, advances the cursor, and appends the rows it finds —
queue verdicts on submitted skins, plus per-image verdicts on any prototype
batch that went to Alex through the sampling lane. Every row carries `stage`
(prototype | implementation | queue) and `rater` (alex | judge:<config-id>);
only `rater: alex` rows are ever calibration data (PRD §5.5). It resumes any
concept whose prototype review has come back, and recomputes `metrics.json`
(per-stream trailing agreement, error split, approval rate, prototype yield) —
all deterministic code. You read its report; you do not redo its math.
Holdout labels are assigned per-stream by `factory append`, wherever a row is
written — not here (PRD §7.3).

### 2. Retrospect — locally, risk-tiered (PRD §11.5)

For each new **non-holdout** decision and each kill, run the retrospective
procedure with the local model over the run trace + feedback + taxonomy (PRD
§11.3). Holdout-assigned decisions are **record-only**: `factory ingest`
already wrote the row; no lesson, no exemplar, no escalation content beyond a
count. For the rest, split what the retrospective proposes by tier:

- **Auto-apply — facts, verbatim:** catalog status and Alex's literal feedback
  attached as a quote. Via `factory append` only.
- **Pending:** any *inferred* lesson (the model's reading of *why*) is
  appended with status `pending` — inert, excluded from every prompt context,
  until a maintenance session activates it. An ideation negative exemplar is
  written **only** when the retrospective routed the rejection to a
  concept-level layer (1, 2, 3, or taste-flavored 3a); an execution failure
  says nothing about the idea.
- **Escalate:** anything behavioral — proposed edits to slot prompts, skills,
  judge rubrics, gates; specific→general promotions; taste proposals; any
  low-confidence or unroutable retrospective. Full evidence bundle to
  `escalations.jsonl`, via `factory append`.

`factory append` refuses non-append mutations; after this step run
`factory write-guard`, which verifies nothing outside `state/`, `runs/`, and
the build worktree changed. If something did: revert it and halt
(`behavioral-edit-outside-maintenance`).

### 3. Check halts — `factory halt-check`

Evaluates PRD §12.4 mechanically: 3 consecutive rejections · 3 consecutive
zero-approval prototype kills or a trailing-10 clean-batch rate under the
floor (the taste stream's own alarm — the agreement rule cannot fire on a
stream the judge labels alone) · 5 consecutive build kills (the CLI re-runs
the deterministic holdout builds and reports only pass/fail per brief + the
bisected suspect change — never holdout traces) · trailing-20 agreement
< 0.60 · repeat root cause · budgets · stale queue including unreviewed
prototype batches (> 14 days) · 3 halts in 7 days → full stop. On any: set `halted` + reason in
`loop-state.json`, `factory append` a `# HALT` to the audit log, notify Alex
with the reason and the one action that unblocks (usually "start a maintenance
session"). Never auto-resume.

### 4. Build (only if pending < `wip_cap` and `factory budget` clears)

1. **Ideate** (PRD §8): run the ideation cycle with the local model — K
   candidate briefs from the tuned prompt + catalog digest + active lessons;
   the concept judge scores each on the absolute rubric; ties break
   deterministically (novelty distance, then least-recently-used tag family,
   then seed priority). No winner above threshold → log and skip building
   this tick.
2. **Prototype** (PRD §9.1): the prototype-prompt slot turns the brief +
   design-direction doc + reference anchors (owned assets from
   `direction/anchors/` only) into a Gemini prompt; `factory` runs a batch of
   **5 independent generations** (metered); each image gets an individual
   verdict + reason, recorded as a `stage: prototype` decision with its
   `rater`. The reviewer follows `config.yaml`'s `prototype_review` lane:
   `judge`, `alex`, or `sampled` — the default — where every Nth batch and
   any batch the judge would kill outright goes to Alex instead. **An
   Alex-routed batch suspends the concept rather than blocking the tick**:
   `factory` persists the images under `state/prototypes/<concept-id>/`, sets
   `creative_status: awaiting_prototype_review`, drops it from the WIP count,
   notifies, and the tick moves on; a later `factory ingest` resumes it.
   Otherwise: any rejection → fold that batch's reasons into a revised prompt
   and rerun **a full fresh batch of 5**, ≤ 3 iterations (earlier approvals
   carry as alternates, not as members of the new batch). Clean batch or ≥ 1
   approval after 3 → proceed with the highest-scoring approved prototype as
   the build reference. Zero approvals → the concept dies here (cost: images,
   not a build); set `creative_status: prototype_killed` and retrospect the
   kill this tick.
3. **Build** (PRD §9.2): fresh worktree; run the `factory` driver on the
   brief + approved prototype. The driver owns the mechanics (image calls —
   metered against the caps — sprite tooling, validator, render, capture)
   and consults the local model only at its defined slots with the tuned
   prompts; self-review judges **prototype fidelity first**. The trace lands
   in `runs/<id>/`.
4. **Outcome**: gates green → submit via the API (origin marker `factory`),
   setting `execution_status: submitted`. Dead after allowed repairs → set
   `execution_status: killed` and run step 2's retrospective on the kill
   trace this same tick. A concept whose queue rejection the retrospective
   routed to build craft (layer 4) keeps its `creative_status:
   prototype_approved` and is re-queued for **one** rebuild from the same
   approved prototype once the lesson lands — no new ideation, no new
   prototype pass.

### 5. Close — `factory commit-tick`

Appends the tick summary to the audit log (decisions ingested, retros run with
applied/pending/escalated counts, builds/kills/submissions, external spend,
metric snapshot, escalation-queue depth) and **commits `state/`** — every tick
is a git diff, which is what makes the append-only ledgers tamper-evident. If
the queue is ≥ 5 items or a week old, or anything needs Alex, send one
consolidated notification — never one ping per item.

## Special invocations

- **"Status"**: phase, approved count vs target, pending, trailing approval
  rate, prototype yield (clean-batch rate, approvals per batch), per-stream
  judge agreement + error split, repeat-root-cause count, spend vs caps,
  active halts, escalation-queue depth and age. All numbers from
  `factory metrics` output, none recomputed by you.
- **"Dry-run a tick"**: the export-gate rehearsal (PRD §12.2 item 4) — allowed
  in `m1`, submissions stubbed, Alex watching; report every step's result.
- **"Pause"/"Resume"**: set/clear `paused` in `loop-state.json` (distinct from
  `halted`, which only Alex clears after a cause is addressed). Maintenance
  sessions set `paused` themselves as their first act (PRD §11.6).

## Hard rules (non-negotiable, from PRD §6.2/§11.5)

1. `audit.md`, `decisions.jsonl`, `lessons.jsonl`, `catalog.jsonl`,
   `escalations.jsonl` are append-only, and only `factory append` writes them.
   Supersede; never rewrite.
2. You never edit prompts, judges, skills, gates, or the direction doc. Those
   change only in Claude Code maintenance/calibration sessions, under their
   validation gates. Outside a build's dedicated worktree, your write surface
   is `state/` and `runs/` — and `state/` only through the CLI.
3. The factory never approves, publishes, or merges its own skins.
4. Pinned means pinned: chat, judge, and embedder configs change only in
   maintenance sessions, never as runtime substitutions.
5. Spend is enforced by the `factory` CLI at every external call; you enforce
   dispatch — never start work a cap check hasn't cleared.
6. Terminal condition: `approved_count >= target` (default 100) → set phase
   `done`, disable the cron entry, send Alex the final report.
