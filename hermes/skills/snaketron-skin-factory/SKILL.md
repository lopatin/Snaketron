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
| `~/Snaketron/skin-factory/state/` | `catalog.jsonl`, `lessons.jsonl`, `decisions.jsonl`, `escalations.jsonl`, `metrics.json`, `audit.md` (append-only), `loop-state.json` |
| `~/Snaketron/skin-factory/prompts/` | GEPA-tuned slot prompts (ideation, build slots). Read-only to you — edited only in maintenance sessions. |
| `~/Snaketron/skin-factory/judges/` | Judge rubrics (concept + craft) + the pinned judge config. Read-only to you. |
| `~/Snaketron/skin-factory/runs/<id>/` | Per-attempt traces (gitignored): slot inputs/outputs, gate reports, judge outputs |

## Preflight (every invocation)

1. `config.yaml` readable; `phase` is `m2` or `m3` — with exactly one
   carve-out: an explicit **"dry-run a tick"** request from Alex is allowed in
   `m1` (submissions stubbed, no state mutation beyond the audit note). Any
   other invocation outside m2/m3: dormant — report and stop.
2. LM Studio serving the pinned chat model **and** the pinned embedding model
   (config names both; a different model/quant than pinned is a halt, not a
   substitution).
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

Polls the decision feed, advances the cursor, appends decision rows, assigns
the 1-in-5 **holdout label at ingest** (PRD §7.3), and recomputes
`metrics.json` (trailing agreement, error split, approval rate) — all
deterministic code. You read its report; you do not redo its math.

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
  until a maintenance session activates it.
- **Escalate:** anything behavioral — proposed edits to slot prompts, skills,
  judge rubrics, gates; specific→general promotions; taste proposals; any
  low-confidence or unroutable retrospective. Full evidence bundle to
  `escalations.jsonl`, via `factory append`.

`factory append` refuses non-append mutations; after this step run
`factory write-guard`, which verifies nothing outside `state/`, `runs/`, and
the build worktree changed. If something did: revert it and halt
(`behavioral-edit-outside-maintenance`).

### 3. Check halts — `factory halt-check`

Evaluates PRD §12.4 mechanically: 3 consecutive rejections · 5 consecutive
kills (the CLI re-runs the deterministic holdout builds and reports only
pass/fail per brief + the bisected suspect change — never holdout traces) ·
trailing-20 agreement < 0.60 · repeat root cause · budgets · stale queue
(> 14 days) · 3 halts in 7 days → full stop. On any: set `halted` + reason in
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
2. **Build** (PRD §9): fresh worktree; run the `factory` driver on the brief.
   The driver owns the mechanics (image calls — metered against the caps —
   sprite tooling, validator, render, capture) and consults the local model
   only at its defined slots with the tuned prompts. The trace lands in
   `runs/<id>/`.
3. **Outcome**: gates green → submit via the API (origin marker `factory`).
   Dead after allowed repairs → mark `killed` and run step 2's retrospective
   on the kill trace this same tick.

### 5. Close — `factory commit-tick`

Appends the tick summary to the audit log (decisions ingested, retros run with
applied/pending/escalated counts, builds/kills/submissions, external spend,
metric snapshot, escalation-queue depth) and **commits `state/`** — every tick
is a git diff, which is what makes the append-only ledgers tamper-evident. If
the queue is ≥ 5 items or a week old, or anything needs Alex, send one
consolidated notification — never one ping per item.

## Special invocations

- **"Status"**: phase, approved count vs target, pending, trailing approval
  rate, judge agreement + error split, repeat-root-cause count, spend vs caps,
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
