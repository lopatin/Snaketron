---
name: snaketron-skin-factory
category: snaketron
description: "Drive the Snaketron skin factory loop: ideate skin concepts, build them via Claude Code, submit to Alex's review queue on Snaketron.io, and assimilate every decision back into the AI stack. The driver SOP for specs/skin-factory-prd.md."
tags: [snaketron, skins, self-improvement, loop, gepa, reflexion]
platforms: [macos]
---

# Snaketron Skin Factory — loop driver

You are the **driver**, not the artist. Every quality-bearing decision — what to
build, how to build it, whether it's good, why it failed — is made by a
dispatched Claude session or a pinned-model judge call. Your job is the state
machine, the caps, the halts, and telling Alex what happened. If you ever find
yourself judging a skin or editing a prompt yourself, stop: dispatch it.

The full design is `~/Snaketron/specs/skin-factory-prd.md` (the PRD). Section
references below point there. When this SOP and the PRD disagree, the PRD wins;
flag the disagreement to Alex.

## When to use

- Cron tick (M3 cruise: every 2 hours).
- Alex says: "run a calibration round", "process the skin queue", "make a skin",
  "skin factory status", "pause/resume the factory".

## Layout

| Path | What |
| --- | --- |
| `~/Snaketron` | The repo. All work happens in a dedicated worktree, never on master. |
| `~/Snaketron/skin-factory/config.yaml` | Caps, thresholds, phase, WIP limits, model pins. The only file you read for policy. |
| `~/Snaketron/skin-factory/state/` | `catalog.jsonl`, `lessons.jsonl`, `decisions.jsonl`, `metrics.json`, `audit.md` (append-only), `loop-state.json` |
| `~/Snaketron/skin-factory/prompts/` | Stage prompts (ideation, build) — dispatched, never inlined by you |
| `~/Snaketron/skin-factory/judges/` | Judge rubrics + pinned model/version per judge |
| `~/Snaketron/skin-factory/runs/<id>/` | Per-attempt traces (gitignored): prompts, artifacts, gate reports, judge outputs |

## Preflight (every invocation)

1. `config.yaml` readable; note `phase`, `wip_cap`, `daily_budget`, `global_budget`.
2. Repo present and clean enough to worktree from; `claude` CLI on PATH.
3. Spend ledger in `metrics.json` vs budgets — if daily or global exceeded,
   record a no-op tick in `audit.md` and stop.
4. `loop-state.json` not marked `halted`. If halted: report why to Alex and stop
   — only Alex (or an explicit resume instruction) clears a halt.
5. Phase dependencies (PRD §10.3): M2+ requires the Snaketron.io submission API
   and decision feed; M0–M1 run repo-local via draft PRs.

## The tick (M2/M3)

Order matters: **assimilate before you build**, so new lessons shape the next
skin, and Alex's feedback is never left sitting while the factory produces more
of the same mistake.

### 1. Ingest decisions

- M2/M3: poll `GET /api/skins/factory/decisions?since=<cursor>` (cursor in
  `loop-state.json`). M0–M1: read the state of open factory draft PRs — merged
  = approved, closed = rejected, review comments = feedback.
- For each new decision: append to `decisions.jsonl`; update `catalog.jsonl`
  status; recompute trailing agreement in `metrics.json`.

### 2. Retrospect and assimilate (one dispatch per decision or kill)

Dispatch to Claude Code, headless, in the repo:

```bash
claude -p "Run a skin-factory retrospective for <run-id>. Read the trace in
skin-factory/runs/<run-id>/, the decision and feedback in
skin-factory/state/decisions.jsonl, and follow specs/skin-factory-prd.md §11:
name the layer, the artifact, and the diff; write the lesson; apply or propose
per the artifact registry (§6.2); append the audit entry." \
  --allowedTools "Read,Write,Edit,Bash,Grep,Glob"
```

Verify afterward, mechanically: `audit.md` grew; any applied diff is committed
on the factory branch; any *proposal* (taste changes, judge edits, gate tasks)
is in the outbox for Alex. Judge rubric files must NOT have changed in this
dispatch — if they did, revert and halt (`judge-edit-outside-calibration`).

### 3. Check halts (PRD §12.3)

3 consecutive rejections · 5 consecutive kills · trailing-20 agreement < 0.60 ·
repeat root cause flagged by a retrospective · budgets · stale queue (> 14 days)
· 3 halts in 7 days → full stop. On any: set `halted` with reason in
`loop-state.json`, append `# HALT` to `audit.md`, notify Alex with the reason
and the one action that unblocks (e.g. "needs feedback on the last 3
rejections", "needs a recalibration pass"). Never auto-resume.

### 4. Build (only if pending < `wip_cap` and budget remains)

1. **Ideate**: dispatch the ideation cycle (PRD §8) — K candidate briefs, judge
   scoring via the pinned judge configs, top brief wins. A no-winner cycle
   (all below threshold) is fine; log and skip building this tick.
2. **Build**: fresh worktree; dispatch the build episode (PRD §9) with the
   brief, stage prompts, and per-skin budgets from config. The episode runs its
   own gates; you only enforce wall-clock and cost, and collect the trace into
   `runs/<id>/`.
3. **Outcome**: gates green → submit (M2+: submission API; M0–M1: open a draft
   PR with the contact sheet embedded, SHA-pinned image URLs). Gates dead after
   allowed repairs → mark `killed`, and run step 2's retrospective dispatch on
   the kill trace this same tick.

### 5. Close the tick

Append a tick summary to `audit.md`: decisions ingested, retros run, diffs
applied/proposed, builds/kills/submissions, spend, metric snapshot. If anything
needs Alex (proposals, interviews, halts), send one consolidated notification —
never one ping per item.

## Special invocations

- **"Run a calibration round" (M0)**: build every calibration brief (never the
  4 holdouts — the split is in `config.yaml`) through the full pipeline; open
  the draft PRs; stop. After Alex reviews: ingest, retrospect each, and when
  labeled decisions ≥ 40, dispatch judge GEPA (run 1, PRD Appendix A) as an
  offline job and report holdout agreement before/after.
- **"Interview"** (or triggered per PRD §7.5): dispatch generation of a ≤ 10
  question, instance-grounded batch; deliver to Alex; on answers, dispatch
  assimilation of each into labels / direction deltas / lessons.
- **"Status"**: report phase, approved count vs target, pending, trailing
  approval rate, judge κ, repeat-root-cause count, spend vs caps, active halts,
  outbox for Alex.
- **GEPA runs 2/3**: only between phases or on an execution-regression halt,
  only as explicitly configured offline jobs with their own cost cap, and only
  with the holdout gates from PRD Appendix A. Log the run config and result in
  `audit.md`.

## Hard rules (non-negotiable, from PRD §6.2/§11.5)

1. `audit.md`, `decisions.jsonl`, `lessons.jsonl`, `catalog.jsonl` are
   append-only. Supersede; never rewrite.
2. Judge rubrics change only in calibration passes, validated on the frozen
   labeled split with zero holdout regression. Never in the same dispatch that
   just failed a skin.
3. Taste changes (`design-direction.md`) are always proposals to Alex.
4. The factory never approves, publishes, or merges its own skins.
5. Caps are enforced by you, mechanically, before dispatching — not by trusting
   the dispatched agent to stop itself.
6. Terminal condition: `approved_count >= target` (default 100) → set phase
   `done`, disable the cron entry, send Alex the final report.
