# PRD: The Skin Factory — a self-improving loop that designs, builds, and ships Snaketron skins

| Field | Value |
| --- | --- |
| Status | Draft for review |
| Product | Autonomous skin generation: ideation, execution, review queue on Snaketron.io, and the feedback-assimilation system that improves every layer of the AI stack from Alex's decisions |
| Scope | A Hermes-driven loop; calibrated LLM judges; a Reflexion ideation engine with a growing catalog; a GEPA-optimized execution pipeline; a retrospective engine that root-causes every piece of feedback to the artifact that should have prevented it |
| Depends on | `specs/skins-prd.md`, `specs/skin-shading-prd.md`, `specs/first-class-skins-prd.md` (PR #84 — platform: schema v2, textures, submission + admin review), `.claude/skills/author-skin` (execution craft), the Notion Snaketron page (design direction + eval set + training plan) |
| Owners | Alex (taste, approvals) / the factory (everything else) |
| Last updated | 2026-08-20 |

## 1. Executive decision

Build the factory as an **agentic pipeline whose every stage is a separately versioned, separately evaluable artifact** — DSPy's program-of-modules discipline without DSPy as the runtime — and apply **GEPA where GEPA is actually affordable**: first to the judge rubrics (one LLM call per rollout, metric = agreement with Alex's recorded decisions), then to the prototype prompt (metric = the calibrated prototype judge, whose labels arrive five per batch), then to execution-stage prompts (metric = the deterministic gate stack that already exists, plus a frozen craft judge for what gates cannot see), and only later to ideation (metric = the calibrated judges, once they have *earned* metric status by demonstrated agreement with Alex). Everything the human gates — the whole-pipeline level where one decision costs Alex a review — runs the same reflective move GEPA makes, but as a **single-sample Reflexion retrospective**: read the trace, root-cause the failure to the artifact that should have prevented it, edit that artifact, validate the edit against held-out labeled data.

**Prototype before build.** Nothing is implemented that has not first been approved as a picture. Between ideation and implementation sits a **prototype loop** (section 9.1): the brief plus context plus reference images go to Gemini, five independent generations per prompt, each approved or rejected — by Alex during calibration, by the calibrated prototype judge in production — with feedback folded into a revised prompt, iterating until a batch comes back clean or three iterations pass; a concept with zero approved prototypes dies there, having cost images instead of a build. This split does three jobs at once: taste is calibrated where labels are cheapest (five per batch, not one per build — the label budget that constrained judge calibration eases by an order of magnitude); the expensive implementation stage only ever starts from an approved visual target; and the craft judge's question collapses from open aesthetic judgment to *does the render match the approved prototype* — a far more tractable question for a local model. Creative approval and execution approval are deliberately separate decision streams, and every recorded reason serves both: training data for the execution stage, calibration data for the judges.

**Where the intelligence lives — two homes, one workspace.** Claude is scaffolding, not a runtime dependency. It is used interactively in Claude Code, with Alex present, to build the factory, run calibration (M0–M1), and periodically maintain it. The production loop that ships skins runs **entirely in Hermes on the local model** (qwen via LM Studio), reading the same `skin-factory/` workspace in the repo. The bridge between the two is GEPA itself: run 3 optimizes the build-stage prompts *for the local task model* — reflection by Claude, execution by qwen, scored by the deterministic gates and a frozen craft judge — which is precisely the distillation step that makes a small local model viable as the production executor. "Export to Hermes" is therefore not a packaging step but a **phase flip**: when the export gate (section 12.2) passes — holdout briefs build green under the local model, a judge configuration clears the agreement threshold — `config.yaml` flips to production and Hermes takes over. Claude reappears only in maintenance sessions Alex starts (section 11.6), which drain the escalation queue and re-run GEPA when the loop's own alarms ask for it.

The direct answers to the framing questions:

**Are Training parts 4 and 5 a GEPA program?** They are GEPA's *reflective mutation step* — read traces + feedback, rewrite the instruction text — extracted from the loop. What Notion's part 4–5 lacks to be actual GEPA is a metric that can be called hundreds of times: GEPA needs forward rollouts scored automatically, and today the scorer for "is this skin good" is Alex. So parts 4 and 5 are implemented here as the **assimilation engine** (section 11): the GEPA move with batch size 1, a human μ, and an audit trail. Actual `dspy.GEPA` runs appear in four places where an automatic μ exists or can be built (section 5.4) — and the first of them, judge-rubric optimization, is the mechanism by which the human μ gets converted into an automatic one. Calibrating the judges is not a preliminary to using GEPA; it **is** the first GEPA run.

**Ax or DSPy?** DSPy (Python). `dspy.GEPA` is the reference implementation maintained by the GEPA authors; the asset tooling the metric has to call (`sprite_sheet.py`, `build_coat_textures.py`, the validator binary) is already Python-adjacent, so μ composes in-process; and the calibration math (agreement, κ, split management) wants to live next to the optimizer. Ax is a faithful TypeScript port and would work, but nothing in this system runs in TS except the web client, and the web client is not where optimization happens.

**"I have the convo history, so I have enough to auto-train the skill."** Half right, and the half matters. Conversation history is a **reflection corpus and a label mine**: it bootstraps the lessons ledger (every correction Alex has ever given a skin session), the judge calibration set (his recorded verdicts and reasons), and the retrospective engine's evidence base. It is **not a metric**: history cannot score a *new* candidate prompt, so it cannot drive an optimization loop by itself. Every improvement iteration still needs fresh rollouts through the gates and judges. History makes the loop start warm; it does not replace the loop.

## 2. Product problem and goal

Skins are Snaketron's content treadmill, and today each one costs a full human-supervised authoring session. The Notion page holds the design direction, a 17-brief eval set, and a fan-out list (countries, states, teams, sports) that implies **hundreds** of skins nobody has time to build by hand. The `author-skin` skill has crystallized the craft; the gate stack (validator, op ceiling, seam metrics, conformance suite) has crystallized the checkable half of quality; PR #84 specs the platform that makes a skin a submittable data object. What is missing is the system that connects them into a loop that runs without Alex in it — except at the single point where his judgment is the product: the approve/disapprove decision.

**Goal:** a Hermes-scheduled loop that ideates unique, chill skin concepts, builds them flawlessly, submits them to a review queue on Snaketron.io, and — this is the actual product — **assimilates every piece of feedback Alex gives into the layer of the stack that should have prevented the issue**, so that the approval rate climbs and the same mistake is never reviewed twice. Terminal condition: 100 approved skins (configurable), or a halt rule fires.

## 3. Goals and non-goals

### 3.1 Goals

1. **Judge calibration** (Notion Training steps 1–3, formalized): run the 17 eval briefs through prototype rounds and then implementation, with Alex reviewing every round; convert his verdicts and reasons into a frozen labeled decision set; optimize judge rubrics against it with GEPA until holdout agreement clears threshold.
2. **Disagreement-driven interviews**: when judges and Alex disagree in ways no recorded evidence explains, generate concrete, instance-grounded questions (never "what do you like?") whose answers become labeled data and design-direction deltas.
3. **Ideation as Reflexion + a growing catalog**: concept generation that reads the catalog of everything tried (approved, rejected, killed), scores novelty by embedding distance, applies the twist-on-popular guideline with an IP-risk gate, and learns from every rejection.
4. **Prototype before build** (section 9.1): five-sample Gemini batches per concept, judged individually with feedback, iterated to a clean batch or killed within three iterations — creative approval as its own decision stream, upstream of and distinct from execution approval.
5. **Execution flawless from an approved prototype**: the `author-skin` craft, decomposed into stages with deterministic gates between them, targeting the approved prototype as reference, with stage prompts optimized offline by GEPA against those gates.
6. **A queue on Snaketron.io** built on PR #84's admin review surface: agent-submitted skins land in Alex's queue; approve/disapprove with optional free-text feedback; the decision and feedback flow back to the loop as structured data.
7. **Feedback assimilation with root-cause routing**: every decision (and every mid-calibration correction) triggers a retrospective that names the layer, the artifact, and the diff — skill file, judge rubric, deterministic gate, ideation catalog, design-direction doc, or platform bug — and applies it under validation. Specific lessons immediately; generalization on recurrence.
8. **The loop until N=100**, phase-gated: calibration → supervised production → cruise, with WIP caps, cost caps, and halt rules throughout.
9. **Everything auditable**: append-only audit log, every self-modification a reviewable diff, git as the substrate.

### 3.2 Non-goals

- **No model fine-tuning.** All improvement is to text artifacts (prompts, rubrics, skills, gates) and code (checks). Weight-level training is not warranted below several hundred labeled decisions and probably not then; trace distillation to a cheaper builder model is a v2 option, noted in section 17.
- **No auto-publication.** The factory never approves its own skins; nothing reaches players without Alex's decision. This is both a safety rail and the source of the training signal.
- **Not a general skin platform.** Player-facing creation, textures, Boost Bux, the Builder — all PR #84's scope. The factory is one (agent) user of that platform.
- **No judge self-modification inside a production run.** Judge rubric edits happen only in calibration passes, validated against the frozen labeled set (section 11.5). A production run that disagrees with its judges records the disagreement; it does not fix it.
- **Not the eval set as product.** The 17 eval briefs are deliberately IP-heavy calibration fixtures. They train the pipeline; they are not published (section 8.4).

## 4. Baseline — what exists, what is specced, what is missing

**Exists on master (verified this branch):**

- `.claude/skills/author-skin` — 632 lines of crystallized execution craft: path selection (document vs the four Rust escalation patterns), the `[T,X,T]` inpaint-wrap and roll-and-repair texture pipelines with measured accept/reject thresholds, sprite-sheet mechanics (16px cell ceiling, y-is-time, mark scale via `cells_for`), the friend/foe `SideCue` rule, the validate → register → render → prove → ship sequence.
- The deterministic gate stack: `validate-skin` binary; `skin::perf` 200-op ceiling; `skin_schema::color::perceptual_distance`; seam/detail/chroma metrics with tuned thresholds (`TARGET_RATIO` 1.2, `STRUCTURAL_RATIO` 1.5) in `sprite_sheet.py` and `build_coat_textures.py`; the conformance suite including team-colour side checks; the classic golden traces; catalog-parity tests.
- Five document skins (`skin-schema/skins/*.skin.json`), four Rust texture/sprite skins, the `/qa/skins` harness, `capture-skin-sheet.mjs`.
- Hermes at `~/.hermes`: skills tree (`~/.hermes/skills/<category>/<name>/SKILL.md`), cron, a local default model (qwen3.6-35b via LM Studio) — an always-on scheduler, not a heavyweight reasoner.
- Claude Code conversation history under `~/.claude/projects/-Users-alex-Snaketron*/` (JSONL transcripts) plus distilled memory files — the bootstrap corpus.
- The Notion Snaketron page: design guidelines, 17 eval briefs, fan-out lists, the training plan this PRD implements.

**Specced but not landed (PR #84, open):** SkinDoc schema v2 (patterns/images/text as document vocabulary), the Texture entity and S3 ladder, skin submission APIs, the admin review gate with publication/`pending_revision` as separate dimensions, content addressing. The factory's production phases depend on a subset of this (section 10.3); its calibration phase does not (section 12.1).

**Missing entirely (this PRD's scope):** the judges and their calibration machinery; the ideation engine and catalog; the prototype loop and its prompt slot; the stage decomposition of execution with GEPA harnesses; review-decision feedback capture and the decision feed; the assimilation engine; the Hermes loop skill; the factory workspace.

## 5. The training question, answered precisely

### 5.1 What GEPA is and what it needs

GEPA (Agrawal et al., arXiv:2507.19457) evolves the instruction text of a program's modules: run the program on training instances, collect execution traces, have an LLM *reflect* on traces plus textual feedback (μf) to propose a rewritten instruction for one module, keep candidates on a Pareto frontier over instances, select by aggregate score (μ). Its sample efficiency (up to 35× fewer rollouts than RL post-training, and better results) comes from the reflection being in natural language over rich traces rather than a scalar gradient. It still needs three things:

1. **A program with distinct text components** whose text is the genome.
2. **A metric μ callable per rollout**, plus textual feedback μf per rollout.
3. **Training instances** to roll out on, with some held out.

### 5.2 Where the factory satisfies those requirements — and where it doesn't

| Stage | Rollout cost | μ available? | Verdict |
| --- | --- | --- | --- |
| Judge scoring (rubric text → verdict on one example) | 1 LLM call, near-free locally (~$0.01 only under the API fallback), seconds | Yes, from day one: agreement with Alex's recorded decision on that example | **True GEPA, immediately.** The cheapest rollouts and the cleanest metric in the whole system. |
| Prototype prompt (brief → Gemini prompt → 5 images) | 5 image calls, ~$0.20–0.75, a minute | During calibration: Alex's per-image verdicts. After: the calibrated prototype judge | **True GEPA, second-cheapest rollouts** — and the batch structure mints 5 labels per rollout, so μ and its own calibration data grow together |
| Execution stages (prototype → doc/assets → render) | Driver-run pipeline with LLM creative slots, 10–30 min; near-free once the task LM is local | Yes: the deterministic gate stack (validator errors, op counts, seam/detail/chroma ratios, conformance results) — dense, textual, hard to game — plus prototype-fidelity judging | **True GEPA, with the local model as task LM.** ~100 rollouts ≈ a weekend of local compute plus Claude reflection calls. Run offline in maintenance sessions, not inline. |
| Ideation (seed → brief) | 1–3 LLM calls, cheap | Only after calibration: the judges are μ *only once they demonstrably agree with Alex* (the 7.4 holdout agreement gate) | **True GEPA, gated on judge calibration.** Before that, optimizing ideation against uncalibrated judges optimizes toward a rubric nobody validated. |
| Whole pipeline (seed → approved skin) | Build + **one Alex review** | No. μ is a human; ~17 calibration instances; each rollout costs the scarcest resource | **Not GEPA.** Reflexion retrospective per decision (section 11) — the same reflective move, batch size 1, audited. |

This ordering is the answer to "calibrate (and/or train?) the judges": *calibrate*, meaning optimize their rubric text against Alex's frozen decisions — which is training in the GEPA sense and requires no weights. Fine-tuning a judge model is unjustified below several hundred labels and would freeze taste into weights that can't be diffed or audited.

### 5.3 Inputs and outputs, concretely

If Alex runs the DSPy program himself, these are the signatures. Each row is a DSPy module (or an agent episode wrapped to look like one); the *genome* column is what GEPA rewrites.

**Judge (concept or craft):**
- **Input x:** artifact under judgment (brief text, or contact-sheet images + doc summary), design-direction doc, the judge's rubric.
- **Output y:** structured sub-scores 1–5 (the rubric's dimensions), verdict ∈ {approve, reject}, critique text.
- **μ:** verdict agreement with Alex's decision on labeled example x (train split); report agreement and the false-approve/false-reject split on holdout.
- **μf:** where they disagreed, Alex's recorded reason vs. the judge's critique.
- **Genome:** the judge's rubric text (GEPA may rewrite individual sub-sections).

**Ideation:**
- **Input x:** seed (fan-out entry, trend, or open), design-direction doc, catalog digest (recent + nearest concepts with outcomes), active ideation lessons.
- **Output y:** brief = `{name, description}` in the eval-set register (2–6 sentences naming structure: head/body/tail treatment, pattern vs sprite, palette intent, motion intent).
- **μ (post-calibration):** calibrated concept-judge score + novelty distance (section 8.2).
- **μf:** judge critiques + nearest-neighbor collision report.
- **Genome:** the ideation prompt and the brief-writing rubric.

**Prototype (the batch loop, treated as one module):**
- **Input x:** brief, design-direction doc, reference images (style anchors from the catalog and guidelines), prior-iteration verdicts + feedback if iterating.
- **Output y:** a Gemini prompt → a batch of 5 independent generations, each carrying its verdict and reason.
- **μ:** per-image approval (Alex during calibration; calibrated prototype judge after), with the clean-batch rate as the convergence signal.
- **μf:** the per-image rejection reasons — which is exactly what the next iteration's prompt revision consumes, so the production loop and the GEPA loop share one feedback format.
- **Genome:** the prototype-prompt slot (`skin-factory/prompts/prototype/`) — the instructions that turn a brief into a Gemini prompt.

**Build (the driver-run pipeline, treated as one module):**
- **Input x:** brief, **the approved prototype image** (the visual target), design-direction doc, active execution lessons, platform constraints digest.
- **Output y:** skin bundle = SkinDoc (+ texture PNGs if sprite) + contact sheet + gate report.
- **μ:** weighted gate pass rate: validator clean, op count ≤ 200, seam ≤ target, detail/chroma in band, conformance green, goldens untouched, palette distances ≥ 0.10 — plus craft-judge score on the render.
- **μf:** the gate stack's own error text — validator messages name the field and the rule; seam reports carry the numbers; conformance failures name the invariant. This is unusually rich μf and is why GEPA should work well here.
- **Genome:** the stage instructions distilled from `author-skin` (section 9.3) — not the skill file itself; the skill file remains the human-readable source that the stage instructions are compiled from.

### 5.4 The four sanctioned GEPA runs

1. **Judge rubrics** (during and after calibration, repeatable): μ = train-split agreement; promotion gate = holdout agreement did not regress. First run as soon as ≥ 40 labeled decisions exist — which the prototype rounds reach fast, at 5 labels per batch.
2. **Prototype prompt** (late M0a, once the prototype judge is calibrated; repeatable): μ = calibrated prototype judge over fresh batches; instances = the 13 calibration briefs; holdout = the 4 holdout briefs. Task LM = local qwen (it writes the Gemini prompts in production).
3. **Execution stage prompts** (between phases M1→M2 and on execution-regression halts): **task LM = the local production model, reflection LM = Claude** — this run is the export step's engine (section 12.2), the point where Claude's craft is distilled into prompts the local model executes well. μ = deterministic gates + frozen craft judge (prototype fidelity + conformance); instances = the 13 calibration briefs with their approved prototypes; holdout = the 4 holdout briefs (section 7.3) which GEPA never sees.
4. **Ideation prompt** (M2+, once the 7.4 holdout agreement gate has held for two consecutive audits): μ = calibrated concept judge + novelty; holdout = a reserved seed list.

### 5.5 Decision records: two streams, two raters

Every decision row carries **`stage`** (`prototype` | `implementation` | `queue`) and **`rater`** (`alex` | `judge:<pinned-config-id>`). Both fields are load-bearing, for different reasons.

`stage` keeps the streams unpooled: a prototype label says "this look is right", an implementation label says "this build honored the look", and a judge calibrated on the mixture learns neither.

`rater` prevents **self-training**, which is otherwise a live hazard rather than a hypothetical one: in production the prototype judge writes its own verdicts into the same `stage: prototype` stream that calibration draws from, so a later calibration pass would train the judge on its own outputs and confidently amplify whatever it already gets wrong. The rule closing that loop, applied everywhere the words appear:

> **A "labeled decision" means `rater == alex`.** Only Alex-rated rows count as calibration data, holdout labels, or agreement-metric inputs (7.4, GEPA run 1). Judge-authored rows are Reflexion inputs, loop control, and audit trail — never a training or evaluation label for any judge.

Each run is an offline batch job with an explicit cost cap, its config and result committed to the audit log. GEPA runs happen inside Claude Code calibration or maintenance sessions, never inside the Hermes loop, and no GEPA run's μ includes a judge whose rubric changed since its last validation.

## 6. System architecture

```
                     ┌────────────────────────────────────────────────┐
                     │  DESIGN DIRECTION  (repo: skin-factory/)       │
                     │  design-direction.md ← synced from Notion      │
                     │  lessons ledger · catalog · labeled decisions  │
                     └────┬────────────────────────▲──────────────────┘
                          │ read by everything     │ written only by
                          ▼                        │ assimilation (audited)
  ┌──────────┐ brief ┌───────────┐ approved  ┌─────┴────┐ gates ┌───────────┐ submit ┌─────────────┐
  │ IDEATION │──────▶│ PROTOTYPE │──────────▶│  BUILD   │──────▶│SELF-REVIEW│───────▶│ QUEUE       │
  │ Reflexion│       │ 5× Gemini │ prototype │ driver + │       │ prototype │        │ Snaketron.io│
  │ +catalog │◀──────│ judge/Alex│           │ slots    │◀─────▶│ fidelity  │        │ Alex decides│
  └────▲─────┘ kill  │ ≤3 iters  │           └──────────┘ repair└───────────┘        └──────┬──────┘
       │             └─────┬─────┘                                                          │ verdict
       │                   │ per-image verdicts + reasons (creative stream)                 │ + feedback
       │                   ▼                                                                │
       │                  ┌──────────────────────────────────────────────┐                  │
       └──────────────────│  ASSIMILATION  (retrospective, root-cause,   │◀─────────────────┘
             lessons      │  route to artifact, validate, apply, audit)  │   execution stream
                          └──────────────────────────────────────────────┘
          both streams feed assimilation; they are never pooled for judge calibration

   Orchestration: in production (M2+) everything above runs in Hermes on the local
   model over deterministic rails; Claude appears only in the calibration phase and
   in maintenance sessions Alex starts in Claude Code.
```

### 6.1 Who runs what — two homes, one workspace

Design constraint, stated once: **no Claude and no API LLM in the production loop.** The two exceptions are data-plane, not intelligence: image generation (no local image model exists here), and judges *only if* calibration proves the local model cannot clear the agreement threshold (the fallback is a recorded export-gate decision, not a default).

| Activity | Runtime | Model | When |
| --- | --- | --- | --- |
| Factory construction; calibration builds; M0–M1 retrospectives; GEPA reflection; maintenance sessions | Claude Code, interactive (sessions Alex starts — like this one) | Claude | M0–M1, then periodic maintenance (11.6) |
| Loop driver (sequencing `factory` subcommands, retrospective routing, notification prose — no arithmetic, no direct ledger writes) | Hermes cron skill | Local qwen | M2+ |
| Ideation, prototype-loop orchestration (writing Gemini prompts, folding rejection feedback into revisions), build creative slots, self-review orchestration, low-risk retrospectives (11.5) | Hermes-dispatched local sessions with GEPA-optimized prompts | Local qwen | M2+ |
| Mechanical rails: build stages (asset tooling, validation, render, capture, packaging) **and** tick bookkeeping (`factory ingest\|metrics\|halt-check\|append\|budget\|commit-tick` — polling and cursors, ledger appends, agreement math, halt evaluation, spend metering with hard refusal of over-budget external calls, the per-tick state commit) | The `factory` CLI driver — deterministic Python/Rust, no LLM at all | — | Always |
| Judges | Local qwen first — verified feasible: the pinned local model serves a vision projector (probed against the running LM Studio server), so craft judging of contact-sheet images stays local. API model only as a recorded export-gate fallback; either way **pinned** (model + version/quant) because a silent change invalidates calibration | Calibrated per-model (7.4) | Always |
| Image generation — prototypes (9.1) and production textures | Gemini API — `gemini-3.1-flash-image` (Nano Banana 2) for prototype batches and iteration, `gemini-3-pro-image` for final assets; never 2.5 | — | Always (the one unavoidable external call) |
| GEPA runs | Offline DSPy jobs inside Claude Code sessions | Reflection LM: Claude; task LM: whatever will run in production (5.4) | Calibration + maintenance only |

Why a 3B-active local model can hold the build: the pipeline is **deterministic rails with narrow creative slots**. The `factory` driver does everything mechanical — invoking the image API, `sprite_sheet.py`, the validator, the render harness — and the LLM is consulted only at defined decision points (structure plan, image prompts, document field values, repair choices), each with a GEPA-tuned prompt optimized *for that model* against the gates. The gates then verify everything the model produced. A small model inside verified rails is a different proposition from a small model running free.

### 6.2 The artifact registry — every mutable thing, who may change it, what validates the change

This table is the heart of the self-improvement design. **Only the target artifact of a given improvement process evolves; everything else in that process is a ratchet.**

| Artifact | Lives at | Mutated by | Validation gate |
| --- | --- | --- | --- |
| `design-direction.md` (taste spec) | `skin-factory/direction/` | Alex directly; assimilation may **propose** (PR-style diff surfaced to Alex) | Alex's sign-off, always — taste changes are never auto-applied |
| Judge rubrics | `skin-factory/judges/*.md` | GEPA calibration runs; assimilation (calibration passes only) | Frozen labeled set: train-split improvement AND no holdout regression |
| Ideation prompt + brief rubric | `skin-factory/prompts/ideation/` | GEPA (run 4); assimilation lessons — maintenance sessions only | Concept-judge score on reserved seeds must not regress |
| Prototype-prompt slot | `skin-factory/prompts/prototype/` | GEPA (run 2); assimilation lessons — maintenance sessions only | Prototype-judge approval on the 4 holdout briefs (un-iterated batches — 7.3) must not regress |
| Execution stage prompts | `skin-factory/prompts/build/` | GEPA (run 3); assimilation lessons — maintenance sessions only | Deterministic gates on the 4 holdout briefs must not regress, executed by the production task LM |
| `author-skin` skill + checklists | `.claude/skills/author-skin/` | Maintenance sessions, as ordinary reviewed commits | Repo review; conformance + goldens stay green |
| Deterministic gates (new checks) | validator / test suites / tools | Assimilation → **spawned as repo tasks with tests** | Normal CI; a gate change that fails existing skins needs explicit adjudication |
| Lessons ledger, catalog, decisions, escalation queue | `skin-factory/state/` | Append-only by their owning stages | Schema check; never rewritten, only superseded |
| Audit log | `skin-factory/state/audit.md` | Append-only, every mutation above logs here | Never edited |
| This PRD, the Hermes skill | `specs/`, `hermes/skills/` | Humans (and proposals) | Repo review |

Hard rules inherited from the self-improvement literature, non-negotiable here: judges and the processes they evaluate are edited in separate passes (an assimilation triggered by a failing skin may never touch a judge to make that skin pass); every self-modification is a diff in git plus an audit-log row; holdout data gates every promotion; cost caps are enforced by the driver, not by convention.

## 7. Judges and calibration

### 7.1 The judges

**Three calibrated judges — one per artifact the pipeline produces** (brief, prototype image, implemented render) — not a panel per taste dimension. Separately-tuned rubrics per dimension, each calibrated against Alex's single approve/reject label, would be over-parameterized: a narrow judge (say, IP risk) can only "agree with Alex" by learning to predict overall approval. Instead, each judge is one rubric with **structured sub-scores**, one calibrated verdict, and one agreement number — GEPA can still rewrite individual sub-sections, and Alex's recorded reason (not the sub-score split) is the real μf. The label budget divides naturally by artifact too, and unevenly in the right way: the prototype judge is where labels accumulate fastest (five per batch iteration versus one per built skin), which is exactly where taste — the hardest thing to calibrate — lives.

**Concept judge** (pre-build, on briefs — cheap, run on every candidate). Sub-scores:

| Sub-score | Question | Notes |
| --- | --- | --- |
| Uniqueness | Meaningfully distinct from everything in the catalog? | Receives the nearest-neighbor report (8.2); scores the *twist*, not just distance |
| Direction fit | "Chill" and confidently in one artistic direction, per the guidelines? | The operational definition of *chill* is seeded in 7.5 and owned by calibration |
| Feasibility | Can the pipeline express it well? Pattern vs sprite; repeat length; reads at 16px cells; head/tail respects the 1.5-cell light/dark rule; op budget plausible | Encodes the craft constraints so bad briefs die before costing a build |

Plus an **IP-risk flag** — deliberately *uncalibrated*: protected mark/likeness vs the style-not-mark transform (the Notion Hello Kitty entry is the worked example; celebrity fan-out requires likeness caution). It is not scored against Alex's verdicts; anything flagged goes to the queue with the flag visible (8.4), so Alex's decision calibrates the *policy*, not the flag.

**Prototype judge** (on each Gemini generation in a prototype batch — this judge reads images, which the pinned local model can do, per 6.1). Sub-scores: taste (unique-and-chill per the direction doc — this is the judge that carries Alex's aesthetic), guideline compatibility (would this survive being worn by a thin, growing, 16px-cell body? head/tail structure plausible; the 1.5-cell light/dark head rule reachable), and buildability (expressible by the pipeline: pattern vs sprite, repeat length, mark scale). One verdict per image, batch verdicts independent of each other. It also carries **its own uncalibrated IP-risk flag**, mirroring the concept judge's: a clean brief can still produce an image that renders a protected mark, so the flag has to exist where the pixels are, not only where the words were.

**Craft judge** (post-build, on the contact sheet + doc summary + **the approved prototype**). Sub-scores: **prototype fidelity** (does the render deliver the approved picture's look — palette, marks, motion intent? — the anchor that makes this judge's job comparison, not open taste), guideline conformance (head light/dark rule with correct core color; no seams; team sides legible; readable at cell sizes 5–15; no mush), and overall quality (would this embarrass us on the Skins page?). Taste largely settled at the prototype gate; this judge checks that implementation honored it.

Protocol for all three: pinned model and version; structured output (sub-scores 1–5, verdict, critique — the critique is μf); blind to which pipeline version produced the input; absolute rubric scoring only (no pairwise mode — see 8.1).

### 7.2 Calibration = Notion Training steps 1–3, instrumented — in two sub-phases

**M0a — prototype calibration** (Notion step 1, literally: run Gemini on each eval description with the guidelines as context). Each round: every non-holdout eval brief gets a prototype batch — 5 independent generations — and Alex plays the judge: verdict + what-to-fix-and-why per image, feedback folded into the next iteration's prompt, batch loop per 9.1. Every verdict lands in `decisions.jsonl` (`stage: prototype`), every reason spawns a retrospective, and the prototype judge is scored against Alex's verdicts before its rubric is GEPA-tuned. This is where most taste labels are minted — 65 per clean round across 13 briefs — and it is cheap and fast enough to run several rounds in an afternoon.

**M0b — implementation calibration.** Only once a brief has an approved prototype does it get built. Each round: implement the approved prototypes through the pipeline → Alex reviews the *renders*, giving verdict + reasons — now about execution: fidelity to the prototype he approved, technical quality, in-game readability. These land as `stage: implementation` decisions, calibrate the craft judge, and are the execution agent's training data. The two streams are deliberately never pooled: a prototype label says "this look is right", an implementation label says "this build honored the look", and a judge calibrated on the mixture learns neither.

Iterate until the eval skins are done (Alex's bar), which typically coincides with the judges having enough labels to be tunable.

### 7.3 The split

Of the 17 eval briefs: **13 calibration** (judges and GEPA see them), **4 holdout** (never used to tune anything; used only to gate promotions). Suggested holdouts, chosen to cover the structural space: one pure-sprite (surfing), one pattern (shark), one head/tail-anchored (wizard wand), one full-scene (christmas). Alex may swap the assignment before round 1; after round 1 the split is frozen. Production-phase decisions (section 10) continuously grow the labeled set, with **every 5th Alex-rated decision assigned to holdout — per stream, by an independent counter, at the moment the row is appended** (`factory append` is the assignment point; `factory ingest` only ever sees queue rows, so assignment cannot live there), and always before any retrospective runs. Holdout-assigned decisions are *record-only*: decision row and catalog status, no lesson, no exemplar, no escalation content beyond a count. A holdout that feeds lessons or rubric edits isn't held out; this is the line that keeps rail 2 real. The 4 holdout briefs are likewise excluded from M0a's iterate-with-feedback loop — feedback iteration *is* tuning — and get only **one-shot prototype batches** whose labels are recorded for gate measurement, never for rubric or prompt improvement.

But a holdout brief still needs an approved prototype, because the export gate requires it to *build* (12.2 item 3) and a build starts from a picture. Resolving that without leaking tuning signal:

- **Re-rolling the unchanged prompt is sampling, not tuning** — no feedback is folded in, so nothing about the holdout informs any artifact. Up to 3 such batches are allowed for a holdout brief.
- The **highest-scoring Alex-approved image becomes that brief's frozen build reference**, recorded once and reused by every later holdout build (including the halt-time bisect), so the export gate and GEPA run 3 measure the same fixture every time.
- If three sampled batches still yield zero approvals, Alex either hand-designates a reference image — logged as a *holdout accommodation* in the audit trail, since it is a human touch on a holdout — or swaps that brief out for a fresh one before the split re-freezes. A holdout the pipeline cannot even prototype is evidence about the prompt, and it goes to the escalation queue as such.

The halt-time holdout bisect likewise reports only pass/fail per brief plus the suspect change, never the holdout build traces.

### 7.4 The standing metric

Judge–Alex agreement on the trailing 20 decisions and on holdout, recomputed at every decision (by `factory metrics`, deterministically) and reported in the audit log — as **raw agreement plus the error split**, false-approves and false-rejects counted separately, because the two directions cost differently: a false-approve pollutes the queue with Alex's time, a false-reject silently narrows the factory's taste. The **agreement gate** is: holdout raw agreement ≥ 80% at n ≥ 10, with at most one false-approve in the window (thresholds in `config.yaml`). It licenses: using judges as GEPA μ for ideation, and export-gate item 2 (12.2). Cohen's κ is computed and logged as a *diagnostic only* — at these sample sizes and skewed approval rates it is unstable (prevalence paradox; undefined when either rater is constant in the window), so it informs maintenance sessions but gates nothing. Trailing-20 agreement < 60% at any point pauses submissions and requests a maintenance session (recalibration).

**Keeping the taste judge's alarm alive.** Agreement is only computable over co-labeled pairs (`rater == alex`, per 5.5), and after export the prototype judge — the one carrying taste — would otherwise never be co-labeled again: it decides alone, and the concepts it rejects die without Alex ever seeing them, so its false-rejects are structurally invisible while its false-approves surface only much later as queue rejections. Three provisions close that:

1. **A standing sampling quota.** Every Nth prototype batch (default 1 in 6, in `config.yaml`) routes to Alex through the `prototype_review: alex` lane regardless of the judge's opinion — including, preferentially, batches the judge would kill outright. This is the only channel through which a false-reject can ever be observed, and it keeps the prototype-stream agreement metric fed.
2. **Honest reporting when it isn't.** Per-stream agreement reports `n/a — stale since <date>` when co-labeled pairs are too few or too old, rather than silently carrying a number from calibration. A stale taste metric is a maintenance trigger.
3. **A prototype-stage halt rule** (12.4), since the < 60% rule cannot fire on a stream with no fresh labels: 3 consecutive zero-approval prototype kills, or a trailing-10 clean-batch rate under the configured floor, pauses the line.

### 7.5 The interview (feature 2)

Interviews are generated, not scheduled — triggered when (a) after a calibration round, judge–Alex disagreements cluster in a way no rubric edit derived from recorded reasons resolves; (b) a proposed generalization would alter `design-direction.md` (taste-level change → needs Alex's word, not the agent's inference); (c) a halt rule fires for repeated rejection without feedback.

Format rules: batched, ≤ 10 questions; every question grounded in a concrete instance ("here are two renders of the dragon head, A and B — which reads better at 15px and why", with images attached), multiple-choice plus optional free text; never open-ended taste surveys. Answers are triple-booked: labeled decisions (if verdict-shaped), direction deltas (if taste-shaped), and lessons (always). Seed definitions the first interview should sharpen: *chill* ≈ one confident aesthetic, low visual noise at arena cell sizes, slow smooth motion, no strobing, palette restraint; *unique* ≈ no near-neighbor in the catalog AND carries a recognizable twist on something popular. These seeds are hypotheses, deliberately falsifiable by calibration.

## 8. Ideation engine (feature 3): Reflexion + growing catalog

### 8.1 Generate → judge → select

Per cycle: draw a seed (weighted: fan-out backlog, trending themes, open creativity — weights tunable), generate K=5 candidate briefs with the ideation prompt + catalog digest + active ideation lessons; the concept judge scores each on the absolute rubric; ties break **deterministically** — higher novelty distance, then least-recently-used tag family, then seed priority. (No pairwise tournament: at K=5 a tie is one or two comparisons, order-bias controls on batches that small fire spuriously, and the cost of an arbitrary tie-break is at most one budget-capped attempt that Alex's queue filters anyway.) Top brief above threshold proceeds to build; the rest land in the catalog as `not_selected` with their critiques (Reflexion memory — next cycle's generator reads why its siblings died).

### 8.2 The catalog

`skin-factory/state/catalog.jsonl`, append-only: `{id, name, description, tags, seed, creative_status, execution_status, prototype_ref, embedding_key, judge_scores, feedback_digest, date}`.

**Status is two fields, not one**, mirroring the two decision streams — `creative_status` (`proposed` | `not_selected` | `awaiting_prototype_review` | `prototype_approved` | `prototype_killed`) set at the prototype gate, and `execution_status` (`unbuilt` | `built` | `submitted` | `approved` | `rejected` | `killed` | `shipped`) set by build and queue outcomes. A single field cannot express the state this design most needs to reason about — *creative approved, execution rejected* — which is precisely the concept that should be rebuilt rather than re-imagined.

`prototype_ref` points at the approved prototype and its alternates under `skin-factory/state/prototypes/<concept-id>/` — **committed, not gitignored**. The approved prototype is the build target, the craft judge's fidelity anchor, a validated taste datapoint, and a candidate future style anchor; leaving it in a disposable run directory would throw away the most reusable artifact the loop produces. Embeddings stored beside it, produced by a **pinned local embedding model** served by LM Studio and named (model + version) in `config.yaml` alongside the judge config — changing it is a maintenance decision that re-embeds the whole catalog, because distances across embedder versions are not comparable. Novelty check = min cosine distance to all prior entries plus tag-collision report, fed to the Uniqueness sub-score (the judge interprets the distance — a low distance with a genuinely new twist can pass; a high distance that's still "another animal print" can fail). Bootstrap: the 19 shipped catalog skins, the 17 eval briefs, and concepts mined from conversation history all enter on day one, so the factory never re-pitches what exists.

### 8.3 Reflexion

Every terminal outcome writes a lesson candidate back into ideation's view — but **only rejections the retrospective routed to a concept-level layer** (1, 2, 3, or a taste-flavored 3a) become ideation negative exemplars. A skin rejected for execution reasons says nothing about the idea, and teaching ideation to avoid that concept would be the wrong lesson at the wrong altitude. Routed rejections with feedback ("too busy", "another neon") become negative exemplars in the catalog digest; approvals become positive anchors. The assimilation engine (section 11) decides whether a lesson stays specific (an exemplar line) or generalizes (an ideation-prompt edit) — ideation itself never edits its own prompt.

### 8.4 IP guardrails

The eval set is IP-heavy by design and **never ships** (calibration fixtures only). For production skins the IP-risk flag (7.1) enforces the style-not-mark transform; the celebrity fan-out line additionally requires no likeness, no name — themes *around* a persona, not the persona. Anything flagged as borderline goes to the queue with the flag visible, so Alex's decision calibrates the policy.

## 9. From brief to skin: prototype, then implement

### 9.1 The prototype loop (creative approval)

No brief is implemented until it has been approved as a picture. The loop, per concept:

1. **Prompt assembly**: the prototype-prompt slot (local model, GEPA-tuned) turns the brief + design-direction doc + **reference anchors** into a Gemini prompt. Anchors are drawn only from `skin-factory/direction/anchors/` — a curated, versioned set of assets we own: approved production prototypes and shipped-skin contact sheets. Never from `calibration_only` catalog entries (the eval set is deliberately IP-heavy, and conditioning a production image on it launders that risk into the pipeline), and never from third-party imagery. Anchor set changes are maintenance decisions, recorded like any other pinned artifact.
2. **Batch**: 5 independent Gemini generations of that prompt — independent calls, not one call asked for variations, because independence is what buys diversity. (*The measured exception:* during M0a, each round also runs a second arm — one Gemini call asked to produce all 5 at once — and both arms are judged blind, side by side. Diversity, approval rate, and cost per approved prototype are recorded in the audit log, and production uses whichever arm wins. This is Alex's requested experiment, run once properly instead of wondered about forever. **Arm B is measurement only:** its images are recorded `arm: single-call`, and its verdicts never count toward the batch's clean/partial state and are never folded into a prompt revision — otherwise arm B's failures would drive the revisions arm A is measured under, and the comparison would be confounded.)
3. **Verdicts**: each image is approved or rejected *individually*, with a reason — Alex during calibration, the prototype judge in production. Every verdict is a `stage: prototype` row in `decisions.jsonl`, carrying its `rater` (5.5).
4. **Iterate**: any rejection folds the batch's reasons into a revised prompt and reruns **a full fresh batch of 5**, up to **3 iterations**. The convergence target is a **clean batch** — zero rejections among *that iteration's own five* — because a clean batch is evidence the *prompt* is dialed, not that one image got lucky; earlier approvals carry forward as alternates but are not members of the new batch and never flatter its rate.
5. **Terminal states**: *clean* (a batch with no rejections) or *partial* (three iterations passed with at least one approval) → proceed, with the highest-scoring approved prototype as the implementation reference and the other approvals archived as alternates. *Zero approvals after 3 iterations* → the concept dies here, for the price of ≤ 15 images instead of a build, and the kill runs a retrospective (was it the brief? the prompt? the concept?).

In production the reviewer is the prototype judge, but `config.yaml` has a `prototype_review: judge | alex | sampled` lane: flipping it routes prototype batches to Alex — reviewing five images is the cheapest way he can steer taste between maintenance sessions, and every verdict he casts is fresh calibration data. `sampled` is the default and implements the 7.4 quota (every Nth batch, plus batches the judge would kill).

**How a headless tick hands Alex five images.** The loop cannot block on a human, so an Alex-routed batch is a *suspension*, not a wait: `factory` writes the batch to `skin-factory/state/prototypes/<concept-id>/<iteration>/` (durable and committed, not the gitignored run dir), the concept's `creative_status` becomes `awaiting_prototype_review`, it is **excluded from the WIP cap** (it is consuming Alex's attention, not the factory's), one notification goes out, and the tick moves on. The platform slice (10.2) carries these batches on the same review surface and the same decision feed, tagged `stage: prototype` — per-image verdict plus reason, exactly like a skin verdict. A later tick's `factory ingest` picks the verdicts up and resumes that concept's loop where it left off. A batch unreviewed for 14 days counts toward the stale-queue halt.

### 9.2 Implementation stages and gates (execution approval)

Implementation is run by the **`factory` CLI driver** — a deterministic script that owns the mechanical work and consults the LLM only at defined creative slots (structure decisions, image prompts, document field values, repair choices). In M0b the LLM behind the slots is Claude working interactively; from the M1 export-gate builds onward it is the local model with GEPA-tuned slot prompts. Same driver, same gates, different model — which is what makes "does the local model clear the holdouts" a runnable experiment rather than a judgment call. Every inter-stage gate is deterministic and its failure text is preserved (it is μf for GEPA and evidence for retrospectives).

| Stage | Output | Gate |
| --- | --- | --- |
| 1. Structure plan | pattern vs sprite; layer plan; repeat length; palette — **read off the approved prototype**, which has already answered most of these questions visually | Feasibility rules (op-budget arithmetic *before* building; 200-op ceiling, tile-count math from `author-skin`) |
| 2. Asset generation (sprite path) | source images via Nano Banana 2, **conditioned on the approved prototype as reference image** → sprite sheets / coats | `sprite_sheet.py` / `build_coat_textures.py` full gate stack: seam ≤ 1.2 target, detail/chroma bands, structural checks, mark-scale `cells_for`; bounded regeneration with prompt feedback on rejection |
| 3. Document authoring | SkinDoc (v2 when landed; v1 + Rust-pattern interim, section 10.3) | `validate-skin` clean |
| 4. Register + render | contact sheet via `capture-skin-sheet.mjs`, `/qa/skins` captures incl. small cell sizes and short-snake pose | Screenshots exist and are non-classic (stale-WASM trap check from `author-skin` §4) |
| 5. Self-review + repair | craft-judge verdicts — **prototype fidelity first** (render vs the approved picture), then conformance and quality; targeted fixes | ≤ 3 repair iterations; then kill (a kill is an assimilation input, not a silent retry) |
| 6. Prove | conformance suite, goldens, catalog parity, op measurement | All green; goldens untouched |
| 7. Package | bundle: doc + assets + screenshots + gate report + brief | Schema check; submitted or (interim) PR opened |

### 9.3 The genome vs the skill

`author-skin/SKILL.md` stays the canonical, human/Claude-side craft document, used during calibration and maintenance. The production artifacts are `skin-factory/prompts/prototype/` and `skin-factory/prompts/build/` — the slot prompts GEPA optimizes and Hermes executes. The two are kept in sync by a maintenance-session checklist item (a skill edit prompts a review of the affected slot prompts, and vice versa), not by machinery: with all edits to either happening inside maintenance sessions (6.2), a freshness-hash system would be automation guarding against a process that no longer exists.

### 9.4 Budgets

Per-concept: **≤ 15 prototype images in 3 batch calls** (5 × 3 iterations); during M0a each round additionally runs one 5-image comparison call, which is measurement and is budgeted separately. Then per-build ≤ $3 external spend (textures, plus judges only under the API fallback), ≤ 45 min wall clock, ≤ 3 self-review repairs, ≤ 2 asset regenerations per texture. Local-model tokens are uncapped in dollars but bounded by the wall clock. Exceeding any budget kills the attempt and files the trace for retrospective. (A kill on budget is *evidence about the pipeline*, not about the concept — the retrospective decides which.)

## 10. Publication and the queue (Snaketron.io)

### 10.1 Reuse PR #84, don't parallel it

The queue **is** the first-class-skins admin review surface. The factory runs as a dedicated agent user (`factory` account, admin-flagged as agent, never admin): creates skins, uploads textures, submits revisions for publication review. Alex's existing `/admin` review queue shows them. Approve = publish (or approve-private, Alex's choice per skin); disapprove = reject the pending revision.

### 10.2 The deltas this PRD adds to the platform

1. **`review_feedback: Option<String>` on the review decision** — the optional free-text Alex attaches when approving or (especially) rejecting. Persisted on the decision record, immutable.
2. **A decision feed the loop can poll**: `GET /api/skins/factory/decisions?since=<cursor>` returning `{stage, rater, subject_ref, verdict, feedback, decided_at}` — `stage: queue` rows for submitted skins, and `stage: prototype` rows for Alex-routed prototype batches (9.1), one row per image. (Polling, not webhooks — the loop already wakes on cron.)
2a. **A prototype review surface**: the Alex lane needs somewhere to show five images and take a verdict + reason on each. Small, and it reuses the queue's review chrome; without it the `sampled` lane — the taste judge's only drift alarm (7.4) — has no delivery mechanism.
3. **Structured feedback capture in the review UI**: verdict buttons + free-text box + optional quick-tags (`concept`, `craft`, `too-similar`, `ip`, `direction`) that pre-route the retrospective. Tags are hints, not the router — the retrospective can overrule them.
4. **A `factory` origin marker on submissions**, so the queue can filter and so review analytics separate agent skins from (future) player skins.

### 10.3 Dependency staging

- **Calibration (M0–M1) needs none of PR #84.** Eval skins are built in-repo: document skins as documents, sprite briefs through the existing Rust patterns (`sprite.rs`/`animal.rs` recipes), reviewed via the drafts-as-PRs flow with contact sheets embedded (SHA-pinned URLs, per the established convention). Alex's PR review = the queue, verdict captured by the loop from PR state + comments.
- **Supervised production (M2) needs the minimum platform slice:** schema v2 (sprite/pattern vocabulary as data — without it every sprite skin is a human-merged Rust PR and autonomy is theater), texture upload, submission API, review queue with feedback, **and the decision feed** (10.2) — the loop cannot learn verdicts without it, and from M2 the loop lives in Hermes. This is the factory's real dependency on PR #84, and it is a *subset* — no Boost Bux, no Builder, no player creation needed.
- **Cruise (M3)** additionally wants the quick-tags; small.

### 10.4 What approval means

Approval publishes (or banks) the skin and marks the catalog entry `approved`. It also, always, runs a light retrospective — approvals carry signal too (what did the judges under-score? what did a gate almost fail?), and the "approved with feedback" case ("love it, but the tail is weak") is a lesson without a rejection.

## 11. Feedback assimilation (the retrospective engine)

The system the user story centers on: *every piece of feedback may touch a different part of the AI stack; find the root cause and fix it there, whatever step of the pipeline it lives in.*

### 11.1 Triggers

Every decision in **either stream** — creative (`stage: prototype`) and execution (`stage: implementation` and queue verdicts); every calibration-round correction; every kill (prototype-loop, gate-failure, or budget); every halt. One retrospective per trigger in supervised phases; batched (but still per-item) in cruise. Prototype-batch verdicts are retrospected *per batch*, not per image — five rejections of one batch are one story about one prompt.

### 11.2 Evidence

The full build trace (the factory logs its own episodes: prompts in, artifacts out, gate reports, judge critiques — it does not depend on mining Claude's transcript format, though transcripts remain available), the brief and its concept-judge scores, the decision + feedback, the current artifact versions (git SHAs), and the lessons ledger (has this happened before?).

### 11.3 The root-cause taxonomy — route to the artifact that should have prevented it

| # | Layer | Symptom pattern | Fix target | Applied how |
| --- | --- | --- | --- | --- |
| 1 | Design direction | Alex's reason states a taste rule no artifact encodes ("we don't do horror") | `design-direction.md` | **Proposal to Alex** — taste is his; never auto-applied |
| 2 | Ideation | Concept itself rejected; duplicate; IP flag confirmed | Catalog exemplar (specific) → ideation prompt (general) | Auto, validated per 6.2 |
| 3 | Brief | Build faithfully delivered a brief that underspecified the thing Alex flagged | Brief rubric | Auto, validated |
| 3a | Prototype | Batch rejections trace to the prompt, not the concept (right idea, wrong picture); or an approved prototype turned out unbuildable at implementation; or a queue rejection traces to the *look* Alex's own prototype gate approved (taste drift — rare and informative) | Prototype-prompt slot; prototype-judge rubric (calibration pass); or a buildability lesson feeding sub-score 3 | Auto for prompt lessons; judge edits queued for calibration |
| 4 | Build craft | Render diverged from the approved prototype, or violates a rule `author-skin` already states | Stage prompt (lesson) and/or `author-skin` edit | Auto for stage prompts; skill edits as reviewed commits. **The concept is re-queued for exactly one rebuild from the same approved prototype** once the lesson lands — creative approval survives an execution failure, and re-ideating a good idea because the build was wrong is the most expensive possible response |
| 5 | Deterministic gate gap | **Alex caught something a machine could have** (a measurable seam, a contrast, a budget) | New/updated check, with tests | Spawned as a repo task; the highest-value route — converts one human review into a permanent rail |
| 6 | Judge miss | Judges passed what Alex failed (or inverse) | Judge rubric — **but only in a calibration pass** | Queued for next calibration; never edited mid-production (6.2 hard rule) |
| 7 | Platform | Renderer/tooling bug or expressiveness limit | Repo issue / spawned task | Human-routed; not a prompt fix |

Routing preferences when several layers could absorb a fix: **most upstream cause wins** (a bad concept should die in ideation, not be polished in build), and **durability wins** — gate > skill/prompt > exemplar, because a gate never forgets. The worked precedent: all four items in the `skin-authoring-constraints` memory (schema-can't-pattern, op ceiling, perceptual-distance-not-contrast, clip-shape blindness) were layer-4/5 lessons learned by human pain; under this system each would have become a skill edit plus, for two of them, new gates — exactly the diffs this engine automates.

### 11.4 Specific → general promotion

Every retrospective writes at least a **specific** lesson (`lessons.jsonl`: id, trigger, layer, artifact, text, evidence links, status). Generalization is earned, not assumed: when ≥ 3 active lessons share a root-cause signature, a promotion pass synthesizes the general rule, applies it to the target artifact, and marks the specifics `promoted` (kept for audit, dropped from prompts). This is the AWS-DevOps-Agent "learned skill" pattern, and it directly encodes the requested ordering: *generalized lessons are better than specific ones, but specific ones are better than none.*

### 11.5 Risk tiers, validation, and safety rails

In production the retrospective runs on the local model, and what it may *apply* is tiered by blast radius:

- **Auto-apply (local, immediately): facts, verbatim.** The decision row, the catalog status, and Alex's literal feedback attached to the catalog entry and lesson ledger *as a quote*. These writes do reach the next tick's prompts — the catalog digest carries outcomes and quotes, and that responsiveness is the point of Reflexion — but they add only ground truth the queue produced, never the model's interpretation of it.
- **Pending (written now, inert until reviewed):** any *inferred* lesson — the local model's reading of *why* — is appended with status `pending` and excluded from every prompt context until a maintenance session activates it. This line is what keeps rail 4 honest: a taste-shaped inference cannot slip into generation behavior dressed as a lesson.
- **Escalate (queued for the next maintenance session):** anything that edits behavior — slot prompts, `author-skin`, judge rubrics, gates, promotions of specific→general, taste proposals — plus any retrospective the local model marks low-confidence or cannot route cleanly. The escalation queue (`skin-factory/state/escalations.jsonl`) is append-only and carries the full evidence bundle, so the maintenance session starts warm.

This tiering is the simplification that lets the loop run without Claude: the local model does the *routing and recording*, which is structured classification over a fixed taxonomy; the *editing of the stack* — the part that demands judgment — waits for a session that has it. Mechanically, every ledger write goes through `factory append` (which refuses non-append mutations) and every tick ends with `factory commit-tick` committing `state/` — append-only as a git-visible property, not an instruction a small model is trusted to follow.

- Every auto-applied edit runs its artifact's validation gate (6.2) before landing; a failed gate turns the edit into a proposal for Alex.
- Judge edits: only in calibration passes; validated on the frozen labeled train split with **zero holdout regression tolerated**; the retrospective that *identifies* a judge miss and the calibration pass that *fixes* it are separate runs — the engine never tunes a judge while a specific skin's fate is pending.
- **Repeat root cause = assimilation failure**, the loop's most important alarm: the same signature recurring after its lesson was applied (twice in any 10 decisions) triggers a meta-retrospective — was the lesson routed to the wrong layer? Written but not surfaced into the prompt context? Too specific? — whose subject is the assimilation engine's own routing, and whose output goes to Alex if it can't resolve itself.
- Append-only audit log: one entry per retrospective and per applied/proposed diff — trigger, evidence links, layer, artifact, diff SHA, validation result. Halts (`# HALT`), recalibrations, and GEPA runs log at the same level.

### 11.6 Maintenance sessions

A maintenance session is an **interactive Claude Code session Alex starts** — the same surface as this one, not a scheduled job — when the escalation queue has accumulated (the Hermes driver reports queue depth in its status and notifications; a suggested cadence is weekly or at ≥ 5 items, whichever first). Its **mandatory first act is pausing the loop** (set `paused` in `loop-state.json`, wait out any in-flight tick) and its **mandatory last act is committing and clearing the pause** — the two acts that guarantee production never reads a mid-edit working tree, since maintenance and the cron loop are otherwise two writers on one live workspace. The session: drains the queue (each item gets the full retrospective treatment of 11.3, with edits applied under 6.2's validation gates), runs any judge recalibration or GEPA re-run the alarms have requested, syncs `author-skin` with the slot prompts (9.3), re-runs the export gate if anything behavioral changed, and commits. Everything Claude does to the stack happens here or in calibration — nowhere else.

### 11.7 Bootstrap from conversation history

Before the first calibration round, a one-time mining pass over `~/.claude/projects/-Users-alex-Snaketron*/` transcripts and the distilled memory files extracts: every skin-related correction Alex has given (→ seed lessons, pre-routed by the taxonomy), every verdict-shaped statement (→ seed labeled examples, marked lower-confidence than queue decisions), and prior concepts (→ catalog). This is what the conversation history is *for* — it warms every store the loop reads, without pretending to be a metric.

## 12. The loop (feature 8)

### 12.1 Phases

| Phase | Home | Gate to enter | WIP cap | Review | Exit criteria |
| --- | --- | --- | --- | --- | --- |
| **M0a Prototype calibration** | Claude Code, interactive | Bootstrap mining done; workspace scaffolded | 13 (the calibration briefs) | Alex verdicts every image, every batch (Notion Training step 1) | Every calibration brief has an approved prototype; **GEPA runs 1 (prototype judge) and 2 (prototype prompt) complete**; the 5-at-once experiment measured |
| **M0b Implementation calibration** | Claude Code, interactive | M0a exit; `factory` driver scaffolded | 13 | Alex reviews every render, every round (Training 2–3) | Eval skins done to Alex's bar; craft-judge calibration under way; ≥ 40 labeled decisions per stream |
| **M1 Interview + export** | Claude Code, interactive | M0b exit | — (no new concepts) | Interview batches | **The export gate (12.2) passes** |
| **M2 Supervised production** | **Hermes, local model** | M1 exit + platform slice (10.3) live | 5 pending | Every skin; retrospective per decision before the slot refills | Trailing-20 approval ≥ 60% AND zero repeat root causes in last 20 |
| **M3 Cruise** | **Hermes, local model** | M2 exit | 10 pending | Alex on his schedule; batch retros | **approved_count ≥ 100** → done; or a halt |

### 12.2 The export gate (M1 → M2)

The handoff from Claude to Hermes is a checklist, run in the last M1 session, and every item is a measurement:

1. Disagreement clusters from calibration resolved or explicitly accepted (interviews done).
2. A judge configuration clears the **7.4 agreement gate on holdout** — local model preferred; if only an API judge clears it, that fallback is recorded as an explicit decision in the audit log, with the local judge's score kept as the number to beat at the next recalibration. This item deliberately precedes item 3: run 3's μ includes the frozen craft judge, so the judge configurations (concept, prototype, craft) must be frozen first.
3. Execution GEPA run 3 complete with the **local model as task LM**, and the 4 holdout briefs build gate-green under the local model, first try (run 2 — the prototype prompt — completed back in M0a).
4. The Hermes skill dry-runs one full tick against the live workspace (ingest → retro-tiering → build → submit stubbed) with Alex watching — the one sanctioned invocation while `config.yaml` still says `m1`.
5. `config.yaml` flips `phase: m2`. Nothing is copied anywhere: the category symlink installs the skill, and the skill reads the same repo workspace directly at `~/Snaketron/skin-factory/`.

If item 3 fails after two GEPA attempts, the honest conclusions are limited: either a slot needs a deterministic assist (more rails, less model) or that slot stays on an API model until distillation (section 17) — both are decisions for Alex, presented with the holdout traces.

### 12.3 One production tick (Hermes cron — M2 on Alex's cadence, M3 every 2h)

Everything mechanical below is a `factory` CLI subcommand; the local model sequences the calls, runs the retrospectives, and writes the prose.

1. `factory ingest`: poll the decision feed, advance the cursor, append decision rows (queue verdicts and any Alex-lane prototype verdicts, each carrying `stage` and `rater` per 5.5), resume any concept whose prototype review has come back, recompute metrics. Holdout labels are assigned per-stream by `factory append` wherever a row is written (7.3).
2. For each new non-holdout decision and each kill: local retrospective → apply/pend/escalate per 11.5, every ledger write through `factory append`. Holdout decisions are record-only.
3. `factory halt-check`: evaluate 12.4 mechanically. If any fire: pause submissions, notify Alex, stop the tick.
4. If pending < WIP cap and `factory budget` clears: ideation cycle → **prototype loop (9.1: 5-image batches, judge-gated, ≤ 3 iterations — a concept with no approved prototype dies here, cheaply)** → build from the approved prototype → self-review → submit (or kill at any stage → retrospective).
5. `factory commit-tick`: append the tick summary to the audit log, commit `state/` (every tick is a git diff — tamper-evidence for the append-only ledgers), report escalation-queue depth.

### 12.4 Halt rules

| Condition | Action |
| --- | --- |
| 3 consecutive rejections | Pause; if without feedback → request feedback / trigger interview |
| 3 consecutive zero-approval prototype kills, or trailing-10 clean-batch rate below the floor | Pause: the prototype stage has drifted or the prompt regressed. This is the taste stream's halt rule, and it exists because the agreement rule cannot fire on a stream the judge labels alone (7.4) |
| 5 consecutive kills (gate failures) | Halt: execution regressed; the driver re-runs the deterministic holdout builds to bisect (recent lesson? platform change?) and queues the kill traces for maintenance |
| Trailing-20 judge–Alex agreement < 60% | Pause submissions; request a maintenance session (recalibration) |
| Repeat root cause (11.5) | Meta-retrospective queued for maintenance; escalate to Alex if unresolved there |
| Daily external cost > $25 or global > $1,000 | Stop until Alex raises the cap |
| Queue stale > 14 days (nothing reviewed, skins or prototype batches) | Idle politely: stop building at WIP cap; never spam the queue |
| 3 halts in 7 days | Full stop + summary to Alex — something structural is wrong |

All caps and thresholds live in `skin-factory/config.yaml`; the driver enforces them mechanically.

## 13. The Hermes skill

`hermes/skills/snaketron-skin-factory/SKILL.md` (in this repo), installed by category symlink, matching the agent-harness convention:

```bash
ln -sfn ~/Snaketron/hermes/skills ~/.hermes/skills/snaketron
```

The skill is the **production SOP**, and it never touches Claude: preflight (repo present; LM Studio serving the pinned chat and embedding models; budgets readable; phase ≥ m2, with one carve-out — Alex's explicit export-gate dry-run runs in m1 with submissions stubbed; not `paused` or `halted`), the state machine of section 12 over `skin-factory/state/`, local dispatches (ideation cycle, the prototype loop with its per-image judge verdicts, `factory` driver builds with local slot prompts, judge calls per the calibrated config), the 11.5 risk tiering on retrospectives, halt enforcement, escalation-queue reporting, and notification of Alex. All arithmetic, polling, ledger writes, halt evaluation, spend metering, and the per-tick commit are `factory` CLI subcommands — the driver model sequences them; it computes nothing and opens no ledger itself. The craft lives in the GEPA-tuned prompts and the deterministic driver, not in the driver model's judgment. Cron entry: every 2 hours in M3. During M0–M1 the skill is dormant — those phases run in Claude Code, and the skill's only pre-export use is the dry-run tick of the export gate (12.2).

The skill file ships alongside this PRD; it is the second artifact of this branch.

## 14. Metrics

| Metric | Definition | Why it matters |
| --- | --- | --- |
| Approval rate | Approvals / decisions, trailing 20 | The headline; phase gates read it |
| Repeat-root-cause rate | Recurrences of an already-lessoned signature per 20 decisions | **The self-improvement KPI** — measures whether assimilation works, not whether skins are good |
| Judge–Alex agreement | Raw agreement + false-approve/false-reject split, trailing 20 + holdout, with a min-n rule (κ logged as diagnostic only — unstable at these sample sizes) | Licenses judges as μ; guards against drift |
| Prototype yield | Clean-batch rate, approvals per batch, and prototype→implementation conversion, trailing | Leading indicator of taste alignment — and it mints labels 5 at a time |
| First-pass gate yield | Builds passing all deterministic gates without repair | Execution quality; GEPA run 3's macro effect |
| Cost / approved skin | Total spend ÷ approvals | Sustainability; expect $5–15 early, falling |
| Novelty floor | Min catalog embedding distance among last 10 approved | Uniqueness isn't drifting as the catalog grows |
| Queue latency honored | Never > WIP cap pending | The loop respects Alex's attention |

## 15. Cost envelope

With the production loop local, external spend concentrates in calibration. Production: per concept ≈ $0.50–2.50 (prototype batch + textures; judges only under the API fallback, at cents), and the prototype gate makes attempts *cheaper on average* — concepts that would have failed after a full build now die for the price of a few images. 100 approved ≈ **$75–350** — plus local compute, which is bounded by wall clock, not dollars. Calibration: prototype and build rounds run inside interactive Claude Code sessions (subscription, not metered API), so their external cost is also mostly images, ≈ $60–200 including the M0a batches; the GEPA runs are the real API line item — reflection calls at $100–400 for run 3, ~$50 for run 2, and ~$20 each for runs 1 and 4. Whole program comfortably under **$1,000**; the caps in 12.4 enforce it mechanically. All figures are caps-enforced estimates, not promises.

## 16. Risks

| Risk | Mitigation |
| --- | --- |
| The local model can't reach the craft bar | The export gate measures it instead of assuming (12.2): holdout briefs, first-try, under the local model. Mitigations in order: more deterministic rails in the driver, a GEPA re-run, a per-slot API fallback recorded as a decision and held until distillation (§17). Never silent degradation — the gates catch what the model fumbles |
| Judges Goodhart — pipeline optimizes toward rubric, Alex drifts away | Agreement and the error split tracked on *every* decision forever, not just in calibration; < 60% pauses the line; judge config pinned (model + version/quant) |
| Assimilation thrash — lessons contradicting lessons | Ledger is append-only with supersedence links; promotion requires 3 concordant specifics; meta-retrospective on repeats |
| Overfitting to the 13 calibration briefs | 4 frozen holdouts + rolling 1-in-5 holdout assignment from production decisions |
| PR #84 slips | M0–M1 are independent (10.3); M2's platform slice is small and can be carved out ahead of the rest |
| Sprite quality ceiling — image models can't reliably hit 16px pixel-art at mark scale | The gate stack rejects rather than ships (the tools already encode this); kills route to retrospectives; worst case the concept mix shifts toward patterns, which is a taste question for an interview |
| Eval-set IP leaks into production | Eval briefs flagged `calibration_only` in the catalog; IP judge on every production brief; Alex is the last gate regardless |
| Feedback fatigue — Alex stops writing reasons | Verdict-only decisions still assimilate (weaker route via judges' hypothesized reason, marked low-confidence); quick-tags lower the cost of a reason to one click; the interview trigger fires on feedback drought |

## 17. Open questions

1. **Approve-to-where**: does approval publish immediately, or bank into a release cadence (e.g., weekly drops)? Affects only the queue UI, not the loop.
2. **Notion write-back**: should accepted direction deltas be pushed to the Notion page (as flagged suggestions via the connector), or does the repo copy become canonical with Notion as Alex's scratchpad? Recommend the latter; decide before M1.
3. **Fan-out mode**: country/state/team skins are template-instantiable (one optimized brief schema × 200 instances) — a different, cheaper loop mode with its own judge emphasis (accuracy of flags/colors over uniqueness). Spec as v2 once the base loop holds.
4. **Trace distillation**: once ≥ 100 successful build traces exist (Claude's calibration builds plus approved production builds), SFT-ing the local model on them is the standard lever for retiring any per-slot API fallback and for raising the local craft ceiling generally. Not before.

---

## Appendix A — GEPA run configs (summary)

All runs: reflection LM = Claude; task LM = **whatever will run that component in production** — optimizing a prompt for a model it won't run on is wasted rollouts.

| Run | Genome | Task LM | Instances | μ | μf | Holdout gate | Budget |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 Judges | rubric texts (concept, prototype, craft — per-stream labels, never pooled) | candidate judge configs, local qwen first | labeled decisions (train split) | verdict agreement | Alex's reason vs judge critique | holdout agreement no-regress | ~$20, hours |
| 2 Prototype prompt | prototype-prompt slot | local qwen | 13 calibration briefs | calibrated prototype judge over fresh batches | per-image rejection reasons | un-iterated batches on 4 holdout briefs no-regress (7.3) | ~$50 (5 images per rollout) |
| 3 Execution | build slot prompts | local qwen (production executor) | 13 calibration briefs + their approved prototypes | gate pass score + frozen craft judge (prototype fidelity) | gate error text | 4 holdout briefs no-regress | ≤ $400, a weekend |
| 4 Ideation | ideation prompt + brief rubric | local qwen | seed list | calibrated concept judge + novelty | judge critiques + collision report | reserved seeds no-regress | ≤ $100 |

## Appendix B — Eval-set split (proposed)

Calibration (13): space invaders, grateful dead bears, barber pole, fish, python, tron vehicle, boat, dragons, hello-kitty-style, anime, golf, thanksgiving, minecraft. Holdout (4): wizard wand, surfing, shark, christmas. Alex may amend before round 1; frozen after.

## Appendix C — Worked routing examples

1. *"The bears are cute but the snake reads as noise at game zoom."* → Layer 4 (craft: small-cell readability is a stated rule) **and** layer 5 candidate: a legibility gate — downsample the contact sheet to cell-size 7 and threshold on mark-scale/contrast metrics. Specific lesson now; gate task spawned; if two more skins draw the same reason, the promotion pass makes the gate mandatory.
2. *"Another space theme?"* → Layer 2. Catalog exemplar immediately; on recurrence, ideation prompt gains a diversity constraint over recent approvals' tag distribution.
3. *Judges 5/5, Alex: "the head rule is violated — first cells are mid-contrast."* → Layer 6 (queued for calibration pass) **and** layer 5: the head light/dark rule is measurable — it should be a validator check, not a judge opinion. Gate wins on durability.
4. *"Love it."* (approval, no feedback) → Light retro: record; check for near-miss gates (op count 194/200 → note for structure planning); no edits.
