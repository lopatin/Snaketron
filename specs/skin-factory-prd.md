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

Build the factory as an **agentic pipeline whose every stage is a separately versioned, separately evaluable artifact** — DSPy's program-of-modules discipline without DSPy as the runtime — and apply **GEPA where GEPA is actually affordable**: first to the judge rubrics (one LLM call per rollout, metric = agreement with Alex's recorded decisions), then to execution-stage prompts (metric = the deterministic gate stack that already exists), and only later to ideation (metric = the judge panel, once the judges have *earned* metric status by demonstrated agreement with Alex). Everything the human gates — the whole-pipeline level where one decision costs Alex a review — runs the same reflective move GEPA makes, but as a **single-sample Reflexion retrospective**: read the trace, root-cause the failure to the artifact that should have prevented it, edit that artifact, validate the edit against held-out labeled data.

The direct answers to the framing questions:

**Are Training parts 4 and 5 a GEPA program?** They are GEPA's *reflective mutation step* — read traces + feedback, rewrite the instruction text — extracted from the loop. What Notion's part 4–5 lacks to be actual GEPA is a metric that can be called hundreds of times: GEPA needs forward rollouts scored automatically, and today the scorer for "is this skin good" is Alex. So parts 4 and 5 are implemented here as the **assimilation engine** (section 11): the GEPA move with batch size 1, a human μ, and an audit trail. Actual `dspy.GEPA` runs appear in three places where an automatic μ exists or can be built (section 5.4) — and the first of them, judge-rubric optimization, is the mechanism by which the human μ gets converted into an automatic one. Calibrating the judges is not a preliminary to using GEPA; it **is** the first GEPA run.

**Ax or DSPy?** DSPy (Python). `dspy.GEPA` is the reference implementation maintained by the GEPA authors; the asset tooling the metric has to call (`sprite_sheet.py`, `build_coat_textures.py`, the validator binary) is already Python-adjacent, so μ composes in-process; and the calibration math (agreement, κ, split management) wants to live next to the optimizer. Ax is a faithful TypeScript port and would work, but nothing in this system runs in TS except the web client, and the web client is not where optimization happens.

**"I have the convo history, so I have enough to auto-train the skill."** Half right, and the half matters. Conversation history is a **reflection corpus and a label mine**: it bootstraps the lessons ledger (every correction Alex has ever given a skin session), the judge calibration set (his recorded verdicts and reasons), and the retrospective engine's evidence base. It is **not a metric**: history cannot score a *new* candidate prompt, so it cannot drive an optimization loop by itself. Every improvement iteration still needs fresh rollouts through the gates and judges. History makes the loop start warm; it does not replace the loop.

## 2. Product problem and goal

Skins are Snaketron's content treadmill, and today each one costs a full human-supervised authoring session. The Notion page holds the design direction, a 17-brief eval set, and a fan-out list (countries, states, teams, sports) that implies **hundreds** of skins nobody has time to build by hand. The `author-skin` skill has crystallized the craft; the gate stack (validator, op ceiling, seam metrics, conformance suite) has crystallized the checkable half of quality; PR #84 specs the platform that makes a skin a submittable data object. What is missing is the system that connects them into a loop that runs without Alex in it — except at the single point where his judgment is the product: the approve/disapprove decision.

**Goal:** a Hermes-scheduled loop that ideates unique, chill skin concepts, builds them flawlessly, submits them to a review queue on Snaketron.io, and — this is the actual product — **assimilates every piece of feedback Alex gives into the layer of the stack that should have prevented the issue**, so that the approval rate climbs and the same mistake is never reviewed twice. Terminal condition: 100 approved skins (configurable), or a halt rule fires.

## 3. Goals and non-goals

### 3.1 Goals

1. **Judge calibration** (Notion Training steps 1–3, formalized): build the 17 eval-set skins through the full pipeline with Alex reviewing every round; convert his verdicts and reasons into a frozen labeled decision set; optimize judge rubrics against it with GEPA until holdout agreement clears threshold.
2. **Disagreement-driven interviews**: when judges and Alex disagree in ways no recorded evidence explains, generate concrete, instance-grounded questions (never "what do you like?") whose answers become labeled data and design-direction deltas.
3. **Ideation as Reflexion + a growing catalog**: concept generation that reads the catalog of everything tried (approved, rejected, killed), scores novelty by embedding distance, applies the twist-on-popular guideline with an IP-risk gate, and learns from every rejection.
4. **Execution flawless from a name and description**: the `author-skin` craft, decomposed into stages with deterministic gates between them, with stage prompts optimized offline by GEPA against those gates.
5. **A queue on Snaketron.io** built on PR #84's admin review surface: agent-submitted skins land in Alex's queue; approve/disapprove with optional free-text feedback; the decision and feedback flow back to the loop as structured data.
6. **Feedback assimilation with root-cause routing**: every decision (and every mid-calibration correction) triggers a retrospective that names the layer, the artifact, and the diff — skill file, judge rubric, deterministic gate, ideation catalog, design-direction doc, or platform bug — and applies it under validation. Specific lessons immediately; generalization on recurrence.
7. **The loop until N=100**, phase-gated: calibration → supervised production → cruise, with WIP caps, cost caps, and halt rules throughout.
8. **Everything auditable**: append-only audit log, every self-modification a reviewable diff, git as the substrate.

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

**Missing entirely (this PRD's scope):** the judge panel and its calibration machinery; the ideation engine and catalog; the stage decomposition of execution with GEPA harnesses; review-decision feedback capture and the decision feed; the assimilation engine; the Hermes loop skill; the factory workspace.

## 5. The training question, answered precisely

### 5.1 What GEPA is and what it needs

GEPA (Agrawal et al., arXiv:2507.19457) evolves the instruction text of a program's modules: run the program on training instances, collect execution traces, have an LLM *reflect* on traces plus textual feedback (μf) to propose a rewritten instruction for one module, keep candidates on a Pareto frontier over instances, select by aggregate score (μ). Its sample efficiency (up to 35× fewer rollouts than RL post-training, and better results) comes from the reflection being in natural language over rich traces rather than a scalar gradient. It still needs three things:

1. **A program with distinct text components** whose text is the genome.
2. **A metric μ callable per rollout**, plus textual feedback μf per rollout.
3. **Training instances** to roll out on, with some held out.

### 5.2 Where the factory satisfies those requirements — and where it doesn't

| Stage | Rollout cost | μ available? | Verdict |
| --- | --- | --- | --- |
| Judge scoring (rubric text → verdict on one example) | 1 LLM call, ~$0.01, seconds | Yes, from day one: agreement with Alex's recorded decision on that example | **True GEPA, immediately.** The cheapest rollouts and the cleanest metric in the whole system. |
| Execution stages (brief → doc/assets → render) | Agent episode, ~$2–8, 10–30 min | Yes: the deterministic gate stack (validator errors, op counts, seam/detail/chroma ratios, conformance results) — dense, textual, hard to game | **True GEPA, budget-limited.** ~100 rollouts ≈ a weekend and low hundreds of dollars. Run offline between phases, not inline. |
| Ideation (seed → brief) | 1–3 LLM calls, cheap | Only after calibration: the judge panel is μ *only once it demonstrably agrees with Alex* (κ ≥ 0.7 holdout, section 7.4) | **True GEPA, gated on judge calibration.** Before that, optimizing ideation against uncalibrated judges optimizes toward a rubric nobody validated. |
| Whole pipeline (seed → approved skin) | Build + **one Alex review** | No. μ is a human; ~17 calibration instances; each rollout costs the scarcest resource | **Not GEPA.** Reflexion retrospective per decision (section 11) — the same reflective move, batch size 1, audited. |

This ordering is the answer to "calibrate (and/or train?) the judges": *calibrate*, meaning optimize their rubric text against Alex's frozen decisions — which is training in the GEPA sense and requires no weights. Fine-tuning a judge model is unjustified below several hundred labels and would freeze taste into weights that can't be diffed or audited.

### 5.3 Inputs and outputs, concretely

If Alex runs the DSPy program himself, these are the signatures. Each row is a DSPy module (or an agent episode wrapped to look like one); the *genome* column is what GEPA rewrites.

**Judge (per dimension d):**
- **Input x:** artifact under judgment (brief text, or contact-sheet images + doc summary), design-direction doc, dimension rubric.
- **Output y:** per-dimension score 1–5, verdict ∈ {approve, reject}, critique text.
- **μ:** verdict agreement with Alex's decision on labeled example x (train split); report κ on holdout.
- **μf:** where they disagreed, Alex's recorded reason vs. the judge's critique.
- **Genome:** the dimension's rubric text.

**Ideation:**
- **Input x:** seed (fan-out entry, trend, or open), design-direction doc, catalog digest (recent + nearest concepts with outcomes), active ideation lessons.
- **Output y:** brief = `{name, description}` in the eval-set register (2–6 sentences naming structure: head/body/tail treatment, pattern vs sprite, palette intent, motion intent).
- **μ (post-calibration):** concept-judge panel score + novelty distance (section 8.2).
- **μf:** judge critiques + nearest-neighbor collision report.
- **Genome:** the ideation prompt and the brief-writing rubric.

**Build (the agent episode, treated as one module):**
- **Input x:** brief, design-direction doc, active execution lessons, platform constraints digest.
- **Output y:** skin bundle = SkinDoc (+ texture PNGs if sprite) + contact sheet + gate report.
- **μ:** weighted gate pass rate: validator clean, op count ≤ 200, seam ≤ target, detail/chroma in band, conformance green, goldens untouched, palette distances ≥ 0.10 — plus craft-judge score on the render.
- **μf:** the gate stack's own error text — validator messages name the field and the rule; seam reports carry the numbers; conformance failures name the invariant. This is unusually rich μf and is why GEPA should work well here.
- **Genome:** the stage instructions distilled from `author-skin` (section 9.2) — not the skill file itself; the skill file remains the human-readable source that the stage instructions are compiled from.

### 5.4 The three sanctioned GEPA runs

1. **Judge rubrics** (during and after calibration, repeatable): μ = train-split agreement; promotion gate = holdout agreement did not regress. First run as soon as ≥ 40 labeled decisions exist (~2 calibration rounds over 17 briefs).
2. **Execution stage prompts** (between phases M1→M2 and on execution-regression halts): μ = deterministic gates + frozen craft judge; instances = the 13 calibration briefs; holdout = the 4 holdout briefs (section 7.3) which GEPA never sees.
3. **Ideation prompt** (M2+, once judge κ ≥ 0.7 holds for two consecutive audits): μ = calibrated concept panel + novelty; holdout = a reserved seed list.

Each run is an offline batch job with an explicit cost cap, its config and result committed to the audit log. GEPA never runs inline in the production loop, and no GEPA run's μ includes a judge whose rubric changed since its last validation.

## 6. System architecture

```
                        ┌────────────────────────────────────────────────┐
                        │  DESIGN DIRECTION  (repo: skin-factory/)        │
                        │  design-direction.md  ← synced from Notion      │
                        │  lessons ledger · catalog · labeled decisions   │
                        └───────┬─────────────────────────▲──────────────┘
                                │ read by everything      │ written only by
                                ▼                         │ assimilation (audited)
   ┌──────────┐   brief   ┌──────────┐  bundle  ┌────────┴───┐  submit  ┌─────────────┐
   │ IDEATION │──────────▶│  BUILD   │─────────▶│ SELF-REVIEW │─────────▶│ QUEUE        │
   │ Reflexion│           │ agent    │  gates   │ craft judges│          │ Snaketron.io │
   │ +catalog │◀──────────│ episode  │◀─────────│ repair ≤3   │          │ Alex decides │
   └────▲─────┘  kill/    └──────────┘  fail    └────────────┘          └──────┬──────┘
        │        retry                                                          │ verdict
        │                                                                       │ + feedback
        │                 ┌─────────────────────────────────────────────┐       │
        └─────────────────│  ASSIMILATION  (retrospective, root-cause,  │◀──────┘
              lessons     │  route to artifact, validate, apply, audit) │
                          └─────────────────────────────────────────────┘

   Orchestration: Hermes cron (local model) drives the state machine and dispatches
   each box to Claude Code (`claude -p`) or to pinned-model API calls (judges).
```

### 6.1 Who runs what

| Component | Runtime | Model | Why |
| --- | --- | --- | --- |
| Loop driver (state machine, polling, caps, notifications) | Hermes cron skill | Local qwen (Hermes default) | Always-on, cheap; makes no quality-bearing decisions |
| Ideation, retrospectives | `claude -p` headless session | Claude (session default) | Needs repo + ledger context and judgment |
| Build | `claude -p` with `author-skin`-derived stage skills | Claude | Tool-using agent episode: validators, renders, screenshots |
| Judges | Direct API calls | **Pinned** model + version per judge | Reproducibility of μ; a silent model upgrade would invalidate calibration |
| Image generation | API | `gemini-3.1-flash-image` (Nano Banana 2) for iteration; `gemini-3-pro-image` for final assets when flash quality is insufficient; never 2.5 | Standing preference; the Notion plan's "Gemini 3 Pro Image" maps to the finals tier |
| GEPA runs | Offline Python (DSPy) | Reflection LM: Claude; task LMs as above | Section 5.4 |

### 6.2 The artifact registry — every mutable thing, who may change it, what validates the change

This table is the heart of the self-improvement design. **Only the target artifact of a given improvement process evolves; everything else in that process is a ratchet.**

| Artifact | Lives at | Mutated by | Validation gate |
| --- | --- | --- | --- |
| `design-direction.md` (taste spec) | `skin-factory/direction/` | Alex directly; assimilation may **propose** (PR-style diff surfaced to Alex) | Alex's sign-off, always — taste changes are never auto-applied |
| Judge rubrics | `skin-factory/judges/*.md` | GEPA calibration runs; assimilation (calibration passes only) | Frozen labeled set: train-split improvement AND no holdout regression |
| Ideation prompt + brief rubric | `skin-factory/prompts/ideation/` | GEPA (run 3); assimilation lessons | Concept-panel score on reserved seeds must not regress |
| Execution stage prompts | `skin-factory/prompts/build/` | GEPA (run 2); assimilation lessons | Deterministic gates on the 4 holdout briefs must not regress |
| `author-skin` skill + checklists | `.claude/skills/author-skin/` | Assimilation, as ordinary reviewed commits | Repo review; conformance + goldens stay green |
| Deterministic gates (new checks) | validator / test suites / tools | Assimilation → **spawned as repo tasks with tests** | Normal CI; a gate change that fails existing skins needs explicit adjudication |
| Lessons ledger, catalog, decisions | `skin-factory/state/` | Append-only by their owning stages | Schema check; never rewritten, only superseded |
| Audit log | `skin-factory/state/audit.md` | Append-only, every mutation above logs here | Never edited |
| This PRD, the Hermes skill | `specs/`, `hermes/skills/` | Humans (and proposals) | Repo review |

Hard rules inherited from the self-improvement literature, non-negotiable here: judges and the processes they evaluate are edited in separate passes (an assimilation triggered by a failing skin may never touch a judge to make that skin pass); every self-modification is a diff in git plus an audit-log row; holdout data gates every promotion; cost caps are enforced by the driver, not by convention.

## 7. Judge panel and calibration

### 7.1 The panel

Two sub-panels, judging different objects at different times:

**Concept judges** (pre-build, on briefs — cheap, run on every candidate):

| Judge | Question | Notes |
| --- | --- | --- |
| Uniqueness | Is this meaningfully distinct from everything in the catalog? | Receives the nearest-neighbor report (section 8.2); scores the *twist*, not just distance |
| Direction fit | Is it "chill" and confidently in one artistic direction, per the guidelines? | The operational definition of *chill* is seeded in 7.5 and owned by calibration |
| Feasibility | Can the pipeline express it well? Pattern vs sprite correctly chosen; repeat length sane; reads at 16px cells; head/tail treatment respects the 1.5-cell light/dark rule; op budget plausible | Encodes the craft constraints so bad briefs die before costing a build |
| IP risk | Does it use a protected mark/likeness, or the style-not-mark transform? | The Notion Hello Kitty entry is the worked example: style yes, mark no. Celebrity fan-out requires likeness caution |

**Craft judges** (post-build, on the contact sheet + doc summary):

| Judge | Question |
| --- | --- |
| Guideline conformance | Head light/dark rule with correct core color; no seams; team sides legible; readable at cell sizes 5–15; production quality, no mush |
| Brief fidelity | Does the render deliver what the brief promised — the named structure, palette, and motion? |
| Overall quality | Would this embarrass us on the Skins page? The catch-all that calibration teaches |

Protocol for all judges: pinned model and version; structured output (per-dimension 1–5, verdict, critique — the critique is μf); blind to which pipeline version produced the input; pairwise comparisons (used in ideation tournaments) always run both orders, and an order-flip disagreement rate above 25% in any batch is a halt-and-audit signal, not noise to average away.

### 7.2 Calibration = Notion Training steps 1–3, instrumented

Each calibration round: build every non-holdout eval brief through the current pipeline → Alex reviews each result, giving verdict + what-to-fix-and-why (structured capture, section 10.2) → every verdict lands in `decisions.jsonl`, every reason spawns an assimilation retrospective (section 11), and the judges are scored *against* Alex's verdicts before their rubrics are GEPA-tuned on the accumulated set. Iterate until the eval skins are done (Alex's bar), which typically coincides with the judges having enough labels to be tunable.

### 7.3 The split

Of the 17 eval briefs: **13 calibration** (judges and GEPA see them), **4 holdout** (never used to tune anything; used only to gate promotions). Suggested holdouts, chosen to cover the structural space: one pure-sprite (surfing), one pattern (shark), one head/tail-anchored (wizard wand), one full-scene (christmas). Alex may swap the assignment before round 1; after round 1 the split is frozen. Production-phase decisions (section 10) continuously grow the labeled set, with every 5th decision assigned to holdout.

### 7.4 The standing metric

Judge–Alex agreement (raw + Cohen's κ) on the trailing 20 decisions and on holdout, recomputed at every decision and reported in the audit log. **κ ≥ 0.7 on holdout** is the gate for: using judges as GEPA μ for ideation; entering cruise phase. Trailing agreement < 60% at any point triggers a recalibration pass and pauses submissions.

### 7.5 The interview (feature 2)

Interviews are generated, not scheduled — triggered when (a) after a calibration round, judge–Alex disagreements cluster in a way no rubric edit derived from recorded reasons resolves; (b) a proposed generalization would alter `design-direction.md` (taste-level change → needs Alex's word, not the agent's inference); (c) a halt rule fires for repeated rejection without feedback.

Format rules: batched, ≤ 10 questions; every question grounded in a concrete instance ("here are two renders of the dragon head, A and B — which reads better at 15px and why", with images attached), multiple-choice plus optional free text; never open-ended taste surveys. Answers are triple-booked: labeled decisions (if verdict-shaped), direction deltas (if taste-shaped), and lessons (always). Seed definitions the first interview should sharpen: *chill* ≈ one confident aesthetic, low visual noise at arena cell sizes, slow smooth motion, no strobing, palette restraint; *unique* ≈ no near-neighbor in the catalog AND carries a recognizable twist on something popular. These seeds are hypotheses, deliberately falsifiable by calibration.

## 8. Ideation engine (feature 3): Reflexion + growing catalog

### 8.1 Generate → judge → select

Per cycle: draw a seed (weighted: fan-out backlog, trending themes, open creativity — weights tunable), generate K=5 candidate briefs with the ideation prompt + catalog digest + active ideation lessons; concept panel scores each (absolute rubric scoring; pairwise tournament with order-swap when scores tie); top brief above threshold proceeds to build, the rest land in the catalog as `not_selected` with their critiques (Reflexion memory — next cycle's generator reads why its siblings died).

### 8.2 The catalog

`skin-factory/state/catalog.jsonl`, append-only: `{id, name, description, tags, seed, status: approved|rejected|killed|not_selected|shipped, embedding_key, judge_scores, feedback_digest, date}`. Embeddings stored beside it. Novelty check = min cosine distance to all prior entries plus tag-collision report, fed to the Uniqueness judge (the judge interprets the distance — a low distance with a genuinely new twist can pass; a high distance that's still "another animal print" can fail). Bootstrap: the 19 shipped catalog skins, the 17 eval briefs, and concepts mined from conversation history all enter on day one, so the factory never re-pitches what exists.

### 8.3 Reflexion

Every terminal outcome writes a lesson candidate back into ideation's view: rejections with feedback ("too busy", "another neon") become negative exemplars in the catalog digest; approvals become positive anchors. The assimilation engine (section 11) decides whether a lesson stays specific (an exemplar line) or generalizes (an ideation-prompt edit) — ideation itself never edits its own prompt.

### 8.4 IP guardrails

The eval set is IP-heavy by design and **never ships** (calibration fixtures only). For production skins the IP-risk judge enforces the style-not-mark transform; the celebrity fan-out line additionally requires no likeness, no name — themes *around* a persona, not the persona. Anything the judge flags as borderline goes to the queue with the flag visible, so Alex's decision doubles as IP calibration.

## 9. Execution engine (feature 4)

### 9.1 Stages and gates

The build is one Claude agent episode internally structured as gated stages; every inter-stage gate is deterministic and its failure text is preserved (it is μf for GEPA and evidence for retrospectives).

| Stage | Output | Gate |
| --- | --- | --- |
| 1. Structure plan | pattern vs sprite; layer plan; repeat length; palette | Feasibility rules (op-budget arithmetic *before* building; 200-op ceiling, tile-count math from `author-skin`) |
| 2. Asset generation (sprite path) | source images via Nano Banana 2 → sprite sheets / coats | `sprite_sheet.py` / `build_coat_textures.py` full gate stack: seam ≤ 1.2 target, detail/chroma bands, structural checks, mark-scale `cells_for`; bounded regeneration with prompt feedback on rejection |
| 3. Document authoring | SkinDoc (v2 when landed; v1 + Rust-pattern interim, section 10.3) | `validate-skin` clean |
| 4. Register + render | contact sheet via `capture-skin-sheet.mjs`, `/qa/skins` captures incl. small cell sizes and short-snake pose | Screenshots exist and are non-classic (stale-WASM trap check from `author-skin` §4) |
| 5. Self-review + repair | craft-judge verdicts; targeted fixes | ≤ 3 repair iterations; then kill (a kill is an assimilation input, not a silent retry) |
| 6. Prove | conformance suite, goldens, catalog parity, op measurement | All green; goldens untouched |
| 7. Package | bundle: doc + assets + screenshots + gate report + brief | Schema check; submitted or (interim) PR opened |

### 9.2 The genome vs the skill

`author-skin/SKILL.md` stays the canonical, human-maintained craft document. The build stages consume **stage prompts compiled from it** (`skin-factory/prompts/build/`), which is what GEPA optimizes — so an optimization can never silently rewrite the skill humans read, and a skill edit (by assimilation or by hand) triggers recompilation of stage prompts followed by a holdout-brief regression run. Divergence between skill and stage prompts is checked by a freshness hash.

### 9.3 Budgets

Per-skin: ≤ $10 LLM + image spend, ≤ 45 min wall clock, ≤ 3 self-review repairs, ≤ 2 asset regenerations per texture. Exceeding any budget kills the attempt and files the trace for retrospective. (A kill on budget is *evidence about the pipeline*, not about the concept — the retrospective decides which.)

## 10. Publication and the queue (Snaketron.io)

### 10.1 Reuse PR #84, don't parallel it

The queue **is** the first-class-skins admin review surface. The factory runs as a dedicated agent user (`factory` account, admin-flagged as agent, never admin): creates skins, uploads textures, submits revisions for publication review. Alex's existing `/admin` review queue shows them. Approve = publish (or approve-private, Alex's choice per skin); disapprove = reject the pending revision.

### 10.2 The deltas this PRD adds to the platform

1. **`review_feedback: Option<String>` on the review decision** — the optional free-text Alex attaches when approving or (especially) rejecting. Persisted on the decision record, immutable.
2. **A decision feed the loop can poll**: `GET /api/skins/factory/decisions?since=<cursor>` returning `{skin_id, revision, verdict, feedback, decided_at}` for the agent user's submissions. (Polling, not webhooks — the loop already wakes on cron.)
3. **Structured feedback capture in the review UI**: verdict buttons + free-text box + optional quick-tags (`concept`, `craft`, `too-similar`, `ip`, `direction`) that pre-route the retrospective. Tags are hints, not the router — the retrospective can overrule them.
4. **A `factory` origin marker on submissions**, so the queue can filter and so review analytics separate agent skins from (future) player skins.

### 10.3 Dependency staging

- **Calibration (M0–M1) needs none of PR #84.** Eval skins are built in-repo: document skins as documents, sprite briefs through the existing Rust patterns (`sprite.rs`/`animal.rs` recipes), reviewed via the drafts-as-PRs flow with contact sheets embedded (SHA-pinned URLs, per the established convention). Alex's PR review = the queue, verdict captured by the loop from PR state + comments.
- **Supervised production (M2) needs the minimum platform slice:** schema v2 (sprite/pattern vocabulary as data — without it every sprite skin is a human-merged Rust PR and autonomy is theater), texture upload, submission API, review queue with feedback (10.2). This is the factory's real dependency on PR #84, and it is a *subset* — no Boost Bux, no Builder, no player creation needed.
- **Cruise (M3)** additionally wants the decision feed and quick-tags; both are small.

### 10.4 What approval means

Approval publishes (or banks) the skin and marks the catalog entry `approved`. It also, always, runs a light retrospective — approvals carry signal too (what did the judges under-score? what did a gate almost fail?), and the "approved with feedback" case ("love it, but the tail is weak") is a lesson without a rejection.

## 11. Feedback assimilation (the retrospective engine)

The system the user story centers on: *every piece of feedback may touch a different part of the AI stack; find the root cause and fix it there, whatever step of the pipeline it lives in.*

### 11.1 Triggers

Every queue decision; every calibration-round correction; every kill (gate-failure or budget); every halt. One retrospective per trigger in supervised phases; batched (but still per-item) in cruise.

### 11.2 Evidence

The full build trace (the factory logs its own episodes: prompts in, artifacts out, gate reports, judge critiques — it does not depend on mining Claude's transcript format, though transcripts remain available), the brief and its concept-judge scores, the decision + feedback, the current artifact versions (git SHAs), and the lessons ledger (has this happened before?).

### 11.3 The root-cause taxonomy — route to the artifact that should have prevented it

| # | Layer | Symptom pattern | Fix target | Applied how |
| --- | --- | --- | --- | --- |
| 1 | Design direction | Alex's reason states a taste rule no artifact encodes ("we don't do horror") | `design-direction.md` | **Proposal to Alex** — taste is his; never auto-applied |
| 2 | Ideation | Concept itself rejected; duplicate; IP flag confirmed | Catalog exemplar (specific) → ideation prompt (general) | Auto, validated per 6.2 |
| 3 | Brief | Build faithfully delivered a brief that underspecified the thing Alex flagged | Brief rubric | Auto, validated |
| 4 | Build craft | Render diverged from brief, or violates a rule `author-skin` already states | Stage prompt (lesson) and/or `author-skin` edit | Auto for stage prompts; skill edits as reviewed commits |
| 5 | Deterministic gate gap | **Alex caught something a machine could have** (a measurable seam, a contrast, a budget) | New/updated check, with tests | Spawned as a repo task; the highest-value route — converts one human review into a permanent rail |
| 6 | Judge miss | Judges passed what Alex failed (or inverse) | Judge rubric — **but only in a calibration pass** | Queued for next calibration; never edited mid-production (6.2 hard rule) |
| 7 | Platform | Renderer/tooling bug or expressiveness limit | Repo issue / spawned task | Human-routed; not a prompt fix |

Routing preferences when several layers could absorb a fix: **most upstream cause wins** (a bad concept should die in ideation, not be polished in build), and **durability wins** — gate > skill/prompt > exemplar, because a gate never forgets. The worked precedent: all four items in the `skin-authoring-constraints` memory (schema-can't-pattern, op ceiling, perceptual-distance-not-contrast, clip-shape blindness) were layer-4/5 lessons learned by human pain; under this system each would have become a skill edit plus, for two of them, new gates — exactly the diffs this engine automates.

### 11.4 Specific → general promotion

Every retrospective writes at least a **specific** lesson (`lessons.jsonl`: id, trigger, layer, artifact, text, evidence links, status). Generalization is earned, not assumed: when ≥ 3 active lessons share a root-cause signature, a promotion pass synthesizes the general rule, applies it to the target artifact, and marks the specifics `promoted` (kept for audit, dropped from prompts). This is the AWS-DevOps-Agent "learned skill" pattern, and it directly encodes the requested ordering: *generalized lessons are better than specific ones, but specific ones are better than none.*

### 11.5 Validation and safety rails

- Every auto-applied edit runs its artifact's validation gate (6.2) before landing; a failed gate turns the edit into a proposal for Alex.
- Judge edits: only in calibration passes; validated on the frozen labeled train split with **zero holdout regression tolerated**; the retrospective that *identifies* a judge miss and the calibration pass that *fixes* it are separate runs — the engine never tunes a judge while a specific skin's fate is pending.
- **Repeat root cause = assimilation failure**, the loop's most important alarm: the same signature recurring after its lesson was applied (twice in any 10 decisions) triggers a meta-retrospective — was the lesson routed to the wrong layer? Written but not surfaced into the prompt context? Too specific? — whose subject is the assimilation engine's own routing, and whose output goes to Alex if it can't resolve itself.
- Append-only audit log: one entry per retrospective and per applied/proposed diff — trigger, evidence links, layer, artifact, diff SHA, validation result. Halts (`# HALT`), recalibrations, and GEPA runs log at the same level.

### 11.6 Bootstrap from conversation history

Before the first calibration round, a one-time mining pass over `~/.claude/projects/-Users-alex-Snaketron*/` transcripts and the distilled memory files extracts: every skin-related correction Alex has given (→ seed lessons, pre-routed by the taxonomy), every verdict-shaped statement (→ seed labeled examples, marked lower-confidence than queue decisions), and prior concepts (→ catalog). This is what the conversation history is *for* — it warms every store the loop reads, without pretending to be a metric.

## 12. The loop (feature 5)

### 12.1 Phases

| Phase | Gate to enter | WIP cap | Review | Exit criteria |
| --- | --- | --- | --- | --- |
| **M0 Calibration** | Bootstrap mining done; workspace scaffolded | 13 (the calibration briefs) | Alex reviews every render, every round (Training 1–3) | Eval skins done to Alex's bar; ≥ 40 labeled decisions; first judge GEPA run complete |
| **M1 Interview + hardening** | M0 exit | — (no new builds) | Interview batches | Disagreement clusters resolved or accepted; judge holdout κ ≥ 0.7; execution GEPA run 2 complete; holdout briefs pass gates first-try |
| **M2 Supervised production** | M1 exit + platform slice (10.3) live | 5 pending | Every skin; retrospective per decision before the slot refills | Trailing-20 approval ≥ 60% AND zero repeat root causes in last 20 |
| **M3 Cruise** | M2 exit | 10 pending | Alex on his schedule; batch retros | **approved_count ≥ 100** → done; or a halt |

### 12.2 One M3 tick (Hermes cron, e.g. every 2h)

1. Poll the decision feed; for each new decision: record → retrospective → assimilate → update catalog and metrics.
2. Check halts (12.3). If any fire: pause submissions, notify Alex, stop the tick.
3. If pending < WIP cap and daily budget remains: ideation cycle → build → self-review → submit (or kill → retrospective).
4. Append the tick summary to the audit log.

### 12.3 Halt rules

| Condition | Action |
| --- | --- |
| 3 consecutive rejections | Pause; if without feedback → request feedback / trigger interview |
| 5 consecutive kills (gate failures) | Halt: execution regressed; run holdout regression to bisect (recent lesson? platform change?) |
| Trailing-20 judge–Alex agreement < 60% | Pause submissions; recalibration pass |
| Repeat root cause (11.5) | Meta-retrospective; escalate to Alex if unresolved |
| Daily cost > $75 or global > $2,500 | Stop until Alex raises the cap |
| Queue stale > 14 days (nothing reviewed) | Idle politely: stop building at WIP cap; never spam the queue |
| 3 halts in 7 days | Full stop + summary to Alex — something structural is wrong |

All caps and thresholds live in `skin-factory/config.yaml`; the driver enforces them mechanically.

## 13. The Hermes skill

`hermes/skills/snaketron-skin-factory/SKILL.md` (in this repo), installed by category symlink, matching the agent-harness convention:

```bash
ln -sfn ~/Snaketron/hermes/skills ~/.hermes/skills/snaketron
```

The skill is the **driver SOP**, deliberately thin: preflight (repo present, `claude` CLI, LM Studio up, budgets file readable, dependencies for the current phase), the state machine of section 12 over `skin-factory/state/`, dispatch commands (`claude -p` invocations per stage, judge API calls), halt enforcement, and notification of Alex. All craft and judgment live in the artifacts the driver dispatches to — the Hermes-local model never makes a quality-bearing decision. Cron entry: every 2 hours in M3; manual invocation in M0–M2 ("run a calibration round", "process the queue").

The skill file ships alongside this PRD; it is the second artifact of this branch.

## 14. Metrics

| Metric | Definition | Why it matters |
| --- | --- | --- |
| Approval rate | Approvals / decisions, trailing 20 | The headline; phase gates read it |
| Repeat-root-cause rate | Recurrences of an already-lessoned signature per 20 decisions | **The self-improvement KPI** — measures whether assimilation works, not whether skins are good |
| Judge–Alex κ | Cohen's κ, trailing 20 + holdout | Licenses judges as μ; guards against drift |
| First-pass gate yield | Builds passing all deterministic gates without repair | Execution quality; GEPA run 2's macro effect |
| Cost / approved skin | Total spend ÷ approvals | Sustainability; expect $5–15 early, falling |
| Novelty floor | Min catalog embedding distance among last 10 approved | Uniqueness isn't drifting as the catalog grows |
| Queue latency honored | Never > WIP cap pending | The loop respects Alex's attention |

## 15. Cost envelope

Per attempt ≈ $3–10 (build-dominant); at a 60% steady-state approval rate, 100 approved ≈ 170 attempts ≈ **$500–1,700** production spend, plus 1–3 execution GEPA runs at $100–400 each and negligible judge-GEPA cost. Calibration (13 briefs × ~3 rounds) ≈ $150–400. Defaults in 12.3 are set so the whole program lands under $2,500 without Alex touching anything; all figures are caps-enforced estimates, not promises.

## 16. Risks

| Risk | Mitigation |
| --- | --- |
| Judges Goodhart — pipeline optimizes toward rubric, Alex drifts away | κ tracked on *every* decision forever, not just in calibration; < 60% pauses the line; judge model pinned |
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
4. **Trace distillation**: once ≥ 100 successful build traces exist, distilling the build stage onto a cheaper model is the standard cost lever. Not before.

---

## Appendix A — GEPA run configs (summary)

| Run | Genome | Instances | μ | μf | Holdout gate | Budget |
| --- | --- | --- | --- | --- | --- | --- |
| 1 Judges | rubric texts | labeled decisions (train split) | verdict agreement | Alex's reason vs judge critique | holdout agreement no-regress | ~$20, hours |
| 2 Execution | stage prompts | 13 calibration briefs | gate pass score + frozen craft judge | gate error text | 4 holdout briefs no-regress | ≤ $400, a weekend |
| 3 Ideation | ideation prompt + brief rubric | seed list | calibrated concept panel + novelty | judge critiques + collision report | reserved seeds no-regress | ≤ $100 |

## Appendix B — Eval-set split (proposed)

Calibration (13): space invaders, grateful dead bears, barber pole, fish, python, tron vehicle, boat, dragons, hello-kitty-style, anime, golf, thanksgiving, minecraft. Holdout (4): wizard wand, surfing, shark, christmas. Alex may amend before round 1; frozen after.

## Appendix C — Worked routing examples

1. *"The bears are cute but the snake reads as noise at game zoom."* → Layer 4 (craft: small-cell readability is a stated rule) **and** layer 5 candidate: a legibility gate — downsample the contact sheet to cell-size 7 and threshold on mark-scale/contrast metrics. Specific lesson now; gate task spawned; if two more skins draw the same reason, the promotion pass makes the gate mandatory.
2. *"Another space theme?"* → Layer 2. Catalog exemplar immediately; on recurrence, ideation prompt gains a diversity constraint over recent approvals' tag distribution.
3. *Judges 5/5, Alex: "the head rule is violated — first cells are mid-contrast."* → Layer 6 (queued for calibration pass) **and** layer 5: the head light/dark rule is measurable — it should be a validator check, not a judge opinion. Gate wins on durability.
4. *"Love it."* (approval, no feedback) → Light retro: record; check for near-miss gates (op count 194/200 → note for structure planning); no edits.
