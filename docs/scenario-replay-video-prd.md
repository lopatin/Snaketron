# PRD: Scenario Replay, Play of the Game & Trailer Video Pipeline

| | |
|---|---|
| **Status** | Draft v3 — research-grounded, adversarially reviewed, revised after build-1 post-mortem |
| **Author** | Claude (for Alex Lopatin) |
| **Date** | 2026-08-15 |
| **Tracking branch** | `claude/snaketron-replay-video-34de16` |

Every load-bearing claim about the current codebase was verified against the worktree at `fa37c90` (file:line citations throughout; Appendix D is the fact sheet). A five-angle adversarial review pass (codebase feasibility, product/UX, scoring heuristics, server systems, video pipeline) ran against v1; all confirmed findings are incorporated in this draft.

---

## 1. Summary

SnakeTron needs one deterministic **ScenarioPlayer**: a reusable component that plays scripted or recorded game moments through the *real* arena renderer — including the JS canvas effect layers (crash explosions, score celebrations) and the DOM addons (combo callout, boost meter) — under a **virtual clock** that supports slow motion and frame-exact stepping.

That one component powers three products:

1. **Tutorial & marketing embeds** — a strict upgrade of today's `TutorialScenePlayer`, adding effect parity and data-driven scenarios.
2. **Play of the Game** — a new section in the game-results modal that replays the match's best recorded moment with a slow-motion payoff. The server selects the moment via "style points" heuristics and ships a tiny replayable clip (anchor state + event/command stream, ~10–15 KB gzipped per 10 s) so every player sees the same play.
3. **Trailer videos** — a headless capture pipeline (Playwright + CDP frame-by-frame rendering) plus a `/snaketron-create-video` agent skill that assembles clips into high-production-quality promotional videos with ffmpeg (camera shake, speed ramps, punch-ins, transitions, titles, music).

The design leans on two existing strengths: the engine is **already deterministic and clock-injectable end to end** (`GameEngine::run_until(ts_ms)` takes caller time; the Rust renderer reads no clock at all), and the codebase's established idiom is *"pose real state, render with the real renderer, never draw look-alikes"* (stated independently in `client/src/tutorial.rs:1-12`, `RosterSnakeCanvas.tsx:14-22`, `crashExplosion.ts:56`).

---

## 2. Goals and non-goals

### Goals

- **G1** — A reusable, embeddable `ScenarioCanvas` React component + WASM `ScenarioPlayer` that renders scripted/recorded gameplay with full visual parity to the live arena (snakes, food, boost, walls, crash explosions, score celebrations, combo callout, optional boost meter) at any playback rate, including slow motion and frame-exact stepping.
- **G2** — A data-driven **scenario script** format (JSON, typed via the existing ts-rs export pipeline) that can express: posed initial states, per-tick command schedules for any number of snakes, pinned mode config, camera, and expected outcomes for CI.
- **G3** — **Play of the Game** in `GameOverCard`: server-side style-points scoring over the match's recorded event/command window, a single deterministic winner, a replayable clip delivered to all clients, played with a slow-motion payoff in the modal.
- **G4** — **Kill attribution** in the engine (cause of death: wall / out-of-bounds / enemy base / self / body-of-snake-X / head-to-head / **banked**), required by G3 and valuable on its own (stats, future killfeed).
- **G5** — A **capture CLI** that renders any scenario/clip to frame-perfect 60+ fps video masters headlessly, with a per-clip event timeline (`meta.json`) for downstream effect placement.
- **G6** — A **`/snaketron-create-video` skill** that takes a prompt + sequence description and produces a finished trailer **that looks like SnakeTron**: EDL-driven ffmpeg assembly, effect vocabulary (shake, punch-in, speed ramp, glow, chromatic aberration, transitions, titles), music with beat-synced cuts, a cheap preview→final iteration loop, and an art direction bound to the game's own visual system (§5.7) rather than an invented one.

### Non-goals (this project)

- **Live-game slow motion.** Committed state is server-driven and prediction is capped ~1.5 s ahead (`common/src/game_engine.rs:492-497`); a slowed live clock would pin against real server ticks. Slow motion exists only in offline/replay contexts where the client owns the clock.
- **Full-match replays / spectator DVR / scrubbing UI.** The clip contract supports longer windows later, but the product scope here is ≤ ~10 s highlights.
- **In-game audio.** The game today has zero audio assets and zero audio code (verified: no audio files, no `Audio`/`AudioContext` usage in `client/`). The video skill ships its own music/SFX story (§5.7); adding game audio is a separate project.
- **Sharing/exporting clips from the client** (download/social). Future work; the clip payload and ACL decision (§5.3.3) are designed not to preclude it.
- **Migrating existing tutorial scenes** to scenario scripts. The 9 hand-posed scenes keep working unchanged on the shared player core; migration is optional cleanup later.
- **Play of the Game for Custom games.** Custom games allow non-canonical arenas and tick durations with no validated bounds (`CustomGameSettings`, `game_state.rs:961-987`); PotG launches on canonical Solo/FFA/Team modes only.

---

## 3. Where the codebase is today (verified)

Facts that shaped this design; citations in Appendix D.

1. **The Rust render/simulation stack is already virtual-time.** `render_game_state` is a pure `GameState → pixels` function with zero clock reads; `GameEngine::run_until(ts_ms)` / `rebuildPredictedState(ts_ms)` take caller-supplied time; the JS effect modules (`drawCrashExplosions`, `drawScoreEffects`) take `now` as a parameter. The *only* wall-clock coupling is in `useGameEngine.ts` / `GameArena.tsx` (the live loop) and a handful of CSS animations.
2. **Crash explosions are canvas-drawn, not DOM** (`crashExplosion.ts`, composited above the WASM frame at `GameArena.tsx:957-967`, in full-arena canvas coordinates). The genuinely DOM-based addons are the combo callout, boost meter/HUD, and modal chrome. This PRD covers both layers.
3. **`TutorialScenePlayer` is the extracted render core we need** — scratch canvas + camera crop + production renderer + injected `elapsed_ms` (`client/src/tutorial.rs:749-898`). It lacks: the crash-explosion layer, DOM addons, data-driven scenarios, engine stepping, and a playback-rate control (trivial).
4. **`GameArena` is not embeddable** — route/auth/socket/window-coupled (`GameArena.tsx:240-295, 611-701`). We extend the tutorial player's architecture, not GameArena.
5. **The engine is deterministic and headless-provable.** Seeded xorshift64 RNG lives inside serialized state; RNG feeds food spawns only; every HashMap/HashSet iteration hazard is explicitly sorted (e.g. `game_state.rs:3224-3229`); `server/tests/sync_equivalence_test.rs` runs 60-virtual-second games bit-reproducibly; `common/src/replay.rs` (`ServerReplay`/`ClientReplay`) proves both replay contracts. WASM-boundary↔engine equivalence is asserted (`client/src/lib.rs:764-834`; the test drives the wasm-bindgen API natively under `cargo test`).
6. **No replay recording is live anywhere.** Both server recorders are dead code (the `.replay` recorder was never wired; the `GameTraceRecorder` flight recorder was orphaned by the executor-v2 rewrite — DEBUGGING.md's server-trace section is stale). `GameEngine.event_log` is declared but never appended. What *is* alive: the client's `syncTrace` ring buffer (20k records incl. join snapshot + every event/command) and the trace-upload endpoint.
7. **Kill attribution does not exist.** The collision loop knows the killer and discards it (`game_state.rs:3213-3218`). Wall/base/OOB/snake branches are already distinct. **`SnakeDied` has two emission sites**: the collision path (`:3241`) and the team-banking path (`:3482-3487` — banking kills and immediately respawns the banker); any death-cause design must cover both.
8. **The live database is DynamoDB** (no postgres dependency in `server/Cargo.toml`; `server/migrations/` is legacy). Completion is a fenced, idempotent record — `CompletionRecordV1` already embeds the full final `GameState` — applied Valkey→DynamoDB (`server/src/completion.rs`). `validate()` hard-fails on schema-version inequality and is re-run on every read/apply (`completion.rs:84-89`; `game_bus.rs:2012-2014`), which constrains how new fields may be added (§5.3.3). There is no per-event persistence and no history API.
9. **Game end is not a message; it's replicated state.** Clients notice `GameStatus::Complete` in committed state. Post-match rating already uses a **REST poll with a `pending/ready/unavailable` state machine** (`useMatchRating.ts`) — the delivery pattern Play of the Game will reuse.
10. **Event publication is split across paths.** Gameplay events go through `publish_event_at` (`game_executor_v2.rs:1291-1343`), but command decisions (`CommandScheduledV2`/`CommandRejected`) go through a separate fenced path (`publish_outcome` → `publish_command_decision_fenced`, `:1237-1289`), and **the terminal tick's event batch is deliberately dropped** — the fenced completion transaction publishes one final snapshot instead (`:1729-1737`; `publish_event_at` no-ops once terminal, `:1291-1298`). Any recording design must tap all of these (§5.3.1).
11. **The drain e2e harness is the sanctioned no-backend capture path** (mock fetch + MockWebSocket + real WASM), and `capture-rating-reveal.mjs` is the precedent for a standalone Playwright-library capture script. Neither produces video; no ffmpeg exists in the repo.
12. **No sub-cell interpolation exists anywhere** — motion is whole-cell per tick, deliberately (`tutorial.rs:92-107`). This is the single biggest visual-quality constraint on slow motion (§5.1.5).
13. **Team matches render per-player rotated** — team 0 sees 270°, team 1 sees 90° on desktop (`GameArena.tsx:773-777`); rotation 0 is only used on short/wide screens. Clip framing must account for this (§5.5).

---

## 4. Product requirements

### P1 — ScenarioPlayer embeds

- **P1.1** As a developer, I can mount `<ScenarioCanvas script={...} timeScale={0.25} />` anywhere in the React app (tutorial modal, results modal, marketing pages, QA routes) with no auth, socket, or route dependency.
- **P1.2** Playback is deterministic: the same script renders the same frames on every machine, every run (this is what makes clips consistent across players and makes video capture reproducible).
- **P1.3** Visual parity with the live arena: everything the live game draws for a given `GameState` appears, including crash explosions, score celebrations, on-food combo value labels, boost skins/contours; DOM addons (combo callout, boost meter) are opt-in per embed.
- **P1.4** Playback controls: play/pause, replay, per-segment `timeScale` ∈ [0.1, 4] (speed ramps are first-class — PotG uses one), seek-to-time (scrubbing capability at the API level; a scrub UI is not required).
- **P1.5** Honors `document.hidden` pause/rebase, DPR ≤ 2, resize. Under `prefers-reduced-motion`: renders a poster frame, and an **explicit play click does animate** (user-initiated motion is the accepted a11y pattern). Note this deliberately goes beyond the tutorial player, which today renders the poster unconditionally with no path to animate (`TutorialSceneCanvas.tsx:103-106`).
- **P1.6** A dev-only `/qa/scenario-player` route (gated exactly like `/qa/rating-reveal`) lists scenarios, supports **URL-parameterized selection** (new; needed for scripted capture), and exposes the capture contract (§5.6.1).

### P2 — Play of the Game

- **P2.1** As a player finishing an eligible match, I see a "Play of the Game" section in the results modal: the star player's name/skin, a caption of *why* it was the play (e.g. "Boosted cut-off — 2 eliminations"), and the moment replaying with a slow-motion payoff.
- **P2.2** Every participant sees the **same** play — selection is server-side, recorded once per game, and rendered in one canonical framing (star-oriented; §5.5).
- **P2.3** The section degrades gracefully at every stage: no qualifying moment, version mismatch, fetch failure, **or a replay/render failure after start** (band collapses to poster, replay control hidden) ⇒ never a frozen half-played band. Reuse the rating band's `pending → ready | unavailable` pattern with a bounded poll.
- **P2.4** The clip is a 9 s window with a speed ramp: 1× into the moment, **0.5× through `[focus − 2 s, focus + 1.5 s]`**, 1× out — with focus at the 6 s mark that is 4 s @ 1× + 7 s of slowed footage + 1.5 s @ 1× = **12.5 s of viewer time, payoff on screen 8 s after playback starts**. (0.5× is the floor for v1: cell-stepped motion degrades below it — §5.1.5.) Autoplays once when visible; replayable on click; ends frozen on the final frame with a replay overlay. Muted concept (the game has no audio). Reduced-motion users get the focus-tick poster + play button.
- **P2.5** Selection is fair and legible: heuristics balance eliminations, banking, combos, and frenzy so different playstyles can win (§5.4); teammate kills and AFK-farming score no glory; tie-breaks are deterministic; a "minimum interestingness" threshold prevents embarrassing PotGs in dud games.
- **P2.6** Solo: eligible via the combo and frenzy categories (boost modifiers are off in Solo — its boost is unlimited, so "boosted" carries no style; frenzy keeps the ≥ 8-pickup bonus). The solo modal has no rating band (`ratingReveal.ts:23-26` returns null for Solo), so PotG is the modal's hero band there.
- **P2.7** Embedded builds (CrazyGames/itch): autoplay is additionally gated on the ad overlay being idle — game completion triggers `requestAd('midgame')` on CrazyGames (`GameArena.tsx:742-747`), and the one-shot autoplay must **defer, not burn**, while `adState !== 'idle'`.

### P3 — Trailer video generation

- **P3.1** A maintainer can run one command to render any scenario (or captured clip file) into a frame-perfect video master + event-timeline `meta.json`, headlessly, from a fresh clone, with documented setup (including the network-neutralization and font requirements in §5.6).
- **P3.2** `/snaketron-create-video <prompt>` produces a finished MP4 (1080p60, yuv420p, faststart) from a natural-language sequence brief like the launch-trailer example in §5.7.6, using scenario clips it captures (or reuses from the clip library) plus title cards, transitions, effects, and music.
- **P3.3** The skill supports an iteration loop: low-res preview renders in seconds, segment-level caching, and a vision-based self-review pass before the final render.
- **P3.4** Slow-motion shots are captured at high virtual frame rates (frame-exact), never synthesized by interpolation, wherever the scenario is re-runnable; the EDL compiler enforces this (§5.7.2).
- **P3.5 (brand fidelity)** Every frame that is not live gameplay — title cards, lower thirds, stat cards, end slate — must read as the same product as the gameplay it surrounds. Concretely: it is built from the app's own visual system (§5.7 Art direction), on the app's paper ground, in fonts the app actually ships, using the app's construction language. A viewer must not be able to tell which frames came from the game and which were made for the trailer. **Sampling the palette is not sufficient** — the same six tokens applied at inverted value polarity produce a different-looking product (this is the exact failure mode observed in the first build).
- **P3.6 (the payoff is on camera)** *This is the non-negotiable one.* Every participant in a shot's advertised event — for an elimination, **both the killer and the victim**; for a bank, the banker and the goal line — is inside the camera rect for the whole beat from approach through payoff. Build 1 shipped a 7-second "DEMOLITIONS!" hero shot in which the victim was **never on screen in any frame**: the Follow camera tracked the killer's head while the kill happened 3 cells below the bottom edge, so the 4× ramp, shake, RGB split, impact SFX and gold callout all landed on empty grid. The capture only recorded the *star's* head into `meta.json`, so nothing downstream could notice.
  - **Enforced at capture, not in review:** `capture.mjs` resolves every event in the scenario's `expect`/cue track to a cell, tests it against the camera rect on the frame it occurs, and **fails the capture** if it is outside — with the offending cell, tick, and rect in the error. A shot whose payoff is off-frame must not become a file.
  - `meta.json` records the position of **every** snake involved in a cue, not only the star's head, so effects and QC can reason about the event rather than the camera subject.
- **P3.8 (copy and data truthfulness)** Every string burnt into the output is either (a) a string the product itself renders, verified by repo grep, or (b) marketing copy recorded in `brand.md` with an approver. The product's tagline is **"Competitive multiplayer Snake"** (`GameStartForm.tsx:265-267`); build 1 shipped an invented **"OWN THE GRID"** that appears nowhere in the repo. Storyboard strings in this PRD (`"DEMOLITIONS!"`) are shot *intent*, not approved copy. **No fabricated player names, ratings, ranks, or ladder rows may appear in published output** — build 1's leaderboard card shipped four invented players and MMR values as a real ladder. Use real anonymized data, or placeholders the copy identifies as such. The trailer also uses the product's **real wordmark** (`client/web/SnaketronLogo.png`, used in five components) rather than drawing a new one.
- **P3.7 (composition)** At the focus frame the subject's bounding box occupies **≥ ⅓ of frame width** and sits on a thirds line, not dead-center-small. Cameras take an explicit cell window; the FOV is a per-scenario field, never a module constant. Shots carry scale references — food, a wall, a team zone, or an opponent must be in frame; a scenario with `rng_seed: null` has **zero food** (`game_state.rs:2860-2863`), which in build 1 left the hero shot 93% blank paper. Any HUD an embed opts into must actually be inside the captured viewport (build 1's `boost_meter: true` rendered below the 1080-px viewport and was silently clipped). Post-stage `punch_in` is a center crop and cannot reframe a bad plate — it is a garnish, not a fix.

---

## 5. Technical design

### 5.1 ScenarioPlayer component

#### 5.1.1 Architecture

Extend the `TutorialScenePlayer` architecture (scratch canvas + camera crop + production renderer), not GameArena. Three layers:

```
┌─ React: <ScenarioCanvas>  ──────────────────────────────────┐
│  virtual clock (owns time; per-segment timeScale, pause,    │
│  seek); rAF loop → elapsed_virtual_ms → WASM renderFrame    │
│  DOM addons (opt-in): <ComboCallout>, <BoostMeter>          │
│  capture contract (QA route only): §5.6.1                   │
├─ WASM: ScenarioPlayer (client/src/scenario.rs) ─────────────┤
│  frame source A: posed keyframes (existing tutorial scenes) │
│  frame source B: engine-stepped (script or recorded clip)   │
│  full-arena scratch render with TWO JS callback slots:      │
│    mid-frame celebration slot (existing, render.rs:2115) +  │
│    NEW post-snakes slot for crash explosions — both fire on │
│    the scratch canvas so full-arena effect coordinates stay │
│    valid, then camera-crop into the target                  │
│  camera (fixed / follow / authored) + rotation + crop       │
│  cue track export (crashes, goals, combo pickups, per-frame │
│  head positions) for effects, capture meta, SFX placement   │
├─ common: engine + scenario loader ──────────────────────────┤
│  GameState::tick_forward — already pub, deterministic       │
│  common/src/scenario.rs — ScenarioScript loader (must live  │
│  in common: posing boost charge touches pub(crate) fields)  │
└─────────────────────────────────────────────────────────────┘
```

The **post-snakes callback slot is a required renderer addition**: today `drawCrashExplosions` composites in full-arena canvas coordinates after the finished frame (`crashExplosion.ts:268-269` hardcodes the arena's 1 px padding), which is wrong on a camera-cropped target. Firing it on the scratch canvas before the crop (mirroring the existing celebration slot) keeps the effect module unchanged.

#### 5.1.2 Frame source B: engine stepping (the new part)

A new WASM type (`ScenarioPlayer`, in `client/src/scenario.rs`) that:

- Constructs a `GameState` from a **scenario script** (§5.2) or a **recorded clip** (§5.3.2) via the `common` loader. Validation via the existing `validate_boost_invariants` gate.
- Steps the **committed state directly** with `GameState::tick_forward` (already `pub`, `game_state.rs:3005-3007`), indexed by tick. This deliberately bypasses `GameEngine`'s committed/predicted split and its **hardcoded 500 ms committed lag** (`game_engine.rs:110`) — a replay has no server to lag behind. This is the one genuinely new WASM stepping binding.
- Applies scheduled commands via the existing `CommandQueue` mechanics with fully-specified `CommandId { tick, user_id, sequence_number }`. The engine performs no ownership check in `common` (ownership is a WS-boundary concern), so one driver puppets every snake — verified behavior, relied on by tests.
- For **recorded clips**, steps `movement_only=true` and applies the clip's replicated messages (gameplay events **and command-decision events** — boost/turns replicate only via commands; `BoostLifecycleTransition` is deliberately not a replicated event, `game_state.rs:390-399`) between ticks. This is the `ClientReplay` contract's mechanics — but note `ClientReplay`'s proof runs *through* `GameEngine::process_server_event` (`replay.rs:454-498`), so the ScenarioPlayer must not reimplement that loop blind: **extract the advance-then-apply helper from `GameEngine` and share it**, and additionally every `HighlightClip` embeds an `end_sync_hash` the player asserts after replay. A failed assertion is a P2.3 render-failure, not a silent desync.
- Exposes per-frame: `renderFrame(elapsed_virtual_ms, target_canvas)` (tick = `elapsed / tick_duration_ms`; monotonic stepping; seeks re-run from the anchor — cheap, ticks are microseconds), plus `cueTrackJson()` and `frameMetaJson(elapsed)` (per-snake head positions, boost/combo state) for the effect layers, DOM addons, and capture metadata.

**Mode split:** scripts run authoritative (`movement_only=false`, regenerating scores/respawns); clips run movement-only + event application (double-application hazard otherwise). The loader picks the mode; this mirrors exactly how `ServerReplay` vs `ClientReplay` already differ.

#### 5.1.3 Virtual clock & the CSS problem

The TS wrapper owns a `VirtualClock { start(), pause(), seek(ms), segments: [{until_ms, timeScale}] }`; `elapsed_virtual_ms = Σ (rAF_dt × active_scale)`. All canvas layers are already clock-injectable — the wrapper passes **virtual** `now` into `drawCrashExplosions`/`drawScoreEffects` and their sync functions (they take `now` as a parameter today; GameArena just happens to pass the rAF timestamp). Backdating math works unchanged in virtual time because both sides use the same clock.

CSS animations/transitions are the residue (combo-callout pop 360 ms, meter smoothing 50/100 ms — `GameArena.css:72,118,310`): they run on the browser's animation clock and ignore `timeScale`. Resolution, in order of preference:

1. **In-browser embeds:** drive them from the virtual clock via the Web Animations API — the wrapper finds the addon's animations (`element.getAnimations()`), pauses them, and sets `animation.currentTime = f(virtual_elapsed)` each frame. This is Remotion's internal pattern and keeps `ComboCallout` untouched for the live game.
2. **Fallback / v1 acceptable:** let ≤360 ms accents play at 1×. At 0.5× a 360 ms pop reads as a slightly snappy accent over a slowed scene — acceptable; documented, not blocking.
3. **Capture:** moot — CDP BeginFrame virtualizes compositor time, so CSS advances in lockstep with the stepped frames (§5.6).

#### 5.1.4 DOM addon parity

- `ComboCallout` is already a pure-props component; the wrapper feeds it from `frameMetaJson` (view-model `buildComboHudView` needs only `{window_ms, max_food_value}` + `{is_alive, combo}` — verified pure).
- The boost meter is currently **inline JSX in GameArena** (`GameArena.tsx:1649-1703`) — extraction into a `BoostMeter` component is an M0 task (mechanical; view-model `buildBoostHudView` is already pure).
- HUD shell/scoreboard/countdown/touch controls are live-match chrome and are **not** part of scenario embeds.

#### 5.1.5 Slow motion quality: the interpolation decision

There is no sub-cell interpolation; at slow motion a snake steps cell-by-cell (a normal-speed snake moves every 100 ms ⇒ 0.5× = one step per 200 ms of viewer time — this is why 0.5× is the P2.4 floor for v1). Two-stage plan:

- **v1 (PotG ships with this):** accept cell-stepped motion, lean into it — the *effect layers* have millisecond granularity, so the money moment (explosion unfolding over 780 ms → ~1.6 s at 0.5×, score wave, combo burst) is smooth even while snake motion steps. The P2.4 speed ramp keeps the slow segment short (3.5 s of game time).
- **v2 (before trailer-grade slow-mo shots):** optional presentation-layer interpolation — render snakes at fractional offsets by extending head/tail along the travel direction by `frac = (elapsed mod tick_ms) / tick_ms`. This is a renderer-level addition (`render_game_state` gains an optional `interp` parameter; compressed-body geometry makes head/tail extension well-defined; turns snap at cell boundaries as they must). Strictly presentation: simulation stays cell-quantized; the flag is never set in live play. This is the highest-risk renderer change in the project; isolated behind a flag with pinned-environment golden-frame tests (M3's capture rig — see M1 note on state-level vs pixel goldens).

#### 5.1.6 Camera

Port the tutorial `Camera` (fixed crop) and add: `Follow { snake_id, deadzone, ease }` and authored `Track { keyframes }`, plus a **rotation parameter** (the underlying `render` already takes rotation) and an **aspect parameter** (tutorial embeds 16:10; PotG band per §5.5; capture masters 16:9 with letterboxing policy owned by the camera). Follow-camera on a snake that dies: **hold the star's final head position** for the remainder of the clip (no cut). Camera outputs a crop rect in (rotated) cell space; the WASM side renders-full-then-crops (`render_scene` precedent, `tutorial.rs:749-817`). Trailer punch-ins/zooms happen at capture/post time (§5.7) — the component's camera stays simple.

### 5.2 Scenario script format

JSON, schema-typed end to end: a Rust `ScenarioScript` struct **and its loader in `common/src/scenario.rs`** (ts-rs-exported like every other shared type) so scripts are authorable in TS with generated types and loadable identically by the WASM player, the native CI runner, and the offline tuning harness. The loader must live in `common`: posing boost charge touches `pub(crate)` fields (`Snake.boost`, `speed_milli`, `movement_credit` — `snake.rs:114-119`) that neither `client/src` nor test code outside the crate can reach, and this PRD does not open that surface.

```jsonc
{
  "format_version": 1,
  "id": "boost-cutoff-demo",
  "world": {
    "game_type": { "TeamMatch": { "per_team": 2 } },   // GameType
    "queue_mode": "Quickmatch",
    "rng_seed": null,              // null ⇒ no food spawning (deterministic pose-only food)
    "overrides": {                 // WHITELISTED fields only — see note below
      "combo": { "window_ms": 2000, "max_food_value": 3 },
      "boost": { "speed_milli": 1500, "capacity_ms": 3000 },
      "player_idle_timeout_ms": 600000   // scripted snakes must not idle-kick mid-scene
    }
  },
  "pose": {
    "snakes": [                    // compressed bodies (head, turns, tail) — snake.rs contract
      { "user_id": 1, "name": "YOU",   "body": [{"x":18,"y":20},{"x":10,"y":20}],
        "direction": "Right", "food": 3, "boost_charge_ms": 3000 },
      { "user_id": 2, "name": "RIVAL", "body": [{"x":30,"y":24},{"x":30,"y":31}],
        "direction": "Up", "food": 0 }
    ],
    "food": [{"x":26,"y":20},{"x":27,"y":18}],
    "team_scores": [[0, 12],[1, 9]],
    "start_tick": 0
  },
  "commands": [                    // script-level schema; loader maps user_id → snake_id,
                                   // synthesizes CommandIds, and builds real GameCommands
    { "at_tick": 10, "user_id": 1, "command": "ActivateBoost" },
    { "at_tick": 28, "user_id": 1, "command": { "Turn": "Down" } },
    { "at_tick": 34, "user_id": 2, "command": { "Turn": "Left" } }
  ],
  "run_ticks": 120,
  "presentation": {
    "camera": { "Follow": { "snake_id": 1 } },
    "default_time_scale": 1.0,
    "star_snake_id": 1,
    "addons": { "combo_callout": true, "boost_meter": false }
  },
  "expect": [                      // optional; CI assertions, same style as engine tests
    { "SnakeDead": { "snake_id": 2, "at_tick": 41 } },
    { "FinalSyncHash": "0x9f2a..." }
  ]
}
```

- **Loader** = the sanctioned test pattern (`sync_equivalence_test.rs:709-742`): `GameState::new` → `add_player_with_team` per snake (tick-0-only, `game_state.rs:2639-2643`) → `status = Started` → apply pose → `validate_boost_invariants()`.
- **Overrides are a whitelist, not generic:** `validate_boost_invariants` is a strict mode-consistency gate — `available_food_target`, `score_limit`, tick duration, and the pad layout must all match the mode-derived values (`game_state.rs:1758-1846`). Permissible overrides: `combo`, `boost` (loader re-derives the pad layout from the overridden config, or pad validation fails), `player_idle_timeout_ms`. Nothing else.
- **No "forced outcome" mechanism** — outcomes are a pure function of pose + schedule; `expect` assertions catch drift when engine changes alter a scene (CI runs every checked-in scenario headlessly via the native engine).
- **Authoring aid**: `common/src/ai.rs::calculate_ai_move` is a pure function of state and can drive "extra" snakes (`"driver": "ai"` per snake, resolved to commands at load). Hero choreography iterates in `/qa/scenario-player` with hot-reloaded JSON.

### 5.3 Replay capture: getting real-game moments out of the server

#### 5.3.1 Server-side recording ring (new, small — and fed from the right places)

Play of the Game needs the match's events, commands, and periodic state anchors. **`publish_event_at` alone is not sufficient** — it is not the only publish path and it never sees the game's final moments (§3.10). The ring taps three feeds inside the game actor:

1. **Event batches inside `advance_live`** — captured from `run_until`'s raw event vec *before* the `terminal_pending()` gate, so the terminal batch (the match-deciding kill/bank, deliberately dropped from the bus at `game_executor_v2.rs:1729-1737`) is recorded. These terminal events exist nowhere else; the clip cutter synthesizes their envelopes (ring stores `(tick, sequence, event)` tuples, not bus envelopes).
2. **Command-decision events** from `publish_outcome` (`CommandScheduledV2`/`CommandRejected`, `:1237-1289`) — without these, replays contain snakes that never turn or boost.
3. **Decisions restored during recovery incorporation** (the restore path re-applies decisions without republishing).

Ring contents: gameplay + command-decision records only — no `Snapshot` republications, no `TickHash` probes. Plus an **anchor `GameState` snapshot every 5 s** (clone via the same machinery as the 1 s recovery checkpoint — cost precedent `server/src/recovery.rs:28-30`), **seeded with an anchor at activation** so the first seconds of a game are always clip-eligible.

**Coverage: the whole match**, with a hard memory cap. At the measured ~4–6 KB/s of raw event JSON plus anchors, a 10-minute 4-snake game is ~3–4 MB; cap the ring at **6 MB** (events + anchors counted together), evicting oldest-first. Under eviction, PotG degrades to "best moment of the recorded tail" — instrument `ring_evicted_seconds` so we know how often that happens in practice. The ring is executor-local, never persisted, and dies with the actor after completion processing.

**Failover reality (stated, accepted):** the recovery envelope carries no event history (`envelope()`, `game_executor_v2.rs:1369-1381`), so a game whose executor migrates or crash-recovers finishes with a truncated or empty ring — **PotG is absent for that game**, and completion during a deploy drain does the same (`commit_completion_until_handoff` cancels mid-flight completion, `:1828-1838`). This is an accepted v1 degradation: emit `potg_ring_truncated` telemetry, and note the future mitigation (rebuild the ring from the durable per-partition Redis event stream during recovery) without committing to it.

#### 5.3.2 Clip payload

```
HighlightClip {
  clip_format_version: u32,
  gameplay_version: u16,          // the engine-compat gate (see Versioning)
  game_id, star_user_id, star_snake_id,
  reason: HighlightReason,        // enum for the caption: BoostedCutoff{kills}, TrapKill,
                                  // GoalRun{points}, ComboFrenzy{max_chain}, ...
  score: u32,                     // winning style-points total (debug/telemetry)
  window: { start_tick, end_tick, focus_tick },
  anchor: GameState,              // nearest ring anchor ≤ start_tick (rng stripped)
  messages: Vec<ClipMessage>,     // gameplay events AND command decisions in
                                  // (anchor.tick, end_tick], tick+sequence ordered
  end_sync_hash: u64,             // asserted by the client player after replay (§5.1.2)
  presentation: { rotation, segments: [{until_tick, time_scale}], camera: Follow{star} }
}
```

- Replayed client-side movement-only + message application (§5.1.2). The client **fast-forwards silently from the anchor to `start_tick`** (≤ 5 s of ticks — microseconds) before presenting.
- **Anchor edge rule:** the scoring domain is constrained to windows whose re-centered clip start (`focus − 6 s`) is ≥ the oldest retained anchor's tick; with match-length coverage and an activation-time anchor this only binds under cap eviction.
- **Estimated size: ~70–100 KB raw / ~15–20 KB gzipped per clip** (anchor 4–8 KB + events ~4–6 KB/s over the 9 s window *plus* up to ~5 s of anchor→start pre-roll messages). RNG is stripped from the anchor (clients never spawn food; `FoodSpawned` events carry spawns — `game_state.rs:3343-3344`).

#### 5.3.3 Persistence & delivery

- **No completion schema bump.** Attach `play_of_the_game: Option<HighlightClip>` (gzip+base64) to `CompletionRecordV1` as `#[serde(default, skip_serializing_if = "Option::is_none")]` — **exactly the `season` precedent** (`completion.rs:62-66`, legacy-shape test `:707-713`). `validate()` is untouched; old servers deserialize records with the unknown-optional field safely (no `deny_unknown_fields`) and simply persist without the clip. A `COMPLETION_SCHEMA_VERSION` bump is ruled out: `validate()` is strict equality re-run on every read/apply, so a bump stalls XP/MMR/persist effects in mixed-version deploys draining the same Valkey queue. A new `CompletionEffect` variant is likewise ruled out (`#[serde(tag = "kind")]` — old servers fail deserialization of the whole record on an unknown kind).
- **Scoring is infallible by contract and isolated:** it runs on the actor task *before* completion materialization, is `Option`-valued, wraps its work (re-sim, flood fill, clip cut) so any error/panic/time-overrun yields `None` + telemetry — a highlight bug must never delay or fail XP/MMR/ranking (the completion path retries materialization forever at 1 s cadence; poisoning it is the worst failure mode in this design). CPU-capped (hard tick budget); the fenced Valkey commit including the clip must fit the existing 750 ms fenced-operation timeout — at ~15 KB gzipped this is comfortable, but it's the stated ceiling.
- **Read store: DynamoDB.** Extend the PersistGame effect writer (`dynamodb.rs:3027-3108`) to store the clip attribute alongside `gameState` (the completion record carrying the field does not by itself reach the queryable item). **Size rule:** measure the existing item for a long 4-snake game first; if `final_state` + clip approaches ~200 KB of the 400 KB item cap, the clip becomes its own item (`gameHighlight#<game_id>`) written by the same effect.
- **Endpoint:** `GET /api/games/{game_id}/highlight`, behind the existing JWT `auth_middleware` (`http_server.rs:255-266`); authorization = requester ∈ `final_state.players ∪ spectators` (per-resource authz is a new pattern in this server — there is none today — but membership data is in the stored state). This ACL is v1; it gets revisited when clip sharing (non-goal) becomes a goal.
- **Client:** a `useHighlight` hook cloned from `useMatchRating`'s poll pattern (bounded attempts, `pending → ready | unavailable`). In practice the completion transaction publishes the terminal snapshot itself (`game_bus.rs:1771-1780`), so the record is committed before the client even learns the game ended — the first poll usually hits.

#### 5.3.4 Versioning & compatibility

- A clip is replayable only by an engine that simulates identically. Stamp `gameplay_version` into every clip; the client hides the section on mismatch (P2.3). Skew is rare — clips are consumed seconds after creation — but deploy windows make it real.
- **The M0 engine change bumps the full constant set together** (per the hand-merge discipline in project memory): `WS_PROTOCOL_VERSION` (`server/src/lifecycle.rs:26`) + `GAMEPLAY_PROTOCOL_VERSION` (`client/web/constants.ts:11`) + **`EXECUTOR_PROTOCOL_VERSION`** (`server/src/cluster_membership.rs:36` — replicas consume the event stream cross-server; this is what keeps mixed-version clusters from misparsing the new `SnakeDied` shape) + `TRACE_FORMAT_VERSION` (`common/src/trace.rs:32`) + **`RECOVERY_SCHEMA_VERSION`** (`server/src/recovery.rs:27` — `SnakeCrash` is serialized state inside checkpoints). New fields carry `#[serde(default)]` so **stored** artifacts (old completion records' `final_state`, old snapshots) stay readable — the version constants gate the wire and cluster, serde defaults gate storage.
- Chaos suite (`sync_equivalence_test.rs`) must stay green — it is the declared regression barrier for any engine change.
- Clips embedded in *marketing pages* (long-lived, unlike PotG) are re-validated by CI via scenario `expect` blocks.

### 5.4 Play of the Game selection

#### 5.4.1 Engine prerequisite: death cause (G4)

Record what the engine already knows instead of discarding it — **at both `SnakeDied` emission sites**:

```rust
enum DeathCause {
  Wall, OutOfBounds, EnemyBase, SelfCollision,
  SnakeBody { killer_snake_id }, HeadToHead { other_snake_id },
  Banked,           // the team-scoring path kills-and-respawns the banker
}
```

- Collision loop (`game_state.rs:3189-3221`): cause from the already-distinct branches; killer id captured at `:3213-3218` instead of dropped.
- Banking path (`game_state.rs:3482-3487`): emits `Banked`. The scorer treats `Banked` as banking, **never** as anti-style — without this variant every bank would eat the death penalty and the banking category would be self-defeating.
- Carried on `GameEvent::SnakeDied { snake_id, cause }` and on the `SnakeCrash` cue. Fingerprint-safe: `sync_hash` hashes an explicit field allowlist (`fingerprint.rs:85+`) that does not include `recent_crashes`, so cue fields are excluded by construction. Version-bump set per §5.3.4.

#### 5.4.2 Scoring model

Runs at completion, server-side, over the actor's ring — pure function `score_windows(ring, config) → Option<Highlight>` in **`common/src/highlight.rs`** (unit-testable, reusable by the offline tuning harness and the video skill).

**Re-simulation contract (pinned):** the scorer re-simulates **ClientReplay-style** — movement-only stepping + application of the ring's gameplay *and command-decision* messages. This is deliberately the same contract the client's clip player uses (§5.1.2), so the scorer's geometry (boost state, positions, flood fill) is bit-identical to what players will watch. (Authoritative re-sim is not used: it would regenerate events and double-apply against the ring.)

**Window mechanics (tick-precise):**

- Windows are **half-open tick ranges** `[start, start + W)` where `W = 10_000 / tick_duration_ms` ticks, stepped by `1_000 / tick_duration_ms` ticks, clamped to the recorded span. A boundary event belongs to exactly one window by the half-open rule.
- Windows are **per star snake**. After the raw maximum is found, the clip is re-centered to `[focus − 6 s, focus + 3 s]` and **re-scored on the re-centered range; the re-centered score and its events are what the caption and threshold use.** This closes two v1 holes: a death just outside the scored window reappearing on screen uncaptioned, and a captioned second kill lying outside the visible clip. Caption facts must be events inside the clip range.
- `focus_tick` = the tick of the highest-value single scoring event in the window.

**Style events (for star S in the window):**

| Style event | Points | Detection |
|---|---|---|
| Elimination — enemy died to S | 90 | `SnakeDied { cause: SnakeBody { killer: S } }`, or `HeadToHead` where the victim ≠ S (non-mutual head-to-heads exist: only snakes that moved this quantum can crash, `game_state.rs:3191`) — **enemy only**: victim `team_id` ≠ S's team |
| Mutual head-to-head trade (both die same tick) | 60 each side | both `SnakeDied` at one tick, causes crossing |
| … S boosting at the kill tick | +40 | re-sim `boost.active`; **gated on the mode's boost not being unlimited** (Solo boost is free — `game_state.rs:1024-1033` — so this modifier is off in Solo) |
| … cut-off signature | +30 | S turned within 600 ms before the death **and** the turn placed S's body across the victim's travel ray (checkable from re-sim geometry). A bare "turned recently" test fires on almost every kill and is not used |
| … proximity: S's head ≤ 3 cells from victim's head at death | +20 | re-sim positions (victim at its rolled-back death pose, `game_state.rs:3230-3231`) |
| … trap | +30 | flood fill from the victim's death cell: obstacles = living snake bodies, walls, arena bounds, and (for the victim) enemy-base cells; **dead snakes excluded** (they are not collidable, `:3215`). Credit requires reachable area < 15 cells, shrunk ≥ 60% vs 2 s prior, **and S's body forming ≥ 40% of the enclosure frontier** (self-boxed victims award nothing). Static-snapshot heuristic; tail-following escapes are knowingly ignored |
| … victim was laden: +4 × victim's `carried_food` at death, cap +50 | +≤50 | re-sim (`carried_food`, `game_state.rs:2586-2591`) — *not* `scores[victim]`, which is cumulative career pickups and saturates in minutes |
| Multi-kill | ×1.5 (flat, 2+ kills) on the **base elimination points only** | — |
| Repeat-victim decay | 2nd kill of the same victim within 30 s ×0.5, 3rd+ ×0 | anti-farming/collusion |
| AFK discount | eliminations of a victim with no accepted command in the prior 10 s score ×0.25 | ring command decisions |
| Teammate kill | −80, and never elimination credit | `SnakeBody` with same-team victim |
| Points banked | +5 / point | re-simulated `recent_goals` cues (regenerated deterministically in the movement path, `game_state.rs:3382-3426`, with `snake_id` + `points`). **Not** "TeamGoal events" — `TeamGoal` is a cosmetic state cue, absent from `GameEvent`; the only replicated banking signal (`TeamScoreUpdated`) has no snake attribution |
| … single carry ≥ 15 points | +50 | one cue |
| Combo chain | +15 × (max chain − 1) | `FoodEaten.combo_chain` |
| … nick-of-time pickup | +10 each | **`0 < combo_remaining_ms_before < 250`** — the strict lower bound matters: `0` is the defined encoding for "this pickup *starts* a chain" (`game_state.rs:2330-2334`), not a clutch save |
| Boosted pickups | +5 each | `FoodEaten.boost_active`; off when boost is unlimited (Solo) |
| Feeding frenzy: ≥ 8 pickups in window | +20 | count |
| S died in the (re-centered) clip range | −60 | `SnakeDied(S)` with cause ≠ `Banked`. Exemption: a kill by S **at the same tick** as S's death (a true trade — all of a tick's collisions resolve in one pass, so trades are same-tick by construction; dead bodies can't kill later, and team-mode respawn is same-tick, so "after death" would be either impossible or a different life). Tick-based, never sequence-based (same-tick deaths order by victim id) |

**Winner:** max re-centered score ≥ **threshold 120**; below, no PotG. Tie-breaks: score → earlier `focus_tick` → lower `snake_id`. Deterministic throughout.

**Calibration notes (stated intent, validated by the harness):**
- A bare kill (90) deliberately does **not** clear 120; a kill plus any modifier worth ≥ +30 does (boost, cut-off, trap, or a laden victim carrying ≥ 8). Proximity alone (90 + 20 = 110) deliberately does not — closeness without another style element isn't a highlight. That's the design statement: PotG celebrates *how*, not just *that*.
- Banking ceiling math at the real score limits (25 Quickmatch / 50 Competitive, `constants.rs:69-70`): a 15-point carry = 75 + 50 = 125 ≥ 120 — a pure support play *can* win, barely and rightly. (v1's +3/point could mathematically never reach threshold; that's why these weights are +5/+50.)
- Modes without boost configs (some Custom setups) can't earn boost modifiers; Custom is excluded at launch anyway (§2).

**Config:** weights + threshold in a versioned `HighlightConfig` (modeled on `ComboConfig`'s snapshotted, never-process-mutable discipline, with a `rules_version`), stored inside the clip for telemetry.

#### 5.4.3 Balancing plan (the honest part)

The table is a starting point. The plan to make it fair:

1. **Corpus:** ~200 bot games (the `bot/` crate runs real AI matches) + dogfood games, recorded via a dev/staging-only env flag that dumps the ring at completion. Note the corpus inherits whatever the ring covers — with match-length coverage this is no longer ending-biased, but cap evictions are logged.
2. **Offline harness** (`cargo run --bin highlight_tune`): run the scorer, emit top window per game with reason + score breakdown; render the top 20 via the capture CLI; human-review ("would you be proud of this PotG?"); adjust; repeat. Explicit review criteria include the adversarial cases: staged kill-trading between colluders, teammate-kill griefing, AFK farming.
3. **Acceptance gate:** ≥ 80% of reviewed PotGs judged "deserved"; no single category > ~60% of winners in team modes; ≥ 70% of non-trivial games (≥ 2 min, ≥ 2 active players) produce a PotG — **met within ≤ 3 tuning rounds, or PotG ships dark behind its server-side config until met** (the escape hatch is a kill-switch, not a lowered bar).
4. **Post-launch telemetry:** `HighlightReason` + score distributions (PostHog) to monitor category skew; weights are server-side, tunable without a client release.

### 5.5 GameOverCard integration

- **Slot:** a full-width band between `RatingReveal` (`GameOverCard.tsx:228`) and the statline (`:230`) — the card is a stack of full-width rows in a `min(560px, 100%)` column.
- **Framing & legibility (decided by arithmetic, verified by mockup):** the card caps at `max-height: min(680px, 100dvh − 40px)`; a 21:9 band at 560 px is 240 px tall. The camera shows a **follow window of ~26×11 cells** ⇒ ~21.5 px/cell at 560 px — matching tutorial density (~21–23 px/cell) — and ~14.4 px/cell at 375 px mobile. M2 carries an explicit legibility exit check (combo food label readable at both sizes; if it fails, in-clip combo labels are dropped at small sizes rather than the framing changed). The follow deadzone is tuned so vertical cut-off maneuvers don't pump the camera; if the mockup shows 11 cells of height starving two-axis plays, the fallback is 16:10 collapsed-by-default. Decide from the M2 mockup, not from modal height alone.
- **Rotation — star-oriented canonical framing:** desktop team players live the match rotated (team 0 at 270°, team 1 at 90° — `GameArena.tsx:773-777`). The clip renders in **the star's live orientation for all viewers**: everyone still sees the identical play (P2.2 intact), and the one player most likely to scrutinize and share the clip sees the geometry they actually played. The camera crops in rotated screen space (§5.1.6). The cost — non-star viewers see the star's orientation rather than their own — is accepted and stated.
- **Timing:** autoplay fires when **all** of: highlight `ready`; rating band **settled, `unavailable`, or absent** (an `onSettled` callback added to `RatingReveal`; "unavailable after poll exhaustion" must count — keying on mode alone would strand autoplay when a Competitive poll times out); band actually visible (IntersectionObserver — short viewports scroll internally and landscape-mobile cards are ~335 px tall; a below-fold autoplay burns the one-shot unseen); and, on CrazyGames, `adState === 'idle'` (P2.7 — completion triggers a midgame ad over the app; defer, don't burn; M2 also verifies the `useHighlight` poll authenticates under the CrazyGames session). Pacing budgets as M2 exit criteria: **payoff on screen 8 s after playback start** (fixed by the P2.4 ramp), and **playback starts ≤ 4 s after modal open when no rating band is present**. On the Competitive path playback is deliberately deferred behind the rating reveal (which alone can take ~8 s when the poll runs long), so the modal-open budget doesn't apply there — instrument time-from-modal-open in telemetry instead.
- **Layout stability:** when the clip payload confirms, the band inserts as a poster-skeleton first (180 ms entrance, matching the card's animation language), then plays — no ~240 px reflow mid-read of the statline.
- **Interaction hazards (verified):** Space is globally captured as Play-again while the modal is open; the replay control must be a real `<button>` (exempted by `targetOwnsSpace`). New controls join the focus trap automatically.
- **Runtime cost:** a second WASM engine object + one more rAF loop while GameArena's two loops still run. Mitigations: idle (no rAF) until autoplay conditions met; pause on `document.hidden` and modal dismiss; `player.free()` on unmount (tutorial precedent). M2 exit criterion names the reference device: **mid-range Android (Moto G-class) or Chrome DevTools 4× CPU throttle — modal interaction stays ≥ 30 fps during clip playback.**
- **Failure containment (P2.3):** replay assertion failure (`end_sync_hash`), WASM error, or canvas context loss mid-playback collapses the band to the poster with the replay control hidden; the QA route includes a malformed-anchor fixture to exercise this path.
- **QA:** `/qa/play-of-the-game` fixture route cloned from `RatingRevealQA` (dev-gated, fixture clips checked in, scenario buttons + URL params), plus a capture script à la `capture-rating-reveal.mjs`. PR screenshots follow the `docs/screenshots/<slug>/` workflow.

### 5.6 Capture pipeline (CLI + clip library)

A standalone Node script using the Playwright **library** (generalizing `tests/capture-rating-reveal.mjs`), not a Playwright test.

#### 5.6.1 Page contract (capture mode)

`/qa/scenario-player?scenario=<id|file>&capture=1` exposes:

```ts
window.__scenarioCapture = {
  ready(): Promise<void>,   // resolves ONLY after: WASM instantiated, crash-sprite
                            // decode() settled, document.fonts.ready, addon image
                            // assets (rank medallions etc.) decoded, first frame posed
  stepMs(dt): Promise<void>,// advances virtual time; resolves AFTER canvas layers are
                            // drawn AND React DOM addon updates have committed
                            // (flushSync) — React 18 batching must not leave the DOM
                            // one frame stale in the screenshot
  renderedTick(): number,
  cueTrack(): ClipMeta,
}
```

The readiness barrier is load-bearing: the live game *tolerates* a cold sprite cache by drawing explosions without the fireball (`GameArena.tsx:549-557`, `crashExplosion.ts:292-294` silently skips) — fine live, fatal for determinism. Capture mode preloads eagerly and never starts before `ready()`.

In capture mode the page also: renders the scenario canvas **full-bleed with all app chrome suppressed** (`ArenaBackdrop` is a self-animating full-viewport canvas mounted on non-play routes — it must not be in frame), and **neutralizes the network** — every `/qa/*` route boots the full provider stack, which fetches `/api/regions` + per-region health pings on mount with a `localhost:8080` default that frequently *hangs* on dev machines (project memory: 8080 is usually something else). Capture mode installs drain-harness-style fetch/WS mocks (the sanctioned pattern) so no real fetch ever pends — unresolved fetches stall `Emulation.setVirtualTimePolicy` and make the fallback path nondeterministic. QA routes are dev-build-only (`App.tsx:28-30`), so the runbook explicitly requires the dev server.

#### 5.6.2 Frame loop

- **Primary (Linux/Docker):** the **`chrome-headless-shell` binary, pinned explicitly** — BeginFrame control and `--deterministic-mode` are old-headless features; since Chrome ~132 they live only in headless-shell, and the `HeadlessExperimental` domain is deprecated in new headless. The CLI probes with one `beginFrame` at startup and fails loudly with remediation if unsupported. Flags: `--deterministic-mode` (implies begin-frame control and all-compositor-stages-before-draw) plus **software raster pinned** (`--disable-gpu`). Per frame: `await stepMs(1000/fps_virtual)` → CDP `HeadlessExperimental.beginFrame { frameTimeTicks, screenshot: { format: 'png' } }`. BeginFrame virtualizes compositor time, so CSS-driven DOM addons advance in lockstep — the only approach that captures canvas + DOM frame-perfectly together.
- **Fallback (macOS dev machines):** `Emulation.setVirtualTimePolicy` budget-stepping + `page.screenshot()` per frame. Works everywhere Playwright works; not bit-deterministic (GPU raster, CoreText). The CLI auto-selects and records which path produced each master.
- **Viewport:** `Emulation.setDeviceMetricsOverride` 1920×1080, `deviceScaleFactor: 1` (the DSF-2 precedent from the rating capture script would silently produce 4K screenshots).
- **Fonts:** the arena renderer draws canvas text in system `"Arial Black", Arial` (`render.rs:285-287, 1464-1466`) and the app bundles no web fonts — a stock Linux container substitutes DejaVu and silently changes every label. The capture Docker image installs metric-compatible Arial/Arial Black (or the app gains a capture-mode `@font-face`); font parity is an M3 exit check.
- **Slow motion = high virtual fps:** capture at 120/240 vfps for 2×/4× slow-mo. Cost scales with it: a 10 s clip is 600 frames at 60 vfps (~1–2 min at 5–15 real fps) but **2,400 frames ≈ 3–8 min at 240 vfps** — cache accordingly.
- **Encode-and-delete in the loop:** pipe PNGs to `ffmpeg -f image2pipe -framerate N -i - -c:v libx264rgb -qp 0 master.mkv`. Masters are **lossless RGB** (`libx264rgb`) — PNG→yuv444p is *not* lossless (RGB→YUV rounding) and an untagged swscale conversion defaults to BT.601 while finals tag BT.709, shifting exactly the 1 px neon colors we care about. The single RGB→YUV conversion happens once, at final render, with explicit `scale=out_color_matrix=bt709` + color tags.
- **Determinism criterion (scoped honestly):** bit-identical re-capture applies to **the Docker BeginFrame path on one machine with software raster**. The macOS fallback and cross-platform comparisons use a perceptual criterion (per-frame SSIM ≥ 0.999 / max-delta threshold). Cross-platform bit-identity is impossible (fonts, raster) and is not claimed.

#### 5.6.3 Clip library

`tools/video/clips/<slug>/`: `master.mkv` + `meta.json`: scenario id + seed, **`capture_vfps` and encoded fps**, resolution, duration, capture path (beginframe|virtualtime), and the event timeline from `cueTrack()` (kills/banks/combos/boost with **timestamps in master seconds**, per-frame star head position). `meta.json` is what lets the EDL place effects and SFX on exact frames without watching the video.

**Fresh-clone runbook (tool README):** `wasm-pack build` first (webpack serves `client/pkg` via the `file:` dependency); `npm install`; `npx playwright install` (+ headless-shell); start the dev server (drain-config port or `npm start`); run the CLI. Worktree trap: `node_modules/wasm-snaketron` must resolve to *this* worktree's `client/pkg` (per-entry symlinks, not a whole-directory symlink) or the browser silently runs the main repo's stale WASM.

### 5.7 `/snaketron-create-video` skill

**Art direction (normative — this governs every other subsection).**

The codebase's own rule is *"pose real state, render with the real renderer, never draw look-alikes"* (`tutorial.rs:1-12`, `RosterSnakeCanvas.tsx:14-22`, `crashExplosion.ts:56`). **That rule extends to marketing graphics.** A hand-authored standalone HTML card that approximates the game's look is exactly the look-alike this codebase refuses to draw anywhere else.

1. **The ground is paper, not graphite.** The app is light-themed everywhere it matters: the arena clears to `#ffffff` (`render.rs:1583`), `body`, `.game-over-card`, and `.rating-reveal` all sit on `#fff`. Non-gameplay frames use the same paper ground. Graphite `#3f3f41` is **ink and rule**, never a backdrop — inverting that polarity is the fastest way to make the trailer look like a different game.
2. **The palette is the app's token block, used the way the app uses it** (`client/web/index.css:root`): `--game-paper #ffffff`, `--game-graphite #3f3f41`, `--game-blue #3b82f6`, `--game-red #ef4444`, `--game-rule #d1d5db`, `--game-muted #667085`, `--game-boost #f8c84a`; plus ink `#14181f` and the renderer's signature `NOS_ORANGE #ff641e` (`render.rs:80`) — the boost-canister accent that appears in nearly every gameplay shot and must appear in the graphics too.
3. **Typography is what the app ships**: `900 … "Arial Black", Arial, sans-serif` for headlines (the same stack the canvas renderer uses — `render.rs:286, 1465, 1519`), `Impact` for the heaviest stamps (`render.rs:1846`), and the system stack for utility copy. **No fonts the product does not use** — the app ships zero `@font-face`, so any webfont fails the allowlist. *Italic is used by role, not banned*: the logotype is bold italic per the checked-in design contract (`client/web/CLAUDE.md:7-9`, backed by 18 italic weight-950 rules on score numerals, e.g. `index.css:249`), while in-arena canvas text is always upright. An upright logotype is as much a brand error as an italic in-arena label.
4. **Construction language is precise, not heavy**: 1–1.5 px hairline borders, 6–7 px radii, soft elevation (`0 18px 48px rgb(31 41 55 / 22%)`), no skew, no 8–18 px hard offset shadows. Motion uses the app's own curves — `cubic-bezier(0.2, 0.8, 0.2, 1)` at 180 ms for entrances, `cubic-bezier(0.2, 1.4, 0.3, 1)` at 360 ms for stamps.
5. **Prefer the real component to a replica.** Where the app already renders the thing being advertised (rank reveal, leaderboard, results card), the fixture card mounts the **real component** with fixture props behind the capture contract (`RatingReveal` is pure-props and rAF-driven — verified). Bespoke HTML is permitted only where no component exists (the logo slate), and then it must import the app's stylesheet rather than restate colors.
6. **A distinct trailer identity is allowed only as an explicit, signed-off exception** — recorded in the skill's `brand.md` with the reason. Absent that, "placeholder" means *the game's own system*, never an invented one.

#### 5.7.1 Shape

```
.claude/skills/snaketron-create-video/
├── SKILL.md                  # workflow, EDL schema, effect vocabulary, guardrails
├── scripts/
│   ├── capture.mjs           # wraps the §5.6 CLI (scenario → clip master + meta)
│   ├── compile_edl.py        # EDL JSON → ffmpeg filtergraphs (LLM never writes raw graphs)
│   ├── beats.py              # librosa/aubio beat grid → music manifest
│   ├── render.py             # preview/final renders, per-segment cache
│   ├── brandcheck.py         # M4 gate 2: font allowlist, value polarity, palette ΔE
│   └── review.py             # frame strips from a render for vision QC
├── references/
│   ├── ffmpeg-recipes.md
│   └── brand.md              # the app's visual system (§5.7), NOT an invented identity
└── assets/                   # CC0 stingers + license manifest, LUT (identity by default)
                              # fixture cards live here only when no real component exists
```

Division of labor follows the best prior art (anthropics/skills' `slack-gif-creator` + `webapp-testing`; `remotion-video-director`'s LLM-emits-a-structured-plan): **the LLM composes a validated EDL; deterministic scripts own all arithmetic** (xfade offsets, CFR normalization, beat snapping, filtergraph assembly, timebase resolution).

#### 5.7.2 EDL — with a defined timebase

```jsonc
{ "output": { "w": 1920, "h": 1080, "fps": 60 },
  "music": { "src": "assets/music/track.wav", "beat_snap": true, "duck_under": "sfx" },
  "timeline": [
    { "title": { "text": "SNAKETRON", "style": "logo", "duration": 2.0 } },
    { "transition": "fadeblack", "duration": 0.3 },
    { "clip": "boosted-cutoff", "in": 1.0, "out": 7.5,
      "speed": [ { "until": "meta:kill-0.4", "rate": 1.0 },
                 { "until": "meta:kill+0.6", "rate": 0.25 },
                 { "rate": 1.0 } ],
      "effects": [ { "t": "shake",     "at": "meta:kill", "amp": 12, "decay": 3 },
                   { "t": "punch_in",  "at": "meta:kill", "zoom": 1.25, "ease": "out" },
                   { "t": "rgb_split", "at": "meta:kill", "dur": 0.25 } ],
      "text": { "value": "DEMOLITIONS!", "at": "meta:kill+0.2", "style": "impact" },
      "sfx": [ { "src": "assets/sfx/impact.wav", "at": "meta:kill" } ] },
    { "transition": "pixelize", "duration": 0.4 }
  ] }
```

**Timebase rules (normative, enforced by `compile_edl.py`):**
- All `in`/`out`, `at`, `until`, `dur`, and `meta:` anchors are **source-master seconds** (the clip's own timeline). `meta.json` timestamps are master seconds at the master's encoded rate.
- `rate` means **output speed relative to real gameplay time**. The compiler resolves every anchor to final output time through the piecewise speed map plus accumulated transition offsets; ffmpeg `enable=` expressions and SFX placements are emitted in their respective local timebases by the compiler, never hand-written.
- **No synthesized slow motion (P3.4):** the compiler verifies `min(rate) × capture_vfps ≥ output_fps` and errors with "recapture at ≥ N vfps" otherwise — a 0.25× ramp demands a ≥ 240 vfps master for 60 fps output, and `meta.json` carries `capture_vfps` precisely so this is checkable.

#### 5.7.3 Effects vocabulary (named recipes in `compile_edl.py`)

Camera shake (overscan + decaying multi-sine crop jitter), punch-in (`scale`+`crop` time expressions; `zoompan` only for slow drifts — it integer-rounds and wobbles), speed ramps (trim/`setpts`/concat), transitions (`xfade`: fadeblack, dissolve, `pixelize` fits the aesthetic, slices, wipes; `acrossfade` for audio), glow (split → luma-key → `gblur` → screen blend), chromatic aberration (`rgbashift` gated by `enable=between(t,…)`), grain/vignette/LUT, letterbox. Titles: `drawtext` for simple cards; **browser-rendered HTML title cards through the same capture pipeline** for hero typography. Remotion optional here — free at the current team size; licensing tripwire at 4+ employees noted in SKILL.md.

#### 5.7.4 Audio (greenfield — the game is silent)

(1) User-provided tracks first-class (path + auto beat-grid via `beats.py`); (2) bundled CC0 assets (Kenney / OpenGameArt / Pixabay CC0) with a license manifest in `assets/`; (3) optional generated stingers (ElevenLabs — licensed-catalog training, commercial-use output) behind an env key. Beat-synced cuts: ffmpeg has no beat detector — `beats.py` extracts the grid offline; the compiler snaps cut points to it. Mix chain: `aresample=48000` → `amix` → `sidechaincompress` (duck music under SFX) → `loudnorm=I=-14:TP=-1.5` last.

#### 5.7.5 Iteration loop & guardrails

1. Capture once per scenario (cached by `(scenario_id, seed, capture_vfps)` hash — mind the 240 vfps cost, §5.6.2).
2. Preview render 640×360 / crf 28 / veryfast with burnt-in timecode — seconds per iteration.
3. Self-review: `review.py` frame strips + vision QC; user reviews the preview.
4. Re-render only segments whose `(clip, effects)` hash changed; final 1080p60 `libx264 -crf 18 -preset slow -pix_fmt yuv420p -movflags +faststart`, single tagged BT.709 conversion (§5.6.2).

Guardrails in SKILL.md: everything CFR before `xfade`/`concat`; RGB masters, one tagged YUV conversion at final; bundle fonts for `drawtext` *and* remember canvas-text font parity is a capture-image concern (§5.6.2); never interpolate slow-mo when the scenario can be recaptured.

#### 5.7.6 Worked example (the launch-trailer prompt)

The brief's sequence maps to: logo title card (HTML capture) → `demolition-cutoff` scenario (speed ramp + shake + "Demolitions!") → `team-45pt-celebration` scenario (celebration layers at 0.5×, "Team matches!") → `boost-combo-clutch` scenario (nick-of-time combo choreographed via `combo_remaining_ms_before`, "Boost! Combos!") → rank-up: **a purpose-built fixture card component** reusing `RatingReveal` (verified pure-props and rAF-driven, so it virtualizes under BeginFrame — but the existing QA route wraps it in the full GameOverCard; a standalone hero-styled card is new work, and rank medallion PNGs join the `ready()` preload set) → leaderboard: a fixture card in `Leaderboard`'s styling (the real component is route/auth/data-coupled with a 60 s refresh interval — styling reuse only). App-UI shots are HTML captures with the same virtual-clock contract and the same network-neutralization requirements as game captures — and, per §5.7 rule 5, they **mount the real components** (`RatingReveal`, the leaderboard rows) against the app's real stylesheet with fixture props, rather than restating their look in standalone CSS. The logo slate is the one genuinely bespoke frame: paper ground, `Arial Black` wordmark in ink `#14181f` set **bold italic** per the design contract, a `--game-blue` rule on the same slant, and the `NOS_ORANGE` accent — no drop shadow, and the slant is the logotype's italic rather than the panel skew forbidden on cards.

---

## 6. Delivery plan

Phases are independently buildable; each has a hard exit criterion.

**M0 — Engine groundwork (small, high-leverage)**
Death cause at both emission sites (§5.4.1) + the **full** coordinated version bump (WS/GAMEPLAY + EXECUTOR + TRACE + RECOVERY, `#[serde(default)]` on new fields) + `BoostMeter` extraction. *Exit:* chaos suite green; death causes (incl. `Banked`) visible in `trace_rca` output on a recorded bot game; a pre-bump completion record still deserializes in a unit test; no visual diff in the live arena.

**M1 — ScenarioPlayer core** *(internal infrastructure + QA demo — first user-facing value lands in M2)*
`common/src/scenario.rs` loader + `ScenarioScript` ts-rs types + CI runner for `expect` blocks; `client/src/scenario.rs` (engine-stepped source, zero-lag stepping, post-snakes callback slot, cue track, shared advance-then-apply helper with `GameEngine`); `ScenarioCanvas` wrapper (virtual clock with segments, effect parity, opt-in addons); `/qa/scenario-player` with URL params; 3 authored demo scenarios (cut-off kill, combo frenzy, team bank). *Exit:* the cut-off scenario plays at 1× and 0.25× in the QA route with explosion + celebration + combo callout visually matching a live-game side-by-side; scenario CI green; tutorial scenes' **state-level goldens** (per-keyframe `sync_hash` — there is no pixel-golden infrastructure until M3's pinned environment, so pixel claims wait) unchanged.

**M2 — Play of the Game**
Executor ring (three feeds + anchors + cap + telemetry), `common/src/highlight.rs` scorer + `HighlightConfig`, clip cutting + `end_sync_hash`, optional-field completion attachment (no schema bump) + DynamoDB writer extension + size measurement → item-split decision, REST endpoint + participant/spectator authz, `useHighlight`, GameOverCard band (star-oriented rotation, ~26×11 follow camera, poster-skeleton insertion, autoplay gating incl. IntersectionObserver + CrazyGames `adState`, render-failure collapse), `/qa/play-of-the-game` (incl. malformed-anchor fixture), telemetry. *Exit:* full-stack dogfood — a real 2v2 ends with all four clients showing the same clip; payoff 8 s after playback start and, on a non-competitive match, playback starting ≤ 4 s after modal open; legibility check at 560 px and 375 px passes (or in-clip labels dropped per §5.5); ≥ 30 fps modal on Moto G-class / 4× CPU throttle; balancing gate per §5.4.3 (≤ 3 tuning rounds or ship-dark).

**M3 — Capture CLI + clip library**
`tools/video/capture.mjs` (pinned headless-shell + BeginFrame probe + software raster; virtual-time fallback; readiness barrier; chrome suppression + network neutralization; encode-in-loop RGB masters), `meta.json`, clip library, Docker capture image with fonts, runbook. *Exit:* a fresh clone (documented steps only) produces a 10 s 1080p60 master of an M1 demo scenario on macOS (fallback, SSIM-criterion) and in Docker (BeginFrame); **two Docker captures on the same machine are bit-identical**; Docker vs macOS masters pass the perceptual criterion incl. font parity.

**M4 — `/snaketron-create-video`**
Skill per §5.7; EDL compiler (timebase resolution, vfps verification, effect vocabulary); preview/final loop; CC0 asset pack; the two fixture card components (rank-up, leaderboard). *Exit:* the §5.7.6 launch-trailer prompt yields a 30–45 s 1080p60 trailer end-to-end with ≤ 1 human-guidance iteration, and a cached re-run renders the final in < 5 minutes — **passing all three gates below**. Gate 3 is not optional and not satisfiable by the other two.

1. **Mechanical QC** (as before): every timeline entry rendered, no black/duplicate frames at splice points, text legible at 1080p, audio ducked under SFX, beat-snapped cuts within 50 ms of grid.
2. **Automated brand conformance** (`scripts/brandcheck.py`, run on the final render):
   - *Font allowlist* — every font family referenced by a fixture card, the wordmark, or a `drawtext` filter is one the app itself ships (Arial Black / Arial / Impact / system stack). Any other font fails the build.
   - *Value polarity* — median frame luminance of each **non-gameplay** segment is within **±10%** of the median luminance of the gameplay segments. (A graphite-ground card against a white arena scores ~0.25 vs ~0.95 and fails. This check exists because palette conformance alone does not catch inverted polarity — the first build sampled all six tokens correctly and still looked like a different product.)
   - *Palette conformance* — ≥ 90% of each non-gameplay frame's pixel mass lies within ΔE < 10 of a §5.7 token color.
   - *Ink budget* — every gameplay segment averages ≥ 12% non-background pixel mass (build 1's hero shot measured 6.5–7.0%).
   - *String allowlist* — every burnt-in string is in the P3.8 allowlist. Any other string fails the build.
3. **Judged quality** (one reviewer, recorded verdict + the stills in `docs/qa/`):
   - *Same-product test* — a still from each non-gameplay segment placed beside a gameplay still; reviewer answers "could these come from the same product?" Any "no" fails.
   - *Payoff on camera* — the capture-time assertion (P3.6) passed for every gameplay shot, and the reviewer confirms the advertised event is visible in the cut. Automatable and blocking; a build-1-style shot where the kill never appears fails here even though every mechanical check is green.
   - *Composition* — at each gameplay shot's focus frame, the star snake's bounding box measures ≥ ⅓ of frame width (script-measurable from `meta.json` positions + camera rect), non-background ink is ≥ 15% of the frame (build 1 measured 6.5–7.0%), and the reviewer confirms no shot is mostly empty arena (P3.7).
   - *Overall* — reviewer judges the cut "shippable as a public trailer" with written reasons. A failed judgement blocks the milestone; it is not overridden by gates 1–2 passing.

**Order:** M0 → M1 → (M2 ∥ M3) → M4. M2 and M3 share only M1.

---

## 7. Risks & mitigations

| Risk | Severity | Mitigation |
|---|---|---|
| Cell-stepped slow motion reads as janky, not cinematic | High (product) | Speed-ramp default keeps the slow segment short; effect layers carry the smoothness; interpolation (§5.1.5-v2) scoped and flagged; decide after M1 output review |
| A scoring/clip bug degrades the completion pipeline (XP/MMR/ranking) | High (eng) | Scoring is `Option`-valued and infallible by contract — error/panic/overrun ⇒ `None` + telemetry, never a failed or delayed completion; CPU-capped; 750 ms fenced-commit ceiling stated |
| Death-cause change destabilizes sync (the one engine-semantics touch) | High (eng) | Cause is derived from already-computed branches — no new simulation behavior; cues fingerprint-excluded by allowlist; chaos suite + boundary-equivalence tests gate; full coordinated version bump (§5.3.4) with serde-default storage compat |
| PotG silently absent after executor failover / deploy drain | Medium (product) | Accepted v1 degradation, **stated**; `potg_ring_truncated` telemetry; future option: rebuild ring from the durable Redis stream |
| Style-point weights produce embarrassing/lopsided PotGs (incl. collusion, teammate-kill, AFK farming) | Medium (product) | Adversarial cases scored explicitly (decay/discount/penalty rows); balancing harness + human review with those cases in the rubric; server-side weights; threshold + ship-dark kill-switch |
| DynamoDB item bloat (record already embeds `final_state`) | Medium | Measure first; split clip to its own item past ~200 KB combined |
| Second engine + third rAF loop hurts low-end mobile in the modal | Medium | Idle-until-autoplay, pause-on-hidden, free-on-unmount, DPR ≤ 2; named-device exit criterion |
| Old-headless deprecation breaks the BeginFrame path | Medium | Pin `chrome-headless-shell` explicitly; startup beginFrame probe with loud failure; virtual-time fallback is first-class |
| Capture nondeterminism from async assets/fonts/raster | Medium | `ready()` barrier; eager preload; software raster pinned; fonts installed in the capture image; determinism criterion scoped to the Docker path |
| CSS-clock addons drift under in-browser slow-mo | Low | Web Animations API driving (§5.1.3); capture path unaffected |
| Version skew bricks the PotG section during deploys | Low | `gameplay_version` gate; section hides on mismatch; completion field is optional-defaulted (no validate() coupling) |
| Scenario scripts rot as the engine evolves | Low | `expect` assertions in CI; failures point at the exact scene |
| **Trailer is mechanically perfect and visually off-brand** — every provable gate passes while the video looks like a different product | **High (product)** | The failure mode actually observed in build 1: with only mechanical exit criteria, effort flows to what is checkable (SSIM, splice hashes, LUFS) and art direction defaults to invention. Mitigated by P3.5/P3.6, the normative §5.7 art direction, and M4's three-gate exit — in particular the value-polarity check and the judged same-product test, which no amount of pipeline correctness can satisfy |
| Composition defaults to "whatever the capture framed" | Medium (product) | P3.6 subject-size rule, per-scenario cell windows, and the script-measurable ≥ ⅓ frame-width check at each focus frame |
| Remotion licensing if team grows ≥ 4 | Low | Remotion optional (title cards only); noted in SKILL.md; HTML-capture path unencumbered |

---

## 8. Open questions (need Alex's call; none block M0/M1)

1. **PotG eligibility at launch:** all canonical modes incl. Quickmatch and Solo (design assumes yes, threshold does the gating), or Competitive-first? Related: is PotG even desirable on CrazyGames, where session-length pressure and the post-match ad already crowd the modal?
2. **Death-cause surfacing beyond PotG:** show "Eliminated by X" in the results standings now that attribution exists, or keep it PotG-internal for this project?
3. **Spectator access & future sharing:** v1 ACL is participants ∪ spectators (§5.3.3). If public clip sharing is on the roadmap soon, the ACL and payload should be revisited in M2 rather than after.
4. **Marketing embeds:** which pages want `ScenarioCanvas` at launch (home-screen `ArenaBackdrop` replacement? mode-select cards?) — affects M1 stretch scope only.
5. **Trailer brand kit:** *no longer an open question for the default path* — §5.7 Art direction now binds the trailer to the app's own visual system (paper ground, app tokens, Arial Black, hairline construction), and "placeholder" explicitly means the game's system rather than an invented one. The remaining question is narrower and optional: does SnakeTron want a **bespoke marketing identity** (distinct wordmark, display face, tagline) that deliberately departs from the in-app system? If yes, it must be signed off and recorded as the §5.7 rule-6 exception before M4 assets lock; if no answer, the default holds and M4 can proceed.
6. **Bot-corpus recording flag:** OK to add the dev/staging-only env flag that dumps completion rings? It's the cheapest path to the balancing corpus.

---

## Appendix A — Style-points quick reference

See §5.4.2. Categories: **Demolition** (eliminations 90 / mutual trade 60 + boost/cut-off/proximity/trap/laden modifiers; multi-kill ×1.5 on base; repeat-victim decay; AFK ×0.25; teammate −80), **Banking** (+5/point, +50 big carry, `Banked` deaths are never anti-style), **Combos** (+15×(chain−1), nick-of-time +10 with strict `0 <` bound), **Frenzy** (+5 boosted pickups where boost is earned, +20 for ≥8 pickups), **Anti-style** (death in clip −60, same-tick-trade exempt). Threshold 120 on the **re-centered** window. Tie-breaks: score → earlier focus tick → lower snake id. All weights in versioned `HighlightConfig`.

## Appendix B — Scenario script schema

See §5.2. Canonical schema + loader in `common/src/scenario.rs` with ts-rs export; JSON files under `client/web/scenarios/` (app-embedded) and `tools/video/scenarios/` (trailer-only).

## Appendix C — EDL schema

See §5.7.2 including the normative timebase rules; JSON-schema-validated by `compile_edl.py`; `meta:` anchors resolve against clip `meta.json` (master-seconds) through the piecewise speed map.

## Appendix D — Codebase fact sheet (research citations)

- Renderer purity & layering: `client/src/render.rs:1550-2740` (no clock reads); celebration slot `render.rs:2115-2143`; explosions composited in JS at `GameArena.tsx:957-967` in full-arena coordinates (`crashExplosion.ts:268-269`), sprites `crashExplosion.ts` (780 ms / 64 frames, `now`-parameterized, silently skips until sprite decode — `:292-294`).
- Live loop wall-clock sites: `useGameEngine.ts:148-154, 548-551`; effect clock `GameArena.tsx:906`; CSS residue `GameArena.css:72, 118, 157, 310`. Prediction cap `game_engine.rs:492-497`; committed lag `game_engine.rs:110`.
- Tutorial player: `client/src/tutorial.rs:640-704` (9 posed scenes), `:749-817` (render_scene crop), `:824-898` (WASM API); wrapper `TutorialSceneCanvas.tsx` (50 ms quantization; reduced-motion renders poster unconditionally `:103-106`).
- Engine determinism: seeding `game_state.rs:1423-1546`; RNG `common/src/util.rs:4-55` (xorshift64, in-state); sorted-iteration guarantees `game_state.rs:2977, 3224-3229, 3294-3304, 3464-3465`; `tick_forward` pub `game_state.rs:3005`; command queue `:1356-1420, 2805-2829`; no ownership check in `exec_command`; movers gate (non-mutual head-to-heads) `:3191`; collision one-pass + rollback `:3189-3241`; banking kill+respawn `:3482-3487`; same-tick respawn `:3243-3247`; chaos suite `server/tests/sync_equivalence_test.rs`; replay contracts `common/src/replay.rs` (ServerReplay :155-371, ClientReplay :385-499 — proof runs through `GameEngine::process_server_event`); boundary equivalence `client/src/lib.rs:764-834` (native `cargo test` over the wasm-bindgen API).
- Snake field visibility (why the loader lives in `common`): `snake.rs:114-119` (`speed_milli`/`movement_credit`/`boost` are `pub(crate)`), mutators `pub(crate)`.
- Invariant gate strictness: `validate_boost_invariants` pins food target, score limit, tick duration, pad layout (`game_state.rs:1758-1846`).
- WASM surface: `newFromState` `lib.rs:50-65`, `runUntil` `:75-95`, per-snake commands `:106-160`, `render` `:362-379`, visual cues `:270-281`.
- Dead replay infra: `server/src/replay/` (never wired), `GameTraceRecorder` orphaned (`sync_trace.rs:92-169`, zero call sites), hollow `event_log` `game_engine.rs:68, 786-787`. Alive: client ring `syncTrace.ts` (20k records), upload endpoint, `trace_rca` CLI.
- Kill attribution gap: killer discarded `game_state.rs:3213-3218`; dead snakes not collidable `:3215`; fingerprint allowlist excludes cues (`fingerprint.rs:85+`; the doc comment at `:9-24` lists other exclusions — cues are excluded by omission from the allowlist).
- Executor publish topology: gameplay events `publish_event_at` `game_executor_v2.rs:1291-1343` (no-op when terminal `:1291-1298`); command decisions `publish_outcome` `:1237-1289`; terminal batch dropped `:1729-1737`; completion publishes the terminal snapshot `game_bus.rs:1771-1780`; handoff cancels completion `:1828-1838` (executor file); recovery envelope has no event history `:1369-1381`; checkpoint cost precedent `recovery.rs:28-30`; fenced op timeout 750 ms `game_bus.rs:150`; completion retry-forever `game_executor_v2.rs:40`.
- Completion & DB: `CompletionRecordV1` `completion.rs:54-70` (embeds `final_state`); `validate()` strict-equality, re-run on reads `completion.rs:84-89`, `game_bus.rs:2012-2014`; the `season` optional-field precedent `completion.rs:62-66` (+ legacy-shape test `:707-713`); effects tagged-enum `:300-302`; DynamoDB writer `dynamodb.rs:2991-3108` (raw `final_state` JSON at `:3027`); JWT middleware `http_server.rs:255-266`; no `/api/games/*` routes, no per-resource authz today (`:268-349`).
- Version constants: `WS_PROTOCOL_VERSION` `server/src/lifecycle.rs:26`; `GAMEPLAY_PROTOCOL_VERSION` `client/web/constants.ts:11`; `EXECUTOR_PROTOCOL_VERSION` `server/src/cluster_membership.rs:36` (checked `executor_cluster.rs:103`, `recovery.rs:532`); `TRACE_FORMAT_VERSION` `common/src/trace.rs:32`; `RECOVERY_SCHEMA_VERSION` `recovery.rs:27`.
- Game-over UI: `GameOverCard.tsx:183-333`; card sizing `index.css:624-634`; Space capture `gamePresentation.ts:380-405`; rating poll `useMatchRating.ts` + `ratingReveal.ts:101-107` (Solo excluded `:23-26`); per-player rotation `GameArena.tsx:773-777` (short/wide exception `:595-602`); CrazyGames midgame ad on completion `GameArena.tsx:742-747`, overlay `CrazyGamesBridge.tsx:283-296`; QA precedent `App.tsx:26-30, 167-176`, `RatingRevealQA.tsx`, `tests/capture-rating-reveal.mjs`.
- Scoring signals: `FoodEaten { points, combo_chain, combo_remaining_ms_before, boost_active }` `game_state.rs:203-218` (chain-start encoding `:2330-2334`); `TeamGoal` is a **cosmetic cue, not an event** (`:342-351`, regenerated in movement path `:3382-3426`); `TeamScoreUpdated` has no snake attribution (`:279-282`); `carried_food` `:2586-2591`; Solo unlimited boost `:1024-1033`; team score limits `constants.rs:69-70`; idle timeout `constants.rs:45`; `player_action_counts` `game_state.rs:1294-1300`.
- Capture pitfalls: drain harness (mock backend, snapshot fixtures, explicit-clock WASM driving ~`:3847`, screenshot sites via `SNAKETRON_VISUAL_DIR`); provider stack fetches regions/health on every route (`WebSocketContext.tsx:2096-2135`, `regionPreference.ts`, default `localhost:8080`); `ArenaBackdrop` on non-play routes (`App.tsx:114-115`); canvas fonts are system Arial Black (`render.rs:285-287, 1464-1466`); sprite lazy-decode tolerance (`GameArena.tsx:549-557`); watchdog 3 s `useGameEngine.ts:44`; worktree WASM staleness (per-entry symlinks); measured sizes: state anchor 3.2 KB (2-snake), events ~4-6 KB/s raw, clip ~10-15 KB gzipped / 10 s.
- Video research (external, verified 2026-08): CDP `HeadlessExperimental.beginFrame` + `--deterministic-mode` are old-headless features — pinned `chrome-headless-shell` since Chrome ~132; HeyGen HyperFrames uses this in production; `puppeteer-capture` actively maintained; timesnap/timecut dormant; MediaRecorder can't see DOM overlays; Remotion free ≤ 3-person companies; anthropics/skills has no video skill (patterns: `slack-gif-creator`, `webapp-testing`); remotion-dev/skills = many narrow skills; ffmpeg recipes §5.7.3; ElevenLabs licensed-catalog music; CC0: Kenney/OpenGameArt/Pixabay.
