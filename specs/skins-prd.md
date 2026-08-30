# PRD: Snake skins and renderer/skin decoupling

| Field | Value |
| --- | --- |
| Status | Implemented. Gate 2 (section 6.2) was delivered later by `specs/skin-shading-prd.md` S2 — see section 15.5 |
| Product | Snaketron client rendering and cosmetics |
| Scope | Arena, roster, tutorial, and results-swatch rendering; skin identity plumbing; AI skin authoring |
| Owners | Product / Client / Server (identity channel only) |
| Last updated | 2026-08-15 |

## 1. Executive decision

Skins are feasible as a client-side cosmetic system with a thin, server-validated identity channel. The rendering rewrite is the real work; the current code is already structurally close, because one shared painter (`draw_alive_snake_skin`) and one color authority (`snake_palette`) serve every surface.

The proposed design is:

- Introduce a Rust `SnakeSkin` trait in the client crate as the ground truth for skin appearance. The generic renderer owns geometry, occlusion, layering, and viewer-perspective classification; a skin is a *package* that owns the pixels of one snake's body, optional declarative themes for the team base and the goal celebration, and a small machine-readable report of its colors and metrics.
- Thread a cosmetic animation clock (`anim_ms`) through the render entry so skins can animate. It is per-frame presentation time, never simulation state: it is not stored, not hashed, and not visible to gameplay. Classic ignores it, so parity goldens simply pin it. Animation varies a skin's paint *arguments*, never its op count — the invariant that keeps conformance and performance checkable.
- Extract the current appearance mechanically into `ClassicSkin` — the default skin — behind that trait. Extraction is verified pixel-for-pixel by committed golden op traces captured from the *current* code before any refactor begins, plus pinned-browser bitmap baselines. The classic skin never passes through an interpreter, so its parity does not depend on any new engine being correct.
- Layer a declarative skin document format (`SkinDoc`, JSON) on top, interpreted by one generic `ParamSkin` implementation of the same trait. Most new skins are documents, not code; a permanent conformance test requires `ParamSkin(classic.skin.json)` to reproduce `ClassicSkin`'s op trace exactly, which keeps the document layer honest forever.
- Resolve skins through a registry keyed by a string `SkinRef` (launch: `classic@1`). Any unknown, invalid, or future-versioned ref deterministically resolves to classic. A frame must never fail because of cosmetics.
- Carry skin identity in a `#[serde(default)]` per-player map on `GameState` alongside `usernames`, on the fingerprint-excluded cosmetic channel. The server validates refs against its catalog at join. Nothing hashed by `sync_hash` changes; the chaos suite stays green by construction.
- Keep the friend-blue/foe-red perspective rule generic. The renderer computes each snake's viewer-relative role; skins map roles to paint and can never reclassify friend and foe. The same rule governs skinned bases and celebrations: friendly stays cool-hued, enemy stays warm-hued, validator-enforced.
- Attribute world dressing deterministically: the **viewer's** skin themes the bases (zone tints, goal walls, endzone text colors — geometry and text layout stay generic), and the **scorer's** skin themes the goal celebration everyone sees, from a palette plus a first-party effect id. Classic themes reproduce today's bytes exactly, which also *resolves* the long-standing hardcoded-hex leak in `scoreEffects.ts` instead of deferring it.
- Ship AI skin authoring as a Claude Code skill (`.claude/skills/author-skin`) backed by a dev-only `/qa/skins` fixture route, a contact-sheet capture script, a native `SkinDoc` validator, and the conformance suite. Player-created skins are a later phase that the architecture must not foreclose, and does not: a skin is data, addressed by content hash, validated server-side by the same Rust validator.

Two verification gates are prerequisites for calling the rewrite done, in this order: golden op traces must be captured and committed *before* the first extraction commit, and the pinned-browser bitmap baselines must land beside them. CI runs the op-trace gate natively today; the bitmap gate stays local unless the wasm+Playwright CI job is funded (section 15.4).

## 2. Product problem

Every snake in Snaketron looks the same. Appearance is welded into the renderer: `snake_palette` hardcodes six fill/outline hex pairs, `draw_alive_snake_skin` hardcodes the pass structure, and satellite surfaces (roster glyphs, results swatches, carried-food ink) read those values through ad-hoc couplings. There is no way to add a second look without editing the renderer, and no safe way for an AI or a player to author one.

We want:

- a pluggable skin abstraction with a defensible boundary between generic rendering and skin-owned appearance;
- the current look preserved exactly — pixel for pixel — as the default skin, so the rewrite is invisible to players;
- skins that can *animate*: time-parameterized effects and gradient-based looks, within the canvas-2D renderer;
- skins that extend beyond the snake's body to the team base and the goal celebration — the cosmetics players actually show off;
- an efficient AI authoring path now (a skill that can design, validate, render, and self-review a skin end to end);
- a future player authoring/selection path that today's decisions do not foreclose.

The mechanic must not:

- change gameplay, timing, collision, or any state hashed by `sync_hash`;
- let a skin hide or camouflage competitive information (boost state, friend/foe identity, carried food);
- let a client inject arbitrary strings or content into other clients' renderers;
- regress per-frame rendering performance (no new per-frame allocation, JSON, or per-cell dispatch); or
- break replays, spectators, traces, or the debug-desync workflow.

## 3. Goals and non-goals

### 3.1 Goals

1. Decouple snake appearance from the generic renderer behind a Rust `SnakeSkin` trait with an explicit, documented boundary.
2. Re-implement the current appearance as `ClassicSkin`, the default, with pixel-for-pixel identical output on every surface (arena, roster, tutorial, results swatch), proven by op-trace goldens and browser baselines.
3. Rule explicitly on every contested boundary element: boost aura, carried-food labels, food and NOS pickups, dead snakes, roster/tutorial surfaces, the color-swatch export, crash explosions, team bases, and score celebrations (section 7).
4. Introduce `SkinDoc` + `ParamSkin` so a skin can be authored as validated data, with `classic.skin.json` as the permanent interpreter-honesty fixture.
5. Support animated skins through a cosmetic animation clock threaded to every skin surface, with op-count invariance, reduced-motion compliance, and goldens pinned to a fixed time.
6. Make the team base (zone tints, goal walls, endzone text colors) and the goal celebration skinnable via declarative themes — viewer-attributed for bases, scorer-attributed for celebrations — with classic themes byte-identical to today, resolving the `scoreEffects.ts` hardcoded-hex leak in the process.
7. Add the cosmetic skin-identity channel (state map, server catalog validation, persistence) without touching the sync fingerprint.
8. Ship the `author-skin` Claude Code skill, the `/qa/skins` fixture route, and the contact-sheet capture script; validate them by authoring one document skin and one Rust-escalation skin end to end.
9. Land the rewrite with zero measurable per-frame performance regression, and take the two known free wins (packed-integer head-gradient keys, interned skin handles).

### 3.2 Non-goals

- Player-facing skin creation, editing, purchase, or marketplace UI. The architecture preserves the path (section 11); v1 does not build it.
- True GPU shaders. The renderer remains canvas-2D; "shader-like" effects are what gradients can express through `PaintCtx` today, with more ops added as skins need them. A WebGL renderer would be a different PRD.
- Sprite/`drawImage` skins. The op exists nowhere until a skin needs it, for the same reason the asset pipeline is deferred.
- Skinning arena *gameplay objects*: food art, NOS canisters, the grid, and the boost HUD stay world territory shared by all players. (Team bases and goal celebrations **are** skinnable — sections 5.6 and 7.)
- Dead-snake appearance overrides. The trait reserves the slot; v1 policy locks death to the identity-erasing gray corpse.
- Re-skinning crash explosions. Recorded as a future extension point with one named hook, nothing more.
- Cross-player skin visibility control, per-viewer skin muting, or "streamer mode".
- Shipping user-provided Rust or WASM code. Custom Rust skins are first-party only, forever.
- A new CI platform. The PRD names the CI gap (section 15.4) and its two resolution options; choosing one is an implementation-time release decision, not an architecture input.

## 4. Repository baseline

Verified facts the design builds on, with locations:

- `render_game_state` (`client/src/render.rs:1550-2736`) is the one arena pipeline. Painting order: white clear → 1px padding translate → zone tints → grid dots → endzone text → food (three passes) → available NOS packets → JS celebration callback slot → snakes (alive via the skin painter, dead via a separate inline gray path) → goal walls → carried-food labels. Dead snakes deliberately paint before walls; carried-food labels deliberately paint after them.
- `draw_alive_snake_skin` (`render.rs:516-849`) already serves both the arena (`render.rs:2186`) and the roster glyph (`render.rs:1252`) — the mechanism that keeps a roster snake identical to the arena snake exists and works. It paints, in order: grid-dot mask (skipped when `mask_color` is `None`), outline layers (boost adds a `#fff200` band of `BOOST_OUTER_EXTRA = 6.0` px under the ordinary contour, and widens the mask from 1.0 to 3.0 px — `render.rs:85-89`), body strokes with round caps, corner-joint circles, tail cap, a 10-cell white head-proximity gradient, the head circle with a 0.3-alpha white overlay, and a `#333` head core at `HEAD_CORE_RADIUS_RATIO = 0.38`.
- `snake_palette` (`render.rs:460-499`) is the sole color authority: viewer-relative (teammates blue `#70bfe3`/`#3c8dde` shades, opponents red `#ff6b6b`/`#e34e5b` shades, spectators canonical team 0/1), with FFA slot colors (steel `#556270`, gold `#f7b731`).
- The per-frame render path crosses zero JSON: `GameClient.render` (`client/src/lib.rs:362-379`) passes the typed `&GameState` to the renderer inside WASM. Per-frame scalars are canvas, integer `cell_size` 5–15, rotation 0/90/180/270, optional local user id, and the celebration callback. The sibling rAF loop serializes state JSON for React UI only, gated by parse-on-change.
- Identity inputs are already a struct: `SnakeSkinInputs` (`render.rs:903-929`; TS mirror `client/web/utils/snakeSkin.ts`) with exactly `{snake_index, team_id, team_member_slot, snake_count, is_team_game, local_snake_id, local_team_id}`; `snakeSkinColors` (`render.rs:1279-1284`) exports `{fill, outline, label}` JSON for DOM swatches.
- The tutorial (`client/src/tutorial.rs:782`) renders whole scenes through `render_game_state`; decoupling the arena automatically covers it. `Snake::for_illustration` carries no cosmetic identity.
- The sync fingerprint already has a cosmetic channel: `common/src/fingerprint.rs:17-18` excludes `usernames`, `spectators`, `game_code`, `host_user_id`, `start_ms`. Snake `speed_milli`, `boost`, and `combo` *are* hashed, so cosmetics must never ride on `Snake`.
- Known appearance leaks outside the renderer (all verified): `scoreEffects.ts` hardcodes four team hexes across two functions — `getScoreEffectTeamColor` returns `#5299bb` (byte-identical to the classic team-blue outline shade) or `#d45454`, and `getScoreReadoutColor` returns `#2b6f8c`/`#a83232` — and mirrors the boost-glow width as `READOUT_BOOST_OUTLINE_PX`; `BoostCanisterMark.tsx` duplicates the NOS palette in SVG; `gamePresentation.ts` `GAME_SHELL_COLORS` duplicates team blue and NOS ink; the e2e suite pins classic hexes exactly (`planned-websocket-drain.spec.js:2384, 3006-3011`); the scoreboard's is-blue/is-red identity is a parallel CSS token system.
- Crash explosions are skin-independent by construction: a sprite sheet plus its own orange ring, painted topmost from TS (`crashExplosion.ts`). No snake color reaches them.
- Test infrastructure: CI runs Rust-only (fmt/clippy/doc/nextest — this includes the client crate's native renderer unit tests at `render.rs:2738-3451`); no CI job builds WASM or JS. The Playwright e2e harness (`client/web/tests/e2e/specs/planned-websocket-drain.spec.js` — "the drain harness" below, named for its original WebSocket-drain coverage and now the repo's deterministic browser suite; distinct from the autoscaling PRD's ECS task draining) is backend-free (MockWebSocket + injected states) and already trusts same-browser `toDataURL` equality and pixel-tone classification probes. Cross-platform golden PNGs are hostile (system-font labels, `measure_text` truncation, DPR asymmetry: the arena canvas is *not* DPR-scaled, the roster canvas is).
- Persistence surfaces exist: DynamoDB user items (`server/src/db/models.rs` `User` with `xp`) for logged-in selection later; versioned localStorage keys plus the CrazyGames preference allowlist (`crazyGamesPreferences.ts`) for guests, exactly as boost input mode does.
- The name "skin" is currently overloaded: `NosBottleSkin` (`render.rs:167-171`) names NOS canister art variants and must be renamed before "skin" means one thing.

## 5. Design principle and architecture

### 5.1 Boundary principle

The generic renderer owns everything that is (a) gameplay state shared by all players, (b) competitive information, or (c) a layout, z-order, or occlusion contract. The skin owns the pixels of one snake's body, declarative *themes* for two pieces of world dressing (the team base and the goal celebration — the renderer keeps their geometry, layout, and classification and consumes only theme colors/ids), and a machine-readable report (`colors()`, `metrics()`) that generic chrome consumes without understanding the skin's internals.

Corollaries:

- Skins never see `&GameState`. They receive a pose (transformed body cells, cell size, boost flag) and an identity (viewer-relative role plus shade slot). This is what lets the roster paint a synthetic two-point pose and the tutorial paint scene states through the same code.
- Skins never decide *whether* something is shown — boost presence, carried food, death, ready checks — only *how their part of it looks*, inside validated constraints.
- Generic chrome (mask, roster layout, label ink, ready check) consumes skin *reports*, never skin *constants*. Today's hardcoded reads of `BOOST_OUTER_EXTRA`/`ORDINARY_OUTLINE_EXTRA` in roster layout (`render.rs:1004-1010`) are exactly the coupling this replaces.

### 5.2 The trait and its supporting types

Names may change in review; the contracts may not.

```rust
// client/src/skin/mod.rs

/// Viewer-relative role — GENERIC policy computed by the renderer from
/// state.players + team ids. Replaces the raw 7-argument snake_palette
/// signature. Roles are FULLY viewer-resolved: a skin's colors(id) is a
/// pure function of SkinIdentity, so every viewer-dependent branch in
/// today's snake_palette is decided by the renderer, never the skin.
pub enum SnakeRole {
    Own,
    Teammate,
    Enemy,
    SpectatedTeam(u8),           // canonical team 0 = blue, 1 = red
    FreeForAll { palette_slot: u8 }, // renderer-computed paint slot 0..=3
}

pub struct SkinIdentity {
    pub role: SnakeRole,
    pub shade_slot: u8, // today's team_member_slot % 2
}

/// Everything a surface must supply to paint one snake. Deliberately no
/// &GameState: the roster's synthetic pose and tutorial scenes use the
/// same entry.
pub struct SnakePose<'a> {
    /// Compressed body, head first, whole-number screen cells, rotation
    /// already applied (same contract as draw_alive_snake_skin today).
    pub cells: &'a [(f64, f64)],
    pub cell_size: f64,
    pub boost_active: bool,
    /// Cosmetic animation clock (section 5.6). Monotonic presentation
    /// milliseconds; never simulation state. Goldens and static surfaces
    /// pin it; classic ignores it.
    pub anim_ms: f64,
    /// Mirrors prefers-reduced-motion; an animated skin must render a
    /// static (or minimally moving) variant when set.
    pub reduced_motion: bool,
}

pub struct SkinColors {
    pub fill: String,      // representative flat fill; contrast source
    pub outline: String,
    pub label_ink: String, // carried-food digits + roster name ink
    pub swatch: String,    // single flat hex for DOM micro-surfaces
}

pub struct SkinMetrics {
    /// Max px painted beyond the body cells per side. Drives the
    /// renderer-owned occlusion mask AND roster row sizing.
    pub overhang_px: f64,
    pub head_core_radius_ratio: f64, // classic: 0.38
    /// Ready-check contract: the white check assumes a dark head core.
    pub head_core_is_dark: bool,
}

/// Viewer-attributed world dressing (section 5.6). Colors only — the
/// renderer keeps zone/wall geometry, rotation, and text layout.
pub struct BaseTheme {
    pub friendly_zone: String, pub enemy_zone: String, // classic: #e6f4fa / #ffe6e6
    pub friendly_wall: String, pub enemy_wall: String, // classic: #7aa8c1 / #c18888
    pub friendly_text: String, pub enemy_text: String, // classic: #c0d8e4 / #e4c0c0
}

/// Scorer-attributed celebration (section 5.6). A palette plus a
/// FIRST-PARTY effect id resolved by the existing TS ScoreEffectRenderer
/// registry — never user code.
pub struct CelebrationTheme {
    pub effect: String,                                  // classic: "goal-impact-wave"
    pub friendly_accent: String, pub enemy_accent: String,   // classic: #5299bb / #d45454
    pub readout_friendly: String, pub readout_enemy: String, // classic: #2b6f8c / #a83232
}

pub trait SnakeSkin {
    fn colors(&self, id: &SkinIdentity) -> SkinColors;
    fn metrics(&self, boost_active: bool) -> SkinMetrics;
    /// Paint one living snake. No mask parameter: occlusion is the
    /// renderer's job, driven by metrics().overhang_px.
    fn paint_alive(&self, ctx: &mut PaintCtx, pose: &SnakePose, id: &SkinIdentity)
        -> Result<(), JsValue>;
    /// Default = the extracted gray corpse, pixel-identical to today's
    /// inline path. SkinDoc v1 has no dead slot; no skin overrides yet.
    fn paint_dead(&self, ctx: &mut PaintCtx, pose: &SnakePose) -> Result<(), JsValue> {
        corpse::paint_default(ctx, pose)
    }
    /// None => classic dressing. Defaults preserve today's bytes.
    fn base_theme(&self) -> Option<&BaseTheme> { None }
    fn celebration_theme(&self) -> Option<&CelebrationTheme> { None }
}
```

The role derivation is normative and must reproduce `snake_palette`'s decision table (`render.rs:460-499`) case by case:

| Today's inputs | Role | Classic paint |
| --- | --- | --- |
| Team game, snake's team == viewer's team (or viewer is the snake) | `Teammate` / `Own` | blue shade table |
| Team game, other team | `Enemy` | red shade table |
| Team game, spectator | `SpectatedTeam(0 \| 1)` | blue / red shade tables |
| Non-team, snake is the viewer's | `Own` | blue\[0] |
| Non-team, `snake_count == 2`, any non-own snake — including **both** snakes for a spectator | `Enemy`, `shade_slot = 0` | red\[0] — today's code yields red for every non-own duel snake regardless of viewer |
| FFA (`snake_count != 2`), spectator | `FreeForAll { palette_slot: index % 4 }` | slots 0–3 → blue, red, steel, gold |
| FFA (`snake_count != 2`), playing viewer, opponent with `index % 4 == 0` | `FreeForAll { palette_slot: 3 }` | gold — today's fall-through, viewer-resolved by the renderer |
| FFA (`snake_count != 2`), playing viewer, other opponents | `FreeForAll { palette_slot: index % 4 }` | red, steel, gold |

The subtlety this table exists to pin: today `snake_index % 4 == 0` is blue only for spectators; for a playing viewer the same index falls through to gold (`render.rs:493-496`). The renderer performs that remap so slot 0 (blue) can never be assigned to an opponent of a playing viewer, and `ClassicSkin::colors` can be verified against `snake_palette` exhaustively. The golden fixture corpus enumerates these resolved roles directly.

`PaintCtx` is an enum-dispatched painter sink with two arms: `Web` forwards 1:1 to `web_sys::CanvasRenderingContext2d`, and a `#[cfg(test)]` `Rec` arm records ops as data for native golden tests. Its op set is only what the skin-owned painters actually need: the ~12 operations the classic painters use, plus radial-gradient fills. Compositing controls, filters, and `drawImage` are deliberately absent — an op nobody paints with is surface to keep working, and adding one when a skin needs it is a few lines plus a golden line. Sprite atlases are therefore a phase 2+ question (size caps, content addressing) rather than an unused pipeline shipped up front. The recorder is compiled out of release builds, and recording is closure-deferred so a release build never even constructs the op it would discard.

### 5.3 Registry, refs, and fallback

```rust
/// Identity + versioning. Built-ins: "classic@1". Future user documents:
/// "sha256:<hash>" — content-addressed so spectators and replays render
/// exactly the published bytes.
pub struct SkinRef(pub String);

pub struct SkinRegistry { /* built-ins + registered documents */ }
impl SkinRegistry {
    /// Unknown, invalid, or future-versioned ref => &ClassicSkin.
    /// Deterministic, logged, never an error.
    pub fn resolve(&self, r: &SkinRef) -> &dyn SnakeSkin;
    pub fn register_doc(&mut self, json: &str) -> Result<SkinRef, SkinDocError>;
}
```

Fallback-to-classic is a hard behavioral rule, testable and load-bearing: old clients meeting a newer schema version, corrupted persistence, or an unlisted ref degrade to classic cosmetics — never to a broken or blank frame.

Built-in versioning: for built-ins, `@N` is a contract epoch, not a pixel pin. `classic@1` means "the current classic" — a sanctioned golden-regenerating cleanup PR (section 6.3) may change classic's pixels without minting `classic@2`, exactly as renderer changes update every player's view today; replays of built-in skins therefore render the replaying client's current pixels, which is the existing behavior. `classic@2` would be minted only if the *contract* consumed by generic chrome (`colors()`/`metrics()` semantics) changed incompatibly — expected never. Byte-exact pinning is what content-addressed `sha256:` refs are for (section 11), and built-ins deliberately do not offer it.

### 5.4 The document layer

`SkinDoc` types and their validator live in a small `skin-schema` crate compiled both natively (server-side validation, CI) and to WASM (client interpretation, later in-browser preview):

```rust
pub struct SkinDoc {
    pub schema_version: u32, // v1; additive-only within a major version
    pub name: String,
    /// Role-keyed palettes, dimensioned to the real decision table:
    /// team: {own_teammate: [{fill, outline}; 2], enemy: [{fill, outline}; 2]}
    ///       (shade index = shade_slot; spectated team 0/1 reuse these two
    ///       tables, stated explicitly so classic is expressible),
    /// ffa: [{fill, outline}; 4] indexed by palette_slot.
    pub palette: RolePalette,
    pub head: HeadStyle,        // core_ratio (default 0.38), core_color, gradient {len_cells, max_opacity, color}
    pub outline: OutlineStyle,  // extra_px (capped), boost_band {color, extra_px <= cap}
    pub labels: Option<LabelStyle>, // explicit label_ink/swatch; else derived via the roster_label_ink rule
    /// Bounded keyframed parameter tracks over palette/outline/gradient
    /// values, driven by anim_ms. Animation varies paint ARGUMENTS, never
    /// op count or structure; freezes at its reduced-motion pose.
    pub animation: Option<AnimationSpec>,
    /// World-dressing themes (section 5.6). Absent => classic dressing.
    pub base: Option<BaseTheme>,
    pub celebration: Option<CelebrationTheme>, // palette + first-party effect id only
}
```

`ParamSkin` implements `SnakeSkin` by interpreting a `SkinDoc` **precompiled at registration** (resolved color strings, fixed layer arrays — zero per-frame allocation or formatting). Validator constraints are concrete, not vibes:

- label ink meets WCAG AA against every fill in the palette, using the existing `relative_luminance` rule (`render.rs:1052-1074`);
- team-role palettes sit inside colorimetric blue/red hue windows (OKLCH), so a teammate always reads blue-family and an enemy red-family to the viewer;
- the boost band is mandatory, contrasts with the fill, and its `extra_px` respects the cap; in schema v1 the band is **pinned to classic's `#fff200` / +6 px** so no document skin can weaken the boost telegraph (first-party Rust skins may restyle it under the conformance test's distinctness assertion);
- `overhang_px` respects a global cap so roster rows and occlusion masks stay bounded;
- ready-check badge contrast: the roster's check mark must clear a contrast minimum against `head.core_color`; a document declaring a light core is rejected in schema v1 (equivalently: v1 documents must report `head_core_is_dark = true`), so the white check (`render.rs:1179`) is always legible;
- animation bounds: track rates and amplitudes are capped, animated values must stay inside their static constraints (hue windows, contrast, overhang) at *every* point of the track, and every animated document must define (or default to) a reduced-motion freeze pose;
- base and celebration hue locks: `friendly_*` theme colors must read cool/blue-family and `enemy_*` warm/red-family (the same OKLCH windows as team roles), and celebration `effect` must name a registered first-party effect id;
- `swatch` and `label_ink` are mandatory flat 6-digit hexes even for future gradient skins — this is the contract that keeps every DOM micro-surface working without understanding skin internals.

`classic.skin.json` is the first document, and `ParamSkin(classic.skin.json)` must reproduce `ClassicSkin`'s op traces byte-for-byte in CI forever. When the interpreter and the hand-written skin disagree, the interpreter is wrong.

### 5.5 What the arena loop becomes

Per alive snake, the renderer: computes `SkinIdentity` from the existing perspective resolution (`render.rs:1603-1620, 2156-2159`); looks up the snake's interned skin handle (a registry index, section 8.3); paints the occlusion mask itself using `metrics(boost).overhang_px` with the same rectangle arithmetic the painter uses today (`render.rs:568-611`); calls `paint_alive`; and queues the carried-food label with the cached `label_ink` (section 8.3). Dead snakes call `paint_dead`. Z-order, wall and label geometry, and the celebration slot's position in the frame are untouched; wall/zone/text *colors* and the celebration *palette* resolve from themes per section 5.6.

Two renderer-owned mask rules preserve today's exact behavior, because after the hoist no skin can produce or suppress a mask:

- the mask is **skipped entirely when the pose is a single cell**, alive or dead — today `draw_alive_snake_skin` returns before its mask pass for single-cell bodies (`render.rs:534-565`), and the hoisted per-point rectangle loop would otherwise emit one;
- **dead snakes always mask with the corpse painter's own fixed 1 px extent** — a corpse constant, never the player skin's `overhang_px`, so a wide-overhang skin cannot widen its own corpse.

Hoisting the mask out of the skin is the **one deliberate structural change** to painting (it is what makes a future non-white arena possible without touching every skin), and it must be proven pixel-inert by the op-trace gate like everything else.

### 5.6 Animation clock and world dressing

**The animation clock.** `GameClient.render` gains an `anim_ms: f64` argument, supplied by the embedding surface and threaded into every `SnakePose`:

- arena: the rAF timestamp (`performance.now()`), so animation is smooth and independent of the simulation quantum;
- tutorial: the scene player's own clock;
- roster glyph: a fixed constant — the roster is a static portrait, and animating dozens of roster canvases is a cost with no product value (revisit only as deliberate polish);
- goldens, `/qa/skins` fixtures, and contact sheets: pinned constants, so op traces and bitmaps stay deterministic.

`anim_ms` is presentation time. It never enters `GameState`, events, snapshots, or `sync_hash`; no gameplay code may read it. Classic is time-invariant, which is why the parity requirement is unaffected: goldens pin `anim_ms` and classic's op stream does not vary with it. The conformance suite enforces the structural rule that makes animation safe for every skin: sampling a skin at multiple `anim_ms` values must produce the *same op sequence with different arguments* — op-count invariance — and `reduced_motion: true` must produce a static pose. "Shader-like" means what canvas 2D can express through `PaintCtx` — today gradients, with more ops added as skins need them (section 5.2); GPU shaders are out of scope (section 3.2).

**Base dressing is viewer-attributed.** The viewer's selected skin themes both bases: its `friendly_*` colors paint the viewer's own zone, wall, and endzone text, and `enemy_*` the opposing side — on top of the untouched generic rule that decides *which* side is which (`render.rs:1638-1642` perspective logic). Spectators get their own selected skin's dressing, falling back to classic. Attribution to base *owners* is rejected: a 2v2 team has two players with potentially different skins and no deterministic owner, and a per-owner mix would break the friend-cool/foe-warm read the hue locks exist to protect. Consequence, stated plainly: other players do not see your base theme — base dressing is how *your* game looks, like a controller theme, not a flex.

**Celebrations are scorer-attributed.** When a player scores, *their* skin's `CelebrationTheme` plays for every viewer — celebration is the flex surface. The effect itself remains a first-party TS renderer selected by id from the existing `ScoreEffectRenderer` registry; the theme supplies the id plus accent/readout colors, replacing the four hexes hardcoded in `scoreEffects.ts` today (classic theme ≡ those bytes). Plumbing: the goal cues already polled per frame (`getPredictedVisualStateJson`) carry the scoring snake; the cue is extended with the scorer's resolved celebration theme (flat data across the boundary, resolved from the interned cosmetic map at cue time). A celebration in flight keeps the theme it resolved with. Friendly/enemy accent selection stays the generic renderer's is-friendly decision, exactly as today's `isBlue` logic.

## 6. Pixel-parity verification

Requirement: with the rewrite landed and every snake resolving `classic@1`, rendered output is pixel-for-pixel identical to today on every surface.

### 6.1 Gate 1 — golden op traces (CI-native, authoritative)

1. **Instrument first; never refactor before goldens exist.** The first commit introduces `PaintCtx` with the Web arm forwarding 1:1 and routes the skin-owned painters — `draw_alive_snake_skin` and the inline dead-snake path — through it, including the roster's calls into them. The roster's generic chrome (DPR transforms, name label, ready check) stays on the raw context: its `measure_text`-driven text fitting cannot be natively goldened and is already covered by the native layout tests (`render.rs:3042-3243`) and Gate 2. Zero logic change; existing unit tests green.
2. **Capture goldens from the current code.** Record ordered op traces (exact f64 bit patterns) via the Rec sink across the canonical fixture corpus: straight, turning, and single-cell snakes; boost on/off; dead multi- and single-cell; the roster glyph in both facings at its tested layout; every `SnakeRole` × shade slot; cell sizes 5, 10, 15; rotations 0/90/180/270 where geometry-relevant; `anim_ms` pinned to a fixed value (classic is time-invariant, so this is free determinism, not a constraint). Commit the traces.
3. **Every subsequent step must reproduce them byte-identically**: corpse extraction, trait introduction, mask hoisting, and the `ParamSkin` conformance test all diff against the same pre-refactor ground truth. This gate runs in the existing nextest CI job — no browser, no WASM build.

### 6.2 Gate 2 — pinned-browser bitmap baselines (backstop)

Extend the drain harness with a `@skins` spec: `toDataURL` captures of fixture scenes on the pinned headless Chromium, compared byte-for-byte against committed baselines, regenerated only via an explicit `npm run skin:approve`. The browser "pin" today is only the lockfile-resolved `@playwright/test` version (`package.json` uses a caret range): pin it exactly, and require baseline regeneration after any Playwright bump. Text-bearing regions (endzone names, roster labels — `measure_text` and system-font nondeterminism) are excluded from strict comparison; the existing pixel-tone probes and pinned CSS-var hex assertions remain untouched as the third, already-trusted anchor. Every pixel run must first verify a fresh in-worktree `wasm-pack` build (fingerprint the loaded pkg): the stale-WASM worktree trap makes comparisons pass falsely.

### 6.3 Preserved quirks (checklist)

These are behaviors, not bugs, until a golden-regenerating cleanup PR says otherwise. Extraction must carry each one verbatim:

- the literal `0.38` head-core ratio in the single-cell branch (`render.rs:563`) duplicating `HEAD_CORE_RADIUS_RATIO` — unify only under op-golden proof;
- single-cell snakes get **no** mask pass and **no** white head overlay, but **do** get boost outline layers;
- mask rectangles paint before outline layers; the mask color is hardcoded `#ffffff`; boost widens the mask from 1.0 to 3.0 px;
- dead snakes paint before goal walls; carried-food labels paint after them;
- the corpse's occlusion mask and border use fixed 1 px extents regardless of any skin's metrics;
- the head-gradient walk's visit order and corner-skip rule (`render.rs:744-798`);
- the roster surface never receives `boost_active` from TS today (`RosterSnakeCanvas.tsx` omits it) — preserve the gap; whether roster glyphs should show boost is a separate product decision.

### 6.4 Pre-golden cleanup

Two changes land *before* goldens are captured so the goldens never encode them: rename `NosBottleSkin` → `NosBottleVariant` (`render.rs:167-171`), and delete the invisible `Tick:` debug text painted at `canvas_height + 20` (`render.rs:2727-2730`).

## 7. Boundary rulings

Each contested element, ruled and locked. Re-litigating any of these requires amending this PRD, not a drive-by widening of the skin contract.

| Element | Ruling |
| --- | --- |
| Boost aura | Split: presence generic, look skin-owned within locks |
| Carried-food labels | Generic HUD; skin contributes ink color only |
| NOS pickup canisters | Arena property; out of skin scope |
| Food and combo value marks | Arena property; out of skin scope |
| Dead snakes | Trait slot with locked v1 policy: generic gray corpse |
| Roster, tutorial, swatch | Consumers of the skin, never owners |
| `snakeSkinColors` export | Kept byte-compatible; additive `swatch` field |
| Crash explosions | Generic in v1; one named future hook reserved |
| Score celebrations | Skinnable: scorer-attributed theme + first-party effect id |
| Team bases (zones, walls, endzone text) | Skinnable: viewer-attributed color theme; geometry and classification generic |
| Friend/foe classification | Generic, always; skins map roles to paint |

1. **Boost aura — split, with a belt-and-suspenders lock.** Presence is competitive telegraphy: the renderer passes `boost_active`, and a conformance test applied to every registry entry asserts that boost on/off produce visibly different paint within the reported overhang. Extracting the band into a generic overlay is rejected: it is interleaved mid-pass (widened mask, layer stack — `render.rs:120-150`) and moving it would break byte parity. The look is skin-owned in the trait, but SkinDoc v1 pins `#fff200`/+6 px (section 5.4). Classic reproduces `#fff200`/6.0/2.0/3.0/1.0 exactly (`render.rs:85-89`).
2. **Carried-food labels — generic HUD.** Anchoring (`body_cell_behind_head`), draw-after-walls ordering, font, and halo are renderer-owned. The skin contributes only `colors().label_ink` (classic derives it via `roster_label_ink(fill)` — identical output). The halo and roster label shadow are derived generically from the ink's relative luminance — today's derivations key on exact string equality with `#ffffff` (`carried_label_halo`, `roster_label_shadow`), which would give a light non-white custom ink an unreadable white halo; the luminance rule produces identical output for classic's two inks (golden-proven) and correct output for arbitrary validated inks. Skins can never move, hide, or restyle the number.
3. **NOS pickup canisters — arena, firmly out of scope.** Drawn from `arena.boost_pads` with one fixed palette shared by all players, duplicated byte-for-byte in `BoostCanisterMark.tsx`. Theming them is a future *arena theme* axis. The `NosBottleVariant` rename ships first so "skin" means exactly one thing.
4. **Food and combo value marks — arena.** Food is drawn wholly from `arena.food` with fixed greens; the combo white-digit marks are food art driven by local combo state. Generic, explicitly out of skin scope.
5. **Dead snakes — trait slot, locked v1 policy.** Extract the ~300-line inline gray path (`render.rs:2223-2526`, `#f0f0f0`/`#d0d0d0`/`#666` X) into a shared corpse painter over the same geometry walker, wired as the trait's *default* `paint_dead`. SkinDoc v1 deliberately has no dead slot. Death stays identity-erasing — a gameplay-readability property — pixel-identical, including the dead-before-walls z-order; the corpse's occlusion uses its own fixed 1 px extent, never the player skin's `overhang_px` (section 5.5), so no skin can restyle or widen its corpse. Allowing overrides later is additive.
6. **Roster and tutorial — consumers.** The roster resolves the same registry entry; `roster_snake_layout` swaps its hardcoded `BOOST_OUTER_EXTRA`/`ORDINARY_OUTLINE_EXTRA` reads for the skin's metrics, the label anchor reads `metrics().head_core_radius_ratio`, and the ready check reads `head_core_is_dark` (its white-check-on-`#333` assumption becomes an explicit contract, backed by the validator's badge-contrast rule in section 5.4). Units, precisely: `overhang_px` is *per side*, so classic reports 1.0 ordinary / 3.0 boost — equal to the old mask extras and half the old outline-extra constants — and `roster_snake_layout` consumes `2 × overhang_px` where it used to subtract the full extra (`render.rs:1004-1011`); with those values every roster number is bit-identical. `RosterSnakeRequest` gains an optional `skin_ref` (serde default → classic), so `RosterSnakeCanvas.tsx` works unmodified until selection lands. The tutorial changes **zero** lines: scenes render through `render_game_state`, carry no cosmetic map, and therefore render all-classic — the correct product default, since tutorials teach the canonical read of the game.
7. **`snakeSkinColors` swatch export — byte-compatible plus one field.** The `{fill, outline, label}` JSON shape is pinned by e2e specs and stays; add one additive field `swatch`, a mandatory flat hex even for future gradient skins. Classic's hexes are declared frozen; the existing e2e pins become its permanent regression fixtures, and new-skin tests parameterize rather than fork them.
8. **Crash explosions — generic in v1.** Verified skin-independent by construction (own sprite and orange ring, painted topmost in TS). Reserve exactly one future hook — an optional `death_accent` in `colors()`, consumable through the existing `snakeSkinColors` path — and nothing more.
9. **Score celebrations — skinnable, scorer-attributed.** Today `scoreEffects.ts` hardcodes four team hexes across two functions: `getScoreEffectTeamColor` (`#5299bb` — byte-identical to classic's team-blue outline shade — and `#d45454`) and `getScoreReadoutColor` (`#2b6f8c`/`#a83232`). v1 routes all four through the scorer's `CelebrationTheme` (section 5.6): the theme supplies accent/readout colors plus a first-party effect id resolved by the existing `ScoreEffectRenderer` registry; the classic theme reproduces the four hexes byte-for-byte (asserted by the existing draw-call-recording unit-test pattern in `scoreEffects.test.ts`). Custom effect *renderers* are first-party TS code registered by id — user documents choose an id and colors, never ship code. Effect lifecycle rules (committed scores only, reset on resync, bounded cell/effect counts, reduced-motion wash) are generic and un-themeable.
10. **Team bases — skinnable, viewer-attributed.** Zone tints, goal-wall colors, and endzone text colors resolve from the viewer's skin `BaseTheme` (section 5.6); zone geometry, rotation handling, wall thickness, text content, fonts, and the which-side-is-friendly classification (`render.rs:1638-1642`) stay generic. Classic's theme is byte-identical to today's six hexes (`#e6f4fa`/`#ffe6e6`, `#7aa8c1`/`#c18888`, `#c0d8e4`/`#e4c0c0`), so goldens are unaffected. Hue locks keep friendly cool and enemy warm (section 5.4). Owner-attribution is rejected (no deterministic owner in 2v2; section 5.6).
11. **Perspective classification — generic, always.** `SnakeRole` is computed only by the renderer from the same seven inputs `snake_palette` consumes today; the derivation table in section 5.2 is normative, including its viewer-dependent FFA slot remap. Skins map fully resolved roles to paint, and the same friendly-cool/enemy-warm rule binds base and celebration themes. The hue-window validator must exist before any non-classic skin is selectable in team games; a red-looking teammate is a competitive-integrity bug, not a style choice.

## 8. Skin identity, selection, and persistence

### 8.1 Wire and state

- `GameState` gains `#[serde(default)] skins: HashMap<u32 /* user_id */, String /* SkinRef */>` beside `usernames`, documented in `fingerprint.rs`'s excluded list. `sync_hash` is untouched; the chaos suite (`server/tests/sync_equivalence_test.rs`) stays green by construction. Old snapshots, replays, and traces deserialize to an empty map and render all-classic; `trace_rca` and debug-desync are unaffected.
- The **server** reads the player's equipped `SkinRef` off their account at match prep, validates it against its catalog allowlist, and writes the map — the client never sends one, so no client can inject arbitrary strings into other clients' renderers. Invalid refs are replaced with `classic@1`, not rejected. (Shipped as `matchmaking.rs::apply_player_skin`; an earlier draft of this section had the client send the ref at join, which was never built.)
- Cosmetics never ride on `Snake` or any hashed field.
- The wire shape change bumps the WS/GAMEPLAY version-gate constants; per the documented merge hazard, any concurrent branch bumping the same constants must be merged by hand past both sides.

### 8.2 Persistence

- `selectedSkin` and `selectedBase` attributes on the DynamoDB user item, surfaced through the auth `UserInfo` response and through the CrazyGames exchange response (regenerate ts-gen types). This is the **only** store, for guests and registered players alike: accounts are created lazily but every player who reaches a match has one, and it is the record match prep reads. There is deliberately no browser-local copy — one existed briefly and did exactly what a second copy of an authoritative value does, leaving the picker claiming one skin while every opponent saw another.
- Equipping therefore needs an account. A signed-out visitor may browse and press Equip; the page holds that intent only until the sign-in prompt resolves, and drops it otherwise.
- The `/qa/skins` route (section 9) is a renderer contact sheet: its selector picks which skin the sheet previews and equips nothing. Catalog entries are compiled-in (`ClassicSkin` plus AI-authored `SkinDoc`s embedded in the bundle).

### 8.3 Per-frame resolution

Skin refs change only when the player map changes. `GameClient` interns, per snake: the resolved skin's **registry index** (not a `&dyn` reference — the registry outlives and is not self-borrowed) and the resolved `SkinColors`/`SkinMetrics` for both boost states, rebuilt on cosmetic-map change. The render loop does a vector index per snake — never a string or `HashMap` lookup, never a per-frame `colors()` call allocating `String`s — and no JSON ever enters the render path. This is what makes the section 10 no-per-frame-allocation budget hold by construction.

## 9. AI skin authoring (the skill)

A Claude Code skill at `.claude/skills/author-skin/`, shaped like the existing `debug-desync` skill. Contents:

1. **SKILL.md** — a decision tree ("can SkinDoc express it?" → document path; else → Rust escalation) plus the end-to-end procedure below.
2. **templates/skin.template.json** — every SkinDoc field present and commented, including the constraint values (boost lock, hue windows, contrast minimums, overhang cap).
3. **templates/custom_skin.rs.tmpl** — a `SnakeSkin` impl skeleton with the pass structure stubbed and the conformance-test hookup pre-written.
4. **checklists/parity-and-review.md** — the preserved-quirks list (section 6.3), the fresh-`wasm-pack`-build rule, registry registration, and the PR-screenshot convention (`docs/screenshots/skins/<name>/`).
5. **Pointers to fixtures** — `client/src/skin/fixtures.rs`, the canonical poses shared with the golden tests.

The procedure the skill encodes:

1. Copy the template → `<name>.skin.json`; fill palette/head/outline from the design brief.
2. `cargo run -p skin-schema --bin validate-skin -- <name>.skin.json` — schema plus semantic constraints, natively, in seconds. (A dedicated binary, not a `cargo test -- <filter>` invocation: everything after `--` in `cargo test` is a test-*name* filter, so a novel filename would match zero tests and exit green having validated nothing.)
3. Render a contact sheet: the dev-only `/qa/skins` route (modeled on `/qa/rating-reveal`) renders the skin across fixtures — arena scene, boosting, single-cell, dead (shows the generic corpse), roster both facings, swatch strip, every role, the themed base scene, a celebration preview, and for animated skins a film-strip row at fixed `anim_ms` samples plus the reduced-motion pose — and `node tests/capture-skin-sheet.mjs <name>` (modeled on `capture-rating-reveal.mjs`: reduced-motion stubbed, DSF pinned, `anim_ms` pinned per frame) writes PNGs the agent Reads as images and self-reviews against the brief.
4. `cargo test -p client skin_conformance` — always this fixed suite-name filter, never the skin's name (a typo'd skin name used as a filter would match zero tests and exit green). The suite auto-discovers every registered skin and fixture document and asserts the op-trace-level invariants: a distinct band layer appears when boost toggles; painted extents never exceed `metrics().overhang_px`; `colors()` returns parseable flat hexes; pass order is sane; sampling multiple `anim_ms` values yields identical op sequences with only argument changes (op-count invariance); `reduced_motion` yields a static pose.
5. Iterate 3–4 until the visuals match intent.
6. Only if the document cannot express the idea: scaffold `client/src/skin/custom/<name>.rs` from the Rust template. The same conformance suite applies automatically because it tests through the trait.
7. PR with the contact sheet committed, the drain `@skins` suite green, and an explicit statement that classic goldens were untouched.

`classic.skin.json` and its op traces double as the skill's worked example and the interpreter's regression fixture.

## 10. Performance

Budget: no per-frame allocation, no per-frame string formatting, no per-frame JSON, and O(snakes) — not O(cells) — new dispatch. The dominant per-frame cost today is thousands of immediate-mode canvas calls across the WASM boundary (~2.3k grid-dot arcs alone on 60×40); the design adds ~4 virtual calls per snake per frame, which is noise against that.

- Skin resolution is not per-frame work (section 8.3).
- `ParamSkin` interprets nothing per frame; documents compile at registration. A conformance assertion requires the interpreter's per-frame op count to equal `ClassicSkin`'s for the classic document.
- The mask hoist paints the same rectangle count with the same arithmetic — moved, not multiplied.
- Animation is architecturally near-free: the canvas already fully repaints every rAF, so an animated skin changes the *values* of ops that were being issued anyway. The op-count-invariance rule makes this a guarantee rather than a hope, and per-frame track evaluation is arithmetic over precompiled keyframes — no allocation. Base theming swaps six color constants per frame; celebration theming resolves once per goal cue, not per frame.
- One known hotspot is fixed during extraction as a proven-inert win: the head-gradient pass currently allocates `format!("{},{}")` `String` keys into a `HashSet` per cell per snake per frame (`render.rs:744-798`); replace with a packed `i64` key — identical visit order and draw ops (proven by op-trace equality), strictly less allocation.
- The cosmetic map adds ~20 bytes/player to state JSON on the React-UI loop; it changes only at join, so the parse-on-change gate almost never fires from it.
- Measurement: add a drain-harness perf smoke (time N `render()` calls on a fixed heavy fixture; assert new/old ratio within tolerance on the pinned browser). The repo has no perf harness today; this is the minimum viable one and it is gated (section 15.3, acceptance criterion 9). A dev-only frame-time readout behind a query flag is an explicit nice-to-have, not gated by anything.

Rejected for performance: per-cell skin callbacks (multiplies boundary crossings), per-frame document interpretation, and resolving skins from the JSON-parsed React state (tick-granular, parse-gated, and outside the typed render path).

## 11. Future player-created skins

Three phases, none built in v1, all preserved by construction — the skin is data, addressed by a string ref, validated by a crate that compiles everywhere:

1. **Selection** — a player-facing catalog UI over compiled-in skins. The channel, persistence, and a dev/QA selector already ship in v1 (section 8.2); this phase is only the visible UI.
2. **Composition** — a player-facing editor is a form over `SkinDoc` with live preview via a wasm `registerSkinDoc` export painting the roster fixture. User documents are stored server-side, content-addressed (`sha256:<hash>`, size-capped ~2 KB), validated by the same Rust validator CI uses, shipped to peers by ref with lazy fetch. Content addressing makes spectator and replay rendering exact by construction.
3. **Unlocks/marketplace** — `User.xp` already exists as unlock currency; catalog entries gain server-side unlock predicates. Nothing in the render path changes.

Fairness rails that make this safe to open later, all enforced by the validator that ships in v1: team-role hue windows, the mandatory contrasting boost band, opacity floors, overhang caps. Custom Rust skins remain first-party forever; the user ceiling is the versioned `SkinDoc` schema. Old clients seeing a newer `schema_version` fall back to classic.

## 12. Migration plan

Ordered, each step gated by the one before it:

1. **Instrument:** section 6.1 step 1. Gate: `cargo test -p client`, clippy, and the drain suite green.
2. **Ground truth:** section 6.4 cleanup, then the section 6.1 step 2 golden capture and the section 6.2 baselines.
3. **Corpse extraction:** move the dead-snake path into `skin/corpse.rs::paint_default` over a shared geometry walker; replace the head-gradient string keys with packed `i64` keys in the same PR. Op-trace equality proves both pixel-inert.
4. **Trait introduction:** land `SnakeSkin` + supporting types; wrap `snake_palette` + `draw_alive_snake_skin` into `ClassicSkin`; renderer computes `SnakeRole` per the section 5.2 table; hoist the occlusion mask under the section 5.5 rules; thread the `anim_ms`/`reduced_motion` fields through `GameClient.render` and every `SnakePose` (classic ignores them; goldens pin them); roster layout and ready check read `metrics()`. All unit tests, op goldens, bitmap baselines, and e2e CSS-var pins green — **this is the gate for goal 2 (section 3.1) / acceptance criterion 2 (section 16).**
5. **Registry:** `SkinRegistry` + interned handle cache in `GameClient`; everything resolves `classic@1`; add the additive `swatch` field; thread `label_ink` into the carried-food queue.
6. **Document layer:** the `skin-schema` crate (types + validator, native and wasm), `ParamSkin` with registration-time compilation (including animation tracks and sprite-atlas decoding), `classic.skin.json`, and the permanent `ParamSkin(classic) == ClassicSkin` conformance test.
7. **World dressing:** route zone tints, goal-wall colors, and endzone text colors through the viewer skin's `BaseTheme`, and the celebration palette/effect id through the scorer's `CelebrationTheme` via the extended goal cues (section 5.6). Classic themes byte-identical, proven by the op goldens and the `scoreEffects.test.ts` draw-call mocks.
8. **Identity:** the `#[serde(default)]` skins map, fingerprint-exclusion documentation, server-side catalog validation at `add_player`, DynamoDB `selectedSkin`, guest localStorage + CrazyGames allowlist, ts-gen regeneration, WS/GAMEPLAY version-gate bumps. In the same step, thread identity through the TS surfaces: `SnakeSkinInputs`, `snakeSkinColors`, and `RosterSnakeRequest` gain an optional `skin_ref` (serde default → classic), and `gamePresentation.ts` threads `gameState.skins[userId]` into the inputs it builds — so roster glyphs and results swatches follow the arena for non-classic skins.
9. **Authoring rig:** `/qa/skins` fixture route (including the dev/QA skin selector from section 8.2), `capture-skin-sheet.mjs`, then the `author-skin` skill (SKILL.md, templates, checklists, goldens as the worked example).
10. **Dogfood acceptance:** author one real document skin and one deliberately doc-inexpressible Rust skin end to end via the skill. Between them they must exercise the new expressive range: at least one animated, and at least one theming the base and the celebration. Fix the friction the runs expose before declaring the system done.

## 13. Edge cases and required behavior

| Case | Required behavior |
| --- | --- |
| Unknown, invalid, or future-versioned `SkinRef` anywhere | Resolve to classic, log once, render normally. Never a broken frame. |
| Old snapshot/replay/trace without a skins map | Deserializes to empty map; all snakes classic; `trace_rca` and debug-desync unaffected. |
| Client submits a ref not in the server catalog | Server writes `classic@1` into the map; the game proceeds. |
| Skin id for a user not in the game / stale entry | Ignored by the interning pass; no lookup escapes the players map. |
| Spectator with no local snake | Team games: `SpectatedTeam` preserves today's canonical colors. Non-team: a spectated duel classifies both snakes `Enemy` (both red, as today); FFA uses the spectator slot mapping. |
| Roster glyph for a boosting player | Unchanged in v1: TS never passes `boost_active` today; the gap is preserved and documented. |
| Single-cell snake in any skin | Renderer: no occlusion mask (section 5.5, alive and dead). Skin conformance: boost layers yes, head overlay no. |
| Skin paints beyond its reported overhang | Conformance failure; the skin cannot ship. |
| Two skins report identical palettes for opposing roles | Validator rejects: inter-role distinctness and hue windows are hard constraints. |
| Non-classic skin in a team game before the hue validator exists | Forbidden; validator availability gates team-game selectability. |
| `paint_alive` returns an error mid-frame | Same contract as the celebration callback: isolate, log, hard-reset transform/alpha, continue the frame. |
| Cosmetic map changes mid-game (late join) | Interned handle vector rebuilds on the map's change; next frame uses the new skin. |
| Animated skin on the roster / in goldens / on contact sheets | Renders the pinned-`anim_ms` static pose; only the arena and tutorial animate. |
| `prefers-reduced-motion` | `reduced_motion: true` in every pose; skins render their static variant (conformance-enforced); the generic celebration reduced-motion wash is unchanged. |
| Scorer's skin unknown or unresolvable at celebration cue time | Classic celebration theme; log via the section 14 fallback counter. |
| Cosmetic map changes while a celebration is in flight | The celebration keeps the theme it resolved with (section 5.6). |
| Both duel players run the same skin package | No conflict by construction: bases are viewer-attributed, celebrations scorer-attributed. |
| Replay of a game with a user document skin (phase 2) | Content-addressed ref pins the exact bytes; replay renders what spectators saw. |

## 14. Observability

The design's failure mode is silent cosmetic degradation, so it must be visible. Add bounded-cardinality metrics and logs without game-ID or user-ID labels:

- fallback-to-classic resolutions, by reason (unknown ref, invalid ref, future schema version, corrupted persistence, unresolvable celebration attribution);
- server catalog rejections at `add_player` (invalid ref rewritten to classic), by reason;
- `paint_alive` error isolations per frame (the section 13 error contract firing at all is a bug report);
- `SkinDoc` registration/validation failures by constraint (hue window, contrast, overhang, schema version);
- once the channel ships: skin selection distribution by catalog entry, to know whether non-classic skins are actually reaching games.

A sustained nonzero fallback or isolation rate after a deploy is the skins equivalent of a sync incident: cosmetically invisible to most players, and exactly what these counters exist to catch.

## 15. Test plan

### 15.1 Native (CI, existing nextest job)

- Golden op-trace equality across the fixture corpus for every migration step (sections 6.1, 12).
- `ParamSkin(classic.skin.json)` == `ClassicSkin` op traces, forever.
- Conformance suite for every registry entry: boost distinctness, overhang honesty, parseable flat hexes, sane pass order, animation op-count invariance across sampled `anim_ms` values, and a static pose under `reduced_motion`. The metrics-honesty check lands **with** the trait, not after the first non-classic skin.
- World-dressing parity: classic `BaseTheme` reproduces the six zone/wall/text hexes in the op goldens; classic `CelebrationTheme` reproduces the four `scoreEffects.ts` hexes in its draw-call-mock unit tests.
- Validator unit tests: WCAG ink rule, hue windows, boost lock, overhang cap, ready-check badge contrast (light-core rejection), schema-version rules (additive-only within a major), rejection messages.
- Registry fallback determinism; interning rebuild on map change.
- A test skin whose `paint_alive` errors after mutating transform/alpha: assert the error is logged, canvas state is hard-reset, and subsequent snakes, walls, and labels in the same frame paint correctly (the section 13 error contract).
- Existing renderer unit tests (`render.rs:2738-3451`) stay green unmodified through step 4 of the migration.

### 15.2 Sync and server

- Chaos suite (`sync_equivalence_test.rs`) green with the skins map present, absent, and mutated mid-game.
- Fingerprint tests: skins map excluded; a skins-only difference produces identical `sync_hash`.
- Server catalog validation at `add_player`: invalid ref → classic, valid ref → stored; snapshot round-trips preserve the map.
- Serde-default tests against stored historical completed-game payloads.

### 15.3 Browser (drain harness; local unless the CI job is funded)

- `@skins` bitmap baselines for classic across fixtures (section 6.2).
- Existing pinned CSS-var hex assertions untouched and green — classic's frozen-hex regression fixtures.
- Contact-sheet capture for each new skin, committed with its PR.
- Roster and swatch color parity with the arena for a non-classic skin (the step 8 TS identity flow).
- Base dressing and celebration theming for a non-classic skin: themed zones/walls/text render, the scorer's celebration plays with its palette, and classic scenes remain byte-identical.
- An animated skin at two pinned `anim_ms` values produces two committed baselines; `reduced_motion` produces the static pose.
- Perf smoke: heavy-fixture `render()` timing ratio within tolerance.

### 15.5 Not built: Gate 2

The `@skins` bitmap suite described in section 6.2 was **not implemented**. What
shipped is `client/web/tests/capture-skin-sheet.mjs`, which captures contact
sheets for human and agent review — it performs no comparison, there are no
committed baselines, and there is no `npm run skin:approve`. The PNGs under
`docs/screenshots/skins/` are review artifacts, not a diffable control.

Consequences, stated plainly so nobody assumes otherwise: classic's parity rests
entirely on Gate 1, the native op traces. That is a strong gate for everything
expressible as ops, and it is the gate that CI actually runs — but it cannot see
a change in how the browser *rasterizes* the same ops, and it stops being a
proxy for appearance at all once `drawImage` enters the op set. Building Gate 2
is a prerequisite for the shading work, where the op stream necessarily changes;
see `specs/skin-shading-prd.md`.

### 15.4 CI gap (decision required)

No CI job builds WASM or JS today. Gate 1 runs in CI natively; Gate 2 and the `@skins` suite are local-only unless a wasm-pack + Playwright job is added. Options: (a) fund the CI job — recommended if skins ship beyond classic; (b) accept process-enforced local gating with the skill's checklist as the control. The decision is owned by Product with Client sign-off and must be made before migration step 2 merges (the baselines land with their enforcement story known); silently drifting into (b) is not acceptable.

## 16. Acceptance criteria

Skins v1 is complete when:

1. `SnakeSkin`, `SkinRegistry`, `ClassicSkin`, `ParamSkin`, and the `skin-schema` crate exist with the boundary exactly as ruled in section 7.
2. With every snake resolving `classic@1`: golden op traces are byte-identical to the pre-refactor captures, pinned-browser baselines are byte-identical, and the existing e2e hex pins pass unmodified — on arena, roster, tutorial, and swatch surfaces.
3. The occlusion mask, roster layout, label ink, and ready check consume `metrics()`/`colors()` reports; no generic code reads a skin constant.
4. The skins map rides the fingerprint-excluded channel; the chaos suite and fingerprint tests prove `sync_hash` is skin-blind; old snapshots/replays/traces render all-classic.
5. The server validates refs against its catalog at join; unknown refs degrade to classic everywhere, deterministically.
6. `classic.skin.json` exists and the `ParamSkin`-vs-`ClassicSkin` conformance test is mandatory in CI.
7. The conformance suite (boost distinctness, overhang honesty, flat-hex colors) gates every registry entry, including classic.
8. The `/qa/skins` route (including the dev/QA selector), contact-sheet script, and `author-skin` skill exist; one document skin and one Rust-escalation skin — between them at least one animated and at least one theming the base and celebration — have been authored end to end through the skill, are selectable via the dev/QA selector, have appeared in a real game through the join-time channel, and the friction they exposed has been fixed.
9. The perf smoke shows no regression; the head-gradient allocation fix and interned handles are in.
10. The section 7 rulings (NOS, food, dead snakes, explosions, celebration/base attribution, perspective) are reflected in code comments or docs where future work would otherwise "discover" them as bugs, and the section 14 observability counters exist.
11. The CI decision (15.4) is made and recorded.
12. The animation clock reaches every surface with the section 5.6 semantics; conformance enforces op-count invariance and reduced-motion static poses; every golden and baseline pins `anim_ms`.
13. Bases and celebrations are themeable exactly as ruled (viewer- and scorer-attributed respectively); classic themes are byte-identical to today; no hardcoded snake- or team-appearance hex remains in `scoreEffects.ts`.

## 17. Risks and feasibility

The rewrite is feasible and lower-risk than it looks, because the current code already funnels every surface through one painter and one palette. The primary risks:

1. **Ground-truth ordering.** Both parity gates must exist before any extraction; a refactor that lands first has nothing to diff against. Mitigation: migration steps 1–2 are mechanical and reviewable in isolation.
2. **Quirk erosion.** The deliberate pixel quirks (section 6.3) are easy to "fix" accidentally. Mitigation: they are checklist items and golden-encoded; any intentional cleanup is a separate PR with regenerated goldens.
3. **Wrapper semantics.** Op-trace equality can pass while pixels differ if `PaintCtx` subtly normalizes styles or formats floats. Mitigation: the Web arm is 1:1 forwarding, and the independent bitmap gate backstops it.
4. **Version-gate merge hazard.** The wire change bumps WS/GAMEPLAY constants that git merges silently; bump past both sides by hand when branches collide.
5. **Stale-WASM false passes.** Fresh worktrees silently run the main repo's old WASM through symlinked node_modules; every pixel gate and the skill fingerprint the loaded pkg first.
6. **Team-readability knife-edge.** The hue validator must land before any non-classic skin is selectable in team games; this is a hard gate, not a nice-to-have.
7. **`ParamSkin` scope creep.** Each new document capability pressures the flat-hex assumptions of the swatch, label ink, and e2e pins, and animation multiplies the temptation (time-varying structure, not just values). Mitigation: mandatory flat `swatch`/`label_ink` from schema v1, and the op-count-invariance rule as a hard conformance gate — animation may vary arguments, never structure.
8. **Attribution surprises.** Viewer-attributed bases mean other players never see your base theme, and scorer-attributed celebrations depend on the cosmetic map being resolvable at cue time. Both rules are deliberate (section 5.6) and cheap to explain, but they are product decisions a playtest could overturn — revisiting them changes plumbing, not architecture, since both are resolved through the same interned map.
9. **Boundary re-litigation.** The `BoostCanisterMark` NOS duplication and the sprite-based crash explosion look like skin gaps to future readers. Mitigation: section 7 rulings plus acceptance criterion 10.

Proceed with the hybrid trait-plus-document design. Treat golden capture, the conformance suite landing with the trait, the hue validator, and the dogfooded skill run as launch prerequisites.

> **Update (skin shading engine, S2).** Gate 2 was built:
> `client/web/tests/skins/skin-parity.spec.mjs` compared every shipped skin
> against a committed baseline sheet in a headless browser, run by the
> `skin-pixels` CI job.
>
> **Update (textured skins).** It has since been removed as too expensive for
> what it caught — a Chromium install per pull request and a megabyte of
> baselines that had to be re-blessed whenever any shared painting changed. The
> CI job it lived in is now `wasm-build`, which keeps the one thing only that
> job did: compiling to `wasm32`. Section 15.5's account of what was missing is
> left intact, and is once again accurate.
