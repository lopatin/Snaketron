# PRD: Boost for matchmade modes

| Field | Value |
| --- | --- |
| Status | Implemented and under release validation |
| Product | Snaketron gameplay |
| Scope | Duel, 2v2, Free-for-All, and Solo; Quickmatch and Competitive |
| Owners | Product / Gameplay / Client / Server |
| Last updated | 2026-08-07 |

## 1. Executive decision

Boost is feasible. It is a shared-engine timing feature, not just a visual effect or a server timer.

The proposed design is:

- Add twelve fixed, symmetric Boost pads to canonical duel, 2v2, and 40x40 Free-for-All maps: four outer 2x2 full-tank packets and eight inner 1x1 quarter-tank packets. Solo has an unlimited full tank and no inert pickups.
- Give every snake a stored Boost meter. Packets add charge up to 100%; collecting a packet never activates Boost.
- Hold Space is the default input mode: keydown sends a predicted, server-authoritative `ActivateBoost` command and keyup sends `DeactivateBoost`. A continuous physical hold survives fuel depletion and issues one fresh activation when charge returns. A compact control beside the key hints lets the player choose Hold or Toggle. This preference is client-local and persisted; the mode-agnostic server processes the same explicit, idempotent start/stop commands in either mode.
- Give every snake an authoritative `speed_milli` property. `1000` is 1.0x and `2000` is 2.0x. Only internal Boost-state transitions may change speed after snake construction. Movement reads speed; collision and other gameplay logic never branch on Boost.
- Support a configurable total speed from 1.0x through 2.0x, with a recommended launch value of 1.5x.
- Use one fixed 50 ms simulation quantum for every Boost-enabled match. The quantum never changes when a player activates Boost.
- Resolve at most one cell of movement per snake per quantum. Integer movement credit converts each snake's own speed into movement opportunities.
- Keep movement derived in the shared Rust engine. Do not send per-cell network events.
- Release with a coordinated maintenance cutover. Terminate active games, deploy the server and client together on gameplay protocol v6, and reopen Quickmatch and Competitive together. There is no staged production rollout, mixed-version operation, or active-game migration.

Performance impact is bounded but real. Boost-enabled matchmade modes run 20 simulation quanta per second instead of 10, so server and replica step frequency approximately doubles. The browser's current rebuild-style prediction can do roughly 4x today's tick-replay work because both target frequency and lag-window ticks double. With at most four snakes and compressed bodies, this is feasible, subject to server and low-end-client performance gates.

Two existing engine changes are prerequisites:

1. Preserve the originating tick and event sequence returned by `GameEngine::run_until`; the v2 executor currently republishes a catch-up batch at the engine's final tick.
2. Restore the production `TickHash` heartbeat and schedule it by elapsed milliseconds, not a fixed number of ticks.

## 2. Product problem

Team matches currently move every living snake one grid cell per 100 ms tick. There is no individual speed, stored power-up charge, fixed pickup site, or player-triggered movement modifier.

Players need predictable map objectives that they can collect, save, and activate at a tactically useful time. The mechanic must not:

- change a teammate's or opponent's speed;
- activate merely because a packet was collected;
- let a snake jump through an intermediate collision;
- change how a match ends (team matches race to a score, with no clock);
- let a client claim charge, speed, or pad state;
- fork native-server and WASM-client simulation; or
- require per-movement WebSocket or Valkey messages.

## 3. Goals and non-goals

### 3.1 Goals

1. Add collectible Boost packets to canonical `GameType::TeamMatch { per_team: 1 | 2 }` and 40x40 `FreeForAll` games, plus unlimited Boost without pickups in Solo.
2. Make packet locations fixed, symmetric, visible, and learnable.
3. Let each snake store packet charge up to a clearly displayed 100% capacity.
4. Let the owning player activate and stop Boost with Space, defaulting to hold-to-boost, with immediate local prediction subject to server reconciliation and uninterrupted physical-hold intent across depletion/refill.
5. Make speed an intrinsic property of every snake and the sole input to movement cadence.
6. Support every integer `speed_milli` from `1000` through `2000` deterministically.
7. Resolve movement and collision one grid cell at a time at every supported speed.
8. Preserve wall-clock match duration, committed lag, prediction cap, liveness windows, checkpoint cadence, and approximately one-second sync heartbeat.
9. Preserve replay, snapshot resync, failover, and native/WASM equivalence for games created after the cutover.
10. Keep added network and storage traffic small.

### 3.2 Non-goals

- Boost in `GameType::Custom` or unsupported future team sizes.
- Host-controlled Boost settings in the current custom-game UI.
- Random pad locations, strengths other than the full/quarter layout contract, offensive power-ups, or speeds below 1.0x or above 2.0x.
- Server-side storage, negotiation, or interpretation of a player's Hold/Toggle input preference. Input mode is client-local policy over explicit start/stop commands.
- Continuous sub-cell physics or per-snake/asynchronous tick intervals.
- Boost-specific score, XP, MMR, or leaderboard rewards.
- A staged production rollout, compatibility cohort, feature capability negotiation, or migration of live games across the release.
- A new audio system. V1 is visual-only unless audio assets and accessibility behavior are approved separately.

## 4. Repository baseline

The current repository provides a strong base and several constraints:

- `common/src/constants.rs` defines a 100 ms default tick. Non-custom games copy it into `GameProperties.tick_duration_ms` in `common/src/game_state.rs`.
- `GameState::tick_forward` currently moves every alive snake once. `Snake::step_forward` in `common/src/snake.rs` is the existing one-cell geometry primitive and should remain so.
- `GameEngine::run_until` in `common/src/game_engine.rs` maps wall time to ticks, keeps authoritative state 500 ms behind wall time, and predicts ahead with the same shared engine.
- The browser calls `rebuildPredictedState` on `requestAnimationFrame`. The current rebuild path clones committed state and replays the full lag window whenever the prediction target advances.
- The production v2 actor in `server/src/game_executor_v2.rs` wakes every 10 ms. That poll is already frequent enough for a fixed 50 ms simulation quantum.
- Duel and 2v2 are `TeamMatch { per_team: 1 }` and `{ per_team: 2 }`; matchmaking creates both on a 60x40 arena with 10-cell end zones.
- Native executors, replicas, replays, and the browser WASM client all use the common Rust engine. Speed, charge, pad, and movement-credit rules therefore belong in `common`, not in a server-only loop or TypeScript timer.
- Recovery checkpoints already contain complete `GameState`, and sync health fingerprints `GameState::sync_hash`.

## 5. Gameplay requirements

### 5.1 Eligible games

After the cutover, Boost is present for canonical 60x40 `TeamMatch { per_team: 1 | 2 }`, canonical 40x40 `FreeForAll`, and Solo. Team and FFA use their mode-specific twelve-pad layouts; Solo uses an unlimited full tank with layout version 0 and no pads.

Both Quickmatch and Competitive use the same resolved balance rules. `GameProperties.boost: Option<BoostConfig>` is the source of truth; an additional `enabled` flag would create an unnecessary contradictory state. Custom, off-canonical collectible maps, and unsupported future team sizes have `None` and no pads.

### 5.2 Recommended configuration

| Setting | Launch default | Validation |
| --- | ---: | --- |
| Active speed | 1.5x (`1500`) | `1000..=2000` milli-normal units |
| Full-tank runtime | 3,000 ms | Positive and divisible by 50 ms |
| Inner packet charge | 750 ms (25%) | Exactly one quarter of capacity and divisible by 50 ms |
| Outer packet charge | 3,000 ms (100%) | Exactly full capacity; snapshotted on each outer pad |
| Pad cooldown | 8,000 ms | Positive and divisible by 50 ms |
| Pad count | 12 | Four outer 2x2 plus eight inner 1x1; layout-version controlled |
| Normal movement interval | 100 ms | Engine constant |
| Simulation quantum | 50 ms | Fixed for every Boost-enabled match |

An inner packet fills one quarter of an empty meter; an outer packet fills it completely. Charge is expressed as milliseconds of active runtime so the configured speed does not alter how long a full tank lasts. A partially full snake may consume either strength, charge caps at 100%, and overflow is discarded under the ordinary collection rule.

These are snapshotted balance values, not live environment reads. A configuration change affects only games created afterward. The speed is total speed: 1.5x means 15 cells per second on average, not normal speed plus another 1.5x.

### 5.3 Packet collection and storage

1. A packet is collectible when its pad has no `respawn_at_tick`.
2. After movement and collision resolution, a surviving snake whose head occupies an available pad may collect it.
3. Collection adds `min(pad.charge_ms, capacity_ms - charge_ms)` to that snake.
4. Collection never changes `boost_active` or `speed_milli`.
5. Stored charge has no passive decay. Unspent charge is preserved when Boost is manually stopped and remains until it is consumed, death, scoring reset, or match completion.
6. A snake at 100% does not consume a packet; the packet remains available.
7. A snake with any available capacity consumes the packet, fills up to 100%, and discards overflow.
8. A consumed pad receives an absolute `respawn_at_tick` derived from its snapshotted cooldown.
9. Packet collection never changes score, carried food, XP, or MMR.

### 5.4 Space input and Boost state commands

1. The default Hold mode sends `GameCommand::ActivateBoost { snake_id }` on the first non-repeat Space keydown and `GameCommand::DeactivateBoost { snake_id }` on keyup through the same predicted, at-least-once command path as turning.
2. The optional Toggle mode sends `ActivateBoost` on a Space press when the client's desired state is inactive and `DeactivateBoost` on the next Space press when its desired state is active. Keyup does not issue a gameplay command in Toggle mode.
3. A compact Hold/Toggle control beside the keybinding hints stores the preference locally on that client. Hold is the fallback for a missing, invalid, or unavailable stored preference. The wire protocol, server, shared engine, snapshots, and replays do not store or branch on input mode.
4. Neither command contains speed, amount, duration, charge, or pad data. The server authenticates that the submitting user owns `snake_id` for both variants.
5. On its scheduled quantum, `ActivateBoost` atomically sets `boost_active = true` and copies the configured speed into `snake.speed_milli` when the snake is living, inactive, and charged. An activation for a dead snake, empty meter, already-active snake, completed game, or unsupported mode is an idempotent no-op.
6. On its scheduled quantum, `DeactivateBoost` atomically clears `boost_active` and restores normal speed when the snake is living and active. A deactivation for a dead or inactive snake, completed game, or unsupported mode is an idempotent no-op. It never clears or spends the unused charge and never resets movement credit.
7. Both variants receive the normal scheduled-command protocol outcome and execute in stable command-ID order before movement credit is added. A successful start or stop can therefore affect movement in that quantum without granting a phase-dependent credit bonus.
8. If activation runs with an empty meter and the snake collects a packet later in the same quantum, it remains inactive. The player must initiate Boost again.
9. Browser keyboard repeat emits no command. In Hold mode, physical key release ends Boost; window blur or a document `visibilitychange` to hidden is treated as a lost release and also ends it. In Toggle mode, a later press ends Boost; blur, hidden visibility, and transient reconnects preserve that explicit latched intent. Arena teardown and input-mode changes clear local state and issue a best-effort `DeactivateBoost` when the match can still accept commands. After reconnect or resync, Hold mode repairs an authoritative active snapshot when Space is no longer physically down, while Toggle mode preserves an authoritative active auto-Boost.
10. Hold intent and authoritative active state are distinct. If a valid Hold activation later drains to zero while the same physical Space or pointer press remains uninterrupted, the client preserves that intent while inactive and sends exactly one fresh `ActivateBoost` as soon as authoritative or predicted charge becomes usable again. Releasing while empty cancels the intent. A new press that begins at 0% does not arm a future pickup.

### 5.5 Charge consumption

- At the start of every active quantum, the engine reserves exactly `tick_duration_ms` of charge and retains boosted speed for that entire quantum.
- A due `DeactivateBoost` executes before this reservation. A manually stopped snake spends no charge for that quantum and retains all unused charge.
- `capacity_ms` and every pad's `charge_ms` must be divisible by 50 ms, so an active snake always has a whole funded quantum remaining.
- The last funded quantum uses boosted speed. If charge is still zero after packet collection, internal lifecycle code deactivates the snake and restores speed to `1000` for the next quantum.
- If that last funded movement lands on a packet, collection can refill the meter before finalization and Boost continues without a one-quantum interruption.
- An active snake can collect packets whenever it has room. Charge is capped at 100%; speed never stacks or multiplies.
- Charge burns with game wall time while active, including quanta in which fractional movement credit does not produce a cell.

### 5.6 Death, scoring, disconnect, and completion

- Wall, base, self, snake, and timeout rules retain their ordinary outcomes.
- Death and team-score respawn clear stored charge and active state, restore speed to `1000`, reset movement credit to `0`, and discard all queued player commands targeting that snake, including turns, activation commands, and deactivation commands.
- Boost never transfers to a teammate or a new life.
- Disconnect alone does not clear charge or active state. The authoritative game continues and a reconnect snapshot restores the exact state. Hold mode sends the ordinary explicit `DeactivateBoost` cleanup command if Space is no longer physically held; Toggle mode preserves the restored latched active state until the next toggle or depletion.
- Executor failover preserves charge, active state, speed, and movement credit.
- Match completion freezes movement, meter, and pad cooldown state with the rest of the game.

### 5.7 Contention

- Collision resolution precedes packet collection.
- If two heads enter the same packet cell and collide, both die and the packet remains.
- If a malformed state contains multiple surviving heads on one packet, the lowest snake ID wins. Packet decisions use stable pad-ID order, then snake-ID order.
- A pad becomes available at exactly its respawn tick. If a surviving head occupies it during that quantum's collection phase, ordinary collection rules apply.

### 5.8 Mode-aware food placement

Initial food and runtime refills use one shared, mode-aware sampler before applying the existing snake, food, pad, and bounds exclusions:

- In Duel and 2v2 (`TeamMatch`), the goal-to-goal axis retains the established center-biased normal distribution, while the cross-field axis along an end-zone width is uniform. Canonical state stores end zones left/right, so this is center-biased `x` and uniform `y`; the ordinary client rotation presents that as vertical center pressure and even horizontal coverage.
- Solo and Free-for-All use uniform distributions on both axes. Custom non-team games inherit the same two-axis uniform rule.
- Distribution changes never place team food inside an end zone and never relax permanent Boost-pad footprint exclusion.

The sampler is deterministic from the snapshotted engine PRNG. It belongs in the shared Rust game state rather than a server wrapper or client renderer.

## 6. Predictable map layout

### 6.1 Layout contract

Pad positions are generated once at match creation and serialized into the arena. They are not randomized, read from live configuration during a game, or duplicated as TypeScript constants.

Every layout version must guarantee:

- 180-degree rotational and top/bottom mirror symmetry;
- equal travel distance for mirrored team starts;
- positions inside `Arena::main_field_bounds`;
- unique, in-bounds cells;
- no wall, goal-boundary, starting-snake, or respawn-head overlap; and
- permanent exclusion from food spawning while a packet is available or cooling down.

If a supported team map cannot satisfy its configured layout, match creation fails closed. Pads never relocate during play.

### 6.2 V3 layout

The canonical 60x40 layout uses twelve symmetric pads. Coordinates are the top-left of each pad footprint:

| IDs | Footprint | Charge | Coordinates |
| --- | --- | --- | --- |
| 0–3 | 2x2 cells | 3,000 ms / 100% | `(14,4)`, `(14,34)`, `(44,4)`, `(44,34)` |
| 4–11 | 1x1 cell | 750 ms / 25% | clockwise: `(26,12)`, `(33,12)`, `(37,16)`, `(37,23)`, `(33,27)`, `(26,27)`, `(22,23)`, `(22,16)` |

The larger full packets sit farther toward the four field corners. The eight smaller packets form a compact, nearly regular octagonal ring around the field center. Footprint and canister construction are the visual value cues: quarter packets use one diagonal blue NOS canister, while each physically larger 2x2 full packet uses one large upright NOS canister with a legible wordmark. The implementation derives and materializes the top-left anchors from the exact canonical arena and end-zone bounds using one documented integer rule. Any position, count, or footprint change requires a new `spot_layout_version` so completed games and replays retain their original geometry.

All twelve packets are available at gameplay start and cool down independently after collection. An unavailable/cooling pad renders nothing—no gray placeholder. Every footprint cell remains permanently excluded from food spawning, including while its pad is cooling down.

## 7. Snake speed and simulation timing

### 7.1 Snake-owned speed invariant

Every snake has speed, movement credit, and Boost state, including a snake that has never collected a packet:

```text
normal:  boost_active = false, speed_milli = 1000
active:  boost_active = true,  speed_milli = BoostConfig.speed_milli
```

At every serialized tick boundary in a Boost-enabled match:

- `0 <= boost_charge_ms <= BoostConfig.capacity_ms`;
- `boost_charge_ms % 50 == 0`;
- inactive implies `speed_milli == 1000`;
- active implies `boost_charge_ms > 0` and `speed_milli == BoostConfig.speed_milli`; and
- `0 <= movement_credit < 100_000`.

When `GameProperties.boost == None`, charge must be `0`, active must be `false`, and speed must be `1000`.

`Snake.speed_milli` is the sole movement-rate input. Only crate-private Boost lifecycle methods may update charge, active state, and speed after construction:

- packet collection changes charge only;
- successful activation sets active state and configured speed atomically;
- successful manual deactivation clears active state and restores normal speed atomically while preserving charge and movement credit;
- charge exhaustion clears active state and restores normal speed atomically; and
- death/scoring reset clears Boost, restores normal speed, and resets credit.

Movement, turns, collision, food, scoring, rendering geometry, bots, and replay advancement must not ask whether a snake is boosted. They consume ordinary snake speed, movement opportunities, or rendered state. The client may inspect Boost state for HUD and cosmetic effects.

Snapshots and event application validate these invariants. Speed is authoritative engine state, never a client claim.

### 7.2 Fixed 50 ms match quantum

Every Boost-enabled duel, 2v2, FFA, and Solo match stores `50` in `GameProperties.tick_duration_ms` at creation and keeps it through completion. Boostless/off-canonical modes retain 100 ms; Custom retains its configured `settings.tick_duration_ms`.

The fixed 50 ms value is the only timing mode introduced by Boost. It gives a 2.0x snake one cell per quantum and a normal snake one cell every two quanta. A Space start or stop changes only the owning snake's speed through Boost state; it never changes the match quantum.

The recommended capacity, packet charge, cooldown, 500 ms committed lag, and 1,000 ms prediction bound all divide exactly into 50 ms quanta. Team matches have no time limit at all: they end when a team reaches the queue's score target (25 Quickmatch, 50 Competitive).

### 7.3 Movement credit

Normal physical speed remains one cell per 100 ms. Each living snake owns an integer residual:

```text
MOVE_THRESHOLD = 1000 * 100 ms = 100_000 milli-ms

each simulation quantum:
    movement_credit += snake.speed_milli * tick_duration_ms
    if movement_credit >= MOVE_THRESHOLD:
        movement_credit -= MOVE_THRESHOLD
        add snake to the generic movers set
```

No floating point participates in speed, fuel, movement, cooldown, or tick conversion. At the maximum `speed_milli = 2000`, one 50 ms addition is exactly the threshold, so a snake can become due at most once per quantum.

| Speed | Credit per 50 ms quantum | Average result |
| ---: | ---: | ---: |
| 1.0x (`1000`) | 50,000 | 10 cells/sec |
| 1.25x (`1250`) | 62,500 | 12.5 cells/sec |
| 1.5x (`1500`) | 75,000 | 15 cells/sec |
| 1.75x (`1750`) | 87,500 | 17.5 cells/sec |
| 2.0x (`2000`) | 100,000 | 20 cells/sec |

Residual credit is preserved when Boost starts and ends, whether the end is manual or caused by depletion, so pressing or releasing Space does not grant a phase-dependent movement bonus. It resets only when the snake itself is reset by death or scoring.

### 7.4 Deterministic quantum order

The shared engine performs this order:

1. Apply due pad respawns and existing system/status transitions. If the game is no longer running, stop under the existing status semantics.
2. Materialize due player commands in stable ID order. Execute due `ActivateBoost` and `DeactivateBoost` commands before movement credit and keep due turns queued separately.
3. For each active snake, reserve one quantum of charge while retaining boosted speed through this quantum.
4. Add credit from each living snake's `speed_milli` and compute the generic `movers` set.
5. For each mover, apply at most one oldest legal queued turn. Turns for nonmovers and extra turns remain queued.
6. Advance all movers simultaneously by exactly one cell through `Snake::step_forward`.
7. Resolve ordinary walls, bounds, bases, heads, bodies, tails, death, scoring, and respawn from the complete before/after state.
8. Resolve food and packet collection for surviving heads. Collection can refill an active snake whose charge was reserved in step 3.
9. If an active snake has zero charge, internally deactivate it and restore speed to `1000` for the next quantum.
10. Run periodic work at its preserved wall-clock cadence and increment the simulation tick.

Only the internal lifecycle in steps 2, 3, 8, and 9 understands Boost state. Movement reads speed. Collision receives the arena and generic movers set and contains no Boost/no-Boost branch. Because step 2 precedes charge reservation, a successful manual stop preserves that quantum's unused charge; command-ID ordering deterministically resolves a start and stop scheduled for the same quantum.

Collision candidates are snakes that moved in the quantum; every living snake body remains an obstacle. Therefore:

- a mover entering a stationary snake's head or body dies, while the stationary snake does not die solely because it did not move;
- two movers entering the same head cell both die;
- a stationary tail has not vacated its cell;
- a moving tail uses the existing simultaneous post-move occupancy rule; and
- no snake can tunnel through a wall, body, food, goal boundary, or packet cell.

### 7.5 Turn semantics

The existing rule becomes “one applied turn per snake movement step,” not “one turn per global tick.” At a 50 ms quantum, a normal snake has one nonmovement quantum between cells. Applying multiple turns before geometry advances could otherwise collapse a double-turn into one cell or allow a reversal.

Due turns for a nonmover remain queued. On its next movement opportunity, the oldest legal turn applies immediately before that snake's one-cell step. Additional turns remain queued for later steps. Native, replica, WASM, replay, bot, and load-test paths use the same ordering.

### 7.6 Rejected timing alternatives

- **Change the global tick when Space is pressed:** rejected because it changes every snake, command assignment, timer, prediction window, and replay label.
- **Give each snake an asynchronous timer:** rejected because it destroys simultaneous deterministic collision resolution.
- **Move a 2.0x snake twice inside a 100 ms tick:** rejected because collision, collection, scoring, and turns need a boundary between the two cells. A visible 50 ms quantum provides that boundary directly for server and client prediction.
- **Support speeds above 2.0x in v1:** rejected because it requires additional quantum sizes or multiple movement substeps. The gameplay value does not justify the extra timing, performance, testing, and rendering complexity.

## 8. Shared state, commands, and events

The exact Rust names may change, but the state contract is:

```rust
struct BoostConfig {
    speed_milli: u16,          // 1000..=2000
    capacity_ms: u32,          // launch: 3000
    packet_charge_ms: u32,     // inner packet: 750, exactly 25%
    pad_respawn_ms: u32,       // launch: 8000
    spot_layout_version: u16,  // launch: 3
    rules_version: u16,        // launch: 2
    unlimited: bool,           // Solo only; full meter and no pads
}

struct BoostPad {
    id: u8,
    position: Position,        // top-left footprint cell
    charge_ms: u32,            // packet-specific 750 or 3000
    size_cells: u8,            // square side: 1 or 2
    respawn_at_tick: Option<u32>,
}

struct SnakeBoost {
    charge_ms: u32,
    active: bool,
    intent: bool,
}

struct Snake {
    // existing fields...
    speed_milli: u16,
    movement_credit: u32,
    boost: SnakeBoost,
}
```

Recommended placement:

- `GameProperties.boost: Option<BoostConfig>` contains immutable per-match rules.
- `GameProperties.tick_duration_ms` is `50` for every Boost-enabled match.
- `Arena.boost_pads: Vec<BoostPad>` contains fixed top-left positions, per-pad charge, footprint size, and live availability.
- Every `Snake` contains its speed, residual credit, and stored/active Boost state.

All fields are mandatory in gameplay protocol v6. The maintenance cutover terminates old active games, so cross-version active recovery is not supported. Deserialization-only defaults keep immutable completed history readable for two exact prior shapes: boostless 100 ms team/Solo/FFA results, and 50 ms Boost team results that used the old 90-second clock. Nonterminal legacy snapshots, stripped current snapshots, and malformed current completions remain rejected. Constructors must still initialize non-Boost snakes to normal speed, zero credit, and empty Boost state.

### 8.1 Command

```rust
GameCommand::ActivateBoost { snake_id: u32 }
GameCommand::DeactivateBoost { snake_id: u32 }
```

The existing `CommandScheduledV2` stream record is the authoritative replay record for both state transitions. A separate “client says active” message is neither needed nor trusted. Both commands are idempotent at execution time, and command retry and deduplication use the existing stable client command identity.

### 8.2 Events

Use absolute post-state for packet collection:

```rust
BoostPacketCollected {
    pad_id: u8,
    snake_id: u32,
    charge_ms_after: u32,
    respawn_at_tick: u32,
}
```

Applying the event sets the exact pad cooldown and absolute charge while preserving the snake's existing active state. Packet event application never activates Boost and never writes speed. Duplicate delivery at the same stream position is harmless.

Activation, manual deactivation, charge reservation, depletion, and pad respawn are deterministic from scheduled commands and snapshotted state and do not require per-quantum events. Server-side metrics may observe those transitions without putting them on the gameplay stream.

### 8.3 Sync fingerprint

`GameState::sync_hash` must include, in stable order:

- complete Boost configuration, rules version, layout version, and fixed quantum;
- pad IDs, positions, and absolute respawn ticks;
- each snake's speed, movement credit, charge, and active state; and
- all queued activation, deactivation, and turn commands through the existing queue fingerprint.

No wall-clock-only cosmetic timestamp belongs in the fingerprint.

## 9. Server changes

### 9.1 Match creation

The match builder must:

1. allowlist canonical `TeamMatch { per_team: 1 | 2 }`, 40x40 FFA, and Solo;
2. resolve and validate one `BoostConfig`;
3. store the fixed 50 ms Boost quantum in game properties;
4. require capacity, packet charge, and cooldown values to be divisible by 50 ms;
5. generate and validate the versioned symmetric pad layout;
6. initialize every snake at normal speed with zero credit and empty charge, except Solo's full unlimited tank; and
7. create pads before initial food and permanently exclude their cells from food placement.

The server never re-reads multiplier, capacity, packet charge, cooldown, or layout configuration during an active match.

At process startup, `SNAKETRON_BOOST_SPEED_MULTIPLIER` optionally overrides the default `1.500` multiplier. It accepts deterministic milli-speed precision from `1.000` through `2.000` (for example `1.25` or `1.750`). Invalid or out-of-range values fail server startup. The resolved and validated balance is stored on the matchmaking manager and copied into every eligible mode while preserving that mode's layout and unlimited-fuel contract.

### 9.2 Command authorization and execution

Extend `authorize_game_command` in `server/src/game_executor.rs` so `ActivateBoost` and `DeactivateBoost` have the same authenticated snake-ownership check as `Turn`. System commands remain forbidden over client connections.

Refactor `GameState::tick_forward` so due activation and deactivation commands execute in stable command-ID order before movement-credit calculation and due turns apply only for movers. Empty-meter, already-active, and already-inactive no-ops are determined at execution time because state may change after submission.

The server never accepts charge, speed, movement credit, active state, or packet state from a client.

### 9.3 Actor cadence

The v2 actor's existing 10 ms poll is frequent enough for a 50 ms quantum and does not need to change for Boost. Continue to catch up through `run_until` when delayed, preserve fair command/control handling, and record batch size and advance duration. The actor must retain enough headroom to finish work before the next 50 ms boundary under certified load.

### 9.4 Originating event ticks

`GameEngine::run_until` returns `(event_tick, event_sequence, event)`. The v2 actor currently discards the first two values and `publish_event` stamps the engine's final catch-up tick and sequence.

Fix this before Boost. A delayed 50 ms game may catch up several quanta at once; publishing a packet collection, collision, or respawn at the final batch tick makes replicas apply it too late. The fenced publication path must preserve the originating tick and engine event sequence while assigning the next transport `stream_seq`.

### 9.5 Replication, recovery, and completion

- Replicas advance the shared state to each event tick and apply the event in stream order.
- Checkpoints contain the complete current-version `GameState`; no separate Boost store is needed.
- Failover must restore nonzero movement credit and charge exactly or it can add/remove a cell or active quantum.
- Completed game JSON stores rules version, resolved configuration, fixed quantum, pads, and final snake Boost state for diagnosis and replay.
- The release does not recover pre-cutover active games. Pre-cutover completed payloads remain versioned/opaque to the new gameplay structs; metadata remains readable through the existing historical-data path or a dedicated legacy reader. They are preserved and never rewritten as Boost games.

### 9.6 Heartbeats and wall-clock timers

`TICK_HASH_INTERVAL_TICKS = 10` currently assumes 100 ms ticks. Replace it globally with a guarded elapsed-time deadline; a Boost match will naturally emit after approximately 20 of its 50 ms quanta. Do not use unguarded integer division because Custom may have an arbitrary configured tick duration. Restore the missing production v2 `TickHash` emission path.

Audit every tick-coupled behavior and preserve its wall-clock cadence, including:

- the team match clock, which counts up and never ends the match;
- approximately one-second checkpoints and hashes;
- food refill opportunities;
- liveness and reconnect watchdogs;
- crash-effect retention;
- bot decisions;
- trace/metric sampling; and
- replay playback speed.

Reducing the quantum must not make any of these run 2x faster in real time.

## 10. Client changes

### 10.1 Rust/WASM prediction and protocol

The WASM client receives speed, credit, charge, active-state transitions, pads, collision, and event application from `common`. TypeScript must not simulate fuel or schedule expiry.

Required changes:

- expose `processActivateBoost(snake_id)` and `processDeactivateBoost(snake_id)` wrappers beside `processTurn` in `client/src/lib.rs`;
- regenerate `client/web/types/generated` through `scripts/gen-types.sh` after Rust wire changes;
- parse the new command/event variants through the lossless raw-frame-to-Rust path;
- preserve FIFO frame draining and at-least-once command retry; and
- reconcile predicted activation and deactivation to their server-assigned command ticks.

The 500 ms committed lag, 1,000 ms prediction cap, and liveness windows remain durations. A Boost match derives their tick counts from its fixed 50 ms quantum.

### 10.2 Space, touch, and accessibility input

In `GameArena`:

- handle `KeyboardEvent.code === "Space"` on both keydown and keyup and call `preventDefault()` so the page does not scroll;
- ignore `event.repeat` and ignore new Space presses while an input, textarea, select, button, or contenteditable control owns focus;
- default to Hold mode, sending one `ActivateBoost` on the physical keydown and one `DeactivateBoost` on keyup;
- in Toggle mode, alternate the desired state with one explicit start or stop command per non-repeat keydown and emit nothing on keyup;
- track physical-key and desired-Boost intent independently from delayed predicted snapshots so rapid input cannot invert twice;
- preserve an already-valid physical Hold intent when charge depletes, emit one new activation edge when usable charge returns, and cancel that intent if the player releases while empty;
- in Hold mode, treat release, blur, hidden visibility, and reconnect/resync without a physical hold as stop edges; preserve Toggle's latched intent across blur, hidden visibility, and transient reconnects; clear either mode during explicit teardown or mode changes; and
- retain normal direction input independently in the same frame while giving focused interactive controls ownership of their keys.

Place a compact, keyboard-accessible Hold/Toggle radio group beside the faint controls legend. Persist it in client-local storage under a versioned key, validate stored values strictly, and default to Hold. Changing it affects only this client and sends no preference message to the server.

Provide an accessible on-screen Boost button for touch/mobile. In Hold mode, pointer-down/up/cancel and focused Space keydown/keyup own the corresponding start/stop edges; the window keyup path remains responsible for release even if depletion disables the focused button. Toggle mode uses ordinary button activation. A future gamepad binding may use the same commands and client-local mode without introducing a separate gameplay command.

### 10.3 HUD and rendering

The local HUD exposes charge percentage, active/inactive state, and interaction availability to assistive technology, but its visible content is deliberately limited to the NOS canister illustration on the left and the percentage on the right. It shows no `Boost` label and no multiplier.

The full-width clickable indicator is a shallow sub-panel that extends from beneath the arena rather than occupying arena canvas space. It is inset slightly from the arena sides so the main panel remains visually dominant. At 0% its paper-like surface is near-white with a restrained blue tint, a muted graphite-blue frame, and low-contrast reservoir marks. Its complete surface—including the icon bay—fills with electric yellow/gold from left to right as charge accumulates and recedes with exact predicted charge; at full capacity the entire indicator is yellow. A faint controls legend near the bottom of the screen shows `Move` with an arrow-key illustration, `Boost` with a blank Space-key illustration, and an explicit `Space behavior` choice between Hold-to-Boost and Press-to-Toggle. The indicator uses predicted Rust state for immediate feedback and retracts on reconciliation. It is not a TypeScript countdown. Its full surface remains an accessible touch target with a Space keyboard-shortcut annotation; pointer-down/up/cancel follows Hold mode, while click follows Toggle mode.

Canvas rendering must:

- draw available packets after the field/food layer and before snakes;
- draw an anti-aliased NOS pickup from normalized Canvas paths: one slightly oversized diagonal canister for 1x1 pads and one large upright canister for 2x2 pads, preserving the blue body, dark edge, white NOS band, orange mark, steel cap, and highlight at every supported cell size;
- skip every arena-grid intersection surrounding an available packet footprint (four dots for 1x1 and nine for 2x2), while cooling packets leave the ordinary grid intact;
- render a cooling packet as nothing rather than a gray indicator;
- show a bounded outline or short trail for an active snake;
- use `team_id` plus restrained within-team shades so the 2v2 roster can map each player to one snake without crossing blue/red team hues;
- work for spectators and arena rotations 0/90/180/270; and
- respect reduced-motion preferences.

The game shell retains Snaketron's existing airy white, restrained-italic, thin-graphite visual language. Free-floating roster content between the scoreboard and arena is centered on the same axis as the scoreboard score. Team halves face each other across a vertical `VS` rule. Every player is represented by a game-scale horizontal staredown snake using that player's exact arena skin, with the player name drawn inside its body near the inward-facing head; roster scores and spectator count are intentionally omitted. These snakes use a reusable, device-pixel-ratio-aware Canvas renderer—not DOM rectangles—with mirrored arena-style stroke geometry, a bounded text-fitting mechanism, deterministic ellipsis, WCAG-AA label contrast, and redraws for resize, resolution, and loaded fonts. Because canvas glyphs are not semantic content, each player wrapper is a named image and exposes the full untruncated name as a native hover affordance. The roster exposes no navigation actions while a match is live. After completion, it may expose Menu and Score Card in symmetric outer air without moving the matchup; the Snaketron logo also becomes a keyboard-accessible home action. Completion opens an accessible results modal with result, final score, stats, XP, Main Menu, Play Again, and Space-to-play-again; the scoreboard never grows a second result row.

The results modal uses neutral white/graphite standings rows headed `Player` and `Points`, without a blue current-player fill or italic column headers. Its title stays on white paper while the centered final score occupies a right-slanted, reusable vector-glass wing. The glass treatment accepts a generic color token plus explicit dark/light contrast mode; its gradients and static facets do not encode a particular result hue. Forced-colors mode removes the facets and restores a plain system-color divider. Its primary metrics are amber XP gained, green Score, graphite Time taken, PPM, and APM, with upright stat labels. Time taken is authoritative elapsed simulation time (`tick * tick_duration_ms`), not final snake length. PPM is the player's final score divided by elapsed minutes. APM is the authoritative count of accepted legal turns plus successful Boost active-state transitions divided by elapsed minutes; retries, rejected commands, reversal/same-direction no-ops, dead-snake commands, empty-Boost attempts, already-active starts, and already-inactive stops do not count. PPM and APM labels expose these definitions in hover- and keyboard-accessible tooltips.

Scoring effects use a small client-side plugin registry. The default effect is a deterministic droplet/ripple made from short-lived, low-alpha arena-cell background coloring at the scoring goal. It follows every arena rotation, uses committed scores only, resets on resync/epoch changes, bounds active cells and effect count, and becomes a brief stationary wash under reduced motion. Elementary `hypot`, exponential falloff, and smoothstep math are sufficient; no effects library is required for this bounded grid animation.

The canvas already redraws near 60 Hz while a 2.0x snake changes cells at 20 Hz, so v1 does not require sub-cell interpolation. Cosmetic effects remain bounded and must never replace collision steps.

### 10.4 Prediction and React performance

The current `rebuild_predicted_state` clones committed state and replays the full ahead window whenever the target tick advances. Moving from 100 to 50 ms can produce roughly 4x tick-replay work because the target advances twice as often and the same wall-clock lag contains twice as many ticks. Profile this path on the agreed low-end target. If it misses the release gates, incrementally advance unchanged prediction and reserve full rebuilds for authoritative correction, reconciliation, snapshot, or resync.

The current React hook serializes predicted and committed state on animation frames and publishes parsed state whenever bytes change. At a 50 ms quantum, state can change at 20 Hz. Before release:

- keep canvas rendering directly from WASM;
- use a compact Boost HUD getter or bounded state publication if profiling shows full game JSON is material;
- key canvas sizing effects to arena dimensions/rotation rather than all game state; and
- keep trails allocation-bounded.

### 10.5 Bots and load clients

Realistic bots should decide on their snake's movement opportunity or at a bounded 100 ms cadence, not every 50 ms quantum. Collision lookahead consumes generic speed/credit/mover state, not Boost branches. Seeking pads or choosing when to start/stop can be simple v1 strategy and is not an engine prerequisite.

Keep an `every-quantum` load-test profile as an intentional worst case, including four continuously replenished 2.0x snakes and command saturation.

## 11. Release and migration

### 11.1 Release model

Production uses one coordinated maintenance cutover to gameplay protocol v6. There is no disabled foundation deployment, capability cohort, regional canary, Quickmatch-first phase, Competitive delay, or mixed old/new engine period. CI, staging, soak, and load testing still occur before the maintenance window; they are release gates, not production rollout stages.

### 11.2 Maintenance cutover

1. Close matchmaking, lobby admission, joins, and spectator admission.
2. Disconnect clients and terminate active games. Maintenance-terminated games do not update ranked results, XP, or leaderboards.
3. Stop gateways, executors, replicas, bots, and load clients.
4. Remove only ephemeral active-game streams, ownership leases, pending commands, recovery checkpoints, replica state, matchmaking entries, lobby/party reservations, join tokens, assignments, and active-game directory indexes. Preserve accounts, ratings, configuration history, and completed-game data.
5. Deploy and restart gateways, executors, replicas, the common engine, Rust/WASM bundle, generated TypeScript types, web client, bots, and load clients as one tested version.
6. Require one exact gameplay protocol version during authentication. Reject a stale tab with a global “client update required”/reload response. This is a single equality check, not per-feature capability negotiation or lobby state.
7. While queues remain closed, smoke-test complete duel, 2v2, FFA, and Solo games; Hold-mode keydown start/key-up stop; Toggle-mode start/stop; partial and unlimited charge; packet collection at 2.0x; reconnect; TickHash; and snapshot resync.
8. Reopen only after every gateway, executor, and replica reports the exact version and healthy readiness; stale queues/indexes are empty; protocol mismatch rejection works; and all smoke tests pass. Then reopen Quickmatch and Competitive simultaneously with the same rules version. Any failure keeps admission closed and triggers rollback.

No pre-cutover active snapshot or old WASM client is admitted after reopening.

### 11.3 Rollback

Rollback is another maintenance cutover: close both queues, terminate games created by the new version without rating effects, disconnect every socket, stop all gameplay tasks, and deploy a prepared previous-stack artifact with its own exact protocol version check. Clear only incompatible ephemeral active state, invalidate HTML/service-worker/CDN entry caches so clients load the matching bundle, smoke-test, and reopen. There is no in-place rollback of an active Boost game.

## 12. Edge cases and required behavior

| Case | Required behavior |
| --- | --- |
| Configured speed is 1.0x | Meter, activation, and visuals work; active speed remains `1000`. Useful for soak tests. |
| Empty snake crosses a packet | Charge increases; speed remains `1000` until a later valid Space command. |
| Space and empty-meter collection occur in one quantum | Activation runs first and no-ops; collection fills later; press again. |
| Inactive snake is exactly 100% full | It cannot consume another packet; the packet remains. |
| Snake has partial capacity | Packet is consumed, charge is capped, and overflow is discarded. |
| Active snake collects a packet | Charge increases up to capacity; active speed does not stack. |
| Final funded quantum lands on a packet | Charge was reserved first; collection refills it and activation continues seamlessly. |
| Continuous Hold drains completely, then charge returns | Preserve the physical Hold intent while inactive and send exactly one fresh activation when charge becomes usable; release while empty cancels it. |
| New Hold begins at 0% | Do not latch a future activation; a later pickup requires another press. |
| Hold-mode Space is released while active | Schedule `DeactivateBoost`; when it executes before charge reservation, restore normal speed and preserve remaining charge and movement credit. |
| Toggle-mode Space is pressed while active | Schedule `DeactivateBoost`; keyup emits no command and unused charge is preserved. |
| Space repeats, start arrives while active, or stop arrives while inactive | Emit no repeated browser command where applicable; any duplicate/retried command is an idempotent no-op and speed never stacks. |
| Start and stop are due in one quantum | Execute in stable command-ID order before charge reservation and movement credit; the final state follows that deterministic order. |
| Window blurs or document visibility changes to hidden | Treat it as a Hold release and stop; clear Toggle's physical key edge but preserve its latched auto-Boost. |
| Game ends/unmounts or input mode changes | Clear local state and issue a best-effort explicit stop if the match can still accept commands. |
| Reconnect/resync finds active Boost | Hold sends `DeactivateBoost` when Space is no longer down; Toggle preserves the authoritative latched auto-Boost. |
| Death or scoring while charged/active | Clear charge and active state, restore speed, reset credit, and discard every queued player command targeting that snake, including turns, activation, and deactivation. |
| Disconnect/reconnect while charged | The server preserves exact charge, active state, speed, and credit. After snapshot restoration, Hold repairs a missed release while Toggle preserves its auto-Boost. |
| Cooldown ends under a head | Packet becomes available on schedule; normal post-collision collection applies. |
| Food targets a pad | Invalid; all pad cells are permanently excluded. |
| Two movers enter one packet cell | Collision resolves first; if both die, neither collects. |
| Mover enters a stationary snake | Generic collision kills the mover; stationary snake is not a collision candidate merely for holding position. |
| Unequal-speed head swap | Resolve from the full one-cell before/after state under one documented generic rule; test explicitly. |
| Background tab resumes | Catch up in the match quantum, retain originating event ticks, and cap prediction. |
| Executor fails mid-start or mid-stop | Recovery restores the exact active state, remaining charge, movement credit, and next movement quantum. |
| Client misses a packet event | Stream gap/hash mismatch requests a snapshot and reanchors prediction. |
| Team score is tied at timeout | Resolve as an explicit draw; remove nondeterministic `HashMap::max_by_key` tie behavior before Competitive reopens. |

## 13. Observability

Add bounded-cardinality metrics without game-ID or user-ID labels:

- packets collected, packet overflow discarded, full-meter collection attempts, and pad respawns;
- activation/deactivation attempts, successful active-state transitions, no-op reason, depletion, active milliseconds, and charge milliseconds spent;
- collection and Boost state transitions by game type, queue mode, team side, pad ID, and configured speed band;
- deaths and scores while active;
- quanta per `run_until`, catch-up batch size, actor lag, and actor advance duration;
- client rebuild duration, engine time, serialization time, React publication rate, and long frames;
- event bytes, snapshot bytes, and events per game-minute;
- stream gaps, hash mismatches, resyncs, checkpoint backlog, and watchdog activations; and
- pickup rate, pad utilization, score distribution, match result, and first-activation win correlation.

Compare release results with historical production and pre-release no-Boost load baselines. There are no production Boost-on/off cohorts. Balance correlations do not prove causation.

## 14. Performance assessment and gates

### 14.1 Expected cost

| Area | Expected change | Risk and response |
| --- | --- | --- |
| Server/common engine | 20 vs. 10 quanta/sec; roughly 2x step count | Low to moderate; profile four 2.0x snakes |
| Actor loop | Existing 10 ms poll remains | Low; require p99 below half the 50 ms quantum |
| Replica/recovery replay | Roughly 2x tick count | Moderate; bound catch-up and preserve event ticks |
| Current browser prediction | Roughly 4x naive tick-replay work | Moderate; profile low-end devices and optimize only if gates fail |
| Canvas | Same display refresh plus bounded pad/meter/trail primitives | Low; maximum position change is 20 Hz |
| React publication | Predicted state may change up to 20 times/sec | Moderate; compact/throttle only if profiling requires it |
| Network | Explicit start/stop commands plus low-frequency packet events | Low; never stream movement or charge drain |
| Checkpoints/Valkey | Same wall-clock cadence; slightly larger state | Low; monitor backlog and bytes |

Performance will suffer relative to today's engine, but the fixed 50 ms scope bounds the increase. Server and replica simulation roughly double; current browser prediction is the largest uncertainty at roughly 4x naive replay work. With at most four compressed snakes, no new service, and no per-cell events, the design is feasible if the release gates pass.

### 14.2 Release gates

- Run duel, 2v2, FFA, and Solo tests with every snake continuously active at 2.0x, saturated turns and explicit Boost start/stop commands, and ordinary food/scoring work.
- At certified concurrent-team-game load, no game remains more than one 50 ms quantum behind its intended 500 ms-lagged target for over one second.
- Actor advance p99 is below 25 ms, with no sustained checkpoint or command backlog.
- On the agreed low-end mobile target, engine plus required state publication stays below 8 ms p95 and gameplay sustains at least 50 rendered frames/sec p95 with four active 2.0x snakes.
- Background-tab catch-up and reconnect do not create a main-thread task over 50 ms p95 or an unrecovered divergence.
- Under the ordinary-player behavior profile, incremental Boost WebSocket/Valkey bytes per game-minute increase by no more than 10% against the same build with Boost traffic disabled, the restored TickHash enabled, and matched play duration/command ingress. Command-saturation tests use matched ingress volume as a capacity test and do not use the 10% product-traffic budget.
- TickHash mismatch, stream-gap, resync, watchdog, and actor-lag rates show no material regression.
- Approximately one-second hashes and checkpoints retain their wall-clock cadence in 50 ms Boost games and across existing per-mode/Custom timing regression tests.

If a gate fails, optimize prediction, state publication, actor scheduling, or collision work. Do not skip collision boundaries, reduce sync validation, or stream less authoritative state to disguise load.

## 15. Test plan

### 15.1 Shared-engine unit and property tests

- Prove every snake starts at speed `1000`, and only internal Boost lifecycle transitions can modify it.
- Validate range boundaries at `999`, `1000`, representative fractional milli-speeds, `2000`, and `2001`.
- Prove every Boost-enabled matchmade mode uses 50 ms, Boostless/Custom modes retain their configured timing, and non-divisible charge/cooldown configuration is rejected.
- Assert exact long-run cell counts and residual credit at 1.0x, 1.25x, 1.5x, 1.75x, and 2.0x.
- Assert no snake moves more than one cell in a quantum at any valid speed.
- Prove packet collection changes charge but never active state or speed.
- Prove full meters leave packets available and partial meters consume/cap deterministically.
- Prove multiple distinct same-snake activation commands execute the first valid transition and then no-op while active, prove repeated deactivation is idempotent, and prove empty activation cannot use a later same-quantum collection.
- Prove manual deactivation restores normal speed before charge reservation and movement credit, preserves unused charge and residual movement credit, and permits a later activation of that charge.
- Prove same-quantum activation/deactivation ordering follows stable command IDs and produces the same result in native, replay, replica, and WASM execution.
- Prove exact whole-quantum drain, last-funded-quantum movement, pickup-on-final-quantum continuation, and restoration to normal speed.
- Prove death, enemy-base death, and scoring reset charge, active state, speed, credit, and all queued player commands for that snake, including turns, activation, and deactivation.
- Test movers versus stationary head/body/tail, simultaneous head-on, same-cell entry, head swap, wall, goal, food, packet, and scoring interactions at unequal speeds.
- Test rapid turns during nonmovement quanta and preserve one legal turn per movement step.
- Test state validation and stable sync hashes for every new field.

### 15.2 Layout and mode tests

- Generate identical symmetric pad IDs, top-left positions, charges, and footprints for every 60x40 duel/2v2 game under layout v3 and every 40x40 FFA under layout v4.
- Prove all 24 footprint cells are unique, in the main field, and never overlap walls, goals, starts, respawns, or food, including while cooling.
- Prove FFA uses the symmetric collectible field layout, Solo uses a full unlimited tank without pads, and Custom/unsupported/off-canonical collectible modes remain Boostless at their configured timing.
- Sample deterministic food placement at scale: Duel/2v2 must be center-biased only on the goal-to-goal axis and uniform cross-field; Solo/FFA must be uniform on both axes. Exercise both initial spawn and refill through the same sampler while preserving end-zone, snake, food, and pad exclusions.
- Run the same Boost rules in Quickmatch and Competitive.

### 15.3 Sync, recovery, and replay tests

- Extend `server/tests/sync_equivalence_test.rs` with charge collection, activation, manual deactivation, restart with preserved charge, drain, depletion, refill, cooldown, packet contention, loss, jitter, duplicate delivery, and snapshot healing.
- Prove retries with the same stable command identity are deduplicated before either start or stop executes a second time, and that distinct duplicates are harmless because both commands are idempotent.
- Compare native and WASM fingerprints with nonzero credit, stored charge, and active Boost at the fixed 50 ms quantum.
- Fail over an executor mid-activation, mid-deactivation, and mid-cooldown; require the same next movement, charge, and final fingerprint as an uninterrupted control.
- Test a multi-quantum catch-up batch and assert each packet, collision, and scoring event retains its originating tick and event sequence.
- Record and replay traces from their stored quantum and rules configuration with no hardcoded 100 or 50 ms TeamMatch assumption.
- Verify completed state retains configuration without awarding Boost-specific XP/MMR.

### 15.4 Client tests

- Parse raw activation/deactivation commands and packet events without losing `u64` command/stream identifiers.
- Verify Hold is the default, uses Space `code`, prevents scrolling, emits exactly one activation on non-repeat keydown and one deactivation on keyup, ignores browser key-repeat and interactive-control focus, and coexists with direction input.
- Verify the compact Hold/Toggle control is keyboard-accessible, persists only the two valid values under its versioned client-local key, falls back to Hold, and sends no mode preference over the wire.
- Verify Toggle alternates explicit start/stop commands on non-repeat keydown and emits nothing on keyup, including rapid presses before predicted snapshots update.
- Verify Hold release, blur, hidden visibility, and reconnect/resync without a held key issue the required deactivation without double-sending; verify Toggle preserves its latch through blur/hidden/reconnect and stops on the next press.
- Verify one uninterrupted keyboard and pointer Hold survives depletion, remains inactive at 0%, emits exactly one new logical activation after recharge despite transport retries, and is cancelled by a release before recharge.
- Verify the touch button applies Hold pointer-down/up/cancel behavior or Toggle click behavior and has an accessible label/state.
- Reconcile locally predicted activation and deactivation to later server-assigned ticks and retract rejected/no-op predictions.
- Reconnect/resync while charged restores exact charge, speed, credit, and pad cooldown; Hold without a physical key sends a stop and preserves remaining charge, while Toggle preserves authoritative auto-Boost.
- Visual-regression test the inset, arena-docked near-white empty indicator and full-surface partial/full/active yellow fill; anti-aliased 2x2 large and 1x1 regular NOS canisters plus their grid-dot masks at every supported cell-size tier; absent cooling pads; DPR-backed canvas teammate/opponent staredown rosters with inward heads and fitted names; results modal; score ripple; reduced motion; desktop/mobile breakpoints; and all arena rotations.
- Verify a mismatched global protocol version receives the reload-required response and cannot join any game.

### 15.5 End-to-end, cutover, and performance tests

- Play complete duel, 2v2, FFA, and Solo games at speed-range boundaries and representative fractional speeds under injected latency in both Hold and Toggle modes.
- Run four snakes at 2.0x with continuous fuel, `every-quantum` commands, executor delay, and stream loss/recovery.
- Exercise background/suspended-tab catch-up, graceful handoff, abrupt executor failure, gateway reconnect, and resync.
- Compare CPU, actor p95/p99, scheduler lateness, catch-up size, Valkey/WS traffic, snapshot size, browser engine time, serialization, React commits, frame time, and long tasks against baseline.
- Rehearse the full maintenance cutover and rollback in a production-like environment, including termination without rating updates, scoped ephemeral cleanup, exact protocol rejection, smoke tests, and simultaneous queue reopening.

## 16. Acceptance criteria

Boost v1 is complete when:

1. Every canonical duel, 2v2, and FFA game receives its versioned symmetric twelve-pad layout—four outer 2x2 full packets and eight inner 1x1 quarter packets—while Solo receives unlimited Boost without pads and unsupported modes remain Boostless.
2. Packets add snake-owned charge up to 100% and never activate or directly alter speed.
3. Hold Space is the default: keydown sends one authorized, predicted `ActivateBoost`, keyup sends `DeactivateBoost`, stopping preserves unused charge and movement credit, and an uninterrupted physical hold automatically issues one fresh activation when charge returns after depletion. A compact persisted client-local control selects Hold or Toggle; Toggle alternates the same explicit commands, while the server remains mode-agnostic.
4. Every snake has authoritative `speed_milli`; only internal Boost lifecycle transitions modify it after construction.
5. Configured speed accepts `1000..=2000`, every Boost match uses the fixed 50 ms quantum, and every valid speed produces exact deterministic average distance.
6. No snake moves more than one cell per quantum, so intermediate collisions cannot be skipped at 2.0x.
7. Collision, food, scoring, and turn logic contains no Boost/no-Boost branch; collision operates on generic movers and ordinary snake state.
8. Charge consumption, manual stop/restart, refill, depletion, death/reset, disconnect, reconnect, cleanup release, and final-funded-quantum behavior match this PRD.
9. Team matches have no time limit or maximum duration; they complete on the tick a team reaches its queue's score target (25 Quickmatch, 50 Competitive).
10. Server, replica, WASM, replay, snapshot, resync, and failover produce equivalent fingerprints.
11. Catch-up events preserve originating tick and engine sequence, and TickHash arrives approximately once per wall-clock second.
12. All four-snake 2.0x server/client performance gates pass; prediction/state-publication optimization is implemented if profiling requires it.
13. No per-cell movement or per-quantum fuel events are sent.
14. The rehearsed maintenance cutover deploys exact gameplay protocol v6 and reopens Quickmatch and Competitive together, with no active-game migration or staged production cohort.
15. Team food remains center-biased only along the goal-to-goal axis and uniform cross-field; Solo and FFA food is uniform on both axes for both initial spawn and refill.
16. Roster snakes are rendered by a responsive Canvas path with player names fitted inside the body near the head, and the arena-docked Boost panel presents icon plus percent only on a low-contrast empty surface with full-width yellow charge fill.

## 17. Feasibility and recommendation

Boost is technically feasible in the current architecture. The shared Rust `GameState`/`GameEngine` used by authoritative servers, replicas, replays, and WASM is the right place for snake-owned speed, stored charge, deterministic start/stop transitions, and fixed pad state. Recovery already snapshots full state, and movement is already reconstructed rather than streamed.

The work is a medium-to-large gameplay-engine change. The primary risks are:

1. client prediction cost when the team quantum changes from 100 to 50 ms;
2. unequal-speed collision and turn ordering;
3. preserving originating event ticks through batched catch-up;
4. reconciling predicted Space start/stop commands and release cleanup; and
5. auditing every formerly tick-count-based timer for wall-clock behavior.

None requires a new service or networking architecture. Proceed with the fixed 50 ms, snake-owned-speed design. Treat event-tick correctness, wall-clock TickHash, command/charge reconciliation, and four-snake 2.0x performance certification as launch prerequisites; add incremental prediction only if profiling shows it is needed.
