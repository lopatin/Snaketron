use crate::util::PseudoRandom;
use crate::{
    BOOST_RULES_VERSION, BOOST_SPOT_LAYOUT_VERSION_FIELD, BOOST_SPOT_LAYOUT_VERSION_NONE,
    BOOST_SPOT_LAYOUT_VERSION_TEAM, BOOST_TICK_INTERVAL_MS, BoostResolution,
    DEFAULT_BOOST_CAPACITY_MS, DEFAULT_BOOST_PACKET_CHARGE_MS, DEFAULT_BOOST_PAD_RESPAWN_MS,
    DEFAULT_BOOST_SPEED_MILLI, DEFAULT_COMPETITIVE_TEAM_SCORE_LIMIT, DEFAULT_CUSTOM_GAME_TICK_MS,
    DEFAULT_FOOD_TARGET, DEFAULT_PLAYER_IDLE_TIMEOUT_MS, DEFAULT_PLAYER_IDLE_WARNING_MS,
    DEFAULT_QUICKMATCH_TEAM_SCORE_LIMIT, DEFAULT_TICK_INTERVAL_MS, Direction,
    MAX_BOOST_SPEED_MILLI, NORMAL_SNAKE_SPEED_MILLI, Player, Position, Snake, SnakeBoost,
    SnakeCombo,
};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

mod sorted_hash_set {
    use serde::{Serialize, Serializer};
    use std::collections::HashSet;

    pub fn serialize<T, S>(
        values: &HashSet<T>,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        T: Ord + Serialize,
        S: Serializer,
    {
        let mut values: Vec<_> = values.iter().collect();
        values.sort_unstable();
        values.serialize(serializer)
    }
}

const DEFAULT_SNAKE_LENGTH: usize = 4;

/// Default time available to continue a combo after each food pickup.
pub const DEFAULT_COMBO_WINDOW_MS: u32 = 1_000;
/// Food values progress 1, 1, 2, 3 and remain capped at 3 for the chain.
pub const DEFAULT_COMBO_MAX_FOOD_VALUE: u32 = 3;
/// Semantic version of the authoritative combo scoring rules.
pub const COMBO_RULES_VERSION: u16 = 1;
/// Defensive upper bound for snapshotted tuning. The production default is
/// one second; the larger ceiling permits experiments without accepting an
/// effectively permanent meter from malformed state.
const MAX_COMBO_WINDOW_MS: u32 = 60_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FoodAxisDistribution {
    Uniform,
    CenterBiased,
}

/// Select how food is distributed on the arena's authoritative axes.
///
/// Team arenas store their end zones on the left and right, so `x` is the
/// goal-to-goal axis even though the client rotates that axis vertically for
/// the usual player view. `y` runs along the width of either end zone. Duel
/// and 2v2 therefore keep their existing center pressure only on `x`, while
/// every non-team mode distributes food uniformly on both axes.
fn food_axis_distributions(game_type: &GameType) -> (FoodAxisDistribution, FoodAxisDistribution) {
    if matches!(game_type, GameType::TeamMatch { .. }) {
        (
            FoodAxisDistribution::CenterBiased,
            FoodAxisDistribution::Uniform,
        )
    } else {
        (FoodAxisDistribution::Uniform, FoodAxisDistribution::Uniform)
    }
}

fn sample_food_axis(
    rng: &mut PseudoRandom,
    min: i16,
    max: i16,
    distribution: FoodAxisDistribution,
) -> i16 {
    debug_assert!(min <= max);
    let range = (i32::from(max) - i32::from(min) + 1) as f32;

    match distribution {
        FoodAxisDistribution::Uniform => {
            // next_f32 is in [0, 1), so every integer cell receives the same
            // half-open interval and `max` cannot be exceeded.
            min + (rng.next_f32() * range).floor() as i16
        }
        FoodAxisDistribution::CenterBiased => {
            let center = (f32::from(min) + f32::from(max)) / 2.0;
            // range / 6 preserves the former three-sigma arena fit.
            rng.next_normal(center, range / 6.0)
                .round()
                .clamp(f32::from(min), f32::from(max)) as i16
        }
    }
}

fn sample_food_position(rng: &mut PseudoRandom, game_type: &GameType, arena: &Arena) -> Position {
    // Team games exclude both end zones; other modes use the complete arena.
    let (x_min, x_max) = arena
        .main_field_bounds()
        .unwrap_or((0, arena.width as i16 - 1));
    let y_min = 0;
    let y_max = arena.height as i16 - 1;
    let (x_distribution, y_distribution) = food_axis_distributions(game_type);

    Position {
        x: sample_food_axis(rng, x_min, x_max, x_distribution),
        y: sample_food_axis(rng, y_min, y_max, y_distribution),
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum GameCommand {
    // User command for movement
    Turn {
        snake_id: u32,
        direction: Direction,
    },

    // Player command for consuming snake-owned stored Boost charge.
    ActivateBoost {
        snake_id: u32,
    },

    // System command for failover
    UpdateStatus {
        status: GameStatus,
    },

    // Player command for releasing active Boost while retaining stored fuel.
    DeactivateBoost {
        snake_id: u32,
    },

    /// Explicit presence acknowledgement used by the inactivity warning UI.
    /// It intentionally has no gameplay effect beyond resetting the player's
    /// authoritative idle deadline.
    PlayerActivity {
        snake_id: u32,
    },
}

/// Stable identity for the at-least-once command protocol. Unlike the engine's
/// tick-scoped `CommandId`, this survives reconstruction and WebSocket reconnect.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct ClientCommandIdentityV2 {
    pub game_id: u32,
    pub user_id: u32,
    pub client_game_session_id: String,
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub sequence: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct GameEventMessage {
    pub game_id: u32,
    pub tick: u32,
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub sequence: u64,
    /// Transport-level sequence, assigned by the publishing game executor and
    /// strictly monotonic per game across ALL published messages (events,
    /// snapshots, tick hashes). Receivers detect lost messages by checking
    /// contiguity. 0 is reserved for locally constructed or explicitly
    /// out-of-band messages that do not advance replicated state.
    #[serde(default)]
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub stream_seq: u64,
    pub user_id: Option<u32>,
    pub event: GameEvent,
}

// Snapshot intentionally carries a full inline GameState: events are message
// envelopes that are serialized or sent through channels, not stored in bulk,
// so boxing the snapshot would add indirection without a meaningful win.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum GameEvent {
    SnakeTurned {
        snake_id: u32,
        direction: Direction,
    },
    SnakeDied {
        snake_id: u32,
        /// The collision or scoring transition that ended this life. Older
        /// durable snapshots/events predate attribution and deserialize as
        /// `Unknown`; current authoritative simulation always emits a
        /// concrete cause.
        #[serde(default)]
        cause: DeathCause,
    },
    /// Authoritative inactivity removal. This is one atomic replicated state
    /// transition so clients learn both why the player left and that their
    /// snake is no longer active without waiting for a repair snapshot.
    PlayerIdleKicked {
        user_id: u32,
        snake_id: u32,
    },
    FoodSpawned {
        position: Position,
    },
    FoodEaten {
        snake_id: u32,
        position: Position,
        /// Exact score and physical growth awarded by this pickup.
        #[serde(default = "default_food_eaten_points")]
        points: u32,
        /// One-based depth of this pickup in the current uninterrupted chain.
        #[serde(default = "default_food_eaten_combo_chain")]
        combo_chain: u32,
        /// Meter value immediately before this pickup refilled it.
        #[serde(default)]
        combo_remaining_ms_before: u32,
        /// Whether authoritative Boost was active at the instant of pickup.
        #[serde(default)]
        boost_active: bool,
    },
    BoostPacketCollected {
        pad_id: u8,
        snake_id: u32,
        charge_ms_after: u32,
        respawn_at_tick: u32,
    },
    Snapshot {
        game_state: GameState,
    },
    CommandScheduled {
        command_message: GameCommandMessage,
    },
    /// Positive semantic result for the v2 at-least-once command protocol.
    /// This is still CommandScheduled—not a gateway receipt/CommandAccepted.
    CommandScheduledV2 {
        command_id: ClientCommandIdentityV2,
        command_message: GameCommandMessage,
        /// True when the executor is returning a previously recorded outcome;
        /// replicas must not schedule the same logical command again.
        deduplicated_replay: bool,
    },
    /// Terminal negative semantic result for a v2 player command.
    CommandRejected {
        command_id: ClientCommandIdentityV2,
        reason: String,
        /// Exact speculative engine command to retract, when the rejecting
        /// gateway/executor successfully decoded one. Out-of-band malformed
        /// delivery rejections may not have this mapping.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        command_id_client: Option<CommandId>,
        /// When present, unresolved identities in this client game session at
        /// or above this sequence have the same terminal rejection. Exact
        /// outcomes and the contiguous resolved watermark take precedence.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
        session_rejected_from: Option<u64>,
    },
    // PlayerJoined { user_id: u32, snake_id: u32 },
    StatusUpdated {
        status: GameStatus,
    },
    /// A player confirmed they have read the pre-match briefing and are ready.
    /// Idempotent: re-delivery of an already-recorded readiness is a no-op, so
    /// the at-least-once transport cannot corrupt the gate.
    PlayerReady {
        user_id: u32,
    },
    /// The pre-match readiness gate resolved — every player confirmed, or the
    /// deadline lapsed — and the simulation is now scheduled to begin at
    /// `simulation_epoch_ms`. `start_ms` is deliberately left untouched: it is
    /// the durable runtime game identity that join authorization compares
    /// against, so it must never move once a match exists.
    MatchStartScheduled {
        #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
        simulation_epoch_ms: i64,
    },
    ScoreUpdated {
        snake_id: u32,
        score: u32,
    },
    TeamScoreUpdated {
        team_id: TeamId,
        score: u32,
    },

    SnakeRespawned {
        snake_id: u32,
        position: Position,
        direction: Direction,
    },

    // XP event
    XPAwarded {
        player_xp: HashMap<u32, u32>,
    }, // user_id -> xp_gained

    /// Periodic server heartbeat carrying the authoritative state fingerprint
    /// at the message's tick (see `GameState::sync_hash`). Clients compare it
    /// against their own committed state to detect divergence, use its arrival
    /// as a liveness signal, and use `server_ts_ms` as a clock reference.
    /// Never mutates state.
    TickHash {
        #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
        hash: u64,
        #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
        server_ts_ms: i64,
    },
}

/// Deterministic attribution for a snake death. This records information the
/// collision pass already knows; it does not change collision semantics.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum DeathCause {
    /// Compatibility value for stored artifacts written before attribution
    /// was added. New simulation never emits this variant.
    #[default]
    Unknown,
    Wall,
    OutOfBounds,
    EnemyBase,
    SelfCollision,
    SnakeBody {
        killer_snake_id: u32,
    },
    HeadToHead {
        other_snake_id: u32,
    },
    /// Team-mode banking intentionally resets and respawns the scoring snake.
    Banked,
}

fn default_food_eaten_points() -> u32 {
    1
}

fn default_food_eaten_combo_chain() -> u32 {
    1
}

/// A collision produced by simulation. Recent cues stay in `GameState` for a
/// short, bounded window so a renderer cannot miss one when prediction catches
/// up by several ticks in a single frame. Including the tick also lets a
/// prediction replay retract or relocate an effect deterministically.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct SnakeCrash {
    pub tick: u32,
    pub snake_id: u32,
    pub position: Position,
    #[serde(default)]
    pub cause: DeathCause,
}

/// Keep cosmetic crash history slightly longer than the web animation. This
/// is deliberately time-based so custom games with faster ticks retain the
/// same rollback-visible window without allowing unbounded snapshot growth.
const RECENT_CRASH_RETENTION_MS: u32 = 1_000;

/// A team goal produced by simulation: a snake carrying at least one point's
/// worth of food entered its own base. Like [`SnakeCrash`], recent cues stay in
/// `GameState` for a short, bounded window so a renderer cannot miss one when
/// prediction catches up by several ticks in a single frame, and so a
/// prediction replay can retract a celebration that never actually happened.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct TeamGoal {
    pub tick: u32,
    pub team_id: TeamId,
    pub snake_id: u32,
    /// The scoring snake's head on the tick it entered its base — the exact
    /// cell a celebration is centered on, not the middle of the goal mouth.
    pub position: Position,
    /// Points this single snake added to its team's score.
    pub points: u32,
}

/// Cosmetic goal history outlives the web celebration for the same reason as
/// `RECENT_CRASH_RETENTION_MS`. The floating score readout runs longer than a
/// crash animation, so this window is correspondingly wider: a cue must stay
/// visible to prediction for the whole time its effect can be on screen, or a
/// natural expiry would look like a rollback retraction.
const RECENT_GOAL_RETENTION_MS: u32 = 1_800;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct TeamZoneConfig {
    pub end_zone_depth: u16, // Depth of each end zone (10 cells)
    pub goal_width: u16,     // Width of goal opening in cells
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct BoostConfig {
    pub speed_milli: u16,
    pub capacity_ms: u32,
    /// Charge carried by each inner 1x1 packet. Layout v3 requires this to be
    /// exactly 25% of capacity; outer 2x2 pads snapshot full capacity.
    pub packet_charge_ms: u32,
    pub pad_respawn_ms: u32,
    pub spot_layout_version: u16,
    pub rules_version: u16,
    /// A tank that never empties: no pickups are placed, the meter starts and
    /// stays full, and a funded quantum costs nothing. Solo runs use it so a
    /// lone player can hold Boost freely with nothing to contest.
    ///
    /// This is the only Boost fuel model that does not require a pad layout,
    /// so it pairs with `BOOST_SPOT_LAYOUT_VERSION_NONE`.
    #[serde(default)]
    pub unlimited: bool,
}

/// An internal, authoritative Boost state transition produced while advancing
/// one simulation quantum. This is deliberately not part of the replicated
/// event protocol: callers may observe it for lifecycle telemetry without
/// changing deterministic game state or wire compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoostLifecycleTransition {
    Activated { snake_id: u32 },
    Depleted { snake_id: u32 },
    ManuallyStopped { snake_id: u32 },
}

impl Default for BoostConfig {
    /// The collectible tank on the canonical team map.
    fn default() -> Self {
        Self {
            speed_milli: DEFAULT_BOOST_SPEED_MILLI,
            capacity_ms: DEFAULT_BOOST_CAPACITY_MS,
            packet_charge_ms: DEFAULT_BOOST_PACKET_CHARGE_MS,
            pad_respawn_ms: DEFAULT_BOOST_PAD_RESPAWN_MS,
            spot_layout_version: BOOST_SPOT_LAYOUT_VERSION_TEAM,
            rules_version: BOOST_RULES_VERSION,
            unlimited: false,
        }
    }
}

impl BoostConfig {
    /// The collectible tank on the teamless free-for-all map. Same balance as
    /// the team default; only the pad geometry differs.
    pub fn field() -> Self {
        Self {
            spot_layout_version: BOOST_SPOT_LAYOUT_VERSION_FIELD,
            ..Self::default()
        }
    }

    /// A tank that never empties and has nothing to collect.
    pub fn unlimited() -> Self {
        Self {
            spot_layout_version: BOOST_SPOT_LAYOUT_VERSION_NONE,
            unlimited: true,
            ..Self::default()
        }
    }
}

impl BoostConfig {
    pub fn validate(&self) -> Result<()> {
        if !(NORMAL_SNAKE_SPEED_MILLI..=MAX_BOOST_SPEED_MILLI).contains(&self.speed_milli) {
            return Err(anyhow::anyhow!(
                "Boost speed_milli must be in {}..={}, got {}",
                NORMAL_SNAKE_SPEED_MILLI,
                MAX_BOOST_SPEED_MILLI,
                self.speed_milli
            ));
        }
        if self.capacity_ms == 0 || !self.capacity_ms.is_multiple_of(BOOST_TICK_INTERVAL_MS * 4) {
            return Err(anyhow::anyhow!(
                "Boost capacity_ms must be positive and divisible by {} so 25% is a whole quantum",
                BOOST_TICK_INTERVAL_MS * 4
            ));
        }
        if self.packet_charge_ms != self.capacity_ms / 4 {
            return Err(anyhow::anyhow!(
                "Boost packet_charge_ms must equal exactly 25% of capacity ({}ms), got {}ms",
                self.capacity_ms / 4,
                self.packet_charge_ms
            ));
        }
        if self.pad_respawn_ms == 0 || !self.pad_respawn_ms.is_multiple_of(BOOST_TICK_INTERVAL_MS) {
            return Err(anyhow::anyhow!(
                "Boost pad_respawn_ms must be positive and divisible by {}",
                BOOST_TICK_INTERVAL_MS
            ));
        }
        let known_layout = matches!(
            self.spot_layout_version,
            BOOST_SPOT_LAYOUT_VERSION_TEAM
                | BOOST_SPOT_LAYOUT_VERSION_FIELD
                | BOOST_SPOT_LAYOUT_VERSION_NONE
        );
        if !known_layout {
            return Err(anyhow::anyhow!(
                "unsupported Boost spot layout version {}, expected one of {}, {} or {}",
                self.spot_layout_version,
                BOOST_SPOT_LAYOUT_VERSION_NONE,
                BOOST_SPOT_LAYOUT_VERSION_TEAM,
                BOOST_SPOT_LAYOUT_VERSION_FIELD
            ));
        }
        // The padless layout and the unlimited tank imply one another: pickups
        // with nothing to fill, or an unlimited tank with objectives that do
        // nothing, are both states the renderer and the player would misread.
        if self.unlimited != (self.spot_layout_version == BOOST_SPOT_LAYOUT_VERSION_NONE) {
            return Err(anyhow::anyhow!(
                "unlimited Boost requires spot layout {} and vice versa, got unlimited={} layout={}",
                BOOST_SPOT_LAYOUT_VERSION_NONE,
                self.unlimited,
                self.spot_layout_version
            ));
        }
        if self.rules_version != BOOST_RULES_VERSION {
            return Err(anyhow::anyhow!(
                "unsupported Boost rules version {}, expected {}",
                self.rules_version,
                BOOST_RULES_VERSION
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct BoostPad {
    pub id: u8,
    /// Top-left cell of this square footprint.
    pub position: Position,
    /// Amount stored by this packet when the meter has enough room.
    pub charge_ms: u32,
    /// Width and height of the square footprint in cells (v3: 1 or 2).
    pub size_cells: u8,
    pub respawn_at_tick: Option<u32>,
}

impl BoostPad {
    pub fn contains(&self, position: &Position) -> bool {
        let size = i32::from(self.size_cells);
        size > 0
            && i32::from(position.x) >= i32::from(self.position.x)
            && i32::from(position.x) < i32::from(self.position.x) + size
            && i32::from(position.y) >= i32::from(self.position.y)
            && i32::from(position.y) < i32::from(self.position.y) + size
    }

    /// Materialize the authoritative footprint in stable row-major order.
    pub fn footprint_cells(&self) -> Vec<Position> {
        let mut cells = Vec::with_capacity(usize::from(self.size_cells).pow(2));
        for y_offset in 0..self.size_cells {
            for x_offset in 0..self.size_cells {
                let x = i32::from(self.position.x) + i32::from(x_offset);
                let y = i32::from(self.position.y) + i32::from(y_offset);
                if let (Ok(x), Ok(y)) = (i16::try_from(x), i16::try_from(y)) {
                    cells.push(Position { x, y });
                }
            }
        }
        cells
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Copy, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct TeamId(pub u8);

impl TeamId {
    pub fn new(index: u8) -> Self {
        TeamId(index)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct Arena {
    pub width: u16,
    pub height: u16,
    pub snakes: Vec<Snake>,
    pub food: Vec<Position>,
    #[serde(default)]
    pub boost_pads: Vec<BoostPad>,
    pub team_zone_config: Option<TeamZoneConfig>, // New field - minimal state
}

impl Arena {
    pub fn add_snake(&mut self, snake: Snake) -> Result<u32> {
        if self.snakes.len() >= u32::MAX as usize {
            return Err(anyhow::anyhow!("Arena is full, cannot add more snakes"));
        }
        let id = self.snakes.len() as u32;
        self.snakes.push(snake);
        Ok(id)
    }

    pub fn is_boost_pad_position(&self, position: &Position) -> bool {
        self.boost_pads.iter().any(|pad| pad.contains(position))
    }

    /// The playable span a Boost layout is drawn inside, by layout version.
    ///
    /// Team layouts inset the field by the end zones so no pad can sit in a
    /// goal; the free-for-all layout has no zones and uses the whole arena.
    /// `None` means this arena cannot host the requested layout, and callers
    /// treat that as "no pads" — validation then rejects it loudly.
    fn boost_field_bounds(&self, spot_layout_version: u16) -> Option<(i16, i16)> {
        match spot_layout_version {
            BOOST_SPOT_LAYOUT_VERSION_TEAM => {
                let (left, right) = self.main_field_bounds()?;
                // Layout v3 is a canonical-map contract. Eligible games on any
                // other geometry fail closed during creation/validation rather
                // than moving objectives or producing asymmetric footprints.
                (self.width == 60 && self.height == 40 && (left, right) == (10, 49))
                    .then_some((left, right))
            }
            BOOST_SPOT_LAYOUT_VERSION_FIELD => {
                // The teamless layout is the same geometry drawn on the whole
                // arena. It is deliberately restricted to the square canonical
                // free-for-all map: the ring's quarter-turn symmetry only
                // closes when the field is square, and an asymmetric map would
                // hand one spawn a shorter path to a full tank.
                (self.team_zone_config.is_none() && self.width == 40 && self.height == 40)
                    .then_some((0, self.width as i16 - 1))
            }
            _ => None,
        }
    }

    /// Pad geometry for a config, dispatched on its layout version.
    ///
    /// The arithmetic below is written against `(left, right)` and `height`
    /// alone, so the canonical 40-cell team field and the 40x40 free-for-all
    /// arena produce the same shape from the same code — the inset, octagon
    /// radius and bevel are all field-relative fractions.
    fn boost_pad_layout(&self, config: &BoostConfig) -> Vec<BoostPad> {
        if config.unlimited {
            // An unlimited tank is never refuelled, so placing pickups would
            // put objectives on the map that do nothing.
            return Vec::new();
        }
        let Some((left, right)) = self.boost_field_bounds(config.spot_layout_version) else {
            return Vec::new();
        };

        let field_width = right - left + 1;
        let arena_bottom = self.height as i16 - 1;
        let outer_size = 2_i16;
        let outer_x_inset = field_width / 10;
        let outer_y_inset = self.height as i16 / 10;
        let outer_left = left + outer_x_inset;
        let outer_right = right - outer_x_inset - (outer_size - 1);
        let outer_top = outer_y_inset;
        let outer_bottom = arena_bottom - outer_y_inset - (outer_size - 1);

        // The standard packets form a nearly regular octagon centered on the
        // half-cell intersection at (29.5, 19.5). Paired integer centers keep
        // every ID exactly mirrored on this even-sized canonical map.
        let center_left = (left + right) / 2;
        let center_right = center_left + 1;
        let center_top = arena_bottom / 2;
        let center_bottom = center_top + 1;
        // On the 40-cell field, the axial and bevel offsets are 7 and 3.
        // Enumerating the resulting vertices clockwise makes a 90-degree
        // view rotation a stable +2 pad-ID rotation inside the ring.
        let octagon_radius = field_width * 7 / 40;
        let octagon_bevel = field_width * 3 / 40;

        let inner_left = center_left - octagon_radius;
        let inner_right = center_right + octagon_radius;
        let inner_top = center_top - octagon_radius;
        let inner_bottom = center_bottom + octagon_radius;
        let inner_left_bevel = center_left - octagon_bevel;
        let inner_right_bevel = center_right + octagon_bevel;
        let inner_top_bevel = center_top - octagon_bevel;
        let inner_bottom_bevel = center_bottom + octagon_bevel;

        let pads = [
            (
                Position {
                    x: outer_left,
                    y: outer_top,
                },
                config.capacity_ms,
                2,
            ),
            (
                Position {
                    x: outer_left,
                    y: outer_bottom,
                },
                config.capacity_ms,
                2,
            ),
            (
                Position {
                    x: outer_right,
                    y: outer_top,
                },
                config.capacity_ms,
                2,
            ),
            (
                Position {
                    x: outer_right,
                    y: outer_bottom,
                },
                config.capacity_ms,
                2,
            ),
            (
                Position {
                    x: inner_left_bevel,
                    y: inner_top,
                },
                config.packet_charge_ms,
                1,
            ),
            (
                Position {
                    x: inner_right_bevel,
                    y: inner_top,
                },
                config.packet_charge_ms,
                1,
            ),
            (
                Position {
                    x: inner_right,
                    y: inner_top_bevel,
                },
                config.packet_charge_ms,
                1,
            ),
            (
                Position {
                    x: inner_right,
                    y: inner_bottom_bevel,
                },
                config.packet_charge_ms,
                1,
            ),
            (
                Position {
                    x: inner_right_bevel,
                    y: inner_bottom,
                },
                config.packet_charge_ms,
                1,
            ),
            (
                Position {
                    x: inner_left_bevel,
                    y: inner_bottom,
                },
                config.packet_charge_ms,
                1,
            ),
            (
                Position {
                    x: inner_left,
                    y: inner_bottom_bevel,
                },
                config.packet_charge_ms,
                1,
            ),
            (
                Position {
                    x: inner_left,
                    y: inner_top_bevel,
                },
                config.packet_charge_ms,
                1,
            ),
        ];

        pads.into_iter()
            .enumerate()
            .map(|(id, (position, charge_ms, size_cells))| BoostPad {
                id: id as u8,
                position,
                charge_ms,
                size_cells,
                respawn_at_tick: None,
            })
            .collect()
    }

    /// Calculate team end zone bounds
    pub fn team_zone_bounds(&self, team_id: TeamId) -> Option<(i16, i16, i16, i16)> {
        self.team_zone_config.as_ref().map(|config| {
            match team_id.0 {
                0 => {
                    // Team 0 zone (left side)
                    (
                        0,
                        config.end_zone_depth as i16 - 1,
                        0,
                        self.height as i16 - 1,
                    )
                }
                1 => {
                    // Team 1 zone (right side)
                    let x_start = self.width as i16 - config.end_zone_depth as i16;
                    (x_start, self.width as i16 - 1, 0, self.height as i16 - 1)
                }
                _ => {
                    // For additional teams, could extend to top/bottom or other zones
                    (
                        0,
                        config.end_zone_depth as i16 - 1,
                        0,
                        self.height as i16 - 1,
                    )
                }
            }
        })
    }

    /// Calculate main field bounds
    pub fn main_field_bounds(&self) -> Option<(i16, i16)> {
        self.team_zone_config.as_ref().map(|config| {
            (
                config.end_zone_depth as i16,
                self.width as i16 - config.end_zone_depth as i16 - 1,
            )
        })
    }

    /// Calculate goal position for a given team
    pub fn goal_bounds(&self, team: TeamId) -> Option<(i16, i16, i16)> {
        self.team_zone_config.as_ref().map(|config| {
            let goal_center = self.height as i16 / 2;
            let half_width = config.goal_width as i16 / 2;
            let y_start = goal_center - half_width;
            let y_end = goal_center + half_width;

            let x_pos = match team.0 {
                0 => config.end_zone_depth as i16 - 1, // Right edge of Team 0 zone
                1 => self.width as i16 - config.end_zone_depth as i16, // Left edge of Team 1 zone
                _ => config.end_zone_depth as i16 - 1, // Default to team 0 position for other teams
            };

            (x_pos, y_start, y_end)
        })
    }

    /// Check if a position is within a wall (not in goal opening)
    pub fn is_wall_position(&self, pos: &Position) -> bool {
        if let Some(config) = &self.team_zone_config {
            // Check if at zone boundary
            let at_team_a_boundary = pos.x == config.end_zone_depth as i16 - 1;
            let at_team_b_boundary = pos.x == self.width as i16 - config.end_zone_depth as i16;

            if at_team_a_boundary || at_team_b_boundary {
                // Check if within goal opening
                if let Some((_x, y_start, y_end)) = self.goal_bounds(if at_team_a_boundary {
                    TeamId(0)
                } else {
                    TeamId(1)
                }) {
                    return pos.y < y_start || pos.y > y_end;
                }
            }
        }
        false
    }

    pub fn is_in_team_base(&self, pos: &Position, team_id: TeamId) -> bool {
        self.team_zone_bounds(team_id)
            .map(|(x_start, x_end, y_start, y_end)| {
                pos.x >= x_start && pos.x <= x_end && pos.y >= y_start && pos.y <= y_end
            })
            .unwrap_or(false)
    }

    pub fn is_in_enemy_base(&self, pos: &Position, team_id: TeamId) -> bool {
        let enemy_team = match team_id.0 {
            0 => TeamId(1),
            1 => TeamId(0),
            _ => TeamId(1),
        };

        self.is_in_team_base(pos, enemy_team)
    }
}

/// Combo balance snapshotted into every game so simulation, prediction, and
/// replay never depend on mutable process configuration.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct ComboConfig {
    /// Authoritative time restored after every successful food pickup.
    pub window_ms: u32,
    /// Highest point/growth value a pickup may reach within one chain.
    pub max_food_value: u32,
    /// Selects the semantic interpretation of the fields above.
    pub rules_version: u16,
}

impl Default for ComboConfig {
    fn default() -> Self {
        Self {
            window_ms: DEFAULT_COMBO_WINDOW_MS,
            max_food_value: DEFAULT_COMBO_MAX_FOOD_VALUE,
            rules_version: COMBO_RULES_VERSION,
        }
    }
}

impl ComboConfig {
    pub fn validate(&self) -> Result<()> {
        if self.window_ms == 0 || self.window_ms > MAX_COMBO_WINDOW_MS {
            return Err(anyhow::anyhow!(
                "Combo window_ms must be in 1..={MAX_COMBO_WINDOW_MS}, got {}",
                self.window_ms
            ));
        }
        if !(1..=DEFAULT_COMBO_MAX_FOOD_VALUE).contains(&self.max_food_value) {
            return Err(anyhow::anyhow!(
                "Combo max_food_value must be in 1..={DEFAULT_COMBO_MAX_FOOD_VALUE}, got {}",
                self.max_food_value
            ));
        }
        if self.rules_version != COMBO_RULES_VERSION {
            return Err(anyhow::anyhow!(
                "unsupported Combo rules version {}, expected {}",
                self.rules_version,
                COMBO_RULES_VERSION
            ));
        }
        Ok(())
    }

    fn food_value_for_chain(&self, chain_count: u32) -> u32 {
        chain_count
            .saturating_sub(1)
            .max(1)
            .min(self.max_food_value.max(1))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct GameProperties {
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub available_food_target: usize,
    pub tick_duration_ms: u32,
    pub time_limit_ms: Option<u32>,
    /// Banked team score that ends a team match. `None` means the mode has no
    /// score target. Team matches set this instead of `time_limit_ms`: they run
    /// until a team gets there, with no clock and no maximum duration.
    #[serde(default)]
    pub score_limit: Option<u32>,
    #[serde(default)]
    pub boost: Option<BoostConfig>,
    /// Combo is universal. A missing historical field receives today's
    /// default; old per-snake state separately defaults to an inactive meter.
    #[serde(default)]
    pub combo: ComboConfig,
    /// Match inactivity policy, snapshotted with the game so every executor
    /// and client resolves the same deadline even across failover.
    #[serde(default = "default_player_idle_timeout_ms")]
    pub player_idle_timeout_ms: u32,
    #[serde(default = "default_player_idle_warning_ms")]
    pub player_idle_warning_ms: u32,
}

fn default_player_idle_timeout_ms() -> u32 {
    DEFAULT_PLAYER_IDLE_TIMEOUT_MS
}

fn default_player_idle_warning_ms() -> u32 {
    DEFAULT_PLAYER_IDLE_WARNING_MS
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct CustomGameSettings {
    pub arena_width: u16,
    pub arena_height: u16,
    pub tick_duration_ms: u32,
    pub food_spawn_rate: f32, // food per minute
    pub max_players: u8,
    pub game_mode: GameMode,
    pub is_private: bool,
    pub allow_spectators: bool,
    pub snake_start_length: u8,
}

impl Default for CustomGameSettings {
    fn default() -> Self {
        CustomGameSettings {
            arena_width: 40,
            arena_height: 40,
            tick_duration_ms: DEFAULT_CUSTOM_GAME_TICK_MS,
            food_spawn_rate: 3.0,
            max_players: 4,
            game_mode: GameMode::FreeForAll { max_players: 4 },
            is_private: true,
            allow_spectators: true,
            snake_start_length: 4,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum GameMode {
    Solo, // Practice mode - just one player
    Duel, // 1v1
    FreeForAll { max_players: u8 },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum QueueMode {
    Quickmatch,  // Quick casual matches
    Competitive, // Ranked competitive matches
}

/// The banked score a team must reach to win, by queue. This is the single
/// source of truth: match construction and snapshot validation both read it, so
/// they cannot drift the way the old post-construction time-limit override did.
pub fn team_score_limit(queue_mode: &QueueMode) -> u32 {
    match queue_mode {
        QueueMode::Quickmatch => DEFAULT_QUICKMATCH_TEAM_SCORE_LIMIT,
        QueueMode::Competitive => DEFAULT_COMPETITIVE_TEAM_SCORE_LIMIT,
    }
}

/// Which Boost fuel model a mode gets on a given map, if any.
///
/// The two collectible models place a fixed pad geometry, and a geometry only
/// exists for that mode's canonical map — so a match on any other size gets no
/// Boost rather than a half-drawn one. That keeps `GameState::new` total: it is
/// called with arbitrary dimensions by tests, previews and Custom games, and
/// none of those should fail to construct.
///
/// Solo is unlimited because a lone runner has nobody to contest pads with, so
/// a meter would only ration a mechanic that exists to feel fast. Needing no
/// geometry, it works on every map.
pub fn boost_config_for(game_type: &GameType, width: u16, height: u16) -> Option<BoostConfig> {
    match game_type {
        GameType::TeamMatch { per_team: 1 | 2 } if (width, height) == (60, 40) => {
            Some(BoostConfig::default())
        }
        GameType::FreeForAll { .. } if (width, height) == (40, 40) => Some(BoostConfig::field()),
        GameType::Solo => Some(BoostConfig::unlimited()),
        _ => None,
    }
}

/// How much food a mode keeps on the field. The crowded modes — 2v2 and
/// free-for-all — carry double, because four snakes competing over one
/// ten-pellet field spend most of the match travelling rather than eating.
/// Duel and Solo keep the baseline: two snakes, or one, do not need the help.
///
/// Read by construction and by the fingerprint, so this is the only place the
/// per-mode value is decided.
pub fn food_target_for(game_type: &GameType) -> usize {
    match game_type {
        GameType::TeamMatch { per_team: 2 } | GameType::FreeForAll { .. } => {
            DEFAULT_FOOD_TARGET * 2
        }
        _ => DEFAULT_FOOD_TARGET,
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum GameType {
    Solo,
    TeamMatch { per_team: u8 },
    FreeForAll { max_players: u8 },
    Custom { settings: CustomGameSettings },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub enum GameStatus {
    Stopped,
    Started {
        #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
        server_id: u64,
    },
    Complete {
        winning_snake_id: Option<u32>,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CommandQueue {
    queue: BinaryHeap<Reverse<GameCommandMessage>>,
    #[serde(serialize_with = "sorted_hash_set::serialize")]
    active_ids: HashSet<CommandId>,
    #[serde(serialize_with = "sorted_hash_set::serialize")]
    tombstone_ids: HashSet<CommandId>,
}

impl Default for CommandQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl CommandQueue {
    pub fn new() -> Self {
        CommandQueue {
            queue: BinaryHeap::new(),
            active_ids: HashSet::new(),
            tombstone_ids: HashSet::new(),
        }
    }

    pub fn has_commands_for_tick(&self, tick: u32) -> bool {
        if let Some(command_message) = self.queue.peek() {
            command_message.0.tick() <= tick
        } else {
            false
        }
    }

    pub fn push(&mut self, command_message: GameCommandMessage) {
        // debug!("CommandQueue::push: Command added to queue");
        // eprintln!("COMMON DEBUG: Command added to queue: {:?}", command_message);
        self.queue.push(Reverse(command_message.clone()));

        if command_message.command_id_server.is_none() {
            self.active_ids
                .insert(command_message.command_id_client.clone());
        } else if self.active_ids.contains(&command_message.command_id_client) {
            // Delete the matching speculative command from the queue. An
            // authoritative-only queue has no active client copy and therefore
            // must not retain one tombstone per server command forever.
            // debug!("CommandQueue::push: Tombstoning client command {:?}", command_message.command_id_client);
            // eprintln!("COMMON DEBUG: Tombstoning client command {:?}", command_message.command_id_client);
            self.tombstone_ids.insert(command_message.command_id_client);
        }
    }

    pub fn pop(&mut self, max_tick: u32) -> Option<GameCommandMessage> {
        // debug!("CommandQueue::pop: Called with max_tick {}", max_tick);
        // eprintln!("COMMON DEBUG: CommandQueue::pop called with max_tick {}", max_tick);
        if let Some(Reverse(command_message)) = self.queue.peek() {
            // debug!("CommandQueue::pop: Peeked command tick: {}, max_tick: {}", command_message.tick(), max_tick);
            // eprintln!("COMMON DEBUG: Peeked command tick: {}, max_tick: {}", command_message.tick(), max_tick);
            if command_message.tick() > max_tick {
                // debug!("CommandQueue::pop: No commands ready for this tick");
                // eprintln!("COMMON DEBUG: No commands ready for this tick");
                return None; // No commands for this tick
            }
        }

        if let Some(Reverse(command_message)) = self.queue.pop() {
            // debug!("CommandQueue::pop: Popped command: {:?}", command_message);
            // eprintln!("COMMON DEBUG: Popped command: {:?}", command_message);
            if command_message.command_id_server.is_none() {
                self.active_ids.remove(&command_message.command_id_client);
                if self
                    .tombstone_ids
                    .remove(&command_message.command_id_client)
                {
                    // eprintln!("COMMON DEBUG: Command {:?} is tombstoned, skipping and popping next", command_message.command_id_client);
                    // Ignore the command if it's a tombstone.
                    // Continue popping the next command.
                    return self.pop(max_tick);
                }
            }
            // debug!("CommandQueue::pop: Returning command: {:?}", command_message);
            // eprintln!("COMMON DEBUG: Returning command: {:?}", command_message);
            Some(command_message)
        } else {
            // debug!("CommandQueue::pop: Queue is empty");
            // eprintln!("COMMON DEBUG: CommandQueue::pop: Queue is empty");
            None
        }
    }

    fn discard_player_commands_for_snake(&mut self, snake_id: u32) {
        let retained: Vec<Reverse<GameCommandMessage>> = self
            .queue
            .drain()
            .filter(|Reverse(message)| {
                !matches!(
                    &message.command,
                    GameCommand::Turn {
                        snake_id: target,
                        ..
                    } | GameCommand::ActivateBoost { snake_id: target }
                    | GameCommand::DeactivateBoost { snake_id: target }
                    | GameCommand::PlayerActivity { snake_id: target }
                        if *target == snake_id
                )
            })
            .collect();
        self.queue = BinaryHeap::from(retained);

        self.rebuild_indexes();
    }

    fn discard_speculative_command(&mut self, command_id_client: &CommandId) {
        let retained: Vec<Reverse<GameCommandMessage>> = self
            .queue
            .drain()
            .filter(|Reverse(message)| {
                message.command_id_server.is_some()
                    || message.command_id_client != *command_id_client
            })
            .collect();
        self.queue = BinaryHeap::from(retained);
        self.rebuild_indexes();
    }

    pub(crate) fn authoritative_commands(&self) -> Vec<GameCommandMessage> {
        let mut commands: Vec<_> = self
            .queue
            .iter()
            .filter(|Reverse(message)| {
                message.command_id_server.is_some()
                    && matches!(
                        message.command,
                        GameCommand::Turn { .. }
                            | GameCommand::ActivateBoost { .. }
                            | GameCommand::DeactivateBoost { .. }
                            | GameCommand::PlayerActivity { .. }
                    )
            })
            .map(|Reverse(message)| message.clone())
            .collect();
        commands.sort_unstable_by(|left, right| left.id().cmp(right.id()));
        commands
    }

    fn rebuild_indexes(&mut self) {
        self.active_ids = self
            .queue
            .iter()
            .filter(|Reverse(message)| message.command_id_server.is_none())
            .map(|Reverse(message)| message.command_id_client.clone())
            .collect();
        let authoritative_client_ids: HashSet<CommandId> = self
            .queue
            .iter()
            .filter(|Reverse(message)| message.command_id_server.is_some())
            .map(|Reverse(message)| message.command_id_client.clone())
            .collect();
        self.tombstone_ids = self
            .active_ids
            .intersection(&authoritative_client_ids)
            .cloned()
            .collect();
    }
}

// Serializable state for snapshots
#[derive(Debug, Serialize, Deserialize, Clone)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct GameState {
    pub tick: u32,
    pub status: GameStatus,
    pub arena: Arena,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent_crashes: Vec<SnakeCrash>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent_goals: Vec<TeamGoal>,
    /// Most recent death transition for each snake. Unlike `recent_crashes`,
    /// this match-history field survives respawns and animation-cue expiry so
    /// post-match presentation can explain what happened. Banking is retained
    /// as `Banked` (and deliberately hidden by the UI) rather than exposing a
    /// previous life as though it were the latest one.
    ///
    /// This is presentation history, not simulation state, and is therefore
    /// intentionally excluded from `sync_hash`.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub last_death_causes: HashMap<u32, DeathCause>,
    pub game_type: GameType,
    pub queue_mode: QueueMode,
    /// Server-attested synthetic game marker. Stress games exercise the full
    /// runtime but must not produce player progression or leaderboard effects.
    #[serde(default)]
    pub is_stress_test: bool,
    pub properties: GameProperties,
    #[cfg_attr(feature = "ts-gen", ts(skip))]
    pub command_queue: CommandQueue,
    // Players by user_id
    pub players: HashMap<u32, Player>,
    #[cfg_attr(feature = "ts-gen", ts(skip))]
    pub rng: Option<PseudoRandom>,
    // Custom game fields
    pub game_code: Option<String>,
    pub host_user_id: Option<u32>,
    // Game start timestamp in milliseconds
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub start_ms: i64,
    // Event sequence number for this game
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub event_sequence: u64,
    // Username mappings by user_id
    pub usernames: HashMap<u32, String>,
    /// Which skin each player is wearing, by user_id.
    ///
    /// Purely cosmetic, and deliberately kept beside `usernames` rather than on
    /// `Snake`: everything on a snake is fingerprinted, and a skin must never
    /// be able to make two clients disagree about the game. Absent entries — and
    /// every state written before skins existed — render as the classic look.
    #[serde(default)]
    pub skins: HashMap<u32, String>,
    /// Which base skin dresses each team's endzone, by team.
    ///
    /// Keyed by team rather than by player because an endzone belongs to a
    /// team, and a 2v2 has two players who may each have equipped a different
    /// base. The server resolves one per team at match creation
    /// (`server::matchmaking::resolve_team_base`) and it never changes
    /// afterwards, so a mid-match joiner reads the same answer everyone else
    /// already has.
    ///
    /// Cosmetic, and out of the sync fingerprint for exactly the reason
    /// `skins` is: how a base is painted must never be able to make two
    /// clients disagree about the game. An absent entry — as in every state
    /// written before base skins existed — means that endzone is painted the
    /// way it always was, from the viewer's own skin theme.
    ///
    /// Deliberately not a protocol bump. The field defaults, `GameState` does
    /// not deny unknown fields, and no client has to understand it for the
    /// match to work — an older one simply paints the endzone the old way. A
    /// hard cutover would disconnect every player mid-match and invalidate
    /// every stored highlight clip (`GAMEPLAY_REPLAY_VERSION` gates playback
    /// on an exact match) to deliver a cosmetic.
    #[serde(default)]
    #[cfg_attr(feature = "ts-gen", ts(type = "Record<number, string>"))]
    pub team_bases: HashMap<TeamId, String>,
    // Spectators by user_id (do not have snakes/players)
    #[serde(serialize_with = "sorted_hash_set::serialize")]
    pub spectators: HashSet<u32>,
    // Score tracking - snake_id -> score
    pub scores: HashMap<u32, u32>,
    /// Cumulative successful pellet pickups by snake. Unlike `scores`, this is
    /// unweighted by combo value and is therefore the progression/XP basis.
    #[serde(default)]
    pub food_pickups: HashMap<u32, u32>,
    // Team scores for team games - team_id -> score
    #[cfg_attr(feature = "ts-gen", ts(type = "Record<number, number> | null"))]
    pub team_scores: Option<HashMap<TeamId, u32>>,

    // XP tracking
    pub player_xp: HashMap<u32, u32>, // user_id -> xp_gained

    /// Authoritative accepted gameplay actions by user. An action is counted
    /// only when execution changes gameplay state: a legal turn or an
    /// inactive-to-active Boost start or active-to-inactive manual stop.
    /// Transport retries, rejected commands, and gameplay no-ops therefore
    /// never inflate this metric.
    #[serde(default)]
    pub player_action_counts: HashMap<u32, u32>,

    /// Tick of the most recent authenticated gameplay input for each player.
    /// Unlike `player_action_counts`, legal no-op inputs count as activity: the
    /// player is demonstrably present even when the requested state is already
    /// active. Transport retries never reach this map twice because command
    /// outcomes are deduplicated before scheduling.
    #[serde(default)]
    pub player_last_activity_ticks: HashMap<u32, u32>,

    /// Players removed from this match for inactivity. They remain in the
    /// roster so results, MMR, and spectators can explain what happened.
    #[serde(default)]
    pub idle_kicked_user_ids: Vec<u32>,

    /// True only when inactivity, rather than the mode's ordinary score/death
    /// condition, produced the terminal result.
    #[serde(default)]
    pub completed_by_inactivity: bool,

    /// Pre-match readiness gate. Present from match creation until every
    /// player has confirmed or the deadline lapses, then cleared for good.
    /// `None` also covers matches created before this protocol existed, which
    /// therefore start straight off `start_ms` exactly as they used to.
    #[serde(default)]
    pub readiness: Option<MatchReadiness>,

    /// Wall clock at which simulation actually begins, once the readiness gate
    /// has resolved. `None` means "use `start_ms`" — either the gate is still
    /// holding the match (in which case nothing may advance at all) or the
    /// match never had a gate.
    #[serde(default)]
    #[cfg_attr(feature = "ts-gen", ts(type = "number | null"))]
    pub simulation_epoch_ms: Option<i64>,
}

/// The pre-match readiness gate: who still has to confirm, and when the match
/// gives up waiting.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct MatchReadiness {
    /// Absolute wall clock after which the gate resolves regardless of who is
    /// still missing.
    #[cfg_attr(feature = "ts-gen", ts(type = "number"))]
    pub deadline_ms: i64,
    /// Users who have confirmed. Serialized sorted so two servers holding the
    /// same logical state produce byte-identical snapshots.
    #[serde(default, serialize_with = "sorted_hash_set::serialize")]
    #[cfg_attr(feature = "ts-gen", ts(type = "Array<number>"))]
    pub ready_user_ids: HashSet<u32>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct CommandId {
    pub tick: u32,
    pub user_id: u32,
    pub sequence_number: u32,
}

impl Ord for CommandId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.tick, self.user_id, self.sequence_number).cmp(&(
            other.tick,
            other.user_id,
            other.sequence_number,
        ))
    }
}

impl PartialOrd for CommandId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Wrapper for BinaryHeap to order commands by their intended execution tick.
#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
#[cfg_attr(feature = "ts-gen", derive(ts_rs::TS))]
#[cfg_attr(feature = "ts-gen", ts(export))]
pub struct GameCommandMessage {
    pub command_id_client: CommandId,
    pub command_id_server: Option<CommandId>,
    pub command: GameCommand,
}

impl GameCommandMessage {
    pub fn tick(&self) -> u32 {
        self.command_id_server
            .as_ref()
            .map_or(self.command_id_client.tick, |id| id.tick)
    }

    pub fn id(&self) -> &CommandId {
        self.command_id_server
            .as_ref()
            .unwrap_or(&self.command_id_client)
    }
}

impl Ord for GameCommandMessage {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.command_id_server
            .as_ref()
            .unwrap_or(&self.command_id_client)
            .cmp(
                other
                    .command_id_server
                    .as_ref()
                    .unwrap_or(&other.command_id_client),
            )
    }
}

impl PartialOrd for GameCommandMessage {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl GameState {
    pub fn new(
        width: u16,
        height: u16,
        game_type: GameType,
        queue_mode: QueueMode,
        rng_seed: Option<u64>,
        start_ms: i64,
    ) -> Self {
        let boost = boost_config_for(&game_type, width, height);
        debug_assert!(boost.as_ref().is_none_or(|c| c.validate().is_ok()));

        // Team matches are raced to a score, never against a clock: no time
        // limit, no maximum duration, and the target depends on the queue.
        // Boost needs the 50ms quantum in every mode that has it: at a 100ms
        // tick a boosted snake would earn two moves in one quantum, which
        // `accrue_movement_credit` refuses. Snakes still cross a cell every
        // 100ms at normal speed — only the simulation granularity changes.
        let boost_tick_ms = if boost.is_some() {
            BOOST_TICK_INTERVAL_MS
        } else {
            DEFAULT_TICK_INTERVAL_MS
        };
        let (tick_duration_ms, time_limit_ms, score_limit) = match &game_type {
            GameType::Custom { settings } => (settings.tick_duration_ms, None, None),
            GameType::TeamMatch { .. } => {
                (boost_tick_ms, None, Some(team_score_limit(&queue_mode)))
            }
            // Solo and free-for-all run until every snake is dead: no clock,
            // and no score target to race to.
            _ => (boost_tick_ms, None, None),
        };

        let properties = GameProperties {
            available_food_target: food_target_for(&game_type),
            tick_duration_ms,
            time_limit_ms,
            score_limit,
            boost,
            combo: ComboConfig::default(),
            player_idle_timeout_ms: DEFAULT_PLAYER_IDLE_TIMEOUT_MS,
            player_idle_warning_ms: DEFAULT_PLAYER_IDLE_WARNING_MS,
        };

        // Set up team zones for team-based games
        let team_zone_config = match &game_type {
            GameType::TeamMatch { .. } => {
                // Calculate goal width as 20% of arena height
                let goal_width = ((height as f32 * 0.2).round() as u16).max(3);
                // Make sure it's odd for symmetry
                let goal_width = if goal_width.is_multiple_of(2) {
                    goal_width + 1
                } else {
                    goal_width
                };

                Some(TeamZoneConfig {
                    end_zone_depth: 10,
                    goal_width,
                })
            }
            _ => None,
        };

        let team_scores = if matches!(&game_type, GameType::TeamMatch { .. }) {
            let mut scores = HashMap::new();
            scores.insert(TeamId(0), 0);
            scores.insert(TeamId(1), 0);
            Some(scores)
        } else {
            None
        };

        let mut arena = Arena {
            width,
            height,
            snakes: Vec::new(),
            food: Vec::new(),
            boost_pads: Vec::new(),
            team_zone_config,
        };
        if let Some(config) = properties.boost.as_ref() {
            arena.boost_pads = arena.boost_pad_layout(config);
        }

        let state = GameState {
            tick: 0,
            status: GameStatus::Stopped,
            arena,
            recent_crashes: Vec::new(),
            recent_goals: Vec::new(),
            last_death_causes: HashMap::new(),
            game_type: game_type.clone(),
            queue_mode,
            is_stress_test: false,
            properties,
            command_queue: CommandQueue::new(),
            players: HashMap::new(),
            rng: rng_seed.map(PseudoRandom::new),
            game_code: None,
            host_user_id: None,
            start_ms,
            event_sequence: 0,
            usernames: HashMap::new(),
            skins: HashMap::new(),
            team_bases: HashMap::new(),
            spectators: HashSet::new(),
            scores: HashMap::new(),
            food_pickups: HashMap::new(),
            team_scores,

            player_xp: HashMap::new(),
            player_action_counts: HashMap::new(),
            player_last_activity_ticks: HashMap::new(),
            idle_kicked_user_ids: Vec::new(),
            completed_by_inactivity: false,
            readiness: None,
            simulation_epoch_ms: None,
        };

        // `new` predates fallible match construction. Keep its API stable but
        // fail closed if an eligible map cannot materialize a complete valid
        // Boost layout; production builders use canonical dimensions.
        state
            .validate_boost_invariants()
            .expect("new game must satisfy Boost invariants");
        state
    }

    /// Authoritative simulated match duration represented by this snapshot.
    /// A zero tick interval remains zero so rate consumers can safely handle a
    /// malformed or historical zero-duration match without manufacturing time.
    pub fn elapsed_match_ms(&self) -> u64 {
        u64::from(self.tick).saturating_mul(u64::from(self.properties.tick_duration_ms))
    }

    pub fn player_action_count(&self, user_id: u32) -> u32 {
        self.player_action_counts
            .get(&user_id)
            .copied()
            .unwrap_or_default()
    }

    pub fn is_player_idle_kicked(&self, user_id: u32) -> bool {
        self.idle_kicked_user_ids.binary_search(&user_id).is_ok()
    }

    /// Record presence only for an authenticated player's own gameplay
    /// command. Callers invoke this at first scheduling, never at execution,
    /// so deferred turns and transport retries cannot extend the deadline.
    fn record_player_activity_for_command(
        &mut self,
        command_message: &GameCommandMessage,
        activity_tick: u32,
    ) {
        let user_id = command_message
            .command_id_server
            .as_ref()
            .map(|id| id.user_id)
            .unwrap_or(command_message.command_id_client.user_id);
        if self.is_player_idle_kicked(user_id) {
            return;
        }
        let snake_id = match &command_message.command {
            GameCommand::Turn { snake_id, .. }
            | GameCommand::ActivateBoost { snake_id }
            | GameCommand::DeactivateBoost { snake_id }
            | GameCommand::PlayerActivity { snake_id } => *snake_id,
            GameCommand::UpdateStatus { .. } => return,
        };
        if self
            .players
            .get(&user_id)
            .is_some_and(|player| player.snake_id == snake_id)
        {
            self.player_last_activity_ticks
                .insert(user_id, activity_tick);
        }
    }

    fn record_player_action_for_snake(&mut self, snake_id: u32) {
        // Keep this at scheduled-command execution, not generic event
        // application. Replicas execute the authoritative scheduled command
        // and later receive its confirming event; counting the confirmation
        // would double-count legal turns.
        // A valid game has exactly one user for a snake. Selecting the lowest
        // ID also keeps behavior deterministic if malformed historical data
        // ever contains duplicate ownership.
        let user_id = self
            .players
            .iter()
            .filter_map(|(user_id, player)| (player.snake_id == snake_id).then_some(*user_id))
            .min();
        if let Some(user_id) = user_id {
            let count = self.player_action_counts.entry(user_id).or_default();
            *count = count.saturating_add(1);
        }
    }

    /// Construct an eligible match with an explicitly resolved, snapshotted
    /// Boost balance. This is the configuration seam for staging, soak tests,
    /// and future server-side balance selection; active games never re-read
    /// live configuration.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_boost_config(
        width: u16,
        height: u16,
        game_type: GameType,
        queue_mode: QueueMode,
        rng_seed: Option<u64>,
        start_ms: i64,
        boost_config: BoostConfig,
    ) -> Result<Self> {
        boost_config.validate()?;
        if !matches!(
            &game_type,
            GameType::TeamMatch { per_team: 1 | 2 } | GameType::FreeForAll { .. } | GameType::Solo
        ) {
            return Err(anyhow::anyhow!(
                "explicit Boost configuration requires duel, 2v2, free-for-all or Solo"
            ));
        }

        let mut state = Self::new(width, height, game_type, queue_mode, rng_seed, start_ms);
        state.properties.boost = Some(boost_config);
        state.arena.boost_pads = state
            .arena
            .boost_pad_layout(state.properties.boost.as_ref().expect("Boost config"));
        state.validate_boost_invariants()?;
        Ok(state)
    }

    /// Whether this is an immutable terminal snapshot from either gameplay
    /// generation immediately preceding the current rules.
    ///
    /// Completed history remains viewable across the coordinated cutover, but
    /// active recovery stays fail-closed. Two exact historical shapes exist:
    ///
    /// - boostless 100ms duel/2v2/Solo/FFA games from before those modes gained
    ///   their current Boost model; and
    /// - 50ms Boost team games that ended on the old 90-second clock before
    ///   team matches changed to a score race.
    ///
    /// Current-protocol snapshots and every nonterminal state must satisfy the
    /// strict invariants instead of entering through this compatibility seam.
    pub fn is_legacy_completed_snapshot(&self) -> bool {
        if !matches!(self.status, GameStatus::Complete { .. }) {
            return false;
        }

        if self.properties.boost.is_none() {
            let legacy_time_limit_ms = match &self.game_type {
                GameType::TeamMatch { per_team: 1 | 2 } => Some(90_000),
                GameType::Solo | GameType::FreeForAll { .. } => None,
                _ => return false,
            };
            return boost_config_for(&self.game_type, self.arena.width, self.arena.height)
                .is_some()
                && self.properties.available_food_target == DEFAULT_FOOD_TARGET
                && self.properties.tick_duration_ms == DEFAULT_TICK_INTERVAL_MS
                && self.properties.time_limit_ms == legacy_time_limit_ms
                && self.properties.score_limit.is_none()
                && self.arena.boost_pads.is_empty()
                && self.arena.snakes.iter().all(|snake| {
                    snake.speed_milli() == NORMAL_SNAKE_SPEED_MILLI
                        && snake.movement_credit() == 0
                        && snake.boost() == &SnakeBoost::default()
                        && snake.combo == SnakeCombo::default()
                });
        }

        if !matches!(self.game_type, GameType::TeamMatch { per_team: 1 | 2 })
            || self.properties.time_limit_ms != Some(90_000)
            || self.properties.score_limit.is_some()
        {
            return false;
        }

        // The previous Boost-team state is otherwise byte-for-byte governed
        // by today's strict invariants. Rewrite only its obsolete completion
        // condition on a clone, then reuse that validator rather than growing
        // a second, weaker copy of the pad/snake rules.
        let mut migrated = self.clone();
        migrated.properties.available_food_target = food_target_for(&migrated.game_type);
        migrated.properties.time_limit_ms = None;
        migrated.properties.score_limit = Some(team_score_limit(&migrated.queue_mode));
        migrated.validate_boost_invariants().is_ok()
    }

    /// The tank size to hand a snake starting a new life, in modes whose Boost
    /// never empties. `None` everywhere else, where a new life starts dry and
    /// the player refuels from the map.
    fn unlimited_boost_capacity_ms(&self) -> Option<u32> {
        self.properties
            .boost
            .as_ref()
            .filter(|config| config.unlimited)
            .map(|config| config.capacity_ms)
    }

    /// Validate every cross-field Boost invariant at a serialized tick
    /// boundary. This is used for snapshot/recovery admission as well as match
    /// creation, preventing malformed state from producing permanent speed or
    /// movement divergence.
    pub fn validate_boost_invariants(&self) -> Result<()> {
        self.properties.combo.validate()?;
        for (snake_id, snake) in self.arena.snakes.iter().enumerate() {
            if snake.combo.remaining_ms > self.properties.combo.window_ms {
                return Err(anyhow::anyhow!(
                    "snake {snake_id} Combo remaining_ms exceeds the configured window"
                ));
            }
            let inactive = snake.combo.remaining_ms == 0;
            if inactive != (snake.combo.chain_count == 0) {
                return Err(anyhow::anyhow!(
                    "snake {snake_id} Combo chain and timer must be active or inactive together"
                ));
            }
            if !snake.is_alive && snake.combo != SnakeCombo::default() {
                return Err(anyhow::anyhow!(
                    "dead snake {snake_id} must have an inactive Combo"
                ));
            }
        }
        for snake_id in self.food_pickups.keys() {
            if *snake_id as usize >= self.arena.snakes.len() {
                return Err(anyhow::anyhow!(
                    "food pickup count references missing snake {snake_id}"
                ));
            }
        }
        for (snake_id, cause) in &self.last_death_causes {
            if *snake_id as usize >= self.arena.snakes.len() {
                return Err(anyhow::anyhow!(
                    "last death cause references missing snake {snake_id}"
                ));
            }
            let attributed_snake_id = match cause {
                DeathCause::SnakeBody { killer_snake_id } => Some(*killer_snake_id),
                DeathCause::HeadToHead { other_snake_id } => Some(*other_snake_id),
                DeathCause::Unknown
                | DeathCause::Wall
                | DeathCause::OutOfBounds
                | DeathCause::EnemyBase
                | DeathCause::SelfCollision
                | DeathCause::Banked => None,
            };
            if attributed_snake_id
                .is_some_and(|attributed| attributed as usize >= self.arena.snakes.len())
            {
                return Err(anyhow::anyhow!(
                    "last death cause for snake {snake_id} references missing attributed snake {}",
                    attributed_snake_id.unwrap()
                ));
            }
        }

        // A mode is Boost-eligible only on a map that can host its layout, so
        // this asks exactly the question construction answered. Custom games
        // are player-defined and never carry Boost.
        let expected_config =
            boost_config_for(&self.game_type, self.arena.width, self.arena.height);
        let eligible = expected_config.is_some();
        if !matches!(self.game_type, GameType::Custom { .. }) {
            let expected_food_target = food_target_for(&self.game_type);
            if self.properties.available_food_target != expected_food_target {
                return Err(anyhow::anyhow!(
                    "{:?} requires food target {}, got {}",
                    self.game_type,
                    expected_food_target,
                    self.properties.available_food_target
                ));
            }
        }
        let normal_interval_ms = self.normal_movement_interval_ms();
        let movement_threshold =
            u64::from(NORMAL_SNAKE_SPEED_MILLI) * u64::from(normal_interval_ms);

        match &self.properties.boost {
            Some(config) => {
                if !eligible {
                    return Err(anyhow::anyhow!(
                        "Boost configuration is only valid for duel, 2v2, free-for-all and Solo"
                    ));
                }
                config.validate()?;
                // The fuel model is a property of the mode, so a state cannot
                // claim (say) an unlimited tank in a contested match.
                let expected_layout_version = expected_config
                    .as_ref()
                    .map(|expected| expected.spot_layout_version)
                    .unwrap_or(BOOST_SPOT_LAYOUT_VERSION_NONE);
                if config.spot_layout_version != expected_layout_version {
                    return Err(anyhow::anyhow!(
                        "{:?} requires Boost spot layout {}, got {}",
                        self.game_type,
                        expected_layout_version,
                        config.spot_layout_version
                    ));
                }
                if self.properties.tick_duration_ms != BOOST_TICK_INTERVAL_MS {
                    return Err(anyhow::anyhow!(
                        "Boost match tick must be {}ms, got {}ms",
                        BOOST_TICK_INTERVAL_MS,
                        self.properties.tick_duration_ms
                    ));
                }
                if self.properties.time_limit_ms.is_some() {
                    return Err(anyhow::anyhow!(
                        "Boost matches are raced to a score and must not carry a time limit, got {:?}",
                        self.properties.time_limit_ms
                    ));
                }
                // Only team matches race to a score. Solo and free-for-all end
                // when every snake is dead, so a target there would be a win
                // condition nothing in the engine ever tests.
                let expected_score_limit = match &self.game_type {
                    GameType::TeamMatch { .. } => Some(team_score_limit(&self.queue_mode)),
                    _ => None,
                };
                if self.properties.score_limit != expected_score_limit {
                    return Err(anyhow::anyhow!(
                        "Boost match score limit must be exactly {:?} for {:?} in {:?}, got {:?}",
                        expected_score_limit,
                        self.game_type,
                        self.queue_mode,
                        self.properties.score_limit
                    ));
                }

                let expected_layout = self.arena.boost_pad_layout(config);
                let expected: Vec<(u8, Position, u32, u8)> = expected_layout
                    .iter()
                    .map(|pad| (pad.id, pad.position, pad.charge_ms, pad.size_cells))
                    .collect();
                let actual: Vec<(u8, Position, u32, u8)> = self
                    .arena
                    .boost_pads
                    .iter()
                    .map(|pad| (pad.id, pad.position, pad.charge_ms, pad.size_cells))
                    .collect();
                // An unlimited tank places no pads; every collectible layout
                // places exactly the canonical twelve.
                let expected_pad_count = if config.unlimited { 0 } else { 12 };
                if expected.len() != expected_pad_count || actual != expected {
                    return Err(anyhow::anyhow!(
                        "Boost layout v{} requires {} canonical value/footprint pads, got {} (expected geometry produced {})",
                        config.spot_layout_version,
                        expected_pad_count,
                        actual.len(),
                        expected.len()
                    ));
                }

                let mut pad_ids = HashSet::new();
                let mut footprint_cells = HashSet::new();
                // Pads must stay inside the span their own layout is drawn in:
                // the end-zone-inset field for team maps, the whole arena for
                // the teamless one. An unlimited config has no pads to place,
                // so it needs no span.
                let field_bounds = self.arena.boost_field_bounds(config.spot_layout_version);
                let (field_left, field_right) = match field_bounds {
                    Some(bounds) => bounds,
                    None if config.unlimited => (0, self.arena.width as i16 - 1),
                    None => {
                        return Err(anyhow::anyhow!(
                            "Boost layout v{} is not drawable on this {}x{} arena",
                            config.spot_layout_version,
                            self.arena.width,
                            self.arena.height
                        ));
                    }
                };
                for pad in &self.arena.boost_pads {
                    if !pad_ids.insert(pad.id) {
                        return Err(anyhow::anyhow!("Boost pad IDs must be unique"));
                    }
                    if !matches!(pad.size_cells, 1 | 2)
                        || pad.charge_ms == 0
                        || pad.charge_ms > config.capacity_ms
                        || !pad.charge_ms.is_multiple_of(BOOST_TICK_INTERVAL_MS)
                    {
                        return Err(anyhow::anyhow!(
                            "Boost pad footprint and charge must satisfy the configured layout"
                        ));
                    }
                    let cells = pad.footprint_cells();
                    if cells.len() != usize::from(pad.size_cells).pow(2) {
                        return Err(anyhow::anyhow!(
                            "Boost pad footprint overflowed coordinates"
                        ));
                    }
                    for cell in cells {
                        if cell.x < field_left
                            || cell.x > field_right
                            || cell.y < 0
                            || cell.y >= self.arena.height as i16
                            || self.arena.is_wall_position(&cell)
                        {
                            return Err(anyhow::anyhow!(
                                "Boost pad footprint must stay in the playable main field"
                            ));
                        }
                        if !footprint_cells.insert(cell) {
                            return Err(anyhow::anyhow!("Boost pad footprints must not overlap"));
                        }
                        if self.arena.food.contains(&cell) {
                            return Err(anyhow::anyhow!(
                                "food cannot overlap a Boost pad footprint"
                            ));
                        }
                    }
                    if pad
                        .respawn_at_tick
                        .is_some_and(|respawn_at_tick| respawn_at_tick <= self.tick)
                    {
                        return Err(anyhow::anyhow!(
                            "Boost pad cooldown must point to a future absolute tick"
                        ));
                    }
                }

                for snake in &self.arena.snakes {
                    if snake.boost.charge_ms > config.capacity_ms
                        || !snake.boost.charge_ms.is_multiple_of(BOOST_TICK_INTERVAL_MS)
                    {
                        return Err(anyhow::anyhow!(
                            "snake Boost charge must be whole-quantum and within capacity"
                        ));
                    }
                    if snake.boost.active {
                        if snake.boost.charge_ms == 0 || snake.speed_milli != config.speed_milli {
                            return Err(anyhow::anyhow!(
                                "active Boost requires funded charge and configured speed"
                            ));
                        }
                    } else if snake.speed_milli != NORMAL_SNAKE_SPEED_MILLI {
                        return Err(anyhow::anyhow!("inactive snake must have normal speed"));
                    }
                    if config.unlimited
                        && snake.is_alive
                        && snake.boost.charge_ms != config.capacity_ms
                    {
                        return Err(anyhow::anyhow!(
                            "living snake in an unlimited Boost match must retain full charge"
                        ));
                    }
                    // Latched intent is deliberately excluded: it describes what
                    // the player is physically doing, not simulation state, and
                    // it survives death by design so a held control resumes on
                    // the new life without a re-press. Comparing the whole
                    // struct against `Default` would reject every snapshot taken
                    // while somebody is holding Boost through a death.
                    if !snake.is_alive
                        && (snake.boost.charge_ms != 0
                            || snake.boost.active
                            || snake.movement_credit != 0)
                    {
                        return Err(anyhow::anyhow!(
                            "dead snake must have cleared Boost charge, activation and movement credit"
                        ));
                    }
                    if u64::from(snake.movement_credit) >= movement_threshold {
                        return Err(anyhow::anyhow!(
                            "snake movement credit must remain below one movement threshold"
                        ));
                    }
                }
            }
            None => {
                if eligible {
                    return Err(anyhow::anyhow!(
                        "{:?} on a {}x{} arena requires Boost configuration",
                        self.game_type,
                        self.arena.width,
                        self.arena.height
                    ));
                }
                if !self.arena.boost_pads.is_empty() {
                    return Err(anyhow::anyhow!("non-Boost match cannot contain Boost pads"));
                }
                for snake in &self.arena.snakes {
                    if snake.speed_milli != NORMAL_SNAKE_SPEED_MILLI
                        || snake.boost != Default::default()
                    {
                        return Err(anyhow::anyhow!(
                            "non-Boost snake must remain normal and uncharged"
                        ));
                    }
                    if u64::from(snake.movement_credit) >= movement_threshold {
                        return Err(anyhow::anyhow!(
                            "snake movement credit must remain below one movement threshold"
                        ));
                    }
                }
            }
        }

        let timeout_ms = self.properties.player_idle_timeout_ms;
        let warning_ms = self.properties.player_idle_warning_ms;
        if timeout_ms == 0 || warning_ms == 0 || warning_ms >= timeout_ms {
            return Err(anyhow::anyhow!(
                "player inactivity warning must be positive and shorter than timeout"
            ));
        }
        if self
            .idle_kicked_user_ids
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(anyhow::anyhow!(
                "idle-kicked player IDs must be sorted and unique"
            ));
        }
        for user_id in &self.idle_kicked_user_ids {
            let player = self
                .players
                .get(user_id)
                .with_context(|| format!("idle-kicked user {user_id} is not a player"))?;
            let snake = self
                .arena
                .snakes
                .get(player.snake_id as usize)
                .with_context(|| format!("idle-kicked user {user_id} has no snake"))?;
            if snake.is_alive {
                return Err(anyhow::anyhow!(
                    "idle-kicked user {user_id} must have a dead snake"
                ));
            }
        }
        for (user_id, activity_tick) in &self.player_last_activity_ticks {
            if !self.players.contains_key(user_id) || *activity_tick > self.tick {
                return Err(anyhow::anyhow!(
                    "player activity tick must reference a current player at or before the snapshot tick"
                ));
            }
        }
        if self.completed_by_inactivity && !self.is_complete() {
            return Err(anyhow::anyhow!(
                "inactivity completion marker requires a terminal game"
            ));
        }
        Ok(())
    }

    /// Validate one state-bearing replicated event against this exact pre-event
    /// state. The ordinary engine event applier is intentionally permissive so
    /// locally-derived/idempotent events remain cheap; untrusted network
    /// deltas need stricter admission because silently applying only the pad or
    /// only the snake half of a Boost collection permanently forks replicas.
    pub fn validate_replicated_event_transition(&self, event: &GameEvent) -> Result<()> {
        let require_snake = |snake_id: u32| {
            self.arena
                .snakes
                .get(snake_id as usize)
                .with_context(|| format!("replicated event references missing snake {snake_id}"))
        };

        match event {
            GameEvent::Snapshot { game_state } => game_state.validate_boost_invariants(),
            GameEvent::SnakeTurned { snake_id, .. }
            | GameEvent::ScoreUpdated { snake_id, .. }
            | GameEvent::SnakeRespawned { snake_id, .. } => {
                require_snake(*snake_id)?;
                Ok(())
            }
            GameEvent::SnakeDied { snake_id, cause } => {
                require_snake(*snake_id)?;
                match cause {
                    DeathCause::SnakeBody { killer_snake_id } => {
                        require_snake(*killer_snake_id)?;
                    }
                    DeathCause::HeadToHead { other_snake_id } => {
                        require_snake(*other_snake_id)?;
                    }
                    DeathCause::Unknown
                    | DeathCause::Wall
                    | DeathCause::OutOfBounds
                    | DeathCause::EnemyBase
                    | DeathCause::SelfCollision
                    | DeathCause::Banked => {}
                }
                Ok(())
            }
            GameEvent::FoodEaten {
                snake_id,
                position,
                points,
                combo_chain,
                combo_remaining_ms_before,
                boost_active,
            } => {
                let snake = require_snake(*snake_id)?;
                if !self.combo_event_fields_are_bounded(
                    *points,
                    *combo_chain,
                    *combo_remaining_ms_before,
                ) {
                    return Err(anyhow::anyhow!(
                        "food event has invalid Combo award/context fields"
                    ));
                }

                // A predicted client may already have consumed this pellet;
                // `apply_event` is deliberately idempotent in that case. When
                // it is still present, however, every telemetry field must
                // describe this exact pre-pickup state.
                if self.has_food(position) {
                    if !snake.is_alive || snake.head().ok() != Some(position) {
                        return Err(anyhow::anyhow!(
                            "food event snake {snake_id} is not alive on the pellet"
                        ));
                    }
                    let expected = self.combo_pickup_context(snake);
                    if expected
                        != (
                            *points,
                            *combo_chain,
                            *combo_remaining_ms_before,
                            *boost_active,
                        )
                    {
                        return Err(anyhow::anyhow!(
                            "food event Combo telemetry does not match authoritative pre-pickup state"
                        ));
                    }
                }
                Ok(())
            }
            GameEvent::PlayerIdleKicked { user_id, snake_id } => {
                require_snake(*snake_id)?;
                let player = self.players.get(user_id).with_context(|| {
                    format!("idle-kick event references missing player {user_id}")
                })?;
                if player.snake_id != *snake_id {
                    return Err(anyhow::anyhow!(
                        "idle-kick event snake {snake_id} is not owned by player {user_id}"
                    ));
                }
                if self.is_player_idle_kicked(*user_id)
                    && self.arena.snakes[*snake_id as usize].is_alive
                {
                    return Err(anyhow::anyhow!(
                        "idle-kick event conflicts with a live removed player {user_id}"
                    ));
                }
                Ok(())
            }
            GameEvent::BoostPacketCollected {
                pad_id,
                snake_id,
                charge_ms_after,
                respawn_at_tick,
            } => {
                let config = self
                    .properties
                    .boost
                    .as_ref()
                    .context("Boost collection delta has no snapshotted Boost config")?;
                let pad = self
                    .arena
                    .boost_pads
                    .iter()
                    .find(|pad| pad.id == *pad_id)
                    .with_context(|| {
                        format!("Boost collection delta references missing pad {pad_id}")
                    })?;
                let snake = require_snake(*snake_id)?;
                if !snake.is_alive || !snake.head().is_ok_and(|head| pad.contains(head)) {
                    return Err(anyhow::anyhow!(
                        "Boost collection delta snake {snake_id} is not alive on pad {pad_id}"
                    ));
                }

                let cooldown_ticks = config.pad_respawn_ms / self.properties.tick_duration_ms;
                let expected_respawn_at_tick = self
                    .tick
                    .checked_add(cooldown_ticks)
                    .context("Boost collection cooldown tick overflow")?;
                if *respawn_at_tick != expected_respawn_at_tick {
                    return Err(anyhow::anyhow!(
                        "Boost collection delta for pad {pad_id} has respawn tick {respawn_at_tick}, expected {expected_respawn_at_tick}"
                    ));
                }

                match pad.respawn_at_tick {
                    // Movement is derived before its confirming event arrives,
                    // so an exact already-applied transition is legitimate.
                    Some(existing_respawn)
                        if existing_respawn == *respawn_at_tick
                            && snake.boost.charge_ms == *charge_ms_after =>
                    {
                        Ok(())
                    }
                    None => {
                        let expected_charge = snake
                            .boost
                            .charge_ms
                            .saturating_add(pad.charge_ms)
                            .min(config.capacity_ms);
                        if snake.boost.charge_ms >= config.capacity_ms
                            || *charge_ms_after != expected_charge
                        {
                            return Err(anyhow::anyhow!(
                                "Boost collection delta for snake {snake_id} has charge {charge_ms_after}, expected {expected_charge}"
                            ));
                        }
                        Ok(())
                    }
                    Some(existing_respawn) => Err(anyhow::anyhow!(
                        "Boost collection delta conflicts with pad {pad_id} cooldown {existing_respawn} and snake charge {}",
                        snake.boost.charge_ms
                    )),
                }
            }
            GameEvent::CommandScheduled { command_message }
            | GameEvent::CommandScheduledV2 {
                command_message, ..
            } => {
                if let GameCommand::Turn { snake_id, .. }
                | GameCommand::ActivateBoost { snake_id }
                | GameCommand::DeactivateBoost { snake_id }
                | GameCommand::PlayerActivity { snake_id } = &command_message.command
                {
                    require_snake(*snake_id)?;
                }
                Ok(())
            }
            GameEvent::TeamScoreUpdated { team_id, .. } => {
                if !self
                    .team_scores
                    .as_ref()
                    .is_some_and(|scores| scores.contains_key(team_id))
                {
                    return Err(anyhow::anyhow!(
                        "replicated score event references missing team {}",
                        team_id.0
                    ));
                }
                Ok(())
            }
            // A replicated readiness confirmation must name a real player of
            // this match. Accepting an arbitrary user id would let a corrupted
            // or spoofed delta pad the ready set and release the gate early.
            GameEvent::PlayerReady { user_id } => {
                if !self.players.contains_key(user_id) {
                    return Err(anyhow::anyhow!(
                        "replicated readiness references user {user_id}, who is not a player"
                    ));
                }
                Ok(())
            }
            GameEvent::FoodSpawned { .. }
            | GameEvent::CommandRejected { .. }
            | GameEvent::StatusUpdated { .. }
            | GameEvent::MatchStartScheduled { .. }
            | GameEvent::XPAwarded { .. }
            | GameEvent::TickHash { .. } => Ok(()),
        }
    }

    /// Apply an untrusted delta transactionally. Any semantic failure or
    /// post-application Boost invariant leaves the original state untouched.
    pub fn try_apply_replicated_event(&mut self, event: GameEvent) -> Result<()> {
        self.validate_replicated_event_transition(&event)?;
        let mut candidate = self.clone();
        candidate.apply_event(event, None);
        candidate.validate_boost_invariants()?;
        *self = candidate;
        Ok(())
    }

    pub fn current_tick(&self) -> u32 {
        self.tick
    }

    fn normal_movement_interval_ms(&self) -> u32 {
        match &self.game_type {
            GameType::Custom { settings } => settings.tick_duration_ms.max(1),
            _ => DEFAULT_TICK_INTERVAL_MS,
        }
    }

    fn wall_clock_interval_due(&self, interval_ms: u32) -> bool {
        let tick_ms = self.properties.tick_duration_ms.max(1) as u64;
        let interval_ms = interval_ms.max(1) as u64;
        let elapsed_before = self.tick as u64 * tick_ms;
        let elapsed_after = self.tick.saturating_add(1) as u64 * tick_ms;
        elapsed_before / interval_ms != elapsed_after / interval_ms
    }

    fn food_refill_due(&self) -> bool {
        // Boost halves the authoritative quantum, but food balance remains a
        // roughly 100 ms wall-clock opportunity. Every other mode retains the
        // original once-per-configured-tick refill attempt—including Custom
        // games whose tick is faster than or not divisible by 100 ms.
        self.properties.boost.is_none() || self.wall_clock_interval_due(DEFAULT_TICK_INTERVAL_MS)
    }

    pub fn is_complete(&self) -> bool {
        matches!(self.status, GameStatus::Complete { .. })
    }

    fn get_snake_mut(&mut self, snake_id: u32) -> Result<&mut Snake> {
        self.arena
            .snakes
            .get_mut(snake_id as usize)
            .context("Snake not found")
    }

    fn iter_snakes(&self) -> impl Iterator<Item = (u32, &Snake)> {
        self.arena
            .snakes
            .iter()
            .enumerate()
            .map(|(id, snake)| (id as u32, snake))
    }

    fn has_food(&self, position: &Position) -> bool {
        self.arena.food.contains(position)
    }

    fn remove_food(&mut self, position: &Position) -> bool {
        // A position denotes one pellet even if malformed historical state
        // contains duplicate Vec entries. Removing every duplicate prevents
        // that logical pellet from being awarded again on a later quantum.
        let before = self.arena.food.len();
        self.arena.food.retain(|candidate| candidate != position);
        self.arena.food.len() != before
    }

    fn combo_pickup_context(&self, snake: &Snake) -> (u32, u32, u32, bool) {
        let combo_remaining_ms_before = snake.combo.remaining_ms;
        let combo_chain = if combo_remaining_ms_before == 0 {
            1
        } else {
            snake.combo.chain_count.saturating_add(1).max(1)
        };
        let points = self.properties.combo.food_value_for_chain(combo_chain);
        (
            points,
            combo_chain,
            combo_remaining_ms_before,
            snake.boost().active,
        )
    }

    fn combo_event_fields_are_bounded(
        &self,
        points: u32,
        combo_chain: u32,
        combo_remaining_ms_before: u32,
    ) -> bool {
        combo_chain > 0
            && combo_remaining_ms_before <= self.properties.combo.window_ms
            && ((combo_chain == 1 && combo_remaining_ms_before == 0)
                || (combo_chain > 1 && combo_remaining_ms_before > 0))
            && points == self.properties.combo.food_value_for_chain(combo_chain)
    }

    fn calculate_starting_positions(&self, player_count: usize) -> Vec<(Position, Direction)> {
        let mut positions = Vec::new();
        let arena_width = self.arena.width as i16;
        let arena_height = self.arena.height as i16;

        // Get snake length from custom settings or use default
        let snake_length = self.starting_snake_length() as i16;

        // For team games, adjust starting positions to be in the main field
        let (left_boundary, right_boundary) =
            if let Some((left, right)) = self.arena.main_field_bounds() {
                (left + 2, right - 2) // Add buffer from walls
            } else {
                (0, arena_width - 1)
            };

        match player_count {
            0 => {}
            1 => {
                // Single snake starts on the right side of main field, facing left
                let x = right_boundary - snake_length;
                let y = arena_height / 2;
                positions.push((Position { x, y }, Direction::Left));
            }
            2 => {
                // Check if this is a TeamMatch (duel) game
                if let GameType::TeamMatch { per_team: 1 } = &self.game_type {
                    // Duel mode: snakes start in their own endzones
                    let y = arena_height / 2;

                    // Team A in left endzone (centered at x=5), facing right toward Team B's goal
                    positions.push((Position { x: 5, y }, Direction::Right));

                    // Team B in right endzone (centered at x=arena_width-5), facing left toward Team A's goal
                    positions.push((
                        Position {
                            x: arena_width - 5,
                            y,
                        },
                        Direction::Left,
                    ));
                } else {
                    // FreeForAll: Two snakes start on opposite sides of main field, facing each other
                    let y = arena_height / 2;

                    // Right side of main field, facing left
                    let x_right = right_boundary - snake_length;
                    positions.push((Position { x: x_right, y }, Direction::Left));

                    // Left side of main field, facing right
                    let x_left = left_boundary + snake_length;
                    positions.push((Position { x: x_left, y }, Direction::Right));
                }
            }
            _ => {
                // More than 2 players: arranged in two columns facing each other
                let left_count = player_count.div_ceil(2);
                let right_count = player_count / 2;

                // Calculate vertical spacing
                let vertical_margin = 2;
                // Coordinates run through `height - 1`. Subtracting from the
                // dimension itself put the lower row at y=38 on a 40-cell map
                // while the upper row was y=2, giving bottom spawns a longer
                // path to their mirrored Boost pad. Use the inclusive span so
                // the rows are exactly mirrored (2 and 37).
                let usable_height = arena_height - 1 - 2 * vertical_margin;

                // Left column (facing right) - use main field boundaries
                let x_left = left_boundary + snake_length;
                for i in 0..left_count {
                    let y = if left_count == 1 {
                        arena_height / 2
                    } else {
                        vertical_margin + (i as i16 * usable_height) / (left_count - 1) as i16
                    };
                    positions.push((Position { x: x_left, y }, Direction::Right));
                }

                // Right column (facing left) - use main field boundaries
                let x_right = right_boundary - snake_length;
                for i in 0..right_count {
                    let y = if right_count == 1 {
                        arena_height / 2
                    } else {
                        vertical_margin + (i as i16 * usable_height) / (right_count - 1) as i16
                    };
                    positions.push((Position { x: x_right, y }, Direction::Left));
                }
            }
        }

        positions
    }

    fn calculate_team_starting_positions(&self) -> Vec<(Position, Direction)> {
        if !matches!(self.game_type, GameType::TeamMatch { .. }) {
            return self.calculate_starting_positions(self.players.len());
        }

        let Some(config) = &self.arena.team_zone_config else {
            return self.calculate_starting_positions(self.players.len());
        };

        let mut positions: Vec<Option<(Position, Direction)>> = vec![None; self.arena.snakes.len()];

        let mut team_snakes: [Vec<usize>; 2] = [Vec::new(), Vec::new()];
        for (idx, snake) in self.arena.snakes.iter().enumerate() {
            match snake.team_id {
                Some(TeamId(0)) => team_snakes[0].push(idx),
                Some(TeamId(1)) => team_snakes[1].push(idx),
                _ => {}
            }
        }

        let _snake_length = self.starting_snake_length() as i16;
        let width = self.arena.width as i16;
        let height = self.arena.height as i16;
        let end_zone_depth = config.end_zone_depth as i16;

        // Place snakes near the goal opening so they face the gap instead of a wall
        let positions_for_side =
            |count: usize, team_id: TeamId, is_left: bool| -> Vec<(Position, Direction)> {
                let mut side_positions = Vec::with_capacity(count);
                if count == 0 {
                    return side_positions;
                }

                let boundary_x = if is_left {
                    end_zone_depth - 1
                } else {
                    width - end_zone_depth
                };
                // Head sits one cell inside the boundary so first move reaches the gate column
                let head_x = if is_left {
                    (boundary_x - 1).max(0)
                } else {
                    (boundary_x + 1).min(width - 1)
                };

                // Use goal opening for vertical placement to align with the gate
                let (_goal_x, y_start, y_end) =
                    self.arena
                        .goal_bounds(team_id)
                        .unwrap_or((boundary_x, height / 2, height / 2));
                let gate_top = y_start.max(0);
                let gate_bottom = y_end.min(height - 1);
                let gate_span = (gate_bottom - gate_top).max(0);

                for i in 0..count {
                    let y = if count == 1 {
                        (gate_top + gate_bottom) / 2
                    } else {
                        // Evenly space within gate interior, avoiding the extreme ends
                        let spacing = (gate_span as f64) / ((count as f64) + 1.0);
                        let pos = gate_top as f64 + spacing * ((i as f64) + 1.0);
                        pos.round().clamp(gate_top as f64, gate_bottom as f64) as i16
                    };
                    let direction = if is_left {
                        Direction::Right
                    } else {
                        Direction::Left
                    };
                    side_positions.push((Position { x: head_x, y }, direction));
                }

                side_positions
            };

        let team0_positions = positions_for_side(team_snakes[0].len(), TeamId(0), true);
        let team1_positions = positions_for_side(team_snakes[1].len(), TeamId(1), false);

        for (idx, pos) in team_snakes[0].iter().zip(team0_positions) {
            if *idx < positions.len() {
                positions[*idx] = Some(pos);
            }
        }
        for (idx, pos) in team_snakes[1].iter().zip(team1_positions) {
            if *idx < positions.len() {
                positions[*idx] = Some(pos);
            }
        }

        let fallback = self.calculate_starting_positions(self.players.len());
        positions
            .into_iter()
            .enumerate()
            .map(|(idx, pos)| {
                pos.unwrap_or_else(|| {
                    fallback
                        .get(idx)
                        .copied()
                        .unwrap_or((Position { x: 0, y: 0 }, Direction::Right))
                })
            })
            .collect()
    }

    fn apply_starting_positions(&mut self, player_count: usize) {
        let starting_positions = if matches!(self.game_type, GameType::TeamMatch { .. }) {
            self.calculate_team_starting_positions()
        } else {
            self.calculate_starting_positions(player_count)
        };

        let snake_length = self.starting_snake_length();

        for (snake_id, snake) in self.arena.snakes.iter_mut().enumerate() {
            if let Some((head_pos, direction)) = starting_positions.get(snake_id) {
                let tail_pos = match direction {
                    Direction::Left => Position {
                        x: head_pos.x + (snake_length - 1) as i16,
                        y: head_pos.y,
                    },
                    Direction::Right => Position {
                        x: head_pos.x - (snake_length - 1) as i16,
                        y: head_pos.y,
                    },
                    Direction::Up => Position {
                        x: head_pos.x,
                        y: head_pos.y + (snake_length - 1) as i16,
                    },
                    Direction::Down => Position {
                        x: head_pos.x,
                        y: head_pos.y - (snake_length - 1) as i16,
                    },
                };

                snake.body = vec![*head_pos, tail_pos];
                snake.direction = *direction;
            }
        }
    }

    fn starting_snake_length(&self) -> usize {
        match &self.game_type {
            GameType::Custom { settings } => settings.snake_start_length as usize,
            _ => DEFAULT_SNAKE_LENGTH,
        }
    }

    /// Food this snake would bank if it reached its own base right now.
    ///
    /// Each awarded combo point is one physical segment. New segments sit in
    /// `snake.food` until `step_forward` extrudes them past the starting
    /// length, so counting queued and extruded segments keeps the total stable
    /// across every growth tick. This is the one definition
    /// behind team scoring, AI base-return decisions, and the carried-food
    /// readout the client renders on each snake.
    pub fn carried_food(&self, snake: &Snake) -> u32 {
        let extra_segments = snake.length().saturating_sub(self.starting_snake_length());
        u32::try_from(extra_segments)
            .unwrap_or(u32::MAX)
            .saturating_add(snake.food)
    }

    fn respawn_event_for_snake(&self, snake_id: u32) -> Option<GameEvent> {
        let starting_positions = if matches!(self.game_type, GameType::TeamMatch { .. }) {
            self.calculate_team_starting_positions()
        } else {
            self.calculate_starting_positions(self.players.len())
        };
        let position_idx = snake_id as usize;

        let mut candidate_positions: Vec<(Position, Direction)> = Vec::new();
        if let Some(preferred) = starting_positions.get(position_idx) {
            candidate_positions.push(*preferred);
        }
        candidate_positions.extend(starting_positions);

        for (pos, dir) in candidate_positions {
            let occupied = self
                .arena
                .snakes
                .iter()
                .any(|s| s.is_alive && s.contains_point(&pos, false));
            if !occupied {
                return Some(GameEvent::SnakeRespawned {
                    snake_id,
                    position: pos,
                    direction: dir,
                });
            }
        }

        None
    }

    pub fn add_player_with_team(
        &mut self,
        user_id: u32,
        username: Option<String>,
        team_override: Option<TeamId>,
    ) -> Result<Player> {
        if self.players.contains_key(&user_id) {
            return Err(anyhow::anyhow!(
                "Player with user_id {} already exists",
                user_id
            ));
        }

        // Only rearrange players on tick 0
        if self.tick != 0 {
            return Err(anyhow::anyhow!(
                "Cannot add player after the game has started"
            ));
        }

        // Store username if provided
        if let Some(name) = username {
            self.usernames.insert(user_id, name);
        }

        // Determine team assignment for team games
        let team_id = match (&self.game_type, team_override) {
            (GameType::TeamMatch { .. }, Some(team)) => Some(team),
            (GameType::TeamMatch { .. }, None) => {
                // Assign teams alternately: A, B, A, B...
                let existing_player_count = self.players.len();
                let team_index = (existing_player_count % 2) as u8;
                Some(TeamId(team_index))
            }
            _ => None,
        };

        // Add new player first with temporary position
        let snake = Snake {
            body: vec![Position { x: 0, y: 0 }, Position { x: 0, y: 0 }],
            direction: Direction::Right,
            is_alive: true,
            food: 0,
            team_id,
            speed_milli: NORMAL_SNAKE_SPEED_MILLI,
            movement_credit: 0,
            // A mode whose tank never empties has no pickups, so a snake that
            // spawned dry could never boost at all.
            boost: SnakeBoost {
                charge_ms: self.unlimited_boost_capacity_ms().unwrap_or(0),
                ..Default::default()
            },
            combo: SnakeCombo::default(),
        };

        let snake_id = self.arena.add_snake(snake)?;
        let player = Player { user_id, snake_id };
        self.players.insert(user_id, player.clone());
        self.player_last_activity_ticks.insert(user_id, self.tick);

        // Calculate starting positions for all players
        let player_count = self.players.len();
        self.apply_starting_positions(player_count);

        Ok(player)
    }

    pub fn add_player(&mut self, user_id: u32, username: Option<String>) -> Result<Player> {
        self.add_player_with_team(user_id, username, None)
    }

    /// Record which skin a player is wearing.
    ///
    /// The caller is responsible for having checked the id against the
    /// catalogue — this is cosmetic state, and an unknown id renders as the
    /// classic look rather than failing, so the engine does not need to guard
    /// it. Passing `None` clears any previous choice.
    pub fn set_player_skin(&mut self, user_id: u32, skin_ref: Option<String>) {
        match skin_ref {
            Some(skin_ref) => {
                self.skins.insert(user_id, skin_ref);
            }
            None => {
                self.skins.remove(&user_id);
            }
        }
    }

    /// Record which base skin dresses one team's endzone.
    ///
    /// Like [`Self::set_player_skin`] this trusts its caller to have checked
    /// the id: an id no client recognises paints the endzone the way it always
    /// was, which is a cosmetic outcome and not a reason to fail anything.
    /// `None` clears the choice.
    pub fn set_team_base(&mut self, team_id: TeamId, base_ref: Option<String>) {
        match base_ref {
            Some(base_ref) => {
                self.team_bases.insert(team_id, base_ref);
            }
            None => {
                self.team_bases.remove(&team_id);
            }
        }
    }

    /// The base skin dressing one team's endzone, if it has one.
    pub fn team_base(&self, team_id: TeamId) -> Option<&str> {
        self.team_bases.get(&team_id).map(String::as_str)
    }

    /// Arm the pre-match readiness gate. Called once at match creation, before
    /// the state is ever published, so no caller can retro-fit a gate onto a
    /// running match.
    pub fn arm_readiness_gate(&mut self, deadline_ms: i64) {
        self.readiness = Some(MatchReadiness {
            deadline_ms,
            ready_user_ids: HashSet::new(),
        });
        self.simulation_epoch_ms = None;
    }

    /// Wall clock at which the simulation starts, or `None` while the
    /// readiness gate still holds the match. Both the authoritative executor
    /// and every client engine read the epoch through this one accessor, so a
    /// held match cannot advance on one side and not the other.
    pub fn simulation_start_ms(&self) -> Option<i64> {
        if self.readiness.is_some() {
            return None;
        }
        Some(self.simulation_epoch_ms.unwrap_or(self.start_ms))
    }

    /// Whether the match is still waiting on the readiness gate.
    pub fn is_awaiting_readiness(&self) -> bool {
        self.readiness.is_some()
    }

    /// Whether `user_id` has confirmed readiness. Always false once the gate
    /// has resolved — readiness is a pre-match concept and is not retained.
    pub fn is_user_ready(&self, user_id: u32) -> bool {
        self.readiness
            .as_ref()
            .is_some_and(|readiness| readiness.ready_user_ids.contains(&user_id))
    }

    /// Players who have not yet confirmed. Spectators are never included: they
    /// have no snake and must not be able to hold a match.
    pub fn players_pending_ready(&self) -> Vec<u32> {
        let Some(readiness) = self.readiness.as_ref() else {
            return Vec::new();
        };
        let mut pending: Vec<u32> = self
            .players
            .keys()
            .copied()
            .filter(|user_id| !readiness.ready_user_ids.contains(user_id))
            .collect();
        pending.sort_unstable();
        pending
    }

    /// Whether the gate may resolve now: everyone confirmed, the deadline
    /// lapsed, or (defensively) the match somehow has no players at all.
    pub fn readiness_gate_resolves_at(&self, now_ms: i64) -> bool {
        match self.readiness.as_ref() {
            None => false,
            Some(readiness) => {
                now_ms >= readiness.deadline_ms || self.players_pending_ready().is_empty()
            }
        }
    }

    /// Whether recording `user_id` as ready would change anything. Guards the
    /// executor against republishing an event for a duplicate delivery.
    pub fn accepts_ready_from(&self, user_id: u32) -> bool {
        self.readiness
            .as_ref()
            .is_some_and(|readiness| !readiness.ready_user_ids.contains(&user_id))
            && self.players.contains_key(&user_id)
    }

    pub fn add_spectator(&mut self, user_id: u32, username: Option<String>) {
        if let Some(name) = username {
            self.usernames.insert(user_id, name);
        }
        self.spectators.insert(user_id);
    }

    /// Spawns initial food items when the game starts
    pub fn spawn_initial_food(&mut self) {
        if self.rng.is_none() {
            return; // Can't spawn food without RNG
        }

        let target_food = self.properties.available_food_target;
        let mut attempts = 0;
        const MAX_ATTEMPTS: usize = 1000; // Prevent infinite loop

        while self.arena.food.len() < target_food && attempts < MAX_ATTEMPTS {
            attempts += 1;

            if let Some(rng) = &mut self.rng {
                let position = sample_food_position(rng, &self.game_type, &self.arena);

                // Check if position is valid (not occupied by food or snake)
                if !self.arena.food.contains(&position)
                    && !self.arena.is_boost_pad_position(&position)
                    && !self
                        .arena
                        .snakes
                        .iter()
                        .any(|s| s.is_alive && s.contains_point(&position, false))
                {
                    self.arena.food.push(position);
                }
            }
        }
    }

    pub fn schedule_command(&mut self, command_message: &GameCommandMessage) {
        // Only allow gameplay commands from active players; spectators should never drive snakes.
        if matches!(
            &command_message.command,
            GameCommand::Turn { .. }
                | GameCommand::ActivateBoost { .. }
                | GameCommand::DeactivateBoost { .. }
                | GameCommand::PlayerActivity { .. }
        ) {
            let issuing_user_id = command_message
                .command_id_server
                .as_ref()
                .map(|id| id.user_id)
                .unwrap_or(command_message.command_id_client.user_id);

            if !self.players.contains_key(&issuing_user_id)
                || self.is_player_idle_kicked(issuing_user_id)
            {
                return;
            }
        }

        self.record_player_activity_for_command(command_message, self.tick);
        self.command_queue.push(command_message.clone());
    }

    pub fn has_scheduled_commands(&self, tick: u32) -> bool {
        self.command_queue.has_commands_for_tick(tick)
    }

    pub fn join(&mut self, _user_id: u32) {}

    fn ensure_player_idle_tracking(&mut self) {
        // Active snapshots written before the inactivity policy existed have
        // no entries. Give those players a full grace period from the first
        // tick under the new executor rather than removing them immediately.
        let current_tick = self.tick;
        for user_id in self.players.keys().copied() {
            self.player_last_activity_ticks
                .entry(user_id)
                .or_insert(current_tick);
        }
    }

    fn player_is_idle_contender(&self, user_id: u32, player: &Player) -> bool {
        if self.is_player_idle_kicked(user_id) {
            return false;
        }
        if matches!(self.game_type, GameType::TeamMatch { .. }) || self.team_scores.is_some() {
            return true;
        }
        self.arena
            .snakes
            .get(player.snake_id as usize)
            .is_some_and(|snake| snake.is_alive)
    }

    fn inactivity_winning_snake_id(&self) -> Option<Option<u32>> {
        let active_players: Vec<(u32, &Player)> = self
            .players
            .iter()
            .filter(|(user_id, player)| self.player_is_idle_contender(**user_id, player))
            .map(|(user_id, player)| (*user_id, player))
            .collect();

        if matches!(self.game_type, GameType::TeamMatch { .. }) || self.team_scores.is_some() {
            let mut active_teams: Vec<TeamId> = active_players
                .iter()
                .filter_map(|(_, player)| {
                    self.arena
                        .snakes
                        .get(player.snake_id as usize)
                        .and_then(|snake| snake.team_id)
                })
                .collect();
            active_teams.sort_unstable();
            active_teams.dedup();
            return match active_teams.as_slice() {
                [] => Some(None),
                [winning_team] => {
                    let winning_snake_id = active_players
                        .iter()
                        .filter_map(|(user_id, player)| {
                            let is_winning_team = self
                                .arena
                                .snakes
                                .get(player.snake_id as usize)
                                .is_some_and(|snake| snake.team_id == Some(*winning_team));
                            is_winning_team.then_some((*user_id, player.snake_id))
                        })
                        .min_by_key(|(user_id, _)| *user_id)
                        .map(|(_, snake_id)| snake_id);
                    Some(winning_snake_id)
                }
                _ => None,
            };
        }

        match active_players.as_slice() {
            [] => Some(None),
            [(_, player)] => Some(Some(player.snake_id)),
            _ => None,
        }
    }

    fn inactivity_xp_awards(&self, winning_snake_id: Option<u32>) -> HashMap<u32, u32> {
        let winning_team = winning_snake_id.and_then(|snake_id| {
            self.arena
                .snakes
                .get(snake_id as usize)
                .and_then(|snake| snake.team_id)
        });
        self.players
            .iter()
            .map(|(user_id, player)| {
                if self.is_player_idle_kicked(*user_id) {
                    return (*user_id, 0);
                }
                let pickups = self
                    .food_pickups
                    .get(&player.snake_id)
                    .copied()
                    .unwrap_or(0);
                let player_team = self
                    .arena
                    .snakes
                    .get(player.snake_id as usize)
                    .and_then(|snake| snake.team_id);
                let won = winning_snake_id.is_some_and(|winner| {
                    player.snake_id == winner
                        || winning_team.is_some() && player_team == winning_team
                });
                (
                    *user_id,
                    pickups.saturating_mul(10) + if won { 50 } else { 10 },
                )
            })
            .collect()
    }

    /// Applies every deadline crossed on this simulation quantum as one
    /// deterministic batch. Evaluating the whole batch before selecting a
    /// winner is what makes simultaneous all-AFK expiry a draw rather than an
    /// iteration-order-dependent win.
    fn resolve_player_inactivity(
        &mut self,
        post_tick: u32,
        out: &mut Vec<(u64, GameEvent)>,
    ) -> bool {
        if !matches!(self.status, GameStatus::Started { .. }) || self.players.len() < 2 {
            return false;
        }

        self.ensure_player_idle_tracking();
        let tick_duration_ms = self.properties.tick_duration_ms.max(1);
        let timeout_ms = self.properties.player_idle_timeout_ms.max(tick_duration_ms);
        let mut expired: Vec<(u32, u32)> = self
            .players
            .iter()
            .filter(|(user_id, player)| self.player_is_idle_contender(**user_id, player))
            .filter_map(|(user_id, player)| {
                let last_tick = self
                    .player_last_activity_ticks
                    .get(user_id)
                    .copied()
                    .unwrap_or(self.tick);
                let idle_ms = post_tick
                    .saturating_sub(last_tick)
                    .saturating_mul(tick_duration_ms);
                (idle_ms >= timeout_ms).then_some((*user_id, player.snake_id))
            })
            .collect();
        expired.sort_unstable_by_key(|(user_id, _)| *user_id);
        if expired.is_empty() {
            return false;
        }

        for (user_id, snake_id) in expired {
            self.apply_event(GameEvent::PlayerIdleKicked { user_id, snake_id }, Some(out));
        }

        let Some(winning_snake_id) = self.inactivity_winning_snake_id() else {
            return false;
        };
        self.completed_by_inactivity = true;
        self.apply_event(
            GameEvent::XPAwarded {
                player_xp: self.inactivity_xp_awards(winning_snake_id),
            },
            Some(out),
        );
        self.apply_event(
            GameEvent::StatusUpdated {
                status: GameStatus::Complete { winning_snake_id },
            },
            Some(out),
        );
        true
    }

    pub fn tick_forward(&mut self, movement_only: bool) -> Result<Vec<(u64, GameEvent)>> {
        self.tick_forward_observing_boost(movement_only, &mut |_| {})
    }

    pub(crate) fn tick_forward_observing_boost(
        &mut self,
        movement_only: bool,
        observer: &mut impl FnMut(BoostLifecycleTransition),
    ) -> Result<Vec<(u64, GameEvent)>> {
        // Terminal state is immutable. `GameEngine` normally stops before
        // calling us again, but replicas/replays may receive late transport
        // messages and must not advance movement, fuel, cooldowns, or time.
        if self.is_complete() {
            return Ok(Vec::new());
        }

        let mut out: Vec<(u64, GameEvent)> = Vec::new();
        let tick_duration_ms = self.properties.tick_duration_ms.max(1);
        self.recent_crashes.retain(|crash| {
            self.tick
                .saturating_sub(crash.tick)
                .saturating_mul(tick_duration_ms)
                <= RECENT_CRASH_RETENTION_MS
        });
        self.recent_goals.retain(|goal| {
            self.tick
                .saturating_sub(goal.tick)
                .saturating_mul(tick_duration_ms)
                <= RECENT_GOAL_RETENTION_MS
        });

        // Emit snapshot on first tick
        if self.tick == 0 {
            self.event_sequence += 1;
            out.push((
                self.event_sequence,
                GameEvent::Snapshot {
                    game_state: self.clone(),
                },
            ));
        }

        // A cooldown ending at the post-step tick makes the packet available
        // for this quantum's movement and collection phase.
        let post_tick = self.tick.saturating_add(1);

        if self.resolve_player_inactivity(post_tick, &mut out) {
            self.tick = post_tick;
            return Ok(out);
        }

        // Drain before movement/collection so a pickup at this quantum's
        // boundary sees the precise time that remained when it was reached.
        // A pickup later below refills to the full configured window and that
        // fresh meter is therefore not charged for the quantum that earned it.
        for snake in &mut self.arena.snakes {
            snake.drain_combo(tick_duration_ms);
        }

        for pad in &mut self.arena.boost_pads {
            if pad
                .respawn_at_tick
                .is_some_and(|respawn_at_tick| respawn_at_tick <= post_tick)
            {
                pad.respawn_at_tick = None;
            }
        }

        // Boost start/stop commands are always resolved before movement credit
        // so a Space press or release can affect this quantum. Turns are held until the
        // generic mover set is known, then applied at most once per movement
        // step rather than once per global quantum.
        let mut due_turn_commands: Vec<GameCommandMessage> = Vec::new();
        while let Some(command_message) = self.command_queue.pop(self.tick) {
            match command_message.command {
                GameCommand::Turn { .. } => due_turn_commands.push(command_message),
                command => {
                    if let Ok(events) = self.exec_command(command) {
                        out.extend(events);
                    }
                }
            }
        }

        // Converge Boost toward each snake's latched intent. This runs every
        // quantum rather than only on the command edge, which is what makes a
        // held control robust: intent latched while the meter was empty (or
        // while the snake was dead) activates on the first quantum it becomes
        // affordable, with no second press.
        if let Some(config) = self
            .properties
            .boost
            .as_ref()
            .map(|config| (config.speed_milli, config.capacity_ms))
        {
            let (speed_milli, capacity_ms) = config;
            for snake_id in 0..self.arena.snakes.len() {
                match self.arena.snakes[snake_id].resolve_boost(speed_milli, capacity_ms) {
                    Some(BoostResolution::Activated) => {
                        observer(BoostLifecycleTransition::Activated {
                            snake_id: snake_id as u32,
                        });
                    }
                    Some(BoostResolution::Deactivated) => {
                        observer(BoostLifecycleTransition::ManuallyStopped {
                            snake_id: snake_id as u32,
                        });
                    }
                    None => {}
                }
            }
        }

        let unlimited_boost = self
            .properties
            .boost
            .as_ref()
            .is_some_and(|config| config.unlimited);
        for snake in &mut self.arena.snakes {
            snake.reserve_boost_quantum(unlimited_boost);
        }

        let normal_movement_interval_ms = self.normal_movement_interval_ms();
        let mut movers: HashSet<u32> = HashSet::new();
        for (snake_id, snake) in self.arena.snakes.iter_mut().enumerate() {
            if snake.accrue_movement_credit(tick_duration_ms, normal_movement_interval_ms) {
                movers.insert(snake_id as u32);
            }
        }

        let mut turned_snake_ids: HashSet<u32> = HashSet::new();
        let mut deferred_commands: Vec<GameCommandMessage> = Vec::new();
        for command_message in due_turn_commands {
            let GameCommand::Turn { snake_id, .. } = &command_message.command else {
                continue;
            };
            let snake_is_alive = self
                .arena
                .snakes
                .get(*snake_id as usize)
                .is_some_and(|snake| snake.is_alive);
            if snake_is_alive && (!movers.contains(snake_id) || turned_snake_ids.contains(snake_id))
            {
                deferred_commands.push(command_message);
                continue;
            }

            if let Ok(events) = self.exec_command(command_message.command) {
                for (_, event) in &events {
                    if let GameEvent::SnakeTurned { snake_id, .. } = event {
                        turned_snake_ids.insert(*snake_id);
                    }
                }
                out.extend(events);
            }
        }

        for mut command_message in deferred_commands {
            // Reschedule one tick later on whichever id `CommandQueue` orders
            // by (the server id when present). The original sequence number is
            // kept so a deferred command still executes ahead of newer inputs
            // that were scheduled for the same tick.
            let next_tick = self.tick + 1;
            match &mut command_message.command_id_server {
                Some(id) => id.tick = next_tick,
                None => command_message.command_id_client.tick = next_tick,
            }
            self.command_queue.push(command_message);
        }

        // Take a snapshot of the existing snakes to rollback dead ones after movement
        let old_snakes = self.arena.snakes.clone();

        // Move snakes
        for (snake_id, snake) in self.arena.snakes.iter_mut().enumerate() {
            if movers.contains(&(snake_id as u32)) {
                snake.step_forward()
            }
        }

        // Check for collisions
        let mut crashed_snakes: HashMap<u32, (Position, DeathCause)> = HashMap::new();
        let width = self.arena.width as i16;
        let height = self.arena.height as i16;
        'main_snake_loop: for (snake_id, snake) in self.iter_snakes() {
            let head = snake.head()?;
            if snake.is_alive && movers.contains(&snake_id) {
                // Check for wall collisions in team games
                if self.arena.is_wall_position(head) {
                    crashed_snakes.insert(snake_id, (*head, DeathCause::Wall));
                    continue 'main_snake_loop;
                }

                // Entering the enemy base kills the snake
                if let Some(team_id) = snake.team_id
                    && self.arena.is_in_enemy_base(head, team_id)
                {
                    crashed_snakes.insert(snake_id, (*head, DeathCause::EnemyBase));
                    continue 'main_snake_loop;
                }

                // If not within bounds
                if !(head.x >= 0 && head.x < width && head.y >= 0 && head.y < height) {
                    crashed_snakes.insert(snake_id, (*head, DeathCause::OutOfBounds));
                    continue 'main_snake_loop;
                }

                // If crashed with other snake
                for (other_snake_id, other_snake) in self.iter_snakes() {
                    let is_self = snake_id == other_snake_id;
                    if other_snake.is_alive && other_snake.contains_point(head, is_self) {
                        let cause = if is_self {
                            DeathCause::SelfCollision
                        } else if other_snake.is_head(head)
                            || (movers.contains(&other_snake_id)
                                && old_snakes[snake_id as usize].head()? == other_snake.head()?
                                && old_snakes[other_snake_id as usize].head()? == head)
                        {
                            // Equal destination cells and simultaneous head
                            // swaps are both head-to-head collisions. The
                            // latter appears as a body intersection after both
                            // snakes move, so compare their pre-move heads to
                            // avoid misattributing the trade as two body kills.
                            DeathCause::HeadToHead { other_snake_id }
                        } else {
                            DeathCause::SnakeBody {
                                killer_snake_id: other_snake_id,
                            }
                        };
                        crashed_snakes.insert(snake_id, (*head, cause));
                        continue 'main_snake_loop;
                    }
                }
            }
        }

        // Rollback and kill snakes that crashed.
        // Sorted order is required for determinism: HashSet iteration order
        // differs between processes (native server vs WASM client), and
        // respawn events consume RNG / emit events whose order must be
        // identical on both sides.
        let mut crashed_snakes: Vec<(u32, (Position, DeathCause))> =
            crashed_snakes.into_iter().collect();
        crashed_snakes.sort_unstable_by_key(|(snake_id, _)| *snake_id);
        for (snake_id, (attempted_head, cause)) in crashed_snakes {
            self.arena.snakes[snake_id as usize] = old_snakes[snake_id as usize].clone();
            let crash_position = Position {
                x: attempted_head.x.clamp(0, width.saturating_sub(1)),
                y: attempted_head.y.clamp(0, height.saturating_sub(1)),
            };
            self.recent_crashes.push(SnakeCrash {
                tick: self.tick.saturating_add(1),
                snake_id,
                position: crash_position,
                cause: cause.clone(),
            });
            self.apply_event(GameEvent::SnakeDied { snake_id, cause }, Some(&mut out));

            if let GameType::TeamMatch { .. } = &self.game_type
                && let Some(event) = self.respawn_event_for_snake(snake_id)
            {
                self.apply_event(event, Some(&mut out));
            }
        }

        // Eat food
        let mut food_eaten_events: Vec<GameEvent> = Vec::new();
        let mut claimed_food_positions: HashSet<Position> = HashSet::new();
        for (snake_id, snake) in self.iter_snakes() {
            let head = snake.head()?;
            if snake.is_alive
                && self.arena.food.contains(head)
                && claimed_food_positions.insert(*head)
            {
                let (points, combo_chain, combo_remaining_ms_before, boost_active) =
                    self.combo_pickup_context(snake);
                food_eaten_events.push(GameEvent::FoodEaten {
                    snake_id,
                    position: *head,
                    points,
                    combo_chain,
                    combo_remaining_ms_before,
                    boost_active,
                });
            }
        }
        for event in food_eaten_events {
            let GameEvent::FoodEaten { snake_id, .. } = &event else {
                unreachable!("food collection only creates FoodEaten events")
            };
            let snake_id = *snake_id;
            let old_score = self.scores.get(&snake_id).copied().unwrap_or(0);
            self.apply_event(event, Some(&mut out));
            let score = self.scores.get(&snake_id).copied().unwrap_or(0);
            if score != old_score {
                self.apply_event(GameEvent::ScoreUpdated { snake_id, score }, Some(&mut out));
            }
        }

        // Resolve packets after collision and food from stable pad/snake IDs.
        // Collection stores fuel only; activation remains an explicit command.
        if let Some(boost_config) = self.properties.boost.clone() {
            let mut available_pads: Vec<BoostPad> = self
                .arena
                .boost_pads
                .iter()
                .filter(|pad| pad.respawn_at_tick.is_none())
                .cloned()
                .collect();
            available_pads.sort_unstable_by_key(|pad| pad.id);

            let cooldown_ticks = boost_config.pad_respawn_ms / tick_duration_ms;
            for pad in available_pads {
                let collector = self
                    .iter_snakes()
                    .filter(|(_, snake)| {
                        snake.is_alive && snake.head().is_ok_and(|head| pad.contains(head))
                    })
                    .map(|(snake_id, _)| snake_id)
                    .min();

                let Some(snake_id) = collector else {
                    continue;
                };
                let charge_ms_after =
                    self.arena
                        .snakes
                        .get_mut(snake_id as usize)
                        .and_then(|snake| {
                            snake.collect_boost_charge(pad.charge_ms, boost_config.capacity_ms)
                        });
                if let Some(charge_ms_after) = charge_ms_after {
                    self.apply_event(
                        GameEvent::BoostPacketCollected {
                            pad_id: pad.id,
                            snake_id,
                            charge_ms_after,
                            respawn_at_tick: post_tick.saturating_add(cooldown_ticks),
                        },
                        Some(&mut out),
                    );
                }
            }
        }

        for (snake_id, snake) in self.arena.snakes.iter_mut().enumerate() {
            if snake.finalize_boost_depletion() {
                observer(BoostLifecycleTransition::Depleted {
                    snake_id: snake_id as u32,
                });
            }
        }

        // Spawn new food
        if !movement_only
            && self.food_refill_due()
            && self.arena.food.len() < self.properties.available_food_target
        {
            // The client will not have rng so it won't be able to spawn food.
            // This is by design as there's no reason for the client to spawn food.
            if let Some(rng) = &mut self.rng {
                let position = sample_food_position(rng, &self.game_type, &self.arena);

                if !self.arena.food.contains(&position)
                    && !self.arena.is_boost_pad_position(&position)
                    && !self
                        .arena
                        .snakes
                        .iter()
                        .any(|s| s.is_alive && s.contains_point(&position, false))
                {
                    self.apply_event(GameEvent::FoodSpawned { position }, Some(&mut out));
                }
            }
        }

        // Cosmetic goal cues are recorded here, in the movement path, rather
        // than inside the authoritative scoring block below. A client advances
        // its committed state with `movement_only` and receives the score and
        // respawn events from the transport, so a cue emitted only under the
        // scoring gate would vanish from committed state the moment it caught
        // up to the goal tick — retracting a celebration that really happened.
        // Recording it here mirrors `recent_crashes` and keeps the cue visible
        // to every prediction rebuild for its full retention window.
        //
        // The trigger is the entry edge (a snake that moved this step and whose
        // pre-step head was outside its own base) so the cue fires exactly once
        // per goal even where the follow-up respawn is applied from the
        // transport a few ticks later instead of being simulated locally.
        //
        // The edge is equivalent to the scoring block's "in own base carrying
        // food" test because carried food cannot change while a snake is inside
        // its base: `sample_food_position` never places food in an end zone
        // (pinned by `team_food_never_spawns_inside_an_end_zone`), and growth
        // moves segments between `food` and `length` without altering their
        // sum. A snake in its base with points to bank therefore always just
        // crossed the goal line.
        if matches!(&self.game_type, GameType::TeamMatch { .. }) && self.team_scores.is_some() {
            let goal_tick = self.tick.saturating_add(1);
            let mut goal_cues: Vec<TeamGoal> = Vec::new();

            for (snake_id, snake) in self.iter_snakes() {
                if !snake.is_alive || !movers.contains(&snake_id) {
                    continue;
                }
                let Some(team_id) = snake.team_id else {
                    continue;
                };
                let Ok(head) = snake.head() else {
                    continue;
                };
                if !self.arena.is_in_team_base(head, team_id) {
                    continue;
                }
                let entered_this_step = old_snakes
                    .get(snake_id as usize)
                    .and_then(|previous| previous.head().ok().copied())
                    .is_none_or(|previous_head| {
                        !self.arena.is_in_team_base(&previous_head, team_id)
                    });
                if !entered_this_step {
                    continue;
                }

                // Keep the cosmetic cue on the same single source of truth as
                // authoritative scoring and the snake's carried-food readout.
                let points = self.carried_food(snake);
                if points == 0 {
                    continue;
                }

                goal_cues.push(TeamGoal {
                    tick: goal_tick,
                    team_id,
                    snake_id,
                    position: *head,
                    points,
                });
            }

            self.recent_goals.extend(goal_cues);
        }

        // Calculate and update scores
        if !movement_only {
            // Track goal touches as simple score increments in team games
            if let GameType::TeamMatch { .. } = &self.game_type
                && self.team_scores.is_some()
            {
                let mut team_score_deltas: HashMap<TeamId, u32> = HashMap::new();
                let mut respawns: Vec<u32> = Vec::new();

                for (snake_id, snake) in self.iter_snakes() {
                    if !snake.is_alive {
                        continue;
                    }

                    let Some(team_id) = snake.team_id else {
                        continue;
                    };

                    let Ok(head) = snake.head() else {
                        continue;
                    };

                    if !self.arena.is_in_team_base(head, team_id) {
                        continue;
                    }

                    let carried_food = self.carried_food(snake);

                    if carried_food == 0 {
                        continue;
                    }

                    *team_score_deltas.entry(team_id).or_default() += carried_food;
                    respawns.push(snake_id);
                }

                let mut team_score_deltas: Vec<_> = team_score_deltas.into_iter().collect();
                team_score_deltas.sort_unstable_by_key(|(team_id, _)| *team_id);
                for (team_id, delta) in team_score_deltas {
                    let current_score = self
                        .team_scores
                        .as_ref()
                        .and_then(|scores| scores.get(&team_id).copied())
                        .unwrap_or(0);

                    self.apply_event(
                        GameEvent::TeamScoreUpdated {
                            team_id,
                            score: current_score + delta,
                        },
                        Some(&mut out),
                    );
                }

                for snake_id in respawns {
                    self.apply_event(
                        GameEvent::SnakeDied {
                            snake_id,
                            cause: DeathCause::Banked,
                        },
                        Some(&mut out),
                    );
                    if let Some(event) = self.respawn_event_for_snake(snake_id) {
                        self.apply_event(event, Some(&mut out));
                    }
                }
            }

            // Check completion conditions
            let alive_snakes: Vec<u32> = self
                .arena
                .snakes
                .iter()
                .enumerate()
                .filter(|(_, snake)| snake.is_alive)
                .map(|(idx, _)| idx as u32)
                .collect();

            if matches!(self.status, GameStatus::Started { .. }) {
                match &self.game_type {
                    GameType::TeamMatch { .. } => {
                        // The match runs until a team banks the target score.
                        // Scoring for this tick has already been applied above,
                        // so the comparison sees the crossing on the tick it
                        // happens. A single bank can carry several points and
                        // both teams can bank on the same tick, so the test is
                        // `>=` and the winner is the higher score among those
                        // that crossed — equal scores at the target are a draw.
                        if let Some(score_limit) = self.properties.score_limit {
                            let reached = self.team_scores.as_ref().is_some_and(|scores| {
                                scores.values().any(|score| *score >= score_limit)
                            });
                            if reached {
                                let winning_team = self.team_scores.as_ref().and_then(|scores| {
                                    let max_score = scores.values().copied().max()?;
                                    let mut leaders = scores
                                        .iter()
                                        .filter(|(_, score)| **score == max_score)
                                        .map(|(team_id, _)| *team_id);
                                    let leader = leaders.next()?;
                                    leaders.next().is_none().then_some(leader)
                                });

                                let winning_snake_id = winning_team
                                    .and_then(|team_id| {
                                        self.arena
                                            .snakes
                                            .iter()
                                            .enumerate()
                                            .find(|(_, snake)| {
                                                snake.team_id == Some(team_id) && snake.is_alive
                                            })
                                            .map(|(idx, _)| idx as u32)
                                    })
                                    .or_else(|| {
                                        winning_team.and_then(|team_id| {
                                            self.arena
                                                .snakes
                                                .iter()
                                                .enumerate()
                                                .find(|(_, snake)| snake.team_id == Some(team_id))
                                                .map(|(idx, _)| idx as u32)
                                        })
                                    });

                                let mut player_xp_awards = HashMap::new();
                                for (user_id, player) in &self.players {
                                    if self.is_player_idle_kicked(*user_id) {
                                        player_xp_awards.insert(*user_id, 0);
                                        continue;
                                    }

                                    let pickups = self
                                        .food_pickups
                                        .get(&player.snake_id)
                                        .copied()
                                        .unwrap_or(0);
                                    let snake = &self.arena.snakes[player.snake_id as usize];
                                    let is_winner = winning_team
                                        .is_some_and(|team| snake.team_id == Some(team));

                                    let base_xp = pickups.saturating_mul(10);
                                    let bonus_xp = if is_winner { 50 } else { 10 }; // Winner bonus or participation
                                    player_xp_awards.insert(*user_id, base_xp + bonus_xp);
                                }

                                self.apply_event(
                                    GameEvent::XPAwarded {
                                        player_xp: player_xp_awards,
                                    },
                                    Some(&mut out),
                                );

                                self.apply_event(
                                    GameEvent::StatusUpdated {
                                        status: GameStatus::Complete { winning_snake_id },
                                    },
                                    Some(&mut out),
                                );
                            }
                        }
                    }
                    _ => {
                        if alive_snakes.is_empty() {
                            let winning_snake_id = None;

                            let mut player_xp_awards = HashMap::new();
                            for (user_id, player) in &self.players {
                                if self.is_player_idle_kicked(*user_id) {
                                    player_xp_awards.insert(*user_id, 0);
                                    continue;
                                }

                                let pickups = self
                                    .food_pickups
                                    .get(&player.snake_id)
                                    .copied()
                                    .unwrap_or(0);
                                let base_xp = pickups.saturating_mul(10);
                                player_xp_awards.insert(*user_id, base_xp + 10);
                            }

                            self.apply_event(
                                GameEvent::XPAwarded {
                                    player_xp: player_xp_awards,
                                },
                                Some(&mut out),
                            );

                            self.apply_event(
                                GameEvent::StatusUpdated {
                                    status: GameStatus::Complete { winning_snake_id },
                                },
                                Some(&mut out),
                            );
                        }
                    }
                }
            }
        }

        // Increment tick
        self.tick += 1;

        Ok(out)
    }

    /// Latch a Boost request, counting it as a player action only when it
    /// actually changes what the player is asking for. Matches without Boost
    /// configured ignore the request outright; that is a fixed property of the
    /// match rather than a transient condition, so nothing is lost by it.
    fn set_boost_intent(&mut self, snake_id: u32, wants_boost: bool) {
        if self.properties.boost.is_none() {
            return;
        }

        let changed = self
            .arena
            .snakes
            .get_mut(snake_id as usize)
            .is_some_and(|snake| snake.set_boost_intent(wants_boost));
        if changed {
            self.record_player_action_for_snake(snake_id);
        }
    }

    fn exec_command(&mut self, command: GameCommand) -> Result<Vec<(u64, GameEvent)>> {
        // debug!("exec_command: Entering with command {:?}", command);
        // eprintln!("COMMON DEBUG: exec_command called with {:?}", command);
        let mut out: Vec<(u64, GameEvent)> = Vec::new();
        match command {
            GameCommand::Turn {
                snake_id,
                direction,
            } => {
                // debug!("exec_command: Processing Turn command - snake_id: {}, direction: {:?}", snake_id, direction);
                // eprintln!("COMMON DEBUG: Turn command - snake_id: {}, direction: {:?}", snake_id, direction);

                // Get current snake state
                let snake = self
                    .arena
                    .snakes
                    .get(snake_id as usize)
                    .context("Snake not found")?;

                // debug!("exec_command: Snake {} state - alive: {}, current_direction: {:?}, requested_direction: {:?}",
                //       snake_id, snake.is_alive, snake.direction, direction);
                // eprintln!("COMMON DEBUG: Snake {} - alive: {}, current: {:?}, requested: {:?}",
                //          snake_id, snake.is_alive, snake.direction, direction);

                if snake.is_alive && snake.direction != direction {
                    // debug!("exec_command: Snake is alive and direction is different");

                    // Always prevent 180-degree turns. Check both the pending
                    // direction and the direction the snake last actually
                    // moved: several commands can execute on one tick (late
                    // inputs get rebased onto the executor's current tick),
                    // and a second turn validated only against the first's
                    // pending direction would let the pair sum to a
                    // 180-degree reversal in a single movement step.
                    if snake.direction.is_opposite(&direction)
                        || snake.travel_direction().is_opposite(&direction)
                    {
                        // debug!("exec_command: Ignoring command - 180-degree turn attempted");
                        // eprintln!("COMMON DEBUG: Ignoring 180-degree turn");
                        // Ignore the command - cannot turn 180 degrees
                        return Ok(out);
                    }

                    // debug!("exec_command: Generating SnakeTurned event for snake {}", snake_id);
                    // eprintln!("COMMON DEBUG: Generating SnakeTurned event for snake {}", snake_id);
                    self.apply_event(
                        GameEvent::SnakeTurned {
                            snake_id,
                            direction,
                        },
                        Some(&mut out),
                    );
                    self.record_player_action_for_snake(snake_id);
                    // debug!("exec_command: SnakeTurned event applied successfully");
                } else {
                    if !snake.is_alive {
                        // debug!("exec_command: Ignoring command - snake {} is dead", snake_id);
                        // eprintln!("COMMON DEBUG: Ignoring - snake {} is dead", snake_id);
                    } else if snake.direction == direction {
                        // debug!("exec_command: Ignoring command - snake {} already facing {:?}", snake_id, direction);
                        // eprintln!("COMMON DEBUG: Ignoring - snake {} already facing {:?}", snake_id, direction);
                    }
                }
            }
            // Boost commands only latch what the player is asking for. They
            // deliberately do not consult fuel or liveness, so a request made
            // at an impossible moment is remembered rather than dropped;
            // `tick_forward_observing_boost` converges toward it every quantum.
            GameCommand::ActivateBoost { snake_id } => {
                self.set_boost_intent(snake_id, true);
            }
            GameCommand::DeactivateBoost { snake_id } => {
                self.set_boost_intent(snake_id, false);
            }
            GameCommand::PlayerActivity { .. } => {}
            GameCommand::UpdateStatus { .. } => {
                // debug!("exec_command: Processing UpdateStatus command");
            }
        }

        // debug!("exec_command: Returning {} events", out.len());
        // eprintln!("COMMON DEBUG: exec_command returning {} events", out.len());
        Ok(out)
    }

    pub fn apply_event(&mut self, event: GameEvent, out: Option<&mut Vec<(u64, GameEvent)>>) {
        if let Some(out) = out {
            self.event_sequence += 1;
            out.push((self.event_sequence, event.clone()));
        }

        match event {
            GameEvent::Snapshot { game_state } => {
                *self = game_state;
            }

            GameEvent::SnakeTurned {
                snake_id,
                direction,
            } => {
                if let Ok(snake) = self.get_snake_mut(snake_id) {
                    snake.direction = direction;
                }
            }

            GameEvent::SnakeDied { snake_id, cause } => {
                self.last_death_causes.insert(snake_id, cause);
                // A dead snake holds nothing — the invariants require a cleared
                // meter. An unlimited tank is refilled by the *respawn*, which
                // is where a new life actually begins.
                if let Ok(snake) = self.get_snake_mut(snake_id) {
                    snake.is_alive = false;
                    snake.reset_boost_and_movement(None);
                    snake.reset_combo();
                }
                self.command_queue
                    .discard_player_commands_for_snake(snake_id);
            }

            GameEvent::PlayerIdleKicked { user_id, snake_id } => {
                if let Err(index) = self.idle_kicked_user_ids.binary_search(&user_id) {
                    self.idle_kicked_user_ids.insert(index, user_id);
                }
                if let Ok(snake) = self.get_snake_mut(snake_id) {
                    snake.is_alive = false;
                    // `None`: an idle removal is permanent, so there is no new
                    // life to hand a full unlimited tank to.
                    snake.reset_boost_and_movement(None);
                    snake.reset_combo();
                    // Ordinary team deaths preserve a physically-held Boost
                    // intent for respawn. An idle removal is permanent, so no
                    // input latch may survive on the dead snake.
                    snake.boost = Default::default();
                }
                self.command_queue
                    .discard_player_commands_for_snake(snake_id);
            }

            GameEvent::FoodSpawned { position } => {
                if !self.has_food(&position) {
                    self.arena.food.push(position);
                }
            }

            GameEvent::FoodEaten {
                snake_id,
                position,
                points,
                combo_chain,
                combo_remaining_ms_before,
                ..
            } => {
                // Validate before removal: a malformed direct caller must not
                // consume a pellet and leave only half of the transition.
                let valid_context = self.combo_event_fields_are_bounded(
                    points,
                    combo_chain,
                    combo_remaining_ms_before,
                );
                if valid_context
                    && self.arena.snakes.get(snake_id as usize).is_some()
                    && self.remove_food(&position)
                {
                    let window_ms = self.properties.combo.window_ms;
                    if let Ok(snake) = self.get_snake_mut(snake_id) {
                        // One combo point is one visible segment of growth.
                        snake.food = snake.food.saturating_add(points);
                        snake.combo = SnakeCombo {
                            chain_count: combo_chain,
                            remaining_ms: window_ms,
                        };
                    }
                    let score = self.scores.entry(snake_id).or_default();
                    *score = score.saturating_add(points);
                    let pickups = self.food_pickups.entry(snake_id).or_default();
                    *pickups = pickups.saturating_add(1);
                }
            }

            GameEvent::BoostPacketCollected {
                pad_id,
                snake_id,
                charge_ms_after,
                respawn_at_tick,
            } => {
                if let Some(pad) = self
                    .arena
                    .boost_pads
                    .iter_mut()
                    .find(|pad| pad.id == pad_id)
                {
                    pad.respawn_at_tick = Some(respawn_at_tick);
                }

                if let Some(config) = &self.properties.boost
                    && charge_ms_after > 0
                    && charge_ms_after <= config.capacity_ms
                    && charge_ms_after.is_multiple_of(BOOST_TICK_INTERVAL_MS)
                    && let Some(snake) = self.arena.snakes.get_mut(snake_id as usize)
                {
                    snake.boost.charge_ms = charge_ms_after;
                }
            }

            GameEvent::CommandScheduled { command_message } => {
                self.record_player_activity_for_command(&command_message, self.tick);
                self.command_queue.push(command_message);
            }

            GameEvent::CommandScheduledV2 {
                command_message,
                deduplicated_replay,
                ..
            } => {
                if !deduplicated_replay {
                    self.record_player_activity_for_command(&command_message, self.tick);
                    self.command_queue.push(command_message);
                }
            }

            // Rejections are terminal protocol outcomes, not game-state
            // mutations. They are retained in the server recovery envelope.
            GameEvent::CommandRejected {
                command_id_client, ..
            } => {
                if let Some(command_id_client) = command_id_client {
                    self.command_queue
                        .discard_speculative_command(&command_id_client);
                }
            }

            GameEvent::StatusUpdated { status } => {
                self.status = status;
            }

            // Both readiness events are idempotent by construction: inserting
            // into a set and clearing an `Option`. A duplicate delivery from
            // the at-least-once transport therefore cannot corrupt the gate,
            // and a late one cannot reopen a match that already started.
            GameEvent::PlayerReady { user_id } => {
                if let Some(readiness) = self.readiness.as_mut() {
                    readiness.ready_user_ids.insert(user_id);
                }
            }

            GameEvent::MatchStartScheduled {
                simulation_epoch_ms,
            } => {
                if self.readiness.is_some() {
                    self.readiness = None;
                    self.simulation_epoch_ms = Some(simulation_epoch_ms);
                }
            }

            GameEvent::ScoreUpdated { snake_id, score } => {
                self.scores.insert(snake_id, score);
            }

            GameEvent::TeamScoreUpdated { team_id, score } => {
                if let Some(ref mut team_scores) = self.team_scores {
                    team_scores.insert(team_id, score);
                }
            }

            GameEvent::SnakeRespawned {
                snake_id,
                position,
                direction,
            } => {
                let snake_length = self.starting_snake_length() as i16;

                // Build compressed snake body: just head and tail for a straight snake
                let tail_pos = match direction {
                    Direction::Left => Position {
                        x: position.x + snake_length - 1,
                        y: position.y,
                    },
                    Direction::Right => Position {
                        x: position.x - snake_length + 1,
                        y: position.y,
                    },
                    Direction::Up => Position {
                        x: position.x,
                        y: position.y + snake_length - 1,
                    },
                    Direction::Down => Position {
                        x: position.x,
                        y: position.y - snake_length + 1,
                    },
                };

                // Now update the snake
                let refill = self.unlimited_boost_capacity_ms();
                if let Ok(snake) = self.get_snake_mut(snake_id) {
                    snake.body = vec![position, tail_pos];
                    snake.direction = direction;
                    snake.is_alive = true;
                    snake.food = 0;
                    snake.reset_boost_and_movement(refill);
                    snake.reset_combo();
                }
                self.command_queue
                    .discard_player_commands_for_snake(snake_id);
            }

            GameEvent::XPAwarded { player_xp } => {
                self.player_xp = player_xp;
            }

            // Pure observability signal; state is never mutated by it.
            GameEvent::TickHash { .. } => {}
        }
    }
}

#[cfg(test)]
mod readiness_tests {
    use super::*;

    const DEADLINE_MS: i64 = 10_000;

    fn gated_duel(player_count: u32) -> GameState {
        let mut state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            Some(7),
            1_000,
        );
        for user_id in 1..=player_count {
            state
                .add_player(user_id, Some(format!("player-{user_id}")))
                .expect("add player");
        }
        state.arm_readiness_gate(DEADLINE_MS);
        state
    }

    #[test]
    fn an_ungated_match_starts_off_its_immutable_start_ms() {
        let state = gated_duel(0);
        let mut ungated = state.clone();
        ungated.readiness = None;

        assert_eq!(ungated.simulation_start_ms(), Some(ungated.start_ms));
        assert!(!ungated.is_awaiting_readiness());
    }

    #[test]
    fn a_gated_match_has_no_simulation_epoch_until_the_gate_resolves() {
        let mut state = gated_duel(2);

        assert!(state.is_awaiting_readiness());
        assert_eq!(state.simulation_start_ms(), None);
        assert_eq!(state.players_pending_ready(), vec![1, 2]);

        state.apply_event(GameEvent::PlayerReady { user_id: 1 }, None);
        assert!(state.is_user_ready(1));
        assert_eq!(state.players_pending_ready(), vec![2]);
        assert_eq!(state.simulation_start_ms(), None);

        state.apply_event(GameEvent::PlayerReady { user_id: 2 }, None);
        assert!(state.players_pending_ready().is_empty());
        // Everyone has confirmed, but the epoch is stamped by the executor's
        // explicit MatchStartScheduled — never inferred by a replica.
        assert_eq!(state.simulation_start_ms(), None);

        state.apply_event(
            GameEvent::MatchStartScheduled {
                simulation_epoch_ms: 25_000,
            },
            None,
        );
        assert!(!state.is_awaiting_readiness());
        assert_eq!(state.simulation_start_ms(), Some(25_000));
    }

    #[test]
    fn a_gated_match_never_ticks_even_when_its_original_start_is_far_in_the_past() {
        let state = gated_duel(2);
        let mut engine = crate::GameEngine::new_from_state(7, state);

        let events = engine
            .run_until(1_000_000)
            .expect("a gated engine should remain runnable");

        assert!(events.is_empty());
        assert_eq!(engine.get_committed_state().tick, 0);
        assert!(engine.get_committed_state().is_awaiting_readiness());
    }

    /// `start_ms` is the durable runtime game identity: join authorization
    /// denies a game whose `start_ms` moved, and completion records key off
    /// it. Releasing the gate must therefore never touch it.
    #[test]
    fn resolving_the_gate_leaves_start_ms_untouched() {
        let mut state = gated_duel(1);
        let original_start_ms = state.start_ms;

        state.apply_event(GameEvent::PlayerReady { user_id: 1 }, None);
        state.apply_event(
            GameEvent::MatchStartScheduled {
                simulation_epoch_ms: original_start_ms + 60_000,
            },
            None,
        );

        assert_eq!(state.start_ms, original_start_ms);
        assert_eq!(
            state.simulation_start_ms(),
            Some(original_start_ms + 60_000)
        );
    }

    #[test]
    fn readiness_events_are_idempotent_under_at_least_once_delivery() {
        let mut state = gated_duel(2);

        state.apply_event(GameEvent::PlayerReady { user_id: 1 }, None);
        state.apply_event(GameEvent::PlayerReady { user_id: 1 }, None);
        assert_eq!(state.players_pending_ready(), vec![2]);
        assert!(!state.accepts_ready_from(1));
        assert!(state.accepts_ready_from(2));

        state.apply_event(GameEvent::PlayerReady { user_id: 2 }, None);
        state.apply_event(
            GameEvent::MatchStartScheduled {
                simulation_epoch_ms: 30_000,
            },
            None,
        );
        // A late duplicate must not reopen a match that already started.
        state.apply_event(
            GameEvent::MatchStartScheduled {
                simulation_epoch_ms: 90_000,
            },
            None,
        );
        state.apply_event(GameEvent::PlayerReady { user_id: 1 }, None);

        assert!(!state.is_awaiting_readiness());
        assert_eq!(state.simulation_start_ms(), Some(30_000));
    }

    #[test]
    fn the_deadline_resolves_the_gate_even_with_players_still_missing() {
        let state = gated_duel(2);

        assert!(!state.readiness_gate_resolves_at(DEADLINE_MS - 1));
        assert!(state.readiness_gate_resolves_at(DEADLINE_MS));
        assert!(state.readiness_gate_resolves_at(DEADLINE_MS + 5_000));
    }

    #[test]
    fn the_gate_resolves_early_once_every_player_confirms() {
        let mut state = gated_duel(2);

        state.apply_event(GameEvent::PlayerReady { user_id: 1 }, None);
        assert!(!state.readiness_gate_resolves_at(0));

        state.apply_event(GameEvent::PlayerReady { user_id: 2 }, None);
        assert!(state.readiness_gate_resolves_at(0));
    }

    /// Spectators have no snake. Letting one confirm — or, worse, counting one
    /// as pending — would either be meaningless or would hold every match
    /// containing a spectator until the deadline lapsed.
    #[test]
    fn spectators_neither_hold_nor_release_the_gate() {
        let mut state = gated_duel(1);
        state.add_spectator(99, Some("watcher".into()));

        assert_eq!(state.players_pending_ready(), vec![1]);
        assert!(!state.accepts_ready_from(99));

        state.apply_event(GameEvent::PlayerReady { user_id: 1 }, None);
        assert!(state.readiness_gate_resolves_at(0));
    }

    #[test]
    fn a_replicated_readiness_for_a_non_player_is_rejected() {
        let mut state = gated_duel(1);

        assert!(
            state
                .try_apply_replicated_event(GameEvent::PlayerReady { user_id: 1 })
                .is_ok()
        );
        let error = state
            .try_apply_replicated_event(GameEvent::PlayerReady { user_id: 4_242 })
            .expect_err("a non-player must not be able to pad the ready set");
        assert!(error.to_string().contains("not a player"));
        assert!(state.is_awaiting_readiness());
    }

    /// Games persisted before this protocol existed deserialize with no gate
    /// and must keep starting exactly as they did.
    #[test]
    fn states_without_readiness_fields_deserialize_ungated() {
        let mut state = gated_duel(1);
        state.readiness = None;
        let mut json: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&state).unwrap()).unwrap();
        let object = json.as_object_mut().unwrap();
        object.remove("readiness");
        object.remove("simulation_epoch_ms");

        let restored: GameState = serde_json::from_value(json).unwrap();
        assert!(!restored.is_awaiting_readiness());
        assert_eq!(restored.simulation_start_ms(), Some(restored.start_ms));
    }

    #[test]
    fn the_ready_set_serializes_in_a_stable_order() {
        let mut state = gated_duel(3);
        for user_id in [3, 1, 2] {
            state.apply_event(GameEvent::PlayerReady { user_id }, None);
        }

        let json = serde_json::to_string(&state.readiness).unwrap();
        assert!(
            json.contains("[1,2,3]"),
            "ready set must serialize sorted so replicas agree byte-for-byte: {json}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SnakeBoost;
    use std::collections::BinaryHeap;

    fn create_command_id(tick: u32, user_id: u32, seq: u32) -> CommandId {
        CommandId {
            tick,
            user_id,
            sequence_number: seq,
        }
    }

    fn create_command_message(
        tick: u32,
        user_id: u32,
        seq: u32,
        with_server_id: bool,
    ) -> GameCommandMessage {
        let client_id = create_command_id(tick, user_id, seq);
        let server_id = if with_server_id {
            Some(create_command_id(tick, user_id, seq))
        } else {
            None
        };

        GameCommandMessage {
            command_id_client: client_id,
            command_id_server: server_id,
            command: GameCommand::Turn {
                snake_id: 1,
                direction: Direction::Up,
            },
        }
    }

    fn assert_death_cause(events: &[(u64, GameEvent)], snake_id: u32, expected: DeathCause) {
        let actual = events.iter().find_map(|(_, event)| match event {
            GameEvent::SnakeDied {
                snake_id: dead_snake_id,
                cause,
            } if *dead_snake_id == snake_id => Some(cause.clone()),
            _ => None,
        });
        assert_eq!(
            actual,
            Some(expected),
            "missing or incorrectly attributed death for snake {snake_id}"
        );
    }

    #[test]
    fn snake_collides_with_itself_after_turning() {
        let mut game = GameState::new(
            10,
            10,
            GameType::FreeForAll { max_players: 1 },
            QueueMode::Quickmatch,
            None,
            0,
        );

        game.arena.snakes.push(Snake {
            body: vec![
                Position { x: 2, y: 2 },
                Position { x: 2, y: 3 },
                Position { x: 1, y: 3 },
                Position { x: 1, y: 2 },
                Position { x: 1, y: 1 },
            ],
            direction: Direction::Left,
            is_alive: true,
            food: 1,
            team_id: None,
            speed_milli: NORMAL_SNAKE_SPEED_MILLI,
            movement_credit: 0,
            boost: Default::default(),
            combo: Default::default(),
        });

        let events = game
            .tick_forward(true)
            .expect("tick_forward should succeed");

        assert!(
            events
                .iter()
                .any(|(_, event)| matches!(event, GameEvent::SnakeDied { snake_id: 0, .. })),
            "expected snake to die after colliding with itself"
        );
        assert_death_cause(&events, 0, DeathCause::SelfCollision);
        assert!(!game.arena.snakes[0].is_alive);
    }

    #[test]
    fn wall_crash_position_is_clamped_to_the_visible_arena() {
        let mut game = GameState::new(10, 10, GameType::Solo, QueueMode::Quickmatch, None, 0);
        game.arena.snakes.push(Snake {
            body: vec![Position { x: 0, y: 5 }, Position { x: 3, y: 5 }],
            direction: Direction::Left,
            is_alive: true,
            food: 0,
            team_id: None,
            speed_milli: NORMAL_SNAKE_SPEED_MILLI,
            movement_credit: 0,
            boost: Default::default(),
            combo: Default::default(),
        });

        // Solo runs a 50ms quantum, so the wall is reached on the quantum that
        // actually moves the snake rather than on the first one.
        let mut events = Vec::new();
        let crash_tick = loop {
            events.extend(
                game.tick_forward(true)
                    .expect("tick_forward should succeed"),
            );
            if !game.arena.snakes[0].is_alive {
                break game.current_tick();
            }
            assert!(game.current_tick() < 8, "snake should have hit the wall");
        };

        assert!(
            events
                .iter()
                .any(|(_, event)| matches!(event, GameEvent::SnakeDied { snake_id: 0, .. }))
        );
        assert_death_cause(&events, 0, DeathCause::OutOfBounds);
        assert_eq!(game.arena.snakes[0].body[0], Position { x: 0, y: 5 });
        assert_eq!(
            game.recent_crashes,
            vec![SnakeCrash {
                tick: crash_tick,
                snake_id: 0,
                position: Position { x: 0, y: 5 },
                cause: DeathCause::OutOfBounds,
            }]
        );

        // Retention is wall-clock, not tick-counted, so the boundary is derived
        // from the mode's own quantum and holds at either simulation rate.
        let retention_ticks = RECENT_CRASH_RETENTION_MS / game.properties.tick_duration_ms;
        while game.current_tick() <= crash_tick + retention_ticks {
            game.tick_forward(true).expect("advance crash history");
        }
        assert_eq!(
            game.recent_crashes.len(),
            1,
            "history must cover the animation and reconciliation window"
        );
        game.tick_forward(true).expect("prune crash history");
        assert!(
            game.recent_crashes.is_empty(),
            "crash history must remain bounded"
        );
    }

    #[test]
    fn snapshots_without_recent_crashes_remain_compatible() {
        let state = GameState::new(10, 10, GameType::Solo, QueueMode::Quickmatch, None, 0);
        let mut json = serde_json::to_value(state).expect("serialize state");
        json.as_object_mut()
            .expect("state object")
            .remove("recent_crashes");

        let restored: GameState = serde_json::from_value(json).expect("deserialize old snapshot");
        assert!(restored.recent_crashes.is_empty());
    }

    #[test]
    fn legacy_death_events_and_crash_cues_default_to_unknown_cause() {
        let event: GameEvent = serde_json::from_value(serde_json::json!({
            "SnakeDied": { "snake_id": 7 }
        }))
        .expect("deserialize legacy death event");
        assert!(matches!(
            event,
            GameEvent::SnakeDied {
                snake_id: 7,
                cause: DeathCause::Unknown,
            }
        ));

        let crash: SnakeCrash = serde_json::from_value(serde_json::json!({
            "tick": 9,
            "snake_id": 7,
            "position": { "x": 3, "y": 4 }
        }))
        .expect("deserialize legacy crash cue");
        assert_eq!(crash.cause, DeathCause::Unknown);
    }

    #[test]
    fn legacy_states_default_to_empty_last_death_history() {
        let state = GameState::new(40, 40, GameType::Solo, QueueMode::Quickmatch, None, 0);
        let mut json = serde_json::to_value(state).expect("serialize current state");
        json.as_object_mut()
            .expect("state object")
            .remove("last_death_causes");

        let restored: GameState =
            serde_json::from_value(json).expect("deserialize pre-attribution state");
        assert!(restored.last_death_causes.is_empty());
    }

    #[test]
    fn last_death_history_survives_respawn_and_banked_replaces_the_previous_life() {
        let mut game = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            Some(7),
            0,
        );
        let victim_id = game.add_player(1, Some("Victim".into())).unwrap().snake_id;
        let killer_id = game.add_player(2, Some("Killer".into())).unwrap().snake_id;
        let body_cause = DeathCause::SnakeBody {
            killer_snake_id: killer_id,
        };

        game.apply_event(
            GameEvent::SnakeDied {
                snake_id: victim_id,
                cause: body_cause.clone(),
            },
            None,
        );
        game.apply_event(
            GameEvent::SnakeRespawned {
                snake_id: victim_id,
                position: Position { x: 20, y: 20 },
                direction: Direction::Right,
            },
            None,
        );
        assert_eq!(game.last_death_causes.get(&victim_id), Some(&body_cause));

        game.apply_event(
            GameEvent::SnakeDied {
                snake_id: victim_id,
                cause: DeathCause::Banked,
            },
            None,
        );
        assert_eq!(
            game.last_death_causes.get(&victim_id),
            Some(&DeathCause::Banked),
            "the latest scoring reset must not expose an earlier life as the latest death"
        );

        let restored: GameState =
            serde_json::from_value(serde_json::to_value(&game).expect("serialize death history"))
                .expect("deserialize death history");
        assert_eq!(restored.last_death_causes, game.last_death_causes);
    }

    #[test]
    fn last_death_history_is_validated_but_excluded_from_the_sync_hash() {
        let mut baseline = GameState::new(
            40,
            40,
            GameType::FreeForAll { max_players: 2 },
            QueueMode::Quickmatch,
            Some(11),
            0,
        );
        baseline.add_player(1, Some("One".into())).unwrap();
        baseline.add_player(2, Some("Two".into())).unwrap();
        let mut with_history = baseline.clone();
        with_history
            .last_death_causes
            .insert(0, DeathCause::SnakeBody { killer_snake_id: 1 });

        with_history
            .validate_boost_invariants()
            .expect("valid attribution history");
        assert_eq!(baseline.sync_hash(), with_history.sync_hash());

        with_history.last_death_causes.insert(
            0,
            DeathCause::SnakeBody {
                killer_snake_id: 99,
            },
        );
        assert!(
            with_history
                .validate_boost_invariants()
                .unwrap_err()
                .to_string()
                .contains("missing attributed snake")
        );
    }

    fn clockwise(direction: Direction) -> Direction {
        match direction {
            Direction::Up => Direction::Right,
            Direction::Right => Direction::Down,
            Direction::Down => Direction::Left,
            Direction::Left => Direction::Up,
        }
    }

    fn step(position: Position, direction: Direction) -> Position {
        match direction {
            Direction::Up => Position {
                x: position.x,
                y: position.y - 1,
            },
            Direction::Down => Position {
                x: position.x,
                y: position.y + 1,
            },
            Direction::Left => Position {
                x: position.x - 1,
                y: position.y,
            },
            Direction::Right => Position {
                x: position.x + 1,
                y: position.y,
            },
        }
    }

    /// Two 90-degree turns can land on the same tick (inputs that arrive past
    /// the committed-lag window are rebased onto the executor's current tick).
    /// The snake moves once per tick, so executing both before that single
    /// movement step would sum them into a 180-degree reversal. Instead the
    /// engine applies the first turn this tick and defers the second to the
    /// next tick — the player's intended two-step maneuver plays out one tick
    /// stretched, and reversal stays structurally impossible.
    #[test]
    fn same_tick_double_turn_defers_second_turn_instead_of_reversing() {
        let mut game = GameState::new(20, 20, GameType::Solo, QueueMode::Quickmatch, None, 0);
        let player = game.add_player(1, None).expect("add player");
        let snake_id = player.snake_id;

        // Let the snake travel for a couple of ticks.
        advance_one_cell(&mut game);
        advance_one_cell(&mut game);

        let snake = &game.arena.snakes[snake_id as usize];
        let travel = snake.direction;
        let first_turn = clockwise(travel);
        let second_turn = clockwise(first_turn); // opposite of `travel`
        let head_before = *snake.head().expect("head");
        let length_before = snake.length();

        let tick = game.current_tick();
        for (seq, direction) in [(0, first_turn), (1, second_turn)] {
            let id = create_command_id(tick, 1, seq);
            game.schedule_command(&GameCommandMessage {
                command_id_client: id.clone(),
                command_id_server: Some(id),
                command: GameCommand::Turn {
                    snake_id,
                    direction,
                },
            });
        }

        // Tick 1: only the first turn applies; the second is deferred.
        advance_one_cell(&mut game);

        let snake = &game.arena.snakes[snake_id as usize];
        assert!(snake.is_alive, "snake must survive a same-tick double turn");
        assert_eq!(
            snake.direction, first_turn,
            "only the first turn may apply on the collapsed tick"
        );
        assert_eq!(
            *snake.head().expect("head"),
            step(head_before, first_turn),
            "snake must move in the first turn's direction, not reverse"
        );

        // Tick 2: the deferred second turn applies — intent preserved.
        advance_one_cell(&mut game);

        let snake = &game.arena.snakes[snake_id as usize];
        assert!(snake.is_alive, "snake must survive the deferred turn");
        assert_eq!(
            snake.direction, second_turn,
            "the deferred second turn must apply on the following tick"
        );
        assert_eq!(
            *snake.head().expect("head"),
            step(step(head_before, first_turn), second_turn),
            "the two-step maneuver must complete, one tick stretched"
        );
        assert_eq!(
            snake.length(),
            length_before,
            "the maneuver must not corrupt the body geometry"
        );
    }

    /// A burst of three turns on one tick executes one turn per tick, in
    /// input order, without ever reversing the snake.
    #[test]
    fn three_same_tick_turns_execute_in_order_over_consecutive_ticks() {
        let mut game = GameState::new(20, 20, GameType::Solo, QueueMode::Quickmatch, None, 0);
        let player = game.add_player(1, None).expect("add player");
        let snake_id = player.snake_id;

        advance_one_cell(&mut game);
        advance_one_cell(&mut game);

        let snake = &game.arena.snakes[snake_id as usize];
        let travel = snake.direction;
        let turns = [
            clockwise(travel),
            clockwise(clockwise(travel)),
            clockwise(clockwise(clockwise(travel))),
        ];
        let length_before = snake.length();
        let mut expected_head = *snake.head().expect("head");

        let tick = game.current_tick();
        for (seq, direction) in turns.iter().enumerate() {
            let id = create_command_id(tick, 1, seq as u32);
            game.schedule_command(&GameCommandMessage {
                command_id_client: id.clone(),
                command_id_server: Some(id),
                command: GameCommand::Turn {
                    snake_id,
                    direction: *direction,
                },
            });
        }

        for turn in turns {
            advance_one_cell(&mut game);
            expected_head = step(expected_head, turn);
            let snake = &game.arena.snakes[snake_id as usize];
            assert!(snake.is_alive, "snake must survive the maneuver");
            assert_eq!(snake.direction, turn, "turns must apply in input order");
            assert_eq!(*snake.head().expect("head"), expected_head);
        }

        assert_eq!(game.arena.snakes[snake_id as usize].length(), length_before);
    }

    /// A deferred turn landing on a tick that already has its own scheduled
    /// turn must execute FIRST: deferral keeps the command's original
    /// sequence number, and the queue orders same-tick commands by sequence,
    /// so input order is preserved — the tick's native command defers in
    /// turn. Shape: turns A and B on tick t, turn C on tick t+1 must apply
    /// as A@t, B@t+1, C@t+2.
    #[test]
    fn deferred_turn_executes_before_the_next_ticks_own_turn() {
        let mut game = GameState::new(20, 20, GameType::Solo, QueueMode::Quickmatch, None, 0);
        let player = game.add_player(1, None).expect("add player");
        let snake_id = player.snake_id;

        advance_one_cell(&mut game);
        advance_one_cell(&mut game);

        let snake = &game.arena.snakes[snake_id as usize];
        let travel = snake.direction;
        let turns = [
            clockwise(travel),
            clockwise(clockwise(travel)),
            clockwise(clockwise(clockwise(travel))),
        ];
        let mut expected_head = *snake.head().expect("head");

        // Turns A and B on tick t (server receive order 0, 1), turn C on
        // tick t+1 (receive order 2).
        let tick = game.current_tick();
        for (seq, (command_tick, direction)) in
            [(tick, turns[0]), (tick, turns[1]), (tick + 1, turns[2])]
                .into_iter()
                .enumerate()
        {
            let id = create_command_id(command_tick, 1, seq as u32);
            game.schedule_command(&GameCommandMessage {
                command_id_client: id.clone(),
                command_id_server: Some(id),
                command: GameCommand::Turn {
                    snake_id,
                    direction,
                },
            });
        }

        for turn in turns {
            advance_one_cell(&mut game);
            expected_head = step(expected_head, turn);
            let snake = &game.arena.snakes[snake_id as usize];
            assert!(snake.is_alive, "snake must survive the maneuver");
            assert_eq!(
                snake.direction, turn,
                "input order must be preserved across deferrals"
            );
            assert_eq!(*snake.head().expect("head"), expected_head);
        }
    }

    /// Advance exactly one cell of normal-speed travel, whatever the mode's
    /// simulation quantum is.
    ///
    /// Boost modes run a 50ms quantum but still move a snake every 100ms, so
    /// in tests about *turn* semantics a "step" means a movement, not a tick.
    /// At a 100ms quantum this is a single `tick_forward`, so the tests read
    /// identically in modes that have no Boost.
    fn advance_one_cell(game: &mut GameState) {
        let quanta = game
            .normal_movement_interval_ms()
            .div_ceil(game.properties.tick_duration_ms.max(1))
            .max(1);
        for _ in 0..quanta {
            game.tick_forward(true).expect("tick");
        }
    }

    /// A contradictory same-tick pair (left-turn then right-turn): the first
    /// applies, the deferred second is then opposite the snake's new travel
    /// direction and gets dropped by the 180-degree guard. First input wins;
    /// the snake never reverses.
    #[test]
    fn contradictory_same_tick_pair_keeps_first_turn() {
        let mut game = GameState::new(20, 20, GameType::Solo, QueueMode::Quickmatch, None, 0);
        let player = game.add_player(1, None).expect("add player");
        let snake_id = player.snake_id;

        advance_one_cell(&mut game);
        advance_one_cell(&mut game);

        let snake = &game.arena.snakes[snake_id as usize];
        let travel = snake.direction;
        let first_turn = clockwise(travel);
        let second_turn = clockwise(clockwise(first_turn)); // opposite of first_turn
        let head_before = *snake.head().expect("head");

        let tick = game.current_tick();
        for (seq, direction) in [(0, first_turn), (1, second_turn)] {
            let id = create_command_id(tick, 1, seq);
            game.schedule_command(&GameCommandMessage {
                command_id_client: id.clone(),
                command_id_server: Some(id),
                command: GameCommand::Turn {
                    snake_id,
                    direction,
                },
            });
        }

        advance_one_cell(&mut game);
        advance_one_cell(&mut game);

        let snake = &game.arena.snakes[snake_id as usize];
        assert!(snake.is_alive);
        assert_eq!(
            snake.direction, first_turn,
            "the deferred contradictory turn must be dropped, not applied"
        );
        assert_eq!(
            *snake.head().expect("head"),
            step(step(head_before, first_turn), first_turn),
            "snake must continue in the first turn's direction"
        );
    }

    /// An outright reversal request is invalid input: it is dropped on the
    /// spot, and — because it never applied — it must not consume the
    /// one-turn-per-tick slot of a valid turn behind it.
    #[test]
    fn rejected_reversal_does_not_consume_the_turn_slot() {
        let mut game = GameState::new(20, 20, GameType::Solo, QueueMode::Quickmatch, None, 0);
        let player = game.add_player(1, None).expect("add player");
        let snake_id = player.snake_id;

        advance_one_cell(&mut game);
        advance_one_cell(&mut game);

        let snake = &game.arena.snakes[snake_id as usize];
        let travel = snake.direction;
        let reversal = clockwise(clockwise(travel));
        let valid_turn = clockwise(travel);
        let head_before = *snake.head().expect("head");

        let tick = game.current_tick();
        for (seq, direction) in [(0, reversal), (1, valid_turn)] {
            let id = create_command_id(tick, 1, seq);
            game.schedule_command(&GameCommandMessage {
                command_id_client: id.clone(),
                command_id_server: Some(id),
                command: GameCommand::Turn {
                    snake_id,
                    direction,
                },
            });
        }

        advance_one_cell(&mut game);

        let snake = &game.arena.snakes[snake_id as usize];
        assert!(snake.is_alive);
        assert_eq!(
            snake.direction, valid_turn,
            "the valid turn must apply this tick; the dropped reversal must not defer it"
        );
        assert_eq!(*snake.head().expect("head"), step(head_before, valid_turn));
    }

    #[test]
    fn test_command_queue_basic_push_pop() {
        let mut queue = CommandQueue::new();

        // Push a command
        let cmd = create_command_message(10, 1, 1, false);
        queue.push(cmd.clone());

        // Pop should return the command
        let popped = queue.pop(10);
        assert!(popped.is_some());
        assert_eq!(popped.unwrap().command_id_client, cmd.command_id_client);

        // Queue should now be empty
        assert!(queue.pop(10).is_none());
    }

    #[test]
    fn test_command_queue_tick_ordering() {
        let mut queue = CommandQueue::new();

        // Push commands with different ticks
        // Note: sequence numbers should increase with tick to maintain consistent ordering
        let cmd1 = create_command_message(20, 1, 3, false);
        let cmd2 = create_command_message(10, 1, 1, false);
        let cmd3 = create_command_message(15, 1, 2, false);

        println!("cmd1 (tick 20): {:?}", cmd1.command_id_client);
        println!("cmd2 (tick 10): {:?}", cmd2.command_id_client);
        println!("cmd3 (tick 15): {:?}", cmd3.command_id_client);

        // Test comparisons
        println!("cmd2 < cmd1: {}", cmd2 < cmd1);
        println!("cmd2 < cmd3: {}", cmd2 < cmd3);
        println!("cmd3 < cmd1: {}", cmd3 < cmd1);

        use std::cmp::Reverse;
        println!(
            "Reverse(cmd2) > Reverse(cmd1): {}",
            Reverse(cmd2.clone()) > Reverse(cmd1.clone())
        );
        println!(
            "Reverse(cmd2) > Reverse(cmd3): {}",
            Reverse(cmd2.clone()) > Reverse(cmd3.clone())
        );
        println!(
            "Reverse(cmd3) > Reverse(cmd1): {}",
            Reverse(cmd3.clone()) > Reverse(cmd1.clone())
        );

        queue.push(cmd1);
        queue.push(cmd2);
        queue.push(cmd3);

        // Should pop in tick order (10, 15, 20)
        let cmd1 = queue.pop(25).unwrap();
        assert_eq!(cmd1.tick(), 10);

        let cmd2 = queue.pop(25).unwrap();
        assert_eq!(cmd2.tick(), 15);

        let cmd3 = queue.pop(25).unwrap();
        assert_eq!(cmd3.tick(), 20);
    }

    #[test]
    fn test_command_queue_max_tick_filtering() {
        let mut queue = CommandQueue::new();

        // Push commands with different ticks
        queue.push(create_command_message(10, 1, 1, false));
        queue.push(create_command_message(20, 1, 2, false));
        queue.push(create_command_message(30, 1, 3, false));

        // Pop with max_tick = 15 should only return tick 10
        let cmd1 = queue.pop(15);
        assert!(cmd1.is_some());
        assert_eq!(cmd1.unwrap().tick(), 10);

        // Pop again with max_tick = 15 should return None
        assert!(queue.pop(15).is_none());

        // Pop with max_tick = 25 should return tick 20
        let cmd2 = queue.pop(25);
        assert!(cmd2.is_some());
        assert_eq!(cmd2.unwrap().tick(), 20);
    }

    /// Two local commands stamped on the same tick (distinct client sequence
    /// numbers) plus a server confirmation of the first: the tombstone must
    /// hit only the matching local copy, leaving the second local command in
    /// the queue.
    #[test]
    fn tombstone_hits_only_the_matching_same_tick_local_command() {
        let mut queue = CommandQueue::new();

        queue.push(create_command_message(10, 1, 1, false)); // local copy A
        queue.push(create_command_message(10, 1, 2, false)); // local copy B

        // Server confirmation of A (same client id, server id attached).
        let mut server_a = create_command_message(10, 1, 1, false);
        server_a.command_id_server = Some(create_command_id(10, 1, 0));
        queue.push(server_a);

        // Pop order by effective id: the server copy (seq 0) first; local A
        // is tombstone-skipped; local B must survive.
        let first = queue.pop(10).expect("server copy");
        assert!(first.command_id_server.is_some());
        assert_eq!(first.command_id_client.sequence_number, 1);

        let second = queue.pop(10).expect("local copy B");
        assert!(second.command_id_server.is_none());
        assert_eq!(second.command_id_client.sequence_number, 2);

        assert!(queue.pop(10).is_none());
    }

    #[test]
    fn test_command_queue_tombstoning() {
        let mut queue = CommandQueue::new();

        // Push client command
        let client_cmd = create_command_message(10, 1, 1, false);
        queue.push(client_cmd.clone());
        assert!(queue.active_ids.contains(&client_cmd.command_id_client));
        assert!(queue.tombstone_ids.is_empty());

        // Push server command with same client_id - should tombstone the client command
        let mut server_cmd = client_cmd.clone();
        server_cmd.command_id_server = Some(create_command_id(10, 1, 1));
        queue.push(server_cmd.clone());
        assert!(queue.tombstone_ids.contains(&server_cmd.command_id_client));

        // Pop should return the server command (not the tombstoned client command)
        let popped = queue.pop(10).unwrap();
        assert!(popped.command_id_server.is_some());

        // Queue should now be empty (client command was tombstoned)
        assert!(queue.pop(10).is_none());
        assert!(queue.active_ids.is_empty());
        assert!(queue.tombstone_ids.is_empty());
    }

    #[test]
    fn test_command_queue_authoritative_commands_do_not_create_tombstones() {
        let mut queue = CommandQueue::new();

        for sequence_number in 1..=2048 {
            let mut command = create_command_message(10, 1, sequence_number, false);
            command.command_id_server = Some(create_command_id(10, 1, sequence_number));
            queue.push(command);
        }

        assert!(queue.active_ids.is_empty());
        assert!(queue.tombstone_ids.is_empty());

        let mut popped = 0;
        while queue.pop(10).is_some() {
            popped += 1;
        }
        assert_eq!(popped, 2048);
        assert!(queue.active_ids.is_empty());
        assert!(queue.tombstone_ids.is_empty());
    }

    #[test]
    fn test_command_queue_multiple_tombstoning() {
        let mut queue = CommandQueue::new();

        // Push multiple client commands
        queue.push(create_command_message(10, 1, 1, false));
        queue.push(create_command_message(10, 1, 2, false));
        queue.push(create_command_message(10, 1, 3, false));

        // Push server command that tombstones the second client command
        let mut server_cmd = create_command_message(10, 1, 2, false);
        server_cmd.command_id_server = Some(create_command_id(10, 1, 2));
        queue.push(server_cmd);

        // Pop should return commands in order, skipping the tombstoned one
        let cmd1 = queue.pop(10).unwrap();
        assert_eq!(cmd1.command_id_client.sequence_number, 1);
        assert!(cmd1.command_id_server.is_none());

        let cmd2 = queue.pop(10).unwrap();
        assert_eq!(cmd2.command_id_client.sequence_number, 2);
        assert!(cmd2.command_id_server.is_some());

        let cmd3 = queue.pop(10).unwrap();
        assert_eq!(cmd3.command_id_client.sequence_number, 3);
        assert!(cmd3.command_id_server.is_none());
    }

    #[test]
    fn test_command_queue_deduplication() {
        let mut queue = CommandQueue::new();

        // Push same command twice (should be deduped using active_ids)
        let cmd = create_command_message(10, 1, 1, false);
        queue.push(cmd.clone());
        queue.push(cmd.clone());

        // Should be able to pop twice (no deduplication implemented)
        assert!(queue.pop(10).is_some());
        assert!(queue.pop(10).is_some());
        assert!(queue.pop(10).is_none());
    }

    #[test]
    fn test_command_queue_same_tick_ordering() {
        let mut queue = CommandQueue::new();

        // Push commands with same tick but different users/sequences
        queue.push(create_command_message(10, 2, 1, false));
        queue.push(create_command_message(10, 1, 2, false));
        queue.push(create_command_message(10, 1, 1, false));

        // Should pop in order: (tick=10, user=1, seq=1), (tick=10, user=1, seq=2), (tick=10, user=2, seq=1)
        let cmd1 = queue.pop(10).unwrap();
        assert_eq!(cmd1.command_id_client.user_id, 1);
        assert_eq!(cmd1.command_id_client.sequence_number, 1);

        let cmd2 = queue.pop(10).unwrap();
        assert_eq!(cmd2.command_id_client.user_id, 1);
        assert_eq!(cmd2.command_id_client.sequence_number, 2);

        let cmd3 = queue.pop(10).unwrap();
        assert_eq!(cmd3.command_id_client.user_id, 2);
        assert_eq!(cmd3.command_id_client.sequence_number, 1);
    }

    #[test]
    fn test_command_queue_server_tick_override() {
        let mut queue = CommandQueue::new();

        // Create command with client tick 10 but server tick 15
        let mut cmd = create_command_message(10, 1, 1, false);
        cmd.command_id_server = Some(create_command_id(15, 1, 1));
        queue.push(cmd.clone());

        // Should not be available at tick 10
        assert!(queue.pop(10).is_none());

        // Should be available at tick 15
        let popped = queue.pop(15).unwrap();
        assert_eq!(popped.tick(), 15);
    }

    #[test]
    fn test_command_queue_empty_operations() {
        let mut queue = CommandQueue::new();

        // Pop from empty queue
        assert!(queue.pop(100).is_none());

        // Push and pop, then try again
        queue.push(create_command_message(10, 1, 1, false));
        assert!(queue.pop(10).is_some());
        assert!(queue.pop(10).is_none());
    }

    #[test]
    fn team_scores_when_returning_food_to_base() {
        let mut game = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            Some(4242),
            0,
        );

        game.add_player(1, Some("Player1".to_string()))
            .expect("add player 1");
        game.add_player(2, Some("Player2".to_string()))
            .expect("add player 2");

        {
            let snake = &mut game.arena.snakes[0];
            snake.body = vec![Position { x: 5, y: 10 }, Position { x: 2, y: 10 }];
            snake.direction = Direction::Right;
            snake.is_alive = true;
            snake.food = 1; // carrying one awarded point/segment
            snake.combo = SnakeCombo {
                chain_count: 2,
                remaining_ms: 500,
            };
        }

        let events = game.tick_forward(false).expect("tick_forward should work");

        let scored = events.iter().any(|(_, event)| {
            matches!(
                event,
                GameEvent::TeamScoreUpdated {
                    team_id,
                    score: 1
                } if *team_id == TeamId(0)
            )
        });
        assert!(scored, "team should score after returning food to base");

        let respawned = events.iter().any(|(_, event)| {
            matches!(event, GameEvent::SnakeRespawned { snake_id, .. } if *snake_id == 0)
        });
        assert!(respawned, "snake should respawn after scoring");

        let reset_without_crash = events
            .iter()
            .any(|(_, event)| matches!(event, GameEvent::SnakeDied { snake_id: 0, .. }));
        assert!(
            reset_without_crash && game.recent_crashes.is_empty(),
            "banking food should reset the snake without recording a collision"
        );
        assert_death_cause(&events, 0, DeathCause::Banked);

        let score = game
            .team_scores
            .as_ref()
            .and_then(|scores| scores.get(&TeamId(0)).copied())
            .unwrap_or(0);
        assert_eq!(score, 1, "team score should increment by carried food");

        let snake = &game.arena.snakes[0];
        assert!(snake.is_alive);
        assert_eq!(
            snake.food, 0,
            "snake should not keep carried food after respawn"
        );
        assert_eq!(
            snake.combo,
            SnakeCombo::default(),
            "banking starts a new combo life"
        );
    }

    /// A team snake one cell outside its own goal mouth, carrying `points`
    /// worth of food and travelling Left into the opening. Boost team games
    /// run a 50 ms quantum with a 100 ms movement interval, so the crossing
    /// step happens on the second quantum.
    fn game_approaching_own_goal(points: u32) -> GameState {
        let mut game = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        game.add_player(1, Some("Player1".to_string()))
            .expect("add player 1");
        game.add_player(2, Some("Player2".to_string()))
            .expect("add player 2");

        let snake = &mut game.arena.snakes[0];
        snake.body = vec![Position { x: 10, y: 18 }, Position { x: 13, y: 18 }];
        snake.direction = Direction::Left;
        snake.is_alive = true;
        snake.food = points;
        game
    }

    /// The cue must land on the cell the snake crossed into its base at (the
    /// goal mouth at the crossing row), not the middle of the goal, and must
    /// carry the exact points that goal was worth.
    #[test]
    fn scoring_records_a_goal_cue_at_the_crossing_cell() {
        let mut game = game_approaching_own_goal(3);
        // Start the team on a non-zero score so the cue's per-goal `points`
        // cannot be confused with `TeamScoreUpdated`'s cumulative total.
        game.team_scores
            .as_mut()
            .expect("team scores")
            .insert(TeamId(0), 5);

        let mut events = Vec::new();
        for _ in 0..2 {
            events.extend(game.tick_forward(false).expect("tick_forward should work"));
        }

        assert_eq!(
            game.recent_goals,
            vec![TeamGoal {
                tick: 2,
                team_id: TeamId(0),
                snake_id: 0,
                position: Position { x: 9, y: 18 },
                points: 3,
            }],
            "the cue must sit on the goal-mouth cell the snake crossed"
        );
        // The event carries the running total (5 + 3); the cue carries what
        // this one goal was worth, which is what the floating readout shows.
        assert!(
            events.iter().any(|(_, event)| matches!(
                event,
                GameEvent::TeamScoreUpdated { team_id, score: 8 } if *team_id == TeamId(0)
            )),
            "the cue's points must be this goal's delta, not the team total"
        );

        // The retention window covers the whole celebration and then releases.
        while game.current_tick() < 39 {
            game.tick_forward(false).expect("advance goal history");
        }
        assert_eq!(
            game.recent_goals.len(),
            1,
            "history must outlive the celebration and its reconciliation window"
        );
        game.tick_forward(false).expect("prune goal history");
        assert!(
            game.recent_goals.is_empty(),
            "goal history must remain bounded"
        );
    }

    /// A client advances its committed state with `movement_only` and applies
    /// the score and respawn from the transport. The cue therefore has to be
    /// produced by movement alone, exactly once, even though the snake keeps
    /// sitting in its own base until the server's respawn arrives.
    #[test]
    fn goal_cues_are_recorded_once_during_movement_only_catch_up() {
        let mut game = game_approaching_own_goal(1);

        for _ in 0..2 {
            game.tick_forward(true).expect("movement-only catch-up");
        }
        assert_eq!(
            game.recent_goals,
            vec![TeamGoal {
                tick: 2,
                team_id: TeamId(0),
                snake_id: 0,
                position: Position { x: 9, y: 18 },
                points: 1,
            }]
        );
        assert_eq!(
            game.team_scores
                .as_ref()
                .and_then(|scores| scores.get(&TeamId(0)).copied()),
            Some(0),
            "movement-only catch-up must leave scoring to the transport"
        );

        // Still alive, still carrying, still inside its base: no repeat cue.
        for _ in 0..8 {
            game.tick_forward(true).expect("stay inside the base");
        }
        assert!(game.arena.snakes[0].is_alive);
        assert!(
            game.arena
                .is_in_team_base(game.arena.snakes[0].head().expect("head"), TeamId(0))
        );
        assert_eq!(
            game.recent_goals.len(),
            1,
            "a snake lingering in its base must not restart the celebration"
        );
    }

    /// Load-bearing for the goal cue's entry-edge trigger: if food could spawn
    /// inside an end zone, a snake already parked in its own base could cross
    /// the scoring threshold without an entry edge, and the authoritative score
    /// would fire with no celebration.
    #[test]
    fn team_food_never_spawns_inside_an_end_zone() {
        let game = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            Some(31_337),
            0,
        );
        let mut rng = PseudoRandom::new(31_337);

        for _ in 0..5_000 {
            let position = sample_food_position(&mut rng, &game.game_type, &game.arena);
            assert!(
                !game.arena.is_in_team_base(&position, TeamId(0))
                    && !game.arena.is_in_team_base(&position, TeamId(1)),
                "food must stay on the main field, got {position:?}"
            );
        }
    }

    #[test]
    fn snapshots_without_recent_goals_remain_compatible() {
        let state = GameState::new(10, 10, GameType::Solo, QueueMode::Quickmatch, None, 0);
        let mut json = serde_json::to_value(state).expect("serialize state");
        json.as_object_mut()
            .expect("state object")
            .remove("recent_goals");

        let restored: GameState = serde_json::from_value(json).expect("deserialize old snapshot");
        assert!(restored.recent_goals.is_empty());
    }

    #[test]
    fn combo_config_is_snapshotted_for_every_game_mode() {
        let games = [
            GameState::new(40, 40, GameType::Solo, QueueMode::Quickmatch, None, 0),
            GameState::new(
                60,
                40,
                GameType::TeamMatch { per_team: 1 },
                QueueMode::Competitive,
                None,
                0,
            ),
            GameState::new(
                40,
                40,
                GameType::FreeForAll { max_players: 4 },
                QueueMode::Quickmatch,
                None,
                0,
            ),
            GameState::new(
                30,
                20,
                GameType::Custom {
                    settings: CustomGameSettings::default(),
                },
                QueueMode::Quickmatch,
                None,
                0,
            ),
        ];

        for game in games {
            assert_eq!(game.properties.combo, ComboConfig::default());
            game.properties
                .combo
                .validate()
                .expect("valid Combo config");
        }
    }

    #[test]
    fn combo_snapshot_validation_rejects_unsupported_or_denormalized_state() {
        let baseline = GameState::new(40, 40, GameType::Solo, QueueMode::Quickmatch, None, 0);

        let mut invalid = baseline.clone();
        invalid.properties.combo.max_food_value = 4;
        assert!(invalid.validate_boost_invariants().is_err());

        let mut invalid = baseline.clone();
        invalid.properties.combo.rules_version += 1;
        assert!(invalid.validate_boost_invariants().is_err());

        let mut invalid = baseline;
        invalid.add_player(1, None).expect("add player");
        invalid.arena.snakes[0].combo.chain_count = 1;
        assert!(invalid.validate_boost_invariants().is_err());
        invalid.arena.snakes[0].combo.remaining_ms = DEFAULT_COMBO_WINDOW_MS + 1;
        assert!(invalid.validate_boost_invariants().is_err());
    }

    #[test]
    fn combo_awards_one_one_two_three_then_caps_and_grows_by_awarded_points() {
        let mut game = GameState::new(40, 40, GameType::Solo, QueueMode::Quickmatch, None, 0);
        let player = game.add_player(1, None).expect("add player");
        let snake = &mut game.arena.snakes[player.snake_id as usize];
        snake.body = vec![Position { x: 5, y: 5 }, Position { x: 2, y: 5 }];
        snake.direction = Direction::Right;

        let mut food_events = Vec::new();
        for expected_points in [1, 1, 2, 3, 3] {
            let head = *game.arena.snakes[player.snake_id as usize]
                .head()
                .expect("head");
            game.arena.food = vec![Position {
                x: head.x + 1,
                y: head.y,
            }];
            for _ in 0..2 {
                food_events.extend(
                    game.tick_forward(true)
                        .expect("normal-speed pickup quantum"),
                );
            }

            let (_, event) = food_events
                .iter()
                .rev()
                .find(|(_, event)| matches!(event, GameEvent::FoodEaten { .. }))
                .expect("FoodEaten event");
            let GameEvent::FoodEaten {
                points,
                combo_chain,
                combo_remaining_ms_before,
                boost_active,
                ..
            } = event
            else {
                unreachable!()
            };
            assert_eq!(*points, expected_points);
            assert_eq!(
                *combo_chain,
                food_events
                    .iter()
                    .filter(|(_, event)| matches!(event, GameEvent::FoodEaten { .. }))
                    .count() as u32
            );
            assert_eq!(
                *combo_remaining_ms_before,
                if *combo_chain == 1 {
                    0
                } else {
                    DEFAULT_COMBO_WINDOW_MS - game.normal_movement_interval_ms()
                }
            );
            assert!(!boost_active);
        }

        let snake = &game.arena.snakes[player.snake_id as usize];
        let physical_growth =
            snake.length().saturating_sub(game.starting_snake_length()) as u32 + snake.food;
        assert_eq!(physical_growth, 10, "1 + 1 + 2 + 3 + 3 physical cells");
        assert_eq!(game.scores[&player.snake_id], 10);
        assert_eq!(game.food_pickups[&player.snake_id], 5);
        assert_eq!(snake.combo.chain_count, 5);
        assert_eq!(snake.combo.remaining_ms, DEFAULT_COMBO_WINDOW_MS);

        // Combo value affects score/growth, while XP remains one unit per
        // pellet (plus the existing winner bonus).
        assert_eq!(game.inactivity_xp_awards(Some(player.snake_id))[&1], 100);
    }

    #[test]
    fn combo_expires_after_exactly_one_second_of_authoritative_time() {
        assert_eq!(DEFAULT_COMBO_WINDOW_MS, 1_000);
        let mut game = GameState::new(40, 40, GameType::Solo, QueueMode::Quickmatch, None, 0);
        let player = game.add_player(1, None).expect("add player");
        let snake = &mut game.arena.snakes[player.snake_id as usize];
        snake.body = vec![Position { x: 5, y: 5 }, Position { x: 2, y: 5 }];
        snake.direction = Direction::Right;
        game.arena.food = vec![Position { x: 6, y: 5 }];
        game.tick_forward(true).expect("first half-step");
        game.tick_forward(true).expect("first pickup");
        assert_eq!(
            game.arena.snakes[player.snake_id as usize]
                .combo
                .chain_count,
            1
        );

        let tick_duration_ms = game.properties.tick_duration_ms;
        assert_eq!(DEFAULT_COMBO_WINDOW_MS % tick_duration_ms, 0);
        let expiry_quanta = DEFAULT_COMBO_WINDOW_MS / tick_duration_ms;
        for _ in 1..expiry_quanta {
            game.tick_forward(true).expect("Combo drain quantum");
        }
        assert_eq!(
            game.arena.snakes[player.snake_id as usize].combo,
            SnakeCombo {
                chain_count: 1,
                remaining_ms: tick_duration_ms,
            }
        );

        game.tick_forward(true).expect("final Combo drain quantum");
        assert_eq!(
            game.arena.snakes[player.snake_id as usize].combo,
            SnakeCombo::default()
        );

        let head = *game.arena.snakes[player.snake_id as usize]
            .head()
            .expect("head");
        game.arena.food = vec![Position {
            x: head.x + 1,
            y: head.y,
        }];
        game.tick_forward(true).expect("next half-step");
        let events = game.tick_forward(true).expect("post-expiry pickup");
        assert!(events.iter().any(|(_, event)| matches!(
            event,
            GameEvent::FoodEaten {
                points: 1,
                combo_chain: 1,
                combo_remaining_ms_before: 0,
                ..
            }
        )));
    }

    #[test]
    fn combo_state_stays_identical_in_authoritative_and_movement_only_simulation() {
        let mut authoritative =
            GameState::new(40, 40, GameType::Solo, QueueMode::Quickmatch, None, 0);
        let player = authoritative.add_player(1, None).expect("add player");
        let snake = &mut authoritative.arena.snakes[player.snake_id as usize];
        snake.body = vec![Position { x: 5, y: 5 }, Position { x: 2, y: 5 }];
        snake.direction = Direction::Right;
        let mut movement_only = authoritative.clone();

        for _ in 0..4 {
            let head = *authoritative.arena.snakes[player.snake_id as usize]
                .head()
                .expect("head");
            let food = Position {
                x: head.x + 1,
                y: head.y,
            };
            authoritative.arena.food = vec![food];
            movement_only.arena.food = vec![food];
            for _ in 0..2 {
                authoritative
                    .tick_forward(false)
                    .expect("authoritative quantum");
                movement_only
                    .tick_forward(true)
                    .expect("movement-only quantum");
            }
            assert_eq!(authoritative.sync_hash(), movement_only.sync_hash());
        }
    }

    #[test]
    fn one_pellet_can_only_produce_one_successful_food_event() {
        let mut game = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        game.add_player(1, None).expect("first player");
        game.add_player(2, None).expect("second player");
        for snake in &mut game.arena.snakes {
            snake.body = vec![Position { x: 20, y: 20 }, Position { x: 17, y: 20 }];
            snake.direction = Direction::Right;
            snake.movement_credit = 0;
        }
        game.arena.food = vec![Position { x: 20, y: 20 }];

        // At 50 ms neither normal-speed snake moves, so this deliberately
        // exercises simultaneous occupancy without collision resolution
        // selecting a winner first.
        let events = game.tick_forward(true).expect("contention quantum");
        let eaten: Vec<_> = events
            .iter()
            .filter(|(_, event)| matches!(event, GameEvent::FoodEaten { .. }))
            .collect();
        assert_eq!(eaten.len(), 1);
        assert!(matches!(
            &eaten[0].1,
            GameEvent::FoodEaten { snake_id: 0, .. }
        ));
        assert_eq!(game.scores.get(&0), Some(&1));
        assert_eq!(game.scores.get(&1), None);
        assert!(game.arena.food.is_empty());
    }

    #[test]
    fn death_and_respawn_clear_combo_but_not_cumulative_score_or_pickups() {
        let mut game = GameState::new(40, 40, GameType::Solo, QueueMode::Quickmatch, None, 0);
        let player = game.add_player(1, None).expect("add player");
        let snake = &mut game.arena.snakes[player.snake_id as usize];
        snake.combo = SnakeCombo {
            chain_count: 3,
            remaining_ms: 700,
        };
        game.scores.insert(player.snake_id, 6);
        game.food_pickups.insert(player.snake_id, 3);

        game.apply_event(
            GameEvent::SnakeDied {
                snake_id: player.snake_id,
                cause: DeathCause::Unknown,
            },
            None,
        );
        assert_eq!(
            game.arena.snakes[player.snake_id as usize].combo,
            SnakeCombo::default()
        );
        game.apply_event(
            GameEvent::SnakeRespawned {
                snake_id: player.snake_id,
                position: Position { x: 20, y: 20 },
                direction: Direction::Right,
            },
            None,
        );
        assert_eq!(
            game.arena.snakes[player.snake_id as usize].combo,
            SnakeCombo::default()
        );
        assert_eq!(game.scores[&player.snake_id], 6);
        assert_eq!(game.food_pickups[&player.snake_id], 3);
    }

    #[test]
    fn historical_combo_fields_default_to_current_config_and_inactive_state() {
        let mut game = GameState::new(40, 40, GameType::Solo, QueueMode::Quickmatch, None, 0);
        game.add_player(1, None).expect("add player");
        let mut json = serde_json::to_value(game).expect("serialize state");
        let state = json.as_object_mut().expect("state object");
        state.remove("food_pickups");
        state
            .get_mut("properties")
            .and_then(serde_json::Value::as_object_mut)
            .expect("properties")
            .remove("combo");
        for snake in state
            .get_mut("arena")
            .and_then(serde_json::Value::as_object_mut)
            .and_then(|arena| arena.get_mut("snakes"))
            .and_then(serde_json::Value::as_array_mut)
            .expect("snakes")
        {
            snake.as_object_mut().expect("snake").remove("combo");
        }

        let restored: GameState = serde_json::from_value(json).expect("deserialize old state");
        assert_eq!(restored.properties.combo, ComboConfig::default());
        assert!(restored.food_pickups.is_empty());
        assert!(
            restored
                .arena
                .snakes
                .iter()
                .all(|snake| snake.combo == SnakeCombo::default())
        );
    }

    #[test]
    fn historical_food_event_defaults_to_a_first_single_point_pickup() {
        let event: GameEvent = serde_json::from_value(serde_json::json!({
            "FoodEaten": {
                "snake_id": 7,
                "position": { "x": 3, "y": 4 }
            }
        }))
        .expect("deserialize historical FoodEaten");

        assert!(matches!(
            event,
            GameEvent::FoodEaten {
                snake_id: 7,
                points: 1,
                combo_chain: 1,
                combo_remaining_ms_before: 0,
                boost_active: false,
                ..
            }
        ));
    }

    /// `carried_food` is the one definition behind team scoring, the AI's
    /// base-return decision, and the number the client draws on each snake, so
    /// it is pinned directly rather than only through the scoring path.
    #[test]
    fn carried_food_counts_queued_and_extruded_growth_as_one_total() {
        let mut game = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            Some(4242),
            0,
        );
        game.add_player(1, Some("Player1".to_string()))
            .expect("add player 1");
        game.add_player(2, Some("Player2".to_string()))
            .expect("add player 2");

        let starting_length = game.starting_snake_length() as i16;
        let at_starting_length = |food: u32| {
            Snake::new(
                vec![
                    Position { x: 20, y: 10 },
                    Position {
                        x: 20 - (starting_length - 1),
                        y: 10,
                    },
                ],
                Direction::Right,
                true,
                food,
                Some(TeamId(0)),
            )
        };

        // One awarded point is one segment, and it does not matter whether it
        // is still queued in `food` or already extruded into the body: a snake
        // shows the same total across every tick it spends growing.
        assert_eq!(game.carried_food(&at_starting_length(0)), 0);
        assert_eq!(game.carried_food(&at_starting_length(1)), 1);
        assert_eq!(game.carried_food(&at_starting_length(2)), 2);
        assert_eq!(game.carried_food(&at_starting_length(5)), 5);

        let mut half_grown = at_starting_length(1);
        half_grown.body[1].x -= 1; // one segment already extruded
        assert_eq!(
            game.carried_food(&half_grown),
            2,
            "queued and extruded awarded segments must both be banked"
        );

        // A snake shorter than the starting length floors at zero instead of
        // underflowing; reachable in Custom games with a long start length.
        let stub = Snake::new(
            vec![Position { x: 20, y: 10 }, Position { x: 19, y: 10 }],
            Direction::Right,
            true,
            0,
            Some(TeamId(0)),
        );
        assert_eq!(game.carried_food(&stub), 0);
    }

    #[test]
    fn snake_dies_on_enemy_base_contact() {
        let mut game = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            Some(777),
            0,
        );

        game.add_player(1, Some("Player1".to_string()))
            .expect("add player 1");
        game.add_player(2, Some("Player2".to_string()))
            .expect("add player 2");

        let enemy_zone_start = game.arena.width as i16
            - game.arena.team_zone_config.as_ref().unwrap().end_zone_depth as i16;

        {
            let snake = &mut game.arena.snakes[0];
            snake.body = vec![
                Position {
                    x: enemy_zone_start + 1,
                    y: 15,
                },
                Position {
                    x: enemy_zone_start - 2,
                    y: 15,
                },
            ];
            snake.direction = Direction::Right;
            snake.is_alive = true;
        }

        let mut events = game.tick_forward(false).expect("first quantum should work");
        events.extend(
            game.tick_forward(false)
                .expect("second quantum should move the normal-speed snake"),
        );

        assert!(
            events.iter().any(|(_, event)| {
                matches!(event, GameEvent::SnakeDied { snake_id, .. } if *snake_id == 0)
            }),
            "snake should die when entering enemy base"
        );
        assert_death_cause(&events, 0, DeathCause::EnemyBase);

        assert!(
            game.arena.snakes[0].is_alive,
            "snake should respawn after dying in team games"
        );

        let score = game
            .team_scores
            .as_ref()
            .and_then(|scores| scores.get(&TeamId(0)).copied())
            .unwrap_or(0);
        assert_eq!(score, 0, "touching enemy base should not award points");
    }

    #[test]
    fn test_command_id_ordering() {
        // Test that CommandId ordering works correctly
        let id1 = create_command_id(10, 1, 1);
        let id2 = create_command_id(10, 1, 2);
        let id3 = create_command_id(10, 2, 1);
        let id4 = create_command_id(11, 1, 1);

        assert!(id1 < id2); // Same tick and user, lower sequence
        assert!(id1 < id3); // Same tick, lower user id
        assert!(id1 < id4); // Lower tick
    }

    #[test]
    fn test_binary_heap_with_reverse() {
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;

        // Create a heap
        let mut heap = BinaryHeap::new();

        // Push some numbers wrapped in Reverse
        heap.push(Reverse(20));
        heap.push(Reverse(10));
        heap.push(Reverse(15));

        // Pop should give us 10, 15, 20 (min-heap behavior)
        assert_eq!(heap.pop().unwrap().0, 10);
        assert_eq!(heap.pop().unwrap().0, 15);
        assert_eq!(heap.pop().unwrap().0, 20);
    }

    #[test]
    fn test_game_command_message_heap() {
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;

        // Create messages with different ticks but same user and sequence
        let msg1 = create_command_message(20, 1, 1, false);
        let msg2 = create_command_message(10, 1, 1, false);
        let msg3 = create_command_message(15, 1, 1, false);

        // Test direct comparison
        println!("msg1: {:?}", msg1);
        println!("msg2: {:?}", msg2);
        println!("msg1.command_id_client: {:?}", msg1.command_id_client);
        println!("msg2.command_id_client: {:?}", msg2.command_id_client);
        println!("msg2 (tick 10) < msg1 (tick 20): {}", msg2 < msg1);
        println!("msg2.cmp(&msg1): {:?}", msg2.cmp(&msg1));
        println!(
            "msg2.command_id_client.cmp(&msg1.command_id_client): {:?}",
            msg2.command_id_client.cmp(&msg1.command_id_client)
        );

        // Push to heap
        let mut heap = BinaryHeap::new();
        heap.push(Reverse(msg1.clone()));
        heap.push(Reverse(msg2.clone()));
        heap.push(Reverse(msg3.clone()));

        // Pop and check order
        let first = heap.pop().unwrap().0;
        println!("First popped: tick = {}", first.tick());
        assert_eq!(first.tick(), 10);

        let second = heap.pop().unwrap().0;
        println!("Second popped: tick = {}", second.tick());
        assert_eq!(second.tick(), 15);

        let third = heap.pop().unwrap().0;
        println!("Third popped: tick = {}", third.tick());
        assert_eq!(third.tick(), 20);
    }

    #[test]
    fn test_simple_message_comparison() {
        // Create two messages with different ticks but same user and sequence
        let msg_tick10 = create_command_message(10, 1, 1, false);
        let msg_tick20 = create_command_message(20, 1, 1, false);

        // Also check if they're actually different
        println!("msg_tick10 == msg_tick20: {}", msg_tick10 == msg_tick20);

        // Print debug info
        println!(
            "msg_tick10.command_id_client: {:?}",
            msg_tick10.command_id_client
        );
        println!(
            "msg_tick20.command_id_client: {:?}",
            msg_tick20.command_id_client
        );
        println!("msg_tick10.id(): {:?}", msg_tick10.id());
        println!("msg_tick20.id(): {:?}", msg_tick20.id());
        println!(
            "msg_tick10.cmp(&msg_tick20): {:?}",
            msg_tick10.cmp(&msg_tick20)
        );
        println!(
            "msg_tick10.command_id_client.cmp(&msg_tick20.command_id_client): {:?}",
            msg_tick10
                .command_id_client
                .cmp(&msg_tick20.command_id_client)
        );

        // Check the actual comparison being used in Ord
        let id1 = msg_tick10
            .command_id_server
            .as_ref()
            .unwrap_or(&msg_tick10.command_id_client);
        let id2 = msg_tick20
            .command_id_server
            .as_ref()
            .unwrap_or(&msg_tick20.command_id_client);
        println!("id1: {:?}", id1);
        println!("id2: {:?}", id2);
        println!("id1.cmp(id2): {:?}", id1.cmp(id2));

        // Let's manually implement what Ord::cmp should do
        let manual_cmp = msg_tick10
            .command_id_server
            .as_ref()
            .unwrap_or(&msg_tick10.command_id_client)
            .cmp(
                msg_tick20
                    .command_id_server
                    .as_ref()
                    .unwrap_or(&msg_tick20.command_id_client),
            );
        println!("Manual cmp result: {:?}", manual_cmp);

        // Check Ord trait directly
        use std::cmp::Ord;
        println!("Ord::cmp result: {:?}", Ord::cmp(&msg_tick10, &msg_tick20));

        // This test will show us what's actually happening
        if msg_tick10 < msg_tick20 {
            println!("tick 10 < tick 20 (expected behavior)");
        } else if msg_tick10 > msg_tick20 {
            println!("tick 10 > tick 20 (inverted behavior!)");
        } else {
            println!("tick 10 == tick 20 (they're equal?!)");
        }
    }

    #[test]
    fn test_game_command_message_ordering() {
        // Test GameCommandMessage ordering directly
        // Note: Using different sequence numbers to avoid identical commands
        let msg1 = create_command_message(10, 1, 1, false);
        let msg2 = create_command_message(20, 1, 2, false);
        let msg3 = create_command_message(15, 1, 3, false);

        // Debug: Let's see what's actually happening
        println!("msg1 (tick 10) < msg2 (tick 20): {}", msg1 < msg2);
        println!("msg1 (tick 10) > msg2 (tick 20): {}", msg1 > msg2);
        println!("msg1.cmp(&msg2): {:?}", msg1.cmp(&msg2));

        // Direct comparison - smaller ticks should be less than larger ticks
        assert!(msg1 < msg2); // tick 10 < tick 20
        assert!(msg1 < msg3); // tick 10 < tick 15
        assert!(msg3 < msg2); // tick 15 < tick 20

        // Test with server IDs
        let mut msg_with_server = create_command_message(10, 1, 1, false);
        msg_with_server.command_id_server = Some(create_command_id(25, 1, 1));

        assert!(msg2 < msg_with_server); // tick 20 < tick 25 (server tick overrides)
    }

    #[test]
    fn test_reverse_game_command_message_ordering() {
        use std::cmp::Reverse;

        // Test GameCommandMessage ordering when wrapped in Reverse
        // Note: Using different sequence numbers to avoid identical commands
        let msg1 = create_command_message(10, 1, 1, false);
        let msg2 = create_command_message(20, 1, 2, false);
        let msg3 = create_command_message(15, 1, 3, false);

        // Wrap in Reverse
        let rev1 = Reverse(msg1.clone());
        let rev2 = Reverse(msg2.clone());
        let rev3 = Reverse(msg3.clone());

        // Reversed comparison - larger ticks should be "less than" when wrapped in Reverse
        assert!(rev2 < rev1); // Reverse(tick 20) < Reverse(tick 10)
        assert!(rev3 < rev1); // Reverse(tick 15) < Reverse(tick 10)
        assert!(rev2 < rev3); // Reverse(tick 20) < Reverse(tick 15)

        // Test in a BinaryHeap to see actual behavior
        let mut heap = BinaryHeap::new();
        heap.push(Reverse(msg2.clone()));
        heap.push(Reverse(msg1.clone()));
        heap.push(Reverse(msg3.clone()));

        // Pop should give us the smallest tick first (min-heap behavior)
        let first = heap.pop().unwrap().0;
        assert_eq!(first.tick(), 10);

        let second = heap.pop().unwrap().0;
        assert_eq!(second.tick(), 15);

        let third = heap.pop().unwrap().0;
        assert_eq!(third.tick(), 20);
    }

    #[test]
    fn game_state_hash_sets_serialize_in_stable_order() {
        let mut state = GameState::new(10, 10, GameType::Solo, QueueMode::Quickmatch, Some(1), 0);
        state.spectators.extend([9, 2, 5]);

        for sequence_number in [7, 3] {
            let client_command = create_command_message(10, 1, sequence_number, false);
            state.command_queue.push(client_command.clone());
            let mut server_command = client_command;
            server_command.command_id_server = Some(create_command_id(10, 1, sequence_number));
            state.command_queue.push(server_command);
        }

        let json = serde_json::to_value(&state).unwrap();
        assert_eq!(json["spectators"], serde_json::json!([2, 5, 9]));

        let active_sequences: Vec<u64> = json["command_queue"]["active_ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|id| id["sequence_number"].as_u64().unwrap())
            .collect();
        assert_eq!(active_sequences, vec![3, 7]);

        let tombstone_sequences: Vec<u64> = json["command_queue"]["tombstone_ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|id| id["sequence_number"].as_u64().unwrap())
            .collect();
        assert_eq!(tombstone_sequences, vec![3, 7]);

        let round_trip: GameState = serde_json::from_value(json).unwrap();
        let round_trip_json = serde_json::to_value(round_trip).unwrap();
        assert_eq!(round_trip_json["spectators"], serde_json::json!([2, 5, 9]));
        assert_eq!(
            round_trip_json["command_queue"]["active_ids"],
            serde_json::json!([
                {"tick": 10, "user_id": 1, "sequence_number": 3},
                {"tick": 10, "user_id": 1, "sequence_number": 7}
            ])
        );
        assert_eq!(
            round_trip_json["command_queue"]["tombstone_ids"],
            serde_json::json!([
                {"tick": 10, "user_id": 1, "sequence_number": 3},
                {"tick": 10, "user_id": 1, "sequence_number": 7}
            ])
        );
    }

    fn boost_test_game_with_speed(player_count: u32, speed_milli: u16) -> GameState {
        let mut game = GameState::new_with_boost_config(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            Some(1234),
            0,
            BoostConfig {
                speed_milli,
                ..BoostConfig::default()
            },
        )
        .expect("valid Boost test game");
        for user_id in 1..=player_count {
            game.add_player(user_id, Some(format!("Player{user_id}")))
                .expect("add Boost test player");
        }
        game
    }

    fn boost_test_game(player_count: u32) -> GameState {
        boost_test_game_with_speed(player_count, DEFAULT_BOOST_SPEED_MILLI)
    }

    fn started_inactivity_team_game(per_team: u8) -> GameState {
        let mut game = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team },
            QueueMode::Quickmatch,
            Some(1234),
            0,
        );
        for user_id in 1..=u32::from(per_team) * 2 {
            game.add_player(user_id, Some(format!("Player{user_id}")))
                .expect("add inactivity test player");
        }
        game.status = GameStatus::Started { server_id: 7 };
        // No RNG is the only food suppression these tests need, and the only
        // one available: the mode's food target is a validated invariant, so
        // zeroing it would make every snapshot round trip fail admission.
        game.rng = None;

        // Pin the generic/legacy fallback as well as the lower-level behavior.
        assert_eq!(game.properties.player_idle_timeout_ms, 60_000);
        assert_eq!(game.properties.player_idle_warning_ms, 10_000);
        game
    }

    fn started_inactivity_ffa_game(player_count: u8) -> GameState {
        let mut game = GameState::new(
            40,
            40,
            GameType::FreeForAll {
                max_players: player_count,
            },
            QueueMode::Quickmatch,
            Some(4321),
            0,
        );
        for user_id in 1..=u32::from(player_count) {
            game.add_player(user_id, Some(format!("Player{user_id}")))
                .expect("add FFA inactivity test player");
        }
        game.status = GameStatus::Started { server_id: 7 };
        // See `started_inactivity_team_game`: no RNG, mode-default food target.
        game.rng = None;
        game
    }

    fn inactivity_timeout_ticks(game: &GameState) -> u32 {
        assert_eq!(
            game.properties.player_idle_timeout_ms % game.properties.tick_duration_ms,
            0,
            "the inactivity deadline must land on an authoritative quantum"
        );
        game.properties.player_idle_timeout_ms / game.properties.tick_duration_ms
    }

    fn schedule_inactivity_test_command(
        game: &mut GameState,
        user_id: u32,
        sequence_number: u32,
        command: GameCommand,
    ) {
        let tick = game.tick;
        game.schedule_command(&GameCommandMessage {
            command_id_client: create_command_id(tick, user_id, sequence_number),
            command_id_server: Some(create_command_id(tick, user_id, sequence_number)),
            command,
        });
    }

    fn make_active_mover(snake: &mut Snake, speed_milli: u16) {
        snake.boost.intent = true;
        snake.boost.active = true;
        snake.boost.charge_ms = 1_000;
        snake.speed_milli = speed_milli;
        snake.movement_credit = 0;
    }

    fn schedule_activation(game: &mut GameState, user_id: u32, snake_id: u32, tick: u32, seq: u32) {
        game.schedule_command(&GameCommandMessage {
            command_id_client: CommandId {
                tick,
                user_id,
                sequence_number: seq,
            },
            command_id_server: Some(CommandId {
                tick,
                user_id,
                sequence_number: seq,
            }),
            command: GameCommand::ActivateBoost { snake_id },
        });
    }

    fn schedule_deactivation(
        game: &mut GameState,
        user_id: u32,
        snake_id: u32,
        tick: u32,
        seq: u32,
    ) {
        game.schedule_command(&GameCommandMessage {
            command_id_client: CommandId {
                tick,
                user_id,
                sequence_number: seq,
            },
            command_id_server: Some(CommandId {
                tick,
                user_id,
                sequence_number: seq,
            }),
            command: GameCommand::DeactivateBoost { snake_id },
        });
    }

    #[test]
    fn accepted_player_actions_are_counted_once_at_the_gameplay_effect() {
        let mut solo = GameState::new(30, 30, GameType::Solo, QueueMode::Quickmatch, None, 0);
        let player = solo.add_player(41, None).expect("add solo player");
        let snake_id = player.snake_id;
        let legal_turn = clockwise(solo.arena.snakes[snake_id as usize].direction);

        let turn_events = solo
            .exec_command(GameCommand::Turn {
                snake_id,
                direction: legal_turn,
            })
            .expect("execute legal turn");
        assert!(turn_events.iter().any(|(_, event)| matches!(
            event,
            GameEvent::SnakeTurned {
                snake_id: event_snake_id,
                direction
            } if *event_snake_id == snake_id && *direction == legal_turn
        )));
        assert_eq!(solo.player_action_count(41), 1);

        // Same-direction and 180-degree commands are accepted by transport but
        // are gameplay no-ops, so neither is an action for APM.
        solo.exec_command(GameCommand::Turn {
            snake_id,
            direction: legal_turn,
        })
        .expect("execute same-direction no-op");
        solo.exec_command(GameCommand::Turn {
            snake_id,
            direction: clockwise(clockwise(legal_turn)),
        })
        .expect("execute reversal no-op");
        assert_eq!(solo.player_action_count(41), 1);

        let mut team = boost_test_game(1);
        let snake_id = team.players[&1].snake_id;

        // Boost commands latch a level. Pressing on an empty meter is a real
        // action with a lasting effect — it is what makes the snake boost the
        // moment fuel arrives — so it counts, while a repeat of the level
        // the player already asked for does not.
        team.exec_command(GameCommand::ActivateBoost { snake_id })
            .expect("latch Boost on an empty meter");
        assert!(team.arena.snakes[snake_id as usize].boost.intent);
        assert!(!team.arena.snakes[snake_id as usize].boost.active);
        assert_eq!(team.player_action_count(1), 1);

        team.exec_command(GameCommand::ActivateBoost { snake_id })
            .expect("execute already-held no-op");
        assert_eq!(team.player_action_count(1), 1);

        team.exec_command(GameCommand::DeactivateBoost { snake_id })
            .expect("release the held control");
        assert!(!team.arena.snakes[snake_id as usize].boost.intent);
        assert_eq!(team.player_action_count(1), 2);

        team.exec_command(GameCommand::DeactivateBoost { snake_id })
            .expect("execute already-released no-op");
        assert_eq!(team.player_action_count(1), 2);

        // A match without Boost configured discards the request entirely; that
        // is a fixed property of the match, not a transient condition. Every
        // matchmade mode now carries Boost, so the boostless case is a Custom
        // game, which is player-defined and never does.
        let mut solo_boostless = GameState::new(
            30,
            30,
            GameType::Custom {
                settings: CustomGameSettings::default(),
            },
            QueueMode::Quickmatch,
            None,
            0,
        );
        let boostless_player = solo_boostless.add_player(7, None).expect("add player");
        solo_boostless
            .exec_command(GameCommand::ActivateBoost {
                snake_id: boostless_player.snake_id,
            })
            .expect("boostless activate");
        assert_eq!(solo_boostless.player_action_count(7), 0);

        let round_trip: GameState =
            serde_json::from_value(serde_json::to_value(&team).unwrap()).unwrap();
        assert_eq!(round_trip.player_action_count(1), 2);
    }

    #[test]
    fn elapsed_match_duration_uses_completed_tick_quanta_and_is_zero_safe() {
        let mut game = GameState::new(10, 10, GameType::Solo, QueueMode::Quickmatch, None, 0);
        game.tick = 25;
        game.properties.tick_duration_ms = 40;
        assert_eq!(game.elapsed_match_ms(), 1_000);

        game.properties.tick_duration_ms = 0;
        assert_eq!(game.elapsed_match_ms(), 0);
    }

    #[test]
    fn boost_config_and_v3_layout_are_fixed_for_duel_and_2v2() {
        for queue_mode in [QueueMode::Quickmatch, QueueMode::Competitive] {
            for per_team in [1, 2] {
                let game = GameState::new(
                    60,
                    40,
                    GameType::TeamMatch { per_team },
                    queue_mode.clone(),
                    Some(99),
                    0,
                );
                let config = game.properties.boost.as_ref().expect("Boost config");
                config.validate().expect("valid default Boost config");
                assert_eq!(game.properties.tick_duration_ms, BOOST_TICK_INTERVAL_MS);
                assert_eq!(config.speed_milli, DEFAULT_BOOST_SPEED_MILLI);
                assert_eq!(config.capacity_ms, DEFAULT_BOOST_CAPACITY_MS);
                assert_eq!(config.packet_charge_ms, DEFAULT_BOOST_PACKET_CHARGE_MS);

                let pads: Vec<(u8, Position, u32, u8)> = game
                    .arena
                    .boost_pads
                    .iter()
                    .map(|pad| (pad.id, pad.position, pad.charge_ms, pad.size_cells))
                    .collect();
                assert_eq!(
                    pads,
                    vec![
                        (0, Position { x: 14, y: 4 }, DEFAULT_BOOST_CAPACITY_MS, 2),
                        (1, Position { x: 14, y: 34 }, DEFAULT_BOOST_CAPACITY_MS, 2),
                        (2, Position { x: 44, y: 4 }, DEFAULT_BOOST_CAPACITY_MS, 2),
                        (3, Position { x: 44, y: 34 }, DEFAULT_BOOST_CAPACITY_MS, 2),
                        (
                            4,
                            Position { x: 26, y: 12 },
                            DEFAULT_BOOST_PACKET_CHARGE_MS,
                            1
                        ),
                        (
                            5,
                            Position { x: 33, y: 12 },
                            DEFAULT_BOOST_PACKET_CHARGE_MS,
                            1
                        ),
                        (
                            6,
                            Position { x: 37, y: 16 },
                            DEFAULT_BOOST_PACKET_CHARGE_MS,
                            1
                        ),
                        (
                            7,
                            Position { x: 37, y: 23 },
                            DEFAULT_BOOST_PACKET_CHARGE_MS,
                            1
                        ),
                        (
                            8,
                            Position { x: 33, y: 27 },
                            DEFAULT_BOOST_PACKET_CHARGE_MS,
                            1
                        ),
                        (
                            9,
                            Position { x: 26, y: 27 },
                            DEFAULT_BOOST_PACKET_CHARGE_MS,
                            1
                        ),
                        (
                            10,
                            Position { x: 22, y: 23 },
                            DEFAULT_BOOST_PACKET_CHARGE_MS,
                            1
                        ),
                        (
                            11,
                            Position { x: 22, y: 16 },
                            DEFAULT_BOOST_PACKET_CHARGE_MS,
                            1
                        ),
                    ]
                );
                for pad_id in 4_u8..12 {
                    let pad = &game.arena.boost_pads[usize::from(pad_id)];
                    let quarter_turn_position = Position {
                        x: 49 - pad.position.y,
                        y: pad.position.x - 10,
                    };
                    let quarter_turn_id = 4 + (pad_id - 4 + 2) % 8;
                    assert_eq!(
                        game.arena.boost_pads[usize::from(quarter_turn_id)].position,
                        quarter_turn_position,
                        "inner Boost IDs must advance by two under a clockwise quarter turn"
                    );
                }
                let footprint: HashSet<Position> = game
                    .arena
                    .boost_pads
                    .iter()
                    .flat_map(BoostPad::footprint_cells)
                    .collect();
                assert_eq!(footprint.len(), 24);
                assert!(footprint.iter().all(|cell| {
                    footprint.contains(&Position {
                        x: 59 - cell.x,
                        y: cell.y,
                    }) && footprint.contains(&Position {
                        x: cell.x,
                        y: 39 - cell.y,
                    }) && footprint.contains(&Position {
                        x: 59 - cell.x,
                        y: 39 - cell.y,
                    })
                }));
                assert!(
                    game.arena
                        .boost_pads
                        .iter()
                        .all(|pad| pad.respawn_at_tick.is_none())
                );
            }
        }

        // Solo carries an unlimited tank: nothing to collect, so no pads, but
        // it is still a Boost match and still runs the 50ms quantum.
        let solo = GameState::new(40, 40, GameType::Solo, QueueMode::Quickmatch, None, 0);
        let solo_boost = solo.properties.boost.as_ref().expect("solo Boost config");
        assert!(solo_boost.unlimited);
        assert_eq!(
            solo_boost.spot_layout_version,
            BOOST_SPOT_LAYOUT_VERSION_NONE
        );
        assert!(solo.arena.boost_pads.is_empty());
        assert_eq!(solo.properties.tick_duration_ms, BOOST_TICK_INTERVAL_MS);
        assert_eq!(solo.properties.score_limit, None);
        assert_eq!(solo.properties.available_food_target, DEFAULT_FOOD_TARGET);
        solo.validate_boost_invariants()
            .expect("solo Boost invariants");

        // Free-for-all collects from the teamless layout on its own square map.
        let free_for_all = GameState::new(
            40,
            40,
            GameType::FreeForAll { max_players: 4 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        let ffa_boost = free_for_all
            .properties
            .boost
            .as_ref()
            .expect("free-for-all Boost config");
        assert!(!ffa_boost.unlimited);
        assert_eq!(
            ffa_boost.spot_layout_version,
            BOOST_SPOT_LAYOUT_VERSION_FIELD
        );
        assert_eq!(free_for_all.arena.boost_pads.len(), 12);
        assert_eq!(free_for_all.properties.score_limit, None);
        assert_eq!(
            free_for_all.properties.available_food_target,
            DEFAULT_FOOD_TARGET * 2,
            "the crowded modes carry double food"
        );
        free_for_all
            .validate_boost_invariants()
            .expect("free-for-all Boost invariants");

        // The teamless layout keeps the same four-fold symmetry the team map
        // has, measured on the full 40x40 arena rather than an inset field.
        // Nothing about a free-for-all spawn quadrant may reach a richer pad.
        let ffa_footprint: HashSet<Position> = free_for_all
            .arena
            .boost_pads
            .iter()
            .flat_map(|pad| pad.footprint_cells())
            .collect();
        assert_eq!(ffa_footprint.len(), 24);
        assert!(ffa_footprint.iter().all(|cell| {
            ffa_footprint.contains(&Position {
                x: 39 - cell.x,
                y: cell.y,
            }) && ffa_footprint.contains(&Position {
                x: cell.x,
                y: 39 - cell.y,
            }) && ffa_footprint.contains(&Position {
                x: 39 - cell.y,
                y: cell.x,
            })
        }));

        let mut spawned_ffa = GameState::new(
            40,
            40,
            GameType::FreeForAll { max_players: 4 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        for user_id in 1..=4 {
            spawned_ffa.add_player(user_id, None).unwrap();
        }
        let heads: HashSet<Position> = spawned_ffa
            .arena
            .snakes
            .iter()
            .map(|snake| snake.body[0])
            .collect();
        assert_eq!(
            heads,
            HashSet::from([
                Position { x: 4, y: 2 },
                Position { x: 4, y: 37 },
                Position { x: 35, y: 2 },
                Position { x: 35, y: 37 },
            ]),
            "FFA starts must mirror across both arena axes"
        );
        let nearest_pad_distances: HashSet<u16> = spawned_ffa
            .arena
            .snakes
            .iter()
            .map(|snake| {
                let head = snake.body[0];
                spawned_ffa
                    .arena
                    .boost_pads
                    .iter()
                    .flat_map(BoostPad::footprint_cells)
                    .map(|cell| head.x.abs_diff(cell.x) + head.y.abs_diff(cell.y))
                    .min()
                    .unwrap()
            })
            .collect();
        assert_eq!(
            nearest_pad_distances,
            HashSet::from([2]),
            "no stable player slot may start closer to a full-tank pad"
        );

        // A 2v2 also carries double food; a duel does not.
        for (per_team, expected_food) in [(1, DEFAULT_FOOD_TARGET), (2, DEFAULT_FOOD_TARGET * 2)] {
            let team = GameState::new(
                60,
                40,
                GameType::TeamMatch { per_team },
                QueueMode::Quickmatch,
                None,
                0,
            );
            assert_eq!(team.properties.available_food_target, expected_food);
        }

        // Off-canonical maps get no Boost rather than a half-drawn layout, so
        // `GameState::new` stays total for previews, tests and Custom games.
        for (width, height, game_type) in [
            (20, 20, GameType::FreeForAll { max_players: 4 }),
            (60, 40, GameType::FreeForAll { max_players: 4 }),
            (40, 40, GameType::TeamMatch { per_team: 1 }),
        ] {
            let odd = GameState::new(width, height, game_type, QueueMode::Quickmatch, None, 0);
            assert!(odd.properties.boost.is_none());
            assert!(odd.arena.boost_pads.is_empty());
            assert_eq!(odd.properties.tick_duration_ms, DEFAULT_TICK_INTERVAL_MS);
            odd.validate_boost_invariants()
                .expect("a boostless off-canonical map is valid");
        }

        let unsupported_team = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 3 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        assert!(unsupported_team.properties.boost.is_none());
        assert!(unsupported_team.arena.boost_pads.is_empty());
        assert_eq!(
            unsupported_team.properties.tick_duration_ms,
            DEFAULT_TICK_INTERVAL_MS
        );

        let custom_tick_ms = 175;
        let custom = GameState::new(
            40,
            40,
            GameType::Custom {
                settings: CustomGameSettings {
                    arena_width: 40,
                    arena_height: 40,
                    tick_duration_ms: custom_tick_ms,
                    food_spawn_rate: 1.0,
                    max_players: 2,
                    game_mode: GameMode::FreeForAll { max_players: 2 },
                    is_private: false,
                    allow_spectators: true,
                    snake_start_length: 4,
                },
            },
            QueueMode::Quickmatch,
            None,
            0,
        );
        assert!(custom.properties.boost.is_none());
        assert!(custom.arena.boost_pads.is_empty());
        assert_eq!(custom.properties.tick_duration_ms, custom_tick_ms);
    }

    fn custom_food_refill_game(tick_duration_ms: u32) -> GameState {
        let mut game = GameState::new(
            40,
            40,
            GameType::Custom {
                settings: CustomGameSettings {
                    arena_width: 40,
                    arena_height: 40,
                    tick_duration_ms,
                    food_spawn_rate: 3.0,
                    max_players: 4,
                    game_mode: GameMode::FreeForAll { max_players: 4 },
                    is_private: true,
                    allow_spectators: true,
                    snake_start_length: 4,
                },
            },
            QueueMode::Quickmatch,
            Some(0),
            0,
        );
        game.properties.available_food_target = 1;
        game.arena.food.clear();
        game
    }

    fn center_third_rate(game_type: GameType, seed: u64, sample_count: usize) -> (f32, f32) {
        let game = GameState::new(60, 40, game_type, QueueMode::Quickmatch, None, 0);
        let (x_min, x_max) = game
            .arena
            .main_field_bounds()
            .unwrap_or((0, game.arena.width as i16 - 1));
        let y_min = 0;
        let y_max = game.arena.height as i16 - 1;
        let x_edge_third = (x_max - x_min + 1) / 3;
        let y_edge_third = (y_max - y_min + 1) / 3;
        let mut x_center_count = 0;
        let mut y_center_count = 0;
        let mut rng = PseudoRandom::new(seed);

        for _ in 0..sample_count {
            let position = sample_food_position(&mut rng, &game.game_type, &game.arena);
            assert!((x_min..=x_max).contains(&position.x));
            assert!((y_min..=y_max).contains(&position.y));

            if (x_min + x_edge_third..=x_max - x_edge_third).contains(&position.x) {
                x_center_count += 1;
            }
            if (y_min + y_edge_third..=y_max - y_edge_third).contains(&position.y) {
                y_center_count += 1;
            }
        }

        (
            x_center_count as f32 / sample_count as f32,
            y_center_count as f32 / sample_count as f32,
        )
    }

    fn assert_uniform_center_third(rate: f32, axis: &str, mode: &str) {
        // The exact discrete center-third width varies by one cell depending
        // on arena size. This broad deterministic band distinguishes a
        // uniform distribution from the roughly 68% center-biased sampler
        // without turning the test into a fragile snapshot of the PRNG.
        assert!(
            (0.29..=0.40).contains(&rate),
            "{mode} {axis} center-third rate should be uniform, got {rate:.3}"
        );
    }

    #[test]
    fn food_distribution_is_mode_and_axis_aware() {
        const SAMPLES: usize = 20_000;

        for (per_team, seed) in [(1, 0xD0E1_u64), (2, 0x2B2_u64)] {
            let (goal_to_goal_rate, end_zone_width_rate) =
                center_third_rate(GameType::TeamMatch { per_team }, seed, SAMPLES);

            assert!(
                goal_to_goal_rate >= 0.60,
                "{per_team}v{per_team} food should remain center-biased on the goal-to-goal axis, got {goal_to_goal_rate:.3}"
            );
            assert_uniform_center_third(
                end_zone_width_rate,
                "end-zone-width",
                &format!("{per_team}v{per_team}"),
            );
            assert!(
                goal_to_goal_rate - end_zone_width_rate >= 0.20,
                "{per_team}v{per_team} axes should have visibly different distributions"
            );
        }

        for (mode, game_type, seed) in [
            ("solo", GameType::Solo, 0x5010_u64),
            ("FFA", GameType::FreeForAll { max_players: 4 }, 0xFFA4_u64),
        ] {
            let (x_rate, y_rate) = center_third_rate(game_type, seed, SAMPLES);
            assert_uniform_center_third(x_rate, "x", mode);
            assert_uniform_center_third(y_rate, "y", mode);
        }
    }

    #[test]
    fn custom_food_refill_retains_every_configured_tick_at_50_and_75_ms() {
        for tick_duration_ms in [50, 75] {
            let mut game = custom_food_refill_game(tick_duration_ms);
            for expected_tick in 1..=4 {
                game.arena.food.clear();
                let events = game.tick_forward(false).expect("Custom food quantum");
                assert!(
                    events
                        .iter()
                        .any(|(_, event)| matches!(event, GameEvent::FoodSpawned { .. })),
                    "Custom {tick_duration_ms}ms tick {expected_tick} lost its per-tick food refill opportunity"
                );
            }
        }
    }

    #[test]
    fn boost_food_refill_remains_on_a_hundred_ms_wall_clock_cadence() {
        let mut game = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            Some(0),
            0,
        );
        game.properties.available_food_target = 1;
        game.arena.food.clear();

        for (expected_tick, should_spawn) in [(1, false), (2, true), (3, false), (4, true)] {
            game.arena.food.clear();
            let events = game.tick_forward(false).expect("Boost food quantum");
            let spawned = events
                .iter()
                .any(|(_, event)| matches!(event, GameEvent::FoodSpawned { .. }));
            assert_eq!(
                spawned, should_spawn,
                "Boost tick {expected_tick} violated its 100ms refill cadence"
            );
        }
    }

    #[test]
    fn boost_config_rejects_values_outside_v3_contract() {
        for config in [
            BoostConfig {
                speed_milli: NORMAL_SNAKE_SPEED_MILLI - 1,
                ..BoostConfig::default()
            },
            BoostConfig {
                speed_milli: MAX_BOOST_SPEED_MILLI + 1,
                ..BoostConfig::default()
            },
            BoostConfig {
                packet_charge_ms: BOOST_TICK_INTERVAL_MS + 1,
                ..BoostConfig::default()
            },
            BoostConfig {
                capacity_ms: DEFAULT_BOOST_CAPACITY_MS + BOOST_TICK_INTERVAL_MS,
                packet_charge_ms: (DEFAULT_BOOST_CAPACITY_MS + BOOST_TICK_INTERVAL_MS) / 4,
                ..BoostConfig::default()
            },
            BoostConfig {
                packet_charge_ms: DEFAULT_BOOST_CAPACITY_MS / 4 + BOOST_TICK_INTERVAL_MS,
                ..BoostConfig::default()
            },
            BoostConfig {
                capacity_ms: 0,
                ..BoostConfig::default()
            },
            BoostConfig {
                spot_layout_version: BOOST_SPOT_LAYOUT_VERSION_TEAM - 1,
                ..BoostConfig::default()
            },
            BoostConfig {
                spot_layout_version: BOOST_SPOT_LAYOUT_VERSION_FIELD + 1,
                ..BoostConfig::default()
            },
            BoostConfig {
                rules_version: BOOST_RULES_VERSION + 1,
                ..BoostConfig::default()
            },
        ] {
            assert!(config.validate().is_err());
        }

        assert!(
            GameState::new_with_boost_config(
                59,
                40,
                GameType::TeamMatch { per_team: 1 },
                QueueMode::Quickmatch,
                None,
                0,
                BoostConfig::default(),
            )
            .is_err()
        );

        for speed_milli in [
            NORMAL_SNAKE_SPEED_MILLI,
            1_001,
            1_250,
            DEFAULT_BOOST_SPEED_MILLI,
            1_999,
            MAX_BOOST_SPEED_MILLI,
        ] {
            let game = GameState::new_with_boost_config(
                60,
                40,
                GameType::TeamMatch { per_team: 2 },
                QueueMode::Competitive,
                None,
                0,
                BoostConfig {
                    speed_milli,
                    ..BoostConfig::default()
                },
            )
            .expect("supported speed should be configurable at match creation");
            assert_eq!(
                game.properties.boost.as_ref().unwrap().speed_milli,
                speed_milli
            );
        }

        let resized_tank = GameState::new_with_boost_config(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            None,
            0,
            BoostConfig {
                capacity_ms: 4_000,
                packet_charge_ms: 1_000,
                ..BoostConfig::default()
            },
        )
        .expect("quarter packet should scale with configured capacity");
        assert!(
            resized_tank.arena.boost_pads[..4]
                .iter()
                .all(|pad| pad.charge_ms == 4_000 && pad.size_cells == 2)
        );
        assert!(
            resized_tank.arena.boost_pads[4..]
                .iter()
                .all(|pad| pad.charge_ms == 1_000 && pad.size_cells == 1)
        );
    }

    #[test]
    fn v3_footprints_do_not_overlap_team_starts_or_each_other() {
        let mut game = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 2 },
            QueueMode::Competitive,
            None,
            0,
        );
        for user_id in 1..=4 {
            game.add_player(user_id, None).unwrap();
        }

        let all_cells: Vec<(u8, Position)> = game
            .arena
            .boost_pads
            .iter()
            .flat_map(|pad| pad.footprint_cells().into_iter().map(|cell| (pad.id, cell)))
            .collect();
        assert_eq!(all_cells.len(), 24);
        assert_eq!(
            all_cells
                .iter()
                .map(|(_, cell)| *cell)
                .collect::<HashSet<_>>()
                .len(),
            24
        );
        for (_, cell) in all_cells {
            assert!(
                game.arena
                    .snakes
                    .iter()
                    .all(|snake| { !snake.contains_point(&cell, false) })
            );
        }
    }

    #[test]
    fn packets_store_charge_without_activating_and_full_snakes_leave_them_available() {
        let mut game = boost_test_game(1);
        let pad_index = 4;
        let pad = game.arena.boost_pads[pad_index].clone();
        let snake_id = game.players[&1].snake_id;
        game.arena.snakes[snake_id as usize].body = vec![
            pad.position,
            Position {
                x: pad.position.x - 3,
                y: pad.position.y,
            },
        ];

        // Activation is resolved before collection, so an empty Space press
        // cannot consume fuel picked up later in this same quantum.
        schedule_activation(&mut game, 1, snake_id, 0, 1);

        let events = game.tick_forward(true).expect("collect packet");
        let snake = &game.arena.snakes[snake_id as usize];
        assert_eq!(snake.boost.charge_ms, DEFAULT_BOOST_PACKET_CHARGE_MS);
        assert!(!snake.boost.active);
        assert_eq!(snake.speed_milli, NORMAL_SNAKE_SPEED_MILLI);
        assert!(events.iter().any(|(_, event)| matches!(
            event,
            GameEvent::BoostPacketCollected {
                pad_id,
                snake_id: collected_by,
                charge_ms_after: DEFAULT_BOOST_PACKET_CHARGE_MS,
                ..
            } if *pad_id == pad.id && *collected_by == snake_id
        )));
        assert!(game.arena.boost_pads[pad_index].respawn_at_tick.is_some());

        game.arena.boost_pads[pad_index].respawn_at_tick = None;
        let snake = &mut game.arena.snakes[snake_id as usize];
        // Release the held control first: the latched intent from the press
        // above would otherwise spend the meter this quantum and make room.
        snake.set_boost_intent(false);
        snake.boost.charge_ms = DEFAULT_BOOST_CAPACITY_MS;
        snake.movement_credit = 0;
        snake.body[0] = pad.position;
        let events = game.tick_forward(true).expect("full-meter packet check");
        assert!(!events.iter().any(|(_, event)| matches!(
            event,
            GameEvent::BoostPacketCollected { pad_id, .. } if *pad_id == pad.id
        )));
        assert!(game.arena.boost_pads[pad_index].respawn_at_tick.is_none());
    }

    #[test]
    fn outer_packet_collects_on_every_footprint_cell_and_fills_the_tank() {
        let template = boost_test_game(1);
        let outer = template.arena.boost_pads[0].clone();
        assert_eq!(outer.charge_ms, DEFAULT_BOOST_CAPACITY_MS);
        assert_eq!(outer.size_cells, 2);
        assert_eq!(outer.footprint_cells().len(), 4);

        for cell in outer.footprint_cells() {
            let mut game = template.clone();
            let snake_id = game.players[&1].snake_id;
            let snake = &mut game.arena.snakes[snake_id as usize];
            snake.body = vec![
                cell,
                Position {
                    x: cell.x - 3,
                    y: cell.y,
                },
            ];
            snake.boost.charge_ms = DEFAULT_BOOST_PACKET_CHARGE_MS;

            let events = game.tick_forward(true).expect("collect outer packet");
            assert_eq!(
                game.arena.snakes[snake_id as usize].boost.charge_ms, DEFAULT_BOOST_CAPACITY_MS,
                "outer packet must cap to a full tank from cell {cell:?}"
            );
            assert!(events.iter().any(|(_, event)| matches!(
                event,
                GameEvent::BoostPacketCollected {
                    pad_id,
                    snake_id: collector,
                    charge_ms_after: DEFAULT_BOOST_CAPACITY_MS,
                    ..
                } if *pad_id == outer.id && *collector == snake_id
            )));
        }
    }

    #[test]
    fn one_outer_footprint_uses_lowest_snake_id_for_contention() {
        let mut game = boost_test_game(2);
        let pad = game.arena.boost_pads[0].clone();
        let cells = pad.footprint_cells();
        let low_id = game.players[&1].snake_id.min(game.players[&2].snake_id);
        let high_id = game.players[&1].snake_id.max(game.players[&2].snake_id);
        for (snake_id, cell) in [(low_id, cells[0]), (high_id, cells[1])] {
            game.arena.snakes[snake_id as usize].body = vec![
                cell,
                Position {
                    x: cell.x - 3,
                    y: cell.y,
                },
            ];
        }

        let events = game
            .tick_forward(true)
            .expect("resolve footprint contention");
        assert_eq!(
            game.arena.snakes[low_id as usize].boost.charge_ms,
            DEFAULT_BOOST_CAPACITY_MS
        );
        assert_eq!(game.arena.snakes[high_id as usize].boost.charge_ms, 0);
        assert_eq!(
            events
                .iter()
                .filter(|(_, event)| matches!(
                    event,
                    GameEvent::BoostPacketCollected {
                        pad_id,
                        snake_id,
                        ..
                    } if *pad_id == pad.id && *snake_id == low_id
                ))
                .count(),
            1
        );
    }

    #[test]
    fn cooling_pad_keeps_its_full_footprint_but_cannot_be_collected() {
        let mut game = boost_test_game(1);
        let pad = game.arena.boost_pads[0].clone();
        game.arena.boost_pads[0].respawn_at_tick = Some(10);
        let snake_id = game.players[&1].snake_id;
        let cell = pad.footprint_cells()[3];
        game.arena.snakes[snake_id as usize].body = vec![
            cell,
            Position {
                x: cell.x - 3,
                y: cell.y,
            },
        ];

        assert!(game.arena.is_boost_pad_position(&cell));
        let events = game.tick_forward(true).expect("cooling footprint quantum");
        assert_eq!(game.arena.snakes[snake_id as usize].boost.charge_ms, 0);
        assert!(!events.iter().any(|(_, event)| matches!(
            event,
            GameEvent::BoostPacketCollected { pad_id, .. } if *pad_id == pad.id
        )));
        assert_eq!(game.arena.boost_pads[0].respawn_at_tick, Some(10));
    }

    #[test]
    fn activation_is_predicted_before_credit_and_distinct_repeats_do_not_stack() {
        let mut game = boost_test_game(1);
        let snake_id = game.players[&1].snake_id;
        let head_before = *game.arena.snakes[snake_id as usize].head().expect("head");
        game.arena.snakes[snake_id as usize].boost.charge_ms = 1_000;

        schedule_activation(&mut game, 1, snake_id, 0, 1);
        game.tick_forward(true).expect("activation quantum");
        let snake = &game.arena.snakes[snake_id as usize];
        assert!(snake.boost.active);
        assert_eq!(snake.speed_milli, DEFAULT_BOOST_SPEED_MILLI);
        assert_eq!(snake.boost.charge_ms, 950);
        assert_eq!(snake.movement_credit, 75_000);
        assert_eq!(*snake.head().unwrap(), head_before);

        schedule_activation(&mut game, 1, snake_id, 1, 2);
        game.tick_forward(true).expect("second active quantum");
        let snake = &game.arena.snakes[snake_id as usize];
        assert_eq!(snake.boost.charge_ms, 900, "repeat must not double-burn");
        assert_eq!(snake.speed_milli, DEFAULT_BOOST_SPEED_MILLI);
        assert_eq!(snake.movement_credit, 50_000);
        assert_ne!(
            *snake.head().unwrap(),
            head_before,
            "1.5x moves on quantum 2"
        );
    }

    #[test]
    fn deactivation_is_predicted_before_credit_and_preserves_unspent_charge() {
        let mut game = boost_test_game(1);
        let snake_id = game.players[&1].snake_id;
        let snake = &mut game.arena.snakes[snake_id as usize];
        snake.boost.charge_ms = 1_000;
        snake.movement_credit = 25_000;
        snake.set_boost_intent(true);
        assert_eq!(
            snake.resolve_boost(DEFAULT_BOOST_SPEED_MILLI, 3_000),
            Some(BoostResolution::Activated)
        );

        schedule_deactivation(&mut game, 1, snake_id, 0, 1);
        let mut transitions = Vec::new();
        game.tick_forward_observing_boost(true, &mut |transition| {
            transitions.push(transition);
        })
        .expect("deactivation quantum");

        let snake = &game.arena.snakes[snake_id as usize];
        assert!(!snake.boost.active);
        assert_eq!(snake.speed_milli, NORMAL_SNAKE_SPEED_MILLI);
        assert_eq!(
            snake.boost.charge_ms, 1_000,
            "release must not burn a quantum"
        );
        assert_eq!(snake.movement_credit, 75_000);
        assert_eq!(
            transitions,
            vec![BoostLifecycleTransition::ManuallyStopped { snake_id }]
        );

        schedule_deactivation(&mut game, 1, snake_id, 1, 2);
        let mut duplicate_transitions = Vec::new();
        game.tick_forward_observing_boost(true, &mut |transition| {
            duplicate_transitions.push(transition);
        })
        .expect("duplicate deactivation quantum");
        assert!(duplicate_transitions.is_empty());
        assert_eq!(game.arena.snakes[snake_id as usize].boost.charge_ms, 1_000);
    }

    #[test]
    fn same_quantum_press_then_release_finishes_inactive_without_burning_charge() {
        let mut game = boost_test_game(1);
        let snake_id = game.players[&1].snake_id;
        game.arena.snakes[snake_id as usize].boost.charge_ms = 1_000;

        schedule_activation(&mut game, 1, snake_id, 0, 1);
        schedule_deactivation(&mut game, 1, snake_id, 0, 2);
        let mut transitions = Vec::new();
        game.tick_forward_observing_boost(true, &mut |transition| {
            transitions.push(transition);
        })
        .expect("press-release quantum");

        let snake = &game.arena.snakes[snake_id as usize];
        assert!(!snake.boost.active);
        assert!(!snake.boost.intent);
        assert_eq!(snake.speed_milli, NORMAL_SNAKE_SPEED_MILLI);
        assert_eq!(snake.boost.charge_ms, 1_000);
        assert_eq!(snake.movement_credit, 50_000);
        // Boost is a level, not a pair of edges: a press and its release inside
        // one quantum collapse to "not held" with no speed change to report.
        assert_eq!(transitions, vec![]);
    }

    /// The reported bug: press and hold Boost with an empty meter, pick up
    /// fuel, and nothing happens until you release and press again. Intent is
    /// a level, so the press must still be in force when the fuel lands.
    #[test]
    fn boost_held_through_an_empty_meter_starts_as_soon_as_fuel_arrives() {
        let mut game = boost_test_game(1);
        let snake_id = game.players[&1].snake_id;
        let capacity_ms = game.properties.boost.as_ref().unwrap().capacity_ms;

        schedule_activation(&mut game, 1, snake_id, 0, 1);
        for _ in 0..4 {
            game.tick_forward(true).expect("held quantum with no fuel");
            let snake = &game.arena.snakes[snake_id as usize];
            assert!(!snake.boost.active, "cannot boost on an empty meter");
            assert!(snake.boost.intent, "the press must stay latched");
            assert_eq!(snake.speed_milli, NORMAL_SNAKE_SPEED_MILLI);
        }

        // Fuel arrives with the control still held and no new command sent.
        game.arena.snakes[snake_id as usize]
            .collect_boost_charge(DEFAULT_BOOST_PACKET_CHARGE_MS, capacity_ms);

        let mut transitions = Vec::new();
        game.tick_forward_observing_boost(true, &mut |transition| transitions.push(transition))
            .expect("first funded quantum");

        let snake = &game.arena.snakes[snake_id as usize];
        assert!(snake.boost.active, "held Boost must start on its own");
        assert_eq!(snake.speed_milli, DEFAULT_BOOST_SPEED_MILLI);
        assert_eq!(
            transitions,
            vec![BoostLifecycleTransition::Activated { snake_id }]
        );
    }

    /// Running the tank dry mid-hold must not silently unlatch the control:
    /// the next packet has to resume Boost without another press.
    #[test]
    fn boost_held_through_depletion_resumes_on_the_next_packet() {
        let mut game = boost_test_game(1);
        let snake_id = game.players[&1].snake_id;
        let capacity_ms = game.properties.boost.as_ref().unwrap().capacity_ms;
        game.arena.snakes[snake_id as usize]
            .collect_boost_charge(2 * BOOST_TICK_INTERVAL_MS, capacity_ms);

        schedule_activation(&mut game, 1, snake_id, 0, 1);
        game.tick_forward(true).expect("first funded quantum");
        assert!(game.arena.snakes[snake_id as usize].boost.active);

        game.tick_forward(true).expect("quantum that runs dry");
        let snake = &game.arena.snakes[snake_id as usize];
        assert!(!snake.boost.active, "an empty meter stops Boost");
        assert!(snake.boost.intent, "but the control is still held");

        game.arena.snakes[snake_id as usize]
            .collect_boost_charge(DEFAULT_BOOST_PACKET_CHARGE_MS, capacity_ms);
        game.tick_forward(true).expect("refuelled quantum");
        assert!(game.arena.snakes[snake_id as usize].boost.active);
    }

    /// A player holding Boost across a death keeps holding it: the new life
    /// boosts on its own fuel with no second press.
    #[test]
    fn boost_held_across_a_death_resumes_on_the_new_life() {
        let mut game = boost_test_game(1);
        let snake_id = game.players[&1].snake_id;
        let capacity_ms = game.properties.boost.as_ref().unwrap().capacity_ms;

        schedule_activation(&mut game, 1, snake_id, 0, 1);
        game.tick_forward(true).expect("latch the held control");
        assert!(game.arena.snakes[snake_id as usize].boost.intent);

        game.apply_event(
            GameEvent::SnakeDied {
                snake_id,
                cause: DeathCause::Unknown,
            },
            None,
        );
        let respawn = game
            .respawn_event_for_snake(snake_id)
            .expect("respawn event");
        game.apply_event(respawn, None);
        let snake = &game.arena.snakes[snake_id as usize];
        assert!(snake.is_alive);
        assert_eq!(snake.boost.charge_ms, 0, "a new life starts empty");
        assert!(snake.boost.intent, "the control is still held");

        game.arena.snakes[snake_id as usize]
            .collect_boost_charge(DEFAULT_BOOST_PACKET_CHARGE_MS, capacity_ms);
        game.tick_forward(true).expect("first funded quantum");
        assert!(game.arena.snakes[snake_id as usize].boost.active);
    }

    /// Releasing while the meter is empty must clear the latch, so fuel picked
    /// up later does not start a Boost nobody asked for.
    #[test]
    fn releasing_an_empty_held_control_does_not_boost_on_later_fuel() {
        let mut game = boost_test_game(1);
        let snake_id = game.players[&1].snake_id;
        let capacity_ms = game.properties.boost.as_ref().unwrap().capacity_ms;

        schedule_activation(&mut game, 1, snake_id, 0, 1);
        schedule_deactivation(&mut game, 1, snake_id, 1, 2);
        game.tick_forward(true).expect("press");
        game.tick_forward(true).expect("release");
        assert!(!game.arena.snakes[snake_id as usize].boost.intent);

        game.arena.snakes[snake_id as usize]
            .collect_boost_charge(DEFAULT_BOOST_PACKET_CHARGE_MS, capacity_ms);
        game.tick_forward(true).expect("fuelled but unheld quantum");
        let snake = &game.arena.snakes[snake_id as usize];
        assert!(!snake.boost.active);
        assert_eq!(snake.boost.charge_ms, DEFAULT_BOOST_PACKET_CHARGE_MS);
    }

    /// A snapshot that claims Boost is running with nobody holding the control
    /// (a lost release, or a stale replica) heals itself within one quantum.
    #[test]
    fn active_boost_without_intent_is_stopped_on_the_next_quantum() {
        let mut game = boost_test_game(1);
        let snake_id = game.players[&1].snake_id;
        let snake = &mut game.arena.snakes[snake_id as usize];
        snake.boost.active = true;
        snake.boost.charge_ms = 1_000;
        snake.speed_milli = DEFAULT_BOOST_SPEED_MILLI;

        let mut transitions = Vec::new();
        game.tick_forward_observing_boost(true, &mut |transition| transitions.push(transition))
            .expect("self-healing quantum");

        let snake = &game.arena.snakes[snake_id as usize];
        assert!(!snake.boost.active);
        assert_eq!(snake.speed_milli, NORMAL_SNAKE_SPEED_MILLI);
        assert_eq!(snake.boost.charge_ms, 1_000, "unspent fuel is kept");
        assert_eq!(
            transitions,
            vec![BoostLifecycleTransition::ManuallyStopped { snake_id }]
        );
    }

    #[test]
    fn final_funded_quantum_can_collect_and_continue_active_boost() {
        let mut game = boost_test_game(1);
        let snake_id = game.players[&1].snake_id;
        let pad_position = game.arena.boost_pads[4].position;
        let snake = &mut game.arena.snakes[snake_id as usize];
        snake.body = vec![
            Position {
                x: pad_position.x - 1,
                y: pad_position.y,
            },
            Position {
                x: pad_position.x - 4,
                y: pad_position.y,
            },
        ];
        snake.direction = Direction::Right;
        snake.boost.intent = true;
        snake.boost.active = true;
        snake.boost.charge_ms = BOOST_TICK_INTERVAL_MS;
        snake.speed_milli = DEFAULT_BOOST_SPEED_MILLI;
        snake.movement_credit = 25_000;

        game.tick_forward(true).expect("final funded movement");
        let snake = &game.arena.snakes[snake_id as usize];
        assert_eq!(*snake.head().unwrap(), pad_position);
        assert!(snake.boost.active);
        assert_eq!(snake.boost.charge_ms, DEFAULT_BOOST_PACKET_CHARGE_MS);
        assert_eq!(snake.speed_milli, DEFAULT_BOOST_SPEED_MILLI);
    }

    #[test]
    fn pad_respawns_on_its_absolute_tick() {
        let mut game = boost_test_game(1);
        game.arena.snakes[0].is_alive = false;
        let respawn_at_tick = 20;
        game.arena.boost_pads[0].respawn_at_tick = Some(respawn_at_tick);
        game.tick = respawn_at_tick - 2;

        game.tick_forward(true).expect("quantum before respawn");
        assert_eq!(
            game.arena.boost_pads[0].respawn_at_tick,
            Some(respawn_at_tick)
        );
        game.tick_forward(true).expect("respawn quantum");
        assert!(game.arena.boost_pads[0].respawn_at_tick.is_none());
    }

    #[test]
    fn faster_mover_hits_stationary_snake_without_boost_collision_branch() {
        let mut game = boost_test_game(2);
        let fast_id = game.players[&1].snake_id;
        let stationary_id = game.players[&2].snake_id;
        {
            let fast = &mut game.arena.snakes[fast_id as usize];
            fast.body = vec![Position { x: 20, y: 12 }, Position { x: 17, y: 12 }];
            fast.direction = Direction::Right;
            fast.boost.intent = true;
            fast.boost.active = true;
            fast.boost.charge_ms = 1_000;
            fast.speed_milli = MAX_BOOST_SPEED_MILLI;
        }
        {
            let stationary = &mut game.arena.snakes[stationary_id as usize];
            stationary.body = vec![Position { x: 21, y: 12 }, Position { x: 21, y: 15 }];
            stationary.direction = Direction::Up;
            stationary.movement_credit = 0;
        }

        let events = game.tick_forward(true).expect("unequal-speed collision");
        assert!(events.iter().any(|(_, event)| matches!(
            event,
            GameEvent::SnakeDied { snake_id, .. } if *snake_id == fast_id
        )));
        assert!(!events.iter().any(|(_, event)| matches!(
            event,
            GameEvent::SnakeDied { snake_id, .. } if *snake_id == stationary_id
        )));
        assert!(game.arena.snakes[stationary_id as usize].is_alive);
        assert_eq!(
            *game.arena.snakes[stationary_id as usize].head().unwrap(),
            Position { x: 21, y: 12 }
        );
    }

    #[test]
    fn faster_mover_collides_with_stationary_head_body_and_tail() {
        for target_y in [15, 17, 19] {
            let mut game = boost_test_game_with_speed(2, MAX_BOOST_SPEED_MILLI);
            let fast_id = game.players[&1].snake_id;
            let stationary_id = game.players[&2].snake_id;

            let fast = &mut game.arena.snakes[fast_id as usize];
            fast.body = vec![
                Position { x: 30, y: target_y },
                Position { x: 27, y: target_y },
            ];
            fast.direction = Direction::Right;
            make_active_mover(fast, MAX_BOOST_SPEED_MILLI);

            let stationary = &mut game.arena.snakes[stationary_id as usize];
            stationary.body = vec![Position { x: 31, y: 15 }, Position { x: 31, y: 19 }];
            stationary.direction = Direction::Up;
            stationary.movement_credit = 0;

            let events = game
                .tick_forward(true)
                .expect("stationary segment collision");
            assert!(events.iter().any(|(_, event)| matches!(
                event,
                GameEvent::SnakeDied { snake_id, .. } if *snake_id == fast_id
            )));
            assert!(!events.iter().any(|(_, event)| matches!(
                event,
                GameEvent::SnakeDied { snake_id, .. } if *snake_id == stationary_id
            )));
            let expected = if target_y == 15 {
                DeathCause::HeadToHead {
                    other_snake_id: stationary_id,
                }
            } else {
                DeathCause::SnakeBody {
                    killer_snake_id: stationary_id,
                }
            };
            assert_death_cause(&events, fast_id, expected);
        }
    }

    #[test]
    fn unequal_speed_same_cell_and_head_swap_kill_both_movers() {
        for (fast_head, normal_head) in [
            (Position { x: 29, y: 20 }, Position { x: 31, y: 20 }),
            (Position { x: 29, y: 20 }, Position { x: 30, y: 20 }),
        ] {
            let mut game = boost_test_game_with_speed(2, MAX_BOOST_SPEED_MILLI);
            let fast_id = game.players[&1].snake_id;
            let normal_id = game.players[&2].snake_id;

            let fast = &mut game.arena.snakes[fast_id as usize];
            fast.body = vec![fast_head, Position { x: 26, y: 20 }];
            fast.direction = Direction::Right;
            make_active_mover(fast, MAX_BOOST_SPEED_MILLI);

            let normal = &mut game.arena.snakes[normal_id as usize];
            normal.body = vec![normal_head, Position { x: 34, y: 20 }];
            normal.direction = Direction::Left;
            normal.movement_credit = 50_000;

            let events = game.tick_forward(true).expect("simultaneous collision");
            for (snake_id, other_snake_id) in [(fast_id, normal_id), (normal_id, fast_id)] {
                assert!(events.iter().any(|(_, event)| matches!(
                    event,
                    GameEvent::SnakeDied { snake_id: dead_id, .. } if *dead_id == snake_id
                )));
                assert_death_cause(&events, snake_id, DeathCause::HeadToHead { other_snake_id });
            }
        }
    }

    #[test]
    fn packet_contention_resolves_collision_before_collection() {
        let mut game = boost_test_game_with_speed(2, MAX_BOOST_SPEED_MILLI);
        let fast_id = game.players[&1].snake_id;
        let normal_id = game.players[&2].snake_id;
        let pad = game.arena.boost_pads[0].clone();

        let fast = &mut game.arena.snakes[fast_id as usize];
        fast.body = vec![
            Position {
                x: pad.position.x - 1,
                y: pad.position.y,
            },
            Position {
                x: pad.position.x - 4,
                y: pad.position.y,
            },
        ];
        fast.direction = Direction::Right;
        make_active_mover(fast, MAX_BOOST_SPEED_MILLI);

        let normal = &mut game.arena.snakes[normal_id as usize];
        normal.body = vec![
            Position {
                x: pad.position.x + 1,
                y: pad.position.y,
            },
            Position {
                x: pad.position.x + 4,
                y: pad.position.y,
            },
        ];
        normal.direction = Direction::Left;
        normal.movement_credit = 50_000;

        let events = game.tick_forward(true).expect("packet contention");
        assert_eq!(
            events
                .iter()
                .filter(|(_, event)| matches!(event, GameEvent::SnakeDied { .. }))
                .count(),
            2
        );
        assert!(!events.iter().any(|(_, event)| matches!(
            event,
            GameEvent::BoostPacketCollected { pad_id, .. } if *pad_id == pad.id
        )));
        assert!(
            game.arena.boost_pads[pad.id as usize]
                .respawn_at_tick
                .is_none()
        );
    }

    #[test]
    fn boosted_movement_keeps_wall_enemy_base_food_and_scoring_rules() {
        // Ordinary food is consumed on the one-cell boosted movement boundary.
        let mut food_game = boost_test_game_with_speed(1, MAX_BOOST_SPEED_MILLI);
        let food_snake_id = food_game.players[&1].snake_id;
        let food_position = Position { x: 30, y: 25 };
        let food_snake = &mut food_game.arena.snakes[food_snake_id as usize];
        food_snake.body = vec![Position { x: 29, y: 25 }, Position { x: 26, y: 25 }];
        food_snake.direction = Direction::Right;
        make_active_mover(food_snake, MAX_BOOST_SPEED_MILLI);
        food_game.arena.food = vec![food_position];
        let events = food_game.tick_forward(true).expect("boosted food movement");
        assert!(events.iter().any(|(_, event)| matches!(
            event,
            GameEvent::FoodEaten {
                snake_id, position, ..
            }
                if *snake_id == food_snake_id && *position == food_position
        )));
        assert!(food_game.arena.snakes[food_snake_id as usize].boost.active);

        // A closed goal-boundary wall still kills and clears Boost.
        let mut wall_game = boost_test_game_with_speed(1, MAX_BOOST_SPEED_MILLI);
        let wall_snake_id = wall_game.players[&1].snake_id;
        let wall_snake = &mut wall_game.arena.snakes[wall_snake_id as usize];
        wall_snake.body = vec![Position { x: 10, y: 5 }, Position { x: 13, y: 5 }];
        wall_snake.direction = Direction::Left;
        make_active_mover(wall_snake, MAX_BOOST_SPEED_MILLI);
        let wall_events = wall_game
            .tick_forward(true)
            .expect("boosted wall collision");
        assert!(wall_events.iter().any(|(_, event)| matches!(
            event,
            GameEvent::SnakeDied { snake_id, .. } if *snake_id == wall_snake_id
        )));
        assert_death_cause(&wall_events, wall_snake_id, DeathCause::Wall);
        // Death drains fuel and stops Boost, but the player is still holding
        // the control, so their latched intent survives into the next life.
        assert_eq!(
            wall_game.arena.snakes[wall_snake_id as usize].boost,
            SnakeBoost {
                charge_ms: 0,
                active: false,
                intent: true,
            }
        );

        // Crossing the open enemy goal into its base remains lethal.
        let mut enemy_game = boost_test_game_with_speed(1, MAX_BOOST_SPEED_MILLI);
        let enemy_snake_id = enemy_game.players[&1].snake_id;
        let enemy_snake = &mut enemy_game.arena.snakes[enemy_snake_id as usize];
        enemy_snake.body = vec![Position { x: 49, y: 20 }, Position { x: 46, y: 20 }];
        enemy_snake.direction = Direction::Right;
        make_active_mover(enemy_snake, MAX_BOOST_SPEED_MILLI);
        let enemy_events = enemy_game
            .tick_forward(true)
            .expect("boosted enemy-base collision");
        assert!(enemy_events.iter().any(|(_, event)| matches!(
            event,
            GameEvent::SnakeDied { snake_id, .. } if *snake_id == enemy_snake_id
        )));
        assert_death_cause(&enemy_events, enemy_snake_id, DeathCause::EnemyBase);

        // Returning carried food through one's own open goal scores, then the
        // ordinary scoring respawn clears Boost and movement phase.
        let mut score_game = boost_test_game_with_speed(1, MAX_BOOST_SPEED_MILLI);
        let score_snake_id = score_game.players[&1].snake_id;
        let score_snake = &mut score_game.arena.snakes[score_snake_id as usize];
        score_snake.body = vec![Position { x: 9, y: 20 }, Position { x: 12, y: 20 }];
        score_snake.direction = Direction::Left;
        score_snake.food = 1;
        make_active_mover(score_snake, MAX_BOOST_SPEED_MILLI);
        score_game
            .tick_forward(false)
            .expect("boosted scoring movement");
        assert_eq!(score_game.team_scores.as_ref().unwrap()[&TeamId(0)], 1);
        let score_snake = &score_game.arena.snakes[score_snake_id as usize];
        assert_eq!(
            score_snake.boost,
            SnakeBoost {
                charge_ms: 0,
                active: false,
                intent: true,
            }
        );
        assert_eq!(score_snake.speed_milli, NORMAL_SNAKE_SPEED_MILLI);
        assert_eq!(score_snake.movement_credit, 0);
    }

    #[test]
    fn death_clears_boost_and_every_queued_player_command_for_that_snake() {
        let mut game = boost_test_game(1);
        let snake_id = game.players[&1].snake_id;
        let snake = &mut game.arena.snakes[snake_id as usize];
        snake.boost.intent = true;
        snake.boost.active = true;
        snake.boost.charge_ms = 1_000;
        snake.speed_milli = DEFAULT_BOOST_SPEED_MILLI;
        snake.movement_credit = 42_000;

        schedule_activation(&mut game, 1, snake_id, 50, 1);
        game.schedule_command(&GameCommandMessage {
            command_id_client: create_command_id(50, 1, 2),
            command_id_server: Some(create_command_id(50, 1, 2)),
            command: GameCommand::Turn {
                snake_id,
                direction: Direction::Up,
            },
        });
        assert!(game.has_scheduled_commands(50));

        game.apply_event(
            GameEvent::SnakeDied {
                snake_id,
                cause: DeathCause::Unknown,
            },
            None,
        );
        let snake = &game.arena.snakes[snake_id as usize];
        assert!(!snake.is_alive);
        assert_eq!(snake.speed_milli, NORMAL_SNAKE_SPEED_MILLI);
        assert_eq!(snake.movement_credit, 0);
        // Fuel and activation are cleared; the held control is not a thing the
        // simulation may forget on the player's behalf.
        assert_eq!(
            snake.boost,
            SnakeBoost {
                charge_ms: 0,
                active: false,
                intent: true,
            }
        );
        assert!(!game.has_scheduled_commands(50));
    }

    #[test]
    fn completed_game_freezes_boost_movement_and_pad_state() {
        let mut game = boost_test_game(1);
        let snake_id = game.players[&1].snake_id;
        let snake = &mut game.arena.snakes[snake_id as usize];
        snake.boost.intent = true;
        snake.boost.active = true;
        snake.boost.charge_ms = 1_000;
        snake.speed_milli = DEFAULT_BOOST_SPEED_MILLI;
        snake.movement_credit = 50_000;
        game.arena.boost_pads[0].respawn_at_tick = Some(game.tick + 1);
        game.status = GameStatus::Complete {
            winning_snake_id: None,
        };

        let before = serde_json::to_value(&game).expect("serialize terminal state");
        assert!(game.tick_forward(false).expect("terminal no-op").is_empty());
        assert_eq!(serde_json::to_value(&game).unwrap(), before);
    }

    /// A team match ends on the tick a side reaches its target, and the side
    /// that got there wins. Overshooting the target (a bank worth several
    /// points) still ends the match on that tick.
    #[test]
    fn team_match_ends_when_a_side_reaches_the_score_limit() {
        let mut game = boost_test_game(2);
        game.status = GameStatus::Started { server_id: 7 };
        game.properties.score_limit = Some(3);

        game.apply_event(
            GameEvent::TeamScoreUpdated {
                team_id: TeamId(0),
                score: 2,
            },
            None,
        );
        game.tick_forward(false).expect("below the target");
        assert!(!game.is_complete());

        game.apply_event(
            GameEvent::TeamScoreUpdated {
                team_id: TeamId(0),
                score: 4,
            },
            None,
        );
        game.tick_forward(false).expect("target reached");
        let GameStatus::Complete { winning_snake_id } = game.status else {
            panic!("reaching the score limit must complete the match");
        };
        let winner = winning_snake_id.expect("the side that got there wins");
        assert_eq!(
            game.arena.snakes[winner as usize].team_id,
            Some(TeamId(0)),
            "the winning snake must belong to the winning team"
        );
    }

    /// Both sides can bank on the same tick. Level at the target is a draw,
    /// exactly as a tied clock finish used to be.
    #[test]
    fn simultaneous_arrival_at_the_score_limit_is_a_draw() {
        let mut game = boost_test_game(2);
        game.status = GameStatus::Started { server_id: 7 };
        game.properties.score_limit = Some(3);

        for team_id in [TeamId(0), TeamId(1)] {
            game.apply_event(GameEvent::TeamScoreUpdated { team_id, score: 3 }, None);
        }
        game.tick_forward(false)
            .expect("both sides arrive together");
        assert!(matches!(
            game.status,
            GameStatus::Complete {
                winning_snake_id: None
            }
        ));
    }

    #[test]
    fn inactivity_fires_on_the_exact_authoritative_deadline() {
        let mut game = started_inactivity_team_game(2);
        let timeout_ticks = inactivity_timeout_ticks(&game);
        let idle_snake_id = game.players[&1].snake_id;

        game.tick = timeout_ticks - 2;
        game.player_last_activity_ticks.insert(1, 0);
        for user_id in [2, 3, 4] {
            game.player_last_activity_ticks.insert(user_id, game.tick);
        }

        let before_deadline = game.tick_forward(true).expect("deadline minus one");
        assert_eq!(game.tick, timeout_ticks - 1);
        assert!(game.idle_kicked_user_ids.is_empty());
        assert!(game.arena.snakes[idle_snake_id as usize].is_alive);
        assert!(!before_deadline.iter().any(|(_, event)| matches!(
            event,
            GameEvent::PlayerIdleKicked { user_id: 1, snake_id }
                if *snake_id == idle_snake_id
        )));

        let at_deadline = game.tick_forward(true).expect("exact deadline");
        assert_eq!(game.tick, timeout_ticks);
        assert_eq!(game.idle_kicked_user_ids, vec![1]);
        assert!(!game.arena.snakes[idle_snake_id as usize].is_alive);
        assert!(matches!(game.status, GameStatus::Started { .. }));
        assert!(at_deadline.iter().any(|(_, event)| matches!(
            event,
            GameEvent::PlayerIdleKicked { user_id: 1, snake_id }
                if *snake_id == idle_snake_id
        )));
    }

    #[test]
    fn distinct_noop_and_player_activity_commands_reset_the_full_deadline() {
        let mut game = started_inactivity_team_game(1);
        let timeout_ticks = inactivity_timeout_ticks(&game);
        game.tick = timeout_ticks - 1;
        for user_id in [1, 2] {
            game.player_last_activity_ticks.insert(user_id, 0);
        }

        let first_snake_id = game.players[&1].snake_id;
        let current_direction = game.arena.snakes[first_snake_id as usize].direction;
        schedule_inactivity_test_command(
            &mut game,
            1,
            1,
            GameCommand::Turn {
                snake_id: first_snake_id,
                direction: current_direction,
            },
        );
        let second_snake_id = game.players[&2].snake_id;
        schedule_inactivity_test_command(
            &mut game,
            2,
            1,
            GameCommand::PlayerActivity {
                snake_id: second_snake_id,
            },
        );

        assert_eq!(game.player_last_activity_ticks[&1], timeout_ticks - 1);
        assert_eq!(game.player_last_activity_ticks[&2], timeout_ticks - 1);
        game.tick_forward(true)
            .expect("both commands reset activity before expiry");
        assert!(matches!(game.status, GameStatus::Started { .. }));
        assert!(game.idle_kicked_user_ids.is_empty());

        let renewed_deadline = (timeout_ticks - 1) + timeout_ticks;
        game.tick = renewed_deadline - 2;
        game.tick_forward(true).expect("renewed deadline minus one");
        assert!(game.idle_kicked_user_ids.is_empty());
        game.tick_forward(true).expect("renewed exact deadline");
        assert_eq!(game.tick, renewed_deadline);
        assert_eq!(game.idle_kicked_user_ids, vec![1, 2]);
    }

    #[test]
    fn duel_inactivity_awards_the_forfeit_to_the_active_opponent() {
        let mut game = started_inactivity_team_game(1);
        let timeout_ticks = inactivity_timeout_ticks(&game);
        let active_snake_id = game.players[&1].snake_id;
        let idle_snake_id = game.players[&2].snake_id;
        game.tick = timeout_ticks - 1;
        game.player_last_activity_ticks.insert(1, game.tick);
        game.player_last_activity_ticks.insert(2, 0);

        let events = game.tick_forward(true).expect("duel forfeit");

        assert_eq!(game.idle_kicked_user_ids, vec![2]);
        assert!(game.completed_by_inactivity);
        assert!(matches!(
            game.status,
            GameStatus::Complete {
                winning_snake_id: Some(winner)
            } if winner == active_snake_id
        ));
        assert!(game.arena.snakes[active_snake_id as usize].is_alive);
        assert!(!game.arena.snakes[idle_snake_id as usize].is_alive);
        assert!(events.iter().any(|(_, event)| matches!(
            event,
            GameEvent::StatusUpdated {
                status: GameStatus::Complete {
                    winning_snake_id: Some(winner)
                }
            } if *winner == active_snake_id
        )));
    }

    #[test]
    fn idle_kick_clears_held_boost_and_round_trips_as_a_valid_snapshot() {
        let mut game = started_inactivity_team_game(2);
        let timeout_ticks = inactivity_timeout_ticks(&game);
        let idle_snake_id = game.players[&1].snake_id;
        game.arena.snakes[idle_snake_id as usize].boost.intent = true;
        game.tick = timeout_ticks - 1;
        game.player_last_activity_ticks.insert(1, 0);
        for user_id in [2, 3, 4] {
            game.player_last_activity_ticks.insert(user_id, game.tick);
        }

        game.tick_forward(true).expect("held-Boost idle removal");

        let idle_snake = &game.arena.snakes[idle_snake_id as usize];
        assert!(!idle_snake.is_alive);
        assert_eq!(idle_snake.boost, Default::default());
        game.validate_boost_invariants().unwrap();
        let restored: GameState = serde_json::from_str(&serde_json::to_string(&game).unwrap())
            .expect("idle-kicked snapshot round trip");
        restored.validate_boost_invariants().unwrap();
    }

    #[test]
    fn simultaneous_inactivity_expiry_is_an_atomic_draw() {
        let mut game = started_inactivity_team_game(1);
        let timeout_ticks = inactivity_timeout_ticks(&game);
        game.tick = timeout_ticks - 1;
        game.player_last_activity_ticks.insert(1, 0);
        game.player_last_activity_ticks.insert(2, 0);
        let snake_ids = [game.players[&1].snake_id, game.players[&2].snake_id];

        let events = game.tick_forward(true).expect("simultaneous expiry");

        assert_eq!(game.idle_kicked_user_ids, vec![1, 2]);
        assert!(game.completed_by_inactivity);
        assert!(matches!(
            game.status,
            GameStatus::Complete {
                winning_snake_id: None
            }
        ));
        assert!(
            snake_ids
                .iter()
                .all(|snake_id| !game.arena.snakes[*snake_id as usize].is_alive)
        );
        let death_order: Vec<u32> = events
            .iter()
            .filter_map(|(_, event)| match event {
                GameEvent::PlayerIdleKicked { snake_id, .. } => Some(*snake_id),
                _ => None,
            })
            .collect();
        assert_eq!(death_order, snake_ids);
    }

    #[test]
    fn ffa_removes_one_idle_player_then_awards_the_last_active_survivor() {
        let mut game = started_inactivity_ffa_game(3);
        let timeout_ticks = inactivity_timeout_ticks(&game);
        game.tick = timeout_ticks - 1;
        game.player_last_activity_ticks.insert(1, 0);
        for user_id in [2, 3] {
            game.player_last_activity_ticks.insert(user_id, game.tick);
        }

        game.tick_forward(true).expect("first FFA idle removal");
        assert_eq!(game.idle_kicked_user_ids, vec![1]);
        assert!(matches!(game.status, GameStatus::Started { .. }));

        let winner_snake_id = game.players[&3].snake_id;
        let second_deadline = game.player_last_activity_ticks[&2] + timeout_ticks;
        game.tick = second_deadline - 1;
        game.player_last_activity_ticks.insert(3, game.tick);
        game.tick_forward(true).expect("FFA survivor win");

        assert_eq!(game.idle_kicked_user_ids, vec![1, 2]);
        assert!(game.completed_by_inactivity);
        assert!(matches!(
            game.status,
            GameStatus::Complete {
                winning_snake_id: Some(winner)
            } if winner == winner_snake_id
        ));
    }

    #[test]
    fn ffa_does_not_idle_kick_a_player_already_eliminated_by_gameplay() {
        let mut game = started_inactivity_ffa_game(3);
        let timeout_ticks = inactivity_timeout_ticks(&game);
        let eliminated_snake_id = game.players[&1].snake_id;
        game.apply_event(
            GameEvent::SnakeDied {
                snake_id: eliminated_snake_id,
                cause: DeathCause::Unknown,
            },
            None,
        );
        game.tick = timeout_ticks - 1;
        game.player_last_activity_ticks.insert(1, 0);
        game.player_last_activity_ticks.insert(2, 0);
        game.player_last_activity_ticks.insert(3, game.tick);
        let winner_snake_id = game.players[&3].snake_id;

        game.tick_forward(true).expect("remaining idle contender");

        assert_eq!(game.idle_kicked_user_ids, vec![2]);
        assert!(matches!(
            game.status,
            GameStatus::Complete {
                winning_snake_id: Some(winner)
            } if winner == winner_snake_id
        ));
    }

    #[test]
    fn legacy_active_snapshot_defaults_idle_policy_and_receives_full_grace() {
        let mut game = started_inactivity_team_game(1);
        game.tick = 123;
        let mut json = serde_json::to_value(game).unwrap();
        let object = json.as_object_mut().unwrap();
        object.remove("player_last_activity_ticks");
        object.remove("idle_kicked_user_ids");
        object.remove("completed_by_inactivity");
        let properties = object["properties"].as_object_mut().unwrap();
        properties.remove("player_idle_timeout_ms");
        properties.remove("player_idle_warning_ms");

        let mut restored: GameState = serde_json::from_value(json).unwrap();
        assert_eq!(
            restored.properties.player_idle_timeout_ms,
            DEFAULT_PLAYER_IDLE_TIMEOUT_MS
        );
        assert_eq!(
            restored.properties.player_idle_warning_ms,
            DEFAULT_PLAYER_IDLE_WARNING_MS
        );
        assert!(restored.player_last_activity_ticks.is_empty());
        restored.tick_forward(true).expect("legacy grace quantum");
        assert!(restored.idle_kicked_user_ids.is_empty());
        assert!(
            restored
                .player_last_activity_ticks
                .values()
                .all(|activity_tick| *activity_tick == 123)
        );
        restored.validate_boost_invariants().unwrap();
    }

    #[test]
    fn two_on_two_kicks_one_player_then_forfeits_when_their_team_is_empty() {
        let mut game = started_inactivity_team_game(2);
        let timeout_ticks = inactivity_timeout_ticks(&game);
        let first_idle_snake_id = game.players[&1].snake_id;
        let remaining_teammate_snake_id = game.players[&3].snake_id;
        let winning_snake_id = game.players[&2].snake_id;
        game.tick = timeout_ticks - 1;
        game.player_last_activity_ticks.insert(1, 0);
        for user_id in [2, 3, 4] {
            game.player_last_activity_ticks.insert(user_id, game.tick);
        }

        game.tick_forward(true).expect("individual 2v2 removal");
        assert_eq!(game.idle_kicked_user_ids, vec![1]);
        assert!(matches!(game.status, GameStatus::Started { .. }));
        assert!(!game.arena.snakes[first_idle_snake_id as usize].is_alive);
        assert!(game.arena.snakes[remaining_teammate_snake_id as usize].is_alive);

        let after_kick = game.tick_forward(true).expect("post-kick quantum");
        assert!(!game.arena.snakes[first_idle_snake_id as usize].is_alive);
        assert!(!after_kick.iter().any(|(_, event)| matches!(
            event,
            GameEvent::SnakeRespawned { snake_id, .. } if *snake_id == first_idle_snake_id
        )));

        let teammate_deadline = game.player_last_activity_ticks[&3] + timeout_ticks;
        game.tick = teammate_deadline - 1;
        for user_id in [2, 4] {
            game.player_last_activity_ticks.insert(user_id, game.tick);
        }
        game.tick_forward(true)
            .expect("last teammate expires at their deadline");

        assert_eq!(game.idle_kicked_user_ids, vec![1, 3]);
        assert!(game.completed_by_inactivity);
        assert!(matches!(
            game.status,
            GameStatus::Complete {
                winning_snake_id: Some(winner)
            } if winner == winning_snake_id
        ));
        assert!(!game.arena.snakes[first_idle_snake_id as usize].is_alive);
        assert!(!game.arena.snakes[remaining_teammate_snake_id as usize].is_alive);
    }

    #[test]
    fn player_kicked_mid_match_receives_no_xp_when_score_later_ends_match() {
        let mut game = started_inactivity_team_game(2);
        let timeout_ticks = inactivity_timeout_ticks(&game);
        game.tick = timeout_ticks - 1;
        game.player_last_activity_ticks.insert(1, 0);
        for user_id in [2, 3, 4] {
            game.player_last_activity_ticks.insert(user_id, game.tick);
        }

        game.tick_forward(true).expect("individual idle removal");
        assert_eq!(game.idle_kicked_user_ids, vec![1]);
        assert!(matches!(game.status, GameStatus::Started { .. }));

        let winning_snake_id = game.players[&2].snake_id;
        let winning_team = game.arena.snakes[winning_snake_id as usize]
            .team_id
            .expect("team assignment");
        let score_limit = game.properties.score_limit.expect("team score limit");
        game.apply_event(
            GameEvent::TeamScoreUpdated {
                team_id: winning_team,
                score: score_limit,
            },
            None,
        );
        game.tick_forward(false)
            .expect("score completion after kick");

        assert!(game.is_complete());
        assert!(!game.completed_by_inactivity);
        assert_eq!(game.player_xp.get(&1), Some(&0));
        let winner_pickups = game
            .food_pickups
            .get(&winning_snake_id)
            .copied()
            .unwrap_or(0);
        assert_eq!(game.player_xp.get(&2), Some(&(winner_pickups * 10 + 50)));
    }

    /// Player inactivity is separate from the score race: a scoreless match
    /// whose players remain present still has no time-limit completion.
    #[test]
    fn an_active_scoreless_team_match_never_ends_on_time() {
        let mut game = started_inactivity_team_game(1);
        assert_eq!(game.properties.time_limit_ms, None);
        let refresh_interval = inactivity_timeout_ticks(&game) - 1;
        let players: Vec<(u32, u32)> = game
            .players
            .iter()
            .map(|(user_id, player)| (*user_id, player.snake_id))
            .collect();
        let mut sequence_number = 1;

        // Well past the old ninety-second clock, with explicit presence before
        // each inactivity deadline.
        for _ in 0..2_400 {
            if game.tick.is_multiple_of(refresh_interval) {
                for (user_id, snake_id) in &players {
                    schedule_inactivity_test_command(
                        &mut game,
                        *user_id,
                        sequence_number,
                        GameCommand::PlayerActivity {
                            snake_id: *snake_id,
                        },
                    );
                    sequence_number += 1;
                }
            }
            game.tick_forward(false).expect("untimed quantum");
        }
        assert!(!game.is_complete());
        assert_eq!(game.tick, 2_400);
    }

    /// Each queue races to its own target, chosen once at construction from
    /// the game's queue mode rather than patched in afterwards.
    #[test]
    fn team_score_limits_come_from_the_queue_mode() {
        for (queue_mode, expected) in [
            (QueueMode::Quickmatch, DEFAULT_QUICKMATCH_TEAM_SCORE_LIMIT),
            (QueueMode::Competitive, DEFAULT_COMPETITIVE_TEAM_SCORE_LIMIT),
        ] {
            let game = GameState::new(
                60,
                40,
                GameType::TeamMatch { per_team: 1 },
                queue_mode.clone(),
                Some(1234),
                0,
            );
            assert_eq!(game.properties.score_limit, Some(expected));
            assert_eq!(game.properties.time_limit_ms, None);
            assert_eq!(team_score_limit(&queue_mode), expected);
            game.validate_boost_invariants().unwrap();
        }

        assert_eq!(DEFAULT_QUICKMATCH_TEAM_SCORE_LIMIT, 25);
        assert_eq!(DEFAULT_COMPETITIVE_TEAM_SCORE_LIMIT, 50);
    }

    #[test]
    fn initial_food_permanently_excludes_every_boost_pad() {
        for seed in 0..64 {
            let mut game = GameState::new(
                60,
                40,
                GameType::TeamMatch { per_team: 2 },
                QueueMode::Competitive,
                Some(seed),
                0,
            );
            for user_id in 1..=4 {
                game.add_player(user_id, None).expect("add team player");
            }
            game.arena.boost_pads[0].respawn_at_tick = Some(160);
            game.arena.boost_pads[6].respawn_at_tick = Some(80);
            game.spawn_initial_food();
            assert!(game.arena.boost_pads.iter().all(|pad| {
                pad.footprint_cells().into_iter().all(|cell| {
                    !game.arena.food.contains(&cell)
                        && !game.arena.is_wall_position(&cell)
                        && game.arena.is_boost_pad_position(&cell)
                })
            }));
        }
    }

    #[test]
    fn boost_snapshot_invariants_reject_cross_field_corruption() {
        let baseline = boost_test_game(1);

        let mut invalid = baseline.clone();
        invalid.properties.available_food_target += 1;
        assert!(invalid.validate_boost_invariants().is_err());

        let mut invalid = baseline.clone();
        invalid.properties.tick_duration_ms = DEFAULT_TICK_INTERVAL_MS;
        assert!(invalid.validate_boost_invariants().is_err());

        let mut invalid = baseline.clone();
        invalid.arena.boost_pads.pop();
        assert!(invalid.validate_boost_invariants().is_err());

        let mut invalid = baseline.clone();
        let non_anchor_footprint_cell = invalid.arena.boost_pads[0].footprint_cells()[3];
        invalid.arena.food.push(non_anchor_footprint_cell);
        assert!(invalid.validate_boost_invariants().is_err());

        let mut invalid = baseline.clone();
        invalid.arena.boost_pads[0].size_cells = 1;
        assert!(invalid.validate_boost_invariants().is_err());

        let mut invalid = baseline.clone();
        invalid.arena.boost_pads[0].charge_ms = DEFAULT_BOOST_PACKET_CHARGE_MS;
        assert!(invalid.validate_boost_invariants().is_err());

        let mut invalid = baseline.clone();
        invalid.arena.boost_pads[7].position = invalid.arena.boost_pads[6].position;
        assert!(invalid.validate_boost_invariants().is_err());

        let mut invalid = baseline.clone();
        invalid.arena.snakes[0].boost.intent = true;
        invalid.arena.snakes[0].boost.active = true;
        invalid.arena.snakes[0].boost.charge_ms = 25;
        invalid.arena.snakes[0].speed_milli = DEFAULT_BOOST_SPEED_MILLI;
        assert!(invalid.validate_boost_invariants().is_err());

        // A team match must be raced to a score, never against a clock.
        let mut invalid = baseline.clone();
        invalid.properties.time_limit_ms = Some(90_000);
        assert!(invalid.validate_boost_invariants().is_err());

        let mut invalid = baseline.clone();
        invalid.properties.score_limit = None;
        assert!(invalid.validate_boost_invariants().is_err());

        // The target must match the queue the match was made for.
        let mut invalid = baseline;
        invalid.properties.score_limit = Some(DEFAULT_QUICKMATCH_TEAM_SCORE_LIMIT + 1);
        assert!(invalid.validate_boost_invariants().is_err());
    }

    #[test]
    fn unlimited_boost_requires_full_charge_for_every_living_snake() {
        let mut solo = GameState::new(40, 40, GameType::Solo, QueueMode::Quickmatch, None, 0);
        let snake_id = solo.add_player(1, None).unwrap().snake_id as usize;
        let capacity = solo.properties.boost.as_ref().unwrap().capacity_ms;
        assert_eq!(solo.arena.snakes[snake_id].boost.charge_ms, capacity);

        solo.arena.snakes[snake_id].boost.charge_ms = 0;
        assert!(solo.validate_boost_invariants().is_err());

        solo.arena.snakes[snake_id].is_alive = false;
        solo.validate_boost_invariants()
            .expect("a dead unlimited snake must be empty");
    }

    #[test]
    fn replicated_boost_deltas_are_atomic_and_reject_missing_or_conflicting_targets() {
        let mut game = boost_test_game(1);
        let pad = game.arena.boost_pads[0].clone();
        let snake_id = game.players[&1].snake_id;
        game.arena.snakes[snake_id as usize].body = vec![
            pad.position,
            Position {
                x: pad.position.x + 3,
                y: pad.position.y,
            },
        ];
        let cooldown_ticks = game.properties.boost.as_ref().unwrap().pad_respawn_ms
            / game.properties.tick_duration_ms;
        let valid_respawn = game.tick + cooldown_ticks;

        let before = serde_json::to_value(&game).unwrap();
        let missing_pad = GameEvent::BoostPacketCollected {
            pad_id: u8::MAX,
            snake_id,
            charge_ms_after: pad.charge_ms,
            respawn_at_tick: valid_respawn,
        };
        assert!(game.try_apply_replicated_event(missing_pad).is_err());
        assert_eq!(serde_json::to_value(&game).unwrap(), before);

        let missing_snake = GameEvent::BoostPacketCollected {
            pad_id: pad.id,
            snake_id: u32::MAX,
            charge_ms_after: pad.charge_ms,
            respawn_at_tick: valid_respawn,
        };
        assert!(game.try_apply_replicated_event(missing_snake).is_err());
        assert_eq!(serde_json::to_value(&game).unwrap(), before);

        let conflicting_charge = GameEvent::BoostPacketCollected {
            pad_id: pad.id,
            snake_id,
            charge_ms_after: DEFAULT_BOOST_PACKET_CHARGE_MS,
            respawn_at_tick: valid_respawn,
        };
        assert!(game.try_apply_replicated_event(conflicting_charge).is_err());
        assert_eq!(serde_json::to_value(&game).unwrap(), before);

        let footprint_food = GameEvent::FoodSpawned {
            position: pad.footprint_cells()[3],
        };
        assert!(game.try_apply_replicated_event(footprint_food).is_err());
        assert_eq!(serde_json::to_value(&game).unwrap(), before);

        game.try_apply_replicated_event(GameEvent::BoostPacketCollected {
            pad_id: pad.id,
            snake_id,
            charge_ms_after: pad.charge_ms,
            respawn_at_tick: valid_respawn,
        })
        .expect("complete valid Boost transition applies atomically");
        assert_eq!(
            game.arena.snakes[snake_id as usize].boost.charge_ms,
            pad.charge_ms
        );
        assert_eq!(
            game.arena.boost_pads[pad.id as usize].respawn_at_tick,
            Some(valid_respawn)
        );
    }

    #[test]
    fn nonzero_boost_state_round_trips_and_validates() {
        let mut game = boost_test_game(1);
        let snake = &mut game.arena.snakes[0];
        snake.boost.intent = true;
        snake.boost.active = true;
        snake.boost.charge_ms = 1_000;
        snake.speed_milli = DEFAULT_BOOST_SPEED_MILLI;
        snake.movement_credit = 75_000;
        game.arena.boost_pads[0].respawn_at_tick = Some(160);

        let encoded = serde_json::to_vec(&game).unwrap();
        let decoded: GameState = serde_json::from_slice(&encoded).unwrap();
        decoded.validate_boost_invariants().unwrap();
        assert_eq!(decoded.sync_hash(), game.sync_hash());
        let json: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(json["arena"]["boost_pads"].as_array().unwrap().len(), 12);
        assert_eq!(json["arena"]["boost_pads"][0]["charge_ms"], 3_000);
        assert_eq!(json["arena"]["boost_pads"][0]["size_cells"], 2);
        assert_eq!(json["arena"]["boost_pads"][4]["charge_ms"], 750);
        assert_eq!(json["arena"]["boost_pads"][4]["size_cells"], 1);
    }

    fn pre_boost_team_payload(status: GameStatus) -> serde_json::Value {
        let mut game = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Competitive,
            Some(44),
            1_234,
        );
        game.add_player(7, Some("Legacy player".to_string()))
            .unwrap();
        game.status = status;

        let mut persisted = serde_json::to_value(game).unwrap();
        persisted
            .as_object_mut()
            .unwrap()
            .remove("player_action_counts");
        for field in [
            "player_last_activity_ticks",
            "idle_kicked_user_ids",
            "completed_by_inactivity",
        ] {
            persisted.as_object_mut().unwrap().remove(field);
        }
        persisted["properties"]["tick_duration_ms"] = serde_json::json!(100);
        persisted["properties"]["time_limit_ms"] = serde_json::json!(90_000);
        for field in [
            "score_limit",
            "boost",
            "player_idle_timeout_ms",
            "player_idle_warning_ms",
        ] {
            persisted["properties"]
                .as_object_mut()
                .unwrap()
                .remove(field);
        }
        persisted["arena"]
            .as_object_mut()
            .unwrap()
            .remove("boost_pads");
        for snake in persisted["arena"]["snakes"].as_array_mut().unwrap() {
            let snake = snake.as_object_mut().unwrap();
            snake.remove("speed_milli");
            snake.remove("movement_credit");
            snake.remove("boost");
        }
        persisted
    }

    #[test]
    fn pre_boost_completed_game_payload_remains_readable_from_durable_history() {
        let persisted = serde_json::to_string(&pre_boost_team_payload(GameStatus::Complete {
            winning_snake_id: Some(0),
        }))
        .unwrap();
        let decoded: GameState = serde_json::from_str(&persisted).unwrap();

        assert!(matches!(
            decoded.status,
            GameStatus::Complete {
                winning_snake_id: Some(0)
            }
        ));
        assert_eq!(decoded.properties.boost, None);
        assert!(decoded.arena.boost_pads.is_empty());
        assert!(decoded.player_action_counts.is_empty());
        assert!(decoded.player_last_activity_ticks.is_empty());
        assert!(decoded.idle_kicked_user_ids.is_empty());
        assert!(!decoded.completed_by_inactivity);
        assert_eq!(
            decoded.properties.player_idle_timeout_ms,
            DEFAULT_PLAYER_IDLE_TIMEOUT_MS
        );
        assert_eq!(
            decoded.properties.player_idle_warning_ms,
            DEFAULT_PLAYER_IDLE_WARNING_MS
        );
        let snake = &decoded.arena.snakes[0];
        assert_eq!(snake.speed_milli(), NORMAL_SNAKE_SPEED_MILLI);
        assert_eq!(snake.movement_credit(), 0);
        assert_eq!(*snake.boost(), Default::default());
        assert!(decoded.is_legacy_completed_snapshot());

        // The WebSocket completed-game path clones and serializes this state
        // directly, so prove the compatibility defaults also produce a full
        // current-protocol payload for the historical result.
        let current_wire = serde_json::to_value(&decoded).unwrap();
        assert_eq!(current_wire["player_action_counts"], serde_json::json!({}));
        assert_eq!(current_wire["arena"]["boost_pads"], serde_json::json!([]));
        assert_eq!(current_wire["properties"]["boost"], serde_json::Value::Null);
        assert_eq!(
            current_wire["arena"]["snakes"][0]["speed_milli"],
            NORMAL_SNAKE_SPEED_MILLI
        );
        assert_eq!(current_wire["arena"]["snakes"][0]["movement_credit"], 0);
        assert_eq!(
            current_wire["arena"]["snakes"][0]["boost"],
            serde_json::json!({ "charge_ms": 0, "active": false, "intent": false })
        );
    }

    /// A player holding Boost when their snake dies keeps the latched intent —
    /// that is what makes the control resume on the new life without a
    /// re-press. Snapshot admission must therefore accept a dead snake whose
    /// intent is still set, while still rejecting one that kept fuel, speed or
    /// movement phase.
    #[test]
    fn a_dead_snake_may_keep_latched_intent_but_nothing_else() {
        let mut game = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            None,
            0,
        );
        let snake_id = game.add_player(1, None).expect("add player").snake_id;
        game.add_player(2, None).expect("add rival");

        {
            let snake = &mut game.arena.snakes[snake_id as usize];
            snake.set_boost_intent(true);
            snake.is_alive = false;
            snake.reset_boost_and_movement(None);
        }
        assert!(game.arena.snakes[snake_id as usize].boost().intent);
        game.validate_boost_invariants()
            .expect("a dead snake holding the control is a legal state");

        // Fuel, activation and movement phase are all still forbidden.
        for corrupt in [
            (|s: &mut Snake| s.boost.charge_ms = 1_000) as fn(&mut Snake),
            |s: &mut Snake| s.boost.active = true,
            |s: &mut Snake| s.movement_credit = 1,
        ] {
            let mut broken = game.clone();
            corrupt(&mut broken.arena.snakes[snake_id as usize]);
            assert!(
                broken.validate_boost_invariants().is_err(),
                "a dead snake must not retain fuel, activation or movement phase"
            );
        }
    }

    #[test]
    fn pre_boost_active_team_payload_is_rejected_by_recovery_invariants() {
        let persisted = pre_boost_team_payload(GameStatus::Started { server_id: 9 });
        let decoded: GameState = serde_json::from_value(persisted).unwrap();
        let error = decoded.validate_boost_invariants().unwrap_err().to_string();
        assert!(
            error.contains("requires Boost configuration"),
            "unexpected fail-closed error: {error}"
        );
    }

    #[test]
    fn four_snake_two_x_saturated_command_stress_stays_within_quantum_budget() {
        // 90 seconds of simulation at the Boost quantum: the historical
        // match length, kept purely as this benchmark's workload budget now
        // that matches end on score rather than on the clock.
        const QUANTA: u32 = 90_000 / BOOST_TICK_INTERVAL_MS;
        let mut game = GameState::new_with_boost_config(
            60,
            40,
            GameType::TeamMatch { per_team: 2 },
            QueueMode::Competitive,
            Some(0xB0057),
            0,
            BoostConfig {
                speed_milli: MAX_BOOST_SPEED_MILLI,
                ..BoostConfig::default()
            },
        )
        .expect("2.0x stress configuration");
        for user_id in 1..=4 {
            game.add_player(user_id, None).expect("add stress player");
        }
        game.status = GameStatus::Started { server_id: 1 };
        game.rng = None;
        game.arena.food = vec![Position { x: 22, y: 20 }];

        // Four disjoint clockwise circuits keep ordinary movement, turn,
        // compressed-body, and collision work active for the whole run.
        let circuits = [
            (12, 19, 3, 8),
            (25, 32, 3, 8),
            (12, 19, 31, 36),
            (25, 32, 31, 36),
        ];
        for (snake, (min_x, _, min_y, _)) in game.arena.snakes.iter_mut().zip(circuits) {
            snake.body = vec![
                Position { x: min_x, y: min_y },
                Position {
                    x: min_x - 3,
                    y: min_y,
                },
            ];
            snake.direction = Direction::Right;
            snake.food = 0;
            snake.reset_boost_and_movement(None);
        }

        let config = game.properties.boost.clone().unwrap();
        let mut sequence = 1_u32;
        let mut quantum_durations = Vec::with_capacity(QUANTA as usize);
        let run_started = std::time::Instant::now();
        for _ in 0..QUANTA {
            let tick = game.tick;
            let mut heads_before = Vec::with_capacity(4);
            let mut commands = Vec::with_capacity(8);
            for (snake_id, ((snake, bounds), user_id)) in game
                .arena
                .snakes
                .iter_mut()
                .zip(circuits)
                .zip(1_u32..=4)
                .enumerate()
            {
                snake.collect_boost_charge(config.capacity_ms, config.capacity_ms);
                snake.set_boost_intent(true);
                snake.resolve_boost(config.speed_milli, config.capacity_ms);
                assert_eq!(snake.speed_milli, MAX_BOOST_SPEED_MILLI);
                assert!(snake.boost.active);
                heads_before.push(*snake.head().unwrap());

                let (min_x, max_x, min_y, max_y) = bounds;
                let direction = match snake.direction {
                    Direction::Right if snake.head().unwrap().x >= max_x => Direction::Down,
                    Direction::Down if snake.head().unwrap().y >= max_y => Direction::Left,
                    Direction::Left if snake.head().unwrap().x <= min_x => Direction::Up,
                    Direction::Up if snake.head().unwrap().y <= min_y => Direction::Right,
                    direction => direction,
                };
                let snake_id = snake_id as u32;
                commands.push(GameCommandMessage {
                    command_id_client: create_command_id(tick, user_id, sequence),
                    command_id_server: Some(create_command_id(tick, user_id, sequence)),
                    command: GameCommand::Turn {
                        snake_id,
                        direction,
                    },
                });
                sequence = sequence.wrapping_add(1);
                commands.push(GameCommandMessage {
                    command_id_client: create_command_id(tick, user_id, sequence),
                    command_id_server: Some(create_command_id(tick, user_id, sequence)),
                    command: GameCommand::ActivateBoost { snake_id },
                });
                sequence = sequence.wrapping_add(1);
            }
            for command in commands {
                game.schedule_command(&command);
            }

            let started = std::time::Instant::now();
            game.tick_forward(false).expect("stress quantum");
            quantum_durations.push(started.elapsed());

            for (snake, head_before) in game.arena.snakes.iter().zip(heads_before) {
                assert!(snake.is_alive, "disjoint stress circuit must remain alive");
                let head_after = *snake.head().unwrap();
                assert_eq!(
                    (head_after.x - head_before.x).abs() + (head_after.y - head_before.y).abs(),
                    1,
                    "2.0x snake must move exactly one cell per 50 ms quantum"
                );
                assert!(snake.boost.active);
                assert_eq!(snake.speed_milli, MAX_BOOST_SPEED_MILLI);
                assert_eq!(
                    snake.boost.charge_ms,
                    config.capacity_ms - BOOST_TICK_INTERVAL_MS,
                    "continuous-fuel harness must leave every snake funded"
                );
            }
            assert!(!game.has_scheduled_commands(tick));
        }

        assert_eq!(game.tick, QUANTA);
        game.validate_boost_invariants().unwrap();
        let run_duration = run_started.elapsed();
        quantum_durations.sort_unstable();
        let percentile = |percent: usize| {
            let index = (quantum_durations.len() * percent / 100)
                .min(quantum_durations.len().saturating_sub(1));
            quantum_durations[index]
        };
        let p50 = percentile(50);
        let p95 = percentile(95);
        let p99 = percentile(99);
        let max = *quantum_durations.last().unwrap();
        eprintln!(
            "four-snake continuous-fuel 2.0x: {QUANTA} quanta/{} moves in {run_duration:?}; p50={p50:?} p95={p95:?} p99={p99:?} max={max:?}",
            QUANTA * 4,
        );
        assert!(
            p99 < std::time::Duration::from_millis(25),
            "isolated four-snake engine p99 {p99:?} exceeded half the 50 ms quantum"
        );
    }
}

#[cfg(test)]
mod team_base_wire_tests {
    use super::*;

    /// The wire shape of `team_bases`, pinned.
    ///
    /// `TeamId` is a newtype over `u8`, and serde only accepts a map key that
    /// serializes as a string — a newtype delegates to its inner type, and an
    /// integer key is stringified. That is not obvious from the type, and the
    /// client's generated `Record<number, string>` is only correct if it holds,
    /// so it is asserted against real JSON rather than assumed.
    #[test]
    fn team_bases_round_trip_through_json_with_numeric_keys() {
        let mut state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 2 },
            QueueMode::Quickmatch,
            Some(7),
            0,
        );
        state.set_team_base(TeamId(0), Some("invaders@1".to_string()));
        state.set_team_base(TeamId(1), Some("dragon@1".to_string()));

        let json = serde_json::to_value(&state).expect("state serializes");
        assert_eq!(
            json["team_bases"],
            serde_json::json!({ "0": "invaders@1", "1": "dragon@1" }),
            "team keys must be stringified integers, which is what Record<number, string> means"
        );

        let restored: GameState = serde_json::from_value(json).expect("state deserializes");
        assert_eq!(restored.team_base(TeamId(0)), Some("invaders@1"));
        assert_eq!(restored.team_base(TeamId(1)), Some("dragon@1"));
    }

    /// Every state written before base skins existed, and every state from a
    /// build that does not know about them, has to load.
    #[test]
    fn a_state_without_the_field_deserializes_to_no_bases() {
        let state = GameState::new(
            60,
            40,
            GameType::TeamMatch { per_team: 2 },
            QueueMode::Quickmatch,
            Some(7),
            0,
        );
        let mut json = serde_json::to_value(&state).expect("state serializes");
        json.as_object_mut()
            .expect("a state is an object")
            .remove("team_bases");

        let restored: GameState = serde_json::from_value(json).expect("older states must load");
        assert!(restored.team_bases.is_empty());
        assert_eq!(restored.team_base(TeamId(0)), None);
    }
}
