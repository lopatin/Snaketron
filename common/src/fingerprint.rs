//! Deterministic fingerprinting of sync-critical game state.
//!
//! `GameState::sync_hash` produces a 64-bit digest of everything that must be
//! identical between the server's authoritative state and a client's committed
//! state at the same tick. The server broadcasts its hash periodically via
//! `GameEvent::TickHash`; clients recompute the hash locally at that tick and
//! compare, turning silent state divergence into a detectable, reportable event.
//!
//! Deliberately excluded from the hash:
//! - `rng`: the client never spawns food, so its RNG state is not advanced.
//! - `command_queue`: legitimately differs while a local command is in flight
//!   (scheduled optimistically on the client, not yet confirmed by the server).
//!   Divergence caused by lost/mis-scheduled commands still surfaces through
//!   snake direction/body positions once the command executes.
//! - `event_sequence`: client and server count locally-generated events
//!   differently by design.
//! - `usernames`, `spectators`, `game_code`, `host_user_id`, `start_ms`:
//!   cosmetic or static; not gameplay state.

use crate::game_state::{GameState, GameStatus};

/// FNV-1a 64-bit, hand-rolled so both native and WASM builds hash identically
/// with no dependencies. All multi-byte values are hashed little-endian.
#[derive(Debug, Clone)]
pub struct SyncHasher {
    state: u64,
}

const FNV_OFFSET: u64 = 0xcbf29ce484222325;
const FNV_PRIME: u64 = 0x100000001b3;

impl Default for SyncHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl SyncHasher {
    pub fn new() -> Self {
        SyncHasher { state: FNV_OFFSET }
    }

    pub fn write_u8(&mut self, v: u8) {
        self.state ^= v as u64;
        self.state = self.state.wrapping_mul(FNV_PRIME);
    }

    pub fn write_bytes(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.write_u8(*b);
        }
    }

    pub fn write_u16(&mut self, v: u16) {
        self.write_bytes(&v.to_le_bytes());
    }

    pub fn write_u32(&mut self, v: u32) {
        self.write_bytes(&v.to_le_bytes());
    }

    pub fn write_u64(&mut self, v: u64) {
        self.write_bytes(&v.to_le_bytes());
    }

    pub fn write_i16(&mut self, v: i16) {
        self.write_bytes(&v.to_le_bytes());
    }

    pub fn finish(&self) -> u64 {
        self.state
    }
}

impl GameState {
    /// Deterministic digest of the sync-critical state. Two states with equal
    /// hashes at the same tick are gameplay-equivalent; a mismatch means the
    /// client has diverged from the server and needs a resync.
    pub fn sync_hash(&self) -> u64 {
        let mut h = SyncHasher::new();

        h.write_u32(self.tick);

        // Status: discriminant plus gameplay-relevant payload. `server_id` is
        // excluded so a mid-game failover does not read as divergence.
        match &self.status {
            GameStatus::Stopped => h.write_u8(0),
            GameStatus::Started { .. } => h.write_u8(1),
            GameStatus::Complete { winning_snake_id } => {
                h.write_u8(2);
                match winning_snake_id {
                    Some(id) => {
                        h.write_u8(1);
                        h.write_u32(*id);
                    }
                    None => h.write_u8(0),
                }
            }
        }

        h.write_u16(self.arena.width);
        h.write_u16(self.arena.height);
        if let Some(zone) = &self.arena.team_zone_config {
            h.write_u8(1);
            h.write_u16(zone.end_zone_depth);
            h.write_u16(zone.goal_width);
        } else {
            h.write_u8(0);
        }

        // Snakes: Vec index is the snake id, so order is canonical already.
        h.write_u32(self.arena.snakes.len() as u32);
        for snake in &self.arena.snakes {
            h.write_u8(snake.is_alive as u8);
            h.write_u8(direction_tag(&snake.direction));
            h.write_u32(snake.food);
            match snake.team_id {
                Some(team) => {
                    h.write_u8(1);
                    h.write_u8(team.0);
                }
                None => h.write_u8(0),
            }
            h.write_u32(snake.body.len() as u32);
            for pos in &snake.body {
                h.write_i16(pos.x);
                h.write_i16(pos.y);
            }
        }

        // Food is a set for gameplay purposes; sort so that removal-order
        // differences between server and client cannot cause false mismatches.
        let mut food: Vec<(i16, i16)> = self.arena.food.iter().map(|p| (p.x, p.y)).collect();
        food.sort_unstable();
        h.write_u32(food.len() as u32);
        for (x, y) in food {
            h.write_i16(x);
            h.write_i16(y);
        }

        // HashMaps are hashed in sorted key order for determinism.
        let mut players: Vec<(u32, u32)> = self
            .players
            .iter()
            .map(|(user_id, p)| (*user_id, p.snake_id))
            .collect();
        players.sort_unstable();
        h.write_u32(players.len() as u32);
        for (user_id, snake_id) in players {
            h.write_u32(user_id);
            h.write_u32(snake_id);
        }

        let mut scores: Vec<(u32, u32)> = self.scores.iter().map(|(k, v)| (*k, *v)).collect();
        scores.sort_unstable();
        h.write_u32(scores.len() as u32);
        for (snake_id, score) in scores {
            h.write_u32(snake_id);
            h.write_u32(score);
        }

        match &self.team_scores {
            Some(team_scores) => {
                let mut ts: Vec<(u8, u32)> = team_scores.iter().map(|(k, v)| (k.0, *v)).collect();
                ts.sort_unstable();
                h.write_u8(1);
                h.write_u32(ts.len() as u32);
                for (team, score) in ts {
                    h.write_u8(team);
                    h.write_u32(score);
                }
            }
            None => h.write_u8(0),
        }

        let mut xp: Vec<(u32, u32)> = self.player_xp.iter().map(|(k, v)| (*k, *v)).collect();
        xp.sort_unstable();
        h.write_u32(xp.len() as u32);
        for (user_id, amount) in xp {
            h.write_u32(user_id);
            h.write_u32(amount);
        }

        h.write_u64(self.properties.available_food_target as u64);
        h.write_u32(self.properties.tick_duration_ms);
        match self.properties.time_limit_ms {
            Some(limit) => {
                h.write_u8(1);
                h.write_u32(limit);
            }
            None => h.write_u8(0),
        }

        h.finish()
    }
}

fn direction_tag(direction: &crate::Direction) -> u8 {
    match direction {
        crate::Direction::Up => 0,
        crate::Direction::Down => 1,
        crate::Direction::Left => 2,
        crate::Direction::Right => 3,
    }
}

#[cfg(test)]
mod tests {
    use crate::{GameState, GameType, Position, QueueMode};

    fn test_state() -> GameState {
        GameState::new(
            20,
            20,
            GameType::TeamMatch { per_team: 1 },
            QueueMode::Quickmatch,
            Some(42),
            1_000,
        )
    }

    #[test]
    fn hash_is_stable_across_recomputation() {
        let state = test_state();
        assert_eq!(state.sync_hash(), state.sync_hash());
    }

    #[test]
    fn hash_ignores_food_order() {
        let mut a = test_state();
        let mut b = test_state();
        a.arena.food = vec![Position { x: 1, y: 2 }, Position { x: 3, y: 4 }];
        b.arena.food = vec![Position { x: 3, y: 4 }, Position { x: 1, y: 2 }];
        assert_eq!(a.sync_hash(), b.sync_hash());
    }

    #[test]
    fn hash_detects_food_divergence() {
        let mut a = test_state();
        let mut b = test_state();
        a.arena.food = vec![Position { x: 1, y: 2 }];
        b.arena.food = vec![Position { x: 1, y: 3 }];
        assert_ne!(a.sync_hash(), b.sync_hash());
    }

    #[test]
    fn hash_ignores_rng_and_command_queue() {
        let mut a = test_state();
        let b = test_state();
        a.rng = None;
        assert_eq!(a.sync_hash(), b.sync_hash());
    }

    #[test]
    fn hash_detects_tick_difference() {
        let mut a = test_state();
        let b = test_state();
        a.tick += 1;
        assert_ne!(a.sync_hash(), b.sync_hash());
    }
}
