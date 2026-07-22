//! Shared executor protocol types and command authorization.
//!
//! Authoritative execution lives in [`crate::game_executor_v2`]. Keeping the
//! wire event and authorization boundary in this small module avoids coupling
//! publishers and replicas to the executor runtime implementation.

use anyhow::Result;
use common::{ClientCommandIdentityV2, GameCommand, GameCommandMessage, GameState, GameStatus};
use serde::{Deserialize, Serialize};

pub const PARTITION_COUNT: u32 = 10;

// Snapshot-bearing events are message envelopes; boxing would add churn
// without a win.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum StreamEvent {
    GameCreated {
        game_id: u32,
        game_state: GameState,
    },
    GameCommandSubmittedV2 {
        game_id: u32,
        user_id: u32,
        command_id: ClientCommandIdentityV2,
        command: GameCommandMessage,
    },
    StatusUpdated {
        game_id: u32,
        status: GameStatus,
    },
}

/// Validates a client command against the authenticated WebSocket identity.
/// Every field in `GameCommandMessage` is client-controlled, including the
/// claimed user and target snake, so downstream execution receives only a
/// normalized, authorized command.
pub(crate) fn authorize_game_command(
    state: &GameState,
    user_id: u32,
    mut command: GameCommandMessage,
) -> Result<GameCommandMessage, &'static str> {
    match command.command {
        GameCommand::Turn { snake_id, .. } => match state.players.get(&user_id) {
            None => return Err("user is not a player in this game"),
            Some(player) if player.snake_id != snake_id => {
                return Err("snake is not owned by the submitting user");
            }
            Some(_) => {}
        },
        GameCommand::UpdateStatus { .. } => {
            return Err("system command submitted over a client connection");
        }
    }

    command.command_id_client.user_id = user_id;
    command.command_id_server = None;
    Ok(command)
}

#[cfg(test)]
mod tests {
    use super::authorize_game_command;
    use common::{
        CommandId, Direction, GameCommand, GameCommandMessage, GameState, GameStatus, GameType,
        QueueMode,
    };

    const USER_A: u32 = 11;
    const USER_B: u32 = 22;

    fn two_player_state() -> (GameState, u32, u32) {
        let mut state = GameState::new(
            30,
            30,
            GameType::FreeForAll { max_players: 4 },
            QueueMode::Quickmatch,
            Some(1),
            0,
        );
        let snake_a = state.add_player(USER_A, None).unwrap().snake_id;
        let snake_b = state.add_player(USER_B, None).unwrap().snake_id;
        (state, snake_a, snake_b)
    }

    fn turn_command(claimed_user_id: u32, snake_id: u32) -> GameCommandMessage {
        GameCommandMessage {
            command_id_client: CommandId {
                tick: 5,
                user_id: claimed_user_id,
                sequence_number: 0,
            },
            command_id_server: None,
            command: GameCommand::Turn {
                snake_id,
                direction: Direction::Up,
            },
        }
    }

    #[test]
    fn turn_for_unowned_snake_is_rejected() {
        let (state, _, snake_b) = two_player_state();
        assert!(authorize_game_command(&state, USER_A, turn_command(USER_A, snake_b)).is_err());
    }

    #[test]
    fn spoofed_user_cannot_drive_another_players_snake() {
        let (state, _, snake_b) = two_player_state();
        assert!(authorize_game_command(&state, USER_A, turn_command(USER_B, snake_b)).is_err());
    }

    #[test]
    fn own_snake_is_authorized_with_authenticated_identity() {
        let (state, snake_a, _) = two_player_state();
        let mut command = turn_command(USER_B, snake_a);
        command.command_id_server = Some(CommandId {
            tick: 9999,
            user_id: USER_B,
            sequence_number: 7,
        });

        let authorized = authorize_game_command(&state, USER_A, command).unwrap();
        assert_eq!(authorized.command_id_client.user_id, USER_A);
        assert_eq!(authorized.command_id_server, None);
    }

    #[test]
    fn spectator_turn_is_rejected() {
        let (state, snake_a, _) = two_player_state();
        assert!(authorize_game_command(&state, 99, turn_command(99, snake_a)).is_err());
    }

    #[test]
    fn client_status_update_is_rejected() {
        let (state, _, _) = two_player_state();
        let command = GameCommandMessage {
            command_id_client: CommandId {
                tick: 0,
                user_id: USER_A,
                sequence_number: 0,
            },
            command_id_server: None,
            command: GameCommand::UpdateStatus {
                status: GameStatus::Complete {
                    winning_snake_id: None,
                },
            },
        };
        assert!(authorize_game_command(&state, USER_A, command).is_err());
    }
}
