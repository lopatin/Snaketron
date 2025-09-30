use anyhow::Result;
use std::collections::HashMap;
use tracing::{info, error};
use crate::db::Database;

/// Persist XP gains for all players in a completed game to the database.
/// Uses atomic ADD operations in DynamoDB to prevent race conditions.
///
/// # Arguments
/// * `db` - Database interface
/// * `game_id` - The ID of the completed game
/// * `player_xp` - Map of user_id -> xp_gained
pub async fn persist_player_xp(
    db: &dyn Database,
    game_id: u32,
    player_xp: HashMap<u32, u32>
) -> Result<()> {
    if player_xp.is_empty() {
        info!("No XP to persist for game {}", game_id);
        return Ok(());
    }

    info!("Persisting XP for game {} with {} players", game_id, player_xp.len());

    for (user_id, xp_gained) in player_xp {
        if xp_gained == 0 {
            continue;  // Skip if no XP gained
        }

        match db.add_user_xp(user_id as i32, xp_gained as i32).await {
            Ok(new_total) => {
                info!(
                    "User {} gained {} XP from game {} (new total: {})",
                    user_id, xp_gained, game_id, new_total
                );
            }
            Err(e) => {
                error!(
                    "Failed to persist {} XP for user {} from game {}: {:?}",
                    xp_gained, user_id, game_id, e
                );
                // Don't fail the whole operation if one user fails
                // This ensures other players still get their XP
            }
        }
    }

    info!("Finished persisting XP for game {}", game_id);
    Ok(())
}