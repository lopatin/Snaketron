use anyhow::{Context, Result};
use std::sync::Arc;
use std::collections::HashSet;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc, oneshot, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{info, error, warn, debug};
use sqlx::PgPool;
use common::GameEvent::StatusUpdated;
use common::{GameCommandMessage, GameEngine, GameEvent, GameEventMessage, GameState, GameStatus};
use crate::{
    game_manager::GameManager,
    player_connections::PlayerConnectionManager,
    raft::{RaftNode, StateChangeEvent},
};
use crate::game_broker::GameMessageBroker;
use crate::raft::ClientRequest;
use crate::replica_manager::ReplicaManager;


/// Create a game engine and run the game loop for a specific game.
async fn run_game(
    server_id: u64,
    game_id: u32,
    raft: Arc<RaftNode>,
    cancellation_token: CancellationToken,
) {
    info!("run_game called for game {}", game_id);
    
    // Create the game engine
    let start_ms = chrono::Utc::now().timestamp_millis();
    let rng_seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    
    let mut engine = GameEngine::new_with_seed(game_id, start_ms, rng_seed);
    info!("Created game engine for game {}", game_id);

    let mut interval = tokio::time::interval(Duration::from_millis(50));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            
            _ = cancellation_token.cancelled() => {
                info!("Game loop for game {} shutting down", game_id);
                break;
            }
            
            _ = interval.tick() => {
                // Run game ticks
                let now_ms = chrono::Utc::now().timestamp_millis();
                match engine.run_until(now_ms) {
                    Ok(events) => {
                        for event in events {
                            let event_msg = GameEventMessage {
                                game_id,
                                tick: engine.current_tick(),
                                user_id: None,
                                event,
                            };
                            
                            if let Err(e) = raft.propose(ClientRequest::ProcessGameEvent(event_msg.clone())).await
                                    .expect("Failed to propose game event") {
                                warn!(game_id, error = %e, "Failed to publish game event");
                            } else {
                                debug!(game_id, "Published game event: {:?}", event_msg);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error running game tick: {:?}", e);
                    }
                }
            }
        }
    }
}


/// Run the game executor service
pub async fn run_game_executor(
    server_id: u64,
    raft: Arc<RaftNode>,
    cancellation_token: CancellationToken,
) -> Result<()> {
    info!("Starting game executor for server {}", server_id);

    let mut state_rx = raft.subscribe_state_events().await;

    let try_start_game = |game_id: u32| {
        tokio::spawn(async move {
            match raft.propose(ClientRequest::StartGame { game_id, server_id }).await {
                Ok(response) => {
                    // Run the game loop here.
                },
                Err(e) => error!("Failed to start game {} on server {}: {}", game_id, server_id, e),
            }
        });
    };

    loop {
        tokio::select! {
            biased;
            
            _ = cancellation_token.cancelled() => {
                info!("Game executor service shutting down");
                break;
            }
            
            Ok(event) = state_rx.recv() => {
                match event {
                    StateChangeEvent::GameCreated { game_id } => {
                        try_start_game(game_id);
                    }
                    
                    StateChangeEvent::GameEvent { event } => {
                        match event.event {
                            StatusUpdated { status: GameStatus::Stopped } => {
                                try_start_game(event.game_id);
                            }
                            
                            _ => {
                                // Handle other game events
                                debug!("Received game event: {:?}", event);
                            }
                        }
                    }
                    
                    _ => {
                        // Handle other state changes
                        debug!("Received state change event: {:?}", event);
                    }
                }
            }
        }
    }

    Ok(())
}