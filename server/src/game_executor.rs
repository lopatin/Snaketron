use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, error, warn, debug};
use common::{GameEngine, GameEvent, GameEventMessage, GameStatus};
use crate::{
    raft::{RaftNode, StateChangeEvent},
};
use crate::raft::ClientRequest;


/// Create a game engine and run the game loop for a specific game.
async fn run_game(
    server_id: u64,
    game_id: u32,
    raft: Arc<RaftNode>,
    cancellation_token: CancellationToken,
) {
    info!("run_game called for game {}", game_id);
    
    // Get the game state from Raft
    let game_state = match raft.get_game_state(game_id).await {
        Some(state) => state,
        None => {
            error!("Game {} not found in Raft state", game_id);
            return;
        }
    };
    
    // Create the game engine from the existing game state
    let start_ms = chrono::Utc::now().timestamp_millis();
    let mut engine = GameEngine::new_from_state(game_id, start_ms, game_state);
    info!("Created game engine for game {} from existing state", game_id);

    // Subscribe to state change events
    let mut state_rx = raft.subscribe_state_events();

    let mut interval = tokio::time::interval(Duration::from_millis(50));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    
    loop {
        tokio::select! {
            biased;
            
            _ = cancellation_token.cancelled() => {
                info!("Game loop for game {} shutting down", game_id);
                break;
            }
            
            // Process state change events
            Ok(event) = state_rx.recv() => {
                match event {
                    StateChangeEvent::GameCommandSubmitted { 
                        game_id: cmd_game_id, 
                        user_id, 
                        command
                    } if cmd_game_id == game_id => {
                        debug!("Processing command for game {} from user {}", game_id, user_id);
                        
                        // Process the command through the game engine
                        match engine.process_command(command) {
                            Ok(scheduled_command) => {
                                // Emit CommandScheduled event with the server-scheduled command
                                let event = GameEvent::CommandScheduled { command_message: scheduled_command };
                                let event_msg = GameEventMessage {
                                    game_id,
                                    tick: engine.current_tick(),
                                    user_id: None,
                                    event,
                                };
                                
                                match raft.propose(ClientRequest::ProcessGameEvent(event_msg.clone())).await {
                                    Ok(_) => {
                                        debug!(game_id, "Scheduled command for execution");
                                    }
                                    Err(e) => {
                                        warn!(game_id, error = %e, "Failed to schedule command");
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Failed to process command for game {}: {:?}", game_id, e);
                            }
                        }
                    }
                    _ => {
                        // Ignore other state change events
                    }
                }
            }
            
            _ = interval.tick() => {
                // Run game ticks
                let now_ms = chrono::Utc::now().timestamp_millis();
                match engine.run_until(now_ms) {
                    Ok(events) => {
                        for (tick, event) in events {
                            let event_msg = GameEventMessage {
                                game_id,
                                tick,
                                user_id: None,
                                event: event.clone(),
                            };
                            
                            match raft.propose(ClientRequest::ProcessGameEvent(event_msg.clone())).await {
                                Ok(_) => {
                                    debug!(game_id, "Published game event: {:?}", event_msg);
                                }
                                Err(e) => {
                                    warn!(game_id, error = %e, "Failed to publish game event");
                                }
                            }
                        }
                        
                        // Check for solo game end
                        if let Some(game_state) = raft.get_game_state(game_id).await {
                            if game_state.game_type.is_solo() && game_state.players.len() == 1 {
                                // Get the single player
                                if let Some((user_id, player)) = game_state.players.iter().next() {
                                    if let Some(snake) = game_state.arena.snakes.get(player.snake_id as usize) {
                                        if !snake.is_alive {
                                            // Calculate score: actual snake length - starting length
                                            let starting_length = match &game_state.game_type {
                                                common::GameType::Custom { settings } => settings.snake_start_length as usize,
                                                _ => 4,  // Default starting length
                                            };
                                            let score = snake.length().saturating_sub(starting_length) as u32;
                                            let duration = engine.current_tick();
                                            
                                            // Send solo game ended event
                                            let event = GameEvent::SoloGameEnded { score, duration };
                                            let event_msg = GameEventMessage {
                                                game_id,
                                                tick: engine.current_tick(),
                                                user_id: Some(*user_id),
                                                event,
                                            };
                                            
                                            if let Err(e) = raft.propose(ClientRequest::ProcessGameEvent(event_msg)).await {
                                                warn!("Failed to publish solo game ended event: {}", e);
                                            }
                                            
                                            // Exit the game loop for solo games
                                            info!("Solo game {} ended. Score: {}, Duration: {} ticks", game_id, score, duration);
                                            break;
                                        }
                                    }
                                }
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

    let mut state_rx = raft.subscribe_state_events();

    let raft_clone = raft.clone();
    let cancellation_token_clone = cancellation_token.clone();
    let try_start_game = move |game_id: u32| {
        let raft = raft_clone.clone();
        let cancellation_token = cancellation_token_clone.clone();
        tokio::spawn(async move {
            match raft.propose(ClientRequest::StartGame { game_id, server_id }).await {
                Ok(_response) => {
                    // Run the game loop here.
                    run_game(server_id, game_id, raft.clone(), cancellation_token.clone()).await;
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
                            GameEvent::StatusUpdated { status: GameStatus::Stopped } => {
                                try_start_game(event.game_id);
                            }
                            
                            _ => {
                                // Handle other game events
                                debug!("Received game event: {:?}", event);
                            }
                        }
                    }
                    
                    StateChangeEvent::GameCommandSubmitted { game_id, user_id, command } => {
                        // The game executor will poll for these commands in the game loop
                        debug!("Command submitted for game {} by user {}", game_id, user_id);
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