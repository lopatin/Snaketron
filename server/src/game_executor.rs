use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, error, warn, debug};
use common::{GameEngine, GameEvent, GameEventMessage, GameStatus, CommandId};
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

    let mut interval = tokio::time::interval(Duration::from_millis(50));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    
    let mut last_polled_tick = 0u64;
    
    loop {
        tokio::select! {
            biased;
            
            _ = cancellation_token.cancelled() => {
                info!("Game loop for game {} shutting down", game_id);
                break;
            }
            
            _ = interval.tick() => {
                // Poll for new commands from Raft
                let commands = raft.get_commands_for_game(game_id, last_polled_tick).await;
                if !commands.is_empty() {
                    debug!("Polled {} commands for game {}", commands.len(), game_id);
                    
                    // Update last polled tick to the highest tick we've seen
                    for (_, tick) in &commands {
                        if *tick > last_polled_tick {
                            last_polled_tick = *tick;
                        }
                    }
                    
                    // Schedule commands in the game engine
                    // We need to access the game state's command queue
                    // Since we can't modify the engine directly, we'll need to emit CommandScheduled events
                    for (command, submitted_tick) in commands {
                        // Calculate when command should execute (e.g., submitted_tick + 2 for latency)
                        let execution_tick = engine.current_tick() + 2; // Simple latency compensation
                        
                        // Update the command with server-side tick information
                        let mut server_command = command;
                        server_command.command_id_server = Some(CommandId {
                            tick: execution_tick,
                            user_id: server_command.command_id_client.user_id,
                            sequence_number: server_command.command_id_client.sequence_number,
                        });
                        
                        // Emit CommandScheduled event
                        let event = GameEvent::CommandScheduled { command_message: server_command };
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
                }
                
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
                            
                            match raft.propose(ClientRequest::ProcessGameEvent(event_msg.clone())).await {
                                Ok(_) => {
                                    debug!(game_id, "Published game event: {:?}", event_msg);
                                }
                                Err(e) => {
                                    warn!(game_id, error = %e, "Failed to publish game event");
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
                    
                    StateChangeEvent::GameCommandSubmitted { game_id, user_id, command, tick_submitted } => {
                        // The game executor will poll for these commands in the game loop
                        debug!("Command submitted for game {} by user {} at tick {}", game_id, user_id, tick_submitted);
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