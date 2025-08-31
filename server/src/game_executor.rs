use anyhow::{Result, Context};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, error, warn, debug};
use common::{GameEngine, GameEvent, GameEventMessage, GameStatus, GameCommandMessage, GameState, EXECUTOR_POLL_INTERVAL_MS};
use serde::{Serialize, Deserialize};
use tokio::sync::mpsc;
use std::collections::HashMap;
use crate::pubsub_manager::{PubSubManager, SnapshotRequest, PartitionSubscription};

pub const PARTITION_COUNT: u32 = 10;
pub const SNAPSHOT_INTERVAL_TICKS: u32 = 10; // Publish snapshot every 10 ticks (1 second at 100ms tick rate)

/// Events that can be sent through PubSub
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum StreamEvent {
    GameCreated { 
        game_id: u32, 
        game_state: GameState 
    },
    GameCommandSubmitted { 
        game_id: u32, 
        user_id: u32, 
        command: GameCommandMessage 
    },
    GameEvent(GameEventMessage),
    StatusUpdated { 
        game_id: u32, 
        status: GameStatus 
    },
}

/// Create a game engine and run the game loop for a specific game.
async fn run_game(
    server_id: u64,
    game_id: u32,
    game_state: GameState,
    mut pubsub: PubSubManager,
    mut command_receiver: mpsc::Receiver<GameCommandMessage>,
    mut snapshot_request_receiver: mpsc::Receiver<SnapshotRequest>,
    cancellation_token: CancellationToken,
) {
    info!("run_game called for game {}", game_id);
    let partition_id = game_id % PARTITION_COUNT;
    
    // Create the game engine from the provided game state
    let _start_ms = chrono::Utc::now().timestamp_millis();
    
    // If the game is in Stopped status, start it before creating the engine
    let mut initial_state = game_state;
    if matches!(initial_state.status, GameStatus::Stopped) {
        info!("Game {} is in Stopped status, starting it", game_id);
        initial_state.status = GameStatus::Started { server_id };
        
        // Emit status update event
        let status_event = GameEventMessage {
            game_id,
            tick: initial_state.tick,
            sequence: initial_state.event_sequence + 1,
            user_id: None,
            event: GameEvent::StatusUpdated { 
                status: GameStatus::Started { server_id }
            },
        };
        
        if let Err(e) = pubsub.publish_event(partition_id, &status_event).await {
            error!("Failed to publish game started status: {}", e);
        }
    }
    
    let mut engine = GameEngine::new_from_state(game_id, initial_state);
    info!("Created game engine for game {} with status: {:?}", game_id, engine.get_committed_state().status);

    // Publish initial snapshot
    if let Err(e) = pubsub.publish_snapshot(partition_id, game_id, &engine.get_committed_state()).await {
        error!("Failed to publish initial snapshot: {}", e);
    }

    let mut interval = tokio::time::interval(Duration::from_millis(EXECUTOR_POLL_INTERVAL_MS));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    
    let mut last_snapshot_tick = 0u32;
    
    loop {
        tokio::select! {
            biased;
            
            _ = cancellation_token.cancelled() => {
                info!("Game loop for game {} shutting down", game_id);
                break;
            }
            
            // Handle snapshot requests
            Some(request) = snapshot_request_receiver.recv() => {
                debug!("Received snapshot request for partition {}", request.partition_id);
                // Only publish snapshot if this game belongs to the requested partition
                if game_id % PARTITION_COUNT == request.partition_id {
                    let snapshot = engine.get_committed_state();
                    if let Err(e) = pubsub.publish_snapshot(partition_id, game_id, &snapshot).await {
                        error!("Failed to publish requested snapshot: {}", e);
                    }
                }
            }
            
            // Process commands from the channel
            Some(command) = command_receiver.recv() => {
                debug!("Processing command for game {}. Command: {:?}", 
                    game_id, command);
                
                // Process the command through the game engine
                match engine.process_command(command) {
                    Ok(scheduled_command) => {
                        // Emit CommandScheduled event
                        let event = GameEvent::CommandScheduled { command_message: scheduled_command };
                        let current_state = engine.get_committed_state();
                        let event_msg = GameEventMessage {
                            game_id,
                            tick: engine.current_tick(),
                            sequence: current_state.event_sequence + 1,
                            user_id: None,
                            event,
                        };
                        
                        // Publish event via PubSub
                        if let Err(e) = pubsub.publish_event(game_id, &event_msg).await {
                            warn!("Failed to publish command scheduled event: {}", e);
                        }
                    }
                    Err(e) => {
                        warn!("Failed to process command for game {}: {:?}", game_id, e);
                    }
                }
            }
            
            _ = interval.tick() => {
                // Run game ticks
                let now_ms = chrono::Utc::now().timestamp_millis();
                match engine.run_until(now_ms) {
                    Ok(events) => {
                        for (tick, sequence, event) in &events {
                            let event_msg = GameEventMessage {
                                game_id,
                                tick: *tick,
                                sequence: *sequence,
                                user_id: None,
                                event: event.clone(),
                            };
                            
                            // Publish event via PubSub
                            if let Err(e) = pubsub.publish_event(game_id, &event_msg).await {
                                warn!("Failed to publish game event: {}", e);
                            }
                        }
                        
                        // Publish periodic snapshots
                        let current_tick = engine.current_tick();
                        if current_tick >= last_snapshot_tick + SNAPSHOT_INTERVAL_TICKS {
                            let snapshot = engine.get_committed_state();
                            if let Err(e) = pubsub.publish_snapshot(partition_id, game_id, &snapshot).await {
                                warn!("Failed to publish periodic snapshot: {}", e);
                            }
                            last_snapshot_tick = current_tick;
                        }
                        
                        // Check if game has completed
                        let game_state = engine.get_committed_state();
                        if matches!(game_state.status, GameStatus::Complete { .. }) {
                            info!("Game {} has completed, exiting game loop", game_id);
                            // Publish final snapshot
                            if let Err(e) = pubsub.publish_snapshot(partition_id, game_id, &game_state).await {
                                warn!("Failed to publish final snapshot: {}", e);
                            }
                            break;
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

/// Helper function to publish events via PubSub (for backward compatibility)
pub async fn publish_to_stream(
    pubsub: &mut PubSubManager,
    _partition_id: u32,
    event: &StreamEvent,
) -> Result<()> {
    // Convert StreamEvent to appropriate PubSub message
    match event {
        StreamEvent::GameEvent(event_msg) => {
            let partition_id = event_msg.game_id % PARTITION_COUNT;
            pubsub.publish_event(partition_id, event_msg).await?;
        }
        StreamEvent::GameCreated { game_id, game_state } => {
            let partition_id = game_id % PARTITION_COUNT;
            // Publish initial snapshot when game is created
            pubsub.publish_snapshot(partition_id, *game_id, game_state).await?;
            
            // Also publish a GameCreated event
            let event_msg = GameEventMessage {
                game_id: *game_id,
                tick: 0,
                sequence: 0,
                user_id: None,
                event: GameEvent::Snapshot { game_state: game_state.clone() },
            };
            pubsub.publish_event(partition_id, &event_msg).await?;
        }
        StreamEvent::StatusUpdated { game_id, status } => {
            let partition_id = game_id % PARTITION_COUNT;
            // Publish status update as an event
            let event_msg = GameEventMessage {
                game_id: *game_id,
                tick: 0,
                sequence: 0,
                user_id: None,
                event: GameEvent::StatusUpdated { status: status.clone() },
            };
            pubsub.publish_event(partition_id, &event_msg).await?;
        }
        StreamEvent::GameCommandSubmitted { game_id, user_id: _, command } => {
            let partition_id = game_id % PARTITION_COUNT;
            // Commands are sent directly to the game via channel, not through PubSub
            // This is handled differently now
            let serialized = serde_json::to_vec(command)?;
            pubsub.publish_command(partition_id, &serialized).await?;
        }
    }
    
    Ok(())
}

/// Run the game executor service for a specific partition
pub async fn run_game_executor(
    server_id: u64,
    partition_id: u32,
    redis_url: String,
    _replication_manager: Arc<crate::replication::ReplicationManager>,
    cancellation_token: CancellationToken,
) -> Result<()> {
    info!("Starting game executor for server {} partition {}", server_id, partition_id);

    // Create PubSub manager
    let mut pubsub = PubSubManager::new(&redis_url).await
        .context("Failed to create PubSub manager")?;
    
    // Subscribe to partition commands and snapshot requests
    let partition_sub = pubsub.subscribe_to_partition(partition_id).await
        .context("Failed to subscribe to partition")?;
    
    let PartitionSubscription { 
        partition_id: _,
        mut event_receiver,
        mut command_receiver,
        mut snapshot_request_receiver
    } = partition_sub;
    
    // Track game channels
    let mut game_channels: HashMap<u32, (mpsc::Sender<GameCommandMessage>, mpsc::Sender<SnapshotRequest>)> = HashMap::new();
    
    let try_start_game = |game_id: u32, 
                          game_state: GameState, 
                          pubsub: PubSubManager, 
                          cancellation_token: CancellationToken, 
                          game_channels: &mut HashMap<u32, (mpsc::Sender<GameCommandMessage>, mpsc::Sender<SnapshotRequest>)>| {
        if game_id % PARTITION_COUNT != partition_id {
            debug!("Game {} belongs to partition {}, not partition {}", game_id, game_id % PARTITION_COUNT, partition_id);
            return;
        }
        
        if game_channels.contains_key(&game_id) {
            debug!("Game {} is already running", game_id);
            return;
        }
        
        info!("Partition {} will start game {}", partition_id, game_id);
        
        // Create channels for this game
        let (cmd_tx, cmd_rx) = mpsc::channel(100);
        let (snap_tx, snap_rx) = mpsc::channel(10);
        game_channels.insert(game_id, (cmd_tx, snap_tx));
        
        tokio::spawn(async move {
            // Run the game loop
            run_game(server_id, game_id, game_state, pubsub, cmd_rx, snap_rx, cancellation_token).await;
            info!("Game {} has ended", game_id);
        });
    };

    loop {
        tokio::select! {
            biased;
            
            _ = cancellation_token.cancelled() => {
                info!("Game executor service shutting down");
                break;
            }
            
            // Process events from partition channel  
            Some(event) = event_receiver.recv() => {
                // Events flow through to replication manager automatically via PubSub
                // The replication manager is subscribed to the same partition channel
                // We just need to publish snapshots to PubSub
                if let GameEvent::Snapshot { .. } = &event.event {
                    debug!("Received snapshot event for game {} on partition {}", event.game_id, partition_id);
                }
            }
            
            // Process snapshot requests
            Some(request) = snapshot_request_receiver.recv() => {
                debug!("Received snapshot request for partition {}", request.partition_id);
                // Forward to all games in this partition
                if request.partition_id == partition_id {
                    for (game_id, (_, snap_tx)) in game_channels.iter() {
                        let _ = snap_tx.send(request.clone()).await;
                    }
                }
            }
            
            // Process commands from PubSub
            Some(command_data) = command_receiver.recv() => {
                // Deserialize and process the command
                match serde_json::from_slice::<StreamEvent>(&command_data) {
                    Ok(event) => {
                        match event {
                            StreamEvent::GameCreated { game_id, game_state } => {
                                info!("Received GameCreated event for game {}", game_id);
                                let pubsub_clone = pubsub.clone();
                                let cancellation_token_clone = cancellation_token.clone();
                                try_start_game(
                                    game_id, 
                                    game_state, 
                                    pubsub_clone, 
                                    cancellation_token_clone, 
                                    &mut game_channels
                                );
                            }
                            StreamEvent::StatusUpdated { game_id, status } => {
                                match status {
                                    GameStatus::Complete { .. } => {
                                        // Game completed, remove channels
                                        game_channels.remove(&game_id);
                                        info!("Game {} completed", game_id);
                                    }
                                    _ => {}
                                }
                            }
                            StreamEvent::GameCommandSubmitted { game_id, user_id: _, command } => {
                                // Route command to the appropriate game
                                if let Some((cmd_tx, _)) = game_channels.get(&game_id) {
                                    if let Err(e) = cmd_tx.send(command).await {
                                        warn!("Failed to send command to game {}: {}", game_id, e);
                                        // The game might have ended, remove from channels
                                        game_channels.remove(&game_id);
                                    }
                                } else {
                                    debug!("Received command for inactive game {}", game_id);
                                }
                            }
                            _ => {
                                debug!("Received other event in partition executor: {:?}", event);
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to deserialize command: {}", e);
                    }
                }
            }
        }
    }

    Ok(())
}