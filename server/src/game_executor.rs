use anyhow::{Result, Context};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::{info, error, warn, debug};
use common::{GameEngine, GameEvent, GameEventMessage, GameStatus, GameCommandMessage, GameState};
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, streams::{StreamReadOptions, StreamReadReply}};
use serde::{Serialize, Deserialize};
use tokio::sync::mpsc;
use std::collections::HashMap;

pub const PARTITION_COUNT: u32 = 10;

/// Events that can be sent through Redis streams
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
    mut redis_conn: ConnectionManager,
    stream_key: String,
    mut command_receiver: mpsc::Receiver<GameCommandMessage>,
    cancellation_token: CancellationToken,
) {
    info!("run_game called for game {} on stream {}", game_id, stream_key);
    
    // Create the game engine from the provided game state
    let start_ms = chrono::Utc::now().timestamp_millis();
    
    // If the game is in Stopped status, start it before creating the engine
    let mut initial_state = game_state;
    if matches!(initial_state.status, GameStatus::Stopped) {
        info!("Game {} is in Stopped status, starting it", game_id);
        initial_state.status = GameStatus::Started { server_id };
        
        // Emit status update to notify other components
        let status_event = StreamEvent::StatusUpdated {
            game_id,
            status: GameStatus::Started { server_id },
        };
        
        if let Err(e) = publish_to_stream(&mut redis_conn, &stream_key, &status_event).await {
            error!("Failed to publish game started status: {}", e);
        }
    }
    
    let mut engine = GameEngine::new_from_state(game_id, start_ms, initial_state);
    info!("Created game engine for game {} with status: {:?}", game_id, engine.get_committed_state().status);

    let mut interval = tokio::time::interval(Duration::from_millis(100));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    
    loop {
        tokio::select! {
            biased;
            
            _ = cancellation_token.cancelled() => {
                info!("Game loop for game {} shutting down", game_id);
                break;
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
                        let event_msg = GameEventMessage {
                            game_id,
                            tick: engine.current_tick(),
                            user_id: None,
                            event,
                        };
                        
                        // Publish to Redis stream
                        let stream_event = StreamEvent::GameEvent(event_msg);
                        if let Err(e) = publish_to_stream(&mut redis_conn, &stream_key, &stream_event).await {
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
                        for (tick, event) in &events {
                            let event_msg = GameEventMessage {
                                game_id,
                                tick: *tick,
                                user_id: None,
                                event: event.clone(),
                            };
                            
                            // Publish to Redis stream
                            let stream_event = StreamEvent::GameEvent(event_msg.clone());
                            if let Err(e) = publish_to_stream(&mut redis_conn, &stream_key, &stream_event).await {
                                warn!("Failed to publish game event: {}", e);
                            }
                        }
                        
                        // Check for solo game end
                        // TODO: Why is this here, and why does SoloGameEnded even exist?
                        // Shouldn't the game engine emit the game end event?
                        let game_state = engine.get_committed_state();
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
                                            
                                            // Publish to Redis stream
                                            let stream_event = StreamEvent::GameEvent(event_msg.clone());
                                            if let Err(e) = publish_to_stream(&mut redis_conn, &stream_key, &stream_event).await {
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
                    Err(e) => {
                        eprintln!("Error running game tick: {:?}", e);
                    }
                }
            }
        }
    }
}

/// Helper function to publish events to Redis stream
pub async fn publish_to_stream(
    redis_conn: &mut ConnectionManager,
    stream_key: &str,
    event: &StreamEvent,
) -> Result<()> {
    let data = serde_json::to_vec(event)
        .context("Failed to serialize stream event")?;
    
    let _: String = redis_conn.xadd(
        stream_key,
        "*", // Auto-generate ID
        &[("data", data)],
    ).await
    .context("Failed to add event to Redis stream")?;
    
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

    // Create Redis connection
    let client = redis::Client::open(redis_url.as_str())
        .context("Failed to create Redis client")?;
    let mut redis_conn = ConnectionManager::new(client).await
        .context("Failed to create Redis connection manager")?;
    
    // Stream key for this partition
    let stream_key = format!("snaketron:game-events:partition-{}", partition_id);
    
    // Consumer group for this executor
    let group_name = format!("executor-{}", server_id);
    let consumer_name = format!("server-{}-partition-{}", server_id, partition_id);
    
    // Create consumer group if it doesn't exist
    let _: Result<(), _> = redis_conn.xgroup_create_mkstream(&stream_key, &group_name, "$").await;

    // Track game channels
    let mut game_channels: HashMap<u32, mpsc::Sender<GameCommandMessage>> = HashMap::new();
    
    let try_start_game = |game_id: u32, game_state: GameState, redis_conn: ConnectionManager, stream_key: String, cancellation_token: CancellationToken, game_channels: &mut HashMap<u32, mpsc::Sender<GameCommandMessage>>| {
        if game_id % PARTITION_COUNT != partition_id {
            debug!("Game {} belongs to partition {}, not partition {}", game_id, game_id % PARTITION_COUNT, partition_id);
            return;
        }
        
        if game_channels.contains_key(&game_id) {
            debug!("Game {} is already running", game_id);
            return;
        }
        
        info!("Partition {} will start game {}", partition_id, game_id);
        
        // Create a channel for this game
        let (tx, rx) = mpsc::channel(100);
        game_channels.insert(game_id, tx);
        
        tokio::spawn(async move {
            // Run the game loop
            run_game(server_id, game_id, game_state, redis_conn, stream_key, rx, cancellation_token).await;
            // Remove from active games when done
            // Note: In real implementation, you'd need a shared state for this
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
            
            // Read from Redis stream using a consumer group
            stream_read = async {
                let options = StreamReadOptions::default()
                    .group(&group_name, &consumer_name)
                    .count(10)
                    .block(100);
                    
                redis_conn.xread_options(&[&stream_key], &[">"], &options).await
            } => {
                match stream_read {
                    Ok(reply) => {
                        let reply: StreamReadReply = reply;
                        for stream_data in reply.keys {
                            for stream_id in stream_data.ids {
                                let msg_id = stream_id.id.clone();
                                
                                // Parse the event from Redis stream
                                if let Some(data) = stream_id.map.get("data") {
                                    if let redis::Value::BulkString(bytes) = data {
                                        match serde_json::from_slice::<StreamEvent>(bytes) {
                                            Ok(event) => {
                                                match event {
                                                    StreamEvent::GameCreated { game_id, game_state } => {
                                                        info!("Received GameCreated event for game {}", game_id);
                                                        let redis_conn_clone = redis_conn.clone();
                                                        let stream_key_clone = stream_key.clone();
                                                        let cancellation_token_clone = cancellation_token.clone();
                                                        try_start_game(
                                                            game_id, 
                                                            game_state, 
                                                            redis_conn_clone, 
                                                            stream_key_clone, 
                                                            cancellation_token_clone, 
                                                            &mut game_channels
                                                        );
                                                    }
                                                    StreamEvent::StatusUpdated { game_id, status } => {
                                                        match status {
                                                            GameStatus::Stopped => {
                                                                // Game stopped, it might need to be restarted
                                                                // In this implementation, we'd need to fetch game state
                                                                // from somewhere (e.g., Redis or database)
                                                                debug!("Game {} stopped, may need restart", game_id);
                                                            }
                                                            GameStatus::Complete { .. } => {
                                                                // Game completed, remove channel
                                                                game_channels.remove(&game_id);
                                                                info!("Game {} completed", game_id);
                                                            }
                                                            _ => {}
                                                        }
                                                    }
                                                    StreamEvent::GameCommandSubmitted { game_id, user_id: _, command } => {
                                                        // Route command to the appropriate game
                                                        if let Some(tx) = game_channels.get(&game_id) {
                                                            if let Err(e) = tx.send(command).await {
                                                                warn!("Failed to send command to game {}: {}", game_id, e);
                                                                // The game might have ended, remove from channels
                                                                game_channels.remove(&game_id);
                                                            }
                                                        } else {
                                                            debug!("Received command for inactive game {}", game_id);
                                                        }
                                                    }
                                                    _ => {
                                                        // Other events are not routed to individual games
                                                        debug!("Received event in partition executor: {:?}", event);
                                                    }
                                                }
                                                
                                                // Acknowledge the message
                                                let _: Result<(), _> = redis_conn.xack(&stream_key, &group_name, &[&msg_id]).await;
                                            }
                                            Err(e) => {
                                                error!("Failed to parse stream event: {}", e);
                                                // Still acknowledge to avoid reprocessing
                                                let _: Result<(), _> = redis_conn.xack(&stream_key, &group_name, &[&msg_id]).await;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        debug!("Failed to read from Redis stream: {}", e);
                        // Sleep briefly before retrying
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }

    Ok(())
}
