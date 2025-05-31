use anyhow::{Context, Result};
use std::sync::Arc;
use std::collections::HashSet;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{info, error, warn, debug};
use sqlx::PgPool;

use crate::{
    game_manager::GameManager,
    player_connections::PlayerConnectionManager,
    raft::{RaftNode, StateChangeEvent},
};

/// Service that monitors Raft state changes and executes game operations
pub struct GameExecutorService {
    server_id: String,
    raft_node: Arc<RaftNode>,
    games_manager: Arc<RwLock<GameManager>>,
    player_connections: Arc<PlayerConnectionManager>,
    db_pool: PgPool,
    /// Track games we've already started to avoid duplicates
    started_games: Arc<RwLock<HashSet<u32>>>,
}

impl GameExecutorService {
    pub fn new(
        server_id: String,
        raft_node: Arc<RaftNode>,
        games_manager: Arc<RwLock<GameManager>>,
        player_connections: Arc<PlayerConnectionManager>,
        db_pool: PgPool,
    ) -> Self {
        Self {
            server_id,
            raft_node,
            games_manager,
            player_connections,
            db_pool,
            started_games: Arc::new(RwLock::new(HashSet::new())),
        }
    }
    
    /// Run the executor service
    pub async fn run(&self, cancellation_token: CancellationToken) -> Result<()> {
        info!("Starting game executor service for server {}", self.server_id);
        
        let mut state_rx = self.raft_node.subscribe_state_changes().await;
        
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("Game executor service shutting down");
                    break;
                }
                
                Ok(event) = state_rx.recv() => {
                    if let Err(e) = self.handle_state_change(event).await {
                        error!("Error handling state change: {}", e);
                    }
                }
            }
        }
        
        Ok(())
    }
    
    async fn handle_state_change(&self, event: StateChangeEvent) -> Result<()> {
        match event {
            StateChangeEvent::GameAssigned { game_id, authority, players } => {
                if authority == self.server_id {
                    // This server has been assigned the game
                    self.start_game_locally(game_id, players).await?;
                }
                
                // Update database status for all servers
                self.update_game_status(game_id, "active").await?;
            }
            
            StateChangeEvent::AuthorityTransferred { game_id, from, to } => {
                if from == self.server_id && to != self.server_id {
                    // We're transferring authority away
                    info!("Transferring authority for game {} to {}", game_id, to);
                    // The GameManager will handle stopping the game
                } else if to == self.server_id {
                    // We're receiving authority
                    info!("Receiving authority for game {} from {}", game_id, from);
                    // TODO: Implement game takeover logic
                }
            }
            
            StateChangeEvent::GameDeleted { game_id } => {
                // Clean up any local resources
                self.started_games.write().await.remove(&game_id);
            }
            
            _ => {
                // Other events we don't need to handle here
            }
        }
        
        Ok(())
    }
    
    async fn start_game_locally(&self, game_id: u32, players: Vec<u32>) -> Result<()> {
        // Check if we've already started this game
        {
            let mut started = self.started_games.write().await;
            if started.contains(&game_id) {
                debug!("Game {} already started, skipping", game_id);
                return Ok(());
            }
            started.insert(game_id);
        }
        
        info!("Starting game {} locally with {} players", game_id, players.len());
        
        // Start the game through the GameManager
        match self.games_manager.write().await.start_game(game_id).await {
            Ok(_) => {
                info!("Successfully started game {}", game_id);
                
                // Notify players that the game has started
                self.notify_players(game_id, &players).await?;
            }
            Err(e) => {
                error!("Failed to start game {}: {}", game_id, e);
                // Remove from started set so it can be retried
                self.started_games.write().await.remove(&game_id);
                return Err(e.into());
            }
        }
        
        Ok(())
    }
    
    async fn notify_players(&self, game_id: u32, players: &[u32]) -> Result<()> {
        // Convert u32 player IDs to i32 for compatibility
        let player_ids: Vec<i32> = players.iter().map(|&id| id as i32).collect();
        
        // Notify players about the match
        self.player_connections
            .notify_match_found(&player_ids, game_id)
            .await;
        
        // Get which players are connected locally for logging
        let local_players = self.player_connections
            .get_connected_players(&player_ids)
            .await;
        
        if !local_players.is_empty() {
            info!(
                "Notified {} local players about game {} starting", 
                local_players.len(), 
                game_id
            );
        }
        
        // Remote players will be notified by their respective servers
        // when they see the GameAssigned event
        
        Ok(())
    }
    
    async fn update_game_status(&self, game_id: u32, status: &str) -> Result<()> {
        sqlx::query(
            "UPDATE games SET status = $1, updated_at = NOW() WHERE id = $2"
        )
        .bind(status)
        .bind(game_id as i32)
        .execute(&self.db_pool)
        .await
        .context("Failed to update game status")?;
        
        debug!("Updated game {} status to '{}'", game_id, status);
        Ok(())
    }
}

/// Run the game executor service
pub async fn run_game_executor(
    server_id: String,
    raft_node: Arc<RaftNode>,
    games_manager: Arc<RwLock<GameManager>>,
    player_connections: Arc<PlayerConnectionManager>,
    db_pool: PgPool,
    cancellation_token: CancellationToken,
) -> Result<()> {
    let executor = GameExecutorService::new(
        server_id,
        raft_node,
        games_manager,
        player_connections,
        db_pool,
    );
    
    executor.run(cancellation_token).await
}