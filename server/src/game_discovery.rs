use std::sync::Arc;
use std::collections::HashSet;
use tokio::sync::RwLock;
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use sqlx::PgPool;
use tracing::{error, info, debug, warn};
use anyhow::{Result, Context};
use chrono::Utc;

use crate::{
    game_manager::GameManager,
    player_connections::PlayerConnectionManager,
    raft::{RaftNode, ClientRequest},
};
use common::GameState;

/// Service that discovers games in PostgreSQL and submits them to Raft
pub struct GameDiscoveryService {
    db_pool: PgPool,
    server_id: String,
    raft_node: Option<Arc<RaftNode>>,
    /// Track games we've already discovered to avoid duplicate submissions
    discovered_games: Arc<RwLock<HashSet<u32>>>,
}

impl GameDiscoveryService {
    pub fn new(
        db_pool: PgPool,
        server_id: String,
        raft_node: Option<Arc<RaftNode>>,
    ) -> Self {
        Self {
            db_pool,
            server_id,
            raft_node,
            discovered_games: Arc::new(RwLock::new(HashSet::new())),
        }
    }
    
    /// Run the discovery service
    pub async fn run(&self, cancellation_token: CancellationToken) -> Result<()> {
        info!("Starting game discovery service for server {}", self.server_id);
        
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        
        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    info!("Game discovery service shutting down");
                    break;
                }
                _ = interval.tick() => {
                    if let Err(e) = self.discover_and_submit_games().await {
                        error!("Game discovery error: {}", e);
                    }
                }
            }
        }
        
        Ok(())
    }
    
    async fn discover_and_submit_games(&self) -> Result<()> {
        // Query for games in 'waiting' status that haven't been discovered yet
        let waiting_games: Vec<WaitingGame> = sqlx::query_as(
            r#"
            SELECT 
                g.id,
                g.server_id,
                g.region,
                g.created_at,
                ARRAY_AGG(gr.user_id) as player_ids
            FROM games g
            INNER JOIN game_requests gr ON gr.game_id = g.id
            WHERE g.status = 'waiting'
            GROUP BY g.id, g.server_id, g.region, g.created_at
            ORDER BY g.created_at ASC
            LIMIT 100
            "#
        )
        .fetch_all(&self.db_pool)
        .await
        .context("Failed to query waiting games")?;
        
        for game in waiting_games {
            // Check if we've already discovered this game
            {
                let discovered = self.discovered_games.read().await;
                if discovered.contains(&(game.id as u32)) {
                    continue;
                }
            }
            
            // Submit to Raft if available
            if let Some(raft_node) = &self.raft_node {
                if let Err(e) = self.submit_game_to_raft(raft_node, game).await {
                    warn!("Failed to submit game to Raft: {}", e);
                    // Continue with other games even if one fails
                    continue;
                }
            } else {
                // Fallback: directly start the game if no Raft (for testing/single-server mode)
                warn!("No Raft node available, falling back to direct assignment");
                if let Err(e) = self.fallback_direct_assignment(game).await {
                    error!("Failed to directly assign game: {}", e);
                }
            }
        }
        
        Ok(())
    }
    
    // async fn submit_game_to_raft(&self, raft_node: &Arc<RaftNode>, game: WaitingGame) -> Result<()> {
    //     debug!("Submitting game {} to Raft for consensus", game.id);
    //     
    //     // Select authority server based on assignment or load balancing
    //     let authority = self.select_authority(&game).await?;
    //     
    //     // Create initial game state
    //     let initial_state = GameState::new(40, 30, None);
    //     
    //     // Convert player IDs to u32
    //     let players: Vec<u32> = game.player_ids.iter().map(|&id| id as u32).collect();
    //     
    //     // Create Raft command
    //     // let command = ClientRequest::CreateGame {
    //     //     game_id: game.id as u32,
    //     //     initial_state,
    //     //     authority_server: authority,
    //     //     players: players.clone(),
    //     //     discovery_source: self.server_id.clone(),
    //     //     discovered_at: Utc::now().timestamp(),
    //     // };
    //     // 
    //     // Submit to Raft
    //     match raft_node.propose(command).await {
    //         Ok(_) => {
    //             info!("Successfully submitted game {} to Raft", game.id);
    //             // Mark as discovered
    //             self.discovered_games.write().await.insert(game.id as u32);
    //             Ok(())
    //         }
    //         Err(e) => {
    //             error!("Failed to propose game {} to Raft: {}", game.id, e);
    //             Err(e)
    //         }
    //     }
    // }
    // 
    async fn select_authority(&self, game: &WaitingGame) -> Result<String> {
        // If a server was already assigned in the database, use it
        if let Some(server_id) = &game.server_id {
            return Ok(server_id.to_string());
        }
        
        // Otherwise, select based on load balancing
        // For now, we'll use region-based assignment with round-robin
        let server = self.select_least_loaded_server(&game.region).await?;
        Ok(server)
    }
    
    async fn select_least_loaded_server(&self, region: &Option<String>) -> Result<String> {
        // Query for the server with least games in the region
        let server_id: Option<String> = sqlx::query_scalar(
            r#"
            SELECT s.server_id::text
            FROM servers s
            LEFT JOIN games g ON g.host_server_id = s.server_id::text
                AND g.status IN ('waiting', 'active')
            WHERE s.status = 'online'
            AND ($1::text IS NULL OR s.region = $1)
            GROUP BY s.server_id
            ORDER BY COUNT(g.id) ASC, RANDOM()
            LIMIT 1
            "#
        )
        .bind(region)
        .fetch_optional(&self.db_pool)
        .await
        .context("Failed to query least loaded server")?;
        
        server_id.ok_or_else(|| anyhow::anyhow!("No available servers in region {:?}", region))
    }
    
    async fn fallback_direct_assignment(&self, game: WaitingGame) -> Result<()> {
        // In fallback mode, assign to this server if no server was specified
        let server_id = game.server_id.unwrap_or_else(|| {
            uuid::Uuid::parse_str(&self.server_id).unwrap_or_else(|_| uuid::Uuid::new_v4())
        });
        
        // Update game assignment in database
        sqlx::query(
            r#"
            UPDATE games 
            SET server_id = $1, host_server_id = $2, updated_at = NOW()
            WHERE id = $3 AND status = 'waiting'
            "#
        )
        .bind(server_id)
        .bind(server_id.to_string())
        .bind(game.id)
        .execute(&self.db_pool)
        .await
        .context("Failed to update game assignment")?;
        
        info!("Directly assigned game {} to server {}", game.id, server_id);
        
        // Mark as discovered to avoid reprocessing
        self.discovered_games.write().await.insert(game.id as u32);
        
        Ok(())
    }
}

#[derive(Debug, sqlx::FromRow)]
struct WaitingGame {
    id: i32,
    server_id: Option<uuid::Uuid>,
    region: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    player_ids: Vec<i32>,
}

/// Run the game discovery service (backward compatibility wrapper)
pub async fn run_game_discovery_loop(
    pool: PgPool,
    server_id: uuid::Uuid,
    games_manager: Arc<RwLock<GameManager>>,
    player_connections: Arc<PlayerConnectionManager>,
    cancellation_token: CancellationToken,
) {
    warn!("Using legacy game discovery loop - this will be deprecated");
    
    // For backward compatibility, run without Raft
    let discovery = GameDiscoveryService::new(
        pool,
        server_id.to_string(),
        None, // No Raft in legacy mode
    );
    
    if let Err(e) = discovery.run(cancellation_token).await {
        error!("Game discovery service error: {}", e);
    }
}

/// Run the game discovery service with Raft
pub async fn run_game_discovery_with_raft(
    pool: PgPool,
    server_id: String,
    raft_node: Option<Arc<RaftNode>>,
    cancellation_token: CancellationToken,
) -> Result<()> {
    let discovery = GameDiscoveryService::new(pool, server_id, raft_node);
    discovery.run(cancellation_token).await
}