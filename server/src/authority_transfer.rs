use anyhow::{Context, Result};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

use crate::game_manager::GameManager;
use crate::replica_manager::{ReplicaManager, ReplicationCommand, AuthorityChange};
use crate::service_manager::ServiceManager;

#[derive(Clone)]
pub struct AuthorityTransferManager {
    server_id: String,
    db_pool: PgPool,
    service_manager: Arc<ServiceManager>,
    replica_manager: Arc<ReplicaManager>,
    game_manager: Arc<RwLock<GameManager>>,
}

impl AuthorityTransferManager {
    pub fn new(
        server_id: String,
        db_pool: PgPool,
        service_manager: Arc<ServiceManager>,
        replica_manager: Arc<ReplicaManager>,
        game_manager: Arc<RwLock<GameManager>>,
    ) -> Self {
        Self {
            server_id,
            db_pool,
            service_manager,
            replica_manager,
            game_manager,
        }
    }

    /// Request to transfer authority of a game to another server
    pub async fn request_transfer(
        &self,
        game_id: u32,
        target_server_id: &str,
        reason: &str,
    ) -> Result<()> {
        info!(
            "Requesting authority transfer for game {} to server {} (reason: {})",
            game_id, target_server_id, reason
        );

        // First, verify we are the current authority
        let current_authority = self.get_game_authority(game_id).await?;
        if current_authority != self.server_id {
            return Err(anyhow::anyhow!(
                "Cannot transfer authority: we are not the current authority (current: {})",
                current_authority
            ));
        }

        // Get the target server's gRPC client
        let client = self.service_manager.get_client(target_server_id).await
            .ok_or_else(|| anyhow::anyhow!("Target server {} not found in cluster", target_server_id))?;

        // Send transfer request via gRPC
        let request = tonic::Request::new(crate::game_broker::game_relay::AuthorityTransferRequest {
            game_id,
            from_server_id: self.server_id.clone(),
            to_server_id: target_server_id.to_string(),
            reason: reason.to_string(),
        });

        let mut client = client.clone();
        match client.transfer_authority(request).await {
            Ok(response) => {
                let resp = response.into_inner();
                if resp.accepted {
                    info!("Authority transfer accepted by server {}", target_server_id);
                    
                    // Update database
                    self.update_game_authority_in_db(game_id, target_server_id).await?;
                    
                    // Send authority change notification
                    let change_cmd = ReplicationCommand::UpdateAuthority {
                        game_id,
                        new_authority: target_server_id.to_string(),
                        version: 0, // Version will be set by replica manager
                        reason: reason.to_string(),
                    };
                    
                    self.replica_manager.get_replication_sender()
                        .send(change_cmd)
                        .await
                        .context("Failed to send authority change notification")?;
                    
                    // TODO: Stop the local game loop
                    
                    Ok(())
                } else {
                    Err(anyhow::anyhow!(
                        "Authority transfer rejected by server {}: {}",
                        target_server_id,
                        resp.error.unwrap_or_else(|| "Unknown reason".to_string())
                    ))
                }
            }
            Err(e) => {
                error!("Failed to transfer authority to server {}: {}", target_server_id, e);
                Err(anyhow::anyhow!("gRPC error: {}", e))
            }
        }
    }

    /// Handle incoming authority transfer request
    pub async fn handle_transfer_request(
        &self,
        game_id: u32,
        from_server_id: &str,
        reason: &str,
    ) -> Result<bool> {
        info!(
            "Received authority transfer request for game {} from server {} (reason: {})",
            game_id, from_server_id, reason
        );

        // Check if we have a recent replica of this game
        if !self.replica_manager.can_accept_game(game_id).await {
            warn!("Cannot accept game {}: no recent replica available", game_id);
            return Ok(false);
        }

        // Check our current load
        let topology = self.service_manager.get_topology().await;
        if let Some(our_server) = topology.servers.get(&self.server_id) {
            if our_server.load_percentage() > 0.8 {
                warn!("Cannot accept game {}: server at {}% capacity", game_id, (our_server.load_percentage() * 100.0) as u32);
                return Ok(false);
            }
        }

        // Accept the transfer
        info!("Accepting authority transfer for game {}", game_id);
        
        // Wait a bit for the database update from the transferring server
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        // Promote our replica to authority
        self.replica_manager.promote_replica_to_authority(game_id).await?;
        
        // Start the game locally
        let mut game_manager = self.game_manager.write().await;
        game_manager.start_game(game_id).await?;
        
        Ok(true)
    }

    /// Transfer all games to other servers (for graceful shutdown)
    pub async fn transfer_all_games(&self) -> Result<()> {
        info!("Transferring all games to other servers");
        
        // Get list of games we're hosting
        let games = self.get_hosted_games().await?;
        
        for game_id in games {
            // Find the least loaded server
            if let Some(target_server) = self.service_manager.get_least_loaded_server().await {
                match self.request_transfer(game_id, &target_server.server_id, "graceful_shutdown").await {
                    Ok(_) => info!("Successfully transferred game {} to {}", game_id, target_server.server_id),
                    Err(e) => error!("Failed to transfer game {}: {}", game_id, e),
                }
            } else {
                error!("No available servers to transfer game {} to", game_id);
            }
        }
        
        Ok(())
    }

    async fn get_game_authority(&self, game_id: u32) -> Result<String> {
        let row = sqlx::query_as::<_, (String,)>(
            "SELECT host_server_id FROM games WHERE id = $1"
        )
        .bind(game_id as i32)
        .fetch_one(&self.db_pool)
        .await
        .context("Failed to query game authority")?;
        
        Ok(row.0)
    }

    async fn update_game_authority_in_db(&self, game_id: u32, new_authority: &str) -> Result<()> {
        sqlx::query(
            "UPDATE games SET host_server_id = $1 WHERE id = $2"
        )
        .bind(new_authority)
        .bind(game_id as i32)
        .execute(&self.db_pool)
        .await
        .context("Failed to update game authority in database")?;
        
        Ok(())
    }

    async fn get_hosted_games(&self) -> Result<Vec<u32>> {
        let rows = sqlx::query_as::<_, (i32,)>(
            r#"
            SELECT id 
            FROM games 
            WHERE host_server_id = $1 
            AND status = 'active'
            "#
        )
        .bind(&self.server_id)
        .fetch_all(&self.db_pool)
        .await
        .context("Failed to query hosted games")?;
        
        Ok(rows.into_iter().map(|r| r.0 as u32).collect())
    }

    /// Handle server failure - promote replicas to authority
    pub async fn handle_server_failure(&self, failed_server_id: &str) -> Result<()> {
        info!("Handling failure of server {}", failed_server_id);
        
        // Find all games that were hosted by the failed server
        let orphaned_games = sqlx::query_as::<_, (i32,)>(
            r#"
            SELECT id 
            FROM games 
            WHERE host_server_id = $1 
            AND status = 'active'
            "#
        )
        .bind(failed_server_id)
        .fetch_all(&self.db_pool)
        .await
        .context("Failed to query orphaned games")?;
        
        for row in orphaned_games {
            let game_id = row.0 as u32;
            
            // Check if we have a replica of this game
            if self.replica_manager.can_accept_game(game_id).await {
                info!("Taking over orphaned game {} from failed server {}", game_id, failed_server_id);
                
                // Promote our replica
                if let Err(e) = self.replica_manager.promote_replica_to_authority(game_id).await {
                    error!("Failed to promote replica for game {}: {}", game_id, e);
                    continue;
                }
                
                // Start the game locally
                let mut game_manager = self.game_manager.write().await;
                if let Err(e) = game_manager.start_game(game_id).await {
                    error!("Failed to start game {}: {}", game_id, e);
                }
            }
        }
        
        Ok(())
    }
}