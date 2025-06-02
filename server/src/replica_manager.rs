use anyhow::{Context, Result};
use common::{GameState, GameEvent};
use sqlx::PgPool;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GameReplica {
    pub game_id: u32,
    pub server_id: u32,
    pub state: GameState,
    pub tick: u32,
}

impl GameReplica {
    pub fn is_stale(&self) -> bool {
        self.last_update.elapsed().as_secs() > 60 // 1 minute staleness threshold
    }
}

#[derive(Clone, Debug)]
pub enum ReplicationCommand {
    UpdateGameState {
        game_id: u32,
        state: GameState,
        version: u64,
        tick: u32,
        source_server: String,
    },
    UpdateAuthority {
        game_id: u32,
        new_authority: String,
        version: u64,
        reason: String,
    },
    DeleteGame {
        game_id: u32,
        version: u64,
        reason: String,
    },
}

#[derive(Clone, Debug)]
pub struct AuthorityChange {
    pub game_id: u32,
    pub new_authority: String,
    pub old_authority: String,
    pub reason: String,
}

pub struct ReplicaManager {
    server_id: String,
    db_pool: PgPool,
    replicas: Arc<RwLock<HashMap<u32, GameReplica>>>,
    replication_tx: mpsc::Sender<ReplicationCommand>,
    replication_rx: Arc<RwLock<mpsc::Receiver<ReplicationCommand>>>,
    authority_changes: broadcast::Sender<AuthorityChange>,
    cancellation_token: CancellationToken,
}

impl ReplicaManager {
    pub fn new(
        server_id: String,
        db_pool: PgPool,
        cancellation_token: CancellationToken,
    ) -> Self {
        let (replication_tx, replication_rx) = mpsc::channel(1000);
        let (authority_tx, _) = broadcast::channel(100);

        Self {
            server_id,
            db_pool,
            replicas: Arc::new(RwLock::new(HashMap::new())),
            replication_tx,
            replication_rx: Arc::new(RwLock::new(replication_rx)),
            authority_changes: authority_tx,
            cancellation_token,
        }
    }

    pub async fn start(&self) -> Result<()> {
        info!("Starting ReplicaManager for server {}", self.server_id);

        // Start replication command processor
        let processor_handle = tokio::spawn({
            let replica_manager = self.clone();
            async move {
                replica_manager.process_replication_commands().await;
            }
        });

        // Start stale replica cleanup task
        let cleanup_handle = tokio::spawn({
            let replica_manager = self.clone();
            async move {
                replica_manager.cleanup_stale_replicas().await;
            }
        });

        // Wait for cancellation
        self.cancellation_token.cancelled().await;

        // Clean shutdown
        processor_handle.abort();
        cleanup_handle.abort();

        info!("ReplicaManager stopped");
        Ok(())
    }

    async fn process_replication_commands(&self) {
        let mut rx = self.replication_rx.write().await;

        while let Some(command) = rx.recv().await {
            if self.cancellation_token.is_cancelled() {
                break;
            }

            match command {
                ReplicationCommand::UpdateGameState {
                    game_id,
                    state,
                    version,
                    tick,
                    source_server,
                } => {
                    self.handle_state_update(game_id, state, version, tick, source_server)
                        .await;
                }
                ReplicationCommand::UpdateAuthority {
                    game_id,
                    new_authority,
                    version,
                    reason,
                } => {
                    self.handle_authority_update(game_id, new_authority, version, reason)
                        .await;
                }
                ReplicationCommand::DeleteGame {
                    game_id,
                    version,
                    reason,
                } => {
                    self.handle_game_deletion(game_id, version, reason).await;
                }
            }
        }
    }

    async fn handle_state_update(
        &self,
        game_id: u32,
        state: GameState,
        version: u64,
        tick: u32,
        source_server: String,
    ) {
        let mut replicas = self.replicas.write().await;

        match replicas.get_mut(&game_id) {
            Some(replica) => {
                // Update existing replica if version is newer
                if version > replica.version {
                    debug!(
                        "Updating replica for game {} from version {} to {}",
                        game_id, replica.version, version
                    );
                    replica.state = state;
                    replica.version = version;
                    replica.tick = tick;
                    replica.last_update = Instant::now();
                    
                    // Authority might have changed
                    if replica.authority_server != source_server {
                        warn!(
                            "Authority mismatch for game {}: expected {}, got {}",
                            game_id, replica.authority_server, source_server
                        );
                        replica.authority_server = source_server;
                    }
                } else if version < replica.version {
                    warn!(
                        "Received stale update for game {} (version {} < {})",
                        game_id, version, replica.version
                    );
                }
            }
            None => {
                // Create new replica
                info!("Creating new replica for game {} from server {}", game_id, source_server);
                
                let replica = GameReplica {
                    game_id,
                    state,
                    version,
                    authority_server: source_server,
                    last_update: Instant::now(),
                    tick,
                };
                
                replicas.insert(game_id, replica);
            }
        }
    }

    async fn handle_authority_update(
        &self,
        game_id: u32,
        new_authority: String,
        version: u64,
        reason: String,
    ) {
        let mut replicas = self.replicas.write().await;

        if let Some(replica) = replicas.get_mut(&game_id) {
            if version >= replica.version {
                let old_authority = replica.authority_server.clone();
                
                info!(
                    "Authority for game {} changing from {} to {} (reason: {})",
                    game_id, old_authority, new_authority, reason
                );

                replica.authority_server = new_authority.clone();
                replica.version = version;
                replica.last_update = Instant::now();

                // Broadcast authority change
                let change = AuthorityChange {
                    game_id,
                    new_authority,
                    old_authority,
                    reason,
                };

                if let Err(e) = self.authority_changes.send(change) {
                    error!("Failed to broadcast authority change: {}", e);
                }

                // Update database
                if let Err(e) = self.update_game_authority_in_db(game_id, &replica.authority_server).await {
                    error!("Failed to update game authority in database: {}", e);
                }
            }
        } else {
            warn!(
                "Received authority update for unknown game {} to server {}",
                game_id, new_authority
            );
        }
    }

    async fn handle_game_deletion(&self, game_id: u32, version: u64, reason: String) {
        let mut replicas = self.replicas.write().await;

        if let Some(replica) = replicas.get(&game_id) {
            if version >= replica.version {
                info!("Deleting replica for game {} (reason: {})", game_id, reason);
                replicas.remove(&game_id);
            } else {
                warn!(
                    "Ignoring stale deletion for game {} (version {} < {})",
                    game_id, version, replica.version
                );
            }
        }
    }

    async fn cleanup_stale_replicas(&self) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let mut replicas = self.replicas.write().await;
                    let stale_games: Vec<u32> = replicas
                        .iter()
                        .filter(|(_, replica)| replica.is_stale())
                        .map(|(id, _)| *id)
                        .collect();

                    for game_id in stale_games {
                        warn!("Removing stale replica for game {}", game_id);
                        replicas.remove(&game_id);
                    }
                }
                _ = self.cancellation_token.cancelled() => {
                    break;
                }
            }
        }
    }

    async fn update_game_authority_in_db(&self, game_id: u32, new_authority: &str) -> Result<()> {
        sqlx::query("UPDATE games SET host_server_id = $1 WHERE id = $2")
            .bind(new_authority)
            .bind(game_id as i32)
            .execute(&self.db_pool)
            .await
            .context("Failed to update game authority in database")?;

        Ok(())
    }

    pub fn get_replication_sender(&self) -> mpsc::Sender<ReplicationCommand> {
        self.replication_tx.clone()
    }

    pub fn subscribe_to_authority_changes(&self) -> broadcast::Receiver<AuthorityChange> {
        self.authority_changes.subscribe()
    }

    pub async fn get_replica(&self, game_id: u32) -> Option<GameReplica> {
        let replicas = self.replicas.read().await;
        replicas.get(&game_id).cloned()
    }

    pub async fn get_all_replicas(&self) -> Vec<GameReplica> {
        let replicas = self.replicas.read().await;
        replicas.values().cloned().collect()
    }

    pub async fn promote_replica_to_authority(&self, game_id: u32) -> Result<()> {
        let replicas = self.replicas.read().await;
        
        if let Some(replica) = replicas.get(&game_id) {
            if replica.authority_server == self.server_id {
                return Ok(()); // Already the authority
            }

            info!(
                "Promoting replica for game {} to authority (was: {})",
                game_id, replica.authority_server
            );

            // Update database to claim authority
            sqlx::query(
                r#"
                UPDATE games 
                SET host_server_id = $1 
                WHERE id = $2 
                AND host_server_id = $3
                "#
            )
            .bind(&self.server_id)
            .bind(game_id as i32)
            .bind(&replica.authority_server)
            .execute(&self.db_pool)
            .await
            .context("Failed to claim game authority")?;

            // Send authority change notification
            let change = AuthorityChange {
                game_id,
                new_authority: self.server_id.clone(),
                old_authority: replica.authority_server.clone(),
                reason: "failover".to_string(),
            };

            if let Err(e) = self.authority_changes.send(change) {
                error!("Failed to broadcast authority promotion: {}", e);
            }

            Ok(())
        } else {
            Err(anyhow::anyhow!("No replica found for game {}", game_id))
        }
    }

    pub async fn can_accept_game(&self, game_id: u32) -> bool {
        // Check if we already have a recent replica
        let replicas = self.replicas.read().await;
        
        if let Some(replica) = replicas.get(&game_id) {
            !replica.is_stale()
        } else {
            false
        }
    }
    
    // New methods for Raft integration
    
    pub async fn handle_replication_command(&self, command: ReplicationCommand) -> Result<()> {
        self.replication_tx.send(command).await
            .context("Failed to send replication command")
    }
    
    pub async fn add_replica(&self, replica: GameReplica) -> Result<()> {
        let mut replicas = self.replicas.write().await;
        replicas.insert(replica.game_id, replica);
        Ok(())
    }
}

impl Clone for ReplicaManager {
    fn clone(&self) -> Self {
        Self {
            server_id: self.server_id.clone(),
            db_pool: self.db_pool.clone(),
            replicas: self.replicas.clone(),
            replication_tx: self.replication_tx.clone(),
            replication_rx: self.replication_rx.clone(),
            authority_changes: self.authority_changes.clone(),
            cancellation_token: self.cancellation_token.clone(),
        }
    }
}