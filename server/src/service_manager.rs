use crate::game_broker::game_relay::game_relay_client::GameRelayClient;
use anyhow::{Context, Result};
use sqlx::PgPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

#[derive(Clone, Debug, sqlx::FromRow)]
pub struct ServerInfo {
    pub server_id: String,
    pub host: String,
    pub port: i32,
    pub grpc_port: i32,
    pub max_game_capacity: i32,
    pub current_game_count: i32,
    pub last_heartbeat: chrono::DateTime<chrono::Utc>,
}

impl ServerInfo {
    pub fn is_healthy(&self) -> bool {
        let now = chrono::Utc::now();
        let duration_since_heartbeat = now - self.last_heartbeat;
        duration_since_heartbeat.num_seconds() < 30 // 30 second timeout
    }

    pub fn load_percentage(&self) -> f32 {
        if self.max_game_capacity == 0 {
            return 1.0;
        }
        self.current_game_count as f32 / self.max_game_capacity as f32
    }

    pub fn grpc_endpoint(&self) -> String {
        format!("http://{}:{}", self.host, self.grpc_port)
    }
}

#[derive(Clone)]
pub struct ClusterTopology {
    pub servers: HashMap<String, ServerInfo>,
    pub last_update: Instant,
}

impl ClusterTopology {
    pub fn new() -> Self {
        Self {
            servers: HashMap::new(),
            last_update: Instant::now(),
        }
    }

    pub fn get_healthy_servers(&self) -> Vec<&ServerInfo> {
        self.servers
            .values()
            .filter(|s| s.is_healthy())
            .collect()
    }

    pub fn get_least_loaded_server(&self, exclude_server_id: Option<&str>) -> Option<&ServerInfo> {
        self.get_healthy_servers()
            .into_iter()
            .filter(|s| exclude_server_id.map_or(true, |id| s.server_id != id))
            .min_by(|a, b| {
                a.load_percentage()
                    .partial_cmp(&b.load_percentage())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    }
}

pub struct ServiceManager {
    server_id: String,
    db_pool: PgPool,
    grpc_connections: Arc<RwLock<HashMap<String, GameRelayClient<Channel>>>>,
    topology: Arc<RwLock<ClusterTopology>>,
    cancellation_token: CancellationToken,
}

impl ServiceManager {
    pub fn new(
        server_id: String,
        db_pool: PgPool,
        cancellation_token: CancellationToken,
    ) -> Self {
        Self {
            server_id,
            db_pool,
            grpc_connections: Arc::new(RwLock::new(HashMap::new())),
            topology: Arc::new(RwLock::new(ClusterTopology::new())),
            cancellation_token,
        }
    }

    pub async fn start(&self) -> Result<()> {
        info!("Starting ServiceManager for server {}", self.server_id);

        // Initial topology fetch
        self.refresh_topology().await?;

        // Start background topology refresh task
        let topology_refresh_handle = tokio::spawn({
            let service_manager = self.clone();
            async move {
                service_manager.topology_refresh_loop().await;
            }
        });

        // Start connection health check task
        let connection_health_handle = tokio::spawn({
            let service_manager = self.clone();
            async move {
                service_manager.connection_health_loop().await;
            }
        });

        // Wait for cancellation
        self.cancellation_token.cancelled().await;

        // Clean shutdown
        topology_refresh_handle.abort();
        connection_health_handle.abort();

        info!("ServiceManager stopped");
        Ok(())
    }

    async fn topology_refresh_loop(&self) {
        let mut interval = time::interval(Duration::from_secs(5));
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(e) = self.refresh_topology().await {
                        error!("Failed to refresh topology: {}", e);
                    }
                }
                _ = self.cancellation_token.cancelled() => {
                    break;
                }
            }
        }
    }

    async fn refresh_topology(&self) -> Result<()> {
        debug!("Refreshing cluster topology");

        // Query active servers from database
        let servers: Vec<ServerInfo> = sqlx::query_as(
            r#"
            SELECT 
                server_id,
                host,
                port,
                grpc_port,
                max_game_capacity,
                current_game_count,
                last_heartbeat
            FROM servers
            WHERE last_heartbeat > NOW() - INTERVAL '30 seconds'
            AND server_id != $1
            "#
        )
        .bind(&self.server_id)
        .fetch_all(&self.db_pool)
        .await
        .context("Failed to fetch active servers")?;

        let mut new_topology = ClusterTopology::new();
        
        for server in servers {
            new_topology.servers.insert(server.server_id.clone(), server);
        }

        // Update topology
        {
            let mut topology = self.topology.write().await;
            *topology = new_topology.clone();
        }

        // Establish connections to new servers
        self.update_connections(&new_topology).await?;

        Ok(())
    }

    async fn update_connections(&self, new_topology: &ClusterTopology) -> Result<()> {
        let mut connections = self.grpc_connections.write().await;

        // Remove connections to servers no longer in topology
        connections.retain(|server_id, _| new_topology.servers.contains_key(server_id));

        // Add connections to new servers
        for (server_id, server_info) in &new_topology.servers {
            if !connections.contains_key(server_id) {
                match self.create_grpc_connection(server_info).await {
                    Ok(client) => {
                        info!("Established gRPC connection to server {}", server_id);
                        connections.insert(server_id.clone(), client);
                    }
                    Err(e) => {
                        warn!("Failed to connect to server {}: {}", server_id, e);
                    }
                }
            }
        }

        Ok(())
    }

    async fn create_grpc_connection(
        &self,
        server_info: &ServerInfo,
    ) -> Result<GameRelayClient<Channel>> {
        let endpoint = server_info.grpc_endpoint();
        
        let channel = Channel::from_shared(endpoint.clone())
            .context("Invalid endpoint")?
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
            .with_context(|| format!("Failed to connect to {}", endpoint))?;

        Ok(GameRelayClient::new(channel))
    }

    async fn connection_health_loop(&self) {
        let mut interval = time::interval(Duration::from_secs(30));
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.check_connection_health().await;
                }
                _ = self.cancellation_token.cancelled() => {
                    break;
                }
            }
        }
    }

    async fn check_connection_health(&self) {
        let topology = self.topology.read().await.clone();
        let mut connections = self.grpc_connections.write().await;

        for (server_id, server_info) in &topology.servers {
            if let Some(client) = connections.get_mut(server_id) {
                // Try a simple health check (we'll use GetGameSnapshot with invalid ID)
                let request = tonic::Request::new(
                    crate::game_broker::game_relay::GetSnapshotRequest { game_id: 0 }
                );

                match tokio::time::timeout(
                    Duration::from_secs(2),
                    client.get_game_snapshot(request),
                )
                .await
                {
                    Ok(Ok(_)) | Ok(Err(_)) => {
                        // Connection is alive (even if request failed)
                        debug!("gRPC connection to {} is healthy", server_id);
                    }
                    Err(_) => {
                        // Timeout - connection might be dead
                        warn!("gRPC connection to {} timed out, reconnecting", server_id);
                        
                        // Try to reconnect
                        if let Ok(new_client) = self.create_grpc_connection(server_info).await {
                            *client = new_client;
                            info!("Reconnected to server {}", server_id);
                        } else {
                            error!("Failed to reconnect to server {}", server_id);
                        }
                    }
                }
            }
        }
    }

    pub async fn get_client(&self, server_id: &str) -> Option<GameRelayClient<Channel>> {
        let connections = self.grpc_connections.read().await;
        connections.get(server_id).cloned()
    }

    pub async fn get_topology(&self) -> ClusterTopology {
        self.topology.read().await.clone()
    }

    pub async fn get_least_loaded_server(&self) -> Option<ServerInfo> {
        let topology = self.topology.read().await;
        topology
            .get_least_loaded_server(Some(&self.server_id))
            .cloned()
    }

    pub async fn broadcast_shutdown(&self, grace_period_ms: u32, affected_game_ids: Vec<u32>) -> Result<()> {
        info!("Broadcasting shutdown notification to all servers");
        
        let connections = self.grpc_connections.read().await;
        let notification = crate::game_broker::game_relay::ShutdownNotification {
            server_id: self.server_id.clone(),
            grace_period_ms,
            affected_game_ids,
        };

        for (server_id, mut client) in connections.iter() {
            let request = tonic::Request::new(notification.clone());
            
            match client.clone().notify_shutdown(request).await {
                Ok(response) => {
                    let ack = response.into_inner();
                    info!(
                        "Server {} acknowledged shutdown, accepting games: {:?}",
                        server_id, ack.accepted_game_ids
                    );
                }
                Err(e) => {
                    error!("Failed to notify server {} of shutdown: {}", server_id, e);
                }
            }
        }

        Ok(())
    }
}

impl Clone for ServiceManager {
    fn clone(&self) -> Self {
        Self {
            server_id: self.server_id.clone(),
            db_pool: self.db_pool.clone(),
            grpc_connections: self.grpc_connections.clone(),
            topology: self.topology.clone(),
            cancellation_token: self.cancellation_token.clone(),
        }
    }
}