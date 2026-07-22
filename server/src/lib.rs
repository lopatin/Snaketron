pub mod api;
pub mod cluster_membership;
pub mod completion;
pub mod db;
pub mod executor_cluster;
pub mod game_bus;
pub mod game_executor;
pub mod game_executor_v2;
pub mod game_server;
pub mod grpc_server;
pub mod http_server;
pub mod lifecycle;
pub mod lobby_manager;
pub mod matchmaking;
pub mod matchmaking_manager;
pub mod mmr_persistence;
pub mod partition_assignment;
pub mod partition_lease;
pub mod pubsub_manager;
pub mod recovery;
pub mod redis_keys;
pub mod redis_utils;
pub mod region_cache;
pub mod replay;
pub mod replication;
pub mod resilience_metrics;
pub mod season;
pub mod sync_trace;
pub mod user_cache;
pub mod ws_server;
pub mod xp_persistence;

pub mod game_relay {
    tonic::include_proto!("game_relay");
}
