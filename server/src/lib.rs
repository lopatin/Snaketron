pub mod game_executor;
pub mod game_server;
pub mod grpc_server;
pub mod matchmaking;
pub mod matchmaking_manager;
pub mod ws_server;
pub mod replay;
pub mod api;
pub mod http_server;
pub mod cluster_singleton;
pub mod replication;
pub mod pubsub_manager;
pub mod ws_matchmaking;
pub mod redis_keys;
pub mod redis_utils;
pub mod db;
pub mod xp_persistence;

pub mod game_relay {
    tonic::include_proto!("game_relay");
}
