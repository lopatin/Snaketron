pub mod api;
pub mod cluster_singleton;
pub mod db;
pub mod game_executor;
pub mod game_server;
pub mod grpc_server;
pub mod http_server;
pub mod lobby_manager;
pub mod matchmaking;
pub mod matchmaking_manager;
pub mod pubsub_manager;
pub mod redis_keys;
pub mod redis_utils;
pub mod region_cache;
pub mod replay;
pub mod replication;
pub mod user_cache;
pub mod ws_matchmaking;
pub mod ws_server;
pub mod xp_persistence;

pub mod game_relay {
    tonic::include_proto!("game_relay");
}
