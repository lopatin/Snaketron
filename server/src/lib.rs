pub mod game_executor;
pub mod game_server;
pub mod grpc_server;
pub mod matchmaking;
pub mod raft;
pub mod learner_join;
pub mod ws_server;

pub mod game_relay {
    tonic::include_proto!("game_relay");
}
