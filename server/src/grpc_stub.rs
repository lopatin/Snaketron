// Stub module for when proto compilation is skipped
// This allows the code to compile without protoc installed

pub mod game_relay {
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;
    
    pub mod game_relay_client {
        use tonic::transport::Channel;
        
        #[derive(Debug, Clone)]
        pub struct GameRelayClient<T> {
            _channel: std::marker::PhantomData<T>,
        }
        
        impl GameRelayClient<Channel> {
            pub fn new(_channel: Channel) -> Self {
                Self {
                    _channel: std::marker::PhantomData,
                }
            }
            
            pub async fn connect<D>(_dst: D) -> Result<Self, tonic::transport::Error>
            where
                D: std::convert::TryInto<tonic::transport::Endpoint>,
                D::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
            {
                unimplemented!("Proto compilation required")
            }
            
            pub async fn transfer_authority(&mut self, _request: tonic::Request<super::AuthorityTransferRequest>) 
                -> Result<tonic::Response<super::AuthorityTransferResponse>, tonic::Status> {
                unimplemented!("Proto compilation required")
            }
            
            pub async fn notify_shutdown(&mut self, _request: tonic::Request<super::ShutdownNotification>) 
                -> Result<tonic::Response<super::ShutdownAck>, tonic::Status> {
                unimplemented!("Proto compilation required")
            }
            
            pub async fn get_game_snapshot(&mut self, _request: tonic::Request<super::GetSnapshotRequest>) 
                -> Result<tonic::Response<super::GetSnapshotResponse>, tonic::Status> {
                unimplemented!("Proto compilation required")
            }
            
            pub async fn notify_match_found(&mut self, _request: tonic::Request<super::NotifyMatchRequest>) 
                -> Result<tonic::Response<super::NotifyMatchResponse>, tonic::Status> {
                unimplemented!("Proto compilation required")
            }
            
            pub async fn raft_rpc(&mut self, _request: tonic::Request<super::RaftMessage>) 
                -> Result<tonic::Response<super::RaftMessage>, tonic::Status> {
                unimplemented!("Proto compilation required")
            }
            
            pub async fn stream_game_messages(&mut self, _request: tonic::Request<tonic::Streaming<super::GameMessage>>) 
                -> Result<tonic::Response<tonic::Streaming<super::GameMessage>>, tonic::Status> {
                unimplemented!("Proto compilation required")
            }
        }
    }
    
    pub mod game_relay_server {
        use tonic::codegen::*;
        
        pub trait GameRelay: Send + Sync + 'static {
            type StreamGameMessagesStream;
            
            fn stream_game_messages<'life0, 'async_trait>(
                &'life0 self,
                request: tonic::Request<tonic::Streaming<super::GameMessage>>,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<tonic::Response<Self::StreamGameMessagesStream>, tonic::Status>> + Send + 'async_trait>>
            where
                'life0: 'async_trait,
                Self: 'async_trait;
                
            fn get_game_snapshot<'life0, 'async_trait>(
                &'life0 self,
                request: tonic::Request<super::GetSnapshotRequest>,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<tonic::Response<super::GetSnapshotResponse>, tonic::Status>> + Send + 'async_trait>>
            where
                'life0: 'async_trait,
                Self: 'async_trait;
                
            fn notify_match_found<'life0, 'async_trait>(
                &'life0 self,
                request: tonic::Request<super::NotifyMatchRequest>,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<tonic::Response<super::NotifyMatchResponse>, tonic::Status>> + Send + 'async_trait>>
            where
                'life0: 'async_trait,
                Self: 'async_trait;
                
            fn raft_rpc<'life0, 'async_trait>(
                &'life0 self,
                request: tonic::Request<super::RaftMessage>,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<tonic::Response<super::RaftMessage>, tonic::Status>> + Send + 'async_trait>>
            where
                'life0: 'async_trait,
                Self: 'async_trait;
                
            type ReplicateGameStateStream;
            fn replicate_game_state<'life0, 'async_trait>(
                &'life0 self,
                request: tonic::Request<tonic::Streaming<super::ReplicationEvent>>,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<tonic::Response<Self::ReplicateGameStateStream>, tonic::Status>> + Send + 'async_trait>>
            where
                'life0: 'async_trait,
                Self: 'async_trait;
                
            fn transfer_authority<'life0, 'async_trait>(
                &'life0 self,
                request: tonic::Request<super::AuthorityTransferRequest>,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<tonic::Response<super::AuthorityTransferResponse>, tonic::Status>> + Send + 'async_trait>>
            where
                'life0: 'async_trait,
                Self: 'async_trait;
                
            fn notify_shutdown<'life0, 'async_trait>(
                &'life0 self,
                request: tonic::Request<super::ShutdownNotification>,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<tonic::Response<super::ShutdownAck>, tonic::Status>> + Send + 'async_trait>>
            where
                'life0: 'async_trait,
                Self: 'async_trait;
        }
        
        #[derive(Debug)]
        pub struct GameRelayServer<T: GameRelay> {
            inner: Arc<T>,
        }
        
        impl<T: GameRelay> GameRelayServer<T> {
            pub fn new(inner: T) -> Self {
                Self { inner: Arc::new(inner) }
            }
        }
        
        impl<T: GameRelay> Clone for GameRelayServer<T> {
            fn clone(&self) -> Self {
                Self {
                    inner: self.inner.clone(),
                }
            }
        }
        
        impl<T: GameRelay> tonic::codegen::Service<()> for GameRelayServer<T> {
            type Response = ();
            type Error = std::convert::Infallible;
            type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

            fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
                std::task::Poll::Ready(Ok(()))
            }

            fn call(&mut self, _req: ()) -> Self::Future {
                std::future::ready(Ok(()))
            }
        }
        
        impl<T: GameRelay> tonic::server::NamedService for GameRelayServer<T> {
            const NAME: &'static str = "game_relay.GameRelay";
        }
    }
    
    // Message types
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct GameMessage {
        pub message: Option<game_message::Message>,
    }
    
    pub mod game_message {
        use super::*;
        
        #[derive(Clone, Debug, Serialize, Deserialize)]
        pub enum Message {
            Command(GameCommand),
            Event(GameEvent),
            Subscribe(Subscribe),
            Unsubscribe(Unsubscribe),
            Snapshot(GameSnapshot),
        }
    }
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct GameCommand {
        pub game_id: u32,
        pub tick: u32,
        pub user_id: i32,
        pub command_data: Vec<u8>,
    }
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct GameEvent {
        pub game_id: u32,
        pub tick: u32,
        pub user_id: Option<i32>,
        pub event_data: Vec<u8>,
    }
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct Subscribe {
        pub game_id: u32,
        pub commands: bool,
        pub events: bool,
    }
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct Unsubscribe {
        pub game_id: u32,
    }
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct GameSnapshot {
        pub game_id: u32,
        pub game_state: Vec<u8>,
    }
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct GetSnapshotRequest {
        pub game_id: u32,
    }
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct GetSnapshotResponse {
        pub game_state: Vec<u8>,
    }
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct NotifyMatchRequest {
        pub player_ids: Vec<i32>,
        pub game_id: u32,
        pub game_host_server_id: String,
    }
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct NotifyMatchResponse {
        pub success: bool,
        pub notified_players: Vec<i32>,
    }
    
    // High Availability messages
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct AuthorityTransferRequest {
        pub game_id: u32,
        pub from_server_id: String,
        pub to_server_id: String,
        pub reason: String,
    }
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct AuthorityTransferResponse {
        pub accepted: bool,
        pub error: Option<String>,
    }
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct ShutdownNotification {
        pub server_id: String,
        pub grace_period_ms: u32,
        pub affected_game_ids: Vec<u32>,
    }
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct ShutdownAck {
        pub acknowledged: bool,
        pub accepted_game_ids: Vec<u32>,
    }
    
    // Raft consensus messages
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct RaftMessage {
        pub message: Option<raft_message::Message>,
    }
    
    pub mod raft_message {
        use super::*;
        
        #[derive(Clone, Debug, Serialize, Deserialize)]
        pub enum Message {
            AppendEntries(RaftAppendEntries),
            AppendResponse(RaftAppendEntriesResponse),
            VoteRequest(RaftVoteRequest),
            VoteResponse(RaftVoteResponse),
            InstallSnapshot(RaftInstallSnapshot),
            SnapshotResponse(RaftInstallSnapshotResponse),
        }
    }
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct RaftAppendEntries {
        pub term: u64,
        pub leader_id: String,
        pub prev_log_index: u64,
        pub prev_log_term: u64,
        pub entries: Vec<u8>,
        pub leader_commit: u64,
    }
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct RaftAppendEntriesResponse {
        pub term: u64,
        pub success: bool,
        pub has_conflict: bool,
        pub conflict_term: u64,
        pub conflict_index: u64,
    }
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct RaftVoteRequest {
        pub term: u64,
        pub candidate_id: String,
        pub last_log_index: u64,
        pub last_log_term: u64,
    }
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct RaftVoteResponse {
        pub term: u64,
        pub vote_granted: bool,
    }
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct RaftInstallSnapshot {
        pub term: u64,
        pub leader_id: String,
        pub last_included_index: u64,
        pub last_included_term: u64,
        pub offset: u64,
        pub data: Vec<u8>,
        pub done: bool,
    }
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct RaftInstallSnapshotResponse {
        pub term: u64,
    }
    
    // Replication messages
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct ReplicationEvent {
        pub game_id: u32,
        pub version: u64,
        pub source_server_id: String,
        pub event: Option<replication_event::Event>,
    }
    
    pub mod replication_event {
        use super::*;
        
        #[derive(Clone, Debug, Serialize, Deserialize)]
        pub enum Event {
            StateUpdate(GameStateUpdate),
            AuthorityChange(AuthorityChange),
            GameDeleted(GameDeleted),
        }
    }
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct GameStateUpdate {
        pub game_state: Vec<u8>,
        pub tick: u32,
    }
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct AuthorityChange {
        pub new_authority_server_id: String,
        pub reason: String,
    }
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct GameDeleted {
        pub reason: String,
    }
    
    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct ReplicationAck {
        pub game_id: u32,
        pub version: u64,
        pub success: bool,
        pub error: Option<String>,
    }
}