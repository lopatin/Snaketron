use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{transport::Server, Request, Response, Status, Streaming};
use tracing::{error, info, warn};
use std::pin::Pin;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use crate::game_relay;
use game_relay::game_relay_server::{GameRelay, GameRelayServer};
use game_relay::{GameMessage};
use crate::game_relay::{RaftProposeRequest, RaftProposeResponse};
use crate::raft::RaftNode;

type GameMessageStream = Pin<Box<dyn Stream<Item = Result<GameMessage, Status>> + Send>>;

pub struct GameRelayService {
    raft_node: Option<Arc<RaftNode>>,
    server_id: String,
    grpc_addr: String,
}

impl GameRelayService {
    pub fn new(
        raft_node: Option<Arc<RaftNode>>,
        server_id: String,
        grpc_addr: String,
    ) -> Self {
        Self { 
            raft_node,
            server_id,
            grpc_addr,
        }
    }
}

#[tonic::async_trait]
impl GameRelay for GameRelayService {
    type StreamGameMessagesStream = GameMessageStream;
    async fn stream_game_messages(
        &self,
        request: Request<Streaming<GameMessage>>,
    ) -> Result<Response<Self::StreamGameMessagesStream>, Status> {
        let client_addr = request.remote_addr();
        let mut client_stream = request.into_inner();
        info!(client_address = ?client_addr, "New game relay connection");

        let (response_tx, response_rx) = mpsc::channel(32);
        
        // Handle incoming messages from remote server
        tokio::spawn(async move {
            while let Some(result) = client_stream.next().await {
                match result {
                    Ok(game_message) => {
                        use game_relay::game_message::Message;
                        
                        match game_message.message {
                            Some(Message::Command(_cmd)) => {
                                // TODO: Handle command forwarding when broker is available
                                warn!("Command forwarding not yet implemented");
                            }
                            Some(Message::Subscribe(_sub)) => {
                                // TODO: Handle subscription when broker is available
                                warn!("Subscription handling not yet implemented");
                            }
                            Some(Message::Unsubscribe(unsub)) => {
                                // Remote server unsubscribing from game
                                info!("Remote server unsubscribing from game {}", unsub.game_id);
                            }
                            _ => {}
                        }
                    }
                    Err(status) => {
                        error!(?status, "Error receiving game message");
                        break;
                    }
                }
            }
            info!(client_address = ?client_addr, "Game relay stream ended");
        });

        let output_stream = ReceiverStream::new(response_rx);
        Ok(Response::new(Box::pin(output_stream) as Self::StreamGameMessagesStream))
    }
    
    async fn notify_shutdown(
        &self,
        request: Request<game_relay::ShutdownNotification>,
    ) -> Result<Response<game_relay::ShutdownAck>, Status> {
        let req = request.into_inner();
        info!("Received shutdown notification from server {}", req.server_id);
        
        // Acknowledge all games for now
        // In a full implementation, this would trigger failover procedures
        Ok(Response::new(game_relay::ShutdownAck {
            acknowledged: true,
            accepted_game_ids: req.affected_game_ids,
        }))
    }
    
    async fn raft_rpc(
        &self,
        request: Request<game_relay::RaftMessage>,
    ) -> Result<Response<game_relay::RaftMessage>, Status> {
        // This will be implemented when we have the Raft node available
        // For now, return an error
        Err(Status::unimplemented("Raft RPC not yet implemented"))
    }
    
    async fn get_raft_status(
        &self,
        _request: Request<game_relay::Empty>,
    ) -> Result<Response<game_relay::RaftStatusResponse>, Status> {
        if let Some(raft_node) = &self.raft_node {
            let is_leader = raft_node.is_leader().await;
            let metrics = raft_node.metrics.read().await;
            
            let response = game_relay::RaftStatusResponse {
                is_leader,
                leader_id: if is_leader { 
                    self.server_id.clone() 
                } else { 
                    metrics.current_leader.map(|id| id.to_string()).unwrap_or_default()
                },
                leader_addr: if is_leader {
                    self.grpc_addr.clone()
                } else {
                    // In production, we'd look up the leader's address
                    String::new()
                },
                current_term: metrics.current_term,
                last_log_index: metrics.last_log_index,
                last_applied: metrics.last_applied,
            };
            
            Ok(Response::new(response))
        } else {
            Err(Status::unavailable("Raft not initialized"))
        }
    }
    
    async fn request_join_as_learner(
        &self,
        request: Request<game_relay::JoinAsLearnerRequest>,
    ) -> Result<Response<game_relay::JoinAsLearnerResponse>, Status> {
        if let Some(raft_node) = &self.raft_node {
            let req = request.into_inner();
            
            // Only leader can add learners
            if !raft_node.is_leader().await {
                return Ok(Response::new(game_relay::JoinAsLearnerResponse {
                    accepted: false,
                    reason: "Not the leader".to_string(),
                }));
            }
            
            // Add the node as a learner
            match raft_node.add_learner(req.node_id.clone(), req.grpc_addr).await {
                Ok(_) => {
                    info!("Added {} as learner", req.node_id);
                    Ok(Response::new(game_relay::JoinAsLearnerResponse {
                        accepted: true,
                        reason: String::new(),
                    }))
                }
                Err(e) => {
                    error!("Failed to add learner: {}", e);
                    Ok(Response::new(game_relay::JoinAsLearnerResponse {
                        accepted: false,
                        reason: e.to_string(),
                    }))
                }
            }
        } else {
            Err(Status::unavailable("Raft not initialized"))
        }
    }
    
    async fn request_promotion(
        &self,
        request: Request<game_relay::PromotionRequest>,
    ) -> Result<Response<game_relay::PromotionResponse>, Status> {
        if let Some(raft_node) = &self.raft_node {
            let req = request.into_inner();
            
            // Only leader can promote learners
            if !raft_node.is_leader().await {
                return Ok(Response::new(game_relay::PromotionResponse {
                    promoted: false,
                    reason: "Not the leader".to_string(),
                }));
            }
            
            // Promote the learner
            match raft_node.promote_learner(req.node_id.clone()).await {
                Ok(_) => {
                    info!("Promoted {} to voting member", req.node_id);
                    Ok(Response::new(game_relay::PromotionResponse {
                        promoted: true,
                        reason: String::new(),
                    }))
                }
                Err(e) => {
                    warn!("Failed to promote learner: {}", e);
                    Ok(Response::new(game_relay::PromotionResponse {
                        promoted: false,
                        reason: e.to_string(),
                    }))
                }
            }
        } else {
            Err(Status::unavailable("Raft not initialized"))
        }
    }

    async fn raft_propose(&self, request: Request<RaftProposeRequest>) -> std::result::Result<Response<RaftProposeResponse>, Status> {
        if let Some(raft_node) = &self.raft_node {
            let req = request.into_inner();
            
            // Only leader can propose
            if !raft_node.is_leader().await {
                return Ok(Response::new(RaftProposeResponse {
                    success: false,
                    client_response: None,
                    error: Some("Not the leader".to_string()),
                }));
            }
            
            // Deserialize the client request
            let (client_request, _): (crate::raft::ClientRequest, _) = bincode::serde::decode_from_slice(
                &req.client_request,
                bincode::config::standard()
            ).map_err(|e| Status::invalid_argument(format!("Failed to deserialize request: {}", e)))?;
            
            // Propose the command to Raft
            match raft_node.propose(client_request).await {
                Ok(response) => {
                    info!("Proposed command to Raft");
                    Ok(Response::new(RaftProposeResponse {
                        success: true,
                        client_response: Some(bincode::serde::encode_to_vec(&response, bincode::config::standard())
                            .map_err(|e| Status::internal(format!("Failed to serialize response: {}", e)))?),
                        error: None,
                    }))
                }
                Err(e) => {
                    error!("Failed to propose command: {}", e);
                    Ok(Response::new(RaftProposeResponse {
                        success: false,
                        client_response: None,
                        error: Some(e.to_string()),
                    }))
                }
            }
        } else {
            Err(Status::unavailable("Raft not initialized"))
        }
    }
}

pub async fn run_game_relay_server(
    addr: &str,
    raft: Arc<RaftNode>,
    server_id: String,
    cancellation_token: CancellationToken,
) -> Result<()> {
    #[cfg(feature = "skip-proto")]
    {
        info!("Game relay gRPC server skipped (proto compilation disabled)");
        cancellation_token.cancelled().await;
        return Ok(());
    }
    
    #[cfg(not(feature = "skip-proto"))]
    {
        let service = GameRelayService::new(
            Some(raft),
            server_id,
            addr.to_string(),
        );
        let svc = GameRelayServer::new(service);
        
        info!("Game relay gRPC server starting on {}", addr);
        
        Server::builder()
            .add_service(svc)
            .serve_with_shutdown(addr.parse()?, cancellation_token.cancelled())
            .await
            .context("Game relay gRPC server failed")?;
        
        info!("Game relay gRPC server shut down");
        Ok(())
    }
}

// Keep the old commented code for reference
// use anyhow::{Context, Result};
// use tokio::sync::mpsc;
// use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
// use tonic::{transport::Server, Request, Response, Status, Streaming};
// use tracing::{error, info, warn};
// use std::pin::Pin;
// use std::net::SocketAddr;
// use chrono::Utc;
//
// tonic::include_proto!("stream_exchange");
//
// use stream_exchange_server::{StreamExchange, StreamExchangeServer};
//
// type ResponseStream = Pin<Box<dyn Stream<Item = Result<InboundMessage, Status>> + Send>>;
//
// #[derive(Debug)]
// pub struct BrokerStreamExchange {
//     server_id: String,
// }
//
// impl BrokerStreamExchange {
//     pub fn new(server_id: String) -> Self {
//         Self { server_id }
//     }
// }
//
// #[tonic::async_trait]
// impl StreamExchange for BrokerStreamExchange {
//     type StartExchangeStream = ResponseStream;
//
//     async fn start_exchange(
//         &self,
//         request: Request<Streaming<OutboundMessage>>,
//     ) -> Result<Response<Self::StartExchangeStream>, Status> {
//         let client_addr = request.remote_addr(); // Get client's address for logging
//         let mut client_stream = request.into_inner();
//         info!(client_address = ?client_addr, "gRPC Server: New client connected to StreamExchange service");
//
//         let (response_tx, response_rx) = mpsc::channel(32); // Channel to send messages to the client
//
//         let server_id_clone = self.server_id.clone();
//
//         // Spawn a task to handle messages received from this specific client
//         tokio::spawn(async move {
//             while let Some(result) = client_stream.next().await {
//                 match result {
//                     Ok(outbound_message) => {
//                         info!(
//                             client_id = %outbound_message.client_id,
//                             payload = %outbound_message.payload,
//                             timestamp = %outbound_message.timestamp,
//                             "gRPC Server: Received message from client"
//                         );
//
//                         // Example: Process the message and send a response
//                         // You can customize this logic extensively.
//                         let response_payload = format!(
//                             "Server {} acknowledging message: '{}'",
//                             server_id_clone, outbound_message.payload
//                         );
//                         let inbound_message = InboundMessage {
//                             server_id: server_id_clone.clone(),
//                             response_payload,
//                             timestamp: Utc::now().timestamp_millis(),
//                         };
//
//                         if response_tx.send(Ok(inbound_message)).await.is_err() {
//                             warn!(client_id = %outbound_message.client_id, "gRPC Server: Failed to send response to client. Client stream likely closed.");
//                             break; // Exit if we can't send, client probably disconnected
//                         }
//                     }
//                     Err(status) => {
//                         error!(?status, "gRPC Server: Error receiving message from client stream");
//                         // Optionally, you could try to send an error message back to the client
//                         // if the response_tx channel is still open, but often the stream is broken.
//                         break; // Exit on error
//                     }
//                 }
//             }
//             info!(client_address = ?client_addr, "gRPC Server: Client stream ended.");
//             // The response_tx will be dropped when this task ends, closing the stream for the client.
//         });
//
//         // Convert the MPSC receiver into a stream for the response
//         let output_stream = ReceiverStream::new(response_rx);
//         Ok(Response::new(Box::pin(output_stream) as Self::StartExchangeStream))
//     }
// }
//
// pub async fn run_grpc_server(
//     listen_addr: SocketAddr,
//     server_id: String,
//     cancellation_token: tokio_util::sync::CancellationToken,
// ) -> Result<()> {
//     let server_impl = BrokerStreamExchange::new(server_id.clone());
//     let svc = StreamExchangeServer::new(server_impl);
//
//     info!(address = %listen_addr, server_id = %server_id, "gRPC server starting to listen");
//
//     Server::builder()
//         .add_service(svc)
//         .serve_with_shutdown(listen_addr, cancellation_token.cancelled())
//         .await
//         .context("gRPC server failed")?;
//
//     info!(address = %listen_addr, "gRPC server shut down gracefully.");
//     Ok(())
// }