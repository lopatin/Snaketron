use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{transport::Server, Request, Response, Status, Streaming};
use tracing::{error, info, warn};
use std::pin::Pin;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use crate::game_broker::{GameMessageBroker, game_relay};
use game_relay::game_relay_server::{GameRelay, GameRelayServer};
use game_relay::{GameMessage, GetSnapshotRequest, GetSnapshotResponse};

type GameMessageStream = Pin<Box<dyn Stream<Item = Result<GameMessage, Status>> + Send>>;

pub struct GameRelayService {
    broker: Arc<dyn GameMessageBroker>,
}

impl GameRelayService {
    pub fn new(broker: Arc<dyn GameMessageBroker>) -> Self {
        Self { broker }
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
        let broker = self.broker.clone();
        let response_tx_clone = response_tx.clone();

        // Handle incoming messages from remote server
        tokio::spawn(async move {
            while let Some(result) = client_stream.next().await {
                match result {
                    Ok(game_message) => {
                        use game_relay::game_message::Message;
                        
                        match game_message.message {
                            Some(Message::Command(cmd)) => {
                                // Deserialize and forward command to local game
                                if let Ok((command, _)) = bincode::serde::decode_from_slice::<common::GameCommand, bincode::config::Configuration>(&cmd.command_data, bincode::config::standard()) {
                                    let cmd_msg = common::GameCommandMessage {
                                        tick: cmd.tick,
                                        received_order: 0, // Will be assigned by game engine
                                        user_id: cmd.user_id as u32,
                                        command,
                                    };
                                    
                                    if let Err(e) = broker.publish_command(cmd.game_id, cmd_msg).await {
                                        error!("Failed to forward command: {:?}", e);
                                    }
                                }
                            }
                            Some(Message::Subscribe(sub)) => {
                                // Remote server wants to subscribe to a game
                                if sub.events {
                                    info!("Remote server subscribing to events for game {}", sub.game_id);
                                    println!("gRPC: Remote server subscribing to events for game {}", sub.game_id);
                                    
                                    // Subscribe to local game events and forward them
                                    if let Ok(mut event_rx) = broker.subscribe_events(sub.game_id).await {
                                        println!("gRPC: Successfully subscribed to game {} events", sub.game_id);
                                        let tx = response_tx_clone.clone();
                                        let game_id = sub.game_id;
                                        
                                        tokio::spawn(async move {
                                            while let Ok(event_msg) = event_rx.recv().await {
                                                // Serialize just the event, not the whole message
                                                println!("gRPC: Serializing event: {:?}", event_msg.event);
                                                if let Ok(event_data) = bincode::serde::encode_to_vec(&event_msg.event, bincode::config::standard()) {
                                                    let grpc_event = game_relay::GameEvent {
                                                        game_id,
                                                        tick: event_msg.tick,
                                                        user_id: event_msg.user_id.map(|id| id as i32),
                                                        event_data,
                                                    };
                                                    
                                                    let message = game_relay::GameMessage {
                                                        message: Some(game_relay::game_message::Message::Event(grpc_event)),
                                                    };
                                                    
                                                    println!("gRPC: Forwarding event for game {} tick {}", game_id, event_msg.tick);
                                                    if tx.send(Ok(message)).await.is_err() {
                                                        break; // Client disconnected
                                                    }
                                                }
                                            }
                                        });
                                    }
                                }
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

    async fn get_game_snapshot(
        &self,
        request: Request<GetSnapshotRequest>,
    ) -> Result<Response<GetSnapshotResponse>, Status> {
        let game_id = request.into_inner().game_id;
        
        // Check if game is local
        match self.broker.is_game_local(game_id).await {
            Ok(true) => {
                // For now, we don't have direct access to GamesManager here
                // In a real implementation, we'd need to coordinate with GamesManager
                // to get snapshots. For testing, return unimplemented.
                Err(Status::unimplemented("Snapshot retrieval not yet implemented"))
            }
            Ok(false) => {
                Err(Status::not_found(format!("Game {} not found on this server", game_id)))
            }
            Err(e) => {
                Err(Status::internal(format!("Error checking game location: {}", e)))
            }
        }
    }
}

pub async fn run_game_relay_server(
    addr: &str,
    broker: Arc<dyn GameMessageBroker>,
    cancellation_token: CancellationToken,
) -> Result<()> {
    let service = GameRelayService::new(broker);
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