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

type GameMessageStream = Pin<Box<dyn Stream<Item = Result<GameMessage, Status>> + Send>>;

pub struct GameRelayService {
    server_id: String,
    grpc_addr: String,
}

impl GameRelayService {
    pub fn new(
        server_id: String,
        grpc_addr: String,
    ) -> Self {
        Self { 
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
    
}

pub async fn run_game_relay_server(
    addr: &str,
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
