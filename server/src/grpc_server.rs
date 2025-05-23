use anyhow::{Context, Result};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{transport::Server, Request, Response, Status, Streaming};
use tracing::{error, info, warn};
use std::pin::Pin;
use std::net::SocketAddr;
use chrono::Utc;

tonic::include_proto!("stream_exchange");

use stream_exchange_server::{StreamExchange, StreamExchangeServer};

type ResponseStream = Pin<Box<dyn Stream<Item = Result<InboundMessage, Status>> + Send>>;

#[derive(Debug)]
pub struct BrokerStreamExchange {
    server_id: String,
}

impl BrokerStreamExchange {
    pub fn new(server_id: String) -> Self {
        Self { server_id }
    }
}

#[tonic::async_trait]
impl StreamExchange for BrokerStreamExchange {
    type StartExchangeStream = ResponseStream;

    async fn start_exchange(
        &self,
        request: Request<Streaming<OutboundMessage>>,
    ) -> Result<Response<Self::StartExchangeStream>, Status> {
        let client_addr = request.remote_addr(); // Get client's address for logging
        let mut client_stream = request.into_inner();
        info!(client_address = ?client_addr, "gRPC Server: New client connected to StreamExchange service");

        let (response_tx, response_rx) = mpsc::channel(32); // Channel to send messages to the client

        let server_id_clone = self.server_id.clone();

        // Spawn a task to handle messages received from this specific client
        tokio::spawn(async move {
            while let Some(result) = client_stream.next().await {
                match result {
                    Ok(outbound_message) => {
                        info!(
                            client_id = %outbound_message.client_id,
                            payload = %outbound_message.payload,
                            timestamp = %outbound_message.timestamp,
                            "gRPC Server: Received message from client"
                        );

                        // Example: Process the message and send a response
                        // You can customize this logic extensively.
                        let response_payload = format!(
                            "Server {} acknowledging message: '{}'",
                            server_id_clone, outbound_message.payload
                        );
                        let inbound_message = InboundMessage {
                            server_id: server_id_clone.clone(),
                            response_payload,
                            timestamp: Utc::now().timestamp_millis(),
                        };

                        if response_tx.send(Ok(inbound_message)).await.is_err() {
                            warn!(client_id = %outbound_message.client_id, "gRPC Server: Failed to send response to client. Client stream likely closed.");
                            break; // Exit if we can't send, client probably disconnected
                        }
                    }
                    Err(status) => {
                        error!(?status, "gRPC Server: Error receiving message from client stream");
                        // Optionally, you could try to send an error message back to the client
                        // if the response_tx channel is still open, but often the stream is broken.
                        break; // Exit on error
                    }
                }
            }
            info!(client_address = ?client_addr, "gRPC Server: Client stream ended.");
            // The response_tx will be dropped when this task ends, closing the stream for the client.
        });

        // Convert the MPSC receiver into a stream for the response
        let output_stream = ReceiverStream::new(response_rx);
        Ok(Response::new(Box::pin(output_stream) as Self::StartExchangeStream))
    }
}

pub async fn run_grpc_server(
    listen_addr: SocketAddr,
    server_id: String,
    cancellation_token: tokio_util::sync::CancellationToken,
) -> Result<()> {
    let server_impl = BrokerStreamExchange::new(server_id.clone());
    let svc = StreamExchangeServer::new(server_impl);

    info!(address = %listen_addr, server_id = %server_id, "gRPC server starting to listen");

    Server::builder()
        .add_service(svc)
        .serve_with_shutdown(listen_addr, cancellation_token.cancelled())
        .await
        .context("gRPC server failed")?;

    info!(address = %listen_addr, "gRPC server shut down gracefully.");
    Ok(())
}