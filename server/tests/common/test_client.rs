use anyhow::Result;
use chrono::Utc;
use common::GameEventMessage;
use futures_util::{SinkExt, StreamExt};
use server::ws_server::WSMessage;
use tokio::net::TcpStream as TokioTcpStream;
use tokio::time::Duration;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

/// Test client wrapper for easier testing
pub struct TestClient {
    ws: WebSocketStream<MaybeTlsStream<TokioTcpStream>>,
    pub user_id: Option<i32>,
}

impl TestClient {
    pub async fn connect(addr: &str) -> Result<Self> {
        let (ws_stream, _) = connect_async(addr).await?;
        Ok(TestClient {
            ws: ws_stream,
            user_id: None,
        })
    }

    pub async fn authenticate(&mut self, user_id: i32) -> Result<()> {
        // For testing with mock auth, we use the user_id as the token
        self.authenticate_with_token(&user_id.to_string()).await?;
        self.user_id = Some(user_id);
        Ok(())
    }

    pub async fn authenticate_with_token(&mut self, token: &str) -> Result<()> {
        // Send the exact token string - useful for testing specific JWT tokens
        self.send_message(WSMessage::Token(token.to_string()))
            .await?;
        // In a real test, we'd wait for a response or check connection state
        Ok(())
    }

    pub async fn send_ping(&mut self) -> Result<()> {
        let client_time = Utc::now().timestamp_millis();
        self.send_message(WSMessage::Ping { client_time }).await
    }

    pub async fn expect_pong(&mut self) -> Result<()> {
        let msg = self.receive_message().await?;
        match msg {
            WSMessage::Pong { .. } => Ok(()),
            _ => Err(anyhow::anyhow!("Expected Pong, got {:?}", msg)),
        }
    }

    pub async fn join_game(&mut self, game_id: u32) -> Result<()> {
        self.send_message(WSMessage::JoinGame(game_id)).await
    }

    pub async fn send_message(&mut self, msg: WSMessage) -> Result<()> {
        let json = serde_json::to_string(&msg)?;
        self.ws.send(Message::Text(json.into())).await?;
        Ok(())
    }

    pub async fn receive_message(&mut self) -> Result<WSMessage> {
        let timeout = tokio::time::timeout(Duration::from_secs(30), self.ws.next()).await;
        match timeout {
            Ok(Some(msg)) => match msg? {
                Message::Text(text) => Ok(serde_json::from_str(&text)?),
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            },
            Ok(None) => Err(anyhow::anyhow!("Connection closed")),
            Err(_) => Err(anyhow::anyhow!("Timeout waiting for message")),
        }
    }

    pub async fn receive_text(&mut self) -> Result<String> {
        let timeout = tokio::time::timeout(Duration::from_secs(1), self.ws.next()).await;
        match timeout {
            Ok(Some(msg)) => match msg? {
                Message::Text(text) => Ok(text.to_string()),
                _ => Err(anyhow::anyhow!("Expected text message")),
            },
            Ok(None) => Err(anyhow::anyhow!("Connection closed")),
            Err(_) => Err(anyhow::anyhow!("Timeout waiting for message")),
        }
    }

    pub async fn receive_game_event(&mut self) -> Result<Option<GameEventMessage>> {
        let timeout = tokio::time::timeout(Duration::from_secs(1), self.ws.next()).await;
        match timeout {
            Ok(Some(msg)) => {
                match msg? {
                    Message::Text(text) => {
                        // Try to parse as WSMessage first
                        if let Ok(ws_msg) = serde_json::from_str::<WSMessage>(&text) {
                            match ws_msg {
                                WSMessage::GameEvent(event) => Ok(Some(event)),
                                _ => Ok(None), // Different message type
                            }
                        } else if let Ok(event) = serde_json::from_str::<GameEventMessage>(&text) {
                            // Fallback to direct GameEventMessage parsing
                            Ok(Some(event))
                        } else {
                            Ok(None)
                        }
                    }
                    _ => Ok(None),
                }
            }
            Ok(None) => Err(anyhow::anyhow!("Connection closed")),
            Err(_) => Ok(None), // Timeout is ok, just no message
        }
    }

    pub async fn disconnect(mut self) -> Result<()> {
        self.ws.close(None).await?;
        Ok(())
    }
}
