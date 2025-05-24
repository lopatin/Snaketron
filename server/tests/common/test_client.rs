use anyhow::Result;
use tokio::time::Duration;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream, tungstenite::Message};
use tokio::net::TcpStream as TokioTcpStream;
use futures_util::{SinkExt, StreamExt};
use server::ws_server::WSMessage;
use common::GameEventMessage;

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
        self.send_message(WSMessage::Token(token.to_string())).await?;
        // In a real test, we'd wait for a response or check connection state
        Ok(())
    }
    
    pub async fn send_ping(&mut self) -> Result<()> {
        self.send_message(WSMessage::Ping).await
    }
    
    pub async fn expect_pong(&mut self) -> Result<()> {
        let msg = self.receive_message().await?;
        match msg {
            WSMessage::Pong => Ok(()),
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
        let timeout = tokio::time::timeout(Duration::from_secs(5), self.ws.next()).await;
        match timeout {
            Ok(Some(msg)) => {
                match msg? {
                    Message::Text(text) => {
                        Ok(serde_json::from_str(&text)?)
                    }
                    _ => Err(anyhow::anyhow!("Unexpected message type")),
                }
            }
            Ok(None) => Err(anyhow::anyhow!("Connection closed")),
            Err(_) => Err(anyhow::anyhow!("Timeout waiting for message")),
        }
    }
    
    pub async fn receive_game_event(&mut self) -> Result<Option<GameEventMessage>> {
        let timeout = tokio::time::timeout(Duration::from_millis(100), self.ws.next()).await;
        match timeout {
            Ok(Some(msg)) => {
                match msg? {
                    Message::Text(text) => {
                        // Try to parse as GameEventMessage
                        if let Ok(event) = serde_json::from_str::<GameEventMessage>(&text) {
                            Ok(Some(event))
                        } else {
                            // Might be a different message type
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