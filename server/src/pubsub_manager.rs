//! Loss-tolerant Redis Pub/Sub fan-out: chat, lobby updates, user counts.
//!
//! Game-critical traffic (partition events, commands, snapshot requests)
//! does NOT go through here — it rides the Redis Streams bus in
//! `game_bus.rs`, which is ordered, replayable, and backpressured. Pub/Sub
//! is at-most-once by design; everything published here must tolerate loss.

use anyhow::{Result, anyhow};
use redis::aio::ConnectionManager;
use redis::{PushInfo, PushKind, Value};
use serde::de::DeserializeOwned;
use tracing::warn;

pub struct ChannelReceiver {
    inner: tokio::sync::broadcast::Receiver<PushInfo>,
    channel: String,
}

impl ChannelReceiver {
    /// Receive the next message for this channel.
    ///
    /// Only a closed broadcast (shutdown) returns an error. Lagging behind
    /// the shared Redis push firehose and malformed payloads are logged and
    /// skipped — before this, either one propagated as a fatal error and the
    /// subscription task above us broke its loop, permanently and silently
    /// severing the channel's feed on this server. Everything on these
    /// channels is loss-tolerant fan-out, so skipped messages are acceptable.
    pub async fn recv<T>(&mut self) -> Result<T>
    where
        T: DeserializeOwned + Send + 'static,
    {
        loop {
            let PushInfo { kind, data } = match self.inner.recv().await {
                Ok(info) => info,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(
                        "Redis push receiver for channel {} lagged, {} pushes skipped; continuing (channel traffic is loss-tolerant)",
                        self.channel, skipped
                    );
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return Err(anyhow!("Redis push broadcast closed"));
                }
            };

            if !matches!(kind, PushKind::Message) {
                continue;
            }

            let [Value::BulkString(ch), Value::BulkString(payload)] = &data[..] else {
                continue;
            };

            let channel = match String::from_utf8(ch.clone()) {
                Ok(channel) => channel,
                Err(e) => {
                    warn!("Skipping Redis push with non-UTF-8 channel name: {}", e);
                    continue;
                }
            };

            if channel != self.channel {
                continue;
            }

            match serde_json::from_slice::<T>(payload) {
                Ok(msg) => return Ok(msg),
                Err(e) => {
                    warn!("Skipping malformed message on channel {}: {}", channel, e);
                    continue;
                }
            }
        }
    }
}

/// Manager for PubSub operations
#[derive(Clone)]
pub struct PubSubManager {
    redis: ConnectionManager,
    pubsub_tx: tokio::sync::broadcast::Sender<PushInfo>,
}

impl PubSubManager {
    /// Create a new PubSub manager
    pub fn new(
        redis: ConnectionManager,
        pubsub_tx: tokio::sync::broadcast::Sender<PushInfo>,
    ) -> Self {
        Self { redis, pubsub_tx }
    }

    pub async fn subscribe_to_channel(&mut self, channel: &str) -> Result<ChannelReceiver> {
        // Subscribe to the redis channel
        self.redis.subscribe(channel).await?;

        // Subscribe to the broadcast channel
        Ok(ChannelReceiver {
            inner: self.pubsub_tx.subscribe(),
            channel: channel.to_string(),
        })
    }
}
