//! Bounded, ephemeral cache storage for verified replay recordings.
//!
//! Cache values are deliberately opaque to this layer. The replay repository
//! owns their integrity envelope, while this module owns deterministic keys,
//! TTLs, size limits, and Valkey/ElastiCache transport behavior.

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use redis::AsyncCommands;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

use crate::redis_utils::RedisConnection;
use crate::replay_store::ReplayObjectMetadata;

pub const REPLAY_CACHE_FORMAT_VERSION: u16 = 1;
pub const REPLAY_CACHE_PREFIX_ENV: &str = "SNAKETRON_REPLAY_CACHE_PREFIX";
pub const REPLAY_CACHE_TTL_SECONDS_ENV: &str = "SNAKETRON_REPLAY_CACHE_TTL_SECONDS";
pub const REPLAY_CACHE_MAX_BYTES_ENV: &str = "SNAKETRON_REPLAY_CACHE_MAX_BYTES";
pub const REPLAY_CACHE_TIMEOUT_MS_ENV: &str = "SNAKETRON_REPLAY_CACHE_TIMEOUT_MS";

const DEFAULT_CACHE_PREFIX: &str = "snaketron:replay-cache";
const DEFAULT_CACHE_TTL_SECONDS: u64 = 60 * 60;
const DEFAULT_CACHE_MAX_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_CACHE_TIMEOUT_MS: u64 = 250;
const ABSOLUTE_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;

// GETRANGE bounds the bytes returned by Redis. Lua distinguishes a missing key
// from an intentionally present empty value without issuing a race-prone
// STRLEN followed by GET sequence. ARGV[1] is an inclusive end offset, so a
// max-byte value returns at most max + 1 bytes and lets Rust reject oversize.
const BOUNDED_GET_SCRIPT: &str = r#"
local value = redis.call('GETRANGE', KEYS[1], 0, ARGV[1])
if string.len(value) == 0 and redis.call('EXISTS', KEYS[1]) == 0 then
    return false
end
return value
"#;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayCacheConfig {
    pub key_prefix: String,
    pub ttl: Duration,
    pub max_value_bytes: usize,
    pub operation_timeout: Duration,
}

impl ReplayCacheConfig {
    pub fn from_env() -> Result<Self> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self> {
        let key_prefix = normalize_prefix(
            nonempty(lookup(REPLAY_CACHE_PREFIX_ENV))
                .unwrap_or_else(|| DEFAULT_CACHE_PREFIX.to_owned()),
        )?;
        let ttl = Duration::from_secs(parse_positive_u64(
            REPLAY_CACHE_TTL_SECONDS_ENV,
            lookup(REPLAY_CACHE_TTL_SECONDS_ENV),
            DEFAULT_CACHE_TTL_SECONDS,
        )?);
        let max_value_bytes = parse_positive_usize(
            REPLAY_CACHE_MAX_BYTES_ENV,
            lookup(REPLAY_CACHE_MAX_BYTES_ENV),
            DEFAULT_CACHE_MAX_BYTES,
        )?;
        let operation_timeout = Duration::from_millis(parse_positive_u64(
            REPLAY_CACHE_TIMEOUT_MS_ENV,
            lookup(REPLAY_CACHE_TIMEOUT_MS_ENV),
            DEFAULT_CACHE_TIMEOUT_MS,
        )?);
        let config = Self {
            key_prefix,
            ttl,
            max_value_bytes,
            operation_timeout,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if normalize_prefix(self.key_prefix.clone())? != self.key_prefix {
            bail!("{REPLAY_CACHE_PREFIX_ENV} must not have surrounding whitespace or colons");
        }
        if self.ttl.is_zero() {
            bail!("{REPLAY_CACHE_TTL_SECONDS_ENV} must be positive");
        }
        if self.max_value_bytes == 0 || self.max_value_bytes > ABSOLUTE_CACHE_MAX_BYTES {
            bail!("{REPLAY_CACHE_MAX_BYTES_ENV} must be between 1 and {ABSOLUTE_CACHE_MAX_BYTES}");
        }
        if self.operation_timeout.is_zero() {
            bail!("{REPLAY_CACHE_TIMEOUT_MS_ENV} must be positive");
        }
        Ok(())
    }

    /// Content-addressed and schema-versioned so a recording rewrite or cache
    /// format rollout cannot make an older value look current.
    pub fn key_for(&self, expected: &ReplayObjectMetadata) -> Result<String> {
        expected.validate()?;
        Ok(format!(
            "{}:v{}:recording-v{}:game:{:010}:sha256:{}",
            self.key_prefix,
            REPLAY_CACHE_FORMAT_VERSION,
            expected.format_version,
            expected.game_id,
            expected.uncompressed_sha256
        ))
    }
}

impl Default for ReplayCacheConfig {
    fn default() -> Self {
        Self {
            key_prefix: DEFAULT_CACHE_PREFIX.to_owned(),
            ttl: Duration::from_secs(DEFAULT_CACHE_TTL_SECONDS),
            max_value_bytes: DEFAULT_CACHE_MAX_BYTES,
            operation_timeout: Duration::from_millis(DEFAULT_CACHE_TIMEOUT_MS),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayCacheLookup {
    Miss,
    Hit(Vec<u8>),
    /// The backend returned exactly the configured limit plus one byte. The
    /// repository must ignore it and fall back to durable object storage.
    RejectedOversized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayCachePutOutcome {
    Stored,
    SkippedOversized,
}

#[async_trait]
pub trait ReplayCache: Send + Sync {
    async fn get(&self, expected: &ReplayObjectMetadata) -> Result<ReplayCacheLookup>;

    async fn put(
        &self,
        expected: &ReplayObjectMetadata,
        value: &[u8],
    ) -> Result<ReplayCachePutOutcome>;
}

/// Production cache backend. `RedisConnection` transparently supports the
/// standalone Valkey development service and ElastiCache's cluster endpoint.
#[derive(Clone)]
pub struct ValkeyReplayCache {
    redis: RedisConnection,
    config: ReplayCacheConfig,
}

impl ValkeyReplayCache {
    pub fn new(redis: RedisConnection, config: ReplayCacheConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self { redis, config })
    }

    pub fn config(&self) -> &ReplayCacheConfig {
        &self.config
    }
}

#[async_trait]
impl ReplayCache for ValkeyReplayCache {
    async fn get(&self, expected: &ReplayObjectMetadata) -> Result<ReplayCacheLookup> {
        let key = self.config.key_for(expected)?;
        let inclusive_end = u64::try_from(self.config.max_value_bytes)
            .context("replay cache maximum does not fit a Redis range offset")?;
        let mut redis = self.redis.clone();
        let value: Option<Vec<u8>> = tokio::time::timeout(self.config.operation_timeout, async {
            redis::Script::new(BOUNDED_GET_SCRIPT)
                .key(&key)
                .arg(inclusive_end)
                .invoke_async(&mut redis)
                .await
        })
        .await
        .with_context(|| format!("replay cache GET timed out for game {}", expected.game_id))?
        .with_context(|| format!("replay cache GET failed for game {}", expected.game_id))?;

        match value {
            None => Ok(ReplayCacheLookup::Miss),
            Some(value) if value.len() > self.config.max_value_bytes => {
                Ok(ReplayCacheLookup::RejectedOversized)
            }
            Some(value) => Ok(ReplayCacheLookup::Hit(value)),
        }
    }

    async fn put(
        &self,
        expected: &ReplayObjectMetadata,
        value: &[u8],
    ) -> Result<ReplayCachePutOutcome> {
        if value.len() > self.config.max_value_bytes {
            return Ok(ReplayCachePutOutcome::SkippedOversized);
        }
        let key = self.config.key_for(expected)?;
        let ttl_seconds = self.config.ttl.as_secs();
        let mut redis = self.redis.clone();
        tokio::time::timeout(self.config.operation_timeout, async {
            redis.set_ex::<_, _, ()>(&key, value, ttl_seconds).await
        })
        .await
        .with_context(|| format!("replay cache SET timed out for game {}", expected.game_id))?
        .with_context(|| format!("replay cache SET failed for game {}", expected.game_id))?;
        Ok(ReplayCachePutOutcome::Stored)
    }
}

#[derive(Clone)]
struct MemoryEntry {
    value: Vec<u8>,
    expires_at: Instant,
}

/// Production-faithful fake with the same key, TTL, and size behavior.
#[derive(Clone)]
pub struct InMemoryReplayCache {
    config: ReplayCacheConfig,
    entries: Arc<RwLock<HashMap<String, MemoryEntry>>>,
    get_calls: Arc<AtomicU64>,
    put_calls: Arc<AtomicU64>,
}

impl InMemoryReplayCache {
    pub fn new() -> Self {
        Self::with_config(ReplayCacheConfig::default())
            .expect("built-in replay-cache config must be valid")
    }

    pub fn with_config(config: ReplayCacheConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            entries: Arc::new(RwLock::new(HashMap::new())),
            get_calls: Arc::new(AtomicU64::new(0)),
            put_calls: Arc::new(AtomicU64::new(0)),
        })
    }

    pub fn config(&self) -> &ReplayCacheConfig {
        &self.config
    }

    pub fn get_call_count(&self) -> u64 {
        self.get_calls.load(Ordering::Relaxed)
    }

    pub fn put_call_count(&self) -> u64 {
        self.put_calls.load(Ordering::Relaxed)
    }

    pub async fn object_count(&self) -> usize {
        self.entries.read().await.len()
    }

    /// Seed malformed or previous-version bytes for fallback/repair tests.
    pub async fn seed_raw(&self, expected: &ReplayObjectMetadata, value: Vec<u8>) -> Result<()> {
        if value.len() > self.config.max_value_bytes {
            bail!("seeded replay cache value exceeds configured maximum");
        }
        let key = self.config.key_for(expected)?;
        let expires_at = Instant::now()
            .checked_add(self.config.ttl)
            .context("replay cache TTL exceeds monotonic clock range")?;
        self.entries
            .write()
            .await
            .insert(key, MemoryEntry { value, expires_at });
        Ok(())
    }
}

impl Default for InMemoryReplayCache {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ReplayCache for InMemoryReplayCache {
    async fn get(&self, expected: &ReplayObjectMetadata) -> Result<ReplayCacheLookup> {
        self.get_calls.fetch_add(1, Ordering::Relaxed);
        let key = self.config.key_for(expected)?;
        let mut entries = self.entries.write().await;
        let Some(entry) = entries.get(&key).cloned() else {
            return Ok(ReplayCacheLookup::Miss);
        };
        if entry.expires_at <= Instant::now() {
            entries.remove(&key);
            return Ok(ReplayCacheLookup::Miss);
        }
        if entry.value.len() > self.config.max_value_bytes {
            return Ok(ReplayCacheLookup::RejectedOversized);
        }
        Ok(ReplayCacheLookup::Hit(entry.value))
    }

    async fn put(
        &self,
        expected: &ReplayObjectMetadata,
        value: &[u8],
    ) -> Result<ReplayCachePutOutcome> {
        self.put_calls.fetch_add(1, Ordering::Relaxed);
        if value.len() > self.config.max_value_bytes {
            return Ok(ReplayCachePutOutcome::SkippedOversized);
        }
        self.seed_raw(expected, value.to_vec()).await?;
        Ok(ReplayCachePutOutcome::Stored)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayCacheFailureMode {
    Reads,
    Writes,
    All,
}

/// Deterministic outage fake used to prove cache failures never become replay
/// availability failures.
#[derive(Clone)]
pub struct FailingReplayCache {
    config: ReplayCacheConfig,
    mode: ReplayCacheFailureMode,
    get_calls: Arc<AtomicU64>,
    put_calls: Arc<AtomicU64>,
}

impl FailingReplayCache {
    pub fn new(mode: ReplayCacheFailureMode) -> Self {
        Self {
            config: ReplayCacheConfig::default(),
            mode,
            get_calls: Arc::new(AtomicU64::new(0)),
            put_calls: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn get_call_count(&self) -> u64 {
        self.get_calls.load(Ordering::Relaxed)
    }

    pub fn put_call_count(&self) -> u64 {
        self.put_calls.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl ReplayCache for FailingReplayCache {
    async fn get(&self, expected: &ReplayObjectMetadata) -> Result<ReplayCacheLookup> {
        self.get_calls.fetch_add(1, Ordering::Relaxed);
        self.config.key_for(expected)?;
        if matches!(
            self.mode,
            ReplayCacheFailureMode::Reads | ReplayCacheFailureMode::All
        ) {
            Err(anyhow!("injected replay cache read outage"))
        } else {
            Ok(ReplayCacheLookup::Miss)
        }
    }

    async fn put(
        &self,
        expected: &ReplayObjectMetadata,
        value: &[u8],
    ) -> Result<ReplayCachePutOutcome> {
        self.put_calls.fetch_add(1, Ordering::Relaxed);
        self.config.key_for(expected)?;
        if matches!(
            self.mode,
            ReplayCacheFailureMode::Writes | ReplayCacheFailureMode::All
        ) {
            return Err(anyhow!("injected replay cache write outage"));
        }
        if value.len() > self.config.max_value_bytes {
            Ok(ReplayCachePutOutcome::SkippedOversized)
        } else {
            Ok(ReplayCachePutOutcome::Stored)
        }
    }
}

fn nonempty(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    })
}

fn normalize_prefix(prefix: String) -> Result<String> {
    let prefix = prefix.trim().trim_matches(':');
    if prefix.is_empty()
        || prefix
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
        || prefix.contains(['{', '}', '*', '?', '[', ']'])
    {
        bail!(
            "{REPLAY_CACHE_PREFIX_ENV} must be a non-empty literal Redis key prefix without whitespace, glob characters, or hash tags"
        );
    }
    Ok(prefix.to_owned())
}

fn parse_positive_u64(name: &str, value: Option<String>, default: u64) -> Result<u64> {
    let Some(value) = nonempty(value) else {
        return Ok(default);
    };
    value
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| anyhow!("{name} must be a positive integer"))
}

fn parse_positive_usize(name: &str, value: Option<String>, default: usize) -> Result<usize> {
    let Some(value) = nonempty(value) else {
        return Ok(default);
    };
    value
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| anyhow!("{name} must be a positive integer"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redis_utils::RedisClient;
    use redis::AsyncCommands;

    fn reference() -> ReplayObjectMetadata {
        ReplayObjectMetadata {
            format_version: 1,
            game_id: 42,
            object_key: "recordings/v1/games/0000000042.replay.json.gz".to_owned(),
            uncompressed_sha256: "a".repeat(64),
            compressed_sha256: "b".repeat(64),
            uncompressed_bytes: 5,
            compressed_bytes: 10,
        }
    }

    #[test]
    fn configuration_and_key_are_bounded_and_versioned() {
        let config = ReplayCacheConfig::from_lookup(|name| match name {
            REPLAY_CACHE_PREFIX_ENV => Some(":custom:replays:".into()),
            REPLAY_CACHE_TTL_SECONDS_ENV => Some("120".into()),
            REPLAY_CACHE_MAX_BYTES_ENV => Some("4096".into()),
            REPLAY_CACHE_TIMEOUT_MS_ENV => Some("75".into()),
            _ => None,
        })
        .unwrap();

        assert_eq!(config.key_prefix, "custom:replays");
        assert_eq!(config.ttl, Duration::from_secs(120));
        assert_eq!(config.max_value_bytes, 4096);
        assert_eq!(config.operation_timeout, Duration::from_millis(75));
        assert_eq!(
            config.key_for(&reference()).unwrap(),
            format!(
                "custom:replays:v1:recording-v1:game:0000000042:sha256:{}",
                "a".repeat(64)
            )
        );

        let error = ReplayCacheConfig::from_lookup(|name| match name {
            REPLAY_CACHE_PREFIX_ENV => Some("bad {shared} prefix".into()),
            _ => None,
        })
        .unwrap_err();
        assert!(error.to_string().contains("literal Redis key prefix"));
    }

    #[tokio::test]
    async fn in_memory_cache_enforces_value_bound() {
        let cache = InMemoryReplayCache::with_config(ReplayCacheConfig {
            max_value_bytes: 4,
            ..ReplayCacheConfig::default()
        })
        .unwrap();

        assert_eq!(
            cache.put(&reference(), b"12345").await.unwrap(),
            ReplayCachePutOutcome::SkippedOversized
        );
        assert_eq!(cache.object_count().await, 0);
    }

    #[tokio::test]
    #[ignore = "requires the local Valkey service"]
    async fn valkey_backend_round_trips_binary_with_ttl() {
        let client = RedisClient::open("redis://127.0.0.1:6379/15", None).unwrap();
        let cache = ValkeyReplayCache::new(
            client.get_managed_connection().await.unwrap(),
            ReplayCacheConfig {
                key_prefix: format!("test:replay-cache:{}", uuid::Uuid::new_v4()),
                ttl: Duration::from_secs(2),
                max_value_bytes: 64,
                operation_timeout: Duration::from_secs(2),
            },
        )
        .unwrap();
        let expected = reference();
        assert_eq!(cache.get(&expected).await.unwrap(), ReplayCacheLookup::Miss);
        assert_eq!(
            cache.put(&expected, &[0, 1, 2, 255]).await.unwrap(),
            ReplayCachePutOutcome::Stored
        );
        assert_eq!(
            cache.get(&expected).await.unwrap(),
            ReplayCacheLookup::Hit(vec![0, 1, 2, 255])
        );

        let key = cache.config().key_for(&expected).unwrap();
        let mut redis = client.get_managed_connection().await.unwrap();
        let ttl: i64 = redis.ttl(&key).await.unwrap();
        assert!((1..=2).contains(&ttl));

        redis
            .set_ex::<_, _, ()>(&key, vec![7_u8; 65], 2)
            .await
            .unwrap();
        assert_eq!(
            cache.get(&expected).await.unwrap(),
            ReplayCacheLookup::RejectedOversized
        );
        assert_eq!(
            cache.put(&expected, &[8_u8; 65]).await.unwrap(),
            ReplayCachePutOutcome::SkippedOversized
        );
        redis.del::<_, ()>(&key).await.unwrap();
    }
}
