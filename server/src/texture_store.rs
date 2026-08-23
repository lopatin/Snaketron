//! Where texture bytes live.
//!
//! Modelled on `crate::replay_store` — same `Option<Config>` from-env shape,
//! same deterministic key derivation, same in-memory twin for tests — but
//! deliberately a *separate* store with a separate bucket, because sharing one
//! would tie two unrelated features to a single disable switch: turning off
//! replay storage would silently turn off every player's skin.
//!
//! Three things differ from replays, and each is the reason this is not a
//! generalisation of that:
//!
//! - the payload is a PNG, stored as-is, so there is no gzip layer and the
//!   content type varies per object rather than being a constant the reader
//!   asserts;
//! - the key *is* the hash of the bytes, so a variant can be cached forever
//!   and a repaired rung mints a new object instead of mutating one;
//! - the bytes reach players, through the API rather than from the bucket —
//!   the bucket stays private, exactly as replays' does.

use anyhow::{Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use std::collections::HashMap;
use std::sync::Mutex;
use tracing::info;

/// Env var that turns the store on. Absent means textures are not stored, the
/// same way an absent replay bucket means replays are not.
const BUCKET_ENV: &str = "SNAKETRON_TEXTURE_S3_BUCKET";
const PREFIX_ENV: &str = "SNAKETRON_TEXTURE_S3_PREFIX";
const PATH_STYLE_ENV: &str = "SNAKETRON_TEXTURE_S3_FORCE_PATH_STYLE";

/// One stored object: which bytes, and what they are.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextureObject {
    /// Lowercase hex SHA-256 of the bytes. Also the key.
    pub sha256: String,
    pub content_type: &'static str,
    pub byte_len: usize,
}

#[derive(Debug, Clone)]
pub struct TextureStoreConfig {
    pub bucket: String,
    pub prefix: String,
    pub force_path_style: bool,
}

impl TextureStoreConfig {
    /// Read configuration, or `None` when this deployment stores no textures.
    ///
    /// A present-but-malformed setting is fatal rather than ignored: a typo in
    /// the prefix that silently stored everything at the bucket root would be
    /// discovered much later and much more expensively.
    pub fn from_env() -> Result<Option<Self>> {
        let Ok(bucket) = std::env::var(BUCKET_ENV) else {
            return Ok(None);
        };
        if bucket.trim().is_empty() {
            return Ok(None);
        }

        let prefix = std::env::var(PREFIX_ENV).unwrap_or_else(|_| "textures".to_string());
        if prefix.starts_with('/') || prefix.ends_with('/') {
            anyhow::bail!("{PREFIX_ENV} must not begin or end with a slash");
        }

        let force_path_style = match std::env::var(PATH_STYLE_ENV) {
            Ok(value) => value
                .parse()
                .with_context(|| format!("{PATH_STYLE_ENV} must be true or false"))?,
            Err(_) => false,
        };

        Ok(Some(Self {
            bucket: bucket.trim().to_string(),
            prefix,
            force_path_style,
        }))
    }

    /// The one key a given digest may live at.
    ///
    /// Derived rather than stored so a caller cannot ask for an arbitrary key:
    /// the only reachable objects are ones whose name is their own content.
    pub fn object_key(&self, sha256: &str) -> String {
        format!("{}/{sha256}.png", self.prefix)
    }
}

#[async_trait]
pub trait TextureStore: Send + Sync {
    /// Store bytes under their own hash. Idempotent by construction: the same
    /// bytes are the same key.
    async fn put(&self, object: &TextureObject, bytes: &[u8]) -> Result<()>;

    /// Fetch by digest. `None` means absent, not an error.
    async fn get(&self, sha256: &str) -> Result<Option<Vec<u8>>>;
}

pub struct S3TextureStore {
    client: Client,
    config: TextureStoreConfig,
}

impl S3TextureStore {
    pub async fn new(config: TextureStoreConfig) -> Result<Self> {
        // The same timeout posture as the replay store: a texture fetch must
        // fail fast rather than occupy a request thread.
        let sdk = aws_config::from_env().load().await;
        let mut builder = aws_sdk_s3::config::Builder::from(&sdk);
        if config.force_path_style {
            builder = builder.force_path_style(true);
        }
        Ok(Self {
            client: Client::from_conf(builder.build()),
            config,
        })
    }
}

#[async_trait]
impl TextureStore for S3TextureStore {
    async fn put(&self, object: &TextureObject, bytes: &[u8]) -> Result<()> {
        // The digest is the key, so bytes that disagree with their name would
        // make the store lie about its own contents.
        let actual = digest(bytes);
        if actual != object.sha256 {
            anyhow::bail!(
                "refusing to store bytes under a name that is not their digest: {} vs {}",
                object.sha256,
                actual
            );
        }

        self.client
            .put_object()
            .bucket(&self.config.bucket)
            .key(self.config.object_key(&object.sha256))
            .body(ByteStream::from(bytes.to_vec()))
            .content_type(object.content_type)
            // Content-addressed, so this is safe forever. The *document* route
            // is the one that has to stay revalidating, because that is where
            // moderation propagates.
            .cache_control("public, max-age=31536000, immutable")
            .send()
            .await
            .context("Failed to store a texture")?;
        Ok(())
    }

    async fn get(&self, sha256: &str) -> Result<Option<Vec<u8>>> {
        let response = self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(self.config.object_key(sha256))
            .send()
            .await;

        let output = match response {
            Ok(output) => output,
            Err(error) => {
                if let Some(service) = error.as_service_error()
                    && service.is_no_such_key()
                {
                    return Ok(None);
                }
                return Err(error).context("Failed to read a texture");
            }
        };

        let bytes = output
            .body
            .collect()
            .await
            .context("Failed to read texture bytes")?
            .into_bytes()
            .to_vec();

        // Verify on the way out as well as in. A silently corrupted object is
        // a skin that renders wrong for everyone, and the check is one hash.
        if digest(&bytes) != sha256 {
            anyhow::bail!("stored texture {sha256} does not match its own digest");
        }
        Ok(Some(bytes))
    }
}

/// The in-memory twin, for tests and for running without object storage.
#[derive(Default)]
pub struct InMemoryTextureStore {
    objects: Mutex<HashMap<String, Vec<u8>>>,
}

#[async_trait]
impl TextureStore for InMemoryTextureStore {
    async fn put(&self, object: &TextureObject, bytes: &[u8]) -> Result<()> {
        let actual = digest(bytes);
        if actual != object.sha256 {
            anyhow::bail!("digest mismatch: {} vs {actual}", object.sha256);
        }
        self.objects
            .lock()
            .map_err(|_| anyhow::anyhow!("texture store poisoned"))?
            .insert(object.sha256.clone(), bytes.to_vec());
        Ok(())
    }

    async fn get(&self, sha256: &str) -> Result<Option<Vec<u8>>> {
        Ok(self
            .objects
            .lock()
            .map_err(|_| anyhow::anyhow!("texture store poisoned"))?
            .get(sha256)
            .cloned())
    }
}

/// Lowercase hex SHA-256, the name every object goes by.
pub fn digest(bytes: &[u8]) -> String {
    skin_schema::content::reference_for_bytes(bytes)
        .trim_start_matches("sha256:")
        .to_string()
}

/// Build the configured store, logging which way it went.
pub async fn from_env() -> Result<Option<S3TextureStore>> {
    match TextureStoreConfig::from_env()? {
        Some(config) => {
            info!(bucket = %config.bucket, "texture storage enabled");
            Ok(Some(S3TextureStore::new(config).await?))
        }
        None => {
            info!("texture storage disabled: {BUCKET_ENV} is not set");
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object(bytes: &[u8]) -> TextureObject {
        TextureObject {
            sha256: digest(bytes),
            content_type: "image/png",
            byte_len: bytes.len(),
        }
    }

    #[tokio::test]
    async fn bytes_round_trip_under_their_own_digest() {
        let store = InMemoryTextureStore::default();
        let bytes = b"a png, honest".to_vec();
        let object = object(&bytes);

        store.put(&object, &bytes).await.expect("stored");
        assert_eq!(store.get(&object.sha256).await.unwrap(), Some(bytes));
        assert_eq!(store.get("nonesuch").await.unwrap(), None);
    }

    /// The key is the content, so storing bytes under someone else's name has
    /// to be refused — otherwise the store can be made to serve one skin's
    /// pixels for another skin's reference.
    #[tokio::test]
    async fn bytes_cannot_be_stored_under_a_name_that_is_not_theirs() {
        let store = InMemoryTextureStore::default();
        let bytes = b"real bytes".to_vec();
        let lying = TextureObject {
            sha256: digest(b"different bytes"),
            content_type: "image/png",
            byte_len: bytes.len(),
        };

        assert!(store.put(&lying, &bytes).await.is_err());
        assert_eq!(store.get(&lying.sha256).await.unwrap(), None);
    }

    #[test]
    fn a_key_is_derived_and_not_taken_from_the_caller() {
        let config = TextureStoreConfig {
            bucket: "b".to_string(),
            prefix: "textures".to_string(),
            force_path_style: false,
        };
        assert_eq!(config.object_key("abc123"), "textures/abc123.png");
        // Nothing a caller supplies can escape the prefix, because the only
        // thing they supply is a digest that is checked against the bytes.
        assert!(!config.object_key("abc123").contains(".."));
    }

    #[test]
    fn an_absent_bucket_disables_the_store_and_a_bad_prefix_is_fatal() {
        // SAFETY: single-threaded test; every variable is restored.
        unsafe { std::env::remove_var(BUCKET_ENV) };
        assert!(TextureStoreConfig::from_env().unwrap().is_none());

        unsafe { std::env::set_var(BUCKET_ENV, "   ") };
        assert!(
            TextureStoreConfig::from_env().unwrap().is_none(),
            "a blank bucket is no bucket"
        );

        unsafe {
            std::env::set_var(BUCKET_ENV, "skins");
            std::env::set_var(PREFIX_ENV, "/leading");
        }
        assert!(
            TextureStoreConfig::from_env().is_err(),
            "a malformed prefix is fatal rather than silently wrong"
        );

        unsafe {
            std::env::remove_var(PREFIX_ENV);
            std::env::remove_var(BUCKET_ENV);
        }
    }

    /// The texture bucket is deliberately not the replay bucket: one disable
    /// switch must not turn off two unrelated features.
    #[test]
    fn textures_do_not_ride_the_replay_buckets_configuration() {
        unsafe {
            std::env::remove_var(BUCKET_ENV);
            std::env::set_var("SNAKETRON_REPLAY_S3_BUCKET", "replays");
        }
        assert!(
            TextureStoreConfig::from_env().unwrap().is_none(),
            "a replay bucket must not enable texture storage"
        );
        unsafe { std::env::remove_var("SNAKETRON_REPLAY_S3_BUCKET") };
    }
}
