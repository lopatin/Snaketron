//! Durable, private object storage for completed game recordings.
//!
//! This module defines the versioned object contract and the production and
//! in-memory implementations used by the completion outbox. Keeping this
//! boundary behind a trait prevents a live game actor from depending directly
//! on S3 while still making archive persistence part of durable completion.

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ChecksumMode, ServerSideEncryption, StorageClass};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use flate2::Compression;
use flate2::GzBuilder;
use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

pub const REPLAY_OBJECT_FORMAT_VERSION: u16 = 1;
pub const REPLAY_MANIFEST_OBJECT_FORMAT_VERSION: u16 = 2;
pub const REPLAY_CHUNK_OBJECT_FORMAT_VERSION: u16 = 3;
pub const REPLAY_CHUNK_MANIFEST_FORMAT_VERSION: u16 = 1;
/// Replays above this canonical JSON size are represented by a small manifest
/// reference in completion metadata. Each content-addressed object remains
/// comfortably below the default ElastiCache value budget.
pub const REPLAY_CHUNK_UNCOMPRESSED_BYTES: usize = 1024 * 1024;
pub const REPLAY_CONTENT_TYPE: &str = "application/vnd.snaketron.replay+json";
pub const REPLAY_CONTENT_ENCODING: &str = "gzip";

pub const REPLAY_S3_BUCKET_ENV: &str = "SNAKETRON_REPLAY_S3_BUCKET";
pub const REPLAY_S3_PREFIX_ENV: &str = "SNAKETRON_REPLAY_S3_PREFIX";
pub const REPLAY_S3_FORCE_PATH_STYLE_ENV: &str = "SNAKETRON_REPLAY_S3_FORCE_PATH_STYLE";
pub const REPLAY_S3_KMS_KEY_ID_ENV: &str = "SNAKETRON_REPLAY_S3_KMS_KEY_ID";
pub const REPLAY_S3_STORAGE_CLASS_ENV: &str = "SNAKETRON_REPLAY_S3_STORAGE_CLASS";
pub const REPLAY_MAX_COMPRESSED_BYTES_ENV: &str = "SNAKETRON_REPLAY_MAX_COMPRESSED_BYTES";
pub const REPLAY_MAX_UNCOMPRESSED_BYTES_ENV: &str = "SNAKETRON_REPLAY_MAX_UNCOMPRESSED_BYTES";

const DEFAULT_KEY_PREFIX: &str = "recordings";
const DEFAULT_STORAGE_CLASS: &str = "INTELLIGENT_TIERING";
const DEFAULT_MAX_COMPRESSED_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_UNCOMPRESSED_BYTES: usize = 64 * 1024 * 1024;
const S3_RUNTIME_MAX_ATTEMPTS: u32 = 5;

const META_FORMAT_VERSION: &str = "snaketron-format-version";
const META_GAME_ID: &str = "snaketron-game-id";
const META_UNCOMPRESSED_SHA256: &str = "snaketron-uncompressed-sha256";
const META_COMPRESSED_SHA256: &str = "snaketron-compressed-sha256";
const META_UNCOMPRESSED_BYTES: &str = "snaketron-uncompressed-bytes";
const META_COMPRESSED_BYTES: &str = "snaketron-compressed-bytes";

/// Stable reference persisted alongside a completed game. Callers pass this
/// exact value back on reads; the store verifies the object against every
/// field before exposing its uncompressed recording bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayObjectMetadata {
    pub format_version: u16,
    pub game_id: u32,
    pub object_key: String,
    pub uncompressed_sha256: String,
    pub compressed_sha256: String,
    pub uncompressed_bytes: u64,
    pub compressed_bytes: u64,
}

impl ReplayObjectMetadata {
    /// Validate a reference before persisting it or attempting an object read.
    pub fn validate(&self) -> Result<()> {
        if !matches!(
            self.format_version,
            REPLAY_OBJECT_FORMAT_VERSION
                | REPLAY_MANIFEST_OBJECT_FORMAT_VERSION
                | REPLAY_CHUNK_OBJECT_FORMAT_VERSION
        ) {
            bail!(
                "unsupported replay object format version {}",
                self.format_version
            );
        }
        if self.object_key.is_empty() {
            bail!("replay object key cannot be empty");
        }
        validate_sha256_hex("uncompressed", &self.uncompressed_sha256)?;
        validate_sha256_hex("compressed", &self.compressed_sha256)?;
        Ok(())
    }
}

/// Immutable manifest stored as its own verified object. DynamoDB and the
/// completion record retain only the manifest object's compact metadata; the
/// potentially long chunk list never enters either bounded record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayChunkManifestV1 {
    pub format_version: u16,
    pub game_id: u32,
    pub recording_uncompressed_sha256: String,
    pub recording_uncompressed_bytes: u64,
    pub chunks: Vec<ReplayObjectMetadata>,
}

impl ReplayChunkManifestV1 {
    pub fn validate(&self) -> Result<()> {
        if self.format_version != REPLAY_CHUNK_MANIFEST_FORMAT_VERSION {
            bail!(
                "unsupported replay chunk manifest version {}",
                self.format_version
            );
        }
        validate_sha256_hex("recording", &self.recording_uncompressed_sha256)?;
        if self.chunks.is_empty() {
            bail!("replay chunk manifest cannot be empty");
        }
        let mut total = 0u64;
        for chunk in &self.chunks {
            chunk.validate()?;
            if chunk.format_version != REPLAY_CHUNK_OBJECT_FORMAT_VERSION {
                bail!("replay manifest contains a non-chunk object");
            }
            if chunk.game_id != self.game_id {
                bail!("replay manifest chunk targets a different game");
            }
            total = total
                .checked_add(chunk.uncompressed_bytes)
                .context("replay manifest byte length overflow")?;
        }
        if total != self.recording_uncompressed_bytes {
            bail!("replay manifest chunk lengths do not match the recording length");
        }
        Ok(())
    }
}

/// A verified recording returned in its original, uncompressed form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayRecording {
    pub metadata: ReplayObjectMetadata,
    pub bytes: Vec<u8>,
}

/// Object-store boundary used by completion persistence and replay reads.
/// Implementations must make repeated writes of the same `(game_id, bytes)`
/// safe and must never return bytes that fail metadata/checksum verification.
#[async_trait]
pub trait ReplayStore: Send + Sync {
    /// Validate a durable reference without performing object-store I/O.
    /// Cache-aside readers use this before trusting a cache hit. Backends with
    /// deterministic key rules should override the default implementation.
    fn validate_reference(&self, expected: &ReplayObjectMetadata) -> Result<()> {
        expected.validate()
    }

    async fn put_recording(
        &self,
        game_id: u32,
        recording_bytes: &[u8],
    ) -> Result<ReplayObjectMetadata>;

    async fn get_recording(
        &self,
        expected: &ReplayObjectMetadata,
    ) -> Result<Option<ReplayRecording>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayStoreConfig {
    pub bucket: String,
    pub key_prefix: String,
    pub force_path_style: bool,
    pub kms_key_id: Option<String>,
    pub storage_class: String,
    pub max_compressed_bytes: usize,
    pub max_uncompressed_bytes: usize,
}

impl ReplayStoreConfig {
    /// A missing bucket cleanly disables durable replay storage. Supplying a
    /// bucket opts the process in and makes malformed companion settings fatal
    /// at startup rather than failing only after a game completes.
    pub fn from_env() -> Result<Option<Self>> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Option<Self>> {
        let Some(bucket) = nonempty(lookup(REPLAY_S3_BUCKET_ENV)) else {
            return Ok(None);
        };
        let key_prefix = normalize_prefix(
            nonempty(lookup(REPLAY_S3_PREFIX_ENV)).unwrap_or_else(|| DEFAULT_KEY_PREFIX.to_owned()),
        )?;
        let force_path_style = match nonempty(lookup(REPLAY_S3_FORCE_PATH_STYLE_ENV)) {
            Some(value) => parse_bool(REPLAY_S3_FORCE_PATH_STYLE_ENV, &value)?,
            None => false,
        };
        let kms_key_id = nonempty(lookup(REPLAY_S3_KMS_KEY_ID_ENV));
        let storage_class = nonempty(lookup(REPLAY_S3_STORAGE_CLASS_ENV))
            .unwrap_or_else(|| DEFAULT_STORAGE_CLASS.to_owned())
            .to_ascii_uppercase();
        validate_storage_class(&storage_class)?;
        let max_compressed_bytes = parse_size(
            REPLAY_MAX_COMPRESSED_BYTES_ENV,
            lookup(REPLAY_MAX_COMPRESSED_BYTES_ENV),
            DEFAULT_MAX_COMPRESSED_BYTES,
        )?;
        let max_uncompressed_bytes = parse_size(
            REPLAY_MAX_UNCOMPRESSED_BYTES_ENV,
            lookup(REPLAY_MAX_UNCOMPRESSED_BYTES_ENV),
            DEFAULT_MAX_UNCOMPRESSED_BYTES,
        )?;
        let config = Self {
            bucket,
            key_prefix,
            force_path_style,
            kms_key_id,
            storage_class,
            max_compressed_bytes,
            max_uncompressed_bytes,
        };
        config.validate()?;
        Ok(Some(config))
    }

    /// Validate programmatically constructed configuration as strictly as
    /// environment-derived configuration.
    pub fn validate(&self) -> Result<()> {
        if self.bucket.trim().is_empty() || self.bucket.trim() != self.bucket {
            bail!(
                "{REPLAY_S3_BUCKET_ENV} must be a non-empty bucket name without surrounding whitespace"
            );
        }
        if normalize_prefix(self.key_prefix.clone())? != self.key_prefix {
            bail!("{REPLAY_S3_PREFIX_ENV} must not have leading or trailing slashes");
        }
        if self
            .kms_key_id
            .as_ref()
            .is_some_and(|key_id| key_id.trim().is_empty() || key_id.trim() != key_id)
        {
            bail!("{REPLAY_S3_KMS_KEY_ID_ENV} cannot be blank or have surrounding whitespace");
        }
        validate_storage_class(&self.storage_class)?;
        if self.max_compressed_bytes == 0 {
            bail!("{REPLAY_MAX_COMPRESSED_BYTES_ENV} must be a positive integer byte count");
        }
        if self.max_uncompressed_bytes == 0 {
            bail!("{REPLAY_MAX_UNCOMPRESSED_BYTES_ENV} must be a positive integer byte count");
        }
        Ok(())
    }

    pub fn object_key(&self, game_id: u32) -> String {
        format!(
            "{}/v{}/games/{game_id:010}.replay.json.gz",
            self.key_prefix, REPLAY_OBJECT_FORMAT_VERSION
        )
    }

    pub fn manifest_object_key(&self, game_id: u32, manifest_sha256: &str) -> String {
        format!(
            "{}/v{}/games/{game_id:010}/manifests/{manifest_sha256}.manifest.json.gz",
            self.key_prefix, REPLAY_MANIFEST_OBJECT_FORMAT_VERSION,
        )
    }

    pub fn chunk_object_key(&self, game_id: u32, uncompressed_sha256: &str) -> String {
        format!(
            "{}/v{}/games/{game_id:010}/chunks/{uncompressed_sha256}.part.gz",
            self.key_prefix, REPLAY_MANIFEST_OBJECT_FORMAT_VERSION
        )
    }
}

#[derive(Clone)]
pub struct S3ReplayStore {
    client: Client,
    config: ReplayStoreConfig,
}

impl S3ReplayStore {
    pub async fn new(config: ReplayStoreConfig) -> Result<Self> {
        config.validate()?;
        let timeouts = aws_config::timeout::TimeoutConfig::builder()
            .connect_timeout(Duration::from_secs(2))
            .operation_attempt_timeout(Duration::from_secs(10))
            .operation_timeout(Duration::from_secs(30))
            .build();
        let retries =
            aws_config::retry::RetryConfig::standard().with_max_attempts(S3_RUNTIME_MAX_ATTEMPTS);
        let shared = aws_config::from_env()
            .timeout_config(timeouts)
            .retry_config(retries)
            .load()
            .await;
        let s3_config = aws_sdk_s3::config::Builder::from(&shared)
            .force_path_style(config.force_path_style)
            .build();
        Ok(Self {
            client: Client::from_conf(s3_config),
            config,
        })
    }

    /// Constructor for tests or applications that already own an SDK client.
    pub fn with_client(client: Client, config: ReplayStoreConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self { client, config })
    }

    pub fn config(&self) -> &ReplayStoreConfig {
        &self.config
    }

    async fn put_encoded_object(&self, encoded: EncodedReplay) -> Result<()> {
        let game_id = encoded.metadata.game_id;
        let checksum_sha256 = BASE64_STANDARD.encode(Sha256::digest(&encoded.compressed));
        let mut request = self
            .client
            .put_object()
            .bucket(&self.config.bucket)
            .key(&encoded.metadata.object_key)
            .body(ByteStream::from(encoded.compressed))
            .content_type(REPLAY_CONTENT_TYPE)
            .content_encoding(REPLAY_CONTENT_ENCODING)
            .checksum_sha256(checksum_sha256)
            .storage_class(StorageClass::from(self.config.storage_class.as_str()))
            // No ACL is supplied: bucket policy/public-access-block remains the
            // sole authority, and the object is never made public by this API.
            .set_metadata(Some(s3_metadata(&encoded.metadata)));

        request = match &self.config.kms_key_id {
            Some(key_id) => request
                .server_side_encryption(ServerSideEncryption::AwsKms)
                .ssekms_key_id(key_id),
            None => request.server_side_encryption(ServerSideEncryption::Aes256),
        };

        request
            .send()
            .await
            .with_context(|| format!("failed to store replay object for game {game_id}"))?;
        Ok(())
    }
}

#[async_trait]
impl ReplayStore for S3ReplayStore {
    fn validate_reference(&self, expected: &ReplayObjectMetadata) -> Result<()> {
        validate_reference_for_config(&self.config, expected)
    }

    async fn put_recording(
        &self,
        game_id: u32,
        recording_bytes: &[u8],
    ) -> Result<ReplayObjectMetadata> {
        let archive = encode_recording_objects(&self.config, game_id, recording_bytes)?;
        for encoded in archive.objects {
            self.put_encoded_object(encoded).await?;
        }
        Ok(archive.root)
    }

    async fn get_recording(
        &self,
        expected: &ReplayObjectMetadata,
    ) -> Result<Option<ReplayRecording>> {
        self.validate_reference(expected)?;

        let response = match self
            .client
            .get_object()
            .bucket(&self.config.bucket)
            .key(&expected.object_key)
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) if is_missing_s3_object(&error) => return Ok(None),
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("failed to load replay for game {}", expected.game_id)
                });
            }
        };

        if response.content_encoding() != Some(REPLAY_CONTENT_ENCODING) {
            bail!("replay object has missing or unsupported content encoding");
        }
        if response.content_type() != Some(REPLAY_CONTENT_TYPE) {
            bail!("replay object has missing or unsupported content type");
        }
        if let Some(content_length) = response.content_length()
            && (content_length < 0 || content_length as usize > self.config.max_compressed_bytes)
        {
            bail!("replay object exceeds configured compressed size limit");
        }
        let actual_metadata = metadata_from_s3(response.metadata(), &expected.object_key)?;
        verify_expected_metadata(expected, &actual_metadata)?;

        let advertised_checksum = response.checksum_sha256().map(str::to_owned);
        let compressed = response
            .body
            .collect()
            .await
            .context("failed to collect replay object body")?
            .into_bytes()
            .to_vec();
        if compressed.len() > self.config.max_compressed_bytes {
            bail!("replay object exceeds configured compressed size limit");
        }
        let computed_base64 = BASE64_STANDARD.encode(Sha256::digest(&compressed));
        if advertised_checksum
            .as_deref()
            .is_some_and(|advertised| advertised != computed_base64)
        {
            bail!("replay object S3 checksum mismatch");
        }

        decode_and_verify(&self.config, expected, compressed).map(Some)
    }
}

/// Deterministic in-memory backend. It stores the exact encoded object, so its
/// behavior exercises the same checksum/metadata validation as S3 without a
/// network dependency.
#[derive(Clone)]
pub struct InMemoryReplayStore {
    config: ReplayStoreConfig,
    objects: Arc<RwLock<HashMap<String, EncodedReplay>>>,
}

impl InMemoryReplayStore {
    pub fn new() -> Self {
        Self::with_config(ReplayStoreConfig {
            bucket: "memory-replays".to_owned(),
            key_prefix: DEFAULT_KEY_PREFIX.to_owned(),
            force_path_style: false,
            kms_key_id: None,
            storage_class: DEFAULT_STORAGE_CLASS.to_owned(),
            max_compressed_bytes: DEFAULT_MAX_COMPRESSED_BYTES,
            max_uncompressed_bytes: DEFAULT_MAX_UNCOMPRESSED_BYTES,
        })
        .expect("built-in replay-store config must be valid")
    }

    pub fn with_config(config: ReplayStoreConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            objects: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub async fn object_count(&self) -> usize {
        self.objects.read().await.len()
    }
}

impl Default for InMemoryReplayStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl ReplayStore for InMemoryReplayStore {
    fn validate_reference(&self, expected: &ReplayObjectMetadata) -> Result<()> {
        validate_reference_for_config(&self.config, expected)
    }

    async fn put_recording(
        &self,
        game_id: u32,
        recording_bytes: &[u8],
    ) -> Result<ReplayObjectMetadata> {
        let archive = encode_recording_objects(&self.config, game_id, recording_bytes)?;
        let root = archive.root;
        let mut objects = self.objects.write().await;
        for encoded in archive.objects {
            objects.insert(encoded.metadata.object_key.clone(), encoded);
        }
        Ok(root)
    }

    async fn get_recording(
        &self,
        expected: &ReplayObjectMetadata,
    ) -> Result<Option<ReplayRecording>> {
        self.validate_reference(expected)?;
        let Some(encoded) = self.objects.read().await.get(&expected.object_key).cloned() else {
            return Ok(None);
        };
        verify_expected_metadata(expected, &encoded.metadata)?;
        decode_and_verify(&self.config, expected, encoded.compressed).map(Some)
    }
}

#[derive(Clone)]
struct EncodedReplay {
    metadata: ReplayObjectMetadata,
    compressed: Vec<u8>,
}

struct EncodedReplayArchive {
    root: ReplayObjectMetadata,
    /// Dependency objects first and the manifest last. Publishing the root
    /// only after every content-addressed chunk avoids exposing a partial
    /// archive even when an S3 request fails midway through a retry.
    objects: Vec<EncodedReplay>,
}

fn encode_recording_objects(
    config: &ReplayStoreConfig,
    game_id: u32,
    recording_bytes: &[u8],
) -> Result<EncodedReplayArchive> {
    if recording_bytes.len() <= REPLAY_CHUNK_UNCOMPRESSED_BYTES {
        let encoded = encode_object(
            config,
            REPLAY_OBJECT_FORMAT_VERSION,
            game_id,
            config.object_key(game_id),
            recording_bytes,
        )?;
        return Ok(EncodedReplayArchive {
            root: encoded.metadata.clone(),
            objects: vec![encoded],
        });
    }

    let mut chunks = Vec::new();
    let mut encoded_chunks = Vec::new();
    for bytes in recording_bytes.chunks(REPLAY_CHUNK_UNCOMPRESSED_BYTES) {
        let digest = sha256_hex(bytes);
        let encoded = encode_object(
            config,
            REPLAY_CHUNK_OBJECT_FORMAT_VERSION,
            game_id,
            config.chunk_object_key(game_id, &digest),
            bytes,
        )?;
        chunks.push(encoded.metadata.clone());
        encoded_chunks.push(encoded);
    }

    let manifest = ReplayChunkManifestV1 {
        format_version: REPLAY_CHUNK_MANIFEST_FORMAT_VERSION,
        game_id,
        recording_uncompressed_sha256: sha256_hex(recording_bytes),
        recording_uncompressed_bytes: recording_bytes.len() as u64,
        chunks,
    };
    manifest.validate()?;
    let manifest_bytes =
        serde_json::to_vec(&manifest).context("failed to serialize replay chunk manifest")?;
    let manifest_sha256 = sha256_hex(&manifest_bytes);
    let encoded_manifest = encode_object(
        config,
        REPLAY_MANIFEST_OBJECT_FORMAT_VERSION,
        game_id,
        config.manifest_object_key(game_id, &manifest_sha256),
        &manifest_bytes,
    )?;
    let root = encoded_manifest.metadata.clone();
    encoded_chunks.push(encoded_manifest);
    Ok(EncodedReplayArchive {
        root,
        objects: encoded_chunks,
    })
}

fn encode_object(
    config: &ReplayStoreConfig,
    format_version: u16,
    game_id: u32,
    object_key: String,
    bytes: &[u8],
) -> Result<EncodedReplay> {
    if bytes.len() > config.max_uncompressed_bytes {
        bail!("replay object exceeds configured uncompressed size limit");
    }
    // An explicit zero mtime keeps the encoded object and its checksum stable
    // across retries, deployments, and hosts.
    let mut encoder = GzBuilder::new()
        .mtime(0)
        .write(Vec::new(), Compression::default());
    encoder
        .write_all(bytes)
        .context("failed to gzip replay recording")?;
    let compressed = encoder.finish().context("failed to finish replay gzip")?;
    if compressed.len() > config.max_compressed_bytes {
        bail!("replay recording exceeds configured compressed size limit");
    }

    let metadata = ReplayObjectMetadata {
        format_version,
        game_id,
        object_key,
        uncompressed_sha256: sha256_hex(bytes),
        compressed_sha256: sha256_hex(&compressed),
        uncompressed_bytes: bytes.len() as u64,
        compressed_bytes: compressed.len() as u64,
    };
    Ok(EncodedReplay {
        metadata,
        compressed,
    })
}

fn decode_and_verify(
    config: &ReplayStoreConfig,
    expected: &ReplayObjectMetadata,
    compressed: Vec<u8>,
) -> Result<ReplayRecording> {
    if compressed.len() as u64 != expected.compressed_bytes {
        bail!("replay object compressed length mismatch");
    }
    let compressed_sha256 = sha256_hex(&compressed);
    if compressed_sha256 != expected.compressed_sha256 {
        bail!("replay object compressed checksum mismatch");
    }

    let maximum = config
        .max_uncompressed_bytes
        .checked_add(1)
        .context("replay uncompressed size limit overflow")?;
    let mut decoder = GzDecoder::new(compressed.as_slice()).take(maximum as u64);
    let mut bytes = Vec::with_capacity(
        usize::try_from(expected.uncompressed_bytes)
            .unwrap_or(config.max_uncompressed_bytes)
            .min(config.max_uncompressed_bytes),
    );
    decoder
        .read_to_end(&mut bytes)
        .context("failed to decompress replay object")?;
    if bytes.len() > config.max_uncompressed_bytes {
        bail!("replay object exceeds configured uncompressed size limit");
    }
    if bytes.len() as u64 != expected.uncompressed_bytes {
        bail!("replay object uncompressed length mismatch");
    }
    if sha256_hex(&bytes) != expected.uncompressed_sha256 {
        bail!("replay object uncompressed checksum mismatch");
    }

    Ok(ReplayRecording {
        metadata: expected.clone(),
        bytes,
    })
}

fn s3_metadata(metadata: &ReplayObjectMetadata) -> HashMap<String, String> {
    HashMap::from([
        (
            META_FORMAT_VERSION.to_owned(),
            metadata.format_version.to_string(),
        ),
        (META_GAME_ID.to_owned(), metadata.game_id.to_string()),
        (
            META_UNCOMPRESSED_SHA256.to_owned(),
            metadata.uncompressed_sha256.clone(),
        ),
        (
            META_COMPRESSED_SHA256.to_owned(),
            metadata.compressed_sha256.clone(),
        ),
        (
            META_UNCOMPRESSED_BYTES.to_owned(),
            metadata.uncompressed_bytes.to_string(),
        ),
        (
            META_COMPRESSED_BYTES.to_owned(),
            metadata.compressed_bytes.to_string(),
        ),
    ])
}

fn metadata_from_s3(
    metadata: Option<&HashMap<String, String>>,
    object_key: &str,
) -> Result<ReplayObjectMetadata> {
    let metadata = metadata.context("replay object is missing integrity metadata")?;
    let read = |key: &str| {
        metadata
            .get(key)
            .cloned()
            .with_context(|| format!("replay object is missing metadata field {key}"))
    };
    let parsed = ReplayObjectMetadata {
        format_version: read(META_FORMAT_VERSION)?
            .parse()
            .context("invalid replay format version metadata")?,
        game_id: read(META_GAME_ID)?
            .parse()
            .context("invalid replay game ID metadata")?,
        object_key: object_key.to_owned(),
        uncompressed_sha256: read(META_UNCOMPRESSED_SHA256)?,
        compressed_sha256: read(META_COMPRESSED_SHA256)?,
        uncompressed_bytes: read(META_UNCOMPRESSED_BYTES)?
            .parse()
            .context("invalid replay uncompressed length metadata")?,
        compressed_bytes: read(META_COMPRESSED_BYTES)?
            .parse()
            .context("invalid replay compressed length metadata")?,
    };
    parsed.validate()?;
    Ok(parsed)
}

fn verify_expected_metadata(
    expected: &ReplayObjectMetadata,
    actual: &ReplayObjectMetadata,
) -> Result<()> {
    if actual != expected {
        bail!("replay object metadata does not match its durable reference");
    }
    Ok(())
}

fn validate_reference_for_config(
    config: &ReplayStoreConfig,
    expected: &ReplayObjectMetadata,
) -> Result<()> {
    expected.validate()?;
    let expected_key = match expected.format_version {
        REPLAY_OBJECT_FORMAT_VERSION => config.object_key(expected.game_id),
        REPLAY_MANIFEST_OBJECT_FORMAT_VERSION => {
            config.manifest_object_key(expected.game_id, &expected.uncompressed_sha256)
        }
        REPLAY_CHUNK_OBJECT_FORMAT_VERSION => {
            config.chunk_object_key(expected.game_id, &expected.uncompressed_sha256)
        }
        _ => unreachable!("metadata validation rejects unsupported versions"),
    };
    if expected.object_key != expected_key {
        bail!(
            "replay object key {} does not match deterministic key {}",
            expected.object_key,
            expected_key
        );
    }
    Ok(())
}

fn is_missing_s3_object(
    error: &aws_sdk_s3::error::SdkError<aws_sdk_s3::operation::get_object::GetObjectError>,
) -> bool {
    error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
        .is_some_and(|code| matches!(code, "NoSuchKey" | "NotFound" | "404"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn validate_sha256_hex(label: &str, value: &str) -> Result<()> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        bail!("replay {label} SHA-256 must be 64 hexadecimal characters");
    }
    Ok(())
}

fn nonempty(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_owned())
    })
}

fn normalize_prefix(prefix: String) -> Result<String> {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty()
        || prefix
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
        || prefix.contains('\\')
    {
        bail!("{REPLAY_S3_PREFIX_ENV} must be a non-empty safe S3 key prefix");
    }
    Ok(prefix.to_owned())
}

fn parse_bool(name: &str, value: &str) -> Result<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => bail!("{name} must be true, false, 1, or 0"),
    }
}

fn parse_size(name: &str, value: Option<String>, default: usize) -> Result<usize> {
    let Some(value) = nonempty(value) else {
        return Ok(default);
    };
    value
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| anyhow!("{name} must be a positive integer byte count"))
}

fn validate_storage_class(storage_class: &str) -> Result<()> {
    if matches!(
        storage_class,
        "STANDARD" | "STANDARD_IA" | "ONEZONE_IA" | "INTELLIGENT_TIERING"
    ) {
        Ok(())
    } else {
        bail!(
            "{REPLAY_S3_STORAGE_CLASS_ENV} must be STANDARD, STANDARD_IA, ONEZONE_IA, or INTELLIGENT_TIERING"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn in_memory_store_round_trips_and_uses_a_versioned_deterministic_key() {
        let store = InMemoryReplayStore::new();
        let bytes = br#"{"game_id":42,"events":[{"tick":1}]}"#;

        let reference = store.put_recording(42, bytes).await.unwrap();
        assert_eq!(
            reference.object_key,
            "recordings/v1/games/0000000042.replay.json.gz"
        );
        assert!(reference.compressed_bytes < reference.uncompressed_bytes + 64);

        let replay = store
            .get_recording(&reference)
            .await
            .unwrap()
            .expect("stored replay");
        assert_eq!(replay.metadata, reference);
        assert_eq!(replay.bytes, bytes);
    }

    #[tokio::test]
    async fn corrupted_bytes_and_hash_metadata_are_rejected() {
        let store = InMemoryReplayStore::new();
        let reference = store
            .put_recording(7, b"authoritative recording")
            .await
            .unwrap();

        {
            let mut objects = store.objects.write().await;
            let object = objects.get_mut(&reference.object_key).unwrap();
            let last = object.compressed.len() - 1;
            object.compressed[last] ^= 0xff;
        }
        let error = store.get_recording(&reference).await.unwrap_err();
        assert!(error.to_string().contains("compressed checksum mismatch"));

        let store = InMemoryReplayStore::new();
        let reference = store.put_recording(8, b"another recording").await.unwrap();
        {
            let mut objects = store.objects.write().await;
            objects
                .get_mut(&reference.object_key)
                .unwrap()
                .metadata
                .uncompressed_sha256 = "0".repeat(64);
        }
        let error = store.get_recording(&reference).await.unwrap_err();
        assert!(error.to_string().contains("metadata does not match"));
    }

    #[tokio::test]
    async fn repeated_put_is_an_idempotent_overwrite() {
        let store = InMemoryReplayStore::new();
        let bytes = b"same immutable replay";

        let first = store.put_recording(99, bytes).await.unwrap();
        let second = store.put_recording(99, bytes).await.unwrap();

        assert_eq!(first, second);
        assert_eq!(store.object_count().await, 1);
        assert_eq!(
            store.get_recording(&first).await.unwrap().unwrap().bytes,
            bytes
        );
    }

    #[tokio::test]
    async fn chunk_manifest_accepts_recordings_beyond_legacy_aggregate_caps() {
        let store = InMemoryReplayStore::new();
        let bytes = vec![b'x'; 65 * 1024 * 1024 + 17];

        let root = store.put_recording(100, &bytes).await.unwrap();
        assert_eq!(root.format_version, REPLAY_MANIFEST_OBJECT_FORMAT_VERSION);
        let manifest_object = store.get_recording(&root).await.unwrap().unwrap();
        let manifest: ReplayChunkManifestV1 =
            serde_json::from_slice(&manifest_object.bytes).unwrap();
        manifest.validate().unwrap();
        assert_eq!(manifest.recording_uncompressed_bytes, bytes.len() as u64);
        assert!(manifest.chunks.len() > 64);
    }

    #[tokio::test]
    async fn content_addressed_manifest_prevents_stale_prestage_overwrite() {
        let store = InMemoryReplayStore::new();
        let successor_bytes = vec![b's'; REPLAY_CHUNK_UNCOMPRESSED_BYTES + 33];
        let stale_actor_bytes = vec![b'o'; REPLAY_CHUNK_UNCOMPRESSED_BYTES + 33];

        let successor = store.put_recording(101, &successor_bytes).await.unwrap();
        let stale = store.put_recording(101, &stale_actor_bytes).await.unwrap();
        assert_ne!(successor.object_key, stale.object_key);
        assert_ne!(successor.uncompressed_sha256, stale.uncompressed_sha256);

        let successor_manifest: ReplayChunkManifestV1 = serde_json::from_slice(
            &store
                .get_recording(&successor)
                .await
                .unwrap()
                .unwrap()
                .bytes,
        )
        .unwrap();
        let stale_manifest: ReplayChunkManifestV1 =
            serde_json::from_slice(&store.get_recording(&stale).await.unwrap().unwrap().bytes)
                .unwrap();
        assert_eq!(
            successor_manifest.recording_uncompressed_sha256,
            sha256_hex(&successor_bytes)
        );
        assert_eq!(
            stale_manifest.recording_uncompressed_sha256,
            sha256_hex(&stale_actor_bytes)
        );
    }

    /// Run explicitly against LocalStack with:
    ///
    /// `AWS_ENDPOINT_URL=http://localhost:4566 AWS_REGION=us-east-1 \
    /// AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
    /// SNAKETRON_REPLAY_S3_BUCKET=snaketron-replays-dev \
    /// SNAKETRON_REPLAY_S3_FORCE_PATH_STYLE=true \
    /// cargo test -p server s3_localstack_round_trip -- --ignored`
    #[tokio::test]
    #[ignore = "requires a configured LocalStack S3 service"]
    async fn s3_localstack_round_trip_and_idempotent_overwrite() {
        let config = ReplayStoreConfig::from_env()
            .expect("valid LocalStack replay configuration")
            .expect("SNAKETRON_REPLAY_S3_BUCKET must be set");
        let store = S3ReplayStore::new(config).await.unwrap();
        let bytes = br#"{"game_id":4294967000,"events":[{"tick":1}]}"#;

        let first = store.put_recording(4_294_967_000, bytes).await.unwrap();
        let second = store.put_recording(4_294_967_000, bytes).await.unwrap();
        assert_eq!(first, second);

        let replay = store
            .get_recording(&first)
            .await
            .unwrap()
            .expect("stored LocalStack replay");
        assert_eq!(replay.metadata, first);
        assert_eq!(replay.bytes, bytes);
    }

    #[test]
    fn environment_config_is_opt_in_and_validated() {
        let disabled = ReplayStoreConfig::from_lookup(|_| None).unwrap();
        assert!(disabled.is_none());

        let configured = ReplayStoreConfig::from_lookup(|name| match name {
            REPLAY_S3_BUCKET_ENV => Some("private-bucket".into()),
            REPLAY_S3_PREFIX_ENV => Some("/game-replays/".into()),
            REPLAY_S3_FORCE_PATH_STYLE_ENV => Some("true".into()),
            _ => None,
        })
        .unwrap()
        .unwrap();
        assert_eq!(configured.bucket, "private-bucket");
        assert_eq!(configured.key_prefix, "game-replays");
        assert!(configured.force_path_style);
        assert_eq!(configured.storage_class, "INTELLIGENT_TIERING");

        let unsafe_prefix = ReplayStoreConfig::from_lookup(|name| match name {
            REPLAY_S3_BUCKET_ENV => Some("private-bucket".into()),
            REPLAY_S3_PREFIX_ENV => Some("recordings/../other".into()),
            _ => None,
        })
        .unwrap_err();
        assert!(unsafe_prefix.to_string().contains("safe S3 key prefix"));

        let invalid_size = ReplayStoreConfig::from_lookup(|name| match name {
            REPLAY_S3_BUCKET_ENV => Some("private-bucket".into()),
            REPLAY_MAX_COMPRESSED_BYTES_ENV => Some("0".into()),
            _ => None,
        })
        .unwrap_err();
        assert!(invalid_size.to_string().contains("positive integer"));
    }
}
